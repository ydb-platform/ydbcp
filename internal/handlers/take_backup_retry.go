package handlers

import (
	"context"
	"errors"
	"fmt"
	"github.com/jonboulle/clockwork"
	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
	"math"
	"strings"
	"time"
	"ydbcp/internal/backup_operations"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/metrics"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"
)

func NewTBWROperationHandler(
	db db.DBConnector,
	client client.ClientConnector,
	s3 config.S3Config,
	clientConfig config.ClientConnectionConfig,
	queryBuilderFactory queries.WriteQueryBuilderFactory,
	clock clockwork.Clock,
	mon metrics.MetricsRegistry,
) types.OperationHandler {
	return func(ctx context.Context, op types.Operation) error {
		return TBWROperationHandler(ctx, op, db, client, s3, clientConfig, queryBuilderFactory, clock, mon)
	}
}

const (
	INTERNAL_MAX_RETRIES = 20
	MIN_BACKOFF          = time.Minute
	BACKOFF_EXP          = 1.5
)

type RetryDecision int

const (
	Error    RetryDecision = iota
	RunNewTb RetryDecision = iota
	Skip     RetryDecision = iota
	Success  RetryDecision = iota
)

func (r RetryDecision) String() string {
	switch r {
	case Error:
		return "Error"
	case RunNewTb:
		return "RunNewTb"
	case Skip:
		return "Skip"
	case Success:
		return "Success"
	default:
		return "Unknown"
	}
}

func exp(p int) time.Duration {
	return time.Duration(math.Pow(BACKOFF_EXP, float64(p)))
}

func shouldRetry(config *pb.RetryConfig, tbOps []*types.TakeBackupOperation, clock clockwork.Clock) *time.Time {
	if config == nil {
		return nil
	}
	ops := len(tbOps)
	lastEnd := tbOps[ops-1].Audit.CompletedAt.AsTime()
	firstStart := tbOps[0].Audit.CreatedAt.AsTime()
	if ops == INTERNAL_MAX_RETRIES {
		return nil
	}

	switch r := config.Retries.(type) {
	case *pb.RetryConfig_Count:
		{
			if int(r.Count) == ops {
				return nil
			}
		}
	case *pb.RetryConfig_MaxBackoff:
		{
			if clock.Now().Sub(firstStart) >= r.MaxBackoff.AsDuration() {
				return nil
			}
		}
	default:
		return nil
	}

	res := lastEnd.Add(exp(ops) * MIN_BACKOFF)
	return &res
}

func HandleTbOps(config *pb.RetryConfig, tbOps []*types.TakeBackupOperation, clock clockwork.Clock) (RetryDecision, error) {
	//select last tbOp.
	//if nothing, run new, skip
	//if there is a tbOp, check its status
	//if success: set success to itself
	//if cancelled: set error to itself
	//if error:
	//if we can retry it: retry, skip
	//if no more retries: set error to itself
	if len(tbOps) == 0 {
		return RunNewTb, nil
	}
	last := tbOps[len(tbOps)-1]
	switch last.State {
	case types.OperationStateDone:
		return Success, nil
	case types.OperationStateError:
		{
			t := shouldRetry(config, tbOps, clock)
			if t != nil {
				if clock.Now().After(*t) {
					return RunNewTb, nil
				} else {
					return Skip, nil
				}
			}
			//t == nil means "do not retry anymore".
			//if we don't want to retry, and last one
			//is Error, set Error
			return Error, nil
		}
	case types.OperationStateRunning:
		return Skip, nil
	}
	return Error, errors.New("unexpected tb op status")
}

func TBWROperationHandler(
	ctx context.Context,
	operation types.Operation,
	db db.DBConnector,
	clientConn client.ClientConnector,
	s3 config.S3Config,
	clientConfig config.ClientConnectionConfig,
	queryBuilderFactory queries.WriteQueryBuilderFactory,
	clock clockwork.Clock,
	mon metrics.MetricsRegistry,
) error {
	ctx = xlog.With(ctx, zap.String("OperationID", operation.GetID()))

	if operation.GetType() != types.OperationTypeTBWR {
		return fmt.Errorf("wrong operation type %s != %s", operation.GetType(), types.OperationTypeTBWR)
	}
	tbwr, ok := operation.(*types.TakeBackupWithRetryOperation)
	if !ok {
		return fmt.Errorf("can't cast Operation to TakeBackupWithRetryOperation %s", types.OperationToString(operation))
	}

	ops, err := db.SelectOperations(ctx, queries.NewReadTableQuery(
		queries.WithTableName("Operations"),
		queries.WithIndex("idx_p"),
		queries.WithQueryFilters(queries.QueryFilter{
			Field:  "parent_operation_id",
			Values: []table_types.Value{table_types.StringValueFromString(tbwr.ID)},
		}),
		queries.WithOrderBy(queries.OrderSpec{
			Field: "created_at",
		}),
	))
	tbOps := make([]*types.TakeBackupOperation, len(ops))
	for i := range ops {
		tbOps[i] = ops[i].(*types.TakeBackupOperation)
	}
	if err != nil {
		return fmt.Errorf("can't select Operations for TBWR op %s", tbwr.ID)
	}

	switch tbwr.State {
	case types.OperationStateRunning:
		{
			do, err := HandleTbOps(tbwr.RetryConfig, tbOps, clock)
			if err != nil {
				tbwr.State = types.OperationStateError
				tbwr.Message = err.Error()
				now := clock.Now()
				tbwr.UpdatedAt = timestamppb.New(now)
				tbwr.Audit.CompletedAt = timestamppb.New(now)

				errup := db.ExecuteUpsert(ctx, queryBuilderFactory().WithUpdateOperation(tbwr))
				if errup != nil {
					return errup
				}
				return err
			}
			if do != Error {
				xlog.Info(
					ctx,
					"TBWROperationHandler",
					zap.String("decision", do.String()),
				)
			}
			switch do {
			case Success:
				{
					tbwr.State = types.OperationStateDone
					tbwr.Message = "Success"
					now := clock.Now()
					tbwr.UpdatedAt = timestamppb.New(now)
					tbwr.Audit.CompletedAt = timestamppb.New(now)
					return db.ExecuteUpsert(ctx, queryBuilderFactory().WithUpdateOperation(tbwr))
				}
			case Skip:
				return nil
			case Error:
				{
					operationIDs := strings.Join(func() []string {
						var ids []string
						for _, item := range ops {
							ids = append(ids, item.GetID())
						}
						return ids
					}(), ", ")
					tbwr.State = types.OperationStateError
					now := clock.Now()
					tbwr.UpdatedAt = timestamppb.New(now)
					tbwr.Audit.CompletedAt = timestamppb.New(now)

					tbwr.Message = fmt.Sprintf("retry attempts exceeded limit: %d.", len(ops))
					fields := []zap.Field{
						zap.Int("RetriesCount", len(ops)),
					}
					if len(ops) > 0 {
						tbwr.Message = tbwr.Message + fmt.Sprintf(" Launched operations %s", operationIDs)
						fields = append(fields, zap.String("OperationIDs", operationIDs))
					}
					xlog.Error(ctx, "retry attempts exceeded limit for TBWR operation", fields...)
					return db.ExecuteUpsert(ctx, queryBuilderFactory().WithUpdateOperation(tbwr))
				}
			case RunNewTb:
				{
					backup, tb, err := backup_operations.MakeBackup(
						ctx,
						clientConn,
						s3,
						clientConfig.AllowedEndpointDomains,
						clientConfig.AllowInsecureEndpoint,
						backup_operations.FromTBWROperation(tbwr),
						types.OperationCreatorName,
						clock,
					)
					if err != nil {
						return err
					}
					xlog.Debug(ctx, "running new TB", zap.String("TBOperationID", tb.ID))
					return db.ExecuteUpsert(ctx, queryBuilderFactory().WithCreateBackup(*backup).WithCreateOperation(tb))
				}
			default:
				tbwr.State = types.OperationStateError
				tbwr.Message = "unexpected operation state"
				now := clock.Now()
				tbwr.UpdatedAt = timestamppb.New(now)
				tbwr.Audit.CompletedAt = timestamppb.New(now)

				_ = db.ExecuteUpsert(ctx, queryBuilderFactory().WithUpdateOperation(tbwr))
				return errors.New(tbwr.Message)
			}
		}
	case types.OperationStateCancelling:
		//select last tbOp.
		//if has last and not cancelled: set start_cancelling to it, skip
		//if cancelled, set cancelled to itself
		{
			xlog.Info(ctx, "cancelling TBWR operation")
			var last *types.TakeBackupOperation
			if len(tbOps) > 0 {
				last = tbOps[len(tbOps)-1]
			}
			if last == nil || !types.IsActive(last) {
				tbwr.State = types.OperationStateCancelled
				tbwr.Message = "Success"
				now := clock.Now()
				tbwr.UpdatedAt = timestamppb.New(now)
				tbwr.Audit.CompletedAt = timestamppb.New(now)
				return db.ExecuteUpsert(ctx, queryBuilderFactory().WithUpdateOperation(tbwr))
			} else {
				if last.State == types.OperationStatePending || last.State == types.OperationStateRunning {
					xlog.Info(ctx, "cancelling TB operation", zap.String("TBOperationID", last.ID))
					last.State = types.OperationStateStartCancelling
					last.Message = "Cancelling by parent operation"
					last.UpdatedAt = timestamppb.New(clock.Now())
					return db.ExecuteUpsert(ctx, queryBuilderFactory().WithUpdateOperation(last))
				}
			}
		}
	default:
		{
			tbwr.State = types.OperationStateError
			tbwr.Message = "unexpected operation state"
			now := clock.Now()
			tbwr.UpdatedAt = timestamppb.New(now)
			tbwr.Audit.CompletedAt = timestamppb.New(now)
			_ = db.ExecuteUpsert(ctx, queryBuilderFactory().WithUpdateOperation(tbwr))
			return errors.New(tbwr.Message)
		}
	}

	return nil
}
