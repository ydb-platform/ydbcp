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
) types.OperationHandler {
	return func(ctx context.Context, op types.Operation) error {
		err := TBWROperationHandler(ctx, op, db, client, s3, clientConfig, queryBuilderFactory, clock)
		if err == nil {
			metrics.GlobalMetricsRegistry.ReportOperationMetrics(op)
		}
		return err
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

func shouldRetry(config *pb.RetryConfig, count int, firstStart time.Time, lastEnd *time.Time, clock clockwork.Clock) *time.Time {
	if config == nil {
		if count == 0 {
			t := clock.Now()
			return &t
		}
		return nil
	}
	if count == INTERNAL_MAX_RETRIES {
		return nil
	}

	switch r := config.Retries.(type) {
	case *pb.RetryConfig_Count:
		{
			if r.Count == uint32(count) {
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

	var t time.Time
	if lastEnd != nil {
		t = *lastEnd
		t = t.Add(exp(count) * MIN_BACKOFF)
	} else {
		t = clock.Now()
	}
	return &t
}

func MakeRetryDecision(ctx context.Context, tbwr *types.TakeBackupWithRetryOperation, tbOp *types.TakeBackupOperation, clock clockwork.Clock) (RetryDecision, error) {
	//retrieve last tbOp run time
	//if there is a tbOp, check its status
	//if success: set success to itself
	//if cancelled: set error to itself
	//if error (or no tbOp):
	//if we can retry: retry, skip
	//if no more retries: set error to itself
	lastState := types.OperationStateError //if no tbOp, same logic applies as after error
	var lastTime time.Time
	if tbOp != nil {
		lastState = tbOp.State
		lastTime = tbOp.Audit.CompletedAt.AsTime()
	}
	switch lastState {
	case types.OperationStateDone:
		return Success, nil
	case types.OperationStateError:
		{
			if tbwr.Audit == nil {
				xlog.Error(ctx, "no audit for tbwr operation, unable to calculate retry need")
				return Skip, fmt.Errorf("no audit for OperationID: %s", tbwr.ID)
			}
			t := shouldRetry(tbwr.RetryConfig, tbwr.Retries, tbwr.Audit.CreatedAt.AsTime(), &lastTime, clock)
			if t != nil {
				if clock.Now().Compare(*t) >= 0 {
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

func setErrorToRetryOperation(
	ctx context.Context,
	tbwr *types.TakeBackupWithRetryOperation,
	ops []types.Operation,
	clock clockwork.Clock,
) {
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
	fields := []zap.Field{
		zap.Int("RetriesCount", len(ops)),
	}

	if tbwr.RetryConfig != nil {
		switch tbwr.RetryConfig.Retries.(type) {
		case *pb.RetryConfig_Count:
			{
				tbwr.Message = fmt.Sprintf("retry attempts exceeded limit: %d.", tbwr.Retries)
			}
		case *pb.RetryConfig_MaxBackoff:
			{
				tbwr.Message = fmt.Sprintf("retry attempts exceeded backoff duration.")
			}
		}
	} else {
		tbwr.Message = fmt.Sprint("retry attempts exceeded limit: 1.")
	}

	if len(ops) > 0 {
		tbwr.Message = tbwr.Message + fmt.Sprintf(" Launched operations %s", operationIDs)
		fields = append(fields, zap.String("OperationIDs", operationIDs))
	}

	xlog.Error(ctx, tbwr.Message, fields...)
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

	var lastTbOp *types.TakeBackupOperation
	if len(ops) > 0 {
		lastTbOp = ops[len(ops)-1].(*types.TakeBackupOperation)
	}

	if err != nil {
		return fmt.Errorf("can't select Operations for TBWR op %s", tbwr.ID)
	}

	switch tbwr.State {
	case types.OperationStateRunning:
		{
			do, err := MakeRetryDecision(ctx, tbwr, lastTbOp, clock)
			if err != nil {
				xlog.Error(ctx, "RetryDecision failed", zap.Error(err))
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
			fields := []zap.Field{
				zap.String("decision", do.String()),
			}
			if do != Error && len(ops) > 0 {
				fields = append(fields, zap.String("TBOperationID", ops[len(ops)-1].GetID()))
			}
			xlog.Info(
				ctx,
				"TBWROperationHandler",
				fields...,
			)

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
					setErrorToRetryOperation(ctx, tbwr, ops, clock)
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

					tbwr.IncRetries()

					if err != nil {
						var empty *backup_operations.EmptyDatabaseError

						if errors.As(err, &empty) {
							backup, tb = backup_operations.CreateEmptyBackup(
								backup_operations.FromTBWROperation(tbwr),
								clock,
							)
							xlog.Debug(
								ctx,
								"Created empty backup instance for empty db",
								zap.String("BackupID", backup.ID),
								zap.String("TBOperationID", tb.ID),
							)
							tbwr.State = types.OperationStateDone
							tbwr.Message = "Success"
							now := clock.Now()
							tbwr.UpdatedAt = timestamppb.New(now)
							tbwr.Audit.CompletedAt = timestamppb.New(now)
							return db.ExecuteUpsert(ctx, queryBuilderFactory().WithCreateBackup(*backup).WithCreateOperation(tb).WithUpdateOperation(tbwr))
						} else {
							//increment retries
							return db.ExecuteUpsert(ctx, queryBuilderFactory().WithUpdateOperation(tbwr))
						}
					} else {
						xlog.Debug(ctx, "running new TB", zap.String("TBOperationID", tb.ID))
						return db.ExecuteUpsert(ctx, queryBuilderFactory().WithCreateBackup(*backup).WithCreateOperation(tb).WithUpdateOperation(tbwr))
					}
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
			if lastTbOp == nil || !types.IsActive(lastTbOp) {
				tbwr.State = types.OperationStateCancelled
				tbwr.Message = "Success"
				now := clock.Now()
				tbwr.UpdatedAt = timestamppb.New(now)
				tbwr.Audit.CompletedAt = timestamppb.New(now)
				return db.ExecuteUpsert(ctx, queryBuilderFactory().WithUpdateOperation(tbwr))
			} else {
				if lastTbOp.State == types.OperationStatePending || lastTbOp.State == types.OperationStateRunning {
					xlog.Info(ctx, "cancelling TB operation", zap.String("TBOperationID", lastTbOp.ID))
					lastTbOp.State = types.OperationStateStartCancelling
					lastTbOp.Message = "Cancelling by parent operation"
					lastTbOp.UpdatedAt = timestamppb.New(clock.Now())
					return db.ExecuteUpsert(ctx, queryBuilderFactory().WithUpdateOperation(lastTbOp))
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
