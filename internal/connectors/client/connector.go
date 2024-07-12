package client

import (
	"context"
	"fmt"
	"time"
	"ydbcp/internal/util/xlog"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Export_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Import_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Operation_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Export"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Import"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"
)

type ClientConnector interface {
	Open(ctx context.Context, dsn string) (*ydb.Driver, error)
	Close(ctx context.Context, clientDb *ydb.Driver) error

	ExportToS3(ctx context.Context, clientDb *ydb.Driver, s3Settings *Ydb_Export.ExportToS3Settings) (string, error)
	ImportFromS3(ctx context.Context, clientDb *ydb.Driver, s3Settings *Ydb_Import.ImportFromS3Settings) (string, error)
	GetOperationStatus(ctx context.Context, clientDb *ydb.Driver, operationId string) (*Ydb_Operations.GetOperationResponse, error)
	ForgetOperation(ctx context.Context, clientDb *ydb.Driver, operationId string) (*Ydb_Operations.ForgetOperationResponse, error)
}

type ClientYdbConnector struct {
}

func (d *ClientYdbConnector) Open(ctx context.Context, dsn string) (*ydb.Driver, error) {
	xlog.Info(ctx, "Connecting to client db", zap.String("dsn", dsn))
	db, connErr := ydb.Open(ctx, dsn, ydb.WithAnonymousCredentials())

	if connErr != nil {
		return nil, fmt.Errorf("error connecting to client db: %s", connErr.Error())
	}

	return db, nil
}

func (d *ClientYdbConnector) Close(ctx context.Context, clientDb *ydb.Driver) error {
	if clientDb == nil {
		return fmt.Errorf("unititialized client db driver")
	}

	xlog.Info(ctx, "Closing client db driver")
	err := clientDb.Close(ctx)

	if err != nil {
		return fmt.Errorf("error closing client db driver: %s", err.Error())
	}

	return nil
}

func (d *ClientYdbConnector) ExportToS3(ctx context.Context, clientDb *ydb.Driver, s3Settings *Ydb_Export.ExportToS3Settings) (string, error) {
	if clientDb == nil {
		return "", fmt.Errorf("unititialized client db driver")
	}

	exportClient := Ydb_Export_V1.NewExportServiceClient(ydb.GRPCConn(clientDb))
	xlog.Info(ctx, "Exporting data to s3",
		zap.String("endpoint", s3Settings.Endpoint),
		zap.String("bucket", s3Settings.Bucket),
		zap.String("description", s3Settings.Description),
	)

	response, exportErr := exportClient.ExportToS3(
		ctx,
		&Ydb_Export.ExportToS3Request{
			OperationParams: &Ydb_Operations.OperationParams{
				OperationTimeout: durationpb.New(time.Second),
				CancelAfter:      durationpb.New(time.Second),
			},
			Settings: s3Settings,
		},
	)

	if exportErr != nil {
		return "", fmt.Errorf("error exporting to S3: %s", exportErr.Error())
	}

	if response.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS {
		return "", fmt.Errorf("exporting to S3 was failed: %v",
			response.GetOperation().GetIssues())
	}

	return response.GetOperation().GetId(), nil
}

func (d *ClientYdbConnector) ImportFromS3(ctx context.Context, clientDb *ydb.Driver, s3Settings *Ydb_Import.ImportFromS3Settings) (string, error) {
	if clientDb == nil {
		return "", fmt.Errorf("unititialized client db driver")
	}

	importClient := Ydb_Import_V1.NewImportServiceClient(ydb.GRPCConn(clientDb))
	xlog.Info(ctx, "Importing data from s3",
		zap.String("endpoint", s3Settings.Endpoint),
		zap.String("bucket", s3Settings.Bucket),
		zap.String("description", s3Settings.Description),
	)

	response, importErr := importClient.ImportFromS3(
		ctx,
		&Ydb_Import.ImportFromS3Request{
			OperationParams: &Ydb_Operations.OperationParams{
				OperationTimeout: durationpb.New(time.Second),
				CancelAfter:      durationpb.New(time.Second),
			},
			Settings: s3Settings,
		},
	)

	if importErr != nil {
		return "", fmt.Errorf("error importing from s3: %s", importErr.Error())
	}

	if response.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS {
		return "", fmt.Errorf("importing from s3 was failed: %v",
			response.GetOperation().GetIssues(),
		)
	}

	return response.GetOperation().GetId(), nil
}

func (d *ClientYdbConnector) GetOperationStatus(ctx context.Context, clientDb *ydb.Driver, operationId string) (*Ydb_Operations.GetOperationResponse, error) {
	if clientDb == nil {
		return nil, fmt.Errorf("unititialized client db driver")
	}

	client := Ydb_Operation_V1.NewOperationServiceClient(ydb.GRPCConn(clientDb))
	xlog.Info(ctx, "Requesting operation status",
		zap.String("id", operationId),
	)

	response, err := client.GetOperation(
		ctx,
		&Ydb_Operations.GetOperationRequest{
			Id: operationId,
		},
	)

	if err != nil {
		return nil, fmt.Errorf("error requesting operation status: %s", err.Error())
	}

	return response, nil
}

func (d *ClientYdbConnector) ForgetOperation(ctx context.Context, clientDb *ydb.Driver, operationId string) (*Ydb_Operations.ForgetOperationResponse, error) {
	if clientDb == nil {
		return nil, fmt.Errorf("unititialized client db driver")
	}

	client := Ydb_Operation_V1.NewOperationServiceClient(ydb.GRPCConn(clientDb))
	xlog.Info(ctx, "Forgetting operation",
		zap.String("id", operationId),
	)

	response, err := client.ForgetOperation(
		ctx,
		&Ydb_Operations.ForgetOperationRequest{
			Id: operationId,
		},
	)

	if err != nil {
		return nil, fmt.Errorf("error forgetting operation: %s", err.Error())
	}

	return response, nil
}
