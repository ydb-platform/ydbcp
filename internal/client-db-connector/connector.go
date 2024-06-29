package client_db_connector

import (
	"context"
	"fmt"
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
	"time"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
)

type ClientDbConnector interface {
	ExportToS3(ctx context.Context, settings types.ExportToS3Settings) (string, error)
	ImportFromS3(ctx context.Context, settings types.ImportFromS3Settings) (string, error)
	GetOperationStatus(ctx context.Context, clientDb types.YdbConnectionParams, operationId string) (types.YdbOperationInfo, error)
}

type ClientDbConnectorImpl struct {
}

func (d *ClientDbConnectorImpl) ExportToS3(ctx context.Context, settings types.ExportToS3Settings) (string, error) {
	clientDbConnectionString := settings.ClientDb.Endpoint + settings.ClientDb.DatabaseName

	xlog.Info(ctx, "Connecting to client db", zap.String("dsn", clientDbConnectionString))
	db, connErr := ydb.Open(ctx, clientDbConnectionString, ydb.WithAnonymousCredentials())

	if connErr != nil {
		return "", fmt.Errorf("error connecting to client db: %s", connErr.Error())
	}

	defer func() { _ = db.Close(ctx) }()

	exportClient := Ydb_Export_V1.NewExportServiceClient(ydb.GRPCConn(db))
	xlog.Info(ctx, "Exporting data to s3",
		zap.String("endpoint", settings.S3.Endpoint),
		zap.String("bucket", settings.S3.Bucket),
		zap.String("source", settings.SourcePath),
		zap.String("destination", settings.DestinationPrefix),
	)

	response, exportErr := exportClient.ExportToS3(
		ctx,
		&Ydb_Export.ExportToS3Request{
			OperationParams: &Ydb_Operations.OperationParams{
				OperationTimeout: durationpb.New(time.Second),
				CancelAfter:      durationpb.New(time.Second),
			},
			Settings: &Ydb_Export.ExportToS3Settings{
				Endpoint:        settings.S3.Endpoint,
				Bucket:          settings.S3.Bucket,
				AccessKey:       settings.S3.AccessKey,
				SecretKey:       settings.S3.SecretKey,
				Description:     settings.Description,
				NumberOfRetries: settings.NumberOfRetries,
				Items: []*Ydb_Export.ExportToS3Settings_Item{
					{
						SourcePath:        settings.SourcePath,
						DestinationPrefix: settings.DestinationPrefix,
					},
				},
			},
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

func (d *ClientDbConnectorImpl) ImportFromS3(ctx context.Context, settings types.ImportFromS3Settings) (string, error) {
	clientDbConnectionString := settings.ClientDb.Endpoint + settings.ClientDb.DatabaseName

	xlog.Info(ctx, "Connecting to client db", zap.String("dsn", clientDbConnectionString))
	db, connErr := ydb.Open(ctx, clientDbConnectionString, ydb.WithAnonymousCredentials())

	if connErr != nil {
		return "", fmt.Errorf("error connecting to client db: %s", connErr.Error())
	}

	defer func() { _ = db.Close(ctx) }()

	importClient := Ydb_Import_V1.NewImportServiceClient(ydb.GRPCConn(db))
	xlog.Info(ctx, "Importing data from s3",
		zap.String("endpoint", settings.S3.Endpoint),
		zap.String("bucket", settings.S3.Bucket),
		zap.String("source", settings.SourcePrefix),
		zap.String("destination", settings.DestinationPath),
	)

	response, importErr := importClient.ImportFromS3(
		ctx,
		&Ydb_Import.ImportFromS3Request{
			OperationParams: &Ydb_Operations.OperationParams{
				OperationTimeout: durationpb.New(time.Second),
				CancelAfter:      durationpb.New(time.Second),
			},
			Settings: &Ydb_Import.ImportFromS3Settings{
				Endpoint:        settings.S3.Endpoint,
				Bucket:          settings.S3.Bucket,
				AccessKey:       settings.S3.AccessKey,
				SecretKey:       settings.S3.SecretKey,
				Description:     settings.Description,
				NumberOfRetries: settings.NumberOfRetries,
				Items: []*Ydb_Import.ImportFromS3Settings_Item{
					{
						SourcePrefix:    settings.SourcePrefix,
						DestinationPath: settings.DestinationPath,
					},
				},
			},
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

func (d *ClientDbConnectorImpl) GetOperationStatus(ctx context.Context, clientDb types.YdbConnectionParams, operationId string) (types.YdbOperationInfo, error) {
	clientDbConnectionString := clientDb.Endpoint + clientDb.DatabaseName

	xlog.Info(ctx, "Connecting to client db", zap.String("dsn", clientDbConnectionString))
	db, connErr := ydb.Open(ctx, clientDbConnectionString, ydb.WithAnonymousCredentials())

	if connErr != nil {
		return types.YdbOperationInfo{}, fmt.Errorf("error connecting to client db: %s", connErr.Error())
	}

	defer func() { _ = db.Close(ctx) }()

	client := Ydb_Operation_V1.NewOperationServiceClient(ydb.GRPCConn(db))
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
		return types.YdbOperationInfo{}, fmt.Errorf("error requesting operation status: %s", err.Error())
	}

	return types.YdbOperationInfo{
		Id:     response.GetOperation().GetId(),
		Ready:  response.GetOperation().GetReady(),
		Status: response.GetOperation().GetStatus(),
		Issues: response.GetOperation().GetIssues(),
	}, nil
}
