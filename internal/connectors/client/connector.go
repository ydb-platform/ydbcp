package client

import (
	"context"
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"
	"ydbcp/internal/config"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Export_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Import_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Operation_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Export"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Import"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"
)

type ClientConnector interface {
	Open(ctx context.Context, dsn string) (*ydb.Driver, error)
	Close(ctx context.Context, clientDb *ydb.Driver) error

	ExportToS3(ctx context.Context, clientDb *ydb.Driver, s3Settings types.ExportSettings) (string, error)
	ImportFromS3(ctx context.Context, clientDb *ydb.Driver, s3Settings types.ImportSettings) (string, error)
	GetOperationStatus(
		ctx context.Context, clientDb *ydb.Driver, operationId string,
	) (*Ydb_Operations.GetOperationResponse, error)
	ForgetOperation(
		ctx context.Context, clientDb *ydb.Driver, operationId string,
	) (*Ydb_Operations.ForgetOperationResponse, error)
	CancelOperation(
		ctx context.Context, clientDb *ydb.Driver, operationId string,
	) (*Ydb_Operations.CancelOperationResponse, error)
}

type ClientYdbConnector struct {
	config config.ClientConnectionConfig
}

func NewClientYdbConnector(config config.ClientConnectionConfig) *ClientYdbConnector {
	return &ClientYdbConnector{
		config: config,
	}
}

func (d *ClientYdbConnector) Open(ctx context.Context, dsn string) (*ydb.Driver, error) {
	xlog.Info(ctx, "Connecting to client db", zap.String("dsn", dsn))

	opts := []ydb.Option{
		ydb.WithDialTimeout(time.Second * time.Duration(d.config.DialTimeoutSeconds)),
	}
	if d.config.Insecure {
		opts = append(opts, ydb.WithTLSSInsecureSkipVerify())
	}
	if !d.config.Discovery {
		opts = append(opts, ydb.WithBalancer(balancers.SingleConn()))
	}
	if len(d.config.OAuth2KeyFile) > 0 {
		opts = append(opts, ydb.WithOauth2TokenExchangeCredentialsFile(d.config.OAuth2KeyFile))
	} else {
		opts = append(opts, ydb.WithAnonymousCredentials())
	}

	db, err := ydb.Open(ctx, dsn, opts...)
	if err != nil {
		return nil, fmt.Errorf("error connecting to client db: %w", err)
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
		return fmt.Errorf("error closing client db driver: %w", err)
	}

	return nil
}

func isSystemDirectory(name string) bool {
	return strings.HasPrefix(name, ".sys") ||
		strings.HasPrefix(name, ".metadata") ||
		strings.HasPrefix(name, "~")
}

func isExportDirectory(fullPath string, database string) bool {
	return strings.HasPrefix(fullPath, path.Join(database, "export"))
}

func listDirectory(ctx context.Context, clientDb *ydb.Driver, initialPath string, exclusions []regexp.Regexp) (
	[]string, error,
) {
	var dir scheme.Directory
	var err error

	err = retry.Retry(
		ctx, func(ctx context.Context) (err error) {
			dir, err = clientDb.Scheme().ListDirectory(ctx, initialPath)
			return err
		}, retry.WithIdempotent(true),
	)

	if err != nil {
		return nil, fmt.Errorf("list directory %s was failed: %v", initialPath, err)
	}

	excluded := func(path string) bool {
		for _, exclusion := range exclusions {
			if exclusion.MatchString(path) {
				xlog.Info(ctx, "Excluded path", zap.String("path", path))
				return true
			}
		}

		return false
	}

	result := make([]string, 0)
	if dir.Entry.IsTable() {
		if !excluded(initialPath) {
			xlog.Info(ctx, "Included path", zap.String("path", initialPath))
			result = append(result, initialPath)
		}
	}

	for _, child := range dir.Children {
		childPath := path.Join(initialPath, child.Name)

		if child.IsDirectory() {
			if isSystemDirectory(child.Name) || isExportDirectory(childPath, clientDb.Scheme().Database()) {
				continue
			}

			var list []string
			list, err = listDirectory(ctx, clientDb, childPath, exclusions)

			if err != nil {
				return nil, err
			}

			result = append(result, list...)
		} else if child.IsTable() {
			if !excluded(childPath) {
				xlog.Info(ctx, "Included path", zap.String("path", childPath))
				result = append(result, childPath)
			}
		}
	}

	return result, nil
}

func prepareItemsForExport(
	ctx context.Context, clientDb *ydb.Driver, s3Settings types.ExportSettings,
) ([]*Ydb_Export.ExportToS3Settings_Item, error) {
	sources := make([]string, 0)
	exclusions := make([]regexp.Regexp, len(s3Settings.SourcePathToExclude))

	for i, excludePath := range s3Settings.SourcePathToExclude {
		reg, err := regexp.Compile(excludePath)
		if err != nil {
			return nil, fmt.Errorf("error compiling exclude path regexp: %s", err.Error())
		}

		exclusions[i] = *reg
	}

	if len(s3Settings.SourcePaths) > 0 {
		for _, sourcePath := range s3Settings.SourcePaths {
			list, err := listDirectory(ctx, clientDb, sourcePath, exclusions)
			if err != nil {
				return nil, err
			}

			sources = append(sources, list...)
		}
	} else {
		// List root directory
		list, err := listDirectory(ctx, clientDb, "/", exclusions)
		if err != nil {
			return nil, err
		}

		sources = append(sources, list...)
	}

	items := make([]*Ydb_Export.ExportToS3Settings_Item, len(sources))

	for i, source := range sources {
		// Destination prefix format: s3_destination_prefix/database_name/timestamp_backup_id/rel_source_path
		destinationPrefix := path.Join(
			s3Settings.DestinationPrefix,
			clientDb.Scheme().Database(),
			time.Now().Format(types.BackupTimestampFormat),
			strings.TrimPrefix(source, clientDb.Scheme().Database()+"/"),
		)

		items[i] = &Ydb_Export.ExportToS3Settings_Item{
			SourcePath:        source,
			DestinationPrefix: destinationPrefix,
		}
	}

	return items, nil
}

func (d *ClientYdbConnector) ExportToS3(
	ctx context.Context, clientDb *ydb.Driver, s3Settings types.ExportSettings,
) (string, error) {
	if clientDb == nil {
		return "", fmt.Errorf("unititialized client db driver")
	}

	items, err := prepareItemsForExport(ctx, clientDb, s3Settings)
	if err != nil {
		return "", fmt.Errorf("error preparing list of items for export: %s", err.Error())
	}

	exportClient := Ydb_Export_V1.NewExportServiceClient(ydb.GRPCConn(clientDb))
	xlog.Info(
		ctx, "Exporting data to s3",
		zap.String("endpoint", s3Settings.Endpoint),
		zap.String("region", s3Settings.Region),
		zap.String("bucket", s3Settings.Bucket),
		zap.String("description", s3Settings.Description),
	)

	response, err := exportClient.ExportToS3(
		ctx,
		&Ydb_Export.ExportToS3Request{
			OperationParams: &Ydb_Operations.OperationParams{
				OperationTimeout: durationpb.New(time.Second),
				CancelAfter:      durationpb.New(time.Second),
			},
			Settings: &Ydb_Export.ExportToS3Settings{
				Endpoint:                 s3Settings.Endpoint,
				Bucket:                   s3Settings.Bucket,
				Region:                   s3Settings.Region,
				AccessKey:                s3Settings.AccessKey,
				SecretKey:                s3Settings.SecretKey,
				Description:              s3Settings.Description,
				NumberOfRetries:          s3Settings.NumberOfRetries,
				Items:                    items,
				DisableVirtualAddressing: s3Settings.S3ForcePathStyle,
			},
		},
	)

	if err != nil {
		return "", fmt.Errorf("error exporting to S3: %w", err)
	}

	if response.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS {
		return "", fmt.Errorf("exporting to S3 was failed: %v", response.GetOperation().GetIssues())
	}

	return response.GetOperation().GetId(), nil
}

func prepareItemsForImport(ctx context.Context, clientDb *ydb.Driver, s3Settings types.ImportSettings) ([]*Ydb_Import.ImportFromS3Settings_Item, error) {
	if len(s3Settings.SourcePaths) == 0 {
		return nil, fmt.Errorf("empty list of source paths for import")
	}

	s := session.Must(session.NewSession())

	s3Client := s3.New(s,
		&aws.Config{
			Region:           &s3Settings.Region,
			Credentials:      credentials.NewStaticCredentials(s3Settings.AccessKey, s3Settings.SecretKey, ""),
			Endpoint:         &s3Settings.Endpoint,
			S3ForcePathStyle: &s3Settings.S3ForcePathStyle,
		},
	)

	items := make([]*Ydb_Import.ImportFromS3Settings_Item, len(s3Settings.SourcePaths))

	for _, sourcePath := range s3Settings.SourcePaths {
		if sourcePath[len(sourcePath)-1] != '/' {
			sourcePath = sourcePath + "/"
		}

		err := s3Client.ListObjectsPages(
			&s3.ListObjectsInput{
				Bucket: &s3Settings.Bucket,
				Prefix: &sourcePath,
			},
			func(p *s3.ListObjectsOutput, last bool) (shouldContinue bool) {
				for _, object := range p.Contents {

					key, found := strings.CutSuffix(*object.Key, "scheme.pb")
					if found {
						items = append(
							items,
							&Ydb_Import.ImportFromS3Settings_Item{
								SourcePrefix: key,
								DestinationPath: path.Join(
									clientDb.Scheme().Database(),
									s3Settings.DestinationPrefix,
									key[len(sourcePath):],
								),
							},
						)
					}
				}

				return true
			},
		)

		if err != nil {
			return nil, err
		}
	}

	return items, nil
}

func (d *ClientYdbConnector) ImportFromS3(ctx context.Context, clientDb *ydb.Driver, s3Settings types.ImportSettings) (string, error) {
	if clientDb == nil {
		return "", fmt.Errorf("unititialized client db driver")
	}

	items, err := prepareItemsForImport(ctx, clientDb, s3Settings)
	if err != nil {
		return "", fmt.Errorf("error preparing list of items for import: %s", err.Error())
	}

	importClient := Ydb_Import_V1.NewImportServiceClient(ydb.GRPCConn(clientDb))
	xlog.Info(
		ctx, "Importing data from s3",
		zap.String("endpoint", s3Settings.Endpoint),
		zap.String("region", s3Settings.Region),
		zap.String("bucket", s3Settings.Bucket),
		zap.String("description", s3Settings.Description),
	)

	response, err := importClient.ImportFromS3(
		ctx,
		&Ydb_Import.ImportFromS3Request{
			OperationParams: &Ydb_Operations.OperationParams{
				OperationTimeout: durationpb.New(time.Second),
				CancelAfter:      durationpb.New(time.Second),
			},
			Settings: &Ydb_Import.ImportFromS3Settings{
				Endpoint:                 s3Settings.Endpoint,
				Bucket:                   s3Settings.Bucket,
				Region:                   s3Settings.Region,
				AccessKey:                s3Settings.AccessKey,
				SecretKey:                s3Settings.SecretKey,
				Description:              s3Settings.Description,
				NumberOfRetries:          s3Settings.NumberOfRetries,
				Items:                    items,
				DisableVirtualAddressing: s3Settings.S3ForcePathStyle,
			},
		},
	)

	if err != nil {
		return "", fmt.Errorf("error importing from s3: %w", err)
	}

	if response.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS {
		return "", fmt.Errorf(
			"importing from s3 was failed: %v",
			response.GetOperation().GetIssues(),
		)
	}

	return response.GetOperation().GetId(), nil
}

func (d *ClientYdbConnector) GetOperationStatus(
	ctx context.Context, clientDb *ydb.Driver, operationId string,
) (*Ydb_Operations.GetOperationResponse, error) {
	if clientDb == nil {
		return nil, fmt.Errorf("unititialized client db driver")
	}

	client := Ydb_Operation_V1.NewOperationServiceClient(ydb.GRPCConn(clientDb))
	xlog.Info(
		ctx, "Requesting operation status",
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

func (d *ClientYdbConnector) ForgetOperation(
	ctx context.Context, clientDb *ydb.Driver, operationId string,
) (*Ydb_Operations.ForgetOperationResponse, error) {
	if clientDb == nil {
		return nil, fmt.Errorf("unititialized client db driver")
	}

	client := Ydb_Operation_V1.NewOperationServiceClient(ydb.GRPCConn(clientDb))
	xlog.Info(
		ctx, "Forgetting operation",
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

func (d *ClientYdbConnector) CancelOperation(
	ctx context.Context, clientDb *ydb.Driver, operationId string,
) (*Ydb_Operations.CancelOperationResponse, error) {
	if clientDb == nil {
		return nil, fmt.Errorf("unititialized client db driver")
	}

	client := Ydb_Operation_V1.NewOperationServiceClient(ydb.GRPCConn(clientDb))
	xlog.Info(
		ctx, "Cancelling operation",
		zap.String("id", operationId),
	)

	response, err := client.CancelOperation(
		ctx,
		&Ydb_Operations.CancelOperationRequest{
			Id: operationId,
		},
	)

	if err != nil {
		return nil, fmt.Errorf("error cancelling operation: %s", err.Error())
	}

	return response, nil
}
