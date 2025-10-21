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
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Export_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Import_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Operation_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Export"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Import"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"
)

type ClientConnector interface {
	Open(ctx context.Context, dsn string) (*ydb.Driver, error)
	Close(ctx context.Context, clientDb *ydb.Driver) error

	PreparePathsForExport(ctx context.Context, clientDb *ydb.Driver, sourcePaths []string, sourcePathsToExclude []string) ([]string, error)
	ExportToS3(ctx context.Context, clientDb *ydb.Driver, s3Settings types.ExportSettings, featureFlags config.FeatureFlagsConfig) (string, error)
	ImportFromS3(ctx context.Context, clientDb *ydb.Driver, s3Settings types.ImportSettings, featureFlags config.FeatureFlagsConfig) (string, error)
	GetOperationStatus(
		ctx context.Context, clientDb *ydb.Driver, operationId string,
	) (*Ydb_Operations.GetOperationResponse, error)
	ForgetOperation(
		ctx context.Context, clientDb *ydb.Driver, operationId string,
	) (*Ydb_Operations.ForgetOperationResponse, error)
	CancelOperation(
		ctx context.Context, clientDb *ydb.Driver, operationId string,
	) (*Ydb_Operations.CancelOperationResponse, error)
	ListExportOperations(
		ctx context.Context, clientDb *ydb.Driver,
	) (*Ydb_Operations.ListOperationsResponse, error)
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
		return nil, fmt.Errorf("error connecting to client db, dsn %s: %w", dsn, err)
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

	excluded := func(p string) bool {
		relPath := strings.TrimPrefix(p, clientDb.Name())
		for _, exclusion := range exclusions {
			if exclusion.MatchString(relPath) {
				xlog.Debug(ctx, "Excluded path", zap.String("path", p))
				return true
			}
		}

		return false
	}

	err := retry.Retry(
		ctx, func(ctx context.Context) (err error) {
			dir, err = clientDb.Scheme().ListDirectory(ctx, initialPath)
			return err
		}, retry.WithIdempotent(true),
	)

	if err != nil {
		return nil, fmt.Errorf("list directory %s was failed: %v", initialPath, err)
	}

	result := make([]string, 0)
	if dir.Entry.IsTable() {
		if !excluded(initialPath) {
			xlog.Debug(ctx, "Included path", zap.String("path", initialPath))
			result = append(result, initialPath)
		}
		return result, nil
	}

	for _, child := range dir.Children {
		childPath := path.Join(initialPath, child.Name)

		if child.IsDirectory() {
			if isSystemDirectory(child.Name) || isExportDirectory(childPath, clientDb.Name()) {
				xlog.Debug(ctx, "System or export directory is skipped", zap.String("path", childPath))
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
				xlog.Debug(ctx, "Included path", zap.String("path", childPath))
				result = append(result, childPath)
			}
		}
	}

	return result, nil
}

func (d *ClientYdbConnector) PreparePathsForExport(
	ctx context.Context, clientDb *ydb.Driver, sourcePaths []string, sourcePathsToExclude []string,
) ([]string, error) {
	if clientDb == nil {
		return nil, fmt.Errorf("unititialized client db driver")
	}

	sources := make([]string, 0)
	exclusions := make([]regexp.Regexp, len(sourcePathsToExclude))

	for i, excludePath := range sourcePathsToExclude {
		reg, err := regexp.Compile(excludePath)
		if err != nil {
			return nil, fmt.Errorf("error compiling exclude path regexp: %s", err.Error())
		}

		exclusions[i] = *reg
	}

	if len(sourcePaths) > 0 {
		for _, sourcePath := range sourcePaths {
			list, err := listDirectory(ctx, clientDb, sourcePath, exclusions)
			if err != nil {
				return nil, err
			}

			sources = append(sources, list...)
		}
	} else {
		// List root directory
		list, err := listDirectory(ctx, clientDb, clientDb.Name(), exclusions)
		if err != nil {
			return nil, err
		}

		sources = append(sources, list...)
	}

	return sources, nil
}

func (d *ClientYdbConnector) ExportToS3(
	ctx context.Context, clientDb *ydb.Driver, s3Settings types.ExportSettings, featureFlags config.FeatureFlagsConfig,
) (string, error) {
	if clientDb == nil {
		return "", fmt.Errorf("unititialized client db driver")
	}

	items := make([]*Ydb_Export.ExportToS3Settings_Item, len(s3Settings.SourcePaths))
	for i, source := range s3Settings.SourcePaths {
		items[i] = &Ydb_Export.ExportToS3Settings_Item{
			SourcePath: source,
		}

		if !featureFlags.EnableNewPathsFormat {
			// Destination prefix format: s3_destination_prefix/rel_source_path
			destinationPrefix := path.Join(
				s3Settings.DestinationPrefix,
				strings.TrimPrefix(source, clientDb.Name()+"/"),
			)

			items[i].DestinationPrefix = destinationPrefix
		}
	}

	exportClient := Ydb_Export_V1.NewExportServiceClient(ydb.GRPCConn(clientDb))
	xlog.Info(
		ctx, "Exporting data to s3",
		zap.String("S3Endpoint", s3Settings.Endpoint),
		zap.String("S3Region", s3Settings.Region),
		zap.String("S3Bucket", s3Settings.Bucket),
		zap.String("S3Description", s3Settings.Description),
	)

	exportRequest := &Ydb_Export.ExportToS3Request{
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
	}

	if featureFlags.EnableNewPathsFormat {
		exportRequest.Settings.SourcePath = clientDb.Name()
		exportRequest.Settings.DestinationPrefix = s3Settings.DestinationPrefix
	}

	response, err := exportClient.ExportToS3(ctx, exportRequest)

	if err != nil {
		return "", fmt.Errorf("error exporting to S3: %w", err)
	}

	if response.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS {
		return "", fmt.Errorf("exporting to S3 was failed: %v", response.GetOperation().GetIssues())
	}

	return response.GetOperation().GetId(), nil
}

type S3API interface {
	ListObjectsPages(input *s3.ListObjectsInput, fn func(*s3.ListObjectsOutput, bool) bool) error
}

func addSlashes(m map[string]bool) map[string]bool {
	res := make(map[string]bool, len(m))
	for el := range m {
		if !strings.HasSuffix(el, "/") {
			res[el+"/"] = true
		} else {
			res[el] = true
		}
	}
	return res
}

func prepareItemsForImport(dbName string, s3Client S3API, s3Settings types.ImportSettings) ([]*Ydb_Import.ImportFromS3Settings_Item, error) {
	backupEverything := len(s3Settings.SourcePaths) == 0

	items := make([]*Ydb_Import.ImportFromS3Settings_Item, 0)
	itemsPtr := &items

	backupRoot := s3Settings.BucketDbRoot
	if !strings.HasSuffix(backupRoot, "/") {
		backupRoot = backupRoot + "/"
	}

	pathPrefixes := addSlashes(s3Settings.SourcePaths)

	err := s3Client.ListObjectsPages(
		&s3.ListObjectsInput{
			Bucket: &s3Settings.Bucket,
			Prefix: &backupRoot,
		},
		func(p *s3.ListObjectsOutput, last bool) (shouldContinue bool) {
			for _, object := range p.Contents {

				key, found := strings.CutSuffix(*object.Key, "scheme.pb")
				if found {
					shouldRestore := backupEverything || pathPrefixes[key]
					if shouldRestore {
						*itemsPtr = append(
							*itemsPtr,
							&Ydb_Import.ImportFromS3Settings_Item{
								Source: &Ydb_Import.ImportFromS3Settings_Item_SourcePrefix{
									SourcePrefix: key,
								},
								DestinationPath: path.Join(
									dbName,
									s3Settings.DestinationPrefix,
									key[len(backupRoot):],
								),
							},
						)
					}
				}
			}

			return true
		},
	)

	if err != nil {
		return nil, err
	}

	if len(*itemsPtr) == 0 {
		return nil, fmt.Errorf("empty list of items for import")
	}

	return *itemsPtr, nil
}

func (d *ClientYdbConnector) ImportFromS3(
	ctx context.Context, clientDb *ydb.Driver, s3Settings types.ImportSettings, featureFlags config.FeatureFlagsConfig,
) (string, error) {
	if clientDb == nil {
		return "", fmt.Errorf("unititialized client db driver")
	}

	var items []*Ydb_Import.ImportFromS3Settings_Item
	if featureFlags.EnableNewPathsFormat {
		items = make([]*Ydb_Import.ImportFromS3Settings_Item, 0, len(s3Settings.SourcePaths))
		for sourcePath, _ := range s3Settings.SourcePaths {
			items = append(items, &Ydb_Import.ImportFromS3Settings_Item{
				Source: &Ydb_Import.ImportFromS3Settings_Item_SourcePath{
					SourcePath: sourcePath[len(s3Settings.BucketDbRoot):],
				},
			})
		}
	} else {
		s := session.Must(session.NewSession())
		s3Client := s3.New(s,
			&aws.Config{
				Region:           &s3Settings.Region,
				Credentials:      credentials.NewStaticCredentials(s3Settings.AccessKey, s3Settings.SecretKey, ""),
				Endpoint:         &s3Settings.Endpoint,
				S3ForcePathStyle: &s3Settings.S3ForcePathStyle,
			},
		)

		var err error
		items, err = prepareItemsForImport(clientDb.Name(), s3Client, s3Settings)
		if err != nil {
			return "", fmt.Errorf("error preparing list of items for import: %s", err.Error())
		}
	}

	importClient := Ydb_Import_V1.NewImportServiceClient(ydb.GRPCConn(clientDb))
	xlog.Info(
		ctx, "Importing data from s3",
		zap.String("S3Endpoint", s3Settings.Endpoint),
		zap.String("S3Region", s3Settings.Region),
		zap.String("S3Bucket", s3Settings.Bucket),
		zap.String("S3Description", s3Settings.Description),
	)

	importRequest := &Ydb_Import.ImportFromS3Request{
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
	}

	if featureFlags.EnableNewPathsFormat {
		importRequest.Settings.SourcePrefix = s3Settings.BucketDbRoot
		importRequest.Settings.DestinationPath = path.Join(clientDb.Name(), s3Settings.DestinationPrefix)
	}

	response, err := importClient.ImportFromS3(ctx, importRequest)

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
		zap.String("ClientOperationID", operationId),
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
		zap.String("ClientOperationID", operationId),
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
		zap.String("ClientOperationID", operationId),
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

func (d *ClientYdbConnector) ListExportOperations(
	ctx context.Context, clientDb *ydb.Driver) (*Ydb_Operations.ListOperationsResponse, error) {
	if clientDb == nil {
		return nil, fmt.Errorf("unititialized client db driver")
	}
	client := Ydb_Operation_V1.NewOperationServiceClient(ydb.GRPCConn(clientDb))
	xlog.Info(ctx, "Listing export operations")
	return client.ListOperations(ctx, &Ydb_Operations.ListOperationsRequest{
		Kind:     "export",
		PageSize: 1,
	})

}
