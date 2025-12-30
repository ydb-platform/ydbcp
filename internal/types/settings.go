package types

type YdbConnectionParams struct {
	Endpoint     string
	DatabaseName string
	// TODO: add auth params
}

func MakeYdbConnectionString(params YdbConnectionParams) string {
	return params.Endpoint + params.DatabaseName
}

type ExportSettings struct {
	Endpoint             string
	Region               string
	Bucket               string
	AccessKey            string
	SecretKey            string
	Description          string
	NumberOfRetries      uint32
	RootPath             string
	SourcePaths          []string
	SourcePathsToExclude []string
	DestinationPrefix    string
	S3ForcePathStyle     bool
	EncryptionAlgorithm  string
	EncryptionKey        []byte
}

type ImportSettings struct {
	Endpoint            string
	Region              string
	Bucket              string
	AccessKey           string
	SecretKey           string
	Description         string
	NumberOfRetries     uint32
	BackupID            string
	BucketDbRoot        string
	SourcePaths         map[string]bool
	S3ForcePathStyle    bool
	DestinationPath     string
	EncryptionAlgorithm string
	EncryptionKey       []byte
}
