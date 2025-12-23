package types

type YdbConnectionParams struct {
	Endpoint     string
	DatabaseName string
	// TODO: add auth params
}

func MakeYdbConnectionString(params YdbConnectionParams) string {
	return params.Endpoint + params.DatabaseName
}

type S3Settings struct {
	Endpoint         string
	Region           string
	Bucket           string
	AccessKey        string
	SecretKey        string
	S3ForcePathStyle bool
}

type ExportSettings struct {
	S3Settings
	Description         string
	NumberOfRetries     uint32
	RootPath            string
	SourcePaths         []string
	DestinationPrefix   string
	EncryptionAlgorithm string
	EncryptionKey       []byte
}

type ImportSettings struct {
	S3Settings
	Description         string
	NumberOfRetries     uint32
	BackupID            string
	BucketDbRoot        string
	SourcePaths         map[string]bool
	DestinationPath     string
	EncryptionAlgorithm string
	EncryptionKey       []byte
}

type ListObjectsSettings struct {
	S3Settings
	NumberOfRetries     uint32
	Prefix              string
	EncryptionAlgorithm string
	EncryptionKey       []byte
}
