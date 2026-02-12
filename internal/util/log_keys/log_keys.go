package log_keys

// Operation-related log keys
const (
	ClientOperationID = "client_operation_id"
	DatabaseEndpoint  = "database_endpoint"
	Limit             = "limit"
	Operation         = "operation"
	OperationMessage  = "operation_message"
	OperationID       = "operation_id"
	OperationIDs      = "operation_ids"
	OperationReason   = "reason"
	OperationState    = "operation_state"
	OperationStatus   = "operation_status"
	OperationType     = "operation_type"
	RetriesCount      = "retries_count"
	Response          = "response"
	RetryDecision     = "retry_decision"
	RunID             = "run_id"
	TBOperationID     = "tb_operation_id"
	YdbOperationID    = "ydb_operation_id"
	YQL               = "yql"
)

// BackupSchedule-related log keys
const (
	BackupSchedule = "backup_schedule"
	ScheduleID     = "schedule_id"
)

// Backup-related log keys
const (
	Backup       = "backup"
	BackupID     = "backup_id"
	BackupStatus = "backup_status"
)

// Configuration-related log keys
const (
	Config     = "config"
	ConfigPath = "config_path"
)

// Watcher-related log keys
const (
	Period = "period"
)

// Signal/System-related log keys
const (
	Signal = "signal"
)

// Proto-related log keys
const (
	Proto                        = "proto"
	TakeBackupWithRetryOperation = "take_backup_with_retry_operation"
)

// Database-related log keys
const (
	ClientDSN = "client_dsn"
	Database  = "database"
	Databases = "databases"
	Endpoint  = "endpoint"
)

// Audit/Request-related log keys
const (
	ContainerID = "container_id"
	Method      = "method"
	RequestID   = "request_id"
	Subject     = "subject"
	TraceID     = "trace_id"
)

// Server/gRPC-related log keys
const (
	Address     = "address"
	GRPCCall    = "grpc_call"
	GRPCMethod  = "grpc_method"
	GRPCRequest = "grpc_request"
	RemoteAddr  = "remote_addr"
	Request     = "request"
)

// Plugin-related log keys
const (
	PluginPath = "plugin_path"
)

// S3-related log keys
const (
	S3Bucket            = "s3_bucket"
	S3Description       = "s3_description"
	S3DestinationPrefix = "s3_destination_prefix"
	S3Endpoint          = "s3_endpoint"
	S3Region            = "s3_region"
	Path                = "path"
)

// Authorization/Check-related log keys
const (
	AuthResults     = "auth_results"
	AuthorizeChecks = "authorize_checks"
	Checks          = "checks"
	Containers      = "containers"
	ResourceID      = "resource_id"
	Resources       = "resources"
	Results         = "results"
	Tokens          = "tokens"
)

// KMS-related log keys
const (
	DEKKey = "dek_key"
	KeyID  = "key_id"
)
