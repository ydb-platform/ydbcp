if [[ -z "$1" ]]; then
  echo "Specify oneof request examples: GetBackup, ListBackups"
fi
doneflag=0
if [[ "GetBackup" == "$1" ]]; then
  grpcurl -plaintext -d '{"id": "12345678-1234-5678-1234-567812345678"}' localhost:50051 ydbcp.BackupService.GetBackup
  doneflag=1
fi
if [[ "ListBackups" == "$1" ]]; then
  grpcurl -plaintext -d '{"databaseNameMask": "", "containerId": ""}' localhost:50051 ydbcp.BackupService.ListBackups
  doneflag=1
fi
if [[ "ListOperations" == "$1" ]]; then
  grpcurl -plaintext -d '{"databaseNameMask": "", "containerId": ""}' localhost:50051 ydbcp.OperationService.ListOperations
  doneflag=1
fi
if [[ 0 == $doneflag ]]; then
  echo "Failed to parse command; nothing done"
fi
