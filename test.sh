#!/bin/bash

if [[ -z "$1" ]]; then
  echo "Specify oneof request examples: GetBackup, ListBackups"
fi
doneflag=0

ARGS=()
GRPCURL="grpcurl -plaintext"
if [[ -n $IAM_TOKEN ]]; then
  ARGS+=("-H" "Authorization: Bearer ${IAM_TOKEN}")
fi
if [[ "GetBackup" == "$1" ]]; then
  $GRPCURL "${ARGS[@]}" -d '{"id": "12345678-1234-5678-1234-567812345678"}' localhost:50051 ydbcp.v1alpha1.BackupService.GetBackup
  doneflag=1
fi
if [[ "ListBackups" == "$1" ]]; then
  $GRPCURL "${ARGS[@]}" -d '{"databaseNameMask": "%", "containerId": "'"$CONTAINER_ID"'"}' localhost:50051 ydbcp.v1alpha1.BackupService.ListBackups
  doneflag=1
fi
if [[ "ListOperations" == "$1" ]]; then
  $GRPCURL "${ARGS[@]}" -d '{"databaseNameMask": "%", "containerId": "'"$CONTAINER_ID"'"}' localhost:50051 ydbcp.v1alpha1.OperationService.ListOperations
  doneflag=1
fi
if [[ "MakeBackup" == "$1" ]]; then
  $GRPCURL "${ARGS[@]}" -d '{"database_name": "/testing-global/ydbc", "database_endpoint": "grpcs://localhost:2135", "source_paths": ["/testing-global/ydbc/orders"], "containerId": "'"$CONTAINER_ID"'"}' localhost:50051 ydbcp.v1alpha1.BackupService.MakeBackup
  doneflag=1
fi
if [[ 0 == $doneflag ]]; then
  echo "Failed to parse command; nothing done"
fi
