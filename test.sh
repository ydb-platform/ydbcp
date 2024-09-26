#!/bin/bash


if [[ -z "$2" ]]; then
  YDBCP_ENDPOINT="localhost:50051"
  YDB_ENDPOINT="localhost:2135"
  YDB_DBNAME="/local"
fi

if [[ "docker" == "$2" ]]; then
  YDBCP_ENDPOINT="localhost:50051"
  YDB_ENDPOINT="grpcs://local-ydb:2135"
  YDB_DBNAME="/local"
fi

if [[ "dev" == "$2" ]]; then
  YDBCP_ENDPOINT="ydbcp.ydb.ydb-dev.ik8s.man.nbhost.net:443"
  YDB_ENDPOINT="grpcs://ydbcp-grpc.ydb-dev.svc.cluster.local:2135"
  YDB_DBNAME="/dev/ydbcp"
fi

if [[ "beta" == "$2" ]]; then
  YDBCP_ENDPOINT="ydbcp.ydb.ik8s-beta.ik8s.man.nbhost.net:443"
  YDB_ENDPOINT="grpcs://ydbc-grpc.ydb-global.svc.cluster.local:2135"
  YDB_DBNAME="/testing-global/ydbc"
fi

if [[ -z "$1" ]]; then
  echo "Specify oneof request examples: GetBackup, ListBackups, ListOperations, MakeBackup, CreateBackupSchedule, ListBackupSchedules"
fi
doneflag=0

INSECURE=${INSECURE-false}
TLS=${TLS-false}

ARGS=()
GRPCURL="grpcurl"
if [[ -n $IAM_TOKEN ]]; then
  ARGS+=("-H" "Authorization: Bearer ${IAM_TOKEN}")
fi
if [[ $INSECURE != "false" ]]; then
  ARGS+=("-insecure")
fi
if [[ $TLS == "false" ]]; then
  ARGS+=("-plaintext")
fi

if [[ "GetBackup" == "$1" ]]; then
  $GRPCURL "${ARGS[@]}" -d '{"id": "12345678-1234-5678-1234-567812345678"}' ${YDBCP_ENDPOINT} ydbcp.v1alpha1.BackupService.GetBackup
  doneflag=1
fi
if [[ "ListBackups" == "$1" ]]; then
  $GRPCURL "${ARGS[@]}" -d '{"databaseNameMask": "%", "containerId": "'"$CONTAINER_ID"'"}' ${YDBCP_ENDPOINT} ydbcp.v1alpha1.BackupService.ListBackups
  doneflag=1
fi
if [[ "ListOperations" == "$1" ]]; then
  $GRPCURL "${ARGS[@]}" -d '{"databaseNameMask": "%", "containerId": "'"$CONTAINER_ID"'"}' ${YDBCP_ENDPOINT} ydbcp.v1alpha1.OperationService.ListOperations
  doneflag=1
fi
if [[ "MakeBackup" == "$1" ]]; then
  doneflag=1
  $GRPCURL "${ARGS[@]}" -d '{"database_name": "'"${YDB_DBNAME}"'", "database_endpoint": "'"${YDB_ENDPOINT}"'", "containerId": "'"$CONTAINER_ID"'"}' ${YDBCP_ENDPOINT} ydbcp.v1alpha1.BackupService.MakeBackup
fi
if [[ "CreateBackupSchedule" == "$1" ]]; then
  $GRPCURL "${ARGS[@]}" -d '{"database_name": "'"${YDB_DBNAME}"'", "endpoint": "'"${YDB_ENDPOINT}"'", "containerId": "'"$CONTAINER_ID"'", "schedule_settings": {"schedule_pattern": {"crontab": "* * * * *"}}}' ${YDBCP_ENDPOINT} ydbcp.v1alpha1.BackupScheduleService.CreateBackupSchedule
  doneflag=1
fi

if [[ "ListBackupSchedules" == "$1" ]]; then
  $GRPCURL "${ARGS[@]}" -d '{"databaseNameMask": "%", "containerId": "'"$CONTAINER_ID"'"}' ${YDBCP_ENDPOINT} ydbcp.v1alpha1.BackupScheduleService.ListBackupSchedules
  doneflag=1
fi

if [[ "GetBackupSchedule" == "$1" ]]; then
  $GRPCURL "${ARGS[@]}" -d '{"id": "'"${SCHEDULE_ID}"'"}' ${YDBCP_ENDPOINT} ydbcp.v1alpha1.BackupScheduleService.GetBackupSchedule
  doneflag=1
fi


if [[ 0 == $doneflag ]]; then
  echo "Failed to parse command; nothing done"
fi
