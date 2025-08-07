package audit

import (
	"context"
	"go.uber.org/zap"
	"strings"
	"ydbcp/internal/util/xlog"
)

type Action int

const (
	ActionUnspecified Action = iota
	ActionCreate
	ActionUpsert
	ActionUpdate
	ActionGet
	ActionDelete
	ActionSetAccessBindings
	ActionUpdateAccessBindings
	ActionList
	ActionRotate
	ActionScheduleForDeletion
	ActionUndelete
	ActionExchange
	ActionRevoke
	ActionStart
	ActionStop
	ActionLogin
	ActionSendConfirmation
	ActionConfirm
	ActionUnref
	ActionResend
	ActionAccept
	ActionGetOrCreate
	ActionValidate
	ActionEncrypt
	ActionDecrypt
	ActionGenerateDataKey
	ActionGetPublicKey
	ActionSignHash
	ActionGetUsage
	ActionGetByKey
	ActionCreateOrRecreate
)

var methodNameActionIndex = map[string]Action{
	//Backups
	"ListBackups":     ActionList,
	"GetBackup":       ActionGet,
	"MakeBackup":      ActionCreate,
	"DeleteBackup":    ActionDelete,
	"MakeRestore":     ActionCreate,
	"UpdateBackupTTL": ActionUpdate,
	//BackupSchedules
	"CreateBackupSchedule": ActionCreate,
	"UpdateBackupSchedule": ActionUpdate,
	"GetBackupSchedule":    ActionGet,
	"ListBackupSchedules":  ActionList,
	"ToggleBackupSchedule": ActionUpdate,
	"DeleteBackupSchedule": ActionDelete,
	//Operations
	"ListOperations":  ActionList,
	"CancelOperation": ActionUpdate,
	"GetOperation":    ActionGet,
}

type Resource string

var ResourceUnspecified Resource = "Unspecified"
var Backup Resource = "Backup"
var BackupSchedule Resource = "BackupSchedule"
var Operation Resource = "Operation"

var methodNameResourceIndex = map[string]Resource{
	//Backups
	"ListBackups":     Backup,
	"GetBackup":       Backup,
	"MakeBackup":      Backup,
	"DeleteBackup":    Backup,
	"MakeRestore":     Backup,
	"UpdateBackupTTL": Backup,
	//BackupSchedules
	"CreateBackupSchedule": BackupSchedule,
	"UpdateBackupSchedule": BackupSchedule,
	"GetBackupSchedule":    BackupSchedule,
	"ListBackupSchedules":  BackupSchedule,
	"ToggleBackupSchedule": BackupSchedule,
	"DeleteBackupSchedule": BackupSchedule,
	//Operations
	"ListOperations":  Operation,
	"CancelOperation": Operation,
	"GetOperation":    Operation,
}

func ActionFromMethodName(ctx context.Context, name string) Action {
	split := strings.Split(name, "/")
	if v, ok := methodNameActionIndex[split[len(split)-1]]; ok {
		return v
	}
	xlog.Error(ctx, "failed to parse method name", zap.String("method", name))
	return ActionUnspecified
}

func ResourceFromMethodName(ctx context.Context, name string) Resource {
	split := strings.Split(name, "/")
	if v, ok := methodNameResourceIndex[split[len(split)-1]]; ok {
		return v
	}
	xlog.Error(ctx, "failed to parse method name", zap.String("method", name))
	return ResourceUnspecified
}
