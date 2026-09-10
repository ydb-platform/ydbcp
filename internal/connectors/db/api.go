package db

import (
	"context"
	"errors"
	"time"

	"ydbcp/internal/types"
)

var ErrNotFound = errors.New("metadata not found")

// DBConnector owns metadata persistence. Its contract contains no database SDK,
// query text, schema names, or transaction handles.
type DBConnector interface {
	GetBackup(context.Context, string) (*types.Backup, error)
	GetOperation(context.Context, string) (types.Operation, error)
	GetSchedule(context.Context, string) (*types.BackupSchedule, error)
	GetScheduleWithBackupInfo(context.Context, string) (*types.BackupSchedule, error)
	ListBackups(context.Context, BackupFilter) ([]*types.Backup, error)
	ListOperations(context.Context, OperationFilter) ([]types.Operation, error)
	ListSchedules(context.Context, ScheduleFilter) ([]*types.BackupSchedule, error)
	ListSchedulesWithBackupInfo(context.Context, ScheduleFilter) ([]*types.BackupSchedule, error)
	ListChildOperations(context.Context, string) ([]types.Operation, error)
	ListExpiredBackups(context.Context, uint64) ([]*types.Backup, error)
	ActiveOperations(context.Context) ([]types.Operation, error)
	Apply(context.Context, Changes) error
	UpdateOperation(context.Context, types.Operation) error
	CreateOperation(context.Context, types.Operation) (string, error)
	CreateBackup(context.Context, types.Backup) (string, error)
}

// Page limits a list after filtering and ordering. Nil means no pagination;
// Limit == 0 means no limit. Public API defaults and tokens belong to services.
type Page struct {
	Limit  uint64
	Offset uint64
}

type BackupOrderField uint8

const (
	BackupOrderCreatedAt BackupOrderField = iota
	BackupOrderDatabaseName
	BackupOrderStatus
	BackupOrderExpireAt
	BackupOrderCompletedAt
)

type BackupOrder struct {
	Field BackupOrderField
	Desc  bool
}

// TimeRange includes both endpoints. A nil endpoint is unbounded.
type TimeRange struct {
	From *time.Time
	To   *time.Time
}

type BackupFilter struct {
	ContainerID      string
	DatabaseNameMask string
	Statuses         []string
	CreatedAt        TimeRange
	Order            *BackupOrder
	Page             *Page
}

type OperationFilter struct {
	ContainerID      string
	DatabaseNameMask string
	Types            []types.OperationType
	CreatedAt        TimeRange
	Page             *Page
}

type ScheduleFilter struct {
	ContainerID      string
	DatabaseName     string
	DatabaseNameMask string
	Statuses         []string
	Page             *Page
}

// Changes is applied atomically, preserving supplied IDs and timestamps.
// Creates retain the existing upsert semantics (a repeated ID replaces supplied
// fields). Updates affect only mutable state, not entity identity or settings
// that are immutable in the API. An empty Changes is a no-op.
//
// Operation updates write status and retries; a nonempty message and nonnil
// completion/update timestamps are written when supplied. Backup updates write
// status, message and expiry (nil clears expiry); nonzero size and nonnil
// completion timestamps are written when supplied. Schedule updates write
// status, cron and paths (empty clears paths), and nonnil name, TTL, RPO and
// next-launch values. These rules are shared by YDB and the in-memory mock.
type Changes struct {
	CreateBackups    []types.Backup
	UpdateBackups    []types.Backup
	CreateOperations []types.Operation
	UpdateOperations []types.Operation
	CreateSchedules  []types.BackupSchedule
	UpdateSchedules  []types.BackupSchedule
}
