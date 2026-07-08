package schedule_watcher

import (
	"context"
	"sync"
	"time"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/handlers"
	"ydbcp/internal/metrics"
	"ydbcp/internal/types"
	"ydbcp/internal/util/log_keys"
	"ydbcp/internal/util/xlog"
	"ydbcp/internal/watchers"

	"github.com/jonboulle/clockwork"
	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
)

func NewScheduleWatcher(
	ctx context.Context,
	wg *sync.WaitGroup,
	cycleSeconds int64,
	db db.DBConnector,
	handler handlers.BackupScheduleHandlerType,
	clock clockwork.Clock,
	options ...watchers.Option,
) *watchers.WatcherImpl {
	seen := make(map[string]*types.BackupSchedule)
	return watchers.NewWatcher(
		ctx,
		wg,
		func(ctx context.Context, period time.Duration) {
			ScheduleWatcherAction(ctx, period, db, handler, clock, seen)
		},
		time.Second*time.Duration(cycleSeconds),
		"BackupSchedule",
		options...,
	)
}

func ScheduleWatcherAction(
	baseCtx context.Context,
	period time.Duration,
	db db.DBConnector,
	handler handlers.BackupScheduleHandlerType,
	clock clockwork.Clock,
	seen map[string]*types.BackupSchedule,
) {
	ctx, cancel := context.WithTimeout(baseCtx, period)
	defer cancel()

	schedules, err := db.SelectBackupSchedulesWithRPOInfo(
		ctx, queries.NewReadTableQuery(
			queries.WithRawQuery(queries.ListSchedulesQuery),
			queries.WithQueryFilters(
				queries.QueryFilter{
					Field: "status",
					Values: []table_types.Value{
						table_types.StringValueFromString(types.BackupScheduleStateActive),
					},
				},
			),
		),
	)

	if err != nil {
		xlog.Error(ctx, "can't select backup schedules", zap.Error(err))
		return
	}

	for _, schedule := range schedules {
		handlerCtx := schedule.SetLogFields(ctx)
		err = handler(handlerCtx, db, schedule)
		metrics.GlobalMetricsRegistry.IncScheduleCounters(handlerCtx, schedule, err)
		reportScheduleBackupMetrics(handlerCtx, db, schedule)
		if err != nil {
			xlog.Error(handlerCtx, "error handling backup schedule", zap.Error(err))
		}
	}

	dropStaleScheduleMetrics(ctx, schedules, seen)
}

// dropStaleScheduleMetrics removes metric series of schedules that were active
// on the previous cycle but are not anymore (deleted or deactivated). Metrics
// are per-process, so this must happen in every replica, not only in the one
// that served the Delete/Toggle API call.
func dropStaleScheduleMetrics(
	ctx context.Context, schedules []*types.BackupSchedule, seen map[string]*types.BackupSchedule,
) {
	current := make(map[string]*types.BackupSchedule, len(schedules))
	for _, schedule := range schedules {
		current[schedule.ID] = schedule
	}
	for id, schedule := range seen {
		if _, ok := current[id]; !ok {
			xlog.Info(
				ctx, "dropping metrics of a no longer active backup schedule",
				zap.String(log_keys.ScheduleID, id),
			)
			metrics.GlobalMetricsRegistry.ResetScheduleCounters(schedule)
		}
	}
	clear(seen)
	for id, schedule := range current {
		seen[id] = schedule
	}
}

func reportScheduleBackupMetrics(ctx context.Context, db db.DBConnector, schedule *types.BackupSchedule) {
	lastBackupEncrypted := false
	if schedule.LastSuccessfulBackupID != nil {
		backups, err := db.SelectBackups(
			ctx, queries.NewReadTableQuery(
				queries.WithTableName("Backups"),
				queries.WithQueryFilters(
					queries.QueryFilter{
						Field:  "id",
						Values: []table_types.Value{table_types.StringValueFromString(*schedule.LastSuccessfulBackupID)},
					},
				),
			),
		)
		if err != nil {
			xlog.Error(ctx, "can't select last successful backup to report size", zap.Error(err))
			metrics.GlobalMetricsRegistry.IncYdbErrorsCounter()
			return
		}
		if len(backups) == 0 {
			xlog.Warn(
				ctx,
				"last successful backup from schedule was not found",
				zap.String(log_keys.BackupID, *schedule.LastSuccessfulBackupID),
			)
			return
		}

		lastBackupEncrypted = backups[0].EncryptionSettings != nil
		metrics.GlobalMetricsRegistry.ReportLastBackupSize(
			schedule.ContainerID,
			schedule.DatabaseName,
			&schedule.ID,
			backups[0].Size,
		)
	}

	metrics.GlobalMetricsRegistry.ReportScheduleBackupsStatus(schedule, lastBackupEncrypted)
}
