package schedule_watcher

import (
	"context"
	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
	"sync"
	"time"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/handlers"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
	"ydbcp/internal/watchers"
)

func NewScheduleWatcher(
	ctx context.Context,
	wg *sync.WaitGroup,
	db db.DBConnector,
	handler handlers.BackupScheduleHandlerType,
	options ...watchers.Option,
) *watchers.WatcherImpl {
	return watchers.NewWatcher(
		ctx,
		wg,
		func(ctx context.Context, period time.Duration) {
			ScheduleWatcherAction(ctx, period, db, handler)
		},
		time.Minute,
		"BackupSchedule",
		options...,
	)
}

func ScheduleWatcherAction(
	baseCtx context.Context,
	period time.Duration,
	db db.DBConnector,
	handler handlers.BackupScheduleHandlerType,
) {
	ctx, cancel := context.WithTimeout(baseCtx, period)
	defer cancel()

	schedules, err := db.SelectBackupSchedules(
		//WithRPOInfo
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("BackupSchedules"),
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
		err = handler(ctx, db, *schedule)
		if err != nil {
			xlog.Error(ctx, "error handling backup schedule", zap.String("scheduleID", schedule.ID), zap.Error(err))
		}
	}
}
