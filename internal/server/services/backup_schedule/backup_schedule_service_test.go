package backup_schedule

import (
	"context"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/ydb-platform/ydbcp/pkg/proto/ydbcp/v1alpha1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"ydbcp/internal/auth"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/metrics"
	"ydbcp/internal/types"
)

func TestListSchedulesWithMetadataMock(t *testing.T) {
	metrics.InitializeMockMetricsRegistry()
	ctx := context.Background()
	provider, err := auth.NewDummyAuthProvider(ctx)
	require.NoError(t, err)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	id := "schedule"
	schedules := map[string]types.BackupSchedule{
		id: {ID: id, ContainerID: "tenant", DatabaseName: "/db", Status: types.BackupScheduleStateActive,
			ScheduleSettings: &pb.BackupScheduleSettings{SchedulePattern: &pb.BackupSchedulePattern{Crontab: "* * * * *"}}},
		"inactive": {ID: "inactive", ContainerID: "tenant", DatabaseName: "/db", Status: types.BackupScheduleStateInactive},
		"other":    {ID: "other", ContainerID: "other", DatabaseName: "/db", Status: types.BackupScheduleStateActive},
	}
	var store db.DBConnector = db.NewMockDBConnector(db.WithBackupSchedules(schedules), db.WithBackups(map[string]types.Backup{
		"backup": {ID: "backup", ScheduleID: &id, Status: types.BackupStateAvailable,
			AuditInfo: &pb.AuditInfo{CreatedAt: timestamppb.New(now), CompletedAt: timestamppb.New(now.Add(time.Minute))}},
	}))
	service := &BackupScheduleService{driver: store, auth: provider, clock: clockwork.NewFakeClockAt(now)}
	result, err := service.ListBackupSchedules(ctx, &pb.ListBackupSchedulesRequest{
		ContainerId: "tenant", DatabaseNameMask: "db", DisplayStatus: []pb.BackupSchedule_Status{pb.BackupSchedule_ACTIVE},
	})
	require.NoError(t, err)
	require.Len(t, result.Schedules, 1)
	assert.Equal(t, id, result.Schedules[0].Id)
	// Read the same summary through the contract; it is computed from backups.
	s, err := store.GetScheduleWithBackupInfo(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, s.LastSuccessfulBackupID)
	assert.Equal(t, "backup", *s.LastSuccessfulBackupID)
	assert.Equal(t, &now, s.RecoveryPoint)
}
