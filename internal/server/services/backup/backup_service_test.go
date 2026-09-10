package backup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/ydb-platform/ydbcp/pkg/proto/ydbcp/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"ydbcp/internal/auth"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/metrics"
	"ydbcp/internal/types"
)

func TestListBackupsWithMetadataMock(t *testing.T) {
	metrics.InitializeMockMetricsRegistry()
	ctx := context.Background()
	provider, err := auth.NewDummyAuthProvider(ctx)
	require.NoError(t, err)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	items := map[string]types.Backup{}
	for i, id := range []string{"old", "middle", "new"} {
		items[id] = types.Backup{ID: id, ContainerID: "tenant", DatabaseName: "/db-prod",
			Status: types.BackupStateAvailable, AuditInfo: &pb.AuditInfo{CreatedAt: timestamppb.New(now.Add(time.Duration(i) * time.Hour))}}
	}
	items["unrelated"] = types.Backup{ID: "unrelated", ContainerID: "other", Status: types.BackupStateAvailable}
	var store db.DBConnector = db.NewMockDBConnector(db.WithBackups(items))
	service := &BackupService{driver: store, auth: provider}
	request := &pb.ListBackupsRequest{
		ContainerId: "tenant", DatabaseNameMask: "prod", DisplayStatus: []pb.Backup_Status{pb.Backup_AVAILABLE},
		CreatedAt: &pb.DateRange{From: timestamppb.New(now.Add(time.Hour)), To: timestamppb.New(now.Add(2 * time.Hour))},
		PageSize:  1,
	}
	first, err := service.ListBackups(ctx, request)
	require.NoError(t, err)
	require.Len(t, first.Backups, 1)
	assert.Equal(t, "new", first.Backups[0].Id)
	assert.Equal(t, "1", first.NextPageToken)
	request.PageToken = first.NextPageToken
	second, err := service.ListBackups(ctx, request)
	require.NoError(t, err)
	require.Len(t, second.Backups, 1)
	assert.Equal(t, "middle", second.Backups[0].Id)
	request.PageToken = "invalid"
	_, err = service.ListBackups(ctx, request)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestDeleteBackupPersistsOperationAndStatusTogether(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "failed commit"
		}
		t.Run(name, func(t *testing.T) {
			metrics.InitializeMockMetricsRegistry()
			ctx := context.Background()
			provider, err := auth.NewDummyAuthProvider(ctx)
			require.NoError(t, err)
			id := types.GenerateObjectID()
			opts := []db.Option{db.WithBackups(map[string]types.Backup{id: {
				ID: id, ContainerID: "tenant", Status: types.BackupStateAvailable, S3PathPrefix: "path", Size: 1,
			}})}
			if fail {
				opts = append(opts, db.WithApplyError(errors.New("commit failed")))
			}
			var store db.DBConnector = db.NewMockDBConnector(opts...)
			service := &BackupService{driver: store, auth: provider}
			response, err := service.DeleteBackup(ctx, &pb.DeleteBackupRequest{BackupId: id})
			b, readErr := store.GetBackup(ctx, id)
			require.NoError(t, readErr)
			ops, readErr := store.ActiveOperations(ctx)
			require.NoError(t, readErr)
			if fail {
				assert.Equal(t, codes.Internal, status.Code(err))
				assert.Nil(t, response)
				assert.Equal(t, types.BackupStateAvailable, b.Status)
				assert.Empty(t, ops)
				assert.Zero(t, metrics.GetMetrics()["operations_started_count"])
			} else {
				require.NoError(t, err)
				assert.Equal(t, types.BackupStateDeleting, b.Status)
				require.Len(t, ops, 1)
				assert.Equal(t, response.Id, ops[0].GetID())
				assert.Equal(t, id, ops[0].(*types.DeleteBackupOperation).BackupID)
			}
		})
	}
}

func TestBackupNotFoundAndTTLClearWithMetadataMock(t *testing.T) {
	metrics.InitializeMockMetricsRegistry()
	ctx := context.Background()
	provider, err := auth.NewDummyAuthProvider(ctx)
	require.NoError(t, err)
	id := types.GenerateObjectID()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var store db.DBConnector = db.NewMockDBConnector(db.WithBackups(map[string]types.Backup{
		id: {ID: id, ContainerID: "tenant", Status: types.BackupStateAvailable, ExpireAt: &now, Size: 123},
	}))
	service := &BackupService{driver: store, auth: provider, clock: clockwork.NewFakeClockAt(now)}
	_, err = service.GetBackup(ctx, &pb.GetBackupRequest{Id: types.GenerateObjectID()})
	assert.Equal(t, codes.NotFound, status.Code(err))
	response, err := service.UpdateBackupTtl(ctx, &pb.UpdateBackupTtlRequest{BackupId: id})
	require.NoError(t, err)
	assert.Nil(t, response.ExpireAt)
	b, err := store.GetBackup(ctx, id)
	require.NoError(t, err)
	assert.Nil(t, b.ExpireAt)
	assert.Equal(t, int64(123), b.Size)
}
