package db_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/ydb-platform/ydbcp/pkg/proto/ydbcp/v1alpha1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"ydbcp/internal/connectors/db"
	"ydbcp/internal/metrics"
	"ydbcp/internal/types"
)

func ptr[T any](v T) *T { return &v }
func backupIDs(bs []*types.Backup) []string {
	ids := make([]string, len(bs))
	for i, b := range bs {
		ids[i] = b.ID
	}
	return ids
}
func operationIDs(ops []types.Operation) []string {
	ids := make([]string, len(ops))
	for i, op := range ops {
		ids[i] = op.GetID()
	}
	return ids
}
func TestMockFiltersOrderAndPagination(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	bs := map[string]types.Backup{}
	for i := 0; i < 5; i++ {
		id := fmt.Sprint(i)
		bs[id] = types.Backup{ID: id, ContainerID: "tenant", DatabaseName: "/db-prod", Status: types.BackupStateAvailable,
			AuditInfo: &pb.AuditInfo{CreatedAt: timestamppb.New(now.Add(time.Duration(i) * time.Hour))}}
	}
	bs["other-tenant"] = types.Backup{ID: "other-tenant", ContainerID: "other", DatabaseName: "/db-prod", Status: types.BackupStateAvailable}
	bs["wrong-status"] = types.Backup{ID: "wrong-status", ContainerID: "tenant", DatabaseName: "/db-prod", Status: types.BackupStateError}
	var store db.DBConnector = db.NewMockDBConnector(db.WithBackups(bs))
	filter := db.BackupFilter{ContainerID: "tenant", DatabaseNameMask: "db-pro_", Statuses: []string{types.BackupStateAvailable},
		CreatedAt: db.TimeRange{From: ptr(now.Add(time.Hour)), To: ptr(now.Add(3 * time.Hour))}, Page: &db.Page{Limit: 1, Offset: 1}}
	got, err := store.ListBackups(context.Background(), filter)
	require.NoError(t, err)
	assert.Equal(t, []string{"2"}, backupIDs(got))
	filter.Page = nil
	got, err = store.ListBackups(context.Background(), filter)
	require.NoError(t, err)
	assert.Equal(t, []string{"3", "2", "1"}, backupIDs(got))
	filter.Order = &db.BackupOrder{Field: db.BackupOrderCreatedAt}
	got, err = store.ListBackups(context.Background(), filter)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3"}, backupIDs(got))
	filter.Page = &db.Page{Offset: ^uint64(0), Limit: 10}
	got, err = store.ListBackups(context.Background(), filter)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestMockChildOperationsFilterAndSort(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ops := map[string]types.Operation{
		"later":     &types.TakeBackupOperation{ID: "later", ParentOperationID: ptr("parent"), State: types.OperationStateDone, Audit: &pb.AuditInfo{CreatedAt: timestamppb.New(now.Add(time.Hour))}},
		"earlier":   &types.TakeBackupOperation{ID: "earlier", ParentOperationID: ptr("parent"), State: types.OperationStateRunning, Audit: &pb.AuditInfo{CreatedAt: timestamppb.New(now)}},
		"unrelated": &types.TakeBackupOperation{ID: "unrelated", ParentOperationID: ptr("other"), State: types.OperationStateDone},
		"parent":    &types.TakeBackupWithRetryOperation{TakeBackupOperation: types.TakeBackupOperation{ID: "parent", State: types.OperationStateRunning}},
	}
	var store db.DBConnector = db.NewMockDBConnector(db.WithOperations(ops))
	children, err := store.ListChildOperations(context.Background(), "parent")
	require.NoError(t, err)
	assert.Equal(t, []string{"earlier", "later"}, operationIDs(children))
	active, err := store.ActiveOperations(context.Background())
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"parent", "earlier"}, operationIDs(active))
}

func TestMockExpiryAndScheduleSummary(t *testing.T) {
	now := time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC)
	created, completed := now.Add(-2*time.Hour), now.Add(-time.Hour)
	bs := map[string]types.Backup{
		"success":  {ID: "success", ScheduleID: ptr("schedule"), Status: types.BackupStateAvailable, ExpireAt: ptr(now.Add(-time.Second)), AuditInfo: &pb.AuditInfo{CreatedAt: timestamppb.New(created), CompletedAt: timestamppb.New(completed)}},
		"failure":  {ID: "failure", ScheduleID: ptr("schedule"), Status: types.BackupStateError, AuditInfo: &pb.AuditInfo{CreatedAt: timestamppb.New(completed), CompletedAt: timestamppb.New(now)}},
		"boundary": {ID: "boundary", ExpireAt: &now},
		"deleted":  {ID: "deleted", Status: types.BackupStateDeleted, ExpireAt: &completed},
		"deleting": {ID: "deleting", Status: types.BackupStateDeleting, ExpireAt: &completed},
	}
	var store db.DBConnector = db.NewMockDBConnector(db.WithBackups(bs), db.WithClock(clockwork.NewFakeClockAt(now)),
		db.WithBackupSchedules(map[string]types.BackupSchedule{"schedule": {ID: "schedule", ContainerID: "tenant", DatabaseName: "db", Status: types.BackupScheduleStateActive}}))
	expired, err := store.ListExpiredBackups(context.Background(), 100)
	require.NoError(t, err)
	assert.Equal(t, []string{"success"}, backupIDs(expired))
	s, err := store.GetScheduleWithBackupInfo(context.Background(), "schedule")
	require.NoError(t, err)
	assert.Equal(t, ptr("failure"), s.LastBackupID)
	assert.Equal(t, ptr(types.BackupStateError), s.LastBackupStatus)
	assert.Equal(t, ptr("success"), s.LastSuccessfulBackupID)
	assert.Equal(t, &created, s.RecoveryPoint)
	plain, err := store.GetSchedule(context.Background(), "schedule")
	require.NoError(t, err)
	assert.Nil(t, plain.RecoveryPoint)
	schedules, err := store.ListSchedules(context.Background(), db.ScheduleFilter{ContainerID: "other"})
	require.NoError(t, err)
	assert.Empty(t, schedules)
}

func TestMockCopiesInputOutputAndAppliesOnlyMutableFields(t *testing.T) {
	metrics.InitializeMockMetricsRegistry()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b := types.Backup{ID: "b", DatabaseName: "db", SourcePaths: []string{"table"}, Size: 123,
		Status: types.BackupStateAvailable, ExpireAt: &now, AuditInfo: &pb.AuditInfo{Creator: "creator", CreatedAt: timestamppb.New(now)}}
	op := &types.TakeBackupOperation{ID: "op", State: types.OperationStateRunning, Message: "keep", SourcePaths: []string{"table"}, Audit: &pb.AuditInfo{Creator: "creator"}}
	input := map[string]types.Backup{"b": b}
	var store db.DBConnector = db.NewMockDBConnector(db.WithBackups(input), db.WithOperations(map[string]types.Operation{"op": op}))
	delete(input, "b")
	b.SourcePaths[0] = "changed"
	op.Audit.Creator = "changed"
	fetched, err := store.GetBackup(context.Background(), "b")
	require.NoError(t, err)
	assert.Equal(t, []string{"table"}, fetched.SourcePaths)
	fetched.SourcePaths[0] = "changed again"
	fetched.AuditInfo.Creator = "changed again"
	require.NoError(t, store.Apply(context.Background(), db.Changes{
		UpdateBackups:    []types.Backup{{ID: "b", Status: types.BackupStateDeleted}},
		UpdateOperations: []types.Operation{&types.TakeBackupOperation{ID: "op", State: types.OperationStateDone, Audit: &pb.AuditInfo{CompletedAt: timestamppb.New(now)}}},
	}))
	fetched, err = store.GetBackup(context.Background(), "b")
	require.NoError(t, err)
	assert.Nil(t, fetched.ExpireAt)
	assert.Equal(t, int64(123), fetched.Size)
	assert.Equal(t, "db", fetched.DatabaseName)
	assert.Equal(t, "creator", fetched.AuditInfo.Creator)
	assert.Equal(t, []string{"table"}, fetched.SourcePaths)
	storedOp, err := store.GetOperation(context.Background(), "op")
	require.NoError(t, err)
	assert.Equal(t, "keep", storedOp.GetMessage())
	assert.Equal(t, types.OperationStateDone, storedOp.GetState())
	assert.Equal(t, "creator", storedOp.GetAudit().Creator)
	assert.Equal(t, now, storedOp.GetAudit().CompletedAt.AsTime())
}

func TestMockApplyFailuresAreAtomic(t *testing.T) {
	for _, injected := range []bool{false, true} {
		t.Run(fmt.Sprint(injected), func(t *testing.T) {
			metrics.InitializeMockMetricsRegistry()
			var opts []db.Option
			if injected {
				opts = append(opts, db.WithApplyError(errors.New("unavailable")))
			}
			var store db.DBConnector = db.NewMockDBConnector(opts...)
			changes := db.Changes{CreateBackups: []types.Backup{{ID: "b"}}, CreateOperations: []types.Operation{&types.TakeBackupOperation{ID: "op"}}}
			if !injected {
				changes.UpdateSchedules = []types.BackupSchedule{{ID: "invalid"}}
			}
			require.Error(t, store.Apply(context.Background(), changes))
			_, err := store.GetBackup(context.Background(), "b")
			assert.ErrorIs(t, err, db.ErrNotFound)
			_, err = store.GetOperation(context.Background(), "op")
			assert.ErrorIs(t, err, db.ErrNotFound)
			assert.Zero(t, metrics.GetMetrics()["operations_started_count"])
		})
	}
}

func TestMockIDsContextAndMissingUpdates(t *testing.T) {
	metrics.InitializeMockMetricsRegistry()
	var store db.DBConnector = db.NewMockDBConnector()
	id, err := store.CreateOperation(context.Background(), &types.TakeBackupOperation{ID: "supplied"})
	require.NoError(t, err)
	assert.Equal(t, "supplied", id)
	generated, err := store.CreateBackup(context.Background(), types.Backup{})
	require.NoError(t, err)
	_, err = types.ParseObjectID(generated)
	require.NoError(t, err)
	require.NoError(t, store.Apply(context.Background(), db.Changes{UpdateBackups: []types.Backup{{ID: "absent"}}}))
	_, err = store.GetBackup(context.Background(), "absent")
	assert.ErrorIs(t, err, db.ErrNotFound)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, store.Apply(ctx, db.Changes{CreateBackups: []types.Backup{{ID: "cancelled"}}}), context.Canceled)
	_, err = store.ListBackups(ctx, db.BackupFilter{})
	assert.ErrorIs(t, err, context.Canceled)
}

func TestMockConcurrentAccess(t *testing.T) {
	metrics.InitializeMockMetricsRegistry()
	var store db.DBConnector = db.NewMockDBConnector()
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprint(i)
			if err := store.Apply(context.Background(), db.Changes{CreateBackups: []types.Backup{{ID: id}}}); err != nil {
				errs <- err
				return
			}
			_, err := store.GetBackup(context.Background(), id)
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	items, err := store.ListBackups(context.Background(), db.BackupFilter{})
	require.NoError(t, err)
	require.Len(t, items, 20)
}

func TestInvalidOperationInputsReturnErrors(t *testing.T) {
	metrics.InitializeMockMetricsRegistry()
	var store db.DBConnector = db.NewMockDBConnector()
	for _, op := range []types.Operation{nil, (*types.TakeBackupOperation)(nil)} {
		_, err := store.CreateOperation(context.Background(), op)
		require.Error(t, err)
		require.Error(t, store.UpdateOperation(context.Background(), op))
	}
	_, err := store.CreateOperation(context.Background(), &types.TakeBackupWithRetryOperation{
		TakeBackupOperation: types.TakeBackupOperation{ID: "invalid-retry"}, RetryConfig: &pb.RetryConfig{},
	})
	require.ErrorContains(t, err, "retry configuration")
	_, err = store.GetOperation(context.Background(), "invalid-retry")
	assert.ErrorIs(t, err, db.ErrNotFound)
}

func TestMockRepeatedCreatesPreserveOptionalColumns(t *testing.T) {
	metrics.InitializeMockMetricsRegistry()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var store db.DBConnector = db.NewMockDBConnector()
	original := types.Backup{ID: "stable", Status: types.BackupStateRunning, ExpireAt: &now, SourcePaths: []string{"table"},
		AuditInfo: &pb.AuditInfo{CreatedAt: timestamppb.New(now)}}
	id, err := store.CreateBackup(context.Background(), original)
	require.NoError(t, err)
	require.Equal(t, original.ID, id)
	_, err = store.CreateBackup(context.Background(), types.Backup{ID: id, Status: types.BackupStateAvailable})
	require.NoError(t, err)
	b, err := store.GetBackup(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, types.BackupStateAvailable, b.Status)
	assert.Equal(t, original.ExpireAt, b.ExpireAt)
	assert.Equal(t, original.SourcePaths, b.SourcePaths)
	assert.Equal(t, original.AuditInfo, b.AuditInfo)
	items, err := store.ListBackups(context.Background(), db.BackupFilter{})
	require.NoError(t, err)
	require.Len(t, items, 1)
}
