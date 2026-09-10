package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	yt "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"google.golang.org/protobuf/types/known/timestamppb"

	"ydbcp/internal/metrics"
	"ydbcp/internal/types"
)

type sessionStub struct {
	query.Session
	sql       string
	opts      []query.ExecuteOption
	execs     int
	queryFunc func() (query.Result, error)
	execErr   error
}

func (s *sessionStub) Query(_ context.Context, sql string, opts ...query.ExecuteOption) (query.Result, error) {
	s.sql, s.opts = sql, opts
	return s.queryFunc()
}
func (s *sessionStub) Exec(_ context.Context, sql string, opts ...query.ExecuteOption) error {
	s.sql, s.opts = sql, opts
	s.execs++
	return s.execErr
}

type clientStub struct {
	session *sessionStub
	run     func(context.Context, query.Operation) error
}

func (c *clientStub) Do(ctx context.Context, op query.Operation, _ ...query.DoOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.run != nil {
		return c.run(ctx, op)
	}
	return op(ctx, c.session)
}

type resultStub struct {
	query.Result
	sets     []query.ResultSet
	closed   bool
	closeErr error
	nextErr  error
}

func (r *resultStub) NextResultSet(context.Context) (query.ResultSet, error) {
	if len(r.sets) == 0 {
		if r.nextErr != nil {
			return nil, r.nextErr
		}
		return nil, io.EOF
	}
	s := r.sets[0]
	r.sets = r.sets[1:]
	return s, nil
}
func (r *resultStub) Close(context.Context) error { r.closed = true; return r.closeErr }

type setStub struct {
	query.ResultSet
	rows []query.Row
	err  error
}

func (s *setStub) NextRow(context.Context) (query.Row, error) {
	if len(s.rows) == 0 {
		if s.err != nil {
			return nil, s.err
		}
		return nil, io.EOF
	}
	r := s.rows[0]
	s.rows = s.rows[1:]
	return r, nil
}

type rowStub struct {
	query.Row
	values map[string]any
	err    error
}

func (r rowStub) ScanNamed(destinations ...query.NamedDestination) error {
	if r.err != nil {
		return r.err
	}
	for _, dst := range destinations {
		ref := reflect.ValueOf(dst.Ref()).Elem()
		value := r.values[dst.Name()]
		if value == nil {
			ref.SetZero()
			continue
		}
		v := reflect.ValueOf(value)
		if v.Type().AssignableTo(ref.Type()) {
			ref.Set(v)
			continue
		}
		if ref.Kind() == reflect.Pointer && v.Type().AssignableTo(ref.Type().Elem()) {
			ref.Set(reflect.New(ref.Type().Elem()))
			ref.Elem().Set(v)
			continue
		}
		return fmt.Errorf("bad test value %T for %s", value, dst.Name())
	}
	return nil
}
func resultWithRows(rows ...query.Row) *resultStub {
	return &resultStub{sets: []query.ResultSet{&setStub{rows: rows}}}
}
func connectorWithResult(result *resultStub) (*YdbConnector, *sessionStub) {
	s := &sessionStub{queryFunc: func() (query.Result, error) { return result, nil }}
	return &YdbConnector{client: &clientStub{session: s}}, s
}

func TestYDBListBackupsBindsDomainFiltersAndDecodesRows(t *testing.T) {
	metrics.InitializeMockMetricsRegistry()
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	result := resultWithRows(rowStub{values: map[string]any{
		"id": "backup", "container_id": "tenant", "database": "/db", "endpoint": "endpoint",
		"status": types.BackupStateAvailable, "created_at": from, "expire_at": to, "size": int64(123),
	}})
	d, session := connectorWithResult(result)
	backups, err := d.ListBackups(context.Background(), BackupFilter{
		ContainerID: "tenant", DatabaseNameMask: "db' OR true", Statuses: []string{types.BackupStateAvailable},
		CreatedAt: TimeRange{From: &from, To: &to}, Page: &Page{Limit: 2, Offset: 3},
		Order: &BackupOrder{Field: BackupOrderExpireAt, Desc: true},
	})
	require.NoError(t, err)
	require.Len(t, backups, 1)
	assert.Equal(t, "backup", backups[0].ID)
	assert.Equal(t, int64(123), backups[0].Size)
	assert.Equal(t, &to, backups[0].ExpireAt)
	assert.Equal(t, from, backups[0].AuditInfo.CreatedAt.AsTime())
	assert.True(t, result.closed)
	assert.Contains(t, session.sql, "database LIKE")
	assert.Contains(t, session.sql, "created_at >= $param2")
	assert.Contains(t, session.sql, "created_at <= $param3")
	assert.Contains(t, session.sql, "ORDER BY expire_at DESC LIMIT 2 OFFSET 3")
	assert.NotContains(t, session.sql, "db' OR true")
	assert.Equal(t, []query.ExecuteOption{
		query.WithParameters(table.NewQueryParameters(
			table.ValueParam("$param0", yt.StringValueFromString("tenant")),
			table.ValueParam("$param1", yt.StringValueFromString("db' OR true")),
			table.ValueParam("$param2", yt.TimestampValueFromTime(from)),
			table.ValueParam("$param3", yt.TimestampValueFromTime(to)),
			table.ValueParam("$param4", yt.StringValueFromString(types.BackupStateAvailable)),
		)),
		query.WithTxControl(readTx),
	}, session.opts)
}

func TestYDBGetNotFoundAndReadFailures(t *testing.T) {
	failure := errors.New("read failed")
	for _, tt := range []struct {
		name   string
		result *resultStub
		want   error
	}{
		{"not found", resultWithRows(), ErrNotFound},
		{"empty stream", &resultStub{}, ErrNotFound},
		{"scan", resultWithRows(rowStub{err: failure}), failure},
		{"row stream", &resultStub{sets: []query.ResultSet{&setStub{err: failure}}}, failure},
		{"result stream", &resultStub{nextErr: failure}, failure},
		{"close", &resultStub{sets: []query.ResultSet{&setStub{}}, closeErr: failure}, failure},
	} {
		t.Run(tt.name, func(t *testing.T) {
			metrics.InitializeMockMetricsRegistry()
			d, _ := connectorWithResult(tt.result)
			b, err := d.GetBackup(context.Background(), "missing")
			require.ErrorIs(t, err, tt.want)
			assert.Nil(t, b)
			assert.True(t, tt.result.closed)
		})
	}
	t.Run("query error", func(t *testing.T) {
		metrics.InitializeMockMetricsRegistry()
		d := &YdbConnector{client: &clientStub{session: &sessionStub{queryFunc: func() (query.Result, error) { return nil, failure }}}}
		_, err := d.GetOperation(context.Background(), "id")
		require.ErrorIs(t, err, failure)
	})
	for _, sets := range []int{2} {
		t.Run(fmt.Sprintf("%d result sets", sets), func(t *testing.T) {
			metrics.InitializeMockMetricsRegistry()
			r := &resultStub{}
			for i := 0; i < sets; i++ {
				r.sets = append(r.sets, &setStub{})
			}
			d, _ := connectorWithResult(r)
			_, err := d.GetBackup(context.Background(), "id")
			require.ErrorContains(t, err, "expected 1 result set")
		})
	}
}

func TestYDBReadRetryDoesNotDuplicateRows(t *testing.T) {
	metrics.InitializeMockMetricsRegistry()
	transient := errors.New("interrupted stream")
	row := rowStub{values: map[string]any{"id": "backup"}}
	first := &resultStub{sets: []query.ResultSet{&setStub{rows: []query.Row{row}, err: transient}}}
	second := resultWithRows(row)
	results := []*resultStub{first, second}
	s := &sessionStub{queryFunc: func() (query.Result, error) { r := results[0]; results = results[1:]; return r, nil }}
	d := &YdbConnector{client: &clientStub{run: func(ctx context.Context, op query.Operation) error {
		require.ErrorIs(t, op(ctx, s), transient)
		return op(ctx, s)
	}}}
	items, err := d.ListBackups(context.Background(), BackupFilter{})
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.True(t, first.closed && second.closed)
}

func TestYDBSpecializedReads(t *testing.T) {
	metrics.InitializeMockMetricsRegistry()
	t.Run("children", func(t *testing.T) {
		d, s := connectorWithResult(resultWithRows())
		_, err := d.ListChildOperations(context.Background(), "parent")
		require.NoError(t, err)
		assert.Contains(t, s.sql, "Operations VIEW idx_p")
		assert.Contains(t, s.sql, "parent_operation_id = $param0")
		assert.Contains(t, s.sql, "ORDER BY created_at")
		assert.NotContains(t, s.sql, "DESC")
	})
	t.Run("expiry", func(t *testing.T) {
		d, s := connectorWithResult(resultWithRows())
		_, err := d.ListExpiredBackups(context.Background(), 7)
		require.NoError(t, err)
		assert.Contains(t, s.sql, "VIEW idx_expire_at")
		assert.Contains(t, s.sql, "CurrentUtcTimestamp()")
		assert.True(t, strings.HasSuffix(s.sql, "LIMIT 7"))
		assert.Equal(t, 1, strings.Count(s.sql, "LIMIT"))
	})
	t.Run("schedule summary", func(t *testing.T) {
		now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		d, s := connectorWithResult(resultWithRows(rowStub{values: map[string]any{
			"id": "schedule", "crontab": "* * * * *", "last_backup_id": "failed",
			"last_backup_status": types.BackupStateError, "last_successful_backup_id": "successful", "recovery_point": now,
		}}))
		schedule, err := d.GetScheduleWithBackupInfo(context.Background(), "schedule")
		require.NoError(t, err)
		assert.Equal(t, &now, schedule.RecoveryPoint)
		require.NotNil(t, schedule.LastSuccessfulBackupID)
		assert.Equal(t, "successful", *schedule.LastSuccessfulBackupID)
		assert.Contains(t, s.sql, "MAX_BY")
		assert.Equal(t, query.WithParameters(table.NewQueryParameters(table.ValueParam("$schedule_id", yt.StringValueFromString("schedule")))), s.opts[0])
	})
}

func TestYDBApplyKeepsChangesInOneTransaction(t *testing.T) {
	metrics.InitializeMockMetricsRegistry()
	s := &sessionStub{}
	d := &YdbConnector{client: &clientStub{session: s}}
	parent := &types.TakeBackupWithRetryOperation{TakeBackupOperation: types.TakeBackupOperation{ID: "parent", State: types.OperationStateRunning}, Retries: 2}
	attempt := &types.TakeBackupOperation{ID: "attempt", BackupID: "backup", ParentOperationID: &parent.ID}
	err := d.Apply(context.Background(), Changes{
		CreateBackups:    []types.Backup{{ID: "backup", Status: types.BackupStateRunning}},
		CreateOperations: []types.Operation{attempt},
		UpdateOperations: []types.Operation{parent},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, s.execs)
	assert.Contains(t, s.sql, "UPSERT INTO Backups")
	assert.Contains(t, s.sql, "UPSERT INTO Operations")
	assert.Contains(t, s.sql, "UPDATE Operations")
	assert.Equal(t, query.WithTxControl(writeTx), s.opts[1])
	assert.Equal(t, float64(1), metrics.GetMetrics()["operations_started_count"])
	assert.Equal(t, "attempt", attempt.ID)
}

func TestYDBApplyFailureValidationAndRetries(t *testing.T) {
	t.Run("failure", func(t *testing.T) {
		metrics.InitializeMockMetricsRegistry()
		failure := errors.New("commit failed")
		s := &sessionStub{execErr: failure}
		d := &YdbConnector{client: &clientStub{session: s}}
		err := d.Apply(context.Background(), Changes{CreateOperations: []types.Operation{&types.TakeBackupOperation{ID: "id"}}})
		require.ErrorIs(t, err, failure)
		assert.Zero(t, metrics.GetMetrics()["operations_started_count"])
	})
	t.Run("validation before execution", func(t *testing.T) {
		metrics.InitializeMockMetricsRegistry()
		d := &YdbConnector{} // Any attempted SDK call would panic.
		require.Error(t, d.Apply(context.Background(), Changes{CreateBackups: []types.Backup{{ID: "ok"}}, UpdateSchedules: []types.BackupSchedule{{ID: "bad"}}}))
		require.NoError(t, d.Apply(context.Background(), Changes{}))
		require.Error(t, d.Apply(context.Background(), Changes{CreateOperations: []types.Operation{(*types.TakeBackupOperation)(nil)}}))
	})
	t.Run("retry counts creation only after success", func(t *testing.T) {
		metrics.InitializeMockMetricsRegistry()
		s := &sessionStub{execErr: errors.New("retry")}
		d := &YdbConnector{client: &clientStub{run: func(ctx context.Context, op query.Operation) error {
			require.Error(t, op(ctx, s))
			s.execErr = nil
			return op(ctx, s)
		}}}
		_, err := d.CreateOperation(context.Background(), &types.TakeBackupOperation{ID: "stable-id", UpdatedAt: timestamppb.Now()})
		require.NoError(t, err)
		assert.Equal(t, 2, s.execs)
		assert.Equal(t, float64(1), metrics.GetMetrics()["operations_started_count"])
	})
}
