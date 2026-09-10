package db

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/jonboulle/clockwork"
	pb "github.com/ydb-platform/ydbcp/pkg/proto/ydbcp/v1alpha1"

	"ydbcp/internal/types"
)

// MockDBConnector is an in-memory implementation of the metadata contract.
// Inputs and outputs are copied; modifying a fetched entity does not persist it.
// Apply holds one lock for the entire batch and validates before changing state.
type MockDBConnector struct {
	guard           sync.Mutex
	operations      map[string]types.Operation
	backups         map[string]types.Backup
	backupSchedules map[string]types.BackupSchedule
	clock           clockwork.Clock
	applyError      error
}

var _ DBConnector = (*MockDBConnector)(nil)

type Option func(*MockDBConnector)

func NewMockDBConnector(options ...Option) *MockDBConnector {
	c := &MockDBConnector{
		operations:      make(map[string]types.Operation),
		backups:         make(map[string]types.Backup),
		backupSchedules: make(map[string]types.BackupSchedule),
		clock:           clockwork.NewRealClock(),
	}
	for _, opt := range options {
		opt(c)
	}
	return c
}

func WithOperations(ops map[string]types.Operation) Option {
	return func(c *MockDBConnector) {
		for id, op := range ops {
			c.operations[id] = cloneOperation(op)
		}
	}
}
func WithBackups(backups map[string]types.Backup) Option {
	return func(c *MockDBConnector) {
		for id, b := range backups {
			c.backups[id] = cloneBackup(b)
		}
	}
}
func WithBackupSchedules(schedules map[string]types.BackupSchedule) Option {
	return func(c *MockDBConnector) {
		for id, s := range schedules {
			c.backupSchedules[id] = cloneSchedule(s)
		}
	}
}
func WithClock(clock clockwork.Clock) Option { return func(c *MockDBConnector) { c.clock = clock } }

// WithApplyError simulates a failed transaction; no changes or success metrics
// are recorded. Reads continue to work so callers can inspect the stored state.
func WithApplyError(err error) Option { return func(c *MockDBConnector) { c.applyError = err } }

func (c *MockDBConnector) GetBackup(ctx context.Context, id string) (*types.Backup, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.guard.Lock()
	defer c.guard.Unlock()
	b, ok := c.backups[id]
	if !ok {
		return nil, fmt.Errorf("backup %s: %w", id, ErrNotFound)
	}
	b = cloneBackup(b)
	return &b, nil
}
func (c *MockDBConnector) GetOperation(ctx context.Context, id string) (types.Operation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.guard.Lock()
	defer c.guard.Unlock()
	op, ok := c.operations[id]
	if !ok {
		return nil, fmt.Errorf("operation %s: %w", id, ErrNotFound)
	}
	return cloneOperation(op), nil
}
func (c *MockDBConnector) GetSchedule(ctx context.Context, id string) (*types.BackupSchedule, error) {
	return c.getSchedule(ctx, id, false)
}
func (c *MockDBConnector) GetScheduleWithBackupInfo(ctx context.Context, id string) (*types.BackupSchedule, error) {
	return c.getSchedule(ctx, id, true)
}
func (c *MockDBConnector) getSchedule(ctx context.Context, id string, withInfo bool) (*types.BackupSchedule, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.guard.Lock()
	defer c.guard.Unlock()
	s, ok := c.backupSchedules[id]
	if !ok {
		return nil, fmt.Errorf("schedule %s: %w", id, ErrNotFound)
	}
	s = cloneSchedule(s)
	clearBackupInfo(&s)
	if withInfo {
		c.addBackupInfo(&s)
	}
	return &s, nil
}

func (c *MockDBConnector) ListBackups(ctx context.Context, f BackupFilter) ([]*types.Backup, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := backupOrder(f.Order); err != nil {
		return nil, err
	}
	c.guard.Lock()
	defer c.guard.Unlock()
	var result []*types.Backup
	for _, b := range c.backups {
		if !matchesIdentity(b.ContainerID, b.DatabaseName, f.ContainerID, f.DatabaseNameMask) ||
			!matchesOne(b.Status, f.Statuses) || !matchesTime(b.AuditInfo.GetCreatedAt().AsTime(), b.AuditInfo.GetCreatedAt() != nil, f.CreatedAt) {
			continue
		}
		copy := cloneBackup(b)
		result = append(result, &copy)
	}
	order := BackupOrder{Field: BackupOrderCreatedAt, Desc: true}
	if f.Order != nil {
		order = *f.Order
	}
	slices.SortFunc(result, func(a, b *types.Backup) int {
		var n int
		switch order.Field {
		case BackupOrderCreatedAt:
			n = compareNullableTime(a.AuditInfo.GetCreatedAt().AsTime(), a.AuditInfo.GetCreatedAt() != nil, b.AuditInfo.GetCreatedAt().AsTime(), b.AuditInfo.GetCreatedAt() != nil)
		case BackupOrderDatabaseName:
			n = cmp.Compare(a.DatabaseName, b.DatabaseName)
		case BackupOrderStatus:
			n = cmp.Compare(a.Status, b.Status)
		case BackupOrderExpireAt:
			n = compareTimePointers(a.ExpireAt, b.ExpireAt)
		case BackupOrderCompletedAt:
			n = compareNullableTime(a.AuditInfo.GetCompletedAt().AsTime(), a.AuditInfo.GetCompletedAt() != nil, b.AuditInfo.GetCompletedAt().AsTime(), b.AuditInfo.GetCompletedAt() != nil)
		}
		if order.Desc {
			n = -n
		}
		if n == 0 {
			return cmp.Compare(a.ID, b.ID)
		}
		return n
	})
	return paginate(result, f.Page), nil
}

func (c *MockDBConnector) ListOperations(ctx context.Context, f OperationFilter) ([]types.Operation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.guard.Lock()
	defer c.guard.Unlock()
	var result []types.Operation
	for _, op := range c.operations {
		if !matchesIdentity(op.GetContainerID(), op.GetDatabaseName(), f.ContainerID, f.DatabaseNameMask) ||
			(len(f.Types) > 0 && !slices.Contains(f.Types, op.GetType())) ||
			!matchesTime(op.GetAudit().GetCreatedAt().AsTime(), op.GetAudit().GetCreatedAt() != nil, f.CreatedAt) {
			continue
		}
		result = append(result, cloneOperation(op))
	}
	sortOperations(result, true)
	return paginate(result, f.Page), nil
}
func (c *MockDBConnector) ListChildOperations(ctx context.Context, parentID string) ([]types.Operation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.guard.Lock()
	defer c.guard.Unlock()
	var result []types.Operation
	for _, op := range c.operations {
		tb, ok := op.(*types.TakeBackupOperation)
		if ok && tb.ParentOperationID != nil && *tb.ParentOperationID == parentID {
			result = append(result, cloneOperation(op))
		}
	}
	sortOperations(result, false)
	return result, nil
}
func (c *MockDBConnector) ActiveOperations(ctx context.Context) ([]types.Operation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.guard.Lock()
	defer c.guard.Unlock()
	var result []types.Operation
	for _, op := range c.operations {
		if types.IsActive(op) {
			result = append(result, cloneOperation(op))
		}
	}
	return result, nil
}
func (c *MockDBConnector) ListSchedules(ctx context.Context, f ScheduleFilter) ([]*types.BackupSchedule, error) {
	return c.listSchedules(ctx, f, false)
}
func (c *MockDBConnector) ListSchedulesWithBackupInfo(ctx context.Context, f ScheduleFilter) ([]*types.BackupSchedule, error) {
	return c.listSchedules(ctx, f, true)
}
func (c *MockDBConnector) listSchedules(ctx context.Context, f ScheduleFilter, withInfo bool) ([]*types.BackupSchedule, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.guard.Lock()
	defer c.guard.Unlock()
	var result []*types.BackupSchedule
	for _, s := range c.backupSchedules {
		if !matchesIdentity(s.ContainerID, s.DatabaseName, f.ContainerID, f.DatabaseNameMask) ||
			(f.DatabaseName != "" && s.DatabaseName != f.DatabaseName) || !matchesOne(s.Status, f.Statuses) {
			continue
		}
		s = cloneSchedule(s)
		clearBackupInfo(&s)
		if withInfo {
			c.addBackupInfo(&s)
		}
		result = append(result, &s)
	}
	slices.SortFunc(result, func(a, b *types.BackupSchedule) int {
		n := compareNullableTime(a.Audit.GetCreatedAt().AsTime(), a.Audit.GetCreatedAt() != nil, b.Audit.GetCreatedAt().AsTime(), b.Audit.GetCreatedAt() != nil)
		if n == 0 {
			return cmp.Compare(a.ID, b.ID)
		}
		return -n
	})
	return paginate(result, f.Page), nil
}
func (c *MockDBConnector) ListExpiredBackups(ctx context.Context, limit uint64) ([]*types.Backup, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.guard.Lock()
	defer c.guard.Unlock()
	var result []*types.Backup
	if limit == 0 {
		return result, nil
	}
	for _, b := range c.backups {
		if b.ExpireAt != nil && b.ExpireAt.Before(c.clock.Now()) &&
			b.Status != types.BackupStateDeleted && b.Status != types.BackupStateDeleting {
			b = cloneBackup(b)
			result = append(result, &b)
		}
	}
	// The TTL contract makes no promise about order.
	slices.SortFunc(result, func(a, b *types.Backup) int { return cmp.Compare(a.ID, b.ID) })
	return paginate(result, &Page{Limit: limit}), nil
}

func (c *MockDBConnector) Apply(ctx context.Context, changes Changes) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := changes.validate(); err != nil {
		return err
	}
	if changes.empty() {
		return nil
	}
	c.guard.Lock()
	defer c.guard.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.applyError != nil {
		return c.applyError
	}
	for _, b := range changes.CreateBackups {
		c.backups[b.ID] = mergeCreatedBackup(c.backups[b.ID], cloneBackup(b))
	}
	for _, op := range changes.CreateOperations {
		c.operations[op.GetID()] = mergeCreatedOperation(c.operations[op.GetID()], cloneOperation(op))
	}
	for _, s := range changes.CreateSchedules {
		c.backupSchedules[s.ID] = mergeCreatedSchedule(c.backupSchedules[s.ID], cloneSchedule(s))
	}
	for _, update := range changes.UpdateBackups {
		b, ok := c.backups[update.ID]
		if !ok {
			continue
		} // UPDATE of an absent row does not insert it.
		u := cloneBackup(update)
		b.Status, b.Message, b.ExpireAt = u.Status, u.Message, u.ExpireAt
		if u.Size != 0 {
			b.Size = u.Size
		}
		if u.AuditInfo.GetCompletedAt() != nil {
			if b.AuditInfo == nil {
				b.AuditInfo = &pb.AuditInfo{}
			}
			b.AuditInfo.CompletedAt = u.AuditInfo.CompletedAt
		}
		c.backups[b.ID] = b
	}
	for _, update := range changes.UpdateOperations {
		op, ok := c.operations[update.GetID()]
		if !ok {
			continue
		}
		u := cloneOperation(update)
		op.SetState(u.GetState())
		if u.GetMessage() != "" {
			op.SetMessage(u.GetMessage())
		}
		if u.GetUpdatedAt() != nil {
			op.SetUpdatedAt(u.GetUpdatedAt())
		}
		if u.GetAudit().GetCompletedAt() != nil {
			audit := op.GetAudit()
			if audit == nil {
				audit = &pb.AuditInfo{}
				setOperationAudit(op, audit)
			}
			audit.CompletedAt = u.GetAudit().CompletedAt
		}
		if dst, ok := op.(*types.TakeBackupWithRetryOperation); ok {
			if src, ok := u.(*types.TakeBackupWithRetryOperation); ok {
				dst.Retries = src.Retries
			}
		}
	}
	for _, update := range changes.UpdateSchedules {
		s, ok := c.backupSchedules[update.ID]
		if !ok {
			continue
		}
		u := cloneSchedule(update)
		s.Status, s.SourcePaths, s.SourcePathsToExclude = u.Status, u.SourcePaths, u.SourcePathsToExclude
		if s.ScheduleSettings == nil {
			s.ScheduleSettings = &pb.BackupScheduleSettings{}
		}
		s.ScheduleSettings.SchedulePattern = u.ScheduleSettings.SchedulePattern
		if u.Name != nil {
			s.Name = u.Name
		}
		if u.ScheduleSettings.Ttl != nil {
			s.ScheduleSettings.Ttl = u.ScheduleSettings.Ttl
		}
		if u.ScheduleSettings.RecoveryPointObjective != nil {
			s.ScheduleSettings.RecoveryPointObjective = u.ScheduleSettings.RecoveryPointObjective
		}
		if u.NextLaunch != nil {
			s.NextLaunch = u.NextLaunch
		}
		c.backupSchedules[s.ID] = s
	}
	reportCreatedOperations(changes)
	return nil
}
func (c *MockDBConnector) UpdateOperation(ctx context.Context, op types.Operation) error {
	return updateOperation(ctx, c, op)
}
func (c *MockDBConnector) CreateOperation(ctx context.Context, op types.Operation) (string, error) {
	return createOperation(ctx, c, op)
}
func (c *MockDBConnector) CreateBackup(ctx context.Context, b types.Backup) (string, error) {
	return createBackup(ctx, c, b)
}
