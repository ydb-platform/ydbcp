package db

import (
	"cmp"
	"regexp"
	"slices"
	"strings"
	"time"

	pb "github.com/ydb-platform/ydbcp/pkg/proto/ydbcp/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"ydbcp/internal/types"
)

func matchesOne(value string, allowed []string) bool {
	return len(allowed) == 0 || slices.Contains(allowed, value)
}
func matchesIdentity(container, database, filterContainer, mask string) bool {
	return (filterContainer == "" || container == filterContainer) && matchesMask(database, mask)
}

// DatabaseNameMask retains the existing substring LIKE semantics, including
// '%' and '_' wildcards. Quoting regex characters keeps this matcher literal.
func matchesMask(value, mask string) bool {
	if mask == "" {
		return true
	}
	var pattern strings.Builder
	pattern.WriteString("(?s).*")
	for _, r := range mask {
		switch r {
		case '%':
			pattern.WriteString(".*")
		case '_':
			pattern.WriteString(".")
		default:
			pattern.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	pattern.WriteString(".*")
	return regexp.MustCompile("^" + pattern.String() + "$").MatchString(value)
}
func matchesTime(t time.Time, present bool, r TimeRange) bool {
	if r.From == nil && r.To == nil {
		return true
	}
	return present && (r.From == nil || !t.Before(*r.From)) && (r.To == nil || !t.After(*r.To))
}
func compareNullableTime(a time.Time, hasA bool, b time.Time, hasB bool) int {
	if !hasA && !hasB {
		return 0
	}
	if !hasA {
		return -1
	}
	if !hasB {
		return 1
	}
	return a.Compare(b)
}
func compareTimePointers(a, b *time.Time) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	return a.Compare(*b)
}
func sortOperations(ops []types.Operation, desc bool) {
	slices.SortFunc(ops, func(a, b types.Operation) int {
		n := compareNullableTime(a.GetAudit().GetCreatedAt().AsTime(), a.GetAudit().GetCreatedAt() != nil, b.GetAudit().GetCreatedAt().AsTime(), b.GetAudit().GetCreatedAt() != nil)
		if desc {
			n = -n
		}
		if n == 0 {
			return cmp.Compare(a.GetID(), b.GetID())
		}
		return n
	})
}
func paginate[T any](items []T, p *Page) []T {
	if p == nil {
		return items
	}
	if p.Offset >= uint64(len(items)) {
		return items[:0]
	}
	items = items[p.Offset:]
	if p.Limit > 0 && p.Limit < uint64(len(items)) {
		items = items[:p.Limit]
	}
	return items
}
func clearBackupInfo(s *types.BackupSchedule) {
	s.LastBackupID, s.LastBackupStatus, s.LastSuccessfulBackupID, s.RecoveryPoint = nil, nil, nil, nil
}
func (c *MockDBConnector) addBackupInfo(s *types.BackupSchedule) {
	var last, success *types.Backup
	for _, b := range c.backups {
		if b.ScheduleID == nil || *b.ScheduleID != s.ID || b.AuditInfo.GetCompletedAt() == nil {
			continue
		}
		if last == nil || b.AuditInfo.CompletedAt.AsTime().After(last.AuditInfo.CompletedAt.AsTime()) {
			b := b
			last = &b
		}
		if b.Status == types.BackupStateAvailable && (success == nil || b.AuditInfo.CompletedAt.AsTime().After(success.AuditInfo.CompletedAt.AsTime())) {
			b := b
			success = &b
		}
	}
	if last != nil {
		s.LastBackupID, s.LastBackupStatus = clonePtr(&last.ID), clonePtr(&last.Status)
	}
	if success != nil {
		s.LastSuccessfulBackupID = clonePtr(&success.ID)
		if success.AuditInfo.CreatedAt != nil {
			t := success.AuditInfo.CreatedAt.AsTime()
			s.RecoveryPoint = &t
		}
	}
}

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
func cloneAudit(a *pb.AuditInfo) *pb.AuditInfo {
	if a == nil {
		return nil
	}
	return proto.Clone(a).(*pb.AuditInfo)
}
func cloneBackup(b types.Backup) types.Backup {
	b.AuditInfo = cloneAudit(b.AuditInfo)
	b.ExpireAt, b.ScheduleID = clonePtr(b.ExpireAt), clonePtr(b.ScheduleID)
	b.SourcePaths = slices.Clone(b.SourcePaths)
	if b.EncryptionSettings != nil {
		b.EncryptionSettings = proto.Clone(b.EncryptionSettings).(*pb.EncryptionSettings)
	}
	return b
}
func cloneSchedule(s types.BackupSchedule) types.BackupSchedule {
	s.Audit = cloneAudit(s.Audit)
	s.SourcePaths, s.SourcePathsToExclude = slices.Clone(s.SourcePaths), slices.Clone(s.SourcePathsToExclude)
	s.Name, s.NextLaunch = clonePtr(s.Name), clonePtr(s.NextLaunch)
	s.LastBackupID, s.LastBackupStatus = clonePtr(s.LastBackupID), clonePtr(s.LastBackupStatus)
	s.LastSuccessfulBackupID, s.RecoveryPoint = clonePtr(s.LastSuccessfulBackupID), clonePtr(s.RecoveryPoint)
	if s.ScheduleSettings != nil {
		s.ScheduleSettings = proto.Clone(s.ScheduleSettings).(*pb.BackupScheduleSettings)
	}
	return s
}
func cloneTB(op types.TakeBackupOperation) types.TakeBackupOperation {
	op.Audit = cloneAudit(op.Audit)
	op.UpdatedAt = cloneTimestamp(op.UpdatedAt)
	op.SourcePaths, op.SourcePathsToExclude = slices.Clone(op.SourcePaths), slices.Clone(op.SourcePathsToExclude)
	op.ParentOperationID = clonePtr(op.ParentOperationID)
	if op.EncryptionSettings != nil {
		op.EncryptionSettings = proto.Clone(op.EncryptionSettings).(*pb.EncryptionSettings)
	}
	return op
}
func cloneOperation(op types.Operation) types.Operation {
	if op == nil {
		return nil
	}
	switch src := op.(type) {
	case *types.TakeBackupOperation:
		dst := cloneTB(*src)
		return &dst
	case *types.TakeBackupWithRetryOperation:
		dst := *src
		dst.TakeBackupOperation = cloneTB(src.TakeBackupOperation)
		dst.ScheduleID, dst.Ttl = clonePtr(src.ScheduleID), clonePtr(src.Ttl)
		if src.RetryConfig != nil {
			dst.RetryConfig = proto.Clone(src.RetryConfig).(*pb.RetryConfig)
		}
		return &dst
	case *types.RestoreBackupOperation:
		dst := *src
		dst.Audit, dst.UpdatedAt, dst.SourcePaths = cloneAudit(src.Audit), cloneTimestamp(src.UpdatedAt), slices.Clone(src.SourcePaths)
		return &dst
	case *types.DeleteBackupOperation:
		dst := *src
		dst.Audit, dst.UpdatedAt = cloneAudit(src.Audit), cloneTimestamp(src.UpdatedAt)
		return &dst
	case *types.GenericOperation:
		dst := *src
		dst.UpdatedAt = cloneTimestamp(src.UpdatedAt)
		return &dst
	default:
		return op.Copy()
	}
}
func setOperationAudit(op types.Operation, a *pb.AuditInfo) {
	switch op := op.(type) {
	case *types.TakeBackupOperation:
		op.Audit = a
	case *types.TakeBackupWithRetryOperation:
		op.Audit = a
	case *types.RestoreBackupOperation:
		op.Audit = a
	case *types.DeleteBackupOperation:
		op.Audit = a
	}
}

// These merges model the optional columns omitted by create/upsert queries.
func mergeAudit(old, next *pb.AuditInfo) *pb.AuditInfo {
	if next == nil {
		return cloneAudit(old)
	}
	if old == nil {
		return next
	}
	if next.CreatedAt == nil {
		next.CreatedAt = cloneTimestamp(old.CreatedAt)
	}
	if next.CompletedAt == nil {
		next.CompletedAt = cloneTimestamp(old.CompletedAt)
	}
	return next
}
func mergeCreatedBackup(old, next types.Backup) types.Backup {
	if old.ID == "" {
		return next
	}
	next.AuditInfo = mergeAudit(old.AuditInfo, next.AuditInfo)
	if next.ExpireAt == nil {
		next.ExpireAt = clonePtr(old.ExpireAt)
	}
	if next.ScheduleID == nil {
		next.ScheduleID = clonePtr(old.ScheduleID)
	}
	if len(next.SourcePaths) == 0 {
		next.SourcePaths = slices.Clone(old.SourcePaths)
	}
	if next.EncryptionSettings == nil {
		next.EncryptionSettings = cloneBackup(old).EncryptionSettings
	}
	return next
}
func mergeCreatedSchedule(old, next types.BackupSchedule) types.BackupSchedule {
	clearBackupInfo(&next)
	if old.ID == "" {
		return next
	}
	old = cloneSchedule(old)
	next.Audit = mergeAudit(old.Audit, next.Audit)
	if next.RootPath == "" {
		next.RootPath = old.RootPath
	}
	if len(next.SourcePaths) == 0 {
		next.SourcePaths = old.SourcePaths
	}
	if len(next.SourcePathsToExclude) == 0 {
		next.SourcePathsToExclude = old.SourcePathsToExclude
	}
	if next.Name == nil {
		next.Name = old.Name
	}
	if next.NextLaunch == nil {
		next.NextLaunch = old.NextLaunch
	}
	if old.ScheduleSettings != nil {
		if next.ScheduleSettings.Ttl == nil {
			next.ScheduleSettings.Ttl = old.ScheduleSettings.Ttl
		}
		if next.ScheduleSettings.RecoveryPointObjective == nil {
			next.ScheduleSettings.RecoveryPointObjective = old.ScheduleSettings.RecoveryPointObjective
		}
		if next.ScheduleSettings.EncryptionSettings == nil {
			next.ScheduleSettings.EncryptionSettings = old.ScheduleSettings.EncryptionSettings
		}
	}
	return next
}
func mergeCreatedOperation(old, next types.Operation) types.Operation {
	if old == nil {
		return next
	}
	old = cloneOperation(old)
	setOperationAudit(next, mergeAudit(old.GetAudit(), next.GetAudit()))
	if next.GetUpdatedAt() == nil {
		next.SetUpdatedAt(old.GetUpdatedAt())
	}
	switch n := next.(type) {
	case *types.TakeBackupOperation:
		if o, ok := old.(*types.TakeBackupOperation); ok {
			mergeTB(o, n)
		}
	case *types.TakeBackupWithRetryOperation:
		if o, ok := old.(*types.TakeBackupWithRetryOperation); ok {
			mergeTB(&o.TakeBackupOperation, &n.TakeBackupOperation)
			if n.ScheduleID == nil {
				n.ScheduleID = o.ScheduleID
			}
			if n.Ttl == nil {
				n.Ttl = o.Ttl
			}
			if n.RetryConfig == nil {
				n.RetryConfig = o.RetryConfig
			}
		}
	case *types.RestoreBackupOperation:
		if o, ok := old.(*types.RestoreBackupOperation); ok && len(n.SourcePaths) == 0 {
			n.SourcePaths = o.SourcePaths
		}
	}
	return next
}
func mergeTB(old, next *types.TakeBackupOperation) {
	if next.RootPath == "" {
		next.RootPath = old.RootPath
	}
	if len(next.SourcePaths) == 0 {
		next.SourcePaths = old.SourcePaths
	}
	if len(next.SourcePathsToExclude) == 0 {
		next.SourcePathsToExclude = old.SourcePathsToExclude
	}
	if next.ParentOperationID == nil {
		next.ParentOperationID = old.ParentOperationID
	}
	if next.EncryptionSettings == nil {
		next.EncryptionSettings = old.EncryptionSettings
	}
}

func cloneTimestamp(ts *timestamppb.Timestamp) *timestamppb.Timestamp {
	if ts == nil {
		return nil
	}
	return proto.Clone(ts).(*timestamppb.Timestamp)
}
