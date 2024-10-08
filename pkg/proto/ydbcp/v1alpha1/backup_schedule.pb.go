// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.34.1
// 	protoc        v4.25.1
// source: ydbcp/v1alpha1/backup_schedule.proto

package ydbcp

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type BackupSchedule_Status int32

const (
	BackupSchedule_STATUS_UNSPECIFIED BackupSchedule_Status = 0
	BackupSchedule_ACTIVE             BackupSchedule_Status = 1
	BackupSchedule_INACTIVE           BackupSchedule_Status = 2
	BackupSchedule_DELETED            BackupSchedule_Status = 3
)

// Enum value maps for BackupSchedule_Status.
var (
	BackupSchedule_Status_name = map[int32]string{
		0: "STATUS_UNSPECIFIED",
		1: "ACTIVE",
		2: "INACTIVE",
		3: "DELETED",
	}
	BackupSchedule_Status_value = map[string]int32{
		"STATUS_UNSPECIFIED": 0,
		"ACTIVE":             1,
		"INACTIVE":           2,
		"DELETED":            3,
	}
)

func (x BackupSchedule_Status) Enum() *BackupSchedule_Status {
	p := new(BackupSchedule_Status)
	*p = x
	return p
}

func (x BackupSchedule_Status) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (BackupSchedule_Status) Descriptor() protoreflect.EnumDescriptor {
	return file_ydbcp_v1alpha1_backup_schedule_proto_enumTypes[0].Descriptor()
}

func (BackupSchedule_Status) Type() protoreflect.EnumType {
	return &file_ydbcp_v1alpha1_backup_schedule_proto_enumTypes[0]
}

func (x BackupSchedule_Status) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use BackupSchedule_Status.Descriptor instead.
func (BackupSchedule_Status) EnumDescriptor() ([]byte, []int) {
	return file_ydbcp_v1alpha1_backup_schedule_proto_rawDescGZIP(), []int{3, 0}
}

type BackupSchedulePattern struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Crontab string `protobuf:"bytes,1,opt,name=crontab,proto3" json:"crontab,omitempty"`
}

func (x *BackupSchedulePattern) Reset() {
	*x = BackupSchedulePattern{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ydbcp_v1alpha1_backup_schedule_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BackupSchedulePattern) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BackupSchedulePattern) ProtoMessage() {}

func (x *BackupSchedulePattern) ProtoReflect() protoreflect.Message {
	mi := &file_ydbcp_v1alpha1_backup_schedule_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BackupSchedulePattern.ProtoReflect.Descriptor instead.
func (*BackupSchedulePattern) Descriptor() ([]byte, []int) {
	return file_ydbcp_v1alpha1_backup_schedule_proto_rawDescGZIP(), []int{0}
}

func (x *BackupSchedulePattern) GetCrontab() string {
	if x != nil {
		return x.Crontab
	}
	return ""
}

type BackupScheduleSettings struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	SchedulePattern        *BackupSchedulePattern `protobuf:"bytes,1,opt,name=schedule_pattern,json=schedulePattern,proto3" json:"schedule_pattern,omitempty"`
	Ttl                    *durationpb.Duration   `protobuf:"bytes,2,opt,name=ttl,proto3" json:"ttl,omitempty"`
	RecoveryPointObjective *durationpb.Duration   `protobuf:"bytes,3,opt,name=recovery_point_objective,json=recoveryPointObjective,proto3" json:"recovery_point_objective,omitempty"`
}

func (x *BackupScheduleSettings) Reset() {
	*x = BackupScheduleSettings{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ydbcp_v1alpha1_backup_schedule_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BackupScheduleSettings) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BackupScheduleSettings) ProtoMessage() {}

func (x *BackupScheduleSettings) ProtoReflect() protoreflect.Message {
	mi := &file_ydbcp_v1alpha1_backup_schedule_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BackupScheduleSettings.ProtoReflect.Descriptor instead.
func (*BackupScheduleSettings) Descriptor() ([]byte, []int) {
	return file_ydbcp_v1alpha1_backup_schedule_proto_rawDescGZIP(), []int{1}
}

func (x *BackupScheduleSettings) GetSchedulePattern() *BackupSchedulePattern {
	if x != nil {
		return x.SchedulePattern
	}
	return nil
}

func (x *BackupScheduleSettings) GetTtl() *durationpb.Duration {
	if x != nil {
		return x.Ttl
	}
	return nil
}

func (x *BackupScheduleSettings) GetRecoveryPointObjective() *durationpb.Duration {
	if x != nil {
		return x.RecoveryPointObjective
	}
	return nil
}

type ScheduledBackupInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	BackupId                    string                 `protobuf:"bytes,1,opt,name=backup_id,json=backupId,proto3" json:"backup_id,omitempty"`
	RecoveryPoint               *timestamppb.Timestamp `protobuf:"bytes,2,opt,name=recovery_point,json=recoveryPoint,proto3" json:"recovery_point,omitempty"`
	LastBackupRpoMarginInterval *durationpb.Duration   `protobuf:"bytes,3,opt,name=last_backup_rpo_margin_interval,json=lastBackupRpoMarginInterval,proto3" json:"last_backup_rpo_margin_interval,omitempty"`
	LastBackupRpoMarginRatio    float64                `protobuf:"fixed64,4,opt,name=last_backup_rpo_margin_ratio,json=lastBackupRpoMarginRatio,proto3" json:"last_backup_rpo_margin_ratio,omitempty"`
}

func (x *ScheduledBackupInfo) Reset() {
	*x = ScheduledBackupInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ydbcp_v1alpha1_backup_schedule_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ScheduledBackupInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ScheduledBackupInfo) ProtoMessage() {}

func (x *ScheduledBackupInfo) ProtoReflect() protoreflect.Message {
	mi := &file_ydbcp_v1alpha1_backup_schedule_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ScheduledBackupInfo.ProtoReflect.Descriptor instead.
func (*ScheduledBackupInfo) Descriptor() ([]byte, []int) {
	return file_ydbcp_v1alpha1_backup_schedule_proto_rawDescGZIP(), []int{2}
}

func (x *ScheduledBackupInfo) GetBackupId() string {
	if x != nil {
		return x.BackupId
	}
	return ""
}

func (x *ScheduledBackupInfo) GetRecoveryPoint() *timestamppb.Timestamp {
	if x != nil {
		return x.RecoveryPoint
	}
	return nil
}

func (x *ScheduledBackupInfo) GetLastBackupRpoMarginInterval() *durationpb.Duration {
	if x != nil {
		return x.LastBackupRpoMarginInterval
	}
	return nil
}

func (x *ScheduledBackupInfo) GetLastBackupRpoMarginRatio() float64 {
	if x != nil {
		return x.LastBackupRpoMarginRatio
	}
	return 0
}

type BackupSchedule struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// backup settings
	Id                       string                  `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	ContainerId              string                  `protobuf:"bytes,2,opt,name=container_id,json=containerId,proto3" json:"container_id,omitempty"`
	DatabaseName             string                  `protobuf:"bytes,3,opt,name=database_name,json=databaseName,proto3" json:"database_name,omitempty"`
	Endpoint                 string                  `protobuf:"bytes,4,opt,name=endpoint,proto3" json:"endpoint,omitempty"`
	SourcePaths              []string                `protobuf:"bytes,5,rep,name=source_paths,json=sourcePaths,proto3" json:"source_paths,omitempty"`                                // [(size) = "<=256"];
	SourcePathsToExclude     []string                `protobuf:"bytes,6,rep,name=source_paths_to_exclude,json=sourcePathsToExclude,proto3" json:"source_paths_to_exclude,omitempty"` // [(size) = "<=256"];
	Audit                    *AuditInfo              `protobuf:"bytes,7,opt,name=audit,proto3" json:"audit,omitempty"`
	ScheduleName             string                  `protobuf:"bytes,8,opt,name=schedule_name,json=scheduleName,proto3" json:"schedule_name,omitempty"`
	Status                   BackupSchedule_Status   `protobuf:"varint,9,opt,name=status,proto3,enum=ydbcp.v1alpha1.BackupSchedule_Status" json:"status,omitempty"`
	ScheduleSettings         *BackupScheduleSettings `protobuf:"bytes,10,opt,name=schedule_settings,json=scheduleSettings,proto3" json:"schedule_settings,omitempty"`
	NextLaunch               *timestamppb.Timestamp  `protobuf:"bytes,11,opt,name=next_launch,json=nextLaunch,proto3" json:"next_launch,omitempty"`
	LastSuccessfulBackupInfo *ScheduledBackupInfo    `protobuf:"bytes,12,opt,name=last_successful_backup_info,json=lastSuccessfulBackupInfo,proto3" json:"last_successful_backup_info,omitempty"`
}

func (x *BackupSchedule) Reset() {
	*x = BackupSchedule{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ydbcp_v1alpha1_backup_schedule_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BackupSchedule) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BackupSchedule) ProtoMessage() {}

func (x *BackupSchedule) ProtoReflect() protoreflect.Message {
	mi := &file_ydbcp_v1alpha1_backup_schedule_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BackupSchedule.ProtoReflect.Descriptor instead.
func (*BackupSchedule) Descriptor() ([]byte, []int) {
	return file_ydbcp_v1alpha1_backup_schedule_proto_rawDescGZIP(), []int{3}
}

func (x *BackupSchedule) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *BackupSchedule) GetContainerId() string {
	if x != nil {
		return x.ContainerId
	}
	return ""
}

func (x *BackupSchedule) GetDatabaseName() string {
	if x != nil {
		return x.DatabaseName
	}
	return ""
}

func (x *BackupSchedule) GetEndpoint() string {
	if x != nil {
		return x.Endpoint
	}
	return ""
}

func (x *BackupSchedule) GetSourcePaths() []string {
	if x != nil {
		return x.SourcePaths
	}
	return nil
}

func (x *BackupSchedule) GetSourcePathsToExclude() []string {
	if x != nil {
		return x.SourcePathsToExclude
	}
	return nil
}

func (x *BackupSchedule) GetAudit() *AuditInfo {
	if x != nil {
		return x.Audit
	}
	return nil
}

func (x *BackupSchedule) GetScheduleName() string {
	if x != nil {
		return x.ScheduleName
	}
	return ""
}

func (x *BackupSchedule) GetStatus() BackupSchedule_Status {
	if x != nil {
		return x.Status
	}
	return BackupSchedule_STATUS_UNSPECIFIED
}

func (x *BackupSchedule) GetScheduleSettings() *BackupScheduleSettings {
	if x != nil {
		return x.ScheduleSettings
	}
	return nil
}

func (x *BackupSchedule) GetNextLaunch() *timestamppb.Timestamp {
	if x != nil {
		return x.NextLaunch
	}
	return nil
}

func (x *BackupSchedule) GetLastSuccessfulBackupInfo() *ScheduledBackupInfo {
	if x != nil {
		return x.LastSuccessfulBackupInfo
	}
	return nil
}

var File_ydbcp_v1alpha1_backup_schedule_proto protoreflect.FileDescriptor

var file_ydbcp_v1alpha1_backup_schedule_proto_rawDesc = []byte{
	0x0a, 0x24, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2f, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31,
	0x2f, 0x62, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x5f, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0e, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2e, 0x76, 0x31,
	0x61, 0x6c, 0x70, 0x68, 0x61, 0x31, 0x1a, 0x1b, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2f, 0x76, 0x31,
	0x61, 0x6c, 0x70, 0x68, 0x61, 0x31, 0x2f, 0x62, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x1a, 0x1e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x2f, 0x64, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x22, 0x31, 0x0a, 0x15, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x53, 0x63,
	0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x50, 0x61, 0x74, 0x74, 0x65, 0x72, 0x6e, 0x12, 0x18, 0x0a,
	0x07, 0x63, 0x72, 0x6f, 0x6e, 0x74, 0x61, 0x62, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07,
	0x63, 0x72, 0x6f, 0x6e, 0x74, 0x61, 0x62, 0x22, 0xec, 0x01, 0x0a, 0x16, 0x42, 0x61, 0x63, 0x6b,
	0x75, 0x70, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x53, 0x65, 0x74, 0x74, 0x69, 0x6e,
	0x67, 0x73, 0x12, 0x50, 0x0a, 0x10, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x5f, 0x70,
	0x61, 0x74, 0x74, 0x65, 0x72, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x25, 0x2e, 0x79,
	0x64, 0x62, 0x63, 0x70, 0x2e, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31, 0x2e, 0x42, 0x61,
	0x63, 0x6b, 0x75, 0x70, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x50, 0x61, 0x74, 0x74,
	0x65, 0x72, 0x6e, 0x52, 0x0f, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x50, 0x61, 0x74,
	0x74, 0x65, 0x72, 0x6e, 0x12, 0x2b, 0x0a, 0x03, 0x74, 0x74, 0x6c, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x19, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x62, 0x75, 0x66, 0x2e, 0x44, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x03, 0x74, 0x74,
	0x6c, 0x12, 0x53, 0x0a, 0x18, 0x72, 0x65, 0x63, 0x6f, 0x76, 0x65, 0x72, 0x79, 0x5f, 0x70, 0x6f,
	0x69, 0x6e, 0x74, 0x5f, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x69, 0x76, 0x65, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x44, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x16,
	0x72, 0x65, 0x63, 0x6f, 0x76, 0x65, 0x72, 0x79, 0x50, 0x6f, 0x69, 0x6e, 0x74, 0x4f, 0x62, 0x6a,
	0x65, 0x63, 0x74, 0x69, 0x76, 0x65, 0x22, 0x96, 0x02, 0x0a, 0x13, 0x53, 0x63, 0x68, 0x65, 0x64,
	0x75, 0x6c, 0x65, 0x64, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x1b,
	0x0a, 0x09, 0x62, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x08, 0x62, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x49, 0x64, 0x12, 0x41, 0x0a, 0x0e, 0x72,
	0x65, 0x63, 0x6f, 0x76, 0x65, 0x72, 0x79, 0x5f, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52,
	0x0d, 0x72, 0x65, 0x63, 0x6f, 0x76, 0x65, 0x72, 0x79, 0x50, 0x6f, 0x69, 0x6e, 0x74, 0x12, 0x5f,
	0x0a, 0x1f, 0x6c, 0x61, 0x73, 0x74, 0x5f, 0x62, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x5f, 0x72, 0x70,
	0x6f, 0x5f, 0x6d, 0x61, 0x72, 0x67, 0x69, 0x6e, 0x5f, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x76, 0x61,
	0x6c, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x44, 0x75, 0x72, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x52, 0x1b, 0x6c, 0x61, 0x73, 0x74, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x52, 0x70,
	0x6f, 0x4d, 0x61, 0x72, 0x67, 0x69, 0x6e, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x76, 0x61, 0x6c, 0x12,
	0x3e, 0x0a, 0x1c, 0x6c, 0x61, 0x73, 0x74, 0x5f, 0x62, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x5f, 0x72,
	0x70, 0x6f, 0x5f, 0x6d, 0x61, 0x72, 0x67, 0x69, 0x6e, 0x5f, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x18,
	0x04, 0x20, 0x01, 0x28, 0x01, 0x52, 0x18, 0x6c, 0x61, 0x73, 0x74, 0x42, 0x61, 0x63, 0x6b, 0x75,
	0x70, 0x52, 0x70, 0x6f, 0x4d, 0x61, 0x72, 0x67, 0x69, 0x6e, 0x52, 0x61, 0x74, 0x69, 0x6f, 0x22,
	0xb2, 0x05, 0x0a, 0x0e, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75,
	0x6c, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02,
	0x69, 0x64, 0x12, 0x21, 0x0a, 0x0c, 0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x5f,
	0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69,
	0x6e, 0x65, 0x72, 0x49, 0x64, 0x12, 0x23, 0x0a, 0x0d, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73,
	0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x64, 0x61,
	0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x65, 0x6e,
	0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x65, 0x6e,
	0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x12, 0x21, 0x0a, 0x0c, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65,
	0x5f, 0x70, 0x61, 0x74, 0x68, 0x73, 0x18, 0x05, 0x20, 0x03, 0x28, 0x09, 0x52, 0x0b, 0x73, 0x6f,
	0x75, 0x72, 0x63, 0x65, 0x50, 0x61, 0x74, 0x68, 0x73, 0x12, 0x35, 0x0a, 0x17, 0x73, 0x6f, 0x75,
	0x72, 0x63, 0x65, 0x5f, 0x70, 0x61, 0x74, 0x68, 0x73, 0x5f, 0x74, 0x6f, 0x5f, 0x65, 0x78, 0x63,
	0x6c, 0x75, 0x64, 0x65, 0x18, 0x06, 0x20, 0x03, 0x28, 0x09, 0x52, 0x14, 0x73, 0x6f, 0x75, 0x72,
	0x63, 0x65, 0x50, 0x61, 0x74, 0x68, 0x73, 0x54, 0x6f, 0x45, 0x78, 0x63, 0x6c, 0x75, 0x64, 0x65,
	0x12, 0x2f, 0x0a, 0x05, 0x61, 0x75, 0x64, 0x69, 0x74, 0x18, 0x07, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x19, 0x2e, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2e, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31,
	0x2e, 0x41, 0x75, 0x64, 0x69, 0x74, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x05, 0x61, 0x75, 0x64, 0x69,
	0x74, 0x12, 0x23, 0x0a, 0x0d, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x5f, 0x6e, 0x61,
	0x6d, 0x65, 0x18, 0x08, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75,
	0x6c, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x3d, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73,
	0x18, 0x09, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x25, 0x2e, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2e, 0x76,
	0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31, 0x2e, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x53, 0x63,
	0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x52, 0x06, 0x73,
	0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x53, 0x0a, 0x11, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c,
	0x65, 0x5f, 0x73, 0x65, 0x74, 0x74, 0x69, 0x6e, 0x67, 0x73, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x26, 0x2e, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2e, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61,
	0x31, 0x2e, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65,
	0x53, 0x65, 0x74, 0x74, 0x69, 0x6e, 0x67, 0x73, 0x52, 0x10, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75,
	0x6c, 0x65, 0x53, 0x65, 0x74, 0x74, 0x69, 0x6e, 0x67, 0x73, 0x12, 0x3b, 0x0a, 0x0b, 0x6e, 0x65,
	0x78, 0x74, 0x5f, 0x6c, 0x61, 0x75, 0x6e, 0x63, 0x68, 0x18, 0x0b, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0a, 0x6e, 0x65, 0x78,
	0x74, 0x4c, 0x61, 0x75, 0x6e, 0x63, 0x68, 0x12, 0x62, 0x0a, 0x1b, 0x6c, 0x61, 0x73, 0x74, 0x5f,
	0x73, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x66, 0x75, 0x6c, 0x5f, 0x62, 0x61, 0x63, 0x6b, 0x75,
	0x70, 0x5f, 0x69, 0x6e, 0x66, 0x6f, 0x18, 0x0c, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x23, 0x2e, 0x79,
	0x64, 0x62, 0x63, 0x70, 0x2e, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31, 0x2e, 0x53, 0x63,
	0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x64, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x49, 0x6e, 0x66,
	0x6f, 0x52, 0x18, 0x6c, 0x61, 0x73, 0x74, 0x53, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x66, 0x75,
	0x6c, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x49, 0x6e, 0x66, 0x6f, 0x22, 0x47, 0x0a, 0x06, 0x53,
	0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x16, 0x0a, 0x12, 0x53, 0x54, 0x41, 0x54, 0x55, 0x53, 0x5f,
	0x55, 0x4e, 0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49, 0x45, 0x44, 0x10, 0x00, 0x12, 0x0a, 0x0a,
	0x06, 0x41, 0x43, 0x54, 0x49, 0x56, 0x45, 0x10, 0x01, 0x12, 0x0c, 0x0a, 0x08, 0x49, 0x4e, 0x41,
	0x43, 0x54, 0x49, 0x56, 0x45, 0x10, 0x02, 0x12, 0x0b, 0x0a, 0x07, 0x44, 0x45, 0x4c, 0x45, 0x54,
	0x45, 0x44, 0x10, 0x03, 0x42, 0x3e, 0x5a, 0x3c, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63,
	0x6f, 0x6d, 0x2f, 0x79, 0x64, 0x62, 0x2d, 0x70, 0x6c, 0x61, 0x74, 0x66, 0x6f, 0x72, 0x6d, 0x2f,
	0x79, 0x64, 0x62, 0x63, 0x70, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f,
	0x79, 0x64, 0x62, 0x63, 0x70, 0x2f, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31, 0x3b, 0x79,
	0x64, 0x62, 0x63, 0x70, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_ydbcp_v1alpha1_backup_schedule_proto_rawDescOnce sync.Once
	file_ydbcp_v1alpha1_backup_schedule_proto_rawDescData = file_ydbcp_v1alpha1_backup_schedule_proto_rawDesc
)

func file_ydbcp_v1alpha1_backup_schedule_proto_rawDescGZIP() []byte {
	file_ydbcp_v1alpha1_backup_schedule_proto_rawDescOnce.Do(func() {
		file_ydbcp_v1alpha1_backup_schedule_proto_rawDescData = protoimpl.X.CompressGZIP(file_ydbcp_v1alpha1_backup_schedule_proto_rawDescData)
	})
	return file_ydbcp_v1alpha1_backup_schedule_proto_rawDescData
}

var file_ydbcp_v1alpha1_backup_schedule_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_ydbcp_v1alpha1_backup_schedule_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_ydbcp_v1alpha1_backup_schedule_proto_goTypes = []interface{}{
	(BackupSchedule_Status)(0),     // 0: ydbcp.v1alpha1.BackupSchedule.Status
	(*BackupSchedulePattern)(nil),  // 1: ydbcp.v1alpha1.BackupSchedulePattern
	(*BackupScheduleSettings)(nil), // 2: ydbcp.v1alpha1.BackupScheduleSettings
	(*ScheduledBackupInfo)(nil),    // 3: ydbcp.v1alpha1.ScheduledBackupInfo
	(*BackupSchedule)(nil),         // 4: ydbcp.v1alpha1.BackupSchedule
	(*durationpb.Duration)(nil),    // 5: google.protobuf.Duration
	(*timestamppb.Timestamp)(nil),  // 6: google.protobuf.Timestamp
	(*AuditInfo)(nil),              // 7: ydbcp.v1alpha1.AuditInfo
}
var file_ydbcp_v1alpha1_backup_schedule_proto_depIdxs = []int32{
	1,  // 0: ydbcp.v1alpha1.BackupScheduleSettings.schedule_pattern:type_name -> ydbcp.v1alpha1.BackupSchedulePattern
	5,  // 1: ydbcp.v1alpha1.BackupScheduleSettings.ttl:type_name -> google.protobuf.Duration
	5,  // 2: ydbcp.v1alpha1.BackupScheduleSettings.recovery_point_objective:type_name -> google.protobuf.Duration
	6,  // 3: ydbcp.v1alpha1.ScheduledBackupInfo.recovery_point:type_name -> google.protobuf.Timestamp
	5,  // 4: ydbcp.v1alpha1.ScheduledBackupInfo.last_backup_rpo_margin_interval:type_name -> google.protobuf.Duration
	7,  // 5: ydbcp.v1alpha1.BackupSchedule.audit:type_name -> ydbcp.v1alpha1.AuditInfo
	0,  // 6: ydbcp.v1alpha1.BackupSchedule.status:type_name -> ydbcp.v1alpha1.BackupSchedule.Status
	2,  // 7: ydbcp.v1alpha1.BackupSchedule.schedule_settings:type_name -> ydbcp.v1alpha1.BackupScheduleSettings
	6,  // 8: ydbcp.v1alpha1.BackupSchedule.next_launch:type_name -> google.protobuf.Timestamp
	3,  // 9: ydbcp.v1alpha1.BackupSchedule.last_successful_backup_info:type_name -> ydbcp.v1alpha1.ScheduledBackupInfo
	10, // [10:10] is the sub-list for method output_type
	10, // [10:10] is the sub-list for method input_type
	10, // [10:10] is the sub-list for extension type_name
	10, // [10:10] is the sub-list for extension extendee
	0,  // [0:10] is the sub-list for field type_name
}

func init() { file_ydbcp_v1alpha1_backup_schedule_proto_init() }
func file_ydbcp_v1alpha1_backup_schedule_proto_init() {
	if File_ydbcp_v1alpha1_backup_schedule_proto != nil {
		return
	}
	file_ydbcp_v1alpha1_backup_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_ydbcp_v1alpha1_backup_schedule_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BackupSchedulePattern); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_ydbcp_v1alpha1_backup_schedule_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BackupScheduleSettings); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_ydbcp_v1alpha1_backup_schedule_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ScheduledBackupInfo); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_ydbcp_v1alpha1_backup_schedule_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BackupSchedule); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_ydbcp_v1alpha1_backup_schedule_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_ydbcp_v1alpha1_backup_schedule_proto_goTypes,
		DependencyIndexes: file_ydbcp_v1alpha1_backup_schedule_proto_depIdxs,
		EnumInfos:         file_ydbcp_v1alpha1_backup_schedule_proto_enumTypes,
		MessageInfos:      file_ydbcp_v1alpha1_backup_schedule_proto_msgTypes,
	}.Build()
	File_ydbcp_v1alpha1_backup_schedule_proto = out.File
	file_ydbcp_v1alpha1_backup_schedule_proto_rawDesc = nil
	file_ydbcp_v1alpha1_backup_schedule_proto_goTypes = nil
	file_ydbcp_v1alpha1_backup_schedule_proto_depIdxs = nil
}
