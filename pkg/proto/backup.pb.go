// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.32.0
// 	protoc        v5.27.1
// source: backup.proto

package ydbcp

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
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

type Backup_Status int32

const (
	Backup_STATUS_UNSPECIFIED Backup_Status = 0
	Backup_PENDING            Backup_Status = 1
	Backup_AVAILABLE          Backup_Status = 2
	Backup_ERROR              Backup_Status = 3
	Backup_CANCELED           Backup_Status = 4
	Backup_DELETED            Backup_Status = 5
)

// Enum value maps for Backup_Status.
var (
	Backup_Status_name = map[int32]string{
		0: "STATUS_UNSPECIFIED",
		1: "PENDING",
		2: "AVAILABLE",
		3: "ERROR",
		4: "CANCELED",
		5: "DELETED",
	}
	Backup_Status_value = map[string]int32{
		"STATUS_UNSPECIFIED": 0,
		"PENDING":            1,
		"AVAILABLE":          2,
		"ERROR":              3,
		"CANCELED":           4,
		"DELETED":            5,
	}
)

func (x Backup_Status) Enum() *Backup_Status {
	p := new(Backup_Status)
	*p = x
	return p
}

func (x Backup_Status) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Backup_Status) Descriptor() protoreflect.EnumDescriptor {
	return file_backup_proto_enumTypes[0].Descriptor()
}

func (Backup_Status) Type() protoreflect.EnumType {
	return &file_backup_proto_enumTypes[0]
}

func (x Backup_Status) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Backup_Status.Descriptor instead.
func (Backup_Status) EnumDescriptor() ([]byte, []int) {
	return file_backup_proto_rawDescGZIP(), []int{0, 0}
}

type Operation_Status int32

const (
	Operation_STATUS_UNSPECIFIED Operation_Status = 0
	Operation_PENDING            Operation_Status = 1
	Operation_DONE               Operation_Status = 2
	Operation_ERROR              Operation_Status = 3
	Operation_CANCELLING         Operation_Status = 4
	Operation_CANCELED           Operation_Status = 5
)

// Enum value maps for Operation_Status.
var (
	Operation_Status_name = map[int32]string{
		0: "STATUS_UNSPECIFIED",
		1: "PENDING",
		2: "DONE",
		3: "ERROR",
		4: "CANCELLING",
		5: "CANCELED",
	}
	Operation_Status_value = map[string]int32{
		"STATUS_UNSPECIFIED": 0,
		"PENDING":            1,
		"DONE":               2,
		"ERROR":              3,
		"CANCELLING":         4,
		"CANCELED":           5,
	}
)

func (x Operation_Status) Enum() *Operation_Status {
	p := new(Operation_Status)
	*p = x
	return p
}

func (x Operation_Status) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Operation_Status) Descriptor() protoreflect.EnumDescriptor {
	return file_backup_proto_enumTypes[1].Descriptor()
}

func (Operation_Status) Type() protoreflect.EnumType {
	return &file_backup_proto_enumTypes[1]
}

func (x Operation_Status) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Operation_Status.Descriptor instead.
func (Operation_Status) EnumDescriptor() ([]byte, []int) {
	return file_backup_proto_rawDescGZIP(), []int{3, 0}
}

type Backup struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id             string        `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	SourceDatabase string        `protobuf:"bytes,2,opt,name=source_database,json=sourceDatabase,proto3" json:"source_database,omitempty"`
	ContainerId    string        `protobuf:"bytes,3,opt,name=container_id,json=containerId,proto3" json:"container_id,omitempty"`
	S3             *S3Settings   `protobuf:"bytes,4,opt,name=s3,proto3" json:"s3,omitempty"`
	Audit          *AuditInfo    `protobuf:"bytes,5,opt,name=audit,proto3" json:"audit,omitempty"`
	Status         Backup_Status `protobuf:"varint,6,opt,name=status,proto3,enum=ydbcp.Backup_Status" json:"status,omitempty"`
	Message        string        `protobuf:"bytes,7,opt,name=message,proto3" json:"message,omitempty"`
	Paths          []string      `protobuf:"bytes,8,rep,name=paths,proto3" json:"paths,omitempty"`
	OperationId    string        `protobuf:"bytes,9,opt,name=operation_id,json=operationId,proto3" json:"operation_id,omitempty"`
}

func (x *Backup) Reset() {
	*x = Backup{}
	if protoimpl.UnsafeEnabled {
		mi := &file_backup_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Backup) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Backup) ProtoMessage() {}

func (x *Backup) ProtoReflect() protoreflect.Message {
	mi := &file_backup_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Backup.ProtoReflect.Descriptor instead.
func (*Backup) Descriptor() ([]byte, []int) {
	return file_backup_proto_rawDescGZIP(), []int{0}
}

func (x *Backup) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Backup) GetSourceDatabase() string {
	if x != nil {
		return x.SourceDatabase
	}
	return ""
}

func (x *Backup) GetContainerId() string {
	if x != nil {
		return x.ContainerId
	}
	return ""
}

func (x *Backup) GetS3() *S3Settings {
	if x != nil {
		return x.S3
	}
	return nil
}

func (x *Backup) GetAudit() *AuditInfo {
	if x != nil {
		return x.Audit
	}
	return nil
}

func (x *Backup) GetStatus() Backup_Status {
	if x != nil {
		return x.Status
	}
	return Backup_STATUS_UNSPECIFIED
}

func (x *Backup) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

func (x *Backup) GetPaths() []string {
	if x != nil {
		return x.Paths
	}
	return nil
}

func (x *Backup) GetOperationId() string {
	if x != nil {
		return x.OperationId
	}
	return ""
}

type S3Settings struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Endpoint   string `protobuf:"bytes,1,opt,name=endpoint,proto3" json:"endpoint,omitempty"`
	Bucket     string `protobuf:"bytes,2,opt,name=bucket,proto3" json:"bucket,omitempty"`
	AccessKey  string `protobuf:"bytes,3,opt,name=access_key,json=accessKey,proto3" json:"access_key,omitempty"`
	SecretKey  string `protobuf:"bytes,4,opt,name=secret_key,json=secretKey,proto3" json:"secret_key,omitempty"`
	Region     string `protobuf:"bytes,5,opt,name=region,proto3" json:"region,omitempty"`
	PathPrefix string `protobuf:"bytes,6,opt,name=path_prefix,json=pathPrefix,proto3" json:"path_prefix,omitempty"`
}

func (x *S3Settings) Reset() {
	*x = S3Settings{}
	if protoimpl.UnsafeEnabled {
		mi := &file_backup_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *S3Settings) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*S3Settings) ProtoMessage() {}

func (x *S3Settings) ProtoReflect() protoreflect.Message {
	mi := &file_backup_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use S3Settings.ProtoReflect.Descriptor instead.
func (*S3Settings) Descriptor() ([]byte, []int) {
	return file_backup_proto_rawDescGZIP(), []int{1}
}

func (x *S3Settings) GetEndpoint() string {
	if x != nil {
		return x.Endpoint
	}
	return ""
}

func (x *S3Settings) GetBucket() string {
	if x != nil {
		return x.Bucket
	}
	return ""
}

func (x *S3Settings) GetAccessKey() string {
	if x != nil {
		return x.AccessKey
	}
	return ""
}

func (x *S3Settings) GetSecretKey() string {
	if x != nil {
		return x.SecretKey
	}
	return ""
}

func (x *S3Settings) GetRegion() string {
	if x != nil {
		return x.Region
	}
	return ""
}

func (x *S3Settings) GetPathPrefix() string {
	if x != nil {
		return x.PathPrefix
	}
	return ""
}

type AuditInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Creator     string                 `protobuf:"bytes,1,opt,name=creator,proto3" json:"creator,omitempty"`
	CreatedAt   *timestamppb.Timestamp `protobuf:"bytes,2,opt,name=created_at,json=createdAt,proto3" json:"created_at,omitempty"`
	CompletedAt *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=completed_at,json=completedAt,proto3" json:"completed_at,omitempty"`
	ExpiredAt   *timestamppb.Timestamp `protobuf:"bytes,10,opt,name=expired_at,json=expiredAt,proto3" json:"expired_at,omitempty"`
}

func (x *AuditInfo) Reset() {
	*x = AuditInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_backup_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AuditInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AuditInfo) ProtoMessage() {}

func (x *AuditInfo) ProtoReflect() protoreflect.Message {
	mi := &file_backup_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AuditInfo.ProtoReflect.Descriptor instead.
func (*AuditInfo) Descriptor() ([]byte, []int) {
	return file_backup_proto_rawDescGZIP(), []int{2}
}

func (x *AuditInfo) GetCreator() string {
	if x != nil {
		return x.Creator
	}
	return ""
}

func (x *AuditInfo) GetCreatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.CreatedAt
	}
	return nil
}

func (x *AuditInfo) GetCompletedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.CompletedAt
	}
	return nil
}

func (x *AuditInfo) GetExpiredAt() *timestamppb.Timestamp {
	if x != nil {
		return x.ExpiredAt
	}
	return nil
}

type Operation struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id             string           `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Type           string           `protobuf:"bytes,2,opt,name=type,proto3" json:"type,omitempty"`
	ContainerId    string           `protobuf:"bytes,3,opt,name=container_id,json=containerId,proto3" json:"container_id,omitempty"`
	Database       string           `protobuf:"bytes,4,opt,name=database,proto3" json:"database,omitempty"`
	YdbOperationId string           `protobuf:"bytes,5,opt,name=ydb_operation_id,json=ydbOperationId,proto3" json:"ydb_operation_id,omitempty"`
	BackupId       string           `protobuf:"bytes,6,opt,name=backup_id,json=backupId,proto3" json:"backup_id,omitempty"`
	Paths          []string         `protobuf:"bytes,7,rep,name=paths,proto3" json:"paths,omitempty"`
	Audit          *AuditInfo       `protobuf:"bytes,8,opt,name=audit,proto3" json:"audit,omitempty"`
	Status         Operation_Status `protobuf:"varint,9,opt,name=status,proto3,enum=ydbcp.Operation_Status" json:"status,omitempty"`
	Message        string           `protobuf:"bytes,10,opt,name=message,proto3" json:"message,omitempty"`
}

func (x *Operation) Reset() {
	*x = Operation{}
	if protoimpl.UnsafeEnabled {
		mi := &file_backup_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Operation) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Operation) ProtoMessage() {}

func (x *Operation) ProtoReflect() protoreflect.Message {
	mi := &file_backup_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Operation.ProtoReflect.Descriptor instead.
func (*Operation) Descriptor() ([]byte, []int) {
	return file_backup_proto_rawDescGZIP(), []int{3}
}

func (x *Operation) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Operation) GetType() string {
	if x != nil {
		return x.Type
	}
	return ""
}

func (x *Operation) GetContainerId() string {
	if x != nil {
		return x.ContainerId
	}
	return ""
}

func (x *Operation) GetDatabase() string {
	if x != nil {
		return x.Database
	}
	return ""
}

func (x *Operation) GetYdbOperationId() string {
	if x != nil {
		return x.YdbOperationId
	}
	return ""
}

func (x *Operation) GetBackupId() string {
	if x != nil {
		return x.BackupId
	}
	return ""
}

func (x *Operation) GetPaths() []string {
	if x != nil {
		return x.Paths
	}
	return nil
}

func (x *Operation) GetAudit() *AuditInfo {
	if x != nil {
		return x.Audit
	}
	return nil
}

func (x *Operation) GetStatus() Operation_Status {
	if x != nil {
		return x.Status
	}
	return Operation_STATUS_UNSPECIFIED
}

func (x *Operation) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

var File_backup_proto protoreflect.FileDescriptor

var file_backup_proto_rawDesc = []byte{
	0x0a, 0x0c, 0x62, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x05,
	0x79, 0x64, 0x62, 0x63, 0x70, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x94, 0x03, 0x0a, 0x06, 0x42, 0x61, 0x63, 0x6b, 0x75,
	0x70, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69,
	0x64, 0x12, 0x27, 0x0a, 0x0f, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x5f, 0x64, 0x61, 0x74, 0x61,
	0x62, 0x61, 0x73, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0e, 0x73, 0x6f, 0x75, 0x72,
	0x63, 0x65, 0x44, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x12, 0x21, 0x0a, 0x0c, 0x63, 0x6f,
	0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x0b, 0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x49, 0x64, 0x12, 0x21, 0x0a,
	0x02, 0x73, 0x33, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x79, 0x64, 0x62, 0x63,
	0x70, 0x2e, 0x53, 0x33, 0x53, 0x65, 0x74, 0x74, 0x69, 0x6e, 0x67, 0x73, 0x52, 0x02, 0x73, 0x33,
	0x12, 0x26, 0x0a, 0x05, 0x61, 0x75, 0x64, 0x69, 0x74, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x10, 0x2e, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2e, 0x41, 0x75, 0x64, 0x69, 0x74, 0x49, 0x6e, 0x66,
	0x6f, 0x52, 0x05, 0x61, 0x75, 0x64, 0x69, 0x74, 0x12, 0x2c, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74,
	0x75, 0x73, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x14, 0x2e, 0x79, 0x64, 0x62, 0x63, 0x70,
	0x2e, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x52, 0x06,
	0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x18, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67,
	0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65,
	0x12, 0x14, 0x0a, 0x05, 0x70, 0x61, 0x74, 0x68, 0x73, 0x18, 0x08, 0x20, 0x03, 0x28, 0x09, 0x52,
	0x05, 0x70, 0x61, 0x74, 0x68, 0x73, 0x12, 0x21, 0x0a, 0x0c, 0x6f, 0x70, 0x65, 0x72, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x09, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x6f, 0x70,
	0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x22, 0x62, 0x0a, 0x06, 0x53, 0x74, 0x61,
	0x74, 0x75, 0x73, 0x12, 0x16, 0x0a, 0x12, 0x53, 0x54, 0x41, 0x54, 0x55, 0x53, 0x5f, 0x55, 0x4e,
	0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49, 0x45, 0x44, 0x10, 0x00, 0x12, 0x0b, 0x0a, 0x07, 0x50,
	0x45, 0x4e, 0x44, 0x49, 0x4e, 0x47, 0x10, 0x01, 0x12, 0x0d, 0x0a, 0x09, 0x41, 0x56, 0x41, 0x49,
	0x4c, 0x41, 0x42, 0x4c, 0x45, 0x10, 0x02, 0x12, 0x09, 0x0a, 0x05, 0x45, 0x52, 0x52, 0x4f, 0x52,
	0x10, 0x03, 0x12, 0x0c, 0x0a, 0x08, 0x43, 0x41, 0x4e, 0x43, 0x45, 0x4c, 0x45, 0x44, 0x10, 0x04,
	0x12, 0x0b, 0x0a, 0x07, 0x44, 0x45, 0x4c, 0x45, 0x54, 0x45, 0x44, 0x10, 0x05, 0x22, 0xb7, 0x01,
	0x0a, 0x0a, 0x53, 0x33, 0x53, 0x65, 0x74, 0x74, 0x69, 0x6e, 0x67, 0x73, 0x12, 0x1a, 0x0a, 0x08,
	0x65, 0x6e, 0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08,
	0x65, 0x6e, 0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x12, 0x16, 0x0a, 0x06, 0x62, 0x75, 0x63, 0x6b,
	0x65, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x62, 0x75, 0x63, 0x6b, 0x65, 0x74,
	0x12, 0x1d, 0x0a, 0x0a, 0x61, 0x63, 0x63, 0x65, 0x73, 0x73, 0x5f, 0x6b, 0x65, 0x79, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x61, 0x63, 0x63, 0x65, 0x73, 0x73, 0x4b, 0x65, 0x79, 0x12,
	0x1d, 0x0a, 0x0a, 0x73, 0x65, 0x63, 0x72, 0x65, 0x74, 0x5f, 0x6b, 0x65, 0x79, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x09, 0x73, 0x65, 0x63, 0x72, 0x65, 0x74, 0x4b, 0x65, 0x79, 0x12, 0x16,
	0x0a, 0x06, 0x72, 0x65, 0x67, 0x69, 0x6f, 0x6e, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06,
	0x72, 0x65, 0x67, 0x69, 0x6f, 0x6e, 0x12, 0x1f, 0x0a, 0x0b, 0x70, 0x61, 0x74, 0x68, 0x5f, 0x70,
	0x72, 0x65, 0x66, 0x69, 0x78, 0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x70, 0x61, 0x74,
	0x68, 0x50, 0x72, 0x65, 0x66, 0x69, 0x78, 0x22, 0xda, 0x01, 0x0a, 0x09, 0x41, 0x75, 0x64, 0x69,
	0x74, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x18, 0x0a, 0x07, 0x63, 0x72, 0x65, 0x61, 0x74, 0x6f, 0x72,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x63, 0x72, 0x65, 0x61, 0x74, 0x6f, 0x72, 0x12,
	0x39, 0x0a, 0x0a, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x5f, 0x61, 0x74, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52,
	0x09, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x41, 0x74, 0x12, 0x3d, 0x0a, 0x0c, 0x63, 0x6f,
	0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x5f, 0x61, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62,
	0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0b, 0x63, 0x6f,
	0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x41, 0x74, 0x12, 0x39, 0x0a, 0x0a, 0x65, 0x78, 0x70,
	0x69, 0x72, 0x65, 0x64, 0x5f, 0x61, 0x74, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e,
	0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x09, 0x65, 0x78, 0x70, 0x69, 0x72,
	0x65, 0x64, 0x41, 0x74, 0x22, 0xa0, 0x03, 0x0a, 0x09, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02,
	0x69, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x04, 0x74, 0x79, 0x70, 0x65, 0x12, 0x21, 0x0a, 0x0c, 0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69,
	0x6e, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x63, 0x6f,
	0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x49, 0x64, 0x12, 0x1a, 0x0a, 0x08, 0x64, 0x61, 0x74,
	0x61, 0x62, 0x61, 0x73, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x64, 0x61, 0x74,
	0x61, 0x62, 0x61, 0x73, 0x65, 0x12, 0x28, 0x0a, 0x10, 0x79, 0x64, 0x62, 0x5f, 0x6f, 0x70, 0x65,
	0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0e, 0x79, 0x64, 0x62, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x12,
	0x1b, 0x0a, 0x09, 0x62, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x5f, 0x69, 0x64, 0x18, 0x06, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x08, 0x62, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x49, 0x64, 0x12, 0x14, 0x0a, 0x05,
	0x70, 0x61, 0x74, 0x68, 0x73, 0x18, 0x07, 0x20, 0x03, 0x28, 0x09, 0x52, 0x05, 0x70, 0x61, 0x74,
	0x68, 0x73, 0x12, 0x26, 0x0a, 0x05, 0x61, 0x75, 0x64, 0x69, 0x74, 0x18, 0x08, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x10, 0x2e, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2e, 0x41, 0x75, 0x64, 0x69, 0x74, 0x49,
	0x6e, 0x66, 0x6f, 0x52, 0x05, 0x61, 0x75, 0x64, 0x69, 0x74, 0x12, 0x2f, 0x0a, 0x06, 0x73, 0x74,
	0x61, 0x74, 0x75, 0x73, 0x18, 0x09, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x17, 0x2e, 0x79, 0x64, 0x62,
	0x63, 0x70, 0x2e, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x53, 0x74, 0x61,
	0x74, 0x75, 0x73, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x18, 0x0a, 0x07, 0x6d,
	0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6d, 0x65,
	0x73, 0x73, 0x61, 0x67, 0x65, 0x22, 0x60, 0x0a, 0x06, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12,
	0x16, 0x0a, 0x12, 0x53, 0x54, 0x41, 0x54, 0x55, 0x53, 0x5f, 0x55, 0x4e, 0x53, 0x50, 0x45, 0x43,
	0x49, 0x46, 0x49, 0x45, 0x44, 0x10, 0x00, 0x12, 0x0b, 0x0a, 0x07, 0x50, 0x45, 0x4e, 0x44, 0x49,
	0x4e, 0x47, 0x10, 0x01, 0x12, 0x08, 0x0a, 0x04, 0x44, 0x4f, 0x4e, 0x45, 0x10, 0x02, 0x12, 0x09,
	0x0a, 0x05, 0x45, 0x52, 0x52, 0x4f, 0x52, 0x10, 0x03, 0x12, 0x0e, 0x0a, 0x0a, 0x43, 0x41, 0x4e,
	0x43, 0x45, 0x4c, 0x4c, 0x49, 0x4e, 0x47, 0x10, 0x04, 0x12, 0x0c, 0x0a, 0x08, 0x43, 0x41, 0x4e,
	0x43, 0x45, 0x4c, 0x45, 0x44, 0x10, 0x05, 0x42, 0x2f, 0x5a, 0x2d, 0x67, 0x69, 0x74, 0x68, 0x75,
	0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x79, 0x64, 0x62, 0x2d, 0x70, 0x6c, 0x61, 0x74, 0x66, 0x6f,
	0x72, 0x6d, 0x2f, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x3b, 0x79, 0x64, 0x62, 0x63, 0x70, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_backup_proto_rawDescOnce sync.Once
	file_backup_proto_rawDescData = file_backup_proto_rawDesc
)

func file_backup_proto_rawDescGZIP() []byte {
	file_backup_proto_rawDescOnce.Do(func() {
		file_backup_proto_rawDescData = protoimpl.X.CompressGZIP(file_backup_proto_rawDescData)
	})
	return file_backup_proto_rawDescData
}

var file_backup_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_backup_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_backup_proto_goTypes = []interface{}{
	(Backup_Status)(0),            // 0: ydbcp.Backup.Status
	(Operation_Status)(0),         // 1: ydbcp.Operation.Status
	(*Backup)(nil),                // 2: ydbcp.Backup
	(*S3Settings)(nil),            // 3: ydbcp.S3Settings
	(*AuditInfo)(nil),             // 4: ydbcp.AuditInfo
	(*Operation)(nil),             // 5: ydbcp.Operation
	(*timestamppb.Timestamp)(nil), // 6: google.protobuf.Timestamp
}
var file_backup_proto_depIdxs = []int32{
	3, // 0: ydbcp.Backup.s3:type_name -> ydbcp.S3Settings
	4, // 1: ydbcp.Backup.audit:type_name -> ydbcp.AuditInfo
	0, // 2: ydbcp.Backup.status:type_name -> ydbcp.Backup.Status
	6, // 3: ydbcp.AuditInfo.created_at:type_name -> google.protobuf.Timestamp
	6, // 4: ydbcp.AuditInfo.completed_at:type_name -> google.protobuf.Timestamp
	6, // 5: ydbcp.AuditInfo.expired_at:type_name -> google.protobuf.Timestamp
	4, // 6: ydbcp.Operation.audit:type_name -> ydbcp.AuditInfo
	1, // 7: ydbcp.Operation.status:type_name -> ydbcp.Operation.Status
	8, // [8:8] is the sub-list for method output_type
	8, // [8:8] is the sub-list for method input_type
	8, // [8:8] is the sub-list for extension type_name
	8, // [8:8] is the sub-list for extension extendee
	0, // [0:8] is the sub-list for field type_name
}

func init() { file_backup_proto_init() }
func file_backup_proto_init() {
	if File_backup_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_backup_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Backup); i {
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
		file_backup_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*S3Settings); i {
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
		file_backup_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AuditInfo); i {
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
		file_backup_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Operation); i {
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
			RawDescriptor: file_backup_proto_rawDesc,
			NumEnums:      2,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_backup_proto_goTypes,
		DependencyIndexes: file_backup_proto_depIdxs,
		EnumInfos:         file_backup_proto_enumTypes,
		MessageInfos:      file_backup_proto_msgTypes,
	}.Build()
	File_backup_proto = out.File
	file_backup_proto_rawDesc = nil
	file_backup_proto_goTypes = nil
	file_backup_proto_depIdxs = nil
}
