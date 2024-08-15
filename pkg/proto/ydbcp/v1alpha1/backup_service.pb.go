// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.32.0
// 	protoc        v5.27.1
// source: ydbcp/v1alpha1/backup_service.proto

package ydbcp

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type ListBackupsRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ContainerId      string `protobuf:"bytes,1,opt,name=container_id,json=containerId,proto3" json:"container_id,omitempty"`
	DatabaseNameMask string `protobuf:"bytes,2,opt,name=database_name_mask,json=databaseNameMask,proto3" json:"database_name_mask,omitempty"`
	// The maximum number of results per page that should be returned. If the number of available
	// results is larger than `page_size`, the service returns a `next_page_token` that can be used
	// to get the next page of results in subsequent ListBackups requests.
	// Acceptable values are 0 to 1000, inclusive. Default value: 100.
	PageSize uint32 `protobuf:"varint,1000,opt,name=page_size,json=pageSize,proto3" json:"page_size,omitempty"` // [(value) = "0-1000"];
	// Page token. Set `page_token` to the `next_page_token` returned by a previous ListBackups
	// request to get the next page of results.
	PageToken string `protobuf:"bytes,1001,opt,name=page_token,json=pageToken,proto3" json:"page_token,omitempty"` // [(length) = "<=100"];
}

func (x *ListBackupsRequest) Reset() {
	*x = ListBackupsRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ydbcp_v1alpha1_backup_service_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListBackupsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListBackupsRequest) ProtoMessage() {}

func (x *ListBackupsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_ydbcp_v1alpha1_backup_service_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListBackupsRequest.ProtoReflect.Descriptor instead.
func (*ListBackupsRequest) Descriptor() ([]byte, []int) {
	return file_ydbcp_v1alpha1_backup_service_proto_rawDescGZIP(), []int{0}
}

func (x *ListBackupsRequest) GetContainerId() string {
	if x != nil {
		return x.ContainerId
	}
	return ""
}

func (x *ListBackupsRequest) GetDatabaseNameMask() string {
	if x != nil {
		return x.DatabaseNameMask
	}
	return ""
}

func (x *ListBackupsRequest) GetPageSize() uint32 {
	if x != nil {
		return x.PageSize
	}
	return 0
}

func (x *ListBackupsRequest) GetPageToken() string {
	if x != nil {
		return x.PageToken
	}
	return ""
}

type ListBackupsResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Backups       []*Backup `protobuf:"bytes,1,rep,name=backups,proto3" json:"backups,omitempty"`
	NextPageToken string    `protobuf:"bytes,2,opt,name=next_page_token,json=nextPageToken,proto3" json:"next_page_token,omitempty"`
}

func (x *ListBackupsResponse) Reset() {
	*x = ListBackupsResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ydbcp_v1alpha1_backup_service_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListBackupsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListBackupsResponse) ProtoMessage() {}

func (x *ListBackupsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_ydbcp_v1alpha1_backup_service_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListBackupsResponse.ProtoReflect.Descriptor instead.
func (*ListBackupsResponse) Descriptor() ([]byte, []int) {
	return file_ydbcp_v1alpha1_backup_service_proto_rawDescGZIP(), []int{1}
}

func (x *ListBackupsResponse) GetBackups() []*Backup {
	if x != nil {
		return x.Backups
	}
	return nil
}

func (x *ListBackupsResponse) GetNextPageToken() string {
	if x != nil {
		return x.NextPageToken
	}
	return ""
}

type GetBackupRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
}

func (x *GetBackupRequest) Reset() {
	*x = GetBackupRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ydbcp_v1alpha1_backup_service_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetBackupRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetBackupRequest) ProtoMessage() {}

func (x *GetBackupRequest) ProtoReflect() protoreflect.Message {
	mi := &file_ydbcp_v1alpha1_backup_service_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetBackupRequest.ProtoReflect.Descriptor instead.
func (*GetBackupRequest) Descriptor() ([]byte, []int) {
	return file_ydbcp_v1alpha1_backup_service_proto_rawDescGZIP(), []int{2}
}

func (x *GetBackupRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

type MakeBackupRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ContainerId      string `protobuf:"bytes,1,opt,name=container_id,json=containerId,proto3" json:"container_id,omitempty"`
	DatabaseName     string `protobuf:"bytes,2,opt,name=database_name,json=databaseName,proto3" json:"database_name,omitempty"`
	DatabaseEndpoint string `protobuf:"bytes,3,opt,name=database_endpoint,json=databaseEndpoint,proto3" json:"database_endpoint,omitempty"`
	// Full path to a table or directory. Empty source_paths means backup of root directory.
	SourcePaths []string `protobuf:"bytes,4,rep,name=source_paths,json=sourcePaths,proto3" json:"source_paths,omitempty"` // [(size) = "<=256"];
	// Regexp for paths excluded from backup.
	SourcePathsToExclude []string `protobuf:"bytes,5,rep,name=source_paths_to_exclude,json=sourcePathsToExclude,proto3" json:"source_paths_to_exclude,omitempty"` // [(size) = "<=256"];
}

func (x *MakeBackupRequest) Reset() {
	*x = MakeBackupRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ydbcp_v1alpha1_backup_service_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MakeBackupRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MakeBackupRequest) ProtoMessage() {}

func (x *MakeBackupRequest) ProtoReflect() protoreflect.Message {
	mi := &file_ydbcp_v1alpha1_backup_service_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MakeBackupRequest.ProtoReflect.Descriptor instead.
func (*MakeBackupRequest) Descriptor() ([]byte, []int) {
	return file_ydbcp_v1alpha1_backup_service_proto_rawDescGZIP(), []int{3}
}

func (x *MakeBackupRequest) GetContainerId() string {
	if x != nil {
		return x.ContainerId
	}
	return ""
}

func (x *MakeBackupRequest) GetDatabaseName() string {
	if x != nil {
		return x.DatabaseName
	}
	return ""
}

func (x *MakeBackupRequest) GetDatabaseEndpoint() string {
	if x != nil {
		return x.DatabaseEndpoint
	}
	return ""
}

func (x *MakeBackupRequest) GetSourcePaths() []string {
	if x != nil {
		return x.SourcePaths
	}
	return nil
}

func (x *MakeBackupRequest) GetSourcePathsToExclude() []string {
	if x != nil {
		return x.SourcePathsToExclude
	}
	return nil
}

type DeleteBackupRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	BackupId string `protobuf:"bytes,1,opt,name=backup_id,json=backupId,proto3" json:"backup_id,omitempty"`
}

func (x *DeleteBackupRequest) Reset() {
	*x = DeleteBackupRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ydbcp_v1alpha1_backup_service_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeleteBackupRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteBackupRequest) ProtoMessage() {}

func (x *DeleteBackupRequest) ProtoReflect() protoreflect.Message {
	mi := &file_ydbcp_v1alpha1_backup_service_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteBackupRequest.ProtoReflect.Descriptor instead.
func (*DeleteBackupRequest) Descriptor() ([]byte, []int) {
	return file_ydbcp_v1alpha1_backup_service_proto_rawDescGZIP(), []int{4}
}

func (x *DeleteBackupRequest) GetBackupId() string {
	if x != nil {
		return x.BackupId
	}
	return ""
}

type MakeRestoreRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ContainerId       string `protobuf:"bytes,1,opt,name=container_id,json=containerId,proto3" json:"container_id,omitempty"`
	BackupId          string `protobuf:"bytes,2,opt,name=backup_id,json=backupId,proto3" json:"backup_id,omitempty"`
	DatabaseName      string `protobuf:"bytes,3,opt,name=database_name,json=databaseName,proto3" json:"database_name,omitempty"`
	DatabaseEndpoint  string `protobuf:"bytes,4,opt,name=database_endpoint,json=databaseEndpoint,proto3" json:"database_endpoint,omitempty"`
	DestinationPrefix string `protobuf:"bytes,5,opt,name=destination_prefix,json=destinationPrefix,proto3" json:"destination_prefix,omitempty"`
	// Paths to s3 objects to restore.
	SourcePaths []string `protobuf:"bytes,6,rep,name=source_paths,json=sourcePaths,proto3" json:"source_paths,omitempty"`
}

func (x *MakeRestoreRequest) Reset() {
	*x = MakeRestoreRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ydbcp_v1alpha1_backup_service_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MakeRestoreRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MakeRestoreRequest) ProtoMessage() {}

func (x *MakeRestoreRequest) ProtoReflect() protoreflect.Message {
	mi := &file_ydbcp_v1alpha1_backup_service_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MakeRestoreRequest.ProtoReflect.Descriptor instead.
func (*MakeRestoreRequest) Descriptor() ([]byte, []int) {
	return file_ydbcp_v1alpha1_backup_service_proto_rawDescGZIP(), []int{5}
}

func (x *MakeRestoreRequest) GetContainerId() string {
	if x != nil {
		return x.ContainerId
	}
	return ""
}

func (x *MakeRestoreRequest) GetBackupId() string {
	if x != nil {
		return x.BackupId
	}
	return ""
}

func (x *MakeRestoreRequest) GetDatabaseName() string {
	if x != nil {
		return x.DatabaseName
	}
	return ""
}

func (x *MakeRestoreRequest) GetDatabaseEndpoint() string {
	if x != nil {
		return x.DatabaseEndpoint
	}
	return ""
}

func (x *MakeRestoreRequest) GetDestinationPrefix() string {
	if x != nil {
		return x.DestinationPrefix
	}
	return ""
}

func (x *MakeRestoreRequest) GetSourcePaths() []string {
	if x != nil {
		return x.SourcePaths
	}
	return nil
}

var File_ydbcp_v1alpha1_backup_service_proto protoreflect.FileDescriptor

var file_ydbcp_v1alpha1_backup_service_proto_rawDesc = []byte{
	0x0a, 0x23, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2f, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31,
	0x2f, 0x62, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x5f, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0e, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2e, 0x76, 0x31, 0x61,
	0x6c, 0x70, 0x68, 0x61, 0x31, 0x1a, 0x1b, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2f, 0x76, 0x31, 0x61,
	0x6c, 0x70, 0x68, 0x61, 0x31, 0x2f, 0x62, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x1a, 0x1e, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2f, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68,
	0x61, 0x31, 0x2f, 0x6f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x22, 0xa3, 0x01, 0x0a, 0x12, 0x4c, 0x69, 0x73, 0x74, 0x42, 0x61, 0x63, 0x6b, 0x75,
	0x70, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x21, 0x0a, 0x0c, 0x63, 0x6f, 0x6e,
	0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0b, 0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x49, 0x64, 0x12, 0x2c, 0x0a, 0x12,
	0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x5f, 0x6d, 0x61,
	0x73, 0x6b, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x10, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61,
	0x73, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x4d, 0x61, 0x73, 0x6b, 0x12, 0x1c, 0x0a, 0x09, 0x70, 0x61,
	0x67, 0x65, 0x5f, 0x73, 0x69, 0x7a, 0x65, 0x18, 0xe8, 0x07, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x08,
	0x70, 0x61, 0x67, 0x65, 0x53, 0x69, 0x7a, 0x65, 0x12, 0x1e, 0x0a, 0x0a, 0x70, 0x61, 0x67, 0x65,
	0x5f, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x18, 0xe9, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x70,
	0x61, 0x67, 0x65, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x22, 0x6f, 0x0a, 0x13, 0x4c, 0x69, 0x73, 0x74,
	0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12,
	0x30, 0x0a, 0x07, 0x62, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x16, 0x2e, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2e, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61,
	0x31, 0x2e, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x52, 0x07, 0x62, 0x61, 0x63, 0x6b, 0x75, 0x70,
	0x73, 0x12, 0x26, 0x0a, 0x0f, 0x6e, 0x65, 0x78, 0x74, 0x5f, 0x70, 0x61, 0x67, 0x65, 0x5f, 0x74,
	0x6f, 0x6b, 0x65, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x6e, 0x65, 0x78, 0x74,
	0x50, 0x61, 0x67, 0x65, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x22, 0x22, 0x0a, 0x10, 0x47, 0x65, 0x74,
	0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x0e, 0x0a,
	0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x22, 0xe2, 0x01,
	0x0a, 0x11, 0x4d, 0x61, 0x6b, 0x65, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x12, 0x21, 0x0a, 0x0c, 0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72,
	0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x63, 0x6f, 0x6e, 0x74, 0x61,
	0x69, 0x6e, 0x65, 0x72, 0x49, 0x64, 0x12, 0x23, 0x0a, 0x0d, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61,
	0x73, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x64,
	0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x2b, 0x0a, 0x11, 0x64,
	0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x5f, 0x65, 0x6e, 0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x10, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65,
	0x45, 0x6e, 0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x12, 0x21, 0x0a, 0x0c, 0x73, 0x6f, 0x75, 0x72,
	0x63, 0x65, 0x5f, 0x70, 0x61, 0x74, 0x68, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x09, 0x52, 0x0b,
	0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x50, 0x61, 0x74, 0x68, 0x73, 0x12, 0x35, 0x0a, 0x17, 0x73,
	0x6f, 0x75, 0x72, 0x63, 0x65, 0x5f, 0x70, 0x61, 0x74, 0x68, 0x73, 0x5f, 0x74, 0x6f, 0x5f, 0x65,
	0x78, 0x63, 0x6c, 0x75, 0x64, 0x65, 0x18, 0x05, 0x20, 0x03, 0x28, 0x09, 0x52, 0x14, 0x73, 0x6f,
	0x75, 0x72, 0x63, 0x65, 0x50, 0x61, 0x74, 0x68, 0x73, 0x54, 0x6f, 0x45, 0x78, 0x63, 0x6c, 0x75,
	0x64, 0x65, 0x22, 0x32, 0x0a, 0x13, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x42, 0x61, 0x63, 0x6b,
	0x75, 0x70, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1b, 0x0a, 0x09, 0x62, 0x61, 0x63,
	0x6b, 0x75, 0x70, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x62, 0x61,
	0x63, 0x6b, 0x75, 0x70, 0x49, 0x64, 0x22, 0xf8, 0x01, 0x0a, 0x12, 0x4d, 0x61, 0x6b, 0x65, 0x52,
	0x65, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x21, 0x0a,
	0x0c, 0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x0b, 0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x49, 0x64,
	0x12, 0x1b, 0x0a, 0x09, 0x62, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x08, 0x62, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x49, 0x64, 0x12, 0x23, 0x0a,
	0x0d, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x4e, 0x61,
	0x6d, 0x65, 0x12, 0x2b, 0x0a, 0x11, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x5f, 0x65,
	0x6e, 0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x10, 0x64,
	0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x45, 0x6e, 0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x12,
	0x2d, 0x0a, 0x12, 0x64, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x70,
	0x72, 0x65, 0x66, 0x69, 0x78, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x11, 0x64, 0x65, 0x73,
	0x74, 0x69, 0x6e, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x50, 0x72, 0x65, 0x66, 0x69, 0x78, 0x12, 0x21,
	0x0a, 0x0c, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x5f, 0x70, 0x61, 0x74, 0x68, 0x73, 0x18, 0x06,
	0x20, 0x03, 0x28, 0x09, 0x52, 0x0b, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x50, 0x61, 0x74, 0x68,
	0x73, 0x32, 0x98, 0x03, 0x0a, 0x0d, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x53, 0x65, 0x72, 0x76,
	0x69, 0x63, 0x65, 0x12, 0x56, 0x0a, 0x0b, 0x4c, 0x69, 0x73, 0x74, 0x42, 0x61, 0x63, 0x6b, 0x75,
	0x70, 0x73, 0x12, 0x22, 0x2e, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2e, 0x76, 0x31, 0x61, 0x6c, 0x70,
	0x68, 0x61, 0x31, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x73, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x23, 0x2e, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2e, 0x76,
	0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x42, 0x61, 0x63, 0x6b,
	0x75, 0x70, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x45, 0x0a, 0x09, 0x47,
	0x65, 0x74, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x12, 0x20, 0x2e, 0x79, 0x64, 0x62, 0x63, 0x70,
	0x2e, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31, 0x2e, 0x47, 0x65, 0x74, 0x42, 0x61, 0x63,
	0x6b, 0x75, 0x70, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x16, 0x2e, 0x79, 0x64, 0x62,
	0x63, 0x70, 0x2e, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31, 0x2e, 0x42, 0x61, 0x63, 0x6b,
	0x75, 0x70, 0x12, 0x4a, 0x0a, 0x0a, 0x4d, 0x61, 0x6b, 0x65, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70,
	0x12, 0x21, 0x2e, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2e, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61,
	0x31, 0x2e, 0x4d, 0x61, 0x6b, 0x65, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x1a, 0x19, 0x2e, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2e, 0x76, 0x31, 0x61, 0x6c,
	0x70, 0x68, 0x61, 0x31, 0x2e, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x4e,
	0x0a, 0x0c, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x12, 0x23,
	0x2e, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2e, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31, 0x2e,
	0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x1a, 0x19, 0x2e, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2e, 0x76, 0x31, 0x61, 0x6c,
	0x70, 0x68, 0x61, 0x31, 0x2e, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x4c,
	0x0a, 0x0b, 0x4d, 0x61, 0x6b, 0x65, 0x52, 0x65, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x12, 0x22, 0x2e,
	0x79, 0x64, 0x62, 0x63, 0x70, 0x2e, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31, 0x2e, 0x4d,
	0x61, 0x6b, 0x65, 0x52, 0x65, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x19, 0x2e, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2e, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68,
	0x61, 0x31, 0x2e, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x42, 0x2f, 0x5a, 0x2d,
	0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x79, 0x64, 0x62, 0x2d, 0x70,
	0x6c, 0x61, 0x74, 0x66, 0x6f, 0x72, 0x6d, 0x2f, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2f, 0x70, 0x6b,
	0x67, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x3b, 0x79, 0x64, 0x62, 0x63, 0x70, 0x62, 0x06, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_ydbcp_v1alpha1_backup_service_proto_rawDescOnce sync.Once
	file_ydbcp_v1alpha1_backup_service_proto_rawDescData = file_ydbcp_v1alpha1_backup_service_proto_rawDesc
)

func file_ydbcp_v1alpha1_backup_service_proto_rawDescGZIP() []byte {
	file_ydbcp_v1alpha1_backup_service_proto_rawDescOnce.Do(func() {
		file_ydbcp_v1alpha1_backup_service_proto_rawDescData = protoimpl.X.CompressGZIP(file_ydbcp_v1alpha1_backup_service_proto_rawDescData)
	})
	return file_ydbcp_v1alpha1_backup_service_proto_rawDescData
}

var file_ydbcp_v1alpha1_backup_service_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_ydbcp_v1alpha1_backup_service_proto_goTypes = []interface{}{
	(*ListBackupsRequest)(nil),  // 0: ydbcp.v1alpha1.ListBackupsRequest
	(*ListBackupsResponse)(nil), // 1: ydbcp.v1alpha1.ListBackupsResponse
	(*GetBackupRequest)(nil),    // 2: ydbcp.v1alpha1.GetBackupRequest
	(*MakeBackupRequest)(nil),   // 3: ydbcp.v1alpha1.MakeBackupRequest
	(*DeleteBackupRequest)(nil), // 4: ydbcp.v1alpha1.DeleteBackupRequest
	(*MakeRestoreRequest)(nil),  // 5: ydbcp.v1alpha1.MakeRestoreRequest
	(*Backup)(nil),              // 6: ydbcp.v1alpha1.Backup
	(*Operation)(nil),           // 7: ydbcp.v1alpha1.Operation
}
var file_ydbcp_v1alpha1_backup_service_proto_depIdxs = []int32{
	6, // 0: ydbcp.v1alpha1.ListBackupsResponse.backups:type_name -> ydbcp.v1alpha1.Backup
	0, // 1: ydbcp.v1alpha1.BackupService.ListBackups:input_type -> ydbcp.v1alpha1.ListBackupsRequest
	2, // 2: ydbcp.v1alpha1.BackupService.GetBackup:input_type -> ydbcp.v1alpha1.GetBackupRequest
	3, // 3: ydbcp.v1alpha1.BackupService.MakeBackup:input_type -> ydbcp.v1alpha1.MakeBackupRequest
	4, // 4: ydbcp.v1alpha1.BackupService.DeleteBackup:input_type -> ydbcp.v1alpha1.DeleteBackupRequest
	5, // 5: ydbcp.v1alpha1.BackupService.MakeRestore:input_type -> ydbcp.v1alpha1.MakeRestoreRequest
	1, // 6: ydbcp.v1alpha1.BackupService.ListBackups:output_type -> ydbcp.v1alpha1.ListBackupsResponse
	6, // 7: ydbcp.v1alpha1.BackupService.GetBackup:output_type -> ydbcp.v1alpha1.Backup
	7, // 8: ydbcp.v1alpha1.BackupService.MakeBackup:output_type -> ydbcp.v1alpha1.Operation
	7, // 9: ydbcp.v1alpha1.BackupService.DeleteBackup:output_type -> ydbcp.v1alpha1.Operation
	7, // 10: ydbcp.v1alpha1.BackupService.MakeRestore:output_type -> ydbcp.v1alpha1.Operation
	6, // [6:11] is the sub-list for method output_type
	1, // [1:6] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_ydbcp_v1alpha1_backup_service_proto_init() }
func file_ydbcp_v1alpha1_backup_service_proto_init() {
	if File_ydbcp_v1alpha1_backup_service_proto != nil {
		return
	}
	file_ydbcp_v1alpha1_backup_proto_init()
	file_ydbcp_v1alpha1_operation_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_ydbcp_v1alpha1_backup_service_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListBackupsRequest); i {
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
		file_ydbcp_v1alpha1_backup_service_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListBackupsResponse); i {
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
		file_ydbcp_v1alpha1_backup_service_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetBackupRequest); i {
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
		file_ydbcp_v1alpha1_backup_service_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MakeBackupRequest); i {
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
		file_ydbcp_v1alpha1_backup_service_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeleteBackupRequest); i {
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
		file_ydbcp_v1alpha1_backup_service_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MakeRestoreRequest); i {
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
			RawDescriptor: file_ydbcp_v1alpha1_backup_service_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_ydbcp_v1alpha1_backup_service_proto_goTypes,
		DependencyIndexes: file_ydbcp_v1alpha1_backup_service_proto_depIdxs,
		MessageInfos:      file_ydbcp_v1alpha1_backup_service_proto_msgTypes,
	}.Build()
	File_ydbcp_v1alpha1_backup_service_proto = out.File
	file_ydbcp_v1alpha1_backup_service_proto_rawDesc = nil
	file_ydbcp_v1alpha1_backup_service_proto_goTypes = nil
	file_ydbcp_v1alpha1_backup_service_proto_depIdxs = nil
}
