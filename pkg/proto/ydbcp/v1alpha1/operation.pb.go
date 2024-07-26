// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.32.0
// 	protoc        v5.27.1
// source: ydbcp/v1alpha1/operation.proto

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
	return file_ydbcp_v1alpha1_operation_proto_enumTypes[0].Descriptor()
}

func (Operation_Status) Type() protoreflect.EnumType {
	return &file_ydbcp_v1alpha1_operation_proto_enumTypes[0]
}

func (x Operation_Status) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Operation_Status.Descriptor instead.
func (Operation_Status) EnumDescriptor() ([]byte, []int) {
	return file_ydbcp_v1alpha1_operation_proto_rawDescGZIP(), []int{0, 0}
}

type Operation struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id                   string           `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	ContainerId          string           `protobuf:"bytes,2,opt,name=container_id,json=containerId,proto3" json:"container_id,omitempty"`
	Type                 string           `protobuf:"bytes,3,opt,name=type,proto3" json:"type,omitempty"`
	DatabaseName         string           `protobuf:"bytes,4,opt,name=database_name,json=databaseName,proto3" json:"database_name,omitempty"`
	YdbServerOperationId string           `protobuf:"bytes,5,opt,name=ydb_server_operation_id,json=ydbServerOperationId,proto3" json:"ydb_server_operation_id,omitempty"`
	BackupId             string           `protobuf:"bytes,6,opt,name=backup_id,json=backupId,proto3" json:"backup_id,omitempty"`
	SourcePaths          []string         `protobuf:"bytes,7,rep,name=source_paths,json=sourcePaths,proto3" json:"source_paths,omitempty"`                                // [(size) = "<=256"];
	SourcePathsToExclude []string         `protobuf:"bytes,8,rep,name=source_paths_to_exclude,json=sourcePathsToExclude,proto3" json:"source_paths_to_exclude,omitempty"` // [(size) = "<=256"];
	RestorePaths         []string         `protobuf:"bytes,9,rep,name=restore_paths,json=restorePaths,proto3" json:"restore_paths,omitempty"`                             // [(size) = "<=256"];
	Audit                *AuditInfo       `protobuf:"bytes,10,opt,name=audit,proto3" json:"audit,omitempty"`
	Status               Operation_Status `protobuf:"varint,11,opt,name=status,proto3,enum=ydbcp.v1alpha1.Operation_Status" json:"status,omitempty"`
	Message              string           `protobuf:"bytes,12,opt,name=message,proto3" json:"message,omitempty"`
}

func (x *Operation) Reset() {
	*x = Operation{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ydbcp_v1alpha1_operation_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Operation) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Operation) ProtoMessage() {}

func (x *Operation) ProtoReflect() protoreflect.Message {
	mi := &file_ydbcp_v1alpha1_operation_proto_msgTypes[0]
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
	return file_ydbcp_v1alpha1_operation_proto_rawDescGZIP(), []int{0}
}

func (x *Operation) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Operation) GetContainerId() string {
	if x != nil {
		return x.ContainerId
	}
	return ""
}

func (x *Operation) GetType() string {
	if x != nil {
		return x.Type
	}
	return ""
}

func (x *Operation) GetDatabaseName() string {
	if x != nil {
		return x.DatabaseName
	}
	return ""
}

func (x *Operation) GetYdbServerOperationId() string {
	if x != nil {
		return x.YdbServerOperationId
	}
	return ""
}

func (x *Operation) GetBackupId() string {
	if x != nil {
		return x.BackupId
	}
	return ""
}

func (x *Operation) GetSourcePaths() []string {
	if x != nil {
		return x.SourcePaths
	}
	return nil
}

func (x *Operation) GetSourcePathsToExclude() []string {
	if x != nil {
		return x.SourcePathsToExclude
	}
	return nil
}

func (x *Operation) GetRestorePaths() []string {
	if x != nil {
		return x.RestorePaths
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

var File_ydbcp_v1alpha1_operation_proto protoreflect.FileDescriptor

var file_ydbcp_v1alpha1_operation_proto_rawDesc = []byte{
	0x0a, 0x1e, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2f, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31,
	0x2f, 0x6f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x0e, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2e, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31,
	0x1a, 0x1b, 0x79, 0x64, 0x62, 0x63, 0x70, 0x2f, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31,
	0x2f, 0x62, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xb1, 0x04,
	0x0a, 0x09, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x0e, 0x0a, 0x02, 0x69,
	0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x21, 0x0a, 0x0c, 0x63,
	0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x0b, 0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x49, 0x64, 0x12, 0x12,
	0x0a, 0x04, 0x74, 0x79, 0x70, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x74, 0x79,
	0x70, 0x65, 0x12, 0x23, 0x0a, 0x0d, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x5f, 0x6e,
	0x61, 0x6d, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x64, 0x61, 0x74, 0x61, 0x62,
	0x61, 0x73, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x35, 0x0a, 0x17, 0x79, 0x64, 0x62, 0x5f, 0x73,
	0x65, 0x72, 0x76, 0x65, 0x72, 0x5f, 0x6f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x5f,
	0x69, 0x64, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x14, 0x79, 0x64, 0x62, 0x53, 0x65, 0x72,
	0x76, 0x65, 0x72, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x12, 0x1b,
	0x0a, 0x09, 0x62, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x5f, 0x69, 0x64, 0x18, 0x06, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x08, 0x62, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x49, 0x64, 0x12, 0x21, 0x0a, 0x0c, 0x73,
	0x6f, 0x75, 0x72, 0x63, 0x65, 0x5f, 0x70, 0x61, 0x74, 0x68, 0x73, 0x18, 0x07, 0x20, 0x03, 0x28,
	0x09, 0x52, 0x0b, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x50, 0x61, 0x74, 0x68, 0x73, 0x12, 0x35,
	0x0a, 0x17, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x5f, 0x70, 0x61, 0x74, 0x68, 0x73, 0x5f, 0x74,
	0x6f, 0x5f, 0x65, 0x78, 0x63, 0x6c, 0x75, 0x64, 0x65, 0x18, 0x08, 0x20, 0x03, 0x28, 0x09, 0x52,
	0x14, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x50, 0x61, 0x74, 0x68, 0x73, 0x54, 0x6f, 0x45, 0x78,
	0x63, 0x6c, 0x75, 0x64, 0x65, 0x12, 0x23, 0x0a, 0x0d, 0x72, 0x65, 0x73, 0x74, 0x6f, 0x72, 0x65,
	0x5f, 0x70, 0x61, 0x74, 0x68, 0x73, 0x18, 0x09, 0x20, 0x03, 0x28, 0x09, 0x52, 0x0c, 0x72, 0x65,
	0x73, 0x74, 0x6f, 0x72, 0x65, 0x50, 0x61, 0x74, 0x68, 0x73, 0x12, 0x2f, 0x0a, 0x05, 0x61, 0x75,
	0x64, 0x69, 0x74, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x79, 0x64, 0x62, 0x63,
	0x70, 0x2e, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31, 0x2e, 0x41, 0x75, 0x64, 0x69, 0x74,
	0x49, 0x6e, 0x66, 0x6f, 0x52, 0x05, 0x61, 0x75, 0x64, 0x69, 0x74, 0x12, 0x38, 0x0a, 0x06, 0x73,
	0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x0b, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x20, 0x2e, 0x79, 0x64,
	0x62, 0x63, 0x70, 0x2e, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31, 0x2e, 0x4f, 0x70, 0x65,
	0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x52, 0x06, 0x73,
	0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x18, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65,
	0x18, 0x0c, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x22,
	0x60, 0x0a, 0x06, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x16, 0x0a, 0x12, 0x53, 0x54, 0x41,
	0x54, 0x55, 0x53, 0x5f, 0x55, 0x4e, 0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49, 0x45, 0x44, 0x10,
	0x00, 0x12, 0x0b, 0x0a, 0x07, 0x50, 0x45, 0x4e, 0x44, 0x49, 0x4e, 0x47, 0x10, 0x01, 0x12, 0x08,
	0x0a, 0x04, 0x44, 0x4f, 0x4e, 0x45, 0x10, 0x02, 0x12, 0x09, 0x0a, 0x05, 0x45, 0x52, 0x52, 0x4f,
	0x52, 0x10, 0x03, 0x12, 0x0e, 0x0a, 0x0a, 0x43, 0x41, 0x4e, 0x43, 0x45, 0x4c, 0x4c, 0x49, 0x4e,
	0x47, 0x10, 0x04, 0x12, 0x0c, 0x0a, 0x08, 0x43, 0x41, 0x4e, 0x43, 0x45, 0x4c, 0x45, 0x44, 0x10,
	0x05, 0x42, 0x2f, 0x5a, 0x2d, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f,
	0x79, 0x64, 0x62, 0x2d, 0x70, 0x6c, 0x61, 0x74, 0x66, 0x6f, 0x72, 0x6d, 0x2f, 0x79, 0x64, 0x62,
	0x63, 0x70, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x3b, 0x79, 0x64, 0x62,
	0x63, 0x70, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_ydbcp_v1alpha1_operation_proto_rawDescOnce sync.Once
	file_ydbcp_v1alpha1_operation_proto_rawDescData = file_ydbcp_v1alpha1_operation_proto_rawDesc
)

func file_ydbcp_v1alpha1_operation_proto_rawDescGZIP() []byte {
	file_ydbcp_v1alpha1_operation_proto_rawDescOnce.Do(func() {
		file_ydbcp_v1alpha1_operation_proto_rawDescData = protoimpl.X.CompressGZIP(file_ydbcp_v1alpha1_operation_proto_rawDescData)
	})
	return file_ydbcp_v1alpha1_operation_proto_rawDescData
}

var file_ydbcp_v1alpha1_operation_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_ydbcp_v1alpha1_operation_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_ydbcp_v1alpha1_operation_proto_goTypes = []interface{}{
	(Operation_Status)(0), // 0: ydbcp.v1alpha1.Operation.Status
	(*Operation)(nil),     // 1: ydbcp.v1alpha1.Operation
	(*AuditInfo)(nil),     // 2: ydbcp.v1alpha1.AuditInfo
}
var file_ydbcp_v1alpha1_operation_proto_depIdxs = []int32{
	2, // 0: ydbcp.v1alpha1.Operation.audit:type_name -> ydbcp.v1alpha1.AuditInfo
	0, // 1: ydbcp.v1alpha1.Operation.status:type_name -> ydbcp.v1alpha1.Operation.Status
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_ydbcp_v1alpha1_operation_proto_init() }
func file_ydbcp_v1alpha1_operation_proto_init() {
	if File_ydbcp_v1alpha1_operation_proto != nil {
		return
	}
	file_ydbcp_v1alpha1_backup_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_ydbcp_v1alpha1_operation_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
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
			RawDescriptor: file_ydbcp_v1alpha1_operation_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_ydbcp_v1alpha1_operation_proto_goTypes,
		DependencyIndexes: file_ydbcp_v1alpha1_operation_proto_depIdxs,
		EnumInfos:         file_ydbcp_v1alpha1_operation_proto_enumTypes,
		MessageInfos:      file_ydbcp_v1alpha1_operation_proto_msgTypes,
	}.Build()
	File_ydbcp_v1alpha1_operation_proto = out.File
	file_ydbcp_v1alpha1_operation_proto_rawDesc = nil
	file_ydbcp_v1alpha1_operation_proto_goTypes = nil
	file_ydbcp_v1alpha1_operation_proto_depIdxs = nil
}
