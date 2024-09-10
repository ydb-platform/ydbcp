// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v4.25.1
// source: ydbcp/v1alpha1/backup_service.proto

package ydbcp

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	BackupService_ListBackups_FullMethodName  = "/ydbcp.v1alpha1.BackupService/ListBackups"
	BackupService_GetBackup_FullMethodName    = "/ydbcp.v1alpha1.BackupService/GetBackup"
	BackupService_MakeBackup_FullMethodName   = "/ydbcp.v1alpha1.BackupService/MakeBackup"
	BackupService_DeleteBackup_FullMethodName = "/ydbcp.v1alpha1.BackupService/DeleteBackup"
	BackupService_MakeRestore_FullMethodName  = "/ydbcp.v1alpha1.BackupService/MakeRestore"
)

// BackupServiceClient is the client API for BackupService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type BackupServiceClient interface {
	ListBackups(ctx context.Context, in *ListBackupsRequest, opts ...grpc.CallOption) (*ListBackupsResponse, error)
	// Returns the specified backup.
	GetBackup(ctx context.Context, in *GetBackupRequest, opts ...grpc.CallOption) (*Backup, error)
	MakeBackup(ctx context.Context, in *MakeBackupRequest, opts ...grpc.CallOption) (*Operation, error)
	DeleteBackup(ctx context.Context, in *DeleteBackupRequest, opts ...grpc.CallOption) (*Operation, error)
	MakeRestore(ctx context.Context, in *MakeRestoreRequest, opts ...grpc.CallOption) (*Operation, error)
}

type backupServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewBackupServiceClient(cc grpc.ClientConnInterface) BackupServiceClient {
	return &backupServiceClient{cc}
}

func (c *backupServiceClient) ListBackups(ctx context.Context, in *ListBackupsRequest, opts ...grpc.CallOption) (*ListBackupsResponse, error) {
	out := new(ListBackupsResponse)
	err := c.cc.Invoke(ctx, BackupService_ListBackups_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *backupServiceClient) GetBackup(ctx context.Context, in *GetBackupRequest, opts ...grpc.CallOption) (*Backup, error) {
	out := new(Backup)
	err := c.cc.Invoke(ctx, BackupService_GetBackup_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *backupServiceClient) MakeBackup(ctx context.Context, in *MakeBackupRequest, opts ...grpc.CallOption) (*Operation, error) {
	out := new(Operation)
	err := c.cc.Invoke(ctx, BackupService_MakeBackup_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *backupServiceClient) DeleteBackup(ctx context.Context, in *DeleteBackupRequest, opts ...grpc.CallOption) (*Operation, error) {
	out := new(Operation)
	err := c.cc.Invoke(ctx, BackupService_DeleteBackup_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *backupServiceClient) MakeRestore(ctx context.Context, in *MakeRestoreRequest, opts ...grpc.CallOption) (*Operation, error) {
	out := new(Operation)
	err := c.cc.Invoke(ctx, BackupService_MakeRestore_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BackupServiceServer is the server API for BackupService service.
// All implementations must embed UnimplementedBackupServiceServer
// for forward compatibility
type BackupServiceServer interface {
	ListBackups(context.Context, *ListBackupsRequest) (*ListBackupsResponse, error)
	// Returns the specified backup.
	GetBackup(context.Context, *GetBackupRequest) (*Backup, error)
	MakeBackup(context.Context, *MakeBackupRequest) (*Operation, error)
	DeleteBackup(context.Context, *DeleteBackupRequest) (*Operation, error)
	MakeRestore(context.Context, *MakeRestoreRequest) (*Operation, error)
	mustEmbedUnimplementedBackupServiceServer()
}

// UnimplementedBackupServiceServer must be embedded to have forward compatible implementations.
type UnimplementedBackupServiceServer struct {
}

func (UnimplementedBackupServiceServer) ListBackups(context.Context, *ListBackupsRequest) (*ListBackupsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListBackups not implemented")
}
func (UnimplementedBackupServiceServer) GetBackup(context.Context, *GetBackupRequest) (*Backup, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetBackup not implemented")
}
func (UnimplementedBackupServiceServer) MakeBackup(context.Context, *MakeBackupRequest) (*Operation, error) {
	return nil, status.Errorf(codes.Unimplemented, "method MakeBackup not implemented")
}
func (UnimplementedBackupServiceServer) DeleteBackup(context.Context, *DeleteBackupRequest) (*Operation, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteBackup not implemented")
}
func (UnimplementedBackupServiceServer) MakeRestore(context.Context, *MakeRestoreRequest) (*Operation, error) {
	return nil, status.Errorf(codes.Unimplemented, "method MakeRestore not implemented")
}
func (UnimplementedBackupServiceServer) mustEmbedUnimplementedBackupServiceServer() {}

// UnsafeBackupServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to BackupServiceServer will
// result in compilation errors.
type UnsafeBackupServiceServer interface {
	mustEmbedUnimplementedBackupServiceServer()
}

func RegisterBackupServiceServer(s grpc.ServiceRegistrar, srv BackupServiceServer) {
	s.RegisterService(&BackupService_ServiceDesc, srv)
}

func _BackupService_ListBackups_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListBackupsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BackupServiceServer).ListBackups(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BackupService_ListBackups_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BackupServiceServer).ListBackups(ctx, req.(*ListBackupsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BackupService_GetBackup_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetBackupRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BackupServiceServer).GetBackup(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BackupService_GetBackup_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BackupServiceServer).GetBackup(ctx, req.(*GetBackupRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BackupService_MakeBackup_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MakeBackupRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BackupServiceServer).MakeBackup(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BackupService_MakeBackup_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BackupServiceServer).MakeBackup(ctx, req.(*MakeBackupRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BackupService_DeleteBackup_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteBackupRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BackupServiceServer).DeleteBackup(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BackupService_DeleteBackup_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BackupServiceServer).DeleteBackup(ctx, req.(*DeleteBackupRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BackupService_MakeRestore_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MakeRestoreRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BackupServiceServer).MakeRestore(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BackupService_MakeRestore_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BackupServiceServer).MakeRestore(ctx, req.(*MakeRestoreRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// BackupService_ServiceDesc is the grpc.ServiceDesc for BackupService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var BackupService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "ydbcp.v1alpha1.BackupService",
	HandlerType: (*BackupServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ListBackups",
			Handler:    _BackupService_ListBackups_Handler,
		},
		{
			MethodName: "GetBackup",
			Handler:    _BackupService_GetBackup_Handler,
		},
		{
			MethodName: "MakeBackup",
			Handler:    _BackupService_MakeBackup_Handler,
		},
		{
			MethodName: "DeleteBackup",
			Handler:    _BackupService_DeleteBackup_Handler,
		},
		{
			MethodName: "MakeRestore",
			Handler:    _BackupService_MakeRestore_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "ydbcp/v1alpha1/backup_service.proto",
}
