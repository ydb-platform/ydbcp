package listoptions

import (
	"fmt"
	"strconv"

	pb "github.com/ydb-platform/ydbcp/pkg/proto/ydbcp/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"ydbcp/internal/connectors/db"
)

// Page preserves the public API's decimal offset tokens and default page size.
func Page(size uint32, token string) (*db.Page, error) {
	p := &db.Page{Limit: uint64(size)}
	if p.Limit == 0 {
		p.Limit = 50
	}
	if token != "" {
		offset, err := strconv.ParseUint(token, 10, 64)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "can't parse page token")
		}
		p.Offset = offset
	}
	return p, nil
}

func BackupOrder(order *pb.ListBackupsOrder) (*db.BackupOrder, error) {
	if order == nil {
		return &db.BackupOrder{Field: db.BackupOrderCreatedAt, Desc: true}, nil
	}
	result := &db.BackupOrder{Desc: order.GetDesc()}
	switch order.GetField() {
	case pb.BackupField_DATABASE_NAME:
		result.Field = db.BackupOrderDatabaseName
	case pb.BackupField_STATUS:
		result.Field = db.BackupOrderStatus
	case pb.BackupField_CREATED_AT:
		result.Field = db.BackupOrderCreatedAt
	case pb.BackupField_EXPIRE_AT:
		result.Field = db.BackupOrderExpireAt
	case pb.BackupField_COMPLETED_AT:
		result.Field = db.BackupOrderCompletedAt
	default:
		return nil, status.Error(codes.Internal, fmt.Sprintf("internal error: did not expect pb.BackupField_%s", order.GetField().String()))
	}
	return result, nil
}

func DateRange(r *pb.DateRange) db.TimeRange {
	var result db.TimeRange
	if r.GetFrom() != nil {
		t := r.GetFrom().AsTime()
		result.From = &t
	}
	if r.GetTo() != nil {
		t := r.GetTo().AsTime()
		result.To = &t
	}
	return result
}
