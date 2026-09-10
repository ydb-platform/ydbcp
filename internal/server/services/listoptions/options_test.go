package listoptions

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/ydb-platform/ydbcp/pkg/proto/ydbcp/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"ydbcp/internal/connectors/db"
)

func TestPublicPaginationAndOrder(t *testing.T) {
	page, err := Page(0, "")
	require.NoError(t, err)
	assert.Equal(t, &db.Page{Limit: 50}, page)
	page, err = Page(7, "14")
	require.NoError(t, err)
	assert.Equal(t, &db.Page{Limit: 7, Offset: 14}, page)
	for _, token := range []string{"x", "-1", "18446744073709551616"} {
		_, err := Page(1, token)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	}
	order, err := BackupOrder(nil)
	require.NoError(t, err)
	assert.Equal(t, &db.BackupOrder{Field: db.BackupOrderCreatedAt, Desc: true}, order)
	fields := map[pb.BackupField]db.BackupOrderField{
		pb.BackupField_DATABASE_NAME: db.BackupOrderDatabaseName, pb.BackupField_STATUS: db.BackupOrderStatus,
		pb.BackupField_CREATED_AT: db.BackupOrderCreatedAt, pb.BackupField_EXPIRE_AT: db.BackupOrderExpireAt,
		pb.BackupField_COMPLETED_AT: db.BackupOrderCompletedAt,
	}
	for in, want := range fields {
		got, err := BackupOrder(&pb.ListBackupsOrder{Field: in, Desc: true})
		require.NoError(t, err)
		assert.Equal(t, &db.BackupOrder{Field: want, Desc: true}, got)
	}
	_, err = BackupOrder(&pb.ListBackupsOrder{Field: pb.BackupField(999)})
	assert.Equal(t, codes.Internal, status.Code(err))
}
func TestDateRangePreservesOpenAndInclusiveBounds(t *testing.T) {
	assert.Equal(t, db.TimeRange{}, DateRange(nil))
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	got := DateRange(&pb.DateRange{To: timestamppb.New(now)})
	assert.Nil(t, got.From)
	assert.Equal(t, &now, got.To)
}
