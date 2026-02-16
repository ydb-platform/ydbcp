package auth

import (
	"testing"
	pb "ydbcp/plugins/auth_nebius/proto/ydb_nebius"

	"github.com/stretchr/testify/require"
)

func TestDBPathFromConnectionString(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		path, err := dbPathFromConnectionString("grpcs://localhost:2135/domain/database")
		require.NoError(t, err)
		require.Equal(t, "/domain/database", path)
	})

	t.Run("empty string", func(t *testing.T) {
		_, err := dbPathFromConnectionString("")
		require.Error(t, err)
	})

	t.Run("no path", func(t *testing.T) {
		_, err := dbPathFromConnectionString("grpcs://localhost:2135")
		require.Error(t, err)
	})
}

func TestContainerIDFromDescribeResponse(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		status := pb.TStatusIds_SUCCESS
		key := ydbcpContainerIDAttrName
		value := "container-id"
		resp := &pb.TResponse{
			StatusCode: &status,
			PathDescription: &pb.TPathDescription{
				UserAttributes: []*pb.TUserAttribute{
					{
						Key:   &key,
						Value: &value,
					},
				},
			},
		}

		id, err := containerIDFromDescribeResponse(resp)
		require.NoError(t, err)
		require.Equal(t, "container-id", id)
	})

	t.Run("non success status", func(t *testing.T) {
		status := pb.TStatusIds_BAD_REQUEST
		reason := "bad request"
		resp := &pb.TResponse{
			StatusCode:  &status,
			ErrorReason: &reason,
		}

		_, err := containerIDFromDescribeResponse(resp)
		require.Error(t, err)
	})

	t.Run("missing attribute", func(t *testing.T) {
		status := pb.TStatusIds_SUCCESS
		key := "Other"
		value := "value"
		resp := &pb.TResponse{
			StatusCode: &status,
			PathDescription: &pb.TPathDescription{
				UserAttributes: []*pb.TUserAttribute{
					{
						Key:   &key,
						Value: &value,
					},
				},
			},
		}

		_, err := containerIDFromDescribeResponse(resp)
		require.Error(t, err)
	})

	t.Run("empty attribute value", func(t *testing.T) {
		status := pb.TStatusIds_SUCCESS
		key := ydbcpContainerIDAttrName
		value := "   "
		resp := &pb.TResponse{
			StatusCode: &status,
			PathDescription: &pb.TPathDescription{
				UserAttributes: []*pb.TUserAttribute{
					{
						Key:   &key,
						Value: &value,
					},
				},
			},
		}

		_, err := containerIDFromDescribeResponse(resp)
		require.Error(t, err)
	})
}
