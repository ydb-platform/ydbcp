package auth

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	pb "ydbcp/plugins/auth_nebius/proto/ydb_nebius"

	"google.golang.org/grpc"
)

const (
	ydbcpContainerIDAttrName = "folder_id"
	describeTimeout          = 5 * time.Second
)

func DiscoverYDBCPContainerID(
	ctx context.Context,
	conn grpc.ClientConnInterface,
	connectionString string,
	securityToken string,
) (string, error) {
	path, err := dbPathFromConnectionString(connectionString)
	if err != nil {
		return "", err
	}

	describeCtx, cancel := context.WithTimeout(ctx, describeTimeout)
	defer cancel()

	resp, err := pb.NewTGRpcServerClient(conn).SchemeDescribe(
		describeCtx,
		&pb.TSchemeDescribe{
			Path:          &path,
			SecurityToken: &securityToken,
		},
	)
	if err != nil {
		return "", fmt.Errorf("TSchemeDescribe request failed: %w", err)
	}

	containerID, err := containerIDFromDescribeResponse(resp)
	if err != nil {
		return "", err
	}

	return containerID, nil
}

func dbPathFromConnectionString(connectionString string) (string, error) {
	if strings.TrimSpace(connectionString) == "" {
		return "", errors.New("connection string is empty")
	}

	parsed, err := url.Parse(connectionString)
	if err != nil {
		return "", fmt.Errorf("failed to parse connection string: %w", err)
	}

	path := strings.TrimSpace(parsed.Path)
	if path == "" || path == "/" {
		return "", fmt.Errorf("connection string has no database path: %q", connectionString)
	}

	return path, nil
}

func containerIDFromDescribeResponse(resp *pb.TResponse) (string, error) {
	if resp == nil {
		return "", errors.New("TSchemeDescribe response is empty")
	}
	if resp.GetStatusCode() != pb.TStatusIds_SUCCESS {
		return "", fmt.Errorf(
			"TSchemeDescribe returned status %s: %s",
			resp.GetStatusCode().String(),
			resp.GetErrorReason(),
		)
	}
	if resp.GetPathDescription() == nil {
		return "", errors.New("TSchemeDescribe response has empty path description")
	}

	for _, attr := range resp.GetPathDescription().GetUserAttributes() {
		if attr.GetKey() != ydbcpContainerIDAttrName {
			continue
		}

		containerID := strings.TrimSpace(attr.GetValue())
		if containerID == "" {
			return "", errors.New("folder_id user attribute is empty")
		}
		return containerID, nil
	}

	return "", errors.New("folder_id user attribute not found")
}
