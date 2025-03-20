ARG DOCKERHUB_MIRROR=artifactory.nebius.dev/dockerhub-mirror
ARG BASE_IMAGE=artifactory.nebius.dev/common-cr/ydb/base:20250114-173555
FROM --platform=linux/amd64 $DOCKERHUB_MIRROR/library/golang:1.22 AS builder
ARG GOPROXY=https://artifactory.nebius.dev/artifactory/api/go/go
ENV GOFLAGS -buildvcs=false
ENV CGO_ENABLED 1

WORKDIR /workspace
COPY . .

# Download all dependencies
RUN go mod download

# Build the ydbcp-watcher application
RUN GOOS=linux GOARCH=amd64 go build -o /opt/ydbcp-watcher/bin/ydbcp-watcher ./cmd/ydbcp-watcher/main.go
RUN  GOOS=linux GOARCH=amd64 go build -C plugins/auth_nebius -buildmode=plugin -o auth_nebius.so

FROM $BASE_IMAGE
WORKDIR /root
COPY --from=builder /opt/ydbcp-watcher/bin/ydbcp-watcher /opt/ydbcp-watcher/bin/ydbcp-watcher
COPY --from=builder /workspace/plugins/auth_nebius/auth_nebius.so /opt/ydbcp-watcher/plugins/auth_nebius.so

## Set the default command
#CMD ["./main", "--config=local_config.yaml"]

# Healthcheck for the service
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=3 CMD nc -z localhost 50052 || exit 1
