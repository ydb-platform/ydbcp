# Use the official Golang image as the base image
FROM golang:1.23-alpine as builder

# Set the Current Working Directory inside the container
WORKDIR /ydbcp

# Copy source code into the container
COPY . .

# Download all dependencies
RUN go mod download

# Install grpcurl
#RUN go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# Build the Go app (bin/ avoids conflict with ydbcp/ source directory in the repo)
RUN mkdir -p bin && \
    go build -o ./bin/ydbcp ./cmd/ydbcp && \
    go build -o ./bin/migrator ./cmd/migrator

# Build integration test app
RUN go build -o ./make_backup ./cmd/integration/make_backup/main.go
RUN go build -o ./list_entities ./cmd/integration/list_entities/main.go
RUN go build -o ./orm ./cmd/integration/orm/main.go
RUN go build -o ./test_new_paths_format ./cmd/integration/new_paths_format/main.go
RUN go build -o ./make_encrypted_backup ./cmd/integration/make_encrypted_backup/main.go

# Command to run the executable
CMD ["./bin/ydbcp", "--config=local_config.yaml"]

# Healthcheck (for Github Actions only)
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=3 CMD nc -z localhost 50051 || exit 1