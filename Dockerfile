# Use the official Golang image as the base image
FROM golang:1.22-alpine as builder

# Set the Current Working Directory inside the container
WORKDIR /ydbcp

# Copy source code into the container
COPY . .

# Download all dependencies
RUN go mod download

# Install grpcurl
#RUN go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# Build the Go app
RUN go build -o . ./cmd/ydbcp/main.go

# Build integration test app
RUN go build -o ./make_backup ./cmd/integration/make_backup/main.go
RUN go build -o ./list_entities ./cmd/integration/list_entities/main.go
RUN go build -o ./orm ./cmd/integration/orm/main.go

# Command to run the executable
CMD ["./main", "--config=local_config.yaml"]

# Healthcheck (for Github Actions only)
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=3 CMD nc -z localhost 50051 || exit 1