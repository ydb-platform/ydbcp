# Use the official Golang image as the base image
FROM golang:1.22-alpine as builder

# Set the Current Working Directory inside the container
WORKDIR ./ydbcp

# Copy source code into the container
COPY ./ ./

# Download all dependencies
RUN go mod download

# Install grpcurl
#RUN go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# Build the Go app
RUN go build -o . ./cmd/ydbcp/main.go

# Build integration test app
RUN go build -o ./make_backup ./cmd/integration/make_backup/main.go

# Build integration test app
RUN go build -o ./list_schedules ./cmd/integration/list_schedules/main.go

# Command to run the executable
CMD ["./main", "--config=local_config.yaml"]
