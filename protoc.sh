#!/usr/bin/env bash
set -eux
find pkg/proto/ -name "*.proto" | xargs protoc -I=. --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative
