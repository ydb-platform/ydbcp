PROTOS=$(shell find . -type f -name '*.proto')
PROTOGO=$(PROTOS:.proto=.pb.go)
PROTOC := $(shell which protoc)
ifeq ($(PROTOC),)
    PROTOC = must-rebuild
endif
PROTOC_GEN_GO := $(shell go env GOPATH)/bin/protoc-gen-go
PROTOC_GEN_GO_GRPC := $(shell go env GOPATH)/bin/protoc-gen-go-grpc
UNAME := $(shell uname)


.PHONY: compile
compile: $(PROTOGO)

$(PROTOC):
ifeq ($(UNAME), Darwin)
	brew install protobuf
endif
ifeq ($(UNAME), Linux)
	sudo apt-get install protobuf-compiler
endif

$(PROTOC_GEN_GO):
	go get -u google.golang.org/protobuf/cmd/protoc-gen-go
	go install google.golang.org/protobuf/cmd/protoc-gen-go

$(PROTOC_GEN_GO_GRPC):
	go get -u google.golang.org/grpc/cmd/protoc-gen-go-grpc
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc

%.pb.go: %.proto | $(PROTOC_GEN_GO) $(PROTOC) $(PROTOC_GEN_GO_GRPC)
	$(PROTOC) -I=. --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative $<
