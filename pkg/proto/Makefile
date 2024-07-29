PROTOS=$(wildcard ydbcp/*/*.proto)
PROTOGO=$(PROTOS:.proto=.pb.go)
compile: $(PROTOGO)
.PHONY: compile
PROTOC := $(shell which protoc)
PROTOC_GEN_GO := $(shell go env GOPATH)/bin/protoc-gen-go
UNAME := $(shell uname)

ifeq ($(PROTOC),)
    PROTOC = must-rebuild
endif

$(PROTOC):
ifeq ($(UNAME), Darwin)
	brew install protobuf
endif
ifeq ($(UNAME), Linux)
	sudo apt-get install protobuf-compiler
endif

$(PROTOC_GEN_GO):
	go get -u google.golang.org/protobuf/protoc-gen-go

%.pb.go: %.proto | $(PROTOC_GEN_GO) $(PROTOC)
	protoc -I=. --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative $<
