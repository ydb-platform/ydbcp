PROTOS=$(wildcard ydbcp/*/*.proto)
PROTOGO=$(PROTOS:.proto=.pb.go)
PROTOC := $(shell which protoc)
ifeq ($(PROTOC),)
    PROTOC = must-rebuild
endif
PROTOC_GEN_GO := $(shell go env GOPATH)/bin/protoc-gen-go
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
	go get -u google.golang.org/protobuf/protoc-gen-go

%.pb.go: %.proto | $(PROTOC_GEN_GO) $(PROTOC)
	protoc -I=. --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative $<
