FILES ?= $(shell find . -type f -name '*.go')
PACKAGES ?= $(shell go list ./...)

.PHONY: all

all: test fmt lint vet proto build

proto:
	$(MAKE) -C pkg/proto

test:
	go test -v ./... -short

fmt:
	go fmt ./...
	goimports -w $(FILES)

lint:
	golint $(PACKAGES)

vet:
	go vet ./...

build: ydbcp
ydbcp:
	go build -C cmd/ydbcp -o ydbcp
