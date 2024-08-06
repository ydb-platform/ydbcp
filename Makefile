FILES := $(filter-out ./pkg/proto/%, $(shell find . -type f -name '*.go'))
PACKAGES ?= $(filter-out ydbcp/pkg/proto/%, $(shell go list ./...))

.PHONY: all

all: test fmt lint proto build

proto:
	$(MAKE) -C pkg/proto

test:
	go test -v ./... -short

fmt:
	go fmt $(PACKAGES)
	goimports -w $(FILES)
	go vet $(PACKAGES)

lint:
	golint $(PACKAGES)

build: ydbcp
ydbcp:
	go build -C cmd/ydbcp -o ydbcp
