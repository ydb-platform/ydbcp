FILES := $(shell find . -type f -name '*.go' ! -path '*/proto/*' )
PACKAGES := $(foreach path,$(shell go list ./...),$(if $(findstring /proto/,$(path)),,$(path)))
RELEASE_DIR := $(shell pwd)
STATICCHECK := $(shell go env GOPATH)/bin/staticcheck


$(info $$FILES = $(FILES))
$(info $$PACKAGES = $(PACKAGES))
$(info $$RELEASE_DIR = $(RELEASE_DIR))

.PHONY: all

all: test fmt lint proto build

proto:
	$(MAKE) -C pkg/proto
	$(MAKE) -C plugins/auth_nebius/proto

test:
	go test -v ./... -short -race

fmt:
	go fmt $(PACKAGES)
	goimports -w $(FILES)

$(STATICCHECK):
	go install honnef.co/go/tools/cmd/staticcheck@latest

lint: | $(STATICCHECK)
	$(STATICCHECK) $(PACKAGES)
	go vet $(PACKAGES)

build: build-plugins build-ydbcp
build-ydbcp:
	go build -C cmd/ydbcp -o ydbcp

build-plugins: build-auth_nebius
build-auth_nebius:
	go build -C plugins/auth_nebius -buildmode=plugin -o auth_nebius.so

clean:
	rm -f plugins/auth_nebius/auth_nebius.so cmd/ydbcp/ydbcp

clean-proto:
	find pkg/proto -type f -name '*.go' -delete
	find plugins/auth_nebius/proto -type f -name '*.go' -delete
