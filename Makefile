TOPTARGETS := all
FILES ?= $(shell find . -type f -name '*.go')
PACKAGES ?= $(shell go list ./...)

SUBDIRS := pkg/proto

$(TOPTARGETS): $(SUBDIRS)
$(SUBDIRS):
	$(MAKE) -C $@ $(MAKECMDGOALS)

.PHONY: $(TOPTARGETS) $(SUBDIRS)

test:
	go test -v ./... -short

fmt:
	go fmt ./...
	goimports -w $(FILES)

lint:
	golint $(PACKAGES)

vet:
	go vet ./...

