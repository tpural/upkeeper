VERSION ?= $(patsubst v%,%,$(shell git describe --tags --always --dirty 2>/dev/null || echo dev))
IMAGE   ?= ghcr.io/tpural/upkeeper
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: build test lint fmt image eval

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/upkeeper ./cmd/upkeeper

test:
	go test -race ./...

lint:
	golangci-lint run

fmt:
	gofumpt -w .

image:
	docker buildx build --platform linux/amd64,linux/arm64 --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) .

eval:
	go run ./cmd/eval $(ARGS)
