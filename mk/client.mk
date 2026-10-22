# Targets owned by the client/gRPC workstream; included by the root Makefile
# via `-include mk/*.mk`.

GOBIN_DIR ?= $(shell go env GOPATH)/bin
BUF ?= $(GOBIN_DIR)/buf

.PHONY: proto proto-lint uploader

## proto: lint the .proto files and regenerate gen/ (needs protoc-gen-go and
## protoc-gen-go-grpc in GOBIN_DIR or on PATH).
proto: proto-lint
	PATH="$(GOBIN_DIR):$$PATH" $(BUF) generate

proto-lint:
	$(BUF) lint

## uploader: build the test client into bin/uploader.
uploader:
	CGO_ENABLED=0 go build -o bin/uploader ./cmd/uploader
