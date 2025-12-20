!make .PHONY: all build server client proto test clean deps

# Build everything
all: build

# Build both server and client
build: server client

# Build the server binary
server: pkg/proto/kvservice.pb.go pkg/proto/kvservice_grpc.pb.go internal/proto/kvlog.pb.go
	@mkdir -p dist
	go build -tags hashicorpmetrics -o dist/kvserver ./cmd/kvserver

# Build the client binary
client: pkg/proto/kvservice.pb.go pkg/proto/kvservice_grpc.pb.go
	@mkdir -p dist
	go build -o dist/kvctl ./cmd/kvctl

# Generate Protocol Buffer files
pkg/proto/kvservice.pb.go pkg/proto/kvservice_grpc.pb.go: pkg/proto/kvservice.proto
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		pkg/proto/kvservice.proto
internal/proto/kvlog.pb.go: internal/proto/kvlog.proto
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		internal/proto/kvlog.proto

# Convenience target for generating proto files
proto: pkg/proto/kvservice.pb.go pkg/proto/kvservice_grpc.pb.go internal/proto/kvlog.pb.go

# Run tests
test:
	go test ./...

# Clean build artifacts
clean:
	rm -rf dist/