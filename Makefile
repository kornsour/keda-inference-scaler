PROTO := externalscaler/externalscaler.proto

# Generate gRPC stubs (requires protoc + protoc-gen-go + protoc-gen-go-grpc on PATH).
.PHONY: proto
proto:
	protoc --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative $(PROTO)

.PHONY: tidy
tidy: proto
	go mod tidy

.PHONY: build
build: tidy
	CGO_ENABLED=0 go build -o bin/keda-inference-scaler .

.PHONY: test
test: tidy
	go vet ./...
	go test ./...

.PHONY: image
image:
	docker build -t keda-inference-scaler:dev .
