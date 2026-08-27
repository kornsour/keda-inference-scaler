PROTO := externalscaler/externalscaler.proto

# Regenerate the committed gRPC stubs under externalscaler/ after editing the
# .proto file (requires protoc + protoc-gen-go + protoc-gen-go-grpc on PATH;
# see README Prerequisites). Not needed to build or test — the generated
# *.pb.go files are checked in.
.PHONY: proto
proto:
	protoc --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative $(PROTO)

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: build
build: tidy
	CGO_ENABLED=0 go build -o bin/keda-inference-scaler .

.PHONY: test
test:
	go vet ./...
	go test ./...

.PHONY: image
image:
	docker build -t keda-inference-scaler:dev .
