PROTO := externalscaler/externalscaler.proto

# Minimum acceptable statement coverage for the main package (see `make test`).
COVERAGE_FLOOR := 80

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
	go test ./... -coverpkg=github.com/kornsour/keda-inference-scaler -covermode=atomic -coverprofile=coverage.out
	@go tool cover -func=coverage.out | tail -1
	@pct=$$(go tool cover -func=coverage.out | tail -1 | grep -oE '[0-9]+\.[0-9]+'); \
	awk -v p="$$pct" -v floor="$(COVERAGE_FLOOR)" 'BEGIN { \
		if (p+0 < floor+0) { printf "coverage %.1f%% is below floor %s%%\n", p, floor; exit 1 } \
		else { printf "coverage %.1f%% meets floor %s%%\n", p, floor } \
	}'

.PHONY: image
image:
	docker build -t keda-inference-scaler:dev .
