# Multi-stage: generate gRPC stubs from the proto, then build a static binary.
FROM golang:1.26-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY go.mod ./
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10 \
 && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
ENV PATH="/root/go/bin:${PATH}"
COPY . .
RUN protoc --go_out=. --go_opt=paths=source_relative \
           --go-grpc_out=. --go-grpc_opt=paths=source_relative \
           externalscaler/externalscaler.proto \
 && go mod tidy \
 && CGO_ENABLED=0 go build -trimpath -o /keda-inference-scaler .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /keda-inference-scaler /keda-inference-scaler
EXPOSE 6000
USER nonroot:nonroot
ENTRYPOINT ["/keda-inference-scaler"]
