# Standalone image for the e2e fake Prometheus/vLLM metrics stub. Built and
# loaded into the kind cluster by test/e2e/run.sh; not part of the scaler's
# own release image (see the top-level Dockerfile for that).
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY test/e2e/fakeprom ./test/e2e/fakeprom
RUN CGO_ENABLED=0 go build -trimpath -o /fakeprom ./test/e2e/fakeprom

FROM gcr.io/distroless/static:nonroot
COPY --from=build /fakeprom /fakeprom
EXPOSE 9090
USER nonroot:nonroot
ENTRYPOINT ["/fakeprom"]
