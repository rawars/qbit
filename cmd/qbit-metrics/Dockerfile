# syntax=docker/dockerfile:1

ARG GO_VERSION=1.26.6
FROM golang:${GO_VERSION}-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w \
      -X github.com/rawars/qbit/internal/buildinfo.Version=${VERSION} \
      -X github.com/rawars/qbit/internal/buildinfo.Commit=${COMMIT} \
      -X github.com/rawars/qbit/internal/buildinfo.Date=${BUILD_DATE}" \
    -o /out/qbit-metrics ./cmd/qbit-metrics

FROM gcr.io/distroless/static-debian12:nonroot

ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown
LABEL org.opencontainers.image.title="qbit-metrics" \
      org.opencontainers.image.description="Prometheus exporter for Qbit queues" \
      org.opencontainers.image.source="https://github.com/rawars/qbit" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.created="${BUILD_DATE}"

COPY --from=build /out/qbit-metrics /qbit-metrics
EXPOSE 9091
USER nonroot:nonroot
ENTRYPOINT ["/qbit-metrics"]
