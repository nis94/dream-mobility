# syntax=docker/dockerfile:1.7

FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/stream-processor ./cmd/stream-processor

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/stream-processor /stream-processor
USER 65532:65532
EXPOSE 9466
ENTRYPOINT ["/stream-processor"]
