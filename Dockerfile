# syntax=docker/dockerfile:1
# Builder pinned to the go.mod toolchain patch release (tag + index digest);
# bump both together.
FROM docker.io/library/golang:1.26.10-alpine@sha256:5cb67f19c5e7994dcb80c074f7cfca8aeeada45bbdca035f73df269cefdcce61 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS
ARG TARGETARCH
# -p 1 and GOGC=25 keep peak compile memory low enough for a 2 GB podman VM
# (compiling aws-sdk-go-v2/service/s3 is the peak).
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} GOGC=25 \
    go build -p 1 -trimpath -ldflags="-s -w" -o /out/ ./cmd/server ./cmd/devicekey \
 && mkdir -p /out/data/staging /out/data/objects

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/server /server
# Device key admin: docker compose exec server /devicekey set -device ID
COPY --from=build /out/devicekey /devicekey
# Pre-create /data owned by the nonroot user so named volumes inherit it.
COPY --from=build --chown=65532:65532 /out/data /data
USER nonroot:nonroot
EXPOSE 8443/tcp 8443/udp 9100/tcp
ENTRYPOINT ["/server"]
