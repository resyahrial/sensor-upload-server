# Sensor Upload Server

A single Go binary that accepts sensor files (photo, audio, video) over three
protocols with the same rules:

| Endpoint | Protocol | Transport |
|---|---|---|
| `/rest/files/` | tus 1.0 (creation, termination, expiration) | HTTPS, HTTP/1.1 or HTTP/2 over TCP |
| `/rest-http3/files/` | tus 1.0 | HTTP/3 over QUIC (UDP, same port as TCP) |
| `/grpc/upload.v1.UploadService/*` | gRPC (`Init`, `Upload` bidi stream, `Status`, `Cancel`) | HTTP/2 over the TLS TCP listener (`/grpc` prefix stripped), plus an optional native gRPC listener |

Every protocol only receives bytes into a staging area. One shared
**finalizer** verifies the SHA-256, deduplicates by sha256, moves the file to
the object store (local disk or S3/MinIO) and records it in PostgreSQL. An
expiry cleaner removes incomplete uploads after `INCOMPLETE_TTL`.

The client contract is [`sensor-upload-integration.md`](sensor-upload-integration.md);
the server requirements are [`upload-server-requirements.md`](upload-server-requirements.md).
Judgment calls are in [`docs/decisions.md`](docs/decisions.md), §15 acceptance
evidence in [`docs/acceptance.md`](docs/acceptance.md).

## Architecture

```
TLS TCP :8443 ── router ── /rest/files/*        → tusd handler ─┐
                        └─ /grpc/* (gRPC, strip) → UploadService ┤
QUIC UDP :8443 ─────────── /rest-http3/files/*  → tusd handler ─┤──→ staging (STAGING_DIR)
native gRPC :9090 ───────── bare + /grpc paths  → UploadService ┘        │
                                                                    finalizer
                                         (sha256 · dedup · object store · Postgres row)
admin :9100 (plain HTTP): /metrics /healthz /readyz      expiry cleaner (every CLEANUP_INTERVAL)
```

- tus auth, device ownership, base-path checks, 410/460 states and the
  "offset == size" recovery live in middleware in front of tusd
  (`internal/tusapi`); tusd itself has no pre-PATCH/HEAD hooks.
- gRPC staging (`internal/staging`) fsyncs every chunk before it is acked.
- One `grpc.Server` serves both the TLS listener (via `ServeHTTP`) and the
  native listener.

## Repository layout

```
cmd/server      server main (config, signals; wiring is in internal/app)
cmd/uploader    test client: rest | rest-http3 | grpc | grpc-native, SQLite outbox, CSV report
cmd/devtoken    dev-only device JWT issuer
cmd/summarize   merges experiment CSVs into results/summary.csv
internal/       app, auth, cleanup, client, config, db, finalize, grpcadapt, grpcapi,
                grpcprefix, httpx, meta, objstore, obs, staging, tusapi
proto/, gen/    upload.v1 proto and generated Go code (buf)
migrations/     goose migrations (embedded, applied at startup)
deploy/         docker-compose.yml (server, Postgres, MinIO), netem/ (experiment harness)
test/e2e        in-process end-to-end tests (integration tag)
test/load       NFR-2 / NFR-3 load tests (load tag)
mk/             extra make targets (proto, experiment, load tests, uploader)
docs/           decisions.md, acceptance.md
```

## Quick start

Needs Go 1.26, `openssl`, and podman (or Docker) with `podman compose`.
`go.mod` pins `toolchain go1.26.10` (go1.26.0 has reachable stdlib CVEs);
the Makefile exports `GOTOOLCHAIN=auto` so `make` targets download and use
it even if your `go env` says `GOTOOLCHAIN=local`. For plain `go` commands run
`export GOTOOLCHAIN=auto` (check with `go version` in the repo). The
Dockerfiles pin `golang:1.26.10-alpine` by tag and digest; bump them together
with `go.mod`.

```sh
make devcert            # certs/server.crt+key (SAN localhost, server), certs/jwt.key+pub (ES256),
                        # certs/jwt-hmac.env (random 32-byte JWT_HMAC_SECRET for HS256, mode 600)
make up                 # builds the image, starts server + Postgres + MinIO (devcert runs first)

go build -o bin/ ./cmd/uploader ./cmd/devtoken
TOKEN=$(bin/devtoken -device dev-1)            # ES256 with certs/jwt.key, 24 h TTL (-ttl to change)
head -c 2097152 /dev/urandom > sample.flac     # content type comes from the extension

# tus over HTTP/2
bin/uploader -protocol rest       -url https://localhost:18443 -token "$TOKEN" -device dev-1 \
  -ca certs/server.crt -file sample.flac -state rest.db -report rest.csv
# tus over HTTP/3 (UDP 18443)
bin/uploader -protocol rest-http3 -url https://localhost:18443 -token "$TOKEN" -device dev-1 \
  -ca certs/server.crt -file sample.flac -state h3.db -report h3.csv
# gRPC with the /grpc prefix on the TLS listener
bin/uploader -protocol grpc       -url https://localhost:18443 -token "$TOKEN" -device dev-1 \
  -ca certs/server.crt -file sample.flac -state grpc.db -report grpc.csv
# gRPC on the native listener
bin/uploader -protocol grpc-native -url localhost:19090 -token "$TOKEN" -device dev-1 \
  -ca certs/server.crt -file sample.flac -state native.db -report native.csv

curl -s localhost:19100/readyz          # {"status":"ok","checks":{"postgres":"ok",...}}; also /healthz, /metrics
make down
```

The same file on a second protocol reports `result=duplicate` (461 /
`ALREADY_EXISTS`); use a new random file per protocol to see four uploads.
Host ports: 18443 TCP+UDP, 19090 native gRPC, 19100 metrics (override with
`HOST_HTTPS_PORT`, `HOST_GRPC_PORT`, `HOST_METRICS_PORT`), published on
`127.0.0.1` only. Set `HOST_BIND=0.0.0.0` to reach them from other machines,
but only on a trusted network. The compose stack
runs with `DEV_MODE=true`, `JWT_ALGS=ES256,HS256` and `OBJSTORE=s3` (MinIO,
bucket `uploads`). The HS256 secret is read from `certs/jwt-hmac.env`, so an
HS256 dev token is
`(set -a; . certs/jwt-hmac.env; bin/devtoken -alg HS256 -device dev-1)`;
delete the file and re-run `make devcert` to rotate it. `SERVER_IMAGE` and `COMPOSE_NETWORK` let a second stack run
next to the first (see `docs/acceptance.md`).

Other useful uploader flags: `-dir` + `-concurrency N`, `-chunk` (default
262144), `-retries`, `-backoff-max` (default 5m), `-priority`, `-h3-fallback`,
`-insecure`, `-v`. The token can also come from `$UPLOADER_TOKEN`. Run
`bin/uploader -h` for the full list. Re-running with the same `-state` DB
resumes an interrupted upload.

## Configuration

Environment variables (`internal/config`). An empty value counts as unset.
All problems are reported together and the server exits 1.

| Variable | Default | Description |
|---|---|---|
| `LISTEN_ADDR` | `:8443` | TLS TCP (HTTP/1.1, HTTP/2, `/grpc`) and QUIC UDP on the same port |
| `GRPC_NATIVE_ADDR` | *(empty = off)* | Native gRPC listener (TLS); serves bare and `/grpc`-prefixed paths |
| `METRICS_ADDR` | `:9100` | Plain-HTTP admin: `/metrics`, `/healthz`, `/readyz`. Unauthenticated and on all interfaces by default: firewall it or set e.g. `127.0.0.1:9100`. `/readyz` reports only `ok`/`fail` per check (`postgres`, `staging`, `objstore`); the error behind a failure is logged at `warn` (`readiness check failed`) |
| `TLS_CERT_FILE`, `TLS_KEY_FILE` | — | Required. TLS 1.2+ on TCP, 1.3 on QUIC |
| `DATABASE_URL` | — | Required. PostgreSQL URL; migrations run at startup |
| `STAGING_DIR` | `./data/staging` | Partial uploads (`tus/`, `grpc/`), tus lock files |
| `TUS_STAGING` | `file` | `file` or `s3` (see known limitations) |
| `OBJSTORE` | `local` | `local` or `s3` |
| `OBJSTORE_LOCAL_DIR` | `./data/objects` | Object root when `OBJSTORE=local` |
| `S3_ENDPOINT` | — | Custom endpoint (MinIO), e.g. `http://minio:9000` |
| `S3_REGION`, `S3_BUCKET` | — | Required when `OBJSTORE` or `TUS_STAGING` is `s3` |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY` | — | Static credentials; unset = AWS default credential chain |
| `S3_FORCE_PATH_STYLE` | `false` | Path-style addressing (needed for MinIO) |
| `MAX_FILE_BYTES` | `524288000` | Maximum file size (500 MB) |
| `INCOMPLETE_TTL` | `168h` | Incomplete uploads idle longer than this are expired |
| `CLEANUP_INTERVAL` | `1h` | Expiry cleaner period (first pass at startup) |
| `GRPC_MAX_CHUNK_BYTES` | `262144` | Maximum gRPC chunk, returned in `InitResponse` |
| `GRPC_ACK_EVERY_N` | `1` | gRPC ack frequency (every N-th chunk; the final chunk always) |
| `QUIC_IDLE_TIMEOUT` | `60s` | QUIC max idle timeout |
| `QUIC_KEEPALIVE` | `20s` | QUIC keep-alive period; must be shorter than `QUIC_IDLE_TIMEOUT` |
| `JWT_ALGS` | `ES256` | Comma list of `ES256`, `HS256`; `HS256` only with `DEV_MODE=true` |
| `JWT_PUBLIC_KEY_FILE` | — | PEM P-256 public key; required with ES256 |
| `JWT_HMAC_SECRET` | — | HMAC secret; required with HS256 (dev), at least 32 bytes |
| `CONTENT_TYPES` | FR-1a list | Comma-separated allow-list (default: `image/jpeg,image/png,image/webp,image/avif,audio/wav,audio/x-wav,audio/flac,audio/ogg,audio/opus,video/mp4,video/quicktime,application/json`) |
| `DEV_MODE` | `false` | Allows HS256; log level debug unless `LOG_LEVEL` is set |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `SHUTDOWN_TIMEOUT` | `30s` | Graceful drain budget for all listeners on SIGTERM |

Tokens: `exp` and a `device_id` claim are required, leeway 120 s, `iat`/`nbf`
are checked when present.

## Testing

| Command | What |
|---|---|
| `make test` | Unit tests with coverage (`go test -cover ./...`) |
| `make test-integration` | Everything with `-tags integration`: Postgres and MinIO via testcontainers-go, in-process e2e for all protocols incl. server restart |
| `make load-test` | NFR-2 / NFR-3 load tests (see below) |
| `make lint` | `go vet` and staticcheck, default and `integration` tags |
| `make lint-load` | Same for the `load`-tagged files |
| `make proto` | `buf lint` + `buf generate` into `gen/` |
| `make build` | Static binaries into `bin/` |

Integration tests under podman: the Makefile sets
`DOCKER_HOST=unix://…/podman-machine-default-api.sock` (override for your
machine; `podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}'`
prints it) and `TESTCONTAINERS_RYUK_DISABLED=true` (every test terminates its
own containers). `INTEGRATION_P` (default 2) limits how many packages run in
parallel, because each starts its own containers in a 2 GB VM. MinIO images
come from `docker.io/pgsty/minio` (upstream images are no longer anonymously
pullable).

Lint uses `go run honnef.co/go/tools/cmd/staticcheck@v0.8.2`; set
`STATICCHECK=staticcheck` to use a local binary built with Go ≥ 1.26.
`make proto` needs `buf` and `protoc-gen-go`/`protoc-gen-go-grpc` in
`$(go env GOPATH)/bin` (`GOBIN_DIR`, `BUF` override).

Load tests run the real `cmd/server` as a subprocess against a Postgres
testcontainer and sample its RSS every 250 ms:

```sh
make load-test                          # rest: 50 x 200 MiB concurrent (NFR-2), then 200 in progress (NFR-3)
make load-test LOAD_PROTOCOL=rest-http3 # or grpc
make load-test-all                      # rest, rest-http3, grpc in turn
make load-test LOAD_RUN=NFR3 LOAD_INPROGRESS=100
```

Knobs: `LOAD_FILES`, `LOAD_FILE_SIZE`, `LOAD_CHUNK`, `LOAD_RSS_LIMIT` (300MB),
`LOAD_INPROGRESS`, `LOAD_INPROGRESS_SIZE`, `LOAD_HOLD`, `LOAD_SERVER_PROCS`
(GOMAXPROCS, default 2), `LOAD_DIR`, `LOAD_TIMEOUT` (90m). NFR-2 needs about
`2 x files x size + 4 GiB` free (≈ 25 GB by default) and checks it first.
Results are in `docs/acceptance.md` ("NFR-2 / NFR-3 memory and concurrency"):
rest and rest-http3 pass; grpc currently fails both (see known limitations).
Keep the host awake during a run (`caffeinate -i make load-test …` on macOS).

## Protocol experiment

Compares resumability of `rest`, `rest-http3`, `grpc` and `grpc-native` under
impaired links. Each run is one uploader in its own container with `tc netem`
on its `eth0`.

| Profile | Rate | Delay ± jitter | Loss | Outages |
|---|---|---|---|---|
| `good-4g` | 5 Mbit | 50 ± 10 ms | 0.1% | none |
| `weak-4g` | 256 kbit | 300 ± 100 ms | 3% | 10 s every ~2 min |
| `leo-sat` | 2 Mbit | 60 ± 20 ms | 1% | link off after 10 min |
| `very-poor` | 64 kbit | 800 ± 200 ms | 10% | 5 s every ~30 s |

```sh
make up
make netem-image                                   # uploader + tc image (deploy/netem/Dockerfile.uploader)
TOKEN=$(bin/devtoken -device netem-0001 -ttl 24h)  # must outlive the whole run
make experiment-smoke TOKEN="$TOKEN" CA_FILE=certs/server.crt   # 16 runs: 1 rep, good-4g + very-poor, 50KB + 2MB, 64KB
make experiment       TOKEN="$TOKEN" CA_FILE=certs/server.crt   # full matrix
make summary                                       # re-merge results/runs/*.csv → results/summary.csv
```

- Full matrix: 4 protocols × 4 profiles × 3 sizes (50 KB, 2 MB, 20 MB) ×
  2 chunks (64 KB, 256 KB) × 5 reps = **480 runs**, about **12 h** at
  `EXPERIMENT_PARALLEL=4` (default).
- Resumable: a run with `results/runs/ID.csv` is skipped, so the driver can be
  stopped and restarted. Each run is stopped after `RUN_TIMEOUT` (7200 s) and
  recorded as `result=timeout`. Logs are in `results/logs/`.
- The server must be reachable as host `server` on `NETEM_NETWORK` (default
  `deploy_default`, the compose network). Other knobs: `SERVER_URL`
  (`https://server:8443`), `GRPC_NATIVE_URL` (`server:9090`), `DEVICE`,
  `PROTOCOLS`, `PROFILES`, `SIZES`, `CHUNKS`, `REPS`, `UPLOADER_ARGS`
  (`-retries 100000 -backoff-max 30s`), `RESULTS_DIR`.
- The macOS host needs GNU `timeout` as `gtimeout` (`brew install coreutils`).
- netem shapes **egress only** (the uploader's upload direction; responses
  and ACKs are unshaped) and sets the container MTU to 1500.

### Reading `results/summary.csv`

One row per `protocol, profile, size, chunk` (sizes and chunks in bytes):

| Column | Meaning |
|---|---|
| `n`, `n_success` | Runs in the cell, and runs whose file ended up on the server (`ok`, `duplicate` or `fallback`) |
| `success_rate` | `n_success / n` (timeouts and failures count against it) |
| `duration_ms_median`, `duration_ms_p95` | Wall time of successful runs |
| `attempts_*` | Upload attempts per file (1 = no retry) |
| `resumes_*` | Attempts that continued from a server offset > 0 |
| `bytes_resent_*` | Payload bytes sent again below the highest offset already sent |
| `overhead_*` | `bytes_sent_total / size` (1.0 = nothing re-sent) |

Medians and p95 (nearest rank) are over successful runs only; a cell with no
success has empty statistics. Per-run rows are in `results/runs/*.csv`
(§11 uploader columns plus `run_id, profile, rep, exit_code`).

## Contract deviations

Differences from `sensor-upload-integration.md` and the requirements:

1. **No 423 on concurrent tus access.** tusd's locker hands the lock over: the
   request already holding the upload ends with **400 `ERR_UPLOAD_INTERRUPTED`**
   (its bytes are kept) and the new request proceeds (409 if its offset is
   stale). A lock not released within 20 s gives 500 (tusd `ERR_LOCK_TIMEOUT`, sent as `internal error`), except where the middleware itself waits for it to finalize a fully staged upload: 423.
   gRPC does return `ABORTED` for a second stream or a Cancel during a stream.
2. **`Location` is an absolute URL** (`https://host:port/rest/files/{id}`),
   not a path. Clients that resolve it with `res.Location()` work unchanged.
3. **OPTIONS needs a token and returns 200** (tusd), not 204. `Tus-Extension`
   lists `creation,termination,expiration`.
4. **`creation-with-upload` is not advertised**, but tusd still accepts a POST
   with a body (it goes through the same checks).
5. **Another device's upload → 404 / `NOT_FOUND`**, not 403, so its existence
   is not revealed. Creating with another device's `device_id` is still 403 /
   `PERMISSION_DENIED`.
6. **DELETE on a completed upload → 404** (its staging is gone); HEAD on it
   answers 200 with `Upload-Offset = Upload-Length`.
7. **HEAD/PATCH/DELETE on an upload that failed its checksum → 460**, not only
   the completing PATCH.
8. **413 for a file above `MAX_FILE_BYTES` comes from tusd** and its body does
   not name the field (other metadata errors name it).
9. **Default port 8443**, not 443 (contract: UDP 443). Set `LISTEN_ADDR=:443`
   in production.
10. **`Alt-Svc: h3=":<port>"; ma=86400` uses the listen port**, so behind a port
    mapping (compose 18443→8443) it advertises the container port.
    Informational only; clients pick `/rest-http3` explicitly.
11. **TCP listener answers `/rest-http3/*` with 404**; an upload can only be
    resumed on the base path it was created on (both → 404 on the other).
12. **Native gRPC listener serves both bare (`/upload.v1.UploadService/…`) and
    `/grpc`-prefixed paths.** The TLS listener serves only the prefixed path.
13. **`grpc` and `grpc-native` are both stored as `protocol = 'grpc'`**; only the
    uploader report tells them apart.
14. **gRPC `Status`/`Cancel`/`Upload` on a cancelled, expired or
    checksum-failed upload → `NOT_FOUND`** ("Init again"); an `Upload` stream on
    a completed upload → `ALREADY_EXISTS`.
15. **Experiment sizes are 50 KB, 2 MB and 20 MB** (requirements §12 also lists
    200 MB). The 200 MB case is covered by the unthrottled §15 acceptance run.
16. **tus `committed_bytes` in the DB is set at completion only**; the staged
    offset (HEAD) is authoritative while uploading.
17. **gRPC chunk commit updates the DB, not the staging info file.** FR-6
    says "update committed offset (staging info + DB)". The server fsyncs the
    `.bin` and updates the DB before every ack, but writes `{id}.json` only
    once, at Init. FR-7 makes the DB offset authoritative and the `.bin` is
    truncated to it on resume, so the per-chunk rewrite (two more fsyncs, a
    rename and a directory fsync) bought nothing (NFR-3).

## Known limitations

- **tus filestore does not fsync.** Bytes acked to a tus client survive a
  process crash (page cache) but not an OS crash or power loss. gRPC fsyncs
  every chunk before acking.
- **`TUS_STAGING=s3`**: tusd's s3store keeps parts smaller than 5 MiB as a
  separate object and downloads, deletes and re-uploads it on every PATCH, so
  64–256 KiB chunks cost up to ~20–80× extra S3 traffic. There is no startup
  recovery for s3 staging, and the expiry cleaner cannot delete abandoned
  multipart uploads; configure a bucket lifecycle rule for them. Use `file`
  staging unless chunks are ≥ 5 MiB.
- **gRPC client window.** The uploader pipelines chunks; `-grpc-window` limits
  the number of unacked chunks in flight (default 1) and must be ≥ the server's
  `GRPC_ACK_EVERY_N`. With a large window, a killed client may re-send
  everything that was still in local buffers (the server never stores a byte
  twice).
- **gRPC server memory is bounded per stream, not globally.** `/grpc` on the
  TLS listener reads at most 64 KiB of buffers ahead of the handler
  (`grpcapi.LimitReadAhead`; `grpc.Server.ServeHTTP` alone buffered
  without limit: 581–739 MB in the load test before the fix). The native
  listener uses a fixed 64 KiB stream window, and HTTP/2 requests on the TLS
  listener get a 256 KiB receive buffer. Load test peaks after the fixes:
  171 / 240–246 MB (`/grpc`, NFR-2 / NFR-3) and 123 / 197 MB (native), limit
  300 MB. RSS follows the GC target, so several hundred streams more than the
  NFR-3 200 would need `GOMEMLIMIT`. See `docs/acceptance.md`.
- **gRPC with many small chunks is slow (NFR-3).** Every chunk is fsynced and
  committed to the DB before it is acked. With 200 streams of 4 KiB chunks
  the dev Mac manages about 320 chunks/s (was ~70/s before the info-file
  rewrite was dropped) against 1,280/s offered. The NFR-3 test passes (fresh
  upload ~10 s, `/healthz` ≤ 51 ms), but the held uploads take ~2m40s
  instead of 30 s. The commit cost is per chunk, so use 64–256 KiB chunks.
- **Static 64 KiB window on the native gRPC listener.** Throughput per stream
  is at most 64 KiB per round trip, enough for the netem profiles (BDP ≤
  ~62 KB) but not for long fat links. The native listener also allows at
  most 100 concurrent streams per connection (further streams wait on the
  client) and 16 KiB of request headers.
- **HTTP/3 throughput on loopback is about 4.5× lower than HTTP/2** (QUIC in
  user space on both sides, server at 2 cores). Not an NFR target.
- **In-VM image build needs more than 2 GB VM memory.** `make up` builds the
  image inside the podman VM; with other containers running, the Go compile
  (aws-sdk, sqlite for the netem image) can be OOM-killed. Stop other
  containers or give the VM more memory. The image itself is ~23–31 MB.
- **podman VM clock drift** (macOS, especially after the host sleeps) breaks
  auth: the server rejects host-issued JWTs (`iat` in the future → 401) and
  MinIO rejects SigV4 requests more than 15 min off (403, seen in integration
  tests). Check with `date -u; podman machine ssh date -u` and re-step with
  `podman machine ssh "sudo date -u -s @$(date +%s)"`.
- **Two concurrent gRPC `Init` calls for the same new sha256 and device can
  create two in-progress rows.** Deduplication still holds: only one can
  complete; the other is cancelled as a duplicate or expires.
- **`deploy/docker-compose.yml` is a dev stack, not a deployment.** It runs
  `DEV_MODE=true` with HS256, default Postgres/MinIO credentials and a
  plain-HTTP admin port, and publishes on `127.0.0.1` by default. For a real
  deployment use ES256 only, real credentials and `METRICS_ADDR` reachable
  only from the scraper.
- **No per-device quota on in-progress uploads.** An authenticated device can
  create many uploads (each up to the size limit) and fill `STAGING_DIR` until
  they expire. Only the global size limit and expiry bound it.
- Single instance only: the tus locker, gRPC stream registry and cleaner are
  in-process.

## Future work

Ideas only, not implemented:

- A real `finalize.Notifier` (currently a no-op) to publish "file completed"
  events, e.g. to a queue for downstream processing.
- Multi-instance deployment: shared locking (e.g. Postgres advisory locks) for
  tus and gRPC, and a single elected cleaner.
- Persist the s3store multipart ID so `TUS_STAGING=s3` can recover and expire
  uploads; an fsync-ing tus file store for OS-crash durability.
- A unique index on in-progress `(sha256, device_id)` rows to stop the gRPC
  `Init` race.
- Per-device limits on in-progress uploads and staged bytes (count and total
  size), returning `429`/`RESOURCE_EXHAUSTED`.
- Run the experiment with ingress shaping (ifb) and the 200 MB size.
