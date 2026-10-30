# Sensor Upload Server

A single Go binary that accepts sensor files (photo, audio, video) over three
protocols with the same rules:

| Endpoint | Protocol | Transport |
|---|---|---|
| `/rest/files/` | tus 1.0 (creation, termination, expiration) | HTTPS, HTTP/1.1 or HTTP/2 over TCP |
| `/rest-http3/files/` | tus 1.0 | HTTP/3 over QUIC (UDP, same port as TCP) |
| `/grpc/upload.v1.UploadService/*` | gRPC (`Init`, `Upload` bidi stream, `Status`, `Cancel`) | HTTP/2 over the TLS TCP listener (`/grpc` prefix stripped), plus an optional native gRPC listener |
| `GET /files/sha256/{sha256}` | "Is this file saved?" lookup, JSON, **no authentication** | HTTPS (HTTP/1.1, HTTP/2) and HTTP/3, same port |
| `GET /files` | List of completed uploads, newest first, JSON, **no authentication** | HTTPS (HTTP/1.1, HTTP/2) and HTTP/3, same port |
| `GET /readyz` | Readiness (same as the admin `/readyz`), **no authentication** | HTTPS (HTTP/1.1, HTTP/2), same port |

Every protocol only receives bytes into a staging area. One shared
**finalizer** verifies the SHA-256, deduplicates by sha256, moves the file to
the object store (local disk or S3/MinIO) and records it in PostgreSQL. An
expiry cleaner removes incomplete uploads after `INCOMPLETE_TTL`; retention
deletes completed uploads after `RETENTION_MAX_AGE` or when the disk passes
`DISK_HIGH_WATERMARK`.

Features at a glance:

- **JWT access tokens + rotating refresh tokens**: `Authorization: Bearer
  <access JWT>` (ES256, 1 h, verified without a database lookup); devices
  register, ops approves with `devicectl`, devices refresh
  ([Authentication](#authentication)).
- **File lookup** `GET /files/sha256/{sha256}`: "is this file saved?",
  no token ([File lookup](#file-lookup-get-filessha256sha256)).
- **File list** `GET /files`: completed uploads, filterable by device and
  content type, no token ([File list](#file-list-get-files)).
- **Storage**: staging while uploading, then the object store under
  `{device_id}/{yyyy}/{mm}/{dd}/{sha256}{ext}` ([Where files are stored](#where-files-are-stored)).
- **Retention and disk-pressure cleanup**: completed files are deleted after
  7 days or when the disk fills ([Retention and disk pressure](#retention-and-disk-pressure)).
- **Remote e2e suite** against a running server: `make e2e-remote`,
  `make e2e-remote RUN=<test>`, `make e2e-remote-list` ([Remote e2e tests](#remote-e2e-tests)).
- **VPS deploy**: `make vps-secrets`, `vps-bootstrap`, `vps-deploy`,
  `vps-device-token`, `vps-status`, ... ([Deploy to a VPS](#deploy-to-a-vps)).
- **Protocol experiment** under `tc netem` profiles ([Protocol experiment](#protocol-experiment)).

The client contract is [`sensor-upload-integration.md`](sensor-upload-integration.md);
the server requirements are [`upload-server-requirements.md`](upload-server-requirements.md).
Judgment calls are in [`docs/decisions.md`](docs/decisions.md), §15 acceptance
evidence in [`docs/acceptance.md`](docs/acceptance.md).

## Architecture

```
TLS TCP :8443 ── router ── /rest/files/*        → tusd handler ─┐
                        ├─ /grpc/* (gRPC, strip) → UploadService ┤
                        ├─ GET /files/sha256/{h} → file lookup (also on QUIC; reads Postgres)
                        ├─ GET /files            → file list (also on QUIC; reads Postgres)
                        └─ GET /readyz           → readiness (same checks as admin)
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
cmd/devicectl   device ops: list | approve | reject | revoke | issue | access-token
cmd/summarize   merges experiment CSVs into results/summary.csv
internal/       app, auth, cleanup, client, config, db, deviceapi, finalize, grpcadapt, grpcapi,
                grpcprefix, httpx, meta, objstore, obs, staging, tusapi
proto/, gen/    upload.v1 proto and generated Go code (buf)
migrations/     goose migrations (embedded, applied at startup)
deploy/         docker-compose.yml (dev stack: server, Postgres, MinIO), netem/ (experiment harness),
                vps/ (single-VPS deployment: compose, gen-secrets.sh, bootstrap.sh, deploy.sh)
test/e2e        in-process end-to-end tests (integration tag)
test/load       NFR-2 / NFR-3 load tests (load tag)
test/remote     manually run e2e suite against a running server, e.g. a VPS (remote tag)
mk/             extra make targets (proto, experiment, load tests, uploader, e2e-remote, vps-*)
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
make devcert            # certs/server.crt+key (SAN localhost, server) and certs/jwt.key (token signing key)
make up                 # builds the image, starts server + Postgres + MinIO (devcert runs first)

go build -o bin/ ./cmd/uploader
TOKEN=$(make -s device-token DEVICE=dev-1)     # access JWT for dev-1, valid 1h (TTL=24h for longer)
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
# Is the file saved? (no token; the hash is what the client queued)
curl -s --cacert certs/server.crt https://localhost:18443/files/sha256/$(shasum -a 256 sample.flac | cut -d' ' -f1)
make down
```

The same file on a second protocol reports `result=duplicate` (461 /
`ALREADY_EXISTS`); use a new random file per protocol to see four uploads.
Host ports: 18443 TCP+UDP, 19090 native gRPC, 19100 metrics (override with
`HOST_HTTPS_PORT`, `HOST_GRPC_PORT`, `HOST_METRICS_PORT`), published on
`127.0.0.1` only. Set `HOST_BIND=0.0.0.0` to reach them from other machines,
but only on a trusted network. The compose stack
runs with `OBJSTORE=s3` (MinIO, bucket `uploads`). `SERVER_IMAGE` and
`COMPOSE_NETWORK` let a second stack run next to the first (see
`docs/acceptance.md`).

Other useful uploader flags: `-dir` + `-concurrency N`, `-chunk` (default
262144), `-retries`, `-backoff-max` (default 5m), `-priority`, `-h3-fallback`,
`-insecure`, `-v`. The token can also come from `$UPLOADER_TOKEN`; `-device`
defaults to the token's device_id (its `sub` claim). Instead of a static
`-token`, `-refresh-token-file PATH` takes a token pair JSON file (`make -s
device-issue DEVICE=dev-1 > dev-1.json`) and refreshes it as needed (see
[Authentication](#authentication)). Run
`bin/uploader -h` for the full list. Re-running with the same `-state` DB
resumes an interrupted upload.

## Authentication

Every upload request (tus, gRPC) carries a short-lived **access token**, a
JWT signed by this server:

```
Authorization: Bearer <access JWT>          # tus (HTTP)
authorization: Bearer <access JWT>          # gRPC metadata, same value
```

A device gets its first token pair by registering (ops approves it) and
renews it with an opaque, rotating **refresh token**. The client contract,
including the sensor-side rules, is sections 1–3 of
[`sensor-upload-integration.md`](sensor-upload-integration.md); the design
notes are in `docs/decisions.md` "Decisions (JWT + refresh)". The file
lookup endpoint needs no token.

| Token | Format | Lifetime |
|---|---|---|
| Access | JWT, ES256. Header `kid` (first 16 hex of SHA-256 of the signing public key's PKIX DER), `typ: at+jwt`. Claims `iss` (`JWT_ISSUER`), `aud` `sensor-upload`, `sub` = device_id, `iat`, `exp`, `jti` | `ACCESS_TOKEN_TTL` (1h) |
| Refresh | 32 random bytes as 64 lowercase hex; only its SHA-256 is stored | Refused after `REFRESH_IDLE_TTL` (90 days) unused, or `REFRESH_MAX_AGE` (365 days) after registration; then the device registers again |

### Device endpoints

All are JSON over the TLS TCP listener and need no `Authorization` header.

| Endpoint | Request | Responses |
|---|---|---|
| `POST /v1/devices/register` | `{"device_id","poll_secret","lat","lon","firmware"}` | `202` `{"status":"pending"}` + `Retry-After` (`REGISTER_RETRY_AFTER`, 1h); `200` token pair once approved; `400 invalid_request`; `403 device_not_active` (rejected/revoked); `409 poll_secret_mismatch`; `429 rate_limited` (per IP) |
| `POST /v1/devices/approve` | `{"device_id":"…"}` | `200` `{"device_id":"wih-001","status":"active"}` (also when already active); `400 invalid_request`; `403 device_not_active` (rejected/revoked: only `devicectl approve` re-activates); `404 device_not_found` (never registered); `429 rate_limited` (per IP, shared with register); `503 temporarily_unavailable` |
| `POST /v1/devices/unregister` | `{"device_id":"…"}` | `200` `{"device_id":"wih-001","status":"unregistered"}` (row deleted in any state; the id can register again with any poll secret and lands pending; its refresh tokens get `401 invalid_grant`); `400 invalid_request`; `404 device_not_found`; `429 rate_limited` (per IP, shared with register); `503 temporarily_unavailable` |
| `POST /v1/devices/token/refresh` | `{"refresh_token":"…"}` | `200` token pair; `400 invalid_request`; `401 invalid_grant` (unknown, reused, idle or max-age expired: register again); `403 device_not_active`; `503 temporarily_unavailable` |

The token pair has the same shape from register, refresh and `devicectl issue`:

```json
{"device_id":"dev-1","access_token":"eyJ…","token_type":"Bearer","expires_in":3600,
 "refresh_token":"<64 hex>","refresh_expires_at":"2027-10-27T03:20:00Z"}
```

Error bodies are `{"error":"<code>"}`.

- **Registration.** Any device may register with a valid device id
  (3-64 letters, digits or hyphens, starting with a letter or digit; case-insensitive (the server lowercases it, so `WIH-001` and `wih-001` are the same device)). The first call creates the device as
  `pending` and stores the poll secret's hash; ops see it in
  `devicectl list -status pending` and approve it (`devicectl approve` or the
  public `POST /v1/devices/approve`) or reject it. After approval, the same poll secret
  collects the pair, again and again until the refresh token is first used
  (so a lost `200` is harmless); then the poll secret is cleared. When the
  refresh family has expired, registering with a **new** poll secret puts the
  device back to `pending` for re-approval.
- **Refresh rotation.** The server keeps two refresh tokens per device,
  `current` and `next`. Refreshing with `current` issues a new `next`
  (`current` keeps working, so a lost response is harmless). Refreshing with
  `next` promotes it to `current`, retires the old `current` and issues a new
  `next`. The device only persists the latest refresh token it received. A
  retired token presented again → `401 invalid_grant`, a security log line and
  a metric; the device is **not** revoked automatically (ops decides).
- **Revocation.** `devicectl revoke` blocks refresh at once (`403
  device_not_active`). Access tokens already issued stay valid until `exp`
  (at most `ACCESS_TOKEN_TTL`), and a gRPC stream authenticated when it opened
  may finish. That window is the price of stateless verification.

### How the server verifies an access token

No database lookup per upload request:

1. Take the bearer token (scheme case-insensitive) and parse it as a JWT.
   The algorithm is pinned to ES256 (`none`, HS256 and every other `alg` are
   rejected, so no algorithm confusion).
2. Pick the public key by `kid`: the signing key's own public key, plus every
   `*.pem` in `JWT_VERIFY_KEYS_DIR`. Unknown `kid` → rejected.
3. Check the signature, `iss` = `JWT_ISSUER`, `aud` contains `sensor-upload`,
   `exp` (required), `iat` not in the future, with 120 s leeway. `sub` must be
   a valid (lowercase) device_id (`^[a-z0-9][a-z0-9-]{2,63}$`; tokens are
   always issued for the lowercased id).
4. The token's device must equal the upload metadata `device_id`
   (create / `Init`), and only the owning device can see or touch an upload.

| Case | tus | gRPC |
|---|---|---|
| Missing, malformed, wrongly signed, unknown `kid`, wrong `iss`/`aud` | `401` `{"error":"invalid_token"}` | `UNAUTHENTICATED` `invalid_token` |
| Valid signature, past `exp` | `401` `{"error":"token_expired"}` (refresh, then retry) | `UNAUTHENTICATED` `token_expired` |
| Metadata `device_id` ≠ token device | `403` `{"error":"device_not_active"}` | `PERMISSION_DENIED` `device_not_active` |
| Another device's upload | `404` | `NOT_FOUND` |

The reason behind a rejection is only in the server log; tokens are never
logged. Uploads no longer answer 503 for auth: verification needs neither
Postgres nor any other service.

### Signing key rollover

`JWT_SIGNING_KEY_FILE` (PEM P-256 private key) signs new tokens. To replace
it: deploy the new key, put the **old public key** (PEM) into
`JWT_VERIFY_KEYS_DIR`, and remove it again after `ACCESS_TOKEN_TTL`, when no
token signed by it can still be valid. `kid` is derived from the key, so
there is nothing else to configure.

### Ops: `devicectl`

`devicectl` (replaces `devicekey`) ships in the server image next to
`/server` and reads `DATABASE_URL`, `JWT_SIGNING_KEY_FILE`, `JWT_ISSUER` and
`ACCESS_TOKEN_TTL`.

| Command | What |
|---|---|
| `list [-status ...]` | Devices and their status (never tokens or hashes) |
| `approve -device ID` / `reject -device ID` / `revoke -device ID` | Ops decisions; `revoke` blocks refresh immediately |
| `issue -device ID` | Dev/ops shortcut: activates the device and prints a token pair JSON |
| `access-token -device ID [-ttl 24h]` | Dev shortcut: activates the device, creating it if needed (refused if rejected/revoked; refresh tokens untouched) and prints only an access JWT (experiments, remote e2e) |

| Where | How |
|---|---|
| Dev stack (`make up`) | `make device-approve DEVICE=dev-1`, `make device-list`, `make device-issue DEVICE=dev-1` (token pair JSON), `make device-token DEVICE=dev-1 [TTL=24h]` (access JWT) |
| VPS | the same targets with a `vps-` prefix, e.g. `make -s vps-device-token VPS=root@$VPS_IP VPS_DEVICE=dev-1` (ssh + `docker compose exec server /devicectl ...`) |
| Any database | `DATABASE_URL=postgres://... JWT_SIGNING_KEY_FILE=certs/jwt.key go run ./cmd/devicectl issue -device dev-1` |

The uploader takes either a static access token (`-token`, e.g. from
`make device-token`) or `-refresh-token-file PATH`: a token pair JSON file
(e.g. `devicectl issue` output) that it refreshes and rewrites atomically.

```sh
make -s device-issue DEVICE=dev-1 > dev-1.json && chmod 600 dev-1.json
bin/uploader -protocol rest -url https://localhost:18443 -refresh-token-file dev-1.json \
  -ca certs/server.crt -file a.flac
```

## Where files are stored

1. **While uploading**, bytes go to `STAGING_DIR` (default
   `./data/staging`; `/data/staging` in the containers): `tus/` for both tus
   endpoints (tusd file store, `{id}` + `{id}.info`, and lock files) and
   `grpc/` for gRPC (`{id}.bin` + `{id}.json`). With `TUS_STAGING=s3` the
   tus parts go to `S3_BUCKET` instead (locks stay in `tus/`).
2. **On completion** the finalizer re-reads the staged file and checks its
   SHA-256 against the metadata `sha256` (mismatch → tus 460 / gRPC
   `DATA_LOSS`, nothing stored), deduplicates by sha256, then writes it to the object
   store under
   `{device_id}/{yyyy}/{mm}/{dd}/{sha256}{ext}`: the date is `captured_at` in
   UTC, the extension comes from the content type (`audio/flac` → `.flac`,
   unknown → `.bin`). The staged copy is deleted.
   - `OBJSTORE=local`: a file under `OBJSTORE_LOCAL_DIR` (default
     `./data/objects`).
   - `OBJSTORE=s3`: an object in `S3_BUCKET`. The dev compose stack uses
     MinIO bucket `uploads` (volume `minio`); the VPS uses MinIO with its data
     in `/srv/sensor-upload/minio` on the host.
3. **The record** is a row in Postgres table `uploads` (status `completed`,
   `storage_key` = the object key, `completed_at`). `GET
   /files/sha256/{sha256}` reads it.
4. **Retention** deletes the object and then the row after
   `RETENTION_MAX_AGE` (7 days), or earlier, oldest first, when `DISK_PATH`
   passes `DISK_HIGH_WATERMARK` ([details](#retention-and-disk-pressure)).
   Incomplete uploads are expired after `INCOMPLETE_TTL` by the expiry cleaner.

## File lookup (`GET /files/sha256/{sha256}`)

Answers "is this file already saved?" for a sensor, gateway or operator,
without a token.

| Request | Response |
|---|---|
| `{sha256}` is 64 lowercase hex and a **completed** upload has it | `200` `{"sha256":"…","status":"completed","size":2097152,"completed_at":"2026-10-22T08:57:32Z"}` |
| no completed upload (unknown, still uploading, failed, cancelled, expired) | `404` `{"sha256":"…","status":"not_found"}` |
| anything else as `{sha256}` (uppercase, wrong length, non-hex) | `400` `{"error":"sha256 must be 64 lowercase hex characters"}` |
| other method | `405`; database error → `500` `{"error":"internal error"}` |

Every answer carries `Cache-Control: no-store`. `completed_at` is RFC 3339
in UTC. Served on the TLS TCP listener (HTTP/1.1, HTTP/2) and on HTTP/3.
Metric `file_lookups_total{result="found|not_found|invalid|error"}`, one
`file lookup` log line per request (an invalid value is logged by length
only). The uploader has no verify mode; use curl (`--cacert certs/server.crt`,
or `-k` for an IP-only self-signed certificate you do not have):

```sh
curl -s --cacert certs/server.crt https://<host>:<port>/files/sha256/<hex>
```

## File list (`GET /files`)

Lists **completed** uploads, newest first (by `completed_at`), without a
token. In-progress, failed, cancelled and expired uploads are never listed,
and neither are storage keys or upload refs.

| Query | Meaning |
|---|---|
| `device_id` | Only this device (case-insensitive, like everywhere) |
| `content_type` | Exact type (`image/jpeg`) or a prefix ending in `*` (`image/*`) |
| `limit` | 1-200, default 50 |
| `cursor` | `next_cursor` of the previous page |

```sh
curl -s --cacert certs/server.crt 'https://<host>:<port>/files?device_id=WIH-001&content_type=image/*&limit=2'
# {"files":[{"device_id":"wih-001","sha256":"…","filename":"20261021_0530.jpg",
#   "content_type":"image/jpeg","size":48213,"captured_at":"2026-10-20T22:30:00Z",
#   "completed_at":"2026-10-20T22:31:04Z"}, …],"next_cursor":"MTc2…"}
```

`next_cursor` is absent on the last page. A bad parameter → `400`
`{"error":"…"}`; database error → `500`. Metric
`file_lists_total{result="ok|invalid|error"}`, one `file list` log line per
request.

**Security:** the endpoint is unauthenticated by design. It discloses only
whether a file with a given sha256 is stored (plus its size and completion
time); never the device, filename, object key, metadata or in-progress
uploads. Anyone who already has a file (or guesses its hash) can therefore
learn that it was uploaded. There is no rate limit; put it behind a reverse
proxy limit or firewall if that matters (see future work).

## Configuration

Environment variables (`internal/config`). An empty value counts as unset.
All problems are reported together and the server exits 1.

| Variable | Default | Description |
|---|---|---|
| `LISTEN_ADDR` | `:8443` | TLS TCP (HTTP/1.1, HTTP/2, `/grpc`) and QUIC UDP on the same port |
| `GRPC_NATIVE_ADDR` | *(empty = off)* | Native gRPC listener (TLS); serves bare and `/grpc`-prefixed paths |
| `METRICS_ADDR` | `:9100` | Plain-HTTP admin: `/metrics`, `/healthz`, `/readyz`. Unauthenticated and on all interfaces by default: firewall it or set e.g. `127.0.0.1:9100`. `/readyz` reports only `ok`/`fail` per check (`postgres`, `staging`, `objstore`); the error behind a failure is logged at `warn` (`readiness check failed`) |
| `TLS_CERT_FILE`, `TLS_KEY_FILE` | — | Required unless `BEHIND_PROXY`. TLS 1.2+ on TCP, 1.3 on QUIC |
| `BEHIND_PROXY` | `false` | `true` behind a TLS-terminating reverse proxy (e.g. APISIX): the public listener is plain HTTP/1.1 + h2c (gRPC), no HTTP/3 or `Alt-Svc`, tus `Location` URLs come from `X-Forwarded-Proto/-Host`, and the register rate limit keys on the last `X-Forwarded-For` entry. Only the proxy may reach `LISTEN_ADDR` |
| `DATABASE_URL` | — | Required. PostgreSQL URL; migrations run at startup |
| `JWT_SIGNING_KEY_FILE` | — | Required. PEM P-256 private key that signs access tokens (ES256); its public key is always accepted |
| `JWT_VERIFY_KEYS_DIR` | *(empty = none)* | Directory of extra accepted public keys (`*.pem`), e.g. the previous signing key during a rollover |
| `JWT_ISSUER` | `sensor-upload-server` | `iss` claim of issued and accepted tokens |
| `ACCESS_TOKEN_TTL` | `1h` | Access token lifetime (also the revocation window); must be shorter than `REFRESH_IDLE_TTL` |
| `REFRESH_IDLE_TTL` | `2160h` (90 d) | A refresh token unused this long is refused (`invalid_grant`) |
| `REFRESH_MAX_AGE` | `8760h` (365 d) | Refresh is refused this long after registration; the device registers again. Must be ≥ `REFRESH_IDLE_TTL` |
| `REGISTER_RETRY_AFTER` | `1h` | `Retry-After` of a pending registration (`202`) |
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
| `CLEANUP_INTERVAL` | `1h` | Expiry cleaner and age-retention period (first pass at startup) |
| `RETENTION_MAX_AGE` | `168h` | Completed uploads (object and row) older than this are deleted; `0` disables |
| `DISK_PATH` | `OBJSTORE_LOCAL_DIR` if `OBJSTORE=local`, else `STAGING_DIR` | Filesystem watched for disk pressure; with MinIO on the same host, mount MinIO's host data dir and point this at it |
| `DISK_HIGH_WATERMARK` | `85` | Used-space percent at which the oldest completed uploads are deleted (1–100) |
| `DISK_LOW_WATERMARK` | `70` | Deletion stops below this percent; must be lower than the high watermark |
| `DISK_CHECK_INTERVAL` | `1m` | Disk usage check period (first check at startup) |
| `GRPC_MAX_CHUNK_BYTES` | `262144` | Maximum gRPC chunk, returned in `InitResponse` |
| `GRPC_ACK_EVERY_N` | `1` | gRPC ack frequency (every N-th chunk; the final chunk always) |
| `QUIC_IDLE_TIMEOUT` | `60s` | QUIC max idle timeout |
| `QUIC_KEEPALIVE` | `20s` | QUIC keep-alive period; must be shorter than `QUIC_IDLE_TIMEOUT` |
| `CONTENT_TYPES` | FR-1a list | Comma-separated allow-list (default: `image/jpeg,image/png,image/webp,image/avif,audio/wav,audio/x-wav,audio/flac,audio/ogg,audio/opus,video/mp4,video/quicktime,application/json`) |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `SHUTDOWN_TIMEOUT` | `30s` | Graceful drain budget for all listeners on SIGTERM |

### Retention and disk pressure

This is a test server, so finished files are not kept forever. Every
`CLEANUP_INTERVAL`, completed uploads older than `RETENTION_MAX_AGE` (7 days)
are deleted: the object (local or S3) first, then the database row, so the
same file can be uploaded again afterwards. Every `DISK_CHECK_INTERVAL`, the
server measures `DISK_PATH` (statfs, the same `Use%` as df). At or above
`DISK_HIGH_WATERMARK` it deletes the oldest completed uploads, re-measuring
after each one, until usage is below `DISK_LOW_WATERMARK`. It logs a warning
if nothing completed is left to delete, or if deleting did not lower usage
(`DISK_PATH` is not the objects' disk). In-progress uploads are never
touched; expiry handles them. With MinIO on the same host, bind-mount the
host's MinIO data directory into the server container and set `DISK_PATH` to
it: the percentage is then that of the disk MinIO and staging share. Metrics:
`uploads_retention_deleted_total{reason="age"|"disk"}`, `disk_used_ratio`.
Deletes run in batches of 500, with one info log line (`retention batch`) per
batch.

Device state (registrations, approval, refresh token hashes) lives in Postgres
(see [Authentication](#authentication)). `JWT_ALGS`, `JWT_PUBLIC_KEY_FILE`,
`JWT_HMAC_SECRET` and `DEV_MODE` from the first JWT version no longer exist and
are ignored if set; use `LOG_LEVEL=debug` for what `DEV_MODE` used to switch on.

## Testing

| Command | What |
|---|---|
| `make test` | Unit tests with coverage (`go test -cover ./...`) |
| `make test-integration` | Everything with `-tags integration`: Postgres and MinIO via testcontainers-go, in-process e2e for all protocols incl. server restart |
| `make load-test` | NFR-2 / NFR-3 load tests (see below) |
| `make lint` | `go vet` and staticcheck, default and `integration` tags, plus `test/remote` (`remote` tag) |
| `make lint-load` | Same for the `load`-tagged files |
| `make e2e-remote` | Manual e2e suite against a running server (see "Remote e2e tests") |
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

## Remote e2e tests

`test/remote` (build tag `remote`) runs against an **already running**
server, e.g. a VPS, and never runs in `make test` or `make test-integration`.
Every run uploads fresh random content (~300 KiB, 64 KiB chunks), so reruns
do not collide. Without `REMOTE_URL` every test skips.

```sh
export REMOTE_URL=https://203.0.113.7:8443        # TLS listener (tus, /grpc, file lookup)
export REMOTE_GRPC_NATIVE=203.0.113.7:9090        # optional; native gRPC tests skip without it
export REMOTE_ADMIN_URL=http://203.0.113.7:9100   # optional; /healthz + /readyz
export REMOTE_CA_FILE=certs/server.crt            # the server's (IP-only) self-signed cert, or REMOTE_INSECURE=1
export REMOTE_TOKEN=$(make -s device-token DEVICE=remote-e2e TTL=24h)          # or vps-device-token / devicectl access-token
export REMOTE_OTHER_TOKEN=$(make -s device-token DEVICE=remote-e2e-2 TTL=24h)  # optional second device

make e2e-remote                                   # all tests
make e2e-remote RUN=TestRemoteUploadREST          # one test (RUN is anchored: ^(RUN)$)
make e2e-remote RUN='TestRemoteUpload.*|TestRemoteHealth'
make e2e-remote-list                              # list the test names
```

Tokens: `REMOTE_TOKEN` (required) is an access JWT from `devicectl
access-token` (it must outlive the run); the test device is its `sub`. `REMOTE_OTHER_TOKEN`
is an optional token of a second device; without it the cross-device
subtest ("other device's upload") skips. `REMOTE_TIMEOUT` (default 15m) is the `go test -timeout`. The raw gRPC
tests wait for an ack per chunk, so the server must run with the default
`GRPC_ACK_EVERY_N=1`. Uploads a test leaves unfinished are cancelled
(tus `DELETE`, gRPC `Cancel`) when it ends.

| Test | Checks |
|---|---|
| `TestRemoteHealth` | Public TLS listener answers (lookup → 404); with `REMOTE_ADMIN_URL`, `/healthz` and `/readyz` are `ok` |
| `TestRemoteUploadREST` | Contract client (`internal/client`) uploads over tus/HTTP/2; lookup → 200 with sha256 and size |
| `TestRemoteUploadRESTHTTP3` | Same over tus/HTTP/3 (UDP must be open) |
| `TestRemoteUploadGRPC` | Same over gRPC with the `/grpc` prefix on the TLS listener |
| `TestRemoteUploadGRPCNative` | Same on the native gRPC listener |
| `TestRemoteResumeREST` | tus: one chunk, a PATCH dropped mid-body, `HEAD` on a new connection gives the kept offset, finish from it |
| `TestRemoteResumeRESTHTTP3` | Same over HTTP/3 |
| `TestRemoteResumeGRPC` | gRPC: stream dropped after 2 acks; second `Init` returns the same `upload_id` and `committed_offset`, `Status` agrees, finish |
| `TestRemoteResumeGRPCNative` | Same on the native listener |
| `TestRemoteDuplicate` | After a completed upload: tus POST (TCP and HTTP/3) → 461, gRPC `Init` → `ALREADY_EXISTS`, client reports `duplicate` |
| `TestRemoteBadToken` | Missing, garbage and wrongly signed tokens → 401 / `UNAUTHENTICATED` (tus TCP + HTTP/3, gRPC, native) |
| `TestRemoteCrossDevice` | Metadata `device_id` ≠ token → 403 / `PERMISSION_DENIED`; another device's upload → 404 (tus HEAD/PATCH/DELETE) / `NOT_FOUND` (gRPC Status/Cancel), owner unaffected |
| `TestRemoteVerifyNotFound` | Random hash → 404 `not_found`; uppercase, short, long, non-hex → 400 |
| `TestRemoteMetadataValidation` | Bad sha256, captured_at, priority, content type, missing filename → 400; gRPC → `INVALID_ARGUMENT`; nothing stored |

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
TOKEN=$(make -s device-token DEVICE=netem-0001 TTL=72h)   # static access JWT that outlives the run; DEVICE defaults to its sub
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
- The experiment uses a static long-TTL access token instead of a refresh
  token file: parallel runs share one device, and each refresh would rotate
  the pair under the other runs. Once the token expires every run fails with
  `token_expired`, so pick a `TTL` longer than the whole experiment (the full
  matrix takes about 12 h; 72h leaves room for restarts).
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

## Deploy to a VPS

`deploy/vps/` runs the server, PostgreSQL and MinIO on one VPS with Docker
Compose, reachable by IP with a self-signed CA. It is meant for test and
experiment servers, not production. Design notes are in `docs/decisions.md`
"Decisions (vps)".

What runs there: device state in the VPS Postgres (`make vps-device-*`
runs `/devicectl` in the server container), an ES256 token signing key, random Postgres/MinIO
credentials, and a bucket-scoped MinIO user for the server. Restart policies,
healthchecks, json-file log rotation (10 MB × 5) and memory limits sized for
a 2–4 GB VPS (server 1 GB with `GOMEMLIMIT=800MiB`, MinIO 768 MB, Postgres
512 MB). Data is in bind mounts under `/srv/sensor-upload/{pgdata,minio,staging}`,
so `df`/`du` on the host show it.

| Port | Exposure | What |
|---|---|---|
| 9000/tcp + 9000/udp | public | TLS: `/rest/files/`, `/grpc/...` (TCP) and `/rest-http3/files/` (QUIC) |
| 9090/tcp | `127.0.0.1` | native gRPC (TLS); `GRPC_BIND=0.0.0.0` in `.env` to publish it |
| 9100/tcp | `127.0.0.1` | admin: `/metrics`, `/healthz`, `/readyz` (plain HTTP) |
| 9001/tcp | `127.0.0.1` | MinIO console |
| Postgres 5432, MinIO S3 9000 | not published | compose network only (MinIO's 9000 is container-internal, no clash) |

### Prerequisites

- A VPS running Ubuntu 24.04 LTS (Debian 12 works too) with root SSH access
  (key-based), 2–4 GB RAM, amd64 or arm64.
- On your machine: Go (see Quick start), `openssl`, `rsync`, `ssh`, and
  docker or podman (only for `build` and `save`; nothing is compiled inside
  a container).

### Steps

```sh
VPS_IP=203.0.113.10                      # your VPS public IP

# 1. Secrets, on your machine, into deploy/vps/secrets/ (git-ignored):
#    ca.crt/ca.key, server.crt/key (SAN IP:$VPS_IP), jwt.key (token signing key), .env.
#    Re-running keeps existing files. VPS_SAN=IP:...,DNS:... adds SANs.
make vps-secrets VPS_IP=$VPS_IP

# 2. One-time VPS setup as root: Docker Engine + compose plugin (Docker's apt repo),
#    /srv/sensor-upload dirs, ufw (22/tcp, 9000/tcp, 9000/udp), QUIC UDP
#    buffer sysctls (rmem_max/wmem_max = 7500000, /etc/sysctl.d). Safe to re-run.
make vps-bootstrap VPS=root@$VPS_IP      # VPS_DEPLOY_USER=ubuntu to deploy as a docker-group user

# 3. Deploy: cross-compiles cmd/server + cmd/devicectl for linux/amd64 (VPS_ARCH=arm64 for ARM),
#    builds the image from deploy/vps/Dockerfile.prebuilt, docker save | ssh docker load,
#    rsyncs docker-compose.yml + .env + certs (not ca.key), docker compose up -d,
#    and waits for /readyz.
make vps-deploy VPS=root@$VPS_IP

# 4. Devices: sensors register themselves, then approval and tokens (README "Authentication"):
make vps-device-list    VPS=root@$VPS_IP STATUS=pending
make vps-device-approve VPS=root@$VPS_IP VPS_DEVICE=dev-1
make vps-device-list    VPS=root@$VPS_IP
make -s vps-device-issue VPS=root@$VPS_IP VPS_DEVICE=dev-1 > dev-1.json   # dev shortcut: token pair JSON
make -s vps-device-token VPS=root@$VPS_IP VPS_DEVICE=dev-1 TTL=24h        # access JWT only

make vps-status VPS=root@$VPS_IP         # compose ps, /readyz, disk usage, images
make vps-logs   VPS=root@$VPS_IP SERVICE=server FOLLOW=1
make vps-tunnel VPS=root@$VPS_IP         # localhost:9100 (admin), localhost:9001 (MinIO console)
make vps-down   VPS=root@$VPS_IP         # stop; data is kept
make vps-wipe   VPS=root@$VPS_IP         # stop and delete all data (asks you to type the host)
```

`VPS_SSH_OPTS="-p 2222 -i ~/.ssh/vps"` is passed to ssh and rsync, and
`VPS_SSH_PORT` sets the SSH port ufw allows. The public port is
`HTTPS_PORT` in `deploy/vps/secrets/.env` (default 9000); if you change it,
pass the same value as `VPS_PUBLIC_PORT` to `make vps-bootstrap`. If `VPS` is an ssh alias, also set
`VPS_IP`. Tuning (`LOG_LEVEL`, `MAX_FILE_BYTES`, `INCOMPLETE_TTL`,
`CLEANUP_INTERVAL`, retention and disk watermarks) is in
`deploy/vps/secrets/.env`. Edit it and run `make vps-deploy` again.

Retention / disk pressure: `RETENTION_MAX_AGE=168h`, `DISK_HIGH_WATERMARK=85`,
`DISK_LOW_WATERMARK=70`, `DISK_CHECK_INTERVAL=1m`. `DISK_PATH=/data/disk` is
the MinIO data directory (`/srv/sensor-upload/minio`, mounted read-only into
the server), so the watermarks follow the filesystem that holds the stored
objects.

### Deploying from the VPS itself

If you would rather build on the server than on your machine, clone the
repo there and run one script as root. It bootstraps the host on the first
run, generates secrets for the public IP, builds the image with the root
Dockerfile (needs ~2 GB free RAM), installs everything into
`/srv/sensor-upload`, starts the stack and waits for `/readyz`:

```sh
git clone https://github.com/resyahrial/sensor-upload-server.git
cd sensor-upload-server && sudo deploy/vps/server-deploy.sh
# later: git pull && sudo deploy/vps/server-deploy.sh
```

Knobs: `PUBLIC_IP` (default: detected), `PUBLIC_PORT` (9000), `SSH_PORT`
(22, allowed in ufw), `UFW=0` to leave the firewall alone, `SAN=DNS:...`.
Clients need `deploy/vps/secrets/ca.crt` from the server (`ca.key` also
lives there, so keep the checkout private). Device admin:
`cd /srv/sensor-upload && sudo docker compose exec server /devicectl ...`.

### Behind a reverse proxy (APISIX)

When a proxy terminates TLS in front of the server
(`https://<public>:9000 → APISIX → <host>:9000`, plain HTTP), deploy with
`BEHIND_PROXY=true`; the script saves it in `.env` for later runs:

```sh
sudo BEHIND_PROXY=true deploy/vps/server-deploy.sh
```

Clients then use the proxy's URL and certificate (not `ca.crt`). Proxy-side
requirements:

- **tus** (`/rest/files/*`, `/v1/devices/*`, `/files/sha256/*`, `/files`, `/readyz`): HTTP
  upstream; keep the `Host` header (or send `X-Forwarded-Host`) and send
  `X-Forwarded-Proto: https`. Allow `PATCH`, `HEAD`, `DELETE` and the
  `Tus-Resumable`, `Upload-*` headers, a body size of at least the client's
  chunk size, and do not buffer request bodies (`proxy_request_buffering
  off`), or a dropped connection loses the whole chunk instead of resuming.
  The WAF should not inspect `application/offset+octet-stream` bodies.
- **gRPC** (`/grpc/*`): a route with upstream scheme `grpc` (h2c); the
  client connects to the proxy with TLS + HTTP/2.
- **HTTP/3 is not available** (`/rest-http3/` returns 404): the proxy speaks
  TCP to the server.
- Rate limiting: one tus upload is many `PATCH` requests, so limit per
  device/IP generously or exclude `PATCH /rest/files/*`.
- Only the proxy may reach port 9000 on the server, since `X-Forwarded-*`
  is trusted.


Clients trust `deploy/vps/secrets/ca.crt` (the CA, not the server
certificate). The certificate is valid for the IP only (plus any `VPS_SAN`),
so connect by IP.

```sh
TOKEN=$(make -s vps-device-token VPS=root@$VPS_IP VPS_DEVICE=dev-1)   # access JWT, 1h (TTL=24h for longer)
CA=deploy/vps/secrets/ca.crt
# or, refreshed automatically: make -s vps-device-issue ... > dev-1.json; bin/uploader -refresh-token-file dev-1.json ...

bin/uploader -protocol rest        -url https://$VPS_IP:9000 -token "$TOKEN" -device dev-1 -ca $CA -file a.flac -state rest.db
bin/uploader -protocol rest-http3  -url https://$VPS_IP:9000 -token "$TOKEN" -device dev-1 -ca $CA -file b.flac -state h3.db
bin/uploader -protocol grpc        -url https://$VPS_IP:9000 -token "$TOKEN" -device dev-1 -ca $CA -file c.flac -state grpc.db
curl -s -o /dev/null -w '%{http_code}\n' --cacert $CA https://$VPS_IP:9000/rest/files/   # 401 = TLS ok, token required
```

| Protocol | URL |
|---|---|
| tus over HTTP/1.1–2 | `https://<ip>:9000/rest/files/` |
| tus over HTTP/3 (UDP 9000) | `https://<ip>:9000/rest-http3/files/` |
| gRPC, `/grpc` prefix | `<ip>:9000` (method path `/grpc/upload.v1.UploadService/...`) |
| gRPC, native | `<ip>:9090`, loopback only unless `GRPC_BIND=0.0.0.0` |

Because the container listens on 9000 itself, `Alt-Svc` advertises
`h3=":9000"` (contract deviation 10 does not apply here).

### Experiment and remote e2e against the VPS

The netem experiment runs the shaped uploader containers on your machine
and points them at the VPS. They only need internet access, so use the
runtime's default network. Shaping is added on top of the real path to the VPS:

```sh
TOKEN=$(make -s vps-device-token VPS=root@$VPS_IP VPS_DEVICE=netem-0001 TTL=72h)
# Native gRPC (9090) is not public, so leave it out of the protocol list.
make experiment-smoke SERVER_URL=https://$VPS_IP:9000 PROTOCOLS='rest rest-http3 grpc' \
  TOKEN="$TOKEN" CA_FILE=deploy/vps/secrets/ca.crt NETEM_NETWORK=podman   # docker: bridge
```

Remote end-to-end suite (`make e2e-remote`):

```sh
# REMOTE_GRPC_NATIVE is left unset (9090 is not public), so the native gRPC tests skip.
make e2e-remote REMOTE_URL=https://$VPS_IP:9000 \
  REMOTE_TOKEN="$(make -s vps-device-token VPS=root@$VPS_IP VPS_DEVICE=e2e-remote TTL=24h)" \
  REMOTE_OTHER_TOKEN="$(make -s vps-device-token VPS=root@$VPS_IP VPS_DEVICE=e2e-remote-2 TTL=24h)" \
  REMOTE_CA_FILE=deploy/vps/secrets/ca.crt
```

Access tokens are checked against the **server's** clock (120 s leeway). A
401 `token_expired` means the static token outlived its TTL: mint a new one.
A 401 `invalid_token` on every request usually means the token was signed by
another key (e.g. after `vps-secrets` regenerated `jwt.key`) or another
`JWT_ISSUER`.

### Backups

Nothing is backed up automatically. For a test server, the simple way is to
stop the stack and copy the data:
`make vps-down`, then `tar` or `rsync` `/srv/sensor-upload` as root (pgdata is
owned by uid 70), then `make vps-deploy`. While the stack is running, use
`docker compose exec postgres pg_dump -U upload upload > upload.sql` (in
`/srv/sensor-upload`) and `mc mirror` for the bucket. Device state (batch
list, approvals, refresh token hashes) is in the database, so a pg_dump
includes it; after `make vps-wipe` every device must be imported, approved
and registered again. Keep `deploy/vps/secrets/` (CA key, `jwt.key`, `.env`)
somewhere safe. Without it you cannot renew the certificate for existing
clients, and a new `jwt.key` invalidates every outstanding access token.

### Security notes

- **Self-signed CA.** Clients must pin `ca.crt`; never use `-insecure` against
  the VPS. `ca.key` stays on your machine.
- **Tokens.** Access and refresh tokens are bearer secrets. A leaked access
  token works until its `exp` (1 h by default; a `TTL=72h` experiment token
  for 72 h); a leaked refresh token until the device is revoked
  (`devicectl revoke`). The server stores only refresh token hashes.
  `jwt.key` can mint tokens for any device, so protect it like `server.key`;
  anyone who can run `docker compose exec` on the VPS (root, docker
  group) can issue tokens.
- **Firewall.** ufw allows only SSH, 443/tcp, 443/udp and 9090/tcp. Docker
  publishes ports through its own iptables rules, which **bypass ufw**. So
  the compose file publishes nothing else on public interfaces: the admin
  port and the MinIO console are bound to `127.0.0.1` (use `make vps-tunnel`),
  and Postgres and MinIO S3 are not published at all. Keep it that way when
  adding `ports:`.
- **`GET /files/sha256/{sha256}` and `GET /files` have no auth**: anyone who
  can reach the server can ask whether a file with a given hash exists, and
  list every completed upload's device id, filename, type, size and times. Accept that on a test
  server, or restrict 443/9090 to known client IPs with your provider's
  firewall.
- The docker group is root-equivalent on the VPS. `server.key` is mode 640 with
  group 65532 (the container user) in a setgid `certs/` directory, and `.env`
  is mode 600.
- Only `POST /v1/devices/register` is rate limited (per IP); uploads have no
  rate limiting or per-device quotas (see known limitations).
  `DISK_HIGH_WATERMARK` and the expiry cleaner are what keep the disk from
  filling up.

### Upgrading

- New server version: update your checkout and run `make vps-deploy`. Each
  deploy loads a new image tag `localhost/sensor-upload-server:vps-<commit>-<time>`,
  retags it as `:vps` and recreates only the server container. Migrations
  run at startup. The three newest tags stay on the VPS for rollback:
  `docker tag localhost/sensor-upload-server:vps-<old> localhost/sensor-upload-server:vps && docker compose up -d`
  (on the VPS, in `/srv/sensor-upload`).
- New IP, or the certificate is about to expire (397 days):
  `make vps-secrets VPS_IP=<ip> VPS_SECRETS_ARGS=--renew-cert` re-signs the
  server certificate with the same CA, so clients keep working. Then run
  `make vps-deploy`, which restarts the server when certificates change.
- Postgres/MinIO image upgrades: bump the tags in
  `deploy/vps/docker-compose.yml` and run `make vps-deploy`. A Postgres major
  upgrade needs a dump and restore (or `make vps-wipe` on a test server).
- `VPS_SECRETS_ARGS=--force` regenerates everything, including the CA (clients
  need the new `ca.crt`) and `.env`. The new passwords no longer match the
  existing pgdata/MinIO data, so only use it together with `make vps-wipe`.
- OS updates: `apt-get upgrade` on the VPS. Containers restart with Docker
  (`restart: unless-stopped`).

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
18. **This server issues the tokens.** The requirements put token issuance
    out of scope; here `POST /v1/devices/register`, `POST
    /v1/devices/token/refresh` and `devicectl` live in the same binary (see
    [Authentication](#authentication)). Upload auth errors carry a JSON body
    (`{"error":"invalid_token"|"token_expired"|"device_not_active"}`) and the
    gRPC status message is the same code.

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
- **podman VM clock drift** (macOS, especially after the host sleeps):
  MinIO rejects SigV4 requests more than 15 min off (403, seen in integration
  tests), and access tokens are checked against the server container's clock
  with only 120 s leeway (a lagging clock rejects fresh tokens as issued in
  the future, a leading one as `token_expired`). Check with `date -u; podman machine ssh date -u` and re-step with
  `podman machine ssh "sudo date -u -s @$(date +%s)"`.
- **Two concurrent gRPC `Init` calls for the same new sha256 and device can
  create two in-progress rows.** Deduplication still holds: only one can
  complete; the other is cancelled as a duplicate or expires.
- **`deploy/docker-compose.yml` is a dev stack, not a deployment.** It runs
  with default Postgres/MinIO credentials and a plain-HTTP admin port, and
  publishes on `127.0.0.1` by default. For a real deployment use real
  credentials and `METRICS_ADDR` reachable only from the scraper, as
  `deploy/vps` does (see "Deploy to a VPS").
- **Revocation is not immediate for uploads.** `devicectl revoke` stops
  refresh at once, but access tokens already issued stay valid until `exp`
  (up to `ACCESS_TOKEN_TTL`, 1 h; longer for `devicectl access-token -ttl`
  tokens), and an open gRPC stream may finish. There is no revoked-token
  list; that is the price of stateless verification.
- **Refresh token reuse is only alerted, not acted on.** A retired refresh
  token presented again gets `invalid_grant` and a security log line and
  metric, but the device stays active until ops revokes it.
- **No per-device quota on in-progress uploads.** An authenticated device can
  create many uploads (each up to the size limit) and fill `STAGING_DIR` until
  they expire. Only the global size limit and expiry bound it.
- **`GET /files/sha256/{sha256}` is unauthenticated and not rate limited.**
  It reveals whether a file with a given hash is stored (and its size and
  completion time), nothing else. Each request is one indexed Postgres
  lookup, so a flood costs DB connections.
- **`POST /v1/devices/approve` is unauthenticated.** Anyone who can reach
  the server can approve any pending device, so approval no longer keeps
  unknown devices out: whoever registers an id first and calls approve gets
  tokens. It never re-activates a rejected or revoked device, and shares the
  per-IP register rate limit (10/min). Restrict it at the proxy (IP
  allow-list or an auth plugin on `/v1/devices/approve`) if that matters.
- **`POST /v1/devices/unregister` is unauthenticated.** Anyone who can
  reach the server can delete any device (active, rejected or revoked
  included), cutting it off at its next refresh and clearing an ops
  rejection; it can then be re-registered under a new poll secret by
  whoever calls first. Access tokens already issued stay valid until they
  expire; uploaded files are kept. Shares the register rate limit.
  Restrict it at the proxy like approve.
- **`GET /files` is unauthenticated and not rate limited.** It discloses
  every completed upload's device id, filename, content type, size and
  times (not the file contents or storage key). Each request is one indexed
  query of at most 201 rows. `GET /readyz` is public too and runs the
  Postgres, staging and object store checks per request.
- **Retention deletes for good.** Completed files are deleted after
  `RETENTION_MAX_AGE`, or earlier under disk pressure (oldest first); there is
  no archive. With `OBJSTORE=s3` on a remote bucket, set `DISK_PATH` to a disk
  that the objects actually fill (or raise the watermarks to 100/99): the
  default `STAGING_DIR` is filled by in-progress uploads, which retention
  cannot free. Retention stops after a batch that frees nothing and warns.
- Single instance only: the tus locker, gRPC stream registry, cleaner and
  retention are in-process.

## Future work

Ideas only, not implemented:

- Immediate revocation for uploads: an in-memory set of revoked device ids
  (or `jti`s), loaded from Postgres and kept for `ACCESS_TOKEN_TTL`, checked
  by the verifier.
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
- A per-IP rate limit on the unauthenticated `GET /files/sha256/{sha256}`
  (e.g. reusing the registration endpoint's limiter; today rely on a
  reverse proxy or firewall).
