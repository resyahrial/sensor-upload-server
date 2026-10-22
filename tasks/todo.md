# Task List — Sensor Upload Server

Conventions: `make test` = unit tests, `make test-integration` = testcontainers (Postgres + MinIO). Every task: `go build ./... && go vet ./...` green, and a commit per milestone (M1–M10).
Every judgment call → one line in `docs/decisions.md`.

---

## Phase 0: De-risk

### T0: API verification spike (no production code)
**Description:** Read current docs/source for tusd v2 (`handler.Config` hooks: `PreUploadCreateCallback`, `PreFinishResponseCallback`, `NotifyTerminatedUploads`, `handler.NewError` / custom HTTP status, `filelocker`, `filestore`, `s3store` part sizes), quic-go `http3.Server` + `quic.Config` (`MaxIdleTimeout`, `KeepAlivePeriod`), grpc-go `ServeHTTP` + keepalive options, pgx v5 pool, goose embed, testcontainers-go with podman. Write findings to `docs/decisions.md` (API notes section) and pin versions.
**Acceptance criteria:**
- [ ] Confirmed (or a workaround designed) that pre-finish can return 460/461 and block the final 204
- [ ] Confirmed the pre-create hook can set the upload ID (UUIDv7)
- [ ] Confirmed tusd v2 behavior for a concurrent PATCH on a locked upload (lock-release request vs 423) and the `Location` header format
- [ ] Confirmed the native `grpc.Server` serves a copied `ServiceDesc` named `grpc/upload.v1.UploadService` (prefixed path works on the native listener)
- [ ] testcontainers runs a Postgres container under podman locally
**Verification:** throwaway `go run` snippet in scratchpad; notes committed.
**Dependencies:** None · **Scope:** S

---

## Phase 1: Foundation

### T1: Scaffold, config, logging (M1)
**Description:** `go mod init github.com/resyahrial/sensor-upload-server`, §4 directory layout, `internal/config` (all §8 env vars, defaults, validation, `HS256` only with `DEV_MODE`), `internal/obs` slog JSON logger, Makefile skeleton (`build`, `test`, `lint`, `devcert`).
**Acceptance criteria:**
- [ ] Bad config (missing `TLS_CERT_FILE`, `HS256` without dev mode, bad duration) → non-zero exit with a clear message
- [ ] Config unit tests cover defaults and every validation rule
**Verification:** `make test`; `make devcert` produces cert/key.
**Dependencies:** T0 · **Files:** `go.mod`, `internal/config/*`, `internal/obs/log.go`, `Makefile`, `cmd/server/main.go` · **Scope:** M

### T2: Listeners, metrics/health server, graceful shutdown (M1)
**Description:** `internal/httpx` TLS TCP server (TLS 1.2+, h1+h2, NFR-4 timeouts), metrics server on `METRICS_ADDR` with `/metrics`, `/healthz`, `/readyz` (pluggable checkers). SIGTERM → drain within `SHUTDOWN_TIMEOUT`. Define all §10 metrics in `internal/obs/metrics.go` (no global registry; inject).
**Acceptance criteria:**
- [ ] `curl --http2 -k https://localhost:8443/` works; `/healthz` 200 on metrics port only
- [ ] SIGTERM exits cleanly within timeout (test with an httptest server)
**Verification:** `make test`; manual curl.
**Dependencies:** T1 · **Files:** `internal/httpx/server.go`, `internal/obs/metrics.go`, `cmd/server/main.go` · **Scope:** M

### T3: Compose, migrations, DB pool, readiness (M1)
**Description:** `deploy/docker-compose.yml` (server, postgres:16, minio + bucket init), `migrations/0001_uploads.sql` (§6 verbatim), goose embedded and run at startup, pgx pool, `/readyz` checks Postgres + staging dir writable + object store reachable (stubbed until T6). `make up`/`down`.
**Acceptance criteria:**
- [ ] `make up` starts everything; `/readyz` returns 200; stopping Postgres → 503
**Verification:** `make up && curl localhost:9100/readyz`.
**Dependencies:** T2 · **Files:** `deploy/docker-compose.yml`, `migrations/*`, `internal/db/{pool,migrate}.go`, `Dockerfile` (basic) · **Scope:** M

#### ✅ Checkpoint A (M1): `make up` green, `/readyz` 200 → commit `feat: M1 skeleton`

### T4: JWT auth + devtoken (M2)
**Description:** `internal/auth.Verifier` — ES256 (public key file) and HS256 (dev), rejects `none`/others via `jwt.WithValidMethods`, requires `device_id` + `exp`, 120 s leeway. Returns `device_id`. Typed errors `ErrUnauthenticated` / `ErrPermissionDenied`. `cmd/devtoken -alg -key -device -ttl`.
**Acceptance criteria:**
- [ ] Tests: valid, expired, nbf in future, wrong alg, `alg:none`, wrong key, missing device_id
- [ ] Coverage ≥ 80% on `internal/auth`
**Verification:** `go test -cover ./internal/auth`; devtoken output verifies.
**Dependencies:** T1 · **Files:** `internal/auth/*`, `cmd/devtoken/main.go` · **Scope:** S

### T5: Metadata model + validation (M2)
**Description:** `internal/meta.Metadata` + `Validate(now, cfg)` implementing FR-1/FR-1a (errors name the field), `ParseTusMetadata(map[string]string)` (tusd already decodes base64; also a raw `Upload-Metadata` header parser for malformed-header tests), `Extension(contentType)`, `FinalKey(m)` → `{device}/{yyyy}/{mm}/{dd}/{sha}{ext}` (UTC).
**Acceptance criteria:**
- [ ] Table tests for every field rule and boundary (24 h future, 255-char filename, `/` and `\`, priority 0/5, size 0/max+1)
- [ ] Final key + extension mapping tests for every allow-listed type
- [ ] Coverage ≥ 80%
**Verification:** `go test -cover ./internal/meta`.
**Dependencies:** T1 · **Files:** `internal/meta/*` · **Scope:** S

#### ✅ Checkpoint B (M2): unit tests green → commit `feat: M2 auth and metadata`

### T6: Object store (M3)
**Description:** `objstore.Store` interface: `Put(ctx, key, r io.Reader, size)`, `Stat(ctx, key) (size, exists, err)`, `Ping(ctx)`. Local impl (write temp + fsync + rename; key validated, no traversal). S3 impl with AWS SDK v2 `manager.Uploader` (multipart, bounded part size/concurrency), path-style option.
**Acceptance criteria:**
- [ ] Put is idempotent: an existing key with the same size → success without rewriting
- [ ] Integration test against MinIO (testcontainers), unit test for local
**Verification:** `make test && make test-integration`.
**Dependencies:** T0 · **Files:** `internal/objstore/{store,local,s3}.go` + tests · **Scope:** M

### T7: DB repository (M3)
**Description:** `internal/db.Repo`: `InsertUploading`, `Get(protocol, ref)`, `FindCompletedBySHA`, `FindInProgress(sha, device, protocol)`, `UpdateCommitted`, `IncrResume`, `Complete(tx…)` (maps unique violation `23505` → `ErrDuplicate`, conditional on `status='uploading'`), `SetStatus`, `ListStale(olderThan)`.
**Acceptance criteria:**
- [ ] Integration tests on Postgres for every query, including the duplicate-violation mapping
**Verification:** `make test-integration`.
**Dependencies:** T3 · **Files:** `internal/db/repo.go`, `internal/db/repo_test.go` · **Scope:** S

### T8: Finalizer (M3)
**Description:** `finalize.Finalizer.Run(ctx, Input{Protocol, Ref, Open func() (io.ReadCloser, error), Delete func(ctx) error, Meta})` implementing FR-8 steps 1–6 with `ErrChecksum`/`ErrDuplicate` sentinels, metrics, a `Notifier` interface + no-op. Retry-safe.
**Acceptance criteria:**
- [ ] Mismatch → row `failed_checksum`, staging deleted, nothing in object store
- [ ] Duplicate race (two goroutines, same sha) → exactly one `completed` row and one object
- [ ] Re-running after a simulated crash between steps 3 and 4 succeeds
- [ ] Coverage ≥ 80%
**Verification:** `make test && make test-integration`.
**Dependencies:** T5, T6, T7 · **Files:** `internal/finalize/*` · **Scope:** M

#### ✅ Checkpoint C (M3): finalizer integration green → commit `feat: M3 finalizer`

---

## Phase 2: Protocols

### T9: tus create path (M4)
**Description:** `internal/tusapi.New(basePath, protocol, deps)` builds a tusd handler with the filestore under `STAGING_DIR/tus` (or s3store), filelocker, extensions creation+termination+expiration, `MaxSize`. Pre-create: validate meta, dedup (461 `already exists`), size (413), set UUIDv7 ID, insert row. Mount on `/rest/files/`.
**Acceptance criteria:**
- [ ] `OPTIONS` advertises `Tus-Extension` + `Tus-Max-Size`; `POST` → 201 with `Location` under `/rest/files/` and `Upload-Expires`
- [ ] Invalid meta → 400 naming the field; completed duplicate → 461
**Verification:** integration test with a raw HTTP client.
**Dependencies:** T8, checkpoint on contract §2–3 · **Files:** `internal/tusapi/{handler,hooks}.go`, `internal/httpx/router.go` · **Scope:** M

### T10: tus auth/ownership middleware (M4)
**Description:** Middleware on every tus request: JWT → 401; POST metadata device mismatch → 403; for HEAD/PATCH/DELETE look up the row by `(protocol, id)`: missing/other device/other base path → 404, `expired` → 410. A HEAD on an in-progress upload increments `resume_count`. Map shutdown → 503. Per-request slog line (§10 fields).
**Acceptance criteria:**
- [ ] Cross-device HEAD/PATCH/DELETE → 404; no/expired/`none` token → 401
- [ ] Upload created on `/rest/files/` and accessed via the other handler → 404
**Verification:** integration tests.
**Dependencies:** T9, T4 · **Files:** `internal/tusapi/middleware.go` + tests · **Scope:** S

### T11: tus completion + termination (M4)
**Description:** `PreFinishResponseCallback` → finalizer with a reader from `store.GetUpload().GetReader`; ErrChecksum → 460, ErrDuplicate → 461, other → 500. Terminate → row `cancelled`. Startup recovery: rows `uploading` whose staged offset == size get finalized again.
**Acceptance criteria:**
- [ ] Last PATCH returns 204 only after the DB row is `completed` and the object exists
- [ ] Wrong sha → 460, staging removed; concurrent PATCH behaves as T0 found (409/423 or lock handoff), documented
**Verification:** integration tests.
**Dependencies:** T10 · **Files:** `internal/tusapi/hooks.go`, `internal/tusapi/recover.go` · **Scope:** S

### T12: tus integration suite incl. restart (M4)
**Description:** `test/e2e` harness that starts the server in-process (restartable) against testcontainers. Full upload over h1 and h2; client disconnect mid-chunk → resume; server stop mid-upload → restart → HEAD offset → resume → complete; duplicate; auth.
**Acceptance criteria:**
- [ ] All listed scenarios pass for `/rest/files/`
**Verification:** `make test-integration`.
**Dependencies:** T11 · **Files:** `test/e2e/{harness,tus}_test.go` · **Scope:** M

#### ✅ Checkpoint D (M4) → commit `feat: M4 tus over TCP`

### T13: HTTP/3 endpoint (M5)
**Description:** `http3.Server` on UDP `LISTEN_ADDR`, same cert, TLS 1.3, `quic.Config{MaxIdleTimeout, KeepAlivePeriod}`; second tusd handler at `/rest-http3/files/` sharing the store + locker. TCP returns 404 for `/rest-http3/*` and sets `Alt-Svc: h3=":port"`. Shutdown wiring. Extend the e2e suite with an `http3.Transport` client (full + resume + restart).
**Acceptance criteria:**
- [ ] Full upload over HTTP/3; `Location` uses `/rest-http3/files/`
- [ ] TCP `/rest-http3/files/` → 404; TCP responses carry `Alt-Svc`
**Verification:** `make test-integration`.
**Dependencies:** T12 · **Files:** `internal/httpx/h3.go`, `cmd/server/main.go`, `test/e2e/h3_test.go` · **Scope:** M

#### ✅ Checkpoint E (M5) → commit `feat: M5 http3`

### T14: Proto + codegen (M6)
**Description:** `proto/upload/v1/upload.proto` copied **exactly** from contract §4; `buf.yaml`, `buf.gen.yaml`, `make proto`; generated code committed.
**Acceptance criteria:** [ ] `buf lint` and `make proto` clean; generated code builds
**Dependencies:** contract file · **Scope:** XS

### T15: gRPC staging (M6)
**Description:** `internal/staging`: `Create(id, info)`, `Open(id)`, `WriteAt(off, data)` + `Sync`, `SaveInfo` (atomic write), `Truncate(off)` on resume, `Reader`, `Delete`. Paths only from a validated UUID.
**Acceptance criteria:**
- [ ] Unit tests: write/fsync/reopen/truncate/delete; a non-UUID id is rejected
- [ ] Coverage ≥ 80%
**Dependencies:** T1 · **Files:** `internal/staging/*` · **Scope:** S

### T16: gRPC Init / Status / Cancel (M6)
**Description:** `internal/grpcapi` auth interceptor (unary + stream; reads `authorization` metadata); Init (validate, dedup → ALREADY_EXISTS, idempotent resume for same sha+device in progress, create staging + row, `max_chunk_bytes`), Status (NOT_FOUND if unknown/expired/other device; resume_count), Cancel. Error mapping per §7.
**Acceptance criteria:**
- [ ] Unit tests via `bufconn` for each code path in §7
**Dependencies:** T8, T14, T15 · **Files:** `internal/grpcapi/{service,auth,errors}.go` + tests · **Scope:** M

### T17: gRPC Upload bidi stream (M6)
**Description:** Stream handler: single active stream per id (`ABORTED`), same-id check, offset == committed (`FAILED_PRECONDITION` with expected offset), size/chunk checks, WriteAt → fsync → DB `committed_bytes` → ack (every `GRPC_ACK_EVERY_N` plus always on the final chunk), finalize at end → `Ack{complete}` / `DATA_LOSS` / `ALREADY_EXISTS`. Per-stream idle timeout of 120 s. On resume, truncate `.bin` to the DB offset.
**Acceptance criteria:**
- [ ] bufconn tests: happy path, offset mismatch, oversize chunk, mixed ids, second stream ABORTED, checksum DATA_LOSS
- [ ] Coverage of `internal/grpcapi` ≥ 80%
**Dependencies:** T16 · **Files:** `internal/grpcapi/stream.go` + tests · **Scope:** M

### T18: gRPC routing, native listener, e2e (M6)
**Description:** Router sends `/grpc/` + `application/grpc*` to `grpcServer.ServeHTTP` with the prefix stripped; optional native listener on `GRPC_NATIVE_ADDR` that accepts **both** the bare and the `/grpc`-prefixed paths (the service is registered a second time under the name `grpc/upload.v1.UploadService`); keepalive + enforcement policy, `MaxRecvMsgSize = chunk + 64 KiB`. E2E: full upload via prefix and native, disconnect resume, server restart resume, cross-protocol duplicate race → one `completed` row.
**Acceptance criteria:**
- [ ] All three protocols pass full + resume + restart e2e
- [ ] The native listener serves a client using the contract's `/grpc` prefix interceptor and also a client without the prefix
**Dependencies:** T17, T13 · **Files:** `internal/httpx/router.go`, `cmd/server/main.go`, `test/e2e/grpc_test.go` · **Scope:** M

#### ✅ Checkpoint F (M6): three protocols end-to-end → commit `feat: M6 grpc` — **review with human**

---

## Phase 3: Lifecycle, client, experiment

### T19: Expiry cleaner (M7)
**Description:** `internal/cleanup` ticker every `CLEANUP_INTERVAL`: `ListStale`, skip if the tus lock is held or a gRPC stream is active, delete staging (tus via the store's terminater, gRPC via staging), set `expired`, metric `upload_failed_total{reason="expired"}`.
**Acceptance criteria:**
- [ ] Stale tus + gRPC uploads → expired, staging gone; later requests get 410 / NOT_FOUND
- [ ] Locked/streaming uploads untouched
**Dependencies:** T18 · **Files:** `internal/cleanup/*`, `test/e2e/cleanup_test.go` · **Scope:** S

#### ✅ Checkpoint G (M7) → commit

### T20: Uploader core (M8)
**Description:** `cmd/uploader` implementing contract §5: SHA-256 computed once when the file is queued, create, always ask the server for the offset before resuming (HEAD/Status), backoff 1 s doubling + jitter up to `-backoff-max` (default 5m), `-retries`, SQLite outbox (`modernc.org/sqlite`, contract §5.1 columns) so a killed client resumes, byte counters (sent / resent). Transports: `rest`, `rest-http3` (optional `-h3-fallback`, default off), `grpc` (contract prefix interceptor, used for both `:8443` and native `:9090`; `-protocol grpc-native` only changes the label/address). Handles 404/410/NOT_FOUND → new upload, 461/ALREADY_EXISTS → done, 460/DATA_LOSS → re-read file and re-upload.
**Acceptance criteria:**
- [ ] Killing the client mid-upload and rerunning resumes with ≤ 1 chunk re-sent, on all three protocols
**Dependencies:** T18, contract §5 · **Files:** `cmd/uploader/{main,tus,grpc,state,backoff}.go` · **Scope:** M

### T21: Uploader batch + report (M8)
**Description:** `-dir`, `-concurrency N`, `-report out.csv` with the exact §11 columns.
**Acceptance criteria:** [ ] 200 MB file succeeds on all three; CSV columns match §11
**Dependencies:** T20 · **Files:** `cmd/uploader/{batch,report}.go` · **Scope:** S

#### ✅ Checkpoint H (M8) → commit

### T22: Netem harness (M9)
**Description:** `deploy/netem/` uploader image + `NET_ADMIN`, one script per profile (rate/delay/jitter/loss via `tc qdisc ... netem rate`, link-down loop via `ip link set` or `tc ... loss 100%`), compose service.
**Acceptance criteria:** [ ] Each profile applies, and a 2 MB upload completes over it for each protocol
**Dependencies:** T21 · **Scope:** S

### T23: `make experiment` + summary (M9)
**Description:** Full-matrix driver (4 protocols × 4 profiles × 3 sizes (50 KB, 2 MB, 20 MB) × 2 chunks × 5 reps = 480 runs): `EXPERIMENT_PARALLEL=N` netem containers at once, resumable (skips cells already in the results), per-run timeout (default 2 h → `result=timeout`). Per-run CSVs → `results/summary.csv` with median and p95 per (protocol, profile, size, chunk) plus a success rate. The aggregator is a small Go program (`cmd/summarize`). A `make experiment-smoke` target runs a 1-rep, small-size subset to check the pipeline.
**Acceptance criteria:**
- [ ] `make experiment-smoke` produces `results/summary.csv` in < 30 min
- [ ] Killing and restarting `make experiment` continues where it left off
- [ ] Full `make experiment` completes for all profiles and produces `results/summary.csv`
**Dependencies:** T22 · **Scope:** S

#### ✅ Checkpoint I (M9) → commit

### T24: Hardening: image, lint, memory/concurrency (M10)
**Description:** Multi-stage Dockerfile → distroless/static nonroot (< 50 MB, CGO_ENABLED=0, amd64+arm64); `make lint` (vet + staticcheck); a load test with 50×200 MB concurrent uploads measuring RSS (< 300 MB), plus 200 concurrent in-progress uploads.
**Acceptance criteria:** [ ] Image < 50 MB; lint clean; RSS under limit (numbers recorded in the README)
**Dependencies:** T21 · **Scope:** S

### T25: Docs + acceptance review (M10)
**Description:** README (quick start, config table, test levels, reading results, limitations, contract deviations, future work), complete `docs/decisions.md`, go through §15 item by item.
**Acceptance criteria:** [ ] Every §15 box is checked with evidence (test name or command output)
**Dependencies:** all · **Scope:** S

#### ✅ Checkpoint J (M10): final review with human → commit
