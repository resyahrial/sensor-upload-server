# Progress Tracker

Integration branch: `feat/upload-server` → squash-merged to `master` when done.
Agents: max 3 at a time (was 2 until wave 6, then 5), each in its own git worktree (`wt/<name>` branches).
Legend: `[ ]` todo · `[~]` in progress · `[x]` done and merged · `[!]` blocked/needs attention

## Wave 0 — Setup (lead)
- [x] Plan + task list (`tasks/plan.md`, `tasks/todo.md`)
- [x] Branch `feat/upload-server`, `go.mod`, commit contract + plan

## Wave 1
**Agent A — `wt/foundation`** (M1, M2) — merged @ 2f3ee96
- [x] T0 API checks: tusd v2, quic-go, testcontainers + podman
- [x] T1 Scaffold, config, logging
- [x] T2 TLS listener, metrics/health, graceful shutdown
- [x] T3 Compose, migrations, DB pool, `/readyz`
- [x] T4 JWT auth + `devtoken`
- [x] T5 Metadata validation

**Agent B — `wt/client`** (M6 part, M8) — merged @ 9cc43bf
- [x] T0 API check: grpc dual registration (`/grpc` prefix on native)
- [x] T14 Proto + buf codegen
- [x] T15 gRPC staging package
- [x] T20 Uploader core (rest / rest-http3 / grpc, SQLite outbox, backoff)
- [x] T21 Uploader batch + CSV report

- [x] Lead: merge wave 1, build + test + integration + lint green

## Wave 2
**Agent A — `wt/finalize`** (M3) — merged @ 8780d48
- [x] T6 Object store (local + S3)
- [x] T7 DB repository
- [x] T8 Finalizer

**Agent B — `wt/netem`** (M9 scaffolding) — merged @ 8e40c3a
- [x] T22 Netem profiles + uploader container (shaping measured; real-server run is pending, wave 4)
- [x] T23 Experiment driver + `cmd/summarize` + `make experiment-smoke`

- [x] Lead: merge wave 2 (build, integration tests, lint green)

## Wave 3
**Agent A — `wt/tus`** (M4, M5) — merged @ 1c3a3f0 (15 e2e scenarios over HTTP/1.1, HTTP/2 and HTTP/3)
- [x] T9 tus create path
- [x] T10 tus auth/ownership middleware
- [x] T11 tus completion + termination + recovery
- [x] T12 tus e2e incl. restart
- [x] T13 HTTP/3 endpoint

**Agent B — `wt/grpc`** (M6) — service merged @ 336948a (95% coverage); adapters + real-DB gRPC tests (incl. restart, duplicate race) merged @ 6c927b8
- [x] T16 gRPC Init / Status / Cancel
- [x] T17 gRPC Upload stream
- [x] T18 Prefix routing + native listener + e2e

- [x] Lead: merge wave 3 (all integration tests + lint green)

## Wave 4
**Agent A — `wt/wire`** — merged @ 10e32d7: gRPC + native listener + cleaner wired; 10 gRPC e2e tests; compose run OK on all 4 protocols
- [x] T19 Expiry cleaner (built by Agent B, merged; gRPC expiry tested. tus side: Agent A is implementing the `TusStaging` interface)
- [x] T24 load test merged @ fdbf100. NFR-2 peak RSS: rest 182 MB, rest-http3 55 MB (limit 300). NFR-3: 200 concurrent uploads, `/healthz` ≤ 3 ms. Image is 22–31 MB, static binary. [~] gRPC load run pending (after acceptance, to avoid disk/VM contention)

**Acceptance agent — `wt/accept`** — merged @ 01970de; all 8 §15 checks it ran PASS (evidence in `docs/acceptance.md`)
- [x] Uploader end-to-end: 200 MB × 4 protocols, client-kill resume, server-kill resume, 460/461, auth, cross-device, expiry
- [x] `make experiment-smoke` produces `results/summary.csv` (16/16 ok)

- [x] Lead: merge wave 4

## Wave 5
- [x] Agent A `wt/grpcwindow` (merged @ 5e333c0; client-kill `bytes_resent` 34 MB → 0 at window 1): limit the gRPC client's unacked window (`-grpc-window`, default 1). After a kill the gRPC client re-sent 34 MB, which fails §15 "re-sent ≤ 1 chunk", and on `very-poor` it had 1.8–3.3× overhead.
- [x] Agent B `wt/docs` (merged @ be01793): README (T25) + decisions cleanup
- [x] (fixed in wave 6) gRPC load test FAILED: NFR-2 peak RSS 581/739 MB (limit 300) on the `/grpc` prefix path, because grpc-go's ServeHTTP buffers the request body without limit. NFR-3 is slow: one durable commit per chunk, ~70 chunks/s on macOS.

## Wave 6
- [x] Fix `wt/grpcflow` (merged @ 5df41fd): 64 KiB read-ahead limit on `/grpc`, fixed 64 KiB stream window on native, 256 KiB HTTP/2 receive buffer, per-chunk `.json` write dropped. NFR-2 peak RSS: grpc 170.5 MB, grpc-native 122.8 MB, rest 200.2 MB. NFR-3: grpc 240–246 MB, grpc-native 197 MB, all PASS (limit 300). Caveat: held gRPC uploads take ~2m40s (~320 chunks/s on macOS)
- [x] Code review: 0 Critical, 3 Important (tus HEAD race can lose a staged file; tusd 500 bodies leak internal paths; gRPC finalize/DB calls have no deadline), 4 Minor (staging orphans after a crash, Cancel race → INTERNAL, some 500s should be 423/503, dead code)
- [x] Security audit: 0 Critical/High, 2 Medium, 4 Low. To fix: Go toolchain patch (36 reachable stdlib CVEs), native gRPC stream/header limits, compose bound to 127.0.0.1, HMAC min length, `/readyz` no raw errors, metadata header cap. Documented only: no per-device quota (would be a new feature; listed under future work).
- [x] Fix `wt/fix-tus` (merged @ 8d7bc8a): HEAD/PATCH race guard, generic 5xx bodies, 423/503 mapping, 4 KiB `Upload-Metadata` cap. Each fix has a test that failed before it.
- [x] Fix `wt/fix-cleanup` (merged): the cleaner finalizes fully staged rows instead of expiring them, retries fully committed rows each pass (≤ 32), sweeps orphan staging after a 1 h margin, and the finalize comments are corrected
- [x] Fix `wt/fix-infra` (merged @ 6444665 with cleanup): toolchain go1.26.10 (govulncheck: 36 → 0 reachable), builder images pinned by digest, compose bound to 127.0.0.1 with a generated HMAC secret, HMAC ≥ 32 bytes (lead lengthened the e2e/load test secrets), `/readyz` returns ok/fail only
- [x] Fix `wt/fix-grpc` (merged @ 4c37345): finalize detached with a 10 m timeout (stream, Init, Status), per-chunk commit 30 s timeout, Cancel re-checks the row → NOT_FOUND, native MaxConcurrentStreams(100) + MaxHeaderListSize(16 KiB). Each fix has a test that failed before it.
- [x] Acceptance refresh `wt/accept2` (merged): client-kill `bytes_resent` = 262144 (1 chunk) for grpc-native ×2 and grpc, PASS. very-poor smoke: 64 KiB overhead 1.16–1.34× (was 1.78–3.34×); 256 KiB 2.6–5.75× because of contract-prescribed client timeouts (kept; see decisions)
- [x] Final gates on `feat/upload-server`: build, vet (default/integration/load), `make test`, `make test-integration INTEGRATION_P=1` (20 packages ok), `make lint`
- [x] Reduced experiment (REPS=1, no 20 MB on `very-poor`): 88/88 runs succeeded, 14:50–15:40 local. tus resumes cheaply everywhere (overhead ≤ 1.04×). gRPC matches tus up to weak-4g (≤ 1.09×). On very-poor with 256 KiB chunks gRPC costs 2.75× (prefix) / 5.50× (native) and 2–4.5× the time, because of the contract's per-chunk timeout. Results in `results/summary.csv` (not committed); stack stopped, volumes kept.
- [x] T25 README, `docs/decisions.md`, §15 acceptance review
- [x] Squash merge `feat/upload-server` → `master`

## Wave 7 — user follow-ups
- [x] `wt/retention` (merged @ 8b6d960): deletes completed uploads (object + row) older than `RETENTION_MAX_AGE` (168h) every `CLEANUP_INTERVAL`; checks `DISK_PATH` every `DISK_CHECK_INTERVAL` (1m) and deletes oldest first from ≥ 85% until < 70%. Same file can be re-uploaded afterwards. Dev compose defaults to 100/99 (VM disk is shared); VPS uses 85/70 on the MinIO disk.
- [x] `wt/verify-e2e` (merged @ 8b0f9e9): unauthenticated `GET /files/sha256/{sha256}` over TCP and HTTP/3 (200 status/size/completed_at, 404, 400); 14-test remote suite `test/remote` (`make e2e-remote`, `RUN=TestName`, `make e2e-remote-list`). All 14 pass against a real server.
- [x] `wt/vps` (merged): `deploy/vps/` single-VPS stack (server + Postgres + MinIO, IP-only self-signed TLS, ES256), `make vps-secrets / vps-bootstrap / vps-deploy / vps-status / vps-logs / vps-token / vps-tunnel / vps-wipe`. Booted locally under podman: all 4 protocols uploaded. Not yet run against a real VPS (bootstrap and the ssh deploy path are untested).
- [x] Merge wave 7, gates (build, vet ×4 tags, test, integration 21 packages, lint), squash merge to `master`

## Wave 8 — device-key auth
- [x] `wt/devicekey` (merged): replace JWT with `Authorization: Bearer {device_id}_{key}` (split at last `_`; SHA-256 of key in Postgres `device_keys`; one active key per device, UPSERT replaces). New `cmd/devicekey` (set/delete/list), `make device-key`, `make vps-device-key`. README: Authentication + Where files are stored sections, feature overview. MQTT rotation is future work.
- [x] Merge, gates (build, vet ×4 tags, test, integration 21 packages, lint), update `master`. Remote suite 14/14 with device keys.

## Wave 9 — JWT access tokens + rotating refresh tokens (`feat/jwt-refresh`)
- [x] Foundation (b35e3a5): `internal/auth` ES256 issuer/verifier, key loading, refresh secrets; config (`JWT_*`, `ACCESS_TOKEN_TTL`, `REFRESH_*`, `REGISTER_RETRY_AFTER`); wiring in `internal/app`.
- [~] Parallel agents: device endpoints (register/refresh), upload auth + `devicectl` + make targets, uploader/experiment/remote suite, docs.
- [x] Docs: README "Authentication" rewritten, `docs/decisions.md` "Decisions (JWT + refresh)" (device keys marked superseded), `docs/acceptance.md` notes, `sensor-upload-integration.md` (register/approve + token refresh, client snippets).

## Notes / issues
- Wave 4 acceptance: the podman VM clock drifted up to 5 h behind the host, causing JWT 401s and MinIO 403s. Fixed by re-stepping the VM clock; documented in the README.
- Wave 4 B: building the full Dockerfile inside the podman VM gets OOM-killed (2 GB VM, about 520 MB free because of other containers). Image size was verified with cross-compiled binaries in the same distroless stage. HTTP/3 loopback throughput is about 4.5× lower than HTTP/2 (QUIC runs in user space, 2 cores).
- Wave 3 A deviations for the README: no 423 on a concurrent PATCH (tusd lock handoff); `Location` is an absolute URL; OPTIONS needs auth and returns 200; tus filestore doesn't fsync, so it survives a process crash but not an OS crash; `TUS_STAGING=s3` has small-chunk amplification and no recovery.
- Wave 2 B (for wave 4): netem expects the server as host `server` on network `deploy_default`, the TLS cert needs `server` as a SAN, and the devtoken TTL must be ≥ ~12 h for the full matrix. Host needs `gtimeout` (coreutils). Netem shapes egress only and sets MTU 1500 (podman's default is 65520).
- Wave 1 A (tusd v2.10.1 findings, for wave 3): no built-in `expiration` extension or `Upload-Expires` header, so it is added through a wrapper. A concurrent PATCH gets a lock handoff (the first request gets 400 `ERR_UPLOAD_INTERRUPTED`), never 423; this goes in the README deviations. After a failed finalize, a retried PATCH gets 204 without the pre-finish hook running, so the middleware must guard against it. Stale filelocker locks after a crash must be cleared at startup. `Location` is an absolute URL.
- Wave 1 A: `minio/minio` images can no longer be pulled anonymously; using `docker.io/pgsty/minio`.
- Wave 1 B: dual gRPC registration works (grpc-go v1.84.0), so the native listener accepts `/grpc/...` and bare paths.
- Wave 1 B: `~/go/bin/staticcheck` was built with go1.23 and can't load go1.26 modules; lint uses `go run honnef.co/go/tools/cmd/staticcheck@v0.8.2`.
- Decision (lead): the gRPC client streams without waiting for acks (contract §5.4), so after a client kill its CSV `bytes_resent` can be > 1 chunk (unacked HTTP/2 window). Server-side re-received bytes stay ≤ 1 chunk (tested). Accepted and to be listed in README known limitations; §15 "re-sent ≤ one chunk" is judged server-side for gRPC.
