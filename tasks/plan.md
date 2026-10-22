# Implementation Plan: Sensor Upload Server

## Overview
A single Go binary that takes in sensor files (photo/audio/video) over three protocols with the same behavior: tus 1.0 over HTTP/1.1+HTTP/2 (`/rest/files/`), tus 1.0 over HTTP/3 (`/rest-http3/files/`), and gRPC (`/grpc/...` prefix plus an optional native listener). Every protocol hands bytes to one shared finalizer, which verifies SHA-256, dedups, moves the file into object storage (local or S3) and records it in Postgres. Also in scope: auth (JWT), expiry cleanup, observability, a test client (`cmd/uploader`) and a netem harness for the protocol experiment.

Sources of truth: `sensor-upload-integration.md` (client contract — **wins on conflict**) and `upload-server-requirements.md`. The repo root is the `upload-server/` directory from §4 of the requirements. Module path: `github.com/resyahrial/sensor-upload-server`.

## Architecture Decisions (each goes into `docs/decisions.md` when implemented)
- **tus auth/ownership lives in HTTP middleware**, not tusd hooks. tusd has no pre-PATCH/HEAD hook, so a middleware in front of each tusd handler checks the JWT, pulls the upload ID out of the path, loads the DB row, and enforces device ownership, protocol/base-path match and expiry (410).
- **Cross-device access → `404`**, not `403`, so the upload's existence isn't revealed. The contract tells the client "404 → create a new upload", which is harmless here.
- **An upload from the wrong base path → `404`** (contract §3: it can't be resumed on the other endpoint).
- **TCP listener answers `/rest-http3/*` with `404`**, so misrouted clients become obvious.
- **tus `committed_bytes` is updated at completion only.** `resume_count` is still incremented on HEAD.
- **tus POST with an in-progress duplicate sha256 → new upload** (FR-3). Matches the contract's §3 fallback ("start a new upload; the server dedups by sha256").
- **Locker: tusd `filelocker`** on `STAGING_DIR/tus`. gRPC uses an in-process registry of active streams (gives `ABORTED`), which the cleaner also checks.
- **Finalizer crash recovery:** every step is idempotent. On startup, and on tus HEAD / gRPC Status / Init, any upload whose staged offset equals its size but is still `uploading` gets finalized again.
- **UUIDv7** (`github.com/google/uuid`) for row IDs, gRPC `upload_id` and tus upload IDs (set in pre-create).
- **Migrations: `goose`**, embedded, run at startup.
- **Native gRPC listener accepts both the bare and the `/grpc`-prefixed paths.** It's the same `grpc.Server` with the service registered twice: once as `upload.v1.UploadService` and once as a copy of the `ServiceDesc` with `ServiceName: "grpc/upload.v1.UploadService"`, so the wire path becomes `/grpc/upload.v1.UploadService/Init`. A contract-compliant client (prefix interceptor) therefore works against both `:8443` (`ServeHTTP`) and `:9090` (native transport) without changes; only the address differs. To be verified in T0.
- **Uploader outbox = SQLite via `modernc.org/sqlite`** (pure Go, no cgo), with the columns from contract §5.1. The contract names SQLite, so it takes precedence over the JSON option in the requirements.
- **Uploader backoff:** 1 s doubling with full jitter, default cap `-backoff-max 5m` (contract §5.1); the experiment passes `-backoff-max 30s`.
- **HTTP/3 → `/rest` fallback (contract §5.3)** is implemented in the uploader behind `-h3-fallback` (default **off**), so experiment runs measure only the requested protocol. If it triggers, the CSV `result` records `fallback`.
- **Ports:** `LISTEN_ADDR` defaults to `:8443` (requirements); production sets `:443` to match contract §3 (UDP 443). Listed under contract deviations in the README.
- **Experiment protocol labels:** `rest`, `rest-http3`, `grpc` (prefix via `:8443`), `grpc-native` (`:9090`). The DB `protocol` stays `grpc` for both gRPC variants.

## Dependency Graph
```
config, obs ─┬─> httpx (TLS listener, metrics server, shutdown)       [M1]
             ├─> db + migrations ─────────────┐                       [M1]
auth, meta ──┼────────────────────────────────┤                       [M2]
objstore ────┴─> finalize (uses db, objstore) ┤                       [M3]
                                              ├─> tusapi (/rest)      [M4]
                                              │     └─> http3 (/rest-http3) [M5]
proto ─> staging ─> grpcapi ──────────────────┤                       [M6]
                                              └─> cleanup             [M7]
contract §5 ─> cmd/uploader ─> deploy/netem + make experiment         [M8, M9]
```

## Task List
The full task breakdown with acceptance criteria is in `tasks/todo.md`. In summary:

### Phase 0: De-risk
- T0 Confirm third-party APIs (tusd v2 hooks/status codes/locking, `Location` format, quic-go, grpc dual registration, testcontainers with podman)

### Phase 1: Foundation (M1–M3)
- T1 Scaffold, config, logging · T2 Listeners, health, shutdown · T3 Compose, migrations, `/readyz`
- **Checkpoint A** (M1)
- T4 JWT + devtoken · T5 Metadata validation
- **Checkpoint B** (M2)
- T6 Object store · T7 DB repository · T8 Finalizer
- **Checkpoint C** (M3)

### Phase 2: Protocols (M4–M6)
- T9 tus create · T10 tus middleware · T11 tus completion · T12 tus e2e incl. restart
- **Checkpoint D** (M4)
- T13 HTTP/3
- **Checkpoint E** (M5)
- T14 Proto · T15 Staging · T16 Init/Status/Cancel · T17 Upload stream · T18 Routing + native + e2e
- **Checkpoint F** (M6) — human review

### Phase 3: Lifecycle, client, experiment (M7–M10)
- T19 Expiry cleaner — **Checkpoint G** (M7)
- T20 Uploader core · T21 Batch + report — **Checkpoint H** (M8)
- T22 Netem harness · T23 Resumable, parallel experiment driver + summary — **Checkpoint I** (M9)
- T24 Image, lint, memory/concurrency · T25 Docs + acceptance — **Checkpoint J** (M10)

## Experiment scope
The purpose is to compare **resumability** under bad links, not raw throughput, so the big sizes are dropped. Sizes: **50 KB, 2 MB, 20 MB** (no 200 MB in the experiment; the 200 MB acceptance check in §15 is still run over an unthrottled link).
4 protocols (`rest`, `rest-http3`, `grpc`, `grpc-native`) × 4 profiles × 3 sizes × 2 chunks × 5 reps = **480 runs**. Worst cell: 20 MB over `very-poor` ≈ 45–60 min, with dozens of outages and resumes per run. Rough total ≈ 50 h serial → ~12 h with `EXPERIMENT_PARALLEL=4`. The driver can be restarted (it skips finished runs) and stops each run after 2 h (`result=timeout`).
`leo-sat` "link off after 10 min": the link goes down permanently at 10 min. With ≤ 20 MB (~80 s at 2 Mbit) every run should finish before that, so this only matters if a run stalls.

## Risks and Mitigations
| Risk | Impact | Mitigation |
|---|---|---|
| tusd v2 may not support custom status codes (460/461) from hooks, or pre-finish may not block the final 204 | High | T0 checks tusd v2 source; fallback is to wrap the ResponseWriter in middleware. |
| tusd v2 lock behavior: a new request asks the current holder to release its lock (meant for resuming dropped connections), which can look different from "second request gets 423" | Med | T0 confirms the actual behavior; the test asserts what tusd does, and the deviation from §7's 423 is documented. |
| tusd emits an absolute `Location` URL; the contract shows a path | Low | Either form works with the contract client (`res.Location()` resolves both). Prefer whatever tusd emits; record it. |
| tus finalize crash window (bytes at offset==size, row still `uploading`) | High | Idempotent steps + recovery on startup/HEAD (see decisions). Covered by a restart test. |
| `docker` is aliased to podman → testcontainers-go needs `DOCKER_HOST` pointing at the podman socket and `TESTCONTAINERS_RYUK_DISABLED=true` | Med | T0 verifies; wired into the Makefile and README. |
| tusd `s3store` 5 MiB minimum part size vs 64–256 KiB chunks | Med | `file` stays the default staging; behavior documented; one s3 smoke test. |
| NFR-2 RSS < 300 MB for 50×200 MB | Med | Streaming everywhere; S3 multipart with bounded part size/concurrency; measured in T24. |
| Experiment still takes hours | Med | Parallel, resumable driver with per-run timeouts; `make experiment-smoke` for quick checks. |
| Dual service registration for the `/grpc` prefix on the native listener depends on grpc-go not validating service names | Low | T0 spike; fallback is a prefix-stripping `grpc.UnknownServiceHandler`. |

## Open Questions
- None blocking.

## Execution
Two agents work in parallel git worktrees branched from `feat/upload-server`. The lead merges each wave into `feat/upload-server`, then squash-merges into `master` at the end. Progress: `tasks/progress.md`.
