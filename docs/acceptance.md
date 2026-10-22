# Acceptance (requirements §15)

Run on 2026-10-21 against the real compose stack (server image built from
this branch, PostgreSQL 16, MinIO `OBJSTORE=s3`, tus `file` staging), started as
a second stack next to other work:

```sh
export SERVER_IMAGE=localhost/sensor-upload-server:acc COMPOSE_NETWORK=sensor-upload-acc_net
make up COMPOSE="podman compose -p sensor-upload-acc" \
  HOST_HTTPS_PORT=38443 HOST_GRPC_PORT=39090 HOST_METRICS_PORT=39100
go build -o bin/ ./cmd/uploader ./cmd/devtoken
TOKEN=$(bin/devtoken -device dev-1 -alg ES256 -key certs/jwt.key)
```

Endpoints: `rest` and `rest-http3` = `https://localhost:38443` (TCP / QUIC),
`grpc` = `https://localhost:38443` with the `/grpc` prefix, `grpc-native` =
`localhost:39090`. CA `certs/server.crt`. Samples are random `.flac` files
(`head -c N /dev/urandom`), so each has its own sha256.
Checks were scripted (uploader + `psql` in the postgres container + `mc` in
the MinIO container). Cases the uploader cannot produce (wrong sha256, bad
tokens, a second device) used a small throwaway Go probe outside the repo that
speaks tus (HTTP/2 and HTTP/3 via quic-go) and gRPC (generated client). Nothing
was added to `cmd/uploader`.

The memory criterion (NFR-2) and the full experiment are not covered here.

| # | Criterion | Status |
|---|---|---|
| 1 | 200 MB over each endpoint | PASS |
| 2 | Client kill → resume, re-sent ≤ 1 chunk | PASS (gRPC client side too since `-grpc-window 1`; re-run 2026-10-22, see below) |
| 3 | Server kill → resume, no data loss | PASS |
| 4 | Wrong sha256 → 460 / DATA_LOSS, no object | PASS |
| 5 | Duplicate → 461 / ALREADY_EXISTS; concurrent duplicates → 1 row, 1 object | PASS |
| 6 | Missing / expired / wrongly signed / `alg: none` tokens rejected | PASS |
| 7 | Device cannot read, resume or cancel another device's upload | PASS |
| 8 | Incomplete uploads older than `INCOMPLETE_TTL` removed | PASS |
| — | `make experiment-smoke` on the real stack | see §9 (re-run 2026-10-22 with `-grpc-window 1`: 64 KiB better, 256 KiB worse) |
| — | testcontainers leak check | see §10 |

## 1. 200 MB over each endpoint — PASS

```sh
bin/uploader -url $URL -protocol $p -token "$TOKEN" -device dev-1 -ca certs/server.crt \
  -file big-$p.flac -state $p.db -report $p.csv      # p = rest | rest-http3 | grpc | grpc-native
```

| protocol | size | duration_ms | attempts | bytes_sent_total | result | DB row | MinIO object sha256 = file sha256 |
|---|---|---|---|---|---|---|---|
| rest | 209715200 | 7554 | 1 | 209715200 | ok | `rest, completed, 209715200/209715200` | `41b78374…ede35cf` = yes |
| rest-http3 | 209715200 | 61172 | 1 | 209715200 | ok | `rest-http3, completed` | `a1aafa1b…5fe2b58` = yes |
| grpc | 209715200 | 3684 | 1 | 209715200 | ok | `grpc, completed` | `75f60034…e6cfbac4` = yes |
| grpc-native | 209715200 | 3453 | 1 | 209715200 | ok | `grpc, completed` | `4bd5e904…34fc61f4` = yes |

Object keys are `dev-1/2026/10/21/{sha256}.flac`; `mc stat` size 209715200 for
each; the sha256 was computed by streaming `mc cat` of the object. (Both gRPC
variants store `protocol = 'grpc'`, see "Decisions (wiring)".)

## 2. Client kill + restart resumes — PASS

**Before** `-grpc-window` (2026-10-21; the gRPC rows are superseded by the
re-runs below). 100 MiB file per protocol, uploader started in the background; once the
outbox `sent_hwm` passed 50 MiB the uploader got `kill -9`; then the same
command was rerun with the same `-state` DB.

| protocol | server offset at resume | report after rerun (attempts, resumes, bytes_sent_total, bytes_resent, result) |
|---|---|---|
| rest | HEAD `Upload-Offset` 52690944 (= client `sent_hwm`) | 2, 1, 52166656, **0**, ok |
| rest-http3 | HEAD 52213260 (tusd kept a partial chunk; client `sent_hwm` 52428800) | 2, 1, 52644340, **215540** (< 262144), ok |
| grpc (`/grpc`) | Init `committed_offset` 17825792 | 2, 1, 87031808, **34603008**, ok |
| grpc-native | Init `committed_offset` 51380224 | 2, 1, 53477376, **1310720**, ok |

Final DB rows `completed`, `resume_count = 1`, object sha256 = file sha256 for
all four.

gRPC, server-side view (DB `committed_bytes` after the kill vs where the
server resumed):

| | killed stream log (`offset_after`, `bytes`) | DB `committed_bytes` after kill | resumed stream `offset_before` | re-received by server |
|---|---|---|---|---|
| grpc | 17825792, 17825792 (68 chunks), `Unavailable: transport is closing` | 17825792 | 17825792 | 0 |
| grpc-native | 51380224, 51380224 (196 chunks) | 51380224 | 51380224 | 0 |

The client-side `bytes_resent` for gRPC in the table above is larger than one
chunk because that client pipelined chunks without waiting for acks.
Everything it had sent beyond the last ack was still in socket / HTTP/2
buffers when it was killed and never reached the service; through the `/grpc`
prefix on the TLS listener that was about 33 MiB (the macOS podman
port-forwarder buffers a lot). The server received nothing twice.

**Re-run with `-grpc-window` (default 1; see "Decisions (grpc window)").** The
client now keeps at most N chunks sent but unacked. Same procedure (100 MiB,
`kill -9` once `sent_hwm` > 50 MiB, rerun with the same `-state`), `-protocol
grpc` (`/grpc` prefix on the TLS listener), chunk 262144. Server: `cmd/server`
built from this branch running on the host (Postgres in a podman container,
local object store), not the compose stack, so the podman port-forwarder was
not in the path; the bound below is enforced by the client and does not depend
on how much the path buffers.

| `-grpc-window` / server `GRPC_ACK_EVERY_N` | client `sent_hwm` at kill | DB `committed_bytes` after kill | report after rerun (attempts, resumes, bytes_sent_total, bytes_resent, result) |
|---|---|---|---|
| 1 / 1 (defaults) | 52690944 | 52690944 | 2, 1, 52166656, **0**, ok |
| 1 / 1 (defaults, 2nd run) | 52953088 | 52953088 | 2, 1, 51904512, **0**, ok |
| 4 / 1 | 52953088 | 52166656 | 2, 1, 52690944, **786432** (3 chunks ≤ 4), ok |
| 4 / 4 | 53477376 | 52953088 | 2, 1, 51904512, **524288** (2 chunks ≤ 4), ok |

All rows `completed`, `resume_count = 1`, object sha256 = file sha256. With the
default window the client-side `bytes_resent` is ≤ one chunk (here 0: the kill
landed between an ack and the next send), so the §15 bound now holds for gRPC
on the client side too; with window N it is ≤ N chunks. The bound is also
asserted in `internal/client` (`TestGRPCKillAndResume`,
`TestGRPCSIGKILLAndResume`, a real child-process SIGKILL).

**Re-run on the compose stack after the gRPC flow-control fixes (2026-10-22)
— PASS.** Branch at `ce32204` (includes the merge `5df41fd`). Server image
`localhost/sensor-upload-server:acc2` built from host cross-compiled binaries
(`CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w"
./cmd/server` copied into `distroless/static-debian12:nonroot`, the
Dockerfile's final stage) to avoid an in-VM compile; compose project
`sensor-upload-acc2` (`COMPOSE_NETWORK=sensor-upload-acc2_net`, host ports
48443 / 49090 / 49100), PostgreSQL 16, MinIO, `file` staging, server defaults
(`GRPC_ACK_EVERY_N=1`). So the macOS podman port-forwarder **is** in the path
here, unlike the host-server rows above. Uploader with its defaults
(`-grpc-window 1`, `-chunk 262144`); 200 MiB random file per run; started in
the background, `kill -9` of the uploader process once the outbox `sent_hwm`
passed 50 MiB, then the same command rerun with the same `-state`:

```sh
bin/uploader -url localhost:49090 -protocol grpc-native -token "$TOKEN" -device dev-1 \
  -ca certs/server.crt -file big.flac -state state.db -report second.csv -v
# grpc: -url https://localhost:48443 -protocol grpc
```

| protocol | client `sent_hwm` at kill | DB `committed_bytes` after kill | resumed stream `offset_before` | report after rerun (attempts, resumes, bytes_sent_total, bytes_resent, result) | DB row | object sha256 = file |
|---|---|---|---|---|---|---|
| grpc-native | 53215232 | 52953088 | 52953088 | 2, 1, 156762112, **262144** (1 chunk), ok | `completed`, `resume_count = 1` | `ffcb25a7…d1849fbe` yes |
| grpc-native (2nd run) | 53739520 | 53477376 | 53477376 | 2, 1, 156237824, **262144** (1 chunk), ok | `completed`, `resume_count = 1` | `18126db0…7f943fe069` yes |
| grpc (`/grpc`) | 54001664 | 53739520 | 53739520 | 2, 1, 155975680, **262144** (1 chunk), ok | `completed`, `resume_count = 1` | `47c1ad86…49504a8c` yes |

In every run the client had exactly one chunk sent but unacked at the kill
(`sent_hwm − committed_bytes = 262144`), re-sent that chunk after the restart
and nothing else, so `bytes_resent` ≤ one chunk holds on the client side for
both gRPC paths, including through the podman port-forwarder that buffered
~33 MiB in the "before" rows at the top of this section (those rows predate
`-grpc-window`). The server re-received nothing: the killed stream's log
line ends at `offset_after` = `committed_bytes` (`status Canceled`, `context
canceled`), and the resumed stream starts there. The `/grpc` row confirms the
`LimitReadAhead` / receive-buffer changes did not regress the prefix path.
Object sha256 was computed from `mc cat` of the stored object.

(A first `grpc-native` attempt was discarded: the harness killed the
wrapping shell instead of the uploader, so the "killed" process finished the
upload while the rerun started; the rerun reported `duplicate`. Harness
error, not a server or client defect.)

## 3. Server kill + restart — PASS

200 MiB file per protocol, uploader running with `-backoff-max 3s`. When the
client `sent_hwm` passed 50 MiB (tus) / 20 MiB (gRPC):
`podman kill -s KILL sensor-upload-acc_server_1` (container `Exited (137)`),
3 s pause, `podman start`. The same uploader process kept retrying and finished.

| protocol | DB at kill | resumed at | report (attempts, resumes, bytes_resent, result) | object sha256 = file |
|---|---|---|---|---|
| rest | `uploading` | HEAD 62390272 | 5, 1, 262144, ok | `785cacf5…17195037a` yes |
| rest-http3 | `uploading` | HEAD 55061258 | 2, 1, 136438, ok | `0fa1a657…df9612cf3` yes |
| grpc | `committed_bytes` 40894464 | Init 40894464 | 6, 1, 44302336, ok | `8341f5fa…ef6330e` yes |
| grpc-native | `committed_bytes` 46661632 | Init 46661632 | 6, 1, 1310720, ok | `5570c830…26e7c177` yes |

For gRPC the offset the server resumed from equals what it had durably
committed (acked) before the SIGKILL, so no acked byte was lost. In every case
the final object hashes to the file's sha256.

## 4. Corrupted upload — PASS

Probe: create with a random (wrong) `sha256`, send 1 MiB of other data.

```
rest        POST 201 → final PATCH 460 "sha256 mismatch" → HEAD 460
rest-http3  POST 201 → final PATCH 460 "sha256 mismatch" → HEAD 460
grpc        Init OK → Upload DataLoss "sha-256 mismatch" → Status NotFound
grpc-native Init OK → Upload DataLoss "sha-256 mismatch" → Status NotFound
```

DB: all four rows `failed_checksum` with `storage_key` NULL. MinIO: no object
key contains any of the four claimed sha256 values, and the bucket's object
count did not change (13 before and after). Staging: no file left for any of the
four upload ids (`find /data/staging -name '<id>*'` on the staging volume).

## 5. Duplicates — PASS

A file already completed in step 2, uploaded again with a fresh outbox each time:

```
rest        attempts=1 bytes_sent_total=0 result=duplicate   (server: POST 461 "already exists")
rest-http3  attempts=1 bytes_sent_total=0 result=duplicate   (POST 461)
grpc        attempts=1 bytes_sent_total=0 result=duplicate   (Init AlreadyExists)
grpc-native attempts=1 bytes_sent_total=0 result=duplicate   (Init AlreadyExists)
completed rows for that sha256: 1
```

Concurrent duplicates:

- Probe, 20 MiB, tus `/rest` and `grpc-native` at the same time: gRPC
  `Ack{complete:true}`, tus final PATCH `461 "already exists"`. Rows:
  `grpc completed`, `rest cancelled`. Completed rows: 1. MinIO objects: 1.
- Two uploader processes, 50 MiB, `rest-http3` and `grpc` at the same time:
  `grpc result=ok`, `rest-http3 result=duplicate` (both sent the full 50 MiB).
  Rows: `grpc completed`, `rest-http3 cancelled`. Completed rows: 1. MinIO objects: 1.

## 6. Auth — PASS

Every token variant against every endpoint, including requests on an
existing upload (HEAD/PATCH, Status/Upload):

```
endpoint     token                        tus POST/HEAD/PATCH/OPTIONS     gRPC Init/Status/Upload
rest         missing                      401/401/401/401
rest         expired (exp −10 min)        401/401/401/401
rest         wrong signature (other key)  401/401/401/401
rest         tampered payload             401/401/401/401
rest         HS256 with a wrong secret    401/401/401/401
rest         alg: none                    401/401/401/401
rest         garbage                      401/401/401/401
rest         valid (control)              201/200/204/200
rest-http3   (same 8 rows, same codes)
grpc         missing … garbage            Unauthenticated ×3 each
grpc         valid (control)              OK / OK / acked 1024
grpc-native  (same as grpc)
```

## 7. Cross-device — PASS

Device A (`dev-1`) created an upload and sent 256 KiB on each protocol; device B
(`dev-2`, valid token) then tried it:

```
rest        B: HEAD 404, PATCH 404 "upload not found", DELETE 404; A afterwards: HEAD 200, Upload-Offset 262144
rest-http3  B: HEAD 404, PATCH 404, DELETE 404;                    A afterwards: HEAD 200, Upload-Offset 262144
grpc        B: Status NotFound, Cancel NotFound, Upload NotFound;  A afterwards: Status OK, committed 262144
grpc-native B: Status NotFound, Cancel NotFound, Upload NotFound;  A afterwards: Status OK, committed 262144
B creating/Init with device_id=dev-1 in the metadata: 403 / PermissionDenied "device_id does not match token"
```

(404/NOT_FOUND rather than 403 for foreign uploads is the recorded FR-2 choice.)

## 8. Expiry — PASS

Server recreated with `INCOMPLETE_TTL=30s CLEANUP_INTERVAL=5s` (compose now
passes both through). One 1 MiB upload with 256 KiB sent was left on each protocol.

```
t=0   rest/rest-http3: uploading, staging tus/{id} + {id}.info; grpc/grpc-native: uploading, committed 262144, grpc/{id}.bin + .json
t=45s all four: status expired, 0 staging files
cleaner log: "upload expired" for each (ttl 30s), "expiry run" expired=4
rest        HEAD 410, PATCH 410 "upload expired"
rest-http3  HEAD 410, PATCH 410 "upload expired"
grpc        Status NotFound, Upload NotFound
grpc-native Status NotFound, Upload NotFound
```

The server was then recreated with the defaults (168h / 1h).

## 9. `make experiment-smoke` on the real stack — PASS

```sh
make netem-image        # rebuilt from this branch (needed the stack stopped: compiling sqlite OOMs the 2 GB VM otherwise)
TOKEN=<devtoken -device netem-0001 -ttl 12h, issued inside a container — see "Decisions (acceptance)", clock skew>
NETEM_NETWORK=sensor-upload-acc_net make experiment-smoke TOKEN="$TOKEN" CA_FILE=certs/server.crt
```

16 of 16 runs `ok` (exit 0), `make exit=0`. The first attempt failed all runs with
`create: HTTP 401` because the host clock was ahead of the podman VM clock and
the server rejected the future `iat` (expected behaviour; see decisions).
`results/summary.csv`:

```
protocol,profile,size,chunk,n,n_success,success_rate,duration_ms_median,duration_ms_p95,attempts_median,attempts_p95,resumes_median,resumes_p95,bytes_resent_median,bytes_resent_p95,overhead_median,overhead_p95
grpc,good-4g,51200,65536,1,1,1,749,749,1,1,0,0,0,0,1,1
grpc,good-4g,2097152,65536,1,1,1,4318,4318,1,1,0,0,0,0,1,1
grpc,very-poor,51200,65536,1,1,1,23920,23920,1,1,0,0,0,0,1,1
grpc,very-poor,2097152,65536,1,1,1,615343,615343,6,6,5,5,4915200,4915200,3.3438,3.3438
grpc-native,good-4g,51200,65536,1,1,1,1566,1566,1,1,0,0,0,0,1,1
grpc-native,good-4g,2097152,65536,1,1,1,4247,4247,1,1,0,0,0,0,1,1
grpc-native,very-poor,51200,65536,1,1,1,16711,16711,1,1,0,0,0,0,1,1
grpc-native,very-poor,2097152,65536,1,1,1,673791,673791,8,8,7,7,1638400,1638400,1.7813,1.7813
rest,good-4g,51200,65536,1,1,1,757,757,1,1,0,0,0,0,1,1
rest,good-4g,2097152,65536,1,1,1,5889,5889,1,1,0,0,0,0,1,1
rest,very-poor,51200,65536,1,1,1,19640,19640,1,1,0,0,0,0,1,1
rest,very-poor,2097152,65536,1,1,1,704210,704210,1,1,0,0,0,0,1,1
rest-http3,good-4g,51200,65536,1,1,1,696,696,1,1,0,0,0,0,1,1
rest-http3,good-4g,2097152,65536,1,1,1,5820,5820,1,1,0,0,0,0,1,1
rest-http3,very-poor,51200,65536,1,1,1,12809,12809,1,1,0,0,0,0,1,1
rest-http3,very-poor,2097152,65536,1,1,1,607916,607916,1,1,0,0,0,0,1,1
```

Observation for the full experiment (not a defect against §15), **before**
`-grpc-window` (that smoke ran an uploader image that pipelined without
limit): on `very-poor`, gRPC resumed 5–7 times and re-sent 1.6–4.9 MB for a
2 MB file (overhead 1.8×–3.3×), because the client pipelined chunks without
waiting for acks and everything after the last ack was sent again after each
outage. tus needed no new attempt there: tusd plus the 60 s per-request
timeout rode through the 5 s outages.

### Re-run with `-grpc-window 1` (2026-10-22) — 64 KiB PASS, 256 KiB worse

Targeted subset on the `sensor-upload-acc2` stack from §2 (branch at
`ce32204`, server defaults incl. `GRPC_ACK_EVERY_N=1`): profile `very-poor`,
`grpc` and `grpc-native`, 50 KB and 2 MB, both chunk sizes, 1 rep (`r1`, so
`OUTAGE_SEED=1` as in the run above). The netem image was rebuilt with the
current uploader: a host cross-compiled `cmd/uploader` (linux/arm64, same
flags as `Dockerfile.uploader`) copied into the `Dockerfile.uploader` runtime
stage, tagged `localhost/sensor-uploader-netem:acc2`, to avoid the in-VM
sqlite compile. Default `UPLOADER_ARGS` (`-retries 100000 -backoff-max 30s`),
so the uploader's default `-grpc-window 1`. Token issued on the host (VM
clock checked in step first). The driver was called directly because
`make experiment-smoke` fixes `PROFILES`/`SIZES`/`CHUNKS` and rebuilds the
image in the VM:

```sh
TOKEN=$(bin/devtoken -device netem-0001 -alg ES256 -key certs/jwt.key -ttl 12h) \
CA_FILE=$PWD/certs/server.crt RESULTS_DIR=$PWD/results \
NETEM_IMAGE=localhost/sensor-uploader-netem:acc2 NETEM_NETWORK=sensor-upload-acc2_net CONTAINER_RT=podman \
PROTOCOLS='grpc grpc-native' PROFILES=very-poor SIZES='50KB 2MB' CHUNKS='64KB 256KB' REPS=1 \
  deploy/netem/experiment.sh
make summary
```

8 of 8 runs recorded, all exit 0, driver exit 0. `results/summary.csv`:

```
protocol,profile,size,chunk,n,n_success,success_rate,duration_ms_median,duration_ms_p95,attempts_median,attempts_p95,resumes_median,resumes_p95,bytes_resent_median,bytes_resent_p95,overhead_median,overhead_p95
grpc,very-poor,51200,65536,1,1,1,24375,24375,1,1,0,0,0,0,1,1
grpc,very-poor,51200,262144,1,1,1,20666,20666,1,1,0,0,0,0,1,1
grpc,very-poor,2097152,65536,1,1,1,805851,805851,13,13,11,11,720896,720896,1.3438,1.3438
grpc,very-poor,2097152,262144,1,1,1,1276077,1276077,15,15,12,12,3407872,3407872,2.625,2.625
grpc-native,very-poor,51200,65536,1,1,1,17074,17074,1,1,0,0,0,0,1,1
grpc-native,very-poor,51200,262144,1,1,1,37344,37344,1,1,0,0,0,0,1,1
grpc-native,very-poor,2097152,65536,1,1,1,680011,680011,7,7,6,6,327680,327680,1.1563,1.1563
grpc-native,very-poor,2097152,262144,1,1,1,2013095,2013095,33,33,27,27,7602176,7602176,4.625,4.625
```

| cell (very-poor, 2 MB) | before (pipelined, 2026-10-21) | after (`-grpc-window 1`) |
|---|---|---|
| grpc, 64 KiB | 6 attempts, 4915200 re-sent, **3.34×**, 615 s | 13 attempts, 720896 re-sent, **1.34×**, 806 s |
| grpc-native, 64 KiB | 8 attempts, 1638400 re-sent, **1.78×**, 674 s | 7 attempts, 327680 re-sent, **1.16×**, 680 s |
| grpc, 256 KiB | not run | 15 attempts, 3407872 re-sent, **2.63×**, 1276 s |
| grpc-native, 256 KiB | not run | 33 attempts, 7602176 re-sent, **4.63×**, 2013 s, result `duplicate` |

50 KB cells: 1 attempt, overhead 1.0 in all four (as before for 64 KiB).

- **64 KiB chunks (the cells measured before): improved.** Overhead fell from
  3.34× / 1.78× to 1.34× / 1.16×. Each resume re-sends at most the one
  unacked chunk, as the window promises; the extra attempts on `grpc` cost
  little because so little is lost per attempt.
- **256 KiB chunks: worse than 64 KiB, and above the old 64 KiB numbers.**
  (No "before" exists for 256 KiB; the earlier smoke ran 64 KiB only.) The
  server log shows most streams ending `Canceled` by the client after 0 or 1
  committed chunks, 60–140 s into the stream, i.e. a 256 KiB chunk often
  does not get acked within the client's ack stall guard on this link. The
  guard is 60 s per started 256 KiB of chunk (`Runner.reqTimeout`), so a
  64 KiB chunk gets 60 s for 64 KiB but a 256 KiB chunk also gets only 60 s;
  256 KiB at 64 kbit is ≥ 33 s on the wire before 10 % loss and the 5 s
  outages every ~30 s (TCP backoff after an outage). Each trip throws away
  the partly sent chunk, which is then re-sent in full. To see the client's
  side, the `grpc-native` 2 MB / 256 KiB cell was run once more with
  `UPLOADER_ARGS='-retries 100000 -backoff-max 30s -v'` (rep `r2`, new file,
  `OUTAGE_SEED=2`; `RESULTS_DIR=results/verbose`): 41 attempts, 31 resumes,
  9961472 re-sent, overhead **5.75×**, 2818 s, `ok`. Of the 40 failed
  attempts, 20 were the stall guard (`upload: no ack for 1m0s with 262144
  bytes unacked`) and 20 were `Unavailable … read: connection timed out`
  (18 on `Upload`, 2 on `Init`). The latter is most likely the client's
  gRPC keepalive (`Time 30s, Timeout 10s` in `internal/client/runner.go`):
  grpc-go sets `TCP_USER_TIMEOUT` to the keepalive timeout on Linux, so data
  unacked by TCP for 10 s (a 5 s outage plus retransmit backoff at 10 % loss)
  kills the connection. Not confirmed beyond the error text. Both timeouts
  are client-side; tus (60 s per request, no TCP user timeout) does not
  have them.
- **`grpc-native` 2 MB / 256 KiB ended `duplicate`.** The server finalized
  the upload (`upload finalized … completed`, stream `OK`) but the client did
  not get the final `Ack{complete}` (the link dropped while it waited), so
  its retry's `Init` got `ALREADY_EXISTS` and the report says `duplicate`
  for the client's own upload. The file is on the server (counted in
  `n_success`); a reader of `result` alone would misread it.
- §15 is not affected (it bounds re-sent bytes per kill at one chunk; that
  holds, see §2). For the experiment, gRPC with 256 KiB chunks on
  `very-poor` is penalized by the client's stall-guard and keepalive
  timeouts rather than by the protocol; this should be decided before the
  full matrix runs (finding reported, code not changed here). One rep per
  cell, so these are indications, not medians.

## 10. testcontainers leak check — PASS (no leak, nothing to fix)

`podman ps -a --filter label=org.testcontainers=true` before and after a clean
`make test-integration` (all packages `ok`, exit 0):

- before: 20 containers, all `docker.io/postgis/postgis:16-3.4` from other
  projects' testcontainers sessions (left alone); none with this project's images
  (`postgres:16-alpine`, `pgsty/minio`).
- after: the same 20 IDs; `comm` of before/after is empty. No
  `postgres:16-alpine` or `pgsty/minio` container exists
  (`podman ps -a --filter ancestor=…` is empty), so there was nothing of ours to remove.

Every package already terminates its containers: `TestMain` → `os.Exit(run(m))`
with `defer testcontainers.TerminateContainer(ctr)` registered right after
`Run` (db, cleanup, grpcadapt, finalize, test/e2e), and
`testcontainers.CleanupContainer(t, ctr)` for per-test MinIO (objstore, e2e
`TestMinIO`). The remaining ways to leak are the ones no in-process cleanup can
cover: the process being killed (Ctrl-C / SIGKILL) or `go test`'s default 10 min
timeout panicking (the slowest package, e2e, takes about 15 s).

The first run of `make test-integration` failed (`internal/finalize` and e2e
`TestMinIO`: S3 `HeadObject` 403) because the podman VM clock had fallen hours
behind the host and MinIO rejects SigV4 requests skewed by more than 15 min.
Running `internal/finalize` again right after `podman machine ssh "sudo date -u -s @$(date +%s)"`
passed, and so did the full run with the VM clock kept in step. That run is the
one reported above. Even the failing run left no containers behind.

## NFR-2 / NFR-3 memory and concurrency

`make load-test LOAD_PROTOCOL=<p>` (`test/load`, see "Decisions (load)"): the
real `cmd/server` binary as a subprocess (GOMAXPROCS=2, no memory limit, local
object store, `file` staging, Postgres testcontainer in the 2 GB podman VM),
RSS sampled every 250 ms. NFR-2: 50 x 200 MiB uploaded concurrently in
256 KiB chunks, limit 300 MB. NFR-3: 200 x 1 MiB uploads held in progress for
30 s, `/healthz` probes and a fresh 1 MiB upload during the hold. Apple
Silicon Mac, 8 cores.

| protocol | NFR-2 peak RSS | NFR-2 wall / throughput | NFR-3 peak RSS | NFR-3 `/healthz` max / fresh 1 MiB upload | result |
|---|---|---|---|---|---|
| rest (HTTP/2) | 182.2 MB (161.3 MB in a first run) | 3m36s / 48.6 MB/s | 182.1 MB | 3 ms / 128 ms | PASS |
| rest-http3 | 55.3 MB | 16m31s / 10.6 MB/s | 103.6 MB | 1 ms / 1.34 s | PASS |
| grpc (`/grpc` on the TLS listener), **before** | **581.2 MB** and **738.8 MB** (two runs; median 175 / 240 MB) | 15m49s / 11.0 MB/s and 14m56s / 11.7 MB/s | 288.6 MB | 66 ms / **timed out** (30 s) | **FAIL** (NFR-2 and NFR-3) |
| grpc, **after** (flow-control fixes) | 170.5 MB | 3m40s / 47.7 MB/s | 240.0 MB and 245.8 MB (two runs) | 44 ms / 9.6 s | PASS |
| grpc-native (`GRPC_NATIVE_ADDR`), before the static window | not measured | | 379.4 MB and 369.9 MB | 54 ms / 11.8 s | **FAIL** (NFR-3 RSS) |
| grpc-native, **after** | 122.8 MB (125.7 MB in a re-run) | 3m38s / 48.1 MB/s | 197.4 MB (173.8 MB in a separate NFR-3 run) | 54 ms / 9.9 s | PASS |
| rest (HTTP/2), re-run after the 256 KiB HTTP/2 receive buffer | 200.2 MB | 1m20s / 131.4 MB/s | 207.8 MB | 3 ms / 46 ms | PASS |

rest and rest-http3 were measured on 2026-10-21 (wave 4); grpc "before" on
2026-10-22 (wave 5); the "after" rows on 2026-10-22 after the fixes in
"Decisions (grpc flow control)". In every NFR-2 run all 50 uploads completed and verified
(`completed`, size, sha256, object size); the gRPC runs failed only on the RSS
limit. NFR-3 "after": the held gRPC uploads take ~2m40s to finish, not 30 s
(about 320 durable chunk commits/s against 1,280 offered; see the NFR-3
finding below). The test passes because its criteria are responsiveness
(fresh upload < 30 s, `/healthz` < 1 s), RSS and completion. RSS on the gRPC
paths follows the GC target (about 2x the live heap) and varied between runs
by up to ~60 MB in the intermediate measurements.

### After the fixes (2026-10-22)

```
make load-test LOAD_PROTOCOL=grpc
RESULT NFR-2 wall=3m39.693s throughput=47.7 MB/s total=10485.8 MB per-upload p50=3m36.602s max=3m39.362s
RESULT NFR-2 server RSS peak 170.5 MB, median 167.3 MB, first 29.3 MB, last 91.7 MB (882 samples every 250ms, 0 errors) (limit 300.0 MB)
RESULT NFR-3 /healthz latency max=44ms p50=15ms over 10 probes
RESULT NFR-3 fresh 1.0 MB upload during hold: 9.631s
RESULT NFR-3 wall=2m38.776s held-upload p50=2m38.512s max=2m38.768s; peak RSS while held 159.9 MB
RESULT NFR-3 server RSS peak 240.0 MB, median 195.6 MB, first 29.6 MB, last 234.7 MB (636 samples every 250ms, 0 errors)
make load-test LOAD_PROTOCOL=grpc LOAD_RUN=NFR3   # repeat
RESULT NFR-3 server RSS peak 245.8 MB, median 218.3 MB (fresh upload 9.576s, /healthz max 28ms)

make load-test LOAD_PROTOCOL=grpc-native
RESULT NFR-2 wall=3m38.193s throughput=48.1 MB/s per-upload p50=3m36.961s max=3m38.033s
RESULT NFR-2 server RSS peak 122.8 MB, median 114.7 MB, first 24.2 MB, last 48.1 MB
RESULT NFR-3 /healthz latency max=54ms p50=16ms; fresh 1.0 MB upload during hold: 9.899s
RESULT NFR-3 wall=2m37.921s; peak RSS while held 147.6 MB
RESULT NFR-3 server RSS peak 197.4 MB, median 175.9 MB
```

What changed (details and the measurements behind each step in
`docs/decisions.md`, "Decisions (grpc flow control)"): a read-ahead limit on
the `ServeHTTP` path (`grpcapi.LimitReadAhead`, 64 KiB of buffers per
stream), a static 64 KiB stream window on the native listener, a 256 KiB
HTTP/2 receive buffer per stream on the TLS listener, and no per-chunk
rewrite of the staging info file. Intermediate gRPC runs with only some of
these in place passed NFR-2 at 246–283 MB, but NFR-3 failed at 327–729 MB.

### gRPC run details

```
make load-test LOAD_PROTOCOL=grpc                # run 1: NFR-2 + NFR-3
RESULT NFR-2 wall=15m48.978s throughput=11.0 MB/s total=10485.8 MB per-upload p50=15m35.103s max=15m39.219s
RESULT NFR-2 server RSS peak 581.2 MB, median 175.2 MB, first 28.9 MB, last 29.3 MB (3777 samples every 250ms, 0 errors) (limit 300.0 MB)
--- FAIL: TestNFR2Memory
NFR-3: every Init → INTERNAL "get upload: failed to connect … connection refused" (Postgres testcontainer port gone) → run discarded

make load-test LOAD_PROTOCOL=grpc LOAD_RUN=NFR3  # NFR-3 again
200 uploads in progress after 6.236s; upload_in_progress gauge mid-hold=200
RESULT NFR-3 /healthz latency max=66ms p50=42ms over 10 probes
RESULT NFR-3 fresh 1.0 MB upload during hold: (err upload: rpc error: code = DeadlineExceeded)
RESULT NFR-3 wall=9m5.205s held-upload p50=9m4.757s max=9m5.196s; peak RSS while held 288.6 MB
--- FAIL: TestNFR3Concurrency

make load-test LOAD_PROTOCOL=grpc LOAD_RUN=NFR2  # NFR-2 again (host kept awake)
RESULT NFR-2 wall=14m56.302s throughput=11.7 MB/s per-upload p50=14m52.146s max=14m55.33s
RESULT NFR-2 server RSS peak 738.8 MB, median 240.1 MB, first 29.2 MB, last 30.5 MB (3587 samples every 250ms, 0 errors)
--- FAIL: TestNFR2Memory
```

Environment notes: the host slept (on battery) during the first run, so its
wall time is the awake time measured by the test (3777 samples x 250 ms ≈ the
reported wall); the second NFR-2 run ran with `caffeinate` and gives the same
picture. The first NFR-3 attempt lost its Postgres container's port
(another agent's tests were using the same 2 GB VM); the repeat ran against
a working database, every held upload completed, and it still failed on time.
The podman VM clock was re-stepped before the run (it was 16 min behind);
it does not matter here (host-signed tokens are checked by a host process).

**NFR-2 finding (before the fix; fixed, see above).** On the `/grpc` path the server's
memory follows how far clients run ahead of what the server has committed,
not a fixed per-stream bound. The TLS listener hands gRPC to
`grpc.Server.ServeHTTP`. In that mode grpc-go
(`internal/transport/handler_server.go`, `HandleStreams`, v1.84.0) runs a
goroutine per stream that reads `req.Body` and appends every read to the
stream's `recvBuffer`, which has no limit. Reading the body gives the HTTP/2
flow-control window back to the client at once, so neither net/http's
per-stream window nor gRPC flow control limits it (grpc-go's docs: "ServeHTTP
does not support some gRPC features available through grpc-go's HTTP/2
server"). The service commits each 256 KiB chunk with two fsyncs (`.bin` and the
info file; `F_FULLFSYNC` on macOS) and a DB update before it reads the
next one, at about 11 MB/s across all 50 streams, while the server process
sat at 8–13 % CPU (sampled every 5 s in the second run). The load client
sends without waiting for acks, as the contract client does, so the backlog
builds up in server memory: median 175–240 MB, peaks 581–739 MB. The native
listener (`GRPC_NATIVE_ADDR`) uses grpc-go's own HTTP/2 transport, which does
enforce flow control; the load test does not exercise it. Possible fixes,
for the lead to decide: limit how far a stream may read ahead on the
`ServeHTTP` path (e.g. a bounded reader in front of `req.Body`), and/or a
client window. The uploader's `-grpc-window` (wave 5) limits the honest client
but not the server.

**NFR-3 finding (before the fix).** 200 concurrent gRPC streams sending 4 KiB chunks (6.4 per
second each, about 1,280 per second in total) need one durable commit per chunk
(two fsyncs + `UpdateCommitted`). The server managed about 70 per second
(38,400 chunks in 545 s), so the held uploads took 9 min instead of 30 s and
the fresh upload hit its 30 s deadline. `/healthz` stayed responsive (66 ms
max), RSS stayed under 300 MB, and all 200 uploads finished. tus has no
per-chunk commit (one paced PATCH per upload), so it passes. The cost per
commit is fixed and does not depend on chunk size, so small gRPC chunks
under high concurrency are the bad case. macOS `F_FULLFSYNC` and a Postgres
behind the podman port-forwarder make it worse; on Linux with a local
Postgres it may pass, but that was not measured. After dropping the per-chunk
info-file rewrite (one `.bin` fsync + `UpdateCommitted` per chunk left) the
rate is about 320 chunks/s and the fresh upload finishes in ~10 s, but the
held uploads still take ~2m40s rather than 30 s. Not re-measured with the
server on Linux.

### Image size (NFR-7)

The distroless image is 22.3 MB (arm64) / 23.8 MB (amd64) when its final
stage gets host cross-compiled static binaries (same flags as the
Dockerfile). A full in-VM `podman build` was OOM-killed in the 2 GB VM (about
500 MB free with other containers running). The existing `:dev` image built
earlier from the same Dockerfile is 30.9 MB. All are under the 50 MB limit:
PASS.
