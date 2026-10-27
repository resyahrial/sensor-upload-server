# Sensor Upload Integration Guide

How sensors and field gateways upload captured files (photo, audio, video) to the platform.

Three endpoints accept the same files with the same rules:

| Endpoint | Protocol | Transport | Use when |
|---|---|---|---|
| `/rest` | tus 1.0 (REST) | HTTPS, HTTP/1.1 or HTTP/2 over TCP | Default; widest client support |
| `/rest-http3` | tus 1.0 (REST) | HTTP/3 over QUIC (UDP 443) | Lossy links or changing IPs; UDP must be allowed |
| `/grpc` | gRPC | HTTP/2 over TCP | Gateways already using gRPC |

Base URL: `https://<platform-host>` (provided per environment).

---

## 1. Common Rules

### Authentication

Every upload request carries the sensor's short-lived **access token**:

```
Authorization: Bearer <access-token>
```

Each sensor holds two tokens. Tokens are never shared between sensors.

| Token | Format | Lifetime | Used for |
|---|---|---|---|
| **Access token** | JWT (ES256), opaque to the sensor | **1 hour** | `Authorization` header on every upload request |
| **Refresh token** | 32 random bytes as 64 lowercase hex characters | Until unused for **90 days**, at most **365 days** after registration | Getting a new access token (section 3) |

- Both are issued together, as a **token pair**, when ops approves the sensor (section 2), and renewed by refresh (section 3).
- The platform checks the access token's signature and expiry without a database lookup, so uploads do not depend on the token store.
- The platform stores only a SHA-256 hash of the refresh token, never the token itself.
- The `device_id` in the upload metadata must match the device the access token belongs to.
- Always send tokens over TLS; never log them.

### File identity and limits

| Rule | Value |
|---|---|
| File ID | SHA-256 of the whole file, lowercase hex (64 chars) |
| Chunk size | 64–256 KB (smaller on worse links) |
| Max file size | 500 MB |
| Incomplete upload expiry | 7 days |
| Duplicate | Same `sha256` already completed → rejected as "already exists"; treat as success |
| Integrity | Server verifies SHA-256 on completion |

### Required metadata

| Key | Example | Notes |
|---|---|---|
| `device_id` | `bioac-0142` | Must match the access token's device |
| `sha256` | `9f86d081…` | 64 hex chars |
| `filename` | `20261021_0530.flac` | Original filename |
| `content_type` | `audio/flac` | MIME type |
| `captured_at` | `2026-10-21T05:30:00+07:00` | RFC 3339 |
| `priority` | `2` | 1 alert · 2 thumbnail · 3 clip · 4 full file |

### Auth errors (all endpoints)

| REST | gRPC | Meaning | Sensor action |
|---|---|---|---|
| `401` `{"error":"token_expired"}` | `UNAUTHENTICATED`, message `token_expired` | Access token past its expiry | Refresh (section 3), then retry the request once |
| `401` `{"error":"invalid_token"}` | `UNAUTHENTICATED`, message `invalid_token` | Access token missing, malformed, wrongly signed, signed with an unknown key, or for another audience | Refresh once and retry; if it fails again, stop and log |
| `403` `{"error":"device_not_active"}` | `PERMISSION_DENIED`, message `device_not_active` | Metadata `device_id` does not match the token's device | Stop uploading; keep files in the outbox |

Upload requests never fail with an auth `503`: the access token is checked locally by the server. A revoked device learns about it on its next refresh (section 3).

---

## 2. Getting a Token Pair

A sensor registers itself when installed and receives its token pair once ops approves it in the platform.

**First boot:** the sensor generates a random **poll secret** (32 random bytes, 64 lowercase hex characters) and stores it. Only a request carrying the same poll secret can collect the tokens, so knowing a device ID is not enough to steal them.

### Register / poll

```http
POST /v1/devices/register HTTP/1.1
Host: <platform-host>
Content-Type: application/json

{
  "device_id": "bioac-0142",
  "poll_secret": "<64 hex chars>",
  "lat": -1.234567,
  "lon": 113.456789,
  "firmware": "1.4.0"
}
```

The sensor repeats the same request to poll until ops decides. No `Authorization` header is needed.

| Response | Meaning | Sensor action |
|---|---|---|
| `202 Accepted` `{"status":"pending"}`, `Retry-After: 3600` | Pending ops approval | Keep buffering files; poll again after `Retry-After` seconds |
| `200 OK` with the token pair (below) | Approved | Save the pair (section 7.1), then delete the poll secret |
| `400` `{"error":"invalid_request"}` | Malformed body or field | Fix; do not retry |
| `403` `{"error":"device_not_allowed"}` | Device ID is not on the pre-imported batch list | Stop; needs a technician |
| `403` `{"error":"device_not_active"}` | Rejected or revoked by ops | Stop; needs a technician |
| `409` `{"error":"poll_secret_mismatch"}` | A different poll secret is already registered for this device ID | Stop; needs a technician |
| `429` `{"error":"rate_limited"}` | Too many requests from this IP | Back off; honour `Retry-After` if present |

**Token pair** (the same shape is returned by registration and by refresh):

```json
{
  "device_id": "bioac-0142",
  "access_token": "<JWT>",
  "token_type": "Bearer",
  "expires_in": 3600,
  "refresh_token": "<64 hex chars>",
  "refresh_expires_at": "2027-10-27T03:20:00Z"
}
```

- `expires_in` is the access token's lifetime in seconds. `refresh_expires_at` is the latest moment the refresh token can work (365 days after registration); it stops earlier if unused for 90 days.
- If the `200` response is lost, poll again with the same poll secret: the platform returns a token pair again, until the refresh token is used for the first time. After that the poll secret is cleared and no longer works.
- The platform checks that the device ID is on the pre-imported batch list, keeps the poll secret's hash from the first call, and shows ops the device ID, location and time before approving.

**Registering again:** when the refresh token has expired (`invalid_grant`, section 3), generate a **new** poll secret and register again. The device goes back to pending and ops approves it again.

---

## 3. Token Refresh

The access token lives for 1 hour. The sensor trades its refresh token for a new pair before the access token runs out. Every refresh returns a **new refresh token**; the sensor always keeps the latest one it received.

### When

- **Proactively** at about **80% of `expires_in`** (≈ 48 min), measured from when the sensor received the pair, with its own (monotonic) clock. Do not compare the JWT's `exp` with the device clock: this way a wrong device clock does not matter.
- **Reactively** once, after an upload request fails with `token_expired` (or `invalid_token`); then retry that request once.

### Refresh

```http
POST /v1/devices/token/refresh HTTP/1.1
Host: <platform-host>
Content-Type: application/json

{"refresh_token": "<64 hex chars>"}
```

| Response | Meaning | Sensor action |
|---|---|---|
| `200 OK` with the token pair (section 2) | New pair issued | Save it atomically, then use the new access token |
| `400` `{"error":"invalid_request"}` | Malformed body | Fix; do not retry |
| `401` `{"error":"invalid_grant"}` | Refresh token unknown, already replaced, unused for 90 days, or older than 365 days | Register again with a new poll secret (section 2) |
| `403` `{"error":"device_not_active"}` | Device revoked | Stop uploading; keep files in the outbox; needs a technician |
| `503` `{"error":"temporarily_unavailable"}`, timeout, connection lost | Transient | Back off and retry; keep using the current access token until it expires |

### Lost responses

The platform keeps two refresh tokens per device: **current** and **next**.

1. Refresh with the current token: the platform issues a new token as **next**. The current token still works.
2. Refresh with the next token: next becomes current, the old current token is retired, and the new token becomes next.

So the sensor only ever needs the **latest** refresh token it received: save it (temp file, fsync, rename, mode `0600`) before using the new access token.

| Failure | Result |
|---|---|
| The refresh response never arrives | The token the sensor sent still works; refresh again with it |
| Saved the new pair, link dropped before using it | Continue with the saved pair |
| Crash while saving | The atomic rename leaves either the old or the new pair, and both refresh tokens work |

A **retired** refresh token presented again is refused with `invalid_grant` and raises a security alert for ops; the device is not revoked automatically. A sensor that follows the rules above never sends a retired token.

### Revocation

When ops revokes a device, its next refresh gets `403 device_not_active`. An access token already issued keeps working until it expires (at most 1 hour), and a gRPC stream opened with it may finish. If a token may have leaked, ops revokes the device and it registers again.

---

## 4. Contract — REST (tus) `/rest`

Protocol: [tus 1.0](https://tus.io/protocols/resumable-upload) with the **creation**, **termination** and **expiration** extensions.

All requests include `Tus-Resumable: 1.0.0` and `Authorization: Bearer <access-token>`.

| Step | Request | Required headers | Success response |
|---|---|---|---|
| Create | `POST /rest/files/` | `Upload-Length`, `Upload-Metadata`, `Authorization` | `201 Created`, `Location: /rest/files/{id}` |
| Upload chunk | `PATCH /rest/files/{id}` | `Upload-Offset`, `Content-Type: application/offset+octet-stream`, `Authorization` | `204 No Content`, `Upload-Offset: <new offset>` |
| Get offset (resume) | `HEAD /rest/files/{id}` | `Authorization` | `200 OK`, `Upload-Offset`, `Upload-Length` |
| Cancel | `DELETE /rest/files/{id}` | `Authorization` | `204 No Content` |

**`Upload-Metadata` format:** comma-separated `key base64(value)` pairs.

```
Upload-Metadata: device_id YmlvYWMtMDE0Mg==,sha256 OWY4NmQwODE...,filename MjAyNjEwMjFfMDUzMC5mbGFj,content_type YXVkaW8vZmxhYw==,captured_at MjAyNi0xMC0yMVQwNTozMDowMCswNzowMA==,priority Mg==
```

**Upload is complete** when a `PATCH` response returns `Upload-Offset == Upload-Length` with `204`.

### Errors

Auth errors (`401`, `403`) are in section 1.

| Status | Meaning | Sensor action |
|---|---|---|
| `400` | Missing or invalid metadata | Fix; do not retry |
| `404` / `410` | Upload unknown or expired | Create a new upload |
| `409` | Offset mismatch | `HEAD`, resume from returned offset |
| `413` | File too large | Do not retry |
| `460` | SHA-256 mismatch on completion | Re-read file from SD, re-upload |
| `461` | Already exists (same `sha256` completed) | Mark as done |
| `5xx`, timeout, connection lost | Transient | Back off, `HEAD`, resume |

### Example (curl)

```bash
# 1. Create
curl -i -X POST https://<platform-host>/rest/files/ \
  -H "Tus-Resumable: 1.0.0" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Upload-Length: 2097152" \
  -H "Upload-Metadata: device_id YmlvYWMtMDE0Mg==,sha256 OWY4NmQw..."
# → Location: /rest/files/abc123

# 2. Upload first chunk (256 KB)
curl -i -X PATCH https://<platform-host>/rest/files/abc123 \
  -H "Tus-Resumable: 1.0.0" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Upload-Offset: 0" \
  -H "Content-Type: application/offset+octet-stream" \
  --data-binary @chunk_000.bin

# 3. After a drop: get offset, then continue PATCH from it
curl -I https://<platform-host>/rest/files/abc123 \
  -H "Tus-Resumable: 1.0.0" -H "Authorization: Bearer $TOKEN"
```

---

## 5. Contract — REST over HTTP/3 `/rest-http3`

**Identical to `/rest`** (same requests, headers, metadata and errors), with these differences:

| Item | Value |
|---|---|
| Base path | `/rest-http3/files/` |
| Transport | HTTP/3 over QUIC, **UDP port 443** |
| TLS | TLS 1.3 (built into QUIC) |
| `Location` header | Returns `/rest-http3/files/{id}` |

**Why use it:** QUIC handles packet loss better than TCP, resumes connections faster, and keeps the connection alive when the device's IP changes (e.g., 4G ↔ Starlink switch).

**0-RTT:** the server disables 0-RTT early data, because a replayed early request would carry a valid token.

**Fallback:** some networks block UDP. If the QUIC handshake fails 3 times in a row, switch to `/rest`. An upload created on one endpoint **cannot** be resumed on the other; start a new upload after switching (the server deduplicates by `sha256`).

---

## 6. Contract — gRPC `/grpc`

All gRPC methods are served under the `/grpc` path prefix:

| Method | Full HTTP/2 path | Type |
|---|---|---|
| `Init` | `/grpc/upload.v1.UploadService/Init` | Unary |
| `Upload` | `/grpc/upload.v1.UploadService/Upload` | Bidirectional stream |
| `Status` | `/grpc/upload.v1.UploadService/Status` | Unary |
| `Cancel` | `/grpc/upload.v1.UploadService/Cancel` | Unary |

Auth: send `authorization: Bearer <access-token>` as gRPC metadata on every call.

```protobuf
syntax = "proto3";
package upload.v1;

service UploadService {
  rpc Init(InitRequest) returns (InitResponse);       // start or resume; idempotent per sha256
  rpc Upload(stream Chunk) returns (stream Ack);      // send chunks, receive committed offsets
  rpc Status(StatusRequest) returns (StatusResponse); // query offset after a dropped stream
  rpc Cancel(CancelRequest) returns (CancelResponse);
}

message InitRequest {
  string device_id    = 1;  // must match the access token's device
  string sha256       = 2;
  int64  size         = 3;
  string filename     = 4;
  string content_type = 5;
  string captured_at  = 6;  // RFC 3339
  int32  priority     = 7;
}
message InitResponse {
  string upload_id        = 1;
  int64  committed_offset = 2;  // 0 = new; >0 = resume from here
  int32  max_chunk_bytes  = 3;  // do not exceed
}

message Chunk {
  string upload_id = 1;  // required on every chunk
  int64  offset    = 2;  // must equal the server's committed offset
  bytes  data      = 3;  // <= max_chunk_bytes
}
message Ack {
  int64 committed_offset = 1;  // durably stored up to this byte
  bool  complete         = 2;  // true once SHA-256 is verified
}

message StatusRequest  { string upload_id = 1; }
message StatusResponse { int64 committed_offset = 1; int64 size = 2; bool complete = 3; }
message CancelRequest  { string upload_id = 1; }
message CancelResponse {}
```

### Flow

1. `Init` → receive `upload_id`, `committed_offset`, `max_chunk_bytes`.
2. Open `Upload`; send chunks from `committed_offset` onward.
3. Read `Ack` messages; `committed_offset` is the only trusted progress value.
4. Done when an `Ack` has `complete: true`.
5. If the stream breaks: reconnect → `Status` → reopen `Upload` from `committed_offset`.

### Errors

Auth errors (`UNAUTHENTICATED`, `PERMISSION_DENIED`) are in section 1.

| gRPC code | Meaning | Sensor action |
|---|---|---|
| `INVALID_ARGUMENT` | Bad metadata or oversize chunk | Fix; do not retry |
| `ALREADY_EXISTS` | Same `sha256` already completed | Mark as done |
| `NOT_FOUND` | Upload expired | `Init` again |
| `FAILED_PRECONDITION` | Offset mismatch | `Status`, resume |
| `DATA_LOSS` | SHA-256 mismatch | Re-read file from SD, re-upload |
| `UNAVAILABLE` / `DEADLINE_EXCEEDED` | Transient | Back off, reconnect, `Status`, resume |

---

## 7. Sensor Integration

### 7.1 Required on every sensor / gateway (all contracts)

1. **Clock sync** (NTP or GPS): TLS certificate validation fails with a wrong clock. Token refresh does not depend on the device clock (section 3), but TLS does.
2. **Server root certificates** for the platform host installed on the device (the CA's roots plus a backup; never pin the server certificate itself).
3. **Token pair** stored securely (the refresh token is the long-lived secret; the poll secret too, until the first refresh):
   - in a file readable only by the upload service (mode `0600`), or encrypted with a key held in a secure element if the board has one;
   - written atomically (temp file → fsync → rename) on every refresh, **before** the new access token is used;
   - only the latest pair is kept (section 3).
4. **Outbox database** (SQLite) with one row per file:

   | Column | Purpose |
   |---|---|
   | `sha256` | File ID (primary key) |
   | `path` | Location on SD card |
   | `priority` | Upload order |
   | `protocol` | `rest`, `rest-http3`, or `grpc` |
   | `upload_ref` | tus upload URL or gRPC `upload_id` |
   | `status` | `queued` · `uploading` · `done` · `failed` |
   | `attempts`, `next_retry_at` | Backoff tracking |

5. **Upload loop:**
   - Pick the highest-priority `queued`/`uploading` row whose `next_retry_at` has passed.
   - Compute SHA-256 once, when the file is queued.
   - **Always ask the server for the current offset before resuming**; never trust the local offset.
   - Refresh the token pair first if it is due (section 3).
   - On `token_expired`, refresh and retry once; on `403` or a refresh `403`, stop the loop; on a refresh `invalid_grant`, register again.
   - Retry with exponential backoff + jitter: 1 s, 2 s, 4 s … capped at 5 min.
   - Mark `done` (and allow local cleanup) **only after** the completion response.

### 7.2 Integrate via REST (tus) — `/rest`

Client libraries: `tus-java-client` (Kotlin/Android/JVM), `tus-js-client` (Node), or the minimal Go client below (no extra dependency).

```go
// TokenStore holds the token pair and refreshes it. Safe for concurrent use.
type TokenStore struct {
    Platform string       // "https://<platform-host>"
    Path     string       // pair file, mode 0600
    Client   *http.Client

    mu      sync.Mutex
    pair    TokenPair
    renewAt time.Time    // ~80% of expires_in after receipt (monotonic clock)
}

type TokenPair struct {
    DeviceID         string    `json:"device_id"`
    AccessToken      string    `json:"access_token"`
    TokenType        string    `json:"token_type"`
    ExpiresIn        int       `json:"expires_in"`
    RefreshToken     string    `json:"refresh_token"`
    RefreshExpiresAt time.Time `json:"refresh_expires_at"`
}

var (
    ErrForbidden     = errors.New("device not active: stop uploading")
    ErrRegisterAgain = errors.New("refresh token expired or refused: register again")
)

// Access returns the current access token.
func (s *TokenStore) Access() string {
    s.mu.Lock(); defer s.mu.Unlock()
    return s.pair.AccessToken
}

// Due reports whether a proactive refresh is due.
func (s *TokenStore) Due() bool {
    s.mu.Lock(); defer s.mu.Unlock()
    return !time.Now().Before(s.renewAt)
}

// Refresh trades the refresh token for a new pair and saves it before use.
// The mutex makes it single-flight: concurrent callers wait for one refresh,
// and a caller whose token was already replaced (seen != current) returns at once.
func (s *TokenStore) Refresh(ctx context.Context, seen string) error {
    s.mu.Lock(); defer s.mu.Unlock()
    if seen != "" && seen != s.pair.AccessToken { return nil } // someone else refreshed
    body, _ := json.Marshal(map[string]string{"refresh_token": s.pair.RefreshToken})
    req, _ := http.NewRequestWithContext(ctx, "POST", s.Platform+"/v1/devices/token/refresh", bytes.NewReader(body))
    req.Header.Set("Content-Type", "application/json")
    res, err := s.Client.Do(req)
    if err != nil { return err } // the sent refresh token still works; retry later
    defer res.Body.Close()
    switch res.StatusCode {
    case 200:
    case 401: return ErrRegisterAgain // invalid_grant
    case 403: return ErrForbidden     // device_not_active
    default:  return fmt.Errorf("refresh: %d", res.StatusCode) // back off
    }
    var p TokenPair
    if err := json.NewDecoder(res.Body).Decode(&p); err != nil { return err }
    if err := writeFileAtomic(s.Path, p, 0o600); err != nil { return err } // temp, fsync, rename
    s.pair = p
    s.renewAt = time.Now().Add(time.Duration(p.ExpiresIn) * time.Second * 8 / 10)
    return nil
}

// Minimal tus client. Works for /rest and /rest-http3 (only the http.Client differs).
type Tus struct {
    Base   string        // "https://<platform-host>/rest/files/"
    Tokens *TokenStore
    Client *http.Client
}

// do sends req with the access token. On 401 it refreshes once and retries.
func (t *Tus) do(req *http.Request) (*http.Response, error) {
    for attempt := 0; ; attempt++ {
        token := t.Tokens.Access()
        r := req.Clone(req.Context())
        if req.GetBody != nil { r.Body, _ = req.GetBody() } // set by NewRequest for bytes.Reader
        r.Header.Set("Tus-Resumable", "1.0.0")
        r.Header.Set("Authorization", "Bearer "+token)
        res, err := t.Client.Do(r)
        if err != nil { return nil, err }
        switch res.StatusCode {
        case 401:
            var body struct{ Error string `json:"error"` }
            json.NewDecoder(res.Body).Decode(&body)
            res.Body.Close()
            if attempt > 0 { return nil, fmt.Errorf("unauthorized: %s", body.Error) } // stop and log
            if err := t.Tokens.Refresh(req.Context(), token); err != nil { return nil, err }
            continue // token_expired or invalid_token: retry once with the new token
        case 403:
            res.Body.Close()
            return nil, ErrForbidden
        }
        return res, nil
    }
}

// Create returns the upload URL. Store it in the outbox as upload_ref.
func (t *Tus) Create(size int64, meta map[string]string) (string, error) {
    var pairs []string
    for k, v := range meta {
        pairs = append(pairs, k+" "+base64.StdEncoding.EncodeToString([]byte(v)))
    }
    req, _ := http.NewRequest("POST", t.Base, nil)
    req.Header.Set("Upload-Length", strconv.FormatInt(size, 10))
    req.Header.Set("Upload-Metadata", strings.Join(pairs, ","))
    res, err := t.do(req)
    if err != nil { return "", err }
    defer res.Body.Close()
    if res.StatusCode == 461 { return "", ErrAlreadyExists }
    if res.StatusCode != 201 { return "", fmt.Errorf("create: %d", res.StatusCode) }
    loc, _ := res.Location()
    return loc.String(), nil
}

// Offset asks the server where to resume.
func (t *Tus) Offset(url string) (int64, error) {
    req, _ := http.NewRequest("HEAD", url, nil)
    res, err := t.do(req)
    if err != nil { return 0, err }
    res.Body.Close()
    if res.StatusCode == 404 || res.StatusCode == 410 { return 0, ErrExpired }
    return strconv.ParseInt(res.Header.Get("Upload-Offset"), 10, 64)
}

// Send uploads from the server's offset to the end, chunk by chunk.
func (t *Tus) Send(url string, f *os.File, size int64, chunk int) error {
    off, err := t.Offset(url)
    if err != nil { return err }
    buf := make([]byte, chunk)
    for off < size {
        n, _ := f.ReadAt(buf, off)
        req, _ := http.NewRequest("PATCH", url, bytes.NewReader(buf[:n]))
        req.Header.Set("Upload-Offset", strconv.FormatInt(off, 10))
        req.Header.Set("Content-Type", "application/offset+octet-stream")
        res, err := t.do(req)
        if err != nil { return err } // caller backs off, then calls Send again
        res.Body.Close()
        if res.StatusCode == 409 { return t.Send(url, f, size, chunk) } // re-sync offset
        if res.StatusCode != 204 { return fmt.Errorf("patch: %d", res.StatusCode) }
        off, _ = strconv.ParseInt(res.Header.Get("Upload-Offset"), 10, 64)
    }
    return nil // complete
}
```

`TokenStore` keeps only the latest pair: `Access()` returns the access token, `Refresh(ctx, seen)` posts the refresh token, saves the new pair atomically before anyone uses it, and runs one refresh at a time (callers that saw an older access token just pick up the new one). `do()` refreshes once on any `401` and retries the request; a second `401` is returned as an error to stop and log.

**Sensor flow:**

1. If `Tokens.Due()`, `Refresh` first.
2. No `upload_ref` yet → `Create` → save the URL to the outbox.
3. `Send(url, …)`. On a network error, schedule a retry; next attempt calls `Send` again (it `HEAD`s first).
4. `ErrExpired` → clear `upload_ref`, `Create` again.
5. `ErrRegisterAgain` → register again with a new poll secret (section 2).
6. `ErrForbidden` → stop uploading; the device is revoked or the `device_id` does not match.
7. `ErrAlreadyExists` or `Send` returns `nil` → mark `done`.

**Client HTTP settings:** keep-alive on, connect timeout 15 s, per-request timeout ~60 s per 256 KB at the slowest expected link speed.

### 7.3 Integrate via REST over HTTP/3 — `/rest-http3`

Use the **same tus client**; only the transport changes.

**Go** (`github.com/quic-go/quic-go/http3`):

```go
tus := &Tus{
    Base:   "https://<platform-host>/rest-http3/files/",
    Tokens: tokens,
    Client: &http.Client{
        Transport: &http3.Transport{ // named http3.RoundTripper in older quic-go versions
            TLSClientConfig: &tls.Config{RootCAs: serverRoots},
        },
        Timeout: 60 * time.Second,
    },
}
```

**Other platforms:**

| Platform | HTTP/3 option |
|---|---|
| Kotlin / Android | Cronet (`HttpEngine` on Android 14+, or the Cronet library). Note: OkHttp and `tus-java-client` do not support HTTP/3, so send the tus requests through Cronet directly |
| Embedded Linux (C) | `ngtcp2` + `nghttp3`, `lsquic`, or `msquic` |
| Microcontroller | Not practical; use `/rest` via a gateway |

**Fallback rule:** after 3 consecutive QUIC connection failures, set the outbox row's `protocol` to `rest`, clear `upload_ref`, and start a new upload on `/rest`.

### 7.4 Integrate via gRPC — `/grpc`

1. Generate client stubs from the `.proto` above (`protoc` / `buf`).
2. **Add the `/grpc` prefix and the token** with client interceptors (generated stubs call `/upload.v1.UploadService/...`):

```go
func withPrefixAndToken(p string, tokens *TokenStore) []grpc.DialOption {
    auth := func(ctx context.Context, token string) context.Context {
        return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
    }
    return []grpc.DialOption{
        // Unary calls (Init, Status, Cancel): on token_expired, refresh once and retry.
        grpc.WithUnaryInterceptor(func(ctx context.Context, m string, req, reply any,
            cc *grpc.ClientConn, inv grpc.UnaryInvoker, o ...grpc.CallOption) error {
            token := tokens.Access()
            err := inv(auth(ctx, token), p+m, req, reply, cc, o...)
            if isTokenExpired(err) {
                if rerr := tokens.Refresh(ctx, token); rerr != nil { return rerr }
                err = inv(auth(ctx, tokens.Access()), p+m, req, reply, cc, o...)
            }
            return err
        }),
        // Upload stream: authenticated when it opens; an auth error arrives on
        // Recv, so upload() refreshes and the caller reopens the stream.
        grpc.WithStreamInterceptor(func(ctx context.Context, d *grpc.StreamDesc,
            cc *grpc.ClientConn, m string, s grpc.Streamer, o ...grpc.CallOption) (grpc.ClientStream, error) {
            return s(auth(ctx, tokens.Access()), d, cc, p+m, o...)
        }),
    }
}

func isTokenExpired(err error) bool {
    st := status.Convert(err)
    return st.Code() == codes.Unauthenticated && st.Message() == "token_expired"
}

opts := append(withPrefixAndToken("/grpc", tokens),
    grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(serverRoots, "")),
    grpc.WithKeepaliveParams(keepalive.ClientParameters{
        Time: 30 * time.Second, Timeout: 10 * time.Second, PermitWithoutStream: true,
    }),
)
conn, _ := grpc.NewClient("<platform-host>:443", opts...)
client := uploadv1.NewUploadServiceClient(conn)
```

   On Kotlin/JVM, do the same with a `ClientInterceptor` that rebuilds the `MethodDescriptor` with full method name `grpc/upload.v1.UploadService/<Method>` and adds the `authorization` header.

3. **Upload loop:**

```go
func upload(ctx context.Context, c uploadv1.UploadServiceClient, tokens *TokenStore, f *os.File, meta *uploadv1.InitRequest) error {
    if tokens.Due() {
        if err := tokens.Refresh(ctx, ""); err != nil { return err }
    }
    init, err := c.Init(ctx, meta) // idempotent: returns existing upload_id + offset on resume
    switch {
    case status.Code(err) == codes.AlreadyExists:
        return nil // already done
    case status.Code(err) == codes.PermissionDenied:
        return ErrForbidden
    case status.Code(err) == codes.Unauthenticated:
        return fmt.Errorf("unauthorized after refresh: %w", err) // stop and log
    case err != nil:
        return err
    }

    token := tokens.Access() // the token the stream opens with
    stream, err := c.Upload(ctx)
    if err != nil { return err }

    // Receive acks in the background.
    done := make(chan error, 1)
    go func() {
        for {
            ack, err := stream.Recv()
            if err != nil { done <- err; return }
            saveOffset(meta.Sha256, ack.CommittedOffset) // optional, for logging only
            if ack.Complete { done <- nil; return }
        }
    }()

    buf := make([]byte, init.MaxChunkBytes)
    for off := init.CommittedOffset; off < meta.Size; {
        n, _ := f.ReadAt(buf, off)
        if err := stream.Send(&uploadv1.Chunk{UploadId: init.UploadId, Offset: off, Data: buf[:n]}); err != nil {
            break // stream broke; error comes through done
        }
        off += int64(n)
    }
    stream.CloseSend()
    err = <-done
    if isTokenExpired(err) {
        if rerr := tokens.Refresh(ctx, token); rerr != nil { return rerr }
    }
    return err // on error: caller backs off, then calls upload() again (Init resumes)
}
```

**Sensor flow:**

1. Call `upload()`. On error, apply backoff and call it again; `Init` returns the server's `committed_offset`, so it resumes automatically. A stream that is already open may finish even if its access token expires meanwhile.
2. `ErrRegisterAgain` → register again (section 2); `ErrForbidden` → stop uploading.
3. `NOT_FOUND` → upload expired; the next `Init` starts fresh.
4. `ALREADY_EXISTS` or `complete: true` → mark `done`.

---

## 8. Choosing an Endpoint

| Situation | Endpoint |
|---|---|
| Default / unsure | `/rest` |
| Lossy link, IP changes, UDP allowed, client supports QUIC | `/rest-http3` (fallback to `/rest`) |
| Gateway with a stable link already running gRPC services | `/grpc` |
| Microcontroller-class device | Send to the field gateway; the gateway uploads via `/rest` |
