# Sensor Upload Integration Guide

How sensors and field gateways upload captured files (photo, audio, video) to the ingest server.

Three endpoints accept the same files with the same rules:

| Endpoint | Protocol | Transport | Use when |
|---|---|---|---|
| `/rest` | tus 1.0 (REST) | HTTPS, HTTP/1.1 or HTTP/2 over TCP | Default; widest client support |
| `/rest-http3` | tus 1.0 (REST) | HTTP/3 over QUIC (UDP 443) | Lossy links or changing IPs; UDP must be allowed |
| `/grpc` | gRPC | HTTP/2 over TCP | Gateways already using gRPC |

Base URL: `https://<ingest-host>` (provided per environment).

---

## 1. Common Rules

### Authentication

Every request carries a device token:

```
Authorization: Bearer <device-jwt>
```

The JWT contains a `device_id` claim, which must match the `device_id` in the upload metadata.

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
| `device_id` | `bioac-0142` | Must match the token |
| `sha256` | `9f86d081…` | 64 hex chars |
| `filename` | `20261021_0530.flac` | Original filename |
| `content_type` | `audio/flac` | MIME type |
| `captured_at` | `2026-10-21T05:30:00+07:00` | RFC 3339 |
| `priority` | `2` | 1 alert · 2 thumbnail · 3 clip · 4 full file |

---

## 2. Contract — REST (tus) `/rest`

Protocol: [tus 1.0](https://tus.io/protocols/resumable-upload) with the **creation**, **termination** and **expiration** extensions.

All requests include `Tus-Resumable: 1.0.0`.

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

| Status | Meaning | Sensor action |
|---|---|---|
| `400` | Missing or invalid metadata | Fix; do not retry |
| `401` / `403` | Invalid token or `device_id` mismatch | Refresh token, then retry |
| `404` / `410` | Upload unknown or expired | Create a new upload |
| `409` | Offset mismatch | `HEAD`, resume from returned offset |
| `413` | File too large | Do not retry |
| `460` | SHA-256 mismatch on completion | Re-read file from SD, re-upload |
| `461` | Already exists (same `sha256` completed) | Mark as done |
| `5xx`, timeout, connection lost | Transient | Back off, `HEAD`, resume |

### Example (curl)

```bash
# 1. Create
curl -i -X POST https://<ingest-host>/rest/files/ \
  -H "Tus-Resumable: 1.0.0" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Upload-Length: 2097152" \
  -H "Upload-Metadata: device_id YmlvYWMtMDE0Mg==,sha256 OWY4NmQw..."
# → Location: /rest/files/abc123

# 2. Upload first chunk (256 KB)
curl -i -X PATCH https://<ingest-host>/rest/files/abc123 \
  -H "Tus-Resumable: 1.0.0" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Upload-Offset: 0" \
  -H "Content-Type: application/offset+octet-stream" \
  --data-binary @chunk_000.bin

# 3. After a drop: get offset, then continue PATCH from it
curl -I https://<ingest-host>/rest/files/abc123 \
  -H "Tus-Resumable: 1.0.0" -H "Authorization: Bearer $TOKEN"
```

---

## 3. Contract — REST over HTTP/3 `/rest-http3`

**Identical to `/rest`** (same requests, headers, metadata and errors), with these differences:

| Item | Value |
|---|---|
| Base path | `/rest-http3/files/` |
| Transport | HTTP/3 over QUIC, **UDP port 443** |
| TLS | TLS 1.3 (built into QUIC) |
| `Location` header | Returns `/rest-http3/files/{id}` |

**Why use it:** QUIC handles packet loss better than TCP, resumes connections faster (0-RTT), and keeps the connection alive when the device's IP changes (e.g., 4G ↔ Starlink switch).

**Fallback:** some networks block UDP. If the QUIC handshake fails 3 times in a row, switch to `/rest`. An upload created on one endpoint **cannot** be resumed on the other; start a new upload after switching (the server deduplicates by `sha256`).

---

## 4. Contract — gRPC `/grpc`

All gRPC methods are served under the `/grpc` path prefix:

| Method | Full HTTP/2 path | Type |
|---|---|---|
| `Init` | `/grpc/upload.v1.UploadService/Init` | Unary |
| `Upload` | `/grpc/upload.v1.UploadService/Upload` | Bidirectional stream |
| `Status` | `/grpc/upload.v1.UploadService/Status` | Unary |
| `Cancel` | `/grpc/upload.v1.UploadService/Cancel` | Unary |

Auth: send `authorization: Bearer <device-jwt>` as gRPC metadata on every call.

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
  string device_id    = 1;
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

| gRPC code | Meaning | Sensor action |
|---|---|---|
| `INVALID_ARGUMENT` | Bad metadata or oversize chunk | Fix; do not retry |
| `UNAUTHENTICATED` / `PERMISSION_DENIED` | Token or `device_id` issue | Refresh token |
| `ALREADY_EXISTS` | Same `sha256` already completed | Mark as done |
| `NOT_FOUND` | Upload expired | `Init` again |
| `FAILED_PRECONDITION` | Offset mismatch | `Status`, resume |
| `DATA_LOSS` | SHA-256 mismatch | Re-read file from SD, re-upload |
| `UNAVAILABLE` / `DEADLINE_EXCEEDED` | Transient | Back off, reconnect, `Status`, resume |

---

## 5. Sensor Integration

### 5.1 Required on every sensor / gateway (all contracts)

1. **Clock sync** (NTP or GPS): TLS and JWT validation fail with a wrong clock.
2. **CA certificate** for the ingest host installed on the device.
3. **Device token** stored securely; refresh before expiry.
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
   - Retry with exponential backoff + jitter: 1 s, 2 s, 4 s … capped at 5 min.
   - Mark `done` (and allow local cleanup) **only after** the completion response.

### 5.2 Integrate via REST (tus) — `/rest`

Client libraries: `tus-java-client` (Kotlin/Android/JVM), `tus-js-client` (Node), or the minimal Go client below (no extra dependency).

```go
// Minimal tus client. Works for /rest and /rest-http3 (only the http.Client differs).
type Tus struct {
    Base   string        // "https://<ingest-host>/rest/files/"
    Token  string
    Client *http.Client
}

func (t *Tus) do(req *http.Request) (*http.Response, error) {
    req.Header.Set("Tus-Resumable", "1.0.0")
    req.Header.Set("Authorization", "Bearer "+t.Token)
    return t.Client.Do(req)
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

**Sensor flow:**

1. No `upload_ref` yet → `Create` → save the URL to the outbox.
2. `Send(url, …)`. On a network error, schedule a retry; next attempt calls `Send` again (it `HEAD`s first).
3. `ErrExpired` → clear `upload_ref`, `Create` again.
4. `ErrAlreadyExists` or `Send` returns `nil` → mark `done`.

**Client HTTP settings:** keep-alive on, connect timeout 15 s, per-request timeout ~60 s per 256 KB at the slowest expected link speed.

### 5.3 Integrate via REST over HTTP/3 — `/rest-http3`

Use the **same tus client**; only the transport changes.

**Go** (`github.com/quic-go/quic-go/http3`):

```go
tus := &Tus{
    Base:  "https://<ingest-host>/rest-http3/files/",
    Token: token,
    Client: &http.Client{
        Transport: &http3.Transport{ // named http3.RoundTripper in older quic-go versions
            TLSClientConfig: &tls.Config{RootCAs: caPool},
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

### 5.4 Integrate via gRPC — `/grpc`

1. Generate client stubs from the `.proto` above (`protoc` / `buf`).
2. **Add the `/grpc` prefix.** Generated stubs call `/upload.v1.UploadService/...`, so add the prefix with a client interceptor:

```go
func withPrefix(p string) []grpc.DialOption {
    return []grpc.DialOption{
        grpc.WithUnaryInterceptor(func(ctx context.Context, m string, req, reply any,
            cc *grpc.ClientConn, inv grpc.UnaryInvoker, o ...grpc.CallOption) error {
            return inv(ctx, p+m, req, reply, cc, o...)
        }),
        grpc.WithStreamInterceptor(func(ctx context.Context, d *grpc.StreamDesc,
            cc *grpc.ClientConn, m string, s grpc.Streamer, o ...grpc.CallOption) (grpc.ClientStream, error) {
            return s(ctx, d, cc, p+m, o...)
        }),
    }
}

opts := append(withPrefix("/grpc"),
    grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(caPool, "")),
    grpc.WithKeepaliveParams(keepalive.ClientParameters{
        Time: 30 * time.Second, Timeout: 10 * time.Second, PermitWithoutStream: true,
    }),
)
conn, _ := grpc.NewClient("<ingest-host>:443", opts...)
client := uploadv1.NewUploadServiceClient(conn)
```

   On Kotlin/JVM, do the same with a `ClientInterceptor` that rebuilds the `MethodDescriptor` with full method name `grpc/upload.v1.UploadService/<Method>`.

3. **Upload loop:**

```go
func upload(ctx context.Context, c uploadv1.UploadServiceClient, f *os.File, meta *uploadv1.InitRequest) error {
    ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

    init, err := c.Init(ctx, meta) // idempotent: returns existing upload_id + offset on resume
    if status.Code(err) == codes.AlreadyExists { return nil } // already done
    if err != nil { return err }

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
    return <-done // on error: caller backs off, then calls upload() again (Init resumes)
}
```

**Sensor flow:**

1. Call `upload()`. On error, apply backoff and call it again; `Init` returns the server's `committed_offset`, so it resumes automatically.
2. `NOT_FOUND` → upload expired; the next `Init` starts fresh.
3. `ALREADY_EXISTS` or `complete: true` → mark `done`.

---

## 6. Choosing an Endpoint

| Situation | Endpoint |
|---|---|
| Default / unsure | `/rest` |
| Lossy link, IP changes, UDP allowed, client supports QUIC | `/rest-http3` (fallback to `/rest`) |
| Gateway with a stable link already running gRPC services | `/grpc` |
| Microcontroller-class device | Send to the field gateway; the gateway uploads via `/rest` |
