//go:build remote

package remote

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
	"github.com/resyahrial/sensor-upload-server/internal/client"
	"github.com/resyahrial/sensor-upload-server/internal/fileapi"
)

// TestRemoteHealth: the public TLS listener answers (TLS handshake, the
// lookup route), and, with REMOTE_ADMIN_URL, /healthz and /readyz are ok.
func TestRemoteHealth(t *testing.T) {
	e := remoteEnv(t)
	t.Log(e)
	if code, _ := e.lookup(t, randSHA(t)); code != http.StatusNotFound {
		t.Errorf("public listener: lookup %d, want 404", code)
	}
	if e.AdminURL == "" {
		t.Log("REMOTE_ADMIN_URL not set: admin /healthz and /readyz not checked")
		return
	}
	hc := &http.Client{Timeout: 10 * time.Second}
	for _, path := range []string{"/healthz", "/readyz"} {
		res, err := hc.Get(e.AdminURL + path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		b, _ := io.ReadAll(res.Body)
		_ = res.Body.Close()
		if res.StatusCode != http.StatusOK || !strings.Contains(string(b), `"status":"ok"`) {
			t.Errorf("%s: %d %s", path, res.StatusCode, b)
		}
	}
}

// uploadWithClient uploads fresh content with the contract client and
// verifies it through the lookup endpoint.
func uploadWithClient(t *testing.T, protocol string) {
	e := remoteEnv(t)
	f := newFile(t, fileSize)
	if code, _ := e.lookup(t, f.SHA256); code != http.StatusNotFound {
		t.Fatalf("lookup before upload: %d, want 404", code)
	}
	res := e.runClient(t, protocol, writeFile(t, f))
	if res.Result != client.ResultOK {
		t.Fatalf("%s upload: result %s (%v)", protocol, res.Result, res.Err)
	}
	e.assertSaved(t, f)
}

// TestRemoteUploadREST: tus over HTTP/2 (/rest/files/).
func TestRemoteUploadREST(t *testing.T) { uploadWithClient(t, client.ProtoREST) }

// TestRemoteUploadRESTHTTP3: tus over HTTP/3 (/rest-http3/files/, UDP).
func TestRemoteUploadRESTHTTP3(t *testing.T) { uploadWithClient(t, client.ProtoRESTHTTP3) }

// TestRemoteUploadGRPC: gRPC with the /grpc prefix on the TLS listener.
func TestRemoteUploadGRPC(t *testing.T) { uploadWithClient(t, client.ProtoGRPC) }

// TestRemoteUploadGRPCNative: gRPC on the native listener.
func TestRemoteUploadGRPCNative(t *testing.T) { uploadWithClient(t, client.ProtoGRPCNative) }

// resumeTus uploads one chunk, interrupts a PATCH mid-body, asks HEAD for
// the offset with a fresh connection and finishes from there.
func resumeTus(t *testing.T, h3 bool) {
	e := remoteEnv(t)
	f := newFile(t, fileSize)
	c := e.tus(t, h3, e.Token(t))
	loc := c.mustCreate(f, e.Device)
	c.sendFrom(loc, File{Data: f.Data[:chunkSize]}, 0)

	// Interrupted PATCH: send part of the rest, then drop the request.
	pr, pw := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, loc, pr)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = int64(len(f.Data) - chunkSize)
	req.Header.Set("Tus-Resumable", "1.0.0")
	req.Header.Set("Authorization", "Bearer "+c.token)
	for k, v := range patchHeaders(chunkSize) {
		req.Header.Set(k, v)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if res, err := c.hc.Do(req); err == nil {
			_ = res.Body.Close()
		}
	}()
	_, _ = pw.Write(f.Data[chunkSize : 2*chunkSize])
	time.Sleep(time.Second) // let the bytes reach the server
	cancel()
	_ = pw.CloseWithError(io.ErrUnexpectedEOF)
	<-done

	fresh := e.tus(t, h3, e.Token(t)) // new connection, like a restarted sensor
	off := fresh.head(loc)
	if off < chunkSize || off >= len(f.Data) {
		t.Fatalf("HEAD after interruption: offset %d, want in [%d, %d)", off, chunkSize, len(f.Data))
	}
	t.Logf("resuming at offset %d of %d", off, len(f.Data))
	fresh.sendFrom(loc, f, off)
	e.assertSaved(t, f)
}

// TestRemoteResumeREST: tus HEAD offset after an interrupted PATCH (HTTP/2).
func TestRemoteResumeREST(t *testing.T) { resumeTus(t, false) }

// TestRemoteResumeRESTHTTP3: the same over HTTP/3.
func TestRemoteResumeRESTHTTP3(t *testing.T) { resumeTus(t, true) }

// resumeGRPC streams part of the file, abandons the stream and checks that
// a second Init returns the same upload_id and the acked offset.
func resumeGRPC(t *testing.T, native bool) {
	e := remoteEnv(t)
	f := newFile(t, fileSize)
	g := e.grpc(t, native, e.Token(t))
	first := g.mustInit(initReq(f, e.Device))
	if first.GetCommittedOffset() != 0 {
		t.Fatalf("new upload: committed_offset %d", first.GetCommittedOffset())
	}
	ack := g.upload(first.GetUploadId(), f, 0, 2*chunkSize)
	if ack.GetComplete() {
		t.Fatal("completed before the interruption")
	}

	again := e.grpc(t, native, e.Token(t)) // new connection
	res, err := again.init(initReq(f, e.Device))
	if err != nil {
		t.Fatalf("second init: %v", err)
	}
	if res.GetUploadId() != first.GetUploadId() || res.GetCommittedOffset() != ack.GetCommittedOffset() {
		t.Fatalf("second init: id %s offset %d, want %s offset %d",
			res.GetUploadId(), res.GetCommittedOffset(), first.GetUploadId(), ack.GetCommittedOffset())
	}
	st, err := again.c.Status(again.ctx(ctxT(t)), &uploadv1.StatusRequest{UploadId: res.GetUploadId()})
	if err != nil || st.GetCommittedOffset() != res.GetCommittedOffset() || st.GetSize() != int64(len(f.Data)) {
		t.Fatalf("status: %+v %v", st, err)
	}
	if final := again.upload(res.GetUploadId(), f, res.GetCommittedOffset(), 0); !final.GetComplete() {
		t.Fatalf("final ack: %+v", final)
	}
	e.assertSaved(t, f)
}

// TestRemoteResumeGRPC: Init committed_offset after a dropped stream (/grpc).
func TestRemoteResumeGRPC(t *testing.T) { resumeGRPC(t, false) }

// TestRemoteResumeGRPCNative: the same on the native listener.
func TestRemoteResumeGRPCNative(t *testing.T) { resumeGRPC(t, true) }

// TestRemoteDuplicate: once a sha256 is completed, a new upload of it is
// "already exists" on every protocol (461 / ALREADY_EXISTS, contract: mark
// done), and the contract client reports result=duplicate.
func TestRemoteDuplicate(t *testing.T) {
	e := remoteEnv(t)
	f := newFile(t, fileSize)
	if res := e.runClient(t, client.ProtoREST, writeFile(t, f)); res.Result != client.ResultOK {
		t.Fatalf("first upload: %s (%v)", res.Result, res.Err)
	}
	e.assertSaved(t, f)

	for _, h3 := range []bool{false, true} {
		res, body := e.tus(t, h3, e.Token(t)).create(len(f.Data), meta{Device: e.Device, SHA256: f.SHA256})
		if res.StatusCode != 461 {
			t.Errorf("tus create (h3=%v): %d %q, want 461", h3, res.StatusCode, body)
		}
	}
	natives := []bool{false}
	if e.GRPCNative != "" {
		natives = append(natives, true)
	}
	for _, native := range natives {
		_, err := e.grpc(t, native, e.Token(t)).init(initReq(f, e.Device))
		wantCode(t, "grpc init", err, codes.AlreadyExists)
	}
	if res := e.runClient(t, client.ProtoGRPC, writeFile(t, f)); res.Result != client.ResultDuplicate {
		t.Errorf("client re-upload: %s (%v), want duplicate", res.Result, res.Err)
	}
}

// TestRemoteBadToken: a missing or forged token is 401 / UNAUTHENTICATED.
func TestRemoteBadToken(t *testing.T) {
	e := remoteEnv(t)
	f := newFile(t, 1024)
	m := meta{Device: e.Device, SHA256: f.SHA256}
	forged := e.Token(t)
	forged = forged[:strings.LastIndexByte(forged, '.')+1] + "c2lnbmF0dXJl" // wrong signature
	for name, tok := range map[string]string{"missing": "", "garbage": "not-a-jwt", "bad signature": forged} {
		for _, h3 := range []bool{false, true} {
			res, body := e.tus(t, h3, tok).create(len(f.Data), m)
			if res.StatusCode != http.StatusUnauthorized {
				t.Errorf("tus %s (h3=%v): %d %q, want 401", name, h3, res.StatusCode, body)
			}
		}
		_, err := e.grpc(t, false, tok).init(initReq(f, e.Device))
		wantCode(t, "grpc "+name, err, codes.Unauthenticated)
		if e.GRPCNative != "" {
			_, err := e.grpc(t, true, tok).init(initReq(f, e.Device))
			wantCode(t, "grpc-native "+name, err, codes.Unauthenticated)
		}
	}
	if code, _ := e.lookup(t, f.SHA256); code != http.StatusNotFound {
		t.Errorf("nothing may be created: lookup %d", code)
	}
}

// TestRemoteCrossDevice: metadata naming another device is 403 /
// PERMISSION_DENIED; another device's upload is 404 / NOT_FOUND.
func TestRemoteCrossDevice(t *testing.T) {
	e := remoteEnv(t)
	f := newFile(t, fileSize)
	t.Run("metadata device mismatch", func(t *testing.T) {
		res, body := e.tus(t, false, e.Token(t)).create(len(f.Data), meta{Device: e.Device + "-x", SHA256: f.SHA256})
		if res.StatusCode != http.StatusForbidden {
			t.Errorf("tus: %d %q, want 403", res.StatusCode, body)
		}
		_, err := e.grpc(t, false, e.Token(t)).init(initReq(f, e.Device+"-x"))
		wantCode(t, "grpc", err, codes.PermissionDenied)
	})
	t.Run("other device's upload", func(t *testing.T) {
		other := e.OtherToken(t)
		owner := e.tus(t, false, e.Token(t))
		loc := owner.mustCreate(f, e.Device)
		owner.sendFrom(loc, File{Data: f.Data[:chunkSize]}, 0)
		thief := e.tus(t, false, other)
		if off := thief.head(loc); off != -http.StatusNotFound {
			t.Errorf("tus HEAD by other device: %d, want -404", off)
		}
		res, _ := thief.mustDo(http.MethodPatch, loc, f.Data[chunkSize:2*chunkSize], patchHeaders(chunkSize))
		if res.StatusCode != http.StatusNotFound {
			t.Errorf("tus PATCH by other device: %d, want 404", res.StatusCode)
		}
		if res, _ := thief.mustDo(http.MethodDelete, loc, nil, nil); res.StatusCode != http.StatusNotFound {
			t.Errorf("tus DELETE by other device: %d, want 404", res.StatusCode)
		}
		if off := owner.head(loc); off != chunkSize {
			t.Errorf("owner HEAD: %d, want %d (untouched)", off, chunkSize)
		}

		g := e.grpc(t, false, e.Token(t))
		f2 := newFile(t, fileSize)
		id := g.mustInit(initReq(f2, e.Device)).GetUploadId()
		og := e.grpc(t, false, other)
		_, err := og.c.Status(og.ctx(ctxT(t)), &uploadv1.StatusRequest{UploadId: id})
		wantCode(t, "grpc Status by other device", err, codes.NotFound)
		_, err = og.c.Cancel(og.ctx(ctxT(t)), &uploadv1.CancelRequest{UploadId: id})
		wantCode(t, "grpc Cancel by other device", err, codes.NotFound)
	})
}

// TestRemoteVerifyNotFound: the lookup is 404 not_found for an unknown
// hash and 400 for anything but 64 lowercase hex characters.
func TestRemoteVerifyNotFound(t *testing.T) {
	e := remoteEnv(t)
	sha := randSHA(t)
	code, body := e.lookup(t, sha)
	if code != http.StatusNotFound || body.Status != fileapi.StatusNotFound || body.SHA256 != sha || body.Size != 0 {
		t.Errorf("unknown: %d %+v", code, body)
	}
	for _, bad := range []string{strings.ToUpper(sha), sha[:63], sha + "0", strings.Repeat("g", 64)} {
		if code, _ := e.lookup(t, bad); code != http.StatusBadRequest {
			t.Errorf("lookup %q: %d, want 400", bad, code)
		}
	}
}

// TestRemoteMetadataValidation: invalid FR-1 metadata is 400 /
// INVALID_ARGUMENT and creates nothing.
func TestRemoteMetadataValidation(t *testing.T) {
	e := remoteEnv(t)
	f := newFile(t, 1024)
	cases := map[string]meta{
		"missing sha256":      {SHA256: "-"},
		"short sha256":        {SHA256: f.SHA256[:10]},
		"bad captured_at":     {CapturedAt: "yesterday"},
		"priority 9":          {Priority: "9"},
		"content type banned": {ContentType: "application/x-msdownload"},
		"missing filename":    {Filename: "-"},
	}
	c := e.tus(t, false, e.Token(t))
	for name, m := range cases {
		m.Device = e.Device
		if m.SHA256 == "" {
			m.SHA256 = f.SHA256
		}
		if res, body := c.create(len(f.Data), m); res.StatusCode != http.StatusBadRequest {
			t.Errorf("tus %s: %d %q, want 400", name, res.StatusCode, body)
		}
	}
	g := e.grpc(t, false, e.Token(t))
	bad := map[string]func(*uploadv1.InitRequest){
		"short sha256":    func(r *uploadv1.InitRequest) { r.Sha256 = r.Sha256[:10] },
		"bad captured_at": func(r *uploadv1.InitRequest) { r.CapturedAt = "yesterday" },
		"priority 9":      func(r *uploadv1.InitRequest) { r.Priority = 9 },
		"zero size":       func(r *uploadv1.InitRequest) { r.Size = 0 },
	}
	for name, mutate := range bad {
		req := initReq(f, e.Device)
		mutate(req)
		_, err := g.init(req)
		wantCode(t, "grpc "+name, err, codes.InvalidArgument)
	}
	if code, _ := e.lookup(t, f.SHA256); code != http.StatusNotFound {
		t.Errorf("nothing may be stored: lookup %d", code)
	}
}
