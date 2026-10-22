package client

import (
	"bytes"
	"cmp"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
	"github.com/resyahrial/sensor-upload-server/internal/grpcprefix"
)

// TestGRPCSIGKILLAndResume kills a real uploader process (gRPC, default
// window of one chunk) with SIGKILL mid-stream and resumes from the same
// outbox. The fake delays acks, so a client that pipelined chunks without
// waiting would have claimed many chunks past the last ack.
func TestGRPCSIGKILLAndResume(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a process")
	}
	dir := t.TempDir()
	state := filepath.Join(dir, "s.db")
	path, data, sha := writeFile(t, dir, "gkill.flac", 12*testChunk+9)

	// Borrow httptest's self-signed 127.0.0.1 certificate for a TLS gRPC listener.
	certSrv := httptest.NewUnstartedServer(http.NotFoundHandler())
	certSrv.StartTLS()
	cert, certs := certSrv.Certificate(), certSrv.TLS.Certificates
	certSrv.Close()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := newFakeGRPC()
	f.ackDelay = 2 * time.Millisecond
	s := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: certs, MinVersion: tls.VersionTLS12})))
	if err := grpcprefix.RegisterWithPrefix(s, &uploadv1.UploadService_ServiceDesc, f, "/grpc"); err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	addr := lis.Addr().String()
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), exe, "-test.run=^TestHelperUploader$", "-test.count=1")
	cmd.Env = append(os.Environ(), "UPLOADER_HELPER=1", "UPLOADER_PROTO="+ProtoGRPC, "UPLOADER_URL="+addr,
		"UPLOADER_STATE="+state, "UPLOADER_FILE="+path, "UPLOADER_CA="+string(caPEM))
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard

	// onChunk runs under f.mu, so proc and killed are guarded by it.
	var proc *os.Process
	killed := false
	f.mu.Lock()
	f.onChunk = func(n int, _ *fakeGUpload, _ *uploadv1.Chunk) error {
		if n == 6 && proc != nil {
			killed = proc.Kill() == nil
			return status.Error(codes.Unavailable, "killed")
		}
		return nil
	}
	if err := cmd.Start(); err != nil {
		f.mu.Unlock()
		t.Fatal(err)
	}
	proc = cmd.Process
	f.mu.Unlock()
	_ = cmd.Wait()
	f.mu.Lock()
	f.onChunk = nil
	wasKilled := killed
	f.mu.Unlock()
	if !wasKilled {
		t.Fatal("helper exited without being killed")
	}
	if row := outboxRow(t, state, sha); row.Status != StatusUploading || row.UploadRef == "" {
		t.Fatalf("persisted row after SIGKILL = %+v", row)
	}

	pool := x509.NewCertPool()
	pool.AddCert(cert)
	cfg := baseConfig(ProtoGRPC, addr)
	cfg.TLS = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	res := runOnce(t, t.Context(), cfg, state, path)[0]
	expect(t, res, ResultOK)
	if res.BytesResent > testChunk {
		t.Fatalf("re-sent %d bytes after SIGKILL, want <= %d", res.BytesResent, testChunk)
	}
	if res.Resumes != 1 {
		t.Fatalf("resumes = %d, want 1", res.Resumes)
	}
	if got, _ := f.stored(sha); !bytes.Equal(got, data) {
		t.Fatal("stored data differs")
	}
	t.Logf("after SIGKILL: attempts %d resumes %d sent %d resent %d", res.Attempts, res.Resumes, res.BytesSent, res.BytesResent)
}

// TestHelperUploader is the child process of the SIGKILL tests; it is
// skipped unless UPLOADER_HELPER=1. UPLOADER_PROTO defaults to rest.
func TestHelperUploader(t *testing.T) {
	if os.Getenv("UPLOADER_HELPER") != "1" {
		t.Skip("helper process only")
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM([]byte(os.Getenv("UPLOADER_CA")))
	proto := cmp.Or(os.Getenv("UPLOADER_PROTO"), ProtoREST)
	cfg := baseConfig(proto, os.Getenv("UPLOADER_URL"))
	cfg.TLS = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	runOnce(t, t.Context(), cfg, os.Getenv("UPLOADER_STATE"), os.Getenv("UPLOADER_FILE"))
	t.Fatal("helper should have been killed")
}

// TestTusSIGKILLAndResume kills a real uploader process with SIGKILL while a
// PATCH is in flight, then resumes from the same SQLite outbox.
func TestTusSIGKILLAndResume(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a process")
	}
	dir := t.TempDir()
	state := filepath.Join(dir, "s.db")
	path, data, sha := writeFile(t, dir, "kill.flac", 8*testChunk+9)
	f := newFakeTus("/rest/files/")
	ts := httptest.NewUnstartedServer(f)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	t.Cleanup(ts.Close)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ts.Certificate().Raw})

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), exe, "-test.run=^TestHelperUploader$", "-test.count=1")
	cmd.Env = append(os.Environ(), "UPLOADER_HELPER=1", "UPLOADER_URL="+ts.URL,
		"UPLOADER_STATE="+state, "UPLOADER_FILE="+path, "UPLOADER_CA="+string(caPEM))
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard

	// The handler runs under f.mu, so proc and killed are guarded by it.
	var proc *os.Process
	killed := false
	f.mu.Lock()
	f.onPatch = func(n int, u *fakeUpload, _ http.ResponseWriter, r *http.Request) bool {
		if n == 4 && proc != nil {
			killed = proc.Kill() == nil
			b, _ := io.ReadAll(r.Body) // whatever arrived before the kill
			u.data = append(u.data, b...)
			panic(http.ErrAbortHandler)
		}
		return false
	}
	if err := cmd.Start(); err != nil {
		f.mu.Unlock()
		t.Fatal(err)
	}
	proc = cmd.Process
	f.mu.Unlock()
	_ = cmd.Wait()
	f.mu.Lock()
	f.onPatch = nil
	wasKilled := killed
	f.mu.Unlock()
	if !wasKilled {
		t.Fatal("helper exited without being killed")
	}
	if row := outboxRow(t, state, sha); row.Status != StatusUploading || row.UploadRef == "" {
		t.Fatalf("persisted row after SIGKILL = %+v", row)
	}

	pool := x509.NewCertPool()
	pool.AddCert(ts.Certificate())
	cfg := baseConfig(ProtoREST, ts.URL)
	cfg.TLS = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	res := runOnce(t, t.Context(), cfg, state, path)[0]
	expect(t, res, ResultOK)
	if res.BytesResent > testChunk {
		t.Fatalf("re-sent %d bytes after SIGKILL, want <= %d", res.BytesResent, testChunk)
	}
	if n := count(f.callLog(), "POST"); n != 1 {
		t.Fatalf("POSTs = %d, want 1", n)
	}
	if got, _ := f.stored(sha); !bytes.Equal(got, data) {
		t.Fatal("stored data differs")
	}
	t.Logf("after SIGKILL: attempts %d resumes %d sent %d resent %d", res.Attempts, res.Resumes, res.BytesSent, res.BytesResent)
}
