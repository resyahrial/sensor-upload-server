//go:build integration

package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/client"
	"github.com/resyahrial/sensor-upload-server/internal/db"
)

// TestGRPCFullUpload: a full upload through ServeHTTP on the TLS listener
// (/grpc prefix, stripped once by the router) and through the native
// listener with and without the prefix.
func TestGRPCFullUpload(t *testing.T) {
	s := StartServer(t, nil)
	for _, m := range grpcModes {
		t.Run(m.name, func(t *testing.T) {
			dev := device()
			g := DialGRPC(t, s, m, dev)
			f := NewFile(t, dev, 3*chunk+123)
			res := g.MustInit(f)
			if res.GetCommittedOffset() != 0 || res.GetMaxChunkBytes() != 262144 {
				t.Fatalf("init = %v", res)
			}
			acks, err := g.UploadFrom(res.GetUploadId(), f, 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(acks) != 4 {
				t.Errorf("acks = %d, want one per chunk", len(acks))
			}
			AssertGRPCStored(t, s, res.GetUploadId(), f)
			st, err := g.C.Status(g.Ctx(), &uploadv1.StatusRequest{UploadId: res.GetUploadId()})
			if err != nil || !st.GetComplete() || st.GetCommittedOffset() != int64(len(f.Data)) {
				t.Errorf("status after completion = %v, %v", st, err)
			}
			_, err = g.C.Init(g.Ctx(), InitReq(f, ""))
			WantCode(t, "init after completion", err, codes.AlreadyExists)
		})
	}
	t.Run("tls-listener-bare-path-not-routed", func(t *testing.T) {
		dev := device()
		g := DialGRPC(t, s, grpcMode{addr: grpcModes[0].addr}, dev)
		_, err := g.C.Init(g.Ctx(), InitReq(NewFile(t, dev, 10), ""))
		WantCode(t, "bare Init on the TLS listener", err, codes.Unimplemented)
	})
}

// TestGRPCDisconnectResume: the client vanishes after one acked chunk;
// Status reports it and a new stream completes the upload.
func TestGRPCDisconnectResume(t *testing.T) {
	s := StartServer(t, nil)
	for _, m := range grpcModes[:2] {
		t.Run(m.name, func(t *testing.T) {
			dev := device()
			g := DialGRPC(t, s, m, dev)
			f := NewFile(t, dev, 3*chunk)
			id := g.MustInit(f).GetUploadId()
			ctx, cancel := context.WithCancel(g.Ctx())
			st := g.Stream(ctx)
			SendRange(t, st, id, f.Data, 0, chunk, chunk)
			if a, err := st.Recv(); err != nil || a.GetCommittedOffset() != chunk {
				t.Fatalf("first ack = %v, %v", a, err)
			}
			cancel()
			res, err := g.C.Status(g.Ctx(), &uploadv1.StatusRequest{UploadId: id})
			if err != nil || res.GetCommittedOffset() != chunk || res.GetComplete() {
				t.Fatalf("status = %v, %v", res, err)
			}
			if again := g.MustInit(f); again.GetUploadId() != id || again.GetCommittedOffset() != chunk {
				t.Fatalf("init resume = %v", again)
			}
			g.MustUpload(id, f, chunk)
			AssertGRPCStored(t, s, id, f)
			if u := Row(t, s, "grpc", id); u.ResumeCount < 1 {
				t.Errorf("resume_count = %d", u.ResumeCount)
			}
		})
	}
}

// TestGRPCServerRestartResume: the server stops mid-upload; bytes written
// after the last DB commit are cut off on resume (FR-7), and the upload
// completes with exactly one object (NFR-1).
func TestGRPCServerRestartResume(t *testing.T) {
	for _, m := range grpcModes[:2] {
		t.Run(m.name, func(t *testing.T) {
			s := StartServer(t, nil)
			dev := device()
			g := DialGRPC(t, s, m, dev)
			f := NewFile(t, dev, 4*chunk+5)
			id := g.MustInit(f).GetUploadId()
			st := g.Stream(g.Ctx())
			SendRange(t, st, id, f.Data, 0, 2*chunk, chunk)
			for range 2 {
				if _, err := st.Recv(); err != nil {
					t.Fatal(err)
				}
			}
			s.Stop()
			_, err := RecvAll(st)
			WantCode(t, "open stream across shutdown", err, codes.Unavailable)
			// Simulate a crash after a write but before the DB commit.
			bin := filepath.Join(s.GRPCDir(), id+".bin")
			fh, err := os.OpenFile(bin, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fh.Write(bytes.Repeat([]byte{0xff}, 1000)); err != nil {
				t.Fatal(err)
			}
			if err := fh.Close(); err != nil {
				t.Fatal(err)
			}
			s.start()

			g = DialGRPC(t, s, m, dev)
			res := g.MustInit(f)
			if res.GetUploadId() != id || res.GetCommittedOffset() != 2*chunk {
				t.Fatalf("init after restart = %v", res)
			}
			st = g.Stream(g.Ctx())
			SendRange(t, st, id, f.Data, 2*chunk, 3*chunk, chunk)
			if a, err := st.Recv(); err != nil || a.GetCommittedOffset() != 3*chunk {
				t.Fatalf("ack after restart = %v, %v", a, err)
			}
			if fi, err := os.Stat(bin); err != nil || fi.Size() != 3*chunk {
				t.Fatalf("staged .bin after resume: %v, %v; want %d bytes", fi, err, 3*chunk)
			}
			SendRange(t, st, id, f.Data, 3*chunk, len(f.Data), chunk)
			if _, err := RecvAll(st); err != nil {
				t.Fatal(err)
			}
			AssertGRPCStored(t, s, id, f)
			if n := Objects(t, s, dev); n != 1 {
				t.Errorf("objects = %d, want 1", n)
			}
			if n := CompletedRows(t, s, f.SHA256); n != 1 {
				t.Errorf("completed rows = %d, want 1", n)
			}
		})
	}
}

// TestGRPCChecksumAndDuplicate: a wrong sha256 → DATA_LOSS (staging
// deleted, nothing stored); a completed sha256 → ALREADY_EXISTS.
func TestGRPCChecksumAndDuplicate(t *testing.T) {
	s := StartServer(t, nil)
	dev := device()
	g := DialGRPC(t, s, grpcModes[0], dev)
	f := NewFile(t, dev, 2*chunk)
	wrong := make([]byte, 32)
	if _, err := rand.Read(wrong); err != nil {
		t.Fatal(err)
	}
	res, err := g.C.Init(g.Ctx(), InitReq(f, hex.EncodeToString(wrong)))
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.UploadFrom(res.GetUploadId(), f, 0)
	WantCode(t, "upload with wrong sha", err, codes.DataLoss)
	if u := Row(t, s, "grpc", res.GetUploadId()); u.Status != db.StatusFailedChecksum {
		t.Errorf("status = %s", u.Status)
	}
	AssertGRPCNotStaged(t, s, res.GetUploadId())
	if n := Objects(t, s, dev); n != 0 {
		t.Errorf("objects = %d, want 0", n)
	}
	_, err = g.C.Status(g.Ctx(), &uploadv1.StatusRequest{UploadId: res.GetUploadId()})
	WantCode(t, "status of failed upload", err, codes.NotFound)

	id := g.MustInit(f).GetUploadId()
	g.MustUpload(id, f, 0)
	_, err = g.C.Init(g.Ctx(), InitReq(f, ""))
	WantCode(t, "init of completed sha", err, codes.AlreadyExists)
	// Completed through tus: gRPC Init sees it too (cross-protocol dedup).
	h := NewFile(t, dev, chunk)
	tc := Endpoint(t, s, H2, "/rest/files/", dev)
	tc.SendFrom(tc.MustCreate(h), h, 0, chunk)
	_, err = g.C.Init(g.Ctx(), InitReq(h, ""))
	WantCode(t, "init of sha completed via tus", err, codes.AlreadyExists)
}

// TestGRPCCrossProtocolRace: the same file is uploaded concurrently over
// tus and gRPC; exactly one completes and one object is stored.
func TestGRPCCrossProtocolRace(t *testing.T) {
	s := StartServer(t, nil)
	for i := range 3 {
		dev := device()
		f := NewFile(t, dev, 2*chunk)
		tc := Endpoint(t, s, H2, "/rest/files/", dev)
		loc := tc.MustCreate(f)
		tc.SendFrom(loc, NewFileFrom(f, chunk), 0, chunk)
		g := DialGRPC(t, s, grpcModes[i%2], dev)
		id := g.MustInit(f).GetUploadId()
		st := g.Stream(g.Ctx())
		SendRange(t, st, id, f.Data, 0, chunk, chunk)
		if _, err := st.Recv(); err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		var tusStatus int
		var grpcErr error
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			res, _ := tc.Do(context.Background(), http.MethodPatch, loc, bytes.NewReader(f.Data[chunk:]), patchHeaders(chunk))
			if res != nil {
				tusStatus = res.StatusCode
				_ = res.Body.Close()
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			if err := st.Send(&uploadv1.Chunk{UploadId: id, Offset: chunk, Data: f.Data[chunk:]}); err != nil {
				grpcErr = err
				return
			}
			_, grpcErr = RecvAll(st)
		}()
		close(start)
		wg.Wait()

		tusWon, grpcWon := tusStatus == http.StatusNoContent, grpcErr == nil
		if tusWon == grpcWon {
			t.Fatalf("round %d: tus %d, grpc %v; want exactly one winner", i, tusStatus, grpcErr)
		}
		if !tusWon && tusStatus != 461 {
			t.Errorf("round %d: losing tus PATCH = %d, want 461", i, tusStatus)
		}
		if !grpcWon {
			WantCode(t, "losing gRPC stream", grpcErr, codes.AlreadyExists)
		}
		if n := CompletedRows(t, s, f.SHA256); n != 1 {
			t.Errorf("round %d: completed rows = %d, want 1", i, n)
		}
		if n := Objects(t, s, dev); n != 1 {
			t.Errorf("round %d: objects = %d, want 1", i, n)
		}
	}
}

// TestGRPCSecondStreamAborted: only one stream per upload_id.
func TestGRPCSecondStreamAborted(t *testing.T) {
	s := StartServer(t, nil)
	dev := device()
	g := DialGRPC(t, s, grpcModes[1], dev)
	f := NewFile(t, dev, 3*chunk)
	id := g.MustInit(f).GetUploadId()
	first := g.Stream(g.Ctx())
	SendRange(t, first, id, f.Data, 0, chunk, chunk)
	if _, err := first.Recv(); err != nil {
		t.Fatal(err)
	}
	second := g.Stream(g.Ctx())
	SendRange(t, second, id, f.Data, chunk, 2*chunk, chunk)
	_, err := RecvAll(second)
	WantCode(t, "second stream", err, codes.Aborted)
	_, err = g.C.Cancel(g.Ctx(), &uploadv1.CancelRequest{UploadId: id})
	WantCode(t, "cancel during stream", err, codes.Aborted)
	SendRange(t, first, id, f.Data, chunk, len(f.Data), chunk)
	if _, err := RecvAll(first); err != nil {
		t.Fatal(err)
	}
	AssertGRPCStored(t, s, id, f)
}

// TestGRPCAuth: UNAUTHENTICATED for bad tokens, PERMISSION_DENIED for a
// metadata device that does not match the token, NOT_FOUND for another
// device's upload.
func TestGRPCAuth(t *testing.T) {
	s := StartServer(t, nil)
	dev := device()
	owner := DialGRPC(t, s, grpcModes[0], dev)
	f := NewFile(t, dev, chunk)
	id := owner.MustInit(f).GetUploadId()
	expired, err := auth.Sign(auth.HS256, []byte(hmacSecret), dev, time.Now().Add(-2*time.Hour), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range grpcModes {
		for name, tok := range map[string]string{"missing": "", "expired": expired, "alg-none": noneToken(dev)} {
			t.Run(m.name+"/"+name, func(t *testing.T) {
				g := DialGRPC(t, s, m, dev)
				g.Token = tok
				_, err := g.C.Init(g.Ctx(), InitReq(f, ""))
				WantCode(t, "Init", err, codes.Unauthenticated)
				_, err = g.C.Status(g.Ctx(), &uploadv1.StatusRequest{UploadId: id})
				WantCode(t, "Status", err, codes.Unauthenticated)
				_, err = g.C.Cancel(g.Ctx(), &uploadv1.CancelRequest{UploadId: id})
				WantCode(t, "Cancel", err, codes.Unauthenticated)
				st, err := g.C.Upload(g.Ctx())
				if err == nil {
					_ = st.Send(&uploadv1.Chunk{UploadId: id, Offset: 0, Data: f.Data})
					_, err = st.Recv()
				}
				WantCode(t, "Upload", err, codes.Unauthenticated)
			})
		}
	}

	other := DialGRPC(t, s, grpcModes[2], device())
	_, err = other.C.Init(other.Ctx(), InitReq(f, ""))
	WantCode(t, "Init with another device's metadata", err, codes.PermissionDenied)
	_, err = other.C.Status(other.Ctx(), &uploadv1.StatusRequest{UploadId: id})
	WantCode(t, "cross-device Status", err, codes.NotFound)
	_, err = other.C.Cancel(other.Ctx(), &uploadv1.CancelRequest{UploadId: id})
	WantCode(t, "cross-device Cancel", err, codes.NotFound)
	if u := Row(t, s, "grpc", id); u.Status != db.StatusUploading {
		t.Errorf("owner's upload status = %s", u.Status)
	}
	if _, err := owner.C.Cancel(owner.Ctx(), &uploadv1.CancelRequest{UploadId: id}); err != nil {
		t.Errorf("owner Cancel: %v", err)
	}
	AssertGRPCNotStaged(t, s, id)
}

// TestCleanupThroughApp: the app's own cleaner (CLEANUP_INTERVAL 200ms)
// expires stale gRPC and tus uploads.
func TestCleanupThroughApp(t *testing.T) {
	s := StartServer(t, map[string]string{"INCOMPLETE_TTL": "1h", "CLEANUP_INTERVAL": "200ms"})
	ctx := context.Background()
	dev := device()
	g := DialGRPC(t, s, grpcModes[1], dev)
	f := NewFile(t, dev, 2*chunk)
	id := g.MustInit(f).GetUploadId()
	st := g.Stream(g.Ctx())
	SendRange(t, st, id, f.Data, 0, chunk, chunk)
	if _, err := st.Recv(); err != nil {
		t.Fatal(err)
	}
	if err := st.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := RecvAll(st); err != nil {
		t.Fatal(err)
	}
	tc := Endpoint(t, s, H2, "/rest/files/", dev)
	h := NewFile(t, dev, 2*chunk)
	loc := tc.MustCreate(h)
	tc.SendFrom(loc, NewFileFrom(h, chunk), 0, chunk)

	if _, err := s.pool.Exec(ctx, `UPDATE uploads SET updated_at = now() - interval '2 hours' WHERE upload_ref = ANY($1)`,
		[]string{id, refOf(loc)}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		a, b := Row(t, s, "grpc", id), Row(t, s, "rest", loc)
		if a.Status == db.StatusExpired && b.Status == db.StatusExpired {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not expired: grpc %s, tus %s", a.Status, b.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}
	AssertGRPCNotStaged(t, s, id)
	AssertNotStaged(t, s, loc)
	_, err := g.C.Status(g.Ctx(), &uploadv1.StatusRequest{UploadId: id})
	WantCode(t, "Status after expiry", err, codes.NotFound)
	if off := tc.Head(loc); off != -http.StatusGone {
		t.Errorf("HEAD after expiry = %d, want 410", -off)
	}
	// Init of the same file starts over.
	if res := g.MustInit(f); res.GetUploadId() == id || res.GetCommittedOffset() != 0 {
		t.Errorf("init after expiry = %v", res)
	}
}

// TestGRPCGracefulShutdown: SIGTERM during a stream that keeps sending;
// the client gets UNAVAILABLE (or completes) and no acked byte is lost.
func TestGRPCGracefulShutdown(t *testing.T) {
	for _, m := range grpcModes[:2] {
		t.Run(m.name, func(t *testing.T) {
			s := StartServer(t, nil)
			dev := device()
			g := DialGRPC(t, s, m, dev)
			f := NewFile(t, dev, 64*chunk)
			id := g.MustInit(f).GetUploadId()
			st := g.Stream(g.Ctx())
			go func() {
				for off := 0; off < len(f.Data); off += chunk {
					if st.Send(&uploadv1.Chunk{UploadId: id, Offset: int64(off), Data: f.Data[off : off+chunk]}) != nil {
						return
					}
				}
			}()
			if _, err := st.Recv(); err != nil {
				t.Fatal(err)
			}
			stopped := make(chan struct{})
			go func() { s.Stop(); close(stopped) }()
			var acked int64
			complete := false
			for {
				a, err := st.Recv()
				if err != nil {
					WantCode(t, "stream during shutdown", err, codes.Unavailable)
					break
				}
				acked = a.GetCommittedOffset()
				if a.GetComplete() {
					complete = true
					break
				}
			}
			<-stopped
			u := Row(t, s, "grpc", id)
			t.Logf("shutdown: acked %d, committed %d of %d, complete %v", acked, u.CommittedBytes, len(f.Data), complete)
			if u.CommittedBytes < acked {
				t.Fatalf("committed %d < acked %d", u.CommittedBytes, acked)
			}
			s.start()
			if complete {
				AssertGRPCStored(t, s, id, f)
				return
			}
			g = DialGRPC(t, s, m, dev)
			res := g.MustInit(f)
			if res.GetCommittedOffset() != u.CommittedBytes {
				t.Fatalf("init offset %d, DB %d", res.GetCommittedOffset(), u.CommittedBytes)
			}
			g.MustUpload(id, f, int(u.CommittedBytes))
			AssertGRPCStored(t, s, id, f)
		})
	}
}

// TestGRPCContractClient runs the uploader's client code (internal/client)
// with -protocol grpc and grpc-native, at the default -grpc-window 1 against
// the default GRPC_ACK_EVERY_N=1, and at -grpc-window 4 against
// GRPC_ACK_EVERY_N=4. A window below the ack interval must still complete
// through the stall guard and retries.
func TestGRPCContractClient(t *testing.T) {
	cases := []struct {
		name       string
		ackEvery   string
		window     int
		ackTimeout time.Duration
		retries    int
		clean      bool // one attempt, nothing re-sent
	}{
		{"ack1-window1", "1", 1, 0, 3, true},
		{"ack4-window4", "4", 4, 0, 3, true},
		{"ack4-window1-stalls", "4", 1, 300 * time.Millisecond, 40, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := StartServer(t, map[string]string{"GRPC_ACK_EVERY_N": tc.ackEvery})
			for proto, url := range map[string]string{client.ProtoGRPC: s.URL(), client.ProtoGRPCNative: s.NativeAddr()} {
				t.Run(proto, func(t *testing.T) {
					cfg := client.Config{
						Protocol: proto, URL: url, Priority: 2, Chunk: chunk, Retries: tc.retries,
						BackoffMax: 50 * time.Millisecond, TLS: TLSConfig(),
						GRPCWindow: tc.window, GRPCAckTimeout: tc.ackTimeout,
					}
					got, f := runContractClient(t, cfg, 5*chunk+7)
					if got.Result != client.ResultOK {
						t.Fatalf("result %+v", got)
					}
					if tc.clean && (got.BytesResent != 0 || got.Attempts != 1) {
						t.Fatalf("result %+v, want one attempt and nothing re-sent", got)
					}
					if !tc.clean && got.Attempts < 2 {
						t.Fatalf("attempts = %d, want the stall guard to end streams", got.Attempts)
					}
					var n int
					if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM uploads WHERE sha256 = $1 AND status = 'completed' AND protocol = 'grpc'`,
						f.SHA256).Scan(&n); err != nil || n != 1 {
						t.Errorf("completed rows = %d (%v)", n, err)
					}
				})
			}
		})
	}
}

// runContractClient uploads a new file of size bytes with internal/client
// (cfg without Token/Device, which are filled in for a fresh device).
func runContractClient(t *testing.T, cfg client.Config, size int) (client.Result, File) {
	t.Helper()
	dev := device()
	f := NewFile(t, dev, size)
	dir := t.TempDir()
	path := filepath.Join(dir, "sample.flac")
	if err := os.WriteFile(path, f.Data, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ob, err := client.OpenOutbox(ctx, filepath.Join(dir, "outbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer ob.Close()
	cfg.Token, cfg.Device = Token(t, dev), dev
	r, err := client.New(cfg, ob)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	results, err := r.Run(ctx, []string{path})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s window %d: %+v", cfg.Protocol, cfg.GRPCWindow, results[0])
	return results[0], f
}
