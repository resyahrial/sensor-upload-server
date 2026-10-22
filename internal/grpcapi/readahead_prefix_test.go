package grpcapi

import (
	"context"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
	"github.com/resyahrial/sensor-upload-server/internal/grpcprefix"
)

// countingBody counts the bytes read from the request body.
type countingBody struct {
	io.ReadCloser
	n *atomic.Int64
}

func (c countingBody) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	c.n.Add(int64(n))
	return n, err
}

// serveTLS serves h over TLS with HTTP/2 and points e.client at it through
// the /grpc prefix. Every request body read is counted in read.
func serveTLS(t *testing.T, e *env, h http.Handler, read *atomic.Int64) {
	t.Helper()
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = countingBody{r.Body, read}
		h.ServeHTTP(w, r)
	}))
	ts.EnableHTTP2 = true
	ts.StartTLS()
	t.Cleanup(ts.Close)

	pool := x509.NewCertPool()
	pool.AddCert(ts.Certificate())
	opts := append(grpcprefix.DialOptions(Prefix), grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(pool, "")))
	conn, err := grpc.NewClient(ts.Listener.Addr().String(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	e.client = uploadv1.NewUploadServiceClient(conn)
}

// settle waits until n has not changed for quiet (or until max) and returns
// its value.
func settle(n *atomic.Int64, quiet, max time.Duration) int64 {
	deadline := time.Now().Add(max)
	last, since := n.Load(), time.Now()
	for time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		if v := n.Load(); v != last {
			last, since = v, time.Now()
		} else if time.Since(since) >= quiet {
			break
		}
	}
	return last
}

// A client that pipelines chunks without waiting for acks must not make the
// ServeHTTP path read (and buffer) more than the read-ahead budget beyond
// what the handler has received, however slowly the service commits.
func TestPrefixPathBoundsReadAhead(t *testing.T) {
	e := newEnv(t)
	gs, err := NewServer(e.svc)
	if err != nil {
		t.Fatal(err)
	}
	var read atomic.Int64
	serveTLS(t, e, PrefixHandler(http.NotFoundHandler(), LimitReadAhead(gs, e.svc.MaxMessageBytes())), &read)

	// Stall the commit loop at the second chunk.
	stall := make(chan struct{})
	var unstall sync.Once
	release := func() { unstall.Do(func() { close(stall) }) }
	t.Cleanup(release)
	var commits atomic.Int64
	e.repo.onCommit = func(context.Context) {
		if commits.Add(1) > 1 {
			<-stall
		}
	}

	f := newFile(t, 1<<20)
	id := e.init(f).GetUploadId()
	initBytes := read.Load()
	s := e.stream(ctxFor(t, device))
	sent := make(chan error, 1)
	go func() {
		for off := 0; off < len(f.data); off += maxChunk {
			end := min(off+maxChunk, len(f.data))
			if err := s.Send(&uploadv1.Chunk{UploadId: id, Offset: int64(off), Data: f.data[off:end]}); err != nil {
				sent <- err
				return
			}
		}
		sent <- s.CloseSend()
	}()

	ahead := settle(&read, 300*time.Millisecond, 10*time.Second) - initBytes
	// While stalled the handler holds at most four chunks (one committed,
	// one committing, one in the receive channel, one in the Recv
	// goroutine); everything else read from the body is read-ahead.
	const msg = maxChunk + 64 // frame header + chunk fields, generous
	limit := int64(readAheadBudget + 4*msg)
	t.Logf("body bytes read while stalled: %d (budget %d, file %d)", ahead, readAheadBudget, len(f.data))
	if ahead > limit {
		t.Fatalf("read %d body bytes while the service was stalled, want <= %d", ahead, limit)
	}

	release()
	if err := <-sent; err != nil {
		t.Fatalf("send: %v", err)
	}
	acks, err := recvAll(s)
	if err != nil || len(acks) == 0 || !acks[len(acks)-1].GetComplete() {
		t.Fatalf("upload: %d acks, %v", len(acks), err)
	}
	equalBytes(t, e.fin.data(f.req.GetSha256()), f.data)
}
