package grpcapi

import (
	"context"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
	"github.com/resyahrial/sensor-upload-server/internal/grpcprefix"
)

func TestPrefixHandlerOverTLSHTTP2(t *testing.T) {
	e := newEnv(t)
	gs, err := NewServer(e.svc)
	if err != nil {
		t.Fatal(err)
	}
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	ts := httptest.NewUnstartedServer(PrefixHandler(next, LimitReadAhead(gs, e.svc.MaxMessageBytes())))
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

	f := newFile(t, 2*maxChunk+1)
	id := e.init(f).GetUploadId()
	acks, err := e.upload(f, id, 0)
	if err != nil || !acks[len(acks)-1].GetComplete() {
		t.Fatalf("upload via ServeHTTP: %v %v", acks, err)
	}
	equalBytes(t, e.fin.data(f.req.GetSha256()), f.data)

	// Non-gRPC requests, and gRPC requests outside /grpc/, go to next.
	hc := ts.Client()
	for _, tc := range []struct{ path, ctype string }{
		{"/grpc/upload.v1.UploadService/Init", "text/plain"},
		{"/rest/files/", "application/grpc"},
		{"/grpcx/upload.v1.UploadService/Init", "application/grpc"},
	} {
		res, err := hc.Post(ts.URL+tc.path, tc.ctype, strings.NewReader(""))
		if err != nil {
			t.Fatal(err)
		}
		_ = res.Body.Close()
		if res.StatusCode != http.StatusTeapot {
			t.Errorf("%s %s: status %d, want next handler", tc.path, tc.ctype, res.StatusCode)
		}
	}
}

func TestNewServerRegistersBothNames(t *testing.T) {
	e := newEnv(t)
	gs, err := NewServer(e.svc)
	if err != nil {
		t.Fatal(err)
	}
	info := gs.GetServiceInfo()
	for _, name := range []string{"upload.v1.UploadService", "grpc/upload.v1.UploadService"} {
		if _, ok := info[name]; !ok {
			t.Errorf("service %q not registered", name)
		}
	}
}

func TestNativeServerLimits(t *testing.T) {
	t.Run("header list size", func(t *testing.T) {
		e := newEnv(t)
		// The first call receives the server's SETTINGS.
		_, err := e.client.Status(ctxFor(t, device), &uploadv1.StatusRequest{})
		wantCode(t, err, codes.InvalidArgument)
		ctx := metadata.AppendToOutgoingContext(ctxFor(t, device), "x-pad", strings.Repeat("a", 2*maxHeaderListSize))
		_, err = e.client.Status(ctx, &uploadv1.StatusRequest{})
		wantCode(t, err, codes.Internal)
	})
	t.Run("concurrent streams", func(t *testing.T) {
		e := newEnv(t)
		_, err := e.client.Status(ctxFor(t, device), &uploadv1.StatusRequest{})
		wantCode(t, err, codes.InvalidArgument)
		ctx, cancel := context.WithCancel(ctxFor(t, device))
		defer cancel()
		for range maxConcurrentStreams {
			e.stream(ctx)
		}
		// The next stream waits for one of the others to end.
		wctx, wcancel := context.WithTimeout(ctxFor(t, device), 300*time.Millisecond)
		defer wcancel()
		if _, err := e.client.Upload(wctx); status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("stream %d: err = %v, want DEADLINE_EXCEEDED", maxConcurrentStreams+1, err)
		}
	})
}
