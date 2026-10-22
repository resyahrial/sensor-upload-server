package grpcprefix_test

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
	"github.com/resyahrial/sensor-upload-server/internal/grpcprefix"
)

// fakeService answers Init with the full method seen by the server and acks
// every chunk on the Upload stream.
type fakeService struct {
	uploadv1.UnimplementedUploadServiceServer
}

func (fakeService) Init(ctx context.Context, _ *uploadv1.InitRequest) (*uploadv1.InitResponse, error) {
	m, _ := grpc.Method(ctx)
	return &uploadv1.InitResponse{UploadId: m}, nil
}

func (fakeService) Upload(stream grpc.BidiStreamingServer[uploadv1.Chunk, uploadv1.Ack]) error {
	var off int64
	for {
		c, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		off += int64(len(c.GetData()))
		if err := stream.Send(&uploadv1.Ack{CommittedOffset: off, Complete: off >= 6}); err != nil {
			return err
		}
	}
}

func newServer(t *testing.T, prefixed bool) *grpc.Server {
	t.Helper()
	s := grpc.NewServer()
	uploadv1.RegisterUploadServiceServer(s, fakeService{})
	if prefixed {
		if err := grpcprefix.RegisterWithPrefix(s, &uploadv1.UploadService_ServiceDesc, fakeService{}, "/grpc"); err != nil {
			t.Fatalf("RegisterWithPrefix: %v", err)
		}
	}
	t.Cleanup(s.Stop)
	return s
}

// exercise calls Init and a bidi Upload and checks the server saw wantMethod.
func exercise(t *testing.T, conn *grpc.ClientConn, wantMethod string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	c := uploadv1.NewUploadServiceClient(conn)

	res, err := c.Init(ctx, &uploadv1.InitRequest{})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if res.GetUploadId() != wantMethod {
		t.Fatalf("server saw method %q, want %q", res.GetUploadId(), wantMethod)
	}

	st, err := c.Upload(ctx)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	for _, d := range []string{"abc", "def"} {
		if err := st.Send(&uploadv1.Chunk{Data: []byte(d)}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		if _, err := st.Recv(); err != nil {
			t.Fatalf("Recv: %v", err)
		}
	}
	if err := st.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if _, err := st.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("final Recv = %v, want EOF", err)
	}
}

func dial(t *testing.T, addr string, creds credentials.TransportCredentials, prefix bool) *grpc.ClientConn {
	t.Helper()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
	if prefix {
		opts = append(opts, grpcprefix.DialOptions("/grpc")...)
	}
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestNativeServerServesBareAndPrefixed(t *testing.T) {
	s := newServer(t, true)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.Serve(lis) }()

	t.Run("prefixed client", func(t *testing.T) {
		exercise(t, dial(t, lis.Addr().String(), insecure.NewCredentials(), true), "/grpc/upload.v1.UploadService/Init")
	})
	t.Run("bare client", func(t *testing.T) {
		exercise(t, dial(t, lis.Addr().String(), insecure.NewCredentials(), false), "/upload.v1.UploadService/Init")
	})
}

func TestNativeServerWithoutPrefixRejectsPrefixedPath(t *testing.T) {
	s := newServer(t, false)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.Serve(lis) }()

	conn := dial(t, lis.Addr().String(), insecure.NewCredentials(), true)
	_, err = uploadv1.NewUploadServiceClient(conn).Init(t.Context(), &uploadv1.InitRequest{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("Init code = %v, want Unimplemented", status.Code(err))
	}
}

func TestServeHTTPWithStripPrefix(t *testing.T) {
	s := newServer(t, false)
	ts := httptest.NewUnstartedServer(http.StripPrefix("/grpc", s))
	ts.EnableHTTP2 = true
	ts.StartTLS()
	t.Cleanup(ts.Close)

	pool := x509.NewCertPool()
	pool.AddCert(ts.Certificate())
	addr := ts.Listener.Addr().String()
	exercise(t, dial(t, addr, credentials.NewClientTLSFromCert(pool, ""), true), "/upload.v1.UploadService/Init")
}

func TestRegisterWithPrefixErrors(t *testing.T) {
	s := newServer(t, true)
	if err := grpcprefix.RegisterWithPrefix(s, &uploadv1.UploadService_ServiceDesc, fakeService{}, "grpc"); err == nil {
		t.Fatal("duplicate registration: want error")
	}
	if err := grpcprefix.RegisterWithPrefix(s, &uploadv1.UploadService_ServiceDesc, fakeService{}, "/"); err == nil {
		t.Fatal("empty prefix: want error")
	}
}

func TestDesc(t *testing.T) {
	d := grpcprefix.Desc(&uploadv1.UploadService_ServiceDesc, "/grpc/")
	if d.ServiceName != "grpc/upload.v1.UploadService" {
		t.Fatalf("ServiceName = %q", d.ServiceName)
	}
	if uploadv1.UploadService_ServiceDesc.ServiceName != "upload.v1.UploadService" {
		t.Fatal("original desc was modified")
	}
}
