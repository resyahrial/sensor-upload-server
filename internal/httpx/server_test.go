package httpx

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"testing"
	"time"
)

func testTLSConfig(t *testing.T) (*tls.Config, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}, pool
}

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func TestTLSServerServesHTTP1AndHTTP2(t *testing.T) {
	tlsCfg, pool := testTLSConfig(t)
	srv, err := Listen(ServerConfig{
		Name: "public", Addr: "127.0.0.1:0", TLS: tlsCfg,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, r.Proto)
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })
	url := "https://" + srv.Addr().String() + "/"

	for _, tc := range []struct {
		name  string
		proto func(*http.Protocols)
		want  string
	}{
		{"http1", func(p *http.Protocols) { p.SetHTTP1(true) }, "HTTP/1.1"},
		{"http2", func(p *http.Protocols) { p.SetHTTP2(true) }, "HTTP/2.0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := new(http.Protocols)
			tc.proto(p)
			c := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}, Protocols: p}}
			res, err := c.Get(url)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			body, _ := io.ReadAll(res.Body)
			if string(body) != tc.want {
				t.Errorf("proto = %q, want %q", body, tc.want)
			}
		})
	}

	// TLS 1.1 must be refused.
	c := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MaxVersion: tls.VersionTLS11}}}
	if res, err := c.Get(url); err == nil {
		res.Body.Close()
		t.Error("TLS 1.1 handshake should fail")
	}
}

func TestPlainServerServesH2CWhenEnabled(t *testing.T) {
	for _, tc := range []struct {
		name    string
		h2c     bool
		wantH2C bool
	}{
		{"enabled", true, true},
		{"disabled", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := Listen(ServerConfig{
				Name: "public", Addr: "127.0.0.1:0", UnencryptedHTTP2: tc.h2c,
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.WriteString(w, r.Proto)
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			go func() { _ = srv.Serve() }()
			t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })
			url := "http://" + srv.Addr().String() + "/"

			p := new(http.Protocols)
			p.SetUnencryptedHTTP2(true)
			c := &http.Client{Transport: &http.Transport{Protocols: p}}
			res, err := c.Get(url)
			if !tc.wantH2C {
				if err == nil {
					res.Body.Close()
					t.Error("h2c request should fail when UnencryptedHTTP2 is off")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			if body, _ := io.ReadAll(res.Body); string(body) != "HTTP/2.0" {
				t.Errorf("proto = %q, want HTTP/2.0", body)
			}
		})
	}
}

func TestListenFailsOnBusyPort(t *testing.T) {
	a, err := Listen(ServerConfig{Name: "a", Addr: "127.0.0.1:0", Handler: http.NotFoundHandler()})
	if err != nil {
		t.Fatal(err)
	}
	defer a.ln.Close()
	if _, err := Listen(ServerConfig{Name: "b", Addr: a.Addr().String(), Handler: http.NotFoundHandler()}); err == nil {
		t.Fatal("expected listen error on busy port")
	}
}

func TestListenBoundsHTTP2ReceiveBuffer(t *testing.T) {
	srv, err := Listen(ServerConfig{Name: "public", Addr: "127.0.0.1:0", Handler: http.NotFoundHandler()})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.ln.Close()
	if got := srv.srv.HTTP2; got == nil || got.MaxReceiveBufferPerStream != MaxReceiveBufferPerStream {
		t.Fatalf("HTTP2 config = %+v, want MaxReceiveBufferPerStream %d", got, MaxReceiveBufferPerStream)
	}
}

// TestRunGracefulShutdown simulates SIGTERM (ctx cancel) during an in-flight
// request: the request must finish, its context must carry the shutdown
// cause, and Run must return nil.
func TestRunGracefulShutdown(t *testing.T) {
	started := make(chan struct{})
	causeSeen := make(chan error, 1)
	cause := errors.New("custom shutdown")
	srv, err := Listen(ServerConfig{
		Name: "public", Addr: "127.0.0.1:0", ShutdownCause: cause,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(started)
			<-r.Context().Done()
			causeSeen <- context.Cause(r.Context())
			time.Sleep(50 * time.Millisecond) // finish the in-flight work
			_, _ = io.WriteString(w, "done")
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- Run(ctx, discardLogger(), 5*time.Second, srv) }()

	resc := make(chan string, 1)
	go func() {
		res, err := http.Get("http://" + srv.Addr().String() + "/")
		if err != nil {
			resc <- "error: " + err.Error()
			return
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		resc <- string(b)
	}()
	<-started
	stop() // SIGTERM

	if got := <-resc; got != "done" {
		t.Errorf("in-flight request result = %q, want done", got)
	}
	if got := <-causeSeen; !errors.Is(got, cause) {
		t.Errorf("request context cause = %v, want %v", got, cause)
	}
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Run = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
}

func TestRunShutdownTimeout(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	srv, err := Listen(ServerConfig{
		Name: "slow", Addr: "127.0.0.1:0",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(started)
			<-release // ignores cancellation: a stuck handler
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- Run(ctx, discardLogger(), 100*time.Millisecond, srv) }()
	go func() {
		if res, err := http.Get("http://" + srv.Addr().String() + "/"); err == nil {
			res.Body.Close()
		}
	}()
	<-started
	begin := time.Now()
	stop()
	select {
	case err := <-runErr:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Run = %v, want deadline exceeded", err)
		}
		if d := time.Since(begin); d > 2*time.Second {
			t.Errorf("shutdown took %v, timeout was 100ms", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after timeout")
	}
}

type failingService struct{ err error }

func (f failingService) Serve() error                   { return f.err }
func (f failingService) Shutdown(context.Context) error { return nil }

func TestRunStopsWhenAServiceFails(t *testing.T) {
	ok, err := Listen(ServerConfig{Name: "ok", Addr: "127.0.0.1:0", Handler: http.NotFoundHandler()})
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	err = Run(context.Background(), discardLogger(), time.Second, failingService{boom}, ok)
	if !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want boom", err)
	}
}
