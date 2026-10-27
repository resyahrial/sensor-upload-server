// Package client implements the sensor upload client of
// sensor-upload-integration.md §5: a SQLite outbox, SHA-256 at queue time,
// resume from the server offset, exponential backoff with full jitter, and the
// tus (/rest, /rest-http3) and gRPC (/grpc) transports.
package client

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
	"github.com/resyahrial/sensor-upload-server/internal/grpcprefix"
)

// Protocol labels accepted by Config.Protocol.
const (
	ProtoREST       = "rest"
	ProtoRESTHTTP3  = "rest-http3"
	ProtoGRPC       = "grpc"
	ProtoGRPCNative = "grpc-native" // same client as grpc; URL points at the native listener
)

// Report result values.
const (
	ResultOK          = "ok"
	ResultDuplicate   = "duplicate" // 461 / ALREADY_EXISTS
	ResultFallback    = "fallback"  // completed on /rest after the HTTP/3 fallback
	ResultFailed      = "failed"
	ResultInterrupted = "interrupted" // context cancelled; resumable on the next run
)

// h3FallbackAfter is the number of consecutive QUIC connection failures that
// trigger the switch to /rest (contract §3).
const h3FallbackAfter = 3

// Config configures a Runner.
type Config struct {
	Protocol    string        // rest | rest-http3 | grpc | grpc-native
	URL         string        // https://host:port (tus) or host:port / https://host:port (gRPC)
	Token       string        // static access token (JWT); ignored when Tokens is set
	Device      string        // device_id
	Priority    int           // 1–4
	Chunk       int           // bytes per chunk
	Retries     int           // retries after the first attempt
	BackoffMax  time.Duration // backoff cap
	Concurrency int           // parallel files
	H3Fallback  bool          // allow rest-http3 → rest fallback
	TLS         *tls.Config   // client TLS (CA pool / insecure)

	// Tokens supplies access tokens (e.g. a RefreshingSource); nil means
	// StaticToken(Token).
	Tokens TokenSource

	// GRPCWindow is the number of chunks the gRPC client may have sent but
	// not yet acked (default 1). It must be >= the server's GRPC_ACK_EVERY_N,
	// or every stream stalls until GRPCAckTimeout.
	GRPCWindow int
	// GRPCAckTimeout ends a stream that got no ack for this long while the
	// window is full; 0 = the per-request timeout (60 s per 256 KiB of chunk).
	GRPCAckTimeout time.Duration

	// GRPCDialOptions are appended after the defaults (tests use bufconn).
	GRPCDialOptions []grpc.DialOption
	// HTTPClient overrides the HTTP client for rest/rest-http3 (tests).
	HTTPClient *http.Client
	// Logger receives progress logs; nil discards them.
	Logger *slog.Logger
}

// Result is one report row (requirements §11).
type Result struct {
	Protocol    string
	File        string
	Size        int64
	Chunk       int
	Duration    time.Duration
	Attempts    int
	Resumes     int
	BytesSent   int64
	BytesResent int64
	Result      string
	Err         error
}

// Runner uploads files through the outbox.
type Runner struct {
	cfg Config
	ob  *Outbox
	log *slog.Logger

	mu         sync.Mutex
	transports map[string]transport
}

// New validates cfg and returns a Runner using ob.
func New(cfg Config, ob *Outbox) (*Runner, error) {
	switch cfg.Protocol {
	case ProtoREST, ProtoRESTHTTP3, ProtoGRPC, ProtoGRPCNative:
	default:
		return nil, fmt.Errorf("unknown protocol %q", cfg.Protocol)
	}
	if cfg.URL == "" || cfg.Device == "" {
		return nil, errors.New("url and device are required")
	}
	if cfg.Tokens == nil {
		if cfg.Token == "" {
			return nil, errors.New("an access token or token source is required")
		}
		cfg.Tokens = StaticToken(cfg.Token)
	}
	if cfg.Chunk <= 0 {
		return nil, errors.New("chunk must be positive")
	}
	if cfg.Priority < 1 || cfg.Priority > 4 {
		return nil, fmt.Errorf("priority %d out of range 1–4", cfg.Priority)
	}
	cfg.Concurrency = max(cfg.Concurrency, 1)
	cfg.Retries = max(cfg.Retries, 0)
	cfg.GRPCWindow = max(cfg.GRPCWindow, 1)
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Runner{cfg: cfg, ob: ob, log: log, transports: map[string]transport{}}, nil
}

// Close releases the transports.
func (r *Runner) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	for _, t := range r.transports {
		errs = append(errs, t.close())
	}
	r.transports = map[string]transport{}
	return errors.Join(errs...)
}

// reqTimeout follows contract §5.2: about 60 s per 256 KiB of chunk.
func (r *Runner) reqTimeout() time.Duration {
	return time.Duration(max(1, (r.cfg.Chunk+262143)/262144)) * 60 * time.Second
}

func (r *Runner) transport(proto string) (transport, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.transports[proto]; ok {
		return t, nil
	}
	t, err := r.newTransport(proto)
	if err != nil {
		return nil, err
	}
	r.transports[proto] = t
	return t, nil
}

func (r *Runner) newTransport(proto string) (transport, error) {
	switch proto {
	case ProtoREST, ProtoRESTHTTP3:
		base := strings.TrimRight(r.cfg.URL, "/") + "/" + proto + "/files/"
		t := &tusClient{base: base, tokens: r.cfg.Tokens, device: r.cfg.Device, reqTimeout: r.reqTimeout(), hc: r.cfg.HTTPClient}
		if t.hc == nil {
			t.hc, t.closeFn = r.httpClient(proto)
		}
		return t, nil
	default:
		target, err := grpcTarget(r.cfg.URL)
		if err != nil {
			return nil, err
		}
		opts := append(grpcprefix.DialOptions("/grpc"),
			grpc.WithTransportCredentials(credentials.NewTLS(r.tlsConfig())),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time: 30 * time.Second, Timeout: 10 * time.Second, PermitWithoutStream: true,
			}),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1<<20)),
		)
		opts = append(opts, r.cfg.GRPCDialOptions...)
		conn, err := grpc.NewClient(target, opts...)
		if err != nil {
			return nil, fmt.Errorf("grpc client: %w", err)
		}
		ackTimeout := r.cfg.GRPCAckTimeout
		if ackTimeout <= 0 {
			ackTimeout = r.reqTimeout()
		}
		return &grpcClient{conn: conn, c: uploadv1.NewUploadServiceClient(conn), tokens: r.cfg.Tokens,
			device: r.cfg.Device, reqTimeout: r.reqTimeout(), window: r.cfg.GRPCWindow, ackTimeout: ackTimeout}, nil
	}
}

func (r *Runner) tlsConfig() *tls.Config {
	if r.cfg.TLS != nil {
		return r.cfg.TLS.Clone()
	}
	return &tls.Config{MinVersion: tls.VersionTLS12}
}

// httpClient builds the contract §5.2/§5.3 client: keep-alive, 15 s connect
// timeout; HTTP/3 uses quic-go's http3.Transport.
func (r *Runner) httpClient(proto string) (*http.Client, func() error) {
	if proto == ProtoRESTHTTP3 {
		tr := &http3.Transport{
			TLSClientConfig: r.tlsConfig(),
			QUICConfig: &quic.Config{
				HandshakeIdleTimeout: 15 * time.Second,
				MaxIdleTimeout:       60 * time.Second,
				KeepAlivePeriod:      20 * time.Second,
			},
		}
		return &http.Client{Transport: tr}, tr.Close
	}
	tr := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSClientConfig:     r.tlsConfig(),
		TLSHandshakeTimeout: 15 * time.Second,
		ForceAttemptHTTP2:   true,
		IdleConnTimeout:     90 * time.Second,
		MaxIdleConnsPerHost: max(r.cfg.Concurrency, 2),
	}
	return &http.Client{Transport: tr}, nil
}

// grpcTarget accepts "https://host:port" or a gRPC target ("host:port",
// "dns:///host:port", …), which is passed through unchanged.
func grpcTarget(raw string) (string, error) {
	if !strings.HasPrefix(raw, "https://") && !strings.HasPrefix(raw, "http://") {
		return raw, nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("grpc url: %w", err)
	}
	if u.Port() == "" {
		return net.JoinHostPort(u.Hostname(), "443"), nil
	}
	return u.Host, nil
}

// Run queues paths (hashing new files once) and uploads them with
// Config.Concurrency workers, highest priority first. Results are in the
// order of paths. The error is non-nil only for setup failures.
func (r *Runner) Run(ctx context.Context, paths []string) ([]Result, error) {
	rows := make([]Row, len(paths))
	for i, p := range paths {
		row, err := r.enqueue(ctx, p)
		if err != nil {
			return nil, err
		}
		rows[i] = row
	}

	order := make([]int, len(rows))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		ra, rb := rows[order[a]], rows[order[b]]
		if ra.Priority != rb.Priority {
			return ra.Priority < rb.Priority
		}
		return ra.NextRetryAt.Before(rb.NextRetryAt)
	})

	results := make([]Result, len(rows))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range r.cfg.Concurrency {
		wg.Go(func() {
			for i := range jobs {
				results[i] = r.process(ctx, rows[i])
			}
		})
	}
	for _, i := range order {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return results, nil
}

// enqueue returns the outbox row for path, hashing the file only if it is new.
func (r *Runner) enqueue(ctx context.Context, path string) (Row, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return Row{}, fmt.Errorf("queue %s: %w", path, err)
	}
	row, ok, err := r.ob.ByPath(ctx, abs)
	if err != nil {
		return Row{}, err
	}
	if ok {
		if row.Status != StatusDone && row.Protocol != r.cfg.Protocol {
			// An upload cannot move between endpoints; start fresh.
			row.Protocol, row.UploadRef = r.cfg.Protocol, ""
			if err := r.ob.Save(ctx, row); err != nil {
				return Row{}, err
			}
		}
		return row, nil
	}
	row, err = describeFile(abs)
	if err != nil {
		return Row{}, err
	}
	row.Priority, row.Protocol, row.Status = r.cfg.Priority, r.cfg.Protocol, StatusQueued
	return r.ob.Insert(ctx, row)
}

// describeFile hashes the file and fills the metadata fixed at queue time.
func describeFile(path string) (Row, error) {
	f, err := os.Open(path)
	if err != nil {
		return Row{}, fmt.Errorf("queue: %w", err)
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return Row{}, fmt.Errorf("queue: %w", err)
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return Row{}, fmt.Errorf("queue: hash %s: %w", path, err)
	}
	return Row{
		SHA256:      hex.EncodeToString(h.Sum(nil)),
		Path:        path,
		Size:        st.Size(),
		Filename:    filepath.Base(path),
		ContentType: contentType(path),
		CapturedAt:  st.ModTime().Format(time.RFC3339),
	}, nil
}

// contentType maps extensions to the server allow-list (requirements FR-1a).
func contentType(path string) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".webp":
		return "image/webp"
	case ".avif":
		return "image/avif"
	case ".wav":
		return "audio/wav"
	case ".flac":
		return "audio/flac"
	case ".ogg":
		return "audio/ogg"
	case ".opus":
		return "audio/opus"
	case ".mp4":
		return "video/mp4"
	case ".mov":
		return "video/quicktime"
	case ".json":
		return "application/json"
	default:
		return "application/octet-stream"
	}
}

func (r *Runner) result(row Row, start time.Time, res string, err error) Result {
	return Result{
		Protocol:    r.cfg.Protocol,
		File:        row.Path,
		Size:        row.Size,
		Chunk:       r.cfg.Chunk,
		Duration:    time.Since(start),
		Attempts:    row.Attempts,
		Resumes:     row.Resumes,
		BytesSent:   row.BytesSent,
		BytesResent: row.BytesResent,
		Result:      res,
		Err:         err,
	}
}

// finish persists the terminal state of row.
func (r *Runner) finish(ctx context.Context, row Row, start time.Time, status, res string, cause error) Result {
	row.Status, row.Result = status, res
	if err := r.ob.Save(context.WithoutCancel(ctx), row); err != nil {
		cause = errors.Join(cause, err)
	}
	r.log.Info("file finished", "file", row.Path, "result", res, "attempts", row.Attempts, "error", cause)
	return r.result(row, start, res, cause)
}

// process drives one file to done or failed (contract §5.1 upload loop).
func (r *Runner) process(ctx context.Context, row Row) Result {
	start := time.Now()
	if row.Status == StatusDone {
		return r.result(row, start, row.Result, nil)
	}
	if row.Status == StatusFailed {
		// A new run retries failed files from scratch.
		row.Attempts, row.NextRetryAt, row.Status = 0, time.Time{}, StatusQueued
	}
	fellBack := false
	step, h3Fails := 0, 0

	for {
		if row.Attempts > r.cfg.Retries {
			return r.finish(ctx, row, start, StatusFailed, ResultFailed, errors.New("retries exhausted"))
		}
		if err := sleepUntil(ctx, row.NextRetryAt); err != nil {
			return r.result(row, start, ResultInterrupted, err)
		}
		row.Attempts++
		row.Status = StatusUploading
		if err := r.ob.Save(ctx, row); err != nil {
			return r.result(row, start, ResultInterrupted, err)
		}

		before := row.SentHWM
		progressed, err := r.attempt(ctx, &row)
		if serr := r.ob.Save(context.WithoutCancel(ctx), row); serr != nil {
			err = errors.Join(err, serr)
		}
		r.log.Debug("attempt", "file", row.Path, "attempt", row.Attempts, "hwm_before", before, "error", err)

		switch {
		case err == nil:
			res := ResultOK
			if fellBack {
				res = ResultFallback
			}
			return r.finish(ctx, row, start, StatusDone, res, nil)
		case errors.Is(err, ErrAlreadyExists):
			return r.finish(ctx, row, start, StatusDone, ResultDuplicate, nil)
		case ctx.Err() != nil:
			return r.result(row, start, ResultInterrupted, ctx.Err())
		case errors.Is(err, ErrPermanent):
			return r.finish(ctx, row, start, StatusFailed, ResultFailed, err)
		case errors.Is(err, ErrExpired):
			row.UploadRef = "" // create a new upload right away
			continue
		case errors.Is(err, ErrOffsetMismatch):
			continue // the next attempt re-syncs with HEAD / Init
		case errors.Is(err, ErrChecksum):
			if rerr := r.reread(ctx, &row); rerr != nil {
				return r.finish(ctx, row, start, StatusFailed, ResultFailed, rerr)
			}
		}

		// Transient (or checksum): back off. Progress resets the exponent.
		if progressed {
			step = 0
		}
		if row.Protocol == ProtoRESTHTTP3 && errors.Is(err, errConn) {
			h3Fails++
		} else {
			h3Fails = 0
		}
		if r.cfg.H3Fallback && row.Protocol == ProtoRESTHTTP3 && h3Fails >= h3FallbackAfter {
			r.log.Warn("HTTP/3 failing, falling back to /rest", "file", row.Path)
			row.Protocol, row.UploadRef, fellBack, step, h3Fails = ProtoREST, "", true, 0, 0
			row.NextRetryAt = time.Time{}
			continue
		}
		row.NextRetryAt = time.Now().Add(backoff(step, r.cfg.BackoffMax))
		step++
	}
}

// attempt runs one begin+send cycle. progressed reports whether the server
// offset advanced.
func (r *Runner) attempt(ctx context.Context, row *Row) (progressed bool, err error) {
	t, err := r.transport(row.Protocol)
	if err != nil {
		return false, err
	}
	s, err := t.begin(ctx, *row)
	if err != nil {
		return false, err
	}
	if s.Ref != row.UploadRef {
		row.UploadRef = s.Ref
		if err := r.ob.Save(ctx, *row); err != nil {
			return false, err
		}
	}
	if s.Offset > 0 {
		row.Resumes++
	}
	if s.Complete {
		return true, nil
	}

	f, err := os.Open(row.Path)
	if err != nil {
		return false, fmt.Errorf("open %s: %w", row.Path, err)
	}
	defer func() { _ = f.Close() }()

	sha := row.SHA256
	p := &progress{
		hwm:       row.SentHWM,
		committed: s.Offset,
		claim:     func(end int64) error { return r.ob.SaveHWM(ctx, sha, end) },
	}
	err = t.send(ctx, *row, s, f, r.cfg.Chunk, p)
	sent, resent, hwm, committed := p.snapshot()
	row.BytesSent += sent
	row.BytesResent += resent
	row.SentHWM = max(row.SentHWM, hwm)
	return committed > s.Offset, err
}

// reread re-hashes the file after a server checksum failure (contract: re-read
// from SD, re-upload). A changed hash re-keys the outbox row.
func (r *Runner) reread(ctx context.Context, row *Row) error {
	fresh, err := describeFile(row.Path)
	if err != nil {
		return err
	}
	old := row.SHA256
	row.SHA256, row.Size, row.UploadRef = fresh.SHA256, fresh.Size, ""
	if fresh.SHA256 == old {
		return r.ob.Save(ctx, *row)
	}
	return r.ob.Rekey(ctx, old, *row)
}

func sleepUntil(ctx context.Context, t time.Time) error {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
