// Package app wires the server together (config → database, stores,
// protocol handlers, listeners) so that cmd/server and the in-process e2e
// harness run the same code.
package app

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/tus/tusd/v2/pkg/handler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/cleanup"
	"github.com/resyahrial/sensor-upload-server/internal/config"
	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/deviceapi"
	"github.com/resyahrial/sensor-upload-server/internal/fileapi"
	"github.com/resyahrial/sensor-upload-server/internal/finalize"
	"github.com/resyahrial/sensor-upload-server/internal/grpcadapt"
	"github.com/resyahrial/sensor-upload-server/internal/grpcapi"
	"github.com/resyahrial/sensor-upload-server/internal/httpx"
	"github.com/resyahrial/sensor-upload-server/internal/meta"
	"github.com/resyahrial/sensor-upload-server/internal/objstore"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
	"github.com/resyahrial/sensor-upload-server/internal/staging"
	"github.com/resyahrial/sensor-upload-server/internal/tusapi"
)

const (
	readyCheckTimeout = 2 * time.Second
	dbStartupTimeout  = 60 * time.Second
	// recoveryTimeout bounds startup crash recovery; leftovers are
	// finalized by the next HEAD/PATCH.
	recoveryTimeout = 2 * time.Minute
)

// Options configures an App.
type Options struct {
	Config config.Config
	Log    *slog.Logger
	// ObjectStore replaces the OBJSTORE backend when non-nil (tests use it
	// to inject faults).
	ObjectStore objstore.Store
}

// App is a fully wired server with its listeners bound.
type App struct {
	cfg     config.Config
	log     *slog.Logger
	pool    *pgxpool.Pool
	public  *httpx.HTTPServer
	h3      *httpx.H3Server
	admin   *httpx.HTTPServer
	native  *nativeGRPC // nil when GRPC_NATIVE_ADDR is empty
	grpcSvc *grpcapi.Service
	grpcSrv *grpc.Server
	cleaner *cleanup.Cleaner
	// retention deletes completed uploads by age and under disk pressure.
	retention *cleanup.Retention
	bound     []io.Closer // listeners to release if New fails
	tusStage  *tusapi.Staging
	Registry  *prometheus.Registry
}

// New connects to the database, applies migrations, builds the stores and
// handlers (tus, gRPC, expiry cleaner), runs tus crash recovery and binds
// all listeners. The UDP
// (HTTP/3) listener uses the TCP listener's port, so LISTEN_ADDR ":0"
// gives one random port for both. Call Run to serve.
func New(ctx context.Context, opts Options) (_ *App, err error) {
	cfg, log := opts.Config, opts.Log
	// nil behind a TLS-terminating proxy (BEHIND_PROXY): plain HTTP, no HTTP/3.
	var tlsCfg *tls.Config
	if !cfg.BehindProxy {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load TLS key pair: %w", err)
		}
		tlsCfg = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	}
	if err := os.MkdirAll(cfg.StagingDir, 0o750); err != nil {
		return nil, fmt.Errorf("create staging dir: %w", err)
	}
	startCtx, cancelStart := context.WithTimeout(ctx, dbStartupTimeout)
	defer cancelStart()
	pool, err := db.Open(startCtx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	a := &App{cfg: cfg, log: log, pool: pool}
	defer func() {
		if err != nil {
			a.close()
		}
	}()
	if err := db.Migrate(startCtx, pool); err != nil {
		return nil, err
	}
	log.Info("database ready")
	// Stateless JWT verification: no database lookup per request
	// (docs/decisions.md "Decisions (JWT + refresh)").
	issuer, verifier, err := newAuth(cfg)
	if err != nil {
		return nil, err
	}
	store := opts.ObjectStore
	if store == nil {
		if store, err = newObjectStore(ctx, cfg); err != nil {
			return nil, err
		}
	}
	a.Registry = prometheus.NewRegistry()
	if err := registerRuntimeCollectors(a.Registry); err != nil {
		return nil, err
	}
	metrics, err := obs.NewMetrics(a.Registry)
	if err != nil {
		return nil, fmt.Errorf("register metrics: %w", err)
	}

	repo := db.NewRepo(pool)
	fin := finalize.New(repo, store, metrics, log, nil)
	staging, err := newTusStaging(ctx, cfg)
	if err != nil {
		return nil, err
	}
	a.tusStage = staging
	recCtx, cancelRec := context.WithTimeout(ctx, recoveryTimeout)
	defer cancelRec()
	if err := tusapi.Recover(recCtx, repo, fin, staging, log); err != nil {
		log.Warn("tus recovery incomplete", "error", err.Error())
	}

	rules := meta.Rules{MaxFileBytes: cfg.MaxFileBytes, ContentTypes: cfg.ContentTypes}
	tusCfg := func(base, protocol string) tusapi.Config {
		return tusapi.Config{
			BasePath:      base,
			BehindProxy:   cfg.BehindProxy,
			Protocol:      protocol,
			Rules:         rules,
			IncompleteTTL: cfg.IncompleteTTL,
			Staging:       staging,
			Repo:          repo,
			Finalizer:     fin,
			Verifier:      verifier,
			Metrics:       metrics,
			Log:           log,
		}
	}
	restTus, err := tusapi.New(tusCfg(tusapi.BasePathREST, tusapi.ProtocolREST))
	if err != nil {
		return nil, err
	}
	h3Tus, err := tusapi.New(tusCfg(tusapi.BasePathRESTHTTP3, tusapi.ProtocolRESTHTTP3))
	if err != nil {
		return nil, err
	}

	if err := a.newGRPC(cfg, tlsCfg, rules, repo, fin, verifier, metrics); err != nil {
		return nil, err
	}
	a.retention, err = cleanup.NewRetention(cleanup.RetentionConfig{
		MaxAge:        cfg.RetentionMaxAge,
		Interval:      cfg.CleanupInterval,
		DiskPath:      cfg.DiskPath,
		HighWatermark: cfg.DiskHighWatermark,
		LowWatermark:  cfg.DiskLowWatermark,
		DiskInterval:  cfg.DiskCheckInterval,
	}, cleanup.RetentionDeps{Repo: repo, Store: store, Metrics: metrics, Logger: log})
	if err != nil {
		return nil, err
	}

	// Public and unauthenticated (see docs/decisions.md "verify endpoint").
	files := fileapi.NewHandler(fileapi.Config{Repo: repo, Metrics: metrics, Log: log})
	// Device registration and token refresh (unauthenticated JSON).
	devices, err := deviceapi.NewHandler(deviceapi.Config{
		Store:       db.NewDevices(pool),
		Issuer:      issuer,
		IdleTTL:     cfg.RefreshIdleTTL,
		MaxAge:      cfg.RefreshMaxAge,
		RetryAfter:  cfg.RegisterRetryAfter,
		BehindProxy: cfg.BehindProxy,
		Metrics:     metrics,
		Log:         log,
	})
	if err != nil {
		return nil, err
	}
	routes := httpx.RouterConfig{
		REST:    restTus,
		Files:   files,
		Devices: devices,
		// The router strips "/grpc" itself, so the grpc.Server goes here
		// without grpcapi.PrefixHandler (which would strip a second time),
		// behind the read-ahead limit that ServeHTTP lacks.
		GRPC: grpcapi.LimitReadAhead(a.grpcSrv, a.grpcSvc.MaxMessageBytes()),
	}
	// The router is built after the listener is bound (Alt-Svc needs the
	// real port), so the server dispatches through this indirection.
	var public http.Handler = http.NotFoundHandler()
	a.public, err = httpx.Listen(httpx.ServerConfig{
		Name:    "public",
		Addr:    cfg.ListenAddr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { public.ServeHTTP(w, r) }),
		TLS:     tlsCfg,
		// gRPC from the proxy arrives as h2c.
		UnencryptedHTTP2: cfg.BehindProxy,
		ShutdownCause:    handler.ErrServerShutdown,
	})
	if err != nil {
		return nil, err
	}
	a.bound = append(a.bound, a.public)
	if tlsCfg != nil {
		port := strconv.Itoa(a.public.Addr().(*net.TCPAddr).Port)
		if err := a.listenH3(cfg, tlsCfg, port, h3Tus, files); err != nil {
			return nil, err
		}
		routes.AltSvc = httpx.AltSvcH3(port)
	}
	public = httpx.NewRouter(routes)

	if cfg.GRPCNativeAddr != "" {
		if a.native, err = listenNativeGRPC(cfg.GRPCNativeAddr, a.grpcSrv); err != nil {
			return nil, err
		}
		a.bound = append(a.bound, a.native)
	}

	checks := []httpx.Check{
		{Name: "postgres", Checker: pool},
		{Name: "staging", Checker: httpx.DirWritable(cfg.StagingDir)},
		{Name: "objstore", Checker: store},
	}
	a.admin, err = httpx.Listen(httpx.ServerConfig{
		Name:    "admin",
		Addr:    cfg.MetricsAddr,
		Handler: httpx.AdminHandler(a.Registry, checks, readyCheckTimeout, a.log),
	})
	if err != nil {
		return nil, err
	}
	return a, nil
}

// listenH3 binds the HTTP/3 listener on the public TCP listener's port.
func (a *App) listenH3(cfg config.Config, tlsCfg *tls.Config, port string, h3Tus, files http.Handler) error {
	h3mux := http.NewServeMux()
	h3mux.Handle(tusapi.BasePathRESTHTTP3, h3Tus)
	h3mux.Handle(httpx.PrefixFiles, files)
	host, _, err := net.SplitHostPort(cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("LISTEN_ADDR: %w", err)
	}
	a.h3, err = httpx.ListenH3(httpx.H3Config{
		Name:            "public-h3",
		Addr:            net.JoinHostPort(host, port),
		Handler:         h3mux,
		TLS:             tlsCfg,
		MaxIdleTimeout:  cfg.QUICIdleTimeout,
		KeepAlivePeriod: cfg.QUICKeepAlive,
		ShutdownCause:   handler.ErrServerShutdown,
	})
	if err != nil {
		return err
	}
	a.bound = append(a.bound, a.h3)
	return nil
}

// Addr returns the public TCP address (the UDP port is the same).
func (a *App) Addr() net.Addr { return a.public.Addr() }

// GRPCNativeAddr returns the native gRPC listener's address, or nil when
// GRPC_NATIVE_ADDR is empty.
func (a *App) GRPCNativeAddr() net.Addr {
	if a.native == nil {
		return nil
	}
	return a.native.Addr()
}

// TusCleanup returns the expiry cleaner's tus staging; use the same value
// for cleanup.Deps.Tus["rest"] and ["rest-http3"].
func (a *App) TusCleanup() tusapi.CleanupStaging { return a.tusStage.ForCleanup() }

// AdminAddr returns the metrics/health address.
func (a *App) AdminAddr() net.Addr { return a.admin.Addr() }

// Run serves and runs the expiry cleaner and retention until ctx is cancelled or a
// listener fails, then shuts down gracefully within SHUTDOWN_TIMEOUT:
//
//  1. the gRPC service rejects new calls with UNAVAILABLE and ends open
//     streams after their current chunk, and Run waits until they let go;
//  2. the TLS TCP listener stops accepting and drains (tus requests and
//     gRPC via ServeHTTP);
//  3. the native gRPC listener stops (GracefulStop, Stop on timeout);
//  4. the HTTP/3 listener drains;
//  5. the admin listener stops (it answers health checks until here);
//
// then the cleaner and retention are stopped and the database pool closed.
func (a *App) Run(ctx context.Context) error {
	defer a.pool.Close()
	nativeAddr, h3Addr := "", ""
	if a.native != nil {
		nativeAddr = a.native.Addr().String()
	}
	if a.h3 != nil {
		h3Addr = a.h3.Addr().String()
	}
	a.log.Info("server started", "listen_addr", a.public.Addr().String(), "h3_addr", h3Addr,
		"grpc_native_addr", nativeAddr, "metrics_addr", a.admin.Addr().String(),
		"behind_proxy", a.cfg.BehindProxy)

	cleanCtx, stopClean := context.WithCancel(context.WithoutCancel(ctx))
	cleanDone, retDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(cleanDone)
		a.cleaner.Run(cleanCtx)
	}()
	go func() {
		defer close(retDone)
		a.retention.Run(cleanCtx)
	}()
	defer func() {
		stopClean()
		<-cleanDone
		<-retDone
	}()

	svcs := []httpx.Service{
		httpx.NewShutdownHook(func(ctx context.Context) error {
			a.grpcSvc.Shutdown()
			return a.grpcSvc.Drain(ctx)
		}),
		a.public,
	}
	if a.native != nil {
		svcs = append(svcs, a.native)
	}
	if a.h3 != nil {
		svcs = append(svcs, a.h3)
	}
	svcs = append(svcs, a.admin)
	err := httpx.Run(ctx, a.log, a.cfg.ShutdownTimeout, svcs...)
	// Nothing is served any more; close whatever ServeHTTP left behind.
	a.grpcSrv.Stop()
	if err != nil {
		return err
	}
	a.log.Info("server stopped")
	return nil
}

// newAuth loads the signing key and builds the access token issuer and a
// verifier accepting the signing key plus JWT_VERIFY_KEYS_DIR.
func newAuth(cfg config.Config) (*auth.Issuer, *auth.Verifier, error) {
	key, err := auth.LoadSigningKey(cfg.JWTSigningKeyFile)
	if err != nil {
		return nil, nil, err
	}
	issuer, err := auth.NewIssuer(auth.IssuerConfig{Key: key, Issuer: cfg.JWTIssuer, TTL: cfg.AccessTokenTTL})
	if err != nil {
		return nil, nil, err
	}
	keys, err := auth.LoadVerifyKeys(cfg.JWTVerifyKeysDir)
	if err != nil {
		return nil, nil, err
	}
	keys[issuer.KeyID()] = issuer.PublicKey()
	verifier, err := auth.NewVerifier(auth.VerifierConfig{Issuer: cfg.JWTIssuer, Keys: keys})
	if err != nil {
		return nil, nil, err
	}
	return issuer, verifier, nil
}

// newGRPC builds the gRPC service (staging in STAGING_DIR/grpc), its
// grpc.Server and the expiry cleaner, which needs the service's registry.
func (a *App) newGRPC(cfg config.Config, tlsCfg *tls.Config, rules meta.Rules, repo *db.Repo,
	fin *finalize.Finalizer, verifier *auth.Verifier, metrics *obs.Metrics) error {
	store, err := staging.New(filepath.Join(cfg.StagingDir, "grpc"))
	if err != nil {
		return fmt.Errorf("grpc staging: %w", err)
	}
	a.grpcSvc, err = grpcapi.NewService(grpcapi.Config{
		MaxChunkBytes: cfg.GRPCMaxChunkBytes,
		AckEveryN:     cfg.GRPCAckEveryN,
		IdleTimeout:   grpcapi.DefaultIdleTimeout,
		Rules:         rules,
	}, grpcapi.Deps{
		Repo:      grpcadapt.NewRepo(repo),
		Finalizer: grpcadapt.NewFinalizer(fin),
		Store:     store,
		Verifier:  verifier,
		Metrics:   metrics,
		Logger:    a.log,
	})
	if err != nil {
		return err
	}
	// The credentials apply to the native listener (grpc.Server.Serve) only;
	// ServeHTTP on the public TCP listener ignores them. Behind a proxy
	// (tlsCfg nil) the native listener is plaintext too.
	var opts []grpc.ServerOption
	if tlsCfg != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}
	if a.grpcSrv, err = grpcapi.NewServer(a.grpcSvc, opts...); err != nil {
		return err
	}
	tus := a.TusCleanup()
	deps := cleanup.Deps{
		Repo:        repo,
		GRPC:        a.grpcSvc,
		GRPCStaging: store,
		Finalizer:   fin,
		Tus:         map[string]cleanup.TusStaging{tusapi.ProtocolREST: tus, tusapi.ProtocolRESTHTTP3: tus},
		Metrics:     metrics,
		Logger:      a.log,
	}
	ccfg := cleanup.Config{TTL: cfg.IncompleteTTL, Interval: cfg.CleanupInterval}
	if cfg.TusStaging != config.BackendS3 {
		ccfg.TusDir = filepath.Join(cfg.StagingDir, "tus")
		deps.TusFiles = newTusFiles(ccfg.TusDir)
	}
	a.cleaner, err = cleanup.New(ccfg, deps)
	return err
}

// close releases what New acquired when New fails.
func (a *App) close() {
	for _, c := range a.bound {
		_ = c.Close() // never served; nothing to drain
	}
	a.pool.Close()
}

func s3Config(cfg config.Config) objstore.S3Config {
	return objstore.S3Config{
		Endpoint:       cfg.S3.Endpoint,
		Region:         cfg.S3.Region,
		Bucket:         cfg.S3.Bucket,
		AccessKey:      cfg.S3.AccessKey,
		SecretKey:      cfg.S3.SecretKey,
		ForcePathStyle: cfg.S3.ForcePathStyle,
	}
}

// newObjectStore builds the OBJSTORE backend.
func newObjectStore(ctx context.Context, cfg config.Config) (objstore.Store, error) {
	if cfg.ObjStore == config.BackendS3 {
		s, err := objstore.NewS3(ctx, s3Config(cfg))
		if err != nil {
			return nil, fmt.Errorf("object store: %w", err)
		}
		return s, nil
	}
	s, err := objstore.NewLocal(cfg.ObjStoreLocalDir)
	if err != nil {
		return nil, fmt.Errorf("object store: %w", err)
	}
	return s, nil
}

// newTusStaging builds the TUS_STAGING backend; locks always live in
// STAGING_DIR/tus.
func newTusStaging(ctx context.Context, cfg config.Config) (*tusapi.Staging, error) {
	dir := filepath.Join(cfg.StagingDir, "tus")
	if cfg.TusStaging != config.BackendS3 {
		return tusapi.NewFileStaging(dir)
	}
	client, err := objstore.NewS3Client(ctx, s3Config(cfg))
	if err != nil {
		return nil, fmt.Errorf("tus staging: %w", err)
	}
	return tusapi.NewS3Staging(dir, cfg.S3.Bucket, client)
}

func registerRuntimeCollectors(reg prometheus.Registerer) error {
	for _, c := range []prometheus.Collector{
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	} {
		if err := reg.Register(c); err != nil {
			return fmt.Errorf("register runtime metrics: %w", err)
		}
	}
	return nil
}
