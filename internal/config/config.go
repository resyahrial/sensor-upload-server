// Package config loads and validates the server configuration from
// environment variables (requirements §8).
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Supported JWT algorithms.
const (
	AlgES256 = "ES256"
	AlgHS256 = "HS256"
)

// MinHMACSecretBytes is the minimum JWT_HMAC_SECRET length (256 bits, the
// HS256 output size; RFC 7518 §3.2).
const MinHMACSecretBytes = 32

// Storage backends.
const (
	BackendFile  = "file"
	BackendLocal = "local"
	BackendS3    = "s3"
)

// DefaultContentTypes is the FR-1a content-type allow-list.
var DefaultContentTypes = []string{
	"image/jpeg", "image/png", "image/webp", "image/avif",
	"audio/wav", "audio/x-wav", "audio/flac", "audio/ogg", "audio/opus",
	"video/mp4", "video/quicktime", "application/json",
}

// S3 holds S3-compatible object storage settings.
type S3 struct {
	Endpoint       string
	Region         string
	Bucket         string
	AccessKey      string
	SecretKey      string
	ForcePathStyle bool
}

// Config is the validated server configuration.
type Config struct {
	ListenAddr     string
	GRPCNativeAddr string
	MetricsAddr    string

	TLSCertFile string
	TLSKeyFile  string

	DatabaseURL string

	StagingDir       string
	TusStaging       string
	ObjStore         string
	ObjStoreLocalDir string
	S3               S3

	MaxFileBytes    int64
	IncompleteTTL   time.Duration
	CleanupInterval time.Duration

	GRPCMaxChunkBytes int
	GRPCAckEveryN     int

	QUICIdleTimeout time.Duration
	QUICKeepAlive   time.Duration

	JWTAlgs          []string
	JWTPublicKeyFile string
	JWTHMACSecret    string

	ContentTypes []string
	DevMode      bool
	LogLevel     slog.Level

	ShutdownTimeout time.Duration
}

// LookupFunc looks up an environment variable; os.LookupEnv satisfies it.
type LookupFunc func(key string) (string, bool)

// Load reads the configuration through lookup, applies defaults and
// validates it. All problems are reported together.
func Load(lookup LookupFunc) (Config, error) {
	p := parser{lookup: lookup}
	c := Config{
		ListenAddr:       p.str("LISTEN_ADDR", ":8443"),
		GRPCNativeAddr:   p.str("GRPC_NATIVE_ADDR", ""),
		MetricsAddr:      p.str("METRICS_ADDR", ":9100"),
		TLSCertFile:      p.str("TLS_CERT_FILE", ""),
		TLSKeyFile:       p.str("TLS_KEY_FILE", ""),
		DatabaseURL:      p.str("DATABASE_URL", ""),
		StagingDir:       p.str("STAGING_DIR", "./data/staging"),
		TusStaging:       p.str("TUS_STAGING", BackendFile),
		ObjStore:         p.str("OBJSTORE", BackendLocal),
		ObjStoreLocalDir: p.str("OBJSTORE_LOCAL_DIR", "./data/objects"),
		S3: S3{
			Endpoint:       p.str("S3_ENDPOINT", ""),
			Region:         p.str("S3_REGION", ""),
			Bucket:         p.str("S3_BUCKET", ""),
			AccessKey:      p.str("S3_ACCESS_KEY", ""),
			SecretKey:      p.str("S3_SECRET_KEY", ""),
			ForcePathStyle: p.boolean("S3_FORCE_PATH_STYLE", false),
		},
		MaxFileBytes:      p.int64("MAX_FILE_BYTES", 524288000),
		IncompleteTTL:     p.duration("INCOMPLETE_TTL", 168*time.Hour),
		CleanupInterval:   p.duration("CLEANUP_INTERVAL", time.Hour),
		GRPCMaxChunkBytes: int(p.int64("GRPC_MAX_CHUNK_BYTES", 262144)),
		GRPCAckEveryN:     int(p.int64("GRPC_ACK_EVERY_N", 1)),
		QUICIdleTimeout:   p.duration("QUIC_IDLE_TIMEOUT", 60*time.Second),
		QUICKeepAlive:     p.duration("QUIC_KEEPALIVE", 20*time.Second),
		JWTAlgs:           p.list("JWT_ALGS", []string{AlgES256}),
		JWTPublicKeyFile:  p.str("JWT_PUBLIC_KEY_FILE", ""),
		JWTHMACSecret:     p.str("JWT_HMAC_SECRET", ""),
		ContentTypes:      p.list("CONTENT_TYPES", DefaultContentTypes),
		DevMode:           p.boolean("DEV_MODE", false),
		LogLevel:          p.level("LOG_LEVEL", slog.LevelInfo),
		ShutdownTimeout:   p.duration("SHUTDOWN_TIMEOUT", 30*time.Second),
	}
	if c.DevMode {
		if _, set := lookup("LOG_LEVEL"); !set {
			c.LogLevel = slog.LevelDebug
		}
	}
	errs := append(p.errs, c.validate()...)
	if len(errs) > 0 {
		return Config{}, fmt.Errorf("invalid configuration: %w", errors.Join(errs...))
	}
	return c, nil
}

func (c Config) validate() []error {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	required := []struct{ name, val string }{
		{"TLS_CERT_FILE", c.TLSCertFile},
		{"TLS_KEY_FILE", c.TLSKeyFile},
		{"DATABASE_URL", c.DatabaseURL},
		{"STAGING_DIR", c.StagingDir},
	}
	for _, r := range required {
		if r.val == "" {
			fail("%s is required", r.name)
		}
	}
	for _, a := range []struct{ name, val string }{
		{"LISTEN_ADDR", c.ListenAddr}, {"METRICS_ADDR", c.MetricsAddr}, {"GRPC_NATIVE_ADDR", c.GRPCNativeAddr},
	} {
		if a.val == "" && a.name == "GRPC_NATIVE_ADDR" {
			continue
		}
		if _, port, err := net.SplitHostPort(a.val); err != nil || port == "" {
			fail("%s: %q is not a host:port address", a.name, a.val)
		}
	}

	if c.TusStaging != BackendFile && c.TusStaging != BackendS3 {
		fail("TUS_STAGING must be %q or %q, got %q", BackendFile, BackendS3, c.TusStaging)
	}
	switch c.ObjStore {
	case BackendLocal:
		if c.ObjStoreLocalDir == "" {
			fail("OBJSTORE_LOCAL_DIR is required when OBJSTORE=local")
		}
	case BackendS3:
	default:
		fail("OBJSTORE must be %q or %q, got %q", BackendLocal, BackendS3, c.ObjStore)
	}
	if c.ObjStore == BackendS3 || c.TusStaging == BackendS3 {
		if c.S3.Bucket == "" {
			fail("S3_BUCKET is required when OBJSTORE or TUS_STAGING is s3")
		}
		if c.S3.Region == "" {
			fail("S3_REGION is required when OBJSTORE or TUS_STAGING is s3")
		}
	}

	positive := []struct {
		name string
		val  int64
	}{
		{"MAX_FILE_BYTES", c.MaxFileBytes},
		{"GRPC_MAX_CHUNK_BYTES", int64(c.GRPCMaxChunkBytes)},
		{"GRPC_ACK_EVERY_N", int64(c.GRPCAckEveryN)},
		{"INCOMPLETE_TTL", int64(c.IncompleteTTL)},
		{"CLEANUP_INTERVAL", int64(c.CleanupInterval)},
		{"QUIC_IDLE_TIMEOUT", int64(c.QUICIdleTimeout)},
		{"QUIC_KEEPALIVE", int64(c.QUICKeepAlive)},
		{"SHUTDOWN_TIMEOUT", int64(c.ShutdownTimeout)},
	}
	for _, v := range positive {
		if v.val <= 0 {
			fail("%s must be positive", v.name)
		}
	}
	if c.QUICKeepAlive > 0 && c.QUICIdleTimeout > 0 && c.QUICKeepAlive >= c.QUICIdleTimeout {
		fail("QUIC_KEEPALIVE (%s) must be shorter than QUIC_IDLE_TIMEOUT (%s)", c.QUICKeepAlive, c.QUICIdleTimeout)
	}

	if len(c.JWTAlgs) == 0 {
		fail("JWT_ALGS must list at least one algorithm")
	}
	for _, alg := range c.JWTAlgs {
		switch alg {
		case AlgES256:
			if c.JWTPublicKeyFile == "" {
				fail("JWT_PUBLIC_KEY_FILE is required when JWT_ALGS contains ES256")
			}
		case AlgHS256:
			if !c.DevMode {
				fail("JWT_ALGS: HS256 is only allowed when DEV_MODE=true")
			}
			switch n := len(c.JWTHMACSecret); {
			case n == 0:
				fail("JWT_HMAC_SECRET is required when JWT_ALGS contains HS256")
			case n < MinHMACSecretBytes:
				fail("JWT_HMAC_SECRET must be at least %d bytes, got %d", MinHMACSecretBytes, n)
			}
		default:
			fail("JWT_ALGS: unsupported algorithm %q (allowed: ES256, HS256)", alg)
		}
	}
	if len(c.ContentTypes) == 0 {
		fail("CONTENT_TYPES must list at least one content type")
	}
	return errs
}

// HasAlg reports whether alg is enabled in JWT_ALGS.
func (c Config) HasAlg(alg string) bool { return slices.Contains(c.JWTAlgs, alg) }

// parser reads typed values and accumulates errors.
type parser struct {
	lookup LookupFunc
	errs   []error
}

func (p *parser) raw(key string) (string, bool) {
	v, ok := p.lookup(key)
	if !ok {
		return "", false
	}
	v = strings.TrimSpace(v)
	return v, v != ""
}

func (p *parser) str(key, def string) string {
	if v, ok := p.raw(key); ok {
		return v
	}
	return def
}

func (p *parser) int64(key string, def int64) int64 {
	v, ok := p.raw(key)
	if !ok {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: %q is not an integer", key, v))
		return def
	}
	return n
}

func (p *parser) duration(key string, def time.Duration) time.Duration {
	v, ok := p.raw(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: %q is not a duration (e.g. 30s, 1h)", key, v))
		return def
	}
	return d
}

func (p *parser) boolean(key string, def bool) bool {
	v, ok := p.raw(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: %q is not a boolean", key, v))
		return def
	}
	return b
}

func (p *parser) list(key string, def []string) []string {
	v, ok := p.raw(key)
	if !ok {
		return slices.Clone(def)
	}
	var out []string
	for item := range strings.SplitSeq(v, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

func (p *parser) level(key string, def slog.Level) slog.Level {
	v, ok := p.raw(key)
	if !ok {
		return def
	}
	var l slog.Level
	if err := l.UnmarshalText([]byte(v)); err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: %q is not a log level (debug, info, warn, error)", key, v))
		return def
	}
	return l
}
