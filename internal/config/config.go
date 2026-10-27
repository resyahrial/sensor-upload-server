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

	// JWTSigningKeyFile is the PEM P-256 private key that signs access
	// tokens; its public key is always accepted.
	JWTSigningKeyFile string
	// JWTVerifyKeysDir holds extra accepted public keys (*.pem), e.g. the
	// previous signing key during a rollover; empty = none.
	JWTVerifyKeysDir string
	// JWTIssuer is the iss claim of issued and accepted tokens.
	JWTIssuer string
	// AccessTokenTTL is the access token lifetime.
	AccessTokenTTL time.Duration
	// RefreshIdleTTL: a refresh token unused for this long is refused.
	RefreshIdleTTL time.Duration
	// RefreshMaxAge: refresh is refused this long after the device's
	// first token (registration); the device must register again.
	RefreshMaxAge time.Duration
	// RegisterRetryAfter is the Retry-After of a pending registration.
	RegisterRetryAfter time.Duration

	StagingDir       string
	TusStaging       string
	ObjStore         string
	ObjStoreLocalDir string
	S3               S3

	MaxFileBytes    int64
	IncompleteTTL   time.Duration
	CleanupInterval time.Duration

	// RetentionMaxAge is how long completed uploads are kept; 0 keeps
	// them forever.
	RetentionMaxAge time.Duration
	// DiskPath is the filesystem watched for disk pressure.
	DiskPath string
	// DiskHighWatermark and DiskLowWatermark are used-space percentages:
	// at or above high, the oldest completed uploads are deleted until
	// usage is below low.
	DiskHighWatermark int
	DiskLowWatermark  int
	DiskCheckInterval time.Duration

	GRPCMaxChunkBytes int
	GRPCAckEveryN     int

	QUICIdleTimeout time.Duration
	QUICKeepAlive   time.Duration

	ContentTypes []string
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
		ListenAddr:         p.str("LISTEN_ADDR", ":8443"),
		GRPCNativeAddr:     p.str("GRPC_NATIVE_ADDR", ""),
		MetricsAddr:        p.str("METRICS_ADDR", ":9100"),
		TLSCertFile:        p.str("TLS_CERT_FILE", ""),
		TLSKeyFile:         p.str("TLS_KEY_FILE", ""),
		DatabaseURL:        p.str("DATABASE_URL", ""),
		JWTSigningKeyFile:  p.str("JWT_SIGNING_KEY_FILE", ""),
		JWTVerifyKeysDir:   p.str("JWT_VERIFY_KEYS_DIR", ""),
		JWTIssuer:          p.str("JWT_ISSUER", "sensor-upload-server"),
		AccessTokenTTL:     p.duration("ACCESS_TOKEN_TTL", time.Hour),
		RefreshIdleTTL:     p.duration("REFRESH_IDLE_TTL", 90*24*time.Hour),
		RefreshMaxAge:      p.duration("REFRESH_MAX_AGE", 365*24*time.Hour),
		RegisterRetryAfter: p.duration("REGISTER_RETRY_AFTER", time.Hour),
		StagingDir:         p.str("STAGING_DIR", "./data/staging"),
		TusStaging:         p.str("TUS_STAGING", BackendFile),
		ObjStore:           p.str("OBJSTORE", BackendLocal),
		ObjStoreLocalDir:   p.str("OBJSTORE_LOCAL_DIR", "./data/objects"),
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
		RetentionMaxAge:   p.duration("RETENTION_MAX_AGE", 168*time.Hour),
		DiskHighWatermark: int(p.int64("DISK_HIGH_WATERMARK", 85)),
		DiskLowWatermark:  int(p.int64("DISK_LOW_WATERMARK", 70)),
		DiskCheckInterval: p.duration("DISK_CHECK_INTERVAL", time.Minute),
		GRPCMaxChunkBytes: int(p.int64("GRPC_MAX_CHUNK_BYTES", 262144)),
		GRPCAckEveryN:     int(p.int64("GRPC_ACK_EVERY_N", 1)),
		QUICIdleTimeout:   p.duration("QUIC_IDLE_TIMEOUT", 60*time.Second),
		QUICKeepAlive:     p.duration("QUIC_KEEPALIVE", 20*time.Second),
		ContentTypes:      p.list("CONTENT_TYPES", DefaultContentTypes),
		LogLevel:          p.level("LOG_LEVEL", slog.LevelInfo),
		ShutdownTimeout:   p.duration("SHUTDOWN_TIMEOUT", 30*time.Second),
	}
	// The disk that fills up: the object directory, or (objects in S3)
	// the staging directory unless DISK_PATH names e.g. MinIO's data dir.
	defDisk := c.StagingDir
	if c.ObjStore == BackendLocal {
		defDisk = c.ObjStoreLocalDir
	}
	c.DiskPath = p.str("DISK_PATH", defDisk)
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
		{"JWT_SIGNING_KEY_FILE", c.JWTSigningKeyFile},
		{"JWT_ISSUER", c.JWTIssuer},
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
		{"DISK_CHECK_INTERVAL", int64(c.DiskCheckInterval)},
		{"QUIC_IDLE_TIMEOUT", int64(c.QUICIdleTimeout)},
		{"QUIC_KEEPALIVE", int64(c.QUICKeepAlive)},
		{"SHUTDOWN_TIMEOUT", int64(c.ShutdownTimeout)},
		{"ACCESS_TOKEN_TTL", int64(c.AccessTokenTTL)},
		{"REFRESH_IDLE_TTL", int64(c.RefreshIdleTTL)},
		{"REFRESH_MAX_AGE", int64(c.RefreshMaxAge)},
		{"REGISTER_RETRY_AFTER", int64(c.RegisterRetryAfter)},
	}
	for _, v := range positive {
		if v.val <= 0 {
			fail("%s must be positive", v.name)
		}
	}
	if c.RetentionMaxAge < 0 {
		fail("RETENTION_MAX_AGE must not be negative (0 disables it)")
	}
	if c.DiskPath == "" {
		fail("DISK_PATH is required")
	}
	wmOK := true
	for _, w := range []struct {
		name string
		val  int
	}{{"DISK_HIGH_WATERMARK", c.DiskHighWatermark}, {"DISK_LOW_WATERMARK", c.DiskLowWatermark}} {
		if w.val < 1 || w.val > 100 {
			fail("%s must be between 1 and 100, got %d", w.name, w.val)
			wmOK = false
		}
	}
	if wmOK && c.DiskLowWatermark >= c.DiskHighWatermark {
		fail("DISK_LOW_WATERMARK (%d) must be lower than DISK_HIGH_WATERMARK (%d)", c.DiskLowWatermark, c.DiskHighWatermark)
	}
	if c.RefreshIdleTTL > 0 && c.RefreshMaxAge > 0 && c.RefreshIdleTTL > c.RefreshMaxAge {
		fail("REFRESH_IDLE_TTL (%s) must not exceed REFRESH_MAX_AGE (%s)", c.RefreshIdleTTL, c.RefreshMaxAge)
	}
	if c.AccessTokenTTL > 0 && c.RefreshIdleTTL > 0 && c.AccessTokenTTL >= c.RefreshIdleTTL {
		fail("ACCESS_TOKEN_TTL (%s) must be shorter than REFRESH_IDLE_TTL (%s)", c.AccessTokenTTL, c.RefreshIdleTTL)
	}
	if c.QUICKeepAlive > 0 && c.QUICIdleTimeout > 0 && c.QUICKeepAlive >= c.QUICIdleTimeout {
		fail("QUIC_KEEPALIVE (%s) must be shorter than QUIC_IDLE_TIMEOUT (%s)", c.QUICKeepAlive, c.QUICIdleTimeout)
	}

	if len(c.ContentTypes) == 0 {
		fail("CONTENT_TYPES must list at least one content type")
	}
	return errs
}

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
