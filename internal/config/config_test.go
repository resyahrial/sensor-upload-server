package config

import (
	"log/slog"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) LookupFunc {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func baseEnv() map[string]string {
	return map[string]string{
		"TLS_CERT_FILE":       "cert.pem",
		"TLS_KEY_FILE":        "key.pem",
		"DATABASE_URL":        "postgres://localhost/db",
	}
}

func with(kv ...string) map[string]string {
	m := baseEnv()
	for i := 0; i < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

func TestLoadDefaults(t *testing.T) {
	c, err := Load(env(baseEnv()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	checks := []struct {
		name      string
		got, want any
	}{
		{"ListenAddr", c.ListenAddr, ":8443"},
		{"GRPCNativeAddr", c.GRPCNativeAddr, ""},
		{"MetricsAddr", c.MetricsAddr, ":9100"},
		{"StagingDir", c.StagingDir, "./data/staging"},
		{"TusStaging", c.TusStaging, "file"},
		{"ObjStore", c.ObjStore, "local"},
		{"ObjStoreLocalDir", c.ObjStoreLocalDir, "./data/objects"},
		{"MaxFileBytes", c.MaxFileBytes, int64(524288000)},
		{"IncompleteTTL", c.IncompleteTTL, 168 * time.Hour},
		{"CleanupInterval", c.CleanupInterval, time.Hour},
		{"GRPCMaxChunkBytes", c.GRPCMaxChunkBytes, 262144},
		{"GRPCAckEveryN", c.GRPCAckEveryN, 1},
		{"QUICIdleTimeout", c.QUICIdleTimeout, 60 * time.Second},
		{"QUICKeepAlive", c.QUICKeepAlive, 20 * time.Second},
		{"LogLevel", c.LogLevel, slog.LevelInfo},
		{"ShutdownTimeout", c.ShutdownTimeout, 30 * time.Second},
		{"S3.ForcePathStyle", c.S3.ForcePathStyle, false},
		{"RetentionMaxAge", c.RetentionMaxAge, 168 * time.Hour},
		{"DiskPath", c.DiskPath, "./data/objects"},
		{"DiskHighWatermark", c.DiskHighWatermark, 85},
		{"DiskLowWatermark", c.DiskLowWatermark, 70},
		{"DiskCheckInterval", c.DiskCheckInterval, time.Minute},
	}
	for _, ch := range checks {
		if ch.got != ch.want {
			t.Errorf("%s = %v, want %v", ch.name, ch.got, ch.want)
		}
	}
	if !slices.Equal(c.ContentTypes, DefaultContentTypes) {
		t.Errorf("ContentTypes = %v", c.ContentTypes)
	}
}

func TestLoadOverrides(t *testing.T) {
	c, err := Load(env(with(
		"LISTEN_ADDR", ":443",
		"GRPC_NATIVE_ADDR", ":9090",
		"OBJSTORE", "s3",
		"TUS_STAGING", "s3",
		"S3_BUCKET", "b",
		"S3_REGION", "us-east-1",
		"S3_ENDPOINT", "http://minio:9000",
		"S3_FORCE_PATH_STYLE", "true",
		"CONTENT_TYPES", "image/jpeg,,video/mp4",
		"MAX_FILE_BYTES", "1024",
		"INCOMPLETE_TTL", "1h",
		"GRPC_ACK_EVERY_N", "4",
	)))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !slices.Equal(c.ContentTypes, []string{"image/jpeg", "video/mp4"}) {
		t.Errorf("ContentTypes = %v", c.ContentTypes)
	}
	if !c.S3.ForcePathStyle || c.S3.Bucket != "b" || c.MaxFileBytes != 1024 || c.IncompleteTTL != time.Hour || c.GRPCAckEveryN != 4 {
		t.Errorf("overrides not applied: %+v", c)
	}
}

func TestExplicitLogLevel(t *testing.T) {
	c, err := Load(env(with("LOG_LEVEL", "warn")))
	if err != nil {
		t.Fatal(err)
	}
	if c.LogLevel != slog.LevelWarn {
		t.Errorf("LogLevel = %v, want warn", c.LogLevel)
	}
}

func TestLoadValidation(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{"missing TLS cert", without("TLS_CERT_FILE"), "TLS_CERT_FILE is required"},
		{"missing TLS key", without("TLS_KEY_FILE"), "TLS_KEY_FILE is required"},
		{"missing database", without("DATABASE_URL"), "DATABASE_URL is required"},
		{"bad duration", with("INCOMPLETE_TTL", "7days"), "INCOMPLETE_TTL"},
		{"negative duration", with("CLEANUP_INTERVAL", "-1s"), "CLEANUP_INTERVAL must be positive"},
		{"zero shutdown", with("SHUTDOWN_TIMEOUT", "0s"), "SHUTDOWN_TIMEOUT must be positive"},
		{"bad int", with("MAX_FILE_BYTES", "lots"), "MAX_FILE_BYTES"},
		{"zero max file", with("MAX_FILE_BYTES", "0"), "MAX_FILE_BYTES must be positive"},
		{"zero chunk", with("GRPC_MAX_CHUNK_BYTES", "0"), "GRPC_MAX_CHUNK_BYTES must be positive"},
		{"zero ack", with("GRPC_ACK_EVERY_N", "0"), "GRPC_ACK_EVERY_N must be positive"},
		{"bad path style", with("S3_FORCE_PATH_STYLE", "maybe"), "S3_FORCE_PATH_STYLE"},
		{"bad log level", with("LOG_LEVEL", "loud"), "LOG_LEVEL"},
		{"bad tus staging", with("TUS_STAGING", "memory"), "TUS_STAGING"},
		{"bad objstore", with("OBJSTORE", "gcs"), "OBJSTORE"},
		{"s3 without bucket", with("OBJSTORE", "s3", "S3_REGION", "r"), "S3_BUCKET is required"},
		{"s3 staging without region", with("TUS_STAGING", "s3", "S3_BUCKET", "b"), "S3_REGION is required"},
		{"bad listen addr", with("LISTEN_ADDR", "8443"), "LISTEN_ADDR"},
		{"bad metrics addr", with("METRICS_ADDR", "localhost"), "METRICS_ADDR"},
		{"bad native addr", with("GRPC_NATIVE_ADDR", "9090"), "GRPC_NATIVE_ADDR"},
		{"keepalive >= idle", with("QUIC_KEEPALIVE", "60s"), "QUIC_KEEPALIVE"},
		{"negative retention", with("RETENTION_MAX_AGE", "-1h"), "RETENTION_MAX_AGE must not be negative"},
		{"bad retention", with("RETENTION_MAX_AGE", "7d"), "RETENTION_MAX_AGE"},
		{"zero disk interval", with("DISK_CHECK_INTERVAL", "0s"), "DISK_CHECK_INTERVAL must be positive"},
		{"low >= high", with("DISK_LOW_WATERMARK", "85"), "DISK_LOW_WATERMARK (85) must be lower than DISK_HIGH_WATERMARK (85)"},
		{"high above 100", with("DISK_HIGH_WATERMARK", "101"), "DISK_HIGH_WATERMARK must be between 1 and 100"},
		{"low zero", with("DISK_LOW_WATERMARK", "0"), "DISK_LOW_WATERMARK must be between 1 and 100"},
		{"bad watermark", with("DISK_HIGH_WATERMARK", "85%"), "DISK_HIGH_WATERMARK"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(env(tt.env))
			if err == nil {
				t.Fatalf("expected error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestBlankValuesUseDefaults(t *testing.T) {
	c, err := Load(env(with("STAGING_DIR", " ", "CONTENT_TYPES", "", "OBJSTORE_LOCAL_DIR", "")))
	if err != nil {
		t.Fatal(err)
	}
	if c.StagingDir != "./data/staging" || c.ObjStoreLocalDir != "./data/objects" || len(c.ContentTypes) != len(DefaultContentTypes) {
		t.Errorf("blank values did not fall back to defaults: %+v", c)
	}
}

func TestLoadReportsAllErrors(t *testing.T) {
	_, err := Load(env(map[string]string{"INCOMPLETE_TTL": "x"}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"TLS_CERT_FILE", "TLS_KEY_FILE", "DATABASE_URL", "INCOMPLETE_TTL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %s: %v", want, err)
		}
	}
}

func TestRetentionAndDisk(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want string // DiskPath
	}{
		{"local objstore", with("OBJSTORE_LOCAL_DIR", "/srv/objects"), "/srv/objects"},
		{"s3 objstore", with("OBJSTORE", "s3", "S3_BUCKET", "b", "S3_REGION", "r", "STAGING_DIR", "/srv/staging"), "/srv/staging"},
		{"explicit", with("OBJSTORE", "s3", "S3_BUCKET", "b", "S3_REGION", "r", "DISK_PATH", "/minio-data"), "/minio-data"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := Load(env(tt.env))
			if err != nil {
				t.Fatal(err)
			}
			if c.DiskPath != tt.want {
				t.Errorf("DiskPath = %q, want %q", c.DiskPath, tt.want)
			}
		})
	}

	c, err := Load(env(with("RETENTION_MAX_AGE", "0", "DISK_HIGH_WATERMARK", "90", "DISK_LOW_WATERMARK", "50",
		"DISK_CHECK_INTERVAL", "15s")))
	if err != nil {
		t.Fatal(err)
	}
	if c.RetentionMaxAge != 0 || c.DiskHighWatermark != 90 || c.DiskLowWatermark != 50 || c.DiskCheckInterval != 15*time.Second {
		t.Errorf("overrides not applied: %+v", c)
	}
}

func without(keys ...string) map[string]string {
	m := maps.Clone(baseEnv())
	for _, k := range keys {
		delete(m, k)
	}
	return m
}
