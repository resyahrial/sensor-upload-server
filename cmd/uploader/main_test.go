package main

import (
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/auth/authtest"
)

func TestParseFlags(t *testing.T) {
	t.Setenv("UPLOADER_TOKEN", "")
	o, err := parseFlags([]string{"-protocol", "grpc-native", "-url", "https://h:9090", "-token", "t",
		"-file", "a.flac", "-device", "bioac-0142", "-priority", "1", "-chunk", "65536", "-retries", "3",
		"-backoff-max", "30s", "-report", "out.csv", "-concurrency", "4", "-state", "s.db", "-h3-fallback", "-grpc-window", "4"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	c := o.cfg
	if c.Protocol != "grpc-native" || c.URL != "https://h:9090" || c.Device != "bioac-0142" || c.Priority != 1 ||
		c.Chunk != 65536 || c.Retries != 3 || c.BackoffMax != 30*time.Second || c.Concurrency != 4 || !c.H3Fallback || c.GRPCWindow != 4 ||
		o.report != "out.csv" || o.state != "s.db" || o.file != "a.flac" {
		t.Fatalf("parsed %+v", o)
	}

	if o.authURL != "https://h:9090" {
		t.Fatalf("-auth-url default = %q", o.authURL)
	}

	tok := authtest.New(t).Token(t, "netem-0001")
	d, err := parseFlags([]string{"-dir", "x", "-token", tok}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if d.cfg.BackoffMax != 5*time.Minute || d.cfg.H3Fallback || d.cfg.Protocol != "rest" || d.cfg.GRPCWindow != 1 {
		t.Fatalf("defaults %+v", d.cfg)
	}
	if d.cfg.Device != "netem-0001" {
		t.Fatalf("device from token sub: %q", d.cfg.Device)
	}
	if o, err := parseFlags([]string{"-file", "a", "-token", tok, "-device", "x-1"}, io.Discard); err != nil || o.cfg.Device != "x-1" {
		t.Fatalf("explicit -device: %q %v", o.cfg.Device, err)
	}
	t.Setenv("UPLOADER_TOKEN", tok)
	if o, err := parseFlags([]string{"-file", "a"}, io.Discard); err != nil || o.cfg.Token != tok || o.cfg.Device != "netem-0001" {
		t.Fatalf("$UPLOADER_TOKEN: %q %v", o.cfg.Device, err)
	}
	// -refresh-token-file wins over $UPLOADER_TOKEN; the device comes from the file later.
	r, err := parseFlags([]string{"-file", "a", "-refresh-token-file", "t.json", "-auth-url", "https://api:8443"}, io.Discard)
	if err != nil || r.cfg.Token != "" || r.refreshFile != "t.json" || r.authURL != "https://api:8443" || r.cfg.Device != "" {
		t.Fatalf("refresh mode: %+v %v", r, err)
	}
	t.Setenv("UPLOADER_TOKEN", "")

	for _, bad := range [][]string{
		{"-token", tok}, {"-file", "a", "-dir", "b", "-token", tok}, {"-file", "a", "extra", "-token", tok}, {"-nope"},
		{"-file", "a", "-grpc-window", "0", "-token", tok},
		{"-file", "a"}, // no credential
		{"-file", "a", "-token", tok, "-refresh-token-file", "t.json"},
		{"-file", "a", "-token", "not-a-jwt"}, // device not derivable
	} {
		if _, err := parseFlags(bad, io.Discard); err == nil {
			t.Errorf("parseFlags(%v): want error", bad)
		}
	}
}

func TestListFiles(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{"b.jpg", "a.flac"} {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	got, err := listFiles(options{dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{filepath.Join(dir, "a.flac"), filepath.Join(dir, "b.jpg")}; !slices.Equal(got, want) {
		t.Fatalf("listFiles = %v", got)
	}
	if _, err := listFiles(options{dir: t.TempDir()}); err == nil {
		t.Fatal("empty dir: want error")
	}
}

func TestTLSConfig(t *testing.T) {
	if _, err := tlsConfig(filepath.Join(t.TempDir(), "missing.pem"), false); err == nil {
		t.Fatal("missing CA: want error")
	}
	bad := filepath.Join(t.TempDir(), "bad.pem")
	if err := os.WriteFile(bad, []byte("nope"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := tlsConfig(bad, false); err == nil {
		t.Fatal("bad CA: want error")
	}
	c, err := tlsConfig("", true)
	if err != nil || !c.InsecureSkipVerify {
		t.Fatalf("insecure: %v %v", c, err)
	}
}

func TestRunReportsConnectionFailure(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "a.flac")
	if err := os.WriteFile(f, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	report := filepath.Join(dir, "out.csv")
	err := run([]string{"-url", "https://127.0.0.1:1", "-token", authtest.New(t).Token(t, "dev-1"), "-file", f,
		"-retries", "0", "-backoff-max", "1ms", "-state", filepath.Join(dir, "s.db"), "-report", report}, io.Discard)
	if err == nil {
		t.Fatal("want error for unreachable server")
	}
	b, rerr := os.ReadFile(report)
	if rerr != nil || len(b) == 0 {
		t.Fatalf("report not written: %v", rerr)
	}
}

func TestRunRefreshTokenFile(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "a.flac")
	if err := os.WriteFile(f, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	tokens := filepath.Join(dir, "tokens.json")
	pair := `{"device_id":"dev-1","access_token":"` + authtest.New(t).Token(t, "dev-1") +
		`","token_type":"Bearer","expires_in":3600,"refresh_token":"` + strings.Repeat("ab", 32) + `"}`
	if err := os.WriteFile(tokens, []byte(pair), 0o600); err != nil {
		t.Fatal(err)
	}
	args := []string{"-url", "https://127.0.0.1:1", "-refresh-token-file", tokens, "-file", f,
		"-retries", "0", "-backoff-max", "1ms", "-state", filepath.Join(dir, "s.db")}
	if err := run(args, io.Discard); err == nil || !strings.Contains(err.Error(), "not uploaded") {
		t.Fatalf("err = %v, want upload failure (unreachable server)", err)
	}
	if err := os.WriteFile(tokens, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run(args, io.Discard); err == nil || !strings.Contains(err.Error(), "refresh_token") {
		t.Fatalf("bad file: err = %v", err)
	}
}
