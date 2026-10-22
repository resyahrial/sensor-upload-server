package main

import (
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestParseFlags(t *testing.T) {
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

	d, err := parseFlags([]string{"-dir", "x"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if d.cfg.BackoffMax != 5*time.Minute || d.cfg.H3Fallback || d.cfg.Protocol != "rest" || d.cfg.GRPCWindow != 1 {
		t.Fatalf("defaults %+v", d.cfg)
	}

	tok := "netem-0001_" + strings.Repeat("ab", 32)
	if o, err := parseFlags([]string{"-file", "a", "-token", tok}, io.Discard); err != nil || o.cfg.Device != "netem-0001" {
		t.Fatalf("device from token: %q %v", o.cfg.Device, err)
	}
	if o, err := parseFlags([]string{"-file", "a", "-token", tok, "-device", "x-1"}, io.Discard); err != nil || o.cfg.Device != "x-1" {
		t.Fatalf("explicit -device: %q %v", o.cfg.Device, err)
	}

	for _, bad := range [][]string{{},{"-file", "a", "-dir", "b"}, {"-file", "a", "extra"}, {"-nope"}, {"-file", "a", "-grpc-window", "0"}} {
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
	err := run([]string{"-url", "https://127.0.0.1:1", "-device", "dev-1", "-token", "t", "-file", f,
		"-retries", "0", "-backoff-max", "1ms", "-state", filepath.Join(dir, "s.db"), "-report", report}, io.Discard)
	if err == nil {
		t.Fatal("want error for unreachable server")
	}
	b, rerr := os.ReadFile(report)
	if rerr != nil || len(b) == 0 {
		t.Fatalf("report not written: %v", rerr)
	}
}
