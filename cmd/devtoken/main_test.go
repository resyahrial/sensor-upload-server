package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
)

func noenv(string) string { return "" }

func TestRunES256TokenVerifies(t *testing.T) {
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalECPrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	keyFile := filepath.Join(t.TempDir(), "jwt.key")
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	now := time.Now()
	if err := run([]string{"-device", "bioac-0142", "-key", keyFile, "-ttl", "1h"}, &out, noenv, now); err != nil {
		t.Fatal(err)
	}
	v, err := auth.NewVerifier(auth.Config{Algs: []string{auth.ES256}, ES256Key: &k.PublicKey})
	if err != nil {
		t.Fatal(err)
	}
	if dev, err := v.Verify(strings.TrimSpace(out.String())); err != nil || dev != "bioac-0142" {
		t.Fatalf("Verify = %q, %v", dev, err)
	}
}

func TestRunHS256FromEnv(t *testing.T) {
	var out bytes.Buffer
	env := func(k string) string {
		if k == "JWT_HMAC_SECRET" {
			return "s3cret"
		}
		return ""
	}
	if err := run([]string{"-alg", "HS256", "-device", "cam-01"}, &out, env, time.Now()); err != nil {
		t.Fatal(err)
	}
	v, err := auth.NewVerifier(auth.Config{Algs: []string{auth.HS256}, HMACSecret: []byte("s3cret")})
	if err != nil {
		t.Fatal(err)
	}
	if dev, err := v.Verify(strings.TrimSpace(out.String())); err != nil || dev != "cam-01" {
		t.Fatalf("Verify = %q, %v", dev, err)
	}
}

func TestRunErrors(t *testing.T) {
	for name, args := range map[string][]string{
		"no device":    {},
		"bad alg":      {"-device", "d", "-alg", "none"},
		"no secret":    {"-device", "d", "-alg", "HS256"},
		"missing key":  {"-device", "d", "-key", filepath.Join(t.TempDir(), "nope")},
		"bad flag":     {"-bogus"},
		"negative ttl": {"-device", "d", "-alg", "HS256", "-secret", "s", "-ttl", "-1h"},
	} {
		if err := run(args, &bytes.Buffer{}, noenv, time.Now()); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
