package auth

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func newPair(t *testing.T, ttl time.Duration, now func() time.Time) (*ecdsa.PrivateKey, *Issuer, *Verifier) {
	t.Helper()
	key, err := GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	iss, err := NewIssuer(IssuerConfig{Key: key, Issuer: "test", TTL: ttl, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	v, err := NewVerifier(VerifierConfig{Issuer: "test", Keys: map[string]*ecdsa.PublicKey{iss.KeyID(): iss.PublicKey()}})
	if err != nil {
		t.Fatal(err)
	}
	return key, iss, v
}

func TestIssueVerify(t *testing.T) {
	_, iss, v := newPair(t, time.Hour, nil)
	tok, exp, err := iss.Issue("dev-0001")
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Until(exp); d < 59*time.Minute || d > time.Hour {
		t.Fatalf("exp in %s, want ~1h", d)
	}
	got, err := v.VerifyHeader(context.Background(), "bearer "+tok)
	if err != nil || got != "dev-0001" {
		t.Fatalf("VerifyHeader = %q, %v", got, err)
	}
	if d, err := DeviceFromToken(tok); err != nil || d != "dev-0001" {
		t.Fatalf("DeviceFromToken = %q, %v", d, err)
	}
}

func TestExpired(t *testing.T) {
	past := func() time.Time { return time.Now().Add(-time.Hour) }
	_, iss, v := newPair(t, time.Minute, past)
	tok, _, err := iss.Issue("dev-0001")
	if err != nil {
		t.Fatal(err)
	}
	_, err = v.Verify(tok)
	if !errors.Is(err, ErrTokenExpired) || errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("err = %v, want ErrTokenExpired only", err)
	}
}

func TestExpiredWithinLeeway(t *testing.T) {
	past := func() time.Time { return time.Now().Add(-2 * time.Minute) }
	_, iss, v := newPair(t, time.Minute, past) // expired 60 s ago < Leeway
	tok, _, _ := iss.Issue("dev-0001")
	if _, err := v.Verify(tok); err != nil {
		t.Fatalf("within leeway: %v", err)
	}
}

func TestRejected(t *testing.T) {
	key, iss, v := newPair(t, time.Hour, nil)
	_, other, _ := newPair(t, time.Hour, nil)
	good, _, _ := iss.Issue("dev-0001")

	sign := func(method jwt.SigningMethod, k any, claims jwt.MapClaims, kid string) string {
		tk := jwt.NewWithClaims(method, claims)
		if kid != "" {
			tk.Header["kid"] = kid
		}
		s, err := tk.SignedString(k)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	now := time.Now()
	base := func() jwt.MapClaims {
		return jwt.MapClaims{"iss": "test", "aud": Audience, "sub": "dev-0001",
			"iat": now.Unix(), "exp": now.Add(time.Hour).Unix()}
	}
	with := func(k string, val any) jwt.MapClaims { c := base(); c[k] = val; return c }
	without := func(k string) jwt.MapClaims { c := base(); delete(c, k); return c }
	pubPEM, _ := EncodePublicKey(&key.PublicKey)

	otherTok, _, _ := other.Issue("dev-0001")
	parts := strings.Split(good, ".")
	cases := map[string]string{
		"garbage":         "not-a-jwt",
		"other key":       otherTok,
		"tampered":        parts[0] + "." + parts[1] + "x." + parts[2],
		"unknown kid":     sign(jwt.SigningMethodES256, key, base(), "nope"),
		"no kid":          sign(jwt.SigningMethodES256, key, base(), ""),
		"wrong aud":       sign(jwt.SigningMethodES256, key, with("aud", "other"), iss.KeyID()),
		"wrong iss":       sign(jwt.SigningMethodES256, key, with("iss", "other"), iss.KeyID()),
		"no exp":          sign(jwt.SigningMethodES256, key, without("exp"), iss.KeyID()),
		"iat in future":   sign(jwt.SigningMethodES256, key, with("iat", now.Add(time.Hour).Unix()), iss.KeyID()),
		"bad sub":         sign(jwt.SigningMethodES256, key, with("sub", "Bad Device"), iss.KeyID()),
		"alg none":        sign(jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, base(), iss.KeyID()),
		"hs256 confusion": sign(jwt.SigningMethodHS256, pubPEM, base(), iss.KeyID()),
	}
	for name, tok := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := v.Verify(tok); !errors.Is(err, ErrUnauthenticated) {
				t.Fatalf("err = %v, want ErrUnauthenticated", err)
			}
		})
	}
	for _, h := range []string{"", "Basic abc", "Bearer ", "Bearer"} {
		if _, err := v.VerifyHeader(context.Background(), h); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("header %q: err = %v", h, err)
		}
	}
}

func TestKeyRollover(t *testing.T) {
	oldKey, _ := GenerateSigningKey()
	newKey, _ := GenerateSigningKey()
	dir := t.TempDir()
	pemOld, _ := EncodePublicKey(&oldKey.PublicKey)
	if err := os.WriteFile(filepath.Join(dir, "old.pem"), pemOld, 0o600); err != nil {
		t.Fatal(err)
	}
	keys, err := LoadVerifyKeys(dir)
	if err != nil {
		t.Fatal(err)
	}
	newIss, _ := NewIssuer(IssuerConfig{Key: newKey, Issuer: "test", TTL: time.Hour})
	oldIss, _ := NewIssuer(IssuerConfig{Key: oldKey, Issuer: "test", TTL: time.Hour})
	keys[newIss.KeyID()] = newIss.PublicKey()
	v, err := NewVerifier(VerifierConfig{Issuer: "test", Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	for _, iss := range []*Issuer{oldIss, newIss} {
		tok, _, _ := iss.Issue("dev-0001")
		if _, err := v.Verify(tok); err != nil {
			t.Fatalf("kid %s: %v", iss.KeyID(), err)
		}
	}
}

func TestLoadSigningKey(t *testing.T) {
	key, _ := GenerateSigningKey()
	b, err := EncodePrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "jwt.key")
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := LoadSigningKey(p)
	if err != nil || !got.Equal(key) {
		t.Fatalf("LoadSigningKey: %v", err)
	}
	if _, err := LoadSigningKey(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("missing file: want error")
	}
}

func TestConstructorsValidate(t *testing.T) {
	key, _ := GenerateSigningKey()
	bad := []IssuerConfig{{}, {Key: key, TTL: time.Hour}, {Key: key, Issuer: "x"}}
	for i, c := range bad {
		if _, err := NewIssuer(c); err == nil {
			t.Errorf("NewIssuer case %d: want error", i)
		}
	}
	if _, err := NewVerifier(VerifierConfig{Issuer: "x"}); err == nil {
		t.Error("NewVerifier without keys: want error")
	}
	if _, err := NewVerifier(VerifierConfig{Keys: map[string]*ecdsa.PublicKey{"a": &key.PublicKey}}); err == nil {
		t.Error("NewVerifier without issuer: want error")
	}
	iss, _ := NewIssuer(IssuerConfig{Key: key, Issuer: "x", TTL: time.Hour})
	if _, _, err := iss.Issue("Bad Device"); err == nil {
		t.Error("Issue invalid device: want error")
	}
}

func TestSecret(t *testing.T) {
	s, err := GenerateSecret()
	if err != nil || !ValidSecret(s) {
		t.Fatalf("GenerateSecret = %q, %v", s, err)
	}
	if len(HashSecret(s)) != 32 {
		t.Fatal("HashSecret length")
	}
	for _, bad := range []string{"", strings.Repeat("A", 64), strings.Repeat("a", 63), strings.Repeat("g", 64)} {
		if ValidSecret(bad) {
			t.Errorf("ValidSecret(%q) = true", bad)
		}
	}
}

func TestCheckDevice(t *testing.T) {
	if err := CheckDevice("a", "a"); err != nil {
		t.Fatal(err)
	}
	if err := CheckDevice("a", "b"); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("err = %v", err)
	}
}
