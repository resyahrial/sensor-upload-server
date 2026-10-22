package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	now    = time.Date(2026, 10, 21, 5, 30, 0, 0, time.UTC)
	secret = []byte("dev-secret")
)

func mustECKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func newVerifier(t *testing.T, pub *ecdsa.PublicKey, algs ...string) *Verifier {
	t.Helper()
	v, err := NewVerifier(Config{Algs: algs, ES256Key: pub, HMACSecret: secret, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func signClaims(t *testing.T, method jwt.SigningMethod, key any, c jwt.Claims) string {
	t.Helper()
	s, err := jwt.NewWithClaims(method, c).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func claims(device string, exp time.Time) Claims {
	return Claims{DeviceID: device, RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(exp)}}
}

func TestVerify(t *testing.T) {
	es := mustECKey(t)
	other := mustECKey(t)
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&es.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})
	both := newVerifier(t, &es.PublicKey, ES256, HS256)
	esOnly := newVerifier(t, &es.PublicKey, ES256)

	valid, err := Sign(ES256, es, "bioac-0142", now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	validHS, err := Sign(HS256, secret, "bioac-0142", now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	none := signClaims(t, jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, claims("bioac-0142", now.Add(time.Hour)))

	tests := []struct {
		name    string
		v       *Verifier
		token   string
		wantDev string
	}{
		{"valid ES256", esOnly, valid, "bioac-0142"},
		{"valid HS256", both, validHS, "bioac-0142"},
		{"expired within leeway", esOnly, signClaims(t, jwt.SigningMethodES256, es, claims("d-1", now.Add(-time.Minute))), "d-1"},
		{"expired", esOnly, signClaims(t, jwt.SigningMethodES256, es, claims("d-1", now.Add(-3*time.Minute))), ""},
		{"nbf in future", esOnly, signClaims(t, jwt.SigningMethodES256, es, Claims{DeviceID: "d-1", RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)), NotBefore: jwt.NewNumericDate(now.Add(5 * time.Minute))}}), ""},
		{"nbf within leeway", esOnly, signClaims(t, jwt.SigningMethodES256, es, Claims{DeviceID: "d-1", RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)), NotBefore: jwt.NewNumericDate(now.Add(time.Minute))}}), "d-1"},
		{"iat in future", esOnly, signClaims(t, jwt.SigningMethodES256, es, Claims{DeviceID: "d-1", RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)), IssuedAt: jwt.NewNumericDate(now.Add(10 * time.Minute))}}), ""},
		{"missing exp", esOnly, signClaims(t, jwt.SigningMethodES256, es, Claims{DeviceID: "d-1"}), ""},
		{"missing device_id", esOnly, signClaims(t, jwt.SigningMethodES256, es, claims("", now.Add(time.Hour))), ""},
		{"alg none", both, none, ""},
		{"HS256 when only ES256 enabled", esOnly, validHS, ""},
		{"RS256", both, signClaims(t, jwt.SigningMethodRS256, rsaKey, claims("d-1", now.Add(time.Hour))), ""},
		{"ES384", both, signClaims(t, jwt.SigningMethodES384, mustP384(t), claims("d-1", now.Add(time.Hour))), ""},
		{"wrong ES256 key", esOnly, signClaims(t, jwt.SigningMethodES256, other, claims("d-1", now.Add(time.Hour))), ""},
		{"wrong HMAC secret", both, signClaims(t, jwt.SigningMethodHS256, []byte("other"), claims("d-1", now.Add(time.Hour))), ""},
		{"HS256 signed with ES256 public key (alg confusion)", both, signClaims(t, jwt.SigningMethodHS256, pubPEM, claims("d-1", now.Add(time.Hour))), ""},
		{"malformed", both, "not.a.jwt", ""},
		{"empty", both, "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dev, err := tt.v.Verify(tt.token)
			if tt.wantDev != "" {
				if err != nil || dev != tt.wantDev {
					t.Fatalf("Verify = %q, %v; want %q", dev, err, tt.wantDev)
				}
				return
			}
			if !errors.Is(err, ErrUnauthenticated) {
				t.Fatalf("Verify = %q, %v; want ErrUnauthenticated", dev, err)
			}
		})
	}
}

func mustP384(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestVerifyHeader(t *testing.T) {
	v := newVerifier(t, nil, HS256)
	tok, err := Sign(HS256, secret, "cam-01", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range []string{"Bearer " + tok, "bearer " + tok, "  Bearer   " + tok + " "} {
		if dev, err := v.VerifyHeader(h); err != nil || dev != "cam-01" {
			t.Errorf("VerifyHeader(%q) = %q, %v", h, dev, err)
		}
	}
	for _, h := range []string{"", "Bearer", "Bearer ", "Basic " + tok, tok} {
		if _, err := v.VerifyHeader(h); !errors.Is(err, ErrUnauthenticated) {
			t.Errorf("VerifyHeader(%q) = %v, want ErrUnauthenticated", h, err)
		}
	}
}

func TestCheckDevice(t *testing.T) {
	if err := CheckDevice("cam-01", "cam-01"); err != nil {
		t.Errorf("same device: %v", err)
	}
	if err := CheckDevice("cam-01", "cam-02"); !errors.Is(err, ErrPermissionDenied) {
		t.Errorf("different device: %v", err)
	}
}

func TestNewVerifierErrors(t *testing.T) {
	for name, cfg := range map[string]Config{
		"no algs":         {},
		"ES256 no key":    {Algs: []string{ES256}},
		"HS256 no secret": {Algs: []string{HS256}},
		"unknown alg":     {Algs: []string{"none"}, HMACSecret: secret},
	} {
		if _, err := NewVerifier(cfg); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestSignErrors(t *testing.T) {
	es := mustECKey(t)
	if _, err := Sign("RS256", es, "d", now, time.Hour); err == nil {
		t.Error("unsupported alg should fail")
	}
	if _, err := Sign(ES256, es, "", now, time.Hour); err == nil {
		t.Error("empty device should fail")
	}
	if _, err := Sign(ES256, es, "d", now, 0); err == nil {
		t.Error("zero ttl should fail")
	}
	if _, err := Sign(ES256, secret, "d", now, time.Hour); err == nil {
		t.Error("wrong key type should fail")
	}
}

func TestParseKeys(t *testing.T) {
	es := mustECKey(t)
	privDER, err := x509.MarshalECPrivateKey(es)
	if err != nil {
		t.Fatal(err)
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&es.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	priv, err := ParseES256PrivateKey(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER}))
	if err != nil {
		t.Fatal(err)
	}
	pub, err := ParseES256PublicKey(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}))
	if err != nil {
		t.Fatal(err)
	}
	tok, err := Sign(ES256, priv, "cam-01", now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if dev, err := newVerifier(t, pub, ES256).Verify(tok); err != nil || dev != "cam-01" {
		t.Errorf("round trip = %q, %v", dev, err)
	}
	if _, err := ParseES256PublicKey([]byte("junk")); err == nil {
		t.Error("junk public key should fail")
	}
	if _, err := ParseES256PrivateKey([]byte("junk")); err == nil {
		t.Error("junk private key should fail")
	}
}
