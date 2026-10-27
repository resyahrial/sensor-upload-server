// Package authtest provides a throwaway signing key with a matching
// Issuer and Verifier for tests.
package authtest

import (
	"crypto/ecdsa"
	"testing"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
)

// Issuer is the iss claim used by Keys.
const Issuer = "authtest"

// Keys holds a fresh signing key, an Issuer (TTL 1 h) and a Verifier that
// accepts it.
type Keys struct {
	Key      *ecdsa.PrivateKey
	Issuer   *auth.Issuer
	Verifier *auth.Verifier
}

// New returns Keys with a freshly generated key.
func New(tb testing.TB) *Keys {
	tb.Helper()
	key, err := auth.GenerateSigningKey()
	if err != nil {
		tb.Fatal(err)
	}
	iss, err := auth.NewIssuer(auth.IssuerConfig{Key: key, Issuer: Issuer, TTL: time.Hour})
	if err != nil {
		tb.Fatal(err)
	}
	v, err := auth.NewVerifier(auth.VerifierConfig{
		Issuer: Issuer,
		Keys:   map[string]*ecdsa.PublicKey{iss.KeyID(): iss.PublicKey()},
	})
	if err != nil {
		tb.Fatal(err)
	}
	return &Keys{Key: key, Issuer: iss, Verifier: v}
}

// Token returns a valid access token for device.
func (k *Keys) Token(tb testing.TB, device string) string {
	tb.Helper()
	tok, _, err := k.Issuer.Issue(device)
	if err != nil {
		tb.Fatal(err)
	}
	return tok
}

// Bearer returns "Bearer " + Token(device).
func (k *Keys) Bearer(tb testing.TB, device string) string {
	tb.Helper()
	return "Bearer " + k.Token(tb, device)
}

// Expired returns a correctly signed access token for device that expired
// an hour ago (well beyond auth.Leeway).
func (k *Keys) Expired(tb testing.TB, device string) string {
	tb.Helper()
	past, err := auth.NewIssuer(auth.IssuerConfig{
		Key: k.Key, Issuer: Issuer, TTL: time.Minute,
		Now: func() time.Time { return time.Now().Add(-time.Hour) },
	})
	if err != nil {
		tb.Fatal(err)
	}
	tok, _, err := past.Issue(device)
	if err != nil {
		tb.Fatal(err)
	}
	return tok
}
