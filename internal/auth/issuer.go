package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/resyahrial/sensor-upload-server/internal/meta"
)

// IssuerConfig configures an Issuer.
type IssuerConfig struct {
	// Key signs tokens (ES256, P-256). Its kid is KeyID(&Key.PublicKey).
	Key *ecdsa.PrivateKey
	// Issuer is the iss claim.
	Issuer string
	// TTL is the access token lifetime.
	TTL time.Duration
	// Now overrides the clock (tests); defaults to time.Now.
	Now func() time.Time
}

// Issuer signs access tokens.
type Issuer struct {
	key    *ecdsa.PrivateKey
	kid    string
	issuer string
	ttl    time.Duration
	now    func() time.Time
}

// NewIssuer checks cfg and returns an Issuer.
func NewIssuer(cfg IssuerConfig) (*Issuer, error) {
	switch {
	case cfg.Key == nil:
		return nil, errors.New("auth: signing key is required")
	case cfg.Key.Curve != elliptic.P256():
		return nil, errors.New("auth: signing key must be P-256 (ES256)")
	case cfg.Issuer == "":
		return nil, errors.New("auth: issuer is required")
	case cfg.TTL <= 0:
		return nil, errors.New("auth: access token TTL must be positive")
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Issuer{key: cfg.Key, kid: KeyID(&cfg.Key.PublicKey), issuer: cfg.Issuer, ttl: cfg.TTL, now: now}, nil
}

// TTL returns the access token lifetime.
func (i *Issuer) TTL() time.Duration { return i.ttl }

// KeyID returns the kid of the signing key.
func (i *Issuer) KeyID() string { return i.kid }

// PublicKey returns the signing key's public half.
func (i *Issuer) PublicKey() *ecdsa.PublicKey { return &i.key.PublicKey }

// Issue signs an access token for device valid for TTL.
func (i *Issuer) Issue(device string) (token string, expiresAt time.Time, err error) {
	return i.IssueTTL(device, i.ttl)
}

// IssueTTL signs an access token for device valid for ttl (dev tooling
// mints long-lived tokens for experiments).
func (i *Issuer) IssueTTL(device string, ttl time.Duration) (string, time.Time, error) {
	if !meta.ValidDeviceID(device) {
		return "", time.Time{}, fmt.Errorf("auth: invalid device_id %q", device)
	}
	if ttl <= 0 {
		return "", time.Time{}, errors.New("auth: ttl must be positive")
	}
	jti, err := GenerateSecret()
	if err != nil {
		return "", time.Time{}, err
	}
	now := i.now().Truncate(time.Second)
	exp := now.Add(ttl)
	claims := Claims{RegisteredClaims: jwt.RegisteredClaims{
		Issuer:    i.issuer,
		Subject:   device,
		Audience:  jwt.ClaimStrings{Audience},
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(exp),
		ID:        jti[:32],
	}}
	t := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	t.Header["kid"] = i.kid
	t.Header["typ"] = "at+jwt"
	s, err := t.SignedString(i.key)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("auth: sign token: %w", err)
	}
	return s, exp, nil
}

// GenerateSigningKey returns a new P-256 key (tests and key generation).
func GenerateSigningKey() (*ecdsa.PrivateKey, error) {
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("auth: generate signing key: %w", err)
	}
	return k, nil
}
