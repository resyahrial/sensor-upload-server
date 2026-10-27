// Package auth issues and verifies device access tokens (FR-2) and the
// opaque secrets used for refresh tokens and registration poll secrets (see
// docs/decisions.md "Decisions (JWT + refresh)").
//
// An access token is an ES256 JWT with sub = device_id, aud = Audience, the
// configured iss, iat and exp, and a kid header naming the signing key
// (KeyID). Verification is stateless: no database lookup per request.
package auth

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/resyahrial/sensor-upload-server/internal/meta"
)

// Audience is the aud claim of every access token.
const Audience = "sensor-upload"

// Leeway is the allowed clock skew for exp, nbf and iat.
const Leeway = 120 * time.Second

var (
	// ErrUnauthenticated means the token is missing, malformed, wrongly
	// signed or for another audience (401 invalid_token / UNAUTHENTICATED).
	ErrUnauthenticated = errors.New("unauthenticated")
	// ErrTokenExpired means the token is otherwise valid but past exp
	// (401 token_expired); the device refreshes and retries. It is not
	// wrapped in ErrUnauthenticated so callers can tell the two apart.
	ErrTokenExpired = errors.New("token expired")
	// ErrPermissionDenied means the token is valid but its device_id does
	// not match the request (403 device_not_active / PERMISSION_DENIED).
	ErrPermissionDenied = errors.New("permission denied")
)

// Claims is the access token payload; Subject is the device_id.
type Claims struct {
	jwt.RegisteredClaims
}

// VerifierConfig configures a Verifier.
type VerifierConfig struct {
	// Issuer is the required iss claim.
	Issuer string
	// Keys are the accepted public keys by kid (see KeyID). During a
	// signing-key rollover it holds both the old and the new key.
	Keys map[string]*ecdsa.PublicKey
	// Now overrides the clock (tests); defaults to time.Now.
	Now func() time.Time
}

// Verifier validates access tokens and extracts the device_id.
type Verifier struct {
	keys   map[string]*ecdsa.PublicKey
	parser *jwt.Parser
}

// NewVerifier checks cfg and returns a Verifier.
func NewVerifier(cfg VerifierConfig) (*Verifier, error) {
	if cfg.Issuer == "" {
		return nil, errors.New("auth: issuer is required")
	}
	if len(cfg.Keys) == 0 {
		return nil, errors.New("auth: at least one verification key is required")
	}
	keys := make(map[string]*ecdsa.PublicKey, len(cfg.Keys))
	for kid, k := range cfg.Keys {
		if k == nil {
			return nil, fmt.Errorf("auth: verification key %q is nil", kid)
		}
		keys[kid] = k
	}
	opts := []jwt.ParserOption{
		jwt.WithValidMethods([]string{jwt.SigningMethodES256.Alg()}),
		jwt.WithLeeway(Leeway),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(), // reject iat in the future beyond the leeway
		jwt.WithIssuer(cfg.Issuer),
		jwt.WithAudience(Audience),
	}
	if cfg.Now != nil {
		opts = append(opts, jwt.WithTimeFunc(cfg.Now))
	}
	return &Verifier{keys: keys, parser: jwt.NewParser(opts...)}, nil
}

// Verify parses and validates token and returns its device_id. Failures
// wrap ErrTokenExpired or ErrUnauthenticated; the detail is for logs, not
// clients, and never contains the token.
func (v *Verifier) Verify(token string) (string, error) {
	var claims Claims
	if _, err := v.parser.ParseWithClaims(token, &claims, v.key); err != nil {
		// Only an expired but correctly signed token is token_expired: the
		// parser checks the signature before the claims.
		if errors.Is(err, jwt.ErrTokenExpired) && !errors.Is(err, jwt.ErrTokenSignatureInvalid) {
			return "", fmt.Errorf("%w: %w", ErrTokenExpired, err)
		}
		return "", fmt.Errorf("%w: %w", ErrUnauthenticated, err)
	}
	if !meta.ValidDeviceID(claims.Subject) {
		return "", fmt.Errorf("%w: sub is not a valid device_id", ErrUnauthenticated)
	}
	return claims.Subject, nil
}

// VerifyHeader extracts the bearer token from an Authorization header
// value and verifies it. ctx is unused today; it keeps the signature stable
// for a future revocation check.
func (v *Verifier) VerifyHeader(_ context.Context, header string) (string, error) {
	token, err := BearerToken(header)
	if err != nil {
		return "", err
	}
	return v.Verify(token)
}

func (v *Verifier) key(t *jwt.Token) (any, error) {
	kid, _ := t.Header["kid"].(string)
	k, ok := v.keys[kid]
	if !ok {
		return nil, fmt.Errorf("unknown kid %q", kid)
	}
	return k, nil
}

// BearerToken returns the token from an "Authorization: Bearer <token>"
// header value. The scheme is case-insensitive.
func BearerToken(header string) (string, error) {
	scheme, token, ok := strings.Cut(strings.TrimSpace(header), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", fmt.Errorf("%w: missing bearer token", ErrUnauthenticated)
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return "", fmt.Errorf("%w: empty bearer token", ErrUnauthenticated)
	}
	return token, nil
}

// CheckDevice returns ErrPermissionDenied unless the token's device ID
// equals the device ID from the upload metadata.
func CheckDevice(tokenDevice, metaDevice string) error {
	if tokenDevice != metaDevice {
		return fmt.Errorf("%w: token device_id does not match metadata", ErrPermissionDenied)
	}
	return nil
}
