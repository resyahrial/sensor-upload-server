// Package auth verifies device JWTs (FR-2) and, for development, issues them.
package auth

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Leeway is the allowed clock skew for exp/nbf/iat.
const Leeway = 120 * time.Second

// Supported algorithms.
const (
	ES256 = "ES256"
	HS256 = "HS256"
)

var (
	// ErrUnauthenticated means the token is missing or invalid (401 / UNAUTHENTICATED).
	ErrUnauthenticated = errors.New("unauthenticated")
	// ErrPermissionDenied means the token is valid but its device_id does not
	// match the request (403 / PERMISSION_DENIED).
	ErrPermissionDenied = errors.New("permission denied")
)

// Claims is the device token payload.
type Claims struct {
	DeviceID string `json:"device_id"`
	jwt.RegisteredClaims
}

// Config configures a Verifier. Enable HS256 only in dev mode; config
// validation enforces that.
type Config struct {
	// Algs lists the accepted algorithms (ES256 and/or HS256).
	Algs []string
	// ES256Key verifies ES256 tokens; required if Algs contains ES256.
	ES256Key *ecdsa.PublicKey
	// HMACSecret verifies HS256 tokens; required if Algs contains HS256.
	HMACSecret []byte
	// Now overrides the clock (tests); defaults to time.Now.
	Now func() time.Time
}

// Verifier validates bearer tokens and extracts the device_id.
type Verifier struct {
	es256  *ecdsa.PublicKey
	hmac   []byte
	parser *jwt.Parser
}

// NewVerifier checks cfg and returns a Verifier.
func NewVerifier(cfg Config) (*Verifier, error) {
	if len(cfg.Algs) == 0 {
		return nil, errors.New("auth: no algorithms enabled")
	}
	v := &Verifier{}
	for _, alg := range cfg.Algs {
		switch alg {
		case ES256:
			if cfg.ES256Key == nil {
				return nil, errors.New("auth: ES256 enabled without a public key")
			}
			v.es256 = cfg.ES256Key
		case HS256:
			if len(cfg.HMACSecret) == 0 {
				return nil, errors.New("auth: HS256 enabled without a secret")
			}
			v.hmac = cfg.HMACSecret
		default:
			return nil, fmt.Errorf("auth: unsupported algorithm %q", alg)
		}
	}
	opts := []jwt.ParserOption{
		jwt.WithValidMethods(cfg.Algs),
		jwt.WithLeeway(Leeway),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(), // reject iat in the future beyond the leeway
	}
	if cfg.Now != nil {
		opts = append(opts, jwt.WithTimeFunc(cfg.Now))
	}
	v.parser = jwt.NewParser(opts...)
	return v, nil
}

// Verify parses and validates token and returns its device_id. Every
// failure wraps ErrUnauthenticated; the detail is for logs, not clients.
func (v *Verifier) Verify(token string) (string, error) {
	var claims Claims
	if _, err := v.parser.ParseWithClaims(token, &claims, v.key); err != nil {
		return "", fmt.Errorf("%w: %w", ErrUnauthenticated, err)
	}
	if claims.DeviceID == "" {
		return "", fmt.Errorf("%w: missing device_id claim", ErrUnauthenticated)
	}
	return claims.DeviceID, nil
}

// VerifyHeader extracts the bearer token from an Authorization header
// value and verifies it.
func (v *Verifier) VerifyHeader(header string) (string, error) {
	token, err := BearerToken(header)
	if err != nil {
		return "", err
	}
	return v.Verify(token)
}

func (v *Verifier) key(t *jwt.Token) (any, error) {
	// WithValidMethods has already restricted the algorithm; map it to the key.
	switch t.Method.Alg() {
	case ES256:
		return v.es256, nil
	case HS256:
		return v.hmac, nil
	}
	return nil, fmt.Errorf("unexpected algorithm %q", t.Method.Alg())
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

// Sign issues a device token (dev tooling and tests). key is an
// *ecdsa.PrivateKey for ES256 or a []byte secret for HS256.
func Sign(alg string, key any, deviceID string, now time.Time, ttl time.Duration) (string, error) {
	var method jwt.SigningMethod
	switch alg {
	case ES256:
		method = jwt.SigningMethodES256
	case HS256:
		method = jwt.SigningMethodHS256
	default:
		return "", fmt.Errorf("auth: unsupported algorithm %q", alg)
	}
	if deviceID == "" {
		return "", errors.New("auth: device_id is required")
	}
	if ttl <= 0 {
		return "", errors.New("auth: ttl must be positive")
	}
	claims := Claims{
		DeviceID: deviceID,
		RegisteredClaims: jwt.RegisteredClaims{
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		},
	}
	s, err := jwt.NewWithClaims(method, claims).SignedString(key)
	if err != nil {
		return "", fmt.Errorf("auth: sign token: %w", err)
	}
	return s, nil
}

// ParseES256PublicKey parses a PEM-encoded ECDSA public key.
func ParseES256PublicKey(pem []byte) (*ecdsa.PublicKey, error) {
	k, err := jwt.ParseECPublicKeyFromPEM(pem)
	if err != nil {
		return nil, fmt.Errorf("auth: parse ES256 public key: %w", err)
	}
	return k, nil
}

// ParseES256PrivateKey parses a PEM-encoded ECDSA private key (SEC 1 or PKCS #8).
func ParseES256PrivateKey(pem []byte) (*ecdsa.PrivateKey, error) {
	k, err := jwt.ParseECPrivateKeyFromPEM(pem)
	if err != nil {
		return nil, fmt.Errorf("auth: parse ES256 private key: %w", err)
	}
	return k, nil
}
