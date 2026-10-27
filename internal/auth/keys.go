package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// KeyID returns the kid of pub: the first 16 hex characters of SHA-256 of
// its PKIX DER encoding. Deriving it from the key means no kid to configure
// or keep in sync.
func KeyID(pub *ecdsa.PublicKey) string {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		// Only unsupported curves fail; NewIssuer and LoadVerifyKeys
		// accept P-256 only.
		panic(fmt.Sprintf("auth: marshal public key: %v", err))
	}
	sum := sha256.Sum256(der)
	return hex.EncodeToString(sum[:8])
}

// LoadSigningKey reads a PEM-encoded P-256 private key (SEC 1 or PKCS #8).
func LoadSigningKey(path string) (*ecdsa.PrivateKey, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("auth: read signing key: %w", err)
	}
	k, err := jwt.ParseECPrivateKeyFromPEM(b)
	if err != nil {
		return nil, fmt.Errorf("auth: parse signing key %s: %w", path, err)
	}
	if k.Curve != elliptic.P256() {
		return nil, fmt.Errorf("auth: signing key %s is not P-256", path)
	}
	return k, nil
}

// LoadVerifyKeys reads every *.pem file in dir as a PEM-encoded P-256
// public key and returns them by KeyID. An empty dir path returns an empty
// map. Use it to keep accepting tokens signed by a previous key.
func LoadVerifyKeys(dir string) (map[string]*ecdsa.PublicKey, error) {
	keys := map[string]*ecdsa.PublicKey{}
	if dir == "" {
		return keys, nil
	}
	paths, err := filepath.Glob(filepath.Join(dir, "*.pem"))
	if err != nil {
		return nil, fmt.Errorf("auth: list verify keys: %w", err)
	}
	sort.Strings(paths)
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("auth: read verify key: %w", err)
		}
		k, err := jwt.ParseECPublicKeyFromPEM(b)
		if err != nil {
			return nil, fmt.Errorf("auth: parse verify key %s: %w", p, err)
		}
		if k.Curve != elliptic.P256() {
			return nil, fmt.Errorf("auth: verify key %s is not P-256", p)
		}
		keys[KeyID(k)] = k
	}
	return keys, nil
}

// EncodePrivateKey returns key as PKCS #8 PEM.
func EncodePrivateKey(key *ecdsa.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("auth: marshal private key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

// EncodePublicKey returns pub as PKIX PEM.
func EncodePublicKey(pub *ecdsa.PublicKey) ([]byte, error) {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return nil, fmt.Errorf("auth: marshal public key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), nil
}

// DeviceFromToken returns the sub claim of token WITHOUT verifying it.
// Clients use it to default their device_id; servers must use Verify.
func DeviceFromToken(token string) (string, error) {
	var claims Claims
	if _, _, err := jwt.NewParser().ParseUnverified(token, &claims); err != nil {
		return "", fmt.Errorf("auth: parse token: %w", err)
	}
	if strings.TrimSpace(claims.Subject) == "" {
		return "", errors.New("auth: token has no sub")
	}
	return claims.Subject, nil
}
