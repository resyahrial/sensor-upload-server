package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// SecretBytes is the number of random bytes in a refresh token or poll
// secret; the value is their lowercase hex encoding (SecretHexLen
// characters).
const (
	SecretBytes  = 32
	SecretHexLen = 2 * SecretBytes
)

// GenerateSecret returns a new random secret (SecretBytes from
// crypto/rand, lowercase hex). Refresh tokens are such secrets.
func GenerateSecret() (string, error) {
	b := make([]byte, SecretBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("auth: generate secret: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// HashSecret returns SHA-256(secret), the value stored instead of the
// secret. A plain hash suffices: secrets are 256-bit random values, so
// there is nothing to brute-force.
func HashSecret(secret string) []byte {
	h := sha256.Sum256([]byte(secret))
	return h[:]
}

// ValidSecret reports whether s is SecretHexLen lowercase hex characters.
func ValidSecret(s string) bool {
	if len(s) != SecretHexLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
