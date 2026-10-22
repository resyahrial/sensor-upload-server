package auth

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"
)

var testKey = strings.Repeat("0123456789abcdef", 4)

func TestParseToken(t *testing.T) {
	tests := []struct {
		name, token, device, key string
	}{
		{"valid", "bioac-0142_" + testKey, "bioac-0142", testKey},
		{"empty", "", "", ""},
		{"no underscore", "bioac-0142" + testKey, "", ""},
		{"empty device", "_" + testKey, "", ""},
		{"empty key", "bioac-0142_", "", ""},
		{"split at last underscore", "bio_ac_" + testKey, "", ""}, // device "bio_ac" is invalid
		{"key with underscore", "bioac-0142_" + testKey[:32] + "_" + testKey[33:], "", ""},
		{"invalid device", "Bioac-0142_" + testKey, "", ""},
		{"device too short", "ab_" + testKey, "", ""},
		{"uppercase hex", "bioac-0142_" + strings.ToUpper(testKey), "", ""},
		{"non-hex key", "bioac-0142_" + strings.Repeat("g", KeyHexLen), "", ""},
		{"key too short", "bioac-0142_" + testKey[1:], "", ""},
		{"key too long", "bioac-0142_" + testKey + "0", "", ""},
		{"jwt", "eyJhbGciOiJIUzI1NiJ9.eyJkZXZpY2VfaWQiOiJ4In0.sig", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dev, key, err := ParseToken(tt.token)
			if tt.device == "" {
				if !errors.Is(err, ErrUnauthenticated) {
					t.Fatalf("ParseToken(%q) = %q, %q, %v; want ErrUnauthenticated", tt.token, dev, key, err)
				}
				return
			}
			if err != nil || dev != tt.device || key != tt.key {
				t.Fatalf("ParseToken = %q, %q, %v", dev, key, err)
			}
		})
	}
}

func TestParseTokenErrorOmitsKey(t *testing.T) {
	_, _, err := ParseToken("bioac-0142_" + strings.ToUpper(testKey))
	if err == nil || strings.Contains(err.Error(), strings.ToUpper(testKey)) {
		t.Fatalf("error %v leaks the key", err)
	}
}

func TestGenerateKey(t *testing.T) {
	a, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	b, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	if !ValidKey(a) || !ValidKey(b) || a == b {
		t.Fatalf("keys %q, %q", a, b)
	}
	if strings.Contains(a, "_") {
		t.Fatal("key contains _")
	}
	dev, key, err := ParseToken(FormatToken("cam-01", a))
	if err != nil || dev != "cam-01" || key != a {
		t.Fatalf("round trip: %q %q %v", dev, key, err)
	}
}

func TestHashKey(t *testing.T) {
	want := sha256.Sum256([]byte(testKey))
	if string(HashKey(testKey)) != string(want[:]) {
		t.Fatal("HashKey is not SHA-256")
	}
}

// slowStore blocks until the lookup context is done.
type slowStore struct{}

func (slowStore) KeyHash(ctx context.Context, _ string) ([]byte, bool, error) {
	<-ctx.Done()
	return nil, false, ctx.Err()
}

func TestVerifier(t *testing.T) {
	store := NewMemoryStore()
	store.Set("bioac-0142", testKey)
	v, err := NewVerifier(store, 0)
	if err != nil {
		t.Fatal(err)
	}
	otherKey := strings.Repeat("f", KeyHexLen)
	tests := []struct {
		name, header, device string
		wantErr              error
	}{
		{"valid", "Bearer bioac-0142_" + testKey, "bioac-0142", nil},
		{"lowercase scheme", "bearer bioac-0142_" + testKey, "bioac-0142", nil},
		{"wrong key", "Bearer bioac-0142_" + otherKey, "", ErrUnauthenticated},
		{"unknown device", "Bearer cam-0001_" + testKey, "", ErrUnauthenticated},
		{"missing header", "", "", ErrUnauthenticated},
		{"basic scheme", "Basic Zm9vOmJhcg==", "", ErrUnauthenticated},
		{"empty token", "Bearer  ", "", ErrUnauthenticated},
		{"malformed", "Bearer not-a-token", "", ErrUnauthenticated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dev, err := v.VerifyHeader(t.Context(), tt.header)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) || dev != "" {
					t.Fatalf("= %q, %v; want %v", dev, err, tt.wantErr)
				}
				if strings.Contains(err.Error(), testKey) || strings.Contains(err.Error(), otherKey) {
					t.Fatalf("error %v leaks the key", err)
				}
				return
			}
			if err != nil || dev != tt.device {
				t.Fatalf("= %q, %v", dev, err)
			}
		})
	}
}

func TestVerifierRotationAndDelete(t *testing.T) {
	store := NewMemoryStore()
	v, err := NewVerifier(store, 0)
	if err != nil {
		t.Fatal(err)
	}
	old, err := store.Issue("cam-01")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.Verify(t.Context(), old); err != nil {
		t.Fatalf("first key: %v", err)
	}
	if again, _ := store.Token("cam-01"); again != old {
		t.Fatal("Token re-issued an existing key")
	}
	rotated, err := store.Issue("cam-01")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.Verify(t.Context(), old); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("old key after rotation: %v", err)
	}
	if dev, err := v.Verify(t.Context(), rotated); err != nil || dev != "cam-01" {
		t.Fatalf("new key: %q %v", dev, err)
	}
	store.Delete("cam-01")
	if _, err := v.Verify(t.Context(), rotated); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("deleted key: %v", err)
	}
}

func TestVerifierStoreFailure(t *testing.T) {
	store := NewMemoryStore()
	tok, err := store.Issue("cam-01")
	if err != nil {
		t.Fatal(err)
	}
	store.SetErr(errors.New("connection refused"))
	v, err := NewVerifier(store, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = v.Verify(t.Context(), tok)
	if !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("store down: %v, want ErrUnavailable only", err)
	}
	// A malformed token never reaches the store.
	if _, err := v.Verify(t.Context(), "junk"); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("malformed with store down: %v", err)
	}
}

func TestVerifierLookupTimeout(t *testing.T) {
	v, err := NewVerifier(slowStore{}, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = v.Verify(t.Context(), "cam-01_"+testKey)
	if !errors.Is(err, ErrUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("slow store: %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("lookup took %s", d)
	}
}

func TestNewVerifierRequiresStore(t *testing.T) {
	if _, err := NewVerifier(nil, 0); err == nil {
		t.Fatal("nil store accepted")
	}
}

func TestCheckDevice(t *testing.T) {
	if err := CheckDevice("a", "a"); err != nil {
		t.Fatal(err)
	}
	if err := CheckDevice("a", "b"); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("mismatch: %v", err)
	}
}
