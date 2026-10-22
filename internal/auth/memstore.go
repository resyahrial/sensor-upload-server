package auth

import (
	"context"
	"sync"
)

// MemoryStore is an in-memory KeyStore for tests and in-process tooling.
type MemoryStore struct {
	mu     sync.Mutex
	hashes map[string][]byte
	tokens map[string]string // tokens issued by Token, by device
	err    error
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{hashes: map[string][]byte{}, tokens: map[string]string{}}
}

// KeyHash implements KeyStore.
func (m *MemoryStore) KeyHash(ctx context.Context, device string) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, false, m.err
	}
	h, ok := m.hashes[device]
	return h, ok, nil
}

// Set stores key as device's only key (like an UPSERT on device_keys).
func (m *MemoryStore) Set(device, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hashes[device] = HashKey(key)
	delete(m.tokens, device)
}

// Delete removes device's key.
func (m *MemoryStore) Delete(device string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.hashes, device)
	delete(m.tokens, device)
}

// Issue generates a new key for device, replacing any previous one, and
// returns its bearer token.
func (m *MemoryStore) Issue(device string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.issueLocked(device)
}

// Token returns the token last issued for device by Token or Issue, issuing
// one if there is none, so repeated calls for one device stay valid.
func (m *MemoryStore) Token(device string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if tok, ok := m.tokens[device]; ok {
		return tok, nil
	}
	return m.issueLocked(device)
}

func (m *MemoryStore) issueLocked(device string) (string, error) {
	key, err := GenerateKey()
	if err != nil {
		return "", err
	}
	m.hashes[device] = HashKey(key)
	tok := FormatToken(device, key)
	m.tokens[device] = tok
	return tok, nil
}

// SetErr makes every lookup fail with err (nil restores normal lookups).
func (m *MemoryStore) SetErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}
