package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/db"
)

// fakeStore is an in-memory keyStore that also verifies tokens.
type fakeStore struct {
	hashes  map[string][]byte
	created map[string]time.Time
	opened  int
}

func newFakeStore() *fakeStore {
	return &fakeStore{hashes: map[string][]byte{}, created: map[string]time.Time{}}
}

func (f *fakeStore) Set(_ context.Context, device string, hash []byte) error {
	f.hashes[device] = hash
	f.created[device] = time.Date(2026, 10, 22, 1, 2, 3, 0, time.UTC)
	return nil
}

func (f *fakeStore) Delete(_ context.Context, device string) (bool, error) {
	_, ok := f.hashes[device]
	delete(f.hashes, device)
	return ok, nil
}

func (f *fakeStore) List(context.Context) ([]db.DeviceKey, error) {
	var out []db.DeviceKey
	for d := range f.hashes {
		out = append(out, db.DeviceKey{DeviceID: d, CreatedAt: f.created[d]})
	}
	return out, nil
}

func (f *fakeStore) KeyHash(_ context.Context, device string) ([]byte, bool, error) {
	h, ok := f.hashes[device]
	return h, ok, nil
}

func (f *fakeStore) open(context.Context) (keyStore, func(), error) {
	f.opened++
	return f, func() {}, nil
}

func runCmd(t *testing.T, f *fakeStore, args ...string) (string, string, error) {
	t.Helper()
	var out, errOut bytes.Buffer
	err := run(t.Context(), args, &out, &errOut, f.open)
	return out.String(), errOut.String(), err
}

func TestSetPrintsVerifiableToken(t *testing.T) {
	f := newFakeStore()
	out, _, err := runCmd(t, f, "set", "-device", "netem-0001")
	if err != nil {
		t.Fatal(err)
	}
	tok := strings.TrimSpace(out)
	if strings.Count(out, "\n") != 1 {
		t.Fatalf("stdout must be the token only: %q", out)
	}
	dev, key, err := auth.ParseToken(tok)
	if err != nil || dev != "netem-0001" {
		t.Fatalf("token %q: %q %v", tok, dev, err)
	}
	v, _ := auth.NewVerifier(f, 0)
	if _, err := v.Verify(t.Context(), tok); err != nil {
		t.Fatalf("printed token does not verify: %v", err)
	}
	if bytes.Contains(f.hashes[dev], []byte(key)) {
		t.Fatal("store holds the key, not its hash")
	}

	// set again replaces the key: the first token stops working.
	out2, _, err := runCmd(t, f, "set", "-device", "netem-0001")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.Verify(t.Context(), tok); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("old token after set: %v", err)
	}
	if _, err := v.Verify(t.Context(), strings.TrimSpace(out2)); err != nil {
		t.Fatalf("new token: %v", err)
	}
}

func TestListAndDelete(t *testing.T) {
	f := newFakeStore()
	if _, _, err := runCmd(t, f, "set", "-device", "cam-01"); err != nil {
		t.Fatal(err)
	}
	out, _, err := runCmd(t, f, "list")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "cam-01") || !strings.Contains(out, "2026-10-22T01:02:03Z") {
		t.Fatalf("list: %q", out)
	}
	for _, h := range f.hashes {
		if strings.Contains(out, string(h)) {
			t.Fatal("list prints key material")
		}
	}
	if _, _, err := runCmd(t, f, "delete", "-device", "cam-01"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runCmd(t, f, "delete", "-device", "cam-01"); err == nil {
		t.Fatal("deleting a missing key must fail")
	}
}

func TestBadArguments(t *testing.T) {
	for name, args := range map[string][]string{
		"no command":      {},
		"unknown command": {"rotate"},
		"missing device":  {"set"},
		"invalid device":  {"set", "-device", "Bad_Device"},
		"short device":    {"delete", "-device", "ab"},
		"extra args":      {"list", "x"},
		"unknown flag":    {"list", "-device", "cam-01"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFakeStore()
			if _, _, err := runCmd(t, f, args...); err == nil {
				t.Fatal("want error")
			}
			if f.opened != 0 {
				t.Fatal("database opened for invalid arguments")
			}
		})
	}
}

func TestOpenDBRequiresURL(t *testing.T) {
	if _, _, err := openDB("")(t.Context()); err == nil {
		t.Fatal("empty DATABASE_URL accepted")
	}
}
