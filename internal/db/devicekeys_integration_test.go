//go:build integration

package db

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
)

func newDeviceKeys(t *testing.T) *DeviceKeys {
	t.Helper()
	ctx := testCtx(t)
	pool, err := Open(ctx, testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return NewDeviceKeys(pool)
}

func testDevice() string { return "dk-" + newRef()[24:] }

func TestDeviceKeysSetReplaceDelete(t *testing.T) {
	k := newDeviceKeys(t)
	ctx := testCtx(t)
	dev := testDevice()

	if _, found, err := k.KeyHash(ctx, dev); err != nil || found {
		t.Fatalf("unknown device: found=%v err=%v", found, err)
	}
	first, second := auth.HashKey("first"), auth.HashKey("second")
	if err := k.Set(ctx, dev, first); err != nil {
		t.Fatal(err)
	}
	h, found, err := k.KeyHash(ctx, dev)
	if err != nil || !found || !bytes.Equal(h, first) {
		t.Fatalf("after set: %x %v %v", h, found, err)
	}
	created := listedAt(t, k, dev)
	if created.IsZero() {
		t.Fatalf("%s not listed", dev)
	}

	if err := k.Set(ctx, dev, second); err != nil { // replace (UPSERT)
		t.Fatal(err)
	}
	if h, _, _ := k.KeyHash(ctx, dev); !bytes.Equal(h, second) {
		t.Fatalf("after replace: %x", h)
	}
	if at := listedAt(t, k, dev); at.Before(created) {
		t.Errorf("created_at went back: %s < %s", at, created)
	}

	if ok, err := k.Delete(ctx, dev); err != nil || !ok {
		t.Fatalf("delete: %v %v", ok, err)
	}
	if ok, err := k.Delete(ctx, dev); err != nil || ok {
		t.Fatalf("second delete: %v %v", ok, err)
	}
	if _, found, _ := k.KeyHash(ctx, dev); found {
		t.Fatal("key still found after delete")
	}
	if at := listedAt(t, k, dev); !at.IsZero() {
		t.Fatal("deleted device still listed")
	}
}

// listedAt returns dev's created_at from List (zero if absent) and checks
// that it is listed at most once.
func listedAt(t *testing.T, k *DeviceKeys, dev string) time.Time {
	t.Helper()
	list, err := k.List(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	var at time.Time
	n := 0
	for _, d := range list {
		if d.DeviceID == dev {
			at = d.CreatedAt
			n++
		}
	}
	if n > 1 {
		t.Fatalf("%d rows for %s, want 1", n, dev)
	}
	return at
}

// TestDeviceKeysWithVerifier runs auth.Verifier on the real table,
// including rotation and a closed pool (store failure -> ErrUnavailable).
func TestDeviceKeysWithVerifier(t *testing.T) {
	k := newDeviceKeys(t)
	ctx := testCtx(t)
	v, err := auth.NewVerifier(k, 0)
	if err != nil {
		t.Fatal(err)
	}
	dev := testDevice()
	oldKey, _ := auth.GenerateKey()
	newKey, _ := auth.GenerateKey()
	if err := k.Set(ctx, dev, auth.HashKey(oldKey)); err != nil {
		t.Fatal(err)
	}
	if got, err := v.Verify(ctx, auth.FormatToken(dev, oldKey)); err != nil || got != dev {
		t.Fatalf("verify: %q %v", got, err)
	}
	if _, err := v.Verify(ctx, auth.FormatToken("dk-unknown-x", oldKey)); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("unknown device: %v", err)
	}
	if err := k.Set(ctx, dev, auth.HashKey(newKey)); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Verify(ctx, auth.FormatToken(dev, oldKey)); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("old key after rotation: %v", err)
	}
	if _, err := v.Verify(ctx, auth.FormatToken(dev, newKey)); err != nil {
		t.Fatalf("new key: %v", err)
	}

	k.pool.Close()
	_, err = v.Verify(context.Background(), auth.FormatToken(dev, newKey))
	if !errors.Is(err, auth.ErrUnavailable) {
		t.Fatalf("closed pool: %v, want ErrUnavailable", err)
	}
}
