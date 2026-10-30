//go:build integration

package db

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"testing"
	"time"
)

func newDevices(t *testing.T) *Devices {
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
	return NewDevices(pool)
}

func randHash(t *testing.T) []byte {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func newDeviceID(t *testing.T) string {
	t.Helper()
	return "dbdev-" + hex.EncodeToString(randHash(t)[:6])
}

func TestDevicesUpdateLifecycle(t *testing.T) {
	d := newDevices(t)
	ctx := testCtx(t)
	id := newDeviceID(t)
	now := time.Now().UTC().Truncate(time.Microsecond)

	// Unknown device: empty state, nothing written by a no-op callback.
	if err := d.Update(ctx, id, func(s *DeviceState) error {
		if s.Device != nil || s.Family != nil {
			t.Fatalf("state = %+v", s)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Get(ctx, id); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("Get before register = %v", err)
	}

	poll := randHash(t)
	lat, fw := -6.25, "1.0"
	if err := d.Update(ctx, id, func(s *DeviceState) error {
		s.Device = &Device{Status: DevicePending, PollSecretHash: poll, Lat: &lat, Firmware: &fw, RegisteredAt: now, LastSeenAt: now}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got, err := d.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.DeviceID != id || got.Status != DevicePending || !bytes.Equal(got.PollSecretHash, poll) ||
		*got.Lat != lat || got.Lon != nil || *got.Firmware != fw || !got.RegisteredAt.Equal(now) || got.ApprovedAt != nil {
		t.Fatalf("Get = %+v", got)
	}

	// A callback error rolls back.
	boom := errors.New("boom")
	if err := d.Update(ctx, id, func(s *DeviceState) error {
		s.Device.Status = DeviceRejected
		return boom
	}); !errors.Is(err, boom) {
		t.Fatalf("Update = %v, want boom", err)
	}
	if got, _ := d.Get(ctx, id); got.Status != DevicePending {
		t.Fatal("failed Update was not rolled back")
	}

	// Approve, add a family, rotate it.
	if err := d.SetStatus(ctx, id, DeviceActive, now); err != nil {
		t.Fatal(err)
	}
	cur, next := randHash(t), randHash(t)
	if err := d.Update(ctx, id, func(s *DeviceState) error {
		s.Family = &RefreshFamily{CurrentHash: cur, StartedAt: now, RefreshedAt: now}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	m, err := d.FindRefreshHash(ctx, cur)
	if err != nil || m.DeviceID != id || m.Retired {
		t.Fatalf("FindRefreshHash(current) = %+v, %v", m, err)
	}
	later := now.Add(time.Hour)
	if err := d.Update(ctx, id, func(s *DeviceState) error {
		if s.Device.Status != DeviceActive || s.Device.ApprovedAt == nil || s.Family == nil || !bytes.Equal(s.Family.CurrentHash, cur) {
			t.Fatalf("locked state = %+v %+v", s.Device, s.Family)
		}
		s.Family.NextHash = next
		s.Family.Used = true
		s.Family.RefreshedAt = later
		s.Device.PollSecretHash = nil
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if m, err := d.FindRefreshHash(ctx, next); err != nil || m.DeviceID != id {
		t.Fatalf("FindRefreshHash(next) = %+v, %v", m, err)
	}
	if got, _ := d.Get(ctx, id); got.PollSecretHash != nil {
		t.Fatal("poll secret not cleared")
	}
	if err := d.Update(ctx, id, func(s *DeviceState) error {
		f := s.Family
		if !f.Used || f.NextHash == nil || !f.RefreshedAt.Equal(later) || !f.StartedAt.Equal(now) {
			t.Fatalf("family = %+v", f)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Revoke: family deleted, hashes retired as reset.
	if err := d.SetStatus(ctx, id, DeviceRevoked, later); err != nil {
		t.Fatal(err)
	}
	for _, h := range [][]byte{cur, next} {
		m, err := d.FindRefreshHash(ctx, h)
		if err != nil || !m.Retired || m.Reason != RetireReset || m.DeviceID != id {
			t.Fatalf("FindRefreshHash after revoke = %+v, %v", m, err)
		}
	}
	if _, err := d.FindRefreshHash(ctx, randHash(t)); !errors.Is(err, ErrTokenNotFound) {
		t.Fatalf("unknown hash = %v", err)
	}
	if err := d.SetStatus(ctx, newDeviceID(t), DeviceActive, now); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("SetStatus unknown = %v", err)
	}
	if err := d.SetStatus(ctx, id, "bogus", now); err == nil {
		t.Fatal("SetStatus accepted a bogus status")
	}
	// Deleting the row: a family left set is an error; once retired, the
	// row and family go and the retired hashes stay.
	var famHash []byte
	if err := d.Update(ctx, id, func(s *DeviceState) error {
		s.Family = &RefreshFamily{CurrentHash: randHash(t), StartedAt: now, RefreshedAt: now}
		famHash = s.Family.CurrentHash
		return nil
	}); err != nil {
		t.Fatalf("Update family: %v", err)
	}
	if err := d.Update(ctx, id, func(s *DeviceState) error { s.Device = nil; return nil }); err == nil {
		t.Fatal("Update deleted a device that kept its family")
	}
	if err := d.Update(ctx, id, func(s *DeviceState) error {
		s.RetireFamily(RetireReset, now)
		s.Device = nil
		return nil
	}); err != nil {
		t.Fatalf("Update delete: %v", err)
	}
	if _, err := d.Get(ctx, id); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("Get after delete = %v", err)
	}
	if m, err := d.FindRefreshHash(ctx, famHash); err != nil || !m.Retired || m.Reason != RetireReset {
		t.Fatalf("FindRefreshHash after delete = %+v, %v; want retired reset", m, err)
	}
}

func TestDevicesListAndPrune(t *testing.T) {
	d := newDevices(t)
	ctx := testCtx(t)
	now := time.Now().UTC()
	pending, active := newDeviceID(t), newDeviceID(t)
	old, recent := randHash(t), randHash(t)
	for _, id := range []string{pending, active} {
		if err := d.Update(ctx, id, func(s *DeviceState) error {
			s.Device = &Device{Status: DevicePending, RegisteredAt: now, LastSeenAt: now}
			if id == active {
				s.Device.Status = DeviceActive
				s.Retire = []RetiredHash{
					{Hash: old, Reason: RetireRotated, At: now.Add(-48 * time.Hour)},
					{Hash: recent, Reason: RetireRotated},
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	list, err := d.List(ctx, DeviceActive)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, dev := range list {
		if dev.Status != DeviceActive {
			t.Fatalf("List(active) returned %+v", dev)
		}
		found = found || dev.DeviceID == active
	}
	if !found {
		t.Fatal("List(active) misses the active device")
	}
	all, err := d.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, dev := range all {
		seen[dev.DeviceID] = true
	}
	if !seen[pending] || !seen[active] {
		t.Fatal("List(\"\") misses devices")
	}

	n, err := d.PruneRetired(ctx, now.Add(-24*time.Hour))
	if err != nil || n < 1 {
		t.Fatalf("PruneRetired = %d, %v", n, err)
	}
	if _, err := d.FindRefreshHash(ctx, old); !errors.Is(err, ErrTokenNotFound) {
		t.Fatalf("old hash after prune = %v", err)
	}
	if m, err := d.FindRefreshHash(ctx, recent); err != nil || !m.Retired || m.Reason != RetireRotated {
		t.Fatalf("recent hash after prune = %+v, %v", m, err)
	}
}

// TestDevicesUpdateSerializes: concurrent Updates of one device run one at
// a time (the advisory lock), so read-modify-write never loses an update,
// even when they race to create the row.
func TestDevicesUpdateSerializes(t *testing.T) {
	d := newDevices(t)
	ctx := testCtx(t)
	id := newDeviceID(t)
	const n = 10
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for range n {
		wg.Go(func() {
			errs <- d.Update(ctx, id, func(s *DeviceState) error {
				now := time.Now()
				if s.Device == nil {
					fw := ""
					s.Device = &Device{Status: DevicePending, Firmware: &fw, RegisteredAt: now, LastSeenAt: now}
				}
				fw := *s.Device.Firmware + "x"
				s.Device.Firmware = &fw
				return nil
			})
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	got, err := d.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(*got.Firmware) != n {
		t.Fatalf("firmware = %q, want %d x (lost update)", *got.Firmware, n)
	}
}

func TestDevicesClosedPool(t *testing.T) {
	d := newDevices(t)
	d.pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := d.Update(ctx, "dbdev-closed", func(*DeviceState) error { return nil }); err == nil {
		t.Fatal("Update on closed pool succeeded")
	}
	if _, err := d.FindRefreshHash(ctx, []byte("x")); err == nil || errors.Is(err, ErrTokenNotFound) {
		t.Fatalf("FindRefreshHash on closed pool = %v", err)
	}
}
