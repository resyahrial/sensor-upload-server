package deviceapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/db"
)

// fakeStore is an in-memory Store with the same persistence rules as
// db.Devices.Update.
type fakeStore struct {
	mu       sync.Mutex
	devices  map[string]db.Device
	families map[string]db.RefreshFamily
	retired  map[string]db.TokenMatch // key: string(hash)
	retireAt map[string]time.Time
	fail     error // returned by every call when set
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		devices:  map[string]db.Device{},
		families: map[string]db.RefreshFamily{},
		retired:  map[string]db.TokenMatch{},
		retireAt: map[string]time.Time{},
	}
}

func (f *fakeStore) Update(_ context.Context, id string, fn func(s *db.DeviceState) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return f.fail
	}
	s := &db.DeviceState{DeviceID: id}
	if d, ok := f.devices[id]; ok {
		s.Device = &d
	}
	if fam, ok := f.families[id]; ok {
		s.Family = &fam
	}
	hadDevice, hadFamily := s.Device != nil, s.Family != nil
	if err := fn(s); err != nil {
		return err
	}
	switch {
	case s.Device == nil && hadDevice:
		return errors.New("fake: delete device")
	case s.Device != nil:
		f.devices[id] = *s.Device
	}
	if s.Family != nil {
		f.families[id] = *s.Family
	} else if hadFamily {
		delete(f.families, id)
	}
	for _, r := range s.Retire {
		k := string(r.Hash)
		if _, ok := f.retired[k]; !ok {
			f.retired[k] = db.TokenMatch{DeviceID: id, Retired: true, Reason: r.Reason}
			f.retireAt[k] = r.At
		}
	}
	return nil
}

func (f *fakeStore) FindRefreshHash(_ context.Context, hash []byte) (db.TokenMatch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return db.TokenMatch{}, f.fail
	}
	for id, fam := range f.families {
		if bytes.Equal(fam.CurrentHash, hash) || (fam.NextHash != nil && bytes.Equal(fam.NextHash, hash)) {
			return db.TokenMatch{DeviceID: id}, nil
		}
	}
	if m, ok := f.retired[string(hash)]; ok {
		return m, nil
	}
	return db.TokenMatch{}, db.ErrTokenNotFound
}

func (f *fakeStore) Get(_ context.Context, id string) (db.Device, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return db.Device{}, f.fail
	}
	d, ok := f.devices[id]
	if !ok {
		return db.Device{}, fmt.Errorf("get %q: %w", id, db.ErrDeviceNotFound)
	}
	return d, nil
}

func (f *fakeStore) PruneRetired(_ context.Context, before time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var n int64
	for k, at := range f.retireAt {
		if at.Before(before) {
			delete(f.retireAt, k)
			delete(f.retired, k)
			n++
		}
	}
	return n, nil
}

func (f *fakeStore) device(id string) db.Device {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.devices[id]
}

func (f *fakeStore) SetStatus(ctx context.Context, id string, status db.DeviceStatus, now time.Time) error {
	return f.Update(ctx, id, func(s *db.DeviceState) error {
		return db.ApplyStatus(s, status, now)
	})
}
