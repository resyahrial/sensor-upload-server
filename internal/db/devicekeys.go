package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DeviceKey is one row of device_keys without the hash.
type DeviceKey struct {
	DeviceID  string
	CreatedAt time.Time
}

// DeviceKeys stores the SHA-256 hash of each device's single active key
// (table device_keys). It implements auth.KeyStore.
type DeviceKeys struct {
	pool *pgxpool.Pool
}

// NewDeviceKeys returns a DeviceKeys on pool.
func NewDeviceKeys(pool *pgxpool.Pool) *DeviceKeys { return &DeviceKeys{pool: pool} }

// KeyHash returns the stored key hash of device; found is false when the
// device has no key.
func (k *DeviceKeys) KeyHash(ctx context.Context, device string) ([]byte, bool, error) {
	var hash []byte
	err := k.pool.QueryRow(ctx, `SELECT key_hash FROM device_keys WHERE device_id = $1`, device).Scan(&hash)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, false, nil
	case err != nil:
		return nil, false, fmt.Errorf("select device key: %w", err)
	}
	return hash, true, nil
}

// Set makes hash the device's only key, replacing any previous one
// immediately (UPSERT; created_at is reset).
func (k *DeviceKeys) Set(ctx context.Context, device string, hash []byte) error {
	_, err := k.pool.Exec(ctx, `INSERT INTO device_keys (device_id, key_hash) VALUES ($1, $2)
		ON CONFLICT (device_id) DO UPDATE SET key_hash = EXCLUDED.key_hash, created_at = now()`, device, hash)
	if err != nil {
		return fmt.Errorf("upsert device key: %w", err)
	}
	return nil
}

// Delete removes the device's key and reports whether there was one.
func (k *DeviceKeys) Delete(ctx context.Context, device string) (bool, error) {
	tag, err := k.pool.Exec(ctx, `DELETE FROM device_keys WHERE device_id = $1`, device)
	if err != nil {
		return false, fmt.Errorf("delete device key: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// List returns all devices that have a key, ordered by device_id.
func (k *DeviceKeys) List(ctx context.Context) ([]DeviceKey, error) {
	rows, err := k.pool.Query(ctx, `SELECT device_id, created_at FROM device_keys ORDER BY device_id`)
	if err != nil {
		return nil, fmt.Errorf("list device keys: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (DeviceKey, error) {
		var d DeviceKey
		err := r.Scan(&d.DeviceID, &d.CreatedAt)
		return d, err
	})
	if err != nil {
		return nil, fmt.Errorf("list device keys: %w", err)
	}
	return out, nil
}
