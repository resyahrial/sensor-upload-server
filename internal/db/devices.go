package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Device errors.
var (
	// ErrDeviceNotFound means there is no devices row for the device id.
	ErrDeviceNotFound = errors.New("device not found")
	// ErrTokenNotFound means no refresh token (live or retired) has the hash.
	ErrTokenNotFound = errors.New("refresh token not found")
)

// DeviceStatus is the registration state of a device.
type DeviceStatus string

// Device states (migration 0004).
const (
	DevicePending  DeviceStatus = "pending"
	DeviceActive   DeviceStatus = "active"
	DeviceRejected DeviceStatus = "rejected"
	DeviceRevoked  DeviceStatus = "revoked"
)

// ValidDeviceStatus reports whether s is one of the Device* states.
func ValidDeviceStatus(s DeviceStatus) bool {
	switch s {
	case DevicePending, DeviceActive, DeviceRejected, DeviceRevoked:
		return true
	}
	return false
}

// RetireReason says why a refresh token hash stopped working.
type RetireReason string

// Retire reasons.
const (
	// RetireRotated: superseded by a refresh; presenting it again is reuse.
	RetireRotated RetireReason = "rotated"
	// RetireReset: its family was replaced or deleted.
	RetireReset RetireReason = "reset"
)

// Device is one row of the devices table.
type Device struct {
	DeviceID       string       `db:"device_id"`
	Status         DeviceStatus `db:"status"`
	PollSecretHash []byte       `db:"poll_secret_hash"` // nil once the refresh token was used
	Lat            *float64     `db:"lat"`
	Lon            *float64     `db:"lon"`
	Firmware       *string      `db:"firmware"`
	RegisteredAt   time.Time    `db:"registered_at"`
	LastSeenAt     time.Time    `db:"last_seen_at"`
	ApprovedAt     *time.Time   `db:"approved_at"`
}

// RefreshFamily is one row of the refresh_tokens table.
type RefreshFamily struct {
	CurrentHash []byte    `db:"current_hash"`
	NextHash    []byte    `db:"next_hash"` // nil = no next
	StartedAt   time.Time `db:"family_started_at"`
	RefreshedAt time.Time `db:"refreshed_at"`
	// Used is set once the device has refreshed with this family.
	Used bool `db:"used"`
}

// RetiredHash is a hash to record in refresh_tokens_retired.
type RetiredHash struct {
	Hash   []byte
	Reason RetireReason
	// At is the retirement time (PruneRetired compares it); zero = the
	// database's now().
	At time.Time
}

// DeviceState is the locked state of one device handed to an Update
// callback. The callback changes it in place; Update then persists it.
type DeviceState struct {
	// DeviceID is read-only.
	DeviceID string
	// Device is nil when there is no row; setting it creates the row.
	// Setting an existing Device to nil is an error (rows are never
	// deleted).
	Device *Device
	// Family is nil when there is none; setting it creates or replaces
	// the family, clearing it deletes the family.
	Family *RefreshFamily
	// Retire lists hashes to record as retired (starts empty).
	Retire []RetiredHash
}

// TokenMatch is the result of FindRefreshHash.
type TokenMatch struct {
	DeviceID string
	// Retired is true for a hash in refresh_tokens_retired, with Reason.
	Retired bool
	Reason  RetireReason
}

const deviceColumns = `device_id, status, poll_secret_hash, lat, lon, firmware,
		registered_at, last_seen_at, approved_at`

// Devices runs the device registration and refresh token queries.
type Devices struct {
	pool *pgxpool.Pool
}

// NewDevices returns a Devices using pool.
func NewDevices(pool *pgxpool.Pool) *Devices { return &Devices{pool: pool} }

// Get returns the device row, or ErrDeviceNotFound.
func (d *Devices) Get(ctx context.Context, id string) (Device, error) {
	rows, _ := d.pool.Query(ctx, `SELECT `+deviceColumns+` FROM devices WHERE device_id = $1`, id)
	dev, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[Device])
	if errors.Is(err, pgx.ErrNoRows) {
		return Device{}, fmt.Errorf("get device %q: %w", id, ErrDeviceNotFound)
	}
	if err != nil {
		return Device{}, fmt.Errorf("get device %q: %w", id, err)
	}
	return dev, nil
}

// List returns the devices with status (every device when status is ""),
// ordered by device_id.
func (d *Devices) List(ctx context.Context, status DeviceStatus) ([]Device, error) {
	rows, _ := d.pool.Query(ctx, `SELECT `+deviceColumns+` FROM devices
		WHERE $1 = '' OR status = $1 ORDER BY device_id`, string(status))
	devs, err := pgx.CollectRows(rows, pgx.RowToStructByName[Device])
	if err != nil {
		return nil, fmt.Errorf("list devices: %w", err)
	}
	return devs, nil
}

// FindRefreshHash returns the device whose live family has hash as its
// current or next token, else the retired entry for hash, else
// ErrTokenNotFound. It takes no locks; Update re-checks under lock.
func (d *Devices) FindRefreshHash(ctx context.Context, hash []byte) (TokenMatch, error) {
	var m TokenMatch
	err := d.pool.QueryRow(ctx,
		`SELECT device_id FROM refresh_tokens WHERE current_hash = $1 OR next_hash = $1`, hash).Scan(&m.DeviceID)
	if err == nil {
		return m, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return TokenMatch{}, fmt.Errorf("find refresh token: %w", err)
	}
	m.Retired = true
	err = d.pool.QueryRow(ctx,
		`SELECT device_id, reason FROM refresh_tokens_retired WHERE hash = $1`, hash).Scan(&m.DeviceID, &m.Reason)
	if errors.Is(err, pgx.ErrNoRows) {
		return TokenMatch{}, ErrTokenNotFound
	}
	if err != nil {
		return TokenMatch{}, fmt.Errorf("find retired refresh token: %w", err)
	}
	return m, nil
}

// PruneRetired deletes retired hashes retired before before.
func (d *Devices) PruneRetired(ctx context.Context, before time.Time) (int64, error) {
	tag, err := d.pool.Exec(ctx, `DELETE FROM refresh_tokens_retired WHERE retired_at < $1`, before)
	if err != nil {
		return 0, fmt.Errorf("prune retired refresh tokens: %w", err)
	}
	return tag.RowsAffected(), nil
}

// Update runs fn on the device's state inside a transaction and persists
// what fn changed. A transaction advisory lock on the device id (which
// serializes all changes of one device, including the first insert) is
// taken first, then the devices and refresh_tokens rows are locked with
// SELECT ... FOR UPDATE. If fn returns an error nothing is written
// and that error is returned unchanged.
func (d *Devices) Update(ctx context.Context, id string, fn func(s *DeviceState) error) (err error) {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin device update: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx)) // the original error matters
		}
	}()
	s, err := lockState(ctx, tx, id)
	if err != nil {
		return err
	}
	hadDevice, hadFamily := s.Device != nil, s.Family != nil
	if err := fn(s); err != nil {
		return err
	}
	if err := persistState(ctx, tx, s, hadDevice, hadFamily); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit device update: %w", err)
	}
	return nil
}

func lockState(ctx context.Context, tx pgx.Tx, id string) (*DeviceState, error) {
	s := &DeviceState{DeviceID: id}
	// A row lock cannot cover a devices row that does not exist yet, so two
	// first registrations would race on the insert; the advisory lock
	// (released at commit/rollback) serializes them.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, id); err != nil {
		return nil, fmt.Errorf("lock device id: %w", err)
	}
	rows, _ := tx.Query(ctx, `SELECT `+deviceColumns+` FROM devices WHERE device_id = $1 FOR UPDATE`, id)
	dev, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[Device])
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return s, nil // no family without a device (foreign key)
	case err != nil:
		return nil, fmt.Errorf("lock device: %w", err)
	}
	s.Device = &dev
	rows, _ = tx.Query(ctx, `SELECT current_hash, next_hash, family_started_at, refreshed_at, used
		FROM refresh_tokens WHERE device_id = $1 FOR UPDATE`, id)
	fam, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[RefreshFamily])
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return nil, fmt.Errorf("lock refresh family: %w", err)
	default:
		s.Family = &fam
	}
	return s, nil
}

func persistState(ctx context.Context, tx pgx.Tx, s *DeviceState, hadDevice, hadFamily bool) error {
	dev := s.Device
	switch {
	case dev == nil && hadDevice:
		return fmt.Errorf("device %q: devices rows cannot be deleted", s.DeviceID)
	case dev != nil && !hadDevice:
		if _, err := tx.Exec(ctx, `
			INSERT INTO devices (device_id, status, poll_secret_hash, lat, lon, firmware,
			                     registered_at, last_seen_at, approved_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			s.DeviceID, string(dev.Status), dev.PollSecretHash, dev.Lat, dev.Lon, dev.Firmware,
			dev.RegisteredAt, dev.LastSeenAt, dev.ApprovedAt); err != nil {
			return fmt.Errorf("insert device: %w", err)
		}
	case dev != nil:
		if _, err := tx.Exec(ctx, `
			UPDATE devices SET status = $2, poll_secret_hash = $3, lat = $4, lon = $5, firmware = $6,
			       registered_at = $7, last_seen_at = $8, approved_at = $9, updated_at = now()
			WHERE device_id = $1`,
			s.DeviceID, string(dev.Status), dev.PollSecretHash, dev.Lat, dev.Lon, dev.Firmware,
			dev.RegisteredAt, dev.LastSeenAt, dev.ApprovedAt); err != nil {
			return fmt.Errorf("update device: %w", err)
		}
	}
	if f := s.Family; f != nil {
		if dev == nil {
			return fmt.Errorf("device %q: refresh family without device", s.DeviceID)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO refresh_tokens (device_id, current_hash, next_hash, family_started_at, refreshed_at, used)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (device_id) DO UPDATE SET current_hash = EXCLUDED.current_hash,
			    next_hash = EXCLUDED.next_hash, family_started_at = EXCLUDED.family_started_at,
			    refreshed_at = EXCLUDED.refreshed_at, used = EXCLUDED.used`,
			s.DeviceID, f.CurrentHash, f.NextHash, f.StartedAt, f.RefreshedAt, f.Used); err != nil {
			return fmt.Errorf("save refresh family: %w", err)
		}
	} else if hadFamily {
		if _, err := tx.Exec(ctx, `DELETE FROM refresh_tokens WHERE device_id = $1`, s.DeviceID); err != nil {
			return fmt.Errorf("delete refresh family: %w", err)
		}
	}
	for _, r := range s.Retire {
		if _, err := tx.Exec(ctx, `
			INSERT INTO refresh_tokens_retired (hash, device_id, reason, retired_at)
			VALUES ($1, $2, $3, coalesce($4, now()))
			ON CONFLICT (hash) DO NOTHING`, r.Hash, s.DeviceID, string(r.Reason), nullTime(r.At)); err != nil {
			return fmt.Errorf("retire refresh token: %w", err)
		}
	}
	return nil
}

func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// RetireFamily appends the family's hashes to s.Retire (with reason, at
// now) and clears s.Family.
func (s *DeviceState) RetireFamily(reason RetireReason, now time.Time) {
	if s.Family == nil {
		return
	}
	s.Retire = append(s.Retire, RetiredHash{Hash: s.Family.CurrentHash, Reason: reason, At: now})
	if s.Family.NextHash != nil {
		s.Retire = append(s.Retire, RetiredHash{Hash: s.Family.NextHash, Reason: reason, At: now})
	}
	s.Family = nil
}

// SetStatus sets an existing device's status (devicectl approve, reject,
// revoke). Approving sets approved_at to now; rejecting and revoking delete
// the refresh family (its hashes are retired as reset). It returns
// ErrDeviceNotFound for a device without a row.
func (d *Devices) SetStatus(ctx context.Context, id string, status DeviceStatus, now time.Time) error {
	if !ValidDeviceStatus(status) {
		return fmt.Errorf("invalid device status %q", status)
	}
	return d.Update(ctx, id, func(s *DeviceState) error {
		return ApplyStatus(s, status, now)
	})
}

// ApplyStatus is SetStatus's change to a locked state (exported so test
// fakes apply the same rules).
func ApplyStatus(s *DeviceState, status DeviceStatus, now time.Time) error {
	if s.Device == nil {
		return fmt.Errorf("device %q: %w", s.DeviceID, ErrDeviceNotFound)
	}
	s.Device.Status = status
	switch status {
	case DeviceActive:
		t := now
		s.Device.ApprovedAt = &t
	case DeviceRejected, DeviceRevoked:
		s.RetireFamily(RetireReset, now)
	}
	return nil
}
