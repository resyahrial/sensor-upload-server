-- +goose Up
-- One active key per device (docs/decisions.md "Decisions (device keys)").
-- key_hash is SHA-256 of the 64-hex-char random key; the key itself is never
-- stored. The primary key is the index the per-request lookup uses.
CREATE TABLE device_keys (
    device_id  TEXT PRIMARY KEY,
    key_hash   BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE device_keys;
