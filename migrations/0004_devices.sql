-- +goose Up
-- Device registration and refresh tokens (docs/decisions.md "Decisions
-- (JWT + refresh)"). Secrets (poll secrets, refresh tokens) are 64-hex-char
-- random values stored as SHA-256 only.

-- Pre-imported allow-list: only these device ids may register.
CREATE TABLE device_batch (
    device_id   TEXT PRIMARY KEY,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per registered device. poll_secret_hash is cleared once the
-- device has used its refresh token (the poll secret then has no purpose).
CREATE TABLE devices (
    device_id        TEXT PRIMARY KEY REFERENCES device_batch (device_id),
    status           TEXT NOT NULL CHECK (status IN ('pending', 'active', 'rejected', 'revoked')),
    poll_secret_hash BYTEA NULL,
    lat              DOUBLE PRECISION NULL,
    lon              DOUBLE PRECISION NULL,
    firmware         TEXT NULL,
    registered_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    approved_at      TIMESTAMPTZ NULL,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX devices_status ON devices (status);

-- One refresh token family per device. current_hash always works (so a lost
-- refresh response can be retried); next_hash is the token issued by the
-- last refresh, which replaces current once used.
CREATE TABLE refresh_tokens (
    device_id         TEXT PRIMARY KEY REFERENCES devices (device_id) ON DELETE CASCADE,
    current_hash      BYTEA NOT NULL UNIQUE,
    next_hash         BYTEA NULL UNIQUE,
    family_started_at TIMESTAMPTZ NOT NULL,
    refreshed_at      TIMESTAMPTZ NOT NULL,
    used              BOOLEAN NOT NULL DEFAULT false
);

-- Hashes that no longer work, for reuse detection. reason 'rotated' means
-- the token was superseded by a refresh (presenting it again is a security
-- event); 'reset' means its family was replaced or deleted (register,
-- revoke, reject, devicectl issue).
CREATE TABLE refresh_tokens_retired (
    hash       BYTEA PRIMARY KEY,
    device_id  TEXT NOT NULL,
    reason     TEXT NOT NULL CHECK (reason IN ('rotated', 'reset')),
    retired_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX refresh_tokens_retired_at ON refresh_tokens_retired (retired_at);

DROP TABLE device_keys;

-- +goose Down
CREATE TABLE device_keys (
    device_id  TEXT PRIMARY KEY,
    key_hash   BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
DROP TABLE refresh_tokens_retired;
DROP TABLE refresh_tokens;
DROP TABLE devices;
DROP TABLE device_batch;
