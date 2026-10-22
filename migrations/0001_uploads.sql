-- +goose Up
CREATE TABLE uploads (
    id               UUID PRIMARY KEY,              -- UUIDv7
    protocol         TEXT NOT NULL CHECK (protocol IN ('rest','rest-http3','grpc')),
    upload_ref       TEXT NOT NULL,                 -- tus ID or gRPC upload_id
    device_id        TEXT NOT NULL,
    sha256           CHAR(64) NOT NULL,
    size_bytes       BIGINT NOT NULL,
    committed_bytes  BIGINT NOT NULL DEFAULT 0,
    filename         TEXT NOT NULL,
    content_type     TEXT NOT NULL,
    captured_at      TIMESTAMPTZ NOT NULL,
    priority         SMALLINT NOT NULL CHECK (priority BETWEEN 1 AND 4),
    status           TEXT NOT NULL CHECK (status IN
                       ('uploading','completed','failed_checksum','cancelled','expired')),
    storage_key      TEXT,
    resume_count     INT NOT NULL DEFAULT 0,        -- times the client resumed
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ
);

CREATE UNIQUE INDEX uploads_sha256_completed ON uploads (sha256) WHERE status = 'completed';
CREATE UNIQUE INDEX uploads_protocol_ref     ON uploads (protocol, upload_ref);
CREATE INDEX        uploads_status_updated   ON uploads (status, updated_at);
CREATE INDEX        uploads_device_created   ON uploads (device_id, created_at DESC);

-- +goose Down
DROP TABLE uploads;
