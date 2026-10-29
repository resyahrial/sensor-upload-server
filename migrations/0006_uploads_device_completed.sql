-- +goose Up
-- GET /files lists completed uploads newest first, optionally for one
-- device; uploads_completed_at serves the unfiltered listing.
CREATE INDEX uploads_device_completed ON uploads (device_id, completed_at DESC, id DESC)
    WHERE status = 'completed';

-- +goose Down
DROP INDEX uploads_device_completed;
