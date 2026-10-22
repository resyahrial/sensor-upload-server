-- +goose Up
-- Retention and disk pressure list completed uploads oldest first.
CREATE INDEX uploads_completed_at ON uploads (completed_at) WHERE status = 'completed';

-- +goose Down
DROP INDEX uploads_completed_at;
