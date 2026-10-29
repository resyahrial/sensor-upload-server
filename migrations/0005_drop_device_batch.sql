-- +goose Up
-- Open registration: any valid device id may register (it stays pending
-- until approved), so the pre-imported allow-list goes away. Per-device
-- serialization moves to a transaction advisory lock (db.Devices.Update).
ALTER TABLE devices DROP CONSTRAINT devices_device_id_fkey;
DROP TABLE device_batch;

-- +goose Down
CREATE TABLE device_batch (
    device_id   TEXT PRIMARY KEY,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO device_batch (device_id) SELECT device_id FROM devices;
ALTER TABLE devices ADD CONSTRAINT devices_device_id_fkey
    FOREIGN KEY (device_id) REFERENCES device_batch (device_id);
