CREATE TABLE IF NOT EXISTS events (
    event_id UUID PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    device_id VARCHAR(50) NOT NULL,
    card_uid VARCHAR(20) NOT NULL,
    access_granted BOOLEAN NOT NULL,
    door_id VARCHAR(50) NOT NULL,
    zone VARCHAR(50) NOT NULL,
    signal_strength INT NOT NULL,
    battery_voltage NUMERIC(3, 2) NOT NULL,
    response_time_ms INT NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    temperature NUMERIC(4, 1) NOT NULL
);

-- Indexing for performance queries if needed
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
CREATE INDEX IF NOT EXISTS idx_events_device_id ON events(device_id);
