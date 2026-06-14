CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL DEFAULT gen_random_uuid(),
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    device_id VARCHAR(32) NOT NULL,
    card_uid VARCHAR(32) NOT NULL,
    access_granted BOOLEAN NOT NULL,
    door_id VARCHAR(32) NOT NULL,
    zone VARCHAR(32) NOT NULL,
    signal_strength INTEGER NOT NULL,
    battery_voltage REAL NOT NULL,
    response_time_ms INTEGER NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    temperature REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_timestamp
    ON events (timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_events_device_id
    ON events (device_id);

CREATE INDEX IF NOT EXISTS idx_events_device_timestamp
    ON events (device_id ASC, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_events_event_type_timestamp
    ON events (event_type, timestamp DESC);

INSERT INTO events (
    event_id,
    timestamp,
    device_id,
    card_uid,
    access_granted,
    door_id,
    zone,
    signal_strength,
    battery_voltage,
    response_time_ms,
    event_type,
    temperature
)
SELECT
    gen_random_uuid(),
    NOW() - (gs * INTERVAL '20 seconds'),
    (ARRAY['RFID-ENT-01', 'RFID-ENT-02', 'RFID-WH-01', 'RFID-OFFICE-01', 'RFID-LAB-01'])[1 + (gs % 5)],
    LPAD(TO_HEX((random() * 4294967295)::BIGINT), 8, '0'),
    random() > 0.12,
    (ARRAY['MAIN_GATE', 'GARAGE', 'SERVER_ROOM', 'OFFICE_A', 'OFFICE_B', 'LAB', 'WAREHOUSE'])[1 + (gs % 7)],
    (ARRAY['GROUND_FLOOR', 'GARAGE', 'SECOND_FLOOR', 'FIRST_FLOOR', 'FIRST_FLOOR', 'LAB_BLOCK', 'WAREHOUSE'])[1 + (gs % 7)],
    (-90 + (random() * 60))::INTEGER,
    ROUND((3.4 + random() * 0.8)::NUMERIC, 2)::REAL,
    (10 + random() * 290)::INTEGER,
    CASE
        WHEN random() < 0.08 THEN 'FORCED_OPEN'
        WHEN random() < 0.18 THEN 'ACCESS_DENIED'
        WHEN random() < 0.58 THEN 'ENTRY'
        ELSE 'EXIT'
    END,
    ROUND((18 + random() * 22)::NUMERIC, 1)::REAL
FROM generate_series(1, 20000) AS gs
WHERE NOT EXISTS (SELECT 1 FROM events);
