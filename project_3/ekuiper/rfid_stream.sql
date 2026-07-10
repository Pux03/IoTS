CREATE STREAM rfid_events(
  event_id string,
  timestamp string,
  device_id string,
  card_uid string,
  access_granted boolean,
  door_id string,
  zone string,
  signal_strength bigint,
  battery_voltage float,
  response_time_ms bigint,
  event_type string
) WITH (DATASOURCE="rfid/events", FORMAT="json", TYPE="mqtt");
