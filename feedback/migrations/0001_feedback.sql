CREATE TABLE event_catalog (
  target_date TEXT NOT NULL,
  event_id TEXT NOT NULL,
  organizer_name TEXT NOT NULL,
  in_digest INTEGER NOT NULL CHECK(in_digest IN (0, 1)),
  event_json TEXT NOT NULL,
  PRIMARY KEY (target_date, event_id)
);
CREATE TABLE processed_events (
  event_id TEXT PRIMARY KEY,
  processed_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE messages (
  message_key TEXT PRIMARY KEY,
  channel_id TEXT NOT NULL,
  message_ts TEXT NOT NULL,
  thread_ts TEXT NOT NULL,
  revision_ts TEXT NOT NULL,
  source_text TEXT NOT NULL,
  reported_date TEXT NOT NULL,
  deleted INTEGER NOT NULL DEFAULT 0,
  received_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX messages_thread ON messages(channel_id, thread_ts);
CREATE TABLE reports (
  message_key TEXT NOT NULL REFERENCES messages(message_key),
  report_index INTEGER NOT NULL,
  event_id TEXT,
  event_date TEXT,
  organizer_name TEXT,
  food_status TEXT NOT NULL,
  missed_by_digest INTEGER,
  report_json TEXT NOT NULL,
  PRIMARY KEY(message_key, report_index)
);
CREATE INDEX reports_event ON reports(event_date, event_id);
-- This is an observation summary, NOT a probability that future events have food.
-- Multiple students reporting the same event count as one event in each category.
CREATE VIEW club_observations AS
SELECT organizer_name,
  COUNT(DISTINCT event_date || ':' || event_id) AS reported_events,
  COUNT(DISTINCT CASE WHEN food_status = 'available' THEN event_date || ':' || event_id END) AS events_with_food_seen,
  COUNT(DISTINCT CASE WHEN food_status = 'none_seen' THEN event_date || ':' || event_id END) AS events_with_none_seen,
  COUNT(DISTINCT CASE WHEN food_status = 'ran_out' THEN event_date || ':' || event_id END) AS events_reported_out,
  COUNT(DISTINCT CASE WHEN missed_by_digest = 1 AND food_status IN ('available', 'ran_out') THEN event_date || ':' || event_id END) AS missed_food_events
FROM reports WHERE event_id IS NOT NULL GROUP BY organizer_name;

CREATE VIEW club_food_observations AS
SELECT r.organizer_name, lower(trim(f.value)) AS food,
  COUNT(DISTINCT r.event_date || ':' || r.event_id) AS reported_events
FROM reports r, json_each(r.report_json, '$.food_items') f
WHERE r.event_id IS NOT NULL AND r.food_status IN ('available', 'ran_out')
GROUP BY r.organizer_name, lower(trim(f.value));
