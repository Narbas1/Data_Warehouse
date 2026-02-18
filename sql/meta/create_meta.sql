CREATE TABLE meta.watermarks (
  pipeline TEXT PRIMARY KEY,
  last_ingested_at TIMESTAMPTZ
);