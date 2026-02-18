CREATE TABLE IF NOT EXISTS gold.latest_prices_eur (
  coin_id TEXT PRIMARY KEY,
  vs_currency TEXT NOT NULL CHECK (vs_currency = 'eur'),
  price NUMERIC NOT NULL CHECK (price >= 0),
  api_last_updated_at TIMESTAMPTZ NULL,
  ingested_at TIMESTAMPTZ NOT NULL,
  source_ingestion_id BIGINT NOT NULL
);

