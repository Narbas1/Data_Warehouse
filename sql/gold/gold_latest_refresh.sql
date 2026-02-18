INSERT INTO gold.latest_prices_eur (
  coin_id,
  vs_currency,
  price,
  api_last_updated_at,
  ingested_at,
  source_ingestion_id
)
SELECT DISTINCT ON (coin_id)
  coin_id,
  vs_currency,
  price,
  api_last_updated_at,
  ingested_at,
  source_ingestion_id
FROM silver.prices
WHERE vs_currency = 'eur'
ORDER BY coin_id, source_ingestion_id DESC
ON CONFLICT (coin_id) DO UPDATE
SET
  price = EXCLUDED.price,
  api_last_updated_at = EXCLUDED.api_last_updated_at,
  ingested_at = EXCLUDED.ingested_at,
  source_ingestion_id = EXCLUDED.source_ingestion_id;
