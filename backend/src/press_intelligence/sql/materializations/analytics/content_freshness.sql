CREATE OR REPLACE TABLE `{google_cloud_project}.{bigquery_dataset_analytics}.content_freshness` AS
SELECT
  COUNT(*) AS total_articles,
  COUNT(DISTINCT section_name) AS active_sections,
  MAX(ingested_at) AS last_sync_at,
  MAX(published_at) AS watermark,
  TIMESTAMP_DIFF(MAX(ingested_at), MAX(published_at), MINUTE) AS freshness_lag_minutes,
  CURRENT_TIMESTAMP() AS refreshed_at
FROM `{google_cloud_project}.{bigquery_dataset_analytics}.articles_latest`;
