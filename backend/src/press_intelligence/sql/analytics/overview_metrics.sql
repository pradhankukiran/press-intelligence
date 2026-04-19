SELECT
  COUNT(*) AS total_articles,
  COUNT(DISTINCT COALESCE(section_name, 'Unknown')) AS active_sections,
  FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', MAX(ingested_at)) AS last_sync_at,
  FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', MAX(published_at)) AS watermark,
  TIMESTAMP_DIFF(MAX(ingested_at), MAX(published_at), MINUTE) AS freshness_lag_minutes
FROM `{google_cloud_project}.{bigquery_dataset_analytics}.articles_latest`
WHERE publication_date BETWEEN DATE(@from_date) AND DATE(@to_date);
