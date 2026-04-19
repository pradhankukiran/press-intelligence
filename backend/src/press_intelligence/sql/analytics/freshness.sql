SELECT
  total_articles,
  active_sections,
  FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', last_sync_at) AS last_sync_at,
  FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', watermark) AS watermark,
  freshness_lag_minutes
FROM `{google_cloud_project}.{bigquery_dataset_analytics}.content_freshness`
LIMIT 1;
