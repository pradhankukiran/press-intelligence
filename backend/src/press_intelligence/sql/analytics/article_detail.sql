SELECT
  guardian_id,
  web_title,
  web_url,
  section_id,
  section_name,
  pillar_name,
  FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', published_at) AS published_at,
  tags,
  raw_payload
FROM `{google_cloud_project}.{bigquery_dataset_analytics}.articles_latest`
WHERE guardian_id = @guardian_id
LIMIT 1;
