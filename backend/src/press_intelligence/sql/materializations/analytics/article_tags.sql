CREATE OR REPLACE TABLE `{google_cloud_project}.{bigquery_dataset_analytics}.article_tags`
PARTITION BY publication_date
CLUSTER BY tag, section_name AS
SELECT
  guardian_id,
  publication_date,
  published_at,
  section_name,
  pillar_name,
  tag
FROM `{google_cloud_project}.{bigquery_dataset_analytics}.articles_latest`,
UNNEST(COALESCE(tags, ARRAY<STRING>[])) AS tag;
