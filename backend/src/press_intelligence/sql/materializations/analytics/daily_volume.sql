CREATE OR REPLACE TABLE `{google_cloud_project}.{bigquery_dataset_analytics}.daily_volume`
PARTITION BY publication_date AS
SELECT
  publication_date,
  COUNT(*) AS article_count
FROM `{google_cloud_project}.{bigquery_dataset_analytics}.articles_latest`
GROUP BY publication_date;
