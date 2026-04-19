CREATE OR REPLACE TABLE `{google_cloud_project}.{bigquery_dataset_analytics}.tag_counts_daily`
PARTITION BY publication_date
CLUSTER BY tag AS
SELECT
  publication_date,
  tag,
  COUNT(*) AS article_count
FROM `{google_cloud_project}.{bigquery_dataset_analytics}.article_tags`
GROUP BY publication_date, tag;
