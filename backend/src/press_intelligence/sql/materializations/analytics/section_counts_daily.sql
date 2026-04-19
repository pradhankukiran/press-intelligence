CREATE OR REPLACE TABLE `{google_cloud_project}.{bigquery_dataset_analytics}.section_counts_daily`
PARTITION BY publication_date
CLUSTER BY section AS
SELECT
  publication_date,
  section_name AS section,
  COUNT(*) AS article_count
FROM `{google_cloud_project}.{bigquery_dataset_analytics}.articles_latest`
GROUP BY publication_date, section;
