CREATE OR REPLACE TABLE `{google_cloud_project}.{bigquery_dataset_ops}.data_quality_results` AS
SELECT
  'Missing section names' AS check_name,
  CURRENT_DATE() AS check_date,
  'warning' AS severity,
  IF(COUNTIF(section_name IS NULL OR section_name = '') = 0, 'pass', 'warn') AS status,
  CAST(COUNTIF(section_name IS NULL OR section_name = '') AS STRING) AS observed_value,
  '0' AS threshold,
  TO_JSON(STRUCT('Section name should always be populated.' AS detail)) AS details_json,
  CURRENT_TIMESTAMP() AS created_at
FROM `{google_cloud_project}.{bigquery_dataset_analytics}.articles_latest`
UNION ALL
SELECT
  'Missing publication date' AS check_name,
  CURRENT_DATE() AS check_date,
  'warning' AS severity,
  IF(COUNTIF(published_at IS NULL) = 0, 'pass', 'warn') AS status,
  CAST(COUNTIF(published_at IS NULL) AS STRING) AS observed_value,
  '0' AS threshold,
  TO_JSON(STRUCT('Published timestamp should always be available.' AS detail)) AS details_json,
  CURRENT_TIMESTAMP() AS created_at
FROM `{google_cloud_project}.{bigquery_dataset_analytics}.articles_latest`;
