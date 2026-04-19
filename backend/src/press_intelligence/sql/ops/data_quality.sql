SELECT
  check_name AS name,
  status,
  observed_value,
  threshold,
  JSON_VALUE(details_json, '$.detail') AS detail
FROM `{google_cloud_project}.{bigquery_dataset_ops}.data_quality_results`
ORDER BY name;
