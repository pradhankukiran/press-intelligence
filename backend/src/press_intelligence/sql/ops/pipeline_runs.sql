SELECT
  run_id,
  dag_id,
  status,
  trigger,
  started_at,
  finished_at,
  `window` AS run_window,
  error_summary
FROM `{google_cloud_project}.{bigquery_dataset_ops}.pipeline_runs`
ORDER BY started_at DESC, run_id DESC
LIMIT @row_limit OFFSET @row_offset;
