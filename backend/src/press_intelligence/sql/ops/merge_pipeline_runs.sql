MERGE `{google_cloud_project}.{bigquery_dataset_ops}.pipeline_runs` T
USING `{google_cloud_project}.{bigquery_dataset_ops}.{staging_table}` S
ON T.dag_id = S.dag_id AND T.run_id = S.run_id
WHEN MATCHED THEN UPDATE SET
  status = S.status,
  trigger = S.trigger,
  started_at = S.started_at,
  finished_at = S.finished_at,
  `window` = S.`window`,
  error_summary = S.error_summary
WHEN NOT MATCHED THEN INSERT (
  run_id, dag_id, status, trigger, started_at, finished_at, `window`, error_summary
) VALUES (
  S.run_id, S.dag_id, S.status, S.trigger, S.started_at, S.finished_at, S.`window`, S.error_summary
);
