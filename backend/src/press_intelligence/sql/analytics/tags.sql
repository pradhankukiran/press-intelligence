SELECT
  tag,
  SUM(article_count) AS count,
  'Live' AS momentum
FROM `{google_cloud_project}.{bigquery_dataset_analytics}.tag_counts_daily`
WHERE publication_date BETWEEN DATE(@from_date) AND DATE(@to_date)
GROUP BY tag
ORDER BY count DESC
LIMIT @row_limit;
