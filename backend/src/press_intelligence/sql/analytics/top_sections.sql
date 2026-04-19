SELECT
  section,
  SUM(article_count) AS count
FROM `{google_cloud_project}.{bigquery_dataset_analytics}.section_counts_daily`
WHERE publication_date BETWEEN DATE(@from_date) AND DATE(@to_date)
GROUP BY section
ORDER BY count DESC
LIMIT 4;
