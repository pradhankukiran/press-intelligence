SELECT
  DATE(published_at) AS publication_date,
  COUNT(*) AS article_count
FROM `{google_cloud_project}.{bigquery_dataset_raw}.articles_raw`
WHERE DATE(published_at) BETWEEN DATE('{from_date}') AND DATE('{to_date}')
GROUP BY publication_date
ORDER BY publication_date;
