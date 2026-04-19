SELECT
  FORMAT_DATE('%Y-%m-%d', publication_date) AS date,
  article_count AS value
FROM `{google_cloud_project}.{bigquery_dataset_analytics}.daily_volume`
WHERE publication_date BETWEEN DATE('{from_date}') AND DATE('{to_date}')
ORDER BY publication_date;
