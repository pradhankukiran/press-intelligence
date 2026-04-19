SELECT
  FORMAT_DATE('%Y-%m-%d', publication_date) AS date,
  world,
  politics,
  business,
  culture,
  climate,
  technology
FROM `{google_cloud_project}.{bigquery_dataset_analytics}.section_daily`
WHERE publication_date BETWEEN DATE(@from_date) AND DATE(@to_date)
ORDER BY publication_date;
