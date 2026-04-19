CREATE OR REPLACE TABLE `{google_cloud_project}.{bigquery_dataset_analytics}.section_daily`
PARTITION BY publication_date AS
SELECT
  publication_date,
  SUM(CASE WHEN LOWER(section) LIKE '%world%' THEN article_count ELSE 0 END) AS world,
  SUM(CASE WHEN LOWER(section) LIKE '%politic%' THEN article_count ELSE 0 END) AS politics,
  SUM(CASE WHEN LOWER(section) LIKE '%business%' THEN article_count ELSE 0 END) AS business,
  SUM(
    CASE
      WHEN LOWER(section) LIKE '%culture%' OR LOWER(section) LIKE '%art%'
      THEN article_count
      ELSE 0
    END
  ) AS culture,
  SUM(
    CASE
      WHEN LOWER(section) LIKE '%climate%' OR LOWER(section) LIKE '%environment%'
      THEN article_count
      ELSE 0
    END
  ) AS climate,
  SUM(
    CASE
      WHEN LOWER(section) LIKE '%tech%' OR LOWER(section) LIKE '%digital%'
      THEN article_count
      ELSE 0
    END
  ) AS technology
FROM `{google_cloud_project}.{bigquery_dataset_analytics}.section_counts_daily`
GROUP BY publication_date;
