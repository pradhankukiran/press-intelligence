SELECT
  guardian_id,
  web_title,
  web_url,
  section_id,
  section_name,
  pillar_name,
  FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', published_at) AS published_at,
  tags
FROM `{google_cloud_project}.{bigquery_dataset_analytics}.articles_latest`
WHERE publication_date BETWEEN DATE(@from_date) AND DATE(@to_date)
  AND (@query_text IS NULL OR LOWER(web_title) LIKE CONCAT('%', LOWER(@query_text), '%'))
  AND (@section IS NULL OR section_name = @section)
  AND (@tag IS NULL OR @tag IN UNNEST(tags))
ORDER BY published_at DESC
LIMIT @row_limit OFFSET @row_offset;
