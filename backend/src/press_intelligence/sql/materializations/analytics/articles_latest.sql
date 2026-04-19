CREATE OR REPLACE TABLE `{google_cloud_project}.{bigquery_dataset_analytics}.articles_latest`
PARTITION BY publication_date
CLUSTER BY section_name, pillar_name AS
WITH ranked_articles AS (
  SELECT
    guardian_id,
    web_url,
    web_title,
    section_id,
    section_name,
    pillar_id,
    pillar_name,
    published_at,
    ingested_at,
    tags,
    api_response_page,
    raw_payload,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(
        guardian_id,
        CONCAT(COALESCE(web_url, ''), '#', CAST(published_at AS STRING))
      )
      ORDER BY ingested_at DESC, api_response_page DESC
    ) AS row_num
  FROM `{google_cloud_project}.{bigquery_dataset_raw}.articles_raw`
)
SELECT
  guardian_id,
  web_url,
  web_title,
  section_id,
  COALESCE(section_name, 'Unknown') AS section_name,
  pillar_id,
  COALESCE(pillar_name, 'Unknown') AS pillar_name,
  published_at,
  ingested_at,
  DATE(published_at) AS publication_date,
  COALESCE(tags, ARRAY<STRING>[]) AS tags,
  api_response_page,
  raw_payload
FROM ranked_articles
WHERE row_num = 1;
