SELECT
  r."creative_page_url" AS "page_url",
  TO_DATE(region_stat.value:"first_shown"::STRING, 'YYYY-MM-DD') AS "first_shown_time",
  TO_DATE(region_stat.value:"last_shown"::STRING, 'YYYY-MM-DD') AS "last_shown_time",
  r."disapproval"[0]:"removal_reason"::STRING AS "removal_reason",
  r."disapproval"[0]:"violation_category"::STRING AS "violation_category",
  region_stat.value:"times_shown_lower_bound"::INTEGER AS "lower_bound",
  region_stat.value:"times_shown_upper_bound"::INTEGER AS "upper_bound"
FROM
  "GOOGLE_ADS"."GOOGLE_ADS_TRANSPARENCY_CENTER"."REMOVED_CREATIVE_STATS" AS r,
  LATERAL FLATTEN(INPUT => r."region_stats") AS region_stat
WHERE
  region_stat.value:"region_code"::STRING = 'HR'
  AND region_stat.value:"times_shown_availability_date" IS NULL
  AND region_stat.value:"times_shown_lower_bound"::INTEGER > 10000
  AND region_stat.value:"times_shown_upper_bound"::INTEGER < 25000
  AND (
    NULLIF(r."audience_selection_approach_info":"demographics"::STRING, 'UNUSED') IS NOT NULL
    OR NULLIF(r."audience_selection_approach_info":"geographic_location"::STRING, 'UNUSED') IS NOT NULL
    OR NULLIF(r."audience_selection_approach_info":"contextual_signals"::STRING, 'UNUSED') IS NOT NULL
    OR NULLIF(r."audience_selection_approach_info":"customer_lists"::STRING, 'UNUSED') IS NOT NULL
    OR NULLIF(r."audience_selection_approach_info":"topics_of_interest"::STRING, 'UNUSED') IS NOT NULL
  )
ORDER BY
  "last_shown_time" DESC
LIMIT 5