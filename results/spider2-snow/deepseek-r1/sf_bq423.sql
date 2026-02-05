WITH "ads_with_croatia_stats" AS (
  SELECT 
    c.*,
    r.value:"region_code" AS "region_code",
    r.value:"times_shown_availability_date" AS "times_shown_availability_date",
    r.value:"times_shown_upper_bound" AS "times_shown_upper_bound",
    r.value:"first_shown_date" AS "first_shown_date",
    r.value:"last_shown_date" AS "last_shown_date"
  FROM "GOOGLE_ADS"."GOOGLE_ADS_TRANSPARENCY_CENTER"."CREATIVE_STATS" AS c,
  LATERAL FLATTEN(INPUT => c."region_stats") AS r
  WHERE r.value:"region_code" = 'HR'
)
SELECT 
  "creative_page_url"
FROM "ads_with_croatia_stats"
WHERE 
  "ad_format_type" = 'IMAGE'
  AND "topic" = 'Health'
  AND "advertiser_verification_status" = 'VERIFIED'
  AND "advertiser_location" = 'CY'
  AND "times_shown_availability_date" IS NULL
  AND "first_shown_date"::DATE > DATE '2023-01-01'
  AND "last_shown_date"::DATE < DATE '2024-01-01'
  AND "audience_selection_approach_info":"demographic_information" = TRUE
  AND "audience_selection_approach_info":"geo_location_targeting" = TRUE
  AND "audience_selection_approach_info":"contextual_signals" = TRUE
  AND "audience_selection_approach_info":"customer_lists" = TRUE
  AND "audience_selection_approach_info":"topics_of_interest" = TRUE
ORDER BY "times_shown_upper_bound" DESC
LIMIT 1