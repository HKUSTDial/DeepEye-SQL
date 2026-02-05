WITH croatian_ads AS (
  SELECT 
    rcs."creative_page_url",
    rcs."disapproval" AS "first_shown_time",
    rs.value:"last_shown_time" AS "last_shown_time",
    rs.value:"removal_reason" AS "removal_reason",
    rs.value:"violation_category" AS "violation_category",
    rs.value:"times_shown_lower_bound" AS "times_shown_lower_bound",
    rs.value:"times_shown_upper_bound" AS "times_shown_upper_bound",
    rs.value:"times_shown_availability_date" AS "times_shown_availability_date"
  FROM "GOOGLE_ADS"."GOOGLE_ADS_TRANSPARENCY_CENTER"."REMOVED_CREATIVE_STATS" AS rcs,
  LATERAL FLATTEN(INPUT => rcs."region_stats") AS rs
  WHERE rs.value:"region_code"::STRING = 'HR'
),
filtered_audience_ads AS (
  SELECT DISTINCT ca.*
  FROM croatian_ads ca
  INNER JOIN "GOOGLE_ADS"."GOOGLE_ADS_TRANSPARENCY_CENTER"."REMOVED_CREATIVE_STATS" AS rcs
    ON ca."creative_page_url" = rcs."creative_page_url",
  LATERAL FLATTEN(INPUT => rcs."audience_selection_approach_info") AS asa
  WHERE ca."times_shown_availability_date" IS NULL
    AND ca."times_shown_lower_bound"::INTEGER > 10000
    AND ca."times_shown_upper_bound"::INTEGER < 25000
    AND asa.key IN ('demographics', 'geographic_location', 'contextual_signals', 'customer_lists', 'topics_of_interest')
    AND asa.value::STRING != 'unused'
)
SELECT 
  "creative_page_url",
  "first_shown_time",
  "last_shown_time",
  "removal_reason",
  "violation_category",
  "times_shown_lower_bound",
  "times_shown_upper_bound"
FROM filtered_audience_ads
ORDER BY "last_shown_time" DESC
LIMIT 5