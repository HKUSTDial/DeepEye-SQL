WITH filtered_ads AS (
    SELECT "creative_page_url", "audience_selection_approach_info", "region_stats"
    FROM "GOOGLE_ADS"."GOOGLE_ADS_TRANSPARENCY_CENTER"."CREATIVE_STATS"
    WHERE "ad_format_type" = 'IMAGE'
      AND "topic" = 'Health'
      AND "advertiser_verification_status" = 'VERIFIED'
      AND "advertiser_location" = 'CY'
),
croatia_stats AS (
    SELECT fa."creative_page_url",
           region.value:"times_shown_upper_bound" AS "times_shown_upper_bound"
    FROM filtered_ads fa,
         LATERAL FLATTEN(INPUT => fa."region_stats") region
    WHERE region.value:"region_code" = 'HR'
      AND region.value:"times_shown_availability_date" IS NULL
      AND region.value:"first_shown" > '2023-01-01'
      AND region.value:"last_shown" < '2024-01-01'
      AND fa."audience_selection_approach_info":"demographic_info" = 'USED'
      AND fa."audience_selection_approach_info":"geo_location_targeting" = 'USED'
      AND fa."audience_selection_approach_info":"contextual_signals" = 'USED'
      AND fa."audience_selection_approach_info":"customer_lists" = 'USED'
      AND fa."audience_selection_approach_info":"topics_of_interest" = 'USED'
)
SELECT "creative_page_url"
FROM croatia_stats
ORDER BY "times_shown_upper_bound" DESC
LIMIT 1