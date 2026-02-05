WITH page_events AS (
    SELECT 
        e."EVENT_TIMESTAMP",
        MAX(CASE WHEN ep."KEY" = 'page_title' THEN ep."VALUE"['string_value']::STRING END) as page_title,
        MAX(CASE WHEN ep."KEY" = 'page_location' THEN ep."VALUE"['string_value']::STRING END) as page_location
    FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210128" e,
    LATERAL FLATTEN(input => e."EVENT_PARAMS") ep
    WHERE e."USER_PSEUDO_ID" = '1362228.4966015575'
        AND e."EVENT_NAME" = 'page_view'
        AND e."EVENT_DATE" = '20210128'
    GROUP BY e."EVENT_TIMESTAMP"
),
url_parts AS (
    SELECT 
        "EVENT_TIMESTAMP",
        page_title,
        page_location,
        SPLIT_PART(page_location, '/', 4) as segment4,
        SPLIT_PART(page_location, '/', 5) as segment5,
        SPLIT_PART(page_location, '/', -1) as last_segment,
        ARRAY_SIZE(SPLIT(page_location, '/')) as segment_count
    FROM page_events
),
classified_pages AS (
    SELECT 
        "EVENT_TIMESTAMP",
        page_title,
        page_location,
        CASE 
            WHEN segment_count >= 5 
                AND last_segment LIKE '%+%'
                AND (
                    segment4 LIKE ANY ('%Accessories%','%Apparel%','%Brands%','%Campus Collection%','%Drinkware%','%Electronics%','%Google Redesign%','%Lifestyle%','%Nest%','%New 2015 Logo%','%Notebooks Journals%','%Office%','%Shop by Brand%','%Small Goods%','%Stationery%','%Wearables%')
                    OR segment5 LIKE ANY ('%Accessories%','%Apparel%','%Brands%','%Campus Collection%','%Drinkware%','%Electronics%','%Google Redesign%','%Lifestyle%','%Nest%','%New 2015 Logo%','%Notebooks Journals%','%Office%','%Shop by Brand%','%Small Goods%','%Stationery%','%Wearables%')
                )
                THEN 'PDP'
            WHEN segment_count >= 5 
                AND last_segment NOT LIKE '%+%'
                AND (
                    segment4 LIKE ANY ('%Accessories%','%Apparel%','%Brands%','%Campus Collection%','%Drinkware%','%Electronics%','%Google Redesign%','%Lifestyle%','%Nest%','%New 2015 Logo%','%Notebooks Journals%','%Office%','%Shop by Brand%','%Small Goods%','%Stationery%','%Wearables%')
                    OR segment5 LIKE ANY ('%Accessories%','%Apparel%','%Brands%','%Campus Collection%','%Drinkware%','%Electronics%','%Google Redesign%','%Lifestyle%','%Nest%','%New 2015 Logo%','%Notebooks Journals%','%Office%','%Shop by Brand%','%Small Goods%','%Stationery%','%Wearables%')
                )
                THEN 'PLP'
            ELSE page_title
        END as display_value
    FROM url_parts
),
ordered_pages AS (
    SELECT 
        "EVENT_TIMESTAMP",
        display_value,
        LAG(display_value) OVER (ORDER BY "EVENT_TIMESTAMP") as prev_display
    FROM classified_pages
),
deduplicated_pages AS (
    SELECT 
        "EVENT_TIMESTAMP",
        display_value
    FROM ordered_pages
    WHERE prev_display IS NULL OR display_value != prev_display
)
SELECT LISTAGG(display_value, ' >> ') WITHIN GROUP (ORDER BY "EVENT_TIMESTAMP") as navigation_flow
FROM deduplicated_pages