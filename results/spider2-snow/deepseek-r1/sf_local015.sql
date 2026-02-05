WITH motorcycle_collisions AS (
    SELECT "case_id"
    FROM "CALIFORNIA_TRAFFIC_COLLISION"."CALIFORNIA_TRAFFIC_COLLISION"."COLLISIONS"
    WHERE "motorcycle_collision" = 1
),
motorcyclist_parties AS (
    SELECT 
        p."case_id",
        p."party_number",
        p."statewide_vehicle_type",
        CASE 
            WHEN p."party_safety_equipment_1" ILIKE '%helmet%' OR p."party_safety_equipment_2" ILIKE '%helmet%' THEN 1
            ELSE 0
        END AS "helmet_worn"
    FROM "CALIFORNIA_TRAFFIC_COLLISION"."CALIFORNIA_TRAFFIC_COLLISION"."PARTIES" p
    INNER JOIN motorcycle_collisions mc ON p."case_id" = mc."case_id"
    WHERE p."statewide_vehicle_type" ILIKE '%motorcycle%'
),
collision_helmet_groups AS (
    SELECT 
        "case_id",
        MAX("helmet_worn") AS "helmet_group"
    FROM motorcyclist_parties
    GROUP BY "case_id"
),
collision_helmet_groups_full AS (
    SELECT 
        mc."case_id",
        COALESCE(chg."helmet_group", 0) AS "helmet_group"
    FROM motorcycle_collisions mc
    LEFT JOIN collision_helmet_groups chg ON mc."case_id" = chg."case_id"
),
collision_counts AS (
    SELECT 
        "helmet_group",
        COUNT(DISTINCT "case_id") AS "total_collisions"
    FROM collision_helmet_groups_full
    GROUP BY "helmet_group"
),
motorcyclist_fatalities AS (
    SELECT 
        v."case_id",
        v."party_number",
        p."statewide_vehicle_type",
        ch."helmet_group"
    FROM "CALIFORNIA_TRAFFIC_COLLISION"."CALIFORNIA_TRAFFIC_COLLISION"."VICTIMS" v
    JOIN "CALIFORNIA_TRAFFIC_COLLISION"."CALIFORNIA_TRAFFIC_COLLISION"."PARTIES" p 
        ON v."case_id" = p."case_id" AND v."party_number" = p."party_number"
    JOIN collision_helmet_groups_full ch ON v."case_id" = ch."case_id"
    WHERE v."victim_degree_of_injury" = 'killed'
      AND p."statewide_vehicle_type" ILIKE '%motorcycle%'
),
fatality_counts AS (
    SELECT 
        "helmet_group",
        COUNT(*) AS "total_fatalities"
    FROM motorcyclist_fatalities
    GROUP BY "helmet_group"
)
SELECT 
    CASE 
        WHEN cc."helmet_group" = 1 THEN 'Helmet'
        ELSE 'No Helmet'
    END AS "helmet_usage",
    cc."total_collisions",
    COALESCE(fc."total_fatalities", 0) AS "total_fatalities",
    CASE 
        WHEN cc."total_collisions" > 0 THEN 
            (COALESCE(fc."total_fatalities", 0) * 100.0) / cc."total_collisions"
        ELSE 0 
    END AS "fatality_rate_percentage"
FROM collision_counts cc
LEFT JOIN fatality_counts fc ON cc."helmet_group" = fc."helmet_group"
ORDER BY cc."helmet_group" DESC;