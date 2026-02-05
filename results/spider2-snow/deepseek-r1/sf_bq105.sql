WITH state_population AS (
    SELECT 
        z."state_name",
        SUM(p."population") AS state_population
    FROM 
        "NHTSA_TRAFFIC_FATALITIES_PLUS"."CENSUS_BUREAU_USA"."POPULATION_BY_ZIP_2010" p
    JOIN 
        "NHTSA_TRAFFIC_FATALITIES_PLUS"."UTILITY_US"."ZIPCODE_AREA" z 
        ON p."zipcode" = z."zipcode"
    WHERE 
        p."gender" IS NULL 
        AND p."minimum_age" IS NULL 
        AND p."maximum_age" IS NULL
    GROUP BY 
        z."state_name"
),
distracted_accidents AS (
    SELECT 
        a."state_name",
        a."year_of_crash" AS year,
        COUNT(DISTINCT a."consecutive_number") AS distracted_accident_count
    FROM 
        "NHTSA_TRAFFIC_FATALITIES_PLUS"."NHTSA_TRAFFIC_FATALITIES"."ACCIDENT_2015" a
    JOIN 
        "NHTSA_TRAFFIC_FATALITIES_PLUS"."NHTSA_TRAFFIC_FATALITIES"."DISTRACT_2015" d 
        ON a."consecutive_number" = d."consecutive_number" 
        AND a."state_number" = d."state_number"
    WHERE 
        d."driver_distracted_by_name" NOT IN ('Not Distracted', 'Unknown if Distracted', 'Not Reported')
    GROUP BY 
        a."state_name", a."year_of_crash"
    UNION ALL
    SELECT 
        a."state_name",
        a."year_of_crash" AS year,
        COUNT(DISTINCT a."consecutive_number") AS distracted_accident_count
    FROM 
        "NHTSA_TRAFFIC_FATALITIES_PLUS"."NHTSA_TRAFFIC_FATALITIES"."ACCIDENT_2016" a
    JOIN 
        "NHTSA_TRAFFIC_FATALITIES_PLUS"."NHTSA_TRAFFIC_FATALITIES"."DISTRACT_2016" d 
        ON a."consecutive_number" = d."consecutive_number" 
        AND a."state_number" = d."state_number"
    WHERE 
        d."driver_distracted_by_name" NOT IN ('Not Distracted', 'Unknown if Distracted', 'Not Reported')
    GROUP BY 
        a."state_name", a."year_of_crash"
),
accident_rates AS (
    SELECT 
        da."state_name",
        da."year",
        da."distracted_accident_count",
        sp."state_population",
        (da."distracted_accident_count" * 100000.0) / sp."state_population" AS accident_rate_per_100k
    FROM 
        distracted_accidents da
    JOIN 
        state_population sp 
        ON da."state_name" = sp."state_name"
)
SELECT 
    "state_name",
    "year",
    "distracted_accident_count",
    "state_population",
    "accident_rate_per_100k",
    RANK() OVER (PARTITION BY "year" ORDER BY "accident_rate_per_100k" DESC) AS rank
FROM 
    accident_rates
QUALIFY 
    rank <= 5
ORDER BY 
    "year", rank