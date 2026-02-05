WITH utah_zip_counts AS (
    SELECT 
        z."zip_code",
        COUNT(l."branch_fdic_uninum") AS location_count
    FROM 
        "FDA"."GEO_US_BOUNDARIES"."ZIP_CODES" z
    LEFT JOIN (
        SELECT 
            l."zip_code",
            l."branch_fdic_uninum"
        FROM 
            "FDA"."FDIC_BANKS"."LOCATIONS" l
        INNER JOIN 
            "FDA"."FDIC_BANKS"."INSTITUTIONS" i 
            ON l."fdic_certificate_number" = i."fdic_certificate_number"
        WHERE 
            i."fdic_insured" = TRUE
            AND l."state" = 'UT'
    ) l ON z."zip_code" = l."zip_code"
    WHERE 
        z."state_code" = 'UT'
    GROUP BY 
        z."zip_code"
),
min_location_zip AS (
    SELECT 
        "zip_code"
    FROM 
        utah_zip_counts
    ORDER BY 
        location_count ASC
    LIMIT 1
)
SELECT 
    z."acs_employment_rate_2017"
FROM 
    "FDA"."GEO_US_BOUNDARIES"."ZIP_CODES" z
JOIN 
    min_location_zip m ON z."zip_code" = m."zip_code"