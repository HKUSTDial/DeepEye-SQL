SELECT
    u."complete_years",
    AVG(u."reputation") AS "avg_reputation",
    AVG(COALESCE(b."badge_count", 0)) AS "avg_badge_count"
FROM (
    SELECT
        "id",
        "reputation",
        DATEDIFF(year, TO_DATE(TO_TIMESTAMP("creation_date" / 1000000)), DATE '2021-10-01') -
            CASE
                WHEN DATEADD(year, DATEDIFF(year, TO_DATE(TO_TIMESTAMP("creation_date" / 1000000)), DATE '2021-10-01'), TO_DATE(TO_TIMESTAMP("creation_date" / 1000000))) > DATE '2021-10-01'
                THEN 1
                ELSE 0
            END AS "complete_years"
    FROM "STACKOVERFLOW"."STACKOVERFLOW"."USERS"
    WHERE TO_DATE(TO_TIMESTAMP("creation_date" / 1000000)) <= DATE '2021-10-01'
) u
LEFT JOIN (
    SELECT "user_id", COUNT(*) AS "badge_count"
    FROM "STACKOVERFLOW"."STACKOVERFLOW"."BADGES"
    GROUP BY "user_id"
) b ON u."id" = b."user_id"
GROUP BY u."complete_years"
ORDER BY u."complete_years"