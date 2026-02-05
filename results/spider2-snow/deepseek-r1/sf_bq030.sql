SELECT
    "country_region",
    ROUND((SUM("recovered") / SUM("confirmed")) * 100, 2) AS "recovery_rate"
FROM
    "COVID19_OPEN_DATA"."COVID19_OPEN_DATA"."COMPATIBILITY_VIEW"
WHERE
    "date" <= '2020-05-10'
    AND "confirmed" IS NOT NULL AND "confirmed" = "confirmed"
    AND "recovered" IS NOT NULL AND "recovered" = "recovered"
GROUP BY
    "country_region"
HAVING
    SUM("confirmed") > 50000
ORDER BY
    "recovery_rate" DESC
LIMIT 3