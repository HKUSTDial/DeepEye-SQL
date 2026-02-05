SELECT "name"
FROM (
    SELECT wy."name", wy.wyoming_count / us.total_us_count AS proportion
    FROM (
        SELECT "name", SUM("number") AS wyoming_count
        FROM "USA_NAMES"."USA_NAMES"."USA_1910_CURRENT"
        WHERE "year" = 2021 AND "gender" = 'F' AND "state" = 'WY'
        GROUP BY "name"
    ) wy
    JOIN (
        SELECT "name", SUM("number") AS total_us_count
        FROM "USA_NAMES"."USA_NAMES"."USA_1910_CURRENT"
        WHERE "year" = 2021 AND "gender" = 'F'
        GROUP BY "name"
    ) us
    ON wy."name" = us."name"
)
ORDER BY proportion DESC
LIMIT 1