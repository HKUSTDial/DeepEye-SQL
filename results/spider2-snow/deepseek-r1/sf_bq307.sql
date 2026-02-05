SELECT
    "BADGE_NAME",
    COUNT("USER_ID") AS "NUM_USERS",
    AVG("DAYS_FROM_CREATION") AS "AVG_DAYS"
FROM (
    SELECT
        b."name" AS "BADGE_NAME",
        b."user_id" AS "USER_ID",
        (b."date" - u."creation_date") / 86400000000 AS "DAYS_FROM_CREATION"
    FROM
        "STACKOVERFLOW"."STACKOVERFLOW"."BADGES" b
    JOIN
        "STACKOVERFLOW"."STACKOVERFLOW"."USERS" u ON b."user_id" = u."id"
    WHERE
        b."class" = 1
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY b."user_id" ORDER BY b."date" ASC) = 1
) t
GROUP BY "BADGE_NAME"
ORDER BY "NUM_USERS" DESC
LIMIT 10