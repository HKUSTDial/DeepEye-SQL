WITH first_messages AS (
    SELECT
        fm."PostUserId" AS user_id,
        COUNT(DISTINCT fv."FromUserId") AS message_score
    FROM
        "META_KAGGLE"."META_KAGGLE"."FORUMTOPICS" ft
    JOIN
        "META_KAGGLE"."META_KAGGLE"."FORUMMESSAGES" fm
        ON ft."FirstForumMessageId" = fm."Id"
    LEFT JOIN
        "META_KAGGLE"."META_KAGGLE"."FORUMMESSAGEVOTES" fv
        ON fm."Id" = fv."ForumMessageId"
    WHERE
        ft."FirstForumMessageId" IS NOT NULL
    GROUP BY
        fm."PostUserId", ft."FirstForumMessageId"
),
avg_score AS (
    SELECT AVG(message_score) AS avg_msg_score FROM first_messages
),
user_max_scores AS (
    SELECT
        user_id,
        MAX(message_score) AS user_max_score
    FROM first_messages
    GROUP BY user_id
),
ranked_users AS (
    SELECT
        user_id,
        user_max_score,
        ROW_NUMBER() OVER (ORDER BY user_max_score DESC) AS rn
    FROM user_max_scores
)
SELECT
    u."UserName" AS username,
    ABS(ru.user_max_score - a.avg_msg_score) AS absolute_difference
FROM
    ranked_users ru
CROSS JOIN
    avg_score a
JOIN
    "META_KAGGLE"."META_KAGGLE"."USERS" u
    ON ru.user_id = u."Id"
WHERE
    ru.rn <= 3
ORDER BY
    ru.rn