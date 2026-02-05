SELECT
  q."id" AS "question_id",
  MAX(LENGTH(q."body")) AS "body_length",
  u."reputation" AS "asker_reputation",
  (u."up_votes" - u."down_votes") AS "net_votes",
  COUNT(b."id") AS "badge_count"
FROM
  "STACKOVERFLOW"."STACKOVERFLOW"."POSTS_QUESTIONS" q
  JOIN "STACKOVERFLOW"."STACKOVERFLOW"."USERS" u ON q."owner_user_id" = u."id"
  LEFT JOIN "STACKOVERFLOW"."STACKOVERFLOW"."BADGES" b ON u."id" = b."user_id"
WHERE
  (q."accepted_answer_id" IS NOT NULL)
  OR
  (q."accepted_answer_id" IS NULL AND
    EXISTS (
      SELECT 1
      FROM "STACKOVERFLOW"."STACKOVERFLOW"."POSTS_ANSWERS" a
      WHERE a."parent_id" = q."id"
        AND a."score" / NULLIF(CAST(NULLIF(a."view_count", 'NULL') AS DECIMAL), 0) > 0.01
    )
  )
GROUP BY
  q."id", u."reputation", u."up_votes", u."down_votes"
ORDER BY
  "body_length" DESC
LIMIT 10