WITH nxt_belts AS (
    SELECT "id", "name"
    FROM "WWE"."WWE"."BELTS"
    WHERE "name" LIKE '%NXT%'
),
nxt_matches AS (
    SELECT m."id" AS match_id, m."title_id", m."winner_id", m."loser_id", m."duration"
    FROM "WWE"."WWE"."MATCHES" m
    WHERE m."title_id" IN (SELECT "id" FROM nxt_belts)
      AND m."title_change" = 0
      AND m."duration" != ''
),
matches_with_seconds AS (
    SELECT *,
           TRY_TO_NUMBER(SPLIT_PART("duration", ':', 1)) * 60 + TRY_TO_NUMBER(SPLIT_PART("duration", ':', 2)) AS duration_seconds
    FROM nxt_matches
),
shortest_match AS (
    SELECT *
    FROM matches_with_seconds
    ORDER BY duration_seconds ASC
    LIMIT 1
)
SELECT w1."name" AS winner_name, w2."name" AS loser_name
FROM shortest_match sm
JOIN "WWE"."WWE"."WRESTLERS" w1 ON TRY_TO_NUMBER(sm."winner_id") = w1."id"
JOIN "WWE"."WWE"."WRESTLERS" w2 ON TRY_TO_NUMBER(sm."loser_id") = w2."id"