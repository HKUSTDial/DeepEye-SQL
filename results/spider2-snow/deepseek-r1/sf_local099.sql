WITH yash_pid AS (
    SELECT TRIM("PID") AS pid FROM "DB_IMDB"."DB_IMDB"."PERSON" WHERE TRIM("Name") = 'Yash Chopra' LIMIT 1
),
collaborations AS (
    SELECT 
        TRIM(c."PID") AS actor_pid,
        TRIM(d."PID") AS director_pid,
        COUNT(DISTINCT c."MID") AS collab_count
    FROM "DB_IMDB"."DB_IMDB"."M_CAST" c
    INNER JOIN "DB_IMDB"."DB_IMDB"."M_DIRECTOR" d ON c."MID" = d."MID"
    GROUP BY TRIM(c."PID"), TRIM(d."PID")
),
yash_collabs AS (
    SELECT 
        actor_pid,
        collab_count AS yash_count
    FROM collaborations
    WHERE director_pid = (SELECT pid FROM yash_pid)
),
other_max AS (
    SELECT 
        actor_pid,
        MAX(collab_count) AS max_other_count
    FROM collaborations
    WHERE director_pid != (SELECT pid FROM yash_pid)
    GROUP BY actor_pid
)
SELECT COUNT(DISTINCT y.actor_pid) 
FROM yash_collabs y
LEFT JOIN other_max o ON y.actor_pid = o.actor_pid
WHERE y.yash_count > COALESCE(o.max_other_count, 0)