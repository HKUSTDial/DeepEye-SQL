WITH yash_pid AS (
    SELECT `PID` FROM `Person` WHERE TRIM(`Name`) = 'Yash Chopra'
),
collab AS (
    SELECT 
        c.`PID` AS actor_pid,
        d.`PID` AS director_pid,
        COUNT(DISTINCT c.`MID`) AS collab_count
    FROM `M_Cast` c
    INNER JOIN `M_Director` d ON c.`MID` = d.`MID`
    GROUP BY c.`PID`, d.`PID`
),
yash_counts AS (
    SELECT 
        actor_pid,
        collab_count AS y_count
    FROM collab
    WHERE director_pid = (SELECT `PID` FROM yash_pid)
),
other_max AS (
    SELECT 
        actor_pid,
        MAX(collab_count) AS other_max_count
    FROM collab
    WHERE director_pid != (SELECT `PID` FROM yash_pid)
    GROUP BY actor_pid
)
SELECT COUNT(*)
FROM yash_counts y
LEFT JOIN other_max o ON y.actor_pid = o.actor_pid
WHERE y.y_count > COALESCE(o.other_max_count, 0)