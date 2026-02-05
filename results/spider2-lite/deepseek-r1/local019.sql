WITH nxt_titles AS (
  SELECT `id` FROM `Belts` WHERE `name` LIKE '%NXT%'
),
match_durations AS (
  SELECT m.*,
    (CAST(SUBSTR(m.`duration`, 1, INSTR(m.`duration`, ':') - 1) AS INTEGER) * 60 + 
     CAST(SUBSTR(m.`duration`, INSTR(m.`duration`, ':') + 1) AS INTEGER)) AS `seconds`
  FROM `Matches` m
  WHERE m.`title_id` IN (SELECT `id` FROM nxt_titles)
    AND m.`title_change` = 0
),
min_match AS (
  SELECT * FROM match_durations
  ORDER BY `seconds` ASC
  LIMIT 1
)
SELECT w1.`name` AS `winner_name`, w2.`name` AS `loser_name`
FROM min_match m
JOIN `Wrestlers` w1 ON w1.`id` = m.`winner_id`
JOIN `Wrestlers` w2 ON w2.`id` = m.`loser_id`