SELECT `L1_model`, COUNT(*) AS total_count
FROM `model`
GROUP BY `L1_model`
HAVING COUNT(*) = (
    SELECT MAX(cnt) FROM (
        SELECT COUNT(*) AS cnt
        FROM `model`
        GROUP BY `L1_model`
    )
)