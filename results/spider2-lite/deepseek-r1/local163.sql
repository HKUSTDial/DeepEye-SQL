WITH rank_avg AS (
  SELECT `FacRank`, AVG(`FacSalary`) AS avg_salary
  FROM `university_faculty`
  GROUP BY `FacRank`
),
faculty_diff AS (
  SELECT 
    u.`FacRank`,
    u.`FacFirstName`,
    u.`FacLastName`,
    u.`FacSalary`,
    ra.avg_salary,
    ABS(u.`FacSalary` - ra.avg_salary) AS diff
  FROM `university_faculty` u
  JOIN rank_avg ra ON u.`FacRank` = ra.`FacRank`
),
min_diff_per_rank AS (
  SELECT `FacRank`, MIN(diff) AS min_diff
  FROM faculty_diff
  GROUP BY `FacRank`
)
SELECT 
  fd.`FacRank`,
  fd.`FacFirstName`,
  fd.`FacLastName`,
  fd.`FacSalary`
FROM faculty_diff fd
JOIN min_diff_per_rank m ON fd.`FacRank` = m.`FacRank` AND fd.diff = m.min_diff
ORDER BY fd.`FacRank`, fd.`FacLastName`, fd.`FacFirstName`