WITH rank_averages AS (
    SELECT "FacRank", AVG("FacSalary") AS avg_salary
    FROM "EDUCATION_BUSINESS"."EDUCATION_BUSINESS"."UNIVERSITY_FACULTY"
    GROUP BY "FacRank"
),
faculty_diffs AS (
    SELECT 
        f."FacRank",
        f."FacFirstName",
        f."FacLastName",
        f."FacSalary",
        ABS(f."FacSalary" - a.avg_salary) AS diff
    FROM "EDUCATION_BUSINESS"."EDUCATION_BUSINESS"."UNIVERSITY_FACULTY" f
    JOIN rank_averages a ON f."FacRank" = a."FacRank"
),
min_diffs AS (
    SELECT "FacRank", MIN(diff) AS min_diff
    FROM faculty_diffs
    GROUP BY "FacRank"
)
SELECT 
    fd."FacRank",
    fd."FacFirstName",
    fd."FacLastName",
    fd."FacSalary"
FROM faculty_diffs fd
JOIN min_diffs md ON fd."FacRank" = md."FacRank" AND fd.diff = md.min_diff