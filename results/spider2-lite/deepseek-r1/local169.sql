WITH RECURSIVE numbers(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM numbers WHERE n < 20
),
cohort AS (
    SELECT id_bioguide, MIN(term_start) AS first_term_start
    FROM legislators_terms
    GROUP BY id_bioguide
    HAVING first_term_start BETWEEN '1917-01-01' AND '1999-12-31'
),
cohort_size AS (
    SELECT COUNT(*) AS total FROM cohort
),
legislator_years AS (
    SELECT 
        c.id_bioguide,
        c.first_term_start,
        n.n AS year_number,
        (CAST(SUBSTR(c.first_term_start, 1, 4) AS INTEGER) + n.n - 1) || '-12-31' AS check_date
    FROM cohort c
    CROSS JOIN numbers n
)
SELECT 
    ly.year_number,
    COUNT(DISTINCT lt.id_bioguide) * 1.0 / cs.total AS retention_rate
FROM legislator_years ly
CROSS JOIN cohort_size cs
LEFT JOIN legislators_terms lt 
    ON ly.id_bioguide = lt.id_bioguide
    AND lt.term_start <= ly.check_date
    AND lt.term_end >= ly.check_date
GROUP BY ly.year_number
ORDER BY ly.year_number