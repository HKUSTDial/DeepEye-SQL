WITH years AS (
    SELECT '2019' AS year UNION ALL SELECT '2020' UNION ALL SELECT '2021'
),
unicorn_companies AS (
    SELECT cd.company_id, strftime('%Y', cd.date_joined) AS join_year
    FROM companies_dates cd
    JOIN companies_funding cf ON cd.company_id = cf.company_id
    WHERE cf.valuation >= 1000000000
        AND strftime('%Y', cd.date_joined) IN ('2019','2020','2021')
),
industry_yearly_counts AS (
    SELECT ci.industry, uc.join_year, COUNT(DISTINCT uc.company_id) AS yearly_count
    FROM unicorn_companies uc
    JOIN companies_industries ci ON uc.company_id = ci.company_id
    GROUP BY ci.industry, uc.join_year
),
industry_totals AS (
    SELECT industry, SUM(yearly_count) AS total
    FROM industry_yearly_counts
    GROUP BY industry
    ORDER BY total DESC
    LIMIT 1
),
top_industry AS (
    SELECT industry FROM industry_totals
),
yearly_counts_for_top AS (
    SELECT y.year, COALESCE(iyc.yearly_count, 0) AS yearly_count
    FROM years y
    CROSS JOIN top_industry ti
    LEFT JOIN industry_yearly_counts iyc ON iyc.industry = ti.industry AND iyc.join_year = y.year
)
SELECT AVG(yearly_count) AS avg_new_unicorns_per_year
FROM yearly_counts_for_top