WITH filtered_jobs AS (
    SELECT job_id, salary_year_avg
    FROM `job_postings_fact`
    WHERE `job_title_short` = 'Data Analyst'
      AND `job_work_from_home` = 1
      AND `salary_year_avg` IS NOT NULL
),
top_skills AS (
    SELECT sj.skill_id
    FROM filtered_jobs f
    INNER JOIN `skills_job_dim` sj ON f.job_id = sj.job_id
    GROUP BY sj.skill_id
    ORDER BY COUNT(*) DESC
    LIMIT 3
),
jobs_with_top_skills AS (
    SELECT DISTINCT f.job_id, f.salary_year_avg
    FROM filtered_jobs f
    INNER JOIN `skills_job_dim` sj ON f.job_id = sj.job_id
    WHERE sj.skill_id IN (SELECT skill_id FROM top_skills)
)
SELECT AVG(salary_year_avg) AS overall_avg_salary
FROM jobs_with_top_skills