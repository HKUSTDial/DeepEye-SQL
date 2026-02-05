WITH filtered_jobs AS (
  SELECT "job_id", "salary_year_avg"
  FROM "CITY_LEGISLATION"."CITY_LEGISLATION"."JOB_POSTINGS_FACT"
  WHERE "job_title_short" = 'Data Analyst'
    AND "salary_year_avg" IS NOT NULL
    AND "job_work_from_home" = 1
),
top_skills AS (
  SELECT sj."skill_id", COUNT(*) as frequency
  FROM filtered_jobs fj
  JOIN "CITY_LEGISLATION"."CITY_LEGISLATION"."SKILLS_JOB_DIM" sj ON fj."job_id" = sj."job_id"
  GROUP BY sj."skill_id"
  ORDER BY frequency DESC
  LIMIT 3
),
relevant_jobs AS (
  SELECT DISTINCT fj."job_id", fj."salary_year_avg"
  FROM filtered_jobs fj
  JOIN "CITY_LEGISLATION"."CITY_LEGISLATION"."SKILLS_JOB_DIM" sj ON fj."job_id" = sj."job_id"
  WHERE sj."skill_id" IN (SELECT "skill_id" FROM top_skills)
)
SELECT AVG("salary_year_avg") as overall_average_salary
FROM relevant_jobs