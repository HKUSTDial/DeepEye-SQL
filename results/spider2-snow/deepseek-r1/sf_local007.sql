WITH player_dates AS (
  SELECT 
    "PLAYER_ID",
    TRY_TO_DATE("DEBUT") AS debut_date,
    TRY_TO_DATE("FINAL_GAME") AS final_date
  FROM "BASEBALL"."BASEBALL"."PLAYER"
  WHERE "DEBUT" IS NOT NULL AND "FINAL_GAME" IS NOT NULL
),
player_years AS (
  SELECT 
    "PLAYER_ID",
    debut_date,
    final_date,
    DATEDIFF('year', debut_date, final_date) AS years
  FROM player_dates
),
player_months AS (
  SELECT 
    "PLAYER_ID",
    debut_date,
    final_date,
    years,
    DATEDIFF('month', DATEADD('year', years, debut_date), final_date) AS months
  FROM player_years
),
player_days AS (
  SELECT 
    "PLAYER_ID",
    debut_date,
    final_date,
    years,
    months,
    DATEDIFF('day', DATEADD('month', months, DATEADD('year', years, debut_date)), final_date) AS days
  FROM player_months
),
career_span_calc AS (
  SELECT 
    "PLAYER_ID",
    ROUND(years, 2) + ROUND(months / 12.0, 2) + ROUND(days / 365.0, 2) AS career_span
  FROM player_days
)
SELECT ROUND(AVG(career_span), 2) AS avg_career_span_years
FROM career_span_calc