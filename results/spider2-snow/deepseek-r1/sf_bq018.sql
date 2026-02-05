WITH daily_totals AS (
  SELECT 
    "date",
    SUM("new_confirmed") AS national_new,
    SUM("cumulative_confirmed") AS national_cumulative
  FROM "COVID19_OPEN_DATA"."COVID19_OPEN_DATA"."COVID19_OPEN_DATA"
  WHERE "country_code" = 'US'
  GROUP BY "date"
),
growth_rates AS (
  SELECT 
    "date",
    national_new,
    national_cumulative,
    CASE 
      WHEN national_cumulative - national_new > 0 
      THEN national_new / (national_cumulative - national_new)
      ELSE NULL 
    END AS growth_rate
  FROM daily_totals
)
SELECT 
  TO_CHAR("date", 'MM-DD') AS day_month
FROM growth_rates
WHERE EXTRACT(MONTH FROM "date") IN (3, 4)
ORDER BY growth_rate DESC NULLS LAST
LIMIT 1