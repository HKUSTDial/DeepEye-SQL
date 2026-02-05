WITH region_summary AS (
  SELECT
    r."id" AS "REGION_ID",
    r."name" AS "REGION_NAME",
    COUNT(o."id") AS "NUMBER_OF_ORDERS",
    COALESCE(SUM(o."total_amt_usd"), 0) AS "TOTAL_SALES_AMOUNT"
  FROM "EDUCATION_BUSINESS"."EDUCATION_BUSINESS"."WEB_REGION" r
  LEFT JOIN "EDUCATION_BUSINESS"."EDUCATION_BUSINESS"."WEB_SALES_REPS" s ON r."id" = s."region_id"
  LEFT JOIN "EDUCATION_BUSINESS"."EDUCATION_BUSINESS"."WEB_ACCOUNTS" a ON s."id" = a."sales_rep_id"
  LEFT JOIN "EDUCATION_BUSINESS"."EDUCATION_BUSINESS"."WEB_ORDERS" o ON a."id" = o."account_id"
  GROUP BY r."id", r."name"
),
rep_sales AS (
  SELECT
    s."region_id" AS "REGION_ID",
    s."id" AS "SALES_REP_ID",
    s."name" AS "SALES_REP_NAME",
    COALESCE(SUM(o."total_amt_usd"), 0) AS "REP_TOTAL_SALES"
  FROM "EDUCATION_BUSINESS"."EDUCATION_BUSINESS"."WEB_SALES_REPS" s
  LEFT JOIN "EDUCATION_BUSINESS"."EDUCATION_BUSINESS"."WEB_ACCOUNTS" a ON s."id" = a."sales_rep_id"
  LEFT JOIN "EDUCATION_BUSINESS"."EDUCATION_BUSINESS"."WEB_ORDERS" o ON a."id" = o."account_id"
  GROUP BY s."region_id", s."id", s."name"
),
max_rep_sales AS (
  SELECT
    "REGION_ID",
    MAX("REP_TOTAL_SALES") AS "MAX_REP_SALES"
  FROM rep_sales
  GROUP BY "REGION_ID"
),
top_reps AS (
  SELECT
    rep."REGION_ID",
    rep."SALES_REP_ID",
    rep."SALES_REP_NAME",
    rep."REP_TOTAL_SALES"
  FROM rep_sales rep
  INNER JOIN max_rep_sales max ON rep."REGION_ID" = max."REGION_ID" AND rep."REP_TOTAL_SALES" = max."MAX_REP_SALES"
)
SELECT
  rs."REGION_NAME",
  rs."NUMBER_OF_ORDERS",
  rs."TOTAL_SALES_AMOUNT",
  tr."SALES_REP_NAME",
  tr."REP_TOTAL_SALES" AS "TOP_SALES_REP_AMOUNT"
FROM region_summary rs
LEFT JOIN top_reps tr ON rs."REGION_ID" = tr."REGION_ID"
ORDER BY rs."REGION_NAME", tr."SALES_REP_NAME"