WITH annual_sales AS (
  SELECT 
    sp."businessentityid" as salesperson_id,
    EXTRACT(YEAR FROM TO_DATE(soh."orderdate")) as year,
    SUM(soh."subtotal") as total_sales
  FROM "ADVENTUREWORKS"."ADVENTUREWORKS"."SALESORDERHEADER" soh
  JOIN "ADVENTUREWORKS"."ADVENTUREWORKS"."SALESPERSON" sp
    ON soh."salespersonid" = sp."businessentityid"::TEXT
  GROUP BY sp."businessentityid", year
),
annual_quota AS (
  SELECT 
    "BusinessEntityID" as salesperson_id,
    EXTRACT(YEAR FROM TO_DATE("QuotaDate")) as year,
    SUM("SalesQuota") as total_quota
  FROM "ADVENTUREWORKS"."ADVENTUREWORKS"."SALESPERSONQUOTAHISTORY"
  GROUP BY "BusinessEntityID", year
)
SELECT 
  COALESCE(s.salesperson_id, q.salesperson_id) as salesperson_id,
  COALESCE(s.year, q.year) as year,
  s.total_sales,
  q.total_quota,
  (s.total_sales - q.total_quota) as difference
FROM annual_sales s
FULL OUTER JOIN annual_quota q 
  ON s.salesperson_id = q.salesperson_id AND s.year = q.year
ORDER BY salesperson_id, year