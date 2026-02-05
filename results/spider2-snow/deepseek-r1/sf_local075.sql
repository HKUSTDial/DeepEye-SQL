SELECT
  "ph"."product_id",
  "ph"."page_name",
  "ph"."product_category",
  COUNT(CASE WHEN "e"."event_type" = 1 THEN 1 END) AS "view_count",
  COUNT(CASE WHEN "ei"."event_name" = 'Add to Cart' THEN 1 END) AS "add_to_cart_count",
  COUNT(CASE WHEN "ei"."event_name" = 'Purchase' THEN 1 END) AS "purchase_count",
  GREATEST(COUNT(CASE WHEN "ei"."event_name" = 'Add to Cart' THEN 1 END) - COUNT(CASE WHEN "ei"."event_name" = 'Purchase' THEN 1 END), 0) AS "left_in_cart_without_purchase"
FROM
  "BANK_SALES_TRADING"."BANK_SALES_TRADING"."SHOPPING_CART_EVENTS" AS "e"
LEFT JOIN
  "BANK_SALES_TRADING"."BANK_SALES_TRADING"."SHOPPING_CART_EVENT_IDENTIFIER" AS "ei"
ON
  "e"."event_type" = "ei"."event_type"
INNER JOIN
  "BANK_SALES_TRADING"."BANK_SALES_TRADING"."SHOPPING_CART_PAGE_HIERARCHY" AS "ph"
ON
  "e"."page_id" = "ph"."page_id"
WHERE
  "ph"."page_id" NOT IN (1, 2, 12, 13)
  AND "ph"."product_id" IS NOT NULL
  AND "ph"."product_id"::STRING != 'NaN'
GROUP BY
  "ph"."product_id",
  "ph"."page_name",
  "ph"."product_category"
ORDER BY
  "ph"."product_id"