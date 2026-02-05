WITH all_dates AS (
  SELECT DATEADD('DAY', SEQ4(), '2017-01-01') AS hist_date
  FROM TABLE(GENERATOR(ROWCOUNT => 1000))
  WHERE hist_date <= '2018-08-29'
),
daily_sales AS (
  SELECT
    DATE("E_COMMERCE"."E_COMMERCE"."ORDERS"."order_purchase_timestamp") AS sale_date,
    SUM("E_COMMERCE"."E_COMMERCE"."ORDER_ITEMS"."price") AS total_sales
  FROM "E_COMMERCE"."E_COMMERCE"."ORDER_ITEMS"
  JOIN "E_COMMERCE"."E_COMMERCE"."ORDERS"
    ON "E_COMMERCE"."E_COMMERCE"."ORDER_ITEMS"."order_id" = "E_COMMERCE"."E_COMMERCE"."ORDERS"."order_id"
  JOIN "E_COMMERCE"."E_COMMERCE"."PRODUCTS"
    ON "E_COMMERCE"."E_COMMERCE"."ORDER_ITEMS"."product_id" = "E_COMMERCE"."E_COMMERCE"."PRODUCTS"."product_id"
  JOIN "E_COMMERCE"."E_COMMERCE"."PRODUCT_CATEGORY_NAME_TRANSLATION"
    ON "E_COMMERCE"."E_COMMERCE"."PRODUCTS"."product_category_name" = "E_COMMERCE"."E_COMMERCE"."PRODUCT_CATEGORY_NAME_TRANSLATION"."product_category_name"
  WHERE "E_COMMERCE"."E_COMMERCE"."PRODUCT_CATEGORY_NAME_TRANSLATION"."product_category_name_english" = 'toys'
    AND DATE("E_COMMERCE"."E_COMMERCE"."ORDERS"."order_purchase_timestamp") BETWEEN '2017-01-01' AND '2018-08-29'
  GROUP BY sale_date
),
historical_data AS (
  SELECT
    all_dates.hist_date AS sale_date,
    COALESCE(daily_sales.total_sales, 0) AS total_sales
  FROM all_dates
  LEFT JOIN daily_sales ON all_dates.hist_date = daily_sales.sale_date
),
day_numbers AS (
  SELECT
    sale_date,
    DATEDIFF('DAY', '2017-01-01', sale_date) AS day_num,
    total_sales
  FROM historical_data
),
regression AS (
  SELECT
    REGR_SLOPE(total_sales, day_num) AS slope,
    REGR_INTERCEPT(total_sales, day_num) AS intercept
  FROM day_numbers
),
prediction_dates AS (
  SELECT DATEADD('DAY', SEQ4(), '2018-12-03') AS pred_date
  FROM TABLE(GENERATOR(ROWCOUNT => 8))
),
predicted_sales AS (
  SELECT
    pred_date,
    (SELECT intercept FROM regression) + (SELECT slope FROM regression) * DATEDIFF('DAY', '2017-01-01', pred_date) AS predicted
  FROM prediction_dates
),
moving_averages AS (
  SELECT
    pred_date,
    AVG(predicted) OVER (ORDER BY pred_date ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS sym_moving_avg
  FROM predicted_sales
)
SELECT SUM(sym_moving_avg) AS total_moving_avg_sum
FROM moving_averages
WHERE pred_date BETWEEN '2018-12-05' AND '2018-12-08'