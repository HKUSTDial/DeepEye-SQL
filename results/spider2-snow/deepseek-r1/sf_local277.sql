WITH monthly_sales_t AS (
    SELECT 
        "product_id",
        "mth",
        "qty" AS sales,
        ROW_NUMBER() OVER (PARTITION BY "product_id" ORDER BY "mth") AS t
    FROM "ORACLE_SQL"."ORACLE_SQL"."MONTHLY_SALES"
    WHERE "product_id" IN (4160, 7790)
        AND "mth" >= '2016-01-01'
        AND "mth" <= '2018-12-01'
),
cma_data AS (
    SELECT 
        "product_id",
        t,
        sales,
        SUM(sales) OVER (PARTITION BY "product_id" ORDER BY t ROWS BETWEEN 5 PRECEDING AND 6 FOLLOWING) AS sum1,
        SUM(sales) OVER (PARTITION BY "product_id" ORDER BY t ROWS BETWEEN 6 PRECEDING AND 5 FOLLOWING) AS sum2
    FROM monthly_sales_t
),
cma_calc AS (
    SELECT 
        "product_id",
        t,
        sales,
        (sum1 + sum2) / 24.0 AS cma
    FROM cma_data
    WHERE t BETWEEN 7 AND 30
),
ratios AS (
    SELECT 
        "product_id",
        t,
        sales,
        cma,
        sales / NULLIF(cma, 0) AS ratio
    FROM cma_calc
),
seasonal_indices AS (
    SELECT 
        "product_id",
        MOD(t - 1, 12) + 1 AS calendar_month,
        AVG(ratio) AS seasonal_index
    FROM ratios
    GROUP BY "product_id", MOD(t - 1, 12) + 1
),
all_months AS (
    SELECT 
        mst."product_id",
        mst.t,
        mst.sales,
        si.seasonal_index
    FROM monthly_sales_t mst
    LEFT JOIN seasonal_indices si 
        ON mst."product_id" = si."product_id" 
        AND MOD(mst.t - 1, 12) + 1 = si.calendar_month
),
deseasonalized AS (
    SELECT 
        "product_id",
        t,
        sales,
        seasonal_index,
        sales / NULLIF(seasonal_index, 0) AS deseason_sales
    FROM all_months
),
regression_data AS (
    SELECT 
        "product_id",
        t,
        deseason_sales
    FROM deseasonalized
    WHERE t <= 24
),
regression_coeffs AS (
    SELECT 
        "product_id",
        REGR_SLOPE(deseason_sales, t) AS slope,
        REGR_INTERCEPT(deseason_sales, t) AS intercept
    FROM regression_data
    GROUP BY "product_id"
),
forecast_t AS (
    SELECT 
        rc."product_id",
        t.t_val AS t,
        rc.intercept + rc.slope * t.t_val AS deseason_forecast
    FROM regression_coeffs rc
    CROSS JOIN (SELECT SEQ4() + 25 AS t_val FROM TABLE(GENERATOR(ROWCOUNT => 12))) t
),
forecast_with_season AS (
    SELECT 
        ft."product_id",
        ft.t,
        ft.deseason_forecast,
        si.seasonal_index,
        ft.deseason_forecast * si.seasonal_index AS forecast_sales
    FROM forecast_t ft
    LEFT JOIN seasonal_indices si 
        ON ft."product_id" = si."product_id" 
        AND MOD(ft.t - 1, 12) + 1 = si.calendar_month
),
annual_forecast AS (
    SELECT 
        "product_id",
        SUM(forecast_sales) AS annual_forecast_sales
    FROM forecast_with_season
    GROUP BY "product_id"
)
SELECT AVG(annual_forecast_sales) AS avg_forecasted_annual_sales
FROM annual_forecast