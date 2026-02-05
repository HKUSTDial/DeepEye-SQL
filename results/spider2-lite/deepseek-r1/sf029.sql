WITH base AS (
    SELECT DISTINCT "DATE", "ASIN", "PROGRAM", "PERIOD", "DISTRIBUTOR_VIEW"
    FROM "AMAZON_VENDOR_ANALYTICS__SAMPLE_DATASET"."PUBLIC"."RETAIL_ANALYTICS_SALES"
    WHERE "PERIOD" = 'DAILY'
      AND "DISTRIBUTOR_VIEW" = 'Manufacturing'
      AND "PROGRAM" = 'Amazon Retail'
      AND CAST("DATE" AS DATE) BETWEEN '2022-01-08' AND '2022-02-06'
    UNION
    SELECT DISTINCT "DATE", "ASIN", "PROGRAM", "PERIOD", "DISTRIBUTOR_VIEW"
    FROM "AMAZON_VENDOR_ANALYTICS__SAMPLE_DATASET"."PUBLIC"."RETAIL_ANALYTICS_TRAFFIC"
    WHERE "PERIOD" = 'DAILY'
      AND "DISTRIBUTOR_VIEW" = 'Manufacturing'
      AND "PROGRAM" = 'Amazon Retail'
      AND CAST("DATE" AS DATE) BETWEEN '2022-01-08' AND '2022-02-06'
    UNION
    SELECT DISTINCT "DATE", "ASIN", "PROGRAM", "PERIOD", "DISTRIBUTOR_VIEW"
    FROM "AMAZON_VENDOR_ANALYTICS__SAMPLE_DATASET"."PUBLIC"."RETAIL_ANALYTICS_INVENTORY"
    WHERE "PERIOD" = 'DAILY'
      AND "DISTRIBUTOR_VIEW" = 'Manufacturing'
      AND "PROGRAM" = 'Amazon Retail'
      AND CAST("DATE" AS DATE) BETWEEN '2022-01-08' AND '2022-02-06'
    UNION
    SELECT DISTINCT "DATE", "ASIN", "PROGRAM", "PERIOD", "DISTRIBUTOR_VIEW"
    FROM "AMAZON_VENDOR_ANALYTICS__SAMPLE_DATASET"."PUBLIC"."RETAIL_ANALYTICS_NET_PPM"
    WHERE "PERIOD" = 'DAILY'
      AND "DISTRIBUTOR_VIEW" = 'Manufacturing'
      AND "PROGRAM" = 'Amazon Retail'
      AND CAST("DATE" AS DATE) BETWEEN '2022-01-08' AND '2022-02-06'
), sales_agg AS (
    SELECT "DATE", "ASIN", "PROGRAM", "PERIOD", "DISTRIBUTOR_VIEW",
           SUM("ORDERED_UNITS") AS total_ordered_units,
           SUM("ORDERED_REVENUE") AS total_ordered_revenue,
           SUM("SHIPPED_UNITS") AS total_shipped_units,
           SUM("SHIPPED_REVENUE") AS total_shipped_revenue
    FROM "AMAZON_VENDOR_ANALYTICS__SAMPLE_DATASET"."PUBLIC"."RETAIL_ANALYTICS_SALES"
    WHERE "PERIOD" = 'DAILY'
      AND "DISTRIBUTOR_VIEW" = 'Manufacturing'
      AND "PROGRAM" = 'Amazon Retail'
      AND CAST("DATE" AS DATE) BETWEEN '2022-01-08' AND '2022-02-06'
    GROUP BY "DATE", "ASIN", "PROGRAM", "PERIOD", "DISTRIBUTOR_VIEW"
), traffic_agg AS (
    SELECT "DATE", "ASIN", "PROGRAM", "PERIOD", "DISTRIBUTOR_VIEW",
           SUM("GLANCE_VIEWS") AS total_glance_views
    FROM "AMAZON_VENDOR_ANALYTICS__SAMPLE_DATASET"."PUBLIC"."RETAIL_ANALYTICS_TRAFFIC"
    WHERE "PERIOD" = 'DAILY'
      AND "DISTRIBUTOR_VIEW" = 'Manufacturing'
      AND "PROGRAM" = 'Amazon Retail'
      AND CAST("DATE" AS DATE) BETWEEN '2022-01-08' AND '2022-02-06'
    GROUP BY "DATE", "ASIN", "PROGRAM", "PERIOD", "DISTRIBUTOR_VIEW"
), inventory_agg AS (
    SELECT "DATE", "ASIN", "PROGRAM", "PERIOD", "DISTRIBUTOR_VIEW",
           AVG("PROCURABLE_PRODUCT_OOS") AS avg_procurable_product_oos,
           SUM("SELLABLE_ON_HAND_UNITS") AS total_sellable_on_hand_units,
           SUM("SELLABLE_ON_HAND_INVENTORY") AS total_sellable_on_hand_inventory,
           SUM("NET_RECEIVED_UNITS") AS total_net_received_units,
           SUM("NET_RECEIVED") AS total_net_received_value,
           SUM("OPEN_PURCHASE_ORDER_QUANTITY") AS total_open_purchase_order_quantity,
           SUM("UNFILLED_CUSTOMER_ORDERED_UNITS") AS total_unfilled_customer_ordered_units,
           AVG("VENDOR_CONFIRMATION_RATE") AS avg_vendor_confirmation_rate,
           AVG("RECEIVE_FILL_RATE") AS avg_receive_fill_rate,
           AVG("SELL_THROUGH_RATE") AS avg_sell_through_rate,
           AVG("OVERALL_VENDOR_LEAD_TIME_DAYS") AS avg_vendor_lead_time
    FROM "AMAZON_VENDOR_ANALYTICS__SAMPLE_DATASET"."PUBLIC"."RETAIL_ANALYTICS_INVENTORY"
    WHERE "PERIOD" = 'DAILY'
      AND "DISTRIBUTOR_VIEW" = 'Manufacturing'
      AND "PROGRAM" = 'Amazon Retail'
      AND CAST("DATE" AS DATE) BETWEEN '2022-01-08' AND '2022-02-06'
    GROUP BY "DATE", "ASIN", "PROGRAM", "PERIOD", "DISTRIBUTOR_VIEW"
), netppm_agg AS (
    SELECT "DATE", "ASIN", "PROGRAM", "PERIOD", "DISTRIBUTOR_VIEW",
           AVG("NET_PPM") AS avg_net_ppm
    FROM "AMAZON_VENDOR_ANALYTICS__SAMPLE_DATASET"."PUBLIC"."RETAIL_ANALYTICS_NET_PPM"
    WHERE "PERIOD" = 'DAILY'
      AND "DISTRIBUTOR_VIEW" = 'Manufacturing'
      AND "PROGRAM" = 'Amazon Retail'
      AND CAST("DATE" AS DATE) BETWEEN '2022-01-08' AND '2022-02-06'
    GROUP BY "DATE", "ASIN", "PROGRAM", "PERIOD", "DISTRIBUTOR_VIEW"
)
SELECT 
    base."DATE",
    base."ASIN",
    base."PROGRAM",
    base."PERIOD",
    base."DISTRIBUTOR_VIEW",
    COALESCE(sales_agg.total_ordered_units, 0) AS total_ordered_units,
    COALESCE(sales_agg.total_ordered_revenue, 0) AS total_ordered_revenue,
    CASE WHEN COALESCE(sales_agg.total_ordered_units, 0) = 0 THEN NULL 
         ELSE sales_agg.total_ordered_revenue / sales_agg.total_ordered_units 
    END AS average_selling_price,
    COALESCE(traffic_agg.total_glance_views, 0) AS glance_views,
    CASE WHEN COALESCE(traffic_agg.total_glance_views, 0) = 0 THEN NULL 
         ELSE sales_agg.total_ordered_units / traffic_agg.total_glance_views 
    END AS conversion_rate,
    COALESCE(sales_agg.total_shipped_units, 0) AS shipped_units,
    COALESCE(sales_agg.total_shipped_revenue, 0) AS shipped_revenue,
    COALESCE(netppm_agg.avg_net_ppm, 0) AS average_net_ppm,
    COALESCE(inventory_agg.avg_procurable_product_oos, 0) AS average_procurable_product_oos,
    COALESCE(inventory_agg.total_sellable_on_hand_units, 0) AS total_on_hand_units,
    COALESCE(inventory_agg.total_sellable_on_hand_inventory, 0) AS total_on_hand_value,
    COALESCE(inventory_agg.total_net_received_units, 0) AS net_received_units,
    COALESCE(inventory_agg.total_net_received_value, 0) AS net_received_value,
    COALESCE(inventory_agg.total_open_purchase_order_quantity, 0) AS open_purchase_order_quantities,
    COALESCE(inventory_agg.total_unfilled_customer_ordered_units, 0) AS unfilled_customer_ordered_units,
    COALESCE(inventory_agg.avg_vendor_confirmation_rate, 0) AS average_vendor_confirmation_rate,
    COALESCE(inventory_agg.avg_receive_fill_rate, 0) AS average_receive_fill_rate,
    COALESCE(inventory_agg.avg_sell_through_rate, 0) AS average_sell_through_rate,
    COALESCE(inventory_agg.avg_vendor_lead_time, 0) AS average_vendor_lead_time
FROM base
LEFT JOIN sales_agg ON base."DATE" = sales_agg."DATE" 
                    AND base."ASIN" = sales_agg."ASIN" 
                    AND base."PROGRAM" = sales_agg."PROGRAM"
                    AND base."PERIOD" = sales_agg."PERIOD"
                    AND base."DISTRIBUTOR_VIEW" = sales_agg."DISTRIBUTOR_VIEW"
LEFT JOIN traffic_agg ON base."DATE" = traffic_agg."DATE" 
                      AND base."ASIN" = traffic_agg."ASIN" 
                      AND base."PROGRAM" = traffic_agg."PROGRAM"
                      AND base."PERIOD" = traffic_agg."PERIOD"
                      AND base."DISTRIBUTOR_VIEW" = traffic_agg."DISTRIBUTOR_VIEW"
LEFT JOIN inventory_agg ON base."DATE" = inventory_agg."DATE" 
                        AND base."ASIN" = inventory_agg."ASIN" 
                        AND base."PROGRAM" = inventory_agg."PROGRAM"
                        AND base."PERIOD" = inventory_agg."PERIOD"
                        AND base."DISTRIBUTOR_VIEW" = inventory_agg."DISTRIBUTOR_VIEW"
LEFT JOIN netppm_agg ON base."DATE" = netppm_agg."DATE" 
                     AND base."ASIN" = netppm_agg."ASIN" 
                     AND base."PROGRAM" = netppm_agg."PROGRAM"
                     AND base."PERIOD" = netppm_agg."PERIOD"
                     AND base."DISTRIBUTOR_VIEW" = netppm_agg."DISTRIBUTOR_VIEW"
ORDER BY base."DATE", base."ASIN"