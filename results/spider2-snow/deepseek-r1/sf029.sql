WITH sales_agg AS (
    SELECT 
        "DATE",
        "ASIN",
        "PROGRAM",
        "PERIOD",
        "DISTRIBUTOR_VIEW",
        SUM("ORDERED_UNITS") as total_ordered_units,
        SUM("ORDERED_REVENUE") as ordered_revenue,
        SUM("SHIPPED_UNITS") as shipped_units,
        SUM("SHIPPED_REVENUE") as shipped_revenue
    FROM "AMAZON_VENDOR_ANALYTICS__SAMPLE_DATASET"."PUBLIC"."RETAIL_ANALYTICS_SALES"
    WHERE "DATE" BETWEEN DATEADD(day, -29, '2022-02-06') AND '2022-02-06'
        AND "DISTRIBUTOR_VIEW" = 'Manufacturing'
        AND "PERIOD" = 'DAILY'
        AND "PROGRAM" = 'Amazon Retail'
    GROUP BY "DATE", "ASIN", "PROGRAM", "PERIOD", "DISTRIBUTOR_VIEW"
),
traffic_agg AS (
    SELECT 
        "DATE",
        "ASIN",
        "PROGRAM",
        "PERIOD",
        "DISTRIBUTOR_VIEW",
        SUM("GLANCE_VIEWS") as glance_views
    FROM "AMAZON_VENDOR_ANALYTICS__SAMPLE_DATASET"."PUBLIC"."RETAIL_ANALYTICS_TRAFFIC"
    WHERE "DATE" BETWEEN DATEADD(day, -29, '2022-02-06') AND '2022-02-06'
        AND "DISTRIBUTOR_VIEW" = 'Manufacturing'
        AND "PERIOD" = 'DAILY'
        AND "PROGRAM" = 'Amazon Retail'
    GROUP BY "DATE", "ASIN", "PROGRAM", "PERIOD", "DISTRIBUTOR_VIEW"
),
inventory_agg AS (
    SELECT 
        "DATE",
        "ASIN",
        "PROGRAM",
        "PERIOD",
        "DISTRIBUTOR_VIEW",
        AVG("PROCURABLE_PRODUCT_OOS") as avg_procurable_product_oos,
        SUM("SELLABLE_ON_HAND_UNITS" + "UNSELLABLE_ON_HAND_UNITS") as total_on_hand_units,
        SUM("SELLABLE_ON_HAND_INVENTORY" + "UNSELLABLE_ON_HAND_INVENTORY") as total_on_hand_value,
        SUM("NET_RECEIVED_UNITS") as net_received_units,
        SUM("NET_RECEIVED") as net_received_value,
        SUM("OPEN_PURCHASE_ORDER_QUANTITY") as open_purchase_order_quantities,
        SUM("UNFILLED_CUSTOMER_ORDERED_UNITS") as unfilled_customer_ordered_units,
        AVG("VENDOR_CONFIRMATION_RATE") as avg_vendor_confirmation_rate,
        AVG("RECEIVE_FILL_RATE") as avg_receive_fill_rate,
        AVG("SELL_THROUGH_RATE") as avg_sell_through_rate,
        AVG("OVERALL_VENDOR_LEAD_TIME_DAYS") as avg_vendor_lead_time
    FROM "AMAZON_VENDOR_ANALYTICS__SAMPLE_DATASET"."PUBLIC"."RETAIL_ANALYTICS_INVENTORY"
    WHERE "DATE" BETWEEN DATEADD(day, -29, '2022-02-06') AND '2022-02-06'
        AND "DISTRIBUTOR_VIEW" = 'Manufacturing'
        AND "PERIOD" = 'DAILY'
        AND "PROGRAM" = 'Amazon Retail'
    GROUP BY "DATE", "ASIN", "PROGRAM", "PERIOD", "DISTRIBUTOR_VIEW"
),
net_ppm_agg AS (
    SELECT 
        "DATE",
        "ASIN",
        "PROGRAM",
        "PERIOD",
        "DISTRIBUTOR_VIEW",
        AVG("NET_PPM") as avg_net_ppm
    FROM "AMAZON_VENDOR_ANALYTICS__SAMPLE_DATASET"."PUBLIC"."RETAIL_ANALYTICS_NET_PPM"
    WHERE "DATE" BETWEEN DATEADD(day, -29, '2022-02-06') AND '2022-02-06'
        AND "DISTRIBUTOR_VIEW" = 'Manufacturing'
        AND "PERIOD" = 'DAILY'
        AND "PROGRAM" = 'Amazon Retail'
    GROUP BY "DATE", "ASIN", "PROGRAM", "PERIOD", "DISTRIBUTOR_VIEW"
)
SELECT
    COALESCE(s."DATE", t."DATE", i."DATE", n."DATE") as "DATE",
    COALESCE(s."ASIN", t."ASIN", i."ASIN", n."ASIN") as "ASIN",
    COALESCE(s."PROGRAM", t."PROGRAM", i."PROGRAM", n."PROGRAM") as "PROGRAM",
    COALESCE(s."PERIOD", t."PERIOD", i."PERIOD", n."PERIOD") as "PERIOD",
    COALESCE(s."DISTRIBUTOR_VIEW", t."DISTRIBUTOR_VIEW", i."DISTRIBUTOR_VIEW", n."DISTRIBUTOR_VIEW") as "DISTRIBUTOR_VIEW",
    COALESCE(s.total_ordered_units, 0) as total_ordered_units,
    COALESCE(s.ordered_revenue, 0) as ordered_revenue,
    CASE WHEN COALESCE(s.total_ordered_units, 0) > 0 THEN COALESCE(s.ordered_revenue, 0) / s.total_ordered_units ELSE 0 END as average_selling_price,
    COALESCE(t.glance_views, 0) as glance_views,
    CASE WHEN COALESCE(t.glance_views, 0) > 0 THEN COALESCE(s.total_ordered_units, 0) / t.glance_views ELSE 0 END as conversion_rate,
    COALESCE(s.shipped_units, 0) as shipped_units,
    COALESCE(s.shipped_revenue, 0) as shipped_revenue,
    COALESCE(n.avg_net_ppm, 0) as avg_net_ppm,
    COALESCE(i.avg_procurable_product_oos, 0) as avg_procurable_product_oos,
    COALESCE(i.total_on_hand_units, 0) as total_on_hand_units,
    COALESCE(i.total_on_hand_value, 0) as total_on_hand_value,
    COALESCE(i.net_received_units, 0) as net_received_units,
    COALESCE(i.net_received_value, 0) as net_received_value,
    COALESCE(i.open_purchase_order_quantities, 0) as open_purchase_order_quantities,
    COALESCE(i.unfilled_customer_ordered_units, 0) as unfilled_customer_ordered_units,
    COALESCE(i.avg_vendor_confirmation_rate, 0) as avg_vendor_confirmation_rate,
    COALESCE(i.avg_receive_fill_rate, 0) as avg_receive_fill_rate,
    COALESCE(i.avg_sell_through_rate, 0) as avg_sell_through_rate,
    COALESCE(i.avg_vendor_lead_time, 0) as avg_vendor_lead_time
FROM sales_agg s
FULL OUTER JOIN traffic_agg t 
    ON s."DATE" = t."DATE" 
    AND s."ASIN" = t."ASIN"
    AND s."PROGRAM" = t."PROGRAM"
    AND s."PERIOD" = t."PERIOD"
    AND s."DISTRIBUTOR_VIEW" = t."DISTRIBUTOR_VIEW"
FULL OUTER JOIN inventory_agg i
    ON COALESCE(s."DATE", t."DATE") = i."DATE"
    AND COALESCE(s."ASIN", t."ASIN") = i."ASIN"
    AND COALESCE(s."PROGRAM", t."PROGRAM") = i."PROGRAM"
    AND COALESCE(s."PERIOD", t."PERIOD") = i."PERIOD"
    AND COALESCE(s."DISTRIBUTOR_VIEW", t."DISTRIBUTOR_VIEW") = i."DISTRIBUTOR_VIEW"
FULL OUTER JOIN net_ppm_agg n
    ON COALESCE(s."DATE", t."DATE", i."DATE") = n."DATE"
    AND COALESCE(s."ASIN", t."ASIN", i."ASIN") = n."ASIN"
    AND COALESCE(s."PROGRAM", t."PROGRAM", i."PROGRAM") = n."PROGRAM"
    AND COALESCE(s."PERIOD", t."PERIOD", i."PERIOD") = n."PERIOD"
    AND COALESCE(s."DISTRIBUTOR_VIEW", t."DISTRIBUTOR_VIEW", i."DISTRIBUTOR_VIEW") = n."DISTRIBUTOR_VIEW"
ORDER BY "DATE", "ASIN"