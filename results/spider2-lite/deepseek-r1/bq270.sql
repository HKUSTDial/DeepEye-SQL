SELECT
  EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', date)) AS year,
  EXTRACT(MONTH FROM PARSE_DATE('%Y%m%d', date)) AS month,
  COUNTIF(hits.eCommerceAction.action_type = '2' AND (hits_product.isImpression IS NULL OR hits_product.isImpression = FALSE)) AS product_detail_views,
  COUNTIF(hits.eCommerceAction.action_type = '3' AND (hits_product.isImpression IS NULL OR hits_product.isImpression = FALSE)) AS add_to_cart_count,
  COUNTIF(hits.eCommerceAction.action_type = '6' AND (hits_product.isImpression IS NULL OR hits_product.isImpression = FALSE)) AS purchase_count,
  SAFE_DIVIDE(COUNTIF(hits.eCommerceAction.action_type = '3' AND (hits_product.isImpression IS NULL OR hits_product.isImpression = FALSE)), COUNTIF(hits.eCommerceAction.action_type = '2' AND (hits_product.isImpression IS NULL OR hits_product.isImpression = FALSE))) * 100 AS add_to_cart_conversion_rate,
  SAFE_DIVIDE(COUNTIF(hits.eCommerceAction.action_type = '6' AND (hits_product.isImpression IS NULL OR hits_product.isImpression = FALSE)), COUNTIF(hits.eCommerceAction.action_type = '2' AND (hits_product.isImpression IS NULL OR hits_product.isImpression = FALSE))) * 100 AS purchase_conversion_rate
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) AS hits,
  UNNEST(hits.product) AS hits_product
WHERE
  _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
GROUP BY
  year, month
ORDER BY
  year, month