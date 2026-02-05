WITH wholesale_loss_rates AS (
  SELECT lr.`loss_rate_%` AS loss_rate
  FROM `veg_whsle_df` w
  INNER JOIN `veg_loss_rate_df` lr ON w.`item_code` = lr.`item_code`
),
stats AS (
  SELECT 
    AVG(loss_rate) AS avg_loss_rate,
    SQRT(AVG(loss_rate * loss_rate) - AVG(loss_rate) * AVG(loss_rate)) AS std_loss_rate
  FROM wholesale_loss_rates
)
SELECT 
  s.avg_loss_rate,
  COUNT(CASE WHEN wlr.loss_rate < s.avg_loss_rate - s.std_loss_rate THEN 1 END) AS count_below,
  COUNT(CASE WHEN wlr.loss_rate BETWEEN s.avg_loss_rate - s.std_loss_rate AND s.avg_loss_rate + s.std_loss_rate THEN 1 END) AS count_within,
  COUNT(CASE WHEN wlr.loss_rate > s.avg_loss_rate + s.std_loss_rate THEN 1 END) AS count_above
FROM wholesale_loss_rates wlr, stats s;