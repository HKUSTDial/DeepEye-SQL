WITH loss_rate_stats AS (
  SELECT 
    AVG("loss_rate_%") AS avg_loss_rate,
    STDDEV("loss_rate_%") AS std_loss_rate
  FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."VEG_LOSS_RATE_DF"
)
SELECT 
  stats.avg_loss_rate,
  COUNT(CASE WHEN lr."loss_rate_%" < stats.avg_loss_rate - stats.std_loss_rate THEN 1 END) AS count_below_one_std,
  COUNT(CASE WHEN lr."loss_rate_%" > stats.avg_loss_rate + stats.std_loss_rate THEN 1 END) AS count_above_one_std,
  COUNT(CASE WHEN lr."loss_rate_%" BETWEEN stats.avg_loss_rate - stats.std_loss_rate AND stats.avg_loss_rate + stats.std_loss_rate THEN 1 END) AS count_within_one_std
FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."VEG_LOSS_RATE_DF" lr
CROSS JOIN loss_rate_stats stats
GROUP BY stats.avg_loss_rate