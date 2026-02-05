WITH small_dollar_totals AS (
  SELECT `cmte_id`, SUM(`transaction_amt`) AS total_small_dollar
  FROM `bigquery-public-data.fec.indiv16`
  WHERE `transaction_amt` > 0 AND `transaction_amt` < 200
  GROUP BY `cmte_id`
  HAVING SUM(`transaction_amt`) > 0
),
committees_with_candidates AS (
  SELECT DISTINCT `cmte_id`
  FROM `bigquery-public-data.fec.ccl16`
),
qualified_committees AS (
  SELECT sdt.`cmte_id`, sdt.`total_small_dollar`
  FROM small_dollar_totals sdt
  INNER JOIN committees_with_candidates cwc ON sdt.`cmte_id` = cwc.`cmte_id`
),
candidate_agg AS (
  SELECT 
    ccl.`cmte_id`,
    COUNT(DISTINCT ccl.`cand_id`) AS num_candidates,
    STRING_AGG(DISTINCT cn.`cand_name`, ', ' ORDER BY cn.`cand_name`) AS candidate_names
  FROM `bigquery-public-data.fec.ccl16` ccl
  INNER JOIN `bigquery-public-data.fec.candidate_2016` cn ON ccl.`cand_id` = cn.`cand_id`
  WHERE ccl.`cmte_id` IN (SELECT `cmte_id` FROM qualified_committees)
  GROUP BY ccl.`cmte_id`
)
SELECT 
  cm.`cmte_nm` AS committee_name,
  ca.`num_candidates`,
  ca.`candidate_names`,
  qc.`total_small_dollar`
FROM qualified_committees qc
INNER JOIN `bigquery-public-data.fec.committee_2016` cm ON qc.`cmte_id` = cm.`cmte_id`
INNER JOIN candidate_agg ca ON qc.`cmte_id` = ca.`cmte_id`
ORDER BY cm.`cmte_nm`