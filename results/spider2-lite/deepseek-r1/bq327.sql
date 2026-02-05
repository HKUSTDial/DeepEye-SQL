SELECT COUNT(DISTINCT `indicator_code`)
FROM `bigquery-public-data.world_bank_intl_debt.international_debt`
WHERE `country_code` = 'RUS' AND `value` = 0 AND `value` IS NOT NULL