SELECT submission.stprba, SUM(numbers.value * SAFE.POWER(10, -numbers.decimal)) / 1000000000 AS total_revenue_billions
FROM `bigquery-public-data.sec_quarterly_financials.submission` AS submission
JOIN `bigquery-public-data.sec_quarterly_financials.numbers` AS numbers ON submission.submission_number = numbers.submission_number
WHERE submission.fiscal_year = 2016 AND submission.countryba = 'US' AND submission.stprba IS NOT NULL AND submission.stprba != '' AND numbers.measure_tag IN ('Revenues', 'SalesRevenueNet', 'SalesRevenueGoodsNet') AND numbers.number_of_quarters = 4 AND numbers.iprx = 0 AND numbers.coregistrant IS NULL AND numbers.units = 'USD' AND numbers.decimal != 32767 AND numbers.decimal BETWEEN -100 AND 100 AND numbers.num_dimensions = 0
GROUP BY submission.stprba
ORDER BY total_revenue_billions DESC
LIMIT 1