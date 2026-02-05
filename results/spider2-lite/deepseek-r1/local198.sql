WITH country_totals AS (
    SELECT c.`Country`, SUM(i.`Total`) AS total_sales
    FROM `customers` c
    INNER JOIN `invoices` i ON c.`CustomerId` = i.`CustomerId`
    GROUP BY c.`Country`
    HAVING COUNT(DISTINCT c.`CustomerId`) > 4
)
SELECT AVG(total_sales) AS median_total_sales
FROM (
    SELECT total_sales,
           ROW_NUMBER() OVER (ORDER BY total_sales) AS row_num,
           COUNT(*) OVER () AS total_count
    FROM country_totals
) 
WHERE row_num IN ((total_count + 1) / 2, (total_count + 2) / 2)