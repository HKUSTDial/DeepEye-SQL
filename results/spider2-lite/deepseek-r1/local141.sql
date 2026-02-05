SELECT 
    s.salespersonid AS businessentityid,
    s.year,
    s.total_sales,
    q.annual_quota,
    (s.total_sales - q.annual_quota) AS difference
FROM (
    SELECT 
        salespersonid, 
        strftime('%Y', orderdate) AS year, 
        SUM(subtotal) AS total_sales
    FROM salesorderheader
    GROUP BY salespersonid, strftime('%Y', orderdate)
) s
INNER JOIN (
    SELECT 
        BusinessEntityID, 
        strftime('%Y', QuotaDate) AS year, 
        SUM(SalesQuota) AS annual_quota
    FROM SalesPersonQuotaHistory
    GROUP BY BusinessEntityID, strftime('%Y', QuotaDate)
) q ON s.salespersonid = q.BusinessEntityID AND s.year = q.year
ORDER BY s.salespersonid, s.year