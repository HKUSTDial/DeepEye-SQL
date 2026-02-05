WITH `customer_spending` AS (
  SELECT `o`.`customerid`, 
         COALESCE(SUM(`od`.`unitprice` * `od`.`quantity`), 0) AS `total_spent`
  FROM `orders` `o`
  LEFT JOIN `order_details` `od` ON `o`.`orderid` = `od`.`orderid`
  WHERE strftime('%Y', `o`.`orderdate`) = '1998'
  GROUP BY `o`.`customerid`
),
`customer_with_group` AS (
  SELECT `cs`.`customerid`, `cs`.`total_spent`, `cgt`.`groupname`
  FROM `customer_spending` `cs`
  JOIN `customergroupthreshold` `cgt` ON `cs`.`total_spent` BETWEEN `cgt`.`rangebottom` AND `cgt`.`rangetop`
)
SELECT `groupname`,
       COUNT(*) AS `customer_count`,
       COUNT(*) * 100.0 / (SELECT COUNT(*) FROM `customer_spending`) AS `percentage`
FROM `customer_with_group`
GROUP BY `groupname`
ORDER BY `groupname`