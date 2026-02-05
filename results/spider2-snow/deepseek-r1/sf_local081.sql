WITH customers_1998 AS (
    SELECT o."customerid", SUM(od."unitprice" * od."quantity") AS total_spent
    FROM "NORTHWIND"."NORTHWIND"."ORDERS" o
    JOIN "NORTHWIND"."NORTHWIND"."ORDER_DETAILS" od ON o."orderid" = od."orderid"
    WHERE EXTRACT(YEAR FROM TO_DATE(o."orderdate")) = 1998
    GROUP BY o."customerid"
), customers_with_group AS (
    SELECT c."customerid", c.total_spent, cgt."groupname"
    FROM customers_1998 c
    LEFT JOIN "NORTHWIND"."NORTHWIND"."CUSTOMERGROUPTHRESHOLD" cgt ON c.total_spent >= cgt."rangebottom" AND c.total_spent < cgt."rangetop"
)
SELECT "groupname", COUNT("customerid") AS customer_count, COUNT("customerid") * 100.0 / (SELECT COUNT(*) FROM customers_1998) AS percentage
FROM customers_with_group
WHERE "groupname" IS NOT NULL
GROUP BY "groupname"