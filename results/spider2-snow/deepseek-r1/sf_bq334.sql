SELECT
  m1."year",
  (m1."merged_avg" - m2."trans_avg") AS "difference"
FROM
  (SELECT
     EXTRACT(YEAR FROM TO_TIMESTAMP("block_timestamp" / 1000000)) AS "year",
     AVG("value") AS "merged_avg"
   FROM "CRYPTO"."CRYPTO_BITCOIN"."OUTPUTS"
   GROUP BY "year") m1
INNER JOIN
  (SELECT
     EXTRACT(YEAR FROM TO_TIMESTAMP("block_timestamp" / 1000000)) AS "year",
     AVG("output_value") AS "trans_avg"
   FROM "CRYPTO"."CRYPTO_BITCOIN"."TRANSACTIONS"
   GROUP BY "year") m2
ON m1."year" = m2."year"
ORDER BY m1."year"