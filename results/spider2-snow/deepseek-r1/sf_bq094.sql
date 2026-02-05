WITH committees_with_candidates AS (
    SELECT "cmte_id", COUNT(DISTINCT "cand_id") AS "num_candidates"
    FROM "FEC"."FEC"."CANDIDATE_COMMITTEE_2016"
    GROUP BY "cmte_id"
),
small_dollar_totals AS (
    SELECT "cmte_id", SUM("transaction_amt") AS "total_small_dollar"
    FROM "FEC"."FEC"."INDIV16"
    WHERE "entity_tp" = 'IND'
      AND "transaction_amt" > 0
      AND "transaction_amt" < 200
    GROUP BY "cmte_id"
    HAVING SUM("transaction_amt") > 0 AND SUM("transaction_amt") < 200
),
candidate_names_per_committee AS (
    SELECT cc."cmte_id", 
           LISTAGG(DISTINCT cn."cand_name", ', ') WITHIN GROUP (ORDER BY cn."cand_name") AS "candidate_names"
    FROM "FEC"."FEC"."CANDIDATE_COMMITTEE_2016" cc
    INNER JOIN "FEC"."FEC"."CANDIDATE_2016" cn ON cc."cand_id" = cn."cand_id"
    GROUP BY cc."cmte_id"
)
SELECT 
    c."cmte_nm" AS "committee_name",
    cc."num_candidates",
    cn."candidate_names",
    s."total_small_dollar"
FROM small_dollar_totals s
INNER JOIN committees_with_candidates cc ON s."cmte_id" = cc."cmte_id"
INNER JOIN "FEC"."FEC"."COMMITTEE_2016" c ON s."cmte_id" = c."cmte_id"
LEFT JOIN candidate_names_per_committee cn ON s."cmte_id" = cn."cmte_id"
ORDER BY c."cmte_nm"