WITH variant_calls AS (
  SELECT 
    v."reference_name",
    v."start",
    v."end",
    call_obj.value:sample_id::STRING AS sample_id,
    call_obj.value:genotype[0]::INT AS allele1,
    call_obj.value:genotype[1]::INT AS allele2
  FROM "_1000_GENOMES"."_1000_GENOMES"."VARIANTS" AS v,
  LATERAL FLATTEN(INPUT => v."call") AS call_obj
  WHERE v."reference_name" = '12'
),
sample_alleles AS (
  SELECT 
    vc."start",
    vc."end",
    si."Super_Population",
    CASE WHEN si."Super_Population" = 'EAS' THEN 1 ELSE 0 END AS is_case,
    (CASE WHEN vc.allele1 = 0 THEN 1 ELSE 0 END + CASE WHEN vc.allele2 = 0 THEN 1 ELSE 0 END) AS ref_count,
    (CASE WHEN vc.allele1 != 0 THEN 1 ELSE 0 END + CASE WHEN vc.allele2 != 0 THEN 1 ELSE 0 END) AS alt_count
  FROM variant_calls vc
  INNER JOIN "_1000_GENOMES"."_1000_GENOMES"."SAMPLE_INFO" si
    ON vc.sample_id = si."Sample"
),
contingency AS (
  SELECT 
    "start",
    "end",
    SUM(CASE WHEN is_case = 1 THEN ref_count ELSE 0 END) AS a,
    SUM(CASE WHEN is_case = 1 THEN alt_count ELSE 0 END) AS b,
    SUM(CASE WHEN is_case = 0 THEN ref_count ELSE 0 END) AS c,
    SUM(CASE WHEN is_case = 0 THEN alt_count ELSE 0 END) AS d
  FROM sample_alleles
  GROUP BY "start", "end"
),
stats AS (
  SELECT 
    "start",
    "end",
    a,
    b,
    c,
    d,
    a + b + c + d AS total,
    (a + b) * (a + c) / total AS exp_a,
    (a + b) * (b + d) / total AS exp_b,
    (c + d) * (a + c) / total AS exp_c,
    (c + d) * (b + d) / total AS exp_d
  FROM contingency
  WHERE total > 0
),
filtered_stats AS (
  SELECT 
    "start",
    "end",
    a,
    b,
    c,
    d,
    exp_a,
    exp_b,
    exp_c,
    exp_d,
    POWER(ABS(a - exp_a) - 0.5, 2) / exp_a +
    POWER(ABS(b - exp_b) - 0.5, 2) / exp_b +
    POWER(ABS(c - exp_c) - 0.5, 2) / exp_c +
    POWER(ABS(d - exp_d) - 0.5, 2) / exp_d AS chi_squared
  FROM stats
  WHERE exp_a >= 5 AND exp_b >= 5 AND exp_c >= 5 AND exp_d >= 5
)
SELECT 
  "start",
  "end",
  chi_squared
FROM filtered_stats
WHERE chi_squared >= 29.71679
ORDER BY chi_squared DESC