WITH split_words AS (
  SELECT
    "words" AS word,
    f.value AS ch
  FROM "MODERN_DATA"."MODERN_DATA"."WORD_LIST",
       LATERAL FLATTEN(INPUT => REGEXP_SUBSTR_ALL("words", '.')) f
  WHERE LENGTH("words") BETWEEN 4 AND 5
),
word_signatures AS (
  SELECT
    word,
    LISTAGG(ch) WITHIN GROUP (ORDER BY ch) AS signature
  FROM split_words
  GROUP BY word
),
anagram_groups AS (
  SELECT
    signature,
    COUNT(*) AS group_count
  FROM word_signatures
  GROUP BY signature
  HAVING COUNT(*) >= 2
)
SELECT
  ws.word,
  ag.group_count - 1 AS anagram_count
FROM word_signatures ws
INNER JOIN anagram_groups ag ON ws.signature = ag.signature
WHERE ws.word LIKE 'r%'
ORDER BY ws.word
LIMIT 10