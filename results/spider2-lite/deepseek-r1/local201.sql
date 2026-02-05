WITH RECURSIVE numbers(n) AS (
    SELECT 1
    UNION ALL
    SELECT n+1 FROM numbers WHERE n < 10
),
all_words AS (
    SELECT DISTINCT `words` AS word
    FROM `word_list`
    WHERE LENGTH(`words`) BETWEEN 4 AND 5
),
split AS (
    SELECT aw.word, SUBSTR(aw.word, n.n, 1) AS ch
    FROM all_words aw
    JOIN numbers n ON n.n <= LENGTH(aw.word)
),
sorted_keys AS (
    SELECT word, GROUP_CONCAT(ch ORDER BY ch COLLATE BINARY) AS key
    FROM split
    GROUP BY word
),
key_counts AS (
    SELECT key, COUNT(*) AS cnt
    FROM sorted_keys
    GROUP BY key
),
candidates AS (
    SELECT sk.word, sk.key, kc.cnt
    FROM sorted_keys sk
    JOIN key_counts kc ON sk.key = kc.key
    WHERE SUBSTR(sk.word,1,1) = 'r'
      AND kc.cnt >= 2
)
SELECT word, (cnt - 1) AS anagram_count
FROM candidates
ORDER BY word
LIMIT 10