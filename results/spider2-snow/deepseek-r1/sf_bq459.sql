WITH stopwords AS (
  SELECT column1 AS word FROM VALUES ('a'), ('about'), ('above'), ('after'), ('again'), ('against'), ('ain'), ('all'), ('am'), ('an'), ('and'), ('any'), ('are'), ('aren'), ('arent'), ('as'), ('at'), ('be'), ('because'), ('been'), ('before'), ('being'), ('below'), ('between'), ('both'), ('but'), ('by'), ('can'), ('couldn'), ('couldnt'), ('d'), ('did'), ('didn'), ('didnt'), ('do'), ('does'), ('doesn'), ('doesnt'), ('doing'), ('don'), ('dont'), ('down'), ('during'), ('each'), ('few'), ('for'), ('from'), ('further'), ('had'), ('hadn'), ('hadnt'), ('has'), ('hasn'), ('hasnt'), ('have'), ('haven'), ('havent'), ('having'), ('he'), ('her'), ('here'), ('hers'), ('herself'), ('him'), ('himself'), ('his'), ('how'), ('i'), ('if'), ('in'), ('into'), ('is'), ('isn'), ('isnt'), ('it'), ('its'), ('itself'), ('just'), ('ll'), ('m'), ('ma'), ('me'), ('mightn'), ('mightnt'), ('more'), ('most'), ('mustn'), ('mustnt'), ('my'), ('myself'), ('needn'), ('neednt'), ('no'), ('nor'), ('not'), ('now'), ('o'), ('of'), ('off'), ('on'), ('once'), ('only'), ('or'), ('other'), ('our'), ('ours'), ('ourselves'), ('out'), ('over'), ('own'), ('re'), ('s'), ('same'), ('shan'), ('shant'), ('she'), ('shes'), ('should'), ('shouldn'), ('shouldnt'), ('shouldve'), ('so'), ('some'), ('such'), ('t'), ('than'), ('that'), ('thatll'), ('the'), ('their'), ('theirs'), ('them'), ('themselves'), ('then'), ('there'), ('these'), ('they'), ('this'), ('those'), ('through'), ('to'), ('too'), ('under'), ('until'), ('up'), ('ve'), ('very'), ('was'), ('wasn'), ('wasnt'), ('we'), ('were'), ('weren'), ('werent'), ('what'), ('when'), ('where'), ('which'), ('while'), ('who'), ('whom'), ('why'), ('will'), ('with'), ('won'), ('wont'), ('wouldn'), ('wouldnt'), ('y'), ('you'), ('youd'), ('youll'), ('your'), ('youre'), ('yours'), ('yourself'), ('yourselves'), ('youve')
),
article_tokens AS (
  SELECT 
    "id",
    "date",
    "title",
    REGEXP_SUBSTR_ALL(LOWER("body"), '[a-z0-9\\-]+') AS words
  FROM "WORD_VECTORS_US"."WORD_VECTORS_US"."NATURE"
),
article_words AS (
  SELECT 
    "id",
    "date",
    "title",
    f.value::TEXT AS word
  FROM article_tokens,
  LATERAL FLATTEN(INPUT => words) f
  WHERE word NOT IN (SELECT word FROM stopwords)
),
article_word_vectors AS (
  SELECT 
    aw."id",
    aw."date",
    aw."title",
    aw.word,
    gv."vector",
    wf."frequency"
  FROM article_words aw
  LEFT JOIN "WORD_VECTORS_US"."WORD_VECTORS_US"."GLOVE_VECTORS" gv ON aw.word = gv."word"
  LEFT JOIN "WORD_VECTORS_US"."WORD_VECTORS_US"."WORD_FREQUENCIES" wf ON aw.word = wf."word"
  WHERE gv."vector" IS NOT NULL AND wf."frequency" IS NOT NULL
),
article_weighted_elements AS (
  SELECT 
    "id",
    "date",
    "title",
    f.index AS dim_idx,
    f.value::FLOAT * (1 / POWER("frequency", 0.4)) AS weighted_value
  FROM article_word_vectors,
  LATERAL FLATTEN(INPUT => "vector") f
),
article_vector AS (
  SELECT 
    "id",
    "date",
    "title",
    dim_idx,
    SUM(weighted_value) AS sum_val
  FROM article_weighted_elements
  GROUP BY "id", "date", "title", dim_idx
),
article_norm AS (
  SELECT 
    "id",
    "date",
    "title",
    SQRT(SUM(POWER(sum_val, 2))) AS norm
  FROM article_vector
  GROUP BY "id", "date", "title"
),
article_unit_vector AS (
  SELECT 
    av."id",
    av."date",
    av."title",
    av.dim_idx,
    av.sum_val / an.norm AS unit_val
  FROM article_vector av
  INNER JOIN article_norm an ON av."id" = an."id" AND av."date" = an."date" AND av."title" = an."title"
  WHERE an.norm > 0
),
query_text AS (
  SELECT 'Epigenetics and cerebral organoids: promising directions in autism spectrum disorders' AS query
),
query_tokens AS (
  SELECT REGEXP_SUBSTR_ALL(LOWER(query), '[a-z0-9\\-]+') AS words FROM query_text
),
query_words AS (
  SELECT f.value::TEXT AS word
  FROM query_tokens,
  LATERAL FLATTEN(INPUT => words) f
  WHERE word NOT IN (SELECT word FROM stopwords)
),
query_word_vectors AS (
  SELECT 
    qw.word,
    gv."vector",
    wf."frequency"
  FROM query_words qw
  LEFT JOIN "WORD_VECTORS_US"."WORD_VECTORS_US"."GLOVE_VECTORS" gv ON qw.word = gv."word"
  LEFT JOIN "WORD_VECTORS_US"."WORD_VECTORS_US"."WORD_FREQUENCIES" wf ON qw.word = wf."word"
  WHERE gv."vector" IS NOT NULL AND wf."frequency" IS NOT NULL
),
query_weighted_elements AS (
  SELECT 
    f.index AS dim_idx,
    f.value::FLOAT * (1 / POWER("frequency", 0.4)) AS weighted_value
  FROM query_word_vectors,
  LATERAL FLATTEN(INPUT => "vector") f
),
query_vector AS (
  SELECT 
    dim_idx,
    SUM(weighted_value) AS sum_val
  FROM query_weighted_elements
  GROUP BY dim_idx
),
query_norm AS (
  SELECT SQRT(SUM(POWER(sum_val, 2))) AS norm FROM query_vector
),
query_unit_vector AS (
  SELECT 
    qv.dim_idx,
    qv.sum_val / qn.norm AS unit_val
  FROM query_vector qv
  CROSS JOIN query_norm qn
  WHERE qn.norm > 0
),
cosine_similarity AS (
  SELECT 
    auv."id",
    auv."date",
    auv."title",
    SUM(auv.unit_val * quv.unit_val) AS similarity
  FROM article_unit_vector auv
  INNER JOIN query_unit_vector quv ON auv.dim_idx = quv.dim_idx
  GROUP BY auv."id", auv."date", auv."title"
)
SELECT 
  "id",
  "date",
  "title",
  similarity
FROM cosine_similarity
ORDER BY similarity DESC
LIMIT 10