WITH stop_words AS (
  SELECT column1 AS word FROM VALUES 
  ('a'), ('about'), ('above'), ('after'), ('again'), ('against'), ('ain'), ('all'), ('am'), ('an'), ('and'), ('any'), ('are'), ('aren'), ('arent'), ('as'), ('at'), ('be'), ('because'), ('been'), ('before'), ('being'), ('below'), ('between'), ('both'), ('but'), ('by'), ('can'), ('couldn'), ('couldnt'), ('d'), ('did'), ('didn'), ('didnt'), ('do'), ('does'), ('doesn'), ('doesnt'), ('doing'), ('don'), ('dont'), ('down'), ('during'), ('each'), ('few'), ('for'), ('from'), ('further'), ('had'), ('hadn'), ('hadnt'), ('has'), ('hasn'), ('hasnt'), ('have'), ('haven'), ('havent'), ('having'), ('he'), ('her'), ('here'), ('hers'), ('herself'), ('him'), ('himself'), ('his'), ('how'), ('i'), ('if'), ('in'), ('into'), ('is'), ('isn'), ('isnt'), ('it'), ('its'), ('itself'), ('just'), ('ll'), ('m'), ('ma'), ('me'), ('mightn'), ('mightnt'), ('more'), ('most'), ('mustn'), ('mustnt'), ('my'), ('myself'), ('needn'), ('neednt'), ('no'), ('nor'), ('not'), ('now'), ('o'), ('of'), ('off'), ('on'), ('once'), ('only'), ('or'), ('other'), ('our'), ('ours'), ('ourselves'), ('out'), ('over'), ('own'), ('re'), ('s'), ('same'), ('shan'), ('shant'), ('she'), ('shes'), ('should'), ('shouldn'), ('shouldnt'), ('shouldve'), ('so'), ('some'), ('such'), ('t'), ('than'), ('that'), ('thatll'), ('the'), ('their'), ('theirs'), ('them'), ('themselves'), ('then'), ('there'), ('these'), ('they'), ('this'), ('those'), ('through'), ('to'), ('too'), ('under'), ('until'), ('up'), ('ve'), ('very'), ('was'), ('wasn'), ('wasnt'), ('we'), ('were'), ('weren'), ('werent'), ('what'), ('when'), ('where'), ('which'), ('while'), ('who'), ('whom'), ('why'), ('will'), ('with'), ('won'), ('wont'), ('wouldn'), ('wouldnt'), ('y'), ('you'), ('youd'), ('youll'), ('your'), ('youre'), ('yours'), ('yourself'), ('yourselves'), ('youve')
),
article_tokens AS (
  SELECT 
    n."id",
    n."date",
    n."title",
    LOWER(t.value) AS word
  FROM "WORD_VECTORS_US"."WORD_VECTORS_US"."NATURE" n,
  LATERAL FLATTEN(INPUT => REGEXP_SUBSTR_ALL(n."body", '(\d+(,\d+)*(\.\d+)?|[a-zA-Z_]+)', 1)) t
  WHERE t.value IS NOT NULL
),
filtered_tokens AS (
  SELECT at."id", at."date", at."title", at.word
  FROM article_tokens at
  WHERE at.word NOT IN (SELECT word FROM stop_words)
),
article_word_vectors AS (
  SELECT 
    ft."id",
    ft."date",
    ft."title",
    ft.word,
    gv."vector",
    wf."frequency"
  FROM filtered_tokens ft
  INNER JOIN "WORD_VECTORS_US"."WORD_VECTORS_US"."GLOVE_VECTORS" gv ON ft.word = gv."word"
  INNER JOIN "WORD_VECTORS_US"."WORD_VECTORS_US"."WORD_FREQUENCIES" wf ON ft.word = wf."word"
),
weighted_vectors AS (
  SELECT 
    awv."id",
    awv."date",
    awv."title",
    f.index AS vector_index,
    f.value::FLOAT * (1 / POWER(awv."frequency", 0.4)) AS weighted_component
  FROM article_word_vectors awv,
  LATERAL FLATTEN(INPUT => awv."vector") f
),
aggregated_vectors AS (
  SELECT 
    "id",
    "date",
    "title",
    vector_index,
    SUM(weighted_component) AS sum_component
  FROM weighted_vectors
  GROUP BY "id", "date", "title", vector_index
),
article_magnitude AS (
  SELECT 
    "id",
    "date",
    "title",
    SQRT(SUM(POWER(sum_component, 2))) AS magnitude
  FROM aggregated_vectors
  GROUP BY "id", "date", "title"
),
normalized_components AS (
  SELECT 
    av."id",
    av."date",
    av."title",
    av.vector_index,
    av.sum_component / am.magnitude AS normalized_component
  FROM aggregated_vectors av
  INNER JOIN article_magnitude am ON av."id" = am."id" AND av."date" = am."date" AND av."title" = am."title"
)
SELECT 
  "id",
  "date",
  "title",
  ARRAY_AGG(normalized_component ORDER BY vector_index) AS normalized_vector
FROM normalized_components
GROUP BY "id", "date", "title"