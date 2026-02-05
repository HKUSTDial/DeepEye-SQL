WITH stopwords AS (
    SELECT * FROM (VALUES
        ('a'), ('about'), ('above'), ('after'), ('again'), ('against'), ('ain'), ('all'), ('am'), ('an'), ('and'), ('any'), ('are'), ('aren'), ('arent'), ('as'), ('at'), ('be'), ('because'), ('been'), ('before'), ('being'), ('below'), ('between'), ('both'), ('but'), ('by'), ('can'), ('couldn'), ('couldnt'), ('d'), ('did'), ('didn'), ('didnt'), ('do'), ('does'), ('doesn'), ('doesnt'), ('doing'), ('don'), ('dont'), ('down'), ('during'), ('each'), ('few'), ('for'), ('from'), ('further'), ('had'), ('hadn'), ('hadnt'), ('has'), ('hasn'), ('hasnt'), ('have'), ('haven'), ('havent'), ('having'), ('he'), ('her'), ('here'), ('hers'), ('herself'), ('him'), ('himself'), ('his'), ('how'), ('i'), ('if'), ('in'), ('into'), ('is'), ('isn'), ('isnt'), ('it'), ('its'), ('itself'), ('just'), ('ll'), ('m'), ('ma'), ('me'), ('mightn'), ('mightnt'), ('more'), ('most'), ('mustn'), ('mustnt'), ('my'), ('myself'), ('needn'), ('neednt'), ('no'), ('nor'), ('not'), ('now'), ('o'), ('of'), ('off'), ('on'), ('once'), ('only'), ('or'), ('other'), ('our'), ('ours'), ('ourselves'), ('out'), ('over'), ('own'), ('re'), ('s'), ('same'), ('shan'), ('shant'), ('she'), ('shes'), ('should'), ('shouldn'), ('shouldnt'), ('shouldve'), ('so'), ('some'), ('such'), ('t'), ('than'), ('that'), ('thatll'), ('the'), ('their'), ('theirs'), ('them'), ('themselves'), ('then'), ('there'), ('these'), ('they'), ('this'), ('those'), ('through'), ('to'), ('too'), ('under'), ('until'), ('up'), ('ve'), ('very'), ('was'), ('wasn'), ('wasnt'), ('we'), ('were'), ('weren'), ('werent'), ('what'), ('when'), ('where'), ('which'), ('while'), ('who'), ('whom'), ('why'), ('will'), ('with'), ('won'), ('wont'), ('wouldn'), ('wouldnt'), ('y'), ('you'), ('youd'), ('youll'), ('your'), ('youre'), ('yours'), ('yourself'), ('yourselves'), ('youve')
    ) AS t(word)
),
tokenized_articles AS (
    SELECT 
        n."id" AS article_id,
        LOWER(TRIM(REGEXP_SUBSTR(n."body", '[a-zA-Z0-9]+', 1, seq.value))) AS word
    FROM 
        "WORD_VECTORS_US"."WORD_VECTORS_US"."NATURE" n,
        LATERAL FLATTEN(SEQUENCE(1, REGEXP_COUNT(n."body", '[a-zA-Z0-9]+'))) seq
    WHERE 
        LOWER(TRIM(REGEXP_SUBSTR(n."body", '[a-zA-Z0-9]+', 1, seq.value))) NOT IN (SELECT word FROM stopwords)
        AND LOWER(TRIM(REGEXP_SUBSTR(n."body", '[a-zA-Z0-9]+', 1, seq.value))) != ''
),
word_data AS (
    SELECT 
        ta.article_id,
        ta.word,
        wf."frequency",
        gv."vector"
    FROM 
        tokenized_articles ta
        LEFT JOIN "WORD_VECTORS_US"."WORD_VECTORS_US"."WORD_FREQUENCIES" wf ON ta.word = wf."word"
        LEFT JOIN "WORD_VECTORS_US"."WORD_VECTORS_US"."GLOVE_VECTORS" gv ON ta.word = gv."word"
    WHERE 
        wf."frequency" IS NOT NULL 
        AND gv."vector" IS NOT NULL
),
flattened_vectors AS (
    SELECT 
        wd.article_id,
        wd.word,
        wd."frequency",
        f.index AS vec_index,
        f.value AS vec_component
    FROM 
        word_data wd,
        LATERAL FLATTEN(INPUT => wd."vector") f
),
weighted_components AS (
    SELECT 
        article_id,
        vec_index,
        vec_component / POWER("frequency", 0.4) AS weighted_component
    FROM 
        flattened_vectors
),
aggregated_vectors AS (
    SELECT 
        article_id,
        vec_index,
        SUM(weighted_component) AS aggregated_component
    FROM 
        weighted_components
    GROUP BY 
        article_id, vec_index
),
norms AS (
    SELECT 
        article_id,
        SQRT(SUM(POWER(aggregated_component, 2))) AS norm
    FROM 
        aggregated_vectors
    GROUP BY 
        article_id
    HAVING 
        norm > 0
),
normalized_vectors AS (
    SELECT 
        av.article_id,
        av.vec_index,
        av.aggregated_component / n.norm AS normalized_component
    FROM 
        aggregated_vectors av
        INNER JOIN norms n ON av.article_id = n.article_id
),
target_vector AS (
    SELECT 
        vec_index,
        normalized_component AS target_component
    FROM 
        normalized_vectors
    WHERE 
        article_id = '8a78ef2d-d5f7-4d2d-9b47-5adb25cbd373'
),
cosine_similarity AS (
    SELECT 
        nv.article_id,
        SUM(nv.normalized_component * tv.target_component) AS similarity
    FROM 
        normalized_vectors nv
        INNER JOIN target_vector tv ON nv.vec_index = tv.vec_index
    WHERE 
        nv.article_id != '8a78ef2d-d5f7-4d2d-9b47-5adb25cbd373'
    GROUP BY 
        nv.article_id
)
SELECT 
    n."id",
    n."date",
    n."title",
    cs.similarity AS cosine_similarity_score
FROM 
    cosine_similarity cs
    INNER JOIN "WORD_VECTORS_US"."WORD_VECTORS_US"."NATURE" n ON cs.article_id = n."id"
ORDER BY 
    cs.similarity DESC
LIMIT 10