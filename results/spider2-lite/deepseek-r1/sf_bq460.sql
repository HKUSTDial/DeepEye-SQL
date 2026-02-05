WITH stopwords AS (
    SELECT column_name AS word FROM VALUES ('a'), ('about'), ('above'), ('after'), ('again'), ('against'), ('ain'), ('all'), ('am'), ('an'), ('and'), ('any'), ('are'), ('aren'), ('arent'), ('as'), ('at'), ('be'), ('because'), ('been'), ('before'), ('being'), ('below'), ('between'), ('both'), ('but'), ('by'), ('can'), ('couldn'), ('couldnt'), ('d'), ('did'), ('didn'), ('didnt'), ('do'), ('does'), ('doesn'), ('doesnt'), ('doing'), ('don'), ('dont'), ('down'), ('during'), ('each'), ('few'), ('for'), ('from'), ('further'), ('had'), ('hadn'), ('hadnt'), ('has'), ('hasn'), ('hasnt'), ('have'), ('haven'), ('havent'), ('having'), ('he'), ('her'), ('here'), ('hers'), ('herself'), ('him'), ('himself'), ('his'), ('how'), ('i'), ('if'), ('in'), ('into'), ('is'), ('isn'), ('isnt'), ('it'), ('its'), ('itself'), ('just'), ('ll'), ('m'), ('ma'), ('me'), ('mightn'), ('mightnt'), ('more'), ('most'), ('mustn'), ('mustnt'), ('my'), ('myself'), ('needn'), ('neednt'), ('no'), ('nor'), ('not'), ('now'), ('o'), ('of'), ('off'), ('on'), ('once'), ('only'), ('or'), ('other'), ('our'), ('ours'), ('ourselves'), ('out'), ('over'), ('own'), ('re'), ('s'), ('same'), ('shan'), ('shant'), ('she'), ('shes'), ('should'), ('shouldn'), ('shouldnt'), ('shouldve'), ('so'), ('some'), ('such'), ('t'), ('than'), ('that'), ('thatll'), ('the'), ('their'), ('theirs'), ('them'), ('themselves'), ('then'), ('there'), ('these'), ('they'), ('this'), ('those'), ('through'), ('to'), ('too'), ('under'), ('until'), ('up'), ('ve'), ('very'), ('was'), ('wasn'), ('wasnt'), ('we'), ('were'), ('weren'), ('werent'), ('what'), ('when'), ('where'), ('which'), ('while'), ('who'), ('whom'), ('why'), ('will'), ('with'), ('won'), ('wont'), ('wouldn'), ('wouldnt'), ('y'), ('you'), ('youd'), ('youll'), ('your'), ('youre'), ('yours'), ('yourself'), ('yourselves'), ('youve')
),
cleaned_bodies AS (
    SELECT 
        "id",
        "title",
        "date",
        REGEXP_REPLACE("body", '’|''s(\\W)', '\\1') AS "cleaned_body"
    FROM "WORD_VECTORS_US"."WORD_VECTORS_US"."NATURE"
),
words AS (
    SELECT 
        cb."id",
        cb."title",
        cb."date",
        LOWER(TRIM(f.value::STRING)) AS "word"
    FROM cleaned_bodies cb,
    LATERAL FLATTEN(INPUT => REGEXP_SUBSTR_ALL(cb."cleaned_body", '((?:\\d+(?:,\\d+)*(?:\\.\\d+)?)+|(?:[\\w])+)')) f
    WHERE "word" != ''
),
filtered_words AS (
    SELECT w."id", w."title", w."date", w."word"
    FROM words w
    LEFT JOIN stopwords s ON w."word" = s."word"
    WHERE s."word" IS NULL
),
word_data AS (
    SELECT 
        fw."id",
        fw."title",
        fw."date",
        fw."word",
        gv."vector",
        wf."frequency"
    FROM filtered_words fw
    INNER JOIN "WORD_VECTORS_US"."WORD_VECTORS_US"."GLOVE_VECTORS" gv ON fw."word" = gv."word"
    INNER JOIN "WORD_VECTORS_US"."WORD_VECTORS_US"."WORD_FREQUENCIES" wf ON fw."word" = wf."word"
),
flattened_vectors AS (
    SELECT 
        wd."id",
        wd."title",
        wd."date",
        wd."frequency",
        f.index AS "vec_index",
        f.value::FLOAT AS "component",
        f.value::FLOAT / POWER(wd."frequency", 0.4) AS "weighted_component"
    FROM word_data wd,
    LATERAL FLATTEN(INPUT => wd."vector") f
),
aggregated_vectors AS (
    SELECT 
        "id",
        "title",
        "date",
        "vec_index",
        SUM("weighted_component") AS "agg_component"
    FROM flattened_vectors
    GROUP BY "id", "title", "date", "vec_index"
),
norms AS (
    SELECT 
        "id",
        "title",
        "date",
        SQRT(SUM(POWER("agg_component", 2))) AS "norm"
    FROM aggregated_vectors
    GROUP BY "id", "title", "date"
),
normalized_vectors AS (
    SELECT 
        av."id",
        av."title",
        av."date",
        av."vec_index",
        av."agg_component" / n."norm" AS "norm_component"
    FROM aggregated_vectors av
    INNER JOIN norms n ON av."id" = n."id"
),
target_vector AS (
    SELECT 
        "vec_index",
        "norm_component"
    FROM normalized_vectors
    WHERE "id" = '8a78ef2d-d5f7-4d2d-9b47-5adb25cbd373'
),
cosine_similarity AS (
    SELECT 
        nv."id",
        nv."title",
        nv."date",
        SUM(nv."norm_component" * tv."norm_component") AS "cosine_sim"
    FROM normalized_vectors nv
    INNER JOIN target_vector tv ON nv."vec_index" = tv."vec_index"
    GROUP BY nv."id", nv."title", nv."date"
)
SELECT 
    "id",
    "date",
    "title",
    "cosine_sim"
FROM cosine_similarity
ORDER BY "cosine_sim" DESC
LIMIT 10