WITH tokenized AS (
    SELECT 
        n."id",
        n."date",
        n."title",
        t.value AS "word"
    FROM "WORD_VECTORS_US"."WORD_VECTORS_US"."NATURE" n,
    LATERAL FLATTEN(INPUT => REGEXP_SUBSTR_ALL(REGEXP_REPLACE(n."body", '’|''s(\\W)', '\\1'), '((\\d+(,\\d+)*(\\.\\d+)?)+|([\\w])+)', 1, 1, 'c')) t
    WHERE LOWER(t.value) NOT IN ('a', 'about', 'above', 'after', 'again', 'against', 'ain', 'all', 'am', 'an', 'and', 'any', 'are', 'aren', 'arent', 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', 'can', 'couldn', 'couldnt', 'd', 'did', 'didn', 'didnt', 'do', 'does', 'doesn', 'doesnt', 'doing', 'don', 'dont', 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', 'hadn', 'hadnt', 'has', 'hasn', 'hasnt', 'have', 'haven', 'havent', 'having', 'he', 'her', 'here', 'hers', 'herself', 'him', 'himself', 'his', 'how', 'i', 'if', 'in', 'into', 'is', 'isn', 'isnt', 'it', 'its', 'itself', 'just', 'll', 'm', 'ma', 'me', 'mightn', 'mightnt', 'more', 'most', 'mustn', 'mustnt', 'my', 'myself', 'needn', 'neednt', 'no', 'nor', 'not', 'now', 'o', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'our', 'ours', 'ourselves', 'out', 'over', 'own', 're', 's', 'same', 'shan', 'shant', 'she', 'shes', 'should', 'shouldn', 'shouldnt', 'shouldve', 'so', 'some', 'such', 't', 'than', 'that', 'thatll', 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there', 'these', 'they', 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 've', 'very', 'was', 'wasn', 'wasnt', 'we', 'were', 'weren', 'werent', 'what', 'when', 'where', 'which', 'while', 'who', 'whom', 'why', 'will', 'with', 'won', 'wont', 'wouldn', 'wouldnt', 'y', 'you', 'youd', 'youll', 'your', 'youre', 'yours', 'yourself', 'yourselves', 'youve')
),
word_info AS (
    SELECT 
        t."id",
        t."date",
        t."title",
        t."word",
        g."vector",
        wf."frequency"
    FROM tokenized t
    INNER JOIN "WORD_VECTORS_US"."WORD_VECTORS_US"."GLOVE_VECTORS" g ON t."word" = g."word"
    INNER JOIN "WORD_VECTORS_US"."WORD_VECTORS_US"."WORD_FREQUENCIES" wf ON t."word" = wf."word"
),
weighted_components AS (
    SELECT 
        wi."id",
        wi."date",
        wi."title",
        wi."word",
        wi."frequency",
        f.index AS "component_index",
        f.value::FLOAT AS "component_value",
        f.value::FLOAT / POWER(wi."frequency", 0.4) AS "weighted_component"
    FROM word_info wi,
    LATERAL FLATTEN(INPUT => wi."vector") f
),
article_components AS (
    SELECT 
        "id",
        "date",
        "title",
        "component_index",
        SUM("weighted_component") AS "component_sum"
    FROM weighted_components
    GROUP BY "id", "date", "title", "component_index"
),
magnitudes AS (
    SELECT 
        "id",
        "date",
        "title",
        SQRT(SUM(POWER("component_sum", 2))) AS "magnitude"
    FROM article_components
    GROUP BY "id", "date", "title"
),
normalized_components AS (
    SELECT 
        ac."id",
        ac."date",
        ac."title",
        ac."component_index",
        ac."component_sum" / m."magnitude" AS "normalized_component"
    FROM article_components ac
    INNER JOIN magnitudes m ON ac."id" = m."id" AND ac."date" = m."date" AND ac."title" = m."title"
)
SELECT 
    "id",
    "date",
    "title",
    ARRAY_AGG("normalized_component" ORDER BY "component_index") AS "article_vector"
FROM normalized_components
GROUP BY "id", "date", "title"