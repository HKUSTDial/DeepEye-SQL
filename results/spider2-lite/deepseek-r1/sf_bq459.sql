WITH stopwords AS (
    SELECT "'a'" AS word UNION ALL SELECT "'about'" UNION ALL SELECT "'above'" UNION ALL SELECT "'after'" UNION ALL SELECT "'again'" UNION ALL SELECT "'against'" UNION ALL SELECT "'ain'" UNION ALL SELECT "'all'" UNION ALL SELECT "'am'" UNION ALL SELECT "'an'" UNION ALL SELECT "'and'" UNION ALL SELECT "'any'" UNION ALL SELECT "'are'" UNION ALL SELECT "'aren'" UNION ALL SELECT "'arent'" UNION ALL SELECT "'as'" UNION ALL SELECT "'at'" UNION ALL SELECT "'be'" UNION ALL SELECT "'because'" UNION ALL SELECT "'been'" UNION ALL SELECT "'before'" UNION ALL SELECT "'being'" UNION ALL SELECT "'below'" UNION ALL SELECT "'between'" UNION ALL SELECT "'both'" UNION ALL SELECT "'but'" UNION ALL SELECT "'by'" UNION ALL SELECT "'can'" UNION ALL SELECT "'couldn'" UNION ALL SELECT "'couldnt'" UNION ALL SELECT "'d'" UNION ALL SELECT "'did'" UNION ALL SELECT "'didn'" UNION ALL SELECT "'didnt'" UNION ALL SELECT "'do'" UNION ALL SELECT "'does'" UNION ALL SELECT "'doesn'" UNION ALL SELECT "'doesnt'" UNION ALL SELECT "'doing'" UNION ALL SELECT "'don'" UNION ALL SELECT "'dont'" UNION ALL SELECT "'down'" UNION ALL SELECT "'during'" UNION ALL SELECT "'each'" UNION ALL SELECT "'few'" UNION ALL SELECT "'for'" UNION ALL SELECT "'from'" UNION ALL SELECT "'further'" UNION ALL SELECT "'had'" UNION ALL SELECT "'hadn'" UNION ALL SELECT "'hadnt'" UNION ALL SELECT "'has'" UNION ALL SELECT "'hasn'" UNION ALL SELECT "'hasnt'" UNION ALL SELECT "'have'" UNION ALL SELECT "'haven'" UNION ALL SELECT "'havent'" UNION ALL SELECT "'having'" UNION ALL SELECT "'he'" UNION ALL SELECT "'her'" UNION ALL SELECT "'here'" UNION ALL SELECT "'hers'" UNION ALL SELECT "'herself'" UNION ALL SELECT "'him'" UNION ALL SELECT "'himself'" UNION ALL SELECT "'his'" UNION ALL SELECT "'how'" UNION ALL SELECT "'i'" UNION ALL SELECT "'if'" UNION ALL SELECT "'in'" UNION ALL SELECT "'into'" UNION ALL SELECT "'is'" UNION ALL SELECT "'isn'" UNION ALL SELECT "'isnt'" UNION ALL SELECT "'it'" UNION ALL SELECT "'its'" UNION ALL SELECT "'itself'" UNION ALL SELECT "'just'" UNION ALL SELECT "'ll'" UNION ALL SELECT "'m'" UNION ALL SELECT "'ma'" UNION ALL SELECT "'me'" UNION ALL SELECT "'mightn'" UNION ALL SELECT "'mightnt'" UNION ALL SELECT "'more'" UNION ALL SELECT "'most'" UNION ALL SELECT "'mustn'" UNION ALL SELECT "'mustnt'" UNION ALL SELECT "'my'" UNION ALL SELECT "'myself'" UNION ALL SELECT "'needn'" UNION ALL SELECT "'neednt'" UNION ALL SELECT "'no'" UNION ALL SELECT "'nor'" UNION ALL SELECT "'not'" UNION ALL SELECT "'now'" UNION ALL SELECT "'o'" UNION ALL SELECT "'of'" UNION ALL SELECT "'off'" UNION ALL SELECT "'on'" UNION ALL SELECT "'once'" UNION ALL SELECT "'only'" UNION ALL SELECT "'or'" UNION ALL SELECT "'other'" UNION ALL SELECT "'our'" UNION ALL SELECT "'ours'" UNION ALL SELECT "'ourselves'" UNION ALL SELECT "'out'" UNION ALL SELECT "'over'" UNION ALL SELECT "'own'" UNION ALL SELECT "'re'" UNION ALL SELECT "'s'" UNION ALL SELECT "'same'" UNION ALL SELECT "'shan'" UNION ALL SELECT "'shant'" UNION ALL SELECT "'she'" UNION ALL SELECT "'shes'" UNION ALL SELECT "'should'" UNION ALL SELECT "'shouldn'" UNION ALL SELECT "'shouldnt'" UNION ALL SELECT "'shouldve'" UNION ALL SELECT "'so'" UNION ALL SELECT "'some'" UNION ALL SELECT "'such'" UNION ALL SELECT "'t'" UNION ALL SELECT "'than'" UNION ALL SELECT "'that'" UNION ALL SELECT "'thatll'" UNION ALL SELECT "'the'" UNION ALL SELECT "'their'" UNION ALL SELECT "'theirs'" UNION ALL SELECT "'them'" UNION ALL SELECT "'themselves'" UNION ALL SELECT "'then'" UNION ALL SELECT "'there'" UNION ALL SELECT "'these'" UNION ALL SELECT "'they'" UNION ALL SELECT "'this'" UNION ALL SELECT "'those'" UNION ALL SELECT "'through'" UNION ALL SELECT "'to'" UNION ALL SELECT "'too'" UNION ALL SELECT "'under'" UNION ALL SELECT "'until'" UNION ALL SELECT "'up'" UNION ALL SELECT "'ve'" UNION ALL SELECT "'very'" UNION ALL SELECT "'was'" UNION ALL SELECT "'wasn'" UNION ALL SELECT "'wasnt'" UNION ALL SELECT "'we'" UNION ALL SELECT "'were'" UNION ALL SELECT "'weren'" UNION ALL SELECT "'werent'" UNION ALL SELECT "'what'" UNION ALL SELECT "'when'" UNION ALL SELECT "'where'" UNION ALL SELECT "'which'" UNION ALL SELECT "'while'" UNION ALL SELECT "'who'" UNION ALL SELECT "'whom'" UNION ALL SELECT "'why'" UNION ALL SELECT "'will'" UNION ALL SELECT "'with'" UNION ALL SELECT "'won'" UNION ALL SELECT "'wont'" UNION ALL SELECT "'wouldn'" UNION ALL SELECT "'wouldnt'" UNION ALL SELECT "'y'" UNION ALL SELECT "'you'" UNION ALL SELECT "'youd'" UNION ALL SELECT "'youll'" UNION ALL SELECT "'your'" UNION ALL SELECT "'youre'" UNION ALL SELECT "'yours'" UNION ALL SELECT "'yourself'" UNION ALL SELECT "'yourselves'" UNION ALL SELECT "'youve'"
),
query_text AS (
    SELECT 'Epigenetics and cerebral organoids: promising directions in autism spectrum disorders' AS text
),
query_tokens AS (
    SELECT LOWER(REGEXP_SUBSTR(f.value, '([A-Za-z0-9]+)', 1, 1, 'e', 1)) AS token
    FROM query_text,
    LATERAL FLATTEN(INPUT => REGEXP_SUBSTR_ALL(query_text.text, '([A-Za-z0-9]+)')) f
    WHERE token IS NOT NULL
    AND LOWER(token) NOT IN (SELECT word FROM stopwords)
),
query_word_data AS (
    SELECT 
        qt.token,
        gv."vector" AS vector,
        wf."frequency" AS frequency
    FROM query_tokens qt
    LEFT JOIN "WORD_VECTORS_US"."WORD_VECTORS_US"."GLOVE_VECTORS" gv 
        ON LOWER(qt.token) = LOWER(gv."word")
    LEFT JOIN "WORD_VECTORS_US"."WORD_VECTORS_US"."WORD_FREQUENCIES" wf 
        ON LOWER(qt.token) = LOWER(wf."word")
    WHERE gv."vector" IS NOT NULL AND wf."frequency" IS NOT NULL
),
query_vector_expanded AS (
    SELECT 
        f.index AS dim_idx,
        f.value::FLOAT / POWER(qwd.frequency, 0.4) AS weighted_value
    FROM query_word_data qwd,
    LATERAL FLATTEN(INPUT => qwd.vector) f
),
query_vector_sum AS (
    SELECT 
        dim_idx,
        SUM(weighted_value) AS sum_value
    FROM query_vector_expanded
    GROUP BY dim_idx
),
query_norm AS (
    SELECT SQRT(SUM(POWER(sum_value, 2))) AS norm
    FROM query_vector_sum
),
query_unit_vector AS (
    SELECT 
        qvs.dim_idx,
        qvs.sum_value / qn.norm AS unit_value
    FROM query_vector_sum qvs
    CROSS JOIN query_norm qn
),
article_tokens AS (
    SELECT 
        n."id",
        LOWER(REGEXP_SUBSTR(f.value, '([A-Za-z0-9]+)', 1, 1, 'e', 1)) AS token
    FROM "WORD_VECTORS_US"."WORD_VECTORS_US"."NATURE" n,
    LATERAL FLATTEN(INPUT => REGEXP_SUBSTR_ALL(n."body", '([A-Za-z0-9]+)')) f
    WHERE token IS NOT NULL
    AND LOWER(token) NOT IN (SELECT word FROM stopwords)
),
article_word_data AS (
    SELECT 
        at."id",
        at.token,
        gv."vector" AS vector,
        wf."frequency" AS frequency
    FROM article_tokens at
    LEFT JOIN "WORD_VECTORS_US"."WORD_VECTORS_US"."GLOVE_VECTORS" gv 
        ON LOWER(at.token) = LOWER(gv."word")
    LEFT JOIN "WORD_VECTORS_US"."WORD_VECTORS_US"."WORD_FREQUENCIES" wf 
        ON LOWER(at.token) = LOWER(wf."word")
    WHERE gv."vector" IS NOT NULL AND wf."frequency" IS NOT NULL
),
article_vector_expanded AS (
    SELECT 
        awd."id",
        f.index AS dim_idx,
        f.value::FLOAT / POWER(awd.frequency, 0.4) AS weighted_value
    FROM article_word_data awd,
    LATERAL FLATTEN(INPUT => awd.vector) f
),
article_vector_sum AS (
    SELECT 
        "id",
        dim_idx,
        SUM(weighted_value) AS sum_value
    FROM article_vector_expanded
    GROUP BY "id", dim_idx
),
article_norm AS (
    SELECT 
        "id",
        SQRT(SUM(POWER(sum_value, 2))) AS norm
    FROM article_vector_sum
    GROUP BY "id"
),
article_unit_vector AS (
    SELECT 
        avs."id",
        avs.dim_idx,
        avs.sum_value / an.norm AS unit_value
    FROM article_vector_sum avs
    JOIN article_norm an ON avs."id" = an."id"
),
cosine_similarities AS (
    SELECT 
        auv."id",
        SUM(quv.unit_value * auv.unit_value) AS cosine_sim
    FROM query_unit_vector quv
    JOIN article_unit_vector auv ON quv.dim_idx = auv.dim_idx
    GROUP BY auv."id"
)
SELECT 
    n."id",
    n."date",
    n."title",
    cs.cosine_sim
FROM cosine_similarities cs
JOIN "WORD_VECTORS_US"."WORD_VECTORS_US"."NATURE" n ON cs."id" = n."id"
ORDER BY cs.cosine_sim DESC
LIMIT 10