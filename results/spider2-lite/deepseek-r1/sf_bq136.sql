WITH edges AS (
    SELECT
        "sender" AS from_addr,
        "to_addr" AS to_addr,
        "id" AS tx_id,
        "block_timestamp" AS ts
    FROM "CRYPTO"."CRYPTO_ZILLIQA"."TRANSACTIONS"
    WHERE "success" = TRUE AND "accepted" = TRUE
    UNION ALL
    SELECT
        "addr" AS from_addr,
        "recipient" AS to_addr,
        "transaction_id" AS tx_id,
        "block_timestamp" AS ts
    FROM "CRYPTO"."CRYPTO_ZILLIQA"."TRANSITIONS"
    WHERE "accepted" = TRUE
),
outgoing_counts AS (
    SELECT
        from_addr,
        COUNT(*) AS out_cnt
    FROM edges
    GROUP BY from_addr
),
first_hop AS (
    SELECT
        from_addr,
        to_addr AS intermediate,
        tx_id AS first_tx_id,
        ts AS first_ts
    FROM edges
    WHERE from_addr = 'zil1jrpjd8pjuv50cfkfr7eu6yrm3rn5u8rulqhqpz'
),
second_hop AS (
    SELECT
        from_addr AS intermediate,
        to_addr,
        tx_id AS second_tx_id,
        ts AS second_ts
    FROM edges
    WHERE to_addr = 'zil19nmxkh020jnequql9kvqkf3pkwm0j0spqtd26e'
)
SELECT
    CONCAT(
        'zil1jrpjd8pjuv50cfkfr7eu6yrm3rn5u8rulqhqpz',
        ' --(tx ',
        SUBSTRING(f.first_tx_id, 1, 5),
        '..)--> ',
        f.intermediate,
        ' --(tx ',
        SUBSTRING(s.second_tx_id, 1, 5),
        '..)--> ',
        'zil19nmxkh020jnequql9kvqkf3pkwm0j0spqtd26e'
    ) AS path
FROM first_hop f
INNER JOIN second_hop s ON f.intermediate = s.intermediate
INNER JOIN outgoing_counts oc ON f.intermediate = oc.from_addr
WHERE f.first_ts < s.second_ts
    AND oc.out_cnt <= 50