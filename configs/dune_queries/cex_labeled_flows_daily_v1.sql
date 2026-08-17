WITH ranked_labels AS (
    SELECT
        blockchain,
        address,
        CASE
            WHEN lower(name) LIKE 'binance%' THEN 'binance'
            WHEN lower(name) LIKE 'okx%' THEN 'okx'
            WHEN lower(name) LIKE 'bybit%' THEN 'bybit'
        END AS exchange,
        CAST(max(updated_at) OVER () AS varchar) AS label_version,
        row_number() OVER (
            PARTITION BY blockchain, address
            ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST, name
        ) AS label_rank
    FROM labels.addresses
    WHERE category = 'cex users'
      AND regexp_like(lower(name), '^(binance|okx|bybit)( |$)')
),
cex_labels AS (
    SELECT blockchain, address, exchange, label_version
    FROM ranked_labels
    WHERE label_rank = 1
      AND exchange IS NOT NULL
),
transfer_window AS (
    SELECT *
    FROM tokens.transfers
    WHERE block_month >= date_trunc('month', DATE '{{start_date}}')
      AND block_month <= date_trunc('month', DATE '{{end_date}}' - INTERVAL '1' DAY)
      AND block_date >= DATE '{{start_date}}'
      AND block_date < DATE '{{end_date}}'
      AND amount IS NOT NULL
),
flow_legs AS (
    SELECT
        t.block_date AS event_date,
        t.blockchain,
        l.exchange,
        upper(coalesce(t.symbol, 'UNKNOWN')) AS asset_symbol,
        CAST(t.contract_address AS varchar) AS token_address,
        'inflow' AS flow_direction,
        t.tx_hash,
        t.amount,
        t.amount_usd,
        l.label_version
    FROM transfer_window t
    JOIN cex_labels l
      ON t.blockchain = l.blockchain AND t."to" = l.address

    UNION ALL

    SELECT
        t.block_date AS event_date,
        t.blockchain,
        l.exchange,
        upper(coalesce(t.symbol, 'UNKNOWN')) AS asset_symbol,
        CAST(t.contract_address AS varchar) AS token_address,
        'outflow' AS flow_direction,
        t.tx_hash,
        t.amount,
        t.amount_usd,
        l.label_version
    FROM transfer_window t
    JOIN cex_labels l
      ON t.blockchain = l.blockchain AND t."from" = l.address
)
SELECT
    CAST(event_date AS varchar) AS event_date,
    blockchain,
    exchange,
    asset_symbol,
    token_address,
    flow_direction,
    count(*) AS transfer_count,
    approx_distinct(tx_hash) AS transaction_count,
    sum(amount) AS amount_native,
    sum(amount_usd) AS amount_usd,
    label_version
FROM flow_legs
GROUP BY 1, 2, 3, 4, 5, 6, 11
ORDER BY 1, 2, 3, 4, 5, 6, 11
