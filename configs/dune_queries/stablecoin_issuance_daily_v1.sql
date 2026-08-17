WITH issuance_legs AS (
    SELECT
        block_date AS event_date,
        blockchain,
        upper(symbol) AS asset_symbol,
        CAST(contract_address AS varchar) AS token_address,
        CASE
            WHEN "from" = 0x0000000000000000000000000000000000000000 THEN 'mint'
            WHEN "to" = 0x0000000000000000000000000000000000000000 THEN 'burn'
        END AS supply_direction,
        tx_hash,
        amount,
        amount_usd
    FROM tokens.transfers
    WHERE block_month >= date_trunc('month', DATE '{{start_date}}')
      AND block_month <= date_trunc('month', DATE '{{end_date}}' - INTERVAL '1' DAY)
      AND block_date >= DATE '{{start_date}}'
      AND block_date < DATE '{{end_date}}'
      AND upper(symbol) IN ('USDT', 'USDC', 'DAI', 'USDE', 'PYUSD', 'FDUSD', 'TUSD')
      AND (
          "from" = 0x0000000000000000000000000000000000000000
          OR "to" = 0x0000000000000000000000000000000000000000
      )
      AND amount IS NOT NULL
)
SELECT
    CAST(event_date AS varchar) AS event_date,
    blockchain,
    asset_symbol,
    token_address,
    supply_direction,
    count(*) AS transfer_count,
    approx_distinct(tx_hash) AS transaction_count,
    sum(amount) AS amount_native,
    sum(amount_usd) AS amount_usd
FROM issuance_legs
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5
