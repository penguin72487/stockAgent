WITH asset_legs AS (
    SELECT
        block_date AS event_date,
        blockchain,
        project,
        upper(token_bought_symbol) AS asset_symbol,
        CAST(token_bought_address AS varchar) AS token_address,
        'buy' AS flow_direction,
        tx_hash,
        taker,
        amount_usd
    FROM dex.trades
    WHERE block_month >= date_trunc('month', DATE '{{start_date}}')
      AND block_month <= date_trunc('month', DATE '{{end_date}}' - INTERVAL '1' DAY)
      AND block_date >= DATE '{{start_date}}'
      AND block_date < DATE '{{end_date}}'
      AND amount_usd IS NOT NULL
      AND amount_usd >= 0
      AND upper(token_bought_symbol) IN (
          'BTC', 'WBTC', 'CBBTC', 'TBTC', 'ETH', 'WETH', 'STETH', 'WSTETH',
          'SOL', 'WSOL', 'XRP', 'USDT', 'USDC', 'DAI', 'USDE', 'PYUSD',
          'FDUSD', 'TUSD'
      )

    UNION ALL

    SELECT
        block_date AS event_date,
        blockchain,
        project,
        upper(token_sold_symbol) AS asset_symbol,
        CAST(token_sold_address AS varchar) AS token_address,
        'sell' AS flow_direction,
        tx_hash,
        taker,
        amount_usd
    FROM dex.trades
    WHERE block_month >= date_trunc('month', DATE '{{start_date}}')
      AND block_month <= date_trunc('month', DATE '{{end_date}}' - INTERVAL '1' DAY)
      AND block_date >= DATE '{{start_date}}'
      AND block_date < DATE '{{end_date}}'
      AND amount_usd IS NOT NULL
      AND amount_usd >= 0
      AND upper(token_sold_symbol) IN (
          'BTC', 'WBTC', 'CBBTC', 'TBTC', 'ETH', 'WETH', 'STETH', 'WSTETH',
          'SOL', 'WSOL', 'XRP', 'USDT', 'USDC', 'DAI', 'USDE', 'PYUSD',
          'FDUSD', 'TUSD'
      )
)
SELECT
    CAST(event_date AS varchar) AS event_date,
    blockchain,
    project,
    asset_symbol,
    token_address,
    flow_direction,
    count(*) AS trade_legs,
    approx_distinct(tx_hash) AS transaction_count,
    approx_distinct(taker) AS taker_count,
    sum(amount_usd) AS amount_usd
FROM asset_legs
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2, 3, 4, 5, 6
