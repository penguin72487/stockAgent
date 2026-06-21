# stockAgent Discord Bot

Run the bot from the repository root:

```bash
/home/user/miniforge3/envs/fintech/bin/python services/discord_bot/bot.py
```

The entrypoint runs with a built-in reload supervisor by default. It watches
Discord bot code/config, `stockagent/live`, `configs/markets`, and
`scripts/live_signal.py`; after the last watched file update it waits 10 seconds
and restarts the child bot process. Runtime artifacts are not watched, so signal
outputs and audit logs do not trigger restart loops.

Reload controls:

- `STOCKAGENT_BOT_RESTART_DELAY_SECONDS=10`
- `STOCKAGENT_BOT_RELOAD_POLL_SECONDS=1`
- `STOCKAGENT_BOT_RELOAD=0` disables the supervisor.
- `STOCKAGENT_BOT_WATCH_PATHS=a,b,c` overrides watched paths.

Required environment:

- `DISCORD_BOT_TOKEN`
- `DISCORD_CHANNEL_ID`
- `STOCKAGENT_MARKETS_DIR` defaults to `services/discord_bot/markets`
- `STOCKAGENT_DEFAULT_MARKET` defaults to `tw`

Market configs:

- One market per YAML file.
- `services/discord_bot/markets/tw.yaml` is the default Taiwan market config.
- Leave `fold_id` empty/null to use the latest `fold_*/checkpoint_best.pt`.
- Set `live_output_dir` per market so live outputs do not mix.
- `enabled` controls whether the bot can produce signals for the market.
- `timezone`, `open_time`, `close_time`, `schedule_time`, `summary_time`, and
  `data_ready_time` control market-hours scheduling and data freshness checks.
- `freshness_max_lag_days` is mainly for 24/7 crypto data; daily markets compare
  the latest parquet/benchmark date against the expected latest trading day.
- `trader_role_ids` / `trader_role_names` can grant restricted command access
  in addition to Discord administrator permission and the default `trader` role.
- `initial_capital` / `current_capital` can define default capital used for
  amount estimates. Runtime overrides can also be set with `/set_capital`.

Useful commands:

- `/signal_now market:tw`
- `/signal signal_id:...`
- `/positions market:tw limit:0 page_size:20 current_capital:1000000` shows
  paged current/target weights and estimated position amounts.
- `/rebalance market:tw limit:0 page_size:20 current_capital:1000000` shows
  paged rebalance deltas with estimated trade amounts.
- `/portfolio_history market:tw days:32 top_changes:5` shows recent daily PnL,
  cumulative return, exposure, position counts, and the largest holding changes
  per day from fold artifacts. Add `initial_capital` to scale from the fold's
  first NAV, or `current_capital` to scale from the latest fold NAV.
- `/stock_history market:tw symbol:2330 limit:32` shows recent per-symbol
  trade and adjustment records from the latest configured fold artifact. It
  joins model target weights, integer-share weights, holdings, and portfolio
  returns; `changes_only:false` shows the latest daily state rows instead of
  filtering to changes. `initial_capital` / `current_capital` use the same
  scaling rule as `/portfolio_history`.
- `/explain_signal market:tw` shows a readable explanation overview plus paged
  per-symbol decision details from the latest saved signal. Useful options:
  - `signal_id` inspects a specific saved signal.
  - `symbol` filters by code or name.
  - `action` accepts `actionable`, `all`, `BUY`, `SELL`, `REDUCE`, `EXIT`, or
    `HOLD`.
  - `sort_by` accepts `delta`, `score`, `target`, `return`, or `rank`.
  - `detail:full` shows multi-line readable rows; `detail:compact` is denser.
  - `attach_file:true` uploads the full markdown decision report.
- Trading-related pages clamp visible rows to at least 10 per page and include
  an investment warning. The warning is informational; still verify price,
  liquidity, fees, and risk before placing orders.
- `/markets`
- `/health`
- `/daily_summary market:tw`
- `/set_market_enabled market:tw enabled:false`
- `/set_schedule market:tw schedule_time:13:15`
- `/set_capital market:tw current_capital:1000000` stores a default current
  capital for amount estimates. Use `initial_capital` instead to scale from the
  fold start; `current_capital` takes priority when both are set. Passing `0`
  clears that value.

Operational files:

- Runtime overrides: `artifacts/discord_bot/state.json`
- Button/action audit trail: `artifacts/discord_bot/audit_events.jsonl`
- Detailed command tracebacks: `artifacts/discord_bot/errors.log`
- Live signal artifacts: each configured `live_output_dir`, usually
  `artifacts/live_signals/<market>/<asof_date>/<signal_id>/`
  - `summary.json`
  - `discord_message.md`
  - `target_weights.parquet` and `target_positions.md`
  - `rebalance.parquet` and `rebalance.md`
  - `decision_explanations.parquet` and `decision_explanations.md`
  - `decision_report.md`
  - `model_explanation.json`

Scheduled markets are controlled by `STOCKAGENT_SCHEDULED_MARKETS`, for example
`tw,us`. Each market uses its own configured timezone and `schedule_time`.
