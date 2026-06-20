# stockAgent Discord Bot

Run the bot from the repository root:

```bash
/home/user/miniforge3/envs/fintech/bin/python services/discord_bot/bot.py
```

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

Useful commands:

- `/signal_now market:tw`
- `/signal signal_id:...`
- `/positions market:tw`
- `/rebalance market:tw`
- `/markets`
- `/health`
- `/daily_summary market:tw`
- `/set_market_enabled market:tw enabled:false`
- `/set_schedule market:tw schedule_time:13:15`

Operational files:

- Runtime overrides: `artifacts/discord_bot/state.json`
- Button/action audit trail: `artifacts/discord_bot/audit_events.jsonl`
- Detailed command tracebacks: `artifacts/discord_bot/errors.log`
- Live signal artifacts: each configured `live_output_dir`, usually
  `artifacts/live_signals/<market>/<asof_date>/<signal_id>/`

Scheduled markets are controlled by `STOCKAGENT_SCHEDULED_MARKETS`, for example
`tw,us`. Each market uses its own configured timezone and `schedule_time`.
