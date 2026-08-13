# stockAgent Discord Bot

Run the bot from the repository root:

```bash
source scripts/runtime_env.sh
run_fintech_python services/discord_bot/bot.py
```

For this host, install the persistent service with:

```bash
bash scripts/install_discord_bot_service.sh
systemctl status stockagent-discord-bot.service
```

The service reads `services/discord_bot/.env`; keep that file mode `0600`.
Use `STOCKAGENT_SCHEDULED_MARKETS` to list only markets whose credentials and
model artifacts are ready. A market remains available to interactive Discord
commands when enabled even if it is not in that automatic schedule.

All Taiwan daily and day-trade market configs share
`scripts/refresh_tw_public_live_snapshot.py`. The updater writes only to the
mutable `/srv/stockagent-live/data_tw_public` tree, runs the strict causal-data
audit, publishes to the Syncthing/desync store, verifies a separately
materialized snapshot, then atomically switches the repository symlink. Check
the four day-trade modes with:

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_tw_day_trade_live_readiness.py
```

The durable report is written to
`artifacts/live/tw_day_trade_readiness/readiness.md`. Data readiness does not
substitute for a missing checkpoint or an unsupported execution contract.

The entrypoint runs with a built-in reload supervisor by default. It watches
Discord bot code/config, `stockagent/live`, `configs/markets`, and
`scripts/live_signal.py`; watched file updates restart the child bot process
immediately on the next watcher tick. Runtime artifacts are not watched, so
signal outputs and audit logs do not trigger restart loops.

Reload controls:

- `STOCKAGENT_BOT_RESTART_DELAY_SECONDS=0` controls file-change debounce.
- `STOCKAGENT_BOT_RELOAD_POLL_SECONDS=0.2` controls watcher responsiveness.
- `STOCKAGENT_BOT_CRASH_RESTART_DELAY_SECONDS=10` controls crash-loop pacing.
- `STOCKAGENT_BOT_RELOAD=0` disables the supervisor.
- `STOCKAGENT_BOT_WATCH_PATHS=a,b,c` overrides watched paths.

Required environment:

- `DISCORD_BOT_TOKEN`
- `DISCORD_CHANNEL_ID`
- `STOCKAGENT_MARKETS_DIR` defaults to `services/discord_bot/markets`
- `STOCKAGENT_DEFAULT_MARKET` defaults to `tw`

User Install:

- Requires `discord.py>=2.4,<3`. The command tree is synced globally once from
  `setup_hook()` and supports both Guild Install and User Install.
- Install Otto Suwen to a personal account with
  <https://discord.com/oauth2/authorize?client_id=1518140996930637877> and
  choose **Add to my apps**. No `bot`, `gdm.join`, or administrator scope is
  required for personal installation.
- User-facing slash commands are available in guild channels, Bot DMs, direct
  messages, and group DMs. Non-ephemeral responses are visible to everyone in
  the current conversation.
- `/set_market_enabled`, `/set_schedule`, and `/set_capital` remain Guild-only
  because they mutate shared Bot state and require administrator/trader access.

Market configs:

- One market per YAML file.
- `services/discord_bot/markets/tw.yaml` is the default Taiwan market config.
- Leave `fold_id` empty/null to use the latest `fold_*/checkpoint_best.pt`.
- Set `live_output_dir` per market so live outputs do not mix.
- `enabled` controls whether the bot can produce signals for the market.
- `timezone`, `open_time`, `close_time`, `schedule_time`, `summary_time`, and
  `data_ready_time` control market-hours scheduling and data freshness checks.
- 24/7 intraday markets such as crypto can set `schedule_interval_minutes: 15`
  instead of `schedule_time`; the bot deduplicates one alert per completed bar.
  `schedule_delay_seconds` waits briefly after bar close before running.
- Daily markets also run a private artifact backfill once per day. The default
  time is `data_ready_time`, then `close_time`, then `summary_time`, then
  `schedule_time`. This refreshes data, writes live signal artifacts, and syncs
  `live_signal_weights.parquet` even if nobody runs a command. Runtime state can
  override it with `artifact_backfill_time` or `backfill_time`.
- `history_frequency: bar` makes `/portfolio_history` and `/stock_history`
  show recent bars instead of collapsing artifacts to daily rows.
- `pre_signal_command` can run a data updater before scheduled signals. Crypto
  uses `downloader/download_okx_perp_15m.py --mode incremental` before alerting.
- `freshness_max_lag_days` is mainly for 24/7 crypto data; daily markets compare
  the latest parquet/benchmark date against the expected latest trading day.
  For 15-minute crypto, prefer `freshness_max_lag_minutes`.
- `trader_role_ids` / `trader_role_names` can grant restricted command access
  in addition to Discord administrator permission and the default `trader` role.
- `initial_capital` / `current_capital` can define default capital used for
  amount estimates. Runtime overrides can also be set with `/set_capital`.

Useful commands:

- `/ask question:...` verifies that Otto Suwen can be invoked from a personal
  installation, including direct messages and private group conversations.
- `/signal_now market:tw` answers from a reusable live artifact when it already
  matches the current market context. With `price_source:auto`, open stock
  markets use current Yahoo quotes when a new inference is needed; closed
  markets use the latest panel close. The command is fast by default and does
  not run the full market data updater when data is already current. If data is
  stale, it automatically runs the configured updater first. If the updater
  fails or the panel remains older than the expected latest trading day after
  refresh, the bot stops instead of emitting a stale signal.
- `/signal_now market:crypto refresh_data:true` runs the configured data updater
  first, then generates the signal.
- `/watch action:add market:tw symbol:2330` adds a symbol to your personal
  watchlist and enables watchlist-only DM alerts for that market.
- `/watch action:update market:tw symbol:2330 new_symbol:2317` replaces a
  watched symbol; `/watch action:remove ...` or `action:delete` removes one.
- `/watch action:enable market:tw` enables personal watchlist alerts, and
  `/watch action:disable market:tw` disables them without deleting the list.
- `/signal signal_id:...`
- `/positions market:tw limit:0 page_size:20 current_capital:1000000` shows
  paged current/target weights and estimated position amounts.
- `/rebalance market:tw limit:0 page_size:20 current_capital:1000000` shows
  paged rebalance deltas with estimated trade amounts.
- `/portfolio_history market:tw days:32 top_changes:5` shows recent PnL,
  cumulative return, exposure, position counts, and the largest holding changes
  from fold artifacts. Daily markets use days; crypto can use 15-minute bars.
  Add `initial_capital` to scale from the fold's first NAV, or
  `current_capital` to scale from the latest fold NAV.
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

Scheduled markets default to every YAML under `services/discord_bot/markets/`.
Use `STOCKAGENT_SCHEDULED_MARKETS` only when you want to restrict the set; set
`all` for every market, or an explicit list such as `tw,us,crypto`.
Each market uses its own configured timezone. Daily markets use
`schedule_time`; interval markets use `schedule_interval_minutes`.
Public scheduled broadcasts are disabled by default
(`STOCKAGENT_PUBLIC_BROADCASTS=0`). Scheduled signals still run for artifacts
and personal `/watch` DM alerts. Set `STOCKAGENT_PUBLIC_BROADCASTS=1` only when
you want the bot to post automatic summaries/details to the shared channel.
For daily markets, the private artifact backfill loop runs independently from
public broadcasts and retries failed runs after
`STOCKAGENT_SCHEDULED_RETRY_DELAY_SECONDS` seconds, default `60`.
If the updater is rate-limited or otherwise produces mostly failed rows, the
backfill is treated as failed and no live signal artifact is written from stale
data.
