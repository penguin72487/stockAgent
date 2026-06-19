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

Useful commands:

- `/signal_now market:tw`
- `/positions market:tw`
- `/rebalance market:tw`
- `/markets`
- `/health`

The scheduled send time defaults to `13:15` in `Asia/Taipei`, matching the
15-minute-before-close Taiwan workflow. Scheduled markets are controlled by
`STOCKAGENT_SCHEDULED_MARKETS`, for example `tw,us`.
