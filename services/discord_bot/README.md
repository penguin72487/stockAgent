# stockAgent Discord Bot

Run the bot from the repository root:

```bash
/home/user/miniforge3/envs/fintech/bin/python services/discord_bot/bot.py
```

Required environment:

- `DISCORD_BOT_TOKEN`
- `DISCORD_CHANNEL_ID`

Useful commands:

- `/signal_now`
- `/positions`
- `/rebalance`
- `/health`

The scheduled send time defaults to `13:15` in `Asia/Taipei`, matching the
15-minute-before-close Taiwan workflow.
