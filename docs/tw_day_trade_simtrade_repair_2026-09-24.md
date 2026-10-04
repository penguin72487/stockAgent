# TW 當沖 `simtrade` 行情閘門修復（2026-09-24）

## 語意

- `simulation_only=true` / `production_order_possible=false` 才是本系統的紙上交易安全邊界。
- Shioaji streaming event 的 `simtrade` 是交易所「試撮」旗標；`false` 表示正式盤行情，不代表切換成真實下單。
- Shioaji request/reply `Snapshot` 沒有 `simtrade` 欄位。Snapshot 的正式盤證據改用：同日且 10 秒內的本機回覆時間、同日 09:00 後的交易所時間、以及 Shioaji Snapshot 來源。

## 根因與修正

1. 舊閘門對所有行情強制要求 `simtrade is False`，使合法 Snapshot 被誤擋為 `waiting_non_trial_quote`。
2. 全目標紙上成交不再建立大量 streaming 訂閱；改用最多 500 檔一批的 Snapshot，待處理目標每 0.5 秒重試。
3. 同一 Snapshot 的多檔成交改為一次 intent state commit、批次 orders/fills append、一次完成 state commit，避免每檔兩次完整 state 寫入。
4. watchdog 若發生在 ledger 已落盤、state 尚未落盤之間，重啟時以 `order_id` 驗證 orders/fills 成對後自動重播；單邊 ledger 仍 fail closed。
5. 網頁目前交易日使用即時 state 的重試成交數，不再被 09:00 immutable registration event 的零成交快照覆蓋。
6. Discord preopen base preparation 不再覆寫已完成的 final-arm receipt；兩階段都完成後停止重複準備。
7. 09:00 gate 同時接受 `causal_best_quote` 與 `causal_market_full_target_at_best_quote`，並檢查 pending、成交／未成交守恆及未完成庫存調整。

## 驗證

- `353 passed`：`test_tw_day_trade_simulation.py`、`test_tw_public_publication_schedule.py`、`test_discord_bot_formatting.py`。
- 2026-09-24 orders/fills：269 組 entry `order_id`，orders 與 fills 完全對稱，重複 ID 為 0。
- 所有服務為 active；Discord 22 個指令同步完成、Gateway connected；paper engine 與 Discord revision lag 為 0。
- `simulation_only=true`、`production_order_possible=false`、`ledger_integrity_ready=true`。

## 今日仍保留的真實未成交

- `7547` 已漲停鎖住，Snapshot 有最佳 bid、沒有最佳 ask；100m 尚待 2,000 股、projection-L1 GELU 尚待 128,000 股。系統不會捏造市價買進。
- 100m 在修復載入前已有 26 個前日庫存調整被舊閘門擋住；gate 保持 failed 並公開此缺口，不把後來修復冒充 09:00:15 前完成。
- 今日舊 final-arm receipt 已被修復前的 base phase 覆蓋，無法事後重建證據；程式修正從下一次 preopen 生效。
