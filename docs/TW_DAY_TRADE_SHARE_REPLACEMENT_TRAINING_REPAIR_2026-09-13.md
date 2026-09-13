# 台股當沖跨日持倉／換股訓練修正驗收（2026-09-13）

## 結論

vastai1T 已具備正式訓練條件。新版公開資料、實體 FIFO 來源快取、兩張 RTX 5090 的嚴格環境閘門、資料完整性閘門，以及原本失敗的 fold 6 epoch 0 exact-execution 路徑均已實測通過。正式 1000 epochs 尚未啟動。

正式產物固定寫入：

`/root/stockAgent/artifacts/markets/tw_day_trade_last_last_only_layernorm_minute50_margin_carry_capital10m_batch32_share_replacement_v4`

舊 `pending_stock_v3` 產物保持不變，不允許沿用其 optimizer／checkpoint 狀態。

## 原始故障與第一性原因

原錯誤發生於 fold 6 的 transferred-strategy epoch 0 guard：

- 日期：`2020-10-07`
- 股票：`5493`（panel index `1866`）
- 故障：持倉仍存在，但 minute mark 與 close mark 皆缺失

`5493` 於 2020-10-07 至 2020-10-16 暫停交易，2020-10-19 恢復交易；事件是現金減資 6%，每 1,000 股舊股換 940 股新股，每舊股返還現金 0.6 元，現金付款日為 2020-10-23。舊來源 ABI 只辨識「有行情／缺行情」，沒有把下列四件事分開：

1. 市場當日不可交易。
2. 前一日實體持倉仍然存在。
3. 暫停期間應沿用最後可觀察估值，但成交容量必須為零。
4. 恢復交易日才依官方比例轉換股數並建立有日期的現金權利。

因此，問題不是單純補一個價格，而是 corporate-action lifecycle 未進入實體持倉狀態機。若直接 forward-fill 並允許成交，或把缺價視為零，都會製造不存在的交易或損益。

## 實作修正

### 公開資料下載與驗收

- `downloader/download_tw_share_replacement_reference.py`
  - schema／parser contract 升為 v3。
  - 分離「完整可執行條款」與「條款不完整但事件已知」；後者保留為保守 mask，不捏造股數、價格或付款日。
  - TWSE 重複列表採固定的最新 revision 收斂。
  - TPEx TLS 不使用 `verify=False`，改用 SHA-256 固定的官方 TWCA intermediate CA。
  - MOPS 日期解析支援明確的民國年發放日句型。
- `downloader/download_tw_corporate_action_entitlements.py`
  - 產出 pending-stock 所需的交付日期欄位，並由資料閘門驗證。

### 實體 FIFO 來源

vastai1T 的 `stockagent/data/tw_day_trade_carry_source.py` 使用：

`tw_day_trade_physical_source_cache_v11_share_replacement_session`

其行為為：

- 暫停交易日：entry／exit 容量為零，沿用上一個官方可觀察 mark。
- 完整換股事件：在「公告恢復日當天或之後的第一個實際交易 session」執行股數比例與現金權利轉換。
- 公告恢復日若為休市日：不把 action 寫到不存在的日期。
- 恢復日在 panel 之外：不污染較早歷史；若停牌已開始，只保留可證明的停牌狀態。
- 條款不完整：從空初始狀態保守封鎖至事件恢復，避免未知持倉穿越事件。
- 一般內部缺價：找出前後都有官方估值的 internal gap，按股票做 prefix exclusion；不插值、不製造成交量。
- source digest 納入換股 parquet、summary、mask 與 action arrays，防止舊 cache 被誤接受。

### 同步／快取競態

`scripts/manage_packed_edge.py` 對 fetch、materialize、prune、GC 加入跨程序 `fcntl` operation lock。排程 GC 遇到進行中的 hydration 會快速寫出 `edge_operation_locked` receipt 並延後，而不會刪除正在驗證的 packed objects。

此修正同時部署於 vastai1T：

- `/root/stockAgent-daytrade-training-20260910/scripts/manage_packed_edge.py`
- `/root/stockAgent/scripts/manage_packed_edge.py`（實際 cron 入口）

## 固定資料版本

- `tw-public` snapshot：`tw-public-20260913T030921278338896Z-l0-penguin-248d0869d7d53be4`
- inventory SHA-256：`248d0869d7d53be425094140f03f4b138329017e57c7646c789971c77af8cb02`
- manifest SHA-256：`f750e8dd608e7d3e1fe137057f274530959a0d11b0cbf060e839e763fc591681`
- `tw-minute-train` snapshot：`tw-minute-train-20260910T121601063496311Z-l0-penguin-09bedd96a2f68c39`
- physical source digest：`b158cef795b9dae6d087516b5895873c57e6b11073d0480c1cb94a9c00e36372`
- physical source manifest SHA-256：`4555e1b221940d6f33f53bf9243451c478557c134be553f591835ba6dfd4e285`

新 `tw-public` 已在 penguin 驗證 1,675 個 data objects、13,304,716,279 bytes，vastai1T materialization 的 `READY` 亦固定到相同 snapshot。

`5493` 的直接快取驗證結果：

- 2020-10-07～2020-10-16：270 個分鐘 mark 均為 41.6、成交容量為零。
- 2020-10-19：官方恢復交易 open 43.7、close 44.8；股數比例 0.94、每舊股現金 0.6、付款日 2020-10-23。

## 驗收證據

### 資料與環境

- `scripts/check_environment.py --require-cuda --strict`：通過；Python 3.12.14、PyTorch 2.11.0+cu128、2 張 RTX 5090。
- 正式 config 的 `train.py --check-data-only`：退出碼 0。
- panel：3,096 sessions × 2,754 symbols × 99 features。
- minute source-gap symbol-days：0。
- exact normal corporate actions：18,879。
- exact share-replacement events：183，其中 112 筆含現金權利。
- share-replacement halt symbol-days：1,337。
- unresolved action／valuation 採保守 mask，未插值價格或成交量。

### 測試

- penguin 下載器與 edge-cache 聚焦回歸：`86 passed`。
- vastai1T training／carry／checkpoint／pretrain／下載器／edge-cache 聚焦回歸：`352 passed, 2 skipped`。
- Ruff：通過。
- `git diff --check`：通過。
- 真實 edge lock 驗收：cron 路徑的 GC 在 lock 被持有時約一秒內以 `edge_operation_locked` 延後。

### 真實 fold 6 單 epoch 驗收

隔離產物：

`/root/stockAgent/artifacts/markets/tw_day_trade_last_last_only_layernorm_minute50_margin_carry_capital10m_batch32_share_replacement_v4_acceptance_fold06_epoch1`

結果：

- 原始 epoch 0 exact validation guard 已通過，`final_alive=true`、`settlement_default_count=0`。
- 一個完整 epoch、validation、2021～2026 full-horizon test、checkpoint 與 cumulative plots 全部完成；程序退出碼 0。
- epoch 1 wall time：39.94 秒；45 個 optimizer steps。
- `fold_06/fold_complete.json`：`status=complete`，test rows 1,382，日期 2021-01-04～2026-09-11。
- 最終 settlement audit：`final_alive=true`、open FIFO lots 0、outstanding cash claims 0。
- `checkpoint_best.pt` SHA-256：`452a04dadbc8a88b44a08be18cf39bf1d99bbd218cb6be64fafee426fcccc4fc`。
- `model.pt` SHA-256：`824640f1908547f1c831ed7d22e9aecd28cf99220e6d3ef0b8232d5839e3f1b7`。

單 epoch 報酬只用於整合驗收，不代表正式模型績效，也不應用來選模。

## 正式訓練命令

在 penguin 連入 vastai1T 後執行：

```bash
cd /root/stockAgent-daytrade-training-20260910
source scripts/runtime_env.sh

run_fintech_python scripts/check_environment.py --require-cuda --strict

run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_last_last_only_training_vastai1t.yaml \
  --check-data-only

run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_last_last_only_training_vastai1t.yaml
```

不可加入 acceptance 的 `--epochs 1`、`--max-folds 1` 或 `--output-dir` override；正式 config 會使用 1000 epochs、完整年度 folds、BF16、DDP、global batch 32，並寫入 v4 正式 artifact root。

## 持久性邊界

目前 vastai1T 的 `/root/stockAgent/artifacts` 位於 container overlay，`workspace_is_volume=false`。stop/start 會保留，但 recycle 或 destroy 會遺失；因此「全部寫入 `artifacts/markets`」已達成，卻不等於永久備份。完成的正式產物仍須通過既有、受限範圍的 artifact ingress／冷儲存發布流程後，才能宣告可在 instance 銷毀後復原。

## Git 再現性邊界

vastai1T 訓練工作樹目前基於 Git HEAD `2d399eccbab241ca8d57a6caf536456c0b60e27f`，但保留了既有的 107 個 modified／untracked 路徑；本次沒有替使用者擅自 commit 或清除其他變更。現有 run manifest 已固定 configuration 與 dataset fingerprint，但不包含完整 dirty diff，故目前狀態是「此節點可直接正式訓練」，不是「只靠該 Git commit 可在全新節點逐位元重建」。關鍵執行檔案另以 SHA-256 核對：

- `stockagent/data/tw_day_trade_carry_source.py`：`97ffbf8aa05c9d6af6d342f982e347a869e245ec0b6f2b44ae319706719ffc48`
- `stockagent/backtest/tw_day_trade_carry.py`：`db7973dc07e37a47a9843fa2147f3b6bba38d43027c2210f4280566b32018c03`
- `stockagent/training/day_trade_carry_bridge.py`：`705cd829e6b7c0c48edb53f00992b651c3f4256bcf90ba17a084dbdc4249a6cf`
- `stockagent/training/trainer.py`：`be9af0a896cbc92df2285847d3170e7086f9761ce7a6d30ce1e6fe0b4b93c72a`
- `stockagent/training/loss.py`：`a94238cbadb1ae70bdd29760c93479801434c70cec27d5c6416c6eceeda0ccfa`
- 正式 config：`acdd11b90fe530f8f8156b65b9919620e88e0c190a3cfe6a8f363a44b85c8ec9`
