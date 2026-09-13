# TW Day-Trade Last-Only 正式訓練就緒紀錄（2026-09-12）

## 1. 執行進度

**READY：資料修復、不可變發布、Syncthing 收斂、vastai1T 物化、嚴格資料閘門、CUDA/DDP 測試，以及真實資料單 fold／單 epoch 端到端驗收都已完成。** 正式 1000-epoch、11-fold 訓練尚未啟動；正式輸出目錄只有 `startup_timing.jsonl`，沒有 checkpoint、model 或 fold completion，可直接使用本文最後的命令開始。

### 遠端產物根統一

- vastai1T 的可部署市場訓練產物統一實體存放於 `/root/stockAgent/artifacts/markets`。
- `/root/stockAgent-daytrade-training-20260910/artifacts/markets` 已改為指向上述 canonical 根的 symlink，因此從訓練 worktree 使用相對 `artifacts/markets/...` 也會落在同一位置。
- 既有 batch16 candidate 的 19 個檔案、30,254,794 bytes 已同檔案系統原子搬移；搬移前後整樹 SHA-256 都是 `1218b4031e07f6f9fa996927254e2059f6df9f00283df7b006072db206e08a98`。
- candidate、batch16 resume、batch32 與 last-only 四份 deployment config 均已由正式 `load_config()` 解析，resolved physical output 全部位於 canonical `artifacts/markets`。
- `artifacts/cache`、`artifacts/acceptance` 與 `artifacts/benchmarks` 仍分開保存；它們是可重建 cache 或驗收／測速證據，不會混入可部署 model roots。

## 2. 固定資料版本

### `tw-public`

- snapshot：`tw-public-20260912T120059377078150Z-l0-penguin-fb318c2349f76952`
- manifest SHA-256：`01bb05cd653c5c69b3a17c6db387d25f05c5f17bea943c4b453eb6cc20cc47cb`
- inventory SHA-256：`fb318c2349f76952eee3533ee0ed4c8bc95995ca427dc70ea3c93d87918c21a7`
- 來源：129,700 files、9,843,912,360 logical bytes
- packed 驗證：1,647 payload objects、13,150,136,129 verified bytes
- vastai1T 狀態：`hot-current`、`ready=true`、`pinned=true`
- pin：`/srv/stockagent-packed-materialized/pins/tw-day-trade-last-last-only-pending-stock-v3-tw-public.pin.json`

### `tw-minute-train`

- snapshot：`tw-minute-train-20260910T121601063496311Z-l0-penguin-09bedd96a2f68c39`
- manifest SHA-256：`ee16387fb1161edf228b87120dc7e112dcd4d6541148ff460a0c9d84bf8e72da`
- inventory SHA-256：`09bedd96a2f68c39a1c636b9ce5967890c35afcf9c33dc169a07e859aa832a9e`
- 來源：3,189 files、13,817,707,306 logical bytes
- packed 驗證：900 payload objects、14,015,770,970 verified bytes
- vastai1T 狀態：`hot-current`、`ready=true`、`pinned=true`
- pin：`/srv/stockagent-packed-materialized/pins/tw-day-trade-last-last-only-pending-stock-v3-tw-minute-train.pin.json`

兩個 pin 都綁定精確 snapshot，不會在訓練途中重新解析 `latest`。vastai1T 保持 index-only edge：物化資料保留，重複 packed payload 在驗證後已安全 prune 為 0 bytes。

## 3. 公司行動資料修復與品質

MOPS 歷史公告可能把句點放在配股數字與「股」之間，例如 `每仟股無償配發99.2436.股`。下載器已改為只擷取合法十進位數字，將句點留在數值外；`4306 / 2014-08-04` 的真實 receipt 可正確解析為 `stock_ratio=0.0992436`。

新版 schema v5 重建結果：

- 覆蓋：2014-01-01 至 2026-09-11
- reference rows：23,185，date+symbol 唯一
- company-year requests：3,533
- discovered / parsed details：4,528 / 4,528
- `failure_count=0`、`coverage_complete=true`、`baseline_established=true`
- exact cash events：18,689
- exact inventory events：7；全部具有正比例、合法 `stock_delivery_date` 且交付日不早於事件日
- 無法以官方明細證實的 stock delivery：2,349 筆維持 avoid/mask，不倒推或捏造條款
- parquet SHA-256：`8f85f4562c08428dc96037e88c36187d4b2c7cb45a21c7dbea8a6017f5109677`
- raw receipt manifest：8,126 entries，SHA-256 `b64c66252b8b4b051cc7b4aecf66a495a260eeb5c16c13db4f37cec3b6b8409d`
- downloader 單元測試：33 passed

## 4. vastai1T 設定與閘門

- worktree：`/root/stockAgent-daytrade-training-20260910`
- 正式 config：`configs/deployments/tw_day_trade_last_last_only_training_vastai1t.yaml`
- config SHA-256：`353d55add594aa53052a9a11e484569f041f334078c627700ae41d74319b7eb6`
- 正式輸出：`/root/stockAgent/artifacts/markets/tw_day_trade_last_last_only_layernorm_minute50_margin_carry_capital10m_batch32_pending_stock_v3`
- 訓練契約：1000 epochs、11 個年度 walk-forward folds、BF16、2-GPU DDP、train batch 32、eval batch 16、10M 初始資金、50% 分鐘成交量、last pooling／last-only query
- 執行契約：官方開盤價做 sizing、09:01 分鐘價進場、13:20 限價、13:24 市價替換、13:30 auction deadline；日 K 代理使用官方 open/close 且不加不利 tick
- 跨日契約：physical FIFO margin inventory、dated cash claims、零股使用整張行情研究假設
- `check_environment.py --require-cuda --strict`：通過；Python 3.12.14、Torch 2.11 + CUDA 12.8、2 × RTX 5090、BF16
- 遠端聚焦測試：167 passed、2 skipped；第二組 207 passed

`train.py --check-data-only` 已以退出碼 0 通過：

- 3,096 sessions、2,754 symbols、99 features、11/11 folds
- `minute_symbol_days=2,798,943`
- `daily_proxy_symbol_days=2,986,868`
- `source_gap_symbol_days=0`
- `unresolved_action_gap_symbol_days=4,222`（按契約 mask，不製造資料）
- physical source ABI：`tw_day_trade_physical_source_cache_v9_pending_stock_delivery`
- carry release：`tw-day-trade-carry:9edcd0a339a1dfa69da8f13f4466f7bc3e0e798ee675fa69c3485325e8510a73`
- cache manifest SHA-256：`3cf1cc0499306b81bd1b4db67549580a27a62c3eee6775a672545bfd2d5f24ac`
- READY SHA-256：`2c07149561f7fedbe870ec5bcf558695e0741fd69d5f16037d587f23dff40000`

## 5. 真實資料端到端 smoke

驗收使用獨立目錄 `artifacts/acceptance/pending-stock-v3-last-only-smoke-20260912`，不污染正式輸出。命令以退出碼 0 完成 fold 1（train 2014、validation 2015、test 2016–2026）的一個 epoch：

- BF16、2-GPU DDP、預訓練權重 identity-by-feature-name 映射：通過
- compiled model forward/backward：通過
- exact physical-FIFO loss forward/backward：通過
- 訓練 epoch：7 batches，約 16.53 秒；fold 全流程約 396.79 秒
- `fold_complete.json`：`status=complete`
- final test：2,606 sessions，2016-01-04 至 2026-09-11
- `minute_nav`：`(2606, 270)`，全為有限值
- `shares_history`：`(2606, 2754)`
- `carry_inventory_cohorts`：`(2606, 2754, 12)`
- `carry_inventory_claims`：`(2606, 2754, 3)`
- `final_alive=true`、`carry_alive=true`
- settlement defaults：0；carry failures：0；outstanding cash claims：0
- checkpoint SHA-256：`8d1e0a3bbc4358d1b82c7e0e80248e777e02d626736698df77841c5936416ca8`
- model SHA-256：`32a2455008f4fe79c4d55cd68a5c57f14c4b7728ef900a0097cf83c768c02941`

一個 epoch 的報酬不具策略評估意義；此 smoke 只證明正式資料、模型、梯度、跨日會計、完整測試回測與 artifact lifecycle 可端到端執行。

## 6. Syncthing 與執行邊界

- penguin ↔ vastai1T：folder idle、completion 100%、need bytes/items/deletes 全為 0、pull/folder/system/watch errors 全為 0、`remoteState=valid`
- 傳輸：QUIC、TLS 1.3
- edge audit：`hydrating={}`、payload 0 files / 0 bytes、process references 空
- lab203 當時離線，因此本紀錄只證明 penguin ↔ vastai1T 與 vastai1T 的訓練就緒，不宣稱 lab203 已同步
- Vast container overlay 可承受 stop/start，但 instance recycle/destroy 後不保證存在；正式訓練完成後仍須經限定 artifact ingress 與冷發布驗收，不能只留在 Vast 本機

## 7. 直接在 vastai1T 啟動正式訓練

確認目前已登入 vastai1T，然後原樣執行：

```bash
cd /root/stockAgent-daytrade-training-20260910
source scripts/runtime_env.sh
run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_last_last_only_training_vastai1t.yaml
```

不要加入 smoke 使用的 `--epochs 1`、`--max-folds 1` 或 `--no-post-train-infer`。新的 `pending_stock_v3` output root 與舊 v2 optimizer 隔離；正式命令會依 config 執行完整 1000 epochs × 11 folds。
