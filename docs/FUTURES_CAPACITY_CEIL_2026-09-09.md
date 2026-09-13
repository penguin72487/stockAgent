# 50% 分鐘容量無條件進位

**最新狀態：使用者已同意接入隔離草案，正式 v5 已通過全部 1,573 日資料預檢。**
新資料版本為 `artifacts/data_preparation/futures_minutes_capacity_ceil_quarantine_v2_20260909`，
使用新 `contract_quarantine_v2_capacity_ceil_v5` 訓練產物目錄。
保留原 `2021-06-21 / LVF:202107` 隔離，另依明確同意加入
`2023-07-13 / PZF:202308` 與 `2024-01-10 / LIF:202401`。
同日其他商品、全部決策日期、原始分鐘與缺口證據均保留；沒有補造成交資料。

本次接入的 [資料預檢](../artifacts/operations/futures_ceil_scope_activation_20260909/preflight.log)、
[strict CUDA](../artifacts/operations/futures_ceil_scope_activation_20260909/environment.log) 與
[隔離／checkpoint 回歸：115 passed](../artifacts/operations/futures_ceil_scope_activation_20260909/tests.log)
均通過。[接入紀錄](../artifacts/operations/futures_ceil_scope_activation_20260909/activation.json)
記錄新 manifest SHA 與使用者同意範圍；正式訓練由下方指令啟動。

依使用者 2026-09-09 指示，新設定
[`tw_stock_futures_day_trade_0845_capacity_ceil_v5.yaml`](../configs/markets/tw_stock_futures_day_trade_0845_capacity_ceil_v5.yaml)
使用 `ceil(0.5 × 該分鐘實際成交口數)`，同時套用進場與退出。

| 實際成交口數 | 0 | 1 | 2 | 3 | 4 | 5 |
|---|---:|---:|---:|---:|---:|---:|
| v5 可用容量 | 0 | 1 | 1 | 2 | 2 | 3 |

只進位參與量容量；可負擔口數仍依實際資金向下取整，缺分鐘棒不產生容量。
13:30 清倉、費稅、原始資料、v4 特徵正規化及 v3 代理梯度均保留。
使用者後續將 v5 的 `training.epochs` 明確調整為 10000；既有 early stopping 仍適用。
這是使用者指定的模擬容量規則，不是交易所規定的成交保證。

## 實作

- 新增 `trading.tw_stock_futures_day_trade_minute_capacity_rounding: ceil`。
  舊設定預設 `floor`，保留原結果的重現能力。
- 資料預檢、panel attachment、訓練與驗證共同讀取相同的分鐘 tape；
  只有 tape 的可用容量進位，帳戶的資金限制沒有修改。
- 新規則寫入 trading fingerprint；不相容 checkpoint 禁止 resume。
  新 output root 名稱含 `capacity_ceil_v5`。實際既有 v4 checkpoint 的 trading
  contract 與目前 floor 實作逐欄相同。
- 建置器支援 `--capacity-rounding` 並從 YAML 繼承；ceil 下缺分鐘來源、
  但官方全日成交量為正的契約日仍列為未解決。建置快取另綁定新規則。
- 歷史 daily proxy 的前日容量保留 floor；本次 2020-03-23 起實驗沒有這類列。

## 真實失敗交易回放

使用原 v4 第一 fold 的最佳模型請求權重 0.0029248767532408237，重播
`2022-03-02 / 2888 / DDF:202203`，原價格與來源均未修改。

| 比較 | floor 50% | ceil 50% |
|---|---:|---:|
| 08:46 可用容量 | 5 | 6 |
| 13:28 可用容量 | 0 | 1 |
| 13:29 可用容量 | 0 | 1 |
| 實際進場口數 | 1 | 1 |
| 13:30 殘倉 | 1 | 0 |
| 帳戶執行狀態 | 失敗 | 有效 |

新規則下該筆 log 報酬約 −0.00002072757，仍為含成本虧損交易。
CPU、CUDA eager、CUDA compile，以及訓練代理開／關的精確 forward 均一致；
各情境梯度有限。[可重播腳本](../artifacts/smoke/futures_capacity_ceil_20260909/replay.py)、
[完整結果](../artifacts/smoke/futures_capacity_ceil_20260909/replay.json)。

## 接入隔離草案前的預檢限制（歷史紀錄）

最初容量計算完成時，完整歷史預檢仍被阻擋。原分鐘資料把以下兩筆列為
`official_subcontract_capacity`，原因都是官方全日僅成交一口，舊公式
`floor(0.5 × 1)=0` 能證明任一分鐘零容量：

| 日期 | 實體契約 |
|---|---|
| 2023-07-13 | PZF:202308 |
| 2024-01-10 | LIF:202401 |

改成 ceil 後，這個推論不成立，卻沒有可定位到分鐘的成交來源。
當時完整預檢列出這兩筆並中止；既有 `gaps.parquet` 是舊容量規則下的產物，
不會自行列出新失效的證明。本機沒有對應原始修復 KBar 檔及 Shioaji 查詢憑證。
當時尚未取得新增隔離授權，因此維持阻擋；後續使用者已明確同意上述草案。

若未來要恢復這兩個契約日，須先取得帶實體契約及來源 receipt 的一分鐘資料，
再產生新快照。已批准隔離範圍的 v5 現可使用以下命令驗證及訓練：

```bash
source scripts/runtime_env.sh
run_fintech_python train.py --config configs/markets/tw_stock_futures_day_trade_0845_capacity_ceil_v5.yaml --check-data-only
run_fintech_python train.py --config configs/markets/tw_stock_futures_day_trade_0845_capacity_ceil_v5.yaml
```

最初容量修正的測試與環境紀錄（當時仍有來源阻擋）：
[資料與 checkpoint 回歸：179 passed](../artifacts/smoke/futures_capacity_ceil_20260909/tests.log)、
[帳戶／梯度回歸：230 passed](../artifacts/smoke/futures_capacity_ceil_20260909/backtest_tests.log)、
[strict CUDA](../artifacts/smoke/futures_capacity_ceil_20260909/environment.log)、
[完整預檢阻擋原因](../artifacts/smoke/futures_capacity_ceil_20260909/preflight.log)。
舊 checkpoint、精確回測與來源 SHA 均保持一致，見
[保護驗證與待補資料清單](../artifacts/smoke/futures_capacity_ceil_20260909/verification.json)。
當時沒有啟動正式訓練；單筆可清倉仍不代表策略績效改善。
