# 永豐個股期貨 2020/03/23 起資料準備

使用者於 2026-09-08 要求只從永豐有的期貨歷史開始準備。
[永豐官方文件](https://sinotrade.github.io/zh/tutor/market_data/historical/#_3)
列出期貨歷史起點為 2020-03-22；所選日盤來源的第一個交易日為 2020-03-23。

## 本次重新核對結果

2026-09-08 23:52 台北時間，正常 builder 已逐檔重驗原始 Tick／憑證，完成全部
1,573 個交易日，範圍為 2020-03-23～2026-09-04。16 項資料完整性檢查通過，
但來源覆蓋仍為 `partial`，**正式訓練尚未就緒**。

| 項目 | 數量 |
|---|---:|
| 候選期貨商品／標的股票 | 311／263 |
| 至少有一天核對通過的分鐘商品 | 297 |
| 通過分鐘核對的合約日 | 352,864 |
| 日線近似合約日 | 0 |
| 缺少來源憑證 | 6,734 |
| 來源空回覆 | 1,119 |
| 未核實近月實體月份 | 20,250 |
| OHLC 身分核對不符 | 321 |
| 待補齊／核對合約日合計 | 28,424 |

更改起點排除原先 9,299 個歷史範圍外缺口；重新讀取已下載來源後，其餘缺口數量
沒有減少。不能把日期縮短解讀為所有分鐘資料均已取得。1,573 天均至少有一個候選
未通過，現有全候選市場的正式 `--check-data-only` 仍應退出 1。

47 項相關回歸測試通過；本機與 vastai1T 的嚴格 CUDA 環境檢查皆無失敗或警告。
這次沒有改寫模型、帳務或訓練迴圈，也沒有啟動正式訓練。
分鐘 manifest SHA-256：`f369a25fb19166605f69971c0716ac9e55e26018fb4776892563341493765580`。
逐商品／年度 coverage、16 項檢查、交付檔案 SHA 與遠端檢查紀錄保存在下方操作目錄，
以 `preparation_audit.json`、`delivery_receipt.json`、`remote_preflight.log` 為證據。

## 訓練範圍與規則

- 沿用 `configs/markets/tw_stock_futures_day_trade_0845_historical.yaml` 與一般 `train.py`。
- `data.panel_start_date: 2020-03-23`，`walk_forward.expected_first_year: 2020`。
  Lookback 保持 32，模型需累積足夠前日觀測才會有第一個訓練樣本；不填補早期視窗。
- 配對日線與股票特徵維持原先固定版本，這次準備範圍到 2026-09-04。
- 08:45 使用前日完成特徵決策，08:46 完成分鐘棒進場；13:20 限價出場、13:24
  撤換市場單，13:25 起消耗分鐘容量，13:30 為最後平倉期限。
- v2 的 `daily_proxy_before: 2020-01-01` 僅保留既有相容性契約；新資料起點在其後，
  全部訓練日期均要求分鐘證據，不含早期日線近似。不把缺資料改為無成交或零報酬。
- 原 2014 起資料、舊設定備份、既有訓練／效能驗收產物全部保留。

## 路徑

| 用途 | 路徑 |
|---|---|
| 新分鐘準備版本 | `artifacts/data_preparation/futures_minutes_from_20200323_20260908/` |
| 配對日線版本 | `artifacts/data_preparation/futures_daily_70a57dd76de74fd0a365/` |
| 新訓練產物 | `artifacts/markets/tw_stock_futures_day_trade_0845_from_20200323_v2_vast5090/` |
| 重新核對與遠端交付證據 | `artifacts/operations/vastai1t_futures_from_20200323_20260908/` |

準備版維持私人、未發布資料角色，含 `coverage.parquet`、`gaps.parquet` 與來源 SHA。
只在完整來源驗收通過後才能宣稱正式開訓就緒；排程批次完成不代表資料完整。

## 一般指令

本機用同一個 builder 重新驗證原始來源並整理：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/build_tw_stock_futures_0900_entries.py \
  --config configs/markets/tw_stock_futures_day_trade_0845_historical.yaml \
  --shioaji-ticks-root data_tw_futures/shioaji_history
```

vastai1T 使用同一個一般入口檢查：

```bash
cd ~/stockAgent
source scripts/runtime_env.sh
run_fintech_python train.py --config configs/markets/tw_stock_futures_day_trade_0845_historical.yaml --check-data-only
```

只有檢查通過後，才移除 `--check-data-only` 開始正式訓練。此次不自動啟動 1000 epochs。
既有雙 RTX 5090 最佳化設定保留；舊效能測量的早期日線樣本範圍不代表新分鐘範圍的速度。
