# 加密貨幣訓練資料：按交易所嚴格隔離

以 2026-09-20 22:27 Asia/Taipei 的本機檔案為準。新的訓練契約是 **Bybit 只看 Bybit、OKX 只看 OKX、Binance 只看 Binance**；跨交易所與總體／ETF 等共用資料保留在來源庫，但不進這三個模型。舊的 [混合來源盤點](../artifacts/data_quality/crypto_training_inventory_2026-09-20/feature_inventory.md) 僅作歷史探索，不再描述現在的訓練配置。

各交易所獨立清單：[Bybit](../artifacts/data_quality/crypto_venue_training_inventory_2026-09-20/bybit/feature_inventory.md)（含逐欄有值數）、[OKX](../artifacts/data_quality/crypto_venue_training_inventory_2026-09-20/okx/feature_inventory.md)、[Binance](../artifacts/data_quality/crypto_venue_training_inventory_2026-09-20/binance/feature_inventory.md)。各目錄另有 `feature_inventory.csv`、`summary.json`；OKX/Binance 還有 `source_catalog.csv`。這些是來源／特徵盤點，不是已訓練或全市場完整的證明。

## 決策時鐘與來源

- 主標的是 Bybit 當前標準線性 USDT 永續；00:00 UTC 只見前一完整 UTC 日，00:05 UTC 以執行 K 棒計價，funding 現金流只進 forward label，不能提前進 feature。
- 同一結算／交易契約的主表最早為 2020-03-26；訓練設定從 2020-03-27 開始，保留前日上下文。Bybit 2018 起的三個 inverse 合約不能與此線性 USDT 合約直接合併。
- OKX、Binance 各有獨立 1m 價量基準，目前只選同交易所 OHLCV 衍生的 15 欄。來源中的 mark、index、funding、OI 等輔助欄仍保留在各自資料目錄；在逐欄因果可用性與商品化執行／費用驗證前不直接選入模型。
- 共用公開資料及其他交易所欄位目前不進任何「單交易所」模型；若未來要研究它們，須另立明示跨市場的實驗與 checkpoint 契約。

## 目前實況與缺口

| 物件 | 實測首日 | 實測末日 | 狀態 |
|---|---|---|---|
| Bybit 所有永續 1m | 2018-11-14 | 2026-09-20 | 867 檔，含 inverse；不是主訓練宇宙 |
| Bybit 線性 USDT 日表與純 Bybit funding 特徵 | 2020-03-26 | 2026-09-04 | 各 401,767 列；日表落後原始 1m |
| Bybit funding | 2020-03-25 | 2026-09-04 | 397 檔；落後原始 1m |
| Binance USD-M 1m | 2019-09-08 | 2026-09-20 10:47 UTC | 574 base、574 hot-tail 檔；62 個來源 schema 欄，15 個模型衍生欄 |
| OKX SWAP 1m | 2019-10-01 | 2026-09-20 14:25 UTC | 483 base、172 hot-tail 檔；46 個來源 schema 欄，15 個模型衍生欄 |

Bybit 日表中有 388,270 列同時通過完整分鐘格、可交易、可執行且未隔離條件；397 列 forward return 被隔離。這只是候選列數，還未套入每年 walk-forward、lookback 和標的遮罩。對截至 2026-09-20 的原始 1m，最後可完成的 UTC 訓練日理應是 2026-09-19；日表距它落後 15 個完整日。

Bybit 新設定 [`bybit_perpetual_daily_0005_historical_pit_v1.yaml`](../configs/markets/bybit_perpetual_daily_0005_historical_pit_v1.yaml) 現在選 15 個基礎價量欄與 7 個純 Bybit funding 欄，並為後者各加缺值遮罩，合計 **29 個模型輸入欄**；外部表的 schema 僅有 `date`、`symbol` 和 7 個 `crypto_bybit_*` 欄。OKX 與 Binance 分別用 [`okx_1m_venue_only_v1.yaml`](../configs/markets/okx_1m_venue_only_v1.yaml) 和 [`binance_1m_venue_only_v1.yaml`](../configs/markets/binance_1m_venue_only_v1.yaml)，各選 15 個同站價量衍生欄、無外部表。三者各有獨立 artifact/cache 根。OKX/Binance 現階段是**價量研究基準**，未驗證永續合約資金費、費率、逐分鐘成交與 funding-adjusted label，不能與 Bybit 00:05 執行績效直接比較。

注意：OKX/Binance 的 1m panel 沿用程式中的 `*_logret_1d` 欄名，實際比較的是相鄰 **1 分鐘列**，不是一天；名稱屬既有模型 ABI，訓練解讀必須以資料頻率為準。

現有主策略宇宙並非所有 Bybit 加密貨幣：在 **2026-09-04 的舊 instrument 快照**另有 **124 個 `symbolType=innovation` 的線性 USDT 合約**有 1m 原始檔，但既有 funding／日表的 standard filter 把它們排除；今日的實際數量尚待新快照確認。另有 2 個僅保留在物理日表的歷史檔，未列入現行 395-symbol 名單。關閉或下架合約仍可能缺漏，因為現行 instrument 發現只查 `Trading`。這是存活者偏誤缺口，未完成獨立合約身分／退市資金費率稽核之前，不把它們冒充本版可訓練宇宙。

「有 schema」不等於「有值／完整」：Bybit 清單有逐欄 `finite_rows`、`nonzero_rows`；OKX/Binance 使用 Parquet footer 與官方來源 catalog 的低成本盤點，沒有逐欄 finite 與完整分鐘格驗證，所以 `completeness_verified=false`。base 與 hot-tail 有重疊可能，不能相加當唯一列數。基礎 15 欄在 panel build 時計算，沒有虛構逐欄 finite 筆數。

## 自動維護

`stockagent-crypto-training-refresh.timer` 每日 02:30、16:30、22:30 Asia/Taipei 嘗試：Bybit funding 增量補抓 → 00:05 日表 → **純 Bybit** funding 特徵 → 歷史來源稽核 → Bybit 單站逐欄盤點 → 經 catalog gate 發布 Bybit 冷資料。原 10:30 重試在台股交易時段曾耗用約 269 CPU 秒與 21.3 GiB 記憶體峰值，已移除；開機漏跑不在盤中補執行，下一個正式時段才重試。若原始寫入程序仍在執行會暫緩並留收據；OKX/Binance 原始下載器依既有服務增量跑，這條 Bybit timer 不替它們重建訓練表。

重跑盤點：`source scripts/runtime_env.sh && run_fintech_python scripts/report_crypto_training_features.py --output-dir artifacts/data_quality/crypto_venue_training_inventory_2026-09-20/bybit`；OKX/Binance 分別跑 `run_fintech_python scripts/report_crypto_venue_1m_features.py okx` 與 `... binance`。訓練入口對 `crypto_exchange_scope` 驗證資料根路徑、所選欄位與外部 Parquet schema；舊 checkpoint 不會被這個 opt-in 契約改寫。

資料監控面板原有 OKX／Bybit／Binance 獨立分組；現在額外註冊純 Bybit 訓練表到 Bybit 組，舊混合研究表則放在跨市場公開研究組，不作為任何單站分組的主列數，也不作為本版訓練輸入。

截至本次盤點，Bybit 日表尚未從 9/4 推進至原始 1m 的 9/19 完成日；**未啟動全量模型訓練**。不能把 1m 下載摘要日期當成訓練資料日期，也不能以隔離契約取代逐 symbol 的歷史、PIT 與執行驗證。
