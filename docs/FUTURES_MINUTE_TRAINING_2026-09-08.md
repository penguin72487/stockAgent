# 個股期貨分鐘資料與訓練接入驗收

本次工作延續 2026-09-07 的使用者要求。新設定為
`configs/markets/tw_stock_futures_day_trade_0845_historical.yaml`，沿用
`train.py`、FinancialTransformer、多基底前日特徵、BF16、整數口帳務與年度 walk-forward。
完整規則與準備指令見 [個股期貨分鐘訓練](tw_stock_futures_day_trade_minute.md)。

**後續範圍調整：使用者已要求只從永豐有的歷史開始。** 同一 historical YAML
現改為 `panel_start_date: 2020-03-23`、walk-forward 起年 2020，獨立產物目錄；
本頁下方 2014 起的數量是原快照驗收紀錄。新範圍與交付狀態見
[2020/03/23 起資料準備](futures_from_20200323_preparation.md)。不延長日線近似分界，
新範圍全部使用原分鐘時鐘。

**2026-09-08 09:00 台北時間驗收結果：既有資料已整理完成，資料檔案與來源對應檢查
通過；全市場歷史訓練尚未就緒。** 37,723 個候選合約日仍缺來源或身分核對證據。
正式 `--check-data-only` 退出碼為 1，於 GPU 訓練前列出缺口，沒有刪掉缺日或改用日線。

## 本次全量整理的實際範圍

| 項目 | 實測結果 |
|---|---:|
| 候選歷史日期 | 2014-01-02～2026-09-04，3,093 個交易日 |
| 候選商品／股票 | 417 種期貨商品／364 檔標的股票 |
| 已核對分鐘資料的日期 | 2020-03-23～2026-09-04 |
| 至少有一天核對通過的分鐘商品／股票 | 297 種商品／250 檔股票 |
| 全日盤已觀測分鐘棒 | 25,574,357 筆 |
| 08:46、13:20～13:30 執行事件棒 | 1,083,437 筆 |
| 候選合約日總數 | 698,324 |
| 2020 年以前日線近似合約日 | 307,737 |
| 已核對通過的分鐘合約日 | 352,864 |
| 待釐清合約日 | 37,723 |

這裡的「商品」按期貨商品代碼計算，標準／小型分開；「合約日」是某個實體月份契約
在某一天的一筆需求。這不是本機全部期貨、R1/R2 或到期月下載器的總清單。
297 種有分鐘資料的商品中，295 種仍有候選日缺口；另外 105 種僅涵蓋早期日線近似，
不能把它們加上去宣稱分鐘下載完整。只有 2 種含分鐘資料的商品在本次候選期間沒有
未解缺口；這仍是來源與契約核對結果，不是全交易所逐筆成交量完整性證明。

| 待釐清原因 | 合約日數 | 下一步需要的證據 |
|---|---:|---|
| 缺少有效來源憑證 | 15,595 | 對應日期、連續別名的有效資料與憑證；部分日期超出永豐提供範圍 |
| API 回傳空資料 | 1,119 | 補查或其他來源，不能直接判定市場無成交 |
| 近月實體身分未核實 | 20,688 | 核對逐日月份與商品生命週期；其中 20,037 筆官方日線沒有觀測列，651 筆有列但月份不一致 |
| 來源 OHLC 與官方日線不符 | 321 | 核對實體月份、成交內容與原始檔修訂，不能放寬檢查直接納入 |

其中 9,299 筆落在 2020-01-01～2020-03-21，另外 28,424 筆在永豐公布的歷史起點
之後。因此只延長日線近似到 2020 年 3 月，也無法使整份資料通過。
目前完全涵蓋的 1,471 個日期皆在早期日線區間；2020 年以後尚無全候選商品皆核對
通過的日期。保留全部候選與缺口，避免以事後資料可得性挑選訓練市場。

年度、逐商品與逐合約明細：

- [逐商品起迄、分鐘天數及缺口 CSV](../artifacts/operations/futures_minute_training_20260907/coverage_by_product.csv)
- [逐年狀態 CSV](../artifacts/operations/futures_minute_training_20260907/coverage_by_year_status.csv)
- [37,723 筆未解合約日 CSV](../artifacts/operations/futures_minute_training_20260907/unresolved_contract_days.csv)
- [全量資料驗收憑證](../artifacts/operations/futures_minute_training_20260907/full_preparation_receipt.json)
- [正式訓練設定的資料檢查結果](../artifacts/operations/futures_minute_training_20260907/full_check_data_20260908.log)

四個成品的 SHA、列數、候選鍵集合、日期分界、分鐘鍵唯一性、數值、逐合約來源 SHA、
原始 Tick 聚合量與執行事件子集等 17 項檢查通過。成品 manifest SHA-256：
`146e419ed5eb9e5aaaa5bd64a9add700b55a267d873fd63211c461b9e0c80e5c`。
這是逐日建置時核實的來源快照；下載器之後新增或修訂原始資料，須重新正常建置才能納入。

既有兩個期貨補查服務的排程已實查：本次觀測為 `waiting / live_priority_window`，
2026-09-08 14:31 台北時間恢復背景查詢。沒有為了準備訓練而重啟或繞過盤中保護。
此排程不代表上述資料必定能補齊，特別是歷史範圍外、商品身分或非空價格不符問題。

## 已實作的日期分界

| 日期 | 執行假設 |
|---|---|
| 2020-01-01 以前 | 缺少永豐分鐘歷史，以同一實體期貨的日盤 Open 進場、Close 出場；按先前交易日成交量限制容量 |
| 2020-01-01 起 | 08:45 決策；08:46 完成棒進場；13:20 限價、13:24 撤換、13:30 截止 |

兩段都保留原本的多空權重、雙邊費用、逐口四捨五入交易稅、標準／小型契約與餘額現金。
每日模型不讀當天股票開盤、期貨最高最低收盤或執行結果。日線近似不偽裝成分鐘棒，
也不聲稱已證明 13:30 能成交。2020 年以後缺來源會阻擋完整訓練，不當作零報酬。
原有嚴格分鐘 v1 與早期日線 v1/v2、09:00 控制組保留，不能與新契約 v2 共用續訓產物。

## 2020 年以前的來源

- [永豐歷史行情文件](https://sinotrade.github.io/zh/tutor/market_data/historical/#_3)
  公布期貨歷史起點為 **2020-03-22**；本機日盤 Tick 的起點為 2020-03-23。
  無法以同一 API 補出 2014–2019，也不能宣稱 2020 年 1–3 月已補齊。
- [期交所歷史資料申購](https://www.taifex.com.tw/cht/3/hisAppForm)所附價目與起迄表
  列出期貨成交簡檔自 1998-07-21 起、每半年 NT$1,000；個別商品仍依實際上市時間。
  其成交日期、實體到期月、時間、價格與 B+S 數量足以重建分鐘成交棒，須按官方格式
  換算撮合口數。尚未購買或申請授權。

原始官方文件保存於 `artifacts/operations/futures_minute_training_20260907/`：
`taifex_historical_availability.odt` 與 `taifex_simple_trade_format.odt`。
價目表 SHA-256 為 `92b3e5a9742267d97b5d20684c4161af1bc53f5b78dcd43b4d225c41ad66247c`。

## 資料與檢查方式

`data_tw_futures/taifex_stock_futures_minute_history_v2/` 保存已核實的近月個股期貨
全日盤分鐘、執行事件分鐘、逐合約日 coverage、gaps 及 manifest。此範圍是既有
股票全市場特徵驅動之近月個股期貨訓練，不把其他期貨模式或全部 R2 別名混稱為已完成。

建置先依官方逐日月契約排序決定 R1 對應身分，再核對 OHLC、查詢日期、別名、
檔案 SHA、憑證 SHA 與列數；不把查詢當下的 target code 套回過去。
成交量只用實際 Tick 觀測值，即使小於官方日成交量也不放大。

逐日分片在 `artifacts/cache/futures_minute_history/`。重新建置會驗證快取對應的原始
Tick 和憑證；未變更的合約日重用，缺口或更新的來源重查。相同內容重建不改 manifest
位元組與 SHA。新資料仍由 catalog 排除於 cold release；完整來源驗收與固定 release
之前，不宣稱 Vast 已收到可正式訓練的版本。

合併使用單次多檔 Parquet 掃描，避免數千個獨立查詢計畫拖慢合併。
`--assemble-cached` 僅用於中斷後組裝已核實的既有分片快照：每個日期分片與 coverage
都必須符合保存的 SHA，缺檔或毀損即停止；它不會重新掃描原始下載資料。
要納入新下載內容，使用下方不含此選項的正常建置指令。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/build_tw_stock_futures_0900_entries.py \
  --config configs/markets/tw_stock_futures_day_trade_0845_historical.yaml \
  --shioaji-ticks-root data_tw_futures/shioaji_history

run_fintech_python train.py \
  --config configs/markets/tw_stock_futures_day_trade_0845_historical.yaml \
  --check-data-only
```

驗收通過並固定來源 release 後，訓練仍使用一般入口：

```bash
run_fintech_python train.py --config configs/markets/tw_stock_futures_day_trade_0845_historical.yaml
```

## 已完成的工程驗收

- 2026-09-08 最終版本：203 項相關測試通過；包含快照組裝一致性、正常重建刷新來源、
  分片缺失／毀損拒絕。紀錄為
  `artifacts/operations/futures_minute_training_20260907/tests_assembly_final_20260908.log`。
- 真實資料小型測試：2330、2317 兩檔股票，2018–2021 共 96 個真實交易日切片、
  98 個前日特徵，CUDA / BF16、兩個年度 fold。降低樣本與 epoch 僅用於工程驗收。
- 各訓練組先完成 epoch 1，再恢復 optimizer、scheduler、RNG 等狀態接續 epoch 2；
  曲線 epoch 序列均為 `[1, 2]`，沒有重複列。完成後重用兩個 fold 不改曲線 SHA。
- Canonical lifecycle 驗證 33 個必要產物，缺少 0、無效 0；包含 checkpoint、
  fold completion、回測與圖。累積 walk-forward 圖正常產出。
- 第二個 fold 在原分鐘容量規則下有 1 個無法完成沖銷的交易日，剩餘 4 口被保留為
  執行失敗並停止模擬帳戶。失敗懲罰不是實際投資虧損估計，也未拿日收盤價補沖銷。
- 相同來源重新整理，4 個 Parquet 及 manifest 的 SHA 全數不變。

詳細證據為
`artifacts/operations/futures_minute_training_20260907/real_smoke/final_smoke_receipt.json`；
測試產物在相鄰 `training_final/`。這些是資料接入、帳務、續訓與報告的工程證據，
不是全市場完整訓練或策略績效證據。
