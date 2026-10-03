# 免費加密貨幣資料與 Vast 訓練交付 — 2026-09-27

## 結論與啟動方式

Vast 的既有 v7 日頻多基底策略已通過資料預檢、雙 GPU 三個 epoch 短訓練，以及 fold 1 的完整 1,728 日測試回放。這是工程可執行性驗收，不是策略有效性或正式六折訓練完成證明。正式訓練尚未啟動。

在 Vast 的終端執行：

```bash
cd /root/stockAgent
bash scripts/run_bybit_free_daily_vastai1t.sh --check
bash scripts/run_bybit_free_daily_vastai1t.sh --train
```

`--check` 不建模型、不跑 optimizer；`--train` 再次檢查後呼叫既有 `train.py --resume`。長訓練請在你既有的持續終端工作階段執行。腳本不會停止其他 GPU 工作；偵測到 compute process 時會拒絕啟動。

正式設定是 `configs/remote/bybit_free_daily_multibasis_vastai1t_20260927.yaml`，輸出是 `artifacts/markets/bybit_free_daily_multibasis_pinned_20260927`。此設定只適用已有 v7 的 Vast；本機 producer 的 trainer 較舊，不可拿它覆蓋遠端新版。

| 項目 | 實際驗證的設定 |
|---|---|
| 產品 | Bybit 線性 USDT 永續合約，可多空及跨日持倉；不是台股 T+2 |
| 決策頻率 / lookback | 日頻 / 32 日，不是 15 分鐘策略 |
| 合約 / 模型輸入 | 歷史聯集合約 397 個 / 133 個特徵（包含 availability indicators）；不是每天 397 個都可交易 |
| 多基底 | 18 個候選家族，每族預設 4 components；`input_features` 接入既有模型，實際去重選擇另有 artifact |
| 動作 | 既有 v7 `score_entmax_cash_v2`，模型選擇方向、曝險與現金；不手訂交易訊號 |
| 費用 / 資金費 | 單邊 0.055%，依實際成交 notional 計算；依持倉跨越的真實 funding settlement 計費 |
| 時點 | 00:00 UTC 決策與理想化零延遲執行；這是研究假設，不是可實盤同價成交保證 |
| 訓練 | 2 × RTX 5090、BF16、DDP、global batch 128、trajectory optimizer cadence |
| 正式生命週期 | 6 folds，每 fold 上限 1,000 epochs；保留 validation early stopping、checkpoint、曲線和 resume |

## 第一性原理：資料為什麼分層

對線性合約，權益變化必須同時包含持倉價格損益、成交費、資金費與真實執行限制。概念上：

`Δequity = Σ position_quantity × Δvaluation_price − 0.00055 × Σ traded_notional − Σ signed_position_notional_at_settlement × funding_rate`

交易數量須包含合約乘數；實際成本與估值沿用既有狀態帳本。資金費不是固定每日扣款；正負號及結算時點取決於實際持倉。報價缺失不能補零、用未來價格代替、或假造清算。

因此資料依責任分成：

1. **交易與損益事實**：合約上市／下市、價格、成交容量、funding，是可執行與可估值的必要條件。
2. **當時可知的解釋資料**：跨所價格、已結算 funding、FRED 初次發布、SEC acceptance timestamp，可在發布時鐘與缺失 mask 約束下輸入模型。
3. **研究歷史／前瞻快照**：今天能下載到 2017 年數值，不代表 2017 年已經知道今天修訂的值。先保留原始回應、event time、available time、revision，再決定能否进入歷史訓練。

資料品質技能的稽核流程使「已下載」「已同步」「可因果訓練」「可實盤」分開驗收。training-reuse 流程則保留既有 trainer、DDP、checkpoint 與回測帳本，沒有另建平行訓練框架。

## 本次取得、整理與保留

只呼叫免費來源／既有免費 API 權限，沒有購買方案或執行付費 Dune 查詢。Vast 本身既有運算／儲存費不屬於免費資料承諾。

| 來源 | 本次實測內容 | 歷史及用途 | 儲存／遠端狀態 |
|---|---|---|---|
| Bybit | 固定 release 的原始日資料／funding；遠端既有修復日資料 397 檔、410,449 列 | 線性 USDT 歷史最早 2020-03-26，正式 panel 起點 2020-03-27；完整回報標籤到 2026-09-24 | 固定 release 已完整驗證、掛載、pin；修復 view 通過來源與輸出 hash 檢查 |
| Binance / OKX | 沿用已驗證的跨所特徵，不重抓數億列重複 K 線；現有增量服務未中斷 | 各別原始庫含 2019 年資料；availability、幣種映射與缺失 mask 仍適用 | 訓練所需衍生特徵隨 Bybit release 到遠端；不是整個交易所歷史庫全部完成 |
| FRED / ALFRED | 11 series、34,359 列；55 cached windows、11 fetched windows | 最早 2005-06-27；initial release、保守 next-UTC-day availability | 原始库本機保留；既有因果衍生特徵已在固定 Bybit release |
| SEC / crypto ETF | 48 source results 完成；SEC 40 檔、2,928 filings、重複鍵 0 | 最早 2013-10-04，最新 2026-09-25；用 acceptance timestamp，不把 filings 當成 ETF 每日申贖流量 | 原始庫本機保留；既有 filing 特徵已隨 Bybit release |
| Coin Metrics Community | 3,260 assets、32 distinct metrics、最新視圖 2,969,377 列；本次新增 2,562 列，保留抓取 vintages | BTC 最早 2009-01-03；本機可證明的觀測 vintage 自 2026-08-16 起 | 本機研究用；依現有授權／`publish:false` 不直接同步原始庫 |
| CFTC 數位資產 TFF | 更新為 926 列，重複鍵 0 | 2017-12-19 至 2026-09-22；歷史發布日尚待逐筆驗證 | 本機研究用；舊 raw、normalized、receipt 已按內容 hash 保存版本 |
| Wikimedia | 8 篇登錄文章、27,206 article-days，重複鍵 0 | 2015-07-01 至 2026-09-25；修訂／改名時鐘尚待驗證 | 本機研究用，未假造舊 available time |
| 免費公共情境 | 本次刷新既有 16 個 active datasets，另新增以下 2 個歷史來源 | FNG、DEX、fees/revenue、options/OI、BTC mempool、ETH gas 等；按個別來源分類 | `free-public-context` 新 release 已在遠端完整驗證、掛載；18 個 active，本次沒有重新刷新 10 個 inactive 歷史登錄 |
| DefiLlama TVL 歷史（新增） | 3,287 列 | 2017-09-27 至 2026-09-26；保留當次 available time | 遠端研究資料，不倒灌到 2020 模型輸入 |
| DefiLlama stablecoin 歷史（新增） | 40,729 列，依 metric／peg denomination 分開 | 2017-11-29 至 2026-09-26；原生掛鉤量與 USD value 不混加 | 遠端研究資料；不是 40,729 個獨立交易日 |
| Dune 既有資料 | 34 檔、2,747,699 列保留 | 2018-11-02 至 2026-08-31；新取得仍 blocked | 本機研究用；修正了「最後一次 blocked=0 列」誤報整個已存庫為空的問題 |

免費公共情境整表為 26,297,831 列，包含多次抓取 vintage，不是等量獨立訓練樣本。來源最早日期只是邊界觀測，不代表全期間／所有幣種無缺口，也不代表非零有效值已逐筆驗收。

**未宣稱「全網所有免費資料下載完成」。** 尚未新增 GDELT／全量新聞、全量 GitHub 開發活動、token unlock 歷史、錢包標籤／交易所淨流入、橋接與安全事件的完整資料管線。Tick／L2／逐筆清算也不在本次日頻＋既有 1m 收集範圍。這些必須先定義分區、授權、可知時鐘與容量，不能把不存在、需付費或事後才知道的欄位補成已完成。

## 固定版本與同步證據

| 資料 | 固定 release | manifest SHA-256 |
|---|---|---|
| Bybit | `bybit-20260926T051004983477667Z-l0-penguin-857d2f9a99f1b88f` | `7af635eb057024bd9e26ed0b0c621c0c124c89c93af7469d1c48e090ab272df6` |
| free-public-context | `free-public-context-20260926T174248126073150Z-l0-penguin-990e503dd3fdc919` | `47c975993423c3448ef5b4c54b9f20223c149590db03ba539ea613adb902a595` |

Bybit 已驗證 629 payload objects，23,958,062,896 bytes；free-public-context 已驗證 69 payload objects，876,509,241 bytes（1,130 個邏輯來源檔、2,028,352,027 bytes）。遠端 `data_bybit`、`data_free_public` 都指向各自 exact release 的不可變 materialization，`READY` 驗證通過。

Syncthing 本次驗收：penguin peer connected、folder idle、completion 100%、remoteState valid；need bytes/items/deletes、pull/system/folder errors 全為 0、watchError 空。這些是本次觀測，不代表未來狀態永久不變。

free-public-context 首次 publication 的後續 scan timeout 曾失敗；未重新偽造 head，沿用 canonical scan retry 後 pending 1 → 0，再做遠端完整 materialization 驗證。收據為 `artifacts/data_quality/crypto_free_preparation_2026-09-27/free_public_scan_retry.json`；遠端 edge 收據為 `/var/lib/stockagent-packed-edge/receipts/packed-edge-1790444925658588073.json`。

Vast 仍是 index-only edge，沒有更動身份或 publication authority。Bybit 另以 `bybit-free-training-20260927.pin.json` 保留熱資料；free-public-context 用管理式七日 lease，並未假裝永遠保留。原始研究 sidecars 的 `publish:false` 邊界未繞過。

遠端 midnight 修復清單 hash 是 `0fef32c982571f309b03499b8e1ca8dd48ad541c6e17e877ce862dd0f917e639`；launch script 會檢查它及基礎 release。資料來源／修復版本變更後須重新驗收，不會靜默 resume 到不同資料。

## 實測與剩餘風險

- 本機最後兩組不重疊回歸測試：91 passed + 20 passed；shell syntax 和本次修改的 diff whitespace 檢查通過。未跑全 repository tests。
- 遠端相關回歸：119 passed、2 skipped；skipped 不當作通過。
- v7 smoke：DDP world size 2，3 epochs 完成；compile graphs 為 7、0、0，epoch wall 約 29.685、0.376、0.351 秒。另有啟動／預熱／產圖／最終回放，不能用穩態 epoch 時間冒充完整六折總工時。
- canonical `validate_completed_training_artifacts` 驗證 18 個必要 artifacts，missing 與 invalid 都為空；fold 1 full-horizon 2022-01-01 至 2026-09-24，共 1,728 日；stitched deployment 為 2022 年 365 日。
- 三個 epoch 的 test 累積報酬 18.84%、benchmark 34.24%、差額 −15.40 個百分點、Sharpe 0.285、最大回撤 −20.45%。這是一次短整合測試，不是挑選模型／調參依據，也沒有證明獲利優勢。
- 原先 v4 smoke 在 HFTUSDT 2026-08-20 持倉估值缺失時按契約中止。保留 `artifacts/smoke/bybit_free_remote_20260927` 失敗證據，改驗證遠端**本已存在**的 v7，沒有降級帳本或捏造價格。
- 既有 v7 修復使用 HFT／VINE／ICX 可知的下市公告、受成交容量約束的退出請求，以及經來源驗證的 funding 修復。這不等於找回下市前缺失的全部分鐘行情；若其他模型留下無法估值的剩餘持倉，仍應 fail closed。
- 00:00 零延遲只是繼承的研究設定；正式實盤須另測資料發布、推論延遲、滑價和可成交性。最後 fold 的 validation/test 年份重合，不能把它當獨立 OOS。
- 容器未掛持久 volume；不能把 Vast 當災難復原備份。毀棄／回收 instance 前，訓練完成 artifacts 仍須走既有完成驗證與 canonical ingress／cold publication 流程；本次沒有發布尚未完成的正式模型。

## 稽核入口

- [全來源取得清單](../artifacts/data_quality/data_acquisition_inventory_2026-09-27.csv)
- [歷史覆蓋與 PIT 分級](../artifacts/data_quality/crypto_free_preparation_2026-09-27/coverage_matrix.csv)
- [來源品質報告](../artifacts/data_quality/crypto_free_preparation_2026-09-27/report.md)
- [機器可讀交付摘要](../artifacts/data_quality/crypto_free_preparation_2026-09-27/readiness.json)
- [遠端短訓練驗收副本](../artifacts/data_quality/crypto_free_preparation_2026-09-27/remote_smoke_evidence/fold_01/fold_complete.json)
- 批次取得收據：`artifacts/daily_downloader/crypto_free_prepare_20260927/step_receipts/crypto-free-prepare-20260927/`。

## 官方來源依據

DefiLlama 官方列出免費 TVL 與 stablecoin history endpoints，並區別需付費的其他 API：[API 說明](https://github.com/DefiLlama/api-docs/blob/main/llms-pro.txt)。FRED 的 `output_type=4` 為 initial-release observations：[官方參數](https://fred.stlouisfed.org/docs/api/fred/series_observations.html)。CFTC 與 Wikimedia 分別提供持倉及 pageview 公開介面：[CFTC](https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm)、[Wikimedia](https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/reference/page-views.html)。可下載性不取代本報告要求的歷史可知時鐘、缺口及授權稽核。
