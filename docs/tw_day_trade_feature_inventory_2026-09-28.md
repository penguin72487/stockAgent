# 台股當沖訓練特徵清點與資料集 — 2026-09-28

## 1. 執行結果與適用邊界

- 清冊快照：2026-09-28T09:34:40.102657Z；補充讀取到 2026-09-28T09:45:08.918238+00:00。下載器持續運作，這是有時間標記的盤點，不是所有來源同時凍結的快照。
- 已清點 **1,869 個註冊來源項目**，以及監控的 **88,691 個實體 dataset×field**；後者包含股票代碼欄、主鍵、文字與不同版本，不是同等數量的模型特徵。
- 補查 **1,107 個 FinLab key、212,289 個 FinMind 當前分區憑證、11,261 個 BEA/Census/MOI 當前 normalized 檔**。舊 content-addressed 版本沒有重複加總。
- 核心：27 個數值 + 27 個缺值標記；2014-01-02～2026-09-24，3,107 個交易日、2,757 檔股票／ETF 的歷史 universe、5,809,578 個 alive date×symbol 列。包含已下市標的，並非今天可交易股票數。
- 日期截至 9/24 並非漏掉 9/28：本地官方休市表列明 9/25 中秋節、9/28 教師節休市，中間是週末。
- 研究：308 個外部欄位（204 TW public/TAIFEX/MOPS + 104 FinLab），共 9,626,904 個稀疏 date×symbol 列；既有研究 selector 加 20 股價衍生欄、308 availability 後是 **636 通道**。這是配置展開數，不是本次 GPU 訓練／完整研究 tensor 壓測結果。
- 嚴格核心驗收：model_safe=True，finding_counts={'critical': 0, 'high': 0, 'medium': 1, 'low': 0}。此處 model_safe 僅限已選特徵、日資料和稽核契約。
- **特徵資料就緒，不等於整個當沖成交訓練已驗收。** `core_feature_audit.yaml` / `research_feature_audit.yaml` 僅為特徵建置與檢查配置；其繼承的 naive close-proxy labels 不是 09:01 成交。不可把這兩個配置的回測當成當沖績效。正式當沖模式仍須接既有 phase-aware/分鐘執行契約並獨立檢查，不可再把已對齊 09:00 的資料多移一天。
- 沒有啟動訓練、修改服務、切換模型、送單、傳送 FinLab 授權原始資料或更動冷庫。

## 2. 可以用、研究限定、不能直接用

| 類別 | 本次判定 | 原因／處理 |
|---|---|---|
| 官方 OHLCV、成交值／筆數、估值、融資券、法人、TAIEX、CBC／DGBAS 原始發布值 | 27 個核心數值通過盤前特徵門檻 | 11 個當日完整資訊欄往下一交易日；10 個籌碼欄已由來源建置器延後；6 個總經欄按原始發布版本 |
| 308 欄寬研究表 | 可做明示近似的研究，不能當成嚴格歷史實盤資料 | 保留現修值、理論財報公布日、晚近快照的限制；禁止反向補到歷史 |
| FinLab 其他已下載 key、FinMind 其他欄位 | 有資料但不自動批准接入模型 | 尚缺 entity/unit/available_at/vintage 的逐來源轉換與驗證；股價重複來源先對帳，不能盲接或重複算成獨立資訊 |
| BEA／Census／MOI | 原始研究候選 | receipt 的 historical_point_in_time=false；觀測期不等於公布時點，數字字串及單位需按資料集解析 |
| NOAA／CWA／MOENV／Finnhub 等 catalog 或近期快照 | 不能直接充當歷史特徵 | 目錄是可下載項目的清單；近期天氣／空品觀測與股票清單不是多年歷史 |
| 海外股／外匯／加密／FRED／CFTC／OpenBB | 清冊已納入；尚未接入台股 feature ABI | 需驗證時區、發布延遲、native grain、實際 vintage、單位和交易日對齊 |
| source/date/symbol/id/path/hash 等 | 不是數值 alpha | 作主鍵或來源證據，不當成連續模型訊號 |

以上不是說只有 27 個特徵永遠合格：其他同源衍生式可能可用，但本次只對列出的核心選集完成端到端盤前驗收。

## 3. 具體缺口與不合格項目

**2026-09-28 後續更正：** 下列為初次盤點當時的狀態。後續已修復 5 個 FinMind 空檔，且 190 個 OpenBB 舊表空欄中有 186 個在較新 L1 已有資料。不要把下列 213 當作全 provider 仍缺資料。最新判定與下載器修復見 [修復紀錄](downloader_null_integrity_repair_2026-09-28.md)，修正清冊為 `artifacts/data_quality/downloader_integrity_20260928/feature_candidates_corrected.csv`；原始證據保留。

- 研究表有 **70 欄直到 2026 年才有第一筆值**。TDCC 官方快照、近期營收／財務／公司基本資料、若干舊期交所摘要等，不能拿來宣稱 2014 年起同樣完整。逐欄逐年缺口見 `research_missing_feature_years.csv`。
- 年度實測：2014 年有 177/308 欄至少一筆值；2026 年截至資料截止日有 287/308 欄至少一筆值，另 21 欄整年尚無值。『至少一筆』不是全市場每日完整。
- 初次盤點所選檔案有 **213 個 dataset×field 為 NULL**、140 個欄位的來源驗證不完整；這不代表全 provider 的現行資料皆空，也不是 core 的 27 個數值欄。OpenBB 舊 compact 的判讀與 FinMind 修復後狀態已由上述後續更正更新。
- 主監控 schema 證據仍為 57,653/57,812 個檔，不完整項不冒充全量驗證。catalog 補充檢查也只驗 footer/receipt；只有採入 core/research 的來源另有相應 bytes/hash 驗證。
- FinMind 佇列：pending=60,292、inflight=4、failed=1、invalid_request=13、calendar_wait=70。observed_empty=83,670 只表示查詢回應空，不能一概當成『缺資料』或『歷史完整』。
- FinMind 失敗分區已再次只讀檢查 18,217 個分區，仍有 5 個檔失敗；其中 5 檔為 0 bytes。原始完成 receipt 與空檔不一致，不能只信 queue 的 complete。未重寫正在運作的下載器或原檔。

| 來源 | 標的／分區 | 問題 |
|---|---|---|
| UKStockPrice | 0NW1.L / history | 0-byte Parquet；receipt rows=4519 |
| UKStockPrice | 0NW2.L / history | 0-byte Parquet；receipt rows=4248 |
| UKStockPrice | 0NW4.L / history | 0-byte Parquet；receipt rows=5138 |
| UKStockPrice | 0NW7.L / history | 0-byte Parquet；receipt rows=4535 |
| TaiwanStockPriceLimit | 全市場 / 2023-08-23 | 0-byte Parquet；receipt rows=2444 |

- 台股分鐘來源監控：2630/2757 標的；底層表 313047232 列，2020-03-02T09:01:00～2026-09-24T13:30:00。標的數覆蓋不代表每個股票日都有 270 個可執行分鐘；本次沒有重新驗證所有分鐘成交容量或用日 K 假冒分鐘。
- FinLab 分鐘／tick 目前仍是有限分區，不可替代全市場歷史；兩個 FinLab key 在清冊標為 degraded（`dividend_otc:權息`、`management_change_events:變更交易開始日`），未採入核心。
- MOPS 原始欄位清冊有 `1911-00-07` 等非法日期界線；它是原始 mixed-format 欄位統計，不是可直接使用的公布日期。XBRL 在研究表走既有解析與理論日期標記，不以這個 min/max 認證 PIT。
- `twse_taiex_ohlc` 沒有相符的獨立註冊來源 ID（可能由集合項目代表）；本次已直接驗收正本與交易日，不把集合狀態當其獨立證據。詳見 `core_source_registry_gaps.csv`，這不是資料檔缺失。
- 核心仍保留一項 medium 限制：並非所有下市事件都有對應的歷史強制回補公告；沒有自行猜補公告時間。

## 4. 缺值、時間與年度切分

- `preopen_core_features.parquet` 只含 date、symbol、27 數值與27布林 available；沒有未來報酬、下一日收盤或成交 labels。原生未知仍為 NULL，真正觀測的 0 仍是 0。
- canonical panel 內未知值的 0 只是張量容器；必須一起使用 availability，不能單看填零後的 finite rate 宣稱 100% 資料覆蓋。
- 研究表市場總體欄在 `__MARKET__` 列，個股欄在各股列，不能用全表 stock_rows 當總經覆蓋率分母；月／季資料的 event 稀疏不代表應補成每日新觀測。
- 沿用年度 walk-forward，2014～2026 無缺年；2026 是截至 9/24 的部分年度。標準化／特徵篩選只能在該 fold 的 train years fit，不得用驗證或測試報酬挑 feature。
- 只做已登記的狀態 carry；事件不可盲目 forward-fill，也沒有插值、把今日快照倒灌歷史或臆造缺價。

## 5. 已修正與測試

- 使用既有官方股票建置器及 TW public 建置器，在獨立目錄重建股票、143 欄原表與憑證。初始 2 個 critical stale receipt 已由重新稽核驗收；重建前後特徵 parquet SHA 相同，代表這次主要是來源版本憑證落後，而非擅改數值。
- 原特徵盤點工具只掃 `twpub_*`，會漏掉 `twfl_*`。已補上 FinLab namespace，並測試缺欄、duplicate grain、NULL 與有效零的區別。
- masked core 的27個數值逐塊比對已驗收 panel，完全相同；新增 availability 不改變既有數值。
- 測試結果見 `test_results.txt`；原始／修復後完整 audit 各自保留。`verification.ipynb` 供重跑核心驗證。若本機沒有 ipykernel，產生器用同一個 runtime Python 依序執行自建 cell 並保存輸出，不宣稱通過 Jupyter kernel 驗收；實際方式在 notebook metadata。未另外做 Notebook viewer 視覺驗收，可在 IDE 的 Jupyter 檢視器開啟核對排版。

## 6. 產物索引

資料集目錄：`artifacts/datasets/tw_day_trade_features_20260928_v1`。清冊目錄：`artifacts/data_quality/tw_day_trade_feature_catalog_20260928`。

| 檔案 | 用途 |
|---|---|
| dataset_manifest.json | 狀態、範圍、SHA、執行限制 |
| preopen_core_features.parquet | 54 通道的明確缺值、09:00 對齊 feature-only 表 |
| features/tw_public_stock_daily.parquet + stocks/ | 重建後的 canonical 輸入 |
| research_features.parquet + .finlab_research.json | 308 外部欄位研究表與映射／來源憑證 |
| source_inventory.csv / source_training_admission.csv | 全部 1,869 註冊來源、下載與訓練資格分開 |
| feature_candidates.csv / physical_fields.csv | 實體欄位及來源候選清單，不是模型維度 |
| model_feature_admission.csv / core_feature_units.csv | 每個已建模型特徵的資格、日期、單位 |
| research_feature_profile/annual_feature_inventory.csv | 每年、每個 feature 的實際 non-null 數 |
| research_missing_feature_years.csv | 2014 年起整年無觀測值的研究欄位 |
| finmind_partition_states.csv / source_failure_recheck.csv | 下載欠帳、損壞檔與可追查 receipt 路徑 |

## 7. 來源與證據

- 本文數量來自本地已留存的 monitor 快照、SQLite queue 唯讀快照、當前 receipt、Parquet footer／全表 profile、canonical panel audit；來源 first/last 與實際可知時間分開。
- FinLab 官方亦區分實際公告日與統一截止日；因此不能把季度代碼直接當可交易日期。[FinLab 官方 FAQ](https://finlab.finance/docs/faq/)
- FinMind 官方技術面資料文件提供各端點欄位與粒度；同名成交量仍須按端點單位契約對帳。[FinMind 官方文件](https://finmind.github.io/tutor/TaiwanMarket/Technical/)

## 附錄：本次核心數值欄位

| feature | 單位 | 未知 cells | 實際為零 cells |
|---|---|---:|---:|
| open_raw | TWD/share | 47769 | 0 |
| high_raw | TWD/share | 47769 | 0 |
| low_raw | TWD/share | 47769 | 0 |
| close_raw | TWD/share | 47769 | 0 |
| trading_volume_raw | shares | 47769 | 0 |
| twpub_official_trading_value_raw | TWD | 3205 | 40028 |
| twpub_official_trades_raw | trades | 3205 | 40028 |
| twpub_pe_raw | ratio | 1755204 | 1 |
| twpub_pb_raw | ratio | 557894 | 1 |
| twpub_dividend_yield_pct_raw | percent | 557034 | 1289912 |
| twpub_margin_balance_lots_raw | official_lots | 417102 | 488150 |
| twpub_short_balance_lots_raw | official_lots | 417102 | 2101242 |
| twpub_margin_buy_lots_raw | official_lots | 417102 | 1304606 |
| twpub_margin_sell_lots_raw | official_lots | 417102 | 1246192 |
| twpub_short_sell_lots_raw | official_lots | 417102 | 3547804 |
| twpub_short_buy_lots_raw | official_lots | 417102 | 3500932 |
| twpub_foreign_net_buy_shares_raw | shares | 774075 | 391522 |
| twpub_investment_trust_net_buy_shares_raw | shares | 774075 | 4221268 |
| twpub_dealer_net_buy_shares_raw | shares | 774075 | 1943276 |
| twpub_institutional_net_buy_shares_raw | shares | 780306 | 144178 |
| twpub_twse_taiex_raw | index_points | 1505 | 0 |
| twpub_cbc_fx_reserves_usd_billion_raw | USD_billion | 0 | 0 |
| twpub_cbc_m1b_yoy_pct_raw | percent | 0 | 0 |
| twpub_cbc_m2_yoy_pct_raw | percent | 0 | 0 |
| twpub_dgbas_cpi_yoy_pct_raw | percent | 0 | 0 |
| twpub_dgbas_unemployment_pct_raw | percent | 0 | 0 |
| twpub_dgbas_gdp_yoy_pct_raw | percent | 0 | 0 |

## 重跑檢查

以下只重跑清冊／特徵驗證，不啟動模型訓練；保留現有產物時請使用新的 dated output 目錄。核心資料更新必須先按本次使用的 canonical `build_tw_official_symbol_parquets.py`、`build_tw_public_training_features.py` 重建，再通過 `audit_tw_public_data_layer.py --strict --require-live-selected-features`，不可只手改 receipt。

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python scripts/prepare_tw_day_trade_feature_catalog.py \
  --output-dir artifacts/data_quality/tw_day_trade_feature_catalog_NEW --workers 4
run_fintech_python -m pytest -q \
  test/test_tw_public_data_layer_tools.py \
  test/test_tw_public_training_features.py \
  test/test_tw_public_research_all_features.py \
  test/test_finlab_research_overlay.py \
  test/test_tw_day_trade_feature_catalog.py
```
