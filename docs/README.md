# stockAgent 文件索引

從根目錄的 [`README.md`](../README.md) 開始。它包含安裝、資料冷庫、自助解封、
Syncthing 驗收、下載、訓練與服務操作的日常指令；本頁負責把深入文件依用途分類。

閱讀優先序：

1. 執行工作前先看現行正確性契約。
2. 部署或維運使用 operational runbook。
3. 修改某個市場／策略時再讀對應設計文件。
4. 歷史 review 只作 provenance，不覆蓋現行程式、config 或契約。

## 現行正確性契約

- [`../AGENTS.md`](../AGENTS.md)：point-in-time、fee/mask、checkpoint、重現性與目前量測建議。
- [`training_spec.md`](training_spec.md)：第一性訓練、評估、checkpoint 與驗收契約。
- [`training_mode_adapter_architecture.md`](training_mode_adapter_architecture.md)：共用訓練核心與模式 adapter 邊界。
- [`tw_execution_modes.md`](tw_execution_modes.md)：台灣日／現金／隔夜 execution mode 語意。
- [`windowed_tensor_pipeline.md`](windowed_tensor_pipeline.md)：lazy window 與 panel-slab 表示。
- [`temporal_multi_basis.md`](temporal_multi_basis.md)：online-safe temporal multi-basis 表示。

## 資料與多機維運 Runbook

- [`packed_dataset_storage.md`](packed_dataset_storage.md)：現行 packed 冷庫、增量 pack/blob、
  多寫者 head、materialize、lease 與 GC。
- [`live_artifact_sync.md`](live_artifact_sync.md)：可變 artifacts hot transport、完成產物 cold
  release、衝突政策與 hard-link 去重。
- [`desync_multiwriter_sync.md`](desync_multiwriter_sync.md)：舊 desync snapshot 的遷移／救援流程；
  不再是新部署的日常入口。
- [`RUN_GUIDE.md`](RUN_GUIDE.md)：補充 operator 指令；使用前仍要核對當前 config 與本機路徑。
- [`data_api_credentials.md`](data_api_credentials.md)：資料 API credential 邊界。
- [`public_dashboards_architecture.md`](public_dashboards_architecture.md)：公開面板的唯讀邊界、資料責任、快取、前端更新、安全與部署驗收契約。

## 資料取得、修復與儲存

- [`tw_public_download_resume_and_rate_limits.md`](tw_public_download_resume_and_rate_limits.md)：
  台灣公開資料 rebuild、repair、daily、續傳與 rate limit。
- [`openbb_archive_downloader.md`](openbb_archive_downloader.md)：OpenBB archive ingestion、resume 與 compaction。
- [`okx_historical_features.md`](okx_historical_features.md)：OKX 歷史資料與 feature 契約。
- [`dune_sec_crypto_etf_history.md`](dune_sec_crypto_etf_history.md)：Dune、SEC 與 crypto ETF 歷史來源。
- [`columnar_storage_architecture.md`](columnar_storage_architecture.md)：columnar storage 設計。
- [`shioaji_hft_dataset.md`](shioaji_hft_dataset.md)：Shioaji Tick/BidAsk capture 與 HFT dataset。

## 訓練、策略與解釋

### 台股與日內

- [`tw_day_trade_daily_no_default.md`](tw_day_trade_daily_no_default.md)
- [`tw_day_trade_realistic_execution.md`](tw_day_trade_realistic_execution.md)
- [`tw_minute_kbar_research.md`](tw_minute_kbar_research.md)
- [`tw_minute_cash_asset_contract.md`](tw_minute_cash_asset_contract.md)
- [`tw_public_explainability_guide.md`](tw_public_explainability_guide.md)

### 期貨與選擇權

- [`tw_index_futures_day_strategy.md`](tw_index_futures_day_strategy.md)
- [`tw_futures_portfolio_day.md`](tw_futures_portfolio_day.md)
- [`tw_index_derivatives_day_multi_basis.md`](tw_index_derivatives_day_multi_basis.md)
- [`tw_index_derivatives_tick_strategy.md`](tw_index_derivatives_tick_strategy.md)

### 跨資產

- [`cross_asset_standalone.md`](cross_asset_standalone.md)

## Review 與歷史工程快照

- [`PROJECT_REVIEW_2026-08-10.md`](PROJECT_REVIEW_2026-08-10.md) 是最近一次全專案 review。
- `ARCHITECTURE_REVIEW.md`、`COMPREHENSIVE_ANALYSIS.md`、`FIXES_*`、
  `OPTIMIZATION_*`、`EXECUTIVE_SUMMARY.md`、`ANALYSIS_INDEX.md` 與
  `CODE_ORGANIZATION.md` 是特定日期的工程快照。
- `QUICK_START_GUIDE.md` 與舊 review 中的命令可能早於目前 runtime、config 或 storage
  架構；執行前以根 [`README.md`](../README.md)、`--help` 與現行 YAML 為準。

新增永久文件時，必須在本頁標明它屬於現行契約、維運 runbook、研究／策略文件，或
歷史快照，避免舊量測被誤當成不可變規格。
