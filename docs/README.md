# stockAgent 文件索引

從根目錄的 [`README.md`](../README.md) 開始。它包含安裝、資料冷庫、自助解封、
Syncthing 驗收、下載、訓練與服務操作的日常指令；本頁負責把深入文件依用途分類。

閱讀優先序：

1. 執行工作前先看現行正確性契約。
2. 部署或維運使用 operational runbook。
3. 修改某個市場／策略時再讀對應設計文件。
4. 歷史 review 只作 provenance，不覆蓋現行程式、config 或契約。

## 現行正確性契約

- [`project_architecture.md`](project_architecture.md)：全專案模組／服務責任導航與可重建的 AI 可讀清單。
- [`architecture_modernization_2026-10-03.md`](architecture_modernization_2026-10-03.md)：第一性原理分析、開發清單與真實工作流效能比較。
- [`architecture_readiness_repair_2026-10-03.md`](architecture_readiness_repair_2026-10-03.md)：續作的破產收益、來源版本一致性、服務修復及冷封存恢復驗收。
- [`architecture_technology_trials_2026-10-03.md`](architecture_technology_trials_2026-10-03.md)：Parquet／Arrow／Polars／DuckDB、PostgreSQL、DuckLake、Iceberg、Temporal、TypeScript、systemd 的真實引擎試驗與 Miniforge／Mamba 工作流程。
- [`architecture_unresolved_repair_2026-10-03.md`](architecture_unresolved_repair_2026-10-03.md)：混合分鐘來源 schema 的正式修復、同一快照的離機 PG 還原與當下遠端 Mamba／還原並行度實測。
- [`target_architecture_execution_2026-10-03.md`](target_architecture_execution_2026-10-03.md)：目標的五階段實作、真實跨節點控制／還原、遠端重建與相容性驗收。
- [`remote_build_workflow.md`](remote_build_workflow.md)：依遠端機器條件實測完整工作流、重測候選並套用版本綁定的最快設定。
- [`remote_build_measurement_2026-10-03.md`](remote_build_measurement_2026-10-03.md)：遠端 CPU quota／NUMA／RAM 清冊與完整來源建表的實測紀錄。
- [`packaging_and_runtime_releases.md`](packaging_and_runtime_releases.md)：可安裝套件、角色依賴、code/runtime receipts、GPU admission 與 API 型別邊界。
- [`../AGENTS.md`](../AGENTS.md)：point-in-time、fee/mask、checkpoint、重現性與目前量測建議。
- [`training_spec.md`](training_spec.md)：第一性訓練、評估、checkpoint 與驗收契約。
- [`training_mode_adapter_architecture.md`](training_mode_adapter_architecture.md)：共用訓練核心與模式 adapter 邊界。
- [`tw_execution_modes.md`](tw_execution_modes.md)：台灣日／現金／隔夜 execution mode 語意。
- [`windowed_tensor_pipeline.md`](windowed_tensor_pipeline.md)：lazy window 與 panel-slab 表示。
- [`temporal_multi_basis.md`](temporal_multi_basis.md)：online-safe temporal multi-basis 表示。

## 資料與多機維運 Runbook

- [`lab203_complete_backup_rollout_2026-10-04.md`](lab203_complete_backup_rollout_2026-10-04.md)：全量冷庫盤點、明確放棄已缺歷史、硬體實測與 NAS ACK 後有界傳輸回收。
- [`lab203_automatic_backup_2026-10-04.md`](lab203_automatic_backup_2026-10-04.md)：持續自動同步／NAS 檔案驗收；固定程式一次安裝後，自動派送 packed 與新 PostgreSQL 邏輯狀態的還原任務、重試及回傳。
- [`continuous_nas_backup_2026-10-04.md`](continuous_nas_backup_2026-10-04.md)：來源端持續增量服務、lab203 既有 worker 回傳收據交接、固定 NAS snapshot 重建與受控 USB 金鑰保管。
- [`lab203_nas_recovery_acceptance_2026-10-04.md`](lab203_nas_recovery_acceptance_2026-10-04.md)：lab203 本機執行三個固定 NAS snapshot 的 packed／SQL 獨立驗收；USB 保管已由使用者確認完成。
- [`agent_workflow.md`](agent_workflow.md)：本機 agent 工具安裝、唯讀狀態、任務紀錄與 tmux／systemd 命令監督。
- [`control_plane_workflow.md`](control_plane_workflow.md)：opt-in PostgreSQL 工程工作、版本／lease／attempt、SSH 私有節點與備份還原。
- [`penguin_source_only_storage_2026-10-01.md`](penguin_source_only_storage_2026-10-01.md)：
  現行源本／服務機分工、遠端按需訓練生成、公開資訊與來源清單、安全清理收據及保留阻礙。
- [`markets_storage_review_2026-10-03.md`](markets_storage_review_2026-10-03.md)：
  markets 容量排名、歷史規則版本、服務保留範圍及逐檔冷覆蓋差異；不是刪除許可。
- [`markets_safe_retirement_2026-10-03.md`](markets_safe_retirement_2026-10-03.md)：
  四個研究目錄的完整 D 冷封存／立即回收、恢復指令及 117 個期貨解析版本的保留阻礙。
- [`packed_dataset_storage.md`](packed_dataset_storage.md)：現行 packed 冷庫、增量 pack/blob、
  多寫者 head、materialize、lease 與非持久 Vast 的 index-only edge cache；
  penguin 以現行儲存契約的 D 槽單份主冷庫為準，舊 C retention 不再啟用。
- [`packed_cold_backup.md`](packed_cold_backup.md)：已退役 C→D 獨立備份的歷史救援說明，
  不是現行部署指令；D 單份主冷庫見 [`d_cold_store_migration_2026-09-25.md`](d_cold_store_migration_2026-09-25.md)。
- [`live_artifact_sync.md`](live_artifact_sync.md)：退役的 artifacts hot transport 與現行
  完成產物 cold release、衝突政策、hard-link 去重；不得重建舊 folder。
- [`storage_pressure_maintenance.md`](storage_pressure_maintenance.md)：磁碟高水位下只回收
  allowlisted 可重建編譯快取、訓練程序保護與 receipt 稽核。
- [`desync_multiwriter_sync.md`](desync_multiwriter_sync.md)：舊 desync snapshot 的遷移／救援流程；
  不再是新部署的日常入口。
- [`RUN_GUIDE.md`](RUN_GUIDE.md)：補充 operator 指令；使用前仍要核對當前 config 與本機路徑。
- [`data_api_credentials.md`](data_api_credentials.md)：資料 API credential 邊界。
- [`public_dashboards_architecture.md`](public_dashboards_architecture.md)：公開面板的唯讀邊界、資料責任、快取、前端更新、安全與部署驗收契約。
- [`acquisition_pages_hierarchy_2026-10-03.md`](acquisition_pages_hierarchy_2026-10-03.md)：抓取頁摘要優先、來源機制差異、原生折疊與共用呈現層。
- [`acquisition_pages_engineering_2026-10-03.md`](acquisition_pages_engineering_2026-10-03.md)：抓取網頁全鏈架構、先行方案、故障／競態／按需工作量、安全與正式站驗收。

## 資料取得、修復與儲存

- [`tej_smart_wizard_acquisition_2026-10-02.md`](tej_smart_wizard_acquisition_2026-10-02.md)：TEJ 全 30 類／255 表／45,826 欄清冊、v4 可操作來源範圍驗證／下載器、桌面與 API 限額邊界、唯讀進度網站及未完成驗證。
- [`tej_smart_wizard_export_runbook_2026-10-01.md`](tej_smart_wizard_export_runbook_2026-10-01.md)：維運 runbook，記錄 Smart Wizard 查詢復原、日期／欄位讀回、Excel 唯讀擷取、單位轉換與有限樣本驗收。
- [`tej_smart_wizard_inventory_2026-10-01.md`](tej_smart_wizard_inventory_2026-10-01.md)：TEJ 已讀資料表逐欄清冊、未讀目錄、兩階段取得與後續跨來源校驗候選；尚未完成全帳號歷史權限查驗。
- [`finlab_stage_eta_scheduling_2026-10-01.md`](finlab_stage_eta_scheduling_2026-10-01.md)：FinLab 分階段 ETA、SDK 增量查核、防飢餓與動態剩餘配額排程，含公開畫面及計算驗收。
- [`finmind_tick_efficiency_2026-10-03.md`](finmind_tick_efficiency_2026-10-03.md)：FinMind 106 項清冊、剩餘分鐘／分點／tick、可逆日曆排除、價差 tick 整日形狀與分階段估時；工程快照，不是全歷史完整性保證。
- [`finmind_progress_calendar_repair_2026-10-03.md`](finmind_progress_calendar_repair_2026-10-03.md)：FinMind 進度與重試條件估時、台股官方休市尾端、缺漏優先／共享配額快照、計算與正式網頁驗收。
- [`finmind_resilience_throughput_2026-10-04.md`](finmind_resilience_throughput_2026-10-04.md)：FinMind 同源故障探測、排程成本、SQLite 連線釋放、共享配額及實際短窗吞吐／新資料／原歷史保留驗收。
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
- [`tw_stock_futures_day_trade_minute.md`](tw_stock_futures_day_trade_minute.md)：08:45 決策、08:46 分鐘執行、13:30 退出的個股期貨當沖。
- [`tw_index_derivatives_day_multi_basis.md`](tw_index_derivatives_day_multi_basis.md)
- [`tw_index_derivatives_tick_strategy.md`](tw_index_derivatives_tick_strategy.md)

### 跨資產

- [`cross_asset_standalone.md`](cross_asset_standalone.md)

## Review 與歷史工程快照

- [`prompt_audit_2026-10-03.md`](prompt_audit_2026-10-03.md) 是提示規則與個人偏好的增量稽核，包含來源、適用範圍與本輪驗證。
- [`user_working_preferences_2026-09-30.md`](user_working_preferences_2026-09-30.md) 是個人偏好技能的初次建立紀錄；後續修正見上方稽核。
- [`PROJECT_REVIEW_2026-09-05.md`](PROJECT_REVIEW_2026-09-05.md) 是該日全專案 review 的歷史快照，包含當時修正、驗證與保留的決策邊界；後續架構進度見本頁現行架構文件。
- [`PROJECT_REVIEW_2026-08-10.md`](PROJECT_REVIEW_2026-08-10.md) 保留為當時的 review 快照。
- `ARCHITECTURE_REVIEW.md`、`COMPREHENSIVE_ANALYSIS.md`、`FIXES_*`、
  `OPTIMIZATION_*`、`EXECUTIVE_SUMMARY.md`、`ANALYSIS_INDEX.md` 與
  `CODE_ORGANIZATION.md` 是特定日期的工程快照。
- `QUICK_START_GUIDE.md` 與舊 review 中的命令可能早於目前 runtime、config 或 storage
  架構；執行前以根 [`README.md`](../README.md)、`--help` 與現行 YAML 為準。

新增永久文件時，必須在本頁標明它屬於現行契約、維運 runbook、研究／策略文件，或
歷史快照，避免舊量測被誤當成不可變規格。
