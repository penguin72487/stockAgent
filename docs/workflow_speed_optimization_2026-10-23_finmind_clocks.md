# FinMind 計時覆蓋與時鐘邊界，2026-10-23

本輪延續完整工作流測速任務，處理 FinMind「已有時間欄位卻未正確投影」
以及「成功請求缺乏單調時鐘」兩個缺口。這是計時覆蓋與精度的改善，沒有
固定工作量加速比較，不能宣稱下載、訓練或完整 collector 已變快。

前輪邊界見[靜態盤點實測](workflow_speed_optimization_2026-10-23_static_inventory.md)、
[TEJ 逐表覆蓋](workflow_speed_optimization_2026-10-23_tej_table_coverage.md)及
[撤回的 C++ 候選](workflow_speed_optimization_2026-10-23_scalar_prefilter.md)。

## 原因與實作

1. `CycleCosts` 會把 JSON 批次的 `seconds` 改成按比例分攤的整輪容量成本，
   原先直接量到的批次耗時保存在 `fetch_validate_publish_seconds`。現在將
   分攤值標為 `allocated_cycle_wall_seconds`，另行輸出直接批次計時。
   分攤值與共用 cycle wall 保留，但不綁定為來源的直接實測。
2. 每表原本只取全域最近 128 筆，會遺漏較少執行的資料集。現在完整保留
   這個視窗，另外選取每個 dataset 的最新一筆，最多 256 個資料集；截斷會
   明確紀錄。所有查詢使用唯讀 URI、`query_only`、同一讀取交易及每庫三秒
   deadline；不建立索引、不改 schema、不觸發供應者請求。
3. 既有 `update_observations.sqlite3` 的 `checks` 已保存成功請求身分、來源、
   時鐘與筆數。按資料集選最新成功紀錄，保留原始 `metadata_json`，避免把
   較新的失敗 head 當成成功計時。原始 UTC 差值只標為
   `wall_clock_delta_seconds`；時鐘倒退、缺漏或數值不一致不補零。
4. `received_rows` 新增可選的單調時鐘起點；free 與 complement 的共用 HTTP
   入口在配額等待及耐久請求記帳之後開始計時，包含 HTTP、解碼及基本回應
   驗證。sponsor 共用後者。收據以獨立的
   `finmind_request_to_parsed_response_monotonic_v1` 契約保存新欄位；既有 UTC
   觀測、資料值、fingerprint、publication clock、重試及配額語意保留。
5. free 的正式根目錄是 `data_finmind`，另外兩個 owner 分別是其 `sponsor`
   及 `complement` 子目錄。初次 delta 尚未納入 free，修正後的完整收集才
   作為本輪最終覆蓋；每筆 observation 都明列相對資料庫路徑。

此單調時鐘不包含限速等待、業務特定驗證、Parquet 發布或 queue commit，
並非整個下載工作流。批次回應可分配至多個任務，因此保留 request ID、
完整回應筆數與任務儲存筆數；不能直接相加重複的請求耗時。

## 驗證與證據

證據根目錄：`artifacts/benchmarks/workflow_speed_optimization_20261021/round16/`。

- `measurement_protocol.json` 記錄量測範圍、三秒讀取邊界及驗收要求。
- `admission.json`、`observation_admission.json` 是唯讀原始探查；初次探查
  的 free 子目錄假設已由正式啟動設定核對並修正，不當作完整覆蓋證明。
- `finmind_binding/` 與 `binding_readback.json` 保留初次 delta：615 筆原始
  SQLite 紀錄、771 個計時樣本、653 個 summary group 經獨立程序讀回。
  此批補回 52 個來源，整體階段覆蓋 1,497 → 1,549，尚未納入 free。
- 第一批受影響回歸 428 項通過。修正正式 free 根目錄並加入 HTTP 呼叫端
  時鐘範圍、限速與記帳邊界、fan-out、持久化、時鐘回撥、未知時間不補零
  等檢查後，最終相關回歸 345 項通過；兩批測試範圍不同且重疊，不能相加。
  一次命令引用不存在的測試檔，以 exit 4 結束且未執行測試；原失敗收據
  保留，修正為實際的 `test_finmind_request_traffic.py` 後才進行上述驗收。
- 正式 CLI 沿用原四個 training roots、depth 15、benchmark depth 32、
  directory budget 500,000、每檔 256 MiB／總計 4,096 MiB 預算及原有明列
  收據；未縮小來源或輸出。TEJ 使用前輪已核對的 135 表 profile。

## 完整收集、缺口修復及最終覆蓋

`collection_full/collection.json` 保存 13 個階段及 12 項輸出。收集本體
674.292 秒；canonical 命令包含匯入、準備及兩種獨立讀回，共 703.227 秒。
父命令回收的 child user/system CPU 為 255.107／266.081 秒。觀測節點可用
CPU 數為 16、開始時保守記憶體 headroom 約 76.9 GiB、load average 約
40.81／53.75／44.03；共享負載與快取沒有固定。上述是完整命令的觀測，
不能與前輪不同資料、快取及負載的耗時作因果加速比較。

`collection_readback.json` 獨立驗證 12 項輸出、6,441 份原始 gzip、223,929
個計時樣本及 20,614 個 summary group。`finmind_full_readback.json` 另外
逐筆核對 619 筆 SQLite 原始紀錄及 776 個 FinMind 計時樣本：97 個 UTC
差值、545 個既有直接／整輪 wall 樣本、134 個分攤校準樣本。後者不作為
來源直接計時。共 66 個 FinMind 來源具備階段紀錄，其中 51 個目前只有
UTC 差值；正式資料尚未出現新 request 單調時鐘欄位。

最終 registry 的階段覆蓋為 **1,553／2,399**，較前輪補回 **56** 個精確
來源 ID，原先已綁定的 ID 無遺失，剩餘 **846** 個項目未綁定。逐項明細
在 `remaining_source_timing_gaps.csv`，本輪驗收摘要是 `phase_acceptance.json`。
這是階段計時覆蓋，逐來源完整工作流 acceptance 仍為 0。

原完整收集在 `artifacts/benchmarks` 的 training timing discovery 達到原
60 秒上限，留下 1 個 traversal gap。原輸出及 timeout 不改寫；原
`full_adoption.json` 的輸出完整性標籤不能當作搜尋範圍完整性的證明。

以原四個 roots、depth、檔名規則、排除規則及原 deadline 重查，預設 thread
設定與 thread 1 均找回相同的 1,138 個檔案，沒有搜尋缺口。本次觀測為
4.315／20.375 秒；namespace、快取與共享負載未固定，不主張加速倍率，也
沒有修改預設設定。初版診斷腳本曾因報表欄位名稱錯誤，在搜尋前退出，
原 source／失敗收據保留；修正版本從原始 CLI argv 讀取四個 roots。

`training_discovery_completion/` 補齊原先未攝取的 **462** 個檔案、**8,427**
筆計時；連同原資料共 232,356 個不同來源／範圍的計時樣本，不能將它們
相加為整體耗時。`training_discovery_completion_readback.json` 核對全部
補充來源 SHA／gzip、完整身分與數值、重複次數及 6,490 個 summary group。
`EPOCH_PHASES` 是 set，各程序的欄位列順序可能不同；初版驗證器誤作有序
list 比較而失敗，修正版改驗完整 row multiset，未改變來源或輸出列順序。
兩次擷取的時點明確分開，不能包裝成一次新的完整流程計時。

下一批唯讀盤點涵蓋 93 個經濟來源及 20 個 OpenBB 來源。經濟來源中 88 個
有 acquired 標記的收據、5 個未找到收據；已檢查的成功／失敗 metadata
均沒有 elapsed/duration/timing/seconds 欄位。此證據只確認時鐘缺口，
沒有重新驗證底層資料完整性；不能從收據更新時間補出歷史耗時。

## 尚未完成

尚未驗證正式下載服務已載入新單調時鐘版本；工作目錄及測試證據不能取代
runtime adoption。歷史觀測的 UTC 差值也不能升級為單調時鐘實測。

既有紀錄仍有成功的本機衍生工作缺乏 transport clock；保留缺口，不能將其
耗時視為零。各來源的等待、失敗、轉換、發布、獨立資料讀回及整體歷史工作
仍需逐項計時與驗收；registry 包含不同粒度的來源、群組、產品及憑證項目，
不是相同工作量的 API 分母。`all_workflows_measured=false`、
`hardware_limit_proven=false`；目前不能宣稱全局優化完成。
