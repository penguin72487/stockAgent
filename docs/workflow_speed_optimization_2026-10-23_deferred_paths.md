# 工作流測速：延後建立路徑候選未達採用門檻（2026-10-23，Round 9）

本輪延續 `workflow-speed-baseline-20261021`。八次完整比較均同值，但第二組未達事前
設定的效能門檻，因此未採用候選，C++ 已逐 bytes 還原到前輪版本。四個新增語意測試
保留，還原後 172 項測試通過。完整 collector 與獨立 reader 已通過；封存與最終
獨立回讀以本頁末端連結的 acceptance receipts 為準。

## 成本與實作

前輪完整 collector 的最大階段為內容檢查：269.602193 秒 wall、159.093578 秒
process CPU。這是先前輸入與主機狀態的觀測，不能作為本輪候選的加速分母。
本輪開始已核對前輪封存、獨立回讀與九個來源檔案身分，見
[起始快照](../artifacts/benchmarks/workflow_speed_optimization_20261021/round9/baseline_manifest.json)。

原 C++ walker 為每個可見容器建立完整 JSON pointer，即使該分支沒有任何計時欄位。
候選仍解析與巡覽全部原有資料，先在深度有界的堆疊借用跳脫後的 key 與 list offset，
到第一個有效計時欄位才建立祖先路徑。同一遞迴 frame 內重用已建立的路徑，返回後
釋放；不持久保存 producer values。Python fallback、深度限制、key 分類、遮罩、
有限值檢查、數值物件、順序與輸出單位保留。

候選改動 C++ walker 與語意測試兩個檔案；未採用的來源完整留在
[凍結候選](../artifacts/benchmarks/workflow_speed_optimization_20261021/round9/optimized_source/stockagent/workflow_metric_tree.cpp)。
目前 [C++ walker](../stockagent/workflow_metric_tree.cpp) 與前輪逐 bytes 相同，僅
[語意測試](../test/test_workflow_metric_tree.py) 保留新增 prefix、offset、Unicode、兄弟分支、
負分支錯誤與借用 metadata 生命週期檢查。候選與還原版本各通過 172 項測試；還原證據見
[selected semantic proof](../artifacts/benchmarks/workflow_speed_optimization_20261021/round9/selected_semantic_tests.json)。

另檢查過以 `msgspec.Raw` 取代完整解碼的想法。[官方說明](https://msgspec.dev/supported-types#raw)
將 Raw 用於延後值的解碼；本機 0.22.0 實測直接替換會漏掉部分無效 UTF-8 與整數位數
限制錯誤，故未採用。完整案例保留於
[否決紀錄](../artifacts/benchmarks/workflow_speed_optimization_20261021/round9/negative_parser_probe.json)。

## 比較契約

[事前凍結 protocol](../artifacts/benchmarks/workflow_speed_optimization_20261021/round9/comparison_protocol.json)
採 `A B B A / B A A B`，A 為前輪已驗收來源，B 為延後建立路徑。每次新程序皆保留：

- 372,550 份固定輸入、9,383,953,790 bytes、107 份 explicit receipts。
- 全部原有讀取、SHA、分類、必要 JSON 解析與巡覽、壓縮、六份輸出與獨立 reader。
- 705,929 個 reported fields、2,006 normalized samples、722 groups。
- 3,739 份唯一保留原文的 gzip 回讀；354,177 份非計時 payload 僅記錄身分與分類，
  不宣稱這些 payload 的原文已另行保存或獨立回讀。
- C++ backend、單一預讀 worker、256 MiB batch；每次前後可見保守 RAM headroom
  至少 4 GiB。未清除 OS cache，未主張獨占主機或冷磁碟。

兩組各自都必須輸出與回讀同值、平均 worker RSS 不高於基準 102%，並符合其中一項：
完整平均 wall 至少減少 2% 且 CPU 不增加；或 wall 在基準 102% 內且 CPU 至少減少 5%。
全部 profiling-off 樣本保留，不按最快單次挑選。操作明細診斷獨立執行，不納入效能選擇。

完整 wall 包含新程序匯入、輸入計畫載入、工作、輸出、獨立 reader 與程序結束。
CPU 為 worker 計時區段的 process time（所有 threads），不含匯入、輸入計畫及 reader
child；RSS 為 worker 程序生命週期峰值，不是父子程序的合計峰值。兩者不能混為同一分母。

## 實測與完整工作流驗收

以下為全部 profiling-off 樣本；RSS 是單一 worker 的峰值。

| 次序 | 來源 | 完整 wall 秒 | Worker CPU 秒 | RSS MiB |
| --- | --- | ---: | ---: | ---: |
| 1 | 基準 A | 393.188455 | 237.403173 | 2084.00 |
| 2 | 候選 B | 206.952725 | 175.288085 | 2090.66 |
| 3 | 候選 B | 206.103095 | 176.213053 | 2088.78 |
| 4 | 基準 A | 240.710392 | 197.719178 | 2082.68 |
| 5 | 候選 B | 217.261755 | 173.257227 | 2089.16 |
| 6 | 基準 A | 211.307726 | 178.694080 | 2084.43 |
| 7 | 基準 A | 180.878274 | 159.421982 | 2097.32 |
| 8 | 候選 B | 169.080713 | 149.679447 | 2089.05 |

第一組 ABBA 平均 wall 316.949424 → 206.527910 秒（少 34.84%），CPU 少 19.22%；
第二組 BAAB 為 196.093000 → 193.171234 秒（少 **1.49%**），CPU 為
169.058031 → 161.468337 秒（少 **4.49%**），RSS 少 0.085%。第二組未達 2% wall
或 5% CPU 門檻，故 `performance_candidate_adopted=false`。

基準本身 393.19 → 240.71 → 211.31 → 180.88 秒有明顯漂移；暖機、主機負載等是可能
因素，未單獨識別其因果。因此不將八次合併平均或最快單次的差距當成可歸因的加速。
原始來源、輸入、主機觀測、六輸出 SHA、每次獨立回讀及兩組門檻均保留在
[完整比較](../artifacts/benchmarks/workflow_speed_optimization_20261021/round9/balanced_comparison.json)。

已增加獨立的序列化診斷：以既有計時元件包住 canonical writer／aggregation calls，分別
測量 JSONL、彙總、三份 CSV、inventory JSON、evidence manifest、輸出雜湊與 collection
manifest 共九步。它不改 canonical 輸出，不納入效能採用。

| 固定輸入診斷：序列化步驟 | Wall 秒 | Thread CPU 秒 | 已完成工作量 |
| --- | ---: | ---: | --- |
| samples JSONL | 0.020431 | 0.020427 | 2,006 samples |
| 樣本彙總 | 0.004160 | 0.004160 | 2,006 samples → 722 groups |
| summary CSV | 0.015396 | 0.015394 | 722 groups |
| reported metrics CSV | 8.089352 | 6.695207 | 705,929 fields／290,283,144 bytes |
| receipts CSV | 5.437825 | 4.492352 | 372,550 receipts／155,551,115 bytes |
| inventory JSON | 0.085888 | 0.002810 | 本固定工作量的空 inventory |
| evidence manifest JSON | 9.689845 | 7.515256 | 372,550 identities／202,684,792 bytes |
| 六輸出雜湊回讀 | 0.666327 | 0.654874 | 651,006,697 bytes |
| collection manifest | 0.063582 | 0.003237 | 1,184 bytes |

另外保留十五個 inspection operation/thread 組合：總計 1,117,543 次 stat、341,023 次
JSON decode、19,076 次 metric-tree 呼叫等。主 thread 的 JSON decode 為 16.634 秒 wall，
metric tree 為 12.120 秒 wall；讀取、分類與 batch 的 inclusive 範圍有巢狀與跨 thread
重疊。兩個時鐘分別記錄，少數聚合 thread CPU 略高於 wall，未改寫原值；不可相加
或以 wall−CPU 推定 I/O。詳細原值、輸出大小與吞吐分母見
[operation units summary](../artifacts/benchmarks/workflow_speed_optimization_20261021/round9/operation_units_summary.json)。
writer 吞吐包含編碼與原有 durability 工作，不是磁碟頻寬。

完整 inherited collector 的 outer wall 為 **420.231446 秒**，另行 reader 為 **19.062018 秒**。
235,874 samples、26,591 groups、13 serial phases、12 outputs、6,560 unique originals
通過獨立驗證，issues=0、traversal_gaps=0，C++ backend frozen identity 驗證通過。
最大階段依序為內容檢查 **298.221368 秒 wall／162.507030 秒 CPU**、路徑選取
32.497778 秒、序列化 27.837936 秒、discovery 23.204856 秒。當前輸入與主機不同，
不與前輪完整 collector 相除來宣稱加速。沒有重建 unchanged package；前輪 wheel 的
collector／loader／C++ 三個資產及 wheel bytes 已重新核對，沒有安裝套件。

來源 registry 共 2,399 項，1,362 項有既存 stage timing 綁定，1,037 項尚未綁定，
完整來源工作流驗收數仍為 0。registry 混有 groups、products、credentials 與 inventory，
不能把它當成同粒度 API 分母。兩筆既有架構快照 config errors 保留。清冊與全部階段見
[remaining coverage](../artifacts/benchmarks/workflow_speed_optimization_20261021/round9/remaining_workflow_coverage.json)。

遠端於 2026-10-22 16:38:26 UTC 重新檢查：已登錄端點的唯讀 SSH 回報
`No route to host`，exit 255；未開始 GPU 計算。沒有重啟服務、發布或新訓練。

最終證據：[source disposition](../artifacts/benchmarks/workflow_speed_optimization_20261021/round9/source_disposition.json)、
[完整 collection](../artifacts/benchmarks/workflow_speed_optimization_20261021/round9/canonical_full_adoption.json)、
[seal](../artifacts/benchmarks/workflow_speed_optimization_20261021/round9/optimization_acceptance.json)、
[independent seal readback](../artifacts/benchmarks/workflow_speed_optimization_20261021/round9/optimization_acceptance_readback.json)。

## 尚未完成

此工作針對測速收據 collector。來源專屬 operation grain、正式服務 loaded-code 採用、
完整來源端到端與遠端 GPU fold 仍需各自證據，不能以 collector 或歷史 producer samples
代替。`all_workflows_measured=false`、`hardware_limit_proven=false` 保留；不宣稱已達計算極限。
下一個局部候選是保持同樣 bytes 的 metadata pretty formatting；先對完整 manifest 的
編碼、原有原子寫入與回讀測量，再驗證完整固定工作量，不直接投射局部改善。
