# 第七輪工作流優化：有界預讀、時間紀錄重用與掃描成本

接續[第六輪](workflow_speed_optimization_2026-10-22_native_cache.md)，沿用
`workflow-speed-baseline-20261021`。本輪證據根目錄為
`artifacts/benchmarks/workflow_speed_optimization_20261021/round7/`。
前輪與本輪各個來源版本、負面結果及原始收據均保留。

讀取可以與處理重疊，分類、預算判定及保存仍須依原順序執行。新增的預讀功能
放在既有 `Evidence` owner，worker 只讀取、stat 與 SHA；主執行緒檢查檔案版本後，
才分類、壓縮與寫出。讀取期間變動的檔案仍拒絕；穩定讀取後再被替換的版本
會退回同步重讀。明確指定的 receipt、超過批次容量與非一般檔案保留同步路徑。

每批最多 32 個路徑、2 MiB 原文；pending batch 最多為 worker 數，加上消費中的
一批。單一 worker 的預讀原文緩衝上限為 `2 * (2 MiB + 1)` bytes，另有 Python
物件、I/O buffer 與同步大檔。整體 RSS 另外實測，沒有把這個局部上限當作
整個程序的記憶體上限。例外或提早停止時會取消未開始的工作並回收執行緒。

第二個候選直接在已建立且由檢查器擁有的 timing row 加上來源欄位，避免再複製
字典。producer 物件與原始數值不變，CSV 欄位與順序不變。既有 C++ 欄位掃描、
語意快取、Python fallback 與敏感欄位規則沿用第六輪，沒有修改原生核心。

所有固定輸入比較使用 **372,550 份、9,383,953,790 bytes** 的獨立副本與
107 份 explicit receipt。manifest SHA：
`f4bb9b16c7d423b7dbb85bc1f1c61cb5e990596de2c2fe881672572b0959044d`。
每次完整讀取、SHA、分類、必要解析、保存、六份輸出與另一程序的讀回皆保留；
705,929 個 reported 欄位、2,006 個正規 benchmark 樣本與 722 個 summary groups
維持一致。reader 核對全部輸入識別及 3,739 份獨特保留原文；非 timing payload
只有識別紀錄，未另存並讀回其原文。

完整 wall 包含新程序啟動、匯入、manifest 載入、native 初始化、處理、輸出與
獨立讀回；編譯準備另列。CPU 是 worker 計時區間的所有執行緒總和，未含匯入、
manifest 前置載入與 reader 子程序；RSS 是單一 worker 的峰值。主機持續有其他
owner 運作，沒有清 OS cache、中止背景服務或宣稱獨占／冷讀測試。

第一批事先宣告同步、2、4、4、2、同步六次完整比較。要求完整平均 wall 至少
少 2%、CPU 增加不超過 10%、平均峰值 RSS 增加不超過 2%，且全部輸出與讀回一致。

| 首輪方式 | 完整平均秒 | worker CPU 秒 | 採用判定 |
| --- | ---: | ---: | --- |
| 同步 | 191.221406 | 128.659042 | 首輪保留 |
| 2 個預讀 worker | 176.314293 | 173.575221 | CPU 增加約 35%，拒絕 |
| 4 個預讀 worker | 223.626511 | 266.102808 | wall 與 CPU 都變差，拒絕 |

同步兩次原始值為 263.644637、118.798175 秒，主機／cache 波動很大；不能用
最早一組的明顯差距直接宣稱加速。`prefetch_mirrored.json` 保留全部六筆。

接著以完整 ABBA 比較同步與「單一 I/O worker 加主執行緒」。原始完整秒數為
同步 175.712247、單一 worker 160.684393、單一 worker 140.342722、同步
168.458184，四次輸出與讀回皆一致。`single_prefetch_abba.json` 的平均如下。

| 單一 worker 確認 | 完整平均秒 | worker CPU 秒 |
| --- | ---: | ---: |
| 同步對照 | 172.085216 | 130.910531 |
| 單一預讀 worker | 150.513557 | 125.252614 |

本批完整 wall 少 **12.5%**、CPU 少 **4.3%**，平均峰值 RSS 少約 0.47%，採用
單一 worker。多 worker 的較高 CPU 與 context switch 提供排程成本線索，
尚未把差異唯一歸因於 GIL。序列化與讀回也有波動，此比例不保證未來重跑相同。

追加候選時，本輪漏查等待程序已載入舊版 gate，讓第一次 row 比較的首組與
training 掃描重疊。原自動驗收因缺少 `row_selection.json` 停止，未啟動完整
collector。原 row 四筆與原 rg 八筆、失敗 run 與所有輸出均保留，排除速度推論；
`measurement_overlap_rejection.json` 記錄兩個 protocol 的起始與原始樣本完成時間。
修正後以全新程序、前一組的 terminal receipt 與入口 SHA 查核，重新串行測量。
這不是剔除負載較高的個別樣本；兩個受影響批次整批排除，其他有效慢樣本保留。

新的 `row_reuse_confirmation.json` 使用相同單一預讀 worker、完整 ABBA 與原
固定輸入。要求 wall 至少少 2%、CPU 不增加、RSS 增加不超過 2%；另有事前宣告
的記憶體取捨條件。此次依前者通過，四筆均保留。

| 時間紀錄重用確認 | 完整平均秒 | worker CPU 秒 |
| --- | ---: | ---: |
| 原字典複製 | 165.246077 | 133.720627 |
| 補欄位後直接重用 | 147.373160 | 129.944367 |

完整 wall 少 **10.8%**、CPU 少 **2.8%**；平均峰值 RSS 增加約 0.08%，沒有宣稱
峰值記憶體改善。原始完整秒數依序為 149.473040、128.257381、166.488939、
181.019114。這與單一 worker 是不同批次，兩個百分比不可相乘或投影為全系統效果。

training discovery 補上 Unix `RUSAGE_CHILDREN` 的 user/system CPU、page fault、
block I/O operations 與 voluntary/involuntary context switch。依
[Python resource 契約](https://docs.python.org/3.12/library/resource.html)，
這些是已終止並等待的子程序資源；block counter 不是邏輯 bytes，wall 減 CPU
也不是 I/O 等待秒數。預讀回收使用
[Executor shutdown 契約](https://docs.python.org/3.12/library/concurrent.futures.html#concurrent.futures.Executor.shutdown)。

乾淨的 `training_rg_confirmation.json` 以原四個根目錄、原深度與所有檔名規則，
執行 default、1、2、4、4、2、1、default。全部路徑與 gap 輸出 SHA 相同，
1,138 個 timing files、gap 0，並綁定 rg binary SHA／版本。每筆包含四個根目錄
搜尋、輸出與獨立讀回；不含外層 Python 啟動，不能和前兩張新程序 wall 混用。

| rg 排程 | 完整平均秒 | 子程序 CPU 秒 |
| --- | ---: | ---: |
| 預設 heuristics | 4.742779 | 30.371315 |
| 1 thread | 16.089968 | 14.865794 |
| 2 threads | 16.838773 | 26.812569 |
| 4 threads | 14.128117 | 39.011495 |

固定 thread 候選沒有通過完整 wall／CPU 門檻，保留 rg 預設排程。主程序 CPU
平均僅約 0.087 秒，子程序約 30.371 CPU 秒；先前僅看主程序 CPU 的缺口已補上。
此結果沒有把 Rust rg 改寫成另一個掃描 owner。

162 項相關測試通過，包含原文、來源變動、替換／symlink、預算、失敗、重複路徑、
bounded queue、提早停止、Python/C++ producer 物件與原數值身分等。語法、focused
lint 與 Git whitespace 檢查通過；隔離 wheel 為 9,229,259 bytes，精確包含現行
collector／loader／C++ source，解開後獨立匯入與原生建置通過，未安裝環境套件。

完整 configured collector 已通過，程序耗時 **342.679796 秒**，另一程序獨立
讀回 **15.588333 秒**。保留原四個 training 根目錄、深度、檔名與 evidence 預算，
沒有縮小收集範圍。核對 **235,870 筆速度樣本、26,584 組摘要、13 個階段、12 份
輸出與 6,543 份獨特保留原文**，原生 backend 查核 1，擷取問題及遍歷缺口皆為 0。
這是最新 live inputs／host 下的完整觀測，不能直接與前輪不同輸入的 550 秒
當作因果比較。

| 完整 collector 階段 | wall 秒 | 主程序 CPU 秒 |
| --- | ---: | ---: |
| 程式盤點與來源保存 | 12.360234 | 12.235761 |
| 環境觀測 | 0.024971 | 0.016262 |
| 範圍盤點 | 0.085559 | 0.085099 |
| service／HTTP／TEJ／storage 收據 | 0.106798 | 0.102283 |
| acquisition 收據 | 2.797117 | 1.888340 |
| engineering command 收據 | 0.515996 | 0.506962 |
| training timing 搜尋 | 4.058720 | 0.090663 |
| training timing 讀取 | 7.983787 | 7.498233 |
| benchmark 搜尋 | 21.266530 | 21.012659 |
| benchmark 路徑選擇 | 27.521724 | 26.379527 |
| benchmark 證據檢查 | 234.564174 | 136.321596 |
| 彙整與輸出序列化 | 28.329836 | 23.746274 |
| 最終輸出 SHA | 1.475117 | 1.452944 |

階段 wall 合計 341.090562 秒；完整程序還包含啟動、初始化及最終 manifest 寫出。
表中 CPU 不含 rg 子程序及獨立 reader；子程序資源另保存在
`canonical_full_adoption.json`。本次實際預讀 371,420 次、同步讀取 4,213 次，
不是全部改為並行。benchmark 候選為 372,584 份，索引欄位 706,952 個；
354,177 份非 timing payload 僅記錄識別，不冒充原文還原證據。

最終來源與 flags 綁定 `implementation_adoption.json`、
`final_collection_arguments.json`，採用
`--evidence-read-workers 1 --training-discovery-threads 0`；一般 CLI 預設仍為
同步讀取，新的環境須重測後選定參數。完整讀回契約與其 SHA 保存在
`collection_full_readback.json`，沒有以 collector 自報成功代替 reader。

本輪共保留 34 筆候選比較：22 筆可用於速度比較，另 12 筆重疊批次僅保留
原文與同值證據。`optimization_acceptance.json` 封存來源、收據、比較輸出、
完整 collector、套件與本報告；`optimization_acceptance_readback.json` 由另一
程序重算檔案 SHA、原始樣本與 68 個平均，重判各個採用門檻。

依最新完整 wall，下一輪優先拆分證據檢查內的讀取／雜湊、分類、JSON 解析、
gzip 保存與輸出轉換，先量 instrumentation 自身成本，再以同一固定輸入、
完整輸出與獨立讀回驗證候選。其次是彙整序列化與路徑選擇；增加執行緒已出現
負收益，不能以核心數推估加速，也不能把 wall 減 CPU 直接歸為 I/O。

目前盤點 2,399 個 source endpoint，其中 1,361 有階段計時、1,038 未綁定階段
計時；逐來源完整 E2E 驗收仍為 0。另有 2 份歷史 deployment config 因引用的
artifact 不存在而無法解析，原路徑與錯誤保留在 `collection_full/inventory.json`。
擷取問題 0 不表示這些 coverage 缺口已解決。

2026-10-22 13:23 UTC 前重新查核 enrolled Vast transport，約 1.623 秒即回報
`No route to host`，記於 `remote_speed_final_admission.json`。未啟動遠端訓練；
這是連線失敗耗時，沒有當作 GPU 計算速度。遠端恢復後仍須以 canonical owner
量完整 fold，包含驗證、曲線、plot、checkpoint、回傳與獨立讀回。

全局工作仍未完成：逐來源完整 E2E、正式服務完整 loaded workflow、遠端完整 GPU
fold 與硬體下界仍須各自驗收。這份工程結果保留 `all_workflows_measured=false`
與 `hardware_limit_proven=false`，沒有刪減資料、曲線、checkpoint、驗證或 readback。
