# R26：完整 reader 的正式整合與全量驗收

R22 八筆比較與額外原版讀回通過、R25 完整比速與獨立讀回結束後，已套用
可逆的三份 canonical source，**712 項回歸全部通過，無失敗或 skipped**。
R26 驗收時的 canonical profile SHA 為
`3301e6d58956dc96efd83d4f05a636e215bb195505d01b713906436f888360da`。
完整 collector 與兩種獨立 reader 已全數成功，兩者除觀測時間外的回傳欄位
完全一致；原 owner 已 succeeded／inactive／MainPID 0。此輪工程整合驗收
完成，完整 collector 的因果加速與所有來源測速仍是後續工作。

後續兩處串流 buffering 與新 source identity 記於
[R28 整合報告](workflow_speed_optimization_2026-10-24_buffer_integration.md)；
本報告的 R26 秒數、來源封存與門檻保持原樣。

## 已完成的整合

三個宣告修改保留 R24 profile 其他全部 bytes：替換 snapshot loop 的 opener／
context、加入共用 helper import，以及把該 helper 納入 measurement source
capture。反轉三個修改，逐 byte 恢復原生日誌版 SHA `35822c0bc4779b0abbeca63109bd56a2a167049ffc588d7c5d28fe756473521e`；既有 TW-public 消費路徑
與其他 dirty work 保留，沒有改輸出欄位、財務語意或 source clock。

採用的 `verify_collection` AST 與 R22 完成計時的 candidate 相同；
helper **4,177 bytes** 與計時版本完全相同。新 test 移除 private benchmark
路徑引用，改從 `stockagent.workflow_snapshot_streams` 匯入，其餘測試 body
保留。三份來源逐 byte 符合 proposal，隨後的完整 pytest child 為
**55.826868 秒／712 passed**，JUnit、log 及執行來源均封存。

| 採用來源 | SHA256 |
| --- | --- |
| `scripts/profile_workflow_speeds.py` | `3301e6d58956dc96efd83d4f05a636e215bb195505d01b713906436f888360da` |
| `stockagent/workflow_snapshot_streams.py` | `b877bffeac11fd39bc9cc55687c335c2934babc39e141b836fe3b5a737fc1593` |
| `test/test_workflow_snapshot_streams.py` | `c384876132856c08a79a464f7ea203c41cfb04732e290e992fd55a9f6decdae9` |

三份 canonical 目的檔均已建立或精確更新；原 R22 執行來源、固定 corpus、
來源 guard 與 gate 保持不變。正式服務未重啟，collector 的 source adoption
與 production service loaded-code 證明仍是不同範圍。

## 後續驗收邊界

採用前已確認 R22 ABBA／BAAB 的 whole-child wall B/A ≤ 0.98、CPU B/A ≤
1.02，八次完整十三輸出與原版額外 oracle 全部通過。R22／R25 原 owner 都
已 succeeded／actual MainPID 0，才開始本輪回歸與全量 collector。R25 writer
兩半較慢，維持不採用，不與 reader 正面結果混合。

採用時已重新核對 Git state、原 profile 及另外七個 source pin；保留所有
其他 dirty work。隨後完成完整新觀測與獨立 scope／SHA／summary 讀回驗證。

acceptance driver 的 `prepare`、`checks` 已成功；前者封存八份來源／proposal
並核對前置 terminal owners，不修改 canonical source。手動採用後執行
`checks`，涵蓋既有七組 collector／source-clock 回歸及新增 opener 測試。
`full` 保留 R24 的 218 explicit receipts、四個 training roots、原生日誌與
全部十三個 phase，另外連結本輪 adoption、R22 readback 及 R25 terminal run。

全量驗收包含 canonical collector、採用版完整 reader、原版完整 reader 加
獨立 native-clock／scope oracle。原版仍從 R22 不變的 archive 執行，核對
另外七份共用來源；不擴大或修改 R22 已凍結的 source guard。scope oracle
沿用 R24 原文，只替換 loader 及描述該 loader 的欄位，反轉必須恢復原文。
兩個 reader 除觀測時間外的全部回傳欄位須一致，最後再核對全部 measurement
source SHA。這是完整行為驗收；新增觀測的 collector 時間不當作固定輸入的
因果加速比較。

全量 run 為 `workflow-r26-full-reader-collect-20261023T194807-fc478e7f`，
已確認成功退出。admission 要求 memory headroom 24 GiB，D reserve／forecast
32／8 GiB、root reserve 8 GiB；當時 D／root free 分別為
**578,200,866,816／43,051,065,344 bytes**。不足會延期，不刪舊證據。

## 完整新觀測結果

| 完整子程序 | Wall | Linux descendant CPU |
| --- | ---: | ---: |
| collector | 581.819945 s | 320.508490 s |
| 採用版完整 reader | 154.302757 s | 33.891759 s |
| 原版完整 reader＋scope／native-clock oracle | 342.136156 s | 63.499869 s |

三階段與最後來源核對 **1,078.821419 秒**；包含 admission／launch 等的 canonical
supervisor command 為 **1,079.693533 秒**。額外原版 oracle 也完整保留在耗時
紀錄，沒有把它省略來美化整輪時間。這個新觀測不是固定輸入 ABBA，不把上表
單次數字或前輪不同環境的 collector 時間當作因果倍率。

兩個 reader 均核對 **十三輸出、6,823 份原始 gzip、240,615 個 samples、
27,603 個 summary groups、十三個 collector phases**；全部回傳欄位除 UTC
觀測時間外相同。所有 **29 份 measurement source** 在本次結束核對時 SHA
一致。source registry **2,399**，既有 stage bindings 無遺失，stage-bound
**1,647**、未綁定 **752**；known inventory 只保留身分，舊 timing 匯入為零。
原生 journal 的一筆 **0.508543692 秒** dispatch、PID／invocation／spec／policy
及未知 work denominator 均由獨立 oracle 重查，沒有資料完整性或 live-readiness
推定，沒有 traversal gaps。

目前同輪主要 phase 為 evidence inspection **309.355206 秒 wall／159.925689
秒 CPU**、aggregation／output **106.681460／27.624694 秒**、final hashes
**39.548669／4.816448 秒**。R27 從第二項中的五個串流 renderer 量測 buffering，
原 continuation 已接續；reader 的固定比較結果仍以 R22 的兩半 gate 為準。

## 證據

- [採用前可逆 bytes、AST 與來源準備證據](../artifacts/benchmarks/workflow_speed_optimization_20261021/round26/integration_preparation.json)
- [完整 proposed patch](../artifacts/benchmarks/workflow_speed_optimization_20261021/round26/proposed_canonical_reader.diff)
- [實際 builder 收據](../artifacts/operations/agent-workflow/runs/workflow-r26-prepare-reader-inte-20261023T190145-bc17eeba/run.json)
- [已執行的完整驗收 driver](../artifacts/benchmarks/workflow_speed_optimization_20261021/round26/accept_reader_integration.py)
- [前置 terminal／八份來源封存](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round26/preparation.json)
- [712 項回歸、完整 JUnit／log 指紋](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round26/checks.json)
- [canonical adoption 與三份來源](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round26/canonical_adoption.json)
- [完整 collector＋兩種 reader 實際 owner](../artifacts/operations/agent-workflow/runs/workflow-r26-full-reader-collect-20261023T194807-fc478e7f/run.json)
- [完整三階段與來源核對驗收](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round26/full_collection_acceptance.json)
- [採用版十三輸出與全部原始 bytes 讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round26/canonical_readback.json)
- [原版獨立 scope／native-clock 讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round26/full_collection_scope_readback.json)
- [R22 完整固定比較與進度](workflow_speed_optimization_2026-10-24_streaming_readback.md)
- [R24 原生日誌與全量範圍驗收](workflow_speed_optimization_2026-10-24_canonical_journal.md)
- [R25 私有輸出比較與接續 owner](workflow_speed_optimization_2026-10-24_native_output.md)
