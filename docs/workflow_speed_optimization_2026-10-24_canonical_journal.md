# 工作流測速第二十四輪：canonical 日誌接入與比較來源隔離

原生日誌入口已合入 `scripts/profile_workflow_speeds.py`，新增可重複的
`--tw-public-operation-journal PATH`。canonical 相關回歸 **696 passed**，完整
pytest 子命令 **48.832828 秒**；接入前後的來源隔離檢查另各四項通過，完整
子命令為 **2.345931／2.524175 秒**。沒有來源請求、GPU 工作或正式服務重啟。
全量 collector 與原版獨立讀回已通過：2,399 個來源 ID 全部保留，stage timing
綁定由 1,646 增至 **1,647**，尚有 **752** 個未綁定。這是覆蓋更新，沒有宣稱
固定輸入的整體工作流加速。

## 已合入的實作

沿用共用 `collect_source_operation`、來源帳本、Evidence 與原版 verifier。
核對原生 ABI、完整 DatasetSpec 指紋、journal PID／boot／invocation、單調時鐘
及狀態；重複呼叫去重，衝突保留耗時並撤下來源綁定，未知工作量維持未知。
沒有以父程序耗時分攤來源，操作計時也不代表資料完整或 live readiness。

八段修改反轉後可還原編輯前全部 bytes，保留既有 dirty work；原版
`verify_collection` AST 不變。canonical profile SHA 為
`35822c0bc4779b0abbeca63109bd56a2a167049ffc588d7c5d28fe756473521e`。
新維護測試 `test/test_tw_public_journal_ingestion.py` 的 42 項案例不依賴私有
artifact、憑證或網路。696 項包含既有 collector、共用時鐘、economic/OpenBB、
TW 下載器／原生時鐘及 journal 接入；無 failures、errors、skips。

## 比較來源的明確邊界

R22 本來就執行封存原 profile 與固定候選 reader，但舊 guard 也要求未執行的
工作目錄 profile 不變，連帶阻擋新日誌接入。已在任何 timing trial 前保存舊
harness，明列 v2 邊界：封存執行 bytes、candidate function／helper、固定輸入
不變；未執行的工作目錄 profile 僅容許原 SHA 與上述新 SHA，其他七份共用來源
仍按原 pin 核對。protocol 保存 amendment SHA 與兩類來源的實際身分。

四項隔離檢查確認 baseline／candidate 核心確實來自 archive，並拒絕未知的
工作目錄 profile 或改變的 archive。A/B/B/A、B/A/A/B、wall B/A ≤ 0.98、CPU
B/A ≤ 1.02、完整十三輸出及額外原版讀回均保留；串流 reader 尚未採用。

原 R20 seal 與 R22 continuation 均繼續執行。R22 pin 也會等待本輪全量觀測
owner 的實際 terminal unit／收據與驗收結果，再啟動 fresh timing children，
避免兩個大量 I/O 工作重疊比速。前置等待不計入 reader 效能樣本。

## 全量範圍與讀回

新 owner：`workflow-r24-full-native-collect-20261023T181622-d0d76095`。
保留原有 **213 份明列收據、四個 training roots、十三階段**，known-source
inventory 使用前輪完整 2,399 個 ID；追加五份工程／遠端收據及已核對的真實
journal。清冊只沿用身分，不匯入舊耗時或健康狀態。新範圍不是與前輪固定輸入
的 collector speed comparison。

fresh admission 預留 D 32 GiB／root 8 GiB，新 D forecast 8 GiB、root scratch
512 MiB、最低 memory headroom 24 GiB，已通過；不足則延期，沒有刪資料。
collector 後由另一程序使用原版完整 reader，獨立核對全部輸出、summary、
gzip／SHA、舊來源 ID／stage binding、journal 原值、PID／invocation、spec／policy
指紋及未知工作量。獨立讀回確認十三輸出／十三階段、全部既有來源與 stage
binding 保留、沒有 traversal gaps，真實 journal 操作耗時 **0.508543692 秒**
原值不變，沒有匯入舊耗時。terminal owner 與最終來源核對另以對應收據為準；
上述結果不代表所有工作流完成測速或已達硬體極限。owner 已 terminal success；
完整 collector **784.681746 秒**、原版 scope reader **434.293382 秒**，兩者合計
**1,218.975764 秒**。最終來源核對確認十二份選定來源、四份採用檔案及
二十八份 measurement source 均與封存 SHA 一致，沿用已完成的完整 reader
證據，沒有重跑大型讀回。

## 下一個實測瓶頸

本輪 benchmark inspection 為 **425.041659 秒 wall／188.409072 秒 CPU**；
aggregation 與輸出為 **173.097097／40.168848 秒**，最終輸出 hashing 為
**48.443095／5.496413 秒**。這些是串行階段觀測，不與重疊取樣相加。

原 producer 的被動取樣長 **10.267166 秒**，CPU 使用 **2.18 秒**，寫入
35,109,648 bytes；200 次取樣中 117 次為 D state、115 次在 `p9_client_rpc`，
135 次位於 write syscall。取樣涵蓋輸出階段，無法據此細分每個函式或宣稱
純裝置吞吐量；沒有對目標送 signal、ptrace 或重啟。它支持優先測試輸出 I/O
成本，尚不支持改語言的加速主張。

既有 Windows 原生 FileStream writer 的 cold namespace 限制仍保留。下一個
候選限私有 benchmark 區，以相同 bytes／SHA、落盤與原子發布作比較，並避開
現有完整 reader 的正式比速；[R25 候選與監督接續](workflow_speed_optimization_2026-10-24_native_output.md)
已準備，還沒有候選通過或全量 collector 加速結果。

## 證據

- [admission、原來源及舊 harness 位置](../artifacts/benchmarks/workflow_speed_optimization_20261021/round24/location.json)
- [採用、可逆 bytes、來源及三批測試](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round24/canonical_adoption.json)
- [696 項回歸與完整子命令時間](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round24/checks_canonical_post.json)
- [固定 reader 與兩個非執行 profile SHA](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round22/worktree_profile_amendment.json)
- [比速前完整觀測 owner gate](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round22/worktree_observation_gate.json)
- [全量觀測的實際 supervisor owner](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round24/full_collection_owner.json)
- [完整輸入與邊界](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round24/collection_full_input_contract.json)
- [原版完整讀回與來源覆蓋](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round24/full_collection_scope_readback.json)
- [terminal owner、完整子程序時間與驗收](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round24/full_collection_acceptance.json)
- [最終選定來源與二十八份 source SHA](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round24/final_source_boundary_readback.json)
- [被動輸出 I/O 取樣](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round24/collector_proc_observation.json)
- [原串流候選及比較判準](workflow_speed_optimization_2026-10-24_streaming_readback.md)
- [前輪真實日誌 delta 與獨立讀回](workflow_speed_optimization_2026-10-24_native_journal_ingestion.md)
