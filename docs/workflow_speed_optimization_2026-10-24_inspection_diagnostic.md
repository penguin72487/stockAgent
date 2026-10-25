# R29：完整 inspection 成本診斷

後續已依 R47 成本實作原生批次讀取候選，完成 16 項語意檢查、3,986 份
輸入的八輪對照，以及 372,550 份固定輸入全量比較。全量雙順序完整 command
wall 降低 4.51%／3.97%，六份輸出語意一致；R85 已接入 canonical source 並
通過 236 項整合檢查；R86 共用 backend 再通過 28 項，合計 264 項。
R87 全部 13 階段、重建及原版 reader 已驗收，整體工作流的受控加速比例仍未測定。
見 [R63–R69 原生讀取實測](workflow_speed_optimization_2026-10-24_native_batch.md)。

最新 R134 已量化同內容的正向 metric-tree 重複工作；R135 的私有容量受限
快取通過 **522 項**語意檢查。R123 在額外 source-list 成員檢查失敗，R136
隨之前置拒絕，沒有執行比較。R137 接續精確來源證明與原始 oracle；新的
R138 原等待它實際成功；三個 owner 後因 boot 改變而中斷，已核對舊程序
不存在並記錄 interrupted。R141 在新 boot 以相同 guard 接續原始驗證，
R142／R143 分別接續固定比較與完整原始核對診斷。
R142 已 **8／8 次完整通過**，ABBA／BAAB 的完整 child wall 均值分別少
**16.7566%／10.1590%**，descendant CPU 分別少 **19.5911%／4.7128%**，
兩組原門檻均通過，六份輸出語意一致。這是固定 inspection 工作流的採用證據；
共用 `profile=879e8457…` 尚未套用快取，完整實際 13 階段仍待驗收。
詳見後段 R134–R157 與逐次原始收據。

R77 另已實作私有的寫出時 SHA 候選，等 R69 與 R70／R74–R76 全部終結後
才進行語意檢查，避免競爭目前全量比較。它沿用 canonical CSV／JSONL
序列化函數，只替換文字 stream：對底層實際接受的 bytes 計算 SHA，關閉後
保留 dev／inode／mode／size／mtime_ns／ctime_ns；輸出換代或未追蹤檔案仍
走原完整重讀。另一程序的獨立 reader 繼續重算所有輸出與原始證據 SHA。

候選只涵蓋 CSV／JSONL，atomic JSON／manifest 不套用這個快取，避免在未
掌握原寫入 descriptor generation 時直接信任記憶體中的 payload。R47
`output_hashing` 的 40.475390 秒僅是先前診斷，不能當成已節省的秒數。
原 R77 因前序 R74 的跨程序 set-order oracle 失敗而未執行；R79 保留原輸出
並完成完整 JSON row multiset 驗收，R80／R81 完成原生計時值的匯出與讀回後，
新 R82 實際完成同一候選的 **16 tests，0 failure／error／skip**。
包含原版 bytes、BOM／換行／Unicode／signed zero／大整數、短寫入、序列化
失敗、相同 mtime 的變動及開啟期間檔案替換；**3.989434 秒 wall／2.842020 秒
descendant CPU**，owner succeeded／inactive／MainPID 0。原 helper／tests 未改，
SHA 候選尚無速度採用結論，仍需固定全量雙順序比較及正式 13-phase 驗收。

- [R77 候選原始碼](../artifacts/benchmarks/workflow_speed_optimization_20261021/round77/write_time_hash.py)
- [R77 凍結來源與序列排程 protocol](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round77/protocol.json)
- [R77 原 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r77-write-time-hash-che-20261024T011033-366ac950/run.json)
- [R82 實際 16 項通過與完整命令時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round82/completion.json)

R78 在 **01:34:33 UTC** 已由 canonical supervisor 啟動，01:35 實查 owner
active／MainPID 1916345，已寫入全量 protocol。啟動前確認 R69、R70、
R79–R82 各自 succeeded／inactive／MainPID 0，R82 的 16 項檢查實際通過。
兩組共同使用 R69 通過的私有原生讀取，唯一比較軸為寫出時 SHA；固定
372,550 份輸入、全部六份輸出、新程序讀回及 A/B/B/A、B/A/A/B 均保留。
另記錄 serialization／output identity clocks、實際 hash hits 與 fallback，
以完整 child wall／descendant CPU 決定，不能只比較省下的讀取 syscall。
已通過 24 GiB memory headroom／root 8 GiB／D 32 GiB 加預估輸出的 admission。
本次凍結 18 個實作／收據檔，canonical profile 仍為 `f0693863…`，helper
SHA 為 `49389981386b1cb686f3ecd349428984818c344d4286d8753179dbd37cdc96fc`。
01:35 當時 R78 尚無速度結果，原生讀取與寫出 SHA 都尚未接入 canonical source。

- [R78 完整比較程式](../artifacts/benchmarks/workflow_speed_optimization_20261021/round78/compare_write_time_hash.py)
- [R78 單一比較軸 worker](../artifacts/benchmarks/workflow_speed_optimization_20261021/round78/inspection_worker.py)
- [R78 實際全量 protocol](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round78/protocol.json)
- [R78 原 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r78-write-time-sha-full-20261024T013433-ce568abe/run.json)

截至 **01:55 UTC**，原 R78 完成前三次，全六輸出的語意 SHA 保持相同。
候選實際命中四份 CSV／JSONL 的已關閉 writer SHA，兩份 JSON 完整重讀；
各次 wall 波動仍大，未以單次結果決定採用。原 owner 持續執行，不另開重複比較。

R83／R84 已準備可審查的整合補丁：批次讀取併入既有 C++ module／source-ABI
cache，Python 保留順序、SHA、來源換代及 byte-budget 判定；超出 native ns
範圍的 timestamp 先回原 Python 路徑。原生 raw payload copies 上界為
`(2 * workers + 1) * (batch_bytes + 1)`，另有 path／物件等 metadata。
訓練 discovery 另納入有正整數間隔的 `epoch_curve_plot_timing_everyN.json`，
維持深度預算與原 parser；輸出 SHA 為獨立可選整合，須待 R78 雙順序通過。

R83 原檢查因新 helper 檔尾多一空行而失敗，原補丁／log 全部保留；R84 僅修正
該 whitespace，三種草稿均通過 AST 與非修改式 `git apply --check --whitespace=error`。
R84 owner succeeded／inactive／MainPID 0，命令 **2.872225 秒 wall**；這僅是
準備工作，未編譯、未執行整合測試、未改 canonical source，也未完成 13-phase。

- [R84 準備程式與 R83 來源 SHA](../artifacts/benchmarks/workflow_speed_optimization_20261021/round84/prepare_native_integration.py)
- [R84 全部變體、前後 SHA 與補丁檢查](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round84/preparation.json)
- [R84 原 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r84-prepare-native-inte-20261024T015414-fea818d1/run.json)

R78 後續於 **02:01:15 UTC failed／exit 1**，實查 inactive／MainPID 0；
完整原 command 為 **1,598.297679 秒 wall**。第四輪的獨立 reader 發現
`stockagent/private_json.py` 從 `15c4ace9…` 變為 `b569df09…`，依原 source pin
拒絕。前三輪有效，第四輪 collector 產生的輸出和 failed readback 全部保留，
未產生八輪 comparison，也沒有 SHA 候選速度決策。

R85 重新核對兩個版本 AST：每個既有非 docstring node 均保留，僅新增
`import math`、`_plain_finite_json`、`ledger_bytes`；collector 使用的
`loads`／`pretty_metadata_bytes` 未變，沒有呼叫新函數。保留這份外部重疊修改，
僅整合 R69 通過的 C++ 與動態訓練 clock discovery，**不採用寫出 SHA**。
236 項正式整合檢查通過；完整 13-phase 及獨立原版 oracle 接續驗收。

後續 R86 的共用 native walker／cache 28 項回歸亦全部通過。R85 full 在啟動前
遇到觀測對象 `signal_engine.py` 的重疊修改而拒絕，沒有執行 collector；
R87 保留所有來源擷取與 scope，改為依 execution／observed 角色檢查，
02:15:16 UTC 啟動完整 13 階段與原始 reader，02:45:19 UTC 成功結束，
實查 inactive／MainPID 0。14 outputs／6,554 unique originals／249,757 samples／
29,667 groups 全部讀回；2,399 個來源身分及原 1,651 個 bindings 集合無遺失。
整套驗收 1,797.671285 秒，其中 collector 626.471373 秒，canonical reader
186.633538 秒、重建 282.544137 秒、原版 reader 加來源／時鐘 oracle
698.783051 秒。兩種 reader 的實體布局及工作不同，不比較它們的倍率。
詳見 R85–R87 整合紀錄的完整計時表；748 個未綁定來源專屬計時與四筆
FinMind 缺時鐘紀錄仍保留。下一項輸出 SHA 候選尚未採用。

- [R78 原失敗讀回 log](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round78/full_trial_3_baseline.readback.log)
- [R85 來源漂移、AST 證明與不採 SHA 的決策](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round85/preparation.json)
- [R85 正式整合與完整驗收](workflow_speed_optimization_2026-10-24_native_batch.md#r85正式整合與完整驗收)
- [R87 全階段、復原及原版 reader 完整收據](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round87/full_acceptance.json)

R89 將實際 import 的程式與 C++ cache 封存後再比較輸出 SHA，避免 R78
受工作目錄後續變動中止。128-input smoke 的兩組完整六輸出與獨立讀回已
通過，02:55 UTC 已進入 372,550-input 全量八輪。兩組都採已整合的原生
batch，只改寫出時 SHA；兩半門檻維持 wall ≤ 0.98、CPU ≤ 1.02。尚無
速度採用結果。R88 準備階段 guard SHA 誤填的失敗亦保留，詳見
[R89 固定來源與目前進度](workflow_speed_optimization_2026-10-24_native_batch.md#r89凍結實際程式後重測輸出-sha)。

沿用 R28 已驗收的 `d3bf90fe…` profile，不改正式來源碼。保留全部 223 份
explicit receipts、四個 training roots、13 phases，使用既有
`--profile-benchmark-inspection`，把診斷明確列為第 14 份輸出。
collector 與兩個 reader 均成功，原 supervisor succeeded／inactive／MainPID 0。

14 outputs、6,823 unique originals、240,623 samples、27,611 groups 均完整
回讀，兩個 reader 除觀測時間外回傳欄位一致，29 份 measurement-source SHA
結束時均未改變。registry 2,399／stage timing 1,647／未綁定 752，原生 journal
clock、PID、invocation、spec、policy 由獨立 oracle 核對。來源 E2E 與既有
四筆 FinMind legacy clock 缺口持續保留。

| 完整階段 | wall 秒 | descendant CPU 秒 |
| --- | ---: | ---: |
| diagnostic collector | 397.748154 | 252.257757 |
| canonical reader | 89.329767 | 22.387304 |
| frozen original reader＋scope／native-clock oracle | 183.853945 | 40.670669 |

三階段與結束檢查 **671.223285 秒**。本次含 timer overhead、真實收據更新及
共享主機狀態變動，絕對秒數作診斷觀測；採用效能候選另需固定輸入比較。

## 個別成本與下一項實驗

inspection 整段 **260.159065 秒 wall／143.336840 秒 process CPU**，後者
包含 read worker，不能當成主執行緒 CPU。下面是主執行緒個別操作，沒有
加上包含它們的 `evidence_read_inclusive`／`classification_inclusive`：

| 主執行緒操作 | calls | wall 秒 | thread CPU 秒 |
| --- | ---: | ---: | ---: |
| snapshot exists | 18,367 | 53.959279 | 5.924818 |
| snapshot write | 3,702 | 18.513584 | 1.857865 |
| JSON decode | 341,104 | 14.702938 | 14.504688 |
| stat | 372,764 | 14.103191 | 12.379034 |
| metric tree | 19,169 | 12.151104 | 11.871116 |
| synchronous read/open/close | 242 | 5.223727 | 4.773780 |
| gzip compress | 3,702 | 3.535954 | 3.527420 |

read worker 的 bounded read 為 **93.800875 秒 wall／16.960804 秒 CPU**，
stat **20.796782／11.424051 秒**，批次 inclusive **144.800654／50.349769 秒**。
這些區間與主執行緒重疊，不能相加成全流程 wall。wall／CPU 差包含等待、
GIL 與排程，尚不能把差值全歸因於磁碟；gzip 或 JSON 的局部替換也不能獲得
整段倍數。此輪沒有證明硬體極限。

優先驗證 snapshot path-query 成本。R30 在原 R29 完整 terminal 驗收後
啟動唯讀機制比較：同一批 6,823 個已存在 snapshot、同一個 `Path.exists`
API，比較完整路徑與 pinned `/proc/self/fd` 父目錄，fresh-child ABBABAAB。
兩邊均保留共同的 namespace pin／前後檢查；whole child 包含來源與 protocol
驗證、路徑準備、全部查詢，loop clocks 另列。最後完整 canonical reader
再次核對十四檔與所有原始證據，無 payload copy 或來源修改。

本 probe 只測已存在檔案，不測首次建立、完整 inspection 或 artifact lifecycle
等價；即使兩半都過，也只允許進一步全流程測量，**不能直接採用到正式
collector**。八次與最後完整回讀均已結束，十四檔及 6,823 份原始證據再次
通過 SHA／彙整驗證，額外 reader **100.069461 秒**，原 owner 已 succeeded／
inactive／MainPID 0。兩半結果如下，保留全部慢樣本：

| 順序 | A whole child 均值 | B whole child 均值 | wall B/A | CPU B/A | 決策 |
| --- | ---: | ---: | ---: | ---: | --- |
| ABBA | 7.631369 s | 7.313908 s | 0.958401 | 0.959656 | 僅此半通過 |
| BAAB | 8.219988 s | 9.026214 s | 1.098081 | 1.070647 | 未通過 |

不進行正式整合，也不以原 54 秒的 snapshot-exists 診斷成本，推定 pinned
路徑能省下相同秒數。獨立 metadata probe 與含預讀／解析的 pipeline 範圍不同。

## R31：維持單執行緒的有界預讀佇列

下一個私有候選只在 `workers=1` 時將 pending batch 從 1 增至 4；
ThreadPool 仍為 1 個 worker，其他 worker 設定與正式預設沒有更改。增加
有限 lookahead，測試能否在主執行緒處理 evidence 時預先完成更多讀取。
不是沿用前輪已拒絕的 2／4 worker 方案。

候選 baseline 為已驗收的 `d3bf90fe…`，candidate 為 `f5df22fd…`，一個
submit-loop 修改與兩段資源註解完全可逆。候選加正在消費的一批最多
**5 × (256 MiB＋1 byte)＝1,342,177,285 bytes** raw buffers，保留 generation
重查、順序、byte budget、完整 SHA、例外與取消後的 worker 關閉。
相關既有語意測試加選定佇列生命週期檢查 **21 passed、0 skipped**，pytest
**4.056865 秒**；正式 profile 保持 `d3bf90fe…`。

完整比較已實際 running，owner 為
`workflow-r31-complete-single-que-20261023T210511-23a13570`。
重用封存的 **372,550 inputs／107 explicit receipts／9,383,953,790 bytes**，
plan SHA `f4bb9b16c7d423b7dbb85bc1f1c61cb5e990596de2c2fe881672572b0959044d`；
沒有建立新 payload copy。8 個 fresh children 採 ABBABAAB，兩半各要求
whole-child wall ≤ 0.98、descendant CPU ≤ 1.02。每輪含完整讀取與輸入 SHA、
六份完整輸出、每份原始證據及另一程序的全檔讀回；actual input provenance
欄位須完全相同，只排除不同的實作來源封套。

每輪前重新觀察 node／memory／D free；memory headroom 至少 24 GiB，D
保留 32 GiB、root 8 GiB，準備時 forecast D 10 GiB。新 command capsule
只補上 per-trial memory admission，原準備版與全部測試來源保留原樣；
不足延期，沒有刪資料、provider dispatch、GPU job 或正式服務重啟。
本比較驗收範圍是完整 inspection 子流程，通過後仍需 canonical integration
及新的全部 13 階段驗收，尚無採用或全流程速度結論。

R32 在此比較存活期間，唯讀取得既有 TW-public 服務的 20 次新原生操作；
[原生時鐘與 delta 驗收](workflow_speed_optimization_2026-10-24_native_tw_public_clocks.md)
保留完整 2,399 個既有身分並確認四個原缺口的新操作時鐘。這批原始 journal
需加入下一次完整 13 階段與獨立來源 oracle 的驗收；目前全量覆蓋維持 R29
的 1,647／752，沒有用 delta 結果代替全量重新計算。

## R34：減少 snapshot 小檔操作的隔離封裝候選

依 R29 的 exists／write 成本，先實作單次 collection 專用的封裝草稿。
重用現行 `stockagent.data_sync.packed_snapshots._SeekablePackBuffer`，將每份
原始內容依完整 SHA 去重，gzip bytes 以 ZIP_STORED 加入新建、獨占持有的
archive，避免每次加入內容都查詢目錄。沒有改正式 collector、既有 gzip
原始證據或 shared packed-store 實作；草稿沒有進入資料發布或清理流程。

封裝格式明列 `workflow_snapshot_pack_draft_v1`。輸入仍核對完整 SHA，raw
member 上限預設 256 MiB、最多 32,768 members，8 MiB 共用寫入緩衝；不符
admission 直接拒絕。關閉檔案與獨立還原為分開的收據，不宣稱 fsync、冷還原
或正式來源保存。例外與失敗寫入保留 staging，不產生完成收據。

**28 passed、0 skipped**，完整 pytest 子命令 **5.244696 秒 wall／3.210121 秒
descendant CPU**。涵蓋既有緩衝的 ZIP bytes／CRC／partial-write 回歸，以及
草稿的 raw byte／SHA 還原、重複內容、空值／原始 NaN 字串／負零 bytes、
資源上限、既存路徑及 symlink 拒絕、損壞與失敗暫存保留。

原 v1 使用工作樹內 pytest 暫存，既有 cold-store 保護規則使一項回歸拒絕，
保留 27 passed／1 failed 的完整證據。v2 僅將 scratch 改到同一 root filesystem
的 `/var/tmp` 唯一目錄；原草稿、shared buffer 與測試未變，可逆衍生與來源 SHA
已封存，原失敗 run 由後續成功 run 追加 superseded 處置。

這完成隔離格式與生命週期檢查，尚無封裝速度或正式採用結論。等原 R31
全八輪及終結驗收後，才可在新 artifact root／明確 storage contract 下啟動
完整固定輸入比較；保留全六份報表、每份 raw SHA、獨立 ZIP／gzip 還原與
原來源身分，通過後仍需新的完整 13 階段驗收。

- [R34 格式、admission 與封存來源](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round34_v2/protocol.json)
- [28 項全部通過、時間與完整來源前後 SHA](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round34_v2/checks.json)
- [僅更正 scratch 的可逆 v2 衍生](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round34_v2/derivation.json)
- [保留的 v1 失敗證據](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round34/checks.json)

## R36／R37：完整 collector 的私有 snapshot adapter

依第一性原理，先減少已量到的跨檔案 metadata 往返；讀取、SHA、分類、
數值與報表算法保持原樣。私有 profile 只將 snapshot destination 接到
獨占的 pack writer，沿用 canonical `gzip.compress(raw, compresslevel=3,
mtime=0)` 的完整壓縮 bytes，再以 ZIP_STORED 封裝。預設仍是 individual
gzip，正式 `scripts/profile_workflow_speeds.py` 維持 `d3bf90fe…`。

格式明列 `workflow_snapshot_pack_v1`：manifest 保留原來每筆 source row、
raw SHA、大小及 logical snapshot 名稱，另加 archive SHA／大小／member
清單。讀回 pin 同一個不可變 archive generation，逐份檢查 ZIP CRC、gzip、
raw SHA 與大小；同時涵蓋 native build receipt 和 known-source inventory
的 JSON 讀取。writer 例外時關閉 descriptor、保留未完成內容，沒有 fsync、
正式發布或冷還原完成的宣稱。shared packed-store 與原有 reader 沒有修改。

首次 R36 檢查為 **212 passed／2 failed／0 skipped**；失敗明確指出中途
重讀需要標準 ZIP 的 `seekable()` 介面。R37 保留 R36 所有來源與結果，僅在
新的可逆來源 cohort 補上此方法，profile candidate SHA 仍為
`37089f7316f4d2acd633147943448685294975a9ebfe5cd1a90b44443f2f4f5a`。
重新檢查 **214 passed／0 skipped**，pytest wall **16.454874 秒**，pytest
process CPU **6.045987 秒**、descendant CPU **0.598152 秒**。這些是工程
檢查成本，不是封裝加速比例。

檢查包含中途重讀後繼續寫入、重複原始內容、canonical gzip bytes、五份
payload bytes 與所有來源 row 相等、byte budget／分類／失敗狀態，以及
archive 替換、同大小改寫並還原 mtime、symlink／FIFO、CRC／raw SHA 損壞、
多餘／重複 member、錯誤 mode／compression、部分讀取與解壓上限拒絕。
舊 R36 失敗 run 已由成功的 R37 run 追加 superseded 紀錄，沒有覆寫原證據。

截至 **2026-10-24 06:13 台北時間**，原 R31 已完成 6／8 輪且所有六份輸出
及原始 SHA 相同。第一半 wall B/A **0.985913136**、CPU B/A **1.022035801**，
未達預設 wall ≤ 0.98、CPU ≤ 1.02，仍保留原 owner 跑完全部八輪再定案。

R37 的單一續接 owner
`workflow-r37-serial-pack-compare-20261023T221328-57d0e8a3` 已實際啟動，
目前只觀察前置狀態。必須等 R31 succeeded／inactive／PID 0、八輪完整
比較與終結收據、舊 queue candidate 被拒絕，才執行新比較；若舊候選通過，
則需明確整合與重定 baseline。等候時間另列，不列入 payload clocks。

新比較固定同一批 **372,550 inputs／107 explicit receipts**，兩邊都是
1 read worker／1 pending batch。八個 fresh children 採 ABBABAAB，各半
仍要求 whole-child wall ≤ 0.98、descendant CPU ≤ 1.02；每輪保留完整六份
輸出與另一程序的所有 raw SHA 讀回。除了不同 profile envelope 與明列的
physical-storage descriptor，所有輸入來源與 provenance 欄位必須相等；
五份共同實作來源也逐份封存與比對。每輪重做 memory／D capacity admission，
不足延期。檢查與來源封存可先完成，完整 I/O 比較維持串行。

目前沒有封裝速度、正式採用或計算極限結論。通過這個完整 inspection
子流程後，仍需 canonical integration、新的全部 13 階段與原語意 oracle；
同時納入 R32 原始 journal，保留全部既有來源身分重新計算全量覆蓋。

- [R36 首次檢查與完整失敗證據](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round36/checks.json)
- [R37 可逆修補、原來源 SHA 與未採用邊界](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round37/repair_derivation.json)
- [214 項語意與生命週期檢查](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round37/checks.json)
- [完整固定輸入比較的來源與可逆衍生](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round37/comparison_preparation.json)
- [串行前置條件、等待與資源門檻](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round37/continuation_protocol.json)
- [R37 唯一續接 owner](../artifacts/operations/agent-workflow/runs/workflow-r37-serial-pack-compare-20261023T221328-57d0e8a3/run.json)

## R38：用未修改的原版 verifier 檢查實體還原

封裝候選增加了 storage ABI，因此另準備私有驗證用的實體還原：獨立以標準
ZIP 讀取器檢查完整 archive SHA、member inventory、mode、大小與 CRC，
將原 gzip member 和所有 top-level 報表 bytes 原樣寫入新的私有目錄。連
`collection.json` 與 `evidence_manifest.json` 都沒有改寫；凍結的原版 reader
直接讀取還原後的實體檔案，不需要修改其函式、gzip 或 Path 存取介面。

**17 passed／0 skipped**，完整 pytest 子命令 **32.266865 秒 wall／12.063837
秒 descendant CPU**。包含原版與候選 reader 的全部語意欄位相等、原始
NaN／負零／Unicode gzip bytes、重複來源、known-source 身分、native build
receipt，及 raw SHA 損壞、錯誤／重複 ZIP member、CRC、FIFO／symlink、
容量 admission、同大小改寫並還原 mtime、檔案替換的拒絕。來源前後 SHA
保持不變，失敗的還原目錄保留。這是小型 fixture 的正確性證據，尚未對
完整 372,550 輸入執行原版 verifier，也不構成冷發布或效能採用。

完整輸入驗證命令已實作，必須先通過原 R37 八輪速度門檻及終結收據：

```bash
source scripts/runtime_env.sh
stockagent-agent run --name workflow-r38-real-original-oracle \
  workflow-speed-baseline-20261021 -- python \
  artifacts/benchmarks/workflow_speed_optimization_20261021/round38/verify_real_pack_with_original.py
```

該命令會先核對原 owner succeeded／inactive／PID 0，重新進行 D 32 GiB／
root 8 GiB reserve、24 GiB memory headroom admission，再還原已完成的第一個
candidate trial，於另一程序執行原版 reader。還原及 original-reader 的完整
wall／CPU 分別記錄，不回填或更改原八輪比較時鐘。此步通過後，仍要進行
canonical integration 和包含 R32 journal 的完整 13 階段驗收。

- [R38 資源 admission、來源與未修改的原版指紋](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round38/protocol.json)
- [17 項實體還原與獨立原版 reader 檢查](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round38/checks.json)
- [完整輸入還原與另一程序原版 readback 命令](../artifacts/benchmarks/workflow_speed_optimization_20261021/round38/verify_real_pack_with_original.py)

## R40：實際 D／9p 的封裝與還原整合檢查

R38 fixture 在 root filesystem 執行，因此另在實際 guarded D／9p 上跑小型
整合，確認跨檔案系統行為。獨占 pack 的中途重讀、加入重複來源後繼續寫入、
seal、sealed reread、實體 individual-gzip 還原與另一程序的原版／候選 reader
都成功，六份輸出與兩份 unique originals 的驗證欄位相同。

命令 wall **5.961944 秒**、parent process CPU **0.394946 秒**、descendant CPU
**1.336893 秒**。這是小型真實掛載點的整合證據，不是完整輸入速度樣本；
正式 profile 與全部凍結來源未改，沒有把這個時間混入 R31 或 R37 的比較。

- [D／9p 實作、來源與資源 admission](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round40/protocol.json)
- [獨立程序原版／候選 reader 相同結果](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round40/readback.json)
- [完整命令、SHA 與時間](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round40/completion.json)

## R41：原 R31 八輪拒絕，封裝比較補齊 protocol metadata 後接續

R31 於 **22:34 UTC** 正常終結，原 owner succeeded／inactive／PID 0。八輪均
保留六份完整輸出、3,740 unique originals，以及 372,550 個固定輸入身分。
各輪 semantic proof 完全相同，來源 SHA 與另程序的原始內容讀回均成功。

| 原 queue 比較 | wall B/A | descendant CPU B/A | 原門檻結果 |
| --- | ---: | ---: | --- |
| ABBA | 0.985913136 | 1.022035801 | 不通過 |
| BAAB | 0.979523478 | 0.997477853 | 通過 |

兩半都通過才採用，故 queue4 **正式拒絕**，canonical 仍為 `d3bf90fe…`，
沒有挑選較好的第二半或放寬 CPU 門檻。原比較完整 controller-child wall
為 **5,353.985591 秒**；這包含全部八輪與 parent 的語意彙整工作，不能當成
單次 inspection 時間。

R37 續接在建立 comparison protocol 時，因缺少預期的 `location.json`
而退出，**當時零個 timed trial、沒有 payload trial 目錄**。R37 原 prepare
已寫有內容完整的 `comparison_location.json`，但舊 comparator 讀取另一個
固定名稱。保留原 failed run、原 comparator／worker／candidate bytes，不
重跑已完成的 R31。R41 以新 source capsule 將同一份 metadata 完整 bytes
補到缺少的名稱，兩者 SHA 都是
`f2cf2e5acd72e738a71f9e26e3118c70c5fbf394467ed155a91e66a866b67412`。
R41 亦獨立重算 R31 的兩半與全部八輪 readback／semantic proofs。

唯一完整封裝比較 owner 改為
`workflow-r41-pack-metadata-resum-20261023T223640-d2c78a50`，**22:36 UTC 已
實際啟動第一個 full baseline child**。沿用未修改的 R37 comparator 和
worker；固定輸入、ABBABAAB、wall／CPU 門檻與全部讀回都維持原定條件。
原 R37 failed run 需等這個新 owner 完整成功後，才追加 superseded 處置，
不能以新程序 running 當作修復完成。

若新比較通過，R38 完整實體還原命令的 owner gate 已更新為 R41，並要求
completion 綁定此 metadata amendment SHA。仍需該完整原版 oracle 與後續
13-phase canonical integration，尚無正式封裝採用或全流程加速結論。

- [R31 全八輪正式拒絕與兩半完整時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round31/comparison.json)
- [R31 原 owner 終結收據](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round31/command_completion.json)
- [R41 metadata 補齊、原失敗與新 owner 身分](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round41/resume_protocol.json)
- [R31 全八輪 readback／semantics 獨立重算](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round41/prior_queue_readback.json)
- [R41 唯一完整比較 owner](../artifacts/operations/agent-workflow/runs/workflow-r41-pack-metadata-resum-20261023T223640-d2c78a50/run.json)

## R43：完整比較通過後，沿原流程自動接續獨立原始驗證器

新增 serial continuation owner
`workflow-r43-serial-original-ora-20261023T224529-d053cf56`。它只等待原 R41
實際終結：canonical run、systemd inactive／MainPID 0、八輪完整 readback、
comparison SHA 和候選採用結果都通過，才執行已準備的 R38 真實語料
實體還原與 untouched original reader。比較失敗、候選拒絕或容量不足，
則保留原因並結束，不會重跑或覆蓋既有 trial。

等待程序每 60 秒讀取原 owner 狀態，最多六小時；等待期間不啟動另一組
測速。真正還原前再次要求 24 GiB memory headroom、D 保留 32 GiB 加
還原上限，以及 root 保留 8 GiB；R38 仍另行精算實際輸出需求。
R43 自身、原始驗證 driver 與還原器均已封存來源 SHA。封裝比較、正式
collector 與 GPU owner 的來源／設定未變。

目前 R43 已驗證為 running／實際 PID 存在，log 為等待 R41；這只證明
續接機制已就緒，尚未證明真實語料原始驗證器通過。即使 R38 完成，
正式整合及含 R32 原生 journal 的完整 13-phase workflow 仍需另外驗收。

- [R43 等待條件、來源封存與原 owner 身分](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round43/protocol.json)
- [R43 續接程序收據](../artifacts/operations/agent-workflow/runs/workflow-r43-serial-original-ora-20261023T224529-d053cf56/run.json)

## R41 完整子流程觀測：五輪已讀回，尚未採用

截至 **2026-10-23 23:18 UTC**，原 R41 實際仍為 running／PID 存在，
完成前五輪，第六輪沿同一 owner 執行。

| trial | 完整 child wall | 完整 child CPU |
| --- | ---: | ---: |
| 1：individual gzip baseline | 659.960202 s | 231.647454 s |
| 2：packed gzip candidate | 323.234345 s | 189.939629 s |
| 3：packed gzip candidate | 303.416657 s | 180.322731 s |
| 4：individual gzip baseline | 357.349890 s | 178.607920 s |
| 5：packed gzip candidate | 329.555088 s | 189.139481 s |

R47 小型驗證獨立重讀五輪 outer SHA、semantic proofs 與 readback，五輪
proof 完全相同：
**372,550 個固定 selected inputs、六份輸出、3,745 份 unique originals、
2,006 samples、722 summary groups** 均通過。這是包含 collector 及獨立
原始證據讀回的五個 child 時間，沒有將它當成八輪穩定結果、完整 controller
wall、全 13-phase workflow 加速或正式採用。仍需兩半 ABBA／BAAB 及
R43 接續的 untouched original oracle；所有現行 source pins 保持不變。

- [原 owner 的逐輪進度與首組時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round37/comparison_progress.json)
- [第一輪原始證據與固定 input 身分讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round37/trial_1_baseline.readback.json)
- [第二輪原始證據與固定 input 身分讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round37/trial_2_candidate.readback.json)

## R47：正式接線補丁、完整範圍與獨立原版驗收準備

正式接線已產生可審查補丁並通過 `git apply --check --whitespace=error`，
尚未修改 root source。三份來源是 collector、共用
`stockagent/workflow_snapshot_pack.py` 與維護測試；backend bytes 與 R37
實測版完全相同。預設保留 individual gzip，選定的完整驗收明確使用
`packed_gzip_v1`，同時捕捉封裝模組及三份 shared packed-store 依賴的 SHA。

完整命令保留 R29 的全部 argv 前綴，僅更換 output root：原本四個 training
roots 是 markets、ablations、operations、**benchmarks**，全部保留後再加入
R45 遠端原生時鐘，合計五個。原 223 份 explicit receipts 保留後增加到
231 份；原 R23 journal 保留，再加入 R32 與 R21，合計三份。inspection
profiling、全部 13 phases、14 outputs、2,399 個已知來源身分與完整來源
覆蓋重新計算，都沒有縮減。

新驗收程式先由 canonical reader 讀回，再以 R38 未修改的實體還原器產生
新的 individual-gzip view，交給 D22 未修改的 original reader。兩邊完整
回傳欄位須相等；另核對既有 1,647 個 stage bindings 不流失、三份 journal
的 clock／PID／thread／invocation／spec／policy，以及四份遠端訓練時鐘
的完整 226 samples。還原、讀回和 collector 各自記錄 wall／CPU，容量
不足延期；這些時鐘不混入原 R41 比較。

準備階段僅執行小型欄位驗證：已封存訓練 delta 的實體 gzip 查找及原版
parser 226 samples 一致；三份 journal 的 retained bytes 在記憶體 fixture
中核對 21 次操作，正確排除一份相同事件的重複匯出。這不是新完整實體
還原或正式維護測試的完成證明。review 命令 **3.458684 秒 wall／
1.375318 秒 descendant CPU**，owner 已 succeeded／inactive／MainPID 0；
runtime、original oracle 與 review 來源均已封存，後續不得修改這個 cohort。

正式套用前仍要求 R41 八輪兩半都通過、原 owner 真正終結，且 R43 完整
原版 oracle 成功並終結。之後才套用補丁、執行至少 214 項維護檢查與新
完整 workflow。尚無正式封裝採用、全 13-phase 固定輸入加速或硬體極限
結論；沒有新 provider／GPU 工作或服務重啟。

- [三份待接線來源、依賴 SHA 與採用條件](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round47/preparation.json)
- [原四個 training roots、完整收據與 journals 範圍](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round47/full_scope_contract.json)
- [補丁套用預檢與範圍前綴一致證據](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round47/patch_check.json)
- [226 筆訓練與 21 次原生操作的欄位驗證](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round47/runtime_review.json)
- [採用 gate、正式檢查與完整執行程式](../artifacts/benchmarks/workflow_speed_optimization_20261021/round47/run_canonical_pack.py)
- [實體還原及未修改 original reader 驗收程式](../artifacts/benchmarks/workflow_speed_optimization_20261021/round47/verify_full_pack.py)
- [小型欄位驗證原 owner](../artifacts/operations/agent-workflow/runs/workflow-r47-runtime-clock-revie-20261023T231849-51d3455c/run.json)

## R52：通過實際驗收後序列接續正式整合

R52 已實際啟動，原 owner 為
`workflow-r52-serial-canonical-pa-20261023T232746-ad6ae9e2`，目前 log 為
等待 R41／R43，實際 systemd active／PID 存在。它不啟動另一組 comparison
或 original oracle；每 60 秒讀取兩個既有 owner，最多等待六小時。

只有 R41 八輪候選通過、R43 真實原版 oracle 完成，且兩個 canonical runs
都 succeeded／inactive／MainPID 0，才會執行 R47 gate。套用前再核對全部
source pins、已審查的 patch SHA、固定 `apply_patch` 工具 SHA，以及 memory
headroom 24 GiB、D reserve 32 GiB 加 8 GiB forecast、root reserve 8 GiB
加 256 MiB scratch。任何拒絕、前置失敗或容量不足均留下原因，不採用。

通過後僅套用 R47 那三份確切來源，逐檔確認 after SHA，再依原 R47 程式
跑維護檢查、完整 13 phases、canonical reader、實體還原與 original oracle。
R47 runtime 與本續接來源均已封存；不要同時手動執行另一個 gate、套用或
完整 collector。服務不重啟、不刪資料、不啟動 GPU／provider 工作。

R51 原版在空補丁自檢時將退出碼誤設為 0 而停止；工具實際對空補丁
回傳 1 並明列 `No files were modified.`。R52 只更正這個自檢，要求退出碼
1 且訊息相同；**正式來源 patch 及正式測試仍必須退出碼 0**。
原 R51 失敗、未更改的三份 root source 與全部收據保留，不以新 owner
running 提前判定整個整合成功。R51 superseded 處置須等 R52 真正完成。

截至 **23:44 UTC**，R41 全部八輪完成，canonical receipt succeeded、
實際 unit inactive、MainPID 0。完整固定輸入的 ABBA baseline／candidate
平均為 **508.655046／313.325501 秒 wall**、205.127687／185.131180 秒 CPU；
BAAB 為 **478.820978／312.913986 秒 wall**、198.254275／185.555619 秒 CPU。
wall 比值 **0.615988／0.653509**、CPU 比值 **0.902517／0.935948**，兩半
均通過原判準；八輪六份輸出、3,745 份原始檔、2,006 samples、722 groups、
372,550 selected paths 語意一致。R37 原 metadata 失敗已由真正完成的 R41
superseded，原失敗仍保留。

這是該固定收據工作與讀回範圍的結果，尚未代表新完整 13-phase workflow
加速。R43 隨後通過真實實體還原與未修改 original reader，兩段分別
**104.741498／135.022664 秒 wall**；original reader descendant CPU 為
20.528628 秒。R43 canonical receipt succeeded，實際 inactive、MainPID 0。
R43 完整命令的 3,745.532518 秒還包含等待 R41，不當作還原成本。

R52 在 R43 實際終結後，才套用三份確切來源並核對 SHA，正式 profile
成為 `f069386346c4fea2e0b9556bf17340e061450a5967b2b2620fb1ba470ae638a9`。
**214 項維護測試全部通過、零失敗、零 skip**，完整測試子命令
8.814583 秒 wall／6.933022 秒 descendant CPU。封裝是明確 opt-in；CLI
預設仍為 individual gzip。新完整 13-phase collector 已開始，之後依序跑
canonical reader、實體還原和原版 oracle；截至 23:49 UTC 尚未完成整段
驗收，沒有以來源採用或測試通過代替完整成果。

- [八輪完整結果與兩種順序判準](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round37/comparison.json)
- [R41 實際完整命令完成收據](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round37/command_completion.json)
- [獨立真實資料還原與原版 reader 結果](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round38/full_trial_2_candidate_completion.json)
- [R43 正式序列驗收完成](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round43/completion.json)
- [正式來源採用與相容性範圍](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round47/canonical_adoption.json)
- [214 項維護測試與確切時間](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round47/checks.json)
- [另批 953 個 crypto workers 的 14,366 筆原生計時與排程下界](workflow_speed_optimization_2026-10-24_crypto_feature_clocks.md)

- [R52 來源封存、兩個前置 owner 與完整採用條件](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round52/protocol.json)
- [空補丁的預期退出碼與實際工具時間](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round52/patch_tool_empty_input_check_command.json)
- [R52 唯一正式整合續接 owner](../artifacts/operations/agent-workflow/runs/workflow-r52-serial-canonical-pa-20261023T232746-ad6ae9e2/run.json)
- [保留的 R51 自檢失敗及 root source 狀態](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round51/failure.json)

## 完整收集結果與 R58 來源版本銜接

R47 完整 collector 已正常結束：**13 phases、14 outputs、6,825 份原始檔、
240,949 samples、27,743 summary groups**。canonical reader 已完整驗證相同
數量。collector 完整子命令 **486.337626 秒 wall／337.358422 秒 descendant
CPU**，canonical reader **123.908884 秒 wall／29.097587 秒 descendant CPU**。
原四個 training roots 加固定 R45 root、三份 journals 與完整 explicit
receipts 都保留；不是八輪控制比較，不能由此宣稱全 13-phase 提速。

本批 inspection 為 **302.480339 秒 wall／196.060722 秒 process CPU**。
封裝的 `snapshot_exists` 18,351 次合計 **0.088281 秒 wall／0.078250 秒
thread CPU**，`snapshot_write` 3,695 次為 4.273480／0.849923 秒。
新的主要區間是 worker `bounded_read` 164.986169／31.144985 秒、worker
stat 44.321431／22.912367 秒，及 main classification inclusive
58.670243／57.860936 秒。classification 已包含 JSON decode 與 metric
walk；worker 與 main 也重疊，不可加成整體 wall 或解讀成純 I/O。

23:50 UTC 另一項工作修改了 `scripts/agent_workflow.py`，新增經驗證的
`WSL_DISTRO_NAME` 環境傳遞。collector 捕捉的是原 launcher
`48d51512e71e03446a34845ca7585792db266e0a28ca6fd3cf9977560ea6ca26`，
目前 launcher 為
`1ebec9a974a1351a29320b686993a71b3b74dd7b02e9a197e1d03570c64d5268`。
33 份 measurement sources 只有這一份變更，三份已採用封裝來源保持原 SHA。

R52 因原版 oracle 前置的固定來源檢查而退出，原 failed 收據、log 和來源
全部保留，沒有還原別項修改。R57 因此前置失敗退出等待，**未執行路徑
診斷**；其成功退出只表示有正常記錄「未執行」。

R58 使用新的 `workflow_historical_launcher_source_context_v2` 證據目錄，
明列 captured/current launcher 的兩個 SHA 及完整差異，只對這個已知來源
角色建立版本銜接。封存的 33 份來源、原始收集與 canonical reader 均不重寫；
D22 original reader bytes 也不改。其他來源仍要求 SHA 相同，歷史 launcher
仍要求原 SHA，目前 launcher 仍要求新 SHA。R58 僅接續實體還原和原版
oracle，驗證歷史這批完整觀測，不宣稱已驗證新 launcher 執行整個流程。
R58 完成 6,825 份原始 gzip 與 15 份 report 檔還原，共 924,238,298 bytes。
但其子程序使用模組的原 `__file__`，仍進入 R47 舊檢查，因而在讀回前
退出。R58 原 source-context 與已還原資料保留；158.298996 秒完整命令包含
還原及失敗子程序，還原本身的精確 monotonic 區間未留存，不重建或猜測。

R60 只呼叫 R58 已封存的 validation modules 的 original-reader 函式，使用
已還原的那一份 view，在新目錄保存結果；不再次還原、收集或跑 canonical
reader。R58 的 source-context 政策原樣保留，D22 原版 reader bytes 不變。
R60 已在 **00:12 UTC** 正常完成，canonical receipt succeeded、實際 unit
inactive、MainPID 0。原版 reader **418.391276 秒 wall／69.776791 秒 process
CPU**；完整命令 420.812251 秒 wall。全 14 outputs、6,825 originals、
240,949 samples、27,743 groups、13 phases 皆與 canonical reader 相等。
這是歷史這批完整觀測的接受結果，仍不宣稱新 launcher 已跑完整流程。

來源 oracle 核對 2,399 個 registry identities，原 **1,647** 個 stage bindings
全數保留，成為 **1,651**，尚有 **748** 個未綁定。三份 journals 共 21 個
不同原生操作，一份重複匯出不重算，clock／PID／thread／invocation／spec／
policy／來源欄位全部相等。固定 D45 的四份訓練時鐘、127 native records、
226 samples 與未修改 original parser 完全相等。來源端到端和正式遠端
case／fold 完成仍未由此證明。

R51 自檢、R52 固定來源及 R58 子程序入口的三份失敗已由真正完成的 R60
superseded，原 failed 收據、source context 及 logs 保留；沒有把它們改為
成功或覆蓋資料，也沒有重做 collector、canonical reader 或實體還原。

R59 隨 R58 前置失敗正常記錄未執行；目前唯一後續路徑診斷 owner 為
R61，等待 R60 真正成功並 inactive／PID 0，才依原 depth、directory budget、
排除規則完整探索並分析 canonical 篩選函式。保留所有 candidate／selected
paths 與完整 profiler，另記 wall／CPU；不重跑 collector、不改來源，
profiler inclusive 區間不當作加速結果。各版原來源與前置更動均已封存。

R61 已在 **00:14 UTC** 完成，receipt succeeded、實際 inactive、MainPID 0。
完整探索 **372,227** 個 candidates，零 traversal gaps；全部通過 canonical
路徑篩選，candidate／selected JSONL 與完整 pstats 都保留。探索為
**29.143707 秒 wall／24.430252 秒 process CPU**，instrumented selection 為
**20.536559／19.525436 秒**。本次與 R47 的機器負載和 instrumentation
不同，不能把 29.493996 → 20.536559 當成新加速。

| 篩選函式的 profiler self 項目 | 呼叫次數 | profiler 秒 |
| --- | ---: | ---: |
| `posix.lstat` | 751,825 | 4.511502 |
| `posix.stat` | 751,825 | 4.197794 |
| `resolved_directory` | 749,583 total | 2.384209 |
| `select_benchmark_paths` | 1 | 2.155453 |
| `posixpath.join` | 749,583 | 1.356826 |
| `posixpath.dirname` | 751,825 | 1.259238 |

profiler 會加入開銷，以上不是純 CPU 或儲存頻寬下界。結果支持先處理完整
inspection 的批次讀取與分類解析；路徑篩選保持 generation／symlink fences，
不為省呼叫而放棄檢查。下一個 native bulk-read 或分類候選需保留同一批
原始 bytes、SHA、短讀／變動／大小限制與 ordered consumption，再做固定
完整輸入對照；目前沒有宣稱該候選已完成或已加速。

- [完整 collector 的命令時間](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round47/collector_command.json)
- [完整 canonical reader 與輸出數量](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round47/canonical_readback.json)
- [canonical reader 命令時間](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round47/canonical_readback_command.json)
- [新完整 inspection 的原生分項](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round47/collection_full/inspection_profile.json)
- [R52 保留的固定來源失敗](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round52/failure.json)
- [R58 明確來源版本契約](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round58/source_context.json)
- [R58 歷史快照驗證程式](../artifacts/benchmarks/workflow_speed_optimization_20261021/round58/verify_full_pack.py)
- [R58 已完成的真實資料還原](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round58/full_reconstruction.json)
- [R60 沿用已還原資料、只讀回的明確範圍](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round60/protocol.json)
- [R60 原版 reader 接續程式](../artifacts/benchmarks/workflow_speed_optimization_20261021/round60/read_retained_original.py)
- [R60 完整原版讀回與來源時鐘 oracle](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round60/untouched_original_readback.json)
- [R60 歷史完整流程接受範圍與確切時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round60/completion.json)
- [R57 未執行的實際結果](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round57/completion.json)
- [R59 序列診斷原 owner](../artifacts/operations/agent-workflow/runs/workflow-r59-source-context-path-20261024T000130-fbdaa529/run.json)
- [R61 目前唯一序列診斷 owner](../artifacts/operations/agent-workflow/runs/workflow-r61-retained-original-p-20261024T000609-ea364841/run.json)
- [R61 完整路徑診斷、原始 path-list SHA 與全部 profiler 來源](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round61/completion.json)

- [R29 完整驗收與來源 SHA](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round29/acceptance.json)
- [per-thread 原始操作成本與重疊範圍](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round29/collection_full/inspection_profile.json)
- [保留全部範圍與十四份輸出的 protocol](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round29/protocol.json)
- [原 R29 owner](../artifacts/operations/agent-workflow/runs/workflow-r29-inspection-continua-20261023T203155-a0c24bbd/run.json)
- [R30 唯讀比較 protocol](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round30/protocol.json)
- [原 R30 owner](../artifacts/operations/agent-workflow/runs/workflow-r30-snapshot-path-probe-20261023T205241-dd740a6a/run.json)
- [R30 完整結果與負面決策](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round30/result.json)
- [R31 可逆候選、完整固定輸入與來源封存](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round31/preparation.json)
- [R31 選定佇列的 21 項語意檢查](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round31/checks.json)
- [R31 per-trial memory admission command capsule](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round31/command_capsule.json)
- [R31 原 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r31-complete-single-que-20261023T210511-23a13570/run.json)

## R134–R138：同 bytes 的正向 metric-tree 快取候選

R116 inspection 已採已驗收的 C++ 批次讀取；原 profile 顯示 main-thread
metric-tree 18,221 次、22.531716 秒 wall／21.666728 秒 thread CPU，
JSON decode 340,121 次、21.108438／20.295577 秒。兩者與 read worker
重疊，不能把秒數直接相加成可節省的完整 wall。

R134 只讀 R116 已驗收的完整 `benchmark_receipts.csv`，穩定 FD 核對
**120,425,440 bytes** 及原 SHA。371,512 個 catalog rows 有 17,483 次
正向 metric 檢查，對應 3,346 個唯一 bytes SHA；**14,137 次重複檢查、
537,810 個重複 metric rows**，全體正向 rows 共 708,132。這是靜態工作量
診斷，不證明全部可從快取命中或建立任何加速上界。

R135 私有候選在原 `Evidence` 內加入 **64 MiB／4,096 entries** 的 LRU。
以完整 bytes SHA 為鍵，保留不可變的 metric scalar tuples，回傳時建立
各路徑獨立 dict；backend／descriptor／walker context 改變即清除。
尺寸保守計入 key、tuple、每個值與 metadata 開銷，超限走原路徑。
原始讀取／完整 SHA、JSON parse／完整 payload、順序、分類、負結果 cache、
證據預算、snapshot、逐路徑 metadata 與全部 reader 均保留；沒有快取大型
原始 payload，也沒有略過 JSON 有效性檢查。

候選只變更 `Evidence` 與 `capture_benchmark`，另加私有 cache class／呼叫
helper；其他既有 function／class AST 不變。已通過 **522 tests，零 failure／
error／skip**：保留原 507 項並加 15 項 bytes／payload／signed-zero／大整數、
路徑互不汙染、JSON 重新解析、容量 bypass／eviction、backend 更換、18,000
筆完整 ordered metrics 與順序預讀案例。檢查 46.203550 秒 wall／39.312555
秒 descendant CPU，owner succeeded／exit 0、實查 inactive／MainPID 0。
候選 SHA 為 `b2c66a41fa3f63bd20e2c7eaf137be6962ebdb31f2cd768b3f132779ae3dd093`。
共用 profile 仍為 `879e8457…`；測試通過不是採用證明。

R136 `workflow-r136-frozen-metric-cach-20261024T101750-725c3659` 已在
R123 失敗後拒絕前置 owner，兩者 failed／exit 1、inactive／PID 0。
沒有建立 benchmark protocol 或執行比較；原來源及失敗保留。
R123 原收集及第一個 reader 通過，失敗是 collector 清單未包含包裝器額外
要求的 IO source；R137 以原 archive／原 code manifest 精確補充，接續原
oracle，詳見[完整整合接續範圍](workflow_speed_optimization_2026-10-24_crypto_feature_clocks.md)。

R138 `workflow-r138-frozen-metric-cach-20261024T104221-7d401af1` 原已排定
等待 R137 與 R135 實際成功／inactive／PID 0，再建立同一來源 capsule。
只變更前置 owner 與補充驗收證明的讀取位置；**trial、獨立 reader、compare、
容量及 confinement 等既有函數 AST 均不變**。沿用原
**372,550 inputs／107 explicit receipts** 固定 corpus，其粒度與 R134 的
R116 catalog 不同；未縮減為較有利的子集。
保留 smoke、ABBA／BAAB、六份完整輸出、每份原始證據及 fresh-process reader。
兩組都完整重讀輸出 SHA，沒有重新加入已拒絕的 write-time SHA 候選。

容量 admission、module confinement、全部 semantic comparator 及 balanced
採用式沿用 R89 的精確 AST；每半組完整 child wall ratio ≤0.98、CPU ratio
≤1.02，兩半皆過才允許後續完整整合。記錄 cache hits／bypass／evictions、
實際保留 bytes、peak RSS、CPU、block I/O 與完整時鐘；來源／native cache
封存後從 capsule 執行，不受工作目錄後續編輯替換。原 R138 未開始比較，
此時尚無後繼 R142 全量結果，
不宣稱快取有效、完整工作流加速或硬體極限。

- [R134 重複工作量與原操作時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round134/diagnostic.json)
- [R134 原 catalog／profile SHA 及診斷成本](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round134/completion.json)
- [R135 私有候選](../artifacts/benchmarks/workflow_speed_optimization_20261021/round135/candidate_profile.py)
- [R135 保留原 507 項與新增 15 項的驗收](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round135/completion.json)
- [R136 固定全量來源 capsule 與原比較函數](../artifacts/benchmarks/workflow_speed_optimization_20261021/round136/run_frozen_metric_cache.py)
- [R136 原 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r136-frozen-metric-cach-20261024T101750-725c3659/run.json)
- [R138 原比較函數與精確接續前置證明](../artifacts/benchmarks/workflow_speed_optimization_20261021/round138/run_frozen_metric_cache.py)
- [R138 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r138-frozen-metric-cach-20261024T104221-7d401af1/run.json)

R139 `workflow-r139-retained-ingestion-20261024T105055-aee74d8c` 原另排在
R138 全量比較**實際成功終結之後**，診斷新整合的 acquisition receipts 成本。
沿用 R138 封存的 baseline `879e8457…`、原三個 bundle 與全部原始列／bytes
檢查；保留 issuer 16、Dune 70、SEC 84，共 **170** 筆與 R129 相同的時鐘
multiset。使用完整 packed evidence、六份輸出及新程序 canonical reader。
僅在該私有程序使用既有 `_InspectionTimings` 拆解 Evidence 原始讀取、快照
重讀、gzip、JSON、Parquet 與整個 bundle 成本；nested 範圍不相加，儀器
有額外開銷，不能作為無 profiler 的速度比較或完整 13 階段驗收。
此時尚無診斷結果；未預先把 479.498013 秒歸因於特定函數，也沒有先套用
另一個 raw-payload 快取。driver SHA 為
`f8f6494dbde896537d33f0059957093f2983e9a52f83e87f2b4f142c6d813ca8`。

- [R139 原生保留資料完整診斷](../artifacts/benchmarks/workflow_speed_optimization_20261021/round139/profile_retained_ingestion.py)
- [R139 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r139-retained-ingestion-20261024T105055-aee74d8c/run.json)

新 boot `cf9d28cb…` 使 R137／R138／R139 的舊程序與 unit 均消失，已以
原 supervisor 的 `run-reconcile` 留下 interrupted 證據，未宣稱比較完成。
R140 的 bootstrap import 失敗亦保留；新 R141 已完成正式 D 身分重新登錄，
guard source 仍為原 **5010eb4b…**，舊 boot enrollment 不變。詳見
[完整整合與跨開機接續](workflow_speed_optimization_2026-10-24_crypto_feature_clocks.md)。

R141 已完整 succeeded／exit 0、inactive／PID 0，覆蓋更新為
2,399 個身分／1,663 個有階段時鐘／736 個未綁定。
R142 `workflow-r142-frozen-metric-cach-20261024T143558-f2a44694` 已封存
28 份來源，兩個 smoke 均通過原始讀回與語意核對，開始相同 baseline、
候選、372,550 inputs／107 explicit、六個輸出的完整 ABBA／BAAB 比較。
smoke 的 14.423997／34.492314 秒是小樣本整合檢查，不作速度採用結論。
只有新驗收位置、guard enrollment
路徑改變：guard 函數的 AST 將該唯一路徑還原後與原版本相同，實際 guard
本體 bytes 相同；trial、semantic、compare、capacity、terminal、confinement
函數 AST 不變。每半組完整 wall ≤0.98、CPU ≤1.02 門檻不變。

首個全量 baseline 子命令已 exit 0，獨立讀回通過 **372,550 個選定身分、
3,767 個 retained originals、六份輸出、2,006 samples／722 groups**。
完整 child-launch-through-wait 為 **1,058.544299 秒 wall／343.674272 秒
descendant CPU**。worker 收據較早結束於 961.678704 秒，內含 inspection
395.729713、serialization 146.243950、完整輸出重讀 SHA 80.009090、獨立
reader 332.469633 秒；worker 邊界外另有 96.865595 秒，尚未拆解，不能全數
歸因於 snapshot close 或硬碟。採用式使用完整子命令的 1,058.544299 秒，
沒有拿 worker 較小值取代。

首個全量 candidate 子命令亦已 exit 0，六份輸出的語意、完整 372,550 個
身分及原始 reader 全數通過；完整 wall **884.932561 秒**、descendant CPU
**286.505975 秒**。worker 較早邊界為 835.877233 秒，其 inspection
282.812142、serialization 88.284058、輸出 SHA 重讀 95.811880、獨立 reader
365.217525 秒。快取實際 **14,634 hits／4,442 misses／3,356 evictions／712
bypasses**，期末 374 entries／59,126,208 bytes；沒有調大原 64 MiB／4,096
上限。這是八輪中的前兩輪，順序及系統快取影響尚未平衡，不能把差額歸因於
候選或宣稱採用，仍等待完整 ABBA／BAAB。

截至 **16:09 UTC**，第三次完整 trial（第二次 candidate）亦已結束並通過
原始資料及六份輸出的獨立讀回。完整 child 為 **1,228.363166 秒 wall／
337.061128 秒 descendant CPU**；worker 較早邊界為 1,133.952926 秒，其中
inspection 358.071067、serialization 174.267153、輸出 SHA 重讀 94.543132、
獨立 reader 498.582868 秒。原 372,550 個選取身分、3,767 份 unique originals、
2,006 samples／722 groups 均核對通過，快取 hits／misses／evictions／bypasses
及期末容量與前一次 candidate 相同。相同候選的完整 wall 從 884.932561 變為
1,228.363166 秒，保留此變動；目前 **3／8**，尚未形成任一完整半組，
沒有據此判定加速、回歸或改動原門檻。

另核對完整 command 與 worker 計時邊界：前三次的 wall 差額依序為
**96.865595／49.055328／94.410240 秒**。原 worker 在 `frozen_runtime`、
固定 plan 的完整 SHA 讀取與 JSON 載入之後才開始計時；該 plan 為
260,789,646 bytes，原程式對 SHA 與解析各呼叫一次 `read_bytes()`。
結果寫出之後的 `finally: close_snapshot_store()` 與程序結束也位於較早
worker 邊界外；獨立 reader 已包含在 worker，不能再加一次。這是既有
時鐘差額及來源呼叫順序的核對，**沒有量出啟動與 close 各自耗時**，不能
把差額全部歸因於任一項。採用門檻持續使用包含這些成本的完整 command；
原八輪執行來源不變，後續如需優化此差額須另行分段量測。
凍結 pack source `a3dcba1d…` 的成功路徑已在序列化 manifest 時 `seal()`
並關閉 descriptors，因此最後 `abort()` 會因 `closed` 立即返回；沒有證據
支持把此差額當成一次尚未記錄的 pack flush。

- [R142 第二次 candidate 完整子命令時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_2_candidate.command.json)
- [R142 第二次 candidate 原始階段與固定快取計數](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_2_candidate.json)
- [R142 第二次 candidate 全輸入與原檔獨立讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_2_candidate.readback.json)

截至 **16:36 UTC**，第四次 baseline 及父程序六輸出 semantic 比較亦已通過，
第一半組 **ABBA／4 次**完整。第四次 child 為 **1,480.151822 秒 wall／
431.820522 秒 descendant CPU**；worker 較早邊界為 1,277.572207 秒，其中
inspection 575.341214、serialization 178.338892、輸出 SHA 重讀 119.499273、
獨立 reader 397.955737 秒。四次各自保留 372,550 個選取身分、3,767 份 unique
originals、六份輸出、2,006 samples／722 groups，父程序的六個 semantic 摘要一致。

| 第一半組完整 child 均值 | Baseline | Candidate |
| --- | ---: | ---: |
| Wall 秒 | 1,269.348061 | 1,056.647863 |
| Descendant CPU 秒 | 387.747397 | 311.783552 |

由原 command 收據獨立重算，第一半組 wall ratio **0.832433512**（少 16.7566%）、
CPU ratio **0.804089348**（少 19.5911%），符合原 **wall ≤ 0.98／CPU ≤ 1.02**。
這只是第一半組通過；第二半組 BAAB 尚未完成，**沒有八次採用決策**，沒有將
這個比例外推成完整 13 階段或新版本組合的速度。原八次順序、所有波動及
R148–R150 的短 root 檢查同時存在之事實均保留。

- [R142 第四次 baseline 完整命令時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_3_baseline.command.json)
- [R142 第四次 baseline 原階段時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_3_baseline.json)
- [R142 第四次 baseline 完整獨立讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_3_baseline.readback.json)
- [R142 第四次 baseline 六輸出 semantic 摘要](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_3_baseline.semantic.json)

截至 **17:09 UTC**，第五次 candidate 的完整子命令、獨立原始資料讀回及父
程序六輸出 semantic 比對均通過，進度為 **5／8**，第六次 baseline 已開始。
第五次完整 child 為 **1,211.598533 秒 wall／372.390411 秒 descendant CPU**；
worker 較早邊界為 1,058.542926 秒，其中 inspection 456.070915、serialization
182.520101、完整輸出 SHA 重讀 104.622599、獨立 reader 306.340619 秒。
仍為 372,550 個選取身分、3,767 份 unique originals、六份輸出、2,006 samples／
722 groups；六個 semantic 摘要與首輪 baseline 完全一致。快取實際計數仍為
14,634 hits／4,442 misses／3,356 evictions／712 bypasses，期末 374 entries／
59,126,208 bytes。此單次結果沒有形成第二半組的加速比例或新採用結論。

- [R142 第五次 candidate 完整命令時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_4_candidate.command.json)
- [R142 第五次 candidate 原階段與快取計數](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_4_candidate.json)
- [R142 第五次 candidate 完整獨立讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_4_candidate.readback.json)
- [R142 第五次 candidate 六輸出 semantic 摘要](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_4_candidate.semantic.json)

截至 **17:28 UTC**，第六次 baseline 的完整子命令、原始資料獨立讀回與父
程序六輸出 semantic 比對均通過，進度為 **6／8**，第七次 baseline 已開始。
第六次完整 child 為 **1,421.957371 秒 wall／406.568445 秒 descendant CPU**；
worker 較早邊界為 1,327.821638 秒／321.949729 秒 process CPU，其中
inspection 400.621039、serialization 192.018778、完整輸出 SHA 重讀
186.174064、獨立 reader 536.949039 秒。相同的 372,550 個選取身分、3,767
份 unique originals、六份輸出、2,006 samples／722 groups 及 15 個 frozen
project modules 均驗證；六個 semantic 摘要與首輪 baseline 完全一致。
此單次最長已量測區段為 reader；不同 trial 的階段次序與波動均保留，不能
用 wall 減 CPU 推定純 I/O 等待，也尚未形成第二半組 BAAB 的採用結論。

- [R142 第六次 baseline 完整命令時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_5_baseline.command.json)
- [R142 第六次 baseline 原階段時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_5_baseline.json)
- [R142 第六次 baseline 完整原始資料獨立讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_5_baseline.readback.json)
- [R142 第六次 baseline 六輸出 semantic 摘要](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_5_baseline.semantic.json)

截至 **17:53 UTC**，第七次 baseline 的完整子命令、原始資料獨立讀回與父
程序六輸出 semantic 比對均通過，進度為 **7／8**，最後一次 candidate 已
開始。第七次完整 child 為 **1,114.198502 秒 wall／363.660696 秒 descendant
CPU**；worker 較早邊界為 1,028.614550 秒／289.813683 秒 process CPU，其中
inspection 369.721406、serialization 118.796125、完整輸出 SHA 重讀
104.993549、獨立 reader 430.346710 秒。相同 372,550 個選取身分、3,767
份 unique originals、六份輸出、2,006 samples／722 groups 均核對，六個
semantic 摘要與首輪 baseline 完全一致。第六、七次為相同 baseline，完整
wall 仍有 1,421.957371／1,114.198502 秒的波動；原值與執行條件保留，
不解讀為候選收益。第二半組 BAAB 尚未完整，沒有八次採用決策。
第七次執行期間 R156 的 root-only GPU FD focused checks 另耗完整 child
2.159053 秒；沒有修改原比較 capsule 或新開 D I/O 實驗。

- [R142 第七次 baseline 完整命令時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_6_baseline.command.json)
- [R142 第七次 baseline 原階段時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_6_baseline.json)
- [R142 第七次 baseline 完整原始資料獨立讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_6_baseline.readback.json)
- [R142 第七次 baseline 六輸出 semantic 摘要](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_6_baseline.semantic.json)

R143 `workflow-r143-retained-ingestion-20261024T143751-b9eb1449` 等待 R142
成功，再接續原 R139 的完整三個 bundle／170 samples 診斷。使用新的 R142
封存 baseline 及 R141 guard；量測與獨立 reader 子程序先載入 capsule，再
讓 guard 載入其 project modules，避免被 admission 程序的工作目錄 imports
混入。原始列／bytes／數值 oracle、六個輸出及新程序重讀均保留；尚無結果。

- [R142 新 boot 的同一固定比較](../artifacts/benchmarks/workflow_speed_optimization_20261021/round142/run_frozen_metric_cache.py)
- [R142 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r142-frozen-metric-cach-20261024T143558-f2a44694/run.json)
- [R142 第一輪 baseline 完整子命令時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_0_baseline.command.json)
- [R142 第一輪 baseline 階段時鐘及其明確較早邊界](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_0_baseline.json)
- [R142 第一輪 baseline 全輸入、輸出及原檔讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_0_baseline.readback.json)
- [R142 第一輪 candidate 完整子命令時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_1_candidate.command.json)
- [R142 第一輪 candidate 全階段與快取實際計數](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_1_candidate.json)
- [R142 第一輪 candidate 全輸入與原檔讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_1_candidate.readback.json)
- [R143 完整保留資料成本診斷](../artifacts/benchmarks/workflow_speed_optimization_20261021/round143/profile_retained_ingestion.py)
- [R143 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r143-retained-ingestion-20261024T143751-b9eb1449/run.json)

R144 `workflow-r144-selected-source-ca-20261024T151149-489821c6` 已排在 R142、
R143 **成功且實際 inactive／PID 0 之後**，目前只等待，尚未執行測試或採用。
它從八個完整 trial 重新計算兩半組原 wall／CPU 門檻，僅在兩者都通過時選擇
已通過 522 項檢查的私有快取來源，否則使用原 baseline；沒有放寬門檻或改寫
R142 的比較。接著只補上 `downloader/artifact_io.py` 的 measurement-source
封存，來源 bytes 及兩種候選 SHA 均已固定。

新 regression 經過實際 13 階段空白 fixture collection、原檔 SHA／封存核對、
工作目錄原檔改變後的獨立 canonical readback，涵蓋 individual／packed 兩種
格式。先要求原版恰有兩個 `atomic_writer_measurement_source_missing` 失敗，
再執行修正版的原 507 項、新 2 項，以及選到快取時原 15 項，共 509 或 524 項。
這些是排定的必要驗證，**尚無通過結果**；正式來源仍為 `879e8457…`，
之後仍須採用與完整 13 階段原始 oracle 再驗收。

- [R144 原比較門檻重算、精確來源修正與序列資格驗證](../artifacts/benchmarks/workflow_speed_optimization_20261021/round144/qualify_selected_source_capture.py)
- [R144 封存、SHA 與原檔換代後讀回的兩個 regression](../artifacts/benchmarks/workflow_speed_optimization_20261021/round144/test_atomic_writer_source_capture.py)
- [R144 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r144-selected-source-ca-20261024T151149-489821c6/run.json)

R145 `workflow-r145-local-credential-c-20261024T153459-8c95c8c2` 已排在
R142／R143／R144 **全部實際 inactive／PID 0 且有終態收據之後**，量測
45 個本機 credential gates 的正式共用稽核。該診斷獨立於快取是否通過，
不因先前候選 failed 而把本機可量測流程一併丟棄；仍保留各前置結果。
目前只等待，尚無測試或完整 audit 結果。

執行精確原 `audit_data_credentials.py` **18cc6b47…**，以原
`OperationTiming` **08ffc340…** 包裝原函式並逐次 delegate，沒有第二份
憑證判定邏輯；保留原呼叫順序、回傳物件、例外、SDK 子程序、狀態原子寫入
和 0600 權限。分別量測 literal presence parse、45 個 presence predicates、
FinLab session probe／resolution、legacy settings 讀取／projection、檔案權限
與名稱盤點。原 wall 時鐘另加 process CPU；FinLab 子程序 CPU 由最外層
完整 child 收據記錄，沒有把 parent process CPU 當成 descendant CPU。
inclusive／nested 區間明確不相加，儀器開銷亦不當作加速對照。

兩個新程序分別執行原版及包裝版，完整非機密狀態除生成時刻外必須相同；
第三個新程序核對全部 45 個身分、原 source SHA、檔案權限與每筆時鐘。
封存僅包含共用程式、原非機密 registry、單元測試及新非機密稽核輸出，
**不封存 .env、SDK credential file、OpenBB secret settings 或任何金鑰值**。
沿用原 SDK 的禁止網路 probe，父程序亦禁止連線；不登入、不刷新 token、
不驗證 entitlement、不呼叫 provider。這是受控本機稽核環境，不冒充正式
排程器的已載入程式或其 process environment。

排定原 48 項 credential tests、原 2 項共用 clock tests 與新 5 項委派／例外／
nested-clock 案例，共 **55 項**；目前僅 AST、固定 SHA 及案例數靜態核對完成，
沒有宣稱測試已通過。driver SHA 為
`cc9068371b324deecaf968affadb014f7f5cd46ee0f3b6c1ab21377d49750adf`。
這個診斷不會直接更新 registry 綁定；完整接入仍須 canonical importer 及
完整 13 階段驗收。R146 的剩餘身分盤點已獨立通過，詳見
[完整剩餘粒度與 Dune 原阻擋證據](workflow_speed_optimization_2026-10-24_crypto_feature_clocks.md)。

- [R145 原共用稽核與 clocks 的精確包裝](../artifacts/benchmarks/workflow_speed_optimization_20261021/round145/profile_credential_audit.py)
- [R145 回傳、例外、還原及 nested clocks 驗證](../artifacts/benchmarks/workflow_speed_optimization_20261021/round145/test_credential_observation.py)
- [R145 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r145-local-credential-c-20261024T153459-8c95c8c2/run.json)

15:44 UTC 重新核對 R144 的 50 個原 source pins，發現唯一非預期變化是
`stockagent/data_sync/packed_snapshots.py`：原 **e8682bfe…** 已變成
**f2f60c22…**（本機檔案 mtime 15:22:42 UTC）。這是目前工作目錄既有修改，
沒有覆寫或還原。R142 的 28 個來源仍從先前 capsule 執行，所以其完整比較
沒有被替換成新版程式，也沒有因觀察超時重新開始。

對照 R129 原始 archive 的 AST，恰好新增 `_materialized_file_sha256`、
修改 `_verify_materialized`，其餘原函式／class 均相同；`_SeekablePackBuffer`
與 `_validate_manifest` 亦相同。新增路徑對 **64 MiB 至 8 GiB** 的檔案，只在
兩個已登錄 completed scratch prefixes 使用原生完整 SHA，保留 Linux FD、
type／size／generation／mode／nlink 與完整樹盤點；這是改動內容的核對，
尚非本 task 對新版行為或速度的驗收。private workflow scratch 未加入其
原生 allowlist，沒有以放寬 namespace 檢查換速度。

R144 此時仍在等待原前置程序，尚未通過 source gate。已執行或排定的
R144 程式及門檻不改寫，原嚴格拒絕條件保留。新 R147
`workflow-r147-current-shared-sou-20261024T155331-b4cf1b3b` 排在 R145
**實際終結**後接續：對原 50 份來源只允許此一精確的 packed-source 變化，
其餘原 pins／archives、原完整快取比較與兩個 negative controls 均保留。
另固定 native adapter、PowerShell helper 及兩份完整 storage tests 的 SHA，
加入 **321 packed-snapshot + 72 Windows I/O = 393** 項回歸。
因此完整資格檢查為原 baseline 分支 **902 項**或快取分支 **917 項**。

R147 的語法、AST 組裝、精確新版 source pins 及變更範圍已唯讀核對通過；
**尚未執行這批測試或採用私有 profile**。測試時限由 300 增為 900 秒，
僅供新增 393 項檢查；root 暫存預估提高為 1 GiB。原完整快取 wall／CPU
採用式、64 MiB 容量、readback 與 source fences 全數不變。即使資格檢查
通過，仍需新版本採用與完整 13 階段原始 oracle 驗收，不能把舊 source 的
對照外推為新版組合的已測速度。R145 的 audit 與 source gate 結果各自保留，
不把獨立 audit 的失敗當作新版儲存行為成功或失敗的證明。

- [R147 精確共用 source 變化及完整回歸接續](../artifacts/benchmarks/workflow_speed_optimization_20261021/round147/qualify_current_packed_source.py)
- [R147 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r147-current-shared-sou-20261024T155331-b4cf1b3b/run.json)
- [R129 保留的原 packed source](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round129/sources/33-packed_snapshots.py)

## R148–R150：本機 credential 時鐘匯入資格驗證

已準備原 baseline／私有快取兩種匯入草稿，延伸現有
`_collect_retained_native_operations`、`Ledger`、source-stage binding 與 CLI，
沒有另一份憑證判定邏輯。新的 `--local-credential-audit-report` 接受固定且
完整的稽核目錄，只讀取八個明確公開收據及宣告的公開程式／registry archives。
保留原 13 個 collection phases；不讀取 .env／SDK session／OpenBB 私密設定。

R148 的 **50 項**合成收據檢查通過，完整 pytest child **7.043452 秒 wall／
5.913699 秒 descendant CPU**。覆核發現初稿只保存 audit、clock helper、
registry 三份來源，缺少實際包裝時鐘的 observer。原程式及檢查結果全部保留，
沒有採用此初稿。R149 新增 observer SHA／唯一公開 archive、明確本機環境
scope 及 inclusive aggregation 驗證；先用缺少 observer 的兩個案例測原版，
**恰有兩個預期失敗**，再對兩種修正版執行 **62 項，零失敗／錯誤／略過**。
新版完整 pytest child **15.671197 秒 wall／8.753203 秒 descendant CPU**；
62 項包含原案例，不與先前 50 項相加。

驗證涵蓋完整原版／量測版 public status parity、全部 45 個精確身分、每筆原
wall float／CPU nanoseconds、重複 bundle 不重算、無效值／缺漏／重複時鐘／
錯誤 source SHA 不產生部分 samples，以及 individual／packed 兩種布局。
只將 `credential_presence_predicate` 的兩個 metric 綁到精確 `credential:`
身分；FinLab 巢狀 probe／resolution 和整體 wall 均保留但不額外分配到 provider。
CPU 明確不含 SDK 隔離子程序，inclusive intervals 不相加，儀器成本不作加速結論。

R150 在 **16:31:18 UTC** 以新程序載入未改的原版 `profile=879e8457…`，
對兩種草稿 × 兩種布局的 **四份合成完整收集**執行原 reader。各自全部
**11 outputs／15 unique originals／97 samples／13 phases** 通過；另核對
90 個精確綁定 samples（45 身分各 wall／CPU 兩筆）、其餘七個不綁定 samples、
observer bytes，以及來源工作目錄已變動後的 frozen clocks。此新程序驗證為
**0.256136 秒 wall／0.243123 秒 process CPU**。

三個 owner 均 succeeded／exit 0，實查 inactive／MainPID 0。這些是 root 上的
短程合成資格檢查，執行期間 R142 第四次 baseline 仍在運行；不把這些時間
算入資料來源本身，亦不改寫其完整比較。**尚未匯入 R145 的實際時鐘、未改
canonical profile、未更新 2,399／1,663／736 覆蓋**。下一步仍需 R145 實際
成功、R147 選定 source 的完整共用回歸、正式採用與全範圍 13 階段原始 oracle；
這四份合成產物不代替上述實際工作流驗收。

- [R148 原匯入草稿與兩種來源準備](../artifacts/benchmarks/workflow_speed_optimization_20261021/round148/prepare_credential_import.py)
- [R148 原 50 項結果](../artifacts/benchmarks/workflow_speed_optimization_20261021/round148/checks.json)
- [R149 observer 來源補全與原版 negative control](../artifacts/benchmarks/workflow_speed_optimization_20261021/round149/qualify_credential_import_provenance.py)
- [R149 兩個預期失敗的原始證據](../artifacts/benchmarks/workflow_speed_optimization_20261021/round149/negative.json)
- [R149 新版 62 項完整結果](../artifacts/benchmarks/workflow_speed_optimization_20261021/round149/checks.json)
- [R149 修正後四份公開來源與精確綁定實作](../artifacts/benchmarks/workflow_speed_optimization_20261021/round149/credential_presence_extension.py)
- [R149 SHA 與完整資格驗證完成收據](../artifacts/benchmarks/workflow_speed_optimization_20261021/round149/completion.json)
- [R150 新程序原版 reader 與原值核對](../artifacts/benchmarks/workflow_speed_optimization_20261021/round150/verify_credential_import_with_original_reader.py)
- [R150 四份完整獨立讀回](../artifacts/benchmarks/workflow_speed_optimization_20261021/round150/readback.json)
- [R149 原 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r149-credential-observe-20261024T162635-284b3fc5/run.json)
- [R150 原 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r150-credential-origina-20261024T163114-414a55c1/run.json)

R151–R153 在等待第五次完整比較時，另以現有稽核量測平常 GPU 負載。
penguin 整卡兩筆為 3,992／4,026 MiB；99／99 個 managed Linux processes
均可讀取，GPU 裝置 handles=0。Windows 歸屬兩次 30 秒 deadline 均逾時，
原 audit 全 child 63.630566 秒 wall／1.829150 秒 WSL reaped-child CPU；
Linux FD audit 0.577643 秒 wall／0.398086 秒 CPU。純 console 控制也在
5.349162 秒逾時，未執行 CIM；外部傳輸／PowerShell 啟動與 Windows 歸屬
仍未驗證，不把未知變成沒有用量。這些觀測與第五次 candidate 同時存在，
未改凍結來源、模型或服務，也沒有新增 source-stage bindings。
原收據、樣本範圍及歸屬限制見
[目前 GPU 資源與 Windows 查詢邊界](gpu_idle_load_2026-10-23.md)。

R154 `workflow-r154-selected-credentia-20261024T171836-f2701ff6` 已由原
systemd owner 排在 R147 與 R145 **成功／exit 0、inactive／PID 0** 之後，
目前實際 PID 682064 等待中。它將核對完整八次比較選定的版本，取 R149
同版本草稿，保留 R147 的共用回歸與 393 個 storage cases，再加入選定版本
原有 **31 個** credential importer cases，應為 **933／948 項**，依是否選用
metric cache 決定。五個原測試函數本體不變，只有 fixture 綁定改為同一個
selected owner；AST、來源 SHA、31 個案例的靜態盤點已核對，**尚未執行測試**。
原 checks 只追加獨立 root `--basetemp` 參數；900 秒 timeout 與 R147 一致，
沒有透過繼承的 `PYTEST_ADDOPTS` 影響測試啟動的子程序。

成功後將以 R145 **實際完成**的八份公開收據匯入所有 stage wall／CPU 原值，
完整來源 SHA、45 個本機 predicate IDs 與 90 個可綁定 samples 均須驗證。
第三個 fresh process 使用 unchanged `879e8457…` reader，重讀六份實際輸出、
凍結原值、registry／observer 公開來源及兩種 layout 的合成 13 階段產物。
實際匯入的 sample 數使用 `2 × len(original stages) + 1`，不把合成 fixture
的 97 筆當成真實 audit 筆數；只有 45 個 predicate 可綁定，nested／total
繼續保留但不額外綁定。容量須有 root 8 GiB＋1 GiB、D 32 GiB＋512 MiB、
RAM 24 GiB，任何前置失敗均保留原收據並拒絕後續。

此資格檢查**不採用 canonical、不更新 2,399／1,663／736 覆蓋、不代表完整
實際 13 階段驗收或全工作流加速**。完成後仍需相同 selected source 的完整
實際工作流與原有全部 native oracles，再決定整合；沒有 provider 請求或
GPU 工作。driver SHA 為 `8e1c893ad93920c0f0bb4b5a742706d8aecf8ef64e69f071a0486547c6f73974`，
已啟動的 R142–R154 來源均保持不變。

- [R154 選定組合、完整共用回歸及真實收據匯入驗證](../artifacts/benchmarks/workflow_speed_optimization_20261021/round154/qualify_selected_credential_import.py)
- [R154 原 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r154-selected-credentia-20261024T171836-f2701ff6/run.json)

R155 `workflow-r155-portable-credentia-20261024T174009-4ea57fee` 補上正式
測試可攜性的前置檢查。原 R149／R154 fixture 的第四份公開來源直接從
`round145/profile_credential_audit.py` 讀取，依賴忽略的 benchmark 檔案，
不能原封移成 maintained test。新稿由 fixture 自行建立公開、僅供來源身分
核對而不執行的 synthetic observer bytes 與邏輯路徑；前三份 audit／timing helper／registry
仍讀 maintained source，真實 R145 收據、observer SHA 與匯入器邏輯不變。

fixture 期間新增 `Path.read_bytes` 哨兵，拒絕讀取既有 repo 的
`artifacts/benchmarks` 來源，只允許該案例自己的 `tmp_path` 產物；這是針對
已確認的 fixture 依賴做檢查，不宣稱作業系統層級隔離。原版 fixture 的兩個
案例須各以 `private_benchmark_source_read` **setup error** 被拒絕，不能用別種
錯誤或 skip 代替；可攜稿再以同一個 selected profile 通過原 **31 個案例**。
五個 test 函數本體的 AST 完全相同，原 assertion 未減少；變動僅 fixture、
repo 定位與不再需要的 imports。新程式亦安排 fresh unchanged reader 重讀
兩種 layout 的合成 13 階段／97 samples 產物。

目前 AST、whitespace、原來源 SHA、31 個展開案例及移除 `round145` 私有
路徑的靜態核對已通過；**實際 negative control、31 cases 與新 fresh reader
尚未執行**，等待 R154 真正成功／exit 0、inactive／PID 0。D guard、root
8 GiB＋256 MiB、D 32 GiB＋64 MiB、RAM 24 GiB 均需准入，失敗保留原收據。
尚未建立正式 `test/` 檔案，也沒有更新 canonical、native clocks 或全域覆蓋；
後續仍須完整實際 13 階段與所有原始 native oracles 才作採用結論。
driver SHA 為 `1c1268665b2e37f22b8a5ad1deeb34766c1a8d3df1ec528daeffa8ba96d7cacb`，
開始後保持來源不變，沒有新增 provider 請求或 GPU 工作。

- [R155 可攜 fixture、原依賴 negative control 與獨立 reader](../artifacts/benchmarks/workflow_speed_optimization_20261021/round155/qualify_portable_credential_tests.py)
- [R155 原 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r155-portable-credentia-20261024T174009-4ea57fee/run.json)

R156–R157 補齊目前 GPU 歸屬在 Linux 端的觀測範圍。沿用 canonical FD audit，
新增明確的 `--include-unmanaged`，保留原預設 managed v1 行為，通過 **16 項**
focused semantic cases；完整 child 2.159053 秒 wall／1.951447 秒 CPU。
兩次全可見範圍完整觀察 **183／183、187／187** 個程序，managed 各 89、
unmanaged 94／98、unknown 0。兩次唯一 `/dev/dxg` 持有者 PID 507606 經
獨立身分核對為 `/usr/bin/nvtop`，89 個 managed 程序仍均無 GPU handle；
Windows VRAM 分配仍未知，沒有把 FD 數推成 4,539／4,562 MiB 的記憶體歸戶。
兩次 CLI 0.558898／0.980600 秒，fresh reader 0.342549 秒，R157 完整 child
5.770836 秒 wall／3.316562 秒 CPU；分段 parent CPU ns 與 NVML calls 亦保留。
R156 與第七次 baseline、R157 與最後一次 candidate 同時存在，保留此條件。
新 FD audit SHA=`45b14a6c…` 不屬於 R129 的舊 50 份 gate source，沒有變更
R142 凍結來源、完整輸入或門檻，沒有新 provider／CUDA compute 工作，也未
改 canonical profile、全域 source bindings 或做新的整體加速宣稱。
程式變更、全部原收據與目前歸屬限制見
[全可見 Linux GPU handles 與 nvtop 身分證據](gpu_idle_load_2026-10-23.md#2026-10-25-續查完整可見-linux-程序與唯一-handle-持有者)。

R142 於 **2026-10-24 18:16:18 UTC** 實際 succeeded／exit 0，systemd 已
inactive／MainPID 0。第八次 candidate 完整 child 為 **1,066.909058 秒 wall／
361.539749 秒 descendant CPU**；worker 950.144483 秒，inspection
455.471407 秒、serialization 95.225053 秒、完整六輸出 SHA 重讀
24.208937 秒、獨立 reader 369.701769 秒。獨立 reader 核對 6 輸出、
3,767 原始證據、372,550 selected identities、2,006 samples、722 groups，
父程序六輸出 semantic SHA 也與全部前次一致，沒有排除差異。

| 原交錯順序 | baseline 完整 child wall 均值 | candidate 均值 | wall 減少 | descendant CPU 減少 |
| --- | ---: | ---: | ---: | ---: |
| ABBA | 1,269.348061 s | 1,056.647863 s | 16.7566% | 19.5911% |
| BAAB | 1,268.077937 s | 1,139.253795 s | 10.1590% | 4.7128% |

八次原始 wall／CPU 門檻（兩組各 wall ratio ≤ 0.98、CPU ratio ≤ 1.02）均通過。
快取上限、順序、完整原始讀取與 SHA、新程序 readback 均未改。
整個 R142 owner 含整合 smoke、來源／准入檢查、八次試驗及父程序語意比對，
monotonic **13,239.115913 秒 wall／3,541.861020 秒 descendant CPU**；此總時間
包含比較兩個方案的成本，不是單一工作流執行時間，也不與分段相加。
保留第 4、5、7、8 次旁行短測試／觀測的條件，以及各次 I/O 波動。
不能把本次比例與 R121 相加，或稱為整體 13 階段／硬體極限。
R143 已在 R142 真正終態後開始 fresh child，接續量化 retained ingestion；
後續資格與實際完整驗收沿原順序執行。

- [R142 八次原始比較與採用判定](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/comparison.json)
- [R142 第八次完整命令時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_7_candidate.command.json)
- [R142 第八次獨立 reader](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_7_candidate.readback.json)
- [R142 第八次六輸出語意 SHA](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round142/full_trial_7_candidate.semantic.json)
- [R142 owner 原始終態與完整資源時鐘](../artifacts/operations/agent-workflow/runs/workflow-r142-frozen-metric-cach-20261024T143558-f2a44694/run.json)

R143 已成功完成 **170 個真實 native samples** 的採集、全部原始 row／receipt／
page oracle、六輸出與 fresh reader；owner succeeded／exit 0、inactive／PID 0。
採集完整 child **849.604960 秒 wall／114.591668 秒 descendant CPU**，
獨立驗證 **30.355454 秒／5.401475 秒**。純 ingestion 範圍含觀測器為
811.463840 秒 wall／110.865708 秒 process CPU；issuer／Dune／SEC 分別
5.350980／576.190468／229.911640 秒，各 16／70／84 samples。

| 被觀測操作 | calls | inclusive wall | process CPU |
| --- | ---: | ---: | ---: |
| stat | 17,298 | 262.782807 s | 22.934493 s |
| 首次同步開啟、讀取、關閉 | 8,649 | 398.510706 s | 23.121936 s |
| 已封存 snapshot 重讀 | 4,345 | 46.965400 s | 6.170184 s |
| JSON decode（全部） | 2,927 | 15.498034 s | 12.560832 s |
| 首次 SHA256 | 8,649 | 0.775953 s | 0.602474 s |

`evidence_read_inclusive` 12,994 calls／762.203269 秒包含上列若干範圍，
不能與它們相加；JSON decode 也含內層 decode。保留全部 8,649 證據 identities、
481,280,233 原始 bytes，完整 serialization＋輸出 SHA 為 14.322175 秒。
這支持先量測重用有界 native 預讀來降低檔案延遲，而不是先改 JSON／SHA 語意。
觀測器有成本，本次沒有加速比例；R143 owner 14,029.392605 秒包含等待 R142，
不能當成一次 ingestion 的計算時間。

R145 另已實際通過 **55 tests** 與原 credential audit 的 public parity：
baseline／instrumented／fresh verifier 完整 child 分別 1.045393／0.992675／
0.524617 秒，descendant CPU 0.580413／0.559727／0.326939 秒。
觀測範圍內 inclusive total 0.253230994 秒；其中 45 個 presence predicates
和 9 種 canonical 操作分段皆有 wall／process CPU，巢狀項不相加。
這是本地 presence audit，沒有驗證登入、權限或 production service 環境。

R144 已因舊 shared source pin 拒絕；R147 進入 negative control 後，因從
R142 capsule 選擇的 profile 在 import 時先載入不完整 capsule `downloader`，
兩項均在 `finmind_updates` import 失敗，未到預期 source-list 斷言；
R154／R155 因前置未成功而終止，全部原收據保留。R158 在新 boot 的 guard
先行拒絕，尚未執行修正。最新 boot 為 `53672714-6641-443a-bc05-a53a45f82f3f`；
R159 已核對舊八個 owner 皆實際終態，重新登錄 **相同 SHA 的 D guard**，
保留 volume marker、PID1 namespace、held FD、ext4／D 容量與 RAM 准入。
來源位元相同的 selected profile 改在非 capsule 路徑執行 negative control，
兩項已在正確的 `atomic_writer_measurement_source_missing` 斷言失敗；
R160 的 917 項正向資格後來為 **797 pass／120 fail**：測試暫存誤放在 Git
worktree，觸發 117 個 cold-store 防護及 3 個 benchmark 路徑排除測試。
沒有改 assertion；R161 僅把相同案例的獨立暫存移至 `/var/tmp`，**917 項
全部通過**，完整檢查 110.547382 秒 wall／63.179573 秒 descendant CPU，
整個 owner 124.607399 秒／67.884665 秒，succeeded／inactive／PID 0。
未改 R142 凍結來源或門檻。

- [R143 完整分段計時](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round143/operation_timings.json)
- [R143 六輸出與獨立 reader 完成收據](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round143/completion.json)
- [R145 本地 audit 完成與原始 clocks](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round145/completion.json)
- [R159 新 boot 保護器登錄與限定重驗](../artifacts/benchmarks/workflow_speed_optimization_20261021/round159/enroll_and_qualify.py)
- [新 boot 登錄原始證據](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round159/enrollment.json)
- [相同來源移出 capsule 的 negative control](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round160/negative_control.json)

R162 已通過 **948 tests**，完整 checks child 113.391317 秒／62.468386 秒 CPU；
實際匯入後因 `credential public status identities differ` 拒絕，R163 因前置失敗
停止。R165 用明確八份 public receipt＋四份 public source allowlist 的純讀取
重現同一拒絕，沒有寫入或讀取密鑰檔。查明 **11 個 OpenBB catalog IDs 與
canonical public IDs 不同**，例如 `openbb_fred` 對應 `openbb:fred_api_key`；
原 fixture 把 catalog 當作 public ID，無法支持實際收據驗收。

R166 保留兩種 ID 與一對一映射，consumer 以 captured registry 的資料欄位
核對公開 ID；測試與獨立 reader 則使用 pinned canonical producer 的 pure
`_presence_row` 作獨立 oracle，不執行觀測到的 archived source，不讀密鑰。
原 31 個案例保留，補 4 個 mapping 拒絕案例及 1 個 portable fixture 的私有
benchmark 路徑拒絕案例，credential 共 36；連同原 917 項，**953 項資格仍在
執行**。新候選 SHA=`ae33150c056f0d5d66f5c904e975b13cd97450dc0f8c0b5cbb3c2390207ab14e`。
先行純讀取診斷已確認實際 bundle 可產生 **113 個原始 wall／CPU samples**，
其中只有 45 predicates／90 samples 能綁定；完整封存與 fresh reader 尚待實跑。
canonical profile 仍 `879e8457…`，全域覆蓋仍 2399／1663／736，沒有把資格
測試或 pure read 診斷當成實際 13 階段驗收。

R164 已準備沿用 R123 完整 13 階段、14 輸出、6 training roots、239 explicit
receipts、43 TW native operations、完整 CEX／issuer／Dune／SEC 原始 oracles
及 reconstruction／untouched reader 的新驗收程式；只對 profile／shared packed
source 增加精確的新資格映射，另加入真實本地 credential receipts。
須 R166 實際成功並接入同位元實作後才啟動，現在尚未執行或採納。

- [R161 全部 917 項及 source-capture 資格](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round161/completion.json)
- [R162 948 項通過，實際匯入仍拒絕的原始 log](../artifacts/operations/agent-workflow/runs/workflow-r162-selected-actual-cr-20261025T033457-3911356a/run.log)
- [R165 僅限公開 bundle 的拒絕重現](../artifacts/operations/agent-workflow/runs/workflow-r165-public-credential--20261025T033944-4e17fb2b/run.log)
- [R166 public／catalog ID 修正及原 oracle 保留](../artifacts/benchmarks/workflow_speed_optimization_20261021/round166/qualify_public_credential_identity.py)
- [R164 尚待執行的完整實際驗收](../artifacts/benchmarks/workflow_speed_optimization_20261021/round164/run_selected_full_workflow.py)
