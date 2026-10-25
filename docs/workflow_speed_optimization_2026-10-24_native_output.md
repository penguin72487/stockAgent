# 工作流測速第二十五輪：私有原生輸出候選

R24 完整輸出階段為 **173.097097 秒 wall／40.168848 秒 CPU**，被動取樣觀察
到大量 write 與 `p9_client_rpc` 等待。因此下一項實驗比較跨 WSL 的檔案寫入
成本。**25 項檢查通過**，Windows 空批次互通與真實小檔 native publication
均通過。完整八次比較與獨立十三檔×八次 SHA 讀回已完成；原生候選兩半
均較慢，因此**不採用**，原 owner succeeded／MainPID 0。正式 cold policy
與 writer 沒有更換，沒有硬體極限結論。

## 機制與範圍

沿用 `stockagent/data_sync/windows_cold_io.py` 的既有 `BinaryWriter`，封存
當下既有 dirty bytes，在隔離 module 中加入精確私有路徑設定；PowerShell
FileStream script 的固定路徑替換可反轉。正式 cold namespace、目前共享
來源、正式服務均未修改，也沒有 source／GPU 工作。

私有邊界只容許 R25 `trials` 中八個指定 trial、獨立 `preflight`，以及十三份指定輸出的新 temporary
檔案。拒絕 append、其他 mode、低於 32 GiB reserve、未知名稱、已存在檔案與
symlink／redirected parent。父目錄在進入寫入區段前檢查，以 dirFD 保持，發布後再次核對 namespace。
原生結果必須有完整 bytes／SHA 與 durable flush，再執行 canonical
`durable_replace`，保留 file fsync、atomic rename、parent fsync；最後從 D
讀回完整 SHA。失敗留存證據，不改用未宣告的 fallback。

25 項檢查涵蓋上述路徑／操作拒絕、mode／trial 配對及允許的 native configuration。互通探測
**4.582811 秒**，僅執行既有空批次 exact marker probe，沒有寫報告檔案；它
不代表真實 payload 已通過或八次比速已完成。十一份執行／參考來源已封存。
隨後實際 **361 bytes** 的 `openbb_operation_observation.json` 通過 native
pipe、flush、canonical rename 與完整 SHA 讀回，**3.749300 秒**。這項真實
小檔 preflight 不納入八次 timing，仍不代表十三份完整報告已驗收。

## 固定比較與接續 owner

固定輸入為 R20 已驗證的完整十三個報告檔，合計 **794,193,072 bytes**。
所有檔案原值、格式、SHA 保留；先按 SHA 複製到 private native scratch，再
由每個 fresh child 讀入，避免把變動的來源內容混入比較。這份 scratch 不取代
原始報告或資料 authority。

比較順序 **A/B/B/A、B/A/A/B**；兩半各自要求 whole child B/A ≤ 0.98。
每個 child 包含來源讀取、輸入 SHA、完整十三檔發布、落盤、rename 與 D SHA
讀回；八次之後另由父程序完整核對所有輸出。Linux descendant CPU 另列，
Windows FileStream 與 Windows 9p server 的 CPU 無法由該 counter 公平比較，
因此沒有 CPU 加速主張。JSON 序列化、aggregation 及整個 collector 不在本項
比速範圍，候選即使通過也仍需 canonical integration 與完整流程測量。

以 R24 單次完整觀測作成本分解，假設整個 173.097097 秒的 aggregation／輸出
階段降至零、其他成本完全不變，collector 加速上限約 **1.283x**；包含原版
獨立讀回則約 **1.166x**。這是條件式 Amdahl 上界，沒有量到上述加速；writer
實際只影響該階段的一部分。425 秒 inspection 與 434 秒 scope reader 仍須
各自處理，不能將寫入局部倍率套用到完整工作流。

容量 admission 預留 D **32 GiB**、root **8 GiB**，forecast D **12 GiB**、
root **2 GiB**、minimum memory headroom **4 GiB**；不足延期，不刪資料。
開始完整輸出前再核對 free space。前置等待不算效能樣本。
等待可能跨越其他工作，因此 v2 也在完整輸入複製及正式 timing 前重查實際
memory headroom。修訂在任何 trial 前完成，v1 driver、manifest、protocol
與 location 均保留；輸入、順序、wall 判準與完整讀回不變，原等待程序繼續。

v3 修正共用 parent guard 的檢查順序：舊版在 empty-body probe 中已進入區段，
才於結束時拒絕 ancestor symlink；新版於進入前拒絕，兩次 probe 均未寫入
payload。25 項測試及修訂收據通過，v2 source／manifest／protocol／location
完整留存於 `preflight_amendment_v2`，R22 source pins 沒有改動。
v3 protocol SHA 為
`fbea11d7422644e61ee64605a0e496d8f4a2c0277bdbff453affb3e23e5a697e`。
在正式全量輸入複製與八次 timing 前，新增以最小真實報告進行 native pipe、
flush、canonical rename 及完整 SHA 讀回；它使用獨立 `preflight` 目錄，
不納入八次樣本。修訂時這項 payload preflight 尚未執行，不能把空批次互通
或小型回歸視為真實 native payload 驗收。

監督 owner 為 `workflow-r25-writer-continuation-20261023T184608-cc4a5e8e`，
等待原 `workflow-r22-reader-continuation-20261023T171701-c52c2138` 的 terminal
unit／成功收據、八次比較及獨立 oracle 結果後才啟動。R22 未成功則保留原因
停止，不另開重複比較，也不把 queued／等待狀態算作測速完成。

19:31 UTC 已確認 R22 原 continuation succeeded／MainPID 0、八次與額外原版
oracle 通過。R25 重新 admission 通過：D free **584,613,466,112 bytes**、
root free **44,957,896,704 bytes**、memory headroom **89,244,098,560 bytes**；
完成上述 361-byte 真實 preflight 後才複製完整輸入。前置等待與複製不作八次
效能樣本，實際 timing 與所有十三檔讀回仍以其後收據為準。

## 完整結果：保留負面實驗

八個 fresh child 全部 exit 0，每個 child 完整發布及讀回十三檔；父程序再
花 **202.148366 秒**，逐一核對全部八組輸出的原始 bytes／SHA。內容驗收
通過，兩半效能門檻均未通過，沒有提前省略最後一筆或獨立核對。

| 順序 | 原版 wall 均值 | 原生候選均值 | B/A | 決策 |
| --- | ---: | ---: | ---: | --- |
| ABBA | 75.035652 s | 89.237193 s | 1.189264 | 不採用 |
| BAAB | 48.438047 s | 60.014676 s | 1.238999 | 不採用 |

固定小檔也能看到額外成本：A1／B2 的 361-byte 報告 publication＋完整讀回
分別為 **0.077138／1.895978 秒**，5,352-byte environment 為
**0.101057／2.127717 秒**。這是同一檔案的兩筆完整範圍觀測，包含 guard、
native interop、flush、rename 及 SHA；尚未各別拆開上述元件，不能全部
歸因於 PowerShell startup。它支持繼續測量減少寫入次數的緩衝候選，沒有
證明 Windows 原生寫入在所有 workload 都較慢。

- [八次比較、兩半門檻與獨立全檔驗證](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round25/comparison_result.json)
- [原 writer continuation terminal 結果](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round25/continuation_result.json)
- [A1 完整逐檔計時](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round25/receipts/A1.json)
- [B2 完整逐檔計時](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round25/receipts/B2.json)

## 後續串流緩衝候選

R27 已準備另一個獨立候選，只在 `write_csv`／`json_lines` 的 open 加上
**1 MiB buffering**；反轉兩個 keyword 即恢復原函式全部 bytes。沒有變更
CSV／JSONL 編碼、newline、欄位、浮點值、close 或 durability policy。
R25 比較的是完整 bytes 發布；這一項針對原本逐 row renderer 的寫入成本，
因此不能用 R25 決策取代其測量。

固定輸入是同一份 R20 已驗證觀測中的五個串流輸出，合計 **620,615,573 bytes**。
沿用 R25 已完成驗證、原 owner 已 terminal 的 ext4 private input copy，保存
其原 copy receipt 並重查五檔完整 SHA；每個 fresh child 仍重新核對輸入。
因此不再建立另一份 payload generation，原 R25 inputs 保持原樣。
CSV 以相同順序的字串欄位重建原 wire bytes，JSONL 保留 JSON row；每個檔案
均須符合原始預期完整 SHA，而非只比較 row count。whole child 包含輸入
SHA／parse、五個完整 renderer、close 及輸出 SHA；另記錄 render 期間
`/proc/self/io` 的 `syscw`／`wchar`，不把 write 次數當作完整 workflow 時間。

八次仍為 ABBABAAB，兩半各自要求 wall B/A ≤ 0.98、Linux descendant CPU
B/A ≤ 1.02，最後獨立再讀全部五檔×八次。原
`workflow-r26-full-reader-collect-20261023T194807-fc478e7f` 已 terminal 成功，
完整新觀測與兩種 reader 的語意核對通過；R27 於 **20:06:28 UTC** 開始
固定輸入比較，八筆 fresh child 均成功，另用 **226.790977 秒**獨立核對
全部五檔×八組 SHA。兩半 wall 與 Linux CPU 判準均通過，原 continuation
已 succeeded／MainPID 0。A1／B2 的 renderer Linux `syscw` 為 **72,842／596**，
`wchar` 均為 **620,615,573 bytes**；write 呼叫次數不等於 9p RPC 數或完整
工作流加速倍率。預留 D 32 GiB、root 8 GiB，forecast D 6 GiB／root
2 GiB，memory headroom 至少 16 GiB。

| 順序 | 原版 whole child 均值 | 緩衝候選均值 | wall B/A | Linux CPU B/A |
| --- | ---: | ---: | ---: | ---: |
| ABBA | 171.941450 s | 101.808953 s | 0.592114 | 0.841818 |
| BAAB | 117.883625 s | 63.619562 s | 0.539681 | 0.751877 |

以上是五份固定報告的完整輸出子流程，包含 startup／輸入 SHA／parse／render／
close／輸出 SHA。CSV reconstruction 使用 wire string，不能把 **40.8%／46.0%**
直接當成 typed collector、訓練 epoch 或全部工作流的加速幅度。

完整 canonical patch 只有上述兩個 keyword；反轉後恢復前版
`3301e6d…` profile 的全部 bytes，候選兩個 renderer AST 與實際測量版本一致。
R28 在上述原 owner 與完整 gate 通過後已套用，profile 成為 `d3bf90fe…`；
focused regression 與新完整 collector／oracle 的驗收各自記錄，歷史 R26／R27
driver 與 source guards 保留原樣。

- [兩個可逆 keyword、固定五檔 SHA 與候選身分](../artifacts/benchmarks/workflow_speed_optimization_20261021/round27/candidate_preparation.json)
- [已成功的候選準備收據](../artifacts/operations/agent-workflow/runs/workflow-r27-prepare-stream-buff-20261023T192727-6db5ca87/run.json)
- [實際執行的完整五檔 renderer 比較](../artifacts/benchmarks/workflow_speed_optimization_20261021/round27/compare_stream_buffering.py)
- [完整 profile 的可逆兩處 patch 準備證據](../artifacts/benchmarks/workflow_speed_optimization_20261021/round27/buffer_integration_preparation.json)
- [原 R26 owner 之後接續的實際 supervisor](../artifacts/operations/agent-workflow/runs/workflow-r27-stream-continuation-20261023T195038-1171587e/run.json)
- [八次完整比較與獨立四十檔驗證](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round27/comparison_result.json)
- [原 continuation terminal 成功與採用條件](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round27/continuation_result.json)
- [新來源 cohort 的整合準備](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round28/preparation.json)
- [R28 canonical 整合與完整驗收](workflow_speed_optimization_2026-10-24_buffer_integration.md)

## 證據

- [前輪實測瓶頸與全量讀回](workflow_speed_optimization_2026-10-24_canonical_journal.md)
- [容量與來源位置](../artifacts/benchmarks/workflow_speed_optimization_20261021/round25/location.json)
- [25 項檢查收據](../artifacts/operations/agent-workflow/runs/workflow-r25-private-writer-test-20261023T191343-c9d961fc/run.json)
- [十一份封存來源與 reversible script 邊界](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round25/source_manifest.json)
- [十三輸出與固定 SHA](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round25/input_manifest.json)
- [兩半比速判準與 timing scope](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round25/comparison_protocol.json)
- [空批次原生互通探測](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round25/runtime_probe.json)
- [361-byte 真實 native 發布與完整讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round25/preflight_readback.json)
- [timing 前 fresh memory admission 修訂](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round25/fresh_admission_amendment.json)
- [寫入前 guard 與真實小檔 preflight 修訂](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round25/private_preflight_amendment.json)
- [v3 修訂執行收據](../artifacts/operations/agent-workflow/runs/workflow-r25-pin-private-preflig-20261023T191624-79562a59/run.json)
- [原監督 owner 收據](../artifacts/operations/agent-workflow/runs/workflow-r25-writer-continuation-20261023T184608-cc4a5e8e/run.json)
