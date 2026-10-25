# 工作流測速 Round 13：完整靜態清冊與 TEJ 階段對應

本輪接續 [Round 12](workflow_speed_optimization_2026-10-23_bounded_reads.md)。
已完成三組各四次完整比較：大量檔案的暖快取契約不成立，原始碼傳輸的第一個
多程序候選超過 CPU 門檻，改為只傳路徑的候選通過相同門檻。
後者的完整靜態清冊平均從 21.308720 秒降至 6.820772 秒，減少 67.99%；
完整程序 CPU 增加 6.95%，屬明確的延遲與 CPU 取捨。
這不是整份清冊或整個交易／訓練工作流加速 67.99% 的證明。

完整原始紀錄保留於
`artifacts/benchmarks/workflow_speed_optimization_20261021/round13/`。

## 先消除資料搬運，再並行

固定靜態清冊含實際 Git 可見範圍的 2,319 個來源檔案、40,369,587 bytes、
1,035 個 entrypoints。在凍結時逐檔核對讀取前後 generation，保留原始 bytes、SHA
與固定 Git 路徑清單。A/B 都讀同一份凍結來源；包含檔案狀態、讀取、SHA、完整
AST 檢查、兩份輸出與新程序獨立讀回。固定清單取代兩邊相同的 live Git discovery，
因此不把 live Git 走訪本身列為這組受控速度收益。

第一個候選在父程序讀取、SHA 後，將原始碼 bytes 複製給四個 parser processes。
保留原始 byte-level oracle、錯誤、順序、flags 與 timing lines；不執行被檢查程式。
`code_protocol.json` 在測量前登錄 A/B/B/A 與門檻：完整階段 outer wall 至少減少
20%、完整 CPU 至多增加 10%、保守 parser RSS 上界不超過 2 GiB、兩輸出 bytes/SHA
相同。四次完整 CPU 平均 20.765743 → 23.178647 秒，增加 11.62%，**拒絕採用**。
雖然 wall 由 22.153528 → 9.028291 秒，仍不放寬門檻或選最快樣本。

第二個候選延續 canonical `visible_code`，讓同一子程序完成單份來源的讀取、
SHA 與解析，父程序只傳路徑並按輸入順序接收小型 metadata。
每批最多 32 個路徑、pending batches 最多 `workers * 2`；每個 active worker
同時持有一份來源與其 AST，queue 不存整批原始碼 bytes。
使用 explicit `spawn`，不繼承呼叫者既有 threads、native pools 或 CUDA 狀態。
pool 在該階段結束前關閉，沒有跨階段 parser owners。

`code_paths_protocol.json` 在新四次開始前登錄相同門檻、相同全部輸入與輸出 oracle。
原拒絕組不參與新平均，也不修改原始紀錄。新四筆全部如下：

| 順序 | 方式 | 完整 outer wall，秒 | 完整 CPU，秒 |
| --- | --- | ---: | ---: |
| A1 | 原同步 | 23.665031 | 22.154302 |
| B1 | 四程序，只傳路徑 | 7.121814 | 22.836241 |
| B2 | 四程序，只傳路徑 | 6.519729 | 21.058847 |
| A2 | 原同步 | 18.952409 | 18.887089 |
| A 平均 | 原同步 | 21.308720 | 20.520695 |
| B 平均 | 四程序，只傳路徑 | 6.820772 | 21.947544 |

保守 parser RSS 上界平均約 718.80 MiB，通過 2 GiB admission；這是
`parent peak + 5 * maximum waited-parser-child peak` 的寬鬆上界，
不是同時峰值或正式清冊整體記憶體變小的宣稱。
測試器使用 Linux child subreaper，並在取 CPU 前等待所有剩餘 children；本組沒有
需由 controller 收養的殘留 children，parser 與 tracker 由 worker 本身收回。
完整 CPU 包含啟動、解析、輸出與獨立 reader。
主機仍有正式背景服務，沒有獨佔 CPU 或冷磁碟的宣稱。

CLI 新增 `--code-inventory-workers 0|1|2|4`，預設維持 0。
本輪整合使用明確的 4。`collection.json` 另記 parser child CPU 與資源範圍；
既有 phase `cpu_seconds` 是父程序時鐘，不能把平行後較低的父程序 CPU 當成
總 CPU 減少。這組結果只支持已測的靜態清冊階段與所列資源取捨。

## 暖快取假設被實測拒絕

`warm_protocol.json` 固定 Round 12 的全部 372,550 份輸入、9,383,953,790 bytes、
107 explicit receipts；每次先完整讀取與 SHA/generation 核對所有檔案。
保留預讀耗時，也把預讀與設定計入完整 outer command；沒有清除全機 cache。
兩邊維持 read worker 1、batch 256 MiB、六輸出與獨立完整 reader。

門檻要求測量 body 的 `ru_inblock <= 16,384`（8 MiB 的 512-byte counter units）。
四次實測為 7,255,136、363,192、177,744、67,992，**四次皆不符合暖快取 admission**。
預讀分別花 179.405724、73.991275、60.708270、46.881779 秒。
這表示完整預讀仍不足以建立本次定義的穩定暖快取條件；block counters 不是純
I/O wait，也不據此猜測是特定背景服務或 WSL 回收政策造成。

全部六輸出與完整 SHA 讀回通過，但此組不支持新的穩定暖快取加速結論。
body 完整 CPU 平均 buffered/raw 為 176.214065/178.365879 秒；原始四筆與失敗 gate
都保留在 `warm_comparison.json`。不挑後三筆、不放寬 cache 門檻，也不將預讀成本
藏在外面。Round 12 原有 raw read 保留為前代實作，本輪未新增其速度採用宣稱。

原先考慮提前排除 `workflow_speed_` 路徑，但檢查 owner 後發現主要目錄已在
discovery 時剪枝，且更早略過檔案 resolution 可能改變 link-loop 的失敗行為。
此候選沒有實作或計入收益。

## 把 TEJ 的既有真實耗時接回來源

canonical `profile_tej_acquisition.py` 的 SQL 已選到穩定 `table_id`，舊匯出卻只有
顯示名稱。v6 現在保留 `table_id`、`task_id`、`active_attempt_id` 與 source scope
contract；不從顯示名稱猜 ID，也不重跑查詢。collector 綁定 exact registry ID、
fresh end-to-end basis 與存在的 task ID；舊 bridge-only／缺 ID 仍不綁來源。
同時納入 producer 已產生的 prebridge 與 guarded transport timings，保留精確
receipt locator。巢狀階段互相重疊，不能相加當作完整 wall。

新唯讀快照觀測 256 個完成任務，19 個非空、237 個空回、113 個 exported rows；
保留 5,888 筆階段 samples，精確綁定 5 個 TEJ 來源。
完整任務中位數 11.525880 秒、scope preparation 中位數 4.683951 秒、
transport admission 中位數 3.734345 秒；這些不同範圍的中位數不可相加。
快照查詢 0.115612 秒，沒有來源呼叫、queue 修改或曝光原始授權值。
這是回溯測速，不是相同輸入速度 A/B、原生歷史完整性、PIT 或全來源 E2E acceptance。

對 Round 12 封存 registry 的 delta 讀回，stage timing 聯集從 1,362 增至 1,367，
未綁定從 1,037 降至 1,032；分母 2,399 包含 group、credential、product 與 inventory
views，不是 2,399 個可執行 API。新完整清冊的即時分母與結果另以正式讀回為準。

受影響的四個測試模組共 288 項通過，包含 exact source/task identity、空回保留、
timing locator、generation/byte limits、JSON/native parity，以及真實 spawn 的
順序、SHA、語法錯誤和禁止執行來源。這不是全測試套件的宣稱。

## 完整整合與剩餘邊界

`final_collection_arguments.json` 保留前代完整範圍，換入 v6 TEJ 快照並明確指定
`--code-inventory-workers 4 --evidence-read-workers 1 --evidence-read-batch-mib 256`。
完整 13 階段、12 份輸出與兩次獨立 reader 均通過：237,675 samples、32,402 summary
groups、6,656 unique originals、375,638 evidence sources、372,589 benchmark receipts、
708,201 個尚未審閱 reported fields；issues/traversal gaps 都是 0。
registry 2,399 項中，1,367 有逐來源 stage timing，1,032 尚未綁定；完整逐來源 E2E
acceptance 仍是 0。靜態清冊階段實際為 8.195614 秒，另記 parser children 的
20.665511 秒 user CPU、2.361801 秒 system CPU；不是只用父程序 0.207299 秒當總 CPU。

本次完整 collector wall 為 619.915946 秒，結束於最終 file hashing／manifest write
之前；完整 command/readback 另有 owner receipt。前輪為 420.560390 秒，兩次來源、
cache 與主機負載不同；本次整體較慢，沒有宣稱整份清冊已加速。
最大階段是 inspection 491.607495 秒 wall / 245.100379 秒父程序 CPU；
aggregation/output 35.442758 秒、path selection 29.598542 秒、discovery 28.561781 秒。

包裝器完成全部收集、原始獨立讀回、source admission 與 `selected_full_adoption.json`
後，最後摘要將 `collection_wall_seconds` 寫成 `collector_wall_seconds`，因此 command
以 KeyError 結束。原程式、失敗 receipt 與 log 完整保留。
`readback_complete.py` 只讀回既有 payload，再執行一次正確的 canonical verify CLI，
沒有重收或修改輸出；`selected_full_adoption_readback.json` 為 verified。
task resolution 指向同一 task 的成功修復 run，區分包裝器失敗與資料驗證結果。

仍需依完整清冊最大成本繼續處理 inspection、path selection、aggregation/output，
並把其餘 producer 的 exact ID、完成工作量及完整接受證據補齊。
遠端 source/runtime/data/checkpoint 與 strict CUDA admission 沿前輪未通過的邊界保留；
本輪沒有啟動訓練、重啟正式服務或更改非交易日 GPU 行為。

`full_collector_speedup_verified=false`、`all_workflows_measured=false`、
`hardware_limit_proven=false` 持續保留。
