# 完整證據讀回的串流實驗：2026-10-24

此輪優先減少重複的檔案系統查詢，保留所有來源 bytes、SHA、解壓、報表及
獨立 summary 核對。候選已在 R26 整合，通過 712 項回歸及完整新 collector／
兩種 reader 驗收，尚未宣稱完整 collector 因果加速。

前輪完整 reader 診斷為 504.296 秒：`Path.resolve` 的 inclusive wall 約 260.294 秒，
gzip 開檔建構約 93.061 秒，JSON 解析約 1.312 秒。`lstat` 包含在 resolve 裡，
不能再相加。這是同一份 A1 證據的操作診斷；不是後續完整 13 階段觀測的基準時間。
瓶頸的第一性原理是 metadata RPC 次數與開檔等待，不能把改用 C++／Rust
視為固定倍率的加速。

## 候選及語意邊界

每個完整 reader 只固定開啟 output／evidence 目錄一次；對 canonical
`evidence/<sha256>.gz` 以目錄 FD 開檔，保留 no-follow、regular-file、完整串流、
前後 file／directory generation 及最後名稱空間檢查。解壓後仍逐塊計算全部 SHA
與 byte count，沒有內容快取或減少驗證。

非 canonical 名稱與指向 output 內的 symlink 使用原安全路徑邏輯；對外 symlink
仍拒絕。損毀 gzip、讀取中替換、FIFO 與原生例外均不可成為成功讀回。
候選只改原 reader 的 snapshot opener 與其 context；反向移除兩個明確變換後，
其餘驗證 AST 與原版逐節相同。六／十三份輸出、樣本分母、來源與 summary 規則
均照原邏輯執行。

## 已完成的驗證

- 八份當下來源在修改前封存；當時 canonical 程式尚未編輯。
- 既有 profile 測試改以候選 reader 執行，加上串流／檔案系統邊界測試，
  **149 passed**，完整 pytest 子程序 **8.115313 秒**。
- 首輪三項既有 selector 測試因 scratch 路徑含 `workflow_speed_` 而被既有
  排除規則擋下；僅修改測試 scratch 位置後通過。候選內容未改，原始腳本、
  JUnit、log 與失敗收據完整保留，沒有放寬原排除規則。
- 實際 guarded D 上兩次串流各 **4,456,448 bytes**，完整 SHA 相符，確認進入
  FD fast path 且正常關閉描述符。這是工程 smoke，沒有充當效能樣本。

## 完整比較的固定範圍

比較 driver 已備妥並交由 canonical supervisor 接續。實際 run
`workflow-r22-reader-continuation-20261023T171701-c52c2138` 已完成
`prior_seal` 等待：18:57:32 UTC 獨立核對 37,050 個封存檔案及 20 份 terminal
收據，原 owner 成功退出且 MainPID 歸零。protocol 隨後固定，18:58:01 UTC
由同一 continuation 啟動
`workflow-r22-full-reader-balance-20261023T185801-205f706d`，沒有另開重複比較。
原前置工作失敗、來源變動或容量不足都會停止接續，不會刪資料、改門檻或假定成功。
每輪為全新 Python 子程序，固定使用前輪完整 collector 的同一份觀測：
**13 份輸出、6,811 份原始 gzip、240,567 個樣本、27,555 個 summary groups**；
13 份報表合計 **794,193,072 bytes**。不重建、裁切或減少觀測。

A/B/B/A 與 B/A/A/B 分別計算，兩半都必須 whole-child wall B/A ≤ 0.98，
且 descendant CPU B/A ≤ 1.02。每輪必須與原完整 reader 的語意證明一致。
計時包含新程序、匯入、來源／磁碟 guard、完整 reader、proof 落盤及退出；
排隊與父層記錄另計。完成後再由另一個程序重算兩半門檻，並用原版 reader
完整讀回一次。額外讀回也不會取代計時八輪。

這個比較即使通過，證明的仍是完整 readback 工作，而非整個 collector 的
讀取→轉換→計算→輸出→讀回倍率。之後的 canonical 整合與完整 collector
接受仍須另留來源指紋、回歸及實際結果；沒有宣稱所有來源已測完或達到硬體極限。

## 目前證據

- [新 artifact root、admission 與來源封存](../artifacts/benchmarks/workflow_speed_optimization_20261021/round22/location.json)
- [可逆 AST 候選身分](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round22/candidate_manifest.json)
- [149 項語意及回歸結果](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round22/checks_v2.json)
- [實際 D 串流 smoke](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round22/native_d_stream_smoke.json)
- [接續程序的實際階段與原 supervisor 狀態](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round22/continuation_progress.json)
- [前輪全量結果及 reader 診斷](workflow_speed_optimization_2026-10-23_guarded_d.md)
- [原生 TW-public 時鐘及自然排程觀測](workflow_speed_optimization_2026-10-24_native_tw_public_clocks.md)

protocol、逐輪、比較與獨立讀回將寫入同一 root 的
`reader_comparison_protocol.json`、`reader_trial_*.json`、`reader_comparison.json`、
`reader_comparison_readback.json`，以實際檔案與 supervisor terminal state 為準。

## 第二十四輪：執行來源隔離的事前修訂

[canonical 日誌接入](workflow_speed_optimization_2026-10-24_canonical_journal.md)
已通過 696 項回歸及前後各四項來源隔離檢查。timing 尚未開始前，已保存舊
harness 並明列 v2 邊界：reader 一律執行原 archive，工作目錄 profile 限原版及
日誌接入版兩個已識別 SHA，其他七個 pin 不變。原 reader AST、candidate
function／helper、固定十三輸出與兩半 wall／CPU 門檻全部保留。
原 continuation 另等新全量 collector 的 terminal owner／驗收後才開始比速，
前置等待不納入 timing。此修訂沒有採用串流 reader 或宣稱新的加速結果。

## 八筆比較與整合準備

19:24 UTC 原比較 owner 已完成八筆，兩半均通過既定 wall／CPU 門檻。
慢樣本與所有後續樣本均保留；兩半絕對時間差異顯示共享環境／cache 影響，
因此分別列出，不將第一半倍率當作通用加速。

| 順序 | 原版 whole-child wall 均值 | 候選均值 | wall B/A | CPU B/A |
| --- | ---: | ---: | ---: | ---: |
| ABBA | 332.344605 s | 125.876988 s | 0.378754 | 0.607073 |
| BAAB | 183.380002 s | 137.898176 s | 0.751980 | 0.744211 |

額外原版 oracle run
`workflow-r22-full-reader-readbac-20261023T192422-85cdd554` 已成功，完整原版
呼叫 **370.693901 秒**，重算兩半判準並再次驗證全部輸出、原始 bytes 與
語意。原 continuation 也成功退出，兩個 actual unit 均 inactive／MainPID 0。
完整 R22 reader 候選已通過採用門檻。R25 比速已 terminal，兩半較慢而不採用；
R26 隨後完成 reader 整合、712 項回歸及完整新 collector／兩種 readback
驗收，尚未把局部 readback 結果視為完整 collector 加速。

- [八筆完整結果與兩半判準](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round22/reader_comparison.json)
- [原版額外 oracle 的實際收據](../artifacts/operations/agent-workflow/runs/workflow-r22-full-reader-readbac-20261023T192422-85cdd554/run.json)
- [獨立重算與原版完整讀回驗收](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round22/reader_comparison_readback.json)
- [原 continuation terminal 結果](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round22/continuation_result.json)

[已採用的整合 patch](workflow_speed_optimization_2026-10-24_reader_integration.md)
已核對 AST／可逆 bytes，並在原 R22／R25 owners 全部 terminal 後採用。
原比較來源與門檻保持不變，canonical 的新 source identity 由 R26 另行驗收。
