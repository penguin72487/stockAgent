# 工作流測速 Round 12：普通檔案的有界讀取

本輪接續 [Round 11](workflow_speed_optimization_2026-10-23_native_encoding.md)
的最大剩餘階段 `benchmark_evidence_inspection`。候選已在修正測試器生命週期後的
新四次完整確認通過預先登錄門檻，採用於 canonical collector CLI；首組 RSS
拒絕完整保留。新四次平均 CPU 減少 9.82%、outer wall 減少 19.85%，RSS 增加 0.27%。
第一筆基準的實體讀取較多，最後基準則接近候選；這些是有限觀測，尚未證明穩定延遲增益。
這不是全部來源、正式服務或遠端訓練已達硬體極限的證明。

完整原始紀錄位於
`artifacts/benchmarks/workflow_speed_optimization_20261021/round12/`。

## 原理與實作邊界

固定清單包含 372,550 份檔案、9,383,953,790 bytes、107 份 explicit receipts。
所有檔案仍完整讀取與 SHA 核對；保留 705,929 個報告欄位、2,006 個 ledger samples、
3,739 份 unique originals，以及六份輸出和獨立讀回。
固定輸入 manifest SHA256 為
`f4bb9b16c7d423b7dbb85bc1f1c61cb5e990596de2c2fe881672572b0959044d`。

普通檔案已有讀取前的大小 N。原先 `BufferedReader.read(N+1)` 常會再讀一次確認 EOF。
候選在既有 `Evidence._prepare_reads` 使用無緩衝讀取；取得 N bytes 後，仍由原 owner
核對讀取前後、消費前的 device/inode/size/mtime/ctime generation 與完整長度。
短讀會在 N+1 的讀取上限內補讀；截斷、成長、替換、symlink 更換及讀取失敗仍走原本
拒絕或同步重試。同步 explicit 路徑、解析、分類、壓縮、預算及輸出 owner 均保留。

read worker 維持 1、batch byte budget 維持 256 MiB；沒有改變金融計算、來源值、
timing 判定、C++ metric walker 或 JSON encoding。此處節省的是檔案讀取工作的包裝與
冗餘 syscall，無須更換交易或訓練演算法。

`file_read_syscalls.json` 是完成第一組比較後的機制診斷：20 份固定小型普通檔案，
buffered/raw 的 `read` 分別為 40/20 次，EOF read 為 20/0 次，內容、SHA、generation
一致。保留兩份 `.trace`；strace 耗時不參與速度門檻，不能把此 50% syscall 差異當成
完整流程 50% 加速。

## 完整比較與記憶體生命週期

比較順序為 A/B/B/A，每次新程序均包含 imports、設定、完整檢查、六份輸出與
獨立 reader。CPU 是被等待的整棵程序 user+system，加上 controller spawn/wait
期間 CPU；worker RSS 是該程序 lifetime peak，並非整棵程序同時峰值。
主機為 Penguin WSL2、Ryzen 7 5800X3D；保留 affinity、cgroup/headroom、主機負載與
I/O counters。沒有清除 page cache 或宣稱獨佔主機。

預先登錄門檻為：完整程序 CPU 不超過基準 98%、完整 outer wall 不超過 102%、
worker peak RSS 不超過 102%，且六份輸出 bytes/SHA 完全相等。

原始四筆 `balanced_comparison.json`：

| 平均指標 | Buffered | Raw |
| --- | ---: | ---: |
| 完整 outer wall，秒 | 302.198882 | 193.872610 |
| 完整程序 CPU，秒 | 215.566643 | 175.395610 |
| Worker peak RSS，bytes | 1,831,315,456 | 1,879,289,856 |

這組 CPU/wall 通過、RSS 增加約 2.62%，因此判定未採用，原始負結果完整保留。
最後基準有 692 次 major fault，且 RSS 明顯偏低；不能據此單獨歸因於程式。

檢查測試器後，發現它在建好 ordered paths/explicit IDs 後，仍持有已無使用的
249 MiB 序列化清單所對應的 decoded plan。`confirmation_worker.py` 對 A、B 同時
釋放該 plan，獨立 reader 仍重新載入並驗證完整清單。這是測試器的記憶體修正，
不能當成正式 collector 或服務的記憶體改善。

`confirmation_protocol.json` 在新測量前登錄同一門檻、唯一一組新四次確認，
原四筆不參與新 gate，也不選最快樣本。另記 setup/inspection/lifetime RSS。
新確認 `confirmation_comparison.json` 的全部四筆如下：

| 順序 | 版本 | 完整 outer wall，秒 | 完整程序 CPU，秒 | Worker RSS，MiB |
| --- | --- | ---: | ---: | ---: |
| A1 | Buffered | 218.606327 | 182.281218 | 1448.980469 |
| B1 | Raw | 141.930682 | 148.440652 | 1449.792969 |
| B2 | Raw | 150.502623 | 152.603434 | 1449.753906 |
| A2 | Buffered | 146.229189 | 151.548350 | 1442.898438 |

| 新四次平均 | Buffered | Raw | 差異 |
| --- | ---: | ---: | ---: |
| 完整 outer wall，秒 | 182.417758 | 146.216652 | -19.85% |
| 完整程序 CPU，秒 | 166.914784 | 150.522043 | -9.82% |
| Worker peak RSS，bytes | 1,516,177,408 | 1,520,197,632 | +0.27% |

三項門檻通過，四次六輸出全部同值。A1 的 `ru_inblock` 為 11,182,776，其他三次為
8、8、24；這是 block counter，不是邏輯 bytes 或純 I/O wait。A2 與兩次候選接近，
因此平均差異包含 cache/排程條件，不能歸因為已證明穩定的 19.85% 程式加速，
也不能與前輪百分比相乘。採用範圍是本輪 gate 與同值原理所支持的 collector 候選，
後續仍需在選定 cold/warm 契約下重測完整工作流。

## 語義、正式清冊與接受範圍

三個受影響測試模組共 215 項通過，包括新增的三種短讀長度，以及既有的 generation、
失敗、預算、順序、來源留存、非 timing 分類、native walker、JSON wire parity。
這是 focused semantic regression，不是完整 test suite 的宣稱。

正式 `collection_full` 已完整收集並由獨立 reader 驗證：235,868 samples、
26,568 summary groups、13 phases、12 outputs、6,681 unique originals；
708,233 個未審閱 reported fields、372,637 benchmark receipts、375,686 evidence sources，
issues/traversal gaps 均為 0。來源與最終參數綁定於 `source_disposition.json`、
`final_collection_arguments.json`、`selected_full_adoption.json`。

實際參數為 `--evidence-read-workers 1 --evidence-read-batch-mib 256`，確認有
372,336 次 prefetched reads；其餘 3,350 次讀取包含 registry/explicit 與同步 fallback。
CLI 未指定參數時仍預設同步，因此採用範圍是上述明確設定的 canonical 路徑。

完整清冊本次 collector wall 為 420.560390 秒，reader/coverage repair command 另為
21.746946 秒，不能相加當作受控速度 A/B。前輪 collector 為 316.599795 秒；
本次整份清冊較慢，來源集合與主機/cache 狀態不同，未證明完整清冊整體加速。
最大剩餘階段仍是 inspection：278.747374 s wall / 170.157340 s CPU；
其後為 aggregation/output 35.416203 s、path selection 34.451602 s、discovery 27.462416 s。
下一輪應從這些實際成本與明確 cold/warm 契約接續，不能套用固定子流程百分比。

collector 已成功後，最初 wrapper 的獨立讀回呼叫漏了必填 `--output-dir`，導致
wrapper 與其依賴 waiter 失敗；原收據與錯誤 log 保留。`recover_readback.py` 只以
正確 CLI 讀回既有完整清冊，沒有重做收集、修改 payload 或啟動第二份清冊，並通過
所有接受檢查。兩筆 wrapper 失敗已由同一 task 的 resolution 指向修復證據。

`remaining_workflow_coverage.json` 觀測 2,399 個 registry entries，1,362 有 stage timing，
1,037 尚未綁定，完整逐來源 E2E acceptance 仍為 0。registry 包含 group、product、
credential 與 inventory view，這不是 1,037 個可直接呼叫的 API。

原生 walker/encoder 的來源與 package 契約沿用 Round 11，沒有重建或安裝依賴。
本輪沒有重啟正式服務或改變非交易日 GPU 行為。

## 遠端 admission 與剩餘工作

先前遠端無法連線的觀測已更新：目前 enrolled SSH 可達，兩張 RTX 5090 的唯讀
NVML 觀測沒有 compute owners。最初本輪探測因本機 ControlPath 過長失敗；該負結果
保留，後續使用 canonical helper 與短私有 socket 目錄成功，完成後關閉自己建立的
control master，沒有更動遠端服務。

標準 `source scripts/runtime_env.sh` / `run_fintech_python` 選到的系統 Python 缺少
Torch、PyArrow、Polars，canonical strict CUDA/environment preflight exit 1。
因此尚未證明 initialized CUDA 或完整 fold。九個核心檔案有六個與本機逐 byte 相同，
`private_json.py` 與 metric walker 的兩個檔案不同；這不是整份來源或 checkpoint ABI
已相容。原始 stdout/stderr、來源 SHA、節點資料均保留，未啟動訓練。

下一步應依新讀取成本重測可行的 read worker/batch 候選，先量剩餘 stat/open/read/SHA
成本；繼續把來源 producer 的實際工作量、來源 ID 與完整 E2E receipt 綁定。
遠端需沿 canonical owner 核對固定 source/runtime/data/checkpoint 契約，再通過 CUDA、
容量與 ownership admission，才量完整雙 GPU epoch/fold。

`all_workflows_measured=false`、`hardware_limit_proven=false` 持續保留。
