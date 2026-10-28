# WSL 空間清點與安全回收：2026-10-27

這是一次持續中的線上清點，不是凍結快照、完整遷移證據或刪除清單。
Penguin 的 D 槽仍是本機 immutable 歷史資料的正式冷儲藏；WSL 保留下載增量、
控制資料、帳本及有讀寫者的工作集。每個舊來源都必須各自證明能從固定 D
版本完整還原，並通過當前讀寫者及既有回收 owner 的關卡。

## 實際已掃到的專案項目

以下為 2026-10-27 21:23–21:28 台北時間的線上觀察。以同一個 `du` 程序的
allocated blocks 計算並全域去重硬連結；目錄中的直接子樹均已輸出，但掃描時
其他服務仍可能寫入，所以這些數字不是一致時間點快照，亦不是可回收量。
整台 WSL 的其他範圍還在掃描；不能將此表直接與舊面板數字相加。

| 路徑，均在 `/root/stockAgent/artifacts/` 下 | 約 GiB | 分類和處理方向 |
| --- | ---: | --- |
| `benchmarks` | 214.34 | 完成的個別測試工作副本可作為候選；先保留可驗收成果、逐批核對來源、冷還原及使用狀態。 |
| `cache` | 161.17 | 混有分鐘、當沖及線上服務快取；由各 cache owner 判斷實際讀者、pin、lease 和可重建性，不能整目錄刪除。 |
| `live` | 119.34 | 正在使用的帳本、執行狀態與歷史回放混放；現行帳本保留，封閉歷史須逐批證明。 |
| `operations` | 111.60 | 包含回執、journal、工作暫存及歷史作業；以 owner、流程狀態和恢復證據分類，不能按名稱刪。 |
| `markets` | 49.20 | 研究／市場資料與服務讀取需逐項釐清；保留尚未證實的資料。 |
| `data_preparation` | 33.59 | 輸入與衍生輸出混合；需先核對讀者、來源及可重建性。 |
| `data_quality` | 23.20 | 檢核資料與證據，需辨別可重建檔案與唯一結果。 |
| `maintenance` | 23.09 | 逐項核對保全、修復與失敗流程。 |
| `code_releases` | 17.65 | 可能包含使用中的部署版本或唯一程式證據，逐版本核對。 |
| `live_signals` | 10.22 | 保存線上訊號／執行歷史；與現行服務分開核對。 |
| `candidates` | 10.06 | 舊候選模型或實驗須確認成果、來源及使用狀況。 |

上述 `artifacts` 整體同一輪已掃到約 794.43 GiB；表中只列較大的項目，
不包含尚未掃到的專案來源及其他 WSL 路徑。`cache` 中分鐘快取
`tw_minute_paper_parity_09bedd96a2f68c39` 約 119.84 GiB；之前的一次候選
dry run 只涵蓋 16 個支援且完成的 generation，未找到重複群組，另有三個
不完整 generation，不代表整個分鐘快取均完成去重驗證。

全機掃描 run 為 `wsl-complete-usage-census-20261027T131757-c5d44689`，
工作目錄 `/var/lib/stockagent/wsl-storage-inventory-20261027T1318Z`。掃描以
低優先級讀檔案系統 metadata，排除不同裝置及其他掛載點，不讀 payload、不
修改來源；其結果明列 `authorizes_retirement: false`。單一大型子樹未輸出
前不計入目前觀察值，已輸出的目錄亦可能在掃描期間變動。`planned_roots`
是切分後的掃描項目，不可直接換算為剩餘時間或掃描百分比。

可開啟 [目前分類清冊](/var/lib/stockagent/wsl-storage-inventory-20261027T1318Z/inventory_summary.md)
及 [全部已計畫路徑 CSV](/var/lib/stockagent/wsl-storage-inventory-20261027T1318Z/path_usage.csv)。
`wsl-census-report-refresh-v4-20261027T141352-a7eab16d` 每 60 秒檢查一次原
掃描收據；只有紀錄大小或 run 狀態改變時才重新輸出，不重掃資料。未走訪
的路徑顯示「尚未量測」，只走訪部分的目錄標示「局部」。硬連結省略只在
原掃描結束並逐一核對後計入已處理。這是目前開機上的監督程序，不保證
重開機後自動恢復；原掃描失敗或中斷也不會冒稱全機完成。

截至 22:00，本次凍結計畫已掃過 `data_finmind` 與 `data_finlab`，並正掃描
`data_openBB`。`data_finmind` 的 `USStockPriceMinute` 第一層有 14,128 個
雜湊目錄，每個可再包含多個日期。目錄數不是股票數，也不能由此推算剩餘
完成時間。程式已調整**下一次**
清點：先列其他來源及服務，再對幾個大型 FinMind 類別逐雜湊子目錄輸出，
整輪仍共用同一 `du` 的 inode 去重集合。原本已在執行的程序沿用原凍結
計畫、不重新啟動；新排序已以跨目錄硬連結及進度測試驗證。

較早的既有面板清點於 19:41 台北時間結束，33 個登錄根只完成 32 個、整體
partial；當時 `/srv/stockagent-live` 約 146.69 GiB、
`/var/lib/stockagent` 約 106.22 GiB、`/srv/stockagent-artifacts-hot` 約
87.24 GiB、`/srv/stockagent-packed-materialized` 約 44.71 GiB。
這些是不同時間、不同涵蓋範圍的觀察，不與上表相加，也不是可刪量。
同一次 partial 面板在專案來源中觀察到 `data_news` 約 14.42 GiB；專案根曾
逾時，這只是當時已看到的數量，不能解讀為目前完整大小。

新的單一 `du` 掃描於 21:18 開始，截至 22:00 已完成下列專案第一層根的
走訪。這是同一輪、非原子線上觀察；若不同根存在硬連結，空間會歸屬於
先掃到的根，表內項目不可直接視為每個目錄獨立的實體副本。

| 專案路徑 | 已觀察占用 GiB | 處理方向 |
| --- | ---: | --- |
| `artifacts` | 794.43 | 詳見上表，工作成果、快取及記錄依各自 owner 分批審查。 |
| `data_finmind` | 168.59 | 下載中的原始分區、收據與工作狀態；須先核對精確 D 版本及讀寫者後，才能逐批退休來源。 |
| `data_finlab` | 149.91 | 大表、分鐘及歷史分區；須先完成原值還原與下載器、reader 切換。 |
| `data_binance` | 135.28 | 加密貨幣來源分區，區分增量、服務讀取與已封存歷史。 |
| `data_binance_archive` | 92.25 | 歷史來源，逐批確認固定 D 重建、讀者切換與保留權限。 |
| `data_coinmetrics_community` | 16.06 | 原始來源與增量狀態須分別驗證，不以重新下載作刪除證據。 |

部分專案根、`/srv`、`/var` 和其他 WSL 路徑仍未走訪完；上表不是全機
總數或已確認可刪除的數量。
`df` 的檔案系統用量與本次 `du` 的可見命名空間也可能因保留區、已刪但仍被
程序持有的檔案或被掛載點覆蓋的路徑而不同，差額需另外調查。

## 明確候選與保留原因

| 範圍 | 固定清冊大小 | 本次狀態 |
| --- | ---: | --- |
| `benchmarks/workflow_speed_optimization_20261021/round11` | 12.48 GiB | 既有 owner 正在重做固定 D 版本獨立還原。尚未產生本次完整退休收據，原來源仍在。 |
| `benchmarks/workflow_speed_optimization_20261021/round7` | 13.51 GiB | 原來源仍在；目前為互不重疊的獨立容量等待，見後續 23:50 更新。上一個舊嘗試曾因 Windows interop 未驗證而暫緩，不能把舊 progress 當成本次結果。 |
| `benchmarks/workflow_speed_optimization_20261021/round6` | 9.20 GiB | 舊清冊有 `non-regular-or-cross-filesystem`，歷史檢查發現符號連結；尚不能退休，需先釐清並重做清冊。 |
| `benchmarks/running_services_20261013/taifex_history_columns_fixture_20261014` | 4.68 GiB | 舊清冊缺支援的完成 producer 證據；保留並釐清，不因名稱判定可刪。 |
| `benchmarks/workflow_speed_optimization_20261021/round4` | 掃描約 26.90 GiB | 只有空間觀察，尚未完成逐批來源、完成證據與冷恢復審核。 |
| `/srv/stockagent-packed-materialized/tw-public/.tw-public-20260825T011559079640521Z-l0-penguin-d00c44591f4a85b7.partial.568b88100e5348efb53cd81b451a55b7` | 約 5.25 GiB | 固定冷版本缺 object；既有處置文件沒有授權刪除來源或 metadata，保留。 |
| `/lost+found` | 約 2.6 GiB | 來源與冷恢復性未知，包含有大量資料列的 Parquet；保留。 |

上述 round11／round7 大小是固定清冊記載的 unique-inode allocation，僅為
候選量，不保證最終可回收；硬連結、程序與實際最後 unlink 均會影響結果。
round8 舊工作副本及 `operations/taifex_web_repair_20261013` 先前已退休，
收據分別記錄回收 13,010,243,584 和 4,036,501,504 bytes；本次再次觀察來源
均不存在，不再計入待清理。這是先前完成的回收，不是本次新增回收量。
22:15 在目前受 systemd 監督的環境中進行一次 Windows interop 唯讀呼叫並
成功，對應 run `wsl-interop-readonly-probe-20261027T141450-a2b9f779`；
這只證明當時 bridge 可用，不能代替 round7 後續實際的固定 D 讀回、
消費者檢查或退休結果。

FinLab 大表的固定 D 原值獨立還原、下載器／reader 切換及來源退休尚未全部
完成；FinMind 與其他來源也不能因已有冷庫或來源可重新下載，就把工作區
直接刪除。近期 materialization dry run 中有四個 pin 與一個活躍 lease，
未找到該輪可回收版本；其他範圍須用自己的 fresh dry run。

## 查詢與批次安排

```bash
cd /root/stockAgent
source scripts/runtime_env.sh

# 即時列出目前已輸出的 artifacts 分類；全機未結束時仍是局部觀察。
run_fintech_python scripts/audit_wsl_storage_usage.py progress \
  --output /var/lib/stockagent/wsl-storage-inventory-20261027T1318Z \
  --under /root/stockAgent/artifacts --max-depth 1 --limit 50

# 單獨看這次清點 run 的狀態，不把 systemd running 當作清點完成。
stockagent-agent run-status wsl-complete-usage-census-20261027T131757-c5d44689

# 只讀規劃例子：兩批已在工作或等待，規劃器應將它們排除，不能重複啟動。
run_fintech_python scripts/plan_wsl_retirement_waves.py --parallelism 2 \
  --cohort artifacts/operations/storage-single-authority-20261025/workflow-round11.inventory.json \
           artifacts/operations/storage-single-authority-20261025/workflow-round11-retirement \
  --cohort artifacts/operations/storage-single-authority-20261025/workflow-round7.inventory.json \
           artifacts/operations/storage-single-authority-20261025/workflow-round7-retirement
```

規劃器比較實際來源和收據根的重疊、固定清冊的舊 blockers、現有回收鎖、
觀察得到的 active／waiting agent runs，並對同一 wave 加總估計空間。
`--parallelism 1` 對應串行規劃，`2` 或以上對應不同、互不重疊批次的
並行規劃。它僅輸出各批要交回既有 owner 的 `canonical_argv`，本身不啟動
壓縮或清理、不持有空間預留、不保證偵測到 agent 以外的所有程序。
容量 forecast 是排除明顯不足的粗估，不是壓縮上界或容量 reservation；仍須
重新查所有既有工作和 writer，並通過 canonical 的容量、來源及 consumer gate。

需要把多批交給既有 owner 執行時，可使用新的
[`run_wsl_retirement_waves.py`](../scripts/run_wsl_retirement_waves.py)。
不加 `--apply` 只讀預檢；每個 `--cohort` 都要提供**已凍結的既有清冊**與
專屬 receipt 目錄。把 `--parallelism` 設為 1 為嚴格串行，設為 2–8 可讓
不重疊批次的原 owner 並行。下列路徑是待換成實際已審核清冊的參數示意，
不是本次 round11／round7 的重複派送指令：

```bash
stockagent-agent run --name reviewed-retirement-waves storage-single-authority-20261025 -- \
  run_fintech_python scripts/run_wsl_retirement_waves.py --parallelism 2 \
  --cohort PATH_TO_INVENTORY_1 PATH_TO_RECEIPT_1 \
  --cohort PATH_TO_INVENTORY_2 PATH_TO_RECEIPT_2 --apply
```

執行器固定每批 inventory SHA，啟動原 owner 時再要求原 owner 在任何 receipt
目錄建立前核對 SHA；不能使用舊成功收據代替本次證明。每 wave 重新檢查
容量及目前觀察得到的 owner／監督工作，需在 authority 的 mount namespace
執行；任一輸入已在執行、等待中、互相重疊或無法核對時整批暫緩，避免跳過
上游資料。前一 wave 全部完成後，必須核對 owner exit、每項 `acceptance.json`、
inventory SHA、固定冷版本身分、回收計數及來源是否消失，下一 wave 才啟動。
執行器自身不壓縮、不發布、不刪檔，不另造 retirement owner；中斷後仍須由
原 journal 與 owner 接手。容量仍是保守觀察，不是跨所有 writer 的硬保留。

22:04 用 round11／round7 的原清冊執行一次現場預檢，正確偵測到原 owner 和
已排隊的後續工作，返回 75 並沒有啟動重複工作。合成測試涵蓋串行順序、
兩個不同 owner 並行、來源或收據重疊、容量不足、SHA 漂移、收據缺失及
source 重現；這些測試不代表真實批次已完成回收。

執行層沿用 `scripts/retire_local_offline_artifacts.py`：同一 inventory 的
items 順序處理，獨立且已核准的 cohort 可各由原 owner 在各自 receipt dir
下並行執行私有編碼與固定 D 的獨立還原；發布及退休時重新取得共同 owner
鎖並刷新完整證明。串行前一批須核對 run exit、inventory SHA、
`all_selected_retired` 和來源是否不存在；任一項失敗或不可觀測時後續批次
不啟動。不能以批次規劃輸出代替原 owner 的證明或另寫刪檔器。

2026-10-27 22:15 台北時間 Windows C 約剩 5.7 GiB，低於既有 25 GiB
門檻；WSL ext4 顯示約 269 GiB 可用，不等於 C 的實體可用容量。
21:38 時 C 約剩 16.8 GiB，期間下降原因尚未證實；21:51 取樣的 30 秒
程序寫入並不足以可靠歸因整段下降，仍須持續採樣，不能僅憑 process
累計值將空間成長歸給單一服務。22:06 的另一個 15 秒 Windows process 寫入
取樣亦無磁碟歸屬，且不含期間已結束的程序，不能證明先前 C 下降的原因。
22:07 依既有 Windows Temp 精確檢查重跑 orphan WSL swap 唯讀盤點，
0 個符合條件、0 bytes 候選，沒有刪除其他 Temp 內容。
22:12 的 20 秒取樣中 C 可用 bytes 下降 14,430,208，已登錄 WSL VHD 檔案
長度不變；VHD 檔案長度不等於實體配置，亦不足以推斷更長時間的成長來源。
現階段不新增大批壓縮工作，
保留正在執行的 round11、當時等待的 round7 及其他正式作業。WSL 內
回收空間後，Windows C 不一定同步增加；VHDX 壓縮是另一個離線維護步驟，
不能為此任意關閉服務。

冷庫原值驗證的預設暫存若位於 ext4 等本機磁碟，已改為在建立暫存樹前
沿用既有實體容量檢查。這台機器的 `/tmp` 是 tmpfs，採用原有 tmpfs 空間
檢查並明示「未驗證實體磁碟支援容量」；tmpfs 仍可能使用 swap，不能把
它的 free 值當作 C 槽容量保證。需要完整暫存及磁碟空間的後續批次應等
容量恢復或由現有 owner 按實際容量安全暫緩。新檢查適用後續載入的程序；
已在執行的 round11 未重啟，不表示它已套用此修改。

在前一輪有限負載測試下，同一組 128 檔完整編碼與驗證流程的 8 路和 2 路
逐檔編碼均慢於 1 路；這僅適用該檔案組與當時負載，不表示獨立 cohort
永遠不能並行。提高併行前須對完整保存／還原流程重新量測，避免延長共同鎖
等待而使證明過期。

另外以 round11 現有 D 端編碼工作樹中選出的 16 檔、88,449 bytes 做
唯讀 ABBA 小樣本；各方案均比對每檔完整 SHA，批次方案還檢查持有的
inode 及前後 generation。WSL 現行逐檔讀取分別為 2.775、0.575 秒；
既有 Windows 原生 16 檔批次讀取分別為 11.440、5.167 秒。這次小樣本
中的原生批次更慢，因此沒有切換執行中的退休程序，也不作為其他檔案
大小或更低系統負載的通用結論。這次只是驗證階段的讀取比較，並非
完整壓縮／回收端到端加速驗收。

### 22:38 驗證加速與容量等待

針對 round11 的 D 暫存樹做唯讀 ABBA 測試，結果限定以下樣本與當時的
並行負載：

| 工作 | 樣本 | 單線程兩次 | 4 線程兩次 | 證明範圍 |
| --- | --- | --- | --- | --- |
| 每檔 metadata 與 SHA-256 | 512 檔、6,957,786 bytes | 40.166、21.201 秒 | 5.505、5.927 秒 | 每檔結果相同；不含整樹掃描或整批回收 |
| 原值編碼讀取、雙向 hash 與前後 generation | 256 個 raw 檔 | 24.401、27.495 秒 | 5.465、14.460 秒 | 每檔結果相同；不含原始來源樹及整批回收 |
| 編碼與來源各自完整 hash 及前後 generation | 128 個 raw 檔、原值共 480,832 bytes | 13.690、1.618 秒 | 1.207、0.808 秒 | 已比對舊 manifest 的每檔 hash；快取／負載差異很大，不含整樹與回收 |

測得瓶頸是 WSL 到 D 的逐檔等待，並非可省略完整 hash。現已限制只對兩個
既有 D 暫存根下的精確 stage 啟用最多 4 個讀取工作者，每次最多送入 64
筆；大檔 native 讀取仍串行。冷庫每個物件與成員的獨立重建、編碼及原值
SHA、解碼前後的完整檔案比對、來源檔前後身分、整棵樹、運送與刪除前
檢查均保留。相同加速也用於回收前的完整來源稽核和既有隔離區續跑。
已在執行中的 round11 行程仍載入原程式，不會聲稱它已取得新速度。

45 個相關測試通過，包含冷物件毀損、解碼後改變編碼、原檔相同大小的
內容變動、缺檔及額外檔案；25 個波次規劃／執行測試通過，包含等待時的
容量變動、清冊變動與其他 owner 介入。另一次較大的舊版回歸在 656 個
通過後，遇到程序參照掃描中的 cwd／路徑變動而保守拒絕；該測試的同項
目標重跑通過。該次回歸隨後中斷，不能稱為全套通過。完成新版回收與
stage pruning 的共用生命週期回歸後，另有 136 個測試通過。

原 round7 等待者經核對 PID／啟動世代、無子程序、無持鎖且未執行 owner
後安全退場；回執為
`/var/lib/stockagent/wsl-cleanup-20261027/round7-capacity-waiter-handoff.json`。
新排程 `wsl-round7-capacity-parallel-20261027T143830-fb441e7c` 每 60 秒
檢查一次，清冊、owner 或其他非容量檢查變動就停止，而非盲目重試。
容量符合後，新排程會再用原 owner 與完整新鮮 preflight 處理 round7，
可以和不重疊的 round11 同時進行；共同發布／回收鎖及原有生命週期仍有效。
22:38 觀察到 C 約 16.2 GiB 可用，波次預檢的保守需求約 50.1 GiB，因此
新排程處於等待而非實際驗證。需求值為估算，不是磁碟硬預留；開始前會
再檢查。C 空間後來仍持續變動，以新排程日誌為準。

22:34 從 round11 行程當下開啟的檔案及未再次驗證的暫存 manifest 推得
約 54,522／59,686 個 payload 位於該 SHA 階段目前位置之前，約 91.4%；
22:43 同方法觀察為 58,417／59,686，約 97.9%。
這只是當時該階段的路徑位置估計，後面仍有目錄、解碼、完整來源稽核與
回收等工作，既不是整批百分比也不是已刪除的證據。兩批來源仍須看各自
的 acceptance、實際來源狀態及配置區塊；WSL 內空間釋放不保證 Windows
C 的 VHDX 實體檔案即時縮小。

### 23:50 驗證掃描及既有 D 工作區

逐檔校驗之外，完整 portable fingerprint、來源／冷庫 generation fingerprint
及解碼後編碼目錄清冊也會等待 DrvFs 的逐檔 metadata。它們都仍完整重掃；
僅讓同一目錄內的獨立 `lstat` 最多四路執行、每次最多排入 64 筆，指紋按
原順序計算。目錄讀取失敗及未知符號連結會直接拒絕；前後兩次檢查不共享
快取。遠端 private control 同時帶上相容的掃描函式，保留遠端原本的
`SnapshotError` 身分，避免舊 checkout 缺新介面。

在同一個 D 暫存子目錄、253 檔的唯讀 ABBA 小樣本：

| 完整子目錄觀測 | 單線程兩次 | 4 線程兩次 | 結果 |
| --- | --- | --- | --- |
| portable 及 stability fingerprint | 4.225、1.148 秒 | 0.534、0.509 秒 | 完整輸出相同 |
| 來源／冷庫 metadata fingerprint | 2.316、3.113 秒 | 0.475、0.560 秒 | 完整輸出相同 |
| 編碼目錄清冊 | 30.831、22.565 秒 | 4.795、4.343 秒 | 全部路徑集合相同 |
| 暫存區逐檔控制收據比對，128 檔 | 6.578、2.163 秒 | 0.558、0.441 秒 | 每份 JSON 與 manifest 項目相同 |
| stage 身分與實體空間，253 檔 | 8.200、4.839 秒 | 0.890、0.893 秒 | 全部身分及 5,095,424 bytes 一致 |

當時還有其他工作共用 I/O，速度差異包含系統負載及快取，不是整批
驗收時間或保證倍率。舊的 round11 仍在運行中、無法在安全交接邊界立即
載入新程式；23:50 其獨立還原的原值解碼階段約 9,000／59,686 檔，後面
仍有完整來源稽核和回收，不是整批百分比。

round7 的原等待器在持有排程與該 cohort owner 鎖、確認 PID／開機世代、
精確參數、無子程序及等待容量後停止，回執為
`/var/lib/stockagent/wsl-cleanup-20261027/round7-fixed-d-waiter-handoff.json`。
第一個新受監督工作 `wsl-round7-fixed-d-capacity-20261027T154931-0bbb3b86`
每 60 秒重新檢查現存 D 工作樹、固定冷庫版本、原清冊及容量。此模式缺少
固定版本即停止，不建立另一份完整 C 暫存樹；保留 32 GiB 下限、逐檔
控制空間估算及最大冷物件的暫存空間。C／WSL 預估門檻從
53,801,899,470 bytes 降為 39,820,853,172 bytes，D 原預估門檻維持
73,244,060,572 bytes。這些是估算、並非硬預留；啟動前和原 owner 的
實體容量、安全與完整復原檢查仍會重新執行。

23:49 新等待器首次觀測 C 可用 36,260,151,296 bytes，仍低於新門檻；
當時 round7 未進入驗證與刪除。兩批在本次觀測仍沒有完成退休收據，
不能把理論候選大小當成已回收。固定工作區消失、版本或清冊變更、其他
owner 介入等均會暫緩而不是盲目重試。

之後發現從「容量已足夠」切換到取得 scheduler lock 的間隙，還需要再比對
等待期間固定的清冊與冷版本；已補上該檢查。第一個新等待器再次經相同
無子程序及雙鎖檢查後交接，回執為
`/var/lib/stockagent/wsl-cleanup-20261027/round7-fixed-d-pinned-handoff.json`；
目前工作是 `wsl-round7-fixed-d-pinned-20261027T155544-1d0c4e15`。

後續暫存區清理也已將完整逐檔收據核對、整棵 stage fingerprint 及隔離後
解碼改為同樣有界的讀取；本程序自己的精確 D quarantine 才能使用平行
讀取，所有獨立前後檢查、失敗保留 quarantine 與原 1,500 秒證明有效期
均維持。原本空間統計會另外重掃整棵樹；現在將其與同一次完整、最新的
stage 身分掃描合併，逐 inode 比對連結數與配置區塊，外部仍有硬連結的
資料不算成可釋放量；不同時間的完整 generation fence 仍各自重新掃描。
另排 `wsl-round11-safe-retry-20261027T155707-a01504ca`，它只等待
原 round11 結束，不持有 scheduler／retirement owner 鎖。原工作成功時須
獨立核對 acceptance 與來源消失；只有該次 progress 明確記錄
`owned legacy full verification expired or its generation changed` 且原清冊、
來源、冷庫版本仍相符，才用新程式排一次全新驗證，並照常等待容量。
未知錯誤、程序身分不符或版本變動均停止，不會無限重試。

目前 round7 控制收據目錄僅約 144 KiB，是未完成階段的實際大小，不能證明
後續最大寫入量；因此仍保留原本每檔 64 KiB 的保守估算，沒有為了立刻
啟動而降低這項保護。23:50 的 Windows orphan swap 重查是 0 個候選、
0 bytes，沒有刪除其他未辨識的 Temp 檔案。

23:59 立即執行既有線上 `fstrim.service`；服務回報 trim 了
12,612,706,304 bytes，但這是檔案系統 discard 的範圍，不是實際縮小的
VHDX 或退休資料量。同時量得 C 可用空間從 36,266,205,184 變為
36,247,908,352 bytes，因其他程序仍在寫入，不能將差額歸因於單一操作，
也不能宣稱 C 已因此釋放空間；既有每小時 timer 維持啟用。

本次相關驗證包括 packed snapshots 342 項、波次規劃／排程 39 項、
固定工作區與 owner 重試的重點測試 18 項，以及目錄清冊、毀損／變動和
遠端舊介面的重點測試；暫存區回收完整測試 69 項、已有其他共享生命週期
測試 136 項通過。另一次較大的背景回歸為降低同時進行的磁碟壓力，在
36 項通過後主動中斷，不能視為全套通過。本次也未聲稱完整實際回收
生命週期已完成。

## 2026-10-28 進度、查檔優化與容量觀察

截至 07:11（台北時間），round11 已完成固定 D 版本的獨立原值還原；
59,686 個原值檔案的解碼完成，之後仍須完成來源、冷庫及 mirror 完整稽核、
consumer／transport gate、退休與最終讀回。當前 owner 仍在完整稽核，
`acceptance.json` 尚未產生，本批尚無實際回收量。round7 在原 owner 與既有
安全容量條件下等待；C 槽約有 15.7 GB，低於該批約 39.8 GB 的預估門檻。
候選 12.48／13.51 GiB 是固定清冊的觀察，不保證實際釋放量，也無法據此
推算整批完成時間。舊的 `failure.json` 是前次嘗試，不是本次錯誤。

全機清點已退出，並非仍在掃描：17,601 個排定輸出均有紀錄，但 `du` 在
掃描期間遇到一個已消失的 `/var/tmp/systemd-private-*` 目錄而返回 1，
因此本輪是 `partial`、agent 以 75 結束。這是線上粗估而非一致快照或
刪除證明；清冊原始 stderr 和結果仍保留，不能標記為全部驗收。

Windows 側的 orphan WSL swap 稽核本次為 0 候選；唯一可辨識的現用 swap
屬於現行 VM，予以保留。`stockagent-data gc --dry-run` 檢查 5 份受管版本，
4 份 pin、1 份 lease 仍有效，無可回收版本。未移除 source、模型、帳本、
失敗／未知暫存或保護中的快取。

本次以套件管理器清除可再下載的 APT／pip 快取，並以 `npm cache verify`
回收其未引用項。量得這三個快取目錄的 allocated bytes 合計減少
168,501,248 bytes；APT 可能另清 pkgcache，未單獨量得。WSL `df` 在 APT／
pip 前後增加 236,220,416 bytes，但同時有其他程序寫入，不能將差額全部
歸因於該操作，也不代表 Windows C 一定增加同樣數量。
另以不使用 `--force` 的 `uv cache prune` 清掉 66 個未引用項，該目錄
allocated bytes 再減少 749,568；工具原有的使用中檢查維持不變。

07:10 的兩次 Windows 實測，WSL VHDX 檔案長度與 Windows 配置量均為
2,955,510,022,144 bytes，屬一般 `Archive`、非 sparse；同一現用 swap 為
33,457,963,008 bytes。約 15 秒的兩次觀察中，兩檔大小皆不變，C 可用
量下降約 12.4 MB；不能據此判定整夜下降的來源。刪除 ext4 檔案只會先
增加 guest 可用量。現有 hourly `fstrim.timer` 及 Windows
`StockAgent WSL Offline Compaction` 分別仍在；後者只在 WSL 已停止時
壓縮，未中斷正在執行的服務，亦不保證回收特定數量的 C 實體空間。

核對新的四工作執行緒 D 編碼驗證時，發現每筆 regular-file metadata 查詢
仍由共用快取鎖序列化。現在只在原有、有界的四工作執行緒編碼驗證中，
在鎖內複製已 pin 且 no-follow 的父目錄 FD，於鎖外重疊查詢；FD 快取仍
有界，額外 FD 至多為呼叫端正在執行的工作數。`recheck` 與 `close` 等待
所有未完成查詢後才繼續，仍核對每個目錄、每筆完整編碼／原值 hash、
後續 source／cold／quarantine 及 transport gate。普通 packed backup 的
既有序列化行為不變，執行中的 round11 也不會動態載入新程式。

實際 D 既有 stage 上抽取同 shard 的 128 個 regular 檔，4 workers、交替
順序測量，舊鎖內查詢為 3.220、2.295 秒，鎖外查詢為 0.664、0.878 秒，
逐檔 signature 全部一致；以上僅是該小範圍的 metadata 查詢，並非完整
source 稽核、還原、刪除的端到端結果，並可能受到 I/O 與快取影響。已
驗證查詢確實重疊、LRU 淘汰、關閉／recheck 等待、異常釋放、完整 archive
proof 等價及毀損 fail-closed。整批加速與退休仍待後續 owner 的完整收據。

同批 59,686 筆 manifest 使用 256 個 payload shard；以其檔案順序模擬
一次每檔目錄查找，原 64 FD 上限扣掉 root／payload 後可留約 62 個
shard，45,206 次 cache miss；在 worker 的 soft limit 為 1,024 時，
新版快取上限為 252 FD（含 root、payload），另預留最多 4 個 worker 的
暫時 dup；約可留 250 個 shard，模擬 miss 為 1,659 次。模擬不含平行執行順序和
其他磁碟負載，不能視為實際耗時。單執行緒和其他 backup reader 的既有
64 FD 行為不變；上限會隨該 process 的 soft FD limit 保守降低。

另在同一實際 D stage 選 192 個 shard 的原有檔案，各查四次、共 768 次，
4 workers，交替量測並包含所有已觀察目錄的身分 recheck：64 FD 上限分別
13.091、7.001 秒，252 FD 上限分別 4.140、3.732 秒，逐次 signature
一致。這是人工安排的有界重複查檔，與實際完整還原、來源比對或退休的
順序與 I/O 負載不同；無法承諾整批具有相同加速倍率。更小的 128 shard、
各查兩次測量有一次結果不分勝負，亦保留此負面觀察。

本次平行 FD 修改後，packed backup、catalog capture、legacy archive 與
retirement 的一輪共 252 項通過；加入 FD budget 後，另完成 7 項 budget、
完整 archive proof／毀損和多執行緒 FD 淘汰測試，以及 stage prune 的
69 項測試。這些測試不代表真實 round11 或 round7 已經完成。

07:19 再量測 Windows C 可用 18,032,820,224 bytes；相對 07:11 有波動，
不能歸因於本次清理。仍比 round7 的 39,820,853,172-byte 安全啟動
估算少 21,788,032,948 bytes，原容量等待器和 32 GiB floor 維持不變。
套件快取清理觀察記錄於
`/var/lib/stockagent/wsl-cleanup-20261027/package-cache-clean-20261028.json`。

## 2026-10-28：容量等待解除與 FinMind 背壓

07:19 左右 Windows C 剩約 16.8 GiB，低於 round7 此次估算需要的
39,820,853,172 bytes，也低於部分下載器的 25 GiB 保留值；這是容量門檻
造成的等待，不是已證實的 mutex 死鎖。約 07:34，其他活動使 C 可用量
獨立增加至約 59 GiB，原 round7 自行離開容量等待。不能把這部分空間
歸因於以下封存轉移。

依使用者要求將兩份已閒置的歷史台股原始封存由 Windows Downloads 移到
`D:\OffloadedFromC\20261028\stockagent-legacy-tw-public-downloads`：

| 檔案 | 長度 bytes | 完整檔 SHA-256 |
| --- | ---: | --- |
| `data_tw_public_20260720.7z` | 6,447,110,389 | `cee110986a9c50d79688691dc79d2c7629ac1c7253bef2a13cf1bcd69fb15f37` |
| `data_tw_public_20260720.tar.zst` | 6,309,806,080 | `1481c4681fa9816b255023b79e855d7b6a91dc4edd34b9acc94a0e26e48341de` |

原始檔的排他開啟、檔案身分、大小、時間、hardlink 與各目的檔完整
SHA-256 重讀均通過，完成與待協調收據位於各 D 目的檔旁；之後透過同一
來源 handle 退休 C 原檔。兩檔合計 12,756,916,469 bytes，未假設不同
格式的內容相同。07:38 第二份處理後 C 約 71.1 GiB 可用；同時其他服務
仍有寫入，總剩餘空間不是全由本操作造成。收據證明封存檔位元組，
不證明其內部資料語義、已納入 immutable lake 或已完成 NAS 備份。
可重用的 `scripts/retire_windows_download_archive.ps1` 現在持有目的檔
排他 handle 至來源退休，減少讀回與刪除之間目的檔被換掉的風險；五項
Windows 原生測試已通過。現存 WSL VHDX 並非 sparse；回收 guest ext4
空間本身不能保證 C 實體容量立即增加。

FinMind complement 原先僅看 WSL guest 的可用空間，現在共用既有
`AcquisitionCapacitySampler`，在啟動、迴圈與派送新工作時同時檢查實際
Windows backing 容量；容量不足或讀不到時停止新派工，讓在途工作完成，
等待 60 秒再試。容量不足期間不拿未完整批次校正 ETA；遙測保留容量
數字、隱藏路徑。07:38 在佇列及預取均無在途工作時重啟 complement，
其狀態已讀回 `dispatch_contract_version=12`，並有新的工作派發；自由版
與 Sponsor 也在容量恢復後各自繼續運行。收到 `SIGTERM` 時釋放 process
pool 與鎖的修正及測試已完成；為不打斷新的在途請求，該最後關閉修正
等待下一次正常重啟載入。其後依使用者暫停下載器要求停止了該服務；
恢復啟動時會載入此修正，現在不自動恢復下載。

07:41:49 complement 新版回報 `USStockPriceMinute`、`SUGP`、2025-07-07
完成 7 筆；獨立讀取其 receipt 與 2,449-byte Parquet，檔案 SHA-256 符合
receipt。這只證明該次來源回應已保存，不代表全資料集已完整。

07:41 round7 在 `verify-materialized-files` 已校驗 5,532 / 78,394 個檔案，
但該數字不是最後驗收，回收計數仍為 0。round11 的獨立原件還原已完成，
仍在更長的來源／鏡像／D 稽核，尚未取得當前批次的退休 acceptance；
不可將候選大小報成已回收。兩條流程均沿用既有 recovery、引用及退休
gate，沒有降低檢查、也沒有依「可再下載」逕刪 FinMind 資料。
round11 的 Linux process I/O 讀取計數在後續觀察中由約 25.26 GB 增至
30.07 GB，顯示該段仍有 I/O 活動，但計數增加不等於稽核完成。

## 2026-10-28 07:44 之後：暫停來源抓取、保留驗證工作空間

使用者表示已暫停下載器；核對時仍看到 FinMind、FinLab、TEJ、新聞、
永豐歷史與加密貨幣增量程序／定時器活動，因此對具名歷史來源服務及
定時器送出停止，原狀態記錄於
`/var/lib/stockagent/wsl-cleanup-20261027/acquisition-pause-20261028.json`。
恢復應按該記錄逐項執行，不自動啟動；交易即時行情的服務未列入停止。
FinLab local refresh 和 registered intraday 的執行中工作以 SIGTERM 中止，
systemd 留下 `failed / signal`，必須保留並在將來重啟後核對各自收據，
不能把信號中斷當成來源資料錯誤或清除紀錄。

新增 `configs/data_sync/acquisition_capacity.json`，下載派工的額外驗證
空間門檻設為 64 GiB，與原 25 GiB cache 門檻取較大值；驗證者本身仍可
使用原有空間策略，不將下載優先於驗證。FinLab、FinMind 及已接共同
acquisition guard 的服務在下次派工時讀取此設定；永豐歷史兩條 runner
在每批開始前檢查，TEJ 在 owner 啟動前檢查，新聞正式 owner 的既有
min-free 設成同樣值。實際 07:45 查得 WSL 約 265.4 GiB、C 約 70.8 GiB，
通過 64 GiB 派工門檻；下載器仍保持暫停。這是可觀測的 admission floor，
不是已向檔案系統預配／鎖住的空間，不會阻止不受控的其他工作寫入，
永豐批次及 TEJ owner 的啟動檢查也不是逐筆硬預留。更大驗證批次仍須
按其需要另做容量 admission，不能承諾任何大小的工作都可在 64 GiB 完成。

07:51 的查核中，round7 的獨立冷庫重建已到 `verify-materialized-files`，
完成 28,834／78,394 個 regular file（36.8%），僅代表該子階段；後續
仍有來源、鏡像、引用與退休等完整安全檢查，不能用此百分比當作全程
進度。round11 仍在來源／鏡像／D 完整稽核，兩批當時的實際退休回收
皆未有成功收據。原固定清冊的候選 allocated size 合計約 26.0 GiB，
並不保證這些空間已回收，或驗證後都可以刪除。

原暫停服務與定時器中有 32 個項目的開機啟動設定仍為 enabled；本次
先記錄每一個設定，檢查所列 systemd unit 均沒有 `Also=` 連帶停用後，
將這 32 個具名項目停用。重查原列 44 個 unit 無仍在運行者，並且 32 個
項目不再是 enabled；未實際重開機驗證。開機設定前後的獨立收據在
`/var/lib/stockagent/wsl-cleanup-20261027/acquisition-pause-enablement-before-20261028.json`
及 `acquisition-pause-enablement-after-20261028.json`。之後若恢復，須按
原 enablement 和原 active 狀態分別還原，並檢查中斷工作，不得一律啟動。
這個控制範圍是具名歷史採集服務；即時行情及既有 FinLab／FinMind 配額
取樣保留，並非阻止所有手動或未列出的外部程式發起請求。

後續檢查發現台股來源事件 daemon 還會在偵測到變更時啟動原下載器，
且官方發布掃描、交易日曆、交易標的及盤前來源檢查的定時器會抓來源。
依使用者暫停下載器的要求，另保存其先前狀態，並停止來源 daemon 與
四個定時器、停用其 5 個開機項目；操作後確認原 9 個 unit 沒有活動中
或仍為 enabled 的目標。交易行情串流未停止，原 feature reconcile、
cold publisher 等非下載工作亦未納入此次暫停。
這也暫停台股盤前來源檢查、交易日曆及標的名單的更新，可能影響盤前
資料新鮮度或訊號；不得將其狀態視為今日交易來源已完成。先前與後續
狀態分別留在 `/var/lib/stockagent/wsl-cleanup-20261027/` 的
`tw-public-acquisition-pause-before-20261028.json` 與
`tw-public-acquisition-pause-after-20261028.json`。未做實際重開機測試；
配額取樣與未列舉的外部程式仍可能發起少量請求。

補充驗證：acquisition capacity、FinLab、永豐排程及新聞等 122 項測試
通過；永豐和 TEJ runner 缺空間時不開始來源工作的 3 項行為測試通過；
FinMind complement、object／JSON pipeline、free／sponsor 與平順停止
共 192 項測試通過。Windows 封存轉移另有 5 項原生測試；這些測試不等於
上述實際回收作業已完成。

## 2026-10-28 08:19：當沖資料優先於驗證與大量下載

上節的台股來源暫停只是當時狀態；使用者後來明確要求當沖服務優先。
守護程式已恢復來源事件、發布掃描與盤前檢查，另依原啟用狀態恢復
交易日曆和股票標的更新。今日資格收據狀態為 `ok`，08:20 的官方
標的更新有 2,315 檔可交易；這些各自的收據並不證明所有盤前驗收已
完成。其他大量來源採集仍暫停，且暫停控制只影響具名的背景維護
啟動，不能阻止當沖來源、盤前檢查、日曆或標的名單。背景暫停不是
檔案系統容量的硬預留。

盤前驗收發現 10/23 休市卻保留了先前發布的當沖資格快照，造成特徵
建置把快照誤當缺失的行情日。建置器現在只排除由完整、SHA 已驗證的
官方 TAIEX 實際交易日收據證明的歷史非交易日；保留原始快照，交易日
缺行情和日曆證據不明仍報錯。時間檢查亦改用已啟動的 Windows 進程
逐次交換時間，並把往返的不確定度列入門檻，避免把進程啟動延遲誤判
為時鐘偏差或進行錯誤修時。相關測試已通過；08:23 開始的新盤前驗收
仍在執行，不以測試或程序活動冒充實際完成。

round7 和 round11 的既有進程維持運行，將其各執行緒降至 nice 19、
I/O idle 類別，優先讓出爭用資源；實際 I/O 排程效果仍受 WSL／Windows
實作影響。盤前來源驗收的 systemd 定義已改至一般系統 slice，無硬性
CPU／記憶體額度，權重高於背景工作、低於即時執行與 Discord；正在
執行的一輪沒有重啟，其執行緒 nice 已由 10 提升為 0，而 slice 移動
需待下次正常啟動。盤前檢查原本在 08:40–09:10 連必須重建的資料與
缺少驗收時都延後；新版將這兩種必要工作留在既有讀寫鎖下執行，只有
可選工作可延後。這不解除一致性鎖，長時間重建仍可能等待或影響同一
來源讀取；既有進程需自然完成後，新版才會由下一輪載入。
08:25 round7 的 `decode-originals` 已處理約
59,141／78,393 個項目，非整批完成度；round11 獨立還原已完成，完整
來源／鏡像／D 稽核仍在進行。兩批均未有實際退休回收成功收據。
