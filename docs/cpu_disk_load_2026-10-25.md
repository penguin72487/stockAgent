# CPU／磁碟工作效率：2026-10-25 至 10-26

本輪處理 Penguin 主機當下的負載，保留原資料工作、來源檢查與正式服務 owner。
**10-26 使用者明確否決以限速降載。新增的 CPU／SSD I/O 上限與監控降優先序已撤回，
另解除七個既有服務的 CPU 配額；目標是縮短全部工作的完整完成時間。**
保留備份可用記憶體的提高、日期位圖解碼重用，並加入私有 catalog 快取的原生解碼。
10-26 上午另減少盤點的重複路徑處理與內容未變時的整份快取重寫；固定輸入結果見下節。
下面保留被撤回方案的原始觀測，不能用其較低的使用率當作加速證據。
尚未證實所有軟體瓶頸已解決或達到硬體極限。

## 當下負載的來源

23:44 起查核 16 個邏輯 CPU 的 WSL。最初 load average 為 38.84；
load 同時包含可執行工作與不可中斷等待，不能直接當成 CPU 使用百分比。
後續保存 procfs／cgroupfs 的前後原始計數、boot ID、PID start ticks、磁碟計數與壓力。
程序 CPU 百分比以單核心為 100%；短命子程序另由 cgroup 計數涵蓋。

| 實際來源 | 量測與機制 |
| --- | --- |
| 背景歷史／增量資料 | 三個 CEX 下載器和 FinMind 等工作可同時執行；追加 I/O 預算前，heavy-data 群組一個 15 秒窗口在根檔案系統讀約 70.21 MB/s、寫約 36.06 MB/s。 |
| 資料驗證與儲存 | 同一窗口之前的固定樣本中，期貨財務來源驗證約 1.61 核、lifecycle 約 1.19 核；它們有獨立 owner，沒有因台股休市自動停止。 |
| 資料監控 | 每 30 秒啟動的完整快照，繁忙幾輪耗 CPU 44.88–51.11 秒；單輪時間超過排程間隔時會接續執行。bar inventory 約占 18.44–25.20 秒。 |
| 備份的局部記憶體壓力 | 原 MemoryHigh=512 MiB、MemoryMax=1 GiB，觀察到約 2.5 GiB swap、數萬次 high 事件；當時主機仍有大量可回收記憶體。這種局部 reclaim 可增加 C 槽換頁。 |
| 其他可見工作 | `htop` 在一個固定窗口約用 0.41 核。Windows 樣本另看見 Code／dllhost；Linux 統計不包含全部 Windows 工作。 |

Windows 23:48:39 的原生計數：CPU 75%，C 槽 idle=0%，D 槽 idle=88%。
00:03:35 的另一個繁忙樣本：CPU 96%，C 槽 idle=0%，D 槽 idle=99%。
因此本輪主要磁碟壓力在 C 槽；不同時段的數字也顯示工作量仍在變動。
`PercentDiskTime` 可因佇列超過 100，這裡以 idle／佇列與流量分別解讀。
DrvFS 的 D 槽不會完整反映在 Linux block I/O，不能由 Linux 零計數推論 D 槽閒置。

證據：[Windows 首份樣本](../artifacts/operations/cpu_disk_load_20261025/windows_before.json)、
[Windows 後份樣本](../artifacts/operations/cpu_disk_load_20261025/windows_after.json)、
[固定 Linux 窗口](../artifacts/operations/cpu_disk_load_20261025/before_memory_fix.json)、
[追加 I/O 預算前的繁忙窗口](../artifacts/operations/cpu_disk_load_20261025/final_observation.json)、
[完整快照階段計時](../artifacts/operations/cpu_disk_load_20261025/recent_snapshot_timings.json)。

## 已採用的修改

| Canonical owner | 修改 | 生效與取捨 |
| --- | --- | --- |
| `stockagent-heavy-data.slice` | **已撤回** CPUQuota=400% 與根檔案系統讀 32 MB/s、寫 16 MB/s 的上限。 | 核心獨立讀回 `cpu.max=max 100000`、`io.max` 為空；未重啟既有工作。既有記憶體保護與相對排程權重不代表這些硬性限速仍存在。 |
| `stockagent-backup-stream.service` | MemoryHigh=4 GiB、MemoryMax=6 GiB。 | 有界預算容納已觀察的工作集，避免原 512 MiB 軟限額反覆 reclaim。設定當下 MainPID 與 start ticks 不變；沒有 restart 或 swapoff。 |
| `stockagent-data-refresh-status-snapshot.service` | **已撤回** Nice=15、CPUWeight=10、IOWeight=10；目前 Nice=0、CPUWeight=100、IOWeight=100。保留 IOAccounting=true。 | 只保留 I/O 計數；不降低監控優先序、不增加排程間隔。 |
| storage-monitor、Temporal、lake-transport-gc、storage-lifecycle、control-release-verification、lifecycle-control-backup、packed-transport | 解除原有 30%–150% 的服務 CPU 配額，來源模板／Temporal 安裝器同步移除配額。 | 七個 live unit 均讀回 infinity；變更前後正在執行的 MainPID 不變，未重新執行安裝器。 |
| `bar_inventory._DateCensus` | 每輪最多重用 256 個已解碼日期位圖。 | 相同位圖的解碼是純函式；每個來源／manifest member 的原 identity 檢查、列數、null、pending、invalid 與日期聯集仍照常執行。快取隨該輪 census 釋放。 |
| `snapshot_data_refresh_services._catalog_snapshot` | 私有快取讀取沿用現有 `private_json.loads` 原生解碼與 stdlib fallback。 | 原鎖定、來源 builder、整份更新、原子發布與 JSON 寫入格式不變；沒有改寫公開 wire 或 canonical hash 的 encoder。 |

對應來源：
[背景群組](../deploy/systemd/stockagent-heavy-data.slice.in)、
[備份 unit](../deploy/systemd/stockagent-backup-stream.service.in)、
[監控 unit](../deploy/systemd/stockagent-data-refresh-status-snapshot.service.in)、
[日期盤點](../stockagent/live/bar_inventory.py)、
[canonical catalog 產生端](../scripts/snapshot_data_refresh_services.py)。

目前的撤回證據：
[新增限速撤回與核心讀回](../artifacts/operations/cpu_disk_load_20261025/throttling_withdrawal_20261026.json)、
[七個既有服務 CPU 配額解除](../artifacts/operations/cpu_disk_load_20261025/existing_cpu_quotas_removed_20261026.json)。
最後一次獨立讀回涵蓋當下載入的 **97 個 StockAgent service／slice**，均無 CPU 硬配額或
I/O 頻寬／IOPS 上限；所有可觀察執行中 cgroup 及其上層的 `cpu.max`／`io.max`
也未見這些上限。這個分母會隨短命工程 run 結束改變，不包含 Windows 程序或所有系統服務。
同一讀回用獨立 stdlib reader 重建兩批完整快取交易的輸出 SHA，16 輪皆一致；
八份渲染 unit 通過 `systemd-analyze verify`，Temporal 安裝器的 Python AST 檢查通過。
[獨立限制、等值與自然週期讀回](../artifacts/operations/cpu_disk_load_20261025/independent_unrestricted_cache_readback_20261026.json)。

下列為**已撤回設定的歷史證據**，不代表現況：
[CPU 預算與核心讀回](../artifacts/operations/cpu_disk_load_20261025/background_cpu_budget.json)、
[I/O 預算與核心讀回](../artifacts/operations/cpu_disk_load_20261025/background_io_budget.json)、
[備份前後程序身分](../artifacts/operations/cpu_disk_load_20261025/backup_memory_change.json)、
[監控優先序](../artifacts/operations/cpu_disk_load_20261025/monitor_priority_change.json)、
[unit 驗證與 nice 實測](../artifacts/operations/cpu_disk_load_20261025/final_unit_readback.json)。
最後一項讀回早於追加 I/O 限額；I/O 的實際核心值以獨立 I/O 收據為準。

撤回前，等待既有 timer 自然啟動下一批 CEX 工作後，完整 40.14 秒窗口量到背景群組
讀 28.01 MB/s、寫 8.86 MB/s，合計約 **36.88 MB/s**；相較限額前窗口的 106.27 MB/s，
來源工作與階段並非相同，不能當成同輸入的改善百分比。
該歷史窗口核心 `io.max` 是讀 32 MB/s、寫 16 MB/s，現已撤回，沒有另外啟動壓力測試。
同一後續窗口全機 Linux CPU busy 約 57.10%、iowait 約 26.54%；限速會增加背景等待，
不能把 CPU 轉成等待說成計算加速，或宣稱 C 槽已完全解除繁忙。
[自然批次原始計數](../artifacts/operations/cpu_disk_load_20261025/natural_batch_with_io_budget.json)、
[獨立 I/O 差值讀回](../artifacts/operations/cpu_disk_load_20261025/independent_io_budget_readback.json)。

## 等值與效能驗證

固定保留 46,609 筆完整日期位圖，25,454 種不同值；以 A/B/B/A、B/A/A/B 比較原解碼與
實作後的有界重用。每輪核對所有解碼整數的有序 SHA 與完整日期聯集；另一個 fresh
reader 直接用 base64／zlib／int 原始公式重算，8 輪全部一致。

| 固定完整位圖向量步驟 | 原實作平均 | 有界重用平均 | 減少 |
| --- | ---: | ---: | ---: |
| wall | 3.396378 秒 | 2.720307 秒 | 19.91% |
| CPU | 3.313791 秒 | 2.647126 秒 | 20.12% |

這個分母只有完整位圖向量解碼／聯集／有序讀回，不是完整監控或整台電腦。
沒有刪日期、跳過來源 identity，或縮小原盤點範圍。

證據：[固定輸入](../artifacts/operations/cpu_disk_load_20261025/fixed_date_bitmaps.json)、
[8 輪原始結果](../artifacts/operations/cpu_disk_load_20261025/fixed_bitmap_abba_baab.json)、
[獨立原公式讀回](../artifacts/operations/cpu_disk_load_20261025/independent_bitmap_readback.json)。
較早的單次篩選保留在
[初步測試](../artifacts/operations/cpu_disk_load_20261025/bitmap_decode_abba.json)，不替代已固定輸入的結果。

`test_bar_inventory`、`test_tick_inventory`、`test_backup_stream` 共 **84 passed**，
包含兩個檔案共享日期集合時仍各自計數，替換其中一個檔案會讀取其新日期的檢查。
三份渲染 unit 通過 `systemd-analyze verify`。
[正式測試 run](../artifacts/operations/agent-workflow/runs/focused-load-reduction-regressio-20261025T155702-936c83bb/run.json)。

### 10-26：私有 catalog 完整快取交易

以保留的 **31,159,024 bytes** 同一份 bar cache 為輸入，完整執行 canonical
鎖定、讀檔、解碼、更新、原子寫入、全檔讀回與逐位元組／SHA 驗證。
cycle 加一強制走完整發布；只有 A 的 stdlib decoder 與 B 的既有原生 decoder 不同。
每輪都重新複製原始 generation，保留未知欄位，原 JSON writer 完全不改。
這個分母包含整份快取交易，**不包含 provider builder、來源檔案盤點或公開狀態的完整產生**。

第一批 8 輪 A/B/B/A、B/A/A/B 曾與相關測試重疊，wall 波動較大；因此使用原來保留的
seed 再獨立確認一批 8 輪。兩批結果都保留，不以不同的即時來源輸入替換對照。

| 完整快取交易 | A wall 平均 | B wall 平均 | wall 減少 | A CPU 平均 | B CPU 平均 | CPU 減少 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 第一批 8 輪 | 1.790251 秒 | 1.655087 秒 | 7.55% | 1.655222 秒 | 1.386965 秒 | 16.21% |
| 相同 seed 確認 8 輪 | 2.527719 秒 | 1.965645 秒 | 22.24% | 2.141508 秒 | 1.754809 秒 | 18.06% |

原始輸入 SHA 為 `2ecf77b5e0e0539f1c57b7bb9d8a8fabe6c69f84493498df6ff10ea38983e912`；
16 次完整輸出逐位元組相同，輸出 SHA 均為
`3da293c8091af3cbec45034dcc275244a25479ff7c5aea52f80ca3a3ba3adbae`。
兩批來源程式 before／after fingerprint 相同；wall 是本機當時其他工作並行的實測，
不可把這些百分比乘到整個監控或所有工作上。

可重跑來源：[完整交易 benchmark](../scripts/benchmark_data_monitor_catalog_cache.py)。
證據：[第一批](../artifacts/operations/cpu_disk_load_20261025/catalog_complete_transaction_20261026.json)、
[相同 seed 確認](../artifacts/operations/cpu_disk_load_20261025/catalog_complete_transaction_confirmation_20261026.json)。

`test_tick_inventory`、`test_bar_inventory`、`test_private_json`、`test_data_monitor_record_codec`、
`test_data_monitor_inventory` 共 **249 passed**；涵蓋 native／fallback、超大整數、負零、
最小 subnormal、Unicode、未知欄位保留與壞快取重建。
[正式測試 run](../artifacts/operations/agent-workflow/runs/catalog-native-focused-regressio-20261025T163200-f4c5cc20/run.json)。

自然後續的完整監控仍輸出 2,401 個 sources、89,209 個 features；
[完整階段計時](../artifacts/operations/cpu_disk_load_20261025/post_change_snapshot_timings.json)
保存快取命中、刷新數與原有 deferred 狀態。這些輪次來源與 cache 狀態正在改變，
不能把其耗時變短全數歸因於本次修改。

撤回限速及 catalog 修改之後，既有 timer 自然完成的最新一輪仍輸出 2,401 個 sources、
89,209 個 features，完整 wall=36.564 秒、程序 CPU=32.046 秒，沒有手動 restart。
這是正常工作繼續運作的觀測，來源與快取持續變動，不能用來推算完整監控的 A/B 加速比，
也不證明來源已全部下載或資料完備。

| 該自然完整週期的可觀察步驟 | wall |
| --- | ---: |
| record inventory 全段 | 21.484 秒 |
| 其中快取解碼 | 4.152 秒 |
| 其中來源發現 | 6.555 秒 |
| 其中 signature scan | 3.746 秒 |
| 其中 aggregate／persist | 6.996 秒 |
| aggregate／persist 內的 encode、write | 2.342 秒、3.167 秒 |
| bar inventory 完整段 | 9.767 秒 |

這些內含步驟不能重複加總；本輪仍刷新 1,903 個檔案、命中 74,824 個檔案，
quick index 因 `source_hint_changed` 正確轉回完整檢查。下一個改善方向是縮短必要的
來源發現、identity 掃描與全量快取重建；不能拿掉來源變更檢查來製造較快的結果。
先前的 record struct GC 與 deferred non-null 實驗已有完整交易負面結果，沿用拒絕結果，
不因微步驟較快而重新採用，見
[既有完整交易實測](running_service_optimization_2026-10-13.md)。

## 10-26 上午：減少重複盤點與未變內容的全量重寫

第一性原理是區分「證明來源仍相同」和「再次製造相同結果」。來源列舉、檔案 identity、
manifest 成員 identity 與未驗證狀態仍須保留；重複建立／排序路徑物件、對同一來源重做日期
投影，以及重新編碼並寫出完全相同的 JSON，才是本輪減少的工作。

| Canonical owner | 本輪實作 |
| --- | --- |
| [record inventory](../stockagent/live/data_monitor_inventory.py) | 平面目錄以 `scandir` 取得名稱及檔案型態，先解決 legacy／manifest 選擇，再排序一次並建立 Path。跨層 glob 保留原路徑。每輪仍列舉目錄，沒有加入 TTL 或跳過來源 identity 檢查。 |
| 同一 record owner 的發布 | 缺失／損壞索引觸發完整檢查後，只有完整 payload 相同且舊快取 inode／時間／大小 identity 仍相同，才保留快取並修復索引。已知來源變動時直接發布，避免多做整份 payload 比較；有變的內容仍採既有原子替換。 |
| [bar inventory](../stockagent/live/bar_inventory.py) | 同一 census 內先查已完成的投影，跨群組共用；重用前仍重新檢查來源及 manifest 成員 identity。FinMind receipt／payload 路徑各解析一次，保留絕對路徑與 `..` 拒絕。 |
| [tick inventory](../stockagent/live/tick_inventory.py) | 單一路徑的 receipt 不再建立 set 並排序；多路徑仍完整去重及排序，全部 before／after identity 檢查保留。 |
| [正式快照產生端](../scripts/snapshot_data_refresh_services.py) | 日誌新增 `inventory_cache_payload_reused`，區別完整檢查後保留原檔、重新編碼發布及原有 quick-index 命中。 |

### 固定輸入與完整輸出比較

新增可重跑的 [inventory 工作測速](../scripts/benchmark_data_monitor_inventory_work.py)。原版
inventory 來源等於 Git `6b5384ace636ef49c1886bbf83f6bb8487d7ea68` 的檔案，SHA-256
`3ecff14c38e8a6f4a7ca19e7bd0ce34277483a80ffa9ed9023012e51c56fef25`。
保留實際 74,824 條來源的 stat／缺失／partition identity 觀測，以及 192,805,898 bytes
完整快取；每次從相同 bytes 開始。A 為原版，B 為新版，8 輪順序為 ABBA、BAAB，各版 4 輪。

| 比較範圍 | A 平均 wall | B 平均 wall | 實際減少的工作 |
| --- | ---: | ---: | --- |
| 實際目錄完整發現，8 輪 | 2.018 秒 | 0.871 秒 | wall 減少 56.8%，CPU 減少 56.7%；每輪有序來源 membership 完全相同。 |
| 凍結觀測重播：內容未變、索引須修復，完整 record→feature→JSON→讀回，8 輪 | 18.966 秒 | 14.466 秒 | wall 減少 23.7%，CPU 減少 23.8%；邏輯寫入由 192,993,079 降至 187,181 bytes，少寫的 192,805,898 bytes 正好是一整份快取。 |
| 凍結觀測重播：來源 identity 改變，最終版 8 輪 | 14.830 秒 | 13.418 秒 | 本批 wall 減少 9.5%；兩版仍各寫 192,805,866 bytes，沒有聲稱減少變動內容的全量寫入。 |

快取交易的計時包含完整解碼、所有來源 signature 檢查、聚合、原子快取／索引發布、特徵
計算與 JSON 序列化，以及完整輸出 bytes 讀回與雜湊。來源 stat／partition identity 是
保留觀測的重播，**不包含即時來源 I/O、目錄發現、Parquet footer 更新、完整服務公開投影、
網路或瀏覽器**；不能把這張表當成整台主機或所有監控輪次的加速比。兩版均設定
`max_refresh_files=0`，沒有僅對新版減少工作。邏輯寫入來自 `/proc/self/io` 的 `wchar`，
不代表 SSD 實際寫入放大或已回收磁碟空間。

每個情境內完整快取／特徵 bytes 與索引語意雜湊均一致；只排除每次暫存 inode 不同造成的
索引 signature 及其 checksum。修復情境每輪仍有 224,472 次來源 stat、374,120 次 identity
檢查、89,209 個特徵。變動情境只修改保留觀測的一個 `ctime_ns`，原始來源不動；兩版均有
149,649 次 stat、299,297 次 identity 檢查，均將該來源保留為未驗證、不發布驗證索引，
所以兩版特徵均為 89,195，不能和另一情境的 89,209 直接比較。所有情境都檢查全部
74,824 條來源，沒有因省略驗證而加速。

保留負面與拒絕結果：第一次 capture 因正式快取在擷取中推進而拒絕，沒有停止正式 writer。
第一批來源變動的 4 輪結果為 15.527→16.235 秒，慢 4.6%；後來對已知變動加入直接發布
短路，使用完全相同保留輸入完成上表最終 8 輪。不同批次的差異受當下競爭負載影響，
不能把全部變化歸因於這一個 guard。inventory owner 首批與最終版的差異只有該 guard，
兩個版本的完整來源 SHA 均保留；測速程式另增加重播與輸入釘選，亦各自記錄 SHA。
獨立讀回另外還原首批 inventory 程式 bytes 並核對雜湊。

證據：[首批完整比較](../artifacts/operations/inventory_work_20261026/full_work_abba_v2.json)、
[最終變動來源重播](../artifacts/operations/inventory_work_20261026/changed_source_final_abba.json)、
[獨立讀回](../artifacts/operations/inventory_work_20261026/independent_readback.json)、
[交付索引](../artifacts/operations/inventory_work_20261026/delivery.json)。證據與完整輸入留在
忽略的私有 artifacts；可維護的測速程式留在 scripts，不將數百 MB 快取加入 Git。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_data_monitor_inventory_work.py \
  --baseline-source artifacts/operations/inventory_work_20261026/data_monitor_inventory_baseline.py \
  --replay artifacts/operations/inventory_work_20261026/full_work_abba_v2.json \
  --case source_identity_changed \
  --output artifacts/operations/inventory_work_20261026/changed_source_new_run.json
```

### 語意驗證及正式運作讀回

共享 inventory／codec／feature／membership／bar／tick／partitioned Parquet 相關回歸共
**307 passed**，最終短路與來源替換 guard 的 focused 回歸 **125 passed**。這是兩組有重疊
的檢查，不把它們加總成不同測試數。另以只用標準庫的獨立腳本重建有序 membership 與
source observation root，讀回完整快取，核對 footer 值、schemas、完整雜湊和來源檢查次數。
舊 codec benchmark 現在會明確使私有 seed 的 prior revision 失效，強制真的編碼發布，
避免新功能合法跳過寫入而產生假的 codec 加速結果；不改正式資料。

正式 timer 未手動 restart，10:46、10:47 的自然完整輪次各為 15.483、16.886 秒，輸出仍為
2,401 個 sources、89,209 個 features，快取覆蓋 74,824 條來源；這兩輪走原有 quick index
且完整 signature probe 通過。它們證明新版正常運作，不能取代同輸入 A/B 或證明來源完備。
CPU quota 讀回仍為 infinity；本輪沒有新增 CPU／I/O 上限、調降優先序、延後排程、刪除來源
或重啟交易服務。

bar 診斷另保留 [完整 profile](../artifacts/operations/inventory_work_20261026/bar_profile_receipt.json)：
profile 含量測開銷及變動中的真實來源，不能當作加速比。該次仍有 76,157 項日期投影待後續
audit，未改既有 cold audit 預算，亦未把 pending 改為已完成。後續重點是變動快取的增量儲存、
receipt／目錄重複遍歷及來源驗證，須另以完整公開投影的固定輸入比較驗收。

## 10-26 第二輪：來源變動的增量發布及後續 CPU 瓶頸

本輪消除「少數來源變動，仍重新編碼並替換完整 record JSON」的儲存成本，並處理測量中
找到的相同 schema 重複正規化與同一 stat 的重複 identity 計算。這些修改沿用既有
inventory owner、writer flock、來源檢查、特徵發布及未驗證狀態，沒有新增資源限速。

### 儲存與讀取契約

[record store](../stockagent/live/data_monitor_record_store.py) 增加獨立的 storage v1：歷史
`record_inventory_cache.json` 可為舊完整 JSON，或只描述本機 SQLite 檔案的小型 JSON。
**邏輯 footer/schema 契約仍為 v10，公開 JSON 不變**。正式 producer 明確選用
`incremental_store=True`，小型快取保留舊方式；已有大快取只遷移一次。舊 quick index
即使有效，也會在這次遷移時完整重驗證，不必製造來源變動或破壞索引。

SQLite 的 metadata 與檔案新增／更新／刪除在同一個 DELETE-journal、FULL-sync 交易內
發布；沒有 WAL 或無限增長的 delta chain，亦不需週期性全量 JSON 重寫。未變的檔案列
保留原物件，新增／更新列才編碼寫入。新物件即使 Python `==` 相等仍會寫入，保留
signed zero、bool/int 等值域差異。來源版本被替換時不能套用舊 delta，改作完整重建。

所有直接讀取端與離線測速工具改用共用 reader／完整 JSON export。快速索引、feature
receipt 的來源 signature 改綁實際資料庫五項 identity，不能只看不變的小 descriptor。
descriptor 只允許嚴格本機檔名；缺檔、symlink、版本不符、損壞資料庫不當成已驗證空資料。
舊 JSON 可直接讀取。回退時須停用 producer 的新遷移選項，持既有 writer flock，由當前
完整交易讀回 payload，沿原 encoder／temporary／atomic replace 寫回完整 JSON；不能把
舊備份覆蓋成新資料。原資料庫保留，下一輪重新驗證索引，不以回退授權來源清理。

### 繼續消除實際 CPU 工作

| 新發現的成本 | 採用方式與語意 |
| --- | --- |
| 逐列解碼 74,824 個小 JSON 所造成的 Python 開銷 | 交易內組成完整文件後批次解碼，保留全部值及舊非有限值行為；這仍有完整讀取成本，不宣稱變成只讀 delta。 |
| 同一個普通檔案的 stat 被重複轉成 identity | 觀測 root 與相鄰 cache guard 共用該次純計算。每次來源 stat 仍保留；partition 的第二次 `resolve_source`／member `verify` 保留。 |
| 同 schema 的數千個寬表，每次刷新都建立相同欄位 pair list | canonical footer owner 已將完整欄位綁定 schema SHA；同輪每個新見 schema 只正規化一次。首份使用當前 footer，不直接信任舊快取。各檔欄位／null vector 仍完整讀取及保留。 |

### 完整固定觀測比較

仍使用上輪保留的 192,805,898 bytes 完整 seed、74,824 條來源及實際 stat／partition
觀測。另挑選 4,096 個最寬的已驗證普通來源，僅在私有 fixture 將 `ctime_ns` 加一；
footer 回傳值為完整保留的原始 count、bounds、schema、null vector，不捏造歷史數值，
不修改來源。兩版均完整刷新這 4,096 個 footer，並計算全部 89,209 個特徵。

比較完整 record→feature→JSON→完整讀回雜湊，A/B/B/A/B/A/A/B 各版四輪。A 是本輪
開始時已含上一輪優化的版本，不拿更舊的原版墊高加速比。B 是上述全部修改的合併結果。

| 8 輪平均 | A | B | 變化 |
| --- | ---: | ---: | ---: |
| 完整 wall | 50.661 秒 | 34.279 秒 | 減少 32.3% |
| 完整程序 CPU | 47.933 秒 | 32.438 秒 | 減少 32.3% |
| 邏輯寫入（含交易 journal） | 193,436,000 bytes | 58,976,784 bytes | 減少 69.5% |
| 每輪來源 stat | 228,568 次 | 228,568 次 | 全部保留 |
| identity 純計算 | 382,312 次 | 232,664 次 | 少做 149,648 次重複普通 identity 計算 |
| 實際更新的檔案列 | 全份 74,824 列 | 4,096 列 | 其餘列不重寫 |

A wall 範圍 47.461–52.723 秒，B 33.484–35.628 秒。兩版完整邏輯 cache、feature bytes
與索引語意雜湊相同；排除的仍只有不同暫存 inode 的 signature／checksum。這不是僅測
encoder 或單一 SQL statement。SQLite 初次建置平均約 7.60 秒，逐輪另記，未藏入穩態
加速；初始 DB 約 209.78 MB，獨立更新讀回的 DB 約 249.57 MB。這些不是磁碟回收量。

分段結果顯示 schema 共用的貢獻很大：signature／footer 處理由 24.098 降至 3.153 秒；
完整 encoder＋write 約 3.947 秒，新交易發布約 3.435 秒。**完整 cache 解碼反而由
4.076 增至 5.745 秒**，已計入上表；不可把全部 32.3% 都歸功於 SQLite。讀取端仍會
組成完整 metadata 文件，是下一項有量測支持的改善方向。

這是凍結來源觀測與保留 footer 的 CPU／快取完整交易比較，**不包含實際 footer I/O、
目錄發現、整份 public projection、網路或瀏覽器**。4,096 個最寬來源是明確壓力情境，
不能外推每輪監控或整台主機均快 32.3%。完整來源／版本／fixture／逐輪計時保留於
[8 輪結果](../artifacts/operations/inventory_delta_20261026/footer4096_bulk_abba.json) 與
[逐輪 checkpoint](../artifacts/operations/inventory_delta_20261026/footer4096_bulk_abba_trials.jsonl)。

負面結果也保留：第一個 fixture 多做 `deepcopy`，首個 A 耗 123.455 秒，已拒絕；修正
fixture 後的逐列 decoder 版本首個 A=48.357、兩個 B=54.377／48.911 秒，雖少寫但未穩定
較快，沒有部署。停止的只是這兩個自有工程 unit；原始 log、程式與拒絕原因留在
[證據目錄](../artifacts/operations/inventory_delta_20261026/)。正式服務未用這些版本。

### 獨立讀回與驗收

初版共享回歸 **432 passed**；批次解碼／schema 共用後的相關回歸 **239 passed**，最終
reader／API／欄位頁檢查 **126 passed**，公開儀表板／API 回歸 **279 passed**。這些組別
有重疊，不加總成不同測試數。包含原子失敗回滾、未提交程序退出、並行 reader 在 commit
期間保留同一代、descriptor 未變但 DB 推進的擷取拒絕、legacy／big integer／signed zero／
非有限值，以及 footer／partition mutation 檢查。

另從同一 seed 重做只更新 4,096 列的交易，由**全新程序直接用標準庫 sqlite3/json**讀取
所有列並執行 `integrity_check`，完整雜湊一致。獨立重建 membership／source observation
root，核對原始 count、bounds、null vectors、schema 均保留，見
[獨立讀回](../artifacts/operations/inventory_delta_20261026/independent_readback.json)。
正式 reader rollout、自然 timer 與 PID 連續性另記於
[交付收據](../artifacts/operations/inventory_delta_20261026/delivery.json)。公開 monitor 的既有
partial／invalid／pending 狀態保留；工程加速不代表資料已全部下載、驗證或訓練完成。

部署先載入相容 reader，只重啟只讀的 `stockagent-public-dashboards.service`，再啟用
producer。Discord、TAIFEX、日內與隔夜四個服務的 PID／啟動時間均未改變；既有 snapshot
timer 未重啟。11:56:47 開始的自然輪次完成一次全量遷移，發布耗時 9.143 秒；11:57:34
開始的下一輪遇到真實來源變動，只更新 **3／74,824** 筆檔案列，未重建 DB，交易發布
1.274 秒。metadata 仍完整更新，因此不是整輪只寫三筆資料或已達純 delta 成本。

後續自然輪次繼續完整 signature probe 並命中 quick index。12:02 的獨立讀回確認
descriptor 為 103 bytes、DB 為 249,135,104 bytes、logical version 10、來源 root 與
cache 相符，SQLite `integrity_check=ok`，canonical signature 綁定 DB。owner API 仍有
2,401 個來源與 89,209 個特徵；schema 覆蓋保留為 **74,805／74,824、partial**，沒有把
未驗證資料改成完成，見[自然運作讀回](../artifacts/operations/inventory_delta_20261026/writer_natural_readback.json)。
真實變動輪次完整耗時 45.680 秒，後續三輪為 25.208／21.862／25.078 秒；來源、快取路徑
及競爭負載不同，這些只證明正式運作，不拿來計算加速比。quota 仍為 infinity，沒有新增
CPU／I/O 限速或降低優先序。剩餘 public projection 中 bar inventory 約十餘秒，以及
完整 metadata 讀取／更新，仍需固定完整輸入另行改善。

## 10-26 第三輪：metadata 分列、完整讀取與日期投影

本輪接續第二輪明列的剩餘成本。當時正式 v1 metadata 為 39,323,444 bytes，其中
39,192,527 bytes 是 schema；舊 reader 先解碼這份 metadata，組成完整文件後再解碼一次。
只有來源 identity 改變時也會重新編碼／更新這整份 metadata。這不是必要來源驗證成本。

### 實作與相容部署

- private storage v2 把 schemas、datasets、dataset memberships 拆成獨立列，小型 root／
  layout 與變動檔案列仍在原本 DELETE-journal、FULL-sync 交易內發布。新值以實際編碼
  bytes 判定是否需要 SQL 更新，保留 bool/int、signed zero、非有限值與未知欄位。
- 當前 footer 已證明相同 schema 時保留原欄位物件，避免把未變的大 schema 當新值
  再編碼。新讀取端使用已存的 JSON key prefix 組成完整文件，schema 不再解碼兩次；
  SQL 不解析／轉換 JSON 數字。舊 JSON、storage v1、v2 都可讀，v2 不會隱式降級。
- schema section 直接依 rowid 掃描小型表的 header，避免索引搜尋後排序大型 JSON
  value。初版這個排序會落磁，造成 176.57 MB 寫入，已拒絕並保留證據；修正後為
  19.77 MB，沒有藉由調整 I/O 限速解決。
- FinMind 日期投影沿用相同安全路徑規則及 POSIX component 排序，用已正規化字串處理
  warm receipt，避免反覆建立 Path／parts／hash。每次 source stat、receipt 前後檢查、
  manifest member 檢查仍保留，沒有以同筆數或相同位置假定來源不變。
- `ParquetSource.member_footers` 暴露該次完整驗證已取得的 footer；日期投影只重用此份
  同 snapshot metadata，避免為同一日期欄位再解析一次寬表 footer。所有 member 的
  before／after proof、完整列數、null、非法日期與實際日期聯集仍保留。

### 三組完整比較

三組都是 A/B/B/A/B/A/A/B，各版四輪；A 是本輪開始的已優化 v1，包含上輪所有修正。
沒有以更舊或缺少檢查的版本作比較，也沒有把三組比例相乘。

| 本輪平均 | A | B | 本批變化 |
| --- | ---: | ---: | ---: |
| 固定 74,824 來源／4,096 footer 刷新觀測，完整 record→feature→JSON→讀回 | 33.647 秒 | 30.453 秒 | wall 減少 9.5%，CPU 33.449→30.575 秒 |
| 同上完整交易邏輯寫入 | 58,976,784 bytes | 19,770,024 bytes | 減少 66.5%，不是 SSD 回收量 |
| 同上首次完整 cache 讀取分段 | 5.469 秒 | 3.924 秒 | 已納入整段耗時，非單獨 decoder 加速比 |
| bar catalog 完整快取交易與公開 JSON，固定來源結果／audit 工作 | 3.163 秒 | 2.340 秒 | wall 減少 26.0% |
| 32 個實際 FinLab 寬表的完整日期 audit、原子 JSON 與讀回 | 30.185 秒 | 23.498 秒 | wall 減少 22.2% |

第一組仍是同一份 192,805,898 bytes seed、來源 stat／partition 觀測與保留 footer。
每輪 228,568 次 source stat、全部 89,209 個特徵、完整邏輯快取與 feature bytes 均一致。
B 實際更新 4,096 筆檔案列與 **1 筆 root metadata**，schema／dataset／membership
列沒有更新。交易發布分段 3.922→2.319 秒；初始建置另記，平均約 7.67 秒。A 完整
wall 範圍 29.003–38.885 秒，B 28.737–31.851 秒，包含當時競爭負載與 GC 差異；
本批平均不代表穩定的整機加速比。上一輪不同時間的 4.076／5.745 秒只保留為歷史觀測，
本輪比較使用當次 A/B。

第一組不含實際來源／footer I/O、目錄發現、整份 public projection、網路或瀏覽器。
第二組含 canonical `_catalog_snapshot` 的完整 lock／decode／路徑與來源 guard／聚合／
原子 cache 發布／公開 JSON／全 bytes 讀回；SQL、receipt、stat、discovery、date
projection 及 cold-work clock 的結果保留後重播，確保兩版完成相同 audit、不把 pending
改成完成。它不包含實際 SQL／來源 I/O，也不是整份 public monitor 的測速。

第三組包含真實來源 I/O。32 個檔案各有 49,783–49,812 欄，每輪全部日期 audit 完成，
不是用時間預算少跑資料。32 個五項來源 identity 在所有輪次前後均穩定；完整輸出一致，
總列數 51,802、實際日期聯集 1,621 日、null／invalid／pending 均為零。這是明確的寬表
壓力情境與 warm OS cache，不外推所有日期來源；正式 audit 排程與原有 budget 未改。

證據：[metadata 8 輪](../artifacts/operations/monitor_metadata_20261026/metadata4096_no_sort_abba.json)、
[bar 完整交易](../artifacts/operations/monitor_metadata_20261026/bar_complete_final_abba.json)、
[同 capsule 再讀回](../artifacts/operations/monitor_metadata_20261026/bar_replay_verified.json)、
[實際日期 I/O](../artifacts/operations/monitor_metadata_20261026/actual_date_work.json)。新
[bar benchmark](../scripts/benchmark_data_monitor_bar_work.py) 保留完整 seed、來源觀測、
record datasets、selected membership 與輸入 SHA，支援 `--replay`，不必再查動態來源。

### 語意、失敗與正式運作

共享 store／inventory／feature／bar／tick／partition／原 reader／公開 API 回歸為
**628 passed**；測速工具的失敗重播另有 **3 passed**。初版 123、119 項屬重疊的
focused 組，不相加。涵蓋未提交程序退出、並行 reader、SQL 失敗整筆回滾、v1→v2
明確升級、有效 quick index 下的遷移、原始值／缺失／null／日期 gap 與來源替換。

另以同 seed 重做交易，由全新程序僅用標準庫 sqlite3/json 讀回全部 74,824 筆檔案、
1,107 個 schema，`integrity_check=ok`，完整 SHA 與上一輪獨立 oracle 相同，見
[獨立讀回](../artifacts/operations/monitor_metadata_20261026/independent_readback.json)。
首次 fixture 使用排序後 JSON 建立全新 metadata 物件，不符合只寫一筆 root 的 fixture
預期；原始完整讀回 SHA 仍一致，已保留失敗腳本／DB，再依 canonical owner 共用已證明
不變的物件重跑。初版 bar fixture 的既有 temporary directory 建立失敗也保留原 log。
測速工具遇到不能精確重播的例外會拒絕比較，不把它轉成別種 inventory 錯誤。

正式 rollout、自然 timer、目前 partial／invalid／pending 及交易服務 PID 連續性記於
[交付收據](../artifacts/operations/monitor_metadata_20261026/delivery.json)。只讀 gateway
先載入 v1/v2 相容 reader，之後才明確啟用 producer 的 `incremental_metadata=True`；
原 v1 DB 保留，不以升級授權來源清理。

12:46:47 開始的自然輪次完成一次 v1→v2 遷移，完整 38.572 秒、初建發布 10.175 秒；
下一輪沿用完整 signature probe 命中 quick index。12:47:57 的真實變動輪次更新
1,172 筆檔案與 **4 筆 metadata**、未重建 DB，交易發布 0.167 秒；後續 1,004 筆檔案
變動也只更新 4 筆 metadata。這些是正式採用證據，來源及競爭負載不同，不計算加速比。

12:51 的[正式獨立讀回](../artifacts/operations/monitor_metadata_20261026/writer_natural_readback.json)
確認 descriptor 103 bytes、DB 216,436,736 bytes、root metadata 69,764 bytes；
39,117,250 bytes 的 schema 分列保留。SQLite `integrity_check=ok`，實際查詢計畫沒有
temporary sort，來源 root 與快取相符，邏輯版本仍為 v10、74,824 筆檔案與 1,107 個
schema。owner API 仍為 2,401 sources、89,209 features；schema 覆蓋
74,805／74,824、partial，日期 audit **52,773 pending／1 invalid**，沒有將未驗證狀態
改成完成。自然完整輪次與 API 狀態可持續變動，以上是該次收據的時間點。

只讀 gateway 本輪重啟一次；Discord、TAIFEX、日內與隔夜服務的 PID／啟動時間均未改變，
snapshot timer 未重啟。啟用後相關 store／tick／測速工具檢查另有 **45 passed**，與上述
組別有重疊。CPU quota 仍為 infinity、Nice=0，沒有新增資源限速或改變來源排程。

## 第四輪：共用統計區塊、批次讀取與 SQL 投影（2026-10-26）

### 實際工作量與實作

固定 74,824 檔快照的 file JSON 有 147,626,873 bytes，其中 stats 為
137,690,862 bytes；null vector 共 35,729,230 格、124,912,029 bytes。
完整 stats 只有 40,559 種、47,609,438 bytes；同樣內容反覆物化與編碼是可移除的
實際工作，不能只縮短來源掃描或少完成 audit。

- 私有 storage v3 將 file header 與 SHA-256 stats 區塊分開。guarded native codec
  驗證原始整數／null／字串，未知欄位、超大整數、浮點／signed zero 等保留舊 JSON
  行為。相同內容只存一份；檔案與 metadata、區塊增刪仍在同一筆 FULL-sync 交易。
- reader 一次解碼整個唯一區塊池，再批次驗證型別。inventory 的唯讀 frame 共用統計
  向量；一般 `read_record_cache` 仍回傳彼此獨立的可修改 dict/list。沒有省略任一路徑
  或 member 的來源 identity 檢查，也沒有改公開 JSON／邏輯 v10／feature ABI。
- 日期 census 只在當輪保留重複 bitmap 的解碼結果，避免原 256-entry LRU 淘汰後
  再解。相對起點日期位移候選在實際 I/O 比較較慢，已撤回；全部來源與 manifest
  member 證明保留。
- FinMind 原 owner 維護部分 covering index：
  `(dataset,typeof(rows),first_data_date,last_data_date,rows)`，範圍仍為
  `state='complete' AND rows>0`。同一讀交易先以型別 prefix seek 確認選中列都是
  integer，才使用較窄日期順序；任何 legacy real/text/blob 皆沿原 status index
  累加，保留 SUM 順序、原始值及 overflow 失敗。reader 不建索引、不改 tasks。

### 固定輸入的完整比較

下表均為 `ABBABAAB` 八輪、每版四輪平均；保留整份輸出與來源工作量對照。

| 同一輸入與工作範圍 | A | B | 解讀 |
| --- | ---: | ---: | --- |
| v2→v3，4,096 來源更新，完整 record→feature→發布→全量讀回 wall | 40.551 s | 33.559 s | 少 17.2% |
| 同上 CPU | 36.110 s | 28.663 s | 少 20.6% |
| 同上完整 cache decode | 5.628 s | 4.328 s | 仍非零 |
| 同上邏輯寫入 | 19,770,024 B | 15,658,744 B | 少 20.8% |
| 最終完整 bar 交易，含真實 complement SQL、其餘來源觀測重播 wall | 5.602 s | 4.462 s | 少 20.3% |
| 同上 CPU | 5.065 s | 4.138 s | 少 18.3% |
| 僅重播 opaque I/O 的完整 bar 交易 wall | 4.675 s | 4.663 s | 近乎持平，不當作 wall 加速 |
| 同上 CPU | 3.512 s | 3.080 s | 少 12.3% |

record 比較保留 74,824 路徑、4,096 次 footer refresh、89,209 features 及
193,378,070 bytes 的完整邏輯 cache；其完整 SHA 為
`13428b8038394b2566526e24b4be843ad75d93edb94c67c0ab3e9d7a289542dd`。
刷新使用原始 footer 值與變動 source identity，stats 本身相同，故 B 不新增 stats
區塊；每筆新讀 stats 仍須驗證／編碼。這不是「任意新 stats 都零成本」的證據。
metadata 皆只更新一筆 root，檔案只更新 4,096 列。B DB 為 139,632,640 bytes，
A 為 216,727,552 bytes。首次建庫在計時外另記，A 8.250 秒、B 12.418 秒，
不可把一次遷移成本藏進穩態加速比。實際 footer I/O、目錄發現、其他 public monitor
投影、HTTP 與瀏覽器不在此 record 比較範圍。

bar 使用完整 canonical lock／cache decode／來源檢查／聚合／原子 cache 發布／
完整公開 JSON 與 bytes 讀回。SQL 組將同一份 2,883,416 列私有 queue 快照的實際
aggregate 及型別 guard 放回每輪；receipt、stat、discovery、date projection 與
cold-work clock 沿同一 capsule 重播。此組合 fixture 的公開 oracle 重新由 A 完整
產生，沒有沿用舊 SQL 計數。它仍不代表所有來源 I/O 或整份 public monitor。

只測完整 SQL 交易含 guard／aggregate／全結果讀回為 3.335→2.198 秒；舊 partial
index 原型 3.018→2.617 秒，已保留且由較快日期型別索引取代。查詢計畫仍有
`COUNT(DISTINCT)` temporary B-tree，未宣稱消除所有 SQL 遍歷。新增索引在該快照
占 172,318,720 bytes，首次建索引／ANALYZE 20.201 秒屬 setup。私有快照中完整
1,024 筆 state／rows／bytes／日期／heartbeat 更新、FULL-sync commit 與全新連線
讀回，A 0.0462 秒、B 0.0558 秒；保留約 0.0096 秒寫入代價，不外推其他 dispatch
索引、所有生產寫入或下載工作。

後續也實測兩層 `GROUP BY dataset,first_data_date` 再彙總，嘗試移除
`COUNT(DISTINCT)` 的逐列 temp-tree 工作。同一 SQL 快照八輪為 1.626→1.608 秒、
CPU 1.523→1.507 秒，約 1% 差距；仍有 outer GROUP BY temp-tree，且尚無完整
bar 優勢證據，因此未採用這項改寫，保留
[screen](../artifacts/operations/monitor_projection_20261026/monitor_grouping_screen.json)。

證據：[record 八輪](../artifacts/operations/monitor_projection_20261026/factored4096_batch_abba.json)、
[最終 bar 含真實 SQL](../artifacts/operations/monitor_projection_20261026/bar_adopted_sql_abba.json)、
[bar 重播](../artifacts/operations/monitor_projection_20261026/bar_bitmap_abba.json)、
[SQL 交易](../artifacts/operations/monitor_projection_20261026/monitor_date_sql_abba.json)、
[索引寫入代價](../artifacts/operations/monitor_projection_20261026/monitor_write_cost.json)。

### 獨立讀回與保留的限制

全新程序僅用標準庫 sqlite3/json 從 raw SQL rows 重建全部 74,824 檔、40,559 個
stats 區塊及完整 metadata，逐值對照原始 192,805,898 bytes seed。
`integrity_check=ok`、區塊 SHA 與引用全數通過且無 orphan。一般與共用 reader 的
完整邏輯 SHA 均與獨立 oracle 相同，見
[獨立讀回](../artifacts/operations/monitor_projection_20261026/factored_independent_readback.json)。
初版 v3 仍逐筆解碼 40,559 個 stats；profile 找到後改成批次，原版結果／程式與
profile 留存，沒有混稱為最後採用版本。一次 bar benchmark 的 CLI 參數誤用造成
exit 2，原 log 保留，修正參數後的八輪才列入比較。

實際 32 個最寬 FinLab 檔案、每輪完成全部 source/footer/date I/O、聯集、原子發布
與讀回，日期位移候選 A 20.908 秒／B 23.088 秒（CPU 20.243／21.346 秒），
不能列為加速。已保留[負結果](../artifacts/operations/monitor_projection_20261026/actual_date_work.json)
及候選程式，正式 `_date_bits`／`_project_dates` 回到原邏輯；保留 bitmap 重複解碼
共用與 SQL 修改後，才重跑上表「最終」八輪。原始 5.485→4.461 秒結果也留存。

共享 store／inventory／feature／bar／tick／partition／原 reader／公開 API 與兩個
FinMind owner 回歸為 **745 passed**。撤回日期位移及修正 legacy-only test fixture
之後，受影響測試另有 **99 passed**，與上述組別重疊、不相加。

### 正式採用與當時狀態

唯讀 gateway 先載入 v1/v2/v3 相容 reader，之後原 snapshot producer 明確啟用
`incremental_stats=True`，原 timer 自然運作。自然 v2→v3 遷移全輪 97.090 秒、
初建交易 10.499 秒；後續一輪只更新 576 個檔案、139 個新 stats／刪除 138 個
無引用舊 stats、2 筆 metadata，交易發布 0.211 秒。另一輪更新 680 個檔案與
205 個新 stats、4 筆 metadata，沒有重建 DB。這些是真實採用證據，來源與競爭
負載不同，不能計算成 A/B 加速。

14:25 的[正式完整讀回](../artifacts/operations/monitor_projection_20261026/writer_natural_readback.json)
確認 descriptor 103 bytes、DB 139,661,312 bytes，完整 74,857 檔／1,109 schemas
與 40,571 個 stats 區塊；`integrity_check=ok`，缺失引用與 orphan 皆為零，舊 v2
DB 保留。實際 API 仍為 2,401 sources、89,209 features、partial，schema 覆蓋
74,838／74,857；日期 audit 為 **50,943 pending／9 invalid**，source_errors=0。
之後資料與 audit 仍可持續變動。175 項啟用後檢查通過，與前述測試組重疊。

兩個 queue 的索引透過 canonical owner helper 與既有 bounded planner optimize
維護。SQLite authorizer 禁止 `tasks` 的 INSERT／UPDATE／DELETE；同一交易內建索引
前後完整聚合一致，未改 task 狀態／數值。實際 sponsor 安裝與驗證 3.107 秒，
complement 因冷資料掃描為 **150.233 秒**，含前後查詢與建索引／planner 維護，
這是一次性正式成本，並非私有 warm image 的 20.201 秒。見
[正式索引收據](../artifacts/operations/monitor_projection_20261026/monitor_index_activation.json)。

本輪只下達一次唯讀 gateway 重啟，snapshot timer 及來源排程／資源 quota 維持原設定。
整段操作期間另觀測到日內 paper 於 14:15:33、Discord 於 14:20:26 變更 PID；
本輪 gateway 重啟為 14:17:45，不能聲稱四個交易服務全程 PID 不變。原先 PID
相等 gate 的拒絕、前後 PID／啟動時間與 journal 已保留，最終讀回只驗收 data-cache／
API 證據，不代替財務 owner 連續性或交易 readiness。見
[原 gate 拒絕](../artifacts/operations/monitor_projection_20261026/reader_rollout_protected_change.json)。

最新保留自然輪次為完整 34.082 秒，inventory 16.062 秒、public projection
17.288 秒；其中完整 cache decode 6.417 秒、bar 8.191 秒、tick 3.676 秒、FinLab
投影 1.540 秒。該輪來源持續更新，feature projection 仍為
`deferred_source_revision`，不能把既有公開 partial features 當作最新來源已全數發布。
這些 live 數值用來定位剩餘工作，不外推固定快照加速比。

## 第五輪：schema 共用、完整公開投影與移動來源（2026-10-26／27）

### 從必要工作拆出重複工作

固定 74,824 檔快照共有 1,107 schemas、2,015,992 組欄位名稱／型別，實際不同
的 pair 只有 154,508 組；原 schema JSON 為 39,117,250 bytes。繼續只加速 JSON
parser，仍會建立大量相同物件，不能消除這部分成本。

- 私有 storage v4 在同一 SQLite 交易內共用精確字串 pair，schema 列改存整數引用；
  所有引用仍完整驗證。內部 `SharedSchema` 只在需要欄位時展開，完整普通 reader
  保留獨立可修改 list／pair。未知形狀與不能存入 SQLite UTF-8 的舊字串保留原 JSON。
  pool 在該 DB 生命週期保留歷史 pair，沒有每輪全量重寫或全量 GC。
- file header 仍完整讀取，但 stats 引用隨同一次 files 查詢取得，移除第二次 SQL
  遍歷與逐路徑 dict 查找。完全符合 codec 1 的 pool 直接 typed decode，避免再複製
  一份向量做型別驗證；legacy／超大整數仍沿既有 fallback。
- 新 footer 的 row-group 物件與列數只物化一次，null_count 只取一次；所有欄、
  group、日期與未知 null 語義保留。新 stats 的 JSON／hash ABI 沒有更動。
- FinLab 公開投影的 1,901 個收據／attempt 超過原 512-entry LRU，每次完整掃描
  都先淘汰自身、沒有重用。原 reader 新增明確 one-pass 模式，省去這些不可重用的
  marshal 封包；保留完整 JSON、每檔前後 identity fence 與可修改結果隔離。通用
  reader 使用已有 lossless native JSON helper，legacy 失敗仍回原解析。

特徵來源驗證原先排在 tick／bar／完整 public work 後。producer 現在於每個 dataset
聚合後立即執行原本的 fresh stat／member／schema／vector 檢查，固定該次有限觀測；
後續公開格式化消費同一 frame、selection、revision 與 cache identity。收據新增實際
`source_verified_at_utc`，沒有把較舊觀測標成發布瞬間的最新來源。

若尾檔在初掃後、聚合前改變，會在原 footer 工作預算內重新讀取，仍核對 footer
前後五段 identity。已被較早 owner 消費的實體檔，後續 alias 不得再替換該 frame
中的 footer；跨 alias 的變動、讀取中替換或無法取得一致證明仍延期。沒有新增 CPU／
磁碟 quota、調低工作量或略過完整發布。

### 固定輸入的完整比較與負結果

所有正式比較皆為 `ABBABAAB` 八輪、每版四輪，逐輪完整輸出一致。各組範圍獨立，
不能相加成整機加速比。

| 完整工作範圍 | A | B | 解讀 |
| --- | ---: | ---: | --- |
| 74,824 檔、4,096 footer refresh 的完整 record／feature／發布／全值讀回 wall（重播來源觀測） | 38.669 s | 32.039 s | 少 17.1% |
| 同上 CPU | 33.817 s | 28.749 s | 少 15.0% |
| 32 個最寬 FinLab 實檔，實際 footer I/O→record→feature→發布→完整讀回 wall | 43.924 s | 39.544 s | 少 10.0% |
| 同上 CPU | 37.078 s | 33.774 s | 少 8.9% |
| 完整 FinLab 公開投影、1,901 收據讀取、JSON 發布與全值讀回 wall | 1.102 s | 0.859 s | 少 22.0% |
| 同上 CPU | 1.053 s | 0.820 s | 少 22.1% |

32 檔組的所有實際 source identity 在整組前後相同，輸出 49,812 features、
11,923,660 bytes cache；完整 cache＋features SHA 為
`ebcca1788f2cf0d33c8b541cf42f188960ab0270f51308c91ee6a0f81c69eada`。
另在每輪新讀的 32 份 stats 上量出 5,857,390 bytes 的 guarded native 編碼，
B 平均 0.0565 秒、CPU 0.0447 秒。這是分開的成本歸因，未偷偷加到 flat-cache
流程，也不是「任意新 stats 零成本」或獨立端到端加速證據。

FinLab 比較保留完整 1,357 sources、3,398,151 bytes 公開輸出與實際來源檔存在檢查；
收據／catalog bytes 先固定，完整讀取、fence、JSON 與讀回每輪重新執行。測試前清
程序內 cache 以對應新的 producer 程序，不外推瀏覽器、HTTP 或其他來源。

早期 v4 仍展開所有 schema 引用，完整 read 3.283→3.262 秒，幾乎持平；保留
[原結果](../artifacts/operations/monitor_materialization_20261026/read_screen.json)。
改為 compact 後，read-only screen 為 8.687→6.497 秒，當時競爭負載波動大，故不
拿這組讀取數字取代完整 workflow。首次完整 record 比較為 31.366→30.439 秒、CPU
29.129→28.970 秒，CPU 幾乎持平；其中 cache decode 3.623→2.380 秒。
採用 footer 改善與追加 alias guard 後另跑最後完整比較，結果如表；cache decode 為
4.779→3.344 秒。保留原組、不混用不同版均值。兩組背景競爭與逐輪時間有波動，
這些均值只代表各自已固定輸入、固定版本的八輪實測，不是穩定通用加速保證。

固定完整 record 組保留 74,824 路徑、4,096 footer refresh、89,209 features、
193,378,070 bytes 完整 logical cache 及原第四輪相同 oracle。其 footer／stat／
discovery 觀測是保留值重播，實際 source/footer I/O、其他 public monitor、HTTP 與
瀏覽器不在此組。4,096 次讀取仍使用原始 stats 值，故新增 stats block 為零，不能
拿這組當作任意新 stats 編碼／寫入的成本。暖態 DB 為 139,632,640→119,599,104
bytes；每輪 setup／首次遷移另計，不藏進穩態均值。
最後一組 setup 平均為 A 12.450 秒、B 21.801 秒；v4 首次建立 field pool 更貴，
不能把暖態改善當作首次遷移也更快。

FinLab 的欄位投影原型保留完整輸出後為 0.970→0.992 秒，沒有 wall 改善，已撤回。
最後採用的是完整解析與 one-pass 封包省略。負結果及候選程式都留在私有 evidence
目錄，沒有把某個較快子步驟當成採用理由。

證據：[最後完整 record](../artifacts/operations/monitor_materialization_20261026/full4096_final_abba.json)、
[首次完整 record](../artifacts/operations/monitor_materialization_20261026/full4096_abba.json)、
[實際寬表全流程](../artifacts/operations/monitor_materialization_20261026/wide_footer_abba.json)、
[FinLab 完整投影](../artifacts/operations/monitor_materialization_20261026/finlab_once_abba.json)、
[撤回的欄位投影](../artifacts/operations/monitor_materialization_20261026/finlab_projection_abba.json)。

### 相容性與正式採用

獨立新程序只用 stdlib SQLite／JSON 從 v4 raw rows 重建全部 74,824 檔、1,107
schemas、40,559 stats blocks 與 154,508 field pairs，逐值／完整 semantic SHA
對照原始 seed；普通、共用與 compact 三種 reader 全數一致，`integrity_check=ok`、
stats 缺失引用／orphan 皆零。見
[獨立讀回](../artifacts/operations/monitor_materialization_20261026/independent_readback.json)。

共享 record／feature／pages／bar／tick／partition／公開 API／FinMind／dashboard
合約回歸 **840 passed**；追加 rollback、stale generation、alias 與來源變動測試後
有 **234 passed**，組別重疊、不相加，也不是整個 repository 全套。

台北 23:58 唯讀 gateway 先載入相容 v1/v2/v3/v4 reader；原 timer 再明確選用
`incremental_schemas=True`／`capture_features=True`，自然完成一次遷移，舊 v3 DB
保留。遷移完整輪為 120.064 秒，初建交易 16.725 秒；首次 full feature shard 因
owner ABI 改變而重建。這是實際一次性成本，不能用固定 warm 比較掩蓋。

之後自然增量一輪只更新 951 個檔案、228 個新 stats／234 個無引用舊 stats、4 筆
metadata，交易 0.456 秒；重用 234 datasets、重建 3 個，feature root 與當輪 record
一致。另一輪仍遇來源移動而延期，保留上一個有證明的 generation；未宣稱所有來源
在發布瞬間都最新。公開 status 的來源列數也會隨收據並行更新而變動，須以各次收據
判讀，不能當作全 catalog 完整性證明。

自然讀回的首版驗證腳本少寫收據檔名中的 `.json`，失敗 log 留存；後版改由 canonical
receipt path helper 取路徑。正式來源／公開檔案未因此改名。最終讀回採 SQLite backup
後在私有副本做完整 stdlib 重建，避免把整段 audit 放在正式 writer lock 內。

正式完整證據與最新 partial／pending／invalid 狀態見
[自然讀回](../artifacts/operations/monitor_materialization_20261026/natural_readback.json)。
2026-10-27 00:10 台北的完整讀回：74,920 檔、1,109 schemas、40,600 stats blocks、
154,519 field pairs，SQLite integrity 正常，stats 缺失引用／orphan 皆零；全值重建
SHA 與正式 reader 一致。公開 HTTP 的全部 89,219 features 與當輪完整檔案逐值
一致，收據來源 root 對應自然成功發布事件，實際 source verification 時間為
00:09:37 台北；沒有把 HTTP 讀取時間當作來源觀測時間。特徵仍是 `partial`，
74,901／74,920 檔有 schema；bar 日期仍有 1,745 pending、17 invalid，均如實保留。
近期自然輪 public projection 約 21–66 秒，bar／日期 audit 仍是下一個主要方向；
這些不同來源與競爭狀態的線上時間不作 A/B 加速比。
本輪僅重啟一次唯讀 gateway，來源與金融 owner、snapshot timer 及資源政策保留；
這項工程驗證不代替交易／財務 readiness。
gateway 載入後，inventory 只追加 producer 的 alias guard；其餘頂層 AST、reader
類別、decoder 與 footer 函式一致，未將不同整檔 SHA 說成同一版本。來源／收據 pins、
terminal runs、失敗記錄、檢查與正式 owner 連續性另記於
[交付核對](../artifacts/operations/monitor_materialization_20261026/delivery_final.json)。

## 第六輪：2026-10-27 CPU 佔用、取消資源上限與完整讀取

### 誰在使用 CPU，以及何時完成

台北 00:26:33–00:26:48 的 procfs 實際差分，16 個邏輯核心整體約 **49.3%**
忙碌、47.9% idle、2.8% iowait；system 佔 20.6%。當時 load average 約 43，
還包含等待中的工作，不能解讀為 CPU 全部跑滿。下表以**單核心 100%**計算，
不是整機百分比。

| 程序／owner | 該 15 秒 CPU | 進度的可證明邊界 |
| --- | ---: | --- |
| status snapshot，PID 1886138 | 99.2% | 週期性完整投影；每輪完成後仍會再次刷新 |
| 新聞全文 owner，PID 1697341 | 71.7% | 常駐，之後也觀察到等待下一個可用來源 |
| OpenBB L1，PID 1880737 | 65.5% | 該輪於 00:30:32 完成，pending files/rows 皆零 |
| lifecycle worker，PID 1318 | 41.7% | 常駐 Temporal owner，不能給「永遠完成」時間 |
| Syncthing，PID 1485 | 34.7% | 持續同步，掃描／傳輸階段需分別判讀 |
| 備份子程序，PID 1681161 | 12.1% | 尚缺可信的剩餘資料量與吞吐率，ETA 未知 |

當時使用者自己的 `htop` 另用約 46.6% 單核心，未中止。Linux 可用記憶體約
83.7 GiB，swap 已使用約 7.55 GiB；沒有做 swapoff。Windows 原生三秒取樣亦有
ChatGPT 約 162% 單核心、dllhost 約 43% 等工作，不能把所有操作卡頓歸因於 WSL。

01:04 的另一次辨識發現期貨財務驗證 accounting worker（PID 1946376）累計平均
約 2.2 個核心、RSS 約 53 GiB；其同一 owner log 已完成 55,852 個來源驗證、
4,968,220 列實體日曆及估值套用，尚無剩餘階段的可信 ETA。凌晨 Binance／OKX／
Bybit 的正式增量下載也已啟動。這些工作與本輪測速的程序均另列，不中止金融驗證。
新聞 owner 的 01:05 收據仍有 277,757 篇 eligible missing body，量測吞吐率與兩種
ETA 均為 null，不能用排隊數自行編造完成時間。

本輪測速／驗證終結後，01:32 的十五秒窗口整機仍約 **91.6%** 忙碌：新的
`run-position-finance-v394.py --worker accounting`（PID 2052887）約 399.9%
單核心，Binance／OKX／Bybit 分別約 162.9%／117.5%／94.5%，三者合計讀約
25.2 MiB/s、寫約 26.1 MiB/s。行情該輪於 01:33:21 完成，完整 owner wall 約
105 秒、CPU 約 337 秒；01:34:20 又開始下一輪。01:36 的另一窗口仍約 **84.9%**
忙碌，accounting 約 230.5% 單核心、RSS 約 43.6 GiB，進度仍在 financial owner，
沒有足夠分母給剩餘時間。RSS 含映射／共用頁，不把各程序 RSS 相加當實體 RAM。
每個窗口僅列首尾皆存活的 PID，已退出的短命子程序不會出現在該排名；另留 cgroup
差分涵蓋穩定 service／scope 的子程序 CPU。CPU、memory PSI 仍高，**卡頓尚未解決**。
這些前後窗口的工作不同，沒有當作資源政策或讀取修改的因果加速比。
01:39 的三秒 cgroup 窗口，主要輪到 FinMind quota snapshot 約 102.8%、FinMind
complement 96.4%、另一個 reference-scan 回歸工作 96.0%、TEJ 78.8%、status
snapshot 59.7% 單核心；financial scope 約 56.5%。父／子 cgroup 為包含關係，
不能相加。金融 owner 後續已出現另一段來源驗證進度，整個 owner 仍 running；
這種階段變化不能拿最早的 accounting CPU 值當成目前持續用量。

原始依據：[前取樣](../artifacts/operations/cpu_efficiency_20261027/load_before.json)、
[Windows 原生取樣](../artifacts/operations/cpu_efficiency_20261027/windows_before.txt)、
[新聞進度](../artifacts/operations/cpu_efficiency_20261027/news_progress.json)、
[01:32 取樣](../artifacts/operations/cpu_efficiency_20261027/load_after.json)、
[01:36 取樣](../artifacts/operations/cpu_efficiency_20261027/load_after_settled.json)、
[目前 owner 進度](../artifacts/operations/cpu_efficiency_20261027/current_progress.json)、
[service/scope 差分](../artifacts/operations/cpu_efficiency_20261027/cgroup_cpu_current.json)。

### 移除造成額外回收的上限

三秒核心差分直接觀察到 OpenBB 在 MemoryHigh=2,816 MiB 附近新增 **179 次 high
事件**，整機當時仍有上述可用記憶體；局部回收因此是有證據的額外工作。其他
owner 的大量 high 累計值有些本次差分為零，沒有當成同時發生的回收。

依使用者明確指示，已安裝本機 StockAgent service／slice prefix drop-in，取消
CPU quota、MemoryHigh／MemoryMax／MemorySwapMax、TasksMax、CPU／NUMA 綁定與
I/O bandwidth／IOPS 上限，包含祖先 slice 及已登錄的 PostgreSQL 控制 instance。
OpenBB 正式命令的 `--threads 2 --memory-limit 1GB` 亦已撤回，預設使用全部可用
CPU affinity（本機 16），DuckDB memory limit 為 `-1`；已在目前安裝版本確認可用。
相對排程權重、來源權限／配額、資料完整性及原本金融契約各有其作用，沒有改成
另一個佔用上限。部署入口與實際路徑記在 [Runtime](agents/runtime.md)。

01:19 的獨立核心讀回涵蓋 **44 個**相關 cgroup 與祖先，CPU／I/O／memory／swap／
task ceiling 違規為零。四個正式金融服務 PID／start monotonic 保持連續；無手動
服務重啟、無來源刪除。下一次自然 OpenBB 輪 01:00:29–01:10:12 的 high／max
事件從零保持零，peak 約 3.82 GiB，已超過原 high 門檻。該輪全程仍需 **583 秒**，
不是「取消上限就保證整輪變快」的證據：footer audit 420.118 秒、unassigned scan
74.179 秒、member count 48.907 秒、query views 25.422 秒；pending 仍皆零。
這些是不同並行負載下的自然輪，不能與上一輪 356 秒直接做因果加速比。

證據：[回收差分](../artifacts/operations/cpu_efficiency_20261027/cgroup_before.json)、
[解除後讀回](../artifacts/operations/cpu_efficiency_20261027/resource_policy_final.json)。
這項安裝範圍是本機 StockAgent，不代表已重設其他主機、Windows／WSL 的全部資源設定。

### 完整 header 候選的判讀與 stats 暫存釋放

曾測試將私有 v3/v4 reader 的全部 file header bytes 與 path／stats 引用在 SQLite
同一個 rowid 順序掃描中組裝，減少每個檔案跨 Python 邊界的 tuple／bytes 暫存。SQL JSON
只生成字串／null 引用；原 header／stats JSON 的數值 bytes 保留原樣。保留列數／
型別／順序檢查、每個唯一 stats 的 SHA／codec 驗證、來源 identity fence 與同一個
SQLite read transaction。普通 reader 仍回傳獨立可變的值。typed／shared stats
解碼後也不再保留無用途的原始 body 引用；legacy 普通 reader 仍保留其必要原文。
候選在沒有 JSON1 或 aggregate 超過 SQLite 字串大小時回到原完整掃描。
最終完整 record 比較未改善，**SQL 組裝已撤回，僅保留無用途 body 暫存釋放**。

全新程序 A/B/B/A，固定前一輪實際 v4 backup 的 **74,920 檔**、完整 **193,439,187
bytes** logical JSON，包含讀取、canonical JSON 匯出、原子發布及全部 JSON 讀回：

| 同輸入完整工作 | A | B |
| --- | ---: | ---: |
| wall | 18.542 s | 15.911 s |
| CPU | 17.646 s | 16.013 s |
| 其中完整 cache read wall | 3.422 s | 3.078 s |

本組 wall 少 **14.2%**、CPU 少 **9.3%**；每一輪完整 SHA 相同。先前同程序八輪
也保留，但首輪與後續配置／競爭成本差異大，因此另以全新程序比較。
這是完整 cache read/export 工作，其他 public monitor、實際來源 footer I/O、
HTTP／瀏覽器及整機負載不在該速度比例內。完整 record→feature 比較另列於
[最終 record 實測](../artifacts/operations/cpu_efficiency_20261027/header_record_final_abba.json)，
其 74,824 檔／4,096 footer 觀測刷新、89,209 features、193,378,070 bytes 完整
cache／index／feature 讀回皆一致，但全流程 wall **24.947→26.479 秒**、CPU
**23.346→24.192 秒**，沒有成立的端到端改善，所以撤回 SQL 候選。這些比較的
來源 stat／footer 是同一份保留觀測；真實 source/footer I/O、完整 public monitor、
HTTP 與瀏覽器不在速度分母內。背景工作仍有波動，沒有把子流程比例套到整條流程。

證據：[全新程序組](../artifacts/operations/cpu_efficiency_20261027/header_fresh_abba.json)、
[同程序八輪](../artifacts/operations/cpu_efficiency_20261027/header_abba.json)、
[撤回判讀](../artifacts/operations/cpu_efficiency_20261027/header_workflow_decision.json)。

### 未採用的候選與量測範圍更正

- 日期欄候選先列出全部 leaf names，增加實際工作；32 個最寬 FinLab 實檔的完整
  cold date audit、cache 發布及讀回為 28.279→30.248 秒，CPU 26.663→27.545 秒，
  已恢復原實作，保留 nested date／null／gaps 語意測試。
- stats identity memo 的完整 record 比較雖為 39.386→31.754 秒，其他未變階段亦有
  大幅波動；本 fixture 刷新 4,096 個普通 footer，stats 值原樣重播，新 block 為零。
  新 footer dict 沒有 memo 重用機會，publication 子步驟反而由 1.692 增至 2.175 秒。
  候選已撤回，不能宣稱任意新唯一 stats 編碼因此加快；跨交易可變值測試保留。
- 縮排公開 JSON 的原生 branch 編碼有 1.562→1.128 秒的結果，但正式完整狀態 owner
  使用 `compact=True`，那組不能當作線上加速。以正式選項重測完整發布／summary
  綁定／全部讀回，12,705,067 bytes 每輪一致，卻由 0.831→1.006 秒、CPU
  0.772→0.907 秒。**已撤回兩種 JSON 候選並逐 byte 核對恢復原 encoder／owner**。
  第一次 whole-tree 原生 guard 未處理浮點字面格式，退回 stdlib 的 CPU 負結果也保留。

證據：[日期 audit](../artifacts/operations/cpu_efficiency_20261027/date_abba.json)、
[stats 判讀與原數字](../artifacts/operations/cpu_efficiency_20261027/stats_candidate_decision.json)、
[正式 compact 組](../artifacts/operations/cpu_efficiency_20261027/compact_publish_abba.json)、
[JSON 撤回核對](../artifacts/operations/cpu_efficiency_20261027/json_candidate_decision.json)。

### 相容性、正式讀回與剩餘成本

候選階段共享 record／feature／bar／API／OpenBB／attempt／latency 合約回歸 1,054
passed；撤回 SQL 後最終保留版本 **1,051 passed**，不是整個 repository 全套。
舊測試硬寫 MemoryHigh=2816M／MemoryMax=3G
與使用者新政策相衝，已更新為無上限並保留 source audit 檢查；新增 header fallback
測試少了一個 import 的三個失敗也保留，補齊後完整回歸通過。

01:28 的獨立新程序用 SQLite backup 副本完整重建 74,920 檔、1,109 schemas、
40,594 stats blocks、154,519 field pairs，stdlib 全值 SHA 與正式 reader 一致，
integrity 正常、stats 缺失引用／orphan 皆零。保留的 body 釋放在這份輸入可少保留
**47,716,102 bytes（45.5 MiB）** 原文至 hydration 結束；原文仍完整讀取／驗證，
這個 byte 數不代表整個程序 RSS 或 CPU 時間已降低同一幅度。
完整公開 JSON 12,698,124 bytes 與原 compact encoder 逐 byte 相同，summary 來源
簽章／SHA 有效；全部 89,219 features 亦與 HTTP 完整讀回一致，實際來源驗證時間
為 01:28:07。cache backup 與 feature 各自保留其觀測 generation，不冒充同一瞬間
的全 catalog。特徵仍 partial；日期仍有 1,902 pending、17 invalid，沒有升格為完成。
見 [正式讀回](../artifacts/operations/cpu_efficiency_20261027/natural_readback.json)。

來源持續變動、每個新唯一 stats 的完整驗證／編碼、完整 schema／file header 的
物化、日期 audit、完整公開 JSON 的必要發布，及 OpenBB 的全部 footer／數百萬
SQL members 遍歷仍有成本。下一個高收益方向是為重複完整 audit／membership
遍歷建立可失效、可恢復的增量證明；目前沒有用「上次成功」跳過本輪驗證。
本輪保留無用途暫存釋放與資源政策修正，未宣稱 CPU／完整流程已加速、計算極限、
所有工作完成或系統保證不卡頓。
交付時再核對 41 個當時存在的相關 cgroup 與祖先，仍無資源 ceiling，四個正式金融
owner 仍連續。可重現 source／input pins、全部 terminal runs、保留負結果與最終
核對見 [交付收據](../artifacts/operations/cpu_efficiency_20261027/delivery_final.json)。

## 第七輪：2026-10-27 可失效的 OpenBB 增量證明

第一性原理的分界是「驗證需要讀哪些新資訊」。若任意 writer 都可修改檔案而
沒有可靠的變動通知，仍需確認每個檔案的身分；但沒有必要因此重新解析每個
Parquet footer。若 tasks／members 的每個 SQL 變動都在原交易被完整捕捉，則
計數及待辦集合可以由變動維護，不必每輪重新推導數百萬列的同一個結果。

三條增量路徑均延伸既有 compactor，預設未選時保留原路徑；正式維護 unit
明確選入，沒有新常駐 owner，也沒有新增 CPU／I/O／記憶體或 thread ceiling。

| 重複工作 | 增量證明與失效條件 | 保留的完整驗證 |
| --- | --- | --- |
| 每輪所有 L1 footer 解析 | SQLite 保存已驗證的 manifest rows／bytes／schema、runtime policy、檔案 dev／inode／size／mtime／ctime；每次重用仍前後核對 regular-file 身分 | 缺失、損壞、過期、換檔或契約變動就重開 footer；24 小時完整刷新及 `--force-full-metadata-audit`；`--audit-only` 保留原深度 audit |
| 每輪數百萬 member 計數 | SQLite triggers 在 member 新增／刪除／endpoint 移動的同一交易維護完整 endpoint totals；核對實際 hooks 與 schema cookie | 初始化或 schema／hook gap 後完整 GROUP BY；status 仍比較 member totals 與 segments 的 source_files，異常拒絕公開發布 |
| 每輪 tasks/member anti-join，只為確認沒有待辦 | 同筆交易維護全部 plan token 的 successful-unassigned workset；tasks 與 member 兩側皆有 hook；查詢仍讀當前原 task 值、沿原排序／過濾／batch 大小 | 初始化／失效後原完整 anti-join；來源變更使 segment stale 時，member 刪除自動把工作放回待辦 |

檔案身分證明的範圍是本機 OS 可觀察到的修改，不是 payload checksum、無聲媒體
bit rot 證明或 financial readiness。mtime 單獨相同不授權重用，ctime／inode 也必須
一致。cache 的 SHA 保護 proof 本身，不冒充 source payload SHA。新 proof table／hooks
先安裝，再擷取既有 source journal 的 schema watermark，避免新增 DDL 使同輪
watermark 失效。持久化只寫新／失效 proof，沒有全量 cache JSON 重寫。

來源 task journal、row/schema 檢查、SEC physical-schema 與發布前來源 fence、原子
view／status 發布及 L0 保護均保留。新資料／stale 重建、rollback、plan 切換、保留
mtime 的同大小毀損、換 inode、缺檔、manifest／schema 變更、proof 損壞／過期及
member corruption 都有語意測試。OpenBB 相關合約最終共 **430 passed**；部署
template 的三個新 flags 也納入本次完整合約測試。

待辦表的精確物化以 canonical archive 的 `INSERT ... ON CONFLICT DO UPDATE`
writer 為範圍。非 canonical 的 SQLite `REPLACE` 若直接把 task 換成 failed／inactive，
可能保留多餘候選 key；查詢仍從原 task 核對當前 active／status，不能把候選表
直接當輸出。這兩種情境另外驗證與原 anti-join 的輸出一致。

固定實際子集 **1,024 個 L1 檔、36,854 個原 task／member、11 endpoints、1,224,999
輸出列**，以新程序 A/C/C/A 完成既有 L1 流程、status parquet、完整 DuckDB catalog／
所有 view 值、全部 tasks／members／segments 的讀回。四輪完整值 SHA 皆為
`adf56f79062699f3f10ca71d538099dfe90f688af04ad93456e57261732b3e5c`，來源
身分前後相同；讀回按實際 row／column 位置保留 null 與非空值，避免寬稀疏表的
大量 Python null dictionary 展開。

這組 owner workflow 平均 wall **1.997→2.071 秒**、CPU **1.250→1.215 秒**；加入
額外全值讀回後 wall **50.238→58.923 秒**、CPU **47.789→51.285 秒**。沒有據此
宣稱小樣本整輪加速。footer 開啟確由 1,024 降為零，實際收益需另驗全庫 owner。
先前只有兩條增量路徑的 A/B/B/A 亦保留，不能只報有利的子步驟。

另以兩組各 1,024 個真實 task、同一 36,854-task 私有子庫，完成兩組 A/C/C/A
原生 `Manifest.claim → mark_executing → completion_batch/complete → WAL checkpoint`。
沿用 canonical 持久化方法與 FULL durability；初始化／reset 分開計時，關閉交易後
用新 connection 讀回所有 task/member/segment 值、3,072 筆原 mutation journal、
完整 pending anti-join 及新 member totals。兩模式各四輪完整值 hash 一致。

| 固定私有原生持久化 | 原版→增量：persistence wall / CPU | 含初始化與完整讀回 wall / CPU |
| --- | --- | --- |
| 已壓實來源刷新 | 1.097→0.897 / 0.657→0.615 秒 | 5.027→5.422 / 3.832→3.846 秒 |
| 新待辦來源 | 1.054→1.044 / 0.544→0.633 秒 | 5.685→6.214 / 3.863→3.997 秒 |

新待辦的 native CPU 增加，且新 index／proof 初始化有成本，均保留為技術取捨。
這不是正式全尺寸 manifest、全部 downloader、provider／payload 或全主機加速；
不能拿小子庫的 writer 倍率直接外推。沒有 provider 呼叫、payload 或正式 DB 寫入。
見 [原生持久化完整對照](../artifacts/operations/openbb_incremental_proof_20261027/writer_workflow_acca.json)。

實測原件：[最終固定子集](../artifacts/operations/openbb_incremental_proof_20261027/workflow_v4_acca.json)、
[前一組比較](../artifacts/operations/openbb_incremental_proof_20261027/workflow_v3_abba.json)、
[部署讀回](../artifacts/operations/openbb_incremental_proof_20261027/rollout_after.json)。
首次 capture 誤把三欄 archive_meta 當兩欄，以及測速器 seed 全值 JSON 展開過重的
失敗／中止均保留；readonly 測試也保留其 SQLite backup 改變 schema cookie、正確
回退完整計數的失敗紀錄，重新登錄該副本後合約通過。最後一次補測曾誤列不存在
的測試檔；錯誤收據保留，改由實際 14 個 OpenBB L1 測試檔完成上述 430 項驗證。

台北 02:16:31 已由既有、當時 inactive 的 OpenBB maintenance owner 開始建立全庫
基線，保留其 archive-idle、opening guard 與原鎖；四個正式金融 owner PID／start
保持連續。初建 terminal attempt 完成，單調時鐘 wall **2,278.788 秒**、CPU
**666.571 秒**：member 計數基線 47.079 秒、全部 plan tokens 待辦基線 457.608 秒、
完整來源 join 1,371.484 秒、footer／proof 建立 349.415 秒。實際寫入 **186,089**
個 proof；這是實測首次成本，沒有用小子集時間乘上全庫檔數。

02:54 再由相同、當時 inactive 的 owner 執行第二輪，terminal attempt wall
**69.997 秒**、CPU **46.844 秒**。186,089 個 proof 全部命中，footer audit
開啟數及 proof 重寫數均為零；source journal 為 incremental、changed task IDs=0。
兩輪皆保持 186,089 active segments、7,331,959 compacted files，pending／new／stale／
failed／deferred failed=0，L0 未刪除；原 `economy.fred_series` 的 103,223 schema
variants／long-form normalization deferred 狀態保留，不能宣稱所有 views 可用。

| 正式 owner 階段 | 部署前最近一輪 | 本輪 proof 重用 |
| --- | --- | --- |
| 完整 terminal attempt wall / CPU | 464.062 / 165.245 秒 | 69.997 / 46.844 秒 |
| 衍生檔 metadata audit | 289.549 秒，開啟 186,089 footer | 37.491 秒，檔案身分／proof 核對，footer 開啟 0 |
| 待辦來源 SQL | 89.095 秒 | 0.004 秒 |
| member count | 37.036 秒 | 0.004 秒 |
| query view 發布 | 40.932 秒 | 25.011 秒 |

這是相同完整工作範圍的正式前後觀測，背景負載／OS cache 不同，不能當作固定
輸入交錯對照的加速倍數；整機互動延遲也未驗證。正式耗時統一採 terminal attempt
單調時鐘。三輪 terminal UTC 起訖差另為 456.723／2,254.709／71.920 秒，與
monotonic 的 464.062／2,278.788／69.997 秒不同，兩種原始時鐘均保留，未抹平差異。
見 [初建收據](../artifacts/operations/openbb_incremental_proof_20261027/owner_seed.json)、
[重用收據](../artifacts/operations/openbb_incremental_proof_20261027/owner_reuse.json)；
terminal events 另由 journald `--all` 保留完整大欄位，不能把預設 JSON 的 null
當成沒有 terminal event。

03:02 的 [完整獨立讀回](../artifacts/operations/openbb_incremental_proof_20261027/natural_readback.json)
已 terminal succeeded：186,089 個 proof 的 SHA／manifest 契約／實際檔案身分逐項
核對，7,331,959 筆 member 重新完整 GROUP BY 與新 totals、segment source_files
逐 endpoint 相等；原始全部 plan tokens anti-join 與持久待辦集合皆為零。完整
status parquet、DuckDB catalog 及所有 view SQL 已讀回，runtime source SHA 未變，
四個金融 owner PID／start 保持連續。獨立驗收本身 wall **428.307 秒**、CPU
**165.815 秒**另列，不混入正常重用輪；正式全庫每個 view 的所有 payload 值並未
重讀，該全值證據限於前述固定實際子集。沒有 financial readiness 或整機延遲宣告。

首次新增 DDL 也讓 source journal 正確以 `schema_changed` 回退完整來源比對。
02:37 的唯讀 `EXPLAIN` 確認當前仍是 member scan → segment PK → task PK，
沒有已驗證的 task-contract covering index，並保留 DISTINCT／ORDER BY 的 temp
B-tree；不是「程式已有索引支援」就代表正式庫已採用。舊索引候選的完整查詢
及寫入成本記於 [先前完整對照](running_service_optimization_2026-10-13.md#1015-0013-完整-sql-對照完成接續原生寫入成本)，
不能直接把舊私人測速套到當前正式 owner。定期 full-source SQL 仍需另作整輪
讀寫取捨驗證。本輪沒有在運行中的來源比對旁建立另一份全庫索引。

Monitor 另於 02:06 留下新觀測：未變動輪 total 25.549 秒，public projection
21.444 秒，其中 bar inventory 15.630 秒；來源變動輪 total 43.136 秒，完整 cache
decode 7.088 秒、cache delta write 1.460 秒、public projection 19.650 秒，其中
bar inventory 14.132 秒。這是自然負載下的階段觀測，非本輪前後加速比較。

因此剩餘 header／唯一 stats、new stats encoding 與 monitor 的正確後續順序是：

1. 為既有 record store 增加同交易的 file/schema/stats delta watermark，讓私有
   resident frame 只套用變更；journal gap、DB 替換、codec／schema ABI 變動仍完整
   重建。目前 status snapshot unit 是 oneshot，單純 module global 不會跨輪重用；
   必須由既有常駐 owner 持有 frame，或保存可驗證的分塊 facts，才有跨程序收益。
   不能只憑 JSON descriptor 或 mtime 直接跳過全部來源與 stats 驗證。
2. 拆出公開投影的純 source facts 與依 now 更新的 freshness／ETA；前者綁完整
   dependency identity 重用，後者仍按當下時計算。完整公開 JSON 必須由所有最新
   facts 產生，不能把精簡回傳或延後發布當作完整發布加速。
3. 日期 audit 已用 native unique／單日 row-group proof；下一步應讓 canonical
   writer 在已有日期欄位時產生完整日期集合／null／count proof，綁實際發布檔案
   身分，避免 monitor 再讀同一欄。不能以 min/max 代替完整日期集合或抹掉 gap。

這三項 monitor 改寫尚未在本輪啟用；完整 header、唯一 stats、日期與其他公開
投影仍有實際成本，沒有宣稱所有優化完成或整機卡頓已消除。

## 第八輪：完整世代證明與實際工作減量（2026-10-27）

本輪延續同一兩個 active task；原先 37.491 秒的 metadata 身分遍歷與 25.011
秒的 view 階段只是上一個自然窗口，不能當成固定硬體速度。當前有其他下載、
財務驗證與測試同時執行，這輪單獨唯讀 profiler 的 metadata audit 為 89.285 秒，
其中有 372,178 次檔案 stat。SQLite 逐列取得證明、路徑物化及證明解碼也有成本。
未加入 CPU、I/O、記憶體或工作數上限，未重啟金融 owner 或刪除來源資料。

### OpenBB：順序讀證明，保留完整發布世代

metadata audit 將 186,089 次 proof primary-key join 改為一次順序載入、按 segment ID
查詢的 attempt-local pool；每列的 body SHA、manifest、runtime、年齡及兩次新鮮
檔案身分檢查均保留。掃描完成立即釋放原始 proof body；強制完整 audit 不載入 pool。
這不是取消檔案遍歷，也不是 bit-rot／payload checksum 證明。首輪冷熱差異很大，
另保留一組暖機後 A/B/B/A，沒有只挑第一輪較慢的 A。

view 的未變更路徑先比對完整 13 欄 catalog、現有 SQL 簽章及全部 current input，
可保留原 DuckDB inode／bytes，省掉 copy／DELETE／INSERT／CHECKPOINT。SEC 的
完整 physical-input signature 與 reader policy 相同時也免做重複 DESCRIBE。
進一步的 `--incremental-view-publication` 將先前**完整 persisted SQL 與 catalog**
驗證結果存於既有 `archive_meta`；只有檔案五項身分、完整 publisher/schema/proof
程式 SHA、DuckDB 版本／設定及 24 小時年齡吻合，且無未 checkpoint 的 WAL，才省去
重複開啟 published DB。仍以新的記憶體連線讀取實際 reader policy，逐一比對當前
聚合、ordered paths、檔案存在性、SEC physical receipts 及發布前後 source fences。
來源／SQL／DB／WAL／runtime／contract 變動或證明損壞均回退完整驗證；可用
`--force-full-view-publication` 明確重新驗證及刷新證明。沒有新增 task writer hook、
來源 index 或 source-journal DDL。

使用正式維護程式、同一完整凍結 manifest／檔案身分，全新程序 A/C/C/A：

| 完整分項，含其完整讀回 | A 原方式 wall／CPU | C 正式候選 wall／CPU |
| --- | ---: | ---: |
| 186,089 個 metadata audit 與全部 50,722 個 SEC proof SHA | 40.364／23.962 秒 | 26.385／19.722 秒 |
| 67 個 endpoint 的 view publication 與完整 catalog／SQL | 34.866／21.345 秒 | 22.168／14.897 秒 |

metadata 計時包含 pool 建置及全部 372,178 次 stat，proof hash 四輪同為
`4ee9c3b59e84836a2bc5d1da4cd1d93a4145b1e328516641195a24e66179b916`。
view 四輪完整 catalog／SQL hash 同為
`c4d705d4a39dfbe596452ad46eafcbd7cf228e18ba1d6f6036e8f1c6dc305a4a`。
前者不含完整來源 SQL、view／status；後者不含 metadata audit、完整來源 SQL 及
正式庫全部 payload values。view 控制實驗將 proof SQLite 持久層隔離在私人記憶體 DB，
正式 owner 則實際寫入既有 archive_meta。證據：
[正式 metadata 對照](../artifacts/operations/incremental_followup_20261027/metadata_pool_final_acca.json)、
[正式完整 view namespace 對照](../artifacts/operations/incremental_followup_20261027/view_proof_final_acca.json)。

正式 guarded owner 自然啟動的一輪為 134.890 秒；之後由相同 unit、相同 opening
ExecCondition 執行證明初建與重用兩輪，分別 58.233、46.490 秒。重用輪 metadata
19.947 秒、view 19.084 秒；186,089 個 metadata proof 全命中，沒有重開 footer 或
重寫 proof。source journal 為 incremental、changed task IDs=0、cutoff=90605。
這些自然窗口背景負載不同，不能拿它們換算固定全流程加速比。另一全新程序完整
讀回 67 個 catalog rows、66 個 persisted view SQL 與所有 status parquet values，
初建前後及重用後全同；45,101,056-byte 公開 DB 的五項身分及 SHA 也完全不變。
正式 unit 已讀回新旗標，CPUQuota、MemoryHigh／Max、TasksMax 均為 infinity，
四個金融 owner 的 PID／啟動時鐘與本輪開始相同。證據：
[初建與重用驗收](../artifacts/operations/incremental_followup_20261027/owner_final_acceptance.json)。
先前同輪的 [完整 member／pending 原始 SQL 獨立 oracle](../artifacts/operations/incremental_followup_20261027/natural_readback.json)
另驗證 7,331,959 個 members 與 all-plan pending；新兩輪並未重做正式庫全部 payload-value census。

1024-segment／36,854-task／11-endpoint 的完整子集 A/C/C/A 也讀回全部 1,224,999
個輸出 rows、每個欄位值及 null 位置，完整 SHA 四輪一致。producer wall
1.581→1.387 秒、CPU 0.982→0.353 秒；**再含全部值讀回 wall 卻為
64.385→72.816 秒、CPU 55.966→61.047 秒**。此負向結果保留，不能宣稱該完整
驗證工作流也加速，分項表也不能外推整機或所有消費者查詢。證據：
[包含全部 view values 的完整子集](../artifacts/operations/incremental_followup_20261027/openbb_workflow_final_acca.json)。

### 定期完整來源 SQL：一次順序快照取代反覆分散取頁

仍執行原本完整來源 SQL、CASE／NULL／路徑正規化／DISTINCT／排序及定期全量
保底，不延長 audit 間隔。`--memory-source-audit` 在容量足夠、沒有 owner 未提交
交易、沒有同名 temp source tables 時，使用 SQLite backup API 建立一致的私有
記憶體快照，再執行**同一條 SQL**；結束關閉快照。容量不足等情況仍完整執行
原查詢，不 commit 他人的交易、不略過來源、不新增持久 covering index 或 writer
hook。這是一次性 working-set 的演算法選擇，沒有加 CPU／I/O／程序利用率上限。

同一 19,861,577,728-byte 完整凍結庫的各次完整 SQL 結果均為相同空 stale-set，
另以缺失／退休／失敗／endpoint／path／rows／clock／NULL／absolute／dotted path
等語義測試驗證非空與特殊情境。全庫單次結果如下；各窗口有背景工作與不同冷熱
狀態，**不是 A/B 多輪平均，也不能外推整機加速比**：

| 完整全庫方案 | wall | CPU | 結果 |
| --- | ---: | ---: | --- |
| 原 SQL、原頁面 cache | 1,016.098 秒 | 304.330 秒 | 完整對照 |
| 原 SQL、file-sized cache hint | 992.895 秒 | 295.339 秒 | 不採用 |
| 暫存 task 投影與 automatic index，全置記憶體 | 1,090.205 秒 | 489.781 秒 | 不採用 |
| 一致記憶體快照 + 原 SQL，含完整複製／查詢／close | 176.594 秒 | 151.005 秒 | 早期候選 |
| 最終正式 helper，另含 allocator 歸還與完整讀回 | 189.904 秒 | 159.062 秒 | 採用版本 |

原 SQL 的 logical read 約 65.17 GB；快照方式只依序讀取完整 19.86 GB，峰值 RSS
約 21.45 GB。memory-materialized 候選只完成 B/A 各一次；在保留兩個完整結果後，
取消兩次不必要的昂貴重複，原 run 的 failed/-15 仍保留，沒有冒稱完成 B/A/B/A。
更早的 disk-materialized 候選在 1,596.97 秒仍未返回，含 source backup 共寫入
56.21 GB；該未完成候選也已拒絕，不當成完整耗時／輸出證明。file-sized cache
候選在診斷啟動前已正常完成，因此沒有送出終止訊號；診斷的競態失敗原件保留。
證據：[完整固定輸入 pair](../artifacts/operations/incremental_followup_20261027/full_sql_complete_pair.json)、
[cache hint 全結果](../artifacts/operations/incremental_followup_20261027/full_sql_page_reuse.json)、
[完整 snapshot 候選](../artifacts/operations/incremental_followup_20261027/full_sql_memory_snapshot.json)。

07:32:54 經既有 opening ExecCondition 啟動一次正式強制 full source owner，
**整輪 358.608 秒、其中完整 source snapshot SQL 291.513 秒／CPU 179.671 秒**；
metadata 34.727 秒、view 20.573 秒及完整 status 發布均包含在整輪內。21.60 GB
峰值 RSS 是這次一次性完整快照的代價。完整來源、所有 metadata 及公開輸出均驗證，
沒有新 stale／failed／new segments。另一全新程序讀回全部 catalog／SQL／status
values，與啟動前完整相同，DB inode／bytes／SHA 也不變。強制 full 旗標只用於
這一輪，啟動後立即移除；日常 unit 保留增量 journal。證據：
[正式完整 owner 與獨立公開讀回](../artifacts/operations/incremental_followup_20261027/owner_snapshot_acceptance.json)。

該正式輪也暴露出 SQLite close 後 libc 仍保留大量已釋放頁面的成本；已加上
allocator 頁面歸還，**不限制任何 live allocation**。最後以正式 helper 在同一
完整凍結庫重做全部 SQL、close、頁面歸還及結果讀回，完整 wall 189.904 秒、
CPU 159.062 秒，RSS 從 21,344,014,336 降到 88,145,920 bytes，完整結果與原 SQL
相同。這個 RSS 是固定 SQL 驗證程序的觀測，不能冒稱正式 owner 的後段 RSS；
上面 358.608 秒正式輪尚未包含最後這個後處理呼叫。對應 15 項來源快照測試再度
通過；較廣的 845 項相關回歸亦通過。沒有在 07:35 保護起點後另行啟動正式
maintenance owner。證據：
[最終完整 SQL 與 OS 頁面歸還](../artifacts/operations/incremental_followup_20261027/canonical_snapshot_release.json)。

同時發現 maintenance unit 還保留舊 Nice=15、CPUWeight=10、IOWeight=10 與
idle I/O 排程；已恢復正常 Nice=0、weights=100、best-effort/4，並核對無硬性
quota。這是取消既有降優先序，**不計入演算法加速**，也沒有更動金融 owner。
來源與四個金融服務的原始生命週期均保留，opening／auction guard 沒有繞過。

### Monitor：來源未變動時不再完整解碼 footer cache

沿用 canonical quick index 已有的完整來源／membership fingerprint、cache 五項
身分與 checksum，在當次 `InventorySnapshot` 傳遞未變動世代證明。投影 manifest
另保存上一輪完整 scan 的 source root、feature revision 與 summary。只有 ABI、
cache 世代、所有 owner／標籤／檔案分母均吻合，且特徵發布前**再做一次完整
檔案身分及 partition member 核對**，才沿用已驗證的 header/schema/stats facts。
這省下的是未變動世代的完整物化；公開 JSON、預覽、來源頁與 receipt 仍完整產生，
每個使用的投影物件仍驗 SHA。來源／cache 替換、標籤變動、證明損壞、物件損壞
或 ABI 變動回到完整 reader。cache 在 probe 與 decode 之間變動時，重新撤銷空 delta。

固定完整實際 namespace（74,824 個 selected file entries、236 個非空 dataset、
89,209 個 fields），全新程序 A/B/B/A，包含 record quick probe、feature publisher、
完整 JSON／pages／receipt 及另一全新程序的完整 JSON 讀回：

| 範圍 | A 原流程 | B 世代證明 |
| --- | ---: | ---: |
| record + feature publisher wall | 10.871 秒 | 4.505 秒 |
| 同範圍 CPU | 6.939 秒 | 3.027 秒 |
| 再含獨立完整讀回 wall | 16.333 秒 | 8.757 秒 |
| 再含獨立完整讀回 CPU | 10.125 秒 | 6.191 秒 |

四輪完整輸出 SHA 均為
`5a0058912898096855ebeffa28ab3b6e44c188b9b4d90da2e6d55878208f4350`，
來源檢查次數同為 150,160。保留 `partial`，沒有改成完整來源／歷史就緒宣告。
輸入是凍結的真實 stat／member 觀測；不包含 live source I/O、目錄發現、變動來源
footer refresh、其他 Monitor provider 或瀏覽器。完整讀回 wall 減少約 46.4%，
不能把它說成整個 Monitor 或整機都加快 46.4%。證據：
[固定完整流程](../artifacts/operations/incremental_followup_20261027/monitor_abba.json)。

### 待辦寫入：只處理資格可能變動的 SQL

pending→running 等兩側皆非成功資格的轉換不再進入待辦觸發器；保留必要的離開
資格刪除、進入資格新增，以及 task ID／endpoint／plan 更新。舊版 hook 的精確
DDL 是唯一可接受的原地換版來源；`BEGIN IMMEDIATE` 內只換兩個已知 hook，核對
其他 schema SQL 完全一致。只有舊 checkpoint 的 cookie 恰好涵蓋換版前完整
schema 才搬移 cookie，保留 cutoff、last-full clock、DB 身分與 contract fingerprint；
既有缺口不搬移，仍回退完整重建。沒有取消定期完整來源 SQL。

同一 36,854-task fixture、各 1,024 筆實際 canonical claim/executing/completion
與 durable checkpoint，全新程序 A/B/B/A、所有主表及 journal 完整讀回一致。
未分配工作直接持久化 CPU 0.793→0.765 秒，但含初始化／完整讀回 CPU
4.969→5.113 秒；membered 對照直接 CPU 0.766→0.639 秒、完整 CPU
5.137→4.589 秒。未宣稱所有 writer CPU 都改善，也未把子集當成 1,300 萬筆
正式庫 writer 成本。證據：
[原生 writer 完整對照](../artifacts/operations/incremental_followup_20261027/writer_abba.json)。

### 已撤回的日期候選與剩餘驗證

每群組先去重 bitmap 再 decode/OR 的候選，完整 bar cache transaction 含 SQL、
所有 source checks、公開 JSON、cache 原子發布與完整讀回，wall
7.308→7.443 秒、CPU 4.881→5.037 秒。已撤回，候選 source／test 與負向收據
保存在本輪 `rejected/`，正式日期 audit 行為不變。
[完整負向實測](../artifacts/operations/incremental_followup_20261027/bar_workflow_abba_fixed.json)。

來源持續變動的世代仍需完整 header/schema/stats 讀取，新唯一 stats 的完整
驗證／編碼也未消失。日期 audit 仍需要 canonical writer 的完整日期集合／null／
count 證明，不能以 min/max 或較低 audit 工作量代替。OpenBB 全檔身分及有序 path／
聚合仍遍歷；定期完整 SQL 已降低分散取頁成本，仍需完整 19.86 GB 快照與完整
tasks／members 比對。以下界線仍有效。

## 第九輪：減少物化，拆開發布與完整值驗證（2026-10-27）

本輪從第八輪的程式凍結 A 基準，而非拿較早的 37.5／25 秒自然窗口相除。
固定輸入、全新程序、平衡順序，保留所有 catalog／SQL、status、source fences
與完整值讀回。沒有增加 CPU／I/O／記憶體／並行數上限，也沒有跳過公開輸出。
本輪證據集中於 `artifacts/operations/incremental_round9_20261027/`；來源與測試的
修改前版本及 SHA 另存於該目錄 `baseline/` 與 `baseline.json`。

### OpenBB：不建立隨後會丟棄的物件

metadata proof 命中的通常路徑，直接以原始字串呼叫 `os.stat`，只在 footer fallback
或錯誤分類時建立 `Path`。仍檢查 regular-file、dev／ino／size／mtime／ctime、
proof body SHA、manifest、runtime、年齡及全部兩次新鮮檔案身分。
SEC view 的 canonical parent／Hive 路徑檢查提到迴圈外；每個來源仍檢查 segment ID、
精確路徑及 symlink。不可變 `ViewSourceProof` 直接擷取欄位，省掉遞迴 `asdict`／
deepcopy；產生的完整 JSON 簽章與舊版逐位元相同。

最主要的發布減量是先算完整 source／policy 簽章，只在簽章變更時建立完整
UNION SQL、representatives 及執行 DESCRIBE。既有 persisted SQL 的完整驗證與
增量證明仍由原 owner 負責，沒有以部分欄位或部分 endpoint 代替。

| 完整分項，含該分項的完整讀回 | A wall／CPU | B wall／CPU |
| --- | ---: | ---: |
| 暖機 metadata，B/A/A/B | 18.989／17.557 秒 | 19.074／14.600 秒 |
| 完整 view namespace，A/B/B/A | 21.811／15.084 秒 | 18.876／12.085 秒 |
| 私有 producer 加舊 ABI 的完整值讀回，A/B/B/A | 92.928／66.003 秒 | 85.374／70.415 秒 |

metadata 的實際遍歷仍為 186,089 個檔案、372,178 次 stat，全部 50,722 個 SEC
proof SHA 均與原值相同。暖機後完整 wall **沒有改善**，CPU 減少約 16.8%；最初
冷熱混合 A/B/B/A 的 48.48→13.10 秒另外保留，不能當成穩定加速比。
view 包含全部 67 個 catalog rows／66 個 views、ordered paths、reader policy 與
完整 catalog SQL／公開 DB 身分讀回；完整 wall 減少約 13.5%、CPU 約 19.9%。
其 SQLite proof 持久層隔離在私人記憶體 DB，未計入正式 archive_meta 寫入或全庫
payload 讀取，不能把這個數字當作正式 owner 完整耗時。

最後一列使用完整保留的 1,024 segments／36,854 tasks／11 endpoints 子集，
讀回全部 1,224,999 rows 的每欄、每值、null 位置、status／catalog、所有主表及
integrity check；舊值雜湊四輪均為
`adf56f79062699f3f10ca71d538099dfe90f688af04ad93456e57261732b3e5c`。
完整 CPU **反而增加**，沒有宣稱完整讀回已加速；查詢與 fetch Arrow 已分項記錄，
大量成本落在逐值轉成 Python 物件及 JSON 驗證。這個子集不是整個正式 payload 庫。
證據：
[暖機 metadata](../artifacts/operations/incremental_round9_20261027/metadata_warm_baab.json)、
[初輪冷熱比較](../artifacts/operations/incremental_round9_20261027/metadata_abba.json)、
[完整 view namespace](../artifacts/operations/incremental_round9_20261027/views_abba.json)、
[舊完整值 oracle](../artifacts/operations/incremental_round9_20261027/full_abba.json)。

### Monitor：減少重複編碼不等於完整工作流已變快

record store 的同一次 `_create`／`write_record_delta` 對相同 stats 物件只做一次
型別驗證、編碼與 SHA。pool 只活在該次發布，保留物件引用避免 ID 重用；不跨
交易沿用可變物件，不合併不同的相等 dict，後次 mutation 仍重新編碼。
header bytes、schema、stats codec／body／hash 及 storage v1–v4 格式均不變。

固定完整 74,824-file retained cache，以私人、明確的 header-only size+1 mutation
測試全部 header／schema／唯一 stats 讀取、delta commit，以及另一全新程序的
一般完整 reader／JSON hash。這不是捏造歷史 source observation，也不包含
live source I/O、目錄發現、footer refresh 或公開 feature publisher。

| A/B/B/A 平均 | A | B |
| --- | ---: | ---: |
| 同一發布 stats 編碼次數 | 74,805 | 40,559 |
| decode／delta／commit wall | 18.702 秒 | 17.130 秒 |
| 同範圍 CPU | 11.007 秒 | 11.299 秒 |
| 再含全新程序完整讀回 wall | 38.583 秒 | 39.653 秒 |
| 再含全新程序完整讀回 CPU | 25.561 秒 | 28.850 秒 |

四輪完整值 SHA 同為
`74ace798887f9ffe1697247c5a4276fea9eced171f2ab240c7a1782d5e9a9566`。
重複編碼減少 45.8%，但整體 wall／CPU 均未證明改善，保留此負向結果；完整
header／唯一 stats 讀取、新唯一 stats 編碼與一般 reader 物化仍在。
[完整 header 變更對照](../artifacts/operations/incremental_round9_20261027/monitor_abba.json)。

公開投影另將每欄一次的 JSON encoder／UTF-8 buffer 改為每個 dataset 一次。
仍使用原 encoder 選項、欄位順序、Unicode 與非有限值拒絕規則，完整公開 JSON、
預覽、source pages 及 receipt 仍必須產生。這部分另以強制所有 owner 重建的完整
固定 namespace 工作流量測，不能套用未變更世代的較快數字。

所有 237 個 selected owners 每輪都重建、保留完整 header／schema／唯一 stats
讀取、89,209 fields、150,160 次來源檢查、全部 JSON／pages／receipt 與新程序
完整讀回。A/B/B/A 的完整 wall 為 32.898→30.337 秒，CPU 為
22.757→23.111 秒；publisher 範圍 wall 27.041→26.311 秒、CPU
18.850→19.839 秒。完整 wall 較短，但 **CPU 沒有改善**，未宣稱整個 Monitor
效率全面提升。四輪公開輸出 SHA 與第八輪相同，pages SHA 同為
`21746e969ed6fe3d63c33b242e520b70579690d223e25c3beef034ea8ae4cbb6`，
保留 `partial`。不包含 live source I/O、目錄發現、新 footer、其他 provider 或
瀏覽器。[強制完整公開投影對照](../artifacts/operations/incremental_round9_20261027/projection_abba.json)。

上述初組的第三輪 B 曾在計時內接受一次 `py-spy dump`，wall 包含該次短暫
暫停，未以估計值扣除；因此初組只作診斷證據，不能當作無取樣的加速驗收。
另重跑完整四輪無 profiler 對照；初始化改為一次建立、SHA 凍結的同一 v4
cache，每個全新程序在計時前取得獨立 byte-identical 副本，並各自暖建完整
projection manifest。計時內的完整 decode、所有 owner 重建、公開輸出與另一
全新程序的完整讀回均保留。初組與 stack 原始證據一併保存。

無取樣 A/B/B/A 的 publisher wall 為 25.158→21.307 秒、CPU
19.239→16.739 秒；含獨立完整讀回的 wall **28.742→24.679 秒**、CPU
**22.423→19.865 秒**，本組分別減少約 14.1%、11.4%。所有輸出／pages SHA
及來源檢查次數與初組一致，每輪 237 owners 全部重建。單輪完整 wall 仍從
20.262 到 32.880 秒，這組均值不構成整機或所有 Monitor provider 的穩定加速證明。
119,169,024-byte 初始化快取建立及 SHA 讀回另計 24.817 秒／16.297 CPU 秒，
每輪獨立副本、輸入解碼及暖建均在該 warm 工作流計時外；完整 managed run 收據
另保留包含所有準備工作的總耗時／CPU，沒有把準備成本隱藏成零。
[無取樣完整公開投影對照](../artifacts/operations/incremental_round9_20261027/projection_clean_abba.json)。

### 完整值驗證：從 Arrow 原生緩衝區取值的獨立 ABI

既有 `benchmark_openbb_l1_view_binding.py` 新增
`ordered_arrow_values_sha256`，使用明確的
`openbb_ordered_arrow_logical_values_v1` 診斷身分，與原 SQL row-multiset／舊 JSON
hash 分開。讀取全部 schema（含 clock 單位／timezone）、行數、完整 null mask
及非 null 原生 values；固定寬度直接 hash 緩衝區，string／binary 保留每個值的
正規化 offset 與完整 bytes，bool 轉明確的 0／1。基本型別保留浮點位元、正負零、NaN
payload、整數寬度、Decimal、欄位與列順序。排除的只是不可達 slice 與 null
slot 的未定義垃圾；nested／未知型別仍完整走舊 JSON-value fallback，其驗證
強度沿用舊 oracle，不額外宣稱 nested NaN payload 的位元證明。

這改的是診斷驗證算法，不改 producer、公開 payload、view SQL 或 consumer query。
新 hash 不冒充舊 hash；校準先在**同一批完整返回的 Arrow tables** 同時計算兩個
oracle，核對原完整舊 SHA，再各自與完整校準值比對。測試另涵蓋 chunk／slice、
null 位置、型別差異、金融整數邊界、Decimal、時間精度與浮點特殊位元。

沿用同一 current producer 的 A/B/B/A，A 為原完整 JSON oracle，B 為新的完整
ordered Arrow oracle；每輪仍讀回上述全部 1,224,999 rows、status／catalog、主表
及 integrity check。完整 wall **76.313→38.446 秒**，CPU
**70.251→32.512 秒**，分別減少約 49.6%、53.7%。兩個 B 的完整 wall 為
26.435／50.457 秒，兩個 A 為 65.828／86.799 秒，背景負載變化仍列在原始收據。
單看 hash 分項，平均 CPU 46.774→8.825 秒；fetch Arrow 的平均 CPU 約
18.725／18.389 秒，不能把驗證改善解釋成 SQL 查詢本身變快。
新完整身分兩輪均為
`d245ae7db0b99624515f31033bd3e24b14255b1dfecc68e6502537f972bbd607`，
與校準值一致；舊完整身分仍與前述保留 SHA 一致。
這是完整診斷工作流的驗證算法改善，未移除任何返回欄位、null、值或公開輸出；
nested／未知型別的完整 JSON fallback、Arrow materialization 與主表遍歷仍有成本。
[雙 oracle 校準及完整對照](../artifacts/operations/incremental_round9_20261027/readback_abba.json)。

### 待辦寫入、完整來源 SQL 與正式採用邊界

將原未分配 writer 全流程拆開：A／B 的初始化 CPU 為 1.347／1.419 秒，直接
持久化 0.793／0.765 秒，完整讀回及其餘工作 2.830／2.929 秒，總計
4.969／5.113 秒。直接寫入下降不能掩蓋完整 CPU 增加；這個分解也不能單憑
時間差認定初始化或讀回變慢的根因。本輪沒有再增加 writer trigger、DDL 或 hook。
[原始四輪分解](../artifacts/operations/incremental_round9_20261027/writer_phase_decomposition.json)。

定期完整來源 SQL 本輪沒有改寫。SQLite 的實際 mmap 上限為 2,147,418,112 bytes，
小於固定快照的 19,861,577,728 bytes；未將不涵蓋完整輸入的 mmap 探針冒充整庫
優化，未重跑無改動的完整 SQL。第八輪順序快照的既有數字仍是前輪證據。
來源 journal／schema cookie、定期完整 SQL、來源快照、日期 audit 與所有失效回退
均保留，尚未消除全檔身分遍歷或定期完整比對。

本輪在開盤保護窗口內只做私人 fixture 與唯讀實際來源對照，沒有繞過原
07:35–13:35 guard 啟動正式 OpenBB 維護 owner，沒有重啟金融服務。
新的 compactor source SHA 會使既有 publication proof 失效一次，下一個合法窗口
須由原 owner 完整驗證並自然重建；私人 benchmark 不能替代該正式採用讀回。

三組測試的檔案範圍互不重複：compactor／世代發布／record store 焦點 182 項，
feature shards 36 項，以及 inventory／codec／membership／dashboard／pages／bar／
tick／footer／metadata／source snapshot／view benchmark 共用回歸 388 項，合計
**606 passed**。共用回歸已包含新增的 Decimal／日期／時間 cases；先前 29 項
Arrow benchmark 測試與另外的補充 smoke 不重複加總。相關 run.json 與完整 run.log
均已終結讀回，未只依啟動成功宣告驗收。

正式 Monitor 已由原排程自然產生與目前 projection ABI 相符的世代，另一全新
程序讀回完整 56,153,873-byte JSON、89,219 fields、所有 237 owners 的完整
引用物件／預覽、source pages 與 receipt，並確認四份發布檔案身分在讀取前後
不變、revision／source-observation binding 相符；當時 record-cache 身分亦與
manifest 相同。這是當前發布世代的完整性讀回，與固定輸入 benchmark 的
74,824 files／89,209 fields 分母分開；自然世代為 75,005 selected file entries，
仍是 `partial`，不代表來源最新、全部歷史完整或瀏覽器驗收。
[正式自然發布讀回](../artifacts/operations/incremental_round9_20261027/monitor_natural_readback_1793071235834357438.json)。

交付前四個金融 owner 的 PID／啟動時鐘與起始基準相同；OpenBB 仍是未啟動
的新版本正式採用 gate。程式、測試、完整對照、自然讀回、run／log 及不變 owner
證據的 SHA 索引見
[第九輪交付收據](../artifacts/operations/incremental_round9_20261027/delivery_final.json)。

## 第十輪：同世代 stats 與精確日期區塊證明（2026-10-27）

本輪將成本拆成必要的來源觀測／完整輸出，以及可以憑失效證明省去的重算。
採用的修改沒有增加 CPU、I/O、記憶體使用率上限，沒有延長等待、減少驗證、
刪除來源或重啟金融服務。A 為本輪開始時凍結的第九輪版本，B 為最終修改；
以下為新程序 A/B/B/A 平均，單位秒。公開投影採下述同 owner 的讀取模式對照。
各列是不同完整工作流，不能相加或外推整機吞吐。

| 工作流與完整讀回範圍 | wall A → B | CPU A → B | 完整 CPU 變化 |
| --- | ---: | ---: | ---: |
| 74,824 個 header 全變更、交易發布、新程序一般 JSON reader | 48.43 → 34.48 | 31.03 → 22.67 | −26.9% |
| 40,559 組新 stats、交易發布、新程序一般 JSON reader | 56.18 → 53.35 | 34.92 → 33.93 | −2.8% |
| 111,817,211 列實檔日期 audit、公開 JSON、新程序冷讀全部來源日期 | 16.11 → 10.12 | 13.11 → 8.68 | −33.8% |
| 已分派待辦：原生 1,024 筆工作生命週期及全部資料表重讀 | 12.28 → 11.75 | 5.87 → 4.53 | −22.9% |
| 未分派待辦：相同完整生命週期及重讀 | 12.69 → 13.04 | 5.29 → 5.53 | **+4.4%** |
| 完整公開投影：237 owners 全重建、完整 JSON／pages／receipt、新程序重讀 | 23.39 → 24.08 | 19.98 → 20.36 | **+1.9%** |

Monitor 的私人 owner frame 現在攜帶唯讀 stats、原始已驗證 bytes、完整
path→stats 引用與 block pool。讀取前後的檔案身分必須一致；發布在
`BEGIN IMMEDIATE` 內再次確認同一資料庫世代，才能沿用既有 bytes 與 SQL 邊證明。
同世代 header 變更從 40,559 次 stats 編碼降為 **0**，同 key 不再重做查詢／垃圾回收。
一般 reader 仍回傳可獨立修改的原生 dict/list；舊格式、外來 frame、世代變動、
新值與未知欄位仍走原驗證／重建路徑。完整 header、schema 引用及唯一 stats
讀取尚未消除。四輪完整值 SHA 同為
`74ace798887f9ffe1697247c5a4276fea9eced171f2ab240c7a1782d5e9a9566`。
[完整 header 對照](../artifacts/operations/incremental_round10_20261027/monitor_header_final_abba.json)。

新 stats 對照在私人 fixture 對每個唯一 count／整數 non-null 加一，明確製造新 facts；
沒有修改或宣稱這是來源觀測。A/B 均完整編碼 **40,559 組、47,765,298 bytes**，
完整值 SHA 均為 `b8ee4a3c8f404d72538e9530025371c7bb6b3b2e352f07f5e784722894f44263`。
本輪減少周邊 SQL 工作，沒有宣稱已解決新向量編碼。
[新 stats 完整對照](../artifacts/operations/incremental_round10_20261027/monitor_newstats_final_abba.json)。

完整公開投影另發現一個整體退步：讓只讀 reader 也建立上述私人 frame 時，
相同 canonical owner／同讀取器開關對照的完整 wall **20.82→23.17 秒**、
CPU **17.63→20.07 秒（+13.8%）**。因此最終實作只由 `refresh=True` 的
更新 owner 請求 frame；只讀聚合保留原讀取路徑，移除無法用於寫回的證明建構。
[原退步組](../artifacts/operations/incremental_round10_20261027/projection_broad_frame_abba.json)。
header／新 stats 對照所測的 store、codec、JSON 及其唯一 inventory helper
`_cache_signature` 均未再變；後續僅改兩個 owner 分派函式，原測量 source SHA
保留，以 [依賴未變證明](../artifacts/operations/incremental_round10_20261027/inventory_reader_scope_bridge.json)
連到最終原始碼。新的讀寫分界另經最終回歸與完整公開投影重測，不能將原退步組
藏在較快的 header 階段中。

最終公開投影用同一 canonical reader，A 關閉新增 frame，B 使用最終 owner 分派；
只讀路徑均不建立多餘寫回證明。74,824 檔案均完整檢查，兩者每輪同為
150,160 次 stat 與 150,160 次身分觀測，237 個 owner 強制重建，89,209 fields
完整輸出 SHA 同為 `5a0058912898096855ebeffa28ab3b6e44c188b9b4d90da2e6d55878208f4350`。
owner 工作 CPU 17.15→16.76 秒，但含獨立重讀的完整 CPU **19.98→20.36 秒**；
完整公開工作流 CPU 仍未證明改善，不能只引用前段較快的數字。
私人 fixture 的複製、bootstrap 與首次投影另含在 managed run，未併入穩態平均。
初組工具的模組別名漏記身分計數、後續控制組的跨模組 SharedSchema 不相容
均被完整輸出／公平性檢查拒絕；原失敗、中止與原始控制輸出保留，沒有當成加速。
[最終完整公開投影對照](../artifacts/operations/incremental_round10_20261027/projection_clean_abba.json)。

日期 audit 分別建立 partition member 與 clock column chunk 證明。member 重用仍由
canonical ParquetSource 驗證完整內容／schema／成員身分；普通檔案則重新雜湊
每個 clock chunk 的實際壓縮 pages，並綁定時鐘欄型別、時區、row-group 列數、
編碼、runtime policy 與精確 bitmap／count／nulls。相同 pages 可沿用日期解碼結果；
內部日期修正、成員移除／替換、版本改變、破損證明與未知 layout 都保留完整解碼。
這使用 [Arrow 的 column chunk metadata](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ColumnChunkMetaData.html)
定位實際資料，沒有用相同 min/max 代替相同內部日期。

實測來源是原有 662,279,488-byte FinLab broker_transactions 檔案，以私人 symlink
引用，來源內容及身分不變；只將私人舊 cache signature 設成 miss。
完整日期讀取由 **1,706 個 row group／111,804,416 列解碼降為 0**；仍逐一讀取／
驗證壓縮 clock bytes，並在計時內另由新程序冷讀全部來源日期比較全部業務值。
業務值 SHA 同為 `f2b7b98e2fba7d8049be556d48b8782d50a0b0fffe9965ba7fd91412a001899c`；
完整原始 JSON／cache SHA 各自重讀保留，收集耗時等診斷欄位本來就不同。
這不是全 production catalog 或實際每日 append 的吞吐結論；真正檔案重寫與
內部日期變更另有語義測試。初建證明 CPU **4.62→5.37 秒**、cache 約
**3.9 KB→516.6 KB**，成本保留。未有證明或日期 bytes 真的改變仍需完整解碼。
[完整實檔日期對照](../artifacts/operations/incremental_round10_20261027/dates_abba.json)。

待辦的 count／source-journal DDL 改為同一明確 writer transaction 內原子安裝，
減少逐條 DDL commit；不增加 writer triggers，也不改動工作欄位／journal 事件。
不支援 schema 的早期拒絕保留 caller transaction，晚期失敗完整 rollback。
已分派情境有改善，未分派情境的完整 CPU 仍增加，**不能宣稱待辦整體 CPU 已解決**。
較早組別也曾出現已分派完整 CPU 增加，均保留於
[初組](../artifacts/operations/incremental_round10_20261027/writer_atomic_abba.json) 與
[最終複測](../artifacts/operations/incremental_round10_20261027/writer_atomic_final_abba.json)。

本輪拒絕兩種沒有形成整體加速的替代方案：

- **全檔身分**：先預掃描目錄描述符版本完整 wall 17.35→21.98 秒；整合原遍歷後，
  全 186,089 檔案／兩次新鮮觀測／50,722 個 SEC proof 的完整 CPU 仍
  **10.94→11.93 秒**。候選已撤回，原路徑逐 byte 回復本輪基準，保留
  [兩組完整負向對照](../artifacts/operations/incremental_round10_20261027/identity_rejection.json)。
- **定期完整 SQL**：在同一 19,861,577,728-byte 唯讀快照保留所有比較欄位，
  欄位投影的第一個完整 B 為 **1,333.26 wall／462.12 CPU 秒**，
  A 為 **553.86／327.52 秒**，結果與 SQL hash 相同。
  快照雖縮小，來源頁面遍歷成本仍高；停止其餘私人重複測量，沒有把未完成
  A/B/B/A 當成完整平均。原實作與 SQL 已逐 byte 回復，候選 source 與完整／
  中斷紀錄保留於 [拒絕收據](../artifacts/operations/incremental_round10_20261027/sql_projection_rejection.json)。

最終共用回歸 **651 passed**，涵蓋只讀／更新 owner 分界、cache 世代、immutable／普通 reader、rollback、
新／舊 stats、精確日期 gaps／nulls／內部修正、publication、完整來源與待辦契約。
沒有把重複執行的測試數相加。日期初輪 hash 因排序方式不同失敗；保留失敗，
確認業務值相同後改用一致排序，再完整重跑四輪。其他中間測試失敗與候選撤回
也保留，不以最後 exit 0 抹除歷史。

最終 OpenBB 私人 fixture 由 canonical producer 執行，再由另一新程序讀回
全部 **1,224,999 view rows／所有欄位與 null 位置、36,854 tasks、1,024 segments、
11 endpoints、status／catalog／SQLite 表與 integrity**。原 native ordered Arrow
完整值 SHA 仍為 `d245ae7db0b99624515f31033bd3e24b14255b1dfecc68e6502537f972bbd607`；
producer 加完整讀回 31.89 秒只是一次驗證，沒有作為吞吐改善數據。
[最終完整 producer 與新程序讀回](../artifacts/operations/incremental_round10_20261027/openbb_final_acceptance.json)。

正式 Monitor 的自然世代亦由新程序完整讀回 **56,281,994 bytes／89,403 fields／
237 owners**，所有引用物件、預覽、pages、receipt、ABI 與世代 binding 一致，
讀取前後發布身分穩定，當時 record-cache 身分與 manifest 相同。
來源狀態仍為 `partial`；不代表歷史完整、最新來源、瀏覽器驗收或所有 owner 已採用。
[自然發布完整讀回](../artifacts/operations/incremental_round10_20261027/monitor_natural_readback_1793080250204893107.json)。
來源 SHA、完整測量、run.json／run.log、失敗／撤回、金融 owner 身分及剩餘 gate
統一見 [第十輪交付收據](../artifacts/operations/incremental_round10_20261027/delivery_final.json)。

## 2026-10-27 第十一輪：直接減少探索、目錄開啟與恢復重播

本輪依「總成本＝實際工作量 × 每單位成本＋等待」處理正在發生的卡頓。
沒有增加 CPU／I/O／記憶體上限、降低並行或延長等待，也沒有中止其他任務。
先分開 Linux、Windows host 與各程序的量測；程序的 100% 是一個 logical CPU，
主機百分比則以 16 logical CPUs 為分母，WSL 與 Windows 的 vmmem 不重複相加。

### 當時是誰在用，以及何時結束

19:54 初查，VS Code npm 探索的 `rg` PID 574749 已跑約 223 秒，
累積 CPU 13 分 10 秒、約 353%；它以 `--no-ignore --follow` 遞迴尋找 package.json，
連大型資料／產物目錄都走訪。這輪在設定修改前自然結束，不能歸功於修正。
另外兩個有限工作自然完成：Discord 收盤維護 19:59:51、訓練配方檢查 20:00:48。

20:44:57 起的 15.22 秒 Linux 窗口，CPU busy **59.35%**、iowait **2.74%**；
主要程序如下。這與早先 19:59 的 42.47% 並非固定工作量前後對照，不能宣稱
全機負載下降。20:38 窗口亦保留本輪私人 benchmark 的 CPU，不把它算成來源服務。

| 程序／PID | 單核基準 CPU | 工作及可知的結束條件 |
| --- | ---: | --- |
| 其他維運工作的 provider 回歸／713654 | 84.95% | 當時仍 running；20:48:24 自然結束，該輪有一個測試失敗，不能稱驗收成功 |
| FinMind sponsor／1969624 | 65.18% | 常駐來源服務，沒有固定結束時間 |
| 儲存 lifecycle／525397 | 57.56% | 常駐工作，當窗讀取約 9.57 MB/s，沒有全任務 ETA |
| 其他維運工作的日內服務驗收／718268 | 31.93% | 該 run 後來自然 succeeded，未由本輪操作 |
| Syncthing／1485 | 22.73% | 同步服務持續運作，不能由 CPU 百分比推算完成時間 |
| 正式 backup stream／702487 | 19.78% | 20:38:17 自然啟動；未提供可信的階段剩餘量 |

備份前兩個自然週期各為 **28 分 50 秒、25 分 13 秒**；來源與工作量不同，
不能當成受控加速，也不能把這個歷史耗時直接當作本輪倒數。期交所夜盤收集依
原排程運作到次日約 05:00:05，沒有為降低負載停止收集。本輪最後的完整私人
恢復重播在 20:38:38 結束，不再留下該測速 worker 長期運作。

Windows native `GetSystemTimes` 的 20:37:33／20:45:00 四秒窗口，busy 各為
**89.01%／100%**，扣除 idle 後的 kernel busy 各為 **76.03%／74.00%**。
計算方式依 [Microsoft 原生計時 API](https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-getsystemtimes)。
同期獨立程序計數包含 vmmemWSL（約 3.71／5.51 cores）、ChatGPT（約 1.11 cores）、
VS Code、多個 PowerShell；20:37 尚見 choco、cargo-install-update，20:40 的
程序名單已不見這兩個 installer。診斷自身 PID 另行標記，沒有將其冒充日常程序。
不同採樣窗不能逐項相加；native CPU 已證實 host 壓力仍在，尚不能把 kernel
耗時全部歸因於某個磁碟、驅動或 Python。沒有關閉應用程式、重啟 WSL 或停更新。
本機非 elevated session，沒有啟動 ETW 錄製；驅動層細項仍未驗證。
[Windows 原生複測](../artifacts/operations/incremental_round11_20261027/windows_kernel_load_final.json)、
[Linux 最後窗口](../artifacts/operations/cpu_disk_load_20261025/r11_final_2048.json)。

### 已實作與完整量測範圍

1. **在探索前排除資料樹**：此版 npm 的 `npm.exclude` 在搜尋後才過濾，既有
   `search.exclude` 不涵蓋這個 `workspace.findFiles` 呼叫。新增根目錄限定的
   `files.exclude` 三條規則；原監看、預設 Git 排除與 npm 自動探索保留。
   執行中的 extension host 實際讀回三條設定為 true；原 npm API 完整執行
   **0.951 秒**，找到根 package 與工作區 plugin package 共兩份。獨立 rg
   等價探索為 0.041 wall／0.124 CPU 秒。資料仍可指定路徑開啟，Explorer
   會隱藏符合內容，詳見 [.vscode 說明](../.vscode/README.md)。沒有把原程序
   的自然退出當成「223 秒降到 0.95 秒」的受控測速。
2. **先彙整完整引用，再依物件目錄順序讀 metadata**：原版本順序會反覆
   淘汰同一批目錄 FD；現在先驗證所有 digest／size 宣告，再逐一讀取唯一物件。
   每個 leaf 仍 fresh no-follow stat，保留五項身分與所有目錄 recheck；
   缺檔／短檔／symlink 仍回填至每個相關 release，目錄替換仍拒絕。
3. **恢復待辦的增量證明**：先 fresh 驗證所有 immutable plans 與 ACK。
   已有精確 NAS snapshot／envelope bytes／完整成員集合的請求不重讀本地封裝。
   不同程式封裝但相同控制 DB 的情況，首次仍完整驗證，將 dispatch／ACK／
   logical state／既有 request 的確切 binding 存入原 owner 私人 ledger。
   任一 binding、證明內容或 request 缺失即回完整驗證。這只證明請求決策
   已完成，不證明目前本地 payload 健康或 NAS 已恢復。公開 request／ACK
   契約未變；手動發布入口也在讓出 owner 前保存新證明，未變更證明不增加
   checkpoint，並保留 transport scan 期間其他 owner 的更新。

| 測量工作範圍 | 原 wall → 新 wall | 原 CPU → 新 CPU |
| --- | ---: | ---: |
| 全部 89,557 可用 packed objects 的 metadata 階段 | 502.94 → 300.24 秒 | 64.01 → 34.57 秒 |
| 完整恢復待辦重播＋原 canonical ledger 寫入＋另一程序完整讀回 | 204.06 → 5.36 秒 | 18.32 → 5.22 秒 |

物件測速採同一 1,337,933,034-byte catalog，3,823 releases、7,989,889 引用，
包括 disposition 保留的 615 個額外物件，沒有縮小可用物件範圍。以保留的
release／required-key 順序重建舊順序，**不是原 manifest 內部精確順序，也不是
完整 capture／備份週期測速**。目錄開啟 **48,008→773（−98.39%）**，stat
同為 90,330；全部五項 metadata 值 SHA 同為
`e5f57a23ed40006e224aff2ab22140ff75dcb13e3bdfcdd602ebeadd856a9d2d`。
表中 CPU 為 metadata 階段，含 fixture／輸出等的 worker CPU 另為 67.16→37.55 秒。
完整 A/B 一組，OS cache／其他工作不受控；較早 ABBA 的 wall 波動亦保留。
[全物件對照](../artifacts/operations/incremental_round11_20261027/object_full_summary.json)。

恢復重播固定真實 1,770 deliveries、395 個已 ACK 的 control deliveries、106 plans；
每輪完整比較所有請求結果／publication intents 與所有原有業務 ledger 欄位。
原生 payload 驗證 **395→首次 289→後續新程序 0 次**，不是把本地驗證換成 mock；
僅攔截對外請求發布，沒有新增實際 NAS 工作。表中新值為兩次新程序的平均，
CPU 各 5.31／5.13 秒，wall 各 5.46／5.25 秒；**已包含整份 ledger 保存與獨立
完整讀回**。首次建立證明則為 43.56 wall／9.08 CPU 秒，ledger 從
63,970,755 增至 64,151,091 bytes，增加 180,336 bytes；成本沒有省略。
全部結果 SHA 同為 `f8ca78c03e5e55de88ddfc569d9367bde7ec958fedb3191b6ac48c39ef889108`。
順序為 baseline、bootstrap、warm、warm；wall 的改善不能全部歸於演算法。
[重播、完整保存與獨立讀回](../artifacts/operations/incremental_round11_20261027/recovery_proof_final_summary.json)。

### 正式採用、失敗證據與剩餘瓶頸

20:44 的正式 source ledger 已有 **290 份新證明**，全部對照當前 106 份 plans、
fresh ACK 與 binding 驗證成功；64,167,415 bytes 完整讀取前後身分穩定。
這確認正式 owner 自然採用；上一個 25 分 13 秒週期開始於 proof 修改前，
不能拿其耗時證明 proof 已在該整輪生效。最後完整週期仍有一筆待 NAS 收據，
auxiliary 的 `SnapshotError` 原因是 **code changed during its frozen working-tree backup**；
來源仍在並行修改，正確保留拒絕，沒有把舊 code backup 冒充本輪成功。
金融四個 owner 在 20:44 讀回時 PID／start ticks 與本輪起始窗口相同；
之後另一個 `opening-r30-native-discord-v5` 工作於 20:45:40 更新 Discord，
20:49 再讀為 PID 719864，其餘三個 owner 身分仍相同。該獨立工作收據明列
57525→719864；本輪沒有執行此重啟，也不宣稱最終四個 owner 全數未變。
37 個 StockAgent／PostgreSQL 及祖先 cgroup 的 CPU／I/O／memory／task ceilings
皆無有限值。[正式 owner 與限制讀回](../artifacts/operations/incremental_round11_20261027/final_owner_readback.json)。

共用測試由既有 lakehouse control role 執行，覆蓋 backup stream、packed／offhost、
lifecycle、metadata ingestion、packed recovery、recovery queue、NAS acceptance 與
auxiliary，手動入口保存修補後 **340 passed、4 skipped（40.82 秒）**。
四項因該角色未提供真實 Restic binary 而跳過，未計為 NAS 恢復成功。
測試驗證每個唯一物件、所有 release 的完整引用／缺損、目錄替換、
全部五個 request binding、證明破損／缺失／ACK／dispatch／request 失效，以及
持久化後 no-op 不改動證明。最終結果及全部 run／log／source SHA 收在本輪
[交付收據](../artifacts/operations/incremental_round11_20261027/delivery_final.json)。
中途 manifest API 使用錯誤、proof validator 介面錯誤、測試試圖改寫 immutable
ACK、fixture 遺漏 disposition 證據、修正期間的 source-pin 拒絕，以及 fintech
缺少 temporalio 所致的失敗均保留。改用專責既有 role，沒有安裝不相干依賴或
刪除失敗收據，亦沒有將未執行的真實 Restic／NAS 恢復算成通過。

仍有實際工作尚未消除：備份 **1.34 GB catalog 的完整物化／雜湊／保存與約
八百萬引用遍歷**、Monitor 變更世代的新 stats／完整公開投影、未分派待辦整體
CPU、日期真實變動與新來源、OpenBB 全檔身分與定期完整 SQL，另有 Windows
host 的 kernel／應用程式工作。後續應在相同來源失效條件下縮小這些實際工作，
不能以較低 CPU 百分比代替完成速度；**本輪是已驗證的階段改善，尚未達到計算極限，
也未證實全機卡頓已排除**。

## 2026-10-27 第十二輪：減少完整 catalog／ledger 的編碼與複製

本輪從實際工作量拆開 CPU：來源遍歷、JSON 編碼、記憶體複製、落盤與讀回各自
量測。針對仍在執行的備份 owner，消除私人狀態的逐層排版與整份文字／bytes
中間副本；沒有增加 CPU／I/O／記憶體上限、降低並行、延長等待或中止其他工作。
原始資料、manifest／ACK／恢復驗證、原子替換及 fsync 均保留。

### 當時的 CPU owner 與結束條件

21:32 起的 15.18 秒 Linux 窗口，16 logical CPUs 的 busy 為 **65.02%**、
iowait **12.78%**；21:53:46 起的 15.20 秒窗口則為 **37.96%／0.78%**。
兩窗工作量不同，其間另有工作自然結束，不能將差異算作本輪全機加速。
最後窗口的程序百分比以一個 logical CPU 為 100%，與主機百分比分母不同。

| owner／PID | 最後窗口 CPU | 工作與可知的結束條件 |
| --- | ---: | --- |
| 備份／897986 | 171.48% | 21:53:02 新程序自然啟動；21:59 stack 仍在完整 manifest metadata 讀取，沒有可信的當輪倒數 |
| WSL 完整空間盤點 du／805926 | 92.62% | 21:17:45 開始，21:59 已跑約 42 分鐘；17,601 個根項目中已有 1,190 份完整記錄，各子樹大小不等，不能用項目比例推算 ETA |
| Syncthing／1485 | 14.87% | 常駐同步服務，無固定結束時間 |
| 夜盤 Shioaji／4142947、4143029、4143100 | 合計 29.14% | 依原夜盤排程運作到次日約 05:00:05，未停止收集 |
| storage lifecycle／525397 | 8.09% | 常駐，當窗讀取約 2.56 MB/s，無全任務 ETA |
| TEJ history／740261 | 8.02% | 來源工作持續，不能以 CPU 百分比估計全部歷史完成時間 |

初查另一個訓練配方維護程序 843116 為 76.03%，後來自然退出；沒有將其退出
歸功於本輪。D 槽 monitor 的 du／796605 亦已退出，但讀回為原有 1,800 秒
timeout、`unavailable`，保留舊 observed timestamp，**不是盤點成功**。
另一個 offline artifact retirement owner 750413 仍在獨立工作；其
reconstruction 計數已到 59,687／59,687，但 acceptance proof 尚未成立，沒有
擅自停止或稱其完成。WSL 盤點讀回只讀取現有 NUL 記錄，沒有再啟動一次全盤掃描。
[起始窗口](../artifacts/operations/cpu_disk_load_20261025/r12_before_2133.json)、
[最後窗口](../artifacts/operations/cpu_disk_load_20261025/r12_final_2154.json)、
[盤點進度](../artifacts/operations/incremental_round12_20261027/census_progress_final.json)。

Windows native 的 21:32:25／21:53:11 四秒窗口 busy 為 **97.74%／100%**，
扣除 idle 後的 kernel busy 為 **73.05%／65.77%**。21:53 的另一組程序計數
包括 vmmemWSL 約 3.14 cores、Arknights 1.33、ChatGPT 1.24、dllhost 1.11，
另有數個 PowerShell；程序計數與 host 計時窗口不同，不逐項相加，也不把 Linux
與 vmmem 再加一次。21:55 的唯讀程序分類確認部分 PowerShell 是 native I/O
bridge admission，但未把所有 shell 或全部 kernel 成本歸因於它。
另一個 Wazuh-Security distribution 已做唯讀程序清單查詢；`ps %CPU` 是生命期
平均，不能證明其當前完整 CPU 份額。沒有關閉遊戲／應用程式、重啟 WSL 或
改動全域 THP；Linux 起始窗口的 compaction stalls 與 host kernel 壓力仍須
進一步因果定位。[Windows 窗口](../artifacts/operations/incremental_round12_20261027/windows_middle.json)、
[shell 類別讀回](../artifacts/operations/incremental_round12_20261027/windows_shell_owners.json)。
未輸出原始或解碼後的 shell command line。較早 canonical native file-I/O bridge
驗證失敗亦保留；後續唯讀 CPU／程序查詢未操作 Windows 來源檔案，不能替代或
繞過檔案 I/O admission 證明。

### 實作與完整固定輸入對照

共用 `atomic_write_chunks` 沿用原本暫存檔、原子替換、file／parent fsync 及失敗
清理。`atomic_write_json` 的串流是明確 opt-in；外層逐列 framing，每列交給
標準函式庫 C encoder，保留原數值與字串拼法。其他 writer 的預設格式未變。
私人 `catalog.json` 與 ledger 的全部 18 個保存入口採用 compact stream；
公開 request／ACK／status、業務欄位、clock、canonical identity 與 checkpoint
時機未改。這改變私人檔案排版與 raw SHA，**不宣稱新舊 JSON 原始 bytes 相同**；
新串流 bytes 另對標準 compact `json.dumps` 驗證。

固定真實完整輸入：catalog **1,350,930,823 bytes**，含 **3,836 releases、
97,735 files、8,073,493 required references**；ledger **64,191,924 bytes、
1,775 deliveries**。以 no-follow FD 複製前後五項身分及完整 SHA 確認未變，
沒有用 hardlink 改動來源 ctime／nlink。來源準備另花 **34.70 wall／32.40 CPU 秒**，
未藏入新版本或省略。私人 fixture／完整輸出位於 `/var/tmp`，寫入前通過容量檢查。

每類按 **A/B/B/A** 共四輪，合計八輪：載入全部輸入、呼叫原／新 canonical
writer，再由另一個新程序重新雜湊完整原始輸入、解碼原始與實際輸出、比較
**全部欄位與數值**，保存輸出 raw SHA。下表完整 CPU 包含 writer、獨立讀回、
子程序匯入與父程序成本；完整 wall 含啟動與等待。每個版本為兩輪平均。

| 完整保存與獨立讀回範圍 | 原 wall → 新 wall | 原 CPU → 新 CPU |
| --- | ---: | ---: |
| 全 catalog | 106.86 → 82.94 秒 | 102.27 → 77.36 秒（−24.36%） |
| 全 ledger | 8.25 → 5.47 秒 | 7.86 → 5.27 秒（−32.88%） |

單獨保存階段 catalog CPU **39.93→18.18 秒**，ledger **3.80→1.05 秒**；
writer catalog 峰值 RSS **4.67→2.88 GiB**，ledger 約 **372→224 MiB**。
完整 catalog 輸出降為 **1,267,959,623 bytes**，ledger 為 **45,727,466 bytes**。
來源 capture、全檔身分遍歷與完整備份週期不在此測速範圍；這仍會寫出整份 JSON，
不是增量發布。來源及版本 SHA、每一輪成本與完整讀回結果均保留於
[八輪原始收據](../artifacts/operations/incremental_round12_20261027/publication_abba_summary.json)。

負向變動沒有刪掉：首輪 baseline 載入 CPU 為 36.55 秒、最後 baseline 為
12.71 秒，OS cache／其他工作並未受控；第一輪 candidate 獨立讀回 CPU
43.71 秒，高於首輪 baseline 的 35.91 秒，第二輪 candidate 則為 34.52 秒。
因此表中只主張這組固定輸入 ABBA 的完整平均，不將單段節省全部外推為全機或
全備份加速，也不宣稱統計信賴區間或硬體極限。

### 正式採用、驗證及下一個瓶頸

21:59:37 正式 owner 的 **45,736,396-byte ledger** 已完整讀回，讀前後五項
身分穩定、mode 0600、compact 格式。全部 **1,776 dispatches、1,774 ACKs**
依 canonical validator 通過；對照當前 **106 plans**，**291 份 request proofs**
皆有效。這證明正式 ledger 保存已自然採用；新一輪 catalog 尚在 metadata
capture，不能說整個新週期已完成。上一輪在 21:52:30 結束，耗時
**1,616.07 秒（26 分 56 秒）**，仍有兩筆待 NAS 收據、auxiliary 無錯誤；
它開始於本輪修改之前，不能當新版本加速或當前倒數。
四個金融 owner 的 PID／start ticks 與本輪起始相同；39 個 StockAgent／
PostgreSQL 及祖先 cgroup 的 CPU／I/O／memory／swap／task ceilings 都無有限值。
[正式完整讀回](../artifacts/operations/incremental_round12_20261027/final_owner_readback.json)。

相關共用回歸為 **437 passed、4 skipped，13 個測試模組**：lakehouse control
role 376 passed／4 skipped（59.62 秒），downloader fintech role 61 passed
（9.18 秒）。覆蓋完整 backup／recovery／lifecycle，以及共用 writer 的 byte
parity、Unicode／精確整數／null、晚期編碼失敗保留舊檔、暫存檔清理、0600 與
fsync 次序。四個 skip 是角色未提供真實 Restic binary，未算成 NAS 恢復通過。
最初將 downloader 測試一同放入 lakehouse role，因缺少 `requests` 而 collection
失敗；原失敗收據保留，改由已有專責角色執行，沒有安裝依賴或移除失敗案例。
交付來源清單首輪將 import 自動產生的 bytecode 納入而拒絕；已將可維護來源與
產生檔分開列證，保留該拒絕證據，未改動已驗證的程式。
最終 source pins 與全部 run／log 收於
[第十二輪交付收據](../artifacts/operations/incremental_round12_20261027/delivery_final.json)。

接下來仍須解決完整 manifest／header 讀取與約八百萬引用的物化／遍歷、完整
catalog 雜湊與發布、Monitor 新 stats／完整公開投影／日期變動、OpenBB 全檔
身分與定期 SQL；需要綁定來源變更的增量證明，不能只沿用 mtime 或跳過驗證。
Windows bridge 的重複 admission／shell 啟動、host kernel 與應用程式工作亦待
分別量化，本輪沒有導入未經 native 檔案驗收的跨程序快取。
**這是已驗證的階段改善；尚未達到計算極限，且尚未證實日常卡頓已排除。**

## 2026-10-28 第十三輪：減少分鐘資料重複分類、排序與中斷重算

本輪接續同一 CPU／磁碟與測速任務，未增加 CPU、I/O、記憶體、執行緒或 task
佔用上限。第一性原理是總 CPU 成本取決於「實際處理次數 × 每次成本」：先減少
重複分類和排序，再讓已驗證的完成分區跨中斷存活；較低瞬時 CPU 不是驗收條件。

### 當下負載、等待與 ETA

以下皆為 2026-10-28 的新觀測，百分比若標示單核，100% 表示一個邏輯 CPU，
不能與整機 16 核百分比直接相加，也不能把 Windows vmmemWSL 再加到 Linux 程序。

- 21:52 的 15.29 秒 Linux 窗口為 busy **66.60%**、iowait **6.69%**；
  Syncthing PID 1485 為單核 120.41%、WSL plan9 PID 7 為 95.10%、
  正式推論 PID 173257 為 90.85%、狀態快照 PID 310273 為 89.54%。
- 22:27:42 結束的 15.33 秒窗口為 busy **69.94%**、iowait **6.18%**，
  swap 讀取約 0.8 KB/s、寫入約 90 KB/s。這不支持把已配置的 swap 總量當成
  當下大量換頁，也不證明本輪修改使整機負載下降。
  該窗口的兩組工程 pytest PID 582467／577944 分別為單核 96.35%／94.72%；
  Syncthing 1485 為 91.20%、Vast 回傳整理 503536 為 72.67%、
  推論準備 528506 為 53.30%、資料集配方維護 502427 為 52.25%。
  Shioaji storage monitor 群組另為 60.49%。這些工作並非同一個可相加的剩餘佇列。
- 因前述程序已退出或換階段，另於 **22:32:06** 完成 15.25 秒讀回：busy
  **65.35%**、iowait **3.25%**；新的完整歷史推論 **PID 669206 為單核
  268.10%**，資料發布 671642 為 56.06%、lifecycle 4165485 為 52.13%，
  Syncthing 1485 為 25.51%。前述兩個 pytest 已退出，不能繼續列成當下主因。
  此次來源及 owner 階段已不同，不是控制條件相同的整機 A/B。
- 22:26:07 Windows 原生四秒 GetSystemTimes 窗口仍為 **busy 100%**，
  扣除 idle 後的 kernel busy 為 **65.66%**。另一組格式化程序計數器中，
  vmmemWSL PID 56784 為單核 583%，Code PID 50704／35524 為 135%／90%，
  dwm 82%、MuMuVMMHeadless 75%、Arknights 60%。計數器窗口與 Linux 不同，
  不把它們加總或以 vmmemWSL 反推單一 StockAgent owner；未關閉應用程式。
- 舊完整歷史推論在 **21:59:21 達到既有 3,600 秒 timeout**；最後紀錄是
  1,100／1,621 個分鐘分區、2,170.4 秒。原有條件式剩餘時間推估因此失效，
  不是正常完成。維護服務依原排程重啟後處理其他模式，22:31:24 才再啟動
  `tw_day_trade_v8_annual_log_cash`。完整推論／報告的 `whole_job_eta` 仍為 null。
- 備份 PID 92100 從 20:26:09 執行，在 22:31 已超過兩小時，但 CPU 計時仍
  為 6 分 49 秒；kernel stack 與 `/proc/locks` 證實正在 `flock` 等待
  `/var/lib/stockagent/backup-stream/owner.lock`。持鎖者為新聞私有備份
  PID **197541**，不是 CPU 配額不足。沒有足夠的剩餘工作量與速率證明可估 ETA，
  未中止持鎖者或繞過 canonical owner lock。

持續同步／GUI 沒有固定整體結束時間；有限批次也必須取得其剩餘單位與速率才可
估時。未用前一輪耗時、服務下一次 retry 時刻或 lifetime `%CPU` 冒充倒數。
觀測詳見[起始窗口](../artifacts/operations/cpu_disk_load_20261025/r13_before_2154.json)、
[22:27 窗口](../artifacts/operations/cpu_disk_load_20261025/r13_final_2228.json)、
[22:32 讀回](../artifacts/operations/cpu_disk_load_20261025/r13_readback_2232.json)、
[Windows 原生窗口](../artifacts/operations/incremental_round13_20261028/windows_final.json)、
[逾時紀錄](../artifacts/operations/incremental_round13_20261028/inference_timeout.txt)與
[持鎖證明](../artifacts/operations/incremental_round13_20261028/backup_lock_wait.json)。

### 已實作與完整輸出驗證

對當時真實推論做一次 10 秒、50 Hz 的有界取樣，共 477 個樣本；
`_dense_bars` 包含 160 個、分鐘機會建置包含 257 個，兩者不可假設互斥。
`np.unique` 排序占 84 個樣本，商品分類也在多個分鐘 tick 驗證中重複出現。
這是該準備階段的診斷，不是完整 fold 的時間比例。

1. `_dense_bars` 以已驗證範圍內的 dense slot `bincount` 取代 `np.unique`
   排序，從 O(N log N) 改為 O(N + 270S)。保留全部原始值、原有成交量累加次序、
   缺值與首個重複商品／分鐘的拒收；300 個重複列也不會計數溢位。
2. 商品類別在原始 compact metadata 上驗證與分類，之後才廣播布林結果。
   `[商品, 1]` 不再先擴成 `[商品, 270]` 字串陣列重複分類；全部 OHLC、日期、
   商品 tick 與價格合法性仍照原規則驗證，也保留空陣列與不相容形狀的行為。
3. `first_minute_only` 不再配置未使用的 `entry_path`／`stop_hits`；
   掃單模式仍保留。以 2,767 個商品計，每個日分區少配置 23,906,880 bytes，
   這是陣列大小推導，不是已量測的整機 RSS 降幅。
4. `PhysicalSessionReuse` 在同一建置 owner／writer lock 下，以單一
   `.session-builds.jsonl` 追加及 fsync 每個已完成分區的證明，避免完整快取未
   提交便逾時後全部重算。每筆綁定 checkpoint contract、來源 digest、完整
   `input_key`、輸出 bytes／SHA 與 gaps／valuation／counts 診斷；仍在查詢前
   核對來源分區完整 SHA。恢復只接受同一未完成世代，並完整驗證唯讀輸出 SHA；
   損壞／變動／無證明的分區重算，截斷尾行不會使已完成紀錄失效。
   跨世代沿用仍要求原有已提交 manifest／READY，完整世代的 READY 驗收不變。

固定兩個真實完整日分區 **2020-03-02／2026-09-10**，共 **791,569 列**、
完整 **2,767** 商品軸；分別有 1,670／2,024 商品通過本測速的 volume／limit
選取條件。條件不包含 production 所有 corporate／source-gap gate，因此另外
以兩日三商品的明確合成 fixture 執行完整 canonical builder／consumer oracle，
不把兩個真實日分區稱為完整 3,124 日歷史推論。

最終來源版本的 A/B/B/A 包含讀入、全分鐘轉換、輸出寫入／雜湊及另一程序完整
讀回，未省略資料、壓縮驗證或輸出欄位：

| 兩日完整工作流，兩次均值 | 修改前 | 修改後 |
| --- | ---: | ---: |
| CPU 秒，包含獨立完整讀回 | 52.4834 | 36.9001 |
| Wall 秒 | 61.2393 | 39.4671 |
| 其中轉換及寫入階段 CPU 秒 | 27.5913 | 19.0401 |

均值 CPU 減少 **29.69%**、wall 減少 **35.55%**。依序四次完整 CPU 是
**A 72.0782、B 48.3704、B 25.4299、A 32.8886 秒**，存在 B 比最後 A 更慢的
負向比較；相同值的獨立讀回本身也有 8.83–20.64 CPU 秒波動，不能把全部差額
歸於演算法。加入復原證明前的第一輪也完整保留，A/B/B/A CPU 為
87.5673／48.6215／20.2298／33.7822 秒。兩輪的每次真實日分區輸出均逐一
核對 **16 個陣列的 dtype、shape 與所有位元**，不是抽樣或只比總和。
小型完整 builder／consumer 每次另比對 **8 個檔案、52 個陣列**；它的單次
時間不作 production 速度結論。

另外在獨立子程序於分區與 journal 已持久化、全局 manifest／READY 尚未建立
時直接 `os._exit(91)`。另一個新程序恢復時禁止再次呼叫 parser／session
serializer，仍完成 READY 與全部 52 個陣列的原版對照；分區 inode／SHA 及
journal SHA 均不變，`resumed_input_sessions=1`、`written_sessions=0`。
這證明新完成分區可跨程序中斷存活，沒有把舊作業留下、缺少此證明的 1,100
個分區回溯認證，也不是實機斷電／完整歷史成功的證明。

最終回歸 **155 passed、7 skipped，9 個模組，86.40 秒**，涵蓋日期商品 grid、
分鐘 schedule、完整來源、cache compaction／reuse、來源依賴改變、SHA 損壞、
唯讀性、symlink、截斷 append 與跨中斷恢復。七個 skip 是缺少 receipt-backed
遠端 futures fixture，未算成該遠端資料驗收，也不是第十二輪的 Restic skip。
原始 fixture／來源 SHA、兩輪每次量測與讀回、突退出證明保留於
[最終對照](../artifacts/operations/incremental_round13_20261028/final_physical_abba_summary.json)、
[初輪對照](../artifacts/operations/incremental_round13_20261028/physical_abba_summary.json)、
[跨程序復原](../artifacts/operations/incremental_round13_20261028/abrupt_resume_summary.json)與
[來源固定](../artifacts/operations/incremental_round13_20261028/physical_fixture_final.json)。

### 正式採用與剩餘工作

四個金融 owner 的 PID／start ticks 與本輪起始一致；讀回 **40 個** StockAgent、
PostgreSQL 及祖先群組，未見有限 CPU、I/O、memory high/max、swap、task 或
cpuset 上限。詳見[資源及 owner 讀回](../artifacts/operations/incremental_round13_20261028/final_owner_readback.json)。

**本輪是工作目錄中的已驗證實作，正式推論尚未採用。** 對 22:24 啟動的
PID 528506 實際 namespace 比對，三個修改來源都仍為舊掛載版本，不能將它的
自然退出、後續成功或下一輪進度當成本輪成果。正式金融 owner 綁定來源 release，
工作目錄的 price rules 還含本輪之前的其他變更；必須沿既有 qualified release
流程審核精確差異、完成 owner／engine ACK，再驗證新載入版本的完整建置／推論，
不能直接熱改唯讀掛載或發布整個 dirty checkout。本輪保留這個採用缺口，未重啟
Gateway／交易／來源 owner。證據見[實際掛載版本](../artifacts/operations/incremental_round13_20261028/runtime_adoption.json)。

剩餘成本包括來源變動後完整 panel／特徵聚合、缺少 checkpoint 的舊未完成世代、
跨變更未提交世代的復用、新聞發布長時間持有共用 backup owner lock、Windows
bridge 重複 admission／程序啟動與 host kernel／其他應用程式。前輪已列出的
完整 manifest／header、Monitor 新 stats／公開 JSON／日期 audit、OpenBB 全檔
身分遍歷與定期 SQL 也沒有在本輪消失。這些都要各自的來源失效證明與完整
工作流驗收；**未達計算極限，未證實整機卡頓已解除**。
可維護來源、精確本輪差異、已終結的受管理 run 與未解決項目彙整於
[第十三輪交付收據](../artifacts/operations/incremental_round13_20261028/delivery_final.json)。

## 2026-10-28 至 10-29 第十四輪：跨世代保留已完成工作，正式採用精確版本

本輪仍沿用原 CPU／磁碟及測速任務。沒有設定 CPU、I/O、記憶體、swap、task
或執行緒佔用上限。第一性原理是減少重複工作及搬移的 bytes，並驗證正式 owner
確實採用；較低的瞬時使用率、工作自然退出及單段函式測速皆不能代替全流程改善。

### 誰在使用 CPU，以及還要多久

- **10-28 23:22:57–23:23:12** 的 16 核 Linux 窗口為 busy **70.29%**、
  iowait **11.50%**，swap 讀／寫約 2.4／0.8 KB/s。單核計數下，選擇權來源
  audit PID 878668 為 **141.83%**，完整歷史推論 669206 為 **94.64%**，
  資料配方維護 874306 為 **51.70%**；另有兩個 `du` 約 42.1%／41.4%。
  其中一個 `du -x -B1 --max-depth=1 /root` 排除了 stockAgent 及 finlab_db，
  屬另一個目錄容量盤點，沒有總工作量／ETA 收據；本輪未中止它。
- 推論 669206 仍載入舊正式 source release：22:46:13 開始重建 1,621 個分鐘
  分區，23:30:33 到 **1,200／1,621**；23:31:36 被原本的 **3,600 秒**
  完整歷史期限終止。這是失敗，不是完成。收據列 **10-29 00:31:36** 為下次
  允許重試的時間，仍須通過原資料／工作准入；它不是預計完成時間。
  舊程序沒有本輪分區證明，不能事後把 1,200 份未驗證輸出當成可恢復成果。
- **23:40:26–23:40:43** busy 57.92%、iowait 1.80%；當時來源特徵發布
  PID 937566 為單核 **307.88%**。後來這個階段也已退出。
- **10-29 00:00:11–00:00:26 附近的最終窗口** busy **47.67%**、
  iowait **2.30%**，swap 讀約 0.54 KB/s、寫約 12.7 KB/s。主要在跑的
  是 FinMind 範圍查核 PID 1012080 **86.69%**、狀態快照 1020035
  **82.60%**、Syncthing 983432 **34.50%**，另有冷發布 981185
  **16.23%**、lifecycle 4165485 **15.31%**。私人備份 853121 為
  **10.42%**，取樣末在 `p9_client_rpc` 等待；整輪 ETA 仍未知。
  程序／來源與前一窗口不同，這些數字不是受控的整機 A/B。
- Windows 原生四秒觀測：23:24:52 busy **100%**，扣除 idle 的 kernel busy
  **54.22%**；10-29 00:00:19 busy **99.41%**、kernel busy **68.14%**。
  後一組格式化程序計數器有 vmmemWSL 366%、兩個 PowerShell 212%／116%、
  MsMpEng 128%、AsusIMEService／dwm 各 91%。前一組另見 MuMu／Arknights。
  它們與 Linux 窗口不同，vmmemWSL 也不能再加進 Linux 總量；本輪沒有關閉
  這些應用程式，亦未證明 Windows 卡頓已消失。00:03:30 的針對性讀回確認
  該次列出的三個 PowerShell PID 已退出，不能持續把其舊百分比列為當下負載。

### 實作：完成分區的證明不再綁死整份來源世代

第十三輪的 journal 可以恢復同世代，但來源 metadata 換代後，未完成世代不能
提供分區。現在 canonical `PhysicalSessionReuse` 可在原世代 writer lock 下，
讀取其完整、私有、未變動的 journal；只在完整輸入鍵相同、診斷向量／計數有效、
輸出唯讀及完整 SHA 相符時，沿用該單一分區。命中前仍核對來源分區完整 SHA；
鎖忙、journal 改變、輸出損壞、key／世代／symbol 不符，都退回原本的計算路徑。
新統計 `reused_checkpoint_sessions` 與同世代 `resumed_input_sessions` 分開。

這不把未完成世代發布成 READY。新世代仍須重建完整診斷、驗證全檔，再發布自己的
manifest／READY。突退出驗證是在實際凍結版本內：第一個程序完成分區後
`os._exit(91)`，只修改私人測試來源的 metadata revision，再用新程序恢復。
恢復階段的 parser／serializer 被設為一旦呼叫就失敗，結果兩者呼叫皆為 **0**，
沿用 **1** 個分區、寫出 **0 bytes**；原分區 inode／SHA 與 donor journal
保持一致，舊世代仍無 READY，新世代完成 READY manifest SHA 驗證。
獨立新程序讀回完整 builder 與 downstream consumer 的 **8 檔／52 陣列**，
逐位元相同。這是兩日三商品完整合成來源驗證，不能冒充 1,621 分區的正式恢復測速。

### 實測：保留退步，區分速度與已減少的工作

精確正式基底對照的真實輸入仍為兩個完整交易日、791,569 列、2,767 商品；
選取條件包含分鐘成交量、官方日量界與有效限價，完整公司事件／來源缺口准入另由
上述完整 builder 驗證。每次測速包含新程序載入、轉換、持久化、完整 hash，及
另一新程序的全部 16 陣列讀回；namespace 控制程序的 CPU 不在 worker＋reader
CPU 合計內，wall 包含 namespace 啟動／驗證。未縮減輸出或省略讀回。

| 完整 A/B/B/A 組別 | 舊版 CPU 均值 | 三檔候選 CPU 均值 | wall 舊／候選 |
| --- | ---: | ---: | ---: |
| 第一組 | 19.296 秒 | 21.226 秒 | 24.918／28.726 秒 |
| 其餘本輪回歸退出後的第二組 | 24.589 秒 | 20.131 秒 | 42.382／27.595 秒 |
| 全部八次合計 | 21.942 秒 | 20.678 秒 | 33.650／28.161 秒 |

第一組 CPU **反而增加 10.00%**，並未刪除。全部八次合計 CPU 少 **5.76%**、
wall 少 **16.31%**；讀回、初始化、I/O 與當下競爭仍有波動。小型完整 builder
單次 CPU 8.945→10.882 秒也較慢。這些測速針對原三檔候選的計算修改，
跨世代新增路徑另外由完整恢復驗證證明少做 parser／serializer，不能把 5.76%
宣稱為新恢復路徑的收益、整段歷史推論的收益或計算極限。

### 精確版本正式採用與驗收

正式 base 有同檔案內不同於研究工作區的價格規則。因此新增
`freeze_discord_code_release.py --include-root`，配合已驗證的 `--base-release`
及明列 `--include-source`，從私有、非 symlink 的 review 副本凍結。未選檔案
保持正式 base；copy、完整 hash、seal、namespace 及既有換版准入皆保留。
實際新 release 的 **1,624 檔只有三檔不同**，其餘 **1,621 檔**與正式 base
一致，沒有把工作區其他歷史價格規則修改一起帶入。

23:25 的換版檢查因原工作仍進行而 exit 75，沒有強行重啟。原工作結束且檢查
轉為可執行後，完成凍結版本測試，再於 **23:59:25** 由正式 qualified reload
取得 `accepted_code_admission_only`。新 release 為
`20261028T154706Z-ef5cbb4df1ec`，Gateway Python PID **1014356**。
獨立讀回確認三個 source SHA 與實際 `/proc/<pid>/root` 一致，四個受保護服務
的 PID／start ticks 連續；三個帳本檔約 869 MB 的原始前綴 SHA 保持一致。
這是程式、Gateway、精確 ACK 的接受，**不是 GPU warmup 或完整歷史資料就緒**；
背景仍有三個失敗的歷史補建工作，既有失敗收據及重試條件均保留。
有效服務及祖先 **44 個 cgroup** 未見有限 CPU／I/O／記憶體／swap／task／CPU-set
上限；本輪沒有修改資源政策。

驗證包含發布／qualified reload／維護 owner **408 passed**，
增量 producer／compaction／完整 source **79 passed**，實際凍結版本內
**48 passed**（與前組有重疊，不相加成唯一案例數），以及上述突退出／完整讀回。
第一次封裝測試漏了測試相依檔；第一次恢復的證據 collector 混收兩個價格快取；
一次重跑的啟動收據記錄系統 Python、缺少 Torch。失敗皆保留。補齊封裝、按實際
世代選擇讀回，並用 runtime_env.sh 明確選定的 fintech 解譯器後，正式驗證通過；
未安裝其他角色套件，也未變更全域啟動器。

原始窗口、兩組完整測速、凍結版本測試、突退出證明、換版及獨立讀回收據保留於
[第十四輪證據](../artifacts/operations/incremental_round14_20261028/)。
價格快取仍以整份 source digest 為 key，完整 panel／來源 SQL、未涵蓋的 header／
新 stats／公開 JSON／日期 audit 及 Windows 競爭也仍存在；尚未證明完整歷史
推論、整輪備份或日常互動延遲加速，不能宣稱已達計算極限。
另在原維護 owner 看見 `tw_day_trade_unit_mgda` 的
`No valid walk-forward folds could be constructed`；其整輪維護仍花費約
11 分 42 秒 wall，不能把整輪成本全部歸給這個 market。後續須沿既有 calendar
preflight 查明資料／fold 範圍，避免對已知不可用日曆重複做昂貴準備，不能以
跳過該 market 或偽造完整資料來當作效能改善。

## 未完成的邊界

- 本機 StockAgent CPU／I/O／記憶體／swap／task 硬性上限已撤回。完成速度依同輸入、完整輸出與正式 owner 收據驗證，
  不以較低 CPU 使用率或把運算改成等待作為改善指標。
- 備份的原高記憶體階段在正式前後窗口前已自然結束，不能把後續少換頁全數歸功於新預算。
  同一備份 owner 後來又出現原有 HTTP 500；保留失敗和既有 timer 重試，沒有冒充備份成功。
- 來源與 metadata 已分列交易式增量發布；第四輪共用 stats／null vector，第五輪減少
  schema pair 物化與 header SQL 遍歷，第八輪未變更世代可沿用完整已驗證 facts。
  完整一般 reader 及變更世代仍讀取全部 header／schema 引用及唯一 stats，變動列
  仍須驗證／編碼新 stats；第十輪對同世代未變 stats 沿用完整 bytes／引用證明，
  header 完整 CPU 已在上述固定輸入對照改善，新 stats 編碼仍未消除。
  完整公開投影工作流 CPU 仍未證明改善。日期 audit 已可重用經實際 clock pages 驗證的精確結果，初建、真實 clock 變動與
  未涵蓋的來源仍有成本。未分派待辦整體 CPU、全檔身分遍歷、定期完整 SQL、
  完整特徵聚合、其他 receipt／目錄／SQL 遍歷與整份公開 JSON 發布也仍有成本；這是有界範圍的
  效能改善，尚未達到計算極限。
- 群組外的期貨財務驗證、來源發布、lifecycle、其他維運測試和 Windows 程序仍可用 CPU／磁碟。
  本輪沒有中止這些任務，沒有重啟正式交易服務，也沒有刪除原始資料。
- 前序 R188、R192 測速工作已實際終結，分別為 failed、succeeded；未再追加全量 profiler。
  R192 的 run 成功不替代其 payload 的完整讀回。

原始觀測與可重跑的輕量腳本保留於
[本輪收據目錄](../artifacts/operations/cpu_disk_load_20261025/)。
