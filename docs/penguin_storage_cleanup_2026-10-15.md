# penguin 儲存清理：2026-10-15

## 原則與範圍

penguin 是資料與服務主機，不是可隨意重建的訓練節點。本次只回收本機已證明
重複或可重新下載的副本；不刪 D 主冷庫、NAS 歷史、唯一來源、服務帳本、模型、
環境或救援原件。原清理階段未停止服務、不操作 Vast、不調整 ext4 保留空間。
使用者後續明確允許關閉 WSL 擴容；這是獨立維護分支，不把派送或預期結果寫成已完成。
21:27 使用者另確認可將 ext4 保留比例調為 1%，已完成並獨立記帳，不算刪檔量。
數字以十進位 GB 表示。

任務：`penguin-safe-storage-cleanup-20261015`。各次執行與完整日誌在
`artifacts/operations/agent-workflow/runs/<run-id>/`。本文件只保存摘要，私有驗證
內容與大型輸出不加入 Git。

## 已完成的回收

| 項目 | 回收實體配置空間 | 驗證與保留內容 |
| --- | ---: | --- |
| npm 套件下載快取 `/root/.npm/_cacache` | 0.878 GB | 無程序引用；使用 npm 自身的 cache clean；不刪專案或已安裝套件 |
| 已輪替 `/var/log/syslog.1` | 1.168 GB | 保留 `.gz`；gzip 完整驗證與解碼 SHA-256 相同；只刪未變更且未被使用的未壓縮副本 |
| Vast 停機救援的獨立驗證解壓副本 | 13.541 GB | 31 檔逐一比對完整 SHA-256、大小與封存清單；隔離後再次檢查；C 原始兩個封閉套件與所有驗收收據仍保留 |
| apt 套件下載快取（21:23） | 0.180 GB | `apt-get clean`，配置量 180,170,752 → 20,480 bytes；不刪已安裝套件 |
| 合計 | **15.767 GB** | D 冷物件刪除 0、NAS 歷史刪除 0、救援原件刪除 0 |

救援副本的本次比對是「仍保留的本機原件與驗證副本完全一致」，不是重新宣稱
D／NAS 已完成一次新的獨立恢復。精確交易收據：

- `artifacts/operations/storage-cleanup-20261015/private-validation-dry-run/private-backup-validation.result.json`
- `artifacts/operations/storage-cleanup-20261015/private-validation-apply/private-backup-validation.journal.json`
- `artifacts/operations/storage-cleanup-20261015/private-validation-apply/private-backup-validation.result.json`
- `npm-download-cache-cleanup-20261015T063604-fdbaa676`
- `compress-closed-system-log-20261015T063730-3d9b9961`
- `private-validation-copy-cleanup-20261015T064612-098e9c45`

## 磁碟與主要候選

### 21:27 重新量測與保留空間調整

上次 Windows 擴容任務實際為 **needs-attention**，錯誤
`WSL_E_DISTRO_NOT_STOPPED`（離線 resize 前 distro 又被開啟）；原生收據
`20261015T164726446-Expand/status.json` 沒有完成 resize，容量仍為 **2 TiB**。
原三個 Windows startup tasks 已恢復；本輪不重新停機或自動重送擴容。

使用者同意後，核對掛載 `/dev/sdd`／ext4／UUID
`b37fb7b3-fe96-4301-a5ba-0e5363ff854c`、536,870,912 blocks × 4,096 bytes，
才用 `tune2fs -m 1 /dev/sdd` 將 reserve 26,843,544 降至 5,368,709 blocks。
保留 **21,990,232,064 bytes**，釋出一般配置額度 **87,960,924,160 bytes**；
即時 `df` 可用 11,340,734,464 → 99,301,494,784 bytes。
此操作未刪資料、未擴容、未縮 VHDX；同時寫入使 `df` 差額略小於 reserve 變化。
完整收據：`ext4-reserve-one-percent-20261015T132712-50f2f912`。

另外觀察到 ext4 保留 2026-09-01 的兩筆 filesystem error marker，最近錯誤時間
仍為該日；本次未發現新錯誤。應另安排離線 filesystem 檢查，不能在線執行
`e2fsck` 或以清除 marker 冒充修復。

晚間完整 `du -x -B1` 掃描後已完成（兩筆 inventory runs 均 exit 0）；
五個大型服務資料樹的聯合 inode 計數與架構瘦身方案，見
[服務儲存架構分析](penguin_service_storage_layout_2026-10-15.md)。
特別是 `/srv/stockagent-live` 內含 46.44 GB 衍生 panel cache，不能全當來源原件，
也不能因其為 cache 就直接回收。
本輪核心服務（Discord、台股模擬、兩個 dashboards、Syncthing、PostgreSQL）
均 active，未因本輪清理停止；這不替代各服務的業務驗收。

### 較早量測與維護歷史

起始 WSL ext4 可用約 **24.65 GB**，使用率 99%；它的虛擬容量約 2.16 TB。
Windows C 尚有空間，不等於 WSL 檔案系統有足夠可配置空間。D 起始可用約
**1.77 TB**。回收會增加 WSL 可用空間，不代表 VHDX 立即縮小；同時執行的下載、
服務與研究也會持續寫入，所以不能把前後 `df` 差額當作唯一回收證據。

2026-10-15 14:56（台北）回讀 WSL 可用約 **37.24 GB**，仍為 99%。本次沒有停止或
重啟服務；Discord、台股模擬、public dashboards、TAIFEX dashboard 與 Syncthing
的 canonical systemd units 當時均 active。這只是程序存活核對，不是完整業務驗收。

使用者補充 WSL 最多可用 2.5T；本次先保守解釋為十進位 **2.5 TB** 上限。
目前 block device 是 2 TiB（2,199,023,255,552 bytes），不是已擴至 2.5 TB。
WSL 2.7.13 的本機 CLI 支援 `--manage --resize`；Microsoft 的標準流程先 shutdown，
所以本次不在服務與恢復交易運行中關閉 WSL 或更改底層 VHD。Windows C 14:58 回讀
可用約 443.09 GB、D 約 1,766.64 GB；未預占、搬移或擴容。參考
[Microsoft WSL 磁碟管理](https://learn.microsoft.com/en-us/windows/wsl/disk-space#expand-vhd-size-using-wsl---manage)。

後續使用者已明確同意關機擴容。本次 worker 目標採用 MiB 對齊的
**2,499,999,170,560 bytes**，不超過十進位 2.5 TB。WSL 2.7.13 的
[`SetSparse` 原始碼](https://github.com/microsoft/WSL/blob/2.7.13/src/windows/service/exe/LxssUserSession.cpp)
明確阻止一般 sparse 啟用，並要求不安全覆寫；本次不使用該覆寫。
安全替代為 hourly TRIM，加上 Windows 只在 WSL 已停止時做 read-only／Quick
compaction。它不自動中斷常駐服務、不零填整顆磁碟，也不保證在線立即縮檔。

新 worker：`scripts/maintain_windows_wsl_storage.ps1`；19 項 focused 契約測試通過，
Windows PowerShell 解析與實機 Plan 通過。Plan 確認唯一 distro `Ubuntu-26.04`、
動態獨立 VHD、原容量 2 TiB；16:42 C 可用約 442.91 GB，可涵蓋未來擴容量與
32 GiB reserve。正式擴容／啟動驗證以 Windows 原生
`%LOCALAPPDATA%\StockAgent\WslStorage\receipts\<time>-Expand\status.json` 為準；
派送、`admitted`、`running` 不等於完成，`needs-attention` 需查看同目錄日誌。

16:45 已部署 hourly `fstrim.timer`，並修正 vendor unit 原本把 WSL 當 container 而
跳過的條件；只對 `wsl` virtualization 生效，服務僅 TRIM `/`。首次執行 exit 0，
回報 8,073,351,168 bytes trimmed；此數字是 discard 通知，不是 Windows 已回收量。
Windows `StockAgent WSL Offline Compaction` 以原生 hash-pinned worker／S4U Highest
每小時檢查；實機驗收 `LastTaskResult=0`、`state=skipped-running`，未中斷服務。
停機擴容交給一次性、無自動重跑 trigger 的 Windows 任務
`StockAgent WSL Storage Expansion 20261015`；未看到原生 `expanded-and-boot-verified`
及回讀前，不宣稱已擴容或全服務就緒。

### 晚間主要分布（21:18–21:34 已完成的局部量測）

以下是各 root 單獨量測的實體配置量，不是全部可回收；不同樹間若有 hardlink，
不可直接相加推算全樹總量或可回收量。`data_tw_public` 是指向 `/srv` 的 symlink，
`du` 不跟隨時顯示 0 不代表沒有資料。各源於掃描期間仍可能增長。

| 路徑／類別 | 配置量 | 本次決策 |
| --- | ---: | --- |
| `artifacts/cache` | 216.57 GB | minute parity 181.55 GB、completed inference 27.70 GB；有當前服務引用，不以短暫沒 FD 當閒置 |
| `artifacts/benchmarks` | 186.10 GB | `running_services_20261013` 約 174.78 GB；多項今天仍在執行，不按目錄名刪除 |
| `/srv/stockagent-live` | 155.44 GB | 正式來源介面與服務資料，保留 |
| `data_finmind` | 136.89 GB | complement 約 123.35 GB；權威來源與版本／收據，保留 |
| `data_binance_archive` | 98.19 GB | 來源 partitions，不能當衍生訓練 cache 丟棄 |
| `/srv/stockagent-artifacts-hot` | 92.48 GB | 舊傳輸區仍混有唯一內容及 hardlink；不是可整根刪除的副本 |
| `artifacts/live` | 75.43 GB | 服務、帳本與 replay 邊界，保留 |
| `data_finlab` | 75.08 GB | datasets／raw／intraday 混合來源，保留原值與 provenance |
| `data_tw_index_derivatives_ticks` | 73.94 GB | 自有行情捕獲來源，保留 |
| `artifacts/operations` | 52.91 GB | 混有部署、訓練與驗收證據；保留 |
| `artifacts/markets` | 52.25 GB | 需逐項來源、服務引用與冷恢復證明；未批次刪除 |
| `/srv/stockagent-packed-materialized` | 48.04 GB | managed GC 無合格到期項；保留 pins／leases，不強制 evict |
| `/root/miniforge3` | 43.24 GB | 角色環境與 runtime，未卸載環境 |
| `/root/.shioaji` | 41.16 GB | SDK kbars／ticks／contract 資料，不能當一般下載快取 |
| `data_okx` | 37.77 GB | 其中 panel cache 約 6.68 GB；尚無精確重建與無引用證明，不刪 |
| `artifacts/maintenance` | 24.79 GB | 混有歷史原件與修復資料；未證明可恢復前保留 |
| 舊分鐘資料 quarantine 加 encoding stage | 約 63 GB | 原交易續跑完整 D 恢復驗證中；未完成前不計入回收 |

本日 15:19 已結束的完整 project 掃描另有 `data_openBB` 約 145.55 GB、
`data_binance` 約 143.24 GB；晚間尚在重掃，這兩個數字是較早基準，不冒充目前值。
最新完整清冊與原 run 狀態：

- `filesystem-allocated-inventory-20261015T131935-a554c84b`
- `project-top-level-inventory-20261015T131936-e57d26ea`

其 `run.log` 完成後包含所有目錄配置量；需先確認 run exit 0。

自動 managed GC 本次 dry run 沒有到期可刪項目；已 pin 的公共資料與服務模型
保持不變。強制 `evict --dry-run` 出現候選也不是完整內容恢復證明，本次未據此
刪除。編譯快取 dry run 的合格回收量為 0，不刪活躍編譯快取。

### minute physical 快取的去重稽核

`parity-cache-duplicate-audit-20261015T065152-e48d4ae6` 已完成唯讀稽核：18,970 個
inode 做完整雜湊、檢查期間變更 0；找到 6,438 組位元組重複、12,532 個重複 inode，
候選實體配置量 92,157,575,168 bytes。這不是已回收量。

目前 canonical consumer gate 明確指出
`tw_day_trade_v8_annual_log_cash.yaml` 引用這個 cache root；雖然觀察當下沒有開啟
的 FD，也不能把短暫沒有 FD 當作沒在用。本次不對服務 cache 執行 generic hardlink
apply、不放寬 inactive-panel allowlist，也不刪 logical paths。未來若做此類去重，
需另驗證服務快取的 writer lock、不可變內容、manifest／READY 與讀取者 generation
相容性；不能直接拿唯讀報表作刪除許可。

完整報表：
`artifacts/operations/storage-cleanup-20261015/parity-duplicate-audit/artifact-dedup-tw_minute_paper_parity_09bedd96a2f68c39-20261015T065829.909122Z.json`。

## 程式驗證

- 相關 cold/legacy/materialization 回歸：206 passed（1,042.89 秒）。
- 本次 pack 壓力退路與 private-validation 篩選測試：24 passed，52 deselected。
- 晚間 shared consumer scope／legacy 正常及中斷回收回歸：134 passed（246.15 秒）。
- `git diff --check` 通過；未 stage 或 commit，其他進行中的修改保持不變。

## 進行中的舊分鐘資料恢復交易

沿用原 frozen inventory、owner 與隔離交易，不以新目錄繞過舊交易：

- 原清單：`artifacts/operations/wsl-storage-cleanup-20261006/retired-minute-view-eligible-inventory.json`
- 交易收據：`artifacts/operations/wsl-storage-cleanup-20261006/retired-minute-view-retirement/`
- 最新 run：`resume-retired-minute-views-20261015T134701-68c3abd5`

兩個舊 research minute view 必須先完成 D 固定 release 的所有物件雜湊、ZIP CRC、
獨立重建、解碼原始 SHA 與目前程序／pin／服務引用檢查，才可刪除 quarantine
及已證明重複的 encoding stage。低空間時 pack 讀取可改為完整原生 D SHA 驗證後
直接 seek，且前後檢查不可變 generation；沒有省略全量驗證、降低 reserve，或
跳過實際 workspace 容量准入。若容量不足或任何證明失敗，保留資料。

不要另開同一交易的第二個 apply，不要手動刪 quarantine。查看目前狀態：

16:44 為配合新授權的 WSL 維護，在
`independent-D-original-recovery-for-quarantine` 唯讀階段以 SIGINT 停止本次 worker。
原 run exit 130、phase 留有 `KeyboardInterrupt` 收據；固定 D 身分與原 quarantine
仍在，回收量仍為 0。擴容完成後沿原 frozen inventory 及同 task／run 名稱重新恢復，
先確認原 runner／子程序／unit 已結束。

21:27 接手發現初始計畫使用 `data_tw_minute` consumer domain，但共用最終 unlink
gate 又擴成整個 repo，誤將只命名 workspace 的 agent parent 當作資料使用者。
已修正共用 gate 的內部 scope 傳遞，由 reviewed legacy spec 衍生，套用於正常
rename 及中斷 resume 的全部最終檢查；一般 artifact 預設仍掃整個 artifacts domain。
沒有新增 CLI 略過選項；真實 data ancestor、選定 view、FD／mmap／cwd、服務、
configuration、pin、fresh transport、完整 D 解碼與隔離後的變更檢查均保留。
新測試涵蓋真實 subprocess 的 workspace／data domain、隔離中的 FD／mmap／cwd
與非法 scope；134 項回歸已通過，21:33 已在同 task／原 inventory 重新啟動。
目前是獨立 D 原值還原階段，不計入已回收量；無須開著 Codex 對話，systemd
run 會依原判準繼續。若任一 gate 失敗，保留副本並在原收據報錯，不自動放寬限制。

21:38–21:47 再次診斷：第一批耗在完整冷 blob 雜湊，不是 owner lock；
`py-spy` 指向 `hash_many`，325 秒 wall 有 125 秒 CPU，反覆產生 mount checker。
原生 adapter 雖已一次處理至多 16 個檔案，組態階段仍對每個檔案重跑完整 shell／
Python mount guard。現在改為每個有界 batch 前後各一次，接受 proof 前再次確認 D；
逐檔 path／role、完整 SHA、bytes 及 inode／metadata signature 不變，無 TTL cache。
這只是檢查啟動次數的改善，不宣稱整段流程有八倍加速。

在唯讀 recovery 階段安全 SIGINT 原 worker，核對 exit 130、unit inactive/dead
後才於 21:47 以同清單重啟，未手動刪除 quarantine 或失敗 scratch。新增原子
`recovery-progress.json` 及 stdout 進度，分別記錄 cold hash、materialization、
原值 decode 與 independent reconstruction 的當階段檔數／bytes。中斷與失敗
會明確保存狀態；`acceptance_proof: false`，不把進度當作可刪除證明。

驗證：`native-batch-recovery-regression-20261015T134244-7c7757ab`
186 passed／215.02 秒；額外
`recovery-progress-regression-20261015T134603-14b9c162`
7 passed／43.75 秒，含 cold object corruption、三階段完整還原、mount 前後失敗、
private 無 READY 與中斷不發 acceptance。此時兩批仍未計入已回收量。

21:49:35 的實際觀測：第一批 `research_dataset_developing_v5` 已全 SHA
640／1,537 個 blob、7,225,113,940／17,379,545,060 bytes，elapsed 145.80 秒。
這只代表 `fetch-encoded-archive/hash-cold-blobs` 當階段約 41.6%，不是整體
63 GB 回收完成率；後面仍有 pack CRC、private materialization、decode originals、
independent reconstruction、fresh quarantine audit 與 stage prune。第二批尚待前批完成。
此時兩批 `reclaimed_allocated_bytes=0`，所有未通過的來源與 cold authority 保留。

```bash
source scripts/runtime_env.sh
stockagent-agent task show penguin-safe-storage-cleanup-20261015
stockagent-agent run-status resume-retired-minute-views-20261015T134701-68c3abd5
cat artifacts/operations/wsl-storage-cleanup-20261006/retired-minute-view-retirement/progress.json
jq '{dataset,state,recovery_step,stage,completed_items,total_items,completed_bytes,total_bytes,checked_at_epoch,acceptance_proof}' artifacts/operations/wsl-storage-cleanup-20261006/retired-minute-view-retirement/legacy-wsl-minute-derived-*/recovery-progress.json
df -h / /mnt/d
```

只有最終 acceptance／prune journal 與實際路徑核對通過後，才更新本文件的已完成
回收量。服務程序存活僅是 liveness，不另宣稱所有服務業務驗收已完成。
