# 四節點檔案架構、硬體實測與持續回收

使用者指定 penguin 為權威資料／網站／下單機，D HDD 保存壓縮增量；lab203
只中繼至 NAS；Vast 保存當期程式、精確訓練來源、本機生成的 views/cache 和
尚未確認回傳的產物。本輪已實作角色辨識、還原 I/O、共用鎖分離與有界重試。
歷史批次仍在正式 owner 下處理；本文件不宣稱全歷史已備份或遠端已清空。

## 第一性原理與檔案責任

保存的是無法重新取得的觀察和必要結果；panel、tensor、split 與 compiler cache
的價值在於節省當期計算。把兩者混進永久資料同步會使租用主機與 SSD 累积多份
可重建資料。另一方面，檔名叫 cache 不代表它只有衍生值，必須先核對內容與來源。

需要的基本能力是單一寫入權威、精確版本身分、原子發布、恢復證明和工作擁有者。
四台異質設備不需要同一個 filesystem 或共享可變工作目錄。

| 位置 | 保存內容與介面 | 生命週期 |
| --- | --- | --- |
| penguin WSL ext4 的專案與來源工作目錄 | Git／Miniforge、可變來源、provider 狀態、PostgreSQL、正式服務與下單 ledger | 保留來源、版本／時鐘／單位／修訂證據；服務所需 projection 受 consumer 保護 |
| penguin D `stockagent-cold-primary/packed`，Linux `/srv/stockagent-packed` | SHA CAS blob／固定 bucket pack、manifest／head、原始來源與已封存產物 | 不可變增量；只有改變的物件進入傳輸，完整恢復與冷庫 GC 是不同交易 |
| penguin D `remote-artifact-incoming` | 舊歷史一次性 tar/zstd 封閉批次 | 單一搬遷例外；不替代正常增量來源發布 |
| penguin D 備份傳輸與 lab203 `/srv/lab203-backup/ingress` | READY、固定 envelope、封閉 objects／metadata | Syncthing 投遞、lab203 驗證、NAS fixed snapshot 還原 ACK；只回收已機器驗收且可重新生成的傳輸副本 |
| lab203 state／receipts／restore scratch | 單一 owner、失敗重试與獨立驗收暫存 | 故障保留；成功後依已驗證的邊界回收。沒有 SSH、沒有訓練／網站角色 |
| NAS 既有 Restic repository | 加密、去重 snapshots | 沿用既有 NAS／SMB；prune 另有保留政策，這次未啟用 |
| Vast `/srv/stockagent-packed` | 冷庫 index／exact source manifest；有界當期 payload | index-only；原值按需取用，拒絕把租用 overlay 當持久冷庫 |
| Vast materialized／訓練工作目錄 | 固定 source release、Git/config/ABI/env、本機生成的 panel／tensor／cache、当前产物 | 可用仍須 READY；job 使用中保護。完整產物返回 penguin、D 全檔原值還原、新鮮 ACK 後回收其精確來源 |

繼續使用原生 ext4 熱路徑、現有 D NTFS HDD、現有 NAS Restic。Parquet／Arrow
保存有型別資料，Polars／DuckDB 做轉換與查詢；Syncthing 傳送已發布內容，SSH／rsync
收取一個固定完成 run，systemd／Vast Supervisor／cron 管理各自 owner。
NAS pool 與 Vast 宿主磁碟不在本機格式化權限內。未部署新的付費儲存或 Ceph。

正常增量來源使用固定 bucket/blob，避免整份巨型壓縮檔每次重新同步。舊資料一次性
回傳可用 tar/zstd；正常訓練直接在 Vast 依 exact source release 重建。
角色定義位於 [`node_storage_roles.json`](../configs/data_sync/node_storage_roles.json)，
實際程序與資料內容仍是保留／回收判準，角色 JSON 自身不授權刪除。

## 現場硬體與完整流程實測

penguin 為 5800X3D，WSL 可見 16 執行緒、單 NUMA；C NVMe／WSL ext4 熱區與
D SATA HDD／NTFS 冷區沿用既有硬體收據。Vast 為雙 socket EPYC 7663、8 NUMA，
雖可見 224 host threads，實際 cgroup CPU 配額只有 53.75999，worker budget 53。
RAM 必須取 cgroup max-minus-current 與 MemAvailable 的保守值，本輪初始可用約
36.4 GB，不能把 host 540.8 GB RAM 當租用實例可用量。兩張 RTX 5090 的實際
訓練持续運作，這次沒有啟動新 GPU 工作或改寫其 frozen source。

同一真实來源交錯測試三次；未清除 OS cache，不宣稱統計顯著或下台租用機同樣快。

| 比較 | 量測內容 | 結果與採用 |
| --- | --- | --- |
| 舊 gzip CSV 規則、adaptive gzip、Zstd 1／3 | 真實 28,731,515 bytes／2 files：編碼、D 發布、完整原值 decode／restore／SHA／mode／mtime | 中位數 9.576／5.607／6.736／5.988 秒；保留既有 adaptive gzip。新增 Zstd 可測能力，沒有把所有正式封存改成 Zstd |
| Vast zstd 4／8／16 threads | 同一真實 880,583,520 bytes／3 NPY：遠端壓縮、SSH、原生 D fsync、完整解碼與原始 SHA、清除己方暫存 | 三次平均 20.431／20.918／21.675 秒；皆 186,405,063 compressed bytes，選 4 threads。4 threads 比較快的差距小，沒有推廣為所有建表／訓練設定 |
| DrvFs 直接 ZIP 讀取、原生 D→匿名 SSD pack | 同一完整 383 檔 pack，57,305,360 encoded bytes／121,696,513 original bytes；object SHA、ZIP CRC、逐檔 fsync／encoded SHA、原值 decode／SHA、清暫存 | 正式實體容量 guard 版本中位數 17.182／5.631 秒，約 3.05 倍；只代表這個 pack 的完整流程 |
| 原生 D blob 讀取 | 3 個真實 objects，共約 46 MB；順序 copy／fsync／雙邊 SHA | 中位數 2.820／2.584 秒，約 1.09 倍；不把小差距當整庫吞吐保證 |

原始收據：`artifacts/operations/four-node-storage-20261005/` 的
`cold-codec-workflow.json`、`remote-compression-workflow.json`、
`native-d-pack-recovery-with-capacity.json`、`native-d-blob-recovery.json`。
完整流程包括驗證，不能僅用 compressor CPU 秒數或裸 disk write 排名。
雙 socket 均在可用 affinity 中，但没有比較 NUMA binding；這條傳輸的 16 threads
没有比 4 快，因此没有為它強制雙 socket 綁定。建表／DDP 調校另測其完整工作流。

## 已實作的根因修正

1. **區分真正的訓練機與複製的服務設定。** Vast 本機 root:600 的
   `/etc/stockagent/node-storage-role.json` 配合節點身分、index-only、Supervisor
   與獨立程序觀察，才可排除只屬 penguin 的舊網站模板。實際 training/config、
   FD/mmap、pin、完整恢復與同步 gate 全部保留。先前兩個網頁 cache 約 73 GB
   因模板被錯誤保護；約 72.59 GB 的那個仍因近期改變而等待十二小時穩定。
2. **缩短跨批次共用鎖。** legacy 私有傳輸／編碼在全域 owner 外進行。發布
   和原值回收才使用原共用 owner；完整 D 還原和同步等待移到 cohort owner 下。
   bulk 也分成 unverified publication 和 verified recovery，完整解碼不阻擋其他
   批次發布。舊主 owner 還在交易內，保留它；安全交接器只在 childless、未持鎖
   的等待點升級。partition 已交接至正式 systemd 新版，main 的交接仍受監督等待。
3. **不可變 pack 顺序读入 SSD。** 沿用實際 D marker 與 8 KiB 正式掛載，避免
   IDE／systemd 的 mount namespace 差異。原生 adapter 不改冷庫身分；每 pack
   完整 SHA、ZIP CRC、所有原值 SHA 和 before/after signature 仍必須通過。
   匿名 SSD 暫存檢查 C 真實容量與 32 GiB reserve，不留訓練資料或整庫熱副本。
4. **舊 cache 的有界自動重試。** 一次註冊 30 個既存 inactive namespaces 的
   portable fingerprint、實測機器 SHA、4-thread 壓縮設定与 256 GiB 上限。五分鐘
   coordinator 重新查 current consumers、RAM／CPU、十二小時穩定，只傳尚未完整
   收妥的原始集合。新 namespaces、不同內容和未来 caches 不會自行加入這次保存。
5. **共享 inode 精確名稱回收。** bulk ACK v2 固定 `unlink_preserved_names_only`，
   使用既有 completed-return 的外部 inode FD/mmap 檢查。只刪已完整封存的名稱，
   外部 alias 保留；只有最後一個 name 被 unlink 才計入實際 allocated bytes。
   v1 ACK 不會取得這個新權限，未通過其他 gate 的目錄不會進入刪除。
6. **正式重試服務。** `stockagent-vast-bulk-return.timer` 在 boot 三分鐘後及上次
   完成五分鐘後執行，沿用 common owner 和單一 journal lock；繁忙退讓為 75，
   不是備份成功。失敗根保留、下一根繼續，下一週期重試。原有 source backup、
   completed return、兩個 legacy cohort、scan retry、edge GC 繼續使用既有 owner。

## 驗收、當下進度與尚未完成的範圍

本輪受影響的 packed/native I/O、legacy、bulk transport／retirement、角色／cache
capture、training return、materialized lease 與 automation 共 **330 tests 通過**，
收據 `storage-regression.xml`；Bash syntax、Python compile、變更 whitespace 核對通過。
外部程序持有共享 FD 和只剩 mmap 的案例均拒絕回收；完整 NAS／training acceptance
不能由這些單元與整合測試代替。

正式 canonical legacy API 另從 D 完整重建一個已回收的 499-byte 原始檔及 manifest，
通過 packed 與原值驗證、真實 C 容量 guard、己方 scratch 清除；
`canonical-native-legacy-recovery.json`，7.752 秒。它不是整批大歷史的完成證明。
真實大 pack 的 383 檔獨立恢復另見上方測量。

2026-10-05 03:48 Taipei 的最新 NAS 狀態：**403,531,516,948／728,498,632,702
bytes** 已有離機檔案收據；3 批待驗收約 21.09 GB，receipt errors 為空。
lab203 v10 worker 與 receiver 持續回報，現場只有 receipt 通道可用，沒有用 SSH
重新登入該機。既有具名 CFTC／TAIFEX packed 和 PostgreSQL logical restore
已獨立通過，USB 金鑰保管已有使用者確認；這些不等於每個資料集、目前 DB 狀態或
全歷史都已獨立還原。

penguin 清冊 133 roots，Vast 215 roots。penguin OpenBB 單根 5,000,000 files 上限
已到，該根大小是下界，不能宣稱全機清冊完整。Vast 清冊的 cache allocated bytes
跨根可能共享，不相加當作可回收量。未分類資料／model/service ledger 不會直接
刪除；企圖按目錄名稱全部 `rm` 會同時刪除本期訓練和不可重抓原值。

目前大型 legacy main 還在原版完整 D 恢復內，bulk 的已收妥批次仍等待它釋放
owner；不能把先前累計回收約 10.68 GB、cache hardlink 去重或本輪 186 MB 新收妥
資料當成本輪新增遠端清理量。約 72.59 GB 舊 cache 還有近期變更；一般 managed
materialization 的 dry run 仍受 pin／七日 lease／實際使用保護，沒有清除 pin 或
改成零租期。這些剩餘批次由已啟用的正式 timer 持續處理。

這次改的是可驗證的資料布局、搬遷與工作邊界。為了保留現有服務和正在執行的
訓練，沒有裸搬全部 `data_*`／`artifacts` 或重格式化磁碟。所有尚未有 exact recovery
或有實際引用的歷史項目仍在遷移清單；本輪沒有宣稱完成全部檔案架構遷移。

## 日常觀察與重試

```bash
source scripts/runtime_env.sh
stockagent-data automation-status --human --live
run_fintech_python scripts/status_vast_bulk_return.py
systemctl status stockagent-vast-bulk-return.timer
journalctl -u stockagent-vast-bulk-return.service -n 30 --no-pager

# 若無 active owner，可提前執行同一正式 service；不用啟動第二份 runner。
systemctl start --no-block stockagent-vast-bulk-return.service
```

安装／測量用 `scripts/run_authority_storage_operation.sh`，進入 PID 1 的正式 mount
namespace 並載入既有 private owner environment。正式 timer 已 enabled／active；
本輪 boot audit 的 infrastructure readiness 通過，但沒有為這次 I/O 優化重開
正在工作的主機或把當下 active 宣稱成新一次 cold reboot 驗收。

## 工具來源與選擇界線

Zstd 的格式與 thread 能力參考 [官方專案](https://github.com/facebook/zstd/blob/dev/README.md)，
這台機器的最佳候選由上方完整工作流決定。lab203 Receive Only 行為與投遞驗證
分工見 [Syncthing folder types](https://docs.syncthing.net/users/foldertypes.html)。
NAS repository 的驗證與 fixed snapshot restore 沿用
[Restic 官方手冊](https://restic.readthedocs.io/en/stable/manual_rest.html)。
產品能力不替代本專案的 SHA、恢復與 current-consumer 證據。
