# Penguin 冷庫移往 D 槽：遷移稽核與驗收

> 本文前半保留遷移前的讀取證據，不能當成目前架構。2026-09-25
> 17:00 起，`/srv/stockagent-packed` 已切為
> `D:\stockagent-cold-primary\packed` 的 guarded bind mount。C 舊冷庫
> 已在逐物件稽核、現行 release 驗證及 peer 收斂後移除；未完成的 D ext4
> 暫存映像也已卸載並移除。
> 實際狀態以文末驗收與即時工具輸出為準。

以下「實際容量與內容」至「切換及清理的必要順序」記錄 2026-09-25
台北時間約 13:00 的**遷移前**狀態；當時的 staging 指令不再適用。
使用者已選擇 D 槽可作唯一實體冷資料所在磁碟，接受同機失去 C/D 獨立磁碟副本的風險。
在完整驗證及服務切換前，`/srv/stockagent-packed` 仍是 C 槽上的權威冷庫。

## 實際容量與內容

| 位置 | 狀態／用途 | 實際量測 |
|---|---|---:|
| C `/srv/stockagent-packed` | Syncthing 現行冷庫，ext4 | 約 419.76 GB allocated |
| D `/mnt/d/stockagent-backup/packed` | additive 備份，Windows DrvFs/9p | 約 615.26 GB allocated |
| D 整個磁碟剩餘 | 含其它使用者資料，不全屬本專案 | 約 2.54 TB |

若日後完整切換並安全退役 C 舊冷庫，理論上可從 C 回收約 419.76 GB
allocated；這是**條件式上限估算**，不是本輪已回收量。D raw 備份會先保留
作回復來源，其 615 GB 是否可移除須另做精確 byte/hash 與服務切換驗收。

D 現有 125 個 current head、216 個 manifest、15,261 個物件。125 個 current
release 去重後引用 12,019 個物件／469,467,949,753 bytes；備份服務最新
`current_heads` 收據為 `up_to_date`、零 backlog／錯誤，並已驗證這些 current
物件。這**不是**歷史完整度證明。107 個 current head 是 artifact 類，其它包括
13 個台灣資料類及 Bybit、TAIFEX、CFTC、舊模型遷移與 crypto 舊封存。

D 上僅歷史 manifest 引用的現存物件約 134.66 GB；另外有 47 個／約
11.07 GB 物件不被 D 現存任何 manifest 引用。它們是**待查孤兒候選**，不是
可依名稱或年齡直接刪除的 cache；其中可能有中斷發版的唯一位元組。
D 的 114 個歷史引用物件缺失，預期約 1.05 GB；C 也沒有這 114 個物件，
且缺口都屬舊 `tw-public` manifest。現行 head 缺件數為 0。C 自己的 128
個 manifest 也有 18 個舊 `tw-public` 引用缺件，現行 head 仍完整。保留
這些歷史 manifest 與錯誤證據，不能把它們稱作可恢復完整版本。

冷庫命名空間只有 `heads`、`manifests`、`objects`，D 備份另有少量
`head-history`；沒有可整棵刪除的 `cache` 目錄。C 現行資料量與 D 歷史量
不可直接相加當成可節省空間，因 release 共用內容雜湊物件。

D 的**庫外**舊 artifact 壓縮 staging 另約 63.48 GB：US 約 54.68 GB
尚未完成正式冷發版，crypto 約 8.80 GB 仍是七日熱副本退役所需的精確
比對來源。兩者目前都不是可刪 cache。受控 materialized-cache GC
的 13:12 dry-run 有 3 個受保護版本、`would_evict=0`；核准編譯快取
清理器以 1 日門檻檢查 `eligible_files=0`。D 的 47 個孤兒 pack 多數修改於
9/12，但檔齡與「沒有 manifest 引用」仍不足以證明其內容有另一份可恢復。
13:34 再跑現行 C 冷庫的核准 retention `plan`：peer 收斂且無 blocker，
但僅有 1 個物件／1 份 manifest 可清，實際 allocated 約 274,432 bytes。
因此舊版本清理不足以替代遷移 D；也不值得在搬遷中途啟動正式 apply。

以下唯讀工具可重新量測。`--fast` 只查 metadata 與物件路徑存在，
避免在 Windows D 目錄逐物件 `stat`；不加此選項才查每個物件大小。
兩者都**不**重讀物件 SHA-256，也永不授權刪除：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_packed_cold_store.py \
  /mnt/d/stockagent-backup/packed --compare-root /srv/stockagent-packed --fast
```

## 為何不能直接改 Syncthing 路徑

`/mnt/d` 目前是 Windows `D:\` 的 DrvFs/9p 掛載。隔離原子寫入測試可
`fsync`／`rename`，但在 D 目錄上的 Linux inotify 監看 **2 秒沒有收到事件**。
因此直接將 Syncthing folder 指到此目錄，會失去現有的 watcher 即時偵測；
必須改成經測試的顯式掃描或把主冷庫放在 D 槽承載的 ext4 檔案系統。
隔離的 D 槽 sparse 檔案、loop、ext4 掛載測試成功，測試映像與目錄已移除；
這只是功能探測，**不是**開機持久性、斷電一致性或正式切換驗收。

13:10 已另建正式**暫存**映像
`D:\stockagent-cold-primary\packed.ext4.img`，虛擬上限 1 TiB、Windows
NTFS sparse flag，建立時實體配置約 185 MB；ext4 UUID 為
`7371835a-d0d1-442a-a50f-9292f79c3c3c`。目前掛在
`/srv/stockagent-packed-d-stage`，**不是** `/srv/stockagent-packed`。
隔離原子 JSON 寫入在 ext4 staging 目錄 2 秒內收到 inotify 事件，
相同探測在原始 D DrvFs 目錄沒有收到。測試檔已移除。已先複製 D 備份
的 `heads` 與 `manifests` metadata，尚未搬入 payload 物件、未更動
Syncthing 或正式服務。這個 sparse 映像仍須驗證 WSL 重啟掛載及實際
全量內容後，才可視為主冷庫。
乾淨卸載、解除 loop、由映像重新掛載後，125 個 head 與 216 個 manifest
仍可讀；此測試不等同 Windows 斷電或 WSL 冷開機驗收。
重新掛載後 sparse 映像的 D 實際配置約 235 MB；1 TiB 是上限，不是
已占用空間。正式搬入約 615 GB payload 後，D 實際占用會隨資料增加。
`scripts/stage_packed_d_cold.sh --plan` 的唯讀 dry-run 列 15,261
個物件、615,189,999,279 bytes、刪除 0；`--copy` 是可重跑的**初始**
物件搬運，只在 D volume marker、ext4 UUID、loop 背景檔與無 symlink
來源全部通過後執行，不切換服務，也不因此宣稱 hash 驗證完成。
`--copy` 搬完物件後才刷新 manifest、head 和 head-history；若備份在搬運
期間收到新 release，須重跑至 current head 與物件檢查皆通過。

盤後可用以下指令搬運及查核；先前的 rsync `--copy` 只作初期試搬，
不再作為推薦搬運入口。驗證工具會按 inode／size／mtime／ctime
保存 SHA-256 收據，重跑時只重算變動的物件。`--scope current` 只證明
現行 head 的完整物件圖，`--scope all` 會雜湊庫中**現存**的歷史與孤兒物件，
但不能把已缺失的 114 個歷史引用變成完整資料：

```bash
ionice -c3 nice -n19 bash scripts/prepare_packed_d_cold.sh
```

驗證進度記錄在 `/var/lib/stockagent-d-cold-migration/verify-status.json`。
目前任何 `verified_*` 狀態仍只代表 D 暫存庫內容核對，不代表正式掛載、
Syncthing 切換、C 清除或 D raw 備份退役已完成。

本次初始搬運從 13:30 後以低 I/O 優先級交由
`stockagent-d-cold-prepare-stream3.service` 暫存 systemd unit 持續執行。
原 rsync 搬運在約 300 GB 時遇到 WSL/DrvFs 的 mmap 記憶體頁配置錯誤，
已停用；完整大小相同不代表內容正確。後續改用固定 4 MiB 緩衝的
`scripts/copy_packed_d_streaming.py`：新物件檢查來源 digest、fsync 暫存檔、
目的端 SHA-256 讀回、原子 hard link；已有物件逐一雜湊，遇錯不覆寫。
每 16 個物件保存一次進度與驗證收據，無須重複複製通過核對的物件。
15:32 觀測到先前 unit 運作中、400／15,261 個物件及約 28.63 GB 已核對，
D ext4 暫存庫約 305.88 GB；這些數字**不是**整庫完成度。當時
Shioaji／TAIFEX 夜盤擷取仍有 3 個程序執行，因此沒有重啟 WSL 或切換
Syncthing 目錄。
15:42 發現某些先前 rsync 檔案長度正確但 SHA-256 錯誤；C 主庫及 D raw
原件 SHA-256 正確。修復器在確認來源 digest 與穩定簽章後，把壞的暫存
inode hard link 留在 D ext4 的 `.migration-quarantine` 並保存收據，再從來源
以串流、目的端讀回與原子 hard link 重建，從不覆寫未知位元組。此目錄已
加入 staging `.stignore`，不會作為 Syncthing 冷物件發布。
16:26 暫停可續傳複製並卸載暫存 ext4；離線 `e2fsck -f -n` 發現 journal
待回放及 free block／inode 計數差異。D raw 與 C 原件均未動；對**尚未
上線**的 D ext4 映像執行 `e2fsck -f -y` 回放 journal／修正計數，隨後
`e2fsck -f -n` 全部五階段乾淨。重新掛載後改用
`stockagent-d-cold-prepare-stream4.service` 續傳，記憶體高水位 12 GiB；
每個目標物件仍須通過 SHA-256／穩定簽章，不能由 fsck 成功直接宣稱可用。
它依序重跑可續傳的串流複製、驗證 current head、驗證庫內全部現存物件，
**不會自動切換主庫或刪檔**。WSL 若在完成前停止，unit 不會自動重建；
重新掛載確認後以同一腳本重跑即可續傳與沿用未變更物件的驗證收據。
可用以下唯讀指令查看，不要把暫存 ext4 的 `Used` 當成 SHA-256 完成證明：

```bash
systemctl status stockagent-d-cold-prepare-stream4.service --no-pager
journalctl -u stockagent-d-cold-prepare-stream4.service -n 30 --no-pager
df -h /srv/stockagent-packed-d-stage /mnt/d
sed -n '1,100p' /var/lib/stockagent-d-cold-migration/copy-status.json
```

## 切換及清理的必要順序

1. 決定並建立 D 上受持久掛載保護的 Linux 冷庫；確保 D 缺席時
   Syncthing、發布器、退役與 GC 一律 fail closed，不在 C 空目錄重建。
2. 搬入 C current 及 D 獨有歷史 metadata／物件；保留舊 D raw backup 作
   暫時回復來源。逐物件驗證 current head 與需保留的版本，明列不可恢復
   的 114 個舊引用；不要宣稱整體歷史完整。
3. 盤後短暫停止寫入者與 Syncthing，取得全域發版鎖，做最後增量與
   head 比對。將 penguin 原有 `.local-state/node-id`、`.stignore`、
   `.stignore-edge`、`.stfolder` 搬入 D ext4（僅同機遷移，不複製到 peer）；
   將 ext4 自帶的 `lost+found` 排除於 Syncthing。再切換固定
   `/srv/stockagent-packed` 路徑。驗證發布、即時
   偵測、peer 收斂、當沖／網站讀取及 WSL 重啟恢復。
4. 舊 backup／retention 定義若同在 D，不能再假稱是**獨立實體備份**；
   切換前必須停用 C→D 備份、C rolling retention、依賴舊 D 備份證明的
   artifact retirement timer，並讓 Syncthing 及發布器在 D 掛載缺失時
   fail closed。在新政策啟用前須阻止舊程式對新主庫做刪除。只有 D 主庫、現行 head、
   必要 pin／lease／quarantine、peer 與服務皆驗收後，才可精確退役 C
   舊冷庫。D 上仍需另立明確的孤兒與歷史保留政策，不能直接 `rm -rf`。

遷移前沒有刪除任何 C/D 正式冷物件、manifest 或 head。
共用 packed 發布入口已加入 `.stockagent-d-mount-required` fallback sentinel
拒絕規則並有測試；**sentinel 只會在正式切換 C 舊目錄後安裝**。這項
程式保護不能取代 systemd 的 mount 依賴與 Syncthing 啟動檢查。

## 實際實施：D DrvFs 單份主庫（2026-09-25）

上述 ext4 image 方案在 Windows D: 的 9p loop I/O 記憶體壓力下失敗，
觀測到 kernel page allocation/I/O errors，因此沒有拿它當主冷庫。
改以受限 8 KiB 9p request 的 D: DrvFs 專用掛載；在實際資源壓力下，
已完成 540 MB 單物件 SHA-256 讀回與隔離的 packed 發布／驗證。
舊 ext4 image 是**未完成、未掛載的 staging**，不含權威資料；其空間
只有在 D 主庫和 peer 完整驗收後才能回收。
其 loop 裝置曾被三個仍存活的服務 mount namespace 保留，即使主 namespace
已卸載。已確認無 fd/mmap/cwd 或服務程式引用 staging，僅在該三個 namespace
解除 `/srv/stockagent-packed-d-stage` 掛載；服務 PID 未變、沒有重啟，
`losetup -a` 與所有可見 namespace mountinfo 已無舊 loop 引用。
主庫收斂後再次確認沒有 loop、檔案或 mount namespace 引用，才精確移除
`D:\stockagent-cold-primary\packed.ext4.img` 單檔；刪除前實際配置
307,340,312,576 bytes，D 可用空間增加約 307.34 GB。這是未完成的
非權威 staging，沒有刪除 D 主冷庫內任何物件或 metadata；此映像本身不可還原。

原 D additive backup 於同一 D volume 上原子重新命名為
`D:\stockagent-cold-primary\packed`，避免重複搬運 615 GB。保持 penguin
原有 node ID、Syncthing Folder ID `stockagent-packed` 和元資料路徑。
`stockagent-d-cold-mount.service` 先核對 enrolled volume/primary marker，
再將 D 主庫 bind 至 `/srv/stockagent-packed`；Syncthing 依賴此 unit，
C 掛載底部只有 `.stockagent-d-mount-required` fail-closed sentinel。
D 不可用時，不能默默改寫 C。

DrvFs 的 inotify 不可靠，因此此節點 Syncthing watcher 關閉、每 300 秒
週期掃描；原子發布也主動要求 Syncthing 依序掃描 objects、manifest/head。
掃描請求失敗會留下本機重試 receipt，不能把已落盤 release 誤報為已同步。
首次從 C 的 Syncthing 索引切到 D，因兩邊 mtime 不同，需要讀取約
615 GB 冷庫內容重建索引；`FolderScanProgress` 是此時進度來源。
`POST /rest/db/scan` 會等掃描完成才回應，首次全庫掃描期間可能逾時並保留
`scan-pending` receipt；不要為了讓它看起來 idle 而重啟服務或刪索引。
這次 9/24 `tw-public` release 在全庫掃描期間已原子落盤，掃描 API 逾時後，
舊重試邏輯又錯掃整個 `objects` 樹而再次逾時。現已修成從 receipt 重放
精確的 89 個新物件路徑，且下一次發布會合併未完成路徑；只有 receipt
損壞才做全樹掃描。重試成功後，遠端 `tw-public` head SHA-256 與 penguin
相同，manifest 和 inventory SHA-256 也驗證成功；不能用「Syncthing 100%」
取代這個 exact-release 檢查。
舊 C→D backup service 與 C-only retention timer 均已停用；D 單份磁碟
不再被報為兩份獨立備份。artifact retirement 的安全核對改為直接驗證 D 主庫。

切換前最後一次 current-head 備份核對為 125 heads、12,019 current
objects、469,467,949,753 referenced bytes，無 pending/error。D 上全部
15,261 個**現存**物件（615,189,999,279 logical bytes）已由新讀回 receipt
或未變更簽章的 SHA-256 receipt 驗證；這不等於全部歷史 release 完整，
已知 114 個舊 tw-public 引用在 C/D 都缺失，仍須保留缺陷證據。
切換後 `audit_packed_c_retirement.py` 將 C 12,143 個物件、125 heads、
128 manifests 與 D 逐項比對，結果 `verified_c_subset_of_d`；12,143
個 C 物件均有可信的 C/D SHA-256 receipt 與未變更簽章。

日常核對：

```bash
./scripts/mount_packed_d_cold.sh --check
systemctl status stockagent-d-cold-mount.service syncthing@root.service --no-pager
./scripts/run_packed_backup.sh status  # retired_single_d_primary
stockagent-data status --human
```

這是單一 D 實體副本。Syncthing 可傳送 current 冷物件，但不能代替獨立備份；
WSL/Windows 冷開機自動恢復必須另做驗收，不能用熱狀態重新掛載宣稱已通過。

### vastai1T 接收端權限修復

本次重新索引時，vastai1T 顯示 482 個 `chmod ... operation not permitted`
pull errors／1,553,012 need bytes；其 Syncthing 是 supervisor 的 `user`
程序，但既有部分 heads／manifest 由 root 擁有。只將遠端
`stockagent-packed` folder 的 `ignorePerms` 改為 `true`，未改資料內容、
忽略規則、folder ID、裝置身分或重啟服務。修改後遠端自身
`needBytes=0`、`needItems=0`、`errors=0`、`pullErrors=0`，
`restartRequired=false`；penguin 觀測 vastai1T completion 100%。
此設定只是不將 POSIX 模式當同步內容；packed release 仍以 SHA-256
內容與 manifest 驗證，不以檔案權限作資料完整性證明。

### 最終切換與清理驗收

- C 舊冷庫先經 `audit_packed_c_retirement.py` 查得
  `verified_c_subset_of_d`：12,143 個物件／473,581,058,121 logical bytes，
  125 個 head、128 個 manifest 均被 D 的現存內容或 head-history 覆蓋；
  物件依 C/D SHA-256 收據及未變更簽章逐項核對。
  稽核收據 `/var/lib/stockagent-d-cold-migration/c-retirement-audit.json` 的
  SHA-256 為 `06a2317b3a5b03a049425d967854d5042d8c7b01130fc13764b454f01e22b913`。
  C 舊路徑
  `/srv/stockagent-packed-c-pre-d-20260925` 移至精確退休路徑後，只在該
  同檔案系統目錄中刪除。C 根檔案系統可用空間從 103,302,692,864 增至
  523,064,758,272 bytes，增加 419,762,065,408 bytes；兩個舊路徑
  目前均不存在。權威冷庫仍是 D 上的 `/srv/stockagent-packed`。
- D 舊 ext4 staging 映像不是權威庫；精確移除前後，D 可用空間從
  2,234,000,490,496 增至 2,541,340,807,168 bytes。這項回收不能
  與 C 回收相加當成同一磁碟的可用容量。
  舊 `/srv/stockagent-packed-d-stage` 空掛載點也已移除，失敗的暫存
  copy unit 狀態已清除；沒有重啟正式服務。
- `stockagent-d-cold-mount.service`、`syncthing@root.service` 均為
  active，掛載身分 `./scripts/mount_packed_d_cold.sh --check` 通過。
  C→D 舊備份服務與 C-only retention timer 為 disabled；
  `run_packed_backup.sh status` 明確回報 `retired_single_d_primary`。
- 本機 `stockagent-packed` 為 idle，need bytes/items/deletes、folder/system/
  pull/watch errors 均為 0。vastai1T 已連線、completion 100%、
  `remoteState=valid`、need bytes/items/deletes 均為 0，連線觀測為 QUIC。
  vastai1T 是 index-only edge，不保有整份 payload，不能算 D 的備份。
- 本機現行 `tw-public` head 的 SHA-256 為
  `2cd7a694826eccb95c9ea8c7ffb046ff5118b6e4ba0baadf6846096769879f04`，
  指向 `tw-public-20260925T111407409405988Z-l0-penguin-ca6aafdd081d9776`；
  冷資料 freshness 與來源均為 2026-09-24。清理後再次執行 exact-release
  `verify` 成功：2,778 個物件、24,708,740,588 bytes 通過 SHA-256，
  pack 的成員與 CRC 亦通過；manifest SHA-256 為
  `747c8ea62d5bb7fa299e5422d600e13eba5a85e26b86daddbed8ddda7a23475f`。
  `materialized_verified=false` 符合冷庫驗證未展開熱快取的預期。
- 清理後，台股當沖服務的本機 `/healthz`（127.0.0.1:8766）及
  公開 dashboard gateway（127.0.0.1:8770）均回 HTTP 200；沒有重啟
  交易或行情服務。其他不同埠的健康端點不可拿來代表這兩個服務。
- C 的 419.76 GB 是 WSL ext4 內部可用空間增加，不等於 Windows C:
  立即多出同量空間。已執行 `fstrim -v /`，回報 350,997,790,720 bytes
  trimmed，但 Windows C: `Get-Volume` 可用量仍約 633 GB，沒有觀察到
  VHDX 實體縮減。要回收 Windows 主機層空間須另安排可停止 WSL／
  交易與資料服務的維護窗口；本次沒有為了壓縮 VHDX 中斷服務。

仍有兩項明確邊界：D 是單一實體冷副本，磁碟故障可能使資料無法恢復；
舊 `tw-public` 歷史 manifest 有 114 個 C/D 都缺少的引用，不能當成完整
可還原版本。另未在服務運行期間重啟 WSL/Windows 驗證冷開機自動恢復；
現有驗收僅證明熱狀態掛載、當前發布及同步路徑。
