# penguin 冷庫 D 槽備份（已退役的歷史操作）

> 2026-09-25 起 penguin 已改為 D 槽單份主冷庫：
> `D:\stockagent-cold-primary\packed` 由 `/srv/stockagent-packed` 受保護掛載使用。
> 下文的 C→D 備份指令只描述舊架構，不可重新啟用、初始化或把舊
> `stockagent-backup` 路徑當成第二份實體備份。請改看
> [D 槽遷移與驗收](d_cold_store_migration_2026-09-25.md)。

最近一次實際清理與未解問題見 [2026-09-17 儲存清理稽核](storage_cleanup_2026-09-17.md)。

## 責任與資料流

penguin 是目前資料權威。收到的 immutable release 保留原 publisher 身分；本工具
只從 penguin 接收完成的檔案複製，不改 fleet 發布權限、不重新發布、不改 Syncthing。

```text
經過 audit 的發布／Syncthing 接收
    ↓ 原子 rename，半成品不進備份
C: WSL /srv/stockagent-packed          權威冷庫
    ↓ inotify + 閒置 300 秒補查；SHA-256 copy + D 回讀
D: /stockagent-backup/packed           獨立、可用既有工具還原的冷副本
    └─ objects → inventories/manifests → 完整驗證後的 heads
```

不備份 mutable downloader workspace、Git、熱快取、未發布 artifacts、憑證或
`.local-state`。這不是整個 C 槽備份。2026-09-21 起，正式設定的
`backup_scope=current_heads` 只維護來源目前各 head 直接指向的 manifest 及其
inventory／pack／blob；每個當前 head 都必須完整，不能退回舊版冒充成功。
D 上既有舊版 manifest、物件及 head-history 不刪除、不覆寫；但舊版及目前未引用物件
不再列入日常備份完成條件，也不保證新發現的舊版會被補入 D。歷史還原能力須另外
執行指定版本驗證，不能從最新版本的綠燈推論。

## 安全與一致性

- D 不加入 Syncthing；源端刪除不傳播，歷史 release 不自動 GC。
- 每個新物件邊複製邊核對檔名 SHA-256，fsync 後回讀 D 再驗 SHA-256；成功才原子改名。
  source 在過程中被修改會拒絕提交。已有 D 物件不符 checksum 時保留原狀並報錯。
- 中斷保留 `.partial`；重啟會逐位元組驗證保留前綴後續傳，不盲信長度或 mtime。
- D 的 current head 只有在相依 inventory／pack／blob 全部驗證後才更新。舊 head 存於
  `packed/head-history/heads/<dataset>/<node>/<head-sha256>.json`；舊 manifest／物件不刪。
  正在接收的新 release 不會使前一份已完整的 D head 消失。每輪結束還會重查來源
  head 集合與內容；掃描期間若有發布變動，本輪不能標為已完成。
- 定期比對檔案 signature 只用於重用**已有 checksum 證明**；不是首次去重或刪除證據。
  WSL 重新掛載 D: 可能只改變 Linux `st_dev`；在掛載、volume marker 及磁碟隔離
  驗證通過的前提下，沿用收據時仍須 inode、大小、mtime、ctime 四欄完全相同。
  任何一欄改變、校驗資料庫遺失、或 checksum 證明逾 30 日都重讀驗證；
  不能因這項規則推論先前未通過完整雜湊的 release 已可還原。
- 大量已驗證物件的 D 槽身分檢查以本輪 `O_NOFOLLOW` 目錄描述符重用，逐檔仍以
  `follow_symlinks=False` 查 regular-file 身分，結尾核對已固定的每層目錄未換位；
  新物件複製及 checksum 回讀仍使用原本的安全路徑檢查。這只減少 DrvFs 目錄查詢，
  不延長 checksum 證明有效期，也不把未驗證物件算作已完成。
- 未變動的 manifest/head 已逐位元組讀回且相同時，不再逐檔重複核對掛載；每輪開始、
  最終完成之前與任何 metadata 寫入前仍強制核對掛載、磁碟身分與 volume marker。
- 只在 config 指定的 D 掛載點、來源磁碟不同、node ID 正確且 volume marker 符合時工作。
  `init` 另外核對 Windows volume UUID。掛載消失／替換即 fail closed，不能寫進空的
  `/mnt/d` 而把 C 槽塞滿。預留 D 槽 20 GiB；空間不足保留所有既有副本。
- 備份服務以低 CPU／I/O 優先序執行，來源強制唯讀。初次複製期間會在小批次之間處理
  新到事件；單一大物件的複製／回讀仍不可瞬間完成。閒置 300 秒補查是兜底，
  inotify 事件可提早喚醒；都不是完成 SLA。
- `up_to_date` 現在只代表設定範圍內的目前各 head 及其相依物件完成；
  `historical_completeness=not_checked` 是明確的未驗收，不代表舊版完整。
  舊版缺件報告與既有 D 檔繼續保留，不以當前版成功沖銷歷史缺口。

## 安裝與日常操作

從 repository root 執行；Python 由 `scripts/runtime_env.sh` 自動解析。
設定入口：`configs/data_sync/packed_backup.json`，不可把這台的 D UUID 複製到別台盲用。
監看器有 inotify 時，新物件／head 事件仍立即喚醒；無變更時每 300 秒對帳一次。inotify 無法使用時每 30 秒輪詢；有進度的積壓批次會連續處理。新設定不略過當前 head 缺件；舊版缺件另由歷史稽核揭露。

```bash
./scripts/run_packed_backup.sh --help
./scripts/run_packed_backup.sh plan           # 唯讀盤點、容量與路徑檢查
./scripts/run_packed_backup.sh init           # 一次性 D UUID 核對及專屬目錄登錄
./scripts/run_packed_backup.sh install-service # 渲染本機 repo 路徑、驗證 unit、enable 並重啟
./scripts/run_packed_backup.sh status
systemctl status stockagent-packed-backup.service --no-pager
journalctl -u stockagent-packed-backup.service -n 10 --no-pager
```

`install-service` 使用 repository 中的 unit template，不能用來無檢查地覆蓋同名外來服務。
WSL／systemd 啟動後常駐；Windows 關機或 WSL 被終止時無法接收、也無法備份。
服務停止不會刪除資料。若 D 未掛載，狀態為 `blocked`，每 30 秒重試。

```bash
sudo systemctl stop stockagent-packed-backup.service
./scripts/run_packed_backup.sh once            # 補齊當下盤點，結束回傳狀態
./scripts/run_packed_backup.sh once --verify-existing # 強制重讀設定範圍內 C/D 物件 checksum
sudo systemctl start stockagent-packed-backup.service
```

上述兩個手動命令可擇一使用；服務運作時以 flock 拒絕第二個備份程序。
全量複查可能讀取上 TB，請選非繁忙時段。服務對仍在來源的物件，每 30 日按到期重驗。
來源已刪、僅保留於 D 的歷史物件不在上述來源盤點內；目前未引用的物件也不在
`current_heads` 盤點內。要獨立稽核整個 D 庫可執行
下列唯讀全庫檢查（缺失歷史物件會報錯，不會刪除或自動修補）：

```bash
./scripts/run_packed_snapshot.sh full-audit \
  --sync-root /mnt/d/stockagent-backup/packed \
  --receipt-dir /var/lib/stockagent-packed-backup/full-audit
```

狀態位於 C：`/var/lib/stockagent-packed-backup/status.json`；D 上每批次也留
`D:\stockagent-backup\status.json`，目前版本完整通過時另留
`last-current-complete.json`。舊的 `last-complete.json` 若存在，是先前全歷史模式的
歷史收據，不代表目前或最新完成狀態。
C 的 `verified.sqlite3` 是可重建的校驗快取，不是還原資料的必要相依項目。

| 欄位 | 判讀 |
|---|---|
| `state`, `backup_scope` | copying／up_to_date／degraded／blocked；先看驗收範圍，目前正式設定為 current_heads |
| `total_bytes`, `verified_bytes`, `remaining_bytes` | 當次來源物件盤點、已驗證、尚待驗證的 bytes |
| `current_object`, `phase`, `current_object_bytes` | 單一物件 copy／resume_prefix／readback 的進度 |
| `pending_objects`, `pending_heads`, `pending_releases` | 設定範圍完成時必須全為 0 |
| `error_count`, `errors` | 必須為 0／空；不可只看 systemd active |
| `current_heads_complete` | 現行各 head 的相依物件是否已驗證；不能替代歷史完整性 |
| `historical_completeness` | current_heads 模式為 not_checked；不是 verified |
| `present_objects_complete` | 這次 C 槽實存物件是否全部完成；不涵蓋來源本來就遺失的物件 |
| `integrity_state`, `previous_pass_errors` | 當前盤點中的檢查狀態與上一輪錯誤；copying 不等於完整性正常 |
| `last_current_complete_at`, `updated_at` | 上次目前版本完成與當前狀態更新時間；要同時看 |
| `stage_timings_ms` | 每輪來源清單、D 槽既有物件信任檢查、實際物件處理、metadata、head 重查等毫秒耗時；`pre_final_status_total` 不含最後狀態／D 收據寫入，不能當作完整端到端備份時間 |
| `phase`, `destination_trust_scanned` | 每輪開始即改為 `checking`，慢速 D 槽逐物件驗證期間約每五秒回報掃描筆數；先前 `last_current_complete_at` 只是前次通過時間，不能當作這輪完成 |

`total_bytes` 是 cold object 邏輯大小，不含少量 manifests／head history／receipt；
不等於 NTFS 實際配置量。最新資料到達可能讓剩餘 bytes 增加，這不是退步或刪檔。

## 還原（只在明確需要時）

D 保留標準 packed 格式，不需要原 C 的 SQLite、node ID 或 Syncthing 資料庫。
先核對 D 的 receipt，再用既有工具檢查指定 dataset 的完整版本。將 `DATASET` 與
`SNAPSHOT_ID` 換成真實值；不要把範例當成現行 release。

```bash
./scripts/run_packed_snapshot.sh status DATASET \
  --sync-root /mnt/d/stockagent-backup/packed
./scripts/run_packed_snapshot.sh verify DATASET \
  --snapshot-id SNAPSHOT_ID --sync-root /mnt/d/stockagent-backup/packed

# 人工還原至獨立目錄，絕不直接覆蓋執行中資料
./scripts/run_packed_snapshot.sh fetch DATASET \
  --snapshot-id SNAPSHOT_ID --sync-root /mnt/d/stockagent-backup/packed \
  --materialized-root /srv/stockagent-restore
```

先核對還原內容與 READY，才另行計畫切換執行路徑。本備份工具不會自動切換。
C 完全故障時應先保存 D，於重建環境中用上述唯讀驗證／人工還原，不要 `init` D 為
另一個 Syncthing publisher，也不要用 `rsync --delete` 重建或清理備份。

### 與 C 槽 rolling-current 的關係

D 是 additive historical archive；C 是 Syncthing 快速 current replica。penguin 的
`stockagent-packed-retention` 只有在 D 已有候選物件的有效 SHA-256 readback receipt、
所有 current heads 完整、所有 fleet peers 收斂且本機沒有使用中引用時，才會移除 C 上
已超過安全窗的歷史 manifest 與無引用物件。它永遠不對 D 執行刪除，也不把 D 加入
Syncthing。

```bash
./scripts/run_packed_retention.sh plan    # 唯讀候選與安全門檻
./scripts/run_packed_retention.sh status  # 最近 plan/apply receipt
```

所以 `run_packed_backup.sh once` 的 C 來源總 bytes 之後可能下降；這不是 D 遺失資料。
D 的舊 release 仍可依上面的 `verify`／`fetch` 流程還原。若 retention 報
`D archive proof incomplete`，先讓 backup 補齊或執行 checksum 複查，不可繞過門檻。

## 驗收與限制

測試：`source scripts/runtime_env.sh && run_fintech_python -m pytest -q test/test_packed_backup.py test/test_packed_snapshots.py test/test_packed_retention.py`。
包含標準還原、只增不刪、head 等候物件、歷史缺物件、校驗失敗、續傳、掛載／容量
防護與原子接收事件。實機另應確認進度持續增加，並抽一個已完成的 release 做獨立
`verify`；首次全量完成前只報告實際進度，不宣稱 C/D 已有兩份完整資料。

兩顆磁碟仍在同一台主機且持續可寫，不能抵禦整機災害、失竊或勒索軟體；這一份是
本機磁碟故障／來源誤刪復原層，不替代離線或異地備份。保留舊資料使 D 用量預期大於
C。D 的刪除仍未獲授權；rolling-current 的使用者核准只涵蓋已由本工具證明可從 D 恢復
的 C 槽候選，禁止把同一政策擴張成 D 鏡像刪除。

### 2026-09-10 初次部署觀察（非永久數字）

初始盤點為 10,840 個現存冷物件、522,342,138,525 bytes；C 在 NVMe 的 WSL VHDX，
D 為另一顆 SATA 磁碟，初始可用約 3.35 TB。首次完整複製已啟動，不代表完成。
部署時在 D 驗證 `cftc-legacy-pre2000` 的 8 個 payload 物件與 inventory，約 63 MB，
原 source 與 D 的 manifest SHA-256 一致；沒有 materialize。證明存於
`D:\stockagent-backup\restore-readiness-smoke.json`。

另發現來源原有歷史缺口：`tw-public` 的 24 個歷史 release 合計引用 113 個遺失物件
（約 1.05 GB），當下各 current head 沒有缺物件。完整清單存於
`/var/lib/stockagent-packed-backup/source-missing-objects.json`。
這些舊資料不能靠備份憑空補回；服務繼續保護現存資料，但歷史完整性保持 degraded，
直到原物件被另外救回。沒有刪除舊 manifest、改寫 hash 或以新內容冒充缺失版本。

### 2026-09-20 閒置掃描成本複測

目前來源有 11,724 個現存物件；只盤點 C: 來源需約 1.9 秒，完整一輪還須逐一核對
D: 的驗證憑證、歷史 manifest 與 head。重新載入服務後首輪於 23:45:33–23:54:24
完成，消耗約 61.4 CPU 秒；後續 45 秒安靜期 CPU 計數不再增加，程序在 inotify 等待。
此前每輪完成後只等 30 秒即再掃；現在無事件時等 300 秒，新發布仍由 inotify
喚醒。這證明安靜期沒有忙輪詢，不是完整長期吞吐測試，也沒有消除上述 113 個
歷史缺件或宣稱 D 歷史已完整。

同日第二輪找出同一個 11,724 物件的 D: 驗證憑證，原先在物件清點和 manifest
核對各讀一次。現在同一輪共用已驗證結果，但 manifest 仍複查 C: 來源身分；新或
改變的 head 在提交 D: 前另做一次完整目標驗證。重載後實機首輪於
00:02:51–00:07:07 完成，約 4 分 16 秒、30.2 CPU 秒，對比上輪的 8 分 51 秒、
61.4 CPU 秒。這是同一主機的兩次完整冷備份對帳，不是所有工作負載的平均加速；
結果仍是 `degraded`，current heads 完整、歷史缺件仍可見。

### 2026-09-21 最新版本維護範圍

依使用者要求，正式設定改為 `backup_scope=current_heads`。01:25:04 重啟備份服務後，
01:29:18 的第一輪 C/D 收據均顯示 `up_to_date`、119 個 head、119 個當前 release、
11,606 個相依物件、459,087,389,625 bytes 已驗證，`pending_*` 與 `error_count` 都是 0。
D 上 `cftc-legacy-pre2000` 的當前 release 另以獨立 verifier 驗證 8 個物件、
63,149,628 bytes。這是依有效校驗收據與檔案身分核對的當前範圍完成；
不等於當次重新雜湊 459 GB，也不等於全歷史完整。先前 113 個歷史缺件報告仍保留，
`historical_completeness=not_checked`；D 上既有舊資料未刪除。
