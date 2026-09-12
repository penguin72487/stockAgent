# penguin 冷庫 D 槽備份

## 責任與資料流

penguin 是目前資料權威。收到的 immutable release 保留原 publisher 身分；本工具
只從 penguin 接收完成的檔案複製，不改 fleet 發布權限、不重新發布、不改 Syncthing。

```text
經過 audit 的發布／Syncthing 接收
    ↓ 原子 rename，半成品不進備份
C: WSL /srv/stockagent-packed          權威冷庫
    ↓ inotify + 30 秒補查；SHA-256 copy + D 回讀
D: /stockagent-backup/packed           獨立、可用既有工具還原的冷副本
    └─ objects → inventories/manifests → 完整驗證後的 heads
```

不備份 mutable downloader workspace、Git、熱快取、未發布 artifacts、憑證或
`.local-state`。這不是整個 C 槽備份。所有 canonical `objects/blobs`、`objects/packs`、
`objects/inventories` 都納入，包含歷史物件與目前未引用的物件；不只 latest release。
manifest 與 head 保留原格式，因此不再封成一份巨型壓縮檔，內容相同的物件只存一份。

## 安全與一致性

- D 不加入 Syncthing；源端刪除不傳播，歷史 release 不自動 GC。
- 每個新物件邊複製邊核對檔名 SHA-256，fsync 後回讀 D 再驗 SHA-256；成功才原子改名。
  source 在過程中被修改會拒絕提交。已有 D 物件不符 checksum 時保留原狀並報錯。
- 中斷保留 `.partial`；重啟會逐位元組驗證保留前綴後續傳，不盲信長度或 mtime。
- D 的 current head 只有在相依 inventory／pack／blob 全部驗證後才更新。舊 head 存於
  `packed/head-history/heads/<dataset>/<node>/<head-sha256>.json`；舊 manifest／物件不刪。
  正在接收的新 release 不會使前一份已完整的 D head 消失。
- 定期比對檔案 signature 只用於重用**已有 checksum 證明**；不是首次去重或刪除證據。
  signature 改變、校驗資料庫遺失、或 checksum 證明逾 30 日都重讀驗證。
- 只在 config 指定的 D 掛載點、來源磁碟不同、node ID 正確且 volume marker 符合時工作。
  `init` 另外核對 Windows volume UUID。掛載消失／替換即 fail closed，不能寫進空的
  `/mnt/d` 而把 C 槽塞滿。預留 D 槽 20 GiB；空間不足保留所有既有副本。
- 備份服務以低 CPU／I/O 優先序執行，來源強制唯讀。初次複製期間會在小批次之間處理
  新到事件；單一大物件的複製／回讀仍不可瞬間完成。30 秒是補查週期，不是完成 SLA。
- 所有歷史 manifest 的相依物件都要可驗證，才能標記整庫 `up_to_date`。完整性問題
  不會因最新 head 可讀而被忽略。這是備份完整性，不是交易資料品質／訓練有效性重審。

## 安裝與日常操作

從 repository root 執行；Python 由 `scripts/runtime_env.sh` 自動解析。
設定入口：`configs/data_sync/packed_backup.json`，不可把這台的 D UUID 複製到別台盲用。

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
./scripts/run_packed_backup.sh once --verify-existing # 強制重讀 C 現存物件及其 D 副本 checksum
sudo systemctl start stockagent-packed-backup.service
```

上述兩個手動命令可擇一使用；服務運作時以 flock 拒絕第二個備份程序。
全量複查可能讀取上 TB，請選非繁忙時段。服務對仍在來源的物件，每 30 日按到期重驗。
來源已刪、僅保留於 D 的歷史物件不在上述來源盤點內；要獨立稽核整個 D 庫可執行
下列唯讀全庫檢查（缺失歷史物件會報錯，不會刪除或自動修補）：

```bash
./scripts/run_packed_snapshot.sh full-audit \
  --sync-root /mnt/d/stockagent-backup/packed \
  --receipt-dir /var/lib/stockagent-packed-backup/full-audit
```

狀態位於 C：`/var/lib/stockagent-packed-backup/status.json`；D 上每批次也留
`D:\stockagent-backup\status.json`，完整通過時另留 `last-complete.json`。
C 的 `verified.sqlite3` 是可重建的校驗快取，不是還原資料的必要相依項目。

| 欄位 | 判讀 |
|---|---|
| `state` | copying／up_to_date／degraded／blocked |
| `total_bytes`, `verified_bytes`, `remaining_bytes` | 當次來源物件盤點、已驗證、尚待驗證的 bytes |
| `current_object`, `phase`, `current_object_bytes` | 單一物件 copy／resume_prefix／readback 的進度 |
| `pending_objects`, `pending_heads`, `pending_releases` | 整庫完成時必須全為 0 |
| `error_count`, `errors` | 必須為 0／空；不可只看 systemd active |
| `current_heads_complete` | 現行各 head 的相依物件是否已驗證；不能替代歷史完整性 |
| `present_objects_complete` | 這次 C 槽實存物件是否全部完成；不涵蓋來源本來就遺失的物件 |
| `integrity_state`, `previous_pass_errors` | 當前盤點中的檢查狀態與上一輪錯誤；copying 不等於完整性正常 |
| `last_complete_at`, `updated_at` | 上次完整備份與當前狀態更新時間；要同時看 |

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

## 驗收與限制

測試：`source scripts/runtime_env.sh && run_fintech_python -m pytest -q test/test_packed_backup.py test/test_packed_snapshots.py`。
包含標準還原、只增不刪、head 等候物件、歷史缺物件、校驗失敗、續傳、掛載／容量
防護與原子接收事件。實機另應確認進度持續增加，並抽一個已完成的 release 做獨立
`verify`；首次全量完成前只報告實際進度，不宣稱 C/D 已有兩份完整資料。

兩顆磁碟仍在同一台主機且持續可寫，不能抵禦整機災害、失竊或勒索軟體；這一份是
本機磁碟故障／來源誤刪復原層，不替代離線或異地備份。保留舊資料使 D 用量可能
大於 C；空間需要清理時必須另訂使用者核准的保留政策，禁止偷偷鏡像刪除。

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
