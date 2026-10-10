# 日常操作與唯一 owner

文中的 `scripts/`、`configs/`、`docs/` 均相對實際 StockAgent checkout；先確認
本機角色，再選本機可用的入口。底下的 read-only 查詢可直接使用；提前執行正式
service 會接續實際資料流程，限本次已授權的維運範圍。不要將 penguin 的 unit
或私有設定複製成 lab203／Vast 的新 owner。

## 1. 查進度與同步

penguin／Vast checkout：

```bash
source scripts/runtime_env.sh
stockagent-data automation-status --human --live
stockagent-data status --human

# 僅 penguin：Mamba 控制角色與正式 mount namespace
bash scripts/run_lakehouse_control.sh status
```

需要完整 JSON 時拿掉 `--human`，保留原始輸出及觀測時間；未安裝 CLI 使用
`bash scripts/run_data_cache.sh`。

要評估回收候選／阻礙時另跑 `stockagent-data gc --dry-run`。它不刪檔，也不
實際續租；Vast edge 仍會取得操作鎖、查收斂／程序引用並寫操作收據，因此不列為
完全唯讀查詢。鎖被占用或來源未收斂時，保留 defer／阻礙，不把沒有候選當作可刪除。
`use` 會下載／解封／續租，不是進度查詢。

penguin 再查來源端 NAS 覆蓋和歷史回傳階段：

```bash
run_fintech_python scripts/manage_backup_stream.py status
run_fintech_python scripts/status_vast_bulk_return.py
systemctl list-timers --all --no-pager
journalctl -u stockagent-backup-stream.service -n 40 --no-pager
journalctl -u stockagent-vast-bulk-return.service -n 40 --no-pager
```

`manage_backup_stream.py status` 讀取既有 `status.json`，不刷新 queue；要保存收據，
將 stdout 導向新的檔案，這個 action 的 `--output` 不會寫入狀態收據。長批次期間
status 可能落後反向 ACK，查接收端 heartbeat／收據年齡及既有 owner，不能為刷新
畫面另起 writer。上方腳本在 penguin 的 cold mount namespace 不同時可能無法完整
讀取 D 進度；依儲存契約到正式 owner namespace 核對，不把空清單當無資料。

Syncthing 完成須同時核對 paired device／folder 正確、connected、`idle`、100%、
need bytes／items／deletes 為零，folder／system errors 為零，valid／remote proof
符合該流程。`scanning` 即使 need 為零也未收斂。冷索引、payload demand、備份 ingress
與反向 receipts 各有邊界，不能拿一個 folder 通過代替其餘。

來源的 raw ACK admission 核對固定 manifest／READY、已登錄 file keys／SHA／
bytes 與真正 NAS 獨立還原回執；不把本機整批重讀放在下一批 sender 之前。
回收仍必須完成原本的 full SHA／exact set／primary 重建與 process gates。
新清冊錯誤保存原診斷並按 600 秒重試，已提交 registry 的來源繼續傳送；新增
版本尚未驗收不可算入完整性。不要讓一次清冊錯誤再次阻塞所有已登錄資料。
NTFS hash／copy 門檻與批次的實測範圍見 `docs/nas_send_verify_parallel_2026-10-06.md`；
不把單批 hash 或 copy 加速換算成 NAS 全流程的改善倍數。

## 2. 既有排程與責任

| 節點 | 排程／owner | 主要設定或契約 |
| --- | --- | --- |
| penguin | `stockagent-backup-stream.timer` | `configs/data_sync/backup_stream.json`；來源封閉批次與 NAS ACK 消費 |
| penguin | `stockagent-temporal.service`／`stockagent-storage-lifecycle.service` | PG persistence/visibility；catalog/source 兩條持久 workflows；私有 `/etc/stockagent/lakehouse-control.json` |
| penguin | `stockagent-lifecycle-control-backup.timer` | 三個 lifecycle DB 的一致性 SQL dump → 原 Restic relay |
| penguin | `stockagent-lake-transport-gc.timer` | 每次一批 NAS 已驗收暫存；canonical gates／每批鎖，保留 primary D/NAS |
| penguin | `stockagent-control-release-verification.timer`／`stockagent-control-backup.timer` | 固定程式碼驗證／private same-MVCC logical backup；`docs/control_plane_workflow.md` |
| penguin | `stockagent-remote-cold-artifact-ingress.timer` | `configs/data_sync/training_return.json`；新完成訓練成果 |
| penguin | `stockagent-legacy-return@main.timer`／`@partitions.timer` | `configs/data_sync/vastai_legacy_archive_return.json`；既存一次性歷史 cohort |
| penguin | `stockagent-vast-bulk-return.timer` | `configs/data_sync/vastai_bulk_preservation.json`；已註冊 bulk／cache cohort |
| penguin | `stockagent-d-cold-scan-retry.timer`／`stockagent-packed-transport.timer` | 精確 scan 重試與 edge payload demand |
| penguin | `stockagent-data-cache-gc.timer`／`stockagent-storage-pressure.timer` | materialization lease 與另行 allowlist 的 compiler cache |
| lab203 | `lab203-backup.timer`／`lab203-backup.service` | 既有單一 backup owner、pipeline 與 semantic post-hook；`lab203-nas.service` 掛載依賴 |
| lab203 | `lab203-lake-relay.timer`／`lab203-lake-relay.service` | 固定 immutable NAS archive adapter，原 NAS mount／owner 鎖，獨立還原後 ACK |
| Vast | Syncthing／Supervisor、同名 data-cache GC／storage-pressure cron | index-only edge；沒有 systemd 的節點從 Supervisor／cron 查，不另裝一份 timer |

用本機 unit／drop-in 和 runtime receipt 確認目前 schedule，runbook 的五分鐘或其他
間隔是部署紀錄。大型工作可能占用數個週期，不是固定完成 ETA。
核對某個正式 service 的精確狀態與最近退出碼：

```bash
systemctl show stockagent-backup-stream.service \
  -p ActiveState -p SubState -p Result -p ExecMainStatus -p FragmentPath -p DropInPaths
```

需要提前巡檢時，對已授權的**同一個** service 執行一次，例如 penguin：

```bash
systemctl start --no-block stockagent-backup-stream.service
```

確認同 unit／owner 沒有另一份手動 runner。不要平行直接啟動底層 Python worker，
不要臨時重啟正在恢復／發布的服務；調整前保存現有收據及程序身分。已授權
的 worker 升級優先等待 publication 邊界；有必要實測中斷接手時，沿既有
`verify_temporal_recovery.py`、durable intents 與實際 retry／history 驗收操作，
保留未封閉 staging，核對真正 loaded code。不把一般 restart 當作接手完成。

## 3. D 與 mount namespace

D HDD 現行 NTFS → guarded 正式掛載 → `/srv/stockagent-packed`，是唯一主冷庫。
熱 materialization 在 `/srv/stockagent-packed-materialized`，訓練 output／source
工作目錄是另一個生命週期。保留 D volume marker、mount／inode 身分及 8 KiB 9p
正式設定；原生 Windows FileStream adapter 只換 byte I/O。

IDE shell 與 systemd PID 1 可能在不同 mount namespace。已列入 allowlist 的
authority inventory／測量／安裝／交接使用現有 wrapper，例如：

```bash
bash scripts/run_authority_storage_operation.sh scripts/audit_node_storage_roles.py \
  --node-id penguin --output artifacts/operations/NEW_STORAGE_AUDIT.json
```

輸出位置需全新；wrapper 會進 PID 1 namespace 並載入既有私人 owner environment。
不要擴張 allowlist 來執行任意 shell，不只在另一個 namespace 做 mount check 後
留在原 namespace 讀寫。D 缺盤／marker 不符便停止冷操作，不新建 C 替代目錄，
不在進行中的交易 remount／格式化。匿名 SSD 還原 scratch 需其實體 backing drive
容量 guard，成功才清自己的暫存，失敗保留證據。
`stockagent-data status`／`automation-status` 在 penguin 已由 canonical wrapper
自動切到正式 namespace；其餘腳本仍按各自 owner 契約操作，不能自行忽略 guard。

## 4. lab203／NAS 全自動鏈

penguin Send Only `/srv/stockagent-backup-ingress-lab203`，由 D backing guarded bind；
lab203 Receive Only `/srv/lab203-backup/ingress`。反向回傳是 lab203 Send Only
`/srv/lab203-backup/receipts` → penguin Receive Only
`/srv/stockagent-backup-receipts-lab203`。配對身分由現行 paired config 核對。

1. 來源 owner 讀取未過期的 receiver readiness，按 config 的容量／pending 上限
   封裝 immutable objects 或一致性 code／SQL dump；核對 envelope identity、原始
   envelope SHA、READY、精確集合與全檔 SHA 後原子發布。`.staging` 不同步。
2. lab203 固定本機程式沿用 `/srv/lab203-backup/state/owner.lock`，驗證 delivery／
   dispatch、runtime、NAS mount、repository 與容量，備份並獨立還原固定 snapshot，
   全檔 SHA 通過才發機器 ACK。週期 `check --read-data` 按現場政策執行，查獨立收據。
3. 來源核對 ACK 身分並增加覆蓋；新 DB logical state 的 recovery request 由原
   queue／post-hook 處理。只有檔案 ACK 不代表 packed／PG 語義驗收通過。

auxiliary 的 `include_documentation: true` 也會把 Git-visible 公開
`.agents/skills` 完整放入 `code/working-tree.zip`，包含 SKILL、UI、references
與 helper；沿用設定值的 privacy filter，忽略／私密設定不納入。這是 NAS
工作樹備份的範圍，Python wheel／`source.zip` 並未因此加入 skills。
送達 NAS archive 不會自動在 lab203 啟用 skill；該節點要從驗證過的固定快照
取得完整目錄，再用當地 Codex discovery 核對。

lab203 已部署固定 pipeline／recovery hook。其本機既有設定與版本以 unit 的
FragmentPath／DropInPaths 和 runtime receipts 為準；維運位置包括
`/etc/lab203-backup/config.json`、`/opt/lab203-backup/receipts/runtime-lock.json`、
`/srv/lab203-backup/state`、`/var/log/lab203-backup/workflows`。私人設定不輸出內容。

在 **lab203 本機** 有需要且已授權時可提前執行同一服務：

```bash
sudo systemctl start lab203-backup.service
sudo journalctl -u lab203-backup.service -u lab203-nas.service -n 40 --no-pager
```

penguin 只有 Syncthing 資料／收據通道，沒有 lab203 SSH。日常工作不要求手動逐批
轉貼、不猜登入、不新開 SSH。語義任務是固定 schema 的資料，不能夾帶 shell commands
或從同步中變動的工具直接執行。程式升級另驗固定交接 manifest 並在本機安全邊界安裝。
沿用 NAS 私有登入與加密 repository；USB 異機金鑰保管已確認，避免再問或同步金鑰。
不啟用 Restic prune；來源已驗收的傳輸副本回收由其原 owner 按精確政策處理，
與 D cold objects／NAS snapshots 的刪除是不同交易。

## 5. 程式版本與控制庫

新 lakehouse 收據在 `/var/lib/stockagent/lakehouse-control`：
`catalog-status.json`、`source-replication-status.json`、`worker-ready.json`、
`traditional-backup-status.json`、`transport-gc-status.json`。只讀明確公開欄位，
不要列印私有設定／PG dump。NAS archive 的反向證據在
`/srv/stockagent-backup-receipts-lab203/lakehouse/relay-status.json` 與
`lake-<full identity>.json`；不能只靠這些檔名當作驗收。
`automation-status --human` 以相對路徑及 SHA 去重兩套已驗收 ledger，顯示總 NAS
覆蓋；archive／Restic 分項仍有重疊，不能相加。使用者轉述的 pilot 證據與
機器獨立還原證據分列，不能把前者當作全檔機器驗收。

lab203 新 relay 固定程式在 `/opt/lab203-lake-relay/code`，私有 policy 在
`/etc/lab203-backup/lake-relay.json`。只在 lab203 本機維運同一服務：

```bash
sudo systemctl start lab203-lake-relay.service
sudo journalctl -u lab203-lake-relay.service -u lab203-nas.service -n 40 --no-pager
```

source 新增 raw waves 已由 Temporal 接手；原 backup stream 保留 ACK／auxiliary／
恢復、原始 manifest/head metadata 和傳統 SQL 備份。現行 `decouple_nas_acceptance`
讓來源持續發布，不等四批 NAS ACK；256 GiB retained spool／64 GiB physical
reserve 控制容量，已發布 file keys 不重複入列。paired 失敗回執只重傳該固定
批次，保留失敗 attempt；其他資料繼續傳送。NAS完成／回收仍需真正 ACK。
Restic source 亦不以 NAS freshness 阻止發布，接收端自行驗實際 NAS guards。
完整生效邊界見 `docs/nas_send_verify_parallel_2026-10-06.md`。公開候選
設定在 `configs/data_sync/lake_source_replication.json`；實際生效需核對私有設定與
worker 已載入的程式身分。完整補傳以固定 cohort 的逐檔機器收據結案，另持續處理
新發布資料，避免 moving denominator 掩蓋進度；見
`docs/nas_sync_catchup_2026-10-06.md`。不還原 live PG files，不同步正在寫的 Restic repository。
已收到但尚未入庫的 legacy compressed carrier 另凍結 received cohort；沿原 bulk
owner 修復／分段發布，原值 cold recovery 後才併入 NAS 逐檔驗收清冊。
`scripts/audit_nas_sync.py --upstream-cohort PATH --upstream-sha256 SHA`
可同時觀察 fixed cold cohort 與 received originals；`--require-transport-convergence`
另要求 paired connected、正反向 idle／need 0／error 0。它是唯讀 observer，
不另開 publisher 或 NAS writer；逾時退出 75、state 非 accepted 均未結案。
新碼需固定本機安裝，不能直接執行後續同步收到的程式。現場已完成固定 v1
安裝，日常資料不需再次詢問 Codex 或手動轉貼。

```bash
bash scripts/run_control_release_queue.sh status
stockagent-agent work status
```

前者核對既有 private control environment／runtime lock，再讀正式收據；不要
列印 env／DSN。code 發布沿原 `scripts/build_project_release.py`，取得固定
receipt 和 `build-source` 後使用 `run_control_release_queue.sh enroll`；精確
命令見 `docs/control_plane_workflow.md`。登錄後 timer 自動處理，以 full receipt
SHA 的 exact work key claim。已成功工作不重送，但持續驗目前 frozen bytes；
任意變動會降低 readiness。不要把登錄、DB row 或同主機 dump 當作 NAS 還原。

`automation-status` 同時觀測 code verifier、control backup 與 NAS backup 的
原 owner／收據。新 control dump 由既有 auxiliary pipeline 封閉／備份，NAS
ACK 後新 logical identity 另由固定 recovery queue／post-hook 實際還原。

## 6. 開機／故障恢復

penguin 先讀 `/var/lib/stockagent/boot-recovery/latest.json` 的觀測時間與
`boot_recovery` 欄位。需要新的診斷收據時用
`bash scripts/run_boot_recovery_audit.sh` 核對既有 Windows supervisor、
foreground WSL holder、PID 1 mount guard、必要 services 與業務收據。WSL 的
systemd active 不能讓 Windows 關閉的 WSL 自行存活。當前 audit 成功是當前
readiness，cold reboot 耗時需實際 boot observer／業務恢復證據。
已選定的 code-verification role 還需 enabled／active timer、最近 service
成功與五分鐘內的 semantic receipt；空 registry、exit 75 或上次失敗不算 ready。
lakehouse gate 另核對四個正式 owner、15 分鐘內 catalog/source/paired relay
收據、兩條 RUNNING workflows 及 worker 載入碼與現行 fingerprint 相符。
Temporal frontend 重啟不連帶停 worker；worker 中止後依 60 秒 heartbeat 接手。
HDD 暫存回收獨立排程，不應當作流程尚未提交 NAS ACK 的理由。
正常 continue-as-new 保留 workflow ID、更新 run ID 並保留鏈；不是流程重建
或資料遺失。長停 NAS 的 continuation 必須攜帶固定 pending delivery，不能
換成 moving latest。遇到新版 code 接手，先做既有持久 history replay。

lab203 保留 Windows 喚醒／WSL holder 和同一 backup owner；Windows 登出驗收
使用者已取消，不能稱已通過。Vast 沿用其 Supervisor／cron，不以宿主重啟推定
overlay 仍有資料；按 exact code/runtime/source 重新準備，checkpoint 相容性另驗。

對應 repo runbooks：`docs/automatic_cold_storage.md`、
`docs/continuous_nas_backup_2026-10-04.md`、`docs/lab203_automatic_backup_2026-10-04.md`、
`docs/lab203_parallel_backup_2026-10-04.md`、`docs/all_services_boot_recovery_2026-10-05.md`。
