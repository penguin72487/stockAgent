# 持續增量同步與 NAS 備份（2026-10-04）

本次把兩個試驗批次延伸成持續備份流程：penguin 封閉固定輸入，Syncthing
送至 lab203，沿用 lab203 既有 worker 寫入 NAS，固定 snapshot 實際還原通過後，
回傳不含私密設定的收據；來源可持續封裝其他批次，不等待上一批收據。NAS 是離機備份；同一 D 槽的 transport
只是傳送暫存。來源與接收端回傳協定已接通，取得真實 NAS 固定還原收據。
本輪完整盤點與歷史處置見[全量備份分析](lab203_complete_backup_rollout_2026-10-04.md)。
使用者最新要求全自動、不再逐批人工轉貼。一般檔案流程已自動運作；新增
還原驗收任務與一次性 v7 post-hook 安裝見
[自動流程](lab203_automatic_backup_2026-10-04.md)。
使用者後續要求整鏈並行，來源已調整為四批／32 GiB 同時在途及四工封裝；
NAS 固定 v10 已取得配對 `pipeline-status.json` 部署證據，現場量測選定四批／
兩工備份／兩工還原／四工驗證；詳見[並行管線](lab203_parallel_backup_2026-10-04.md)。
服務啟用與逐批檔案還原均有各自的收據，全歷史覆蓋持續自動補傳。

## 目前證據

| 項目 | 已驗證範圍 |
| --- | --- |
| 既有兩個 NAS 批次 | 使用者轉交 lab203 驗收；保留 user-relayed 標記，原始私有收據仍在 lab203 |
| 第二批 TAIFEX | 401 檔、409,347,973 bytes，完整流程 21.29 秒；snapshot `d9cf528ac01b42408e00ad41cc3891f7d82ab924c6361dea0ba539836ce32317` |
| 來源 transport | 原 419 檔、472,532,460 bytes 複製至既有 D 槽並逐檔核對，原 C 檔保留；bind mount 不更改 Syncthing 公開路徑 |
| 全量冷庫盤點 | 267 個觀察 release、242 個有效保留；17,544 個可用檔、684,032,385,699 bytes；158 個 current head 的物件集合完整；含 81 個無引用冷物件與 49 個 head-history 檔 |
| 歷史缺件處置 | 使用者本輪明確放棄 25 個非目前版本、114 個已缺物件恢復；現存 metadata／物件保留，不宣稱全歷史成功 |
| NAS 機器回傳 | 兩個 code／control 批次與第一波 98 檔冷資料，均取得固定 NAS snapshot 的獨立還原／完整 SHA 收據 |
| 有界 transport 現場驗收 | 第一波 98 檔、1,055,120 bytes 回收；來源完整 SHA／NAS 收據／覆蓋保留，lab203 completion 100%、needDeletes 0 |
| 來源加密 code／SQL 還原 | 一次固定本機 Restic snapshot，2339 個程式 ZIP 成員逐檔吻合；50 張表、60 筆資料全部欄位／列指紋吻合；完整流程 5.793 秒 |
| 相關回歸 | 134 項通過、0 skipped（`all-backup-focused-tests-accepted-final-v2-20261004.xml`）；含真實 Restic 命令、歷史處置、回收與中斷接手、物理／bind 別名引用、單一 owner 交接、容量／配對保護、跨批重建及 runtime identity |
| NAS code／SQL、NAS canonical 重建 | v7 三項固定驗收已取得機器收據；後續新 release／logical identity 仍各自驗收，不能推定全歷史 |
| NAS 並行 driver | v10 固定 manifest 已由 penguin 驗證；四批／backup 2／restore 2／verify 4，同一 timer 完成後 15 秒接續 |
| USB 金鑰保管 | 2026-10-04 使用者明確確認受控 USB 已保管完成；不再詢問。舊檔案 ACK 不驗證此項，仍保留其原欄位 |

數量是本次盤點，不代表來源即時性、所有未發布資料或整台電腦已備份。14 個
catalog `publish:false` 的資料集不會被備份程式改名繞過發佈限制。程式快照包括
未提交、未追蹤但 Git-visible 的公開程式、設定、文件、測試及 tracked deletion
清單；忽略的私密設定、憑證、API 金鑰及 Git object database 不會傳送。

來源端證據集中在
`artifacts/operations/two-node-lab-nas-backup-20261003/`，其中
`local-code-sql-recovery-mamba-20261004/acceptance.json` 為本機實際還原，
`history-vast-exact-recovery.json` 為歷史物件查找。失敗的原始執行／收據保留，
成功後新增證據，不把失敗紀錄改成成功。

## 已部署來源端

| 項目 | 現行值 |
| --- | --- |
| Producer | penguin，既有資料／網站來源權威 |
| source service／timer | `stockagent-backup-stream.service`／`stockagent-backup-stream.timer`，同一 owner 完成後 30 秒接續 |
| source owner／ledger | `/var/lib/stockagent/backup-stream/owner.lock`／`ledger.json` |
| source runtime | 既有驗收 Miniforge／Mamba backup role；`/etc/stockagent/backup-stream.env` 選擇，執行前核對 runtime lock |
| canonical cold | `/srv/stockagent-packed`，不修改其 heads、原物件、publisher 身分或發佈排程 |
| transport backing | `/srv/stockagent-d-volume/stockagent-backup-ingress-lab203`，已有 D volume 身分／mount guard |
| transport bind | `/srv/stockagent-backup-ingress-lab203`，mount service 與 Syncthing 依賴保證缺盤不落回 C |
| sender Folder ID | `stockagent-backup-ingress-lab203`，penguin Send Only／lab203 Receive Only |
| 回傳 Folder ID | `stockagent-backup-receipts-lab203`，penguin Receive Only／lab203 Send Only |
| source 回傳接收路徑 | `/srv/stockagent-backup-receipts-lab203` |
| 預設上限 | 每批 8 GiB／512 個資料成員，最多 4 個未驗收批次；pending 32 GiB／staging 16 GiB，transport 保留上限 1 TiB；單輪 timeout 8 小時 |
| 來源 worker | copy 4／verify 4；每輪至多 4 wave，掃描上一批同時封裝下一批；失敗 30／120／600 秒退避 |
| 最低餘量 | 來源、lab203 ingress、NAS 至少保留 64 GiB；接收端現場可提高保留量或降低單批上限 |

來源優先備份程式及 same-MVCC PostgreSQL dump，再送 current heads 與物件，
接著送尚未覆蓋的保留歷史；相同 immutable SHA 跨版本只送一次。程式快照
每小時檢查或在控制庫的新 dump 出現時檢查。控制庫 dump 仍由既有排程持有，
transport 排程與 worker 數不代表資料庫 dump 的 RPO。
本次盤點最大可用物件 5,707,388,416 bytes；沒有現有物件超出 8 GiB 單批預算。
未來出現更大物件會明確等待容量調整，不切掉物件或跳過後稱完整。

`.staging` 不同步；驗證成功才在同一檔案系統原子 rename 為
`batch-<envelope_identity>`。先保存 journal 再 rename，重新啟動可接手未完成發布。
failed copy 與舊快照保留且計入容量上限，不自行刪除。每次來源 poll 寫入
`tools/source-status.json` 與可恢復版本／固定 NAS snapshot 的
`tools/recovery-index.json`。後者標明使用者回報或機器收據的來源，不單憑索引
宣稱還原成功。

全量冷檔大於 lab203 可用 ingress，因此來源另啟用精確限定的冷物件傳輸暫存回收。
僅 machine-ACKed `incremental_cold_objects`、來源與 full-SHA 檔案身分未變、
精確集合／程序／配對與 idle convergence 全部通過者可回收；保存 dry-run、
耐久 owner 意圖與固定 NAS recovery index，刪除後完整掃描 Syncthing。
code／SQL、user-relayed pilot、失敗 staging、canonical source 與 NAS snapshots
保留。接收端 runner 不自行刪批，來源擁有的 Send Only 刪除會同步到 lab203。
完整規則與現場驗收見[全量備份分析](lab203_complete_backup_rollout_2026-10-04.md)。

`readiness.json` 需由接收端在持有既有 owner、實際核對 NAS mount／repository、
runtime lock／容量後產生；15 分鐘失效。沒有有效 readiness 時，來源服務可
完成本機盤點並等待，但不發布大型批次。Syncthing idle／100% 只證明傳輸。
來源 NAS 覆蓋只增加於固定身分驗收收據或明確標記的既有使用者回報基線。
來源狀態檔與 service exit 0 都不能當作 NAS 備份全部完成。

來源讀取狀態：

```bash
bash scripts/run_backup_stream.sh status
systemctl status stockagent-backup-stream.timer stockagent-backup-transport-mount.service
journalctl -u stockagent-backup-stream.service
```

## lab203 已接通的協定與剩餘驗收

已接通的 adapter 來自 `tools/continuous-backup-20261004-v2/`。
本輪最新分析、公開 source policy 與恢復工具另封閉於
`tools/continuous-backup-20261004-v4/`；v4 補齊正式 514 檔 NAS 驗收與物理別名引用檢查。
source config 是來源部署的紀錄，
不要將其 `/root/stockAgent` 路徑套在 lab203。不改寫原 v1／v2／v3。
其中 `handoff-manifest.json` 固定每個成員的 SHA-256／bytes，摘要會提供其原始
檔 SHA-256。package 另含 closed envelope／READY，可先用已驗收的
`ingress/tools/verify_backup_delivery.py` 及交接摘要提供的規範化 envelope 身分
驗整個 package，核對 manifest 原始檔與精確成員集合，再複製到本機 owner 管理的
`/opt/lab203-backup/`。執行程式不要直接追蹤 Syncthing 中移動的版本。
v1 已傳送且保留；v2 的 paired profile 補齊 readiness 900 秒有效期，避免接收端
直接沿用該 profile 時缺少 age 欄位。v2 已用於接通回傳，不改寫已封閉 package。
先前 queue／回收調整沿用既有 lab203 worker；最新 NAS 階段並行已完成固定 v10 本機升級。

### 保留現場部署並接上共同協定

沿用 `lab203-backup.service`、`lab203-backup.timer`、
`/opt/lab203-backup/code/worker.py`、`/etc/lab203-backup/config.json`、
`/opt/lab203-backup/env`、`/srv/lab203-backup/state/owner.lock`、
`/opt/lab203-backup/receipts/runtime-lock.json` 與 `lab203-nas.service`。
私有 config 保持 600，不建立第二個 backup owner。v10 使用同一 timer 的 drop-in，
完成後 15 秒接續，保留舊 worker／設定、既有重試 journal 與 v7 post-hook。
新 driver 位於 `/opt/lab203-backup/backup-pipeline-code-v10`，使用
`/etc/lab203-backup/backup-pipeline-v10.json`／`.env`。
既有 retries／失敗保留／後續有效批次繼續處理維持。延長正式單次 timeout 以容納
8 GiB restore 及週期全庫 read-data，不能沿用只有 63 MB 試驗的短 timeout。

回傳 folder 配對僅限下列兩個身分，不開自動接受、不恢復舊分享或訓練／網站：

- penguin：`QZTXXEL-YBCBYK7-ZK2ZSMS-DQKVSCE-IFC7MIX-4CWG6LE-KHPDLJI-7DSE6QW`
- lab203：`TLOI2HH-6EZOSBC-H6YMGVW-APQOGFW-P2A7FZT-EKX6IUP-2HMRKS6-LBDHOQM`
- Folder ID：`stockagent-backup-receipts-lab203`
- lab203：`/srv/lab203-backup/receipts`，Send Only
- penguin 已設定 `/srv/stockagent-backup-receipts-lab203`，Receive Only

已交付 [`configure_backup_receipts_syncthing.py`](../scripts/configure_backup_receipts_syncthing.py)
及 paired profile。在 lab203 用現有 Syncthing 本機 API credential 取得方式；腳本
預設 discovery 如與現場 config 位置不同，沿用現場 discovery，憑證不要輸出或放入
回傳 folder。設定只新增這個 folder，核对所有既有分享前後不變。

共同標準庫介面在
[`backup_delivery_receipt.py`](../scripts/backup_delivery_receipt.py)：

```python
from scripts.backup_delivery_receipt import (
    read_json, verify_dispatch_delivery, publish, publish_readiness,
)

# 均在現有 worker 的 owner lock 內；下面 proof 必須來自實際命令／檢查。
dispatch = read_json(dispatch_file)
verify_dispatch_delivery(delivery, dispatch)  # backup 前的共同 gate
publish(delivery, dispatch, private_acceptance_proof_file, receipt_output_root)
publish_readiness(paired_configuration, private_readiness_proof_file, receipt_output_root)
```

`tools/dispatch/<envelope>.json` 固定 paired devices、目標 repo、兩種 envelope
hash、完整檔數／bytes、delivery kind。資料成員可能先於或晚於 dispatch 到達；
未齊維持 75／稍後重試，不能用 75 發成功 ACK。接收端必須另核對 paired device
及 repo 等於現場既有 repository。每批 READY／exact set／完整 SHA 均要通過。
新的 `incremental_cold_objects` 只是固定物件片段，不能套用「本批已包含整個
release」假設。`code_and_control_backup` 包含公開 code ZIP 與一致性 dump。

`publish()` 的私有 acceptance proof 必須從現場完整流程產生，所需欄位如下；
不要用預填 True 或 service returncode 合成證據：

```text
repository_id, snapshot_id（64 字元完整固定值）, envelope_identity_sha256,
envelope_file_sha256, complete_files, complete_bytes, repository_check_mode,
repository_check_verified, independent_restore_verified, source_verifier_verified,
all_files_sha256_verified, command_exit_codes（所有實际驗收命令均 0）,
complete_workflow_seconds（完整流程實測）, accepted_at_utc（含時區）
```

`publish_readiness()` 所需的私有 proof：

```text
repository_id（從 guarded NAS 真實讀取）, ingress_free_bytes, nas_free_bytes,
reserve_bytes（至少 64 GiB）, maximum_batch_bytes（至多 8 GiB）,
repository_check_mode, single_owner_verified, nas_mount_guard_verified,
runtime_lock_verified, automatic_pruning=false, automatic_batch_deletion=false
```

容量用實際 `disk_usage`，repo ID 必須是
`c717a3d315e20a5a3314c595e66607fcb7d6646cb73ad4412cc2cccb22abb798`。
每次 timer 執行刷新 readiness 並掃描回傳 folder；NAS 失連不能用未掛載的同名本機
資料夾替代。private proof 保留在現有 state／workflow 路徑。adapter 只把允許欄位
及其 canonical hash 放入回傳 folder，不同步私有 config、NAS 路徑、密碼或金鑰。
此 hash 是內容識別，對端身分由既有 Syncthing 配對與加密連線提供。

WSL 另核對實際 VHDX 所在 Windows 磁碟的餘量，不能只看虛擬 filesystem 上限。
每批預留最多两份獨立 restore 的暫存空間及既有保留餘量。完整驗收、固定 NAS
snapshot 與逐檔 hash 收據耐久保存後，才可回收該次 worker 自行建立、無程序引用
的專用 restore scratch；保留 cleanup inventory／結果。這不包含 ingress 原批次、
NAS snapshots／repository 或 canonical source。接收端自行刪批與 NAS prune 仍關閉；
來源限定範圍的已驗收傳輸回收另見上節。
不能讓數百 GB 的重複 restore scratch 無限累積。

備份成功後必須保存真正固定的 NAS snapshot ID，重啟接手沿用該 snapshot；
實際 restore 到新獨立目錄，再跑來源 verifier 及完整檔案 SHA。ACK 不接受
`latest`、短 snapshot ID、不一致 repo／raw envelope、非 0 命令或缺漏步驟。
重複驗收保留第一次完整固定 snapshot 的不可變 ACK。

### 對現場設備測量再調整 repository check

初期延續 `full_read_data`。同一已封閉 TAIFEX 批次，在同一 NAS／同一 role 下
交錯各三次量完整 backup、check、固定 restore、逐檔 SHA；比較全庫
`check --read-data` 與 `check` 加當批固定 snapshot 的完整獨立 restore。
兩者都必須完成驗收，不用只計 backup 或取消 SHA 改善數字。記錄庫大小、快取
是否清除、首次／暖機、實際總時間及所有 exit codes。

只有現場驗證覆蓋及實測效益成立才採
`structural_plus_fixed_snapshot_full_restore` 作每批模式。這一模式驗證當批所有
restore bytes，但沒有逐批讀完整庫的舊資料；需於同一 owner 排入至少每週一次
全庫 `check --read-data`，另存全庫驗收收據。週期查核失敗保留真實故障並告警，
不可拿後續當批成功將它清除。不自動 prune；接收端不另設刪批 owner。

### NAS 原始資料與 PostgreSQL 實際還原

三個來源固定 NAS snapshot 與單獨回傳協定已準備，v5 保留一次性診斷介面。
最新 v7 以資料任務與現有 service 的固定 post-hook 自動接手，同一 owner 執行
CFTC、TAIFEX canonical 重建及 50 表／60 列控制庫 logical restore；後續新的
已備份 DB 邏輯狀態自動派送，不需每批轉贴。詳見
[自動流程](lab203_automatic_backup_2026-10-04.md)。
成功才回傳 semantic acceptance，來源下一輪自動列在 `independent_recovery`
與 `automatic_recovery`，一般檔案 ACK 的原契約維持。

首次先從固定首批 NAS snapshot
`2145243446a4b6b3a29523cbfd3efa8d4001f378a1196d296bb377a33ef25444`
還原 CFTC 批次，沿用共同 packed verifier，驗 canonical pack／CRC、fetch，
再比較原始來源 fingerprint。固定 release：

```text
dataset: cftc-legacy-pre2000
snapshot: cftc-legacy-pre2000-20260902T164739014992260Z-l0-penguin-d53c80bf8d88e2e9
manifest SHA-256: efc966614610dbb35d1d7078e97fca7144bff337eebbcda75621d5d9f8f46322
source fingerprint: 9069d037cf15525b36eb3bd5f729b40aae6e9789292fd031aae51f6d7df307da
envelope: 350a50881fdf7ae082c77e03671c6ae61bdcb0d92ea1bfa4206f64c415c0803f
```

[`verify_restored_backup.py`](../scripts/verify_restored_backup.py) 的 `packed`
mode 處理完整舊批次，`auxiliary` mode 核對 code ZIP 的精確內部集合、逐檔
SHA、same-MVCC dump，`--logical-restore-root` 進一步建立 fresh 私有 PostgreSQL
cluster 做實際 restore，全欄位／列 state 要吻合；cluster 不開 TCP，不改正式庫，
驗收後停止該新 cluster，保留證據。SQL recovery 是獨立於當批 file ACK 的證據。

先在現有 role 檢查 psycopg／PG 工具；若缺少，使用已交付 control-recovery 的
Linux explicit lock 建立新的 Miniforge／Mamba prefix，不修改正式 backup role
或 runtime lock。新的 PG binaries 必須可由 `nobody` 讀取／執行。fresh cluster
放 `/var/lib/` 下，不放不可穿越的 `/root` 祖先。用該 role 的
`FINTECH_ENV_PATH`、共同 `scripts/runtime_env.sh`／`run_fintech_python` 執行。
驗證的是新 NAS code／SQL batch 的獨立 restore，來源本機 SQL 結果不能替代。

跨新批次還原使用 `tools/recovery-index.json` 挑出特定 release manifest／SHA
及必需物件對應的固定 NAS snapshots，各自實際 restore。將各已還原 delivery
的根路徑、規範化 envelope hash、原始 envelope SHA 写入私有 JSON list，再執行
[`assemble_restored_backup.py`](../scripts/assemble_restored_backup.py)：

```text
--restored-inputs <private JSON list>
--cold-root <fresh independent union>
--materialized-root <fresh source output>
--dataset <fixed dataset> --snapshot-id <fixed source release>
--manifest-sha256 <source-pinned manifest SHA> --output <external private evidence JSON>
```

工具拒絕缺少任一 closure 物件；只從已驗證還原檔複製所需 manifest／immutable
objects，重驗 pack/blob、CRC，再 fetch／完整 source fingerprint，不複製
`.local-state` 或 moving heads。caller 另保留固定 NAS restore 的 provenance。

### 受控 USB 金鑰保管

2026-10-04 使用者明確表示「USB 金鑰保管已經保管好了不要再問了」。保管項目
記為使用者確認完成，證據在
[`backup_key_custody_20261004.json`](../configs/data_sync/backup_key_custody_20261004.json)。
不再要求媒體或副本操作。這是保管完成的使用者證據，不改写舊檔案 ACK 的
`independent_key_custody_verified=false`；該欄位只說明該 ACK 未驗證 custody。
來源另列 `usb_key_custody_user_confirmed=true`。金鑰內容、金鑰雜湊及私人保管
路徑仍不進 Git／Syncthing／聊天。Windows 登出驗收依使用者指示維持取消。

### 接續驗收的自動回傳

日常 managed batch 的兩種 envelope hash、完整檔數／bytes、NAS repository／
固定 snapshot、ACK 身分與語義驗收，由固定 runner 經反向 Syncthing folder
自動回傳；penguin 驗 readiness／ACK 後接續下一批，不要求使用者轉貼。
原始私有流程收據留在 `/var/log/lab203-backup/workflows`，檔案、SQL 與 canonical
重建維持分開的 machine evidence；USB 保管依既有使用者確認。
不以新批次成功宣稱全歷史完整；114 個已缺物件依本輪指示放棄恢復，
未發佈資料維持不同的覆蓋範圍。

## 維持的操作界線

不開 SSH、不建立付費資源、不恢復舊 Syncthing 分享、不傳 key、不同步 live
Restic repository／PostgreSQL data directory，不啟用 prune 或 canonical source 清理。
來源的傳輸副本回收只適用上節精確 gate；接收端不自行新增刪除流程。
已驗收的 Windows 交接檔不重做。接收端回傳已接通，但啟用 timer 不代表
全量 backlog 已跑完；每一批仍需真實固定 NAS 還原收據。
