# lab203 一次性安裝指令

給 lab203 本機 Codex。使用者已授權本機一次性安裝；後續資料全部由
服務自動接收、複寫、恢復驗收並回 ACK，不需逐批轉貼。

現場一次性安裝已完成；反向機器 ACK 已證明固定 catalog/data 的實際 NAS
獨立還原、PG logical restore 和 native DuckLake 全表／snapshot 核對，以及
多批原 CAS 的完整讀回 SHA。下方是保留的一次性安裝紀錄，不需逐批重跑。
最新部署／scope 見[總 runbook](ducklake_temporal_replication_2026-10-05.md)。

```text
請在 lab203 的 Ubuntu WSL 安裝 immutable lake relay，保留現有
lab203-backup.service/timer、lab203-nas.service、Syncthing identity 與
USB 金鑰；不啟用 SSH、不刪任何新資料／NAS snapshot、不啟用 prune。

套件：/srv/lab203-backup/ingress/tools/immutable-lake-relay-20261005-v1
套件固定 manifest identity：
fbb02cbe5003f9eec69fa71976d2bb529302fe90c2e97dcafd6b8b41b1cd4130
scripts/install_lab203_lake_relay.py SHA-256：
7771361a92ed9cd9baab04ecd7b7b4a6b434d2b51a6ac4ab1e73c22c278cbb55

先確認該 package 接收完整，核對 installer SHA，再用已驗收的本機
Mamba backup interpreter 執行。PYTHONDONTWRITEBYTECODE=1；透過
package/scripts/runtime_env.sh 的 run_fintech_python -B，不更新原環境。

run_fintech_python -B PACKAGE/scripts/install_lab203_lake_relay.py \
  --expected-identity fbb02cbe5003f9eec69fa71976d2bb529302fe90c2e97dcafd6b8b41b1cd4130

PACKAGE 替換成上述完整路徑。現行 private pipeline 預設
/etc/lab203-backup/backup-pipeline-v10.json；若現場實際路徑不同，依當前
unit/env 載入的設定加 --existing-pipeline PATH。不要輸出設定內容。
若 PG server binaries 不在 /usr/lib/postgresql，使用已驗收的本機
control-recovery role 的 initdb/pg_ctl/pg_restore 所在目錄，加 --pg-bin PATH。
不要提供帳號／密碼／金鑰給 penguin。

安裝器先驗 sealed exact set／SHA，複製固定 public code 到
/opt/lab203-lake-relay/code，再用既有 Miniforge/Mamba 根據 Linux explicit
lock 建立獨立 role。沿用既有 NAS mount policy 與 owner.lock；rclone 僅寫
NAS user directory 的新 stockagent-immutable-lake-v1 archive，原 Restic
repository 保留。新增 lab203-lake-relay.service/timer，每次完成後 30 秒
再巡檢；owner busy 為 exit 75，不能當作驗收成功，也不能中止舊 owner。

啟動同一服務並確認 /srv/lab203-backup/receipts/lakehouse 下的
relay-status.json 與 lake-<full identity>.json。必须完成 NAS rclone
copy --immutable、獨立讀回、exact set／full SHA，以及 catalog/data 類型的
隔離 PG restore 和原生 DuckLake 全表／snapshot 核對，才允許 NAS ACK。
反向 Syncthing receipt folder 保持 Send Only。失敗 evidence 保留在
/srv/lab203-backup/lake-archive-state，不自行清 source／NAS／transport。

請優先處理這一次安裝；之後不需要人工逐批執行。安裝及首批結果可簡短
回報，真正自動接手以反向機器 ACK 為準。若在安裝器途中失敗，保留原
logs／receipt，依 source pin 修正現場環境，不替換 moving synced code。
```
