# lab203 固定 NAS snapshot 的獨立重建驗收（2026-10-04）

USB 金鑰保管已由使用者明確確認完成；不再詢問、不再安排媒體操作。
此記錄是使用者確認，舊檔案 ACK 內的 custody=false 只表示該 ACK 沒有驗證金鑰保管，
不代表保管未完成。Windows 登出驗收維持取消。

使用者最新要求全自動、不再逐批人工交接。常態流程改用
[v7 自動任務／同一 service post-hook](lab203_automatic_backup_2026-10-04.md)，
本頁保留 v5 的固定 snapshot 診斷介面與驗收語義。正常批次不需要執行下列手動命令。

## 交給 lab203 本機 Codex 執行

沿用 `lab203-backup.service`／timer、`/opt/lab203-backup/code/worker.py`、
`/etc/lab203-backup/config.json`、`/srv/lab203-backup/state/owner.lock` 與現有 NAS。
不新增 SSH、不建立第二個備份排程；資料同步與既有五分鐘備份繼續運作。
新工具只在同一 owner lock 內執行一次固定 NAS snapshot 的還原與語意驗收。

1. 在 `/srv/lab203-backup/ingress/tools/continuous-backup-20261004-v5/`，使用來源
   提供的規範化 envelope 身分和 manifest 原始 SHA 核對 READY、精確集合與全部 SHA。
   來源會另外提供這兩個識別。原 v1～v4 與正在執行的 worker 都保留。
2. 將完整、已驗證 v5 複製到新的 `/opt/lab203-backup/recovery-code-v5/`，從這個
   固定目錄執行。不要在同步中的可變路徑直接執行，也不要替換目前 worker。
3. 先核對現有 backup role／runtime lock；若已有可用的 psycopg、PostgreSQL
   `initdb`／`pg_ctl`／`pg_restore`，可沿用被核對的 role。若缺少，使用新獨立
   Miniforge／Mamba prefix 和包內
   `configs/environments/locks/control-recovery-linux-64-20261003.explicit.txt`。
   不修改正式 backup role；新 role 的 binaries 與新 prefix 父目錄需可由 `nobody`
   執行。以包內 `stockagent.runtime_identity.runtime_identity()` 建立新 role 的
   private runtime-lock.json，保留 explicit 套件鎖／安裝收據。
4. 沿用現場 NAS 掛載與 Restic 密碼檔。不要把 private config、金鑰內容或原始日誌
   放入 ingress、回傳 folder 或聊天。若現有 NAS guard profile 不符合 `NasTarget`
   格式，從現有私有設定建立 `/etc/lab203-backup/nas-recovery-target.json`，600。
   它的格式為 `schema_version=1`、`automatic_pruning=false`、`reserve_bytes`，
   `nas={host,share,mount_point,relative_directory,repository_directory}`。
   `relative_directory` 是現有 repository 父目錄相對既有 SMB mount 的路徑；
   `repository_directory` 是現有最後一層目錄。不得建立新的 repository。
   執行前另以現場既有方法核對 Ubuntu VHDX 所在 Windows 實體磁碟的剩餘容量，
   保留至少 64 GiB 加本次還原／重建預算；工具的 Linux disk_usage 不代替這項檢查。
5. 使用同一 owner 執行下列命令。`$recovery_prefix`、`$nas_profile`、`$restic_binary`、
   `$password_file`、`$recovery_runtime_lock` 均由現場已核對的私有設定選取；不輸出內容。

```bash
cd /opt/lab203-backup/recovery-code-v5
export FINTECH_ENV_PATH="$recovery_prefix"
unset PYTHON_BIN
source scripts/runtime_env.sh
run_fintech_python scripts/run_nas_recovery_acceptance.py \
  --plan configs/data_sync/lab203_nas_recovery_plan_20261004.json run \
  --nas-configuration "$nas_profile" \
  --restic "$restic_binary" --password-file "$password_file" \
  --runtime-lock "$recovery_runtime_lock" \
  --owner-lock /srv/lab203-backup/state/owner.lock \
  --pg-bin "$recovery_prefix/bin" \
  --output "/var/lib/lab203-nas-recovery-$(date -u +%Y%m%dT%H%M%SZ)" \
  --receipt-root /srv/lab203-backup/receipts --wait-for-owner
```

## 固定驗收輸入

固定 plan identity：`46a06550de246c7a9cf7aad97251c524d1df2faf0b470891e03868e522b25a89`。
[`lab203_nas_recovery_plan_20261004.json`](../configs/data_sync/lab203_nas_recovery_plan_20261004.json)
同時固定 paired devices、repository、完整 NAS snapshot、两種 envelope hash、
完整檔數／bytes、source manifest／fingerprint 與 same-MVCC SQL 身分。

| 工作 | 固定 NAS snapshot | 預期 |
| --- | --- | --- |
| CFTC canonical 重建 | `2145243446a4b6b3a29523cbfd3efa8d4001f378a1196d296bb377a33ef25444` | 13 檔，63,166,382 bytes；指定 manifest、pack/blob／CRC、fetch 與原始 source fingerprint |
| TAIFEX canonical 重建 | `d9cf528ac01b42408e00ad41cc3891f7d82ab924c6361dea0ba539836ce32317` | 401 檔，409,347,973 bytes；指定 source release 的完整 closure、CRC、fetch／fingerprint |
| 程式／控制庫 logical restore | `54e0b3fbdd8e1e558aa7dc29026b3de283ca17dc22e33cc0e615743b003a1ee8` | 7 檔，12,425,117 bytes；code ZIP 全部 SHA、50 表／60 列全部欄位與列的 same-MVCC 狀態一致 |

NAS repository ID 必須仍為
`c717a3d315e20a5a3314c595e66607fcb7d6646cb73ad4412cc2cccb22abb798`。
全流程會反覆核對原 CIFS mount 身分與 repository，拒絕本機同名資料夾、moving
latest、短 snapshot ID、不同 envelope 與不完整集合。每份 snapshot 都還原到新的
獨立目錄，再由共同 packed／SQL 工具處理。PostgreSQL 使用新的私有 Unix socket
cluster，不開 TCP，不接正式控制庫；結束時停止該 cluster，證據與失敗 scratch 保留。

來源 planner 只讀已固定的原批次与原始冷庫 metadata，沒有將本機驗收當作 NAS
驗收。此 plan 是兩個指定 release／一個指定 SQL snapshot 的語意驗收，並非所有
17,544 檔或全歷史都已重建。全量備份仍由正式排程持續推進。

## 回傳與來源接手

成功才產生 immutable
`/srv/lab203-backup/receipts/recovery-acceptance-46a06550de246c7a9cf7aad97251c524d1df2faf0b470891e03868e522b25a89.json`。
它只包含允許的固定來源身分、實際成功步驟、計時與 private evidence SHA；不包含
NAS 私人路徑、密碼、金鑰、SQL 原文或 restore 路徑。回傳資料夾 watcher 已在運作；
可用現場 Syncthing 本機 API 主動 scan 同一 return folder，憑證不輸出。

private 證據保留在本次 `/var/lib/lab203-nas-recovery-.../`。失敗不發成功收據；換新
output 接續時仍沿用相同固定 plan／NAS snapshots，保留先前 failure.private.json。
已有合法完成收據時工具沿用第一次收據，不覆寫它。

來源正式 queue 下一輪會讀取並驗此新協定；結果放入
`tools/source-status.json` 的 `independent_recovery`。一般檔案 ACK 維持其原契約，
語意驗收失敗會單獨顯示，不會清除成功檔案備份或停止其後的同步。
請本機 Codex 回覆脫敏結果：三項 state、plan identity、acceptance identity、完整
流程秒數與命令退出碼；不重做 USB 與 Windows 登出工作。

來源備份相關回歸 154 項通過、0 skipped，使用實際 Restic binary；證據為
`artifacts/operations/two-node-lab-nas-backup-20261003/nas-semantic-all-backup-tests-20261004.xml`。
包含錯誤 NAS 掛載／runtime、原 owner 正在執行、不同 source fingerprint／SQL 狀態、
缺漏工作／非零命令、回傳私密欄位與語意失敗不停止普通備份的測試。
