# lab203 自動同步、NAS 備份與還原驗收

2026-10-04 使用者要求停止逐批人工轉貼。日常流程由固定程式與既有 systemd
負責，不需要 Codex 常駐對話。USB 金鑰保管已由使用者確認完成；不再詢問。
Windows 登出驗收已取消。

## 全自動流程已接通（2026-10-04）

lab203 已完成 v7 一次性安裝。penguin 直接驗證回傳的固定三項 NAS 語義驗收：
CFTC／TAIFEX canonical 重建通過，PostgreSQL 50 表／60 列與 logical identity
吻合；完整流程 52.331828 秒，11 個 Restic 命令皆退出 0。接收端持續更新
heartbeat，來源目前為 `receiver_polling`／`named_nas_semantic_recovery_verified`，
不再等待安裝或人工回報。lab203 回報其安裝副本修正了 PowerShell 中文輸出
解碼；固定同步套件保持原樣。來源沒有遠端讀取其 private 安裝檔，接收端
持續運作與實際 NAS 還原依配對通道上的機器收據確認。

2026-10-04 來源端接通檢查另確認：

- 正向批次與反向收據兩個 Syncthing folder 皆 idle、completion 100%、
  need bytes/items/deletes 與錯誤為 0；來源備份、Syncthing、control backup
  與必要 mount 排程／服務已啟用。
- 26 份 managed file ACK 與來源 dispatch 相符；最新程式／控制庫批次
  7 檔／12,597,504 bytes 已自動在 NAS 固定還原並逐檔驗證。
- 清單自動納入新 release，現為 17,579 檔／684,109,237,219 bytes；
  冷資料已有 9,208 檔／53,632,816,703 bytes 的 NAS 覆蓋，剩餘自動補傳。
  未送檔最大 5,707,388,416 bytes，小於實際單批 8 GiB；沒有缺件、metadata
  錯誤或超過單批上限的檔案。容量 readiness 已核對，保留 64 GiB reserve。

實際檢查收據為
`artifacts/operations/two-node-lab-nas-backup-20261003/automatic-sync-live-acceptance-20261004.json`。
這是自動流程接通及當時的部分覆蓋；全量 backlog 與其他 release 的完整 packed
重建不能據此標為完成。來源 status 在長批次執行期間可能落後反向收據，
下一輪會由既有 owner 接手更新，不能為刷新畫面另啟第二個 writer。

## 已持續運作的資料流程

penguin 的 `stockagent-backup-stream.timer` 自動盤點已允許發布的冷檔、程式
快照及既有一致性 PostgreSQL dump，產生有 READY 與完整 SHA 的封閉批次。
Syncthing 傳到 lab203 後，既有 `lab203-backup.timer`／worker 自動驗證、寫入
guarded NAS Restic repository、獨立還原固定完整 snapshot、核對所有檔案，
再由回傳 Syncthing folder 送回機器收據。

上述 v7 流程已接通，不必每批傳話。使用者後續要求整條鏈並行；來源現已改成
最多四批／32 GiB 同時在途、四工封裝與 SHA，沿用同一 owner，完成後 30 秒接續。
來源在前批等待 NAS 收據時繼續封裝、傳輸其他批次，失敗批次個別重排。
lab203 已完成固定 v10 升級，penguin 實際驗證其新 `pipeline-status.json`，
`upgrade_pending=false`，同一 owner／runtime 通過；NAS 實測選定四批、兩工
backup、兩工 restore、四工 verify，完成後 15 秒接續。新 v10 包含中斷後不完整
snapshot 的重排修正、每輪新實驗庫的 NAS 量測與 v8 相容升級，保留已完成計畫
及語意還原 post-hook，不需重新安裝或逐批轉貼。
完整實作、量測及恢復安裝參考見 [並行管線](lab203_parallel_backup_2026-10-04.md)。
下方五分鐘 post-hook／v7 安裝介面保留為既有部署與恢復紀錄。

## 新增的自動語義驗收

來源在同一 queue owner 下自動發布
`ingress/tools/recovery-requests/plan-<64 字元身分>.json`：

- 首次固定 CFTC／TAIFEX packed 重建與已固定 PostgreSQL snapshot 的獨立還原。
- 新的 `code_and_control_backup` 取得實際 NAS file ACK 後，讀取封閉批次的
  same-MVCC logical state，對尚未排入的邏輯狀態自動產生新的 PostgreSQL
  還原任務。code-only 改動且 DB 狀態相同時不重複建立 DB 驗收。

任務只包含來源／接收裝置、repository、完整 NAS snapshot、兩種 envelope hash、
精確檔數／bytes、來源 fingerprint 或 DB state hash、容量上限。沒有 shell
command、登入憑證、私有 NAS 路徑或可執行程式更新。接收端固定程式只派送
已實作的 packed／control verifier；不從同步資料夾直接執行正在變動的程式。

接收端新增**現有 `lab203-backup.service` 的一個 ExecStartPost**，沿用既有
owner.lock。既有 worker 與 timer 不替換，不建立第二個 backup owner。
一次最多執行一份計畫；失敗依 5／15／60 分鐘延遲，在之後的既有五分鐘排程
重試。缺 NAS、snapshot、容量或 runtime lock 都不能產生成功收據。失敗保留
private evidence，其他有效任務與一般檔案備份繼續。

成功的 public semantic acceptance 是不可變的，重啟不重新驗同一計畫。新程式
會在原 owner 下，保存完整清理盤點與 proof，核對無 symlink／其他 mount／
hardlink／程序引用後只回收它自己建立且已成功驗收的 private scratch。
保留 compact logs／logical-restore proof，不刪 ingress、NAS snapshots、原始來源
或失敗 scratch。WSL 另實查 registry 所識別的 ext4.vhdx 所在 Windows drive
餘量；虛擬 filesystem 上限不能代替實體餘量。

來源狀態新增 `automatic_recovery`：收據尚未出現時明確標示
`awaiting_receiver_hook`，不把送達程式當成接收端已安裝。接收端自動更新
`recovery-queue-status.json` 與 `recovery-acceptance-<plan>.json`，來源自動讀取。
`independent_recovery` 仍單獨列示固定首批 packed／control 結果。

這一版自動驗收首次兩個完整 packed releases 與後續不同 DB 邏輯狀態。分散在
多個增量 NAS snapshot 的其他 packed releases，其 recovery index 與
`assemble_restored_backup.py` 已可重建，但這一版不宣稱已對全目錄排程重建。
每個新增冷檔的 NAS 全檔案還原／SHA 驗證仍由既有 worker 自動持續進行。

## lab203 首次安裝程序（已完成，供恢復參考）

這台 penguin 只有 Syncthing 通道，無 SSH 或接收端 systemd 控制權。lab203
本機 Codex 已完成固定 v7 的一次性安裝；任務、重試與結果走現有同步通道。
下列保留首次安裝介面供恢復參考，現有正常部署不需重新安裝或逐批轉貼。

新 package：`/srv/lab203-backup/ingress/tools/continuous-backup-20261004-v7/`。
用**原本已驗收的** source verifier，按 penguin 的 publication receipt 核對
規範化 envelope、原始 handoff-manifest SHA、READY 與完整集合。
來源也把同一組固定識別自動寫入
`ingress/tools/automatic-backup-bootstrap-v7.json`；本機 Codex 可直接讀取，
不用請使用者再轉貼 hash。它是資料描述，接收端不會因為檔案到達而自行執行安裝。
不改寫 v1–v6。v7 補齊 installer 自己禁止 bytecode 寫入的保護；不需 caller
另外設 `PYTHONDONTWRITEBYTECODE`，也不會因為 bootstrap imports 破壞 READY 精確集合。

使用 [安裝工具](../scripts/install_lab203_recovery_queue.py)。本機 Codex 從現有
private config 取得 Restic binary／password file，沿用現有 NAS mount；若 v5 的
`NasTarget` private profile 尚未建立，依
[固定 NAS 驗收 runbook](lab203_nas_recovery_acceptance_2026-10-04.md) 轉成
`/etc/lab203-backup/nas-recovery-target.json`，600，不複製帳密到同步 folder。

安裝介面（私有路徑由本機 Codex 用現場既有設定填入，固定 hash 自動讀 bootstrap）：

```bash
# 先用既有 trusted verifier 驗 v7，才執行其中的安裝工具。
# source canonical scripts/runtime_env.sh，使用現場既有 run_fintech_python。
run_fintech_python scripts/install_lab203_recovery_queue.py \
  --package /srv/lab203-backup/ingress/tools/continuous-backup-20261004-v7 \
  --bootstrap /srv/lab203-backup/ingress/tools/automatic-backup-bootstrap-v7.json \
  --profile configs/data_sync/backup_receipts.json \
  --nas-configuration /etc/lab203-backup/nas-recovery-target.json \
  --password-file <現場既有-private-password-file> \
  --restic <現場既有-restic-binary> \
  --role-prefix /opt/stockagent-control-recovery-20261004-v7 \
  --mamba <既有-Miniforge-mamba> --distribution Ubuntu \
  --evidence /var/log/lab203-backup/recovery-install-v7
```

若 role 不存在，installer 使用已附 Linux explicit lock 新建隔離 Mamba PG／
psycopg role；不修改正式備份環境。驗 `nobody` 可執行新 PG binaries，保存新
runtime lock，驗 Windows 實體餘量，複製已核對的 package 到固定
`/opt/lab203-backup/recovery-code-v7`，然後 `daemon-reload` 載入單一 post hook。
installer 不停止或重啟正在處理資料的 backup service；下一輪既有 timer 接手。
驗原 service／timer／原其他 drop-in SHA 不變、timer enabled／active。

完成後在現場核對 `systemctl show lab203-backup.service -p ExecStartPost`、journal
及反向 folder 中的 queue status。來源看到兩次更新的 heartbeat、固定計畫
acceptance 及新的 code/control 自動派送驗收，才可聲明持續語義驗收已接通。
installer 本身的成功只代表安裝，不能代替 NAS 實際還原結果。

真實 DB 工程演練發現：64 字元 plan ID 與新獨立 restore 目錄會使 PostgreSQL
Unix socket 路徑超過系統上限。共用 `restore_control` 現在對長路徑新建短、
隨機且 700 的 private socket 目錄，保持不開 TCP；停止新 cluster 後只移除
已空的 socket 目錄，保存位置與必要的失敗證據。資料身分的完整 hash 維持。

prune 與接收端批次自動刪除維持關閉。來源僅回收已有 NAS 固定還原 ACK 且
來源仍完整的冷資料傳輸暫存；詳見
[持續 NAS 備份](continuous_nas_backup_2026-10-04.md)。

## 本機工程驗證（2026-10-04）

相關備份回歸 178 項通過、0 skipped，使用實際 Restic binary。涵蓋同一 owner、
資料任務不得帶 command／private fields、失敗重試公平性、固定成功收據接手、
實體容量保護、清理中斷接手及真實 Unix socket bind。

另以真實封閉程式／same-MVCC dump，建立新的本機加密 Restic repository，
跑完整備份、`check --read-data`、任務輪詢、固定還原、全部檔案 SHA、程式 ZIP
核對與新私有 PostgreSQL logical restore。50 表／60 列的 logical identity
`85b75c8b6d15de5bd00f9eb8871eb065d9dd3802206e08864c0b745eae76065f`
吻合；成功 scratch 清理、再次輪詢不重複還原均通過，完整流程 **18.765497 秒**。
這次的 CIFS／Windows admission 是工程模擬，**不代表 NAS 或 lab203 已驗收**。

第一次測試目錄設定遭隔離檢查拒絕；第二次真實 PostgreSQL 啟動發現長 socket
路徑問題；兩次失敗證據保留，第三次修正後通過。证據在
`artifacts/operations/two-node-lab-nas-backup-20261003/`：
`automatic-recovery-all-backup-final-20261004.xml`、
`automatic-handoff-bootstrap-final-20261004.xml`、
`automatic-control-local-acceptance-20261004.json` 與三次 managed run 日誌。
bootstrap 另以一般 Python `-I script --help`（不加 `-B`）實際啟動新 installer，
啟動後再次核對全部 envelope／READY／精確集合，確認沒有 `__pycache__`；證據為
`automatic-handoff-bytecode-accepted-20261004.xml`。
