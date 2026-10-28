# WSL 根目錄備份與 D 小檔封存優化（2026-10-29）

## 根目錄備份

六個明確列名的備份目錄及 `/root/StockAgent` 空目錄樹，檢查進程引用、搬移前後身分並逐檔 SHA 比對，實體資料已原子搬到 `artifacts/operations/root-backup-consolidation-20261028/data/`，共 13,753 檔。原來 `/root` 的七個名稱現在是相容連結，沒有刪除備份內容；相同 ext4 內更換位置不釋放空間。單次遍歷六個備份搬移前的配置量約 6.56 GiB，硬連結不重複計算。逐目錄 plan、intent 和 receipt 均在 `artifacts/operations/root-backup-consolidation-20261028/`，處理程式為 `scripts/relocate_root_backup_alias.py`。

| 原名稱 | 檔案數 |
| --- | ---: |
| `StockAgent`（只有空目錄） | 0 |
| `stockagent-sync-backups` | 1 |
| `stockAgent-git-recovery-20261015.j0E2PL` | 4,077 |
| `stockAgent-github-sync-20261015.LTXWb1` | 4,319 |
| `stockAgent-two-node-sync-20261015.Luk1nu` | 4,187 |
| `stockAgent.git-recovery-backup-20260620-restore` | 1,149 |
| `syncthing-merge-backups` | 20 |

相容路徑的 Git 物件讀取及原設定 remote 查詢均實測成功。這只驗證上述目錄的搬移，**不表示** 2026-10-15 以後的 19 個受事故影響工作樹改動或忽略檔已找回；後續搜尋及 lab203 交接仍以 [事故紀錄](wsl_worktree_recovery_incident_2026-10-28.md) 為準。

`/root/.shioaji` 當時有當沖等進程持有鎖；`/root/finlab_db` 有當沖相關排程與現行 FinLab 執行者。它們仍是根目錄下的真實目錄，尚未完成原路徑、使用者與無中斷切換驗證，不能稱根目錄整理已完成。兩個小型 `stockAgent-test-*` 根目錄是事故調查建立的測試證據，含相對連結，暫時保留。OpenBB 和 Syncthing 執行環境也不因名稱相似而當作無用資料刪除。

另清理本次開始前已完成的 `sourceplan-focused` pytest 暫存 38,662,144 bytes、本次另外完成的十批 pytest 暫存 39,956,480 bytes、後續十二批 pytest 暫存 12,734,464 bytes，以及本次受控效能試驗的五個重複 ZIP 56,893,440 bytes；依既有 owner 以固定清冊、進程引用及身分核對處理，試驗 ZIP 另逐檔完整 SHA 與 CRC。無正式資料、D 物件或 NAS 快照被刪除。後三類是本次測試自己產生的暫存，不能算成既有資料的淨回收。`/var/tmp/stockagent-sourceplan-tests-20261028` 是中止測試留下的約 0.59 GiB，仍待單獨辨別。

## 小檔來源讀取

舊程式逐筆從 D／9p 讀取來源後寫入 ZIP。唯讀測試選同一 D 暫存封存的 64 個小檔、共 390,326 bytes；1、4、8、8、4、1 執行緒的完整 SHA 耗時依序為 3.334、0.526、0.408、0.405、0.565、3.305 秒，所有 SHA 一致。另一種 16 檔 Windows native batch 在同一小檔族群比逐檔 Linux 讀更慢，未採用。

已將有界平行來源預讀接入原本的 packed publisher，**只有手動處理的已註冊 D benchmark／offline stage** 預設選 8 個讀取 worker；其他呼叫維持原本順序讀取。每檔仍以 no-follow descriptor、前後 stat 和 SHA 檢查，寫入後再檢查一次 source path；檔案順序、ZIP 位元組、原有 callback 次序、source guard、D 完整還原與刪除 gates 均保留。較大的檔案和 mutable control 維持原本串流，預讀最多 8 個不超過 8 MiB 的成員，不限制其他服務資源。

第二組以 D 來源 128 檔、約 10.92 MiB，輸出至 WSL ext4 的完整 ZIP A/B/B/A 實測：單工 9.245、30.682 秒，8 worker 6.971、8.973 秒，4 份 ZIP SHA 完全相同；獨立重複的 8-worker probe 為 3.371 秒並通過 ZIP CRC。時間受併發工作波動明顯，這不是 D-to-D 完整發布、獨立還原、NAS 或整輪回收時間。試驗最初把合法的「ZIP 回寫跨緩衝區所以需完整重算 SHA」誤當錯誤；補測確認 producer digest 為 `null`，原有 publisher 會完整重算，而非使用未完成的摘要。固定結果與清理收據見 `artifacts/operations/storage-single-authority-20261025/prefetch-benchmark-scratch-20261029.*.json`。

`test/test_packed_prefetch.py` 驗證封包逐 byte 相同、成員及回呼順序、獨立完整重建、實際讀取重疊、讀後變更拒絕、回寫重算與非法 worker 數。與既有 packing checkpoint、legacy publication、owner、來源防護及 root 原子搬移測試一併執行，具體範圍及結果以當次測試紀錄為準；沒有聲稱所有服務或所有歷史來源已通過整機驗收。

後續新啟動的回收子程序可以載入新程式，現行第 9 輪從舊程式映像繼續，不中斷正在進行的發布。未來輪次另外記錄 `publication-progress.json` 的階段、檔數及位元組；此記錄明確標記 `acceptance: false`，既不等於完整還原，也不能直接換算整輪 ETA。

## 清冊鎖、逾時與私人備份交接

正在執行的清冊工作與版本登錄共用 `catalog-owner.lock`。先前成功的整次登錄記錄為 6,531.66 秒，但另一條 workflow 的 activity 原逾時為 20 分鐘，鎖競爭時反覆觸發重試。已在既有 owner 增加明確的鎖佔用等待狀態；只把此鎖的競爭視為等待，不掩蓋其他 I/O 錯誤。透過 Temporal 的版本化分支，為新排程的清冊與來源 activity 調整逾時至 24 小時，歷史重播維持原選項；已經排出的 activity 仍使用原逾時。heartbeat 和工作進程存在都不能單獨證明處理進度。

清冊 metadata 仍對每份物件檢查 SHA、長度及 UTF-8，並保留順序；符合既有 D 冷區條件時使用 4 路有界讀取。對實際 D 冷區的 64 份 metadata、共 2,223,223 bytes 做 1／4／4／1 工作者 A/B/B/A：1 工作者 1.2365、0.9562 秒；4 工作者 0.5719、0.6022 秒，全部逐檔結果一致。這僅是 metadata 階段的唯讀比較，並非整次發布／還原的速度或硬體極限。

為避免新增來源破壞尚未完成的固定版本，來源清冊只新增項目時會保留既有已驗證證明、待處理清單與重試狀態，並先完成既有已發布版本的還原；更動或移除既有來源仍沿用原本規則。七個根目錄備份的新私人來源已做唯讀預檢：13,753 個檔案、7,002,610,337 logical bytes，七份搬移收據均可核對，沒有內部符號連結；本次掃描沒有發現 SQLite header。候選來源為 `host-private-root-backups-20261028`，標為非公開，不會把私人內容公開到資料目錄。

已排入既有 `stockagent-agent` 監督的自然交接 `wsl-private-root-enroll-and-cata-20261028T162527-af48c963`：等待清冊、來源與版本鎖全部釋放後，重新核對來源、收據、程式及清冊身分，才交接原 lakehouse worker 並新增上述來源。交接限時 24 小時；逾時或身分變動會安全失敗，原資料保留。截至 2026-10-29 00:27 台北時間它仍在等待，原 worker 尚未載入新版，清冊尚未登錄該來源。即使交接成功，新來源仍要排入既有私人來源佇列，經 D 的固定版本完整重建，並另取 NAS 接收及還原 ACK；事故恢復樹不在本次新增範圍內。

已在鎖定的 control runtime 重播三條實際 workflow 歷史（1,285、544、116 個事件；不執行 activity），並執行 metadata、鎖、來源轉換、私人來源端到端發布與重建、實際鎖交接等選定測試。這證明所測試的程式契約，不能取代線上交接結果、正式冷區重建或 NAS 回執。

2026-10-29 00:28:08 至 00:30:11，獨立運作的 `tw-public` 固定版本 `verify-cold-objects` 由 6,558,754,897 增至 6,819,716,010／35,976,707,808 bytes，平均約 2.12 MB/s。若這個短時速度持續，僅此階段約在 04:20 結束；其他負載、檔案大小與停頓可使結果大幅改變，完整重建、回收及 NAS 仍未包含。兩次觀察與相同 attempt 身分固定在 `artifacts/operations/storage-single-authority-20261025/tw-public-verify-progress-two-observations-20261029.json`，均非 acceptance 證明。

## 環境與未解項目

執行期間觀察到共享 `fintech` Conda 環境在 2026-10-28 23:53 被其他程序執行 `mamba update --all -y`，Python 由 3.12.15 變為 3.12.16；已執行的第 9 輪 `/proc/.../exe` 顯示舊映像 `(deleted)`。新 Python 下 Polars、DuckDB、PyArrow、NumPy 可載入，且新增與既有選定測試通過，但這不證明所有使用此環境的現行或下一次啟動的服務都已驗收。沒有為此停止當沖或中途重啟儲存工作。

第 9 輪仍持有既有共同發布鎖；公開與私人 NAS 所有資料都已同步的證據仍不存在。現行私人 NAS 備份僅涵蓋已保留的 private-history 清冊，不代表上述新路徑或事故恢復樹已送達 NAS。保留本機正本，並等待上述交接與各自的實際驗收。另需追查歷史 25 個版本的 114 個來源物件。下一輪 ETA 必須依實際各階段取樣，不以 ZIP 速度代替完成時間。
