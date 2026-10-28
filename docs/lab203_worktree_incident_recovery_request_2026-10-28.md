# lab203／NAS：查找工作樹事故前的較新副本

請 lab203 的 Codex 沿用當地既有的 NAS／Restic 稽核與回執通道，以唯讀方式查找以下 Penguin 工作樹是否有 **2026-10-15 至 2026-10-28 約 23:26–23:28（Asia/Taipei）事故前**的實際檔案副本。依檔案實際來源及固定時間判斷是否比現存快照新；事故後建立、但包含事故前來源的封存也應檢查。Penguin 曾誤刪 19 個工作樹，現只恢復到 2026-10-15 固定快照；不能因 Git commit 或工作樹名稱存在就假設未提交修改及忽略檔也在。

```text
/root/stockAgent-optimized-ablations-20261013
/root/stockAgent-architecture-regression-20261003
/root/stockAgent-frontier-kernels-20261013
/root/stockAgent-frontier-training-20261014
/root/stockAgent-futures-v8-20260927
/root/stockAgent-gpu-structural-20261014
/root/stockAgent-ledger-graph-20261015
/root/stockAgent-modernization-release-20261003
/root/stockAgent-readiness-final-release-20261003
/root/stockAgent-readiness-regression-20261003
/root/stockAgent-readiness-release-20261003
/root/stockAgent-readiness-verified-20261003
/root/stockAgent-remote-build-measurement-20261003
/root/stockAgent-remote-build-socket-20261003
/root/stockAgent-target-architecture-20261003
/root/stockAgent-target-architecture-final-20261003
/root/stockAgent-target-architecture-final2-20261003
/root/stockAgent-target-architecture-final3-20261003
/root/stockAgent-window-rms-v3-20260921
```

請先查既有各類備份的 catalog、snapshot 時間與原始路徑；別把 `code/working-tree.zip` 的主儲存庫備份視為上述所有其他工作樹的備份。對找到的較新候選，保留 snapshot identity、檔案路徑、大小、SHA-256、模式及 symlink target 等可驗證證據。若內容在 NAS，沿用當地容量與保護規則，先恢復到獨立私有位置並和固定清冊對帳，不覆寫 Penguin 的 10 月 15 日復原快照，也不刪除任何來源或備份。透過既有機器回執通道回傳「逐工作樹是否找到、時間、snapshot identity、可復原範圍與失敗原因」；不要傳憑證或私人設定內容。

Penguin 端事故與既有證據見 `docs/wsl_worktree_recovery_incident_2026-10-28.md`，19 個原始路徑和驗證結果也在 `artifacts/operations/storage-single-authority-20261025/worktree-incident-20261028/restore-snapshot-20261015.json`。這是一次性查找請求，無須重新設置或逐批轉貼既有備份管線。
