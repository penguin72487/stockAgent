# 2026-10-06 傳送與 NAS 驗收並行

使用者指定：penguin 持續把所有已登錄資料送出，lab203 同時驗收，失敗才重傳。
來源封閉、傳送、NAS 複寫／還原、驗收與暫存回收是獨立階段；NAS ACK 決定
完成與可回收，不再占住來源的四批發布窗口。

## 生效行為

- `stockagent-source-replication-v1` 的活動沿原 owner／journal 持續發布，
  `decouple_nas_acceptance: true` 取消 pending ACK 的批數／bytes 門檻。
  256 GiB 保留暫存與 D 的 64 GiB free reserve 是容量保護；每批仍以 2 GiB
  為目標、最多 1,024 成員，單物件上限 8 GiB。已發布 keys 不重複列入正常波次。
- 原 source 活動也使用同一 `register`／catalog lock 更新版本。某個 catalog
  export 等 NAS 時，新 canonical releases 仍能進入 raw 傳送清單。
  新清冊更新失敗會保存 `catalog-refresh-errors.json`／
  `source-catalog-refresh.json`，並以 600 秒間隔重試；最後已提交的 registry
  繼續提供固定來源清單，錯誤不扣住已登錄資料。新增版本尚未通過時不宣稱納入。
  source 活動的成功更新也至少隔 600 秒才再次盤點，已登錄清單可以連續發布，
  不為每個波次重做 HDD 全清冊。catalog workflow 的既有更新入口保留。
- 原 Restic source stream 的 `publication_requires_receiver_ready: false`
  核對已配對 receiver/repository、收據格式／身分及 owner/runtime，但不拿
  NAS mount、回執年齡、NAS 剩餘空間或未驗收四批作為本機發布條件。
  receiver 本機每個 NAS 工作仍必須通過實際 mount／容量／runtime guards。
  首次未知配對、偽造或異機收據仍拒絕；本機容量與封閉 SHA 仍要通過。
  Restic source stream 的保留容量也設為 256 GiB；其實體盤點包含同一 ingress
  的 lakehouse／staging 與舊批次，避免解除 ACK 門檻後沿用舊 1 TiB 容量。
- paired `relay-status.json` 的 `deferred`／`rejected`／`retry_wait` 是失敗
  批次的重傳通知。來源只接受已登錄 identity，核對原 manifest／READY 與
  exact file keys，從 authority 按原 SHA／bytes 重送。新 attempt 身分包含
  parent、attempt 與 request 身分；保留原失敗目錄和 NAS 內容，不覆寫它。
  同一失敗 root 同時僅有一個重傳在途；只有接收端再回報該 attempt 失敗才
  重試下一次，間隔至少 600 秒、最多 3 次。其他資料繼續前進。檔案未到齊的
  `FileNotFoundError`，或暫時 timeout／connection／capacity 等待，不觸發來源
  重複傳送；由傳輸與既有接收端重試接手。
- 成功重傳只按其真正 ACK 增加去重後覆蓋；不替原失敗 attempt 虛構 ACK。
  原失敗暫存保留。傳統 Restic 接收端沿固定 v10 的逐階段重排／重試繼續；
  不因一個失敗重建或重送整庫。
- raw ACK admission 沿 canonical `acceptance(..., enrolled_source=row)`，
  共用 bounded manifest／READY parser，核對已登錄 authority 的 exact file
  keys／SHA／bytes 與 paired NAS 完整還原收據。這一步確認固定 archive，
  不再阻塞 sender 逐檔重讀本機暫存；local exact set／full SHA、primary 原值
  重建與 process gates 仍在 transport GC 回收前執行。一般／catalog acceptance
  與 GC 的原 `acceptance` 預設完整驗證不變。後來損壞的暫存不會否定已獨立還原
  的固定 NAS archive，但會被完整回收驗證拒絕，不得刪除。
- catalog acceptance 先持久化，傳輸回收由原 `stockagent-lake-transport-gc`
  處理；raw／catalog 暫存輪流選一批，不再把 HDD 清理放在 catalog 完成路徑。

所有資料指來源 catalog 授權的 canonical 清冊與原備份範圍；不擴張
`publish: false`、私有金鑰或授權來源的可分享範圍。

## 部署與驗證

資料機使用原 Miniforge/Mamba role；沒有變更 PostgreSQL／Temporal server、
Syncthing、lab203 安裝或 GPU／下單服務。最初 policy 在 publication owner
邊界移交；量測後的兩次版本沿既有 `verify_temporal_recovery.py` 實測 active worker
中斷、systemd 接手與 interrupted activity retry，分別於 36.56／83.12 秒通過
兩條工作鏈的 live query／history-prefix／retry 檢查。最後一次包含已量測的
native copy 門檻，沿同一 503 檔 durable intent 接手。未封閉 staging 保留，
不刪除 source／NAS；physical host reboot 未測試。NAS 一側使用既有固定
接收器即可識別新封閉批次，不執行同步來的新程式。

實際部署收據：
[`worker-copy-measured-deployment.json`](../artifacts/operations/nas-send-verify-decoupling-20261006/worker-copy-measured-deployment.json)。
生效 private policy 來自 `configs/data_sync/lake_source_replication.json`；設定
原文與可恢復副本留在本機私人目錄，公開收據僅包含 SHA 與公開容量政策。
部署時 lab203 回報 ingress free 約 536.25 GB，NAS free 約 15 TB，所選 256 GiB
spool 小於 ingress free 減 64 GiB reserve。這是容量時間點，不是永遠可用保證。
實際中斷接手見
[`measured-worker-recovery.json`](../artifacts/operations/nas-send-verify-decoupling-20261006/measured-worker-recovery.json)
與 [`measured-copy-worker-recovery.json`](../artifacts/operations/nas-send-verify-decoupling-20261006/measured-copy-worker-recovery.json)。

分組驗證：168 項 source/control、真 rclone、Windows I/O、offhost backup 與
transport cache 測試，以及 47 項 status／boot-gate 測試通過；兩項因小 control
role 未配置 Restic CLI 而跳過的測試，改用既有獨立 backup role 的真實 Restic
重跑，兩項均通過，共 217 項測試。Restic 工程測試使用本機臨時 repository，
不當作實際 NAS 的新驗收。兩條實際持久歷史共 2,852／522 events replay
通過，沒有 reset workflow。首次測試 fixture 的 ledger 路徑錯誤及小角色缺少
網站測試依賴的失敗收據保留；來源測試修正後重跑，網站／boot 測試使用原 runtime。
這些工程測試不代表全歷史 NAS 驗收完成。

正式流程在原四批尚未驗收時已發布第五批，自動重傳失敗的 21 個檔案、
2,128,800,733 bytes。新 attempt 的 manifest／READY 身分及原 file set／SHA／
bytes 全部對應；原失敗目錄保留。13:40 UTC 觀測 ingress 與 reverse receipts
均 idle，peer need bytes／items 為 0，新 attempt 的 NAS ACK 尚未收到。
此為實際並行傳輸與單批重傳證據，不宣稱新 attempt 已通過 NAS 還原。
見 [`source-transmission-acceptance.json`](../artifacts/operations/nas-send-verify-decoupling-20261006/source-transmission-acceptance.json)。

最後版本接手的是另一個正常新資料波次：503 檔、2,145,224,553 bytes，
`redelivery: null`；此前五批仍待回執時，來源依同一固定 journal 繼續準備。
14:34 UTC 部署收據記錄正在複製，沒有把 staging 的成員數當作送達或 NAS 完成。
同時 paired Syncthing READY availability 已顯示前述重傳批次由 lab203 持有，
見 [`redelivery-peer-ready-availability.json`](../artifacts/operations/nas-send-verify-decoupling-20261006/redelivery-peer-ready-availability.json)。
最新去重觀測為 800.906／1,038.236 GB（77.14%）；未完成約 237.329 GB／
2,509 檔。這是當時已登錄清冊的覆蓋，仍保持 `all_history_backup_verified: false`。
見 [`latest-automation-status.json`](../artifacts/operations/nas-send-verify-decoupling-20261006/latest-automation-status.json)。
最後本機觀測確認 Temporal、lifecycle worker、Syncthing、來源 timer 與 transport
GC timer 均 active／enabled，worker 的 loaded code 與磁碟一致；503 檔波次仍在
完整封閉校驗，不宣稱已送達。固定來源與 NAS 尚待驗收的資料由原 owner 繼續
處理，不需要另一個 agent 或人工逐批轉貼。
見 [`final-live-owner-observation.json`](../artifacts/operations/nas-send-verify-decoupling-20261006/final-live-owner-observation.json)。

## Penguin NTFS 完整雜湊與複製路徑實測

對三個現存 immutable 9–10 MB 成員交錯量測，WSL `file_digest` 為
4.634／3.415／5.523 秒；guarded Windows native 為 2.255／2.629／2.358 秒。
再以同一批 16 個真實成員（157,173,436 bytes）比較完整雜湊與來源穩定檢查，
WSL 70.176 秒、native batch 4.816 秒，約 14.57 倍。全部 SHA 與原固定清單一致。
沒有清 OS cache，單批比較不代表統計顯著，也不代表整條 NAS 流程加速 14.57 倍。
見 [`ntfs-hash-path-comparison.json`](../artifacts/operations/nas-send-verify-decoupling-20261006/ntfs-hash-path-comparison.json)
與 [`ntfs-batched-hash-comparison.json`](../artifacts/operations/nas-send-verify-decoupling-20261006/ntfs-batched-hash-comparison.json)。

同批兩個真實成員共 20,037,364 bytes 的複製、完整 SHA、flush、來源穩定檢查
及獨立目的檔完整 SHA，WSL 為 6.956 秒、native batch 為 4.294 秒，約 1.62 倍。
來源原檔不變；測試使用自有未發布 staging，沒有寫入 NAS。沒有清 OS cache，
這是有界實測，不外推為全量同步的固定倍數。
見 [`ntfs-copy-path-comparison.json`](../artifacts/operations/nas-send-verify-decoupling-20261006/ntfs-copy-path-comparison.json)。

本次採用 Penguin NTFS 的 8 MiB native hash／copy 門檻與最多 16 成員的既有 adapter，
primary recovery 也按同一批次取得全檔 SHA，仍核對每檔原值及前後 signature。
guarded D immutable lake 僅增加 read-only hash 路徑，拒絕 overlay 或寫入模式；
Linux／其他路徑維持原 file I/O。沒有降低校驗內容或變更現行 NTFS／8 KiB mount。
新的硬體與工作負載需重新量測，這些秒數不作永久保證。

## 查詢與接手

```bash
bash scripts/run_data_cache.sh automation-status --human --live
bash scripts/run_lakehouse_control.sh status
```

新的 source status 有 `publication_waits_for_nas_ack: false`、
`retained_transport_bytes`、`redelivery_attempts`、個別 `receipt_errors`。
NAS待驗收與來源仍在發送可以同時成立，不能只看 state 字串判定串行等待。
固定清冊的全檔機器驗收仍由原 `audit_nas_sync.py` observer 追蹤；送達不等於
NAS 還原完成。不刪 D authority、不啟用 NAS prune／archive deletion。

後續 local policy／worker 升級沿原入口，output 必須全新：

```bash
bash scripts/run_lakehouse_control.sh python scripts/rollout_lakehouse_source_policy.py \
  --output artifacts/operations/EXACT_NEW_POLICY_ROLLOUT.json
```

這是本機變更操作，需在當次已授權維運範圍使用；進度查詢不呼叫它。
工具取得原 publication locks、保存私有 rollback、核對實際 loaded code 及
workflow chain，不另起來源 writer，不重啟 authority PG／Temporal server。
