# 備份 owner 公平交棒與固定控制包派工（2026-10-29）

## 問題和邊界

Penguin 的正式備份 owner 在同一把鎖下，逐一驗收並退休已經有配對 NAS ACK 的可重建傳輸副本。2026-10-29 晚間實測它持續處理多批小檔案與 D 槽 metadata；三個約 1.17 MB 的固定控制包儘管已送達 lab203，補 NAS 派工仍須等待整串工作結束。這是長時間持鎖造成的飢餓，沒有證據顯示相依鎖循環或死鎖。

`BackupStream.retire_transport_cache` 現在只在**完整單批退休交易已結束且 `/proc/locks` 顯示原 owner 有 kernel 等待者**時，持久化日誌並交出原鎖。重新取得後，以原 owner 重讀和恢復日誌，再重新檢查配對 Syncthing 狀態；若同步狀態改變，延後後續退休和刪除通知。無等待者時不增加整批重新讀取。缺少可讀的 `/proc/locks` 時採用交棒的保守退化。交易中間不釋放鎖、不省略原來源／ACK／程序參照檢查，也不新增備份 owner。

`backup_transport_cache.retire` 的各次日誌持久化改用原 `BackupStream._save_ledger`。固定 1,957 筆 delivery 的來源快照，交錯執行 A/B/B/A，A 既有 JSON 寫入與 fsync 為 4.150、4.475 秒，B 精簡串流為 1.341、2.544 秒；四次各自的**非計時**讀回均與同一記憶體輸入完全相同。輸出分別為 82,661,747 與 59,231,769 bytes。固定快照 SHA、實測 CPU／wall、讀回結果與範圍在本機 `artifacts/operations/storage-single-authority-20261025/ledger-serialization-abba-20261029.private.json`。這是單機、單日誌序列化實驗，不是整輪退休或 NAS 吞吐量的驗收。

## 三個固定控制包

`scripts/backup_frozen_handoff.py` 驗證固定 envelope、完整成員 SHA、來源 manifest 和配對身分，在原 owner 下執行現有容量與保留空間檢查，透過原 `publish_closed` 發布同一固定封套。原 `tools/` 控制包保留；新派工不是 NAS ACK；不設定來源刪除或代表全歷史備份。中斷保留 stage 和原 ledger，下一次使用原 `recover_publications` 接續。

2026-10-29 23:11 已以 `lab203-paired-control-backup-202-20261029T151145-131197a0` 在既有 agent task 下排隊，等待原 owner，範圍：

- `storage-source-evidence-20261029-v1`：`1d1fb24558388997640f61fc459584bcf039ec9768e81be720d3cf211f164243`
- `storage-capsule-binding-20261029-v1`：`7c5f0ab836251b1f88c24620bcbf3db941ee4598e5de4d1d127c83aa96ad5cca`
- `storage-retired-scan-20261029-v1`：`0f61755e8bc37e3f27f02a63f4300e494fdeba90ed49cefef3c4c5a568368b29`

當時執行中的原 owner 採舊版程式，未強制中斷；新的 owner 進程才會載入交棒與日誌改善。是否真的發布、對端收到、成功備份和獨立 NAS 還原，應分別查 agent run、原 ledger、receiver 個別固定派工與配對 ACK，不能以這份文件替代驗收。

## 驗證

2026-10-29 執行 backup stream、handoff、relay、transport cache、private cold 與新 handoff backup 的相關測試：`211 passed, 12 skipped`。其中包含源資料損壞、容量不足、中斷後恢復、內核等待者交棒、並行 owner 的日誌變更保留、交棒後同步狀態變化，以及退休失敗時不可作為完成交易交棒。測試不是遠端正式 ACK，也不是全量資料完整性證明。
