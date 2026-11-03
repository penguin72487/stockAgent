# lab203 儲存接收背壓交接：2026-10-29

請 lab203 的 Codex 沿既有 paired Syncthing、唯一的 relay／Restic owner 和
`stockagent-backup-receipts-lab203` 回執通道處理；不要新增第二個 writer、
修改既有封閉 envelope、覆寫歷史收據或移除尚有用途的來源。這份文件是
診斷請求，不是 NAS 備份或安全刪除的證明。

## Penguin 觀測，台北時間 11:33 左右

- `readiness.json` 顯示 lab203 ingress 可用 777,426,976,768 bytes，NAS
  可用 12,410,065,772,544 bytes；`storage-relay-audit.json` 的 admission
  要求 803,158,884,352 bytes，`receiving_backpressure=true`。這是兩份不同
  時間的快照，實際剩餘差額應由 lab203 重新量測。
- 回執列出 3,270 個 pending retry jobs，但 `retry_jobs` 明細僅列 20 筆，
  這 20 筆的理由都是 `pending_or_unknown_recovery_pin`；不能推斷其餘
  3,250 筆皆為相同原因。全域 gate 的最新狀態為 `resolved`，不代表
  個別工作均已解除。
- 此側原 backup-stream 的 11:20 快照有 83 個 envelope 尚待回執，
  共 34,657,806,071 bytes；既有全域 pending admission 為 34,359,738,368
  bytes。這會讓私人歷史新增波次等待，直到既有工作取得有效 ACK；
  不應只因 NAS 剩餘空間充足就加大接收或跳過容量安全門檻。
- Penguin 端 ingress 的 Syncthing folder 自 10:16:44 起仍顯示 scanning，
  11:37 的 need／errors 為零，local database 顯示 814,422 筆 deletion
  records；最後取得的該 folder scan progress event 是 10:44，這些
  指標都不能單獨證明死鎖或證明 deletion records 就是原因。請比對
  lab203 實際收件與 indexed queue，釐清是否妨礙後續 envelope 抵達。
  不要刪 Syncthing 索引、tombstone、來源或停止現行服務製造收斂。
- 私有 immutable 歷史目錄的 2026-10-28 固定清冊約 817,226,447,345
  bytes，其中收到機器 ACK 的為 14,467,469 bytes。這是獨立於目前
  公開資料清冊的範圍；完整原始還原及全歷史備份尚未驗證。
- 頂層 audit 仍列出 `stockagent_storage_operations_skill_not_delivered`，
  但其中 `fixed_private_rescue_handoff.skill` 又記載已逐項驗證並從
  明確封包路徑讀完 skill。兩者必須比較範圍、時間及身分，不能直接
  將其中一個值改成成功或推斷最新版本已經啟用。
- 同一份 audit 另列 `training_private_capsule_inventory_not_delivered`
  並指定 Penguin 接手，與固定 v7 救援封包已有部分 NAS 還原證據是
  不同可能範圍。請指出缺的是哪個確切 capsule／inventory 身分與
  必要欄位，並與既有交付清冊比對；不能因舊 gate 的授權欄位就推翻
  使用者之後明確允許的私有備份，也不能把這項缺件直接改成成功。

## 請執行並回報

1. 量測 ingress 的實際容量、保留量及目前 backpressure 計算，追查
   這 83 個未 ACK envelope 是否已到達；逐項核對封閉身分、接收狀態、
   原 NAS／Restic 寫入及獨立讀回。確認哪些是正在處理、排隊、失敗或
   等 Syncthing；繼續使用原 owner，不能憑傳輸進度製作 ACK。
2. 分類 3,270 個 retry 的完整原因與下一次重試時間，檢查是否有已
   結束但遺留的 pin、誤認為 unknown 的有效 pin 或重試狀態未更新。
   對確實仍使用的 pin 和 process/FD 必須保留；只透過原系統解除經
   證據核對的失效狀態，然後重跑原 dry-run／restore／retirement gates。
3. 確認目前 relay 是否正在安全回收已完成、可重新還原的中繼副本，
   以及可平行且不互相爭用的階段。回報實際釋放的 allocated bytes、
   原因與剩餘工作；不要刪 NAS 原件、未驗證 envelope 或未知工作目錄。
4. 對上述 skill 狀態矛盾核對目前 session 的已驗證封包、discovery
   與適用範圍；查看原通道 `tools/storage-relay-20261028-v1/` 及既有
   `docs/lab203_tw_rescue_history_news_2026-10-28.md`，回報實際已採用
   的 identity 與仍缺什麼，不要以讀過舊封包代替新版部署驗收。對
   capsule inventory 的 blocker 回報固定版本、缺少的證據與預期位置，
   讓 Penguin 可以按確切身分沿原 owner 補件。
5. 針對回執中 25 個 tw-public 歷史版本／114 個 unavailable objects，
   依固定 manifest SHA 在既有 NAS、Restic snapshot 與其他已登錄來源
   查找；有副本時逐物件驗 hash 後走原補件流程，沒有的範圍逐項記錄。
   另依原接手文件處理 TW 原始 runtime、prepared reference 與新聞範圍；
   不以 metadata 存在取代原始內容或完整還原證明。

請沿原回執通道回報時間、各原 owner／lock 身分、各階段件數與 bytes、
讀回結果、剩餘背壓與不可恢復缺件；不要傳密碼、金鑰或私有原文。
