# lab203 每分鐘運行與排程補件

## 2026-11-02 17:27：先恢復原 NAS guard 與接收許可

Penguin 已完成容量不足時的快速等待、原進度時鐘保全與網頁分鐘刷新。
目前阻塞不是 Syncthing 在線狀態：lab203 容量回執每分鐘到達，但原私有
接收許可的量測時間仍為 **2026-11-02 12:49:56.719108 +0800**，依原
900 秒有效期於 **13:04:56.719108** 到期。NAS 容量的最後有效量測為
**13:22:55.302303**；17:24 的新嘗試失敗為 `SnapshotError`。這些時鐘
不能用本機 observer 的新時間延長；現有固定 ACK 仍按原契約保留。

請 lab203 Codex 優先沿原 owner 處理：

1. 查明 NAS guard 的真實錯誤及持鎖程序。在既有工作結束的安全邊界，
   重做原 NAS mount、實體容量、repository identity 與已接受 runtime
   檢查，再由原 producer 寫入接收許可及其真實量測時間。若 repository
   正被 backup／restore 使用，安排單項工作的有界空檔重試，不建立
   「所有佇列清空才允許檢查」的全域障礙；回報原 next retry 與原因。
2. Restic 的 `cat config` 可能申請 repository lock；必須核對目前部署
   命令及原錯誤，不能假定唯讀命令無鎖。不要刪 lock／pin、強停正在
   執行的備份、加 `--no-lock` 或只刷新檔案時間來通過 guard。合成測試
   不能替代實際 NAS／repository 讀回。
3. 許可與容量兩道 gate 分開回報。17:26 Penguin D 槽僅約 18.80 GiB，
   也低於現行傳輸門檻；接收端恢復不代表來源端已具備新一波配置空間。
   原 owner 持續接收 ACK、驗證及回收已接受傳輸副本，不改門檻或記帳。
4. 沿原反向通道交回實際修復版本、原失敗／恢復結果與至少兩輪分鐘
   取樣；保留各工作進度時鐘。下面的 CPU／RAM、完整遠端排程與階段
   分母補件仍需要，不得以新 heartbeat 冒充這些計數或 NAS ACK。

此補件需要 lab203 原 owner 的執行結果；Penguin 沒有該節點管理連線，
不在來源端製造接收許可，也不宣稱遠端已修復。此文件不是 ACK。

## 持續觀測與排程契約

Penguin 已上線每分鐘觀測、28 組本機排程／常駐 owner、流程接續條件、
失敗與 kernel lock 等待提示。lab203 的配對容量／接收回執每分鐘持續到達，
但尚無 CPU／RAM、完整遠端排程及各驗證階段分母；目前網站明確顯示未知。
這份文件不是 ACK，也不是新 receiver 的部署或授權替代物。

請 lab203 Codex 沿原 owner 完成以下補件；不需要 Penguin 管理連線：

1. 使用既有 storage observer／backup／relay 的原服務，只讀現有計數與
   systemd 排程，每分鐘原子更新小型狀態。不新增 payload 掃描、全檔 hash、
   備份 writer 或第二個總控排程器，不因網站顯示而重啟備份。
2. 保留 `pipeline-status.json`、`storage-relay-audit.json` 與其簽章／配對 ABI。
   `pipeline-status` v1 有嚴格欄位白名單，不能直接加欄位。沿既有反向通道
   提供獨立、有界的 `pipeline-progress-v1.json` 固定樣本；提案契約
   `stockagent.nas_relay_progress.v1`，與原 producer／receiver／repository
   配對身分及當次樣本 identity 綁定，所有身分與完整性方式沿原系統。
3. 回報實際已安裝的 backup、relay、Restic、儲存監控／回收 owner：
   是否啟用、是否執行、上次 outcome／退出碼、下次檢查與原 next retry。
   下次檢查不是開始時間；沒有原證據的值填 null，不照抄 Penguin 的排程。
4. 每個既有 job 保留原 envelope／dispatch、attempt、phase 身分，回報
   目前階段、完成／總 bytes、完成／總 items、原阻擋代碼與下一個相依條件。
   分開壓縮傳輸、NAS 寫入、獨立還原與完整 SHA；不可混加百分比。
5. 分開 `observed_at_utc`、`progress_at_utc`、`phase_started_at_utc` 與
   `next_retry_at_utc`。重讀舊計數不是新進度，固定 ACK 完成日保持不變。
   若已蒐集 CPU／RAM，只交付匿名數值與來源時鐘；不交付程序參數、帳密、
   新聞原文或檔案內容。遠端觀測失敗應保留上次值和原時鐘，另記本次失敗。
6. 鎖等待只依真正 kernel／原 owner 證據；不可刪 lock／pin／retry，
   不能因檔案名稱還在就宣稱持鎖，也不能把持鎖直接認定為死鎖。
7. 沿原回執通道交回一份固定 identity／SHA 的樣本、欄位與單位說明，
   以及實際安裝版本／至少兩輪每分鐘更新證據。Penguin 再依固定樣本加入
   獨立檔配對解析與估時驗收；目前 reader 不會自動採信未實作的新格式。

原傳輸、NAS 獨立讀回、paired ACK、容量／pin／回收條件照常執行，
這項補件不阻塞它們。原始來源、NAS archive 與原失敗證據均保留。
原分階段提案與現行監控驗收見
[自動接續 runbook](storage_automatic_continuation_2026-10-31.md)。
