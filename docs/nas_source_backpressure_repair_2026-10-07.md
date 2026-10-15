# Penguin 來源容量與 NAS 補齊修正（2026-10-07）

**原缺口已補齊。2026-10-07 21:00:57（Asia/Taipei），原缺少的 1,519 檔、
19,305,483,099 bytes 全部取得 NAS 機器還原驗收；此範圍待驗收檔數及 bytes
均為零。** 最終部署收據於 21:09:20 核對，177 項不同測試通過，沒有失敗或跳過。

本次修正來源端的回收公平性、共用暫存容量、並行容量盤點、catalog NAS
收據接納，以及不必要的重複補傳。沿用既有 Temporal、systemd、Syncthing
和 lab203 驗收 owner。正式流程繼續處理後續增量及 archive 轉換；固定清冊
驗收完成不要求這些持續前進的工作也同時清空。

## 根因與固定驗收範圍

19:02 的來源清冊有 35,574 檔、1,096,401,901,376 bytes；相對路徑與 SHA
去重後，已有 34,055 檔、1,077,096,418,277 bytes 取得機器還原驗收。
尚欠 **1,519 檔、19,305,483,099 bytes**，其中 1,442 檔是 packed 物件、
77 檔是冷庫 metadata；當時全部尚未發布至 lake transport。

| 來源阻礙 | 現場證據 | 影響 |
| --- | --- | --- |
| lake 暫存額度用盡 | 上限 274,877,906,944 bytes，已占 274,877,906,715 bytes，只剩 229 bytes | 連最小下一個物件都放不下，但舊狀態錯報 `monitoring_increments` |
| 回收反覆選中同一失敗批次 | 首批 `d60b15fc…` 有 NAS ACK，仍缺 31 個 authority 原檔，無已準備的回收 journal | 完整讀取 transport 後原檔檢查失敗，下次又選相同批次，其他可回收批次飢餓 |
| 備份與 lake 共用實體 ingress | 全目錄 287,722,938,314 bytes，其中失敗／未完成 staging 11,474,136,504 bytes | 超過 Restic 的 256 GiB transport 額度，輔助 code／SQL 擷取回報 `SnapshotError` |
| 並行 GC 與容量 scan 競態 | GC rename／unlink 後，完整 scan 遇到 `FileNotFoundError` | 舊流程誤報 receiver readiness，或在最後一次盤點直接失敗 |
| catalog ACK 活動反覆逾時 | 正式 snapshot 126 的相同 ACK 活動多次 `StartToClose timeout`；期限兩分鐘 | 收到 NAS 完整還原證明後，仍逐次重讀本機所有 payload，延後原 workflow 前進 |
| 傳輸未齊被誤判為補傳 | receiver 的 `ValueError` 同時明確標示 `transport_action=wait_for_Syncthing`，舊來源只依例外類型判斷 | 仍在正常傳輸的原批次被另外複製 206 檔／2,138,921,117 bytes，占用 I/O、頻寬與暫存 |

另外一批 `cb57be74…` 也缺原檔；兩批合計 **35 個不同缺檔**，均精確吻合
之前已明確丟棄的測試物件收據。這些物件沒有重新建立，也沒有把缺原檔的
批次冒充可回收。另有 **141 批、270,594,035,580 payload bytes** 的原檔
存在，可繼續走原有完整回收檢查；存在檢查本身不代表 SHA／恢復驗收。

固定缺口保存在
[`baseline-fixed-remaining.json`](../artifacts/operations/nas-source-snapshot-repair-20261007/baseline-fixed-remaining.json)。
19:11 的持續觀測另外固定一份完整 canonical 清冊，包含之後新增的 8 檔、
251,355 bytes；兩份清冊的身分不同，不能用持續變動的最新分母宣稱原缺口補齊。

## 已部署修正

- `retire_lake_transport_cache.py` 保留每次最多一批的完整回收，新增私有耐久
  `transport-gc-schedule.json`。尚未嘗試或最久未嘗試的批次先走；失敗批次
  個別退避 600／1,200／2,400／3,600 秒，不再壟斷所有後續回收。
  昂貴檢查前先保存嘗試身分；程序中斷後下一次也能讓其他批次前進。
  容量吃緊時，同為尚未嘗試的候選優先回收較大的 raw 批次；容量正常時
  恢復 catalog／raw 交替。現場 2 GB raw 完整回收 120.96 秒，19 MB／488
  檔 catalog 則花 330.51 秒，容量阻塞時輪流選小 catalog 會拖慢可用餘量。
- 原檔重建、完整檔案集合、SHA、NAS 獨立還原、process reference、實體／
  bind root、檔案簽名及 dry run／apply 全部沿用原有檢查。僅回收已驗收
  的傳輸副本；來源原檔、NAS archive、失敗 staging 與證據均保留。
- source replication 在正額度仍小於最小未發布物件時，正確回報
  `waiting_transport_capacity`，附 `publication_capacity_room_bytes`、
  `minimum_next_source_object_bytes`、`unpublished_source_object_count`。
- 備份狀態保留既有 `auxiliary_error` 欄位，另附已知 `SnapshotError` 的有界
  理由，例如 `retain failed staging; auxiliary capacity budget reached`。
  其他例外只顯示類型，不輸出私有設定內容。
- 實際回收恢復後另觀察到容量盤點與 GC rename／unlink 競態，曾把
  `FileNotFoundError` 誤歸類為 receiver readiness。容量盤點改為最多三次
  **完整重新掃描**，不略過消失的路徑；每次仍核對實體 authority、no-follow
  regular-file／同 volume 檢查。持續變動時拒絕回傳部分容量，保留資料並
  等下一 cycle，避免把移入 staging 的同一批次少算。
- worker 以原 publication 邊界載入，第一次載入 SHA 為
  `4f3e2d9369218bf2abf7e2ac7409d825fd261c1c218a36bf16920ae750f7b09d`。
  加入容量盤點重試後，載入 SHA 為
  `ab2f8437b9b460584cf77d00f1ae9d08a556b9cb0284dba3c92626e317595c6d`。
  兩次原工作鏈與 PostgreSQL、Temporal server PID 均保留。
- catalog 收據接納沿用 source 的分工：只在精確的 `published` export journal
  中找到相同 snapshot／delivery、正確實體路徑、已完整核對的 producer copy
  proof、固定 manifest SHA／完整檔數／bytes／退出碼，才採用有界 metadata
  檢查接納 NAS 收據。NAS pairing、全檔還原與 native DuckLake／PG semantic
  restore proof 仍必須全部通過；GC 的呼叫不使用這個快速接納參數，繼續完整
  核對本機 SHA／原檔重建／引用。NAS 可恢復版本與本機 materialization 是
  分開的證據；後來本機損壞不抹除 NAS 證明，也不允許回收損壞的傳輸副本。
  沒有修改 durable workflow 排程或 Temporal history ABI。
  這個版本已按原 owner 邊界載入，實際 SHA 為
  `f2fdd01b4655fb58bb8c25d60d89162294b8122ea743f38a44174bdef011075e`。
- 原 raw retained 額度與整個共用 ingress 都是 256 GiB；只靠公平 GC，在
  raw 持續發布時仍可能將 metadata／code／SQL 排除在容量之外。共用上限
  維持 **256 GiB**，raw 自身上限改為 **224 GiB**，留下 **32 GiB** 給
  共用 staging 與傳統備份，其中既有 staging 上限為 16 GiB。實體磁碟
  的 64 GiB reserve 和接收端容量檢查繼續保留；兩條管線仍可同時發布／
  驗收，沒有重新引入逐批等 NAS ACK 的規則。已有超額 transport 只等待原
  安全 GC 回收，沒有為符合新上限直接刪除。
- 原 10/6 backfill 的 metadata 已全部取得機器 ACK。保留舊檔與收據後，將
  既有 backfill 指向原缺口的 **77 檔／3,324,074 bytes** 固定 metadata 投影，
  每一檔先核對捕獲內容或仍在冷庫的固定 bytes／SHA，其中包括 8 個捕獲的
  head 世代。投影使用自己的身分並引用原清冊及來源收據 SHA，沒有冒充完整
  canonical 清冊；未覆寫正在前進的 authority head。
  77 檔全部 NAS 驗收後保留原投影，再將同一 backfill 切至觀測清冊中另一個
  已前進的固定 head 世代（1 檔／518 bytes），避免把該清冊的舊路徑版本
  當成最新 head 而漏備。每次切換均先確認上一投影全部取得機器驗收。
- 接收端明確回報 `wait_for_Syncthing` 或 `waiting_for_manifest_and_READY`
  時，來源尊重等待指示，不因同時出現 `ValueError` 就建立另一份 RAW 副本。
  未齊的批次繼續原 Syncthing 傳輸，新資料仍可發布；真正的結構化補傳請求、
  舊 receiver 的失敗請求、配對／容量／獨立退避與 attempt 上限全部沿用。
  現場原批次 `dbdf65de…` 與多餘副本 `ab5898d4…` 的 206 個 file keys 和 bytes
  完全一致，原批次後來已有合法 NAS 還原 ACK；兩份封閉副本保留原驗收／回收
  流程，沒有為取消多餘補傳直接刪除任何資料。

## 驗證與持續觀測

工程驗證共 **177 項不同測試通過**。前一版 170 項包含 142 項來源／備份／恢復回歸，另以現場
既有 rclone binary 重跑原本因 PATH 跳過的 3 項真實複製測試。涵蓋失敗批次
不阻擋下一批、中斷後交接、獨立退避、錯誤來源保留、容量小於下一個物件、
有界錯誤理由，以及真實 rclone 續傳與 immutable 衝突拒絕。再新增 4 項容量
競態測試，覆蓋真實 directory-FD scan 遭 unlink 後重掃、rename 到 staging
仍計入所有保留 bytes、連續碰撞拒絕部分結果，以及 authority 被替換時保留
兩份根目錄並拒絕容量證據；相關 95 項回歸與補充 authority 測試通過。
容量壓力排序另有 2 項測試，核對較大 raw 優先、失敗仍獨立退避、catalog
證據保留，以及容量正常時恢復原交替；來源／GC 的 48 項回歸通過。
catalog 接納新增 15 項測試，包含世代／路徑／檔數 bytes／manifest SHA／
數值退出碼、已完成 enrollment、真實配對與 native restore 必要性，以及
本機損壞後 GC 保留所有原檔／副本；journal symlink／FIFO／非 mapping 均拒絕。
最後完整重跑來源／備份／回收的 166 項測試（含真實 rclone），零跳過；
另在鎖定的 control role 執行 4 項 Temporal 活動與 history continuation 測試。
明確 receiver wait 再增加 6 個 state／action 組合與 1 個真正結構化補傳測試，
核對新資料持續發布、原 payload 不變、沒有虛構 ACK，以及真正失敗仍只補傳
精確集合。最終重跑 **173 項來源回歸與 4 項 Temporal 測試，零失敗、零跳過**。
最終工程程式 SHA 為
`d5e21b95ec3283d36e6785680f410a31c4cc3815e28c27503ce9b2613a5b0101`；
實際 owner 載入與工作鏈延續另以部署收據核對。

19:14 首個健康批次 `0782d234…` 完成原有全部回收檢查，實際釋放
**2,142,280,472 bytes**，完整流程 **120.96 秒**；journal 為 `retired`，
`primary_source_files_deleted` 與 `nas_archive_files_deleted` 均為零。
後續發送與 NAS 固定清冊收據仍由原服務和唯讀 observer 持續追蹤。

19:45 的容量壓力排序已完成 **5,707,389,339 bytes／189.14 秒** 的完整回收，
來源及 NAS 刪除仍為零。固定原缺口已減為 **693 檔／15,023,124,149 bytes**；
原 77 檔 metadata 全部取得配對機器還原收據。這是該時間點進度，後續以
固定清冊收據判斷。現場相同固定 catalog 126／125 的新接納流程分別為
**20.62／14.03 秒**，兩筆都核對到 native restore proof；這只量測 ACK
接納，不是全量來源／NAS 流程速度。

20:06 最新備份 cycle 已完成，沒有 `readiness_error`、`auxiliary_error` 或
pipeline scan／export 錯誤；舊 head 世代由相同 backfill owner 補發。
NAS snapshot `b286d674…` 的獨立全檔還原收據已接納；該批 code receipt 中
四個修正程式及 224 GiB 容量政策 SHA 與目前部署檔全部相符。這份 auxiliary
收據證明 code／SQL 檔案可還原，本身未宣稱 SQL logical restore。

20:11 第一個容量恢復後的新 RAW 批次 `d30a980a…` 已取得配對 NAS 還原證明，
包含 247 檔、2,142,553,985 bytes。其後同時傳輸／驗收的獨立批次持續發布，
當時已封閉約 6.43 GB。來源 ledger 在下一 cycle 持久化新 ACK，固定清冊
observer 只計已入 authority ledger 的配對證明；送達與驗收仍分開報告。

## 最終正式驗收

21:00:57，既有 canonical 檔案驗收器以同一份固定清冊、相對路徑與 SHA
去重核對機器收據，退出 0。原缺口 **1,519 檔／19,305,483,099 bytes** 全部
NAS 驗收；包含觀測期間新增 8 檔的固定完整清冊則為 **35,582 檔／
1,096,402,152,731 bytes**，機器待驗收為 **0 檔／0 bytes**。Restic 與 lake
重疊 bytes 已扣除，沒有把兩條管線容量直接相加。

最終清冊身分為
`35dfa6926d570b11de852ab72884668df7ad445b747db14f831727d05478203f`，
清冊檔案 SHA 為
`df237fe06f3c759a58b877ce4e50ac30735faf0305eb41c57ead4c8574bd1eb5`。
最後補齊的 RAW 批次 `5690e33d…` 有配對 NAS 全檔獨立還原證明；來源
authority ledger 已持久化收據，沒有以 Syncthing 送達率取代 NAS 驗收。

21:09:20 的正式部署核對確認最終 worker 已載入
`d5e21b95ec3283d36e6785680f410a31c4cc3815e28c27503ce9b2613a5b0101`。
此程式版本及 224 GiB source 政策也取得 NAS 固定 snapshot
`b77ccb117b9e9b46ede18e8393dc51382049436d887a5dd4e2600524c4e82d05`
的獨立檔案還原收據，五個核心程式／政策 SHA 均與現行部署相符。這份
auxiliary 證據僅證明檔案還原，不將 SQL 檔案可讀視為 PostgreSQL 邏輯還原。

本次已完成 **43 批**安全 transport 回收，實際釋放
**89,517,056,211 bytes（89.52 GB）**；authority 來源檔和 NAS archive
刪除數均為零。最新 backup cycle 的 readiness／auxiliary 錯誤為空，
來源與備份 receipt／scan／export 錯誤清單均為空。catalog 工作鏈已從
snapshot 126 推進到 130；原工作鏈及 PostgreSQL、Temporal server 程序保留。

來源 lifecycle、GC timer、backup timer、Syncthing 和 Temporal 為
`active / enabled`；PostgreSQL 18 cluster 為 `active / enabled-runtime`。
原 90 分鐘唯讀 observer 退出 75，以及要求全域 transport idle 的續接
observer，均以同一清冊後續成功的檔案驗收替代；後者只停止診斷程序，
沒有停止正式傳輸、發布或備份 owner。

本次完成的是固定來源缺口與修正程式部署。後續新資料和 Restic 轉 immutable
archive 仍自動處理；21:09:20 的全域來源待 NAS 驗收為 17,109,992,112 bytes，
不屬於本次已補齊的固定缺口，亦未宣稱全域同步已 idle。此清冊沒有涵蓋
未發布工作區；`all_history_backup_verified=false` 仍保留。

| 證據 | 位置 |
| --- | --- |
| 最終固定缺口、部署與服務核對 | [`production-acceptance.json`](../artifacts/operations/nas-source-snapshot-repair-20261007/production-acceptance.json) |
| 固定完整清冊的 canonical NAS 檔案驗收 | [`nas-fixed-cohort-file-accepted.json`](../artifacts/operations/nas-source-snapshot-repair-20261007/nas-fixed-cohort-file-accepted.json) |
| 最終程式／政策的 NAS 固定 snapshot 還原 | [`final-code-nas-observation.json`](../artifacts/operations/nas-source-snapshot-repair-20261007/final-code-nas-observation.json) |
| 最後 RAW 批次的配對 NAS 全檔還原 | [`last-fixed-wave-nas-accepted.json`](../artifacts/operations/nas-source-snapshot-repair-20261007/last-fixed-wave-nas-accepted.json) |
| 唯讀 observer 結案與正式流程保留 | [`observer-handoff.json`](../artifacts/operations/nas-source-snapshot-repair-20261007/observer-handoff.json) |
| 工程回歸 | `artifacts/operations/nas-source-snapshot-repair-20261007/focused-source-gc-accepted.xml` |
| 真實 rclone | `artifacts/operations/nas-source-snapshot-repair-20261007/real-rclone-accepted.xml` |
| 並行容量競態回歸 | `artifacts/operations/nas-source-snapshot-repair-20261007/concurrent-capacity-accepted.xml` |
| 容量重試的 authority 邊界 | `artifacts/operations/nas-source-snapshot-repair-20261007/capacity-authority-accepted.xml` |
| 壓力排序與原正常排序 | `artifacts/operations/nas-source-snapshot-repair-20261007/pressure-priority-accepted.xml` |
| 最終完整來源／備份／回收回歸 | `artifacts/operations/nas-source-snapshot-repair-20261007/final-source-regression-accepted.xml` |
| receiver wait 修正後的完整工程驗證 | `artifacts/operations/nas-source-snapshot-repair-20261007/receiver-wait-engineering-validation-accepted.json`、`receiver-wait-source-regression-accepted.xml`、`receiver-wait-temporal-accepted.xml` |
| catalog 身分／恢復與回收界線 | `artifacts/operations/nas-source-snapshot-repair-20261007/catalog-enrollment-accepted.xml`、`catalog-admission-guards-accepted.xml` |
| Temporal 活動／history continuation | `artifacts/operations/nas-source-snapshot-repair-20261007/temporal-catalog-admission-accepted.xml` |
| catalog 現場固定 proof 接納實測 | `artifacts/operations/nas-source-snapshot-repair-20261007/catalog-admission-live-trial.json` |
| worker 載入／工作鏈保留 | `artifacts/operations/nas-source-snapshot-repair-20261007/worker-reload-accepted.json` |
| 容量重試程式實際載入 | `artifacts/operations/nas-source-snapshot-repair-20261007/concurrent-capacity-worker-reload-accepted.json` |
| 共用容量政策與 owner 交接 | `artifacts/operations/nas-source-snapshot-repair-20261007/shared-spool-policy-accepted.json` |
| catalog 接納程式載入 | `artifacts/operations/nas-source-snapshot-repair-20261007/catalog-admission-worker-reload-accepted.json` |
| 已部署 code 與新 catalog 的 NAS 收據 | `artifacts/operations/nas-source-snapshot-repair-20261007/new-code-and-catalog-nas-proof.json` |
| 容量恢復後的新 RAW NAS 配對還原 | `artifacts/operations/nas-source-snapshot-repair-20261007/post-capacity-source-nas-accepted.json` |
| 明確等待卻重複補傳的現場診斷 | `artifacts/operations/nas-source-snapshot-repair-20261007/receiver-wait-redelivery-diagnosis.json` |
| 最終 receiver wait 程式的 owner 交接 | `artifacts/operations/nas-source-snapshot-repair-20261007/receiver-wait-worker-reload-accepted.json` |
| 健康與缺原檔批次清冊 | `artifacts/operations/nas-source-snapshot-repair-20261007/gc-primary-presence-inventory.json` |
| 已丟棄物件精確核對 | `artifacts/operations/nas-source-snapshot-repair-20261007/missing-source-disposition-correlation.json` |
| 固定 metadata 投影與全部 SHA | `artifacts/operations/nas-source-snapshot-repair-20261007/metadata-backfill-projection-accepted.json` |
| 觀測清冊的已前進 head 世代 | `artifacts/operations/nas-source-snapshot-repair-20261007/observer-metadata-backfill-accepted.json` |
| 固定完整清冊與觀測 | `artifacts/operations/nas-source-snapshot-repair-20261007/fixed-catalog.json`、`nas-cohort-observer.json` |

## 日常查核

使用相同 owner namespace 查詢；不要新增 publisher、手動偽造 ACK、提升額度
掩蓋回收失敗，或直接刪除未驗收的 staging。

```bash
bash scripts/run_lakehouse_control.sh status
stockagent-agent run-status nas-fixed-cohort-file-acceptance-20261007T125953-86193d68
jq '{state, original_scope_pending_files, original_scope_pending_bytes, all_history_backup_verified}' artifacts/operations/nas-source-snapshot-repair-20261007/production-acceptance.json
systemctl status stockagent-lake-transport-gc.timer stockagent-backup-stream.timer
```

固定清冊檔案驗收以該清冊的機器待驗檔案／bytes 為零為準。全域正反向
transport 收斂另用於診斷最新傳輸；持續發布後續增量時，不把全域 idle
作為已固定清冊的 NAS 檔案驗收條件。
`waiting_transport_retry` 表示所有剩餘回收候選仍在退避，退出 75；
`retirement_deferred` 表示該批的恢復或引用門檻尚未通過，不能當成已釋放容量。
一份固定保留清冊完成也不代表未發佈工作區、已放棄歷史或所有未來增量均已備份。
