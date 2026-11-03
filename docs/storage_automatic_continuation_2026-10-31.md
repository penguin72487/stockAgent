# 儲存工作自動接續

使用者於 2026-10-31 要求所有既有工作自動排程，避免每批人工命令。
本次沿用現行 publisher、NAS relay、Vast return、Temporal 與 retirement
執行者；新增的是持久派工與獨立觸發，沒有另一套封存／刪檔程式。

| 工作 | 自動接續方式 | 完成依據 |
| --- | --- | --- |
| 原值遷移、catalog／source replication | 原 Temporal workflows、storage-lifecycle service | 原固定版本、完整重建及 registry 收據 |
| 公開 NAS 備份 | 原 backup-stream timer | 配對封套與獨立 NAS 讀回 ACK |
| 私有 NAS 備份 | private-cold-backup timer 每 30 秒 | 同一 owner／ledger 的逐批配對 ACK |
| 封閉批次掃描通知 | backup-notify timer 每分鐘 | 原 dispatch scan 收據；不是 NAS ACK |
| 固定 WSL 第 12、13 輪驗證及回收 | wsl-retirement-queue timer 每分鐘 | 原完整 recovery、retirement、acceptance 及 source absence |
| Vast 成果／歷史回傳 | 原 ingress／legacy-return／bulk-return timers | 原完整性、保留期限及回傳驗收 |
| lab203 → NAS | 原 lab203 backup／lake-relay owners | 原 NAS 獨立還原與配對 ACK |
| 冷區整理、傳輸暫存及使用快取回收 | 原 cold-tiers、lake-transport-gc、data-cache-gc timers | 各自原 lease、capacity、重建與刪除條件 |

## 排程原則

1. 先辨識同一工作的現有執行者；忙碌時保留原工作，不能重複捕捉清冊或刪鎖。
2. 原值遷移、來源複製、傳輸、NAS 讀回及暫存回收可以各自前進，但完成證據
   不互相代替。公開的大型 metadata capture 不再延後私有下一波派工。
3. 回收只接續已登錄的固定清冊，SHA 不符或 source 重現就保留並記錄原因。
   重開機後讀原 journal／receipt，重新核對所有原 owner gates，不沿用舊程序
   的記憶體 proof，也不把舊 process 的 `running` 當完成。
4. 容量或既有 owner 等待不消耗失敗次數；實際 owner 失敗有退避及三次上限，
   每次另存不可變失敗紀錄，其他可執行且不重疊的 cohort 繼續。維護中的
   執行程式版本改變後自動重新嘗試，不需人工清理 retry 狀態；原完整 gates
   仍重新執行，舊失敗檔不刪除。
5. 當沖、現行帳本、NAS 正本及 D 正本的保護條件保留。不新增 CPU／GPU／
   記憶體／磁碟吞吐上限，不為搶執行權強停現有 worker。

## 設定與觀察

- 固定清冊：[wsl_retirement_queue.json](../configs/data_sync/wsl_retirement_queue.json)。
- 私有派工：`backup_stream.json` 的 `private_history_schedule=independent_timer`。
- 本機服務狀態：`service_data/storage-retirement/status.json`，包含每批狀態、
  原 acceptance、失敗次數、下一次重試時間與阻擋原因。
- 私有備份狀態：沿用 `/var/lib/stockagent/backup-stream/private-status.json`。
- 三個新增／接續 timer 均由 systemd enable，停機錯過的觸發由 Persistent
  timer 接手；同 service 不會因下一次時間到就再開一份。
- 安裝入口沿用 `scripts/install_enrolled_artifact_retirement.sh --automation-only`。

## 全資料面板的備份、驗證與回收進度

全資料網站的「所有機器的儲存狀況」先顯示備份、驗證與空間回收摘要，
再顯示磁碟容量與可展開分類。執行中與前置待處理工作在前，其他工作及
已完成範圍可展開；每個工作保留各自的證據範圍、收據時間與程序活動時間。
預定排程從本機已安裝的 systemd timers 讀取，標示「下次檢查」，不是
工作開始或完成承諾。NAS 端排程由 lab203 原 owner 管理，接收狀態沿反向
回執通道觀測；不為網頁建立新的遠端管理連線。

沿用原 storage-monitor，每 60 秒由
[storage_progress.py](../stockagent/data_sync/storage_progress.py) 收集有界本機
metadata；頁面每 60 秒讀取原 `/data-monitor/api/storage`。HTTP 只讀已物化
快照並再次白名單投影，不執行 systemctl、SSH、du、hash 或備份／回收。
來源為原 backup-stream 小型 status、private-status、scan status，遷移的
current pointer 與 progress／recovery receipts，已登錄回收佇列及其固定
receipt 目錄，還有 lab203 pipeline-status／storage-relay-audit。
不讀大型 catalog、private-catalog 或 ledger。單一來源讀取失敗不隱藏其他
工作或磁碟容量。公開投影不包含檔案路徑、帳號、連線資訊或原始錯誤字串。

分階段 ETA 使用同一版本、階段、計量單位與分母的觀測增量。每工作最多
保留最近 30 分鐘內的 24 個純量樣本於
`service_data/live/data_monitor/storage_operations_history.json`，不複製來源內容。
階段、版本、分母或倒退計數重置取樣；相同收據時間不新增速度樣本。
估時包含有心跳但沒有進展的時間；五分鐘無新收據／進展，或預估已逾期，
停止顯示完成時間。bytes 與 items 都有完整分母時分開取樣，採較慢的實測
投影，避免大量小檔尾段只有少量 bytes 卻仍需長時間核對。bytes 已滿、檔數
未滿時以檔數繼續取樣，不宣稱待驗收完成；缺少任何速度時不虛構尾段 ETA。
最快／較慢範圍來自同階段區間速度，包含零速度時不聲稱有限的較慢上界。
沒有後續階段的工作量與等待時間時，總完成時間維持待估並說明原因。

進度計數達目標仍是待驗收；固定冷庫 NAS 檔案覆蓋、私有冷庫覆蓋、D 原始
資料還原、分工 pack 驗證與來源回收不互相代替，也不相加為全域百分比。
沿用舊固定版本的驗證證據另列，不冒充剛重新驗證。WSL 釋放量只加總本次
已登錄且原 acceptance 與清冊一致的回收批次；lab203 累積回收獨立顯示，
不算進 Penguin。這份進度投影不改動原工作、pin、retry、鎖或刪除條件。

本輪驗證：102 項相關測試通過，177 項非本次範圍未執行；公開網站的
Chromium 1440／390 寬度共 13 個原生及故障情境檢查通過，涵蓋展開狀態、
鍵盤焦點、斷線時暫停估時、XSS 文字與無水平溢出。原生頁面檢查與合成
故障情境分別記錄，不當作 NAS 或回收驗收。
進度觀測三次本機實測中位數約 0.33 秒，純進度公開投影約 18 KB；這是
新增監測成本，並非原檔驗證吞吐量。估時在公開投影時扣除快照年齡，剩餘
時間與預估完成時刻保持同一時間基準；逾期估時等待新觀測，不沿用倒數。

- [相關回歸 run](../artifacts/storage_layout/storage-dashboard-eta-age-regression-launch-20261031.json)
- [公開瀏覽器驗證](../artifacts/storage_layout/storage-dashboard-browser-final-20261031/browser_acceptance.json)
- [新增監測成本量測](../artifacts/storage_layout/storage-dashboard-observer-final-benchmark-20261031.json)
- [公開及本機讀回](../artifacts/storage_layout/storage-dashboard-final-readback-20261031.json)
- [限定網頁程序更新與原 owner PID 核對](../artifacts/storage_layout/storage-dashboard-eta-age-deployment-20261031.json)

### lab203 Codex：NAS 分階段進度補件

2026-10-31 使用者將面板更新頻率改為每分鐘；這項前端設定不需要 lab203
改動或重啟服務。另於本機讀取既有反向同步回執：`pipeline-status.json`
的 15:44:54 UTC 版本列有 21 筆 job，每筆只有封套 identity 與 state；
`storage-relay-audit.json` 的 15:48:23 UTC 版本未回傳分階段完成量／總量。
因此可以顯示接收狀態，但不足以計算 NAS 獨立驗證的可靠完成時間。
以下是待 lab203 實作並回傳固定樣本的相容擴充提案，尚未宣稱已部署：

1. 沿用原 owner，每分鐘原子寫入有界的 `pipeline-progress-v1.json`，
   契約建議 `stockagent.nas_relay_progress.v1`，經原反向回執通道回傳。
   **保留既有 `pipeline-status.json`／audit 及簽章格式**：Penguin 的
   `backup_relay_pipeline.receiver_status` 對 v1 欄位採嚴格白名單，直接
   增加欄位／狀態會破壞既有機器讀取。新進度檔以原配對身分、原回執
   identity 與原完整性方式綁定，不能拿它替代正式 ACK。不要為網頁
   新增掃描、hash、備份 owner 或每分鐘重跑驗證；只讀現有工作計數。
2. 每筆 job 保留原封套／派工身分，補上穩定的 attempt／phase 身分、階段
   名稱（接收、NAS 寫入、獨立還原、全檔核對、回收），並區分 queued、
   running、waiting、failed、completed。保留不同階段的原驗收證據。
3. 同階段提供 `completed_bytes`／`total_bytes` 與
   `completed_items`／`total_items`；尚未知的分母填 null。單位與範圍明確，
   不混加壓縮傳輸 bytes、原始還原 bytes 或不同階段計數。
4. 分開回報 `observed_at_utc` 心跳、`progress_at_utc` 計數觀測時間、
   `phase_started_at_utc`，以及 `next_retry_at_utc`、固定阻擋原因代碼。
   原工作沒有更新計數時，不以新心跳冒充新進展。
5. 在這份獨立進度檔補上原排程器的下次檢查時間與目前是否執行；
   不把下次檢查當作完成時間。完成後回傳一份固定身分樣本，讓 Penguin
   補上獨立檔的配對驗證、欄位解析與估時驗收。現有 Penguin receiver 投影仍沿用舊欄位，
   不會僅因新增欄位就自動宣稱已有 NAS ETA。

只交付狀態與身分，不交付原始資料或金鑰；既有配對 ACK、容量、pin、retry
與回收規則照原 owner 執行。這項補件不阻塞既有傳輸、備份、驗證或回收。
現有 v1 jobs 是上一輪結果清單；Penguin 已將 queued、accepted、retry_wait、
rejected 分開呈現，不再把清單長度當成正在執行的 worker 數。
每分鐘更新與這項計數修正的回歸為 103 passed；公開靜態資源及 API 已讀回
確認 `storage.js?v=9`、60 秒週期，且 receiver 的上一輪 17 queued／4 accepted
不再列為 active workers。證據見
`artifacts/storage_layout/storage-monitor-minute-regression-launch-20261031.json`
及 `storage-monitor-minute-readback-20261031.json`；未重啟當沖或原儲存工作 owner。
  這是初次部署或修復命令，平常不需人工執行。

## 驗證

新持久佇列最終 **18 項測試通過（1.63 秒）**，覆蓋：重開接續、不重複派工、來源重現、SHA 變動、
容量／鎖等待、失敗退避與隔離、缺少原 acceptance 時拒絕成功、來源重疊與
外部／符號連結控制路徑拒絕、忙碌時不把舊結果投影成當次驗收，以及程式
修復後自動接續且保留舊失敗紀錄。較早整合原 wave planner/executor、公開／
私有備份的五個測試檔共 **247 passed in 47.57s**，exit 0。

19:52 實機已 enable 三個 timer，實際 queue tick 回報
`waiting_existing_wave`／`all_enrolled_accepted=false`，兩個固定 cohort
保持登錄，沒有另開第 12／13 輪；下一分鐘會重新檢查。private timer 的
原 private-cycle 程序已啟動並等待原 owner，舊工作不被中斷。
storage-lifecycle、當沖、Syncthing、backup-stream 四個既有程序的
PID／start-ticks／boot 身分前後一致。CPU quota、MemoryHigh／Max 的
實際值均為 infinity，沒有新增資源上限。

部署證據：`artifacts/storage_layout/storage-automation-install-20261031.json`；
整合測試 run：`storage-automatic-scheduling-tes-20261031T115015-62d3ad08`。

真實資料完成時間與 WSL 釋放量仍以後續原收據核對；安裝 timer 不代表這些
資料已完成，也不能解決來源本來已遺失的觀測值。

## 11/1：三端運行觀測與實際阻擋修復

全資料頁現在先顯示 Penguin、lab203、Vast 三張狀態卡，再顯示驗證、備份、
回收、排程與磁碟分類。原 storage-monitor 與頁面都每 60 秒更新；HTTP
只讀已產生的快照，不啟動遠端工作、掃檔、hash、模型或 GPU。

- Penguin：讀取 `/proc` 的 CPU 時間差、RAM 與固定服務白名單。初次取樣的
  CPU 使用率未知，重開機會重置時間差；不為面板喚醒本機 GPU。
- Vast：沿用同一次容量 SSH，讀本機 portal metrics 與 Supervisor 的
  StockAgent 程序數。GPU 記憶體保留來源標示的 MB，不暗自改成 MiB；程序、
  GPU 使用率與 CUDA／資料驗收分開。觀測失敗保留上次值及原時間，不能報零。
- lab203：重用原簽章與配對的 storage-status 回執確認在線。尚無 CPU／RAM
  及逐階段工作量時顯示待回報，不能由上一輪 jobs 推算 active workers。

每個節點獨立判斷過期與失聯。工作清冊新增同步收斂、等待 owner／引用、
回收前核對與編碼暫存整理等階段；等待階段不再計成正在 hash。
下一次排程檢查仍不是開始或完成時間，沒有完整分母／速度就不顯示倒數。

私有備份的實際故障及修正：

1. 接收就緒回執超齡原本直接退出；現在以 `RelayReadinessClockError`
   明確區分時鐘／年齡等待與壞簽章、錯配對、掛載或 runtime 錯誤。
   同一個等待期間先檢查小回執，不重讀大型 catalog／ledger；回執更新後
   立即走原流程，沒有延長有效期或放寬容量檢查。
2. 新一輪活動狀態覆寫計數時，面板原本會退回更舊總表。原 owner 現在另寫
   小型 `private-coverage.json`（`stockagent.private_cold_NAS_coverage.v1`），
   綁定原 catalog 與實際 coverage 時間。新心跳不改寫 coverage 時間；新
   catalog 不繼承舊 generation 的進度。它是投影，不能代替 NAS ACK。
3. 拆開記錄 staging、retained transport、physical 三個獨立容量邊界。
   00:21 的原 owner 收據顯示 retained 配額僅剩 3,406,682 bytes，實體磁碟
   扣原保留空間後仍有 80,857,534,464 bytes；並非 CPU 已滿。
   依使用者既有硬體容量授權，將 retained ceiling 從 256 調到 512 GiB，
   每批仍受實體 free、64 GiB reserve、24 GiB staging、未 ACK 上限及
   接收端條件限制。00:25 已由原服務自動開始約 7.99 GiB／96 檔的一波，
   當輪上限回到雙端 8 GiB batch limit；不是已完成傳送或 NAS 驗收。

本次完整相關回歸為 **489 passed in 69.11s**，另做隔離工具包依賴驗證。
隔離工具包與 runtime 回歸為 **51 passed in 8.82s**；包含在沒有 Penguin
checkout 的隔離目錄執行已宣告工具包成員的 `--help`，核對 import 依賴閉包。
公開 HTTPS Chromium 桌面／手機含異常、刷新、展開與安全輸出共 **15 案通過**。
只重啟唯讀 observer 與公開 gateway，當沖、隔夜、期貨行情、Syncthing 與
原儲存 worker 的程序身分保持不變。原固定交付包及 lab203 已安裝版本沒有
改寫；後續新工具包準備清單納入新 helper 的依賴，不表示已部署到 lab203。

證據入口：

- `artifacts/storage_layout/trinode-complete-tests-20261101.launch.json`
- `artifacts/storage_layout/trinode-handoff-tests-20261101.launch.json`
- `artifacts/storage_layout/trinode-browser-20261101/browser_acceptance.json`
- `artifacts/storage_layout/trinode-retained-transport-before-20261101.json`
- `artifacts/storage_layout/trinode-public-local-readback-20261101.json`

### 00:45：編碼暫存計數與當前回收邊界

原 worker 已提供 `manual-stage-prune-progress-v1`，面板原本沒有讀取這份
計數。現在只接收固定 enrollment 的 manifest、dataset、snapshot、原
retirement 收據與當前 phase 時間一致的觀測；規劃、核對、冷庫還原與
刪除子階段分開取樣。`prune_step`、計數範圍或 metadata／內容計量改變時
重置速度樣本；100% 計數不能代替整批 acceptance。觀測若尚無分母，仍
維持未知，不為了畫進度條另掃來源。這次只載入 observer／gateway 新版，
原驗證與回收 worker 持續執行。

新增觀測回歸 **305 passed in 41.63s**，另單獨核對不同計數範圍的重置測試；
這些與前述測試有重疊，不相加為獨立案例總數。

00:45 公開讀回：第 12 輪已進入 `apply-unlink-quarantine`，原檔刪除收據
記錄 WSL 釋放 **7,665,729,536 bytes（7.14 GiB）**；編碼暫存收尾及整批
acceptance 尚未完成，不能納入面板「已驗收批次釋放 WSL」總計。
第 13 輪整批已驗收 **4,011,114,496 bytes（3.74 GiB）**。兩者的
`backup_verified=false` 保留，不能冒充 NAS 驗收或 Windows C 槽實體釋放。

原生 Windows 磁碟計數的一次觀測顯示 D 槽讀取約 1.01 MB/s、寫入
3.95 MB/s、queue 22；原執行緒分別在 native copy 與逐檔 metadata 檢查。
這支持繼續降低小檔 I/O 與避免重複讀取的方向，不能由單次樣本宣稱已達
硬體上限。既有 64 GiB reserve 與完整驗證保持；原自動排程均為 enabled。

- `artifacts/storage_layout/trinode-stage-progress-tests-final-20261101.launch.json`
- `artifacts/storage_layout/trinode-stage-public-readback-20261101.json`
- `artifacts/storage_layout/trinode-stage-browser-20261101/browser_acceptance.json`
- `artifacts/storage_layout/trinode-bottleneck-20261031/native-disk-counters.json`

### 請交給 lab203 Codex 的補件

沿原 backup owner 與反向回執通道進行，無需新管理連線或另一套掃描／備份。

1. 延續上節 `pipeline-progress-v1.json` 的逐階段計數、原證據時間與排程
   補件。既有 `pipeline-status.json` 的 strict ABI、原 pin、ACK 與失敗 gate
   保持原樣。先交固定 identity 樣本，Penguin 再實作配對解析及 NAS 分段 ETA。
2. 每分鐘從既有 observer 提供獨立、版本化、簽章及配對的 CPU／RAM 觀測：
   明確列 source clock、實測範圍／單位、活動 owner 與等待原因；未知填 null。
   不以心跳替換進度時間，不把上次完成的 jobs 當作正在執行的程序。
   Penguin 目前只支援既有 storage-status 的在線觀測；新格式仍需解析驗收。
3. 由原 owner 在長工作之間保持 readiness 更新：重新執行原就緒與容量觀測，
   忙碌或無法重新確認就明確回報等待。不得僅刷新舊回執時間，或延長 lease
   來讓新波次通過。這三項補件不阻塞現有傳輸、NAS 還原、驗證與安全回收。

## 22:20 接續修復與實機觀測

第 13 輪已完成原 retirement 與 acceptance；本次重新執行原 `_accepted`
核對清冊 SHA、完成收據與來源不存在，實際釋放 WSL **4,011,114,496 bytes
（3.74 GiB）**，D stage 另回收 905,039,872 bytes。原收據仍是
`backup_verified=false`，不能說成 NAS 已驗收。第 12 輪完整 D recovery
已完成 4,991.59 秒的階段，現有程序持續進行 encoded stage audit，沒有重啟。

私有備份原固定清冊的 NAS 檔案覆蓋為 **33,881／48,276** 檔、
224,933,764,711／817,226,447,345 bytes；這不是全專案或 canonical
重建驗收。19:48 之後未看到新私有 ACK。22:14 直接核對發現新 batch
READY／dispatch 在 D 上存在，Syncthing `/rest/db/file` 均回 404；
同期已索引資料卻顯示 peer 100%，lab203 pipeline 的 jobs 為空。
因此百分比的分母不含尚未索引的新批次，不能代表全部已送達。

修復沿原 notifier：`busy_scan_notification_enabled=true` 時，即使既有
scanner 忙碌，也只排一筆「tools + 原清冊第一個未通知封閉 batch」的指定
路徑掃描，交由 Syncthing 自行接續，不中斷現有掃描。獨立 notification
lock 防止重複提交，POST 前持久化 exact intent；逾時或崩潰後，只要仍在
scanning，就保留未確認狀態且不重送，等實際 idle 再接續。網路等待不持有
publication owner，回來仍重新讀 ledger／匹配原 dispatch 才記掃描收據。
這個 intent／HTTP 成功都不是 NAS ACK，原資料及所有刪除條件不變。

原 notifier 同時改用 `--wait-for-owner` 等待原 flock 的接手點，避免每分鐘
恰好撞上 private owner 而無限 exit 75；等待狀態會明確寫入小型收據。
service 不因排鎖等候而被 30 分鐘的啟動 timeout 中止，實際每筆網路請求
仍保留原 900 秒期限。22:26 原 notifier 已在 `/proc/locks` 排入原 owner
後方，22:26:52 已取得並釋放 publication owner，持久化第一筆原封閉 batch
的 intent，狀態為 `waiting_existing_scanner_queued_scan`。這只是正在等原
scanner 接續，尚未收到此筆掃描成功或 NAS ACK。

最終公開／私有備份、relay、offhost 及 retirement queue 回歸為
**205 passed、14 skipped（47.20 秒）**，run
`notifier-waiter-regression-20261031T142428-98c2e704`；原 installer 的
`--automation-only` 已驗證 systemd units 並載入新設定，未重啟當沖、
lakehouse、Syncthing 或現行 R12。

最後再補上 `syncing`／`scan-waiting`／`error` 三種狀態的語義回歸：
未知結果必須看到真正的 `idle` 才能解除，不能只憑「不再 scanning」。
最終 **208 passed、14 skipped（45.43 秒）**，run
`notifier-idle-state-regression-20261031T143043-8931ae19`。22:28:40
Windows 實體 C／D 可用量分別為 39,965,376,512／149,615,882,240 bytes；
仍是並行負載下的當前 headroom，不等同累積回收量。

實機部署與原鎖接手證據：
`artifacts/storage_layout/trinode-notifier-live-20261031T142832Z.json`。
來源 ledger 仍有 26 批、121,936,693,597 dispatched bytes 待配對 ACK；
此數包含不同 delivery 類型，不能當成私有清冊的獨有剩餘資料量。

規格依據：[Syncthing 指定路徑掃描](https://docs.syncthing.net/rest/db-scan-post.html)，
以及 v2.1.5 原 `folder.Scan`／`doInSync` 的同步排隊與 HTTP 逾時後可能
繼續執行行為。檔案級狀態、CPU／磁碟短測、負面效能試驗與三節點固定
證據見 [分工驗證紀錄](cooperative_packed_verification_2026-10-31.md)。

## 2026-11-01 上午：完成批次誤擋、掃描讓路與後續自動回收

第 12 輪原 `progress.json` 在 00:48:36 已記錄完整回收完成，來源配置
空間釋放 7,665,729,536 bytes，D 編碼暫存另釋放 1,561,919,488 bytes。
排程卻在讀取完成收據前，先要求已依原流程回收的 D 暫存再次存在，因而
錯報 `enrolled cold manifest differs`。現在先走原 `_accepted`，逐筆核對
固定 inventory、原 manifest、原 acceptance 與來源不存在；只有尚未完成
的批次仍要求既有固定暫存。沒有補造 acceptance 或把 D 驗證改寫成 NAS ACK。
planner 也不再重讀這些已退休的暫存名稱。

第 12、13 輪在原每分鐘 queue 中均為 `accepted`，收據合計釋放 WSL
11,676,844,032 bytes（10.87 GiB）。這是這兩個固定批次的累積已驗收
配置量；不是本次部署剛釋放的數字，也不等於 Windows C 的實體回升。

另一個現場鎖競爭來自可選的網站分類掃描：它持有原 scheduler 的 shared
flock，但優先權檢查只辨識 agent run，漏掉每分鐘啟動的正式 systemd
queue。現在也讀取本機已登錄、啟用、同 contract 的 queue 小型狀態，只有
新鮮的 `waiting_existing_wave`、`dispatching_original_owner`、`inspecting`
才讓分類掃描讓路；過期、完成、容量等待不會永久停住清點。既有中斷流程
只終止並 join 自己的可選 `du` 子程序，再釋放 shared lock，沒有清鎖、
中止驗證或重啟當沖／lakehouse／Syncthing。當時觀測到的是可重現的鎖競爭，
不是已證實的永久死鎖。

已清點並登錄原第 15 輪封閉測試副本：30,073 檔、5,912,353,717 logical
bytes、5,990,043,648 allocated bytes（5.58 GiB）；inventory SHA 為
`b57b184b0a21c0912dae0dfebc3eb730b9e900fe1564fdc3302e5b093280aa87`。
原 producer 是 `workflow_round15_scalar_prefilter_acceptance_v1`，只寫
兩份完成收據，沒有 `source_disposition.json`；原 8.52 MiB seal、8 次
fixed-input trial／32 個 means 及 `candidate_admitted=false` 完整保留。
只針對這個已審閱 ABI 接受兩收據與 16 MiB seal 上限，未開放其他輪次、
未變更原失敗結果。歷史外部 verifier／agent run／report references 只作
原證據參考，不增加這個批次可以刪除的路徑。原完整 SHA、D 原值還原、
引用、quarantine、同步及原 acceptance 仍須逐項通過。

第 15 輪沿 `stockagent-wsl-retirement-queue.timer` 自動檢查，採原
`d_stage_recovery_only`，不在 C 新建完整資料還原樹。09:22–09:24 的
dry run 與正式 queue 都顯示 `waiting_capacity: windows_c`：C 實體
可用約 30.21 GiB，原規劃要求約 33.84 GiB；WSL guest 可用約 194.26
GiB 不能取代 C 的實體餘量。原 `fstrim.service` 回報處理 6,217,646,080
bytes，但未量到 C 對應回升；保留原容量門檻，沒有為啟動任務而降低 reserve。
容量恢復後原 timer 自動重查與接手，不需要手動重送。

尚未啟動的批次原本缺執行 `progress.json` 就被面板顯示成無法讀取。現在
只有完整 enrollment SHA、inventory SHA、receipt target 及 queue contract
匹配的前置狀態，才可顯示已排程／容量等待。清冊預計回收量與檔數獨立
標記，不能算作已驗證、已回收或用來產生完成時間。完成及未知的狀態門檻
保持原樣；NAS 階段文字也改為中性，避免已完成卡片仍顯示「等待」。

09:24 公開 API 的私有固定清冊 NAS 檔案覆蓋為 45,730／48,276 檔、
452,495,910,970／817,226,447,345 bytes（55.37%），與已登錄冷庫總表
有重疊，不能相加。這一階段的速度估計仍為滾動觀測，不是全部工作完成
承諾。三節點每分鐘取樣：Penguin CPU 43.27%、RAM 約 9.40 GiB；Vast
CPU 1.5%、GPU 0%、GPU memory 4 MB；lab203 有新鮮配對在線及磁碟
回執，CPU／RAM 與逐階段工作量仍未知。未接到 `pipeline-progress-v1.json`，
因此仍需按上節三項交接由 lab203 原 owner 補件，不能把心跳當作進度。

實機證據：`artifacts/storage_layout/trinode-morning-deployment-after-20261101.json`
核對當沖、隔夜、期貨行情、lakehouse 及 Syncthing 的 PID、start ticks、
boot identity 均未變。原受保護服務沒有因本次觀測／排程修復被重啟。
公開快照、容量 dry run、browser 與回歸 launch 收據都保存在同目錄，
使用 `trinode-*-20261101` 前綴；測試失敗紀錄亦保留，最終結果另記於任務
milestone，不以先前未通過的測試作驗收。

最終回歸 `trinode-preflight-final-regressi-20261101T012529-3754ed8e`：
**670 passed（48.92 秒）**。部署 `storage.js?v=11` 後，公開 HTTPS
桌面／手機 **17 項 browser cases 通過**，其中兩個實機容量卡片測試均
核對第 15 輪為等待、清冊量尚未釋放。09:28:36 再讀公開 API，三節點
狀態及七個既有 timers 均可觀測；R15 仍等待 C，D 冷庫原值還原為 58／61
版本，私有 NAS 檔案覆蓋 55.37%。此刻沒有新的可靠階段 ETA，故顯示未知，
不沿用先前的 22 小時估值。Penguin 與 Windows C 的可用量分別為
194.24／30.18 GiB。`trinode-final-protected-services-20261101.json`
再次核對五個受保護原程序完全連續；原始清冊、所有失敗與完整讀回門檻保留。

### 2026-11-01：備份使用原預留池及主機空間分帳

使用者補充「原本保留就是留給備份用的」。原排程把 32 GiB 當作本輪工作
結束後仍須完整保留的 floor，使備份、驗證、退休所需的空間又被加算一次。
現在固定清冊的 queue 明確登錄 `capacity_reserve_policy: use_backup_reserve`
及該用途指示；政策經原 wave planner、每波 fresh preflight、canonical apply
傳到 guarded-D archive preparer。一般 caller／下載器的保留設定不變。
只有明確的 guarded-D 回復模式可選此政策，完整工作量仍須符合 WSL、Windows
實體 backing 與 D 的各別可用空間；未量測、空間不足、owner 忙碌、來源或
固定 SHA 改變仍會暫緩。source／consumer／完整 cold reconstruction／transport／
quarantine／retirement 證據全部保留，不能回退成 C 上完整解壓工作集。

第 15 輪 30,073 檔的 control workspace forecast 為 1,970,864,128 bytes
（1.84 GiB）；原 33.84 GiB 的 WSL／C 門檻不再重複扣留 32 GiB。D 的兩份
工作量預估為 15,766,435,690 bytes（14.68 GiB）。這是逐波預估與重新准入，
不是空間已預約、完整吞吐上限或已授權刪除的證明。09:46 原 queue 已自然
啟動第 15 輪；本次未重啟該 owner，正在跑的程序繼續使用其已載入版本，
下一次正式 queue turn 自然讀新政策。10:04 觀測仍為封存中，完成清冊／
原值還原／退休後才能把預計 5.58 GiB 計為實際回收。

網頁 `storage.js?v=13` 分開顯示 WSL 檔案系統可用、主機新增配置餘量及
Windows／WSL 的實體占用。每分鐘沿既有原生 capacity probe 讀 registry 中
固定 VHD metadata，以 `GetCompressedFileSizeW` 取得實體配置量，不遍歷
使用者檔案，不讀 VHD payload，也不靠 logical file length 猜 sparse 配置。
10:03:39 的觀測：目前 WSL 2,890.75 GiB、其他 WSL 36.32 GiB、Windows／
其他檔案 851.86 GiB、主機剩餘 35.41 GiB；WSL 客體內可用 194.21 GiB。
主機可分配給目前 WSL 的總量 = 目前 VHD 實體配置 + 主機剩餘，當時為
2,926.15 GiB，**已包含目前占用，不能當作額外可用空間**。未證實的 VHD
內部可重用區塊沒有給額外准入額度；量測非原子快照，未知／過期維持未知。
Windows 休眠、離線 VHD compact 與 WSL resize 沒有在此步驟執行。

官方容量與配置定義：[WSL disk space](https://learn.microsoft.com/en-us/windows/wsl/disk-space)、
[GetCompressedFileSizeW](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getcompressedfilesizew)。
原生探測、原第 15 輪工作量預估、公開快照及驗收收據均在
`artifacts/storage_layout/` 的 `backup-reserve-*20261101*` 與
`windows-wsl-accounting-native-20261101.json`。原 Windows Temp swap owner 的
audit 找到 **0 個可安全回收候選**；目前 WSL 的活動 swap 仍保留。沒有為了
報告「清理」而刪來源或無法驗證的暫存。

本次驗收：planner／runner／queue／archive **214 passed（76.04 秒）**，
monitor **74 passed（3.25 秒）**，offline retirement／storage progress
**371 passed（421.52 秒）**，合計 **659 passed**。最終公開 HTTPS
`storage.js?v=13` 桌面／手機 **17 項 browser cases 通過**，包括原生
Windows／WSL 分帳、主機資料缺漏、過期、稀疏分類、進度卡與展開狀態。
這些是工程回歸的執行時間，不是第 15 輪生產作業的完成時間。
最初 baseline 的 3 筆相容性測試失敗及其修正證據保留，不混入通過結果。
最終 owner evidence 再次核對五個受保護原程序的 PID／start ticks 均未變，
原 queue 仍是 09:46 的同一程序。當次 R15 封存為 8,448／30,073 檔，
2,150,497,730／5,912,353,717 source bytes；本回合**沒有新增已回收空間的
宣告**。只重新載入儲存觀測與唯讀 dashboard 服務，其餘保留原 owner。

### 11/1 11:40：傳輸暫存阻擋與階段分母修正

完整資料流仍為原來源清點／內容重用 → D 固定發布 → 分工完整還原 → 原退休
驗收；NAS 則由原公開／私有派工 → Syncthing → lab203 → NAS 獨立讀回 →
配對 ACK，最後才回收精確傳輸副本。這些 owner 的鎖、完整 SHA、來源引用
與收據不共用完成百分比；原來源退休與傳輸副本退休也是不同交易。

11:19 的原同步 API 顯示 scanner 忙碌，但 local/peer 的所有 pending、error
為零且 peer 100%。原 cache GC 僅允許 idle，D 可用量又低於該派工的原
64 GiB reserve，因此私有下一波約 364.53 GB logical 尚待送。現在明確啟用
`allow_scanning_when_fully_converged`，只放行 scanner 狀態本身；配對 NAS
完整讀回 ACK、原冷來源、完整成員／signature、FD／mmap、掛載及 rename
後再檢查全部保留。刪除通知保留 journal，scanner 忙碌時不持 publication
owner 等第二次掃描；idle 後由原 timer 重送精確子路徑。

原 GC 亦會先走完整份歷史候選，才讓同一個 turn 發下一波。新增完整交易
輪次，每輪成功回收 4 批後接續原派工；其他錯誤候選保留而不擋住後續項。
這是工作交錯，不是 CPU／I/O 佔用上限；既有執行中的交易不強停，下一次
原服務自然載入。原 backup、private、notify、retirement、Temporal 及 Vast
回傳排程均保留；新 worker 由固定候選、原 journals 的自然邊界交接排隊，
只有正式 loaded-code 收據才能證明常駐 worker 已換版。

10 秒非阻塞 py-spy 取樣共 197 個樣本，顯示 GC 主要時間落在 metadata
核對及 journal 序列化。移除同一 proof pass 的重複 leaf lookup，保留各
交易邊界的重新觀測。在真實 D 槽、固定 ACK 來源的 128 檔唯讀 ABBA，
相同來源觀測的中位數為 0.504 → 0.268 秒（約少 47%）。這只量測 metadata
步驟，沒有在 benchmark 重新執行完整 NAS 還原，也不宣稱全流程加速 47%。

面板修正同時涵蓋 catalog 與第 15 輪：routing 用 files_routed，已 hash
檔數另屬後續小檔 packing 的工作，不能除以全檔數估出數年的時間。來源
logical bytes、prepare 的 source bytes、實際 phase items 各用原階段收據；
新版 counter 語義重置速度樣本，不混接舊分母。對小檔尾段同時觀察 bytes／
items，取較慢的可量測投影；後續未知階段不納入虛構的全域完成時刻。

Vast 連線的單次 pre-auth reset 最多重試一次，兩次共用 15 秒期限；認證、
host key、資料格式錯誤及 timeout 不盲重試。失敗保留上次量測及原時鐘。
每分鐘三端卡片與公開唯讀 API 均沿原 observer，沒有 HTTP 端 SSH／掃檔／GPU
工作。三次公開取樣已確認 Penguin、lab203 與 Vast 的個別觀測時間前進。

原 GC 現另提供小型 `stockagent.transport_cache_retirement_progress.v1`
收據：固定候選 identity、當輪批數、complete logical bytes、原觀測時間及
完成證據。觀測端不為刷新畫面反覆解析大型 ledger，亦不以這份衍生收據
取代配對 ACK 或原退休 journal。尚未提供該收據的既有執行者顯示「待回報」，
不顯示假零值、假完成或假 ETA。這類回收不加進 WSL allocated 回收總數。

最終驗證：核心 **381 passed（62.49 秒）**；補上舊執行者缺收據案例後，
觀測／相容性先為 **157 passed（6.78 秒）**；再修正公開投影對「從未收到
收據」誤稱「舊收據過期」的問題，最終 **158 passed（5.96 秒）**。
各組有重疊，不相加為唯一案例數。
公開 HTTPS 桌面／手機 **19 cases** 通過、provider calls 0；relay／offhost
回歸 144 passed、14 skipped 另存，不把 skipped 當通過。只重新載入唯讀
觀測及 gateway；九個受保護原服務的 PID／start ticks 前後一致。

原 GC 已開始逐批退休，11:59:33 固定對帳查得新增 30 批、119,012,169,289
complete logical bytes（110.84 GiB）。這是已驗收的 D 槽傳輸副本，並非 WSL 原始資料
allocated 回收量；第 15 輪仍要完成原還原／退休／acceptance。新輪次交錯、
metadata 重用與小型收據由下一個原 backup turn 載入；本次不停止既有交易。
同時的最近容量觀測為 D 可用 157.61 GiB、WSL 可用 191.27 GiB、Windows C
新增配置餘裕 35.18 GiB；各自有原觀測時鐘，並非原子快照。原始來源及 NAS
snapshot 在這批傳輸回收交易的刪除數均為 0。

證據均位於 `artifacts/storage_layout/`：
`trinode-cache-progress-accepted-launch-20261101.json`、
`trinode-old-owner-progress-fallback-launch-20261101.json`、
`trinode-latest-public-browser-20261101/browser_acceptance.json`、
`trinode-progress-final-deployment-v2-20261101.json`、
`trinode-source-metadata-proof-ABBA-20261101.json`、
`trinode-minute-public-witness-20261101.json`、
`trinode-transport-retirement-before-20261101.json`。
最新實際進度另存 `trinode-backup-final-evidence-v2-20261101.json`，前一份
`trinode-backup-final-evidence-20261101.json` 保留；持續任務不
因本次工程回歸通過而被標為全歷史已完成。

11:56 原 worker 交接因現行 publication 尚未結束而到達等待期限，並非
強停交易。已登錄的固定候選接續由原交接器等待自然邊界，逐次釋放未取得
完整 owner 集合的鎖。正式 loaded-code 收據尚未產生之前，常駐 worker
仍按原版本回報，不能把磁碟上的新版 SHA 當作已載入證明。

lab203 Codex 交接：現有接收／備份不用重啟。若要補 CPU／RAM 與 NAS
逐階段 ETA，請沿本文件前述「NAS 分階段進度補件」保持原 owner 及原
簽章配對，回傳固定 identity 樣本；Penguin 目前只能證明新鮮簽章回執
在線，尚未取得接收端 CPU／RAM。這項資訊補件不阻擋現有同步和驗收。

### 11/1：每分鐘監測與進度／驗收時間分開

13:06 公開 API 的觀測時間正常前進，但已登錄 NAS 的計數時間仍是 11:22、
整體遷移計數仍是 10:56。原投影把這些「只在交易或階段更新時才變更」的
時間一律套用五分鐘過期判斷，所以每分鐘重新讀取後，卡片仍顯示「收據已過期」。
固定完成證據的日期也被標為 stale。這與觀測服務是否每分鐘工作是兩件事。

原 collector 現在逐輪寫 `checked_at_utc`，只表示本次有重新檢查狀態；
原 `observed_at_utc`、計數、驗收證據及活動時間不改寫。已存在的 systemd
排程查詢與本輪 runtime 提供 `owner_state`，不增加 HTTP 端探測或 payload
掃描。卡片區分監測過期、等待原始收據、等待新計數、前置條件與執行者異常，
並直接列出本次檢查和進度／驗收原始時間。重讀同一收據不增加速度樣本，
舊計數的 ETA 繼續停估；固定版本的原完成證據保留完成狀態，不能用新檢查
時間刷新任何恢復、刪除或接收 gate 的有效期。真正的觀測失敗仍保留上次
時鐘並告警，沒有延長原過期門檻。

前景頁面的 60 秒輪詢不再因儲存區塊捲出畫面而停止。隱藏頁籤仍遵守
瀏覽器的背景節流，切回時立即重新讀取。`storage.js?v=15` 僅取原唯讀
快照；原來源 API、下載器、驗證及當沖執行者均未重啟。

驗收：`minute-storage-freshness-regress-20261101T051236-8bfcac39` 為
**167 passed（7.28 秒）**；公開 HTTPS 桌面／手機 **23 cases 通過**，
包含虛擬時間前進一分鐘、儲存區塊在畫面外仍刷新、原收據時間保持不變、
固定驗收不誤判過期，以及讀取失敗保留上一份資料。這些瀏覽器 fixture
明確標為非生產驗收，另以真實公開 API 連續觀測驗證時鐘。

證據：`artifacts/storage_layout/minute-storage-freshness-deployment-20261101.json`
確認七個受保護常駐程序的 PID／start ticks 不變；
`minute-storage-freshness-browser-20261101/browser_acceptance.json` 保存
23 個瀏覽器案例，`minute-storage-freshness-public-witness-20261101.json`
逐筆保存公開讀回、本次檢查與原進度時鐘。NAS 尚未提供的 CPU／RAM 與
逐階段工作量仍標示未知，不因本次時鐘修正而填入推測值。

真實跨分鐘讀回已通過三次：13:16:26、13:17:23、13:18:25 的公開快照均有
15 張工作卡片，每張 `checked_at_utc` 前進、監測 stale 為 0；其中 6 張仍
保留舊計數並標示待原工作回報。Penguin、lab203、Vast 的各自來源時鐘也均
前進。這不是把舊進度時間刷新成現在，亦不是全歷史備份完成證明。完整
索引與驗收檔 SHA 在 `minute-storage-freshness-final-evidence-20261101.json`。

### 11/1：完整排程、阻礙與受控路徑修復

原本只投影七組 timer，現在由同一 observer 的兩次有界 systemd 查詢
列出 **27 組已登錄備份／回傳／保全／整理 timer，加上一組常駐 lifecycle owner**。
每組保留實際下次檢查、上次觸發、是否啟用、執行狀態、上次結果與退出碼。
來源封存、原值還原、WSL 回收、同步、NAS 獨立讀回及傳輸暫存回收的六個
階段，另列各固定版本的接續條件；其他已封閉版本仍可並行，不是全域串行鎖。
排程僅觀測原 owner，不新增下載、備份或刪除執行者。

四個既有 owner inode 的 `/proc/locks` 觀測顯示真正 holder 與等待數；
藉固定服務的 MainPID 與有界 ancestry 辨識其子程序，不發布 PID、程序參數或
檔案路徑。檔案存在但無 kernel holder 時不算忙碌，鎖等待也不當成死鎖證明。
目前阻礙區列出原服務失敗、排程停用、來源相依、舊計數與鎖等待的接續說明。
同一新鮮觀測中的真實前置問題，優先於一般「等待新計數」提示。
API 讀取失敗時，前端將舊排程、流程及鎖觀測一起標為過期，不能繼續宣稱
舊常駐 owner 或舊等待者仍是即時狀態。HTML 使用 `storage.js?v=18`。

封存卡片另列原 `files_completed/files_total` 與原始內容 bytes，讓單一 pack
尚未封閉時仍可看見逐檔進展。它們與封閉 pack 計數重疊，不相加，也不代表
已傳輸或 NAS／原始內容還原驗收。速度與完成時間仍依原 phase／generation
與計數，不把原 owner 在線或重讀同一收據當成工作增加。

13:36 的第一輪完整排程投影揭露兩項部署後路徑錯誤：永豐 inventory cache
及原 physical-minute／completed-forward compaction policy 均未先處理受控的
`artifacts/live`／`artifacts/cache` placement alias。因此，即使位元組未變，
原本的嚴格 no-follow 檢查仍拒絕了合法移位。修正沿用
`canonical_service_path` 只轉換已登錄 alias，保留 cache 私有權限、未知 target、
子目錄 symlink、原 ABI／SHA／consumer／writer-lock 等既有檢查。
未修改 factorized 產生器、當沖帳本或任何來源數值。

原兩個失敗 oneshot 已各自重新執行：去重排程退出 0；兩個服務 cache 的
原結果均為 compacted、零額外 allocated bytes 回收、零 logical path 刪除，
未把已共用資料再次算成節省。永豐排程也已退出 0，成功寫回 6,096,384 檔、
147,735,174,562 bytes 的新清冊；這一輪完整掃描為 532.58 秒，不是每分鐘監控成本。
原清冊的開始時間與掃描耗時保留，不能用完成時間回填歷史內容時鐘。

開機稽核仍退出 75：FinMind 原先暫停的三個 owner 不可用，且 lakehouse
仍待原 worker 自然邊界載入已驗證版本。保留原失敗 gate；不為了清除網站
告警而啟動大量下載器、殺掉 publisher 或改寫驗收。原已固定的換版 run
`trinode-tested-worker-handoff-v3-20261101T034912-be8165ba` 仍等待原發布鎖，
未宣稱新版已載入或 Vast 已處理本次未封閉版本。

驗收與量測：

- 最終相關語意／公開投影：**175 passed、189 deselected（6.69 秒）**，
  `full-storage-schedules-final-reg-20261101T054226-fcbf5db8`。
- 路徑及 compaction 相關 **104 passed（8.46 秒）**；原 service placement
  **10 passed（1.20 秒）**，包含合法 alias、未知 target 與子連結拒絕。
- 最終 Chromium 桌面／手機 **25 cases 通過**，含真實頁面、每分鐘離開可視
  區塊仍刷新、完整排程、逐檔封存計數、安全文字、展開／焦點保留及斷線
  不再沿用舊運行狀態。fixture 不當作生產資料證明，使用 CPU-2D、零 provider call。
- 真實 HTTPS 13:39:02、13:40:03、13:41:01 三輪快照的三端來源時鐘均前進；
  每輪有 28 組 schedule、六階段，工作 metadata checks 無 stale。
- 新排程／鎖觀測的三次本機完整量測為 **0.564、0.452、0.292 秒**；範圍僅
  兩次 systemd 查詢與四個 inode，不掃來源樹、不 hash payload、不查 provider。
  不將此數字當成整個備份或 NAS 驗證的加速倍數。

R15 已於 12:48:58 通過原退休／驗收，實際釋放 5,990,043,648 allocated bytes；
目前自動佇列登錄 R12／R13／R15 共 17,666,887,680 allocated bytes。
相對 11:25 固定 baseline，13:38 原 journal 已新增完成 70 個傳輸回收交易，
合計 236,000,349,539 logical bytes；這是 **D 傳輸暫存**，不是 WSL allocated
釋放量，也不是原始 D／NAS archive 刪除。已審核的三個 WSL cohort 均已完成，
不依目錄名稱自動發現新的刪除根；其餘來源仍由原遷移與回收流程處理。

主要證據位於 `artifacts/storage_layout/`：
`full-storage-schedule-deployment-v2-20261101.json`、
`full-storage-schedule-minute-witness-20261101.json`、
`full-storage-schedules-offline-browser-20261101/browser_acceptance.json`、
`full-storage-schedule-observation-cost-20261101.json` 及
`full-storage-schedule-retirement-observation-20261101.json`。
每次觀測／網站部署均核對七個受保護常駐服務 PID／start ticks 保持不變。
完整版本 hash、原 oneshot 結果與最後實際公開讀回由本次 final evidence 索引保存。

lab203 的 CPU／RAM、完整遠端 timer 及 NAS 分階段分母仍未提供；來源的
待封閉 packs 和後續驗證尚未結束，因此全域完成時間維持未知。
[lab203 接手文件](lab203_minute_schedule_handoff_2026-11-01.md) 要求沿既有回執
交回固定樣本後再驗證 reader，不擅自擴充嚴格 `pipeline-status` v1 ABI，
也不阻塞原同步／NAS／ACK／回收工作。
