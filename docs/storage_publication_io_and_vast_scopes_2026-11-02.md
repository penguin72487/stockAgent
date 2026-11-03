# 來源封存 I/O、Vast 分工及備份容量

觀測範圍：2026-11-01 深夜至 11-02 凌晨，Penguin 原生命週期 owner、原
Syncthing／lab203 回執通道與已登錄 Vast。這是工程優化及特定工作驗收，
不代表全部歷史可恢復、全部來源已刪除或硬體吞吐已達上限。

## 目前最花時間的地方

本輪 61 個來源中有 59 個保留原還原驗收；主線 `tw-index-futures` 仍在
封存 4,601,669 個來源檔、469,259,065,729 原始 bytes。16:15:56 UTC 的
原進度為 1,078,765 檔、333,729,338,251 原始 bytes，3／16 個分組。
分組內資料量差異大，不能用 3／16 推算全部剩餘時間。

15 秒、50 Hz 的非阻塞取樣中，publisher 的 745 個取樣全落在
`desync_snapshots.sha256_file`；相鄰 10 秒 I/O 取樣約 6.57 MB/s 實際讀取、
0.33 個 CPU core。這證明該取樣階段受封包重讀影響，不代表整段多小時
工作均在重讀，也不是 CPU 已滿。Syncthing 同時有其他讀取，不能將兩個
時間窗的速度當成獨立硬體極限。

ZIP member 的 header 回寫跨出 8 MiB 緩衝窗，會使邊寫邊 SHA 失效，原流程
因此安全退回完整重讀。修正於 member 開始前使用保守大小提示換窗，正常
情況只順向寫入；提示不足或大型回寫仍保留原完整 SHA fallback。封包格式、
原值、模式、CRC、來源世代及還原條件不變。

## 實測及載入邊界

`scripts/benchmark_pack_windows.py` 使用 4,096 個真實檔案、36,980,294 原始
bytes，在 D 執行舊／新／新／舊完整流程：封存、必要 SHA、獨立 encoded SHA、
所有 member CRC 與原值 SHA。四輪 encoded bytes 都是 17,038,248，SHA 都是
`723aff1a050e3471ee6b3595d004797594a21cafa480f6acc55c26f07d4cca38`。

| 完整小樣本流程 | 秒 |
| --- | ---: |
| 舊一 | 6.7131 |
| 新一 | 4.5936 |
| 新二 | 11.1613 |
| 舊二 | 14.6311 |

平均 10.6721 → 7.8775 秒，耗時降低約 26%；背景 I/O 差異明顯，不能外推
全部歷史的加速倍數。新流程兩輪均沿用 fused SHA，獨立還原驗證照常執行；
benchmark 暫存已於核對後清除，原始資料未修改。

目前正式來源發布尚未在每包持久化全部來源提示，現在重啟會損失記憶體內
工作。因此沿原 rollout 固定 operation、plan 及程式 identity，等待
`tw-index-futures` 的精確 snapshot／manifest 提交才載入。
`storage-pack-window-owner-handoff-20261101-waiting.json` 是等待證據，
不是已載入或完整還原證據。程式或 operation 改變會拒絕該次交接。

## Vast 分工及自動排程

原 spool 只搜尋 `/srv/stockagent-packed/.local-state/private-history`，當時
正在寫入的正式來源位於 `/srv/stockagent-packed`。已在同一 queue owner
明確登錄兩組 root／dataset，保留舊 checkpoint，使用獨立 context checkpoint。
不以相同檔名、mtime 或舊成功計數替代完整 encoded SHA、來源世代、boot、
worker 及清冊綁定。

11-02 00:15 台北時間，私有來源候選 192／192 包、155,368,406,610 bytes；
正式來源 64／96 包、23,571,959,567／33,210,494,515 bytes，失敗 0。
候選分母會增加；這是預驗證，沒有 NAS 或來源回收授權。

兩個遠端工作槽將順序 D 讀取／上傳與遠端解壓重疊。每回合最多 32 包，
有進度且還有候選就立即接續；無進度、受阻或清空才交回每分鐘 timer。
服務不再用固定 30 分鐘牆鐘中斷健康的大包回合；每項 SSH／遠端請求仍有
原有期限、鎖與容量檢查。Vast 暫存保留七天的既有條件不變。

預驗證不需要 GPU。提高 GPU 占用率或增加遠端槽數，不能省掉必要的本機
原檔讀取及 D 寫入；先前四槽全流程未呈現穩定獲益，保留該負面實測。

## 控制資料庫備份與容量

三個控制資料庫實測合計 228,500,541 bytes，原備份因 D 剩餘低於 64 GiB
而失敗。依使用者「保留空間就是給備份用」的指示，僅這項 SQL 備份明確
登錄使用保留池，保留既有 native D writer 的 32 GiB 底線；一般來源的
64 GiB 保留、NAS 配對及回收條件不變。

先查實際 DB 大小，以兩倍實測量加 16 MiB 做初始准入；每份 dump 在寫入前
再按實際 dump 及 logical-state 大小檢查 D 實體餘量。容量不足變成原 timer
可重試的等待；若並行寫入吃掉餘量，保留並記錄尚未發布的 staging，不清除
先前固定 dispatch／ACK。原相同 MVCC snapshot、逐表 fingerprint、封套
全 SHA、簽章派工及獨立 NAS 讀回不變。

原備份服務已完成新封套 `5e1646b670e2672ae814f4a1b798a5c9caa1e639ad7e14781c20cc4c4e5c1e77`，
11 檔、26,457,295 bytes，狀態為 `waiting_encrypted_nas_backup`。
這僅證明新 SQL 備份產生及派工，舊封套 ACK 不能驗收這份新封套。

## 網站與空間回收

全資料監控每 60 秒更新 Penguin、lab203、Vast 的觀測及原排程清冊；保持
observer 時鐘、原工作進度、候選清點時鐘及驗收時鐘分開。lab203 目前回執
未提供 CPU／RAM，網站保留未知，不以在線推測負載。

正式與私有 Vast 候選分開顯示；目前候選做完顯示「等候新封包」，不再暗示
整份來源完成。來源分組內採逐檔／bytes 進度估當階段時間，封存提交、完整
還原及 NAS 後續缺少可靠速度時維持待估。新增 SQL 備份的容量及交付狀態。

原安全清理服務本次未找到新增可回收項目，**本次新增 WSL 刪除量為 0**。
已登錄第 12／13／15 輪累計驗收 17,666,887,680 allocated bytes 是既有成果，
不是本次新釋放。已驗收傳輸副本的 11,814,669,969 logical bytes 位於 D，
不能再算成 WSL 回收。當沖、期貨行情、資料庫與原儲存 owner 保持原程序，
未以強制解鎖、清 pin 或刪除未驗收原檔換取空間。

Vast 訓練救援仍因 D reserve 被原服務拒絕；開機稽核也仍列出三個不可用的
FinMind owner。因此不能宣稱所有服務已恢復。沒有為了消除稽核失敗而開啟
下載、變更訓練救援範圍或降低一般容量門檻。

lab203 每分鐘回執持續收到；CPU／RAM 與逐工作階段分母仍需原接收端提供。
可沿既有交接 [每分鐘排程補件](lab203_minute_schedule_handoff_2026-11-01.md)
處理；此補件不阻塞現有 NAS 接收及驗收，也不另開管理通道。

## 可追查驗收

- Pack 窗口／prefetch／checkpoint：377 passed。
- 原 dataset migration／workflow／recovery／handoff：509 passed、8 skipped。
- SQL 容量／DB 邏輯／spool／BackupStream：151 passed、3 skipped；skipped
  的 PostgreSQL 整合項目另以原服務實際 dump 與封套派工核對。
- 最終 monitor／runtime／spool／容量：213 passed。先前錯誤使用不含 NumPy
  的控制環境、以及 census 測試的真實 subprocess 污染假 sleep，保留失敗
  run；改用專案資料環境及隔離該測試外部 probe 後通過，未放寬正式驗證。
- Chromium 桌面／手機 31 項驗收、跨三個每分鐘週期的公開 API 讀回與受保護服務
  PID／start ticks 比對見最終固定快照。

讀回測試保留了兩種負面結果：gateway 重新載入期間一次 HTTP 502，以及
在每分鐘週期邊界提前約一秒取樣，尚未讀到下一輪 producer 時鐘。最終
跨三個週期以 70 秒間隔核對三端原時鐘，網站及 producer 排程仍維持 60 秒；
沒有把舊進度改成現在，也沒有以測試重試抹去故障紀錄。

00:27 台北時間的固定驗收快照：正式來源 1,131,476／4,601,669 檔；Vast
兩個範圍分別 192／192、111／115 包，失敗 0，同一遠端佇列程序已自動
進入下一回合。當階段來源核對的中央估時約九小時，因檔案／封包大小與
間歇 I/O 差異很大，快慢觀測範圍很寬，不能當成整體備份或 WSL 回收期限。
WSL 虛擬檔案系統可用約 196.05 GiB，C 實體可用約 32.46 GiB，D 約
52.10 GiB；三者不能相加，也不能用 WSL 虛擬可用量取代 C 實體餘量。

主要證據：`artifacts/storage_layout/storage-publication-io-acceptance-20261102.json`、
`storage-pack-boundary-abba-20261101.json`、`storage-live-publisher-profile-20261101.txt`、
`storage-publication-io-minute-v2-20261102.json`、
`storage-publication-io-browser-v23-20261102/browser_acceptance.json`。
