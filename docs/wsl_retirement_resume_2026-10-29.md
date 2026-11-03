# WSL 回收續跑與收據掃描：2026-10-29

這是 08:47–08:56 台北時間的觀察，不是全部資料已備份、已刪除或硬體極限
驗收。持續沿用 `storage-single-authority-20261025` 和既有 retirement、
publication、backup owners；當沖服務維持最高優先權。

## 空間與目前波次

- 08:56 WSL ext4 可用約 308.6 GiB，Windows C 實際可用約 67.9 GiB，D 可用約
  352.3 GiB。WSL 的邏輯可用量不等於 Windows VHDX 的實體承載餘量。
  一般下載的 64 GiB 門檻不是獨占預留，其他寫入仍會改變可用量。
  回收 ext4 檔案會先增加 WSL 可用量；現有 VHDX 並非 sparse，不能把
  這等同於 Windows C 立即增加相同空間。既有 `fstrim.timer` 仍啟用；
  Windows 壓縮工作只在 WSL 已停止時執行，不會為了此任務停止服務。
- Round 7／8 先前的兩份退休收據合計回收 27,516,260,352 allocated bytes，
  約 25.63 GiB；這不是本次新增的刪除量。
- Round 10（固定清冊候選 11.09 GiB）正在原 owner 的
  `prepare-exact-source-archive`。既有程序沒有提供階段進度；08:54:44
  和 08:56:11 唯讀列出 D 暫存的收據檔名，分別為 3,231 和 3,426，
  凍結的 source plan 共 40,510 筆。後者約占 8.46%，但只是檔名數，
  不代表每張收據驗證成功或已通過後續還原／退休。約 87 秒增加 195
  筆，依這個短樣本線性外推，僅目前封存準備還需約 4.6 小時；工作負載、
  檔案大小及來源條件會變，這不是可信的整輪完成時間或時限保證。
  Round 11（候選 12.48 GiB）在既有 scheduler flock 等待。
  Round 9（候選 7.13 GiB）和 Round 6（候選 9.20 GiB）之前未退休，
  本次已掛入受監督的原排程路徑：
  `wsl-round9-6-owned-resume-202610-20261029T004000-8a221d07`。
  其日誌已顯示 `waiting_for_scheduler`；等待期間不持有該鎖。兩個等待者
  的先後取決於鎖的實際交接，Round 9 與 6 在自身工作內串行。
  39.90 GiB 是四份固定清冊的候選總量，並非已回收量，也不保證最終
  可回收量。實際刪除仍以各自 receipt 和後置核對為準。
- `tw-public` 固定 D 版本已完成獨立原始檔重建：210,267 檔、
  20,737,908,836 bytes；同一版本的 4,112 冷物件為 35,976,707,808 bytes。
  本證據只涵蓋該固定 D 版本，並不代表 NAS 驗收或來源已退休。
  新聞 30／30 波已在本機封閉發布，`NAS_restore_ACK_verified=false`；
  所有資料是否已可從 lab203／NAS 還原仍未證明。

08:53 已另處理 `/var/tmp/stockagent-sourceplan-tests-20261028` 下的
`test_adaptive_large_binary_com0`，沒有整批刪除中止測試目錄。這個個別
fixture 的三個來源檔與對應測試程式產生的常數逐位元組一致，測試用
封存及還原也獨立通過；內部只有清冊列出的 32 個檔案，沒有外部硬連結、
符號連結或現行程序引用。固定清冊後隔離，重核檔案身分與 SHA，再清除；
收據記錄回收 42,696,704 allocated file bytes（約 40.72 MiB）。其中的
「冷庫」只是這個測試建立的合成目錄；正式來源、D 正本及 NAS 快照皆未
刪除。這是本次新增的暫存回收，不是上述四輪回收。其餘未完成辨識的
中止測試暫存繼續保留。

清冊與收據位於
`artifacts/operations/storage-single-authority-20261025/test-fixture-retirement-20261028/interrupted-adaptive-synthetic-20261029.{inventory,retirement}.json`。
首次檢查發現 pytest 的 `current` 是絕對連結而非預期的相對連結，
因此在任何 rename／刪除前停止；核對它確實指向同一 fixture 後才重跑。

## 阻塞與修正

Round 9 的固定 D 原始檔還原與完整審核曾通過，但 rename 後 transport
gate 未通過，來源仍在原交易擁有的 quarantine，不是已刪除。原 wave
planner 只看原始路徑是否存在，將合法中斷續跑誤判成來源消失；修正後只
依凍結清冊、原始計畫、state 和 quarantine 的固定身分辨識候選。這只是
派工觀察：owner 仍重新核對固定冷版本、完整原始資料、進程引用、
transport 和最終 unlink gates。沒有刪除 lock file，也沒有繞過 owner。

等待中的 wave 以前會在真正等 scheduler 前就因既有 owner 的活躍紀錄
而退出；現在若 scheduler 確實忙碌，先固定 inventory／publication 身分，
不持鎖等待，輪到後仍重查身分、重疊、空間與所有原 owner gates。無法
觀察或不相符仍暫緩；不把等待鎖本身稱為死鎖。

Round 6 先前封存準備報一個 D payload `ENOENT`，其後檔案存在且與其
原始來源逐位元組一致；舊 failure receipt 沒有 traceback，不能確認是
哪個操作及底層原因。新程式對 guarded D 剛封存的**同一路徑**，只在
雜湊／stat 讀取遇到 `FileNotFoundError` 時做最多三次短重讀；其他路徑、
其他錯誤或持續缺件仍立即或最終失敗，成功仍需實際 SHA 和後續完整還原。
新 failure receipt 記錄錯誤類型、errno 與呼叫位置，不記錄 frame 變數。
這個修正不保證能處理所有來源的 ENOENT。

08:48 的 Syncthing 實際探測顯示本機 folder 正在 scanning，need bytes、
need items、need deletes 和錯誤皆為零，該 gate 未設定必需 peers。
這不證明 Round 9 當時只因掃描而失敗，也不是 NAS 收據。續跑仍等待新的
transport proof，並在刪除前重新檢查。

## 已量測的效能與可觀測性

第 6 輪固定 source plan 有 56,320 個檔案，已知缺的 per-file 收據對應
第 28,326 筆。之前完整 manifest 恢復會依序檢查先前的收據；guarded D
新路徑以最多四個執行緒、32 筆為一組讀取，仍按原 source-plan 順序
消費結果。每份收據用 no-follow fd、fstat、大小限制和前後檔案身分
核對；缺件、損毀、資料改變、冷還原及刪除前 gates 的原條件不放寬。

唯讀測試直接使用第 6 輪相同的前 32 份現有收據，A/B/B/A：原串行
23.371 秒、新四執行緒 2.118 秒、新四執行緒 1.514 秒、原串行
17.529 秒；四次回傳的有序內容完全一致。兩組平均相差約 11.3 倍，
但樣本小、當時負載有波動，僅涵蓋收據讀取，不能推算整輪 ETA，
也未證明已達硬體上限。正在執行的 Round 10 舊 Python process 不會
熱載入；排隊的後續原 owner 啟動時才會讀取新版本。

後續 owner 會在 `preparation-progress.json` 分別記錄中斷收據掃描和
封存／重驗的已處理檔案與總數；封存另記來源 bytes。這是觀察值，
`acceptance=false`，不代表後續原始檔還原或退休完成。Round 10 不因
新增觀測而被中途重啟，避免丟棄已完成工作。

最新聚焦回歸為 75 passed；原 owner 進度落盤及符號連結凍結、四 worker
中斷續跑和回收計數另 4 passed。稍早的整合測試在盤前為避免持續掃描大量現行 `/proc` 而
主動中止，當時 3 passed，未完成的部分不計為通過。現有測試與小樣本
不替代真實回收 acceptance、NAS ACK 或開盤後當沖服務驗收。

上一次 Round 9 的獨立 D 原始檔還原約 42 分鐘、完整審核約 47 分鐘；
排隊等待、檔案組成與其他工作負載均會改變，現時不能給可信的整體
完成時間，也不能把舊階段耗時當作本輪保證。

## 10:16 重開後的續跑與盤點鎖交接

WSL 於 10:16:33 重開，之前三個 transient agent run 不再存在；舊版
`running` phase 不是目前活躍的證據。以新的 boot ID、systemd unit、
PID 世代、固定清冊 SHA 及既有 quarantine 檢查後，將三個舊 run 對帳為
interrupted，沒有虛構退出碼或完成收據。Round 10 的 D stage 當時有
10,935 個 receipt 檔名，固定 plan 共 40,510 筆；這只是中斷現場的檔名
數，不是已驗證或已退休數。對帳證據是
`artifacts/operations/storage-single-authority-20261025/wsl-retirement-reboot-reconcile-20261029.json`。

重開後的唯讀儲存面板 `du` 持有 retirement scheduler 共用鎖，掃描最多可
等待 30 分鐘，期間新的回收排程無法取得獨占鎖；這是長時間鎖競爭，
不是已證實的死鎖。已精確核對該 `du` 的 PID、父程序、boot ID、start
ticks、cgroup 與命令，僅終止並等待這個唯讀掃描子程序，留下
`readonly-monitor-scan-handoff-20261029.json`；沒有刪 lock inode、
回收 owner 或資料。修改 `storage_monitor.py` 後，共用鎖下的本機盤點
會在排定回收時主動停止自己的掃描，等待子程序退出才交還鎖；已完整
輸出的目錄作為 lower bound，沒輸出的盤點則保留先前資訊。重新載入的
storage-monitor 在回收執行時會讓路。終止後的 10:37 左右該服務 cgroup
約 39 MB；舊掃描時約 17 GB，大部分是 file cache 和 slab，兩個觀測值
不能直接相減當作永久釋放的實體 RAM。

互動 shell 的 D 掛載 `msize=65536`，PID 1／正式服務命名空間是既定的
`msize=8192`。Round 11 因此以正式命名空間重新唯讀預檢，固定 D manifest
SHA 為 `c6e57faae4dfa524a004d1a3133119484d11d8a191c16e6e81498d0ea6f01a70`，
無預檢 deferral；沒有更改掛載或繞過安全檢查。第 9、6、10 輪已依序
進入既有 owner 的 supervised 排程，第 11 輪另外等待同一 scheduler 鎖：

- `wsl-reboot-round9-6-10-resume-20-20261029T023420-1e8182bf`
- `wsl-reboot-round11-fixed-resume--20261029T023641-e8fef54a`

10:40:48 的 Round 9 在 `independent-cold-reconstruction` 的
`verify-materialized-files`：8,156／40,419 檔；前面的獨立冷物件重建
已逐項跑到 40,419／40,419，這個階段尚未完成，更不是退休 acceptance。
最近 128 秒由 4,135 到 8,156 檔，約每秒 31.4 檔；若速度及後續檔案
組成不變，此單一階段約再 17 分鐘。這只是短期線性外推，不包括接下來
的解碼、完整樹／來源、運送和最終刪除 gate，不能推算四輪完成時間。
10:40 同步觀測為 scanning，need 與錯誤為零但 `folder_idle=false`，
所以不宣稱已通過最終 transport gate；也不是 NAS 的獨立還原 ACK。

10:43 的 Syncthing 個別資料夾唯讀狀態：`stockagent-packed` 和
`stockagent-backup-ingress-lab203` 均自 10:16:44 處於 scanning；
兩者的 need items、need bytes、folder／pull errors 皆為零，但不等於
掃描完成。接收 lab203 回執的 `stockagent-backup-receipts-lab203` 當時
為 idle，沒有目前的 folder error；其 journal 自 10:16 起記錄 63 行
失敗或重試訊息，包含頻繁更新的 `storage-relay-audit.json` 暫時
`pull: no such file`，之後有成功同步紀錄。這是另一個資料夾的歷史
瞬時錯誤，不可將當下零錯誤解釋成從未失敗，也不能把回執 folder idle
當作所有 NAS 快照已完整驗收。lab203 ingress 索引有 814,422 個
deletion records；此觀測尚不足以證明它是長掃描原因，不刪 Syncthing
索引或 tombstone。10:44 的 Syncthing `FolderScanProgress` 對 ingress
回報 current 8,290,261／total 15,080,938 bytes、rate 0；這是掃描
事件當下的 bytes 計數，不是 NAS 全部檔案或回收的進度。短暫訂閱期間
沒有取得 packed folder 的進度事件，不能判定其已卡住。兩個掃描的
剩餘時間目前沒有可信的完整工作量分母。

使用正式掛載命名空間，以 Round 9 同一批 256 個 D 編碼檔做唯讀
4／8／8／4 執行緒完整 SHA、大小及前後 generation 檢查，耗時
12.126／2.166／1.419／1.894 秒，結果一致；首輪冷讀和後續快取差異
很大，不能證明提高內容驗證工作者可加快整輪，因此維持既有設定。
當時另有獨立研究工作共用機器；沒有中斷該工作或當沖服務。

另對 `/var/tmp/stockagent-sourceplan-tests-20261028/`
`test_new_compression_profile_p0` 單一案例完成辨識、清冊、合成來源
逐位元組確認、封存逐檔驗證、程序引用檢查與隔離後再驗證，刪除其
11 個測試檔及精確的 pytest `current` 連結，收據記錄檔案 allocated
bytes 共 26,370,048（約 25.15 MiB）。清冊與收據為
`test-fixture-retirement-20261028/interrupted-compression-synthetic-20261029.{inventory,retirement}.json`；
不含任何正式來源、冷物件或 NAS 快照。四輪 39.90 GiB 仍只是候選，
本次尚無其完成退休收據。

儲存面板、波次 planner 和 scheduler 共 126 項聚焦測試通過；第一次由
互動 shell 跑 scheduler 單元測試時，有 16 項受實際 mount namespace
檢查擋下。測試現在只為假 owner 的單元環境固定 namespace 観測，並
保留異常 namespace 必須拒絕的獨立測試；正式執行檢查沒有放寬。
仍需看各波次的完成收據及 lab203／NAS 獨立還原證據。
## 11:34 補充：掃描重試、隔離稽核和 NAS 背壓

第 9 輪固定 D 版本的獨立原始資料還原在 11:09 完成，耗時約 2,067 秒；
其後的完整隔離區稽核仍在運作。8 秒 `/proc` 取樣增加約 5.69 MB
`rchar`、868 次讀取系統呼叫，僅證明當時有活動，無法推算整輪 ETA。
後續輪次的隔離稽核新增細階段與檔案／bytes 進度；這只是觀測，不是
退休 acceptance。現行 process 不熱載入，也不為此重啟。

D packed 的 `training-dataset-recipes` 版本清單有 2,403 個檔案；舊
全目錄掃描兩次超過 120 秒後呼叫端逾時，而 Syncthing 仍繼續掃描。
重試專用的單次等待上限改為 480 秒，`scanning`／`scan-waiting` 均不
重送。D 掃描與 NAS 通知各由原 owner 平行執行；timer 的完成後重查
由五分鐘改為一分鐘。11:33:55 的實際重試在 56.68 秒後收到本機 scan
ACK，pending 1 → 0；這不是 peer convergence 或 NAS 驗收。

新 immutable release 將確切 manifest 路徑持久記入原 queue，並依
object → manifest → head 的順序請求掃描；Syncthing 可能因自身索引狀態
擴大掃描範圍。併發新發布保留合併後的新版 generation。舊 receipt、
損毀或無法證實 manifest 範圍的意圖仍做完整
目錄掃描。Syncthing 2.1.7 的隔離 WSL ext4 測試，以 2,403 份合成
manifest 各更新一份，目錄／精確檔案／精確檔案／目錄依序為
0.6208／0.0091／0.0097／0.5878 秒，四次都以索引讀回驗證更新；
這不代表 D 槽或整輪回收亦有相同加速。121 項聚焦測試通過，另有
一項真實 quarantine resume 單例通過；較大的整合測試因 `/proc`／D
爭用而中止，43 項已通過，不宣稱整套通過。

11:39 的正式 `training-dataset-recipes` 發布回執確認，新版本重新使用
10,019 個檔案、異動 10 個，新增傳輸量 1,722,428 bytes；其 durable
scan queue 實際包含該新 manifest 的確切路徑，`full_manifests_scan=false`。
11:40 的 retry 在 6.76 秒收到本機 scan ACK，pending 1 → 0；該次記錄
仍明示 peer convergence 及 release verification 未檢查，NAS 通知則因
另一個 folder 正在 scanning 而延後。不能拿不同內容或快取條件的單次
數字直接估算整體加速。兩版 minute recipe 比對僅有 `code_files` 差異，
因其現行合約綁定全部 repository Python 程式；有其他工作同時修改程式，
因此新 recipe 是真實身分變動，不能當作時間戳假變動刪除或忽略。

lab203 ingress 的 admission 仍有背壓，私人歷史及 NAS 全歷史未驗收；
接收端檢查事項見 `docs/lab203_storage_backpressure_handoff_2026-10-29.md`。
