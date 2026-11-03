# WSL 離線工作副本：重開後裝置編號與續跑

2026-10-29 約 14:00–14:24（台北時間）的本機觀察，並非其他時間的容量快照或 NAS 驗收。

第 9 輪已有原 owner 的完成收據，WSL 退休的 allocated bytes 為 7,653,269,504；D 端該輪暫存另有 1,557,876,736 allocated bytes 的回收收據。WSL 檔案系統重用空間與 Windows C 槽實際可用容量分開計算，不能假設刪除後 VHDX 立刻縮小。

第 6 輪舊清冊有 56,660 個條目，第 10 輪有 40,554 個；分別和目前完整 metadata tree 比對後，每個條目的唯一差異都是 `st_dev`，從 2096 變為 2112。舊 stage 的 56,320／40,510 筆檔案計畫亦只有 device 變化。第 6 輪的三個符號連結與舊計畫目標完全一致；第 10 輪沒有連結。這只是 metadata 觀察，不能證明內容、冷庫或退休。

修正僅允許人工選定且受保護的 D stage，在整份舊計畫和新觀察均只有一致 device 重編號時接續；原 `source_plan.json`、`symlink_plan.json` 與舊收據保留，執行時以新 observation 做即時來源 fences。連結目標及其他所有欄位必須一致，任何部分或混合重編號均拒絕。後續仍須全量原始值 SHA、原 D 冷庫獨立還原、來源及使用者檢查、同步和退休驗收；不以本判定作為刪除依據。

修正後另對兩棵現存樹呼叫新的純 admission 比對，分別回報 56,660／40,554 筆一致及 `source_bytes_verified: false`；正式 owner 必須重做全部使用者、程序和內容門檻。完整 archive／link／manifest 相關回歸為 153 項通過；之後加入的五個重編號拒絕案例與局部接續測試另行通過，並非真實資料退休驗收。

目前已觀察到第 6 輪有 28,351 個、第 10 輪有 10,938 個既存編碼收據名稱；能否重用須由 owner 逐一驗證，不是已完成檔案或完成比例。兩輪已排入 `wsl-round6-10-renumber-resume-20-20261029T060951-5c858582`，以 `--d-stage-recovery-only`、最多兩輪並行及原 owner 執行；現等待第 11 輪的排程器。排隊不等於開始或已成功，執行前重新檢查容量和清冊。

第 11 輪的 `independent-D-original-recovery-for-quarantine` 於 14:17 完成，回執記載 59,687 個編碼原件獨立讀回與 materialized 驗證；它是隔離狀態的階段證據，並非退休驗收或刪除完成。14:22 已進入原 owner 的 `full-quarantine-resume-audit`，仍需完成該稽核及同步門檻。14:19 的 D 冷庫 Syncthing 仍在掃描，其他本機檢查為真、Vast 對該資料夾完成同步；掃描完成和完整驗收仍須各自確認。第 6／10 輪清冊的 allocated bytes 是候選容量，並非保證可釋放量。

## 備份來源清冊讀取

來源 owner 原本已對唯一物件的 metadata 做有限並行，但 manifest 本身仍串行開檔讀取。現改成在既有四個 metadata worker 額度內分批預讀，保留每個 leaf 的 no-follow 開啟、讀前後及名稱身分檢查、目錄 FD pin 和最終目錄 recheck；JSON 驗證、錯誤順序、清冊聚合與發布仍由原 owner 按原順序處理。worker 未執行完時 recheck／close 會等待，不以檔案名稱或 metadata 快取代替內容讀取。執行中的服務仍使用其啟動時載入的程式；不為此中斷當前清冊，新的程式須由後續服務週期載入並觀察。

在 D 冷庫固定 64 個 manifest、853,520 bytes 上，以同一 pinned reader 做唯讀 A/B/B/A：單 worker 2.574 秒、四 worker 1.234 秒、四 worker 1.315 秒、單 worker 2.733 秒；四次的每檔 SHA 與檔案身分完全一致，均通過目錄 recheck。此為小樣本、同時有其他工作，不能推算完整清冊或對 lab203 的傳輸吞吐量。備份與 packed backup 測試 150 項通過，包含並行實際重疊、串並行結果及錯誤一致、目錄快取淘汰、同步 fence 與拒絕符號連結；不是現場完整備份或遠端驗收。

## 14:32–14:33 空間及階段觀測

Windows 的 [GetCompressedFileSizeW](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getcompressedfilesizew) 唯讀兩次，14:32:48 至 14:32:58 之間，Ubuntu 與 Wazuh 兩個 ext4.vhdx 的配置量均沒有變化；同時 C 槽 available 從 87,844,257,792 降到 87,726,419,968 bytes。這個十秒樣本不支持把該窗口 C 槽下降歸因於兩個 VHDX 的配置成長，不能證明更長期間的來源，也不代表已定位 Windows 其他寫入。取樣沒有停止程序或更動磁碟。

第 11 輪 `verify-materialized-files` 子階段在 14:28:31 為 571／59,687，14:34:09 為 12,062／59,687；數字只是此子階段逐檔 SHA 進度，不是全部隔離稽核或退休比例。當前速率和其他工作都可能變化，不能把單階段外推當成完整回收 ETA。

lab203 在 14:00 的回執指出兩個固定控制包仍因接收背壓未收到。已更新[來源證據交接](lab203_source_evidence_handoff_2026-10-29.md)；來源本地讀回與 NAS 獨立還原是兩件事。不要修改接收上限、保留空間、pin、retry 或計數，也不要刪除 source、NAS 或 ingress 資料來繞過這個阻礙。

2026-10-29 15:29，第 11 輪原 owner 的 `retirement.json` 回報 `deleted=true`，WSL 實際退休 13,404,483,584 allocated bytes；其隔離稽核及固定 D 版本的獨立原值重建先前完成，退休回執記錄 `backup_verified=false`、`resilience=single_d_volume`。15:42 的另一個只讀對帳顯示同一版本全部 15 個必要檔案在來源端 Restic 與 immutable archive ledger 的既有機器收據聯集內各有身分覆蓋；沒有新的 NAS 讀回，也沒有跨這些收據重建完整 release，因此不能修改原退休回執或宣稱整版 NAS 還原已驗收。細節與固定回執見[來源證據交接](lab203_source_evidence_handoff_2026-10-29.md)。

15:42 時，第 11 輪仍在 `prune-verified-encoding-stage`；可見四個 worker 在 D 的 9p metadata／讀取工作中，尚無 stage prune 最終回執。第 6／10 輪原排程等待此 owner 結束，不刪除鎖或繞過排程器。先前第 9 輪相同大階段耗時 4,686 秒，只是不同資料及負載的歷史觀察，不能當成本輪完成 ETA。

備份 code／control 時，source control receipt 原先在解析與計算 SHA 時讀取兩次，且來源預檢後若換版，ledger 的 unchanged 與 export 分支可能保留預檢收據 SHA，而非實際使用的收據 SHA。現改為以同一份 bytes 解析與雜湊，並以各分支真正使用的收據回填 ledger；原封套、來源穩定性與還原核對仍保留。三個並行換版案例先重現失敗，修正後與 backup auxiliary／stream／handoff／packed／catalog 回歸共 259 項通過；recovery queue／relay／offhost／transport cache 另 76 項通過、15 項跳過。已有執行中的服務程序不會熱載入更動，下一週期自然載入；尚未以線上成功 receipt 宣稱已部署驗收。

15:49 使用固定開啟的實際清冊及來源 ledger，逐一比較 4,119 個版本、10,158,323 個引用：覆蓋判斷結果完全一致。一般字串引用改為直接集合成員檢查，異常格式沿用原 set 轉換，包含不可雜湊的後續引用仍會報錯。交錯順序量測僅此成員判斷的 CPU 為 1.120→0.626 秒，不是整輪備份的加速倍率；清冊開啟的 inode／size／mtime 穩定，但此次未重算整份清冊 SHA。可稽核數字見 `artifacts/operations/storage-single-authority-20261025/backup-coverage-parity-20261029.private.json`。

15:50 的 live stack 樣本顯示來源備份程序正使用四個 worker 對 D 冷庫物件做 no-follow metadata 檢查；第 11 輪的四個 worker 正在讀取並雜湊 materialized 檔案，兩者均進入 D 讀取等待。這是該瞬間的觀察，不代表全程或硬體已飽和，也不是刪除鎖的理由。來源 service 15:38 一輪因工作樹變動而未發 READY；程式修改停止後須等原 service 的下一個自然週期與實際收據，才能確認線上採用。

對固定清冊中 512 個 D 物件、原 pinned reader 的 no-follow metadata 做四／八／八／四 worker 唯讀樣本：分別 1.105／0.785／0.734／0.929 秒，四次檔案簽章相同並重新核對目錄。此樣本未讀檔案內容，其他 D 工作並行，亦未測完整清冊或退休流程；不能只憑此結果增加線上 worker 或推算全流程時間。測量與樣本身分見 `artifacts/operations/storage-single-authority-20261025/cold-metadata-worker-sample-abba-20261029.private.json`，可用於後續原 owner 的完整對照。

## 16:42 進度與備份修正

第 11 輪原 owner 的 `stage-pruned.json` 現已完成，回報 D 暫存回收 2,501,287,936 allocated bytes、冷庫物件刪除 0；WSL 端 13,404,483,584 bytes 的退休結果仍以原回執為準，兩者不可相加為 WSL 回收。先前對隔離區目錄的觀測顯示檔案在減少，程序堆疊位於 9P 的 `unlinkat`：這是一個串行小檔尾段，不是鎖死。第 6、10 輪於 16:42 經原排程器重新檢查後並行進入 `prepare-exact-source-archive`，原清冊分別為 9,881,169,920 與 11,910,537,216 allocated bytes；其 source SHA、D 完整重建、程序及退休門檻均尚未完成，因此只是候選量。

16:42:19 的 WSL 可用 308,637,904,896 bytes、Windows C 實體可用 69,215,485,952 bytes、D 可用 381,521,362,944 bytes。日內可用空間變化包含其他程序的新增寫入；短時間的 `/proc` I/O 取樣顯示當沖推論正在寫入它使用的分鐘工作快取，不能刪除使用中的 generation 或推斷所有新增 bytes 都來自該程序。此刻 C 已接近部分下載管線的 64 GiB 保留線；當沖工作依既有優先權繼續。既有 minute cache 套用工具仍會拒絕有程序引用的根目錄，不以兩天的抽樣相同 SHA 視為完整去重證明。

備份 source 的上一個完成週期遇到工作樹變動而保留失敗暫存、沒有發出不一致的 READY，下一個完整週期因全歷史清冊而耗時。現在只對三種明確的工作樹競態在同一原 owner 週期內立即重試一次，每次重新執行容量與穩定性檢查；任何其他錯誤或第二次失敗仍回報，失敗暫存不刪除。狀態記錄嘗試次數和第一次重試原因。ZIP 工作樹讀回把每個成員的 CRC 與 SHA 放在同一個讀到 EOF 的流程，保留完整檔名集合、重複名稱、來源與檔案 SHA 檢查；CRC 或有效 ZIP 但內容改變均會拒絕。

從一份已封閉的既有封套校驗並複製工作樹 ZIP 到私有 `/var/tmp`，在 2,380 個成員、11,842,445 compressed bytes 的同一份輸入做完整讀回 A/B/B/A：舊流程 0.6658／0.7695 秒，新流程 0.3739／0.4060 秒，平均分別約 0.718／0.390 秒。樣本是 WSL 熱快取下的單一讀回子階段，不是完整備份或 NAS 吞吐量。備份 stream、auxiliary、handoff、offhost 與 relay 測試共 184 passed、14 skipped，包含空成員 CRC 錯誤；現存 16:25 啟動的服務仍是舊程式，須等下一個自然週期載入並以新收據確認，不能憑測試宣稱線上採用。

使用 PID 1 的正式 D 掛載、僅建立並回收隔離的 8 目錄／1,024 個合成小檔，串行／四 worker／四 worker／串行刪除分別為 3.9342／1.9104／0.7142／4.0795 秒。同步工作負載與樣本形狀均不同，沒有測完整退休工作，不能將此倍率套用到真實資料；本次沒有改變正在執行的原 owner 的刪除方法或以其他程序接管其隔離區。

Penguin Syncthing 日誌顯示 lab203 連線於 15:56:42 中斷、16:37:42 短暫連回，並收到了 16:39:45 的接收稽核；16:41:20 再次斷線。16:39 的對端稽核顯示 admission budget 為 0、ingress logical bytes 為 85,904,587,016，狀態在斷線後不再新鮮。斷線期間 `storage-relay-audit.json` 回執報對端不可用；短暫連線期間也有 `finishing: pull: no such file`，需在對端和本機的同步日誌繼續定位，不能假定兩個錯誤相同。這不是 NAS 完整接收或兩端同步的證明；恢復連線後仍需對固定控制包、對端背壓、原始 paired ACK 和整版獨立還原逐項核對。現有 ceiling、reserve、pin、retry、原 owner 與 ingress 不變。

16:45:04 第 6／10 輪的 `inspect-interrupted-receipts` 分別讀到 5,792／56,320 與 6,752／40,510 個前綴；此階段一旦遇到缺失收據會提早結束並轉回一般準備，若前綴中有損毀則必須按原順序報錯，因此其分母不是全流程完成比例，也不能外推成準確 ETA。此時 C 實體可用空間已低於 64 GiB；不能把 WSL 虛擬可用量當作 C 的可用量。

16:47:49 的只讀 stack 觀察，第 6 輪主執行緒正等待四個既有收據讀取 worker；第 10 輪已因中斷收據不完整而轉入 `encode-and-check-receipts`，並有 worker 對已存在內容做 SHA 核對。16:47:56 的進度分別為第 6 輪檢查 18,400 個收據前綴、第 10 輪準備 1,376 個檔案；並非退休完成量。這兩個 owner 均繼續執行，後續完整內容驗證、D 讀回與退休門檻尚未驗收，暫無可負責任的整輪 ETA。

16:48 左右的備份程序 stack 位於 `scan_transport` 的 Syncthing HTTP 等待；程式在該等待前已保存 journal 並釋放 owner lock，實際掃描發生的範圍與完成時間仍須以後續收據核對。設定的單輪掃描總預算為 900 秒；單次 stack 不能證明已逾時，亦不保證下一輪完成。16:47 的 API 查詢顯示來源 ingress 為 `scanning`，lab203 對端 `connected=false`，其來源視角 `needBytes=32,743,896,719`、`remoteState=unknown`，不能將斷線時快照視為對端驗收。

16:46:36 WSL 檔案系統可用 308,598,710,272 bytes，而 Windows C 實體可用 67,247,067,136 bytes；C 已低於一般下載的 64 GiB 保留線。自動線上 `fstrim.timer` 持續啟用，前次紀錄曾回報 discard 但 C 實體可用量未同步增加，因此不能將已釋出的 guest 空間當成 Windows 已回收，也未為縮檔停止 WSL 或當沖服務。

16:49 依 `scripts/cleanup_windows_wsl_temp_swap.ps1` 的原流程稽核，Windows Temp 有一份非現行 VM、已閒置約 21 小時的 `swap.vhdx`：目錄只有該檔、不含 reparse point、可獨占開啟。額外用 Windows `fsutil` 讀回確認它沒有其他 hardlink 名稱且非 sparse；目前 VM 的 swap 則跳過。16:50 再次檢查後套用原腳本，僅刪除該一份，apply 收據記載 33,525,071,872 bytes，C 實體可用量由 63,549,288,448 增至 97,074,360,320 bytes；獨立檢查確認舊檔不存在、現行 swap 存在，當沖與兩個退休 worker 仍運行。收據為 Windows `C:\Users\agari\AppData\Local\StockAgent\maintenance\receipts\wsl-temp-swap-audit-20261029T084939.7746081Z.json` 與 `wsl-temp-swap-apply-20261029T085017.0695993Z.json`。這是 C 上一份失效 swap 的實際回收，與 WSL ext4 各輪回收不可重複相加；也不能據此推斷短時間 C 容量下降都由它造成。

16:51 第 6 輪遇到第 28,326 筆收據缺件，原檢查以 `incomplete` 結束並由既有流程接續重新準備，不是宣稱檔案內容錯誤；第 10 輪的 `encode-and-check-receipts` 為 6,752／40,510。兩者仍待後續各項門檻。額外執行 transport cache 與 private cold backup 回歸 44 項通過，加上前述 184 項通過、14 項跳過；這些測試不構成真實 NAS 接收證據。

lab203 16:51:39 經 relay 再度連線，16:52:15 的來源端快照為 connected、remoteState valid，peer folder 尚需 32,743,896,719 bytes。新的 16:52 接收稽核仍記錄 admission budget 0、ingress logical 85,904,587,016 bytes；原接收 owner 有其他已驗收工作副本的退休進展，但 `ingress_deletion_enabled=false`，不能把它當作 ingress 釋出或新批次已收到。`storage-relay-audit.json` 出現短暫 pull error 後又收到新版本，不能單憑它判定連線 EOF 的原因。見[來源證據交接](lab203_source_evidence_handoff_2026-10-29.md)。
