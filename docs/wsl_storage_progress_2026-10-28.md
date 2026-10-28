# WSL 冷庫驗證與空間回收：2026-10-28

## 22:50：補齊私人 NAS 範圍、解除批次占鎖及減少重複驗證成本

本節取代下方舊進度的「目前」解讀，保留舊測量及失敗供追查。尚未完成
全專案 NAS 驗收；來源清冊、封閉發布、同步、NAS 全檔還原與原 runtime
可用性是不同證據，不能拿其中一項推算其他項目的百分比。

| 範圍 | 已確認 | 尚未完成 |
| --- | --- | --- |
| WSL 原來源回收 | Round7／8 合計 25.63 GiB，屬先前完成量 | Round9／6／10／11 候選共 39.90 GiB；本節未新增生產來源回收 |
| 本次可拋棄測試工作檔 | 十三組已結束 pytest，實際釋放 924,434,432 bytes，約 0.86 GiB | 收據與 JUnit 留存；沒有刪來源、D 正本或 NAS snapshot |
| 專案來源登錄 | 61 個來源、55 個私人 cold head；專案根目錄沒有未登錄的 `data_*` 資料夾 | 這是 metadata 清點，未證明所有可變尾端已入冷庫 |
| 私人不可變冷庫 | 固定清冊 48,276 檔、817,226,447,345 bytes，約 761.1 GiB，101 個 retained releases；metadata error／missing object 均為零 | 沿原 owner 分批交付，尚未取得全閉包 NAS ACK；不可與公開清冊的覆蓋量直接相加 |
| 固定新聞版 | 22:55 已達 19／30 波，6,243／11,494 原始檔，5,218,519,039／9,776,598,701 bytes | 新版已沿相同 plan 接續；paired NAS restore ACK 尚未取得 |
| 原公開冷檔清冊 | 22:46 完整收據：99,277／99,432 檔已覆蓋，尚差 155 檔、124,737,310 bytes | 新增來源使分母增加；不包含本次私人閉包及尚未入庫來源 |

來源範圍見忽略的操作證據
`nas-all-source-scope-final-20261028.json`，來源 alias 依實際解析身份歸屬，
不是把 symlink 再算成一份漏列資料。61 個來源含原 publication catalog 及
canonical dataset-version migration 額外登錄的來源。artifacts／服務帳本、
code／config／SQL、尚未封閉的來源增量及獨立 runtime 還原，仍由各原
owner 提供證據；「沒有漏列 data root」不是全檔案系統已備份。

本次實作與部署：

1. `publish_archive` 的 pre-commit 與 post-commit 完整驗證，對已登錄的
   D 手動封存都傳入原四工 reader。仍完整解碼、核對原值／encoded SHA、
   exact set、source signature 與程序引用。正在跑的 Round9 是舊程序的
   單工完整驗證，不中斷重做；後續 Round6／10 子程序會載入新實作。
2. `verify_backup_delivery.py` 對原生 D I/O 的逐檔 no-follow 路徑分類也
   使用原 worker 預算，解除 SHA worker 啟動之前的串行 metadata 瓶頸。
   portable 接收端預設及每個位元組、inode、目錄集合的檢查保持完整。
3. `private_history_backup` 是使用者 10/28 個人 NAS 指令的獨立備份政策，
   沿原 `BackupStream`、owner／ledger、配對加密 ingress、NAS 容量及
   機器還原 ACK；沒有把原 private marker 或 `publish: false` 改成公開。
   保留所有 retained manifests 所引用的 unique objects，只送一次。
   固定閉包全部 ACK 後，下一次原排程才重新捕捉新不可變版本。
4. 同一私人 owner turn 完整盤點傳輸容量一次，再以每波完整 payload／
   控制檔大小保守累加。GC 只能降低上界，每波仍觀測實體空間；失敗、
   下個 turn 或重新取得 owner 都重新完整盤點。沒有可送檔案時只等 ACK，
   不重掃傳輸樹、不重複複製。維持原 8 GiB batch、24 GiB staging、
   32 GiB pending 上限及 reserve，沒有啟用新的下載器或限制交易服務。
5. 新聞舊程序持原 owner 橫跨全部 30 波，使其他備份長時間等待。22:39
   只在唯讀目的檔驗證界線停止舊程序，保留 17 個 ledger commit 與完整
   未提交 staging；舊 run 正確記錄 exit 143，不算成功。新版每波讓出
   owner，重新取得後重讀 ledger／ACK、再做容量入場，不覆寫其他提交。
   續跑從 ledger 重建進度；全波發布後只對帳 ACK，避免已完成資料重抄。
6. 另修正短暫讓鎖可能被 30 秒輪詢永遠錯過的飢餓問題：受監督的私人
   補傳使用原 `--wait-for-owner` 核心等待；metadata capture lock 不阻擋
   公開 publisher。一般週期預設仍非阻塞。狀態明列等待 owner，不再長期
   卡在「capturing」假象。原備份 unit 的暫緩碼 75 已標為預期退出，timer
   照常續排；這不是備份完成或把實際錯誤改成成功。
7. 私人首輪的真實 stack 又顯示小物件逐檔啟動 Windows reader。原
   `copy_verified_many` 現在支援這條明確 native 路徑的小檔批次：每組
   最多 16 檔／16 GiB、單檔最多 8 GiB，原四路 worker；captured head
   仍使用原固定 bytes。重用原 `copy_many` 的逐檔 SHA／flush／來源簽名，
   封閉發布仍獨立完整核對目的檔。其他路徑保留原預設，不把本次小檔
   測量擴張到其他來源。完整物件複製、原生 I/O 與 offhost 回歸為
   **115 passed、2 skipped**，涵蓋來源改變、目的損毀與部分寫入保留。
   這證明呼叫合併與保護 gate，尚不是實機全流程的加速倍數。

真實測量只支持各自範圍：

- Round9 的 192 個固定小檔、3,167,976 原始 bytes，1／4／4／1 工的
  完整 encoded／original SHA 讀取為 23.726／5.824／4.422／15.205 秒；
  平均 19.47 → 5.12 秒，約 3.80 倍，所有 hash 相同。這是小檔讀取驗證，
  不是整批封存、NAS 或 WSL 刪除的加速倍數。
- 另測整個 509 檔、8,696,013 bytes 的封閉新聞批次：舊版 594.686 秒，
  新版兩次 160.788／144.932 秒，三次完整 proof 相同。但第四次舊版遇到
  `Native batch failed (startup-eof, output_bytes=0); no metadata sent`，
  **ABBA 未完成**，不宣稱公平對照或全流程改善倍數。保留三次測量及
  失敗的 `closed-delivery-509-abba-failed-20261028.json`；benchmark 介面
  已改為逐次持久記錄，後續失敗不再遺失先前測量。

驗證：完整 legacy archive 回歸 111 passed；backup／offhost／news／native／
GC 等較廣回歸 224 passed、2 skipped；最後私人容量／核心等待與共用
backup stream 回歸 113 passed。這些集合有重疊，不能相加。新聞讓鎖與
進度修正另有 41／20 項 focused 回歸，均通過。
最後私人階段進度的 18 項與固定 head／交接的 2 項回歸也通過，仍與上列重疊。

原 task 下持續運行：

- Round9／6／10：`wsl-independent-9-6-10-d-recover-20261028T111537-685a99ef`。
- Round11：`wsl-round11-after-independent-wa-20261028T112424-e6f2a4c6`。
- 新聞原 plan 續跑：`news-private-fair-owner-resume-20261028T143955-ff372381`。
- 私人 NAS 補傳：`private-nas-native-group-catchup-20261028T151406-9eb59da6`。
  先前 kernel-wait run 已在首批提交後受控交接，保留中斷而不計成功。

22:43 的獨立容量入場樣本為 C 實體約 59.61 GiB，低於該下載入場的
64 GiB reserve，WSL 虛擬可用約 309 GiB；兩者不能互換。D 約 387 GiB
可用，22:40 paired readiness 的 NAS 約 11.29 TiB 可用。C 實體容量本晚
大幅波動，這些是具時間的樣本，不是永久可用量或授權降低 reserve。

Round9 仍在完整驗證，NAS ingress／cold index 的 scanning 與 owner 等待
尚未全部消失。無可外推的穩定全流程吞吐量，所以**全 WSL／NAS 完成
時間目前未知**，下方 19:38 情境不再當作即時倒數。保持 9 → 6 → 10 →
11 有序，檔案內平行、發布／隔離／刪除依原 gate 串行，既有當沖優先與
來源／NAS 保護不變。操作證據均在
`artifacts/operations/storage-single-authority-20261025/`，不加入 Git。

23:00 再讀實際程序：私人補傳已取得原 owner、完成首次容量盤點，正在
四路 `backup-copy` 複製與核對原物件；不是只新增設定後仍無執行。
這個 turn 啟動於最後階段狀態修正之前，`private-status.json` 仍可能保留
舊的 waiting 字樣；以本次具 PID 的實際 stack／milestone 區分，不為了
刷新畫面再次重啟這一輪。下一次原 owner turn 自動載入階段進度修正。
22:56 的 cold index 及反向 receipts 為 idle、錯誤零；backup ingress 仍
scanning，雖 need files／bytes 均為零，不能視為同步收斂。22:58 paired
連線仍 connected、走 relay；連線可用也不代表所有檔案已還原。
最新跨範圍紀錄在 `wsl-NAS-current-milestone-20261028.json`、
`final-syncthing-observation-20261028.json` 與 `final-paired-link-20261028.json`。

23:14 首批私人閉包已完成兩遍獨立目的驗證與封閉發布：512 個來源檔，
含 envelope／READY 為 514 檔、34,696,204 bytes，固定 envelope 為
`de45017f29b6892fdf3a351aa3c3c9dd8ec9a10a53a3b752f8f8917b1aea4615`。
仍未取得 NAS 還原 ACK。只在下一波來源唯讀／部分 staging 階段交接至
已通過 115 項回歸的 grouped-copy 版本；第一批 ledger／READY 保留，新
程序按同一 catalog 繼續，不重抄第一批。第二波未提交 staging 留作診斷，
未刪除任何來源。原通知入口回報 ingress 還在 scanning，通知待原 timer
接續，不以 deferred 當成功。交接前 stack、首批完整 dispatch 與中斷身份
見 `private-native-batch-rollout-stop-20261028.json`；受監督接續 run 如上。

23:13 Round9 實際 stack 已進入 `publish_packed_snapshot` 的檔案清點，
不是仍停留在先前的逐檔 encoded hash。這是工程階段觀測；尚無完整
冷還原／來源退休收據，仍不計入 25.63 GiB 已回收量。

## 19:38：隔離單輪失敗、恢復後續排程與修正預估

本節取代 15:43 的排程預估。舊佇列在 Round11 的
`retirement post-rename transport not converged; quarantine retained`
退出，後面的 Round9、6、10 根本沒有啟動；不是那些輪次已持續處理了
數小時。Round11 的來源已移入私人 quarantine，仍占 WSL，不能因原
路徑消失就算回收。

目前按實際收據區分：

| 輪次 | 已驗收的 WSL 回收 | 狀態 |
| --- | ---: | --- |
| Round7 | 13.51 GiB | 原來源與 D stage 收尾均完成；stage 約 2.84 GiB 是 D 空間 |
| Round8 | 12.12 GiB | 原來源與 D stage 收尾均完成；stage 約 2.58 GiB 是 D 空間 |
| Round9 | 尚未完成 | 19:15 啟動原 owner，正在封存；19:38 觀察到 8,527／40,418 份單檔暫存收據 |
| Round6、10 | 尚未完成 | 排在 Round9 後；Round6 的三個符號連結沿已驗證格式保存 |
| Round11 | 尚未完成 | 原隔離區保留，固定原 inventory 與 D manifest，排在獨立三輪之後接續 |

前兩輪累計 **25.63 GiB** 是既有回收量，本節沒有新增完成輪次。
單檔暫存收據數只用來觀察準備工作：最近兩次樣本約 **6.04 檔／秒**，
不是逐份重新驗證的結果，也不是整個 archive／冷還原／刪除的完成率。

本次沿既有實作修正三處：

1. `--continue-independent-waves` 讓不重疊批次在某輪未完成時繼續。
   每輪仍重查 inventory、owner、容量與 canonical 驗收；留下失敗並
   回傳 incomplete。重啟時必須有完整原收據與來源 absence 才能略過
   已完成批次。重疊或失去全域 owner 觀測仍拒絕啟動。
2. 已在 quarantine 的唯讀完整稽核，可以在其他健康條件全部通過、
   只剩掃描未 idle 時完成，明列 transport blocker。接續刪除前若
   再遇到掃描，canonical executor 回傳明確的「尚未開始 unlink」
   結果，釋放共用 owner 再等；不重做已完成的整批 byte audit，也不
   改寫原 1,500 秒期限。過期、來源／引用／pin／generation 改變仍
   拒絕。初次 rename、未知失敗及部分 unlink 不套用此重試。
3. 備份清冊中每個 unique object 的 no-follow metadata 查詢，對正式
   D reader 改成最多四工、每批 32 項。每個物件仍 fresh stat，完整
   SHA／manifest、缺失歸屬、固定 FD、ancestor/root recheck 不變。
   固定 1,024 個真實物件的 1／4／4／1 工耗時為
   **10.168／1.515／1.244／3.949 秒**，輸出身分完全一致；這不是
   整批 NAS 備份的速度比。另啟動完整唯讀 catalog 讀回驗收，尚未
   以它宣稱整個備份完成。

正式 lakehouse worker 已經由原交接入口驗收載入
`f110476a304dc82bc0031da424e2902b87b40516582841a71ba687d2b60ac10c`，
原 PostgreSQL／Temporal 與持久 workflow chain 保留。正在執行的
backup-stream cycle 不被中斷，下一輪載入新清冊實作。相關 retirement
回歸 **433 passed**；最新 backup／pinned-reader／排程回歸
**203 passed**，範圍有重疊，不相加成獨立案例數。

排程為 **Round9 → Round6 → Round10 → Round11**。兩條受監督工作
共用原 scheduler inode，等待者沒有取得 publication／retirement
owner；單輪的檔案處理仍最多四工、中繼資料最多八工。同資料集的
commit、隔離、刪除有序。19:36 的 C 實體可用約 **37.52 GiB**，
WSL 虛擬可用約 **312.96 GiB**；9 與 11 同時執行的保守控制預算
加原 **32 GiB** 餘裕需約 **38.33 GiB**，所以不強行同時啟兩輪。
每輪開始仍觀測真正 C／D 容量，不把預估當空間預約。

下表以 **19:38** 為起點，明列每個尚未完成階段的工程情境；包含
來源回收及 stage 收尾，不含獨立 NAS 最終驗收。來源空間可能提早
於 stage 收尾釋放。

| 順序 | 候選 WSL 配置量 | 中間情境完成時間（台北） |
| --- | ---: | --- |
| Round9 | 7.13 GiB | 10/29 約 01:40 |
| Round6 | 9.20 GiB | 10/29 約 09:00 |
| Round10 | 11.09 GiB | 10/29 約 15:00 |
| Round11 | 12.48 GiB | 10/29 約 19:00 |

四輪候選合計 **39.90 GiB**。整批較快／中間／保守情境約為
**10/29 08:00／10/29 19:00／10/30 11:00**；不是機率區間或保證
期限。新使用中引用、來源變更、同步未收斂或 C 容量跌破入場門檻
會暫緩，不能把這些等待當已完成進度。Round12 以後仍需原 producer
ABI 審核，不在本表分母；全 WSL 與 NAS 也沒有因此全部驗收。

新證據集中於
`artifacts/operations/storage-single-authority-20261025/schedule-recovery-20261028T1124/`：
`scheduled-runs.json`、`prior-round11-failure-evidence.json`、
`retirement-schedule-forecast-v2.json`、`catalog-signature-1024-abba.json`、
`catalog-metadata-lifecycle-reload.json`。舊 forecast 原樣保留供追查。

19:47 再核對，Round9 原封存程序存活；Round11 的受監督程序正等原
scheduler lock，沒有開始新的 payload 處理。完整平行 catalog 唯讀
驗收仍在執行，不計為通過。NAS 19:45 回執顯示目前可用冷檔清冊
99,325 檔，99,259 檔已覆蓋，仍有 **66 檔／88.52 MiB** 待覆蓋；
當前傳輸批次的 2.12 MiB 不是整體剩餘量。當次備份 cycle 為
1,971.98 秒，metadata error／missing object 均為零，但完整歷史備份
與 canonical reconstruction 旗標仍為 false。
即時讀回保存在 `final-runtime-readback.json`。

## 15:43：修復等待、超時與隔離區續跑，接續後續輪次

目前確認的阻塞有三種，已沿原 owner 修正：

- 傳輸回收的 systemd `TimeoutStartSec=600` 會在完整檔案驗證結束前
  終止程序。部署改為兩小時；配對 NAS 入場使用原封閉 enrollment，
  完整本機內容、來源及刪除 gate 仍保留。回收重用同一遍完整雜湊
  清冊，移除緊接著重複的全樹雜湊。
- Round7 的 D stage 在 rename 到隔離區後，完整解碼花超過原證明的
  1,500 秒期限而停下。原來源的 **13.51 GiB** 已有回收收據，不能因
  stage 收尾失敗再算成來源未刪。新接手核對唯一隔離區、舊 PID 已死、
  完整檔案 fingerprint、同一 D generation、來源不存在與無使用中引用，
  已把精確隔離區恢復到原 stage，重新完整稽核。保留舊失敗 plan 原始
  bytes 與 SHA。若新的 post-rename 完整解碼又超過期限，必須重新完整
  獨立重建 D release 才能出新證明，不能只延長舊時間。
- Round6、10 原先的容量估算計入完整 C 還原樹，造成不必要等待。新
  `--d-stage-recovery-only` 限定原 guarded D encoding，禁止改建 C
  還原樹，仍保留每檔控制預算、**32 GiB** 安全餘裕及原 D 預算。
  15:43 的 C 實體可用為 **41.00 GiB**，三輪各需 **34.47／35.44／
  34.47 GiB**，當次沒有容量 deferral；這是觀測而非空間預約。

已核對 Round9、6、10 三個舊程序只有等待 scheduler／capacity、沒有
payload owner，停止並換成單一受監督接續工作。精確順序是
**Round11 → Round9 → Round6 → Round10**，不同資料集的 NAS 備份與
Round7 D stage 收尾獨立運作。每個 archive 採既有最多四工，metadata
可用原有最多八工；同資料集的發布、隔離、刪除仍依原鎖與 journal
有序進行。未調降安全門檻，也未修改當沖服務或重新啟動下載器。

Round11 於 15:29 左右完成本次獨立 D 還原，**59,686 檔**完整工作
耗時 **3,271.62 秒**，15:43 正在 full-source/mirror/D retirement audit。
來源仍在，不能宣稱已釋放 **12.48 GiB**。Round7 的來源已退休，但
D stage 最終清理尚未驗收；它的約 **2.84 GiB** stage 是 D 空間。

另外用原 catalog 的固定前 **128 檔**執行唯讀來源驗證 ABBA，完整
`validate_sources` 包含路徑、大小、全 SHA 與前後 generation。單工／
四工中位 **13.93／1.81 秒**，四輪來源身分結果一致。這是該固定
子集的來源驗證改善，**不是整批回收加速比**。新的有界四工保留所有
來源／配對 NAS／隔離 gate，FD 餘裕不足使用單工。控制 runtime 測試
**122 passed、7 skipped**；退休原回歸 **351 passed**、stage suite
**94 passed**，後續新語義重點回歸 **76 passed**（有重疊，不能相加
當獨立案例數）。新增 phase history 保留完成／失敗與巢狀深度，供下次
估計，不把 parent 與 child 耗時重複計入。

以下是 **15:43 起**、只涵蓋已固定的四輪之工程情境估計；不是機率
區間，也不是全部 WSL／NAS 完成承諾。估計已包含待執行階段及目前
舊 owner 可能需要的 stage 接手；來源可早於 stage 收尾完成釋放。

| 順序 | 候選 WSL 配置量 | 中間情境：來源回收與 stage 收尾完成 |
| --- | ---: | --- |
| Round11 | 12.48 GiB | 10/28 約 20:00 |
| Round9 | 7.13 GiB | 10/29 約 01:00 |
| Round6 | 9.20 GiB | 10/29 約 08:00 |
| Round10 | 11.09 GiB | 10/29 約 13:00 |

四輪候選配置量合計約 **39.90 GiB**，實際回收以最終 inode 收據為準。
三個總完成情境約為 **10/29 04:00／10/29 13:00／10/30 02:00**。
根據原 128 小檔編碼、實際完整還原與 stage prune 耗時，為每輪明列
寬範圍假設；新的使用中引用、pin、來源變更、同步不收斂、實體磁碟
壓力及 NAS 外部回覆，仍可能使時間往後移。不能用一個局部解碼百分比
宣稱全部幾分鐘後完成；Round12 以後的 producer ABI 尚待逐一審核，
不在這個 ETA 分母內。

私有可讀回證據：

- `artifacts/operations/storage-single-authority-20261025/retirement-schedule-forecast-20261028.json`
- 同根 `transport-gc-fix-20261028/qualified-followup-runs.json`
- 同根 `transport-gc-fix-20261028/idle-queue-handoff.json`
- 同根 `transport-gc-fix-20261028/benchmark_primary_validation.json`
- 同根 `transport-gc-fix-20261028/lifecycle-parallel-validation-reload.json`

以上預估不會取代各輪 `acceptance.json`、來源 absence、固定 D 恢復及
獨立 NAS 備份證據；Windows C 上 VHDX 實體檔是否縮小亦須另外量測。

## 14:59：後續輪次、日增量與架構容量

以下是當時量測，不是全系統最小容量或新的刪除授權。磁碟 GiB 使用
二進位單位；WSL 位於 C 的虛擬磁碟內，不能將兩者使用量相加。

| 磁碟 | 已用 | 可用 | 範圍 |
| --- | ---: | ---: | --- |
| WSL | 2,618.6 GiB | 312.8 GiB | ext4；另有保留空間，不等於 C 實體可用量 |
| Windows C | 3,771.9 GiB | 42.4 GiB | 包括 WSL VHDX 與 Windows 其他用途 |
| Windows D | 3,336.4 GiB | 389.6 GiB | 包括冷庫與其他用途 |
| NAS | 3,788.5 GiB | 11,560.6 GiB | 包括共享磁碟其他用途；不代表個別備份已驗證 |
| Vast | 461.8 GiB | 538.2 GiB | 當前訓練節點；實例可能移除 |

原 owner 的收據顯示 round7 原來源已回收 **13.51 GiB**，但 D 編碼
暫存的獨立驗證與清理仍在進行，且該來源尚無 NAS 驗證回執；round8
已回收原來源 **12.12 GiB**。這些是既有收據，不是本節新增刪除量。
Round11 上次完成還原後，在最後 gate 被目錄掃描程序引用及同步
`folder_idle=false` 阻擋；本次盤點的兩個掃描程序確實造成部分引用，
已停止。新的受監督工作固定同一 D manifest 與清冊重做驗證，不跳過
同步／使用中／隔離 gate。它仍在執行，沒有完成回收收據。

新的原始 producer 收據 adapter 經測試並對實際收據讀回後，另外建立
round9、round10 的精確清冊，分別為 **7.13 GiB／11.09 GiB**，兩者
均無初步 blocker、無符號連結；這只是各根目錄內 unique-inode 配置，
不等於可釋放量。round6 為 **9.20 GiB**，其三個原始符號連結仍由
已測試的 opaque-link 格式保全；round11 候選為 **12.48 GiB**。
四者不得因跨目錄 hardlink、後續內容變化或使用中引用直接相加當
作已回收。Round9、10 與 6 已各排入原 owner，等待容量和前一個 wave
完成；C 空間不足時繼續等待，且排隊不會持有 scheduler lock。當沖
服務優先，不受這些工作限制。

原排程在 capacity 恢復時若 scheduler 正忙會直接退出，現可等待鎖、
每次重新觀測容量並核對原 inventory／固定出版身分；任意 owner 錯誤
不自動當成可重試。原 WSL 全域分類掃描會被退休的程序引用 gate 正確
識別為使用中，導致昂貴驗證後不得刪除。現讓 supervised wave 與本機
目錄 census 共用排他／共享鎖；已有可執行 wave 時暫緩新的本機分類
掃描，容量每分鐘繼續觀測。中斷的舊分類標記為 partial 或 not_scanned，
保留原觀測時間，不偽稱仍在掃描。本協調不涵蓋未使用 wave 鎖的其他
獨立 owner，仍須實際重新檢查程序引用。

粗略的單樹本機 `du` 曾觀察 `artifacts/cache` **161.67 GiB**、
`artifacts/live` **125.38 GiB**、`artifacts/operations` **111.66 GiB**；
掃描中來源可能變動且跨樹可能有 hardlink，不能相加，也不是可刪量。
既有儲存觀測對 WSL 的 33 個登錄根只完成 32 個，**523.60 GiB** 是
分類下限，不是完整 WSL 分類。較舊的 181.55 GB 分鐘副本已在
[10/15 報告](penguin_service_storage_layout_2026-10-15.md)中壓實到
101.30 GB，不應再把當年確認的重複量當成現在仍待回收。最新狀態
清冊另顯示 lakehouse 的 dataset-version operations 約 **90.32 GiB**，
其中包含正在進行或已封閉的不同 generation；不能因名稱或年齡刪除。

容量模型應按角色分開，而非將所有磁碟數字相加：

1. WSL 必需空間＝系統／環境＋服務真正需要的投影、ledger、狀態＋
   尚未安全移交的增量與 journal＋研究按需工作集；另加並行任務的
   最大同時暫存與當沖需要的安全餘裕。
2. D 必需空間＝所有仍有 manifest 引用之不同壓縮內容＋索引／收據＋
   增量／整理與恢復驗證的同時工作集＋預留。按 digest 共用，歷史
   不同版本仍需保存；理論壓縮極限需要知道原始內容，不可由檔名估計。
3. NAS 必需空間依實際備份內容、保留的版本與去重方式獨立計算；
   lab203 只計待中繼／恢復空間；Vast 計在用輸入、檢查點及七日
   產物。不同機器副本因復原目的存在，不能當成同磁碟重複。

目前無法給出全部資料的單一最小 GiB，也無法用既有資料算出所有來源
真實每日增量：部分來源的 writer／reader 尚未完成 cold-native 切換，
全庫 unique digest、歷史版本／備份保留及實際增量尚未完整量測。
現有永豐相關十群組的 **74 日邏輯大小淨變化約 1.35 GiB／日**；
其最近 30 日按 mtime 的寫入活動約 **1.26 GiB／日**，會包含重寫，
這兩者都不是全資料的物理增量，也不是未來下載速度保證。來源：
`artifacts/live/shioaji_storage/summary.json`，2026-10-28 14:09 產生。

已開始在 `artifacts/live/data_monitor/storage_capacity_daily_history.json`
記錄六個磁碟每天第一個新鮮觀測；只記實體已用／可用與時間，保留
120 天，不計重複 WSL＋C，也不把淨用量變化解釋為原始下載量。現在
每個磁碟只有一天基線，跨日比較尚不可用。新增功能、歷史 producer
收據、排程及鎖協調相關測試共 **328 passed**。下一步是持續逐根完成
冷版本、原生 reader／writer、消費者與備份驗收；安全回收後 WSL 內部
可用空間上升，不保證 Windows 上的 VHDX 實體檔自動縮小。

15:08 補查 WSL 根檔案系統的已 unlink 仍開啟一般檔：按 device/inode
去重後只有三項，logical 合計約 **0.366 GiB**，主要是本次仍在使用的
Codex 執行檔和小型日誌；不終止服務來換取這少量空間。這不能解釋
2.6 TiB 的 WSL 占用，後續應繼續追查可見資料與尚未完成的交接。

後續落實順序如下；每列完成都須有該 owner 的實際收據，不以排入佇列
取代驗收。不同冷資料集可以有界平行編碼／驗證；同一資料集的發布、
最終查核、隔離和刪除維持有序，並依實際峰值容量控制同時工作數。

| 順序 | 實作範圍 | 完成與容量驗收 |
| --- | --- | --- |
| 1 | 持續 round11，容量允許時接續 round9、6、10；round7 完成 D stage 清理 | 原來源 absence、實際 inode 回收量、固定 D 還原、stage prune 與收據均一致 |
| 2 | 逐一審核其他已封閉 benchmark 的原 producer ABI，不擴大解讀 complete flag | 精確來源／清冊、獨立恢復與目前 consumer gate；尚未審核的輪次先保留 |
| 3 | 按登錄根盤點 operations、cache、service projection 與舊 generation | 每份內容分類為唯一來源、目前工作集、可重建投影或復原／備份；不得按名稱刪除 |
| 4 | 逐 provider 移除歷史全量重寫，讓 writer 只寫受影響分區、reader 透過既有冷庫按需讀取 | 原值／完整性、版本、服務與精確恢復一致，之後才收回舊路徑；另一視窗管理的 factorized 檔案維持只檢查 |
| 5 | 用不同的來源收據與磁碟跨日樣本量測日增量，持續驗證冷區整理與 NAS 回執 | 區分原始新增、覆寫、唯一壓縮新增、工作集及備份；不得把 API 流量當成儲存增量 |

目前仍有必要暫存、服務使用或驗證未完成的內容，與舊 writer/reader
尚未遷移的內容；這些是距離目標容量的可執行工作清單。現有 managed
materialized 的 256 GiB 與 Panel family 的 64 GiB 是**該範圍快取上限**，
不能當成整個 WSL 的必要容量，也不能在當沖實際需要資料時硬套上限。
完整的數值最小值須在上述內容身分、保留版本與並行峰值可量測後重算。

## 14:20：round6 連結保全與 D 小檔封存

round6 重新清冊為 **56,320 個一般檔、9,881,169,920 bytes unique-inode
配置量，以及三個符號連結**；既有 producer 完成證據與即時引用檢查未發現
blocker。兩個連結指向原 benchmark 內的檔案或目錄，另一個指向不存在的
名稱。新清冊是 `workflow-round6-symlinks-d-20261028.inventory.json`，
SHA-256 為 `68dc097b6e8c8f0bea3b1d260c51808aa5acdf261f9a61f8fe01777eeefd86e4`。
這是候選量，不是已回收量。

新增僅供明確核准的離線 benchmark 使用的連結保全格式：逐個凍結連結名稱、
目標原始 bytes、身分及時間，**不追隨目標**；僅接受文字目標仍在原 benchmark
內且沒有 `..` 的路徑。還原時先恢復目錄和一般檔，再建立連結。絕對目標按
原文保存，因此要恢復原本解析結果必須還原至原始位置。其他來源仍拒絕此
模式；既有格式、cold checksum、producer、pin、process、同步、隔離與 inode
回收門檻均保留。私有 fixture 已測試一般連結、dangling、非 UTF-8 目標、
竄改／路徑衝突、中斷續跑，以及實際經原 owner 隔離回收、清理 D stage、
從 cold 重建；Round 6 真實資料本身尚未回收，也不能宣稱 NAS 已驗收。

以 Round 6 實際 128 個小檔測量完整的單檔讀取、durable D 編碼、SHA 及
durable 收據，1／4／4／1 工依序 **59.384／16.210／15.273／36.690 秒**；
每輪來源身分與 encoded SHA 一致，中位數 **48.037／15.742 秒**。這是
共同機器負載下的局部 ABBA，不是整批 archive、校驗或回收 ETA。已在現有
`prepare_archive` 的已核准 D 暫存路徑加入最多四工、每批最多 32 項；大型
檔案序列化，FD 預留不足退回單工，最終順序、來源重新掃描及所有 gate
不變。測試亦比較 serial／parallel manifest，注入部分失敗後重新執行，
檢查結果與 serial 相同且原來源未改。

新增連結格式及平行封存後，相關 archive、retirement、stage prune、
planner／scheduler 測試共 **271 項通過（116.69 秒）**。
另執行單一 D 正本、各項最終 gate 與跨批硬連結回收的 retirement 測試，
**9 項通過（33.58 秒）**。實際清冊已交給
受監督工作 `wsl-round6-symlink-capacity-wait-20261028T061917-72ed0916`；
它每分鐘重新檢查容量，不持有回收鎖。14:19 首次回執為
`waiting_for_capacity`／`windows_c`；容量恢復後仍須重新核對清冊、來源、
消費者、冷庫及所有刪除門檻，不能把排隊視為已封存或已刪除。

14:19 左右 WSL ext4 可用約 **314 GiB**，C 實體可用約 **30 GiB**，D 約
**390 GiB**。round6 的 frozen D-stage wave 原有保守預檢要求 C 約
**44.5 GiB**，因此不能跳過該容量門檻啟動。C 容量持續變動；8 秒系統
計數觀察到 Windows 搜尋、掃毒和一個安裝程序有寫入，不能僅憑此判定
可用量下降的原因。原資料、C floor、當沖資源優先序及暫停中的大量下載
均保留，不能用 WSL 邏輯空間冒充 Windows C 的實體可用量。

14:20 的 round7 程序已完成原來源退休，D 暫存稽核進到逐份收據檢查；
round11 的既有程序仍在完整來源／D metadata binding，尚無最終回收收據。
當沖模擬、Discord 和公開來源事件主服務均 active，不能僅據此認定資料
完整或交易就緒。14:00 Discord 收盤快取曾成功，14:01 的後續觸發等候
公開資料更新，14:11 因等待超時失敗；FinLab 當沖 projection 仍在工作，
尚無本輪完成驗收。本項僅記錄觀察，未重啟或修改其他視窗正在處理的
factorized 產生器。

## 14:01 最新狀態

round7 的來源已完成退休，收據記錄釋放 **14,506,016,768 bytes**；冷庫
暫存的後續稽核仍在執行，且 NAS 尚無驗證回執。round11 尚未退休，14:01
的程序抽樣位於逐檔原值核對的 **57,376／59,686**；這是單一階段位置，
不是整體進度。另有不支援還原符號連結的 round6 候選暫時保留。

14:01 的可用空間為 WSL ext4 約 **314.93 GiB**、Windows C 約 **33.30
GiB**、D 約 **390.28 GiB**。各處同時有其他寫入，與單批來源的實際回收
量不可直接相減。WSL 內釋放空間也不代表 Windows 實體磁碟同步釋放。
詳細依據與小範圍效能實測見下方「13:44 補充」。

## 11:03 原始快照

以下是 11:03 Asia/Taipei 的限定範圍快照；即時結果以各批次的
`progress.json`、`acceptance.json` 與服務狀態為準。空間分別為 WSL ext4
257.83 GiB、Windows C 34.85 GiB、D 397.33 GiB 可用。WSL 可重用空間
不代表 Windows 實體磁碟已回收；沒有停止 WSL、調整磁碟格式或中斷當沖服務。

| 固定清冊 | 候選 unique-inode 配置量 | 已確認進度 | 尚欠的證明 |
| --- | ---: | --- | --- |
| benchmark round7 | 14,506,184,704 bytes | 78,393 原始檔已由固定 D 版本獨立還原；目前來源已隔離，正在做刪除前的逐檔雜湊與原始內容核對。 | 新的完整回收收據及實際回收量；隔離不是刪除。 |
| benchmark round11 | 13,404,770,304 bytes | 59,686 原始檔已由固定 D 版本獨立還原；來源與固定 D 版本的完整稽核仍在執行。 | 後續即時傳輸、引用、隔離與刪除驗收。 |

round7 的冷庫 manifest 固定為
`21ff9ee0becfea089c7ea5d3ebad42019b8a5e7231d835641d47bfd5cd39184a`，
來源清冊為 `7b2fa622475308d89deace970e0e08509892a8bae3f4c28b7fd1b618a2ec6827`。
新增監督中的一次性後續工作
`wsl-round7-verified-lock-retry-8-20261028T030130-2d1caf76`，同一個
`owner.lock` 保障既有工作與後續工作不會同時修改；前一工作完成則讀回其結果，
未完成則由既有 owner 對相同清冊與固定 D 版本重新驗證後接續。round11 已有
既存的受監督後續工作。這些工作不能把本機 Syncthing 收斂解讀為 NAS 驗收。

## 驗證瓶頸與限定優化

D 槽的固定編碼資料含大量小檔案，完整驗證中的檔案中繼資料讀取具有較高的
9p 延遲。`legacy_artifact_retirement` 現在僅對既有 D staging 白名單中的正確
archive 路徑，以最多八個執行緒做完整 metadata binding；檔案雜湊與原始
還原仍使用原本各自的並行限制。其餘路徑仍以單執行緒掃描。固定大小的
排隊上限、完整樹、硬連結、例外、source/cold/peer/process gates 和過期檢查
均保留。新的工作才會載入程式，現有程序並未中斷。

對實際 round11 D archive 的一個 253 檔 shard，完整 `metadata_tree` 的
4／8／8／4 執行緒依序耗時 0.525／0.473／0.410／0.529 秒，輸出完整清冊與
fingerprint 相同；中位數為 0.527 與 0.442 秒，約減少 16%。這是單 shard
並行掃描的局部測量，不能推算整批驗證或回收完成時間。

## 已執行及保留

- Windows Temp 中兩份 FortiClient 安裝檔都是有效簽章、大小 200,368,128
  bytes，SHA-256 一致且沒有執行中的 Windows Installer。確認獨占開啟後，
  已刪除非版本命名的一份、保留另一份，並驗證留存檔案 hash。收據為
  `artifacts/operations/storage-single-authority-20261025/windows-temp-verified-duplicate-installer-20261028.json`。
  這是 Windows C 的空間，不是 WSL 回收量。
- 受管理資料快取 GC dry-run 與編譯快取稽核皆無符合條件的候選。
  `/var/tmp/stockagent-r12-json-20261027` 約 6.5 GiB，含備份目錄和帳本
  歷史測試快照；目前沒有完整冷庫驗收，予以保留。另一個 764 MiB 的
  packed metadata SQLite 確認正被程序開啟，也予以保留。
- 當沖狀態檔大小約 5.7 MB，實測 10 秒增加 13 次 revision，應用程式寫入
  量可能明顯大於檔案淨成長。這是寫入放大的線索，不代表每次重寫都可省略；
  未更動交易帳本、訊號、即時行情或執行中的服務。交易資料與資源維持最高優先。

## 11:10 補充：完成的測試和實際回收

- 三個相關儲存測試模組共 **227 passed**。本次實際測試生成的
  `/tmp/pytest-of-root/pytest-1246` 已完成且不是 `pytest-current`；先固定
  3,411 個一般檔清冊，確認無程序與 inode 外部硬連結引用、隔離後指紋一致，
  再逐個名稱回收。包含一個該測試刻意建立的 FIFO，其身份與路徑另有檢查。
  該目錄位於 `/tmp` 的 **tmpfs**，回收 inode 配置量 **702,922,752 bytes**
  （約 670 MiB）是記憶體／交換空間的暫存回收，**不是 WSL ext4 磁碟回收量**。
  清冊與收據在
  `/var/lib/stockagent/wsl-cleanup-20261028/pytest-1246.inventory.json` 及
  `/var/lib/stockagent/wsl-cleanup-20261028/pytest-1246.retirement.json`。
  冷物件與 NAS 快照回收量均為零。這是本次已完成的本機測試暫存清理，
  不是 round7／round11 的完成收據。
- 使用 round11 固定 D 編碼資料的 128 個小檔案，對每次的檔案身分和
  encoded SHA-256 做完整比對，1／4／4／1 執行緒實測依序為
  19.79／2.31／2.84／9.36 秒；各次 128 檔結果相同。兩組中位時間
  約 14.58／2.58 秒。這只是該抽樣，不是整批回收時間或速度保證；正在
  執行的 round11 仍是舊版單執行緒，既有後續 owner 才會載入新版。
- 11:09 的單一執行緒 round11 `verify-materialized` 實際抽樣位置約
  9,376／59,686 個檔案；最近 20 秒約 6.3 檔／秒，若速度不變，此階段
  剩餘約 132 分鐘。這不是整批 ETA，後續還有原始內容、使用者、peer、
  隔離和回收驗收。round7 正在隔離後驗證；原有 1,500 秒期限不會延長，
  若期限到仍未完成，會保留隔離並由同鎖後續工作重新驗證。
- 11:10 可用空間：WSL 約 257.63 GiB、C 約 33.51 GiB、D 約 397.32 GiB。
  其他程序同時持續寫入，`df` 變化不能等同上述 inode 回收量；非 sparse
  VHDX 內釋放檔案也不能當作 C 槽已回收相同空間。

11:12 再次觀察，round7 的隔離後逐檔驗證約 44,224／78,393；round11 的
編碼檔核對位置約 11,439／59,686。這些都是各自階段的執行中取樣，尚無
兩批正式回收收據。同期 WSL 約 257.59 GiB、C 約 33.08 GiB、D 約
397.31 GiB 可用。Windows Temp 中約 32 GiB 的 `swap.vhdx` 是現用 WSL
交換空間；保留且沒有更動。當沖、Discord 和來源事件服務均在執行，
近期單次行情請求可用 95／95，這不是對全部資料或交易結果的驗收。

## 12:13 補充：重複測試輸出回收與稽核加速

已確認 `r12` 私有測試成功完成，四組 retained 檔案與六個測試輸出的完整
SHA-256 逐一一致。先固定清冊與 producer 收據、檢查程序、FD、mmap、inode
和硬連結，再隔離並按精確名稱刪除六個重複輸出；實際最後 inode link 釋放
**4,143,955,968 bytes**（約 **3.86 GiB**）的 WSL ext4 配置空間。
原始 frozen catalog／ledger 和各一份 candidate 保留，D 冷物件和 NAS
快照刪除量均為零。每個被刪除檔案對應的保留副本、指紋、程序及配置量見
`/var/lib/stockagent/wsl-cleanup-20261028/r12-duplicate-publication.inventory.json`
和 `r12-duplicate-publication.retirement.json`；這批是**完成的效能測試
暫存重複副本**，不是原始來源或上述兩批 benchmark 已完成回收的證據。
刪除後另外獨立讀回四個留存檔案，確認 SHA 一致及六個刪除名稱不存在，
證據為同目錄的 `r12-duplicate-publication.readback.json`。

round7 在相同固定 D 版本重新做獨立還原，已完成 78,393 個原始檔的解碼，
之後還要逐檔核對解碼後的 D 編碼目錄、完整樹、隔離區來源、同步、引用與
刪除。12:13 的 decode 後 encoded 核對為約 9,335／78,394；它只是這個階段，
不代表可刪。round11 舊程序仍在完整稽核，12:08 依執行位置抽樣約
41,938／59,686；這是位置估計而非正式進度收據。兩批都仍無最終回收收據。

`legacy_artifact_archive` 的 encoded tree census 現在以自己的 metadata 階段
報告進度；看到多餘檔案時不顯示虛假的 100%，metadata 計數永不宣稱 byte audit
通過。僅針對既有明確登錄的 D archive、且內容解碼為四執行緒的路徑，這個
獨立 metadata census 使用八執行緒，並仍保留全部檔案、失敗與符號連結檢查。
內容解碼、SHA 與原值核對仍遵守原限制。實際 round11 D archive 的 253 個
檔案 shard，4／8／8／4 執行緒的完整 census 為 1.588／1.319／0.934／
1.700 秒，中位數約 1.644／1.126 秒，集合一致；約減少 31%，僅限此 shard。
既有工作程序不重啟；新程式供之後啟動的 owner 採用，不能推算整批完成時間。
儲存 archive 測試模組的完整執行結果為 **108 passed**，包含原始資料損壞、
編碼樹多餘內容、symbolic link、異動及平行驗證邊界。

12:13 的 WSL 可用空間約 311 GiB、Windows C 約 32 GiB、D 約 398 GiB。
可用空間受其他工作同時變動影響；WSL ext4 刪檔也不等於 Windows C 的 VHDX
實體空間立即縮小。當沖服務持續運行，近期行情請求單次可用 60／60；
這不是全部交易或資料完整的驗收。

12:18 再查 WSL 約 304.5 GiB、C 約 30.9 GiB、D 約 397.6 GiB 可用；
round7 的 decode 後 encoded 核對為 26,795／78,394，round11 在 12:17 的
執行位置抽樣約 46,383／59,686。兩個數字都只代表各自階段，兩批的
`progress.json` 仍為 `all_selected_retired: false`、回收 0 bytes。
12:13 至 12:18 的 WSL 可用量從約 311 GiB 降到約 305 GiB；五秒程序 I/O
抽樣顯示當沖服務有約 42 MiB 寫入，但重寫次數與淨配置成長不同，因此不足以
判斷可用量下降的原因。當沖沒有被中止或降低優先級。
12:19 round7 同一階段的進度為 30,839／78,394；尚未出現兩批的正式完成收據。

## 13:03 補充：已完成的測試暫存與兩批大型驗證

`r161`、`r169`、`r180`、`r182` 的執行收據均成功，分別保留 917、582、
576、768 個成功測試的 JUnit、摘要和執行收據。核對 XML 的測試數與原摘要，
`r169`、`r180`、`r182` 的 XML SHA-256 與原收據一致；`r161` 則保存此次
量測的 XML SHA。四個已完成、位於 `/var/tmp` 的 pytest `--basetemp` 先做
逐檔清冊、檢查程序／FD／mmap 與外部硬連結，並在持有原執行鎖時隔離，
重新比對清冊後僅刪除精確名稱。共刪除 **110,477 個一般測試暫存檔**，
最後連結實際釋放 **564,895,744 bytes**（約 **538.7 MiB**）WSL ext4 檔案區塊。
目錄自身區塊不計入此數；先前 `/tmp` tmpfs 與 r12 重複輸出是另外的成果。
已另外讀回四份保留的執行、摘要與 JUnit SHA，並確認原暫存與隔離名稱不存在。
失敗的 r162、r166、r168、其他未證明用途的暫存、D 冷物件及 NAS 快照均保留。
清冊和逐批收據位於 `/var/lib/stockagent/wsl-cleanup-20261028/pytest-r{161,169,180,182}.*.json`。

round7 的固定 D 版本已完成 **78,393 個原始檔的獨立還原與解碼**；13:03
的執行中抽樣已通過此輪 materialized encoded 檔 SHA，正在做完整目錄 metadata
掃描，之後仍有隔離區原值比對和刪除前 gate。round11 的執行中抽樣位於
原始檔解碼與來源核對階段，當時約 **2,319／59,686 檔**；這是程序堆疊
取樣而非持久進度收據，也不代表整批完成比例。兩批尚無正式 retirement
acceptance，候選空間約 **13.51 GiB** 與 **12.48 GiB**，不能列為已回收。
原有兩個低優先程序與等待接手的 owner 均保留；等待期間的稽核不跳過 SHA、
pin、程序引用、完整還原或同步門檻。12:13 記錄的八執行緒 metadata census
僅供之後啟動的相容 owner 使用，不能推算既有程序的整批完成時間。

13:02 WSL 約 302.8 GiB、C 約 29.7 GiB、D 約 390.6 GiB 可用；各處同時
有其他寫入，不能用 `df` 差值代替上述逐 inode 回收數。C 的實體空間也不能
由 WSL ext4 釋放量推算。五秒抽樣中 PostgreSQL 主程序統計的寫入量較高，
但此數可包含已等待子程序的 I/O 且不等於淨增長；當時其 DB base 約 227 MiB、
WAL 約 33 MiB，尚不能歸因 WSL 可用空間變化。當沖、Discord 與來源事件服務
持續 active；13:03 一次開盤訊號行情請求回覆 41／41，這是單次請求而非所有
行情完整性的證明。沒有恢復批次下載或調低當沖優先級。

13:04:44 的 round11 執行位置抽樣為約 3,119／59,686 檔；round7 仍在
materialized 目錄的 metadata 掃描。此數是各自的子階段而非整批進度。

## 13:44 補充：round7 本機回收與 D 暫存稽核加速

round7 已有原 owner 的 `retirement.json`：`deleted: true`，實際釋放
**14,506,016,768 bytes**（約 **13.51 GiB**）WSL 檔案區塊；另讀回
退休狀態 `cold-only`，來源目錄已不存在。指定固定版本的 D 正本和獨立還原
已驗，但收據為 `backup_verified: false`、`resilience: single_d_volume`，
不能據此宣稱 NAS 完成備份。該 owner 正做後續 D 編碼暫存的完整稽核與回收，
最後 `acceptance.json` 尚未出現；原 `progress.json` 仍是尚未收尾的舊快照，
不能用它否認 `retirement.json` 已記錄的實際本機回收。

同時間 round11 原 owner 在對固定 archive 解碼並逐檔核對本機來源。13:44 的
程序堆疊抽樣約 **42,363／59,686 檔**，約 10.88 GB 原始內容經過該階段；
尚有後續完整樹、來源、冷庫與退休 gate。這不是持久進度收據，也不是總 ETA。
13:47 的對應抽樣為 45,477／59,686。另有 round6 清冊候選約 9.20 GiB，
其中三個歷史 symlink probe 包含絕對路徑與一個預期不存在的目標；現行 legacy
archive 不支援逐一還原此類成員，因此保留，不能以一般檔回收證明代替。

本次實測採交錯 ABBA，均在兩個原工作仍運行、低優先的條件下讀取同一批 D
暫存檔：

- 固定 512 份 receipt 的 metadata，以 4／8／8／4 個執行緒耗時
  1.230／0.678／0.664／1.057 秒，兩組中位數 1.144／0.671 秒；每次完整
  身分欄位一致。
- 固定 256 份 receipt，完整讀取 JSON、對原始 bytes 計 SHA、讀前後 inode
  身分，4／8／8／4 耗時 3.276／1.548／1.162／2.261 秒，中位數
  2.768／1.355 秒，結果一致。
- round7 的一個 335 檔 D payload shard，執行實際 `_stage_prune_signatures`
  並完整核對身分和 hardlink-aware allocated bytes，4／8／8／4 耗時
  1.584／0.717／0.795／1.092 秒，中位數 1.338／0.756 秒。

新的 stage-prune metadata／receipt 計畫僅在已核准的精確 D 暫存或同 owner
隔離路徑、且程序檔案描述元上限足夠時使用八執行緒。冷檔解碼與 SHA 的
四執行緒上限、完整的 cold/source/process/pin/peer 檢查及隔離前後完整樹
fingerprint 均不變；其他位置及 descriptor 空間不足維持原限制。這些數字
是小樣本，不能推算全流程加速或完成時間。正在執行的既有 owner 保留，不在
驗證途中中斷或熱替換；相容後續工作才會載入變更。

`test_legacy_archive_stage_prune.py` 與 `test_legacy_artifact_archive.py` 全部
**189 passed**；涵蓋 1／4／8 執行緒完整 fingerprint 與配置量一致、link／
FIFO／更動與不可讀錯誤拒絕、descriptor 與精確 D 範圍限制，以及冷庫還原前後
的一致性。新增測試第一次把 benchmark 附帶驗收檔的總數寫成兩個，修正為比較
同一固定版本在回收前後的完整還原結果後重跑通過；不隱藏第一次的失敗。
之後補上目前已用 descriptor 的餘裕檢查；不足或 `/proc/self/fd` 不可讀時
退回四執行緒。針對這項變更及 stage fingerprint／完整冷庫讀回再跑的
**24 項測試全部通過**；189 項的執行時間在餘裕檢查加入之前。

另外以低 CPU／I/O 優先級執行 `test_local_offline_artifact_retirement.py`
與 `test_legacy_artifact_retirement.py`，**370 項通過，耗時 912.51 秒**。
其中涵蓋實際隔離、中斷接續、final gate 及冷庫還原；這輪在上述 descriptor
餘裕補丁加入之前啟動，補丁本身由後續 24 項測試涵蓋。
