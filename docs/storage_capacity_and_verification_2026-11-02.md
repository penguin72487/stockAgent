# 2026-11-02 備份容量、完整驗證與回收

本次接手 `storage-single-authority-20261025`。下列容量、程序及計數為當次
觀測，固定來源驗收、NAS 驗收及本機釋放量分開計算。沒有修改 factorized
產生器、訓練契約、當沖帳本或下載來源原值。

## 17:27 續行：停止無法配置時的重複全量清點

正式原始資料還原仍為 **60／61**，最後一組 `tw-index-futures` 正在
封存；本次修復的是等待循環及其顯示，沒有把等待或預驗證算成完整驗收。
70 組舊效能測試副本的 157.18 GiB 為前述政策累計，本次續行沒有新增
正式來源刪除。當沖及原大型發布 owner 均未重啟。

### 本次定位與修復

原公開備份在 D 槽低於 64 GiB 保留空間時，仍擷取約 2 GiB catalog，
重新解析約 4,500 份 manifests，最後才拒絕新增傳輸。一輪先前觀測為
**792.70 秒**；17:01–17:17 的舊程序亦持續約 16 分鐘。這些工作不能
讓容量不足的傳輸開始，反而和主要封存爭用 D 槽。

現在同一 BackupStream owner 先恢復既有發布、接收配對 ACK、執行原
驗證過的傳輸副本回收，再用新鮮實體可用量決定是否需要清點。容量
不足就延後 auxiliary 來源 hash、catalog capture 及傳輸 census，保留
既有完整覆蓋計數、identity 與原觀測時鐘，另寫活動／容量時間。舊
auxiliary 失敗也保留。這只決定現在不能寫入；空間恢復後仍重建完整
清冊，原配置、完整 hash、NAS 及刪除門檻全部重做。

原 timer 已自行載入：17:18 第一次等待循環 **10.69 秒**，17:26 後續
循環 **5.99 秒**。這是容量阻塞時省掉的空轉，不是整體備份或完整還原
快了同樣倍數。實際工作量、NAS 已驗證 bytes 及原進度時鐘沒有因此
增加，原留存空間也沒有降低。

私有備份現另記接收許可的原量測、有效期限、本次檢查時間。網頁分開
顯示「D 槽容量不足」與「接收許可已過期」；每分鐘重新觀察不能延長
接收端的 900 秒許可。公開 API 僅投影白名單欄位，不交付私有狀態。

### 三節點、容量與剩餘階段

| 範圍 | 17:26–17:27 的實際觀測 |
| --- | --- |
| Penguin 原來源封存 | 3,373,234／4,601,669 檔，421,804,080,273／469,259,065,729 原始 bytes；小檔桶 11／16，`acceptance: false` |
| Vast 原 compute spool | 595／596 個已發現封閉包已完成預驗證，一個新包在途；其中既有私有 256／256、共用來源 339／340 |
| 登錄來源 NAS 覆蓋 | 1,667,105,778,389／1,788,337,701,383 bytes（93.22%），105,789／108,105 檔；原計數 17:17 |
| 私有來源 NAS 覆蓋 | 718,647,796,617／817,226,447,345 bytes（87.94%）；原計數 13:05，活動時間仍每分鐘前進 |
| WSL 可用 | 359,276,666,880 bytes，約 334.60 GiB；虛擬 ext4 可用，不代表 Windows 可配置量 |
| Windows C／D 實體可用 | C 約 21.95 GiB；D 約 18.80 GiB。D 仍低於私有 32 GiB、公開 64 GiB 傳輸門檻 |
| lab203 | 17:26 容量回執在線；CPU／RAM、完整遠端排程與分階段工作量尚未提供 |
| NAS | 13:22 的舊容量值保留並標記過期；17:24 新 guard 嘗試為 `SnapshotError`，不能描述成即時可用 |

17:32 的後續公開讀回已到 **3,497,093 檔、426,423,203,499 原始 bytes、
小檔桶 12／16**；Vast 為 **599／600** 包，原排程正在處理新包。
D 的來源端最近檢查已降至約 **15.61 GiB**。目前進度仍前進，但容量
阻塞尚未解除；不把其他暫存位置當作 D 主冷庫已可退休的證明。

NAS 兩種覆蓋有交集，不相加。登錄來源分母隨封閉新包增加，比例下降
不代表已接受的 bytes 遺失。Vast 仍以既有兩槽管線重疊傳輸與遠端驗證，
新包封閉即自動派工；當前來源尚未產生的包不能預先驗證。GPU 空閒符合
這段完整 SHA／解壓的 CPU 工作，不以製造重複工作提高利用率。

17:26 原速度樣本對「剩餘來源內容」的中間估計約 **89.5 分鐘**，
約台北 **18:56**；當次區間約 **18:21–20:57**。這些只估剩餘來源處理，
未包含最後提交、完整還原、NAS 及 reader／writer 切換。NAS 等待容量
與接收許可，不能提供可信的全流程完成時間；網站顯示阻塞及暫停 ETA。

額外唯讀清點 D 上 `.stockagent-workflow-speed-20261021` 時，約八分鐘
仍未完成。已停止這個本次啟動的 `du`，保留原發布、驗證及回收服務，
避免盤點本身持續競爭 HDD。兩個小型 archive benchmark 各約 106 MB；
其餘候選尚無完整清冊，沒有依目錄名稱刪除或宣稱騰出其容量。

### 實際載入、回歸與網頁驗收

- 公開／私有備份原 oneshot timer 已讀到新版；只讀儲存 observer 與
  公開 gateway 已重載。當沖 PID 366、隔夜 PID 2021、原 lifecycle
  PID 2610446 保持。其餘受影響的原 timers 繼續自動執行。
- 備份回歸 **193 passed**；監控／投影回歸 **244 passed**；control role
  回歸 **190 passed、7 skipped**。最後的備份測試在目前程式上另跑
  一次仍為 193 passed。不是將重跑的同一批測試加成不同驗證範圍。
  固定發布邊界與候選變更拒絕另有 **4 passed**；兩次舊重載失敗以
  保留原失敗的 `expected-rejection` 收據對帳，不改成執行成功。
- 1440／390 viewport 均無水平溢出或 JavaScript 錯誤；接收許可過期
  文字實際可見。頁面保持開啟 **58.67 秒**後自行更新，三節點各保留
  自己的來源時鐘。第一次 gateway 尚未重載造成的等待失敗保留，
  見[瀏覽器驗收](../artifacts/storage_layout/storage-capacity-browser-20261102.json)。
- [連續公共讀回及原 owner 時鐘](../artifacts/storage_layout/storage-capacity-minute-proof-20261102.json)
  保留實際容量、coverage identity、原覆蓋時間與新的活動時間。
  HTTP 成功不代表完整備份或可退休。

先前固定候選 `6171841f…` 與本次第一個重載請求因後續實作改變候選
fingerprint 而正確拒絕，失敗收據保留，沒有重啟原大型工作。現在的
固定候選為 `118ed59773d3c5e6bfe68a1204837e393d9b6687d18491f49a3f15f241739765`，
原 rollout 在 `tw-index-futures` 已提交邊界後自動交接，**目前仍等待，
不是已載入**。見[目前邊界等待](../artifacts/storage_layout/storage-capacity-final-boundary-reload-20261102-waiting.json)。

請將[更新後的 lab203 交接](lab203_minute_schedule_handoff_2026-11-01.md)
交給該節點 Codex，由原 owner 修復 NAS guard、恢復真實接收許可並補上
遠端分鐘指標。Penguin 沒有該節點管理入口，也不以來源端新時間冒充
接收端許可。容量或許可恢復後，原排程會繼續，不需要再逐批手動下指令。

## 13:13 續行：70 組測試副本清理完成

固定政策的 **70／70 組**均已完成，失敗及保護阻擋為 0；刪除交易累計
實際釋放 **168,772,026,368 allocated bytes（157.18 GiB）**。這是此政策
從第一組開始的累計成果，不是本次回覆又釋放同樣大小。
最後 `round159` 有 29,992 個一般檔案，另有測試 FIFO、符號連結與非 UTF-8
名稱，實際釋放 182,087,680 bytes。原 timer 已自行續跑成功，沒有手動
繞過 owner 或直接刪除正式資料。

本次修正：僅已授權的本機測試副本可將閒置 FIFO 作為不讀取的名稱移除；
正式來源、一般保全及 socket／device／跨檔案系統仍受原保護。小型結論
收據只讀一般檔，不會卡在 FIFO。discard 稽核用 JSON escape 精確保存
POSIX 非 UTF-8 名稱，不改 source／packed manifest 編碼或 plan identity。
同一程式世代錯誤三次便停止；程式修復後先保存原失敗，再准許有限重試。
來源／世代／程序引用檢查仍全部重做，不清除歷史錯誤來冒充成功。

13:09 的公開容量觀測：WSL 可用約 337.58 GiB，Windows C 實體約
29.61 GiB，D 約 31.74 GiB。三者是不同容量界線，不能相加。
`fstrim.timer` 的 13:04 執行已成功，但這不證明 VHDX 縮小或 C 回收同額
空間；本次沒有停用 WSL、重新配置 VHDX 或中斷當沖服務。

### 三節點與正式備份

| 工作 | 13:09 附近的實際範圍 |
| --- | --- |
| Penguin 固定來源還原 | 60／61；最後 `tw-index-futures` 還在整理 4,601,669 檔、469,259,065,729 bytes |
| 最後來源內容處理 | 901,386 檔、326,875,893,071 bytes；小檔桶 3／16。各分母代表不同成本，不混成完成率 |
| Vast 既有遠端暫存／預驗證 | 13:10 已完成 568／568 包、249,064,139,270 encoded bytes、失敗 0；新包到達即沿同一 queue 派工 |
| lab203／NAS 登錄來源 | 1,667,105,778,389／1,776,260,419,099 bytes，93.85%；原計數時間 12:59 |
| lab203／NAS 私有來源 | 718,647,796,617／817,226,447,345 bytes，87.94%；原計數時間 13:05 |

兩組 NAS 範圍重疊，不相加；在線、預驗證及傳輸完成也不是 NAS 獨立還原
或原始來源可刪除的證明。現在主要耗時是最後來源的本機清點／封裝及
後續獨立讀回。Vast 有空閒時，不能藉增加 GPU 使用率或重送已完成包
加快尚未產生的上游工作。已存在的兩槽管線繼續將上傳與遠端解壓重疊。

網頁以逐檔和 bytes 的較慢估時顯示「剩餘來源內容」；13:09 該階段暫估
約 9.3 小時，速度波動大。這不是全流程完成承諾，還不含最後提交、完整
還原、NAS 或 reader／writer 切換。舊工作計數沒有更新時暫停 ETA，
不因每分鐘重新讀取而偽造新進度。當沖 PID 366、隔夜 PID 2021、期貨行情
PID 371 及原 lifecycle PID 2610446 保持運行。

### 重複稽核讀取與部署

來源複寫每輪原本重讀 1,687 份回收稽核，合計 102,561,934 JSON bytes，
只取四個排程結果欄位。現在原 owner 可保留最多 4,096 份小型 terminal
projection；每次核對 inode、type、uid、link count、size、mtime、ctime。
檔案替換、改寫或消失即失效，非 terminal 不快取，呼叫者也不能改壞快取。
這不供應 payload SHA、NAS ACK、刪除權限或新的工作時鐘。

唯讀 ABBA 的四欄結果逐項一致：原路徑 0.6707／0.7994 秒，第一次填入
0.8956 秒，暖快取 0.0519 秒。第一次較慢；暖快取只省掉這段重讀，不能
宣稱整體備份加速 15 倍。見[原始四輪證據](../artifacts/storage_layout/closed-retirement-receipt-cache-abba-20261102.json)。
正確 control role 回歸 **190 passed、7 skipped**；第一次用 fintech role 的
9 個 `temporalio` 缺件失敗保留，不修改正式環境或跳過這九項。測試清理
的 FIFO、名稱與重試恢復回歸另有 **142 passed**。

新版已由原 rollout 排在 `tw-index-futures` 固定 snapshot／manifest 提交
邊界後自動載入；目前只證明等待中，**尚未證明載入**。固定候選為
`6171841ff8cbbe14e2c9178bbcc9af9c4a101ef8a1ff538d9e1f36691afd4890`，
保留原工作 journals，候選或 operation 改變會拒絕交接。
見[交接等待收據](../artifacts/storage_layout/storage-latest-boundary-reload-20261102-waiting.json)。

公開頁面已讀到 70／70 與 157.18 GiB；Penguin、lab203、Vast 的各自
觀測時鐘持續前進。原傳輸、備份、驗證、回收及遠端 spool timers 均保持
自動執行。lab203 的 CPU／RAM、完整遠端排程及分階段分母仍缺固定樣本，
請其 Codex 沿[原交接文件](lab203_minute_schedule_handoff_2026-11-01.md)
回傳；Penguin 不以在線代替這些量測。見[公開讀回](../artifacts/storage_layout/storage-flow-review-public-second-20261102.json)
及[每分鐘連續取樣](../artifacts/storage_layout/storage-flow-minute-readback-20261102.json)。

## 11:10 續行與直接清理

以下更新取代後文早期觀測的「尚未完成」狀態，早期數字保留作為時間序列證據。

使用者明確允許刪除舊效能驗證資料，不必先備份。因此固定
[70 組舊測試副本](../configs/data_sync/local_testing_artifact_disposition_20261102.json)，
沿原 discard owner 檢查正式引用、FD／mmap／cwd inode、pins、世代與最後
unlink，直接處理。不是把測試資料另外封存一次。來源資料、帳本、正式
設定與 D／NAS 正本不在這份刪除授權內。

11:10 已完成前兩組，實際 last-link allocated-byte 回收如下：

| 固定舊測試根 | 實際釋放 WSL bytes |
| --- | ---: |
| `operations/incremental_followup_20261027` | 21,344,768,000 |
| `benchmarks/workflow_speed_optimization_20261021/round4` | 27,406,680,064 |
| 合計 | **48,751,448,064（45.40 GiB）** |

11:15 的後續固定讀回已完成 **6 組，80,355,483,648 bytes（74.84 GiB）**，
尚在接續其他根，當次失敗及保護阻擋皆為 0。這是刪除回執的累計量，
不是用 `df` 前後差推算；並行寫入使可用容量的淨變化不同。
見[逐組實際刪除與服務讀回](../artifacts/storage_layout/storage-local-cleanup-actual-readback-20261102.json)。

第一組兩個外部連結目標仍保留，原 `openbb_incremental_proof_20261027`
仍存在。連結指向範圍外代表另一個位置；不代表目標不存在。對這些已授權
測試根，只刪連結名稱，包含原本已斷掉的連結，不跟隨外部目標。

固定候選原盤點約 161.75 GiB，僅是候選觀測，不是全部可刪或已釋放。
[逐根盤點 CSV](../artifacts/storage_layout/storage-local-cleanup-reviewed-targets-20261102.csv)
與[執行狀態](../service_data/storage-discard/performance-20261102/status.json)
分開保存。數十萬小檔與單一大檔不能按「完成幾組」外推同一速度；未有
完整工作量時，網站不承諾全批完成時間。固定政策不自動刪除同一路徑
重新產生的新世代。

原 `stockagent-wsl-retirement-queue.timer` 每分鐘自動續行同一清理實作，
仍持同一 mutation owner；已有清理執行者時不重掃、不強搶鎖。明確錯誤
每根最多三次，保留原因並繼續不相依目標。第一次跨 bind mount 的
`EXDEV` 未刪來源；已修正為同掛載、來源旁的私有隔離目錄，保留原失敗。

目前非測試資料的主線：

- 原來源遷移 **60／61** 已驗證，最後 `tw-index-futures` 仍在發布；
  4,601,669 個來源檔、469,259,065,729 bytes。正式 writer／reader 切換與
  原始來源退休未完成，不能把原資料根直接刪除。
- D 的加密貨幣與美股舊編碼均完成完整還原及退休，共釋放
  **63,477,919,744 bytes（59.12 GiB）**；這是 D 空間，不能加成 WSL
  本次釋放量。美股該次 32 個驗證工作實際全部在 Penguin，不能算成
  Vast 成果。另一組期貨的 168 個驗證工作亦全部在本機。
- Vast 原 compute spool 到 11:08 已預驗證 **538 packs**，等待／持續
  接收新的封閉 pack。候選計算不等於整份來源發布、NAS 驗收或可退休。
  原 verifier 握手重測 0.817 秒成功，沒有據此宣稱遠端完整工作已驗收。
- private NAS 覆蓋 675,835,232,215／817,226,447,345 bytes（約 82.70%），
  登錄來源覆蓋約 93.86%。這兩個範圍重疊，不能直接相加。private 原
  owner 已恢復多波傳輸；發布封套與 NAS 配對 ACK 分開計算。
- 私有封閉波次現在按已授權備份 pool 使用 native 32 GiB floor；一般
  下載與其他流量的 64 GiB reserve 不變。staging ceiling 24→64 GiB，
  舊失敗 staging 約 26.06 GB 全部仍計入，沒有藏掉或另開傳輸通道。
  11:07 SQL 容量檢查仍差約 205 MiB；其他發布寫入可能改變這個差額。

本次 WSL 檔案刪除不會自動壓縮 Windows 上的 VHDX；C 實體可用不能把
WSL 虛擬可用相加。網站保持每分鐘觀測／頁面刷新，保留各節點自己的
來源時鐘。lab203 CPU／RAM 仍未回傳，不能以回執在線代替這兩個量測。

驗證：discard、materialized cache、process scope、進度投影合計 290 項
測試通過；最新 ETA／直接刪除投影另有 152 項通過。原當沖 PID 366、
overnight PID 2021、TAIFEX dashboard PID 380、lifecycle PID 2610446
保持；只重載只讀 observer 與公開 gateway。第一次公開讀回正好遇到
gateway 啟動而回 502，11:08 已恢復並讀到新的清理卡片；瀏覽器驗收
另有 1440／390 兩種 viewport 通過，原生展開儲存摘要、顯示實際釋放量、
無頁面錯誤及無水平溢出。最後驗收亦確認清理卡片列在進行中工作最前，
摘要納入直接刪除量；頁面保持開啟 **59.10 秒後自動刷新**，不需手動
重新整理。初次未展開最外層儲存區造成的等待失敗保留，
不能當成成功。見[瀏覽器實際驗收](../artifacts/storage_layout/storage-local-cleanup-browser-20261102.json)。
11:08 與 11:12 的公共讀回確認三個節點各自的來源時鐘前進，
見[第二次公共讀回](../artifacts/storage_layout/storage-local-cleanup-public-second-20261102.json)。
只讀 API 與畫面驗收皆不代表來源已完成備份或可刪除。

## 主要阻塞

07:19 起的檢查確認 D 槽實體可用為 0；08:06 的 `df -B1` 仍為 0。
同時 WSL 虛擬可用約 198 GiB，Windows C 實體可用約 32 GiB。這兩個數字
不能相加或把虛擬空間當作新配置能力。D 無空間時，原發布器仍曾先走完
數百萬個檔案的 census 才在寫入階段失敗。

原固定來源工作共 61 組、59 組已驗證，剩下 `local-tw-futures` 及
`tw-index-futures`。後者的舊失敗保留 ENOSPC，前者保留 EIO；attempt 642
不是 642 次完整成功重建。尚無這兩組的完整新版本，也未授權其原始來源
退休。4,601,669 個候選檔案／469,259,065,729 原始 bytes 的檔案走訪與發布
是另一個主要成本，不能靠增加 Vast GPU 使用率消除。

08:07 的既有 Vast compute spool 已處理全部當時可用的 393 packs、
248,925,378,887 encoded bytes，失敗 0，正在等待新封閉 pack。這只是候選
計算證據，不是 NAS 驗收或來源退休。Vast 監控同時顯示 CPU 約 0.2%、
兩張 GPU 約 0%；當時缺的是可交付工作，不是更多 GPU。

## 已修正的路徑

1. 在原 private stream 及固定來源 migration 的昂貴 census 前增加實體
   容量負向檢查。D 不足先等待，不消耗一次完整嘗試；已提交版本的恢復
   仍可繼續。這是便宜的必要條件，後續完整容量與寫入門檻仍然保留。
2. 共用來源也沿原 publisher 保存 sealed-pack hints，精確綁定來源與世代，
   中斷後可重用已封閉 pack。舊未保存 hints 的候選不能回溯宣稱已取得
   可恢復 checkpoint。原 lakehouse owner 已在 07:37 載入此修正，保留
   59 個驗證結果及三條既有 workflow chains。
3. 原 enrolled-artifact owner 以前會永久跳過 `cold-only`；因此原檔已退休
   的編碼工作副本仍占 D。新增兩個逐項固定 enrollment：加密貨幣研究
   約 8.19 GiB、美股研究約 50.93 GiB。每組綁定 snapshot、manifest SHA
   與原退休收據 SHA，完整冷庫還原、原值 SHA、程序／pin、隔離及最後
   世代檢查全部通過後，才移除臨時編碼。其他來源不按目錄名稱推定可刪。
4. 原 oneshot timer 改為前次 owner 結束後 60 秒再檢查，不重疊執行。
   每個固定 binding 最多三次失敗、短暫重試並保留版本化錯誤；獨立 cohort
   可繼續，不把一組永久錯誤變成全域阻塞。
5. 對上述兩組編碼改用原 Windows native byte adapter，批次讀取／完整
   SHA，保留 Linux FD 與前後世代 fence。合併 native 程序啟動，不建立
   新資料正本。來源不符合 enrollment、FD 或批次容量條件仍走原路徑。
6. v2 owned proof 將完整編碼／原值驗證的 SHA 清冊與固定 cold inventory
   做精確聯集比對，獨立還原每個 cold file，再核對 portable metadata。
   同一世代不再額外整批讀取同一份編碼，並在原 1,500 秒時限內沿用原
   decode 結果完成隔離核對。時間戳不延長，任何身分、計數、世代、pin
   或新引用差異都阻止刪除。原 Penguin／Vast pack verifier 已逐項登錄
   兩組資料，實際分工仍須查該次 per-node execution 證據。
7. checkpoint 保全原本把 D reserve 不足當成 `rescue_rejected`／exit 65，
   現在只對這個具體容量例外回報等待、交回原 timer（exit 75）。資料損壞、
   讀取錯誤、來源 enrollment 及 runtime 差異繼續失敗；D floor 未降低，
   沒有啟動／轉移訓練。控制資料庫 NAS 備份亦仍要等 D 約 32 GiB floor
   加當次實測 dump 工作空間，第一組回收的 8.19 GiB 還不足以通過。

最新 native helper 的 lakehouse 載入另由原 publication-boundary handoff
等待 `local-tw-futures` 固定發布完成；不為載入額外 helper 再打斷正在
執行的大型 census。等待收據不是已載入證明。

## 已實測的效能與限制

- 六個固定 raw 編碼、505,719,232 bytes 的完整 SHA ABBA：原路徑
  74.69／75.77 秒，native 批次 5.11／11.46 秒，中位數約 9.08 倍。
  全 SHA 相同；這是這六個檔案的讀取比較，不是整體備份速度。
- 單檔 native 程序有 3–5 秒啟動成本，單檔實測沒有穩定勝出，故採批次。
- 另取 2,048 個固定 WSL 小型來源檔、完整 SHA 與前後世代比較：原序列
  約 0.20–0.21 秒，較小讀取 buffer 約 0.17–0.21 秒，四條 thread
  約 0.91 秒。故不對全部來源盲目加入 threads，也未把這個局部約
  0.03 秒差異當作整體發布加速；原路徑保留。
- 最後一輪五組相關語義回歸 455 passed，116.51 秒；另有原控制流程
  132 passed。保留錯誤入口與第一次瀏覽器等待競態的原失敗紀錄。
- checkpoint 容量分類及既有保全語義另有 117 passed，103.62 秒。
  08:21 的原 timer 自動執行已讀回 `waiting_d_capacity`／exit 75、
  systemd `Result=success`；未啟動訓練，原保全及備份門檻保持。
- 桌面 1440 與手機 390 的 31 項瀏覽器驗收通過，涵蓋分鐘更新、離開
  可視範圍後仍刷新、失敗／舊觀測提示、流程、排程與 ETA。

證據：

- [固定探針實際回收](../artifacts/storage_layout/storage-fixed-probes-apply-20261102.json)
- [完整 SHA 批次 ABBA](../artifacts/storage_layout/storage-reviewed-raw-native-batch-abba-20261102.json)
- [WSL 小檔序列／平行比較](../artifacts/storage_layout/storage-portable-source-hash-comparison-20261102.json)
- [455 項驗收](../artifacts/operations/agent-workflow/runs/storage-stage-fusion-final-20261-20261102T000230-b85d2695/run.json)
- [網站驗收](../artifacts/storage_layout/storage-encoding-browser-retry-20261102/browser_acceptance.json)
- [三節點來源時鐘讀回](../artifacts/storage_layout/storage-fleet-minute-clock-readback-20261102.json)
- [第一組編碼副本回收讀回](../artifacts/storage_layout/storage-crypto-encoding-retirement-readback-20261102.json)
- [checkpoint 容量分類驗收](../artifacts/operations/agent-workflow/runs/storage-rescue-capacity-20261102-20261102T002020-211a8f5a/run.json)
- [原 timer 容量等待讀回](../artifacts/storage_layout/storage-rescue-capacity-owner-readback-20261102.json)

## 網站與自動接續

[全資料面板](https://penguin72487.ddnsgeek.com/data-monitor/) 沿原每分鐘
observer、公共只讀投影與頁面 poll，顯示三節點來源時鐘、29 組排程、原
失敗、真正鎖等待及下一個相依條件。D 實體不足時，未完成發布／固定
版本驗證顯示容量阻擋與未知 ETA，不沿用失真的倒數。

兩組編碼回收已獨立列卡，區分 D 暫存回收與 WSL／冷庫正本；進度只讀
原 owner 的輕量狀態，不為顯示重跑 hash。階段有真實分母與速率樣本才
估計時間；metadata 階段不能直接拿 decode 的 bytes 外推整個任務。

07:51 與 08:06 的實際公共投影比對確認 Penguin、lab203 與 Vast 的來源
觀測時鐘均前進。lab203 CPU／RAM／遠端逐階段分母仍未由其原回執提供，
網站保留未知；接收在線與空佇列不能代表所有來源 NAS 驗收完成。需要
lab203 Codex 接手的欄位與不變 ABI 見
[既有分鐘補件](lab203_minute_schedule_handoff_2026-11-01.md)。

八組既有、完整讀回的 benchmark probe 已回收 544,309,248 allocated bytes
（約 519 MiB），位置在 D；並行發布器其後又使用了這些空間。
08:11:16 第一組加密貨幣編碼已通過原 v1 全部還原／隔離／退休門檻，
實際回收 8,795,815,936 allocated bytes，CAS 與 NAS 刪除均為 0。
隨後 D 實體可用讀回 8,795,918,336 bytes。第二組美股約 50.93 GiB
仍是待驗證候選，不能算成已釋放。原 owner 在第二組唯讀階段停止，
核對 inode、無 plan／隔離目錄及第一組完成收據後，08:11:38 換版續行；
v2 冷庫證據融合與既有 Penguin／Vast 分工由此組採用，不能回溯歸功於
第一組的完成時間。
本次尚未新增 WSL 原始來源退休，不把歷史已完成輪次的 16.45 GiB 算成
本次成果。完整全域完成時間仍未知，因兩組來源發布、NAS 獨立驗收及
來源退休尚有各自未完成的門檻。

08:24 公共讀回：期貨來源核對 1,302,404／2,000,034 entries（65.12%），
此階段中間估計約 12 分鐘、範圍約 8–20 分鐘；美股編碼原值核對
78,807,837,826／120,843,566,601 bytes（65.21%），此階段中間估計
約 5 分鐘、範圍約 3–16 分鐘。這些都只估當前階段；後續完整冷庫還原、
回收與 NAS 驗收不能共用這個截止時間。逐分鐘投影仍是持續更新的來源，
固定觀測見 [本次最後公共讀回](../artifacts/storage_layout/storage-progress-final-public-20261102.json)。
