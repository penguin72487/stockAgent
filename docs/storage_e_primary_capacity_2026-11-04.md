# E 主儲存、C 服務與 D 備份：容量及執行狀態

本文件依 2026-11-04 使用者最新指示：**WSL C 留服務，資料使用 E，D 保存備份**。
取代先前將進冷庫前的 E 僅稱為暫存區的位置建議；既有 D 固定歷史版本及其
SHA 不改名、不刪除，也不因角色名稱變更再完整複製一次。E 承接來源工作資料；
D 的封閉版本是可還原備份，lab203／NAS 是離機備份。遷移與備份各自驗收。

## 18:00 台北：依最新分工執行

本節是本輪執行計畫，不把後面的舊容量或一次 copy 當成全量遷移完成。
目前 E 已核對 ext4 UUID、block device 與 filesystem 均為 **3 TiB**；可用約
2.19 TiB。C 實體可用約 98 GiB，WSL guest 可用約 329 GiB，實際新寫入仍
受兩者及原 reserve 的較小值限制。D 可用約 37 GiB，必須先按既有內容定址
備份判斷實際新增 bytes；未知或不足時保留 E 原值並等空間，不把 E copy
稱為 D／NAS 驗收。16:22 原 Windows `CompactIfStopped` 收據顯示 C VHDX
檔案長度縮小約 62.91 GiB；15:37 的聯合維護仍是失敗收據，兩者分開保留。

順序與允許的並行如下：

1. **清冊與分工。** 用實際 mount、registry、活躍 owner、現有固定版本及
   source scope 為每個 C 原始根列出 E／D 對應和未驗證部分。程式、SQL、
   現行交易帳本、服務必要 projection、鎖與控制資料留 C；來源資料及
   可重建工作集往 E。原 provider 工作由另一視窗的原 owner 接續。
2. **E 安全搬移。** 獨立資料集可有界並行；對單一資料集先核對 E volume
   和完整實體／guest 容量、writer／reader／鎖及來源世代，再以原
   `workspace_relocation` 建立可中斷續跑清冊、複製及逐檔 SHA 讀回。
   來源變動或無法辨認 reader 時保留來源並重試／報錯；不得回落寫 C。
3. **服務交接與 C 回收。** 由原 owner 在確定可切換的邊界沿既有路徑
   交接到 E，核對各 reader 的 mount namespace、增量寫入與服務恢復。
   只有 E 完整版本、應有的 D 獨立還原、來源穩定及程序／pin／lease
   條件全部成立才逐項回收 C；已驗證 copy 的邏輯 bytes 不是釋放量。
4. **D／NAS 備份。** 沿現有 packed-v2/CAS 及原 catalog、Temporal、lab203
   owner 複寫封閉版本；只傳新增或改變的 objects，小檔有索引地打包，
   已壓縮 Parquet 優先保留原 bytes。D 需完整物件／原檔讀回，NAS 需
   配對獨立回執；接收端或容量不足時續跑其他獨立工作，不刪正本。
5. **讀寫效能與恢復。** 用分區／索引選出需要的資料，批次順序讀 pack，
   日常只處理變動分區；重建保留來源 SHA、metadata 與失敗證據。依現場
   磁碟和負載測整條 copy／pack／fsync／讀回／恢復流程再調並行，不能
   用單次小樣本聲稱達到硬體極限。夜盤和當沖服務始終優先。
6. **自動化與進度。** 沿既有 task／timer 和 owner 的 durable journal
   接續；顯示每個資料集的來源、目的地、copy、讀回、切換、D／NAS 備份、
   C 回收、剩餘 bytes／檔數、阻擋及最後實際進度時間。未知速率或外部
   gate 不給虛構完成時間。電源中斷後先核對 identity、原 lock 與收據。

第一個獨立候選為 `data_binance_archive`：上次 WSL 清冊約 92.25 GiB，
2026-11-04 18:04 的 bounded process-reference 掃描未見目前持有者，E 已
通過 128 GiB 預算的容量准入。先用既有 source-copy owner 進行 E staging
與獨立讀回；不以這個觀測取代後續 writer fence、切換、D 恢復或 C 回收。

### 18:45 執行進度與下一個瓶頸

`audit_workspace_source_placement.py` 對 checkout 的 `data_*`、`service_data`、
`artifacts` 與選定服務／SDK 路徑作只讀位置盤點：44 個選定根中 41 個根目錄
位於 C、3 個位於 E；巢狀 mount 另列。這不是整台機器的遞迴容量清冊，舊
容量掃描不具原子性，也不能將父子目錄或 hardlink 數字相加。

沿既有 `workspace_relocation` owner 和 E volume／容量租約，以下固定世代已
完成來源 SHA 與 E 目的地逐檔讀回，尚未切換原路徑或刪除 C 來源：

| 資料 | 檔案數 | 邏輯資料量 |
| --- | ---: | ---: |
| Binance 公開歷史 | 117,416 | 92.01 GiB |
| OKX 歷史 | 2,090 | 35.86 GiB |
| Frankfurter 外匯 | 1,781 | 0.27 GiB |
| 合計 | 121,287 | 128.13 GiB |

收據與原始 journal 位於 E 的 `verify/*-e-primary-20261104-v1.json` 與
`migration/*-e-primary-20261104-v1/`。這是複製時的固定世代，不能推論後續
來源未變、D 完整讀回成功、NAS 已驗收，或已實際釋出等量的 C 空間。

對已驗收的 Binance／OKX E 工作清冊與 10/23 D packed 清冊按路徑、長度、
SHA 比較，分別有 116,013／1,112 個檔案相符，變更或新增的來源邏輯量
共約 27.19 GiB。該觀測未讀回 D 物件，亦未證明原始世代至今不變；
封裝、壓縮、舊 chunk 重用及 metadata 都可能使 D 實際新增量不同。D 當時
物理可用約 36.1 GiB，部分 D 寫入流程另有 32 GiB writer floor，不能按
差異邏輯量直接啟動整批備份。私有比較結果保留在 E 的
`verify/e-stage-vs-d-inventory-20261104-v1.json`。

`stockagent-workspace-source-staging.timer` 已啟用，每分鐘重試已登錄資料；
可安全並行的來源最多兩個，沿各自原 copy lock 和共用容量租約，不另啟
資料下載。它目前只覆蓋明確登錄的四個來源，尚不包括所有來源、切換、
D／NAS 獨立還原與 C 退休；任何這些階段不得由 staging 成功推論。查到
原程序或同一路徑的 argv 後等待原 owner，不能把一次程序掃描視為鎖。
選擇權日資料目前被夜盤程序引用而等待；期貨逐筆根包含夜盤輸出與
node-local runtime，須先以原 owner 分出封閉分區，尚未整根加入搬移佇列。

Windows 原生 E 容量檢查曾因 `UtilAcceptVsock ... accept4 failed 110` 暫時
失敗；當次沒有建立 payload，下一輪在重新測量物理容量後已完成外匯
複製。排程將這種特定橋接錯誤和原生檢查 timeout 顯示為等待重試，身份
不符仍為阻擋，不沿用過期可用量。監控已逐分鐘列出各複製階段、來源
等待原因及有證據時的階段 ETA；沒有可量測進度或外部驗收時不虛構 ETA。

### 19:26 接續：歷史 head 讀取與本機新聞 staging

原 backup-stream 程序的兩次非阻塞 stack 取樣顯示，原本耗時階段是 D 上
`head-history/heads` 的逐檔 metadata 讀取與 9p 往返；它仍有實際系統呼叫，
不是單憑等待狀態判成死鎖。沿用 `PinnedObjectSignatures` 的 no-follow、
完整內容與前後檔案世代核對，將此階段改為既有 metadata worker 上限內的
有界平行預讀；驗證、錯誤紀錄和 catalog 寫入仍依原排序執行，結束時仍
重核目錄身分。執行中的 owner 未重啟，既有 timer 已恢復；新程式只會在
下一次原 owner 啟動時載入，現階段不宣稱 production cycle 已使用新版。

固定 96 個實際 D 歷史 head 的唯讀 ABBA 樣本，單工平均 0.543 秒、
四工平均 0.272 秒，四輪完整讀回內容、檔案世代與目錄核對一致；
樣本共 50,156 bytes，沒有來源寫入或 NAS 驗收。它只量 metadata 讀取階段，
不能外推成整條備份或整機加速。結果在 E 的
`verify/backup-head-read-abba-20261104-v1.json`。241 個 backup history／
stream／packed-backup 測試通過，包含平行重疊、serial catalog parity、
損壞或 symlink 的錯誤證據。

以原來源清冊 `publish: false` 和原本機用途為界，`data_news` 已加入同一
E staging queue；不授權對外或 NAS 新增交付。19:26 的固定清冊為 11,697
檔、19,457,342,845 邏輯 bytes，正在複製，C 原值保留。copy、獨立讀回、
服務切換與任何將來可用的備份驗收仍分別呈現。台股選擇權仍等待夜盤
原使用程序；原排程根據程序參數及檔案引用重試，不停交易服務。

### 19:41 接續：已登錄來源、備份排程與固定版本差異

E staging 擴充為 11 個已審核來源；到 19:39 為止其中 10 個已逐檔
讀回，共 173,055 檔、195,208,384,305 邏輯 bytes（181.80 GiB）；
選擇權日資料仍因夜盤程序引用而等待。這不是全部來源、路徑切換或
C 實際回收量。新聞、加密 ETF 與 keyed catalog 的原 `publish: false`
權限不因放在 E 而變更。當來源 catalog 已登錄協作鎖時，staging 持有原鎖
再執行原 copy owner；鎖忙則等待，未知來源不能自動加入。監控將同一
工作群組的 E I/O 說明為群組累計，不能解讀成某個來源的驗證進度。

原 backup-stream 的 19:02 cycle 於 19:36 完成一次約 33 分 19 秒的
完整 catalog 工作；之後的 worker reload 曾等待 dataset-version 的獨立
owner，而這把鎖不由 backup-stream 持有。原 `publication_boundary` 等待時
會先釋放所有已取得的 partial locks，因此備份捕獲現在對該獨立鎖採與
SQL catalog owner 相同的並行政策；真正 sender namespace handoff 或
source-replication 邊界仍讓路，任何 source generation／NAS／退休 gate
均未改變。備份 timer 已恢復，19:41 新 cycle 已啟動；不能因服務啟動
推定新版每個階段均已完成或 NAS 已驗收。275 個 backup、packed 與 staging
相關測試通過，含獨立鎖等待時的真實 fixture cycle。

第二個只讀小樣本比較了 192 個固定 D CAS 物件的 metadata stat：
4／8／16 工平均分別約 0.381／0.236／0.193 秒；首次 4 工較慢、樣本偏小
且另有正式工作並行，不能外推成全流程結果，故沒有憑此調高正式
worker 上限。第一次 benchmark 的統計縮排錯誤有記在結果中，未寫來源；
正式結果為 E `verify/backup-cas-stat-abba-20261104-v1.json`。

可重複的只讀 `audit_e_staging_against_cold.py` 保留每個固定 E copy 與
D packed inventory 的路徑／SHA／長度差異，結果在 E
`verify/e-stage-vs-d-inventory-20261104-v2.json`；不代替 D 的完整物件
讀回或新增量的實體容量預估。它也揭露 CFTC 的兩個原始 ZIP 原本已依
closed-source 合約從 C 回收並固定在 D，因此 E 的 C 工作集副本本來不含
該兩個 ZIP。已啟動原 collector lock、固定 pin 和 canonical packed fetch
的獨立 E 還原；其完成狀態以 E 專用收據為準，不能由 staging copy 推定。

19:47 實際結果：CFTC 固定 release `cftc-legacy-pre2000-20261023T124625200401807Z-l0-penguin-012749c3fec4ef61`
的 10 檔、63,158,789 bytes 已使用原 collector lock、固定 pin 與 packed
fetch 於 E 完整還原並讀回；兩個 ZIP 共 42,178,743 bytes 另以新程序再次
完整 SHA 讀回。收據分別是 E 的
`verify/cftc-closed-release-e-20261104-v1.json` 與
`verify/cftc-E-second-readback-20261104-v1.json`。這是指定固定版本的 E
工作副本：C 原工作路徑、D 固定 release 與 NAS 皆未更改，也尚未完成原
collector 的 E 路徑交接。E 上 20 MiB 左右的舊 staging 工作集與該還原
副本暫時有重疊，後續由原 owner 核對並處理，不能把它稱為單副本完成。
面板 CFTC 資料列顯示兩份原始 ZIP 的讀回時間與大小，NAS 驗收另計。

### 20:08 接續：原寫入者鎖、擴充 staging 和期貨清冊重複觀測

來源 staging 原先持有 catalog 明列的協調鎖，但沒有取得 publisher 已知的
Binance／OKX／Bybit 分頻原生寫入者鎖。現沿用 publisher 的原生鎖清單，
並為有原程式證據的六個來源登錄其既有根目錄 `.download.lock`。同一鎖
按實際路徑去重，鎖忙時釋放已取得的鎖並等待，不停止下載器或改 source。
這些鎖保護的是新啟動的複製；既有完成收據的範圍仍是當時固定世代。
針對 native grain 鎖全部持有、busy 時釋放及 root collector 鎖的測試
已加入，workspace staging／storage progress／monitor 共 337 項測試通過。

同一既有 timer 新增 Binance 市場、crypto reference、公開 crypto context、
Dune 與 FRED crypto macro 五組。20:07 Binance 固定來源 inventory 為
2,398 檔、145,266,057,024 邏輯 bytes（135.29 GiB），已開始 E seed；
crypto reference 正在清點，其餘依原兩工佇列執行。此階段尚未完成全文
SHA 及獨立 E 讀回，更不代表 C 原路徑切換、C 可釋放空間或 D／NAS 備份。
新增來源中的既有限制用途仍沿原 catalog，E staging 不改變對外發布權限。

20:14 另確認 Coin Metrics 原 collector 沒有跨程序寫入鎖：新增其既有共用
`exclusive_dataset_lock` 契約，完整下載與本機 staging 都持有同一路徑的
`.download.lock`，busy 時不發出 provider request。17 項登錄來源中加入
Coin Metrics。20:22 以同一 `run_source`、原鎖、原容量租約及低優先級啟動
獨立 bootstrap，與既有 Binance 來源並行，不改 queue 狀態檔，也不打斷其
複製；後續 queue 下一輪會接手已有收據或依原鎖等待。Coin Metrics 清冊為
16,214 檔、17,208,192,468 邏輯 bytes，正在 seed，尚非讀回完成。
134 項 collector／共用 primitive／staging 測試與後續 250 項
staging／監控／collector 測試通過，這些測試未向提供者呼叫 API。

20:17 修正 queue 投影：啟動後及各來源完成／等待時立即持久化各自狀態，
大型來源不再使同輪其他來源的結果等到整輪結束；新登錄但未啟動的來源
顯示排隊，不冒充鎖衝突或進度心跳。這段 queue 程式在下一輪由原 timer
載入，當前 Binance 複製不中斷；監控與唯讀網站已載入排隊標籤。345 項
staging／storage progress／monitor／Coin Metrics 測試通過，含長工作尚未
結束時另一來源結果已寫出的實際平行測試。20:12 四個新來源皆已有完整
copy／readback 收據，與前十個合計 248,287 檔、195,979,919,247 邏輯
bytes；均未切換 C 原路徑、刪 C 原檔或因此聲稱 D／NAS 驗收。

20:27 從固定 SQLite inventory 分別檢查 Binance、Coin Metrics、crypto
reference 的實際 inode 分組，均沒有來源範圍內需保留的共同硬連結；
2,398／16,214／70,824 檔的查詢分別約 0.047／0.078／0.200 秒。
原 copy owner 現先使用該已建清冊判斷：有內部硬連結才使用原 rsync seed，
否則直接四工複製並保留完整來源 SHA、獨立 E 讀回與目錄核對。外部硬連結
不在所選範圍、不能視為 E 需重建的別名。這會避免無內部硬連結批次的
一次完整 seed，但此次 Binance 已 seed 完成，不中斷或重跑；新程式由
下一次原 copy 載入。278 個相關測試通過，包含內外部硬連結的不同結果；
尚不能把這項機制說成已實測的整批倍速。

20:32 在本機 C→E 以自建四檔、共 128 MiB 的非正式 fixture 做 ABBA：
原 seed 再完整驗證平均 0.831 秒，直接複製加完整驗證平均 0.795 秒；
四輪均執行來源 SHA、目的地讀回與 namespace 核對，fixture 已刪除。
測試樣本小、讀寫快取及其他服務仍有影響，兩方法差距不能視為整批已證明
加速；確定的效果僅是無內部硬連結時不再先進行多餘的全量 seed。測試
沒有讀取或刪除生產來源，結果在 E 的
`verify/workspace-seed-strategy-abba-20261104-v1.json`。

20:31 Binance 市場固定世代 2,398 檔、145,266,057,024 bytes 的
來源 SHA、E 逐檔獨立讀回及完整 namespace 核對已完成。包括已完成的
Coin Metrics，17 組已登錄來源中 16 組合計 266,899 檔、
358,454,168,739 邏輯 bytes；剩餘臺指選擇權日資料等待夜盤來源使用者。
這不是全部來源的完成比例，也不是實際釋出的 C 空間。C 原檔仍保留，
D/NAS 驗收與服務路徑切換還需原 owner 分別完成。
20:35 原 staging timer 的下一輪已讀到全部 17 項及新版 queue 程式，
16 項沿既有固定世代收據辨識為完成，選擇權來源等待原程序；這次排程
觀測不重新驗證舊來源的新鮮度，也不授權退休 C 原檔。

20:34 唯讀比對這份 Binance E 固定世代與 D 現有
`binance-20261023T142851467108114Z-l0-penguin-74482eb0462030c7` 清冊：
1,230 檔路徑、大小及 SHA 完全相符；1,162 檔內容／大小不同，這些 E
來源檔合計約 122.96 GiB，另 6 個檔案只在 E 清冊出現。這不是增量實際
寫入量：原有 8 MiB chunk、其他已存在物件及封裝可能降低新寫入量；也未
讀回 D 每個物件、未檢查最新來源或授權刪除。結果在 E
`verify/e-stage-vs-d-binance-market-20261104-v1.json`。

只讀分析 25 份期貨 prepared 版本 `terms/manifest.json` 的來源宣告，
合計 1,971,835 筆、61,508 種宣告 SHA，重複宣告 1,910,327 筆；64 筆
實際原檔抽樣 SHA 均與宣告吻合。這些不是已驗證的全量內容或實際可節省
bytes，也不是檔案去重或刪除許可。它指出後續適合以完整 SHA／長度、
原有 owner 與固定清冊建立共用 immutable 物件與索引，供各版本重用，
按需組裝；執行前仍須完整讀取、檢查引用與 restore。私有結果為 E
`verify/futures-prepared-manifest-reuse-20261104-v1.json`。

19:41 的 backup-stream cycle 於 20:07 結束，狀態為已發布、等待 NAS
收據，wall 約 25 分 58 秒；前一輪約 33 分 19 秒，但兩輪工作集合未固定，
不能將差異全部歸因於 head 讀取最佳化。下一輪曾因原 source owner 忙碌
退出 75，保留 timer 重試；沒有中止 owner，也未將發布當作 NAS 讀回。

## 實測容量與邊界

01:xx 初始原生 Windows 量測：Ubuntu-26.04 的 C VHDX 實體配置為
3,103,917,080,576 bytes；另有 Wazuh-Security VHDX 約 39.00 GB。
C 服務檔案系統已用約 2.71 TB。當時 E 的 VHDX 約 606.41 GB、虛擬資料卷
約 1 TiB；E 實體 free 約 3.49 TB。此數字不代表 18:00 的 E 容量；有包含關係，不能相加當可用量。

**現在即可準確量測 C 實體 free 與 VHDX allocated bytes**；離線 compaction
是為了把可回收區塊歸還 Windows，不是量測準確的前提。VHDX 配置與 ext4
已用量之差包含未歸還區塊及格式／檔案系統開銷，不是保證可回收的數字。

服務持續運作時的有效准入仍取新鮮 guest／實體 backing 的較小值並保留原
應用程式 reserve。這是已登錄 StockAgent 寫入的准入，不能保證其他 Windows
程式不會消耗空間。虛擬容量不是預先保留的實體容量。

### 本輪已量測到的 C 大宗

以下是本輪 live `du` 已輸出的子樹，非原子快照，也不是可刪除量。沒有輸出的
FinMind 原 C 回復副本等仍在清點，不能當零；父子或相容別名不能相加。

| 路徑 | 已觀察 GiB | 用途及下一步 |
| --- | ---: | --- |
| `service_data/` | 691.66 | 混合服務與大量歷史／還原副本，依內容拆分 |
| `service_data/cache`（上列內含） | 209.68 | 現行服務 cache 與研究／歷史 cache 分開，後者移 E |
| `service_data/live`（上列內含） | 199.10 | 逐項分開現行帳本和封閉回放／產物，不整根刪除 |
| `service_data/stockagent-live`（上列內含） | 156.03 | 服務投影／模型／行情混合，服務必要部分留 C |
| `service_data/operations`（上列內含） | 106.31 | 歷史工作、恢復及證據；payload 移 E，小型控制留 C |
| `data_tw_index_futures/` | 369.14 | 期貨來源與研究資料，原研究 owner 交接至 E |
| `data_tw_index_futures/training_prepared`（上列內含） | 339.32 | 訓練準備；不是單憑名稱便可刪除的快取 |
| `artifacts/` | 319.43 | 部分相容入口與 service_data 重疊，不能相加；獨立產物移 E |
| `data_binance_archive/` | 92.25 | 加密貨幣歷史來源，移 E |
| `data_tw_index_derivatives_ticks/` | 87.80 | 期貨／選擇權歷史及仍在寫入的 capture，封閉分區先移 E |
| `/root/miniforge3/` | 40.80 | Python／編譯環境，保留 C |
| `/root/.shioaji/data/` | 38.87 | SDK 歷史行情 cache，原讀寫者交接後移 E |
| `data_okx/` | 35.86 | 加密貨幣行情，移 E |
| `data_tw_minute/` | 20.67 | 台股分鐘歷史，移 E |
| `/root/.codex/` | 15.78 | 開發工具與工作紀錄，不是金融 raw data |
| `/root/finlab_db/` | 10.10 | FinLab SDK 本機資料，須沿原讀寫者遷移 |
| `/tmp` | 0.45 | 不是本輪 TB 級占用的主因 |

原 C provider 回復副本仍有三個入口；其中 FinMind 包含明確排除於前一輪 E
複製的美股分鐘子樹，**不能將整根當成已完整搬移的重複檔案刪除**。
本輪讀取到的凍結來源狀態記錄 US 分鐘 complete 2,948,464、observed_empty
3,289,164，Parquet bytes 12,939,968,597；這是舊來源任務／logical bytes，
不是目前 allocated 容量或檔數驗收。其大量任務碎片的 metadata 清點仍在執行。

私有完整逐路徑報告在 E：
`verify/wsl-layout-inventory-20261104/{inventory_summary.md,path_usage.csv,status.json}`。
獨立 agent-workflow report sidecar 每分鐘只讀三份既有掃描輸出與 run 收據，
不再遍歷來源；所有掃描結束後退出，六小時有界。部分清單、失敗、原觀測時鐘、
跨掃描／父子包含關係和不授權退休均明列。清點及所屬原始錯誤檔保留，不以
一次 `du` 或既有 E copy 的 logical bytes 當作 source retirement。

## 放置、封裝與讀取方式

| 類型 | 位置／做法 |
| --- | --- |
| 程式、Python、PG／Temporal、當沖／隔夜帳本、鎖與小型控制索引 | C 原生 ext4；服務與交易 owner 保留 |
| 下載原值、行情歷史、來源收據與待整理增量 | E 原生 ext4；已部署 provider alias 缺 E 時拒絕回退 C |
| 訓練準備、封裝、還原、獨立驗證、傳輸 payload | E；按工作使用，完成驗收後由原 owner 回收 |
| 查詢資料 | 沿既有 compactor 產生依市場／資料集／日期或 symbol 分區的 Parquet；保留原值、單位、發布與來源版本 |
| 封閉來源及產物備份 | 既有 packed-v2／CAS 保存於 D；未變物件重用，增量只寫改變的 bytes |
| 離機備份與計算 | lab203 → NAS；Vast 只保存當期 exact source／工作集及待回傳產物 |

`service_data/` 目前混有大批歷史原值、研究快取和封閉還原副本，不能因目錄
名稱便全部當作服務必要資料。遷移按內容與現行引用分割；保留必要 live projection，
其餘移 E。已驗證的 E provider 副本直接重用，原 C 回復副本不再次複製。
另一視窗仍負責 FinLab／FinMind／TEJ 搬移及下載器，這裡保留其程式與 owner。

封裝沿既有 `packed_snapshots`：小檔進有索引的封閉 ZIP packs；大檔使用 blob
或既有 8 MiB chunk。Parquet／其他已壓縮來源使用 Stored，不為追求壓縮率
反覆解壓重壓。manifest 指向完整可重建集合，不需依序重播整條增量鏈。
讀取先查 SQL／manifest，按 pack 聚合同一批要求，以一次順序讀取供多個成員
驗證；保留原檔 SHA、ZIP CRC、原始格式及獨立還原驗收。單一巨型 tar 會使
少量查詢與增量更新放大，不能當作所有資料的查詢正本。

每天整理的對象應是已封閉增量，不能每日全資料解壓、全目錄重掃和重新封裝。
既有封閉版本、下載器與資料模型的實際改動仍須各自原 owner 驗收；本輪的
有限 IO 測試不代表已把所有資料改成同一種新格式或已達到硬體極限。

## E 擴至 3 TiB 與 C 離線回收

已擴充原 `scripts/maintain_windows_wsl_storage.ps1`，沒有另建磁碟管理 owner：

1. 核對 C／E 的固定 WSL 登錄、VHDX 路徑、E NTFS volume GUID、原 ext4 UUID。
2. E 目標固定為 **3,298,534,883,328 bytes＝3 TiB，約 3.30 TB**；只擴不縮。
3. 容量准入使用 `目標 virtual bytes − 目前 NTFS allocated bytes`，不是只算
   兩個 virtual ceilings 的差。E 完整未來成長另保留 64 GiB，之後再讀回。
4. 沿原單一 Windows maintenance mutex，保存並暫停原三個開機任務；正常
   關閉 Ubuntu 與明列的 Wazuh-Security，再關閉只有資料用途的 StockAgentData。
   未知執行中的 distro 仍阻擋，不強殺其他程序。
5. 離線擴 E，C read-only attach／Quick compaction；無 zero-fill、unsafe sparse、
   格式化、source 刪除或 D／NAS 改動。
6. 恢復原任務、C 與明列 companion，核對 C UUID／容量、原 boot audit、E
   block device／ext4 精確容量及 UUID。實際維護失敗保留 needs-attention。

準備階段 **沒有 Windows administrator token**，先在原生 Windows 目錄準備
SHA 固定 worker、verifier 與入口。使用者後續執行的實際結果及服務恢復見下節。
在 Windows **管理員 PowerShell** 執行：

```powershell
# 不帶 -Apply 可先讀取計畫；兩種模式都會先核對固定程式 SHA。
& "$env:LOCALAPPDATA\StockAgent\WslStorage\rebalance-c-e-3tib.ps1" -Apply
```

這次維護會暫停 Ubuntu 的網站／交易等服務、StockAgentData 及 Wazuh-Security；
結束後依原服務配置恢復。入口及所有工作檔在 Windows 原生磁碟，不依賴停止後
消失的 UNC 或 WSL 程序。原生權限檢查拒絕未提升的 Apply，不會假裝成功。

來源準備證據在 E `verify/windows-c-e-3tib-preparation-20261104-v4.json`。
每分鐘觀測只讀這份固定準備與原 owner 的單一
`receipts/latest-c-e-maintenance.json`，不列舉數百個歷史回執、不跑 PowerShell、
不觸發停機。兩張面板分別顯示 E 擴容與 C compaction；外部管理員前置條件
不因計畫超過五分鐘而變成假進度或新的維護成功。
若原 boot audit 因金融來源或其他服務尚未就緒而失敗，仍保留整體
needs-attention，並繼續獨立核對 E 的 UUID 與精確容量；E 通過不代替服務
就緒，也不會被不相關的 readiness 等待藏掉。

### 再次執行前的關機與續跑修正

上次 Ubuntu 的 journal 顯示 systemd 已抵達 `poweroff.target`，但 Windows
仍回報執行中，因此原流程等到逾時，實際沒有開始擴容或壓縮。新版在停機前
為 Ubuntu 和當時正在執行的 Wazuh 建立各自的唯讀 journal observer，核對
本次隨機標記之後、PID 1 送出的關機終點。只有已停止或確認抵達終點的已知
systemd 執行個體，才允許執行原生 `wsl --shutdown`；未知執行個體、缺少
證據或逾時仍拒絕往下。`StockAgentData` 若原先未執行，維護後仍維持該狀態；
若它原先執行，缺乏明確程序續跑證據時先停止維護。

關閉 Ubuntu 前記錄原 transient FinLab 工作的受限請求清單與程序身分；開機後
核對 E 工作區與既有 owner，再交給原帳號鎖及下載器續跑。未知的 transient
工作會阻止關機，不會假裝已恢復。C 的離線 compaction 實際讀回獨立記錄；
即使後續 boot audit 失敗，也能區分磁碟作業與服務恢復結果。

Windows 原生 verifier 的停機觀測順序與 PID 檢查通過，Ubuntu 和 Wazuh 的
唯讀 observer 已實測可取得各自標記；242 項相關測試通過。這些驗證沒有
停系統或改磁碟，也不代替此次實際擴容、compaction 與開機後的驗收。

## C 的 virtual ceiling

本入口**回收 C 實體配置，不縮 C 的 ext4 或 virtual ceiling**。C 現仍有約
2.71 TB 資料，不能直接把磁碟截小成只夠服務的容量。先完成原值搬 E、還原與
C 退休，再依服務實測峰值和 backing 預留量規劃小型 C 檔案系統。

官方 WSL 文件要求離線後擴容，`Resize-VHD` 也不等於空白區塊回收；compaction
與縮 virtual ceiling 是不同操作。[WSL 磁碟空間](https://learn.microsoft.com/en-us/windows/wsl/disk-space)、
[Resize-VHD](https://learn.microsoft.com/en-us/powershell/module/hyper-v/resize-vhd?view=windowsserver2025-ps)。
目前不以未先縮小的 ext4 配合直接截斷 VHDX，也不保證任意 Windows 寫入後仍
存在固定 free。維護收據會回報 allocated＋host free−32 GiB 的當時所有權
容量預算；它包含已有配置，不能當成額外可用量。

## 本輪驗證

339 個 focused tests 通過，涵蓋 Windows 磁碟 scope、E 掛載身分、清冊、監控
及兩個新維護階段。另在實際 Windows PowerShell 執行 AST／native verifier：
stderr／exit code、四個完整成長算例、負值／非法大小、未配對 E／companion
均通過拒絕或等價檢查；測試未改磁碟。這些不代替管理員離線執行與真正開機驗收。

## 等量小檔與大檔的實際 IO 比較

在原服務的 PID 1 mount namespace 重新核對 E 身分與 D mount 後，用既有
`benchmark_storage_filesystems.py` 跑三輪，每組同為 8 MiB。比較 128 個
64 KiB 小檔與 2 個 4 MiB 大檔，包含 write／flush／fsync／rename、列舉、
完整 SHA 讀回及己方 scratch 回收；沒有變更來源、清 OS cache 或修改 mount。

| 已量測路徑 | 小檔中位秒 | 大檔中位秒 |
| --- | ---: | ---: |
| E 原生 ext4 工作卷 | 0.931 | 0.034 |
| D HDD 原 DrvFS 備份磁碟 | 18.618 | 1.741 |

12 次有限工作流程全檔 SHA 通過，scratch 全部刪除，總測量 72.04 秒。
證據為 E `verify/e-d-packed-io-20261104-v2.json`。這是目前路徑、硬體與並行
負載下的等量樣本，不是相同來源的一整條下載／壓縮／NAS 倍率，也不是 raw
device 的理論上限；它支持將封閉小檔聚合、批次持久化、順序傳輸的方向。
最初互動 namespace 的 D request-size guard 失敗與未完成 run 原樣保留；
沒有修改 guard 或 mount option，正式重測在原 service namespace 通過。

## 01:15 維護結果與開機恢復

使用者已執行 Windows `rebalance-c-e-3tib.ps1 -Apply`。實際固定收據為
`WslStorage/receipts/20261104T011552797-Compact/status.json`，狀態為
`needs-attention`：正常關機等待逾時，boot audit／readback 及 Wazuh companion
恢復亦記錄失敗。前面的 native verifier 通過只證明測試程式通過。
本次 E 的 expansion 停在 planned，C 的 before／after allocated bytes
完全相同；不能宣稱已擴至 3 TiB、已 compact 或已縮小 C filesystem。

恢復時發現原 `operations` 與 `/var/lib/stockagent` 的 native compatibility
bind 未掛回，服務因而看不到原 frozen release、workspace enrollment 和
部署來源。原資料仍在 `service_data`。先停原排程、核對 original inode／slot
及程序引用，再把未掛載期間新增的少量控制檔完整保留到
`service_data/.interrupted-boot-20261104-*`，沿原 `restore_bind` 在 PID 1
mount namespace 恢復；無 source／D／NAS payload 刪除。

已修復原 organizer：已掛妥的 exact inode 不再受無關 active service 阻擋；
既有 bind 的 boot replay 只阻擋實際引用／寫入 mountpoint 的程序。
非空 placeholder 不會被覆蓋；必要 bind 未掛回時 exit 75，相依服務等待。
原 unit 加上有界間隔重試，安裝器只允許精確舊 managed unit 升版。
現場六組 service aliases 的讀回通過，103 個既有 unit 的啟動排序已更新，
安裝過程未重啟健康交易服務。恢復原 58 個 timer，E 與三 provider mount、
當沖／Discord、PostgreSQL、Temporal、Syncthing、監控及備份 owner 已重啟或保留。

160 個 focused semantic tests 通過，涵蓋 bind 重播、影子控制檔保留、owner
引用、開機失敗回報、精確舊 unit 升版及容量保留。現場資料 readiness
仍以原 preopen／completed-session 收據為準；服務 active 不代表模型、全歷史
或 NAS 還原已驗收。完整證據位於 E
`verify/service-recovery-20261104-v1/`，包括原 namespace 掛載恢復、公開
監控快照與 `final-priority-acceptance-tests.log`。本次未再次執行離線 Windows 維護。

最後又核對實際服務的 private mount namespace，而非只看 PID 1：8 個核心
服務的控制目錄、E workspace 與三個 provider aliases 全部對到 authority 的
相同 inode，證據為 `service-namespace-final.json`。四個 reader 的開機順序
已改為等待原 E／provider mount 嘗試完成後才建立 private namespace；使用
Wants／After，E 故障仍保留 C 已驗收 cache 與 broker quotes 的服務能力。
原 provider mount bridge 恢復資料視圖，原本唯讀的 FinMind 子目錄另核對
並恢復唯讀；udevd 的非資料消費 namespace 原樣保留，不宣稱全系統 namespace
均已改動。原 code release 的唯讀綁定未修改。

當沖與隔夜正常重啟後，8 份已保存交易紀錄前綴的全 SHA 讀回一致，見
`journal-readback.json`。53 GiB 的 `signals.jsonl` 保留原 inode 與長度；本次
沒有重新驗證其全部內容。第一次誤用整檔讀取的己方診斷已停止，後續交易
前綴改用 4 MiB 串流，避免再把整份大歷史放入 RAM。
SQLite 影子佇列在 pinned 的原服務 namespace 內獨立備份並確認沒有 task；
只把 `/proc/PID/root/...` 路徑交給 SQLite VFS 不足以建立該 namespace 的
來源證據。誤讀 authority 的檢查副本已重新標記為 not-shadow，原檔均保留。

兩次原 storage worker code-only handoff 均保留原三條 workflow 的 run 鏈、
中斷 journal 與 PG／Temporal server；最新結果為
`final-priority-worker-reload.json`。FinLab 原 4 組有限請求與已登錄 missing56
請求由一個新 pin 的原 handoff 接續，仍使用原帳號鎖、配額與 E futures
workspace。FinMind／TEJ 保留其原有限執行者；不為了讓 boot audit 變綠而
另啟動已明確 disabled 的三條舊連續 FinMind lane。

09:27 的原 source receipt 已確認官方收盤及全部衍生層到 2026-11-03，
Factorized／UnitMGDA 的新投影仍在原 producer 建立中，不能宣稱六個模型
全已就緒。完整 boot audit 的 critical owners 與 enabled timers 均無缺失，
worker code／三條 workflow 亦吻合；冷庫 catalog 收據的 freshness 仍未通過，
因此保留整體未驗收狀態。網站實測 60 秒更新設定與三節點近期觀測，仍不代表
所有歷史、NAS 還原、E 擴容或 C compaction 已完成。
