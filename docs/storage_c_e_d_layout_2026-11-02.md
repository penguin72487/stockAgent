# Penguin 的 C／E／D 分工

依 2026-11-02 使用者最新指示：**D 仍是主冷庫，所有資料進冷庫前使用 E**。
本文件取代先前將下載／封裝暫存留在 C 的位置建議，不更改歷史版本、NAS
驗收、來源可用性與退休規則。

| 位置 | 用途 | 生命週期 |
| --- | --- | --- |
| C 的既有 WSL | 程式、Python 環境、網站／交易服務、帳本、SQL、Temporal、控制索引與遷移收據 | 正式服務持續使用；不當大量來源或封裝暫存 |
| E NVMe：`/srv/stockagent-workspace` | downloads、increments、build、verify、restore、transport、migration | 下載與更新、封裝、逐檔還原驗證、傳輸暫存；完成相應驗收後回收 |
| D HDD：`/srv/stockagent-packed` | 不可變 CAS、封閉 packs／manifests／heads、來源冷歷史 | 唯一本機主冷庫；本次不搬走或改寫其身分 |
| lab203／NAS | 原配對 ingress、獨立還原、離機備份及 ACK | 沿原 owner，不將 E 暫存當作 NAS 驗收 |
| Vast | 壓縮工作集、獨立驗證、訓練、回傳 | 沿原工作契約；不是 Penguin 冷庫 |

E 是 Samsung PM9E1 4096GB NVMe；D 是 ST4000NE001 SATA HDD。E 的 NTFS
volume UUID 固定於 `configs/data_sync/workspace_volume.json`。E 不格式化；在
`E:\StockAgent\workspace\ext4.vhdx` 建立純資料 WSL instance `StockAgentData`，
不啟用 systemd、Windows interop 或業務服務。它只經 WSL 的共享 mount 提供
原生 ext4；全部應用程式仍在原 C WSL 執行。原生 Volume GUID、登錄 VHD 位置、
ext4 UUID、私有 enrollment、marker 與 mount alias 必須一致，缺 E 不回退寫 C。

目前資料卷約 1 TiB。保持 C 服務運行時，WSL 的初始 3500GB resize 回報
`WSL_E_DISTRO_NOT_STOPPED`；收據保留在
`/var/lib/stockagent/workspace-volume/initial-resize.json`。沒有執行整機
`wsl --shutdown`。任何大型搬移均取 E 實體 free 與資料卷 available 的較小值，
再保留驗證空間；不能宣稱 E 的 4 TB 都已可被此 guest 使用。

## I/O 與封裝

既有 packed 格式、原值 SHA 與獨立還原不變。builder 在 E 關閉／驗證物件，
D 只接收完整物件的順序寫入。跨 filesystem 不使用 hardlink 假裝原子搬移：
先複製並核對完整 SHA 到 D incoming、flush，再安裝不可變名稱；成功前保留 E
來源。不可覆蓋別人的 CAS winner。C 保留可查詢的 SQLite／manifest 索引，
不靠每分鐘遞迴掃描大量來源小檔；公開監控容量每 60 秒觀察，分類獨立慢速盤點。

同一來源透過原 writer／reader 和固定 manifest 重用。Parquet／Arrow 原本已壓縮的
資料不為追求表面壓縮率重複解壓重壓；小 JSON／控制檔由原 pack 合併。沒有改成
全部塞入單一大壓縮檔，避免每次查詢與增量都必須重寫整個歷史。

當前負載下兩輪、每個工作負載同為 8 MiB 的完整 write/fsync/rename/read/SHA 實測：

| 工作區 | 128 × 64 KiB 小檔：中位數秒 | 2 × 4 MiB 封閉檔：中位數秒 |
| --- | ---: | ---: |
| E 原生 ext4 | 1.841 | 0.041 |
| E 的 `/mnt/e` Windows 掛載 | 18.148 | 0.709 |
| C 現行 WSL 控制工作區 | 29.986 | 0.292 |

證據：`/var/lib/stockagent/workspace-volume/filesystem-benchmark-v1.json`。
所有測試檔 SHA 核對且自行移除；未清 OS cache，沒有將磁碟型號、服務競爭和
filesystem 演算法隔離，不能由這個小測試宣稱整機已達硬體上限。

## 遷移與恢復

`scripts/manage_workspace_volume.py install-services` 安裝 E mount 與可續作 timer；
只在已完成 enrollment 後使用。掛載在 PID 1 namespace，避免 CLI 看得到但服務
看不到。原 `scripts/install_backup_stream.py --relocate-to-workspace` 取得既有
backup cycle／backup owner／source replication locks，僅暫停原配對 Syncthing
folder；建立 C-side SQLite 精確清冊，在 E 複製、逐檔 SHA 獨立讀回、核對來源
世代與完整名稱集合後，才將原 sender alias 切到 E。保留全部 envelope、派工、
ACK 與配對身分。D 舊 transport 副本經程序引用檢查、quarantine 與再次 E 核對
才回收；不刪 D CAS、來源觀測或 NAS 原件。程序／原 owner 忙碌則 timer 續等，
不強制解鎖。這是 transport 位置證明，不能冒充來源歷史、NAS 或 runtime 驗收。

E 掛載的 boot ordering 在修改 sender 配置之前安裝。原 timer 在搬移完成後另呼叫
既有 `rollout_lakehouse_source_policy.py --reload-code-only`，每次有界等待 45 秒，
只在原 publisher 釋放鎖後交接；不使用強制重啟。保留 PG／Temporal 與持久
workflow 鏈的正式驗收。owner 忙碌可續等；其他錯誤每一固定程式版本最多三次，
保留失敗記錄。這個 code reload 尚未執行，須待本次搬移驗收完成。

其餘來源按目前 writer／reader 與活動中的 publication 分組遷移；不能將整個
`service_data` 搬到 E，也不能在 460 萬檔 publication 中途改來源 inode，使已做
完的查核全部失效。尚未切換者仍列為待遷移，不宣稱已完成。

## 2026-11-02 現場證據

- 23:48 台北：原傳輸清冊為 44,257 檔、138,422,642,761 bytes；已複製及獨立
  SHA 讀回 10,896 檔、31,522,377,604 bytes（22.77%）。本輪沒有刪除來源。
  剩餘約 84 分鐘只是當時複製／讀回階段的估計；後續完整名稱集合、來源穩定、
  alias、引用與舊副本回收另計，不當作全部資料或 NAS 的完成時間。
- 公開 API 與實際 Chromium 頁面均確認 8 個磁碟位置。展開「儲存與證據」可看到
  E physical／ext4 分列與不可加總關係，頁面無 JS error。容量與原程序進度每分鐘
  由背景 observer 更新；讀取網頁不觸發檔案遞迴或資料 API。
- 已安裝 E mount／搬移 timer；原 D 主冷庫身分、Syncthing folder／peer 配對、
  原有來源與 NAS 收據不變。正式交易與網站服務仍在原 C WSL。
- 儲存回歸在既有 FinTech、lakehouse 角色及正式 PID 1 mount namespace 分開
  執行；共覆蓋 912 個通過、7 個跳過的測試項目。先前環境缺少套件或 namespace
  掛載參數不符的失敗 log 同樣保留，未靠改正式 guard 讓測試通過。
  主要記錄為 `/var/lib/stockagent/workspace-volume/regression-storage-v5.log`、
  `regression-lifecycle-role-v1.log`、`regression-native-pid1-v7.log`。
- 未完成：既有 C 原始資料逐組位置切換、E 資料卷擴到 3500GB、真實冷開機驗收，
  以及原有各批來源／NAS gate。這次沒有把 E 工作卷宣告為第二主冷庫。

官方原理參考：[WSL 檔案系統效能](https://learn.microsoft.com/en-us/windows/wsl/filesystems)、
[Microsoft 共享 mount 說明](https://github.com/microsoft/WSL/blob/master/doc/docs/technical-documentation/init.md)、
[WSL 磁碟容量管理](https://learn.microsoft.com/en-us/windows/wsl/disk-space)。

## 2026-11-03 自動接續與觀測修正

- D → E 原傳輸暫存以原有 owner 複製並獨立讀回，00:12 台北約完成
  50,546,514,065／138,422,642,761 bytes、17,128／44,257 檔。每檔及整體
  實際耗時不同，當時複製階段估計約 84 分鐘；後續來源世代、名稱集合、切換與
  回收另計。這一階段尚未刪 D 舊副本，**不會直接釋放 C／WSL 空間**。
- D 當時實體可用為零，是正式冷庫發布與剩餘一個登錄資料集驗證的容量阻礙。
  E ext4 可用約 909 GiB，E 實體約 3.7 TiB；C 實體約 38 GiB、WSL guest
  內約 337 GiB，後兩者不能相加。以面板最新實測為準。
- 回補移轉期間的原 owner 已持鎖且暫停成對 folder；同步通知 timer 不再把這個
  狀態誤判成配對損壞，也不嘗試搶鎖或宣稱 NAS 驗收。面板分別呈現每分鐘
  重試觀測及上次實際掃描時間，避免新重試偽裝成新掃描。
- 搬移清冊中同時有少於 1 MiB 的大量檔案與最高約 7.93 GiB 的檔案；舊固定
  八檔批次會等待批內最慢項目。新程式以既有四個 worker、最多八個在途工作
  逐件接續，完成量定期提交；逐檔完整 SHA、來源世代、目的端讀回、精確
  namespace 與後續回收條件不變。合成 32 檔、1 個 0.4 秒及 31 個 0.02 秒
  作業的 ABBA 排程試驗，舊版 0.575／0.534 秒、新版 0.401／0.401 秒；
  這不是實際 D／E I/O 吞吐量。實際正在執行的搬移仍使用啟動時舊程式，
  不為載入新排程而中斷及重讀已驗證資料；新程式用於後續啟動或自然續跑。
  故障測試也發現 SQLite 未關閉 cursor 時，同一程序續跑可能被舊 read snapshot
  鎖住；新版先等待在途工作、關閉 cursor 與連線，再允許續跑。恢復已驗證檔案
  同時核對目的端 mode、擁有者、mtime、大小及內容，不因 SHA 相同忽略 metadata。
  排程、volume 與原 placement 共 19 項測試通過；合成試驗資料在
  `/var/lib/stockagent/workspace-volume/scheduler-abba-20261103.json`。
- 修復開機後 `/run/stockagent-agent-ssh` 遺失造成的 Vast SSH socket 失敗；
  以固定 tmpfiles 與 worker 起始前檢查重建，既有已封閉物件識別不變。實測
  兩個既有範圍合計 624／624 個已發現封包完成遠端內容預驗證，新的封包由
  原 timer 接續；這不是完整來源、D 重建或 NAS 驗收。
- 依使用者對舊效能驗證工作副本的授權，新一輪精確清冊 4／4 完成，實際釋放
  1,260,695,552 配置 bytes；保留小型稽核證據。此前獨立一輪是 70／70、
  168,772,026,368 bytes，不能把本輪說成又額外回收那個數字。兩個範圍與
  時間在網頁分開顯示，持續由原 retirement owner 執行。來源、帳本及冷庫未刪。
- 公開 HTTPS 的 Chromium 桌面／手機實測 8 個容量卡與蜂巢導覽均通過；
  逐分鐘刷新、等待／阻擋、時間範圍及危險字串的模擬案例另行標記。瀏覽器
  檢查改按 API 清冊推導卡片數，避免新增磁碟時的六張卡固定常數造成誤報。
- lab203 已有成對容量回執，但尚無主機 CPU／RAM 的實際回報；本機程式已支援
  舊回執相容的逐分鐘主機取樣契約，接收端須按
  [交接](lab203_runtime_observer_handoff_2026-11-03.md)安裝並回傳證據。
  目前不把回執在線聲稱為 CPU、服務健康或 NAS 驗收。
- 此輪對 observer、排程、掃描狀態、面板進度、接收器及搬移執行 318 項相關測試；
  已部署 Penguin observer。當沖服務原 PID 保持 430。C 既有 FinLab／FinMind
  原始資料的 writer／reader 仍未逐組切至 E；不能將 D → E 傳輸搬移說成
  原始資料全部遷移完畢。E 3500 GB guest 擴容及冷開機驗收仍未完成。

## 2026-11-03 E 工作區與受保護來源

- 與 D → E 傳輸並行，對舊 `local-openbb` 作業的一個 19,862,511,616 bytes
  SQLite **準備副本**執行 C → E、目的端完整 SHA 讀回、原作業鎖與唯一引用／
  程序／inode 檢查後，原子切換準備引用並刪除舊準備副本；回執實測 WSL
  釋放 19,862,634,496 allocated bytes，沒有刪資料來源、D 冷庫或 NAS。
  定時器可在未完成時續跑；完成回執存在後不再重複讀取大檔。這只是暫存
  位置與內容證據，並非整個資料集冷庫還原或 NAS 驗收。
- 新的 migration SQLite、凍結控制檔與 public／private 封包建立作業，按 E
  的登錄身分與實體容量檢查進入 E 暫存；正式 CAS／head 仍在 D。修正跨卷大檔
  在 E 暫存時誤用只支援 D 的 Windows writer。E 已登錄但不可用時保留失敗，
  不回退到 C。既有 storage worker 在 D → E 搬移與原 owner 安全交接後才載入
  程式；正在運行的 worker 尚不是本版，不能視為所有新來源均已切換。
- 00:47 台北，D → E 已獨立讀回 71,036,268,349／138,422,642,761 bytes
  及 25,968／44,257 檔；複製階段當次均速約 14.1 MB/s，約 80 分鐘的剩餘
  估算只適用於此階段，實際檔案大小與 I/O 波動很大。之後仍需固定世代、
  引用、切換及舊傳輸副本回收；當時 D 實體可用為零，Vast 歷史回傳及一個
  D 還原作業因而等待。原 timer 保留並在空間恢復後自動續試；不以持續重解碼
  與失敗寫入消耗資源。此 D → E 工作本身不直接釋放 WSL。
- 舊 TW 部分還原約 5.25 GiB 缺對應本機 D CAS 封包，先保留而非把「無程序
  參照」誤當可還原證明。已在 [lab203 交接](lab203_runtime_observer_handoff_2026-11-03.md)
  加入精確 SHA 的唯讀查找要求。
- 00:47 WSL guest 約有 355 GiB 可用，Windows C 實體約有 38 GiB；前者可重用
  已有虛擬磁碟空間，但不能代表後者也釋出同額。E ext4 約有 871 GiB，E
  物理卷約 3.7 TiB；其容量也不能相加。當沖優先資料來源原本因整代至多
  16 GiB 加 C 實體 25 GiB 保留而受阻。其專用 systemd owner 可使用其中
  5 GiB 物理保留，其他來源仍為 25 GiB；當沖 guest 檢查、全部 generation
  預算、其他活躍 reservation 及冷庫保護不變。第一次重試通過準備檔的准入，
  但在巢狀的同一當沖 BASE panel 寫入遭原物理門檻擋住；已將同一例外限縮
  套用到該精確 panel 目錄，其他 panel 不變。兩個原 owner（當沖優先來源與
  FinLab 當沖投影）都可觸發同一生產者，因此兩者的 systemd 環境均已啟用；
  準備鎖仍由原生產者取得，另一個 owner 只等待。首次重試也遇到執行期間
  程式身份變更而拒絕，失敗證據保留，沒有強制解鎖或重啟交易服務。實際資料
  完整度與開盤時效須等該 owner 的發布收據驗證，不能只憑服務啟動下結論。
  00:58 的新一輪已通過準備與巢狀 panel 的空間檢查，進入增量特徵處理，
  仍未完成發布。面板將這兩個原程序的退出碼 75 顯示為暫緩，其他錯誤
  仍顯示失敗；兩者的實際狀態不代表交易或資料已驗收。
- 原 60 秒面板增加上述準備副本、D 空間等待及階段進度；實際桌面與手機
  Chromium 測試通過。`cache_capacity`／Windows 實體容量相關測試 141 項、
  最新 observer／排程／cache 組合 291 項通過（測試集有重疊）；packed
  publication／migration／monitor 回歸的原掛載 namespace
  不符失敗保留記錄，受影響 10 項在正式 PID 1 mount namespace 重跑通過。
  Vast 已預驗證的 624 個封包不是全歷史或 NAS 驗收；該時點 lab203 CPU／RAM
  尚待接收端沿既有簽章回執補上，後續狀態見下節。

## 2026-11-03 固定控制快照、磁碟瓶頸與接收端阻礙

- 01:07–01:08 將三組固定歷史 migration 準備副本搬至 E：TW public 47 檔、
  1,992,596,286 bytes；TW index futures 862 檔、1,687,007,536 bytes；
  OpenBB 36 檔、865,661,000 bytes。搬移前核對固定清冊與來源世代，目的端
  全檔 SHA 加獨立讀回，再在原作業鎖下核對引用、程序 FD／inode、硬連結與
  目的端容量，原子改指向 E 後只回收舊準備副本。三份完成收據的 WSL 配置
  空間分別釋放 1,992,921,088、1,688,784,896、865,767,424 bytes，共
  **4,547,473,408 bytes（4.235 GiB）**。這與前節 18.50 GiB SQLite 副本是
  不同範圍；都不是原始資料、D 冷庫、NAS 或整個資料集的刪除／驗收。
  固定任務由每分鐘 timer 依 journal 中斷續跑，完成後跳過無需再次讀大檔；
  面板獨立顯示三批的階段、資料量及完成證據。
- 01:19 台北，D → E 原傳輸副本已讀回 35,016／44,257 檔、
  97,987,198,108／138,422,642,761 bytes。當次按約 14.1 MB/s 推算剩餘
  約 48 分鐘，只是複製與目的端逐檔讀回階段，來源完整集合、引用、切換及 D
  舊傳輸副本回收還未完成。D 冷庫當時物理餘額為零；這項 D → E 工作本身不
  釋放 WSL。當時 WSL guest 可用 384,022,982,656 bytes、E 工作 ext4
  903,509,299,200 bytes；各自的物理後端仍需個別看最新觀測，不可相加。
- 已核對 C／E 為 NVMe SSD、D 為 SATA HDD。Windows D 取樣曾見每秒約 47 次
  磁碟讀取、總計不到 1 MB/s，顯示當次小檔／隨機讀成本明顯，不構成磁碟
  最大吞吐測試。既有 packed-v2 對新增小檔使用封閉物件桶、大檔使用定址
  chunk，E 建包後以順序物件進 D；Vast 只接收已封閉物件做獨立預驗證。
  當前 Syncthing 44,257 個固定名稱仍由接收與驗收協定引用，因此不在傳輸
  中途更換為另一種封包或重寫現有不可變冷庫。
- `packed_snapshots` 的 seekable 匿名 E 暫存檔取消中途 `fsync`，仍 flush
  後逐 member SHA／CRC 核對，並保留來源世代、完整物件 SHA 及正式 CAS 的
  持久化要求；暫存檔不作崩潰後持久性證據。E ext4 有快取合成 ABBA（256
  檔、67,135,510 bytes）端到端包含複製與全部 SHA／CRC：中位 0.4755 秒
  降至 0.3416 秒，約 1.39 倍。這不是 D HDD 或正式批次的效能驗收；明細為
  `/var/lib/stockagent/workspace-volume/pack-scratch-io-20261103.json`。
- 修正 D 實體可用空間為零時，DuckLake 登錄不應反覆掃來源再寫入失敗：候選
  程式先保留已提交版本並回傳容量等待，原 workflow 60 秒後重試。storage
  專用 runtime 相關 167 項測試通過，三條現有 Temporal 歷史（315、40、572
  個事件）已唯讀重播通過。E 準備／封包及面板測試也已通過；目前原 storage
  worker 還在處理舊搬移程式，候選程式待該批完成後由原有安全交接載入，
  不能把候選測試或歷史重播描述成正式活動已採用。
- lab203 01:16 的配對 `storage-status.json` 已含自身 CPU／RAM 量測；每分鐘
  主機觀測不代表 NAS 驗收。01:14:43 固定診斷回報 Restic 排他鎖及 guard
  返回碼 11，01:16 的 owner 觀測仍等待重查舊 readiness。面板分開顯示最近
  診斷與新觀測時間，不把舊鎖記錄偽裝成持續持鎖。須由 lab203 原 owner
  判定鎖持有者並重做原就緒檢查；原 PID 消失不等於鎖可刪。精確缺件及
  交接內容見 [lab203 交接](lab203_runtime_observer_handoff_2026-11-03.md)。
- 當沖模擬服務持續運行；當沖資料投影於 01:14 產生 `verified` 的固定
  generation，後續來源頭檢查回報 350 筆 `source_heads_current`。另一來源
  的上次執行於 01:13 因同一投影當時仍在運行而暫緩；這些不構成實際開盤
  時效或券商交易驗收。其餘 C 上仍有未切換的 writer／reader 及未驗收來源，
  不能宣稱所有資料已搬至 E。

## 2026-11-03 02:40 台北：傳輸副本與排程對帳

- 原 D → E 固定清冊已對照 44,257 個檔案、138,422,642,761 logical bytes；
  E 現行檔案 44,095 個內容一致、3 個內容不同、159 個當時不存在。這是指定
  清冊的對照工作量，不等於實際磁碟讀取量，也不是 NAS 獨立驗收。
- 對 162 個不同或不存在的舊 D 檔，以固定來源世代與 SHA 順序封裝到 E；
  獨立讀回驗證完成，封包 SHA-256 為
  `0656ff4f74431b27d567c585548f1545593665b04471c783f2a01651cb051b7c`。
  隨後在 E 匿名暫存檔逐一重建並讀回 162／162 檔，共 1,356,456,159 bytes，
  核對內容及可攜式檔案中繼資料，實測 10.35 秒。這是個別 member 還原演練，
  並未宣稱整棵樹或 NAS 已驗收；原 D 名稱在這項演練期間沒有刪除。
- Syncthing 自身的 sender namespace 已核對指向 E；10 個其他存活命名空間
  仍保留舊 D 掛載。即使該次路徑查詢不可達，也不把它當成掛載交接完成。
  原 timer 繼續核對；沒有強制關閉服務或清鎖。舊 D 固定清冊的接續回收
  現使用中斷可續的逐檔 journal：必須先通過全部 namespace、原三把 writer
  lock、程序及所有清冊檔案與目錄的 inode 引用，逐檔重驗 E 同一路徑的內容
  與中繼資料，才回收原 D 副本；變動、無法確認及意外項目保留。差異封包
  的還原演練只證明內容可恢復，不能證明同步程式能在原路徑取得該版本；
  因此僅在封包中存在的差異檔會繼續保留於 D，直到 owner 完成路徑交接。
  空的舊根目錄保留，該任務完成不代表 D 主冷庫或 NAS 驗收。尚無實際回收
  數據或整體完成時間，亦不把 128.92 GiB logical 舊副本當成 WSL 已釋放量。
- 已完成的 WSL 回收批次每分鐘須重新核對固定原收據及來源缺席，過去即使
  只是只讀核對，也與分類掃描爭獨占 wave lock，使掃描反覆中斷。現在僅在
  所有已登錄批次原先均有接受證據時，於相同 scheduler 的共享讀鎖下重新做
  完整原收據核對；有真正執行中的獨占 wave 則仍等待，任何收據或來源變動
  仍回到原獨占前置檢查，絕不在讀鎖下派工刪除。現場持共享鎖啟動原 timer
  owner，重新核對後回報 `all_enrolled_accepted`；這是鎖協作測試，不是新的
  資料回收量或完整 I/O 吞吐驗收。
- 網頁新增差異檔還原演練與逐檔回收的獨立階段；10 個舊掛載、阻塞及每份
  收據原始時間分開呈現。等待中的工作不再沿用之前速度計算完成時間；恢復
  執行後重新取樣。觀測器及原排程仍為每分鐘節奏，不用新觀測時間替舊進度
  或 NAS ACK 續命。當時 D 實體可用量仍為零；C 實體餘額與 WSL guest、E
  guest 可用量各自量測，不能相加或把虛擬可用量當成保證可寫容量。

## 2026-11-03 02:51 台北：現場驗證與目前限制

- 原 162 筆差異在 E 的封包及逐檔匿名還原演練已驗證，但只在封包中存在
  的內容不能保證同步程式仍能以原名稱使用。逐檔回收程序現在要求 E 的
  **同一路徑**再次符合原 SHA 與中繼資料，才回收該 D 名稱；差異仍未對齊
  時只保全、不刪除。已退休的完整批次再次執行時，只檢查原名稱是否重現，
  不再為無待處理項目的批次重掃所有程序；有待處理的批次保留原檢查。
- 範圍測試 `383 passed`；真實 Chromium 以 CPU 2D 於 1440 與 390 像素驗證
  面板、三端狀態、阻塞、排程及展開收合。分鐘刷新、過期與等待場景另有
  fixture 測試；兩次公開 API 實際取樣時間約隔一分鐘，不能代表 NAS 每分鐘
  都完成一批。部署觀測器時分類掃描保留部分清點結果並重新啟動，完整掃描
  尚未完成。
- 02:49 的公開資料顯示已登錄 NAS 清冊 107,945／108,153 個檔、
  1,688,337,364,326／1,815,068,133,977 bytes；這是固定清冊而非全專案。
  私有清冊 48,202／48,276 的進度時間為 11 月 2 日 13:05 台北，應以舊值
  顯示，不能與新值合併。Vast 兩個固定來源分別完成 256 與 368 個封閉包的
  預驗證，等待來源封閉新包；預驗證不代表原件或 NAS 已驗收。
- 此時 D 實體可用量仍為零；WSL guest 約 358 GiB、Windows C 約 38 GiB、E
  guest 約 792 GiB 可用。E／WSL 的 guest 剩餘空間分別受其宿主空間限制。
  D 傳輸舊清冊仍有 10 個存活 namespace 保留掛載；其中包含當沖相關服務、
  網關和目前的 WSL 工作階段，不為清空而重啟或強制拆除。無法以有限速度
  合理預估需要人機交接或待容量釋放的整體完成時間。
- 曾列入的舊效能清單已核對歷史回收收據；剩餘部分 benchmark 具有外部
  hardlink，單刪其中一個名稱不會釋放配置空間，也可能影響仍在使用的路徑，
  因此不以表面大小認定本輪釋放量或繞過原程序保護。

## 2026-11-03 08:40 台北：當沖優先及逐分鐘觀測

- Penguin 的公開儲存 API 於 08:38:35 與 08:39:34 分別產生新快照；
  每個節點的量測及各批實際進度仍使用自己的時間。08:39 已登錄冷庫的
  NAS 檔案驗證為 108,060／108,153 檔、1,814,954,018,551／
  1,815,068,133,977 bytes；這是固定登錄清冊的 99.99% bytes，非全專案
  歷史完成。私有清冊 48,202／48,276 檔的進度仍是 11 月 2 日 13:05，
  等待 lab203 更新已過期的原 readiness。接收端 08:40 的原 owner 正在
  `start-pre`，不把心跳當成驗收或把先前 exit 75 當成目前鎖仍失效。
- D 實體可用仍為 0；E 原生 ext4 可用約 803 GiB、E 實體約 3.56 TiB；
  WSL guest 約 356 GiB、C 實體約 32 GiB。各數字分別是不同時間的即時
  觀測，不能將 guest 與宿主相加。D → E 的 sender 已切換，但先前固定
  162 個差異只有 E 封包成員讀回證據、10 個舊掛載 namespace 尚存；
  原 owner 每分鐘重查，不以封包讀回或暫時不可達冒充同名檔案及程序交接。
- 觀測器的大型 `du` 清點先前仍可在當沖開盤保護時段執行。現改為讀取
  當沖 guardian 的新鮮交易日／保護時段狀態，只讓可中斷的選用目錄掃描
  讓路；沒有可信 guardian 時，08:20–09:10 保守暫停，休市日可依新鮮
  回執繼續。原有掃描的程序會在下次檢查時收回，容量、節點與工作狀態
  仍每分鐘更新；保留已取得部分清冊。`test_storage_monitor.py` 89 項
  通過，包含掃描啟動後接到保護訊號的中斷與程序回收。
- 唯讀儲存觀測器已載入修正。公用網站的既有 gateway 尚在盤中使用舊投影，
  因此「開盤讓路」新原因目前可能尚未顯示，但不影響其原有每分鐘快照。
  為避免中斷盤中網站，已設定 13:36 台北一次性安全載入：僅在原 gateway
  PID 未更換且固定程式 SHA 未改變時重啟公用 gateway，變更時跳過；實際
  發生與網頁新原因仍需之後的正式觀測，不聲稱已驗收。
- Vast 的兩組既有固定範圍分別完成 256／256 與 368／368 個封閉包預驗證，
  正等待新封包；這不能代替全歷史、D 完整重建或 NAS 驗收。總完成時間仍
  無法計算：D 容量、舊掛載交接及接收端 readiness 都有尚未解除的前置條件。
- 實際備份 owner 的完整 catalog 快照為 2,054,087,965 bytes；08:30 與
  08:41 的公開清冊 identity 相同，但先前仍完整重寫。非阻塞程序取樣看到
  四個 metadata worker 當下都等待 D 槽讀 manifest；兩輪完整工作約 269 與
  310 秒，不能據此假定網路或 Vast 是目前瓶頸。
- 現在每輪仍按原規則完整讀取、核對來源 metadata 和目錄 pin；只有新清冊
  identity、逐檔來源世代指紋及已保存清冊本身的 inode／size／mtime／ctime
  等全部一致，才避免重寫相同的大型清冊。另以小型 observation 記錄新一輪
  時鐘，原 `catalog.json` 的舊時鐘不偽裝成新寫入；收據缺失、毀損、來源或
  目標變動時退回原完整寫入。它不代表原始資料重建、NAS ACK 或刪除權限。
  相關備份與觀測器共 228 項測試通過，含變更來源世代、清冊檔世代及損壞
  observation。為測試期間避免新舊版本重疊，只短暫停掉下一輪 timer，沒有
  中止當時已運行的 owner；timer 已恢復，08:48 新一輪正在執行。首輪仍需
  建立 observation，下一個內容完全相同的輪次才可觀測是否實際重用。
- E ext4 合成 ABBA 試驗在 8,157,071 bytes 清冊、10 萬個重複引用與
  1,000 個來源世代下，完整序列化／持久寫入中位 0.0522 秒，固定清冊
  重用與小 observation 中位 0.00728 秒；僅是該次合成的儲存子步驟，
  不代表 D 上 2 GiB 清冊或整個備份週期已有同等加速。測試只移除自己的
  E 暫存資料。
- 觀測器舊 cgroup 在重啟時的約 42.9 GiB memory peak 不是 Python heap：
  08:41 仍歸屬約 24.6 GiB file cache、9.0 GiB 可回收 slab、31 MiB
  anonymous memory；同期系統 `MemAvailable` 約 80.5 GiB。這是該時點
  cgroup 類型分解而非峰值當下逐項分解；避免開盤反覆掃描可減少後續
  cache／metadata 壓力，不以強制清快取或硬性佔用限制冒充效能改善。

## 2026-11-03 08:55 台北：來源錯誤與新版清冊首輪

- `tw-index-futures` 的前次處理曾記錄 `OSError: [Errno 5]`，目前重試又
  受到 D 槽實體空間不足阻擋。兩者是不同時間的事實：空間恢復後需由原
  owner 重試確認 I/O；不能只因更換阻擋原因就宣稱來源錯誤消失。監測程式
  已保留白名單分類 `io_error`，不將內部路徑或原始錯誤傳到公開 API；與
  現在的容量阻擋分開顯示。畸形狀態不得造成公開投影錯誤。
- 唯讀觀測器已載入，私有監測快照可同時看到上述兩個分類。公開 gateway
  的新投影與文字仍待前述 13:36 具 PID／檔案世代保護的一次性載入；若
  中途有變更就跳過，必須另行確認，不能視為已在公開頁面生效。
- 新版備份 owner 首輪已於 08:54 完成，歷時約 382 秒；此輪按設計寫入
  新的 2,054,087,965 bytes 清冊與 518 bytes observation。只有後續來源
  未變的輪次可以驗證是否免寫，不能把合成測試加速倍率當成正式週期結果。
  原 backup timer 及唯讀監測服務均已啟用。
- 此次新增 I/O 錯誤狀態及公開投影防禦後，儲存進度與監測測試共 256 項
  通過，網站 JavaScript 語法檢查通過。這些測試不是 NAS 獨立驗收，亦不
  賦予刪除來源的權限。
- 08:56 的原開機稽核以 exit 75 回報未就緒：FinMind 的 free、sponsor、
  complement 三個 unit 為 disabled／inactive；使用者先前曾暫停下載器，
  此處不自行恢復。另原 lakehouse 稽核顯示清冊回執過期、工作者載入版本
  與工作樹尚未核對一致，因此整體工作流程就緒未通過；owner 本身在運行。
  按原交接，更新工作者須等傳輸搬移的原有驗收與鎖交接，不能以強制重啟
  消除稽核。上述失敗原因及原稽核時間已納入本機每分鐘唯讀快照，公開
  gateway 仍待前述 13:36 條件式載入。新增案例後儲存進度／觀測測試為
  257 項通過；超過 12 分鐘的舊稽核不冒充目前原因。
- 08:58:59 的下一輪正式來源捕捉後，`catalog-observation.json` 已記錄
  `unchanged_snapshot_reused`，來源清冊 identity、逐檔世代摘要及既存清冊
  檔案世代均與首輪相同；清冊的原始保存時間仍為 08:53:09。這證明本輪
  避免重寫 2,054,087,965 bytes 的相同清冊，並保留本輪觀測時鐘；不證明
  封存整體速度達到硬體極限，也不代表任何原始資料或 NAS 驗收。
  該輪整個 owner 於 09:00 完成、耗時約 307 秒；前輪約 382 秒，因兩輪
  並非同負載的對照試驗，不能把差額全部歸因於免寫清冊。

## 2026-11-03 10:24 台北：舊掛載交接完成，D 回收已執行

- 阻礙並非 E 複製未完成：10 個既存 mount namespace 還保留已無法查找的
  舊 D bind。原 sender owner 取得原三個 writer lock，以固定 namespace FD、
  開機身分、完整 namespace census 和原 E 身分逐一交接。所有 10 份逐項
  journal 已是 `alias_verified`，新 census 為零舊掛載，Syncthing 原 alias
  指向 E；沒有重啟交易、Temporal、Discord、安全服務或更改父掛載傳播。
- 最早的兩個交接使用正常 bind 即可；另外八個 namespace 缺少 E filesystem，
  需要先在原 owner 建立帶完整 VFS 權限的 detached mount FD，再交給固定
  namespace 附接。第一版的失敗、已移除舊掛載但尚未附接的 intent 均保留，
  新版沿原 journal 續作；零舊掛載本身不能跳過未完成 intent。隔離 clone
  實測完成 0.159 秒，原 namespace／PID 1 不變；不是正式冷開機驗收。
- 固定候選共 44,257 檔、138,422,642,761 bytes。10:24 的原退休日誌已核對
  並刪除 10,749 個 D 傳輸副本名稱、31,310,777,433 logical bytes，3 檔保留。
  每檔仍在 E 完整 SHA 讀回、核對來源世代、程序引用與持久 prepared 日誌後
  才回收；D CAS、原始觀測及 NAS 原件刪除數均為零。此數是傳輸名稱的邏輯
  位元組，不能當作 WSL 新釋放量，也不等於並行寫入下的 D 淨可用量。
- 三個不同檔案為舊 `recovery-index.json`、`source-status.json` 與 transport
  marker；另 159 個 E 同名缺件主要是舊 `.staging` 分塊。原 E 差異 tar 與
  162 個成員的獨立還原已通過，但不能單靠封包保全刪除目前仍未同名驗證的
  舊 D 名稱。全輪回收、來源恢復與 NAS 驗收仍分開計算。
- 原鎖等待現在持續合作式重試，任何一把鎖忙碌即釋放其餘已取得的鎖，不
  刪鎖或中止工作。已排隊搬移的新鮮 hint 也會讓原 backup cycle 在收據
  ingestion／安全傳輸回收後先讓出執行權，避免再開始一次大清冊掃描。
  舊 coverage 與 catalog 時鐘保留，活動時間另記，不能偽裝成新 NAS 證據。
- 可選的大型目錄分類在新鮮搬移／回收工作期間讓路，容量、三節點狀態及
  工作日誌仍每 60 秒取樣。D 回收進度同時提供 bytes 與檔數；長短檔混合時
  不再用完成檔數推算整輪到期。原每分鐘 timer、下一次重試及前置條件仍
  顯示；無法量測的 NAS／來源等待沒有虛構的整體完成時間。
- 實際 retirement 程序取樣看到四個 E SHA worker 等待、主執行緒在 D unlink。
  新版重用原有 bounded pool，僅在該批 prepared 日誌持久化後平行處理獨立
  unlink，SQLite 仍由單一 owner 更新。原執行中的舊批次保留，新版由其
  後續原排程載入；未宣稱此輪已採用或整體已加速。測試覆蓋每個 unlink
  前可從獨立連線讀到 prepared、中斷後補記與內容／世代差異仍保留。
- 後續若只剩待核對成員，先以原清冊及 journal 比對並檢查它們的 E 同名完整
  SHA／中繼資料；沒有任何可回收項時不重掃數萬個已回收 D 名稱、不刪檔，
  另記等待活動時間，原進度時鐘不變。有一項恢復可用即重入原完整驗證；
  缺／錯 journal 不能啟用此捷徑。這是避免無效工作的 no-action 判斷，不是
  重新驗收舊缺件、延長還原證明或限制下載配額。
- D metadata 的 384 檔 ABBA 有界試驗，4／16／16／4 workers 分別約
  0.450／0.367／0.391／0.299 秒；沒有穩定的增加 worker 收益，因此沒有
  直接提高平行數。這是暖 metadata 子步驟，不能宣稱硬體吞吐上限。
- 原 worker 的新版載入改為比對目前 candidate fingerprint；舊的 accepted
  reload receipt 不能使新碼免驗。原 alias 及全部 namespace 已驗證後，即使
  少數舊名稱仍需保留，也可由原 drained rollout 交接，不必等待它們消失。
  尚須取得新的 loaded-code／workflow chain 收據，不能將 timer 排入視為完成。

API 依據：[open_tree](https://man7.org/linux/man-pages/man2/open_tree.2.html)、
[mount_setattr](https://man7.org/linux/man-pages/man2/mount_setattr.2.html)、
[move_mount](https://man7.org/linux/man-pages/man2/move_mount.2.html)。
正式執行證據在 `/var/lib/stockagent/workspace-volume/transport-relocation-v1/`；
失敗與隔離演練也保留。主要未完成項仍是 C 的其餘大量來源逐組遷移、D 最後
一個資料集的真實 I/O 重試，以及 lab203 更新原就緒回執／完成 NAS 獨立讀回。

## 2026-11-03 10:50 台北：公開驗收與持續回收

- 10:49:55 的原回收日誌為 16,893 檔、50,319,206,617 logical bytes
  （46.86 GiB），3 檔保留，固定候選總量未變；仍持續回收，未記作整輪完成。
  10:45 公開快照的 D 淨可用約 41.06 GiB，C 實體約 19.99 GiB，WSL
  guest 可用約 340.24 GiB、E 工作卷約 803.36 GiB。它們是不同取樣時點，
  且 C／WSL 不能相加；其餘 C 大量來源並未宣稱全部搬完。
- 公開 API 的 10:39:57 與 10:41:57 快照均有 Penguin／lab203／Vast
  各自新取樣。原回收計數未變的分鐘仍保留 10:39:51 進度時鐘，沒有以
  心跳刷新進度。網頁已用 bytes 作回收百分比，同時顯示檔數、原 owner
  timer 與等待原因。過長批次另以原 kernel lock 觀察是否仍忙碌，避免
  120 秒進度樣本過期時又啟動可選 du；不延長任何驗證或 NAS 證明。
- 相關回歸 459 項通過；其後單次 non-following 查詢、平行回查與回收、
  held no-action、進度／監測等聚焦 309 項通過。失敗項修復後另有通過
  重跑；測試不等於 NAS／原始資料的正式還原驗收。
- 桌面 1440 與手機 390 寬度的 Chromium 共 35 個案例通過。版面驗收固定
  本次實際 API 10:45:07 快照，避免檢查中途換分鐘造成來源與 DOM 不一致；
  實際 polling timer 的分鐘更新、等待轉接續及過期狀態另用 fixture／虛擬
  時鐘驗證。測試讀取原公開頁面資源，採 CPU 2D、provider API 呼叫零。
  收據及圖在 E 的 `verify/storage-browser-20261103-live-v6/`，先前因收合
  清單或繼承過期 fixture 欄位而失敗的演練亦保留，不冒充正式資料異常。
- 唯讀 storage-monitor 已載入這些變更；交易與公用 gateway 原 PID 保留。
  gateway 新原因文字／投影的 13:36 條件式載入仍只接受原 PID 和已核對的
  程式 SHA；目前僅更新已測試的三個 storage 成員，其他 gateway 成員未變。
  條件變動會跳過，不把預定載入時間當作已完成。
- lab203 的 CPU／容量心跳持續更新，原 readiness 仍待重新檢查；Vast 的
  624 個已分配封閉包預驗證完成，等候上游新封包。全流程仍有未量測等待，
  因此保留未知整體 ETA；不以本輪回收比例推估 NAS 或全部 C 遷移期限。
- 10:54 公開快照仍保留完整 D 回收完成 gate；其舊原因文字卻將已完成的
  namespace 核對一併描述為未完成。已將這個一般原因改為「D 舊副本尚未
  全部完成回收」，與真正的 stale namespace 原因分開，沒有放寬完成驗收。
  狀態投影 170 項通過；已更新原 13:36 條件式載入的單一成員 SHA，其他
  gateway 成員繼續逐項核對。該文字尚待原計畫載入，不能視為目前 backend
  已換版。最新 10:55 日誌為 20,477 檔、59,972,591,187 bytes（55.85 GiB），
  仍非整輪完成；10:54 的 D 淨可用 59,968,540,672 bytes（55.85 GiB）。

## 2026-11-03 13:06 台北：實測大型 CAS 讀回與排程交接

- 原 D → E 逐檔回收於 11:19:50 達 44,095／44,257 檔，累計
  137,066,186,602 logical bytes（127.65 GiB），尚保留 162 檔、
  1,356,456,159 bytes。保留的 E 差異封包／逐檔重建證據沒有變成原名稱
  可用證據；3 個控制檔世代不同與 159 個缺少 E 同名的舊 staging 檔仍保護。
  後續每分鐘檢查保留項不重掃 D、不重做刪除，也不刷新原回收計數時間。
- 13:04 的公開實測：C 實體可用 20,351,602,688 bytes（18.95 GiB），
  WSL guest 可用 354,130,546,688 bytes（329.80 GiB），D 實體可用
  117,831,557,120 bytes（109.74 GiB），E 工作卷可用
  857,518,100,480 bytes（798.63 GiB）。E 宿主剩餘約 3.56 TiB，不代表
  目前約 1-TiB guest 已擴到 3.5 TB；各數字與回收量亦不可直接相加。
- 原 worker 取樣仍在既存 CAS winner 的完整 SHA 核對，因此以原唯讀
  `benchmark_cold_blob_read.py` 做 ABBA，不搬資料、不清 OS cache：
  8 個真實 blob、95,918,672 bytes 的 DrvFs 為 40.775／14.312 秒，原生
  Windows 批次為 9.385／4.107 秒；另一個 248,934,776-byte 真實 pack，
  DrvFs 為 151.345／82.895 秒，原生為 63.516／16.678 秒。每次均驗完整
  SHA／前後世代，後者單步中位數約快 2.92 倍；樣本有快取及並行負載，
  不能換算成完整備份倍數或硬體最高速。精確原始收據在 E 的
  `verify/cas-native-hash-20261103.json` 與 `verify/cas-large-pack-hash-20261103.json`。
- 大型既存 CAS winner 及 concurrent winner 現改用同一 native full-SHA
  adapter，限原 canonical 公／私冷庫、64 MiB 至 adapter 原本的 8-GiB 上限。
  原權威、精確成員、世代、checksum 與 durability 驗收仍在；失敗保留原
  incoming／E 暫存，不降級成未驗證重用。小檔及其他路徑仍走原 portable IO。
- 原 placement timer 已自然載入新排程：已驗 E alias 的 worker 換版先走它
  自己的 publication boundary，不再排在無關的 sender／GC 鎖後。單次等待
  由 45 秒改為 600 秒，最多每 30 秒更新真實等待；每次皆先釋放部分取得
  的鎖。只有未取得邊界的 `PublicationOwnerBusy` 不消耗錯誤重試額度，真正
  的 admission／body 錯誤保留並有限重試。13:06 正式 worker 仍為原 PID
  683806，不能宣稱新 CAS／E scratch 碼已載入；沒有中斷它正在發布的來源。
- 測試亦修正隔離 transport root 誤用本機正式 E enrollment 的問題；只有
  canonical D transport 可跟隨 E 切換。明列 D 與 E 的 Windows path 測試
  不再依賴機器當下預設。相關 packed／placement／面板 553 項、原控制角色
  lifecycle 118 項、native IO／monitor／runtime 212 項通過。錯誤角色缺
  `temporalio`、舊環境耦合失敗及早先失敗日誌保留；不算 NAS 實際驗收。
- 唯讀 observer 13:06 已載入新投影；公用 gateway 維持原 PID，仍排定 13:36
  僅在原 PID／固定 SHA 全符時載入。已在同一 manifest 只更新測過的
  `storage_progress.py` SHA；新增 worker 交接列會分開顯示，不以等待心跳
  冒充資料進度或 NAS 驗收。
- Vast 持續由原 timer 接新封閉包：13:04 兩個候選範圍為 256／256 及
  424／425 packs（後者的候選總量會隨新封包增加）。這是預驗證，不是最後
  來源驗收。原 D migration 仍是 60／61；最後 `tw-index-futures` 的
  13:06 原進度為 1,000,038／4,601,679 檔，處理 330,729,719,777／
  469,290,349,287 logical bytes，尚在封裝。C 的既有 FinLab／FinMind／
  Binance／新聞等原始樹沒有宣稱全部遷移。lab203 心跳持續更新，原就緒
  回執仍待 owner 實際重驗，見原交接文件；不把全面完成時間寫成固定倒數。
