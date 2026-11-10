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

## 2026-11-04 00:04 台北：排程及 SQL 優化正式載入

依使用者先前允許立即重啟儲存 worker 的授權，先固定通過測試的 source 與
完整 SQL parity，再暫停獨立的 transport maintenance waiter；原 code-only
交接器於 23:52:22 驗收，儲存 PID 3881792 → **282975**。只有這個 storage
worker 的未完成階段由原 durable journals／Temporal heartbeat 重排；原 PG、
Temporal server、工作鏈與 **61 個固定版本**保留。原 maintenance timer 已恢復。
原下載器與當沖 PID 430／隔夜 PID 2335 的啟動時鐘不變。

正式 candidate／loaded identity 同為
`b83277fcd0b63b1df53de6ff88cf337813490bf32c76c0df2fabaa860cce41da`。
完整交接、before intent 與實測證據位於 E 的
`verify/backup-optimization-handoff-20261103-v1.json` 及同名 `-intent.json`，
原 timer 的 `worker-reload.json` 也由同一 helper 寫入真實 accepted，並非觀測器
改時間或複製舊 ACK。切換前因開發期間 candidate 改變而中止的 waiter 證據保留，
沒有清除失敗、鎖或任何來源。這次沒有新增 C 原件、D authority、NAS 刪除。

排程修正的實機覆核另抓到兩層問題，現已一併處理：

- `sender_boundary` 的 GC purpose 曾在 installer 的外層 `BlockingIOError`
  處理被覆寫為通用 hint。現在由 `SenderBoundaryBusy` 保留類型，外層原
  `record_placement_wait` 沿同一封閉控制檔保存；例外經過完整 timer 出口後仍
  能分辨獨立 GC 與 namespace 交接。23:55 原回執的 purpose 為
  `old_D_transport_retirement`，不再借用 cutover 優先權。
- 目前持 catalog-owner 的是 SQL／catalog activity；BackupStream 不取得這個
  SQL lock，讓它停住也不能釋放該鎖。新鮮 worker 交接 hint 只在來源 stream
  能讓出的邊界上優先；獨立 catalog 等待不抑制新備份，真正 namespace 交接
  仍保留優先。全部原有 locks、physical reserve、完整讀回與退休 gate 不變。

共用 export／restore 的 SQL digest 已按數學等價條件優化：先確認原欄序及
非 NULL 唯一鍵，再以 SQL `dense_rank` 確認排序語義下也無 ties，避免大小寫
collation 把不同字串當成相同排序鍵。可用 SQL budget 與目前 host／cgroup
headroom 足夠時，只按前兩個唯一鍵排序一次，仍串流全部欄位產生原 canonical
JSON SHA。低記憶體／觀測失效維持 64 MiB／2,048 列分頁；異常鍵、NULL、
重複或欄序不符維持原 `ORDER BY ALL`，沒有省略、去重或改 source identity。
記憶體判斷不調高任何正式限制；舊 restore capsule 缺少可選觀測 helper 時也
保留原演算法。

| 固定 snapshot 1049，全 10,248 列 | SQL／digest 秒 | process peak RSS | 完整 SHA |
| --- | ---: | ---: | --- |
| 單次唯一鍵排序，獨立 probe | 88.48 | 7.93 GiB | `e0ce8e23…fee20fa` |
| 原完整 `ORDER BY ALL`，獨立對照 | 139.95 | 10.88 GiB | 相同 |
| 最終 adaptive helper，含 SQL rank／容量判斷 | 112.33 | 7.99 GiB | 相同 |

原內容為 **3,909,791,481 bytes**；三者前後 snapshot 均為 1049，code source
在各自試驗期間未變，沒有 production catalog／source write。最終 adaptive
實測是 **1 次 single-pass sort、0 次 payload page sort**。這些是並行工作負載
下的 SQL／digest 單階段結果，不是 A/B/B/A、公平的全備份倍數或硬體極限。
證據為 E `verify/catalog-native-digest-probe-20261103-v1.json`、
`verify/catalog-native-full-reference-20261103-v1.json`、
`verify/catalog-adaptive-native-probe-20261103-v2.json`。完整 SHA 及來源固定版本
另列於 [lab203 交接](lab203_runtime_observer_handoff_2026-11-03.md)。

最終回歸為 **488 個備份／排程／監控測試**、**270 個原生 control／還原／
workflow 測試**、**166 個交接模式／生命週期測試**；三組有重疊，不相加。
公開 HTTPS 桌面／手機另有 39 案例通過，provider calls 為 0。早期金融環境的
62 passed／1 skipped 沒有當作完整驗收，後續用鎖定的 control role 完整重跑。

### 尚未完成與目前耗時

23:43:27 有一輪原備份實際發布新 envelope，完整循環 **1,069.11 秒**；
23:15:50 另一次為 670.77 秒。負載、冷目錄與並行試驗不同，不能用這兩個
自然循環聲稱整體已加速。23:55 的新 worker stack 在
`register → capture_catalog → prefetch_manifest/read_metadata`：目前瓶頸
已包含來源改變後的完整 metadata 清點，最終正式 export／NAS 讀回仍另驗。
全量原始碼與 index/manifest 的增量設計仍有後續工作，尚未證明只剩硬體限制。

三端分鐘觀測仍有各自原時鐘；23:54:56 Penguin、23:54:36 lab203、23:54:55
Vast 皆可讀。Vast private 為 256／256、public 為 697／697 個已封閉候選；
它持續接新包，沒有派送重複的候選。私有 NAS 仍是 87.94%，差 91.81 GiB，
正式 readiness 仍待 lab203 原 owner。C 三個回復入口仍保留，不能把本輪
278.58 GiB E copy 當成 WSL 或 C 已釋放。舊 D transport 的 162 個差異名稱
仍依原回收 gate 保留，不以 E archive 的成員演練代替 live-name 完整讀回。

lab203 的新審核包使用獨立 `storage-operations-20261103-v2`，沿原不可變
control namespace 發布，不覆寫 v1，也不部署 receiver、修改資料 ledger 或
代替 NAS ACK。先完成原就緒／repository 檢查，才能繼續私有歷史的驗收。

## 2026-11-03 23:20 台北：區分新備份交接與獨立的舊 D 副本回收

沿用同一 task 與使用者指定的視窗分工，本輪未修改 provider 搬移、下載器或
共用 copy 實作。三份 E 工作區交接與 61／61 固定 D 重建證據保留；C 三個
回復副本仍存在，沒有新的 C 原件、D authority 或 NAS 刪除。

獨立發現的排程問題是：舊 D transport 的 162 檔保留差異仍由原 GC 檢查，
等待原三個 writer locks 時持續發出通用 `waiting_for_original_owner_or_reader`。
BackupStream 會把新鮮訊號當成 cutover，先 ingest 既有 ACK 後跳過 catalog
capture，原 status 因而反覆為 `waiting_for_transport_placement`。這不是所有
循環都沒有派送：23:15:50 仍有一輪 `published_waiting_nas_receipt`，完整循環
670.77 秒。問題是獨立 GC 的等待可以重複抑制後續來源捕捉，不能以 source
服務 active 或通用等待標籤認定 throughput 正常。

原 sender boundary 現在明示 `namespace_handoff` 或
`old_D_transport_retirement`。新鮮的真正 namespace 交接仍優先；獨立 GC
等待不再抑制新備份。另讀原 `worker-reload.json` 的固定 contract、候選 SHA、
非強制／未更新進度 flags 與 120 秒有效期，讓真正的 drained-worker 交接仍能
取得安全邊界。所有 partial locks 仍於等待前釋放，單次 60 秒回到原 timer；
沒有改動 mount、physical reserve、來源讀回、NAS ACK 或退休條件。

474 個 backup／placement／progress／monitor 核心回歸通過，包含真實 fixture
備份循環在 GC 等待時仍執行 catalog capture、真正交接仍讓路，以及失敗／
過期 metadata 不清除任何鎖。新增 publication 監控後的 295 個 progress／
monitor 測試另通過；兩組範圍重疊，不相加。E 證據為
`verify/backup-gc-wait-isolation-20261103-v1.log` 與
`verify/backup-wait-monitor-20261103-v2.log`。

23:22:12 公開 HTTPS 的桌面／手機 39 案例通過，provider calls 為 0，收據在
E `verify/backup-wait-browser-20261103-v1/browser_acceptance.json`。監控同時揭露
台股公開發布的最後真實阻擋：19:07:56 原發布收據報告
`stale_official_symbol_build_receipt`。頁面只顯示白名單原因及原收據時間，
不送出 traceback、私有路徑、原始值，也不把分鐘心跳當成新的發布或故障。
原建置收據 gate 保留，仍由原建置／發布 owner 處理。

### 另行定位的 SQL 匯出瓶頸

唯讀原程序 stack 顯示 catalog owner 正在 `_metadata_rows` 的分頁 SQL，
不是僅因鎖檔存在便判為死鎖。固定 snapshot 1049 實際有 10,248 列、
3,909,791,481 原內容 bytes，最大單列 4,275,841 bytes。一次窄欄位統計
前後 snapshot 皆為 1049，沒有 catalog／來源寫入，證據是 E
`verify/catalog-export-metadata-size-20261103-v1.json`。

既有唯讀 benchmark 增加「觀測目前原生資源」模式，不恢復歷史 cgroup 限額，
也不更改任何正式設定。先確認 RAM 可容納完整排序，再對全部欄位逐列 canonical
JSON 雜湊，結果保持自己的快照、程式／owner 身分與實際耗時。第一次唯一鍵
排序讀完全部 10,248 列，88.48 秒，程序尖峰 RSS 8,511,258,624 bytes；完整
SHA 為 `e0ce8e232d9b163400a8b36746b3cabf0ca9795da17ce0e46b7f5eee7fee20fa`。
這只是本機完整 SQL／digest 階段，不能外推成公平的整條備份倍率、硬體上限、
NAS 完成或新的 source retirement。原完整 `ORDER BY ALL` 另作獨立對照，
正式採用狀態與後續驗證依下一段收據更新。

## 2026-11-03 22:20 台北：E 交接完成，C 回收與 NAS 驗收分開呈現

沿用使用者的視窗分工，本次只修改監控及其獨立驗證，沒有接管或修改另一視窗
的 provider 搬移／下載程式。原 `provider-state-temporary-20261103/result.json`
於 21:59:22 記錄 `verified_e_provider_placement`，三份 selected copy、placement
與 namespace 收據均通過原觀測條件，原來源刪除計數為 0。

| 本輪選定來源 | 已複製並獨立讀回的檔數 | 邏輯 bytes |
| --- | ---: | ---: |
| FinLab | 1,086,965 | 168,813,852,834 |
| FinMind | 2,663,365 | 118,221,861,150 |
| TEJ | 1,617,244 | 12,085,487,004 |
| 合計 | 5,367,574 | 299,121,200,988（278.58 GiB） |

這些是本輪選定工作區的 E 完整讀回量，**不是 C 可回收量、全部來源歷史或 NAS
驗收量**。FinLab 已存在的 E 期貨子樹沿用；FinMind 排除的美股分鐘子樹仍依原
唯讀來源合約保留。本次沒有刪除三個 provider 的 C 回復副本，也沒有新的實際
WSL 釋放量。原 `resumed.json` 於 21:59:02 記錄六個原排程單元恢復；有限任務
已由另一視窗沿原排程啟動，其資料驗收不能由 dispatcher 的 running 代替。

監控新增「E 交接後的 C 回復副本」獨立列。每分鐘只對已登錄的三個固定入口
做 metadata 檢查，不掃描其中 536 萬檔，不將已複製邏輯 bytes 當作 C 配置量；
入口缺失、換盤或改成連結也不會變成回收驗收。原 owner 尚未提供本輪 C 回收
證據時，明示待驗收且不套用 E 複製的 ETA；觀測器過期仍照常告警。
FinMind 三個舊常駐單元全部實測 disabled／inactive 時，開機稽核只陳述舊單元
已停用，有限任務另驗；未啟用重複下載器，也沒有清除真正的儲存回執過期告警。

22:19:43 的三端公開快照中，Penguin、lab203、Vast 的獨立時間皆在同一分鐘
附近。原 D 固定重建維持 61／61；Vast 候選預驗證為 private 256／256、public
663／663，等待新封閉封包，不能因此稱 NAS 完成。私有 NAS 仍是
718,647,796,617／817,226,447,345 bytes，差 98,578,650,728 bytes（91.81 GiB）；
正式 readiness 仍為 11 月 2 日 12:49:56。新的 CPU／RAM 心跳沒有更新舊驗收，
01:14 的 Restic 鎖診斷也不是目前仍持鎖的證明。原 lab203 接手要求仍在
[每分鐘觀測交接](lab203_runtime_observer_handoff_2026-11-03.md)，沿既有固定控制包
與回執通道，不另建備份 owner。

本輪 projector／monitor 回歸為 **287 passed**，JavaScript 語法與限定檔案 diff
檢查通過，完整 log 在 E `verify/provider-retained-monitor-20261103-v1.log`。
這些是監控語義與工程驗證，不是 C 副本退休授權或硬體吞吐極限的實測。

22:31:11 公開 HTTPS 的桌面／手機 Chromium **39 案例通過**，`provider_calls: 0`，
證據在 E `verify/provider-retained-browser-20261103-v2/browser_acceptance.json`。
未知 ETA 顯示「完成時間：尚無可用估計」，阻擋原因只呈現一次，避免在
「預估完成」標題後重複長段文字或讓未知時間看似完成。自然取得的三份公開
快照中，lab203 原取樣增量為 59.998、60.001 秒；Penguin／Vast 的實際完成
時間有探測延遲（約 52–73 秒），沒有將它們回填成整分鐘。原 observer 及前景
頁的排程均為 60 秒；這三份證據不是最壞情況 SLA。自然更新證據是 E
`verify/provider-retained-natural-refresh-20261103-v1.json`。只重啟唯讀 observer
及 gateway，當沖 PID 430／隔夜 PID 2335 與 11 月 2 日的啟動時鐘一致。

後續釋放空間的原作業依賴仍明確保留：原 provider 搬移 owner 固定 C 副本的
清冊、排除子樹與實際配置量，通過獨立還原、世代和程序引用條件後接續正式
退休；lab203 原 backup owner 需回傳新的就緒及逐批 NAS 證據。不能只將 E
可讀、C 入口不存在、NAS 心跳或 Vast 候選全數驗完當成這兩項已完成。本視窗
沒有替另一視窗啟動第二套搬移／刪除器，也沒有宣稱這個新增 C 回收階段已排程。

## 2026-11-03 20:43 台北：限監控接手，確認搬移子任務的兩項例外

使用者確認另有視窗更新 provider 搬移程式，本視窗只處理監控與獨立問題。
未修改該組 `relocate_provider_workspace.py`、`provider_workspace.py`、
`workspace_relocation.py` 或其原搬移進度，沒有停止仍在複製的 FinMind。

唯讀堆疊顯示原父程序在 `ProcessPoolExecutor.__exit__` 等待其他子任務結束，
兩個 child 已閒置。為取得尚未印出的既有例外，僅讀取已辨識 migration
owner 的 CPython 3.12 exception 物件，核對 PID／start ticks／boot，沒有
向目標注入執行或更改記憶體；完整診斷留在 E 私有驗證區。

- FinLab 原任務在舊 namespace 讀取 registry 時發生 `KeyError: 'finlab'`。
  E 複製收據仍有效；失敗的是路徑交接，不能標成整份複製失敗，也不能稱交接完成。
  另一視窗已準備新的 FD-based namespace helper，仍由其原接手器處理。
- TEJ 原任務為 `ValueError: source namespace changed after relocation`。
  另一次有界 10,000-entry 盤點看到 `api_trial_v1` 目錄的 mtime／ctime 與原清冊
  不同；這不是完整差異清冊。原 owner 仍需找齊真正差異與寫入來源，不可直接
  改舊驗收或將整份來源當作已完整遷移。保留原索引、已驗證的 E payload 與失敗證據，
  再由該視窗決定原流程的增量重核／replan。

診斷在 E `verify/provider-runtime-diagnostic-20261103-v1.json`，是非 ACK
的監控輸入。每分鐘只核對小型 metadata、原父子 PID／start／boot，以及 exact
intent／progress 的 SHA；新進度、新 intent、程序替換或真正交接收據出現後，
舊診斷自動不再覆蓋目前狀態。診斷時間維持 20:33 的實際例外觀測時間，不寫成
例外發生時間、工作進度或驗收。公開頁只顯示 allowlist 原因與時間，不傳 traceback、
process identity、私有路徑或來源資料；沒有新增持續掃記憶體的監控程序。

監控自身的遞迴分類也讓路給已登錄、尚未完成的 C／E provider 搬移；容量、
運行與工作進度照常每分鐘更新。分類 fingerprint 納入 provider 位置／交接狀態，
讓切換完成後自動重新清點，不把 heartbeat 當佈局改變或重掃全部遠端磁碟。
這是監控 I/O 排程，沒有調整 provider workers、配額、來源或原 publication gates。
原搬移視窗後續可在每個子任務返回前記錄原生例外與階段計數，並即時收集已完成
的 futures；保留尚在運作的其他任務。這能取代本次一次性的例外記憶體診斷，
避免父程序等最慢任務收尾時，其他失敗工作仍停在最後一個 100% 計數。

本輪 281 個 projector／monitor 測試通過；桌面／手機 39 個 Chromium 案例通過，
provider calls 為 0，包含例外顯示、原進度時間保留及不可繼續給有限 ETA。
E 證據分別是 `verify/provider-exception-monitor-20261103-v2.log` 與
`verify/provider-exception-browser-20261103-v1/browser_acceptance.json`。
公開 20:53:42 快照的三端觀測在同一分鐘內，兩項故障已明確顯示，FinMind
仍持續複製。觀測到 TEJ intent 於 20:43:03 有另一視窗的新修改；上方診斷
指原執行者保留的例外收尾，不表示該新清冊已重跑並失敗。新原執行者／進度
或真正驗收將取代這個暫態診斷。當沖／隔夜 PID 與 11 月 2 日原啟動時間保留。

## 2026-11-03 晚間：61／61 固定來源完成，工作區搬移分階段觀測

18:32:54 的原 `dataset-versions/current.json` 為 `verified`，61／61 固定來源
完成 D 原始重建；最後 `tw-index-futures` 的 4,601,679 檔通過驗證，177 個
pack 由 Penguin 完成 167 個、Vast 完成 10 個。這是固定 D release 的恢復
證據，不能代表後來新增的 provider 原始值、NAS 或所有歷史皆已驗收。
原 lifecycle 在 18:35:19 自然交接，`worker-reload.json` 的 candidate／loaded
identity 相同，PG／Temporal 與原 workflow chains 保留。
晚間追加的工程修改另有新 candidate，仍由同一 timer 等完整發布邊界；
不能用 18:35 的接受證據宣稱後來每一份程式都已載入主 worker。

這輪從 preparation JSON 的固定名稱逐項 stat，舊 186.8 GB 宣告量中，仍在
C 的已註冊工作副本只剩約 111 MiB；不是額外可回收 186.8 GB。沿原
`relocate_private_sqlite_scratch.py` 完成 FinLab `intraday/market_index.sqlite3`
的 E 複製、完整 SHA、獨立讀回、綁定與程序／世代核對，再回收
**116,158,464 allocated bytes（110.78 MiB）**。SQLite 原來源、D、NAS 的
刪除計數皆為 0。證據位於 `7fa3bf9d5be100aa-c113544519884b839486d655bbbe3d66`
operation 的 `workspace-relocation/556d999838396b17023d5e0b/complete.json`。

三個 provider 的原搬移工作由另一個既有 task 執行；沒有接管其原 coordination
locks、修改 raw、刪 C 回復副本或重新啟動第二份搬移器。每分鐘 observer 只讀
固定 registry、intent、copy／placement／namespace JSON，加入獨立容量及檔數
進度。複製階段 ETA 取 bytes 與 file tail 較慢者；原路徑交接用另一列及自己的
計數，不將 100% 複製或複製速率當作 namespace 完成時間。原服務恢復仍另驗。
本輪選定清冊為 5,367,574 檔、299,121,192,775 logical bytes；FinLab 已在 E
的期貨子樹沿用，FinMind 美股分鐘的三個排除子樹仍留在 C 的固定唯讀副本。
這些排除部分不能宣稱已搬 E，三來源 C 原件也不能當成本輪已回收量。

### D 冷庫名稱列舉

19:50 對當時原 backup child 的單次非阻塞堆疊取樣，定位在
`_capture_catalog` 的 `os.walk(cold / 'objects')`；該輪總執行時間已超過
十多分鐘，單次取樣不能證明整段時間全耗在此行。原程式已取得引用物件的
fresh regular-file signatures，又在遍歷目錄時重做每個 leaf 的類型查詢。
現在使用同一 `PinnedObjectSignatures` 的 no-follow 目錄 FD 列舉名稱；原本
每個引用物件的本輪簽章、未引用物件的獨立 regular-file 簽章、完整 SHA、
目錄／root identity 重查及所有 NAS／退休 gate 保留。非標準目錄結構沿原
遞迴診斷，不靜默漏件；Linux 對 no-follow symlink 回 ENOTDIR 的情形明確拒絕。

原 owner namespace、D `objects/blobs/00` 的 209 個名稱 ABBA 實測：舊步驟
0.0697／0.0510 秒，新步驟 0.0263／0.0361 秒，中位比約 1.93 倍；四輪名稱
集合 SHA 相同，未清 OS cache。只證明這個列舉步驟，沒有宣稱完整 backup
或硬體極限。證據在 E `verify/backup-cas-name-census-abba-20261103-v1.json`。
首次從非 owner namespace 進行測量被原 D mount request-size guard 拒絕，
沒有降低 gate；之後以原 control wrapper 實測。207 個 backup／pinned-reader
測試通過；先前 symlink regression 的失敗日誌保留，修正後重跑通過。
只在原 backup service 已退出的邊界暫停 timer 完成驗證，之後恢復同一 timer；
未中斷一個正在執行的備份。正式整輪效益仍看新一輪原 owner 的收據。
20:08:54 的實際新 backup child 堆疊已出現 `directory_names`／
`_cold_object_paths`，證明原 oneshot 已採用新路徑，並非只修改原始碼。
原 unit 的下一輪於 20:12:31 正常結束：systemd 記錄完整 process wall time
12 分 12.133 秒、CPU 7 分 31.379 秒；前一個完整清點／發送循環為
16 分 19.695 秒、CPU 9 分 26.217 秒。這些是原單一 owner 的實際循環，
含程序啟動、不含獨立 NAS 驗收；期間清冊與併行負載不同，不能當作純程式
加速比或硬體峰值證明。固定 journal 摘要保存在
E `verify/backup-live-cycles-20261103-v1.json`。原 timer 20:13:02 已自動接續
下一輪，沒有另外啟動一套備份或改寫前一輪驗收。

工作區 projector／monitor／原 SQLite 搬移器 283 項測試通過。新階段測試另
驗證「交接尚未開始」沒有工作計數、沒有 ETA，不誤稱壞 metadata；每分鐘
觀測本身過期時仍顯示過期。早先 focused stale-clock 失敗及 observer 重載
期間讀到前一版快照的 browser 失敗都保留，最終瀏覽器驗收須在新快照與
公開服務就緒後執行，不能以較早的 37 案例替代新階段驗收。
最後先核對公開 API 已使用 20:09 的新快照與交接等待原因，再啟動獨立驗收；
E `verify/storage-browser-provider-e-20261103-final/browser_acceptance.json`
的桌面／手機 37 案例通過、provider calls 為 0，包含交接列不可沿用複製 bytes／
ETA 的驗證。20:11 公開快照三端觀測皆在一分鐘內；FinMind 為
102,446,860,605／118,221,861,150 bytes（86.66%）、
1,356,097／2,663,365 files，複製階段暫估 21:06 台北。此後的名稱集合、
路徑交接、C 原件回收和 NAS 不能套用該 ETA；原頁面每分鐘持續重估。

lab203 主機分鐘觀測正常，原 readiness 仍未更新；私有備份尚待
98,578,650,728 bytes。既有固定 `storage-operations-20261103-v1` 控制包已由
paired receiver 索引回報可用，仍不等於接收驗證或 NAS ACK。請原 lab owner
核對目前前置階段、repository／Restic 鎖與實際重試時間，執行真正就緒檢查，
沿原反向通道回傳新 readiness 與逐批獨立還原結果。本報告晚間新增內容
不是已凍結 v1 控制包的新成員版本。

## 2026-11-03 17:05 台北：WSL 新增回收 39.24 GiB

舊 preparation metadata 有 62 組仍指向 C，宣告 logical bytes 合計
186,757,358,581；它不是實際配置量或可刪除量。這輪只選沒有固定發布或
還原綁定的舊 `7fa3bf9d5be100aa-c113544519884b839486d655bbbe3d66`／
`finlab-research`，加入原 controls-relocation timer 的第四組固定清冊。
16:57:52 啟動，17:03:35 完成 1,456 檔、42,125,547,602 logical bytes
的 E 複製、完整 SHA 讀回、引用切換、程序與刪除前世代核對，實際回收
**42,137,661,440 allocated bytes（39.24 GiB）**。source／D CAS／NAS
刪除計數皆為 0。固定 controls identity 為
`c109782cf06edb7cbf6a45ac0f53a3bf9e28bfeec9729b7e45016da87684a933`，
原日誌在該 operation 的 `workspace-relocation/1cadf944abb9cf863ff3f0c9/`。

17:05 公開快照的 WSL guest available 為 392,736,608,256 bytes，C 實體
free 24,514,011,136、D free 65,767,956,480、E 工作卷 available
817,171,791,872 bytes；guest 回收不是 Windows VHDX 同額縮小，D 淨 free
亦受新冷庫寫入影響。13:36 排定的 gateway 載入已完成；這輪收盤後補入
FinLab 副本及 bytes 進度條，ETA 保留較慢 file tail 的正確 items 單位。
17:03 重複重啟造成短暫 502，17:04:33 恢復，17:05 公開端點已核對。
沒有重啟交易服務。相關搬移／進度 180、後續 projector／monitor 264、控制
交接 41 項通過；Chromium 桌面／手機 35 案例通過。舊控制包測試的
1-MiB 斷言已與原有 2-MiB 上限對齊，早先失敗日誌保留。

17:06 期貨仍在第 15／16 組、4,458,704／4,601,679 檔；主 worker 仍待
原安全交接。lab203 readiness、C 其餘來源及有固定綁定的舊 scratch
仍未完成，不將上述回收或分鐘心跳當作整體驗收。

## 2026-11-03 17:32 台北：封裝進度與兩種長等待修復

- 同一 bucket 會包含多個有界 pack，原 producer 在每個 pack 後提早報
  bucket 完成。已將 bucket 完成移至內迴圈結束後；面板改讀實際
  `files_hashed`／bytes，並用 `source_phase_counters_v3` 重置舊估算樣本。
  舊 worker 尚未換版時也不再因 bucket 計數而提早顯示 100%。相關 packed／
  progress 532 項及原 lifecycle 118 項通過。
- 17:20 堆疊顯示 compute spool 的唯一 D reader 卡在 native stdout
  `peek(1)`，讀取子程序自 16:40 起沒有完成第一個 byte。原 SSH deadline
  不能涵蓋阻塞的 source.seek/read。已對此唯讀供料路徑加入 120 秒無進度
  逾時，首 byte 與後續 read 都受保護；保留全部 bytes／SHA／source-generation
  gate，逾時不冒充 EOF、不自動隱藏部分輸出。原 legacy reader 其他 caller
  預設行為未改，這是 speculative spool 的明確 opt-in。
- 固定 cgroup／parent／PID start ticks，並將其 PowerShell 完整內容與
  canonical `mode=read` CAS 命令比對後，只交接原 compute-spool unit，
  不重啟主 migration、backup、PG、Temporal 或訓練服務。原 477 個 public
  scope proof 保留，17:27:24 新 spool 載入；17:32 實際推進至 478／517，
  私有 scope 256／256。新候選會繼續加入；這不是 NAS 或最終來源驗收。
  原停止記錄的 signal 結果保留，不改寫成自然成功。
- placement 曾進入無期限 sender／GC wait，錯過重新評估自己的 worker
  換版條件。已讓該獨立鎖等待每 60 秒返回原 timer，釋放全部部分取得的鎖；
  process churn 的不完整 namespace census 最多重取三次，不把失敗觀測
  當作通過。原 service 自然退出後載入，17:29 已恢復新的 worker 等待心跳。
  新 reader／sender／spool 相關 142 項通過，資料與刪除 gate 未放寬。
- 17:16:02 原通道確認固定控制包五個成員均由 paired receiver 索引回報可用。
  封套 identity 為 `bd5599a6a851b91819eafcc498e3cfa185fdfc1c5f2b947471d8dcc07268cf07`，
  只含兩份 runbook、56,414 bytes；未取得 receiver verifier 或 NAS ACK。
  該 v1 成員固定於 17:08，本報告之後新增的段落不是 v1 成員的新版本。

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

E 掛載的 boot ordering 在修改 sender 配置之前安裝。E alias 及 namespace
驗收後，原 timer 呼叫既有 `rollout_lakehouse_source_policy.py --reload-code-only`，
每次有界等待 600 秒、最多每 30 秒回報真正的鎖等待，不排在無關 GC 鎖後。
只在原 publisher 釋放完整邊界後交接，保留 PG／Temporal 與持久 workflow
鏈的正式驗收。`PublicationOwnerBusy` 可續等；其他錯誤每一固定程式版本
最多三次並保留失敗記錄。是否真正載入以原 `worker-reload.json` 的 exact
candidate／loaded-code 及 workflow 驗收判定，不能由 timer 或預定時刻推論。

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
