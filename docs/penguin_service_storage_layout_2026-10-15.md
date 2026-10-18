# penguin 服務儲存瘦身：2026-10-15

## 結論與目前進度

問題不是單純「資料太多」，而是來源、服務投影、跨版本快取、測試工作區與歷史
帳本混在同一個熱儲存生命週期。先把保存責任與執行需求分開，再回收實體副本。
penguin 保留來源與服務；完整訓練輸入和訓練 cache 由遠端按需重建。

依使用者「不是一次性清理，要改資料產生端」的要求，目前已修改既有 canonical
producer：分鐘 session 增量重用、Panel 相同陣列共用、completed forward 相同
輸出共用，以及下述 benchmark 輸入重用、盤後模型診斷按需生成與 Panel byte chunks。
不是另寫平行服務，也不是等全部重建後再每日去重。程式測試與線上
code admission 分開驗收；線上切換結果見下方。

### 產生端如何重用，而不是產生完才清理

| 區域 | 原產生端的修改／責任 | 目前邊界 |
| --- | --- | --- |
| `artifacts/cache` | 原分鐘 writer 共用不變 session；forward writer 共用相同輸出，miss 優先從原 catalog 固定 D 版本恢復；TAIFEX 曲線 writer 重用固定 base、只重寫小型 delta | cold-first 原生驗收、完整成長預留與實體餘量保護已完成；全 consumer 圖、namespace 硬容量與自動退休仍未完成 |
| `artifacts/benchmarks` | 原 benchmark 凍結入口重用完整 SHA 驗證的封閉輸入；完整 DB 仍各自可寫；完成後沿原冷恢復／退休交易處理 | 不把不同 SQLite DB 硬連結；不自動退休未完成試驗 |
| `/srv/stockagent-live` | 原 Panel writer 依 8 MiB 不變 chunks 提交小型 manifest，舊歷史塊不因新增日期重寫；原 audit 先重用有效 Panel，cold build 只讀既定模型需要的特徵 | chunk writer 已驗收；audit 投影完成工程驗證、完整原生重驗待原排程；舊 46.44 GB 不強制重建，總容量上限未完成 |
| `artifacts/live` | 原現役模型、訊號、帳本引用先作保護；推論盤後重用原 CPU cache | 歷史帳本／候選版本的封閉分段與按需恢復尚未完成，現行帳本不動 |
| 舊 `artifacts-hot` | 沿既有 catalog、精確 cold recovery、process-reference 與 retirement gate 逐項處理 | 尚未整樹退休；未知唯一檔案與跨樹 hardlink 原封保留 |

**10/16 09:06 最新續修：**已接回原 factorized projection producer，完整輸入
bits／座標相同的歷史區塊不再重新壓縮／另寫 payload；只從上一個同 owner 的
已驗證唯讀世代重用，壞 donor 回原編碼器重建，不動舊可寫檔。217,315,593 B
可寫字典先建立一份安全唯讀副本，之後跨版本共用。真實小型 ABBA 的更新
寫入 **3,071,415 → 814,158 B**，含驗證讀回的更新 wall 少 **19.3%**；初次
建置反而較慢，完整來源正規化／因果 sweep／真實大字典未包含在此量測。
138 項 CPU 回歸通過、3 項 CUDA 跳過；原生全量採用待原 owner 正常時鐘，
目前沒有回收舊版本，也不宣稱全量準備提速。
詳見[原 factorized 產生端續修](#1016-續修原-factorized-產生端先比完整輸入再共用不變區塊)。

**10/16 08:48 續修：**修正原完整 audit 的 include＋exclude 解析，避免先
載入全部 143 個公開特徵，再丟掉模型未選取的欄位。原有效 cache 優先重用，
只有 miss 才下推最終 8 欄投影；17 個交易規則欄、既定 24 欄模型面板及所有
來源稽核範圍不減少。同一 65,536 列真實小型 fixture 的 fresh-process ABBA，
讀取階段 wall **0.760 → 0.321 秒**，程序 peak RSS **707.2 → 350.7 MiB**；
保留值與全部規則逐位元相同，205 項相關回歸通過。原生 08:45 後的排程因
開盤保護正確延期，完整 audit 重驗待原 09:10 排程，**不是已實測完整 55.29 GiB
峰值的降幅**。7 個現役 owners／132-file 模型實作不變，未重啟或刪資料。
詳見[原稽核產生端續修](#1016-續修原完整稽核先重用再做必要欄位投影)。

**10/16 08:23 續修：**原 `cached_forward` 已接上固定冷庫優先恢復，非另建服務。
五模式完整獨立推論逐位元相同；正式五模式重讀未初始化 CUDA，另以非現役的真實
歷史結果驗證原入口從 D 恢復，不搬動現役快取。132-file 新程式已由 canonical
Discord owner 載入，五模式 ACK 與帳本 prefix 核對；盤前暖機仍為原時鐘控制，
本次**不宣稱 GPU 冷／暖妥或開盤延遲已驗收**。758 項共享回歸通過。
詳見[原入口恢復與採用的完整邊界](#1016-續修原產生端先復用固定結果再允許新計算)。

**08:28 最新來源回讀：**4 模式 artifact 仍可直接重用；factorized 模式因
`completed_source_generation_changed` 正確失效，config／實作 SHA 仍相同。
沒有據此宣稱資料全新，也未重標舊 summary。原 maintenance 自 08:07 受保護，
minute retry owner 正常，既定完整維護下一輪 13:40；不為這次驗收繞過窗口。

先前原每日維護已能從 canonical 市場／模型選擇及最新訊號自動解析
五個現役模型的 forward pins；控制檔、pointer 或 summary 有缺失／競態時保留整份
清冊，不另建 pin registry，也沒有 GPU／provider 工作。大型 completed benchmark
已接回原手動冷恢復／退休流程：小型原始驗收 JSON 留在本機，mutable DB 不能
以 hardlink 共用。最大案例於 03:46 完成原固定退休交易，**實際回收原工作副本
47.08 GB**；七份原始驗收 JSON 留在本機，完整工作庫可從固定 D 身分恢復。
另清除本次新增的 10.15 GB 編碼暫存，不與原工作副本節省量重複相加；
見下方最新續修。這筆 proof 是單一 D volume，不代表 NAS 離機備份已驗收。

04:10 再完成原 Bybit／OKX 下載器日期欄位重用：1,974 份完整實際 tail 的四輪
全值 ABBA，合併階段 **130.646 → 95.702 秒**，原定時 owner 已實際執行。
同時修復原 daily dedup 的清單 JSON 故障與逐檔重掃 marker；04:11 原服務完整
成功，04:12 獨立驗證全部 863 個替換／1,127 個邏輯路徑，未刪路徑或帳本。
詳見下方續修；不是 HTTP／完整 collector 的同倍數提速或全目標完成。

04:35 再將快取路徑解析從推論引擎抽回原輕量 market config：完整 516 筆依賴
逐欄／順序不變，fresh-process ABBA 平均 **6.535 → 1.436 秒**、程序峰值 RSS
平均 **764.32 → 180.58 MB**，不再因儲存查詢載入 Torch。五模式完整輸出、正式
CPU cache 與原生冷 owner 已驗收；另定位到永豐 hourly storage scan 的 547.8 萬
檔案／605.6 秒／13.5G cgroup peak，後續修復如下，不用目錄 mtime 假裝內容未變。
詳見[依賴解析的邊界與證據](#1016-0435-續修儲存依賴查詢不載入推論引擎)。

04:56 已將原永豐連續期貨下載器與清點服務接上同一 mutation epoch。完整
192 萬檔案的固定來源／固定時鐘 ABBA，fresh-process 平均 **29.243 → 1.286 秒**；
773 合約重用僅需 1.18 MB 清冊，不複製 Tick。原 systemd owner 仍完整處理十類
資料，04:52 成功使用新版；197 項相關測試通過。其他資料群組仍沿原掃描，
全 consumer／硬容量與封閉歷史退休仍未完成，見[本輪證據](#1016-0456-續修原下載器版本驅動的儲存清冊)。

05:20 再修正原 TAIFEX 曲線投影產生器：小型增量不再重寫全部歷史 Parquet。
完整七區間、真實 tail、正式 thread／allocator 的八輪 ABBA，寫入
**30,616,012 → 16,939 B（-99.94%）**，更新 wall 中位數 **13.700 → 9.782 秒**。
原唯讀 owner 已採用，現行七區間完整公開欄位對舊 reader 一致；帳本前綴／行情與
交易 owners 保留。完整 prefix SHA 尚未省略；自然新來源增量另待下一交易時段，
見[本輪架構與限制](#1016-0520-續修taifex-曲線投影重用固定歷史與小型增量)。

05:41 再補齊原 consumer gate：主要／unconstrained forward 都列 pin，隱含
forward-cache root 也列服務依賴；分鐘回放、估值與盤點共用同一份輕量路徑宣告。
517 筆完整依賴的八輪 ABBA，wall **0.9566 → 0.9042 秒**、peak RSS 平均少
**9.66 MB**；這是小幅改善，canonical config 仍用 NumPy／Polars。原每日 owner
完成、四個現役 owners 不變、5 keys 的 126 份 canonical code identity 保留；
517 項共享回歸通過。詳見[本輪引用與原生驗收](#1016-續修現役快取引用補齊與共用分鐘路徑)。

05:52 再處理原每日 generic 維護的重複 metadata 成本：完成標記掃描不再為每個
無關目錄建立／解析 Path，也不組裝未使用的檔名清單；仍逐目錄掃描與新鮮讀取
巢狀 marker，保留 symlink、完整 SHA 與原 eligibility／apply。完整 5,247 檔的
四輪 fresh ABBA 全群組／metadata／計數相同，wall **25.144 → 20.443 秒**、
CPU **24.174 → 17.733 秒**；155 項相關回歸通過。不是新增大型常駐索引或減少
內容驗證。05:53 原每日三階段 owner 實際成功執行，四個現役 owner 與五個模型
keys 不變，詳見[本輪量測與原 owner 驗收](#1016-續修通用維護只解析需要的路徑)。

06:07 再修復容量管理共用的引用安全邊界：`/proc` 權限／I/O 錯誤不再等同
「零引用」，正常程序退出仍不阻塞。原 daily owner 已成功載入，189 commit／
5 current keys／13.162 GB 原配置保留；本輪沒有回收資料或重跑 GPU。
333 項共享、77 項 transport 與 87 項 focused 回歸通過（focused 有重疊）。
這是硬容量准入前的必要修復，不宣稱已完成容量上限，見[本輪證據與後續設計](#1016-續修引用掃描不完整時保留資料)。

06:29 完成容量預留核心及完整 CPU 提交流程原型：核心檔案鎖追蹤進行中工作，
未知資料計入但保留；有效結果不必重新准入。暫時控制記錄只需原子可見，移除其
不必要的 fsync，正式 payload／receipt 仍耐久提交。260 項相關回歸通過；
尚未接正式 writer／安裝容量政策，詳見[量測及部署邊界](#1016-續修容量准入核心與完整-cpu-提交實測)。

10/16 又完成原 canonical packed publisher 的**指定原檔直接發布**：可只封存
同一 key 的 payload／receipt，不必先複製兩檔到另一棵工作樹，也不遍歷其他
模型世代。原 catalog／來源鎖、完整 SHA 與固定版本恢復不變；這是共用發布
能力，尚未註冊正式 forward 冷存政策或切換服務，見[完整流程驗收與邊界](#1016-續修原檔直接發布避免封存前的工作副本)。

10/16 再完成**固定 pin 的直接還原核心**：共用原 fetch 的逐檔複製／完整驗證，
直接寫入使用端暫存，不先建立第二棵 managed tree。五份現行結果的完整 CPU
交易，還原 payload 寫入 **4,937,698 → 2,468,849 B**；CPU／wall 存在波動，
不宣稱穩定提速。這仍是共用能力與原型驗證，正式 cold-first writer／容量政策
尚未接入，詳見[固定 pin、直接還原與驗收邊界](#1016-續修固定-pin-直接還原不再複製第二棵熱工作樹)。

10/16 07:44 已將完整結果接入**既有 catalog 與正式 D 冷庫**：5 current keys 加
2 份真實歷史重複結果，7 個固定版本、14 原檔及完整 CPU 輸出均獨立還原一致；
五個現行版本重複發布新增物件為 **0**。兩個歷史 key 實際共用 D 的同一個
219,537 B payload，未另建 CAS／排程；所有原檔、126 份模型 code 與四個保護
owners 保留。另避免每個結果重整不相關訓練配方，修正部署中 shell 變長造成
提交成功卻誤報失敗的問題。**原正式 inference 尚未 cold-first，容量政策也未
啟用**，不因此退休熱副本；見[正式登錄與完整恢復](#1016-續修完整結果逐-key-正式登錄與冷庫重用)。

前一里程碑：completed-forward 成功使用取樣與 inode-aware working-set 清冊
已接原 producer／每日維護 owner，不新增服務。184 個提交 key 的容量規劃 ABBA
平均 **5.923 → 1.636 秒（-72.38%）**，不雜湊大型 payload；460 項相關回歸、
五模式原完整輸出／訊號及新原生冷 owner 驗收通過。每 key 每分鐘最多取樣一次，
損壞／失敗 usage 不觸發 GPU 重算。**這不是已完成硬容量上限或全 consumer pins；
沒有 usage／恢復證據的舊 key 保留，不用檔齡推測可刪除。** 詳見下方最新續修。

前一里程碑：Panel 不再因新增日期就重寫整份歷史；8 MiB 不變 byte chunks
沿原 writer／manifest 原子提交，Linux reader 保留連續 NumPy mmap ABI。完整真實
panel 的單日 append 寫入量 **1,257.33 → 15.86 MB（-98.74%）**；ABBA 中含載入與
全量讀回的 append 平均 **10.389 → 8.324 秒**，冷建置則略慢。426 項相關回歸、
全陣列／軸與五模式原完整模型／訊號比較通過；新 Discord 冷 owner 已接受。
既有 46.44 GB 並未強制遷移或刪除；下次真正需要重建才使用新格式。
這不是全五樹或全服務總目標完成；總容量／leases、mutable 測試 DB 與歷史退休仍待續。

前一里程碑已避免未消費的 candle／basis 診斷在模型內先組裝再丟棄。年度模式
有界模型峰值顯存 **1.856 → 0.188 GB**，652 項當期回歸及原冷 owner 驗收見下節；
不能與本輪重疊測試數相加，也不代表 factorized 模式或整機占用等比例下降。

10/16 再接續修正兩個既有 benchmark 的**來源凍結入口**，由完整 SHA 驗證後共用
既存封閉輸入，避免每次建立完整測試資料副本。全量 172 檔、908,718,264 bytes
的相同來源第二次擷取，新增來源 payload 配置量為 **0 bytes**；仍保留每次試驗的
邏輯路徑、程式與驗收證據。這不是宣稱大型 SQLite variants 已退休或全五樹完成。

01:06 又完成既有 completed-forward 副本共用：133 個已提交 key 的配置量從
34.07 GB 降至 **11.79 GB**，減少 **22.28 GB**；保留 133 個原路徑及所有收據。
另八個缺收據 payload 不在驗收範圍，原封保留。日常去重改為只讀真正的重複
候選，避免已無重複時仍為維護反覆雜湊數十 GB；完整稽核與原生讀取驗證不變。

既有分鐘副本已完成專用壓實與獨立全 SHA 回讀，該目錄實體配置由 181.55 GB
降至 **101.30 GB**（減少 **80.25 GB**）；所有邏輯路徑保留，未刪來源／模型／帳本
或冷庫原件，未對服務資料使用 generic dedup。**其餘生命週期階段仍是實作目標，
不宣稱五個根目錄皆已遷移。** 前一輪 63 GB 舊分鐘 view 恢復交易獨立執行，
不混入本機制的數字；WSL 配置量降低也不代表 Windows VHDX 已縮小。

## 量測方法與分布

單一 `du -x -B1 --max-depth=2` 程序，依序掃 cache、benchmarks、live、`/srv/live`、
舊 hot，對共同 inode 只計一次。2026-10-15 21:51–21:52（台北）完成，exit 0：
`service-storage-breakdown-20261015T135139-591f8f45`。
數字使用十進位 GB；掃描中的服務仍可寫入，因此是區間觀察值。

| 分類 | 本次計入配置量 | 已查明的大項／解釋 |
| --- | ---: | --- |
| `artifacts/cache` | 216.57 GB | minute physical generations 181.55 GB、completed inference 27.70 GB |
| `artifacts/benchmarks` | 186.10 GB | `running_services_20261013` 174.78 GB；包含完整 private fixtures |
| `artifacts/live` | 75.43 GB | 目前台股帳本 9.18 GB、期貨模擬 6.80 GB，另有多套歷史候選／回放 |
| `/srv/stockagent-live` | 155.44 GB | TW public 74.44 GB，其中 stocks panel cache 46.44 GB；Bybit 22.97 GB；另有研究版本 |
| `/srv/stockagent-artifacts-hot` | 49.77 GB | 在前四項計數之後才計入的量；單獨掃是 92.48 GB，有跨樹 hardlink |
| 本次五樹聯合配置量 | **683.32 GB** | 不是全部可回收量，也不是整個 WSL 用量 |

49.77 GB 不代表舊 hot 完全獨佔這些 inode：五樹以外的目錄也可能有 hardlink。
因此不能把原本五個獨立 `du` 的數字相加，也不能把 unlink 名稱數當成釋出量。

### 不是每個 generation 都是無用副本

- Minute cache 有 15 個 `physical-<digest>` 目錄，其中 12 個是目前 v12 ABI。
  原架構每次日期／來源／股票軸的全域 digest 改變，會生成約 11 GB 的整套實體檔。
  完整 SHA 稽核已找到 **92.16 GB** 的位元組重複候選；有服務引用，還不是 apply 許可。
- TW public 的 panel cache 是 **8 個不同 backend contract、8 個 generation**，不是
  8 個可以隨便刪的同版日更副本。既有 writer 已回收同 backend 的舊來源 generation；
  真正需要改善的是跨 backend 的共用內容、服務所需範圍與總容量管理。
- Completed inference 有 104 個 `.forward`（27.68 GB logical），而 receipt 有 96 個。
  需要逐 key 配對、鎖與使用檢查，不能直接宣稱相差的 8 個都可刪。
  現行 1 GiB 是**單一 payload** 上限，沒有全目錄 byte budget／LRU。
- 最大 OpenBB fixture 是 **47.08 GB**，包含兩個約 23.54 GB 的 SQLite index variants。
  兩份 DB 不是相同位元組，且測試會寫入，不能 hardlink 合併。

## 目標架構：保存原件，不常駐所有工作副本

| 責任 | 保存位置與生命週期 | 不應再做的事 |
| --- | --- | --- |
| 封閉來源、唯一研究結果、來源／恢復證明 | D 既有 immutable authority；保留原值、單位、時間及權限 | 在 C 再常駐一份完整冷庫；按名稱或新舊刪唯一歷史 |
| 持續採集 | WSL 保留尚在寫入的 tail、journal、locks；封閉 partition 驗證入冷庫 | 對 mutable producer 直接 hardlink；寫入未封閉就視為冷恢復完成 |
| 服務資料 | WSL 留部署模型、權威帳本與符合模型 ABI 的最小投影 | 為每個研究 release 常駐完整多年訓練 panel |
| 大型 cache | 不可變內容共用＋小型 revision manifest；有總 byte budget、pin／lease 與回收 | 每次全域來源 digest 改變就再實體複製所有歷史 session |
| 測試／benchmark | 程式、設定、hash、驗收結果長存；大型 fixture 為受限工作區 | 把每次完整 DB／輸入副本永久留在 SSD；縮減測試內容冒充提速 |
| 遠端訓練 | 從指定來源版本重建訓練 view／cache；完成成果依既有回傳契約保存 | 要求 penguin 長存每種訓練衍生物 |

沿用既有 catalog、lifecycle、D guard 與 retirement 入口，不另建第二個同步器。
revision manifest 保存精確來源身分，不要求每個 revision 再複製整棵資料樹。
`publish:false`／授權限制仍有效；轉入 D 不等於允許對外發布。

## 實作順序與驗收邊界

### 1. 分離 benchmark 證據與 bulk 工作區

優先檢查 `running_services_20261013` 中已完成的私有 fixture。保留原始碼、參數、
完整資料身分、全流程 timing／parity 收據；唯一且要保存的 bulk 檔先封存到 D。
相同來源可以共用經完整 SHA 驗證的**獨立不可變基底**，不能共用仍在寫的 producer。
會寫的 SQLite variants 保持獨立，序列執行試驗以降低尖峰，不能用 hardlink 冒充 CoW。

回收前仍需目前程序／設定、pin／lease、完整冷恢復與 fresh inventory 檢查；
本輪 shared service consumer 對 benchmarks 回傳 0 引用，不代表其他 benchmark process
已停止。未完成的其他工程工作區不得動。建立新大 fixture 前需預估基底、variants、
全量輸出與 reserve 的聯合容量；既有 reserve 檢查不是總量配額。

### 2. Minute cache 改成 session 級共用，而不是整代複製

責任入口：`stockagent/data/tw_day_trade_carry_source.py`。將未變更歷史 session 的
不可變 payload 依完整內容身分共用，保留 generation manifest 的日期／股票軸、
source digest、每檔 SHA 及 READY；修正歷史 session 或新增股票造成內容不同時，
只產生真正不同的 payload。不能僅以日期或相同檔名重用。

需在 canonical writer locks 下提交；共用物件唯讀、writer 只 atomic replace。
舊 generation 的 pin／lease、讀者與復原路徑必須可查；最後一個引用消失才能回收
熱物件，不能留下無 owner 的永久 CAS。92.16 GB 是已量測候選，不是保證節省量；
不能再加上「刪舊 generation」的估計，兩者有重疊。

### 3. Panel／completed-forward 改成有上限的服務投影

責任入口：`stockagent/data/panel_cache.py`、`stockagent/live/completed_inference_cache.py`。
按**目前部署的每個獨立模式**解析必要特徵、完整 lookback、因果時間與當期服務日；
保留 full-size 重建路徑，不任意砍日數、股票、auxiliary tensors 或降低數值精度。
對相同內容的陣列提供不可變共用，避免不同 backend 複製相同價格／mask。

Completed forward 增加整體 byte budget 與實際使用 lease；目前模式／當期 session
固定保護，舊 code/config identity 在無讀者及可重建證明後才 LRU 回收。
`.forward` 和 receipt 以同一 key 交易管理；不刪開啟的 mmap 或正在 singleflight 的檔。
不能把一般 cache 納入既有七日 managed GC 就宣稱自動受保護。

### 4. 帳本按封閉時段分段，歷史回放不常駐完整服務副本

責任入口沿用 live simulation、replay／promotion 與 dashboard indexes。
目前台股帳本中 `signals.jsonl` 約 5.81 GB；它是事件歷史，不是可按重複行刪的 cache。
封閉日以不可變 segment 保存，熱讀模型／索引提供查詢，當日 tail 繼續 append。
歷史 candidate／addition／replacement 保留其獨立模式帳戶與證據，冷恢復通過後
才退休舊 bulk；熱層保留已驗收部署與明確 rollback。不能覆寫或合併不同帳戶帳本。

### 5. 退休舊 hot bridge

`/srv/stockagent-artifacts-hot` 已不是目標服務／傳輸架構，但混有唯一版本與跨樹連結。
逐個精確子樹沿用 legacy/manual preservation contract，獨立恢復其實際內容後才
移除名稱；保持外部 hardlinks，記錄最後連結與配置量。不要整根 `rm -rf`。

### 線上切換的共同驗收

先在私有隔離樣本／完整相同輸入上驗證 writer、崩潰恢復、併發讀取與 GC race；
比較完整輸出、帳本 prefix、source/feature/financial ABI 與 whole-workflow latency。
實測冷啟動、熱讀、尖峰容量及恢復時間，再選 byte budget；不先硬塞一個不足的上限。
修改 data/model/live 身分會使 resident inference 失效，必須依 canonical qualified
reload 流程切換，不能在線改檔後把舊程序當新版本。保留可回滾版本直到驗收通過。

## 本輪已實作的共用修正

### 資料產生端：未改變的內容不再重寫

責任入口仍為原本 `tw_day_trade_carry_source.py`、`panel_cache.py` 和
`completed_inference_cache.py`；不新增 daemon、第二個 CAS 或同步索引。
`immutable_cache_files.py` 是只處理完整 SHA／唯讀檔／原子連結的底層共用模組；
`physical_cache_reuse.py` 擁有分鐘 manifest、writer lock 與 session 依賴契約，
維護工具重用它，而不是讓正式 producer 依賴清理工具。

| 產生端 | 現在直接重用的單位 | 仍須重新產生的情況 |
| --- | --- | --- |
| 分鐘 physical session | 原始 partition SHA、股票有序軸、當日價格／量／漲跌停、前一可觀測 mark、執行 mask、政策完全相同的 session | 新增日期或任一實際依賴改變；歷史修正不會命中舊輸入 |
| Panel backend | 完整相同的小 NPY／未啟用格式之陣列；已啟用的大型歷史共用完整 SHA 相同的 byte chunks | 真正不同的塊及新增尾塊；滑動 live tail 保留 NPY，詳見續修節 |
| Completed inference | 同一輸出 profile、已各自計算並驗收的新 payload，完整 bytes 與另一個 committed key 相同 | 不同輸入仍計算；不同輸出仍寫入；服務訊號另有明確投影／診斷 policy，不改數值精度 |

分鐘輸入 key 包含來源 partition SHA，且每次命中前**仍完整驗原始 partition SHA**。
同一 session 的 gap／valuation masks、mode counts 也綁在 manifest 中，命中時恢復
原診斷，再照原邏輯計算全域未知區間與 actions。全域 release 身分改變但歷史
session 依賴未改時，不再解析 Parquet、計算 path 或重複序列化／寫入該 session。
舊 manifest 沒有 session key 時，首次仍計算，再在記憶體序列化後按完整輸出 SHA
重用既有不可變檔；不為補欄位而強迫既有有效快取重建。

Panel 在開啟新輸出 memmap 前先比對既有 committed variants，完整驗證實際 array
fingerprint、檔案 SHA 與穩定身分；不是只信 metadata 的 hash。保留各 backend
manifest，沿用原 writer lock／generation 退役契約。共享陣列被其 donor generation
退休後，新 generation 自己持有連結，不會斷檔。

Forward 的 input key／receipt／完整輸出與 `MAP_PRIVATE` 讀取不變；不同 key 只共用
完整 bytes 相同的唯讀 payload，仍各自驗 source/config/code 並保留獨立 receipt。
內容查找後、receipt commit 前再驗一次輸入，避免慢速序列化／SHA 期間的變更。
不把另一個 key 的輸入當權威，不因此略過應有 model call，也不在線修改舊可寫
donor 權限。本輪唯讀清點為 122 receipts、32.78 GB logical payload、53 種 hash，
其中 **20.99 GB 重複候選**；這是完整 SHA 待驗候選，不是已回收量。

新 payload 唯讀，writer 只 atomic replace；忙碌的 donor 使用 nonblocking 原鎖，
遇到 busy／損壞／可寫／未知格式就按原路徑計算，不篡改來源或留下 ready 假象。
輸入 key 索引只活在一次 build 的記憶體，payload 由原 generation／key 名稱擁有。
沒有額外永久 payload pool，也就沒有新增一層無 owner 的 GC 責任。

分鐘 manifest：`storage_contract=physical_minute_incremental_build_v1`、
`session_builds`、`storage_build`；Panel meta：`storage_build.contract=panel_array_reuse_v1`
或 `panel_time_chunk_reuse_v2`（依是否實際寫入 chunks）；
Forward 回傳收據：`storage_build.contract=completed_forward_payload_reuse_v1`。
它們記錄 reused/written 次數與 avoided/written bytes，不改 source／feature／financial
ABI 或既有必要檔案的位元組契約。Native helpers 已加入 live implementation admission，
必須讓新 owner 經 qualified reload 驗收，不能把舊程序算作已使用新程式。

### 舊 v12 session 副本的回填共用

既有副本另外沿用 `physical_cache_compaction.py` 與
`scripts/deduplicate_artifacts.py --service-cache-policy PATH`。這是回填／防漏工具，
不是新 writer 增量重用的必要前置排程。

- 只支援完整 commit 的 v12、已知 uncompressed session encoding 與固定 session
  檔名。原 manifest／READY 每個位元組不改，unique payload、gaps/actions 陣列不動。
- 全量 SHA 稽核不持有 writer locks；前後 inventory 必須相同。套用時逐組取得原
  per-generation nonblocking locks，再驗完整 SHA、原 metadata／alias 集合。
  忙碌中的 generation／group 保留，不長時間獨占全部服務快取的寫入鎖。
- 相同 payload 改成 generation 之間的唯讀 hardlinks，不建立第二個 CAS。
  清掉 write bits 但不增加原本的 read permissions；inode／ctime／衍生檔 mtime
  可改變，原值與 source hashes 不變。canonical reader 自動拒絕舊 stat proof，
  重新做 SHA；canonical atomic-replace writer 仍能獨立重建任一 generation。
- 外部 hardlink、ACL、未知格式、symlink、損壞 proof、正在使用的 FD／mmap 都不能
  當成可處理項。故障留下相同 bytes 的舊或新名稱，保留失敗／中斷收據。
- 完成後核對所有原路徑／原 bytes 身分、manifest／READY 和退休 inode 引用；
  用 distinct inode 的 `st_blocks` 計數。後續版本以 bounded FD pin 持有被替換 inode
  直到 post-unlink 程序檢查完成，避免 inode 號碼立即被其他服務重用造成誤判。

正式套用完成於 23:21：12 個完整 v12 generations、11,234 個路徑改為內容共用，
對應 session 配置 137,976,934,400 → 57,725,960,192 bytes。23:22 獨立回讀重驗
8,077 個 session inode、57,688,170,578 bytes，剩餘 duplicate groups=0；原 manifest／
READY 通過。另三個 incomplete generations 原封保留，不是可刪的舊 ABI。

原 apply 收據保守的 `retired_allocated_bytes=0` 是因為退休 inode 號碼被 Finmind
SQLite SHM 重用，post-unlink 檢查命中非 cache 檔案；不回寫該歷史收據假造成功。
實測目錄 `du` 與獨立 inode inventory 都確認 **80,250,974,208 bytes 的 cache namespace
配置降低**。不能把這個數字直接等同 Windows 已收回的 VHDX 空間。

23:38 重量測 `artifacts/cache` 共 142.77 GB；分鐘目錄 101.30 GB。Forward 另增至
34.14 GB，因此全 cache 的淨差與單一分鐘目錄的差不同，不能把增長藏在節省量中。
本次 native producer 改善不等於 forward 總量配額／LRU、Panel chunked append、
帳本分段、benchmark bulk 退役、七日版本回收或 D GC 也已完成。

本機啟用須另外登錄 `/etc/stockagent/service-cache-compaction.json`；範本為
`configs/data_sync/service_cache_compaction.json`。沿用每日 03:30、最多延遲 30 分鐘的
`stockagent-artifact-dedup.timer` 與同一 wrapper lock，不新增 timer。個別 cache
失敗仍讓原 generic completed-artifact 任務執行，但整體 exit 保留 cache 失敗結果。
操作／plan fingerprint 指令見 [README](../README.md#服務分鐘快取保留路徑共用相同內容)。
本次已完成本機設定登錄，root:root／0600、與 repo policy bytes 相同，native policy
解析通過；既有 timer 為 active/waiting，下次 2026-10-16 03:30:36（台北）。沒有
為了測排程在模型暖機期間再啟動一次大型全量 SHA。

初輪 storage/consumer/boot tests：**88 passed**。加入 native minute／Panel producer
後，13 個相關測試檔 **284 passed，35.12 秒**；加入 Forward 內容共用與 commit 前
再驗後，**290 passed，31.08 秒**。另一次核心／reload focused **175 passed，33.16 秒**。
涵蓋原值／gap counts 保留、日更只處理新增 session、歷史依賴變更失效、原始 SHA
拒絕、backend 陣列共用、donor 退役、並行 busy fallback、FD/mmap、故障交易與
policy 範圍拒絕。Ruff 和 diff check 通過；沒有為提速縮減模型輸入或跳過驗證。

#### 服務觀察邊界

23:05:44（台北）既有模擬交易 owner 發生 90 秒 watchdog timeout，systemd 自動
重啟，23:05:55 已重新啟動；壓實階段沒有下達服務 restart。該時刻壓實 worker 尚未
開始 session sharing（attempt 開始於 23:05:49，仍須完整 SHA），因此不能把它
當成 hardlink 切換造成的證據，也不能據此排除背景 I/O 等其他因素。
23:09 新 owner 持續送 watchdog、Discord ready 且已讀到新 engine revision。
既有 canonical isolated-restart audit 驗證 inventory 可精確恢復、目前無持倉；
其帳本 prefix 基線建立於 23:09，不能用它宣稱涵蓋先前 watchdog 的完整前後交易。
測試、資料保留、目前健康和全天候穩定性仍是不同驗收邊界。

#### Native producer 正式接入：accepted

23:44:27–23:44:50 使用既有 `reload_tw_day_trade_services_if_flat.py --discord-only --apply`
完成 qualified reload。只重載 Discord owner，execution engine／行情 receiver／網站
等受保護 owner 的 PID 與 InvocationID 皆未變。新的同一 run 五個模式暖機
**5/5 ready、resident_ready=true，10.491 秒**；Gateway connected，原 signal IDs
保持不變，engine ACK matched、revision lag=0。Code admission digest 為
`c77840f764f2f71c64f43893fea928129c456686f51fb950149114d7626105b3`。

既有 helper 只延長 10 分鐘 no-publication deployment warmup，截止 23:54:25；
沒有增加排程模式、改策略、下真單或讓非交易時間永久占用 GPU。分鐘／Panel
既有有效 manifest 不強迫重建；新版增量流程在下一次自然重建時生效，其日更與
失效語義由上述實際 producer 測試驗證，不能把這次暖機說成全歷史增量已跑完。

切換驗收及 23:45:40 獨立 canonical prefix readback 都通過 events／orders／fills
原始前綴 SHA，檔案大小亦未變；沒有修改正式帳本。已知背景歷史回補仍保留一個
factorized 模式的先前 failed 狀態（23:44:00，早於切換），不因 Gateway／暖機
通過而宣稱所有歷史補齊或整站零錯誤。

### 零 request 的回放來源依賴

`artifact_consumers.py` 先前把任何非空 replay receipt 的空 `source_files` 都視為錯誤，
導致現行台股回放中三個零持倉／零待成交股票日阻塞所有回收依賴檢查。
已對照 canonical producer，僅接受已知歷史分鐘 counterfactual contract、明確整數零
request/resolution/ignored counters、沒有 missing/carried/source counts，且沒有歷史
Bid/Ask claim 的 no-work scope。它只表示該日沒有分鐘輸入，不放行其他依賴。
未知 contract、計數非零／缺失／型別錯誤及其他 malformed pins 仍 fail closed；
沒有改寫 ledger 或 receipt。

Focused consumer tests：**36 passed**。共享 consumer／artifact／legacy／local-offline
retirement 回歸：**169 passed，390.58 秒**（exit 0）。最新 live dependency readback
成功，五樹的 shared consumer 引用數依序為 cache 10、benchmarks 0、live 18、
`/srv/live` 44、舊 hot 0；這不替代其他 collector／benchmark 的依賴與程序盤點。
這項 source-consumer 修正本身不是大型 cache 去重；與 session 共用機制分別驗收，
兩者都不代表上述五階段已完成。

### 10/16：benchmark 從產生端重用完整既存來源

原本 `benchmark_tw_public_feature_assembly.py::capture` 與
`benchmark_openbb_metadata_audit.py::prepare` 每次建立工作區都 `copyfile` 全部來源。
已讓兩個原入口共用 `ImmutableFixtureInputs`，並沿用既有
`immutable_cache_files.link_immutable_payload` 完整 SHA／唯讀檔／原子連結實作。
不新增 payload pool、CAS、daemon、同步器或另一套退休規則。

機制與邊界：

- 單次 capture 建立有界記憶體索引，只查同層已封閉的實驗輸入收據；既有 TW
  full-builder fixture 也可直接供應唯讀輸入，不為新增索引而複製或改寫舊收據。
  至多掃 1,024 個 donor roots，超限明記 `donor_scan_truncated` 並按原路徑複製，
  不因去重搜尋上限而少抓來源或少做測試。
- 每個實際來源仍完整驗 SHA。選中的 donor 再驗完整 bytes、size、唯讀與安全
  inode 身分；新工作區有自己的 hardlink，不依賴 donor 名稱永久存在。
  **永不直接 hardlink 可變下載來源。**
- 真正改變的來源仍獨立複製、驗證及 fsync；SQLite shadow／索引變體與試驗
  輸出不納入共用。不能把會寫的 47.08 GB SQLite variants 合成同一 inode。
- 新 owner 有原子 committed-input receipt 與 capture lock；busy／損壞／可寫／
  未完成／不明 donor 為 miss。ENOSPC／EIO 等實際發布失敗仍拋錯，不能假 ready。
  source／member／symbol-name／原 builder 的既有完整驗證不刪減。
- 正式來源 writer 忙碌時，在建立新工作區前拒絕，避免每次 deferred 都新增空
  generation。正式抓取鎖沒有被移除、忽略或強制解開。

完整實測（00:04–00:05 台北，來源為當次 coherent snapshot，不宣稱此後永遠最新）：

| 擷取範圍 | 完整來源 | 共用／新寫 payload | 完整 capture wall |
| --- | ---: | ---: | ---: |
| 既有封閉全量 fixture | 172 檔／908,070,939 B | 共用全部；新寫 0 B | 非吞吐對照，僅驗證舊資料可重用 |
| 當次正式來源對舊 fixture | 172 檔／908,718,264 B | 共用 90 檔／197,622,336 B；新寫 82 檔／711,095,928 B | 8.79 s |
| 同一正式來源再次擷取 | 同一 172 檔／908,718,264 B | 共用全部；新寫 0 B | 5.46 s |

兩次工作量的寫入數不同，不能當同條件 A/B 加速比。第二次仍有程式、JSON、lock
等小型配置，**0 B 僅指新增來源 payload，不指整個工作區 0 B**。
不同內容的 82 份 Parquet 仍保留新版本；本輪粒度是完整輸入檔，尚未實作表內
row-group／日分區增量，不能說每個表都只寫變更列。

獨立 readback 不依賴 producer 的 reused counter：核對三個完整 cohort 的精確
集合、原始 bytes SHA、唯讀與前後 inode 身分，完整重讀 254 個不同 inode／
1,619,166,867 bytes。確認 repeated cohort 的 172 個來源都與前次同 inode，
新增來源配置量為 0 B，來源前後未改，沒有 provider／模型／GPU 呼叫。

共享回歸 **153 passed，11.70 秒**，涵蓋兩個真正 capture caller、可變原件隔離、
修訂只影響對應檔、donor 退休、busy fallback、SHA／路徑／FIFO／空間失敗、
崩潰未提交與來源 writer 拒絕，並回歸分鐘、Panel、Forward 的現行 producer。
Ruff 與 diff check 通過。Discord PID／InvocationID 未變；本輪沒有 reload 服務、
修改帳本或刪除任何來源／舊 fixture。

保留首次計時 wrapper 的 exit 127，以及之後 native writer 忙碌時的正確拒絕，
沒有覆寫為成功；後續成功擷取與 regression 分別驗證正確 launcher 和 read guard。
benchmark bulk 冷封存／退休、服務 cache 總量 budget、Panel 分塊 append、歷史帳本
分段及舊 hot 精確退休仍沿原計畫接續，尚未實作完成。

### 10/16：完整建置驗證，不把候選技術當成已接受的優化

沿用同一份已凍結來源，沒有再複製來源、縮小 symbol／年份或省略輸出。新增
`scripts/verify_tw_public_feature_trials.py`，獨立核對完整來源集合（包括 supplemental）、
原 builder、symbol universe、同一 harness／CPU pool、已完成收據及全量輸出。
Parquet 編碼不同時，才退到有界 Arrow batches 的逐欄全值位元／NULL mask 證明；
不是抽樣、浮點容差或只比 summary。簽章、signed zero、NaN payload 與列順序
不一致均拒絕；來源、輸出及收據在比對後再完整重驗。

本次完整範圍為 **172 個來源檔、908,070,939 bytes、9,635,384 列、143 features**。
兩輪 original、兩輪 streaming、兩輪 eager，六份完整 Parquet 的實際 bytes 都相同：
`7309ad22b8464b43b716dbc9f9583c7e953fabe03685440b8cab1737c49cd534`。
獨立 receipt 為 `full-feature-all-candidates-proof-20261016.json`，狀態
`accepted_exact_output`，**只接受完整輸出一致，不表示候選已上線或正式模型已驗收**。

| 候選 | 完整 wall，兩次平均 | CPU 秒，兩次平均 | peak RSS，兩次平均 | 採用決策 |
| --- | ---: | ---: | ---: | --- |
| 原始 eager | 203.847 s | 1,341.444 s | 50.56 GB | 保留正式路徑 |
| streaming projection | 208.896 s | 1,509.090 s | 46.72 GB | wall 多 2.48%、CPU 多 12.50%，不設為預設 |
| eager projection | 184.246 s | 1,308.272 s | 51.47 GB | 非平衡時序且主機負載不同，尚不能宣稱穩定加速，不部署 |

前四輪為 B-A-A-B，之後才做 E-E；不是六輪平衡的三向比較，也不是獨占主機。
eager stock merge 的波動和原始路徑重疊，未證明省去足夠的實際重複欄位運算。
上述較短 wall 不寫成 10% 正式加速；正式 data/model 身分未變，不為未接受的候選
reload GPU owner。當次工程 unit 限定 16 threads、58 GiB memory、swap 0、低
CPU／IO weight；這不能推論整台 WSL 沒有其他 swap 或資源競爭。

本輪 producer／全值 oracle 共享回歸 **154 passed，19.45 秒**。來源、模型與帳本
沒有刪除；大型試驗輸出仍在私有 benchmark 工作區，尚未假稱已入冷庫或退休。
保留錯誤的首次 pytest 路徑造成 collection exit 4，另以正確的既有測試路徑通過，
原失敗收據已由成功結果 supersede。

線上另有獨立的 **5/5 當期模式收盤 CPU-cache reuse** 驗收，panel date 均為
`2026-10-15 13:30:00`，model forward／provider calls 均為 0，沒有初始化 CUDA。
原生 Discord journal 也確認非交易時段釋放 inference models。這不宣稱整張卡
使用率為 0，也不替代缺少歷史交易日的正式回放或下一次開盤驗收。

### 10/16：completed-forward 回填共用與低 I/O 維護

`completed_forward_compaction.py` 只處理原生 v3 的已提交 payload／receipt，與分鐘
工具共用 `immutable_cache_compaction.py` 的不可變檔處理。每個 key 仍由原生
singleflight lock 擁有；內容相同不代表可以用別的 input identity 略過 model forward。
新輸出若完全相同，原生 writer 現在可以直接重用這批封閉舊檔，不必另寫副本。

本次 full-SHA 計畫有 133 個穩定 committed keys、127 個不同 inode，18 組完全
相同的 payload。01:04–01:06 按原計畫 fingerprint 套用，74 個檔名改指向原有的
唯讀檔；實體配置 **34,070,290,432 → 11,793,879,040 bytes**，退休 inode 持有 FD
直到外部 FD／mmap 再檢查，確認無引用後減少 **22,276,411,392 bytes**。
未移除邏輯名稱、receipt、來源、模型、帳本或冷庫物件，不宣稱 Windows VHDX
已縮小，也不含八個缺少 commit 收據的未知 payload。

獨立回讀不用 compactor 的 SHA 函式，逐個核對原計畫中的 133 個路徑與 133 份
receipt bytes；實際重讀 53 個唯一 inode、11,792,796,055 bytes，18 組均已共用，
全部與套用前一致。這是儲存內容驗收，**不是目前部署 code/source admission**。
原生 reader 的 MAP_PRIVATE 隔離、新 key 重用舊封閉檔、atomic replacement、
external hardlink、busy writer、FD／mmap、中斷及損壞拒絕皆有測試。

另外修正了正常併發更新被誤判為全批損壞的問題：audit 不持有全目錄 writer lock，
新 key 共用 donor 會改變 nlink／ctime。現在重新清點並保留變動中的 key／新 key，
只對原計畫仍穩定的 key 套用；真正 SHA 不符仍拒絕。第一次原生 full-audit 因此
失敗的收據保留，以修正版成功稽核 supersede，沒有回寫舊失敗為成功。

日常既有 03:30 owner 採 `--sharing-candidates-only`：仍讀完整 commit metadata，
但只對可能共用的完整重複類別讀取完整 SHA；沒有重複時 payload 讀取為 0。
本機分鐘檢查 **5.464 秒、8,077 個 metadata-only inode、0 payload bytes**；Forward
檢查 **0.784 秒、53 個 metadata-only inode、0 payload bytes**。這不是全 cache
完整性證明；移除該旗標的預設仍為 full-SHA，canonical consumer 驗證也未省略。

`/etc/stockagent/completed-forward-compaction.json` 已以 root:root／0600 明確登錄，
只允許 canonical `artifacts/cache/completed_live_inference`。沿用同一 daily wrapper、
同一 flock，不加 daemon／timer、不擴大 generic artifact allowlist。已安裝 policy
實際 no-op apply 為 **1.043 秒、0 替換、0 payload bytes**。

本輪共享回歸 **183 passed，20.74 秒**；Ruff／shell syntax／diff check 通過。
仍未實作 forward 整體 byte budget／LRU／當期使用 lease、Panel 分塊 append、
大型 mutable DB 的獨立冷恢復退休、帳本分段或舊 hot 逐項退休。

#### 當期模式與線上 code admission 分開核對

00:54 的獨立 `factorized_sparse_projection.py` 修改使四份較早的 summary 正確拒絕
舊 code identity。保留該修改，不回滾或重標歷史輸出。第一個 qualified reload
又重現既有部署窗口的午夜錯誤：只允許當天 13:40 後，凌晨會永遠等不到暖機。
已補允許 07:00 前的**明確、最多 15 分鐘 deadline**；07:00–13:40、active opening、
過期／無時區／超長 deadline 仍拒絕，不改正常交易日及準備時間。
本次改的是有限時部署驗證，不是把每日 GPU 暖機提前到凌晨。

01:12:30 的原生 qualified Discord-only reload 驗收通過：同一 run 五模式
5/5 ready、21.382 秒；Gateway／code／entrypoint／原 signal IDs 與 ACK matched，
revision lag=0。execution／行情等所有 protected owners 未變，events／orders／fills
原前綴 SHA 與大小皆不變。第一個 timeout 收據保留，以上述成功切換 supersede。
原生 latest-close cache owner 另完成 attempted=5、failures=0，9.281 秒、507 MB peak。

01:13 的獨立 canonical reuse 為 **5/5 accepted**，均是
`2026-10-15 13:30:00`、0 model forward／provider calls、未初始化 CUDA；這五個
實際 current keys 都在壓實前原計畫中，payload SHA 相同，沒有為版本對齊而改寫
舊 receipt 或冒充重新計算。Code／cache 與正式歷史完整度、未來 09:00 SLO 仍分開。
部署驗證的臨時 deadline 已在驗證完成後清空，避免作為下一次常態保留 GPU 的設定。

另量測目前五個完整 native payload 的 tensor metadata，沒有 model／provider／
CUDA 工作。annual 的 882.48 MB 中，`temporal_basis_input_features` 單欄為
**571.82 MB**；其他兩個 multi-basis 的相同欄分別為 78.62／138.40 MB。
後續應先追蹤服務實際消費的 aux 與 summary ABI，設計可精確重建的服務投影，
而不只替全輸出加更多壓縮／副本。這個清點不是刪除 aux 或修改模型輸出的許可；
本輪仍保留所有原欄位及完整 bytes，未以截斷 lookback／降低精度冒充提速。

午夜／窗口／qualified reload／cached forward 回歸 **253 passed，11.03 秒**；
另修正既有 idle verifier 的舊 admission schema，加入必須匹配的 bot entrypoint SHA，
不把具備新欄位的正確 owner 誤判為失敗，也不略過錯誤 entrypoint。

部署驗證後沿用既有 idle-owner baseline／control 流程，先確認 flat、無 pending
work／受保護 owner 不變，僅重載 Discord 為無暖機的冷程序。01:19:23 實際 readback
為 `accepted_native_discord_loaded_code_admission`：**GPU 裝置控制代碼 0**、當期五份
canonical cache 仍可重用、code／entrypoint 相符、命令同步與 Gateway／ACK 正常，
本次新 run 沒有暖機或 fatal／watchdog 日誌。另讀原 qualified events／orders／fills
完整前綴與全部 protected owners 均不變。不是等到 01:22 才釋放，也不宣稱整張
GPU 或其他程序的使用率為 0；正常盤前暖機仍由原 session 排程擁有。
idle verifier／watchdog／window 回歸 **80 passed，5.09 秒**，與上述回歸有重疊，
不相加宣稱獨立測試總數。

### 10/16 01:35：從產生端縮小盤後訊號投影

追蹤 `signal_engine` 所有 aux 消費者後，確認日常訊號只使用權重、tuple 的原始分數，
以及 `score_logits`、`rank_logits`、`centered_score_logits`、`stock_market_gate`、
`z_market_delta`。其餘 candle／basis／embedding 是完整模型解釋的中間輸出，
不應因為 `return_aux=True` 就全部成為每次盤後快取的資料依賴。

原 canonical forward 不變；新增 `completed_tw_signal_values_v1`，在 **CPU 複製與
序列化之前**投影。Profile 進完整 input identity，沿用同一 writer、singleflight、
完整 SHA、MAP_PRIVATE、receipt 與原子 commit，沒有另一條計算／發布管線。
只有 completed-session 使用投影；開盤／一般 forward、訓練及完整模型解釋 ABI 不變。
不是縮短 lookback、少算股票、降低精度或移除原始特徵。

摘要原本的 `aux_fields` 名稱清冊保持相同；receipt 新增 `output_projection`，明列
`source_aux_fields` 與 `retained_aux_fields`。**名稱清冊不是已保存的張量**；投影也
不是能還原全部診斷的壓縮。完整診斷仍走原模型與精確來源／checkpoint，既有 full
payload 及收據本輪全部保留並獨立重驗 SHA，未作不具恢復證據的歷史退休。

同一 2026-10-15 收盤輸入，五模式實際 payload（十進位單位）：

| 模式 | 原完整診斷快取 | 新訊號投影 |
| --- | ---: | ---: |
| 100m | 13.578 MB | 398.417 KB |
| factorized scale-separated cash | 25.146 MB | 422.673 KB |
| multi-basis | 130.988 MB | 223.761 KB |
| multi-basis 22 | 233.012 MB | 399.761 KB |
| annual log cash | 882.485 MB | 219.537 KB |
| 合計 | 1,285.209 MB | 1.664 MB |

每組新結果寫入量減少 **99.8705%**；不是宣稱硬碟另釋出 1.283 GB。舊檔沒有刪除，
此次也未移動原始來源、模型或帳本。先前 full payload 共用維護仍可處理 v3 的一般
safe-container 格式；這次投影由 identity 的獨立 profile 辨認，不冒用原 full key。

驗收不是只比較投影：既有 benchmark 的 `--forward-control` 先對新 GPU forward
**所有原張量**逐 byte 比較舊 full control，再投影並完整比較所有保存值。修改一個
未被服務消費的 aux 仍必須在 commit 前失敗。安裝工具要求這份真實新 forward、
後續 warm hit 與 GPU 釋放證明；獨立程序再禁止 model／CUDA／HTTP，從正式 key
執行完整訊號，核對全部 weights／decision rows／model explanation 的原 SHA。
五模式全部相同，沒有用舊張量冒充新程式的 forward。

非獨占主機的 cache 暖讀觀察：annual **500.427 → 10.883 ms**，其他模式原為
28.8–145.2 ms、新為 7.5–12.3 ms。完整訊號仍含來源／輸入檢查及格式化；multi-basis
本次觀察 514.8 → 584.1 ms，不能宣稱每個完整流程都變快或等比例提速。
CPU 程序峰值 RSS 大致不變：舊 reader 已是 mmap，這次主要消除不必要的搬運、
序列化與每次完整 payload SHA I/O，不把檔案減量冒充整機 RAM 減量。

原生 postclose owner 完成 attempted=5、failures=0，7.480 秒；獨立正式 close readback
5/5 accepted。沿用既有 maintenance lease、`reload_plan`、idle baseline／control
只切換 Discord 到新的冷 owner，**不為採用純服務投影在凌晨強制 GPU 暖機**。
PID 974273 的 code／entrypoint、Gateway 命令同步、五模式原 signal IDs／ACK lag=0
已接受，GPU device handles=0；交易、行情、夜盤等所有受保護 owner、原庫存與
events／orders／fills 的完整既有 prefix SHA／大小皆不變。

本輪最終相關回歸 **395 passed，13.70 秒**；包含後補的 HTTP transport 級禁止測試，
不只封鎖已知行情 fetcher；Ruff／diff check 通過，未宣稱全 repo suite。
第一個 benchmark 遇既有 exclusive 來源更新鎖正確延後，保留失敗收據、等原 producer
完成後重試；沒有刪鎖或弱化來源 coherence。

01:35 時，本節只避免服務搬運與保存；模型內仍產生完整大型診斷。
該缺口的現行五模式續修結果見下一節。總 cache budget／當期 leases、Panel 分塊
append、mutable benchmark DB、歷史帳本與舊 hot 的獨立冷恢復退休仍未全部完成。

### 10/16 02:00：模型診斷按需生成，不再先算再丟

第一輪 candle-only 實測揭露另一條 `raw_features` basis 分支仍組裝大型診斷；
保留該 screen 收據，但未將它當成本次完成證據。最終沿同一個 `call_model`、
FinancialTransformer、factorized transport 與父層 temporal-basis encoder 修正，
沒有複製模型運算、建立替代 checkpoint 或另開推論服務。

`materialize_candle_aux=False` 是 **eval + inference mode 才能使用的 per-call 選項**，
只由 completed-session 訊號要求。模型的預設值仍為 True；訓練、完整解釋與開盤
維持原有完整輸出，state_dict／checkpoint ABI、原精度、股票／特徵軸與遮罩不變。
不修改共用 model flag，不把一次精簡請求留到下一次完整請求。

- `input_features` basis 保留原 `collect_aux=True` 數值路徑，係數仍供原投影／RMS
  運算；只略過未消費的係數展平／拼接、原路徑反事實診斷及額外輸出保留。
  不能因為不要診斷，就切到運算順序不同的 algebraic no-aux contraction。
- `raw_features` 的 fused projection 本來就不讀診斷係數與寬拼接；現在不再額外
  計算這套重複係數、拼接後再遮罩複製。真正的 fused kernel 及必要的小型 basis
  bank／weight 拼接保留。非 fused 的 embedded 分支仍建立其運算必需的輸入。
- 未組裝的診斷在暫態 aux 清冊明確為 None，進原訊號投影後不保存值；保留完整
  欄位名稱不代表已保存這些張量。`candle_and_basis_diagnostics_metadata_only_v1`
  進完整 input identity，新 key 不冒用原 full／舊 policy 的 key。

同一 2026-10-15 收盤資料，penguin RTX 5070 Ti、每模式四組交錯 AB/BA 的有界觀察：

| 模式 | 完整／按需峰值 allocated MB | 完整／按需模型中位 ms |
| --- | ---: | ---: |
| 100m | 151.811 / 146.016 | 23.513 / 23.174 |
| factorized scale-separated cash | 10,479.187 / 10,479.187 | 275.784 / 293.186 |
| multi-basis | 327.226 / 146.096 | 37.109 / 29.422 |
| multi-basis 22 | 345.010 / 122.179 | 55.576 / 45.675 |
| annual log cash | 1,856.199 / 187.973 | 80.715 / 68.863 |

年度模式不再組裝約 571.8 MB 的 `temporal_basis_input_features` 等診斷，模型峰值
下降約 **89.87%**。factorized 的峰值仍由原完整形狀輸入／數值運算主導，未降低；
本次也沒有所有模式都加速。主機非獨占，表格不是完整訊號、冷啟動或開盤 SLO，
更不是整機 RAM／顯存的節省數字。第一個 qualification sample 額外計算完整模型
oracle 與量測配對，不能拿它當正式 cache miss 的延遲。

驗收分三層，未以「少比較輸出」換取通過：

1. 新程式的預設完整 forward 先將**每個原張量**逐 byte 對照原 full control。
2. 精簡 forward 的明列診斷為 None；其餘全部值、完整來源欄位清冊、訊號投影與
   所有 weights／decision rows／model explanation SHA 必須相同。
3. CPU-only 安裝後，另開五個程序禁止 model／CUDA／HTTP，從正式 key 完整產生
   訊號；再驗原 payload／receipt 完整 SHA，舊檔刪除數為 0。五份服務投影仍合計
   1,664,149 bytes，沒有為新政策保存另五份完整模型診斷。

原 postclose owner 已完成 attempted=5／failures=0；正式 close readback 5/5 accepted。
01:59:57 新 Discord Python PID **1019716** code／entrypoint admission、Gateway 命令
同步、五模式 engine ACK lag=0、正式 keys 與 qualified models 一致，GPU handles=0。
沿原 maintenance lease／reload plan／idle baseline-control 只切換 Discord，沒有
凌晨強制模型暖機。全部受保護 owner、原庫存及三份帳本的完整既有前綴 SHA 不變。

最終相關回歸 **652 passed，60.86 秒**；完整／精簡、raw／embedded／input basis、
六種 attention route、factorized transport、GPU-cold 與安裝邊界均涵蓋，非全 repo suite。
原失敗收據保留：新增測試的 Haar lookback、必要小矩陣拼接及混跑 GPU 後的冷程序
fixture 已修正；另加明確 CUDA 已初始化拒絕測試，正式 cold guard 未放寬。
該里程碑當時仍未完成的 Panel 分塊見下一節；總 cache budget／leases、大型 mutable
benchmark DB、封閉帳本及舊 hot 的精確冷恢復退休仍待續，不能宣稱全系統完成。

### 10/16 續修：Panel 追加只寫變動塊，共用歷史而不另組整份副本

從原 writer 追到三個根因：全域 shape 改變使完整 NPY 失去重用資格；把陣列切檔
後若 reader 又 concatenate，就只是把重複成本搬到 RAM；共用歷史檔的舊 mtime
又會讓已提交的新快取被誤判過期。這輪沿原 `save_panel_cache_v2`、variant writer
lock 與 generation commit 修正，沒有新增下載器、CAS、同步器或 GC。

- 大型、C-contiguous、非 object 陣列採 `panel_time_chunks_v1`，8 MiB 固定 byte
  邊界；不按 session row 對齊，避免某些寬 row 迫使 chunk 接近整個 GB。原 dtype、
  shape、有序軸、NaN payload／正負零與完整 content fingerprint 不變。
- 每代擁有自己的唯讀 chunk links／descriptor，完整 SHA 相同才共用；新日期只寫
  尾塊與新增塊，歷史修正只換受影響塊。未封閉或可寫來源不作 hardlink donor。
  來源內容仍需掃描／雜湊，這不是將增量 CPU 複雜度虛稱為 O(新增資料)。
- 64-bit Linux reader 先保留完整的私有虛擬位址範圍，再在**已擁有的範圍內**映射
  各 chunk；不是猜 `/proc` 空洞。結果保持連續 `np.memmap`／ndarray ABI，沒有
  全歷史 concat、額外完整 RAM copy 或另一份連續暫存檔。`r` 不可寫；`c` 為私有
  CoW，不污染舊塊；view 維持映射生命週期，部分失敗釋放映射與 FD。
- 描述檔先驗證 shape／dtype／manifest 一致、完整有序 byte coverage、對齊、路徑
  與 bounds；開檔拒絕 symlink、可寫或截斷塊。不支援平台／page size 明確走 owned
  array 恢復路徑，不冒稱零複製 mmap。唯讀格式拒絕 `r+`／`w+`。
- freshness 由**對應來源 hash、backend、version 的已提交 manifest**判斷，不再
  取所有共用 payload 的最早 mtime；資料來源變動與損壞檢查不因此跳過。
- 保留小陣列 NPY。另對原 live-tail producer 明確 `chunk_storage=False`：短尾窗
  每次移除第一天會平移 bytes，分塊反而增加開檔與重寫成本；不能把適用於追加
  歷史的技術強套到 sliding window。

映射方法的約束依 [Linux mmap(2)](https://man7.org/linux/man-pages/man2/mmap.2.html)
與 [NumPy memmap](https://numpy.org/doc/stable/reference/generated/numpy.memmap.html)
核對；壓縮、量化、換 feature、縮短日期／股票軸均不在本輪變更。

#### 全量真實輸入與 ABBA

固定既有 manifest `98cd1f4e…5204f7af.json`／generation
`e5957528983a4557a4c1d5e297195286`，**3,119 日 × 2,761 symbols × 23 features、26 陣列**。
最後一個真實 session 用作 append；不是捏造資料或宣稱歷史 PIT 重建。
`scripts/benchmark_panel_chunk_storage.py` 依 whole／chunks／chunks／whole 量測原
canonical save、append、load、全陣列 byte SHA 及所有軸；原來源／收據的完整 SHA
和 inode metadata 於前後一致，production mutations／provider calls 都是 0。

| 同一完整 Panel 工作 | 原整陣列 | 8 MiB chunks |
| --- | ---: | ---: |
| append 真正新寫 payload | 1,257,328,370 B | 15,856,436 B |
| append 平均 wall | 9.605 s | 7.428 s |
| append＋load＋全量 byte 讀回平均 | 10.389 s | 8.324 s |
| initial build 平均 | 8.791 s | 9.278 s |
| load 兩次觀察 | 8.9–39.7 ms | 67.4–105.7 ms |

新格式的 append 重用 148 塊，寫 24 塊（15,818,752 B）及兩個小 NPY（37,684 B），
避免 1,241,513,984 B 重寫。第一次建置和開檔較慢，長期追加寫入明顯減少；不是
每次操作都提速。主機同時有採集／備份負載，這不是獨占硬體數字、API 吞吐結果、
整機 RAM 節省或真實 09:00 SLO。32 MiB screen 的較大尾塊重寫結果亦保留。

量測後才將 benchmark 的已封閉、全 SHA 相同輸出共用；不在計時內靠現成 trial
跳過建置，也不修改正式來源。跨次試驗沿既有 writer lease 驗 donor；完整工作流
證據保留，沒有每跑一次再永久多留四份等值完整 payload。獨立程序另對四組產物
重驗 **shape、dtype、全部 raw bytes、日期與兩個名稱軸**，不是只看 features checksum。

#### 正式服務接線與邊界

完整 reader 支援先到位，writer 需顯式 `STOCKAGENT_PANEL_CHUNK_STORAGE=1`；舊 NPY
及 pinned manifest 繼續可讀。`tw_minute` daily context 與 crypto geometry audit 改用
同一個 array reader，不另寫格式分支。Discord／postclose deployment templates
及本機 canonical units 已啟用；沒有清 cache 迫使 46.44 GB 全部重新產生。

新程式五模式各跑原預設完整模型全張量 oracle，再驗 metadata-only 所有非省略值
與完整訊號 SHA；CPU-only 安裝正式新 key 後，另五個程序禁止 model／CUDA／HTTP
仍得到相同完整訊號。舊 full payload／receipt 全 SHA 留存不變。原 postclose owner
於新設定下 attempted=5／failures=0，正式 close readback 5/5、model／provider calls=0。

沿原 maintenance lease、reload plan、idle baseline-control 只讓 Discord 採用新碼；
新 Python PID **1073828** code／entrypoint 相符，Gateway 命令同步、五模式 ACK lag=0，
GPU device handles=0。交易／行情／夜盤等受保護 owner、庫存及三份帳本完整既有
前綴 SHA 不變；沒有凌晨強制 GPU 暖機。這是 reader／writer 能力已部署，不等於
既有全目錄已轉成 chunks；下一次真正來源更新的實際寫入仍需其當期收據驗收。

相關回歸 **426 passed，29.27 秒**，包含截斷／路徑越界／symlink／可寫塊拒絕、
source donor 真實 SHA 變動、partial mmap／descriptor commit 失敗恢復、CoW view
生命週期、native minute context、短尾窗原格式及既有 cache／GPU-idle 邊界；不是
全 repo suite。先前 filename 與小型 fixture 失敗收據留存，由此次成功回歸 supersede。
總容量／pin-leases、mutable benchmark DB、封閉回放／舊 hot 冷恢復退休仍未完成。
另現行 background maintenance 的 factorized **完整歷史**範圍缺口仍保留錯誤紀錄，
不是本輪 5/5 盤後訊號 cache 的失敗，也不以縮短其歷史來掩蓋。

### 10/16 續修：成功使用取樣、實體容量清冊與低成本引用檢查

沿原 `cached_forward` 的 key lock，在完整 payload／identity 驗證及 caller 的
source/code revalidation 成功之後，才更新獨立 `.usage.json`。小於 4 KiB 的
bounded、no-follow、private regular JSON 綁定 key／receipt SHA／payload SHA／bytes；
每 key 每分鐘最多 atomic replace 一次。它是使用取樣，不是完整 hit counter 或
冷恢復證明。失敗計算、驗證、提交不續用；損壞、深層 JSON、symlink／FIFO、
I/O 失敗與回撥時鐘均不把原有效 cache 變成 miss，也不覆寫不明使用證據。

既有 `deduplicate_artifacts.py --completed-forward-cache-policy` 結果新增
`working_set`，原 `/etc/stockagent/completed-forward-compaction.json` 和每日 timer
繼續持有維護責任，沒有新增 GC daemon／全樹 scanner。清冊區分邏輯 bytes、
unique inode 配置量、未知／忙碌提交、外部 aliases 和程序引用；新增／變動的
namespace 不冒充穩定快照。可傳 soft byte target 做壓力計算，不能因此刪檔。

當期清冊為 184 個 receipt-coherent key、八個未驗證 payload 原封保留；整個 immediate
regular namespace 配置 **13,160,804,352 B**，其中已提交 payload unique 配置
**11,795,554,304 B**。這是 metadata 清點，不是全 payload 完整性重驗。最終另以
canonical enabled close-artifact 驗證得到的五個現役 key 做 explicit protected plan；
它不是全 fleet 的持續 pin registry，日常清冊也不宣稱 pins 已完整。

最初引用 probe 把每個 `/proc` path 比對 184 個 payload paths。因任何引用本來就
保守保留整份 inventory，改成一個 root 邊界＋完整 inode set，保留外部 hardlink 的
inode 引用閘門。完整 metadata-plan ABBA：舊 fanout 6.062／5.784 秒，新邊界
1.675／1.597 秒，平均 **5.923 → 1.636 秒，少 72.38%**；四次配置量和提交 key
數相同、引用皆空、payload hash bytes=0。主機非獨占；不是整個服務或下載提速倍數。

另對當期五個正式 key 做 1,200 次 CPU hit，全部張量 bytes／完整容器值相同、
payload／receipt generation 不變，沒有 model、provider 或 CUDA 工作。每 hit 寫
usage 的對照在 400 calls 有 400 次寫入；取樣組同一窗口 400 calls 沒有額外寫入。
**舊版未追蹤 usage 本來就沒有這些 writes**，不能把對照當成消除既有寫入的成績。
平均 hit 時間：未追蹤 4.238 ms、每 hit 寫入 4.929 ms、取樣 4.617 ms；新增可觀測性
相對未追蹤約多 0.379 ms，本輪不宣稱所有 hit 更快。

最終相關回歸 **460 passed，41.91 秒**，不是全 repo suite，亦不與舊輪次加總。
五模式新完整預設 forward 全張量、lean 非省略值及所有訊號 SHA 與原控制相同；
正式安裝全部共用已存在的 payload，避免另寫 1,664,149 B，舊 full controls 不刪。
五個 fresh CPU-only 程序禁止 model／CUDA／HTTP 後仍相同。

原 postclose owner 成功，formal close readback 5/5，model/provider=0。新原生 Discord
Python PID **1115601** code admission、Gateway／五模式 ACK lag=0、GPU handles=0；
protected owners、庫存和 events／orders／fills 原完整前綴不變。沒有強制 GPU 暖機。
第一輪資格驗證期間補入 JSON 深度防護，舊 resident 正確被 code admission 拒絕；
保留原失敗並由固定程式後的五模式完整驗證取代，沒有放寬 guard。

仍待完成：全 consumer pins／硬 byte budget、需要 exact recovery 的舊 key 與大型
mutable benchmark DB 退休、歷史帳本與舊 hot 冷恢復後退休、下一個真實來源更新的
chunk receipt，以及其餘正在執行服務的完整流程量測。既有 46.44 GB Panel 沒有
強制遷移，舊完整歷史 calendar 的既有錯誤也沒有被清掉或假稱已修復。

- [本輪完整原生驗收](../artifacts/operations/service-cache-compaction/forward-working-set-production-20261016/acceptance.json)
- [完整容量清冊與當期明確保護項目](../artifacts/operations/service-cache-compaction/forward-working-set-production-20261016/working-set-current-consumers.json)
- [完整 metadata-plan ABBA](../artifacts/operations/service-cache-compaction/forward-working-set-production-20261016/working-set-probe-abba-final.json)
- [完整 CPU hit 與 usage 成本](../artifacts/operations/service-cache-compaction/forward-working-set-production-20261016/cache-hit-balanced-final.json)

## 10/16 續修：現役引用自動解析與封閉 benchmark 工作副本

本輪改的是資料管理責任的連動，不是再加一個常駐掃描／清理服務。

### 當前服務引用

`artifact_consumers.completed_forward_consumer_pins` 直接沿用 canonical live market
loader、model selection 與 `resolved_live_output_dir`，只讀選定版本的
`latest_signal.json` → `summary.json` → forward key。另修正一般 service dependency
清冊漏列 `.yml` 的問題，與原 live loader 的 YAML/YML 範圍及順序一致。

市場設定、model selection、runtime enable state、pointer／summary 都有 bounded
regular-file 讀取與 generation／SHA 記錄，整次清冊前後再查一次；runtime enable 與
獨立 overnight consumer 也不能因 Discord toggle 關閉而被隱藏。超大／FIFO／symlink、
錯誤 market/signal identity、未知 forward contract 或查核期間新增市場，都標成
incomplete，保留已觀察到的 key，並保留整份快取；不由一份空清冊推定沒有需求。

原 daily compactor 在完整 working-set plan 前後各取一份 scoped consumer census，
變動時保護兩份 key 聯集。原 `/etc/stockagent/completed-forward-compaction.json`、
owner／鎖與 timer 不變。`consumer_pins_complete_for_scope=true` 只表示當前 enabled
TW day-trade latest-signal 範圍；`consumer_pins_exhaustive=false`、
`eviction_allowed=false` 仍保留，不能因此宣稱全 fleet pins 或硬容量淘汰已完成。

真實來源 CLI audit **1.609 秒**；同入口 apply（與工程／冷編碼同時執行）**2.757 秒**，
完整 immediate namespace 13,160,804,352 B，已提交 payload unique 配置
11,795,554,304 B；五個現役 key 全部找到 verified commit，八個不明提交保留，
沒有額外 payload hash／replacement／邏輯路徑刪除。兩個單次時間不是 ABBA 加速比較。

原生 readback 仍是 Python PID **1115601**、原 code admission，GPU handles=0，
五模式 CPU-cache reuse 與 engine ACK 5/5、cache errors=[]，沒有服務重啟、provider
請求或交易寫入。本輪沒有改動模型／資料 ABI，這是既有 acceptance 的當前 readback，
不是新一輪獨立 GPU numerical oracle 或下次開盤 SLO。

最終相關共享回歸 **429 passed，131.39 秒**，涵蓋 consumer parsing／競態、原
artifact retirement／冷恢復、benchmark 輸入、forward／usage／physical compaction、
原生 cache、incremental producers 與 Panel chunks。這不是全 repo suite，也不把
前一輪的 460／422／305／128 項重疊數字相加。

### 封閉工作庫與驗收證據分離

新鮮 `du` 基線：`running_services_20261013` 配置 183,516,581,888 B。
最大案例 `openbb_full_size_native_writer_index_20261015` 有 13 個原檔，邏輯
47,074,348,861 B、regular-file unique 配置 47,077,916,672 B。其中兩份 SQLite
是原 native persistence ABBA 的 mutable 變體，不符合 immutable-input inode 共用。

既有手動 WSL preserving owner 現可明確選取一個 `benchmarks/<suite>/<case>`，
但拒絕整個 suite／benchmark 根與 automatic enrollment。只有既存 producer 的完成
comparison receipt 可進入這個流程；未知／失敗／未完成／超界 evidence 都保留原件。
完成檢查收斂到既有 `benchmark_fixtures`，canonical `source_plan` 與外層 inventory
共用；直接呼叫保存層也不能繞過。這是 producer-declared completion，並不等於
重新執行原數值 benchmark。真實 13 檔與已保留 JSON 的新共用層 readback 相同，
不載入 Torch，也不重送大型交易。額外測試涵蓋重複 JSON key、非字串 contract、
未完成 trial 及 bypass outer CLI 的呼叫。
7 個原 JSON（fixture、workload、四次 trial、ABBA acceptance）共 **1,326,909 B**
已逐 bytes 保留在本機退休 receipt 旁，刪除前與最後 readback 都會再次比對。

這一個案例已完成來源／設定／service／FD/mmap 清點，blockers=[]，並在 nice 15／
低 I/O 優先權下沿原 canonical capture、D publication、獨立原始恢復與 quarantine
流程執行。03:28 已取得獨立 D 原始恢復 proof：13 檔、47,074,348,861 B，
原始解碼及 cold objects 均驗證、source drift=[]、驗證 scratch 已移除；尚待同步
收斂與最後退休清點，**沒有宣稱已刪除或已回收 47 GB。**來源與 D/NAS 原件
不會因一份 acceptance JSON 或 transport 成功就被略過恢復驗證。

正在執行的交易保留啟動時三份原碼與 SHA，沒有因上述共用層抽取而重啟／重送。
後續交易另外修正 phase 標籤：取得共用鎖後標成 `publish-under-shared-owner`，
不再把整段 owner 內發布誤寫成等待鎖；測試確認發布期間仍持有原共用 owner。

03:31 定位到獨立恢復後的等待並非傳輸未完成：必要 peer 當時 completion=100、
needBytes=0、packed idle／folder errors=0，但全域 recent errors 還保留前日
16:51 的四筆 index 磁碟不足告警。逐一查核原四個 folder 已 idle／scanning、
errors=0、pullErrors=0、watchError 空；index filesystem free 約 7.70%，高於原
1% 限制。先保存完整告警與當前 readback，再在訊息仍完全相同、空間仍充足時
使用 [Syncthing 官方 recent-error acknowledgement](https://docs.syncthing.net/rest/system-error-clear-post.html)。
HTTP 200 後 errors=null；**沒有改同步設定、降低原門檻或忽略未知錯誤**。
原固定交易隨即進入 `full-source-mirror-D-retirement-audit`，未重啟或重編碼。

03:39 狀態：完整 source／mirror／D plan 的 blockers=[]、固定 cold 身分一致；
已進入 canonical `retiring` quarantine 及 `verify-and-retire-hot-source`。
原路徑的移走不等於 unlink／回收，原檔仍由這筆隔離交易保護；尚無最終
`retirement.json`／acceptance，不預先計入 47.08 GB。

七份本機 JSON 另逐檔比對原 archive manifest 的 `original_sha256` 與 size，完全
相同；該小型 manifest 原 bytes 亦保留在 receipt 旁。以後看 benchmark 結論直接讀
本機 evidence；只有要重跑原工作庫，才使用既有 `manage_legacy_artifact_archives.py
restore`，選這筆交易的 `catalog.json`、固定 dataset 與一個不存在的新 destination。
完整獨立恢復已驗，不為補文件再物化 47 GB。此冷歸檔仍是 non-deployable，
不會因 restore 自動加入服務或訓練。

固定證據入口：

- [實際每日 owner 與五個自動引用](../artifacts/operations/service-cache-compaction/consumer-pins-production-20261016/daily-native/completed-forward/completed_live_inference/latest.json)
- [原生 CPU-cache／GPU idle readback](../artifacts/operations/service-cache-compaction/consumer-pins-production-20261016/runtime-readback.json)
- [新鮮 benchmark inventory](../artifacts/operations/service-cache-compaction/consumer-pins-production-20261016/openbb-benchmark-inventory.json)
- [固定退休交易進度](../artifacts/operations/service-cache-compaction/consumer-pins-production-20261016/openbb-retirement/progress.json)
- [小型原始證據清冊](../artifacts/operations/service-cache-compaction/consumer-pins-production-20261016/openbb-retirement/legacy-wsl-offline-65b8cfd5eb7a116a3ccfef89/retained-benchmark-evidence.json)
- [獨立 D 原始恢復 proof](../artifacts/operations/service-cache-compaction/consumer-pins-production-20261016/openbb-retirement/legacy-wsl-offline-65b8cfd5eb7a116a3ccfef89/cold-recovery.json)
- [七份本機 JSON 與原 archive digest 的獨立比對](../artifacts/operations/service-cache-compaction/consumer-pins-production-20261016/openbb-retirement/legacy-wsl-offline-65b8cfd5eb7a116a3ccfef89/retained-original-digest-readback.json)
- [429 項最終共享回歸](../artifacts/operations/agent-workflow/runs/consumer-pins-and-shared-closure-20261015T192619-657a58e9/run.log)
- [真實來源與保留 JSON 的共用完成檢查](../artifacts/operations/agent-workflow/runs/shared-benchmark-real-closure-re-20261015T192618-783dfe08/run.log)
- [四筆歷史磁碟告警與所有當前 folder 狀態](../artifacts/operations/service-cache-compaction/consumer-pins-production-20261016/syncthing-error-review.json)
- [已排除告警 acknowledgement 與獨立 readback](../artifacts/operations/service-cache-compaction/consumer-pins-production-20261016/syncthing-alert-ack.json)

03:46 原固定交易 `completed-benchmark-openbb-cold--20261015T190939-8908773c`
已 exit 0；`retirement.json` deleted=true、`stage-pruned.json` pruned=true。
03:47 另一個唯讀程序獨立核對原 source、該精確 quarantine 與編碼 stage 皆不存在，
七份 retained JSON 的集合、size、SHA 均等於原 archive；state=cold-only、固定
snapshot／manifest 身分一致。原工作副本實際最後-link 配置回收 **47,077,916,672 B**；
本次臨時 stage **10,151,731,200 B** 另外列示，不能再加成既存資料的減量。
冷物件與 NAS snapshot 刪除皆為 0。恢復保障仍為 `single_d_volume`，
`backup_verified=false`，不可把 D 原始解碼驗證說成 NAS 離機備份完成。

- [固定最終 acceptance](../artifacts/operations/service-cache-compaction/consumer-pins-production-20261016/openbb-retirement/legacy-wsl-offline-65b8cfd5eb7a116a3ccfef89/acceptance.json)
- [獨立 namespace、原始 JSON 及固定身分 readback](../artifacts/operations/service-cache-compaction/consumer-pins-production-20261016/openbb-retirement/legacy-wsl-offline-65b8cfd5eb7a116a3ccfef89/independent-final-readback.json)
- [退休後固定 D manifest 與七個既驗證 cold objects 的 generation readback](../artifacts/operations/service-cache-compaction/consumer-pins-production-20261016/openbb-retirement/legacy-wsl-offline-65b8cfd5eb7a116a3ccfef89/independent-cold-generation-readback.json)

仍待完成：其餘大型工作副本／closed ledgers／舊 hot，
全 consumer graph／硬 byte budget、真實新來源更新的 Panel chunk receipt，以及其餘
服務完整流程的量測。保留既有 46.44 GB Panel 舊代與沒有恢復證據的唯一檔案；
不以局部工程成功結束全服務效能目標。

另對原完整 `service_artifact_paths` 做 fresh-process 診斷：516 個 paths，單次
4.555 秒、峰值 RSS 763,080,704 B、Torch imported=true、CUDA initialized=false。
獨立 import-only 歸因中，`stockagent.config` 約 0.346 秒／86 MB、沒有 Torch；
`live.signal_engine` 約多 4.309 秒／670 MB，載入 Torch，但未初始化 CUDA。
這不是模型計算變慢，也不是已驗收的完整解析器優化；是下一步把路徑解析從重型
推論模組解耦的可重測基線。新的 scoped pin probe 已避免這個重型解析入口。

## 10/16 04:10 續修：重用已正規化的 tail，修復原維護 owner

### 下載器不再反覆生成相同日期欄位

在既有 intraday owner 對三個交易所的完整原生程序取樣後，下一個重複工作是
Bybit／OKX 對已排序、已為標準秒字串的 date 再推斷格式、轉字串及排序。
共用 `ohlcv_hot_tail.canonical_date_frame` 仍逐值驗證 ASCII 文法、有效日期、
非空及嚴格遞增；不相信 sorted metadata。重複／亂序、無效、含時區、次秒或
其他型別回到原完整規則，保留舊 row priority；Binance 的 close-time 修復規則不動。
不改 OHLCV、單位、時間、抓取範圍、API 配額或落地欄位。

同一次封閉擷取的 **1,974 份、2,057,365,470 B** 實際 tail（Bybit 894／OKX 505／
Binance 575）沿既有 immutable-fixture owner 重用；Bybit 另兩個現存 base 沒有 tail，
不冒稱也有 tail 驗收。沒有複製約 176 GB 歷史 base，重試量測也不再複製這 2 GB。
四輪 ABBA 逐檔完整 read／fresh-overlap 準備／normalize／merge／全值指紋，結果相同：

| 完整 tail 合併階段 | 原路徑平均 | 重用路徑平均 | 變化 |
| --- | ---: | ---: | ---: |
| Wall | 130.646 秒 | 95.702 秒 | -26.75%，1.365 倍 |
| CPU | 222.508 秒 | 181.087 秒 | -18.62% |

fresh overlap 是原本已觀測的末三列，不虛構新 K 棒。全欄位型別、每列有效性、
所有有效數值位元、筆數與 changed flag 參與比較；浮點 signed zero／NaN payload
保持區別。先前 IPC 儲存位元組比較曾因 StringView 配置與未定義 NULL 槽而拒絕，
原失敗收據保留；改用 typed validity/value-bit oracle 後全量通過，不放寬有效值。
Arrow 允許不同的字串 view 配置與 NULL 內容，不能把實體 IPC 布局當作值本身。
參考 [Arrow columnar format](https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-view-layout)。

這是完整「tail 合併階段」，**不是** HTTP、落地、reconciliation 與整段 collector
lifecycle 的提速證明；未清 OS page cache、主機不獨占，亦不是完整歷史抓取完成。
493 項下載器／reader／oracle／admission 相關回歸通過（15.93 秒）。
04:10 原定時服務的新 owner 實際執行 helper，Bybit／OKX 各 174／9 個 stack samples，
同一 invocation／owner 前後相同，六個 protected owners 不變；沒有重啟服務或額外
provider 呼叫，也未初始化 CUDA。新舊 prototype 與正式完整 module AST 的唯二
差異受 admission 檢查，只允許兩個 provider 的 import 與 normalizer 前置段改動，
不能只用程式標籤推斷已採用。

- [全量 ABBA 與固定程式／輸入身分](../artifacts/benchmarks/running_services_20261013/cex_date_normalization_typed_validity_20261016/comparison.json)
- [原定時 owner 的實際 helper 執行驗收](../artifacts/operations/running-services-optimization-20261013/cex-date-reuse-20261016/native-admission.json)
- [493 項回歸](../artifacts/operations/agent-workflow/runs/cex-normalization-adopted-closur-20261015T200927-447cc67a/run.log)
- [被拒絕之序列化比較的診斷，非數值成功宣稱](../artifacts/operations/running-services-optimization-20261013/cex-date-reuse-20261016/rejected-serialization-diagnostic.json)

### 原每日維護的格式故障與不必要走訪

03:59 原 `stockagent-artifact-dedup.service` invocation
`771d452ddfe846bc8f41d8aee942250d` 真正失敗：把其他 producer 的清單型
`progress.json` 當 lifecycle dict。已修共用 parser：只有 state/phase 均 complete
的物件能授權；array、null、scalar、壞 JSON、壞 UTF-8 均阻擋其子樹，不中止其他
合格範圍，也不繼承較上層 complete 的權限。

維護走訪改為**目錄層級**的最近 marker 判定；用 complete-prefix 集合略過沒有
任何完整 owner／完整 descendant 的分支，不再每個檔案掃整份 marker 清冊。
先掃 marker 時同樣略過明定排除的頂層。仍容許 blocked parent 裡有自己的 complete
child，保留原 most-specific 語意，沒有放寬穩定期／來源／帳本保護。
109 項相關儲存維護回歸通過（6.57 秒）；與前述下載器測試有重疊，不相加。

原規則全目錄唯讀稽核成功：5,025 個合格檔案、136 complete／143 blocked roots，
260 個 exact groups／842 個 duplicate inodes；候選皆在 ablations／benchmarks／
markets／operations，沒有 cache、live、orders 或 mutable DB。沒有邊讀邊變動；
此時只列候選 841,273,344 B，不當作已釋出量。新稽核完成後，精確核對並停止仍在
走訪的舊工程 audit child，保存 -15 與原碼；不停止 production，也不拿未完成的舊
稽核宣稱完整 speedup。04:10 已讓原 installed、Nice=19／CPUWeight=1／idle I/O
的每日 owner 恢復執行；完整套用及獨立 readback 另驗，不以 start 成功冒充完成。

04:11:26 原 invocation `59b451b1f9d14b4f94adebdae98aecea` 完整 exit 0：分鐘共享、
forward 共享及 generic completed-run 三階段全部成功。整個 owner 約 66.2 秒／
54.361 CPU 秒，峰值記憶體約 100 MB；此前的 pruned 唯讀 audit 完整 37.059 秒。
這不是與被停止的舊 audit 做公平全流程速度比。

Generic 重新依當次時鐘判定後是 5,082 eligible／264 groups／863 replacements，
不固定沿用前次 24 小時門檻的候選集合。04:12 獨立程序逐一核對全部 **1,127** 個
邏輯路徑與 inode，對 **264** 個實體內容完整 SHA；只雜湊 **431,734,575 B**，
不重讀每個相同 hardlink。所有原 digest／size／新 inode 相同、metadata 穩定、
六個 protected owners 不變、code 與恢復前 SHA 相同，沒有刪邏輯路徑／來源／帳本。
原 generic receipt 沒有舊 `nlink` census，所以這次不把 881,770,496 B 候選配置量
直接宣稱為最後-link 實際回收；不加進前述 47.08 GB 回收量。

- [109 項回歸](../artifacts/operations/agent-workflow/runs/dedup-marker-and-traversal-regre-20261015T200741-57d53016/run.log)
- [原規則唯讀候選計畫](../artifacts/operations/service-cache-compaction/daily-marker-recovery-20261016/pruned-audit/artifact-dedup-artifacts-20261015T200841.222202Z.json)
- [停止被替代工程 audit 的精確 owner 收據](../artifacts/operations/service-cache-compaction/daily-marker-recovery-20261016/stop-superseded-unpruned-audit.json)
- [原服務恢復前的 code／audit SHA 與 protected owners](../artifacts/operations/service-cache-compaction/daily-marker-recovery-20261016/native-before.json)
- [全部替換、SHA、原生 owner 與三階段完成的獨立 readback](../artifacts/operations/service-cache-compaction/daily-marker-recovery-20261016/native-independent-complete-readback.json)
- [獨立程序的完成紀錄](../artifacts/operations/agent-workflow/runs/daily-dedup-independent-complete-20261015T201227-46b8fae2/run.log)

另 03:41 的最新全服務 census 有 87 個 installed service 加兩個工程 transient，
67 timer、3 path；取樣期間仍有下載、冷編碼等實際工作，不能稱整機 idle。
GPU device 3%／約 3.5 GB 含 Windows，不能當成 WSL GPU 工作歸因。
剩餘全服務、硬容量／完整 consumer graph、歷史帳本與舊 hot 等目標仍 active。

- [完整原生服務 census 與工程負載區分](../artifacts/operations/running-services-optimization-20261013/all-native-services-after-producer-reuse-20261016.json)

## 10/16 04:35 續修：儲存依賴查詢不載入推論引擎

### 改原呼叫架構，不另複製解析規則

原本 `artifact_consumers.service_artifact_paths` 只是為了取得 panel cache root，
卻 import `signal_engine`，連帶載入 Torch、模型與資料模組。現在將原函式**完整
搬移一次**到既有 `live.market_config.live_panel_disk_cache_root`，live writer 保留
原私有名稱的 import alias，依賴清冊呼叫同一個 public helper。相對環境 override
仍相對於 process cwd，預設仍使用 repo root；不改任何資料／欄位／來源／模型路徑。
獨立 AST 比較確認整份原 engine 只少該函式、多一個 alias；原 config 只增加原
函式與 `os` import，沒有第二份 resolver 或 benchmark-only production 邏輯。

保留原 consumer module，八個 fresh worker 交錯 ABBA × 2，完整讀取目前所有
市場、model selection／candidate、experiment config、implicit caches、minute
fallback、replay source pins。全部 **516** 筆 dict 的欄位、內容與順序相同，SHA
`20a61bb72201f7ddad3cd5e8a1ac2b3e1ca5576267877a0571ae78b66ec2ee1b`。

| 完整依賴查詢，每組四次平均 | 原版 | 共用輕量 resolver |
| --- | ---: | ---: |
| Wall time | 6.535 s | 1.436 s |
| CPU time | 5.986 s | 1.292 s |
| Fresh-process peak RSS 的平均 | 764,321,792 B | 180,579,328 B |

新版在 worker 載入階段禁止真正 import Torch／CuPy／Shioaji／signal engine，
所有 worker 禁止 network connect，CUDA 均未初始化。Polars 的 optional dependency
`find_spec` 探測不是實際載入；第一版 checker 誤攔這種探測，失敗收據保留，修正
checker 後另用新命名完成全八輪，並新增「允許探測／仍禁止 import」回歸。
Reference worker 載入目前 engine，其**僅原函式搬移**已由整份 AST 比較獨立證明。
非獨占主機、未清 OS cache；這是完整 metadata resolver，不是整機／儲存工作流
同幅提速，也沒有強制重啟既有 resident storage workers 來卸載它們先前的 imports。

### 與實際推論、帳本及原生 owner 分開驗收

Engine 檔案身分改變，未偽造舊 completed-forward key 為新碼。沿原 benchmark
跑五模式真正 fresh forward，與保留的完整原輸出逐張量 bytes、全部非省略值及
完整訊號 SHA 比較；經原 qualification／source lease 安裝五個新碼 CPU keys。
再由五個全新程序禁止 model／CUDA／HTTP，從正式 keys 完整產生訊號，SHA 相同。
這是有界工程 qualification，不是新訓練、broker fills 或下一次開盤 SLO。

只在原 maintenance lease／reload plan／idle admission 下切換 Discord。04:29
後新 Python PID **1282257** 的 code／entrypoint、Gateway／command sync、五模式
engine ACK lag=0 都通過；GPU device handles=0，六個 protected owners、原庫存與
events／orders／fills 的完整前綴不變。04:34 原 postclose owner attempted=5、
failures=0；正式 strict cache-only 回讀五模式均 0 model／provider、未初始化 CUDA。
最終共享回歸 **345 passed，43.31 秒**；儲存／退休／訓練回傳相關 **229 passed，
135.42 秒**。兩組按各自範圍列示，不當成完整 repository suite。新 script、原
market config 與新增測試 Ruff、修改範圍 diff whitespace／py_compile 通過。

### 04:35 時定位：永豐儲存清冊仍逐檔掃描

04:16 全服務 census（87 installed＋一個 engineering、67 timers、3 paths）定位
到**原每小時**的 Shioaji storage monitor，不是每分鐘面板／行情 API 輪詢。
04:09 invocation `64a3d41b515542e58fa6c03e64746afe` 於 04:19 成功完成：
5,478,072 files、130,089,188,407 logical B；**605.554 wall s／213.713 CPU s，
systemd cgroup peak 13.5G**。取樣當時有約 514 MB／22.05 s 的 block read，後續
cgroup 以 file cache／可回收 slab 為主，不是 Python heap 或重新解碼行情。

後續應沿原 capture `PartWriter`、session／worker manifests、history receipts
與既有 streaming byte counters，建立可驗證的分區／generation 聚合，封閉原件
重用清點結果、active／補洞只算 delta。不能只以父目錄 mtime 未變便略過檔案
內修改；未知檔案、原子替換、人工修復、分區退休、失敗／競態及過期清冊都須
明確失效／回退，另保留全量 metadata oracle 驗收。**本輪未修改此 scanner、採集
flush／持久化頻率、登入、API、全域 kernel cache 或 live 行情 owner。**

總目標仍 active：本項不等於完整 consumer 圖／hard byte budget、其他 completed
fixtures／歷史帳本／舊 hot 的精確恢復退休，或真實新來源的 Panel chunk receipt。

- [全量清冊八輪與完整計時](../artifacts/operations/service-cache-compaction/lightweight-consumer-resolver-20261016/complete-paths-abba-v2.json)
- [整份原 engine／config 的原函式搬移證明](../artifacts/operations/service-cache-compaction/lightweight-consumer-resolver-20261016/exact-relocation-proof.json)
- [五模式完整 oracle、正式 cache、原 owner 與全部證據 SHA](../artifacts/operations/service-cache-compaction/lightweight-consumer-resolver-20261016/acceptance.json)
- [原生冷 owner 最終回讀](../artifacts/operations/service-cache-compaction/lightweight-consumer-resolver-20261016/final-native-readback.json)
- [原 postclose owner 的完整 invocation 記錄](../artifacts/operations/service-cache-compaction/lightweight-consumer-resolver-20261016/postclose-native-proof.json)
- [345 項共享回歸](../artifacts/operations/agent-workflow/runs/consumer-resolver-final-shared-t-20261015T202638-3af4ba95/run.log)
- [229 項生命週期回歸](../artifacts/operations/agent-workflow/runs/consumer-resolver-storage-lifecy-20261015T202804-64c14481/run.log)
- [包含實際監控讀取負載的全服務 census](../artifacts/operations/running-services-optimization-20261013/all-native-services-after-cex-and-dedup-20261016.json)

## 10/16 04:56 續修：原下載器版本驅動的儲存清冊

### 先修寫入責任，才能安全重用

`download_shioaji_tx_futures_ticks.main` 原有兩條 manifest 路徑沒有完整 writer
lease：無 pending 時直接寫 manifest，以及登入／下載 finally 釋放鎖後才寫最後
manifest。原 batch 又在 child 返回後修改 unavailable manifest。現在這些都在
既有 `download.lock` 下完成；Tick、receipt、calendar、重試與數值轉換八個
原函式的 AST 完全相同，API 請求與登入策略不變。

共用 `shioaji_storage_epoch` helper 在**任何資料修改之前**，將新的隨機 nonce
寫入原 lock 並 fsync；中途失敗也使舊清冊失效。不用 `os.replace` 更換 lock inode，
避免舊、新兩個互斥 owner。觀察者以短 nonblocking shared lease 讀取版本，完整
find/stat 掃描在 lease 外進行；只有掃描前後 epoch／control 相同才提交可重用結果。
寫入中或未符合契約時，保留原本的完整 metadata scan，不阻塞下載等待長掃描。

既有 773 個 history roots 以原 batch lease 進行 local-only enrollment；若還有
已載入舊碼的 downloader process 或 batch owner，先 deferred，不重啟行情／登入。
本次 apply 全部成功；獨立重讀全部 773 份原始 manifest 的完整 SHA 都未變。
只修改現有 download.lock 控制內容，沒有修改 Tick、source receipts 或 manifest。

### 重用的是小型統計，不是另一套行情庫

清點以每個台北曆日的 bytes／file count／最大 mtime 聚合，保留原 30 個完整日的
成長定義、總容量、總檔數與最新修改時間。日期跨午夜仍由原午夜邊界重新選取。
快取位於原 monitor output 下的 private `.inventory-epochs`；檔案限量讀取、
checksum、權限與 no-follow 驗證。合約增加／移除、nonce、manifest／控制項改變、
損壞、24 小時期限或 `--force-full-scan` 都會重掃；不靠父目錄 mtime 判定內容沒變。

**邊界：**未走 writer helper 的人工原地修改不能即時被 epoch 察覺，必須明確
full reconcile；每日完整 reconciliation 保留。這是容量監控，不是來源品質、PIT、
訓練、完整覆蓋或冷恢復驗收。舊 manifest、source hashes 與 source receipts 不重標。
其他九類資料群組尚未改為 epoch 重用，仍按原清單掃描。

### 完整量測及實際 owner

相同完整 history、1,920,521 files、17,080,457,125 logical B、相同原始程式
reference／producer control／固定時鐘，fresh workers ABBA 的全部容量、檔數、
最新時間及 30 日成長值 SHA 均為
`6d5d2c38fd2b3330ec49d8d685164ec347b91850756a23a9985050ca4f33b940`。

| 完整 history 清點，兩輪平均 | 原版 | epoch reuse |
| --- | ---: | ---: |
| Fresh-process wall | 29.243 s | 1.286 s |
| CPU，含 find children | 31.178 s | 1.181 s |
| Process peak RSS | 29.29 MB | 29.18 MB |

首次建置另保留 24.456 秒量測，不能宣稱第一次也零成本。重用 773 個合約、避免
1,920,520 次逐檔清點；parent batch.lock 仍逐次計數。新增 metadata 共 773 檔、
1,179,243 logical B／3,166,208 allocated B，未複製行情 payload。

以一次明確、已 qualification 的 start 執行原 `stockagent-shioaji-storage-monitor.service`，
不改 timer 或新建排程。04:52:28–04:52:49 invocation
`52395f5749f44a7d846b34dd0181bd34` 成功：**十類／5,479,768 files／
130,128,002,184 logical B**，整體 scan 21.257 秒，其中 history 0.346 秒／773 reused。
CPU 25.135 秒，cgroup peak 76.06 MB；這些是本次原生採用觀察。與舊 605.554 秒／
13.5G 的全服務輪次相比，OS file／inode cache、背景負載與即時來源都不同，
**不能把整體差值或 cgroup peak 當受控全流程／RAM 節省倍數**；受控結果只限上表。
沒有清 kernel cache、降低更新頻率或縮減資料群組。

六個 quote／execution／acquisition／web protected owners 的 PID／Invocation 不變；
原 events／orders／fills 共 624,595,919 bytes 的完整前綴 SHA 均不變。197 項相關
回歸（9.42 秒）、Ruff、diff whitespace 通過；不是整個 repository suite。
正常下載主路徑有完整模擬 API／單登入／原 receipt 驗證，但原歷史 owner 正在其
connection reservation 等待窗，**尚未用額外登入強造下一份真實下載 mutation 證據**。
新碼已位於它的下一批 canonical module 入口；實際新資料 writer receipt 另續驗。

04:58 再取樣全部 87 installed services＋本次一個 engineering transient、67 timers、
3 paths，資源區間 20.887 秒；本次 `resource_pressure_findings` 為空，不代表全部
資料或業務已完整。FinMind 0.773 core、永豐 TAIFEX dashboard 0.280 core 是當時
主要服務負載；後者約 4.42 GB 是 cgroup file cache，不是同量 Python heap。
整板 GPU 2%／3,313 MiB 為瞬間觀察，未完整歸因 Windows／WSL 程序，不能當作
本輪 cache 省下多少 VRAM 的證據。這份 census 供下一個原 owner 優化使用。

- [固定最終程式的完整 ABBA](../artifacts/operations/service-cache-compaction/shioaji-storage-epochs-20261016/final-history-abba.json)
- [首次填入與原版參照比較](../artifacts/operations/service-cache-compaction/shioaji-storage-epochs-20261016/complete-history-abba.json)
- [773 原 manifest 獨立 SHA 回讀](../artifacts/operations/service-cache-compaction/shioaji-storage-epochs-20261016/enrollment-independent-readback.json)
- [原生 owner、來源函式 AST、帳本及範圍驗收](../artifacts/operations/service-cache-compaction/shioaji-storage-epochs-20261016/acceptance.json)
- [197 項最終測試](../artifacts/operations/agent-workflow/runs/shioaji-storage-final-regression-20261015T205254-0bc86d83/run.log)
- [04:58 全原生服務取樣](../artifacts/operations/running-services-optimization-20261013/all-native-services-after-storage-epochs-20261016.json)

重測原資料，不建立完整副本：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_shioaji_storage_epochs.py \
  --reference-root artifacts/operations/service-cache-compaction/shioaji-storage-epochs-20261016 \
  --cache-dir artifacts/live/shioaji_storage/.inventory-epochs \
  --output artifacts/operations/service-cache-compaction/shioaji-storage-epochs-20261016/retest.json \
  --reuse-only

# 人工越過原 downloader 修改過 history 時，立即完整對帳；不呼叫 provider。
run_fintech_python scripts/update_shioaji_storage_summary.py --force-full-scan
```

全 consumer pins／整體硬容量、其餘 closed fixtures／歷史帳本／舊 hot 精確退休、
其他來源群組的增量清點，以及真實新來源 Panel chunk receipt 仍是 active 工作。

## 10/16 05:20 續修：TAIFEX 曲線投影重用固定歷史與小型增量

### 修的是原投影 writer，不是刪除完整帳本

TAIFEX web 每次真實分鐘 append，原欄式 cache 雖只解析新 NDJSON，仍把完整
歷史 Parquet 重新編碼／fsync。現在沿原 `history_scan`、原 per-source flock 及
canonical projector，改為 versioned `base_delta_v2`：最多引用一份固定 base 與
一份小型 delta；delta 對應來源累積超過 32 MiB 才合併一次。沒有新增 collector、
排程或 broker session，也沒有更改行情頻率、價格、資金／成交公式或公開 schema。

原 v1 payload 經完整 SHA 驗證後，以同 inode 導入 base，不再複製 37.78 MB
既存投影。之後保留 base，只編碼舊小 delta＋真實新 tail；只有上一份已驗證 receipt
明列、且新 receipt 已不再引用的私有派生物可以回收。未知檔案、v1、原帳本及
backfill receipts 均保留。這個「最多兩份引用」不是全 cache 目錄硬 byte budget，
崩潰／I/O 失敗留下的未引用物也不假裝已完成全樹退休。

來源改變仍校驗**完整舊前綴 SHA256**，保留 dev／inode／size／mtime／ctime fences；
原地修改、截斷、替換、壞 receipt／payload 回原來源重建，壞來源保留原 parser error。
損壞 cache 不能變成成功空集合。以原 flock single-flight、pinned FD、durable／atomic
manifest 提交；v1 receipt 也改為有界 no-follow regular-file 讀取，FIFO 不再卡住。
[Polars streaming sink](https://docs.pola.rs/api/python/stable/reference/api/polars.LazyFrame.sink_parquet.html)
保留完整既有 schema、nullable／Float64／行順序，不使用 GPU、lossy 格式或縮減資料。

### 全流程比較，不只計算省了多少寫入

重用原先完整 SHA 驗證的約 4.99 GB 真實 source fixture；沒有另複製 JSONL／DB。
prefix-only seed 由既存 verified projection 推出，generation 是明列的工程 prefix
metadata，不冒充原生歷史 writer receipt。新增的仍是原捕捉 tail，沒有捏造觀察值。
原程式從當時 clean HEAD 保存；四執行緒、角色相同 allocator 的 fresh workers
A/B/B/A ×2，七區間完整 JSON（包含固定 observer clock）等於獨立舊控制。

| 完整七區間，八輪中位數 | 原 v1 writer | base／delta writer |
| --- | ---: | ---: |
| Update wall | 13.700 s | 9.782 s |
| Update CPU，含 native threads | 19.612 s | 14.757 s |
| New Parquet bytes | 30,616,012 B | 16,939 B |
| Fresh-process peak RSS，含後續 warm sequence | 999.73 MB | 987.10 MB |
| Subsequent complete seven-range warm wall | 5.778 s | 7.538 s |
| Subsequent warm CPU | 10.662 s | 11.871 s |

**暖讀單獨有退步，不隱藏。** 每次 trial 的 update＋後續完整 warm sequence 合計，
wall 中位數 **20.179 → 17.286 秒**，CPU **30.965 → 26.628 秒**；整體仍改善，
所有暖讀均零 Parquet 寫入。不是所有 query 都有同倍率提速，亦非全機或遊戲 FPS
驗收；source 變動時的 4.38 GB prefix proof 仍是下一個架構成本，沒有換成 mtime 猜測。

### 原生採用及未完成邊界

先隔離 prewarm，再 qualification 後**只重載原唯讀 web**；新 invocation
`8f9b76f30a874e9dbce79e022fd41934`／PID `1365525`。原 TAIFEX BidAsk、TW paper
及 Discord owners 不變；六份原始帳本／replay 共 **6,220,453,689 B** 的完整前綴
SHA 不變，沒有 broker orders 或來源改寫。

原服務實際打開兩個 `.segments-v2/part-*.parquet` 路徑及相同 verified inodes。
獨立讀回 2,557,389 rows／37,777,288 B 的來源／payload SHA；HTTP 七區間皆通過
原 all-strategy clock 檢查，端點為 `2026-10-15T20:59:15.422240+00:00`。
當下是夜盤已結束，`health=waiting` 正確，不冒稱正在串流。

另以**保存的舊 reader**讀當前完整來源，逐欄核對原生七個公開結果；只有請求觀測
時間 `generated_at_utc` 不同，所有來源時計、完整曲線、順序及 counts 都完全一致。
142 項共享回歸、Ruff／shell／diff checks 通過；不是全 repo suite。測試開發中一次
錯置斷言誤讀測試 FIFO 已終止並修正；另把只讀也會變化的 atime 排除於「未改寫」
比較，保留 dev／inode／size／mtime／ctime。失敗收據未覆寫。

**尚未聲稱自然新來源 writer 已驗收。** 休市後沒有為證據重寫帳本；下一個交易時段
仍要觀察原生小 delta 更新／compaction。全 consumer pins、硬容量上限、其他封閉
fixtures／歷史 ledger／舊 hot 退休、Panel 自然新來源 receipt 及全服務總目標仍待續。
可設 `STOCKAGENT_TAIFEX_HISTORY_COLUMN_LAYOUT=v1` 經原唯讀 owner 回退；原來源不動。

05:20:39 的後續全服務取樣中，原新 web PID 為 **0.00183 core**、匿名記憶體
147.05 MB，取樣間 cgroup I/O 增量為零；是當次已休市／無曲線 rebuild 的觀察，
不能與先前活躍分鐘的 0.28 core 或不同 cgroup file-cache charge 直接作因果倍率。
目前 PID 未映射 Torch／CUDA libraries，也不據此聲稱 Windows 整卡 VRAM／遊戲
FPS 已最佳化。全服務 source readiness 與此 scoped acceptance 分開。

證據：

- [八輪完整 update＋warm 比較](../artifacts/benchmarks/running_services_20261013/taifex-base-delta-20261016/final-runtime-comparison.json)
- [首次四輪比較，與正式設定分開](../artifacts/benchmarks/running_services_20261013/taifex-base-delta-20261016/measurement.json)
- [Qualified prewarm](../artifacts/operations/running-services-optimization-20261013/taifex-base-delta-qualified-prewarm-20261016.json)、[只重載 web／帳本與 owners](../artifacts/operations/running-services-optimization-20261013/taifex-base-delta-qualified-reload-20261016.json)
- [原生 reader／七區間／完整 SHA](../artifacts/operations/running-services-optimization-20261013/taifex-base-delta-native-readback-20261016.json)、[當前完整公開欄位對舊 reader](../artifacts/operations/running-services-optimization-20261013/taifex-base-delta-native-value-oracle-20261016.json)
- [142 項共享回歸](../artifacts/operations/agent-workflow/runs/taifex-base-delta-final-shared-t-20261015T211615-ee33af90/run.log)
- [Scoped acceptance，不是全目標完成](../artifacts/operations/running-services-optimization-20261013/taifex-base-delta-scoped-acceptance-20261016.json)、[後續全服務取樣](../artifacts/operations/running-services-optimization-20261013/all-native-services-after-base-delta-20261016.json)

## 10/16 續修：現役快取引用補齊與共用分鐘路徑

### 先補完整引用，避免容量管理造成 GPU 重算

原 `signal_engine._live_forward_outputs` 可產生主要 masked forward 與 optional
unconstrained forward；原 current-consumer census 只記主要 key，漏掉同次訊號
另一個有效依賴。現在沿原 `artifact_consumers` 記錄兩種 output role，重複 key
只形成一個 pin；optional contract／key／巢狀格式不可信時保留已觀測 key，並讓
整份容量計畫 fail closed，不把未知結果排成可刪除候選。

同一個 shared artifact-consumer gate 也補上原 completed-forward cache root 的
隱含服務依賴，保留 writer 的 override 語意；沒有擅自 strip／expanduser 或用
live output 路徑取代 cache root。原維護 owner 仍用同一份市場／selection／最新
signal pointer，沒有另一套 pin registry、清理 daemon 或模型／provider 工作。

本機即時盤點為 **5 個 current masked keys、31 份控制檔、189 份結構相容 commit 的
metadata、13,161,635,840 B namespace allocated**；該時點沒有 current
unconstrained role，新增情境由測試覆蓋，不宣稱現役已因此多回收空間。metadata
完整不等於本輪重新驗完每個 payload；容量計畫仍是 `eviction_allowed=false`。

### 將既有 producer 的路徑宣告共用，不為取常數載入估值工具

原依賴盤點為了 5 個本機分鐘來源／快取路徑 import 整個 minute-curve builder。
現在四個入口共用 `stockagent/live/minute_source_paths.py` 的原始宣告：原分鐘
curve builder、原 open-price replay、原缺口 recheck 與 shared consumer census。
兩個 producer 保留原 public constant 名稱的 alias；路徑值、相對性、順序、解析、
receipt 與價格邏輯不變。完整 AST 證明兩個 producer 只移動原宣告，另外兩個
reader 只改 import；共用模組只含原常數／Path，沒有資料讀取或計算。

保存四份原 source 共 **343,695 B** 作 reference，不另複製資料庫或行情資料。
八個 fresh processes 按 ABBA × 2 完整解析目前 **517 筆** dict；全部欄位、順序與
內容一致，SHA `96854746cc505d39bb14bd88e2b631302cdb841ec238e6f9a7eebc06be9c163a`。
本輪 517 筆相較上一輪 516 筆，是上面新補的 implicit forward-cache dependency，
不是丟掉原依賴來加速；本輪 baseline 與 candidate 均包含它。

| 完整 dependency resolver，四次平均 | 原分鐘工具 import | 共用路徑宣告 |
| --- | ---: | ---: |
| Wall | 0.9566 s | 0.9042 s |
| CPU | 0.9433 s | 0.8933 s |
| Fresh-process peak RSS 的平均 | 181,206,016 B | 171,544,576 B |

是約 **5.5% wall／9.66 MB RSS** 的小幅改善，不是整機同幅提速，也不拿上一輪
不同時間的 1.436 秒當本輪 baseline。主機非獨占、未清 OS cache，既有完整配置
驗證仍會載入 NumPy／Polars；新版只禁止無必要的 valuation／replay／model／
provider imports，所有 worker 禁止 network connect、CUDA 未初始化。最初以
「整份 resolver 禁止 NumPy，再禁止 Polars」為 guard 的試跑都明確失敗，指出
原 canonical config validator 的依賴；未為了通過而改寫或跳過該驗證。

這輪不更動 `signal_engine`、`market_config` 或 completed-forward writer 的模型
code identity；不做額外 GPU forward，也不把舊 keys 冒稱新模型驗收。硬容量上限、
全 fleet consumer 圖、封閉歷史帳本與舊 hot 的恢復／退休仍待後續驗收。

- [八輪完整清冊與 AST／計時證據](../artifacts/benchmarks/running_services_20261013/minute-source-paths-20261016/qualified-full-census-abba.json)
- [本輪原 source reference 身分](../artifacts/benchmarks/running_services_20261013/minute-source-paths-20261016/reference/original-code.json)
- [目前 metadata plan 與四個真實 native owners](../artifacts/operations/service-cache-compaction/forward-reference-coverage-20261016/final-before-native-owner.json)

### 正式原 owner 回讀與測試

原 `stockagent-artifact-dedup.service` 於 **05:37:12–05:38:49** 完成，invocation
`79a29b17269249839c0ded0d36d2c922`，Result=success／exit=0。完成的 forward
receipt 實際含新版 `output_kind`、current-consumer plan 完整且穩定，189 個
相容 keys／5 個 current pins／namespace allocated 在獨立前後盤點保持一致。
因無新 duplicate candidates，forward phase 雜湊 payload=0 B、替換=0；並不是
重新全讀 13.16 GB 後才說省計算。這個原 owner 本次完整 wall 96.40 秒、CPU
57.75 秒、cgroup memory peak 1.20 GB；其中 generic artifact 階段約 80.01 秒，
仍是後續可優化範圍，不拿 0.904 秒的 resolver 子流程冒充整個維護服務。

原 generic 階段沿既有 complete-marker policy 共用 75 個額外的 completed
benchmark 邏輯副本。獨立 readback 核對全部 75 個路徑、30 個最終 inodes 的完整
SHA 及 donor alias，對應 **41,881,785 B logical shared bytes**；沒有移除邏輯
名稱。此數字不直接等於 last-link 真正釋放的磁碟空間，也不是另做冷恢復退休。
本輪其他 raw source／ledger 不在這個 batch scope。

原 Discord、TW paper、TAIFEX BidAsk 與 TAIFEX web 的四份實際
loaded/active unit PID／invocation 在 native 維護前後不變；沒有重啟交易或 GPU
服務。另直接執行原 completed-cache 的五個純 CPU identity helpers，不載入
Torch／NumPy／Polars／provider，確認 **5 個現役 keys 均仍匹配原 126 份完整
程式身分**。這是 code-admission 保留，不冒稱本輪重新比較全部 tensor／source
就緒；原模型／全部資料運算沒有因此重跑。

最終共享回歸 **517 passed，307.55 秒**，涵蓋 current pin／unknown／race、
sharing、physical cache、retirement/cold recovery、minute curves、原 open-price
replay 與缺口 recheck。Ruff 九個相關 source/test 檔案及修改範圍 whitespace 通過。
早期紅燈、NumPy／Polars 過廣 guard、一次錯誤測試路徑（零測試）均保留，後續
用最終新命名完整重驗；初版 owner census 的 BidAsk unit 名誤植，不作正式行情
owner 保留證據，最終前後 census 明確要求四個真實 units loaded/active。

硬 byte budget、全 fleet 非目前訊號引用、自然新來源的 writer receipts，以及
封閉歷史帳本／舊 hot 恢復後退休仍未完成，總目標保持 active。

05:41:48 另沿原全服務 census 實測 20 秒：88 units（含工程 transient）、67
timers、3 paths；本輪 resource-pressure findings 為空，timer findings 為空。
這不是全機 idle／所有資料已完成：business receipts 仍列三個 crypto perpetual
steps 合計 23 個來源缺口、crypto-training source lease budget 等待與 TW cold
publish scan pending。另原 formal-history maintenance 仍正確拒絕把 factorized
的 211 日投影冒充完整 2014 起歷史；本輪沒有重啟它或縮短原請求來消除錯誤。

- [原每日 owner、程式身分與完成收據](../artifacts/operations/service-cache-compaction/forward-reference-coverage-20261016/native-daily-owner.json)
- [75 個新增 aliases 的獨立完整 readback](../artifacts/operations/service-cache-compaction/forward-reference-coverage-20261016/native-generic-independent-readback.json)
- [維護後同一份 current metadata census](../artifacts/operations/service-cache-compaction/forward-reference-coverage-20261016/final-after-native-owner.json)
- [5 個現役 keys 的 canonical code 身分保留](../artifacts/operations/service-cache-compaction/forward-reference-coverage-20261016/existing-key-code-admission.json)
- [517 項最終共享回歸](../artifacts/operations/agent-workflow/runs/forward-minute-path-qualified-re-20261015T213525-0666055c/run.log)
- [05:41 原全服務資源／資料狀態 census](../artifacts/operations/running-services-optimization-20261013/all-native-services-after-reference-reuse-20261016.json)
- [本輪固定範圍驗收與全部證據 SHA](../artifacts/operations/service-cache-compaction/forward-reference-coverage-20261016/scoped-acceptance.json)

## 10/16 續修：通用維護只解析需要的路徑

上輪原 daily owner 的 generic 階段約 80.01 秒包含實際 75 個 aliases 的重連，
不能直接當成不變資料的每日掃描成本。本輪先在既有資料樹執行原唯讀完整 CLI，
cProfile 量到 **33.716 秒／35,882,190 function calls**；其中 completion census
**31.115 秒**，`os.walk` 約 457,110 次，單純 Path equality 的 cumulative 就約
8.82 秒。sample 約 0.83 秒，完整 hashing 不是這次主要瓶頸。

原 `artifact_dedup._artifact_completion_roots` 改成有界 DFS／scandir：只保留目錄
名稱與一個 marker 是否存在，只有真的找到 marker 才建 Path／讀 JSON。每次仍
完整走所有非排除分支、重新讀 marker，保留降序 stack 的確定性 traversal 與
下降前 fresh symlink check；新 nested marker、原檔原位改寫並還原 mtime、
running parent 下的 completed child 都不靠舊索引判定。未建立第二個 catalog、
hash cache 或大型 topology sidecar，也沒有用幾十萬個目錄 mtime 推測整樹未變。

保留原 20,368 B module 作 reference，不複製來源／測試 DB。新的可重跑 benchmark
呼叫原本完整 `find_duplicate_groups`，不另寫一套選取規則；固定 eligibility clock、
四個 fresh workers A/B/B/A。除了 25 群組／所有路徑與 12 項 counters，亦對完整
dataclass metadata 計 SHA，四輪全部相同。完整 AST 比較確保其餘 eligibility、
sample、完整 SHA、硬連結 apply 都沒有更動。

| 完整唯讀稽核平均 | 原版 | 新版 |
| --- | ---: | ---: |
| wall | 25.1444 s | 20.4426 s |
| CPU | 24.1745 s | 17.7328 s |
| peak RSS | 32.860 MB | 30.859 MB |

這是同機非獨占／未清 OS page cache 的 ABBA，case 本身 wall 有 15.60–29.48 秒
波動；平均 wall -18.70%、CPU -26.65%。完整候選仍 **5,247 檔／12,344,144,087 B**，
136 complete／143 blocked markers、1,425 sampled distinct inodes／52 full hashes。
27 個尚可共享名稱不是來源缺口，也不等於已釋放的實體 blocks。

聚焦 37 項與較廣 **155 項／6.84 秒**皆通過，兩者重疊不可相加。新增測試覆蓋
無 Path-per-directory、fresh nested/state changes、symlink replacement／directory
marker、unreadable branch；原 source sharing、三階段 CLI、panel 原退休邊界一併
回歸。Ruff／whitespace 通過；不是全 repository suite 或全服務已完成。

**原 owner 採用與獨立回讀：**05:53:03–05:53:47，原
`stockagent-artifact-dedup.service`／原 flock／三階段 wrapper invocation
`242bc45da383468b93ad92761e370b0e` 成功。完整 wall **44.090 秒**、cgroup CPU
**33.021 秒**、cgroup MemoryPeak **115,691,520 B**；其中 generic **17.259 秒**。
該次與上輪 96.40 秒的 apply 批次不同，不是同輸入的整服務加速倍率，亦不把
cgroup peak 直接當 Python RSS。相同輸入的收益以上方完整唯讀 ABBA 為準。

原 owner 這次共用剩下 **27 個 completed aliases／5,889,482 logical B**，零
skipped；generic report 的完整群組及全部 counters 仍等於固定 baseline。25 個
final inodes 的獨立 full SHA／permissions／xattrs／before-after metadata 回讀
通過，名稱全保留；logical shared bytes 不是最後 link 的 allocated blocks 回收量。
Discord、TW paper、TAIFEX BidAsk 及 TAIFEX web 的 PID／invocation 保留；前後
517 筆依賴、5 current pins／189 commit metadata、namespace allocated
13,161,635,840 B 不變。模型 code admission 另依原 canonical helper 回讀，不
啟動 GPU 或對來源重新下載。此輪只驗收這個既有 owner，不是全 fleet 容量上限、
完整冷恢復、自然新來源 append 或全總目標完成；原任務保持 active。

05:55:55 原全服務 auditor 再觀測 88 services／67 timers／3 paths，這個取樣窗的
resource-pressure／timer-schedule findings 為空。它仍列 crypto training source
lease 等待、三個 perpetual backfill steps 合計 23 個來源缺口、TW cold publication
scan pending；未把監測成功當成資料全齊、全機 idle 或遊戲幀率證明。

- [完整 reference／candidate audit ABBA](../artifacts/benchmarks/running_services_20261013/generic-artifact-scan-20261016/full-audit-abba.json)
- [保留原 source 身分](../artifacts/benchmarks/running_services_20261013/generic-artifact-scan-20261016/reference/reference.json)
- [原完整 CLI profile 收據](../artifacts/benchmarks/running_services_20261013/generic-artifact-scan-20261016/reports/artifact-dedup-artifacts-20261015T214818.135957Z.json)
- [155 項共享回歸](../artifacts/operations/agent-workflow/runs/generic-artifact-shared-regressi-20261015T215128-c46b15a6/run.log)
- [原 daily owner 的新 invocation](../artifacts/operations/service-cache-compaction/generic-census-20261016/native-daily-owner.json)
- [27 個 aliases 獨立完整回讀](../artifacts/operations/service-cache-compaction/generic-census-20261016/native-generic-independent-readback.json)
- [維護後 current references 與 protected owners](../artifacts/operations/service-cache-compaction/generic-census-20261016/after-native-owner.json)
- [現役模型原 canonical code identity](../artifacts/operations/service-cache-compaction/generic-census-20261016/existing-key-code-admission.json)
- [05:55 原全服務重新觀測](../artifacts/operations/running-services-optimization-20261013/all-native-services-after-generic-census-20261016.json)
- [本輪固定範圍驗收與證據身分](../artifacts/operations/service-cache-compaction/generic-census-20261016/scoped-acceptance.json)

## 10/16 續修：引用掃描不完整時保留資料

**根因與範圍：**原 shared scanner 對 live links、FD、inode 與 maps 的所有
`OSError` 都忽略，orchestrator scanner 也忽略不可讀 cmdline。權限／I/O 故障
因此可能被當作無引用，不能據此執行容量退休。本機本輪 baseline 只觀察到
20 次正常 `ENOENT`／`ESRCH`，**沒有證據顯示本次曾因權限盲區誤刪資料**；
這個問題由故障注入重現，原 23 項測試有 21 項失敗，保留原失敗收據。

修改原 `materialized_cache` 與 `artifact_maintenance`，共用明確的程序枚舉／
錯誤語意：個別程序或 FD 消失可略過；`/proc` 本身缺失、權限與 I/O 不完整
則拋 `SnapshotError`。維持成功時的 bounded list API、精確 sibling 邊界、
hardlink inode／deleted mmap 與 ancestor orchestrator 判定；不是另建 process
registry 或放寬系統權限。進行中的 writer／最終退休鎖與精確冷恢復仍各自必要。

**驗證：**333 項共享回歸、77 項 transport／return／recovery 回歸、另 87 項
focused 回歸通過；focused 與共享有重疊，不相加稱不同測試。故障注入確認
過期 materialization 的 payload／lease／current link 保留，forward sharing
保留原 inode／bytes 並記錄 `failed`，容量觀察不會回傳虛假的乾淨清冊。
新增測試也涵蓋合法退出、缺失 namespace、hardlink 與 deleted mmap；未跑全 repo。

以既有 189 commit／5 current keys／13,161,635,840 allocated B 的**完整**
metadata、current pins、process working-set 流程做 fresh-worker ABBA，四份完整
計畫（只排除觀察時鐘）完全一致，無 payload hash／模型或網路工作。平均 wall
**1.7986 → 1.8762 秒**、CPU **1.7445 → 1.8094 秒**，並非提速證據；主機
非獨占且未清 OS cache。本輪接受的是安全修復，不把小幅時間差包裝為效能收益。

06:06 原三階段 daily owner invocation `57f5cb182264469d944ba58ff1e5f8db`
成功，完整 wall **25.573 秒**、cgroup CPU **23.574 秒**、MemoryPeak
**135,352,320 B**。批次與上一輪不同，不能稱整服務加速倍率。未新增 scheduler、
未替換 payload、未刪 logical path；四個 protected owners、517 筆依賴、
5 current keys 與 126 份 canonical 模型 code identity 由前後回讀保留。
只證明原 fresh daily owner 採用，不代表所有長駐 worker 已重載此 helper。

**下一個架構步驟，尚未實作／啟用：**

1. 原 writer 在昂貴計算前，以 namespace 鎖預留 byte budget，計入 unknown
   allocations、所有硬連結與進行中 reservation；已有有效結果直接重用。
2. 熱區 miss 先找固定、已驗證的 canonical cold 身分，精確恢復再讀；已知 cold
   不可用時保留 deferred/error，不把它當作理由重算 GPU。
3. 容量回收只處理已完成且可精確恢復的衍生結果，持有原 key／writer 鎖，重查
   現役／in-flight 引用；unknown／incomplete／來源／帳本全部保留。容量觀察
   和 sampled use 不是自動 eviction 授權。

以上新 cold／budget lifecycle 必須接回既有 owner、catalog、原子提交與原恢復
流程並做 pressure／race／restart 驗收；不能以刪掉快取或縮小歷史範圍完成目標。
五區域的全 namespace 硬上限、封閉歷史／舊 hot 與自然 writer receipts 仍待續。

- [當期未觀察到權限盲區的 baseline](../artifacts/operations/service-cache-compaction/reference-visibility-20261016/baseline-visibility.json)
- [原故障測試 21 failed／2 passed](../artifacts/operations/agent-workflow/runs/reference-visibility-red-tests-20261015T220221-bdfa234a/run.log)
- [333 項共享回歸](../artifacts/operations/agent-workflow/runs/reference-visibility-shared-test-20261015T220508-c657ac98/run.log)
- [77 項 transport／return／recovery 回歸](../artifacts/operations/agent-workflow/runs/reference-visibility-transport-r-20261015T220836-34058727/run.log)
- [87 項最終 focused 回歸](../artifacts/operations/agent-workflow/runs/reference-visibility-adversarial-20261015T220740-8de91cc9/run.log)
- [完整 fresh-worker 計畫 ABBA](../artifacts/operations/service-cache-compaction/reference-visibility-20261016/complete-plan-abba.json)
- [原三階段 native owner](../artifacts/operations/service-cache-compaction/reference-visibility-20261016/native-daily-owner.json)
- [原服務後獨立引用回讀](../artifacts/operations/service-cache-compaction/reference-visibility-20261016/after-native-owner.json)
- [原 canonical 模型 code admission](../artifacts/operations/service-cache-compaction/reference-visibility-20261016/existing-key-code-admission.json)
- [本輪綜合驗收](../artifacts/operations/service-cache-compaction/reference-visibility-20261016/scoped-acceptance.json)

## 10/16 續修：容量准入核心與完整 CPU 提交實測

新增 `completed_forward_capacity`，作為**原 writer 的 miss 邊界原型**：短暫
namespace 鎖只負責盤點／預留，昂貴計算與耐久提交期間改由各工作自己的
kernel flock 保持預留，不持有全域鎖。按實際 inode／`st_blocks` 計量，包含
目錄、未知檔案及全部進行中工作的完整成長上界；外部 hardlink 不給回收額度。
程序退出只可回收 schema／inode／root 身分都吻合的控制記錄，從不刪資料。
其他容量政策仍有工作時延後切換，損壞／共享／重導記錄不當作空目錄。

第一輪五份現行結果的完整 CPU 流程，未加控制平均 wall 0.6265 秒；耐久
控制記錄反而需 0.8972 秒。控制記錄不承載原始值或永久提交，活性由核心鎖
決定：同 boot 當機可辨識 stale；斷電後重新盤點實際配置，不沿用不存在的
工作。因此只改控制記錄為原子、非耐久寫入並取消 unlink 的 dir fsync；
**原 payload／receipt 的 durable=True 不變**，不是省略資料驗證。

凍結原版本後，以六個 fresh CPU workers 做 baseline／durable／atomic／
atomic／durable／baseline；每個 worker 都完整讀取五個**現行**輸出，包含
clone、序列化、原 producer 耐久提交、完整 warm read、所有 tensor bits／
metadata 比對與自己的副本退休。平均結果如下：

| 完整五輸出 CPU 流程 | wall 秒 | process CPU 秒 | private allocated B |
| --- | ---: | ---: | ---: |
| 原 producer，無容量控制 | 0.4598 | 5.8371 | 2,506,752 |
| 原型，耐久控制記錄 | 0.7865 | 9.7121 | 2,506,752 |
| 原型，原子控制記錄 | 0.4371 | 5.4884 | 2,506,752 |

主機非獨占、OS cache 未清；相對無控制的 0.023 秒差距不作加速結論。
CPU 使用包含 Torch 多執行緒，不能當單核 wall。此結果是**完整現行 CPU
儲存交易**，沒有模型、GPU 或 provider 工作，也不是整體推論／全機收益。
五份現行訊號投影合計只有約 2.5 MB；歷史 namespace 約 13.16 GB 與舊完整
aux 的 13–842 MiB，不能用來推測每個現行輸出的寫入量，也不能據此縮小
保護所有合法輸出所需的 1 GiB 上界。

另在真實完整 namespace 做 durable／atomic／atomic／durable 控制流程，
平均 wall **57.16 → 14.61 ms**，CPU **19.31 → 14.61 ms**。原檔案世代、
5 current keys 及四個 protected owners 不變，只留下零 byte 的 namespace
鎖；沒有 payload 讀取／模型／資料退休。這一項不是完整資料提交實測。

最終 **260 tests passed／15.36s**，包括獨立程序正常退出／當機、並行工作、
跨政策切換、錯誤／權限／路徑與控制記錄故障、完整 producer 交易及副本
保留。最初 barrier 測試 teardown race 與後來讀取後 atime 改變的測試誤判，
都保留失敗收據；修正只針對斷言／清理同步，不放寬 bytes／inode／mutation
metadata。自己產生且已驗證的 private benchmark 副本已移除，原快取保留。

**部署邊界：**核心與測試已完成，正式 writer 尚未呼叫、沒有安裝生產容量
政策，不聲稱已硬限制整個 namespace。需要把固定版本的 canonical cold
發布／獨立恢復、cold-first miss、所有 writer 的完整成長上界與引用 gate
一併接回既有 owner，再做完整正式模型准入／服務切換；不能用容量拒絕後
另開 GPU 重算、刪 unknown 或複製一套冷庫來完成。總目標維持 active。

- [完整 CPU ABCCBA 原始收據](../artifacts/benchmarks/running_services_20261013/forward-capacity-20261016/atomic-control-transaction-abccba.json)
- [初版完整 CPU ABBA（較慢版本仍保留）](../artifacts/benchmarks/running_services_20261013/forward-capacity-20261016/complete-transaction-abba.json)
- [真實全 namespace 控制實測](../artifacts/operations/service-cache-compaction/forward-capacity-20261016/real-namespace-bookkeeping.json)
- [260 項最終回歸](../artifacts/operations/agent-workflow/runs/forward-capacity-atime-regressio-20261015T222944-b6cbca3d/run.log)
- [保留 atime 斷言失敗](../artifacts/operations/agent-workflow/runs/forward-capacity-atomic-control--20261015T222612-9a22b352/run.log)
- [保留 barrier teardown 失敗](../artifacts/operations/agent-workflow/runs/forward-capacity-kernel-tests-20261015T221703-3d8f8262/run.log)

## 10/16 續修：原檔直接發布，避免封存前的工作副本

原 `publish_packed_snapshot` 與 `publish_data_releases` 新增 opt-in
`included_root_files`。每次明確指定至多 4096 個直屬 regular files，原地完整
讀取與驗證，不做 `copytree`／全目錄 census，也不把同目錄其他 key 加進去。
manifest 記錄選取範圍；來源根被替換、指定檔變動、遺漏、symlink、錯誤清單
或和 omission filters 混用均拒絕。全樹發布的預設語意維持不變；不同 scope
或不同選取證明不能因內容恰好相同而誤認同一 release。原 catalog 權限、
publish 開關、來源鎖與退場門檻仍適用，沒有另建冷庫格式或常駐服務。

重用既有 benchmark，以五份**真實現行** payload／receipt，做 fresh-worker
ABBA：修改前凍結發布器＋兩檔 staging，對照新版原檔直接發布。每輪都包含
來源完整 SHA、canonical 本機 packed 發布、依固定 snapshot ID 完整恢復、
獨立全 bytes 比對及僅退休本次已驗證的 private 副本，不只測 hash／複製片段。

| 完整五-key 私有冷存交易 | 原 staging | 指定原檔 |
| --- | ---: | ---: |
| 交易 wall 平均（秒） | 0.8898 | 0.8828 |
| process CPU 平均（秒） | 0.2047 | 0.1858 |
| 額外 staging payload 寫入（B） | 2,468,849 | **0** |
| private regular inode 配置（B） | 7,553,024 | 5,066,752 |

配置數字不含目錄 blocks；0 只指**發布前 staging**，不代表冷物件／恢復不寫入。
前一輪 wall 0.9172 → 0.7340 秒，但最終凍結版本重測差距縮至約 0.007 秒，
**不宣稱穩定 wall 加速**；確定收益是取消額外 staging 寫入與工作副本。
主機非獨占、OS cache 未清；不是原生 D 吞吐、GPU 推論或全機收益。五個現役
keys、所有十份原檔及四個 protected owners 保持不變；自己的已驗證測試副本
已移除，原資料可用性與現行帳本未改。

421 項 focused 回歸通過。較廣測試另重現兩個既有假發布器未執行 owner callback
的失敗：使用修改前 packed 發布器也同樣失敗，並非這次選取功能的回歸。
修正測試替身並新增實際發布器測試，5 項鎖定契約測試通過：完整私有驗證／
獨立恢復在 shared owner 外，來源重查／正式 commit 及最後精確退休仍在鎖內。
沒有移除來源檢查或把昂貴 I/O 重新包回全域鎖；完整共享回歸另列收據。
最終共享回歸 **463 passed／308.45s**，涵蓋 packed publish／fetch、catalog、
materialized／retention／backup、native reads、legacy 發布／退休／scratch
以及 forward 容量／compaction／usage；與上述 focused 測試重疊，不相加。
Ruff、修改範圍 diff check 及本節證據連結檢查通過；不是全 repository 驗收。

**仍待接入：**正式 forward 的 catalog、固定 cold binding／pin、miss 先恢復，
以及所有 writer 的成長上界／容量政策，之後才可正式切換與退休歷史快取。
本次沒有新增正式 catalog 項目、D 發布、模型／provider 呼叫或交易服務重啟；
不宣稱五樹架構或總目標已完成。

- [最終凍結版本完整 ABBA 與版本綁定](../artifacts/benchmarks/running_services_20261013/forward-cold-selection-20261016/complete-cold-pipeline-accepted-abba.json)
- [較早完整 ABBA（保留量測波動）](../artifacts/benchmarks/running_services_20261013/forward-cold-selection-20261016/complete-cold-pipeline-final-abba.json)
- [421 項 focused 回歸](../artifacts/operations/agent-workflow/runs/forward-cold-final-focused-regre-20261015T224352-0b625c5b/run.log)
- [修改前發布器重現同兩項舊測試失敗](../artifacts/operations/agent-workflow/runs/forward-cold-legacy-frozen-reche-20261015T224644-0aa72986/run.log)
- [5 項實際／模擬 owner 契約測試](../artifacts/operations/agent-workflow/runs/forward-cold-owner-contract-regr-20261015T224708-0a8efe86/run.log)
- [463 項最終共享回歸](../artifacts/operations/agent-workflow/runs/forward-cold-final-shared-regres-20261015T224724-9330c181/run.log)

## 10/16 續修：固定 pin 直接還原，不再複製第二棵熱工作樹

原全樹 fetch 的 blob／pack 複製抽為共用引擎，原全樹的目錄／symlink、完整
fingerprint、READY、耐久與失敗保留不變。新的 `stage_packed_root_files` 只接受
完整明列的 bounded root-file release：原 consumer namespace 內建立獨占平面
暫存名，逐檔完整 SHA／mode／inode／root 身分回讀，不建另一份 managed tree。
失敗留下精確暫存且繼續計入容量，不把半成品當提交，也不刪 unknown。

`resolve_packed_pin` 沿用既有四欄 standard pin 格式，有限讀取、重複 JSON 欄位
拒絕、核對 manifest 完整內容及 SHA，再解析**固定 snapshot ID**。最新 head
已前進仍還原原 pin；pin／cold／mount 缺失或變動是錯誤，不能轉成 GPU miss。
這不是新增第二個 registry、sidecar 格式或常駐 owner。
指定的冷庫路徑先通過其 authority／mount guard，才解析轉址；若解析後的位置
不同，再檢查該目標，避免先 resolve 就失去原 canonical 位置的檢查條件。

還原持有原 publication／retention 鎖。故障注入另外重現四個新原型競態：
等待鎖或複製期間，owner 檔名被移除／替換／改為 symlink，舊 FD 不保護新的
owner。已改成只開啟既存 regular inode、不可 create／truncate／follow，進鎖與
離鎖重查同一身分及 D mount；異常保留未提交 stages。原 publisher／全樹 fetch
的鎖定策略未改，不能稱所有既有 lock helper 均已硬化。

同一共用 copier 另補齊成長預留的必要前提：來源若在驗證後變大，最多多讀
一 byte 判斷並在寫超出預留量前停止，不能等整個超大物件寫完才報 checksum。
原 durable 路徑的 chmod 在 fsync 後，也改為完整 bytes／SHA 驗證及 FD 上
最終 mode 設定後再做**同一次** fsync，不增加第二個 barrier。兩項先重現失敗
再修正；這是程式順序及故障測試，不冒稱所有 filesystem 的實體斷電驗收。

重用既有 benchmark，五份真實現行輸出的四 fresh workers ABBA，每輪包含：
完整來源 SHA → canonical 指定原檔發布 → standard fixed pin → 原 key lock／
完整成長預留 → 全 cold 驗證與還原 → 原 v3 CPU decoder／全 tensor bits 和
metadata 比對 → payload 先、receipt 後耐久提交 → 原 reader 的完整 warm hit →
只退休本次 exact private 副本。原模型／網路 import 與 compute 禁止，不只是
片段 memcpy 微測試。兩組均保留完整原輸出，沒有縮成訊號值來取得收益。

| 完整五-key 私有 CPU 交易，最終凍結版 | 原 managed 還原再複製 | 直接還原 |
| --- | ---: | ---: |
| 還原 payload 寫入（B） | 4,937,698 | **2,468,849** |
| private regular inode 配置（B） | 7,593,984 | **5,087,232** |
| 交易 wall 平均（秒） | 1.3169 | 1.0260 |
| process CPU 平均（秒） | 0.4332 | 0.3752 |
| 含 fresh process 啟動 wall 平均（秒） | 3.8601 | 3.5394 |

配置不含目錄 blocks；寫入數字只計還原 payload，非全部 metadata／cold publication。
主機非獨占、未清 OS page cache；較早一輪交易 wall 1.5622 → 1.0046 秒、CPU
0.4041 → 0.2993 秒，但前一凍結版 CPU 0.4896 → 0.5290 秒、process-wall
5.8683 → 5.9189 秒沒有改善。因此最終版上述時間雖有界改善，仍只接受
**少一份完整還原副本／寫入減半**，不宣稱穩定速度、CPU、D HDD 或全機收益。
五個 current keys／十份原檔與四個 protected owners 保留；獨立 code admission
另按原 126 份 canonical code 檢查。正式 writer 尚未採用這兩個 API。

首輪完整 consumer benchmark 的兩個失敗，是工具保留自己的 mmap tensor 到
退休時，安全 gate 正確拒絕回收。修正為完成完整比對後釋放工具的讀取物件，
**沒有移除 `/proc/self/maps` 保護**。故障收據及原鎖競態 red tests 保留；成功
focused／共享回歸與最後版本身分以綜合驗收收據為準，測試有重疊不相加。

**仍未完成：**正式 catalog per-key 登錄／pin 生命週期、cold-first 接入原
`cached_forward`、所有 writer 的 full-growth 預留及容量政策、完整模型／原生
服務採用驗收、封閉 live／舊 hot 退休。沒有發布到正式 D、重啟交易服務或新增
GPU／provider 工作，不把本機 private cold proof 當成原生恢復或全總目標完成。

- [最終版完整 fixed-pin consumer ABBA](../artifacts/benchmarks/running_services_20261013/forward-cold-restore-20261016/complete-fixed-cold-restore-authority-abba.json)
- [前一版 mode／size 修正 ABBA](../artifacts/benchmarks/running_services_20261013/forward-cold-restore-20261016/complete-fixed-cold-restore-durable-abba.json)
- [前一凍結版 ABBA（CPU／process wall 未改善）](../artifacts/benchmarks/running_services_20261013/forward-cold-restore-20261016/complete-fixed-cold-restore-final-abba.json)
- [較早 ABBA（保留波動）](../artifacts/benchmarks/running_services_20261013/forward-cold-restore-20261016/complete-fixed-cold-restore-abba.json)
- [267 項 packed／pin／還原共享回歸](../artifacts/operations/agent-workflow/runs/forward-cold-fixed-pin-and-stage-20261015T230310-cd7469f1/run.log)
- [工具 readback 生命週期修正後 311 項回歸](../artifacts/operations/agent-workflow/runs/forward-cold-consumer-release-re-20261015T230815-b7e5de5a/run.log)
- [四個 owner 世代競態 red tests](../artifacts/operations/agent-workflow/runs/forward-cold-owner-generation-re-20261015T231015-c0e65133/run.log)
- [owner 修正後 245 項 focused 回歸](../artifacts/operations/agent-workflow/runs/forward-cold-owner-generation-gr-20261015T231038-d7998462/run.log)
- [mode／fsync 順序的原兩項失敗](../artifacts/operations/agent-workflow/runs/forward-cold-mode-before-fsync-r-20261015T231533-c48812be/run.log)
- [超出預留量的原失敗](../artifacts/operations/agent-workflow/runs/forward-cold-copy-admitted-size--20261015T231650-9fd54a1d/run.log)
- [修改前原 copier 同樣重現三項失敗](../artifacts/operations/agent-workflow/runs/forward-cold-original-copy-red-p-20261015T231958-80d11844/run.log)
- [原指定 authority admission 順序的兩項失敗](../artifacts/operations/agent-workflow/runs/forward-cold-lexical-authority-r-20261015T232121-620e7869/run.log)
- [最終版 254 項 focused 回歸](../artifacts/operations/agent-workflow/runs/forward-cold-authority-final-foc-20261015T232140-3cf826d0/run.log)
- [共用 copier 修正後 568 項共享回歸](../artifacts/operations/agent-workflow/runs/forward-cold-durable-final-share-20261015T231711-65c566f6/run.log)
- [最終共享回歸](../artifacts/operations/agent-workflow/runs/forward-cold-authority-final-sha-20261015T232139-e72c0827/run.log)
- [本輪綜合驗收及原檔／模型 code／owner 身分](../artifacts/operations/service-cache-compaction/forward-cold-restore-20261016/scoped-acceptance.json)

## 10/16 續修：完整結果逐 key 正式登錄與冷庫重用

本輪將上一節共用能力接入原 `publish_data_releases.py`，不是另一個封存器或
定時服務。單位是原完整 v3 identity 的 **64 位 key＋原 output／receipt 兩檔**，
不先複製工作樹、不掃其他模型世代、不把命中次數／lock／未知原始資料封進去。
持有產生端同一個既存 key 鎖；缺失、被替換、symlink／hardlink／FIFO 的 owner
不會另建替代鎖。來源須完整通過原 CPU v3 reader、全 SHA、readonly payload、
identity digest、receipt bounds 與來源 generation；固定 cold release 的 inventory
另與兩份原檔 SHA／size 比對，不只相信 manifest metadata 的宣告。

Catalog schema 1 新增選用 `publication_families`，**不是**新增一個 concrete
training/provider dataset。原 48 個 `datasets` 未改，FinLab／TEJ 等原
`publish:false` 仍不動。原配方／dashboard／修復器的來源清冊不會把 family
template 判成缺漏資料；CLI status 才明列它需要顯式 key，`--all-ready` 不選整樹。

物理保存沿原 packed blob CAS，以 `loose_threshold_bytes:1` 讓完整 payload 不因
被包進不同 receipt ZIP 而複製。多個 key 可共用同一物件，但每份原 receipt、
logical file／inventory／manifest 身分獨立。不是另外維護永久全域快取索引。
本輪先前已固定的五份 current release 使用原 pack 格式，**原樣保留**；不為格式
改變重寫固定 pins，也不重打包已存在結果。新登錄的兩份歷史結果驗收 blob 共用。

原發布器完成後，重新持有既存 publication／retention owner，獨立驗證確切
release，再以既有 standard pin 格式獨占安裝到 retention config 指定的
`materialized_root/pins/completed-tw-forward/`。pin 不跟 latest；既有 binding
完整驗證後直接重用，不寫 pin、不動 head、不重打包。異常 binding 是失敗，不能
當作新 miss。標準 writer 的舊預設格式不變，新增 exclusive 安裝不覆寫意外檔案。

| 正式 D 操作／獨立回讀 | 實際結果 |
| --- | --- |
| 既有 current 結果 | 5 keys／10 原檔；重複發布新增 objects／bytes 均為 **0** |
| 兩份原真實歷史結果 | 完整 output 同為 **219,537 B**；實際引用同一 blob |
| 第二份歷史 publication | 只新增 receipt＋inventory，**52,945 B**；不再寫另一份完整 output |
| 完整 CPU 恢復 | 7 固定 releases／14 原檔，全部 tensors／metadata 逐位一致；NaN／負零另有 fixture 測試 |
| 原路徑／owner | 原 key／完整原檔 generation、126 canonical code、4 protected owners 不變 |
| Retention／副本處理 | 7 pins 均由原 retention parser 辨識；本次 35 個 private 驗證檔安全回收，原熱檔不刪 |
| GPU／provider | 0 次模型與 provider 呼叫，CUDA 未初始化；沒有交易／訊號發布或 service reload |

驗證工作副本 regular-file 配置為 3,063,808 B；它不是原庫回收量。整段獨立
驗證約 10.31 秒，未作 HDD 冷 cache／正式延遲 ABBA，不宣稱 D 或全機速度倍數。
最終 523 項共享回歸、277 項較早 CAS／wrapper focused 回歸有重疊，不能相加。

同時修復兩個原始架構問題：

- 每次 output publication 原本都重整整份 training cookbook，但 output 並未改
  training source。原 wrapper 在 CLI 成功確認 explicit family 後略過它；普通
  source、recipe publication、失敗傳遞與原定配方 owner 不變，沒有新 cron。
- 第一輪五份 D receipts 已成功提交，外層 run 卻在 EOF 後執行新腳本尾段而失敗。
  舊測試只覆蓋「檔案變短」；新測試重現兩個 wrapper 的「變長」失敗。現在將
  函式呼叫及最終 exit 放在同一 parsed block，避免續讀新尾段。原失敗收據保留；
  修正後只重用五個原固定結果、零新增 payload，也沒有把 API 失敗假裝成功。

初始功能 red tests、shell 競態與工具 namespace cwd 錯誤均保留。
原配方 owner 已額外正常 reconciliation，`completed-tw-forward` 不再出現在
source errors；既有 public 10/14 與 minute 10/15 不相容仍誠實保留，未用 cache
驗收掩蓋來源進度。完整原始呼叫、來源 SHA 與成功／失敗鏈以驗收收據為準。

重複驗證既有 key 可用原指令（不會重打包或重跑模型）：

```bash
bash scripts/run_data_release.sh publish completed-tw-forward \
  --completed-forward-key 0a966307c87c9046efec6d2a37f4dfc10dc349da1910e02dbc444e65cef059c9 \
  --sync-root /srv/stockagent-packed
```

**截至 07:44 的剩餘項（原入口已由下節續修）：**原 `cached_forward` 的自動 cold-first、所有 writer 的完整成長預留、
正式容量／全 consumer policy，以及封閉 live／舊 hot 的恢復及退休。這次沒有改
126 份推論 code 或以新 ABI 冒充舊模型；後續接原 writer 須一次完成五模式完整
qualification 與 canonical owner 採用驗收，不能由本次純 CPU 還原自行放行。
7 個結果也不是 189 個 commit 全庫、全歷史或 NAS／離機 ACK 已完成。

- [本輪來源／原 code／owners preflight](../artifacts/operations/service-cache-compaction/forward-catalog-production-20261016/preflight.json)
- [523 項最終共享回歸](../artifacts/operations/agent-workflow/runs/forward-catalog-frozen-final-reg-20261015T234811-9a3b0dc3/run.log)
- [522 項前版共享回歸](../artifacts/operations/agent-workflow/runs/forward-catalog-final-lifecycle--20261015T234153-cb8a06ce/run.log)
- [277 項來源 family／CAS／wrapper 回歸](../artifacts/operations/agent-workflow/runs/forward-catalog-source-family-se-20261015T234048-500fe55d/run.log)
- [變長腳本的兩個原始失敗](../artifacts/operations/agent-workflow/runs/forward-release-growing-shell-re-20261015T234119-f546f323/run.log)
- [原提交已成功但外層失敗](../artifacts/operations/agent-workflow/runs/forward-catalog-native-five-key--20261015T233751-5b19ff44/run.log)
- [修正後原固定結果重用＋兩份真實 CAS 發布](../artifacts/operations/agent-workflow/runs/forward-catalog-native-reconcile-20261015T234238-29c62948/run.log)
- [正式 D 固定版本完整 CPU 獨立回讀](../artifacts/operations/service-cache-compaction/forward-catalog-production-20261016/native-fixed-cold-readback.json)
- [原配方 owner reconciliation](../artifacts/operations/agent-workflow/runs/forward-catalog-recipes-reconcil-20261015T234414-0afce6d9/run.log)
- [本輪綜合驗收](../artifacts/operations/service-cache-compaction/forward-catalog-production-20261016/scoped-acceptance.json)

## 10/16 續修：原產生端先復用固定結果，再允許新計算

修改的是原 `cached_forward`，不是旁路還原器或另一個 cache pool：

1. 在原 key owner 下驗證完整熱結果，命中直接重用；不讀 D／儲存設定，不因
   流量、容量或使用量 metadata 故障而重跑模型。
2. canonical root 的 miss 持有完整成長預留，經原 catalog／retention 固定 pin
   取回原 payload＋receipt。沿原共用 copier 直接暫存到原 flat namespace，完整
   SHA、CPU v3 decoder、tensor bits／metadata、輸入／設定／owner generation
   都通過才提交；不建立另一份 managed tree，不重新 `torch.save`。
3. 只有從未登錄的真實新 key 才進原計算。已登錄的 pin／manifest／object
   缺失、損壞或競態會停止，不把它當成允許 GPU 重算的新 miss。

原 kernel reservation 涵蓋整段 miss／恢復／序列化／原子提交，預留
`2 * 1 GiB + 8 MiB` 最壞成長及 32 GiB 檔案系統餘量。**不是已安裝的
namespace quota**，不限制其他 filesystem writers，不因此准許刪除舊資料。
私有 offline benchmark root 仍是原獨立行為，沒有假冒 production binding。

正式來源在本輪自然更新；舊 control 因 input 不同被正確拒絕。沒有重標舊輸出，
而是在同一 source lease 下以兩個獨立 fresh processes 跑原模型／原完整訊號，
比較全部保留值及輸出樹，五模式都通過。原 126 個實作檔中的另外 125 個 SHA
不變；新 132-file identity 納入實際參與的 storage helpers。首次 CUDA 裝置
暫不可用的失敗保留，嚴格 CUDA 檢查通過後，只恢復未完成模式，不重跑已驗模式。

| 實際驗收 | 結果與範圍 |
| --- | --- |
| 五模式獨立完整推論 | 所有輸出逐位元相同；每次首算後 allocated／reserved 都釋放至 0 |
| 正式 current 五模式，fresh process＋兩次暖讀 | 全部 `disk_hit`，CUDA 未初始化，快取段每次 7.589–19.244 ms |
| 完整訊號的暖讀中位時間 | 五模式分別 292／452／416／474／673 ms；不是只報 cache 段，也不是開盤 SLO |
| 原 catalog／正式 D | 本輪另登錄 6 個真實完整 keys／12 原檔並完整驗證，沿用原 CAS／固定 pins |
| 非現役 10/14 真實歷史結果 | 先驗完整 D、當前 consumer pins、原 owner 及 process refs；僅將本輪新建 pair 搬到救援位置，未動現役 key |
| 原入口 cold-first 實測 | `cold_restored → disk_hit → disk_hit`；398,417 B 原 payload／receipt 完全相同，無模型／CUDA／provider／再編碼 |
| 該次 D 恢復時間 | cache transaction 623.445 ms，完整首個訊號 1,840.246 ms；非 HDD cold-cache ABBA，不宣稱一般倍數 |
| 原生採用 | 只重載 Discord，132-file code admission、五模式 ACK lag 0、保護 owners 與三份 ledger prefix 核對；不寫交易帳本 |

也修復原重載／驗收的三個根因，而非再重啟一次製造成功：

- 同一 run 全五模式均為 `LoadedImplementationChanged` 時，需要 fresh owner
  卻被「暖機 degraded」一律擋住。新增**明確**的
  `--discord-only --code-admission-only`；未知錯誤、busy／opening 保護照常擋住。
  原完整暖機驗收不變，code-only 不代表 GPU resident-ready。
- plan／apply 改用原 bounded coherent ACK pair。完整 prefix 檢查可能超過
  心跳 TTL，結尾再取新 pair，不拿先前 snapshot 的時鐘當成現在的 liveness。
- 日誌依固定 invocation 分別搜尋啟動成功、fatal／watchdog、warm-ready，各只取
  最後一筆符合記錄，不再讓大量普通進度把成功或早期 fatal 擠出最後 100 行。
  無權／無法讀取仍失敗，不把不可讀當成「沒有 fatal」。

首次 idle-only 採用在 **08:05 起的原盤前準備窗口**被拒絕，沒有繞過時鐘。
一次 canonical code-only 重載後，新 owner 已正確載入；原回讀因沿用舊心跳
誤報，原失敗 receipt 保留。修復後以**唯讀獨立回讀**驗收，沒有第二次重載。
當時 native 暖機為 `pending` 且可見 `/dev/dxg` handles；不聲稱服務 GPU idle，
也不在準備窗口強制釋放。正式快取 CPU-only 測試與此 native 時鐘是不同證據。

08:28 再次唯讀回查原消費端，4 模式仍接受，factorized 因後續 source generation
變更而失效，config／implementation 仍吻合。這是正確的資料邊界，不是假裝 cache
hit 或另一輪 GPU 失敗。metadata 診斷為零模型／CUDA／provider；原 minute retry
仍在運作，但完整維護受 08:07–13:40 窗口保護，下一輪完整維護為 13:40。
本輪接受的是已量測輸入的數值／cold-first 行為及新 native code／ACK，**不是
保證後續來源變更後的五份 artifacts 永遠新鮮**。

758 項最後共享回歸通過；與先前 704／186 項有重疊，不相加。原來源、舊完整
熱結果與 receipts 保留，本輪未刪資料；新建歷史測試 pair 的救援副本也保留。
**尚待：**新 key 自動 publication policy、namespace 總容量／精確 eviction、
全 fleet consumer 圖、封閉 live／舊 hot 退休；其餘 producer 的完整成長預留
不能由本次單一 writer 代替。08:00 來源完整 audit 仍出現大型 dense panel RAM
峰值，是下一階段要優化的實際 producer，不能宣稱全機或所有樹已完成。

- [五模式同輸入 qualification](../artifacts/operations/service-cache-compaction/forward-cold-first-production-20261016/qualification.json)
- [正式 D 六 keys 完整回讀](../artifacts/operations/service-cache-compaction/forward-cold-first-production-20261016/published-full-readback.json)
- [原入口 cold-first 接受證據](../artifacts/operations/service-cache-compaction/forward-cold-first-production-20261016/native-cold-first-accepted.json)
- [新 native owner 唯讀回讀](../artifacts/operations/service-cache-compaction/forward-cold-first-production-20261016/native-code-adoption-readback.json)
- [最新來源與四成功／一待更新的逐模式診斷](../artifacts/operations/service-cache-compaction/forward-cold-first-production-20261016/latest-source-cache-diagnostic.json)
- [758 項最後共享回歸](../artifacts/operations/agent-workflow/runs/forward-cold-final-acceptance-re-20261016T002507-400b640c/run.log)
- [本輪 scoped acceptance 與原檔 SHA](../artifacts/operations/service-cache-compaction/forward-cold-first-production-20261016/scoped-acceptance.json)

## 10/16 續修：原完整稽核先重用，再做必要欄位投影

原生 08:00 完整 audit 的 stage receipt 已把問題定位到原 Panel 產生端：
來源表是 9,635,384 列、143 個公開模型特徵；既定輸出僅
`3,119 dates × 2,761 symbols × 24 features`，卻在 `panel_load_or_build`
花 **222.398 秒**，程序 HWM 達 **59,365,257,216 B（55.29 GiB）**。
這是原完整量測，不是新候選的效能結果。

原因是原 full builder 遇到非空 `feature_exclude` 就不啟用公開欄位投影。
本輪改原 `audit_tw_public_data_layer._panel_kwargs`：以既有 canonical selector
先解析 schema、include／exclude／wildcards／raw features／開盤 gap 的 opt-in
順序與錯誤，再將精確選取清單交回**原 builder**。不換另一個 Panel 實作，不改
原值、缺值、時鐘、價格、標籤、股票範圍或規則。全來源與全欄位品質稽核仍完整
執行；只有這個既定模型面板的 cold-build input 被投影為需要的 8 個公開欄。

原 `load_cached_panel` 仍先用原 include／exclude key 找已接受快取，然後才查
投影後的等價 key；任一有效結果都直接重用。避免因優化 selector 的寫法而重新
生成同一份 Panel。miss 才進原 builder／原 chunk writer，所有規則欄獨立保留。
新 summary 明確記錄這是 **cold-build-only** 的 input projection，不把 cache
hit 說成又讀了來源；原 dependency receipt 含 auditor SHA，因此舊 audit 不會
被悄悄重標為新程式的驗收結果。

### 固定真實樣本量測與範圍

benchmark 僅保留第一次擷取的 65,536 列／2,057,387 B Parquet，四個 fresh
process 依 A/B/B/A 讀**同一份** fixture；全部程式、fixture 與輸出 bits 雜湊
核對，實際 Arrow／Polars pools 都為 2。重試及後續程式世代量測只重用該輸入，
不再讀 live history 或建立第二份資料庫；每次 attempt 的原始成功／失敗證據
保留。沒有把保留資料 SHA 比較省略到量測之外。

| 相同必要值＋全部規則的讀取／正規化／陣列及 hash | 原路徑 | 投影路徑 |
| --- | ---: | ---: |
| 讀取的公開模型特徵 | 143 | 8 |
| 保留的交易規則欄 | 17 | 17 |
| wall 平均 | 0.759904 s | 0.320798 s（-57.8%） |
| CPU 平均 | 1.135893 s | 0.389007 s（-65.8%） |
| fresh-process peak RSS 平均 | 707.2 MiB | 350.7 MiB（-50.4%） |
| 保留欄位及全部規則的完整 bits | 相同 | 相同 |

這是 **有界 external-reader 工程量測**，不是全歷史 Panel／完整 audit 的倍數
或全機 RAM 節省量。小型完整 builder 回歸另逐一比較所有 Panel dataclass 欄位、
ndarray dtype／shape／bytes、raw／負值／NaN／signed zero、開盤欄位、各種
include／exclude、完整規則及原／新有效 cache 命中。最後 6 個相關 test files
共 **205 passed in 8.64 s**，包含原 publication schedule／audit reuse 回歸；
與早先 88／98／100 項有重疊，不相加。Ruff 與修改範圍 diff check 通過。

### 原生採用與剩餘工作

原 08:45 後的 owner 實際回讀為 `deferred`／`strict_model_safety_audit`，並非
新 full audit 已完成；原 guard 為 08:40–09:05，原 timer 的開盤後接續時點是
09:10。這與另外的 bulk-maintenance 08:07–13:40 保護不同，不混成同一時鐘。
7 個現役 owner 的 PID／invocation 及 132 個模型／資料實作檔完整 identity
均未變，沒有因這次修正重啟服務或提早啟動全量工作；不因此宣稱 opening SLO
或 resident GPU ready 已驗收。

另對 factorized 08:00 前後兩份完整 manifest 作診斷：26 個 FinLab dataset
digests 及 public source SHA 不同；154 個 individual block proofs 中，135 個
相同、19 個不同，dates／symbols／common／dictionary 相同。那 135 個宣告
相同的區塊合計 186,743,824 B，目前是不同 inode、皆有可寫 member。這是下一個
**原 producer 不變區塊重用候選**，不是已驗證的回收量或 raw values 差異證明；
不能為了重用而降低原 input fence、硬連結可寫檔或重標舊 forward。完整 member
回讀、未變區塊安全發布及同輸入／同輸出驗收仍待後續。

本輪沒有刪除來源、帳本、原生快取或歷史版本。共用 full-builder 的所有 callers、
namespace 硬容量／精確 eviction、封閉 `artifacts/live` 及舊 `artifacts-hot` 的
逐項 cold recovery／退休仍未完成；不把單一 audit 修正說成五樹全部完成。

- [本輪綜合證據](../artifacts/operations/service-cache-compaction/audit-panel-projection-20261016/scoped-acceptance.json)
- [最終同一輸入 ABBA](../artifacts/benchmarks/running_services_20261013/audit-panel-projection-20261016a/attempt-7615b35c8e0f46e189504934dc500126/abba-receipt.json)
- [205 項最後回歸](../artifacts/operations/agent-workflow/runs/audit-projection-and-schedule-fi-20261016T004538-78f0aec0/run.log)
- [原生 owner／identity／排程唯讀回查](../artifacts/operations/service-cache-compaction/audit-panel-projection-20261016/native-readback.json)
- [factorized 原始 manifest 差異與 inode 邊界](../artifacts/operations/service-cache-compaction/audit-panel-projection-20261016/factorized-source-delta.json)

## 10/16 續修：原 factorized 產生端先比完整輸入，再共用不變區塊

### 原因與同一 owner 的修改

追溯到供應服務的 `artifacts/data_preparation`：相鄰完整 manifest 的 154 個
individual proofs 中，135 個相同、19 個改變，但原
`prepare_tw_day_trade_factorized_live.py` 每次重新壓縮／寫入全部區塊。原字典
另有 **217,315,593 B**，既有檔案為可寫 hardlink；不能視為不變資料，也不能
每代都複製字典，否則反增重複資料。

現在沿原 `prepare.lock`、source guard 和原子世代發布，不另建服務／CAS：

1. 比對全部 Float32 values、欄位座標、logical shape、common dtype／layout
   與實作依賴 SHA；不靠檔名、日期或 mtime 猜未變，不減特徵或更改因果規則。
2. 只索引同 owner 上一個已封存世代的小型 manifest。相同完整輸入才走既有
   immutable publisher；完整 SHA、大小、型別、唯讀、權限、同裝置及發布前後
   驗證仍在。壞／可寫 donor 回原 `write_array_block`；畸形 optional manifest
   不阻塞新世代。新 attempt 內相同內容也可共用。
3. 只封存新檔；舊檔不 chmod／覆寫。可寫原字典先做一份經完整 SHA 驗證的
   唯讀副本，以後共用該副本，仍核對原字典。這是首次額外成本，並非已回收
   原字典；舊可寫 generation 會安全 miss 一次。
4. 全部原 member／parent／source／calendar／config checks 保留，新實作另綁
   storage fingerprint 與發布前 fence。`storage_build` 記錄寫入／重用／拒絕，
   字典 bytes 分開列。儲存可測 NaN／inf bits，原模型 reader 仍拒絕非有限值。

新 generation 仍綁當期完整 source provenance；這次物理共用不會重標舊 forward
或降低推論 input fence。原 caller 與 codec 重用，不複製一份平行下載／推論管線。

### 固定真實區塊的完整持久化量測

原鎖下擷取 4 個未變區塊、1 個真實變動 tail 與 common；唯一固定輸入
**3,870,995 B**。原／新版本皆做「初次世代 → 更新 → 全 member SHA → 原
decoder → 全選定 values／coordinates 讀回指紋」；fresh-process A/B/B/A 的
Arrow／Polars pools 實際都是 2。沒有複製全資料庫、全部歷史或大字典。

| 同一真實輸入 | 原 writer | 輸入指紋＋唯讀重用 |
| --- | ---: | ---: |
| 初次世代 wall 平均 | 0.436050 s | 0.551559 s（多 26.5%） |
| 更新 wall 平均 | 0.404817 s | 0.326529 s（少 19.3%） |
| 更新 CPU 平均 | 0.398559 s | 0.291924 s（少 26.8%） |
| 更新新寫 payload | 3,071,415 B | 814,158 B（少 73.5%） |
| 更新共用 payload | 0 | 5（4 個區塊＋common） |
| 初次＋一次更新 wall | 0.840867 s | 0.878088 s（此組合尚未加速） |
| 全選定值／座標及原編碼 payload | 相同 | 相同 |

初次封存／fsync 成本不省略；長期更新與一次性建置分開。此量測不含全部來源
正規化、完整 causal sweep、模型 forward 或真實 217 MB 字典的初次複製，不能
外推整機 RAM、整個準備時間或 135 個候選的回收量。後續重試重用同一固定輸入，
失敗 run 保留；最初兩次超過小型輸入 guard 都在擷取前停止，未複製額外歷史。

最後 9 個相關 test files **138 passed、3 skipped，24.06 s**，含原 updater、
storage／reader、值投影、refresh／repair 與 CPU 完整模型輸出。三個 skip 是
CUDA cases，本輪未執行 GPU。新增測試涵蓋變動值／-0／座標／shape 不可誤重用、
非有限值不得繞過模型 admission、壞 manifest／payload 重建及可寫原字典跨代
只複製一次；Ruff、修改範圍 diff check 通過。測試數與先前回歸有重疊，不相加。

### 原生時鐘與剩餘邊界

原 producer 已接線，未在開盤強制全量 prepare；正常 phase 採用與完整 wall／RAM
尚待驗收，不能拿 benchmark 當現役完整收據。7 個現役 owner 的 PID／invocation、
132 個已載入 model／data 實作檔完整 generation＋SHA，以及擷取的 production
members metadata 均未變。本輪無重啟、GPU／provider 呼叫、帳本寫入或刪除，
實際回收量為 0。

shared full-builder 所有 callers、namespace 硬上限／有恢復證明的 eviction、
封閉 live 帳本與舊 hot 逐項退休仍未完成，目標維持進行中。

- [綜合工程驗收與初次成本](../artifacts/operations/service-cache-compaction/factorized-producer-reuse-20261016/scoped-acceptance.json)
- [真實固定輸入 ABBA](../artifacts/benchmarks/running_services_20261013/factorized-block-reuse-20261016a/abba-receipt.json)
- [138 項回歸與 Ruff](../artifacts/operations/agent-workflow/runs/factorized-reuse-regression-20261016T010145-476d0349/run.log)
- [現役 owner／132-file／原檔唯讀回查](../artifacts/operations/service-cache-compaction/factorized-producer-reuse-20261016/native-readback.json)

## 10/16 續修：factorized 失敗重試重用已完成區塊

本輪接手上述原 factorized immutable-member 重用原型，補上另一個尚未閉合的
產生路徑：中途失敗的 `.generation.attempt.uuid` 沒有完整 manifest，下一次重試
原先不會採用它已經完成的區塊。僅保留失敗檔案、再重新壓縮一份，不能降低
此服務的穩態與故障重試佔用。

修改仍在原 `prepare_tw_day_trade_factorized_live`／`prepare.lock` owner：

- 新寫區塊先沿原 codec 完成、唯讀封存及 fsync，再以**每 attempt 一份**
  `storage_progress.json` 原子、耐久替換，沒有每個 block 一個 sidecar 或新 CAS。
- 重試只枚舉同一完整 source／config／code generation 的有界 attempts；採用前
  核對原完整輸入 bits／座標 key、全檔 SHA、唯讀／owner／路徑。全 generation
  的原始來源、日曆與設定 fence 仍在原最終驗收，不能藉重用放寬。
- 已完成 block、common 及字典直接沿原同裝置 immutable-link helper 重用；不同
  bits、損壞、可寫、重導、未知或尚未 checkpoint 的檔案保留，但不作 donor。
- 失敗進度不是 READY、完整 manifest、部署或冷庫恢復證明。最後仍驗原完整
  factorized members，再走原原子 generation／`current` 切換。沒有新增 timer、
  外部 daemon 或自動刪除；使用者不用每次再請 agent 搬資料。

### 量測與工程驗收範圍

重用先前已固定的**同一份真實區塊 fixture**，不再複製來源。四個 fresh workers
按 A/B/B/A，各執行「完成區塊但不提交 manifest → 新 producer 重試 → 全量
SHA／解碼及所有緊湊陣列／座標 bits 回讀」；baseline 使用修改前保存的原 helper。
實際 Arrow／Polars pools 都為 2，沒有 provider／GPU 呼叫或正式資料寫入。

| 完整中斷＋重試 boundary，平均 | 原 helper | 原產生端續用 |
| --- | ---: | ---: |
| inode 去重後實體配置，含進度／manifest metadata | 6,189,056 B | 3,112,960 B（-49.7%） |
| 唯一 payload 實體配置 | 6,168,576 B | 3,084,288 B（-50.0%） |
| wall | 1.368755 s | 1.173670 s（-14.3%） |
| CPU | 1.264830 s | 0.965727 s（-23.6%） |
| 重試新寫 payload | 全部再寫 | 0 B；全部原值相同 |

此量測只涵蓋原 block/common persistence 與完整驗證，**不含來源正規化、
causal sweep、217 MB 原字典或全準備流程**，不是全服務速度或 GB 級回收承諾。
另外以真實 `os._exit(71)` 中斷 fresh process，下一 fresh owner 在禁止呼叫
encoder 的情況下，恢復相同 SHA／inode；故障注入也涵蓋耐久收據寫入失敗、
signed zero 改變、壞 progress／payload、重導、未知檔與有界掃描。

最後 11 個相關 test files 為 **218 passed、3 skipped**；未執行完整 repo suite。
前兩次共享命令因測試檔名不存在而 exit 4、未執行測試，原失敗 run 留存，不把
它們改成通過。Ruff 與修改範圍 diff check 通過。

### 原生採用與仍未完成事項

本輪只改原 producer／storage helper／有界 benchmark／相關測試及文件；不修改
現役 132-file 模型／資料實作，不重啟 Discord 或交易／擷取 owners，不重跑盤中
完整準備，也不改原始值、訊號、策略、checkpoint 或帳本。既有 updater hook
直接呼叫原 `prepare`，所以不需另行安裝開關；**正式採用收據仍待下一次原生
準備**，不能將本輪私有驗證說成已觀察到原生新來源更新完成。

本輪沒有刪除歷史資料，也沒有回收既有 GB；接受的是往後少寫重複資料的機制。
舊的可寫／無進度 attempts 不被追認。namespace 總容量／其他 writer 完整預留、
全 fleet consumer 圖及封閉 live／舊 hot 的精確冷恢復與退休仍待接續；原任務
保持 active，不宣稱所有服務或所有五個大目錄已完成。

09:12 首次獨立回讀拒絕了「完整模型 identity 未變」的預期，失敗收據保留。
逐檔核對確認：原 132 檔的內容／metadata 全部不變，但並行工作新增
`stockagent/data/news_dataset.py` 及 `news_dataset_sources.py`，現有共用 identity
會納入它們而變成 134 檔。後續證據分別記錄**原檔保留**與**完整 identity 漂移**，
不將新增檔案默認為已獲 native code admission。這不是本 storage patch 的改動；
保留其他工作，不移除新聞模組、不縮小 guard scope，也不為製造通過而重載盤中
服務。正式服務 code／準備採用驗收因此仍未完成，交由原 qualification／reload
流程在其安全條件下處理；不能宣稱本輪已證明現役所有模式 ready。

- [本輪 scoped acceptance](../artifacts/operations/service-cache-compaction/factorized-resume-20261016/scoped-acceptance.json)
- [獨立 payload／code／owners 回讀](../artifacts/operations/service-cache-compaction/factorized-resume-20261016/independent-readback.json)
- [同一 fixture 的完整中斷＋重試 ABBA](../artifacts/benchmarks/running_services_20261013/factorized-block-reuse-20261016a/resume-attempt-020661308fa94f76a58b4a0180adc177/abba-receipt.json)
- [218 項通過／3 跳過的共享回歸](../artifacts/operations/agent-workflow/runs/factorized-attempt-resume-integr-20261016T010915-4c1097fc/run.log)

<a id="shared-panel-producer-20261016"></a>

## 10/16 續修：共用 Panel 先重用，再按需生成

本輪繼續修改**原始產生端**，沒有新增平行資料管線、定期去重 daemon 或另一份
全量資料庫。原則是同一有效輸入只有一個可重用的邏輯快取；需要建置時，只解碼
消費者會用的模型欄位，交易規則仍完整保留；提交成功之前不讓讀者看到半成品。

定位到的剩餘問題及修正：

1. 原完整 builder 只有在特定 include／exclude 組合才提前投影，其餘情況先讀
   全部外部模型欄位再丟棄。現在完整、tail、audit 共用 schema 級 selector，
   wildcard、raw opt-in、gap、欄位順序和錯誤拒絕行為走原契約；規則不參與
   模型特徵篩選，float64 規則值、來源時間及完整 fingerprint 都不縮減。
2. 外部資料原本先以 boolean mask 複製完整個股 subset，再按股票切片。
   現在按已排序的連續 symbol 範圍直接建立 view，市場及個股共享原 buffer，
   不改日期、排序、dtype 或零值／缺值。非連續的重複 symbol 拒絕，不靜默覆寫。
3. `load_cached_panel` 原本不能指定 writer 的獨立 cache root，audit 也未完整
   使用配置的 root；已有快取因此可能未被消費者找到。現在讀寫共用明確的
   `panel_cache_root`，audit 優先原設定 key，再相容讀取舊 projected key；cache
   miss 只由原 kwargs 建置，不再為投影改另一個 key。有效舊檔直接讀，不補寫
   metadata；來源／設定變更仍失效，缺失不代表可降級為不驗證來源。
4. 私有、未發布 generation 原本逐陣列反覆同步同一個目錄。現在保留每個
   payload／JSON 原有的資料 fsync，集中提交 namespace；generation、其父目錄、
   cache 及 cache parent 四個必要 barrier 均在 variant 發布之前，任一失敗就
   拒絕發布、保留舊 accepted generation。獨立檔案寫入、chunk/link 與最後的
   原子發布仍走原路徑。這遵守資料持久化與目錄可見性分開處理的
   [Linux 檔案系統契約](https://www.kernel.org/doc/html/latest/filesystems/vfs.html?highlight=writeback)；
   故障注入及程序回讀不冒充實體斷電驗證。
5. 原 08:30 audit 的 dependency fingerprint 補入 Panel、cache、chunk code，
   不讓舊品質收據跨過實作變更被誤用；來源 acquisition、完整性與原排程不變。

### 固定真實輸入的完整流程驗收

重用既有 65,536 列外部 capture，9 份真實 OHLCV（包含原設定 benchmark 2330），
不另複製完整來源。比較保存的本輪修改前 `panel.py` **及 cache writer**，每次
fresh process、2 threads，順序 A/B/B/A。量測包含完整冷建置、耐久提交、每個
Panel dataclass 欄位的 raw-bit hash、完整 cache 回讀，以及原 key 第二次命中。

| 同一有界工作負載，兩次平均 | 原產生端 | 新產生端 |
| --- | ---: | ---: |
| 完整流程時間 | 11.676 s | 5.724 s |
| CPU time | 8.524 s | 3.102 s |
| process 峰值 RSS | 801.575 MB | 427.403 MB |
| 外部模型解碼欄位 | 143 | 8 |
| 獨立交易規則欄位 | 17 | 17 |
| 持久快取配置容量 | 3.645 MB | 3.643 MB |

四次輸出均為 `3120 × 9 × 24`，完整 SHA-256 一致：
`2a8bc4167ecb22f6cd1a1ef4593de451255ef630dbf5d0ee4024f934b6658be1`。
第二次命中 **0 source payload decode／0 cache write**；新唯讀 loader 也能讀取
原 writer 的原 key。磁碟配置差異僅約 2 KiB，不能宣稱這輪回收了 GB。

時間有明顯主機排程／I/O 波動：最後 baseline 9.241–14.111 s，新版
4.783–6.666 s；較早同輸入另一輪也曾 4.256 → 4.675 s，沒有刪掉該退步收據。
這是固定有界 complete builder 的證據，不外推全歷史倍速、epoch、策略效益，
也未證明剩餘瓶頸都屬硬體。共享語意／durability／live consumer 回歸共
**473 passed**；四個 pre-publication barrier 故障案例均保留原 meta 與舊可讀值。

### 操作與重跑

原服務及 audit 直接使用共用修正，不需新增排程。以下只查參數：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_tw_public_data_layer.py --help
run_fintech_python scripts/benchmark_panel_producer_projection.py --help
```

保留既有 fixture 的完整重跑（每次另寫私有 attempt，不更新正式 cache、不呼叫
provider／模型／GPU、不清理先前證據）：

```bash
source scripts/runtime_env.sh
POLARS_MAX_THREADS=2 OMP_NUM_THREADS=2 OPENBLAS_NUM_THREADS=2 CUDA_VISIBLE_DEVICES= \
STOCKAGENT_PANEL_CHUNK_STORAGE=1 run_fintech_python scripts/benchmark_panel_producer_projection.py \
  --config configs/deployments/tw_day_trade_multi_basis_projection_l1_gelu_fold11.yaml \
  --external-fixture artifacts/benchmarks/running_services_20261013/audit-panel-projection-20261016a/bounded_features.parquet \
  --parquet-root /srv/stockagent-live/data_tw_public/stocks \
  --baseline-panel artifacts/operations/service-cache-compaction/shared-panel-producer-20261016/original_panel.py \
  --baseline-cache artifacts/operations/service-cache-compaction/shared-panel-producer-20261016/original_panel_cache.py \
  --output artifacts/benchmarks/shared-panel-producer-20261016 \
  --threads 2 --reuse-fixture
```

`--baseline-panel`／`--baseline-cache` 要求明確保存、非重導的私有原實作，不從目前
Git dirty tree 猜舊版；`--reuse-fixture` 核對固定 config、來源 capture 及原實作 SHA，
不再次抓取 live 資料。這些 payload 留在 ignored artifacts，不加入 Git。

### 原生採用與未完成邊界

- 473 項測試及有界全 builder 通過，不等於原生全來源 audit 通過。
  目前完整正式 source/config key 沒有可重用的完整 cache，唯讀診斷回報
  `awaiting_exact_source_bound_native_cache`；不為製造通過強建另一份全量資料。
  原 08:30 owner 仍負責下一次正式資料品質與建置驗收。
- 依原安全工具執行 Discord-only code admission；第一次被實際來源更新鎖擋住，
  `restart_requested=false`，沒有停來源 producer、清鎖或改帳本。來源完成後第二次
  於 23:44 通過 `accepted_code_admission_only`：新 Discord invocation
  `51f835451f854f458f888fb654b3ef8c`、完整 implementation
  `8b4b1cc8be3c98211a4fdee4a36d2775b3ab17e71ebd24c65f2948963f253167`，
  原帳本前綴、五模式 signal ACK／engine run、六個 protected owners 全部保留。
  交易引擎未重啟。這是程式載入／Gateway／ACK，不是 GPU warmup 驗收；既存
  factorized 背景準備失敗仍存在，該組由其他工作擁有，本輪維持只讀。
- D 冷庫、原始來源、模型、ledger、舊 generation 及 factorized 準備組皆未刪改。
  **本輪沒有回收歷史資料空間，廣義服務架構任務仍 active。** 仍要核對其他
  producer 的 live/cache 多根路徑、可重建衍生物的保留上限／安全回收及正式全量
  consumer 採用；不能把尚未量測的軟體瓶頸稱作硬體下限。

證據：

- [本輪 scoped acceptance](../artifacts/operations/service-cache-compaction/shared-panel-producer-20261016/scoped-acceptance.json)
- [獨立完整 cache／程式回讀](../artifacts/operations/service-cache-compaction/shared-panel-producer-20261016/independent-readback.json)
- [最後固定輸入完整 ABBA](../artifacts/benchmarks/shared-panel-producer-20261016/attempt-9a0843784f7b4d90820594da21cc955b/abba-receipt.json)
- [473 項共享回歸](../artifacts/operations/agent-workflow/runs/shared-panel-final-shared-contra-20261016T153756-6c5a970c/run.log)
- [qualified 原生採用結果](../artifacts/operations/service-cache-compaction/shared-panel-producer-20261016/native-code-adoption-retry.json)
- [第一次來源寫鎖安全拒絕](../artifacts/operations/service-cache-compaction/shared-panel-producer-20261016/native-code-adoption.json)
- [完整正式 key 唯讀診斷](../artifacts/operations/service-cache-compaction/shared-panel-producer-20261016/native-complete-cache-diagnostic.json)

## 操作證據入口

- Panel chunk／正式 CPU cache／原生冷 owner 綜合驗收：`artifacts/operations/service-cache-compaction/panel-chunks-production-20261016/acceptance.json`
- 五模式原完整模型／非省略值／正式 key 及原 SHA：`artifacts/operations/service-cache-compaction/panel-chunks-production-20261016/numerical-and-cache-acceptance.json`
- 真實全量 Panel ABBA：`artifacts/benchmarks/panel_chunk_storage_20261016_final/acceptance.json`
- 獨立全陣列 shape／dtype／bytes／軸及來源不變：`artifacts/benchmarks/panel_chunk_storage_20261016_final/independent-complete-array-readback.json`
- 426 項回歸：`artifacts/operations/agent-workflow/runs/panel-chunks-rollout-regression-20261015T182113-279c78cf/run.log`
- 原生切換／protected owners／帳本：`artifacts/operations/service-cache-compaction/panel-chunks-production-20261016/qualified-cold-cutover.json`
- 啟用新 writer 的正式 close readback：`artifacts/operations/service-cache-compaction/panel-chunks-production-20261016/enabled-close-readback.json`
- 當期新 owner 最終獨立回讀：`artifacts/operations/service-cache-compaction/panel-chunks-production-20261016/cold-owner-final.json`

- 當期模型產生端／正式 CPU cache／新冷 owner 綜合驗收：`artifacts/operations/service-cache-compaction/basis-aux-production-20261016/acceptance.json`
- 完整模型與精簡值 oracle／原 payload SHA／交錯量測：`artifacts/operations/service-cache-compaction/basis-aux-production-20261016/numerical-and-cache-acceptance.json`
- 652 項回歸：`artifacts/operations/agent-workflow/runs/basis-aux-regression-final-20261015T175735-9fe39942/run.log`
- 切換、protected owners 與帳本前綴：`artifacts/operations/service-cache-compaction/basis-aux-production-20261016/qualified-cold-cutover.json`
- 新 owner 獨立最終回讀：`artifacts/operations/service-cache-compaction/basis-aux-production-20261016/cold-owner-final.json`

- 五模式訊號投影、全量 old/new SHA／正式 key／效能觀察：`artifacts/operations/service-cache-compaction/signal-projection-20261016/acceptance.json`
- 新原生冷 owner／code／Gateway／ACK／GPU handles：`artifacts/operations/service-cache-compaction/signal-projection-20261016/cold-owner-accepted.json`
- 有界切換／所有 protected owners／帳本 prefix：`artifacts/operations/service-cache-compaction/signal-projection-20261016/qualified-cold-cutover.json`
- 正式五模式收盤 readback：`artifacts/operations/service-cache-compaction/signal-projection-20261016/native-close-readback.json`
- 新完整 GPU forward oracle／全訊號 parity：`artifacts/operations/service-cache-compaction/signal-projection-20261016/*-qualified.json`
- 無 model／CUDA 的正式 key 全訊號比較：`artifacts/operations/service-cache-compaction/signal-projection-20261016/*-native-cpu.json`
- transport 級 HTTP 禁止後的獨立重驗：`artifacts/operations/service-cache-compaction/signal-projection-20261016/*-native-cpu-strict.json`
- HTTP／CUDA／正式 keys／全部訊號 SHA 與當期冷 owner 綜合驗收：`artifacts/operations/service-cache-compaction/signal-projection-20261016/strict-http-acceptance.json`
- 395 項最終回歸：`artifacts/operations/agent-workflow/runs/signal-projection-http-final-tes-20261015T173957-63df42d5/run.log`

- 最新原生冷 owner／無 GPU handles／五份快取：`artifacts/operations/service-cache-compaction/forward-idle-owner-accepted-20261016.json`
- 冷切換後獨立帳本 prefix／全部 protected owners：`artifacts/operations/service-cache-compaction/forward-cold-owner-ledger-prefix-20261016.json`
- 午夜 qualified 切換、原帳本 prefix／protected owners：`artifacts/operations/service-cache-compaction/forward-midnight-qualified-reload-20261016.json`
- 目前五模式 canonical CPU-cache reuse：`artifacts/operations/service-cache-compaction/completed-forward-close-after-20261016.json`
- 現行五 key 位於原始完整壓實計畫：`artifacts/operations/service-cache-compaction/current-keys-preserved-by-sharing-20261016.json`
- 五模式完整 payload 欄位容量：`artifacts/operations/service-cache-compaction/current-forward-field-footprint-20261016.json`
- Forward 完整計畫與實際套用：`artifacts/operations/service-cache-compaction/forward-20261016-apply/completed-forward/completed_live_inference/20261015T170412.167467Z-c21ef51c.result.json`
- Forward 獨立完整 bytes／receipt 回讀：`artifacts/operations/service-cache-compaction/forward-20261016-apply/independent-byte-acceptance.json`
- Forward 已安裝 policy 的實際低 I/O 維護：`artifacts/operations/service-cache-compaction/forward-20261016-installed/completed-forward/completed_live_inference/latest.json`
- 分鐘 candidate-only 維護：`artifacts/operations/service-cache-compaction/forward-20261016-minute-screen/physical-minute/tw_minute_paper_parity_09bedd96a2f68c39/latest.json`
- 183 項共享回歸：`artifacts/operations/agent-workflow/runs/forward-sharing-final-regression-20261015T170505-a99f9e4f/run.log`

- 完整來源／六次全量結果獨立驗證：`artifacts/benchmarks/running_services_20261013/full-feature-all-candidates-proof-20261016.json`
- producer／oracle 最新回歸：`artifacts/operations/agent-workflow/runs/immutable-storage-producers-veri-20261015T164022-dbd41523/run.log`
- 當期五模式收盤 cache-only：`artifacts/benchmarks/running_services_20261013/postclose-cache-reuse-20261016a.json`
- 00:17 全服務觀察（含工程負載，不是全機 idle）：`artifacts/operations/running-services-optimization-20261013/all-services-after-close-20261016a.json`
- benchmark 共用獨立驗收：`artifacts/operations/service-cache-compaction/benchmark-input-reuse-20261016/acceptance.json`
- benchmark 真實來源擷取：`artifacts/operations/agent-workflow/runs/public-current-input-reuse-captu-20261015T160442-1e7f29ae/run.log`
- benchmark 同來源重用：`artifacts/operations/agent-workflow/runs/public-current-input-reuse-repea-20261015T160526-fb3d665f/run.log`
- benchmark／服務 producer 共享回歸：`artifacts/operations/agent-workflow/runs/fixture-reuse-final-shared-regre-20261015T160443-20a8a5d1/run.log`
- 清點：`artifacts/operations/agent-workflow/runs/service-storage-breakdown-20261015T135139-591f8f45/run.log`
- session 共用回歸：`artifacts/operations/agent-workflow/runs/immutable-session-compaction-reg-20261015T150127-861bf584/run.log`
- session 共用唯讀計畫：`artifacts/operations/agent-workflow/runs/service-minute-sharing-audit-20261015T145732-47bd99b1/run.log`
- 正式壓實：`artifacts/operations/service-cache-compaction/physical-minute/tw_minute_paper_parity_09bedd96a2f68c39/20261015T150549.988928Z-f53074e1.result.json`
- 獨立全 SHA 回讀：`artifacts/operations/service-cache-compaction/physical-minute/tw_minute_paper_parity_09bedd96a2f68c39/20261015T152124.230205Z-9cba8710.result.json`
- Native producer／Forward 回歸：`artifacts/operations/agent-workflow/runs/producer-and-forward-regression-20261015T154144-7aab59bf/run.log`
- 重量測：`artifacts/operations/agent-workflow/runs/service-storage-after-reuse-20261015T153833-2900d26f/run.log`
- Qualified reload：`artifacts/operations/service-cache-compaction/native-producer-reload/qualified-reload.json`
- 獨立帳本 prefix：`artifacts/operations/service-cache-compaction/paper-prefix-after.json`
- 回歸：`artifacts/operations/agent-workflow/runs/empty-replay-consumer-regression-20261015T135843-de02460d/run.log`
- 服務依賴回讀：`artifacts/operations/agent-workflow/runs/service-consumer-readback-20261015T140443-592a9d79/run.log`
- 精確重複候選：`artifacts/operations/storage-cleanup-20261015/parity-duplicate-audit/artifact-dedup-tw_minute_paper_parity_09bedd96a2f68c39-20261015T065829.909122Z.json`
- 工程任務：`stockagent-agent task show penguin-service-storage-optimization-20261015`
- 既有清理／63 GB 恢復交易：[當日清理報告](penguin_storage_cleanup_2026-10-15.md)
- 不可繞過的資料／刪除邊界：[Storage contract](agents/storage.md)
