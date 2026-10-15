# 本機執行服務效能清點與最佳化

目前目標仍在進行；已驗收的結果與尚待正式載入的部分分開列出：

- **10/15 09:17 FinMind 保護期後恢復下載已完成獨立回讀。** 同一正式
  Complement owner 在 09:10:00.013602 更新 running 心跳，09:11:04.656011
  觀察到第一筆 complete（結束保護後 64.656 秒，包含初始化與來源延遲）。
  抽查三份新收據／Parquet，共 10,260 列，身分、筆數與完整 SHA 通過；
  舊收據及來源也保留。900 秒跨界觀察不是純 idle／API 吞吐比較，更不是
  全部歷史完成。47 項有重疊的等待／回讀／工程 fixture 測試通過。
- **10/15 08:51 FinMind 開盤保護等待的重複初始化已原生驗收。** 原本每
  600 秒仍重做 queue／seed／修正／狀態初始化，沒有取得新資料；改成等到
  原日曆准許的 09:10，每 30 秒只更新心跳並檢查日曆。原生 690 秒觀察
  跨過舊重建邊界，僅用 0.053314 秒 CPU、狀態循環未換代；原 180 秒
  窗口含一次初始化用了 18.756198 秒 CPU。這是不同長度的保護等待取樣，
  不當作 API／全機降幅。1,121 項共享回歸、62 項有重疊的工程驗收測試
  與完整 SQL 回讀通過；目前僅 Complement owner 已獨立驗收，09:10
  恢復另由上述跨界觀察及來源回讀證明，不宣稱 Free／Sponsor 或所有服務已採用。
- **10/15 08:29 FinMind 分組失效快取已正式驗收。** 完整 3,572,121 筆
  queue、每輪 16 次發布／24,248 原始列，固定資源 A/B/B/A：平均 wall
  6.502→3.612 秒（少 44.4%）、CPU 3.040→1.199 秒（少 60.5%）；完整
  tasks、status、收據及 Parquet 相同。1,099 項共享回歸通過；原 owner
  在無在途工作的開盤保護等待期載入，獨立完整 SQL 回讀與原生狀態全部
  欄位相同，五個其他保護 owner 未變。這是本機發布／狀態流程，不是整個
  API 下載 1.80 倍。首次冷啟動仍有約 77 秒初始化；後續保護期重複初始化
  已另行量測與修正，沒有宣稱首次啟動成本消失。
- **10/15 07:59 五模式盤後快取續接已驗收。** 修正 path 失效後沒有短版
  備援喚醒的根因；同一 controller 分開短版／完整歷史預算。新來源世代由
  原 owner 完成，獨立 consumer 五模式全數只讀 cache、零 model／provider／
  CUDA；最後兩輪真實 timer 不再啟動 worker，六個保護 owner 未變。
  完整 idle-probe CPU 平均 1.127→0.086 秒、post-exec 峰值 163.78→23.78 MiB，
  283 項回歸通過；52／52 當下可讀 managed Linux processes 無 GPU handles。
  這是本機盤後備援的有界驗收，不是全部服務／Windows 顯存已最佳化。
- **10/15 07:32 全資料清冊的 dict 快徑已原生驗收。** 最終完整 68,043
  路徑／89,097 特徵、每輪 204,129 次 frozen-source checks，八輪鏡像比較
  CPU 平均 9.031→8.446 秒（少約 6.5%），完整 cache／feature bytes 相同；
  368 項相關回歸通過。正式 native guards、世代收據及全部來源身分已獨立
  讀回；不是全機／完整 CLI 加速或 RAM 降幅。另兩個 GC／deferred 候選
  完整流程反而增加 CPU，因此未部署。
- **來源換代不沿用舊 cache。** 07:32 的五模式來源失效已在 07:59 依新
  世代恢復；舊拒絕診斷與收據保留，未放寬 source／model／config gate。
  原事件 path 的 `failed/resources` 仍如實呈現，已有不用 inotify 的 timer
  備援。VS Code 舊 watcher 仍待使用者 `Developer: Reload Window`，未為此
  終止 IDE、提高 kernel limit 或重啟交易。Windows 整卡顯存仍未完整歸因。
- **10/15 OpenBB 全量 footer 讀取減少不必要的建構成本。** 完整 185,963
  真實 footers／50,722 view proofs，暖機分開、八輪鏡像比較：metadata-only
  平均 wall 139.898→120.816 秒、CPU 81.125→70.160 秒（少 13.5%）。
  所有完整輸出 SHA 相同，430 項相關回歸通過；06:48 新正式 owner 的全來源
  集合／每檔世代／完整 native AST／JSON-Parquet 獨立讀回已接受，六個
  交易／行情保護 owner 與 native 資源 limits 未變。
  不採用兩個 worker，因 CPU 平均反而多 48.1%；未縮減檔案驗證或來源範圍。
  06:47 五模式收盤快取 5／5 accepted、模型前推／provider／CUDA 為零，
  68／68 可讀 managed Linux 程序沒有 GPU device handles。這不是全機
  CPU／Windows 整卡顯存歸零的證明。
- **10/15 OpenBB 完整狀態發布已減少 SQL 分組成本。** 完整 7,327,309
  source files／334,949,985 rows、67 endpoints 的暖快取 A/B/B/A＋B/A/A/B：
  wall 平均 2.299→1.946 秒，CPU 少 12.9%，八輪完整 JSON／Parquet
  發布與讀回相同。最終 380 項相關回歸通過，06:03 正式原生採用與原 SQL
  全量讀回已獨立接受；六個保護 owner 未變。
  冷暖未分開的首次 64% 降幅不當正式收益；這不是完整 compaction 加速。
  五個收盤模式仍零重算／CUDA、64／64 managed Linux 程序無 GPU handles。
- **10/15 三來源 hot-tail 清冊的重複日期轉換已修正。** 全部 1,976 個已存
  商品、前後完整清冊 durable 發布 A/B/B/A：123.227→24.922 秒、4.945×，
  CPU 約少 79.1%；四輪完整輸出 SHA 相同。三個既有 collector 已接線，
  非標準格式維持原驗證／異常語意；05:29 獨立 native probe 已觀測三個
  正式 owner 實際執行新版入口，沒有重啟下載。最終 389 項相關測試通過；
  05:32 五模式收盤快取仍零前推、54／54 managed Linux processes 無 GPU
  裝置句柄。這不是完整 API 下載 4.945 倍或 Windows 整卡顯存歸零。
- **10/15 04:41 FinLab 分鐘優先准入已原生驗收。** 完整 280,704 分區、
  2,461 股票／5,711 sessions 的控制循環 A/B/B/A：30.603→16.734 秒，
  CPU 約少 46.4%，完整清冊、准入結果、候選及 durable readback 全相同。
  新 owner 實際執行六次 minute-only 檢查，約 1.136 秒；三來源驗證通過、
  六個保護 owner 未變，沒有重啟下載。這不是全 API 下載 1.83 倍。
- **10/15 04:49 發現仍未解決的主要閒置負擔：VS Code fileWatcher。**
  同一原生程序占 12.89 GiB RSS、約 0.997 CPU core，監看 4,187,900 項，
  單一程序已占 UID watch limit 99.85%。工作區排除規則已修正；舊程序
  尚未釋放，需要 `Developer: Reload Window` 後回測，未終止編輯器或
  增加 kernel limit。registered intraday 的 main exit＝0／來源更新成功，
  卻同時有 inotify ENOSPC 與 stop timeout；不能把這個服務狀態問題當成
  所有來源下載失敗，也尚未證明釋放 watchers 就完全解決 timeout。
- **10/15 04:28 Bybit 固定原 release 完整恢復完成。** 同一原始還原 owner
  成功處理 4,677 檔／22,357,148,836 bytes，切換到 mutable workspace，
  cold 原件未改。原 pipeline 接續 888 symbols 的 1m tail 更新，報告全部
  updated；04:57 固定 scope、原件／新 tail readback 與當時 native owner
  獨立驗收接受。服務停止生命週期仍有上述資源問題，未聲稱全歷史／全部
  symbol 都沒有缺口，也未聲稱 stop timeout 已解決。
- **10/15 04:08 FinLab 完整分區清冊已自然載入索引查詢修正。** 同一固定
  280,391 筆真實索引、完整 2,461 股票／5,711 交易日、64 次持久寫入及
  逐筆讀回，A/B/B/A 的流程中位數 10.293→3.807 秒，CPU 約少 64.8%；
  全部有效分區、完整摘要與候選結果相同。434 項相關回歸通過，新 owner
  的實際 TEMP view 指紋獨立回讀吻合，原排程持續抓取，未重啟服務。
  這不是全下載／全機 2.70 倍；五個收盤模式再次 CPU cache 命中、零 GPU
  前推。Bybit 固定原 release 的同一完整還原仍在執行，未宣稱已完成。
- **10/15 03:39 新程式世代的盤後快取已重新正式驗收。** 五模式完整輸入
  僅 code 換代；每個新模型隔離前推一次，全部 tensor／auxiliary 位元相等。
  經 canonical durable cache 安裝、正式 cache-only owner 及獨立 CUDA-cold
  回讀，5／5 可用。只重載合格閒置的 Discord consumer；新 owner 的 code
  准入、完整數值、Gateway／commands／engine ACK 均接受，無 GPU handles、
  新暖機或 fatal。03:41 的 67／67 managed Linux processes 均可讀且無 GPU
  handles；不是 Windows／整卡零顯存或遊戲 FPS 保證。穩定 20 秒取樣的
  Discord 為 0.0141 CPU core、516.04 MiB anonymous RAM；不是全機 A/B 降幅。
  本輪 182 項共享回歸通過。Bybit 已確認是失效的 immutable-cache symlink，
  正在以固定原 release 完整校驗／還原到 mutable workspace；未宣稱已追新。
- **盤後 GPU 閒置已正式採用。** 19:55 僅重載合格閒置的 Discord owner，
  19:56 獨立原生驗收：五模式完整輸出、目前正式鍵、engine ACK 全部一致，
  新 owner 沒有 GPU handles／暖機／模型重算。另取樣 61／61 本機 managed
  processes，GPU owner／handles 均為零；不代表 Windows／整卡 VRAM 為零。
  收盤快取以完整輸入／模型／程式世代為界；來源繼續追新，輸入不變不重算。
  22:29 最新 54／54 managed processes 均可讀、GPU handles 為零。21:39
  V8／22:08 multi_basis_22 曾正確拒用更新世代；22:26 已確認原一分鐘
  cache owner 自動恢復到當前五模式，零 model／CUDA／provider 呼叫。
  本次 100m／multi_basis／V8 確有輸入 hash 變化，不是 config／code
  變動；factorized／22 的 pure-forward 鍵未變。新資料不能硬沿用舊摘要。
  **23:48 再正式載入程式世代保護。** 新 Gateway 已回報固定的啟動版本；
  23:49 獨立驗收五模式快取鍵／摘要 SHA 未改，六個受保護 owner 未變，
  零 GPU handles／暖機；這是載入與快取重用驗收，不是新的 GPU 數值對照。
  23:52 的最新取樣為 73／73 managed Linux processes 可讀、GPU handles
  為零；Discord CPU 約 0.0135 core、匿名記憶體約 503 MiB。Windows
  顯存／遊戲 FPS 仍未全部歸因。
  **10/15 01:03 再次獨立回讀五模式當前收盤快取全部通過**：直接呼叫
  canonical consumer，禁止模型／網路／CUDA 操作；五個正式鍵與 10/14
  13:30 收盤相符，模型前推、provider 呼叫與 CUDA 初始化均為零。
  00:59 取樣的 56／56 可見 managed Linux processes 無 GPU 裝置句柄；
  不以程序數變動推論服務停止，也不充作全卡／Windows 顯存證明。
  **10/15 01:56 當時的狀態已不同**：62／62 可讀 managed 程序仍無 GPU
  handles，但五模式 canonical consumer 全因來源／程式世代變更拒用，
  零模型／provider／CUDA 操作。三份 factorized 資料／模型程式已在其他
  工作中改變；01:03 的成功不延伸為這一世代成功。本輪未重啟 GPU owner、
  未改寫其程式、未以舊快取冒充目前結果；後續須重新驗收正常更新流程。
- **10/15 01:51 唯讀 TAIFEX 面板已採用同 generation 的完整驗證證明重用。**
  固定 5.15 GB 有效真實來源、八輪七區間增量刷新／durable readback，
  第一階段 wall 少 11.5%、CPU 少 6.5%，SHA 驗證量少 44.3%；加入暖讀
  後的完整兩階段中位數 wall 少 7.1%、CPU 少 1.2%。七區間全輸出一致，
  一日原始解析器獨立對照一致；不是全機降幅。新 owner 七區間時鐘與
  重載後增量收據全部通過，行情／模擬交易 owner 未變；161 回歸通過。
- 閒置訊號／收盤排程的四輪原始 caller 對照已接受：每 30 秒 CPU 平均
  1.654→0.0348 秒（少 97.9%），各 task 300→6 次喚醒；截止前 30 秒、
  未完成鍵及交易確認仍維持 100 ms。這是兩個閒置 callback 的量測，不是
  全機 CPU 降幅；19:55 已載入。19:51–19:53 五模式目前正式鍵及 15 次
  獨立完整回讀通過，模型／CUDA 呼叫被禁止，輸出仍與 fresh GPU 控制一致。
  16:13 的舊世代證明不再充作當前驗收；這也不是券商成交或五模式完整歷史
  都已完成的證明。新版來源 owner 已自動補完後續當沖資格／法人資料換代。
- 五個台股模式的收盤完整快取，盤前按當時程式／來源世代回讀通過：零模型前推、
  CUDA 未初始化。Discord 盤前常駐窗口改從預設 08:05 開始，正式冷 owner
  與 08:08 的自然暖機均已接受；成功暖機耗時 109.097 秒，早於 08:15。
  09:00 的端到端延遲未達標，原紀錄保留；不能把盤前驗收套用到後續換代。
- 自然暖機的三次股票尾端讀取合計 82.511 秒，是目前準備熱點。完整
  全市場對照已否決較小讀取門檻（牆鐘反增 27.3%、CPU 增 30.1%）；
  有界原始 Arrow 尾端共用的完整四輪對照通過：workflow 牆鐘少 27.3%、
  CPU 少 35.1%，peak RSS 增 4.6%。canonical hook／idle clear 接入後，
  實際原始 caller 的新四輪對照亦接受：wall 少 27.3%、CPU 少 37.8%、
  peak RSS 增 4.4%，全部五模式 BASE fields 一致；正式載入仍另驗。
- 永豐完整本機稽核的六程序對照：牆鐘少 20.2%、CPU 秒少 19.4%、
  峰值 RSS 少 35.1%，全待辦／三摘要／寫檔一致。選出的容量已成預設，
  未替換 memo 的完整正式函式回讀亦通過；不是 API 全批下載倍率。
- 主機清冊為 87 個已安裝服務、67 timers、3 paths；07:51、暖機前的
  79 個可讀 StockAgent Linux 程序均無 GPU 裝置 handle。這不歸屬所有 Windows
  顯存，也不證明所有服務已達最優。18:24 最新 70／70 managed Linux 程序
  可讀，仍只有舊 Discord 的兩個 GPU handles，沒有宣稱新閒置程式已正式採用。
  FinMind 新索引已在 09:06 的真實排空
  等候窗口正式載入；原正式歷史失敗及今日開盤恢復仍是未完成項目。
- 開盤來源準備的 1,200 秒期限與 180 秒推論 watchdog 衝突已修正。
  原生 owner 維持不變，來源準備自然完成約 486 秒，第五模式隨後產生訊號；
  來源完成與五模式引擎 acknowledgement 已讀回接受，不是準時開盤或券商成交。
  完整來源準備的正式快取 caller 四輪對照已接受：wall 少 31.4%、CPU
  少 35.2%、peak RSS 少 39.8%，全部 BASE／delta／154 個輸出 members
  與原版一致。原始切片方案的 wall 少 8.0%、RSS 反增 3.5% 仍保留，
  不混作同一方案；新增記憶體 miss admission／既有隔離 owner 恢復後，
  最新完整暖讀已與原始 owner 全輸出一致，updater 於 13:19 啟用 canonical
  annual cache 與實際 Polars／Arrow pool 記錄。16:04 自然完成的原生 updater
  已讀回 PL2／Arrow2、四次 annual hit／兩次 build；不是把私有 warm benchmark
  的降幅套用到新資料正式工作。13:40
  正確等待今日官方日資料，不把昨日資料或 provisional snapshot 當完成。
- 盤後一次性 allocator 回收已補 pinned-host 公開 API、CPU-only 快取與
  模型先被失效清空後的待回收狀態。四個 fresh CUDA 程序的有界對照中，
  原版留下 256 MiB 閒置 pinned RAM，新版全數歸還；active sentinel／小型
  模型完整輸出不變，五次後續 idle 檢查零 CUDA／GC。290 項共享回歸通過，
  不是五模式正式推論或整卡 VRAM 歸零的 acceptance；服務採用仍另讀回。
- OpenBB 的 32,768 檔來源工作量原先超過 8,192 檔索引入口而退回昂貴查詢；
  已修正入口邊界。完整固定 1,329 萬 task 清冊的四輪對照中，該搜尋階段
  wall 少 58.2%、CPU 少 32.1%，RSS 增 0.9%，不是全 compaction 倍率。
  16:50–16:55 正式排程自然載入新入口：索引搜尋 16.58 秒／CPU 9.28 秒、
  pending/new/stale/failed 為零，來源及壓縮後 334,949,985 rows 收據一致。
- OpenBB 物理 schema 重複計算的全 185,963 檔固定來源四輪對照接受：
  metadata 階段 wall 少 33.3%、CPU 少 29.4%、peak RSS 少 4.5%；暖快取
  原版／候選 CPU 59.92／42.96 秒，完整 50,722 份 proofs SHA 一致。
  21:22 的正常 owner 已正式採用並獨立驗收，沒有重啟服務／提高 memory cap。
  該次碰上每日全來源 SQL 稽核，整段 1,258 秒中 SQL 為 1,154 秒，footer
  為 62 秒；不是全 workflow 少 33.3%。完整 SQL 的磁碟瓶頸仍需處理。
  22:14 下一次正常 owner 確實沿用 verified journal：無來源變更時 SQL
  為零，但 footer 209.642 秒／索引搜尋 87.265 秒，整段 336.207 秒。
  不隱藏此慢樣本；已開完整私有 SQL 快照／covering-index 成本實測，
  未在正式庫安裝新索引，也未提高 3 GiB cap。
  **10/15 00:13 完整四輪 SQL 對照已完成。** 固定完整清冊下，SQL 平均
  2,306.950→193.496 秒，CPU 458.981→61.316 秒；含完整 SHA 等工程
  驗證的 worker 平均 2,390.953→263.078 秒。輸出與來源身分一致；不是
  全 compaction 倍率。新索引增加 3.54 GiB；10/15 00:53 完整私人
  寫入四輪已完成，固定 1,024 筆原生操作 wall 平均增加 0.229 秒、
  CPU 增加 0.148 秒。驗證索引的 canonical lookup 已接入並通過 607
  項回歸，**正式庫仍未安裝實體索引**；缺失時沿用原查詢，不宣稱
  正式服務已獲得該完整 SQL 加速倍率。
- 完整歷史工作另有「圖表是同一 inode 卻重複複製」及「30 分鐘逾時後從頭
  重建 1,613 個分鐘 session」兩個原因。圖表修復通過 131 項生命週期測試；
  V8 完整歷史預算改為 3,600 秒，120／600 秒啟用／收盤期限不變。
  新完成判定、calendar preflight、失效 ready hint 的同 owner 恢復、閒置／快取
  共享回歸最終 618 passed。17:11 的正常 serial owner 已補完 100m／multi_basis
  的 matching full completion proof；V8 雖完成 1,613 sessions，仍因 fullgraph
  推論錯誤未完成。已修復編譯邊界並通過 103 項模型／快取回歸，原生完整
  V8 工作在正常 18:59 retry owner 下於 19:20 完成完整推論／stitching／圖表
  及 matching completion marker 至 10/14；不是所有後續來源修訂皆已注入的
  全歷史資料／模型數值獨立驗收。
- 目前未完成的明確範圍：factorized 的 210 日 live projection 不等於
  3,116 日完整歷史（缺少 2,906 個 benchmark sessions）；已於 0.078 秒前置
  拒絕錯誤 scope、沒有 child／GPU 呼叫，仍需正確完整輸入。100m、multi_basis
  與 multi_basis_22 已有 10/14 matching lifecycle hint；該 cheap scheduling hint
  不替代 source／模型數值及全部報表內容的驗收。V8 matching marker 已到
  10/14；factorized 的完整 scope 尚待補齊。全 87 installed services 逐項
  最佳化仍在進行，Gateway 新閒置程式已正式採用。

<details>
<summary>前序部署／來源世代／逐階段驗收紀錄</summary>

盤後 GPU 階段已完成目前五模式正式快取與零 CUDA 回讀，詳見
[盤後推論快取驗收](completed_session_inference_cache_2026-10-13.md)。
10 月 14 日 01:44 的再驗收正確拒用舊程式世代的快取；01:59 核對為
`stockagent/config.py` 新增訓練 optimizer 設定，並非已證實的行情資料缺口。
03:15 已用目前五個正式鍵、15 次獨立完整訊號讀取重新驗收，與原先獨立
控制的完整 SHA256 一致，CUDA 未初始化、GPU 配置量為零。
03:27 只重載 Discord 與唯讀網頁入口；03:32 再驗收通過，交易與行情
捕捉程序未重載。這不代表原有兩個正式歷史工作失敗已解決。
04:10 期貨唯讀展示已載入經完整對照的四執行緒與局部 allocator 回收設定；
04:12–04:13 主機取樣中該程序 RSS 約 171 MiB，原交易與行情捕捉 owner 不變。
04:15 台股五模式再次通過目前正式鍵、獨立完整輸出與零 CUDA 驗收。
04:39 只重載網站唯讀入口，04:47 四種當前資料分頁／搜尋／換代的 HTTP
內容逐 byte 與獨立原始解析一致；原交易、行情捕捉及下載器 owner 不變。
04:54 最後再次驗證五個目前正式鍵及獨立完整控制：零 model forward、
零 provider call、CUDA 未初始化，沒有交易寫入。
05:34 僅重載唯讀網站入口，日期目錄預熱已正式載入；05:35 完整歷史
341,952 點及最新單日 2,192 點的 HTTP bytes 均與重載前一致。
05:36–05:37 正式入口 RSS 約 769 MiB，詳見下方日期目錄階段；
不把有界流程的 36.4% RSS 降幅當作全機或整個 gateway 的降幅。
05:22 隔日當沖資格表更新，使 04:54 的摘要來源世代不再適用；
05:37 起五模式完整重播仍全數命中原 forward cache，CUDA 禁用且
完整訊號與獨立控制一致。來源世代與純模型輸入快取是兩個不同契約。
05:39 canonical cache-only 維護器在模型／CUDA／行情 API 禁用條件下
更新五模式摘要；05:41 再次正式讀回均命中原 forward key、零 GPU，
最新來源世代已接受，未啟動兩個失敗的完整歷史工作。
05:56 原生監聽器已補上兩個 canonical 資格檔；05:58 工程通知真實
喚醒既有 cache-only owner，五模式摘要／weights／forward key 均不變。
06:00 正常三來源 watch set 與停止後維護器讀回通過，沒有新增輪詢 daemon。
06:07 起對永豐歷史回補做非阻塞取樣；已定位每批三次全庫摘要內的
重複收據遍歷，單次遍歷修正與 256 項共享回歸通過。06:51 完整真實
清冊 ABBA 通過所有待辦與摘要／寫檔對照；中位牆鐘減少 48.7%、CPU
減少 36.9%，但首輪明顯冷讀、RSS 中位增加約 24%，不能當整體最佳化。
06:57 起同清冊的隔離快取容量 ABBA，完成後因牆鐘／CPU 變慢而未採用。
07:15 原歷史 owner 正常批次完成，正式單次遍歷載入已接受，未重啟下載器。
07:19 僅重載 Discord，修正早於準備窗口長時間常駐 GPU；新 owner 五模式
快取及零落後同步通過，無 GPU device handle。詳細原生前後讀回在下方。
07:33 完成新的主機取樣：87 個已安裝服務與一個工程 transient 分開列出；
Discord 匿名記憶體約 519 MiB、CPU 約 0.049 core，未混成全機最佳化。
完整永豐 audit profile 亦已通過原始全輸出 oracle；後續 owned-FD 雜湊
修正通過 80 項測試，正做獨立舊程式的完整平衡對照，尚不宣稱加速。
以下 FinMind／OpenBB 及 21 時 52 分主機數據保留原觀察時點，
不當成後續重載的現況；全服務目標仍待逐項 profile 與驗收。
10 月 14 日續作的 common OHLCV reader 有界完整對照與 Bybit 路徑診斷，
見 [分鐘行情讀取最佳化](ohlcv_bounded_read_optimization_2026-10-14.md)。
10 月 14 日 00:47 起，原全資料面板排程已載入專用清冊編碼器；
兩輪完整清冊重播降幅 15.3%／29.2%，300 項共享回歸與正式 reader 相容性通過，
詳見 [清冊編碼最佳化](data_monitor_record_encoding_optimization_2026-10-14.md)。
這是內部 metadata 編碼改善，不是整個狀態服務或全機的相同倍率。
10 月 14 日 01:34 的 FinMind 完整狀態查詢對照亦通過；牆鐘減少 45.3%、
CPU 減少 46.5%。正式 owner 尚未載入：連續歷史批次沒有合格空閒窗口，
本次安全重載觀察已明確延後，沒有強行中斷下載。細節見下方新階段。

FinMind 每分鐘預估的重複清冊掃描已移除，正式取樣程序已載入新版。
OpenBB 改為保留來源標記的字典編碼並分批驗證，完整抽樣檔案驗證通過；
已開始的舊版全庫驗證未被中斷，在 10 月 14 日 00:22 自行結束但未通過
歸屬檢查；下一次程序才載入新版。詳見下方全量 audit 結果。
行情擷取、交易帳本、checkpoint、特徵、精度及配額上限均未修改。

</details>

這是本機服務普查與已定位熱點的有界最佳化，不是所有服務、所有資料及遠端
訓練已達理論最快的宣告。效能、資料完整性與服務健康各自驗收。

目前收盤快取的版本驗收與載入證據：

- [目前五模式完整輸出與零 GPU 對照](../artifacts/operations/running-services-optimization-20261013/postclose-current-code-full-output-acceptance.json)
- [只重載兩個讀取服務及保留交易／捕捉身分](../artifacts/operations/agent-workflow/runs/current-cache-readonly-consumer--20261013T192732-168c570d/run.log)
- [重載後五模式正式回讀](../artifacts/operations/running-services-optimization-20261013/postclose-after-consumer-adoption-20261014.json)
- [03:33 全服務取樣](../artifacts/operations/running-services-optimization-20261013/census-after-readonly-adoption-20261014.json)
- [目前公開模式／訊號頁／原交易程序回讀](../artifacts/operations/running-services-optimization-20261013/postclose-current-public-owner-readback-20261014.json)

## 已驗收的改善

下列為每版三個獨立程序的中位數，交錯執行。主機上仍有其他服務；
不清空 OS page cache，不把本次倍率當跨日期或跨硬體保證。

| 完整受測操作 | 原版 | 新版 | 改善 |
| --- | ---: | ---: | ---: |
| FinMind 完整 ETA 更新牆鐘 | 29.746 秒 | 12.590 秒 | 減少 57.7% |
| FinMind 完整 ETA 更新 CPU | 28.466 秒 | 12.405 秒 | 減少 56.4% |
| FinMind 程序峰值 RSS | 163.76 MB | 163.50 MB | 大致不變 |
| OpenBB 40 檔完整檔案驗證牆鐘 | 0.969 秒 | 0.284 秒 | 減少 70.7% |
| OpenBB 同一驗證 CPU | 1.505 秒 | 0.395 秒 | 減少 73.8% |
| OpenBB 程序峰值 RSS | 376.21 MB | 224.64 MB | 減少 40.3% |

CPU 秒包含工作執行緒，與牆鐘秒不同。MB 採十進位；RSS 取 worker 自身
`/proc/self/status` 的 VmHWM，不採 fork 繼承的父程序 rusage。

FinMind 使用唯讀 online backup 的固定佇列／流量快照，保留 106 個資料集，
完整 workload、telemetry、secondary admission 及所有階段 ETA 的 SHA256
六次完全一致。各 owner 分別凍結，不宣稱跨 owner 原子發布。
OpenBB 使用 10,000 個成功候選中的 32 個前段樣本與 8 個大表，合計
40 檔、20,312,780 bytes、1,921,111 筆。這是刻意包含解碼重表的抽樣，
不是全庫平均分布，也不是全庫下載吞吐量。兩版沿用同一個 canonical
`_audit_success_files`，所有語意結果一致；只排除原有的兩個 stopwatch 欄位。
兩項 benchmark 均沒有 provider 呼叫、正式佇列寫入或原始來源修改。

收據：

- [FinMind 最終完整對照](../artifacts/benchmarks/running_services_20261013/finmind_eta_final.json)
- [OpenBB 最終完整抽樣對照](../artifacts/benchmarks/running_services_20261013/openbb_audit_final.json)
- [初次主機基線](../artifacts/operations/running-services-optimization-20261013/baseline.json)
- [包含主機服務的最後取樣](../artifacts/operations/running-services-optimization-20261013/final.json)
- [程式身分與正式取樣驗收](../artifacts/operations/running-services-optimization-20261013/acceptance.json)

## 計算成本與根因

端到端時間包含來源允許的等待、網路、解碼、驗證、落盤及發布。吞吐量受
配額、網路、處理與儲存中最慢的環節限制；多開執行緒不等於增加帳號配額。
本次先移除重複處理，再量測，不跳過驗證、不縮減模型輸入、不換近似公式。

FinMind 的初次 cProfile 中，完整 ETA 約 35.6 秒，
`build_finmind_workload` 被呼叫兩次：ETA 先掃清冊，次要校驗准入又重新掃一次。
telemetry 還重新計算 queue state counts 與已接受分區的 renewal counts。
profile 含量測額外成本，沒有混入上述未 profile 的比較。

現在同一次更新共用唯讀交易取得的暫存聚合。聚合只能用於完全相同的
佇列路徑與 UTC 觀察時點；缺表、讀取錯誤、時點／owner 不匹配保持原本的
unknown 或 fail closed。實際下載 dispatch 仍重查即時工作與共用配額，
沒有把 ETA 快照當成下載許可。

有限優先工作查詢從掃全部 tasks 加上逐筆 EXISTS，改為優先名單外層搭配
task 主鍵查找。DISTINCT 保留舊名單重複 key 時的 EXISTS 語意。
這沿用 SQLite 的明確 join 順序，沒有新增高成本寬索引，也沒有強制所有
GROUP BY 改走 NOT INDEXED；後者的實測沒有改善，未採用。
[SQLite query planner](https://www.sqlite.org/optoverview.html#manual_control_of_query_plans_using_cross_join)

OpenBB 需要核對第一筆來源標記，原版卻先把整個首 row group 的五個重複字串
欄位展開。現在先做原有 footer／schema 檢查，對大於 8,192 列的首 group
重用 footer 開啟 dictionary reader，以 65,536 列 batch 消費完整首 group。
所有原有 requested pages 都解碼，後段壞頁仍會失敗；不是只讀一筆略過驗證。
小檔不重開 reader；缺欄位仍分類為 metadata mismatch，而不是提前被 Arrow
誤分類成 unreadable。零列、encoded wrapper、擁有權、筆數與來源比對都保留。
[Apache Arrow ParquetFile](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html)

## 所有運行服務的清點

最後一次 20 秒級取樣於台北 21:52 完成：86 個已安裝 StockAgent services、
2 個工程 transient services、67 個 timers、3 個 paths。
已安裝服務中有 18 個 running daemon、4 個 activating 工作。
主機 system manager 合計記錄 60 個 active／activating service units，
其中 39 個 running／activating，其餘包含 active exited 的初始化工作。
兩個主機 CPU 計數無法形成有效 delta，保持未量測，不當成零。
基線是 85 個已安裝服務及 66 個 timers；期間其他工作新增了服務，兩份清冊均保留。

CPU cores 是取樣 CPU 秒除以取樣牆鐘，1 代表一個 logical core 忙碌，
不是全主機 100%。以下為最後取樣的主要本機服務，括號內以 core 表示；
完整每個 unit 的 PID、狀態、匿名記憶體、file cache、I/O、歷史收據與 timer
皆在上方最後取樣 JSON。inactive 與 failed 也保留，不會從清冊消失。

| 服務或責任 | 取樣結果與處置 |
| --- | --- |
| OpenBB archive | 0.911 core；正在收尾的舊版完整檔案驗證，非空轉輪詢；新版不強制中斷現有 audit |
| FinMind complement | 0.291 core；抓取與處理仍運行；共用帳號、日曆、release reserve、工作順序不變 |
| FinMind sponsor | 0.0002 core；沒有據此提高官方配額或判定已抓全 |
| FinMind free | 0.0001 core；等待／idle heartbeat 與來源完成證明保持分開 |
| FinMind quota snapshot | 每分鐘短工作；新版正式 health 已成功產生，實際更新約 12～18 秒隨主機負載變動 |
| Public dashboards | 0.021 core；資料頁與狀態投影可讀，HTTP 200 不等於 data health 合格 |
| Discord bot | 0.050 core，匿名記憶體約 2.53 GiB；重用今日已驗收的五模式暖機，不再重啟 consumer |
| TW day trade simulation | 0.0033 core；保留原 engine 與帳本，沒有改成交或會計語意 |
| TW overnight simulation | 0.0005 core；保留交易／特徵時鐘 |
| Shioaji TAIFEX bidask | 0.131 core；夜盤擷取受保護，未重啟或更改 broker session |
| Shioaji TAIFEX dashboard | 0.011 core；file cache 明顯多於匿名記憶體，不據此宣告記憶體洩漏 |
| Shioaji historical market data | 輕量 waiting wrapper；沒有據此宣告歷史回補已完成 |
| Shioaji minute backfill | 輕量 waiting wrapper；不額外登入、不製造分鐘值 |
| Shioaji top200 | 輕量 waiting wrapper；保留 canonical owner |
| Shioaji TX history backfill | 輕量 waiting wrapper；保留配額與既有排程 |
| TW public source events | 0.028 core；保留來源事件及發布時鐘 |
| TEJ history | 0.039 core；Windows Smart Wizard 的 UI／授權／輸出流程不以多執行緒強行平行化 |
| Storage lifecycle | 0.116 core；不刪資料、不變更 authority、lease、冷庫或發布流程 |
| Temporal | 0.022 core；等待與 durable orchestration，不當成無效 CPU 熱點 |
| Backup stream | activating，0.300 core；實際傳輸／備份成本，不以停止備份換取效能數字 |
| Legacy partition return | activating，0.513 core；保留回傳工作及恢復邊界 |
| Packed transport | activating；此取樣沒有有效 CPU delta，未量測而非零成本 |
| Data refresh status snapshot | activating；此取樣沒有有效 CPU delta，匿名記憶體約 900 MiB，仍需完整工作量 profile |
| News fulltext | 基線 0.126 core，最後取樣 inactive；新聞正有其他並行修改，未覆寫或歸為本次節省 |
| Syncthing | 0.198 core；記錄資源，不改同步或回收策略 |
| PostgreSQL | 0.0039 core；沒有清空 cache、VACUUM 或重啟資料庫 |
| OS 常駐服務 | systemd、DNS、cron、journald、chrony、snapd 等逐項記錄；沒有足以支持調整的熱點 |

GPU 三次取樣為 6～11%，已用記憶體約 2.8～3.1 GiB；是瞬時主機數字，
不是推論耗時，也無法據此把所有 Windows／GPU 工作歸屬某個模型。
主機最後 CPU PSI some avg10 約 3%、I/O some 約 3.42%；
不是全 CPU 或 GPU 全天滿載的證據。OpenBB 最後 cgroup file cache 約
15.93 GiB、匿名記憶體約 130.5 MiB，兩者不能都當成 Python RSS。
最後 OpenBB archive 記憶體 high／OOM 事件取樣增量均為零。
另有匹配到的 L1 compaction 歷史收據記錄 3,353 次 memory high 事件，
這是該次批次的壓力證據，不是當前 archive daemon 的 OOM。

## 推論與上線界線

重用今日既有的完整 live inference 驗收，而非另改模型：第五模式的熱推論
中位數為 0.571 秒，完整輸入與輸出 SHA 相等；五模式正式 consumer 暖機
10.925 秒、5／5 ready。這不是本次新做的加速，也不證明下一次開盤的
取價、下單或成交延遲。[完整推論驗收](tw_day_trade_live_inference_optimization_2026-10-13.md)

FinMind quota timer 仍每分鐘取樣，使用 official account 的動態上限；
21:57 的 SponsorPro 收據為 20,000 requests/hour。本次沒有改流量上限、
固定追新 reserve、失敗重試政策或歷史優先序，沒有額外消耗 provider 額度。
OpenBB 的現有 audit 不 hot reload；後續 canonical supervisor 的獨立 audit
會讀新版。不得拿抽樣通過冒充正式全庫驗證完成。

最後服務清冊仍有 boot recovery audit、Discord artifact maintenance、
registered intraday、Vast bulk return 四個 failed units；intraday 的匹配收據
指出 Bybit perpetuals failed step。六個本機公共狀態頁雖然可讀，仍有
blocked／degraded／starting 狀態。這些健康或資料缺口不因效能測試而消失。
正式 OpenBB 全庫收據、盤後維護恢復及下次真實交易時段的端到端驗收
仍是後續事項；本次沒有把它們改成 complete。

## 10 月 14 日全量 audit 結果

00:22:10，原先執行中的 OpenBB archive 自行退出，systemd 為 failed／exit 2，
不是本次終止或重啟。其 [唯讀收據核對](../artifacts/operations/agent-workflow/runs/openbb-terminal-audit-readonly-20261013T163114-14a44841/run.log)
保留全量 audit JSON 的 SHA256、寫入時點與原 invocation。
JSON 的 `checked_at` 為 10 月 13 日 20:48:34 的開始取樣時點，不能當成完成時點；
檔案於 10 月 14 日 00:22:09 發布，owner 隨後退出。

成功工作的 7,326,790 個檔案、334,814,433 筆的既有受測範圍中，缺檔、
不可讀、manifest 筆數不符、來源 metadata 不符皆為零。這不是所有 payload
欄位／所有 row groups 的逐值驗證。catalog follow-up audit 亦通過，
13,148 個 parent 檔對應 1,126,953 個必要 unique follow-ups，缺漏為零。

真正未通過的是來源檔案歸屬：共掃描 7,473,976 個 Parquet，
146,575 個 inactive manifest-success 歷史檔正確保留；另有 611 個檔案沒有
合格的 active／inactive-success owner，或屬 foreign-active 範圍。
目前 samples 沒有對這 611 個逐一提供細分類，不能直接說是毀損、沒有價值，
也不能改成忽略而宣稱 audit 通過。下一階段沿原 owner 核對 task／來源 metadata／
回收與保留契約，保留原檔，不重新呼叫 API 或建立空資料替代。

完整舊版 ownership index 建置花 1,031.03 秒，掃描 691.32 秒；這是另外的
全量瓶頸，不由先前 40 檔 metadata benchmark 證明已最佳化。
沒有啟動下一次幾小時級的全庫重跑、compaction，亦未清除 failed 狀態。

最新 [00:22 服務普查](../artifacts/operations/running-services-optimization-20261013/postclose-current-census-20261014.json)
仍涵蓋 86 個已安裝服務、67 個 timers、3 個 paths；OpenBB 已退出，不能再列為
目前正在消耗 CPU 的 audit。此取樣的主要活動為 registered intraday、
storage lifecycle、FinMind complement、歷史 partition return 及 backup stream；
仍需各自端到端 profile 與同輸入驗收，不以停止必要工作換取 idle 數字。

## 10 月 14 日 FinMind resident owner 狀態查詢

20 秒的 live stack 取樣把主要主執行緒活動定位到 canonical `_status`，
不是單純多開網路 worker 就能消除的成本。約 316 萬個 task 的聚合原先
使用 `(dataset,state)` 窄索引，再回查 table 取得 rows／bytes／日期。
採用涵蓋這七個聚合欄位的 covering index，減少 retained-history table 查找；
沒有改工作順序、交易日曆、release reserve、來源請求範圍或官方帳號配額。
依據 [SQLite covering index](https://www.sqlite.org/queryplanner.html#covering_indexes)。

固定唯讀 online backup 的 3,162,288 列，兩份 byte-identical 起點，凍結同一
觀察時間及 symbol 名單，交錯 ABBA 共 12 次。受測操作是完整 `_status`、
dispatch／history frontier／retry policy 與原子 JSON 發布，不只是 SQL microbenchmark。
所有狀態輸出的語意 SHA256 相同，沒有 provider 呼叫或正式佇列寫入。

| 同輸入完整操作 | 原版中位數 | 新版中位數 | 改善 |
| --- | ---: | ---: | ---: |
| 狀態查詢牆鐘 | 2.639 秒 | 1.444 秒 | 減少 45.3% |
| 狀態查詢 CPU | 2.623 秒 | 1.403 秒 | 減少 46.5% |

新增索引約 289,116,160 bytes（275.7 MiB），不刪原有索引。每輪也實測
64 個實際 task 的 canonical claim／release、128 次 WAL／full-sync transactions；
還原 task 內容並保留原有 unrelated in-flight claim。寫入牆鐘沒有觀察到劣化，
但不宣稱寫入 CPU、RAM、API 吞吐或整台電腦都有相同改善。

兩個負例保留：新增索引後未更新 bounded planner statistics 的完整操作變成
約 26 秒，強制 table scan 亦變慢，均未採用。新索引另曾搶走 fresh queue 的
dispatch plan；修正為保留既有 pending／due-refresh partial-index seeks，
不是移除失敗測試。最終 381 項受影響共享回歸通過。

部署仍待正式 owner 載入。01:40 起的 300 秒觀察沒有取得 coherent waiting、
零 active task／in-flight claim 及足夠 deadline 的批次邊界，收據為
`deferred_no_safe_idle_window`、`restart_requested=false`。FinMind 原 invocation
不變，交易與 TAIFEX 夜盤 owner 也沒有重啟。不要把 offline 測試通過說成
正式下載器已提速，也不要對連續忙碌程序重複盲目輪詢或清除 claim。

09:05 發現原生 `protected_opening` 已是完成週期後的真實 waiting，active task
與兩類 in-flight claims 都為零，deadline 為 09:10。舊部署工具只認
`batch_complete/current_queue`，因此把這個安全窗口也拒絕了。Complement
的此 wait 沒有 `wake_if`，只有 `incremental_reserve` 會提早按 quota 喚醒；
修正沿用 canonical 交易日曆 gate，只在仍受 opening protection 時接受
該理由，並在 preservation sample 後、dispatch 前重新核對完整 idle proof。
沒有創造等待、清除 claim 或打斷 batch。35 項相關回歸通過。

09:06 的正式採用已獨立讀回：FinMind MainPID 176 → 1,920,404，InvocationID
`363d312b0ccd402894ac5dc6574f9006` → `165a95f3e8514ad491fdb7cb28f61654`，
新 schema statistics 為 3,271,139 個 task，aggregate plan 使用
`COVERING INDEX idx_finmind_status_covering`，status 時點晚於 dispatch。
原交易／TAIFEX 行情 owner 不變，所選最後來源 receipt／parquet hashes 不變。
這是正式 schema/status 載入驗收；抽樣保存不是全歷史完整性證明，也不把
離線 45.3% 當成正式 API 吞吐倍率。上述 01:40／07:02 延後收據仍保留。

收據：

- [完整 ABBA、plan、寫入與程式世代對照](../artifacts/benchmarks/running_services_20261013/finmind_status_dispatch_final_20261014.json)
- [live CPU 取樣](../artifacts/benchmarks/running_services_20261013/finmind_live_cpu_20261014.json)
- [381 項回歸](../artifacts/operations/agent-workflow/runs/finmind-status-all-shared-contra-20261013T174041-1d214787/run.log)
- [安全重載延後](../artifacts/operations/running-services-optimization-20261013/finmind-status-index-rollout.json)
- [35 項日曆保護、排空、race 與原 schema 回歸](../artifacts/operations/agent-workflow/runs/finmind-protected-drained-reload-20261014T005936-534a13c5/run.log)
- [09:05 真實 waiting／零 claims／同 owner 只讀准入](../artifacts/operations/running-services-optimization-20261013/finmind-protected-drained-index-admission-20261014.json)
- [09:06 正式採用、planner／新 status／保存來源與交易 owner 讀回](../artifacts/operations/running-services-optimization-20261013/finmind-protected-drained-index-adoption-20261014.json)
- [盤後拒用原因唯讀核對](../artifacts/operations/agent-workflow/runs/postclose-source-identity-diff-20261013T175922-9c48462f/run.log)

重現使用 `run_fintech_python -m scripts.benchmark_finmind_status --help`，指定新
output；不要直接修改 live queue 的 schema。正式重載沿
`scripts/reload_finmind_at_idle.py` 的接受收據／loaded-code／claim／deadline
檢查，沒有合格窗口就保留 deferred，不能把啟動成功當成部署驗收。

## 10 月 14 日 FinLab dispatch 恢復

02:01 的新普查有 88 個服務、67 個 timers、3 個 paths。數量含新工程 transient
及其他並行工作，不當作本次新增常駐服務數。FinLab quota 02:02 的官方收據尚有
1,839.262 MB，實際下載 unit 卻未啟動；其 relative timer 的 next_elapse 為 0，
下一個固定觸發要等 07:00。原 `OnUnitInactiveSec=1min` 缺少本次 boot 的
第一次 deactivation，不能靠上次開機留下的 calendar stamp 自行開始 relative retry。
本機唯讀 admission 也確認 `allowed=true / workload_unverified_recovery`，
不是已證實沒有工作或官方額度用完。

新增 `OnActiveSec=1min` 為 timer 啟動種子，保留所有原有固定時刻、完成後一分鐘
重試、開盤保護、實際官方 quota 與 sync lock。只有一個 canonical service owner，
已有 active 工作時 timer 不會另啟動一個並行 owner。
[systemd 官方 timer 定義](https://github.com/systemd/systemd/blob/main/man/systemd.timer.xml)

沿現有 installer 新增 `finlab-timer-only` 模式，只驗證、部署及重啟該 timer，
不覆蓋 FinLab worker／quota observer 或其他 dirty service template。
120 項相關測試通過，包含實際 installer 在隔離命令替身下的只改 timer 證明。
既有 boot audit 曾把 HEAD news timer 的合法 `OnBootSec` 排除；依官方定義
補入 OnBoot／OnStartup 獨立 anchor，沒有刪除或跳過原失敗測試。

02:13:10 原 timer 自動觸發，兩個原 ExecCondition 均通過，下載 owner 開始
執行。後續 real SDK 收據有 downloaded 更新且 provider_errors=0，pending
從 185、177、169、161 降至 153；這是 02:21 前的觀察，不是所有歷史已抓全，
也不是新增非商業授權、PIT 或冷庫發布權限的判定。當沖及 TAIFEX 夜盤的
原 invocation 未變。沒有以關掉必要抓取換取低占用，也沒有放寬額度或優先序。

收據：

- [02:01 全服務取樣](../artifacts/operations/running-services-optimization-20261013/current-census-20261014T0204.json)
- [本機 admission](../artifacts/operations/agent-workflow/runs/finlab-idle-eligibility-readonly-20261013T180334-d6d7196d/run.log)
- [120 項排程回歸](../artifacts/operations/agent-workflow/runs/finlab-timer-activation-contract-20261013T181009-c8584ef0/run.log)
- [只部署 FinLab timer](../artifacts/operations/agent-workflow/runs/finlab-dispatch-clock-only-deplo-20261013T181134-ae238937/run.log)

TAIFEX 展示在這次 20 秒取樣曾接近 0.97 core，但不是整小時持續滿核的證明。
普通 Python profiler 20 秒只有六個活動 samples；native 60 秒亦只有六個並有
81 個 sampling errors，不能用那些樣本百分比當作 CPU 秒或準確歸因。另有
約 4.0 GB 的 live marks 與 0.97 GB 的有效 replay；後續沿原完整曲線 projector
完成欄式 cache 的同輸入對照與唯讀展示 owner 重載，見下一節。

## 10 月 14 日 TAIFEX 唯讀曲線欄式快取

以原始帳本可驗證前綴及有效 replay 的私人副本，約 4.99 GB，執行完整
1d 曲線操作。原始／候選各四個獨立程序交錯 ABBA；最終程式世代的所有
金融欄位、排序、曲線、來源時計及 counts 的完整 JSON SHA256 一致。
新增完整 schema 的 Float64／Int64／nullable Parquet **派生投影**，不改帳本、
計算公式、價格／成交、資料單位或 quantization。這不是原始來源全集已壓縮成
35 MB：只有此既有 projector 消費的欄位，原始約 5 GB 全部保留。

| 完整 1d 曲線，最終 ABBA 中位數 | 原始 JSON reader | 欄式 reader |
| --- | ---: | ---: |
| CPU 秒，包含 native 工作執行緒 | 21.051 | 4.462 |
| fresh worker 峰值 RSS，十進位 MB | 4,250.42 | 163.28 |
| 牆鐘秒 | 4.026 | 4.115 |

CPU 約減少 78.8%、峰值 RSS 約減少 96.2%；牆鐘 **沒有改善**，小幅增加且
分布重疊，不能宣稱等待更快或全機等比例提速。首次建置另外需 10.39 秒，
CPU 29.08 秒／峰值 RSS 約 4.24 GB；真實捕捉尾端 256 KB 追加另外需
7.83 秒／CPU 10.53 秒。這些成本沒有省略，35.02 MB 的私有投影由原來源
核對後重建或更新。正式首次預建在獨立工程 process 完成，不把它保留在 server。

每個來源 generation 綁 dev／inode／size／mtime／ctime 與完整 SHA256；
來源改變先核對全部舊前綴，不假定只能 append。增量僅解析真實 tail，
舊內容被改寫則重新建立；payload 也驗證完整 SHA／schema。用 kernel flock
與 pinned FD 橫跨整次查詢、atomic／durable 發布；壞快取或 FIFO 回到原讀取器，
壞原始值保留原 parser failure，不報成功空集合。相同 process／generation 的
已驗證 source 與 payload proof 可重用，不每次掃描約 5 GB 的 hash。
底層沿 [Polars streaming Parquet sink](https://docs.pola.rs/api/python/stable/reference/api/polars.LazyFrame.sink_parquet.html)
及既有 canonical projector，沒有加裝 GPU 資料框架來消耗遊戲顯卡。

249 項共享回歸通過，另有 21 項 cache／runtime 證明及最後 14 項 runtime／
原嚴格時計測試（互有重疊，不加總為不同測試）。只有 canonical 唯讀 display
launcher 預設啟用，可用 `STOCKAGENT_TAIFEX_HISTORY_COLUMN_CACHE=0` 回原
reader。未修改 service unit／優先序、capture、provider 配額或 source publication。
02:37 僅重載展示 owner，invocation 改為 `841df5066b934760a2ef6a7b7ac1b4f0`；
原夜盤 BidAsk 與 TW 當沖 invocation 全程未變。後續收據確認新 owner 真的
更新私有 Parquet receipt，七個 API 區間的整體時計也已追上 source。

**整個策略資料就緒仍未通過。** 原 all-strategy 時計檢查保留並失敗於
`long_strip_1c2p` 的舊端點。02:30、部署前的原始 reader 固定副本已有同一
缺口，lag 約 1,926 秒；完整結果 SHA 仍等於原 ABBA 控制，證明不是新投影
丟掉資料。live strategy 仍 `alive=true`，目前新估值是 null，不當成已終止
策略或補造成交。新 runtime receipt 的 `deployed_physical_cache_verified`
只表示已部署且快取機制讀回，`api.strict_data_ready=false` 明列所有區間的
嚴格失敗原因，不能拿窄證明 supersede 原資料缺口。

02:42 的 30 秒主機取樣，展示服務為約 0.20 core，較 02:01 的 20 秒取樣
0.97 core 低；不同時段的工作量不相同，且另有並行工程與 FinLab 抓取，
不是受控全機倍率驗收。GPU telemetry 約 3–4% 是整卡即時值，不能證明
Windows 遊戲 FPS、某 process 的推論狀態或 CUDA 可用性。

收據：

- [最終完整來源 ABBA／冷建／真實追加／程式 fence](../artifacts/benchmarks/running_services_20261013/taifex_history_columns_final_balanced_20261014.json)
- [249 項共享回歸](../artifacts/operations/agent-workflow/runs/taifex-column-cache-shared-contr-20261013T183000-7244c6c7/run.log)
- [正式 cache 隔離預建](../artifacts/operations/running-services-optimization-20261013/taifex-column-cache-live-prewarm.json)
- [只重載唯讀展示](../artifacts/operations/agent-workflow/runs/taifex-display-cache-only-reload-20261013T183704-449500e6/run.log)
- [正式快取／所有區間／未變交易 owner 讀回](../artifacts/operations/running-services-optimization-20261013/taifex-physical-cache-data-boundary-readback.json)
- [原嚴格資料就緒失敗，未掩蓋](../artifacts/operations/running-services-optimization-20261013/taifex-column-cache-formal-readback.json)
- [部署前原始 reader 缺口獨立證明](../artifacts/operations/running-services-optimization-20261013/taifex-original-raw-gap-diagnostic.json)
- [後續全服務取樣](../artifacts/operations/running-services-optimization-20261013/current-census-after-taifex-cache-20261014.json)

### 02:49 追加競態與目前載入版本

前一版已降低重複解碼，但正式 source 在每分鐘分批追加時，generation proof
可能碰到正在寫入的帳本。不能把這當穩定 cache hit；立即退回原始 reader
又會產生多 GB 的瞬間工作集。現在只對明確的 source generation race 最多
重觀察三次，每次仍做完整 prefix／inode 核對；超過上限才沿原 reader。
未增加睡眠、永久錯誤重試或未驗證 prefix 的旁路。

最終 bounded-race 程式版另跑 238 項共享回歸與四加四 ABBA：完整輸出一致。
原始 CPU 21.658 秒／候選 5.420 秒，約減少 75.0%；峰值 RSS 4,255.70 MB
至 164.72 MB，約減少 96.1%。牆鐘 5.222 至 5.242 秒，仍無改善。
冷建 8.30 秒／CPU 27.73 秒，真實尾端追加 5.59 秒／CPU 9.05 秒，皆另列。
這才是目前 helper 程式 fence，前一版測量保留歷史，不改寫成新版結果。

另外在同一個常駐 process 對不變 source 各跑三次完整 1d builder：欄式版
第一次約 4.77 秒核對 source，其後約 0.241／0.245 秒、CPU 0.465／0.400 秒；
原 reader 第二／三次約 3.70／4.29 秒。所有完整輸出 SHA 一致。這是額外
resident reuse 檢查、不是交錯 ABBA 或追加工作量，也不宣稱整個 API 同倍率。

02:55 最新版在隔離 worker 重讀既有正式投影，無須再解碼全帳本；峰值 RSS
160.79 MB。02:56 再次只重載展示 owner；目前 invocation
`083d117663d8410db7d6d0f2d2eaee56`，兩個受保護交易／capture owner 仍未變。
最新版 readback 已確認 Parquet receipt 由新 owner 推進，原策略估值缺口
仍明列 `strict_data_ready=false`，沒有修改、跳過原完整資料就緒檢查。

- [目前 bounded-race 完整對照](../artifacts/benchmarks/running_services_20261013/taifex_history_columns_race_final_balanced_20261014.json)
- [238 項最新共享回歸](../artifacts/operations/agent-workflow/runs/taifex-column-bounded-writer-rac-20261013T184932-2f6b6082/run.log)
- [原 reader resident 完整曲線](../artifacts/benchmarks/running_services_20261013/taifex_resident_baseline_20261014.json)
- [欄式 resident 完整曲線](../artifacts/benchmarks/running_services_20261013/taifex_resident_candidate_20261014.json)
- [最新版本預讀](../artifacts/operations/running-services-optimization-20261013/taifex-column-race-live-prewarm.json)
- [最新版本正式讀回](../artifacts/operations/running-services-optimization-20261013/taifex-race-cache-final-runtime-readback.json)

### 七種區間、候選演算法與展示執行緒

03:38 起，對同一 SHA-verified 真實凍結來源，完整建立 `1h/1d/1w/1mo/1q/1y/all`
七種曲線，保留所有財務數值、時鐘、極值、重複時戳及 null／舊格式處理。
分組端點再 join 的候選雖降峰值記憶體，CPU 反增約 20%；改用 struct 集合
過濾的候選也沒有整體收益。兩者只保留為離線實驗，沒有正式啟用。
正式 query 的視窗選點規則原封不動，抽為可測試的純函式。

展示原本讓 Polars 使用本機預設 16 條執行緒。已核對
[官方 thread pool 說明](https://docs.pola.rs/api/python/version/1/reference/api/polars.thread_pool_size.html)：
此設定須在程序啟動前決定，官方不建議不經測試隨意覆寫。因此沒有改動
全域 downloader／訓練設定，而是實測 1／2／4／8／16，再交錯驗收這個角色。

第二輪 ABBA 沒有並行啟動本次 pytest；其他原服務仍在工作。每版四個新
程序，各完整建立七種區間、包含完整 JSON 對照編碼；共 56 個完整輸出，
每種區間 SHA256 都與未改寫的獨立完整控制一致，1d 也匹配先前原始 NDJSON
控制。沒有改變來源、重算模型、呼叫 broker 或 provider。

| 完整七區間操作中位數 | 原 16 執行緒 | 4 執行緒 |
| --- | ---: | ---: |
| 牆鐘秒 | 7.865 | 7.988 |
| CPU 秒 | 15.984 | 12.054 |
| worker 峰值 RSS，十進位 MB | 1,229.13 | 1,101.63 |

CPU 減少 24.6%、峰值 RSS 減少 10.4%；牆鐘增加約 1.6%，不宣稱更快。
較早一輪有牆鐘改善 8.6%，不能把它當最後一輪的速度結論。
這是同一硬體的完整展示工作流程，不是所有服務、所有 API 或整機 RAM
的相同降幅；未宣稱無欄式 cache 的第一次全帳本建置也更快。
218 項最新 reader／runtime／public 共享測試通過；原 strict 策略時鐘
validator 保持不變，部署與完整資料就緒仍各自驗收。

- [兩次 join 候選完整對照，未採用](../artifacts/benchmarks/running_services_20261013/taifex_seven_range_grouped_balanced_20261014.json)
- [struct 過濾候選完整對照，未採用](../artifacts/benchmarks/running_services_20261013/taifex_seven_range_predicate_balanced_20261014.json)
- [完整執行緒篩選，不是公平 ABBA 結論](../artifacts/benchmarks/running_services_20261013/taifex_seven_range_thread_screen_20261014.json)
- [四執行緒最終 ABBA 與 56 個完整輸出](../artifacts/benchmarks/running_services_20261013/taifex_seven_range_four_threads_final_balanced_20261014.json)
- [218 項目前 reader／runtime 共享回歸](../artifacts/operations/agent-workflow/runs/taifex-role-thread-runtime-regre-20261013T194958-351100c1/run.log)

03:54 的隔離預讀峰值 RSS 147.33 MB；03:54:39 之後只重載期貨展示
owner，invocation 為 `6521c3fc9c2b44618d4ca5f86178e797`。
已讀回實際 `POLARS_MAX_THREADS=4` 與 `polars-0` 到 `polars-3` 的執行緒；
03:55 七種 API 區間、實際 column receipt 推進及原交易／capture 身分
驗收通過。設定保留顯式使用者覆寫，只影響這個唯讀展示 launcher。
`strict_data_ready=false` 的來源估值缺口仍保留，沒有改變正常夜盤擷取。
台股五模式在此之後也再次通過零 GPU 正式快取讀回。

- [四執行緒正式預讀](../artifacts/operations/running-services-optimization-20261013/taifex-four-thread-live-prewarm-20261014.json)
- [只重載展示及保持原交易／capture owner](../artifacts/operations/agent-workflow/runs/taifex-readonly-four-thread-adop-20261013T195439-a648b4f7/run.log)
- [七區間與目前展示 owner 正式回讀](../artifacts/operations/running-services-optimization-20261013/taifex-four-thread-runtime-readback-20261014.json)
- [台股五模式再次零 GPU 回讀](../artifacts/operations/running-services-optimization-20261013/postclose-after-taifex-role-adoption-20261014.json)
- [03:59 一分鐘全服務取樣](../artifacts/operations/running-services-optimization-20261013/census-after-four-thread-adoption-20261014.json)

全卡 GPU 取樣由先前 3–4%／約 3.4 GiB 變為這輪 9%／約 4.85 GiB，
NVML compute-app 清單在 WSL 中仍為空；不能據此歸因某個程序，也不能把
五模式零 CUDA 的工程驗收說成整張卡或 Windows 遊戲已完全閒置。
這輪主機取樣含明列的隔離 allocator benchmark，不是無量測工作的整機基線。

FinMind safe reload 的直接 CLI 曾缺 repository import bootstrap；已修復並
在 repository 外、清除 PYTHONPATH 的程序測試。9 項 reload／status index
測試通過。最新只讀觀察仍無安全批次空閒窗口，沒有重啟目前 worker，
不能把 index 的離線改善當成正式下載器目前吞吐已改善。

- [FinMind 直接 CLI 與 index 回歸](../artifacts/operations/agent-workflow/runs/finmind-safe-reload-cli-contract-20261013T194645-ffff4c5e/run.log)
- [最新 FinMind 延後證據，沒有重啟](../artifacts/operations/running-services-optimization-20261013/finmind-current-idle-observation-20261014.json)

## 04:10 展示服務閒置記憶體回收

本機安裝的 Polars 1.38.1 初始化值仍為 `muzzy_decay_ms:-1`；
未使用頁面可以被保留，不能直接把較高 RSS 判定為資料洩漏。
[jemalloc 官方說明](https://jemalloc.net/jemalloc.3.html#opt.muzzy_decay_ms)定義
回收的是未使用頁面，而非目前仍需要的資料；
[Polars 目前官方原始碼](https://github.com/pola-rs/polars/blob/main/py-polars/src/polars/__init__.py)
已採 1,000 ms 的 muzzy 回收設定。本次沒有升級套件、全域改 allocator、
丟棄 OS page cache 或強制清除交易／下載器記憶體。

兩輪各八個新程序，固定四執行緒，依 ABBA 比較完整七區間、
十秒實際閒置及再次完整 1d 讀取；所有完整輸出與原獨立控制相同。
第二輪沒有並行跑本任務的測試或其他 benchmark，原正式服務照常執行：

| 同版本完整操作或資源 | 舊回收設定 | 局部 1,000 ms | 解讀 |
| --- | ---: | ---: | --- |
| 七區間牆鐘中位數 | 8.185 秒 | 7.815 秒 | 此輪少 4.5%，不宣稱普遍加速 |
| 七區間 CPU 中位數 | 12.115 秒 | 12.135 秒 | 約相同，增加 0.17% |
| 程序峰值 RSS | 1,054.7 MiB | 918.3 MiB | 少約 13% |
| 十秒閒置 RSS | 1,022.6 MiB | 155.0 MiB | 少約 85% |
| 十秒閒置 CPU | 0.016 秒 | 0.082 秒 | 回收有小幅非零成本 |
| 再次完整 1d 讀取 | 0.181 秒 | 0.179 秒 | 第二輪大致相同 |

第一輪再次讀取曾由 0.207 增到 0.267 秒；不能只選第二輪說暖讀必然更快。
接受條件保留完整輸出、資料與目前程式／安裝版本 hash，並限制全流程
退化與後續暖讀額外成本。啟動器只設定展示角色的 `_RJEM_MALLOC_CONF`，
保留明確 operator override；下載器、broker／paper owner 與訓練不受影響。

04:08:44 只重載展示，正式 owner 為 `de3573388d954aaeb797e3b7e731febe`。
第一次讀回尚未等到新來源 generation，因此原檢查拒絕宣稱新 column refresh；
等來源自然更新後 04:10 正式讀回七區間、實載四執行緒／allocator、
實際新 column receipt，原交易／capture invocation 均一致。
`long_strip_1c2p` 原來源估值缺口仍保留，`strict_data_ready=false` 沒有放寬。
117 項 cache／曲線／readback／零 GPU 相關回歸通過，與前述測試有重疊。

04:12–04:13 取樣中展示 process RSS 179,318,784 B，約 171 MiB；
cgroup MemoryCurrent 約 202 MiB，其中 anonymous 約 102 MiB、file 約 96 MiB。
這些指標不同，不把可回收 file cache 當成同量無法回收的程序 RAM。
主機 resource delta 清單有 61 個單元，其中 35 個為 active/running；
另有 activating 的單次更新／備份，不能把全部登錄服務都叫常駐程序。
全卡 GPU 此輪 6%→3%、4,990→3,463 MiB，屬不同時點的觀察，
不是 allocator 導致的 GPU 改善或 Windows 遊戲效能證明。

- [第一輪完整 allocator、閒置與暖讀對照](../artifacts/benchmarks/running_services_20261013/taifex_allocator_idle_balanced_20261014.json)
- [第二輪 ABBA 確認](../artifacts/benchmarks/running_services_20261013/taifex_allocator_idle_confirmation_20261014.json)
- [局部設定的隔離正式預讀](../artifacts/operations/running-services-optimization-20261013/taifex-allocator-live-prewarm-20261014.json)
- [新來源 generation 後七區間正式讀回](../artifacts/operations/running-services-optimization-20261013/taifex-allocator-runtime-after-generation-20261014.json)
- [部署後全服務一分鐘資源取樣](../artifacts/operations/running-services-optimization-20261013/census-after-allocator-adoption-20261014.json)
- [台股五模式再次匹配目前控制、零 CUDA](../artifacts/operations/running-services-optimization-20261013/postclose-after-allocator-current-controls-20261014.json)

## 全資料網站 feature index 長駐記憶體實驗

真實、SHA 固定的 55,797,215 B 清冊包含 88,865 欄；
每個新程序完整解碼、驗證、建索引、遍歷全部分頁、搜尋與換代重置，
1,130 個結果合計 83,794,398 B，八程序全輸出 SHA 一致。
不含網路、HTTP/gzip、producer 改寫或整個 gateway 的吞吐。

第一版共享每世代內相同的 immutable 標籤，保留原 JSON 數值解析器、
未知欄位、Unicode、負零、整數、NULL、所有行、搜尋和 revision 語意；
pool 每類最多 4,096 值，超過只不再共享，不截斷資料，也不全域 intern。
索引保留 RSS 約 183.1→128.4 MB，少約 30%，峰值少約 17%。
初步總耗時相近，但這版的乾淨第二輪總 CPU 約 1.605→1.788 秒，
增加約 11%；主要是新 generation 的建索引時間多約 0.18 秒。
沒有拿初輪時間當作新版本加速宣告，這個較慢版本沒有重載正式網站。

254 項分頁、側車、gateway、安全／唯讀與 dashboard 契約測試通過；
後續微調需要另留目前程式證據，不把舊測試或 prototype 當正式部署。

- [原 prototype 全分頁 ABBA](../artifacts/benchmarks/running_services_20261013/public_feature_residency_balanced_20261014.json)
- [整合版本、獨立 control 與完整輸出](../artifacts/benchmarks/running_services_20261013/public_feature_residency_production_balanced_20261014.json)

### 已正式載入的原生解碼版本

最後採用現有 msgspec 0.22.0 的 untyped metadata 解碼與有界標籤共享，
沒有使用會把整數轉浮點的型別轉換，也沒有換掉公開 JSON 編碼器。
缺少套件或不支援的合法舊格式走原 stdlib 備援；Infinity／NaN 仍拒用。
4,096 個浮點數含極小值、負零、極大值做 IEEE64 逐 bit 比對；
整數超過 2^53／2^64、Unicode、未知欄位與 pool 滿額後不丟列亦通過。
套件與原生 extension 本身的 SHA256 也納入當前 benchmark fence。
[官方 JSON 型別說明](https://msgspec.dev/supported-types)僅作介面依據，
實際相容性與效能仍以本次完整資料、測試及讀回為準。

| 完整清冊工作流程／資源中位數 | 原解析器 | 原生解碼與共享 |
| --- | ---: | ---: |
| 完整牆鐘 | 1.752 秒 | 1.750 秒 |
| 完整 CPU | 1.752 秒 | 1.733 秒 |
| worker 峰值 RSS，十進位 MB | 315.9 | 218.7 |
| 索引保留 RSS，十進位 MB | 184.4 | 160.3 |
| 建索引牆鐘 | 0.961 秒 | 0.789 秒 |
| 全部分頁／搜尋牆鐘 | 0.732 秒 | 0.884 秒 |

峰值 RAM 少約 31%、索引保留 RAM 少約 13%；全流程 CPU／時間相近。
建索引較快，但本輪分頁階段較慢，不宣稱每個子階段都變快；
完整 1,130 個結果仍與第一版獨立原始 control 的完整 SHA 一致。
上述為完整 feature-index 工作流程，不是整個 gateway、所有 HTTP 或全機降幅。
257 項目前分頁、側車、gateway、唯讀／安全與 dashboard 契約測試通過；
另 36 項目前 portable readback／完整來源與程式 fence／query 規則測試通過，
兩組有重疊，不相加當不同測試總量。

04:39:33 只重載 `stockagent-public-dashboards.service`，目前正式 owner
為 `5e76016bdd3f4ae49d3032bfb2cd74c6`／PID 1237926。
獨立控制使用當前 raw JSON 的原 stdlib 解析，不使用新 decoder 當 oracle。
原驗證腳本一度錯用內部 `search` 參數、且後續頁未帶 revision，回應 400；
保留網站原 `q`／revision 安全規則，增加 actual parser 的測試後再驗證。
04:47 當前 88,865 列的四種搜尋、分頁與換代重置 HTTP 內容逐 byte 一致，
交易、capture、FinMind、Discord、期貨展示五個 protected owner 均不變。
04:40 台股五模式正式鍵／獨立完整輸出也再次通過，零 model forward、零 CUDA。

04:41–04:42 的 gateway process RSS 943,792,128 B（約 900 MiB），
此時尚未完成 valid feature search 的新索引預熱；不能把整體高 RSS 歸因
feature index，也不能把角色重載後不同時點的數字當公平 A/B 因果證明。
全卡 GPU 仍約 4%／3.4 GiB，不是模型快取又在做 GPU 推論的證據。
下一步清點入口預熱、官方價格查詢及 native allocator 的長駐／閒置分布；
不先假定是 model、JSON、Arrow 或資料洩漏，也不對其他服務做全域回收。

- [最終原生解碼、完整 ABBA、原始 control 與安裝 extension hash](../artifacts/benchmarks/running_services_20261013/public_feature_native_residency_final_balanced_20261014.json)
- [當前原解析器 live oracle 預讀](../artifacts/operations/running-services-optimization-20261013/public-feature-live-oracle-prewarm-20261014.json)
- [實際新 owner 四種 HTTP 結果逐 byte 驗收](../artifacts/operations/running-services-optimization-20261013/public-feature-current-valid-http-readback-20261014.json)
- [入口重載後台股五模式零 GPU、目前完整控制](../artifacts/operations/running-services-optimization-20261013/postclose-after-native-gateway-adoption-20261014.json)
- [04:54 目前五模式完整控制與零 CUDA 最後讀回](../artifacts/operations/running-services-optimization-20261013/postclose-final-current-readback-20261014.json)
- [入口重載後全服務一分鐘取樣](../artifacts/operations/running-services-optimization-20261013/census-after-native-gateway-adoption-20261014.json)
- [36 項目前 readback 與 query 契約](../artifacts/operations/agent-workflow/runs/public-feature-real-wire-query-g-20261013T204705-d77a0e84/run.log)

## 日期目錄預熱：保留完整曲線，移除未使用的歷史常駐圖

正式 gateway 啟動預熱原本呼叫 `_benchmark_history_index`，載入
131,352 筆 benchmark marks。實際 chart reader 已採日期分區投影及
外層 bytes cache，啟動預熱需要的只是 156 個日期，不需要整份 Python
物件圖長期常駐。來源 `benchmark_history.json` 為 331,514,015 bytes；
已接受的日期 head 只有 36,112 bytes。

先以正式 owner 的數值執行緒／索引目錄環境重播完整 gateway 預熱，
不啟動第二個 collector 或常駐服務。舊版觀察 RSS 約 893 MB，
最後的「完整 benchmark index 預熱」額外留下約 366 MB。
另一輪一次性 Python allocation 診斷中，477 MB live Python objects
約有 386 MB 來自 stdlib JSON decoder。tracemalloc 本身有顯著開銷，
這些診斷牆鐘及 RSS 不當作正式服務效能。

OfficialDailyQuoteSource 已經採日期 predicate 與必要欄位投影；本次
預熱只留下三日 7,071 列。Arrow pool live bytes 為零、peak 約 19 MB，
不支持「Arrow 活資料吃掉近 1 GB」的歸因。一次性 allocator release
只釋出約 39 MB，因此未部署每次 request 呼叫 `release_unused()` 的
政策；[Arrow 官方文件](https://arrow.apache.org/docs/python/generated/pyarrow.MemoryPool.html)
也將其定位為可能昂貴、影響效能的 debug／benchmark 操作。

修改 `warm_dashboard_session_indexes`：有效 head 只讀日期目錄，
不把空選擇寫入完整列 cache。真正 chart read 的 shard SHA 驗證不變；
缺少／過期／壞 head 仍走原完整 reader。position locators、帳本 byte
索引、line counts、歷史持倉與任何圖表列都不刪除。

### 完整受測流程與正式讀回

八個獨立程序交錯 ABBA × 2，正式 gateway 使用的
`use_memory_cache=False` 設定，完整包含所有索引預熱、全歷史 8 條
曲線、三個指定交易日 8 條曲線、編碼、GC／閒置觀察及再次完整 warm
讀取。每個 worker 使用私人 derived index cache；沒有修改來源、
正式 cache、API 配額或 GPU。所有曲線、索引計數及再次讀取均 exact。

| 整個受測工作流程中位數 | 原完整列預熱 | 日期目錄預熱 | 降幅 |
| --- | ---: | ---: | ---: |
| 牆鐘 | 3.3043 秒 | 2.3609 秒 | 28.5% |
| CPU | 3.3541 秒 | 2.4102 秒 | 28.1% |
| 閒置 RSS，十進位 MB | 417.3 | 265.4 | 36.4% |
| 峰值 RSS，十進位 MB | 449.1 | 297.0 | 33.9% |
| 再次完整 warm 讀取牆鐘 | 0.6756 秒 | 0.6502 秒 | 未退化 |

全歷史 341,952 個點、8 條曲線；最早／中間／最新指定日各 2,192 點，
每條曲線完整 bytes 與全部索引計數一致，不是抽樣前 N 列。
這是上述完整有界流程，不包括 gateway status／其他 upstreams／
任意 range 組合，也不代表全主機的同倍率改善。

較早的兩次量測因整份 `state.json` 心跳 mtime fence 拒絕，舊失敗
收據保留。其中一次已定位唯一變動為 `state.json`，不是歷史檔或程式。
canonical history 計算只消費 `product`、`enabled_markets`，最新版
分開記錄這兩欄的計算輸入 fence 和整檔 metadata，仍 fence 全部歷史
來源、SHA256、四個程式檔；改變產品／市場／任何歷史來源的測試仍拒絕。
採用的八次量測是重新執行，不將被拒絕的舊量測改寫成成功。

05:34 的重載只涉及 `stockagent-public-dashboards.service`。
正式讀回證明全歷史 15,814,439 bytes 與最新單日 105,915 bytes
逐 byte 相同；protected trading／capture／FinMind／Discord／TAIFEX
display owners 在本次重載前後一致。05:30 原生 Shioaji SDK 維護
排程另行重載了自己的行情及交易 owners，不是本次操作，也不宣稱
這些 owners 整晚都未改變。

新版相同環境完整 gateway 預熱診斷觀察 RSS 753 MB、6.75 秒；正式
服務 05:36–05:37 取樣 RSS 806,248,448 bytes（約 769 MiB）。這兩個
數字屬不同流程／時點，不將其與有界 ABBA 的 265 MB 混在一起。
同時全卡 GPU 利用率 3–4%、占用 3,388 MiB；不是本次模型 cache
使用 GPU 的證據，也未識別所有 Windows GPU clients。

本版 207 項共享回歸與 22 項專用 fence／query／prewarm 測試通過，
兩組有重疊，未聲稱全 repo suite 完成。HTTP verifier 首次誤用
內部 `history_encoding` keyword，依真正 public parser 改用
`encoding=v2` 並加入正式 query 契約測試，未放寬 API。重載尚在
`activating` 時的一次 probe 亦正確拒絕，待服務完成預熱後才驗收。

- [正式設定八次完整 ABBA、全部 SHA／索引計數與資源中位數](../artifacts/benchmarks/running_services_20261013/tw_catalog_prewarm_gateway_settings_20261014.json)
- [正式重載前完整 HTTP 控制](../artifacts/operations/running-services-optimization-20261013/tw-catalog-valid-previous-reader-20261014.json)
- [正式新版完整 HTTP bytes 與 protected owners 讀回](../artifacts/operations/running-services-optimization-20261013/tw-catalog-ready-actual-http-readback-20261014.json)
- [舊版正式環境全 gateway 記憶體歸因](../artifacts/operations/running-services-optimization-20261013/public-gateway-owner-matched-residency-20261014.json)
- [新版完整 gateway 預熱診斷](../artifacts/operations/running-services-optimization-20261013/public-gateway-current-catalog-residency-20261014.json)
- [05:36–05:37 全服務與 GPU 取樣](../artifacts/operations/running-services-optimization-20261013/census-after-catalog-adoption-20261014.json)
- [207 項共享語意回歸](../artifacts/operations/agent-workflow/runs/tw-catalog-current-schema-regres-20261013T211704-8206c3da/run.log)
- [22 項目前 public query 與完整輸入 fence 測試](../artifacts/operations/agent-workflow/runs/tw-catalog-wire-contract-regress-20261013T213410-d56dae7c/run.log)

### 隔日資格發布後：更新摘要，不重算相同模型輸入

原資格 watcher 的不可變收據確認：05:22 將下一交易日（10 月 14 日）
資格表加入 live root，TWSE 1,226 檔／TPEx 838 檔。這是實際來源世代
改變，不是 SDK 維護造成，也不是交易引擎的 `state.json` 心跳。
因此 04:54 摘要在 05:35 後的驗證正確回報
`completed_source_generation_changed`；未回填舊 summary 的 fingerprint。

最新資料完整重播五模式，每模式三次完整 signal 操作，在模型、CUDA
及 quote provider 禁用條件下均命中原始 forward key，完整輸出與原
獨立控制一致。canonical worker 的 `signal_cache_only=True` 分支
沿用原有鎖、來源接受、訊號出版及 owned-cache 回收，05:39 只更新
五個使用者摘要／weights pointer，沒有交易帳本操作或正式歷史重跑。
05:41 新摘要再次接受來源世代，驗證五模式正式 keys 與完整控制一致。

這證明「來源更新需刷新摘要」不等於「相同輸入要重做 GPU forward」。
未改純 forward identity，也未將日曆或資格表的來源 fence 移除。
本次受控更新不當成未來每次事件已自動正確觸發的證明；事件連動的
部署與獨立原生工程通知驗收見下一節。

- [05:22 下一交易日資格來源不可變收據](../artifacts/data_refresh/tw_day_trade_eligibility/runs/20261014T043206043762.json)
- [五模式最新來源的完整輸出與零 GPU 重播](../artifacts/operations/agent-workflow/runs/postclose-new-source-five-cache--20261013T213742-6303c237/run.log)
- [既有 canonical cache-only 維護器，禁止 GPU／模型／行情 API](../artifacts/operations/agent-workflow/runs/canonical-postclose-source-refre-20261013T213941-d73be7a1/run.log)
- [05:41 五模式新來源正式讀回、原 forward key 與獨立完整控制](../artifacts/operations/running-services-optimization-20261013/postclose-after-source-generation-formal-readback-20261014.json)

### 原生事件連動：只在 committed source 變動時喚醒

舊 `stockagent-discord-artifact-maintenance.path` 只監聽收盤接受收據，
因此隔日資格表更新未自動喚醒 cache-only worker。現在同一個 path
加上 canonical live root 的 TWSE／TPEx 資格 parquet，仍指向既有
`stockagent-discord-postclose-cache.service --signal-cache-only`。
不監聽會定期改寫的等待／heartbeat `latest.json`，不新增輪詢 daemon，
不啟動完整歷史維護器。這兩個絕對路徑與 publisher 預設及目前五模式的
實際 resolver root 一致；自訂 rule root 時需重新配置並驗證 watch set。

[systemd 官方 path 說明](https://raw.githubusercontent.com/systemd/systemd/v255/man/systemd.path.xml)
描述原生 inotify 監聽與 `PathChanged` 的 close-write／多來源行為。
實際主機是 systemd 259；功能接受仍以本次原生 owner、atomic-replace
工程通知、終止及後續閒置讀回為準，不只靠 man page 或 unit 字串。
資料接受、來源 shared lock、opening guard、完整 forward identity 及
單一 cache-worker lease 全部沿用既有實作。

05:58 使用一次明確標示的 `/run` 工程通知，同一 native path 真實
喚醒同一 cache-only owner。沒有改任何市場原始資料／mtime／發布時鐘，
也不把工程通知當成真正的新資格發布。原生 worker 成功處理五模式，
6.154 秒後退出、CPU 6.096 秒、峰值約 512 MiB；這是完整新 process
啟動與所有 reuse gates 的成本，不是模型推論速度。native log 沒有
產生新 signal 的 `postclose_cache_ready`；前後五個完整 summary bytes、
weights bytes、來源 identity 與原 forward key 都不變，禁止模型／CUDA
的 canonical verifier 亦通過。

測試用 path drop-in 已先撤除並只重載 path owner，再移除通知檔，
避免在仍受監聽時刪除 marker 又觸發額外工作。測試通知保留工程副本。
06:00 正式讀回只剩三個必要來源、無 runtime drop-in；維護器仍是
05:58 同一個 terminal handle，沒有反覆啟動。六個 protected reader／
trader／capture／download owners 前後不變。真正下一次資格發布的
端到端時點尚未發生，仍保留 `real_new_publication_tested=false`。

147 項共享 cache／Discord／event-wire 測試通過。初版驗證工具用 dict
解析 `systemctl show`，把多次 `Paths=` 只保留最後一個，導致兩次工程
讀回錯判；已修為保留全部值、加上真實 CLI 格式回歸，舊失敗保留。
錯判發生在發送通知前，沒有因此改寫來源或重算模型。

- [原生目前完整 watch set 與五模式基線](../artifacts/operations/running-services-optimization-20261013/postclose-event-current-multivalue-control-20261014.json)
- [原生通知交付、terminal handle 與五模式完整 bytes／key 不變](../artifacts/operations/running-services-optimization-20261013/postclose-event-current-native-delivery-20261014.json)
- [撤除探針後三來源與閒置 terminal handle 讀回](../artifacts/operations/running-services-optimization-20261013/postclose-event-normal-idle-readback-20261014.json)
- [保留的非市場工程通知](../artifacts/operations/running-services-optimization-20261013/postclose-event-marker-proof-20261014.json)
- [147 項相關語意／多值 wire 回歸](../artifacts/operations/agent-workflow/runs/postclose-event-multivalue-wire--20261013T215711-359a7ec0/run.log)

全服務目標仍 active。FinMind 舊 owner 的 covering index 尚待安全空閒
窗口；新取樣最高 CPU 為原生 Shioaji 歷史回補，須用其完整 request／
驗證／persist 流程另行量測，不能中斷歷史下載或拿 index microbenchmark
宣稱全部改善。兩個正式歷史 GPU 工作失敗亦仍未解決。

### 永豐歷史回補：單次完整收據遍歷

06:07 的 `--nonblocking --rate 10 --duration 20` 取樣只有 18 個有效
stack samples，並有 5 個錯誤；不把比例解讀成 CPU 秒或完整查詢吞吐。
同時實際子程序 RSS 約 1.37 GiB、其中匿名頁約 1.22 GiB；這是當時
回補批次，不是所有服務的 RAM。原生日誌的每批 500 queries 包含首筆、
第 250 筆與最後三次 summary audit，近期每次約 47–52 秒。

舊 summary 再讀 KBar 收據建立 tick 日期，並呼叫完整 `build_tasks`
重建待辦；新 summary 在同一次既有 SHA／device／inode／size／mtime／ctime
驗證中計算覆蓋、官方活動缺口、tick 目標及待辦數。派工與摘要共用
retry predicate；不是用已完成數扣除待辦，也沒有跳過未知空回應或
合約不可查的覆蓋缺口。KBar/tick 轉換、順序、單位、日曆、窗口、
三次摘要頻率、既有修復冷卻、provider 請求和 publication gate 均未改動。

已直接核對[目前官方限制](https://sinotrade.github.io/tutor/limit/)：
行情查詢共用 50 次／10 秒，與本機現行 limiter 相符；沒有依 bundled
技能中較舊的 50 次／5 秒說明提高配額。

本階段量測包含固定清冊的 inventory、官方 activity、完整待辦、三次
全清冊 summary 與真實 atomic persist；不含 SDK 初始化／登入、網路
查詢與 publication，因此不能據此宣稱整個下載批次或全機同樣提速。
真實來源逐 receipt/data pair 做 signature fence 後獨立複製，不使用
hard link、更改原始資料／mtime、改名或清理歷史檔案。主機仍有其他
服務，OS page cache 不清空；比較使用獨立程序的 ABBA 次序。

第一次 literal filesystem snapshot 超過 300,000 檔安全上限而停止；
保留的私人部分快照為 300,000 檔、4,379,627,045 bytes，失敗收據未刪除。
改為目前清冊真正遍歷的 KBar chunk 與已驗證 KBar／官方 activity tick
聯集；過期的舊 query range 仍留在正式 archive，不列為這次工作量。
固定的來源檔案、完整 task/summary hash、實際節點/cgroup/runtime 與
寫檔讀回共同作為接受條件。06:51 的四程序對照已完成：

| 完整本機 audit 中位數 | 原先多次遍歷 | 單次遍歷 |
| --- | ---: | ---: |
| 牆鐘時間 | 387.85 秒 | 198.85 秒 |
| CPU 時間 | 317.38 秒 | 200.35 秒 |
| 程序峰值 RSS | 1,331,910,656 bytes | 1,651,480,576 bytes |

四次皆有 7,579 個合約、135 個真實待辦；完整排序 task SHA、三份
全部摘要 SHA 與 atomic 寫檔回讀逐欄一致。對照順序為舊／新／新／舊，
首輪舊版 physical read 約 7.16 GB，而最後舊版約 15.27 MB；不能忽略
磁碟冷熱差。最後暖讀舊版與新版中位比較約為牆鐘少 26.6%、CPU 少
26.2%，也不是受控 page-cache 或獨占主機的倍率。RSS 中位增加約
24% 是尚待修正的結果，不因牆鐘改善而宣稱記憶體改善。

有效清冊含 144,941 份收據及 117,470 份資料檔；原收據 LRU 為
131,072、SHA LRU 為 16,384，均小於其工作集。循環全庫遍歷仍大量
淘汰；新版四次 audit 的 logical read 約 11.33 GB，原版約 14.19 GB。
06:57 啟動隔離的容量 ABBA：原配置對照收據 4,096／SHA 131,072，
只在獨立 worker 重包原函式 decorator，使用相同清冊、官方活動、
完整待辦／摘要／persist，並以已接受的原版所有輸出 hash 為 oracle。
四次全輸出及來源 fence 均通過；中位牆鐘由 158.36 秒增加到 167.33 秒
（+5.7%），CPU 由 160.29 秒增加到 168.89 秒（+5.4%），峰值 RSS 由
1,627,254,784 降到 1,020,571,648 bytes（−37.3%）。小收據快取每次
都 miss，SHA 重用的收益不足以抵銷完整收據重讀成本；未採用這個容量
政策，原收據／SHA 上限保持不變。另以同清冊完整 cProfile 定位下一個
CPU 熱點，profile 已完成且全待辦／三摘要輸出一致：445,479 次
`Path.resolve()`、3,096,485 次 `stat`、4,900,269 次 `lstat`。解析路徑
累計約 72.74 秒；累計時間彼此包含，不能相加。profile 本身約 286.76 秒，
包含量測成本，不能直接當公平加速倍率。不把 hash cache 當
免驗證，device/inode/size/mtime/ctime 與 atomic replacement fence 全保留。

下一個候選保留原 `c343eed…` repair source 的獨立副本與已接受 oracle，
不從新版生成舊版控制。SHA cache lookup 改用不解析每層路徑的絕對名稱，
cache miss 必須開啟一個自己擁有的 regular-file descriptor，`fstat` 的
五欄身分在讀取前後一致，返回前再核對路徑身分。非 blocking 開啟防止
檔案被替換成 FIFO 時卡住；雜湊前設回 blocking，結束一定關閉 descriptor。
這使用 [Python 3.12 `hashlib.file_digest`](https://docs.python.org/3.12/library/hashlib.html#file-hashing)，
不是換雜湊算法或略過 SHA。來源競態／I/O 錯誤在外層收據 validator
fail closed 且不 memoize 暫時失敗，不中止整個 audit。

80 項共享回歸涵蓋完整計畫、逐欄摘要、SHA memo、同大小恢復 mtime 的改寫、
symlink ABA、cache-hit 期間切換、原子替換、刪除、FIFO、防 descriptor 洩漏，
另有 profile／獨立 baseline 身分與失敗收據測試。這不是全 repository 測試。
六個獨立程序以原版／compact FD／default FD／default FD／compact FD／原版
比較原容量與兩種候選容量；在對照完成前沒有採用容量變更、沒有重啟 owner。

07:50 六程序完整對照接受，CPU／牆鐘／RSS 的中位數如下。所有 task SHA、
三份完整 summary SHA、persist 回讀與來源／程式 fence 相同，provider calls
為零。OS page cache 未清空，其他原服務繼續工作，沒有全機獨占保證。

| 完整相同本機工作 | 舊 SHA／原容量 | FD／原容量 | FD／選出容量 |
| --- | ---: | ---: | ---: |
| 牆鐘秒 | 156.735 | 144.705 | 125.132 |
| CPU 秒 | 157.770 | 146.443 | 127.218 |
| 峰值 RSS bytes | 1,651,818,496 | 1,642,520,576 | 1,071,228,928 |

選出容量在三個資源面向都優於兩種控制：相對原版牆鐘少 20.2%、CPU 少
19.4%、RSS 少 35.1%。後者不是 06:57 的原 SHA 小快取負例；只有配合
移除重複路徑解析的 FD 身分驗證才觀察到此改善。不把兩輪資料混在一起。

07:51 先獨立保存受測兩模組原始 bytes／SHA／AST，再只改兩個 literal LRU
容量：收據 4,096、SHA 131,072。07:58 無替換 decorator 的 canonical 完整
計畫／三摘要／persist 回讀再次一致；全模組 AST 只允許那兩個容量不同。
這次單一回讀不是重新比較速度；上述倍率仍來自六程序完整平衡對照。
新預設通過 95 項共享測試，與此前 80 項重疊，不相加。

目前只證明程式預設與完整本機流程；沒有為此重啟下載 owner。原排程的
盤中保護仍保留，下一個自然 batch／正式 API owner 載入另驗，
`live_owner_adoption_verified=false`。沒有把 zero pending、來源空值或
不可查歷史改成 complete。

正式歷史 owner `e17122c4dd2c4242a3e00c71640519fd` 已自行進入
`waiting_source`；06:09 收據仍為 source gaps，pending=0 不是全資料完整。
07:09 因既有 TX history 持有 shared login slot，原生排程正常等 60 秒，
07:11 起自行取得槽位並載入 `summary_strategy=single_pass_receipt_v1`。
07:15 以首筆／最後 `coverage_passes=1`、canonical 摘要與原排程收尾讀回
接受正式載入；history owner 與其受保護 owner 在這次讀回都不變。
實際本批為 40 次原生來源查詢，摘要首筆 38.122 秒、final 75.175 秒；
同時有其他 CPU 工作，沒有匹配原版的 API／全批控制，不能把離線倍率
套到本批或稱原生 final 比原版快。工程 verifier／benchmark 的 provider
calls 為零，原生下載本身的 40 次查詢不混為零。仍有 66 次正活動空回應、
21 個官方活動缺日期與 1,540 個不可查合約，`all_source_data_complete=false`。

- [非阻塞原始 stack samples](../artifacts/operations/running-services-optimization-20261013/shioaji-history-nonblocking-20261014.txt)
- [第一個快照安全停止紀錄](../artifacts/operations/agent-workflow/runs/shioaji-full-local-audit-fixture-20261013T221256-f520920b/run.log)
- [有效清冊快照的原生執行紀錄](../artifacts/operations/agent-workflow/runs/shioaji-canonical-target-audit-f-20261013T222128-91619bdc/run.log)
- [四程序完整清冊 ABBA、全部輸出 hash、runtime 與 I/O](../artifacts/benchmarks/running_services_20261013/shioaji_canonical_audit_abba_20261014.json)
- [原生 ABBA handle 的完整紀錄](../artifacts/operations/agent-workflow/runs/shioaji-canonical-full-audit-abb-20261013T222851-d5a97b41/run.log)
- [256 項最後共享契約回歸](../artifacts/operations/agent-workflow/runs/shioaji-single-pass-final-contra-20261013T222637-ab5bdd68/run.log)
- [36 項含 fixture 競態／有效目標範圍的回歸](../artifacts/operations/agent-workflow/runs/shioaji-audit-target-safety-test-20261013T222238-ba6c4918/run.log)
- [隔離快取容量 ABBA 執行紀錄](../artifacts/operations/agent-workflow/runs/shioaji-complete-cache-capacity--20261013T225708-7cb9fab8/run.log)
- [快取容量全輸出、CPU／RSS 負例](../artifacts/benchmarks/running_services_20261013/shioaji_cache_capacity_abba_20261014.json)
- [完整 profile、全輸出相等與原始 syscall 計數](../artifacts/benchmarks/running_services_20261013/shioaji_compact_full_audit_profile_20261014.json)
- [原始 SHA 實作的獨立副本與 hash／oracle 綁定](../artifacts/benchmarks/running_services_20261013/shioaji_verified_hash_reference_v2_20261014/reference.json)
- [80 項 FD 身分、完整 audit 與 profile 回歸](../artifacts/operations/agent-workflow/runs/shioaji-owned-fd-final-full-sema-20261013T233208-0b390e36/run.log)
- [六程序原始 SHA／兩候選、完整 oracle、節點與 I/O](../artifacts/benchmarks/running_services_20261013/shioaji_verified_hash_full_balanced_20261014.json)
- [容量變更前保存的兩份受測原碼與 AST 綁定](../artifacts/benchmarks/running_services_20261013/shioaji_measured_capacity_reference_20261014/reference.json)
- [未打 memo 補丁的完整正式預設驗收](../artifacts/benchmarks/running_services_20261013/shioaji_unpatched_default_full_audit_20261014.json)
- [95 項正式預設、FD 與完整共享契約測試](../artifacts/operations/agent-workflow/runs/shioaji-default-capacity-full-re-20261013T235246-b4c83a9f/run.log)
- [正式 owner 自然載入的基線](../artifacts/operations/running-services-optimization-20261013/shioaji-audit-natural-control-20261014.json)
- [原 owner 自然完成、實際 marker／摘要／排程驗收](../artifacts/operations/running-services-optimization-20261013/shioaji-audit-natural-terminal-20261014.json)

### Discord 盤前 GPU 常駐窗口

07:03 原程序完成五模式暖機曾測得 15.338 秒，但原窗口硬編碼從準備
時間 08:15 提前 75 分鐘，釋放保護又獨立寫死 07:00。暖機完成不等於
盤後 forward cache 被使用，也不等於所有閒置服務已無 GPU 配置。
本次把啟動暖機與 GPU 常駐保護共用相同窗口：這台主機預設準備前
600 秒（08:05），保留 60／120／240 秒重試與完整五模式暖機餘裕。
`STOCKAGENT_STARTUP_WARM_LEAD_SECONDS` 可依其他主機實測設定，限制
60–4,500 秒；非有限／格式錯誤沿用預設。未知 schedule 仍保守保護，
正常開盤重試、live 其他市場、model mutex 與 postclose source gate 不改。

成功釋放 model cache 才清除 process-hot flag，不重設 permanent failure；
否則隔日同一程序會因舊的 warm-ready flag 跳過重新準備。health 另標示
`model_cache_resident_ready`，把歷史暖機完成與當前常駐狀態分開。
嚴格 HH:MM 負例發現舊 helper 接受 25:00，新 GPU 窗口自己拒絕越界
時鐘，沒有趁機改其他市場 schedule parser。

07:18–07:19 僅重載 Discord；原交易、行情捕捉、兩個相關下載／展示與
公開入口 owners 不變。新 Python PID 1,674,213（原 1,045,265）重新連線、
global commands sync 成功／或明確 defer 必須可讀，五模式 engine revision
與 signal IDs 零落後；目前所有收盤 summary SHA、forward key 及獨立完整
權重／決策／解釋 oracle 不變，GPU device handles 從 2 變 0，沒有提前
暖機 ready 日誌。這是正式閒置窗口採用，不代表 08:05 下一次暖機或
09:00 opening SLO 已發生及驗收，也沒有修復原兩個失敗的 formal history。

08:05 原生第一次暖機遇到來源發布寫鎖：五模式均在 `_build_panel` 的
`live_tw_public_source_read_guard` 非阻塞讀鎖返回 `BlockingIOError`。
既有 60 秒重試在 08:06:21.418 開始下一輪，08:08:10.515 完成五模式，
合計 109.097 秒；成功早於 08:15 準備時間，沒有強制暖機、重載或移除
一致性保護。08:10 唯讀接受確認同一 PID／InvocationID、五模式當前
resident-ready、GPU device handles、原生日誌及 engine acknowledgement
零落後；verifier 不呼叫 CUDA／provider、不寫交易。08:06 的早期探針
因尚未完成而正確拒絕，失敗保留並由實際成功讀回 supersede。

這次暖機不能沿用先前 15.338 秒當固定估計，也不能稱 109 秒全部花在
GPU。接受收據內的原生階段計時如下；`pre_quote_prepare_ms` 包含
checkpoint、面板讀建與對齊等取價前工作，尚不是獨立 panel profiler。

| 模式 | 完整暖機秒 | 取價前準備秒 | 模型階段秒 | 面板 tier |
| --- | ---: | ---: | ---: | --- |
| V8 Annual Log Cash | 40.837 | 37.494 | 3.062 | rebuilt |
| Multi Basis 22 | 35.193 | 33.897 | 1.126 | rebuilt |
| Multi Basis | 0.811 | 0.165 | 0.406 | memory |
| 100M | 0.481 | 0.150 | 0.125 | memory |
| Factorized Scale Separated Cash | 31.761 | 28.883 | 2.768 | rebuilt |

三個 rebuilt 模式的取價前準備明顯較重；其他兩模式命中 memory。
後續先依相同語意的 panel／來源鍵分析可共用範圍，不能直接混用不同
feature、checkpoint universe 或來源世代。08:15–09:05 不重載交易相關
owner、不重跑重 GPU benchmark；真正 opening SLO 仍未驗收。

原控制的 NVML 全卡顯存 15,130 MiB，新原生讀回另記錄當時數字；
07:19 人工讀回約 3,174 MiB。全卡仍有 Windows 桌面／Sunshine／其他
程式，不能把差值逐 byte 歸因給 Discord，也不強制停止非專案應用。
新的 WDDM 歸因探針只做兩次有界唯讀取樣，每次約 9.25 秒，沒有安裝
polling daemon；Windows PID 不映射成 Linux PID。Microsoft 亦記載
[GPU Process Memory counter 可能不準確](https://learn.microsoft.com/en-us/troubleshoot/windows-client/performance/gpu-process-memory-counters-report-wrong-value)，
同一共享配置不可把每個程序的記憶體加總當作整卡占用；不以 counter
較大就宣稱 source corruption／memory leak 或終止程序。

- [真正重載前的五模式、原 owner、GPU handle／NVML 控制](../artifacts/operations/running-services-optimization-20261013/discord-prewarm-reload-admission-20261014.json)
- [新 owner／零 handle／full cache／commands／zero-lag 原生接受](../artifacts/operations/running-services-optimization-20261013/discord-prewarm-native-idle-acceptance-20261014.json)
- [僅 Discord 的原生重載動作](../artifacts/operations/agent-workflow/runs/discord-only-idle-prewarm-clock--20261013T231836-c23c9d5f/run.log)
- [189 項暖機窗口、native verifier 及原共享回歸](../artifacts/operations/agent-workflow/runs/discord-idle-native-all-existing-20261013T231735-84439167/run.log)
- [五模式自然暖機、當前常駐、owner／engine 原生接受](../artifacts/operations/running-services-optimization-20261013/discord-natural-prewarm-terminal-20261014.json)
- [57 項自然暖機、日期／clock／resident 與原生讀回回歸](../artifacts/operations/agent-workflow/runs/discord-natural-warm-clock-final-20261014T000507-3cec0fef/run.log)
- [Windows WDDM／board 的分開取樣](../artifacts/operations/running-services-optimization-20261013/windows-gpu-process-observation-20261014.json)
- [FinMind 07:02 仍無 drained idle window，沒有重啟](../artifacts/operations/running-services-optimization-20261013/finmind-early-morning-idle-observation-20261014.json)
- [下游 FD 優化後，目前五模式仍完整零 CUDA 回讀](../artifacts/operations/running-services-optimization-20261013/postclose-after-shioaji-fd-defaults-20261014.json)

### 全市場股票尾端準備：先否決門檻，再測原始資料共用

同一 Discord InvocationID 的成功暖機日誌，三個 rebuilt BASE panel 的
`symbols` 階段分別為 29.379／28.921／24.211 秒，合計 82.511 秒。
相同三輪的 external／assemble_rules／total 也保留；這是原生階段計時，
不是把所有取價前時間誤歸給 GPU。另兩個模式命中面板 memory cache。
不能混用不同 feature、checkpoint universe、raw-input 或來源接受世代；
可以測試共用相同的物理股票原始資料，後續仍各走 canonical feature builder。

CPU-only 完整 BASE panel fixture 保存 2,760 個股票檔、共 2,768 個來源／
資格與權利參照／收據成員，1,601,263,137 bytes。四個獨立程序按
original／candidate／candidate／original 執行；每輪重建五個配方、保留
memory reuse、私有 disk cache 發布，逐一比較 `PanelData` 所有 fields、
日期／商品順序、dtype、mask、可選欄位及浮點 bits。沒有 provider 呼叫、
GPU 初始化、行情／訊號／交易寫入或原始資料改寫。

| 完整 BASE panel 工作的中位數 | 原門檻 8,192 | 候選門檻 256 | 判斷 |
| --- | ---: | ---: | --- |
| workflow 牆鐘秒 | 86.316 | 109.922 | 增 27.3%，不採用 |
| CPU 秒 | 140.453 | 182.675 | 增 30.1%，不採用 |
| process peak RSS bytes | 1,963,984,896 | 1,915,781,120 | 減 2.5%，不足抵消時間退步 |

較小八檔樣本的結果分布混合，不能替代此全市場結果。正式 reader 仍保留
8,192，不繼續用同一假說盲試更低值。workflow 包含面板建置、memory reuse、
disk publication 與完整 oracle；imports、完整 fixture fences 在該 stopwatch
之外。它不是 factorized attachment、checkpoint／model、quotes、signals、
opening SLO 或完整訓練 epoch 的改善證據。

新候選只共用同檔／tail 長度／全部 logical parts 世代的原始 Arrow table。
使用 64 MiB 上限、短鎖與 LRU，I/O 不持有全域 mutex；來源變動或 transient
error 不回傳舊 cache。clear 有 in-flight epoch fence。Arrow 的
[slice 是 zero-copy，而 buffer size 會計入整個被引用 buffer](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html)，
因此快取前以 `take` 壓成實際尾端 rows，再按 `get_total_buffer_size` 計額度；
不以 48-row 邏輯 nbytes 掩蓋整個歷史 row group 的常駐記憶體。
source read lease 仍由 canonical live caller 持有，stat 只是防線，不是
資料發布／接受或 owned-FD provenance 的替代證明。

新候選通過 80 項 raw-cache／完整 oracle／固定來源／原始 reader 回歸，
原先獨立保存並綁 SHA 的完整控制與四輪全市場對照亦通過。兩次候選均為
2,760 misses／5,520 hits、zero evictions，實際 cache buffers 27,542,765 bytes
（26.3 MiB），容量上限 64 MiB。每輪皆全欄位／完整來源及 code fence 通過。

| 原始尾端共用的完整 BASE panel 中位數 | 原版 | 候選 | 變化 |
| --- | ---: | ---: | --- |
| workflow 牆鐘秒 | 95.407 | 69.398 | 減 27.3% |
| workflow CPU 秒 | 155.311 | 100.817 | 減 35.1% |
| process 牆鐘秒（imports／fences 亦計入） | 105.531 | 79.434 | 減 24.7% |
| process CPU 秒（imports／fences 亦計入） | 165.410 | 110.834 | 減 33.0% |
| process peak RSS bytes | 1,965,694,976 | 2,055,501,824 | 增 4.6% |

這是同一 live host、nice 19、Arrow／Polars threads 2、panel workers 8 的
CPU-only BASE panel 實測；OS cache 未控制。不能把原門檻實驗的 baseline
與這一輪交叉當作倍率，不能當完整 GPU 暖機或 API／全機改善。額外 RSS
不全是 cache buffers；正式整合須沿 idle/deployment cache clear 釋放來源
快取，避免把盤前換取的重複解碼減量變成盤後長期常駐成本。
此時仍未整合進 production，沒有重載開盤保護中的 owner。

- [原生成功暖機的同 owner 階段日誌](../artifacts/operations/agent-workflow/runs/native-warmup-panel-stage-attrib-20261014T002627-45c6a857/run.log)
- [完整全市場 BASE panel fixture](../artifacts/benchmarks/running_services_20261013/five_mode_live_base_panel_cutoff_20261014/fixture.json)
- [已拒絕的門檻四程序完整對照](../artifacts/benchmarks/running_services_20261013/five_mode_live_base_panel_cutoff_abba_20261014.json)
- [獨立原始 controller／完整 oracle 的保存收據](../artifacts/benchmarks/running_services_20261013/live_base_panel_controller_reference_20261014.json)
- [80 項來源快取與獨立 oracle 回歸](../artifacts/operations/agent-workflow/runs/raw-cache-independent-oracle-tes-20261014T005411-30619270/run.log)
- [原始尾端共用四輪完整輸出與 process 資源對照](../artifacts/benchmarks/running_services_20261013/five_mode_live_base_panel_source_cache_abba_20261014.json)
- [有界原始資料快取](../downloader/live_tail_source_cache.py)、[完整面板對照入口](../scripts/benchmark_live_base_panel_cutoff.py)

### 09:00 開盤現場：入口權限與來源 watchdog 修復

當日原 owner 的 opening attempt 收據，四模式先等待已驗證的開盤覆蓋，
第五模式另有 `PermissionError`：configured direct command
`scripts/update_tw_day_trade_factorized_live.py` 有 shebang，Git mode 卻為
100644。09:02 只把此明確 entrypoint 改為 100755，內容 SHA256
`7b8c56e84cc6255dddacdb5772f4df2d6b270db8b1c60636974b0c114367db9f`
不變。原 bot 的 PATH 已包含 canonical fintech；直接 `--help` 成功，
原生重試自動啟動 updater，沒有注入 signal／price 或重載交易／行情 owner。
21 項 updater 與既有 tiny CPU/CUDA transport/adapter 回歸通過；此小 fixture
GPU 測試在 09:05 後執行，不是正式全市場推論／opening SLO 或零 CUDA 測試。

隨後原生 journal 記錄 `scheduled-watchdog`：第五模式 opening attempt
elapsed／idle 180.130 秒，last progress 仍為 `scheduled attempt started`，
以 code 70 自行退出並由 systemd 重啟。這不是本次 FinMind reload 或手動
Discord restart。後續 bot run 又換代；最新 status 的 ready 5/5 不可沿用
08:08 舊 owner 的 natural-warm acceptance。缺少 validated opens／no-print
證據的 coverage 仍待來源恢復；不降 quorum、不捏造 opening price。

09:12 的有界觀察中，四模式已有 ready opening rows，距離 09:00 分別
約 570–579 秒；第五模式尚在來源準備。因此保留當日開盤 SLO 未達標，
不是把 coverage 等待、準備完成、引擎接收或 broker fill 混成一個狀態。

09:22 修正真正的階段衝突：第五模式 source command 原設定允許 1,200 秒，
`preopen_catch_up` 卻被冷推論 watchdog 180 秒反覆殺掉。新的微型
`SourceOperationRegistry` 只追蹤既有 single-flight command 的真實 Popen
物件，或實際 `LOCK_NB` 失敗的 writer wait（最近 2.5 秒內重新確認）。
來源期限從該工作起點固定，不能因 heartbeat、stdout 或鎖輪詢而滑動。
只在同一 market 的 catch-up／同一解析後 command 允許等待；dead child、
錯誤 command、過期期限或離開來源階段，一律恢復原 hot/cold watchdog。
取價、平行 readonly inputs、模型與 ledger watchdog 不放寬；也不把等待
寫成資料 ready、不刷新假的 model progress、不新增 daemon／source owner。

原 systemd 因舊的 180 秒錯誤自行換代後，Python PID 1,956,508、
InvocationID `e29efcaa6cea46b4acd3477d9e8d43f9` 自然載入新碼。
09:25 原生日誌已證實 `action=wait_bounded_source`：actual child PID
1,956,704、start ticks 4,581,117，idle 180.510 秒而來源剩餘 1,019.497 秒；
沒有手動重啟或額外啟動 projection。唯讀驗收同時確認 Gateway/current
process、journal InvocationID／PID、code／保護 owners，並以事件時間、
boot/start ticks 拒用 PID reuse 或已過期的舊等待證據。

該來源在同一 owner 下自然完成，canonical receipt 為 `verified/preopen`、
attempt 1、decision session 2026-10-14，209 日、14,726 欄，builder wall
485.800 秒（不是含 import／activation 的整個子程序總時間）。current pointer
指向 complete generation `91e7a0e…`；原生日誌 returncode 0，第五模式
09:31:32 的訊號隨後由引擎接收。完整五模式 signal IDs 與當前 engine revision
零落後讀回接受；此驗收沒有重新雜湊全 projection、沒有執行模型／CUDA／
provider 或交易寫入，也不證明當日準時開盤、價格全涵蓋或券商全成交。

09:29 該 live child RSS 約 16.25 GiB，cgroup MemoryHigh 為 16 GiB、
MemoryMax 24 GiB；沒有放大限制。為定位實際成本，對同一 handle 做
10 秒、49 Hz `py-spy --nonblocking`，454 個樣本／4 個讀取錯誤，全部保留。
287 樣本位於 `causal_state_blocks`，155 位於 `write_array_block`，其他 12；
這是短時間 Python stack 線索，不是全 workflow CPU 比例，也未涵蓋完整
native thread 堆疊／配置來源。nonblocking 本來就可能不一致，參見
[原工具說明](https://github.com/benfred/py-spy)。下一階段先對原狀態 sweep、
compact-column 合併及保留正／負零的 lossless writer 做完整輸出對照，
再調物理 buffer／壓縮，不改 TTL、publication、issuer lifecycle 或 Float32 值。
原始尾端 cache 的 canonical 整合／正式採用亦仍待完成；offline 效益不等於已部署。

- [入口 mode 前後、同 SHA、直接 CLI 與原 owner](../artifacts/operations/agent-workflow/runs/factorized-preopen-executable-mo-20261014T010248-4c87903c/run.log)
- [21 項入口與 tiny transport/adapter 回歸](../artifacts/operations/agent-workflow/runs/factorized-updater-entrypoint-re-20261014T010527-7c3b3d53/run.log)
- [當前 owner／原生 watchdog／真實 opening attempt 的有界只讀觀察](../artifacts/operations/running-services-optimization-20261013/opening-source-watchdog-observation-20261014.json)
- [當前 owner 的來源固定期限／PID／journal 時間讀回](../artifacts/operations/running-services-optimization-20261013/discord-source-watchdog-clock-fenced-native-wait-20261014.json)
- [同一 owner 的來源自然完成及五模式引擎接收](../artifacts/operations/running-services-optimization-20261013/discord-bounded-source-native-terminal-20261014.json)
- [384 項 source watchdog、owner clock、排程、閒置／盤後及 worker 回歸](../artifacts/operations/agent-workflow/runs/discord-source-watchdog-full-aff-20261014T012843-802c973f/run.log)
- [最新 terminal verifier 正／負例及完整相關十檔：397 passed](../artifacts/operations/agent-workflow/runs/discord-source-watchdog-final-se-20261014T013556-728bff16/run.log)
- [同一來源 handle 的 10 秒非阻塞取樣身分與完整輸出](../artifacts/operations/running-services-optimization-20261013/factorized-source-owned-nonblocking-profile-20261014.json)
- [未刪減的 454 樣本堆疊](../artifacts/operations/running-services-optimization-20261013/factorized-source-owned-nonblocking-profile-20261014.txt)

### 10:00 年度事件投影：完整原版與候選對照

本輪先保留 source／sweep／writer 原始程式和已正式發布的全部投影分塊。
固定輸入包含 2025、2026 年共 58,318,833 筆年度事件，以及 970,501 筆
當期 delta；209 個交易日、2,757 檔、14,655 個個股特徵及 65 個共用
特徵全部在分母內。訓練仍使用原始 Float32 值、相同 NULL barrier、
TTL、發布時計、報表期間與 issuer lifecycle，沒有改成稀疏模型或低精度。

候選只在私有原版副本內改兩處：用按日期的連續切片取代整年
`partition_by`，並用 `np.take(..., axis=-1)` 取代欄位擷取後的第二次
contiguous 複製。兩者是待測假說，不是「zero-copy 必定更好」的規則；
Arrow／Polars 的切片可能仍持有完整的來源緩衝區。

每個 fresh worker 執行完整 resets → 年度事件正規化／因果 sweep →
lossless NPZ/Zstd 寫出 → SHA/readback → 私有 atomic publication。
逐一比對全部 154 個個股分塊及 common 陣列的 proof、shape、座標、
header 與壓縮 bytes；不是抽樣、feature checksum 或單一模型 scalar。
完整分塊相同也保留所有中性零及負零的解碼語義。

四輪順序為原版、候選、候選、原版。流程之外的輸入 freeze、程式 import、
完整 fixture fence 和獨立 output oracle 成本另外保留在 child wall；
workflow 內仍有資料與輸出驗證。來源與原版只讀，不呼叫 provider／CUDA，
不重啟 Discord、交易或抓取服務。PyArrow/Polars 為 2 threads、nice 19；
主機不獨佔、OS cache 不受控，與正式 Discord 的 MemoryHigh/Max 不是同一
cgroup，不能將離線峰值直接視為正式 RSS 或全機 RAM 改善。

10:20 四輪均已完成，所有正式原版分塊的 byte equality 全部通過。
完整 209 日的平均值如下；CPU 是 CPU 秒，RSS 是每個 fresh process 的
`VmHWM`，不是 cgroup 匿名 memory、主機 RAM 或 GPU VRAM。

| 完整投影流程 | 原版 | 切片＋single gather 候選 | 變化 |
| --- | ---: | ---: | ---: |
| workflow wall | 320.416 秒 | 294.881 秒 | −8.0% |
| CPU | 487.245 秒 | 457.666 秒 | −6.1% |
| peak RSS | 24,724,615,168 bytes | 25,601,361,920 bytes | +3.5% |

第二個候選 peak RSS 達 26,511,151,104 bytes（24.69 GiB）；正式 Discord
所屬 cgroup 的 `MemoryMax` 為 24 GiB。離線 cgroup 不同，這不能直接斷言
正式版本必定 OOM，但不構成正式容量安全的證明。因此保留正向的 CPU/wall
觀察與負向的記憶體觀察，**尚不把此組合帶入正式實作**。slice 留存整個
底層 buffer 是下一個要驗證的機制，不以短 profile 宣稱已唯一定位所有
allocation。原版與候選的所有輸出、原始碼及四份 process/node 觀察都保留。

另已凍結完整 canonical owner 的來源：350 個 receipt-bound FinLab 表、
2,771 個官方來源／相依 receipt 成員、固定年度種子與原始設定。
容量估計 3,289,145,449 bytes，保留 30 GiB 安全空間。這份 fixture 的
FinLab 原始物件是另一次實際 receipt capture，不能假裝與較早正式投影的
FinLab head 全數相同。完整 owner 對照另含上游 normalize/base panel，
逐欄位比對整個 BASE PanelData、全部 delta parquet 以及投影，並禁止
provider／CUDA；目前僅完成入口與語義守衛，**尚未完成完整 owner 效能驗收**。

74 項相關測試通過（包含前序 21 項，不相加），涵蓋分塊數值、NULL／
跨期報表／生命週期、原始輸出 oracle、候選 guard，以及發布時計、來源、
lag、PIT、訓練權限等不可忽略的 manifest 欄位。下一個較大的候選是把
不變的年度事件按內容定址準備一次，避免每天重新排序／拆表；仍須原版
完整 oracle、cold/warm 分列及完整 owner 證明，不把 cache-hit 或 HTTP
成功當作資料完整或全專案最佳化。

- [完整原始輸入／程式與正式原版投影 fixture](../artifacts/benchmarks/running_services_20261013/factorized_causal_projection_20261014/fixture.json)
- [四輪逐階段進度](../artifacts/benchmarks/running_services_20261013/factorized_causal_projection_slices_take_abba_20261014/progress.json)
- [完整四輪結果、全輸出相等與資源／主機觀察](../artifacts/benchmarks/running_services_20261013/factorized_causal_projection_slices_take_abba_20261014/receipt.json)
- [完整上游來源與 receipt fixture](../artifacts/benchmarks/running_services_20261013/factorized_live_owner_20261014/fixture.json)
- [74 項相關語義測試](../artifacts/operations/agent-workflow/runs/factorized-whole-owner-and-candi-20261014T020854-0f06aa8e/run.log)
- [完整投影對照入口](../scripts/benchmark_factorized_live_projection.py)、[完整 canonical owner 對照入口](../scripts/benchmark_factorized_live_owner.py)
- [NumPy take 官方說明](https://numpy.org/doc/stable/reference/generated/numpy.take.html)、[Polars 非複製切片官方用法](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.iter_slices.html)；API 行為與實際效能／記憶體驗收分開。

### 10:42 年度事件內容快取：實作與完整資料驗收

新增 [年度座標快取](../stockagent/data/annual_event_cache.py)，目前只由私人
benchmark 候選使用，正式 AnnualObservationEvents／Discord owner 尚未改動。
按來源完整 SHA、feature／symbol 軸、原始準備程式 SHA、cache ABI、Polars／
Arrow 版本定址；保存 canonical 去重後的原始 Float64、NULL、負值、signed
zero 與 NaN payload，不預先儲存依未來資料改變的財報 period floors。
每日資料寫成獨立 Parquet row groups；若一天跨多組，仍整天讀回再呼叫原始
causal_report_events。暖讀只解碼一個發布日，不持有整年度的切片母體。
依照 touched state keys 取先前版本，未觸及的歷史版本保持不變；跨年 floors、
NULL barrier、TTL、生命週期、reset 與最後 writer 不改動。

來源與 payload 每次驗證完整 SHA；單一 writer、private attempt、payload
fsync／metadata-last 原子發布，保留至少 30 GiB disk reserve。損壞的 derived
generation 留存並記錄，一次呼叫最多重建一次；沒有 provider、CUDA、來源
覆寫、GC、冷庫同步或正式 publication。首次建立與暖讀成本各自記錄，不把
混合 cold/warm 平均當作暖快取收益。

第一個完整來源 run 安全拒絕：舊年檔的 period/value 欄位順序不同，ordered
Schema 比較錯誤地排除同型別、同名稱的資料。未發布任何私有投影。已修成
按欄位名／型別驗證，**保留原本欄位順序與值**，增加 legacy annual 順序
測試；當時相關六檔 91 passed／12.90 秒（含先前案例，不累加）。第二個完整
cold run 已結束，全部 154 compressed members、common 與原版逐位元一致：
626.591 秒、CPU 1,245.704 秒、peak RSS 18,264,047,616 bytes。相比先前同一
frozen cohort 原版均值 320.416 秒／487.245 CPU 秒／24,724,615,168 bytes，
RSS 下降但耗時／CPU 大幅上升，**不採用**。主機不獨佔，不能把不同時間點
比較當成嚴格速度倍率；只用這個結果拒絕沒有完整 CPU 收益的候選。

cache construction 分別 71.971／1.738 秒，其餘費用仍在完整因果投影、writer
及驗證，不把扣掉 cold 成本後的差值冒稱實測 warm 時間。一個 owned child
py-spy 非阻塞 dump 在逐日 Python prior 字典，僅是改動線索，不是熱點百分比
或單一根因證明。已留存當時 helper/controller 原始碼，沒有反覆執行同一
失敗假設的 whole-owner 四輪測試。

下一候選保留 canonical **每年一次** causal_report_events，連同年度因果
結果、期末版本 subject 字串一起存。完整 definitions 與前序檔 SHA 參與 key；
後續 delta 新 subject 即使插入全域排序，暖讀仍經原始 period_order 重建 rank。
跨年、prefix 改變、group policy 改變、terminal state 損毀／重建及新 rank
插入等測試加入後，最新六檔 96 passed／15.73 秒。新候選完整 cold 與全新
程序 warm oracle 都通過全部 154 members／common 逐位元比較。

| 完整因果投影（不含 upstream／model） | wall 秒 | CPU 秒 | peak RSS bytes |
| --- | ---: | ---: | ---: |
| 原版先前 A1/A2 均值 | 320.416 | 487.245 | 24,724,615,168 |
| prefix causal cache 首次 | 271.472 | 765.582 | 22,732,627,968 |
| prefix causal cache 單次暖讀 | 145.192 | 233.559 | 5,504,344,064 |

首次 cached preparation 131.369 + 1.624 秒仍記在 cold workflow，不能說首次
CPU 也下降。暖讀 6 hits、0 builds、0 rejections；hit 驗證 1.755 秒仍計入
workflow。這是有利的單次候選觀察，不是同時段 ABBA 正式倍率，也不是所有
服務、API 吞吐或原始資料完整性的證明。

完整 owner priming 已完成：含 350 FinLab tables、2,771 公開相依檔的 normalize、
全部 BASE PanelData fingerprint、delta、投影、校驗與私人發布，356.534 秒、
CPU 965.573 秒、peak RSS 14,945,136,640 bytes。只有此 fixture 的新 delta 做
兩次 build（4.921 + 0.117 秒），四個不變年檔 hit；provider/CUDA hard-fail
fence 無觸發。這不是已比對原版的整體結果；原版／候選／候選／原版四輪
對照已由 canonical task owner 啟動，正式 source owner／預設仍未改動。

11:39 第一輪 whole-owner A/B 已驗證一致；baseline 549.940 秒／848.633 CPU
秒／24,084,766,720 peak RSS bytes，暖讀候選 361.027 秒／531.079 CPU 秒／
14,618,251,264 bytes。候選六個年度讀取全命中，沒有 build／repair；完整
BASE、delta SHA、所有 compressed members／common 及來源／因果／權限
契約均由 frozen original 比對。這是第一輪實測，不先宣告 ABBA 已完成。

canonical 整合仍有兩項 gate：原 service MemoryHigh 16 GiB／MemoryMax
24 GiB，而本次 benchmark 的 cgroup 沒有該上限；不得由暖讀峰值推論冷
建置一定適合原 owner。年度快取的 sweep／policy／helper identity 必須納入
來源 generation，先在離線 CPU 子程序準備與驗證，再交既有 source owner
讀取。既有 checkpoint、source receipts 與模型 ABI 不變，不於盤中重啟
trading service。

11:43 第二個候選程序也通過 whole-owner contract，384.537 秒／557.418 CPU
秒／14,880,153,600 peak RSS bytes；六份 cache 全命中且零 build／repair。
最後 original 程序仍在執行。另一次只讀 GPU FD probe 當下檢視 61 個 managed
Linux processes 全部可讀、zero GPU owners；Discord owner 已由其他流程換成
PID 2231758／InvocationID `efada561d7924f35ae43189600e56ac1`。本工作沒有
重啟或停止它，故不把重啟後這個瞬時零 FD 歸因於本候選，也不當作盤後
GPU／全 Windows allocation 的驗收。

11:52 whole-owner ABBA **全部完成與一致**，不是只挑最快 worker：

| 四個全新程序的兩輪均值 | frozen original | 暖讀候選 | 變化 |
| --- | ---: | ---: | ---: |
| 完整 normalize／BASE／causal／write／validate／private publish 秒 | 545.401 | 372.782 | 減 31.65% |
| 完整 workflow CPU 秒 | 850.836 | 544.249 | 減 36.03% |
| process peak RSS bytes | 23,392,735,232 | 14,749,202,432 | 減 36.95% |

各輪完整 BASE fingerprints、delta bytes、154 compressed members／common、
來源／日期／feature／PIT／clock／permission／ABI 均一致；兩輪候選各自六
份 cache 全 hit、零 build／repair。原版 A2 540.861 秒／853.040 CPU 秒，
不是沿用先前不同時段原版作分母。主機不獨佔、OS cache 未控制，仍不得當
成全部 services／API／training／model inference 的改善百分比。

12:00 已將候選整合到 canonical `AnnualObservationEvents(cache_root=...)`，
預設 `None` 保持原功能；cold／uncached 共用唯一原始 source-part precedence
helper。live `prepare` 可指定 `annual_cache_root`，尚未修改 updater 排程來
啟用它。helper／sweep／publication rules／runtime 加入 generation 指紋，
並補上 config／actual uploads／publication lower bounds input hashes 與發布
前讀回；source clocks 改變後不會誤用 already_current。terminal sidecar 寫完
再次檢查 30 GiB reserve，避免只檢查主 payload 漏掉最後寫入。115 項相關
測試通過／17.97 秒（含前述案例，不累加）。

canonical cold whole-owner 私人投影與派生 cache 已開始；cache 寫到既有
`artifacts/cache/tw_factorized_live/annual`，無原始資料、trading／GPU／API
寫入。此步是預先準備可被原 owner 讀的派生內容，尚非原 owner 部署驗收。
完整 source code 也保留在私人 worker 的 `shared-code`；驗收只允許逐項驗證
過的 code/config/context metadata 差異，其他全欄位仍與 frozen original
strict 比較，禁止無條件刪除來源或模型指紋來宣稱 parity。

12:09 canonical cold worker 與其原版驗收已成功。完整 workflow 505.972 秒／
1,449.204 CPU 秒／22,989,430,784 peak RSS bytes，六份 cache 都是首次
build，zero rejections。全部 BASE／delta／compressed members／common 與
independent original 一致；code/config/publication metadata 逐項驗證後才
允許正規化。**cold CPU 成本高於原版，不宣稱首次 CPU 改善**；派生 cache
已在正式 cache root，正式投影 current／updater 啟用仍未改動。下一個全新
程序正在測同來源暖讀，還要 zero build／repair 及完整 parity。

30 GiB reserve 在 terminal sidecar 寫後遭耗盡的額外測試已通過（1 case／
3.61 秒），留下 attempt、不發布 current head。原始 reader／engine 共
471,167 bytes 亦已按實際 SHA 凍結，供後續 64 MiB raw-tail 正式 hook 的
獨立 oracle 使用，保留當時其他開盤 timing 修改，不反轉使用者 dirty work。

- [canonical cold 全管線收據](../artifacts/benchmarks/running_services_20261013/factorized_canonical_owner_cold_20261014/receipt.json)
- [canonical cold 與原版完整接受證明](../artifacts/benchmarks/running_services_20261013/factorized_canonical_owner_cold_acceptance_20261014/receipt.json)
- [實際 BASE reader／engine 原始碼](../artifacts/benchmarks/running_services_20261013/live_tail_canonical_callers_20261014/receipt.json)

12:18 canonical warm full-owner 379.436 秒／922.883 CPU 秒／17,312,722,944
peak RSS bytes，六個 hits、zero builds/rejections，完整原版 parity 通過。
**CPU 並未較前面原版改善，不由這一輪宣稱已適合 production**。查到
agent launcher 的 `RUNTIME_ENV_KEYS` 只轉送 runtime/identity/CUDA 等六項，
不包含 shell export 的 Polars／OMP／BLAS pool 設定。ABBA controller 本來
在每個 child 的 `env` 明確設四個 pools；直接 agent worker 則沒有這項
保證，先前 standalone cold／prime／warm 的 shell requested=2 不能當
observed Polars=2。Arrow worker 另有明確 set_cpu_count，與 Polars 不同。

12:24 新增純 stdlib、import 無副作用的 scoped CPU-source profile，在
source updater／benchmark CLI 的任何 numeric import **之前** 明確設定。
僅這些 CPU-source 子程序使用，非全 service、training/GPU 政策或 process
CPU hard quota。source CLI 預設 2，可由 `STOCKAGENT_FACTORIZED_PREP_THREADS`
指定；benchmark 固定 2 且記錄實際 `pl.thread_pool_size()`、`pa.cpu_count()`
與 requested four-pool env，對照不相符即拒絕。原始資料／model／checkpoint
ABI 不變。source updater cache-root 仍未啟用，native loaded-code acceptance
亦尚未宣稱。

canonical source 最新四輪對照已啟動；這次 baseline/canonical 均由相同入口
套用實際 profile，且 candidate 必須 zero build／repair。不得把不同 pool
設定的先前 standalone 秒數混入此新對照分母。

最新九檔 related semantic／fingerprint／pool／reserve 回歸 124 passed／18.50
秒，含前述案例、不累加。numeric import 前設定符合
[Polars thread-pool 官方契約](https://docs.pola.rs/api/python/stable/reference/api/polars.thread_pool_size.html)；
Arrow 的可配置 CPU pool 仍使用其
[set_cpu_count 官方 API](https://arrow.apache.org/docs/python/generated/pyarrow.set_cpu_count.html)，
不是把兩種 pool 的行為混為一談。本 repo/data package entrypoints 已確認
無 numeric eager import，因此 scoped stdlib 設定確實在 Polars 初始化之前。
新四輪 canonical owner run 是 `canonical-full-owner-observed-po-20261014T042528-7152df3c`，
仍由既有 task owner 監督；共享 source code 封存中，不併行修改 panel／runtime
來破壞比較。整體 goal 保持 active，native cache 啟用、完整新世代盤後
zero-CUDA／五模式復用、raw-tail 正式 hook、opening SLO 和其他服務餘項均
還須獨立 current receipts，不能由這一份 CPU benchmark 宣稱全部完成。

- [124 項 source／profile／完整契約回歸](../artifacts/operations/agent-workflow/runs/explicit-source-pools-all-contra-20261014T042419-559ef967/run.log)

- [限定 CPU-source 入口的 runtime profile](../stockagent/data/source_processing_runtime.py)
- [explicit profile／invalid setting／affinity 回歸](../test/test_source_processing_runtime.py)
- [canonical warm 完整 parity](../artifacts/benchmarks/running_services_20261013/factorized_canonical_owner_warm_acceptance_20261014/receipt.json)

- [完整 owner 四程序 ABBA](../artifacts/benchmarks/running_services_20261013/factorized_prefix_causal_owner_abba_20261014/receipt.json)
- [canonical annual opt-in 與原始 report-floor 回歸](../test/test_annual_observation_events_cache_integration.py)
- [configuration／publication context 失效回歸](../test/test_factorized_live_preparation_identity.py)
- [115 項完整契約與 fingerprint guards](../artifacts/operations/agent-workflow/runs/canonical-annual-cache-proof-gua-20261014T035917-3786748d/run.log)

10:47 canonical 普查為 87 installed services + 2 engineering transients、67
timers、3 paths，其中 active 23（含工程程序）。OpenBB archive 正在執行，
不是沿用凌晨 failed 狀態。工程 cold candidate 在該 21.03 秒 interval 使用
4.624 CPU cores；Polars/Arrow/BLAS requested threads=2 是各自 pool 設定，
**不是程序 CPU 硬上限**。不把工程負載算成正式服務閒置佔用或全機改善。

- [第一次完整資料拒絕與堆疊](../artifacts/operations/agent-workflow/runs/complete-annual-cache-cold-proje-20261014T023905-baf4192d/run.log)
- [最新欄位順序與來源／causal／writer 回歸](../artifacts/operations/agent-workflow/runs/annual-cache-legacy-schema-regre-20261014T024051-721fede5/run.log)
- [原始事件快取測試](../test/test_annual_event_cache.py)
- [完整 raw-cache 負結果與全輸出一致性](../artifacts/benchmarks/running_services_20261013/factorized_prepared_coordinates_cold_v2_20261014/receipt.json)
- [當時 helper 原始碼](../artifacts/benchmarks/running_services_20261013/factorized_prepared_coordinates_cold_v2_20261014/original_cache.py)、[當時 controller 原始碼](../artifacts/benchmarks/running_services_20261013/factorized_prepared_coordinates_cold_v2_20261014/original_controller.py)
- [96 項年度因果／prefix／terminal version 回歸](../artifacts/operations/agent-workflow/runs/annual-causal-prefix-cache-guard-20261014T025854-a4ace05e/run.log)
- [完整 prefix causal 首次結果](../artifacts/benchmarks/running_services_20261013/factorized_prefix_causal_cache_cold_20261014/receipt.json)、[全新程序暖讀與完整輸出一致性](../artifacts/benchmarks/running_services_20261013/factorized_prefix_causal_cache_warm_20261014/receipt.json)
- [完整 source owner priming 與所有 BASE 欄位 fingerprint](../artifacts/benchmarks/running_services_20261013/factorized_prefix_causal_owner_prime_20261014/receipt.json)
- [工程測試期間的完整服務清冊](../artifacts/operations/running-services-optimization-20261013/census-during-annual-coordinate-cold-20261014.json)

### 12:40 正式 annual-cache 呼叫端：實際執行緒的完整對照

新的四輪 whole-owner ABBA 使用正式 `AnnualObservationEvents` 快取呼叫端，
所有 worker 都讀回 actual Polars=2／Arrow=2。原版仍是事先凍結、獨立保存的
owner／sweep／writer，不拿修改後的程式自稱原版。第一輪 original 完整
507.663 秒／790.315 CPU 秒／24,504,553,472 peak RSS bytes；第一輪 canonical
暖讀 360.901 秒／542.714 CPU 秒／14,672,453,632 bytes。兩者全部 BASE
fields／delta SHA／154 個 compressed members／common／source clocks／PIT／
permission／ABI 已一致。candidate 六份 cache 全 hit，zero build／repair。

此時只完成 2/4，最後兩輪還在執行，尚不宣告正式 ABBA 的百分比或 native
採用。updater 的 annual-cache 參數仍未啟用；正式 bot 為另個流程重啟後的
PID 2231758、InvocationID `efada561d7924f35ae43189600e56ac1`，這個來源子程序
仍受 MemoryHigh 16 GiB／MemoryMax 24 GiB 約束。原版與候選都在有其他工作
的主機量測，不是 exclusive-host／OS cold-cache 或 GPU 推論結果。

- [四輪目前的全欄位、actual-pool 與資源進度](../artifacts/benchmarks/running_services_20261013/factorized_canonical_owner_observed_pools_abba_20261014/progress.json)
- [盤中 61 個受管理程序 GPU FD 身分取樣](../artifacts/operations/running-services-optimization-20261013/managed-gpu-during-prefix-coordinate-cold-20261014.json)：只有 intraday Discord owner 的兩個 FD；不是全部 Windows allocation 或整卡 zero-use。
- [Arrow row-group 寫入規則](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html)、[完整 row-group 讀取 API](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html)；官方 API 說明不是本機效能證據。

### 12:56–13:07 完整四輪接受；容量不足只委派既有 owner

上述 ABBA 已完成 4/4，不再是 12:40 的中途估計。兩輪原版完整 workflow
平均 532.150 秒／829.459 CPU 秒／24,318,144,512 peak RSS bytes；兩輪
正式快取 caller 平均 365.169 秒／537.727 CPU 秒／14,641,229,824 bytes，
分別改善 31.4%、35.2%、39.8%。四輪 actual Polars=2／Arrow=2，全部
209 dates、2,757 symbols、14,726 channels 的 BASE 原始 bit／delta bytes／
所有 members／common／source clocks／PIT／permission／ABI 相同。兩輪
candidate 各六個 cache hit，zero build／repair。包含完整資料準備、寫出、
驗證與 private atomic publication／BASE fingerprint；不包含模型前推、
provider/API、native publication 或已排除其他主機工作影響的宣稱。

四輪完成後，source owner 才新增 `MemoryAdmittedObservationCache`，不修改
已暖好六份快取的 key／coordinate helper。只有實際 miss 的 canonical builder
callback 才觀察 MemAvailable 與祖先 cgroup headroom；暖讀不做資源探測。
冷 seed 的 24 GiB working-set 加 2 GiB reserve 是保守的工程估計，並非所有
未來來源的數學上界。不足／無法證實容量立即留下 `cache_warming_required`
收據，保留 last-good，不在同 cgroup 反覆重試。既有隔離 artifact owner
只在休市／完成發布門檻通過時執行原 canonical source command；讀回 verified
或 already_current 才接受。derived panel 過舊不阻止來源恢復，盤中不能藉此
執行重建／GPU 模型。不是增加新 downloader、daemon、pipeline 或記憶體限額。

即時行情的 raw Arrow 尾端共用也已接入正式 `build_tail_panel`，上限 64 MiB，
可設 0 關閉。只有真實 tail build 才 lazy 建立；同 key 單次 reader、source
stat 前後 fencing、bounded LRU／實際 buffer bytes 與 epoch-fenced idle clear
仍依原 cache 契約。不同模式的 feature build／permission／PIT／模型輸入
各自產生，不能拿另一模式的 feature panel 共用。原生 adoption 與五模式
新世代零 CUDA 回讀尚未接受，不能沿用凌晨的正式 key。

最新 focused 回歸分別是 184 passed／18.20 秒與 84 passed／19.03 秒；含
共享 key、seed 不重建／不探測、delta 才 admission、跨年度 floors 原值與
負零、updater 四次重試設定下只執行一次、隔離 owner 讀回失敗拒絕及原
retry probe 邊界。兩組重疊，不相加稱 268 個不同案例。先前失敗來自測試
feature pattern／NaN equals 及錯誤測試路徑，已修正測試；原失敗收據保留。

- [正式 caller 完整四輪結果](../artifacts/benchmarks/running_services_20261013/factorized_canonical_owner_observed_pools_abba_20261014/receipt.json)
- [資源 admission 與 warm-only 行為](../stockagent/data/annual_cache_admission.py)
- [既有隔離 owner 的 cold-seed 恢復](../test/test_factorized_cache_worker_recovery.py)
- [184 項 canonical raw-tail／admission 回歸](../artifacts/operations/agent-workflow/runs/canonical-raw-tail-and-admission-20261014T050157-400cbc3e/run.log)
- [84 項 canonical updater／recovery／causal integration 回歸](../artifacts/operations/agent-workflow/runs/canonical-cache-admission-recove-20261014T050712-d5d51a56/run.log)
- [修改前實際 panel／engine caller 的私有封存證據](../artifacts/benchmarks/running_services_20261013/live_tail_canonical_callers_20261014/receipt.json)

### 13:10 正式 raw-tail hook：五模式完整 ABBA 接受

新四輪使用正式 engine 的 lazy raw cache getter 和實際 `build_tail_panel`
hook；candidate 不替換 reader。baseline 從改動前 400-mode immutable
panel／engine 原始碼恢復原 caller，另逐 AST 驗證未改動的其他 helpers，
並依既有獨立 full-output oracle 驗證全部 PanelData fields／dtypes／ordering／
mask／raw float bits／memory reuse／private disk publication。五模式各自的
feature build 不共用，只有來源原始 Arrow 尾端在相同 stat cohort 內共用。

原版兩輪 workflow 中位 92.254 秒／146.698 CPU 秒／2,026,346,496 peak
RSS bytes；正式 candidate 67.087 秒／91.272 CPU 秒／2,115,907,584 bytes，
wall 少 27.3%、CPU 少 37.8%、RSS 增 4.4%。含 fresh child startup 的
process wall 分別 103.046／76.562 秒，不能拿不同分母混合。每輪 candidate
2,760 misses／5,520 hits，27,542,765 actual resident buffer bytes，小於
64 MiB hard budget，zero eviction；idle clear 的 epoch fence 防止尚在讀取
的舊 callback 重新放回快取。Polars=2、Arrow=2、panel workers=8 均讀回。

收據為 `accepted_complete_base_panel_only`，沒有 factorized attachment、
checkpoint／模型／quote／signal／交易、API call、CUDA call 或 native
deployment。後續完整來源 guarded warm worker 使用所有最新 source/cache
檔案的 SHA fencing，與獨立原始 owner 再驗後才啟用 updater 參數。

另外發現 updater CLI 與 benchmark 的 Arrow pool 設定不一致：原 scoped
profile 只設定 Polars／BLAS。CLI 已補明確 `pa.set_cpu_count(effective_threads)`，
在 CPU-only source 邊界使用；成功 updater 收據另記 actual Polars／Arrow，
不是假定 environment 設定已被遵守。fresh subprocess 回歸檢查 2／3 的
設定、visible-affinity clamp、help 未啟動 data owner、CUDA 未初始化。

- [正式 raw-tail 呼叫端四輪與完整原版 oracle](../artifacts/benchmarks/running_services_20261013/five_mode_canonical_raw_tail_hooks_abba_20261014.json)
- [canonical raw-tail hook／idle／並行 epoch 回歸](../test/test_canonical_live_tail_source_cache.py)
- [Arrow 的獨立 CPU pool 官方契約](https://arrow.apache.org/docs/python/generated/pyarrow.set_cpu_count.html)

### 13:16–13:40 guarded warm 接受；啟用既有 updater，不冒充原生換代

最後一次完整 guarded warm 使用目前 source/cache/admission 檔案，209 日期、
2,757 股票、14,726 channels，wall 395.377 秒／CPU 572.612 秒／peak RSS
13,731,569,664 bytes，六份 annual cache 都 hit，zero build/rejection。
全部 BASE fields／delta／154 個外部 members／clock／PIT／permissions
與原始完整 owner 控制一致。它是一輪接受，不拿與不同負載的先前數字相除
宣稱新的速度倍率。13:19 才啟用 updater 的 canonical annual root；
76 項 updater/source contracts 與 324 項較廣 panel/live/cache 回歸通過。

cache miss 的資源 admission 不變。另補 source-warming 的恢復責任：只有既有
`stockagent-discord-artifact-maintenance.service` 完整隔離 owner 可重建；
cache-only bridge 在載入 bot／model 前委派。即使沒有歷史 failed-job，也能由
原 retry timer 讀回明確的 warming receipt，且不能自行重寫歷史 job 狀態。
資源 headroom 抖動不當作新 source；未變工作的喚醒合併 300 秒，committed
source 換代才可提早喚醒。164 項回歸含隔離／bridge、禁用／選擇範圍、lock、
readback、無 Torch 的 fresh warming probe。原測試缺少 future retry deadline
而誤入禁止 backfill 分支，修正 fixture 後接受；沒有調整 production backoff
來遷就測試，原失敗 log 保留。

13:40 自然維護 timer 正常觸發，五個模式皆讀到 latest=2026-10-13，
target=2026-10-14，返回 `waiting_source`，attempted=0、deferred=5、failures=0；
未重啟服務、未另派重複下載／重建。native updater receipt 此時仍是舊的
`verified/preopen`，沒有 annual-cache/pool 欄位，不能證明新參數已被正式採用。

- [guarded 完整 owner 暖讀](../artifacts/benchmarks/running_services_20261013/factorized_canonical_owner_guarded_warm_20261014/receipt.json)
- [與原始完整 owner 的全輸出接受](../artifacts/benchmarks/running_services_20261013/factorized_canonical_owner_guarded_warm_acceptance_20261014/receipt.json)
- [164 項隔離恢復／retry／bridge 回歸](../artifacts/operations/agent-workflow/runs/source-warming-isolated-owner-fi-20261014T053324-f61f9495/run.log)

### 13:53–13:56 閒置 allocator：回收 pinned RAM，不反覆碰 GPU

真實盤後取樣仍見主服務保留約 3.4 GiB RAM。13:37:55 的主服務重啟是其他
原生操作，不是本任務所做；13:44 以目前 PID 再查，63 個可讀 StockAgent
Linux 程序只有 Discord 持有兩個 `/dev/dxg` FD。FD 不等於實際 VRAM bytes，
整卡使用量也不能全部歸屬這一程序。13:30／13:38 的五模式結果是
`postclose-fast` provisional snapshots，不能拿來驗收當日正式完整收盤快取。

先修正確定的共享邊界，不猜所有 RAM 都是同一原因：原本只看 model cache
是否非空，會跳過 CPU-only 快取，且在部署失效已清空 models 後漏掉 allocator
回收。現在只檢查既有快取，不建立新快取；model invalidation 保留待回收旗標，
同步／workspace／device／host flush 全成功才清除，失敗仍可由原 idle owner 重試。
仍使用原交易時段／其他市場／推論 mutex 保護，未把 `empty_cache` 放進每次
forward 或每輪 idle。真正空閒立即 no-op，沒有 GC、CUDA probe 或同步。

已檢查本機 PyTorch 2.13 的公開 `torch.accelerator.empty_host_cache()`；
它與 device allocator 的 `torch.cuda.empty_cache()` 分開。在四個 fresh
程序中，原始函式從 immutable 原 caller AST 還原，使用真正 H2D、CUDA
allocation／kernel／synchronize 與 256 MiB inactive pinned staging、64 MiB
device scratch，另保留仍 active 的 1 MiB pinned sentinel。

原始釋放後 host owned=269,484,037 bytes、active=1,048,576 bytes；兩輪
候選釋放後 owned=active=1,048,576 bytes，inactive=0。兩輪候選 RSS 各降
268,435,456 bytes（256 MiB）；兩輪原版不降。四輪小型模型完整 output SHA256
及 active sentinel bits 一致；所有 device allocated/reserved 均回到零。
一次性候選釋放 wall 0.196／0.228 秒，之後各五次 idle 檢查零 CUDA／GC。
這是**有界 allocator 機制**，不是金融模型完整流程、整機 RAM 或 Windows
整卡占用倍率；原生新碼採用及其閒置讀回仍待驗。

首次 probe 的 reproduction assertion 失敗：CUDA synchronize 完成 copy 不代表
host allocator 的 pending-event 統計已退休。第二版用正常的同容量再分配退休
已完成事件，再判定 inactive bytes；沒有用 host flush 預先消除受測問題。
失敗收據／log 保留，由四輪通過收據 supersede。198 項初始回歸擴為同一輪
290 項 shared lifecycle／controller／updater／cache 回歸通過，不相加冒充 488。
BASE-only 原 caller oracle 明確排除 idle teardown 的改動，資料／模型 builders
與獨立 full-output 控制仍受原 fence；不把較早速度結果當作新版本重測。

- [四輪原始／候選 allocator 對照](../artifacts/benchmarks/running_services_20261013/idle_pinned_host_allocator_event_retirement_abba_20261014.json)
- [290 項共享回歸](../artifacts/operations/agent-workflow/runs/canonical-postclose-lifecycle-sh-20261014T055331-f5542a3f/run.log)
- [13:44 現任程序 GPU handle 讀回](../artifacts/operations/running-services-optimization-20261013/managed-gpu-first-postclose-verified-owner-20261014.json)
- [PyTorch 公開 host-cache API](https://docs.pytorch.org/docs/2.14/generated/torch.accelerator.memory.empty_host_cache.html)
- [官方 pinned allocator／非同步事件與 phase-change 回收說明](https://docs.pytorch.org/devlogs/eager/2026-08-09-pinned-memory-allocator/)

### 14:00 原生來源仍在發布；下一個完整 owner 熱點

14:00 正常完整維護再次觸發，讀到 `tw_public_refresh_in_progress`，五模式仍是
latest=2026-10-13、target=2026-10-14，zero attempt／failure、五項 deferred，
wall 29.772 秒、CPU 9.427 秒、memory peak 490.6 MiB。這是正確等待一致來源，
不是成功模型快取，也不能用程序 exit 0 代替完成收據。新 annual cache／pool
的 native adoption 與今天五模式 zero-CUDA 完整讀回仍待真正 committed source。

14:00:55 的 20 秒普查列出 87 installed services＋1 engineering transient，
其中 28 個 installed units 為 active/activating，67 timers、3 paths。
Discord 此段為 0.0855 CPU core、3,303,723,008 current bytes，其中 anon
2,866,470,912、file cache 420,294,656；不是新 idle hook 正式採用的改善證據。
同一段 `stockagent-tw-public-publication-sweep.service` 為 3.754 cores／
13,297,573,888 anon bytes，且主機 PSI some avg10 CPU=38.44、memory=11.28。
後續 live readback 的 sweep RAM 已降為約 1.75 GB，顯示是特定處理階段，
不是持續占用或已證實 leak。下一階段定位其完整來源→處理→發布流程的
重複計算與峰值，保存原版全輸出後才選 thread／vectorization／bounded cache。
OpenBB compaction 的 matched-stage memory.high events 仍是獨立未完成事項，
不直接提高全機配額或清掉 OS page cache；沒有更動其 storage owner。

- [14:00 全服務、匿名 RAM／file cache 與 PSI 普查](../artifacts/operations/running-services-optimization-20261013/census-after-once-only-idle-code-20261014.json)

### 14:12–15:00 真正等待、閒置排程與盤前／收盤世代

#### 原生發布不是一直在跑 CPU

保留同一個 publisher→finalizer→entitlement child 的身分追蹤，不重啟、不另開
下載。第一輪 active-only nonblocking py-spy 為 0 samples／1 error，沒有 CPU
熱點結論。第二輪納入 idle threads，10 秒得到 1,890 samples／0 errors、10 個
Python threads；stack 主要為 shared limiter 的 Event wait、HTTP/TLS/DNS I/O
及官方拒絕頁面的 bounded retry sleep。樣本權重是多執行緒 wall time，不能
加總當 CPU 秒；沒有增加並行或繞過官方限制。

14:28 的正式 completed-session receipt 為 `status=ok`、`after.current=true`、
target=2026-10-14。entitlement 首次 961.968 秒／return 2，第二次 292.441 秒
接受，失敗沒有隱藏。public feature 完整建置 9,633,014 rows／143 features，
`mode=full`、reused=0：stock_build=145.196 秒、stock_merge=45.832 秒、
stock_margin=29.038 秒、output_assembly=25.567 秒。原生 source unit 報告
43m16.585s CPU／27m40.079s wall／44.8G memory peak；不是常駐 RAM 或 leak 證明。

增量 prefix 的既有 gate 要求所有依賴 bytes 不變，今日原始 OHLCV／TAIFEX 等
來源改變，不能把 14-day 選項強行解釋成只算最後 14 天：未有歷史分區修訂
證據時，full fallback 保留正確性。後續忙碌補觸發又進入 `preopen_all`：
41 個來源真的改變、6 個下載失敗、15.8G memory peak／3m30.738s CPU。
這個 fallback 很昂貴，但不能宣稱整輪完全冗餘；本階段沒有改 phase/source
scope、跳過新資料或冒稱 full builder 已最佳化。

- [active-only 原生樣本，無有效 CPU 結論](../artifacts/operations/running-services-optimization-20261013/native-close-entitlement-speedscope-20261014.json)
- [idle-inclusive 原生等待 stack](../artifacts/operations/running-services-optimization-20261013/native-close-entitlement-idle-speedscope-20261014.json)
- [非阻塞取樣官方說明與限制](https://github.com/benfred/py-spy#how-can-i-avoid-pausing-the-python-program)

#### 兩個真實 idle callbacks：不要每 100 ms 重讀設定

原 bot source 保存在 immutable snapshot。四個 fresh exec 程序依 ABBA 執行
完整 `scheduled_signal`／`postclose_fast_cache` 的 settled idle 路徑，各 30 秒；
使用全部五個目前 enabled TW modes、完整 readiness/state snapshot、原 calendar
resolver。來源／模型／交易／發布 callback 禁用，四輪完整 keys／calendar
decisions 一致、CUDA 未初始化、候選 source SHA256 相同。

原版 CPU 平均 1.653963086 秒，候選 0.034781286 秒，少 97.8971%；兩個 task
各 300→6 次喚醒，state read 6,000→180、readiness read 1,505→11。
Wall 都是觀測窗口 30 秒，不是工作變快 50 倍。peak RSS 約 854→853 MB、
差異極小，不作 RAM 優化結論。排程 priority sort 一次讀完整 readiness，
沒有新的 TTL 或漏掉下一輪來源替換。

已在既有 Discord tasks 上使用公開 `change_interval`：平時最長 5 秒，
已知截止前 30 秒進入原 100 ms；未完成的 schedule key／input job，包括
broker confirmation／retry，保持 100 ms。分鐘 delay、跨日（含不整除一天的
interval）、各市場 timezone、DST、閉市／unknown fail-closed、已完成 close
no-op 與真實 Discord Loop interval 變更均測試。新設定的 idle 外部變更最多
下一個 5 秒 poll 才可見；不是任意 user request 的 latency 保證。

首次四輪未完成：前三輪舊 code receipt 保留，期間候選 generation 改變，
沒有接受 aggregate。stable-v2 保留每一輪 stdout/stderr、完整四輪 hash 一致
後才接受。初始 pytest 亦曾因猜測不存在的兩個 test paths 而無 tests executed；
後續實際 15 個檔案的 472 passed 取代，並未把 command error 當產品錯誤。

- [完整原始／候選 idle ABBA 與各 worker 收據](../artifacts/benchmarks/running_services_20261013/canonical_idle_scheduler_abba_20261014/stable-v2/receipt.json)
- [可重跑的 canonical callback 對照程式](../scripts/benchmark_live_idle_scheduler.py)
- [Discord tasks 公開變更 interval API](https://discordpy.readthedocs.io/en/stable/ext/tasks/index.html#discord.ext.tasks.Loop.change_interval)

#### 4／5 正式 close cache；剩餘 projection 沒有升到 close

14:36 的原生 close worker attempted=5／failures=1：四模式產生今日官方收盤
快取，factorized 模式報 `factorized projection does not cover the requested live
calendar`。其既有 generation 本來是合法的盤前資料：calendar 209 dates 到
2026-10-13，paper receipt decision_session=2026-10-14、completed_session=2026-10-13。
decision-session 標籤不證明已包含當天收盤。第一輪 readonly canonical BASE 診斷
遇到來源 writer lease，正確在模型／BASE 工作前被拒絕。15:12 在既有 read guard
允許後重查，真實 BASE 為 48 dates（2026-08-05～2026-10-14）、2,760 symbols；
projection 的 requested calendar 確實只缺 2026-10-14，且為 tail-only。使用既有
disk panel cache 的完整日期比較耗時 0.257 秒，零模型／provider、CUDA 未初始化、
source writes=0。這不是 feature values 或五模式 close acceptance；沒有插值、
用舊值蓋過或略過 calendar validation。原 lease 拒絕紀錄保留，由同一 BASE
檢查的新成功執行 supersede，不改寫原 exit code。

根因在 cheap close bridge：原恢復僅處理 `AnnualCacheBuildDeferred`，普通
`verified/preopen` 的舊 projection 不觸發 configured completed-session updater。
現在 receipt-only controller 同時辨識這一種 calendar-tail lag，僅把目前
enabled 且綁定既有 canonical updater 的 mode 排入 `source_projection_refresh`。
hint 非 acceptance、不載入模型。cheap bridge 一律 defer source 重建；完整隔離
owner 仍要求非交易中、accepted official close、同一來源 lease／memory admission、
configured canonical completed command。成功必須同時有 accepted updater receipt
與已更新的真實 manifest 日期，不用 transport success 或標籤代替。

新 cold receipt-only probe 0.00968 秒、`model_runtime_imported=false`、source writes=0，
正確要求 2026-10-14，而 projection_end=2026-10-13。資料品質 scoped check 只處理
此日期 join／新鮮度問題，沒有重新做另一份全資料報告或把 sparse features 當壞值。
新增 ABI／future clock／pointer escape／無官方收據／錯誤日期／enabled scope／
市場時段／只報成功未實際更新等測試；一輪 fixture 曾受 native scheduled selection
影響而 no due，已隔離 env，完整 472 項共享回歸接受。不加總此前 114／319／243
等重疊測試冒稱更高覆蓋。scoped Ruff 與 `git diff --check` 通過。

現任 Gateway 仍是 13:37 的 invocation、reload=false；本階段沒有重啟它，
因此 **97.9% idle 改善、新 pinned-host release 的 Gateway 採用尚未證明**。
新 controller 是既有一次性 timer/worker 路徑，可由下次正常啟動載入；仍需讀回
source owner 的 annual cache/pools、新 projection 和五模式 zero-forward/CUDA。
其他 actor 正在改交易／新聞模組，保留其 dirty work，不把 runtime 全重載當作
只有本階段修改。全 87 installed services 的目標維持 active，full-source peak、
publication catchup plan、OpenBB compaction 等仍各自待實測與驗收。

- [真實冷 receipt-only source hint](../artifacts/operations/running-services-optimization-20261013/native-stale-projection-source-hint-20261014.json)
- [15:12 原生 BASE／projection 完整日期差集](../artifacts/operations/running-services-optimization-20261013/native-factorized-close-calendar-after-source-20261014.json)
- [可重跑的只讀日期／source hint 診斷](../scripts/diagnose_factorized_live_calendar.py)
- [472 項目前完整共享回歸](../artifacts/operations/agent-workflow/runs/full-idle-projection-controller--20261014T070006-1b05e557/run.log)
- [原 BASE 診斷被 source lease 正確拒絕的紀錄](../artifacts/operations/agent-workflow/runs/native-factorized-close-calendar-20261014T064453-39aa0deb/run.log)

## 07:33 全服務正常佔用再取樣

在六程序 benchmark 開始前，canonical 普查先完成 21.16 秒資源 interval。
87 個已安裝 StockAgent services、一個普查 transient、67 timers、3 paths
均留在清冊；CPU／memory／I/O 的數值與是否工作、失敗及資料完整性分開。
Discord 匿名記憶體 544,399,360 bytes（約 519 MiB）、CPU 0.0491 core；
公開 gateway 匿名記憶體約 679 MiB、CPU 0.0200 core，TAIFEX 展示無額外
高耗用判定。這不是程序 RSS、整卡 VRAM 或前後等負載的全機改善倍率。

較高 CPU 是正在工作的保證金 action（1.006 core）、partition return
（0.971）、TX history（0.879）和註冊 intraday 抓取（0.567）；不以停止它們
來改善取樣。FinMind complement 正常工作約 0.347 core，離線 covering index
仍因未有合格 drained idle window 而未正式載入。Windows、kernel file cache
與遠端工作另有邊界，WSL service 資源不能全部相加當作應用程式 RAM。

本次沒有 resource pressure finding；CPU PSI some avg10=1.8%、memory
some=0.02%、I/O some=1.99%，不是零延遲或全日無壓力的證明。清冊仍保留
OpenBB、Discord 正式歷史維護、crypto refresh、registered daily、cold publish、
release archive、Vast bulk return 的失敗，不把優化的健康／快取驗收套到它們。

- [所有目前可發現服務、主機與匿名／file cache 分拆](../artifacts/operations/running-services-optimization-20261013/census-after-native-gpu-window-20261014.json)
- [canonical 普查執行紀錄](../artifacts/operations/agent-workflow/runs/all-services-after-gpu-window-id-20261013T233234-596e059a/run.log)
- [79 個可見受管理 Linux 程序的 GPU FD／身分逐項取樣](../artifacts/operations/running-services-optimization-20261013/all-managed-gpu-handle-idle-20261014.json)
- [GPU FD 身分、unreadable／退出及多控制器 7 項測試](../artifacts/operations/agent-workflow/runs/managed-gpu-fd-inventory-portabl-20261013T234943-a21143c6/run.log)

### 15:30–17:00 完整來源試驗、原生索引採用與盤後正式工作

#### 143 欄全資料組裝：保留不採用的試驗

私有 immutable fixture 包含 172 個實際 Parquet 與補充來源，共 905,936,286
bytes。原版／streaming candidate 均保留完整 9,633,014 rows、143 features，
產物為 1,335,613,801 bytes，完整 SHA256 一致：
`5ddd5ebcead7bf2cd7b92f97eabf7ce2da1d596969860735bbd886488b807a4c`。

原版 wall=842.618／CPU=1,352.116 秒／peak RSS=41.021 GB；候選
803.389／1,407.836／41.675。只有單對照，wall 少 4.7%，但 CPU 增 4.1%、
RSS 增 1.6% 且 merge 變慢，**未採用**。不是刪欄、減 symbol／年數、移除
驗證或改低精度來追求數字。suffix-safe 94 項測試通過。

- [原版全資料試驗](../artifacts/operations/agent-workflow/runs/tw-public-full-assembly-original-20261014T072929-12355cdf/run.log)
- [候選全資料試驗](../artifacts/operations/agent-workflow/runs/tw-public-full-assembly-streamin-20261014T074435-65179e93/run.log)

#### OpenBB 全 manifest 32,768 檔工作量：同輸入與原生階段分開驗收

原生 unit 的 `--max-source-files 32768` 遇到 ordered partial covering index
入口的 8,192 guard，會繞過索引。僅將此第一入口調至 32,768；另一個按 endpoint
排序的 fallback 仍保留 8,192。沒有修改 API／配額、排序、schema、footer、
來源 rows 或 storage owner，也沒有新增 DB index／DDL。

私有 read-only online backup 為 19,731,976,192 bytes／13,297,719 tasks，
完整 DB SHA256 為 `4e6c967857ad96be18fc55cd258b200da40b14e4f4c294d52e876f8d6185152a`。
四個 fresh 程序按 A-B-B-A 執行完整 canonical source selector，包括所有選中
source footer。此時真實全部 manifest 無未分配來源，四輪 selected=0、完整
有序 dataclass 結果相同；非空批次另以 2／8192／8193／20000／32768、
無 index／無 plan／filter／32769 fallback／row mismatch 的語意測試驗證。

| 階段 | 原版兩輪平均 | 候選兩輪平均 | 變化 |
| --- | ---: | ---: | ---: |
| wall 秒 | 23.232 | 9.701 | -58.2% |
| CPU 秒 | 13.840 | 9.403 | -32.1% |
| peak RSS MB | 150.864 | 152.175 | +0.9% |

OS page cache 未控制；A1=34.358 秒、A2=12.106 秒，候選為 9.351／10.050。
不能把冷／暖差異當改法收益，也不能宣稱整個 compaction 快 58%。373 項
compaction／archive／columnar lake 共享測試通過；只有有界階段 ABBA 接受。

下一次正常排程 `f0b983295e4145f3a480ddb32c7bd363` 在 16:50:56 啟動，
16:55:34 exit 0，實際索引階段讀回 16.58 秒／CPU 9.282 秒，沒有 global fallback。
完整原生工作 276.533 秒，L0 7,327,309 files／334,949,985 rows 與 compacted
收據一致、185,963 active segments、pending/new/stale/failed=0，未刪 L0。
原生 metadata audit 仍占 226.045 秒；memory.high events=6,917、peak 約 2.75 GiB，
oom／oom_kill=0，仍是獨立下一熱點，不擅自提高配額或清 OS cache。
原生各輪 concurrent pressure、輸入與 cache 不同，276 秒不是受控全工作降幅。

- [完整 manifest ABBA 接受收據](../artifacts/benchmarks/running_services_20261013/openbb_native_unassigned_batch_20261014/abba-receipt.json)
- [373 項共享回歸](../artifacts/operations/agent-workflow/runs/openbb-32768-selector-full-seman-20261014T081434-2f1ade0c/run.log)
- [原生新 owner／indexed phase／來源收據讀回](../artifacts/operations/running-services-optimization-20261013/native-openbb32768-index-owner-phase-acceptance-20261014.json)

#### 今日五模式收盤 cache 與歷史工作不是同一件事

16:13 由 canonical `_run_completed_session_signal_cache_sync` 在 generate／CUDA
禁止條件下驗證 5／5 今日 `2026-10-14 13:30` 正式 summary、完整 weights、
current forward key 可重用，沒有 model forward／provider／交易寫入。
新五個鍵尚未各自配對新的獨立 full-output control，不能把昨日 key 的控制
直接換標當今天接受。16:17 真實 BASE 48 dates／2,760 symbols 與 projection
210 dates 到今日，0 missing、disk hit／0.432 秒；不是完整訓練 features 驗證。
16:04 原生 completed updater 實際 PL2／Arrow2、4 hits／2 builds／0 rejections，
735.442 秒；有新資料與不同主機壓力，不與 frozen warm ABBA 直接比較。

- [五個目前正式鍵的 zero-CUDA reuse](../artifacts/operations/running-services-optimization-20261013/native-five-current-close-zero-cuda-reuse-20261014.json)
- [真實 BASE calendar 與 pool 讀回](../artifacts/operations/running-services-optimization-20261013/native-factorized-current-close-calendar-and-pools-20261014.json)

#### 圖表別名、完成證明與逾時：先消除白做工

100m 的 `equity_curve.png` 與 `leverage_equity_curve.png` 實際是同 inode
hardlink，`shutil.copyfile` 會在計算／回報表完成後拋 `SameFileError`。
canonical `_copy_plot` 只在 `source.samefile(target)` 時重用，其他情況仍 checked
copy；不 unlink、不忽略缺失來源、不跳圖表。四種 absent／distinct／hardlink／
symlink 的真實 PNG 測試保留 full returns NPZ、metrics、weights、圖表與
fold completion；完整 131 項 reporting／lifecycle 回歸接受。
前兩次 fixture 失敗原 exit／log 保留並由成功收據 supersede，沒有刪除。

V8 同一 source digest
`22a6debbfac0fecc2579deaef81c1032436b81ed411571f58c1eda372ec46488`
兩次準備 3,116 rows／2,760 symbols／1,613 minute sessions。第二次前置 panel
485.242 秒，來源 1,100／1,613 已耗 1,199 秒，16:40:48 仍被原 1,800 秒上限
中止，下一輪重做。只將 V8 正式歷史期限調為 3,600 秒；啟用 120、收盤 600
秒、source/model/accounting/checkpoint 與 full requested scope 不變。
這是避免已知不可能的短 deadline，不是新的完整完成 ETA；現有 run 不被強制中止。

讀回發現「回報日期到了」不等於 workflow 完成：100m 與 multi_basis 新表
已到 10/14，但 matching complete proof 不成立。新增 cheap gate 使用 canonical
`TrainingArtifactLayout.required_fold_paths`、matching full-horizon／stitched
marker／fold／test_date_end、非空必要產物及未晚於完成 marker 的 returns revision。
不在 idle 反覆解碼整個模型／NPZ；此 scheduling hint 不替代完整 artifact validator。
舊 ready hint 在新 readback 失效時，CAS 變回可重試狀態並保存 prior-ready receipt；
不覆寫 concurrent running、既有 failure/backoff。相同 serial owner 立即處理
可修復項目並重查 priority gates，不另開 worker。

另有 factorized 完整歷史使用 2025-12-01 起 210 日 live projection，但完整
BASE 是 2014-01-06 起 3,116 日。訓練嚴格拒絕是正確的，不能改為縮短歷史或
補造成觀測。新 calendar preflight 在 child／GPU 啟動前先查真正 benchmark
日曆，證明缺少 2,906 sessions。16:51 全五模式診斷 0.389 秒，factorized
0.078 秒；child/provider/model/CUDA=0。只提前拒絕已證實錯誤 scope，未知日曆
與完整 member／universe／numerical／source 驗證仍由 canonical trainer 負責。
正確全歷史輸入及 V8 下一次完整完成尚未接受，不冒稱資料抓齊。

- [圖表別名完整 131 項回歸](../artifacts/operations/agent-workflow/runs/fold-report-same-file-full-lifec-20261014T082616-d9f6f32c/run.log)
- [五模式原生來源的 zero-CUDA 前置診斷](../artifacts/operations/running-services-optimization-20261013/native-formal-lifecycle-calendar-cuda-forbidden-20261014.json)
- [最終 618 項完成標記／controller／日曆／idle／forward cache 共享回歸，含 prior-ready 保存](../artifacts/operations/agent-workflow/runs/formal-idle-shared-regression-pr-20261014T091123-d8c882dc/run.log)

原生 physical-source 非阻塞 10 秒／49 Hz profile 得 434 samples／1 error；
duplicate-key `unique`、廣播後產品 `isin`、tick-grid 與 NPZ 寫入為後續候選。
樣本權重不是完整 CPU 歸因；尚未修改價量、價格格點、容量／stop 規則或
source-cache ABI。下次以所有 1,613 sessions／完整輸出對照再選演算法。

最新官方技術資料亦核對：PyTorch device `empty_cache` 只歸還不用的 device
allocation，不會代為歸還 pinned-host，也不會釋放還有 tensor/graph 引用的
記憶體；public `empty_host_cache` 用於另一 allocator。只在確定無推論工作後
回收一次，不能讓每次 idle 檢查初始化 CUDA／GC，或靠低精度／跳驗證降占用。
本機實際版本 Torch 2.13.0／Polars 1.38.1／NumPy 2.4.6／Arrow 25.0.0；
沒有為追新直接升級正式模型 runtime，仍以已安裝版本實測為準。

- [PyTorch device allocator 官方語意](https://docs.pytorch.org/docs/stable/notes/cuda#memory-management)
- [PyTorch pinned-host 回收官方 API](https://docs.pytorch.org/docs/stable/generated/torch.accelerator.memory.empty_host_cache.html)
- [NumPy 有界整數 bincount](https://numpy.org/doc/stable/reference/generated/numpy.bincount.html)
- [原生 physical source profile](../artifacts/operations/running-services-optimization-20261013/native-physical-source-active-speedscope-20261014.json)

### 17:02–18:22 原生盤後進度、真實來源換代與完整推論修復

本階段沿用原目標：逐項最佳化所有 running services，尤其閒時 CPU/RAM/GPU；
同一份有效輸入／模型／程式世代的收盤 forward 只計算一次，其後讀完整 CPU
cache。新的真實發布、部署或程式修訂必須另驗，不把舊驗收改名成新世代。
未重啟 Gateway／provider／交易服務、未呼叫 provider 或 broker、未改原始
資料、冷庫、策略／checkpoint 或計算精度，沒有新增代理或平行 collector。

17:02 的完整清冊仍是 87 installed services 加一個工程 transient，67 timers／
3 paths。58／58 managed Linux processes 可讀，只有舊 Discord PID 2503228
擁有兩個 `/dev/dxg` handles；13:38 啟動的 owner 尚未載入新的 idle cadence／
allocator 程式。整卡觀察 2% utilization、4.13–4.21 GiB used、41.7–49.3 W
不是全部由這個行程造成，也不是本次修改的因果降幅。OpenBB archive 的
9.38 GB cgroup memory 中 anon 約 152 MB、file 約 6.91 GB，不當成 Python
heap leak；其長時間全庫 audit 與 compaction metadata audit 仍是後續熱點。
16:50 的 native compactor 有 6,917 memory.high events，但 OOM／kill 為零；
不為加快 metadata audit 擅自放寬記憶體或刪 cache。

- [17:02 全服務／壓力／CPU 清冊](../artifacts/operations/running-services-optimization-20261013/native-all-services-postclose-final-census-20261014.json)
- [17:01 managed GPU FD 身分清冊](../artifacts/operations/running-services-optimization-20261013/native-all-managed-postclose-gpu-fds-final-20261014.json)

18:24 再取樣 70／70 managed processes，仍只有相同舊 Discord owner 的兩個
handles；probe 未初始化 CUDA／呼叫 provider／更動服務。全清冊仍為 87
installed 加一個 transient、67 timers／3 paths；21.29 秒資源取樣的 memory
PSI full avg10=1.48、I/O full=3.41，仍有真實資源競爭，不能把工程測試通過
描述為全部服務或遊戲卡頓已解決。

- [18:24 全服務最新資源清冊](../artifacts/operations/running-services-optimization-20261013/native-all-service-postclose-after-compile-repair-census-20261014.json)
- [18:24 最新 GPU owner／零 probe-CUDA 清冊](../artifacts/operations/running-services-optimization-20261013/native-all-managed-gpu-contexts-after-compile-repair-20261014.json)

17:11 的原生完整 owner `e697d617fbaa47fd85c8b0b751222841` 自行載入完成判定／
圖表修復：100m 在 17:21 ready、multi_basis 隨後完成，100m 的三份 leverage
plot timing 均為 `same_file_reused=true`，沒有重寫或刪除原圖。18:10 新的五模式
唯讀 preflight 0.373 秒，100m／multi_basis／multi_basis_22 有 10/14 matching
full-scope marker，factorized 為 10/02、V8 為 10/12；model/provider/child/CUDA=0。
這只證明排程用的 scope／lifecycle hint，不冒稱全部數值、來源或報表皆完整。
原生 owner 在 prior-ready preservation patch 載入前已啟動，該欄位的正式採用
尚未證實；最新 618 項測試已驗證後續新 owner 的保存行為。

V8 17:29–17:58 的正常完整工作已採用 3,600 秒預算，cold panel 491.906 秒，
完成全部 1,613 個分鐘 sessions；trainer handoff 在 1,703.359 秒才到 CUDA。
這次不再被 1,800 秒期限中斷，但 fullgraph 在 CandleEncoder 內呼叫
`torch.is_inference_mode_enabled()`，Dynamo 明確回報不支援；沒有完成 marker。
不能把 timer active、cache files 或完整來源重建當成正式推論成功。

新增五個 CPU fullgraph reproduction 全數重現原錯誤。修復使用 public
`torch.compiler.is_compiling()`，將 host-only ownership 檢查留在 eager：compiled
eval 仍走原始 out-of-place FP32 正規化，eager inference 繼續 owned clone
fast path。沒有關掉 compile、改 inference_mode、壓低精度、換模型或縮短歷史。
103 項回歸通過，包括實際 log-cash panel-slab caller 的 exact eager/compiled
輸出、輸入 bit preservation、grad 與 state_dict／checkpoint ABI。這是小型 CPU
trace 與工程回歸，不是 native full-fold CUDA／Inductor 的完成或速度驗收；
新檔案會自動進入 implementation fingerprint，舊 forward keys 不被重標。

- [18:10 五模式當前完成標記／完整日曆 preflight](../artifacts/operations/running-services-optimization-20261013/formal-history-current-completion-preflight-20261014T1010.json)
- [五個 fullgraph 原錯誤重現，保留失敗](../artifacts/operations/agent-workflow/runs/candle-fullgraph-native-eval-bug-20261014T100216-4287344f/run.log)
- [103 項完整模型／owned normalization／factorized inference／forward cache 回歸](../artifacts/operations/agent-workflow/runs/fullgraph-eval-model-normalizati-20261014T101002-bce40782/run.log)
- [PyTorch public compiler context API](https://docs.pytorch.org/docs/2.14/generated/torch.compiler.is_compiling.html)

盤後 finalizer 另有可避免的 read-only 成本：已 current 的資料仍拿排他鎖、
在同一 writer fence 內重做兩遍 dates／coverage／dependency hashes。改為共享
讀鎖和一次完整驗證；只有真的 stale／force 才升級排他鎖。lock conversion
可能讓其他 writer 先通過，因此升級後必須重新驗證。真正的重建、四個步驟、
bounded retries、失敗出口與完整 after validation 皆保留。新增 additive
`completed_session_read_then_write_v2` report contract／validation_count／lease_mode。
沒有以 file mtime、日期或舊 `after.current=true` 取代原驗證。

55 項初步 lease/cache/event tests 與最新 147 項 shared publication／source guard／
finalizer／benchmark refusal tests 通過（兩組重疊，不相加）。readers 可並行、
writers 仍被排除；另一 writer 在升級時完成、force、重試與失敗均已驗證。
完整真實資料的 fresh-process ABBA 已 capture 2,963 files，A1 7.656 秒／
CPU 8.019 秒、B1 4.785／4.980、B2 4.865／5.010；18:04 六份 TPEx 原始資料
真正換代，A2 在驗證開始前拒絕，四輪不完整。因此不宣稱平均降幅、完整 ABBA
通過、native adoption 或 stale-build 倍率；前三輪與失敗原 log 均保留。

換代涉及 QFII／industry／institution summary／margin adjustment／attention／
basic-company；stock/public feature summaries 的 recorded sizes 確實不同，
不只是 heartbeat。先前三次 current-cache probes 因真實 EX writer 被拒，
producer 是 canonical `stockagent-tw-public-source-events.service` PID 197。
已用後出的 36 項語意測試及綁定 criterion 收據記錄 expected denial；未把
失敗改為成功。接下來需檢視非 close-price source 更新到 derived finalizer 的
dependency dispatch／coalescing，不能跳過這六份新資料或拿 16:13 的 cache
冒充當前驗收。19:00 的既有 feature-reconcile 與 19:40 publication sweep 為
另有正常排程的路徑，不等於這次事件已及時完成。

- [共享鎖／換代拒絕／發布流程 147 項回歸](../artifacts/operations/agent-workflow/runs/completed-close-validation-depen-20261014T101751-94c98074/run.log)
- [原始 current-generation fixture，不重標或刪除](../artifacts/benchmarks/running_services_20261013/completed_close_full_validation_abba_20261014/fixture.json)
- [來源換代 A2 拒絕，無第四輪結果](../artifacts/operations/agent-workflow/runs/completed-close-full-validation--20261014T100309-743a5641/run.log)

### 18:42–19:20 非收盤端點相依事件恢復與原生採用

根因不在日期過期：舊事件服務只在兩個 close-price 端點觸發 finalizer，
且可拿先前 `after.current=true` 的收據當作完成。實際 TPEx／TAIFEX 原始
資料換代並被 downloader 確認後，沒有把非 close 更新送到四類衍生資料，
所以兩者同樣標成 10/14，input size／hash 卻不同。19:01 admission 為七份
落後，正常來源後續增加為九份；不把新資料漏掉或沿用舊完成狀態。

新增輕量共用 `DERIVED_INPUT_RECEIPTS`，事件路由與 canonical validator
使用相同四類 input keys。只有 content changed 且 accepted 的相關來源
排入 durable completed-session queue；無變動不讀 manifests，無關來源不
觸發全市場重建。缺失／畸形／超大 summary 或 writer-busy 時保守排入。
這個 routing hint 不替代原始日期、coverage、完整 SHA256 驗證。

重建仍由既有 serial finalizer 負責：盤後才執行，短來源批次最多 coalesce
60 秒、官方 close 優先；失敗保留已確認事件，5 秒起 bounded retry、最長
300 秒，不須等待另一個 provider 事件。owner startup 可恢復未完成衍生
工作，未強制重新下載已接受來源。fresh receipt／匹配日期／實際 current
都必須成立，舊成功收據不能讓失敗 child 通過。空閒時不每秒重寫相同狀態。

124／128 項逐步 routing/main tests、275 項共享 feature/calendar/cache 回歸、
152 項 native-owner/lease/routing tests 通過；彼此重疊，不相加。保留誤用
測試檔名與兩個 admission observer 錯誤的原始失敗，已由後出不可變成功
收據 supersede，沒有重寫成成功。

19:10 僅在 source owner 沒有 children／pending work、protected owners 不變、
當前程式指紋匹配的 admission 下重載一次來源事件服務。舊 PID 197 的
canonical Python mapping 已被環境更新刪除，admission 明確記錄舊／新 binary
SHA；新 owner 必須為目前 runtime inode。新 PID 3298917，InvocationID
`30637e10c7534ca7a207e96b6bd01504`，runtime contract
`completed_dependency_reconcile_v2`。Discord、simulation、兩個 Shioaji、
FinMind 及唯讀網站 owner 全部不變。

19:18 serial child 完成 stock panel 9,021,620 rows／2,760 symbols，public feature
panel 9,633,014 rows／143 features；各一步成功，沒有 provider redownload。
19:19 的獨立 fresh read-only verifier 再做 canonical dates／dependency checks：
四類 derived errors 全空、`current=true`，zero provider calls／CUDA initialized
false，並核對新 owner、fresh heartbeat、reviewed code 與 protected owners。
這才是實際採用證明；159／159 observed endpoint coverage 只是監測清冊，
不是所有歷史或九份來源外所有資料皆抓齊的宣稱。

原生 V8 的正常 18:59 retry owner 在 19:06 前取得 verified physical cache：
1,613 sessions、2,798,943 minute symbol-days、3,033,007 daily proxies，gap=0，
manifest full SHA256 accepted。warm prepare handoff 472.671 秒，並實際通過
compiled fullgraph validation/test chunks；19:13 已輸出 fold plots。此數字不是
冷啟動 ABBA 倍率；19:20 全歷史 stitching／正式完成 proof 尚未接受。

仍待處理：新程式／新來源世代五模式完整獨立模型控制、舊 Gateway 的安全
閒置採用、factorized 210 日與完整 3,116 日 scope 差異、coherent full-preparation
source lease，以及其餘服務熱點。另定位純 forward code identity 目前掃描全部
`stockagent/data/*.py`，可能讓未使用的資料處理改動觸發不必要 GPU 前推；
尚未縮小依賴或宣稱已修復，必須先證明 forward closure 與完整輸出一致。

- [275 項 source／feature／calendar／cache 回歸](../artifacts/operations/agent-workflow/runs/tw-source-dependent-features-cal-20261014T105330-1ac7b210/run.log)
- [152 項 owner／lease／routing 回歸](../artifacts/operations/agent-workflow/runs/tw-public-event-adoption-current-20261014T110122-cef37e48/run.log)
- [原生閒置 admission 與七份真實落後收據](../artifacts/operations/running-services-optimization-20261013/tw-public-event-idle-admission-bound-calendar-20261014.json)
- [唯一來源 owner 重載，交易／Discord 不重載](../artifacts/operations/agent-workflow/runs/tw-public-event-single-idle-owne-20261014T110951-332ae14c/run.json)
- [獨立原生來源採用與 actual current 驗收](../artifacts/operations/running-services-optimization-20261013/tw-public-event-current-derived-native-adoption-20261014.json)

### 19:24–20:03 完整驗證 ABBA、五模式獨立控制與盤後 GPU 閒置採用

先前不完整的四輪資料／失敗完全保留。重新取得當前 2,963 個真實 input
files，fixture v2 綁定完整 verifier dependencies、Python binary 與 NumPy／
Polars／Arrow／Torch runtime versions；任何 dependency／runtime／source
變動都拒絕，不把 v1 fixture 重標為新版。144 項 finalizer/lease/event/native
owner/benchmark 回歸通過。這組與 275／152 測試重疊，不相加。

四個 fresh processes 的完整 already-current finalizer ABBA 均接受，完整
semantic receipt SHA 相同，所有 dates／coverage／dependency hashes 保留：

| 完整 already-current 驗證 | 原 orchestration 平均 | 新 orchestration 平均 | 降幅 |
| --- | ---: | ---: | ---: |
| wall seconds | 6.7123 | 3.7832 | 43.6% |
| CPU seconds | 7.1217 | 4.0858 | 42.6% |
| peak RSS bytes | 416,647,168 | 383,832,064 | 7.9% |

此完整對照只測 current validation，不是 stale rebuild／模型推論／全機倍率；
OS page cache 未受控，未暫停下載器或來源。時間外的 admission 覆蓋了保守
package code closure；它不是未經驗證的新 inference cache dependency policy。

19:28–19:29 使用新的 private cache，五模式各作真正完整 GPU forward 並保存
獨立控制，不借用先前 GPU output／正式 source pointer 作新控制。每模式首輪
後釋放所有模型 device allocations；原 checkpoint、原精度／完整 auxiliaries、
symbols、weights、decisions、explanation 均保留。嚴格 CUDA preflight 通過。

隨後四次冷 reader 拒絕是實際 writer fence，不是 cache 算錯：前兩次為同期
宏觀公告工作，後續當沖資格／法人資料換代又產生 canonical finalizer。第四次
直接記錄 `.locks/tw-public-refresh.lock` inode 10185336、WRITE PID 3360992。
未繞過 source fence、未暫停 writer；原 failed receipts／logs 保留，分別以
綁定 criterion 與後出成功 run 記錄 `expected-rejection`。首次過早 admission
因完整 CPU controls 尚不存在而失敗，亦保留並由真實後出 admission supersede。

新版 serial source owner 自動接住 19:35 的當沖資格、之後上櫃／上市法人資料。
19:48:55 latest finalizer 成功，19:50:49 正常 postclose cache owner 五模式完成。
19:51–19:52 五個 fresh CPU processes 共 15 次完整回讀均與上述獨立 GPU 控制
SHA 完全相同，**CUDA 未初始化、zero model forwards／network／production
writes，且五個 pure forward keys 未變**。來源世代更新不等於重做 GPU math。
同時另由 canonical consumer 回讀正式 artifacts，證明實際 pointer 可重用；
不是只在 private benchmark cache 成功。

19:53 idle admission：fresh Gateway heartbeat／connected／ready，interactive
與 background running=0、暖機未啟動、五 mode signal／engine ACK lag=0，
actual current formal keys 與完整 controls 皆匹配；舊 PID 2503228 有兩個 dxg
handles。19:55 在動作前重新執行同樣 gate，並核對 owner／protected services／
code／signals／startup contract 不變後，僅重載 Discord 一次。

19:56 獨立 control 接受新 InvocationID `fefd567971904acf84346497030c4ad9`，
Python PID 3414411、wrapper PID 3414376，zero GPU handles，Gateway／engine
五 ACK 一致、signals／formal summary SHA／code 不變、無 warm-ready／fatal／
watchdog，command sync／Gateway connection 恢復。Simulation、兩個 Shioaji、
FinMind、期貨及公開唯讀網站 protected owners 不變；未操作訂單／ledger。
沒有新增排程 daemon，原一次性 source／cache／maintenance owners 繼續運作。

同期全 managed process probe 有 61／61 可讀、GPU owner=0／handles=0，probe
未初始化 CUDA。全服務清冊為 87 installed 加一個工程 transient，67 timers、
3 paths；21.241 秒取樣的 Discord CPU=0.01297 core，cgroup memory current
564,609,024 bytes、anon 529,743,872、file cache 30,457,856，high/OOM events=0，
block read=0、write=86,016 bytes。單 process RSS 含共享 mappings，不把 RSS、
cgroup charge、file cache、舊 peak 混為同一指標，也不宣稱全機降幅。
Memory PSI avg10 some/full=0；I/O full=4.72%，仍有其他真實 I/O 工作。

下一次盤前暖機與 09:00 latency 仍待其正式時點驗收。Windows／其他 Linux
非 managed processes／整卡 VRAM／遊戲 FPS 並未全部歸因，不能說全 GPU 零
用量或所有 87 服務都已最佳化。剩餘優先熱點包含 feature builder 在 source
更新時因 `base_contract_not_verified` 回到全歷史、過寬 forward code identity、
完整 prep coherent lease、OpenBB metadata audit、factorized full-history scope。
尚未縮小依賴、跳過歷史驗證或引入新的模型／precision／optimizer。

#### 主計總處公告服務的 TLS 與無效重試

19:30 既有 archive owner 因 stat.gov.tw 的缺失中繼鏈失敗；原先僅為附件
ws.dgbas.gov.tw 補另一條 TWCA chain，不能驗證目前主網站 leaf issuer。
核對官方 HTTPS DER 及 [TWCA SSL CA 資訊](https://www.twca.com.tw/upload/saveArea/filePage/20240327/9a2b62d266824935ac33759f90cd1e23/9a2b62d266824935ac33759f90cd1e23.pdf)
CYBER Root 節的 subject／serial／期限／SHA1，加入固定 SHA256 的中繼憑證，
按主網站與附件 hostname 選擇正確 chain。保留原 trust roots、hostname／TLS
驗證；不加 root／leaf、不修改 system trust、不 `verify=False`。一次 process
內 singleflight 共用 bundle，issuer HTTPS 亦須驗證、禁止 redirect、限制大小、
拒絕 fingerprint 變動。網站 401／403 直接報 access blocked，不再浪費五輪。

105 項 TLS/redirect/singleflight/pin/access-denial/archive/receipt 回歸通過；
19:45:41 真實 canonical `_fetch` 取得官方 listing，解析 20 rows，未改 raw
dataset／model／服務、無 CUDA。這證明 TLS transport 與列表 parser 恢復，
**不等於完整 archive／歷史已抓齊**，原失敗服務仍需下次 canonical 全工作
完成後才能宣告健康。一次跨 task registry 相容性錯誤未建立測試 run，沒有
手改或刪除舊收據；隨後正常 launcher 執行上述 105 項測試成功。

- [完整 v2 同來源／同 dependency／同 runtime ABBA](../artifacts/benchmarks/running_services_20261013/completed_close_bound_validation_abba_20261014/receipt.json)
- [144 項 lease／routing／owner／benchmark 回歸](../artifacts/operations/agent-workflow/runs/completed-validation-bound-closu-20261014T112325-b5c2202b/run.log)
- [五模式真正獨立 GPU 控制與釋放](../artifacts/operations/agent-workflow/runs/current-nine-revised-independent-20261014T112857-056cfb41/run.json)
- [五模式 15 次 fresh CPU 完整比對](../artifacts/operations/agent-workflow/runs/current-five-cpu-hit-after-insti-20261014T115126-8182639f/run.json)
- [原生正式 pointer zero-CUDA 回讀](../artifacts/operations/running-services-optimization-20261013/native-institution-reconciled-close-current-no-cuda-20261014.json)
- [當前閒置 admission](../artifacts/operations/running-services-optimization-20261013/discord-institution-reconciled-idle-admission-20261014.json)
- [唯一 Discord owner 重載，具動作前 gate](../artifacts/operations/agent-workflow/runs/discord-native-idle-guarded-sing-20261014T115454-2a5cdd52/run.json)
- [獨立原生 GPU idle adoption](../artifacts/operations/running-services-optimization-20261013/discord-current-closed-gpu-idle-native-adoption-20261014.json)
- [61／61 managed processes GPU handles 清冊](../artifacts/operations/running-services-optimization-20261013/native-all-managed-gpu-after-closed-idle-adoption-20261014.json)
- [全服務／CPU／cgroup／壓力最新清冊](../artifacts/operations/running-services-optimization-20261013/native-all-service-after-gateway-closed-idle-adoption-census-20261014.json)
- [V8 matching full completion scheduling hint](../artifacts/operations/running-services-optimization-20261013/native-formal-history-after-v8-completion-20261014.json)
- [DGBAS 真實 verified TLS／listing readback](../artifacts/operations/running-services-optimization-20261013/dgbas-pinned-main-chain-live-diagnostic-20261014.json)
- [105 項 TLS／archive 回歸](../artifacts/operations/agent-workflow/runs/dgbas-pinned-chain-archive-tests-20261014T114843-89be8162/run.log)

### 20:15 正常維護後的盤後快取與 GPU 追蹤

20:00 正常維護產生新的正式 summary，並非沿用上一筆 summary 的顯示狀態。
20:15:12 canonical 回讀五模式，皆為 2026-10-14 收盤、同一個純 forward
cache key，與 19:30 的獨立 GPU 控制完整輸出相符；model forward=0、
CUDA initialized=false、provider calls=0，沒有 trading writes。
Source identity／summary 路徑可以更新，不代表模型輸入變動或必須重算。

20:15:05 的另一個原生 probe 在 74／74 可讀 managed Linux processes 中，
GPU owners=0、device handles=0，probe 本身未初始化 CUDA，未重載服務。
這延伸了 19:56 的接受證據，確認至少這次正常維護沒有重新占用 GPU；
不是 Windows 全卡 VRAM 歸因、整晚持續取樣或下一次盤前暖機的驗收。

- [正常維護後五模式 canonical 快取完整回讀](../artifacts/operations/running-services-optimization-20261013/native-closed-cache-after-normal-2000-maintenance-20261014.json)
- [74／74 managed processes GPU handles 清冊](../artifacts/operations/running-services-optimization-20261013/native-all-managed-gpu-after-normal-2000-maintenance-20261014.json)
- [快取回讀 run](../artifacts/operations/agent-workflow/runs/native-closed-cache-after-normal-20261014T121502-d14ef535/run.json)
- [原生裝置清冊 run](../artifacts/operations/agent-workflow/runs/native-all-managed-gpu-after-nor-20261014T121502-a6de7ed2/run.json)

### 21:04 全量 OpenBB footer 對照：不採用未證實的 mmap 加速

新測試不是縮小歷史或另開處理管線，而是呼叫 canonical
`_mark_stale_segments`：全部 185,963 個成功 L1 segments 都逐檔開啟、驗證
筆數與 Arrow 指紋，SEC 的 50,722 份完整 view proofs 另包含原始 bytes、
前後 file signatures 與 physical identity。只複製 private manifest 112 MiB，
沒有複製／刪除 L0 或 payload，也沒有 provider calls／model runtime。
fixture 綁定七個實作檔、Python binary／runtime、Arrow／SQLite、完整 manifest
與全部來源 signatures；signatures 是 race fence，不是 payload bitrot proof。
原七個實作檔完整保存於 fixture 的 `code-closure`，逐份 SHA 與 capture 相符。

每個 trial 為 fresh process，維持原生 compactor 的 MemoryHigh 2816 MiB、
MemoryMax 3 GiB、CPUWeight／IOWeight 10，nice 19；未提高正式服務資源上限。
四輪 canonical 完整輸出 SHA 均為
`0dd42c46b760617764c5c9711b75e0762529074dfa9aca5e6fe38b3c42770619`：

| 完整 derivative audit | wall 秒 | CPU 秒 | peak RSS bytes | process physical read bytes |
| --- | ---: | ---: | ---: | ---: |
| 原版 A1 | 186.4953 | 94.2759 | 219,394,048 | 3,593,170,944 |
| mmap B1 | 146.1762 | 86.9367 | 211,001,344 | 3,657,109,504 |
| mmap B2 | 66.5880 | 66.5502 | 213,467,136 | 0 |
| 原版 A2 | 66.1296 | 63.2985 | 222,261,248 | 34,033,664 |

OS page cache 未受控、host 非 exclusive。混合冷暖 ABBA 的 wall 平均降幅
15.8% 不能單獨歸因於 mmap：暖快取原版 A2 比 mmap B2 還快，CPU 也較少。
**mmap 未採用**。原生 20:50 owner 正常完成 133.6966 秒，metadata audit
84.45 秒；與 20:16 的 152.406 秒不同，程式仍為原版，不當成新技術收益。
20:50 InvocationID 為 `dc61dfc9067b4994b65c8bb6ce64dbe7`；沒有重載服務或
干預 owner。B2 等該原生 owner 結束後才開始，未刻意清空 OS cache。

這證明 all-success derivative metadata audit 的一致性，不是整段 compaction、
source membership SQL、payload value audit、全 provider 歷史完整性或全機倍率。
下一候選針對物理 schema 的重複 reflection：50,722 個 SEC 分區只有 12 個
manifest Arrow schema fingerprints。新的 attempt-local cache 仍每檔計算
當前 Arrow 指紋；物理結果命中要求完整 tree、exact KV、version／revision
及 native descriptor equality，容量有界，未知格式 opt out、MemoryError
原樣中止。72 項負向／完整 proof 回歸通過；在全量對照接受前不接正式 owner。

- [185,963 個完整 footer 原版／mmap 四輪對照](../artifacts/benchmarks/running_services_20261013/openbb_footer_all_20261014/abba-receipt.json)
- [來源／manifest／完整實作綁定](../artifacts/benchmarks/running_services_20261013/openbb_footer_all_20261014/fixture.json)
- [原七檔完整保存與 SHA 核對](../artifacts/operations/agent-workflow/runs/openbb-footer-freeze-v1-code-clo-20261014T130554-89377291/run.json)
- [正常原生 20:50 owner 完成的獨立回讀](../artifacts/operations/agent-workflow/runs/openbb-footer-native-original-20-20261014T125421-5109413a/run.log)
- [72 項 schema／source fence／完整結果回歸](../artifacts/operations/agent-workflow/runs/openbb-physical-cache-negative-t-20261014T130914-2b62be49/run.log)

### 21:19–21:48 有界物理 schema 快取與原生採用

不是把檔案驗證 cache 成「仍有效」，而是重用已完整驗證的物理 reflection。
每檔仍開啟自己的 Parquet footer、計算自己的當前 Arrow 指紋並驗證筆數，
SEC 仍驗證 bytes 及前後 signatures。命中鍵包含 version／revision、Arrow
digest、完整且已驗證的群組 tree、exact metadata KV，另須 native descriptor
equality；INT96、nested group field IDs、Geo metadata、欄位順序、不同 KV
bytes 均不可混用。未知表示 opt out，必須的 Arrow 錯誤與 MemoryError 仍中止。

只在單次 canonical derivative audit 持有 cache；至多 64 entries、serialized
留存 budget 2 MiB。Native schema 會保留其 FileMetaData parent，因此同時
限 entries 與 serialized weight，不將後者假稱 decoded RAM 上限。完整實測
RSS 未增加。物理 identity revision、完整 digest、資料與模型 ABI 未改變。
初版全量原 code closure 與候選 v2／正式 v3b closure 都保存，source manifest
SHA `afbea9fd442315f389b683d50d503a6e79497254d601c138a6e403891944ea88`
及全部來源 signatures 相同，不把新版 capture 偽裝成舊版。

| 全部 185,963 檔 canonical audit | wall 秒 | CPU 秒 | peak RSS bytes |
| --- | ---: | ---: | ---: |
| 原版 A1 | 100.8308 | 91.3842 | 221,519,872 |
| 有界 cache B1 | 64.3190 | 63.8722 | 207,060,992 |
| 有界 cache B2 | 43.0218 | 42.9570 | 214,536,192 |
| 原版 A2 | 60.0673 | 59.9234 | 220,155,904 |

完整 audit output SHA 仍為 `0dd42c46b760617764c5c9711b75e0762529074dfa9aca5e6fe38b3c42770619`。
ABBA 平均 wall／CPU／RSS 降幅為 33.29%／29.40%／4.55%；OS cache／host
仍未受控，暖 B2／A2 比較另有 wall／CPU 約 28.38%／28.31% 降幅。兩個
cache trials 都為 hits 50,710、misses 12、entries 12、serialized retention
66,683 bytes，bypass／eviction 為零。這是完整 metadata 階段對照，非整段
compaction、API、payload bitrot／value audit 或全機倍率。

共用回歸第一輪 69 failed／518 passed：量測參數誤傳給 query publisher，
未變更資料比對結果。修正 call site 後，同一 13-file selection 為 587 passed；
舊 failure／log 保留，以新成功 run supersede，未改寫成成功。另 11 項新的
native observer 負向 tests 通過：running／舊 Invocation、過期 publication、
部分 coverage、未完成 controls、超量 cache、漏 proof、counts mismatch、
pending 不可替代 acceptance。未執行維護的 repository 全測試。

21:22:48 timer 正常啟動 InvocationID `3f07728099de4f8ebf68fa9363a9741c`，
沒有重啟／停止／訊號／資源上限調整。21:43:47 完整成功：1,258.0015 秒，
query/status published、new/stale/failed/pending 為零，7,327,309 個來源與
334,949,985 rows 的 source／compacted counts 一致，185,963 個 footer
實際開啟。原生新 cache hits／misses 為 50,710／12，12 entries、66,789
serialized bytes；首個代表 footer 與 private trial 不同，因此 bytes 略異，
沒有將代表檔筆數或有效性重用到其他檔案。

該次是 `full_audit_due`：source fingerprint 仍為
`14a1881e6553f890692d81741f7c046bbc4f23c6056370da0b1cb2a59a562088`，
last full 為 10/13 21:08，24 小時稽核到期；不是新 cache 造成 source contract
變更。1154.321 秒停在完整 source SQL，62.047 秒為 derivative metadata，
其餘含索引搜尋 9.677 秒、view publication 12.778 秒、status 18.145 秒。
不暫停的 py-spy sample 正好在 SQL fetch，當時約 8.95 GB physical read；
21:38 仍在同一次 source SQL、約 19.72 GB physical read。每日 SQL 才是
這次最大磁碟瓶頸，不能拿前一次 incremental 工作的 134 秒作同條件基準。

21:46 獨立 read-only observer 接受 exact terminal native Invocation、完整
source generation、cache policy、attempt/publication 全 counts 與 current
code/runtime；不是只讀 systemd active。隨後正式 v3b 的 fresh-process
完整 canonical trial 又逐檔計數 185,963 次 open，50,722 份完整 proofs 與
上述原始 controls SHA 相同。該回讀 wall 87.017／CPU 83.128 秒，保留此
快取／host 狀態差異，不把較早的 43 秒宣稱為所有正式工作保證。

21:27 read-only GPU probe 55／55 observed、owner／handles 為零。21:39 的
新五模式 CPU 回讀被 `tw_day_trade_v8_annual_log_cash` 的
`completed_source_generation_changed` 拒絕，沒有繞過保護或作 GPU forward；
原 failed run 保留。此前 20:15 五模式成功仍是當時證明，不能將其重標為
現在都有效。接續需拆查 V8 source／resolved config／code 身分與原生 retry
／maintenance 的連動，再確認最新世代；不關閉資料追新或強行接受舊 artifact。

21:57:25 的 final read-only GPU sample 更新為 63／63 observed，GPU
owners／handles 仍為零，probe 無 CUDA initialization／service action。這是
Linux managed-process 當次取樣，非整卡 VRAM 或 Windows／遊戲 FPS 證明。

- [全來源 metadata 有界 cache 四輪對照](../artifacts/benchmarks/running_services_20261013/openbb_physical_cache_all_20261014/abba-receipt.json)
- [587 項修正後共享回歸](../artifacts/operations/agent-workflow/runs/openbb-physical-cache-shared-reg-20261014T132227-7683565d/run.log)
- [保留的首次 telemetry 整合失敗](../artifacts/operations/agent-workflow/runs/openbb-physical-cache-shared-pro-20261014T132057-04258913/run.json)
- [11 項原生 observer 負向 tests](../artifacts/operations/agent-workflow/runs/openbb-native-metadata-observer--20261014T132924-4ec94c70/run.log)
- [不暫停的 full source SQL 堆疊證據](../artifacts/operations/agent-workflow/runs/native-openbb-cache-nonblocking--20261014T133014-787ae7ab/run.log)
- [完整原生採用、source counts 與 publication 驗收](../artifacts/operations/running-services-optimization-20261013/native-openbb-physical-cache-actual-owner-acceptance-20261014.json)
- [正式 v3b 的完整實際 footer-open 控制](../artifacts/benchmarks/running_services_20261013/openbb_physical_cache_adopted_v3b_20261014/current-all-file-actual-open-count-control.json)
- [最新 55／55 managed GPU 裝置 handle 清冊](../artifacts/operations/running-services-optimization-20261013/native-all-managed-gpu-after-openbb-cache-adoption-20261014.json)
- [21:57 最新 63／63 managed GPU 裝置取樣](../artifacts/operations/running-services-optimization-20261013/native-gpu-final-openbb-milestone-sample-20261014.json)
- [V8 更新世代的正確拒用紀錄](../artifacts/operations/agent-workflow/runs/native-five-closed-cache-during--20261014T133954-5dd3afed/run.log)

### 22:08 起：來源換代恢復、完整 SQL 候選與新全服務清冊

#### 五模式收盤快取：真實來源修訂不是錯誤的 cache hit

22:08 的 no-model 驗證仍拒用 multi_basis_22 的
`completed_source_generation_changed`。不能為了令測試成功而換用舊
資料／取消身份檢查。逐項讀取原 full-forward receipt 後，確認 20:06
與後續 100m／V8 鍵之間，只有完整 `input.sha256` 改變；resolved
config、checkpoint、code、mask、device／AMP／runtime 字段皆未改。
22:26 當前診斷亦確認全部五模式的 resolved config／implementation 相符，
canonical dependencies 最近的變動是 live feature Parquet 22:14:23
及其 summary 22:14:43，非新 GPU owner 或取消來源追新。

`verify_completed_session_cache.py --diagnose` 現在可保存每個拒用模式的
composite identity 與 canonical calculator 的完整 preimage／members。
member set 由原 helper 產生，不複製一份列舉規則；它是 stat generation
的定位證據，不是 payload hash／資料接受的替代證明。過期模式仍拒用，
結果 `state=rejected`、exit 1；不對其呼叫 signal builder。有效模式會
實際走原 consumer，generate、CUDA 與 requests provider 呼叫均禁止。
oversized／異版本 receipt、path-traversal key、非相同 code 和 stale mode
均有 focused tests；沒有改 forward-cache ABI、precision 或模型。

22:26 五模式正式 consumer 為 5／5 accepted，原 Gateway invocation
`fefd567971904acf84346497030c4ad9` 未改；原一分鐘 callback 已自行恢復。
100m／multi_basis／V8 的鍵更新，factorized／22 沿用既有 pure-forward
鍵。這次證明的是當前 close artifact 可重用，**不是**將 19:51 的獨立
full-output controls 配到改過輸入的鍵，亦不是五模式全部正式歷史已補完。
新增診斷及 retry／signal helpers 回歸 105 passed；原失敗記錄完整保留。

22:29:21 GPU probe 54／54 Linux managed processes 可讀，owners／device
handles=0，probe 未初始化 CUDA。22:31 的完整普查共 89 services，必須
拆為 **87 installed＋2 engineering transient**；另有 67 timers／3 paths。
此時 private index builder 仍在運作，不拿工程負載當日常 baseline。
20 秒取樣的 FinLab／FinMind／TAIFEX dashboard CPU 分別約
0.731／0.320／0.289 cores；實際下載及期貨夜盤工作沒有停掉。
來源完備性、公開頁健康、GPU 閒置與 CPU 最優是分開的驗收。

- [保留的 22:08 來源換代拒用](../artifacts/operations/agent-workflow/runs/native-closed-cache-current-2209-20261014T140843-3c2ecd7b/run.log)
- [當前五模式 canonical 身分／完整 dependency diagnostics](../artifacts/operations/running-services-optimization-20261013/native-closed-cache-all-identity-diagnostic-20261014.json)
- [105 項 cache／retry／signal 回歸](../artifacts/operations/agent-workflow/runs/completed-cache-diagnostic-seman-20261014T142546-00bec06e/run.log)
- [22:29 全 managed GPU 取樣](../artifacts/operations/running-services-optimization-20261013/native-all-gpu-after-source-recache-20261014.json)
- [22:31 全服務／host pressure 清冊](../artifacts/operations/running-services-optimization-20261013/census-after-source-recache-20261014.json)

#### 不重掃完整 SQL，但還有未解 I/O 成本

下一次正常 OpenBB owner `dd1382af90df476b89bbc18324e93d6b` 於
22:14:14–22:19:50 自然完成：journal `incremental/journal_verified`、changed
task IDs=0、source SQL=0，source files／compacted files 同為 7,327,309，
rows 同為 334,949,985。全部 185,963 footer 實際開啟，reflection cache
50,710 hits／12 misses、12 entries、66,789 serialized bytes。
pending／new／stale／failed／deferred failed=0；已和相同固定完整控制驗收。
這次 footer wall 209.642 秒、source selector 87.265 秒、whole 336.207 秒；
host／page cache 條件不同，不把前輪 62 秒當永久表現。沒有再 force full
或以略過 row／schema／metadata validation 來掩蓋固定成本。

每日 full SQL 的原生 query plan 是 member scan → segment PK lookup →
task PK lookup，task lookup 仍需取完整 task record。候選只增加涵蓋七個
來源契約字段的 index，全部 CASE、NULL、path-normalizer、DISTINCT、
ordering 及源範圍均使用原查詢；SQLite covering index 可以免除第二次
table-row lookup，但是否值得需測量。依據
[SQLite 官方 planner 說明](https://www.sqlite.org/optoverview.html#covering_indexes)
及[官方 EXPLAIN QUERY PLAN](https://www.sqlite.org/eqp.html)，這不是看到
新技術就替換資料引擎或放寬既有來源／journal 契約。

`benchmark_openbb_l1_source_contract.py` 只在新 private online backup 建
`bench_l1_source_task_contract`。完整 snapshot 含 13,297,719 tasks、
7,327,309 members、185,971 total segments（包括非 active），不是抽樣。
native-style engineering memory.high=2816 MiB／max=3 GiB、Nice15、
CPUWeight／IOWeight10；只限制本工程 transient，不修改原 owner。
執行前預留 backup＋index／sort 空間及 30 GiB physical reserve。
原版／候選均用 native 相同 2,000 KiB SQLite page cache，完整 SQL
結果逐 row hash 後比較；fresh code／binary／Arrow／SQLite／完整 DB
SHA 前後核對，另記錄建 index／backup CPU、wall、增量空間和 whole
engineering workflow 成本。SOURCE SQL ABI／正式 DB schema 均未改。

本候選 123 項 source-contract／journal／compaction 回歸已通過，包含
全部 stale reason、orphan／retired／NULL、異 endpoint、path fallback、
同 segment 多種 DISTINCT reason、readonly snapshot、變動契約與
不一致 ABBA 的拒用。私有四輪測量尚未全部完成，亦未測正式
writer 維護成本，**不能宣稱候選已加速或部署**。

22:37 private capture 已完成：SQLite 為 23,536,005,120 bytes，完整
SHA256 `072f2785d7f8572e20d2d637c19c3438ce3c14074be83ab533aa60a088d8748e`。
online backup 181.351 秒；index build wall 479.748／CPU 216.384 秒，
增量 3,804,028,928 bytes（3.54 GiB）。包含雙完整 SHA、code freeze 等
完整 capture wall 1,026.530／CPU 488.343 秒；原來源沒有寫入或刪除。
22:43 原 canonical engineering owner 已開始四個 fresh workers 的
original／covering／covering／original，先記錄原版 query plan。
它們共用同一 immutable full snapshot，維持 3 GiB hard cap；尚無
完整四輪結果，更沒有在正式 DB 安裝 index。當前約 144 GiB physical
free 是建庫後的容量觀測，不當長期容量保證。

- [22:14 原生增量、全 footer 與 publication 驗收](../artifacts/operations/running-services-optimization-20261013/native-openbb-incremental-after-source-full-20261014.json)
- [123 項來源／索引語意回歸](../artifacts/operations/agent-workflow/runs/openbb-full-source-covering-sema-20261014T141943-b91af06f/run.log)
- [保留的完整 private capture／index-build 操作](../artifacts/operations/agent-workflow/runs/openbb-full-source-index-private-20261014T142042-e2f623fd/run.json)
- [完整 capture／成本／固定 code 與 runtime](../artifacts/benchmarks/running_services_20261013/openbb_full_source_contract_index_20261014/fixture.json)
- [正在進行的完整 SQL 四程序交錯對照](../artifacts/operations/agent-workflow/runs/openbb-full-source-index-abba-20261014T144308-02f706fb/run.json)

### 23:03–23:52 程式世代准入、正常回補與原生冷 owner

#### 啟動版本不能被磁碟上的新版替換

發現既有 `implementation_identity()` 會讀目前磁碟上的 source，但長駐
Python 可能仍使用舊的 config class／model functions。直接用新版 source
hash 標記舊模組的結果不是可接受的 cache identity。
[Python 官方 module-cache 說明](https://docs.python.org/3/reference/import.html#the-module-cache)
確認 `sys.modules` 先於再次載入；本修復不以局部 `reload` 拼接新舊物件。

canonical cache helper 新增 `loaded_implementation_identity()`：Discord 角色
在匯入 config／data／model 之前綁定一份不可被 caller 改寫的版本，engine
匯入前後再檢查一次。後續新增／刪除／替換檔案需 fresh process，不能重試
後把新版磁碟 hash 當成舊 resident model 的版本。completed identity、一般
signal 入口、model cache hit／miss 都經同一檢查；compute-before-commit 的
原有 hash／checkpoint 驗證未移除。數值、模型／checkpoint、source、mask、
price／execution 與 forward-cache v3 內容契約不變；source code 換版本來就
會產生新 key，舊來源與舊 cache／收據均保留。

200 次完整 121 檔 CPU 版本檢查的交錯小型實測：原磁碟 identity 平均
9.705 ms／call，新准入檢查 14.970 ms／call。這是正確性成本，不是加速
成果，也不是完整 signal latency。heartbeat 只輸出啟動時固定的 SHA，
不每秒重新掃程式。新增／刪除／hash／generation 變化、caller mutation、
resident hit／miss 及 source 前置拒絕等負向檢查通過；共用 cache／idle
回歸 192 passed，後續 source-admission 回歸 154 passed，最後 runtime／
暖機／Discord 相依測試 251 passed，三組重疊，不相加稱唯一測試數。

#### 更新鎖不是快取成功，也不以重啟跳過

23:03 observer 確實被 canonical writer FLOCK 擋住。新版 `--diagnose`
會保留逐 mode 的 admission details，仍 exit nonzero；一般驗收維持 fail
closed，未知 validation／CUDA／provider 錯誤不被吞掉。23:16 原正常
postclose owner 已載入准入 helper，23:18 讀回為 3／5，另兩 mode 仍是
程式世代未追上；23:20 自動回補後 5／5 當前 config／code 身分一致，
observer 禁止 model／CUDA／provider，無 trading writes。

23:34 正式 reload admission 再被真實 source update 擋住，沒有操作服務。
23:39 對原 source-events owner 做一次短 stack dump：主執行緒正在等待
canonical finalizer child，而不是空轉。後續鎖／子程序觀察顯示 finalizer
確有處理工作；單次 stack 不當成 deadlock 或整段效能證明。

23:45 idle admission 通過，23:48 操作前在 source shared lease 下再次
核對原 owner、protected owners、程式、五個摘要與快取鍵及準備窗口不變，
**只重載 Discord 一次**。新 InvocationID
`45294dc1bc2840f781db6630ca30b9bf`，wrapper PID 3973740、Python
PID 3973783；獨立 control 接受啟動准入 SHA、五模式現存快取、Gateway／
command sync 與 engine ACK。summary SHA／pure keys 不變、GPU handles=0、
無新 warm-ready／watchdog／fatal；六個行情／simulation／FinMind／網站
protected owners 未變。沒有新的 broker actions 或推論 daemon。

`--code-admission-only` 有獨立的 `discord_native_loaded_code_admission_v1`
契約，明確 `full_numerical_oracle_proven=false`。原 GPU numerical 模式
仍必須提供兩份獨立 benchmark 路徑，不能拿新模式冒充 GPU 數值驗收，
也不能跨契約使用 control。下一個真實暖機／開盤 latency 與 factorized
完整歷史缺口仍未完成。

23:52 全服務取樣包含 **87 installed＋3 engineering transients**，67
timers／3 paths。Discord 在 21.677 秒內平均 CPU=0.013498 core，cgroup
memory current=562,774,016 bytes、anon=527,753,216、file cache=30,617,600，
block read／memory high／OOM events=0。另獨立 73／73 managed-process
取樣均可讀、GPU owners／handles=0；不是 Windows／整卡的零顯存宣告。
同時仍有真實 FinLab／FinMind／cold publish／return／夜盤工作，不能把
工程 SQL 對照期間當成獨佔、零工作的整機 idle baseline。

#### 完整 SQL 三輪已完成，四輪及寫入成本仍待驗

固定 13,297,719 tasks／7,327,309 members／185,971 total segments 的
23,536,005,120-byte private DB 及八個固定 helper 的 SHA 均未改。第一個
原版 SQL=2,285.569 秒／CPU436.427 秒；兩個 covering-index SQL 分別
188.210／198.781 秒，CPU58.121／64.512 秒。含完整 DB SHA 等工程
證明的各完整 worker 為 2,365.388／256.577／269.579 秒，不省略該成本。

三輪實際 stale_rows=0、輸出 digest 一致；非空 stale／NULL／孤兒／路徑
情形另由先前 123 項語意回歸覆蓋。最後一個原版仍在 exact run
`openbb-full-source-index-abba-20261014T144308-02f706fb` 運作，**尚未接受
ABBA aggregate 或原生新索引**。23:24 cumulative observer 約 99.16 GB
block read／1,154 萬 read I/O／110,351 memory-high events，沒有 max／OOM；
包括先前 proof hashing／工程工作，不冒充單一 SQL 的純 I/O 指標。
沒有提高 3 GiB cap、改動 frozen helpers 或停止原測試；需先完成四輪，
再測 full-size private writer/update/index maintenance，才考慮正式 DDL。

本階段證據：

- [192 項 code／cache／idle 回歸](../artifacts/operations/agent-workflow/runs/live-code-admission-semantic-20261014T151142-df535afc/run.log)、[154 項 source 准入回歸](../artifacts/operations/agent-workflow/runs/live-code-and-source-admission-r-20261014T151646-40761b01/run.log)、[251 項 runtime 相依回歸](../artifacts/operations/agent-workflow/runs/discord-code-adoption-contracts-20261014T153220-104a15a0/run.log)
- [CPU 准入成本](../artifacts/operations/running-services-optimization-20261013/live-code-admission-cost-20261014.json)、[3／5 過渡世代](../artifacts/operations/running-services-optimization-20261013/native-cache-current-admission-diagnostic-20261014.json)、[5／5 自動恢復](../artifacts/operations/running-services-optimization-20261013/native-cache-admission-after-recache-20261014.json)
- [source owner 短 stack](../artifacts/operations/agent-workflow/runs/tw-public-source-owner-stack-20261014T153940-26a784a7/run.log)、[真正 reload admission](../artifacts/operations/running-services-optimization-20261013/discord-code-admission-current-idle-baseline-20261014.json)、[合格動作紀錄](../artifacts/operations/running-services-optimization-20261013/discord-code-admission-qualified-reload-20261014.json)、[獨立原生 code control](../artifacts/operations/running-services-optimization-20261013/discord-code-admission-native-control-20261014.json)
- [最新 73 程序 GPU 取樣](../artifacts/operations/running-services-optimization-20261013/managed-gpu-after-native-code-adoption-20261014.json)、[完整 87 installed 清冊](../artifacts/operations/running-services-optimization-20261013/census-after-native-code-adoption-20261014.json)
- [原版 A1](../artifacts/benchmarks/running_services_20261013/openbb_full_source_contract_index_20261014/01-original.json)、[候選 B1](../artifacts/benchmarks/running_services_20261013/openbb_full_source_contract_index_20261014/02-covering.json)、[候選 B2](../artifacts/benchmarks/running_services_20261013/openbb_full_source_contract_index_20261014/03-covering.json)

### 10/15 00:13 完整 SQL 對照完成，接續原生寫入成本

原 exact run `openbb-full-source-index-abba-20261014T144308-02f706fb`
已 terminal succeeded／exit 0；最後原版 A2 SQL=2,328.331 秒、
CPU481.535 秒、含工程驗證 worker=2,416.518 秒。四輪 A/B/B/A 均在
同一 23,536,005,120-byte DB／13,297,719 tasks／7,327,309 members／
185,971 segments、原生 2,000 KiB SQLite cache／3 GiB cap 下完成；
八檔 helper／runtime／完整 snapshot SHA 不變。

平均 SQL wall=2,306.950→193.496 秒（少91.612%），CPU=458.981→
61.316 秒（少86.641%）；每個完整 worker，包括 DB 全檔 SHA 證明，
wall=2,390.953→263.078 秒（少88.997%）。沒有把 SQL 倍率套到完整
compaction、下載 API 或全主機。實際四輪 stale_rows 都是0，完整輸出
digest 一致；非空／NULL／孤兒／路徑語意另有負向回歸。主機非獨占、
OS page cache 未清除，建索引479.748秒與增加3,804,028,928 bytes仍計入
技術取捨，**尚未正式安裝新索引**。

00:19另外檢查同一私人快照的**未強制原始查詢**：`EXPLAIN QUERY PLAN`
仍選`sqlite_autoindex_tasks_1`，不選現存covering index。四輪候選的加速
來自已記錄的`INDEXED BY`選擇；只安裝實體索引不足以宣稱原生會加速。
後續接入必須先驗證名稱／完整column／collation／partial-layout，讓當前
canonical source query使用同一已量測lookup，缺失則沿用原正確路徑；
SQL predicates／NULL／DISTINCT／排序與source mutation journal仍不變。
這次只是plan診斷，沒有重跑完整查詢，也沒有正式DB DDL或服務操作。

新增 `benchmark_openbb_l1_source_write_cost.py` 不複製 downloader SQL：
從相同完整快照的 L1 member rowid 分層取1,024份真實成功 task，私人
副本上合成 pending→running→success 生命週期；直接呼叫既有
`Manifest.claim`、`mark_executing`、`completion_batch`／`complete`。
保留原256筆批次上限、WAL／synchronous FULL／2,000 KiB page cache、
全部 source mutation triggers。每筆三個 journal entry、完整 task 欄位與
attempts、commit 後獨立 connection readback 都核對；checkpoint 成本
不被省略，payload quarantine／模型／provider 不允許進入。
[SQLite 官方 WAL 說明](https://www.sqlite.org/wal.html)
也區分 commit 與 checkpoint；測試不降低 durability 以取得較漂亮數字。

兩個完整副本保留 original／covering 的實體差異，private original 只
drop該私人索引、不做VACUUM；其freelist／page count另記錄，不能冒充
正式無索引庫的完全相同物理尺寸。來源快照仍唯讀及全檔SHA檢查；
writer綁定私人副本dev／inode，拒絕redirected path、未知code／workload／
journal、容量不足或scope縮小。全size DB不等於寫入全部1,329萬tasks；
本測試是固定1,024筆原生持久化的成本，尚非正式 downloader 吞吐。

focused首輪327 passed，新增實際 private prepare＋四次worker＋aggregate、
來源不變及寫入路徑重導拒絕等檢查後，含原compactor／downloader回歸
418 passed in25.22s。首個測試run誤引用不存在的manifest test檔而未跑
測試，失敗保留並由418項通過的exact run supersede，不冒充成功樣本。
`ruff --select F,E9`與`git diff --check`通過。當前canonical runtime為
Python3.12.15／SQLite3.54.0；本輪未替換任何服務環境。

00:13已啟動同task的`openbb-full-size-native-writer-private`，準備兩份
保留的完整私人copy；下一步待prepare收據完成後，以fresh workers跑
相同A/B/B/A再計算aggregate。沒有提高正式owner資源、重啟下載器、
對正式DB做DDL／DML、清理source或改動任何策略／模型flag。

證據與重跑入口：

- [完整 SQL 四輪結果](../artifacts/benchmarks/running_services_20261013/openbb_full_source_contract_index_20261014/abba-receipt.json)、[最後 A2](../artifacts/benchmarks/running_services_20261013/openbb_full_source_contract_index_20261014/04-original.json)
- [未強制原始查詢仍選主鍵索引](../artifacts/operations/running-services-optimization-20261013/openbb-private-unforced-source-query-plan-20261015.json)
- [418 項 focused／完整私人流程回歸](../artifacts/operations/agent-workflow/runs/openbb-full-size-writer-complete-20261014T161230-4129a8ea/run.log)
- [完整私人 writer copy exact run](../artifacts/operations/agent-workflow/runs/openbb-full-size-native-writer-p-20261014T161351-61d5c836/run.json)
- [原生寫入測試入口](../scripts/benchmark_openbb_l1_source_write_cost.py)、[語意與安全負向測試](../test/test_openbb_l1_source_write_cost.py)

### 10/15 00:53–01:09：原生寫入四輪、查詢接線與當前快取驗收

`openbb-full-size-native-writer-a-20261014T163456-a140e297` 已 terminal
succeeded／exit 0。四個 fresh worker 使用前述完整私人 DB 與相同
1,024 筆工作量；全 task 欄位 SHA、3,072 筆 journal entry／multiset
SHA、6,144 次直接＋trigger changes、獨立 connection 回讀與來源
完整 SHA 全部相同。固定原生 claim→mark-executing→四個 256 筆
completion batch→WAL checkpoint 都計入，沒有省略 durable commit。

原版／covering 的 persistence wall 均值 1.015442／1.244171 秒
（+22.525%），CPU 0.550526／0.698902 秒（+26.952%）。完整工程
worker wall 均值 338.223／218.124 秒，包括全檔 SHA 和驗證，受
OS cache／共享主機 I/O 明顯影響，**不解讀成下載或原生寫入加速**。
這個有界成本與每日完整 source audit 的讀取收益、479.748 秒建索引
及 3.54 GiB 磁碟成本一起評估；不能把 1,024 筆 replay 外推成
全部 1,329 萬 tasks 的正式 API 吞吐。

新增共用 `openbb_l1_task_contract_index.py`，先以實際
`main.index_list`／`index_xinfo` 驗證固定名稱、完整七欄順序、
非 unique／非 partial／非 expression、ascending BINARY layout；
缺失或不相容就保持原 SQL。canonical `_mark_stale_segments` 的
全量與 journal 有界分批共用同一 lookup，沒有改 CASE／NULL／
DISTINCT／排序／路徑 fallback，也沒有改 source contract fingerprint。
實體 schema 改動仍觸發既有 full audit，無變更的 journal 仍檢查每個
derivative footer。新 helper 也納入往後 benchmark 的 code closure。

[SQLite 官方 `INDEXED BY`](https://www.sqlite.org/lang_indexedby.html)
說明這是強制要求而非 hint；接線只在實體 layout 已驗證時固定已量測
的查詢，並在索引於驗證後移除時，僅針對獨立確認的 exact missing-index
error 重試原讀取。其他 SQL／資料錯誤保留，不吞掉、不改標成成功。
每次 attempt 記錄 `source_task_lookup` 與 validation 時間；本次沒有
自動 CREATE INDEX、ANALYZE 正式庫或改 source journal watermark 規則。
更新 statistics 後由 planner 自選的完整對照尚未測量，不宣稱全域最優。

首輪 canonical 接線 174 passed；包含原生 downloader、compactor、
來源 journal、完整私人 writer、footer／view／metadata／attempt 的
較廣回歸 607 passed in 66.22s。兩組重疊，不能加總成不同測試數。
兩個新增檔案 formatting 後另外 40 passed in 2.90s；F／E9 lint 和
`git diff --check` 通過，未跑 repository 全測試。

01:03 的獨立 verifier 使用正式 consumer 回讀所有五個 enabled 台股
模式，全部 reusable，對應同一 10/14 13:30 completed close；模型
前推／provider calls=0、CUDA initialized=false。這是當前 artifact
重用的診斷驗收，不是新的獨立 GPU 數值 oracle、全歷史或券商成交。
00:59 GPU 裝置取樣觀察 56／56 程序可讀、owners／handles=0。

下載本身仍有獨立問題。原生 archive 10/14 20:33 結束 exit 2；
其 audit `checked_at=2026-10-14T07:26:07.614353+00:00` 是稽核
起算時間，不是完成時間。7,327,309 份成功檔／334,949,985 rows 的
缺檔／不可讀／列數／metadata mismatch 都是 0，但掃過 7,474,495
份檔案中有 611 份沒有目前成功 owner；另有 146,575 份已註冊的
inactive-success 歷史。ownership 建索引 3,147.392 秒、scan 1,094.903
秒，也是後續熱點；不能直接把 failed service 重啟當成修復。

只對三份舊共識 source 做獨立 task／SHA／所有欄位讀回：task ID／
canonical path 一致，內容有 target／price／analyst 數值，非 encoded
empty wrapper。擷取在 7/18，現在 task 是 inactive pending，8/20
記錄 yfinance empty；因此 current success ownership 拒絕它們。
保留原值、原擷取時間與 bytes，不把三份樣本推論成全部 611 正確、
不強制 pending→success、不 backdate／移動／刪除原始資料。

01:08:36 原生 compaction timer 自然啟動，01:11:31 exit 0；精確
invocation `63c870cfb7c444119d2b839dedf859b5` 的獨立回讀已接受。
全次 attempt 173.433756 秒、footer scan 132.628 秒／185,963 份；
journal 無變更、source lookup 缺實體索引正確走 canonical fallback，
7,327,309 sources／334,949,985 rows 的 attempt 與 publication 一致，
pending／new／stale／failed 全為 0。這不處置 archive 的 611 份 ownership
缺口，也不是 covering index 加速的正式驗收。

01:09 起始 receipt 的 `phase=lock` 只在 start／finish 時持久化，**不能
當成程序仍在等鎖**；它不是目前執行 stack。後來已由 terminal receipt
核對完整 stage timings，不把該過時 phase 誤判為卡住。
磁碟安全可用約 96.1 GiB，低於既有 archive 100 GiB floor；本次不降低
floor 或啟動下載來製造活動狀態。實體索引採用及 ownership 修復仍待
獨立驗收。21.086 秒的全 loaded-unit counter 取樣另保留；原始
`list-unit-files` 含 transient 與 template，不直接用其 86 列代替服務
數量，四個 legacy-return instances 仍保留在完整明細，未縮小原目標。

01:18 的 canonical 全服務 observer 另完成 20 秒完整取樣，分類仍為
87 個 installed＋1 個 engineering transient，67 timers／3 paths。
當時 installed 持續服務的 CPU 熱點是 TAIFEX dashboard 約 0.333 core、
FinMind complement 約 0.296 core；storage lifecycle 約 0.237 core。
GC／backup 同時有 activating 工作，不能把活動批次算成靜止 baseline。
TAIFEX dashboard 匿名記憶體約 125 MiB，另有約 3.91 GiB cgroup file
cache，後者不是同量不可回收的 Python heap 或 GPU 顯存。

該 observer 保留原生 compaction 的 443 次 memory.high event；max／
OOM／OOM-kill／swap 都為 0。不隱藏 soft-limit 壓力、不調高 cap 來
美化時間。後續沿真正耗時的 footer／I/O 與常駐 dashboard 完整請求
路徑實測；TAIFEX 夜盤仍開市，不能因台股收盤就停止有效資料串流。

證據：

- [完整原生 persistence 四輪結果](../artifacts/benchmarks/running_services_20261013/openbb_full_size_native_writer_index_20261015/abba-receipt.json)
- [607 項接線／原生共享回歸](../artifacts/operations/agent-workflow/runs/openbb-native-lookup-full-regres-20261014T170105-f8b3e343/run.log)
- [五模式當前收盤快取完整診斷](../artifacts/operations/running-services-optimization-20261013/completed-cache-current-readback-after-lookup-20261015.json)
- [最新 managed GPU 裝置取樣](../artifacts/operations/running-services-optimization-20261013/managed-gpu-after-lookup-code-integration-20261015.json)
- [正式 compactor fallback／完整 footer／publication 回讀](../artifacts/operations/running-services-optimization-20261013/native-openbb-lookup-fallback-20261015.json)
- [全 loaded-unit 配對 counter 與分類限制](../artifacts/operations/running-services-optimization-20261013/service-census-after-native-lookup-20261015.json)
- [87 installed 的 canonical 全服務清冊與壓力讀回](../artifacts/operations/running-services-optimization-20261013/all-native-services-canonical-census-20261015.json)
- [三份保留原檔 task／SHA 讀回](../artifacts/operations/running-services-optimization-20261013/openbb-orphan-source-readback-20261015.json)、[所有欄位分類](../artifacts/operations/running-services-optimization-20261013/openbb-orphan-content-classification-20261015.json)
- [共用 lookup 驗證](../scripts/openbb_l1_task_contract_index.py)、[原生接線／fallback 測試](../test/test_openbb_l1_task_contract_index.py)

### 10/15 01:25–02:03：TAIFEX 重複來源驗證移除及原生採用

30 秒、49 Hz 的原生 `py-spy` 只作熱點診斷，取到 active Python 約
1.33 秒，不能拿這個樣本稱整段 CPU 百分比。`taifex-history-1d` 路徑
涉及欄式資料轉換、JSON 輸出與完整 SHA；主 marks 當時約 4.18 GB。
實體資料夾另有舊 replay，canonical `_history_backfill_receipts` 按既有
契約選出目前 v3；本次固定的是其全部有效來源約 5.15 GB，而不是刪除
或忽略任何本來有效的來源。舊 replay 與原始檔全部保留。

原欄式快取已在 `_source_digests` 完整讀取來源，並以 descriptor／path
的 device、inode、size、mtime、ctime 作兩次 generation fence；建立／
發佈後，下一查詢卻又對同一來源完整 SHA。改為有界 16 entries、帶鎖
LRU，只將已完成且通過 fence 的 source proof 記入本程序。expected SHA
也是 key；重啟丟失所有 memory proofs，來源改寫／換 inode／metadata
換代依然驗證全部前綴。**新 payload 不預先 seed**，下一 reader 仍獨立
驗證其 SHA。沒有假設永遠 append-only、mtime 相同就可信，也沒有取消
schema、收據、並行 lease 或壞資料 fallback。

工程來源副本與原版 module SHA 在編輯前保留；八次交錯順序為
`baseline, candidate, candidate, baseline` 重複兩次，各 fresh worker
使用獨立 initial cache。只在私人副本一次 append 已捕獲的真實 260,856
bytes，沒有捏造 ticks／quotes。每輪第一階段包含全部七種完整曲線、
durable JSON 寫入及讀回；第二階段同程序再讀七區間，不能略掉後續成本。

| 中位數，完整固定有效來源 | 原版 | 修正 | 實測差異 |
| --- | ---: | ---: | ---: |
| 增量後七區間＋durable readback wall | 14.4976 s | 12.8237 s | -11.5% |
| 同階段 CPU | 21.4020 s | 20.0163 s | -6.5% |
| SHA helper 完成的驗證 bytes | 9,429,984,329 | 5,252,215,986 | -44.3% |
| 兩階段合併 wall，逐 trial 相加再取 median | 19.9593 s | 18.5443 s | -7.1% |
| 兩階段合併 CPU，逐 trial 相加再取 median | 31.6131 s | 31.2243 s | -1.2% |

peak RSS 1,064,007,680→1,059,790,848 bytes，差異不到 0.5%，不稱明顯
記憶體收益。暖讀各自 SHA proof bytes 為零；暖讀時間有約 4.7% 波動，
不把其單獨宣稱加速。八輪七區間完整 JSON SHA 一致，一日完整曲線另與
原始 NDJSON parser 相同，當時 raw control 6.71 秒。本機同時仍有下載／
夜盤／儲存工作，wall 中位數不是獨佔硬體上限或統計顯著性證明。

第一個測量 launcher 因使用 `atomic_write_json` 不支援的 `mode` 失敗；
修正入口、補八項 worker／路徑／讀回 smoke，保留失敗檔及原始 run，
從新 `comparison-v2` cache 目錄完成正式八輪。不將失敗改寫成成功。
獨立准入重算 trial medians、驗證原版 module／原始對照 SHA、完整七範圍、
16 proof 上限及 warm 成本；30 項 smoke、153 相關回歸後採用，最後版本
161 passed，包括實際 owner birth／較新程式／舊 owner 收據拒絕測試。
這些測試有重疊，不相加稱 344 個不同測試；未跑全 repository suite。

01:50 獨立預暖完成，01:51:35 **只重載唯讀 dashboard**，新 invocation
`1343a444b1ae4f519fdb0d21e7b6d2f8`、PID 138733。01:53 的 runtime
驗收全部七區間 strict data clock 通過，coverage end 為 01:52:43。
主 projected cache 2,068,041 rows／31,797,445 bytes，v3 397,785 rows／
4,418,206 bytes；主收據確實晚於新 process start，而不只是晚於 prewarm。
新 owner 啟動晚於全部已驗收 code files；兩個原行情／模擬交易 owner
InvocationID 不變，無 broker／provider／CUDA／source／ledger 操作。

01:55 canonical 20.98 秒取樣仍是 **87 installed＋1 engineering transient**、
67 timers／3 paths。TAIFEX 面板當時 CPU 約 0.170 core、anon 130 MiB、
file cache 73 MiB；與 01:18 約 0.333 core／3.91 GiB file cache 的捕獲
階段不同，尤其重啟會改變 page-cache 的 cgroup 歸屬，**不作正式降幅**。
FinMind complement 當時約 0.854 core 是下一持續熱點。compactor 的
新 invocation `b7ce592d7b4142a5bf1b97395ec7c137` 記錄 1,045 memory.high
events，max／OOM／OOM-kill／swap 為零；soft-limit 壓力仍是待最佳化項目。

01:56 62／62 managed Linux processes 無 GPU device handles；Windows／
整卡／遊戲 FPS 不在此證明範圍。同次五模式 completed-session consumer
**0／5 拒用**，`completed_source_generation_changed`，配置一致但目前
implementation SHA 已變：`factorized_sparse_cache.py`、
`factorized_sparse_rows.py`、`factorized_sparse_projection.py`。這些是
同 workspace 其他工作，未由本輪修改；相關收盤 owner 上一次成功為
23:45，不把舊 generation 重貼成目前成功，也不強迫重算 GPU／重啟
Gateway／覆蓋 checkpoint。當前目標繼續處理 generation 正常刷新與
87-service 下一熱點；歷史 2,906 列 gap、OpenBB physical index／ownership／
容量及次日開盤端到端驗收仍分開保留，不標記全部最佳化完成。

證據：

- [原生短 stack 診斷](../artifacts/operations/running-services-optimization-20261013/taifex-dashboard-native-profile-20261015.json)
- [原版 module 證明](../artifacts/benchmarks/running_services_20261013/taifex_proof_memo_20261015/original-code-proof.json)、[完整八輪七範圍／增量／暖讀](../artifacts/benchmarks/running_services_20261013/taifex_proof_memo_20261015/comparison-v2.json)
- [最後 161 項共享回歸](../artifacts/operations/agent-workflow/runs/taifex-proof-memo-final-regressi-20261014T175440-03f0db7d/run.log)
- [原生獨立預暖](../artifacts/operations/running-services-optimization-20261013/taifex-proof-memo-native-prewarm-20261015.json)、[重載後 owner／全區間／新收據驗收](../artifacts/operations/running-services-optimization-20261013/taifex-proof-memo-native-acceptance-20261015.json)
- [全 installed 服務及壓力取樣](../artifacts/operations/running-services-optimization-20261013/all-services-after-proof-memo-census-20261015.json)、[GPU 裝置取樣](../artifacts/operations/running-services-optimization-20261013/managed-gpu-after-proof-memo-20261015.json)
- [當前五模式 cache 拒用的完整原因](../artifacts/operations/running-services-optimization-20261013/current-close-cache-after-proof-memo-20261015.json)

## 10 月 15 日 02:08–02:52：workflow 身分邊界修復與 FinMind 原生控制路徑

### 已採用：外地收據不再阻塞無關本機執行

02:08 原生 profiler 的 canonical launcher 被兩份 Vast 回傳的 run 收據阻擋：
`Workflow.runs()` 對每份紀錄套用本機 host／checkout 驗證，將外地成功宣告
誤當作不相容的本機紀錄，連無關 task 的 status／新 run 都無法使用。
保留原收據、原版 workflow 程式及 SHA；沒有刪除紀錄、查外地 PID、偽造
遠端存活，也沒有把遠端宣告的 exit 0 當成本機成功。

修正 listing 的所有權分類：完整 schema／host／checkout 的外地收據只列
`foreign_unverified`，allowlist 顯示其宣告結果與來源。定點 read／execute／
reconcile／寫入仍要求本機身分；同 task／同名未驗證紀錄仍不授權重試或
完成；格式錯誤仍拒絕，不藉此跳過壞紀錄。50 項 workflow 回歸通過。

02:30 獨立原生 launcher 已實際執行本機驗收：6,382 本機／2 外地未驗證
收據可並存；定點外地讀取仍拒絕，原 profile 收據 SHA 與修正前完全相同。
後續 profiler、完整快照 benchmark、回歸及 service census 都從這個已恢復
的 canonical launcher 留下實際 exit 收據，不把 queued 當成功。

### FinMind：先量測，未達完整收益的候選不部署

實際 owner 維持 invocation `165a95f3e8514ad491fdb7cb28f61654`，MainPID
1,920,404，Python PID 1,920,445。30 秒／49 Hz py-spy 有 1,840 samples、
零 errors。主執行緒 `_status` 約 6.92／20.39 weighted sample-seconds，
約 34%；`incremental_reservation` 約 2.43。這是 stack weight，不是全機
CPU 百分比、完整下載速率或所有 worker 的可相加 wall time。

唯讀同一 SQLite snapshot 有 3,482,652 tasks；3,421,449（98.24%）具有
非預設 rows／bytes／日期／attempt 資訊。故不實作「全量計數＋部分統計」
拆掃描：幾乎全部紀錄仍在部分索引內，另加計數與維護不構成明確省工。

追新預留查詢只涉及 complement 的 21,068 筆 priority-0 非 derived 工作，
sponsor 約 64–65 筆。先在隔離真實 online-backup 上測 partial covering
index，保持原查詢、cohort／canary、calendar、due-hour、alias、priority
與逐項原始資料語意。SQLite 的部分索引須符合查詢 predicate；
`INDEXED BY` 是必要索引約束而非一般 hint，故固定索引亦只當隔離原型
比較，未強迫正式 reader 採用。
[SQLite partial index 文件](https://www.sqlite.org/partialindex.html)、
[SQLite INDEXED BY 文件](https://www.sqlite.org/lang_indexedby.html)。

初次 12 輪 ABBA 的 reservation 中位數 16.61→7.48 ms；不能據此把完整
流程／API 吞吐宣告成倍速。同一保留的雙 queue 輸入又完成 18 次平衡三路
比較，包含完整 native reservation、metadata 讀取、完整 `_status`／selector／
frontiers／retry summary、JSON fsync，以及每輪各 64 筆 history／release
task 的 canonical claim／release、每組 128 次 durable transactions。
所有輸出 SHA 一致，所有 selected task values 恢復，來源佇列未寫入。

| 固定輸入三路中位數 | 原路徑 | 新索引／自動規劃 | 固定索引原型 |
| --- | ---: | ---: | ---: |
| 完整 reservation wall | 24.31 ms | 11.63 ms | 17.17 ms |
| 完整 reservation CPU | 23.46 ms | 11.51 ms | 15.97 ms |
| reservation＋完整 status wall | 1.877 s | 1.952 s | 1.954 s |
| reservation＋完整 status CPU | 1.842 s | 1.897 s | 1.915 s |

完整控制路徑波動很大，未證明可靠的整體加速；release 組 64 筆寫入的
wall 中位數亦為 1.073→1.228 秒，CPU 則 0.0532→0.0455 秒，不把這些
混合差異都歸因索引。部分索引另外佔 complement 約 1.62 MiB／sponsor
20 KiB；建置掃描約需 5.5–6.5 秒。**決定不部署此候選**，維持正式
下載器、配額、交易日與排程。實驗程式收斂到獨立 benchmark helper；
三個生產檔案逐一 SHA 回讀都與保留原版一致，正式雙 queue 均沒有候選
index，原服務 invocation／PID 未變。沒有以縮減歷史／跳過驗證換快。

較廣回歸先發現一個只接受 `idx_finmind_outstanding_dispatch` 名稱的舊
assertion。用保留原版 `_db`／`_next_task` 與候選獨立比較，兩者都走相同
`idx_finmind_status_covering(dataset=?,state=?)` seek、選同一資料集；不是
本次造成的退化。測試改為要求指定 dataset **且** outstanding state 的
有界 seek，並分別覆蓋有／無 status covering index，不接受全表掃描。
原失敗 run 保留，沒有刪掉測試或略過這個條件。

最後正式程式已復原、原型已隔離的版本：**1,074 passed／81.84 秒**，涵蓋
60 個 FinMind 測試檔、workflow、runtime、drained idle reload；包含既有
及新原型／prepared-fixture／clock／cohort／legacy／source-preservation
測試。48 項前期小集合是重疊驗證，不相加當成不同測試數；沒有跑整個
repository suite。

### 最新 GPU／收盤 cache 邊界

02:49:57 的 **68／68** 可見 managed Linux processes 完整可讀、GPU owner／
device handles 均為零；probe 沒有 CUDA 初始化。這不是全 Windows／
整卡 VRAM／遊戲 FPS 歸因，也不代表所有服務已最佳化完成。

02:51:25 五模式 canonical completed-session consumer 仍 **0／5**：
`completed_source_generation_changed`，resolved config 相同，但 implementation
已換代。沒有模型前推、provider 呼叫或 CUDA 初始化；保留既有 cache／
checkpoint，不硬套舊 generation。後續仍須檢查正常 source-events／
postclose owner 如何安全完成新世代，不能把 01:03 的成功充作當前成功。
02:51:46 canonical 21.12 秒新普查為 89 services／67 timers／3 paths，含
本輪 engineering transient；service-count 變化不是已完成最佳化的證據。

證據：

- [workflow 外地身分／原收據 SHA／原生 launcher 獨立驗收](../artifacts/operations/running-services-optimization-20261013/workflow-owner-native-boundary-20261015.json)
- [50 項 workflow 回歸](../artifacts/operations/agent-workflow/runs/workflow-foreign-owner-boundary--20261014T181408-693abac1/run.log)
- [原生 FinMind stack profile](../artifacts/operations/running-services-optimization-20261013/finmind-native-cpu-profile-20261015.json)、[唯讀完整 tasks census](../artifacts/operations/running-services-optimization-20261013/finmind-status-materialized-stats-census-20261015.json)
- [第一輪完整控制／寫入／planner 比較](../artifacts/benchmarks/running_services_20261013/finmind_release_reserve_20261015.3tw97C/comparison.json)、[固定索引第二輪](../artifacts/benchmarks/running_services_20261013/finmind_release_reserve_20261015.3tw97C/comparison-v2.json)、[相同保留輸入三路比較與拒用決策](../artifacts/benchmarks/running_services_20261013/finmind_release_reserve_20261015.3tw97C/comparison-three-plans.json)
- [正式源碼原 SHA／native invocation／雙 queue 無候選 index](../artifacts/operations/running-services-optimization-20261013/finmind-native-candidate-not-deployed-20261015.json)
- [最後 1,074 項共享回歸](../artifacts/operations/agent-workflow/runs/finmind-reserve-final-isolated-r-20261014T184923-3762253f/run.log)
- [最新 GPU 零 handle 取樣](../artifacts/operations/running-services-optimization-20261013/managed-gpu-after-finmind-screen-20261015.json)、[當前五模式 cache 拒用診斷](../artifacts/operations/running-services-optimization-20261013/current-close-cache-after-finmind-screen-20261015.json)、[完整 service resource 普查](../artifacts/operations/running-services-optimization-20261013/all-services-after-finmind-screen-20261015.json)

重現原型用 `run_fintech_python -m scripts.benchmark_finmind_reservation --help`，
指定新 fixture／output；可用 `--prepared-from` 綁定原收據、保留原輸入，
用 `--compare-automatic` 做平衡三路比較。這些是本機控制路徑原型，沒有
正式 API 限額／吞吐／下載完成／GPU 數值結果的接受權。

## 10/15 03:15–03:42：新世代快取的完整數值准入與 Bybit writer 根目錄修復

### 先區分過期快取、程式准入與 GPU 真正工作

02:51 的 0／5 是正確拒用，不是可移除的檢查：既有 Gateway 已載入的
Python modules 與三份後續修改的 factorized data／model 程式不同。
source-event path 監測來源與 eligibility，不會因工作樹 Python 改動就
自動宣告新模型相容。未放寬 `loaded_implementation_identity()`、未縮減
code member 集合、未把舊 receipt 改寫成新版本，也未動其他工作的程式。

在既有 `benchmark_live_inference.py` 加入兩個明確分離的工程入口：

- `--capture-forward-identity` 沿 canonical preparation／mask／window／來源
  lease 與最後 identity fence，到真正 forward boundary 就停止。拒絕 model、
  CUDA、HTTP、發布及歷史訊號生成；forward cache 既不讀也不寫。既有 CPU
  panel caches 可正常使用。契約 `completed_forward_identity_observer_v1`
  明確不是數值／artifact 准入。
- `--forward-control` 只接受完整 input identity 除 code 外一致的既有對照。
  舊 payload 依原身分完整 SHA／safe CPU readback，候選用私有 cache 正常
  執行新模型；比較全部 output tree、shape、dtype、auxiliary、NaN bits 與
  signed zero。不同輸入在 GPU 前即拒絕，不截 top-N、不量化、不省欄位。
  舊 tensor 不是新模型 forward 的替代品；比對失敗不提交候選 payload。

五模式 CPU input observation 為 1.44–2.79 秒，**全部只有 code 改變**：
config／checkpoint、完整視窗與 mask、symbols／features、CPU threads、
AMP／TF32／runtime 均一致。含 14,726 features 的 factorized 模式亦沿
原 transport，未建立縮小資料集或另設模型。strict CUDA preflight 成功後，
五模式各正常前推 **一次**，第二輪 cache hit；所有完整輸出位元相等，
每個模型釋放後 allocated／reserved 均為零，工程 GPU 程序已自行結束。

`install_completed_inference_cache.py` 是**手動明確的已驗證 cache 安裝**，
不是新增推論服務。它要求 real-new-forward＋warm-hit＋完整 bitwise oracle
與 GPU release 收據，在 source shared lease 下重新取得 canonical CPU
identity，再次比對舊對照並以 `cached_forward` 的原 kernel lease／SHA／
durable commit 安裝新 generation。沒有模型、CUDA、provider、原始資料
修改或 signal publication。舊 keys／payload／控制記錄都保留。

### 正式 owner 與獨立回讀

03:35:15–03:35:36，canonical `stockagent-discord-postclose-cache.service`
InvocationID `1795feb8df9443dfaa3a453f482fc855` 終止成功：attempted=5、
failures=0，來源 warming 與 formal history 不在 cache-only 工作範圍。
原服務自報 21.036 秒 wall、24.570 CPU 秒、約 1.2G memory peak；這是
有工作批次，不能當作日常 idle。五正式 forward keys 均接回 current code。

各 fresh CUDA-cold 程序的完整 output digest 與獨立 GPU 控制一致，兩次
讀取皆 disk hit／GPU calls=0，CUDA 未初始化：

| 模式 | fresh process 完整訊號 | warm 完整訊號 | GPU 前推 |
| --- | ---: | ---: | ---: |
| 100m | 2.279 秒 | 0.384 秒 | 0 |
| factorized scale-separated cash | 1.913 秒 | 0.718 秒 | 0 |
| multi-basis | 1.928 秒 | 0.395 秒 | 0 |
| multi-basis 22 | 1.431 秒 | 0.438 秒 | 0 |
| V8 annual log cash | 4.593 秒 | 0.913 秒 | 0 |

包含完整 preparation、auxiliary 與金融格式化，未控制 OS page cache；
不是 09:00 latency、券商成交、完整 fold 或 GPU kernel 的單獨時間。
最新 canonical consumer 另 **5／5 accepted**，沒有 CUDA／provider／模型。

只在既有 idle admission 和 source lease 下再次確認原 owner、六個 protected
owners、程式、五摘要／keys 與準備窗口不變，才重載 Discord 一次。
新 InvocationID `c8d04971f9bc4fe88e78aa6538e98021`、wrapper 457438、Python
457512。獨立 numerical control 與 code-admission control 都接受：Gateway／
commands sync、五模式 engine ACK zero lag、summary SHA／keys／protected
owners 不變，runtime code SHA 正確，無 GPU handles、warm-ready、watchdog
或 fatal。行情、模擬成交、FinMind、TAIFEX／public dashboard owners 均未動。
下一個自然暖機／09:00 開盤仍需獨立驗收。

03:41:43 **67／67** managed Linux processes 可讀，GPU owners／handles=0，
probe 不初始化 CUDA。03:42:12 的 21 秒全服務取樣涵蓋 89 services（含工程
transients）、67 timers、3 paths；Discord 是 **0.01407 CPU core**、
541,106,176 bytes anonymous、20,164,608 bytes file cache、零 high／OOM
事件。03:38 的 0.161 core 包含剛重載，不冒充 steady idle 或全機 A/B。
FinLab／FinMind／backup／storage 仍有真實批次；也不把單卡瞬間約
3993 MiB／12% GPU 利用率全歸於 StockAgent，Windows 及遊戲 FPS 未驗收。

### Bybit：不能把未掛載／已回收誤作空歷史

原 registered-intraday Bybit 子步驟在 `output_dir.mkdir()` 失敗，因
`data_bybit` 指向已不存在的 immutable materialization；formal owner namespace
的 status 證實 **cold-only**，不是只在 IDE namespace 看不到。
冷庫 latest 有 4,677 files／22.375 GB logical，但不能擅自把它替換成原
writer generation。原 broken link 固定 release
`bybit-20260926T051004983477667Z-l0-penguin-857d2f9a99f1b88f`
仍有全部 objects／manifest，SHA
`7af635eb057024bd9e26ed0b0c621c0c124c89c93af7469d1c48e090ab272df6`，
4,677 files／22,357,148,836 bytes。僅 manifest／objects presence 當時
不等於完整冷恢復；没有mkdir空樹、刪除 link 或寫進 immutable hot cache。

`restore_missing_crypto_workspace.py` dry-run 預設，只允許既有三個 CEX；
本次 explicit apply 僅 Bybit。固定 release＋manifest SHA＋broken link stat
身分、32 GiB 剩餘空間、相同 `bybit_source_publish.lock`、真實 mutable root、
canonical **durable** 全檔 fetch／hash／metadata verification 都保留。
全驗後使用既有 no-replace rename 到 `/srv/stockagent-live/data_bybit`，
再原子換 link；未經本次收據的既有 destination 一律不覆蓋。若剛安裝
workspace 就中斷，仍須全驗後才可續接；失敗 scratch／冷庫／原始證據不刪。
這不是每日 downloader 的自動 cold-to-hot hook，之後 writer 不再依賴會過期
的 immutable materialization。還原只證明原 snapshot baseline，追新／歷史
缺口仍須由原 canonical downloader 的新收據另行證明。

03:40 仍是同一 recovery PID 439596、start_ticks 11121648；native stack
顯示正在 `verify_packed_snapshot → sha256_file` 冷庫 I/O，不因觀察時間到期
重啟／複製另一個 owner。**此時尚未換 link，尚未恢復 Bybit 追新**。
重新使用 system PATH 的 canonical `py-spy` 後實際 dump 成功；先前錯把
Rust profiler 查到 Python env/bin 的失敗收據保留，不當成來源卡死。
03:47 的同 PID／start_ticks 10 秒唯讀取樣，rchar／read_bytes 均前進
176,513,308 bytes，沒有寫入／重啟；約 16.83 MiB/s 是當下冷庫讀取樣本，
不是完整還原吞吐或全局 ETA。後續要保留同 owner 完成，不能重抓另一份。

### 本輪驗證與證據

最後 11 個 shared 測試檔 **182 passed／14.00 秒**，涵蓋完整 tensor oracle、
純 CPU observation、qualifier、cache integrity、來源 lease、cache-only owner、
native idle／loaded-code admission、materialized-cache 與 writer recovery。
前期 54／62／75、7 項是重疊集合，不相加；首次 wider invocation 指到不存在
的 `test_data_cache_cli.py`，exit 4／零測試，已用實際存在集合重跑並保留失敗。
Ruff、git diff --check 通過；沒有聲稱 repository 全測試或全服務最優。

證據：

- [五模式 complete CPU identity 與 code-only 差異](../artifacts/operations/running-services-optimization-20261013/forward-input-generation-comparison-20261015.json)
- [strict CUDA preflight](../artifacts/operations/agent-workflow/runs/current-close-code-strict-cuda-p-20261014T192625-ccbb1197/run.json)、[五模式一次新 forward／完整舊 oracle](../artifacts/operations/agent-workflow/runs/current-code-full-forward-old-or-20261014T192735-8dc35a6e/run.json)
- [新 generation canonical cache 安裝](../artifacts/operations/agent-workflow/runs/qualified-new-code-five-cache-in-20261014T193326-ec64860f/run.json)、[fresh CUDA-cold 完整重讀](../artifacts/operations/agent-workflow/runs/fresh-cuda-cold-five-current-cac-20261014T193516-42d9e075/run.json)
- [5／5 current canonical consumer](../artifacts/operations/running-services-optimization-20261013/postclose-current-five-reuse-20261015.json)、[合格單 owner 動作](../artifacts/operations/running-services-optimization-20261013/discord-qualified-current-code-reload-20261015.json)
- [新 native numerical control](../artifacts/operations/running-services-optimization-20261013/discord-new-native-numerical-adoption-20261015.json)、[獨立 loaded-code 准入](../artifacts/operations/running-services-optimization-20261013/discord-new-native-code-admission-20261015.json)
- [67／67 managed GPU 零 handles](../artifacts/operations/running-services-optimization-20261013/managed-gpu-stable-after-code-adoption-20261015.json)、[穩定全服務取樣](../artifacts/operations/running-services-optimization-20261013/all-services-stable-after-code-adoption-20261015.json)
- [Bybit 原 link／固定 release 身分](../artifacts/operations/running-services-optimization-20261013/bybit-cold-source-identity-20261015.json)、[無寫入 dry run](../artifacts/operations/running-services-optimization-20261013/bybit-mutable-recovery-plan-20261015.json)、[仍在執行的 recovery](../artifacts/operations/agent-workflow/runs/bybit-fixed-cold-to-mutable-reco-20261014T193247-75a17f03/run.json)、[同 owner 原生 stack](../artifacts/operations/agent-workflow/runs/bybit-full-reconstruction-native-20261014T194008-2e590b7c/run.log)、[10 秒原生 I/O 前進](../artifacts/operations/running-services-optimization-20261013/bybit-recovery-live-io-progress-20261015.json)
- [182 項 shared 回歸](../artifacts/operations/agent-workflow/runs/close-cache-and-recovery-shared--20261014T193451-e74f8fe4/run.log)

技術選擇沿既有 `torch.inference_mode` 與 restricted `torch.load(...,
map_location="cpu", weights_only=True)`，先減掉沒有需要的 forward，再討論
kernel／compile。官方亦說明 inference mode 移除 view/version tracking 開銷，
CPU map 可避免反序列化 GPU tensor 的顯存上衝；不拿文件代替這次本機完整
實測，也沒有為「最新」直接升級共享 PyTorch 或改精度／checkpoint：
[inference-mode 官方說明](https://docs.pytorch.org/docs/2.14/generated/torch.autograd.grad_mode.inference_mode.html)、
[CPU／restricted load 官方說明](https://docs.pytorch.org/docs/2.14/generated/torch.load.html)。

## 10/15 03:55–04:09：FinLab 完整進度清冊的主鍵選擇與原生載入

本輪沿用既有 owner、配額、交易日／發布時程、分鐘優先／tick residual、
raw Arrow／Polars、完整 source receipt 與單位契約。沒有增加 provider
呼叫、改造 SDK、放寬 reserve、截斷年份／股票或降低進度更新頻率。
工程偏好及儲存維運流程要求先量整段工作，固定完整索引、保留每次失敗及
來源，確認真正部署，不將已在工作的 CPU 當成閒置浪費。

### 原生瓶頸及方案

03:56:45 自然啟動的 FinLab Invocation
`41aadf4b19164b138b14f295a1504be0`，tick Python PID 507612／start ticks
11269829，在完整程式身分前後相同下做 20 秒、29 Hz 非阻塞取樣：467 samples、
6 errors 均保留，約 91.65% 活躍 sample weight 含 `_summarize`。這是
Python 活躍樣本歸因，不是整機／Windows CPU 百分比。完整本機摘要的額外
cProfile 對照有 18 次 SQLite execute，累計約 6.019 秒；不是憑名稱猜瓶頸。

`effective_partitions` 原先對 direct／derived 分鐘全體做 `ROW_NUMBER`
排序，再由每個完整統計／候選查詢消費。改為同一 TEMP view 的主鍵 partner
probe，對每檔／日只判斷是否有勝出的另一來源：

- direct 成功時優先；direct 失敗只會輸給成功的 derived。
- 兩邊失敗仍保留 direct；沒有 direct 時保留 derived（含其失敗）。
- tick 仍獨立；原 rows／bytes／狀態／source origin／檢查時間完全保留。

既有 `(kind,symbol,trade_date)` 主鍵已足夠，不新增持久索引／快取、沒有
schema migration 或延遲失效；原 transaction 下立即看到修復／新增資料。
選擇依[SQLite 官方索引／query planner 說明](https://www.sqlite.org/optoverview.html)
及[官方 EXPLAIN QUERY PLAN](https://www.sqlite.org/eqp.html)實測；不為
「最前沿」無依據替換共享環境或放鬆財務／來源語意。

### 固定完整資料比較

`benchmark_finlab_effective_partitions.py` 使用 readonly native SQLite online
backup，再在私人副本執行 canonical `_record`、`_summarize`、完整股票／日曆
及 bounded hole planner、完整 JSON 發布。原版 SQL 作保留控制，新版直接呼叫
正式 helper，不用另一份 benchmark-only candidate。來源／正式 schema
未寫；provider/network 在工程程序中禁止。

最終固定 snapshot SHA
`d4a64712dc4d4da57f9b0c425bd829d3705d67335e76780c0e9aae2ec33f126d`：
280,391 原 partition、280,294 effective rows、2,461 股票、5,711 session，
2003-08-01 至 2026-10-14。完整輸入 context／原版／候選 SQL／每 case DB、
完整輸出與 cProfile 都保留。64 次 `_record` 仍逐次 commit／即時讀回，
沒有省略持久寫入來提高查詢分數。

| 固定完整本機流程 | 原版中位數 | 主鍵選擇中位數 | 本次變化 |
| --- | ---: | ---: | ---: |
| 索引寫入／讀回＋完整清冊／候選／JSON 發布 wall | 10.293 s | 3.807 s | −63.0%（2.70×） |
| 相同流程 process CPU | 9.563 s | 3.365 s | −64.8% |

全股票／calendar／FinLab daily footer 的固定輸入 preparation 另量 3.296 秒，
不是省略它；cProfile 和全部 effective rows 的排序 SHA 是計時外獨立驗證。
同一 case relation 的每一欄、完整摘要、候選順序及 64 份讀回結果一致。
OS page cache 未清除，host 仍有真實下載／冷庫工作；不同輪固定 cohort 不
混算。這是完整本機索引／清冊工作量收益，**不是端到端 API throughput、
配額上限或全機最優證明**。首次 prototype 280,307-row A/B 亦全量相符，
但不把其 2.06× 與最終同輪 2.70× 平均成一個不存在的實測。

### 不重啟的正式 adoption

舊 tick owner 04:05:09 以 exit 0 自然完成，未送 signal。相同已部署 timer
04:06:10 啟新 Invocation `c206f7f9f61e4958aae83e4821560365`、MainPID
528986；新 tick Python 530318／11324794（同 boot）自然載入新程式。
`active_run.json` 新增 versioned `partition_index_view` observation，只記錄
實際已開 TEMP view 的 SQL SHA，不靠磁碟程式檔名稱推定 loaded code。

獨立 verifier 比對完整 benchmark 的 code SHA、native cgroup／cmdline／cwd／
canonical Python／PID generation、新鮮 progress，實際 SQL 指紋為
`7e33888a9c178b16e85e1a7cb8f54903ebcbfda59abfad03bec078b48a17f7ad`。
前後同 owner，attempt 30→31，已成功 11；新近 `5438／5434／5432`
2026-08-26 分鐘來源的 source receipt、完整 file SHA、rows、bytes 和原
validator 全部通過。三分區證明是有界獨立 readback，不等於全歷史完整。

434 項相關回歸涵蓋 121 組 direct／derived presence/status、即時修復、實際
view identity、分鐘/tick priority、Arrow、quota、pipeline/recovery、dashboard
與 ETA。先前 124／433 是重疊集合不相加。初次 pytest 因 iterator parametrize
的新 deprecation 被正式 warning gate 拒絕，改成 tuple 而非關閉 warning。
一輪 canonical comparison 正確因工程 code observer 變更被 generation guard
拒收，保留失敗，固定最終版本重跑成功；不把該輪時間當正式採用證據。

04:05 全部 68／68 observed managed Linux processes 沒有 GPU handles／owner，
probe 未初始化 CUDA；04:08 再回讀五個正式收盤模式，5／5 current canonical
cache 命中，model forward／provider／trading writes＝0、CUDA 未初始化。
不推論 Windows VRAM／遊戲 FPS 或下一次盤前 readiness。

04:09 新一輪完整清點含 89 services（也含工程 transient）、67 timers、3 paths。
該 20 秒樣本 Discord 約 0.0149 core／520 MiB anonymous RAM，FinLab 0.471
core、FinMind complement 0.314 core；兩個 downloader 正在抓取，這不是
matched-source 全下載 A/B，也不把 89 個觀測項當成 89 個已最佳化服務。

Bybit 還原仍由原 439596／11121648 owner 處理全部固定 source；04:05 已觀察
實際讀取 31,365,085,367 bytes、寫入 4,097,265,664 bytes，進入有資料寫出的
完整重建，尚未有 final recovery receipt。這些 I/O counters 不能換算成原值
完成百分比、ETA 或最新歷史完整。未切換 symlink、未重啟、未清 partial／cold。
04:11 同 PID generation 的獨立 5 秒重觀測，讀／寫各前進約 56.1 MB；
有確實 I/O 前進，不由 lock file 或 elapsed time 猜測卡死／完成。

證據：

- [完整 prototype control](../artifacts/benchmarks/running_services_20261013/finlab_effective_relation_20261015/comparison.json)、[固定最終 canonical A/B/B/A](../artifacts/benchmarks/running_services_20261013/finlab_canonical_effective_relation_final_20261015/comparison.json)
- [正式 PID 非阻塞 profile](../artifacts/operations/agent-workflow/runs/finlab-native-active-python-prof-20261014T200113-a19428c2/run.log)、[保留 467 samples](../artifacts/operations/running-services-optimization-20261013/finlab-native-tick-speedscope-20261015.json)
- [generation guard 拒收的工程輪](../artifacts/operations/agent-workflow/runs/finlab-canonical-full-index-abba-20261014T200321-057bb339/run.log)、[434 項完整相關回歸](../artifacts/operations/agent-workflow/runs/finlab-view-observer-regression-20261014T200436-99e80d4c/run.log)
- [新 native connection／同 owner／來源 readback 驗收](../artifacts/operations/running-services-optimization-20261013/finlab-native-keyed-view-adoption-20261015.json)
- [68／68 managed GPU-cold](../artifacts/operations/running-services-optimization-20261013/managed-gpu-finlab-view-stage-20261015.json)、[五模式收盤 CPU cache 再驗](../artifacts/operations/running-services-optimization-20261013/postclose-five-after-finlab-view-20261015.json)
- [04:09 完整服務資源觀測](../artifacts/operations/running-services-optimization-20261013/all-services-finlab-native-view-20261015.json)、[Bybit 同 owner 持續重建的 I/O 證據](../artifacts/operations/running-services-optimization-20261013/bybit-reconstruction-stage-progress-20261015.json)

## 10/15 04:14–04:49：FinLab 准入不重建清冊、Bybit 恢復與 IDE watch 壓力

### FinLab：保留資料與排程，只取消不會使用的清冊計算

前一輪原生 profile 已指出 `_summarize` 占 active Python samples 的約
91.65%。其中 tick 的 minute-first admission 每到既有一分鐘／重置檢查點，
只是要判斷分鐘缺口是否允許抓 tick，卻重建完整 tick 統計、來源分類、
4,922 個顯示列及所有 coverage phases，再丟棄除 minute priority 外的結果。

本輪抽出共享 `_minute_priority`，完整網頁清冊仍由 `_summarize` 原 cadence
更新、欄位不刪減；准入用 `_minute_admission` 僅計算相同的 minute inputs。
不保存跨 transaction 的舊決定，不改發布、retry、quota reset/drain、開盤
runway、交易日、上市／退市、分鐘先於 tick 的規則，也不更動 provider call
cadence／worker／資料範圍。first-date 保留 known-symbol、selected-horizon、
accepted observation 的原定義，minute 優先、沒有 minute 才用 tick 起點，
不能用未過濾的全資料 MIN 偷換分母。

保留原 `9d4ff9...` 完整模組 oracle，SQLite online backup 固定全部 280,704
raw partitions、2,461 股票與 5,711 sessions（2003-08-01～2026-10-14）；
failure attempt receipts 也先固定，兩組不讀不同時點的 native receipts。
每輪保留 32 次 canonical durable upsert／effective readback、八次准入、
前後兩份完整清冊發布與全部 known-gap candidates。只有未發布、未使用的
中間完整清冊省掉，不用小資料 microbenchmark 代替完整控制循環。

| 同一完整控制循環，中位數 | 原路徑 | 修正路徑 |
| --- | ---: | ---: |
| Wall time | 30.603 秒 | 16.734 秒 |
| CPU time | 30.082 秒 | 16.126 秒 |
| 有效分區 | 280,607 | 280,607 |
| 完整輸出 SHA | `6b327394...348a` | 相同 |

ABBA 接受：1.82875×、wall 少約 45.3%、CPU 少約 46.4%。完整準備另計
5.558 秒；host 非獨占、OS page cache 未清。這是指定完整 index/control
workflow 的實測，不是整批 SDK／API 吞吐、全機降幅或全域最佳解。

原 563695 owner 跑完 256 attempts 自然離開；原 timer 啟動新 invocation
`ef9de857...8b1a`，新 tick owner 603516／11496712 的進度收據確實含六次
已執行准入、實測 1.136 秒及完整決定，不是只用磁碟 code label 證明採用。
04:41 獨立同 PID-generation／cgroup／canonical Python／cwd／code guard
回讀，attempt 129→130、success 83→84；三個最近來源的 receipt、全檔 SHA、
rows、bytes、source validator 全通過。六個交易／quote／FinMind／dashboard
保護 owner 與基準相同。是已採用的有界工程修正，不是全來源歷史完整。

483 個相關測試包含新增 49 個准入、來源狀態、minute/tick bounds、未知
symbol、非 session、退市、即時 transaction、08:00 retry reset、stale/future
runtime clocks 與 fresh-output guard。初次新測試把 reset 前一分鐘當成同一
週期、使用錯誤 coverage 欄名，修正 fixture/assertion 後通過；沒有放寬規則。
434／471／483 是重疊集合，不能相加。

### Bybit：完整恢復和增量更新分開證明

原 `bybit-fixed-cold-to-mutable-reco-20261014T193247-75a17f03`／439596 owner
於 04:28:46 成功完成。固定原 20260926T051004... release、manifest
`7af635eb...df6`、4,677 files／22.36 GB，complete canonical verification
後 durable 安裝到 `/srv/stockagent-live/data_bybit`，再原子更新 repo link。
沒有改用新 head、修改 cold、清 partial、啟動另一個 restore 或建立空來源。

正式 registered pipeline 隨即回到 source 1m hot-tail 增量。04:39:44～04:41:13
的目前報告：888 symbols、888 updated、793,581,794 logical rows；保留原
source＋hot-tail canonical reader，不重寫多年 base 來追加一分鐘。
這是 downloader 的 current run report，不是全歷史品質或供應商分母證明。

新增獨立 native probe 檢查固定 manifest/inventory、所有 original paths、
三個原件 SHA 及三組真實新 tail／canonical logical reader／OHLCV／closed
minute timestamps，不重新 materialize cold 或阻擋 live writer。17 個來源
恢復／reader guards 通過，包括零量合法、null/NaN/負量、未收盤／重複時間
與 generation change 拒收。工程第一輪在組裝結果時誤用不存在的
`ResolvedSnapshot.snapshot_id`，已改成 canonical manifest ID 並加完整
result regression；後續 live-only probe 正確拒收已退出的 native unit。
不把 watcher／unit 已退出的拒收當來源資料錯誤；加入 provenance/value 與
native-owner/lifecycle 的分階段結果，`--require-live-owner` 仍可要求完整
live 准入，native 未觀測時只回報 source readback、不能宣稱 lifecycle ready。
這些分界補兩個 regression，最後 68 項 focused tests 通過（49 FinLab＋19
Bybit／恢復）；與前述 500 項共享集合重疊，不相加。

04:57 新一輪獨立 stage probe 實際接受：固定原 manifest/inventory hash、
4,677 檔／6 dirs／22.36 GB original path scope、三個原件 SHA，以及三組
新 tail 與 canonical logical reader。新 tail 分別 35,664／35,662／35,662
rows，三者 latest data＝2026-10-14 20:54 UTC；closed minute、唯一時間、
有限值、OHLCV 邏輯與兩個物理檔 generation 前後皆相同，當時原生 source
owner 與 unit 也一致。Probe calls／source writes＝0。
這不是重新驗所有 evolving mutable file contents 或全部 cold objects，也
不是全部 symbols latest／全歷史無缺口／服務正常 stop 的證明；相關 claim
在正式收據明確 false。先前失敗工程收據保留。

正式 main 已 exit＝0、888 更新完成，但 unit 於 stop 階段 timeout。
journal 同時有 memory/control inotify watch ENOSPC；先處理 host 資源，
不能為得到綠燈重啟、拉高 stop timeout、跳過來源驗證或偽造 running。

### IDE／host：不讓資料湖變成數百萬 IDE 監看事件

04:45 實際辨識 4078945 為 VS Code `--type=fileWatcher`，不是模型或
StockAgent downloader；其 inotify fd28 有 **4,187,900** watches，PID1 有
166。kernel max_user_watches＝4,194,304（另外 instances＝1,024、queued
events＝32,768）。這不是磁碟沒空間；worktree 當時尚有約 50 GiB free。

採用符合實際安裝 VS Code 1.141.2 schema 的 workspace 設定，保留 editor
80-tab／dirty exclusion、原 search.exclude，補跨 request-root 的 data_*／
artifacts glob、明確 local absolute 規則及 JS/TS 的資料輸出排除。只排除
原始資料與 runtime artifacts 的遞迴 IDE watch；程式、configs、docs、test
仍維持監看，沒有刪檔、停 service、改 source API 或改 kernel 限制。
Microsoft 的[監看架構](https://github.com/microsoft/vscode/wiki/File-Watcher-Internals)
說明 extension request 與 workspace watch 的不同；官方
[relative-root issue](https://github.com/microsoft/vscode/issues/311908) 亦有相似
排除未生效回報，但不把別人的 issue 當成本機已證明的同一 bug。

04:49 原 PID 的完整 streamed fdinfo／10 秒 CPU 再樣本：依然 4,187,900
watches、13,836,959,744 bytes RSS（12.887 GiB）、0.99670 core，單一程序
就占 UID limit 99.8473%。配置寫入不等於 native 生效；需要 user 儲存
編輯後 `Developer: Reload Window`，再驗新 owner/watch/RAM/CPU 和服務
stop outcome。已提出非阻塞詢問；尚未強制關閉 IDE、未聲稱已省掉 13 GiB。
inotify ENOSPC 與 service stop timeout 的因果仍須釋放後對照。

04:27 五模式 completed-close readback 再接受，model forward／provider／
trading writes＝0、CUDA 未初始化；65／65 observed managed Linux processes
無 GPU handles。04:49 全清點 89 services／67 timers／3 paths，Discord 約
0.0128 core、520 MiB anon，FinMind 約 0.268 core。兩個 sampled batch 換代
的 CPU 欄為 null，不能填零、不能拿來算改善比例。未測 Windows VRAM、
遊戲 FPS 或下一次開盤 readiness。

05:01 最終同 generation 再回讀，五個 requested modes 全部接受；model
forward／provider calls／trading writes＝0、probe 未初始化 CUDA。另一個
native probe 觀測 68／68 managed Linux processes，GPU owner／device
handles＝0、觀測期間 service generation 未變。這是當次有界 Linux
readback，不是整機原子 snapshot、Windows VRAM 歸因或全服務最佳化完成。

這輪沿用工程偏好保留完整來源／輸出／語意，並依儲存維運技能只將已完整
驗證的固定來源安裝到 mutable workspace；沒有把 cold 或歷史 receipts 當成
可重寫暫存。目標仍 active，下一優先是 watcher 生效與 canonical source
服務正常完成／重試；其他服務仍依完整工作量瓶頸清冊逐一處理。

證據：

- [完整控制循環 A/B/B/A](../artifacts/benchmarks/running_services_20261013/finlab_minute_admission_20261015.WddHfnc6/trial/comparison.json)、[固定完整來源及 attempts](../artifacts/benchmarks/running_services_20261013/finlab_minute_admission_20261015.WddHfnc6/trial/fixed-input-context.json)
- [483 項相關回歸](../artifacts/operations/agent-workflow/runs/finlab-minute-gate-final-regress-20261014T203126-66c06b16/run.log)、[FinLab 原生 shortcut／同 owner／來源 readback](../artifacts/operations/running-services-optimization-20261013/finlab-native-minute-priority-20261015.json)
- [Bybit 完整原來源恢復](../artifacts/operations/running-services-optimization-20261013/bybit-mutable-recovery-applied-20261015.json)、[保留第一次工程結果組裝失敗](../artifacts/operations/agent-workflow/runs/bybit-recovery-independent-live-20261014T203436-dccb8ebf/run.log)、[保留 native 已退出的拒收](../artifacts/operations/agent-workflow/runs/bybit-live-recovery-proof-final-20261014T204114-e2608c0e/run.log)
- [Bybit 固定 scope／原件／增量值與 native owner 分階段驗收](../artifacts/operations/running-services-optimization-20261013/bybit-restoration-source-readback-20261015.json)、[500 項正式共享回歸](../artifacts/operations/agent-workflow/runs/finlab-bybit-final-shared-tests-20261014T205111-39f5cc85/run.log)
- [原生 watcher 壓力與 settings identity](../artifacts/operations/running-services-optimization-20261013/vscode-watch-pressure-after-settings-20261015.json)、[04:49 完整服務資源清點](../artifacts/operations/running-services-optimization-20261013/all-services-after-minute-gate-20261015.json)
- [收盤五模式 cache-only 回讀](../artifacts/operations/running-services-optimization-20261013/postclose-five-after-minute-gate-20261015.json)、[65／65 managed GPU-cold](../artifacts/operations/running-services-optimization-20261013/managed-gpu-after-minute-gate-20261015.json)
- [05:01 最終五模式同 generation CPU-cache 驗收](../artifacts/operations/running-services-optimization-20261013/postclose-five-final-minute-gate-20261015.json)、[68／68 managed Linux GPU-cold 再驗](../artifacts/operations/running-services-optimization-20261013/managed-gpu-final-minute-gate-20261015.json)

## 10/15 05:05–05:33：全來源 hot-tail 清冊消除日期字串往返及原生驗收

05:05 原 canonical 20 秒級普查保留 89 services／67 timers／3 paths。
registered intraday 正在真實更新，取樣 CPU 約 5.4377 cores、匿名記憶體
約 1,328 MiB；FinMind 約 0.235 core、Discord 約 0.0192 core。CPU PSI some
avg10＝47.91，host 非獨占。不能把資料處理成本或其他工程測試當成盤後
模型空轉，也不能從一次 batch 取樣推估全天耗用。

一輪 20 秒原生 Bybit profiler 在程序自然完成後未通過同 owner gate；183
samples／3 errors 只當定位線索，不當正式效能／部署證明。後續八秒 profile
在原 native owner 上正常結束，108 samples／零 errors、同 PID generation
與 unit；沒有重開 source。兩次保留原紀錄。

三來源的 `_load_logical_existing*` 都對 tail 日期做
parse→strftime→sort→parse，再只取比 base latest 新的筆數與最早時間。
這些 overlap facts 不需要格式化或排序。新增共用 `tail_newer_summary`：
只有非 NULL、長度 19、完整匹配原生 writer 秒級格式且全部成功解析的字串
才走單次 Polars 原生解析；未知格式、NULL、時區、次秒、其他 dtype 仍
呼叫各 provider 的原 normalization／post-parse。重複時間仍重複計數，
base cutoff 不四捨五入，不用 footer 推算新筆數、不新增「歷史完整」證明。
價格／成交量／單位、provider 範圍、配額、請求、retry、交易日與收盤 gate
皆未改。參數與格式依 [Polars 官方文件](https://docs.pola.rs/api/python/stable/reference/series/api/polars.Series.str.to_datetime.html)
核對；使用現行 Polars 1.38.1／PyArrow 25.0.0，沒有盲目升級相依套件。

先保存原三 collector 和原 hot-tail 模組，未接線原型在記憶體只替换相同
日期 overlap block，其餘完整 function 保留；canonical production 在完整
benchmark 接受後才接回共享入口。

固定完整 stored universes：Bybit 896／OKX 505／Binance 575，共 1,976
base paths，包含 retained historical symbols，沒有縮成目前 provider 名單。
多年 base 合计約 176 GB，只保持唯讀參照及完整 dev/inode/size/mtime/ctime
前後 guard；全部可變 tail 固定到私人快照，共 1,678,109,330 bytes。
copy 前後 generation 不同便拒收；不 hard-link mutable source、不修改 cold、
不為 benchmark 鎖住 native writer。這不是全 host 的原子快照或全部歷史
values SHA 重驗。快照準備 5.496 秒另計，32 GiB 磁碟 reserve 保留。

每輪完整讀回全部 information tuples（rows、首末時間、interval、error），
保留初始／最終完整清冊與 JSON durable 發布。A/B/B/A 的四個輸出 SHA 都是
`f723b78c85571c1b3f3c258e25f83eb87cdab7643308f072e0793c5c4c192d5d`，
全部 base generations 與 pinned tail SHA 前後相同，native API／source writes＝0。

| 完整三來源 information/control workflow 中位數 | 原路徑 | 新路徑 |
| --- | ---: | ---: |
| Wall time | 123.227 秒 | 24.922 秒 |
| CPU time | 122.207 秒 | 25.579 秒 |
| 完整 stored symbol 範圍 | 1,976 | 相同 |
| 完整輸出 SHA | `f723b78c...92d5d` | 相同 |

Wall 少 79.78%、CPU 少 79.07%、吞吐 4.94456×。reference wall 範圍
104.68–141.78 秒，candidate 22.32–27.52 秒；host 工作並行、OS cache 未清，
不能把數字稱為全球最優、完整 HTTP／merge／訓練加速或 native 全機降幅。

89 個新語意測試含原生／fallback、時區／次秒、NULL／錯誤／重複時間、
完整 physical loader／相同異常及來源 bytes 不變。先前較廣集合的 routing-only
mock 未提供已存在的 `execution_excluded_dates`／`canonical_string_dates`
audit 欄位，原結果為 KeyError；只補 mock 契約，不放寬 materializer，
修正後 296 項共享測試通過，production 接線後 358 項相關回歸通過。
最終版本再以 389 項共享回歸驗證，28.79 秒全部通過，包含 daily materialize
與 perp scope。上述集合重疊，不相加。另九項 observer guards 驗證：同名 frame 不代表
正確來源、不同 date projection 拒收、未觀測／owner 换代不能升格成功、
三個實際 native frame 與 fresh evidence gate；不是全 repository suite。

新 verifier 比對 preserved 完整函式／原 physical loader／normalizer AST
與已接受原型，只觀察原 timer 的 fresh canonical Python/cwd/cgroup/boot/PID
generation；五秒低頻並行 profiles 要有實際 `tail_newer_summary` frames、
前後同 owner／code／六個保護 owner。沒有三個有效 owner 時保留
`awaiting_native_helper_execution`，最多等 180 秒；不把短批次已完成當資料
錯誤、不重啟下載。正式 source value readback／all-history／服務 stop outcome
仍是不同界線，後两項不因本輪 benchmark 變成完成。

05:29:14 正式 registered timer 的三個 fresh native collector 已獨立驗收。
當時 unit InvocationID 為 `f8c01491cb0848aa97b4c9f7074ffa84`，三個 Python
程序皆在 producer code 的 05:20:10 世代之後啟動；五秒 profile 的實際
helper samples 為 OKX 53、Binance 50、Bybit 104，profiler exit 皆為零。
前後 boot／PID／start ticks、collector code 及六個保護 owner 不變，完整
live function／physical loader／normalizer AST 與 accepted prototype 相符。
工程未呼叫 provider、改寫 source 或重啟下載；此證明是入口實際採用，
不是全部 source values 無缺口，也不是 unit 停止生命週期驗收。

05:32:38 另外重新從 canonical consumer 回讀五個當前收盤模式，5／5
接受，model forward＝0、provider calls＝0、trading writes＝false、CUDA
initialized＝false。05:32:32 的 managed Linux GPU audit 可讀 54／54
程序，GPU owners／device handles＝0，probe 未初始化 CUDA 或改變服務。
Windows 所有顯存配置仍未歸因，不能把這次 Linux 接受證明說成遊戲 FPS
或整卡顯存保證。快取仍以實際輸入／模型／程式世代作失效判斷，不因
「收盤只算一次」忽略後續真正發布的新資料。

05:28 VS Code 舊 4078945 仍在，RSS 約 12.89 GiB；registered main 正常完成
但 stop 仍 timeout。尚待 user `Developer: Reload Window` 後重驗資源與
生命週期因果。全服務目標 active，不把這個待決事項擴成其他工作的 blocker。
工程偏好維持原始值／時鐘／整段輸出，儲存維運技能限制私人快照與唯讀
base 參照，未回收／改寫 source、cold 或 prior receipts。

05:38:04 再完成原 canonical 普查，仍為 89 services／67 timers／3 paths，
resource window 21.333 秒。當時前三項 CPU 為 legacy partitions return
0.660 core、OpenBB L1 compaction 0.624 core、TX history backfill 0.562 core；
FinMind 0.312、FinLab 0.085 core。CPU PSI some avg10＝1.74、memory＝0.26、
I/O＝12.30。這些工作狀態與 05:05 不同，不能把 PSI 下降當本輪全機 A/B
改善；其來源處理、hash／publication 與 idle 成本須再各自定位。舊 VS Code
watcher 的 RSS 仍約 12.89 GiB，registered main＝0／stop-sigterm 仍待
生命週期讀回；沒有關閉服務或用提高 inotify limit 掩蓋監看數量問題。

證據：

- [05:05 完整服務清點](../artifacts/operations/running-services-optimization-20261013/all-services-next-bottleneck-20261015.json)
- [完整 A/B/B/A 與輸出一致性](../artifacts/benchmarks/running_services_20261013/cex_timestamp_overlap_20261015.vmdjKiUu/trial/comparison.json)、[固定全部 source/tail inputs](../artifacts/benchmarks/running_services_20261013/cex_timestamp_overlap_20261015.vmdjKiUu/trial/fixed-inputs.json)
- [保留自然完成而同 owner gate 拒收的 profile](../artifacts/operations/agent-workflow/runs/bybit-active-next-hot-profile-20261014T210654-f220dca7/run.log)、[八秒同 owner 原生 profile](../artifacts/operations/agent-workflow/runs/bybit-pre-overlap-bounded-profil-20261014T211840-1e2282d6/run.log)
- [296 項較廣語意回歸](../artifacts/operations/agent-workflow/runs/cex-tail-overlap-semantic-regres-20261014T211537-4a5f6bd1/run.log)、[接線後 358 項相關回歸](../artifacts/operations/agent-workflow/runs/cex-tail-overlap-final-verified--20261014T212412-8af14af0/run.log)
- [三個 native collector 實際採用](../artifacts/operations/running-services-optimization-20261013/cex-tail-overlap-native-generation-20261015.json)、[最終 389 項共享回歸](../artifacts/operations/agent-workflow/runs/cex-tail-overlap-complete-regres-20261014T213228-f90a7146/run.log)
- [05:32 五模式當前收盤快取](../artifacts/operations/running-services-optimization-20261013/postclose-after-cex-native-20261015.json)、[05:32 managed Linux GPU 句柄清點](../artifacts/operations/running-services-optimization-20261013/managed-gpu-after-cex-native-20261015.json)
- [05:38 全部服務下一輪資源基線](../artifacts/operations/running-services-optimization-20261013/all-services-after-cex-native-20261015.json)

## 10/15 05:40–06:04：OpenBB 全量成員統計、完整狀態發布與原生採用

上一輪屬 progress：三個 CEX 真正 native 入口與最終 389 回歸已接受。
這一輪先從 05:38 全服務基線接續。legacy partitions return 的同一個原生
批次已正常結束，沒有重啟回傳或冷操作。OpenBB 原 05:36:49 owner
`d606eee187b641a69b086bfe5ec26ae1` 則完整完成 178.99193 秒：
new／stale／failed／pending 為零、185,963 derivative footers 真正開啟；
124.162 秒 metadata／source audit、29.353 秒 unassigned selector、
13.397 秒 status member count、14.954 秒 status publication。晚間已採用的
reflection cache 繼續有效，並不是讀相同頁面就可取消檔案驗證。

逐 endpoint 的 member cardinality 必須持續核對：缺少、額外、錯置成員
都要在替換 status 前拒收。本輪改成單一 recursive statement，利用既有
endpoint index 尋找下一個完整 endpoint，再原生 COUNT 每一個成員；
仍數齊全部 7,327,309 records，只移除每列 GROUP BY 的分組機器操作，
沒有拿 segments 的 SUM 當成 members 的校驗，也沒有新增 DB index、
快取 member 有效性、改來源／rows／units 或增加 provider calls。
MIN/range seek 的依據見 [SQLite 官方最佳化](https://www.sqlite.org/optoverview.html#the_min_max_optimization)；
實際 query plan 與收益另外以目前 SQLite、同一真實索引測試，不以文件
保證 wall time。NULL-capable／未知 type、COLLATE、partial／錯誤／不存在
index 或 endpoint filters 仍走原 SQL，缺表與未知 DB error 不偽裝成空值。

測試中的 guard 涵蓋 empty、2,100 endpoints（沒有硬編小名單）、Unicode、
NUL、BLOB、case、重複、filters／SQL 特殊字元、NULL／NOCASE／partial
index fallback、read-only bytes 與 caller transaction 不變。所有成員
異常的既有 publication-before-replace checks 仍在共享回歸中執行。

先保存完整原 compactor，使用既有 23,536,005,120-byte 唯讀 private
archive，不另複製／回收原件；32 GiB 安全 reserve、native 同 2816 MiB
MemoryHigh／3 GiB MemoryMax、兩個 source threads 保留。私人 manifest
全檔 SHA／signature、完整 implementation closure 前後固定，沒有 production
SQL write、API、模型或排程修改。完整 status function 原型只換 member
query，另外加版本化 observational `member_count_audit`；原所有業務欄位、
membership reject、full JSON／Parquet durable publication／逐筆回讀都保留。
它不是 footer/source-contract/value audit 或整段 compaction 的對照。

首次完整 A/B/B/A 所有輸出 SHA 相同，但 reference 第一輪 segment query
12.639 秒、後輪 0.157 秒；平均 64.07% wall 少幅混入 OS cache 變化，
**拒絕以此作正式可歸因收益**。另保留單 SQL screen，亦沒有將 23.324 秒
冷讀與 0.540 秒暖讀稱成 43 倍。其結果只用於定位與完整同值驗證。

後續分開完整暖機 15.194 秒，再以 A/B/B/A＋B/A/A/B 八輪量測整個 status
發布／全 Parquet 與 JSON 讀回。完整 output SHA 同為
`9ebb1cf364b2be69325a125327cc402e021573303301bfd41757c9f851e1fb2a`；
Parquet bytes、全部 endpoint／source／compacted counts 與已有業務欄位一致。
實際 attempt clocks／private roots 與新的 policy 欄位屬 observational，
不把它們當數據內容相等或反填原始發布時間。

| 完整 status publication/readback 平均 | 原始分組 | endpoint seeks |
| --- | ---: | ---: |
| wall 秒 | 2.298846 | 1.945628 |
| CPU 秒 | 2.226446 | 1.938485 |
| 全 source files | 7,327,309 | 相同 |
| 全 source rows | 334,949,985 | 相同 |
| endpoints／active segments | 67／185,963 | 相同 |

wall 少 15.365%、CPU 少 12.934%。host 非獨占、OS cache 未刻意清空；
第一個暖 A 仍 3.128 秒，而後半 B/A/A/B 的均值 wall 約少 10.7%。
這是保留全部 scope 的較小實測收益，不能稱整機、全下載／全整理的倍率，
更不能說完整 179 秒工作已降到 2 秒。CPU／RAM/GPU 的原本負擔另驗。

05:55 接回原 `_write_status`，完整函式 AST 等於已接受原型；source
selection 及 `_mark_stale_segments` 的完整函式未變。24 項新 guards 與
362 項共享回歸通過、Ruff／diff check 接受；另 18 項 observer guards
拒絕 running／舊／失敗 owner、incomplete publication、只貼 policy label、
部分或錯置 endpoint counts、落後 row totals。測試集合有重疊，不相加
成不同 tests，亦不宣稱 repository 全測試完成。最終相同集合加入 observer
guards 後為 380 passed／45.40 秒。

儲存維運技能促使本次先保存確實 terminal 的原 native attempt/publication
與 private fixed archive，工程偏好促使在冷暖混合收益之外完成平衡全流程。
05:58 確認既有 unit 已 inactive／MainPID＝0 後，只提前啟動同一正式服務
一次，InvocationID `2785d4184bbe47058a7a250aaba00383`。沒有重啟正在跑的
owner、另開 compactor、提高 native limits、刪除 L0/cold 或改動 broker。
06:00 仍為相同實際 native owner；06:01:40 真正成功結束，完整 180.829558
秒、footer audit 142.998 秒、unassigned 13.093 秒、status 13.223 秒，其中
member count 11.314 秒。與原 178.992 秒的整段相近；host／I/O 狀態不同，
這不是整段加速證明。沒有改排程定義；這次提前執行會按既有 OnUnitInactive
規則自然移動下一次觸發時間。

06:03:40 獨立 observer 接受新 terminal Invocation、目前完整 status AST
與已測原型一致，以及未變的 source selector／footer function。重新用原
GROUP BY SQL 直接數齊全部 members，所有 67 endpoint counts 與新 policy
執行證據、JSON／Parquet／attempt source counts 全相符，7,327,309 files／
334,949,985 rows／185,963 active segments 保留。probe 未寫 source DB、
呼叫 provider、初始化模型或變更服務；不是全歷史 values／bitrot 稽核。
六個事前保存的保護 owner 在讀回時仍相同，不能把 policy label 取代獨立
資料／完整函式／正式工作接續證據。

06:03:50 另回讀全部五個當前收盤模式，5／5 accepted，model forward＝0、
provider calls＝0、trading writes＝false、CUDA initialized＝false。
06:03:41 的 Linux GPU audit 64／64 managed processes 都可讀，owners／
device handles＝0；Windows 所有配置／整卡／遊戲 FPS 仍未完全歸因。
舊 VS Code 4078945 的 RSS 約 12.88 GiB，未重新載入、未再掃四百多萬個
watches 來製造額外負擔；尚待使用者 reload 後驗釋放與 stop timeout 的
因果。下一輪主要仍定位 footer metadata／source selector 的完整成本，
全服務目標保持 active。

證據：

- [原 native 全 attempt 與 publication](../artifacts/operations/running-services-optimization-20261013/openbb-before-status-counts-native-20261015.json)
- [保留單 SQL screen](../artifacts/benchmarks/running_services_20261013/openbb-member-count-index-screen-20261015.json)、[混合冷暖完整對照，未作正式倍率](../artifacts/benchmarks/running_services_20261013/openbb_complete_status_counts_20261015/comparison.json)
- [暖機分開的八輪完整比較](../artifacts/benchmarks/running_services_20261013/openbb_warm_balanced_status_counts_20261015/comparison.json)、[完整 immutable code/source capture](../artifacts/benchmarks/running_services_20261013/openbb_warm_balanced_status_counts_20261015/fixture.json)
- [362 項共享回歸](../artifacts/operations/agent-workflow/runs/openbb-status-counts-shared-regr-20261014T215523-37b12edc/run.log)、[只啟動一次既有 inactive service 的交接](../artifacts/operations/running-services-optimization-20261013/openbb-status-counts-native-handoff-20261015.json)
- [最終 380 項共享回歸](../artifacts/operations/agent-workflow/runs/openbb-status-counts-final-share-20261014T215959-f92ab159/run.log)、[原生完整採用及原 SQL 全量核對](../artifacts/operations/running-services-optimization-20261013/openbb-native-status-counts-accepted-20261015.json)
- [六個保護 owner 獨立讀回](../artifacts/operations/running-services-optimization-20261013/openbb-status-counts-protected-readback-20261015.json)、[五模式當前收盤快取](../artifacts/operations/running-services-optimization-20261013/postclose-after-openbb-status-counts-20261015.json)、[Linux GPU 句柄清點](../artifacts/operations/running-services-optimization-20261013/managed-gpu-after-openbb-status-counts-20261015.json)

## 10/15 06:04 起：OpenBB 全量 footer I/O、平行成本與盤後快取再驗

上一輪是實際 progress：原生完整狀態發布與原 SQL 全量獨立讀回已通過。
本輪保留既有全量 metadata／physical reflection cache 的所有 source SQL、
row count、Arrow schema、physical descriptor、完整 metadata、每檔 signature
與 stale-state 處理。減少的只是 constructor 為「讀取 payload 欄位」建立、
此 metadata-only caller 根本不用的 nested-column prefix index，以及逐檔
filesystem inference。不是快取某檔「已有效」或跳過當前 footer。

`FooterOpener` 一個 attempt 共用 LocalFileSystem；metadata-only 子類禁止
所有 payload read 方法。私有 hook 僅在目前實測 PyArrow 25.0.0 與兩個
constructor/hook 原碼 SHA 完全相符時啟用，未知版本／修改版用公開 reader。
URI／permission／bad footer 的非資源例外回原 reader，MemoryError 不重試，
不把 memory pressure 當 stale source。未知 shape／路徑仍維持原錯誤語意。
公開能力界線見 [ParquetFile 文件](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html)
與 [LocalFileSystem 文件](https://arrow.apache.org/docs/python/generated/pyarrow.fs.LocalFileSystem.html)；
私有 hook 的收益不是文件保證，而是本機完整測試與原碼 gate 的結果。

另實作但未正式部署的 bounded prefetch：最多 2 workers／4 pending readers，
按原 SQLite source 順序交回 caller；SQL、proof/cache、state apply 不跨 worker，
錯誤／取消會 close 所有自己的 reader 並 join 自己的 pool。pending 是數量上界，
不是 decoded memory bytes 上界。Python 3.12 的 eager map 不能拿來宣稱有界，
依 [futures 官方文件](https://docs.python.org/3.12/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor)
與實際錯誤注入驗證。平行不是目的，增加 CPU 卻未改善均值就不採用。

完整 scope 為全部 185,963 successful derivative segments、50,722 SEC view
source proofs。沿用唯讀 private archive，只複製約 111 MiB 的 segment table，
不另複製 23.536 GB archive 或 L0 原件；至少 30 GiB reserve。所有 11 個
執行 code closure 原碼、manifest 全檔 SHA、每檔 source signatures 前後固定，
每輪 fresh process，native 同 MemoryHigh 2816 MiB／MemoryMax 3 GiB、
Nice＝15、CPU／IOWeight＝10、threads＝2，沒有 GPU／provider calls。

暖機 116.956 秒／CPU 64.839 秒單獨列出，不算收益；之後鏡像八輪：

| 次序／完整 audit variant | wall 秒 | CPU 秒 |
| --- | ---: | ---: |
| 1 adopted | 135.099 | 75.570 |
| 2 local filesystem | 134.016 | 73.264 |
| 3 metadata-only | 113.567 | 62.803 |
| 4 bounded prefetch | 106.991 | 120.093 |
| 5 bounded prefetch | 139.652 | 120.256 |
| 6 metadata-only | 128.064 | 77.518 |
| 7 local filesystem | 102.099 | 77.480 |
| 8 adopted | 144.698 | 86.679 |

八輪完整結果 SHA 都是
`0dd42c46b760617764c5c9711b75e0762529074dfa9aca5e6fe38b3c42770619`。
adopted 平均 wall／CPU＝139.898／81.125 秒；metadata-only＝120.816／70.160，
少 13.641%／13.516%；local-only＝118.057／75.372，CPU 只少 7.091%。
以降低常態 CPU 成本為本輪首要目標，選 metadata-only 而不是單純最短 wall。
prefetch 平均 123.322／120.175，CPU 比 adopted 多 48.136%，也比 serial
metadata-only 慢；拒絕部署。metadata-only RSS 約多 0.3%，不稱 RAM 改善。
這是完整 footer phase，不含 source-member/journal audit、views 或整段 compaction；
不是所有 payload values／bitrot 稽核、API 下載效率或全機加速證明。

host 非獨占、未清 OS page cache；06:30 daily job 與自然 06:32 compactor
仍照常執行，因此保留逐輪波動，不以單次 106 秒宣稱穩定效能。06:32 的
舊 reader 正常 owner `644006b340c64336a8c1ce84aae673d5` 完整 171.153342 秒，
footer 118.686 秒，尚不是新 reader 的正式採用證據。

完整 controls 結束後只補了未採用 prefetch 的 close-error teardown，及不被
native 匯入的 benchmark factory instrumentation。全部 frozen 11-code closure
仍保存／逐檔 SHA 驗證；實測 serial helper 的完整 AST 沒改。獨立 observer
只允許 native import 與 footer factory／observational counter 三處變化，其他
完整 native module AST、selector、source SQL／units／state semantics 必須相同。
共用回歸涵蓋 plain／nested／int96／timezone／geo／field IDs／UUID／empty、
版本／原碼 fallback、壞檔／missing／directory、MemoryError、bounded queue、
close 失敗不遺漏 reader／pool、完整 source proofs 與既有 compaction lifecycle。
最終 18 個相關 modules 合計 **430 passed／53.36 秒**，Ruff／修改範圍 diff
check 通過。focused sets 有重疊、不相加、不宣稱 repository 全 suite 完成。

依儲存維運技能先保存舊 native attempt/publication、六個保護 owner 與資源
limits，再於 06:46 僅啟動同一個 inactive unit 一次，新 InvocationID
`00a9af62170c430a9805a89d225136d2`；沒有重啟 active owner／另開 writer，
沒有冷庫／L0 刪除、交易／配額或 timer 定義修改。06:48 新 owner 正常結束，
完整 120.913518 秒、footer 95.111 秒、unassigned 9.590 秒、views 10.591 秒、
status 4.155168 秒。這是本次正式工作觀察值，與舊整段 171.153342 秒的差異
包含 host／I/O／其他 phase 波動，不稱其降幅為單一 reader 的因果收益。

06:48:52 原 canonical observer 獨立接受 terminal Invocation、全部 185,963
來源集合六欄位 digest、每檔 file signature、原碼／runtime closure、完整
native module AST 與 JSON／全 Parquet readback：7,327,309 source files、
334,949,985 rows、67 endpoints 都相符，new／stale／failed／pending 為零。
實際 metadata-only opens＝185,963、canonical retries＝0；physical reflection
cache 為 50,710 hits／12 misses、12 entries／66,789 retained serialized bytes，
仍低於 64 entries／2 MiB budget，沒有 validity cache、bypass 或 eviction。
六個保護交易／行情 owner 與 native limits 完全相同；新 observed policy label
不是單獨的採用證明。observer 沒有 source／service／trading write、provider
call、GPU 或模型匯入，不把來源統計／schema parity 說成 payload value 稽核。
沒有改 timer 定義；本次提前啟動依原 OnUnitInactive 規則自然移動下次觸發。

06:47 的 canonical completed-session consumer 再次 5／5 accepted，
model forward＝0、provider calls＝0、trading writes＝false、CUDA 未初始化；
同世代收盤只讀 CPU cache。新來源／模型／程式世代仍必須拒用舊鍵，不把
「一次 cache」解釋成凍結之後的真實更新。68／68 managed Linux processes
可讀且 GPU owners／device handles＝0，這是 sampled handles，不是全卡 VRAM。

06:36 Windows 兩次唯讀 WDDM 各有 48 process-adapter rows，全部名稱可讀，
沒有標記 StockAgent reference；Sunshine／DWM／MAA／Code 等 counters 可見。
整卡另取樣約 3,812 MiB、4% utilization，不能將 counters 相加或把 DWM
共享 surfaces 當其私人顯存，更不能單憑名稱消除 WSL／Linux attribution 邊界。
參見 [Microsoft WDDM 計數說明](https://devblogs.microsoft.com/directx/gpus-in-the-task-manager/)
與 [counter accuracy 限制](https://learn.microsoft.com/en-us/troubleshoot/windows-client/performance/gpu-process-memory-counters-report-wrong-value)。
未停止任何 Windows 應用；沒有全 Windows 歸因或遊戲 FPS 改善證明。

06:31 全服務基線為 89 services／67 timers／3 paths；daily job 約 4.39 cores、
7.86 GB anonymous RAM；public status snapshot 仍每約 30 秒使用 14–15 CPU
秒，下一輪優先處理完整 inventory decode／encode／dirty-source rebuild。
06:50 正式採用後再完整清點為 88 services／67 timers／3 paths，數量包括
當時 transient probe owners，不能以一個 unit 的差異推論停止正式服務。
此時 daily 約 2.08 cores／0.575 GB anonymous RAM，是同一次工作的不同
phase，不稱其與 06:31 差異為本輪最佳化效果。snapshot 仍約 0.916 GB
anonymous RAM、Discord 約 0.548 GB；整卡 3,810 MiB／5% utilization。
六個 localhost projection GET 都能讀到有效回應，但 day-trade／data-monitor
仍回報 degraded，不能把 HTTP 200 延伸成所有資料完整或 live readiness。
VS Code 原 12.88 GiB watcher 程序仍待使用者 Reload Window，沒有再次全掃
百萬 watches、增加 kernel limit 或終止編輯器；stop timeout 仍未證明修復。
全服務最佳化目標保持 active。

證據：

- [完整 source/code capture](../artifacts/benchmarks/running_services_20261013/openbb_complete_footer_io_20261015/fixture.json)、[暖機分開與八輪 full-scope comparison](../artifacts/benchmarks/running_services_20261013/openbb_complete_footer_io_20261015/comparison.json)
- [最終 430 項共用語意回歸](../artifacts/operations/agent-workflow/runs/openbb-footer-final-shared-regre-20261014T224313-cf65af74/run.log)、[同一 inactive native unit 交接及舊完整收據](../artifacts/operations/running-services-optimization-20261013/openbb-footer-io-native-handoff-20261015.json)
- [完整原生採用與全來源集合／signature／JSON-Parquet 獨立驗收](../artifacts/operations/running-services-optimization-20261013/openbb-native-footer-io-accepted-20261015.json)、[六個保護 owner 與原資源限制讀回](../artifacts/operations/running-services-optimization-20261013/openbb-footer-protected-readback-20261015.json)
- [目前五模式盤後 CPU cache](../artifacts/operations/running-services-optimization-20261013/postclose-during-openbb-footer-native-20261015.json)、[68 程序 Linux GPU handles](../artifacts/operations/running-services-optimization-20261013/managed-gpu-during-openbb-footer-native-20261015.json)
- [全服務實際資源基線](../artifacts/operations/running-services-optimization-20261013/all-services-during-footer-comparison-20261015.json)、[Windows WDDM／整卡的分離觀察](../artifacts/operations/running-services-optimization-20261013/windows-gpu-during-footer-comparison-20261015.json)
- [06:50 正式採用後全部服務／資源／六個公開讀取再量測](../artifacts/operations/running-services-optimization-20261013/all-services-after-openbb-footer-native-20261015.json)

## 10/15 07:03：全資料清冊 GC 方案實測拒絕，不關閉全域 GC

正式 snapshot clean hit 仍完整驗證 68,042 個來源路徑；乾淨一輪約 4.83 秒，
但來源變動時 record cache 的 131 MB JSON 解碼、統計陣列、彙整與持久寫入
可讓單輪升到約 19 秒 CPU／1.46 GB peak。兩種 phase 不能相減宣稱改善。
完整 cProfile 診斷涵蓋 decode、逐檔驗證、aggregation、atomic cache/index
writes；其中 fixture capture／輸出一致性計算另外標明，不當正式單輪吞吐。

先沿原 writer、strict metadata schema、未知型別 stdlib fallback，僅改四個
暫存 validation Struct 的 GC tracking 做私有試驗；沒有 `gc.disable()` 或
跳過欄位。ABBA＋ABBA 八輪都驗證全部 68,042 個真實來源的 frozen identity，
cache bytes 與完整值輸出全部相同；50 項相關語意測試通過。然而 tracked
中位 wall／CPU＝4.968／4.776 秒，untracked＝5.334／5.244 秒；CPU 多
9.787%，沒有穩定收益，**拒絕正式採用**。正式 Struct 仍維持 GC tracking，
既有全域 GC 不變；`verified` 只證明輸出／覆蓋一致，不代表效能接受。

下一個候選改測大量 `non_null` 陣列的延後展開：先完整驗證每個整數／null，
再保留緊湊表示；所有原始 JSON keys、缺省／顯式 null、任意精度整數、
未知欄位與 legacy fallback 都須保持。只有特徵計算實際需要時才展開，
以完整 record＋feature 計算及 JSON 量測，不以單獨 decoder 微測試選型。
在完整 CPU 中位成本至少少 5%、完整 cache／feature bytes 全相同前，不接入
正式 reader／writer；不以全域 GC、漏驗證或減少欄位換效能。

證據：

- [完整 record profiling](../artifacts/benchmarks/running_services_20261013/data-monitor-full-record-before-20261015.json)
- [GC 八輪同輸入及原碼／實際 layout 核對](../artifacts/benchmarks/running_services_20261013/data-monitor-record-struct-gc-abba-20261015.json)
- [82 項延後統計與 legacy／GC 語意測試](../artifacts/operations/agent-workflow/runs/data-monitor-deferred-semantic-v-20261014T231336-ae77ec57/run.log)

## 10/15 07:16：延後統計展開亦拒絕，完整下游不能省略

相同 68,042 個來源／89,097 個特徵，131,345,213 bytes canonical seed，
全部 source identities frozen、source count 不縮減。延後展開候選以原碼
schema 推導嚴格 proof，不新增第二份手維護 schema；完整 int/null 驗證後才
存 canonical raw fragment，未知／legacy 仍回原讀寫。不使用 float 儲存統計。
feature zip 每個 member 只展開一次，沒有每欄重解碼或全域永久 retaining。

82 項相關測試通過；完整八輪的 cache bytes、全部 feature JSON bytes、
任意精度數值與所有來源驗證相同。但完整 record＋feature JSON 流程：
eager 中位 wall／CPU＝7.882／7.867 秒，deferred＝8.670／8.642 秒，
CPU 約多 9.9%，未達先定的改善門檻，**拒絕正式 reader／writer 採用**。
candidate encoder 雖較快，decode 多輪 raw mapping／逐欄轉換及 feature 再
展開抵消收益；不能只報單獨 encode 的好看數字。private benchmark 整個
process peak 同時含 fixture、前後拷貝與 parity，不能拿來稱每 variant RAM。

正式 JSON v10、native reader／writer、GC 與來源更新維持。候選只留下可
重現的私有試驗與精確型別 guard，不產生 deferred cache 或替代服務。
下一個更小的候選針對 profile 的 326 萬次 Mapping ABC instance check：
目前 cache 的普通 dict 先用 C-level dict 檢查，其他 Mapping 仍完整走原
ABC fallback，不刪除 validation。以完整同輸入 record＋feature 發布量測，
不以 `isinstance` 微測試宣稱整個服務收益。

證據：[完整八輪 record＋feature 結果及代碼指紋](../artifacts/benchmarks/running_services_20261013/data-monitor-deferred-full-abba-20261015.json)。

## 10/15 07:32：dict 快徑的完整計算實測與原生世代讀回

完整 profile 有約 326 萬次 Mapping ABC instance check。JSON cache 本來
就是 ordinary dict，先做 native dict 檢查，其他 UserDict／MappingProxy／
Mapping implementations 仍走原 ABC fallback。只改 runtime guard tuple，
不替換 annotation 的 Mapping、不省略任何 schema／source identity 驗證，
不改數值、缺值、JSON v10、membership v1、欄位或來源範圍。
`isinstance` tuple 與 Mapping 的公開語意見
[Python builtin 文件](https://docs.python.org/3.12/library/functions.html#isinstance)、
[Mapping ABC 文件](https://docs.python.org/3.12/library/collections.abc.html#collections.abc.Mapping)；
效能取自本機完整流程，而非文件推論。

第一輪全 namespace 鏡像測得較大收益，但 decode／encode 各階段也隨主機
負載顯著波動，不能全部歸因於型別快徑。把正式碼改成清楚的 runtime tuple
並移除 live module 對已拒絕 deferred prototype 的 import 後，再以最終碼
做第二輪 ABBA＋BAAB。deferred prototype 移至 scripts 的 benchmark-only
module；正式 `_COUNT_VECTOR_TYPE` 仍只有 list，沒有留下可被 JSON 啟用的
另一套儲存格式／環境開關。

最終固定 seed 131,288,104 bytes、68,043 unique source paths、89,097
features；每輪完整 204,129 個 stat/absence observations，逐檔五部分身分
仍重驗。包含完整 record decode／aggregation／cache/index atomic write，
完整 feature scan／整數還原／JSON；不含 imports、directory discovery、
live/cold I/O、footer refresh、全 provider label projection、網路或瀏覽器。
沒有 OS cache 清空或暫停其他工作；原 input／code SHA 前後相同。

| 順序 | 完整 variant | wall 秒 | CPU 秒 |
| --- | --- | ---: | ---: |
| 1 | 原 ABC | 6.244 | 6.212 |
| 2 | dict first | 7.468 | 7.466 |
| 3 | dict first | 8.214 | 8.192 |
| 4 | 原 ABC | 7.252 | 7.219 |
| 5 | dict first | 7.282 | 7.271 |
| 6 | 原 ABC | 11.532 | 11.019 |
| 7 | 原 ABC | 12.027 | 11.675 |
| 8 | dict first | 12.031 | 10.854 |

完整 cache bytes、feature JSON bytes 與數值輸出八輪全部相同。
CPU **平均 9.031292→8.445763 秒，少 6.483%**；wall 平均
9.263430→8.748691 秒，少 5.557%。CPU 中位數少 14.145%，但每輪有
波動且頭一個 ABBA block 沒有變快；主摘要採較保守的完整平均，不把
首輪的大幅下降或中位數當固定 SLA／因果純效應。這是減少確知重複檢查
並通過輸出驗收的 bounded optimization，不是全域最佳解證明。

368 項最新共享回歸通過（10.12 秒），包括 exact integers／non-finite
fallback、schema drift、partial／unavailable、membership reassignment、
全量與 shard publisher、原子回讀、source races、Mapping implementations、
annotation compatibility 及原生 event binding。先前 360／119 tests 有
重疊，不相加成新的不同測試數。Ruff 與 `git diff --check` 通過。

正式 oneshot 不重啟即自然載入。獨立 probe 沿 canonical generation reader
核對完整 feature bytes／receipt／revision，重新列出全部 membership，
再掃每個實際來源 identity，並綁定真正 journal invocation
`5b2e1a58a8c848c5ac82fde26577c2ca` 的 runtime guards／source root／revision。
完成後 cache／feature source root 相符，89,097 個公開特徵已讀回；native
該 clean frame 為 wall 5.608／CPU 5.237 秒、whole snapshot reuse。
這不是與另一 phase 的 before/after latency 對照，不能稱正式單輪少 6.5%。
source scan 68,043 中 68,024 有可用 schema，其餘 19 仍是 partial；沒有
因效能修正改成 complete／零值，也不宣稱所有歷史／訓練資料完整。

第一次獨立 readback 正確拒絕更新中 cache／feature 不同世代；沒有暫停
來源、拿舊 generation 冒充最新或略過 fence，等 canonical coherent frame
後再通過。原失敗收據保留，以後續成功驗收 supersede。

07:32 再清點 89 services／67 timers／3 paths（含 transient probes），
六個 localhost 回應有效，但 day-trade／data-monitor 仍 degraded。
Discord 約 0.01364 CPU core／416.33 MiB anonymous RAM；不是本輪 A/B
RAM 降幅。整卡仍約 3,786 MiB／5% usage，Windows 配額尚未完整歸因。
盤後 probe 的來源邊界另外如下：

- 07:27 最先在 100m 拒絕 `completed_source_generation_changed`。
- 07:29 官方 source producer 持有更新鎖，五模式均 defer；零 model／CUDA。
- 07:32 鎖已釋放，五模式仍是 source generation 改變；config／implementation
  比對全部相符，沒有偽裝成模型／設定升版。是否 pure-forward input key
  仍相同，尚待 canonical cache owner 驗證，不能推論數值一定改了。
- 這些是 **正確拒絕的驗收，不是恢復完成**。獨立 jq 分類 run 成功綁定
  原始診斷；三筆舊 failed receipts 保留並記為 expected-rejection，沒有
  清除來源缺口、放行舊 cache 或把各模式改成 ready。
- 45／45 當下可讀 managed Linux processes 無 GPU handles；不代表 Windows
  整卡為零。artifact event path 仍 failed/resources，既有 retry 排程有開盤
  保護窗口，沒有為了亮綠燈強制重啟或改排程。VS Code 原 PID 4078945
  現有 RSS 13,435,568 KiB（12.81 GiB），待使用者 Reload Window；未再次
  全掃 418 萬 watches 或增加 kernel limit。此分支及新世代 cache 續接保持待辦。

證據：

- [第一輪鏡像全流程](../artifacts/benchmarks/running_services_20261013/data-monitor-mapping-full-mirror-20261015.json)、[最終八輪原始結果](../artifacts/benchmarks/running_services_20261013/data-monitor-mapping-final-mirror-v2-20261015.json)
- [正式 guard／全世代／完整來源身分獨立驗收](../artifacts/operations/running-services-optimization-20261013/data-monitor-mapping-native-accepted-v2-20261015.json)、[368 項共享回歸](../artifacts/operations/agent-workflow/runs/data-monitor-final-complete-regr-20261014T233432-780cf5a3/run.log)
- [GC／deferred 正式拒絕決策](../artifacts/operations/running-services-optimization-20261013/data-monitor-gc-deferred-rejections-20261015.json)、[benchmark-only 原型](../scripts/data_monitor_deferred_counts_prototype.py)
- [更新鎖的五模式拒絕診斷](../artifacts/operations/running-services-optimization-20261013/postclose-mapping-source-diagnostic-20261015.json)、[鎖釋放後的來源換代診斷](../artifacts/operations/running-services-optimization-20261013/postclose-mapping-after-source-guard-20261015.json)
- [45 程序零 Linux GPU handles](../artifacts/operations/running-services-optimization-20261013/managed-gpu-after-source-guard-20261015.json)、[全部本機服務／排程／公開讀取／資源基線](../artifacts/operations/running-services-optimization-20261013/all-services-after-data-monitor-mapping-20261015.json)

## 10/15 07:51 起：短版盤後快取備援、世代未變不載入資料處理依賴

### 根因與責任分離

07:40 實際 path 為 `failed/resources`（inotify 資源），最後短版 cache
完成於 05:22。舊 retry timer 排除 07–12 時，且 service 的 ExecCondition
對所有工作套用完整歷史 owner 的 123 分鐘 runway；實際 06:17 起便不允許
啟動。新來源世代的 summary 正確拒用，真正缺少的是短版 owner 的續接。

依 [systemd 官方 path 定義](https://github.com/systemd/systemd/blob/main/man/systemd.path.xml)，
path 使用 inotify，不能將其當成永不漏失的唯一喚醒來源。保留原 path 及其失敗
狀態，不提高 kernel limit、不終止 IDE；以原有 retry controller／timer 備援，
不是新增第二個模型／下載／交易管線。

新的 `--request-due-work` 在同一個 CPU-only controller 內分別處理：

- 短版 `stockagent-discord-postclose-cache.service`：10 分鐘 start、90 秒 stop
  加排隊餘裕，使用 13 分鐘 runway；週中 08:07–13:40 不啟動。其 unit 自身也
  有相同 ExecCondition，path 事件不能繞過。只有完成收盤的 cache，不載入正式歷史。
- 完整歷史 `stockagent-discord-artifact-maintenance.service`：原本 123 分鐘
  runway、retry/backoff/source warming 責任保持不變；沒有為提早 cache 放寬兩小時工作。
- timer 每分鐘檢查，但不 import Torch、pandas、呼叫 provider 或執行模型；
  真正 kernel owner/source lock 存在時不重送。相同已請求世代五分鐘 coalesce，
  中斷後可重試，來源／設定變動可提早喚醒。
- `systemctl start` 成功只有 dispatch receipt；必須由 canonical worker 讀回
  所有目標 summary／weights，逐一通過既有 close、freshness、deployment、source
  reuse gates，並確認前後依賴世代穩定，才寫入 worker acknowledgment。
  deferred、缺資料、舊 summary、preview、部分失敗都不會變成完成。
- acknowledge 只是避免重複喚醒的私有證據；每次真正讀訊號仍用原始完整來源／
  hash／generation gates。檔案 stat 絕不取代金融資料的驗收。

### 常態成本實測與拒絕不實 RAM 數字

首次實測完整 observation 約 1 秒、會載入約 160 MiB 的設定／資料依賴。
因此 worker 保存可失效的依賴觀察計畫：每一檔原地寫入仍 stat，目錄世代
涵蓋新增／刪除檔案，繼承 YAML 由 canonical `load_config(source_signatures=...)`
取得依賴；code、checkpoint、source、factorized manifest／members、選定 market
環境及產物身分均保留。未變世代不解 YAML、不載入 NumPy／Arrow／Torch。
這是 advisory wakeup plan，不是另一套金融／factorized source validator。

最終同來源 **3,127 metadata members、五個模式**，最後 shared-reader 修正版本
的完整 fresh-process A/B/B/A（v3；不挑前輪較大降幅）：

| 整段觀察程序 | 每次完整重解 | 未變世代計畫 | 差異 |
| --- | ---: | ---: | ---: |
| 完整 process wall 平均 | 1.3527 s | 0.1552 s | 少 88.5% |
| process CPU 平均 | 1.1273 s | 0.0864 s | 少 92.3% |
| post-exec `/proc/self/status` VmHWM 平均 | 163.78 MiB | 23.78 MiB | 少 85.5% |

四輪完整 decision／plan 完全相同，code 與所有來源世代前後一致；無 provider、
dispatch、模型或 source writes。共享主機、OS cache 未清；這不是完整推論加速、
正式 controller SLA 或全機 RAM 降幅。v1 benchmark 的 `ru_maxrss` 繼承 fork
前父程序高水位，使兩組都顯示約 163 MiB；不採用它作記憶體收益，v2 改讀
post-exec VmHWM，保留兩份原收據。

### 定點部署與目前驗收邊界

原安裝器新增 `--retry-only`，僅更新 retry service／timer 與短版 cache service。
先保存原三份 unit，strict CUDA preflight 通過後部署；不改寫／重啟 Gateway、
全歷史、來源、行情或交易服務。07:51 原短版 owner 已由新的正式 controller
喚醒，逐模式重建新世代；不是手動起另一個推論程序。

最後 **283 項相關回歸通過**（16.66 秒），含 source writer race、真實 flock、
backoff、clock rollback、新股／原地修正／繼承 YAML／code 新增失效、無 runtime
import 快徑、partial／preview 不認證、原生 owner／unit／timer／artifact 拒絕條件。
先前 v1 的 18 項失敗是 fixture 缺少 canonical base config 的 `frequency`，
修正 fixture 後通過；正式來源與配置未改。測試不相加稱更多獨立案例。

07:51:04–07:53:20 的正式 owner 終端成功，五模式全部完成並 acknowledgment。
該次 unit CPU 224.188 秒、wall 約 136.044 秒、unit MemoryPeak 6.43 GiB；
這是一次新來源世代的完整工作，不是常態 RAM、CPU 或先後 A/B 加速。
07:54 獨立 canonical consumer 在禁止 generate/model、CUDA、provider 的條件下
通過五模式全數 cache reuse。07:54／07:55／07:56 的真實 timer 各次都只讀
metadata，`requested=false`、`model_runtime_imported=false`，沒有再次啟動 worker。
管理讀回第一次因互動 shell 未載入 native `.env` 的 market scope，正確拒絕
同世代宣告；第二次用與正式 wrapper 相同 env 讀回通過，未放寬 fingerprint。

最後另外修正 lock probe：source 用共享鎖試探，只 defer 真正 exclusive producer；
job worker 仍用 exclusive 試探。合法長讀取不是更新中，不能因此餓死 cache。
追加對應測試及最後程式世代原生讀回；控制程式世代變動只喚醒原 worker 再驗證，
不能使數值未變的模型重算。

07:57 的最後控制程式版本由原 owner 以 **8.507 秒 wall／8.462 秒 CPU**
完成再驗證，沒有新模型進度／前推。07:58／07:59 兩個不同的正式 timer
InvocationID 都回報同世代 idle（probe 各 0.0711／0.0612 秒），worker
InvocationID／CPU 終端計數保持不變，完整 artifact 及 source/code/config
世代再次讀回相同。最終封存為 `accepted_bounded_postclose_wakeup`，五模式
canonical CPU reuse、六個保護程序未變與 52／52 managed Linux GPU-handle
零持有的當下樣本分別驗證；不是僅憑 service active／exit 0 宣稱成功。

07:58 全服務新基線為 88 service definitions／67 timers／3 paths，
20.945 秒採樣中未觸發新的 resource-pressure finding；六個 localhost GET
有效，但 day-trade／data-monitor 仍 degraded，OpenBB stopped，未改成綠燈。
Discord 約 0.01464 core／414.33 MiB anonymous RAM；短版 worker 已退出。
較高 CPU 是 news 0.510 core、FinMind 0.402 core、backup 0.254 core 等當時
工作，尚需分清有效吞吐與空轉再做下一批改善，不能因有工作就停服務。

07:54 工作結束後的 49／49 Linux managed processes 沒有 GPU handles，仍只是
採樣；重建期間允許真正新 input key 的一次推論，不將它冒稱永遠沒有 GPU 工作。
IDE watcher 的 Reload 及其他服務的完整 profiling 仍在目標內。
07:53 margin-actions owner 已 inactive，是有終端的 07:30
排程工作；先前一核 CPU 樣本不能稱它常駐空轉，不為此停用必要操作。

證據：

- [完整 ABBA v3](../artifacts/benchmarks/running_services_20261013/postclose-cpu-wakeup-abba-v3-20261015.json)、[283 項最後回歸](../artifacts/operations/agent-workflow/runs/postclose-wakeup-shared-reader-r-20261014T235606-5ec1525a/run.log)
- [原生部署前與保護 owner 基線](../artifacts/operations/running-services-optimization-20261013/postclose-wakeup-native-before-20261015.json)、[定點安裝收據](../artifacts/operations/agent-workflow/runs/postclose-wakeup-scoped-install-20261014T235028-a36a5c05/run.log)
- [五模式禁止模型／CUDA 的獨立 reuse](../artifacts/operations/running-services-optimization-20261013/postclose-wakeup-canonical-reuse-20261015.json)、[原生 env 管理讀回](../artifacts/operations/running-services-optimization-20261013/postclose-wakeup-native-env-after-v2-20261015.json)
- [最後世代原生讀回](../artifacts/operations/running-services-optimization-20261013/postclose-wakeup-native-final-v3-20261015.json)、[最後五模式獨立 reuse](../artifacts/operations/running-services-optimization-20261013/postclose-wakeup-canonical-reuse-v3-20261015.json)、[兩輪實際 timer／穩定 worker 封存驗收](../artifacts/operations/running-services-optimization-20261013/postclose-wakeup-acceptance-v3-20261015.json)
- [全服務目前資源／排程／GET 基線](../artifacts/operations/running-services-optimization-20261013/all-services-after-postclose-wakeup-20261015.json)
- [CPU-only 喚醒](../scripts/discord_postclose_wakeup.py)、[單一 controller 與原 worker](../scripts/run_discord_artifact_maintenance.py)、[獨立只讀 native verifier](../scripts/verify_discord_artifact_wakeup_runtime.py)

## 10/15 08:03–08:29：FinMind 精確分組失效、完整發布實測與原生採用

### 根因、方法與語意

先核對真正的 Python child，不把 shell 當成取樣目標。原 owner 的 35 秒
nonblocking py-spy 記錄有 381 個 stack weights、7 次取樣錯誤；它是有偏的
stack 分布，不是精確 CPU 佔比。排程選擇、發布、資料指紋、基線讀取、預留
配額與完整狀態都有成本；當中完整狀態占樣本 10.76%。同窗口共享 account
有 59 次 request starts，不等於成功下載或由單一 owner 發出的 59 次。

此次只改善 `_status` 每次重讀全部 tasks 的分組成本，不換來源、排程、配額、
API 範圍、原始數值、校驗或 durable publication。新增
`downloader/finmind_status_aggregates.py`，接入原 `_db`／`_status`：

- connection-local TEMP triggers 僅記錄哪些 `(dataset,state)` 已變；實際數值
  仍用原 SQL 的 COUNT／SUM／MIN／MAX 計算，保留 NULL、整數與 extrema。
- INSERT、DELETE、identity／REPLACE 可能刪除別組，因此失效整份 memo；
  一般結果更新只重算改變的群組。沒有變更就不重掃。
- 外部 writer commit 以 `PRAGMA data_version` 失效；schema／TEMP trigger
  世代改變重新安裝。未提交交易、普通／唯讀 connection 回到完整 SQL，
  rollback 只會多失效，不快取未提交的數值。
- 不新增永久 trigger、main-schema、排程 owner 或磁碟來源副本，不改來源收據。

TEMP 觀察與外部 commit 的設計依據見
[SQLite TEMP triggers](https://www.sqlite.org/lang_createtrigger.html) 與
[data_version](https://www.sqlite.org/pragma.html#pragma_data_version)。

### 完整本機工作量，而非只計一條 SQL

固定當時全部 3,572,121 筆真實 tasks 與 16 個 UK 歷史 bodies；每輪共 24,248
原始列。保留 claim/release、原始校驗、Parquet／Zstd、內容定址與 SHA、舊收據
保存、指紋／更新觀測、結果落盤、完整 status JSON 與 fsync。最後全部 task
values 逐列串流比較；四輪完整 status、receipt、Parquet hash 均相等。

| 同輸入 A/B/B/A 平均 | 原版完整 SQL | 精確分組失效 | 降幅 |
| --- | ---: | ---: | ---: |
| wall 秒 | 6.502149 | 3.612283 | 44.44% |
| CPU 秒 | 3.039796 | 1.199340 | 60.54% |

最後四輪固定 nice 15、MemoryHigh 4 GiB／MemoryMax 6 GiB，前後資源限制與
code/source hashes 相同。這是本機發布 quantum，HTTP 是明確的固定離線輸入；
不宣稱網路／全機 1.80 倍，也不拿共享 host 的 OS 暖快取差異當冷啟動收益。

初輪 stepped backup 在持續寫入時重頭開始，僅終止自己的工程子程序，原生
下載未停止；改以有界 read snapshot 固定來源。第二輪完整 parity 有效，但
工程 cgroup MemoryHigh 在中途改變，**不採其效能數字**，第三輪才是上表。
不刪除失敗報告、來源或私有比較 fixture。

### 正式載入與獨立驗收

最後完整 FinMind／runtime／workflow 回歸 **1,099 passed，75.48 秒**。
先前同範圍兩個 browser 啟動逾時保留；未改 UI 或 timeout，獨立 3 個 browser
案例與完整重跑均通過，僅記錄同時存在的 host I/O 壓力，不推斷唯一原因。
新增的 clock-readback 與 group/reload focused scope 另 **33 passed，2.29 秒**，
與 1,099 多數重疊，不相加成不同測試數。

08:23 原 owner 已有 canonical protected-opening wait、兩種 claim table 均為
零，才由既有 `reload_finmind_at_idle.py` 重載這一個 owner。原最後 Japan
來源收據／Parquet SHA 與實際 fetch timestamp 未改，五個其他保護 owner
invocation 未變；不重啟行情、交易、Gateway 或其他來源。

08:29 透過普通唯讀 SQLite connection 執行原來的完整 `_status`，與原生 JSON
**全部欄位一致**。原生 journal 顯示先 full 121 組、再 unchanged 0 組重算。
最初驗收誤將 top-level／frontier 兩次獨立時鐘固定成同一值，相差 36 微秒；
改為依原記錄分別重播，不排除任何資料或時鐘欄位；失敗收據原樣保存。
當時 FinMind native PIDs 無 GPU device handles，觀察器未 import Torch、
無 provider request／來源寫入／service control。

正式啟動至完整 status 約 77 秒，stack 指向既有 supplemental seed／populate。
暖發布的收益不能掩蓋這項冷啟動成本。08:25 的最新全服務 census：89 個
service definitions、67 timers、3 paths（不是 89 個 active services）；
21.335 秒窗口 FinMind 為零 CPU 是開盤保護等待，不能當成 .402 busy core
降到零的效益。Discord 約 1.268 core 正在真正盤前 factorized preparation，
不是證明盤後 cache 失效；不終止這種必要的開盤工作。六個只讀 HTTP 均 200，
但部分資料面板仍 degraded／waiting，不宣稱全業務 ready。IDE watcher 的
Reload Window 與 Windows 整卡顯存歸因仍未解決。

證據：

- [原生取樣摘要](../artifacts/operations/running-services-optimization-20261013/finmind-runtime-profile-summary-v2-20261015.json)
- [固定資源完整發布比較](../artifacts/benchmarks/running_services_20261013/finmind-group-status-abba-v3-20261015.json)
- [1,099 項完整回歸](../artifacts/operations/agent-workflow/runs/finmind-final-shared-regression-20261015T002117-e366b7ef/run.log)
- [既有 owner 閒置重載](../artifacts/operations/running-services-optimization-20261013/finmind-group-cache-native-reload-20261015.json)、[獨立完整 SQL 驗收](../artifacts/operations/running-services-optimization-20261013/finmind-group-cache-native-acceptance-v3-20261015.json)
- [最新全服務資源普查](../artifacts/operations/running-services-optimization-20261013/all-services-after-finmind-group-cache-20261015.json)

## 10/15 08:31–08:51：保護等待只更新心跳，不重建下載 queue

### 原生根因與修正邊界

原本禁止一般資料抓取的開盤保護期間，600 秒等待結束後仍先跑 `_populate`、
supplemental seed、worker corrections 與完整狀態，最後再判定不可抓取。
180 秒原生觀察剛好跨過一次這種重建：用了 18.756198 秒 cgroup CPU，
實際最後取得來源仍是原 Japan 歷史，來源收據／Parquet hash 均未變。
180 秒 nonblocking profile 的 336 個 stack weights、8 次錯誤只用來定位
熱點；inclusive frame 有重疊，不當作精確 CPU 百分比。

沿原 `finmind_scheduling.py`／`finmind_runtime.py` 修正，不新增 queue 或 owner：

- 原來已判定 `protected_opening` 才延伸等待到日曆的 09:10，保留 08:20
  起點、休市、未知平日、特殊週末交易日的原語意。
- 每 30 秒僅更新原心跳並檢查本機 canonical 日曆。若官方休市更新解除
  保護，下一個心跳提早離開；不以 cache 固定失效的日曆結論。
- Free 日曆自身的更新／重試邊界仍可提早喚醒；普通 release、task、
  backoff 與 quota 的原排程不改。不為減少 CPU 跳過應執行的工作。
- 新增一次性的 loaded-runtime SHA／等待截止 journal，區分磁碟程式變更
  與真正已載入版本；狀態／心跳 ABI、資料範圍、配額及來源收據不變。

### 測試、原生採用與長窗口驗收

最終 FinMind／runtime／workflow scope **1,121 passed，67.73 秒**；工程
sealer／observer／readback focused scope **62 passed，2.52 秒**，多數與
前者重疊，不相加稱不同測試數。涵蓋精確窗口、特殊交易日、90 次模擬
心跳、日曆提早解除、Free calendar refresh／retry 與拒絕錯誤 loaded SHA。

08:38 僅重載自然排空的 Complement owner；新 invocation 的 runtime SHA
與截止 09:10 均獨立讀回。完整 SQL 與原生 status 全部欄位一致，五個其他
保護 owner 未變；原實際 fetch clock 與來源檔案 SHA 保留。新 owner 的
首次初始化仍存在，這次沒有把它算成已消除。

08:39–08:50 的 **690.001122 秒**原生窗口，cgroup CPU 合計 **0.053314 秒**。
全部觀察仍是 protected-opening／無 active task，同一狀態循環，原生 owner、
來源及實際取得時間未變。窗口跨過舊循環的 08:48:26 重建邊界，不只是
在重掃前短暫看到 idle。08:51 的獨立 sealer 驗證目前 owner、loaded SHA、
完整 SQL、原始回歸 run／log 與輸入證據 hash，接受有界保護等待修正。
不同長度的含重建／無重建窗口不可當成固定工作量的全下載加速比例。

**剩餘邊界：** Free／Sponsor 仍需各自正式載入及驗收；本次證據尚未證明
未來 09:10 能重新開始抓取、全部歷史完整，或所有服務最佳化。
08:42 的 20.808 秒全服務取樣中 FinMind Complement 約 0.000097 core，
而 storage lifecycle 約 0.307 core、backup stream 0.151 core；其中存在實際
清冊／同步工作，不把 busy／idle 混作效益。六個 GET 均可達，但部分面板
仍 degraded／waiting。Windows 整卡顯存與 IDE watcher 重建仍待驗證。

證據：

- [原生保護期基線](../artifacts/operations/running-services-optimization-20261013/finmind-protected-native-baseline-20261015.json)、[690 秒原生觀察](../artifacts/operations/running-services-optimization-20261013/finmind-protected-native-reuse-20261015.json)
- [定點閒置重載](../artifacts/operations/running-services-optimization-20261013/finmind-protected-idle-native-reload-20261015.json)、[完整 SQL 回讀](../artifacts/operations/running-services-optimization-20261013/finmind-protected-current-sql-acceptance-20261015.json)
- [獨立長窗口驗收](../artifacts/operations/running-services-optimization-20261013/finmind-protected-idle-acceptance-20261015.json)
- [1,121 項共享回歸](../artifacts/operations/agent-workflow/runs/finmind-protected-final-regressi-20261015T003626-62df26ad/run.log)、[62 項工程驗收回歸](../artifacts/operations/agent-workflow/runs/finmind-idle-seal-final-contract-20261015T004841-472c942a/run.log)
- [全服務目前資源與狀態](../artifacts/operations/running-services-optimization-20261013/all-services-after-protected-wait-20261015.json)

## 10/15 08:54–09:04：儲存清冊計畫、watcher 現況與開盤 GPU 邊界

這一段先量測、建立隔離候選，**尚未修改或重啟正式儲存 owner**。
依儲存維運 skill 使用 PID 1 namespace／原 private Mamba 控制角色；不刪除
原始資料、暫存、收據或備份，也不變更保存期限、NAS ACK 或容量政策。

### 儲存候選與完整流程判準

`storage-lifecycle` 35 秒 profile：161 個 stack weights、21 個取樣錯誤；
inclusive `stat` 118、來源 signature lambda 71、`capture_catalog` 88。
另對真正 backup-stream Python child 做 35 秒 profile：177 weights、16 個
錯誤，`capture_catalog` 175、`add_metadata` 117、`stat` 65、`lstat` 56。
這些 frame 互相重疊，不可加總成 CPU 佔比。兩個服務正在做真正的清冊工作，
不是無效輪詢；採樣期間未停止發布或其他背景工作。

`register()` 原 signature 為同一檔案呼叫 **四次** `stat()`，分別取 inode、
size、mtime_ns、ctime_ns。候選每個 pass／每個檔案取一次完整 stat_result，
保持原 ordered tuple、policy bit 及 hash，不跨 pass 記憶，保留最後再驗、
每小時完整重查與異常回復。完整 capture／SHA／DuckLake／NAS 皆不省略。

隔離 script 只以 AST 精確替換這個 lambda，原 `register` 其餘程式不變。
先做完整 idle-register replay：真實全 metadata path set、原 mount guards、
真正 READ_ONLY DuckLake snapshot/count 查詢，以及私有 status／durable readback。
「新鮮 reuse gate」是明確的工程 fixture，**不是目前來源等於已提交 catalog
的證明**，不能當作 full-capture 或備份加速；正式來源及 PG 不寫入。
暖機另列，A/B/B/A 比較，來源集合／指紋、owner、limits 改變便拒絕採用。
完整 recapture／原生載入驗收仍是後續工作。

原私有控制 runtime 的最後工程 scope **147 passed，15.11 秒**，涵蓋
完整 idle 回讀、來源變更、read-only catalog 前進、每小時失效、缺失 release、
恢復／持久化等案例。一般金融 runtime 曾因沒有 `temporalio` 跳過 workflow
測試；改用本來鎖定的角色後四個 continuation cases 均執行，未安裝新套件
或放寬 skip。這不是全 repository tests、native acceptance 或全服務最佳化。
目前正式 worker-ready 的 loaded fingerprint 仍是原 `7fc809f0…`。

### watcher 與開盤 GPU 不混作盤後 idle

08:54 同一 VS Code watcher 實測仍有 **4,187,968 watches**、RSS
**12,548,108,288 bytes（11.69 GiB）**，23.679 秒窗口約 **0.9992 CPU core**；
占 UID watch limit **99.8489%**。現有排除設定尚未讓舊 owner 釋放，已非阻塞
請使用者儲存並 `Developer: Reload Window`；沒有終止 IDE、提高 kernel limit，
也沒有宣稱已回收 RAM 或解決所有 service stop timeout。

09:01 Windows 兩次 WDDM 取樣各 51 個 process-adapter rows，皆可識別，
整卡另測約 15,011–15,048 MiB／1–7% utilization。09:02 的 managed Linux
FD 探針 51／51 可讀，只有正式 Discord owner 持有兩個 `/dev/dxg` handles。
這是開盤模型保護窗口，不是原 07:59 盤後零 handles 的延伸，沒有為降低數字
強制清除開盤模型。Windows `vmwp.exe` 與 Linux 服務的 PID／顯存並未完成
逐一映射；Sunshine／DWM 等 counters 也不能相加成唯一配置。依
[Microsoft WDDM 說明](https://devblogs.microsoft.com/directx/gpus-in-the-task-manager/)
與[計數器準確度界線](https://learn.microsoft.com/en-us/troubleshoot/windows-client/performance/gpu-process-memory-counters-report-wrong-value)，
不把這次取樣當成 VRAM leak、整卡已全部歸因或遊戲 FPS 的驗收。

證據：

- [儲存候選與不變條件計畫](../artifacts/operations/running-services-optimization-20261013/storage-signature-optimization-plan-20261015.json)
- [storage 原生 profile](../artifacts/operations/running-services-optimization-20261013/storage-lifecycle-profile-20261015.raw)、[backup-stream 原生 profile 摘要](../artifacts/operations/running-services-optimization-20261013/backup-stream-native-profile-summary-20261015.json)
- [隔離完整 idle replay](../scripts/benchmark_lakehouse_source_signature.py)、[147 項控制角色回歸](../artifacts/operations/agent-workflow/runs/lakehouse-idle-reference-final-c-20261015T010435-2258736f/run.log)
- [watcher 原生再量測](../artifacts/operations/running-services-optimization-20261013/workspace-watcher-current-pressure-20261015T0057.json)
- [Windows 開盤 WDDM／整卡](../artifacts/operations/running-services-optimization-20261013/windows-gpu-opening-attribution-20261015.json)、[managed Linux 開盤 FD 清冊](../artifacts/operations/running-services-optimization-20261013/managed-gpu-opening-owner-observation-20261015.json)

## 10/15 09:10–09:17：保護等待結束後的真實下載回讀

900 秒唯讀窗口從 08:58:03 到 09:13:03，正式 Complement InvocationID
`71fc9d09c4284f3398b3c58fcb871884` 未換代。running 心跳為
09:10:00.013602；兩秒取樣第一次看見它在 09:10:01.734534。第一個觀察到
的新 complete 為 09:11:04.656011，不能把心跳時間當作資料完成時間。

以同一 queue 的主鍵直接查回前三個不同的成功項目，沒有掃描 357 萬任務
或額外呼叫 provider：

| 資料 | 商品 | 新收據 fetched（台北） | 原始列數 | 獨立回讀 |
| --- | --- | --- | ---: | --- |
| UKStockPrice | 0ID1.L | 09:11:03.788805 | 2,187 | receipt 身分、Parquet footer／size／完整 SHA 通過 |
| UKStockPrice | 0IEE.L | 09:11:27.789130 | 1,211 | 同上 |
| EuropeStockPrice | MLCFD.PA | 09:11:48.798104 | 6,862 | 同上 |

舊 JapanStockPrice／2722.T 的 receipt bytes／Parquet SHA 在等待、恢復
及獨立回讀後均未改變。驗收僅限 Complement 的這個跨界窗口；Free／Sponsor
仍需各自的 loaded-code 採用，未宣稱全歷史完整或所有 queue 已完成。
900 秒混合了等待與真正下載，89.705514 秒 CPU **不可**與純等待 690 秒
取樣做吞吐／節省比例。沒有提前解除開盤保護，也沒有變更 provider 配額。

新增 verifier 的 47 項相關測試通過（包含既有等待／封存／儲存 fixture，彼此
重疊，不與 1,121 相加）；拒絕舊成功時間、只有心跳、owner／來源變動、
超時喚醒、錯誤列數與 SHA。正式驗收只讀 queue、receipt 和原始檔，沒有
請求 provider、重排 queue 或重啟服務。

儲存的首次完整 idle replay 在 09:13 被拒絕：09:11 fixture 為 catalog
snapshot 566／1,883 releases，09:12 正式 owner 已提交 567／1,884 releases，
私有 replay 依原程式落回 full capture，工程入口正確拒絕代替正式 publisher。
原失敗 log 保留；沒有修改 reuse 條件、停止 producer 或宣稱候選加速。
09:15 另啟一次新 fixture 對照，目前正式儲存程式與 worker 尚未修改。

證據：

- [900 秒原生跨界觀察](../artifacts/operations/running-services-optimization-20261013/finmind-native-opening-resume-window-20261015.json)
- [三份新來源的獨立驗收](../artifacts/operations/running-services-optimization-20261013/finmind-native-opening-resume-acceptance-20261015.json)、[回讀程式](../scripts/verify_finmind_opening_resume.py)
- [47 項回歸](../artifacts/operations/agent-workflow/runs/finmind-opening-readback-contrac-20261015T011653-a0eafad7/run.log)
- [首次儲存對照拒絕](../artifacts/operations/agent-workflow/runs/lakehouse-full-idle-signature-ab-20261015T011019-ce1aa0e5/run.log)

## 測試與重現

較廣的 FinMind、准入、OpenBB monitor／ownership、服務清冊與 dashboard
量測回歸為 1,456 passed。最後程式版本另跑 572 passed focused tests，
涵蓋相同時計／owner reuse、NULL renewal、legacy 重複優先 key、後段
metadata 壞頁、缺欄位分類與零列分類；兩組有重疊，不相加稱 2,028 個不同測試。
修改範圍 `git diff --check` 通過；未宣稱 repository 全測試完成。
工程失敗紀錄保留並由成功收據 supersede，不刪除、不重寫成成功。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_service_latency_coverage.py \
  --sample-seconds 20 --include-host-services \
  --output artifacts/operations/running-services-optimization-20261013/retest.json
run_fintech_python scripts/benchmark_finmind_eta_runtime.py \
  --fixture artifacts/benchmarks/running_services_20261013/finmind_eta_v2 \
  --output artifacts/benchmarks/running_services_20261013/finmind_eta_retest.json
run_fintech_python scripts/benchmark_openbb_metadata_audit.py \
  --fixture artifacts/benchmarks/running_services_20261013/openbb_audit \
  --output artifacts/benchmarks/running_services_20261013/openbb_audit_retest.json
```

新的 fixture 使用 `--prepare-only` 和新路徑；舊 fixture 不覆蓋。先確認有
至少 30 GiB 安全剩餘空間加來源快照容量，再做 SQLite backup 或來源抽樣。
這些測試不需要新登入或 GPU 訓練；既有訓練與 source receipts 不改動。
