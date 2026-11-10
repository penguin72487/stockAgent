# 下載器清點與恢復：2026-11-10

本文件記錄 2026-11-10 晚上台北時間的現場觀察及本輪修正。來源、配額與佇列持續變化；服務啟動、來源收據、歷史完整及可供訓練是不同的驗證層次。

## 清冊與儲存

- [來源、API、工作與群組清冊](../artifacts/data_quality/acquisition_restoration_2026-11-10/README.md) 由既有資料監控快照輸出，含 2,759 筆登錄項目與 45 筆憑證目錄。包含別名及參考項，不能加總為獨立資料集數或當作全市場完整證據；憑證存在也不等於權限已驗證。
- [程式候選清冊](../artifacts/data_quality/acquisition_restoration_2026-11-10/collector_program_candidates.csv) 有 370 個下載、修復與支援程式候選，附檔案雜湊；其中不是每個都是獨立下載服務。
- [資料根目錄](../artifacts/data_quality/acquisition_restoration_2026-11-10/data_roots.csv) 列出 33 個 `data_*` 目錄及其實際路徑與檔案系統；不遞迴宣稱涵蓋所有子目錄或所有版本。
- FinLab、FinMind、TEJ 的現行來源工作檔位於 E 工作區；原始檔、Parquet、來源收據與 SQLite 任務佇列依 provider 各自管理。FinLab 寬表仍可能以整表內容識別檔保存，不能說全部來源工作檔已是只存差異。
- D 是既有不可變封裝與冷資料層，E 也存放已登錄的常用封裝；manifest、內容識別、位置索引及備份 owner 的分工見 [儲存配置](storage_warm_archive_placement_2026-11-10.md)。來源工作區寫入不等於冷庫或 NAS 驗收。
- 現場曾量得 E 檔案系統約 934 GiB 可用，D 約 51 GiB 可用；另一次獨立實體容量探測量得 E 約 940 GiB、C 約 281 GiB。時間不同、數字會變，不能將 WSL `df` 視為實體 backing 空間。

## 本輪修正與運行狀態

### 通用排程恢復

舊的使用者暫停控制仍要求儲存修復期間停止大量下載。依本輪恢復下載的明確要求，在既有交易服務控制 lease 下保留原控制內容，將暫停標記改為 released。守護程式已恢復 OpenBB、註冊資料日線、加密盤中與歷史回補四個原計時器；盤前保護窗及既有容量檢查仍生效。

重新啟動後，已觀察到 Binance、OKX、Bybit 盤中下載工作及 OpenBB 歷史 archive supervisor 執行。OpenBB archive 仍固定使用原有 2026-07-18 結束日；這個固定歷史批次不證明後續所有增量已完成。原 OpenBB 與註冊背景服務仍有先前失敗紀錄，應以新收據而不是計時器狀態判斷資料恢復。

### FinLab

先前每分鐘帳號流量取樣與本機工作量預估共用一個 30 秒服務；預估器在模擬每個任務時反覆掃描所有剩餘任務，造成二次方工作並曾逾時。現在用各優先級的最小需求堆與到期增量堆維護同樣條件，配額週期變動時重新建索引；分階段語意和未知值不變。250 組本地差分情境與修改前結果一致；最新服務觀察約 13–15 秒完成，另一次本機 workload 計算約 7.15 秒。這是本機計算改善，非來源傳輸速率上限。

23:07 左右帳號回報 3,386.763 / 5,000 MB 已用；數字為當次觀測，依原 08:00 台北時間重置規則及實際帳號回報更新。下載器有新資料；[抽樣讀回](../artifacts/data_quality/acquisition_restoration_2026-11-10/receipt_readback.json) 的 `disposal_information` 收據 SHA 與 Parquet 8,153 列吻合。

監控工作量曾顯示一般資料已知部分的來源查核權重為 100%，但分母排除來源全空／權限及 Tick；這不是全歷史或實際帳號流量 100%。`management_change_events:變更交易開始日` 的最新來源嘗試是 `provider_empty`，不能將它算成成功下載。部分筆數估計仍未知，總完成時間保持未知。

### FinMind

帳號在 23:08 回報 SponsorPro 20,000 次／小時、已用約 126 次；同一時點排程有 2,748 個到期追新請求及 2,777 次預留，Sponsor 背景工作因追新優先而等待。大多待辦來自 Complement 海外股票與期貨資料。實際吞吐遠低於額度，尚不能宣稱已達官方上限。[官方方案](https://finmind.github.io/Pricing/)與帳號當次回報都要一起看；來源逐端點的實際回應及限制仍可能不同。

修正原 Complement 管線排空後容量樣本已恢復，卻仍回報 `disk_guard` 並結束整輪的問題。現在必須同時重新確認本機與實體容量，才回到原 owner 重新檢查日曆、佇列和配額；空間不足或實體容量無法確認時仍停止。另修正並行寫入時，觀測 SQLite 檔已建立但 schema 尚未建立造成的例外。

現場 Python stack 顯示更正核對反覆以 Polars 掃描同一組期權 Parquet。本輪以檔案裝置／inode／大小／mtime／ctime、原始 SHA 及預期契約鍵的雜湊綁定快取，只保存核對結果與數量，不保存原始值。任一世代或條件改變即重新核對，並在讀取前後再檢查；未通過仍維持未通過。本機實際樣本首次 0.399 秒、相同證據再核對 0.001 秒；不是全管線效能或完成時間測量。

23:xx 的原 Complement 仍在下載；[抽樣](../artifacts/data_quality/acquisition_restoration_2026-11-10/receipt_readback.json) 的 UKStockPrice 收據 SHA 與 Parquet 2,219 列吻合。上述新 FinMind 程式尚未被長駐 owner 載入：原安全重載器一次因自訂 SIGTERM 防護拒絕，後續 [十分鐘安全重載](../artifacts/data_quality/acquisition_restoration_2026-11-10/finmind_final_reload.json) 未找到可核對的空檔，未送出重啟。已加強拒絕時的狀態回報，且凍結準備期的檢查新增一般 in-flight 任務；不刪鎖或強制中斷來源。工作流程指令正常結束不等於重載成功，須以重載收據及新版本來源收據為準。

### TEJ

[TEJ 試用 API 官方文件](https://api.tej.com.tw/documents.html) 的額度包括每日 500 次、50,000 列、每頁最多 10,000 列及每查詢最多 50,000 列。23 點附近本機保守帳本為 36 次、50,000 列，剩餘 464 次但列額度為零；官方快照早於之後下載，不能用舊快照覆蓋較新的本機流量。原 timer 按既有保守日界及滾動 24 小時規則重試，非保證官方重置時間。API 本地有 760 頁、1,849,997 儲存列，工作為 complete 570、pending 819、blocked 55；均不代表全史完成。55 個 blocked 均為 HTTP 400 `query_rejected`，分布於 `TAIM1A` 40、`TAFUTR` 14、`TASALE` 1；抽樣工作都有分頁 cursor，但目前沒有來源錯誤本文，不能僅據此斷言 cursor 已過期，也不能無條件重置並重抓。

依當下本機帳本推算，既有保守規則首次釋出可用列約為 11 月 11 日 17:25:50 台北時間。已另設一次性 timer 在 17:25:52 喚醒**同一個** API service，以減少原 15 分鐘輪詢的等待；[推算證據](../artifacts/data_quality/acquisition_restoration_2026-11-10/tej_api_quota_wake.json)不代表來源實際重置時間。這個 transient timer 不跨重開機，原固定 timer 仍保留。

23:27 對照原工作、失敗 request 與已保存分頁後，已從三張出現 HTTP 400 的表各挑選一筆工作，保留游標、既有列數和原始證據，交回原 API owner 一次性查證；當下未呼叫來源。佇列現為 52 筆原樣 blocked、3 筆 retry，後者仍須等待列配額及原有工作優先順序。程式會為之後的 HTTP 錯誤保存長度受限且去識別化的來源本文；若再被拒絕仍標記 blocked，不能先把這三筆當成已修復或已下載。

Smart Wizard 的原查詢於 2026-11-10 結果不明。第一層障礙是原 WSL relay 的 socket 雖存在但曾無法啟動 Windows；現在只在原 relay 未取得執行許可時嘗試另一條，同時要求原互動工作階段，並由 native bridge 再驗證視窗與工作簿。另一條 relay 有時即使 session 相同也不能取得 Excel COM，故不能僅憑 session 相同就認定可操作。

第二層障礙是原有排程要求桌面已就緒才檢查使它不就緒的對話框。修正後只允許原工作簿與有效 relay 的**無新資料查詢**核對，未就緒時仍禁止重放。23:07 實際核對到 TEJ 原視窗回報 `查無此DB(wscap)`；目前保留原 request、query stage、未知結果、對話框與檢查收據，標示 `vendor_database_wscap_unavailable`，並未偽造空資料或重送原查詢。因來源錯誤及原查詢未決，Smart Wizard 尚未恢復新增下載；API 的獨立佇列不因此冒稱已完成。

## 驗證及未完成事項

- FinLab 85 項、FinMind 393 項、TEJ Smart Wizard 236 項及 TEJ API 58 項針對性測試通過；這些不是全庫覆蓋測試。FinLab 另以 250 組模擬情境比對舊版預估，非來源驗收。
- 已抽樣驗證兩個新收據，並更新程式與來源清冊；未驗證每一個來源全部歷史、修正公告、PIT 或模型訓練契約。
- FinMind 的容量與更正核對優化仍等待安全重載和新的來源收據；當前追新會使 Sponsor 背景工作等待，未達 20,000 次／小時。
- TEJ `wscap` 錯誤的真正原因可能是來源資料庫、權限或本機來源設定，尚不能單憑錯誤判定哪一個。原結果仍需核對，Smart Wizard 不能宣稱已恢復。
- TAIFEX 既有訓練面板最近一次報固定 1,936 槽不足、至少需要 1,937 槽；不能直接更動舊模型 ABI 冒充修復。其他服務重新排程後仍需逐來源驗證新成果。
