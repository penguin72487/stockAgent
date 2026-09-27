# 高優先公開來源修復與實測（2026-09-27）

23:55 後續更新：[Census／BEA 新金鑰驗證、EODHD 免費方案與 FinLab 大表修復](eodhd_finlab_macro_repairs_2026-09-27.md)。Census 19/19 指定查詢與 BEA 9/9 表已取得；下方較早的金鑰拒絕／未啟用狀態不代表目前狀態。

本次實作、部署及實際下載，不是把「已寫程式」當作「全來源已下載」。
資料快照約為台北時間 22:35；背景工作持續變動，最新數字以收據及下列稽核指令為準。

## 判斷依據

分開判斷五件事：端點存在、帳號可用、指定查詢已下載、數值正規化品質、歷史發布版本／訓練可用性。
API 回傳成功、服務 active、無待辦，以及目錄列數，都不能單獨證明完整歷史。
沿用既有 OpenBB manifest、工作執行器、HTTP 邊界配額、原子寫入及公開唯讀面板。
沒有另開 EIA／IMF／OECD 下載管線，也沒有修改交易、Shioaji、Discord 或模型設定。

證據：

- [22:25 稽核：揭露原追新錯誤](../artifacts/data_quality/provider_repair_2026-09-27/verification_20260927T142540Z.json)
- [22:35 稽核：修復後的執行中快照](../artifacts/data_quality/provider_repair_2026-09-27/verification_20260927T143500Z.json)
- [22:41 最新交付快照](../artifacts/data_quality/provider_repair_2026-09-27/verification_20260927T144104Z.json)：指定工作 721 個初始成功、11 個初始空結果、7 個初始仍待執行／執行中；追新 258 成功、228 空結果、34 個仍待執行／執行中。不是全 OpenBB 分母，也不是全來源完成率。
- [逐項數值歷史清冊 CSV](../artifacts/data_quality/provider_repair_2026-09-27/verification_20260927T143500Z.csv)
- [公開全資料面板](https://penguin72487.ddnsgeek.com/data-monitor/)

## 新增或補強的高優先資料

| 來源 | 實作範圍 | 實際狀態／邊界 |
| --- | --- | --- |
| Census | 18 個 EITS 經濟指標程式：零售、批發、庫存、製造訂單、住宅、營建、服務、財稅、外貿、企業申請等 | 已寫完整歷史與增量工作；現有 key 被官方導向 `invalid_key.html`，尚未取得數值。不能以 1,807 筆目錄代替數值歷史 |
| BEA | 9 張 NIPA 表；GDP、實質 GDP、平減指數、消費、PCE 物價、所得儲蓄、政府收支、國民所得 | 一次全可用年份及原生頻率；現有 key 回覆 API error 4，需完成啟用。尚無成功數值下載 |
| 內政部實價登錄 | 官網實際列出的 58 個季度 ZIP，加目前發布批次，共 59 項 | 59 項原檔均保存，28,231,384 筆主表及土地／建物／車位明細列；不是不重複交易數 |
| Frankfurter | 既有 ECB 檔案前端補到 1999-01-04；新增 BBK、HKMA、AMCM、BOC、RBA 原生基準匯率 | 5 個來源取得 711,129 筆；不把預設混合匯率覆寫為 ECB，也不聲稱每個幣別都有 1948 年資料 |
| EIA | 沿用 WPSR／STEO；新增天然氣週庫存、日價格、電力月度零售 | 新增三個端點的歷史及追新均成功，詳見下一節 |
| OECD | 原端點排程修復；GDP 預測改用最新版資料流、保留負值與小數、加來源版本 | 9 組修復工作已入同一 manifest；22:35 已有一組取得 3,528 列、來源版本 1.5，其餘依配額繼續執行 |

Census／BEA 資料名稱、單位維度、缺值／抑制符號都保留，不擅自把未知單位當美元或百分比。
目前未取得有效授權回應，所以這 27 個數值工作只完成程式與離線測試，不能宣稱全部端點已通過實網驗證。

官方依據：[Census 經濟指標](https://www.census.gov/data/developers/data-sets/economic-indicators.html)、
[BEA API 指南](https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf)、
[內政部發布檔案](https://plvr.land.moi.gov.tw/DownloadOpenData)。

### EIA：不是從零，也不重抓已成功工作

原本「沒有有效成功數值」的判讀不完整：直接按 task ID 查 SQLite 和 Parquet footer，找到舊計畫下的
33 張 WPSR、26 張 STEO 成功檔案，以及 IMF／OECD 等既有工作。本次恢復相同 task ID 的歸屬，
沒有因 active-plan 摘要漏列就重新下載。

| 新增端點 | 實存最早觀測 | 實存最新觀測 | 初始歷史列數 | 追新回傳列數 |
| --- | --- | --- | ---: | ---: |
| `gas_storage_weekly` | 2010-01-01 | 2026-09-18 | 6,912 | 112 |
| `gas_prices_daily` | 1993-12-20 | 2026-09-22 | 37,799 | 67 |
| `electricity_retail_monthly` | 2001-01 | 2026-07 | 114,204 | 744 |

追新含修訂重疊區間，兩欄不能相加當唯一觀測數。月資料最新為七月不直接等於下載故障，應看官方發布資料。
新轉接每頁最多 5,000 列，驗證 declared total、唯一鍵、查詢範圍、分頁停滯、回應大小；截斷不算成功。
直接保留來源單位及小數文字。[EIA API 文件](https://www.eia.gov/opendata/documentation.php)。

WPSR 兩張舊 reclassification 表在近期範圍沒有觀測，SDK 卻對空列表 `concat` 而報錯。
修復後先驗證完整來源表，再在本機按日期過濾，只有驗證後真正空的查詢才標成 empty。
22:35 的 WPSR 追新為 31 success、2 authoritative empty。STEO 為 24 success、2 running。
另保留 SDK 包裝的 TimeoutError 型別，避免空錯誤訊息把暫時逾時判成永久缺口。
僅針對七個已確認待修工作做一次有備份的重試，保留原 attempt 計數，沒有清空 provider 全部錯誤或配額。

### IMF／OECD／UN Comtrade：端點與工作清單，不只 provider 名稱

本次復原清冊包含 IMF direction of trade、港口／關鍵航道資訊與流量；
OECD CPI、領先指標、GDP、房價、利率、股價指標及失業率；另包括既有 export destinations 工作。
最初復原／補列 739 個 task，當時其中 736 個已有成功或 authoritative empty 證據，
另 3 個是新增 EIA 歷史工作。其後新增追新與特定 adapter 修復工作，不能把不同時間的分母混為一談。

修復的排程問題：

1. 以已完成查詢的 end-date 推進 cursor，保留修訂重疊；前一工作未完成時不每天另造一個重複工作。
2. authoritative empty 也推進「已檢查範圍」，但不推進資料最新日期，下一輪仍保留重疊。
3. 年頻回看兩個完整年度，季／月頻從前一年年初回看；日頻保留 31 日。
   原先統一 31 日會漏掉年初標記的年資料，也容易得到尚未發布的新季度空查詢。
4. 新工作入庫後寫入小型原子通知檔。執行器以 stat 低成本偵測、閒置最遲下個 30 秒 heartbeat 喚醒；
   不必等待無關 FMP 的配額重置，也不每 30 秒重掃約九百萬筆 manifest。
5. 舊計畫的初始截止日保留 2026-07-18；追新是獨立工作，不偽造初始歷史快照。

OECD GDP forecast 的 SDK 問題比排程更重要：原套件固定 `DSD_EO@DF_EO,1.1`，
並將非成長數值轉整數、刪除負值及零值。本次轉接使用官方「版本留空即最新版」契約，
保存 `STRUCTURE_ID`、`source_vintage_version`、`OBS_VALUE` 與所有 SDMX 單位／狀態欄。
`source_value` 保留原始小數文字；相容欄 `value` 的成長率仍為百分比除以 100，其他為原始 level，
不再丟掉負成長。新版使用不同 task ID，舊資料不覆寫。
實網已觀察版本 **1.5**，原始值例如 `20580911999999.9` 不再被截整數。
這是目前版回看歷史，不是還原當年的預測版本；模型不可拿 observation period 當發布日期。
[OECD API 版本規則](https://www.oecd.org/en/data/insights/data-explainers/2024/09/api.html)、
[官方 Economic Outlook 119](https://data-explorer.oecd.org/vis?dataflow%5BagencyId%5D=OECD.ECO.MAD&dataflow%5BdataflowId%5D=DSD_EO%40DF_EO&dataflow%5BdatasourceId%5D=DisseminateFinalDMZ&locale=en)。

UN Comtrade 仍是既有 EconDB 工作內的公開 fallback，僅涵蓋可找到的年度出口目的地，
不是全球商品×夥伴×年月全史。本次加入 preview 500 列上限檢查；達上限不得當完整下載，
並標示 `history_complete=false`。未新增需付費或未經容量評估的全球貨品明細工作。
[UN Comtrade preview 限制](https://uncomtrade.org/docs/what-is-data-preview/)。

### Frankfurter：來源、年代與單位分開

| 官方來源／原生基準 | 筆數 | 實存期間 |
| --- | ---: | --- |
| BBK／DEM | 222,241 | 1948-06-21～1998-12-30 |
| HKMA／HKD | 182,729 | 1981-01-02～2026-08-31 |
| AMCM／MOP | 226,142 | 1986-01-02～2026-09-25 |
| BOC／CAD | 60,645 | 2017-01-03～2026-09-25 |
| RBA／AUD | 19,372 | 2023-01-03～2026-09-25 |

最早／最新取自取得的資料，不把 provider 目錄界線直接當成功覆蓋。
來源定向查詢避免 v2 預設 blended rates；一個原生基準請求涵蓋所有報價幣別。
既有 ECB 下載器新增共用請求 Future、8 個已完成快取上限，以及有雜湊證據的歷史前端補洞。
調整開始日期現在真的會查缺少的頭部，不再只追加末端；不捏造週末報價，拒絕零／負／非有限匯率。
ECB 全市場前端修復已接每日排程，本輪沒有把全部 legacy pair 都實網重抓當作完成證據。
[Frankfurter 官方來源與 v2 文件](https://frankfurter.dev/)。

### 實價登錄：原檔完成不等於每列無缺陷

已保存 59/59 個實際發布批次，其中官方 `101S1` ZIP 本身為空，保留空檔證據，不無限重試。
28,231,384 列含主表與子表。243 個 CSV 邏輯紀錄因欄數或超長欄位異常隔離；
其中 `110S4` 的破損引號曾吞入數千實體文字行，所以 243 不是精確遺失交易數。
原 ZIP 與隔離欄位均保存，不猜測逗號位置來湊成完整交易。
508 筆日期疑點另標記；未來交易日不納入正規日期上下界，過早日期保留原始民國字串並標記需核對。
例如來源出現 1921 年，不能因此宣稱實價登錄有 1921 年起連續歷史。

CSV、ZIP CRC、路徑、解壓大小、欄位數、日期、物件雜湊均有檢查。
原始 ZIP 在正規化前先安全落盤；解析修正可重用本機檔，不必耗新配額。
本次最後一批修復已驗證 **0 次網路請求**。
目前收據引用約 1.23 GiB 原檔＋正規化檔；不含早前修正留下的舊 generation，不是整個目錄實體總大小。

## 既有來源的真正阻擋

| 來源 | 現況與本輪處理 |
| --- | --- |
| FinLab | 保留既有排程；3 個必要鍵仍受阻：`broker_transactions` 大表資源預算、`dividend_otc:權息` 與 `management_change_events:變更交易開始日` 來源無非空值。本輪未完成大表有界分區，沒有突破記憶體限制或製造空值資料 |
| FinMind | 沿用 free／Sponsor／complement 分工及共同配額；free lane 10,686/10,686 個已列交易日工作完成，不代表 111 個 lane/dataset 全史完成。Sponsor 仍運行，新聞仍停用 |
| TAIFEX | 公開歷史來源收據 state=complete，但 completion_claim 僅 source acquisition；1,836 筆仍等待可驗證次交易日可用性。不是 1,836 筆網路下載失敗，本輪沒有抹除日曆 gate |
| Dune | 已修分頁完整性、offset、total、重複累積記憶體，以及把「處理過」誤當「取得」的進度。最新一輪為 64 個待辦、0 個新取得、1 個 HTTP 402、63 個未啟動；不再顯示 100% 或零秒 ETA。本輪未觸發付費查詢 |
| Finnhub | 有效 key 實測 AAPL recommendations 回 4 期、earnings 回 4 季；EPS estimates 為 HTTP 403。只有能力探測，不宣稱免費全市場共識歷史可得，也未另啟一套全市場抓取 |
| ToAlpha | 保留互動查詢。2026-09-24 條款明確要求書面授權才可批量建立資料庫或用於 AI 訓練，因此未啟用該用途的批量排程；請求配額不等於使用授權 |

Dune 已備份舊 summary／progress，再依實際 HTTP 402 訂正後續未啟動工作的 credit／subscription 標籤；
throughput 只算新取得分區，attempt throughput 另列。沒有動到之前已下載的 Dune 歷史。
[Finnhub 能力實測](../artifacts/data_quality/provider_repair_2026-09-27/finnhub_capabilities.json)、
[ToAlpha 現行條款](https://toalpha.tw/terms)。

## 排程、資源與操作

新增兩個每小時 timer，交易日與休市日都運行：

- `stockagent-public-economic-history.timer`：Census、BEA、MOI 獨立 provider 並行，單 provider 有寫鎖、共用 limiter、每輪 60 次上限；不是 60 次／秒。BEA 每請求至少 10 秒、回應 16 MiB 上限。
- `stockagent-openbb-public-priority.timer`：僅修補／追加同一 OpenBB manifest；不開第二組下載流量。

公開經濟資料 worker 設定 Nice=15、MemoryHigh=1 GiB、MemoryMax=2 GiB、單回應最多 64 MiB、
預留 20 GiB 磁碟空間。最近完整 MOI 批次 systemd 實測約 1 GiB peak、3 分 20 秒 wall time；
此數字是當時剩餘 42 個網路請求工作量，不是全資料下載 ETA。
正常未改動的版本只查檔案簽章，變動才重驗 SHA-256，避免每小時掃全部歷史 bytes。

Numeric TTL 為一天；MOI 目前批次一天、歷史季度 30 天；Census／BEA 增量保留兩個觀測年修訂區，
每 90 天重查全期間。認證失敗冷卻一小時，其他失敗依類型短暫退避及 Retry-After，不一律冷卻七天。
Frankfurter 沿既有 daily-all-markets 排程執行，未新增平行來源寫入者。

先修正 `.env` 中 **`CENSUS_API_KEY`**、**`BEA_API_KEY`**。BEA 需完成啟用信；
Census 目前值被拒，請換成官方有效 key。不要把 key 貼到聊天或網頁。
設定完成後可手動觸發，不用等交易日；既有認證錯誤退避仍可能等待最多一小時：

```bash
systemctl start stockagent-public-economic-history.service
systemctl start stockagent-openbb-public-priority.service
source scripts/runtime_env.sh
run_fintech_python -m scripts.audit_public_provider_repairs
```

首次只看計畫、不抓取：

```bash
source scripts/runtime_env.sh
run_fintech_python -m downloader.download_public_economic_history
run_fintech_python -m scripts.queue_openbb_public_priority
```

稽核程式僅按計畫中精確 task ID 讀取 SQLite 與 Parquet footer，不重掃全庫，不呼叫 provider。
OpenBB footer／row count 驗證不同於全內容 hash 或 PIT 稽核，報告保留這項限制。

## 面板與驗證

沿用既有面板 UI；新增 91 個逐資料集收據列：Census 18、BEA 9、MOI 59、Frankfurter 5。
顯示實際首末觀測、筆數、錯誤／隔離、下次檢查及未知 ETA；台灣地政與外匯／總體分類分開。
依 dashboard 品質檢核採本機收據投影，不因瀏覽網頁呼叫上游，也不在刷新時全掃 Parquet。
已實際驗證公開 `/data-monitor/api/status` 返回 91 列；未做瀏覽器畫面渲染回歸，未改頁面版型。

22:40 面板實測 MOI 7 列 current、52 列 degraded，是日期疑點與隔離資料的品質警示，
不是 52 個 ZIP 都沒下載。沒有為了讓面板全綠而移除這些警示。

509 項相關測試通過；新增來源解析、精度、分頁、原檔重用、查詢去重、時間窗、閒置喚醒、
直接 script／module 啟動、負值／版本與面板收據測試。`ruff` F/E9、shell 語法與 `git diff --check` 通過。
擴大檢查舊 crypto 合約測試時另有一項失敗：缺少
`services/discord_bot/markets/tw_day_trade_multi_basis_projection_l1_gelu.yaml`；沒有為此改動無關策略。

新經濟歷史及 v2 匯率維持本機 `publish:false`；沒有同步新原始資料到冷庫、遠端、訓練，
沒有把 observation date 偽稱為 publication timestamp，也沒有聲稱所有 provider 已全部抓齊。
