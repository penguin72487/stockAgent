# FinMind 全清冊、完整性與排程修復（2026-09-29）

## 結論與時間邊界

截至台北時間 2026-09-29 22:48，**尚未全部抓全，回補已重新執行**。
先前 `current_queue` 只表示當時已建立的工作沒有到期項目；沒有包含未接通的
逐檔端點，也沒有證明空回應、股票歷史名單、欄位或所有年份完整。

本次對照官方資料集文件與三條既有下載管線，清冊共有 **106 個唯一項目**，
包含使用者明確停用的新聞。131 個管線別名不是 131 種不同資料。
清冊涵蓋本次取得的官方 `llms-full.txt` 資料集標題；`TaiwanFuturesKBar`
另有官方衍生品文件。不是對未公開、帳號未授權或未來新增商品的完整性保證。

- [逐資料集 CSV 清冊](../artifacts/data_quality/finmind_repair_2026-09-29/final_inventory/finmind_query_ranges_20260929T144658943276Z.csv)
- [同版 JSON 契約與來源](../artifacts/data_quality/finmind_repair_2026-09-29/final_inventory/finmind_query_ranges_20260929T144658943276Z.json)
- [修復前面板](../artifacts/data_quality/finmind_repair_2026-09-29/before.json)
- [22:48 面板驗收](../artifacts/data_quality/finmind_repair_2026-09-29/acceptance_status.json)

CSV 的日期是實際觀測範圍／請求下界，不能以首末日期取代中間完整性證明。
`observed_empty` 是查過但沒有回傳數值，不是已取得資料，也不是來源一定錯誤。

## 實際檔案驗證

沿用資料品質流程，分開檔案正確、來源可取得、歷史完整、發布時點四個命題。
下表是本次掃描當時的檔案頭；管線繼續運行，因此不是永不變動的即時總數。

| 管線 | 已檢查收據 | 校驗失敗 | 證明範圍 |
| --- | ---: | ---: | --- |
| Complement | 36,796 | 0 | 身分、大小、SHA-256、Parquet 筆數 |
| Sponsor | 155,737 | 0 | 身分、大小、SHA-256、Parquet 筆數 |
| Free 兩組市場日內統計 | 10,688 | 0 | 以上加來源原生時間格；0 個 partial |
| 合計 | 203,221 | 0 | 不等於所有來源內容、欄位或 PIT 皆完整 |

Free 兩組各 5,344 個交易日、各 10,859,074 筆。
證據：[Complement](../artifacts/data_quality/finmind_repair_2026-09-29/complement_integrity.json)、
[Sponsor](../artifacts/data_quality/finmind_repair_2026-09-29/sponsor_integrity.json)、
[Free](../artifacts/data_quality/finmind_repair_2026-09-29/free_integrity.json)。

## 已修正的原因與實作

1. **美股識別碼錯誤**：名錄 `BRK/A` 等斜線代碼被價格 API 拒絕；實測
   `BRK-A` 可取回數值，`BRK.A` 是空回應。加入可稽核的別名遷移，不任意改點號。
   13 個原失敗項目中，12 個已取得非空完整歷史請求結果；`RAC-WS` 仍為空回應。
   原錯誤工作及收據不刪除，保留 `identifier_alias` 與遷移前狀態。
2. **海外日價更新太慢**：原成功後 30 天再查，改為每日檢查、7 日重疊增量，
   定期完整重查。已停止更新的舊序列與空識別碼採較低優先級再查，避免每天刷舊名單。
   發現重疊區間 `Adj_Close` 改變時即重抓完整歷史，避免只更新尾端造成調整基準混雜。
   US 的 08:00 是官方更新說明；其他海外市場的 08:05 為檢查時段，**不是發布時間證明**。
3. **增量安全**：先驗證舊收據／檔案、日期、身分、重複日，再保留舊區間並替換重疊段。
   本機基底損壞時改完整重抓。非空歷史不會被一次空回應清空；保留所有原收據版本。
4. **配額被保留到無法回補**：大量海外 priority-0 工作不能把整個帳號配額全部占作預留。
   海外預留最多帳號每小時配額的四分之一；台股固定發布工作另外保留。
   Complement 在流量允許時，每四次派工給非追新工作一次機會，避免歷史工作飢餓。
   仍共用既有跨行程 limiter、流量帳本與官方用量樣本，沒有建立第二個配額桶。
5. **更正工作假性卡住**：來源回空但已查過的更正工作，改記
   `observed_empty_unverified`，不是永遠顯示尚未發送的 `queued`；也不標為 repaired。
   後續取得可驗證收據仍可轉為 repaired。
6. **官方更正排程**：審閱並加入 9/28、9/29 公告的確切內容雜湊及範圍，重抓
   `TaiwanStockEvery5SecondsIndex` 全歷史收盤指數，以及適用的逐筆／分 K 修正區間。
   舊 `001` 指數不偽造轉換，改排官方 `TAIEX`；指數分 K 下界為 2005-01-03，
   不跟一般股票的 2019 起點混用。每日 06:10 公告檢查含休市日持續啟用。

對照：[美股更新規則](https://finmind.github.io/tutor/UnitedStatesMarket/Technical/)、
[官方更正公告](https://finmind.github.io/WhatIsNew/)。

## 原未接通的項目與現在分工

| 工作 | 來源／策略 | 排序 |
| --- | --- | --- |
| 期貨價差日交易、可轉債月份分析、兩種資產交換日資料 | 每商品盡可能一次取完整範圍；共用 Complement SQLite | 一般必要歷史 |
| 股票／期貨／美股分鐘線、股票與權證分點明細 | 可續跑的商品×日游標；每資料集只具體化最多 128 個未結工作 | 大量非 tick 歷史 |
| 股票／期貨／選擇權 tick、期貨價差 tick | 同一游標與流量帳本 | 最低優先級 |
| 券商分點統計 `TaiwanStockTradingDailyReportSecIdAgg` | 從驗證過的股票分點原始明細本機彙總，0 次額外 HTTP | 父資料到齊後 |
| `TaiwanFutOptTickInfo` | 期權合約名錄快照 | 參考資料 |
| 股票／期貨／選擇權即時快照 | 固定時段留存，不宣稱可回溯取得過去即時快照 | 小型固定更新 |

新增合計 13 個歷史 API 資料集、1 個本機衍生表、1 個名錄與 3 個快照來源。
權證以券商查詢取得該分點所有權證，避免只有現存權證名單造成到期商品遺漏。
期權日線與原已由 Sponsor 負責的全市場端點仍沿用原 owner，不重複啟動 Complement。

官方摘要有兩處不足，已對照完整範例及實測修正：

- 券商統計端點還需要 `securities_trader_id`。逐股票×逐券商下載會重複原始明細，
  改由原始買賣股數計算總量及價格加權平均。保留未四捨五入的本機計算值、父 SHA、
  衍生標記，**不是另外一份官方原始統計回應**。
- 權證分點要用專屬 endpoint、`securities_trader_id` 與 `date`，不能把券商當股票代碼。
  已實測 5920／2023-06-21 取得 7,641 筆，9800／2026-09-24 取得 12,698 筆。
- 即時快照使用專屬 endpoint。期貨／選擇權空 ID 全市場查詢會被拒絕，改用驗證過
  合約名錄的商品前綴；已取得 TXF、TXO 與其他商品的非空回應。未交易商品可正常回空。

證據：[歷史來源試取](../artifacts/data_quality/finmind_repair_2026-09-29/supplemental_probes.json)、
[券商彙總比對](../artifacts/data_quality/finmind_repair_2026-09-29/broker_aggregation_probe.json)、
[權證修正後試取](../artifacts/data_quality/finmind_repair_2026-09-29/warrant_broker_probes.json)、
[期權快照試取](../artifacts/data_quality/finmind_repair_2026-09-29/derivative_snapshot_probes.json)。
初次試取檔保留已修正前的錯誤，不回寫成綠燈。
官方參照：[籌碼面](https://finmind.github.io/tutor/TaiwanMarket/Chip/)、
[即時 API](https://finmind.github.io/tutor/TaiwanMarket/RealTime/)。

## 執行與驗收

- Sponsor 與 Complement 已重啟／啟動，Free 持續執行。
- 22:48 Sponsor 正在重抓歷史指數：227 個正值分區、4 個 in-flight、5,113 個 pending；
  被重抓的舊檔仍保留，pending 不代表舊檔消失。
- 新增資產交換固定收益來源已寫入 121 份非空收據、3,132 筆；另外 35 個商品回空。
- 面板 `/finmind/api/status` 已驗收 updating，主責工作 failed=0、invalid_request=0。
  Complement 留存的一筆舊 `TaiwanDailyShortSaleBalances/3658` HTTP 504 屬 Sponsor 接手的
  委派別名，不以刪除紀錄來製造零錯誤，也不另開重複 per-ID 請求。
- 官方帳號上限 6,000 次／小時；用量每分鐘取樣。22:48 本機近一小時帳本 935 次，
  官方取樣 858 次，兩者取樣時刻／窗口不同，不能要求完全相等。
- 共 737 項 FinMind／共享 FinLab 回歸通過；另做受影響的收據、路由、分工、日曆、
  彙總、預留配額及頁面回歸。未執行模型訓練，未改交易服務。

## 還不能宣稱完成的部分

目前約 7,778 萬個未具體化的「已知商品×日曆日」候選，包含休市日、上市前、
下市後與本來無交易的組合。**這不是少了 7,778 萬根 K 棒，也不是精確 HTTP 次數**。
會持續以實際日曆／有效來源縮小；面板把尚未展開的範圍算入，不再拿前 128 個工作
當全部歷史分母。未知商品生命週期、上游空值及新商品仍不構成可證明的完成日期。

確認是上游本身的限制包括官方公告：2005 年第一季 23 個交易日的收盤指數尾差
0.01、2017-05-08 部分上櫃指數缺 09:00:00 原始點。保留原值及標記，不能捏造補點。
`RAC-WS`、部分權證／選擇權空回應則**尚不能斷言來源錯誤**，不列成已取得數值。
即時快照只從實際抓取時間累積；所有新資料仍是 raw/research，未據此宣稱 PIT／可交易。
