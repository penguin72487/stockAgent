# FinMind 免費台灣市場歷史（2026-09-25）

本機 `data_finmind` 是原始研究工作區，`configs/data_sync/packed_datasets.json` 將 `finmind-free` 登錄為 `publish: false`。它沒有自動進入 TW 官方資料、FinLab 表、冷庫或訓練特徵。**依使用者要求，不下載新聞**，也不碰需 FinMind backer／sponsor 的逐筆、分 K、分點及其他付費資料。免費來源清單依 [FinMind 官方資料集目錄](https://github.com/FinMind/FinMind-MCP/blob/master/knowledge/datasets.md)；當前目錄有 53 類 Free／Free(w/ data_id)，新聞排除後為 52 類。現有主下載器管 4 類，補充下載器管另外 48 類。

## 自動來源與驗證範圍

| 資料集 | 取得方式 | 完成條件 |
|---|---|---|
| `TaiwanStockTradingDate` | 每約 20 小時查一次，自 2005-01-01 起 | 提供預排交易日；未來日不算行情完成 |
| `TaiwanStockStatisticsOfOrderBookAndTrade` | 每交易日一請求，自 2005 年起 | 09:00–13:30 的實際 1 分或 5 秒連續格點，逐日收據；全市場累計委託／成交，不是逐檔 L2 |
| `TaiwanVariousIndicators5Seconds` | 同上 | 同樣逐日檢查實際格點；不預設早期真的每 5 秒 |
| `TaiwanStockInfoWithWarrant` | 每日 14:00 後擷取不帶日期篩選的全表 | 保存當日全表來源快照；表內 `date` 不等於已驗證的歷史 PIT 名單 |

官方 [API 說明](https://finmind.github.io/en/quickstart/)列出無 token 300 次／小時、註冊 token 600 次／小時。下載器在兩種情形都使用比上限稍慢的主機共用節流器，無併發搶同一來源額度；token 可選填 `.env` 的 `FINMIND_TOKEN`。流量額度不等於付費資料授權。[兩種盤中來源](https://finmind.github.io/tutor/TaiwanMarket/Technical/)文件稱從 2005 年起；本機 2005-01-03 和 2010-01-04 實測是 271 個一分鐘點，2015-01-05、2020-04-06 是 3,241 個五秒點，所以逐日依實際回應驗證，不能把整段標成五秒史。

官方一日資料若空、缺格點或失敗，非空列仍保存，並留下 `provider_empty`／`partial`／`failed` 收據短期重試；**不補造 K 線、不以 HTTP 200 當資料完整**。當日只在臺北時間 14:00 後進入候選，交易日 08:20–09:10 暫停背景請求。排程優先最近五個交易日，再由 2005 年起補歷史。檔案與收據原子寫入；`status.json` 提供每來源已驗證日數、首末日期、筆數、待補數與只按流量計算的最低網路時間。後者不是可信完工倒數，因為來源缺口與重試不可預知。

## FinLab 未能證明涵蓋的免費來源

FinLab 的鍵名、列數或最新快照不能證明它與 FinMind 同欄位、同歷史起點、同逐股覆蓋。因此下面 48 類 FinMind 原始來源獨立保存、獨立顯示，**不是直接覆蓋 FinLab 或宣稱 48 類皆是全新特徵**。對重疊欄位仍須按標的／日期／數值／發布時間核對後，才可進入特徵表。

| 分組 | FinMind Free 來源 | 工作粒度 |
|---|---|---|
| 主檔 | `TaiwanStockInfo`, `TaiwanSecuritiesTraderInfo`, `TaiwanStockActiveETFInfo`, `TaiwanFutOptDailyInfo`, `USStockInfo`, `UKStockInfo`, `EuropeStockInfo`, `JapanStockInfo` | 每日來源快照；取得代號清單 |
| 全市場與總經 | `TaiwanStockTotalMarginPurchaseShortSale`, `TaiwanStockTotalInstitutionalInvestors`, `TaiwanStockDelisting`, `TaiwanStockSplitPrice`, `TaiwanStockParValueChange`, `GoldPrice` | 從官方明列的起點開始；五個較小的來源先用一個完整歷史範圍請求，驗證後在本機拆成逐年收據；黃金量大，仍按年抓 |
| 台股逐檔 | `TaiwanStockPrice`, `TaiwanStockPriceAdj`, `TaiwanStockPER`, `TaiwanStockDayTrading`, `TaiwanStockPriceLimit`, `TaiwanStockMarginPurchaseShortSale`, `TaiwanStockInstitutionalInvestorsBuySell`, `TaiwanStockShareholding`, `TaiwanStockSecuritiesLending`, `TaiwanStockMarginShortSaleSuspension`, `TaiwanDailyShortSaleBalances`, `TaiwanStockFinancialStatements`, `TaiwanStockBalanceSheet`, `TaiwanStockCashFlowsStatement`, `TaiwanStockDividend`, `TaiwanStockDividendResult`, `TaiwanStockMonthRevenue`, `TaiwanStockCapitalReductionReferencePrice` | 免費方案逐檔 `data_id` 請求；`TaiwanStockInstitutionalInvestorsBuySellWide` 由本機長表轉置產生，另外保存帶來源收據，不再消耗一次 API 額度 |
| 期貨與選擇權 | `TaiwanFuturesDaily`, `TaiwanOptionDaily`, `TaiwanFuturesInstitutionalInvestors`, `TaiwanOptionInstitutionalInvestors`, `TaiwanFuturesDealerTradingVolumeDaily`, `TaiwanOptionDealerTradingVolumeDaily` | 每個期選主檔代號獨立請求；券商量的全市場不帶代碼查詢要求較高方案 |
| 海外股票 | `USStockPrice`, `UKStockPrice`, `EuropeStockPrice`, `JapanStockPrice` | 各市場主檔逐檔請求 |
| 固定指標代號 | `TaiwanStockTotalReturnIndex`, `InterestRate`, `CrudeOilPrices`, `GovernmentBondsYield`, `TaiwanExchangeRate` | 官方文件列出的指數、央行、原油、美債年期與 19 個幣別逐代號請求 |

補充下載器 `downloader.download_finmind_complement` 使用 `data_finmind/complement/queue.sqlite3` 記錄任務、短期重試及下次查新；非空來源回應以不可變雜湊 Parquet 和原子收據保存。兩支下載器共用 `finmind-v4-data` 主機限流器及 `data_finmind/request_traffic.sqlite3` 流量觀測，不會各自占滿每小時 600 次。工作優先順序為主檔／當年、2014 年後全市場、台股逐檔、早期全市場、期選、海外；市場開盤保護時間暫停背景補抓。磁碟可用空間低於 25 GiB 則停止新增請求。

**證據界線：**逐檔任務只有「這次請求取得了非空回應」，不代表供應商歷史無缺、已納入所有已下市代號、已驗證發布時間或可直接訓練。`observed_empty`、`failed`、`not_entitled`、未知代號分母各自列出。`TaiwanStockInfo` 等每日主檔是當前快照；下載器另外聯集本機 TWSE／TPEx 官方下市清單，2026-09-25 實測比 FinMind 主檔額外多 235 個代號（需依實際來源再查新），但這也不是全部歷史代號的完整證明。FinLab 現有的 6 個未取得鍵，不能在未逐欄比對前宣稱被 FinMind 補上。這份下載器不把 FinLab 資料與 FinMind 來源自動合併。

2026-09-25 對照[官方完整資料集說明](https://finmind.github.io/llms-full.txt)後修正查詢契約：`TaiwanExchangeRate`、`TaiwanFuturesDealerTradingVolumeDaily`、`TaiwanOptionDealerTradingVolumeDaily`、`TaiwanStockCapitalReductionReferencePrice` 的 Free 存取需要指定 `data_id`；不帶代碼取全市場要求 Backer／Sponsor。實測指定 `USD`、`TX`、`TXO`、`2330` 均回 HTTP 200；其中 `2330` 在選定日期空回是可接受的事件稀疏性。舊 508 筆遭拒的逐年任務保留為 `deprecated_query_shape` 稽核紀錄，不再排程，也不計入新進度分母。官方已明列起點之前的舊排程記為 `outside_documented_range`，不冒稱曾從 API 查到空資料；例外是 `TaiwanStockParValueChange`：文件稱 2020 年起，但既有 API 收據確有 2019-09-09 一列，因此改用一次 1900 年起的全史查詢尋找實際最早資料。來源空回的年份算「已查驗」，**不算有數值的分區或資料筆數**。

依[官方 IP 封鎖政策](https://finmind.github.io/en/BanIPPolicy/)，權限、無效 token 與參數錯誤不再自動重試；403 `ip banned` 全域暫停至少 30 分鐘，402／429 配額限制依 `Retry-After` 或至少一小時延後。修正 token 或查詢參數後，先停止補充服務，再用 `run_fintech_python -m downloader.download_finmind_complement --retry-blocked --max-requests 1` 明確重排；確認回應後再啟動服務。大量 5xx／傳輸錯誤才短期重試。這些是本機 API 使用紀律，**不是帳號跨應用配額的實測上限**。

整段下載不是無條件使用：本機 `GoldPrice` 已有單年逾九萬列的回應，仍按年分塊；較小的五類全市場來源先一次請求完整歷史，再核對先前非空年份沒有變成空值，最後按年原子保存。實測 `TaiwanStockDelisting` 單次回傳 621 列、2001–2026 共 26 年；`TaiwanStockTotalMarginPurchaseShortSale` 單次回傳 18,999 列，已寫回逐年收據。後續新資料仍更新當年分區，無須反覆下載全部歷史。整體融資融券與法人總表分別排在官方 21:00、15:00 發布後 10 分鐘查新；未記載發布時刻的下市／分割／面額事件表每日收盤後查一次（**不宣稱 14:00 為真實發布時刻**）。[官方 API 規格](https://finmind.github.io/en/quickstart/)允許只提供 `start_date` 取得從該日起至最新的資料；非同步逐代號查詢仍會佔用逐次配額，不能視為免費批量捷徑。

### 與 FinLab 六個未取得鍵的實際關係

2026-09-25 本機 FinLab 面板為 1,104／1,110 鍵已取得。這六鍵是**FinLab 的鍵**，不能以 FinMind 資料集數量直接相減。可行的補充候選如下；「近似」不代表欄位定義、歷史覆蓋或發布時點相同。

| FinLab 未取得 | FinMind Free 可做什麼 | 不可聲稱的部分 |
|---|---|---|
| `dividend_otc:權息`（供應商整欄空值） | `TaiwanStockDividendResult`／`TaiwanStockDividend` 可逐檔取得除權息結果與股利資料 | 尚未驗證 `權息` 的同義映射；不能補成原欄位真值 |
| `management_change_events:變更交易開始日`（供應商整欄空值） | Free 清單沒有已確認等價的交易狀態起始日欄位 | 不能用價格／處置日期推造公告欄 |
| `after_market_fixed_price:資料來源`（高記憶體整表） | Free 清單沒有已確認等價的盤後定價來源標籤 | 不把一般收盤價當盤後定價來源 |
| `broker_transactions`（高記憶體整表） | `TaiwanSecuritiesTraderInfo` 只有券商主檔；FinMind 分點交易 `TaiwanStockTradingDailyReport` 是 Sponsor | 主檔不等於逐日分點成交 |
| `tw_minute:2330`, `tw_tick:2330`（日期分區） | FinMind Free 不含歷史逐檔分鐘或 Tick；原 FinLab 專用分區下載器持續處理 | 不以日價冒充分鐘／Tick |

因此本次真正取得的是**額外原始來源與年份候選**，不是宣告這六個 FinLab 原始鍵已被修復。FinMind 免費來源有不少與 FinLab／TWSE／TPEx 資料重疊；做訓練前須比對來源定義和 point-in-time 口徑。

免費新聞 API 只能逐股逐日查；本機實測不帶股票代碼的全市場查詢要求升級會員。為避免每年數十萬至數百萬請求消耗兩種主要來源額度，且依使用者最新決定，**`TaiwanStockNews` 不在下載器、排程或訓練中**。

## 操作

```bash
source scripts/runtime_env.sh
run_fintech_python -m downloader.download_finmind_free --max-requests 4
run_fintech_python -m downloader.download_finmind_complement --max-requests 4
# 啟用低 CPU／I/O 權重常駐服務；第一次成功後持續續補。
sudo bash scripts/install_registered_data_refresh_services.sh finmind-only
systemctl status stockagent-finmind-free.service
systemctl status stockagent-finmind-complement.service
```

查資料與進度：`data_finmind/status.json`、`data_finmind/complement/status.json`、`data_finmind/complement/queue.sqlite3`、`data_finmind/calendar.json`、兩個工作區的 `receipts/` 與 `parquet/`，以及資料監控面板的 FinMind 群組。停用相應服務可使用 `sudo systemctl disable --now stockagent-finmind-free.service stockagent-finmind-complement.service`；**不要刪除原始檔或收據**。舊日格點可能是來源本身的 1 分鐘觀測；任何研究模型都需再校驗發布／可用時點與適用市場。

獨立追蹤頁在 `/finmind/`，唯讀 API 為 `/finmind/api/status`。`data_finmind/request_traffic.sqlite3` 從啟用後保存本站兩支下載器每次對外請求的時間與資料集；面板的滾動 60 分鐘用量不等於 FinMind 帳號跨程式總用量。追蹤不足一小時時顯示觀測下界，沒有固定重置時刻證據時不捏造倒數。頁面不呼叫 FinMind，只讀本機狀態與收據。
