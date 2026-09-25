# FinLab 歷史來源設定與台股缺口盤點（2026-09-23）

結論：2026-09-23 帳號升級後，原先 5 個 VIP 限定鍵已全部成功下載，精選候選達 **19/19**；另有 2 個目錄擴充鍵，合計 **21 份本地 Parquet 通過 SHA-256／列數稽核**。舊 Free 快取曾大多截到 2018 年；強制雲端刷新後台股收盤價、營收、財報等均延伸至 2026 年。`get_status()` 方案標籤一度仍顯示 Free，但原 VIP 限定鍵成功與每日 5,000 MB 額度是實際權限證據；此時**不必重新登入**。資料目錄鍵名、帳號可下載、逐檔完整、PIT 可訓練、可跨主機再散布仍是五個不同的判定。

## 2026-09-25 修正與當日證據

- 證交所官方日曆把 9/25 判作中秋節休市。舊 FinLab `ExecCondition` 卻只看週一至週五，08:20–09:10 擋住休市日擷取；現僅 FinLab 服務改用本機已驗證的 TWSE 開休市表。若官方日曆遺失／不可信，平日仍保護開盤時窗，不會猜成休市。額度重置日改以 FinLab 官方 08:00 台北時間為準，取代「上次查詢滿 24 小時」的滾動計時；休市日重置後即納入新一輪來源查詢。FinLab 配額確認、逐分鐘觀測及 50 MB 保留仍維持。
- 官方 [更新日誌](https://finlab.finance/docs/en/change-log/) 已撤回 2.0.22/2.0.23；升級 2.1.1 後，原先報 `vip_only` 的 `inventory` 與 `rotc_broker_transactions` 在同一個 `vip_m` session 都成功。前者 23,212,544 列、2016-11-04–2026-09-18、約 179.9 MB Parquet；後者 22,467,309 列、2018-01-02–2026-09-24、約 174.7 MB Parquet。不能把舊版 SDK 的錯誤當成帳號未購 VIP。
- 09:10 目錄仍為 1,110 鍵，整表收據從 1,102 增至 **1,104 鍵**。剩餘六鍵是 `dividend_otc:權息` 和 `management_change_events:變更交易開始日`（2.1.1 再查仍分別 2,395×1,153、123×61 且零非空值）；`after_market_fixed_price:資料來源` 和 `broker_transactions`（整表資源限制）；`tw_minute:2330` 和 `tw_tick:2330`（日期分區，不是整表）。相鄰 `dividend_otc:權值／息值／權+息值` 與 `management_change_events:變更交易` 有值，但不是缺欄的原始值替代證明，不偽造全空欄位。
- 按 [FinLab 官方 intraday 規格](https://finlab.finance/docs/en/details/intraday/) 新增逐股票、逐日、可續抓 Parquet/收據。已實際取到 2330 的 2026-06-01 和 2026-09-24 分鐘／逐筆資料；逐筆同一時間戳的不同 `sequence` 均保留。預設從官方明示樣本日 2026-06-01 回補，不能因此聲稱這就是供應商最早歷史。6/2 的明確供應商代碼是 `not_ready`；6/2–6/11 兩種分區的首輪探測均拋 `DataError`，其他日是否同因尚待逐日代碼確認。未上架交易日不偽造完成，已證實休市／無交易日才記為完成。每輪最多 8 個分區，額度每日重置後（休市亦然）在整表查詢前優先處理，交易日開盤保護時段除外。
- `after_market_fixed_price:市場別` 已有舊版收據，但 2.1.1 強制追新 90 秒達約 9.9 GiB RSS，已停止並標記「已存舊版、追新資源暫緩」，不能稱為最新。`broker_transactions` 舊整表探測曾耗約 726 MB 流量和 8.8 GiB RAM、逾 50 分鐘未完成；自動排程不重複這種無收據耗損。這兩個鍵需另行驗證官方支援的伺服端分區或在受控記憶體下做分段提取；一般 `data.get(start,end)` 只保證 Arrow 讀取切片，不能保證網路端小分區。

以上是逐鍵原始來源取得證據，不是 PIT 訓練驗證或全史完整證明。新版打包仍須通過全部目錄鍵的新鮮度與 SHA-256 守門，不能因逐日分區已有兩天或兩個大表修復就提前發布新版。

## 設定和下載

官方 SDK 已固定在 `requirements-finlab.txt`，2026-09-25 已把 `fintech` 環境升至 `finlab==2.1.1`；2.0.22/2.0.23 已被官方撤回，排程拒用較舊版本。未來重建環境可用：

```bash
source scripts/runtime_env.sh
run_fintech_python -m pip install -r requirements-finlab.txt
```

優先在**執行下載器的同一台 WSL 機器**依 [FinLab CLI](https://finlab.finance/docs/cli/) 登入：

```bash
source scripts/runtime_env.sh
run_fintech_python -m finlab login
```

CLI 會產生形如 `https://www.finlab.finance/auth/cli?s=...` 的一次性連結，開啟後在瀏覽器完成登入；CLI 會持續輪詢，成功後把憑證加密存於 `~/.finlab/credentials.json`。空白的 `s=` 網址不是可用登入連結，也不能只複製這個檔案到另一台機器，因為官方 SDK 使用機器綁定加密。登入完成後可執行 `run_fintech_python -m finlab status` 查方案與額度；**不要**把完整 session URL、`token --env` 輸出或憑證貼到對話、commit 或 log。下載器會驗證 SDK 是否能讀取完整 session，沒有可用 session 會直接拒絕，不自行啟動互動式登入。

只有在跨機器或無法互動的排程環境，才從已登入機器執行 `run_fintech_python -m finlab token --env`，私下設定 `FINLAB_REFRESH_TOKEN`、`FINLAB_SESSION_ID`、`FINLAB_API_KEY`。`.env` 不會被程式覆寫。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/download_finlab_history.py catalog
run_fintech_python scripts/download_finlab_history.py discover
run_fintech_python scripts/download_finlab_history.py fetch --dataset 'monthly_revenue:當月營收'
# 分批掃描 1,109 個台股目錄鍵；SDK 自管快取與增量，遇額度邊界停止。
run_fintech_python scripts/download_finlab_history.py sync --limit 8 --refresh-days 1
# 日期分區鍵不可用上面的整表 fetch；本機逐日收據與狀態另見：
run_fintech_python scripts/download_finlab_intraday.py --limit 8
# 帳號升級後若仍看到 2018 年 Free 快取，可針對舊鍵強制雲端刷新。
run_fintech_python scripts/download_finlab_history.py fetch --dataset 'price:收盤價' --refresh
# 不用 API，重驗本地檔案雜湊、列數、事件日期範圍。
run_fintech_python scripts/download_finlab_history.py audit-local
```

`discover` 從 SDK 取得 1,109 個台股可搜尋鍵，存於 `data_finlab/catalog/discovery.json`；**目錄不等於已下載**。`sync` 先處理精選缺口，再分批試其餘鍵，逐鍵留下成功收據或無敏感資訊失敗分類；未驗證價格／值更新不能只靠日期尾端判斷。資料檔以內容雜湊命名，供應商修訂時保留舊版，不改寫已存版本；`runs/latest.json` 留下本輪進度、剩餘候選估計與額度。`data_finlab` 仍是 `publish:false` 的本機帳號研究工作區；原始 FinLab 表和快取不發布到 Syncthing／冷庫。另有僅含合併訓練特徵與精確收據的私有研究 release。

## 升級後本帳號實測結果（2026-09-23）

下表列數是每表**非空日期／期別列或事件列**，不是全部股票的非空數值總數。首末為來源索引；事件長表取 `date` 欄，`key_date` 不是歷史發布日。未來日期的法說會是**會議日期**，不是當下已知的公告日。合計 21 項本地通過稽核；這不證明逐檔、逐年度完整。

| 資料鍵 | 非空列／事件列 | 最早 | 最新 | 結果 |
|---|---:|---|---|---|
| `price:收盤價` | 4,776 | 2007-04-23 | 2026-09-23 | 已刷新；與官方價交叉驗證，不覆蓋官方來源 |
| `monthly_revenue:當月營收` | 260 | 2005-02-10 | 2026-09-10 | 已刷新；日期不等於逐股公告證明 |
| `financial_statement:每股盈餘` | 54 | 2013-Q1 | 2026-Q2 | 已刷新；期別非公告日 |
| `trading_attention` | 91,433 | 2001-01-02 | 2026-09-23 | 已刷新 |
| `disposal_information` | 8,030 | 2001-01-02 | 2026-09-23 | 已刷新 |
| `important_info_announcement` | 1,256,307 | 2006-01-01 | 2026-09-23 | 已刷新；事件時間待與原公告核對 |
| `security_lending:借券餘額` | 4,850 | 2007-01-02 | 2026-09-22 | 已刷新 |
| `tw_taifex_futures_large_trader` | 1,965,712 | 2004-07-01 | 2026-09-23 | 已取得；合約／到期月份粒度另驗 |
| `futures_institutional_investors_trading_summary:多空未平倉口數淨額` | 4,728 | 2007-07-02 | 2026-09-23 | 已刷新 |
| `tw_business_indicators:景氣對策信號(分)` | 578 | 1984-02-27 | 2026-08-27 | 已刷新；實際起點晚於目錄宣稱的 1960 |
| `tw_total_pmi:製造業PMI` | 171 | 2012-07-02 | 2026-09-01 | 已刷新 |
| `taiex_total_index:收盤指數` | 6,847 | 1999-01-05 | 2026-09-23 | 已刷新 |
| `cb_price:收盤價` | 4,847 | 2007-01-02 | 2026-09-23 | 已取得；可轉債代碼不得當股票代碼合併 |
| `foreign_investors_shareholding:全體外資及陸資持股比率` | 4,781 | 2007-04-23 | 2026-09-22 | 已刷新 |
| `investors_conference` | 44,728 | 2007-01-09 | 2026-10-14 | 已刷新；末端是未來會議日期，不是已知公告時點 |
| `tw_etf_beneficiary_stats` | 235 | 2007-01-31 | 2026-07-31 | 已取得 |
| `etl:inventory:大於四百張佔比` | 510 | 2016-11-04 | 2026-09-18 | 已刷新 |
| `block_trade:成交金額` | 2,669 | 2015-01-05 | 2026-09-23 | 已取得 |
| `tw_etf_nav_daily:折溢價(%)` | 1,125 | 2023-03-04 | 2026-09-23 | 已取得 |
| `after_market_fixed_price:市場別`、`after_market_fixed_price:成交價` | 各 1,603 | 2020-01-02 | 2026-09-23 | 目錄擴充取得；仍需語義映射 |

升級前的 Free 快取實測為 14/19 項、多數至 2018 年底；另 5 項當時回覆 VIP 限定。這是歷史比較，**不是現在的下載狀態**。[FinLab SDK 更新紀錄](https://finlab.finance/docs/change-log/)也提到 2018 年底 Free 快取，而[舊 FAQ](https://finlab.finance/docs/faq/)仍寫免費至 2021 年底，兩者不一致。

## 候選優先順序與原有本機缺口

表中 FinLab 起點是官方目錄宣稱的供應商**資料起點**，不等於實際已取得起點。右欄描述下載前現有**官方本機資料缺口**，不可解讀成 FinLab 的下載結果；實際結果見上表。本機官方原始表的某些 `date` 是擷取日，不是公告日。

| 優先 | 資料類別／候選鍵 | FinLab 宣稱起點 | 本機現況與可補性 |
|---|---|---|---|
| 高 | `monthly_revenue:當月營收` | 2005-02-10 | TWSE/TPEx 月營收原表共 36,789／29,383 列，但 `資料年月` 僅民國 11506–11508（2026-06–08）；VIP 現可交叉驗 2005–2026，仍須逐股發布日。見[欄位頁](https://finlab.finance/data/monthly-revenue/current-month-revenue)。 |
| 高 | `trading_attention`, `disposal_information` | 2001-01-02 | 本機上市注意 30、上櫃注意 1,498、上市處置 292、上櫃處置 814 列，這些主表擷取日均自 2026-07-18 起；舊史價值高。見[欄位頁](https://finlab.finance/data/trading-attention-stocks)。 |
| 高 | `important_info_announcement` | 2006-01-01 | 本機上市重訊 4,834 列，主表擷取日自 2026-07-18 起。公告事件對齊需另驗原始時間。見[欄位頁](https://finlab.finance/data/important-announcements)。 |
| 高 | `security_lending:借券餘額` | 2007-01-02 | 本機上櫃借券相關主表現為 2026 近期快照；FinLab VIP 可交叉驗至 2026-09-22。見[欄位頁](https://finlab.finance/data/securities-lending)。 |
| 高 | `tw_taifex_futures_large_trader`; `futures_institutional_investors_trading_summary:多空未平倉口數淨額` | 2004-07-01；2007-07-02 | 本機 TAIFEX 相關擷取主表主要自 2026-07-18 起，但另有官方歷史工作區；先依交易日期與合約粒度去重比較，不能相加。見[大額交易人](https://finlab.finance/data/tw-taifex-futures-large-trader)、[法人未平倉](https://finlab.finance/data/futures-institutional-flow)。 |
| 中 | `financial_statement:每股盈餘`；`foreign_investors_shareholding:全體外資及陸資持股比率` | 2013-05-15；2007-04-23 | 可與現有 XBRL／持股來源交叉核對至 2026；財報修訂與公布時點另驗。見[EPS](https://finlab.finance/data/financial-statements/eps)、[外資持股](https://finlab.finance/data/foreign-ownership)。 |
| 中 | `tw_business_indicators:景氣對策信號(分)`；`tw_total_pmi:製造業PMI`；`taiex_total_index:收盤指數` | 1960-02-27；2012-07-01；1999-01-05 | 與官方總經／指數史比較，不能僅用期別或抓取日當 PIT 日。見[總經資料](https://finlab.finance/data/macro-indicators)。 |
| 中 | `cb_price:收盤價`；`investors_conference`；`tw_etf_beneficiary_stats` | 2007-01-02；2007-01-09；2007-01-31 | 三項 VIP 均已取得；可轉債是不同商品代碼，法說會日期不等於公布日期。見[可轉債](https://finlab.finance/data/convertible-bonds)、[法說](https://finlab.finance/data/investors-conference-schedule)、[ETF 受益人](https://finlab.finance/data/tw-etf-beneficiary-stats)。 |
| 中 | `etl:inventory:大於四百張佔比` | 2016-11-04 | 本機集保分散主表 207,009 列，擷取日 2026-09-16–19。FinLab 實際取得 2016–2026，但不補 2014–2016 年 11 月前。見[集保分散](https://finlab.finance/data/shareholding-distribution)。 |
| 低 | `block_trade:成交金額` | 2015-01-05 | 不能補 2014。見[鉅額交易](https://finlab.finance/data/block-trade)。 |
| VIP | `tw_etf_nav_daily:折溢價(%)` | 2023-03-04 | 已取得 2023–2026；不能補更早年度。見[ETF NAV](https://finlab.finance/data/etf-nav-discount-premium)。 |
| 交叉驗證 | `price:收盤價` | 目錄未核實 | 官方本機 TWSE 日價 5,328,187 列、2004-02-11 起；TPEx 日價 4,417,991 列、2003-08-01 起。FinLab 不應覆蓋官方價。 |

上述 19 個鍵是按歷史缺口挑選；其餘 1,090 個目錄鍵會由有額度邊界的 `sync` 逐批測試，不能預先宣稱已取得或與本機現有資料語義不同。逐項收據、股票／商品鍵、單位、源頭發布時間、歷史修訂與合法用途仍須驗證；目錄行數或網站尾端不是完整度。

## 分鐘與逐筆的界線

2026-09-25 排程優先級調整：FinLab 一般整表歷史資料（缺值、追新、到期重試）先跑；其本輪可執行待辦清空後，先完成可發版的私人研究資料處理，最後才允許 Tick 使用帳號**剩餘**額度。Tick 預設額外保留 500 MB 給當日後續一般資料；一般資料仍按原本 50 MB 保留值運作，可用 `FINLAB_TICK_QUOTA_RESERVE_MB` 調整 Tick 專用保留量。每次 Tick 啟動前會用最新本機目錄與收據重新檢查一般待辦，清冊不可信、一般工作仍待處理、額度不足、連續逾時或開盤保護窗時不啟動 Tick。這裡的「一般待辦清空」僅指**當時可執行的任務**；VIP 拒絕、來源全空、過大資料表及短暫冷卻仍是未解缺口，絕不稱為全史完整。只有 FinLab 帳號額度共享的工作在此排序；其他獨立來源服務保持自己的排程。

2026-09-25 起，全市場排程**只向 FinLab 請求 `tw_tick:代號`**，不再以 `tw_minute:代號` 消耗帳號下載額度。`scripts/derive_finlab_minute.py` 從本機 Tick Parquet 依台北時區、時間戳與來源 `sequence` 排序，僅保留 `session=regular` 且成交量大於零的成交，按左標記分鐘計算 OHLCV、`tick_count`、成交量加權 `vwap`；13:30 集合競價獨立列，14:30 盤後定價不混入，無成交分鐘不補價。量維持 `provider_native` 單位，不能直接宣稱股數。合成收據另存於 `intraday/derived_minute/receipts`，記錄原 Tick 物件 SHA-256；舊直接下載分鐘檔及收據保留，但不列入「Tick 合成分鐘」完成數。已存 Tick 可用 `run_fintech_python scripts/download_finlab_market_intraday.py --status-only --derive-existing` 離線補合成，不呼叫 API。90 個既有 Tick／分鐘配對、6,578 根分鐘棒的 OHLCV 與筆數對照完全一致；這是該樣本的來源間一致性，不證明所有歷史交易日或逐時點訓練可用性。後續分鐘覆蓋受 Tick 上架範圍限制，不能以合成填平供應商尚未上架的 Tick 缺口。

[FinLab 官方分鐘／逐筆文件](https://finlab.finance/docs/en/details/intraday/) 的 `tw_minute:代號`、`tw_tick:代號` 是**動態資料鍵**；目錄中的 `2330` 範例不是全市場清冊。免費試用例為 `2330` 的 **2026-06-01** 一天，其他逐股逐日分區須個別驗證此帳號權限及來源是否上架；一般 VIP 日資料成功不等於全部分鐘／逐筆歷史也已上架。此帳號在 2026-09-25 已實際取得 `1101` 的 2026-09-24 分鐘 266 列、Tick 4,622 列，以及 `1102` 的 2026-09-24 分鐘 257 列；但 2026-06-02 多個 `2330` 分區與 `1101` 的 2004-02-11 分區回 `not_ready`。已下市的 `2867` 於 2026-08-31 的查詢也回 `not_ready`。因此「可帶任意代號呼叫」不等於「全市場全歷史均已上架」。其分鐘棒採**左標記**，13:30 集合競價另列，無交易分鐘會省略；不能據此宣稱可補齊本機 2020-03-02 起全市場永豐 1 分鐘 K 缺口。

`scripts/download_finlab_market_intraday.py` 現在將本機官方股票名單、TWSE／TPEx 下市紀錄與最新官方上市櫃基本資料交叉，逐檔建立兩種日分區候選，並以實際 FinLab 回傳檔與收據分開計數。2026-09-25 觀測：2,460 個股票類代號（2,440 個普通股，另明列 20 個六位數 TDR；最新基本資料仍列 1,982、歷史下市或已離開目前名單 478），其中 128 檔沒有本機日線界限，103 檔下市時間早於目前 2003-08-01 的候選交易日下界；這些不是「已抓齊」。分母是 2003-08-01 至 2026-09-24 的候選交易日：分鐘及 Tick 各 8,649,871 個股日分區；2009 年以前使用已下載的 TWSE／TPEx 官方每日行情日期，較早或來源未記載交易日仍未驗證。這是需要驗證的工作量，不是 FinLab 已公布的可取資料量。來源已上架範圍與單分區額度成本未知，無可信總完成 ETA。全市場工作依帳號餘額及磁碟 25 GiB 保留量有界運行，持久化新交易日待辦與較早日期前沿；最近 30 天來源未上架分區在兩小時後可重試，較老失敗則在下次帳號配額重置後按有限比例公平重試，不會讓錯誤重試吞掉全部新資料額度。`partition_not_ready` 保留失敗證據，不能補零或偽稱完整。全市場和 2330 範例都顯示在 FinLab 網頁，`/data-monitor/` 額外列分鐘與 Tick 兩個來源族群。暫未涵蓋 ETF、權證；它們與普通股分開做生命週期與資料粒度驗證，不能混入「所有股票」分母。重建清冊但**不花 API 額度**可用 `run_fintech_python scripts/download_finlab_market_intraday.py --status-only`。

下市股另有每檔末段交易日的優先探測隊列；這只提早驗證來源是否有分區，**不代表**該股票更早年份已補齊。新交易日加入時，未完成日期與較早歷史的游標會持久保存，不重置成只有最新一天的進度。

另以 FinLab 已下載 `price:收盤價` 的 Parquet 頁尾逐股檢查**非空**日價列數，而不是只看資料鍵完成收據。2026-09-25 的 2,460 個股票類代號中，2,252 檔有非空 FinLab 日收盤價欄（含 1,982 檔現有、250 檔歷史下市普通股、20 檔 TDR）；208 檔普通股歷史下市代號在 FinLab 價表中沒有欄位，其中 100 檔本機另有日線、108 檔兩邊都沒有。這 108 檔是明確尚未解決的來源缺口；有欄位的 2,252 檔仍需逐日驗證交易日、價格與發布時點。此逐股清點已顯示在 FinLab 面板，與分鐘／Tick 分區分開計數。

## 訓練可用性界線

- SDK 的 `data.search()` 只證明有鍵名；`data.get()` 後收據才證明此帳號在此時取得了哪些非空索引。升級後使用 `--refresh` 避免把舊 Free 快取誤當 VIP 完整歷史。
- 本次 `financial_statement:每股盈餘` 用 `2014-Q1` 等**財報期別**，不是公告日；不能直接轉成該季首日。[FinLab FAQ](https://finlab.finance/docs/faq/) 也警告部分期別索引不能直接當日期。月營收這個鍵則有供應商日期索引，但仍須逐股驗其公告時間。原始 Parquet 不改寫這些標籤。
- 今日抓到的老資料可能經過後來更正；沒有原始歷史版本時，不得宣稱是當年所見數值。與本機官方資料合併前須逐欄做 symbol、單位、修訂、缺值、公告日、權限與授權核對。
- 下載排程只更新本機原始研究工作區；正式／即時模型不注入 FinLab 原始值。獨立研究 ABI 採用次一交易日／理論財報申報期限映射，但仍標示 `historical_point_in_time=false`，不能宣稱當年實際可見或可執行績效。私有跨機研究 release 是另一個資料集，不改變原始 `finlab-research` 的 `publish:false`。

## 自動排程與本機研究表

FinLab 官方公布帳號每日下載額度於**台北時間 08:00** 重置；這是額度重置時間，**不是各資料鍵的內容發布時間**。`stockagent-finlab-quota-snapshot.timer` 對準台北時間每分鐘整點取樣，包含 08:00；觀測逐筆寫入本機 SQLite，保留舊 JSON 歷史供轉換，不因高頻取樣而每分鐘重寫整份檔案。公開面板顯示近 30 天：近 24 小時逐筆，其餘以 20 分鐘桶降採樣，原始分鐘觀測仍在本機。`stockagent-finlab-local-refresh.timer` 於 08:00:05 啟動，但必須由帳號查詢確認額度已恢復（接近全額，或相對 08:00 前近 30 分鐘觀測有明顯下降）才開始下載。若供應商重置稍晚，08:03 再試，不把日曆時間單獨當成已重置證據。台股開盤保護窗為平日 08:20–09:10：晨間批次只啟動能在 08:18 前容納完整單鍵超時與終止寬限的工作，且不在晨間建研究表／發冷庫；09:10:05 接續，16:10 再做盤後增量。這避免把重置後的額度閒置至 16:10，也不讓未知慢鍵卡住開盤資源。

每輪預設最多 500 批 × 1 鍵，實際會在帳號額度餘量門檻先停，每鍵重啟 SDK，以免多張寬表累積快取造成 swap。單鍵 SDK 最多執行 600 秒，逾時只標記該鍵並暫時跳過；只有連續兩鍵逾時才結束本輪，中間有正常結束的 SDK 呼叫就重置連續逾時計數，避免兩張相隔很遠的大表停止其他可抓鍵。服務有記憶體上限且禁止使用 swap；若單鍵本身超限，必須另行拆分或排除，不能宣稱已下載。既有 19 個精選鍵每日檢查，其餘先測未取得鍵，避免新目錄鍵被舊鍵刷新永久餓死。同步模式現在強制查來源，不以 SDK 舊快取證明「最新」；帳號餘量保留值由 `FINLAB_SYNC_MIN_QUOTA_MB` 控制，預設 50 MB，不再強制另留每日額度 10%。FinLab 一般 `data.get(start,end)` 會先經過完整來源刷新再本機按日篩選，不能用它保證將 `inventory`／券商整表切成小型網路請求；`tw_minute`／`tw_tick` 則另須日期分區且單次最多 31 天，不能沿用整表下載器。排程可用 `scripts/install_registered_data_refresh_services.sh finlab-only` 安裝下載及帳號額度兩組 timer。查看 `data_finlab/runs/latest.json`、收據和 `systemctl status stockagent-finlab-local-refresh.timer`。缺憑證、額度資訊不明時失敗關閉。

2026-09-25 的限額輪次暴露另一個排程飢餓：已有收據、但今日需重查的非精選鍵原按字母排序；若每天可用額度在目錄前段耗盡，後段鍵會一直得不到雲端檢查。現在保持「精選鍵 → 未下載鍵」優先，其餘到期鍵按各自 `source_checked_at_utc` **最舊先查**，缺／壞時點也排前面；仍逐鍵強制雲端、逐鍵寫完整收據、依帳戶額度停止，沒有用本機快取冒充最新。09:14 自然執行已從先前字母前段轉往更舊的 `capital_reduction_otc` 鍵；這是排序生效與公平性的證據，不代表 1,110 鍵已於 24 小時全驗或資料已適合 PIT 訓練。單個寬表欄位可消耗約 75 MB，而小表可少於 1 MB；額度吞吐不能按鍵數線性推估，仍需以完整一個配額週期的實際來源收據評估。相鄰 FinLab 測試 **44 passed**。

2026-09-24 實測：帳號觀測於 07:58:54 為已用 4,720.003／5,000 MB，08:03:54 為 0／5,000 MB。舊下載 timer 仍等到 16:10；更新排程後，10:36 因持久 timer 追補而啟動，約一分鐘內完成的目錄鍵由 82 增至 103，並持續下載。這只證明當次服務與收據有進展；明日 08:00 自動重置啟動仍須按新 timer 的實際日誌再次驗證。

依使用者的新要求，**未追齊全部目錄鍵前不重建／打包新版**。`scripts/finlab_release_gate.py` 要求目錄本身及每一個鍵均於最近 24 小時內完成 `force_download=True` 的來源查詢；所有檔案存在後，stage 再逐一 SHA-256 核對，並把 `catalog_latest_verified`、`source_hashes_verified`、目錄數和收據內容摘要寫入分發收據。冷庫 catalog 同樣要求這兩個欄位與 24 小時發版收據有效期；直接對舊 staged 目錄執行 `stockagent-data publish` 也會失敗。來源雜湊完全相同時不因檢查時間更新而製造 timestamp-only 冷庫新版。帳號額度、目錄規模與 `broker_transactions` 等資源暫緩可能使守門長期無法通過，排程會繼續取得原始鍵但不會發新包；既有已驗證舊版保留，不宣稱最新。這是「最近 24 小時 SDK 所見版本」的操作定義，不是 FinLab 保證全部股票年度完整或歷史 PIT 真值。

逐鍵收據現在記錄完整 SDK 抓取加本機序列化的耗時與 Parquet 大小。網站對同鍵用最近實測，或至少三個同類成功鍵的中位數至較慢樣本估計**單鍵工作耗時**；未有樣本顯示未知。排隊順序、跨日配額與其他同帳號用途沒有可靠上界，故未開始鍵的實際完工日不假裝可算；正在抓的鍵只在實測區間內給暫定完成時間，逾時即撤回倒數。逐鍵表亦顯示最後強制來源查詢時刻、是否待追新及全目錄打包守門。

`after_market_fixed_price:資料來源` 是非數值來源標籤；實測單鍵在此機器觸及 6 GiB 記憶體軟上限，故自動掃描暫緩此鍵，不計為已下載。它仍可從官方目錄看到，也可人工指定下載；日後若需要來源標籤，應先設計分欄／分日期擷取及容量驗證。

`broker_transactions` 於 2026-09-23 的整表 SDK 下載耗用約 726 MB 帳號流量、8.8 GiB 記憶體，在 6 GiB 高水位以上停留逾 50 分鐘且未留下完成收據。已停止該輪工作並列為**資源暫緩**，不再自動重試整表；需先驗證 FinLab 是否提供按日期／欄位分區的受支援介面，再用有界分區回補。這不是「已下載」或「沒有資料」，其餘鍵下次排程仍可繼續。

9/24 起，排程的每個預設單鍵 SDK 子程序有 600 秒上限（`FINLAB_SYNC_KEY_TIMEOUT_SECONDS` 可設 1～3600），單輪至多兩次**連續**超時（`FINLAB_SYNC_MAX_TIMEOUTS`）。只有精確相符的進行中 attempt ID 可被標記為 `timed_out`；剩餘資料維持 `partial`。固定七日冷卻已移除：一般 SDK 錯誤從 5 分鐘起倍增、最多 1 小時；尚未取得的逾時鍵從 30 分鐘起、最多 2 小時；已取得但追新逾時從 15 分鐘起、最多 1 小時；VIP 回覆從 30 分鐘起、最多 4 小時，帳號升級時可顯式強制重試。登入／配額錯誤不做逐鍵冷卻，改由帳號狀態與重置守門。失敗次數只在上次成功後連續計算，成功後重置；額外的 `OnUnitInactiveSec=15min` 讓可重試鍵不必等次日固定排程。這是避免未知大型鍵獨占排程的故障隔離，**不是**官方下載加速或資料已齊證明。預設 `FINLAB_SYNC_BATCH_SIZE=1` 才有逐鍵上限；手動改成多鍵時上限涵蓋整個子程序。

2026-09-24 23:00 台北時間的 1,109 鍵盤點：1,101 鍵有完成收據與本機檔案，8 鍵沒有；另有 1 鍵雖已取得，但最近 24 小時強制來源查詢未成功。網站把「未取得」8 鍵置頂，並逐鍵列出原因、上次嘗試與最早重試時間；此表不把來源目錄鍵名誤認為訂閱權限。當時未取得明細如下：

| 原因 | 鍵 | 現況與修正界線 |
|---|---|---|
| 資源暫緩 | `after_market_fixed_price:資料來源`、`broker_transactions` | 前者是高記憶體來源標籤，後者整表超記憶體／額度預算；無可證實的小型供應商網路分區前不反覆下載整表。 |
| SDK 查詢錯誤 | `dividend_otc:權息`、`management_change_events:變更交易開始日` | 最近回執只有 `ValueError` 類別、沒有原文；根因未證實，短退避自動重試，不宣稱權限不足。 |
| 單鍵逾時 | `inventory`、`rotc_broker_transactions` | 最近一次達 600 秒上限；未取得鍵 30 分鐘起有界退避，另需驗證供應商是否支援伺服端分區。 |
| 呼叫方式不適用 | `tw_minute:2330`、`tw_tick:2330` | 官方 API 要求起訖日期、單次最多 31 個日曆日；一般整表 `data.get(key)` 必然失敗。已從整表排程排除，仍列作未取得，需獨立逐日分區下載器、訂閱與歷史覆蓋驗證。 |

已取得但待追新的 `after_market_fixed_price:市場別` 不列入 8 鍵：本機檔案仍在，最近一次強制來源查詢逾時，短退避後可再試。完整目錄的打包守門仍未通過（當時 1,100 鍵最近 24 小時檢查、8 缺、1 舊）；不能把目前舊版私有冷庫研究表稱為最新。一般 `data.get(start,end)` 的日期篩選不能保證將 `inventory`、券商整表切成較小的遠端請求；不要為了讓進度變綠而跳過這些缺鍵或放鬆守門。

2026-09-25 00:05 台北時間續跑後，SDK 目錄增加到 **1,110 鍵**，新出現的 `delisting_announcements` 已取得，因此變成 **1,102/1,110 已取得、8 未取得**。比「逾時／一般錯誤」更精確的新證據是：`dividend_otc:權息` 的 SDK 回應為 2,395 列 × 1,153 欄、`management_change_events:變更交易開始日` 為 123 列 × 61 欄，但兩者均 **0 非空值**；現在記為 `provider_empty`，不把空值表當成完整資料。`inventory`、`rotc_broker_transactions` 的最新 SDK 回覆則要求 VIP，現在記為 `vip_only`。進一步用官方 `get_status()` 只讀查詢，此本機 session 的 `plan.role` 回覆 **`vip_m`**；因此「帳號有 VIP」與「這兩個鍵仍被拒」是供應商回覆間的實際矛盾，不能推定使用者沒購買、登入了錯帳號，或靠重試必然解決。需向 FinLab 核對該鍵的實際授權／供應狀態，不擅自重新登入。`after_market_fixed_price:市場別` 已存舊版，但最新重查也回覆 VIP 要求，因此仍是「已取得但待追新」。兩個高資源鍵與兩個日期分區鍵仍未取得。短退避和 15 分鐘續跑排程仍會重試合適的鍵；不能靠重試頻率補出供應商回傳的全空值或繞過權限。

00:15 的 `OnUnitInactiveSec` 續跑已由 systemd 實際觸發；下一輪是約 00:30，不是日曆條款原先顯示的 08:00。監控快照現在比較 systemd 的 realtime 與 monotonic 兩種 deadline，取真正較早者，因此「下次排程」會顯示 15 分鐘續跑。此排程時刻是 timer 觸發時間；若鍵尚在短退避、額度不足或進入台股開盤保護窗，該輪可能沒有資料請求，面板不得把它寫成保證完成時間。

## 網頁進度與跨主機界線

既有 [`/data-monitor/`](https://penguin72487.ddnsgeek.com/data-monitor/) 會將 FinLab 的全部 SDK 目錄鍵逐項列於「台股相關」，並顯示專屬下載進度。分母是最近一次 `discover` 的去重目錄鍵；分子只計算有收據且本機檔案仍存在的鍵。畫面另列待下載、權限／供應商失敗、資源限制暫緩、最近 15 分鐘新增、上次成功收據、最近一次帳號配額回執及下次 timer 排程。配額回執**不是即時餘額**；各鍵大小與同帳號其他用途未知，所以全目錄 ETA 必須顯示「無可信倒數」，不能拿目前鍵數速度直接外推。此進度也不代表每鍵所有年度／股票完整，更不代表公告日、歷史修訂與 PIT 已通過訓練驗證。公開頁只展示摘要與鍵名，不提供 FinLab 原始資料或憑證。

使用者已確認跨主機冷庫亦為其私人、非商業研究主機，未對外提供資料服務。`finlab-research` 原始工作區依目錄設定維持 `publish:false`；私人研究用途的衍生訓練表註冊為 `tw-public-research-finlab-2014-v4`，僅傳輸合併特徵、來源收據與官方公司行為參照。這是依使用者對其用途的聲明操作，不是對所有 VIP 帳號或商業用途的授權判定；[FinLab 服務條款](https://studio.finlab.finance/terms)未明確保證任意跨機複製／再散布權。若用途變更、增加第三方節點或對外提供任何原始值，先停用這份私有 release 並向 FinLab 確認。公開面板只顯示統計，沒有表格原始值下載端點。

`scripts/build_finlab_research_overlay.py` 可把驗證雜湊的 FinLab 數值與事件欄位加入**另一份** `artifacts/research_features/tw_public_research_finlab_2014_v4.parquet`；`configs/markets/tw_public_preopen_finlab_research_2014_v4.yaml` 只給本機研究訓練。事件時間早於 09:00 可落同一交易日，其他日期／事件落下一交易日；季度 EPS 用官方申報期限代理。無逐股公告時點、歷史版本或可對應股票代碼的資料不硬接到正式台股表；所有原始 Parquet 原封保留。

2026-09-23 20:00 本機快照：timer 已啟用，下次排程 **2026-09-24 16:10**；當時少量試跑後已下載 **23/1,109** 個目錄鍵，尚有 **1,086** 鍵未下載或未驗證。後續背景掃描仍會變動，請以 `data_finlab/runs/latest.json` 與收據為準；它受 FinLab 與同帳號其他工作共用的每日額度限制。研究表已建成 **9,624,546 列、104 個 FinLab 欄位、16 個來源**，大小約 950 MiB；與底表列數相同。2014 年起非空值例：月營收 262,169、EPS 87,270、借券餘額 4,473,442、外資持股比率 5,795,109、重大訊息事件 439,034。這些是**稀疏觀測數**，不是每個交易日／每檔均完整。價格與 TAIEX 可與官方源重複，保留研究命名空間但不覆寫官方列；可轉債代碼、期貨大額交易人合約粒度、尚未完成語義映射的盤後定價欄位不注入此股票研究表。尚未執行實際模型訓練或證明績效。

## 私有遠端研究訓練版本與核驗

`scripts/stage_tw_public_finlab_research_release.py` 先要求全目錄最近 24 小時強制來源查詢且逐檔 SHA-256 通過，再驗證研究表、104 個 FinLab 數值欄、204 個台股公用數值欄、上游 all-observed 與正式台股特徵表 lineage，然後複製精確表與公司行為參照。這份 ABI 為 **636 個模型通道**：20 個股票基底，加上 308 個公用／FinLab 原始值及其 308 個可用性指示。這不是 636 個全史完整特徵，也不是 PIT 真值。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/stage_tw_public_finlab_research_release.py
stockagent-data publish-status tw-public-research-finlab-2014-v4
stockagent-data publish tw-public-research-finlab-2014-v4
run_fintech_python scripts/audit_finlab_research_cold.py
```

2026-09-23 本機冷庫已發布且逐物件驗證 release `tw-public-research-finlab-2014-v4-20260923T130343102331947Z-l0-penguin-e7b189c0665b9c7d`，8 個邏輯檔、約 997.7 MB 新傳輸物件。這只證明 **penguin 冷庫**；Syncthing 對端收齊、Vast 持久儲存、遠端 `READY`、與正式台股底表相同 SHA、GPU 訓練，都要個別核驗，不把其中任一個寫成已完成。

遠端以同一個 Git 版本，先為所需的 `tw-public` 精確正式底表和本研究 release 各執行一次 `stockagent-data use ... --verify`，將前者連到 `data_tw_public`、後者連到 `data_finlab_training`。**先檢查這兩個連結不存在或確為可替換的唯讀 materialization；不可覆蓋任何下載器正在寫的 live workspace。**研究 release 的收據含 `base_feature_sha256`；若遠端 `tw-public` 頭版較舊，`scripts/check_finlab_remote_training_ready.py` 會拒絕，不可忽略並直接啟動訓練。正式台股底表已補發為 `tw-public-20260923T131544986916179Z-l0-penguin-b530582ab4e2bcc1`，其特徵 SHA 與 FinLab 研究 release 的 `base_feature_sha256` 精確相同；本機 Syncthing 索引觀測對端收斂，但 index-only Vast 還須水合物件及建 `READY`，故目前不能宣稱遠端 READY。

```bash
# 在遠端且 Syncthing 物件收齊後；index-only 邊緣節點先依既有 fetch 流程水合
stockagent-data use tw-public --snapshot-id tw-public-20260923T131544986916179Z-l0-penguin-b530582ab4e2bcc1 --verify --link "${PWD}/data_tw_public"
stockagent-data use tw-public-research-finlab-2014-v4 --snapshot-id tw-public-research-finlab-2014-v4-20260923T130343102331947Z-l0-penguin-e7b189c0665b9c7d --verify --link "${PWD}/data_finlab_training"
source scripts/runtime_env.sh
run_fintech_python scripts/check_finlab_remote_training_ready.py
run_fintech_python train.py --config configs/deployments/tw_public_finlab_research_2014_v4_remote.yaml --check-data-only
```

獨立 [`/finlab/`](https://penguin72487.ddnsgeek.com/finlab/) 仿永豐 API 監控頁，以 SDK `get_data_status()` 每 5 分鐘低頻觀測帳號已用／上限／剩餘 MB，另列 1,109 鍵下載進度、逐鍵首末資料索引、研究表／冷庫／遠端三階段。官方文件指出每日額度於台北時間 08:00 重置；頁面重置日期是**規則推算**，不是供應商逐次回覆。官方未公布本帳號固定每秒請求數上限，因此顯示「官方未公布」，不虛構 10 req/s。下載器完成收據滯後時，帳號流量觀測可先增加；兩個時鐘與來源分開標示。

新版 `/finlab/` 沿用永豐面板的「所有管線 → 流量與費用 → 儲存容量 → 歷史明細 → 執行摘要 → 口徑」六段導覽、卡片、進度與篩選樣式，但 FinLab 沒有已驗證的即時訂閱、逐鍵帳號流量或固定 requests/s 上限，對應欄位保留未知而不套永豐數值。儲存容量只統計目前成功收據指向的原始 Parquet 和單一研究表，**不含舊版、SDK 快取或冷庫物件**；尚無連續容量快照時日均成長顯示未知。
