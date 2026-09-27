# API 與金融經濟資料清點（2026-09-27）

> 後續更正（同日 22 時）：以下為當時快照，不代表目前狀態。直接核對 OpenBB SQLite 與 Parquet 後，發現舊計畫仍有 EIA、IMF、OECD 的成功資料，原先只看 active-plan 摘要會漏算；已恢復歸屬並新增追新排程。Census／BEA／實價登錄數值下載器及 Frankfurter 早期官方歷史也已實作。詳見 [優先來源修復與實測](public_provider_priority_repairs_2026-09-27.md)。

## 結論

已核對 `.env`、`.env.example` 的設定名稱、既有註冊表、監控清冊及下載收據；沒有輸出金鑰、重新登入、呼叫帶 key 的 API、重啟服務或改排程。

- **42 組憑證來源：26 組已設定、1 組部分設定、15 組未設定。** `.env` 有 47 個設定名稱；範本有 52 個，差額為 4 個 FinLab 認證欄位及 1 個 tick 配額設定。FinLab 目前使用 SDK session，不必因 `.env` 欄位空白就判定未登入。沒有發現未登錄的 credential-name 候選或重複設定名稱。
- 依研究用途分類：**29 組直接金融／經濟**（行情與企業 13、總體產業 11、加密 5）、**11 組另類經濟風險**、**2 組識別／認證基礎設施**。這是用途分類，不是因果或預測能力證明。
- 現有 **1,772 筆監控登錄列**、37 個公共來源群組、158 筆 TAIFEX 目錄項，以及 1,829 筆 NOAA／Census／FIRMS 資料集／年份目錄。這些不同層級互相重疊，**不能相加成獨立 API、特徵或已取得資料數**。
- 1,772 列中有 1,468 列可列出至少一個來源索引／資料日期；其餘 304 列為未知或不適用，包含憑證入口、聚合、待接入等，**不是 304 個下載故障**。本次未逐筆重掃原始資料，也未證明任一 provider 全史無缺口。

監控快照：**2026-09-27 19:41:04 台北時間**。補充收據擷取：19:53:46；OpenBB 原始收據自身時間是 **16:12:27**，不能當成 19:53 重新驗證資料庫。各來源時間保留於 CSV。

## 可直接篩選的完整清冊

| 檔案 | 回答的問題 |
| --- | --- |
| [42 組 API 分類與狀態](../artifacts/data_quality/financial_api_inventory_2026-09-27/provider_financial_inventory.csv) | 每組環境變數名稱、憑證存在狀態、金融關聯、資料類型、排程、日期證據及限制 |
| [1,772 筆資料／端點清冊](../artifacts/data_quality/financial_api_inventory_2026-09-27/dataset_inventory_with_dates.csv) | 逐項 title、provider、首末日期、筆數、更新週期、下次排程、發布時間依據、配額、分工與受阻原因 |
| [FinLab 1,105 鍵收據](../artifacts/data_quality/financial_api_inventory_2026-09-27/finlab_receipt_inventory.csv) | 每個已存欄位的實際來源 index 起訖、寬表列數、欄數、抓取及檢查時間 |
| [FinLab 6 個缺一般收據的鍵](../artifacts/data_quality/financial_api_inventory_2026-09-27/finlab_missing_receipts.csv) | 資源受限、來源空值、盤中分區只部分取得，三者分開 |
| [FinMind 111 筆 lane／dataset 收據](../artifacts/data_quality/financial_api_inventory_2026-09-27/finmind_receipt_inventory.csv) | free、sponsor、complement 分工；回傳資料日期與完成查詢分區日期分開 |
| [OpenBB provider 成功／空結果收據](../artifacts/data_quality/financial_api_inventory_2026-09-27/openbb_provider_receipt_inventory.csv) | 成功任務、空結果與累計回傳列，不把「已處理」當成「有資料」 |
| [官方產品／年份及宣告期間](../artifacts/data_quality/financial_api_inventory_2026-09-27/discovered_public_datasets.csv) | NOAA 11、Census 1,807、FIRMS 11；只有目錄，不代表數值已下載 |
| [TAIFEX 158 項](../artifacts/data_quality/financial_api_inventory_2026-09-27/taifex_endpoints.csv)／[公共來源 37 組](../artifacts/data_quality/financial_api_inventory_2026-09-27/source_groups.csv) | 不需要放 API key 的來源也納入 |

日期讀法：`reported_first/last` 優先使用監控的實存統計，其次用下載器回傳資料日期；`reported_date_basis` 保留差別。`advertised_first/last` 是官方目錄宣告範圍。`configured_request_start` 只是查詢起點。三者不能互換。FinLab 的季度或月份 index 也不能直接當成精確發布時間。

## 1. 金融行情與企業：13 組

「已設定」僅代表憑證／session 存在。下表是本機收據現況，不是本次重新測試 API。

| Provider | 設定 | 可研究的資料 | 本機取得狀況 |
| --- | --- | --- | --- |
| 永豐 Shioaji | 已設定 | 台股、期貨、選擇權合約、K 棒、成交與委買賣 | 有股票分鐘歷史及衍生品資料；股票分鐘主表首末為 2020-03-02～2026-09-24，缺口不等於全補齊 |
| FinMind | 已設定 | 行情、籌碼、財報、營收、ETF、可轉債、期權、商品、外匯 | 有大量數值；仍有待回補、受阻及委派項目，不可只加總分母算完成率 |
| FinLab | SDK session | 台美股價量、財報、比率、籌碼、ETF、總體 | 清冊 1,111 鍵、一般資料收據 1,105 鍵；6 鍵狀況另列 |
| ToAlpha | 已設定 | 個股預估、分析師共識、券商評等 | 已有互動查詢工具；未找到可驗證的定期全歷史庫 |
| Finnhub | 已設定 | 證券名單、行情、基本面、企業事件 | **只有 31,116 筆美股證券名單目錄**，不是行情／財報歷史 |
| SEC identity | 已設定 | EDGAR、XBRL、申報、ETF 資料 | 有 OpenBB 申報及 ETF／發行商歷史；SEC 身分字串不是付費 key |
| FMP | 已設定 | 價量、企業基本面、財報、估值 | OpenBB 收據 235 成功任務、5,234 回傳列；仍受配額／方案限制 |
| Tiingo | 已設定 | 價量、公司行動等 | 427 成功任務、339,217 回傳列；不是 Tiingo 全產品完成 |
| Alpaca | 未設定 | 美股／ETF、加密行情 | 產品已登錄；沒有本機成功歷史證據 |
| Alpha Vantage | 未設定 | 股票、外匯、加密、基本面、經濟指標 | 未驗證目前帳號端點／資料 |
| Nasdaq Data Link | 未設定 | 金融、商品、經濟資料庫 | 未取得；免費與付費資料庫須分開 |
| Benzinga | 未設定 | 企業事件、新聞、行事曆 | 未見成功數值；維持使用者「先不抓新聞」的要求 |
| Intrinio | 未設定 | 行情與基本面授權產品 | **26 個 empty 任務、0 成功數值列**，不能算已取得 |

FinLab 官網目前展示約 1.8K 欄位，範圍包含台美股；本機 SDK 發現的是 1,111 鍵。兩者分母、版本或權限尚未對齊，不能直接宣稱漏了精確幾欄，也不能宣稱 1,111 就是官網全量。[FinLab 官方資料庫](https://finlab.finance/data)。FinMind 的技術、籌碼、基本面及衍生品分類可對照[官方清單](https://finmind.github.io/tutor/TaiwanMarket/DataList/)。

### FinLab 缺一般收據的 6 鍵，不是 6 鍵全部沒有資料

| 鍵 | 已觀察的原因 |
| --- | --- |
| `after_market_fixed_price:資料來源` | 來源標籤寬表超出單鍵記憶體預算，屬 metadata 類欄位 |
| `broker_transactions` | 整張券商表超出單鍵記憶體與配額預算，待有界分區 |
| `dividend_otc:權息` | SDK 回傳資料框，但沒有非空來源值 |
| `management_change_events:變更交易開始日` | SDK 回傳資料框，但沒有非空來源值 |
| `tw_minute:2330` | 盤中分區已有 2/84 個工作日；一般整表收據不存在，不等於零資料 |
| `tw_tick:2330` | 同上；84 是已列工作分區，不代表可取得全部歷史只有 84 日 |

## 2. 總體與產業經濟：11 組

| Provider | 設定 | 金融經濟資料 | 取得狀況／頻率判讀 |
| --- | --- | --- | --- |
| FRED | 已設定 | 利率、物價、就業、貨幣、匯率、宏觀與部分發布版 | 有成功數值；OpenBB 累計回傳 115,816,034 列。各系列有日／週／月／季頻率，不是每分鐘有新值 |
| BLS | 已設定 | CPI、PPI、就業、失業、工資與工時 | 有成功數值；累計回傳 62,241,851 列。多為月／季系列，須依發布事件 |
| EIA | 已設定 | 石油、天然氣、電力、庫存、產量與能源價格 | **此 OpenBB 收據未列成功數值**；不能將「無待辦」當成已下載。FRED 轉載不能當成 EIA 直接管線證據 |
| BEA | 已設定 | GDP、消費、所得、儲蓄、產業、區域及國際經濟 | 最近探測為 **credential_activation_required**，尚無成功目錄證據 |
| Census | 已設定 | 人口、所得、住宅、零售、製造、建築、貿易、就業 | **只保存 1,807 筆資料集／年份目錄**；各地區各變數數值尚未取得 |
| CFTC | 已設定 legacy token | COT、期貨交易人部位 | 有 OpenBB 歷史及 1986～1999 舊檔；週資料與靜態歷史包分開 |
| EconDB | 未設定 | 跨國宏觀與產業序列 | **已有 1,238,254 累計回傳列**；沒有目前 key 不代表未抓過 |
| USDA NASS | 未設定 | 農作產量、單產、面積、畜牧及價格 | 未取得本 API 歷史證據；頻率依調查／發布日 |
| ENTSO-E | 未設定 | 電力負載、發電、跨境流量與電價 | 未取得；產品粒度可為 15 分鐘／小時等，不是股票分鐘線 |
| UN Comtrade | 未設定 | 國別、貨品、夥伴進出口額與量 | 有登錄 OpenBB；未找到成功數值證據；月／年資料 |
| Trading Economics | 未設定 | 全球總體、經濟行事曆及市場資料 | 未找到成功數值證據；可用性依方案及端點 |

FRED/BLS/OpenBB 的列數是**成功任務回傳量**，沒有跨端點去重，不能合計為唯一觀測數或與主表列數相加。[FRED API](https://fred.stlouisfed.org/docs/api/fred/)、[BLS API](https://www.bls.gov/developers/)、[EIA Open Data](https://www.eia.gov/opendata/)、[BEA 主題](https://www.bea.gov/data)、[Census API 產品](https://www.census.gov/data/developers/data-sets.html)。

## 3. 加密 API：5 組

| Provider | 設定 | 資料 | 現況與增量策略 |
| --- | --- | --- | --- |
| Dune | 已設定 | 已註冊 SQL 的鏈上／協議統計 | 已存 2,747,699 列、2018-11-02～2026-08-31；最新批次 64 個到期分區中 1 個 credits 受阻、63 個 subscription 受阻；不是歷史檔消失 |
| CoinGecko | 已設定 | 資產名單、市場／全域市值快照 | 有名單與快照；目錄／市場每日、全域 15 分鐘；不是全交易所 K 棒歷史 |
| Etherscan | 已設定 | Ethereum gas 等鏈上狀態 | 專用來源 operational；狀態 1 分鐘檢查、供給日更按端點能力；不代表鏈上全史 |
| CoinGlass | 已設定 | OI、funding、清算、多空指標 | **not_entitled / Upgrade plan**；未見成功歷史數值 |
| CoinMarketCap | 已設定 | 資產 ID、市值、行情 | **fallback_only**；作識別備援，全市場重複封存停用，非下載故障 |

Binance、OKX、Bybit 的公開 K 棒不依賴這 5 組 key，另列於下方。現有交易所範圍與「先 1 分 K」要求不變；不因本次清點啟動新交易所、全 tick 或 L2。[Dune 官方 API](https://docs.dune.com/api-reference/overview/introduction)。

## 4. 另類經濟風險：11 組

關聯是研究假設：天氣可能連結能源／農產，交通可能連結物流／消費，政策可能連結產業事件；沒有宣稱一定提升模型績效。

| Provider | 設定 | 候選研究資料 | 本機狀況 |
| --- | --- | --- | --- |
| NOAA CDO | 已設定 | 氣溫、降水、乾旱、氣候 | **11 個產品目錄**，無全球歷史觀測完成證據 |
| CWA 氣象署 | 已設定 | 台灣測站、預報、颱風、地震 | 本次目錄快照列 876 筆測站觀測，時間 2026-09-27 18:00；不是全史 |
| MOENV 環境部 | 已設定 | 空品、污染與環境 | 84 筆 AQI 快照，時間 2026-09-27 18:00；歷史未回補 |
| NASA FIRMS | 已設定 | 衛星火點與熱異常 | **11 個可用期間目錄**；不是已下載火點，也不是衛星影像全量 |
| AirNow | 已設定 | 美國空品與預報 | 待區域／日期／全球容量計畫，未有歷史成功證據 |
| Congress.gov | 已設定 | 法案、立法與政策事件 | 已有 265,785 成功任務、537,693 累計回傳列；事件更新 |
| GitHub | 未設定 | 公開專案提交／release／開發活動 | 未建可驗證歷史庫；未設定 token 不代表公開 API 完全不可用 |
| openFDA | 未設定 | 藥品／醫材／食品通報與警示 | 未取得本機歷史證據；產業事件候選來源 |
| TDX | 未設定 | 交通、運量、路網、物流活動 | client id／secret 未設定，未取得歷史證據 |
| Copernicus CDS | 部分設定 | ERA5 等氣候再分析格點 | URL 與 key 不可混同，尚無全球觀測下載完成證據 |
| Copernicus Data Space | 未設定 | Sentinel 等影像與場景 | 未設定 client credentials；全球原始影像另需容量配額規劃 |

NOAA／Census／FIRMS 目錄通常本機每小時檢查、24 小時 TTL；CWA／MOENV 快照本機每小時更新。**這是本機策略，不是所有官方產品的發布間隔**。全球明細依先前決定仍先估容量與配額，見[容量文件](global_public_data_capacity_2026-09-27.md)。

## 5. 識別與認證基礎設施：2 組

| Provider | 設定 | 用途 | 已取得狀況 |
| --- | --- | --- | --- |
| API.Data.gov | 已設定 | 美國多機關共用 key 入口 | 本身沒有獨立觀測資料，不應算成一個已抓的特徵 |
| OpenFIGI | 未設定 | FIGI／ISIN／ticker 證券識別映射 | 未有映射歷史證據；不是市場價格 API |

## 6. 公共來源與重要資料庫首末日期

除了永豐、FRED 等明列認證來源，下表大多有不需 `.env` key 的公開取得路徑；不能只清點金鑰而遺漏它們。

| 來源／資料 | 本機可驗證首筆 → 末筆（各庫總範圍） | 筆數口徑 | 現有更新策略／缺口 |
| --- | --- | --- | --- |
| Shioaji 台股分鐘主表 | 2020-03-02 → 2026-09-24 | 313,047,232 實存列 | 交易日增量；來源缺口另有 mask，不是每檔每分鐘全滿。此列仍需永豐認證，只為日期比較列於此 |
| TAIFEX 期貨日表 | 1998-07-21 → 2026-09-24 | 2,005,925 主表列 | 交易日盤後；不是日內全史 |
| TAIFEX 選擇權日表 | 2001-12-24 → 2026-09-24 | 2,886,600 主表列 | 交易日盤後 |
| TAIFEX 公開部位／風險統計 | 2001-12-24 → 2026-09-24 | 6,175,936 主表列 | 歷史回補＋盤後，仍有 1 個必要子端點未到最新 |
| TAIFEX 近期成交 tick | 2026-06-24 → 2026-09-24 | 46,163,837 主表列 | 官方近期檔；不代表多年 tick 全史 |
| Yahoo 全球股票日線 | 1962-01-02 → 2026-09-25 | 47,579,544 實存列 | 本機每日 06:30；最近批次 12 個失敗／部分完成項 |
| Frankfurter／ECB 外匯 | 2000-01-03 → 2026-09-25 | 5,814,944 實存列 | 每日；官方可取範圍比本機既有版本更廣，見下一節 |
| Coin Metrics Community | 2009-01-03 → 2026-09-26 | 2,969,377 實存列 | 每日，不是每一資產都從 2009 有資料 |
| Binance 主表 | 2019-09-08 → 2026-09-26 | 728,921,996 實存列 | 1 分鐘增量；2 個必要子端點未到最新 |
| OKX 主表 | 2019-10-01 → 2026-09-27 | 455,950,576 實存列 | 1 分鐘增量；10 個必要子端點未到最新 |
| Bybit 主表 | 2018-11-14 → 2026-09-26 | 783,064,305 實存列 | 1 分鐘增量；1 個必要子端點未到最新 |
| SEC／ETF 發行商 | 2013-10-07 → 2026-09-26 | 38,516 多張主表列 | 申報事件＋日檔，不是每個 ETF 都有完整流量歷史 |
| FRED crypto 宏觀發布版 | 2005-06-27 → 2026-09-25 | 34,359 主表列 | 每日及發布事件；FRED API 使用既有 key，非所有路徑都匿名 |

其他無單一首末日的來源同樣列入完整 CSV：

- **TWSE、TPEx、MOPS、TDCC、央行、主計總處、財政部、SITCA／data.gov.tw**：已有價量、財報、營收、籌碼、ETF、利率／匯率／經濟資料；台股公開群組仍有 32 個必要子端點受阻，另外 27 項宏觀特徵已有數值但品質／發布證據待查，不能統稱沒抓。
- **DefiLlama、Alternative.me、mempool.space、Blockscout**：有 DeFi、恐懼貪婪、Bitcoin／Ethereum 上下文歷史或快照；28 項清冊有 4 個受阻，逐項以 CSV 為準。
- **Federal Reserve、U.S. Treasury／government_us**：OpenBB 成功回傳列分別 97,783、1,717,505；不與 FRED 轉載合計。
- **IMF、OECD、World Bank**：已登錄來源群組，本次沒有逐資料集成功歷史證據可據以宣稱完整；ECB 的既有匯率另由 Frankfurter 管線負責。
- **Stooq**：待授權用途確認。**GDELT／新聞**：尚未啟動全量，維持先不抓新聞。**Hyperliquid、Deribit、Coinbase、Kraken、Bitfinex**：依先前需求不新增交易所。**全市場 crypto tick／L2／liquidation**：維持暫緩，不當成 downloader 故障。
- **地政／實價登錄**：本次 42 組 API 清冊未見獨立已運行的全史下載項目；未證實已取得，不能用一般 data.gov.tw 已連線來代替證據。

上述首末日都是表內 union 範圍，不是每個 symbol 的共同完整期間。快照 capture time、每日觀測時間、季度所屬期及發布時間不能混用；列數不跨來源加總。

## 7. 官方可取得歷史與本機已存歷史分開看

| 來源／產品 | 可取得範圍證據 | 本機差別 |
| --- | --- | --- |
| NOAA GHCND | 已保存官方目錄宣告 1763-01-01～2026-09-24 | 只是產品整體範圍，不是每站都自 1763 齊全；本機只有目錄 |
| NASA MODIS_SP | 已保存 availability 宣告 2000-11-01～2026-06-30 | 未下載全球火點；NRT 與 standard 分開，不可重複當新事件 |
| NASA VIIRS_SNPP_SP | 已保存 availability 宣告 2012-01-20～2026-06-30 | 同上，NOAA20、NOAA21 各有不同起日 |
| Census | 每個 dataset/year 另列 1,807 筆；例如 ACS、經濟普查、月度貿易並非同一起日 | 有目錄但無逐地區變數值；vintage 不直接當 observation date |
| Frankfurter v2 | 官方說明多來源匯率最早可到 1948 | 目前主庫自 2000 起，且既有 ECB-only／v1 不能被全來源混合值靜默覆蓋；不是每個 pair 都可補到 1948 |
| ERA5 single levels | 官方產品為 1940 起逐時資料 | 本機尚未取得這份全球歷史 |
| FinLab | 官網整體展示 1960～2026，並非每個欄位的共同範圍 | 收盤價實存 2007-04-23～2026-09-24；月營收 index 2005-02-10～2026-09-10；EPS 為 2013-Q1～2026-Q2；逐鍵收據另外列出 |

來源：[NOAA CDO metadata 文件](https://www.ncei.noaa.gov/cdo-web/webservices/v2)、[NASA FIRMS 歷史](https://firms.modaps.eosdis.nasa.gov/content/active_fire/)、[Census 分產品期間](https://www.census.gov/data/developers/data-sets.html)、[Frankfurter v2](https://frankfurter.dev/)、[ERA5 官方產品](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels?tab=overview)、[FinLab](https://finlab.finance/data)。沒有可驗證端點期間的 provider 保留「未知／按資料集」，沒有填造假的共同最早年。

## 8. 本次發現的清冊品質問題與後續順序

1. **高影響／已證實：OpenBB 任務解決不等於取得數值。** 原收據 8,909,579 任務中，7,317,642 success、46,191 empty、1,545,250 unavailable、471 pending、25 repair。`resolved=8,909,083` 包括 unavailable；不能顯示成 99.99% 資料已取得。Intrinio 的 26 個 empty 是具體例子。
2. **高影響／已證實：有 key 但沒有可用數值。** BEA 待啟用、CoinGlass 方案阻擋、EIA 未見成功列；應分別處理認證、訂閱與下載器接線，不是一律重試。
3. **中影響／已證實：泛用憑證映射漏掉子來源。** Etherscan 在原 API CSV 的 linked count 是 0，但專用 crypto receipt 明確 operational。本報告已交叉列明；沒有擅自修網站或生產 registry。
4. **中影響／未完全對齊：目錄分母與實存／品質混合。** FinLab 官網與 SDK 範圍不同、FinMind 同 dataset 存在多個 lane、宏觀 quality-only 不等於缺數值。後续應以唯一事實的 owner／粒度／版本建立分母。
5. **中影響／限制明確：歷史資料與可訓練性尚未合併驗收。** 本次日期來自收據／footer 投影，不代表每檔無缺、單位一致、PIT 安全、跨機授權或冷庫同步完成。

建議補齊順序：先修已有 key 的 BEA 啟用與 EIA 實作／receipt 對照，補 Census 經濟指標數值，再核對 FinLab 六鍵與 Dune 限制；NOAA／FIRMS／ERA5 等全球明細依容量計畫進行。這是建議，**本次只完成清點，沒有額外啟動下載**。

## 重現方式

本報告由[只讀分析程式](../artifacts/data_quality/financial_api_inventory_2026-09-27/build_analysis.py)、[安全收據摘錄](../artifacts/data_quality/financial_api_inventory_2026-09-27/local_receipt_evidence.json)、[分類註解](../artifacts/data_quality/financial_api_inventory_2026-09-27/provider_annotations.json)及[核對 notebook](../artifacts/data_quality/financial_api_inventory_2026-09-27/inventory_checks.ipynb)支援。收據摘錄只有允許的資料統計，不包含憑證值。

四個 notebook code cells 已以同一 Python namespace 依序執行並保存結果，nbformat 格式驗證通過。主機沒有 ipykernel／kernelspec，未做 Jupyter kernel 或 GUI 顯示驗收；這項限制不影響已執行的清冊斷言，但不宣稱 notebook 互動介面已測試。

```bash
source scripts/runtime_env.sh
run_fintech_python artifacts/data_quality/financial_api_inventory_2026-09-27/build_analysis.py
```

預設重播凍結證據，不讀 `.env`、不連網。加 `--capture` 只更新本機允許清單中的收據摘錄；監控資料 CSV 仍保有自己的原始快照時間。這是一次性稽核，不是新增排程。
