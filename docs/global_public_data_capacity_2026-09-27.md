# 全球公開資料可以逐批補齊；全衛星影像不能當作一般 API 表格下載

## 結論與執行邊界

本次核對日期：2026-09-27。使用者已要求全球明細；本文件先估算容量、配額與歷史取得方式，**不是已下載完成清單，也不是全部機關資料已獲授權的聲明**。

- 本機 `df -h . /srv/stockagent-live` 當次觀測：兩路徑同在 `/dev/sdd`，可用 **436 GiB**，不是兩份獨立空間。現有訓練、行情與下載仍會使用它。
- 小型目錄、發布時程、全域匯率、總體統計、臺灣地政／環境表格可先分批做；NOAA 日資料優先官方 bulk，避免以百萬次 API 取代少量檔案。
- NOAA ISD 官方介紹的全史約 **600 GB 未壓縮**；全球高解析度衛星影像，單一假設產品的十年情境就有 **43.8–438 TB**。這兩者不能在剩餘磁碟上無界展開。
- 「目錄完整」「排程無待辦」「收到 HTTP 200」「有 API key」「取得最新快照」都不能代表全球歷史完整。完成分母必須是實際枚舉出的產品 × 版本 × 地區／測站 × 年份／分區。

本文只讀取環境範本變數名、憑證清冊安全欄位、小型監控 JSON、程式碼與官方網頁；沒有讀取或輸出 `.env` 金鑰，沒有驗證付費權限、呼叫帶 key 的資料 API、下載全球檔案或修改服務。下列壓縮容量均另外標示「官方數字／目錄數字／假設情境」，不把估算冒充實測。

## 1. 本機已有什麼，還不能宣稱什麼

本節是本次修改前的讀取快照；同時進行中的下載器改動不會自動使這份歷史觀測變成目前完成證據。

| 來源 | 安全設定／現有程式證據 | 當次本機覆蓋證據 | 關鍵缺口 |
| --- | --- | --- | --- |
| NOAA CDO、CWA、MOENV、NASA FIRMS | `.env.example` 有 `NOAA_CDO_TOKEN`、`CWA_API_KEY`、`MOENV_API_KEY`、`NASA_FIRMS_MAP_KEY`；公開狀態快照的 credential gate 為已設定 | 原憑證清冊仍標 `planned`／`planned_p0`；本次初始搜尋未找到對應資料下載器 | 設定存在不是認證成功，更不是全產品歷史；NOAA 原 registry 還把 NWS 說明頁當氣候歷史入口，兩種服務須分開 |
| Census、BEA、API.Data.gov | 清冊有 `CENSUS_API_KEY`、`BEA_API_KEY`、`API_DATA_GOV_KEY` 欄位 | 這裡只確認註冊欄位，未測 key 或全資料覆蓋 | Census data 與 metadata 的現行認證規則不同；共用 key 也不代表每個子機關全權限 |
| Frankfurter／ECB | `downloader/download_forex_frankfurter.py` | 監控 footer 統計：870 檔、5,814,944 列，2000-01-03 至 2026-09-25 | 預設起日為 2000-01-01，不能代表官方最早；每 pair 呼叫一次有重複工作；現程式是 v1，不是全 v2 providers |
| FRED | `downloader/download_fred_crypto_macro_vintages.py` 的 11 個預設系列；另有 OpenBB | 專用 crypto-macro 表 34,359 列，2005-06-27 至 2026-09-25 | 11 個系列不等於 FRED 全系列；observations 的日內發布鐘另有推定規則 |
| EIA／FRED／OECD（OpenBB） | `downloader/download_openbb_archive.py` | 對應列為「目前無待處理任務」，但該列 rows／first／last 為 null | 不能據此推定沒有缺歷史、沒有漏端點或全機關下載完成 |
| 內政部地政實價 | 初始 `downloader/scripts/configs` 關鍵字搜尋沒有找到現存實價 collector | 未證實本機有全史 | 需獨立 free bulk owner；不得把付費坐標欄位列為免費可補 |

本機證據入口：`configs/data_api_credentials.json`、`configs/free_public_data_sources.json`、`artifacts/live/data_monitor/public_status.json`。只使用其公開安全投影，不在報告附憑證值。

## 2. NOAA：全史用檔案，API 用發現與小量補缺

### GHCN Daily

官方 bulk 目錄本次列出 `ghcnd_all.tar.gz` **3,715,465,935 bytes（3.72 GB／3.46 GiB）**；這是目錄標示，不是已下載、解壓或校驗結果。`by_year/` 確實列有 **1763.csv.gz**；這只能證明有該年度檔，不能說每個國家／測站／欄位都從 1763 完整。逐站、逐要素期間應取 station inventory，再由實際資料驗證。官方產品介紹的歷史敘述與檔案目錄不完全一致，不能只摘一個年份作全域起日。[官方檔案目錄](https://www.ncei.noaa.gov/pub/data/ghcn/daily/)、[年度目錄](https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/)

首次選 `all.tar.gz` 或按年 CSV **一種 canonical 原始布局**；不要再下載同內容的按站、按年、全量三套。全量與 CSV 格式的列粒度不同，容量不可互推。README 另列 `superghcnd_full`／`superghcnd_diff`，可在驗證差分語義後增量；日更、週末重建及遲到品質更正都要保存版本，不能永遠只 append 最新日。[下載與欄位 README](https://www.ncei.noaa.gov/pub/data/ghcn/daily/readme.txt)、[官方更新與品質說明](https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily)

### ISD／Global Hourly

官方產品說明為 **1901 起**、超過 35,000 個歷史測站、目前超過 14,000 活躍測站日更，總量約 **600 GB 未壓縮且持續增長**。這是官方概略量，不是 2026-09-27 全目錄加總。官方明確推薦大量需求走 HTTPS WAF；以年份／測站檔與 `isd-history` 的起訖來枚舉，不能用現存測站數乘 126 年假裝每站連續存在。ISD-Lite 是縮減欄位與頻率的衍生資料，不能替代使用者要求的完整 ISD。[ISD 官方產品說明](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database)、[年份檔案入口](https://www.ncei.noaa.gov/pub/data/noaa/)

### 配額與單位

CDO 需 token，限制 **5 requests/s、10,000 requests/day、每頁最多 1,000 列**；此限制是 CDO API，不可套成所有 NCEI HTTPS bulk 的固定配額。bulk 沒查到可依賴的數值吞吐承諾，需少量並行、重用連線、遵守 429／Retry-After，不能以「無公布數字」當作無限制。[CDO v2 文件](https://www.ncei.noaa.gov/cdo-web/webservices/v2)

GHCN 溫度與降雨常為整數縮放值，例如 TMAX/TMIN 是 0.1 °C、PRCP 是 0.1 mm；`-9999` 是缺值，不是真實負雨量。保留 raw integer、scale、unit、MFLAG/QFLAG/SFLAG、站號與有效期間。ISD 也要依欄位字典解讀縮放與旗標；禁止一律把負數當異常或移除品質旗標。[GHCN 欄位規格](https://www.ncei.noaa.gov/pub/data/ghcn/daily/readme.txt)

## 3. NASA：熱點表和 MODIS／VIIRS 影像是不同容量級別

FIRMS 官方熱點覆蓋：MODIS Terra **2000-11**、Aqua **2002-07**；VIIRS S-NPP **2012-01-20**、NOAA-20 **2018-04-01**、NOAA-21 **2024-01-17**。這是熱點產品，不是所有影像的起日。歷史 archive 的 NRT 後續會由標準科學處理版本替換，通常延遲 2–3 個月、可能到 5 個月，應保存版本及其捕獲時間。[FIRMS 產品與歷史說明](https://firms.modaps.eosdis.nasa.gov/content/active_fire/)

目前 Area API `DAY_RANGE` 是 **1–5 天**，可用 `world`；不要套用舊文章的 10 天。上限 **5,000 transactions／10 分鐘**不是 5,000 HTTP requests。官方範例指出全球 VIIRS 一天約 **30,000–100,000+ 筆**、其中一次查詢計入 36 transactions。排程應讀 quota 回應／狀態並留近期更新額度，不可只做 req/s limiter。[Area API](https://firms2.modaps.eosdis.nasa.gov/api/area/)、[官方 API 範例](https://firms.modaps.eosdis.nasa.gov/content/academy/data_api/firms_api_use.html)

先查 `data_availability` 的每個 source 實際起訖；歷史可用 archive／標準產品檔，近期增量用 Area API。Archive 入口需要 Earthdata 或 email 登入；有 `NASA_FIRMS_MAP_KEY` 不代表已有 Earthdata Download token。熱點也不是已確認火災事件數：要保留 sensor、acquisition 時鐘、處理版本、confidence、FRP 單位、座標和 day/night，不能將不同衛星／重處理版全部當獨立新火災。[可用性入口](https://firms.modaps.eosdis.nasa.gov/api/data_availability/)、[Archive 登入與下載](https://firms.modaps.eosdis.nasa.gov/download/)

正式教學中 `/api/data_availability/csv/{key}/all` 的 CSV 欄位為 **`data_id,min_date,max_date`**，不是 `source`；min/max 是 GMT 資料可用期間，不是這次抓取的熱點起訖。教學中一次 availability request 消耗 5 transactions，亦不可算成固定 1。[availability schema 與計量範例](https://firms.modaps.eosdis.nasa.gov/content/academy/data_api/firms_api_use.html)

MODIS／VIIRS L1/L2/L3 影像另走 LAADS／Earthdata，以產品、collection version、granule 清冊為分母。NASA 指定資料產品免費，但自有雲端儲存與計算費用自付；下載認證和 FIRMS MAP_KEY 不共用。原始 HDF／NetCDF 保留科學 metadata，不能只存展示 PNG 或把全部像素硬轉成長表 Parquet。[LAADS 下載與授權](https://ladsweb.modaps.eosdis.nasa.gov/learn/)、[雲端與費用邊界](https://ladsweb.modaps.eosdis.nasa.gov/cloud/)

## 4. CWA 與 MOENV：不能拿最近一期冒稱歷史全部

| 來源 | 起日可證實的範圍 | 增量頻率與 bulk／歷史入口 | 認證、配額及限制 |
| --- | --- | --- | --- |
| CWA CODiS 站點觀測 | 官方地面觀測介紹說 **1990 以來**；臺北站 metadata 的資料起日為 **1896-08-11**，但不是所有觀測欄位可透過免費 API 回補至該日的證明 | CODiS 站點、格點及歷史典藏分開枚舉；不同資料產品各有更新週期，不能把所有產品都排每分钟 | 開放平台 Authorization key 與 CODiS 的可下載／申購範圍不能混同；本次未找到可驗證的全平台數值 rate ceiling，標 unknown，不填假的官方 10 req/s |
| CWA 開放 C 系列 | `C-B0024-002` 是**一年**觀測；`C-B0025-002`／`C-B0026-002` 是**過去九年**；屬滾動窗 | 每個 dataset 的 metadata/updateFrequency 決定排程；舊年另查 CODiS，而不是重複抓 rolling 最新快照 | 需要驗證 schema、測站遷移、缺值與自動／人工觀測變更 |
| MOENV AQX_P_488 | 官方頁有月度 archive 清單，但本次沒有枚舉所有月；**最早月份未知**，不能從頁上「119 項」逆推且冒稱已驗證 | **每小時**更新；`year_month=YYYY_MM` 選歷史月份，不傳只取最新；優先頁面的月打包檔 | 需 `MOENV_API_KEY`；`offset/limit` 分頁；本次未證實 v2 通用最大 page size 或固定日／秒配額，不能拿舊 v1 的 1,000 列或範例值當全平台契約 |
| MOENV AQX_P_19 及其他測項 | 個別 dataset 起日由歷史下載月清單／資料字典確定；日平均和小時值不可混加 | AQX_P_19 標示 **每日**；其他監測、品保年版、裁處、污染源資料各自排程 | 官網 AQX_P_19 標題是日平均、描述卻提每小時；需以欄位與實際粒度檢核，不盲信展示文案 |

來源：[CWA 地面觀測](https://climate.cwa.gov.tw/GroundObservation)、[站點起訖](https://hdps.cwa.gov.tw/static/state.html)、[CWA 開放資料列表](https://www.cwa.gov.tw/Data/data_catalog/obs_element_item_table.pdf)、[MOENV 歷史 API 規格](https://data.moenv.gov.tw/paradigm)、[AQI 歷史資料](https://data.moenv.gov.tw/dataset/detail/aqx_p_488)、[日平均資料](https://data.moenv.gov.tw/dataset/detail/AQX_P_19)。CWA 列表 PDF 有舊版日期，實作須以當前 dataset metadata 再驗證，不能固定抄舊代碼後不再更新。

MOENV 必須保留 `DataCreationDate`（發布時間）以及觀測時間，SO2/O3/NO2 的 ppb、CO 的 ppm、PM 的 μg/m³不能一律當相同量綱。AQI 是指數，不是濃度。兩機關每個資料集的授權名稱、URL、版本都要保存；key 存在不會替代資料授權與申購規則。

首次小型 collector 的 schema 驗收：

- **CWA `O-A0001-001`**：官方產品說明每小時更新一次；Observation v1.05 明列 `Station/ObsTime/DateTime` 是「觀測時間」，格式含 `+08:00`。REST 外層 `records` 本次未帶 key 實測，需在 smoke 檢核；不能把內層 ISO 時間重新標為精確發布時間。[產品頻率](https://www.cwa.gov.tw/Data/data_catalog/2-1-2.pdf)、[官方欄位標準 p3](https://opendata.cwa.gov.tw/opendatadoc/Observation/O-A0002-001.pdf)
- **MOENV `AQX_P_432`**：最新 AQI 快照每小時更新，欄位是 `publishtime`（資料發布時間）；歷史 `AQX_P_488` 的 `datacreationdate` 不可混用。2026-01-14 官方公告已調整 API 回傳格式，`total/field/limit` 等狀態移到 `/api/v2/{dataset}/status`。解析應兼容頂層陣列及舊 `records` 包裝，實測確定 root shape；不能將缺 `records` 一律判為無資料。[AQX_P_432 metadata](https://data.moenv.gov.tw/dataset/detail/AQX_P_432)、[2026 格式異動公告](https://data.moenv.gov.tw/announcement?p=1&type=%E8%B3%87%E6%96%99%E5%85%AC%E5%91%8A)

一小時 TTL 是上述兩個快照的有界初版排程，不是所有 CWA/MOENV 產品的共同更新頻率，亦不會自動補回過去歷史。

## 5. Census／BEA：先下載完整表，不逐地區逐欄位詢問

### Census

官方 **2026 年 6 月**公告：所有 **data queries 需要 API key**，metadata queries 可不帶 key。舊 2021 指南的「無 key 每 IP 每日 500 次」不可當作目前匿名可用承諾。一般 query 文件仍列最多 50 variables；大表優先 bulk，不用 50 欄切片把每個地區反覆取回。已配置 key 的額外 rate ceiling 本次未確認，需依目前帳戶／回應而非宣稱無限量。[現行 key 政策](https://content.govdelivery.com/accounts/USCENSUS/bulletins/41aaf23)、[一般 query 限制文件](https://www.census.gov/data/developers/guidance/api-user-guide/query-components.html)

ACS 官方 FTP：summary files **2005 起**、PUMS **1996 起**、variance replicate **2014 起**；不是每個年份都具有今天相同地理粒度。ACS 5-year 的年份指估計視窗，不是五次獨立年度觀測。2021 起主用 table-based summary，2005–2017 是 sequence-based，2018–2020 另有 prototype。Discovery catalog 覆蓋許多人口、經濟、貿易與地理資料，不等於拿到其中全部值。[ACS bulk 入口與期間](https://www.census.gov/programs-surveys/acs/data/data-via-ftp.html)、[格式沿革](https://www.census.gov/programs-surveys/acs/data/summary-file/getting-started.html)、[全 API 清單](https://www.census.gov/data/developers/data-sets.html)

Census 不是全世界每個人的明細；免費公開範圍是其發布的統計與匿名公開微觀樣本，不包含機密個人資料。保留 geography vintage、estimate／margin-of-error、suppression、universe、dollars／persons／percent、名目／實質口徑。

### BEA

官方 GDP 例：年度 **1929 起**、季度 **1947 起**；不可外推為所有 Regional/MNE/IIP/InputOutput 表的共同起日。以 `GetDatasetList` → parameter/parameter-values → `GetData` 枚舉實際維度；有官方 ZIP/CSV 的全集優先 bulk。GDP 頁另提供 vintage history，當期下載的修訂值不能標成當年初值。[GDP 與 vintage 入口](https://www.bea.gov/data/gdp/gross-domestic-product)、[BEA bulk 與 API](https://www.bea.gov/open-data)

2026-04-20 API guide 的三個 gate 是 **100 requests/min、100 MB/min、30 errors/min**；任一超限會暫停，目前說明約一分鐘但可動態調整，遵循 **429 的 Retry-After**。不要沿用舊文「一定鎖一小時」。`UserID` 需有效註冊；日／月／季／年的單位縮放、季調與年率需原樣保存。免費已發布統計不表示能取得機密底層企業資料。[BEA 最新 API guide](https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf)

## 6. 內政部地政實價：免費 bulk 可做，但少了付費坐標且舊包不回改

實價登錄制度 **2012-08 起**，查詢網 2012-10-16 上線；每月 **1、11、21 日**發布。以官方免費當期／歷史季度封裝為下載單位，不逐縣市逐案件查詢。**免費資料沒有付費版的交易案例坐標**，且已封裝的免費包不會隨異動案件重寫；不能將舊 ZIP 與動態查詢頁視為完全相同版本。[官方 QA](https://lvr.land.moi.gov.tw/R11/jsp/qa.jsp)、[供應系統說明](https://plvr.land.moi.gov.tw/Index)、[當期／歷史下載入口](https://plvr.land.moi.gov.tw/DownloadOpenData)

下載 manifest、schema-main、schema-build、schema-land、schema-park 與分表，按原案件識別與子項序號關聯；買賣／租賃／預售屋分開，重複跨期不是重複交易。交易年月日、申報／公布期與擷取時間不可混用；總價元、單價元/平方公尺、坪數換算需標記，車位價缺值不是零。地政、地籍圖資、建物交易是不同產品；本次沒有證實全部地籍圖層都有相同免費全史權限。[官方資料集及 schema 說明](https://data.gov.tw/en/datasets/25119)

## 7. 容量與 requests 的可重算情境

單位：GB/TB 為十進位，GiB 為 2³⁰ bytes。`N` 是去除重試／同版重複後的邏輯列或 cell；**以下除明列官方 bytes 外，都是規劃假設，尚未做全史抽樣壓縮測試**。不將各列盲目相加為精確「全球總容量」。

| 範圍 | 維度／假設公式 | 正規化壓縮儲存情境 | HTTP／配額情境 |
| --- | --- | --- | --- |
| GHCN 全域日要素 | 假設 3–6B 有效 `station×date×element`，每列 12–28 bytes；不是已量得列數 | **36–168 GB**；另保留原始包與有界暫存。目錄 all tar 3.72 GB 不足以證明轉換後大小 | all tar 1 次資料 GET＋metadata；按年 1763–2026 最多 264 年檔（以清單為準）。若改 CDO 逐頁，3–6M requests 在每日 10k 理想下也需 **300–600 日**，應避免 |
| ISD 全域全史 | 官方約 600 GB 未壓縮；假設 gzip 為其 15–40%、Parquet 為 10–30% | raw gzip **90–240 GB**＋Parquet **60–180 GB**＝**150–420 GB**，仍是壓縮比假設，未含新增版本／暫存 | 以實際 station-year 檔清單計數；不可用 35k×126 當完整有效分母。別把 600 GB 全展開到 RAM／磁碟 |
| FIRMS 全球熱點 | 示範 **3 感測器×10 年×365 日×30k–100k 列/日**＝328.5M–1.095B；這不是實際各衛星全生命週期總量 | 假設 Parquet 30–80 bytes/列＝**9.9–87.6 GB**；CSV 100–200 bytes/列＝**32.9–219 GB**；旺季可更高 | 若 API 5 日窗全程可用，約 **2,190 HTTP**＋metadata／重試；transactions 必須另計。長史優先 archive，不假設所有 SP/NRT 可查窗相同 |
| 全球單一 VIIRS 6 分鐘影像產品，10 年 | 官方示例 240 granules/day；假設每檔 50–500 MB，`240×365×10`＝876k 檔 | **43.8–438 TB** 原產品；這不是 LAADS 真實全站總量。再乘產品、衛星、版本，量級更大 | 約876k 檔案 GET＋metadata；Earthdata/LAADS 認證、頻寬、存儲限制，不受 FIRMS MAP_KEY quota 管理 |
| CWA 小時站點，36 年 | 假設平均100–1,000站×36×365.25×24＝31.6M–315.6M 寬列，40–160 bytes/列 | **1.3–50.5 GB**；若可取得全期間10分鐘資料，維度再×6，但本次**未證明**該全史存在 | 依官方支持的 station-period/bulk分區；不是 1000×36 年都可用。現況10分鐘快照不能反推36年歷史 |
| MOENV 小時站點 | 假設平均100站×10–40年×365.25×24＝8.8M–35.1M 寬列；80–240 bytes/列 | **0.7–8.4 GB**；所有環境資料集／污染源事件另計，這不是 MOENV 全站上限 | 月 bulk 約120–480檔／dataset；若每頁1000列，約8.8k–35.1krequests。1000只是情境值，不是假定v2官方上限 |
| Census 高細度多表 | 假設250k地理單元×1k–30k欄位×20年＝**5–150B cells**；2–8bytes/cell | **10–1,200 GB**＋metadata／公開微觀檔。這是 cell，不是「150B人」或寬表列數 | 假設50欄/query、52地域分區、20年：20.8k–624kqueries；bulk可大幅降低。表/群組API或ZIP入口能否再減需逐dataset契約核實 |
| BEA 多維統計 | 示範200表×1000地區×50期至1000表×3000地區×300期＝10M–900M cells；16–64bytes/cell | **0.16–57.6 GB**，非已測全站大小；不適用沒有地區維度的表格 | 應按表＋允許ALL的維度取最大安全範圍；既要限制100req/min，也要限制100MB/min。遇回應過大才有證據地切分 |
| 臺灣實價交易 | 假設每年0.3–1.5M主案件×14年＝4.2M–21M；100–500bytes/主列 | 主表 **0.42–10.5 GB**；子表／版本若×2–4約 **0.84–42 GB**；不是官方案件數或實測壓縮比 | 若官方列出2012Q3–2026Q2共56季度且每季全臺一包，約56 ZIP GET，非22縣市×3種類逐案呼叫；未枚舉不得直接宣稱56包齊全 |

影像 granule 數的官方例子來自 [LAADS 檔案布局指南](https://ladsweb.modaps.eosdis.nasa.gov/learn/how-to-navigate-the-laads-online-archive-to-download-data/)。表內 50–500 MB 是容量敏感度假設，非該示例檔案的實測大小。熱點列數依 [FIRMS 官方範例](https://firms.modaps.eosdis.nasa.gov/content/academy/data_api/firms_api_use.html)，不能把所有感測器每年的活動密度當相等。

配額／時間的下界只在服務正常且沒有其他共用用量時成立：

```text
Q_api = Σ ceil(每個合法分區的有效列數 / 該端點可用頁容量)
Q_bulk = 官方非重複資料檔數 + 必要 metadata/校驗 requests
B_peak = 已保留 raw + 已產出 canonical + 最大在處理分區的暫存 + runtime保留
T >= max(下載bytes/實測吞吐, Q_api/有效配額速率, 規範化bytes/實測處理速率)
```

CDO 的 daily、BEA 的 bytes/min、FIRMS 的 transactions 都要用其原計費單位。`T` 還需加 quota reset 等待、排程與重試；不能用一次小查詢的耗時乘所有任務後顯示精確日期。

## 8. 在剩餘 436 GiB 下的分批方式

本節是建議資源預算，**尚未修改服務或磁碟限制**：先為現有系統保留 100 GiB，另留 20 GiB轉換／重試緩衝，則本輪新資料最大約316 GiB（約339 GB）。已存在檔、同時下載、cache、原子替換的雙份空間都會侵蝕它。只看壓縮後的「最終Parquet」會低估峰值。

1. **先列全目錄但不假裝全數據**：保存dataset/產品/版本、可下載資源、官方起訖與頻率、認證範圍、byte size（未知保留null）。給每個計畫一個可稽核的scope hash，枚舉中分母可變動。
2. **小表＋全域日資料先行**：MOI免費包、MOENV/CWA可公開表格、BEA、已知宏觀／外匯；GHCN選一種raw布局並逐分區解碼。每個來源先用1–3個有代表性分區量 `rows、wire bytes、raw bytes、Parquet bytes、CPU seconds、peak RSS`，再更新全史估算。
3. **全球熱點分感測器／年回補**：近期NRT留流量，標準版逐步替換，raw保留可復原證據。全球 all-sensor熱點歷史必須依實際生命期與archive清冊重算，不能把表中3×10年示例當完成分母。
4. **ISD／Census細地理全史分波**：可以規劃全部，不代表一次全部塞進本機。每波完成後走既有 catalog/receipt/audit/packed release，只有符合既有資料保存與冷庫合約才可處理本機來源；此文件不授權刪除原始資料。
5. **全球影像先做產品與granule清冊**：完整全球影像屬數十TB以上量級，436GiB不夠。將實際檔案尺寸彙總、免費下載權限與儲存目的地確認後才開大規模下載；不是以 silently skip 偽稱完成。

壓縮使用 lossless、按自然分區排序與字典編碼；不為了降低 bytes 改變 float 精度／單位，不把缺值填0。數值與units/quality flags可分表，但不能為了5NF/6NF製造每次訓練的大量join與小檔。原始影像保留HDF/NetCDF等原有壓縮；對已壓縮payload再套gzip不保證有效。

## 9. 可直接複用的既有下載邊界，以及最小修正

| 現有檔案／函式 | 可複用部分 | 有界修正／注意 |
| --- | --- | --- |
| `downloader/download_free_public_context.py`：`_capture_dataset`、`_append_observations`、`_merge_manifest_results` | 有界抓取、原始證據、觀測值與manifest模式 | 新機關不能只copy整個框架；應接共同http transport／rate-limit／receipt。catalog與data值分開計數 |
| `downloader/download_openbb_archive.py`：`_plan_endpoint`、`build_initial_plan`、`populate_initial_plan`、request checkpoints | 現有端點排程與可恢復工作 | EIA/FRED已存在owner，不另起第二套重複collector；確認OpenBB已支持的route與需新補的raw provider route |
| `downloader/download_forex_frankfurter.py`：`_load_supported_currencies`、`_download_pair`、`_normalize_rate_rows` | 已存在FX日表與增量合併 | 同一provider的共同base一次取多quote；補官方可證實的早年，不因latest已到今天就略過左側缺口；v2預設blend不能默默覆蓋既有ECB-only資料 |
| `downloader/download_fred_crypto_macro_vintages.py`：`_realtime_windows`、`_fetch_window`、`parse_initial_release_rows` | 原始發布版觀測與realtime-window receipts | 11系列只是已配置scope；擴系列前先分類同源重複、官方起日、vintage可用性及第三方授權 |

Frankfurter v2 官方目前提供多個央行來源、最早覆蓋可到1948，支援按provider、長區間和NDJSON；無日/月固定quota不代表無anti-abuse限速。既有collector預設2000起，是明確可查的左側缺口候選，但1948不是ECB或每一pair共同起日。應使用provider-specific基準匯率作canonical，其他貨幣交叉匯率按同一時點推導，保留raw與derived之分。[Frankfurter 官方文件](https://frankfurter.dev/)

EIA API通常JSON最多5000列，必須用`response.total`及offset驗證；只讀`/v2/`樹是metadata，不是資料。官方說明大致建議持續速率**低於約9000/hour**、burst**低於5/s**，仍可能因route複雜度更慢；不能把9000寫成保證額度。API更新與發布事件有關，WPSR/WNGSR上API可在公布後兩小時內；需要發布瞬間資料時走官方release server，不能將bulk更新時間代替真正發布時鐘。[EIA FAQ](https://www.eia.gov/opendata/faqs.php)

## 10. 全量驗收不可省的證據

- 目錄快照與download plan的scope固定並留版本；每個來源顯示`catalogued/pending/downloaded/validated/unavailable`，解析成功和原檔保存分開。
- 每分區raw hash、bytes、provider request範圍、observed_at；每canonical表記rows、actual first/last、units、QC與版本。`requested_start/end`不可充當實際數據起訖。
- 針對有預期coverage的測站／日期／測項驗缺；已撤站、尚未發明的衛星、未公布的普查年份不能列下載失敗。
- 未公布rate ceiling、license、歷史可用起日保持unknown；統計值空白／suppressed／no-data分開，不填假數值製造完整度。
- `observation_time`、`published_at`、`first_seen_at`、`revision_time`分開；推定發布日可以另存`estimated`與依據，不能回填成官方精確時間。
- 保存機關metadata是catalog完成；保存近期API回應是snapshot完成；保存全有效分區且通過檢核才是所宣告scope的歷史完成。以上都不自動等於PIT可訓練或跨機同步完成。

未執行：全球payload下載、壓縮sample benchmark、對所有歷史分區枚舉、金鑰實測、跨主機容量／冷庫可用性驗證。這些未完成項不可由本文件的情境數字取代。
