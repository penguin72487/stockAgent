# 本機 data_* 資料集導覽

<!-- stockagent:data-readme:v1 -->

目前逐一說明專案根目錄的 **33 個 `data_*` 資料目錄**，含符號連結；每個目錄都有自己的 README。
這是資料用途與讀取入口索引，不是下載進度、完整歷史宣告，也不代表所有資料已注入訓練。

## 先從研究問題選入口

- 台股日資料與財務：先看台股公開資料、FinLab、FinMind；分鐘與微結構另選對應資料集。
- 期貨／選擇權：分清官方日表、逐合約行情、逐筆、保證金／限額公告及衍生研究視圖。
- 新聞：訓練通常從 data_news 的固定邏輯版本開始；後續版本共用不變分區，data_news_fulltext 是持續寫入的來源庫。
- 加密：Binance、OKX、Bybit 各自獨立；宏觀／鏈上背景不是交易所原生價格。
- 全球總體／外匯：先辨識 provider、經濟序列、單位、期間與修訂版本，再選長表或相容視圖。

## 台股與台灣衍生品

| 資料目錄／個別說明 | 內容 | 角色 |
| --- | --- | --- |
| [data_tw_public](../data_tw_public/README.md) | 台灣股票與總體公開資料 | 官方來源、逐股日資料與研究特徵並存 |
| [data_tw_minute](../data_tw_minute/README.md) | 台股一分鐘行情與分鐘研究資料 | 分鐘原始行情、缺口證據與衍生訓練表 |
| [data_tw_microstructure](../data_tw_microstructure/README.md) | 台股逐筆成交、五檔委託簿與盤中微結構 | 前瞻串流原件與重建的研究快照 |
| [data_tw_shioaji_history](../data_tw_shioaji_history/README.md) | 永豐期貨、選擇權與指數歷史行情原件 | 逐合約查詢結果、可查範圍與收據 |
| [data_taifex_public_history](../data_taifex_public_history/README.md) | 期交所籌碼、風險指標與歷史規則公告 | 官方原件、正規化統計與規則文件 |
| [data_tw_futures](../data_tw_futures/README.md) | 台灣期貨商品、結算、連續行情與組合研究資料 | 商品／合約目錄、來源補充與多版本研究表 |
| [data_tw_index_futures](../data_tw_index_futures/README.md) | 期交所全期貨日表、指數近月視圖與規則準備來源 | 官方日資料主表與可重建的研究視圖 |
| [data_tw_index_options_daily](../data_tw_index_options_daily/README.md) | 台指選日行情、月週選全鏈與 ATM 配對 | 官方日行情正規化與選擇權槽位視圖 |
| [data_tw_index_derivatives_ticks](../data_tw_index_derivatives_ticks/README.md) | 台指期與台指選逐筆成交及事件研究表 | 官方逐筆原件、TX／TXO 分區與串流補充 |
| [data_finlab](../data_finlab/README.md) | FinLab 原始表、分鐘／逐筆與研究正規化 | 授權來源的不可變表版本與可追溯研究資料 |
| [data_finmind](../data_finmind/README.md) | FinMind 台股、衍生品、總體與新聞資料 | 分資料集的來源分區、配額與增量／修正證據 |
| [data_tej](../data_tej/README.md) | TEJ Smart Wizard 與試用 API 歷史資料 | 授權表格原值、查詢範圍與雙路徑取得證據 |

## 新聞

| 資料目錄／個別說明 | 內容 | 角色 |
| --- | --- | --- |
| [data_news](../data_news/README.md) | 新聞標題與全文研究資料集 | 已驗證固定版本的文章、去重文字與來源關聯 |
| [data_news_fulltext](../data_news_fulltext/README.md) | 新聞下載來源庫、原始正文與抓取證據 | 持續變動的來源權威庫，不是固定訓練切分 |

## 加密貨幣

| 資料目錄／個別說明 | 內容 | 角色 |
| --- | --- | --- |
| [data_binance](../data_binance/README.md) | Binance 永續合約一分鐘行情與交易特徵 | 單交易所分鐘主資料、日視圖與相容舊表 |
| [data_okx](../data_okx/README.md) | OKX 永續合約一分鐘行情與交易特徵 | OKX 單交易所行情、原生統計與日視圖 |
| [data_bybit](../data_bybit/README.md) | Bybit 合約分鐘、資金費率與單交易所日特徵 | 原生行情、資金費率及明確區分的研究表 |
| [data_binance_archive](../data_binance_archive/README.md) | Binance 官方公開歷史封存與合約生命週期 | 批次封存分區、下載計畫與物件接受狀態 |
| [data_coinmetrics_community](../data_coinmetrics_community/README.md) | Coin Metrics Community 資產指標與觀測版本 | 公開資產指標、可用欄位目錄與本機版本 |
| [data_crypto_reference](../data_crypto_reference/README.md) | 加密資產身分、供應商目錄與市場快照 | 跨來源參考快照，不是可回溯的全市場行情 |
| [data_crypto_historical_public](../data_crypto_historical_public/README.md) | 加密資產 CFTC 部位與 Wikimedia 關注度歷史 | 公開歷史背景與明確標示的研究可用時間 |
| [data_dune_crypto](../data_dune_crypto/README.md) | Dune 鏈上查詢歷史與分區執行結果 | 經查詢定義綁定的鏈上聚合，不是全鏈原始事件 |
| [data_crypto_etf](../data_crypto_etf/README.md) | 加密 ETF 發行商指標、持倉及 SEC 申報 | 基金歷史指標、持倉快照與法定申報事實 |

## 全球金融、總體與公共資料

| 資料目錄／個別說明 | 內容 | 角色 |
| --- | --- | --- |
| [data_fred_crypto_macro](../data_fred_crypto_macro/README.md) | FRED 初次發布版本的金融與流動性總體指標 | 發布日期版本與明確的日界可用政策 |
| [data_cftc_legacy](../data_cftc_legacy/README.md) | CFTC 2000 年前 Legacy 交易人部位歷史 | 官方壓縮年史補充與分報告類型視圖 |
| [data_forex_frankfurter](../data_forex_frankfurter/README.md) | Frankfurter 官方參考匯率與來源分離的長期歷史 | 日參考匯率，相容 OHLC 外觀但非盤中成交行情 |
| [data_openBB](../data_openBB/README.md) | OpenBB 多供應商金融經濟資料與端點封存 | 多端點原件、壓實封存與查詢視圖 |
| [data_free_public](../data_free_public/README.md) | 免費公開市場、鏈狀態與風險背景觀測 | 多來源長格式觀測表及原始回應 |
| [data_public_economic](../data_public_economic/README.md) | BEA、Census 與台灣實價登錄歷史原值 | 官方經濟觀測與逐批可追溯的不動產資料 |
| [data_keyed_public_catalogs](../data_keyed_public_catalogs/README.md) | 需金鑰公開來源的目錄、範圍與當前快照 | 發現／容量規劃資料，部分端點為實際前瞻快照 |
| [data_yahoo](../data_yahoo/README.md) | Yahoo 股票、外匯與加密資產行情 | 供應商行情、公司行動與相容特徵 |
| [data_peperstone](../data_peperstone/README.md) | Pepperstone 名單分組的 Yahoo 行情 | 分組研究清單的行情代理，不是券商原生交易紀錄 |

## 舊版相容資料

| 資料目錄／個別說明 | 內容 | 角色 |
| --- | --- | --- |
| [data_parquet](../data_parquet/README.md) | 舊版台股逐股行情與財務衍生特徵 | 保留的歷史特徵庫，不是現行唯一來源 |

## 版本不是全量副本

每版應是完整可重現的邏輯視圖；實體沿用未變動分區，只新增或重寫受影響部分。
不是每次將整個 data_* 複製到新版本，也不是用增量下載掩蓋後端每版全量另存。
data_news 的新版 --refresh 已支援此 copy-on-write 清單格式；舊全量基準仍保留。
其他資料目錄仍依各自既有 writer 與 manifest，本次沒有宣稱已全部遷移。
全部根目錄已登錄共同政策；共享發布器的大檔區塊版本、reader-first 啟用及實際部署界線見 [全資料集增量版本架構](dataset_incremental_storage.md)。
所有保留版本仍引用的物件都要保護；不能僅看檔案位於舊版本目錄就刪除。

## 不能混為一談的數量與時間

一列可能是一根 K 棒、一個指標值、一個交易事件、一篇文章、一組股票關聯，或一份資料目錄。
寬表的一列還可能包含數萬個標的欄位。不同粒度不能直接相加；原件、壓實檔、版本與研究視圖也不能重複計數。

README 的檔案抽樣只讀少數 footer；抽樣起訖不代表全市場無缺口。正式資料集若有 manifest／驗證收據，
應使用該版本的數量與完整性結論。data_news 的現有人工說明與固定版本統計原樣保留，由新聞建置器維護。

資料所屬期間、事件時間、發布時間、交易日與取得時間需分別處理。估計發布規則可以保留供已選研究使用，
但不得改寫成來源證實的精確時刻；缺價格與缺內文不會為湊完整而捏造。

## 本次範圍與維護

範圍是根目錄實際存在的 data_*，不把 configs/data_sync 等設定當成資料集；子表／分區由各自 README 介紹。
data_bybit 與 data_tw_public 是符號連結，既有入口保持不變；README 工具不搬資料，正式遷移另以逐項收據驗收。

人類描述維護在 [configs/data_readmes.yaml](../configs/data_readmes.yaml)。
新增 data_* 時先補上個別用途、粒度與來源；檢查器會拒絕沒有專屬說明的新目錄，不自動生成空泛模板。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/write_data_readmes.py --write
run_fintech_python scripts/write_data_readmes.py --check
```

工具只維護 README／此索引及指定的查核收據。不讀 .env、不呼叫 API、不改排程、原值或訓練設定。
資料檔本身維持原本保存政策；可重建的本機 README 抽樣不代表資料可刪除。
