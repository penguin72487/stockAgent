# TEJ Smart Wizard 完整欄位清冊、下載器與監控

日期：2026-10-02（Asia/Taipei）。這份是現行取得／維運文件；
[10 月 1 日清單](tej_smart_wizard_inventory_2026-10-01.md)保留為不完整目錄的歷史快照。
**最新吞吐量／並行分析與部署接受見
[2026-10-04 TEJ 實測](tej_throughput_2026-10-04.md)。下方「2026-10-03 晚間：
不明查詢與無回應外掛恢復」及其他有限批次、常駐啟動／停機紀錄是各自時間點的
歷史證據，不是永久運行狀態。**

## 2026-10-04：本機缺口優先、依資料價值派工

API 與 Smart Wizard 現在共用
[`tej_local_gap_value_priority_v1`](../downloader/tej_value_priority.py)，
策略可在 [設定檔](../configs/tej_value_priority.json)調整。
只改排程，不改原始數值、來源鍵、原生欄位次序、公司／日期範圍、分頁 cursor、
已完成工作、收據、授權／配額或模型 ABI。已支付的 API 分頁保留原 cursor 繼續，
不為換排序重新取第一頁。

### 完整清單與排序含義

- [283 張表的下載價值排序](../artifacts/data_quality/tej_value_priority_2026-10-04/applied_v1/ranked_tables.csv)：
  255 張 Wizard 表＋28 張 API 表，包含價值理由、優先欄位、本機缺口／校驗候選數。
- [47,352 項欄位／科目排序](../artifacts/data_quality/tej_value_priority_2026-10-04/applied_v1/ranked_fields.csv)：
  Wizard 45,826 欄＋API 974 原生欄＋552 財會系列定義。
  552 項是 276 科目在累計／單季的定義，不是另已取得 552 項完整歷史。
- [全來源本機對照證據](../artifacts/data_quality/tej_value_priority_2026-10-04/applied_v1/local_feature_evidence.csv)：
  本次重新讀取 9,124 項來源欄位記錄，沿用 FinLab 收據、FinMind 完成任務／欄位樣本、
  公開監控清冊；不是只查 TEJ 資料夾。另標示 Wizard 已取得的非空欄位格點。
- [套用收據](../artifacts/data_quality/tej_value_priority_2026-10-04/applied_v1/application.json)：
  230 個 Wizard 待執行工作及 1,078 個 API 待執行工作已換優先序。
  同一 SQLite 寫交易核對了 Wizard 1,270 個保留狀態工作、API 1,084 個工作，
  請求／生命週期與 API cursor／已取得筆數 SHA 在套用前後一致；沒有發送來源請求。

排序為「本機沒有明確觀測、歷史／範圍缺口或尚待核對」在前，「已有對照、可做校驗」在後；
各組內依研究價值排序，同價值表輪轉。不是依表名、表小或快照就優先。

| 價值層 | 主要內容 | 排序理由 |
| --- | --- | --- |
| 最高 | 契約調整、交易／容量限制、除權息、停復牌與下市生命週期 | 補執行、契約及樣本存續的必要上下文 |
| 高 | 期權結算／未平倉／隱含波動、財報與發布欄位、借券／融資／籌碼、營收 | 補商品獨有狀態及可用時鐘，而不只重複 OHLC |
| 中 | 產業／治理／審計、總經、利率、匯率與跨市場資訊 | 輔助不同市場狀態與企業背景研究 |
| 後 | 已有行情概念、基金／不動產輔助表、來源 log 衍生欄 | 相同概念留校驗；輔助／非原始輸入仍保留，不取消下載 |

分數是可修改的**工程優先序**，不是經模型驗證的 alpha／特徵重要性。
原生 Preview／API 一次會包含上下文及多個欄位，以該批次最有價值的缺口決定表優先序；
不拆成每欄另呼叫，也不為排序重排已驗證 schema。純代碼、名稱、期間鍵降低獨立價值，
仍隨數值一起取得。清楚早於主要 2014 研究視窗的舊選擇權表保留、但排後；
其實際取得範圍仍由原有來源軸／1900 年搜尋設定決定，不裁掉較早歷史。

本次分類有 47,271 項 `missing_or_gap`、81 項 `crosscheck_candidate`。
**不能說本機真的缺 47,271 個獨立特徵**：其中包含未知同義字、表義／頻率差異、
發布／逐欄日期未知、已有部分 TEJ 匯出但剩餘範圍未證明、以及結構欄與長表科目。
一個 provider 的最早日與另一個 provider 的最新日不拼成完整區間；全空欄不當取得；
已知美股／crypto 同名欄不當台股／TAIFEX 覆蓋。校驗候選也不是全部 symbol／日期完整證明。
API 只按實際可查的試用切片比較，不因本機欠 2014 年而優先重抓 API 已有的 2025 年重複值。
真正跳過 Wizard 格點仍只使用下節已驗證的 API 鍵／欄位／單位收據對應。

### 常駐更新、實際派工與操作

每 15 分鐘在既有工作邊界更新本機缺口／排序；不在每個來源 call 重掃歷史 Parquet，
不呼叫 API 或 GUI 做排序。新清點表、新 lazy 分片與新 API seed 也繼承共用優先序。
清冊或快取暫壞時保留已核准排序並在 15 分鐘後再查，不因此停住健康下載器，
也不解除未知 Preview 結果、來源隔離、重試時間或官方配額的保護。

01:59 套用後實際取得 `TDR Cash Dividends`，重啟後持續取得
`Company Suspended Records`；安全重啟只換 Python supervisor，不重開 Excel 或改登入。
API 下一個已改為 `TRAIL/TAMT`，後續依序為選擇權日交易、財報發布封面、營收、
期貨／財務科目等；當時官方日筆數已達 50,000，所以是配額等待，沒有為優先序重排
額外呼叫資料。配額恢復後由既有 timer 按新排序繼續；官方重置時刻尚未驗證，不承諾午夜解鎖。

公開 TEJ status 會附 `planning.acquisition_priority` 和逐表 `collection_priority/rank`，
ETA 的 P1/P2/P3 改為按實際價值排序與依賴表推算，不再假設 P1 一定先於 P2。
它們仍是資料對照分類、連續執行情境，不是保證抓完日期；來源阻礙／未知等待仍單獨保留。

02:10（臺北）[實際運行接受快照](../artifacts/data_quality/tej_value_priority_2026-10-04/applied_v1/runtime_acceptance_v1.json)：
兩條佇列的待執行優先序錯配均為 **0**；最終 supervisor 啟動後已有 25 份下載查詢收據、
99 列結果（包含合法空查詢；不是唯一觀測筆數），下一個可執行工作為高價值的股東會資料。
公開唯讀端點已回傳共用排序及三個按價值依賴推算的階段 ETA，原始值不外傳。
既有 22 個來源／中繼資料隔離工作仍保留，不能把排序修正說成來源錯誤已全修復；
21 組非股價跨通道對應仍待核對，不因同名就取消 Wizard 取得範圍。
較廣 TEJ／公開面板迴歸 **1,670 項通過**；最後快取防護修正後的排序／ETA／scheduler／API
聚焦驗證 **664 項通過**。沒有重啟交易、行情擷取或 Discord 服務。

重算／檢查（輸出用新目錄，舊收據不覆寫）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/rank_tej_acquisition_value.py \
  --output artifacts/data_quality/tej_value_priority_2026-10-04/REVIEW_NEW
# 確認價值設定後，加 --apply 套用兩條既有佇列；不呼叫資料來源。
run_fintech_python -m pytest -q test/test_tej*.py \
  test/test_crypto_client_transport_retry.py test/test_public_dashboards.py
```

## 2026-10-04：試用 API 清冊與 Smart Wizard 分工

本次使用 `.env` 既有 `TEJ_API_KEY` 驗證實際帳號，沒有把金鑰、帳號身份或
原始觀測值寫進公開清冊。官網公開 `TRAIL` 目錄為 25 表，帳號權限另列
3 張不動產表，合計 **28 表／974 個表內 schema 欄位**；27 表有非空回應，
`TALANDTR` 的本次無篩選查詢為空，不能推定為永久無資料。
28 表的公開欄位定義均與本次授權 metadata 完整相符。

| 清冊／證據 | 內容 |
| --- | --- |
| [全部資料表](../artifacts/data_quality/tej_trial_api_inventory_2026-10-04/tables.csv) | 分類、欄數、頻率語義、公開目錄年份／筆數與實際日期分開列示 |
| [974 欄完整定義](../artifacts/data_quality/tej_trial_api_inventory_2026-10-04/fields.csv) | 原始欄名、中文、型別、來源單位、描述、表內最早／最新觀測；不是全欄歷史完整證明 |
| [帳號逐表可用範圍](../artifacts/data_quality/tej_trial_api_inventory_2026-10-04/account_table_scopes.csv) | 28 表逐一驗證；授權起始年不冒充實際資料起始年，未宣告主鍵仍列為未知 |
| [財報會計科目](../artifacts/data_quality/tej_trial_api_inventory_2026-10-04/financial_account_fields.csv) | 276 個來源會計科目、累計／單季合計 552 個系列定義；不是 552 個已補齊的歷史特徵 |
| [API／Wizard 分工](../artifacts/data_quality/tej_trial_api_inventory_2026-10-04/source_allocation_plan.csv) | 28 表 API 工作、22 組 Wizard 表級候選對應、已核對與未核對的排除範圍 |
| [逐表下載快照](../artifacts/data_quality/tej_trial_api_inventory_2026-10-04/download_status_snapshot.csv) | 批次列數與首末日期、巡檢取樣列數、已完成／待執行查詢分開；不是唯一觀測或全歷史完成比例 |
| [日股價 33 欄對應](../artifacts/data_quality/tej_trial_api_inventory_2026-10-04/reviewed_daily_price_field_pairs.csv) | 32 欄通過單位檢查；市值因來源單位矛盾不排除 Wizard。僅未調整日股價 |
| [本次接受快照](../artifacts/data_quality/tej_trial_api_inventory_2026-10-04/acceptance.json) | 實際 API 收據、配額基準、服務、測試、可排除格點及未完成事項；不是永久狀態 |

### 可取得資訊與真實歷史界限

- 公司／交易：基本資料 `AIND`、股東會 `TAMT`、月營收盈餘 `TASALE`、
  融資融券 `TAGIN`、未調整日股價 `TAPRCD`、外資持股 `TAQFII`、
  三大法人買賣超 `TATINST1`。
- 財報：會計科目字典 `TAIACC`；累計值 `TAIM1A`、累計封面 `TAIM1AA`；
  單季值 `TAIM1AQ`、單季封面 `TAIM1AQA`。
  數值表是 `coid/mdate/acc_code/acc_value` 長表；會計科目是潛在特徵，
  不能只計成四欄，也不能未核對累計／單季／幣別／單位就排除 Wizard 寬表。
- 基金：`TAATT`、`TANAV`；境外基金 `TAOFATT`、`TAOFCAN`、`TAOFIVA`、
  `TAOFIVP`、`TAOFMNV`、`TAOFNAV`、`TAOFSUSP`、`TAOFUNDS`，涵蓋基本資料、
  淨值、股息、區域／產業配置、績效與暫停計價。
- 期貨／選擇權：`TAFUTR`、`TAOPBAS`、`TAOPTION`，不宣稱包含分鐘、逐筆、
  L2 或已確認的完整歷史部位限額。
- 不動產：`TAAPRRENT`、`TAAPRTRAN`、`TALANDTR`。

本次實查日股價、融資融券、三大法人、期貨等主要日表的首末日期為
**2025-01-02～2025-12-31**；月表為 2025-01～12，季財報為 2025-03～12。
公司與部分基金／選擇權基本資料快照是 2026-01-01；不動產交易目前只驗到
2025-05-15。完整逐表差異見範圍 CSV。
官網文字、公開目錄 `minYear` 與帳號授權 `dataStartYear` 都不是逐檔歷史覆蓋證明。
`mdate` 是資料業務日期，不是精確發布時間；metadata 刷新時間也不作為發布時間。
頻率欄是來源語義分類，固定發布時刻仍未驗證，不承諾試用庫追到 2026 年即時最新。

### 配額、取得與失敗恢復

[TEJ 官方試用限制](https://tejtw.github.io/EN-TEJAPI/)為
每天最多 **500 次／50,000 筆**，每頁最多 **10,000 筆**，每個查詢分頁累計最多
**50,000 筆**。本帳號回應 `multiConn=false`，API 同時只設一個 writer；
API 與既有串行 Smart Wizard 可以同時運行，但未認定兩者授權／配額必然獨立。
本次未取得可信每秒上限，不把 10 req/s 或任何自訂間隔冒充官方上限。

使用既有共用傳輸、重試工具、dataset lock、原子收據與 Parquet 路徑，不開第二套
Smart Wizard 框架。每個 HTTP 動作在 SQLite 先預留配額；不明網路回應保留整頁
配額，已收到但本機轉檔失敗用原始 JSON＋精確 intent 恢復，不重新呼叫 API。
支援 gzip、完整 Decimal 數值及 zstd Parquet；來源數值欄的空字串／符號或精度超出
宣告型別時保留字串並在收據標記，不變成零、假觀測或有損四捨五入。
原始張／千股、千元等來源單位仍保留；這批 native 儲存不是已正規化為股的訓練 ABI。

本機 admission 保守計入 metadata／帳號等所有 HTTP 動作，官方
`todayReqCount` 另列為有時間戳的快照，不把不同口徑稱為相同流量。
官方每日重置時區尚未確認，暫行「日曆＋滾動 24 小時」界限；
不能聲稱已做到官方重置瞬間追抓。429／503 有 `Retry-After` 時保存冷卻期限，
冷卻或已知配額不足期間的排程只讀本機，不反覆呼叫來源。

帳號目前有效期 **2026-10-04～2027-01-04**。公開目錄筆數合計
11,586,826，假設數字準確且範圍不再擴大，每日 50,000 筆至少需 **232 天**；
這是容量下界，不是實測 ETA，公開筆數已發現與實際查詢不一致。
因此三個月試用 API 不足以承諾取得全部目錄資料，必須保留 Wizard 的補充工作。

API 的 28 表都已建立工作清單；基本資料／字典及小表先取得，
一個既有最早日股價工作另提高優先度，用於真實範圍排除接受驗證。
按已驗到的日期、月／季週期與需要時的互斥 entity 範圍分片，不拿現存股票清單
排除下市標的。查詢碰到 50,000 筆界限改建互斥子範圍，不重置 cursor 冒充無限分頁。
仍可能有來源未宣告的粒度與版本問題；不宣稱每個 native 表已有唯一鍵或全歷史完成。

### 精確分工而不是整表移除

API 所有原始表寫到私有 `data_tej/api_trial_v1/`，不變成假的 Wizard 收據。
只有核對過的 `TRAIL/TAPRCD` 與
`TEJ Equity / TSE/OTC Unadjusted_Price(Daily)` 的 33 個欄位對應中，
**32 欄**能用實際收據與逐欄單位檢查支援「標的＋日＋欄位」非空格點排除。
市值 `mv` 的中文是「百萬元」，來源單位卻是 `NTD,T`；未做數值互校前，不推測
哪一個才正確，也不拿它排除 Wizard 的 `Market Cap.(NTD MN)`。
原始值保留，materialized coverage 改版為 `tej_exact_native_fact_delegation_v2`，
重建衍生索引而不刪除來源；舊契約的 eligibility 不沿用。
**API 尚未取得、來源空值／型別未確認、數值版本衝突、較早或較新歷史仍由 Wizard 補。**
另外 21 組候選表還需要語義／鍵／單位核對，不整表排除；快照、月頻與第三鍵事件
不沿用日股價格點契約。本次分工是部分已驗證部署，不是全 28 表零重複承諾。

2026-10-04 **01:37（臺北）**接受快照：API 原始收據共 70 頁、49,997 列，
其中 49,938 列為正式批次、59 列為範圍取樣；原始 JSON→Parquet 值完整核對通過。
早期部分取樣沒有記錄原始 Parquet hash，沒有偽造舊 hash；改用保留的 raw SHA
及逐值對照驗證當下 materialization。帳號端當天回報 **50,000 筆／73 次資料呼叫**；
本機包括帳號與 metadata 的保守 admission 為 109 次。配額已知不足後排程不再取資料。
日股價兩個批次各 10,000 筆、鍵互斥，已建立 20,001 個已觀測標的／日索引，
可支援 563,777 個非空且單位通過的欄位格點；這不是 563,777 筆獨立資料列。
市值排除 bit 為零。6 個查詢完整結束、1,078 個尚待處理，沒有 API blocked 工作；
全部歷史仍未完成。Wizard 新程序正常取得新收據、未留下不明提交 barrier。
目前尚未產生真實 Wizard repartition 審計；範圍扣除完整性已由 canonical queue
fixture／不明狀態保留／全部格點測試驗證，但不能稱為已量得桌面 query 節省
或全來源去重接受完成。

Wizard 在來源提交之前，將 pending 工作精確拆成剩餘矩形，保留原始工作、
獨立分配審計與原有收據。原工作記 `superseded_exact_api_scope_v1`，不是 completed。
只改實際桌面工作分母；按欄位拆分可能增加 Preview 行數，不能把 API 筆數當成
Wizard 已完成列數或把工作拆分當作已量得的整體速度提升。
已提交、正在執行、不明結果及 blocked 的來源工作不會被此機制重新分配或重送。

現場 API 分頁另驗出同一個 cursor 可回傳互斥的新資料頁，token 不變不表示
資料卡住；以原生頁面內容雜湊判斷是否重複。初次被舊 token 旋轉假設擋下的
第二頁已保留原始 JSON 與精確 intent，可本機重建收據，不另付一次來源流量。
續頁保留原始 page size；未確認 server 能在 cursor 中途縮頁前，剩餘配額不足
完整頁時不冒險送出，改讓其他可用新查詢使用剩額。

維運指令（均在 repo root）：

```bash
source scripts/runtime_env.sh
run_fintech_python -m downloader.download_tej_api status
run_fintech_python -m downloader.download_tej_api run --max-pages 1
run_fintech_python -m downloader.download_tej_api recover
run_fintech_python -m downloader.download_tej_api reference-fields
```

`inventory` 會重新呼叫每表首末資料、消耗配額，不是唯讀狀態指令。
完成首次 inventory／seed 後，使用
`bash scripts/install_tej_api_trial_service.sh --start` 安裝 API oneshot 與
`stockagent-tej-api-trial.timer`；每個完成批次後 15 分鐘檢查一次。
只以當下有效帳號與 admission 配額抓取；大批次有界、idle／等待不是全歷史完成。
監控目前是私有 metadata 與上述接受快照，本次沒有新增網頁 API 進度欄位或
遠端訓練發布。現場部署與持續運行須讀新收據，不以 service active 單獨證明。

## 清點完成的是什麼

實際登入帳號的 **30／30 個 Type 分類**均有完成巡檢證據，合計 **255 張表、
45,826 個獨立表內欄位**，其中 **2 張表的欄位選單為空**。
空選單不推定為無授權、永久無資料或完成下載。
這是帳號當下可見目錄，不是 45,826 個不同經濟概念，也不代表全歷史已下載。

| 清冊 | 用途 |
| --- | --- |
| [全部欄位 CSV](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/plan_v3/all_fields.csv) | 每欄來源表、欄位身份、分類、單位、既有資料比對與階段 |
| [全部資料表](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/plan_v3/tables.csv) | 包括沒有欄位的表，保留 schema SHA256 |
| [分類完成證據](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/plan_v3/catalog_scopes.csv) | 30 個分類的巡檢範圍；不以整份檔案最後一次失敗抹去已完成分類 |
| [完整表格報告](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/plan_v3/inventory.md) | 不做 Top-K 截斷的全部表格 |
| [來源摘要與 SHA](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/plan_v3/summary.json) | 三份原始目錄收據、完整分類聯集、快照與比對範圍 |
| [逐表歷史與下載狀態快照](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/acquisition_snapshot_v6/table_history_status.csv) | 本版實際已取得／待驗的範圍、列數、容量、條件式估時；不是即時頁 |
| [全部欄位下載狀態快照](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/acquisition_snapshot_v6/feature_download_status.csv) | 45,826 個欄位逐列狀態；空格代表未知，並非零 |

同一張表的不同欄位、同名欄位在不同產業／合併／個體／單季／累計版本中，
保留各自身份；不直接平均或覆蓋其他來源。

### 2026-10-02 範圍撤回與重新驗證

**完整的是目錄，不是全歷史。** 實機發現兩個 `MySql:malloc` 視窗及接續的
`Cannot find table 0.`，公司／日期群組因此停用。舊版還能讀到緩存清單，
造成台股、TDR、選擇權等不同來源取得同一份外匯公司清單。
因此先前「253 表／1,416,341,180 格點」和 `acquisition_snapshot_v1`
的歷史軸不再作為完成率分母；原始證據保留，不覆寫為成功。

取得範圍升級為 **v4／`editable_source_scope_v1`**。來源事件改為一次正常同步通知、
等待完成，不以已設好的 combo index 當成公司選單已刷新；每個來源選取／日期搜尋後，
以及最終資料查詢之前，驗證沒有可見來源通知、公司與日期群組可用、
Type／SmartID／表／完整欄位讀回一致。不強制啟用被停用的控制項，
正常 plan／download 僅透過既有 New Table checkbox 選擇可編輯的新查詢模式；
讀回既有結果不切換該模式，也不修改 Office／授權／安全政策。
這個 checkbox 不表示建立、儲存或覆蓋使用者工作簿。正常空回必須確認精確視窗真的關閉。
未知視窗仍停止。歷史復原要求獨立核對的精確 HWND；本次維運另增加明確啟用的
metadata-only 隔離規則，僅處理已核對的兩種來源 runtime 通知，詳見下節。
不處理登入／配額／權限視窗，不重送失敗表的資料查詢。

撤回時保留 14 次已完成下載的原始檔、Parquet、收據及完成工作狀態：
**5,480 列／87,673 個非空值／1,049,246 bytes**。以獨立 legacy metadata 顯示，
未算入新版覆蓋率；253 張非空表重新排程，2 張空欄位表仍待核對。
這不表示所有舊值已被證明錯誤，而是沒有可靠的完整來源範圍證明。
v4 的 task／來源目錄與 Parquet 路徑不覆寫舊版本；結果不明的 Preview 必須先恢復，
不能用撤回範圍來繞過該 barrier。

新版首張表已重新核對為 735 個系列／670 個月格點，選單範圍 1971-01～2026-10；
這依然不是已證實的原生最早／最新歷史。首 3 次新版下載實際取得
**3,256 列／52,096 個非空值**，含 2 次明確空回。
2026-10-02 **07:48:37（臺北）**的 v5 狀態快照已有 **33 張表的可用查詢軸、
8,129 列／130,064 個非空值、6 次明確空回**，另有 2 張表被隔離待檢查。
10 個工作有限下載批次已正常結束；當時另一有限批次正在清點剩餘歷史軸，
只有此批次正常結束才接續最多 100 個資料工作，未知結果會立即停下。
此時間點不是永久執行狀態或完成承諾。
其餘範圍與全歷史未完成，狀態請以網站的工作觀測時間為準。

`Delisted Option Attribute(before 2009)`、`Delisted Option Attribute(2024~2025)`
的來源選單準備回報 `MySql:malloc`。
已保留診斷，尚未判定為原始資料錯誤或沒有權限。這種 discover 工作沒有送市場
Preview：獨立核對並正常確認精確 runtime 視窗後，再以唯讀 readback 確認沒有通知、
正常來源選取控制項可用，才把它標成可恢復的來源中繼資料錯誤，讓其他表繼續。
來源失敗可能讓 combo 準備尚未完成／回退，所以記錄當前 binding，不冒充它等於失敗目標，
也不採納其公司／日期範圍。該表仍 blocked／需檢查，不自動重試，不增加完成率。
不明資料 Preview、無法讀回的來源 selector、配額／權限／登入視窗不可走此路徑。
這兩張表沒有恢復為已完成，也沒有把來源分配記憶體失敗歸因為使用者 RAM 不足、
原始資料錯誤或權限不足；真正原因尚需來源端確認。

### 本次運行診斷與修復

本次開始檢查時，**collector 已停止，不是正常持續下載**：
`Delisted Option(2022~2023)` 的來源選單通知未完成，而不是成功下載；
面板／gateway 可讀不代表 collector 存活。沒有將其結果冒充為空資料或沿用另一張表。

已修正的取得路徑：

- **選單失敗不再卡住所有可用表。** `verified_metadata_only_isolation_v1` 要求原始私有
  plan 與 task 完整相符，該 action 不可能送市場 Preview；以有界唯讀檢查確認介面穩定、
  正常 selector 可用且無通知後，保留該表 blocked，其他表可繼續。
  `metadata_failure_isolation` 與 `acknowledge_known_metadata_runtime_notices` 明確啟用時，
  最多核對三輪／正常確認兩個精確、同 owner、單一 OK 的已知 runtime 通知。
  不未知重試、不採納失敗表的緩存軸，也不解除不明資料 Preview。
- **來源清點與下載交錯。** `phase_preserving_discovery_download_interleave_v1` 保留 P1→P2→P3，
  同階段每最多四個資料工作後清點一張新表，資料表間輪轉；不再先把所有來源清點完才下載。
  有限批次結束另記 `batch_finished`／原因／嘗試數，不冒充全歷史完成或仍在執行。
- **修正實機 E8033。** `TSE/OTC Adjusted_Price(Daily)-Ex_R+D` 的 33 個特徵加兩鍵，
  被 Preview 明確拒絕。這與[官方操作手冊](https://www.tej.com.tw/TEJPLUS/TEJE3_TCHINESE.pdf)
  的 Preview 30 欄限制相符；不是資料不存在。精確失敗視窗只確認一次，保留原始拒絕證據。
  現在每片最多 28 特徵＋兩鍵，不把官方預覽限制誤當每日 API 配額。
- **按需建立分片。** 規劃 contract 為 `preview_30_columns_lazy_fields_v1`，來源／值解讀仍為 v4；
  新分片的精確範圍參與 task identity／plan fingerprint，不能用不相容計畫續跑。
  共用已驗證公司／日期軸、欄位 schema 與緊湊索引範圍，每表至多一個待執行／執行中資料工作，
  不先展開數十萬 Cartesian 工作。28 欄片與尾片交錯，尾端特徵不必等第一片全部歷史抓完。
  已完成的完整欄位 scope（含明確空回）從新計畫排除，舊 pending 記錄標記 superseded 而不刪。
  首次升級另檢查 SQLite journal 空間；既有原始檔、Parquet、收據和實體 DB 均保留。
- **修正選取事件／DPI／焦點。** 原生清單選取可改變，但 WinForms 邏輯仍使用上次 SelectedItems，
  造成最後少欄或加入錯欄。[Microsoft 的控制項規則](https://learn.microsoft.com/en-us/windows/win32/controls/lbn-selchange)
  明示程式選取不會自行產生該通知；現在核對原生項目名稱／scope、送一次正常選取通知，
  再讀回加入清單的個數與目標名稱，最後完整比對欄位／公司／日期才允許 Preview。
  不依賴清單像素座標、不反覆點選。日期仍要求精確前景／群組焦點；無法取得就不輸入其他程式。
  PowerShell `$null` 傳入 C# string 變空字串的邊界也已修正，清空列表不讀取 index −1。
- **失敗證據分型。** 只有 exact task 的 structured proof 確認 Preview 不可能送出，
  日期／清單本機輸入失敗才有最多兩次、五秒間隔的安全重試。
  次數耗盡留 blocked；實際修正實作後，operator 可用最新 attempt 的精確 proof＋唯讀介面檢查
  有審計地恢復有限預算。自動 loop 不會重設次數；配額、權限、已提交／不明查詢都不適用。
  舊橋接沒有 structured proof 的故障只接受已審閱原始 bridge SHA 與 exact pre-Preview callsite，
  不拿今天的程式推測昨天是否送過查詢。
- **增加 Preview 啟用檢查。** 日期不必修改時原先可能跳過前景啟用；
  [Microsoft 說明](https://learn.microsoft.com/en-us/windows/win32/controls/bm-click)指出 inactive dialog
  的 BM_CLICK 可能失效。`active_owned_preview_submission_v1` 在寫入查詢 stage／提交之前，
  核對精確 foreground、owner thread active window 和 Preview 按鈕可見／可用。
  未通過就記 `local_query_activation_failed_before_preview`，不送 Preview。
  這是已實作的失效防護；目前未讀到結果的根因尚未由來源端證實，不把此推論寫成定論。

新增 [唯讀完整收據稽核](../scripts/audit_tej_history.py)：一次 SQL snapshot 的全部 v4 完成下載，
逐份重驗原始／Parquet SHA、容量、schema、來源鍵範圍、列數與逐欄非空數；
不操作 Windows、不呼叫 TEJ、不覆蓋前次報告，也不把後續新 commit 混入當次統計。
[11:49（臺北）的修復中快照](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/runtime_repair_v2/audit.json)
核對 23 個完成下載、11,848 分片列／228,707 非空值，零檔案驗證錯誤、零欄位計數不一致。
這是本機收據一致性證明，不是原生全歷史或發布時點已完整。

**12:17（臺北）的歷史接受邊界：當時尚不能宣稱整體恢復正常。**
[本次完整本機稽核](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/runtime_repair_v3/audit.json)
仍為 23 個已完成下載、11,848 分片列／228,707 非空值，零檔案驗證錯誤與欄位計數不一致。
已有 37 張表取得 verified axes；資料下載沒有全部完成。
本次新取得 10 個完成下載收據（包含正常空回），不是原生歷史完整率。
`Equity View & ROI (From 2008)` 實機已正確讀回 **28 欄／32 公司／208 日**的
pre-Preview 選取，原先欄位事件問題不再攔住該步；但這一次提交後 150 秒沒有可核對的 Preview。
兩次只讀既有結果恢復未取得資料；native data grid 形狀為 0／0，
這不等於來源明確回覆「沒有資料」，也不能當成零筆成功。
當時保留 `unknown_outcome_no_auto_retry`，collector 停止，沒有反覆送查詢。
另有上述三張選擇權表的 metadata-only 故障仍被隔離、未冒充修復。
因此總共四個 blocked 工作；單一資料未知結果仍是全域 barrier。
當時曾詢問是否允許一次人工重試；後續新的修復要求與本輪單次 operator replay
已取得新結果，詳見最新接受快照。不能把新結果回填成原未知查詢從未送出。

12:17 的工作計畫以 37 張已驗證表保留 813,875 個未 materialize 的分片查詢，
加 36 個 pending／一個 blocked 資料工作；這不是所有 253 張非空表的完整分母。
598,788 個舊 pending 範圍保留為 superseded，不再作 ready 工作。
DB 實體約 4.39 GB 未刪／未縮；沒有把保留頁數當成已回收容量。
本機唯讀 task-selection 五次樣本中位約 33 µs；不等於外部下載加速比例。

### 本輪新增的三鍵修復計畫與資料邊界

2026-10-02 13:28 後的實機查詢，`Shareholder Meeting` 顯示精確 E8033
「超過 30 欄」，但已選特徵只有 28 欄。已獨立核對同一來源的原生 `Key=3`
標籤，並正常確認一次精確拒絕視窗；不是空資料，不重送該被拒絕的原分片。
[官方手冊](https://www.tej.com.tw/TEJPLUS/TEJE3_TCHINESE.pdf)也區分 Key=1／2／3 格式；
不能把所有資料表都假定成兩個鍵。

修復順序：先核對來源鍵格式 → 保留舊計畫、拒絕與查詢證據 → 建立有不同
fingerprint 的 27 特徵＋3 鍵計畫 → 小批量真實驗證 → 全收據稽核與面板驗收。
`native_company_period_record_key3_v1` 使用獨立 Parquet 子路徑；第三鍵保留原始字串
（含前置零），同公司／同日的不同事件不合併、不填零、不去掉額外鍵。
同鍵重複、日期越界、未命名鍵格式和錯誤欄數仍拒收。Preview 樣本校驗也按完整三鍵比對。
正常空回的鍵欄名稱只是空 schema placeholder，不冒充讀到的原生欄名。

已完成舊兩鍵 scope 的表不得自動換粒度後重抓／重算覆蓋率；須另做保留覆蓋的 migration。
進一步實機檢查發現 `Offshore Fund's Attribute` 是 Key=1，舊計畫誤把
9,425 個代號乘上 14,948 個歷史日期，產生 140,884,900 個錯誤日期格點。
修復計畫擴充為 `native_company_observed_snapshot_key1_v1`：先獨立核對單鍵格式，
保留舊計畫而不發舊歷史查詢，再按代號／最多 29 特徵分片，驗證真實來源鍵與完整輸出。
新快照使用不同 fingerprint／Parquet 子路徑，保存實際 `observed_at_utc`；
query period 為 null，不填入舊日期、不將目前基本資料複製到每個歷史日。
單鍵快照可清點／下載，但不是可以當作 2014 年既有值的歷史訓練資料。
`auto_key3_replanning` 僅對結構化證據確認 Preview 未送出的格式不符，
在獨立穩定 Key=3 readback 後重排。結果不明或已提交查詢不適用。
本段是修復計畫與實作契約；真正取得筆數以後面的最新接受快照為準。

## 取得順序與不重複查詢

| 階段 | 表內欄位數 | 清單與判定 |
| --- | ---: | --- |
| P1 新增候選 | 44,492 | [未找到明確同概念對照](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/plan_v3/phase1_new_feature_candidates.csv)，不是已證明其他來源完全沒有 |
| P2 歷史／口徑缺口 | 811 | [補歷史候選](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/plan_v3/phase2_history_gap_candidates.csv)，可比較的同頻率／口徑內，筆數由少到多 |
| P3 多源校驗 | 523 | [獨立來源校驗候選](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/plan_v3/phase3_cross_source_validation.csv)，下載不等於已完成逐鍵校驗 |

同表有 P1 欄位時，同一次歷史排程也涵蓋其 P2／P3 欄位；受 Preview 限制按不重疊欄位片抓取，
不因階段不同，重抓同一欄位／公司／日期 scope。
這份快照的 253 張非空表都含 P1 候選，另 2 張空欄位表待核對，所以「獨立 P2／P3 下載表」為零；
面板另列候選欄位與併入前階段的表數，逐表篩選則按該表包含的欄位階段。
尚未完成全來源校驗，不能把這個排程當成語義完全去重證明。

## 速率、每日限制與 API 用量

Smart Wizard 桌面登入與 TEJ API 是不同通道。
本次查過官方桌面指南、API 文件及帳號本機設定位置；**未取得此桌面帳號的
req/s、每日請求／列數、月列數、重置時間或已用量**，不能宣稱無限，也不套用舊方案範例。

[官方 Python 文件](https://api.tej.com.tw/document_python.html)（頁面標示 2020-01-10 更新）
列出 `ApiConfig.info()` 的 `reqDayLimit`、`rowsDayLimit`、`rowsMonthLimit`、
`todayReqCount`、`todayRows`、`monthRows`。這需要獨立 API key；回傳同時包含金鑰和身分，
不能原樣印出或送到瀏覽器。
文件另載 API 每頁最多 10,000 筆、一次 paginate 總量最多 1,000,000 筆；
分頁會產生多個請求，**不是 Smart Wizard 桌面限額**。
[官方 E-Shop API 方案](https://eshop.tej.com.tw/E-Shop/Edata_intro)目前列三種方案
皆每日 1,000 次，列數分別 100,000／500,000／1,000,000；這也是特定 API 方案，
不能當成本機桌面帳號的上限。每秒速率與帳號重置時點仍未核實。
[REST 文件](https://api.tej.com.tw/document_rest.html)可查表格主鍵、欄位與單位，
但不能把 API 的表代碼／單位未核對地套到桌面表。

`.env.example` 的 `TEJ_API_KEY` 只是另外申請 API 授權後的預留位置。
**目前桌面下載器不使用它，也沒有宣稱 API 採集已實作**。
面板每分鐘以本機佇列工作開始收據取樣滾動 60 分鐘次數；不含手動診斷，
不等於 TEJ 內部 HTTP 次數、帳號官方用量或線上傳輸 bytes。
實際官方數字未知就顯示未知。

官方桌面操作參考：[Smart Wizard User Guide](https://www.tej.com.tw/TEJPLUS/Smart%20Wizard%20User%20Guide.pdf)。
來源配額、登入、權限或不明視窗會停下，不自動關閉或重送；
只有來源與選取範圍已核對的正常 `ERROR1:No data !!(table)` 回覆會被記錄並正常確認。

## 第一性設計與已修正問題

1. **先完整註冊，再發現真實歷史軸。** 1900 年是搜尋起點，不是最早資料年份。
   每表先驗證介面／通知／來源，再取實際公司／系列及日期選單、分割工作；
   255 張表已註冊，但歷史軸與下載尚未全部完成。
2. **每次有界、欄位片不重複。** 單一桌面持有者、跨程序 lock 與 Windows mutex，
   2026-10-03 顯式升級後，每次最多 10,000 列／400,000 個儲存格、公司批次上限 128，
   兩次查詢開始至少間隔 2 秒（包含本次端到端耗時，不再固定完成後多等 2 秒）。
   既有已完成或暫緩重排的計畫仍保留其原本的容量與範圍。
   另遵守 Preview 30 欄（含來源的一、二或三鍵）。這是本機記憶體／UI 邊界與預覽限制，不冒充官方速率。
   每欄位片填滿容許的公司／期間範圍，不逐欄重抓已完成資料。
3. **不用錯誤的勾選讀回。** 此版本 Checkbox 是 owner-draw 按鈕；
   `BM_GETCHECK` 一律回零，改由 MSAA role 44 的 CHECKED state 驗證。
   Calendar Date／Last Period／Advanced Date 不沿用；舊未核實格點保留並標記 superseded。
4. **日期與 DPI 安全。** 區分穩定的 `yyyy/MM/dd` 外層 EDIT 與移動的輸入遮罩，
   用精確 HWND／owner 焦點輸入、每個字元及完整日期讀回；幾何只核對共享 mask 身分，不拿來點擊。
   月選單會排除第一天的下界月份，只擴大「選單搜尋」一天，最後仍精確選取原定月份。
5. **全公司不能沿用舊 Sector。** 每表核對 ALL 與實際選取集合；
   `code==>name` 與舊樣本 `code=>name` 正確解析，不把額外等號當公司代碼。
   債券頁的單位換算也可能被 UIA 命名為 Sector，而真正的產業選單已隱藏、
   仍留著上一張選擇權表的選項。改用原生可見狀態、父群組及 ALL 選單辨識；
   沒有可見產業篩選時，採該來源表的實際全代號選單，不操作隱藏舊控制項。
   新查詢把可見單位換算設為 `-` 並讀回；恢復既有結果只讀原設定、不修改。
   這不是 `-` 語義或全表單位已完成驗證的證明，設定與適用性都保留在收據中。
6. **Preview 不是一個固定名稱就能辨識。** 兩個 DataGridView 分別是資料與條件／備註，
   按實際資料欄位標頭辨識，不讀備註作市場資料。四個 tab 的 Preview 是 index 3；
   恢復已有結果只切 tab，不按新的 Preview 查詢。
   另要求 Preview tab 與資料格都原生可見，來源通知也只處理當前可見視窗；
   不靠可能錯報的 UIA IsOffscreen 採納上一張表的隱藏格點／空回。
7. **避開已重現的 Excel ActiveX 故障。** v3 不按 Export to Excel，直接保存完整、
   有界的原生 MSAA 資料格。這是精確保存「來源顯示字串」，不是底層 Excel Value2 精度證明。
   舊 v2 空回收據保留，v3 Parquet 進版本目錄，不覆寫舊來源。
   故障匯出曾清空來源預覽；只在確認工作簿沒有取得結果、保存故障證據後，手動重排該筆一次。
8. **稀疏不是填零的理由。** 來源原生模式可省略無資料公司／期間；
   驗證 Preview 列數、全輸出鍵集合的範圍／重複、schema、有限值與有界首末樣本，
   保存省略格點數，沒有強湊每個公司 × 日期的假觀測。
9. **可恢復而不重抓。** 原始 JSON、ZSTD Parquet、來源／檔案 SHA、逐欄非空數與期間、
   durable receipt 先落地；SQLite 在一次 transaction 採納，重複恢復不重複加數。
   正在執行或結果不明的工作構成全域 barrier，不能默默重送或當已完成。
   只有橋接端另外證明「日期／清單輸入失敗且 Preview 尚不可能送出」的工作可重試，
   每筆最多 2 次、間隔至少 5 秒。提交可能旗標在唯一 MSAA default action 之前設置，
   所以提交途中崩潰／逾時依然是未知，不能走安全重試。
10. **監控不採集。** 既有公開 gateway 只讀索引化 SQLite 中繼資料，
    不掃 Parquet、不呼叫 TEJ、不操作 Excel。短 transaction + rollback journal
    避免唯讀服務因 WAL `-shm` 不可寫而顯示「面板無法讀取」。
    本版以 scope contract 篩選目前工作，舊完成收據另外計數；活躍狀態索引及
    covering metadata／timing index 避免每次掃過被撤回工作的大型 request 內容。
11. **減少跨程序 UI 成本。** 找群組／按鈕／清單／日期輸入時只枚舉實際 HWND，
    不反覆展開全部公司項目或上一批 Preview 儲存格。COM 的列／欄數只在每列讀一次，
    不在每個儲存格重讀。完整讀回仍有界，總工作期限 900 秒；舊 240 秒會切斷
    已送查詢的大格點讀回。逾時只記錄並恢復現有結果，絕不自動重送。
    另外使用 [MSAA 的 child ID 值讀取](https://learn.microsoft.com/en-us/windows/win32/api/oleacc/nf-oleacc-iaccessible-get_accvalue)，
    首末樣本仍由既有物件路徑讀取並核對。不支援屬性／child ID 時才退回物件路徑，
    傳輸錯誤不忽略。[同一份來源結果的全部格點對照收據](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/msaa_child_id_full_parity_v1.json)
    驗證 3,256 列／58,608 個資料儲存格逐格一致、零 fallback、沒有新查詢；
    優化讀回約 40 秒。這是本表此結果的讀回驗證，不能當成全部表的精度／速度保證。
12. **工作佇列不複製 schema。** 大表數百欄若在每個公司／日期分批工作重複存兩份欄名，
    尚未下載就可能佔 GB 級空間。改用 table ID＋schema SHA 參照，共用不可變欄位定義；
    呼叫前還原原始完整 request，task ID／選取範圍保持不變。升級逐筆驗證精確還原，
    既有原始檔／Parquet／收據都不刪。SQLite 釋出的頁會重用，不冒充磁碟檔已縮小。
    [本機升級收據](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/queue_encoding_upgrade_v1.json)
    驗證 132,486 個工作，參數內容 2,590,668,738 → 403,700,825 bytes，減少 84.42%；
    不是來源資料容量或 SQLite 實體檔案的縮小率。
13. **磁碟與程序身分保護。** 新工作之前至少保留 5 GiB 本機餘裕，不夠就停抓，
    不刪來源。資料根目錄保持 mode 700，不接受家目錄或 workspace 根目錄當資料目標。
    私有 worker 收據保存 PID、程序起始 ticks 和有界工作期限；公開面板只核對是否仍活著，
    不暴露 PID，也不以固定五分鐘 TTL 把合法長讀回誤標為中斷。

十進位數值欄只解析明確的數字表示；原字串保留。未知地區千分位／sentinel 不猜、不補零。
千股、千元、百萬元只在來源欄名明示時另建換算欄；不改 log、不插值、不任意捨入，
來源本身的 log 衍生欄仍保存並標記不作原始值輸入。
換算 contract `explicit_field_units_v2_exact_power10_with_scale_guard` 直接移動十進位指數，
避免 Python 預設 28 位 Decimal context 對長數字默默捨入；原始字串始終保留。
若恢復結果明示已選 Thousand／Million 等顯示轉換，不再按舊欄名重乘一次。
舊收據沒有單位選取證據就維持未知，不回頭假設它們用了現在的 `-` 設定。
發布時點、原生版本、單位未知仍保留未知，不自動注入 strict/live 訓練。

## 下載與恢復指令

保持既有已登入的指定 Book2 測試查詢開啟。
批次需要實際的桌面焦點；執行時不要同時手動操作這個 Book2／查詢視窗。
`data_tej/desktop_session.json` 僅存精確 PID／HWND／標題／工作簿，mode 600，
不存帳密；視窗重新開啟後必須重新檢查 scope，不能照貼舊 handle。

```bash
source scripts/runtime_env.sh

# 本次固定歷史截止日；重複註冊不建立另一份全歷史。
# 舊 v3 工作區必須先執行下方 revalidate-scopes，再重複 register。
run_fintech_python -m downloader.download_tej_history register \
  --inventory artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/plan_v3 \
  --cutoff 2026-10-01

# 舊佇列可無損轉成共用欄位定義；保留 task ID、範圍與來源收據。
run_fintech_python -m downloader.download_tej_history compact-queue

# 本次已完成：30 欄預覽安全分片＋lazy 工作計畫；保留已完成及被取代的證據。
# 結果不明的資料工作仍是硬 barrier，不能透過升級繞過。
run_fintech_python -m downloader.download_tej_history upgrade-preview-planning

# 舊 v3 工作區需先撤回未驗證選單範圍、註冊 v4 重驗；不刪來源，重複執行不重建。
# 僅在沒有正在執行／結果不明查詢時允許，本機此步已完成。
run_fintech_python -m downloader.download_tej_history revalidate-scopes

# 只先清點各表歷史軸；不送新的 Preview 資料查詢。
run_fintech_python -m downloader.download_tej_history run --discover-only --max-tasks 253

# 有限批次；遇到不明結果立即停下。
run_fintech_python -m downloader.download_tej_history run --max-tasks 100

# 只下載已有 verified axes 的表；仍使用同一 queue／lock／scope 驗證。
run_fintech_python -m downloader.download_tej_history run --download-only --max-tasks 10

# 只恢復本機已落地的 exact task 原始檔，不發新查詢。
run_fintech_python -m downloader.download_tej_history recover \
  --task-id <task_id> --evidence <data_tej/raw/exact-task-file.json>

# 人工確認當前畫面仍是同一筆結果後，從現有 Preview 採納。
run_fintech_python -m downloader.download_tej_history recover-desktop \
  --task-id <task_id> --response preview
```

`recover-desktop --response empty` 只接受精確、同選取範圍的正常空回。
`--response plan` 恢復尚未完成的歷史軸清點：先核對現有 Type／SmartID／表／
已選全部欄位，不重送不明的 Select All；只繼續公司／日期選單的準備，不送市場資料 Preview。
大 schema 的 Clear All／Select All 也改為只提交一次、等待清單數與全名順序讀回，
不因 30 秒同步控制項逾時又點一次。
`--response plan-empty-fields` 另外要求現有來源 schema 相同、選取欄位確實為空、
沒有來源通知，才提交一次 Select All。`--response plan-preparation` 僅用於 discover 工作，
容許完成局部來源選單準備；同樣不按資料 Preview。這些選單操作仍可能有來源內部請求，
不能稱成零 API 呼叫或用其時間估計資料下載吞吐量。
已知 metadata-only 來源錯誤的隔離使用
`settle-metadata-error --task-id <discover_id> --evidence <exact_private_runtime_ack.json>`；
要求精確私有確認收據，僅做來源介面唯讀核對。它不是錯誤視窗自動關閉工具、
不重試該表、不承認其範圍，也不能解除未知資料查詢。
本次新增 `isolate-metadata --task-id <discover_id> --evidence <exact_private_original_plan.json>`，
對原始 plan 和介面逐項核對；runtime notice 的正常確認另外受明確 opt-in 與精確樣式限制。
`recover-local-input --task-id <download_id> --evidence <exact_private_original_request.json>`
只用於實作修正後的已證明未送 Preview 本機輸入故障；它本身不送資料查詢、不採納來源列。
請勿把這個 operator 指令排成自動重置迴圈。
已知 Excel 初始化錯誤的確認需要額外明確、已獨立巡檢的 `--error-window`；
這個操作不在自動下載 loop 中，不修改 ActiveX／Office／巨集／執行政策，
也不關閉 Excel、使用者工作簿或登入視窗。
v3 正常取得不再使用 Excel 匯出，因此這只是歷史故障復原工具。

目前是**有限桌面歷史批次**，不是無人值守即時追新服務。
本次歷史截止日不可改成隔天再重建同一份全歷史；增量發布時程、修訂窗口、
全表公司軸與獨立 API 權限尚未全驗證，沒有偷偷啟用每日 GUI timer。
授權原始值位於 `data_tej`，不自動公開、上傳冷庫或送遠端訓練。

## 2026-10-02 無滑鼠輸入路徑

下載、既有結果讀回及目錄清點的滑鼠注入已移除：不再使用 `SetCursorPos`、
`mouse_event` 或像素點擊備援。沿用同一個 collector、私有 session、queue、
Windows owner mutex、來源驗證與收據；不是另外建立一條資料管線。
初版執行 policy 為 `native_control_events_guarded_date_focus_no_mouse_v1`；本輪升級為
`native_control_events_guarded_date_focus_acknowledged_chars_no_mouse_v2`，
記入新準備請求／新查詢 stage 與 runtime policy；來源範圍／值 contract 仍是 v4，
不把此修改回填為舊查詢當時使用的輸入方法。

| 操作 | 實作與成功條件 |
| --- | --- |
| 主頁／既有 Preview 頁籤 | 核對 owner、四頁、非 `TCS_BUTTONS` 樣式；一次 `TCM_SETCURFOCUS`，再讀回頁碼。下載、復原與目錄清點都不用滑鼠備援。 |
| 公司／欄位／日期選單 | 沿用原生索引及一次正常 selection notification，再核對完整選取集合。 |
| 日期文字 | 精確 owner thread 的正常 `SetFocus` → 確認清除 → 同一空欄位 `EM_SETSEL` 定位至起點 → 每個字元只送一次並等待讀回 → 完整日期相符才送 Tab；失去焦點就停，未確認字元不重送。單鍵快照完全不輸入日期。 |
| Search／選取按鈕 | 沿用限定 HWND 的正常控制項事件，不送全域滑鼠按下／放開，不移動游標。 |
| Preview | 讀回實機 MSAA role=43／default action=Press；在獨立 STA thread 只執行一次 `accDoDefaultAction`，不再用可能無聲失效的 BM_CLICK／PostMessage，也沒有第二方法重送。非空結果另要求相對查詢前的資料格 signature 有可驗證變化。 |

TEJ 實機的兩個日期框與共享 mask **沒有提供 UIA ValuePattern**，不能假設直接
`SetValue` 可用。[Windows SetFocus 文件](https://learn.microsoft.com/en-us/windows/win32/api/winuser/nf-winuser-setfocus)
說明跨 thread 焦點需附接輸入佇列；現在只短暫附接精確已核對 owner，`finally` 解除附接，
不是偽造 `WM_SETFOCUS` 或強制啟用停用控制項。群組／控制項／process／thread／可見／
可用狀態或前景不符就停止。日期框邊界只用來驗證共享 mask 屬於正確日期框，
不拿來計算點擊位置。清除沒有真的改變文字就停止，不以重新寫入相同值的最後結果冒充成功。
頁籤使用[會正常通知的 focus 訊息](https://learn.microsoft.com/en-us/windows/win32/controls/tcm-setcurfocus)，
而非不會送正常通知的 `TCM_SETCURSEL`。

**不操作滑鼠不等於不占用桌面。** 日期輸入和 Preview 啟用仍需要原桌面、前景視窗／
鍵盤焦點，不保證可以一邊玩遊戲一邊無干擾地跑；這不是 headless API 或無人值守服務。
若要完全不占用這個桌面，需要另外確認正式 TEJ API 權限或獨立互動桌面環境；
現有 Smart Wizard 登入不當成已取得 API 授權。

驗收：

- 316 個 Python focused regression 與 3 個 Node 請求協調測試通過；三支 PowerShell
  完整語法檢查通過，兩支實際 acquisition helper 的 C# 均在 Windows 編譯成功。
- [獨立 Windows fixture](../scripts/verify_tej_mouse_free_input.ps1)只建立／關閉自己的
  測試表單，不接 TEJ／Excel；16 項驗收包含兩個日期真正改值再還原、正常頁籤通知、
  停用／唯讀／非日期群組控制項／非法日期拒絕，以及焦點在清除後被搶走時停止輸入、
  不寫進不相關文字框。[fixture 收據](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/no_mouse_input_v1/windows_fixture.json)
  是工程測試，不是全歷史下載成功證據。
- 13:02:15（臺北）TEJ 實機 `verify-input` 有私有審計收據：先看見 `____/__/__`，
  再逐字重新寫回 `2025/11/21`、`2026/10/01`，兩個日期的原值／格式、來源、完整欄位
  及公司／日期清單均一致；11.035 秒含完整 bridge 呼叫。沒有 Search／Preview、
  沒有採納資料列、沒有修改 queue 工作狀態／解除結果不明 barrier。
  [本次摘要](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/no_mouse_input_v1/acceptance_summary.json)
  僅含驗收中繼資料；日期與 GUI 細節留在 `data_tej/input_acceptance`／`data_tej/raw`。

需要重驗時，先保持原私有 session 的精確工作簿／來源／完整欄位／兩個日期不變：

```bash
source scripts/runtime_env.sh
run_fintech_python -m downloader.download_tej_history verify-input \
  --task-id <exact_existing_download_task_id>
```

這是明確 operator 操作，不由 scheduler 自動執行。只重新輸入同一日期，不修改來源，
不送 Search／Preview、不設成功、不恢復／重試工作。條件不符就拒絕；
原本 `ec8198f8bd884a795d0267d7` 的 `unknown_outcome_no_auto_retry` 仍保留，
不能以「改成不用滑鼠」當成該歷史查詢已有結果或可以自動重送的證明。

## 2026-10-02 再次完整運行檢查與修復計畫

本輪先固定基準，再修程式，不把前輪工程測試當成下載已恢復。
[13:11（臺北）基準稽核](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/runtime_repair_v4/baseline_audit/audit.json)
確認 23 個 v4 完成下載、11,848 分片列、228,707 非空值；檔案與逐欄計數錯誤均為零。
collector 不存活，37 表已清點，213 個 discover 尚待執行，另有 3 個已隔離的
metadata-only 錯誤與一筆結果不明的資料查詢。未知查詢的既有預覽無法恢復，
沒有新 Preview、沒有把空白 grid 認作正常空回。資料取得尚未正常持續運行。

修復範圍與接受順序：

1. **實際桌面能力檢查。** 限定現有 Excel／工作簿／Wizard HWND／PID；只讀控制項
   default action、owner、可用狀態、來源選取與日期。先確認實際事件能力，才選提交方法。
   不用滑鼠、不改 Office／授權／執行政策、不關閉使用者工作簿。
2. **每次查詢獨立證據。** 保存新 request／stage／結果的 attempt 身份，來源範圍與值 ABI
   維持 v4；恢復只接受該次 stage，不用多次查詢中任選一份原始證據，也不拿舊同 schema
   的 Preview 當成新結果。提交後錯誤／逾時仍是未知，不自動再次提交。
3. **一次性 operator 重試。** 精確指定既有未知 download 與原始私有 request，先驗證
   介面穩定且無通知，留存未知 attempt 與審計，不假稱它未曾送出或已完成。
   明確 operator 操作才允許同一唯讀範圍再查一次；不由 scheduler 自動解除 barrier。
   新結果需重新通過 schema、鍵、列數、數值／單位、Parquet 與收據採納。
4. **有限實機接受。** 先驗證現有卡住表與小批次實際回傳；通過後才接續 canonical
   P1→P2→P3、每四個資料工作交錯一張清點、同階段資料表輪轉。
   metadata-only 失敗仍逐表保留 blocked，未知資料查詢或登入／配額／權限通知停止批次。
5. **全鏈接受。** 重跑 focused regression、獨立 Windows fixture、完成收據全稽核與
   公開唯讀投影檢查；分開報告工程接受、真實取得資料、來源缺口與持續服務狀態。
6. **容量與配額。** 本輪約 65 GB 可用磁碟、5 GiB 最低保護；不刪原始資料或 4.39 GB
   歷史 queue。Smart Wizard 每日／每秒上限仍未知，不套用 TEJ API 的不同產品配額。
   尚未清點的表不填零，不造全域完成日。無人值守增量／冷庫／訓練注入不在本輪啟用。

來源事件依據：[Microsoft BM_CLICK](https://learn.microsoft.com/en-us/windows/win32/controls/bm-click)
明示 inactive dialog 可能失效，送訊息成功不等於資料已回傳；
[MSAA default action](https://learn.microsoft.com/en-us/windows/win32/api/oleacc/nf-oleacc-iaccessible-accdodefaultaction)
要求控制項本身提供能力，且部分實作可能阻塞，因此不能失敗後再補送另一種事件。
[TEJ API 官方文件](https://api.tej.com.tw/document_rest.html)的 keyInfo 配額獨立於本桌面產品。
最後結果在本節後補記，不預先承諾全部修好。

### 本輪下載與收據接受快照（14:21 臺北）

[完整本機收據稽核](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/runtime_repair_v4/final_audit/audit.json)
重驗 31 個完成下載、12,080 個**欄位分片列**、231,543 個非空值；
原始檔／Parquet 的 SHA、容量、來源鍵與逐欄計數均一致，零檔案失敗、零計數不一致。
其中 170 個欄位已有非空來源值；14 次明確空回不當作有值或原生歷史完成。
這不是唯一公司／日期歷史筆數，亦不是所有 45,826 欄已下載。
[逐欄狀態](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/runtime_repair_v4/final_inventory/feature_download_status.csv)
與[逐表狀態](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/runtime_repair_v4/final_inventory/table_history_status.csv)
涵蓋完整 255 表／45,826 欄，空格仍表示未知。

- 原未知的 `Equity View & ROI (From 2008)` 經一次明確 operator replay 取得正常
  `No data` 回覆。原 attempt／stage 與重試可能消耗流量的審計保留，不追改成原先未提交。
- `Shareholder Meeting` 真實三鍵分片取得 6 列／162 個非空值；原生鍵為
  `Company Code`／`Date_Meeting`／`Meeting YND`。第三鍵保存來源字串，不能自行解釋成事件序號。
  另兩張銀行表已核對 Key=3，未送 Preview 的舊計畫保留並安全重排。
- `Offshore Fund's Attribute` 單鍵快照已真實下載與稽核：舊錯誤公司×日期計畫
  35,345 次查詢，改成 590 次（9425 個代號、38 特徵、29＋9 欄兩片），
  **該表計畫查詢數減少 98.33%**；不是全部 provider 的實測速度提升。
  工作分母為 18,850 個按欄片計的公司快照，而非 281,769,800 個偽歷史工作格點。
  不操作停用的日期群組；來源查詢日期與首末歷史期為 null，保存真正擷取 UTC。
- 新五份資料收據綁定 exact attempt 的 prepared request／stage SHA；26 份較早收據
  保留實際舊契約，不追補成新提交方法已驗證。整合後沒有原始檔、失敗收據或舊計畫被刪。
- Indexed ready-task 五次樣本中位約 16–20 µs；4.39 GB queue 大小不是目前外部等待的證據。
  各資料庫仍以單一桌面擁有者、P1→P2→P3、每四個下載交錯一張清點與同階段表輪轉。
  有時程估計的表只估完整工作速度；尚餘 210 個未清點表，不能造全域 ETA。

### 來源故障與共享介面的恢復邊界

三張舊選擇權表仍保留失敗：`Delisted Option Attribute(before 2009)`、
`Delisted Option Attribute(2024~2025)`、`Delisted Option(2022~2023)`。
本輪再次只準備前兩表的選單，仍遇來源 metadata 錯誤，沒有送市場資料 Preview；
不把 `MySql:malloc`／`Cannot find table 0.` 當成數值本身錯誤或歷史為空。
第三表本輪沒有重查，不能說它本輪已獨立重驗。

之後來源主視窗持續停用，但無已知 owned error notice；相同 TEJAddin 程序還能完成
有界 WM_NULL 回應、沒有 Windows hung-window 標記。當次 private bytes 約 338 MB，
不能由 `malloc` 字樣直接判成整機 RAM 不足。主視窗未恢復，不聲稱整體持續正常抓取。
只有直接 owned `#32770` 的 notices=[] 不代表可用，新增只讀 root enabled／visible、
同程序 top-level 視窗、有限 Text／Button 標籤及訊息回應診斷；不讀輸入框值、帳密或工作表。

`desktop_interface_recovery_required` 是 durable SQLite barrier，損壞介面恢復前，
`run` 不 claim 工作、不送查詢／新增流量。面板明確顯示「桌面查詢介面停用」，
不是「抓完」或僅待下一次 quota reset。
在 14:21 驗收之前，使用者明確允許的範圍只有重開 Book2 查詢；正常系統關閉及
[WM_CLOSE](https://learn.microsoft.com/en-us/windows/win32/winmsg/wm-close)要求皆未完成，
兩次失敗與舊 session 保留，**沒有強制啟用、DestroyWindow、殺程序、關閉 Excel 或 TEJ Pro**。
WM_CLOSE 留給來源正常 FormClosing／確認機制，不等於強制銷毀。
當時需要更大範圍的 TEJAddin 重啟，須再獲明確允許；不能把「允許重開查詢」擴充成殺程序。
後續使用者明確表示「所有都可以重啟」，本輪才新增並執行下方有界的 exact-addin 恢復。

### 2026-10-02 14:47：明確授權後的 TEJAddin 恢復

以[恢復驗收](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/addin_restart_v1/recovery_acceptance.json)
核對：卡住的獨立 `TEJAddin` 已停止；透過原 Excel 的正常 TEJ Database Settings 命令
開啟新的程序／查詢視窗，執行檔 SHA256 與原程序一致。Book2 工作簿、Excel、
TEJ Pro、來源與收據保留；沒有儲存或關閉工作簿，沒有重啟無關服務。
只讀驗證來源 selectors 可用、binding 穩定與無已知 notice 後，才清除 durable barrier。
失敗的三個 metadata 工作沒有重設或改成完成，未確認資料結果的工作為零。

這次查出並修正四個實際恢復問題：

- `TEJProConnector` 的附屬關閉確認窗，UIA 將 native Static／Button 暴露成無名稱
  Pane；所以 UIA `Name` 空字串、Text／Button 數為零，不代表對話框真的沒有內容。
  只讀診斷改成有界原生 Static／Button caption 讀回與 PID／IsChild 核對，不讀 Edit。
  實際內容是 `Had been setting conditions.` 與 `Do you close form?`，以 CRLF 分隔，
  按鈕是 `&Yes`／`&No`；恢復程序只接受這個三控制項布局與 exact connector owner。
- 重啟額外權限與只重開查詢權限分開。停程序前重查 exact PID、start UTC、
  image SHA、原查詢與 companion 布局；只停止已檢查的 `TEJAddin` process object，
  不用程式名稱批次殺程序，不強制啟用視窗，也不對未知授權／配額／登入提示按鈕操作。
- Excel 的 Database Settings 實際支援 `TogglePattern`，不是 `InvokePattern`。
  先列出支援的 pattern，確認 Toggle 為 Off 後只送一次正常 `Toggle()`；
  無該 pattern 時才在提交前選擇已支援的 Invoke。例外後不換動作猜測或重送。
  這符合 [Microsoft 的 Toggle API 定義](https://learn.microsoft.com/en-us/dotnet/api/system.windows.automation.togglepattern.toggle?view=windowsdesktop-10.0)，
  不是用座標點擊／強制事件。
- add-in 停止後，Excel 曾切到 Book1。恢復只在 Book2 的既有唯一 Window Hwnd
  仍精確一致時，正常 `Activate()` 該工作簿 view；不換工作簿身份、不 Save／Close。

生命週期使用 `explicit_exact_addin_restart_v1`，每階段有新的 private 證據與原 session
備份。第一次 open 在取得不支援的 Invoke pattern 時失敗、尚未送 launcher 動作，
因此後續從已驗證的 stop 收據接續，而非再停程序。
每次 normal open 前另寫 immutable submission intent；若動作可能已送出，
就拒絕以舊的未提交失敗紀錄再次 open。停用介面仍保持 barrier，直到獨立來源讀回通過。
這是 operator 的有界恢復，不是無人值守的自動殺程序／自動重送下載器。

新增命令（只有使用者明確授權重啟獨立 add-in 時使用）：

```bash
source scripts/runtime_env.sh
run_fintech_python -m downloader.download_tej_history restart-addin \
  --task-id <registered_task_id> \
  --allow-restart-addin --allow-discard-query-settings
# 僅接續 exact stopped run 且 open 在 Unsupported Pattern 的 getter 失敗：
# 原 session／inspect／stop 均須吻合，沒有任何 normal open submission intent。
run_fintech_python -m downloader.download_tej_history restart-addin \
  --task-id <registered_task_id> \
  --allow-restart-addin --allow-discard-query-settings \
  --recovery-run data_tej/query_lifecycle/<exact_stopped_run>
```

本輪 429 個 TEJ focused Python 測試、3 個 Node 協調測試與四個 PowerShell AST 語法
檢查通過；新增測試覆蓋雙重權限、unknown Preview、PID／start／image pin、階段停止、
跨工作簿拒絕、接續時不再停程序，以及先前 open 可能提交時禁止重送。
新的[真實公開瀏覽器驗收](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/addin_restart_v1/rendered/rendered_acceptance.json)
在桌面 1440／手機 390px 顯示「歷史資料下載中」，搜尋／分頁／P2、無 overflow／JS
error／外部 provider requests 均通過；全域 ETA／官方 quota 仍顯示未知。
這個頁面收據在有界批次進行時取得，不能當成無人值守 worker 持續存活的證明。

### 財報原始結果的欄名對應修復

重啟後的第一個八工作批次，完成兩張新來源軸清點、兩個有值下載與一個明確空回，
另有一個可驗證未送查詢的日期焦點錯誤短重試與一次 Key=3 正常重排。
`IFRS_TEJ Consolidated First(Qly)-ALL` 已取得來源 Preview，但 exact schema 比較不符而
等待三分鐘後停止。只讀診斷核對，來源在顯示欄名時將逗號改成一個空白：
例如清冊的 `Total property, plant and equipment` 對應 native header
`Total property  plant and equipment`。不是數值缺失，也不是「沒有財報資料」。

新增 `injective_literal_comma_to_space_preview_headers_v1` metadata 契約：

- 先要求 source binding、選取欄位完整順序、公司／期間及原 query attempt 的 Preview
  transition 均通過。每欄只能 exact 原名或 literal 逗號→空白，其他大小寫／標點／空白
  改寫、欄位交換及映射碰撞都拒絕；不是一般 fuzzy match。
- 來源 JSON 保留真正 native header、每一筆原始值與 Preview samples；樣本仍以真正
  native header 比對。只將訓練候選 Parquet 欄位身份對回清冊，數值、負值、零、稀疏
  及三鍵／單鍵粒度皆不改。新收據保存 native header 和明確 mapping contract，
  writer 也把契約寫入 Arrow metadata。
- 原未知下載直接 `recover-desktop --response preview` 採納既有結果，**沒有重送查詢**；
  取得 1,284 列／35,952 個非空值。原失敗、stage、診斷與費用不確定性證據保留，
  readback 工時不作新抓取速度樣本。剩下的公司×期間格點不能虛構資料補滿。
- 稽核升版 `tej_local_receipt_integrity_audit_v3`：驗證 source 原始欄名、收據 mapping
  contract 和 Parquet canonical 欄順；不是只檢查 file exists／HTTP 200。
  新增測試涵蓋 1／2 鍵映射、原值不變、碰撞／非字面映射拒絕及收據遭改寫時稽核失敗。

[批次後真實瀏覽器驗收](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/addin_restart_v1/rendered_after_batch/rendered_acceptance.json)
在 15:02 顯示「批次已結束 · 仍有工作需檢查」；那是 finite batch 結束，不是來源再次
停用或全歷史完成。該次[稽核](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/addin_restart_v1/final_audit/audit.json)
36 個已取得分片／13,803 匯出列／275,874 非空值，zero artifact／feature count mismatch，
三個 blocked metadata、沒有 blocked unknown download。仍有 208 張未清點來源表，
不可用 queue 的 materialized pending 數推算全域只剩多少分片。
同時凍結[255 表／45,826 欄位清冊](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/addin_restart_v1/final_inventory/manifest.json)，
這個清冊只公開 metadata，不傳原始值／金鑰／帳號到網頁。

15:04 又透過同一 canonical queue、`run --download-only --table-id <財報表> --max-tasks 1`
完成一個**新的**財報查詢分片；原查詢恢復與新查詢接受不是同一件事。
同樣取得 1,284 列／35,952 個非空值，使用新的 attempt／stage／原始檔／收據，
沒有先前因欄名不符而無效等待三分鐘的錯誤。這只證明該真實分片的新查詢路徑恢復，
不是全 provider 完整歷史或整體下載倍速的證明。

最新 [15:05 稽核](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/addin_restart_v1/fresh_financial_audit/audit.json)：
37 個已取得下載分片（含明確空回）／15,087 分片匯出列／311,826 原始非空值；
local artifact failures=0、feature count mismatches=0、兩份 header mapping 收據接受。
相較重啟前新增 6 個完成分片、3,007 匯出列與 80,283 非空值；不是新增 6 個完整資料庫。
仍有三個已知 metadata 失敗與 208 張未清點表，unknown download=0、running task=0。
有界驗收批次已結束，無人值守排程／全歷史完成／全域 ETA 仍未宣稱。
[最新清冊](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/addin_restart_v1/fresh_financial_inventory/manifest.json)
與[批次後桌機／手機真實頁面](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/addin_restart_v1/rendered_after_financial/rendered_acceptance.json)
已刷新；445 個 TEJ focused Python 測試、3 個 Node 測試與四個 PowerShell AST 檢查通過。

### 可重跑的有界操作

都須指定 exact task／私有證據，保留原工作簿與來源檔，不由定時服務自動做：

```bash
source scripts/runtime_env.sh
# 僅驗證共享介面穩定，未重送 Preview／未重設失敗資料工作。
run_fintech_python -m downloader.download_tej_history verify-interface --task-id <registered_task_id>
# 明確允許放棄這個 scratch 查詢設定才使用；不關閉工作簿或其他程式。
run_fintech_python -m downloader.download_tej_history reopen-query \
  --task-id <registered_task_id> --allow-discard-query-settings
# 格式重排只接受已拒絕／可驗證未提交、無已完成舊粒度資料的 scope。
run_fintech_python -m downloader.download_tej_history repair-key-layout --task-id <exact_task_id>
# 資料結果未知不能自動重送；operator 額外查詢可能再次計費／佔用流量。
run_fintech_python -m downloader.download_tej_history retry-unknown \
  --task-id <exact_unknown_download> --evidence <exact_original_private_request> \
  --acknowledge-unknown-usage
# 介面與收據接受後，按共用排程跑有限批次，不代表無人值守或全歷史完成。
run_fintech_python -m downloader.download_tej_history run --max-tasks 8
```

日期處理依[Microsoft SendWait 文件](https://learn.microsoft.com/en-us/dotnet/api/system.windows.forms.sendkeys.sendwait?view=windowsdesktop-10.0)，
不能把送鍵返回當成另一程序已處理。清除後確認真正空值，將已核對的空日期編輯器游標
置零；每字元只送一次並確認文字已變，八位數完全一致才 Tab。任何焦點或字元確認失敗
立即停下，私有診斷保留前後文字／caret；不把未送 Preview 的本機輸入失敗當來源空值。

## 網頁與進度口徑

[TEJ 下載監控](https://penguin72487.ddnsgeek.com/tej/)已整合到全來源網站導覽與 Provider 頁，
沿用 FinLab／FinMind 的共用樣式、公開唯讀 gateway 和更新排程。
逐欄使用伺服器搜尋／50 欄分頁；逐表搜尋與階段篩選，不一次渲染 45,826 列。
頁面隱藏時暫停輪詢；30 秒刷新、本機投影快取 20 秒，無重疊 status 請求。
同一欄位頁的未完成讀取會共用一個請求，新的搜尋／分頁取消舊請求，
舊回覆不能覆蓋新篩選；讀取完成前鎖定上下頁按鈕，失敗保留上次資料。
此取消只影響公開中繼資料 GET，不會取消或重送 TEJ 採集工作。

進度分開顯示：

- 實際取得列數、實際非空儲存格、逐欄最早／最新非空「查詢期間」、本機來源＋Parquet容量。
- 已清點表的公司／系列數、日期選單首末期間：另外標為選單範圍，不冒充原生觀測或發布日。
  舊範圍收據只在 writer 的安全工作邊界做一次有界本機中繼資料升級，面板不讀原始檔。
- 查詢範圍：來源有結果或明確空回的格點；空回不增加資料列數。
- 分片計數：一個公司／日期有多個不重疊欄位片時，匯出列數與工作格點都按片計；
  另保留唯一公司／日期格點數，不把分片列數說成唯一歷史觀測筆數。
  尚未 materialize 的 lazy 工作也算入剩餘工作，不能因 queue 只有少數 pending 就誤判完成。
- 尚未清點的表不填零，所以**嚴格已核實**的全域總列數、百分比與排定完成日仍為未知。
  schema v7 另外提供有明確外推依據的全量情境、預估工作進度及分階段依賴里程碑，
  與上述嚴格欄位分開；外推不是已證實原生歷史完整率。
- 舊有同表至少兩次完整工作的格點速度估算仍保留相容；新面板主要使用
  `tej_staged_query_scenarios_v1` 的每次完整查詢成本與近似範圍樣本（見下節）；
  不把恢復既有結果的讀回時間當成重新抓取速度，也不拿結果列數去除剩餘格點。
  正常樣本包含整個 request 展開、橋接、驗證、Parquet、收據與 SQLite 採納，
  另計設定的兩秒工作間隔；舊版僅測橋接的時間樣本已排除，不偽稱完整下載時間。
  不把候選欄位完成數當 bytes，不把稀疏格點目標當真實原生歷史筆數。
  新容量模型按「每次查詢的本機容量」估算，避免一、兩列的小試抓收據固定成本被乘成 TB。
  容量及結果列數以「已取得＋剩餘範圍預估」計算，不可能小於已取得；樣本偏稀疏、
  公司批次或歷史區間改變時仍會修正，不是承諾上界。
- 無法讀取、執行中、待抓、來源空回、schema 驗證失敗與結果不明分開；
  昨日 service active／HTTP 200 不證明已下載或已更新到最新。

## 2026-10-02 分階段估時與查詢分片優化

本節以 [15:43 臺北的固定快照](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/stage_eta_v1/final_inventory/public_status.json)
為依據，歷史 cutoff 仍是 **2026-10-01**；不冒充 10 月 2 日的新資料已全部取得。
清冊 255 表／45,826 欄；42 表已清點查詢軸，211 張非空表尚待核實。
全量計畫不是 queue 眼前的 44 筆 pending：**785,826 次已核實剩餘資料查詢**包含 lazy 工作。
完整下載時間樣本有 37 筆／21 張表，排除恢復舊結果與舊版 bridge-only 時間。

### 計量單位與推導

1. **剩餘工作量**是已清點 lazy 計畫＋目前 pending／running／blocked，排除 superseded。
   未知歷史軸以同 Type universe、同頻率／分類或跨表樣本的 q10／q50／q90 建模，
   並保留每表 `geometry_basis`／`timing_basis`。來源軸未知不等於零工作或已完成。
2. **時間**按查詢計，不用「實際匯出列數 ÷ 速度」：正常空回也要選取、請求與驗證。
   同表先取分位數，再在表之間平衡；已清點樣本優先使用相近工作格點量，
   避免反覆微型試抓支配大型查詢模型。同表至少兩個相近範圍樣本才用同表模型，否則外推。
   成本含 request 展開、來源、驗證、保存、收據／採納與兩秒間隔；
   尚未量測的 worker 啟動／計畫 refill、人工等待、修復與官方配額等待不冒充已知。
3. **資料筆數**由相近查詢樣本的 `SUM(actual_rows) / SUM(expected_rows)` 外推，
   不平均稀疏比例、不補零；**容量**用 `SUM(actual_bytes) / COUNT(query)`，
   本機原始匯出＋Parquet，不是網路 bytes。資料期初偏空、Key=3 一格可多筆、
   第一次清點會變更模型，因此筆數／容量不是保證上界。
4. **階段完成**是「最後一張所需共享表完成查詢 scope」的里程碑，
   不是逐欄最早 ready、原生歷史完整或訓練驗收。
   依現有 P1→P2→P3 owner 優先級、單一桌面與逐表輪轉計算；
   待清點成本保守前置，實際仍交錯清點。以閉式 round horizon 計算，不展開數百萬工作。
5. **實際排定日**與「如果現在開始連續跑的情境日」分開。
   目前只是有限批次，故 `scheduled_complete_at_utc=null`；即使單批正在執行，也不當作全程已排定。
   日期以臺北時區顯示並加「約」，不以年級距外推提供假精確的時分秒。

### 每階段預估

以下均假設單一桌面 **24 小時連續執行**，來源故障已修復、沒有配額／人工等待；
q10／q50／q90 是實測外推情境，不是統計信賴區間，實際可以落在範圍外。

| 範圍／依賴 | 較快工時 | 中間工時 | 較慢工時 | 中間情境完成日（臺北） |
| --- | ---: | ---: | ---: | --- |
| 僅剩餘 211 張歷史軸清點（含 3 失敗） | 0.80 小時 | 1.78 小時 | 3.32 小時 | 未排定；這些工時已包含於下面下載模型，不能再加一次 |
| 僅 42 張已清點表的剩餘查詢 | 167.39 天 | 228.93 天 | 286.31 天 | 不是全量完成日 |
| P1 新增候選：44,492 欄／253 張表 | 705.17 天 | 1,161.12 天 | 1,624.04 天 | 約 2029-12-06 |
| P2 歷史／口徑候選：811 欄／118 張共享表 | 703.95 天 | 1,159.41 天 | 1,621.94 天 | 約 2029-12-05 |
| P3 校驗候選資料：523 欄／52 張共享表 | 589.57 天 | 991.20 天 | 1,400.54 天 | 約 2029-06-19；**不是校驗完成日** |
| 真正跨來源逐鍵校驗 | 未量測 | 未量測 | 未量測 | 尚未排定；需先確立鍵、單位、歷史版本與權限 |

P2／P3 此清冊沒有獨立 owner 表，新增查詢為零，但不代表完成工時為零：
118／52 張表都併在 P1 取得，各自等待共享表的最後 scope。
所以 P3 候選資料可先於全部 P1 ready，不代表已提前開始或完成多源校驗。

中間情境全量剩餘約 **3,938,841 次查詢、2,423 萬分片列、53.74 GiB**；
較快／較慢容量情境約 12.36／334.02 GiB，差異反映尚未清點與稀疏樣本不確定性。
當次可用 53.18 GiB，保留 5 GiB 後剩 48.18 GiB，**中間情境也放不下**。
面板會提示容量限制；collector 依實際餘裕停止，不刪來源資料、不假定模型低估不會發生。
年級距的主要原因是目前整個公司 universe × 年代日期軸 × 欄位分片、單一桌面與大量空範圍，
不是 45,826 欄各送一次就抓完。尚未證實上市／下市／表版本的原生有效區間前，
不任意跳過舊年分；下一步應取得可驗證的有效範圍，再做另版不漏值的 scope 縮減。

### 已套用的最少均勻矩形查詢

沿用既有 Preview／collector，不增加另一條下載管線或其他 provider。
每片特徵最多 `30 - Key`；每片工作 row bound
`R=min(10000, floor(200000/(features+Key))-1)`。
對最多 32 個公司批次大小 `s`，計算完整均勻矩形切分：

```text
Q(s) = floor(C/s) × ceil(D/floor(R/s))
     + [tail>0] × ceil(D/floor(R/tail)), tail=C mod s
選擇 Q 最小的 s；同數量才選較大的批次。
```

這是**有界均勻矩形切分的最少查詢**，不是任意不規則 packing 的全域理論極限。
部分已完成 scope 有洞時，保留精確覆蓋、只接受總剩餘查詢減少的計畫；
Key=1 仍採最大公司批次，沒有歷史日期維度。
Key=3 的工作格點不是原生事件列數上限，仍必須驗證真實輸出，不丟第三鍵或截斷超量資料。
例如 7,366 公司、223 季格點、R=6,665，由 32 公司拆為 29 公司，
完整欄位片約 462→254 次；不提高 Preview／rows／cells 上限。

本次私有審計 `data_tej/planning_migrations/ec89e1f848424757b4a626d3fb5d8a38/audit.json`
記錄 **15 張表、少 34,945 次桌面查詢**，不是已證實的 HTTP 次數／實際流量節省。
每張舊 SQL 計畫都保存帶 SHA 的原樣備份；舊 pending 標記 superseded，完成範圍／來源檔／收據不改。
執行中、結果不明、blocked data 與錯誤 fingerprint 都是 hard barrier。
Future DB 三個舊版 native 小範圍 pending 經範圍驗證後保留，該表暫不重排；
不是把 pending 當完成。`query_tiling_deferred_tables` 記錄它，完成後可重跑同一升級。
首次升級因 transient lock 未取得而停止；未強制 unlock 或刪 lock，之後正常取得 canonical lock。
第二次 scope barrier 也未更改計畫，局部備份保留；只有最後 committed audit 是生效證明。

```bash
source scripts/runtime_env.sh
run_fintech_python -m downloader.download_tej_history upgrade-query-tiling
# 有界續跑；整批實際狀態另記，這不是無人值守全程排程。
run_fintech_python -m downloader.download_tej_history run --max-tasks 20
```

### 接受證據與效能邊界

- 同一 canonical bridge 實機送一次新分片：29 公司 × 15 個**尚未完成**季格點／28 特徵，
  23.87 秒完整採納、來源明確空回；不是新增數值，也不當作所有大批次的吞吐量證明。
  38 份完成下載的 [全部本機稽核](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/stage_eta_v1/optimized_query_audit/audit.json)
  保留 15,087 分片列／311,826 非空值，零檔案失敗／欄位計數差異。
- [全表／逐欄狀態與 ETA 固定快照](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/stage_eta_v1/final_inventory/manifest.json)
  是 report contract v2／public schema v7，附 `eta_input_sha256`，不是原始資料公開或全歷史完成。
- [公開桌機／手機驗收](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/stage_eta_v1/rendered_handoff/rendered_acceptance.json)
  檢查三階段、非零共享依賴工時、嚴格未知分母、預估進度、未排定的真實完成日、
  逐欄搜尋／分頁；零 JS error／外部 provider 請求／document overflow。
  有整段估時與階段表的實際截圖；只是 HTML／2D 驗收，不宣稱 GPU 或全量來源驗證。
  實際截圖另發現手機的三個情境被共用兩欄 grid 錯排；改以同一 value stack 放在第二欄，
  次要說明／進度亦置於第二欄，驗證三行同 x、按 y 遞增、右端不超出 viewport。
- 全部 255 表／45,826 欄投影，十次未同時執行本輪大組 regression 的樣本暖機中位 **76.63 ms**、P95 **86.17 ms**；
  [同時執行 regression 的樣本](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/stage_eta_v1/metadata_projection_benchmark_v2.json)
  中位 200.29 ms／P95 269.91 ms，負載差異不能冒充優化前後速度比例。
  透過 SQL authorizer 禁讀大型 plan JSON，audit guard 禁網路及原始／datasets 檔；
  不展開 393 萬工作、無來源呼叫，20 秒 metadata cache／30 秒頁面刷新仍保留。
- 相關 TEJ、ETA、migration、Key=1/3、收據、desktop lifecycle、gateway 與 benchmark
  focused/regression **1,193 項通過**，另有 6 項 Node 前端語義測試（Python gateway 測試亦呼叫這些）。
  升級版本、原樣備份、含洞範圍不重複／不遺漏、native pending 延後、錯誤 fingerprint、
  finite batch 非持續排程與 unknown 非完成均有語義測試。

本輪只重新載入既有公開唯讀 gateway，以及一次 canonical TEJ 測試批次；
沒有重新啟動其他 provider collector／交易服務，沒有降低 Office 安全政策、讀取金鑰或刪除來源。
批次於 **15:42:38（臺北）**正常結束，**目前不是全程持續下載**。
三張選擇權來源選單故障仍待另行修復，不把它們改成空資料、成功或權限不足。
最後巡檢另見 Future DB 原有 native probe 的 **15:49:32** 本機日期輸入短重試標記，
沒有存活下載者／持有 TEJ dataset lock；該筆仍 pending、未當作空回或完成。
「五秒後可重試」只是工作就緒時間，不代表五秒後確實有 worker 會執行。
本輪新增的估時將這種情形顯示為未執行、實際完成日未排定；不修改或刪除該原有工作。
交接時的動態狀態另見
[交接快照](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/stage_eta_v1/handoff_inventory/public_status.json)，
與前述固定計算快照的內容 SHA 可不同，不能把不同時間的 worker 狀態拼成同一 snapshot。

## 常駐自動下載（2026-10-02，automatic_v1）

### 執行計畫與責任邊界

本輪把既有 canonical queue 接到常駐 supervisor，沒有再建立一份下載清單或第二個 collector。
保留原本 P1 新特徵 → P2 歷史／口徑缺口 → P3 多源候選的分工與來源清點／下載交錯。
同一個桌面查詢只能有一個持有者；P3 的來源取得不冒充逐鍵多源校驗已完成。

| 層級 | 實作與行為 |
| --- | --- |
| 常駐服務 | `stockagent-tej-history.service`，已安裝及啟用；不是 `run --max-tasks` 的有限批次 |
| 排程 | [tej_scheduler.py](../downloader/tej_scheduler.py)，沿用 `run_one`；整體 owner lock 與每工作 dataset lock 分開 |
| 桌面取得 | 同一 [Windows bridge](../scripts/tej_smart_wizard_bridge.ps1) 與私有精確工作簿／查詢視窗 pins；不選另一帳號或新增登入 |
| 可續跑 | 已完成 scope 不重送；重啟僅可採納同一 attempt 的完整、本機、驗證通過回覆；未知 Preview 不自動重試 |
| 等待 | 工作間隔最少 2 秒；桌面／磁碟／介面恢復通常 60 秒檢查、30 秒 heartbeat，不空轉也不靠面板呼叫來源 |
| 關機 | SIGTERM 完成當前有界工作再退出；不能把未完成查詢自動改回 pending |
| 面板 | [公開 TEJ 頁](https://penguin72487.ddnsgeek.com/tej/)，public schema v8：排程存活、查詢中、安全等待與停止分開顯示 |

CLI `watch` 不接受 operator replay／重啟／錯誤視窗等旗標。服務重啟不抹掉 local-input、
storage、source validation、Key=3／期間重排或 unknown 的 durable barrier。
私有 PID/start-ticks、query deadline 與 heartbeat 分別驗證；heartbeat 不延長資料查詢期限。
`waiting_desktop`／`waiting_storage`／待到期工作不再被 ETA 標成持續下載；真正完成日期仍未排定。

### 已修正的根因

1. **有限批次結束後沒有 worker。** 新增常駐 supervise 既有 queue，失敗後有界退避，
   `Restart=on-failure` 不代表允許未知資料查詢 replay；同時保留手動 CLI 和 mutex。
2. **日期的畫面文字與內部值不等價。** 曾測試 native display text 路徑，但實機未更新
   舊式日期模型，因此撤回該做法。v5 使用正確 owner 的正常鍵盤輸入、每字元／caret 確認、
   Tab commit 及完整日期讀回；不靠已相同的畫面文字跳過輸入。
   初始 `____/__/__` 是合法空遮罩，不再誤判為日期框消失；其他空白 textbox 仍不能冒充目標。
3. **Excel 作用中視窗誤判。** `GetActiveObject` 當時回報 Book1／401110，但原查詢仍綁定
   Book2／17637508。正常取得改核對指定 workbook 的唯一 `Window.Hwnd`，不要求使用者一直
   保持 `ActiveWorkbook=Book2`，也不自動 Activate／關閉工作簿。
   [官方 ActiveWorkbook 定義](https://learn.microsoft.com/en-us/office/vba/api/excel.application.activeworkbook)
   是作用中的工作簿，不是下載來源身份；
   [Window.Hwnd](https://learn.microsoft.com/en-us/office/vba/api/excel.window.hwnd) 用於核對指定視窗。
   指定視窗真的不可用時，精確 before-Preview 證據讓工作等待桌面，不誤判為已送查詢或消耗兩次程式錯誤重試額度。
4. **空 scope 阻擋 Key=3 修復。** Directors Holding(Yearly) 的來源格式確實是三鍵，
   舊式兩鍵計畫已存在可驗證的完整空回。只在 raw SHA／receipt／完整原欄位範圍證明一致時，
   用空 scope 覆蓋完整的新三鍵欄位片；本次恢復 3,584 工作格點，不重送已證明空的範圍。
   非空兩鍵資料不能捏造第三鍵；Key=1 snapshot 也不能沿用有日期空 scope 作歷史覆蓋。
5. **來源 YYYYMM 被當成 daily Date。** 新增 `literal_source_yyyymm_period_key_v1`：
   字面 YYYYMM 只表示月鍵，不代表發布日或已證實的觀測頻率。新 Parquet／receipt 有明確
   contract 及獨立版本子路徑，舊 ABI 不被全域改寫。Daily→Monthly 只透過實際 Monthly
   radio 的狀態讀回做 metadata-only 重排；原 source／receipt／完成範圍保留。
   Fund's Turnover 的剩餘計畫查詢 **11,852→535，減少約 95.49%**，不是實測速度倍數、
   HTTP 流量節省或所有表都可用 Monthly。已保存的 16 列原回覆透過精確 interpretation
   sidecar 採納，沒有再送 Preview。
6. **未知通知沒有私有診斷。** 新版在拒絕未知通知時，有界保存 exact-owner 的
   Static／Button caption，沒有讀取 Edit 值、關閉未知視窗或確認配額／登入提示。
   年價格工作先前的 transient 通知原因仍未證實；後來只讀同一次已完成 Preview 的
   226 列，不重送未知查詢。不能因恢復成功就聲稱從未失敗。

日期輸入的兩次 Windows fixture 曾因字元確認失敗而停止；失敗檔保留。
最後 [v5 owned fixture](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/automatic_v1/fixture-v5-diagnostic-86d22fea03d84439afca920997c8b6d3.json)
**36 項通過**，含空遮罩、正確日期／焦點、唯讀與 foreign 控制拒絕、精確原生清單查找、
背景 Preview 一次正常 action 與新結果 signature。它只操作自建表單，不接 TEJ／Excel，
也不替代實際來源下載證據。輸入仍需要互動桌面及正確 foreground；不是 headless，
不保證操作桌面／遊戲時完全不受影響。Preview 本身可透過 owned MSAA action，不需滑鼠注入。

### 實際接受證據

**17:52:47（臺北）**重新啟動服務後，連續完成財報與月價格兩次非空取得：
IFRS_TEJ Parent Financial_Security(Acc)-4 **527 列**（查詢格點 6,467）與
TSE/OTC Adjusted_Price(Monthly)-Ex_R+D **77 列**。不是把格點數當作已下載筆數。
資料與收據已落到既有 `data_tej`；原失敗 attempt／scope 收據沒有覆蓋。

**17:54:34（臺北）**的
[完整本機收據稽核](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/automatic_v1/receipt_audit_handoff_v1/audit.json)
驗證 **48 份新版完成下載、15,994 列／336,791 個非空值**；
raw／Parquet SHA、列數、單位／keys、期間解釋、attempt 綁定與 feature 計數差異皆為零。
快照後的新增收據不在這份稽核內；舊版 legacy 範圍不混入新版完成率。
仍有 4 張 metadata 故障表及當時 204 張 pending discovery，不把它們標為已取得或正常空回：
Bond Indicators Yield-Government、三張原選擇權表。
**17:58** 又實際發現 Delisted Option Attribute(2010~2013) 的正常 binding event 未完成。
canonical metadata-only 隔離保留原通知／診斷，不採納軸也不重送 Preview；
因此此時間點是 **5 張 metadata 待修表**，並非前述 17:54 稽核發生計數錯誤。
服務隨後完成下一資料工作並繼續財報取得，來源選單失敗沒有卡住全部可用表。

**18:00～18:10** 的銀行表另揭露 `Data YYMM` 月鍵，不是普通 Date。
新增獨立 `literal_source_data_yymm_period_key_v1`，不將原 `YYYYMM` ABI 偷改成泛用日期規則，
也不依 `YYMM` 字樣猜兩位數年份；只有精確欄名、四位數年、來源 key 範圍／唯一性及
正常 Monthly radio 讀回均通過才生效。T13-3 Fin. Structure -Bank 原回覆 **192 列**
以 exact attempt／raw SHA sidecar 恢復，未重送 Preview；剩餘計畫查詢 **1,101→52**。
舊已採納的日鍵 Parquet／receipt 保持原樣，只另做有證據的 planning-only 月覆蓋解釋。
相鄰日片的部分月份重疊，以壓縮月份 interval 聯集為完整欄位片／公司分配新增權重，
恢復 **1,024 格點**，不重複計數，不建立全歷史稠密笛卡兒積，也不平均／刪除舊來源值。
曾有一次 Monthly 讀回缺少新的精確 contract 證明而被阻擋；修正 guard 並重新做
metadata-only 讀回後才提交重排，不把缺少的證明改成 true。

**18:10:25（臺北）**的
[更新版全收據稽核](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/automatic_v1/receipt_audit_handoff_v2/audit.json)
驗證 **51 份新版完成下載、16,186 列／340,055 個非空值**，
零 artifact／source-key 驗證失敗、零欄位計數差異；包含 3 份明確版本的月鍵回覆。
服務重新啟動後又連續完成兩個工作，並接續另一張監理資料表；不是僅 `active` 的存活證明。
最新 [執行驗收快照](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/automatic_v1/runtime_acceptance_handoff_v2.json)
另核對服務已 enabled、真實 worker 存活且 `public_state=running`，本次啟動已完成 **6 個工作**。
17:52 起累計完成 10 個工作、增加 **827 分片列**；其中包含前述 192 列 saved-response recovery，
不能把全部完成工作都說成重送的新查詢。先前等待恢復的 runtime_v1 快照原樣保留。

[逐表／45,826 欄固定快照](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/automatic_v1/inventory_handoff_v2/manifest.json)
與 [桌機／手機驗收](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/automatic_v1/rendered_handoff_v2/rendered_acceptance.json)
另外驗證唯讀投影、三階段 ETA、搜尋／分頁、等待與存活的區分；
不是公開原始授權資料、全歷史完成或實際固定完成日。頁面不向 provider 發請求。

最終 TEJ 與 public gateway 全套 **1,278 項通過（37.00 秒）**；
面板等待狀態修正另有 **603 項 focused regression 通過**，bash／JavaScript／systemd unit 語法檢查通過。
TEJ metadata 投影 [十次有界量測](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/automatic_v1/metadata_projection_benchmark.json)
暖機中位 178.08 ms／P95 213.31 ms（同時有 regression 負載），
禁讀大型 plan JSON、raw／datasets，無來源呼叫；不把它與不同負載舊樣本比較成速度提升倍數。

### 啟停、資源與仍需處理的限制

```bash
# 首次安裝；桌面 session 必須已登入並精確核對。
bash scripts/install_tej_history_service.sh --start
systemctl status stockagent-tej-history.service --no-pager
journalctl -u stockagent-tej-history.service -n 30 --no-pager
# 有界 graceful stop；重啟不會解除未知 Preview 的阻擋。
systemctl stop stockagent-tej-history.service
systemctl start stockagent-tej-history.service
```

服務採 `Nice=10`、CPU／IO weight 20、BLAS 一執行緒；**沒有 CPUQuota 硬限速**。
WSL worker 記憶體／CPU 是服務自身，不包含 Windows TEJAddin／PowerShell，不能用它宣稱全機低占用。
idle 用 event 等待，metadata cache 20 秒、頁面刷新 30 秒，隱藏分頁暫停輪詢。
桌面仍單一序列；numeric 官方 Smart Wizard 日配額／速率上限未知，顯示未知，不當作無限或 10 req/s。

本次磁碟僅約 **51.1 GiB 可用**，持續保留至少 5 GiB；早期中間容量外推接近餘裕，
18:10 的新稀疏樣本使它下降到約 38.3 GiB，正好說明少樣本外推的不確定性，並非刪資料省下的容量。
低餘裕時停止新取得、留下 truthful waiting_storage，不自動刪除任何來源。
queue 仍約 4.4 GB，保留既有 superseded 歷史／receipt；本輪沒有以優化名義 GC 或砍資料。

**這輪是既有截止 2026-10-01 歷史清單的常駐回補。** 全表原生最早／最新歷史、數值原始精度、
所有單位及 Transformation、發布／PIT、5 張來源 metadata 故障、全量容量、逐鍵多源校驗，
以及將未來日期自動追加到完整來源計畫的增量追新仍未完成；不是全部 255 表已可用於訓練。
沒有啟用 raw 跨主機發布或遠端訓練注入，也沒有自動修改登入／授權／Office 安全設定。

## 22:54 再次檢查與容量／本機狀態恢復（runtime_capacity_v1）

本輪開始時 systemd 雖為 active，最後完成卻停在 **18:35**；18:36 起為
`waiting_recovery`。原因是 TDR Qfii Broker Trading 的第三鍵讓同公司／日期有多筆記錄，
32 公司 × 312 日的 9,984 個查詢格點不能當作預期來源列數。Preview 已送出、回傳超過
10,000 列／200,000 格的本機上限；既有擷取例外被歸為未知結果，因而安全停止。
本次沒有把服務 active 當作取得成功，也沒有直接清除未知結果 barrier。

修復沿用既有序列下載器、lazy plan、attempt 收據與桌面 lock，沒有另一套下載框架：

- **精確已知容量超限可自動重排。** Windows bridge 有界核對同一新 Preview 的原生列下界、
  欄數與 signature，只產生不含數值的 overflow descriptor。controller 重驗 exact attempt／
  prepared request／stage／原計畫 fingerprint，保留原回覆及計畫，再對尚未取得的 scope
  增加密度估計、縮小查詢片。計畫與 task identity 更新；不把超限算成完成或從未提交。
  `auto_preview_capacity_replanning=true` 僅適用已證實的三鍵超限，並非未知查詢自動重試。
  密度估計不是觀測筆數或容量保證；單一公司／期間仍超限則留 `source_capacity_requires_review`。
- **SQLite contention 不遮蔽較強的回覆證據。** 新增 scope／kind／state／error covering index，
  不讀 59 萬筆 superseded request blobs。遇到 SQLITE_BUSY／LOCKED，先保留本機 annotation，
  五秒後檢查 exact saved response 或有時間界限的 before-Preview negative proof；
  資料庫損毀不走 busy retry。重啟不重送未知 Preview、不重設原兩次程式錯誤預算。
  [實際大型 queue 量測](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/runtime_capacity_v1/count_index_acceptance.json)
  三次計數讀取交易為 **86.9／106.5／103.0 ms**，確實使用 covering index；不是外部下載加速倍數。
- **重排保留舊空 scope。** Directors Holding(Yearly) 容量重排曾因舊 Key=2 完成 request
  不符合新 Key=3 plan 而停在 `source_validation_failed`。現在共用既有 key-layout repair 的
  explicit-empty 驗證，要求原始 SHA／receipt／實際零列／完整欄位與範圍一致，保留原收據，
  只繼承 planning-only 空回範圍證明與 **3,584 個工作格點**。非空舊資料不捏造第三鍵。
  隨後又一次實際容量超限已正常重排，密度估計由 17 修正到 42；不是來源資料已完成。
- **桌面不可用與輸入故障分開。** 只將明確發生在任何 SendWait 之前的 exact-focus
  availability marker 視為等待桌面，60 秒後再檢查，不消耗程式錯誤預算。
  字元輸入未被確認仍立即停止，保留兩次有限預算，不盲目補送字元、換視窗或繞過焦點檢查。
  面板 schema 9 另明示 `waiting_metadata`／容量需檢查，不將其顯示為完成。

### 真實下載與全收據接受

TDR 修復後兩次較小的真實查詢取得 **4,546＋4,607＝9,153 分片列、82,377 個非空值**，
原始 JSON、Parquet、receipt 均已寫入；讀回既有超限 descriptor 的恢復本身沒有再送查詢，
後續較小的新查詢仍按正常來源使用量記錄。

**22:51:45（臺北）**再啟動 `stockagent-tej-history.service`，到 **22:53:40** 已連續完成
四項工作，其中一項為來源清點，三項為新的非空資料下載：

| 來源表 | 新分片列數 | 完成時間（臺北） |
| --- | ---: | --- |
| TSE/OTC Adjusted_Price(Yearly)-Ex_R+D | 306 | 22:52:11 |
| Event_Daily | 312 | 22:53:05 |
| IFRS_TEJ Parent Financial_Security(Acc)-4 | 527 | 22:53:40 |

最後觀察仍為單一 canonical worker 執行下一工作，沒有 paused reason；不是只看 PID 或 HTTP 200。
**22:56:00** 的後續觀察已連續完成八項工作，下一項為 T13-3 Fin. Structure -Bank；
同一個服務 PID 沒有重新啟動，仍無 paused reason。這是有限觀察期的持續取得證據，
不是長期無故障承諾；以下全收據統計仍固定在各自明示的 SQL snapshot 時間。
**22:57:35** 再核對已連續完成十項、仍在執行下一項、無 paused reason；
公開 HTTPS 狀態頁亦讀到 schema 9、worker alive／running，未呼叫來源補畫面。
**22:54:39** 的
[固定 SQL snapshot 全收據稽核](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/runtime_capacity_v1/receipt_audit_v3/audit.json)
核對 **97 個完成下載、41,536 分片列、700,571 個非空值**；原始／Parquet 雜湊、來源 schema／鍵、
列數與 feature 計數差異皆為零。這些列按欄位片計，不能直接當作不同公司／日期的唯一筆數；
快照後的完成項不在本次稽核內。71 份收據有 attempt SHA 綁定，26 份保留較早真實契約。

[全 255 表／45,826 欄狀態快照](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/runtime_capacity_v1/acquisition_snapshot_v3/manifest.json)
與[桌機／手機唯讀驗收](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/runtime_capacity_v1/rendered_v2/rendered_acceptance.json)
分別核對固定完整清冊及公開 schema 9／搜尋／分頁／篩選；不公開原始授權值、不呼叫 provider 補畫面。
本輪 TEJ Python 回歸 **1,248 項通過（15.79 秒）**，公開 gateway 回歸另 **111 項通過
（27.49 秒）**，Node 前端 **7 項通過**。

Windows fixture **v2 的 39 項通過**，包含真實編譯 C# 的原生容量下界測試；
[v3 失敗證據](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/runtime_capacity_v1/windows_fixture_v3.json)
仍保留：共享桌面上第一個日期字元沒有確認，立即停止，沒有送市場查詢。
不能將 fixture v3 說成通過，或由幾次真實下載推論 UI 永不失焦。現行輸入仍需要互動桌面，
尚不能保證鎖屏／操作其他程式時持續 headless 下載。

此快照仍有 **7 張 metadata 故障表、190 個待清點表**。被隔離的是來源選單準備／分配錯誤，
不是已證明原始值錯誤或無權限；其他可用表繼續抓取：Bond Indicators Yield-Government、
Delisted Option Attribute(2010~2013)、Delisted Option Attribute(2024~2025)、
Delisted Option Attribute(before 2009)、Delisted Option(2010~2013)、Delisted Option(2022~2023)、
Public Consolidated Long-Term Investment Detail。它們沒有算成完成。

本輪沒有刪 source／receipt／舊 queue、沒有啟用冷庫或訓練注入。仍為既有截止
**2026-10-01** 的歷史回補；未來日期追加、全歷史、全表原始數值精度／單位／發布時點、
逐鍵多源校驗及官方桌面配額仍未完成或未核實，不能宣稱所有資料已可訓練或無限流量。

## 23:23 日期輸入移除前景焦點依賴（runtime_focus_v1）

**日期輸入的前景依賴已移除，整體下載狀態仍須另外檢查。** 舊 v5 用正常 SetFocus／
全域 SendKeys／Tab 提交；即使字元前後都檢查 foreground，檢查與送鍵之間仍可能被使用者
切換程式，且不能保證 SendKeys 的 active-window 路由不變。增加延遲、無限重搶前景
或把失焦算完成都不能消除這個競態。

本輪實作 `owned_edit_messages_acknowledged_date_model_no_foreground_v6`，沿用同一個
collector、桌面 mutex、scratch query、prepare／stage／receipt。日期共用入口現在：

1. 驗證精確 PID／thread／HWND／Date Setting 父群組、可見／可用、非 read-only、
   WinForms EDIT 類型、沒有來源通知及真正有效的八位日期。
2. 對這個日期控制項送一次 EM_SETSEL／WM_CLEAR，確認原遮罩真的清空；
   不是只以 WM_SETTEXT 改外觀，也不動剪貼簿。
3. 每個字元只向同一個 HWND 發送一次有界 WM_KEYDOWN／WM_CHAR／WM_KEYUP，
   每步重驗 owner／群組狀態及文字或 caret 確認，最後八位日期必須完全相同。
4. 正式日期路徑不呼叫 Activate／SetFocus／AttachThreadInput／SendKeys，不依賴 foreground，
   不在失敗後補送另一種輸入方法。後續 Search、完整公司／日期／欄位選取、來源 binding、
   fresh Preview 與 Parquet／receipt 驗證都保留。

[Microsoft 的字元訊息定義](https://learn.microsoft.com/en-us/windows/win32/inputdev/wm-char)
與[WinForms MaskedTextBox 官方實作](https://github.com/dotnet/winforms/blob/main/src/System.Windows.Forms/System/Windows/Forms/Controls/TextBox/MaskedTextBox.cs)
提供訊息／遮罩事件的依據；但是否真的更新 TEJ 日期模型，以以下實機測試為準。
`FocusDate` 仍供明確 operator 的焦點診斷，不再由正式日期輸入呼叫；
來源錯誤對話框或 TEJ 元件自己的 MSAA default action 仍可能啟用視窗，不能承諾整個產品
完全沒有 UI 或可跨鎖屏、休眠、桌面 session 中斷運行。

### 背景輸入與真正查詢的接受證據

- 自建 Windows 表單的完整驗收三次各 **46 項通過**，耗時 **0.949／0.929／0.649 秒**：
  [第一次完整通過](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/runtime_focus_v1/windows_full_v6_2.json)、
  [第二次](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/runtime_focus_v1/windows_full_v6_3.json)、
  [第三次](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/runtime_focus_v1/windows_full_v6_4.json)。
  包括 owner 真正在背景、起訖日期互不修改、空遮罩、途中切換焦點仍完成、前景輸入框
  未被修改、群組途中停用則在下一字元前停止、readonly／foreign／無效日期拒絕，以及
  單次 Preview 能力／容量界限。沒有連 TEJ 或送來源資料查詢。
  最後與目前完整 bridge bytes 對應的
  [最終 fixture](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/runtime_focus_v1/windows_full_v6_final.json)
  另 **46 項通過（0.937 秒）**，包括最終 PowerShell 解析與 C# helper 編譯。
- 第一輪 full fixture 尚在舊的**測試前景啟用**步驟失敗，未進入新日期輸入；
  [失敗檔](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/runtime_focus_v1/windows_full_v6_1.json)
  原樣保留。之後把測試本身也改成觀察實際背景，而非先強搶前景才測「不需前景」。
  不是反覆嘗試舊方法直到通過，也不把先前失敗改成成功。
- 真實 TEJ 起訖日期都觀察到「原值 → 空遮罩 → 原日期」，沒送 Preview。
  再將查詢改到 2015 年，Search 實際回傳該來源 **201502～201512 的 11 個月格點**；
  還原 1995-06-30～2024-02-29 後，原 **344 個月格點**恢復。
  這驗證內部日期模型，不只是畫面相同；11 個格點仍受既有月份選單下界行為影響，
  不冒充完整 2015 年或任何原生資料／發布日。
- **23:19:10（臺北）**一個既有年價格 pending 分片用 v6 正式流程完成 **375 列**，
  task `9c566aa2b60f1c91b9ec6d94` 的原始／Parquet／receipt 落在原 `data_tej`，
  stage 明確記錄 `date_input_requires_foreground=false`、`global_keyboard_input_sent=false`。
- 隨後同 task 的正式 `verify-input` v2 私有接受收據，SHA 綁定原 probe 回覆並驗證兩個
  日期實際編輯、來源／欄位／axes／queue 不變；觀察為
  `date_input_started_in_background=true`、`date_input_foreground_unchanged=true`。
  不送新 Preview、不重設其他未知結果或採納任何新來源值。
  私有收據位於 `data_tej/input_acceptance/9c566aa2b60f1c91b9ec6d94-82fd0d01d43941bdb409ee7cf364dfde.json`。

v6 正式請求與新 stage 保存新的 input contract；v5 prepared request 加入明確 legacy
白名單，原 request／stage／receipt SHA 及原來源值 ABI v4 均不改寫。
operator 輸入接受報告升級 v2，缺少或矛盾的「無全域鍵盤／不需前景」證明會拒絕。
本輪 TEJ Python 回歸 **1,251 項通過（21.83 秒）**，公開 gateway 回歸另 **111 項通過
（26.22 秒）**。沒有 GPU／訓練／交易動作、沒有更動 Office 或登入／授權設定。

### 整體服務狀態與仍存在的另一個問題

23:23 已將 `stockagent-tej-history.service` 啟動到 v6。**它仍保留
`source_validation_failed` 的恢復等待，不等於持續全速下載。** 本輪開始前 **22:58:08** 的
Public Security Regulatory Capital Adequacy 回覆含 **2024-03** 來源月份，而該工作的
選取範圍只到 **2024-02**，因此沒有採納，也沒有為了測焦點放寬範圍驗證、裁掉來源列、
重送該查詢或把 task 改完成。這不是失焦，也尚不能判定 TEJ 原始資料錯誤；
需要另外核實該表的月份／發布期間對應規則。

[23:21:18 固定全收據稽核](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/runtime_focus_v1/receipt_audit_v1/audit.json)
核對 **103 個完成下載、46,904 分片列、817,637 個非空值**，原始／Parquet／來源鍵驗證及
feature 計數差異皆為零。未採納的上述回覆不算完成；七張 metadata 故障表仍隔離。
此稽核時有限測試批次已結束，worker alive=false 是真實當時狀態，不把稍後 systemd
active 或背景輸入成功當作全歷史完成。

## 2026-10-03 下載進度與網頁連動（web_progress_v1）

[TEJ 公開頁面](https://penguin72487.ddnsgeek.com/tej/) 現在每 **5 秒**同步現有
collector 的工作狀態，不靠新增來源查詢填補畫面。原先只有批次／累計收據，缺少
目前查詢表與讀回步驟；服務重啟後的本批完成數也不能代表全部已下載量。
本輪沿用原 queue、attempt、receipt 與唯讀 gateway，沒有另一套下載器。

### 顯示與計算契約

- 公開 status schema **10**、`tej_download_activity_v1`：目前表、階段、步驟、
  已耗秒數、累計完成下載／清點、持久化最後成功時間，以及有界的等待工作清單。
  最新成功取已完成 queue 記錄，不取排程 heartbeat，不因重啟歸零。
- 原生 bridge 寫 `tej_native_readback_progress_v1` 私有 sidecar，僅含精確
  task／attempt、時間、步驟與已掃／總列槽；沒有來源儲存格、帳號或金鑰。
  實際讀回以兩秒節流發布 metadata，不逐格寫檔，原子替換；來源查詢與日期輸入仍沿用 v6。
  Windows UNC 實測最初的 File.Replace 未能更新，改用已驗證的
  `MoveFileExW(REPLACE_EXISTING)`；失敗 fixture 保留，不冒充第一次就成功。
- 網頁的「讀回來源列槽」包含可能的空列，**尚未驗證入庫**；讀回 100% 不是
  下載完成。只有正式驗證並寫入資料／收據後，已入庫分片列、非空值及容量才增加。
  明確來源空回可完成一項查詢，但不會增加有值資料筆數。
- 筆數及容量進度條的分母是「已入庫實際量＋中間情境剩餘估計」，不是完成表數。
  分片列不是跨欄位去重後的經濟觀測；容量是本機 receipt 記錄，不是 HTTP 流量。
  未核實的分母維持未知；P1／P2／P3 的較快／中間／較慢 ETA 是條件估算，
  不將故障等待或未核實官方配額換算成保證完成日期。
- worker 必須仍有有效 owner／deadline、task 與 attempt 都相符才顯示活躍工作。
  telemetry 不延長 query deadline、不驅動 retry、不改採納規則。
  較舊步驟會標示；過期 status 或 HTTP 失敗不繼續宣稱正在下載。
- gateway status 快取 **4 秒**，失敗不回放 stale status；頁面每 5 秒原位更新，
  背景分頁暫停、切回立即同步。欄位細節只在完成修訂變更或 30 秒到期時刷新，
  不阻塞 status，保留搜尋／分頁／焦點，不讀來源值檔或巨大 plan JSON。

### 驗收與執行狀態

- TEJ 與共用 gateway 回歸 **1,378 項通過（47.72 秒）**；包含真正
  DesktopBridge → 公開 status → 正式 commit 的隔離流程測試，以及 telemetry
  損壞不影響採納驗證、不觸發新增 query 的檢查。前端協調測試 **10 項通過**，
  已含於 gateway 回歸，不另加總。
- [Windows native fixture](../artifacts/data_quality/tej_web_progress_2026-10-03/windows_native_v3.json)
  **52 項通過（1.046 秒）**，實際編譯 helper、原生表格讀回、UNC 原子替換、
  無來源值曝露／額外 Preview／telemetry 搶焦點；只用自建表單，沒有連接 TEJ。
- [第一次公開瀏覽器驗收](../artifacts/data_quality/tej_web_progress_2026-10-03/public_browser_v1/rendered_acceptance.json)
  00:35（臺北）確認桌機 1440px／手機 390px 無 overflow／JS error，搜尋、分頁、
  分階段估算及等待原因可讀。隔離 HTTP fixture 驗證 5 秒自動更新的
  等回覆 → 讀回 → 入庫 → HTTP 失敗 → 恢復等待，5 次 status 僅 2 次欄位請求。
  fixture 不改 production queue，不代表真的下載了測試列。
- [完整 metadata 測量](../artifacts/data_quality/tej_web_progress_2026-10-03/metadata_benchmark_v1.json)
  255 表／45,826 欄，warm p50 **0.136 秒**；沒有來源查詢、原始值檔或 plan blob
  讀取。這不是實際下載加速、全系統 CPU 或 GPU 效能證明。
- 00:33:53 只重啟既有 TEJ supervisor 與公開 gateway。00:35 的實際自動排程仍為
  `waiting_recovery/source_validation_failed`，103 個完成下載、46,904 分片列、
  817,637 非空值、38,172,138 bytes；本輪沒有為了顯示進度重送失敗查詢。
  00:38 另觀察到既有 Future DB 有限工作，公開 status 顯示實際 preparing_scope；
  00:38:34 明確來源空回完成後，00:39 status 已變為 **104 個完成下載**，
  最後成功表／時間及容量 38,179,118 bytes 隨之更新，資料列與非空值不增加。
  [00:41 第二次公開瀏覽器驗收](../artifacts/data_quality/tej_web_progress_2026-10-03/public_browser_v2/rendered_acceptance.json)
  確認桌機／手機都呈現 104 及新的最後成功時間；同一組自動刷新隔離測試仍通過。
  自動排程仍待來源驗證修復；有限工作不等於全程恢復或全歷史完成。

## 2026-10-03：同範圍批次效率與正常空回修復

工作為 `tej-acquisition-efficiency-20261003`；保留既有 serial collector、queue、
source request／stage／receipt、桌面身分與唯讀 gateway，不另建下載器。

### 實測瓶頸與修正

- **批次容量未用滿。** 200,000 cells 下，30 欄的批次含 header 只能容納
  6,665 列；提高本機 cells budget 到既有 native guard 允許的 400,000，
  列數仍限制 10,000。公司數上限 32→128，既有矩形規劃器在上限內選擇
  查詢數最少的 uniform company/date batch，而非一律取 128。
- **查詢間固定空等。** 新 `minimum_query_start_interval_v1` 以開始時間限流：
  等待 `max(0, 2 - complete_query_seconds)`，仍只有一個桌面持有者；
  ETA 使用同樣的 `max(2, complete_query_seconds)` 成本。重試、磁碟、未知結果及
  shared interface 的安全等待保留。舊契約仍維持完成後間隔，不靜默追改。
- **單表結果阻擋整個 provider。** `Public Security Regulatory Capital Adequacy`
  的 exact attempt 已完整保存 247 列，但含請求範圍外的 2024-03 鍵。
  在 prepared／stage／active attempt、fresh/full native capture、schema/sample 及
  finished clock 全部核對後，只隔離此表。沒有採納、不裁切或重送原資料；
  unknown outcome、缺失 binding、shared interface 故障仍為全域阻擋。
  normal selection、table rotation、targeted acceptance 都排除該表的其他待送分片。
- **實際發現的 150 秒空等與假未知。** 首次放大批次仍逾時；保留失敗後，
  只讀診斷找到 TEJ 已產生 `ERROR1:No data !!(wis4)`，但是 dialog 的 `GW_OWNER=0`，
  舊 direct-owner 枚舉完全漏判。新候選要求同 PID、同 GUI thread、query root
  停用、原生 `#32770`、且該 process 只有 root 與唯一 modal 兩個可見 top-level
  視窗；再核對唯一標準 No-data 文案與唯一 enabled OK、父視窗、真實 control ID。
  只向該 modal 送標準 OK control notification，不啟用介面、不搶焦點，不關閉
  quota／permission／login／server error 或未知通知。

無 owner 的 task-modal 與 inactive dialog 的 BM_CLICK 行為，分別可對照
[Microsoft MessageBoxW 文件](https://learn.microsoft.com/en-us/windows/win32/api/winuser/nf-winuser-messageboxw)、
[BM_CLICK](https://learn.microsoft.com/en-us/windows/win32/controls/bm-click) 及
[BN_CLICKED／WM_COMMAND](https://learn.microsoft.com/en-us/windows/win32/controls/bn-clicked)。
本機 Win32 MessageBox fixture 實際讀回單一 OK button 的 control ID 為 2，
不能由顯示文案假定為 1；僅允許已核實單一 OK 的標準 ID 1／2。
ownerless／acknowledgement 契約保存到 raw 與 receipt，舊收據不追補。

### 同範圍查詢量證據

[只讀容量比較](../artifacts/data_quality/tej_efficiency_2026-10-03/capacity_before_v1.json)
的 48 張可比較表為 976,179→775,319 次，減少 200,860（20.58%）。
這是提案，不把它描述為全部已部署。

實際容量遷移為 **44 張表、865,673→689,428 次，減少 176,245（20.36%）**。
原計畫有 durable SHA backup，已完成 scope／工作權重／來源檔／收據保留，
不增加已完成格點的重抓。11 張 snapshot、原生待送 scope、阻擋表或鍵值年代待
對帳的表未被強制重排；另外兩張計畫不減少次數，所以不重寫。
private migration audit 為
`data_tej/planning_migrations/f5c3950730534b24a61773d81011d975/audit.json`。
key/period 不同的表不靠丟資料降低次數，延後表的 overflow 核對其 immutable
former plan bounds，不能錯拿新的全域上限拒絕舊請求。

上面是**查詢次數下降，不是相同比例的 wall-time／bytes／CPU 加速**。
官方 [Smart Wizard 中文操作手冊](https://www.tej.com.tw/TEJPLUS/TEJE3_TCHINESE.pdf)
說明 Preview 30 欄；不能將它泛化成 ExportToExcel 或 REST API 的全通道上限。
10,000 列／400,000 cells／128 公司／2 秒間距是本機設定，不是官方帳號 quota。
目前 `.env` 沒有已設定的 TEJ API key；桌面每日／每秒配額仍未知。
此輪不重試過往 ActiveX bulk export 故障，也不假裝已達官方整批通道理論最快。

### 實際接受與恢復

- [升級前 104 份收據](../artifacts/data_quality/tej_efficiency_2026-10-03/receipts_before_v1/audit.json)
  全部通過，46,904 列／817,637 非空值。
- [Windows modal fixture v5](../artifacts/data_quality/tej_efficiency_2026-10-03/ownerless_modal_fixture_v5.json)
  13 項通過：包含背景確認不變更 foreground、quota 文案不確認、額外可見視窗
  造成 ownership 歧義時不確認。只有自建 fixture，未連 TEJ／Excel 或送 provider 查詢。
  先前 MSAA／UIA／假定 IDOK=1 的候選實際失敗，沒有套到 provider 或追改為通過。
- 01:41:07 只恢復原逾時 attempt 的 exact saved scope 與現有正常 No-data modal；
  `data_query_repeated=false`，完成數 104→105，數值不增加，原失敗診斷保留。
  前面提出的一次重試確認沒有使用，亦未送 operator replay。
- 01:43:49／01:44:38 的兩次新正式批次各為 9,984 個格點／28 個 feature 欄，
  各取得 1,083 列、fresh end-to-end 37.126／48.789 秒。
  合計新增 **2,166 列／60,648 非空值**，不是恢復讀回耗時冒充 acquisition 速度。
- [升級後 107 份收據](../artifacts/data_quality/tej_efficiency_2026-10-03/receipts_after_v1/audit.json)
  全部通過，49,070 列／878,285 非空值；raw／Parquet／receipt hashes、source keys、
  schema 與 feature counts 都沒有失配。此為固定 SQL snapshot，不含之後新完成項目。
- 01:46:15 啟動既有自動 service；新 config/runtime policy 生效，未知結果不因 restart
  而重送。[公開瀏覽器 v2](../artifacts/data_quality/tej_efficiency_2026-10-03/public_browser_v2/rendered_acceptance.json)
  在真實 1440／390px 頁核對新的本機容量、5 秒狀態、結果列／容量 bar，
  並以同一 rendered HTTP generation 驗證，避免拿另外一次 request 的新完成數
  誤判 UI。無 overflow／JS error／外部 provider request；隔離 polling fixture
  不代表真實下載或修改 production queue。
- [自動執行後固定快照的 109 份收據](../artifacts/data_quality/tej_efficiency_2026-10-03/receipts_after_auto_v2/audit.json)
  在 01:52:34 通過本機完整性驗證：49,751 分片列／892,586 非空值，
  `local_artifact_failures=0`、`feature_count_mismatches=0`。其中 26 份為沒有新
  attempt SHA binding 的 legacy 收據，不能宣稱全部歷史已升級成新版 binding。
  [同次全表／欄位快照](../artifacts/data_quality/tej_efficiency_2026-10-03/acquisition_after_auto_v1/manifest.json)
  涵蓋 255 表／45,826 欄，沒有呼叫 provider 或公開原始值；
  [逐表歷史狀態](../artifacts/data_quality/tej_efficiency_2026-10-03/acquisition_after_auto_v1/table_history_status.csv)
  與 [逐欄下載狀態](../artifacts/data_quality/tej_efficiency_2026-10-03/acquisition_after_auto_v1/feature_download_status.csv)
  是有界 metadata 快照，不是全量歷史完成證明。
- 01:59:11 的實際唯讀 status 顯示重啟後已完成 6 個新下載，累計 113 份，
  正在下載 `Directors Holding(Yearly)`，`paused_reason=null`；
  `Delisted Option Attribute(2020~2021)` 與 `(from 2026)` 的 metadata 錯誤已個別
  延後，沒有使整個 provider 再次停止。113 是後續即時完成數，**不把它冒充
  上面已重驗的 109 份**；01:59:11 的資料量為 52,423 分片列／935,338 非空值，
  不等於去重後的經濟觀測筆數。
- 最終 [TEJ／gateway 回歸記錄](../artifacts/operations/agent-workflow/runs/efficiency-regression-20261002T174654-6b161047/run.log)
  為 **1,422 項通過**，另有 10 項 Node 前端測試與上述 13 項 Windows fixture 通過。
  這是本次受影響合約的驗證，不冒充全 repository／全 provider 或底層數值品質驗證。

程式位置：[容量／分片](../downloader/tej_planning.py)、
[完成結果隔離](../downloader/tej_source_isolation.py)、
[開始間距](../downloader/tej_scheduler.py)、[同範圍 audit](../scripts/audit_tej_acquisition_efficiency.py)、
[native modal fixture](../scripts/verify_tej_ownerless_modal.ps1)。
目前仍有已隔離的來源 metadata／period-key 問題與未發現的表軸；沒有以這輪優化
宣稱全部歷史已取得、底層數值／PIT 已完整驗證或全市場最終日期已保證。

## 2026-10-03 持久化逐表重試與持續取得

### 當下故障與原因

08:24（臺北）巡檢時，自動 supervisor 還活著，卻停在
`list_selection_prequery_needs_review`；不是正常持續下載，也沒有證據表示配額耗盡。
失敗工作是 `Bond Indicators Yield-Corporate` 的一個公司／日期／欄位分片。
三份原始 attempt 都明確保存 `market_data_query_submission_possible=false`，
沒有 Preview stage、完整資料回覆或成功收據；不是不明成交／查詢可以任意重送。

實機診斷顯示，native 清空後選一項仍只有一項，但送出來源選取事件通知後，
又出現舊項目，變成兩項，因而在 Select／Preview 之前被拒絕。
這是來源介面與選取事件的問題，不能當成來源數值錯誤或「沒有資料」。
只有 native `LB_SETSEL` 不會自動發出 `LBN_SELCHANGE`；
參見 [Microsoft 官方通知說明](https://learn.microsoft.com/en-us/windows/win32/controls/lbn-selchange)。
這份文件解釋一般控制項事件，並不證明 TEJ 自訂事件處理程式的內部原因。

六種自建 WinForms case（單選／兩種複選，各含 bound／unbound）
在[最終 native fixture](../artifacts/data_quality/tej_resilience_2026-10-03/list_fixture_native_final_v10.json)
通過 37 個檢查：精確名稱、先後兩次 native／managed selection、禁用拒絕、
不送 Select、前景不變。**通用 fixture 不能重現或宣稱修好 TEJ 特定 callback。**
清空先通知的候選雖通過 fixture，實機仍失敗；MSAA／UIA 候選也沒有通過完整 fixture。
失敗 artifact v4～v9 保留，候選均未留在正式選取路徑，不把 native readback 合格
當成 TEJ 模型必然一致，也不採用未驗證的 fallback。

### 已部署的恢復設計

沿用 canonical task、attempt、SQLite、dataset lock、Windows mutex、systemd 與唯讀 gateway；
沒有第二套下載器或私人帳戶／工作簿搜尋。

- 明確啟用 `exact_unsent_table_retry_window_v1`，沿用原本兩次短重試；
  用盡後不再讓單張表永久卡住其他表。
- 逐表持久化 `desktop_retry_windows`：60、120、240、480、900 秒，之後最多每
  900 秒一個探測分片。以原 task 與最新 finished／proved-not-submitted attempt 探測，
  同表其餘分片不得繞過等待；重新啟動也不清空退避。
- 重試許可要求 exact prepared request、scope、active attempt、註冊路徑、SHA、
  aware attempt clocks、明確 false 的未送出證據，且沒有 raw／Preview stage／adopted rows。
  再做一次正常唯讀 UI 核對：新鮮、同範圍、介面穩定、selector／公司群組可用且無通知。
  整個恢復核對 `provider_queries_sent=0`，不送資料查詢或採納來源軸。
- 保留兩次短重試已用盡的計數、原診斷與 audit；DB committed 後即使 audit 的
  secondary marker 寫入失敗，也不重複恢復或重置預算。只有同 task 的來源回覆
  通過原本嚴格驗證並入庫，才清除自己的逐表窗口。
- 正在執行／未知結果、來源 period barrier、停用介面、quota／login／permission／
  未知通知仍保持共享安全阻擋。不能用穩健性當成重送結果未知的理由。

關鍵程式：[原始 attempt 與重試證明](../downloader/tej_desktop_attempts.py)、
[共用選取／入庫](../downloader/tej_history.py)、[常駐監督](../downloader/tej_scheduler.py)、
[負面與重啟語義測試](../test/test_tej_prequery_recovery.py)。

### 網頁與實際接受

公開 [TEJ 進度頁](https://penguin72487.ddnsgeek.com/tej/) 的 schema 升至 12，
新增逐表等待原因、退避次數、最早可重試時間，以及「已到期，等待桌面與先行工作」。
這是 earliest eligibility，不是保證開始／完成時間；重試仍算未完成。
有故障依賴的階段另標示等待重試數，不把 heartbeat、未驗證讀回、格點或欄位數
混成成功資料列數。所有顯示仍只讀本機中繼資料，每 5 秒更新，不呼叫 TEJ／Excel
填畫面，也不公開 audit 路徑、來源原始值或帳號。

- [恢復前固定快照](../artifacts/data_quality/tej_resilience_2026-10-03/receipts_before_v1/audit.json)
  的 150 份 v4 下載收據全部通過，68,402 分片列／1,293,322 非空值。
  測試期間另有已授權工作完成 `Future DB`；不能把它算成本次 supervisor 的修復成效。
- 08:50:55 啟動既有 `stockagent-tej-history.service`。原失敗分片被確認未送查詢後，
  成功進入逐表等待；08:53:08 自動取得 `Fund's Turnover` **46 列**，
  之後其他資料工作繼續，含正常來源空回。第二次 bond 分片選取仍失敗，
  其窗口變成 120 秒，沒有再次卡住所有表，也沒有虛報修復來源選取。
- [08:54:41 固定 SQL 快照全收據重驗](../artifacts/data_quality/tej_resilience_2026-10-03/receipts_after_v1/audit.json)
  的 **154 份**全部通過，**68,453 分片列／1,294,357 非空值**；
  `local_artifact_failures=0`、`feature_count_mismatches=0`。
  後續即時完成數會繼續增加，不冒充這份固定快照已審核。
- 09:00:28 再取得 `IFRS_TEJ Consolidated First Financial_Security(Acc)-4`
  **2,613 列**。本次重啟後兩批非空資料合計 **2,659 列**，不含測試期間其他工作
  取得的 `Future DB`，亦不以來源空回覆充當有數值資料。
  [之後固定快照重驗](../artifacts/data_quality/tej_resilience_2026-10-03/receipts_after_auto_v2/audit.json)
  包含這兩批；檢查筆數與時點以該收據為準，不混用持續增長的即時總數。
- [全表與全部欄位快照](../artifacts/data_quality/tej_resilience_2026-10-03/acquisition_after_v1/manifest.json)
  包含 255 表／45,826 欄；空欄仍是未知，不等於零或全歷史完成。
- [受影響 TEJ／gateway 回歸](../artifacts/operations/agent-workflow/runs/resilience-regression-final-20261003T005249-20be6939/run.log)
  **1,246 項通過**，另有 **11 項 Node 前端測試**與上述 Windows 37 項檢查通過。
  早期測試 fixture 的 callback 參數錯誤已修正並重跑；原失敗 run 保留。
- [公開頁面驗收](../artifacts/data_quality/tej_resilience_2026-10-03/public_browser_v2/rendered_acceptance.json)
  使用真實 source-backed HTTP generation，在 1440／390px 核對資料量、逐表重試、
  搜尋／分頁／篩選、5 秒同步及狀態切換；具體結果以該收據為準。
  隔離 polling fixture 只驗證前端轉換，不冒充 TEJ 真實下載。

**仍未完成：** bond 表來源選取本身仍待修復，既有被隔離的 metadata／source validation
問題及尚未清點的歷史軸仍存在。這輪證明持續下載與逐表恢復，不是所有表、
全部原生歷史、底層精度／單位或 point-in-time 的完整驗證，更不保證桌面來源永不故障。

## 2026-10-03 晚間：不明查詢與無回應外掛恢復

19:55 的 status 查核發現最後成功收據仍是 09:36；服務活著卻已停止取得約十小時。
卡住的是 `Fund's Holding(Weekly)`，原 attempt 有 `prepreview_verified` stage、
沒有完整 raw response／成功收據，並留下 `Query identity or control owner changed`。
本次先停用 collector，確認 Windows 原生 HWND 的 PID／標題仍與原 session 相同，
但 `IsHungAppWindow=true`；不能把它當成未送出或正常空回。

### 修正及操作邊界

- **無回應時仍核對原視窗。** 外掛重啟路徑使用原生 HWND owner、頂層 caption、
  window class、程序啟動時間及 image SHA。跨程序頂層 caption 可由
  [Microsoft GetWindowTextW](https://learn.microsoft.com/en-us/windows/win32/api/winuser/nf-winuser-getwindowtextw)
  讀取，不必等待無回應程序的 UIA 回覆。沒有重綁 Windows ghost 視窗、改 workbook
  pin、強制啟用控制項，或對任意程序按名稱執行終止。
- **處理此時已不存在的 connector。** 本次原程序只剩唯一原生 hung Wizard，
  沒有 `TEJProConnector`。只在原視窗 hung、原 PID／caption／class／workbook
  pin 與 image/start-time 均一致，且全程序沒有其他應用程式表單／對話框時，
  接受 `sole_native_hung_query_no_other_application_windows_v1`。隱藏的其他 Wizard
  或 `#32770` 對話框同樣會拒絕，不能以 connector 缺席略過 auth／quota 通知。
- **操作員專用，不是自動重送。** 新增 `restart-unknown`，要求 exact task、原始
  prepared request、restart/discard/usage 三項明確確認。無參數的既有 restart
  仍拒絕 unresolved Preview；scheduler 的 `unknown_outcome_auto_retry` 仍為 false。
  已有 raw／receipt、不同 active attempt、其他 running／unknown 或範圍不符，均在
  reset 前拒絕。原始 attempt 保留 unknown，不改寫成未送出、成功或空值。
- **三鍵資料不能把公司×日期當成原生筆數。** 原查詢是 100 基金×100 日期、
  四欄、第三鍵為持股記錄；其實際筆數尚未取得，不能宣稱已證明容量超限。
  本次明確使用 `operator_unknown_record_geometry_prior_v1` 的 density prior=250，
  將每次選取矩形限制到最多 40 組公司／日期，再由既有 capacity 路徑處理真正量測
  到的 overflow。250 是標示過的復原幾何先驗，不是觀測列數、官方上限或完整性保證。
- **全範圍保留。** 沿用 canonical lazy planner 排列該表所有尚缺的欄位／公司／日期；
  已完成 scope 排除、總工作量與完成工作量一致，舊 plan 有 SHA backup。
  舊 unknown task 變成 `superseded_operator_unknown_geometry_v1`，原 error、attempt、
  prepared/stage 和 traffic 都保留；它不是完成下載。替代分片會使用正常來源流量，
  可能再次計入原先未保存查詢的用量。復原／重排本身沒有發送資料查詢。

本次原生重啟、fresh-session 驗證與小分片重排在 20:28 成功，20:29:25 重新啟動
既有 `stockagent-tej-history.service`。PID／HTTP 200 不作下載成功驗收，後續必須
核對新的非空 source receipt、連續批次與 metadata-only 公開狀態。

```bash
source scripts/runtime_env.sh
# 先確認完整 saved response／existing Preview 無法採納，並停妥原 collector。
# TASK 與 PREPARED 必須是當下同一個 unresolved active attempt，不能照抄舊 task。
run_fintech_python -m downloader.download_tej_history restart-unknown \
  --task-id EXACT_TASK --evidence data_tej/requests/EXACT_ACTIVE_ATTEMPT.json \
  --allow-restart-addin --allow-discard-query-settings --acknowledge-unknown-usage \
  --record-density-prior 250
```

省略 density prior 時僅允許同一 scope 的明確 one-shot operator replay；重啟證據、
原始 query SHA 與 consumed-once authorization 保留。不要對已知大三鍵矩形直接重送。
任一 stop／open 可能已送出但無完整回覆時，保留 lifecycle barrier，不盲目重開。

本次實作驗證：[836 項回歸](../artifacts/operations/agent-workflow/runs/restore-final-regression-v2-20261003T122655-0126a789/run.log)、
[Windows native fixture](../artifacts/data_quality/tej_restore_2026-10-03/windows_fixture_v2.json)、
[177 份恢復前完整性稽核](../artifacts/data_quality/tej_restore_2026-10-03/receipts_before_v1/audit.json)。
fixture 只操作自己的新表單，不證明 TEJ 來源的全部值正確；恢復前稽核為
86,712 分片列／1,699,645 非空值，artifact 與 feature count mismatch 都是零。

### 恢復後的實際接受證據

20:42:00（臺北）的[固定 SQL 快照／全收據重驗](../artifacts/data_quality/tej_restore_2026-10-03/receipts_final_v1/audit.json)
是 **186 份成功下載收據、92,485 分片列、1,762,860 非空值**；
local artifact failure 與 feature count mismatch 均為零。相對恢復前新增
**9 份成功下載收據、5,773 分片列、63,215 非空值**。新增非空資料來自
`Contract Adjustment of Stock Futures`（4 列）、
`TSE/OTC Adjusted_Price(Daily)-ROI`（499 列）及
`Qfii/Dealer/Invest. Buy/Sell`（5,270 列）；其他新成功下載包含明確來源空回，
不把空回當成已取得數值，也不把分片列當成去重後原生觀測。

[20:42 公開瀏覽器接受](../artifacts/data_quality/tej_restore_2026-10-03/public_browser_v4/rendered_acceptance.json)
通過桌機、手機、來源-backed status 與每 5 秒原位更新；isolated fixture 額外覆蓋
等待來源、讀回、入庫、HTTP 錯誤及等待安全恢復，且不改 production queue。
這一輪調整的是驗收流程：實際展開共用清冊 disclosure、等待 deferred 區塊 layout
並捲至 feature 清單，才驗證可見區域的 lazy loading；沒有為了驗收強制全頁載入，
瀏覽器也沒有發送 TEJ provider 查詢或接收原始數值。

當下新 TEJ 外掛及原 Excel 都回報 responding；Excel 啟動時間保持 10/1 原值。
沿用既有 collector、queue、lazy planner、receipt、systemd 與唯讀公開 gateway，
沒有建立第二套下載管線。**仍有 15 項來源驗證／metadata 待修及 1 項逐表 retry**，
已隔離並在網頁保留，不阻擋其他可正常處理的表。基金 density prior 重排已接受，
但此快照尚未驗證其新分片的實際列密度／非空收據；不得宣稱該表修復完成、全部歷史
下載完或 UI 的條件式年級工時是保證完成日。

## 驗證與已知未完成

程式：[collector](../downloader/tej_history.py)、[CLI](../downloader/download_tej_history.py)、
[Windows bridge](../scripts/tej_smart_wizard_bridge.ps1)、[唯讀投影](../stockagent/live/tej_dashboard.py)、
[網頁驗收](../scripts/verify_tej_dashboard.py)、[全欄位下載狀態快照](../scripts/snapshot_tej_acquisition_inventory.py)。
前輪 545 個 focused semantic／gateway regression 測試通過，另含 3 個 Node 前端請求協調測試，
已做 Python／JavaScript／PowerShell 語法檢查；
公開桌面 1440px 與手機 390px 的真實搜尋、下一頁、P2 篩選、零外部 provider 請求與
無 JS error 均有[瀏覽器驗收收據](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/public_v14/rendered_acceptance.json)。
手機卡片溢出覆蓋分頁的問題已實際發現並修正，不以 DOM 計數取代可視驗收。
該收據是畫面可用性證明，當時下載仍需檢查，**不代表下載已完成**。

前輪 `runtime_repair_v4` 的 395 個 TEJ focused Python 測試與 3 個 Node 協調測試通過。
該輪[真實瀏覽器驗收](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/runtime_repair_v4/rendered_v3/rendered_acceptance.json)
在 1440／390px 實際搜尋、分頁、P2 篩選、無 overflow／JS error／外部 provider 請求，
顯示的是當時仍停用的真實介面狀態，不把可讀網頁當成來源恢復。
有界的[Windows fixture v6](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/runtime_repair_v4/windows_fixture_v6.json)
27 項通過；v5 曾在字元確認失敗時停止輸入且未送查詢，失敗收據保留。
v6 增加錯誤時私有 caret／前後值診斷，**不是把 v5 改成通過，也不證明共享桌面永不失焦**。
fixture 只驗證自建表單，來源收據、桌面恢復與全歷史完整性仍分開。

下載狀態也可凍結成新的全表／全欄位 CSV；不覆蓋前一次證據、不送來源查詢：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/snapshot_tej_acquisition_inventory.py \
  --output artifacts/data_quality/tej_acquisition_snapshot_<unique_timestamp>
```

這是分開的短 metadata read transaction 的有界快照，CSV 空格表示未知，不是零。
不是即時頁，也不是「全部原始檔已重驗」或「全部歷史已完成」證明。

仍未完成：全部表的真實公司／日期軸、全部歷史值、逐表底層數值精度／單位、
來源 Transformation 設定逐表驗證、發布與原始歷史版本、官方桌面配額、經全量範圍與實際排程核實的 ETA、無人值守增量追新、
跨來源逐鍵校驗以及遠端訓練／冷庫授權。不能以這份完成目錄或網頁已上線代替上述證據。
