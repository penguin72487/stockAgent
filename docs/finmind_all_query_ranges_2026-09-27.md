# FinMind 全清冊查詢範圍與請求最小化

## 先定義「最少」

目標是覆蓋所有仍必要的資料，不是讓請求數看起來小。資料的唯一事實鍵包括
來源、商品、事件／觀測日期、頻率及欄位；不同欄位的同名指數不能互相冒充。
API 支援的選取形狀、帳號權限、已完成缺口、回應大小與配額是限制條件。

- 真正全市場區間端點：每個連續待抓區間，理想下一次請求。只在實際資源限制
  下縮段；已完成且未到修訂時間的區間不為了湊長查詢而重抓。
- 單一商品全歷史：每個必要商品至少一次；SDK ID 清單仍是多次 HTTP。
- 全市場單日：若只能走此形狀，至少每個必要日期一次。另一方向是逐商品全史，
  但只有商品全集與缺口都已證明時，才可比較 `未完成日期數 D` 與
  `Σ 每商品所需區間數 + 全集驗證成本`，不能只看 `min(D, S)` 就宣布全市場完整。
- 財報／月營收：一次全市場期別通常遠勝逐檔。現有起點至 2026/09/27 的
  期別 anchors 為損益 146、資產負債 59、現金流 73、營收 296，共 574。
  3,384 個目前候選 ID 各查四表則為 13,536 次；候選 ID 並非已證全歷史全集。
  尚未核實的 2014 年前非期別日期不因這個算式被靜默刪除。
- 本機可正確衍生者不發 API，例如法人 wide 沿用已驗 long；snapshot 一次取表，
  但今天主檔不等於歷史快照。

這是有條件的下界，不是宣稱所有 FinMind 端點已找到可證明的全域最優解。
官方未給通用最大年數，不能把「未公布」寫成「保證無限」。

## 全清冊與原始證據

`scripts/audit_finmind_query_ranges.py` 用唯讀 SQLite／既有 JSON 狀態清點既有
Sponsor、Complement、Free、未啟用及停用新聞的聯集；不發 API、不改 queue、
不掃全庫 Parquet。每端點獨立列出 owner/alias、查詢形狀、設定起點、實際最早
與最新、已取得／回空／待抓／失敗、最大區間政策、下界可否成立及官方來源。

目前清冊 **105 個唯一端點**；舊網站 128 個項目含跨 worker alias，不是128種
獨立資料。兩個結算價轉逐商品 owner 後有130個展示／來源角色，但唯一端點仍105。
報告基於本機已註冊清冊，不宣稱涵蓋供應商日後新增而尚未登錄的 API。
實際套用後清冊：[CSV](../artifacts/data_quality/finmind_query_ranges_20260927T031243794859Z.csv)、
[JSON含各owner與政策](../artifacts/data_quality/finmind_query_ranges_20260927T031243794859Z.json)。
筆數與任務數是11:12左右的本機觀測，背景工作會繼續推進；不是永久定值。

| 主責查詢形狀 | 唯一端點數 | 呼叫最小化方式 |
|---|---:|---|
| 全市場單日 | 41 | 保留必要日期；商品全集尚未證明時，不盲換逐檔歷史 |
| 全市場財報／營收期別 | 4 | 每個必要期別一次，不逐股票拆 |
| 全市場／總體區間 | 17 | Sponsor 11 + Complement 6；最長連續到期區間，實際資源限制才縮段 |
| 逐股票／商品／固定指標歷史 | 12 | 10既有逐ID + 2結算價；每個必要ID從最早設定日期到最新 |
| 主檔／日曆快照 | 12 | 每輪一次全表，不拆年份；不冒充歷史快照 |
| 本機衍生 | 1 | 法人 long → wide，直接API 0次，父資料仍需取得 |
| 明確未排程 | 17 | 高容量、其他授權或特殊接口，保留各自原因，沒有偷偷宣告完成 |
| 使用者停用新聞 | 1 | 不發請求 |
| 合計 | **105** | Alias不重複計數 |

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.audit_finmind_query_ranges
```

## 從 8 類擴至 11 類最長區間

原8類及其93分區／7次正式下載證據見
[第一階段報告](finmind_maximum_range_2026-09-27.md)。新增：

| 來源 | 單次驗證區間 | 返回列數 | 本機比對／最後一天 |
|---|---|---:|---|
| 停止融資融券 | 2015/01/01–2025/12/30 | 34,817 | 11年度全欄／逐列一致，末日6列 |
| 暫停當沖 | 2014/06/01–2025/12/31 | 33,189 | 12年度全欄／逐列一致，末日7列 |
| 處置期間 | 2001/01/01–2025/12/31 | 6,901 | 已有12年度一致，末日9列 |

三次請求同時驗最長跨度、schema、重複次數和 inclusive end，不另拆邊界探測。
事件的 `end_date`／`period_end` 是處置或停券期間，不拿它當觀測日期驗證。
證據：[三來源實測](../artifacts/data_quality/finmind_max_ranges_20260927T024359706616Z.json)。

## 修正兩個看似「沒有歷史」的結算價

同樣查完整封閉年度：期貨1998–2025，不帶 ID 回0，`TX` 回680列；選擇權
2001–2025，不帶 ID 回0，`TXO` 回754列。官方一般文件採逐商品，MCP摘要
未明確標出 ID 要求；實測否定「無ID回空＝全市場無值」。
證據：[四次對照](../artifacts/data_quality/finmind_max_ranges_20260927T024357385865Z.json)。

修正沿用 Complement `id_history`，以已驗 SHA／size 的 `TaiwanFutOptDailyInfo`
展開所有已知商品，不只硬編 TX/TXO。每商品初次從1998／2001抓到執行當天；
不將全市場單日或逐年空查詢再排一次。Sponsor舊查詢列與收據保留，先存精確
migration audit，再標 `deprecated_query_shape`，網站顯示轉交 owner，不能繼續
算成全市場已取得。商品全集仍屬已觀測主檔，不保證已含所有下市歷史商品。

原始時鐘與精度：`date` 是到期／結算觀測日，`fetched_at` 是本次實際取得時間，
不冒充當年的發布時間；原始 `settlement_price`、`notional_value` 保留精度，
不套報價 tick 四捨五入；不把結算價當成交，也不接入任何交易／模型變更。

## 總體歷史修訂也要合併

Complement 原有5類整體歷史首次會整批，但90日修訂時回到逐年查詢。
新規劃把同一來源連續且**已到期**的 pending／failed／修訂年度合併，保留
尚未到期的完成區間與冷卻缺口；原子 claim／復原、整批驗證後逐分區落盤。
實際過大或讀取逾時才學習更小批量；錯誤不覆蓋 last-good 非空年分。
增量預留不足時，當期請求不能夾帶舊年度。
64 MiB解碼body與100萬列是本機安全值，不是官方限制；最小分區若仍超限，
保持可見錯誤，不能以截斷資料換成功。所有年份先做欄位／Arrow schema預驗，
才開始落盤，避免後一年度壞資料讓前一年度先被更新；I/O中斷仍由既有逐分區
收據及claim恢復，不假稱跨所有分區的檔案系統交易。

目前全部單商品歷史原本就使用最早設定起點至最新，不能靠加長5年限制再省
一次請求；後續完整修訂與增量尾端合併是另一項取捨，不虛稱本輪已全部解決。
GoldPrice 另有日內終點問題，不能直接複製純日期來源的查詢邊界，見下節。

## GoldPrice 年底缺口與 timestamp 邊界

一次短區間實測 `2016-01-01 → 2018-01-02` 返回 1,409 列；終日只有
`2018-01-02 00:00:00`，但本機完整當天原有312列。因此 `end_date` 是
午夜時間上界，**不是包含整個最後一天**。同次回應比舊2017年分區多出
262列12月31日日內資料，證明舊查詢邊界確實會漏值。

另有261列2016年日期從 `YYYY-MM-DD` 變成 `YYYY-MM-DD 00:00:00`，
價格相同；報告保留原始字串比對差異，只有驗證比較使用明確記錄的午夜
語意正規化，不改寫來源價格或假造原始日期。修正採下一天午夜作 provider
上界，再精確裁切到需要的半開時間區間；不把下一天午夜混入前一年。
API 查詢、資料涵蓋日期、實際抓取時間在收據分開記錄。

證據：[唯一一次 Gold 邊界探測](../artifacts/data_quality/finmind_gold_range_20260927T025754682232Z.json)。
日期字串差異改按已明示的午夜語意驗證後，使用同一SHA固定回應做
[零API本機重驗](../artifacts/data_quality/finmind_gold_range_20260927T030804094218Z.json)：
1,147列既有重疊沒有數值遺失，新增262列皆位於2017/12/31日內；不是再抓一次。

## 不會以錯誤捷徑省次數

- `TaiwanVariousIndicators5Seconds` 是 TAIEX；`TaiwanStockEvery5SecondsIndex`
  另有 `stock_id/price/kind/time` 的產業指數，不能把前者5343日完成當成後者完成。
- 週／月K與持股分級支持逐商品區間，但全市場只明示單日查詢；已知日期例外
  保留，沒有假設一定星期五或月初就把其餘日期刪掉。
- `streaming_all_data` 是即時 tick 模式，不是歷史全市場開關；`zip_enabled`
  不是擴大範圍授權。SponsorPro 批量物件與目前 Sponsor 不同，沒有繞過權限。
- 新聞仍依使用者要求停用；未啟用高容量 Tick／分K／分點端點仍列出原因。
- HTTP200、service active、回空，分別不等於內容正確、全歷史完整或沒有資料。

官方依據：[技術面](https://finmind.github.io/tutor/TaiwanMarket/Technical/)、
[籌碼面](https://finmind.github.io/tutor/TaiwanMarket/Chip/)、
[基本面](https://finmind.github.io/tutor/TaiwanMarket/Fundamental/)、
[衍生性商品](https://finmind.github.io/tutor/TaiwanMarket/Derivative/)、
[OpenAPI](https://api.finmindtrade.com/openapi.json)、
[更新與更正公告](https://finmind.github.io/WhatIsNew/)。

## 正式下載與遷移驗收

2026-09-27 **10:57 台北**，Sponsor v6 已套用：

- 處置期間2001–2013的13個待抓年度，**1次 HTTP** 正式補齊2,078列；
  13個 Parquet SHA、實際列數、逐列年度邊界與共同 request ID 均通過。
- 期貨29個、選擇權26個舊無商品代碼任務，共55列，逐列保留精確舊狀態
  JSON audit，再標為不再派工；舊空回收據沒有刪除，亦不冒充全市場完整。
- Sponsor 背景程序重啟後 `active/running`、`NRestarts=0`，不重啟交易或 Discord。

驗證腳本最初用了錯誤的本機欄名 `last_attempt`；正式下載本身成功。
改用 `last_attempt_at_utc` 後，僅重做本機驗收，**沒有多發 API**。
兩份證據均保留：[原執行紀錄](../artifacts/data_quality/finmind_all_ranges_sponsor_acceptance_20260927T025749059611Z.json)、
[修正後本機驗證](../artifacts/data_quality/finmind_all_ranges_sponsor_verified_20260927T025814922621Z.json)。

連同第一階段，指定缺口共106個分區、17,937個新落盤列，正式請求8次；
舊排法這批需要79次，新排法少約89.9%。這不是全庫所有剩餘工作的比例。
一次性API契約驗證成本另外計：原11次、新3個區間、4個結算對照、1個Gold
邊界，共19次，不灌入下載節省數字。

### Complement、Gold與網站

2026-09-27 **11:10台北** 正式驗收三次必要資料請求，均使用既有queue／
共享配額／限流／收據，不是繞過排程的獨立下載程式：

| 工作 | 正式HTTP | 結果 |
|---|---:|---|
| 期貨結算價BRF | 1 | 80列，2018/08/02–2026/03/03，日期、商品ID、SHA與筆數一致 |
| 選擇權結算價AAA | 1 | 回空，保留observed_empty；不是宣告選擇權歷史完整 |
| GoldPrice 1900/01/01–2026/09/27 | 1 | 127分區，50非空／77回空，638,863列 |

Gold查詢到今天，**實際回傳最後資料是2026/09/25**，兩者不能混為一談。
整次canonical worker（帳號驗證、seed、HTTP、驗證與寫入）12.88秒，並非純HTTP
延遲。這次127年度修復若逐年查需127次，實際一次；未把已完成的Gold原庫全部
算成新取得資料。source日期觀測最早1970/01/01只有1列Price=17，屬待核實的
異常早期點，原樣保留，不把它背書成可靠連續歷史或可用訓練起點。

全庫Gold舊值比對：原637,069列保留，新增**1,794列全是12/31日內漏值**：
2017年262、2018年206、2019年208、2020年312、2021年267、2024年270、
2025年269。新舊Parquet SHA通過，日期字串按明示的午夜語意比較，舊值缺失／
變更0；127個舊收據歷史archive亦逐一通過原始SHA，沒有刪除來源證據。

證據：[正式三次下載驗收](../artifacts/data_quality/finmind_all_ranges_complement_acceptance_20260927T031053006123Z.json)、
[Gold全庫零API比對](../artifacts/data_quality/finmind_gold_full_span_parity_20260927T031225227903Z.json)。

新商品佇列包含已驗主檔的1,081個期貨代碼、265個選擇權代碼；這是候選商品
全集，不是1,346個皆有歷史。11:13本機再查，期貨62個非空／127個回空、892個
待抓；選擇權1個回空、264個待抓，背景下載確實持續前進。這裡不將回空轉成
虛構價格，不宣称所有商品都抓完。

Sponsor、Complement與公開唯讀gateway均已載入新版，當次`active/running`、
`NRestarts=0`。本機與公開 `/finmind/api/status` 都HTTP200、130展示角色，
兩個舊Sponsor結算項目顯示已轉交Complement，不重複計分母。網站移除
「待抓分區×每次限速＝理論最低耗時」錯誤公式；明確區分未知ETA和假設
每分區一請求的未合批投影，不能把後者顯示成完成倒數。
證據：[服務、公開API與原始收據SHA驗收](../artifacts/data_quality/finmind_all_ranges_runtime_20260927T031348774432Z.json)。

完整FinMind／配額／取得門檻／收據／批次／Gold／schema回歸**410 passed**；
公開gateway FinMind路由另4 passed；面板桌面／手機測試另3 passed，與部分
focused回歸有重疊，不混加為總數。compile、JavaScript syntax與指定diff check通過。
未改交易、Discord、即時行情或冷庫發布權限。全域理論最少請求數、所有商品
歷史完整性與PIT仍不宣告已證明。
