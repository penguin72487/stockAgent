# 全來源取得分工與配額修復（2026-09-27）

FinMind 後續呼叫效率、批次與晚發布修正，見
[2026-09-27 實作及驗證結果](finmind_request_efficiency_2026-09-27.md)。

本次沿用既有下載器、來源 registry、收據、共享限流及 systemd。
清冊不是另一套下載系統，也不是「全部歷史完整」證書。
資料品質檢查將 **數值取得、歷史完整、發布時間／PIT、可用訓練、冷庫授權** 分開處理。

## 執行順序

1. 保護即時訂閱／交易，以及已到可信發布邊界的固定增量工作。
2. 同一帳號先留下一小時固定增量需求與在途餘裕，其餘才補必要歷史／缺口。
3. FinLab 一般資料沒有未完工作後，剩餘配額才可補低優先 Tick。
4. 額外 API 校驗還必須通過全來源必要取得門檻及本機佇列／共享餘額檢查。
5. 比較已存在的兩份本機資料不需要 API 配額；以有限 CPU／記憶體獨立稽核。

未發布不等於過期；冷卻、失敗、blocked 不等於完成。不同上游配額可同時使用，
不加一個跨所有來源的 10 req/s 總限流。若無官方數字，既有安全上限不是官方承諾。

## 分工

| 來源 | 主要責任 | 不重抓規則／限制 |
|---|---|---|
| TWSE／TPEx | 官方每日股價量、估值、法人、融資券、上市生命週期等各自欄位 | 原始官方同鍵優先；其他源不能整表覆蓋；股數口徑與交易機制必須一致 |
| MOPS／TDCC／TAIFEX／央行／主計／財政部 | 財報、公告、集保、衍生品、總體等各自事實 | 沿用官方歷史／事件增量，缺數值與僅缺歷史發布版本分列 |
| 永豐 Shioaji | 自有分鐘、逐筆、期權及即時訂閱 | 保留現有共享 person／每日 bytes／行情查詢限制與開盤保護，不新增登入 |
| FinLab | 獨有寬表、財報／事件研究欄位 | `price:收盤價` 首次仍取；後續純校驗須最後；一般資料未完成時不抓 Tick |
| FinMind Sponsor | 具權限的全市場日分區與其他歷史補集 | 同一 dataset 不再由 Complement 平行逐檔重抓；法人 wide 由 long 本機衍生 |
| FinMind Free／Complement | 官方交易日曆、全市場盤中統計及 Sponsor 未接管的資料 | 與 Sponsor 共用帳號配額、冷卻、固定增量預留 |
| Binance／OKX／Bybit | 各自交易場館的 1 分 K 與已啟用原生衍生品特徵 | 不同場館不是同一價格；合格 1 分 K 可衍生日 K，不新增 crypto Tick／L2 管線 |
| OpenBB＋直接公共下載器 | 各自實際上游的總體／財報／公告／外匯等 | Yahoo、FRED、SEC 共用對應上游限流桶；未核實欄位等價者不冒充已去重 |
| Dune／SEC／ETF 發行商／公共鏈資料 | 已註冊的鏈上、申報、持倉、供給／活動等事實 | SQL entitlement、歷史版本缺口仍顯示；不靠重試繞過權限 |

同一事實的判定包含 instrument episode、venue、event time、grain、field、
adjustment、currency、unit、revision。單看名稱、日期或檔案大小，不能證明資料重複。
股票數量使用股；期貨口數、金額、比例、crypto base/quote volume 不得乘上股票倍率。

## 本次實作的修正

- `downloader/finmind_scheduling.py`：固定增量需求與目錄抽成共用 leaf；Free、Complement、Sponsor 共用預留計算。
- Sponsor `_next`：必要任務包含尚未到重試時間的 failed／blocked／inflight；不能因無「目前到期」工作就提前派 P3。
- Complement：Sponsor 的配額預留／429／IP cooldown 是共享等待，不是所有權失效；不另啟重複逐檔路線。修正批次結束後使用已關閉 SQLite 連線的錯誤。
- Sponsor 以已驗交易日曆判斷保護時段，已知休市不白等 08:20–09:10；特殊週六開市仍保護。
- FinLab：necessary outstanding 與 actionable 分開；整表 key 不含 `tw_tick:*`／`tw_minute:*` 日期分區示例。同步開始／結束寫 `core_acquisition_status.json`。
- FinLab Tick：與既有排程共用帳號鎖；批次中每分鐘／配額重置重驗一般資料優先權；帳號或額度錯誤停止整批。
- OpenBB：`fred → fred_api`、`sec → sec_edgar`，保留較慢個別速度、共享冷卻與 HTTP preclaim 去重。CFTC Socrata 與 legacy ZIP 不因同名字就合桶。
- FinMind 原生格點：2011-01-17 起 15 秒、2014-02-24 起 10 秒、2014-12-29 起 5 秒，早期為 1 分鐘；完整性仍須日期、所有格點、SHA、筆數一致。
- 四個已實測來源將 API inclusive `end_date` 改為本機 exclusive end 減一天；沒有放寬返回日期驗證。僅重排有證據對應的舊錯誤，保留 `query_shape_repair_audit`。

四個修復來源：`TaiwanStockInfoWithWarrantSummary`、`TaiwanBusinessIndicator`、
`CnnFearGreedIndex`、`TaiwanOptionVix`。
每個只做一次探測，日期／列數／摘要見
`artifacts/data_quality/finmind_partition_semantics_2026-09-27.json`。

## 額外 API 校驗門檻的精確邊界

`downloader/acquisition_policy.py` 是唯讀 veto，不發 API、不建立任務、不宣稱全歷史完成。
目前接線到 **Sponsor P3** 與 **FinLab 純價格刷新校驗**；其他必要下載仍由各自 worker 執行。
CSV 將設計政策與實際接線分欄，不能把所有 provider 說成已完成欄位級去重。

- 使用同一份全資料網站 snapshot，驗 schema、registry 分母、完整性檢查、時間及重複 ID。
- 過期 15 分鐘、未知或矛盾完成證據不放行；檔案讀取／佇列計數有短期快取。
- Alias、storage group、已登錄未實作、未啟用、純 PIT 候選、連續串流等明確分列，不當必要歷史任務分母。
- 明確的價格／商品投影依 canonical owner 判斷；owner 缺失不能放行。
- FinLab 需新鮮 core receipt；FinMind 混合價格端點由 priority < 8 的真實任務判定，避免 P3 自己等自己。
- snapshot-only 日曆／主檔需專用本機證據，不要求不存在的「所有歷史快照」分母。
- TAIFEX daily 等尚缺專用歷史證明接線者仍是 `unproven`，不是重新抓取指令，也不是資料遺失結論。

## 本機交叉稽核

`scripts/audit_finmind_sponsor_overlap.py` 僅比已落盤的未調整日價與股數；
先查 Sponsor receipt 及官方 source receipt，再實際核 SHA／大小／來源穩定性。
拒絕重複日期標的鍵，不用 MAX 消掉衝突；列出雙方缺 key 與值差異。
相同已驗來源摘要才重用 SQL；限制 2 threads／1 GB、單分區 31 日期／20 萬列。
差異是調查線索，不自動覆蓋官方值。沒有 Sponsor 分區時如實回 `waiting_sponsor_price`。

## 驗收／重現

2026-09-27 00:55 左右（臺北）實際本機修復：兩組 10,686 個分區皆通過 SHA／日期／筆數／原生格點驗證；
1,956 個舊 partial 轉完整（每組 978），仍 partial 0、失敗 0、API 請求 0。
原始 Parquet 不變，原收據保留於 `versions/`，正式收據新增驗證版本。
Sponsor 253 個舊邊界錯誤已寫入 migration audit 後重新排程，舊 blocked 計数已降為 0；
此數字是錯誤解除，不是 253 個分區全部已重新下載完。
三個 FinMind 服務及 OpenBB 資料服務已載入修正；FinLab 由原 timer 載入，
其 core receipt 實際報 196 必要工作＝193 既有待例行核對＋2 缺 key＋1 資源阻塞。
此 196 不是 196 個缺欄位。相關整合回歸 **276 passed**，其後補上 FinMind
前端 15／10 秒明細與 API allowlist，FinMind dashboard 5 passed（包含新增 1 項）；另 OpenBB
限流／quota／cooldown focused sweep 50 passed（與整合套件有重疊，不相加）。
`py_compile`、shell syntax 與指定變更的 `git diff --check` 通過。
資料頁 `/finmind/api/status`、`/data-monitor/api/summary` 本機 HTTP 200；
Free 服務的正式 status 已為 10,686／10,686、pending 0、retry 0，重啟後此次 session API 0 次。
HTTP 200 不代表全來源健康：當時全頁仍有必要回補／無法取得端點，額外 API 校驗門檻保持關閉。
門檻在本機連續三次量測約 245／7／8 ms（第一次含 metadata／SHA與匯入成本），沒有 GPU 工作。
FinMind 兩组每日間隔明細均為 1 分 1,503 日、15 秒 764 日、10 秒 214 日、5 秒 2,862 日，
加總各為 5,343。頁面沿用原版型，只補齊遺漏類別；已驗證 API 與 JS syntax，
此輪 Chromium／瀏覽器連線不可用，未宣稱桌面／手機的視覺驗收。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/export_data_acquisition_inventory.py \
  --output artifacts/data_quality/data_acquisition_inventory_2026-09-27.csv \
  --policy-output artifacts/data_quality/data_acquisition_policy_2026-09-27.json
run_fintech_python -m scripts.audit_finmind_sponsor_overlap
```

Free 歷史格點的一次性本機修復需先停其 worker；CLI 自己再取得相同 worker lock：

```bash
run_fintech_python -m downloader.download_finmind_free \
  --root data_finmind --revalidate-sessions-local
```

保留原 Parquet 與收據 bytes，結果寫 `data_finmind/grid_revalidation/session_grids.json`。
不得一邊執行同一來源下載，一邊無鎖改正式收據。

## 尚未完成／不擅自改動

全部 provider 欄位／日期／標的的跨源等價 reconciliation 尚未完成；
OpenBB 與部分獨立來源的內容重疊也不能只靠共用限流解决。
FinLab 的 `broker_transactions` 資源限制、兩個空值 key，Shioaji 缺口／無合约，
Dune SQL 權限與各歷史未完成任務都仍需處理。
CryptoHistorical 的 CFTC 與 Wikimedia generic bucket 尚需按實際上游權限細分。
沒有更改冷庫 catalog 授權、沒有重啟交易／Discord／即時行情服務、沒有刪除原始資料。

## 官方核對

- [FinMind API usage](https://finmind.github.io/api_usage_count/)：使用 `user_count`／`api_request_limit` 動態計額度，不把所有 token 寫死為 600。
- [FinMind 技術資料](https://finmind.github.io/tutor/TaiwanMarket/Technical/)：歷史原生間隔、交易機制／張與股口徑。
- [Shioaji 使用限制](https://sinotrade.github.io/zh/tutor/limit/)：行情查詢 50／10 秒；訂閱優先、重用登入、歷史快取、依實際帳號 bytes。
- [FinLab 更新紀錄](https://finlab.finance/docs/change-log/)：每日配額重置資訊 UTC+8 08:00；不同於永豐開盤日重置。
- [FRED errors](https://fred.stlouisfed.org/docs/api/fred/errors.html)：120 requests/min。
- [SEC developer resources](https://www.sec.gov/about/developer-resources)：合計每使用者 10 requests/s，本次共享只驗同機。
