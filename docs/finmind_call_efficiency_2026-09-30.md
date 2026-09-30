# FinMind 請求最小化：2026-09-30

本輪以「相同必要資料覆蓋，減少 HTTP 請求」為目標，不以加執行緒、刪除
未知歷史或把回空當完整來降低數字。沿用既有三個 worker、共享配額、排程、
不可變原始檔與收據。沒有變更交易、模型 ABI 或來源數值。

## 官方約束與選擇

- 帳號實測 Sponsor，官方回報 6,000 次／小時；不是每個 worker 各 6,000 次。
- [OpenAPI](https://api.finmindtrade.com/openapi.json) 的 `data_id` 是可選欄位，
  但不同 dataset 仍有自己的限制，不能推論所有表都支援全市場日期區間。
- [台股技術資料](https://finmind.github.io/tutor/TaiwanMarket/Technical/)與
  [期貨資料](https://finmind.github.io/tutor/TaiwanMarket/Derivative/)的分鐘／
  tick 單次單商品、單日查詢仍保留。全市場物件下載是 SponsorPro 權限，
  本輪沒有嘗試繞過、也沒有購買新方案。
- 美股分鐘以 `^DJI, 2026-09-23..25` 附加 `end_date` 的一次探測被拒；
  沒有把同一錯誤查詢放進自動重試。既有單日形狀保留。
- 官方 `/datalist` OpenAPI 只列七個匯率／總體資料集，不支援股票分鐘與 tick；
  沒有對全部 dataset 盲打此端點。

## 修正

### 可轉債月份分析：逐券改全市場完整歷史

`TaiwanStockConvertibleBondMonthlyAnalysis` 原來由 1,846 個候選債券逐一查詢。
實測省略 `data_id` 即可取得多月份與全部返回債券，不需要用當前主檔篩掉
歷史代碼。這是查詢方向修正，不是減少 feature 或縮短歷史。

- 保留 2026-05-01 的官方起點，每次查到執行當天；歷史新回補／修訂也包含在內。
- 根據[官方可轉債文件](https://finmind.github.io/tutor/TaiwanMarket/ConvertibleBond/)
  的週一至六 18:00 更新時間，排下一次全表更新，不再逐券每天重抓。
- 既有逐券 queue 精確舊內容寫入 `finmind_query_shape_migrations`，標記
  `deprecated_query_shape`；不刪檔、不把舊任務標為下載成功。
- 相同全表完整性需求由一個 canonical `id_history`（空 ID）任務負責，
  收據記錄新查詢契約 v2。未知 provider 完整性／PIT 不因此被宣告通過。
- 仍有 64 MiB 解碼回應與 100 萬列守門；超限報錯，不能截斷後假裝完整。

單輪初始排程比較為 **1,846 → 1 次，減少 1,845 次（99.946%）**；這只適用
此資料表，不是全 FinMind 流量下降比例。契約探測另計，不混入正式下載節省。

### 11 類已驗日期區間：把到期重試與更新也合批

Sponsor 原先只向較早方向合併 pending 鄰居。現在可包含已到期的
failed／complete／observed_empty，並從 seed 兩側合併連續範圍。
共用 helper 保留 pending-only 的相容預設，worker 明確啟用到期更新合批。

- 只有實際到期的 task 可加入；未到期的完成區間、冷卻缺口不會被跨過。
- 共享額度保留不足時，追新請求不夾帶歷史。
- 原子 claim、全回應驗證、原始值／重複列保留、逐分區收據仍沿用原實作。
- 過大回應／逾時依實際失敗縮批，不加入任意五年上限。
- ETA 使用同一規劃器，三個相鄰到期年度由三次改為一次；不可只改顯示分母。
- 合批契約升為 v3；不宣稱尚未發生的更新批次已實際節省流量。

### 帳號流量取樣：跨程序共用一次／分鐘

Free、Complement、Sponsor、每分鐘 quota sampler 原先各有自己的記憶體快取。
改用同一 root 的 process-shared `flock` 與 credential-bound 快取。

- 同一 token 在觀測成功且有效時，同一分鐘的並發呼叫共用一個 `/user_info` 網路請求。
- 以實際觀測時刻分分鐘，不因讀取快取更新 `observed_at_utc`。
- 換 token、跨分鐘、時鐘異常、資料損壞均重新驗證；失敗不冒用過期額度。
- 快取只含雜湊與白名單 quota 欄位，沒有 token、email 或完整帳號回應。
- 共享 data API 限流與 402／429 冷卻不變；此項省的是重複帳號查詢，
  不把它算成 `/data` 已取得資料量。

### 已支出的預留份額不再重複扣除

海外日資料待更新的候選數很大，原排程每輪都保留每小時額度的四分之一，
即使過去一小時該類請求已超過這個份額。2026-09-30 01:06 台北時間讀取時，
UKStockPrice 已支出 3,558 次，但排程仍替海外資料另外保留 1,500 次，
使其他歷史補齊工作無法使用實際剩餘額度。

- 改成 `max(0, 海外保障份額 − 過去一小時該類已發請求)`，所有 worker 合算一次。
- 台股具名追新、未來一小時發布需求、Free 基本檢查及在途額度照常保留。
- 本地 ledger 缺漏／損壞不抵扣預留；仍以官方回報用量加觀測後本地請求決定能否發送。
- 滑動一小時查詢沿用既有時間索引，不掃全歷史 ledger。這是修正本地分配，
  不是提高官方上限，也不假定整點一定重置。

## 驗證與可重現指令

2026-09-30 01:12–01:15 台北時間完成本輪正式驗收：

| 證據 | 結果 |
| --- | --- |
| 可轉債全市場查詢 | 1,527 筆、434 個來源返回代碼；資料日期 2026-05-01 至 2026-08-01 |
| 逐券與全市場對照 | `13166` 四個月所有欄位的 multiset 一致，沒有去重或改值 |
| 結束日測試 | 查到 2026-07-01，回 1,143 筆；含七月、不含八月，確認不是忽略 `end_date` |
| 正式下載 | 一次請求寫入 1,527 筆；Parquet 28,519 bytes；收據大小／列數／SHA-256 通過 |
| 排程遷移 | 1 個全市場任務 complete；1,846 個舊逐券任務保留為 `deprecated_query_shape`，未冒稱成功 |
| 下次追新 | 2026-09-30 18:00 台北時間；來源未提供九月值，不補造九月資料 |
| worker | Complement／Sponsor／Free 均於 01:12:32 載入新程式；截至觀測時無自動崩潰重啟 |
| 重啟後真實活動 | 已發 100 次五秒指數、99 次英股日資料、29 次新聞請求；另確認新成功收據持續產生 |
| 唯讀面板 | 全市場任務 1／1、1,527 筆與起訖日期正確；`all_history_complete_claim=false` |
| 測試 | 645 項 FinMind 相關測試通過；compileall 與限定檔案 diff whitespace 檢查通過 |

共享預留的新 ledger 查詢在本機 20 次暖讀量測：中位數 **3.18 ms**、最大
**7.51 ms**，查詢計畫使用既有時間索引。這不是全系統吞吐量基準，也不把
測試通過當作所有來源資料完整。原始 Complement queue 尚有一筆過去
`TaiwanDailyShortSaleBalances/http_504`，該 dataset 已委派 Sponsor；沒有為了
消掉非主責舊錯誤而重開重複下載。Sponsor 原始 queue 觀測時沒有失敗任務。

本輪診斷實際消耗：5 次有保存雜湊證據的查詢（可轉債全表／單券／區間邊界，
兩個交換資產表的全市場單日探測），以及 1 次被拒的美股分鐘跨日探測；正式
可轉債下載另計 1 次。因預留流量而暫緩的診斷未發送 data API。

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q test/test_finmind*.py test/test_probe_finmind_max_ranges.py
run_fintech_python -m scripts.audit_finmind_call_efficiency
run_fintech_python -m scripts.audit_finmind_call_efficiency --runtime
run_fintech_python -m scripts.audit_finmind_query_ranges
```

前兩個 audit 只讀本機證據，沒有發 API。必要的有限探測使用
`scripts.probe_finmind_call_efficiency.py`，共用正式 limiter 與追新預留判斷；
相同探測會讀取舊結果，不重複扣配額。原始回應只存私有 diagnostics 路徑。

證據目錄：[`finmind_call_efficiency_2026-09-30`](../artifacts/data_quality/finmind_call_efficiency_2026-09-30/)。

- [`range_acceptance.json`](../artifacts/data_quality/finmind_call_efficiency_2026-09-30/range_acceptance.json)：區間與逐券等價驗證。
- [`runtime_acceptance.json`](../artifacts/data_quality/finmind_call_efficiency_2026-09-30/runtime_acceptance.json)：正式資料、遷移、下次排程。
- [`service_acceptance.json`](../artifacts/data_quality/finmind_call_efficiency_2026-09-30/service_acceptance.json)：程序啟動、來源 SHA、面板與真實請求活動。
- [`全資料集查詢清冊 CSV`](../artifacts/data_quality/finmind_call_efficiency_2026-09-30/finmind_query_ranges_20260929T171314469464Z.csv)：106 個已註冊資料集的主責、可查範圍、查詢方式與 queue 狀況；132 個 owner 別名不重複算為資料集。

## 未被省略的範圍

新聞與高容量分鐘／tick 仍保留。沒有以少量空回應逆推最早日期；沒有將
美股 IPO 年、當前商品主檔、日 K 缺列直接當成「以前所有分鐘資料不存在」的
證明。台股夜盤的曆日也沒有套現貨休市規則。

全歷史候選工作包含尚未核實的日期與生命週期，因此本輪不是全域理論最少
請求數的證明。分點資料另有逐券商方向，但歷史券商全集與既有逐股票覆蓋
的等價性尚未證明，本輪未用更小的代碼數冒充完整覆蓋。

## 17:38 追加：滾動流量對帳與順序 ETA v3

這次修正的是估時計算與既有 `/finmind/` 面板；沒有改資料範圍、下載優先級、
API 限速或交易服務。沿用既有資料品質檢查與儀表板流程，將觀測工作量、
未來負載模型、來源候選和真正完成收據分開。

### 不一致的原因與新口徑

舊面板使用滾動 60 分鐘請求數，ETA 中間情境卻採對齊時鐘的 15 分鐘活躍
區間 P50，再扣除全部未來週期負載。兩者既不是相同時間窗，也不是相同
物理量；UI 沒有顯示扣除算式。更重要的是，舊的 core/non-tick/all 估時
未納入目前 `finmind_priority_tasks` 指定的期貨分鐘前置工作，並提前把低
優先級分鐘／tick 負載扣到前面的階段。

- 中間速度改用同一觀測時點 `(now − 60 分鐘, now]` 的實發數，包含閒置。
  滿一小時才採用，不再偷偷退回前一天的活躍區間中位數。
- 淨回補速度為 `max(0, min(實發速度, 共用限速上限) − 本階段未來追新模型)`。
  最快用目前共享限速；保守用完整 15 分鐘區間 P10，包含零流量，不快於中間。
- 當下預留量是 dispatch 守門，未來每小時追新量是容量模型，不能重複相減。
  已到期欠帳列入有限工作量一次，未來負載按實際任務優先級分開。
- 目前第一階段只承擔 priority 0 的持續追新與指定的有限回補，不提前扣
  普通背景刷新／分鐘／tick。後續階段再逐步加入應承擔的負載。
- 分鐘／tick 等待前段時新增的日分區，以既有 frontier 代號數與日粒度推估，
  另外列成 `forecast_arrival_requests`；不算成取樣時已確認的缺漏。
- 順序為「當前到期追新／指定回補 → 主要歷史／新聞 → 其餘分鐘與分點 →
  tick → 校驗」。同一帳號容量只用一次；每階段顯示已知請求、本段工時、
  開始時間及三情境的累計完成時間。worker 可以交錯工作，這是條件式容量
  排程，不宣稱它們被改成互斥串行。
- 重試等待可以和前段耗時重疊，不重複累加；未知配額等待／外部校驗准入
  不捏造完成日。僅當 FinMind 自身前置工作是唯一准入條件時，才條件式
  推估排在必要工作之後的校驗。已無已知校驗請求時不額外延後總時間。
- 開盤保護沿用既有日曆；超過一年用平日投影，超過十年不提供絕對日期。
  快照過期五分鐘即隱藏估時；公開投影只輸出白名單欄位。

### 當次線上驗收快照（台北 2026-09-30 17:38）

滾動實發 **5,332 次／小時**，第一階段未來追新模型 **1,023 次／小時**，
淨回補速度 **4,309 次／小時**。當下 quota 預留為 **39 次**，並未再次扣除。
官方帳號用量是獨立的 `/user_info` 觀測，不假定和本站滾動窗相同，參見
[官方用量文件](https://finmind.github.io/api_usage_count/)。

| 階段 | 取樣時剩餘請求 | 最快累計完成（台北） | 中間累計完成（台北） | 保守累計完成（台北） |
| --- | ---: | --- | --- | --- |
| 到期追新／指定期貨分鐘回補 | 114,194 | 2026-10-01 17:52 | 2026-10-01 20:58 | 2026-10-01 21:22 |
| 主要歷史／日資料／財報／總經／新聞 | 39,747 | 2026-10-02 02:34 | 2026-10-02 06:54 | 2026-10-02 07:29 |
| 其餘分鐘與分點候選 | 60,222,698 | 約 2028-10 | 約 2029-03 | 約 2029-04 |
| tick 候選 | 17,564,726 | 約 2029-08 | 約 2030-04 | 約 2030-05 |
| 跨來源校驗 | 0 | 目前不增加工時 | 目前不增加工時 | 目前不增加工時 |

合計已知請求 **77,941,365**，每個請求只歸入一個階段。以上是當次取樣的
條件式投影，不是保證：分鐘／tick 分母仍包含「代號 × 日曆天」搜尋候選，
尚未完全排除上市前、休市日與不適用商品。長期負載也未包含未來新增商品、
供應商變更、所有重試與來源退役。不能將數年遠期投影當成可信交付期限，
也不能把請求開始數當成成功率或資料完整證明。網頁每分鐘更新，不用前端
假倒數；流量卡採網頁讀取時刻，會比 ETA 快照新數秒至一分鐘。

### 修改位置與驗證

- `downloader/finmind_eta_work.py`：同一唯讀 SQLite 快照取得有限優先回補；
  不將它再加到資料集分母，合批增量的未合批成本另行保留。
- `downloader/finmind_eta_telemetry.py`：滾動窗與含閒置速度、分優先級的未來負載。
- `downloader/finmind_eta_stages.py`：沿用既有 `estimate_completion`／日曆的
  順序估算層；內層估算契約 v3，外層收據讀取格式保持 v1 相容。
- `stockagent/live/finmind_eta_projection.py` 與既有 FinMind 網站：公開對帳與
  階段表，不在網頁請求路徑重新掃 queue 或呼叫供應商。
- 最新 **662 項 FinMind／acquisition policy 測試通過**；另 **4 項公開網站
  FinMind 整合測試通過**。涵蓋合批分母、冷卻重疊、配額阻塞、有限前置工作、
  下游新增候選、過期／秘密欄位防護及 390／1440 px 真實 Chromium 渲染。
- 廣泛網站回歸另有一項既有 `test_day_trade_clock_ends_at_auction_without_overnight_contract_details`
  失敗：當沖收盤文案已是另一研究規則，斷言仍要求舊文案。本次未修改該功能。
- 17:37:53 僅重啟唯讀 `stockagent-public-dashboards.service`；三個 FinMind
  worker 的 PID（Complement 96883／Free 213／Sponsor 218）前後一致，未重啟。
- 每分鐘估算維持既有 timer；近期單次約 4.4–5.9 秒、約 103 MiB 程序峰值，
  不使用 GPU。這是取樣觀測，不是整體下載效能基準。

線上只讀 API 驗收與瀏覽器證據：
[`finmind_eta_2026-09-30`](../artifacts/data_quality/finmind_eta_2026-09-30/)。
`local_acceptance.json`／`public_acceptance.json` 保留同一 17:38 快照與逐階段
請求數、時鐘對帳；`browser_acceptance.json` 與圖片保留實際桌面／手機畫面。

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.snapshot_finmind_quota --eta-only
run_fintech_python -m pytest -q -s test/test_finmind* test/test_acquisition_policy.py
run_fintech_python -m pytest -q -s test/test_public_dashboards.py -k 'finmind or quota or completion'
```
