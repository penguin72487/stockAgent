# FinMind 剩餘資料、tick 效率與分階段估時

這是 2026-10-03 的工程／維運快照，不是完整歷史保證。資料清冊取樣時間為
2026-10-03 00:57:49（Asia/Taipei）；配額取樣為 00:57:00。
後續實際數字以既有 [FinMind 面板](https://penguin72487.ddnsgeek.com/finmind/) 的新取樣為準。

## 結論與剩餘範圍

清冊有 **106 個獨立資料集、132 個包含 owner 別名的面板項目**。
97 個資料集在已知排程範圍內沒有待辦，9 個仍有明細歷史候選。
這不表示每個歷史商品、欄位、修訂版本或 point-in-time 品質均已驗證完整。
已排程的主要日資料、財報、月營收、總經與新聞目前沒有歷史待辦。

以下數字是「已建待辦＋尚未建佇列的搜尋候選」，不是缺失的 tick 筆數。
全市場歷史代號及存續期間尚未完全核實，未知範圍沒有直接猜成零。

| 資料集 | 設定搜尋起日 | 已抓筆數 | 仍待請求／搜尋候選 | 優先級 |
| --- | --- | ---: | ---: | ---: |
| TaiwanStockKBar：台股分鐘 | 2019-01-01；TAIEX 2005-01-03 | 44,991 | 6,375,308 | 8 |
| TaiwanFuturesKBar：期貨分鐘 | 2011-01-03 | 26,853,606 | 6,071,181 | 8 |
| USStockPriceMinute：美股分鐘 | 2021-04-28，為設定搜尋日 | 48,177 | 36,772,879 | 8 |
| TaiwanStockTradingDailyReport：台股分點明細 | 2021-06-30 | 442,757 | 4,333,519 | 8 |
| TaiwanStockWarrantTradingDailyReport：權證分點明細 | 2023-06-21 | 48,324 | 832,551 | 8 |
| TaiwanStockPriceTick：台股 tick | 2018-12-07 | 0 | 6,430,014 | 10 |
| TaiwanFuturesTick：期貨 tick | 2011-01-03 | 0 | 6,217,912 | 10 |
| TaiwanOptionTick：選擇權 tick | 2011-01-03 | 0 | 1,524,280 | 10 |
| TaiwanFuturesSpreadTick：期貨價差 tick | 2026-04-27 | 0 | 159 | 10 |

**正式 tick 歷史尚未輪到，不是正在下載但只有 0 筆。**
仍依原先要求，先取得非 tick 必要資料；tick 保持最低優先級。
本次價差 tick 探測有取到資料，但探測收據不冒充正式歷史下載完成。

完整 106 項逐項清冊包含最早／最新觀測日、筆數、bytes、owner、狀態、
請求形狀、候選數與發布時鐘：

- [inventory.csv](../artifacts/data_quality/finmind_tick_efficiency_2026-10-03/after/inventory.csv)
- [inventory.json](../artifacts/data_quality/finmind_tick_efficiency_2026-10-03/after/inventory.json)
- [目前尚未取得及分階段估時](../artifacts/data_quality/finmind_tick_efficiency_2026-10-03/after/inventory.md)

部分已抓資料例子：台股日價 101,099,586 筆，1994-10-01 至 2026-10-02；
新聞 4,224,129 筆，2000-03-31 至 2026-10-02；
財報 2,986,030 筆，1990-03-31 至 2026-06-30。
首末日是資料集總體首末日，不代表每個商品都有相同期間或每個中間日期皆有值。

## 為什麼 tick 要很久

已確認帳號為 Sponsor，三個 worker **共同分享 6,000 次／小時**；
取樣的滾動一小時實發為 5,245 次，不是三個 worker 各拿 6,000 次。
股票普通 tick 及目前使用的普通期貨／選擇權歷史形狀是單商品、單日。
把 end_date 拉長或開更多執行緒不能將這種端點變成全年全市場查詢。
依據：[台股官方文件](https://finmind.github.io/tutor/TaiwanMarket/Technical/)、
[衍生品官方文件](https://finmind.github.io/tutor/TaiwanMarket/Derivative/)、
[方案與配額](https://finmind.github.io/Pricing/)。

只算現在的 68,557,803 個候選，假設每個都要一 call、獨占 6,000/h、
不停機且沒有追新／重試，算術時間為：

`68,557,803 / 6,000 / 24 = 476.10 天`

只算 tick 的 14,172,365 個候選則為 **98.42 天**。
這是固定候選計畫的理想情境，不是全來源資料完整性的時間下界：
未來可能證明更多日期無須查詢，也可能發現更多歷史商品、來源缺口或重試。
它也不是現在排程的完成日期，因為 tick 還要等前面的工作。

目前模型已計入原先優先序、到期追新、已知身份的新分區、週期更新與開盤保護：

| 階段 | 現有候選 | 最快情境完成 | 中間情境完成 | 保守情境完成 |
| --- | ---: | --- | --- | --- |
| 已到期追新、主要歷史／日資料／財報／總經／新聞 | 當時 0 | 已知佇列無待辦 | 同左 | 同左 |
| 五類分鐘／分點明細 | 約 5,439 萬 | 2028-08-24 | 2029-02-08 | 2029-04-07 |
| 最低優先 tick，包含前階段等待 | 約 1,417 萬 | 2029-04-17 | 2029-12-16 | 2030-03-12 |

以上均為 Asia/Taipei 日期。長期模型對身份存續期、未來交易日、
來源延遲與未來新商品仍有不確定性；這三種情境不是信賴區間或完工保證。
ETA 每分鐘依實發流量與工作分母更新，不能把年級工作的小幅吞吐變動當成精確日期。

## 本次實作與證據邊界

沿用 Complement 下載器、SQLite frontier、共同配額及公開唯讀 gateway，
沒有新增另一條下載管線，也沒有提升帳號方案。

1. **休市日使用小型官方證據集合，不建代號 × 休市日巨量佇列。**
   重用 Sponsor 的 TWSE exact-byte receipt 驗證器。證據涵蓋 1999-01-05 至
   2026-10-02，3,258 個閉市日期。只套現貨股票、分點及權證分點，不套期貨夜盤日期。
   不是只看星期六／日：若官方證明星期六開市，仍保留。
   若證據改版／失效則重開候選並回退游標；如果既有非空資料與閉市證據衝突，
   停用該資料集的排除。原始資料與舊收據不刪除。
2. **已建佇列與未建 frontier 分母互斥。**
   部分歷史期貨分鐘早於 lazy cursor 已下載，原估時仍把它們多算。
   新計數扣掉相同身份／分區的既有任務，休市排除和任務去重也不重複相減。
3. **價差 tick 整日取得全商品。**
   2026-10-02 一次取得 21,173 筆；CAF 子集 135 筆與單商品查詢精確相同。
   2026-06-12 一次取得 108,417 筆、309 個商品；BRF 子集 2 筆也精確相同。
   比對保留重複筆數，並允許價差的負價格。
   正式形狀改成 `whole_market_day`，不再乘上 1,081 個 master 身份；
   此次計畫 **171,879 → 159** 個請求候選，減少 99.91%。
   128 個舊形狀任務已完整封存，標為 `deprecated_query_shape` 而非偽稱已下載；
   舊 frontier 留作證據但不再乘入 ETA 或未來追新流量。
4. **拒絕不等價的批次形狀。**
   兩種資產交換資料的無 ID 整市場請求僅回最新日；
   固定收益抽樣是 1 筆 vs 單商品歷史 12 筆，選擇權是 1 筆 vs 58 筆。
   因不等價，仍保留各商品完整歷史請求，不用最新快照冒充歷史。
5. **追新工作集不因歷史工作集滿而消失。**
   每資料集歷史 working set 128；允許至多額外 128 個 forward 待辦。
   總體有界，不展開數千萬任務；維持資料集優先級，因此未來 tick 不會越過非 tick。
   發布前不建當日分區，發布邊界測試涵蓋 15:49 / 15:50。
6. **契約與顯示同步。**
   Supplemental contract 3、frontier estimate contract 3、ETA snapshot contract 5；
   公開 DTO 保留原始候選、已證實休市排除及既有任務去重數。
   清冊按 canonical owner 選完整歷史 row，不能誤選只代表最新回應的 observer row。

### 固定快照量測

同一份 322,014 任務的 SQLite 備份，取樣 2026-10-03 00:50:17：

- 原候選計畫 77,914,632 → 修正後 68,558,463，減少 **9,356,169（12.01%）**。
- 其中 9,052,248 是休市候選／佇列展開，132,201 是已建任務的估時重算；
  **兩者不是實測省掉等量 API 呼叫**。舊日曆本來也會將已有證據的休市任務標成 non-session。
- 171,720 是價差 tick 換成整日形狀後的請求候選差異。
- 136,267 個非空任務含收據指標的 checksum 完全不變；production queue 寫入 0、provider call 0。
- 新計畫建構約 0.515 秒，frontier 狀態查詢中位數約 0.411 秒。
  這不是下載實發吞吐提高 12.01% 的證明。

量測收據：[fixed_plan_benchmark.json](../artifacts/data_quality/finmind_tick_efficiency_2026-10-03/fixed_plan_benchmark.json)。
既有流量控制沒有繞過 provider、沒有把空回應當非空資料，也沒有猜上市／下市期間直接刪工作。

## 部署與驗收

2026-10-03 00:53:15 重新啟動既有 Complement 與公開 gateway；Free／Sponsor 沒重啟。
分鐘配額 sampler 自然更新 ETA，沒有為了面板額外呼叫資料端點。

- 最後 874 個 FinMind／日曆／gateway 廣泛測試通過（58.28 秒），另有 125 個 focused tests 通過。
- 00:56:28 驗證到部署後真實非空分點收據：00989A、2026-09-30、115 筆；
  Parquet SHA、列數與 contract 3 全部一致，不只依 systemd active 判定。
- 正在負責的佇列無 failed／not-entitled／invalid-request。
  另保留一筆已委派給 Sponsor 的舊 504 失敗記錄，不抹除歷史錯誤。
- 公開 HTTPS 在 1440、1024、390 寬度通過：API 200、無 JS 錯誤／水平溢出、
  進度分母一致，瀏覽器沒有呼叫 FinMind provider。

驗收收據：[runtime_acceptance.json](../artifacts/data_quality/finmind_tick_efficiency_2026-10-03/runtime_acceptance.json)、
[browser_acceptance.json](../artifacts/data_quality/finmind_tick_efficiency_2026-10-03/browser/browser_acceptance.json)。

## 尚未解決／不能冒充已補齊

- 官方已載明股票 tick 2018-12-22、2019-02-20～22，以及部分 2019-05-16 ETF 缺失；
  選擇權 tick 2019-01-16～2019-06-30、價差 tick 2026-06-12 以前也有來源不完整說明。
  新舊日單商品 parity 只證明請求形狀等價，不能補出官方原本沒有的筆數。
- 現有全市場 master 不足以證明完整歷史存續期。未知美股／衍生品閉市規則不套台股日曆。
- 普通全市場 tick 批次與更高請求配額需要相應 SponsorPro 權限；
  本次未自動付費、升級，也沒有以 Sponsor 憑證試呼叫已知受限端點。
  若仍要保留最大範圍，這是下一個結構性加速選項，不是更多執行緒可以替代。

## 可重跑入口

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.audit_finmind_remaining --output artifacts/data_quality/finmind_remaining_current
run_fintech_python -m scripts.benchmark_finmind_history_plan --output artifacts/data_quality/finmind_plan_current.json
run_fintech_python -m scripts.verify_finmind_history_efficiency \
  --since 2026-10-02T16:53:15+00:00 --output artifacts/data_quality/finmind_runtime_current.json
```

上述入口不呼叫 provider 資料 API；量測只改一次性 SQLite 備份。
實際下載仍由 `stockagent-finmind-complement.service`、
`stockagent-finmind-sponsor.service`、`stockagent-finmind-free.service` 負責。
