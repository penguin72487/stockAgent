# 永豐期貨歷史下載器修復與排程

2026-09-07 的修復沿用既有下載器、來源目錄、API 共用限流器及 systemd 服務。已啟用自動更新與補查；資料是否補齊仍以每筆收據、雜湊及官方成交日期核對，不能由服務 `active` 判定。

## 修復範圍

1. **商品清單會更新。** 連續期貨每輪重讀 Contract V2；實際月份清單每小時更新。新代碼加入，舊代碼保留，不因下市、換碼或 API 暫時找不到就刪除歷史。空清單或查詢失敗不能覆蓋前一份清單。
2. **API 回空不再永久略過。** 官方有成交的空回覆採 1、2、4、7 日退避；成交狀況未知者採 7、14、28、30 日退避。沒有查詢時間的舊空收據先補查。已驗證的非空連續 Tick 不因更新而整包重抓。
3. **月份 Tick 的目標獨立核對。** 目標是有效 KBar 觀測日，加上官方資料中同商品根、同到期日的正成交量日期。不能因 KBar 沒回某天，就認定那天不需要 Tick。R1／R2 對照只使用月份契約，週契約不會擠掉近月順位。
4. **非空 KBar 也檢查漏日。** 官方有成交但 KBar 未觀測到的日期會促使對應切片重查。新回覆與已驗證的舊分鐘依 `ts` 合併，新回覆優先；較少或空白的回覆不能移除舊分鐘。收據保留新回覆筆數、前一版雜湊、缺日及下次補查時間。
5. **查詢進度和來源完整性分開。** `source_empty`、`contract_unavailable`、`query_failed` 及官方成交缺日各自保留。API 查過不等於取得市場資料，批次跑完不等於歷史已完整。

查詢、寫檔和重試共用原有限流與流量帳本。連續批次只登入一次，最多 5,000 次請求、每代碼每輪最多 32 日；未知空日每代碼每輪最多探查 2 日。月份／選擇權／指數服務保留原集合，將實際月份期貨排在前面，每批最多完成 500 次查詢。失敗會退避，連續三個契約查詢失敗會結束本批；已完成的資料與收據可續用。

## 啟用的排程

| 服務 | 時間與批次後行為 |
| --- | --- |
| `stockagent-shioaji-tx-history-backfill.service` | 每日 14:31 啟動／恢復，開機 10 分鐘後恢復；成功批次後 1 小時重新掃描 |
| `stockagent-shioaji-historical-market-data.service` | 每日 14:31 啟動／恢復，開機 12 分鐘後恢復；有待查工作則 30 秒後續批，無到期工作則 1 小時後再查 |

兩個 timer 都設為 `Persistent=true`。服務本身是可續跑的長駐監督迴圈；timer 負責每日及開機恢復，迴圈負責每批與退避。

- 平日 **07:45–14:31** 保留既有即時行情優先窗口；每次 API 嘗試前再次檢查，重試也不能跨入保護窗口。
- **14:31 先補過去已完成交易日**；全期貨當日資料要等 **16:30 以後，且官方 TX 日曆已有當日**，才可在下一輪成為截止日。股市收盤不能代替全期貨收盤。TAIFEX 黃金期貨日盤至 16:15，因此全商品採較保守的共同截止點。[TAIFEX 契約規格](https://www.taifex.com.tw/cht/2/tGF)
- 保留帳戶 **90% 歷史下載流量上限**。額度用盡就等待下一個安全窗口，再查實際 `usage()`；不假定額度一定在某時重置。
- 歷史服務採 `Nice=10`、`CPUWeight=25`、`IOWeight=25`。既有期貨五檔與 top-200 即時服務沒有重啟。
- 歷史 Tick／KBar 只用於補歷史；即時資料仍由原訂閱服務收集。[Shioaji 歷史資料說明](https://sinotrade.github.io/tutor/market_data/historical/)

## 下載前的可重現缺口

以下是截止 **2026-09-04**、新排程首次執行前的快照，不是執行後的剩餘工作量。

| 項目 | 重查結果 |
| --- | ---: |
| 舊連續清單 | 743 個 R1／R2 代碼 |
| 有官方成交卻回空的連續代碼日期 | 18,964 |
| 舊連續清單的未取得收據日期 | 20,468，集中於 14 個 API 不可用代碼 |
| 成交狀況未知、已到期可探查的連續空日期 | 424,948；不是 424,948 個已證明漏成交的日期 |
| 實際月份保留清單 | 1,824 個契約 |
| 原本未列入 Tick 目標的官方成交日 | 15 個契約日期，涉及 10 個契約 |
| 非空卻漏官方成交日的 KBar 切片 | 11 份，涉及上述 15 個日期 |
| 月份期貨待查工作 | 4,847 次：4,795 個 KBar 切片、52 個 Tick 日期 |
| 52 個 Tick 工作的組成 | 15 個新增目標＋37 個原先回空日期 |

4,795 個 KBar 工作包含 4,784 個到期空回覆及 11 個部分缺日切片。大多數空回覆尚不能判定原因，不能直接把它們稱為確定漏成交。供應商仍不提供的資料會保留 `waiting_source`，不補造價格或宣稱完整。

來源資料保持於 `data_tw_futures/shioaji_history/`、`data_tw_index_futures/shioaji_history/TXFR1/` 與 `data_tw_shioaji_history/contracts/futures/`。這三個根目前均為本機可寫來源目錄。發布仍走 catalog 包裝器；有未解查詢、官方成交缺口或失敗時，本輪不發布。另已補齊 catalog 對 `python -m downloader...` 寫入程序的偵測。

## 狀態與驗收

2026-09-07 **14:31 已實際登入、刷新清單及開始查詢**。新的 API 清單包含 **385 個商品根、752 個目前連續代碼**；保留 14 個舊代碼後追蹤 **766 個**，比原清單新增 **23 個**。清單及目前清單的 SHA-256 已驗證。台指與小台 2022-01-03 重新查詢仍為 `source_empty`，收據已記錄 9 月 8 日再試。月份服務亦已寫入新的空回覆及非空 KBar 修復收據。這是啟動與實際查詢驗收，尚非全部補下載完成。

```bash
systemctl status stockagent-shioaji-tx-history-backfill.service stockagent-shioaji-historical-market-data.service
systemctl list-timers 'stockagent-shioaji-*'
journalctl -fu stockagent-shioaji-tx-history-backfill.service
journalctl -fu stockagent-shioaji-historical-market-data.service
```

結構化狀態：

- 連續排程：`artifacts/data_repair/shioaji_futures_history/scheduler.json`
- 連續本批：`artifacts/data_repair/shioaji_futures_history/latest_batch.json`
- 月份排程：`artifacts/data_repair/shioaji_historical_market_data/scheduler.json`
- 月份下載：`data_tw_shioaji_history/summary.json`、`progress.json`、`availability/`
- 清單更新：`data_tw_futures/shioaji_contracts/manifest.json`、`data_tw_shioaji_history/inventory/manifest.json`

`scheduler.json` 提供等待原因與下次嘗試時間。連續 `batch_finished` 只是完成本輪；月份 `query_receipt_state=complete` 也只是目標都有有效回覆。仍須看 `coverage_state`、官方成交缺口與失敗數。未知空回覆一直保留可辨識狀態，不能解讀成每分鐘完整性。

測試涵蓋空回覆到期、官方根／到期日配對、週契約順位、盤中保護、額度空回覆、單次登入、多批續用、清單保留、收據查詢身分、KBar 缺日合併與發布門檻。相關回歸測試 **122 項通過**，Python 編譯與四份 shell 語法檢查通過。

## 證據

- [原始資料盤點](SHIOAJI_FUTURES_INVENTORY_2026-09-07.md)
- [連續期貨離線重查](../artifacts/operations/shioaji_futures_repair_20260907/latest_plan.json)
- [月份期貨最終離線重查](../artifacts/operations/shioaji_futures_repair_20260907/exact_futures_plan_final.json)
- [服務啟用、清單雜湊與實際查詢驗收](../artifacts/operations/shioaji_futures_repair_20260907/activation_receipt.json)
- [實際 KBar 補查、雜湊與剩餘缺日樣本](../artifacts/operations/shioaji_futures_repair_20260907/live_repair_samples.json)
- [15 個新 Tick 目標與初版待查清單](../artifacts/operations/shioaji_futures_repair_20260907/exact_futures_queue.json)：此清單先於非空 KBar 缺日修復建立，未包含後加的 11 個切片；最終總數以上一份重查為準。

排程已啟用不等於補下載已完成，也不等於 vastai1T 已同步或訓練資料已就緒；這些仍各自驗收。
