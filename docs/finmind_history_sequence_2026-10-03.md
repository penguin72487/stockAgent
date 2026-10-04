# FinMind 九類歷史回補順序與進度面板

2026-10-03 實作與部署驗收。最新使用者設定是「美股分鐘 K 放最後」。
這是指定九類歷史回補的排序調整，不代表所有 FinMind 歷史已下載完成。

## 最終順序

| 順序 | 資料 | FinMind dataset |
| ---: | --- | --- |
| 1 | 台股分鐘 K | TaiwanStockKBar |
| 2 | 期貨分鐘 K | TaiwanFuturesKBar |
| 3 | 台股分點明細 | TaiwanStockTradingDailyReport |
| 4 | 權證分點明細 | TaiwanStockWarrantTradingDailyReport |
| 5 | 台股 tick | TaiwanStockPriceTick |
| 6 | 期貨 tick | TaiwanFuturesTick |
| 7 | 選擇權 tick | TaiwanOptionTick |
| 8 | 期貨價差 tick | TaiwanFuturesSpreadTick |
| 9 | 美股分鐘 K | USStockPriceMinute |

到期追新及原有有限指定修補仍可優先插入；主要日資料、財報、總經與新聞沿用
原有前置優先級。已走過階段的到期重驗仍可執行，剩餘跨來源校驗保留最低順位。
本次不調整查詢粒度、下載範圍、來源數值、既有收據或 Sponsor 共享配額。

## 根因與共用修正

原本明細與 tick 的同一優先級輪轉，不能表達使用者指定的九類序列。
只排序畫面也不能改變實際 downloader 或累計完成時間。

- `downloader/finmind_history_order.py` 統一定義資料集、順位與階段 key。
  Worker、ETA、公開投影與清冊共用這份定義，前端沒有第二份排序常數。
- Worker 使用有界只讀查詢辨認最早未完成歷史階段；未物化 frontier、處理中與
  等待重試也算未完成。128 筆工作集清空不能被誤認為完整歷史已抓完。
  後續歷史不能跨過此前階段；到期追新、主要資料及有限修補沿用原有入口。
- ETA snapshot 與 stage schema 升到 v6，分開九類維護負載與新分區抵達率，
  每一階段加入前置階段耗時、追新負載、配額等待及可驗證的重試時間。
  缺乏成功吞吐／可恢復證據時顯示未知或等待重試，不用官方上限冒充實際速度。
- v6 公開投影核對整份順序及 stage keys，拒絕把舊順序的完成日期只換標籤。
  Worker status 的 `dispatch_contract_version=3` 與順序一致時，面板才顯示已套用。
- 面板顯示前置追新、主要歷史、九類歷史與校驗，共 12 階段；九類各有獨立進度條。
  進度為 `checked_partitions / target_partitions`，另列非空成功分區與真實資料筆數。
  完成查驗的空回應不是有值資料；搜尋候選數不是缺漏 K 棒／資料列數。

## 當次驗收

2026-10-03 08:41 台北時間，只重啟既有 Complement downloader 與公開唯讀 gateway。
沒有重啟交易引擎、重新登入券商、重設配額或修改來源資料。

- 全 FinMind、公開 dashboard 與 data-monitor 相關回歸：963 passed，102.54 秒。
- 順序、lazy frontier、重試守門、逐階段 ETA、共享流量、UI 與清冊聚焦：99 passed。
- 公開 HTTPS 瀏覽器：1440、1024、390 三種寬度，12 階段／9 進度條，
  順序、比率、篩選、零頁面水平溢出、零 JS error、零瀏覽器 provider API 呼叫通過。
- 只讀 runtime 驗收：新 dispatch contract 生效，背景首個未完成歷史是台股分鐘 K；
  public 與 ETA v6 順序一致。重啟後 UKStockPrice 的一筆 2,193 列新收據通過
  SHA256 與 Parquet footer 列數核對。
  當時主要／到期日資料仍可優先，不能把這筆英股日資料說成台股分鐘下載進度。
- 當次整體 ETA 是 `waiting_retry`，仍有未恢復的失敗；這次驗收不宣稱修復這些
  來源失敗或所有資料已抓完，也不提供無根據的絕對完成日期。

固定唯讀 queue 複本的排程微基準：359,702 個任務，5 次量測；相對 HEAD 基準，
wall median 0.16665 → 0.09839 秒，約下降 41%。這含此前未提交的索引／排程修改，
不是純本次排序的因果歸因，也不是 API 下載吞吐改善的證明。沒有 provider 呼叫
或 production queue 寫入。

驗收證據：

- [Runtime 收據與九類實際分母](../artifacts/data_quality/finmind_history_sequence_2026-10-03/runtime_acceptance.json)
- [公開瀏覽器驗收](../artifacts/data_quality/finmind_history_sequence_2026-10-03/browser/browser_acceptance.json)
- [來源／未完成工作清冊](../artifacts/data_quality/finmind_history_sequence_2026-10-03/inventory/inventory.md)
- [唯讀 dispatch 微基準](../artifacts/data_quality/finmind_history_sequence_2026-10-03/dispatch_benchmark.json)
- [回歸測試 log](../artifacts/operations/agent-workflow/runs/regression-20261003T003827-fc8857bd/run.log)

## 最佳五檔：可取得性與目前資料要分開

[Shioaji 股票即時串流](https://sinotrade.github.io/tutor/market_data/streaming/stocks/)
支援 BidAsk 五檔，既有股票 Top-200 及選定期貨／選擇權 collector 已保存五檔事件。
此次各取一個本機 Parquet footer，只驗證買賣價格／數量 1–5 檔欄位存在，
樣本各 104 與 332 列；不是全市場、多年歷史、每秒完整度或目前串流健康的證明。
本次沒有新增或擴大五檔蒐集任務。

[FinMind 目前即時資料文件](https://finmind.github.io/tutor/TaiwanMarket/RealTime/)
列出的股票及期權快照是單一 `buy_price/buy_volume/sell_price/sell_volume`，不能
當成五檔或歷史五檔。舊 v3 文件曾列 `TaiwanStockPriceMinuteBidAsk`，但不能據此
假設目前 v4 存在可回補的全市場歷史端點或帳號有權限。Tick／K 棒也無法反推
沒有觀測過的五檔委託簿。
