# 2026-10-06 當沖開盤延遲：來源身分快取與持久發布

本輪沿用 `tw-daytrade-opening-latency-20261005` 工程任務，範圍是
09:00 → 四模式完整訊號 → 因果報價 → 紙上帳本持久提交。
沒有恢復先前暫停的全服務／儲存整理目標，也沒有更換模型、刪除股票、
降低訊號完整性、重寫歷史成交或接入券商委託。

## 結論與真實基線

使用當天原始 opening attempt 與 `latency.jsonl` 的相同 `signal_id`
收據，不使用 UI 秒級顯示、歷史 logical `entry_completed_at` 或快取命中來冒充實測。

| 10/06 真實邊界 | 自 09:00 起 |
| --- | ---: |
| 首個行情 callback 本機收到 | 994 ms |
| 完整模型輸入所需行情覆蓋 | 1,799 ms |
| 首個不可變訊號 ready | 2,387.570 ms |
| 四模式全數 ready | 3,802.992 ms |
| 首個紙上帳本持久提交 | 3,216.965 ms |
| 四模式紙上帳本全部持久提交 | 6,072.713 ms |

因此畫面上的 `09:00:06` 不能直接解讀為模型推論花六秒。
9:00 到第一個訊號、全部訊號，以及全部進場帳本，是不同終點。
目前仍是本地紙上帳本；Shioaji 提供行情，並非 Shioaji StockDeal 或交易所成交。

各模式行情／計算會重疊，不能把每個模式的排隊時間、行情子階段與父階段全加起來。
完整訊號／帳本時間分別是所有必要模式的 `max(ready_at)`／
`max(ledger_persisted_at)` 減 09:00。對照測試的節省不能直接從 6,072.713 ms
扣除，宣稱成為下一次真實開盤的結果。

昨日同步 usage 的開盤阻塞本次已不再出現：取價前、後 usage spans 均為零，
但流量位元組仍是「未歸因／未知」，不是免費或零流量。
參照 [10/05 原始修正與驗證](tw_day_trade_opening_latency_2026-10-05.md)。

## 找到的成本與共用修正

1. 當天首模式在 09:00 後仍等待約 618.6 ms 的 realtime preparation。
   四模式重做已驗證設定、同一個股票目錄 inventory 與 benchmark Parquet 日期 metadata。
2. 每個模式完成訊號註冊後，要持久發布完整 state、positions、status、service-sync。
   縮排 JSON 使標準 encoder 走大量 Python 遞迴；這不是金融計算。

修正沿用原有 canonical 實作，不增加第二條訊號或下單路徑：

- canonical `load_config` 可選擇收集整個繼承圖的來源身分。
  一般呼叫的解析、驗證、預設與模型／執行欄位保持相同。
- Runtime freshness 只重用完成完整驗證後的不可變資料位置與 benchmark 名稱。
  根設定、父設定或祖先設定修改、替換、刪除及解析失敗都會拒絕舊值。
- Parquet 日期純量以路徑、device、inode、size、mtime_ns、ctime_ns 與日期粒度為鍵。
  成功讀取後再確認身分；讀取失敗及讀取中替換不會成為負快取。
- 股票檔名 inventory 以目錄身分綁定不可變 tuple；增／刪／改名使其失效。
  目錄快取只綁檔名，不綁檔案內容；檔案內更新由 per-file 日期身分另行檢查。
- 移除同一個 freshness 呼叫內重讀 benchmark 日期的工作。
  市場時鐘、交易日、expected latest date、enabled 與 checkpoint 狀態仍每次重算。
  不使用畫面 TTL cache 通過開盤守門。
- 完整狀態改為精簡 JSON；每個欄位、Unicode、float 精度、大整數與原有 default=str 保留。
  檔案 fsync、原子 replace、目錄 fsync、四個公共視圖及最後 service-sync 提交順序均保留。

既有 startup／盤前／最後武裝都呼叫同一份 runtime status，所以第一次來源驗證可以
在 09:00 前完成。重開服務或來源變更的冷檢查仍必須付出真實成本，不把它標為熱快取。
檔案身分只是本地快取失效依據，不是來源內容的密碼學驗證、發布收據或 point-in-time 證據；
原有資料與 checkpoint 契約仍獨立適用。

## 成對測速

來源固定於該次實驗，控制／候選在同一程序交替 ABBA，每組保留八個完整樣本。
未發 provider 請求，未寫生產訊號或帳本，亦沒有排除慢樣本。

| 完整實驗邊界／中位數 | 控制 | 修正 |
| --- | ---: | ---: |
| 四模式完整 runtime status 並行檢查 | 186.299 ms | 17.638 ms |
| 完整 state 與四個公共視圖持久發布 | 249.118 ms | 158.790 ms |

每次 status 的完整 DTO digest 必須一致；所有部署模型／執行設定亦測試逐欄一致。
四份持久視圖的所有值與 revision 必須一致；監控 projection 的實際檔案身分
先通過驗證，才在跨實驗語意 digest 中排除必然不同的 inode／大小／時鐘。

第一版只處理設定與日期純量，並行目錄重掃仍在；一輪中位數反而是
159.863 → 255.432 ms。保留該輪結果，補上目錄 inventory 身分綁定後才得到上表。
首輪持久發布成對結果 289.049 → 251.895 ms 亦保留，不能只展示較有利結果。
共同 I/O 與 CPU 負載會影響絕對值；profile 模式另標記，不能混入未 profile 測速。

證據位於 `artifacts/operations/opening_latency_20261006/`：

- `preparation_paired.json`：未接受的第一版性能結果。
- `preparation_paired_v2.json`：完整來源綁定、並行四模式 status 對照。
- `persistence_paired.json`、`persistence_instrumented_paired.json`：完整狀態、四視圖與耐久性對照。
- `runtime_status.prof`、`persistence_profile.*.prof`：診斷用 profile，非未加 instrumentation 的耗時。
- `before_deployment.json`、`after_deployment.json`：真實開盤收據、下單／成交／測速檔案雜湊及部署驗收。

`before_deployment.json` 的早期 `api_ms` 同時計入其後的 ledger hash 讀取，不作 API 測速使用。
`after_deployment.json` 的 API 計時只包住 HTTP 回應及解碼。

## 持續分項測速

每次成功完整發布記錄實際 revision、內容 projection/fingerprint 耗時，及每個文件：

- JSON 編碼；
- 寫入／檔案 fsync；
- 原子替換／目錄 fsync；
- 完整發布總耗時。

結果為 `state_publication` 寫入原有 append-only `latency.jsonl`，按同一訊號合併至
開盤 API／畫面。這些是 `ledger_compute_persist_ms` 的子階段，UI 明示不可再相加。
發布失敗會清除當次 metrics，不能沿用之前成功資料；沒有歷史測速時仍是未知。
10/06 原始收據沒有這些新子階段，沒有事後補造。

重測命令：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_opening_preparation.py \
  --repeats 8 --output artifacts/benchmarks/opening-preparation-retest.json
run_fintech_python scripts/benchmark_day_trade_persistence.py \
  --state artifacts/live/tw_day_trade_simulation/state.json \
  --repeats 8 --output artifacts/benchmarks/opening-persistence-retest.json
```

## 驗證與部署邊界

- 最終相關回歸：538 個 Python 測試通過；60 個 Node 測試通過。
  包含四個現行部署設定的完整解析同值、父設定更新、並發 source replacement、
  檔案／目錄變更、精度、fsync 順序、失敗不能報成功、冷恢復與重複成交防護。
- 更廣的 architecture 檢查仍有兩個既存失敗：封存 ABI 期待拒絕但沒有拒絕，
  以及其他工作的 untracked deployment 指向不存在的本地實驗設定。
  在 isolated process 載入 HEAD 的原 config loader 重測仍得到相同兩個失敗。
  沒有修改該研究工作，也不宣稱全庫全綠。
- 15:59 收盤後確認紙上帳本全部零持倉，僅重載 day-trade engine、Discord 與
  read-only public gateway。沒有重啟 TAIFEX／採集器／訓練／儲存工作。
- 新 invocation 已載入；Discord Gateway connected、22 個 global commands 已同步，
  引擎與 Discord revision lag=0、ledger divergence=0。收盤健康值為 `waiting`。
- 原始 orders、fills、latency 的大小與 SHA256 全部不變；當日首／末 ready 與
  首／末 ledger 測速值亦不變。狀態由 2,778,783 降為 2,085,044 bytes 只是 JSON 空白減少。
- 原有全頁 CDP 稽核在公網及 localhost 都遇到 Runtime.evaluate 等待繪製逾時，
  保留失敗報告，不以 console 無錯或命令 exit=0 當作全頁驗收。
- 另外使用 Playwright managed headless browser，實際開啟公網當沖頁、展開開盤測速區塊，
  驗證繪製機會、內容與刷新操作；1366 px viewport 的 document scrollWidth 也是 1366 px，
  console errors=0，並已人工檢視截圖。
  單次刷新點擊至 API 回應為 400.374 ms，包含瀏覽器操作等待，不冒充純 API 或完整繪製延遲。
  這只驗收本次相關區塊，未取代失敗的全頁／完整歷史稽核：
  [瀏覽器收據](../artifacts/operations/opening_latency_20261006/browser-focused/receipt.json)、
  [實際畫面](../artifacts/operations/opening_latency_20261006/browser-focused/opening-1366x768.png)。
- 全系統 unattended guardian 仍顯示 `degraded`；本輪相關服務、帳本與同步驗收，
  不代表所有資料來源、其他服務或完整系統均正常。

1 秒仍是未達成的目標。本次為收盤後工程驗證，不是下一次 09:00 的完成證據。
當天完整行情最早於 1,799 ms 被本機取得；沒有這些真實資料就不能推論相同模型。
毫秒級的部分準備工作已實測，不能因此宣稱全流程或交易所成交也是毫秒級。
下一次開盤需由原有自動收據驗收全部 ready／ledger，並依真實 callback 等待、
輸入整理、完整輸出、後續可成交報價及新持久發布子階段繼續找瓶頸。
