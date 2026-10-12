# 2026-10-08 當沖開盤延遲事故：來源門檻、逐標的恢復與測速

## 判定

今天不是單純模型推論慢。最初排程喚醒只約 **2.942 ms**；兩個模式卻約
09:46 才發布，另兩個到中午仍反覆失敗。根因是 10/07 的來源防呆把
「模型可判讀的尚無首筆成交」也當作「整個模式尚不能運算」，並讓訊號重試
擋住來源修復工作。10/08 修正沒有換模型、減少標的、回寫既有訊號或修改
既有成交；不能把遲到的模擬回放當作即時開盤成功。

截至 14:12 的驗收分成四項，不能互相替代：

- 三路並行取價已載入，2,301 檔空價格快取實網達原門檻為 933.915／244.676 ms。
  這是盤中重測，不是下一交易日 09:00 的一秒內全鏈驗收。
- 使用者另核准的今日模擬收盤沖銷已完成：727 筆原持倉餘額全部結清、四模式
  未平倉為 0，原訊號及帳本前綴保留，沒有券商下單。
- 今日分鐘曲線仍缺 530／1,080 點，13:30 終點已與清算 NAV 對上；未虛構中間
  價格，既有補曲線排程下一次 14:35，這份紀錄不宣稱該未來工作已完成。
- 公網資料健康仍 `degraded`；盤後暖機另遇除權息衍生層與參考檔版本不一致，
  不以 Gateway 連線成功或沖銷成功宣稱後續模型資料已就緒。

上述是 14:12 當時觀測，不是後續狀態。14:24 既有來源重試成功，後續完整交易日
驗收也已通過；版本一致性保護與消費端驗收另見
[除權息來源修復](tw_corporate_action_generation_repair_2026-10-08.md)。
這項來源修復不會把當日已錯失的開盤 SLO 或分鐘曲線缺口改寫成成功。

## 多源平行開盤取價：2026-10-08 續作

已實作並於收盤後載入三路全市場取價。空開盤快取實網測試請求 **2,301 檔**，
兩次達原有門檻並保存收據後返回 **933.915／244.676 ms**，正值開盤價
**2,284 檔**、衝突 **0**。這是 13:44 的來源測試，**不是 09:00 全鏈一秒驗收**；
全部模式到模擬進場仍未達標。使用者後續核准的收盤沖銷另列下節，不用假清倉
或回寫舊成交來掩蓋開盤延遲。

### 可用來源與物理邊界

| 來源 | 此機實際狀態 | 09:00 用途／限制 |
| --- | --- | --- |
| 永豐 Shioaji | 現有模擬引擎 session，已實測 | 全 universe 以不超過 500 合約的非阻塞 Snapshot 批次同時送出，收到批次即合併；成交另需因果、非試撮的執行行情 |
| 官方 TWSE／TPEx MIS | 已接入；本次重測回傳 0 個有效開盤價 | 官方網站約 5 秒快照；逾時／空回覆不能阻止其他來源，缺漏續抓 |
| FinMind Sponsor tick snapshot | 現有憑證與全市場權限已實測 | `data_id=""` 全市場快照，約 10 秒更新；只取獨立 `open`，不是用 `close` 冒充 |
| Fugle TSE／OTC／TIB | adapter／語義測試完成，但此機沒有 key | 選用 `FUGLE_API_KEY`；全市場快照約 5 秒，需相應方案；沒有宣稱實網接通 |
| 富邦即時行情 | 尚無已設定／已驗證的 SDK session | 官方有 snapshot／quote 與串流；需要獨立帳號、憑證與行情權限，本輪未假裝接入 |
| TEJ、FinLab、本機一分鐘 K 線 | 原歷史／回放路徑保留 | 現有資料與契約不能證明在 09:00 第一秒取得全市場首次成交；不放入即時開盤競速 |

其他券商 API 或交易所授權直接 feed 需要各自帳號／合約／SDK；本次的「全部」
是盤點現有可接通來源及明確列出缺權限項目，不表示已取得所有供應商授權。

更新頻率與首次成交是不同物理邊界。沒有首筆成交、延後開盤或停牌的股票，
任何 provider 都不能提供可執行的開盤價。MIS 的「明確 0 量、尚無首筆」可依
既有 v4 ABI 解析輸入，未知仍未知；保留原 90% resolved 門檻，不降低覆蓋或
刪除原訊號。API 回應速度低於一秒，也不能保證供應商在 09:00 第一秒發布資料。
參考：[TWSE 交易制度／MIS 更新說明](https://wwwc.twse.com.tw/zh/products/system/trading.html)、
[Shioaji Snapshot](https://sinotrade.github.io/tutor/market_data/snapshot/)、
[FinMind 即時資料](https://finmind.github.io/en/tutor/TaiwanMarket/RealTime/)、
[Fugle 全市場 snapshot](https://developer.fugle.tw/docs/data/http-api/snapshot/quotes/)、
[富邦即時行情](https://www.fbs.com.tw/TradeAPI/docs/market-data/http-api/getting-started/)。

### 共享機制及修正

`opening_price_acquisition.py` 是所有當沖模式與缺漏修復共用的 session-fenced
single-flight owner。初次讓每個來源同時請求完整 universe；逐批合併並喚醒模型，
達原有門檻即持久化、返回，慢來源在背景完成。之後保留成功來源，只請求未解析
標的或新 universe 的差集；FinMind／Fugle 按發布週期退避，權限與配額錯誤退避。
晚到、跨日、過期、試撮或 shape 錯誤不會提高覆蓋。第一份合法開盤價保持不變，
不同原始價格則保存 conflict 證據，不以慢來源覆蓋快來源。價格資料不代表成交。

修正的實際瓶頸包含：

- 永豐完成後才查 MIS 的串行依賴，以及盤前 MIS 必須成功的單一來源門檻。
  盤前只證明某一路 transport 可回應；模型資料、資格、價格與執行仍分別驗收。
- 長網路等待佔住開盤快取／stock mutex。共享行情工作移到原引擎內單一 worker，
  native callbacks 只投遞資料；帳本 owner 繼續接收原子訊號，不多開永豐登入。
- 第一批同步 callback 在送出請求時已到、等待迴圈卻仍睡到最後一批的丟失喚醒。
- 每次 2 ms 輪詢反覆解析同一份 JSON；改核對原子檔案 identity，只有更新才解析。
- 每股票反覆轉換日期、每欄位重建陣列；日期邊界快取與向量化判斷經原 scalar
  語義／邊界測試，不更動來源值、模型、eligibility、費用或整張限制。
- 舊 native Snapshot timestamp 把台北 wall clock 誤當 UTC，造成八小時偏移。
  2330 的真實讀取證實這個差異；新資料分別保存 receipt 與正規化 exchange clock，
  帶 `snapshot_exchange_clock` 契約。舊訊號、成交及歷史 timestamp 沒有偷偷改寫。
- 取價 prefetch 在模型 mutex 外執行，仍核對原 request fingerprint 與 age；失敗
  記錄最後階段及 prefetch 耗時，不能讓失敗樣本消失。

每日來源事件寫入 `artifacts/live/tw_opening_snapshots/DATE.acquisition.jsonl`，
包括來源 dispatch、first batch、first valid open、完成／失敗、quorum、全 universe
雜湊與 counts；scheduled opening 和 engine entry 原測速繼續保留。nested spans
不是相加項，不把平行服務耗時加成一個虛構總延遲。

### 測試、載入與剩餘工作

**463 項相關 Python 回歸通過**，包含全 provider 初次並行、慢來源不阻塞 quorum、
逐批合併、多模式差集、失敗隔離、0 量／未知區分、跨日拒絕、衝突保存、native
clock 四種單位、callback 早到／慢最後一批、共享 IPC 相容性、盤前獨立 source
proof、prefetch 失敗測速、帳本恢復及原交易限制。不是整個 repository 全測試。
收盤清算修正完成後，將兩組有交集的測試合併重跑，**621 項通過**（17.26 秒），
不把 463 與 419 重複計數，也不宣稱未執行的全庫測試通過。

13:38 收盤後受控停止並核對 checkpoint，**727 筆持倉及四個原訊號 ID 完整恢復**。
engine Invocation ID 為 `8f2a7895aa174f92be29f4e03e6b229d`；13:47 最後 source
暖機修正載入 Discord，Invocation ID 為 `f4404fbf736a4b6aa5088904546ead54`。
上述階段的原 events／orders／fills 完整 SHA 前綴保持，未強制清倉、重新發
盤中訊號或送出券商委託。這些 Invocation ID 是當時載入的歷史證據；14:03
再次載入後的現任 owner 列於今日沖銷驗收。公網 health 仍 `degraded`，不是
交易／資料就緒宣告。

實網 source probe 兩次冷開盤快取均返回 2,284 檔：首次來源／HTTP 連線成本
933.915 ms，第二輪 244.676 ms；全部來源完成 1093.732／719.158 ms，沒有等待
後者才送模型。本輪 MIS 0 有效開盤價，未知 17 檔未變成價格。盤前 transport
真實讀取另測得 226.683 ms，即使 MIS 尚在 probe，永豐回覆可繼續暖模型；
這只證明連通性，不是提前取得當日開盤價。

原四模型、固定真實 receipt、獨立 model 與 ledger process 的隔離測試保留三輪：
全部發布 1922.941／1083.413／2870.482 ms；完成模擬 register／持久化 probe
2777.391／1906.049／3740.752 ms。缺價原目標仍 `partial_waiting_source`；測試
books 是明示 synthetic one-lot fixture，**不是市場成交證據，也不是 09:00 SLO**。
這次量測仍有 artifact／帳本提交及 I/O 的大幅波動，不能宣稱總完成一秒內；
完整冷準備與 worker startup 另保存，不以暖路徑冒充冷啟動或刪除較慢樣本。

下一個驗收是下一交易日真正的 open gate → source quorum → 每模式 immutable
pointer → 首次／完整 paper fill。仍需驗收即時執行來源證據與訂閱
200 cap；今日未平倉已經使用者核准的可追溯模擬沖銷解除，但即時執行來源與
暖機驗收仍獨立保留。使用者指定暫用永豐、MIS、FinMind 三路；Fugle／富邦
未提供權限，本輪不再擴大接入，也不宣稱所有供應商已上線。

證據：

- [空快取實網各來源／逐階段數字](../artifacts/operations/opening_fan_in_20261008/network_final_v2.json)
- [獨立 process 四模型與 canonical 帳本 probe](../artifacts/operations/opening_fan_in_20261008/application_process_chain.json)
- [盤前 transport 邊界實網驗證](../artifacts/operations/opening_fan_in_20261008/transport_live_probe.json)
- [native clock 誤差實際觀察](../artifacts/operations/opening_fan_in_20261008/native_clock_before_fix.json)
- [停止後 checkpoint 完整恢復](../artifacts/operations/opening_fan_in_20261008/stopped_inventory_restore.json)
- [新 owner 完整恢復](../artifacts/operations/opening_fan_in_20261008/loaded_inventory_restore.json)
- [帳本原前綴 SHA 驗證](../artifacts/operations/opening_fan_in_20261008/final_ledger_prefix.json)
- [載入後服務／原訊號／資料狀態](../artifacts/operations/opening_fan_in_20261008/runtime_final.json)
- [463 項回歸日誌](../artifacts/operations/agent-workflow/runs/opening-multisource-final-accept-20261008T054504-e722ba12/run.log)
- [最終 621 項開盤／清算聯合回歸](../artifacts/operations/agent-workflow/runs/opening-and-close-final-20261008-20261008T061802-8f1feae9/run.log)
- [三路取價工程範圍驗收，含未達標邊界](../artifacts/operations/opening_fan_in_20261008/acceptance.json)

重測來源用 `scripts/benchmark_tw_opening_sources.py --receipt 當日收據 --rounds 2
--no-connection-prewarm --output 私有JSON路徑`；這會實際請求 API，但不發訊號／
委託，也不新增永豐登入。完整模型／帳本 probe 在下面入口加 `--simulate-entry`，
仍限隔離測試，不能拿 synthetic book 或 write=False 速度宣稱券商成交。

## 今日 727 筆模擬收盤沖銷：另經核准後完成

使用者核准「接著修復模擬沖銷」後，先保存 live checkpoint 與完整帳本前綴。
原持倉餘額分別為 1 億模式 697、多基底 20、多基底22 2、V8 8，總計 727。
沒有更換模型、訊號、固定 sizing NAV、checkpoint 或 initial capital。

13:58／13:59 的 TPEx canonical collector 真正收到空回覆；非 strict 模式的
`status=ok` 並不能證明今日收盤已發布，`skipped_dates` 在這裡也不是跳過網路。
14:00:49 透過同一 collector 的 strict session／calendar 收據再次取得當日
TPEx report；已存在的 TWSE report 與新 TPEx 原始回覆均保留，未冒充前日資料。

14:01 既有 paper engine 依既有明示契約
`source_close_full_deliverable_residual_no_capacity_paper_only_v1` 自動追加 727
筆收盤沖銷，沒有手工覆寫 ledger 或送出券商委託。有效清算時間 13:30，實際
記錄時間約 14:01；全餘額成交是此契約的**模擬流動性假設**，不是市場容量或
券商實際成交證明，也不能讓今日錯失開盤的 SLO 變成通過。

| 模式 | 此次結清筆數 | 清算後未平倉 | 清算後累積 NAV | 今日分鐘點／應有 |
| --- | ---: | ---: | ---: | ---: |
| 當沖 1 億 | 697 | 0 | NT$ 105,210,361.93 | 46／270 |
| Multi-Basis | 20 | 0 | NT$ 13,048,683.15 | 224／270 |
| Multi-Basis22 | 2 | 0 | NT$ 16,000,995.50 | 225／270 |
| V8 Annual Log Cash | 8 | 0 | NT$ 12,204,567.06 | 55／270 |

新增只讀 `scripts/audit_tw_day_trade_close_reconciliation.py`，逐筆驗證原持倉
餘額、source SHA、日期、合法 Tick、官方價與正值整股成交量；獨立重算分攤
進場費、出場費、交易稅、commission rebate、已實現損益及 NAV，並核對
13:30 mark 與清算後 NAV。原 signals／consumed IDs、sizing NAV 及
events／orders／fills 前綴逐項保留；缺少分鐘點另報，不用插值把驗收變綠。

為避免某一交易所延遲時阻擋另一個已成功的來源，terminal hook 改為上市／
上櫃獨立讀取、獨立驗證。跨交易所 symbol 衝突保持未知；0、NaN、Inf、負值
或非整股量都不授權清算。官方 parser 允許 TWSE 真實 notes-only 附表，但
實際 close 表仍必須有完整 schema、精確日期及同長度資料列。

新增來源隔離／schema／清算稽核測試，與相關帳本及恢復回歸共 **419 項通過**
（14.24 秒）。14:03:20 僅在四模式已平後用 canonical reload 載入新 hook，
不能把 14:01 由既有 policy 產生的沖銷歸功於 14:03 才載入的程式。
現任 engine Invocation ID `44fb4321f3f542bf9c61ed35441f22a8`、Discord
`7a242a4a50e248bc8ba4c0f3235b4822`；兩者無後續自動重啟，Gateway 連線、
revision 同步 lag 0，再次精確恢復通過。已驗證下一 session 2026-10-12
不再 `blocked_open_position`，但這只是 inventory gate，不是未來開盤就緒。

另觀察盤後 cache 工作出現
`TW exact entitlement was built from another reference size`：目前參考層
34,606 列，已驗收 entitlement receipt 綁定先前 34,605 列版本。來源一致性
guard 正確拒絕，不修改 receipt 雜湊或跳過驗證；本輪尚未重建該上游衍生層。
既有 14:20 feature reconcile 與 18:00 action 工作也不能未經證據就宣稱會
修復這份 historical entitlement 契約。

今日曲線 550／1,080 點，仍缺 530 點。四個 13:30 終點已接受，完整曲線尚未
接受；保留既有分鐘維護 timer／path 與 14:31 前優先保護，下一次 timer
14:35 才能獨立驗證實際來源回補，不在這裡預報完成。

證據：

- [今日沖銷範圍驗收](../artifacts/operations/tw_daytrade_paper_close_20261008/acceptance.json)
- [727 筆來源、股數、費用、NAV 與曲線稽核](../artifacts/operations/tw_daytrade_paper_close_20261008/close_accounting_audit.json)
- [載入後精確恢復](../artifacts/operations/tw_daytrade_paper_close_20261008/postreload_restart.json)
- [公網四模式平倉與健康狀態](../artifacts/operations/tw_daytrade_paper_close_20261008/public_summary_after_close.json)
- [419 項相關回歸](../artifacts/operations/agent-workflow/runs/final-close-regression-20261008T060933-6a3f2be6/run.log)

可重跑只讀稽核；原 before checkpoint 與 raw source 須保留：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_tw_day_trade_close_reconciliation.py \
  --date 2026-10-08 \
  --before artifacts/operations/tw_daytrade_paper_close_20261008/preclose_restart.json \
  --output artifacts/operations/tw_daytrade_paper_close_20261008/recheck.json
```

## 實際時間線與因果拆解

| 階段／模式 | 今日證據 | 判讀 |
| --- | --- | --- |
| 首次排程喚醒 | 約 2.942 ms | 排程不是主要阻塞 |
| V8 首次失敗 | 09:00:06.644，33／2301 個有效開盤價，要求 2071 個 | 源資料回應完整不等於所有股票已成交 |
| 多基底22 首次失敗 | 09:00:11.900，280／2293，要求 2064 | 全市場正值開盤價門檻擋住推論 |
| 多基底 首次失敗 | 09:00:16.859，697／2292，要求 2063 | 同一共用入口阻塞 |
| 1億 首次失敗 | 09:00:21.230，899／2291，要求 2062 | 同上 |
| 多基底發布 | 09:46:41.496205，距開盤 2,801,496.205 ms | 真實發布遲到，不是 09:01 即時訊號 |
| 多基底22 發布 | 09:46:44.142375，距開盤 2,804,142.375 ms | 真實發布遲到 |
| 1億／V8 | 12:04 已分別失敗約 1629／1648 次 | 通過覆蓋門檻後，又被任一非零目標缺價擋住 |

總延遲是排程等待、來源證據、輸入準備、推論、原子發布、執行行情、帳本提交
的相依鏈。今天的模型 forward 約 52～57 ms，等待數十分鐘主要由以下邏輯造成：

1. 全市場至少 90% 必須出現正值開盤價。未成交股票的首筆價格沒有固定到達
   上限；等待它們不是 CPU 加速能解決的問題。
2. 每個非零原始模型權重都必須先有開盤價，才能發布整個模式。大量不足一張
   或尚未成交的非零權重，使已具備證據的目標也被攔住。
3. 只要任一模式處於 `retry`，背景來源修復就略過，形成模型等來源、來源又等
   模型的循環依賴。原收據因此仍停在 10/07。
4. 失敗測速只記 RuntimeError 類型，未保留詳細原因與已完成階段；重試還把
   盤中累計遲到時間標成首次 scheduler wake，難以正確定位。

## 共用修正與正確性邊界

來源／開盤取價契約升為 v4，保留原本覆蓋比例，改為核對三種不同證據：

- 當日正值開盤價：保留原值、來源時間與成功收據，不重抓、不被缺漏覆蓋。
- 官方 MIS 當日 09:00 後明確回報 0 成交量、尚無開盤價：可判讀為「尚無首筆
  成交」，不是資料下載失敗。沿用既有缺價的零 gap 特徵 sentinel；不製造價格，
  不因此授權股數或成交。
- 未知／過期／來源失敗：保持未知、繼續修復，不計入已解析筆數。

`PriceSnapshot.no_open_print_timestamps_ms` 在共享傳輸、持久收據、全 universe
展開及合併後仍保留。真正 positive-open completeness 仍獨立呈現；來源 quorum
達標不代表所有股票都已取得開盤價。引擎再次驗證 resolved = open + no-print，
且不得超出 active universe；ready flag 不能繞過此核對。

訊號只發布一次原始模型目標。缺價的非零目標不再被刪成零，也不讓整個模式
重複推論：保存原權重、固定 session NAV 與 `waiting_source` 的未定股數委託。
真實開盤價、合法漲跌停、因果非試撮行情到齊後才計算整張股數，再走原本市價
模擬路徑。其他已具備證據的標的可以繼續；13:20 起未進場委託取消，避免出場
期間又開新倉。資格、放空權限、Tick、整張、NAV 資金與費用限制仍保留。

背景修復只在**實際有開盤推論進行**時讓出資源；retry 的退避間隔仍修復來源。
失敗收據升為 schema 3，保留最後階段、原因、部分耗時、覆蓋證據、首次喚醒
與各次距開盤延遲。可能包含環境憑證的錯誤訊息先遮罩；完整私有 traceback
仍由原有日誌輪替管理。

## 可重測的證據，不能冒充實際 09:00 延遲

使用今天實際來源收據、原部署模型及原 checkpoint 的隔離驗證：

| 模式 | 首次隔離執行 | 第二輪已暖機 | 缺價非零目標保留數 |
| --- | ---: | ---: | ---: |
| 1億 | 3317.734 ms | 254.517 ms | 36 |
| 多基底 | 1565.496 ms | 276.864 ms | 0 |
| 多基底22 | 1535.419 ms | 249.175 ms | 0 |
| V8 | 2015.226 ms | 499.276 ms | 9 |

這些數字包含模型輸入／推論／結果整理，但**不包含首次網路取價、實際發布、
進場或頁面顯示**。沒有利用 write=False 的速度聲稱開盤已達標。

另外執行原本完整四模式原子發布／完整 artifact 的隔離 benchmark，保留全部
三次樣本。四模式發布耗時 2544.245、1627.486、2067.549 ms，中位
**2067.549 ms**；完整 artifacts 中位 **3172.967 ms**。完整歷史、模型標的、
權重與解釋契約均未縮減，但離一秒內仍有差距。這是暖機、固定收據、無網路
的軟體鏈測試，不包含真正 09:00 的物理首筆交易或因果行情延遲。

模型及執行未換成券商真實下單；全部仍是明確標示的 paper simulation。
正常 live 模式與既有 missed-opening 09:01 反事實恢復各自保留原始發布時間，
後者不得用來重置實際開盤 SLO。

取價與成交證據仍分開：官方 Shioaji 將 Snapshot 定義為請求型快照，每次最多
500 合約，盤中即時行情應使用訂閱，不應拿重複輪詢當即時 feed。這次沿用
既有長連線與訂閱 owner，沒有用額外登入或突破供應商限制換取測速數字。
參考：[Shioaji 官方 Snapshot 文件](https://sinotrade.github.io/tutor/market_data/snapshot/)。

## 載入與原帳本保護

12:33 的只讀預檢已確認 34 筆既有模擬持倉、已消耗訊號、待進場目標及固定
NAV 可由原 engine constructor 完整恢復。只複製必要 checkpoint／commit
events 做隔離驗證，不另複製 4.3 GB 的歷史 signal 明細。

12:35:58 受控停止 Discord 與當沖服務；12:36:04 再驗證最新 checkpoint
完整恢復且無待提交／分歧。12:36:08 當沖 owner READY，12:36:11 Shioaji
prewarm 完成。34 筆持倉和原訊號 ID 保留；新 engine run ID 為
`eafac9f6385543e2aefb738c087952d5`。

部署後 Discord Gateway 連線及 22 個應用命令同步正常。1億／V8 分別在
12:36:21.273010／12:36:20.555247 發布當日訊號，原多基底與多基底22 的
09:46 原始訊號 ID 保留、不重新生成。既有 missed-opening 模擬流程在
12:45:30 也接收了1億模式；12:46 的 runtime 收據證實四模式均已發布且
由 engine 登記，但不能把反事實 09:01 定價當成當時真實發布或券商成交。

1億的實際模擬結果仍是 `partial`，其餘三個已登記模式為 `filled`，這不是
全部股數已成交的宣告。分鐘來源恢復有獨立進度，未阻塞持倉處理／watchdog。
背景開盤來源修復收據已更新為 10/08；12:45:51 共 2267／2301 個正值開盤價，
剩 34 個中有 33 個明確尚無首筆成交、1 個未知來源，保持 `waiting_source`。
私有 dashboard summary 也顯示四個當日 signal ID，資料健康仍為 `degraded`。

12:46:42 再核對原 events／orders／fills 完整前綴 SHA 均不變；新增模擬紀錄
只追加，不改寫原成交。兩服務新 Invocation ID 分別為 Discord
`7c8812cc49ab4202bbd8009a3d91cb6d`、當沖
`ef8597037efd4f8ca89bd72ad83f15fb`，均沒有後續自動重啟。

下一交易日真正 09:00 至發布／模擬進場的總延遲仍需現場測量，現在不能
宣稱毫秒級、一秒內、全市場所有價格已齊，或因果即時交易已補回今天早盤。

## 重測入口

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_tw_opening_latency.py --runtime \
  --output artifacts/operations/opening_latency_repair_20261008/latest_attempts.json
```

加 `--infer --rounds 2` 才跑目前交易日收據限定、零網路、不發布的四模型 probe。
歷史事故測速可用 `--session-date` 讀取，但不允許拿目前行情重建歷史推論。

完整發布鏈用 `scripts/benchmark_opening_signal_pipeline.py`，輸出位於隔離臨時
目錄；不可把它的暖機數字當開盤網路／成交延遲。

工程回歸包含來源時間／未知／0 量、shared payload 往返、背景修復不餓死、
失敗階段測速與遮罩、原權重保留、部分目標進場、未知股數跨重啟恢復、因果
行情、費用／NAV、13:20 取消以及原成交前綴保護，共 **426 項通過**；沒有
宣稱整個 repository 全部測試完成。

主要收據：

- [原始事故及四模型隔離驗證](../artifacts/operations/opening_latency_repair_20261008/final_probe.json)
- [完整四模式發布鏈三次測速](../artifacts/operations/opening_latency_repair_20261008/publication_benchmark.json)
- [停止後的原帳本恢復預檢](../artifacts/operations/opening_latency_repair_20261008/stopped_restart_preflight.json)
- [部署後四模式／Gateway／來源恢復現況](../artifacts/operations/opening_latency_repair_20261008/runtime_final.json)
- [原成交／委託／事件前綴 SHA 保持](../artifacts/operations/opening_latency_repair_20261008/final_ledger_prefix.json)
- [426 項回歸日誌](../artifacts/operations/agent-workflow/runs/opening-acceptance-20261008-20261008T043508-f48b3353/run.log)
