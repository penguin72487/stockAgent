# 永豐模擬委託與開盤價的隔離試驗

狀態：只讀對照工具及共用行情 owner 的程式候選已實作；**候選未部署、尚未
完成真實共用連線驗收，也未送出或排定自動模擬委託**。
2026-10-30 的既有開盤來源觀測已排程，但不是模擬委託實測。不得把此文件或
測試夾具當作券商實測收據。

## 為何不能用全市場模擬委託取代開盤行情

證交所開盤前集合競價只收限價 ROD；市價、IOC、FOK 不能預掛參與該次開盤。
09:00 後的市價成交即便成功，其價格是委託後的模擬成交價，未必等於該股票
當市首次實際成交價；延後開盤、暫停撮合、無對手單與部分成交還會影響結果。
永豐公開模擬文件沒有說明模擬成交價必然等於交易所首筆，也不支援興櫃或零股。
所以結果只能作為獨立實驗，不得寫入開盤價格、替代實際成交或改寫模擬績效帳本。

永豐文件列出委託及改刪單相關 API 合計 250 次／10 秒，每人最多五條登入
連線，且即使未觸及上限，過載仍可能被停止服務。這是風控上限，不是建議
用量；不能假設模擬與正式或其他既有服務的額度完全隔離。以 10/29 已驗證的
2,314 檔為純算術例子：若忽略網路及成交耗時，且假設無其他委託、每 10 秒的
250 次都立刻送完，單方向第 2,314 次最早也要在第十個窗口、約 90 秒後；兩
方向 4,628 次最早約 180 秒。實際應留有額度餘裕，且拒單、成交回報及清倉
可能更慢。這並不保證成交或完整報價。

現行排程文件預留三個期貨行情與兩個股票行情連線；實際可用連線必須用已有
owner 的即時 `api.usage().connections`、帳戶身份及預留需求驗證，而不是從
systemd active 猜測。當沖引擎使用 `simulation=True` 登入但以
`subscribe_trade=False` 建立行情 client；目前已部署的 quote IPC 只有行情，
沒有委託命令／回報介面。候選在**同一既有 simulation-only owner** 訂閱回報，
不另外登入，但目前無原生整合入口；帳戶權限、回報、重連、行情不中斷及交易日
實際行為都還沒有真實驗收，不能只靠單元測試推定安全下單。

## 應先完成的安全驗收

1. 使用實際交易日官方清冊與證券狀態，挑選少量、符合整張與市價規則的標的；
   例如再核實永豐範例標的 2890 當日仍可交易，不能把它當永久有效清單。
2. 維持一個模擬帳戶一份 canonical journal 和單一 writer；意圖先持久化，並用
   真正 `StockDeal` 回報作成交證據。ACK、逾時、報價或測試夾具不是成交。未知
   委託先對帳，不重送；回報遺失時仍保留未知狀態。
3. 在安全驗收的共用 simulation owner 提前訂閱交易回報；不啟用真實委託、不
   擠掉任何既有服務的保留連線，也不把測試優先於正式當沖。權限、owner、回報
   或連線容量任何一項不能證明時，不送單並保留原因。
4. 在開盤後，先對單一標的提出一張模擬市價 IOC，記錄送單前、接受與每筆
   成交回報；若買單確認成交，再只按已確認成交量送出相反方向的平倉委託。
   即使是模擬帳戶也可能拒單、部分成交或留下未平部位；不能宣稱保證平倉。
5. 依相同當日／標的／明確時鐘，對照各來源實際收到的開盤 observations，以及
   盤後可核對的官方開盤價。記錄差異、缺資料與回報，不以預設相等代替證據。
   擴大樣本前，驗證對當沖及其他服務的延遲、quota 與回報沒有不良影響。

## 2026-10-30 共用連線候選與跨日預檢修正

使用者選擇保留當沖、期貨及其他服務，完成共用連線驗收後再測。實作仍在候選：

- `quote_provider.py` 只對它自己以 `simulation=True` 建立的現有 client 記錄
  process-local 身分；共用交易回報要求排他 callback lease，不能傳入任意
  client 或在找不到行情 client 時隱式登入。共用 session detach 不登出、不
  取消行情 owner 的連線；重連換代與非阻塞送單由獨立生命週期鎖隔離，不把
  SDK 送單持有在股票行情快取鎖中。
- `tw_stock_simulation_execution.py` 同一模擬帳戶採用跨 journal 的排他
  writer lock 和持久化的帳戶／journal UUID 綁定，原 journal 即便被替換到
  同一路徑也不能重用交易標籤。這只能約束遵守此協定的本機 writer，無法
  宣稱管理了其他獨立程式或券商端所有交易。首次建立綁定前仍須核對帳戶
  及其既有 journal／庫存，不能把空白登記簿當作帳戶從未交易的證據。
  原有策略帳本沒有改寫。
- 已登入的同一 client 才能設定 callback 並明確 `subscribe_trade`。回報快取
  必須 `Healthy` 且沒有 reasons，journal 無未知／未完成委託與待處理回報，
  才能通過候選 preflight；即便通過，仍不是實際 callback 或成交證據。
  共用訂閱、基線查詢不得從 08:59 至 09:01 開始，以避免占用開盤準備時間。
  未知送單不可重送、不可卸除 callback；真實 broker `StockDeal` 才可記成交。
- 本機 Shioaji 1.7.9 原生離線測試證實未登入時設定 order callback 會
  `AuthError`；私人 staged session 的順序已改為模擬登入但不訂閱、設定
  callback、明確訂閱。這不代表允許為此次實驗啟動額外登入。
- 既有安全重載預檢發現 MGDA 2026-10-29 的 1786、3105、3691、8099 已是
  `cancelled_exit_priority`，剩餘欄位是未成交數量，不是已成交庫存。跨日後
  引擎把狀態改為 `waiting_09_00_signal`，舊檢查因此誤判為 4 筆待處理。
  已增加僅限先前交易日、訊號已消費、收盤結算與完整成交證據存在、持倉及
  進出場／收盤待處理均為零的嚴格條件；未知或同日狀態仍阻擋。
  修正後的一次唯讀預檢為 0 持倉、0 待處理、9 筆保留終態紀錄，沒有重啟或
  改動帳本；它只是當時的快照，不授權直接套用或證明將來仍可重啟。

離線假 API 測試涵蓋重連、送單前換代、送單結果未知、遲到成交、回報快取
退化、callback 掛載／移除失敗、同帳戶多 journal 與開盤保護。真實 SDK 的
未登入檢查完全沒有 login／訂單。候選仍缺少將此共用 session 安全納入既有
運行中 quote owner 的受控入口與交易日原生驗收；不能把 Python 程式候選、
健康程序、快取 `Healthy` 或假回報當成明天已能自動送單。依使用者的順序，
在這些前提成立前不安排自動模擬委託，不停止其他服務來挪連線。

### 這次驗證範圍

- 受影響的七組本機回歸測試共 **696 passed**；另外以固定程式快照及固定測試
  輸入，在隔離 namespace 執行三組測試，**341 passed**。這些是工程驗證，
  並非券商連線、實盤資料或下單驗收。
- 候選快照為
  `artifacts/operations/service_code_releases/20261029T162543Z-cfbdb21846b5/`，
  source SHA-256 為
  `c668ebe341a169fdb179af0ffe5fa3470886694e5286fc1a89f33e6b17d4e2b4`；
  狀態是 `prepared_not_adopted`，現有服務未載入此版本。快照只在既有部署
  版本上覆蓋本次的兩個共用連線相關模組，沒有啟用未驗收的實驗。
- 隔離測試早期曾使用測試帳戶在預設 registry 產生測試綁定，確認無程序持有後
  已完整移至 `artifacts/operations/tw_opening_simulation_probe_20261030_fixture_owner/`；
  未刪除或替換正式帳本。

## 已實作的只讀對照工具

`scripts/audit_shioaji_opening_simulation_probe.py` 在另一個研究流程取得實際
simulation-only `StockDeal` 並寫入 canonical journal 後，可唯讀比較指定 market
的成交與開盤來源日誌；不登入、不查 provider、不送單、不修改交易帳本：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_shioaji_opening_simulation_probe.py \
  --session-date 2026-10-30 \
  --journal /absolute/path/to/existing/canonical-stock-simulation.sqlite \
  --source-journal artifacts/live/tw_opening_snapshots/2026-10-30.acquisition.jsonl \
  --twse-daily /srv/stockagent-live/data_tw_public/twse_daily_ohlcv.parquet \
  --tpex-daily /srv/stockagent-live/data_tw_public/tpex_daily_ohlcv.parquet \
  --output artifacts/live/tw_day_trade_simulation/opening_audits/2026-10-30/simulation-probe.json
```

預設 market 是 `shioaji_opening_order_probe`，不讀取其他策略的成交。不存在
journal 時顯示 `waiting_simulation_deal` 並不建立資料庫，也不掃行情日誌。
報告列出券商模擬成交時間、相符的 callback 收到時間、來源日誌事件時間、
價格差異及來源衝突。來源觀測只檢查目標標的的 observations_delta，不是對
完整 journal 或官方價格的驗收；日誌事件時間不是網路 wire time 或 fsync 完成
時間，牆鐘差也不能冒充單調時鐘／交易所延遲。兩個 daily 路徑可共同省略；
提供時會複用現有的交易所日資料讀取器、在讀取前後確認檔案 hash，並要求
正的每日成交量才比較日開盤價。這只證明輸入檔案與所讀資料的身分，仍須核對
其來源與發布收據，不能只憑使用者指定的路徑宣稱官方資料已驗收。零成交量且
無 OHLC 的狀態單獨保留，不能當成有開盤成交。

2026-10-29 收盤後只讀掃描原始 380,828,215 bytes acquisition journal 的
2330、2890 observation，在本機耗時 4.81 秒，並觀測到 FinMind 與永豐的價格
各自一致；這不是模擬下單測試，也不證明全市場資料完整或未來延遲。原完整
開盤稽核另標記一筆時鐘異常，因此整份日誌仍為 degraded。同次只讀測試以
兩個現有 venue 日資料檔及內容 hash 查這兩檔，取得正成交量日開盤欄位的耗時
為 1.663 秒；尚未在此工具內核對 collector 收據。

## 明日既有觀測

`stockagent-tw-opening-audit.timer` 已啟用，預定 08:55、09:00:11、09:00:31、
09:01 與收盤後讀取原生來源日誌及六模式時鐘。這些排程不登入、不送單；
必須等交易日收據產生後才能判斷實際覆蓋與端到端結果。

## 官方來源

- <https://sinotrade.github.io/tutor/limit/>
- <https://sinotrade.github.io/tutor/simulation/>
- <https://www.twse.com.tw/zh/products/system/trading.html>
