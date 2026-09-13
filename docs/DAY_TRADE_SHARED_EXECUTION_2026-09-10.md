# 當沖訓練、歷史回補與券商模擬成交契約

## 1. 執行進度

實作中，尚未通過全套訓練／回補逐筆一致性驗收，尚未切換正式服務。
原帳戶、既有分鐘曲線及今日已記帳的成交必須保留，不能追溯偽稱為券商回報。

本次已完成 staged 股票 simulation adapter、零股殘倉例外的純函式、
持久化委託／原始回報／成交收據、唯讀庫存切換檢查及本機測速工具。
未完成正式 paper 帳本消費 broker 收據、實際 simulation 送單成交驗收、
GPU 跨日分鐘庫存 executor 與完整 epoch 的訓練／網頁逐筆一致性驗收。
不能提供一條沿用舊 executor 的指令，卻宣稱它已符合新的訓練基本假設。

## 已實作的成交證據邊界

- `stockagent/live/tw_stock_simulation_execution.py`：只在模組內建立
  `Shioaji(simulation=True)`，不接收外部的 production client、不啟用 CA。
  本機實際安裝為 1.7.0；native client 沒有可讀取的 `simulation` 屬性，
  所以安全性建立在受限建構入口，不是假讀一個不存在的旗標。
- 委託使用 `timeout=0` 非阻塞 API；初始 Trade 即使寫 Filled 也不記成交。
  Native `StockDeal` 才能產生 `shioaji_simulation_stock_deal_v1` 收據。[5]
- SQLite WAL + `synchronous=FULL`；prepare／claim 合併成一次原子 commit，
  成功落盤後才可進入送單。六字元標記在同一持久帳戶 journal 中不回收；
  不得刪 journal 重建號碼或為同一帳戶分散建立多份獨立 journal。
- 原始 callback 先由短 callback 放入佇列，owner 在 callback thread 外落盤。
  回報持久化後，即使在成交記錄與事件完成標記之間中斷，也能重播去重。
  一個 journal 只有一個 session writer；consumer 必須把 receipt ID／游標
  與帳務變更一起原子提交，不能僅把 JSONL append 當作 exactly-once。
- 以帳戶、委託、股票、方向、整張單位、委託條件、交易日及 exchange_seq
  驗證回報。張數轉股數只做一次；重複回報不再記帳，衝突回報保留原文並阻擋。
- 5 秒未獲可驗證回報的送單改為 durable `unknown`；啟動時未完成的舊送單
  也先要求對帳。改 key 不能規避不明送單。查無紀錄不是「沒送出」證據。
  模擬環境的歷史 order_deal_records 查詢不能保證補回遺失回報。[1]
- 明確的新單拒絕不製造本機成交，也不因單一拒單停止其他模型；改／刪單失敗
  則保留原委託。已取消單不會被較晚到達的舊 New 回報重新變成有效委託。
- 實際送單使用本機當下時鐘，不接受歷史 replay clock；檢查當日 Contract V2
  TWD、整張交易單位、停牌狀態及規則資料日期。收盤集合競價階段拒絕 MKT，
  不因「市價」二字捏造必定成交。日曆／資格與策略風控仍由 canonical runner 負責。
- 零股例外只減少已存在的餘數：1,500 股最多本機處理 500 股；正常 1,000 股、
  新開倉、錯方向、缺價、過期／未來／非因果報價、錯 tick 均拒絕。
  當前例外函式僅支援連續交易時段的真實整張 best bid/ask；集合競價的零股
  例外尚未接上，不可用此函式偷渡尾盤保證成交。其證據明列
  `local_odd_lot_assumption`、`broker_confirmed=false`。

## 庫存交接的實測阻擋

2026-09-10 17:14 台北時間，canonical readiness audit 讀得五個模式的資料、
checkpoint 及當日資格前提均通過，但這不是券商組合成交驗收。
現行帳本仍為本機 paper，庫存交接結果為 `blocked_legacy_inventory`。

| 模式 | 尚未平倉的取得批次 | 含零股的批次 |
| --- | ---: | ---: |
| tw_day_trade_100m | 75 | 0 |
| tw_day_trade_attention_layernorm | 1 | 0 |
| tw_day_trade_multi_basis | 2 | 0 |
| tw_day_trade_multi_basis_22 | 1 | 0 |
| tw_day_trade_multi_basis_projection_l1_gelu | 2 | 0 |

合計 81 筆是舊本機持倉，不等於券商持倉；數量為取得批次，不是去重股票數。
本次未平掉、刪除或重標這些部位，也未重啟現有服務。
需要使用者決定是保留舊帳本並另起零庫存的 broker 執行區段，或採另一個明確
交接方案；不能默認把舊部位以收盤價帳面結清再宣稱績效連續。

稽核產物：`artifacts/live/tw_day_trade_readiness/simulation_cutover_20260910/readiness.json`
及同目錄 `readiness.md`。稽核程式原先把資料前提寫為「端到端可執行」且硬編碼
四模式，本次已改為實際啟用模式數，並明示資料前提與 broker 驗收為不同健康域。

## 實測與可重現命令

測試及測速皆沒有網路送單；本機安裝的 Shioaji 1.7.0 `StockOrder` 建構也完成
離線 schema 驗證，但這不替代 simulation 登入與實際成交回報測試。
最終上述八個測試檔合計 **327 passed / 9.11 秒**，其中新增 adapter 測試 73 項。
Ruff 語法／致命錯誤檢查及 `git diff --check` 通過。沒有執行訓練完整 epoch。

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q \
  test/test_tw_stock_simulation_execution.py \
  test/test_tw_order_price_grid.py \
  test/test_tw_day_trade_contract.py \
  test/test_shioaji_simulation_lifecycle.py \
  test/test_day_trade_margin_carry.py \
  test/test_day_trade_execution_reconciliation.py \
  test/test_tw_day_trade_simulation.py \
  test/test_tw_day_trade_open_price_replay.py
run_fintech_python scripts/benchmark_tw_stock_simulation_journal.py --samples 200
run_fintech_python scripts/audit_tw_day_trade_live_readiness.py \
  --output-dir artifacts/live/tw_day_trade_readiness/simulation_cutover_20260910
```

測速在 `/root/stockAgent/artifacts/live` 同一檔案系統的兩個隔離 journal 交錯執行，
保留 FULL sync。暫存 fixture 由測速工具清理，不涉及正式帳本。

| 階段 | 次數 | P50 ms | P95 ms | 最大 ms |
| --- | ---: | ---: | ---: | ---: |
| 分開 prepare／claim | 200 | 7.4603 | 39.5771 | 127.4552 |
| 合併原子 prepare／claim | 200 | 3.8198 | 8.1841 | 33.0245 |
| 合成成交回報 commit | 400 | 3.8708 | 9.1154 | 68.1448 |
| 重複回報檢查 | 400 | 0.1967 | 0.5622 | 1.6721 |

送單前 durable 準備的 P50 在這次樣本降低約 49%。尾延遲受主機磁碟負載影響；
數字不含 broker 網路、帳戶帳務消費、模型推論、Discord 或網頁推送，
不能拿來保證 09:00 SLA 或宣稱達到理論極限。

## 契約與證據

相同的是策略、股數、帳務與風控語義，不是不同資料源的成交價格。
訓練及歷史回補使用官方開盤價決策／定股數，09:01 右標記分鐘 VWAP
（Amount / volume_shares；缺 VWAP 時只能用同根來源 Close）入場。
同一帳戶、股票、來源分鐘的所有成交共用 50% 成交量上限。
減倉、反手、不同取得批次及重複處理不得重複取得容量。
原始 OHLCV、單筆成交、平均價、委託價及帳戶 NAV 必須保持不同語義。

即時正常整張委託必須送入 `Shioaji(simulation=True)`；收到成交回報前不建立
已成交持倉。拒單、逾時與查無委託均不構成可重送或可本機補成交的證據。
委託意圖先持久化；送單結果不明時先對帳，不能盲目重送。
成交回報可能早於委託回報，需以身分比對並去重。[1][2]

永豐模擬環境明確不支援零股及興櫃委託。[1] 零股殘倉例外沿用本機整張行情
假設，必須標示獨立的 `local_odd_lot_assumption` 證據，不得冒稱 API 成交。
此例外不授權為整張拒單、斷線或未成交製造成交。
全部可轉融資融券是本機研究信用假設，不是券商保證；保留實際 API 拒單結果。

持倉保留至有成交證據才能減少；無法平倉時按研究假設轉持有融資／融券庫存。
隔日按開盤 economic NAV 定目標，再以目標減已有股數執行，先減舊部位再加新部位。
應保留 FIFO 成本、按日曆天的利息、除息權利／支付日期及實體股數變更。
不得以收盤帳面結清抹掉隔夜風險。

## 待解決的完整性邊界

- 舊訓練 `scheduled_events_50pct` 仍使用壓縮事件 tape 和收盤帳面結清；不能
  只把名稱或 50% 參數改掉就稱與網頁一致。
- 現有早期訓練日線代理使用加減 tick，不能冒充 09:01 分鐘成交。
- 本次讀得企業行動 reference 涵蓋 2000～2026-09-10，但精確現金權利及換股
  receipt 只涵蓋 2026-01-01～2026-09-10。reference 完整不代表實體持倉
  所需精確條款完整；缺少更早條款不得靜默假設沒有企業行動。
- 券商帳戶與模型帳戶的隔離、既有庫存切換及模擬重啟可恢復性需另外驗收。
- 歷史限價／停損順序只能依資料粒度判斷；分鐘 OHLC 無法證明逐筆排隊。

## 價格精度與時間版本

股票 tick 已確認有 2005-03-01 制度邊界，普通股票漲跌幅有 2015-06-01 邊界。
ETF 與股票 tick 不同；外國成分／槓反 ETF 的漲跌幅也不能一律設為 10%。[3][4]
歷史查價必須傳入執行日期，缺日期不能默認今天的制度；實際委託須符合
該商品的合法格點，VWAP、成本及 NAV 不得強制取 tick。
不能從法規修正公布日直接推定施行日，也不能宣稱未知未來制度已被涵蓋。

## 驗收

1. 相同起始帳戶、目標權重和來源事件，逐筆比較股數、價格、費用、FIFO、
   企業行動、殘倉及 NAV；不能只比較最終 YTD。
2. 模型 BF16；價格、股數與帳務採可重現的高精度計算，不把 BF16 用於 tick 判斷。
3. 零量／缺價、跨價級距、反手、週末利息、除息／換股、重啟與重複回報必須測試。
4. 訓練沿用 canonical train.py／loss／checkpoint／lifecycle，不另建 trainer。
   新執行契約使用新產物目錄，不 resume 舊契約 optimizer。
5. 回補候選每個完成交易日 270 個來源分鐘點；有未平倉時不強制 promotion。
6. API 成交實測、合成回歸、完整 epoch 和部署驗收分開記錄，未完成者保持未完成。

## 來源

[1] SinoPac，Simulation Mode，2026-09-10 查閱：
<https://sinotrade.github.io/tutor/simulation/>。

[2] SinoPac，Order Event 與委託狀態文件（Shioaji 1.7.4 隨附參考文件），
並須以本機安裝版本及實際回報再驗證；不從 HTTP 成功或 PendingSubmit 推定成交。

[3] 臺灣證券交易所，集中市場交易制度介紹，2026-09-10 查閱：
<https://www.twse.com.tw/zh/products/system/trading.html>。

[4] 臺灣證券交易所，受益憑證買賣辦法歷史條文，第 7、8 條（2023-05-30）：
<https://twse-regulation.twse.com.tw/TW/law/DAT06_print.aspx?FLCODE=FL007114&FLDATE=20230530&LSER=001>。

[5] SinoPac，[Non-blocking Mode](https://sinotrade.github.io/tutor/advanced/nonblock/)，
2026-09-10 查閱。文件中的效能範例不是本機測速，不混入上表。
