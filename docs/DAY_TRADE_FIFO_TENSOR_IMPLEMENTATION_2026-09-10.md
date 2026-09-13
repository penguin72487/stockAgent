# Attention LayerNorm：跨日 FIFO 訓練帳務核心

後續進度見 [vastai1T 準備與核心實測](DAY_TRADE_VASTAI1T_PREPARATION_2026-09-10.md)：新增 GPU 原子失敗狀態、FIFO 路徑積分、270 分鐘 NAV 對照及遠端資料驗證。以下 20:02 紀錄保留為前一階段證據，不代表最新完成狀態。

## 1. 執行進度與交付邊界

2026-09-10 20:02（Asia/Taipei）：使用者明確拒絕「日末殘倉成本近似」；本次新增了真正保留股數的 tensor 帳務元件與開盤差額交易核心，直接與既有 paper engine 做差異測試。**完整訓練／驗證／測試串接仍未完成，新訓練指令尚未通過驗收。**

- 新增 `stockagent/backtest/tw_day_trade_inventory.py` 與 `test/test_tw_day_trade_inventory.py`。
- 45 項新測試通過；連同舊分鐘、paper、tick、年度切分與 checkpoint／續訓等共 293 項通過（19.94 秒）。
- 沒有啟用候選設定、啟動模型訓練、變更既有 checkpoint、重啟服務、覆寫網頁帳本或送券商委託。
- `configs/markets/tw_day_trade_attention_layernorm_paper_parity_candidate.yaml` 仍由 `load_config` 拒絕；兩項候選防錯測試仍通過。不得移除 guard 或偷偷關閉 carry 以取得「可執行」指令。
- 指定 attention／full_then_last／LayerNorm 架構、50% 分鐘量、年度 expanding walk-forward 皆未變更。2020-03-02 是已觀察的分鐘來源起點，不是所有必要資料完整的證明。

## 2. 已實作的狀態與事件

這是既有 backtest 分層中的專用帳務代數，不是第二套 trainer、live account 或模型選擇器。重用既有分鐘整張 sizing／容量函式、共同 PnL／利息原語，以及 canonical safe checkpoint writer／loader。

| 狀態／事件 | 本次行為 |
| --- | --- |
| acquisition cohorts | 每一批保留 signed shares、basis、剩餘 entry cost、原成交價、當沖／跨日退出費率、融券轉換費率、取得日期、是否已轉換及計息游標 |
| 13:30 無法退出的殘倉 | `convert_inventory_to_margin` 只登記轉換／必要融券稅差與處理費，不清空股數、不建立假出場；轉換當天不先扣一天利息 |
| 跨週末／假日 | `accrue_inventory_interest` 依實際日曆天數、尚存本金計費；同一天再次呼叫不重複扣款 |
| 09:00 sizing | 先計價原庫存及成本，依官方 open 凍結 economic NAV，再算新目標整張股數；不用 09:01 的未來價格決定股數 |
| 09:01 執行 | 使用傳入的來源 VWAP／Close；先 FIFO 減倉，再依 target minus remaining inventory 加倉；同一 symbol-minute 的 50% 容量只用一次 |
| blocked reduction | 未賣完舊多倉，不能直接加開反向空倉；停牌可阻擋退出，但單純失去新開倉資格不會阻擋減倉 |
| funding | 符合 paper 的 gross-NAV 預算：保留剩餘持倉名目金額、扣已發生退出費及尚未支付企業行動應收款；空單賣出收入不是免費購買力 |
| 企業行動 | 用旧股數認列有正負號的現金權利；換股調整實體股數與 basis，原成交價不改；本金不再重複減現金返還 |
| 零股／不足一股 | 實際換股產生的零股可按既有整張行情假設調整；不足一股且沒有 cash-in-lieu 條款者拒絕，不能四捨五入造股 |
| 付款日 | claim 認列與實際付款分開；付款只改應收／應付與 cash，不重複認列 NAV |
| 重啟／分段 | state payload 綁定 ordered universe、精確 release ID、庫存 ABI 與研究假設；拒絕 `latest`、換股池、換 release、降精度 state |
| 時序 | decision_day 與 observed_day 防止同日再次用量、倒退事件、提前取得未來股利；缺少取得日轉換者不能在數日後補成當日零利息 |

帳務式與 paper 共用：

```text
未實現淨損益 = signed shares × (mark − basis)
                − 剩餘進場淨費用 − abs(shares) × mark × 適用退出淨費率
NAV = 初始本金 + 累計已實現淨損益 + 已認列企業行動
      − 累計持有成本 + 未實現淨損益
```

成本、均價與帳務使用 float64；模型 AMP 設定未改。對帳曾抓到 `torch.where(condition, Python float, Python float)` 產生 FP32 利率的問題，已改為符合帳務 dtype 的常數 tensor，未藉由放寬容忍值略過。

`inventory_nav` 是估值而非平倉。非正 NAV 不重設為初始本金。此處也不是券商已交割 cash／購買力或舊訓練器 T+2 flat-stock 淨損益 queue 的替代名稱。

## 3. 測試證據

新測試實際呼叫 `TwDayTradeSimulationEngine`，不是另一個自己寫的 scalar 答案。主要包含：

1. 多／空 2,000 股跨四個日曆日、隔夜跳空後減倉，逐項核對實現損益、剩餘 entry fee、利息與 NAV（絕對誤差門檻 1e-7 TWD）。
2. 不同取得價及費率的多批庫存，以 FIFO 部分平倉；所有批次共用一個容量，不得按 cohort 重複放量。
3. 12 組完整開盤情境：保留目標、加減倉、多空翻轉、部分退出失敗、資金上限；09:00 NAV、09:01 數量及費用與 paper 一致。
4. 2,746 檔合成股池的資金配額，與既有 `_scale_lot_buys_to_budget_with_fixed_fees`（minimum=0、rounding=none）逐股相同。
5. 75% 換股、signed cash return、1,500 股殘倉退出、付款重試；未支付大額股利不能提前用來購股。
6. 七日同一序列分三次 checkpoint 還原後，所有 state tensor 與不分段執行逐位一致。
7. CPU／CUDA 的帳務對照、跨日梯度非零且有限、狀態 detach 使用私有儲存；`torch.compile(fullgraph=True, backend="eager")` 的 FIFO 與 opening 圖擷取及梯度與 eager 一致。

第 7 項的 compile 測試只證明完整圖擷取，**不是 TorchInductor 性能、CUDA Graph replay、全市場 epoch 或 canonical trainer 的 BPTT 串接驗收**。安全存檔測試只驗證此 namespaced state 的 serialization，不代表已把它寫進正式 epoch checkpoint。

重跑本次回歸：

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q \
  test/test_tw_day_trade_inventory.py \
  test/test_day_trade_attention_parity_candidate.py \
  test/test_tw_day_trade_contract.py \
  test/test_day_trade_margin_carry.py \
  test/test_day_trade_execution_reconciliation.py \
  test/test_tw_order_price_grid.py \
  test/test_tw_day_trade_minute_execution.py \
  test/test_walkforward_folds.py \
  test/test_checkpoint_manifest.py \
  test/test_training_resume_early_stop.py \
  test/test_resume_completion_marker.py
```

這是測試命令，不是新模型訓練命令。Ruff、py_compile、git diff --check 另行檢查。

## 4. 仍需完成，不能略過的串接

1. **來源與事件 tape**：舊 26 欄壓縮事件不包含所有分鐘保護／13:25～13:29 退出／完整 dated corporate actions。新來源 adapter 必須驗證全日 OHLCV、每日合法價格限制／tick、停復牌、現金條款與付款日，再把 accepted actions 傳給核心；不可用本元件自己生資料。
2. **分鐘委託流程**：同時管理當日 bracket／stop、13:20 passive、13:24 replace 與後續可執行分鐘，共用容量，最後保存真正殘倉。本次新增的是 inventory 與 opening 元件，尚非整天 scheduler。
3. **canonical 訓練串接**：`simulator -> loss -> trainer` 的 train batch、eval chunk、validation/test initial state、stitched deployment、報表 NPZ、epoch checkpoint 全部需要攜帶此新狀態；不能只靠 `final_weights`、`final_cash` 或 `final_equity_scale` 重建 FIFO。
4. **fingerprint／相容性**：新 tape、execution、terminal policy 與 checkpoint contracts 必須共同更新；只有在上述串接完整後才讓候選政策通過。舊 policy 留作舊 checkpoint 的可重現對照，不修改成假稱等價的新行為。
5. **真正的驗收**：相同來源／權重／初始庫存下逐筆、逐分、逐日對帳；完整年度 fold 生命週期與續訓 smoke；實際資料完整性通過後，才公布新的 `train.py --config ...` 指令。

### 目前資料證據的限制

20:02 再讀本機 summary（本次不是全量 object hash 複驗）：

- corporate-action reference：34,463 列，宣告從 2000 年起至 2026-09-10。
- entitlements：23,184 列，宣告 2014-01-01～2026-09-10。分類 coverage_complete 不能解讀成每件事件的精確條款都已齊全。
- 根目錄 share replacement：僅 2026-01-01～09-09、5 列、只有 TWSE、complete_all_markets=false。
- `execution_actions` share replacement：2026-01-01～09-10、20 列、TWSE／TPEx 兩市場。

因此不能宣稱 2020～2026 的必要實體換股資料已齊全，也不能自行把年度 walk-forward 改成幾十天 train/val/test 来迴避。source 修復／資料完整期間確認仍待後續完成。

## 5. 價格及券商邊界

仍採現有 dated stock／ETF price-rule 模組，不把單筆委託 tick 套到 VWAP、成本、費用或 NAV；[證交所第 62 條](https://twse-regulation.twse.com.tw/TW/law/DOC01_print.aspx?FLCODE=FL007304&FLNO=62) 與[交易制度說明](https://wwwc.twse.com.tw/zh/products/system/trading.html) 是合法委託價格的官方依據。

全部可轉融資融券、零股按整張行情仍是使用者指定的研究假設。永豐 [simulation 文件](https://sinotrade.github.io/tutor/simulation/) 明載 simulation 不支援零股；本元件不送委託、不以 KBar 模擬填單冒充券商 deal callback，也沒有聲稱即時 Shioaji simulation 串接已因此完成。
