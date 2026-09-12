# 當沖資料與執行標準對齊

後續使用者已允許日 K 代理，並選定官方開收盤、不加不利 tick；本文件保留前一輪時點紀錄。最新口徑、archive 修復與未完成驗收見 [正式訓練就緒盤點](DAY_TRADE_FORMAL_READINESS_2026-09-10.md)。完整跨日持倉要求不變。

## 1. 執行進度

本次修正候選 physical FIFO 核心及來源檢查，不改正式網頁帳本、不重新推論舊訊號、不啟動訓練或遠端部署。保留使用者指定的 09:00 決策、09:01 回補成交、50% 分鐘量、跨日股數、全部可轉融資融券及零股按整張行情等研究假設。

已完成程式修改與既有 paper differential tests；已用 2026-09-09 的保留真實資料比對分鐘取價。年度 `train.py` 的 accepted-source adapter、跨 chunk/fold 狀態與正式輸出仍未完成，因此不能提供聲稱全套一致、可正式訓練的新指令。這是工程未完成，不等於所有來源缺项都必须先補完。

## 2. 之前實際怎麼做

本機指定原產物的 `run_manifest.json` 現查為：

```text
artifacts/markets/tw_day_trade_hybrid_minute_v12_attention_full_then_last_layernorm_commission20_capital10m_all_features_v1
panel_start_date = 2014-01-06
day_trade_minute_execution_allow_daily_proxy = true
day_trade_minute_execution_policy = scheduled_events_50pct
max_volume_participation = 0.5
terminal_policy = flat_after_1330_margin_conversion_with_t_plus_2_net_claim
```

原訓練不是完整跨日股票持倉：`tw_day_trade_minute.py` 在日末收盤估值、扣除殘倉退出與融資成本，再清空股票帳面，剩下 T+2 淨現金差額。因此它不會遇到實體股數跨過隔日除權／減資的完整會計需求。舊程式還允許分鐘歷史以前的日 K 代理；不能把這些已被使用者否決的近似重新打開來通過驗收。

網頁則不同：`_margin_corporate_action_gate` 只對前期實際持倉檢查持有期間的企業行動；完整來源目錄存在 `avoid` 分類，不等於所有策略都要停止。`_minute_bar_rows` 去除零量填補 K 棒；完整來源中沒有 09:01 執行證據就不成交。分鐘估值可明示沿用先前已觀測成交價，但不能當作新的委託／成交價。

前期 replay 的成功範圍是指定模型與 2/25 起的實際訊號、交易及持倉需求，不是 2020 年起全股票、全事件均有完整條款的證明。公開來源完整度、特定持倉軌跡可計算、年度訓練串接可运行須分開驗收。

## 3. 本次修正

### 股數、現金權益與企業行動分開

- `tw_day_trade_carry.py` 的 `action_mask` 先與「事件前取得、尚未退出」的實體 cohort 相交。無舊持倉不要求該事件精確條款；當日新買股票也不因此領取舊權益。
- 真正持有跨日多／空部位遇到未解條款，CPU 明確失敗，CUDA 回傳 failed／NaN；全帳戶不留下半套已套用變更，不誤標投資破產。
- `source_gap_mask` 明定為分鐘來源缺口。只阻擋該股當日新單，不重分配權重；若原本持有實體股數仍阻擋，不能清空持倉或捏造退出。
- 已確認金額及付款日的現金應收應付不需要股票分鐘行情。保留原 claim，依日期收付，不重複認列收入。共用 `_valid_claim_terms` 仍拒絕 NaN、無付款日、小數日期及非法 paid flag；這不是接受未知權益。
- `CARRY_SESSION_ABI` 升為 v3，拒絕 v2 的 continuing-state checkpoint。沒有放行原 optimizer 或候選設定的年度訓練 guard。

### 成交證據與分鐘估值分開

- `paper_minute_opportunities` 與既有 paper loader 一樣，不使用零量 K 棒更新價格、觸發 stop／take-profit 或重訂限價。
- 正量但不足一張容量仍是有效市場觀測；50% 整張容量可以為零，不能把它與零量填補混為一談。
- 缺中間分鐘時，估值只沿用同一交易日較早的已觀測 Close。`mark_source_index` 保存原始分鐘位置，`-1` 代表尚無觀測；不得使用未來價格倒填開頭。
- 估值沿用不會增加執行容量、不補 09:01 成交、不插值。實體持倉若連可用估值也沒有仍失敗。停牌跨日估值仍需要原有可驗證且已處理企業行動的來源，不由此函式自動製造。
- source schedule ABI 升為 v2；正式報表串接尚未完成，不能把 metadata 已有當成正式訓練報告已發布。

### 來源報告不再混稱阻擋

`audit_tw_day_trade_source_gaps.py` 新增分層判定：source inventory、trajectory accounting、annual training integration。所列 2,132 筆 `avoid` 是條款分類數，不是下載失敗數或全域訓練阻擋數。尚未評估某條實際持倉軌跡時，明示 `not_assessed`，不冒稱 ready，也不把別的股票的未知事件全加在它身上。

## 4. 測試與真實資料

最終相關回歸 **486 passed / 25.42 秒**；包括實際 CUDA 錯誤傳播測試。修改檔案 Ruff、`py_compile` 與 scoped `git diff --check` 通過。完整輸出：`artifacts/operations/daytrade_source_gaps_20260910/paper_standard_regression.log`。這不是完整市場 epoch 或訓練效能測量。

測試覆蓋：無持倉未知事件可正常入場但不領權、持有多空遇未知事件拒絕、已知正負現金 claim 在無股價時正確結算、非法 claim 拒收、精確股票日 mask 不重分配、零量極端價格不觸發 stop、缺 09:01 不倒填、CUDA 失敗原子性及舊 checkpoint 拒收。既有 paper 引擎 differential tests 逐點比較整天 270 與跨日 810 個 NAV。

真實来源比對：2026-09-09 的 2330、0050、0056、2321，共 797 根保留資料、1,080 個分鐘格，與原 paper loader 的來源 Close／last-trade-carry 結果完全一致：

- 首次觀測前 236 格仍為 NaN；沒有倒填。
- 47 格明示沿用較早成交價；没有增加成交量。
- 2330 當日 266 根、0050 與 0056 各 264 根、2321 有 3 根。270 格是估值時鐘，不代表每股真的每分鐘都有成交。
- 原始分鐘 manifest／partition 雜湊符合，來源及當日限價檔在檢查前後不變。

這是保留來源及執行語义測試，不是四檔的實際策略績效，也不證明全部年份的資料已完成驗收。

重新盤點仍為 127 個明列 provider-gap 股票日、124 檔現行合約缺失；後者在期間有 71,194 個正日成交量觀測，其中 156 股票日已有本機分鐘來源。這些是待核對／補資料清單，不自動變成全期間交易 mask。換股來源 attempt 仍有 20 件未解；未持有事件不阻擋，不代表事件已下載成功。

可重跑來源驗證指令：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_tw_day_trade_source_gaps.py \
  --public-root /srv/stockagent-live/data_tw_public \
  --start-date 2020-03-02 --end-date 2026-09-10 --focus-start 2026-02-25 \
  --output docs/DAY_TRADE_DATA_GAPS_2026-09-10.md \
  --inventory artifacts/operations/daytrade_source_gaps_20260910/inventory_paper_standard.json \
  --minute-comparison-date 2026-09-09 \
  --minute-comparison-symbols 2330,0050,0056,2321 \
  --minute-comparison-limits artifacts/live/tw_price_limits/2026-09-09.parquet
```

此指令不登入券商、不下載、不改來源／帳戶或發布冷儲存。機器驗證 receipt 保存於上述 JSON；修正說明僅使用 Markdown。
