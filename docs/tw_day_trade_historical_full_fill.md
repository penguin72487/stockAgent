# 台股當沖歷史全量紙上回放（隔離候選）

本流程只改寫**新建的反事實候選帳本**，不改原始 `artifacts/live/tw_day_trade_simulation`，不向 Shioaji 補送歷史委託，也不把紙上成交寫成券商成交。現行主服務仍是本地紙上執行；`stockagent/live/tw_stock_simulation_execution.py` 的 Shioaji 模擬委託／StockDeal 日誌尚未接到主服務。當前未完成的真實模擬委託，只能以 StockDeal、狀態更新等 API 回報決定成交量。

## 反事實規則

1. 用來源帳本鎖定每個日期、模式的訊號 ID 與檔案雜湊。若同一標的已有本地紙上 entry 成交，驗證原始 `fills.jsonl` 行雜湊，保留其原成交時間及數量加權價格，將合法且 NAV 可負擔的目標股數標為反事實全量成交。來源原成交量另行保留，**不能**推定完整股數獲券商成交。
2. 沒有既有紙上成交時，用有來源的 09:01 一分鐘 VWAP，否則用該分鐘 Close。兩者都沒有則不成交，不能把 09:00 開盤價、昨收或任意報價冒充 09:01 價格。
3. 數量不受本地 50% 分鐘量／L1 深度裁切，但保留整手、當日合法價、確切可交易資格與帳戶 NAV 檢查。日內退出只在有交易的已完成一分鐘 K 棒上假設全量；沒有可用棒或交易時間結束時保留未平倉及隔日承接，不能虛構成交。
4. 逐日產生 09:01–13:30 的 270 個右標記分鐘點。獨立分鐘重算必須和執行帳本完全相符，且不得改動委託、成交或原始來源帳本。
5. 完整重播結束時會把用到的原始紙上成交紀錄，以行雜湊篩選後存入候選內的 `prior_paper_source_records.jsonl`。候選驗證要求這份不可變快照；日後交換目錄也不能失去來源證據。較早建立但尚未存證的候選，須先用下方 `stage` 指令補齊。

## 指令

盤中僅執行低優先序、`--local-only` 的隔離重播；不要重啟主服務或升格候選。`--state-dir` 必須是不存在或空目錄：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_tw_day_trade_capacity_recovery.py \
  --ledger-dir artifacts/live/tw_day_trade_simulation \
  --through 2026-09-17 \
  --output artifacts/data_quality/tw_day_trade_capacity_recovery_2026-09-17.json \
  --missing-pairs-output artifacts/data_quality/tw_day_trade_missing_0901_price_pairs_2026-09-17.csv

run_fintech_python scripts/rebuild_tw_day_trade_open_price_replay.py \
  --state-dir artifacts/replays/CHOOSE_NEW_CANDIDATE_DIR \
  --start-date 2026-02-25 --end-date 2026-09-17 \
  --source-ledger-dir artifacts/live/tw_day_trade_simulation \
  --historical-full-fill-0901 --local-only --replay-intraday-kbars

# 僅供較早建立且未附來源快照的隔離候選；新建候選會自動執行。
run_fintech_python scripts/stage_tw_day_trade_prior_paper_source.py \
  --candidate-dir artifacts/replays/CHOOSE_NEW_CANDIDATE_DIR

run_fintech_python scripts/rebuild_tw_day_trade_minute_curves.py \
  --state-dir artifacts/replays/CHOOSE_NEW_CANDIDATE_DIR \
  --start-date 2026-02-25 --end-date 2026-09-17 \
  --output-dir artifacts/replays/CHOOSE_NEW_CURVE_CHECK_DIR \
  --simulation --recompute-existing-strategy-marks \
  --revalue-carried-marks --require-revaluation-parity --publish

run_fintech_python scripts/promote_tw_day_trade_replay.py \
  --live-dir artifacts/live/tw_day_trade_simulation \
  --candidate-dir artifacts/replays/CHOOSE_NEW_CANDIDATE_DIR \
  --expected-market tw_day_trade_100m \
  --expected-market tw_day_trade_attention_layernorm \
  --expected-market tw_day_trade_multi_basis \
  --expected-market tw_day_trade_multi_basis_22 \
  --expected-market tw_day_trade_multi_basis_projection_l1_gelu \
  --allow-margin-carry --validate-only \
  --validation-receipt artifacts/data_quality/CHOOSE_VALIDATION_RECEIPT.json
```

用 `scripts/promote_tw_day_trade_replay.py` 的只讀候選驗證確認官方日曆、來源、帳務、缺口、分鐘點、未平倉和交易模式；**不要**在開盤服務持有倉位或仍寫帳時交換正式目錄。即使歷史候選合格，也不代表現行主服務改成了 Shioaji 模擬下單。

`missing_0901_price_pairs` 是舊帳本缺價清單，不等於重新查詢後仍缺；應先用既有 Shioaji 一分鐘 K 棒下載器在非交易時段按其帳號額度補來源，再重建隔離候選。對官方或已發布資料確實沒有價格的標的，仍須明列為無法回填。

## 2026-09-18 隔離驗收結果

- 候選 `artifacts/replays/tw_day_trade_full_target_2026-02-25_to_2026-09-17_candidate_v3`：141 個已完成交易日、5 模式、705 組來源訊號註冊；45,290 筆進場沿用原紙上成交時間／價格，9,819 筆使用來源 09:01 分鐘價。新候選沒有本地容量裁切缺口，但仍有 30,138 列／約 9,999.8 萬股缺 09:01 價格，不能捏造成交。
- `minute_curve_receipt.json`：190,350 個右標記點，每個交易日與模式皆為 270 點；56,442 組估值來源全部可用，獨立估值與原始權益差異 0、API 請求 0。
- `artifacts/data_quality/tw_day_trade_full_target_candidate_v3_validation.json`：`status=validated`；來源／帳務完整範圍稽核 141 日、5,746 個分鐘來源檔、錯誤 0。它只證明**隔離反事實候選**符合此契約，不證明這些數量在交易所或券商模擬環境全數成交。
- 截至驗收時，正式服務仍有未平倉的本地紙上部位，不符合安全切換門檻；未執行交換或改寫原始帳本。主服務尚未接入 Shioaji 模擬委託／StockDeal，不能稱它目前是 API 成交帳本。
