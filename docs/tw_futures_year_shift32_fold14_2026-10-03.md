本次修正把期貨訓練的年度所有權改成使用者指定的「每年首個交易日＋lookback，到下一年首個交易日＋lookback」，適用於新版 margin-components 實驗。lookback=32 代表往後移 32 個實際交易日，從該年的第 33 個交易日開始。

舊設定的 `lookback_context: panel_history` 只允許模型取得切片之前的歷史特徵，並不會移動年度邊界。原本仍由曆年切分 train/validation/test，因此 Fold 14 測試包含 2026 年一月。這是切片設定與要求不一致，不能靠把圖上的一月隱藏來修正。

令完整交易日曆為 `d[0], ..., d[T-1]`，`s_y` 為日曆中年份 y 的第一個交易日索引，則年度開始索引 `b_y = s_y + training.lookback`。年度 y 的所有權為半開區間 `[b_y, b_(y+1))`，最後一個尚未結束的年度延伸到已驗證資料的最後一天。下一曆年最初 32 個交易日歸前一年度，不會被刪掉；相鄰 folds 也在相同邊界切換模型。

使用目前已驗證行情截至 2026-09-04 的完整 3,881 個交易日，Fold 14 如下：

| 區間 | 年度標籤 | 首日 | 末日 | 交易日數 |
| --- | --- | --- | --- | ---: |
| 訓練 | 2011–2024 | 2011-02-24 | 2025-02-25 | 3,430 |
| 驗證 | 2025 | 2025-02-26 | 2026-02-25 | 243 |
| 測試 | 2026 | 2026-02-26 | 2026-09-04 | 131 |

因此，Fold 14 的測試曲線從 2026-02-26 開始，策略與大台近月持續轉倉 benchmark 使用相同的 131 日。2026-01-02 到 2026-02-25 歸驗證年度 2025；若訓練所有 folds，這段期間也由前一 fold 負責測試與部署。完整連續走勢仍會包含一月，那是前一年度模型的合法交易紀錄。

切片、checkpoint 相容性、首個測試年度抽樣、年度報表與跨 fold 曲線共用這個邊界。每次執行會寫出 `walkforward_period_boundaries.json`，記錄完整日曆摘要、SHA-256、位移單位及每年度的開始日期。報表用這份完整日曆證據標記日期，不會在某個子區間內重新數 32 天。`panel_history` 保留因果特徵上下文，不再丟棄一輪 warmup。

新設定為 `configs/markets/tw_futures_v8_margin_components_repaired_year_shift32_capital100m_fold14_v3.yaml`，繼承 v2，差異只有年度所有權和獨立產物目錄：

```yaml
training:
  lookback: 32
walk_forward:
  year_boundary_mode: lookback_shifted
  lookback_context: panel_history
runner:
  output_dir: artifacts/markets/tw_futures_v8_margin_components_repaired_year_shift32_capital100m_fold14_v3
  start_fold: 14
  resume: false
```

邊界位移從 `training.lookback` 推導，修改 lookback 時會一起更新，不另外增加一個可互相矛盾的位移參數。`split_only` 會再丟棄一次 warmup，所以與新模式並用會明確拒絕。原本的曆年模式仍可重播舊實驗；新模式的 checkpoint 指紋不同，不能續接舊切法的 optimizer 或完成標記。

本次保留 v2 的一億元起始資金、全部已驗證期貨候選、整數口數／保證金／逐日結算、交易成本、TX benchmark、普通隨機初始化、梯度 v12、BF16/DDP、最大 1,000 epochs，以及連續 100 次驗證未改善的早停。年度 ownership 的修正本身不能證明 Sharpe 或報酬改善。

新版凍結程式位於 `artifacts/runtime/tw_futures_margin_year_shift_v3/source`，程式 SHA-256 為 `c643fdaf79d8eacca2183bfe67a558d581f2e9eb6b86f956d74fb235bd8e0f71`；receipt 及 wheel/source bundles 已驗證，共 1,096 個 receipt-owned 檔案。主工作樹目前有未完成 Git merge 衝突，所以本次使用獨立程式，不改正在使用的 v2 runtime。可檢視的實作差異在 `artifacts/analysis/tw_futures_year_shift32_fold14_20261003/implementation.patch`。

自行開始 Fold 14 的指令如下，不開 profiler，不使用舊切法的 `--resume`：

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
export STOCKAGENT_CODE_RELEASE_RECEIPT="$PWD/artifacts/runtime/tw_futures_margin_year_shift_v3/code-release/release.json"

run_fintech_python artifacts/runtime/tw_futures_margin_year_shift_v3/source/train.py \
  --config configs/markets/tw_futures_v8_margin_components_repaired_year_shift32_capital100m_fold14_v3.yaml \
  --start-fold 14 --max-folds 1 \
  --no-retrain-completed-folds
```

若要從 Fold 1 訓練全部 folds，改用 `--start-fold 1` 並移除 `--max-folds 1`。這個新產物目錄必須從頭訓練；舊曲線和 checkpoint 沒有被改寫，本次也沒有啟動或停止正式訓練。

工程驗證為 33 項測試通過，包含實際 ownership 的無漏日／無重疊、歷史特徵上下文、下一年一月仍歸前年度、model handoff、部分末年、不相容 checkpoint 拒絕、年度報酬可複合回完整報酬，以及首個測試年度曲線的跨年保留。完整資料預檢也已通過：3,881 個交易日、2,754 個股票上下文槽位、99 個特徵、Fold 14；實際預檢生成的日曆邊界證據與上述計算完全一致。預檢沒有建立模型、optimizer、checkpoint 或 fold 完成標記。

實際日曆證據保存在 `artifacts/analysis/tw_futures_year_shift32_fold14_20261003/boundary_audit.json`；完整資料預檢證據另存於同目錄的 `preflight.log`、`preflight/`，最終工程驗收為 `engineering_acceptance.json`。已合併解析的設定差異 `effective_config_diff.json` 證明，與 v2 相比只有實驗名稱、產物目錄及 `walk_forward.year_boundary_mode` 三個欄位不同。
