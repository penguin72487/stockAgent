# 指定 margin-components Fold 14 的績效與訓練診斷

本次分析對應使用者指定的 `artifacts/runtime/tw_futures_margin/source/train.py` 與
`configs/markets/tw_futures_v8_margin_components_capital100m_20261003.yaml`。
結論是：虧損主要來自實際持倉損益，交易成本進一步拖累；訓練後期另有確定的學習率下限接線錯誤。
目前證據不能把虧損全歸因於帳戶結算 bug，也不能聲稱修正 scheduler 就會超越大台。

分析於 2026-10-04 執行。原訓練於臺北時間 2026-10-04 14:46 完成；本次沒有啟動正式訓練、修改設定、修改封存 runtime 或覆寫原訓練產物。
以下所有比例都以實際完成的 Fold 14 為準。

**先固定比較對象。**

設定的 output root 名稱雖然包含 `fold10`，`run_manifest.json`、完成標記和 checkpoint 顯示選定的是 Fold 14。
訓練年度為 2011–2024，驗證年度為 2025，測試資料實際涵蓋 2026-01-02 至 2026-09-04，共 163 個交易日。
初始資金一億元，整數期貨口數、保證金帳戶與跨日持倉；不是只交易大小台的策略。

| 相同測試日期 | 模型，扣費稅 | 大台近月連續轉倉，1x gross |
| --- | ---: | ---: |
| 累積報酬 | −36.53% | +54.52% |
| Sharpe | −1.23 | 2.03 |
| Sortino | −1.63 | 3.09 |
| MDD，絕對值 | 52.24% | 17.22% |

Sharpe/Sortino 使用專案 canonical 對數日報酬口徑，年化日數 252；若改用簡單日報酬，模型 Sharpe 為 −0.94，不能把兩種口徑混在同一比較中。
大台參考線沿用 `load_tx_front_rolling_benchmark()`：近月每日 close-to-close、轉倉日使用新合約自己的前一收盤價，不把兩個不同合約的價差算成獲利。
此參考線是未扣費的 1x 指數期貨報酬，不是一口固定口數、保證金槓桿帳戶或可成交填單紀錄。

原產物的 `benchmark_contract` 實際是 `flat_cash_nominal_twd_no_interest`，163 列 benchmark return 全為零。
本次沒有改寫它，而是在完全相同的測試日期另行重算大台參考線。
參考來源為 `verified_scope_2011_20260928/daily/continuous_daily.parquet`，SHA-256 為
`8f3f62da483b6b7ee1652b9c615be8f7b41b4305cfca84a459fa3aeb8e5ed551`。
模型與大台日對數報酬相關係數約 0.112；低相關性已經存在，但沒有轉化成正報酬。

**從帳戶恆等式分辨成本、行情與結算。**

令 (q_t) 為實際成交後的整數口數，(V^o_t,V^m_t) 為當日每口開盤與結算合約價值，單位都是新臺幣。
跨日損益必須用同一物理合約的前次結算價值、當日開盤價值，以及有日期依據的公司行動現金計算；不能直接相減兩條連續價格序列。

\[
E_t-E_{t-1}
=\mathrm{overnight\ PnL}_t
+\sum_i q_{t,i}(V^m_{t,i}-V^o_{t,i})
+\mathrm{terminal\ adjustment}_t
-\mathrm{commission}_t-\mathrm{tax}_t.
\]

本次使用保存的逐日口數、carry 映射與官方／研究 ABI 的合約價值欄位對帳，沒有重新訓練或改造執行器。
163 日逐日誤差最大為 12 元；相對一億資金是 FP32 金額計算級別的差異。

| 損益來源 | 新臺幣 |
| --- | ---: |
| 跨日損益，包含已保存的日期公司行動現金 | −5,472,828 |
| 開盤至結算／終止事件的持倉損益 | −25,100,535 |
| 持倉毛損益合計 | −30,573,363 |
| 手續費 | −5,556,640 |
| 交易稅 | −404,871 |
| 實際帳戶損益 | −36,534,828 |

期末資金 63,465,172 元。費稅約占總虧損 16.3%；持倉損益占約 83.7%。
這是「已執行持倉」的算術歸因，不能把毛損益當成移除費用後重新交易的策略結果；費用改變 NAV 後也會改變可交易整數口數。
但它足以排除「只降低交易成本就能把目前策略變成盈利」這個解釋。
開盤至結算部分，多單毛損益約 −888 萬、空單約 −1,622 萬；兩側都虧損，不能據此把禁止放空當成修正。

沒有觀察到 settlement default、margin call、開盤強平或未完成強平，期末 alive 為 1。
這份測試的損失不是因為帳戶破產後的零曲線，也不是保證金強平反覆觸發。
逐日帳戶加總能對上，不代表所有價格來源都是可成交價格，也不代表整數交易的 surrogate 梯度已經正確。

**真正需要改善的是持倉決策與風險單位。**

實際平均總名目曝險為開盤權益的 2.54 倍，最大約 3.87 倍；平均多方 1.45 倍、空方 1.09 倍。
保證金可負擔只表示訂單／帳戶合法：

\[
\sum_i |q_i|M_i+\mathrm{required\ reserves}\le E
\]

並不表示這些部位的報酬波動很低。(V_i/M_i) 不同時，同樣的保證金配置可以產生不同的名目曝險與損益敏感度。
保證金帳戶的現金餘額也不能直接當成名目本金未投入的比例。

| 月份 | 模型報酬 | 大台報酬 | 平均多方／空方名目曝險，相對當日開盤 NAV |
| --- | ---: | ---: | ---: |
| 2026-01 | −11.80% | +10.29% | 0.31x / 1.66x |
| 2026-02 | −20.52% | +10.02% | 0.54x / 2.04x |
| 2026-03 | +11.96% | −10.93% | 0.45x / 1.93x |
| 2026-04 | −12.16% | +21.97% | 0.92x / 1.51x |
| 2026-07 | −20.68% | −6.98% | 2.37x / 0.29x |

一、二月的大空方曝險及七月較大的多方曝險，都與實際虧損同時出現。
這支持優先檢查方向判斷、名目風險辨識與部位調整能力，而不是先增加 epochs。
表內大台不是每個個股期貨的底層標的；不能用這張表把所有商品損益精確歸因成單一市場 beta。
逐商品 CSV 只歸因開盤至結算／終止事件與該商品費稅，不分派沒有商品軸的 overnight audit。

平均每天交易約 811 口，每天持有約 84 個物理合約槽位。
有 2,517 個持倉合約日沒有當日來源觀測，但這些列沒有新交易；source non-executable 列的交易口數也為零。
因此不能把 carry 估值列誤讀成虛構的新成交。
本次來源仍使用明確命名的 `frozen_contract_value_research_v1`；daily/rules manifest 的 `point_in_time_verified` 都是 false。
報告中的收益是該研究 ABI 下的結果，不能擴張成所有歷史規則都已正式 PIT 驗證。

**loss 變平不等於策略已經學好。**

保存的 loss 是 `log_utility`，輔助 direction/rank/volatility loss 均為零，turnover 額外懲罰也是零。
持倉費稅已經包含在 NAV 中，不能把 `gamma_turnover=0` 解讀成完全沒有扣交易成本。
目前主目標為：

\[
L(\theta)=-252\,\mathrm{mean}\left[\log(E_t/E_{t-1})\right].
\]

這裡 `gamma_sharpe=1` 是此分支的倍率，不是 Sharpe ratio；配置中的 CVaR／drawdown 係數沒有自動把這個目標變成 Sharpe 或 MDD 最佳化。
對簡單日報酬 (R) 做小變動展開，有
\(\mathbb E[\log(1+R)]\approx\mathbb E[R]-\tfrac12\mathbb E[R^2]\)。
因此 log growth 有波動代價，但不等於最佳 Sharpe；MDD 依賴整條資金路徑，也不是平均值能完全描述。

記錄共 102 epochs，每個 epoch 只有一次完整 trajectory 的 optimizer update；這是選定的 exact-account 更新契約，不是少跑 batch 的錯誤。
最佳驗證 checkpoint 在第 2 epoch，之後 100 次驗證沒有改善，正常觸發使用者設定的早停。
這不是候選搜尋耗盡，也不是 1000-epoch 上限沒有生效。

| epoch | train loss | validation loss | LR |
| --- | ---: | ---: | ---: |
| 1 | 0.472417 | −0.157058 | 1.00e−5 |
| 2，選中 | 0.461606 | −0.173439 | 1.00e−5 |
| 50 | 0.050018 | −0.137678 | 6.25e−7 |
| 102 | 0.026271 | −0.120857 | 1.95e−8 |

train loss 全程仍為正；在保存的純 log-utility 契約下，不能描述成「模型在訓練上大賺，僅測試過擬合」。
更準確的觀察是訓練目標改善、驗證早早停在最佳點，後期又接近學習率凍結。
訓練 loss 是逐段記錄的 trajectory 目標，驗證是固定模型的完整評估；兩者也不是同一批資料上的相同計算。

102 次更新的全域梯度都非零，clip 前 norm 為 4.77–7.50，每次超過設定的 clip norm 1。
這僅證明有全域訓練訊號，不證明方向／現金／個別商品 head 的梯度有效。
AdamW 的 adaptive normalization 使全域 clipping 不能簡化成「步長必定縮小到 1/7」；不能只據此移除 clip。
第 2 至 102 epoch，`futures_action_head.weight` 相對 L2 移動約 0.188%，bias 約 0.308%。
匯出的 `model.pt` 與最佳 checkpoint 的所有 state tensors 位元相同，沒有觀察到匯出成最後一個 epoch 的錯誤。

**已確認的工程錯誤與仍待重播的問題。**

1. 本次封存 runtime 的 `_create_lr_scheduler()` 沒有向 `ReduceLROnPlateau` 傳入 `min_lr`，因此 `lr_scheduler_eta_min: 1e-6` 對 Plateau 不生效。
   同時使用預設相對 threshold；對可為負數的 loss，應明確使用絕對改善門檻。
   官方 API 的預設是 `min_lr=0`、`threshold_mode='rel'`，見 [PyTorch 文件](https://docs.pytorch.org/docs/stable/generated/torch.optim.lr_scheduler.ReduceLROnPlateau.html)。
   用原始 factory 重播全部已記錄 validation loss，學習率與原曲線誤差為零。
   當前 repository 的 factory 已接上 `min_lr` 與 `threshold_mode='abs'`，同一 loss 序列會停在 1e−6；相關現有回歸測試本次通過 1 項。
   這只是 scheduler 重播，不是修正後策略的回測。現有回測使用 epoch 2，當時 LR 尚未降低，因此此 bug 不能單獨解釋現有虧損。

2. 所指命令使用的封存來源並非當前 repository：run provenance 的 git head 為 `e480264a...`，而本次 repository 為 `c20dbe668...`。
   保存的梯度 contract 為 v11，當前主程式為 v12；更新根目錄程式不會自動更新 `artifacts/runtime/tw_futures_margin/source`。
   v12 對終止事件失敗梯度的修正不應被宣稱是本次全部虧損的原因；目前沒有紀錄到對應 default 事件。

3. 原 benchmark 是現金，且年度切法仍是 calendar，沒有使用先前要求的 annual lookback-shifted +32。
   所以這個命令的產物仍然包含一月。新實驗應明確選擇 `walk_forward.year_boundary_mode: lookback_shifted`、保留 lookback 32 與 `panel_history`，並重新驗證實際 session 邊界。
   主程式現有大台設定入口是 `trading.tw_futures_portfolio_benchmark_mode: tx_front_rolling_1x_gross`，外部 benchmark 路徑必須同時 pin SHA-256。
   更換切片後須使用新測試日期重算 benchmark，不能挪用本報告整個 1–9 月的數字。

4. epoch 2 的 sampled-test objective 為 0.677565、列數 163；完成 NPZ 的 NAV 對應 `-252*mean(log_return)` 為 0.702939。
   差距不能只用「有抽樣標籤」解釋，因為列數也為 163。曲線沒有保存逐列動作，尚未完成相同 checkpoint、相同輸入、相同 dtype／分批／rank 的成對重播，根因仍未確認。
   工程修正前應先比較 logits、映射後請求、整數口數、carry 與 NAV；金融整數執行可能把很小的推論差異放大成不同交易。
   正式績效以完成的 NAV 產物為準，不用曲線的 test loss 替代。

**建議的改善順序，以模型能力與可驗證規則為主。**

1. 先把已存在的工程修正整合進新的 canonical source release，接上 LR 下限、負 loss 的絕對門檻、正確 TX benchmark 與 +32 年度切法。
   保留初始一億、合法可用的全部商品、可學現金、原本整數口數／費稅／保證金／carry 規則，以及最多 1000 epochs、100 次驗證無改善早停。
   原 Fold 14 已完成，`--resume --no-retrain-completed-folds` 會沿用／跳過它；不能把重新執行同一命令當成修正後的新訓練。
   更新 source、梯度、split 或 optimizer 契約時使用相容的新 artifact root，不接續不相容的 optimizer 狀態。

2. 對梯度做有意義的離散診斷。整數口數的 forward 是階梯函數，autograd 的非零 surrogate 梯度本來不是它的精確解析導數。
   需在固定訓練歷史和合法帳戶狀態下，對可執行候選加／減一口，用 canonical executor 比較扣費後 loss 改善方向與 surrogate 方向。
   分別記錄方向、現金／曝險、稀疏支持集合、未持倉候選與資金不足邊界的有效梯度；費用、禁止成交、公司行動、到期與 overnight 都要沿用原契約。
   先解決上述 curve/final 重播差異，再根據量測調整 LR／更新 cadence，不能把每 batch 更新當成無語意代價的加速。

3. 增加模型對自身可執行狀態的辨識。帳戶最適策略的狀態包含市場資訊、實際既有口數、NAV、決策時已知的合約名目本金／保證金與換倉成本。
   同一市場狀態下，原本持有 +10 口與 −10 口，其最適交易與成本不同；只有市場特徵不足以直接辨認這個差別。
   目前模型已接上底層股票嵌入、期貨候選、保證金編碼與共同市場注意力；應沿用它們。
   此輸出 adapter 的候選欄位沒有逐日實際已成交的帳戶持倉/NAV 狀態；擴充前仍要核對原有 execution context，避免重複新增通道。
   可由維持因果時鐘的 recurrent policy context 提供這些事實，讓模型學會持有、減碼、換倉與現金，不靠固定 top-K、固定曝險上限或禁止放空來美化曲線。
   此項是能力改善提案，尚未正式訓練或證明能提高收益。

4. 讓驗證與目標一致。保留完整淨值的真實 log-growth 訓練訊號；在驗證端清楚列出 Return、Sharpe、Sortino、MDD 及同日期 TX 指標，而不是把 `gamma_sharpe` 的名稱當作 Sharpe 訓練。
   若更換訓練 utility 或 checkpoint 選取標準，先說明其風險／收益取捨並更新契約，不額外插入執行時的人為持倉規則。
   2025 一年選出的 epoch 2 在 2026 失效，應增加較早年份的時間順序驗證證據，確認不是單一年份適配；可用現有 walk-forward lifecycle，不需要另一套訓練迴圈。
   2026 已被本次分析查看，之後針對它反覆改模型所得的提升應標為研究結果；需要較早未參與選模的 folds 或新資料作額外驗證。
   反覆試驗造成的選模偏誤見作者原文 [The Probability of Backtest Overfitting](https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf)。

**本次交付的可檢查證據。**

- [完整診斷圖](../artifacts/analysis/tw_futures_components_fold14_performance_20261004/diagnostics.png)：同日期權益、未平滑 loss、scheduler 重播、持倉毛損益與費稅歸因。
- [Canonical lifecycle／NAV 稽核](../artifacts/analysis/tw_futures_components_fold14_performance_20261004/canonical_audit.json)。
- [TX 比較及來源 SHA](../artifacts/analysis/tw_futures_components_fold14_performance_20261004/benchmark_comparison.json)。
- [逐日帳戶對帳](../artifacts/analysis/tw_futures_components_fold14_performance_20261004/book_attribution.json)、[逐日 CSV](../artifacts/analysis/tw_futures_components_fold14_performance_20261004/daily_book_attribution.csv)、[逐月 CSV](../artifacts/analysis/tw_futures_components_fold14_performance_20261004/monthly_diagnostics.csv)。
- [商品的日內／終止損益與費稅](../artifacts/analysis/tw_futures_components_fold14_performance_20261004/within_session_product_attribution.csv)：不包含 overnight 分派。
- [訓練與 checkpoint 診斷](../artifacts/analysis/tw_futures_components_fold14_performance_20261004/training_diagnostics.json)、[原 factory／當前 factory 的 scheduler 重播](../artifacts/analysis/tw_futures_components_fold14_performance_20261004/scheduler_reproduction.json)。
- 重跑 CPU 歸因：`source scripts/runtime_env.sh`，再執行 `run_fintech_python artifacts/analysis/tw_futures_components_fold14_performance_20261004/attribute_saved_book.py`。
- 重跑 CPU 診斷與圖表：執行 `run_fintech_python artifacts/analysis/tw_futures_components_fold14_performance_20261004/analyze_training.py`。

Canonical lifecycle 驗收成功與每日帳戶可對帳，是工程證據；目前策略仍沒有達到報酬、Sharpe 和 MDD 優於大台的目標。
本報告沒有以增加測試次數、平滑曲線、改成零部位初始狀態或忽略費稅宣稱策略改善。
