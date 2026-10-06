# 全期貨模型的因果帳戶回饋

本次改善的是策略能觀測與回應的資訊。原模型按市場資料批次輸出配置，實際成交口數、NAV、費稅與保證金占用卻只在後面的帳本出現；相同市場訊號下，持有五口與沒有持倉，原模型會提出相同配置。因此不能期待它完整學到持有、換倉成本與資金變化的影響。

實作採用批次市場係數加逐日帳戶回饋，沿用 canonical `train.py → risk_aware_loss → run_backtest_torch → integer margin ledger`。沒有第二個訓練迴圈，也沒有新增選股數、手動曝險上限或現金比例規則。新設定為 [causal account v5](../configs/markets/tw_futures_v8_margin_components_causal_account_capital100m_v5.yaml)。

**先固定實驗與會計邊界。**

- 初始一億元、已支援的全期貨物理合約軸、整數口數、跨日保證金、原費稅／容量／持倉限制／公司行動／結算／破產吸收規則。
- 沿用使用者指定 `margin_components_current` 的 daily 與 schema-8 rules。daily SHA-256 為 `6e1a8e652a64f1a6cd20a33ddc112beec2daaeefca73f58ffde0d6ba0569560a`；rules 為 `43f7fe780a59d1d8ea5aa0c05f9967acf5a961cec6af68d58920cc4928cb3e20`。
- 該資料的 `frozen_contract_value_research_v1` 與歷史持倉規則研究假設保持明列；來源 receipt 完整不等於全部金融數值為官方歷史 PIT。這次不改資料的估值或合法交易範圍。
- 每年的 target 區間為第一個交易日往後 32 個 sessions，到次年相同偏移前，使用 `lookback_shifted` 與 `panel_history`；仍使用同日期 TX 近月持續轉倉 1x gross comparator。
- 正式設定仍最多 1000 epochs，每 epoch 驗證，連續 100 次未改善早停；學習率 1e-5，plateau 下限 1e-6；每完整訓練軌跡更新一次 optimizer。原非零市場配置初始化保留。

**從狀態、決策到財富的推導。**

令市場歷史為 \(x_{t-1}\)，前一次真正成交並完成結算的口數、NAV 為 \((q_{t-1},E_{t-1})\)。最低限度的策略形式是 \(a_t=\pi(x_{t-1},q_{t-1},E_{t-1},\text{known rules/costs})\)。事後回填另一個策略的持倉，或把當天結算 NAV 當輸入，都不符合這個因果關係。

市場 backbone 批次計算每個物理合約的基礎 logit \(b_{t,i}\) 與 16 個可學習回應係數 \(c_{t,i,k}\)。exact 帳本在逐日 recurrence 中提供真實狀態：

\[
z_{t,i}=b_{t,i}+\sum_k c_{t,i,k}\phi_k(q_{t-1},E_{t-1},\text{prior observations}),
\qquad w_t=\texttt{score\_entmax\_log\_cash}(z_t,\text{existing candidate mask}).
\]

\(w\) 是有正負方向的保證金預算；實際口數仍由原整數帳本挑選。模型可學習維持、減少、反向與增加配置；總配置絕對值小於一的部分沿用原輸出模式的現金選擇。保證金未使用比例與名目本金未使用比例是不同概念，不能混寫成同一個現金指標。

| 資訊 | 16 維狀態中的表示 | 來源與時間 |
| --- | --- | --- |
| 實際持倉 | signed notional/NAV、signed margin/NAV、absolute held margin/NAV | 前次 exact 成交後留存口數；已公告公司行動可先轉換物理身分／口數 |
| 整口與成本 | 一口 IM/NAV、持有部位 roundtrip cost/NAV、每口 roundtrip cost/notional | 開盤前已知 IM、固定費與依前次結算估計的稅，不使用今天開盤稅額 |
| 合約槓桿與風險 | notional/IM、持倉 notional × prior RMS/NAV、prior RMS、觀測覆蓋率 | 原因果合約價值與過去 32 sessions 的已觀測結算報酬 |
| 帳戶 | log NAV/initial、gross/net notional/NAV、IM/MM/NAV、free collateral/NAV | 前次結算 NAV 與實際 retained book |

`asinh`、`log1p`、百分比與 basis points 只是單位／輸入尺度轉換，不是減碼觸發器。未獲准成為新候選的 retained contract 仍屬帳戶風險：若沒有當前 candidate margin context，使用 executor 中前次結算 mark、IM、MM 的因果欄位計入全帳戶風險。

波動統計只納入兩端都實際觀測、同一物理合約的已完成報酬，先移到下一個 panel session 才做 rolling 計算。缺失／合成估值不算零報酬；以觀測數除以 32 明列短歷史與缺失。更改未來來源列不能改變過去的風險欄位。

NAV 輸入在今日 gap PnL、今日開盤現金／成交和今日結算之前取值；hard executor 依原規則處理當天價格、可成交性與可用資金。eligibility mask 仍是原 ABI 的合法候選邊界，不是新增 alpha 訊號。

**梯度與效率的界線。**

模型新增 `Linear(d_model,16)`（目前 d_model=32，即 528 個 trainable parameters）。只把新的 residual head 初始化為零；原 action head 與 backbone 不歸零，也保留建立新 head 前後的 RNG，避免改掉原 dropout 初始序列。

另外開啟既有 `futures_margin_amount_context`：原設定只有 IM／名目本金和 MM／IM 比率，新設定也提供 IM 的絕對額。兩維改三維的 margin encoder 在 d_model=32 時多 32 個參數；回饋 head 的 528 個參數與它分開計算。初始化一致性測試比較的是同樣開啟 amount context 的 market-only 控制，沒有宣稱改變輸入維度後仍逐位重現原舊模型。

批次前向產生 `[T,S,25]` 的 versioned policy packet（market-only weights、logits、16 coefficients、mask 與 6 個已知觀測）。它不是 `[T,S]` 可直接執行配置。帳本逐日解析並輸出實際 `[T,S]` requested weights；報告與 final backtest 保存實際要求及實際整數成交，不能把係數封包當配置存檔。

模型改為狀態回饋後，跨 fold 的接續 replay 也必須重新解析動作。各 fold 從重設資金產生的 `[T,S]` requests，不能用來代替延續前一 fold 口數與 NAV 的決策。因此 owned deployment NPZ 另存 `futures_account_policy_packet` 與 version，canonical stitched ledger 在延續帳戶中重新套用係數；完整重疊 test NPZ 不重複保存多年封包。完成 gate 要求這項報告契約與 owned packet，缺少時拒絕拼接。測試已證明同一係數在 reset／carried book 下產生不同配置，且拼接口數／requests／NAV 與 uninterrupted exact replay 相同。

exact forward 完全使用整數帳本；backward 沿用錨定實際成交／結算的 physical STE，並在 shadow recurrence 中再次解析狀態回饋，使該 recurrence 範圍內的早期決策能影響後續 NAV／部位／決策的梯度。捕獲的 recurrent block 與 batch 邊界仍從 detached exact state 開始，因此是截斷的、帶偏的離散控制梯度估計，不是整數交易函數的精確導數，也不是無限長 BPTT。已知狀態會跨邊界繼續傳遞，梯度的信用分配範圍則較短；不能把「看得到持倉」誤當成「能對所有未來持有收益完整反傳」。

全部 trainable head 運算仍在 DDP model forward 內；帳本只讀係數與已知觀測，避免在 DDP forward 外呼叫新的神經網路參數。FP32 金融欄位不因 BF16 AMP 改成永久 BF16 儲存。沿用 model batch、exact ledger CUDA graph 與原生命週期；目前尚未宣稱提高吞吐量。

第一次真實跨 fold 驗證在 Fold 2 編譯時出現 OOM：前一 fold 完成後，rank 0／1 在新資料快取之前只剩 10.59／3.46 GiB，失敗時仍有 5.08 GiB 的 private graph pools。`empty_cache()` 不會釋放仍被引用的張量／graph。新配置啟用既有 `runner.isolate_train_folds`，由 canonical launcher 依序使用新的 DDP 子程序，退出即回收該 fold 的 CUDA context，並保留每 fold 的完整評估、報表與 stitched deployment。最終完整帳戶回饋候選已完成 Fold 1／2；兩個 ranks 的 FP32 GPU cache 與 forward/backward compile probes 均通過，沒有重現跨 fold OOM。

另外修正報告切片：原年度統計與「首年」圖仍按曆年，漏掉次年模型切換前的 1 月。現在統一讀取完整 panel 的 `walkforward_period_boundaries.json`，子集不自行重算 +32，也不刪除次年尚屬前一期的交易日。年度報告契約為 version 1。這是報告修正，不改策略報酬序列、成交或 checkpoint；短程產物完成後以 canonical report functions 刷新，並對所有 NPZ／checkpoint 做前後 SHA-256 比對。

**驗證與實際結果。**

測試涵蓋：零 residual 與 legacy exact 成交／NAV 一致、非零市場初始化、持倉/NAV/成本/風險的決策敏感度、未來價格與來源列的 prefix 因果性、企業移轉、未獲准持倉的全帳戶風險、padding、chunk carry、破產吸收、canonical loss/evaluator、checkpoint 拒絕舊 ABI，以及 BF16 CUDA graph forward/backward 一致性。新模式 13 項測試（含 CUDA graph）、124 項回測／接續／生命週期回歸，以及年度修正後 64 項相關回歸均通過；部分測試重疊，不能加總成獨立測試數。

較早 folds 的短程 DDP 驗證使用正式 trainer 完整 train/val/test、curves、checkpoint 和 artifact gate；只在獨立 smoke root 將 epochs 限為 3。兩 folds 已完成，每 epoch 一次 optimizer update，梯度有限且非零；best/last checkpoint 的新回饋 weight／bias 都從零學成非零。這可以驗證真實資料與跨 fold 整合，不能據此證明收斂或策略優於大台。

以下為按完整 panel +32 邊界切出的首測試期，每個 fold 重設初始帳戶；使用 canonical log-return Sharpe，MDD 顯示損失幅度。不是多年重疊 test 的績效，也不是接續帳戶的統計。

| Fold／首測試期 | 策略報酬 | TX 報酬 | 策略 Sharpe | TX Sharpe | 策略 MDD | TX MDD |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1：2013-02-25～2014-02-24 | 8.56% | 14.10% | 0.306 | 1.115 | 19.42% | 8.18% |
| 2：2014-02-25～2015-02-25 | -36.81% | 15.42% | -1.499 | 1.311 | 42.63% | 11.50% |

真正接續帳戶涵蓋 494 sessions，2013-02-25～2015-02-25：初始 100,000,000 → 68,451,712 TWD，報酬 -31.55%、Sharpe -0.664、Sortino -0.858、MDD 48.15%；同期 TX 報酬 31.69%、Sharpe 1.208。接續帳戶沒有 default，gross notional/open equity 平均 2.28x、最高 3.89x；可合法交易不代表曝險適合或策略有效。

Fold 1 的訓練 loss 0.1149 → -0.1257，但 2012 驗證期接近全損，val loss 16.1158 → 16.1327。Fold 2 訓練帳戶在包含 2012 的軌跡中仍破產，loss 8.1546 → 8.0105；最佳 2013 驗證期報酬 12.39%、Sharpe 0.439，後續 2014 測試期仍虧損。三次更新沒有學到足夠的生存與泛化能力，不能把 head 非零或訓練 loss 下降宣稱為績效提升；也沒有做同預算舊模型對照，因此不能由此認定新分支造成虧損。

年度 report-only refresh 前後所有 NPZ／checkpoint 的 SHA-256 相同。canonical audit 驗證了整數口數、同日期 NAV/return 一致性、跨 fold NAV 接續、日期唯一／順序、年度 ownership、policy packet version 與完整 artifact gate。收據和可重跑稽核見 [earlier_folds_audit.json](../artifacts/analysis/tw_futures_causal_account_20261004/earlier_folds_audit.json)、[earlier_fold_comparison.json](../artifacts/analysis/tw_futures_causal_account_20261004/earlier_fold_comparison.json)、[policy_head_training_evidence.json](../artifacts/analysis/tw_futures_causal_account_20261004/policy_head_training_evidence.json) 與 [report_refresh_acceptance.json](../artifacts/analysis/tw_futures_causal_account_20261004/report_refresh_acceptance.json)。正式千 epoch 尚未啟動。

一個舊 regression 原先要求「未成交 margin close」直接觸發 default 並抹去 retained book 反彈損益。已先用修改前 HEAD 的帳本重現，確認與 v12 的既有規則不符：實際 retained 口數 ±5，NAV 0.3 → 5.35，default 為零。測試改為同時驗證沒有反彈／有反彈時的實際損益與梯度；沒有修改 executor 讓測試假設成真。

完整 `test` suite 尚不能宣稱通過：13 個 collection errors 來自環境缺少 duckdb、finlab、pydantic、defusedxml。另有兩個原有 checkpoint assertions 固定在舊 margin ABI v6／舊 gradient 欄位位置；已用 HEAD 重現，這次沒有為了通過測試修改正式 ABI。詳細結果與原始 log 見 [verification_status.json](../artifacts/analysis/tw_futures_causal_account_20261004/verification_status.json)。

**剩餘限制與下一階段驗證。**

1. 這是市場依賴的 16 維狀態回應，並非任意高容量的非線性帳戶網路。持倉、NAV 和成本已能影響決策，但是否學到可泛化的持有／換倉行為，仍要靠完整較早 folds 的驗證期與真正測試期決定。
2. `notional × RMS` 是每合約的因果風險尺度；gross/net notional 是帳戶曝險，不是投組協方差。未觀測或短歷史會顯示 coverage，不能把缺失 RMS 當成已證實低風險。模型仍可自由配置，沒有新增風險觸發減碼規則。
3. 小 batch／短 recurrent block 下的梯度信用分配有截斷。若完整較早 folds 仍顯示「訓練改善而驗證惡化」，下一個可識別實驗應比較較長 recurrence 的同資料／同會計控制，另建 optimizer 指紋與產物根目錄，不能直接續接本 checkpoint，也不能靠圖表平滑掩蓋離散交易或破產。
4. 下一階段使用本 YAML 從 Fold 1 正式訓練。每個 fold 只按自己的驗證期選 checkpoint，再評估 +32 ownership 的首測試期與延續帳戶；對 TX comparator 報酬、Sharpe、Sortino、MDD 使用相同日期。成本、交易數、default、gross/net exposure 與現金配置一起檢查。這次三 epoch 診斷不參與宣稱達標，也不拿 2026 測試期來調出較早 folds 的好看結果。

**自跑與可調設定。**

凍結 source／wheel 已通過 1199 個檔案的完整性、完全相同 wheel 的重建、checkout 外 import 與 CLI 驗收。以下是正式設定的指令，和短程驗證的 3 epochs override 分開：

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
export STOCKAGENT_CODE_RELEASE_RECEIPT="$PWD/artifacts/runtime/tw_futures_causal_account_v5/code-release-v5/release.json"

run_fintech_python artifacts/runtime/tw_futures_causal_account_v5/accepted-v5/source/train.py \
  --config configs/markets/tw_futures_v8_margin_components_causal_account_capital100m_v5.yaml \
  --start-fold 1
```

設定可在上述 YAML 調整，繼承後完整設定見 `artifacts/analysis/tw_futures_causal_account_20261004/effective_config.yaml`。正式產物預定寫入 `artifacts/markets/tw_futures_v8_margin_components_causal_account_capital100m_v5`，本次沒有啟動該正式訓練。

2026-10-05 已修復來源搬移後的 benchmark 路徑；目前 checkout 的 v5 YAML 明確綁定 `data_tw_index_futures/benchmarks/` 下的新投影與 SHA，舊 `effective_config.yaml` 保留本次研究當時設定。每日 benchmark 數值完全相等，來源身分改變，不混用舊 checkpoint；詳見 [路徑修復與驗收](tw_futures_benchmark_path_repair_2026-10-05.md)。

短程訓練執行 source SHA 為 `fae68ca528e405c1c3cc509519695d8badcce7b38c4947f727cdf748010ddee2`。一般指令使用補上年度報告修正的 `63a0c930c7ba3a537e7c2470083a2e73a6217e134afe7760a69bc2a876216765`；兩者只差 `report.py` 與 trainer 的報告呼叫／年度分組／報告 metadata，model、data、ledger、loss 與 checkpoint contract 源碼皆相同，差異證據見 [report_only_source_change.json](../artifacts/analysis/tw_futures_causal_account_20261004/report_only_source_change.json)。

新 policy/model/data-feature/gradient 指紋會拒絕原 v10–v16 或 margin-components checkpoint 的 optimizer resume。需要重跑時使用獨立 output root；正常中斷後才能對同一套新 ABI 的未完成 folds 使用 `--resume --no-retrain-completed-folds`。

這個設計與有交易摩擦的 neural control 問題一致：持倉和調整成本會影響下一步可行動作與收益；可參照作者原始 [Deep Hedging](https://arxiv.org/abs/1802.03042)。它是本專案具體狀態回饋的工程推導，不是原論文保證本期貨策略績效。DDP 同步要求依 [PyTorch 官方 DDP 文件](https://docs.pytorch.org/docs/stable/generated/torch.nn.parallel.DistributedDataParallel.html)；實際成立與否以本次測試和 runtime 收據為準。
