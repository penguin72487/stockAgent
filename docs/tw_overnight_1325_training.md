# 13:25 決策、收盤進場、次一交易日開盤退出

本實驗把指定遠端當沖基準改成固定 close-to-next-open 的**隔日沖研究模式**。設定為
[`tw_overnight_1325_multi_basis_22_capital10m.yaml`](../configs/markets/tw_overnight_1325_multi_basis_22_capital10m.yaml)，
使用原有 `tw_overnight`、共同 FinancialTransformer、trainer、loss、整股帳本與 lifecycle。
使用者於 2026-09-09 明確授權：缺少 13:25 價格時，直接使用當日收盤價。
目前 v2 已啟用此研究近似，2014 起的完整資料準備已通過，使用標準 `train.py` 即可訓練。
替代樣本帶有相對於 13:25 的前視資訊；缺口不再阻擋研究，但結果不能當成嚴格 13:25 可執行績效。

## 基準來源與保留項目

2026-09-09 以 SSH 只讀查驗遠端：

`artifacts/markets/tw_day_trade_daily_multi_basis_22_effective_rank_projection_l1_tplus2_close_commission20_capital10m_v1/run_manifest.json`

其 SHA256 為 `a44a92ad8e731effe6f30fef718f138148b8817b1b091e14ed8fc89e5875bdd5`，
configuration fingerprint 為 `0ac5e79702688f497c3c0d6f45282efe18ba6cd89ff026823e974f87cdf7ee32`。
本地只讀副本位於 `artifacts/operations/tw_overnight_1325_20260909/baseline/run_manifest.json`。
[`configs/baselines/tw_day_trade_multi_basis_22_remote_v1.yaml`](../configs/baselines/tw_day_trade_multi_basis_22_remote_v1.yaml)
把實際生效的主要設定明寫出來，避免只憑同名 YAML 推定遠端實驗內容。

| 項目 | 本次契約 |
|---|---|
| 模型 | FinancialTransformer，22 temporal bases，保留各基底 effective-rank components |
| 視窗與輸入 | lookback 32；23 個已完成日線／官方特徵，加 1 個 13:25 或收盤替代價差特徵，共 24 維 |
| 主要架構 | d_model 32、原本 temporal/cross/joint 層數、latent/market tokens、projection-L1 |
| 訓練 | 1000 epochs、BF16 AMP、DDP、batch 16、lr 0.0003、weight decay 0.01、log utility、seed 42 |
| 本金 | TWD 10,000,000；整股 1,000 股／張、零股餘額留現金 |
| 手續費 | 毛佣金 0.1425%，兩折、日末退佣；兩邊分別計費 |
| 流動性 | 繼承前一完成日成交量 50% 容量上限；這是研究容量假設，不能代表集合競價深度 |
| Walk-forward | 保留 2014 起、panel-history、逐年擴張與每 fold 即時累積報告 |

來源遠端 manifest 的資料快照為 3,081 日、截至 2026-08-19；這只是該次來源基準，
新實驗另以準備時的日線、分鐘資料雜湊與實際資料日期建立自己的 checkpoint 契約。
舊當沖權重、optimizer state 及已完成 fold 不能當成本模式已完成的訓練結果。
`baseline_comparison.json` 已比對主要生效設定：唯一文字差異是將原本 include/exclude 的選取規則
展開成明列的 24 個特徵，其次序與遠端 `data_summary.feature_names` 完全一致。

## 時間與資料責任

令 `t` 與 `t+1` 為完整日線交易日曆中相鄰兩個交易日，跨週末與休市日仍按交易日處理。

1. **13:25 決策**：讀到 `t-1` 收盤為止的 23 個原特徵，加上 `t` 的已完成、右標 13:25 分鐘 Close。
   僅在該價格缺失時，依使用者授權改用當日收盤價。其餘 23 個特徵仍截至 `t-1`；
   當日最終成交量、高低價與翌日開盤不因這項例外而成為模型輸入。
2. **當日收盤集合競價進場**：模型只產生 close-entry 權重。前一 cohort 在今天開盤的退出量完全不受下午新訊號影響。
3. **次一交易日開盤全部退出**：帳本強制沖銷所有持股。開盤無法退出時進入既有 absorbing execution failure，
   不把當日晚些時候的成交冒稱為開盤沖銷，也不自動延長持倉。
4. **split 最後一天**：仍執行開盤沖銷，但不新增收盤 cohort，避免缺少下一日退出標籤的終端部位。

新增輸入通道：

```text
decision_price[t,s] = valid_price_1325[t,s] if available else valid_official_close[t,s]
next_session_1325_gap_logret[t-1,s] = log(decision_price[t,s] / official_close[t-1,s])
```

它放在 feature row `t-1`，配合共用 dataset 的 lag 1，在交易日 `t` 的決策窗中可見。
舊 `next_session_open_gap_logret` 通道被替換；不把今天完整 OHLCV 偷換成「13:25 日線」。
`overnight_1325_close_fallback_mask[t,s]` 記錄實際替代的位置，屬資料來源與報告 metadata，
不額外增加模型輸入維度。若當日收盤價也缺失、非正值、非有限值或股票當日無有效存續報價，則不替代。

保留共同 P3 action ABI `[due_exit_at_open, entry_at_open, entry_at_close]`，本設定硬性固定為
`[1, 0, w_close]`。不新增平行模型、loss、optimizer、AMP、checkpoint 或報告框架。
直接使用 simulator 時必須傳入 `overnight_fixed_close_to_open=True` 與 `portfolio_activation='pre_normalized'`。

## 與原當沖基準的必要差異

- **先實作只做多**：跨夜不能把原當沖「不限量轉融券」當成可融券證明。若要多空版本，須另確認歷史借券／融券資格、容量與費用契約。
- **股票賣出稅為 0.3%**，ETF 按既有每檔分類費率，不套用股票同日當沖的 0.15%。
- **T+2 使用原 carrying account 的逐交易日交割佇列**：交割於 T+2 session OPEN 處理，
  保留 payable、receivable、settled cash 與資金限制；不把未交割應收當成任意可用現金。
  這與來源當沖實驗名稱中的 `tplus2_close` 時點不同，已寫入新的模式與 checkpoint 契約。
- **新 artifact root**：
  `artifacts/markets/tw_overnight_1325_close_fallback_next_open_multi_basis_22_projection_l1_capital10m_v2`。
  v1 嚴格來源版本仍保留，v2 不接受其 optimizer resume。

## 成交與委託數量的證據邊界

此版本保留來源基準的 **auction-price target-weight 研究近似**：13:25 決定權重，
tensor 帳本按成交價曝險計算，整股 oracle 以成交價把目標權重換算股數，再套用共同費用、整張、容量與資金限制。
因此，它尚不等於「13:25 已送出固定股數與限價」的委託回放。若要驗證那種可實際下單的策略，
需要另接 13:25 委託股數、價格限制、資金預留與逐筆集合競價成交證據；不能從本次研究結果直接推論。

日線 official OPEN/CLOSE 在此只作為歷史競價**參考成交價**。日線或 Kbar 並不能證明排隊成交、
特定股數都能成交、收盤一定在 13:30 完成，或開盤延遲時仍能在 09:00 成交。
例如 TWSE 存在延緩收盤機制。新模式產物明寫 `historical_price_reference_not_queue_fill_proof`
及 `auction_price_target_weight_research_approximation`，不能宣稱 production ready。

制度參考：[TWSE 交易制度](https://www.twse.com.tw/zh/products/system/trading.html)、
[財政部證券交易稅說明](https://www.etax.nat.gov.tw/etwmain/tax-info/understanding/tax-knowledge/rwG2M1N)。
現有獨立 overnight paper service 的交易回執與 live fills 是另一套運行證據，本次未部署或修改其服務設定。

## 來源檢查與收盤替代

13:25 來源必須具備 `research_ready` manifest、右標時間契約、唯一交易日分區與每個分區 SHA256。
準備時檢查日期、Asia/Taipei 時間、價格有效性、同日同股票不重複，讀取前後重驗檔案與 manifest。
將準備時的 manifest 雜湊保存在 panel 與 checkpoint；訓練中不重新解析可變的最新 manifest。
已快取的日線 panel 不含此通道，13:25 資料另行掛載，避免污染共用日線 cache。

設定 `data.overnight_1325_missing_price_policy: same_session_close` 時，以下缺失均可使用當日收盤價：

- 整份分鐘來源不存在，或來源期間未涵蓋該交易日。
- 宣告的交易日分區檔案尚未存在。
- 個別股票缺 13:25 分鐘紀錄，或該價格為 null、非正值、NaN／inf。

已存在來源的雜湊錯誤、無效 manifest、時間標籤錯誤或重複紀錄仍會停止，不能拿缺價政策掩蓋資料損壞。
同時缺少 13:25 與有效收盤價的股票不能新增倉位；開盤沖銷規則保持獨立。
系統預設政策仍為 `reject`，只有本實驗明確啟用替代。

2026-09-09 完整準備結果如下，證據在
`artifacts/operations/tw_overnight_close_fallback_20260909/data_audit.json`：

| 觀察 | 數量／範圍 |
|---|---|
| 完整日線 panel | 2014-01-02 至 2026-09-09；3,096 日、2,753 檔、24 features |
| 驗證過的分鐘分區 | 1,591 個；另有 1,504 個歷史交易日缺分區 |
| 使用真實 13:25 價格 | 1,800,209 個 symbol-session |
| 使用收盤替代 | 3,936,461 個 symbol-session；逐年統計保存於 data_summary |
| 有有效決策價格 | 5,736,670 個 symbol-session |
| 無有效決策價格 | 2,783,865 個 symbol-session；包含上市前、下市後與其他無報價格位 |

這是已按研究政策準備完成的證據，不代表原始 13:25 資料完整。
v1 的阻擋紀錄仍保存在 `artifacts/operations/tw_overnight_1325_20260909/data_preflight.json`，
沒有刪除來源缺口或縮短 2014 起的正式設定。

可重跑的稽核（會建立可重建的日線 cache，不會下單）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_tw_overnight_training_data.py \
  --config configs/markets/tw_overnight_1325_multi_basis_22_capital10m.yaml \
  --output artifacts/operations/tw_overnight_1325_data_audit.json
```

訓練使用標準入口（正式設定為 1000 epochs）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py --config configs/markets/tw_overnight_1325_multi_basis_22_capital10m.yaml
```

## v2 工程驗收

新增政策、逐筆替代、既有真實價格優先、缺失收盤價、來源損壞、checkpoint 不相容與帳本失敗報告的測試。
本次 495 項隔日沖、checkpoint、模式、模型、帳本與報表回歸測試通過，見
`artifacts/operations/tw_overnight_close_fallback_20260909/final_regression.log`。

真實資料驗收使用全部 2,753 檔、24 features、22 基底、lookback 32、batch 16、BF16 與同一共同 trainer。
保留完整 panel 日曆與歷史特徵，僅將工程驗收的 train/val/test 所有權縮為 2014／2015／2020 各前 64 個交易日，
執行一個完整 epoch、驗證、test 整股回測、checkpoint 與九張累積圖。
完成後再次載入相同資料／模式契約與 checkpoint，正確跳過已完成 epoch，曲線 SHA256 不變。
此本機驗收停用 compile 與 DDP；正式 YAML 保留原基準設定，工程結果不是完整 walk-forward 績效。

在驗收過程發現，共同報表曾將帳本歸零的 `-inf` log return 當成缺值清成 0。
現已修正為保留吸收性歸零：累積報酬與最大回撤為 -100%，之後不能恢復獲利。
log-risk 統計採 float64 NAV 下溢界限作有限表示，避免平方與繪圖溢位；這些失敗情境的風險統計不作投資品質判斷。
原始回測的 `-inf`、`settlement_default` 與 `final_alive=false` 均保留。
實際測試模型出現過開盤沖銷失敗；「訓練流程完成」不等於這個未充分訓練模型可交易或有獲利能力。
具體案例是 2020-01-06 的 6188、8,000 股，當日 opening sell mask 為 false；
`execution_failure_audit.json` 保留持股與價格證據。此處帳本歸零是違反強制退出約束的吸收性失敗標記，
實際市場經濟損失與後續處理必須另以可成交證據評估。

當前證據位於 `artifacts/operations/tw_overnight_close_fallback_20260909/`：

- `data_audit.json`：完整期間資料與逐年替代數量。
- `train_smoke.py`、`train_smoke_report_v2.log`：真實資料的工程驗收範圍與流程。
- `full_universe_smoke_b16_report_v2/`：修正報表後的 checkpoint、曲線、回測與驗收回執。
- `train_smoke_resume.log`、`full_universe_smoke_b16_report_v2/resume_acceptance.json`：完成後恢復驗證。
- `final_regression.log`：495 項測試；`regression.log` 為先前 137 項針對性驗證。
- `full_universe_smoke_b16/`：保留早期發現報表錯誤的證據，不作最後驗收結果。

## v1 歷史驗收

截至本次實作，模型、P3、現金帳本、整股執行、費稅、因果遮罩、來源雜湊、checkpoint 與 resume
的 475 項共同回歸測試通過。其後新增來源固定與無效上午 action 不得稀釋收盤資金的保護測試，
13 項隔日沖測試全數通過，連同 checkpoint 的最新針對性測試為 108 項通過，
見 `test/test_tw_overnight_1325.py`、`final_contract_tests.log`。

小型完整測試使用 RTX 5070 Ti、BF16、22 個基底、24 features、lookback 32、兩檔合成股票、四個年度、
兩個 epochs、一個 fold。僅縮小資料與 epochs，停用 compile 與 DDP；不把結果當成真實策略績效或速度基準。
共同 lifecycle 驗收 18 個必要產物，九張根層級 walk-forward 圖均已產生；完成後 resume 正確跳過且 epoch 曲線 SHA256 不變。
另在固定 seed 42 的獨立測試中，刻意於 epoch 1 的 checkpoint 與曲線寫入後中斷，
恢復時載回 optimizer 與 Python/NumPy/Torch RNG，從 epoch 2 接續，曲線恰為 `[1, 2]`。
最終模型所有 state tensors 與不中斷對照通過 `rtol=1e-5, atol=1e-6` 比對。
此 baseline 不啟用 scheduler，BF16 的 scaler state 為空；沒有把停用的狀態宣稱為已啟用功能。

證據位於 `artifacts/operations/tw_overnight_1325_20260909/`：

- `final_regression.log`：共同回歸測試。
- `run_smoke.py`、`smoke_seeded.log`、`smoke_seeded_resume.log`：固定 seed 的工程流程與記錄。
- `smoke_seeded/smoke_acceptance.json`、`smoke_seeded/smoke_resume_acceptance.json`：完成與 resume 驗收。
- `smoke_seeded/train_2020/epoch_curve.jsonl`、`checkpoint_last.pt`、`fold_01/fold_complete.json`：逐 epoch 與完成證據。
- `smoke_seeded_epoch_stop.log`、`smoke_seeded_epoch_resume.log`、`smoke_seeded_epoch_resume/smoke_resume_acceptance.json`：中斷恢復與模型一致性。
- `data_audit_cli.json`：可重跑 CLI 的完整期間缺口回執；`acceptance.json` 彙總本次證據與限制。

上述 v1 工程腳本與回執保留當時證據；當前 config 已改為 v2，重跑應使用本次的 `train_smoke.py`。
早期 `smoke_v2` 保留首次成功驗收；`smoke_epoch_resume` 保留未固定初始 seed 的無效對照紀錄，
最終重現性結論以 `smoke_seeded*` 為準。

這些驗收支持本研究模式能在共同訓練流程運作；目前沒有完整歷史訓練績效、借券可行性或實際集合競價成交的結論。
