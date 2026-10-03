# v16 的市場相關性與從 Fold 1 訓練

本次修正 Fold 1 的初始化設定，並檢查已完成 v16 的每日報酬與真實整口持倉。
沒有修改既有 v16 產物、帳本、費稅、梯度或 loss。新設定 v17 是 v16 的
逐 fold 從頭訓練版本；沒有宣稱它已取得低相關性或更高報酬。

## Fold 1 啟動錯誤

使用者提供的例外為：

```text
pretrained initialization requires exactly one source checkpoint
fold_policy='matching_train_and_validation_years'
train=[2011] val=[2012] matches=0
```

v16 指向 v10 預訓練根目錄，但該根只有 Fold 14，即訓練 2011–2024、
驗證 2025。Fold 1 需要訓練 2011、驗證 2012 的來源，所以匹配失敗。
這是正確的未來資料防護；不能用 Fold 14 模型初始化 Fold 1，也不能只改
年份標籤或關閉匹配檢查。`--no-resume` 只關閉 optimizer/checkpoint 續訓，
不會關閉另行配置的 pretrained initialization。

新設定：
`configs/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_scratch_all_folds_v17.yaml`。

- `pretrained_initialization_root: null`，各 fold 各自初始化。
- `warm_start_from_previous_fold: false`。
- `futures_flat_action_initialization: false`，保留正常非零初始化。
- 初始本金與容量參考資金均為 1 億元。
- 保留 v16 模型、保證金面額觀測、可行梯度、計算優化、LR 1e-5 與 plateau scheduler。
- 保留跨日持倉、自由多空、log-utility；沒有加入中性化、固定配比或候選否決。
- 每 fold 最多 1,000 epochs，每輪驗證，100 次未改善早停；改善門檻 0.0001。
- `start_fold: 1`，新產物目錄，完整命令不加 `--max-folds 1`。

## 每日報酬量測

共同日期為 2026-01-02～2026-09-04，共 163 日。以各策略保存的 net log
return 經 `expm1` 還原每日簡單報酬，全部與同一條大台近月轉倉 benchmark
按日期對齊；不是算兩條累積淨值的相關性。Benchmark 為一倍名目曝險、
未扣費的同合約收盤報酬，已逐值核對 `futures_benchmark_audit.npz`。

| 已完成策略 | 與大台每日相關係數 | 單因子 beta | 期間累積報酬 |
|---|---:|---:|---:|
| v10，Fold 14 | 0.95764 | 0.77973 | 55.6553% |
| v16，Fold 14 | 0.94073 | 0.93561 | 64.1990% |
| 股票當沖 v5，Fold 10 的相同日期片段 | −0.11210 | −0.03497 | 42.8946% |
| 大台近月轉倉 benchmark | 1 | 1 | 54.5166% |

股票對照使用
`artifacts/markets/tw_day_trade_v8_combined_annual_log_cash_sub_lot_first_session_all_folds_v5/fold_10/test_backtest.npz`。
它與期貨的資金、訓練期間及交易契約不同，這裡比較報酬依賴性，不能據此
歸因某一項模型改動。2026 已反覆用於研究，並非未曾查看的測試集。

v16 的描述性迴歸為 `r_strategy = intercept + beta * r_TX + residual`，
使用每日簡單報酬、有截距、零無風險利率。R² = **88.50%**，指本期間日報酬
變異的線性解釋比例，不是「88.50% 的獲利來自大盤」。當 benchmark 下跌時，
70 日中只有 4 日策略獲利；其平均日報酬 −1.4844%，benchmark 為 −1.4834%。

## 為什麼與股票當沖不同

1. **可下單的商品不同。** 本次實際引用的
   `verified_scope_2011_20260928/build_summary.json` 明列商品只有 TX、MTX 月契約。
   原因是歷史保證金與同一實體合約前結算價的完整驗證範圍，不是按績效篩選。
   兩者追蹤同一台股指數。1,936 個固定輸出欄位是動態實體契約的槽位，
   不是 1,936 個獨立市場。股票背景的 `symbol_names` 也不是這些期貨動作的商品名稱；
   本次按逐日 `portfolio_slot` 重新連接原始實體契約，確認所有持倉均為 TX／MTX。
2. **實際學到的方向仍是多頭。** 163 日均持有大台及小台多單，空單日數為零；
   平均名目 gross 為開盤 NAV 的 1.04748 倍。設定允許做空，這是已學策略的行為，
   不是執行器禁止空單。
3. **主要利潤來自隔夜。** 依帳戶金額，淨利約 64,198,976 元，隔夜損益
   54,853,000 元，占淨利 85.44%；開盤後損益及其餘成本淨額約 9,345,976 元。
   此為相同帳戶的金額分解，不能把後者當成另一個獨立當沖帳戶的回測績效。
4. **目標沒有要求低相關性。** 扣費後 log-utility 會獎勵能成長的合法策略，
   包括承擔獲利的市場方向曝險。Sharpe 較高、回撤較小，也不等於與市場無關。
5. **初始化帶有既有方向。** v10 已有 0.95764 的相關性；v16 是從它出發微調。
   這使保留既有策略成為合理假說，但沒有同條件、完整從頭訓練對照，不能把全部
   相關性因果歸於初始化。v17 只移除這個起點，不保證消除市場曝險。

## 從目標推導下一步

現有固定日期集上的目標是：

```text
L(theta) = -252 / T * sum(log(1 + r_strategy(theta, t)))
```

若只改成減去 benchmark 的 log return：

```text
L_relative(theta) = L(theta) + 252 / T * sum(log(1 + r_benchmark(t)))
dL_relative / dtheta = dL / dtheta
```

因此這種修改不會產生新的去相關學習訊號。也不應用全測試期間估出的 beta
事後扣除大盤，然後把那條 residual 曲線宣稱成可交易收益。

符合「模型在合法環境自由學習」的研究次序是：先確認實際可交易集合是否符合
需求，再以逐 fold 的因果資料檢查模型是否能預測獨立收益。股票當沖有跨股票
選擇與日內訊號；TX／MTX 主要需要選時或跨月份相對價差訊號。擴大可交易商品
前須補齊各商品歷史保證金、費稅、到期／契約轉換及容量資料，不能把今日規則
回填歷史。大量股票輸入特徵本身不會創造股票下單能力。

改成每天平倉會移除隔夜持倉，但也會改變環境與交易成本，且日內行情仍可能與
大盤同向；不是保證低相關性的方法。若「市場中性」本身是必須達到的限制，
便需要明確改變訓練目標或動作空間，這與保留所有策略自由度是不同要求。
本次尚未收到改為日內平倉的選擇，所以 v17 保留原有跨日契約。

低相關性與穩定高報酬是兩個不同目標。全現金的相關係數未定義，純噪音也可能
低相關，都不能當成獨立獲利能力的證據。正式評估應同時報每日相關性、beta、
上漲／下跌期間報酬、收益／回撤，以及跨 fold 的穩定性。

理論參考：William Sharpe 的 [The Sharpe Ratio](https://web.stanford.edu/~wfsharpe/art/sr/sr.htm)
指出 Sharpe 本身未納入與其他資產的相關性；Bailey 等人的
[The Probability of Backtest Overfitting](https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf)
討論反覆依歷史回測選擇策略的過擬合問題。

## 執行與驗證

從 Fold 1 開始依序跑全部 14 folds：

```bash
source scripts/runtime_env.sh
run_fintech_python train.py \
  --config configs/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_scratch_all_folds_v17.yaml \
  --start-fold 1 \
  --cpu-threads 16 \
  --torch-compile-threads 16
```

預設 `resume: true`：新根目錄沒有模型，首次會各 fold 從頭訓練；中斷後用同一
命令可續訓相容 checkpoint。不能指回舊 v16 根。若只要 Fold 1，另加
`--max-folds 1`。Fold 1 訓練 2011、驗證 2012，完整測試為 2013–2026；
累積 deployment 報表依後續 folds 的擁有期間串接，不把重疊完整測試直接相乘。

產物根：
`artifacts/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_scratch_all_folds_v17`。

本次只執行獨立根的三輪 Fold 1 檢查，尚未啟動上述正式全 folds 訓練。
雙 RTX 5090、BF16、完整 train/validation/test/plot 生命週期完成；train loss
3.48941 → 3.17497 → 2.90466，梯度有限且非零，訓練帳戶存活。
驗證 loss −0.12635 → −0.13004 → −0.12531，選第 2 輪。這證明能正常訓練，
不證明收斂或良好績效。smoke 的 `--epochs 3` 使比例早停 patience 為 1；
正式設定仍是 1,000 × 0.1 = 100 次驗證，不能拿 smoke root 續跑正式實驗。

strict CUDA 無警告／失敗；新增報酬單位、日期對齊、現金相關係數與期貨槽位
測試，以及既有初始化／面額觀測測試，共 **53 passed**。

完整證據在 `artifacts/analysis/tw_futures_v16_benchmark_dependence_20260928/`：
`benchmark_dependence.json` 保存統計與 SHA-256；`v17_fold1_smoke_audit.json`
保存正式產物結構與帳戶核對；`tests.log` 與 `v17_fold1_smoke.log` 保存執行紀錄。
可用 `run_fintech_python -m scripts.analyze_tw_futures_benchmark_dependence --help`
查看重跑介面。
