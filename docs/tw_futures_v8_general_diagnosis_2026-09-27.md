# v8 一般期貨策略：績效診斷與修復

本次稽核對象是遠端 `artifacts/markets/tw_futures_v8_general_fast_fold10`。
雖然目錄名稱包含 fold10，實際保存 **10 個 folds**；原始目錄不覆寫。
診斷、來源雜湊、增量修補與驗證位於
`artifacts/markets/tw_futures_v8_general_fold10_diagnosis_20260927/`。

## 原本表現究竟差在哪裡

| 指標 | fold10 獨立 10M 帳戶測試 | 2017–2026 連續部署 |
|---|---:|---:|
| 策略累積報酬 | +14.66% | +2.96% |
| Sharpe | 2.586 | 0.063 |
| 最大回撤 | −3.89% | −18.03% |
| TX 近月 1 倍名目基準累積報酬 | +49.02% | +566.91% |

fold10 訓練 2015–2024、驗證 2025、測試 2026-01-02 至 2026-09-04，共 163 個測試日。
其 **連續部署** 2026 報酬是 +12.71%，和重新投入 10M 的 +14.66% 是不同帳戶路徑。
基準是 TX 近月、約 1 倍名目曝險、未扣交易費稅；原圖繼承 `2330` 名稱有誤，
不能據此認為原回測基準是台積電。修正圖名不會改變收益。

完整部署在 2017、2018、2020、2023、2025 為負；2019 為現金，2024 接近現金。
這是跨年度泛化不足的實際證據，不能拿 fold10 的正報酬掩蓋。
另一方面，fold10 年化波動約 8.18%，基準約 33.31%；其原始報酬差距包含曝險差異，
不能直接解讀為同風險條件下的選股能力差距，也不能直接放大槓桿推算可達報酬。

## 從配置到實際持倉的落差

以原 NPZ 的整數口數逐筆連接 **同日、同 slot** 的開盤價與契約乘數，
並使用每日開盤帳戶淨值還原實際名目曝險；17,755 個非零配置格與 2,810 個非零持倉格
均找到來源，缺漏為零。公式與來源 SHA 在 `fold10_allocation_gap.json`。

| 日平均 | 模型要求 | 實際持倉 |
|---|---:|---:|
| 多頭名目曝險 | 52.11% | 20.20% |
| 空頭名目曝險 | 22.06% | 6.61% |
| 總名目曝險 | 74.17% | 26.82% |
| 非零 slots | 108.93 | 17.24 |

平均曝險落差 **47.35 個百分點**，不是模型主動決定只持有 26.82% 曝險。
持倉格包含前日延續的部位；不能把持倉格數當成當日成交筆數。

- 85.04% 非零 slot 配置不夠一口；合併同標的、同到期月份的標準／小型契約後，
  仍有 81.34% 非零群組不夠一口，涉及 48.21% 的配置絕對值。
- 27.66% 配置絕對值落在前日成交量可用容量為零的契約。
- 67.63% 配置絕對值所在格沒有持倉。上述原因重疊，百分比不能相加。
- TX、MTX、TMF 在這段測試全程零口。各自一口名目約占當時 NAV 的
  72.91%、18.15%、3.65%，原模型給它們的配置均不到一口。

原模型看得到前期成交量，但沒有啟用契約面額 encoder；期貨連續特徵在進入 Linear
以前也沒有逐欄尺度正規化。先做 Linear 再對 token 做 RMSNorm，無法消除 Linear
之前不同欄位的量級差距。來源樣本中 return/spread 約 0.015–0.034，而 volume/OI log
約 3.9–5.0，差距可達百倍。這是輸入設計的可修正缺口；是否改善收益仍須實測。

## 已重現並修正的工程缺陷

| 問題 | 反例與影響 | 修正 |
|---|---|---|
| 小容量整筆放棄 | 原候選前緣最低約為目標 5/32，mini-only 僅 full/zero；想買 500 口、只能買 20 口時可全部落空 | 先把目標相對既有持倉的增減口數裁切到真實容量與方向限制，再做原有資金選擇 |
| 零口漏算成本梯度 | floor 後為零，`abs(0)` 導數為零；PnL 的 STE 有梯度，entry/exit cost 卻沒有 | 零成交邊界採用連續交易意圖的成本方向導數；forward 仍只收取實際成交費稅 |
| 精確零配置沒有方向梯度 | `sign(w) * STE(abs(w))` 在 w=0 導數消失 | signed identity STE；零報酬不產生固定買入偏向 |
| 浮點容差可選超支部位 | 選擇器容許高於資金的 epsilon，最後審核卻拒絕，差 1 元可能造成假違約 | 全程禁止超支，最後審核沿用選擇器相同 group cash 算式與加總順序 |
| STE 浮點相消 | 理論 exact 的 `shadow + (exact-shadow).detach()` 可產生微小 FP32 誤差 | `exact + (shadow-shadow.detach())`，開關梯度時 forward 逐位元一致 |
| 報表單位錯誤 | `weights_history` 保存 signed contract quantities，部分圖表當成權重 | 沿用帳本 ABI，配置圖與 daily weights 改讀 requested history 並明確標成要求曝險；未冒稱實際成交曝險 |

前四項保留大小契約合併、反向交易先消耗平倉容量、方向限制、保證金強平與整數部位限制。
沒有重新分配未成交現金到其他標的，也沒有強制滿倉。候選仍是原本 1/32 離散前緣，
並非所有整數組合的全域最優求解器。

這些反例證明程式缺陷存在，**不等於每個缺陷都是原始虧損的主因**。
原 fold10 最佳 epoch 的違約指標為零；其梯度也非零。原 folds 2、3、8 留在現金，
是後續模型未通過 validation 的現金基準，而非有證據顯示模型永久卡在零梯度。
當時 epoch0 的現金 checkpoint 是暫存替代，訓練開始前已還原移植模型。

| 選擇現金的 fold | 訓練過程最佳 validation loss | 最後 train loss |
|---|---:|---:|
| 2 | +0.014512 | −1.1814 |
| 3 | +0.004217 | −1.0085 |
| 8 | +0.041016 | −0.8076 |

目標是最小化負 log utility，現金 loss=0；三者都沒有驗證優於現金的已訓練權重。
把現金基準拿掉只會強迫選擇較差的驗證策略，並不能修復泛化。
全 10 folds、1,981 個 epochs 的記錄均無 training default，`train_zero_grad_batches`
也均為零。這不是逐參數梯度正確性的證明，但連同非零成交收益與持續變化的 loss，
足以排除「整段訓練永久停在零配置」的解釋。
詳見 `original_all_fold_training_health.json`。

以 **同一份原 fold10 requests** 重播後，這個判斷更明確：

| 執行器 | 測試報酬 | 平均實際持倉名目 gross | 違約 |
|---|---:|---:|---:|
| 原版 | 14.6632395% | 26.8153% | 0 |
| 只修容量 | 14.6673122% | 26.9809% | 0 |
| 只修資金 | 與原版相同 | 與原版相同 | 0 |
| 全部帳本修正 | 14.6673122% | 26.9809% | 0 |

容量修正只增加 **0.00407 個報酬百分點**；新增 52 個非零契約日，重平衡交易口數
6,890 → 6,964，總費稅 295,522 → 298,526 元。它修掉實際缺陷，卻不是本 fold 大幅
落後基準的主因。原重播的回報、turnover、所有整數口數及期末 NAV 與原 NPZ 逐位元
相同；9 日 equity scale 中間值相差一個 FP32 ULP、最多約 NT$1.19，已記入收據。
固定 action 的 no-grad 重播無法衡量梯度修正效益，必須重訓。
證據：`fold10_saved_requests_replay.json`、四份 `replay_*.npz`。

## 泛化與訓練設定

原 fold10 最佳 epoch=60，validation loss −0.126895、驗證報酬 +13.02%；
早停於 epoch160。train loss 由最佳時 −0.549394 繼續下降至 −0.753825，
validation 卻劣化至 +0.016732，符合訓練集改善、驗證集退步的現象。
早停與最佳 checkpoint 選擇有作用；把訓練跑滿 1000 epochs 不是目前的解法。

v5 移植了 122 個 tensor，但有 26 個期貨 encoder／embedding／attention／head tensor
重新初始化，4 個股票輸出 tensor 不使用。它保留股票背景骨幹，沒有繼承一個已學好的
期貨配置策略。模型每 epoch 是完整 chronological trajectory 的一次 optimizer step，
不應將 77 個 batch 誤認成 77 次更新。

本次對照維持原 seed42、兩張 RTX5090、BF16、global batch32、eval batch16、
學習率 1e-4、1000 epoch 上限與 100 epoch 無改善早停，沒有根據 2026 測試報酬調參。
選擇 checkpoint 仍只依 2025 validation。2026 已被檢視，後續比較屬已觀察期間的
研究性回測，不能再稱為完全未見的最終 holdout。

一般模式仍是全名目資金約束，並未轉成可加槓桿的保證金模式。成交沿用日 OPEN
研究代理、前日成交量 × 50% 的容量上限、每口每邊 40 元與既有日期稅率；未新增
逐筆 Bid/Ask 排隊或市場衝擊模型。這些是本次固定的回測假設，不是已驗證的券商成交。
日資料無法證明逐筆成交品質，本次沒有用虛構價格填補這項缺口。

## 新模型實驗的可檢查契約

`configs/markets/tw_futures_v8_general_causal.yaml` 繼承原 general config，只增加：

1. 前一期同實體合約結算價 × 來源日期乘數，加上當時可知費稅，作為一口資金估計。
   當日 OPEN、當日高低收與當日成交量不進模型；實際整口 sizing 仍由 executor
   根據當日成交價格與動態帳戶淨值完成，模型不再做一次硬性整口投影。
2. 對已 shift 的 17 個期貨連續特徵，以訓練集 `valid_indices` 和 candidate mask
   fit 逐欄 RMS；沿用共用 fit/cache/DDP broadcast 流程。驗證、測試不參與 fit。
   scale 與 active mask 保存於 checkpoint；exact resume 直接還原保存的 buffers，
   不以重新 fit 的值覆蓋。未在訓練期啟用的欄位不因未來大值而啟用。

面額 encoder 以固定 10M 參考本金換算一口比率，尚未把即時 NAV 與既有持倉作為
模型輸入；因此提供的是面額提示，不是模型已掌握所有帳戶限制的證明。
動態 NAV、容量、整口與持倉轉換仍由同一個真實帳本決定。

保持模型可選多、空、現金；不增加最低槓桿、固定 top-K 或強制交易。
新輸入／forward／surrogate 均寫入 checkpoint 指紋，不能用舊 optimizer state 靜默續訓。

另已準備獨立的一因子實驗 `configs/markets/tw_futures_v8_general_tradable.yaml`，
但 **v4 未啟動訓練，沒有績效結果**：
保留原模型與特徵尺度，只要求模型候選具備 **前日已知至少一口的容量**。
原本 27.66% 配置落在零容量標的，對既有執行規則來說當日就是不可改變部位的 action。
以 `floor(previous_volume × participation) >= 1` 過濾模型候選，讓現金選項承擔不交易的選擇。
它不在成交後搬移資金，也不強迫模型滿倉；現有持倉仍保留原帳本價格、數量、
持倉損益與清算規則，完全不能因模型遮罩而消失。
此版本不開啟上述 RMS／面額 encoder，避免把兩個實驗的效果混在一起。
完整固定來源驗證中，候選由 2,166,316 降至 1,284,133；驗證 2025 由 246,334 降至
137,099，測試 2026 由 147,823 降至 95,876。28 個其餘 sidecar 欄位逐項完全相同。
沒有任何原本有候選的日期變成全空，訓練 2,439 日、驗證 243 日、測試 163 日完全保留。
資料收據：`prior_capacity_source_preflight.json`。

股票資料 release 保持
`tw-public-20260913T030921278338896Z-l0-penguin-248d0869d7d53be4`；
期貨 parquet SHA 保持
`70a57dd76de74fd0a3652b1ecf932ccaf32320d01c252bf7b608a30b3f598df5`。
原先因官方到期結算缺漏而整份隔離的 3597 個實體契約不變；這是回溯資料品質隔離，
不能宣稱已涵蓋完整歷史可交易 universe。契約乘數沿用既有日期來源，未額外聲稱
本次完成所有公司行動乘數稽核。

## 對照結果與驗收

遠端 GPU 驗收：100 passed，涵蓋三模式、CUDA Graph/eager 帳戶與梯度、資金臨界值、
forward 有無梯度逐位元一致，以及報表來源單位。

帳本修正版完整 fold10 已完成，exit code 0，總程序 740.59 秒、160 epochs、最佳 epoch60。
原版及修正版 dataset fingerprint 都是
`87800a7be0f0608fc8ad463ccb028c4bfce1b5da1dc3a3d9dca2eea329fc0585`。

| Fold10 | 原版 | 只修帳本後重訓 v2 | 帳本＋RMS＋前期面額 v3 |
|---|---:|---:|---:|
| 最佳 validation loss | −0.126895 | −0.134214 | −0.179615 |
| 最佳 epoch／結束 epoch | 60／160 | 60／160 | 316／416 |
| 2025 validation 報酬 | 13.0165% | 13.8169% | 18.9104% |
| 2025 validation Sharpe | 1.7126 | 1.7777 | 1.6524 |
| 2026 test 報酬 | 14.6632% | 14.1124% | −7.3036% |
| 2026 test Sharpe | 2.5861 | 2.5229 | −1.1506 |
| 2026 test 最大回撤 | −3.8883% | −3.6434% | −8.7847% |
| 平均要求名目 gross | 74.1677% | 74.1495% | 80.9322% |
| 平均實際開盤持倉名目 gross | 26.8153% | 26.8728% | 33.6113% |

驗證改善、測試報酬略降、測試回撤減輕，是混合結果。工程修正有獨立正確性依據；
這組單一種子、原有非確定性 BF16 實驗不能證明獲利顯著改善。
修正版仍有 81.41% 非零群組不足新開最小一口，指數期貨仍零持倉；
帳本修正沒有消除主要配置／整口缺口，不能將其宣告成已解決的模型問題。
產物：`artifacts/markets/tw_futures_v8_general_repaired_fold10_v2/`。

原版／修正版記錄到的 epoch3 以後最慢 rank 牆鐘中位數為 **3.748 / 3.855 秒**。
原版只有 36/158 期保存此欄，修正版為 158/158，故約 2.9% 的差異不是完整配對測速。
兩者皆每 epoch 一次 optimizer update、2 個帳本 CUDA Graph，
修正版 busy eager fallback 與 graph eviction 都為零。新增正確性檢查保留了既有加速。
來源：`training_comparison_summary.json` 與各 epoch 曲線，並非整個程序都已 compile 的宣稱。

v3 已完整結束，`fold_complete.json` 為 complete、程序 exit code 0；
總程序 1800.74 秒，epoch3 以後 max-rank 中位數 **3.903 秒**，414/414 期有記錄。
它同時增加期貨逐欄 RMS 與前期面額 encoder，不能把結果歸因於其中單一項。
最佳 checkpoint 按 2025 validation loss 選在 epoch316；2026 test 轉為 −7.30%，
因此 **驗證改善並未泛化到這段測試資料**，不能宣稱模型優化已改善獲利。
比較 JSON 的 `selected_candidate=causal` 僅表示它的 validation loss 最低；
不代表測試表現最佳、正式採用或已部署。

v3 實際 gross 增至 33.61%，但模型要求亦增至 80.93%，兩者仍差 **47.32 個百分點**。
78.24% 非零群組仍不足新開最小一口，27.78% 要求配置絕對值仍落在前日零容量契約；
TX／MTX／TMF 仍完全零口。換手率從原版 0.2991 升至 0.4928，
實際平均空頭 gross 從 6.61% 升至 14.21%，平均實際 net 則從 13.59% 降至 5.18%。
這些是配置與交易行為的變化，尚不能單憑彙總數字拆出各項對虧損的因果貢獻。
配置稽核的 13,621 個非零要求格與 2,526 個非零持倉格均有同日、同 slot 來源，
缺漏為零；保存口數均為整數。

v3 遠端完成產物為 `artifacts/markets/tw_futures_v8_general_causal_fold10_v3/`。
本機 `causal_evidence/` 已取回 54 份檔案並逐檔驗證 SHA-256，
`causal_allocation_gap.json` 保存來源與帳戶曝險公式；
[三組比較圖](../artifacts/markets/tw_futures_v8_general_fold10_diagnosis_20260927/experiment_comparison.png)
及 [比較 JSON](../artifacts/markets/tw_futures_v8_general_fold10_diagnosis_20260927/experiment_comparison.json)
由 `summarize_experiments.py` 重建。所有比較採同一個 fold10 獨立測試帳戶，
未混入連續部署路徑，亦未依 test 收益自動選模。

使用者已將後續主線改為 **保證金模式**。一般模式實驗在 v3 結果封存後停止；
v4 僅完成設定、程式與資料前置檢查，原定訓練已取消，**未執行、無 checkpoint／績效**。
沒有啟動新的一般模式訓練，沒有把 v3 部署為正式策略；以下命令僅保留供稽核。

## 保留供稽核的遠端命令

原先準備的全 folds 帳本修正命令如下；本次未執行，轉向保證金模式後不自動接續。
另開目錄可防止舊 optimizer 靜默跨契約續訓：

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py --config configs/markets/tw_futures_v8_general.yaml \
  --start-fold 1 --max-folds 10 --no-resume --profile-timing \
  --output-dir artifacts/markets/tw_futures_v8_general_repaired_all_folds_v2
```

上述全十 fold 新目錄未在本次 fold10 對照中宣稱已完成。
因果輸入實驗使用 `configs/markets/tw_futures_v8_general_causal.yaml`，
本次輸出為 `artifacts/markets/tw_futures_v8_general_causal_fold10_v3/`。
同契約中斷後可以用原輸出目錄與 `--resume` 接續；已完成 fold 由既有生命週期檢查後跳過。
