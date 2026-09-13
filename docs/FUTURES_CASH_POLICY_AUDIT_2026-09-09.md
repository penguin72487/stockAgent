# 最佳模型為何空手：第一性原理診斷與 v4 修正

2026-09-09，`/root/stockAgent`，雙 RTX 5090、BF16、同一 FinancialTransformer。

**v4 只證明第一 fold 的局部學習改善；跨 fold 空手與測試清倉失敗仍未解決。**
後續實際 v4 訓練已重現同一筆測試失敗，第二 fold 最新紀錄也多數零損失。
先前將這項局部改善稱為「已修正空手學習停滯」過度概括，予以更正。
最新逐筆重算與執行狀態見 [v4 後續稽核](FUTURES_V4_FOLLOWUP_2026-09-09.md)。

以下保留原本 128 輪有界對照的結果：
只啟用專案現成的訓練期逐特徵 RMS 正規化，128 輪第一 fold 的最佳模型便從
完全空手變成訓練 99 口、驗證 7 口，兩段均無殘倉。驗證累積淨報酬為
**+0.01885%**。但測試第一筆交易於 2022-03-02 無法依原分鐘容量清倉，
因此測試契約失敗。不能用「終於交易」代替策略品質驗收。

新設定：[gradient_v4.yaml](../configs/markets/tw_stock_futures_day_trade_0845_gradient_v4.yaml)。
完整量測：[results.json](../artifacts/smoke/futures_cash_audit_20260909/results.json)、
[v3 動作](../artifacts/smoke/futures_cash_audit_20260909/diagnosis.json)、
[v4 動作與整口回放](../artifacts/smoke/futures_cash_audit_20260909/v4_diagnosis.json)。

## 1. 先釐清「最佳」與「空手」

v3 的 Adam 已更新 128 次，各訓練參數及動量均有改變；不是忘記 `optimizer.step()`，
也不是 BF16 GradScaler 跳過所有更新。v3 第 2–128 輪精確訓練損失皆為 0，
128 輪驗證損失也都是 0。最佳 checkpoint 留在第 1 輪，是因選模條件要求
驗證損失嚴格改善。當所有候選模型都空手時，保留早期同分模型是合理行為。
改為同分時選最後一輪，仍只會得到另一個空手模型。

必須分開檢查整條鏈：

```text
原始特徵 → 多基底與股票表示 → 分數 z → 資金權重 w
         → 整口組合 B(|w|E) → 分鐘進出場 → 實際淨損益／殘倉
```

`||∂L/∂θ|| > 0` 只代表存在更新方向，並不代表輸出有跨越整口門檻。
精確整數函數幾乎處處的普通導數為 0，v3 使用的是已明確標示的代理梯度。
STE 原始研究亦將這類估計與真實梯度區分，不能以非零作為充分證明。
[Bengio 等，2013](https://arxiv.org/abs/1308.3432)。

## 2. 資金門檻證據：有分數，卻買不起一口

L1 球內的現行輸出為 `w_i = z_i / A`，A 是當日因果候選股票數。
本 fold 的 A 中位數為 189；此縮放保持現金選項，也沒有把總曝險強制推到 1。
本次測得所有日期都在球內，故不存在 L1 稀疏投影截斷大部分梯度的解釋。

令 E 為實際帳戶權益，C_i 為至少一口所需的現有研究預留資金：

```text
N = entry_price × multiplier
C = N + 2 × (commission + floor(N × tax_rate + 0.5))
至少一口要求：|w_i| × E ≥ C_i
球內換成分數單位：|z_i| ≥ A × C_i / E
```

這裡 C 是既有全名目金額預算，不是宣稱交易所保證金等於全額。
本次沒有套用不適用於個股期貨的當沖保證金減半。官方規則及先前梯度推導見
[v3 規則報告](FUTURES_GRADIENT_RULES_2026-09-09.md)。

2020 年 164 個訓練日，共 3,775 個可進至少一口的股票日。以固定 TWD 10M
只量測門檻幾何，結果如下；這不是跳過 recurrent equity 的策略回測。

| v3 量測 | 最佳，第 1 輪 | 最後，第 128 輪 |
|---|---:|---:|
| 日總請求曝險中位數 | 7.647% | 1.563% |
| 候選股票絕對分數中位數 | 0.06543 | 0.009277 |
| 可進場股票的預算／一口門檻，中位數 | 1.518% | **0.215%** |
| 相同比值最大值 | 87.69% | 74.61% |
| 跨過一口門檻的股票日 | 0 | 0 |

一口所需資金中位數約 TWD 178,088。代入 A=189、E=10M，代表性分數門檻約 3.37。
這是各自中位數的示意計算；完整逐股票日比值已另存，沒有用中位數乘除替代分布。
模型後期分數越學越小，並不是只是還差一點點就能成交。

## 3. 可重現根因：欄位尺度不平衡，加上強烈縮倉梯度

原設定有逐列 RMSNorm，但未啟用逐特徵正規化。兩者作用不同：

```text
逐列 RMSNorm： y_j = x_j / sqrt(mean_k(x_k²) + ε)
因此 y_j / y_k = x_j / x_k（忽略可學 gain），欄位間的單位差異仍在。

訓練期逐欄 RMS： s_j = sqrt(mean_training_alive_cells(x_j²))
                x'_j = x_j / s_j
```

若單獨把某欄位改成 a_j 倍單位，同時 s'_j=a_j s_j，則正規化輸入保持相同。
這是本次補入的完整模型「輸出與參數梯度單位不變性」回歸測試。
逐列 RMSNorm 的公式與作用依官方文件；逐欄處理則直接重用本專案既有實作。
[PyTorch 2.11 RMSNorm](https://docs.pytorch.org/docs/2.11/generated/torch.nn.RMSNorm.html)、
[RMSNorm 原論文](https://arxiv.org/abs/1910.07467)。

僅使用 2020 年訓練可見列量測：

- MOf／CBC／DGBAS／匯率／TAIEX 等總體欄位占原始平方能量 **82.687%**。
- `twpub_mof_export_log` 的 RMS 為 20.55；`close_logret_1d` 約 0.02535，約差 811 倍。
- 這是進入模型前的尺度證據，不是「82.7% 的決策由總經控制」的歸因結論。
  [完整 98 欄位量測](../artifacts/smoke/futures_cash_audit_20260909/feature_scales.json)。
- v3 最後一輪有 571 個股票日的第一口在兩個方向都不能按期清倉，占可進場股票日
  約 15.1%；這些列提供約 **89.7%** 的動作梯度絕對量。
- 促使既有動作縮小的梯度絕對量為 3.606，增加幅度的為 0.272，約 13.3 倍。
  這是動作空間的梯度分解，不能直接當作參數空間 Jacobian 的貢獻百分比。

因此「共享表示難以區分股票，強約束又持續壓低動作」是具體假說。
後續只改正規化即恢復實際成交，支持輸入條件不良是這次停滯的一項實質原因，
並不排除其他表示、樣本數或代理目標的限制。沒有削弱殘倉懲罰來製造結果。

## 4. 跟其他訓練差在哪，重用了什麼

| 方法 | 實際差異 | 本次處理 |
|---|---|---|
| v3 個股期貨分鐘 | 98 欄位直接共同做 row RMSNorm；標準／小型須整口，嚴格退出 | 保留帳務與梯度，修正欄位尺度 |
| 專案較新的股票一分鐘訓練 | 已有 train-only feature RMS、checkpoint scales/mask；另有執行上下文 | 直接重用 normalizer，不搬入現股 T+2 或轉融資語意 |
| 全期貨 `CrossSectionalAllFuturesModel` | 合約 token 含面額／級距等因果資訊，另有 action head | 確認這是表示差異；本次未複製另一套模型或改為留倉 |
| 連續權重／可細分數量問題 | 小額權重能直接產生連續損益，較少整口門檻問題 | 不用分數合約代替實際成交 |
| 可微凸最佳化層 | 有條件時可對連續凸解求導 | 本案整口組合與成交容量不是該類光滑凸問題，未引入新求解器 |

可微凸最佳化論文限定了其可微問題形式，並不是「把整數執行器換成凸層就解決」。
[Agrawal 等，2019](https://www.web.stanford.edu/~boyd/papers/diff_cvxpy.html)。
直接交易績效訓練也須明確計入成本及狀態，而非只學價格方向。
[Moody 等，1998](https://doi.org/10.1002/%28SICI%291099-131X%281998090%2917%3A5/6%3C441%3A%3AAID-FOR707%3E3.0.CO%3B2-%23)。

本專案程式對照：`stockagent/models/financial_transformer.py` 的 CandleEncoder；
`stockagent/training/trainer.py::_fit_group_causal_feature_rms`；
`configs/markets/tw_day_trade_1m_strict_exact_2020.yaml`；
`stockagent/models/cross_sectional_all_futures.py`。
這是程式／方法比較，不是宣稱不同產品的績效可直接互相比高低。

## 5. v4 實作與驗證

只開啟 `training.financial_transformer.causal_feature_rms_normalization: true`。
其餘 model、training、data、trading 與 v3 相同；config loader 亦會同步派生
未選用 executable-model 的預設值。沒有新增 trainable 參數。

每個訓練 fold 分別擬合，保留 98 欄位 ABI。首 fold 擬合日期為
2020-03-23 至 2020-12-30，包含 374,490 個 alive 股票日；最後訓練目標日的
未完成特徵、驗證與測試列皆未參與。69 欄訓練已觀測，29 欄訓練未啟用者
在該 fold 維持零值，避免未訓練的投影欄位日後因資料源啟用而突然生效。
這是訓練期資料可用性處理，沒有事後選股或特徵排名截斷。

normalizer scale、active mask 存入 checkpoint buffers；驗證與推論使用相同值。
model fingerprint 改變，v3 checkpoint 載入 v4 被實際相容性檢查拒絕。
trading fingerprint 與整數執行器 SHA 保持相同：

```text
ab1780efc14cff974fea9c41b76126e3cddc06944108ad1d859ec30227123f69
```

完整對照：2020 訓練 164 日、2021 驗證 244 日、2022–2026 測試；
2,753 股票、同來源指紋、seed 42、626,132 trainable parameters、BF16／DDP，
每完整日期軌跡一次 Adam 更新。兩組各 128 輪，沒有早停；v4 正式 YAML 仍為 1000。

| 指標 | v3 | v4，只改 feature RMS |
|---|---:|---:|
| 最佳 epoch | 1 | 122 |
| 精確訓練帳戶有效輪數 | 127/128 | 128/128 |
| 非零精確訓練 loss 輪數 | 1（失敗輪） | 112 |
| 最佳驗證 annualized log loss | 0 | −0.000194594 |
| 最佳 checkpoint 訓練回放 | 0 口，0% | 99 口，+1.11774% |
| 最佳 checkpoint 驗證回放 | 0 口，0% | 7 口，+0.018847% |
| 訓練／驗證殘倉 | 0／0 | 0／0 |
| 正式測試回放 | 空手 | 1 口進場，1 口殘倉，失敗 |

checkpoint 訓練回放是固定參數重跑完整 recurrent equity，與該 epoch 更新前的
training loss 不同；固定 TWD 10M 門檻計數也不能代替 recurrent 成交。
v4 最佳驗證回放的 annualized loss 與正式選模結果相符，差小於 2e−11。
最後一輪的驗證帳戶反而失敗，這也驗證了必須保留最佳驗證 checkpoint，不能用最新檔取代。

![同來源同 epoch 的 v3/v4 對照](../artifacts/smoke/futures_cash_audit_20260909/comparison.png)

v3/v4 穩態 training 中位數分別約 0.687／0.716 秒，完整 epoch 約 1.019／1.066 秒。
這次是學習條件修正，並非加速宣稱；控制組沿用先前同機已完成結果，執行時段不同。
兩組皆完成 fold lifecycle、測試、checkpoint、9 張規定 root 圖表。

## 6. 測試失敗為何不能靠改報酬或偷看未來修復

失敗交易：**2022-03-02，股票 2888，DDF:202203，標準契約，多 1 口**。
請求權重 0.00245343，08:46 bar 均價 10.763636、成交量 11，足以進場。

| 事件時間 | 觀測成交量 | 在現行退出契約中的意義 |
|---|---:|---|
| 13:20 | 無該事件棒 | 沒有現行規則所需的限價錨點 |
| 13:23、13:24 | 11、4 | 沒有有效 13:20 限價，不會憑空產生該限價成交 |
| 13:28、13:29 | 各 1 | `floor(0.5 × 1)=0`，每分鐘可用容量為 0 |
| 13:30 | 無該事件棒 | 仍有 1 口殘留，觸發嚴格失敗 |

該合約前日成交量 3,592、當日總成交量 2,043；高日量也不保證指定幾分鐘有對手。
完整原始事件列、實體合約與 source SHA 已保存在 `results.json`。
此處沒有把無事件棒自動當成缺檔，也沒有將這個測試交易加入新的 quarantine。

近 −100% 是 `log(1e−7)` 的既有不可行帳戶標記；**不是這一口實際虧掉 TWD 10M**。
保留使用者要求的 13:30 清倉，就必須保留這個失敗證據。跨分鐘累積容量、
提早市價出場、前值限價錨點或放寬參與率，都會改變本次已固定的執行語意，
不能為了消掉測試失敗而偷偷套用。

工程層面已定位並改善空手停滯；投資層面只見訓練改善及極小驗證收益，
另有明確測試失敗。後續若研究因果期貨流動性表示，必須重新設計訓練／驗證比較，
並將已檢視的 2022 測試案例標為研究暴露，不能再稱完全未接觸的測試選模依據。

## 7. 可重現指令與驗收範圍

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/markets/tw_stock_futures_day_trade_0845_gradient_v4.yaml --check-data-only

# 新實驗目錄；正式設定保留 1000 epochs。
run_fintech_python train.py \
  --config configs/markets/tw_stock_futures_day_trade_0845_gradient_v4.yaml
```

本次實際只完成第一 fold 的 128 輪研究對照，未啟動全部 folds × 1000。
探測用 `rms_only.yaml` 與正式 v4 的有效 model/training/data/trading 已逐欄比對相同；
探測 CLI 只覆寫 epochs、fold 範圍、早停及 output root。

驗證包含 strict CUDA、完整資料 preflight、158 個模型／checkpoint／分鐘梯度／
完整軌跡更新測試、跨特徵單位變更下輸出和參數梯度一致、既有 train-only fitting
測試、實際 checkpoint 跨版本拒絕、整數帳本源碼 SHA 不變，以及回放口數／殘倉。

逐特徵量測及動作診斷工具保存在
`artifacts/smoke/futures_cash_audit_20260909/{feature_scales,diagnose,collect_results}.py`。
既有已租用股票 materialization 本次通過 114,841 個檔案的完整雜湊驗證；
這只證明本機訓練來源有效，不宣稱 edge cold payload 已完整或其他機器已可用。

v2/v3 舊資料、模型、實驗結果及私人分鐘來源皆保留。沒有下載 ticks、改寫資料
收據、增加事後候選過濾、改成分數合約、強制曝險或續用舊 optimizer。
