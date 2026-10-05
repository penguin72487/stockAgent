# Bybit v6 全折診斷與零延遲 v7 候選

檢查日期：2026-09-26。研究對象為
`artifacts/markets/bybit_perpetual_daily_0005_historical_public_pit_learned_cash_trajectory_v6`。
本次只讀舊訓練產物、準備新設定／派生資料與工程測試，**沒有啟動正式訓練**。
所有既有 v6 checkpoint、curve、來源 release 與 00:05 repair view 均保留。

## 結論與證據邊界

v6 不是「沒有更新」：六折共 1,241 個已記錄 epoch，每次均有一個完整
trajectory optimizer update，沒有記錄到 zero-gradient batch 或死亡帳戶。
問題同時包含可證明的梯度缺陷、輸出參數化限制，以及尚未由實驗解決的泛化問題。
不能承諾修正後所有 fold 都獲利，也不能把帳本變正確說成報酬必然增加。

數值來源：固定診斷路徑
`artifacts/markets/bybit_perpetual_daily_0005_v6_training_audit/performance.json`，
列出讀取來源的 SHA-256；同目錄的 `output_mechanism.json` 保存有界機制診斷。
這些是小型診斷結果，不是資料 snapshot 或訓練副本。

## 1. 先看真正可比較的折

只取每折第一個測試年，避免多次計入重疊的未來尾段。

| Fold | 訓練年 | 驗證年 | 首測試年 | 淨報酬 | 最大回撤 | 最佳 epoch |
| --- | --- | --- | --- | ---: | ---: | ---: |
| 1 | 2020 | 2021 | 2022 | −45.23% | −46.02% | 1 |
| 2 | 2020–2021 | 2022 | 2023 | +44.00% | −33.43% | 202 |
| 3 | 2020–2022 | 2023 | 2024 | −2.65% | −34.28% | 1 |
| 4 | 2020–2023 | 2024 | 2025 | −43.82% | −47.81% | 218 |
| 5 | 2020–2024 | 2025 | 2026 至 09-24 | +23.10% | −5.77% | 58 |
| 6 | 2020–2025 | 2026 | **同一 2026** | +58.07% | 不納入 OOS 比較 | 161 |

Fold 6 的驗證與測試同一區間，是已設定的最新年實驗，不是獨立 OOS。
排除該折後，首年串接為 **−46.90%，最大回撤 −59.98%**。
這是每折重設帳戶的報酬串接，並非跨模型保留數量、轉倉成本與連續 NAV
容量的部署重播。這次已檢視所有測試年；後续迭代不可再稱這些年完全未見。

舊 v4 相同口徑為 −60.24%／−77.52%。v6 整體較好，但 2025 明顯惡化；
兩者同時改過帳本、來源與正規化，不能把差異全部歸因於某一個優化。

2022 同路徑費用加回仍為 −42.07%；2025 仍為 −38.69%（原為 −43.82%）。
2025 平均執行 gross 0.820、net long 0.561。費用不能單獨解釋失敗；
方向／曝險泛化值得懷疑，但這不是已證實的因果歸因。
加回費用固定保存的持倉路徑，不是移除費用後重新跑帳戶。

## 2. 第一性原理：帳戶究竟在學什麼

線性永續的狀態是持倉數量與現金，權重只是用當期 NAV 表示的介面：

```text
E_next = E_open + Σ q_i (P_next_i − P_i)
                  − Σ q_i funding_rate_event × mark_price_event
                  − buy_fee × buy_notional − sell_fee × sell_notional
w_next_i = q_i P_next_i / E_next
order_notional_i ≤ 1% × preceding_completed_daily_turnover_i
loss = −365 × mean(log(E_next / E_open))
```

Bybit 官方說明確認線性 P&L、實際持倉在 funding 時點的費用，以及 mark-price
持倉價值計算。[P&L 說明](https://www.bybit.com/en/help-center/article/FAQ-Profit-Loss-Calculation)、
[Funding 費用](https://www.bybit.global/en/help-center/article/Funding-fee-calculation)。
本研究固定單邊 taker 0.055%，並不證明每個歷史日期／VIP 身分費率皆相同。
日資料、1% 前日成交額與 Kline open 也不等於真實市場深度、逐筆成交或完整
Bybit margin/liquidation engine。

### 已核對的帳本

- v6 使用 forward contract 5：live-NAV 容量、FP64 equity carry、真實成交費用、
  funding 與價格漂移；禁止用未來缺 label 反推今日平倉。
- 六份完整 NPZ 的累積 log return 與 summary 完全一致；FP64 NAV 與
  `exp(cumsum(saved_log_returns))` 最大差異小於 `2.78e-8`，末筆 NAV 與
  final equity 相等，final alive 皆為真。這是內部一致性，非獨立交易所對帳。
- 已有測試涵蓋 long/short、跨 chunk、fees、padding、失去估值、公告退出、
  容量限制與獨立數量／現金 oracle。本次不把嚴格缺值錯誤變成零報酬。
- HFT/VINE 8/19 公告後，8/20 的策略退出不交給模型選擇；新時鐘下為
  **8/20 00:00 提出歸零指令**，但仍受成交權限與容量約束。
  ICX 對應 9/17。指令強制不代表市場保證全部成交；殘倉仍必須有估值。

## 3. 確證修正：零委託變動被容量函數吃掉梯度

舊裁切寫成 `sign(delta) * min(abs(delta), cap)`。
當 `delta=0`、`cap>0` 且允許雙向交易時，真實函數在零點附近是 identity，
所以導數應為 1；舊 autograd 的 sign/abs 組合卻給 0。
因此即使改成可微分的 cash head，零部位仍可能被下一層殺掉梯度。

現在使用相同合法區間的 signed clamp；區間退化成單點（零容量／全禁止）
明確給零導數。單邊允許的邊界是不可微點，只選取允許側的合法次梯度。
這不是 straight-through 或假造成交；一般可微區間符合數值差分。
測例單日報酬 +5%、target=0：舊導數 0，修正後 0.05，中央差分亦為 0.05。

Forward contract 仍是 5；另新增 `CRYPTO_PERPETUAL_BACKWARD_CONTRACT_VERSION=1`，
進入 training fingerprint 與 kernel cache key。禁止續接舊 optimizer，
不把數值相同的 inference forward 誤判為同一條 optimizer 軌跡。

## 4. 輸出模式：為何全模型有梯度仍可能無法學方向

原 `learned_cash`：

```text
w_i = (1 − sigmoid(cash_logit)) × score_i / Σ abs(score_j)
```

只有一個有效幣、score 非零時，這會退化為 gate × sign(score)，
對方向 score 的局部導數為零；score 恰為零的 flat 分支也為零。
cash head 仍能學曝險，其他日期也可能給方向梯度，因此不能說整個模型完全不能學。

以 canonical dataset 計，2020 真正有效 target 為 249 日（04-27 至 12-31），
其中 **210 日只有一個候選，占 84.34%**；38 日四個候選、1 日沒有候選。
這不是全年 calendar-row 平均的分母。對早期 fold，方向梯度問題有實際覆蓋面。

六折 best checkpoint 的少量固定日期無更新 forward 亦未顯示 cash sigmoid
飽和（觀察導數約 0.120–0.250）；它只排除這些樣本的 gate 飽和，
不能宣稱完整日期／所有特徵都已做 explainability。

v7 沿用既有 **`score_entmax_cash_v2`**，不另造模型或訓練迴圈：

```text
p = entmax15(abs(scores), causal_mask)
w_i = p_i × score_i / (1 + abs(score_i))
cash = 1 − Σ abs(w_i)
```

所有零分數精確為 cash；單一 score=0.1 時，`w=0.090909`、
`dw/dscore=0.826446`；score=0 時導數為 1。
保留 FP32 權重、L1≤1、模型決定多空／現金，沒有人工 top-K、多空比例或固定風險。
此模式**不再有獨立 cash gate**，分數同時影響選擇與 conviction；這是明確取捨。
Entmax 的零支持區仍可能有零梯度，故它是有針對性的研究候選，並非萬用解法。
舊 learned_cash 實作與所有歷史設定保持原樣。

## 5. 訓練、梯度更新與選模逐項核對

| 層次 | 本次結論／處置 |
| --- | --- |
| 訓練目標 | 使用 canonical 扣費／funding 後的 mean log utility、365 年化；無測試年選模 |
| 更新頻率 | 每 epoch 固定參數跑完整 chronological trajectory，一次 clip／AdamW step |
| Chunk 權重 | 各 chunk 按有效日期占全 trajectory 比例累積，不平均大小不同的 batch |
| Recurrent state | weights、alive、FP64 NAV 保留 forward；128 日 global-batch 邊界 detach，並非完整跨年 BPTT |
| DDP | gather 全帳戶 action、共用同一 loss；以 focused coordination／parity tests 查驗 |
| 數值安全 | BF16 model compute，金融敏感累積保留適當精度；資料錯誤協調停止、不提交部分更新 |
| 梯度裁切 | v6 所有已記錄單次更新 norm>1、都有裁切；裁切本身不是錯誤，不直接加大 LR 或 cap |
| 特徵正規化 | 保留 fold-train-only column RMS、train-unseen mask、resume 恢復 buffers |
| 公共資料 | 保留 00:00 截止的歷史 PIT 來源與 availability，沒有擴張 snapshot-only 特徵 |
| 近端配置器 | 仍使用原成本 no-trade 區，費用另在 canonical 帳本實扣；不是「多扣一次 fee」 |
| 過擬合 | Fold 1、3 在 epoch 1 最佳，之後 train 改善而 val 惡化；更長訓練不是已知解法 |
| 選模／停止 | 保留 validation-only 最小 loss、1000 上限／100 patience，不用 test 改 checkpoint |
| 年際轉換 | 各 fold 重設帳戶；未宣稱跨模型 inventory transfer 已實作 |

設定檔中繼承的 `gamma_cvar`／`gamma_drawdown` 不會在選定的 log-utility
分支啟用 CVaR／drawdown 正則；不能看到參數存在就說有做尾風險訓練。
同理，TW 的 32 日 settlement gradient horizon 不等於這個 crypto 路徑的 128 日
batch detach。真正雙 rank Gloo 小型測試對照單程序 oracle，涵蓋兩種 metadata
路徑、尾端 padding、不同 global batch 與 clip 強度，更新值與 loss 一致。

`grad_clip_norm=1` 作用於全模型累積梯度；AdamW 的更新還受動量與二階矩影響，
不能只把裁切比例當成有效 learning rate。
[PyTorch AdamW 定義](https://docs.pytorch.org/docs/main/generated/torch.optim.AdamW.html)。
新增的 audit telemetry 分開「未量測」、「真正零值」、「單次更新裁切」和
多更新 epoch 平均，不把未知值預設成零。

輸入尺度舊報告的一個分母更正：v6 正式 fitter 的 crypto feature lag 是 0，
因 daily row 本身已代表當天 00:00 前的完成資料。2020 receipt 為 280 個
train-visible dates、510 alive cells、126 active features。舊手算使用 lag 1 的
279／505／124 統計不能冒充正式 fitter 的精確重現；量綱不均衡的方向結論仍成立。

另外，固定日期樣本顯示訓練 RMS 並不能消除未來尺度漂移：fold 2 的 2023-07
SOFR normalized RMS 約 100.97；fold 6 的稀有 0/1 availability 經 RMS 可被放大
約 48 倍。這是待驗證的輸入穩健性風險，不是允許用測試年重新 fit scales。
本輪保留既有因果 fitter；如要比較壓縮或 binary-channel 單位規則，須另立候選，
而非在已檢視的測試年上找最有利轉換。72 個時間基底在 32 日去均值視窗的
實際 rank 為 31，符合該視窗上限，不能只憑基底數大於 rank 判定程式有錯。

本次不憑一次歷史觀察同時修改 LR、dropout、模型尺寸、訓練年權重與 patience。
先處理確證梯度／輸出問題及使用者指定時鐘；若下一輪仍 train-val 分離，
再用 train/validation 的固定方案比較模型容量／regularization。
不可為了改善早期報表刪掉 2020、移除虧損幣或按測試結果新增 mask。

## 6. 使用者指定的新時鐘：00:00、零延遲

新設定是 00:00 UTC 完成決策、零延遲訊號／00:00 Kline open 成交代理。
這是**研究假設**，不是已證明資料傳遞、計算及下單可在零時間完成。
模型只讀先前完成 UTC 日 OHLCV 與截止時刻可用的公共資訊，不讀當日 high/low/close/volume。

不能把 00:05 日表改名：必須由同一最新 release 的 1m/funding 重建 execution price、
前日容量的價格換算、funding `(start,end]` 所有權與 total-return labels。
邊界 funding 視為先歸前一持倉結算，再產生新 target；新開倉不賺／付已結算事件。
00:00 的 quote availability 在此明示瞬時假設下可用；舊 00:05 builder 在
00:00 決策前使用 00:05 quote availability 的因果限制，不宣稱已回溯修好。

儲存仍只有一份 canonical raw/public source。新的固定派生位置：
`artifacts/cache/bybit_perpetual_daily_0000_repaired/perpetual_daily`。
這些價格／label 與 00:05 不同，不能把兩者當成 byte-identical 重複檔案刪除；
不建立時間戳 snapshot、不發布 cold release、不改 immutable source。

實際準備結果：397 份日表、410,449 列、65,213,172 bytes（約 62.2 MiB）；
來源日期 2020-03-26–2026-09-26。收據核對 1,588 份來源及 397 份輸出 SHA，
重跑只驗證、不重建。配置從 2020-03-27 開始；有效完整交易標籤仍只到
2026-09-24，未完成的尾端日期不可當零報酬測試列。397 檔沒有壞 execution price、
錯 clock/version 或重複日期；有效 `adjclose` ratio 與 funding-adjusted simple
return 的最大誤差 `6.66e-16`。這是建置一致性，不是逐筆實盤成交驗證。
同一固定 cache 根的約 505 MiB panel 是可重建訓練快取，不是第二份原始來源。

既有公共特徵表維持其已記錄的 00:00 資訊截止。
Bybit funding cashflow **資訊特徵**的前一日 00:05 價格分母仍屬已知歷史，
不拿它當新的 00:00 帳本 funding label；ICX 修復尾段的公共欄位缺失仍保留
availability，沒有假裝全部補成已觀察值。

## 7. 執行與驗收

新的固定輸出根：
`artifacts/markets/bybit_perpetual_daily_0000_historical_public_pit_score_cash_trajectory_v7`。
v6 optimizer 不相容，不能搬 checkpoint_last 到新根。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/prepare_bybit_midnight.py
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/markets/bybit_perpetual_daily_0000_historical_public_pit_score_cash_trajectory_v7.yaml
```

正式訓練由使用者執行。本次工程測試、資料準備通過不代表 v7 已完成任何 fold。
之後按各折第一測試年檢查淨報酬、最大回撤、gross/net、換手、最佳 epoch、
實際更新次數與缺值／default，不以含驗證重用的總曲線判定成功。

本次已執行 strict CUDA 環境檢查；關聯 CPU／雙程序 Gloo 回歸 **654 passed、
3 個 opt-in CUDA case skipped**，三項 CUDA compiled/eager 帳本／梯度測試
另行啟用後 **3 passed**。實際 v7 全尺寸模型的微型無更新 forward/backward
輸出有限、gross≤1，並未執行 optimizer。正式入口 `train.py --check-data-only` 通過：
2,375 panel dates、397 symbols、133 features、6 folds；沒有建立模型、optimizer、
checkpoint 或 fold-completion marker。這個 gate 證明來源／panel 可用，不能證明
任意新策略持倉在 HFT/VINE 的缺估值尾段一定能依容量完全退出。
完整 repo 測試並未宣稱全綠：既有 exchange-scope 的 OKX/Binance 兩個 case
引用本地缺失的 `okx_1m_venue_only_v1.yaml`／`binance_1m_venue_only_v1.yaml`，
它們與這次 Bybit 修改分開列示，未建立假設定掩蓋。

診斷工具可重用於新舊 root：

```bash
run_fintech_python scripts/audit_crypto_training.py \
  --root artifacts/markets/bybit_perpetual_daily_0005_historical_public_pit_learned_cash_trajectory_v6 \
  --output artifacts/markets/bybit_perpetual_daily_0005_v6_training_audit/performance.json
```
