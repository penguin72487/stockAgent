# Fold 14 v3 loss：實際帳戶與學習梯度的對照

稽核根目錄：`artifacts/analysis/tw_futures_margin_v3_loss_20260928`。
原始訓練：`artifacts/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_gradient_v3`。

## 原始證據

原始 run 只有 17 個完整 epoch；epoch 18 以 `SystemExit: 143` 結束，沒有完成 fold lifecycle。
不能把這份曲線稱為已收斂的 1000-epoch 結果。每輪是完整 3430 日、一次 Adam 更新，17 輪也只有 17 次更新。

- epoch 10 / 11 / 12 / 13 的 train loss 為 −0.3780 / 1.1423 / 1.0985 / −0.4222。
- epoch 11、12 都在訓練索引 1915，即 **2018-10-12**，觸發 reason 4：強制平倉未完成。
- 梯度沒有歸零，也沒有 NaN；原始 pre-clip norm 約 12.3–17.4，`train_zero_grad_batches=0`。
- 最佳驗證為 epoch 7、loss=0.57028693。現金的 log-utility loss 是 0，故這個模型在驗證窗仍輸給現金。
- 舊圖使用 symlog，會放大零附近的視覺落差；真正的尖峰仍存在，不能靠平滑、改座標或刪除違約日修正。

使用同 seed、模型、來源、global batch=32、學習率與 scheduler，canonical `train.py` 重跑 20 輪。
前 17 輪 train/validation/test loss 與原始檔逐值一致。
診斷啟動器只在 rank 0 保存出現違約的 batch 輸入；它增加同步開銷，不能拿該 control 的時間宣稱公平加速。
`control_packets/default_packet_00.pt`、`default_packet_01.pt` 保存可重播證據。

第一個違約 batch 的實際帳本：

| 日期 | 前收 NAV | 開盤 NAV | 當日結算 NAV，吸收失敗前 | 未完成強制平倉 |
|---|---:|---:|---:|---:|
| 2018-10-11 | 491,891,008 | 300,019,168 | 148,392,016 | 0 |
| 2018-10-12 | 148,392,016 | 174,570,624 | 180,978,848 | 127 口 |

前一天維持保證金需求是 160,656,000，高於 NAV 148,392,016，因此下一次開盤必須平倉。
slot 954 前持 291 口，可執行容量只有 164 口，留下 127 口。
**正的帳面 NAV 不等於通過這個嚴格執行契約。** 此契約將未完成的強制平倉設為吸收式失敗；它不是在宣稱券商帳戶的經濟淨值真的已歸零。
本次保留該既有規則，沒有改成假設無限成交量、移除容量或允許隔日繼續賭反彈。

## 已證實的 backward 缺陷

實際帳本在違約日回傳 log return **−15.94238472**，之後停留在失敗狀態。
但舊 grouped shadow 在兩個違約 batch 的同一天分別得到 **+0.20148085**、**+0.29653025**；它只檢查淨值是否為正，沒有把未完成的強制平倉納入學習邊界。
因此，forward 在懲罰這條路徑，backward 卻仍可能追逐違約後的持倉收益。

另以可手算的雙 denomination 測試，證明 grouped shadow 會借用另一個合約的容量，將不能賣的已持合約當作可平倉。
**這次 TX／MTX 的執行群組實際是 singleton，因此跨 denomination 容量混用不是這次尖峰的直接原因。** 它是替換無實體持倉的 grouped shadow 時一併修正的通用缺陷，不能混稱為實際 run 的根因。

## 修正與推導

目標保持

\[
L=-\frac{252}{T}\sum_{t=1}^T\log(E_t/E_{t-1}).
\]

實體合約的自融資關係為

\[
E_t^{open}=E_{t-1}+\sum_j q_{t-1,j}(O_{t,j}-P_{t-1,j}),
\]
\[
E_t=E_t^{open}-C_t^{entry}+\sum_j q_{t,j}(P_{t,j}-O_{t,j})
 +\sum_j q^{close}_{t,j}(P^{terminal}_{t,j}-P_{t,j})-C_t^{exit}.
\]

新 `_margin_physical_backward` 使用 canonical executor 的實際選口、平倉、存續部位、NAV 與強平狀態，在同一 batch 內對上式建立梯度：

1. 群組預算仍由原模型輸出。局部微分按實際選中的 denomination 保證金份額分配；現金狀態使用可交易的最小成本 denomination，並平均處理並列。
2. 每個實體合約使用自己的買／賣容量、價格、費稅及現金結算終點。不會用另一個合約平掉已持部位。
3. 自願委託保留明示的容量 STE，以便跨越離散／容量平台；禁止方向沒有幽靈成交梯度。強制平倉使用實際剩餘部位的敏感度，不能把未平的量微分成已消失。
4. 強平失敗時，backward 使用實際違反的維持保證金、開盤風險或部位限制餘裕；到期平倉失敗則使用未平合約的保證金量。此時不再以反彈收益作為成功訊號。
5. 整口選擇、費稅、容量、官方結算、default reason、吸收失敗和輸出報酬都保持原樣。對外回傳仍為 `exact + (shadow - shadow.detach())`，零 forward tangent 保留原帳本值。

這仍是 **有偏的局部 STE**，並非離散選口函數的精確導數，也不保證每次更新都降低實際 loss。
STE 的一般方法背景見 [Bengio、Léonard、Courville（2013）](https://arxiv.org/abs/1308.3432)；論文不是這個期貨修正或投資績效的外部驗證。
既有 0.10 的破產邊界 smooth-wealth 尺度未增加新的可調值；模型沒有新增參數，也沒有新增固定現金、曝險或槓桿目標。

`MARGIN_TRAINING_GRADIENT_CONTRACT_VERSION` 升為 4；訓練 resume 拒絕舊 Adam 狀態，舊模型推論仍可依原 forward 契約重播。
舊 grouped relaxation 保留為研究參考；正式的 exact-margin 路徑改走 physical backward。新配置仍明確使用 `futures_portfolio_training_surrogate_only: false`。
另外修正 epoch JSONL 遺漏的 `best_val_loss` 與 `improved`，讓曲線資料能正確追蹤選模；原始已保存曲線不回寫。

## 邊界

- 用 2011–2024 訓練、2025 選模；已看過的 2026 歷史測試也屬開發回饋，不能再稱完全未觸及的 holdout。
- train curve 是更新前完整軌跡，validation 是更新後模型，且年份不同；兩條線無須重合。
- 多年 log utility 不是直接最小化 MDD；本次沒有偷偷加入 Sharpe／Sortino／MDD 的加權係數。
- 正確的容量與失敗邊界本來就可能有尖峰。修正梯度不等於保證 loss 單調或消除每次違約。
- 20 輪只用來驗證工程與短期學習行為，不代表已證明策略收益或完成正式 1000-epoch 訓練。

## 完整 20 輪短測結果

每個 run 都完成 Fold 14 的完整 3430 日訓練、逐輪驗證／測試、最佳 checkpoint、最終回測與產物 lifecycle。
表中績效取 canonical `summary.json` 的 **validation-selected checkpoint**，不是事後按測試報酬挑 epoch。

| 版本 | 最佳驗證 epoch | 驗證 loss | 驗證報酬 | 驗證 Sharpe | 驗證 Sortino | 驗證 MDD | 訓練失敗輪數 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 舊 v3，batch 32 | 7 | 0.570287 | −42.30% | −0.3762 | −0.4411 | −94.01% | 2 |
| physical backward，batch 32 | 13 | 0.529379 | −39.98% | −0.3566 | −0.4132 | −93.43% | 1 |
| physical backward，batch 128＋v4 cache | 14 | 0.361111 | −29.41% | −0.2562 | −0.2977 | −92.31% | 2 |

同 batch 32 的對照支持這個 backward 修正有小幅改善驗證，但 **第 20 輪的 validation loss 仍跳到 19.1797**；只修梯度沒有消除模型在未見年份的失敗。
v5 組合的第 20 輪 validation loss 是 0.8239，較最佳 epoch 14 差；仍必須按驗證選模並保留早停。
batch 128 的訓練仍在 epoch 12、13 違約，不能宣称修正後沒有尖峰或已無風險。

2026 部分年度的 test 報酬依序為 +202.86%、+177.50%、+169.15%；test MDD 依序為 −63.34%、−61.59%、−60.84%。
**測試報酬沒有同步增加，不能宣稱全面勝過原模型。驗證 MDD 仍超過 92%，尚不是滿意的策略。**

新梯度下，batch 32 的 epoch 3–20 最大 rank wall-time 中位數為 8.920 秒；batch 128＋GPU cache 為 6.045 秒、567.38 valid dates/s，峰值約 14.66 GiB/GPU。
這是整個 v4 runtime 組合的比較，並非只改一個 batch 參數；batch 128 同時改變梯度截斷邊界，不宣稱 optimizer 軌跡相同。
舊 control 有 packet-capture instrumentation，故不納入此次公平速度比較。

原始數值、SHA-256、lifecycle 與重播結果：`artifacts/analysis/tw_futures_margin_v3_loss_20260928/summary.json`。
未平滑的線性座標曲線：`artifacts/analysis/tw_futures_margin_v3_loss_20260928/loss_comparison.png`。

驗證還包含 32 個隨機／對抗案例及 2 個真實違約 packet：在相同 CPU runtime 下，新舊所有公開 exact forward 欄位／真實 packet 報酬一致，梯度有限。
真實 packet 從 CUDA 搬到 CPU 重播時，FP32 reduction 可相差約 2.4e−7；沒有把 CPU 與 CUDA 的數值差異混稱為新舊 forward 的差異。
CUDA eager/capture 的 margin 結果與梯度亦另外比較，舊 optimizer 拒絕／舊 inference 可讀由測試驗證。
最終整合 CPU／checkpoint／loss／ledger suite：**463 passed，4 個 CUDA opt-in skipped**；另行啟用 CUDA 後相關 margin suite：**20 passed**。
較早的完整三種 futures CUDA graph／GC suite 亦有 44 passed；新增 margin 邊界後以上述最終 margin suite 和兩個 DDP 完整 lifecycle 再驗證。

## 正式訓練

新配置 `configs/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_physical_gradient_v5.yaml` 沿用 v4 的 global batch=128、FP32 GPU cache、BF16、雙 GPU DDP、每完整軌跡一次更新；batch=128 同時改變 truncated-BPTT 邊界。
保留原始資料 release、100M 資本、模型輸出、1000 epochs 上限與 100 次驗證未改善早停。
使用獨立 v5 目錄；這次只跑獨立診斷目錄，正式 v5 尚未啟動。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_physical_gradient_v5.yaml \
  --start-fold 14 --max-folds 1
```

可重播 exact forward 比較：

```bash
source scripts/runtime_env.sh
run_fintech_python artifacts/analysis/tw_futures_margin_v3_loss_20260928/verify_forward.py
run_fintech_python artifacts/analysis/tw_futures_margin_v3_loss_20260928/summarize.py
```
