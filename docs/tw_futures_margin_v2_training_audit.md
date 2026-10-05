# TX/MTX 保證金 v2：完整訓練診斷與梯度修正 v3

分析來源：`artifacts/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_fold14_v2`。
可重算證據：`artifacts/analysis/tw_futures_margin_v2_diagnosis/audit.json`，內含每個來源檔的 SHA-256、14 個 fold、逐年接續帳戶、訓練梯度與 checkpoint 選擇摘要。

## 結論與量測範圍

14 個 fold 的 canonical lifecycle、checkpoint、epoch curve、回測與圖表完整性檢查通過。
檔名中的 `fold14` 不代表只訓練第 14 折；原啟動器以 `--start-fold 1` 跑完 14 折。
完成訓練流程不等於策略有效。真正接續同一帳戶的結果嚴重虧損，而且找到會誤導學習的 margin backward 問題。

| 評估範圍 | 累積報酬 | TX 近月轉倉 benchmark | Sharpe（log） | Sortino（log） | MDD |
|---|---:|---:|---:|---:|---:|
| 2013-01-02 至 2026-09-04，同一帳戶接續 | −99.8811% | +953.5968% | −0.5458 | −0.6704 | −99.9281% |
| Fold 14，2026 年，獨立重置一億元 | +67.8412% | +54.5166% | 0.3334 | 0.4560 | −79.1012% |
| Fold 14，接續實際剩餘資金 | 0% | +54.5166% | 0 | 0 | 0% |

接續帳戶剩 **118,894 元**。從 **2019-12-13** 起沒有持倉，但模型仍持續提出非零請求。
這不是已學會全現金的證據，也不是正式違約：接續路徑的 `settlement_default` 事件數是 0。
小額權益、各群組請求預算與整口限制會共同影響可成交量；不能僅由零持倉判定整個帳戶永遠買不起任意一口。
獨立 fold 的新一億元績效不能替代這條部署路徑。

原報表的 Sharpe/Sortino 以每日 **log return** 計算。稽核另外列出每日 **simple return**、零利率/零最低可接受報酬、252 日年化版本：
接續帳戶為 −0.1234 / −0.1699，Fold 14 獨立測試為 1.5302 / 2.4344。
高波動時兩種口徑差異很大；不得混用。此次保持 canonical 指標定義，新增明確命名的稽核伴隨值。

## 從資金恆等式看問題

期貨盈虧是 `口數 × 乘數 × 價差`，不是保證金的價格報酬。
模型輸出 `w` 在本實驗代表「原始保證金預算占權益」，因此近似名目槓桿為：

`L ≈ |w| × (期貨價格 × 乘數) / 每口原始保證金`。

所以 `sum(abs(w)) <= 1` 並不表示名目槓桿小於一倍。
接續帳戶歷史最大名目槓桿 **20.46 倍**；Fold 14 獨立測試平均 **8.82 倍**、最大 **13.57 倍**。
Fold 14 最差單日約 **−47.80%**；接續路徑最差單日 **−73.80%**。
帳戶符合交易所/券商保證金限制，不會自動滿足策略的波動與回撤偏好。
期交所也明確區分保證金與契約價值，說明其槓桿效果：[期貨交易問答](https://www.taifex.com.tw/cht/9/futuresQA)。

原 `log_utility` 優化的是費稅後平均 log return；不是 Sharpe、Sortino 或 MDD 的直接目標。
設定內 `gamma_cvar`、`gamma_drawdown` 等其他目標的欄位在這個 loss 分支不生效。
單一年份驗證獲利很大、同時有深回撤，仍可能被選為最佳 checkpoint。
例如 Fold 14 驗證報酬 +533.72%，MDD 卻為 −73.96%。
這是目標與風險偏好的差距，不是 benchmark 能修復的問題；benchmark 也沒有進入此 log-utility 目標。

## 各 fold 的第一個測試年度

右側接續結果使用前一年真正留下的資金與持倉；左側獨立結果每折重置一億元。
不得把各折包含所有未來年度的完整 test tail 當作互不重疊樣本來平均。

| Fold | 年度 | 最佳 epoch / 已跑 epochs | 獨立首年報酬 | 獨立首年 MDD | 接續帳戶該年報酬 | 接續有持倉日 |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 2013 | 1 / 101 | +59.41% | −27.75% | +59.41% | 246 |
| 2 | 2014 | 97 / 197 | −92.60% | −94.59% | −87.23% | 248 |
| 3 | 2015 | 120 / 220 | −68.90% | −91.69% | −80.69% | 244 |
| 4 | 2016 | 44 / 144 | +4.36% | −74.61% | −26.09% | 244 |
| 5 | 2017 | 223 / 323 | −31.38% | −65.27% | −62.21% | 246 |
| 6 | 2018 | 1 / 101 | −88.10% | −89.58% | −82.28% | 196 |
| 7 | 2019 | 109 / 209 | +82.37% | −52.49% | −38.92% | 88 |
| 8 | 2020 | 2 / 102 | +75.39% | −86.95% | 0% | 0 |
| 9 | 2021 | 1 / 101 | +231.13% | −65.21% | 0% | 0 |
| 10 | 2022 | 29 / 129 | +54.81% | −37.39% | 0% | 0 |
| 11 | 2023 | 134 / 234 | −97.15% | −97.70% | 0% | 0 |
| 12 | 2024 | 4 / 104 | +164.97% | −73.75% | 0% | 0 |
| 13 | 2025 | 93 / 193 | +86.34% | −63.95% | 0% | 0 |
| 14 | 2026，部分年度 | 175 / 275 | +67.84% | −79.10% | 0% | 0 |

早期 fold 只有短訓練歷史，且驗證年度單一；這支持存在跨年度不穩定性，不能單凭表格將全部損失歸因於某個模型層。
Fold 14 的 275 個 epoch 每次全軌跡更新前梯度都超過 clip=1，中位 norm=126.39，最大=387.85；沒有零梯度 batch，但有 61 個 epoch 回報訓練帳戶失活。
「有梯度」不能證明梯度方向正確，更不能證明已泛化。
舊 timing 未做 CUDA 同步，不能用它判定精確瓶頸或宣稱本次加速。

## 可重現的 backward 缺陷與修正

`run_tw_futures_portfolio_integer_torch` 的整口 forward 是權威；訓練透過零 forward 值的 STE 使用 grouped shadow 梯度。
STE 是近似估計器，並非離散整口函數的精確導數；相關原始方法：[Bengio et al., 2013](https://arxiv.org/abs/1308.3432)。
本次沒有將 shadow 收益當作實際績效，也没有修改 exact executor 的選口、成本或違約公式。

1. **隔夜虧損後的新倉資金基準錯誤。**
   Exact 使用 `A = min(E_previous_close, E_open)`；舊 shadow 用前一天 NAV 的完整預算。
   修正 shadow 新請求為 `w × clamp(1 + gap_return, 0, 1)`，並同步修改增倉可用資金與 backward funding barrier。
   同一 batch 內保留隔夜部位對新倉預算的敏感度；跨 batch 仍依原契約 detach。

   手算案例：起始 1000、每口保證金 100；第一天買四口，第二天每口隔夜虧 100，開盤 NAV=600。
   新請求 50% 應配 300 元，即三口；日內每口賺 50，收盤 NAV=750。
   舊 shadow 配五口，錯算 NAV=850。修正後與 exact 同為 750；對當天輸出的局部導數為 `0.6 × 0.5 / 0.75 = 0.4`。
   隔夜盈利不提前增加可用資金；長、短兩側都有解析測試。

2. **現金結算到期日的梯度使用錯誤終點。**
   對 `CASH_SETTLEMENT` 且 `must_liquidate` 的到期日，改用官方 `TERMINAL_MARK`，不再只看普通日結算價。
   測試用 entry=1000、普通 settlement=1050、final settlement=1100、每口 margin=100，驗證 shadow 對應最終結算盈虧及導數。

3. **禁止買進/賣出的方向仍可能產生虛假日內收益梯度。**
   依 `CAN_BUY/CAN_SELL` 分別聚合容量，禁止方向不再借用另一方向的可成交量。
   長/短禁止方向、允許方向及整口 exact parity 均有測試。

4. **應強制平倉的帳戶仍向當天新請求傳遞日內收益梯度。**
   Shadow 依前日維持保證金與開盤風險比例觸發零目標，仍受可成交方向/容量約束。
   測試證明已達條件時不能靠當天政策重新開倉來學習不可取得的反彈收益。

這些修正不表示 shadow 已等於所有離散約束的精確微分。Grouped allocation、整口與容量邊界仍是既有 STE；群組持倉上限、非現金結算部分強制平倉等情境仍由 exact forward 決定結果。
本次驗證的是上述解析案例、exact forward 完全相等與 CUDA 實作一致性；沒有將近似梯度描述為無偏或完整清算模型。

## 反證：僅降低請求並不足夠

使用相同已保存政策請求、同一 canonical executor，重播有交易的 2013–2019 年：

| 請求倍率，僅診斷 | 累積報酬 | 最差單日 |
|---|---:|---:|
| 1.00 | −99.8811% | −73.80% |
| 0.50 | −81.8785% | −41.23% |
| 0.25 | −41.1903% | −22.46% |

1.00 倍重播逐日 log return 與原結果最大差異 **0.0**。
這個對照支持「原請求不只是槓桿太高」；降低曝險改善此路徑的損失，但沒有變成正報酬。
它不是重訓結果，也不是挑選最佳倍率的搜尋；新設定沒有加入固定曝險倍率。

## 新設定與執行

設定：`configs/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_gradient_v3.yaml`。
保留相同已驗證 2011–2026 TX/MTX 來源、固定 stock release、1936 action slots、2754 股票資訊上下文、整口、費稅、carry、官方結算、100M 資本、BF16、雙 GPU DDP、`score_entmax_log_cash`、log utility，以及每 epoch 一次完整軌跡更新。
1000 epochs 上限，保留 100 epochs 無改善早停，從 fold 1 開始；未新增模型參數、手動倉位上限或現金比例。

Training checkpoint 新增 `futures_margin_backward_contract_version=3`，使舊 Adam 軌跡不能在新梯度下續訓。
Forward / inference 契約保持原樣；新輸出目錄獨立於 v2。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
CUDA_VISIBLE_DEVICES=0,1 run_fintech_python train.py \
  --config configs/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_gradient_v3.yaml \
  --start-fold 1 --epochs 1000 --resume --no-retrain-completed-folds \
  --no-post-train-infer
```

`--resume` 只會讀取 v3 的新輸出目錄；第一次沒有 checkpoint 時從頭訓練。

重算既有結果稽核：

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.audit_tw_futures_margin_training \
  --artifact-root artifacts/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_fold14_v2 \
  --output artifacts/analysis/tw_futures_margin_v2_diagnosis/audit.json
```

## 驗證與尚未證明的部分

- 144 項 margin、解析梯度、funding/feasibility 測試通過。
- 341 項 adapters、sidecar、reporting、checkpoint、canonical backtest 測試通過；CUDA opt-in 項目另外執行。
- 10 項實際 CUDA margin graph / funding 檢查通過；比較 exact 輸出與 backward，不只檢查能否載入 CUDA。
- 5 項新稽核測試通過，包含錯誤報酬、偷重置帳戶、非整口與錯誤欄位契約拒絕。
- 雙 RTX 5090 strict 環境檢查通過；Fold 14、完整 2011–2024 訓練段的三 epoch DDP smoke 完成。
  每 epoch 108 個 chronological batches、一次 optimizer step，零梯度 batch=0，沒有訓練帳戶失活；梯度 norm 為 15.94 / 17.37 / 16.99。
  Checkpoint 實際記錄 backward contract=3；包含驗證、測試、整口帳戶、圖表的 lifecycle gate 通過，`progress.state=complete`。
  產物：`artifacts/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_gradient_v3_smoke3`。
  這個短訓練的驗證累積報酬仍為 **−49.61%**、MDD **−94.02%**；測試為 +202.80%、MDD −62.55%。
  相反的驗證/測試表現不能支持泛化或穩健性宣稱，更不能用三 epoch 結果取代完整重訓。

完整 1000-epoch 上限的 v3 正式訓練尚未啟動，因此不能宣稱報酬、Sharpe、Sortino 或 MDD 已改善。
後續應先重訓同一切分，再比較首年測試與接續帳戶；這些已被看過的歷史測試年度也是開發回饋，不能再當作完全未觸及的 holdout。
若修正梯度後仍偏好巨大尾部風險，才另行明確定義風險目標，使用完整軌跡一致的 loss/validation/selection；不能把批次 Sharpe 或 MDD 平均後冒充全軌跡目標。
