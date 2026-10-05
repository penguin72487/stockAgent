# Fold 14 保證金期貨：loss、梯度、交易與結算診斷

本次先核對實際執行版本、原始帳本與公告，再完成修正及工程驗證。**沒有啟動正式訓練，沒有宣稱新版績效已改善。** 原始模型、曲線、資料與程式發布版本均保留。

## 1. 實際分析範圍與績效

使用者指令執行的是 `artifacts/runtime/tw_futures_margin/source/train.py`，不是工作目錄的 `train.py`。原程式來源雜湊為 `d67dc441b1dab91058aa9a4b4b663cdb8adbf8f8ce49a6137d9f940e29298c2a`。兩者的資料驗證、排程器與基準實作不完全相同，故本次從已驗證的原始 source bundle 建立獨立修正版。

實際產物在 `artifacts/markets/tw_futures_v8_margin_components_capital100m_fold10`，名稱雖含 fold10，本次完成的是 **Fold 14**：訓練 2011–2024、驗證 2025、測試 2026-01-02 至 2026-09-04，共 163 筆測試交易日。

| 相同測試期間 | 原策略，含執行成本 | TX 近月持續轉倉，1 倍名目本金、未扣成本 |
|---|---:|---:|
| 累積報酬 | 19.7624% | 54.5166% |
| 年化報酬 | 32.1552% | 95.9556% |
| Sharpe | 1.0775 | 2.0290 |
| Sortino | 1.5412 | 3.0896 |
| 最大回撤幅度 | 24.6558% | 17.2172% |

比率沿用專案定義：每日 log return、無風險利率 0、252 日年化；報酬與回撤由累積淨值計算。原報告中的 benchmark 實際是 **0% 現金**，不是大台。因此原產物不能直接用來判定是否擊敗大台。

證據：[原帳本稽核](../artifacts/analysis/tw_futures_margin_components_fold14_20261003/original_audit.json)、[同期間大台比較](../artifacts/analysis/tw_futures_margin_components_fold14_20261003/benchmark_comparison.json)。

## 2. loss 異常的可重現原因

原訓練共 334 epochs，最佳驗證在 epoch 234，最後因連續 100 次驗證未改善而停止。

以下 10 個 epochs 都在訓練第 1175 列，即 **2015-10-08**，出現 reason 4 的交易義務失敗：

`44, 60, 74, 75, 76, 89, 106, 120, 134, 141`

例如 epoch 43 的 train loss 為 −0.1355，epoch 44 突然成為 +1.1818；裁剪前梯度範數從 1.9243 增至 204.4916。異常 epochs 的梯度最高 385.0084，正常 epochs 約 1.7–3.3；設定的裁剪上限為 1。

![原始 loss、梯度與學習率](../artifacts/analysis/tw_futures_margin_components_fold14_20261003/original_training_diagnosis.png)

圖中是原始紀錄，未平滑、未刪除異常點；綠色虛線為最佳驗證 epoch 234。

### 2.1 公告股數被 OCR 誤讀，造成假違約

為升 ODF 的調整契約 OD1，自 2015-10-08 至 2015-11-18，每口代表 2,299.6612 股；與 ODF 合併計算的自然人部位上限是 **4,599,323 股**。OCR 將第二個千位逗號讀成小數點，存成 **4,599.323 股**，相當於只允許約 2 口 OD1。

已核對期交所原始公告第 2 頁：[2231_20151008.pdf](https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/2231_20151008.pdf)。保留的官方原件、PDF SHA-256 與影像證據位於 [複核紀錄](../configs/data/tw_futures_odf_position_review_20261003.json)；[本地第 2 頁影像](../artifacts/analysis/tw_futures_margin_components_fold14_20261003/2231_20151008.page2.png)可直接檢視。

新契約首日沒有該實體契約的前日成交量，因此可新增／減碼的因果容量為 0。原本合法的公司行動移轉部位超過錯誤上限，又無可用減碼容量，帳本便判定義務未履行。**不能用當日未來成交量、忽略法定上限或虛構成交來掩蓋這個問題；應修正錯誤的公告數值。**

以真實 2015-10-08 執行 tape，注入前日 3 口 ODF，經正式移轉及現金調整後：

| 3 口多單重播 | 修正前 | 修正後 |
|---|---:|---:|
| 實際結算 NAV | 99,920,728 元 | 99,920,728 元 |
| default reason | 4 | 0 |
| 帳戶存續 | 否 | 是 |
| 報酬 log 值 | −15.94238 | −0.00079306 |

3 口空單亦通過相同方向對稱驗證，NAV 為 100,079,272 元。修正完全沒有增加價格損益，只移除了錯誤上限導致的終止。這是匹配實際失敗日期的機制重播；未保留每個異常 epoch 的模型權重，故不宣稱逐一重播了十個原模型。

### 2.2 到期失敗梯度錯誤包含正常隔夜部位

原 `_margin_physical_backward` 在某契約終端平倉失敗時，使用所有商品的 `abs(held - closing) × initial_margin` 作為失敗邊界。正常留倉商品的 closing 本來就是 0，因而被誤算成違約殘留。

現在先建立 `terminal_obligation`，只對當天確實具有終端義務的契約計算殘留。其他正常隔夜商品不再承受這項無關梯度。真正破產仍吸收終止；真實到期失敗仍保留其減倉梯度。

這個缺陷經獨立測試重現；上述十次原訓練事件的主要證據指向公告上限錯誤，不能將兩者混為同一個已證實的原因。

### 2.3 凍結程式沒有使用 YAML 的學習率下限

原執行版本的 `ReduceLROnPlateau` 沒有傳入 `min_lr` 與 `threshold_mode`。因此設定中的 `lr_scheduler_eta_min: 0.000001` 未生效，實際最後降至 **0.00000001953125**，低於設定下限 51.2 倍。

此外，對需要最小化的負 loss，relative 門檻 `best × (1-threshold)` 可能把輕微變差視為改善。新版明確使用 absolute improvement，並傳入原先設定的最小學習率。這修正排程器語意，不能保證驗證 loss 單調下降。[PyTorch 官方 API](https://docs.pytorch.org/docs/stable/generated/torch.optim.lr_scheduler.ReduceLROnPlateau.html)

### 2.4 後期也有泛化差距，並非所有波動都是 bug

epoch 234 → 334，train loss −0.27513 → −0.27519，驗證 loss 卻由 −0.32289 退至 −0.27900。這表示訓練改善已非常小，驗證表現仍可能退化。模型使用驗證最佳 checkpoint，最後一個 epoch 不會自動取代最佳結果。

因此不以抹平圖形、重設全現金、刪掉真實虧損日或強迫降低曝險作為修正。先消除資料／梯度／排程器錯誤，再由新版訓練判斷剩餘的估計誤差與泛化能力。

## 3. 交易與結算的一階原則

期貨保證金是擔保品，不是買進時消耗的名目本金。設上一日結算權益為 E、公司行動後實體口數為 q、公司行動現金為 C，每口開盤價值 O、前次可比結算價值 P、當日結算 S，則：

```text
開盤權益 = E + C + Σ q × (O − P)
收盤權益 = 開盤權益 − 進場成本 − 終端平倉成本
         + Σ 成交後持倉 × (S − O)
         + Σ 終端平倉口數 × (終端結算價值 − S)
```

公司行動需在同一實體所有權鏈上移轉口數、現金及可比前值。跨月價差不能直接當作已持倉期貨報酬；下一日開盤價差與當日結算段不能重複計算。官方契約價值與保證金計算依據見 [期交所計算說明](https://www.taifex.com.tw/cht/5/margingCal)。

原測試帳本已驗證：全部為整口、NAV 連續、存續日 log return 與 NAV 比值一致、163 日均有實際部位、沒有測試期違約。平均請求保證金 gross 為 0.7212，平均實際名目 gross／開盤權益為 1.0577，最高 1.8066。保證金預算與名目曝險不能混為同一個比例。

本次保留既有每日開盤／結算風險時鐘、前期容量、漲跌停方向限制、手續費、交易稅、公司行動與正式結算。日線驗證不等同已證明盤中逐筆追繳或強平路徑。

## 4. 梯度能證明什麼

整口選擇與可成交性是離散函數，不能宣稱存在處處平滑的真實導數。現有訓練採用以實際成交軌跡為錨點的 STE：前向維持整口帳本，反向估計相鄰可行動作的影響；它是有偏梯度估計。

修正後，在同一份 30 日真實修正 tape 與相同小幅多空請求下，原／新帳本全部 15 個 tensor 結果欄位逐位元相同，正常存續路徑的梯度亦逐位元相同。只有終端失敗邊界的歸屬被修正。CUDA Graph 與 eager 路徑另通過整口帳戶、梯度、分段繼承狀態驗證。

單次 optimizer update 跨完整訓練 trajectory 的設定保留，沒有改成每個 batch 更新而混用不同策略的跨日帳戶。裁剪上限亦保留；移除錯誤的極端梯度來源比只調大裁剪值更有依據。

## 5. 已完成的修正及資料邊界

1. 建立來源綁定的數值複核工具。僅修正 ODF／OD1 指定期間的 177 列、兩種部位上限欄位；其餘規則與範圍外資料完全相同。合法小數股數仍保留，例如 HOF／HO1 的 720,958.42 股，沒有全域去小數點或一律乘 1000。
2. 經原發布器、財務數值、公司行動與延續驗證建立獨立 release；完成與原始相同市場資料所綁定的 2816 固定槽位 metadata，預檢可載入。
3. 將終端失敗梯度縮限於實際義務契約，梯度合約 11 → 12；前向帳本合約維持原規則。
4. 修正 plateau absolute 門檻與 min_lr，並納入 checkpoint 相容性合約。
5. 接上獨立、SHA-256 鎖定的 TX 基準來源。轉倉日使用**新近月契約自己的前一交易日收盤價**；不以兩個不同契約的價格相除。缺少行情或 checksum 不符會拒絕；基準不會被加入策略可交易商品範圍。
6. 新版 fold 完成時另輸出 `futures_benchmark_audit.npz`，逐日保存實體月份、轉倉旗標、當日與前日同契約價格，且驗證其報酬確實等於圖表所使用的 benchmark。

市場行情檔新舊 **byte-identical**：`805d6e73c898e6bc86a375f2c75d48a5cfe3e568c3c450aea5dc773138878f23`。新版 rules：`2e50bf4b567b0144a74cb5b335d4092784daf2e472abd4ac03fdf3d4eb211c8f`。TX 來源：`8f3f62da483b6b7ee1652b9c615be8f7b41b4305cfca84a459fa3aeb8e5ed551`。

仍沿用 685 商品、4,180,811 筆行情的既有選取研究範圍。原 release 含 `position_research_contract` 及事後完整性選取，並未擁有全部商品完整的官方歷史部位上限。本次沒有把這些假設改寫為已驗證的實盤歷史，也沒有擴張商品範圍。2026 已被本研究觀察，不能再稱為未曾看過的獨立測試集。

## 6. 驗證結果與重現方式

- 工作目錄：140 項既有保證金／公司行動／資料／排程器測試，18 項本次回歸測試，9 項 CUDA 保證金路徑測試，合計 **167 項通過**。
- 獨立執行版本：再執行上述 18 項回歸及 9 項 CUDA 測試，合計 **27 項通過**。
- 真實問題日多／空各 3 口重播、正常路徑全欄位前向與梯度一致性通過。[重播驗證結果](../artifacts/analysis/tw_futures_margin_components_fold14_20261003/replay_validation.json)
- 完整 `--check-data-only` 通過：3881 個 panel sessions、2754 個股票背景 symbols、99 features、選中 Fold 14。此處 2754 是背景股票數，期貨動作槽位仍為 2816。
- 未執行正式 epoch、未產生新版策略績效，也未聲稱提高訓練吞吐量。

封存後再次使用新 receipt 執行完整資料預檢，亦通過。新程式 SHA-256：`4681beae759c8e5ba7663a3141075602118171c7d03b96d26ad842d6f1e391be`，1093 個發布檔案及 wheel／source ZIP 已驗證。[工程驗收紀錄](../artifacts/analysis/tw_futures_margin_components_fold14_20261003/engineering_acceptance.json)、[最終預檢紀錄](../artifacts/analysis/tw_futures_margin_components_fold14_20261003/preflight_pinned.log)、[原始產物保留驗證](../artifacts/analysis/tw_futures_margin_components_fold14_20261003/original_preservation.json)。

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python -m pytest \
  test/test_tw_futures_margin.py test/test_tw_futures_margin_corporate.py \
  test/test_plateau_negative_loss.py test/test_tw_stock_context_futures_portfolio.py \
  test/test_futures_margin_repair_v2.py -q
STOCKAGENT_TEST_CUDA_GRAPH=1 run_fintech_python -m pytest \
  test/test_futures_cuda_graph.py -q -k margin

run_fintech_python scripts/verify_tw_futures_margin_repair.py \
  --evidence-dir artifacts/analysis/tw_futures_margin_components_fold14_20261003 \
  --old-source artifacts/runtime/tw_futures_margin/source \
  --old-release artifacts/markets/tw_futures_v8_margin_preparation/margin_components_20261003/release \
  --new-release artifacts/markets/tw_futures_v8_margin_preparation/margin_components_position_review_20261003/release \
  --review configs/data/tw_futures_odf_position_review_20261003.json
```

## 7. 可編輯設定與訓練指令

[新版 YAML](../configs/markets/tw_futures_v8_margin_components_repaired_capital100m_fold14_v2.yaml) 繼承原本可見的 [完整設定](../configs/markets/tw_futures_v8_margin_components_capital100m_20261003.yaml)，只明列本次變更。

| 設定 | 新版 |
|---|---|
| 初始資金 | 100,000,000 TWD |
| epochs 上限 | 1000 |
| 驗證與早停 | 每 epoch；連續 100 次未改善；min_delta = 0.0001 |
| 輸出模式 | `score_entmax_log_cash`，沿用原型 |
| 初始化 | 原本隨機初始化；無 pretrained root、無跨 fold warm start、未改成全現金 |
| 學習率 | 1e-5；plateau factor 0.5、patience 10、absolute threshold 1e-4；下限 1e-6 |
| batch | train 128、eval 16；保留每 trajectory 一次 optimizer update |
| 精度／分散式 | BF16、原 DDP 設定 |
| profiler | 不傳入 `--profile-timing` |

首次跑新版使用獨立產物根目錄，**不要 resume 舊版 optimizer／checkpoint**。公告規則、梯度及基準合約已變更。

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
export STOCKAGENT_CODE_RELEASE_RECEIPT="$PWD/artifacts/runtime/tw_futures_margin_repaired_v2/code-release/release.json"

run_fintech_python artifacts/runtime/tw_futures_margin_repaired_v2/source/train.py \
  --config configs/markets/tw_futures_v8_margin_components_repaired_capital100m_fold14_v2.yaml \
  --start-fold 14 --max-folds 1 \
  --no-retrain-completed-folds
```

產物：`artifacts/markets/tw_futures_v8_margin_components_repaired_capital100m_fold14_v2`。若這個新版中途被中斷，才在相同指令加入 `--resume` 接續同一版。

## 8. 績效目標與下一步判定

`log_utility` 最小化的是負平均 log 成長；它不等同同時最大化 Sharpe、報酬並最小化 MDD。例如小報酬近似下 `E[log(1+r)] ≈ E[r] − E[r²]/2`，已偏好成長相對於變異，但不直接控制整條路徑的最大回撤。

因此本次先修正有證據的規則／微分／報表問題，保留原自由多空與現金輸出。沒有另加強制曝險、benchmark 跟隨、回撤停機或人工選股規則，也沒有憑已看過的 2026 報酬搜尋參數。

新版正式跑完後，先檢查十次舊異常的相同日期是否仍發生義務失敗、梯度是否有限、學習率是否遵守下限、最佳驗證快照是否正確使用。**曲線不必完全平滑；任何新的跳躍都需先定位到日期、實體契約、帳戶原因與梯度來源。**

績效比較使用同一期間、同一 return 定義：報酬 > 54.5166%、Sharpe > 2.0290、MDD 幅度 < 17.2172%，三項同時成立才算達成使用者目前的 Fold 14 目標。這些是原資料下的測試比較門檻，不能用來選擇訓練中的 epoch。

如果修正後仍泛化不佳，下一步應先以訓練／2025 驗證資料檢查逐年貢獻、訊號集中度及輸出對擾動的穩定性，再評估模型表徵或目標函數。是否改成顯式風險目標需要單獨記錄，不能把策略假設變更包裝成計算 bug 修復。
