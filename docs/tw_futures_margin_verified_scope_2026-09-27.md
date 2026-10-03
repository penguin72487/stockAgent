# 台指期貨保證金訓練資料準備

## 事前資料契約

此版本沿用 v5 股票背景模型、固定 1,936 期貨輸出欄位、既有保證金帳本與
`train.py`。使用者同意先交付規則可驗證的商品與期間，初始資金為新臺幣一億元。
第一版限定 TX、MTX 月契約，自 2021-05-20 起；其他商品與更早資料原樣保留。
這是資料證據選擇，不依驗證集或測試集報酬挑選商品。

資訊時鐘：

1. 開盤前模型只能取得前一已完成交易日的股票／期貨特徵，以及此前已公告且生效的規則。
2. 原始公告只有日期時，以當日 23:59:59 作為「最遲已知」上界，不能假裝有精確公告時間。
3. 「一般交易時段結束後」調整在收盤後帳戶評價階段生效，當天開盤仍用舊額度。
   一般日評價邊界為 13:45，到期日現金結算邊界為 13:30。
4. 每日結算價只作收盤後評價；前次結算價必須來自同一實體合約，不能從重用欄位取得。
   新掛牌合約在有前一交易日官方結算價之前不納入可交易範圍。
5. 收盤無成交但有官方結算價時，可評價持倉，不能用結算價捏造成交。
6. 歷史保證金 CSV 必須與同公告 PDF 的幣別、六個調整前後數值及生效日互證；
   連續調整事件的前後值必須銜接。部位限額採自然人歷史公告。

沿用現有保守研究政策：不計跨月保證金抵減、不使用當沖保證金折扣、同標的絕對口數
總和按 TX:MTX = 1:4 限制；不代表券商完整盤中風控。日頻資料只能驗證日頻開盤調整／
結算追繳模型，不能聲稱重播夜盤及盤中強平。佣金每邊每口 40 元是研究設定。

完成條件是來源雜湊、規則時鐘、逐日帳本覆蓋、CUDA 環境與遠端完整訓練生命週期均通過。
單有資料檔案、預檢或合成測試不構成遠端可訓練證明。驗收結果在完成後追加。

## 可重建的資料版本

正式設定：`configs/markets/tw_futures_v8_margin_verified_capital100m.yaml`。
資料位於 `artifacts/markets/tw_futures_v8_margin_preparation/verified_scope_20260927_v3/`。

| 項目 | 範圍 |
|---|---|
| 可交易商品 | TX、MTX 月契約；138 個實體合約 |
| 日期 | 2021-05-20 至 2026-09-04；1,290 個交易日 |
| 對齊後規則 | 15,352 個合約交易日，逐列有保證金、部位限額及價格界線 |
| 歷史事件 | 每商品 39 次保證金調整；9 次自然人部位限額調整 |
| 來源證據 | 609 份具雜湊的原始公告、解析、公告索引及資料證明 |
| 無成交評價 | 176 列使用同日官方結算價，保持不可成交 |
| 新契約首日 | 128 列因缺前日同實體結算價而未納入；未移除合約中間持倉日 |
| 未納入商品 | 原日資料另有 505 種商品；完整清單在 `daily/all_product_scope.csv` |

`daily/continuous_daily.parquet` 沿用原固定欄位契約，修正實體合約前值與每日／最後結算價。
`rules/rules.parquet` 沿用既有 validator。來源原檔及原本全商品資料都保留。
2017 年官方[規則修正對照表](https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/網站公告(5).pdf)
第 11–24 頁提供 TX/MTX 的交易時段、前次日盤結算價上下 10% 與 4:1 合併部位依據；
保證金金額和部位限額則逐次採各自公告，不由現行表回填。
一份 2023-01-30 公告的前兩頁是影像，已直接檢視 PDF 原始內嵌頁面，
將發文／生效日期與原始 PDF SHA 綁定於 `configs/data/taifex_margin_source_reviews_20260927.json`。

## 修正資料截止日與 fold 邊界

最初 smoke 的實體帳戶期末仍有正權益，報酬序列卻近乎歸零：資料建置與通用 dataset
把樣本最後一天轉成強制平倉，在遠月合約成交量不足時觸發吸收失敗。這是研究樣本
邊界與真實交易事件混淆，不是市場損失。

此版在資料截止日、訓練切分及 stitched replay 邊界，以官方結算價標記帳戶淨值，
保留整口持倉及保證金稽核；未平倉收益是評價損益，沒有宣稱已成交變現。
真正到期現金結算、依法應執行的風險平倉與無法平倉證據仍由原帳本處理。
保證金 checkpoint forward contract 更新為 `exact_integer_margin_account_v3_marked_boundary`，
artifact margin contract version 為 2；舊邊界語義的 checkpoint 不可冒充相容續訓。
逐日規則資料 schema 仍為 1，數字欄位並未改義。

另修正規則對齊時 `datetime64[D]`、`ms`、`ns` 的日期字串差異。
本機相關資料、帳本與一般期貨回歸共 83 項通過。遠端以小範圍 patch 更新共同模組，
保留遠端另有的 crypto 修改及修改前原檔。

## 訓練設定與啟動方式

沿用 v5 架構：99 個股票背景特徵、22 個時間基底、32 日 lookback、固定 1,936 期貨輸出欄位。
BF16、兩張 RTX 5090 DDP、global batch 32（每卡 16），每個完整時間序列更新一次 optimizer。
初始資金一億元，以保證金預算產生整口部位；模型可保留現金及形成名目槓桿。
資料放 host memory，沿用模型 compile、既有帳本加速與每輪驗證／測試曲線。

此範圍的 fold 4 為訓練 2021–2024、驗證 2025、測試 2026 至 9 月 4 日。
沒有符合相同訓練年份的 v5 checkpoint，因此從頭訓練所有參數，不凍結股票骨幹。
formal 設定為 1,000 epochs，沿用原設定的提前停止規則；3 輪 smoke 停用提前停止。

在 Vast 遠端執行：

```bash
cd /root/stockAgent
bash artifacts/markets/tw_futures_v8_margin_preparation/run_margin_verified_capital100m_fold4_20260928.sh
```

正式產物：`artifacts/markets/tw_futures_v8_margin_verified_capital100m_fold4/`。
此指令先檢查 CUDA、設定及規則 release，接著使用原本 `train.py`；可恢復同一正式目錄，
已完成 fold 不會重新訓練。來源固定為本次派生期貨版本及股票 release
`tw-public-20260913T030921278338896Z-l0-penguin-248d0869d7d53be4`。

完整規則原始庫仍在本機 producer；遠端收到的是驗證後的小型訓練資料及來源證據包。
這次沒有改下載排程、啟用新的同步資料夾或宣稱所有商品歷史完整。

## 遠端驗收結果（2026-09-28）

最終 smoke：`artifacts/markets/tw_futures_v8_margin_verified_capital100m_fold4_smoke3_20260928_r3/`。
兩張 RTX 5090、global batch 32、BF16，完整跑完 3 輪及所有 fold/root 圖表、
NPZ 與 checkpoint；canonical lifecycle validator 通過。145 組 optimizer state
皆有 3 次更新，141 組一階動量非零，模型參數及梯度狀態有限，期貨 action head 非零。
本機與遠端各 83 項相關測試通過。

測試及 stitched deployment 的 163 列帳本均能把累積報酬核對至期末評價權益，
兩者期末權益皆為 296,149,600 元、保留 323 口未平倉。3 輪 smoke 的測試報酬約
196.15%、最大回撤約 62.62%、最高名目槓桿約 8.98 倍；這些數字展示高槓桿風險與
帳本一致性，只有 3 次參數更新，不能作為績效改善或實盤可用的結論。

相同 3 輪命令的 completed-fold resume 已跳過訓練；checkpoint 與 epoch curve SHA 不變。
這是完成後跳過測試，不是中斷後 optimizer/RNG 恢復測試。正式 1,000 輪沒有自動啟動。
第 3 輪 maximum-rank 約 2.54 秒僅為單次樣本，compile／初始化、root 報表另計，
不據此宣稱計算極限或預估 1,000 輪總耗時。

收據：`artifacts/markets/tw_futures_v8_margin_preparation/remote_verified_margin_smoke3_receipt_20260928_r3.json`。
交付清單及遠端驗證：同目錄的 `remote_delivery_inventory_final_20260928.json`、
`remote_ready_20260928.json`；共同程式變更另有 `remote_boundary_patch_receipt_20260928.json`。
