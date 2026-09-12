# 2026-09-10：隔離合約日保留持倉，官方結算估值與 v11 修復

後續更新：使用者的 v11 正式訓練已完成前三折，之後於第 4 折第 58 epoch 碰到嘉澤調整合約缺口。最新來源修復與 v14 入口見 [第 4 折來源修復紀錄](FUTURES_FOLD4_JF_RF_REPAIR_2026-09-10.md)。下文保留 v11 當時的驗證結果與範圍。

使用者已明確核准：「隔離日保留持倉、禁止該合約成交，以核實的官方結算價記帳，下一交易日恢復原規則」。程式、設定及來源補齊已安裝於 vastai1T 的 `/root/stockAgent`，預檢、舊模型全期間重播及雙 GPU 短訓練已通過；可用下方指令開始新的 v11 訓練。正式 1,000 epoch 訓練尚未啟動。

本機與遠端有不同的既有修改，本次只針對遠端版本增量修補。本機供核對的遠端程式位於 `artifacts/operations/futures_carry_quarantine_repair_20260910/patched/`，沒有以本機較舊版本覆蓋遠端訓練器。

## 實際失敗

遠端 `artifacts/markets/tw_stock_futures_0845_carry_v10_vast5090` 完成第 320 個 epoch 後，在第 321 個 epoch 的驗證階段停止。
`FuturesCarryDataError` 指向 `2021-06-21 / LVF:202107 / 5871`，當時帶有前日的多單 1 口。
它是已明確隔離的合約日；v10 隔離只移除新倉候選，後來加入的 residual carry 仍保留原實體合約，因此碰到未知分鐘來源的拒絕條件。

`compiled 7 new graph(s)` 是第一個 epoch 的編譯暖機訊息，並非這次失敗原因。兩張 RTX 5090 與 CUDA 的嚴格環境檢查通過。

## 已核對的來源與範圍

| 隔離合約日 | 隔離前曾為候選的日期數 | 當日保留的其他候選合約 | 備註 |
| --- | ---: | ---: | --- |
| 2021-06-21 / LVF:202107 | 2 | 209 | 官方結算價 199；一般成交 2 口，分鐘時間仍未知 |
| 2023-07-13 / PZF:202308 | 0 | 244 | 此日之前沒有該實體合約的候選建倉日期 |
| 2024-01-10 / LIF:202401 | 13 | 250 | 官方結算價 87；一般成交 1 口，分鐘時間仍未知 |

候選日期數不等於實際持倉證據。LVF 已由這次失敗持倉證實；LIF 是需處理的潛在同類情況。
LVF 與 LIF 的結算價經既有官方來源 receipt 與原始檔 SHA-256 驗證。結算價只能證明估值，不證明特定分鐘可成交。不可把缺少的分鐘改成零成交、插值或日線成交。
PZF 的日線來源與候選日期亦經核對；它不在 carry 官方估值 sidecar 的這兩筆列中，不能以有其他日線資料就宣稱相同 sidecar 證據已驗證。

## checkpoint 保留

| 檔案 | epoch | SHA-256 |
| --- | ---: | --- |
| `train_2020/checkpoint_last.pt` | 320 | `1c9478093a83d4e1c794186d9ba5a90342f41be5f8ae10ac35ed94aec1682083` |
| `fold_01/checkpoint_best.pt` | 249 | `c8e6905e066f8b56b54f36b6abc45a44e417a4307988f1ba1ccf891ed9e15fe6` |

兩個檔案在讀取前後雜湊相同。`checkpoint_last.pt` 包含 optimizer 與 RNG state；備份不表示可以跨不同執行規則直接續用 optimizer。
程式、設定、失敗證據與 checkpoint 備份在本機及遠端的 `artifacts/operations/futures_carry_quarantine_repair_20260910/`。

## 已實作的規則與相容性

`configs/markets/tw_stock_futures_day_trade_0845_carry_v11.yaml` 繼承 v10，啟用 `tw_stock_futures_day_trade_quarantined_carry_policy: hold_official_settlement`，並固定下述 v3 合約轉換資料。其他交易時鐘、模型、費稅、50% 向上取整容量及訓練參數保持原設定。

- 只對既有明列的實體合約日停用所有買賣，包括新倉、減倉、反向與排程退出。保留原多空口數，當日不產生交易費稅；其餘合約照常運作。
- 用核實的同日同合約官方結算價做日終損益與持倉估值。日終結算價不參與當日開盤可用資金或委託口數。
- 次一交易日恢復原本目標減持倉差額及 08:46／13:20／13:24–13:30 成交規則；不保證市場必有足夠容量成交。
- 原始分鐘來源仍標示未核實，並保留原始資料。不把未知來源宣稱成官方零成交，不新增隔離。缺少官方估值、錯誤身分、不支援公司行動與隔離碰到到期日仍拒絕。
- 執行契約 v6 增為 75 個通道；資料、loss、回測、報表與 checkpoint 共用版本常數。新增來源與政策納入指紋。舊版本預設 `reject`，保留原相容性。
- 修正 `--check-data-only` 未傳入 residual carry 參數的缺漏，預檢現在也檢查持倉來源、官方結算及調整合約。

v11 使用新的 `artifacts/markets/tw_stock_futures_0845_carry_v11_vast5090`，從新 optimizer 開始訓練；沒有把 v10 的 320 epoch 當成新規則下已完成的訓練。兩份舊 checkpoint 保留並用於唯讀重播。

## 重播發現並修復的 CLF → CL1 資料缺口

第 249 epoch 最佳模型的完整測試期間，額外走到 `2023-12-08 / CLF:202312 / 2886` 的持倉合約轉換。依[期交所兆豐金調整公告](https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/2886_20231208.pdf)，既有合約轉為 CL1，乘數仍為 2,000、口數一比一；新 CLF 不得拿來平掉舊的 CL1。

沿用原本 PL1 的公告與實體持倉轉換機制，建立不可覆寫的新資料版本：

`artifacts/data_preparation/futures_corporate_transitions_v3_20260910/manifest.json`

SHA-256：`8ae9c433a74c0a6fdfd49a4408c0442c1d4d61c521d120182bce2ac20ae69372`。

CL1:202312 自 12 月 8 日到 12 月 20 日的 9 個交易日，以 141 筆永豐原始 Tick 重建分鐘；各日開高低收與扣除價差交易腿後的一般成交量完全等於官方日報。Tick 原始檔與 receipt 的雜湊、日期、查詢代碼及實體交割月均逐次驗證。先抓取的 92 筆 KBar 因 Amount 截斷而與 OHLC 不符，留存原始證據但不投入訓練，也未放寬校驗。

到期日採[期交所最後結算表](https://www.taifex.com.tw/cht/5/sSFFSP?down_type=1&queryYear=2023&queryMonth=12)的完整每口價值 **77,759 元**，並非只算 `38.76 × 2,000 = 77,520`。新資料版本總共 120 個合約日，保留前版 PL1／VNF 來源；沒有增加隔離或改動新倉候選。

## 驗證結果

- CUDA 嚴格環境檢查通過；兩張 RTX 5090，沿用 BF16/DDP。
- 隔離與既有 carry／公司行動測試 86 通過；共用 checkpoint／resume／訓練整合測試 140 通過。
- 新增 Tick 真實來源校驗與公司行動回歸測試 21 通過，其中 7 項為新測試；包含重新計算雜湊後仍拒絕錯誤日期、身分、官方成交量、價格與人為修改的分鐘帶。
- 第 320 與第 249 epoch 模型各完成全部 244 天驗證、1,133 天測試。另以實際 LVF/LIF 來源測試多空既有持倉、隔離日零交易與次日恢復，均通過。
- 最終 v3 資料的預檢通過全部 1,573 天。雙 GPU 短訓練上限 3 epoch，依原早停公式於第 2 epoch 結束，包含每輪訓練／驗證／測試及完整最終測試；每輪 1 次 optimizer 更新。第一輪記錄 19.19 秒、第二輪 1.87 秒，時間記錄不含全部啟動／最終報表耗時，且 GPU 計時未強制同步，不作為正式吞吐量保證。第一輪 Dynamo 新增 7 張圖，第二輪 0 張；BF16 scaler state 為空。
- `validate_completed_training_artifacts` 通過，缺漏與非法產物均為零；九張必要的累積 walk-forward 圖全部存在。再次執行相同命令正確略過相容的已完成 fold，啟動 0 個新 fold，epoch 曲線維持 `[1, 2]` 無重複。這次實機重啟驗證的是完成結果重用；optimizer/scaler/scheduler/RNG 恢復由共用 resume 回歸測試覆蓋。

重播使用現有完整資料切分、特徵正規化、BF16 模型及共同 evaluator；不匯入舊 optimizer。`checkpoint_replay_v3/receipt.json` 記錄原 checkpoint 雜湊、期間與結果，`*.npz` 保存逐日結果。`acceptance.json` 彙整實際程式雜湊、設定與產物檢查。這些是修復與整合證據，不代表正式訓練已完成或策略已可實盤。

## 遠端訓練指令

在 vastai1T 的 `/root/stockAgent`：

```bash
source scripts/runtime_env.sh
run_fintech_python train.py --config configs/markets/tw_stock_futures_day_trade_0845_carry_v11.yaml
```

維持原本的直接入口。若再次中斷，只有相同 v11 契約與資料版本的 checkpoint 可以原規則續訓。

## 本機可用資料與冷儲存的證據邊界

訓練固定使用 `tw-public-20260906T151104843336596Z-l0-penguin-246aab6e5c72e427`。遠端既有 materialization 的 114,841 個檔案（9,270,708,788 bytes）已重新通過完整 inventory、逐檔 SHA-256 與 portable fingerprint；READY 的 release／manifest／inventory 身分相符。既有七日 lease 有效至 2026-09-16 11:15 UTC。

此舊版本在 penguin 與遠端 cold store 都缺 102 個物件（912,878,521 bytes）。因此 `run_data_cache.sh use ... --snapshot-id ...` 的重新補齊逾時，不能聲稱這版冷儲存可完整重建；Syncthing 100% 也不能補足權威端已缺的歷史物件。訓練使用的是上述已逐檔核實的現存資料，不是從缺失物件重建。沒有刪除這份 materialization、改用最新 release、改寫 READY 或繞過 GC 保護。證據為 `hot_cache_verification.json` 與 `edge_audit.json`；冷備份修復是獨立待辦。

## 可重現的唯讀核對

```bash
source scripts/runtime_env.sh
PYTHONPATH=. run_fintech_python artifacts/operations/futures_carry_quarantine_repair_20260910/audit_quarantine.py
```

輸出 `quarantine_audit.json` 包含來源證據、精確隔離範圍、候選日期、原始錯誤與 checkpoint／備份雜湊。它是診斷報告，不是訓練完成或就緒證明。
