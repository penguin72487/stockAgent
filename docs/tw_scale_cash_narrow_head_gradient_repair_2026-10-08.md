# 16 維 FP32 head：保留 Inductor 的梯度修復與新版 27 項消融

## 1. 執行進度

- 使用者決定：新版 **27 項全部重跑**；只跑每項最後一個 fold，保留舊正式產物。正式長訓練尚未啟動。
- 使用者最新要求：**不接受 AOT eager 作為正式修復**。已從正式候選移除；先前 AOT eager 的工程測試僅保留為數值 oracle／被否決方案紀錄。
- 已驗收並固定 source：`30db2fe4f91db56bcfc9b0adf4936de1654e18ba3c2fc3703b90a8c2b9f8c530`。小型 head 仍以 Inductor `fullgraph=True` 編譯；encoder、分鐘會計 loss 核心與 commit 的既有 Inductor 路徑不關閉，沒有把 I/O／排程宣稱為 compiled。
- 實際 RTX 5090 雙卡 DDP 完整 3 epoch 與從第 1 輪 checkpoint 恢復到第 3 輪，均退出 0。兩次 canonical lifecycle 各 **18 個必需產物**全部通過，包含 train／val／test、回測、checkpoint 與 PNG。
- 本機 focused tests **88 passed**，共享 model／checkpoint 回歸 **292 passed**。遠端 Torch 2.11 語義／ABI 測試 **47 passed、1 deselected**；未執行的 CPU Inductor toy 不冒充 CUDA 驗收，另有真實 CUDA captured-head 與完整 DDP 證據。
- 遠端預設入口已切到全新 **v6** root。27 項配置／calendar gate 全部通過，26 項最後 fold11、lookback256 最後 fold10；正式 1,000 epoch／項的整輪尚未啟動。

在 vastai1T 前景執行：

```bash
cd /root/stockAgent
bash scripts/run_tw_scale_cash_annual_ablations_vastai1t.sh
```

首次執行從第 1 項開始新版 27 項，不沿用 v5 optimizer；同一 v6 中斷後預設 canonical resume，已完成項目經產物 gate 後跳過，未完成項目接續。每項完成即刷新比較圖，profiling 與 debug timing sync 預設關閉。

## 2. 真正失敗的位置

使用者貼上的第三項 `embedding__d16`，在兩個 rank 的 full-trajectory optimizer 前報 `non-finite gradient after full-trajectory accumulation`。

原始 frozen source 為 `c3edd343c330a7a64cbf2db7a7d6653b71724c3a303ac5fb9ba97a051b87edcc`。使用同一配置、正式資料和雙卡、禁止任何 optimizer 更新的 11 批重播顯示：

1. loss、配置輸出、loss 對配置的梯度都有限，118 個參數梯度張量也有限。
2. 第一批梯度 FP64 norm 為 `4768.5828`，第四批後達 `1.3719014630915455e25`；FP32 平方和溢位，使原先裁切 norm 變成 Inf。
3. 只將 head 改為 eager 作為診斷 oracle，encoder 和 loss 仍 compiled，完整 11 批 norm 為 `11.998024912720421`。因此不能只修 norm 然後接受巨大有限梯度。
4. 相同真實第 4 批 head 快照，拆開 context 與 allocation：allocation 單獨 compiled 的參數梯度 relative L2 誤差約 `3.25e-5–3.91e-5`；錯誤範圍縮到 context。
5. 再縮到第一個 latent attention：前向最大差約 `1.19e-7`，Q 梯度和輸出投影梯度接近 oracle，但 K/V 路徑及其輸入投影梯度錯誤；部分 compiled 佈局會放大到極大有限數。

這些證據定位了實際小型 compiled SDPA 的 K/V adjoint 路徑；**尚未證明是哪一個上游 Triton/CUDA kernel 或哪一條生成指令**，不把其他上游 issue 當成本專案的已證明根因。

## 3. 修復算法：邏輯 4 通道，實體對齊 tile

`d_model=16`、4 個 attention heads，因此每個 head 的邏輯通道數是 4。原始長 key 軸為 2,757。只補通道、只補 key、只換連續佈局，均未通過梯度驗收。

在共享 `FlashSDPAAttention` 的 **portfolio head、compiled、Blackwell、FP32、cross attention、head_dim < 8** 路徑加入雙軸對齊；以明確的模組角色旗標保證 temporal encoder 不受影響：

- Q/K 的 RMS normalization、RoPE 仍在原本邏輯維度上計算，之後才補零通道到 16。
- 長 key 軸對齊 128：2,757 → 2,816，新增的 59 個 key 全部遮罩為不可見。
- 短 latent／market contexts 按 16 對齊；原有 16 個 token 不擴成 128，避免把這些 stock reads 放大 8 倍。
- 明確保留原始 scale `1/sqrt(4)=0.5`，不能改用補零後的 `1/sqrt(16)`。
- 用原生 SDPA 與 Inductor 編譯，輸出切回原有 4 通道。沒有新增可訓練參數、沒有改成 64 維 embedding、沒有 eager/CPU fallback。

此遮罩與 scale 的語義依 [PyTorch 2.11 SDPA 官方契約](https://docs.pytorch.org/docs/2.11/generated/torch.nn.functional.scaled_dot_product_attention.html)：布林遮罩 True 才參與 attention，並可明確指定 scale。版本對準遠端 2.11，不把其他版本的生成 kernel 當作本次證據。

實數算術中，補零後的 `Q'K'^T = QK^T`；新增 key 的 softmax 權重為 0，故 `(softmax(Q'K'^T/2)V')[:4]` 與原版相同。反向經 padding 與 slicing 回到原本 Q/K/V，邏輯 adjoint 也相同。有限精度下仍須實測，而不能只引用這個等式。

實際兩 rank、128 筆／rank、2,757 股票的完整 head，用正式 TF32/BF16 runtime 與真實 upstream gradient 驗證：

| 誤差 | rank 0 | rank 1 |
| --- | ---: | ---: |
| head 輸出最大絕對差 | 0.0002097115 | 0.0001008958 |
| embedding 梯度 relative L2 | 0.0003149852 | 0.0001872013 |
| 全部 head 參數梯度 relative L2 | 0.0003697756 | 0.0001669288 |

全部梯度有限。此處是 TF32 開啟的比較，不能宣稱逐位一致。獨立第一層 attention 的雙軸對齊測試在 FP32 highest 下，輸入投影梯度最大絕對差不超過 `4.66e-10`。兩者是不同精度／驗證範圍，不能混成一個結果。

證據：[實際 head-only captured probe](../artifacts/operations/training_diagnostics/tw_scale_cash_d16_20261008/head-canonical-head-only-v2.json)；獨立第一層 attention 的歷史定位證據在遠端同目錄 `head-attention-tiles-v1.json`。

## 4. 裁切穩定性與相容性

- 只把梯度 norm reduction 升至 FP64，參數及梯度儲存仍是 FP32；BF16 AMP、配置和財務 loss 的既有精度政策保留。
- 使用 foreach reduction，不在每批額外掃描所有梯度做同步。
- 對裁切係數使用兩次平方根乘法，避免有限的 `2e38` 梯度因 FP32 次正規係數被 flush 成 0。正常梯度與原裁切結果保持浮點誤差內一致。
- 真正 NaN／Inf 不會被 `nan_to_num` 或零乘法藏掉，仍由 canonical finite gate 拒絕。
- 小型 head graph contract 改為 `compiled_partition_sm12_narrow_fp32_head_only_aligned_sdpa_adjoint_abi_v9`，拒絕錯誤 v6 graph、先前 AOT v7 和過廣對齊 v8 候選的 optimizer，不能用舊 checkpoint 續接新版。
- 新版消融 root 為 `artifacts/markets/tw_scale_cash_annual_last_fold_selected_20261008_v6`。此版本建立新 optimizer；往後同版本中斷仍走 canonical resume。

## 5. 完整雙卡、續跑與耗時驗收

相同正式資料、14,726 features、2,757 symbols、lookback32、global batch256（128／rank），保持 BF16 AMP、FP32 head 與原會計／50% 進場容量／收盤不限容量設定。每個 epoch 全年軌跡累積 11 批、只更新 optimizer 一次；沒有用小面板或較少日期替代。

| 連續訓練 epoch | 裁切前梯度 norm | 完整 epoch MAX rank 秒 | train MAX rank 秒 |
| --- | ---: | ---: | ---: |
| 1（cold） | 11.996969 | 201.115553 | 144.962525 |
| 2 | 9.132857 | 90.844334 | 76.896468 |
| 3（穩態驗收） | 11.050696 | 94.763937 | 81.396872 |

第 3 輪 checkpoint I/O 約 0.324 秒；新 Dynamo graph 數為 0，沒有每輪重新編譯。每輪 train 分支有 2,685 次 compiled session calls 與 2,685 次 compiled commit calls，zero-gradient batches、flat-terminal fallback batches 均為 0。最終完整 fold 報表、回測與彙總圖亦完成，並通過 canonical artifact validator。

安全 AOT eager 工程參考的第 3 輪 MAX rank 為 95.093196 秒，新版 94.763937 秒；差距約 0.35%，**不宣稱整輪有顯著加速**。修復成果是以對齊算法保留正確的 Inductor head，而非關閉編譯。Cold 編譯／cache 狀態不同，不拿兩次啟動時間當公平速度比較；也未證明機器的理論 lower bound。

續跑使用同一 3-epoch scheduler、同一 source、實際第 1 輪 checkpoint。兩 rank 均從第 2 輪恢復 Python／NumPy／Torch RNG，最終 curve 正好 `[1,2,3]`，無重複。118 組 AdamW state 的 step 均為 3，scheduler `last_epoch=3`，模型與 optimizer 值全部有限，BF16 GradScaler 保持停用。

連續訓練與續跑的最大模型值差 `6.0913153e-6`，第 2 輪 train loss 相同。這不是 bitwise reproduction：整數張數／成交門檻可放大小浮點差，工程測試的最終測試報酬差為 **0.0202623 個百分點**（1.7701343% vs 1.7498719%）。此處只驗證恢復機制與數值邊界，不以 3 輪報酬得出投資或消融結論。

驗收：[repair-acceptance-v2.json](../artifacts/operations/training_diagnostics/tw_scale_cash_d16_20261008/repair-acceptance-v2.json)。完整 log 在遠端同目錄 `aligned-ddp-smoke-v2.log`、`aligned-ddp-resume-v2.log`；工程產物分別在 `artifacts/markets/tw_scale_cash_aligned_head_ddp_smoke_20261008_v2` 與 `tw_scale_cash_aligned_head_ddp_resume_20261008_v2`，不得拿其 3-epoch optimizer 接到正式 1,000-epoch queue。

## 6. 隔離、保留與剩餘邊界

候選由已接受報表 source `f7954bf29761d90abdfbe929b88422f93991c55ac32571925a7dfa0afee2ef50` 衍生，只有四個 core files 改動：`factorized_input.py`、`transformer_base_portfolio.py`、`checkpoint_contract.py`、`trainer.py`。沒有把本機其他 dirty work 混入 frozen release。原本的 incremental comparison plots 保持不變。

過廣對齊候選 `f5fa65d9d7e5` 曾完成 2 個實際雙卡 epoch，梯度有限，epoch 1／2 MAX wall 為 269.13／94.56 秒；但也修改了大量 temporal last-query 的實體佈局，增加 cold compile／workspace。故沒有發布，僅中斷此隔離工程工作，保留其 checkpoint 與紀錄。新版以 head 角色旗標移除這個額外成本；第 2 輪時間不能代替第 3 輪正式穩態驗收。

舊 v5 的兩項已完成消融與模型 checkpoint 未刪除／覆寫；重新驗證其 **124 個既有檔案** SHA256 均不變。診斷期間，原本失敗的 `embedding__d16` launcher／setup 紀錄及 queue summary 共 7 個檔案曾更新，因此不能聲稱整個 v5 目錄 byte-identical，明細保留於驗收 JSON。預設入口切換前也保留了舊薄控制程式與 shell 副本於 v6 operations 目錄。

新版入口先驗證 immutable source／bundle、真實雙卡數值修復收據與全 27 項因果 calendar，再啟動 canonical runner。AOT backend、單卡證據、未通過 fullgraph／adjoint／resume 的候選會被 gate 拒絕；此行為有 focused regression tests。報表 scripts 與已接受版本逐檔相同，每項產圖功能保留。

遠端入口驗收：[entrypoint-acceptance-v1.json](../artifacts/operations/training_launches/tw_scale_cash_annual_last_fold_selected_20261008_v6/entrypoint-acceptance-v1.json)；[全 27 項 calendar](../artifacts/operations/training_launches/tw_scale_cash_annual_last_fold_selected_20261008_v6/calendar-context.json)。本機／遠端四份控制檔 SHA256 一致，`--list` 與全輪 `--dry-run` 退出 0，新正式 root 尚無 checkpoint。

正式 source、output root、optimizer ABI 明確分版；沒有更改資料、特徵、年度 ownership、股價 tick、費用或容量規則。工程驗收僅覆蓋本次真實 embedding16 形狀與環境；27 項全部只完成配置／calendar 準備，尤其 embedding64／128 的正式 VRAM 與完整 epoch 仍待實際執行。不是完整 test suite、不是 27 項全數訓練完成，也不是永久不出錯的保證。
