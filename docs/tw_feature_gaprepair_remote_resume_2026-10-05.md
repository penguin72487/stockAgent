# 台股補洞資料：vastai1T 恢復交付（2026-10-05）

## 1. 執行進度

- **10/05 最新要求已覆蓋舊通道設計：新實驗只輸入數值，不含可用性、年齡、更新旗標。** 新 panel 已完成逐位元投影，14,726 通道（14,720 經濟數值＋6 基本輸入）；原始 NULL、公布時間、TTL、生命週期及成交／資格限制仍保留。最新細節見 [數值-only／BF16 遠端驗收](tw_daytrade_value_only_bf16_remote_2026-10-05.md)。
- `project-value-only-native-panel--20261005T062419-d7761d3f` 退出碼 0；移除 44,160 狀態通道，個別特徵檔由 17,244,621,609 降到 2,667,280,861 bytes。新版 manifest SHA256：`33bed11d08665f5578b6ef11f3689ee80c99b22b260f661683d3e35eb21d9f50`。
- 新版最終 **214 項相關回歸**、fold11 資料／容量 gate、擴大雙卡 3 次 FP32＋3 次 BF16 optimizer oracle 已通過；真實 14,726 通道的完整 fold11 工作流與獨立嚴格 artifact／checkpoint gate 均退出碼 0，**固定研究版本的工程訓練已就緒**。工程最多三輪、實際兩輪依原規則早停，正式最多 1,000 輪不改設定、不帶入工程 optimizer。
- 使用者已確認清理完成，撤回原本「先不要傳」的暫停要求。
- 本機 gaprepair-v4 完整來源驗證通過；60 項補值／NULL／panel 針對性測試通過。
- 固定來源已發布並完整還原；新版 panel 已成功建完，不再重建已驗證的 view。
- 新版精確程式碼、選定設定及 20 份設定依賴已在遠端逐檔驗證；嚴格 CUDA 檢查退出碼 0，兩張 RTX 5090 可見。
- 完整 panel：3,109 個交易日、2,757 個商品、14,720 個數值特徵（14,655 個別＋65 共享），加上狀態通道共 58,886 個邏輯模型輸入。完整建置 2,653.80 秒；全 11 fold 資料 gate 與 checkpoint 磁碟預算均通過。
- 172 項模型／checkpoint／GPU 管理回歸、51 項寬 panel／更新週期／NULL／清冊／容量回歸通過；新增混合精度明確區塊後，兩張 GPU 的原數值 oracle 通過 3 次 FP32＋3 次 BF16 optimizer 更新，未放寬誤差門檻。
- 下列 58,886 通道建置及 v2 oracle 是保留的工程歷史，不是目前的新實驗。其待啟動完整 fold11 已依最新要求取消，未建立 optimizer；新數值-only 驗收沿用固定來源及 base／physical cache，不讀取舊 optimizer。
- 本次不刪除資料、舊快取、checkpoint 或 optimizer，不啟動正式長訓練。

## 2. 固定來源與實際補值

來源目錄：`/srv/stockagent-live/data_tw_daytrade_panel_sources_20261004_gaprepair_v4`。

- 來源 manifest SHA256：`167190ad3e17d43aa590bdb4160e8f9c26e5ffe9951e7d9f9069683e0280811f`。
- 資料截止日：`2026-10-02`。
- 7,821 筆跨來源月營收補值，已確認寫入實際觀測檔，保留逐格證據。
- 80,285 筆先前同 provider 的真實歷史觀測，只補在最新回應缺少有限值的位置；最新有限值和真實零值不被覆蓋。
- 原始 NULL 保留，不插值、不捏造金融數字、不把空值當作零。
- 仍是本人授權的私人研究來源：`historical_point_in_time=false`、`research_only=true`、`live_eligible=false`、`training_ready=false`。

來源缺口、更新週期、不同基準的增量與尚未證明的欄位，見 [完整補洞報告](tw_feature_expected_gaps_2026-10-04.md)。低頻、商品不適用和未接線不等於下載損壞；不能將所有 NULL 都宣稱補完。

## 3. 發布、傳輸與還原各自驗收

Dataset：`tw-daytrade-panel-sources-20261004-gaprepair-v4`。

Snapshot：`tw-daytrade-panel-sources-20261004-gapre-20261004T163754531166169Z-l0-penguin-6160c54fa6faa702`。

Packed manifest SHA256：`ad0ed2f1154e98fc957581612774d43c1b0a1fea6c6542668b545a4a65354b8a`。

| 階段 | 證據／數量 | 狀態 |
| --- | --- | --- |
| 本機來源語意與全成員驗證 | `verify-fixed-gaprepair-source-re-20261004T162758-166a1f9e`，退出碼 0 | 通過 |
| 正式 cold 發布 | 5,917 檔（含 source manifest），87 objects，5,300,700,829 packed bytes | 通過 |
| 新增傳輸物件 | 19 objects，1,273,852,436 bytes | 已發布並被遠端要求 |
| Syncthing | `idle`、needBytes／items／errors／pullErrors = 0 | 傳輸佇列清空 |
| 遠端還原／READY／full SHA | `hydrate-gaprepair-v4-20261004T163850-cf4ba7ca`，退出碼 0 | 通過 |
| 新數值 panel | `prepare-fixed-native-panel-v4-20261004T164340-1e9e6c85`，退出碼 0，完整建置 2,653.80 秒 | 通過 |
| 資料 gate、checkpoint budget | `verify-native-data-after-build-v-20261004T165531-fe73f60e`，全 11 fold、退出碼 0 | 通過 |
| v2 compiled／dense 雙卡數值 oracle | `precision-islands-dual-gpu-v2-20261005T055611-6e51dd37`，兩 rank、6 optimizer steps、退出碼 0 | 工程小型 oracle 通過 |
| 三份 exact source 續租 | `renew-exact-training-sources-v1-20261005T055126-9315e970`，退出碼 0、完整 SHA／七日租約 | 通過 |
| 舊 58,886 通道完整 fold11 | `actual-fold11-full-lifecycle-v2-20261005T060334-03f45834` | 依使用者新輸入要求取消，未開始訓練 |
| 新 value-only panel | `project-value-only-native-panel--20261005T062419-d7761d3f`，逐值／NULL／mask 保留，退出碼 0 | 通過 |
| 新 value-only 完整 fold11 | `value-only-fold11-full-bf16-v4-20261005T074109-254fb019`，雙卡、batch32、最多三輪／實際兩輪正常早停，44.47 分鐘 | 訓練／驗證／最終回測／同步圖／checkpoint／總報告退出碼 0 |
| 新 value-only 嚴格獨立驗收 | `value-only-fold11-artifact-accep-20261005T082809-97d40f7f`，18 lifecycle artifacts、group-last／fold-best strict manifest、有限 Float32 state | 退出碼 0，前景入口可用；正式長訓練未啟動 |

上表首次交付是 10/04 的 Syncthing index-only edge／exact-demand 證據。10/05 已部署的新架構以 rclone SFTP 傳 payload、Syncthing 傳索引；此次續租沿用既有 transport owner，不啟動第二個 writer。來源資料不以 SCP／rsync 另送；SSH／SCP 僅用於程式碼、設定與小型操作證據。

penguin 保持來源 authority；immutable lake、DuckLake 固定註冊與 Temporal lifecycle 分別持有資料及工作責任。Vast 用 canonical `run_data_cache.sh use` 固定 snapshot、完整 SHA、READY 與租約，再從固定來源重建訓練 view。已完成的新版 view 不必再次展開。新架構與實測恢復證據見 [DuckLake／Temporal 架構](ducklake_temporal_replication_2026-10-05.md)。

首次交付時手動要求 reconcile 曾因 owner lock 退出 75；未繞過鎖、未停止其他工作，既有排程隨後完成傳輸。這段歷史不是目前尚在等待的故障。

發布花費約 417.69 秒，包含完整來源雜湊、既有 D 冷庫物件再驗證、封裝及發布掃描；不能將這個時間稱為網路傳輸時間。

遠端 hydration 從 `2026-10-04T16:38:51.099286Z` 到 `16:41:23.922252Z`，約 152.82 秒，包含等待 exact-demand、Syncthing、物件雜湊、解包與完整還原驗證，不是純網路測速。租約為 `verification=full`；87 objects 包含 86 payload files 與 inventory metadata。`retained_payload=true`，實際刪除 0 bytes／0 files；完成後約 98.10 GB 可用空間。

## 4. 遠端目錄、硬體與設定

本段記錄建立原來源 view 的目錄、硬體與旧工程設定；最新 value-only 入口與新 artifact root 以 [新版報告](tw_daytrade_value_only_bf16_remote_2026-10-05.md) 為準。原 view 保留供無損投影及 base／physical cache 重用。

原訓練準備相關產物位於：

`/root/stockAgent/artifacts/markets/tw_day_trade_factorized_panel_20261005_gaprepair_v4`。

開始前實測約 104.79 GB 可用空間，容器 CPU quota 約 53.76 核、可用記憶體額度約 66.82 GB；不能以主機可見的 224 CPU 或 519 GB MemAvailable 當成容器實際額度。兩張 RTX 5090 各約 32.6 GB VRAM，檢查時閒置。這些是當下觀察，不是永久容量保證。

固定來源設定：[tw_day_trade_factorized_native_20261005_gaprepair_v4.yaml](../configs/deployments/tw_day_trade_factorized_native_20261005_gaprepair_v4.yaml)。目前數值驗收候選：[precision_v2.yaml](../configs/deployments/tw_day_trade_factorized_native_20261005_gaprepair_v4_precision_v2.yaml)。

- 延續已選定 v10：FinancialTransformer、last／last_only、lookback 32、d_model 32、無 24 維瓶頸。設定字面是 market-token，但同時啟用 latent factors／market tokens，因此 factory 實際解析為既有 `latent` 組合模式；未更改注意力拓樸。
- 正式全域 batch 32、雙卡 DDP、BF16 AMP、敏感 basis contraction FP32、TF32 關閉。
- 不開 sparse-event 模式；磁碟的無損零欄壓縮會還原完整邏輯欄位，不是更換稀疏模型。
- 執行契約沿用選定 v10：50% 容量、`first_minute_only`、收盤不限容量。未擅自改為逐分鐘續單或 Shioaji 模擬器。
- 使用已批准的日 K 研究代理時，維持官方開／收盤價、不加不利 tick、原代理容量公式；這不是歷史 broker fill。
- 原始 panel 仍 NULL；舊訓練 view 用最近已公布值＋可用性＋資料年齡＋更新旗標。最新 value-only 實驗只輸入最近已公布數值，移除三種狀態通道，保留 TTL、NULL 屏障、報告／商品生命週期 reset；未觀測的模型數值使用中性零，不能當成真實原始零值。
- `resume=false`、不讀 pretrained optimizer；新來源、新 view、新 cache、新訓練 output。
- v2 將時間區塊與 compact attention／配置 head 指定為 FP32 精度區塊，保留其餘可用 BF16 AMP；不是將整個訓練偷偷改成 FP32。兩個旗標都進入 checkpoint ABI，不能續接舊精度 optimizer。

首次資料建置程式碼 SHA256：`ea0cbf70ca41b8e20f9d41cf8acbcb95513e8673ecc6fe24cfd732234caf5b24`。

包含新訓練設定的 release receipt SHA256：`4dfcf6a70669816ff8b73d035e16d968c0e9eec789f0f08681d8b991af144f09`；source bundle SHA256：`b45b6563b72cb8150fc3de42a5950e3eaa77d84985a25ad5ccb7170304a8c606`。1258 frozen files、20 YAML 依賴逐檔驗證。這是 exact-source/config acceptance，不宣稱 wheel binary rebuild 或訓練驗收。

目前 v2 程式碼 SHA256：`f5b51ad3f2f569e9314eec824382bc71e61630513f82c959c57374b778b0b008`。release receipt SHA256：`24fccebf43120b6e6db913549044bd0f5b41e016c5c51affdf2c7e996317cb3b`；source ZIP SHA256：`4ae5448a676b1fbbffaa2486c2bd91c40188dcf7e2731c3fb75308de1ebaf6ed`。1,329 檔、22 YAML 依賴已在遠端逐檔驗證，位於 `code-training-precision-v2/`；原 bundle 保留。

## 5. 原 58,886 通道的數值修正與驗收歷史

checkpoint 容量 gate 已通過：參數上界估計約 9.9145 億，單 fold 的 canonical Adam group-last、原子替換、fold best 與 4 GiB 報告預留共需 32,056,480,354 bytes。這是磁碟預算，不是 GPU／CPU 工作空間已驗收。10/05 06:00 UTC 實測約 174.94 GB 磁碟可用、17.21 GB 容器保守記憶體 headroom，主要已用記憶體是 page cache；不把主機 MemAvailable 當容器額度、不宣稱已取得可回收記憶體。

診斷找到 rank1 在 BF16 不同 stock chunk 形狀下，時間區塊最大絕對差約 0.001953；初版僅將此區塊改成 FP32，第一個 AMP step 通過，但第二次 Adam 更新後配置 head 又超門檻，仍被拒絕。v2 加上共享 compact attention／配置 head 的 FP32 區塊後，兩 rank 的六次更新均通過原門檻，最大梯度絕對差 `0.000133514404296875`。沒有降低模型維度、刪 feature、改 batch、交易公式或放寬門檻。

這個 oracle 使用 10 特徵的小型受控來源，不能取代真實 58,886 通道的驗收。失敗的 v1／原候選收據保留，不能續接其 optimizer。

以上是舊 58,886 通道的 source/READY、完整 panel／清冊、`train.py --check-data-only`、磁碟容量與小型雙卡 oracle 歷史。三份 exact 來源已續租成功；新 value-only panel、資料 gate、擴大雙卡六步 oracle、RMS／PCA、完整 fold11 訓練／驗證／測試／同步圖／checkpoint 都已實測通過（最多三輪、依原規則實際兩輪早停）。最新啟動方式與驗收收據見新版報告；完整歷史 PIT 與實盤可用性仍不在驗收範圍。

遠端建置及驗收使用現有 `scripts/agent_workflow.py` 的 tmux 監督，不依賴 SSH 長連線存活。驗收工作先讀指定 builder 的完成狀態，再驗證 manifest 固定 ID 與來源 SHA、重新跑 CUDA strict、完整資料 gate 和 checkpoint disk budget。只有全部退出碼 0 才寫 `research-data-acceptance.json`；該 receipt 仍維持 `training_ready=false`，不把 data-only acceptance 誤報成 BF16、全形狀 DDP 或完整 fold 驗收。所有工作紀錄在上述遠端產物根目錄的 `workflow/runs/`，本文進度是紀錄時點，不是持續自動刷新的畫面。

## 6. 本機證據

- [來源重新驗證](../artifacts/operations/agent-workflow/runs/verify-fixed-gaprepair-source-re-20261004T162758-166a1f9e/run.log)
- [60 項針對性測試](../artifacts/operations/agent-workflow/runs/fixed-native-source-panel-regres-20261004T163211-cf8647ae/run.log)
- [正式發布 receipt](../artifacts/data_quality/tw_feature_expected_gaps_20261004/source-publish-v4.json)
- [完整發布工作](../artifacts/operations/agent-workflow/runs/publish-fixed-gaprepair-source-v-20261004T163057-b694a3ff/run.log)
- [遠端 exact-source 與 CUDA strict](../artifacts/operations/agent-workflow/runs/vast-fixed-consumer-and-cuda-v5-20261004T163527-598c2b0f/run.log)
- [固定新訓練設定的 code release](../artifacts/data_quality/tw_feature_expected_gaps_20261004/code-release-resume-v6/20261004T163959383813Z-ea0cbf70ca41/release.json)
- [完整遠端 hydration log](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-resume-v4-metadata/hydrate-run.log)
- [遠端程式與選定設定驗證](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-resume-v4-metadata/training-code-extraction-acceptance.json)
- [建 panel 前的實際容器容量](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-resume-v4-metadata/node-profile-before-panel.json)
