# 台股 Nullable Panel、共同／個別特徵與遠端訓練驗收

## 1. 執行進度

2026-10-04 12:48 UTC：`prepared-v5` 的共同 NULL 語意增量修復、4,666 頁逐項報告與 code-v13 全 11 folds 執行 data gate 都已通過。**正式雙卡完整 fold 尚未驗收：磁碟不足與 BF16 compiled oracle 未通過，不宣稱可直接正式訓練**。

- 使用者決定：原始未觀測座標維持 NULL；訓練使用最近已公布值、可用性、日曆日年齡與更新旗標。不是填造原始觀測，也不是補成交價。
- 固定新版私人來源：`tw-daytrade-panel-sources-20261004-v3-20261004T095518325247256Z-l0-penguin-695ffc0c14ffe3e0`。
- 發布 manifest SHA256：`aa1369864f3e69a47009b480aa4806f8b0bdb11a2237a165929501d8e25a7432`。
- 清理後來源 4,731,824,483 bytes：2,757 股票來源、553 原有／新接線規格（508 FinLab）、54 季原生 IFRS 財報，以及 FinMind 財報、台灣總經與其他共同來源。來源規格數、科目數、表示數及最終模型維度必須分開計數。
- source v3 完整整理約 97 秒；原 v2 被停止並保留不完整目錄，不能據此聲稱已測得完整流程加速倍數。82 個 required objects 已完成 exact hydrate／SHA／READY／lease 驗收。
- 最新聚焦 factorized／模型／PCA／checkpoint／精度回歸 193 項通過（34.22 秒）；清冊／native／capacity 組 66 項（9.19 秒）、執行來源／增量發布組 25 項（9.03 秒）、傳輸組 20 項亦通過。範圍重疊，不相加；不是全 repo suite 或完整 fold 驗收。
- 最新 exact code-v13，source SHA256 `6c71052a6611c0df93d66a8c0fb35ec57845ec5d07caef36d7776e257d89f297`。所有 frozen source/config members 已在新根驗證；strict CUDA 退出碼 0。實際 Torch `2.11.0+cu128`、Numba `0.64.0`，不能套用舊 CUDA13 環境觀察。
- `prepared-v1/v2/v3` partial 全部保留。code-v6 已完成 `prepared-v4` 數值 view，manifest `status=complete, feature_view_ready=true, training_ready=false`。原 builder 最後因 native MOPS 清冊缺頂層 `scope` 而退出 1；數值檔未損壞。修正報告從 canonical rule 解析 scope 並拒絕矛盾，不改 immutable dictionary／manifest；另一次報告工作已成功產出。
- 最終 manifest：3,109 日期（2014-01-06～2026-10-02）、2,757 實體、14,655 個別經濟量、65 共同量、58,886 模型通道。union training cutoff `2024-12-31` 前有 281,538,843 筆非空原始觀測。修復後 `prepared-v5` manifest SHA256 `3d432f93a4edbde179a64aa9cc4af17b9a5c7ec975675d3dfc5c3390350861e4`；dictionary SHA256 `d17b7cb993fd5e4fdf9df6a9c49f07fce0318b30594135e68e07db388e1ffcf2`。immutable v4 與其原 fingerprint 仍保留。
- 個別數值區塊 17,223,425,411 bytes，192 日期區塊 × 22 股票區塊，保留全部邏輯通道與 Float32 位元。數值建置 2,649.10 秒；其中來源觀測 372.81 秒、canonical 基礎 panel 35.697 秒。原 builder 報告階段失敗，故**沒有成功的完整 preparation-profile 時間**；這些數字不冒充來源到完整報告的吞吐量或加速倍數。
- [完整入選、合併與隔離 Markdown 清冊](../artifacts/data_quality/tw_day_trade_factorized_panel_20261004_v1/metadata-v13/feature_report/index.md) 已產出並取回。新報告 `tw_nullable_factorized_feature_report_v3_named_coverage` 修復匿名標題並附逐項可用座標計數，4,666 頁、4,665 索引連結皆存在、零空標題；metadata archive SHA256 `ae3bab8f2281b9d63cb7cb5e4fd7c13202fc9bb1c10cc096d777f5f2b82e9377`。179 alias、451,548 筆排除紀錄是 context／表示紀錄，**不是 451,548 個不同 feature**。每項保留來源、單位、時鐘、適用粒度與排除原因。
- 主 BF16 實驗在選定 factory（lookback32、d_model32、globalbatch32、實際 head）工程 oracle 仍拒絕 1／48 權重，最大絕對差 0.000221464，未放寬誤差。TF32 高精度模式亦未通過 FP32 gate，因此修正 runtime 確實尊重 `use_tensor_cores:false`；不關閉 BF16 的 Tensor Core kernels。獨立 **FP32 診斷配置**通過雙 rank 各三次 optimizer step，最大梯度差 1.78814e-6；fixture 只有 10 channels／3 stocks，不能代替 58,886 channels 完整 fold，更不自動改正式精度。
- 首次完整 data gate 發現 v3 發布器漏列 TWSE／TPEX 日價格與換股 reference／summary 四檔，已補發**增量私人執行 companion**。Syncthing 7 required objects、11 materialized files、SHA／READY／租約已通過；code-v12 全 11 folds data gate **退出碼 0**。1,606 分鐘 session 的執行快取建置 733.8 秒；含來源驗證／panel／fold 的入口累計 765.386 秒。未建立模型、optimizer 或 fold-completion marker。
- 逐項 availability 檢查發現既有公開月／季頻共同欄位展開日曆後，把「當天沒有公布」的 NULL 當成新空發布，錯誤清除 carried state。規則的月／季頻 TTL 本來正確，根因是**座標缺值與發布事件混淆**。已新增 scoped `public_market_unobserved_coordinate_v2`，僅對有 canonical 來源／時鐘／低頻規則證明的共享欄生效；native FinLab／MOPS／FinMind／FRED 真實 NULL 仍是屏障。49 聚焦組及 182 相關回歸通過（範圍重疊）；`prepared-v5` 成功，包含固定來源 SHA 驗證與完整報告的增量工作耗時 481.46 秒。保留所有原始 NULL，重用 4,735 個 hard-linked members；未修改個別股票區塊，未受影響共同欄位逐位元一致。
- 本次新執行 cache 全 1,609 檔 SHA／READY 驗證、無程序引用、持有 owner lock 後，以同檔案系統原子 rename 搬到 `artifacts/markets/.../runtime-cache/physical-source/physical-e4ebe...`。沒有複製 11.48 GB 或刪除資料，也未碰共享 cache 的其他 digest。code-v13 已以新 root 重新完整 SHA 驗證，cache 驗證耗時 8.3 秒，沒有信任舊 root 的 run-verification receipt 或重建分鐘 cache。全入口累計 126.568 秒，包含所有特徵／來源與 11 folds 檢查；首次冷建置為 765.386 秒。兩次 feature manifest 不同，這是 cold／cache reuse 工作流觀察，不是純演算法或 epoch 加速倍數。
- code-v13 容量 audit 所需空閒最低估計 32,056,480,354 bytes（29.85 GiB），檢查時只有 10,022,735,872 bytes（9.33 GiB）；data gate 完成後再查只剩 8,992,624,640 bytes（8.38 GiB），不足 21.48 GiB，**容量未通過，不啟動完整 fold**。所選無瓶頸、22 基底 family 模型的參數上限估計為 991,450,638；預算包含 FP32 參數、Adam moments、原子更新時舊／新 group checkpoint 與 fold-best，以及報告保留空間，不包含跳過 checkpoint 的捷徑。本次已盤點 partial 可重建內容，但僅清理這些仍不足最低門檻；沒有刪除特徵、舊模型、來源或正在使用的 `prepared-v3/base_panel_cache`。
- 遠端新根：`/root/stockAgent/artifacts/markets/tw_day_trade_factorized_panel_20261004_v1`，不修改舊模型、服務或既有訓練輸出。

相關 supervised run 證據在 `artifacts/operations/agent-workflow/runs/`：`publish-native-panel-sources-20261004T094247-8513a77d`、`freeze-factorized-code-v2-20261004T100757-83c1f2fd`、`remote-factorized-environment-20261004T101002-b2b85cfd`。

## 2. Panel 的形狀與 NULL 的意義

邏輯輸入為 `[交易日, 可交易實體, 模型通道]`；日期與實體由原有執行 panel 決定。上市、下市、停牌、當沖資格與可買／可賣遮罩仍歸原有執行模組，不用某項財報是否有值判斷能否交易。

| 層 | 實體粒度 | 實際儲存／行為 |
| --- | --- | --- |
| 個別原始觀測 | ready session × symbol × quantity × 原始期間 | nullable Parquet 座標表；未出現的座標等同 NULL；保留原始來源與憑證 |
| 共同原始觀測 | ready session × quantity × 原始期間 | 市場值只存一次，`symbol="*"` 不是交易目標 |
| 個別訓練 view | date × symbol × quantity | value、available、age_days、updated；lossless Float32 分日期／股票區塊 |
| 共同訓練 view | date × quantity | `common.npy` 一份；僅在有界模型 slab 中廣播 |
| 執行／報酬資料 | canonical 執行 panel | 不從訓練的 carried value 產生成交價或企業行動條款 |

每項經濟數量有四通道。真實公布的 0 是 `value=0, available=1`；缺值數值哨兵則 `value=0, available=0`，兩者不可混淆。未公布、商品不適用、原始 NULL、未知單位與來源壞檔是不同狀態，不一律判為下載失敗。

訓練延續有界：日資料不跨缺日延續；月頻 62 日、季頻 200 日；TDCC 依 2015 年頻率切換使用既有月／週規則。新報告中的 NULL 為屏障，MOPS／FinMind 新報告清除同群組未披露科目；股票代碼生命週期更換清除前身狀態。晚到舊季報保留在原始表，不能覆蓋已知較新報告。

`prepared-v5` 的增量修復只重建共同 state 與清冊／fingerprint；17,223,425,411 bytes 個別區塊以驗證過的 hard link 重用，並對未受影響的共同欄逐位元比較。原始 observation／annual event 的 NULL 仍完整保留；calendar padding 僅在訓練事件流中不算新發布，不從值是否改變判定公布。

下表是修復前後訓練 view 的可用日期數，不是原始公布筆數，也不是補造每日觀測。全軸 3,109 日；最後一日因既有 lag-one 儲存契約沒有可存放的下一 session，不造新資料。

| 共用量 | 修復前 | 修復後 |
| --- | ---: | ---: |
| 外匯存底變化／美元十億元原值（各一欄） | 154 | 3,108 |
| M1B／M2 年增率（各一欄） | 155 | 3,108 |
| CPI 年增率 | 152 | 3,108 |
| GDP 年增率 | 102 | 3,108 |
| 失業率原始百分比 | 153 | 3,108 |
| 失業率原有系列 | 154 | 3,108 |

模型讀取 canonical lag-one 欄位時，ready session `t` 的狀態存於 `t-1`；這是原有 `next_session_open_gap_logret` 同一儲存 ABI，不是再增加一次安全延遲。最後已完成日不臆造下一個交易日觀測。已有 native `available_at` 的來源按首次可用的台北 09:00 決策對齊，不重複套用來源已做的安全延遲。

## 3. 同義合併、單位與未接線項目

- 官方 taxonomy 10 份 ZIP、62,403 QName alias、12,431 schema／label semantic key 已取到；固定 dictionary SHA256 `31cb7e0cda66d34b28a0fc3a068e27a2e945b68f914eae66c0691dd33c602629`。
- QName 只在同官方 family、元素名、schema 定義與標準標籤證據一致時合併。未知命名空間、不同單位、合併／個別財報、單季／累計與維度 context 都保留區別。尚未完整解析 reference／calculation linkbase，因此不聲稱所有同名科目均有完整會計等價證明。[官方 taxonomy](https://mopsov.twse.com.tw/mops/web/t203sb03)、[TWSE XBRL 標準說明](https://wwwc.twse.com.tw/staticFiles/listed/manual/XBRL_15.pdf)。
- FinLab revised 與原始期間表只有同 provider 科目、原始期間值驗證通過才作 missing-only 聯集；已存在非空值不被覆寫。FinMind 亦只使用既有明確科目／單位／累計轉單季映射，再驗證至少 100 重疊點與 99.5% 一致性，不用數值反推匯率或縮放。
- 數字 ID、代號、SHA、路徑、receipt、日期鍵與來源管理資訊只存證據，不進數值訊號。
- BEA／Census 已保存來源，但各原生 program 公布時鐘尚未完成接線；不能把 period end 加一天當作發布日。房屋相關用途仍排除。TEJ 目錄、preview 與未證明歷史日期／授權的觀測不混入正式輸入。
- 2026 才首次可用、在任何 canonical fold 訓練所有權內均無觀測的系列，不造出 2014 歷史，也不建立大量只能為零的訓練輸入；保留來源與逐項原因，之後可在有訓練歷史的新實驗重新納入。
- FRED `WALCL`／`NFCI` 按官方週頻使用 bounded carry，其餘已收系列保留日頻規則。[H.4.1](https://www.federalreserve.gov/releases/h41/default.htm)、[NFCI 更新時程](https://www.chicagofed.org/research/data/nfci/current-data-aws)。

全來源 160,610 筆候選表示與逐項分類另見 [既有完整清冊](tw_daytrade_meaningful_features_2026-10-04.md)。該數量不是獨立模型維度；本次 view 的全部入選／合併／隔離明細在[固定 manifest 的逐項分冊](../artifacts/data_quality/tw_day_trade_factorized_panel_20261004_v1/metadata-v13/feature_report/index.md)，並綁定 manifest、dictionary 與 coverage hash。

## 4. 訓練相容性與效能機制

沿用 canonical `build_panel → CrossSectionalDataset → WindowedSplitTensors → FinancialTransformer → trainer／loss／backtest／checkpoint`。不另寫 trainer、報酬公式或改成 sparse-event 執行模式。

- `FactorizedPanelFeatures` 拒絕整包 `np.asarray`；共同值一次儲存，個別區塊只解壓所需日期與股票。
- 個別 block 可省略**所有位元都是正零**的欄，NPZ＋ZSTD 同時保存剩餘 Float32 值與欄座標；負零亦完整保留。讀入模型前還原全部邏輯通道，這不是特徵裁切、稀疏模型或 sparse-event 成交。入選經濟量與四通道形狀不改變。
- code-v6 在狀態 sweep 中直接產生帶邏輯欄座標的區塊；只有該股票區塊已觀測過的量需要評估 TTL／年齡，不先建立大量全零日矩陣再刪零欄。32 日期上限與實際 buffer byte budget 共同控制記憶體；與舊 dense sweep 在 NULL、負零、報告屏障、生命週期及最終 available count 的位元還原 oracle 通過。重用價格／執行 cache 仍由 canonical fingerprint 閘門驗證，不重用未驗收的特徵 view。
- RMS／PCA 的統計 tile 僅跳過證明全零的欄；RMS 保留完整 alive-cell 分母、逐日非零旗標跨股票 OR，PCA 保留原邏輯 observation count 與 owned-date／halo 契約。輸入位元還原、RMS、PCA（包括 80 日／131 股票、跨 32 日期 tile 與 128 股票 tile）均有 dense oracle 回歸。
- 股票／時間編碼在原架構的獨立部分分段，最後仍交給同一市場 attention、score allocation 與 recurrent loss；沒有刪除特徵、恢復 24 維瓶頸或改 attention 設計。
- raw temporal basis 的共用 effective kernel 每次 forward 計算一次，股票區塊共用；non-reentrant checkpoint 在 backward 重讀對應資料，不保留整包寬特徵 GPU activation。
- 新實驗明確設定 `temporal_basis_fp32_contraction=true`，共用基底及最終 basis projection 維持 FP32；其餘網路仍使用選定 BF16 AMP。預設 false 的舊配置及 checkpoint fingerprint 不變；true 進入新模型契約，不能續接舊 optimizer。這是明示的精度契約，不宣稱與舊 BF16 訓練位元相同。
- AOT backward 的 `off` 對 math partition 的 lazy compile 與 invocation 同時生效；canonical factorized 訓練 worker 也讓 compiled loss 使用同一 backward 假設，不將 forward/loss 的 AMP 延伸到 backward。
- code-v10 讓三個 math partition 明確傳入 `emulate_precision_casts:true`，保留每個 BF16／FP32 operation 的宣告捨入；對應新 precision fingerprint `fp32_basis_operation_dtype_v2`。它解決較小 fixture 的 fusion 差異，但選定完整 lookback 的 BF16 oracle 尚未通過，不把這一選項視為充分證明。
- code-v9 對 FP32 raw-basis 投影按 lag 作 batched GEMM：rolling slab 只是共享原始儲存的 view，不先將每筆窗口重複為 `batch × lookback` 大矩陣。CPU 輸出／梯度與原 einsum oracle 通過；全形狀 CUDA、TF32 及完整 fold 數值／時間仍須驗收。
- eager I/O 邊界與內部 compiled math 分離；這個 partitioned 編譯路徑仍需真實 CUDA／DDP 實測，不能以 CPU 一致性測試宣稱全部已 compiled。
- 訓練 RMS 和 PCA/KLT 都只取 owned training windows；分段累計既有協方差，不把 validation／test 加入 fit。FP32 永久特徵儲存、FP64 金融／統計敏感 reduction 不改成永久 BF16。
- schema 原先每個 FinLab 欄重建一次，已改為每檔一次。完整 source v3 97 秒是建置觀察值，不是 DDP epoch 時間。
- 原生觀測按年度串流整理，避免全歷史長表一起進入記憶體。新增 `preparation_profile.json` 記錄包含來源驗證、執行 panel、觀測、nullable view、全部 Markdown 報告的總時間；manifest 內建置秒數不冒充完整報告工作流。
- 小陣列 Numba 使用同一 scalar bytecode 的獨立序列編譯 cache identity；Numba 的原 cache key 不包含 `parallel=True`，不可讓兩個 dispatcher 共用 qualname。遠端 3,109 筆、53 threads 的 shift 中位數為 parallel 26.34 μs、serial 9.06 μs；log ratio 為 26.46／16.91 μs。16 threads 的 log ratio 反而是 parallel 較快（11.52／16.81 μs），所以不是所有硬體／kernel 通用的最佳解；完整 workflow 仍待量測。
- 保留每 epoch 曲線／測試／同步繪圖與所有 checkpoint；測速要包含完整 workflow，不以跳過工作縮短耗時。

本次依前輪特徵工作所選 **v10 research143 no_bottleneck** 作為 base。當前解析值是 market_token、last/last_only、d_model=32、22 個 temporal basis family、global batch=32，並非自動換成 Full-Then-Last。執行亦沿用此 base：`tw_day_trade_entry_remainder_policy=first_minute_only`，**本報告不宣稱它已等同後來網頁的逐分鐘續單規則**。新增模型通道／時鐘／儲存 ABI 使用新 artifact root，禁止續接舊 checkpoint／optimizer。

## 5. Syncthing 與程式驗收問題

新來源發布成功後，首次 hydrate 仍缺 36 物件。來源端 `.stignore` 的新 exact exception 被放在 `#include .stignore-edge` 後面，前面的舊排除規則已先匹配，因而可能兩端報 idle／100% 卻未真正持有 required objects。修正共享 `manage_packed_transport.py` 的順序，exact block 在舊 include 之前，保留其他規則、既有租約、SHA 驗證與原始物件。20 項傳輸回歸通過。[Syncthing 官方 first-match 規則](https://docs.syncthing.net/users/ignoring.html)。

首次 hydrate 600 秒因等待舊規則超時；同一 exact snapshot 的可恢復重試已退出碼 0，82 個物件、materialized READY 與 lease 均成立。不換成 latest、不透過 SSH 傳來源資料。

執行 companion 是 canonical consumer 真正需要的來源，不是模型數值 view：

- snapshot：`tw-daytrade-physical-20261004-v1-20261004T121443923061282Z-l0-penguin-f34b9ce2dc82eb29`。
- packed manifest SHA256：`b671793a6679d7498c9c94369dc51e6656b98b21d460df39be5df55f87e0a7d4`。
- 原 v3 feature／企業行動成員維持精確原 bytes；增加現有官方價格／換股檔，不補造條款或價格。
- `hydrate-factorized-physical-20261004T121750-49deb88f` 退出碼 0；7 required objects 共 1,658,777,252 bytes，materialized／READY／full-verification lease 成立，沒有刪除任何 payload。
- execution root 已固定到新 snapshot，數值模型 view 仍固定 `prepared-v4`；新 output `training-operation-dtype-v3`、FP32 診斷 output `training-fp32-oracle-v2`，不續接舊 optimizer。
- code-v13 改固定 `prepared-v5` 的共同 NULL 語意修正版，output 改成 `training-operation-dtype-v4`／獨立 `training-fp32-oracle-v3`；都不續接上述原 output。
- transport 手動 reconcile 曾退出 75（既有 owner lock），沒有繞過鎖或中斷排程；後續 hydrate 已以實際物件證明通過，而不是只看 idle／100%。

程式碼可經 SSH 傳 exact release，來源資料仍走 Syncthing。遠端重建 wheel 與本機 wheel 只有 setuptools Generator（81.0.0／78.1.0）及連動 RECORD 不同；不假稱 binary wheel acceptance 通過，也不升級遠端環境。改使用 canonical `extract_recorded_sources` 驗證所有 frozen source/config hashes；嚴格 CUDA 環境檢查退出碼 0。實測 Torch CUDA 是 12.8，不能套用舊筆記的 CUDA13 假設。

## 6. 尚待完成的真實驗收

1. 數值 view 與完整語意清冊已完成；容量 audit 尚未通過，仍須實際 host／GPU／checkpoint 預算驗收。
2. code-v13、prepared-v5 及新 cache root 的嚴格 data gate 已通過全部 11 folds；尚未測得完整 fold 的 RMS／PCA fitting、VRAM、訓練或繪圖／checkpoint workflow。
3. 保持原誤差門檻，追查 BF16 數值差異；獨立 FP32 診斷分支已通過選定 factory 的小型雙卡三步，但不自動升為正式設定。[PyTorch 官方 backward 語意](https://docs.pytorch.org/docs/main/user_guide/torch_compiler/torch.compiler_backward.html)。
4. 雙卡 DDP fold11 至少三 epoch，包含 validation、test、曲線、同步圖與 checkpoints；測 cold compile、穩態最大 rank wall、RSS／VRAM、實際 source cache 與完整 fold wall。
5. 對原數學路徑逐項比對輸出、梯度、財務 metric 與產物 fingerprint；分開報告工程驗收、研究結果與未證明的來源歷史。

在以上結果出現前，`training_ready=false` 不能被來源發布或測試數量自行改成 true。

### 證據定位

| 驗收項 | Supervised run／固定檔案 | 結果 |
| --- | --- | --- |
| 真實數值 view | `prepare-remote-factorized-compac-20261004T111929-d659decc` | 數值 manifest 完成；末段報告 KeyError 使原 run 退出 1 |
| 清冊恢復與容量 | `factorized-report-and-capacity-20261004T120724-f523b02d` | 4,666 MD 完成；容量不足使整體退出 2 |
| BF16 selected factory | `factorized-cuda-factory-l32-b32-20261004T120145-e69d3e1e` | 原誤差門檻拒絕，兩個 rank 一致退出 |
| FP32 selected factory | `factorized-cuda-factory-fp32-20261004T120802-44c813ba` | 小型雙卡三步 oracle 通過；不是完整 fold |
| 補發執行來源 | `publish-factorized-physical-20261004T121248-60b0dac1` | 退出碼 0 |
| 新執行來源遠端交付 | `hydrate-factorized-physical-20261004T121750-49deb88f` | Syncthing／物件 SHA／租約通過 |
| exact code-v12 | `freeze-factorized-code-v12-20261004T121806-afb15ac0` | source SHA 固定，遠端逐檔驗證 |
| CUDA v12 | `remote-factorized-env-v12-20261004T121907-30d63f6e` | strict 退出碼 0 |
| 全量 data gate v12 | `factorized-full-data-gate-v12-20261004T122024-0318da2a` | 退出碼 0；11 folds；未啟動訓練 |
| Capacity v12 | `factorized-capacity-v12-20261004T122025-811ac9ef`／遠端 `capacity-audit-v12.json` | 未通過，未啟動完整 fold |
| 個別資料零重建的共同修復 | `repair-factorized-common-v13-20261004T123420-25ff1d71` | 退出碼 0；481.46 秒，原始 NULL 不變／未受影響共用欄位位元一致 |
| Cache root 整理 | `relocate-factorized-physical-cac-20261004T123422-67c5dda5` | 全成員 SHA／无引用後原子 rename；零刪除 |
| Capacity v13 | `factorized-capacity-v13-20261004T124251-eae9ceab`／[receipt](../artifacts/data_quality/tw_day_trade_factorized_panel_20261004_v1/metadata-v13/capacity-audit-v13.json) | 退出碼 2；檢查時 9.33 GiB，至少需 29.85 GiB；後續 free 降到 8.38 GiB |
| 全量 data gate v13 | `factorized-data-gate-v13-20261004T124251-756d6dee` | 退出碼 0；58,886 channels／11 folds，126.568 秒；新 root full SHA 驗證 |

### 執行資料 gate 的範圍與已知缺口

- 真實分鐘價格：2,798,943 個股票日期。
- 已批准日 K 代理：3,016,858 個股票日期，其中 2,526,587 位於分鐘資料起點之前、490,271 位於其後。價格用官方開／收盤、無不利 tick；容量仍採原 50%×日量÷271。這不等同真實 09:01 成交。
- 官方無一般成交：78,317 個股票日期，進出容量為 0；估值採前次可觀測市價，不捏造成交。
- 換股事件目錄 609 筆；185 筆有 exact mapping，414 筆仍未解析。使用空初始狀態前綴遮罩（452,047 個股票日期），不猜換股條款。
- 8,096 個未分類內部估值缺口涉及 355 實體；前綴取得遮罩 580,997 個股票日期。未解析行動／缺口合併後 584,151 股票日期；各計數重疊，不能相加當成不同缺值總數。
- 嚴格 gate 的 `source_gap_symbol_days=0` 是**批准代理與安全遮罩之後**的執行一致性，不代表原始歷史分鐘／企業行動資料全部完整。
