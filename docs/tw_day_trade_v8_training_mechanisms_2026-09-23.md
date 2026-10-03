# v8 訓練泛化、資金規模與多空方向診斷

這次優先處理「訓練時的可成交問題，是否與驗證、測試相同」，而不是微調 RMS。
舊 12 組消融均保留。新方法是研究候選，工程驗證不代表已提高未見資料報酬。

## 1. 最重要的契約差異：十一年複利帳戶對上一年新本金帳戶

原訓練將 2014–2024 連成一個 TWD 10M 帳戶，每個 epoch 重播整段軌跡；
2025 驗證與 2026 測試則各自從 TWD 10M 開始。模型只輸出比例，沒有接收當下 NAV。
整張交易不具尺度不變性：一張最低權重 `1000 × 開盤價 / NAV`；容量占 NAV 比例也會改變。

這不是純理論上的差別。原 baseline 被選中 epoch 的 train loss 為 `-0.69069`，
learned_cash 為 `-0.66638`；目前目標是 `-252 × mean(log return)`，無額外周轉罰則。
以本次 runtime 的 2,653 個有效訓練日代入 `exp(-loss × N / 252)`，代表該 epoch 訓練軌跡的期末資金
約為起始的 1,400 倍、1,100 倍。這是由訓練 loss 推算的量級，**不是保存 checkpoint
重新重播的精確資金值**。不能把這樣的訓練 loss 與 TWD 10M 單年驗證當成同一難度。

修正候選 `tw_day_trade_v8_annual_training_candidate.yaml`：

- 全部 2014–2024 訓練日期仍計入，每個曆年建立獨立 TWD 10M 訓練帳戶。
- 年內維持 exact FIFO、費稅、整張、容量與複利。年度邊界不可以丟掉持倉、未付權利或非法狀態。
- 共用原 trainer；完整 epoch 才做一次 AdamW 更新，loss 依實際日期數加權。
- validation/test、選擇 checkpoint 的規則、原 1,000 epochs 上限不變。
- 這是刻意改變訓練分布的新契約，使用新 root 與 checkpoint fingerprint；預設關閉。
- 跨年 batch 會切成不同長度，因此這個候選明確啟用既有 `backtest_compile_dynamic`。
  靜態編譯短測在第一個 epoch 超過重編譯上限，失敗紀錄保留在 benchmark 的
  `annual_accounts`；修正版另放 `annual_accounts_dynamic`。

年度帳戶減少資金分布偏移，但不保證單年最適配置能適應所有行情。它也不處理
早期 daily proxy 與後期真實 minute 來源差異。不能把新的 train loss 直接與舊連續帳戶排名。

## 2. 整張量化有真正的梯度死區

現有程式在第一次整張取整有 straight-through estimator，但後續 target/sign/FIFO
以零成交數量決定分支，仍會把不足一張的候選梯度截斷。
因此「整體 gradient norm 非零」不足以證明每個有潛力的候選都有學習訊號。

可重現例子：資金 10M、股價 1,000、最後官方收盤 1,010，使用原費稅與分鐘來源。
權重 `0.001`、`0.099` 均零成交、零報酬、零梯度；`0.1` 才能成交一張並收到梯度。
原 forward 的零成交正確，問題在用它直接學習跨越離散門檻的能力。

已保存 fold test 要求權重的診斷如下。分母是要求 gross，使用該日實際期初 NAV
與 canonical panel 的開盤價估算一張門檻；這不是完整成交拒單歸因，各欄可以重疊。

| 變體 | 要求資金不足一張 | 開盤方向權限不允許 | 要求資金落在 daily proxy |
|---|---:|---:|---:|
| baseline | 19.0% | 6.9% | 9.2% |
| learned_cash_output | 90.4% | 4.6% | 12.1% |
| no_temporal_basis | 55.7% | 9.7% | 4.0% |
| no_pretrained_initialization | 24.6% | 6.1% | 13.1% |

這支持 learned_cash 的 gross 約 58%、實際周轉卻很小的成交門檻解釋；它不是單純選擇大量現金。
權重比例若在膨脹後的訓練帳戶可成交，在測試的 10M 帳戶卻可能大多不足一張。

第二個獨立候選 `tw_day_trade_v8_lot_recovery_candidate.yaml`：

- 訓練仍跑原 exact 帳戶；另外用原 FIFO reduction、同一分鐘機會與費稅，計算獨立一張的報酬。
- 只在不足一張、方向合法、來源完整、入口容量足夠且未使用要求預算能負擔一張時，補入 backward 訊號。
- 不變更實際成交、現金、NAV、loss 數值或 eval。已成交候選仍使用原梯度。
- 使用 `score_entmax_cash_v2`，使全零 score 不會在輸出層再次截斷梯度。
- 此候選包含輸出零點梯度與帳本 sub-lot 訊號兩部分；若要分離兩者的績效貢獻，
  還需額外的「只換 v2 輸出」控制，不能將整包差異全部歸因於帳本修正。
- 這是明確宣告的 surrogate gradient，不是離散帳本的數學導數；預設關閉、獨立 ABI。
  它也無法保證恢復 entmax 支援集合之外的所有候選，或解決全部容量飽和問題。

## 3. 空頭偏向與標籤來源需拆開看

原 baseline 約 84.2% 的要求 gross 在空頭，no_temporal_basis 為 100%。
原資料中，依當日可開空條件篩選的股票，其平均開盤至收盤 log return 為：
訓練約 -15.2 bps/日、驗證 -17.1 bps/日、已檢視測試 -21.6 bps/日。
這是等股票、等日的原始描述統計，未扣費、未套容量，**不是可實現的空頭策略報酬**。
它提供模型普遍偏空的一個資料機制，但不證明有泛化選股 alpha。
按本實驗設定，一般股票在買賣價相同時，折扣後來回佣金加當沖賣出稅約 20.7 bps，
已高於訓練、驗證的上述平均跌幅，還沒計入分鐘入場價格差與容量。因此不能把
「整體盤中偏跌」直接當作「任意做空都會賺錢」。

來源 manifest 還包含 2,986,868 個 daily-proxy symbol-days 和 2,798,943 個 minute
symbol-days。早期代理以官方 OPEN/CLOSE 與日量推估容量；後期實際 minute 有入場價差與
排程限制。這是訓練來源分布的另一差異，不能靠提高資本、放寬容量或更改費稅消除。

依使用者確認，新增純方向消融，均沿用原 99-feature v8、原 loss、22-family basis、
相同初始化、seed、global batch 32、1000 epochs。年度帳戶與梯度候選不混入這兩組。

- `long_only`：模型與交易契約都只能開多，仍可持有現金。
- `short_only`：在 entmax 選擇之前限制合法方向；保留原 signed score，正分數不會被取絕對值變成空單信心。
  沿用原開空權限與費稅，仍可持有現金。模型 checkpoint fingerprint 與雙向、做多各自獨立。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/run_ablation_experiments.py \
  --spec configs/ablations/tw_day_trade_last_last_only_v8_twpublic_248d0869_ofat.yaml \
  --start-fold 11 --max-folds 1 \
  --multi-gpu-strategy distributed_data_parallel --auto-resume --stop-on-fail
```

輸出：`artifacts/ablations/tw_day_trade_last_last_only_v8_twpublic_248d0869_ofat`，
方向變體位於其中的 `long_only/`、`short_only/`。
2026-09-24 已合併為完整 14 組主清單；上方指令會略過已完成的 fold 11。
舊 `tw_day_trade_v8_direction_20260923.yaml` 透過 `base_spec` 繼承同一主清單，
不再產生另一份方向套件；雙 GPU 仍序列執行各組。

## 可重現診斷與驗證邊界

`scripts/audit_day_trade_learning_mechanisms.py` 會核對保存 universe、dates、physical
release，唯讀計算並確認來源 archive SHA-256 未變。結果在
`artifacts/analysis/tw_day_trade_v8_ofat_20260923/learning_mechanisms.json`。

單元驗證包含：方向鏡像、全正分數不開空、空遮罩、BF16/compile/重載；
一張候選與原 exact 帳本（dense/sparse、提前退出/最後收盤）逐值比較；
禁止交易、零容量、停牌、來源缺口、無資金不補梯度；以及跨年 batch 的日期加權 loss
與分年度獨立帳本一致、年度邊界拒絕丟掉持倉。
候選的一張交易還需通過盤中最低 NAV 的保守存活檢查，不能因最後收盤有利而
忽略途中破產。舊 schema 1–3 或缺少 manifest 的 checkpoint 不可續接新的訓練契約，
但模型權重遷移仍可使用原有獨立驗證流程。

完整資料雙 GPU bounded lifecycle 位於 `artifacts/benchmarks/day_trade_direction_20260923`
與 `artifacts/benchmarks/day_trade_learning_20260923`。正式長訓練狀態以各自 launcher log、
progress.json 與 fold_complete.json 為準。bounded 結果不能當成完成 1000 epochs 或獲利驗收。
2025 已被多次選模、2026 已反覆檢視，新增比較都是探索性，最終需其他時間 folds 與後續未見資料驗證。

## 本次驗收與正式執行

- 嚴格 CUDA 環境檢查通過，實跑使用兩張 RTX 5090、BF16 DDP、global batch 32。
- 較廣回歸測試為 402 passed / 3 skipped；最後加入 legacy checkpoint 與邊界檢查後，
  最新聚焦測試為 155 passed。兩批有重疊，不可相加；完整輸出保存在 benchmark root。
- 方向短測：做多因短測的 early-stopping patience 為 1，在第 1 輪後停止；做空完成第 2 輪。
  兩者都完成整個 fold lifecycle、要求權重方向合法、沒有零梯度批次。
  這兩輪短測的 scheduler horizon 不同於正式訓練，不能以其報酬判斷方向優劣。
- 學習機制短測：控制、年度帳戶與 sub-lot 候選各完成 3 輪及完整 fold lifecycle。
  三組皆維持正式的 1,000-step scheduler horizon / 32-step warmup；每輪一次更新、
  loss 有限、非零梯度，沒有 compiled-prep failure。

| 三輪完整資料短測 | 第 1 輪秒數 | 第 2 輪秒數 | 第 3 輪秒數 |
|---|---:|---:|---:|
| 控制 | 74.16 | 66.16 | 75.14 |
| 年度帳戶、動態編譯 | 142.59 | 71.82 | 62.84 |
| sub-lot 梯度候選 | 98.31 | 88.46 | 87.95 |

數字是 `epoch_curve.jsonl` 記錄的 `epoch_total_s`，第一輪含編譯。
這是候選的可運作性與成本觀察，**不是最大 rank 公平加速測試或策略績效驗收**。
sub-lot 候選的第一輪 train loss 與控制同為 `-0.009576600044965744`，
而 gradient norm 由 `12.7963` 改為 `16.5252`，符合 forward 不變、backward 改變的設計。
逐值 forward 保真仍以單元測試的 canonical 帳本對照為準，不以單一彙總 loss 取代。

完整驗收收據：

- `artifacts/benchmarks/day_trade_learning_20260923/training_acceptance.json`
- `artifacts/benchmarks/day_trade_direction_20260923/acceptance.json`
- `artifacts/benchmarks/day_trade_learning_20260923/original_artifacts_preserved.json`：原 107 個受核對檔案未改變。

正式方向實驗曾於 `2026-09-23 15:33 UTC` 啟動（runner PID `2193701`），
隨後依使用者「給我指令我自己跑」的要求停止，已確認該 runner 與子程序全部退出。
之後使用者自行執行，兩組 fold 11 均已完成：只多共 100 輪、只空共 218 輪。
2026-09-24 依要求將完整變體目錄搬入原 OFAT root，合併主清單、摘要與比較圖；
checkpoint、曲線與逐組產物保留原始位元組。原方向 root 下的兩個變體路徑
保留相容 symlink，原啟動收據與兩組歷史摘要仍留在舊 root。
使用上方主清單指令可手動續跑，保留 1,000 epochs 上限及原 100-epoch 無改善停止規則。
`launch_receipt.json` 記錄原啟動命令、Git HEAD 與程式雜湊，
`manual_handoff.json` 記錄停止原因。重新執行後的即時狀態請查
各變體 `progress.json` 與 `epoch_curve.jsonl`。合併稽核與舊主摘要備份位於
`artifacts/analysis/tw_day_trade_v8_direction_merge_20260924`；本次合併未啟動訓練。
合併前後共核對 869 個逐組檔案（約 2.81 GB），內容雜湊與 inode 均未變；
三種比較圖表都含 14 組，熱圖的橫軸改用實際 fold ID，正確顯示 fold 11。
年度帳戶與 sub-lot 的正式長跑設定已準備好，**尚未啟動，也尚未升格為改善報酬的新基準**。

## 2026-09-24 繼續研究：先讓因果可辨識，再跑長訓練

上表尚不能回答「年度帳戶」或「sub-lot」是否有用，因為兩列各自捆綁了兩個以上的改變：

| 候選 | 同時改變的機制 | 三輪結果可回答什麼 |
|---|---|---|
| 年度帳戶 | 資金狀態分布 + calendar boundary + dynamic compile | 只能證明捆綁後可執行，無法把 loss 或時間差歸因於年度 reset |
| sub-lot | `score_entmax_cash_v2` 的零點導數 + 帳本 sub-lot surrogate | 只能證明 forward 不變但總梯度改變，無法分辨是輸出層還是帳本貢獻 |

從底層數學看，模型要求比例 `w` 在資金 `N`、價格 `P`、整張 `L=1000` 下的目標股數為：

```text
q(w, N, P) = L × floor(|w| N / (L P))
```

若可交易分數股、所有費用與容量均對 `N` 線性縮放，則單日比例報酬與資金尺度無關，
`sum(log(1+r_t))` 將在跨年連續帳戶與每年重置帳戶之間等價。這裡不等價的來源不是一般化的
「每年重置比較穩定」，而是 `floor`、費用四捨五入、張數容量、margin/FIFO 狀態與 default gate
對絕對資金尺度的非線性。因此年度 reset 是在測試「訓練的 NAV 分布是否對齊單年 TWD 10M
驗證帳戶」，不是一個無條件比較好的 regularizer。

sub-lot 問題則是離散映射在 `|w|N < LP` 的局部常數區間，真實導數幾乎處處為零。
任何能跨過這個門檻的 backward 訊號都是 gradient estimator，而不是離散帳本的真導數。
[Bengio et al. 的 straight-through estimator](https://arxiv.org/abs/1308.3432) 也明確把這類做法定義為啟發式估計。
這也是本候選必須同時保留 exact forward，並單獨驗證 backward 的原因。

`entmax` 本身的稀疏性也不等於整張可成交性。
[sparsemax](https://proceedings.mlr.press/v48/martins16.html) 與
[α-entmax](https://aclanthology.org/P19-1146/) 建立的是 simplex 上的稀疏輸出與 Jacobian；
本專案還有有符號多空方向、現金、整張、分鐘容量與實體帳本。所以輸出層導數 `B` 與帳本 estimator
`C` 必須分開實驗。

### 可辨識的實驗矩陣

新規格 `configs/ablations/tw_day_trade_v8_learning_mechanisms_factorial_20260924.yaml`
生成 9 組獨立 artifact roots：

- `baseline`：當前連續帳戶、v1 輸出、無 sub-lot。
- `continuous_dynamic_compile_control`：只切 dynamic compile，隔離編譯模式。
- `output_v2_only`：只改零分數的 backward derivative；forward 動作不變。
- `sub_lot_only_v1`：保留 v1 輸出，只改帳本 surrogate。
- `output_v2_sub_lot`：原先捆綁候選，可與 `output_v2_only` 比較 `C | B=1`。
- `annual_only_v1`、`annual_output_v2`、`annual_sub_lot_v1`、`annual_output_v2_sub_lot`：
  估計 `A`、`A×B`、`A×C`、`A×B×C` 交互作用。

這不是用一個黑盒列表找最高報酬。對任一相同訓練區間，例如 sub-lot 在 v2 輸出下的邊際效果是
`L(B+C)-L(B)`，而不是 `L(B+C)-L(0)`。年度帳戶也應先與 dynamic-compile control 比較，再談 `A`。
九組設定的 dry-run 已全部通過；因 2025/2026 已被查看，結果只能視為 exploratory evidence。

### sub-lot 計算的等價優化

原實作在每個 epoch、每個 batch 重新跑一次「獨立一張 FIFO 成交結果」。但該結果只依賴已驗證的
physical sessions 與費稅率，不依賴模型參數。最終實作改為：

1. 每個 source release、device、symbol subset、fee identity 只計算一次每日 one-lot long/short PnL 與 source-valid label。
2. 快取粒度是「每個交易日」，不是整個 batch；擴展 walk-forward fold 可共用相同日期，不會每個 fold 複製大量 GPU tensor。
3. 每次 forward 仍重算當下權重的 sub-lot mask、方向權限、NAV/funding、盤中償付能力、account-flat 與 default gate。
4. 快取是 process-local，不寫入 checkpoint，也不改 checkpoint ABI；authoritative exact forward 完全不變。

最終雙 RTX 5090、BF16 DDP、global batch 32、fold 11、正式 1,000-step scheduler horizon 的
bounded 結果如下。數值為 rank 0 `epoch_curve.jsonl` 的同口徑；舊基準沒有保留可用的每輪 maximum-rank
收據，因此這些數字只能支持候選優化，尚不是最終的硬體基準升格。

| epoch | 舊 sub-lot 總秒數 | 每日快取總秒數 | 舊 label/mask 秒數 | 每日快取 label/mask 秒數 |
|---:|---:|---:|---:|---:|
| 1（編譯冷啟動） | 98.31 | 100.84 | 25.38 | 2.71 |
| 2 | 88.46 | 66.78 | 24.90 | 2.61 |
| 3 | 87.95 | 69.06 | 24.85 | 2.69 |
| 4 | — | 70.78 | — | 2.81 |

相同 horizon 的 epoch 2–3 平均由 `88.20s` 降至 `67.92s`，wall time 減少 `23.0%`，等價處理量提高約
`29.9%`；label/mask 熱點時間減少 `89.3%`。epoch 3 的 train valid rows/s 由 `32.2` 升至 `42.0`。
最重要的保真證據是前 3 輪的 `train_loss`、`val_mean`、`test_mean`、learning rate 與 clip 前 gradient norm
全部 Python 浮點值相等，不只是大約接近。

第一次快取短跑 `artifacts/benchmarks/day_trade_learning_cache_20260924/output_v2_sub_lot`
曾用 CLI `--epochs 4` 間接把 scheduler horizon 改成 4，它的報酬與 epoch 2 後軌跡全部作廢，只保留作為負面案例。
正確候選 config 明確固定 `lr_scheduler_t_max: 1000`：

- `configs/deployments/tw_day_trade_v8_lot_recovery_cached_profile_candidate.yaml`
- `artifacts/benchmarks/day_trade_learning_cache_per_day_final_20260924`

`progress.json` 與 `fold_complete.json` 皆已完成，最終 test 範圍是 2026-01-02 至 2026-09-11、168 rows。
這只是四輪工程 lifecycle；2025 與 2026 已在每輪查看，其報酬不可用來宣稱泛化改善。

緩存與 exact 帳本的焦點回歸為 `248 passed / 3 skipped`，包含不同門檻權重的 cached/uncached
forward、shadow loss、gradient 逐值相等，live permission gate，以及連續兩個 epoch 的 cache reuse。

### 實際訓練採單一組合版

使用者於 2026-09-24 選擇不執行 9 組分離實驗，而是把年度獨立帳戶、
`score_entmax_cash_v2`、sub-lot surrogate、dynamic compile 與每日 label cache 放入同一次一般訓練。
可辨識矩陣保留為研究設計記錄，不是訓練前置作業。實際設定為
`configs/deployments/tw_day_trade_v8_combined_annual_output_v2_sub_lot_fold11_v1.yaml`：

- fold 11：2014–2024 訓練、2025 validation、2026 test。
- 1,000 epochs 上限，100 epochs 無 validation 改善時早停。
- scheduler horizon 明確固定 1,000，不受短測 `--epochs` 覆寫。
- 全新 artifact root，可從同一組合版 checkpoint 安全 resume，不讀舊輸出契約的 optimizer state。
- `--check-data-only` 已通過：3,096 sessions、2,754 symbols、99 features、fold 11 與 exact physical-source SHA-256 均已驗證，未啟動模型或 checkpoint。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_v8_combined_annual_output_v2_sub_lot_fold11_v1.yaml \
  --start-fold 11 --max-folds 1 \
  --multi-gpu-strategy distributed_data_parallel
```

這個選擇可直接產生一個策略候選，但單一組合結果無法辨識哪個子機制貢獻改善。
2025/2026 已被查看，訓練完成後的結果仍是 exploratory；正式升格需後續未見時間。
