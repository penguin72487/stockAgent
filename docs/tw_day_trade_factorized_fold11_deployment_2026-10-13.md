# Factorized values／無基底／scale-separated cash fold11 上架前置驗收

## 範圍與目前狀態

使用者指定：
`artifacts/markets/tw_day_trade_factorized_values_20261006_no_basis_flat_bf16_tf32_b128_scale_separated_cash_v1/training-bf16/fold_11`。

採用 `stockagent-day-trade-strategy-switch` 的 canonical 部署流程；因本機缺產物，
另依 repository storage contract 從固定冷庫版本恢復原值。
工程任務為 `day-trade-factorized-fold11-deployment-20261013`。

**目前已完成原值恢復、來源抽樣逐位驗收、原模型推論與單日 270 分鐘 strict replay；尚未 promotion，正在準備完整歷史新增。10/13 官方完成來源已驗收，原四模式已自行平倉。**
使用者已明確選擇「新增獨立模式、保留原四個策略」，並確認公開範圍是模型訊號，
不是原始資料。保留原 source 的 private／research 標記；不因此斷言模型衍生訊號
一定禁止公開，也不把使用者的範圍確認描述成資料商授權證明。

新候選的 `enabled=false`、selector 為 manual；沒有更改原四個 market／selector，
尚未替換正式帳本或重啟服務，沒有券商下單。使用者已另行授權在今日官方
收盤資料到齊後，補做既有四模式的模擬清算；此操作必須保留原帳本、
官方價格 SHA、實際修復時間與回放標記，不能冒稱原引擎當時成交成功。
初次 canonical `plan --candidate-preflight` 回傳 `blocked_preflight`／
`plan_complete=false`；14:20 的正式 `plan --add-mode` 已回傳 `ready_to_apply`，
但計畫不代表歷史重建或 promotion 完成。尚未執行正式 `apply`／`verify`。
不能把封存成功或 lifecycle complete 當成 live 特徵相容、模型 warmup、分鐘曲線與公網驗收。

## 已驗證的精確身分

- 從固定 dataset `legacy-vast-return-c8778c5898d80ceaf3ecfe7e` 取回 68 檔、
  95,902,098 原始 bytes；canonical decoder 完整比對原值 SHA 與檔案集合，
  未覆寫任何既有目的目錄。
- snapshot：
  `legacy-vast-return-c8778c5898d80ceaf3ecf-20261010T025558623293716Z-l0-penguin-586aae5a948a4dce`。
- checkpoint SHA-256：
  `eb27db1a15ac32ae1fc359d11290788f46e4872f98661eaff3c98949f2811150`。
- `validate_completed_training_artifacts` 驗證 fold11 及原始 train group：
  **18 項必要產物完整、missing=0、invalid=0**。這不是 18 個單元測試，也不是全庫測試。
- `fold_complete.json`：complete；test 2026-01-02 至 2026-10-02。
- FinancialTransformer、`score_entmax_scale_separated_cash`、2,757 symbols、
  14,726 features。保留原始 manifest 與 checkpoint，不改 head、特徵順序或原始權重。
- archive `.LEGACY_RESTORED.json` 的 `deployable=false` 保留；原值恢復不自動授權 promotion。

固定證據：[preflight.json](../artifacts/operations/tw_day_trade_factorized_fold11_deployment_20261013/preflight.json)。
詳細 log：
[冷檔 materialization](../artifacts/operations/agent-workflow/runs/materialize-fixed-model-archive-20261013T035236-c3f3e751/run.log)、
[原值還原](../artifacts/operations/agent-workflow/runs/restore-exact-model-originals-20261013T035437-0cce7b3f/run.log)、
[共享 lifecycle gate](../artifacts/operations/agent-workflow/runs/completed-training-artifact-gate-20261013T035622-5fab0e35/run.log)。

第一次 systemd 還原因未帶入實際 WSL distribution 被 storage admission 拒絕。
已由互動環境與 Windows `wsl --list --quiet` 核實 `Ubuntu-26.04`，
同一 task／run 名稱以正確環境重試成功；原失敗 log 留存，沒有放寬 admission。

## 新增候選與防錯機制

- stable market：`tw_day_trade_factorized_scale_separated_cash`，獨立初始模擬資金 1,000 萬，
  沒有改動原四個帳戶的資金。
- [market 設定](../services/discord_bot/markets/tw_day_trade_factorized_scale_separated_cash.yaml)
  與 [manual selector](../services/discord_bot/models/tw_day_trade_factorized_scale_separated_cash.yaml)
  精確指向指定 `training-bf16/fold_11`，啟用 model-scoped signal output，沒有自動換模型。
- [候選完整配置](../configs/deployments/tw_day_trade_factorized_scale_separated_cash_fold11_candidate_v1.yaml)
  由 original immutable run manifest 的 `configuration` 原值產生，獨立讀回字典完全相等。
  解決原 base-config chain 在本機遺失的問題，不改模型、交易設定、fitted RMS 或特徵順序。
  原始資料路徑仍保留，未假裝缺檔已恢復。
- [shared feature 前置檢查](../stockagent/live/feature_source_contract.py)
  在 source lock、cache、checkpoint／quote I/O 與 model runtime 變動前拒絕
  未驗證 paper projection 的 factorized 輸入，包含 panel override／prefetch 路徑。
  不走只有六個 executable base columns 的錯誤推論路徑；原四模式沒有 manifest 時不做額外 I/O。
- [canonical CLI](../scripts/switch_tw_day_trade_strategy.py) 的 `--candidate-preflight`
  只允許 disabled independent candidate 的 `plan`，輸出 actual／proposed mode set、
  精確 checkpoint、原四模式 config hashes、官方完成 session 與缺少來源。
  此 receipt 不能交給 `apply`；不能把 replacement 的 mode-set gate 當成已完成新增帳戶交易。

候選檢查 log：
[independent-mode-candidate-plan](../artifacts/operations/agent-workflow/runs/independent-mode-candidate-plan-20261013T041201-4596cd02/run.log)。
官方完成範圍為 **2026-02-25 至 2026-10-12，共 155 sessions**；若完成新增與全模式
回放，應驗收 `155 × 5 × 270 = 209,250` 個 strategy minute points。此為要求，不是已建置數。

遠端原 prepared manifest 另經 read-only SSH 核對：
SHA-256 `33bed11d08665f5578b6ef11f3689ee80c99b22b260f661683d3e35eb21d9f50`、
14,726 model channels、最後日期 **2026-10-02**、`live_eligible=false`、research-only。
原 source manifest 本機亦標示 end=2026-10-02；不能將日期改成 10/12 或混接 141-column
wide research table 來冒充同一 checkpoint 輸入。
[原 source contract log](../artifacts/operations/agent-workflow/runs/original-factorized-source-contr-20261013T041304-f55ddafa/run.log)。

## 本輪工程驗證

受影響的 feature guard、candidate switch、live signal／tail cache、deployment／promotion
回歸 **142 passed in 10.52s**；不是全庫測試，也不是模型 forward／live ABI 相容驗收。
[完整測試 log](../artifacts/operations/agent-workflow/runs/candidate-feature-and-switch-tes-20261013T041852-1d1ebad5/run.log)。
最初誤列不存在的 `test_market_config.py`，沒有執行任何測試；已依實際檔案清單重跑，
原失敗 log 保留。新 module／CLI／signal engine 的 `py_compile` 與 tracked diff whitespace 檢查通過。

12:19:29 engine state 的啟用集合仍是四個既有模式；open_position_count
為 785／27／1／12。engine PID 208392、InvocationID
`34227ee9d55a401f9471c032d43bc9e9` 與本 task 開始相同。
新增設定及 source guard 尚未透過本 task 重啟正式服務；這些觀察不是第五模式 public acceptance。

原四份 market config SHA 另與只讀 plan 獨立重算比對，完全相同。local 8766 與 gateway 8770
的 `/api/status` 均回傳原四模式、ledger integrity ready=true、health=degraded；沒有把既有
資料問題標成正常，亦未以本機 gateway 讀回聲稱第五模式已經公網上架。
本輪接手收據：[independent_candidate.json](../artifacts/operations/tw_day_trade_factorized_fold11_deployment_20261013/independent_candidate.json)。

## 上架前尚需完成

1. 已確定新增獨立模式，不覆蓋 V8／Multi-Basis；還需完成 canonical additive
   cutover 與原四模式 history parity，不能以 replacement apply 繞過 mode-set 檢查。
2. 原 configuration 的 physical feature dependency 已按固定 snapshot 恢復；
   完整 14,726-feature attachment 與原 checkpoint 推論已成功。
   原分片的兩日抽樣逐位 parity 已通過；剩餘 gate 是今日來源及新模式完整歷史回放與分鐘驗收。
3. 原四個 live paper modes 已於官方來源到齊後自動平倉，canonical 清算 plan
   的 settle_count／blocked_count 均為 0，沒有人工補成交。依 skill flat gate，
   cutover 前仍須重新獨立讀回 flat，不能只依賴較早的觀察。
4. 完整來源與新增流程完成後，綁定此精確 artifact root 與初始資金執行完整 read-only `plan`。
   正式 `latest` 由 receipt-verified TAIEX completed sessions 決定，不猜今天是否完成。
5. 同一計畫的 `apply` 僅在 flat gate 成立後執行：保留非目標 signal pins、隔離候選、
   每 completed session／active mode 的 09:01–13:30 共 270 分鐘點驗收，原子交換與 rollback。
6. `verify`／`status` 需證明 engine integrity、enabled-mode equality、目標 fingerprint
   warmup 與 local／public API 歷史可見。缺資料只能標示缺口，不冒充完成。

本文件是接手紀錄，不是部署完成收據，也沒有建立自動盤後切換排程。

## 15:29 前段隔離執行驗收（尚未 promotion）

- 2026-02-25～04-13 前 31 個 completed sessions，以本次固定來源生成的訊號
  完成 canonical replay；只讀本機分鐘資料，沒有額外 Shioaji 請求或券商下單。
  [執行 log](../artifacts/operations/agent-workflow/runs/factorized-first-31-session-exec-20261013T071944-35cec75f/run.log)。
- 31 × 270 = **8,370** strategy minutes；source pair coverage 139/139、
  linear_interpolation_used=false、unchanged fills SHA 保留。獨立 validate-only
  回傳 full_requested_range_passed=true、validated_rows=8370、
  unverified_historical_interior_rows=0。這只是前 31 日檢查，不代替完整 156 日。
  isolated new-only candidate 的 cutover ready=false 是正確結果，不能用它
  覆蓋現有四模式；正式流程仍須 additive union／全五模式驗收。
  [獨立驗收 log](../artifacts/operations/agent-workflow/runs/factorized-first-31-independent--20261013T072834-c95daf64/run.log)。
- 3/05 的 0052、3/17 的 2485 本機序列從 09:03 才開始；沒有 09:01 價格證據，
  訊號保留、執行維持 blocked／quote_not_after_signal，未製造 09:01 成交。
  僅憑缺少 bar 不能區分「當時未撮合」與「來源漏列」；這是來源／執行邊界，
  不能把後來的價格移到較早時鐘，也不能以頁面上線表示每個目標必定成交。
- 首次分鐘 CLI 少給了 required session scope，明確拒絕、沒有 publication；
  改用 receipt 中的完整 31 日清單與 market 同時傳入後成功，原失敗 log 保留。

## 14:59 固定完成來源與全歷史推論里程碑（尚未 promotion）

- 10/13 完成來源的 projection 已驗收，共 209 個資料日期，next decision=10/14。
  下一交易日準備時鐘改由共享官方 TWSE 行事曆判定；不必等下一日資格清單
  才能準備昨日特徵。此時鐘 **不是當沖資格、可交易或成交證據**。
  行事曆／TAIEX 檔案 SHA 與此區別保留在 generation receipt；unknown 仍拒絕。
- immutable generation `fdc97c87b3927a1aeea98aa7079ff68aeb9ea714c2cb7536fcca393b21a8dfc0`
  的 manifest SHA 為 `498564ac05c36637c96a9aa8e428cb055526d93a7a1089b08aa0db8b3a072460`。
  原始 2/24、9/30 兩日個股／共同特徵再驗，共 **81,166,080 Float32 cells，different=0**；
  仍是兩日抽樣，不是全期間資料品質驗收。
  [本次 parity receipt](../artifacts/operations/tw_day_trade_factorized_fold11_deployment_20261013/completed_projection_parity/parity_receipt.json)。
- 完整 156-session 訊號工作於 14:59:41 取得有限、逐位相同的來源副本；其 public
  source fingerprint 與上列 projection 相同。來源更新不會再改變本次推論輸入，
  原始實際生成時間與回放 decision clock 分開保存，不冒稱歷史即時訊號。
  [固定來源 receipt](../artifacts/operations/tw_day_trade_factorized_fold11_deployment_20261013/historical_input_snapshot_completed_20261013/source_snapshot_receipt.json)。
- 最新共享契約回歸 **555 passed in 83.97s**；另有 privacy／browser boundary
  **25 focused passed**、Node component **16 passed、0 skipped**。各次範圍重疊，
  不相加成不重複測試總數，也不是全 repository 測試通過。
  [共享回歸 log](../artifacts/operations/agent-workflow/runs/factorized-final-shared-contract-20261013T064136-60c6a513/run.log)。
- 這個 14,726-channel 模型的歷史輸入是約 5.2 GB 的 32 日 FP32 window。
  實測歷史單次準備／forward 尚為秒級，受共享 GPU／記憶體工作影響；本次上架
  不宣稱開盤到訊號已達毫秒級。原價格、特徵精度及完整歷史未為縮短測試而刪減。

## 14:35 上架準備里程碑（尚未 promotion）

- 原引擎於 14:01 官方收盤來源到齊後自動完成既有模式清倉；沒有使用人工
  清算授權、沒有重寫舊成交。獨立 flat／原四設定 SHA 證據：
  [closed_original_modes.json](../artifacts/operations/tw_day_trade_factorized_fold11_deployment_20261013/closed_original_modes.json)。
- 除權息第一次來源嘗試有 26 個 stock_delivery_detail HTTP-200 節流／拒絕頁，
  不完整 attempt receipt 留存；canonical 自動重試成功，23,331 rows、
  coverage_complete=true、failure_count=0。沒有放寬完整性門檻或虛構發放日。
  官方 completed-session receipt 於 14:28:17 核對所有相依層至 10/13、errors=[]。
- 正式 readonly addition plan 核對 156 sessions 與預期
  `156 × 5 × 270 = 210,600` strategy minute points；這仍是要求，不是已建置數。
  [plan log](../artifacts/operations/agent-workflow/runs/factorized-final-addition-readon-20261013T062011-fdcbadf3/run.log)。
- 歷史重建改為有限三份 byte-identical source snapshots 與 immutable factorized
  generation pin；來源原始 SHA／copy readback／實際 capture time 保留。官方 producer
  之後可繼續更新，不必暫停服務；source coherence guard 未被繞過。
- 修正 updater 的 finalize→prepare writer race：busy writer 只消耗有界等待時間，
  不耗盡 build attempts；逾時仍 failed_retryable、保留 last-good。其他來源錯誤維持
  有界失敗重試，不轉為正常。23 項 updater／snapshot／publication 測試通過。
- 既有四模式公網頁在五種 viewport 的原生模式／日期／訊號／圖例互動與無橫向溢出
  通過；使用 cpu-2d profile，不代表 WebGL/GPU 驗收。第五模式尚未可見，
  不以此 reference run 取代新模式上架驗收。
  [reference browser receipt](../artifacts/operations/tw_day_trade_factorized_fold11_deployment_20261013/browser_reference_recheck/receipt.json)。
- 最終 browser acceptance 另加入首日／最新日 public signal API 的模式與日期核對，
  以及私有特徵 explicit no-proxy policy 與 rendered disclosure；9 項防誤判測試通過。

## 13:55 工程里程碑與授權邊界（尚未上架）

- chronological projection 原始 2026-02-24／2026-09-30 兩日共
  **81,166,080 個 Float32 cells，different=0**。比較完整 individual／common
  feature channels，不含六個 executable base prices；這是兩日抽樣證據，
  不是全期間或所有 provider 都已驗收。
  [來源 parity 收據](../artifacts/operations/tw_day_trade_factorized_fold11_deployment_20261013/chronological_parity/parity_receipt.json)。
- 同一 checkpoint 的 owned inference workspace 僅減少 FP32 暫存複製，
  原 FP32 RMS 的 divide／multiply 順序、BF16 AMP／TF32 與訓練梯度路徑不改。
  七個 session、19,299 symbol rows、289,485 浮點欄位逐位一致（含 NaN／signed zero）；
  model-derived financial summary 也相同。
  [輸出 parity 收據](../artifacts/operations/tw_day_trade_factorized_fold11_deployment_20261013/seven_session_output_parity.json)。
  共用 GPU／磁碟負載不同，該 A/B 不足以宣稱穩定延遲改善或達到毫秒級。
- canonical 2026-02-25 單日 replay：6 entry fills、42,000 shares、期末 flat；
  270 sourced minutes，其中 268 interior minutes、unsourced=0，strict independent
  acceptance 通過。原 benchmark state/history 作為只讀 source，沒有借用開盤價造分鐘成交。
  [strict acceptance log](../artifacts/operations/agent-workflow/runs/factorized-strict-one-session-ac-20261013T053456-a3221cff/run.log)。
- 最新受影響模型／adapter／回放／新增／交易引擎回歸 **406 passed in 29.49s**；
  不是整個 repository full suite。兩個舊 frontend 字串斷言更新為 disclosure-aware
  價格呈現與正整數 cache version，未改價格、計算或資料完整性門檻。
  [回歸 log](../artifacts/operations/agent-workflow/runs/factorized-comprehensive-regress-20261013T055343-682046de/run.log)。
- 13:45 canonical close_initial 未接受 10/13：當時 TWSE／TPEx aggregate 仍到 10/12；
  原失敗 receipt 保留。之後 TWSE raw 已有 10/13，但 TPEx 仍未到齊；
  不以其中一個來源、HTTP 200 或 TAIEX 日期前進冒充全市場收盤已完成。
- 使用者授權補做 **10/13 官方收盤模擬清算**。13:44 觀察 711 residual paper
  positions；實際操作前必須重新 plan／核對 source 与 book fingerprint。
  沒有授權以昨日價、bid／ask 或任意最後價格替代今日缺少的官方 close。
- 原子上架前仍須全期間新策略訊號／replay、五模式 270 分鐘 source proof、
  flat／原四模式 account parity、Discord 精確 checkpoint warmup，最後才驗證
  公網瀏覽器的原生模式／日期／訊號／圖例互動与多裝置 screenshots。

## 13:14 工程里程碑（仍未 promotion）

- 固定 source manifest、原 feature dictionary、2025/2026 年事件 seed 以精確成員／SHA
  有界取回；沒有複製整個遠端訓練 workspace。原 physical source 由 canonical storage
  materialization／pin 完成，原 config 的 immutable path 不改。
- 原模型 ABI 正式推論成功：
  [inference probe](../artifacts/operations/agent-workflow/runs/factorized-model-inference-probe-20261013T045533-85839407/run.log)。
  首次失敗發現 effective checkpoint 模型 contract 將四個 scalar 記為 float：
  `input_dropout`／`candle_dropout`／`dropout=0.0`、`rope_base=10000.0`；
  只修正 runtime spelling，不改原 manifest、權重、數值或 fingerprint validator。
- 新增 canonical `--add-mode`：獨立新模式先回放驗收；舊四模式 account dictionaries
  及 signals／fills／orders／events／latency 原始 bytes 作為 prefix 保留；新 append segment
  與獨立 replay SHA 也核對。分鐘估值獨立補齊，不用「重新成交」改寫舊訊號。
  promoter 仍採 flat gate、雙鎖、same-filesystem exchange，保留配置及帳本 rollback。
- 每日 updater 用原有 opening activation／completed-session finalize 後再生成 factorized
  projection；失敗保留 last-good 與 failed attempt，不能把不完整目錄當已完成。
- 受影響 live／adapter／switch／promotion 回歸 190 passed；新增 append／rolling-window
  驗證 86 passed；source chronology、minute curves、audit／promotion 回歸 142 passed。
  這些是分批 focused tests，不加總冒充唯一測試數或全庫通過。
- 來源逐位比較發現第一版投影錯誤：混用 absolute／relative 事件檔路徑時，字串排序
  可能把 October delta 放到年度 seed 前，阻擋過去事件，部分輸入變為零。
  已停掉本次兩個尚未發佈的候選回放，保留輸出與失败 receipt。
  canonical `AnnualObservationEvents` 現在按資料 release-date 排序並拒絕重疊範圍；
  有效 TTL、NULL／report／issuer barriers 不變。正在重建並重新比對，原四模式沒有停機。
- Factorized 相鄰推論窗口重用前 31 個 immutable rows，只讀新 row；buffer 不原位改動，
  signed-zero／NaN／Inf normalization exact-bit tests 通過，並以 byte budget 控制 cache。
  這是演算法成本修正，尚未宣稱開盤能在一秒內完成。
