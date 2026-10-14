# Factorized values／無基底／scale-separated cash fold11 上架與驗收結果

## 範圍與目前狀態

使用者指定：
`artifacts/markets/tw_day_trade_factorized_values_20261006_no_basis_flat_bf16_tf32_b128_scale_separated_cash_v1/training-bf16/fold_11`。

採用 `stockagent-day-trade-strategy-switch` 的 canonical 部署流程；因本機缺產物，
另依 repository storage contract 從固定冷庫版本恢復原值。
工程任務為 `day-trade-factorized-fold11-deployment-20261013`。

**18:22：指定 fold11 已新增為獨立第五模式，原四模式與帳本保留。
17:41:21 完成同檔案系統原子交換，18:09:37 完成消費者復原，正式 verify 為
`verified_with_data_gaps`。五模式 156 日／210,600 分鐘驗收通過，Discord 實際
推論暖機 5/5，公網五種畫面比例與原生操作通過；不是全庫測試、實盤成交或下一次開盤 SLO 證明。**
使用者已明確選擇「新增獨立模式、保留原四個策略」，並確認公開範圍是模型訊號，
不是原始資料。保留原 source 的 private／research 標記；不因此斷言模型衍生訊號
一定禁止公開，也不把使用者的範圍確認描述成資料商授權證明。

新模式目前 `enabled=true`、selector 為 manual，精確指向指定 checkpoint，
不自動替換模型。原四個 market 設定 SHA、金融帳戶與五條決策／執行串流 prefixes
均重新核對；只有平倉後同日單調前進的 mark heartbeat 可不同。原四模式於
14:01 前已自行完成清倉，使用者授權的補做模擬收盤清算**沒有執行**。
切換及復原全程沒有券商下單，原整份 live 保留為 rollback。
初次 canonical `plan --candidate-preflight` 回傳 `blocked_preflight`／
`plan_complete=false`；14:20 的正式 `plan --add-mode` 已回傳 `ready_to_apply`，
但計畫不代表歷史重建或 promotion 完成。後續 apply／verify 的實際完成證據如下；
後面的時間標題保留各階段當時觀察，不代表現在仍未上架。
不能把封存成功或 lifecycle complete 當成 live 特徵相容、模型 warmup、分鐘曲線與公網驗收。

## 18:22：正式上架與獨立讀回

- stable market：`tw_day_trade_factorized_scale_separated_cash`；公開名稱為
  「台股・Factorized 無基底 Scale Cash 當沖研究模擬（初始 1,000 萬）」。
  [當沖網站](https://penguin72487.ddnsgeek.com/tw-day-trade/) 的第五卡片、模式選單、
  完整訊號明細與曲線都可使用，沒有公開私有原始訓練特徵或其數值代理。
- 指定 fold11／checkpoint `3132bca00eb5`，完整 SHA 為下節原始 artifact identity。
  2026-02-25～10-13 的 **156 份不同交易日訊號**均來自此模型；新模式 42,120 分鐘，
  五模式合計 **156 × 5 × 270 = 210,600**。未驗證 historical interior rows=0，
  不插值。無成交分鐘的來源契約與估值沿用仍明確揭露；分鐘網格完整不代表每分鐘都有新成交。
- 第五帳戶期末模擬權益 NT$ 17,427,425.689575978；五模式未平倉均為 0。
  這是依指定模型、來源及回放契約計算的研究模擬，不是券商報酬或投資結論。
- `promotion_receipt.json` 記錄 `same_filesystem_atomic_directory_exchange`、
  17:41:21 的交換及完整 rollback
  `artifacts/live/tw_day_trade_factorized_scale_separated_cash.addition.s8y99flq`。
  恢復 Discord／公網消費者時 `second_ledger_promotion_performed=false`；沒有再回放／交換一次。
- 引擎 run `969b8a84a707441997bc7ece759c7600`：五模式相等、ledger ready=true；
  Discord run `2237234-1791886083169178138`：Gateway connected、core ready、相同引擎、
  startup warm ready 5/5 且第五指紋正確。local／gateway 的 revision lag=0、divergence=0，
  首末歷史日及訊號 API 均獨立讀回。
- 再次對原四帳戶及原 signals／fills／orders／events／latency 的 byte prefixes
  雜湊、四 market 配置 SHA、原 checkpoint SHA 及 rollback existence 全部核對通過。

證據：[原子交換](../artifacts/live/tw_day_trade_simulation/promotion_receipt.json)、
[已完成操作](../artifacts/operations/tw_day_trade_strategy_switch/tw_day_trade_factorized_scale_separated_cash/status.json)、
[正式 verify](../artifacts/operations/agent-workflow/runs/factorized-final-current-deploym-20261013T101015-9041d4c4/run.log)、
[原四模式保留證明](../artifacts/operations/agent-workflow/runs/factorized-original-four-final-p-20261013T101017-3ce8ee43/run.log)、
[消費者復原](../artifacts/operations/agent-workflow/runs/factorized-bounded-memory-consum-20261013T100740-881cc23c/run.log)。

## 盤後消費者／記憶體根因與修正

1. 實際 Discord `.env` 的明確 scheduled allow-list 原本只有四模式。
   canonical switch 現在僅追加所選第五模式，保留原順序與其他環境值，
   receipt 不複製 token。私有 `.env` 原子寫入在 temporary 建立時即用 0600，
   不先以預設 0644 寫出憑證；exclusive create 拒絕並保留其他 writer 的 temporary。
   目前 `.env` 已獨立讀回 0600。另提供有期限、限盤後且沒有進行中開盤任務的部署暖機窗口，
   重用既有無報價／無 publication／無下單的真實推論流程；不偽造 ready receipt。
2. 第一個五模式 warm owner 的 factorized FP32 視窗為
   `32 × 2,757 × 14,726 × 4 = 5,196,746,496 bytes`（4.84 GiB）。
   `np.nan_to_num(copy=True)` 再複製整個視窗且建立巨大 masks，觸發 12 GiB
   `memory.high` reclaim；實際 thread 等待 `mem_cgroup_handle_over_high`、high events
   178,974，17:56 的程序最後於 18:05:35 watchdog timeout。這是保留的真實失敗，不是模型就緒。
3. 共用 normalized window 對 factorized reader 的自有 FP32 slab 做最多 1,048,576
   值的分塊原位清理；不修改 source/view/cache storage，沒有改 feature 順序、RMS、
   金融精度或模型。NaN／Inf／signed zero、source view 隔離及原 rolling cache 的
   逐位元一致測試通過。18:08 的新 owner 真正完成推論：第五 cold warm 42.077 秒，
   五模式總共 51.929 秒；MemoryPeak=8,344,502,272 bytes（7.77 GiB），
   high/max/oom/oom_kill=0。這是含已存在 disk panel cache 的新程序量測，
   不是完全失效來源的重建量測，也不是 09:00 的 hot latency。
4. 同日實際開盤需要另保留不修改 base 的 private live window；以實測 peak 加
   4.84 GiB 視窗估算約 12.61 GiB，不能繼續沿用舊 12 GiB 軟閾值。
   canonical unit template 與現有 unit 同步為 MemoryHigh=16G，MemoryMax=24G、
   swap=4G、watchdog 與交易 gate 不變；daemon-reload 後讀回 cgroup
   `memory.high=17179869184`，未重啟程序。開盤峰值為估算，下一次真實開盤尚未量測。

驗證：[138 input／factorized／browser tests](../artifacts/operations/agent-workflow/runs/factorized-bounded-input-memory--20261013T100641-df6f4de5/run.log)
（24.01 秒）、[111 Discord／排程／資源 regression](../artifacts/operations/agent-workflow/runs/factorized-final-discord-memory--20261013T101330-1a4f9a54/run.log)
（11.02 秒）、[234 共用紙上執行測試](../artifacts/operations/agent-workflow/runs/factorized-close-shared-runtime--20261013T092359-fc70fb8f/run.log)
（7.09 秒）、[219 immutable close／promotion regression](../artifacts/operations/agent-workflow/runs/factorized-pinned-source-final-t-20261013T091312-fc8312d7/run.log)
（16.77 秒）。各批有重疊，不加總成唯一測試數或全庫通過。

最後的私有原子發佈／原四保留／Discord 回歸為
[152 passed in 9.67s](../artifacts/operations/agent-workflow/runs/factorized-private-env-publicati-20261013T102012-d96e7213/run.log)，
共用 atomic primitives／private permissions 為
[19 passed in 2.99s](../artifacts/operations/agent-workflow/runs/factorized-shared-atomic-primiti-20261013T102041-646dd21f/run.log)。
18:22 公網獨立讀回仍為五模式、paper only、revision lag=0、divergence=0；
新模式 `session_flat_after_exit`、open positions=0。現有 Discord 程序 NRestarts=0、
memory high/max/oom events=0；`.env`=0600，硬上限仍為 24 GiB。

## 最終公網瀏覽器驗收與保留限制

- 實際 HTTPS、Chromium CPU-2D、五個 viewport：1366×768/DPR1、1280×720/DPR2、
  2560×1440/DPR1、820×1180/DPR2、390×844/DPR3。
  實際切換新模式、日曆起訖日、訊號點擊／私有資料與 PIT 提示、曲線圖例關閉再恢復，
  首末交易日 2,757 訊號與 paper-only 邊界均驗證；無 browser errors 或頁面水平溢出。
- 第一個 browser probe 的 404 是測試 URL 的 query separator 遺失：
  `urljoin(..., 'api/signals?')` 會移除空 query。已修正 helper 並新增實際 URL 語義測試，
  不將它誤認為網站帳本消失。
- 手動檢視手機截圖發現 X 軸日期標籤重疊及尾標籤裁切，shared chart renderer 現依 CSS
  width／resize 調整刻度數與右留白。只改 labels，沒有刪分鐘資料；新 asset version=6。
  35 個 Node tests 全通過，最後五 viewport 再次通過，並手動檢視 laptop／phone screenshots。
- 這些是 viewport 模擬，不是五種實體裝置，GPU／WebGL 不在本次 browser acceptance 範圍。
- 新模式歷史 09:01 bar 仍有六個明確缺口：03-05/0052、03-17/2485、05-27/2489、
  07-09/00715L、07-15/00715L、08-21/7751。完整模型訊號保留，缺價標的 fail closed；
  不用 open、Bid/Ask、沿用價或虛構量補成交。原四模式目前的 official-open 缺價、
  partial fill 與今日 opening acceptance failed 警告也保留，因此整體資料健康仍為 degraded。
- 已完成的是第五模式上架與上述工程驗收；不保證下一次開盤一秒內完成，
  不將研究/PIT 限制消除、不宣稱所有資料或整個 repository 無問題。

證據：[最終五 viewport receipt](../artifacts/operations/tw_day_trade_factorized_fold11_deployment_20261013/browser_final_chart_v6/receipt.json)、
[手機最終曲線](../artifacts/operations/tw_day_trade_factorized_fold11_deployment_20261013/browser_final_chart_v6/phone_chart.png)、
[35 Node tests](../artifacts/operations/agent-workflow/runs/factorized-final-shared-browser--20261013T101438-9bc459e8/run.log)、
[新模式來源／缺價 receipt](../artifacts/live/tw_day_trade_simulation/addition_evidence/new_mode/rebuild_receipt.json)。

工程結案保留原 failed／cancelled run、exit code 與 log。對因錯誤候選中止的三個
訊號階段，現在同名階段重新執行的命令是 **read-only canonical verify**，
核對已發佈的完整 156 日訊號與現有部署，而非重跑不完整候選、生成新成交或改寫舊 run。
其他失敗用 canonical `task resolve-run --outcome superseded` 對應同 task 的後續成功
來源恢復、精確 parity、測試、原子部署／消費者復原或 browser receipt；
只標記被有效完成證據取代，不清除既有資料缺口與 SLO 失敗。

## 17:11：完整歷史與切換復原根因

- 完整 156 日訊號（2026-02-25～10-13）已完成，checkpoint `3132bca00eb5`。
  新模式 42,120 分鐘／808 source pairs、合併五模式 210,600 分鐘已驗收；
  缺少 source pair 與未驗證的歷史 interior rows 均為 0，不插值。
  這是回放證據，不冒稱當時存在的訊號或券商成交。
- 16:41:03 guardian 對有意停止的引擎執行 `enable --now`，使第五模式的未成交
  shadow account 與操作紀錄在切換期間出現。promoter 在原子交換前拒絕；
  disabled 設定已還原。原四帳戶僅平倉後 mark heartbeat 不同，所有原始五個
  決策／執行串流的 byte prefixes 均一致；第五 shadow 零持倉、零 fill/order／P&L。
- 共用 [維護 FD lock](../stockagent/live/day_trade_maintenance.py) 現在協調 coordinator
  與 guardian；只有實際 held OS lock 才延後修復，程序崩潰由 kernel 釋放，
  stale marker 不會停掉自動復原。真實 guardian 在 lease 期間回報
  `planned_maintenance`、`actions=[]`、`repair_enabled=false`、`ready=false`。
- 新增及替換模式共用 canonical promoter 的 `before_exchange`：完整來源驗證完成
  後才停引擎，不在昂貴驗證期間停服務／提前 enable 新模式。維護、promotion、
  aborted empty account 與恢復負向測試 **92 passed in 5.39s**，不是全庫測試。
- `--resume-addition-candidate` 僅允許完全同一失敗且未 promotion 的 operation；
  checkpoint、資金、完成日期、old accounts、execution prefixes 仍重驗。
  對空 shadow，額外保存其 state 與每條追加 record；有任何 fill/order、金融變動、
  其他模式紀錄或未知 event 都拒絕。原 live 不 truncate，正常 exchange 保留整份 rollback。
- 再驗收發現 10/13 TWSE 收盤 raw 最新檔已被 collector 更新，先前 SHA
  `bf162436…` 不再匹配。讀回受影響 6257 close 仍為 279，但**不跳過 SHA**。
  shared official-close parser 現以 canonical immutable raw writer 固定實際消費的 bytes，
  runtime、歷史 replay 與清算 plan 都保留 content-addressed close evidence。
  失敗候選完整保留，重新建置新模式的 replay／分鐘／合併候選，沒有僅改 SHA 冒充舊來源。

證據：[合併驗收](../artifacts/operations/tw_day_trade_strategy_switch/tw_day_trade_factorized_scale_separated_cash/runs/20261013T161623+0800/validate_addition.stdout.log)、
[92 tests](../artifacts/operations/agent-workflow/runs/factorized-maintenance-regressio-20261013T090029-e4703108/run.log)、
[真實 guardian 維護觀測](../artifacts/operations/tw_day_trade_factorized_fold11_deployment_20261013/maintenance_guardian_probe/latest.json)。

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

## 16:15 全歷史完成與切換心跳 race 修正（尚未 promotion）

- 156/156 個交易日訊號已完成，範圍 2/25～10/13；全部 checkpoint fingerprint
  為 `3132bca00eb5`，固定來源最終 hash fence 通過。
  [全歷史訊號 log](../artifacts/operations/agent-workflow/runs/factorized-156-session-final-com-20261013T065651-7adc5b73/run.log)。
- 第一輪正式 isolated replay 完成 156 日，期末 open_position_rows 全為 0；
  42,120 strategy minutes、808/808 required source pairs、interpolation=false，
  fills SHA 不變。這是新模式 component，不代表五模式已合併或上線。
- additive snapshot 複製期間被原引擎的 post-close `last_mark_at` 心跳更新擋下。
  獨立 diff 四模式只顯示這個欄位前進；其他帳戶欄位、決策／執行 bytes 沒有改變。
  失敗發生在切換之前，promotion_completed=false；原四模式沒有中斷，新模式仍 disabled。
  [被正確保留的失敗 log](../artifacts/operations/agent-workflow/runs/factorized-fifth-mode-canonical--20261013T075834-715162aa/run.log)。
- shared merge 與 promoter 對 running source 的比較，現在只允許已清倉、同日、
  有時區且單調前進的 `last_mark_at`。其他所有欄位仍逐值相等，candidate 與
  prior snapshot 自身仍完全相同，不修改 snapshot 的時鐘或回填金融數值。
  切換前另外重比五種原始 decision／order／fill／event／latency 完整 byte streams；
  即使財務未變，期間多了任何一筆紀錄也會擋下，避免遺失新資料。
- 收盤有庫存、金額變更、未知欄位、逆時鐘、跨日、naive／缺漏／壞時鐘，以及
  每一種 execution stream 新增紀錄均有反例；相關 shared promotion／switch／
  accounting 回歸 **92 passed in 5.03s**。
  [最終回歸 log](../artifacts/operations/agent-workflow/runs/factorized-final-heartbeat-and-l-20261013T081504-0dea7db2/run.log)。

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
  positions；實際操作前必須重新 plan／核對 source 與 book fingerprint。
  沒有授權以昨日價、bid／ask 或任意最後價格替代今日缺少的官方 close。
- 原子上架前仍須全期間新策略訊號／replay、五模式 270 分鐘 source proof、
  flat／原四模式 account parity、Discord 精確 checkpoint warmup，最後才驗證
  公網瀏覽器的原生模式／日期／訊號／圖例互動與多裝置 screenshots。

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
  已停掉本次兩個尚未發佈的候選回放，保留輸出與失敗 receipt。
  canonical `AnnualObservationEvents` 現在按資料 release-date 排序並拒絕重疊範圍；
  有效 TTL、NULL／report／issuer barriers 不變。正在重建並重新比對，原四模式沒有停機。
- Factorized 相鄰推論窗口重用前 31 個 immutable rows，只讀新 row；buffer 不原位改動，
  signed-zero／NaN／Inf normalization exact-bit tests 通過，並以 byte budget 控制 cache。
  這是演算法成本修正，尚未宣稱開盤能在一秒內完成。
