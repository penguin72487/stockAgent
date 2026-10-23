# Unit MGDA 獨立研究模擬模式

## 範圍與精確身分

新增第六個模式 `tw_day_trade_unit_mgda`，初始資金 NT$10,000,000；
不是替換既有五模式，也沒有券商實單。

最新使用者指示：未能在 09:01 前產生的訊號，統一按 09:01 作研究模擬。
本次 MGDA 歷史都是現在另行生成，故新增候選採明列的 full-target 09:01
反事實情境，不沿用分分鐘 50% 成交量掃單。合法資格、整張、價格界線與 NAV
限制不變；不聲稱當時有足夠流動性或真實成交。缺少有效 09:01 來源仍列缺價，
不以 09:09 等未來價格倒填。原始生成時間保留；既有五模式訊號、成交與帳戶不因此重寫。

- 原實驗：`geometry__unit_mgda_sgd_step/attempt-00/training`。
- 選定：`fold_10`，best epoch 128（原訓練停止於 epoch 228）。
- checkpoint SHA256：`2c59e3dd3af0dbb377cbc1796ab77607f71de3fc4c741311132ad6b01225d1af`。
- 執行 fingerprint：`08f27475759e`。
- 保留原模型、14726 features、2757 symbols、原精度與輸出／loss ABI；沒有重新訓練。
- 原始產物為 `research_only=true`、`historical_point_in_time=false`；
  2026 年亦曾用於研究選擇，不宣稱為 untouched holdout 或可實現歷史績效。

完整原產物從已核對的遠端冷封存恢復，位於
`artifacts/operations/day-trade-unit-mgda-deployment-20261021/restored-model/`。
模型 selector 與部署設定分別是
`services/discord_bot/models/tw_day_trade_unit_mgda.yaml`、
`configs/deployments/tw_day_trade_unit_mgda_fold10_live_v1.yaml`。

## 執行環境

原訓練曾使用環境變數選定 sparse adjoint；部署以
`configs/deployments/tw_day_trade_unit_mgda_fold10_runtime.env` 還原，
不是放寬 checkpoint 相容性檢查。原五模型的語意 fingerprint 在此 profile
前後完全一致。QuACK 選用依賴獨立列於 `requirements-quack.txt`，
安裝時沒有替換 Torch／CUDA；已通過 FP32／BF16 kernel 對照與實際模型推論。

## 歷史與正式新增流程

依官方完成交易日產生 2026-01-02～2026-10-21 共 193 日的獨立訊號，
實際 CUDA 推論已完成。保留真實生成時間與來源 pins，歷史明確標記反事實回放；
09:00 官方 open 僅供模型輸入／整張 sizing，09:01 紙上進場使用可追溯完成分鐘。
既有五模式的決策、成交及帳戶由 canonical additive merge 原樣保留。

唯一切換入口仍是 `scripts/switch_tw_day_trade_strategy.py` 的
`plan --add-mode → apply --add-mode → verify → status`。
此情境的 plan／apply／verify 均加 `--historical-full-fill-0901`，
由 plan 與驗收明確綁定 `retrospective_prior_paper_fill_else_09_01_minute_price_full_target_no_liquidity_claim_v1`；
不把這個研究假設改寫成即時執行規則，也不把不同的 v4 容量掃單當成相同部署。
完成判準是原五模式 flat、獨立候選驗收、同檔案系統原子交換、保留 rollback、
引擎六模式同步、Discord 六模式指定 checkpoint 暖機及公網瀏覽器驗收。
最終狀態以以下當前 receipt 為準，不以本文件或 HTTP 200 代替：

`artifacts/operations/tw_day_trade_strategy_switch/tw_day_trade_unit_mgda/status.json`

每個完成日與模式須有 270 個 09:01～13:30 點；新增模式為 52,110 點，
六模式合計為 312,660 點。原訓練結束日與線上新增日不同；後續訊號仍須走正式
盤前資料驗證與開盤來源，不以歷史回放證明開盤延遲達標。

## 切換前發現的共用估值缺陷

第一個完整候選在分鐘驗證安全停止：2026-07-17 的 09:01 已有 4 個部位，
第 5 個部位直到 09:09 才首次成交。舊 flat-session rebuilder 卻假設全天最終
部位數、成交股數與平均成本都已在 09:01 生效，亦會把後續追加成交的成本提前套用。
當次 `promotion_completed=false`，正式帳本與原五模式未切換。
後續 v4 候選亦於使用者澄清 09:01 語意時停止，未切換。這些診斷時間點
是估值／成交時序，不是模型訊號生成時間；本次發布另建 09:01 情境候選。

修正 `scripts/rebuild_tw_day_trade_minute_curves.py`：一般當沖與 carried valuation
共用 `_ImmutableMinuteInventory`，按 `fill_at`（非重建 `recorded_at`）逐筆推進
首次進場、追加、費用分攤與沖銷。一般當沖只用當日 fills，從 accepted opening
還原已實現損益基線；09:01、13:30 的持倉數與已實現損益仍嚴格核對。
09:01 僅要求當時已持有標的的完成分鐘來源，不要求未來才進場的標的有早盤價格。
保留 exit-only legacy book 的既有介面，沒有更動成交、部位或最終損益。

驗證：`unit-mgda-minute-inventory-accep-20261021T074456-ee9ff136`，
136 passed，涵蓋延後／分批進場、部分沖銷、費用、選定區間基線、
開盤內已實現損益不得重複累加、端點拒絕及原有留倉／策略切換契約。

另以第一個未切換的真實候選完整驗證 193 日、52,110 點，2,062 個必要
symbol-day 來源均由本機覆蓋，沒有發起 API；開盤重估與原模擬引擎的權益差異
為零，成交 SHA 不變。7 月 17 日獨立讀回亦確認 09:01／09:08／09:09／13:30
持倉數為 4／4／5／0。證據是工作目錄下的
`full-minute-regression/minute_curve_receipt.json` 與
`july17-minute-inventory-readback.json`；這是重建器驗證，不代替正式新增驗收。

09:01 情境另通過 312 項切換／回放／分鐘估值回歸與 2 項模擬執行 oracle；
確認滿額假設不繞過價格、資格、整張及 NAV 限制，並拒絕盤中使用。
正式 plan 已綁定 193 日、六模式 312,660 點及明列的 full-target 09:01 ABI；
獨立候選已完成重建，完整來源與帳務驗收如下；不以單元測試代替發布完成。

部署操作使用原服務唯讀程式 namespace，僅在子 mount namespace 套入已驗證、
唯讀固定的切換 CLI、開盤回放器與分鐘重建器；不修改原 frozen release，
也不吸收其他工作中的程式變動。固定版證據位於
`artifacts/operations/day-trade-unit-mgda-deployment-20261021/full-0901-code-release.json`。
下載 fallback 使用正式 `.env`，不將憑證缺漏誤報成 provider 缺價；
僅讓 canonical collector 處理 local-first 之後的真實缺口。

## 09:01 完整回放的獨立讀回

193 日回放完成，1,678 筆進場全部是 09:01，晚於 09:01 與未標示為紙上模擬
的筆數均為零；193 個原始生成時鐘都保留，193 日皆完成模擬沖銷。
`unit-mgda-0901-full-history-cloc-20261021T082724-fe48fd9a` 保存獨立讀回。

完整分鐘驗收攔到 `009802/2026-09-01`：已由來源發布的 09:01 Close 建立
明列的 full-target 研究部位，但選定的分鐘估值 tape 沒有 09:01 新成交。
普通 replay／live 缺價仍拒絕；只有具備精確 full-target ABI、09:01 時鐘、
非 synthetic、來源價格與 paper 標記的部位，可以暫沿該部位的來源紙上價格，
估值維持 stale／fresh=0，直到真正的新分鐘價格到達。不偽造成交量或新成交。
147 項聚焦回歸包含此正例及錯誤契約／paper／synthetic／來源的拒絕測試。

修正後新模式 52,110 點、2,064 組必要 symbol-day 均由本機來源覆蓋，
14 組開盤估值使用上述明列的來源紙上價格沿用，沒有 API 請求，
與原回放引擎的權益差異為零，fills SHA 不變。收據位於
`full-0901-candidate-recovery/new_mode_minute_curves/minute_curve_receipt.json`；
此階段固定版改為 `full-0901-valuation-code-release.json`。
原五模式的 additive merge 已通過精確帳戶與 ledger byte-prefix 保留檢查；
六模式完整分鐘另通過下列獨立驗收；正式 promotion／服務／公網驗收分開記錄。

當前工作樹的 dashboard／quote provider／simulation／service ACK 與 supervisor
回歸為 474 passed（`unit-mgda-current-services-accep-20261021T084245-cf8d66e7`）。
只將既有測試的預設範圍期待改為使用者已要求、頁面已實作的「最新交易日」，
沒有因此更改頁面預設或原模型帳本；這項回歸不代替六模式發布後的實測。

六模式來源準備耗時約 27 分鐘後，發現既有五模式有 80 組 1～2 月 symbol-day
未被原 Shioaji 路徑覆蓋。SDK 在此 namespace 的預設狀態目錄解析亦失敗；
明確指定既有 `SJ_HOME_PATH=/root/.shioaji` 後，simulation 登入與 usage 查詢通過，
但 `00883B` 已不在目前券商合約清單，仍不能以該 API 回補。沒有下單或重開原連線。

本機已保存完整 FinMind `TaiwanStockKBar` objects；1/2 的 `00883B` 有 8 個
有效分鐘，首個右標籤是 09:16，而不是 09:01。分鐘估值器新增 local-only 備援，
重用 `normalize_finmind` 的原始時鐘／phase 正規化，核對 receipt 身分、原檔 SHA、
大小、全檔日期／schema、列數及來源未變；不將 native volume 當成股數或執行容量。
既有來源優先，嚴格 per-symbol execution pins 不得改用此備援。這只補來源接線，
不把晚出的價格倒填為 09:01，亦不修改原成交。

實際本機讀回 `unit-mgda-local-finmind-source-r-20261021T091423-8785054f`
已通過來源核對，API 請求為零；接線的 323 項回歸通過。另量測原分鐘鍵轉換
與向量化試驗，100 次分別約 0.376／0.162 秒且輸出完全一致；新增時區、空值、
重複分鐘與零量拒絕回歸後才可使用，這不是全年工作流程加速的驗收數字。

最後七檔回歸為 327 passed（82.37 秒），完整 run 為
`unit-mgda-final-minute-source-re-20261021T091524-b6227d96`。
全工作樹的封存曾因無關程式仍在編輯而拒絕；沒有要求其他工作停止，亦沒有
採用不完整的 release。改以 `scoped-cli-source-receipt.json` 固定本次三個已驗證
CLI 的 SHA，在操作的 private mount namespace 套用，沿用原服務已固定的其他
相依程式；這不是重新 admission 原 daemon，也不複製資料或憑證。
同一固定 namespace 另外完整通過 327 項（52.92 秒），證據為
`unit-mgda-scoped-namespace-regre-20261021T093856-35923d00`；
不是僅用更新中的工作目錄測試來推定發布版本相容。
六模式來源與分鐘驗收由
`unit-mgda-local-source-candidate-20261021T092008-90f7d494` 接續執行，
完成分鐘重建後，在獨立來源審核安全停止；修正與重新驗收見下節。

## 六模式來源與帳務驗收

六模式完整重建為 193 日、312,660 點，必要的 120,757 組 symbol-day
均由本機已驗證來源覆蓋，API 請求為零；其中 80 組使用 30 個日期的本機
FinMind 原始 objects。收據為
`full-0901-local-source-recovery/combined_minute_curves/minute_curve_receipt.json`。
原五模式的 10/19～10/21 有 3,202 個中間估值依來源重新定價，最大權益差
NT$127,528.59；進出場成交、帳戶與已接受端點不變。不能把這個合併驗證
說成所有估值逐點差異為零；新 MGDA 的獨立回放仍是零差異。

來源審核發現 14 筆 full-target 09:01 進場的原始零量 KBar Close，
與後續正成交量的 intraday tape 由不同檔案擁有。舊 auditor 先依盤中 owner
篩選，因而漏掉已固定在 entry book 的開盤來源。修正
`scripts/audit_tw_day_trade_margin_replay.py`，先核對 entry book SHA、原始檔
receipt／SHA、精確標的／09:01／來源路徑、原始 Close 與時間窗，再僅對具備
明確 full-target ABI 與完整 paper 標記的進場接受價格證據。這不證明成交量、
可執行流動性或券商成交；一般進場、沖銷與原成交量驗證不放寬。

操作使用另外固定的四 CLI capsule，未修改原 daemon frozen release；身分為
`scoped-audit-cli-source-receipt.json`。同一固定 namespace 的八檔測試為
374 passed（53.82 秒）：`unit-mgda-source-close-scoped-re-20261021T100522-e5ec9e7c`。
完整六模式來源／帳務驗收亦通過：
`unit-mgda-source-close-full-cand-20261021T100536-c01363c8`，
`full-0901-source-close-validation.json` 記錄 312,660 個分鐘點、
310,344 個已審核歷史內部點、未驗證內部點 0、來源／帳務錯誤 0、
最大帳務差異 0；14 筆來源 Close 研究模擬明列計數。候選通過不代替服務切換。

## 原子切換與消費端驗收進度（18:15～18:36 觀察）

2026-10-21 18:15:24 +08:00 完成 canonical additive promotion；公開帳本現在
包含六模式，MGDA 設定已啟用。原五模式保留於 rollback，沒有第二次交換，
也沒有券商委託。獨立讀回
`unit-mgda-promoted-ledger-indepe-20261021T101735-247aa31b` 證明：
原五模式經濟帳戶與 signals／fills／orders 的完整原始 byte prefix 不變；
新模式 checkpoint fingerprint 為 `08f27475759e`，1,678 筆進場全部是
09:01 且完整標示研究模擬，六模式均已平倉。

切換後排程寫入曾被原服務的唯讀程式 mount 拒絕。操作改用既有
`STOCKAGENT_DISCORD_ENV_FILE` 指向 PID 1 namespace 的同一個原始 `.env`，
已核對 inode／device 與私有權限；不複製憑證、不放寬 production code mount。
canonical consumer recovery 已更新六模式排程並重新連接 Discord，
該階段只有四模式暖機完成：Factorized 與 MGDA 共用投影仍停在 10/20，
不能用 Gateway connected 或 engine active 代替六模式最新日就緒證明。
該次正式操作為 `failed_retryable`、`promotion_completed=true`，當時尚未完成。

投影 admission 原本被 5 秒 inventory 時限擋住，不是磁碟不足；唯讀 census
核對 78,159 個項目、約 13.83 GB allocated、約 241.97 GB 可用。
只將 `maximum_inventory_seconds` 改為 30；200,000 項目上限、25 GiB 可用空間
底線與 generation budgets 不變，未刪任何資料。138 項容量／updater 回歸通過。
canonical completed updater 已由
`unit-mgda-canonical-completed-pr-20261021T102130-35cd99a4` 接手，
使用原始服務固定程式，從正式來源建置 10/21 投影；觀察到從第 0 列重新掃描，
不能把這次成本稱為單日增量。完成後仍須重做同版本六模式暖機與 verify/status。

公網實際瀏覽器第一次在訊號 API 遇到 504：promotion 更換來源 inode 後，
公開程序重新建立約 7.9 GB signals 帳本的日期索引。索引完成後同一筆請求為
200／約 0.45 秒；這是已完成索引的讀取，不是冷建置速度。第二次瀏覽器驗收
停在全期間訊號表的載入逾時；兩次失敗的 managed run log 均保留。獨立公網瀏覽器診斷隨後
讀到 MGDA 的 09:01 回放訊號，沒有 page error，但這不代替五尺寸完整互動驗收。
一次重測誤用第二次的 output directory，已停止該重測；該路徑現為中途結果，
不可當成原失敗 receipt 或完整成功證據。驗收 CLI 現以原子建立全新目錄的方式
拒絕覆寫既有證據，23 項回歸通過
（`unit-mgda-browser-evidence-guard-20261021T103408-f49e7c47`）。
另一個獨立路徑已通過兩種筆電尺寸，之後公網 `api/status` 遇到 ECONNRESET，
亦保留失敗；未把連線錯誤當作 UI 成功。五尺寸完整驗收仍待通過。

後續由受監督工作
`canonical-unit-mgda-projection-c-20261021T103532-9f4f20fd` 等待上述唯一的
canonical projection owner 成功；不另啟來源建置。成功後才執行已發布帳本的
canonical consumer `apply -> verify -> status`，沿用原 frozen code namespace、
精確 checkpoint 與六模式 flat/control-lease gates，不再次交換或重播帳本。
最後重跑公開網站五尺寸互動，使用全新證據目錄。來源失敗、逾時或任何驗收
未通過即停止；完成收據與影像尚須獨立讀回，task 保持 active。

另有切換前既存告警：15:30 分鐘維護工作於 15:53 停在 10/16 的 mutable
research partition SHA 改變，原 receipt 固定 `ec05d4a3…13a9e0`，
15:40 重新發布的 partition 為 `659c4862…17fe3c`。未改寫原 pins、未恢復 alias，
亦未將此告警或整體 degraded 狀態視為修復完成。

## 來源投影與正式消費端驗收（21:47）

上述唯一 canonical projection owner 已成功完成；完整等待 3,631.22 秒，
producer 建置 3,601.04 秒。新 generation 為
`73ae329e58af15e83d7cfdaa42c09b2de6b92f22a78aaf523ca29101d6c843a4`，
共 3,121 日到 2026-10-21、14,726 個模型通道。由於歷史事件日曆或 lifecycle
改變，本次 `reused_rows=0`、`rebuilt_rows=3121`；雖重用 2,882 個 payload、
避免約 2.52 GB 寫入並只新增約 12.56 MB，這不是單日計算增量或效能問題全部解決。

原 Discord owner 在 19:31:18 完成同 run 的六模式暖機，該輪 38.005 秒；
先前 consumer CLI 的 240 秒等待在 native 重試完成前逾時。
沒有保留最初各模式的完整錯誤，不猜測其根因。修正 canonical consumer recovery：

- 先驗證目前 owner 的新鮮 receipt、同一 warmup/run ID、精確六模式逐項 ready、
  engine ACK 與目標 checkpoint，通過即重用；舊 run、漏項、重複、失敗模式一律拒絕。
- public consumer 亦先驗證服務活動、歷史首末日、訊號、六模式與 ledger 同步，
  只有實際未通過才重啟；不為重測無故採用其他工作中的 source 變更。
- 整段持有 canonical control lease；先核對 flat 與已發布分鐘範圍，再改排程。
  每次恢復獨立保存 stage/error 收據；未拿到 lease 不得覆寫其他 owner 的狀態。
- 恢復僅補完已交換版本的 consumers，不重播、再次交換或變更原五模式交易。

最終四個 CLI 固定於 `consumer-recovery-final-cli-source-receipt.json` 所記錄的
唯讀 capsule；切換 CLI SHA 為
`5fe9ab50670c93de606b2d9e83d500968417a5c1755ad3b1118c16a98f1d1310`。
原 daemon source release 不變。同一 native namespace 的九檔後端回歸為
411 passed／40.90 秒（`unit-mgda-final-consumer-native--20261021T134228-ab8faf21`）。
工作目錄新版 browser verifier 的測試另行執行，不要求 frozen daemon 吸收無關工具。

正式流程已完成：

| 步驟 | managed run | 結果 |
| --- | --- | --- |
| plan | `canonical-unit-mgda-final-consum-20261021T134229-503b5062` | 精確身分／193 日／09:01 ABI 已一致，無新交換 |
| apply | `canonical-unit-mgda-final-consum-20261021T134329-59a05cf0` | `complete_with_data_gaps`，21:46:23 完成 |
| verify | `canonical-unit-mgda-final-verify-20261021T134652-1714b47d` | `verified_with_data_gaps`，21:47:14 完成 |
| status | `canonical-unit-mgda-final-status-20261021T134733-a49e0ef9` | durable operation receipt 與 apply 一致 |

沒有再次交換，沒有重啟已就緒的 Discord 或 public service。當前 Discord run 為
`2944880-1792581773524118165`，同 run 暖機為 6/6；engine run 為
`e08e40180894479e85db16f9439e5b52`，六模式、revision lag 0、ledger divergence 0。
暖機成功不代表盤後閒置釋放後仍常駐 GPU，更不證明下一次開盤延遲達標。

21:44 的獨立讀回 `final-promoted-ledger-independent-readback.json` 再次確認：
原五模式帳戶與完整 signals／fills／orders byte-prefix 均不變；MGDA 為精確
`08f27475759e`，1,678 筆進場全在 09:01 且明列反事實模擬；六模式皆 flat。
原 18:15 promotion 時間與 rollback 目錄保留。

另以 `unit-mgda-final-native-gateway-f-20261021T135213-ab477dab` 獨立驗證 API
回傳：native v1 與 gateway v2 各有六模式、193 日、每模式 52,110 點，且每天
時間精確等於 09:01～13:30，合計 312,660 點，沒有缺點、重複或 downsample。
此輪已建索引的讀取分別 1.405／0.331 秒，payload 18.73／17.31 MB；
這是熱來源讀取的單次測速，不是 cold rebuild 或外網端到端速度。

原生 dashboard 的歷史 API 不接受 `encoding=v2`，兩次驗收命令誤送新 gateway
參數而被 400 正確拒絕；未為測試放寬服務 query validation，後續依各自介面驗收。
另一次 Python 公網請求得到 403；同一路徑 curl 與實際 browser 隨後為 200，
沒有據此斷言 WAF 根因已修好或所有客戶端皆正常。

這次 fresh public browser 首輪在全年訊號表等待逾時；後續獨立診斷讀到 100 筆，
訊號 request→response 約 4.09 秒，未穩定重現逾時。為保留可診斷證據，
browser verifier 新增分階段計時、最多 256 個 API 時序事件與失敗關閉前的
表格／篩選／錯誤狀態及截圖；不重試掩蓋失敗，不覆寫舊證據。27 項測試通過
（`unit-mgda-browser-failure-eviden-20261021T135158-d14e2524`）。
最後的五尺寸公網驗收使用全新 `browser-final-instrumented-20261021` 目錄，
結果另列於下節；不以單日 API 成功代替完整互動驗收。

仍忠實保留的非本次新增模式告警包含：舊模式官方開盤價缺漏、1 筆 09:01
來源缺價、1 筆部分建倉，以及今天五模式未準時完成的 opening acceptance。
歷史回放沒有消除這些事實，整體 health 仍為 degraded。

## 最終公網與獨立驗收（21:57）

`unit-mgda-instrumented-public-fi-20261021T135301-11abd1b8` 在真實 HTTPS 網址
完整成功，waited command 93.92 秒；各 viewport 的 16.97～19.27 秒包含身分、
首末訊號、導航、篩選、詳細資料、legend、圖表及截圖，不是單一 API 延遲。

| 裝置 | CSS viewport | DPR | 結果 |
| --- | --- | --- | --- |
| 筆電 | 1366 × 768 | 1 | passed |
| 2K 縮放筆電 | 1280 × 720 | 2 | passed |
| 2K 桌機 | 2560 × 1440 | 1 | passed |
| 平板 | 820 × 1180 | 2 | passed |
| 手機 | 390 × 844 | 3 | passed |

六張模式卡、精確 native checkpoint 身分、1/2 與 10/21 訊號、私有特徵不公開
及研究／非 PIT 提示皆通過；實際點擊模式／日曆篩選、訊號詳細資料與 legend，
canvas 曲線可見，沒有整頁水平溢位或未預期 console/runtime 錯誤。
筆電與手機的曲線截圖已實際檢視。這不證明 WebGL 或券商執行。
全期間訊號表的既有 100,000 筆掃描上限仍明確顯示，不假稱所有訊號均已載入。

完整 receipt 與每個尺寸的圖位於
`artifacts/operations/day-trade-unit-mgda-deployment-20261021/browser-final-instrumented-20261021/`；
receipt SHA256：`734b86f1f040fe939fa7ff4ca8e4b2215f0cb89e51302f74b1e4e24bc54d9c73`。
前三個 viewport 曾讀到 `stale`、後兩個為 `degraded`；成功指互動與本次模式發布
的驗收，不將這些來源健康狀態改寫成正常。前述 timeout／504／ECONNRESET／403
原始失敗收據保留；後續成功不是「已根除所有偶發網路問題」的證明。

瀏覽器完成後，再以同一固定 native capsule 執行
`canonical-unit-mgda-post-browser-20261021T135725-28b46c7f`：
21:57:46 為 `verified_with_data_gaps`，三個正式服務 active、同 run 六模式暖機、
engine 與 dashboard 同步、完整歷史／分鐘驗收均通過。Discord、public、simulation
的 MainPID 分別仍為 2944796／3122280／2773600，NRestarts 都是 0。

本次獨立 MGDA 模式的發布與 09:01 回放驗收完成；不是整個系統／歷史來源
所有告警清零，也不是下一次 09:00 即時成交 SLA 的證明。
小型最終索引為同工作目錄的 `acceptance-final-20261021.json`，固定驗收收據 SHA；
原始模型、來源、帳本、rollback 及失敗紀錄均保留，沒有實單。
