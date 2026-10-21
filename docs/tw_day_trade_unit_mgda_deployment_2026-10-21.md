# Unit MGDA 獨立研究模擬模式

## 範圍與精確身分

新增第六個模式 `tw_day_trade_unit_mgda`，初始資金 NT$10,000,000；
不是替換既有五模式，也沒有券商實單。

最新使用者指示：未能在 09:01 前產生的訊號，統一按 09:01 作研究模擬。
本次 MGDA 歷史都是現在另行生成，故新增候選採明列的 full-target 09:01
反事實情境，不沿用分分鐘 50% 成交量掃單。合法資格、整張、價格界線與 NAV
限制不變；不聲稱當時有足夠流動性或真實成交。缺少有效 09:01 來源仍列缺價，
不以 09:09 等未來價格倒填。原始生成時間保留；既有五模式帳本不因此重寫。

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
新的獨立候選正在重建，尚未以測試通過代替發布完成。

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
六模式完整分鐘與正式 promotion／服務／公網驗收尚未完成。

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

另有切換前既存告警：15:30 分鐘維護工作於 15:53 停在 10/16 的 mutable
research partition SHA 改變，原 receipt 固定 `ec05d4a3…13a9e0`，
15:40 重新發布的 partition 為 `659c4862…17fe3c`。未改寫原 pins、未恢復 alias，
亦未將此告警或整體 degraded 狀態視為修復完成。
