# 指定 Attention LayerNorm 模型：訓練與網頁執行一致性

## 1. 執行進度

2026-09-10 20:02 更新：使用者拒絕日末殘倉近似後，已新增可微分 FIFO 實體庫存、日曆日融資成本、企業行動權利及 09:00 sizing／09:01 差額交易元件。45 項新測試、合計 293 項相關回歸通過；**完整分鐘 scheduler、來源 tape 與 canonical train/val/test 狀態串接仍未完成，候選設定仍阻擋，未提供假稱一致的訓練指令**。詳見 [跨日 tensor 實作與驗收邊界](DAY_TRADE_FIFO_TENSOR_IMPLEMENTATION_2026-09-10.md)。沒有改動正式網頁帳本或重啟服務。

2026-09-10 11:10 更新：指定產物已經 Syncthing 驗證取得，並依使用者同意，以新 ID `tw_day_trade_attention_layernorm` 接入獨立 1,000 萬模擬帳戶，原四帳戶保留。2/25～9/9 共 135 日／36,450 分鐘點完成，網頁與 Discord 模型身分及同步驗收通過。今天訊號已登記；原 100m 的 16 檔漏查紀錄及已發生的開盤 SLO 失敗仍保留為降級。**訓練／paper 全套執行器統一與新訓練指令仍未完成**。完整證據見 [獨立帳戶部署紀錄](DAY_TRADE_ISOLATED_ATTENTION_ACCOUNT_2026-09-10.md)。以下為各時間點原始記錄，舊的「尚未同步／尚未接入」不代表目前狀態。

2026-09-10 09:55 續作：使用者已指定來源 **vastai1T**，並確認**保留全部可轉融資融券、零股按整張行情成交**兩項研究假設。已完成遠端產物 lifecycle 驗證、共用計價／利息原語與股票／ETF tick 修正，350 項相關測試通過（10.31 秒）。**尚未同步指定產物、切換網頁、統一跨日訓練執行器或重算全期結果；沒有啟動訓練、重啟服務或覆寫正式帳本。** 後續新增測試結果見第 7 節。

以下 09:32 記錄保留為原始診斷；遠端來源已由第 7 節的新證據補足，不再需要使用者提供機器名稱。

2026-09-10 09:32（Asia/Taipei）查核結果：**已完成模型身分防錯、執行差異診斷及 117 項相關回歸測試；尚未完成訓練／網頁執行器統一、指定模型全期重算或部署。**

使用者指定的完整產物為：

```text
artifacts/markets/tw_day_trade_hybrid_minute_v12_attention_full_then_last_layernorm_commission20_capital10m_all_features_v1
```

penguin 本機尚未找到該產物。已查本機 `artifacts/markets`、`/srv/stockagent-artifacts-hot/markets`，並在 `/srv/stockagent-packed` 的 manifests、heads、`.local-state` 搜尋其確切名稱；沒有取得指定 checkpoint 或測試集結果。這不代表已證明其他機器沒有，也不代表整個 Syncthing 資料夾已完成同步。

官方 receipt 驗證的可比較期間為 **2026-02-25～2026-09-09，135 個已完成交易日**。09/10 尚在盤中，不能充作已完成日的測試結果。當時正式 paper 帳本仍有未平倉；本次沒有切換模型、覆寫帳本、重啟服務或下真實委託。

先前零股按整張行情、融資融券跨日差額調整的修復只完成在 paper 路徑，**不能當作 tensor 訓練執行器已同步修正的證據**。本文件補充並釐清先前 YTD 修復紀錄的這項邊界。

## 2. 首先不是同一個模型

必須追蹤 `market -> selector -> resolved config -> fold -> checkpoint hash`，不能只比較網頁名稱或 YAML 表面值。

| 身分 | 使用者指定 | 現行網頁實際選中 |
| --- | --- | --- |
| 設定檔 | `configs/markets/tw_day_trade_1m_hybrid_v12_attention_full_then_last_layernorm.yaml` | `configs/deployments/tw_day_trade_hybrid_minute_v12_layernorm_fold11.yaml` |
| 產物 | 上述 `artifacts/markets/...all_features_v1` | `artifacts/ablations/tw_day_trade_hybrid_minute_v12_reference_architecture_checkpoint_finetune_ofat_v2/layernorm` |
| temporal pooling | `attention` | `last` |
| temporal query | `full_then_last` | `last_only` |
| normalization | `layernorm` | `layernorm` |
| 特徵數 | resolved config 為 99；未驗證其缺失 checkpoint | 99 |
| checkpoint | 未取得 | `fold_11/checkpoint_best.pt` |

現行穩定 market ID 是 `tw_day_trade_multi_basis_projection_l1_gelu`，selector 為 `services/discord_bot/models/tw_day_trade_multi_basis_projection_l1_gelu.yaml`。09:32 讀得目前 checkpoint SHA-256：

```text
fbb4fbdcd2fab3c47bf4aee84b6c345f1361605288cd1348e320705d6ee317d1
```

Discord warmup 記錄 fingerprint 為 `4307f9d1b764`，亦指向目前 OFAT 模型，不是指定的 attention 模型。`ready` 只表示該 warmup 狀態，不能證明指定模型已部署、今日執行正常或兩端報酬一致。

## 3. 同參數名稱不代表同執行規則

本表比較目前程式的 `scheduled_events_50pct` 訓練路徑與已啟用的 paper 契約。指定 checkpoint 不在本機，因此尚不能證明它當時保存的程式版本與目前訓練路徑完全一致；必須再核對原始 run manifest／mode contract。

| 項目 | 目前 tensor 訓練執行 | 現行 paper／歷史回補 |
| --- | --- | --- |
| 每日目標 | 每日開盤決策一次 | 每日開盤決策一次；持有殘餘時交易目標與庫存的差額 |
| 下單資金 | 已交割 cash；T+2 待收款尚不能增加當次股數 | 凍結當日 economic NAV 的總曝險預算；不是已驗證券商購買力 |
| 歷史入場 | 官方 open 定股數，09:01 分鐘 VWAP／Close、50% 分鐘量整張容量 | 同類歷史價格及容量契約；仍須逐筆驗證是否相同 |
| 即時入場 | 歷史分鐘代理 | 訊號產生後可觀察的因果最佳報價；不能回填成早已完成的分鐘成交 |
| 盤中保護 | 此壓縮事件路徑沒有全日逐分鐘止盈／止損／價格限制前一檔流程 | 有盤中保護流程 |
| 13:20 限價 | 用 13:20 Close；13:21～13:23 嚴格穿價才按限價計成交 | 依 paper 委託事件與來源判定，包含跳空價格處理 |
| 13:24 市價 | 消耗右標記 13:24 bar，其區間已在送單時結束 | 13:24 發單後，歷史最早用右標記 13:25 bar |
| 後續退出機會 | 壓縮 tape 只處理 13:24、13:25、13:30 | 按可取得的後續來源分鐘／集合競價事件處理 |
| 13:30 殘餘 | 收盤估值、計退出費與一日融資／借券成本後，股票帳面清空，只保留 T+2 淨現金差額 | 保留真實模擬股數，隔日承受漲跌並按持有天數計成本；按新目標減庫存調整 |
| 零股與企業行動 | 此訓練狀態沒有跨日股票庫存 | 採使用者授權的零股按整張行情假設，處理企業行動、零股庫存及權利 |

原始碼定位：

- `stockagent/backtest/tw_day_trade_minute.py`：`deployable_twd = capital0 * cash`、scheduled 退出事件、residual accounting close，以及 `run_tw_day_trade_minute_execution` 的 flat-stock-book 契約。
- `stockagent/data/tw_day_trade_execution.py`：第 264／265 分鐘分別寫入 `MARKET_VWAP_1324`／`MARKET_VWAP_1325`，目前為 26 欄事件 tape。
- `stockagent/training/trainer.py`：`_mode_artifact_contract_for_config` 保存 `flat_after_1330_margin_conversion_with_t_plus_2_net_claim`。
- `stockagent/live/tw_day_trade_simulation.py`：`session_sizing_nav_twd`、`_convert_residual_to_carry` 與跨日庫存調整。

因此不能把「一樣是 1,000 萬、手續費兩折、50% 成交量」解讀成同一回測。也不能僅把網頁 NAV 調高，或把真實跨日庫存清空來追上原始研究數字。

## 4. 實際呼叫兩套執行器的受控反例

這不是指定模型的 YTD，也不是市場資料回測。使用相同的可重現人工輸入，分別呼叫既有 tensor 執行器及 canonical paper engine，以隔離演算法差異；沒有另建一套計算器充當結果。

| 輸入 | 值 |
| --- | ---: |
| 起始資金 | 10,000,000 元 |
| 第一日目標權重／進場價 | 0.2／1,000 元 |
| 兩端第一日實際進場股數 | 2,000 股 |
| 第一日所有退出成交量 | 0 |
| 第一日收盤估值 | 1,000 元 |
| 第二日目標權重／開盤及執行價 | 0／900 元 |
| 一般手續費與交易稅 | 兩端皆設為 0；保留融資成本 |

實際輸出：

| 輸出 | tensor 訓練 | paper |
| --- | ---: | ---: |
| 第一日後是否仍持有股票 | 否 | 是，2,000 股 |
| 第二日結束淨值 | 9,999,473.690987 | 9,799,473.972603 |

差額為 **199,999.718384 元**。主要來源正是 `2,000 × (900 - 1,000) = -200,000`：paper 持有的股票遭遇隔夜下跌；tensor 前一日已帳面結清，不再承擔股票價格變化。小額尾差涉及浮點／成本計算，並不改變這個 20 萬元量級的機制。

這已反證「同一批目標、價格和一般費率必然產生相同 NAV」。但尚未量化這個因素在使用者指定模型 135 日內的貢獻，不能把本反例數字套到真實 YTD。

機器驗證產物：

- `artifacts/audits/day_trade_training_parity_20260910/identity_and_scope.json`
- `artifacts/audits/day_trade_training_parity_20260910/residual_counterexample.json`
- `artifacts/audits/day_trade_training_parity_20260910/regression.log`

JSON 為程式驗證 receipt，不是另建網頁筆記；說明紀錄僅有本 Markdown。

## 5. 本次已實作的防錯與測試

在既有 `scripts/switch_tw_day_trade_strategy.py` 新增可選的 `--expected-artifact-root`。指定時，在載入替代 experiment、計算日曆、存取帳本之前，強制確認：

1. 使用者指定的產物目錄存在。
2. selector 解析後的 output directory 就是該目錄；相似名稱或相同 normalization 不算一致。
3. fold 明確、checkpoint 指向該產物的 `fold_XX/checkpoint_best.pt`，且檔案存在。
4. 保存 checkpoint SHA-256；身分通過仍記錄 `training_execution_parity_proven: false`，不能把身分相同當成執行相同。

保留既有 lifecycle、分鐘完整度、未平倉及原子 promotion 門檻。這是誤部署防護，**不是已完成核心執行器統一**。

已執行以下唯讀命令，正確以 `FileNotFoundError: requested training artifact is unavailable` 拒絕，不拿目前 OFAT 代替：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/switch_tw_day_trade_strategy.py plan \
  --market-config services/discord_bot/markets/tw_day_trade_multi_basis_projection_l1_gelu.yaml \
  --start-date 2026-02-25 --end-date latest \
  --expected-initial-capital 10000000 \
  --expected-artifact-root artifacts/markets/tw_day_trade_hybrid_minute_v12_attention_full_then_last_layernorm_commission20_capital10m_all_features_v1
```

相關測試 **117 passed / 5.97 s**：

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q \
  test/test_switch_tw_day_trade_strategy.py \
  test/test_tw_day_trade_minute_execution.py \
  test/test_day_trade_margin_carry.py \
  test/test_day_trade_execution_reconciliation.py \
  test/test_promote_tw_day_trade_replay.py
```

其中新增 5 項身分檢查測試，涵蓋缺失產物、相似但不同模型、錯誤 checkpoint、相同路徑別名及先於其他處理拒絕。Ruff 與這次程式變更的 diff 檢查通過。沒有執行完整倉庫測試、訓練 epoch 或指定模型全期推論。

## 6. 繼續完成所需證據與驗收

使用者已確認產物位於 vastai1T，遠端查核見第 7 節。仍需使用 Syncthing／經驗收 publication 流程取得完整已完成產物，不用 SSH 複製、猜測替代 checkpoint 或將同名設定檔冒充訓練成果。

需要的不只是 `.pt`：亦包含 resolved config、run manifest、fold completion、mode contract、測試集逐日結果及 requested weights（若有）、資料 release／feature schema／ordered symbols 等可重現證據。先核對完整產物，再決定可用的確切 fold 與測試所有權日期，不能預設現行 OFAT 的 fold 11 就是指定模型的正確選擇。

拿到原始產物後，尚待實作／執行：

1. 固定 exact checkpoint、資料版本、ordered symbols、特徵窗口、資格及方向遮罩，核對逐日原始分數與目標權重；區分原始 walk-forward 測試與單一部署 fold 重播。
2. 以已授權的可執行假設為共同契約，補齊 canonical 訓練／評估執行器的跨日股票、成本與企業行動狀態，以及完整因果退出事件；重用既有訓練 lifecycle／checkpoint 基礎設施，不另起平行 trainer。
3. 對相同事件輸入、相同起始 NAV／庫存／T+2 佇列，逐日比較 weights → 股數 → fills → 費用 → 持倉 → NAV。離散交易必須一致；浮點帳務使用事先明示的小額容忍度，不能只比最後報酬或曲線形狀。
4. 使用新 execution contract／新產物版本重算 02/25～最新已完成日。保留原始訓練測試結果作為舊契約證據；若原假設不可執行，新報酬可以降低，不能承諾維持原先較高數字，也不能偽稱已按新契約訓練過。
5. 每個完成日驗收 270 個右標記 09:01～13:30 來源分鐘；這只證明分鐘覆蓋，另外還要驗證未成交與殘餘持倉。候選驗證後，僅在符合既有未平倉安全門檻時升版，保留 rollback，再檢查網頁／Discord 同一 revision 與計算結果。

最後的「相同」必須定義清楚：**相同歷史資料、權重、事件、帳戶起始狀態及執行器的重播應一致**；即時最佳報價與 09:01 分鐘 VWAP 不是相同輸入，不能承諾其真實結果逐分完全相同。區間報酬亦必須共用期初錨點，不可一邊從首個已扣費分鐘重設為零，另一邊從扣費前本金開始。

## 7. 已確認的研究假設、遠端產物與 tick 修正

### 使用者決定與共用帳務

保留 `assumed_all_marginable_residual_next_signal_delta_v1` 與
`assumed_odd_lot_at_regular_board_price_v1`，不是改成缺少授信／券源／零股行情就一律拒絕成交。這兩項仍是研究假設，並非券商承諾。

新增 `stockagent/backtest/tw_day_trade_contract.py`，集中既有時鐘、參與率及融資／借券假設。paper 的未實現淨損益與跨假日利息現在呼叫同一組純量／tensor 相容原語；舊分鐘訓練器沿用同值成本常數，**沒有改寫其殘倉結清、T+2 狀態或舊 checkpoint 語義**。

測試包含多空、751 股零股、企業行動後成本基礎、進場淨費用／退佣不得重複扣除、三個日曆日利息與 tensor 梯度。這些是共用數學的證據，不是完整訓練／paper 逐筆對帳已通過。

### vastai1T 的唯讀查核

使用 SSH 僅讀取遠端驗收資訊，沒有透過 SSH／rsync 搬運模型檔案。

- 指定產物：493 個檔案，1,248,531,869 bytes；canonical `validate_cold_artifact_source` 的 `training-lifecycle-v1` 檢查通過，fold 1～11 完成，當次 process references 為空。
- fold 11：訓練年度 2014～2024、驗證 2025、測試 2026；測試實際範圍 **2026-01-02～2026-08-19，151 筆**，不是已更新到 09/10 的測試集。
- checkpoint：`fold_11/checkpoint_best.pt`，7,577,585 bytes，SHA-256 `12a46eb8d1ceac2a54a3bcfa15250f6ca6eb63d39ffbacb8977772b767a1d012`。
- manifest 確認 `financial_transformer`、`attention`、`full_then_last`、`layernorm`、`raw_features`；其 mode contract 仍明確是 `flat_after_1330_margin_conversion_with_t_plus_2_net_claim`。
- 新增未啟用的候選 deployment config：`configs/deployments/tw_day_trade_hybrid_minute_v12_attention_full_then_last_layernorm_fold11.yaml`。正式 selector 完全未改。

這不是跨機同步驗收：尚未確認全部檔案的冷庫 object hash、Syncthing convergence、penguin materialization 或本機 checkpoint forward。vastai1T 是 index-only 接收節點，不能直接發布 canonical cold；現有 ingress 以 rsync 傳送，與使用者要求的 Syncthing 不符，故未執行。若採新增僅含指定模型封裝的 Syncthing 暫存通道，需明確登記 scope、先進隔離區、驗收後才由 penguin 發布，不可將 vast 的整棵 artifacts 加進 hot folder。

當次正式帳本讀得目標市場仍有 2 筆未平倉，所有模式合計 94 筆；該 state 的 updated_at 是 09:01。依策略切換安全門檻不能強行 promotion；這個讀值也不能當作當下行情／服務健康證據。

### 股票／ETF 的合法價格，不等於固定小數位

依[證交所交易制度與升降單位表](https://www.twse.com.tw/zh/products/system/trading.html)，股票與 ETF 必須分開計算 tick；ETF 小於 50 元為 0.01，50 元以上為 0.05，不能套普通股票六級距。ETF 的每日限制還依商品類型而異，不可從 tick 表推定全部都是 ±10%。

已實作：

1. 在 canonical `tw_price_rules.py` 新增商品類型參數，修正 paper 初始／跨日 bracket 與歷史 13:20 被動掛價。股票 1,000 往下一檔為 999、上一檔為 1,005；ETF 50 往下一檔為 49.99、上一檔為 50.05。
2. quote-to-paper 邊界驗證 bid／ask／上下限的合法格點；拒絕不合法價格並列出欄位，不替它造一個四捨五入後的行情，也不修改 provider 原始陣列。僅浮點表達誤差可在驗證後正規化到分。
3. 即時 snapshot 與 Shioaji reference fallback 不再為 ETF 猜測普通股票的 10% 上下限；缺少來源時維持缺值，不擅自偽造可執行邊界。尚未因此修補或重新發布歷史上下限資料集。
4. 分鐘 VWAP、成本基礎、權重、費用與 NAV 不強制取至 tick。平均價格可能由多筆合法價成交加權而得，比單筆報價的 tick 更細是正常的；量化平均價會直接扭曲 PnL。
5. 新增 `order_price_contract_version=1` 到新寫出的服務狀態及 replay receipt；舊 stock panel price-rule v3 保留。strategy-switch 的 `already_current` 判斷必須見到正確的整數版本，不能把舊曲線直接視為已修正；尚未重新驗收舊歷史輸出為這個版本。

驗證：350 項相關測試通過（包含新增 13 個格點／ETF／原始資料不變測試，以及實際 engine ETF bracket 測試），Ruff 與 `git diff --check` 通過。合成 2,749 檔、30 次 quote-map 測量中位數 11.75 ms、P95 16.84 ms；包含完整轉換，**不含網路與模型推論，亦不是相較舊版的速度改善證據**。未進行正式服務 restart、真實委託、全市場完整 epoch 或 02/25 至今的重新 promotion。

### 訓練指令狀態

另外執行 checkpoint manifest、resume early-stop、completion-marker 三個測試檔：110 項通過（9.44 秒）。連同上述執行／tick 測試為 460 項。其後新增 5 項價格版本驗收案例，strategy-switch 測試檔 13 項全部通過（2.27 秒），累計不重複案例 465 項。350 項執行回歸亦再次通過（7.66 秒）。不包含正式模型訓練 smoke 或完整 epoch。

現有 attention YAML／dual-5090 launcher 仍代表舊契約，不能把它們列為「已與網頁跨日庫存一致」的新訓練指令。尤其不可在 penguin 執行會重指向 live `data_tw_public` 的既有 launcher。

新版訓練指令需等第 6 節第 2～3 項的 canonical 跨日股票狀態、成本／企業行動及逐筆對帳完成，再給新的 output root、資料 release 與 execution fingerprint。此次**尚未完成新版訓練設定／指令**；共用常數、通過單元測試與候選 deployment config 都不能取代這個實作。

## 8. 使用者批准後的指定模型同步結果

2026-09-10 已完成狹義 Syncthing quarantine transport：vastai1T 指定完整產物
493 files／1,248,531,869 bytes 已逐檔驗證，經 penguin 發布 canonical cold release，
再安裝至本機原指定 artifact 路徑（0 replaced／0 conflicts）。11 folds lifecycle
通過，fold 11 SHA 與第 7 節相同；本機 strict load + BF16 合成前向亦通過。

正式模型**尚未切換**：canonical promotion flat gate 看到四個 modes 共 94 個
非零持倉而拒絕；保留原 selector、帳本及服務。模型安裝不等於策略 promotion。
本節更新第 7 節「尚未收到本機」的狀態，不改寫舊訓練契約不一致的結論，亦未
完成 02/25 至今回補或新訓練指令。完整證據與可重跑命令見
[Syncthing ingress 驗收紀錄](ARTIFACT_SYNCTHING_INGRESS_2026-09-10.md)。
