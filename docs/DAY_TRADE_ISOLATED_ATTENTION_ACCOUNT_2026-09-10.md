# Attention Full-Then-Last LayerNorm 獨立模擬帳戶

## 1. 執行進度

2026-09-10 11:10 續作：**指定模型的獨立帳戶已接入，歷史與網頁／Discord 驗收通過，整體狀態為 `verified_with_data_gaps`，不是全綠。** 原四個帳戶保留，新帳戶本金 1,000 萬。2/25～9/9 的 135 日訊號、成交、持倉及 36,450 個分鐘點完成；五帳戶合計 182,250 個歷史分鐘點驗收通過。沒有真實券商委託。

本機與公開 gateway 均返回 HTTP 200，看到完整 135 日及 2/25 的 2,746 筆訊號明細；兩端抽驗 `revision_lag=0`、`ledger_integrity.ready=true`、divergence 0。Discord Gateway 連線、22 個 global commands 同步成功，五個模型預熱完成，指定 checkpoint fingerprint 為 `9b5e1f5af112`。本機 autocomplete 與排程狀態通過，但沒有冒用使用者的 Discord interaction，也沒有聲稱已做真人 DM 收件驗收。

今天五帳戶均已持久化訊號。新帳戶及原 100m／舊 v12 使用錯過開盤的 09:00 input → 09:01 來源價格回補；另兩個帳戶保留原本 09:00 的即時最佳報價執行。新模型這次預熱後推論約 0.6 秒，這是單次模型流程測量，不是端到端 Discord 收件 SLO，也不是每天保證值。

最後一組相關回歸測試為 **342 passed / 10.07 s**，含五帳戶冷啟動、跨日殘倉、既有帳本保留、來源前綴、quote scope、tick 與 Discord 排程；不是完整倉庫測試。

## 帳戶與模型身分

- 新帳戶 ID：`tw_day_trade_attention_layernorm`，獨立本金 10,000,000 元。
- 原四個帳戶全部保留，尤其 `tw_day_trade_multi_basis_projection_l1_gelu` 不改名、不換權重、不清空持倉。
- 使用既有引擎的 per-market 本金、持倉、FIFO 與損益隔離；共用行情連線，不另造交易執行器，也不混用帳戶資金。
- 模型產物：`artifacts/markets/tw_day_trade_hybrid_minute_v12_attention_full_then_last_layernorm_commission20_capital10m_all_features_v1`。
- fold 11 checkpoint SHA-256：`12a46eb8d1ceac2a54a3bcfa15250f6ca6eb63d39ffbacb8977772b767a1d012`。
- deployment config：`configs/deployments/tw_day_trade_hybrid_minute_v12_attention_full_then_last_layernorm_fold11.yaml`。
- selector：`services/discord_bot/models/tw_day_trade_attention_layernorm.yaml`。

## 已實測的歷史證據

- receipt-verified 2026-02-25～2026-09-09，共 135 個完成交易日。9/10 盤中不當成已完成日。
- 每日保留實際推論產生時間，另標示歷史 09:00 counterfactual 決策時間。
- 官方 09:00 open 作輸入與定量；09:01 來源分鐘 VWAP，否則該 bar 的 Close 作歷史執行代理。不是即時最佳報價，也不是交易所成交回報。
- 候選共 3,018 筆模擬 fills；其中 605 筆為 09:01 進場。沒有額外不利一 tick 或無來源的尾盤保證成交。
- 保留全部可轉融資融券、零股採整張行情兩項授權研究假設。跨日股數、利息、企業行動及下一日目標差額由既有 paper 引擎處理。
- 36,450 個右標記 09:01～13:30 分鐘估值，1,090 個必要股票日期資料組合均有來源，缺漏 0。無成交分鐘可沿用已觀察價格並標記 stale；不是假造每分鐘都有成交，亦未線性插值。
- 獨立分鐘估值差異點 0，最大絕對尾差 `1.862645149230957e-09` 元。來源／帳務 auditor `error_count=0`，`full_requested_range_passed=true`。
- 9/9 期末模擬淨值 14,586,479.884061657 元，仍有 1 筆研究假設允許的融資融券殘倉。不能稱為完全平倉或實際可保證獲得的報酬。
- 參考曲線：0050、2330 各 36,585 分鐘點；台指近月連續轉倉 40,500 分鐘點。股票參考含 09:00 點，台指為 08:45～13:44，與策略 270 點的時間窗不同。

候選與驗收：

- `artifacts/live/tw_day_trade_attention_layernorm_candidate_20260910/`
- `artifacts/live/tw_day_trade_counterfactual_open_inputs/tw_day_trade_attention_layernorm/backfill_receipt.json`
- `artifacts/operations/tw_day_trade_isolated_account/addition_plan.json`
- `artifacts/operations/tw_day_trade_isolated_account/deployment_progress.json`
- `artifacts/operations/tw_day_trade_isolated_account/status.json`
- `artifacts/operations/tw_day_trade_isolated_account/public_acceptance.json`
- `artifacts/operations/tw_day_trade_isolated_account/discord_acceptance.json`

## 接入安全設計

`scripts/add_tw_day_trade_paper_account.py` 僅接受不存在於舊 state／所有交易紀錄的新 ID。重用 canonical replay validator、writer lock、minute lock、projection 與 atomic directory exchange，不放寬既有 replacement 工具的未平倉拒絕規則。

來源驗收在服務正常運行時完成，之後才暫停寫入。建立完整 successor，逐一核對所有舊帳戶狀態、所有舊 ledger 的原始 byte prefix；新帳戶的紀錄只追加、不改寫舊紀錄。舊的歷史分鐘／fills 必須仍保留原驗收 hash 的完整前綴，後續只能是較新的交易日。

組合後再驗證五帳戶的完整分鐘格線及參考曲線。原 minute receipt 存入 `account_receipts/<新 ID>/previous_minute_curve_receipt.json`；新的組合 receipt 明載兩個已獨立估值的證據來源，不聲稱重新計算舊交易。原 full-replay receipt 保留舊範圍，新帳戶完整 replay receipt 另存；不得把原四帳戶 receipt 冒充五帳戶全量重播。

健康檢查與冷測試改用啟用設定的帳戶集合，避免寫死四個模型。Discord 的明確排程清單也必須加入新 ID，僅新增 YAML 不代表已排程。

第一次接入在 source-stability gate 被攔下，未交換正式帳本，服務自動恢復。原因是 `quote_broker/` IPC 與 `preopen_readiness.json` 仍有獨立寫入者；已將它們明確排除於財務快照驗證。新 root 不搬入 in-flight query，客戶端沿用既有逾時重試；原 IPC 仍留在回復副本。財務、signals/orders/fills/marks、舊 mode 完整相等檢查沒有放寬。

成功交換時間為 11:00:31。完整回復副本：`artifacts/live/account-addition-nbnzvi29/ledger`。第一次未發布候選 `artifacts/live/account-addition-4jtdxud9/ledger` 留作失敗證據，不能當作正式帳本或回復版本。

## 實測額外修正與未解決項目

1. 排程以引擎已提交的交易日為主，不會因最新 pointer 消失而重算；合法、已完成轉換的前日融資融券殘倉可生成下一個訊號，真正未解決的殘倉仍阻擋。
2. 歷史 summary 的實際 `generated_at` 與 `replay_effective_signal_at` 分開，今天產生的昨天回補不再阻擋今日開盤訊號。
3. 新目標為零的舊持股仍要查價。漏跑回補優先用 signal 的開盤價，缺欄位時可用來源行情的 `open`；不能改用 `last`、09:01 VWAP 或自行加 tick 充當開盤價。
4. 09:01 query 完成狀態綁定 `attempted_symbols`；新增模型或持股後，新的 symbol 仍會查詢。已驗證無成交的舊 symbol 不會無限重抓。
5. Quote discovery 不再以初始本金預篩掉可能由複利 NAV 買得一張的股票；最終股數仍交 canonical engine 用凍結 NAV、來源量及資金限制決定。
6. 實時 engine 的 receipt publication clock 與歷史金融事件時間分開，回補 09:01 不再把 11:00 的服務心跳倒填成 09:01。離線重播仍保留可重現時鐘。

**仍需明確保留的降級：** 原 100m 今日 16 檔已被舊的 initial-capital 預篩漏查並記為未成交。尾端 ledger 與 query receipt 對照證實這 16 檔沒有被請求過，不能宣稱來源真的沒有 KBar。程式已修正後續查詢，但沒有把這些已提交的舊帳戶紀錄回寫成成交；若要修正今日已發生的歷史，需要另做保留原證據的當日重播。當日 09:00:15 SLO 失敗亦為已發生事實，不因事後恢復刪除。其他部分成交／容量限制保留可見，不提供市價必定成交保證。

## 尚未完成的訓練一致性

本次是把指定的既有 checkpoint 放到現行 paper 契約重播與執行，**不是把舊 checkpoint 改成按新假設訓練過**。原 tensor 訓練的殘倉帳面清空、T+2 購買力與壓縮退出事件，仍與 paper 的真實模擬跨日庫存不同。尚未完成全套訓練執行器統一或新訓練指令；不能保證原測試集與本次 paper 報酬相同。見 `docs/DAY_TRADE_TRAINING_EXECUTION_PARITY_2026-09-10.md`。
