# Bybit v7：Fold 5 的末段持倉估值失敗

> 後續使用者已批准公告生效日全額減倉、僅公告退出豁免成交量上限。
> v8 的一般交易上限另依最新更正設為 50%；下文 1% 是 v7 的歷史設定。
> 實作與新訓練入口見 [v8 公告退出契約](bybit_v8_announced_exit.md)。
> 下文保留 v7 調查及當時尚待確認的決策邊界，不改寫舊資料或結果。

## 已確認的原因

使用者提供的 2026-09-26 22:50 UTC traceback 來自 v7 第 5 fold。
`progress.json` 記錄 epoch 36 失敗；`epoch_curve.jsonl` 只完成到 epoch 35。
兩個 rank 同時回報 `CryptoPerpetualDataError`，其後的 `ChildFailedError`
是子程序失敗的結果，不是 CUDA OOM、NCCL collective 錯位或 RNG 恢復失敗。

原始位置 `evaluation chunk rows=[0,267), row=231, symbol_index=157` 對應：

| 項目 | 實際資料 |
| --- | --- |
| 評估窗口 | 2026-01-01 至 2026-09-24，共 267 日 |
| 第一筆失敗 | 2026-08-20 / HFTUSDT |
| policy / force exit | False / True |
| 買入、賣出及開空原始權限 | True / True / True |
| 00:00 執行價 | 0.006446 USDT |
| 前一完成日成交額 | 2,292,212.544646 USDT |
| 1% 退出容量代理 | 22,922.12544646 USDT |
| 該日 price / price-plus-funding forward label | 均為 NaN |

上表成交額保留來源精度；現行 FP32 panel 的對應值為 2,292,212.5，
reference capacity weight 為 0.022922124713659286，之後再除以 carried NAV scale。
此微小數值差異不改變缺值或容量殘倉的判斷。

這排除了「公告旗標漏傳」。現行 forward contract 5 的 `force_exit_mask`
是受容量限制的零目標請求，不是保證全額成交。
本列是執行減倉後仍有非零持倉、卻無法計算持有區間損益，因此帳本拒絕出具報酬。
失敗 epoch 36 的模型沒有存檔；不能用 epoch 35 或 best checkpoint 34
冒充其確切持倉、NAV 或殘倉金額。

禁止新倉的 mask 不能抹除昨天的合約。僅補零、跳過該日、把未成交殘倉清空、
改掉異常捕捉或重跑同一 checkpoint 都不能修復這個會計問題。

## 官方來源重新查詢：先前的空回覆不足以證明無資料

這次縮小至下架以前的實際時間範圍後，官方 API 成功回傳 HFT/VINE 的末段
trade、mark、index Kline 及 funding。舊查詢 `limit=3`、end 在下架後的回覆
仍是空清單，所以「該次 query 沒有回傳資料」不能被解釋成「歷史資料已不存在」。

兩幣 2026-08-20 22:00 至 08-21 08:59 均取得 660 個連續 trade/mark 分鐘；
08-21 00:00 trade open 為 HFT 0.006659、VINE 0.008318。
這些真實價格及對應 funding marks 可以修復 08-20 至 08-21 的持有損益，
但**不能單憑這些數值宣布整個下架帳本已補齊**。

與本地尾段核對，HFT 32 根、VINE 33 根重疊分鐘的 OHLC、volume、turnover
完全一致；實際可補 628／627 根分鐘及 3／5 筆 funding event（均有官方 hourly
mark open）。原始查詢證據已保存在固定路徑：
`artifacts/markets/bybit_perpetual_daily_0000_v7_training_audit/terminal_source_probe.json`。
18 次查詢保留 URL、UTC、原 response、parsed JSON 及各自 SHA-256；收據檔
1,075,230 bytes，SHA-256
`ff9d376d955584846eb83a0a894ce03ba966c819d1a73796624404a3728f36ff`。
來源查詢可由 `scripts/probe_bybit_terminal_sources.py` 查閱；既有收據不會被重跑覆寫。

官方 [HFT 公告](https://announcements.bybit.com/en/article/delisting-of-hftusdt-perpetual-contract--art5f92db89ae63/)
及 [VINE 公告](https://announcements.bybit.com/en/article/delisting-of-vineusdt-perpetual-contract--art131027cca589/)
指定 08-21 09:00 UTC 下架，以之前 30 分鐘的平均 index price 關閉殘倉。
取得 30 根 index 分鐘 OHLC，不等於取得交易所正式使用的結算價：
例如 HFT 的 minute-open 平均約 0.0064792333、minute-close 平均 0.0064806；
VINE 分別約 0.0076174、0.0076065。不可任選其中一個，標成官方精確結算。

Closed instrument metadata 亦確認下架時間。公開 delivery-price endpoint 對兩個
perpetual symbol 回 `10001 / Symbol Invalid`，沒有提供實際結算數字。
官方 [費用說明](https://www.bybit.com/en/help-center/article/Futures-Contracts-Fees-Explained)
另將下架結算費列為 0.05%，與普通 taker 0.055% 不同；若實作正式下架事件，
也須獨立處理此費用，不能只套用普通策略成交。這是目前官方一般規則，
不是這兩個歷史帳戶的個別成交憑證。

因此應分清三個契約：公告後停止新倉、00:00 策略減倉、交易所 09:00 下架結算。
00:00 零延遲訊號也不表示無限流動性或免收費用。

## 本輪修正與未完成邊界

修正範圍先限於錯誤診斷與官方來源證據；未放寬容量、未更換模型、未重啟訓練，
也未改寫 v7 的資料 hash、panel、checkpoint 或原始 immutable release。
錯誤診斷保留未知估值的 hard failure，新增可追溯的日期、合約及估值／退出狀態，
並在跨 DDP 傳遞時保留同一份 structured evidence。

工程回歸：crypto evidence／DDP coordination／accounting／announcement exits／
capacity gradients／daily pipeline／trajectory optimizer／DDP parity、canonical
backtest consistency、best-validation snapshot 及 v7 config 共 **334 passed、
3 skipped**（既有 opt-in CUDA compiled checks，本輪未啟用）。Python 語法及
`git diff --check` 通過。這不是正式訓練、績效改善或來源完整性的證明。
覆核另修正 failure-only 初始 NAV 的 device／scalar shape 對齊，避免診斷本身
遮蓋原始資料錯誤；strict CUDA 通過，完整 evidence 測試（含 CPU 初始 NAV
傳入 CUDA ledger）再驗 **13 passed**。未啟動模型訓練或 optimizer resume。

前次 `--check-data-only` 與合成測試通過，證明結構、來源 hash 與已測試帳本語意，
不代表每一種 learned policy 都有完整的下架估值路徑。此次正式模型遇到的殘倉
就是兩者的差別；v7 不能宣稱已完成全部 folds。

仍須明確選擇：

1. 保持原 1% 容量，修復末段真實來源，並取得可驗證的正式結算證據與相應
   終端帳本；來源未完整以前不應繼續正式訓練。
2. 將「公告生效日強制平倉」明確定義為當日 00:00 真實價格下的全額減倉研究假設，
   僅公告退出豁免容量；正常交易、side permission、費用、00:00 時鐘仍保留。
   這改變成交數量與訓練軌跡，必須新 config／artifact root，不能 resume v7 optimizer。

第二項須由使用者確認，不能偷偷以修錯之名加入。若採第一項，真實來源修補也會
改變資料 fingerprint，舊產物應保留作為原資料版本的證據，不可靜默覆寫。
