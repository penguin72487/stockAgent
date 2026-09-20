# 13:20 決策、13:30 收盤進場、次一交易日開盤退出

正式設定是
[`configs/markets/tw_overnight_1320_multi_basis_22_capital10m.yaml`](../configs/markets/tw_overnight_1320_multi_basis_22_capital10m.yaml)。
它繼承指定遠端 artifact 的 FinancialTransformer、22 組 temporal bases、lookback 32、
projection-L1、TWD 1,000 萬、兩折佣金、T+2 帳戶、BF16、雙 GPU DDP、1,000 epochs
及 panel-history walk-forward，只改動隔日沖必要的資訊與執行契約。

每個交易日 `t` 的模型輸入由截至 `t-1` 收盤的 23 個日線／官方特徵，加上已完成、
右標的 `t` 日 13:20 一分鐘 Close 相對 `t-1` 收盤的 log gap 組成。模型在 13:20
決定目標權重，先用 13:20 決策價與當時帳戶淨值換算並向下取整為 1,000 股整張
模擬訂單；13:30 再以 `t` 日官方收盤價計算實際成交金額與費稅，下一個交易日以
官方開盤價強制沖銷。若收盤漲價造成資金不足，整數股帳戶按成交時資金限制縮減，
不會事後用 13:30 價格重新放大或縮小原始委託。split 最後一天只處理既有部位，
不新增缺少下一開盤結果的 cohort。

GPU 訓練仍使用可微分的收盤目標權重帳戶作 surrogate；每個 fold 的正式整數股驗收、
持倉與 stitched deployment 則使用上述 13:20 固定張數契約。兩者會在 artifact contract
中分開標示，避免把連續權重最佳化誤稱為逐筆撮合證明。

缺少 13:20 價格時，依使用者指定使用同日收盤價。這些位置會寫入
`overnight_1325_close_fallback_mask` 相容欄位、dataset identity、checkpoint fingerprint
與 run manifest。因為同日收盤在 13:20 尚不可見，這些樣本是帶前視資訊的研究近似；
報表不能解讀為嚴格 13:20 可執行績效。已有但損毀的 manifest、partition SHA、重複列
或錯誤時間仍會 fail closed，不會被缺價政策掩蓋。

跨夜融券需要逐日、逐檔的歷史借券或融券資格、容量與費率。本資料沒有這份證據，
因此模型固定只做多，且不繼承當沖不限量轉融資券。股票賣出使用一般 0.3% 證交稅；
ETF 仍按既有分類費率。日線 OPEN/CLOSE 只代表歷史價格參考，不證明特定委託在集合
競價中的排隊與成交量。

vastai1T 已包成單一入口；它會鎖住同一實驗、要求兩張可見 GPU、執行嚴格 CUDA
環境檢查及完整資料 gate，成功後才進入共同 trainer：

```bash
bash scripts/run_tw_overnight_1320_training_vastai1t.sh --check-only
bash scripts/run_tw_overnight_1320_training_vastai1t.sh
```

正式輸出使用新的 artifact root，不會讀取或覆寫 13:25 實驗及原當沖 optimizer state：

```text
artifacts/markets/tw_overnight_1320_close_fallback_next_open_multi_basis_22_projection_l1_capital10m_v1
```

2026-09-15 的 vastai1T 實例使用隔離 worktree
`/root/stockAgent-overnight-1320-v1`，正式程序由 Supervisor 管理：

```bash
supervisorctl status stockagent_tw_overnight_1320_training
tail -f /var/log/portal/stockagent_tw_overnight_1320_training.log
cat /root/stockAgent-overnight-1320-v1/artifacts/markets/tw_overnight_1320_close_fallback_next_open_multi_basis_22_projection_l1_capital10m_v1/progress.json
```

這台實例的 `/workspace` 與 `/root` 都不保證在 destroy/recreate 後保留；stop/start 可保留目前
狀態。完成的 fold 必須通過 lifecycle 與 checksum 驗收後再走既有 artifact ingress／cold
publication 流程，不能把進行中的 artifact 目錄當成耐久備份。
