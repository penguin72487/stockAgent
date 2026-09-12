# 第 5 折美律 MKF 合約調整修復（2026-09-10）

## 已定位的失敗

vastai1T 的 v14 第 5 折在 epoch 77 訓練時，於 `2024-08-30 / MKF:202409 / 2439` 持有多單 1 口，觸發 `unresolved_corporate_contract_transition`。DDP 的 `ChildFailedError` 是外層結果。原始第 76 epoch checkpoint、best checkpoint、曲線及前三／四折完成證據已備份，沒有覆寫。

原 v14 的各折已保存 epoch 數為 513、146、109、139、76；前四折有完成標記。修復作業目錄為本機與遠端 `artifacts/operations/futures_mkf_repair_20260910/`。

## 合約與來源

依[期交所美律契約調整公告](https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/2439_20240830.pdf)（2024-08-20 公告、08-30 生效），舊 MKF 轉為 MK1，乘數仍為 2,000、持倉口數一比一轉入。既有多單每口權益加 8,297 元，空單扣 8,297 元；調整後合約另含認股權價值。

沿用共用持倉轉換及現金權益記帳，不新增成交、轉倉費用或訓練器。資料 builder 在調整目的合約的當日現金欄寫入公告整數金額；executor 先轉移舊持倉，再對其計算一次有號調整。新開標準合約不領取舊持倉權益，隔日金額歸零。schema 1 的舊零現金轉換規則保留，schema 2 才接受有公告來源的現金股利加認股權轉換。

公告列出五個交割月；原始前日候選僅可能持有 202409。因此只新增該實體合約自 08-30 至 09-18 到期的 13 個交易日，保留原候選、1,573 個日期、98 個股票特徵與既有交易時鐘。官方[2024 年 9 月最後結算表](https://www.taifex.com.tw/cht/5/sSFFSP?down_type=1&queryYear=2024&queryMonth=9)的每口完整價值為 254,130 元，不能只用 `126.59 × 2,000 = 253,180`；多出的 950 元包含認股權價值，與 08-30 的現金調整分開記帳。

新版不可覆寫來源：`artifacts/data_preparation/futures_corporate_transitions_v6_20260910/manifest.json`，SHA-256 `00a7dbf263228340e4a02ae30b7349626d14589ea8be3671fa2c1e5024ada523`。共 147 個已核實來源的合約日、42 個執行分鐘觀測，完整保留 v5 的來源。

### 不能混稱為逐筆完整的 1 口差額

- 其餘 12 天共 405 筆 exact Tick，逐日 OHLC 與扣除價差腿後的官方一般成交量完全相同。
- 08-30 的 226 筆 exact Tick 及 73 根 exact KBar 都合計 314 口；每分鐘的 VWAP、最高、最低、收盤及成交量全部一致，官方日報則是 315 口。
- 重新查詢官方當日日報仍為 315 口；鉅額交易資料未列 MK1，歷史官方逐筆下載端點未提供 ZIP。因此差額原因仍未知，不能稱為已補齊的 1 口。
- 依既有 `ExactMinuteRecovery` 的 `exact_month_kbars_within_official_range_observed_volume_only` 規則，這天使用日期及實體身分核實、Amount/Volume 合法的 73 根 KBar；保留 Tick 作逐分鐘交叉核對。**只用 314 口觀測量，不將日報差額分配到分鐘、不增加成交能力。** 沒有放寬嚴格 Tick 量比對規則，也沒有新增隔離。
- `mkf_observed_volume_audit.json` 明載 `unexplained_volume_gap: 1`、`trade_completeness: not_proven`。來源可供既有觀測容量研究，不代表已取得交易所每一筆成交。

## 驗證

嚴格 CUDA 檢查通過；美律專項 14 項測試通過，涵蓋多空現金調整一次性、空倉無舊權益、分段持倉、CUDA 編譯值與梯度、舊來源保留及不相容 optimizer 拒絕。37 項相關回歸、111 項共用 checkpoint／resume 測試通過，修改程式 Ruff 通過。

- 原第 5 折 last（epoch 76）與 best 模型各完成全部 1,135 天訓練、243 天驗證、163 天測試回放，共六次；不載入舊 optimizer，也不宣稱重現含 dropout 的 epoch 77 隨機更新。
- 真實資料多空各 1 口探針確認 MKF 轉為 MK1、權益差額為正負 8,297 元、到期後無殘餘口數；既有隔離日保持零成交與官方結算記帳，下一交易日恢復原規則。
- 兩張 RTX 5090 以直接 `train.py` 完成第 5 折 3 個完整 epoch、全部最終測試及九張必要累積報表。這是獨立目錄 `fold5_acceptance` 的有界驗收，正式 YAML 的 1,000 epoch 設定沒有改動，正式輸出尚未建立。
- epoch 1 完整流程約 30.23 秒，epoch 3 約 5.39 秒；optimizer 步數為 3，BF16 scaler 為空，沒有資料錯誤收據。這些是工程驗證，不能視為策略績效或全部折已跑完。
- `verify_acceptance.py` 呼叫共用完成契約檢查通過，缺漏與非法產物皆為零，核對 29 份既有訓練產物／入口／設定雜湊不變。`final_acceptance.json`、`checkpoint_replay/receipt.json` 保存詳證。
- 再次執行相同驗收命令後，正確略過相容的已完成折；沒有新增訓練子程序或重複 epoch，checkpoint 雜湊不變。`handoff_verification.json` 記錄交付時沒有驗收訓練程序、正式 v15 輸出尚未建立。

程式修改部署在遠端；本機較舊的主訓練程式沒有覆寫。作業目錄 `patched/` 與 `code_final.patch` 保存可審查的變更，`verified_remote/` 保存已逐檔核實雜湊的來源副本及遠端驗收收據。這是本次檔案交付證據，不代表 fleet Syncthing 或既有 cold release 的完整性問題已解決。

其餘尚未支援的潛在公司行動事件仍有 206 個；實際持倉碰到仍會拒絕，不以零報酬或新增隔離掩蓋。

## 遠端入口

v15 保留 v14 的參數與現金／隔離規則，換用新來源與獨立輸出。正式上限仍為 1,000 epoch；新來源不能直接載入 v14 optimizer，舊結果保持可重現。

```bash
source scripts/runtime_env.sh
run_fintech_python train.py --config configs/markets/tw_stock_futures_day_trade_0845_carry_v15.yaml
```

正式輸出：`artifacts/markets/tw_stock_futures_0845_carry_v15_vast5090`。
