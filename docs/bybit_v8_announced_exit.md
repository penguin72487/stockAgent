# Bybit v8：使用者批准的公告日全額減倉

## 研究假設與不變項

使用者明確批准「允許公告生效日全額平倉，其他交易仍受原上限限制」，
隨後更正一般交易上限應為 **50%**，不是原先繼承的 1%。
新設定為
`configs/markets/bybit_perpetual_daily_0000_historical_public_pit_score_cash_trajectory_v8.yaml`。
它繼承 v7，只改實驗名稱、訓練輸出根與以下兩個交易設定：

```yaml
trading:
  max_volume_participation: 0.5
  crypto_announced_exit_unlimited_volume: true
```

每日 00:00 UTC 決策、零延遲訊號、同時點官方 1m Kline open 執行代理、可跨日
留倉、初始參考本金 1M USDT、每側 0.055% 費用，
以及 v7 的模型／輸出、學習率、trajectory optimizer、BF16/DDP、1000 epochs
上限都不變。這是歷史研究成交近似，不是保證市場有無限深度。

普通交易每幣每次調倉的名目金額上限為 **前一個已完成 UTC 日官方 USDT
成交額 × 0.5**，涵蓋一般開倉、加倉、減倉與反向；不是持倉占 NAV 的 50%，
也不是執行分鐘或未來當日的全天成交量。沿用原始容量口徑與 live NAV 換算，
只把比例從 0.01 改成 0.5；零容量仍不得一般交易。日成交額代理也不代表
00:00 當下確有相同可成交深度。

| 合約 | 公告發布日 | 第一個可知後的 00:00 UTC 決策 | 原資料的執行價（USDT） |
| --- | --- | --- | --- |
| HFTUSDT | 2026-08-19 | 2026-08-20 | 0.006446 |
| VINEUSDT | 2026-08-19 | 2026-08-20 | 0.007486 |
| ICXUSDT | 2026-09-16 | 2026-09-17 | 0.010680 |

日期規則沿用原本的保守公告契約：公告只有日期，故隔日決策才可使用。
同一規則也處理 ICX，不另外為 HFT/VINE 寫績效導向特例。
三筆執行價與原始買／賣權限已在現有 v7 資料及分鐘來源核對。

## 帳本精確邊界

1. 有日期與官方來源的 `force_exit_mask` 把該合約目標設為零，不能開新倉或反向。
2. 僅此公告退出的 volume ceiling 豁免；未公告部位仍受 50% 容量限制，不把未用額度
   重新分配給別的幣種，也不改動一般風險減倉規則。
3. 原始交易權限、真實可執行價要求、普通買／賣費用及換手限制不豁免。
   本設定原本 `max_turnover_ratio: 0`，即未另設換手上限；因此這三筆可全額平倉。
4. 午夜 funding 歸舊持倉，在前一持有區間 `(start, end]` 入帳後，才處理新目標。
   平倉日只扣實際減倉交易費，不收已平掉部位之後的 funding 或價格損益。
5. padding 不執行，破產帳戶不復活。若權限阻擋而留下未知估值的持倉，仍拋
   `CryptoPerpetualDataError`；缺值本身永遠不是退出事件。

這不是 HFT/VINE 08-21 09:00 或 ICX 09-18 09:00 的交易所下架結算。
不採用未知正式結算價，也不把分鐘 index 均價或最後成交價冒稱結算價。
背景、官方來源與 v7 失敗原因見
[v7 末段估值失敗調查](bybit_v7_terminal_valuation_failure.md)。

## 版本、資料及保留方式

- opt-in 帳本契約為 **6**；未啟用時仍保留 **5**。梯度契約仍為 **1**。
- trading fingerprint 與 mode artifact 記錄豁免規則；v7/v8 的 optimizer resume
  及舊報表混用均被拒絕。v8 自己後續的同契約 resume 仍可用。
  `max_volume_participation` 也在既有交易 fingerprint 內；1% 與 50% 不能混續。
  更正比例前確認 v8 僅有資料預檢的 `startup_timing.jsonl`，沒有 checkpoint、
  訓練曲線或執行中的 Bybit 工作，所以直接修正尚未訓練的 v8，沒有另建 v9。
- 使用同一個 `artifacts/cache/bybit_perpetual_daily_0000_repaired/perpetual_daily`
  及原 panel cache，不新增 snapshot、不重建時鐘、不複製 raw／public／daily 資料。
- 新訓練輸出為
  `artifacts/markets/bybit_perpetual_daily_0000_historical_public_pit_score_cash_trajectory_v8`。
  v7 的 source hashes、快取、曲線及 checkpoint 都保留。
- 前次找到的 terminal API 補件仍作為來源調查證據保留，不偷偷寫入 v7 輸入。
  此次策略已在未知尾段之前、以既有真實價格清倉，故不依賴那些未知尾段。

## 執行方式

由使用者執行正式訓練；不要把 v7 checkpoint 複製進 v8 根目錄：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/markets/bybit_perpetual_daily_0000_historical_public_pit_score_cash_trajectory_v8.yaml
```

若僅要檢查資料，在最後一行加 `--check-data-only`；不要額外包一層 `torchrun`。
舊結果不會變成新規則的有效績效。這次變更解決獲批退出假設與帳本實作的差異，
並不保證每一 fold 報酬提高；仍需使用者完成全折訓練後再評估。

## 50% 更正驗證（沒有正式訓練）

- 既有 canonical 容量 helper 已參數化，不修改帳本公式；訓練、分塊驗證、
  最終回測及 stitched replay 使用同一設定。
- v8 與 v7 的完整有效設定比對僅差實驗名稱、輸出根、公告退出開關與參與率。
  v7 保留 1%，v8 為 50%；資料／panel cache 路徑完全相同。
- 分塊回測測試同時涵蓋 1%／50%、三種 chunk 大小與兩種 history 模式，
  一般入場確實觸及容量限制，公告退出才豁免；與單次 canonical 帳本一致。
- 同一 v8 規則僅改參與率時，1% 與 50% 的 resume／artifact 相容檢查
  雙向拒絕；相同 50% 契約可續跑。資料 fingerprint 不變。
- 設定、checkpoint、整合 wiring、公告退出、帳本及容量梯度回歸：
  **228 passed、3 skipped**。跳過的是需明確啟用的 CUDA 選測；本次未重跑
  GPU 編譯或正式訓練。`git diff --check` 通過。

## 公告退出初版驗證（更正 50% 前，沒有正式訓練）

- `--check-data-only` 通過，直接載入原 cache：2375 個 panel 日期、397 幣種、
  133 特徵、6 folds；沒有啟動模型、optimizer、checkpoint 或完成標記。
  新輸出根目前僅有前置檢查的 `startup_timing.jsonl`。
- 三幣 × 多空共 6 個原始資料兩日帳本案例，全數公告日平倉；前日含午夜 funding
  的損益與平倉費手算核對，final NAV 誤差小於 `3e-11`。
- 2026-01-01 至 09-24 的 267 日中，alive 且 forward valuation 不完整的原資料
  僅 HFT／VINE 08-20、ICX 09-18。新規則分別在同日或前日已平，後續不能重進。
  此結論限於已驗證的資料版本與範圍，不表示未來新增資料不需再驗證。
- strict CUDA 通過；新規則 compiled/eager 數值與梯度對照通過。
- 雙 rank Gloo 合成 trajectory 測試涵蓋舊／新退出規則、零與非零初始 policy、
  global batch 2／4、裁切與尾端 padding、兩種 metadata 路徑，共 16 個比較案例；
  loss、梯度、參數更新及每 epoch 一次 optimizer／scheduler 更新與單程序一致。
  這是單元測試，不是正式模型訓練或 full-fold 績效驗證。
- 擴大至帳本、資料製備／panel、梯度、loss、DDP、checkpoint、v5/v7/v8 設定及
  最佳驗證輸出回歸：**715 passed、5 skipped**；新公告退出 CUDA compiled/eager
  測試另行啟用，**1 passed**。語法與 `git diff --check` 通過。
- 舊 v7 四個已完成 fold 的完整 `mode_artifact_contract.json` 與新程式在
  flag=false 下產生的契約逐份相等，未讓舊結果套上新執行規則。
