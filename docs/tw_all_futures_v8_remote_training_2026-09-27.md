# v5 期貨訓練：當沖、一般與保證金模式

遠端 `vastai1T` 使用 `/root/stockAgent/train.py`。市場設定直接或透過一般模式繼承既有的
`configs/deployments/tw_day_trade_v8_v5_frozen_training_snapshot.yaml`；已核對其內容與
`artifacts/markets/tw_day_trade_v8_combined_annual_log_cash_sub_lot_first_session_all_folds_v5/run_manifest.json`
中的 `configuration` 完全一致。

| 模式 | 設定 | 正式產物位置 |
|---|---|---|
| 當沖 | `configs/markets/tw_futures_v8_intraday.yaml` | `artifacts/markets/tw_futures_v8_intraday/` |
| 一般留倉 | `configs/markets/tw_futures_v8_general.yaml` | `artifacts/markets/tw_futures_v8_general/` |
| 保證金留倉 | `configs/markets/tw_futures_v8_margin.yaml` | `artifacts/markets/tw_futures_v8_margin/` |

三者沿用 v5 的 FinancialTransformer、22 種時間基底、99 個股票背景特徵、
`score_entmax_log_cash`、BF16、雙 GPU、global batch 32、eval batch 16、
每 fold 最多 1,000 epoch、早停及每 epoch 曲線。只有期貨輸出、資訊時間與持倉帳務作必要調整。
股票作為背景資訊，交易標的是台灣股票、ETF 與指數期貨；不交易股票或選擇權。

## 目前選擇：當沖（2026-09-27）

最新 1 億元當沖設定與 v4 完整分鐘包，請見
[遠端訓練準備與驗收](tw_futures_v8_intraday_capital100m_remote_2026-09-27.md)。
v4 已通過 431,434 個合約交易日的完整資料驗證及遠端傳輸核對；
14:40 UTC 完成雙 RTX 5090 三輪 DDP、實際梯度、報表及完成後 resume 驗收。
正式 1,000 epoch 尚未啟動，請使用上述最新文件的 1 億元啟動指令。
以下 09:54–11:20 UTC 的 v3 與補抓數字保留作歷史紀錄，不代表最新缺口。

2026-09-27 11:20 UTC：依使用者要求啟用暫時期貨補抓優先。
`stockagent-futures-gap-priority-20260927.service` 暫停原本 active 的
FinMind Free／Sponsor／Complement 三個背景下載服務，未改 enabled 狀態、
timer 或交易／行情／Discord。四個下載 worker 共用原有帳號 limiter 與增量預留；
502／504 等暫時錯誤每筆最多重試兩次，每次仍計入請求上限。
先前兩筆 502／504 曾使舊續抓包裝於 10:35 UTC 停止；本次已保留原收據並重排。

11:19:17–11:20:17 UTC 實測期貨 99 次／分鐘（該分鐘其他 data API 為零），
對照先前五分鐘平均 57 次／分鐘約 1.74 倍；這是短窗口下載速率，非整個任務 ETA。
目前批次前 200 次成功、零失敗，依該進度收據尚約 7,145 個商品日待抓。
systemd `ExecStopPost` 會在補抓完成、失敗或停止時恢復原三個服務；
下載包裝在 14:00 UTC（台灣 22:00）停止新增請求，外層另有三小時 runtime 上限。
原排程、目前驗收與最後恢復收據保存於
`artifacts/markets/tw_futures_v8_intraday_preparation/priority_20260927/`。
此工作只補抓與驗證来源，尚未啟動正式訓練。

**09:54 UTC 進度：資料整理仍未完成，尚未執行真實雙 GPU 當沖 fold。**
最新已完成組裝的 v3 bundle 位於
`artifacts/data_preparation/all_futures_intraday_v8_20260927_v3/`，manifest 仍為 `partial`：

| v3 契約交易日證據 | 筆數 |
|---|---:|
| 有驗證分鐘觀測 `minute_verified` | 397,091 |
| 官方無單式成交證據 | 23,897 |
| 官方量上限證明 floor 參與率下不足一口 | 472 |
| 合計 accepted | **421,460 / 431,434** |
| 仍待驗證來源 | **9,974** |

官方零成交／不足一口證據不是分鐘成交觀測。09:50 時的 v2 為 419,538 accepted／
11,896 gaps；v3 比它多驗證 1,922 個契約交易日，但仍未達完整訓練資料契約。
不得以下載成功數或分片存在宣稱可訓練。
目前 config 仍指向舊分鐘包，須待新包完整驗證後才更新路徑及同步遠端。

本次沿用 `scripts/build_tw_stock_futures_0900_entries.py`，已接上全期貨 selector、
日期版本商品價格網格／交易時段、多來源 SHA、ETF 次查詢日尾盤重建及 FinMind
實體月份 Tick。整個選取範圍已有官方單式量證據，並保留已驗證 block 扣除；
797 筆超量來源強制重驗，未裁切成交量或猜測去重。base、快取、raw、exact repair
與最終組裝均守同一官方上界。相關證據位於
`artifacts/markets/tw_futures_v8_intraday_preparation/official_entire_selected_universe/`
及 `accepted_volume_revalidate_final_keys.json`。原始來源仍留本機；衍生分鐘包供本帳戶
遠端訓練使用，未新增公開分發或冷庫同步。

遠端股票背景 canonical panel 已完成快取預熱：**1,578 × 2,754 × 99**，
股票日期為 2020-03-23 至 2026-09-11；期貨策略仍由 `intraday_session_mask` 選取
實際期貨日（截至 2026-09-04）。這是背景 panel 預熱，不是期貨執行資料驗收；
receipt `stock_panel_prewarm_20260927.json` 明確保留 `training_ready: false`。

已通過遠端 CUDA 環境檢查，以及當沖 CUDA Graph 精確帳本／梯度 9 項測試
（20 項其他案例未選取）。新 KBar 修復遠端 focused tests 為 104 passed，本機綜合
224 passed／2 CUDA-skipped；這些結果不等於完整 DDP fold 驗收。
09:51 UTC 另在遠端 CPU 實際執行 canonical checkpoint resolver、建模與權重轉移，
證據保存在
[`pretrained_transfer_validation_20260927.json`](../artifacts/markets/tw_futures_v8_intraday_preparation/pretrained_transfer_validation_20260927.json)：

- 目標 **fold 5：train 2020–2024、validation 2025、test 2026**。原 v5 的
  `fold_10/checkpoint_best.pt`（epoch 30）使用相同 validation 年，只多出 2015–2019
  的較早訓練年份；明確套用 `matching_validation_and_causal_train_superset`。
  來源 SHA-256 為 `25d52616598046c7983adaf1285adc0f4efc6662c2c2e6e037e22536f213e5a2`。
- 用來源 manifest 的 99-feature／2,754-symbol 軸實際轉移成功，122 個骨幹張量複製，
  新期貨模組保留初始化；未匯入 optimizer。預熱 receipt 的 99 個 feature 名稱順序也與
  來源一致；完整目標 symbol 順序及期貨執行 panel 仍須由資料預檢核對。
- 22 種時間基底 ABI 相容；來源 PCA 證據只用來源 fold 訓練資料，validation/test rows 均為 0。
  保留來源 RMS 與 learned projection，99 個 RMS buffer 中 70 個 active，轉移後逐位元一致；
  不能稱作僅用目標 2020–2024 重新估計的標準化。exact resume 保留 checkpoint buffers。

策略維持 TWD 10M、notional 全額預留、日內平倉、雙 GPU BF16、global batch 32、
eval batch 16、每 fold 最多 1,000 epoch。容量是
`floor(0.5 * observed minute volume)`；無法於截止時間平倉仍會觸發執行失敗。
config 的 `runner.start_fold: 1` 防止繼承一般模式 fold 10；以下明確選擇當沖 fold 5。

**待新分鐘包完整驗證、同步遠端並更新 config 後**，先在 GPU 空閒時執行資料預檢
及獨立目錄的 3 epoch 完整生命週期短測：

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict &&
run_fintech_python train.py --config configs/markets/tw_futures_v8_intraday.yaml \
  --start-fold 5 --max-folds 1 --check-data-only \
  --output-dir artifacts/markets/tw_futures_v8_intraday_fold5_smoke3 &&
CUDA_VISIBLE_DEVICES=0,1 run_fintech_python train.py \
  --config configs/markets/tw_futures_v8_intraday.yaml \
  --start-fold 5 --max-folds 1 --epochs 3 --no-resume \
  --profile-timing --no-post-train-infer \
  --output-dir artifacts/markets/tw_futures_v8_intraday_fold5_smoke3
```

本模式每完整訓練軌跡更新一次 optimizer，因此 3 epoch 只有 3 次更新，仍在
32-step warmup；短測用於驗證資料、VRAM、帳本、checkpoint、報表與續跑，不能據此
判斷投資績效。短測產物須通過 `validate_completed_training_artifacts`（fold `[5]`、
group `train_2020-2021-2022-2023-2024`），並核對 `progress.json` 為 complete。
同一短測指令改成 `--resume` 後，已完成 fold 應跳過，epoch curve 不得新增重複列；
此檢查與中斷後恢復 optimizer／scheduler／RNG 是不同驗收項目。

**只有資料及上述短測驗收通過後，才可執行以下正式指令；目前尚未就緒。**
正式訓練使用另一個新目錄，沿用原設定的 1,000 epoch 上限：

```bash
CUDA_VISIBLE_DEVICES=0,1 run_fintech_python train.py \
  --config configs/markets/tw_futures_v8_intraday.yaml \
  --start-fold 5 --max-folds 1 --no-resume --profile-timing \
  --no-post-train-infer \
  --output-dir artifacts/markets/tw_futures_v8_intraday_fold5_v2
```

後續續訓保留完全相同資料與設定，將 `--no-resume` 改為 `--resume`。
原始 08:22:40 UTC 預檢曾回傳 1，舊 prepared bundle 缺 50,149 契約交易日；這是歷史證據，
不是上表 v3 缺口，也不代表原始來源皆不存在。原命令、resolved config 與 stdout 保存在
`intraday_fold5_preflight_20260927T082240Z.json/.log`，最初來源盤點與
`first_existing_source_build/` 仍保留於同一 preparation 目錄。

2026-09-27 已加入共用 CUDA Graph 及精確資金選擇加速，一般模式 fold 10 的確定性
完整比較為 29.43 → 5.60 秒／epoch。測速證據、資料限制與獨立實驗指令見
[fold 10 效能驗證](tw_futures_v8_fold10_performance_2026-09-27.md)。

同日針對 `tw_futures_v8_general_fast_fold10` 完成績效與帳本稽核後，另修正容量裁切、
零口成本梯度、零配置方向梯度與資金臨界值，並加入可獨立選用的因果面額／特徵 RMS
與事前容量候選設定。原始訓練產物保留；新的執行與梯度契約必須另開輸出目錄訓練。
實測對照、限制及最新重訓指令見
[一般模式績效診斷與修復](tw_futures_v8_general_diagnosis_2026-09-27.md)。
下面的短測與速度數字是各次實驗當時的紀錄，不能代替修正後的完整 fold 驗收。

## 遠端執行

在遠端主專案初始化環境，正式訓練前檢查 CUDA：

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
```

一般模式：

```bash
run_fintech_python train.py --config configs/markets/tw_futures_v8_general.yaml
```

當沖模式（目前會因下述分鐘資料缺口停止，補齊同一來源契約後使用原指令）：

```bash
run_fintech_python train.py --config configs/markets/tw_futures_v8_intraday.yaml --start-fold 1
```

資料檢查直接在上述 `train.py` 指令加上 `--check-data-only`。
沿用原本 DDP、fold 隔離、checkpoint、續訓、報表與產物驗證流程；三個模式各自續訓。
同一組 GPU 一次執行一個訓練。前次準備完成下述短測；本次保證金擴充查核時，
遠端已有一般模式 fold 10 工作在兩張 5090 上執行，保證金準備未重啟該工作。

保證金模式（目前缺完整歷史規則，預檢會停止）：

```bash
run_fintech_python train.py --config configs/markets/tw_futures_v8_margin.yaml --check-data-only
```

完整規則通過後，在 GPU 空閒時執行：

```bash
run_fintech_python train.py --config configs/markets/tw_futures_v8_margin.yaml
```

保證金的模型輸出、規則來源、風險政策及資料缺口詳見
[保證金契約](tw_futures_v8_margin_contract.md)。

舊文件中的獨立 worktree、`run_tw_futures_v8.sh`、設定產生器與額外分鐘資料整理器
已退出本方案。主專案沒有加入這些入口；資料直接使用既有 prepared bundle 和分鐘驗證器。

## 期貨必要調整

- 決策只讀先前完成的資料，股票當日開盤 gap 再延後一個交易日，避免期貨決策偷看 09:00 股票開盤。
- 保留 v5 的方向與現金配置模型，接上既有期貨 action head 與整口執行帳本。
  研究資金 TWD 10M、每口每邊佣金假設 TWD 40、既有日期版本的期交稅、成交量參與率 50%。
- 一般模式每日開盤再平衡，可續抱同一實體合約，到期採官方結算價。
  當沖採近月合約，08:45 決策、08:46 起執行，13:20 限價出場、13:24 起市價出場、13:30 截止。
  無法平倉保留原執行器的失敗狀態。當沖使用現金基準，避免混入隔夜基準損益。
- 沿用既有完整軌跡更新與期貨可恢復梯度，正向損益仍以整口帳本計算。
  股票的年度帳戶重置、零股與 T+2 語義不套到期貨。
- 按因果 fold 規則移植來源 v5 骨幹；使用目標驗證集守門，不依來源測試結果挑模型。
  整個帳本的編譯候選未通過完全一致比較，該開關維持關閉；後續通過驗證的 CUDA Graph
  重播與局部資金融合已啟用。模型編譯與 BF16 沿用既有流程。

## 資料狀態與驗證

股票背景固定於 `tw-public-20260913T030921278338896Z-l0-penguin-248d0869d7d53be4`。
期貨日資料為
`artifacts/data_preparation/futures_daily_70a57dd76de74fd0a365/continuous_daily.parquet`，
SHA-256 `70a57dd76de74fd0a3652b1ecf932ccaf32320d01c252bf7b608a30b3f598df5`，截至 2026-09-04。

一般模式在遠端主專案通過 `train.py --check-data-only`：3,096 個股票背景交易日、
2,754 檔背景股票、99 個特徵、10 folds。固定 1,936 個期貨輸出 slot 並非每天都有 1,936 個可交易合約。
既有結算驗證隔離了缺官方到期結算價的 3,597 個實體合約，因此可用範圍是經驗證的歷史子集。
一般模式帳本限制總名目曝險不超過資產淨值。新增的保證金模式改為分配原始保證金預算，
具備每日結算與追繳帳務；日頻風險觀察仍不能重建夜盤、盤中券商即時追繳。

當沖直接核對既有
`artifacts/data_preparation/futures_minutes_capacity_ceil_quarantine_v2_20260909/minutes.parquet`。
2020-03-23 至 2026-09-04 尚缺 **50,149 個合約交易日**：指數 28,111、ETF 21,814、股票期貨 224。
入口會在建立模型或啟動 GPU 訓練之前停止；必須透過既有資料取得流程補足，不能用日 OHLC 代替分鐘成交。

本次主專案整合的本機針對性測試 161 passed，遠端 139 passed（兩組重疊），新增檔案 Ruff 通過。
驗證包括 v5 模型設定繼承、當沖共用執行器的數值與梯度一致性、無成交日、因果資訊、checkpoint 和續訓。
此次遠端主專案另完成雙 RTX 5090 的三 epoch 短測，global batch 32、BF16，
首 fold 訓練 2015、驗證 2016、測試 2017–2026，完整產生 checkpoint、曲線及報表。
既有 lifecycle 驗證器的 18 項產物檢查全部通過；第 3 epoch 梯度非零，每條軌跡更新一次。
最佳 checkpoint 仍為現金基準；短測驗證訓練流程，尚不能證明策略有效。
可重現的短測指令如下；正式指令不需要這些限制：

```bash
run_fintech_python train.py --config configs/markets/tw_futures_v8_general.yaml \
  --epochs 3 --early-stopping-no-improve-ratio 0 --max-folds 1 --no-resume \
  --output-dir artifacts/markets/tw_futures_v8_preparation/smoke_general
```

本次來源備份、程式雜湊、測試及資料檢查紀錄存於
`artifacts/markets/tw_futures_v8_preparation/`。
