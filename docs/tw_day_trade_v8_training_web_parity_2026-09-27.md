# V8 訓練與網頁執行合約對齊（2026-09-27）

> 後續決策：使用者已改為四個啟用模型全部重算，13:30 以有來源的收盤價不限容量平倉。
> 最新實作與驗收見 [全部模型不限容量收盤重算](tw_day_trade_all_models_unlimited_close_2026-09-27.md)。
> 下文 v7 的「尾盤仍限量」是保留的歷史比較基準；新的訓練候選為 `tw_day_trade_v8_web_close_unlimited_v8.yaml`。

## 1. 執行進度與驗收邊界

目前為候選版本，**尚未通過完整雙卡 fold 生命週期，不可宣稱正式訓練就緒**。已停止本次候選測試，等待使用者確認會改變損益的成交／認購權條款；未啟動正式訓練，未部署新模型。

最新驗收摘要：本機核心／續單 179 passed；設定／來源／artifact 41 passed；訓練／resume／checkpoint 140 passed、3 skipped（組別可能重疊，不相加）；雙卡 6 passed／每 rank；修正後真實案例 CUDA initcheck 0 errors。最新完整 fold 10 測試完成 epoch 0 的 55／77 批，進入第 56 批後因下述條款選擇而主動停止，**尚無成功 epoch checkpoint／fold_complete**。新 checkpoint 尚未部署，下面即時 broker 與非現金企業行動差異尚未解除。

待決策：

1. 即時採使用者先前指定的 Shioaji simulation 成交回報，或另改 50% 分鐘容量本機模擬；不可把目前本機全量 bid/ask 記帳當作券商回報。
2. 舊 v5 physical source 將現金增資認購權的官方參考理論價值 `subscription_ratio * max(reference_price - subscription_price, 0)` 在有效除權日做現金等值入帳；網頁未採此假設。要取消此假想入帳並排除缺完整條款的股票區間，或讓網頁也保留原研究假設，需要使用者確認。這不是可由效能修正默默決定的交易規則。

- vastai1T：`/root/stockAgent`，2 × RTX 5090（各約 32 GiB），Torch 2.11.0+cu128；嚴格 CUDA 環境檢查通過。
- 新版正式資料 `--check-data-only` 退出碼 0：3,096 日、2,754 檔、99 特徵、fold 10。
- 獨立紙上帳戶對照：多次分批成交、長／短、跨日反向 FIFO、利息、分鐘容量、期末淨值通過。
- 同一資料發布的 2026-02-25 與 2026-09-11：訓練與 09:00 網頁輸入形狀 `[32,2754,99]`，特徵及政策資格遮罩逐元素相等，最大差 0。
- 真實雙卡梯度測試：eager、逐分鐘 compiled 及 activation-rematerialization 路徑均通過；最新重跑每 rank 39.6 秒、梯度相同。這不是完整訓練驗收。
- 首輪完整 fold smoke 在逐分鐘 FIFO 中觸發雙卡 OOM；GPU 0 PyTorch allocation 約 30.47 GiB。沒有正式模型或成功 fold 標記。
- 已加入日／分鐘兩層非 reentrant activation checkpoint；CPU 跨日曲線與梯度對照完全一致。新完整 smoke 觀測兩卡各約 10.2 GiB，運行約 11 分鐘尚未完成 epoch，主動停止以改善冗餘續單掃描；不能把沒有 OOM 當成完整驗收成功。
- 新增有條件的 dense FIFO 尾段積分：只有證明已無可執行續單，且剩餘路徑淨值為正，才重用既有批次 FIFO；不改成稀疏來源、不省略分鐘 NAV，也不放寬成交規則。最新版本雙卡兩種容量情境均通過（每 rank 2 passed / 27.0 秒，對照禁用 compile、checkpoint、尾段捷徑的逐分鐘 oracle），完整 fold 仍在驗收。
- 已補回歸：舊部位停損不誤取消尚未開啟的反向新單；已完成的持倉目標不因舊部位出場而重新買回。
- 設定／checkpoint／resume 單元測試 126 passed；續單最新 focused 測試 15 passed（nested checkpoint 另測）。各組有重疊，不累加成虛假的測試總數。
- 最新核心、來源、跨日與獨立 replay：158 passed；庫存、訓練 adapter、artifact、live signal 與紙上服務：357 passed / 3 skipped。Ruff 與已修改 tracked 檔案的 `git diff --check` 通過。
- 雙卡測試曾因同一程序逐案例重建 NCCL group 失敗，已改 module-scope group；修正後兩案例完整通過，沒有將該次失敗隱藏為訓練成功。
- 完整 fold 在 batch 40（首個分鐘來源區段，rows 1248–1280）曾停留於 PyTorch AOT tracing，診斷重跑兩個 rank 均收到 SIGSEGV。只停用新續單編譯後，同 batch 以 121.6 秒完成；保留其他 compile 與所有成交規則。這是隔離證據，不足以斷言 PyTorch 的唯一根因。
- 續單合併改成純欄位構造，移除可微分切片覆寫及無效 concat；正式尺寸 `[32,2754,12]` FIFO、32 個 claims rows、非連續分鐘切片的雙卡值／梯度測試通過，包括 nested checkpoint（每 rank 2 passed / 約 22.6 秒）。單步冷啟動約 19.1 秒，暖測案例約 0.069 秒，不能拿它當完整 epoch 的速度。
- 該修正後本機庫存與續單 92 passed；來源／設定／checkpoint 122 passed。完整 fold 已重新驗收，仍未取得完成證明。
- 追蹤確認 claims rows 10、11、12……逐日新增重複 step graph。編譯入口改為最小 32 列、2 次方補零，返回時還原真實 claims 列數；非零正／負應收款及梯度對照通過。最新雙卡 4 passed / 每 rank 35.2 秒；本機核心、adapter、artifact 210 passed / 3 skipped。資料與模型沒有縮小。
- 最新完整 smoke 已通過第 61／77 批；先前卡住的 batch 40 保持 compile 開啟後以 60.8 秒完成（禁用新續單 compile 的隔離測量為 121.6 秒，兩次均列印 loss 0.03425839；這不是完整 epoch 配對測速）。後續分鐘批次有約 14–49 秒的暖測觀察，仍待完整 fold 與第三 epoch 測量。
- 該輪後續完成全部 77 批訓練，但 rank 1 測試集在 2026-08-03、row 138 的 physical inventory gate 失敗，**沒有成功 epoch checkpoint 或 fold-complete**。來源缺價／未解 action 遮罩均未命中，不能直接歸因為缺資料。當日 13 個 action 中有 1459 的 0.75 股數替換／每舊股 2.5 元現金；僅是診斷線索，尚未證明它為根因。已終止這次失敗的指定 torchrun 工作，不影響其他服務。
- 新增失敗限定的單日 CPU replay 與無效索引診斷，以既有 executor 定位具體 invariant，並以 v5 固定 requested weights 隔離新執行合約。最近本機核心 164 passed；獨立紙上曲線 6 個案例均比較完整 270 點，包括首分鐘零量及跨日；設定與續單測試合計 27 passed。各組仍有重疊。
- 固定舊 v5 requests 的隔離 replay 在 2026-06-29 重現同類 GPU 拒絕，CPU 單日 oracle 卻通過。已保留遠端 `artifacts/markets/tw_day_trade_v8_web_parity_v7_diagnostics/failed_session_2026-06-29.pt`，是測試輸入，不是模型 checkpoint。
- Compute Sanitizer `initcheck` 捕捉到 FIFO 編譯 NAV 的 `searchsorted/gather` 融合 kernel 未初始化記憶體讀取：初次 1,141 errors（後續第二次診斷因 sanitizer 額外負擔 OOM；不把它當一般訓練 OOM）。生成檔 `c3xtvnnvj4riajcip4rzdcmq5u42k5dzftdmhmijnlq56aozl3kz.py:2669`；錯誤 kernel 涉及 2,754 × 270 非整齊尾塊與間接讀取。不能因此直接宣稱所有舊 v5 artifact 已錯，舊產物需另驗證。
- `tw_inventory_scan.py` 沿用已有 native ATen boundary，新增非可微索引查找 `inventory_searchsorted`，FIFO 的五處索引統一走原生 CUDA；其餘張量／模型仍 compiled，沒有 CPU fallback 或修改成交量、價格、費用。修正後同一失敗 capsule 的 `initcheck` **0 errors**，NAV `12775355.126832088` 與 CPU oracle 一致。新的本機 scan／庫存／carry 156 passed；完整 fold 尚待重跑。
- 該 native lookup 修正仍不足：重複雙卡測試 rank 0 最後一分鐘 NAV 曾差 13,000 元；獨立比對也觀測到 `ENTRY_COST` 95.80987345834728 從 symbol index 2747 錯置到 1865，CPU 與 eager GPU 則一致。`_compiled_inventory_path` 尚未沿用其餘 carry 路徑的 bounded-fusion／停用 coalesce tiling 設定，已補齊並納入 compile key；再次驗收中。單次 sanitizer 無錯不等於數值／全部 inventory 欄位正確。
- 隔離測試顯示上述 compiler options 與停用 buffer reuse 仍不足，完全禁止融合可消除該次欄位差異。採更窄修正：將 `_columns` 的 stack 變成原生 GPU layout boundary，反向為精確 unbind；不改模型或交易數學、不全面停用 compile。修正後真實 capsule 三次重複、所有 inventory 欄位、270 分鐘 NAV、非整齊尾塊梯度、nested checkpoint 的雙卡 **6 passed / 每 rank 93.98 秒**；本機 native op schema／fake／梯度 14 passed。完整 fold 尚待重跑。
- 最新修正後完整 smoke 的 batch 40 以 58.3 秒完成；之後觀測約 23–83 秒／分鐘區段 batch，兩卡各約 15.6 GiB。不是完整穩態 epoch 測速，不宣稱理論極限或正式訓練就緒。為避免未核准認購權條款下的無效長跑，驗收已停止並保留所有診斷／候選檔案。
- 獨立紙上引擎新增兩檔多空續單爭用資金的案例；分批價格改變時，最終股數與 NAV 仍與訓練 executor 相符。此案例另測 1 passed，不與前述重疊組數累加。
- 不曾重啟網頁／Discord，不曾重算或覆寫現有帳戶、舊 checkpoint、optimizer 或正式曲線。

## 2. 要統一的是同一個計算問題

報酬可比較的前提為：同一 checkpoint、同一資料版本、同一歷史視窗與資格遮罩、同一帳戶起始狀態，以及同一成交／費用／企業行動合約。單純對齊報表起訖日不足以保證相同報酬。

舊 v5 測試帳戶從年初已有損益，網頁帳戶從 2/25 的一千萬元開始；股數取整與容量限制使不同資本路徑無法僅靠曲線 rebasing 消除。舊訓練另有尾盤不限量清倉，以及僅首分鐘入場等差異。

| 項目 | 新共同歷史執行合約 |
| --- | --- |
| 模型 | 保留 v5 的 99 特徵、22 基底家族、score_entmax_log_cash；不接舊 optimizer |
| 決策 | 每交易日一次，以 09:00 官方開盤觀察／帳戶淨值凍結目標股數 |
| 開始成交 | 09:01 右標分鐘的 VWAP／已允許 close 方法；不額外加不利 tick |
| 未完成委託 | 同一目標續至 13:19；不是每分鐘重新推論或重新依 NAV 配股 |
| 容量 | 同股票同分鐘的減倉與增倉共用 50% 真實分鐘量；不能各用一份 |
| 優先順序 | 止盈止損／退出、舊部位減倉、新增部位；已退出的新單不重新進場 |
| 資金不足 | 續單按實際持倉原始成交成本及剩餘淨手續費預留，對所有股票採共同的比例／股數取整分配 |
| 尾盤 | 沿用 13:20 限價、13:24 改市價與收盤撮合的來源時間合約；不得假設不限量保證清倉 |
| 隔夜 | 實體 FIFO、融資融券成本、收付款／股票交付日延續，不因跨年把持倉歸零 |
| 研究假設 | 保留全部可轉融資融券、零股按整張價格，以及核准的日 K 代理 |
| 日 K 代理 | 官方開／收盤，無不利 tick，容量公式仍為日量 × 50% ÷ 271；不捏造中間分鐘成交 |

買賣委託價要符合日期／商品 tick，但多筆成交的平均成本或 VWAP 不應被硬捨入到一個 tick。歷史 KBar 模擬仍不是可驗證的券源、授信或真實 broker queue。

即時訊號之後的 broker 模擬成交回報，與 09:01 歷史反事實回補是不同證據。不能把歷史分鐘假設宣稱為即時已成交，也不能要求兩者每筆成交完全相等。

本次實際讀碼發現：目前 active 網頁不是 Shioaji simulation 成交回報。`tw_day_trade_simulation.py` 明確不呼叫 broker order API；runner 選擇 `causal_market_full_target`，即訊號後第一筆符合時序的 bid/ask 以全目標量作本機紙上記帳。它既不是 50% 分鐘量重播，也不是券商模擬成交。已另外詢問使用者要保留先前指定的 Shioaji simulation 回報，還是即時也採 50% 分鐘容量；在這個選擇及對應 adapter 驗收完成前，不宣稱即時成交已經統一。本次未改 active 即時成交政策。

另有部署阻擋：physical training source v12 可處理股票股利／交付日鎖定及 subscription-right reference-value 研究條款；目前 paper `_margin_corporate_action_gate` 只會接受精確現金／既有換股條款，其他非現金事件仍阻擋。六個獨立紙上分鐘案例不涵蓋完整企業行動矩陣，因此不是所有歷史日的完整 parity 證明。不得把 YAML 合約相同當成 runtime 已支援；正式切換前需補同一 corporate-action corpus、交付日、現金收付與跨日庫存驗收。本次沒有放寬 gate 或把未交付股票當作可賣。

對固定 tw-public release 的 2026-01-02～2026-09-11 直接計數：exact_cash 2,113 筆、avoid 247 筆、exact_inventory 股票股利事件 0 筆；其中 70 筆 avoid 符合 source 的 subscription-reference 研究入帳條件。這是來源事件數，**不是策略實際持倉／實際獲利筆數**；不能把 70 筆全算為已發生報酬差異。

比較 2026 測試集時，紙上帳戶必須從相同第一個測試交易日及一千萬元起跑；如果面板只篩選 2/25 起，應承接先前持倉、現金、費用及應收款後再切報表日期，不能在 2/25 另重設一千萬元。部署前另驗證同一資料發布、checkpoint 及帳戶起點的重播。官方日 K 開盤值仍不保證每檔都在 09:00:00 已可觀察。

## 3. 程式與設定

- `stockagent/backtest/tw_day_trade_inventory.py`：固定目標續單、同日單一加權 FIFO cohort、零成交 no-op、共用容量與資金分配。
- `stockagent/backtest/tw_day_trade_carry.py`：真正按分鐘推進，持倉存在後才鎖定 stop；activation rematerialization 只改記憶體保存方式，不改梯度或成交。
- `stockagent/data/tw_day_trade_schedule.py` 與 `tw_day_trade_carry_source.py`：保存原始入場量價及尚未鎖定的 stop 觀察，新指紋隔離舊快取。
- `stockagent/training/day_trade_carry_bridge.py`、`train.py`：仍使用既有 source、trainer、DDP、checkpoint、epoch 與最終評估流程。
- `stockagent/live/tw_day_trade_simulation.py`：新增 opt-in `proportional_net_reservation_v1`；舊帳戶預設不變。
- `scripts/run_tw_day_trade_simulation.py`：依新訓練合約選相同續單資金分配。
- `scripts/audit_tw_day_trade_web_input_parity.py`：讀取已驗收快取，比對真正 training dataset/window builder 與 live window/mask builder。
- `configs/deployments/tw_day_trade_v8_v5_frozen_training_snapshot.yaml`：從 v5 run manifest 固定原始設定，非另猜一套參數。
- `configs/deployments/tw_day_trade_v8_web_parity_v7.yaml`：新訓練，輸出 `artifacts/markets/tw_day_trade_v8_web_parity_v7`。
- `configs/deployments/tw_day_trade_v8_web_parity_v7_inference.yaml`：直接繼承新訓練合約，只換即時 producer 路徑及本機 cache；尚未切換 active selector。
- `configs/deployments/tw_day_trade_v8_web_parity_v7_smoke.yaml`：完整 fold、3 epochs，獨立 smoke 產物。

遠端交易服務模組比 penguin 舊，不把遠端的舊 paper 程式當作新網頁規則證據。紙上帳戶獨立比對在 penguin 完成，遠端負責新的實體 executor／正式資料／DDP 驗證；不覆寫遠端其他研究更動。

## 4. 固定來源與量測

- TW public：`tw-public-20260913T030921278338896Z-l0-penguin-248d0869d7d53be4`。
- 分鐘來源：`tw-minute-train-20260910T121601063496311Z-l0-penguin-09bedd96a2f68c39`。
- Physical source：`tw-day-trade-carry:3b84332683cc07e534ea1a15fb68ffabe0abdd5ebc27b3b94864d7248c314322`。
- Physical manifest SHA-256：`2b1a9bdec86e386fdc28d979b24c0c1ac08d2f99ecc17733c7f424dbf973679e`。
- Panel generation：`08a15ef9d099407db04be05d1a4c360c`；source hash `39946da06a6c54be251487c0a36aa243fa48de684af3fa94d9bc7d6f631471fc`。
- 分鐘股票日 2,798,943；日 K 代理股票日 2,986,868；未處理 source gap 0。這個 0 不等於全部都有分鐘資料。
- 歷史未知換股／估值區間採已有的保守持倉排除；receipt 包含 unresolved action gap 563,786 股票日，不隱藏此限制。
- 首次完整資料準備約 1,417 秒（其中實體來源建立 772 秒）；再次載入與完整 SHA 驗證約 69 秒。
- 新增完整分鐘欄位後 dense RAM 估計 155.3 GiB，超過原 128 GiB 上限，故不預載全資料；不能沿用只有 5 欄時的錯誤估算。
- 實體 executor 快取新增約 48 GiB，最新遠端檢查剩餘約 93 GiB（磁碟狀態會變）；本次沒有刪除任何舊資料。

## 5. 驗收後的遠端操作

以下都在 **vastai1T `/root/stockAgent`** 執行，不在 penguin 執行，也不背景執行。

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_v8_web_parity_v7.yaml \
  --start-fold 10 --max-folds 1 --check-data-only
```

只有完整驗收補齊後，才可使用以下正式命令：

```bash
run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_v8_web_parity_v7.yaml \
  --start-fold 10 --max-folds 1 --no-isolate-train-folds
```

原有 DDP launcher 會啟動兩卡，全域 batch 32，BF16。維持年度 walk-forward（train 2015–2024、val 2025、test 2026）；關閉會重設年度帳戶的額外 episodes，以及仍按舊單次入場產生的 backward-only sub-lot 標籤。這不等於改變年度切分。

## 6. 尚待完成

1. 雙卡局部數值／梯度已通過；仍需正式分鐘來源連續 batch 的編譯／OOM 回歸。
2. 真實 full-universe、batch 32、完整 fold 的 epoch／final-test／artifact／resume 驗收。
3. 新 checkpoint 訓練完成後，另走 canonical strategy-switch／同一起始帳戶與資料版本重播／270 點分鐘曲線驗收，再切換網頁。這次尚未承諾新模型的投資績效。
4. 即時 backend 與 50% 分鐘歷史 replay 的差異已揭露，待使用者確認成交證據契約；不得把本機 bid/ask 全量紙上記帳標為 Shioaji simulation 回報。
5. 網頁非現金 corporate-action／pending-stock／subscription-right contract 尚未與 physical trainer 全面對齊；不因此切換現有帳戶或宣稱完整同報酬。

參考：[PyTorch 2.11 activation checkpoint 文件](https://docs.pytorch.org/docs/2.11/checkpoint.html)；[證交所交易制度](https://www.twse.com.tw/zh/products/system/trading.html)。前者描述重算換取記憶體，並不替代本專案的梯度對照；後者不支持任意尾盤不限量必成交的研究假設。
