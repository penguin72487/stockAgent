# Attention LayerNorm：整日 FIFO、損失串接與 Vast 編譯修復

## 1. 執行進度與驗收邊界

2026-09-10。這輪完成 **整日執行器 → 共用 simulator → 共用 log-utility loss** 的候選接口、跨日狀態及分鐘權益對帳，並在 vastai1T 修復整日圖的編譯失敗。

**尚未完成年度 train.py 端到端整合，尚未啟動正式訓練，也沒有已驗收的新訓練指令。** 來源 adapter、train/eval/fold/stitched 狀態傳遞及正式 artifact serializer 都仍須完成。不能把這些工程工作全部歸因於資料缺口。

- 本機 focused regression：530 passed，24.86 秒；不是全 repository test suite。
- 最終遠端 focused regression：146 passed，11.19 秒；包含原生 scan 的 opcheck／梯度、carry、候選防錯與既有 checkpoint／resume。最終 recurrent benchmark 結果見下方更新。
- `load_config` 的分鐘來源＋carry 防錯仍保留。新增 legacy NPZ writer 防錯，拒絕把新的 physical state／minute NAV 寫成遺失 FIFO 的舊 artifact。
- 沒有送券商委託、重啟 Discord／網頁、部署新權重、覆寫正式 2/25 起的帳本或更動 Vast 原工作目錄。
- 依 training-reuse 沿用共用 loss／return math，不另建 trainer、optimizer、AMP、scheduler 或 checkpoint writer；依 strategy-switch 保持隔離驗收，不自動 promotion。

## 2. 帳務的第一性原理

09:00 的目標股數以當時官方 open／已知庫存淨值決定；09:01 才消耗可觀測的歷史成交價格及分鐘容量。當天模型目標減去既有實體股數，先減倉再加倉，兩者共用每檔同分鐘的 50% 整張容量。不能用未來能否平倉反推開盤股池。

日報酬的分母是 **前一個已提交收盤淨值**，不是重新設成今天開盤淨值或原始本金。270 個分鐘點是同一本 FIFO 帳的淨清算估值；估值不是平倉成交證據。

新 `DayTradeCarryState` 包含：

- `inventory`：實體 cohorts、basis、取得時費率、現金權利、利息及事件游標。
- `last_nav`：上一個已提交收盤權益，float64。
- `alive`：經濟破產狀態，與資料錯誤 `inventory.failed` 分開。
- `initial_capital`、`last_session_day`：拒絕換本金續跑、重複日、倒退日及破產後復活。

破產後保留原實體庫存證據，不再交易、不重設本金；補形狀用的零部位 cohort 也維持取得日期順序，確保安全 checkpoint 可還原。缺少持有股票的分鐘估值在 CPU 明確拋错、GPU 回傳 failed／NaN；loss 不得把它清洗成零報酬或記成投資虧損。

已證明停牌時，`opening_marks` 可與不可成交的 `official_open` 分開。這只接受外層提供的停牌／估值證據，不在執行器內插價格。

安全 state payload 綁定 ABI、ordered universe、精確 release 與本金；使用 `torch.load(weights_only=True)` 測試。這是 namespaced 元件的存取測試，**還不是正式年度 epoch checkpoint 已整合**。

## 3. 逐日與逐分鐘驗證

- 三日權重 `+0.21 → -0.21 → +0.10`，價格 `1000 → 1010 → 1020`；09:01 每分鐘 6000 股提供 3000 股容量。
- 第一天持有 2000 股；第二天先減 2000 股再開空 1000 股，不能重用容量開空 2000 股；第三天正常退出。
- 不分批與 `1 日 + 2 日` 分批續跑的全 state、報酬及 810 分鐘點一致。
- 直接调用正式 `TwDayTradeSimulationEngine` 與 `_replay_historical_intraday` 對帳，810 點與整日 tensor executor 的絕對差通過 `1e-7 TWD` 門檻。
- simulator 与 loss 的報酬、最終持倉 state 一致；反向梯度有限且非零。
- GPU 缺價測試確認 NaN 抵達 loss、沒有錯記成 financial default、checkpoint 拒收，且 CUDA context 仍能正常執行其他運算。
- tensor → NumPy 的 physical report 保留 float64，state 移至 CPU 私有儲存；舊模式維持原輸出精度。

## 4. 合法 tick 與歷史出場

既有 historical replay 在 13:20 產生 `Close ± 1 tick` 被動限價代理時，可能越過當天漲跌停。現在與 source-side schedule 一起約束在當天合法限價內；缺少精確限價即拒絕。已增加多空漲跌停邊界的 paper differential cases，原 ETF tick 測試補上應提供的來源限價。

這裡的 ±1 tick 是 **13:20 歷史被動委託代理**，不是在 09:01 成交價上加不利一檔，也不宣稱它是已取得的歷史最佳 bid/ask。VWAP、平均成本、費用與權益不可依單筆委託 tick 四捨五入。[證交所交易制度](https://wwwc.twse.com.tw/zh/products/system/trading.html)

這個候選保留使用者先前選定的「全部可轉融資融券、零股按整張行情」研究假設。它不是後續 staged `day_trade_strict_intraday` 實時紀律的改寫，更不是 Shioaji 成交回報；不會修改舊回補或實盤服務來強行達成一致。

## 5. 遠端編譯問題與實作修復

環境重新驗證：vastai1T 2 × RTX 5090、PyTorch 2.11.0+cu128、strict CUDA check 無 failures；執行前 GPU 沒有其他 compute process，隔離檔案系統可用約 374 GiB。未變更共享套件／驅動。

整日圖的 Inductor 編譯在 `TritonSymbols.get_block_shape` 失敗：`TypeError: list indices must be integers or slices, not NoneType`。進一步限制 fusion 後明確暴露 `SplitScan` 失敗節點。嘗試兩側 predicate、關閉 coalesce tiling、fusion size 1 及更換來源 stride 都未解決，失敗 receipts 保留，沒有稱為通過或偷偷退回 eager。

`stockagent/backtest/tw_inventory_scan.py` 以公開 `torch.library.custom_op` 建立原生 `torch.cumsum` 的明確編譯邊界，並註冊同 shape／dtype／device 的 fake tensor 與反向累積和 adjoint。只用於新的 FIFO 核心；不改全域 Inductor lowering，不移到 CPU，也不改金流公式。其他運算維持 fullgraph 編譯。[PyTorch 自訂算子與驗證文件](https://docs.pytorch.org/tutorials/advanced/python_custom_ops.html)

CPU／CUDA、非連續 tensor、空維度、正負 axis 的 opcheck、數值梯度 gradcheck 與前向對照通過。沒有新增另一套累積和數學實作；實際計算仍為原生 ATen device kernel。

### 固定日期／固定形狀測速

以下為合成資料，capital=1e12 只是避免大型合成持倉破產，不是將使用者 TWD 10M 設定改成 1e12。

| 形狀 | 工作 | eager 中位數 | compiled 中位數 |
| --- | --- | ---: | ---: |
| 64 檔、8 批、270 分鐘 | 整日前向 | 15.376 ms | 2.032 ms |
| 同上 | 前向＋模型權重反向 | 24.597 ms | 4.847 ms |
| 2754 檔、64 批、270 分鐘 | 整日前向 | 15.546 ms | 2.429 ms |
| 同上 | 前向＋模型權重反向 | 24.845 ms | 3.924 ms |

大型測試編譯首次前向 29.46 秒、首次反向 38.43 秒；已有其他 compile cache，並非全新機器的 cold-start 保證。前述反向只對模型權重求導，**尚不代表梯度穿過前日庫存的完整成本**，追加 recurrent 測試另列。

所有 receipts 都明載 `full_epoch_measured=false`、`train_py_inventory_integration_complete=false`。日期／cohort shape 變動的編譯重用、DDP、全模型及 train/val/test/plot/checkpoint I/O 仍待實測。

## 6. vastai1T 位置與重跑

隔離目錄：`/root/stockAgent-daytrade-parity-20260910`。程式碼經限定路徑 Git patch 傳送與 hash 比對；資料沒有經 SSH 搬移。上輪精確 Syncthing public/minute releases 與驗收範圍仍見 [準備紀錄](DAY_TRADE_VASTAI1T_PREPARATION_2026-09-10.md)，本輪未發佈新的資料 release。

以下只是驗證與執行器測速指令，**不是正式訓練指令**：

```bash
cd /root/stockAgent-daytrade-parity-20260910
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python -m pytest -q \
  test/test_tw_inventory_scan.py test/test_tw_day_trade_carry.py \
  test/test_day_trade_attention_parity_candidate.py \
  test/test_tw_minute_audit_release_scope.py test/test_checkpoint_manifest.py \
  test/test_training_resume_early_stop.py test/test_resume_completion_marker.py
run_fintech_python scripts/benchmark_tw_day_trade_inventory.py \
  --whole-session --device cuda:0 --symbols 2754 --cohorts 64 \
  --repeats 5 --threads 8 \
  --output artifacts/operations/daytrade-parity-20260910/whole_session_native_scan_recurrent_s2754_k64.json
```

`configs/deployments/tw_day_trade_attention_parity_vastai1t_candidate.yaml` 保留年度 expanding walk-forward、選定 attention/full_then_last/LayerNorm、1000 epochs、BF16、TWD 10M、50% 分鐘量。不得直接繞過 guard 執行。

## 7. 尚未完成

1. `execution_actions` 的最新 attempt（13:02:45 UTC，本輪重新讀取）仍是 68 筆 MOPS 參考條款恢復、36 筆失敗，`accounting_ready=false`；本輪沒有另啟下載。231 筆參考事件不等於完整的付款日／零碎股帳務條款。
2. 接好 accepted source adapter：精確 release、全日 OHLCV／合法限價、逐股停復牌、完整 action／cash-in-lieu；不能由約數或價格倒推條款，也不能把缺來源當成零成交量。
3. 完成共用 trainer 的 chronological batching／padding、eval chunk、fold ownership、stitched prefix、正式 NPZ／epoch state 序列化與相容性 fingerprints。現在 simulator/loss 的接口測試不代表 trainer 已採用。
4. 通過有效年度 walk-forward 全流程及 exact resume，再對真實全 universe 的 cold epoch 與 epoch 2 含全部報表／checkpoint 工作測速。不得以本文件的微測速或幾十天假切分取代。

## 8. 最終 recurrent 實測更新

遠端最後一輪把模型權重與前日 physical cohorts 都設為需要梯度，驗證 eager／compiled 的完整結果與兩組梯度；持倉梯度有限且非零，帳戶沒有在合成測試中破產或只走空帳分支。

| 2754 檔、64 批、270 分鐘 | eager 中位數 | compiled 中位數 | 比值 |
| --- | ---: | ---: | ---: |
| 整日前向 | 15.635 ms | 2.506 ms | 6.24 倍 |
| 前向＋模型權重／前日持倉反向 | 28.497 ms | 5.169 ms | 5.51 倍 |

反向測量峰值 allocated memory：eager 579,286,016 bytes，compiled 496,294,912 bytes。這是單日 kernel 的配置，不是完整訓練峰值 VRAM；沒有測到 model/optimizer 或多日保留圖的總記憶體。

最後一轮首次前向 5.817 秒、首次反向 40.775 秒，前向已有先前編譯 cache。收據：`artifacts/operations/daytrade-parity-20260910/whole_session_native_scan_recurrent_s2754_k64.json`；status=passed，`backward_inputs=[model_weights, carried_physical_cohorts]`，正式 epoch／trainer 整合的 false 旗標保持不變。

Ruff（包含共用 trainer）、py_compile 與 `git diff --check` 通過。新增檔案及限定變更都留在既有工作樹；沒有回退其他 dirty changes，也沒有建立／刪除 Git commit。正式訓練依第 7 節條件保持未啟動。
