# Attention LayerNorm 正式訓練就緒盤點

## 1. 執行進度

前階段完成：官方開收盤日 K 代理價格口徑、physical FIFO archive 保存／恢復、精確截段端點與原子寫入，相關回歸 **493 passed / 22.16 秒**。最新補上「未提及假設繼承指定產物」設定與防漂移測試，候選設定／分鐘代理／checkpoint 回歸 **139 passed / 8.57 秒**。沿用既有 `train.py`、年度 walk-forward、simulator/loss、checkpoint 和回測存檔入口，沒有另建訓練器。

**正式訓練現在已就緒，但未啟動 1000 epochs。** Accepted-source adapter、年度 train/eval/fold 狀態路由、physical FIFO archive、checkpoint/resume 與報表已完成端到端整合，且在 Vast 雙 RTX 5090 上以真實資料完成全新的一折一輪驗收。本輪沒有切換網頁、重跑線上帳戶、下券商委託、發布模型或改變 Vast 同步角色。

後續遠端實作已另記於 [Vast 訓練整合實測](DAY_TRADE_VASTAI1T_TRAINING_TEST_2026-09-10.md)：新增隔離 worktree；跨 batch FIFO、DDP 全域帳戶、eval chunk／segment 交接已接入共用執行函式，並完成雙 RTX 5090 的指定模型 BF16 真實資料整合測試。這取代下表的早期缺口快照；下表保留用來說明原始驗收項目，目前狀態以第 7 節為準。

使用者已確認：日 K 代理採官方 Open/Close，不加不利 tick；容量保留 `floor((當日成交股數 / 271) × 0.5 / 1000) × 1000`。當日總量是事後成交容量研究假設，不是 09:00 模型可知特徵。有分鐘資料時保持原本 50% 分鐘量。完整跨日持倉、全部可轉融資融券、零股按整張行情等原有研究假設不變。

**最新確認已落實：** 未明確變更的假設全部繼承指定的 `tw_day_trade_hybrid_minute_v12_attention_full_then_last_layernorm_commission20_capital10m_all_features_v1` 產物。因此候選恢復 **2014-01-06** 起、原年度 walk-forward，以及只在第一個 canonical 分鐘 partition 之前使用日 K 代理（目前界線為 **2020-03-02**）。後期個別股票日缺分鐘，不自動改用日 K。此決定取代先前待確認的起年／代理範圍，不撤回已明確要求的跨日實體持倉與官方代理價無不利 tick。

## 2. 正式啟動驗收表（歷史缺口快照，第 7 節已取代）

| 項目 | 目前證據 | 尚需完成 |
| --- | --- | --- |
| 模型與年度切分 | 已固定 2014 起、截至 2026 年共 11 folds；保留指定 attention/full_then_last/LayerNorm、BF16、1000 epochs、10M、50% | 驗收完整歷史的資料與狀態路由；新 artifact 不能沿用不同帳務契約的 optimizer |
| 日 K 代理價格 | 新 policy 已接 config、共用 loader、train.py、快取、checkpoint fingerprint、report contract | 新 physical source adapter 要沿用此價格契約，不能借用舊日末假平倉 executor |
| 分鐘來源 | 本輪重驗 2020-03-02～2026-09-10 的缺口清單及 request/response 雜湊 | 合併已保留來源；逐股票日驗證 OHLCV、09:01、合法限價、停復牌與 source regime |
| 企業行動 | 12886 筆 exact_cash 分類；2132 筆其他分類；換股 attempt 247 筆參考解析、20 件未解 | 真正持有期间需要精確股數、現金、付款日及零碎股條款；未持有事件不是全域阻擋 |
| 多日 FIFO 核心 | simulator/loss 全程與分段、梯度、CUDA 錯誤傳播測試通過 | 正式 caller 必須傳遞 physical cohorts，不能從 final_weights 還原 |
| Archive／截段 | 本輪 schema 8 保存股數批次、成本、claim、游標、NAV、270 點曲線；存後續跑逐值一致 | 年度 report caller 提供來源／有序股票／本金／分段起始 NAV／真實端點 state |
| train/eval/DDP/fold/stitched | 工程尚未完成，config guard 保留 | 時間順序、padding no-op、state 隔離；部署換 fold 不重設帳戶 |
| 完整年度 smoke／續訓 | 共用 checkpoint/resume 回歸通過 | 真實年度端到端；optimizer/scaler/scheduler/RNG 恢復及完成標記驗收 |
| 效能 | 本機及 Vast strict CUDA environment 通過 | 全 universe 冷 epoch 與 epoch 2+，含所有評估、圖表、checkpoint、峰值 VRAM，才能選 batch/compile/DDP |
| Vast | 本輪確認雙 RTX 5090；既有 minute cache hot-current | 本輪新 code/data 尚未送達；须經 catalog／Syncthing／READY 驗證，不是舊 cache 存在就就緒 |
| Vast 持久性 | 本輪重查 workspace_is_volume=false | recycle/destroy 會失去容器內資料；尚未完成新訓練產物 off-box 保存 |

## 3. 代理能替代什麼

兩個開收盤價不能推出 09:01 價格、13:20 被動單是否成交、止盈止損先後或每分鐘容量。日 K 代理是研究條件，不是新下載的分鐘行情；也不能從除權後價格倒推出換股比例、付款日或零碎股折現條款。

永豐官方列示股票歷史行情起日為 2020-03-02，歷史 KBar 用於盤後／回測，不是即時行情介面。本輪只核對官方定義及已保留來源，未新增券商登入或反覆請求界線以前的分鐘資料。[永豐歷史資料](https://sinotrade.github.io/tutor/market_data/historical/#historical-periods)

本機 receipt-backed 日曆：2014-01-06～2026-09-10 共 3095 個 session，1503 個早於分鐘界線。SHA-256：`3f272a327bb55b9a066a3a4c4e1cbebf0f2d2783d57872a87439c9f0432c1416`。這不代表所有股票 OHLC／企業行動都完整。

共用 `build_expanding_year_folds` 實算：2014～2026 有 11 folds，2020～2026 有 5 folds。前者首 fold 訓練 2014、驗證 2015、測試後續年；末 fold 訓練 2014～2024、驗證 2025、測試 2026。沒有自行改成年內隨機切分。

## 4. 本輪修復

### 未提及假設繼承與實驗隔離

實讀指定產物的 `run_manifest.json`，其 `configuration` 與目前 base config 經 `load_config` 解析後的共同欄位逐項比較，**零差異**。Manifest SHA-256：`b32051089759b8241376357fc27b749cf390b323e01c0f81f75eb0f252498ec9`。

候選沿用原模型、特徵、損失、費用、年度切分、DDP、matching-train-and-validation-year 預訓練初始化及 validation guard；移除先前未經最新決策支持的 2020 起年、取消預訓練、DDP auto 與禁止自身續訓覆寫。`resume: true` 僅針對候選獨立輸出目錄，仍受 checkpoint 契約驗證；從原匹配 fold 移轉模型權重，不等於恢復舊帳務模式的 optimizer。

與原設定的差異明列於 regression test：實驗名稱、獨立輸出／快取目錄、跨日融資融券及新代理價格 policy。Vast 的 CPU／編譯執行緒上限與精確 release 路徑仍是機器部署設定，並非另一套交易假設。本次設定修訂沒有同步或啟動遠端工作。

### 代理價格與相容性

新增 `data.day_trade_minute_execution_daily_proxy_price_policy`：`legacy_adverse_tick` 保留舊實驗；本次候選使用 `official_open_close`，多空入場都是官方 Open、退出都是官方 Close，容量公式不變。

共用 `daily_proxy_price_arrays` 驗證股票／ETF dated tick、保留原 float32 來源表示誤差容忍；不把 VWAP/NAV 按委託 tick 四捨五入。新 policy 另有 cache key；cache 命中也驗證輸入價格，防止 float32 hash 把非法 double quote 混過去。Preprocessing fingerprint 納入 policy，並關閉新口徑沿用「舊 checkpoint 未 hash minute tape」的相容捷徑。

候選已設定 `allow_daily_proxy: true` 與 `official_open_close`。尚未擴大後期缺分鐘的 proxy 範圍，也沒有解除獨立的年度狀態整合 guard。

### Physical archive

`stockagent/training/day_trade_carry_artifact.py` 只做 canonical archive 的 mode-specific encoding/validation。讀取 `allow_pickle=False`，財務 state float64，重用 `DayTradeCarryState` checkpoint 驗證，不另寫會計公式。

Context 必須提供 exact release、有序唯一股票、原始本金及分段前一收盤 NAV。拒絕 latest、錯版本／股票／本金、缺欄位、舊 ABI、float32 會計狀態、NaN、非整數股數、混入 legacy T+2 queues，以及日報酬和分鐘收盤 NAV 不一致。破產保持吸收態，不能續跑復活。

截段保留分鐘曲線；可續跑端點必須有該日真實 FIFO state，不能继承未來期末庫存。只供報表用的 prefix 可沒有端點 state，但不能寫成完整可續跑 archive。同目錄暫存、flush/fsync、physical 讀回驗證後原子替換；寫入／驗證失敗保留上一版。

另修正報表入口：只有 tw_overnight 讀取隔夜專用設定，不讓非隔夜模式最小 config 在輸出報表時報錯。

## 5. 來源缺口與遮罩

完整清單：[來源盤點](DAY_TRADE_FORMAL_SOURCE_AUDIT_2026-09-10.md)；receipt：`artifacts/operations/daytrade_source_gaps_20260910/formal_source_audit.json`。該次盤點範圍為 2020-03-02～2026-09-10；恢復 2014 起年不代表較早期間的股票日 K 與企業行動已通過完整正式驗收。

- 127 個 provider-gap 股票日，89 檔、27 日期。
- 124 檔無現行可查合約，期間仍有 71194 個正日量觀測；其中 156 股票日已有本機 receipt 分鐘證據，待 canonical ingestion，不可當歷史不存在。
- 2/25 後明列缺日：00643K（5/5、6/16）、2321（7/30）、3629（9/10）、5878（8/20）。
- 20 件換股未解，643 份 raw request/response 已驗證。2132 筆 avoid 是分類，不全是下載失敗。

缺口只影響明列股票日新單，不重分配別股權重。已持有股數不能被 mask 刪掉；確定金額／付款日的現金 claim 則不依賴股價。後期缺分鐘維持原排除／零容量語義，不自動 proxy；既有持倉的估值與企業行動仍須獨立驗收，不能把未知企業行動變 exact 或捏造 270 根真實成交。

## 6. 驗證與遠端邊界

最終 493 項回歸完整输出：`artifacts/operations/daytrade_source_gaps_20260910/formal_readiness_regression.log`；代理 focused 162 passed：`artifacts/operations/daytrade_source_gaps_20260910/official_proxy_regression.log`。Ruff、py_compile、git diff --check 通過。這些不是完整年度訓練或 full-epoch 測速。

最新繼承修訂另跑 `test/test_day_trade_attention_parity_candidate.py`、`test/test_tw_day_trade_minute_execution.py`、`test/test_checkpoint_manifest.py`，共 **139 passed / 8.57 秒**；修改測試的 Ruff 與 `git diff --check` 通過。新增測試逐欄限制 base recipe 的差異，並驗證 2014～2026 的 11 個年度 folds 及 Vast 候選的共同假設。

本機 Python 3.12.14、Torch 2.13.0/CUDA 13、RTX 5070 Ti；Vast `/root/stockAgent-daytrade-parity-20260910` 為 Python 3.12.14、Torch 2.11.0+cu128、雙 RTX 5090。兩端 strict environment warnings/failures 均空。

Vast 使用固定 release `tw-minute-train-20260910T121601063496311Z-l0-penguin-09bedd96a2f68c39`；後續已完成遠端 materialization、物件驗證、Syncthing convergence 與新 code 傳送，詳細證據見 [Vast 訓練整合實測](DAY_TRADE_VASTAI1T_TRAINING_TEST_2026-09-10.md)。

候選 `train.py --check-data-only` 現已通過，解析 3,095 個 panel sessions、2,754 檔、99 features 與 11 個年度 folds；沒有建立模型、optimizer、checkpoint 或 fold completion marker。

## 7. 最新端到端驗收與可用命令

正式 source cache v8 固定 release ID、manifest 與 READY proof；它包含 2,797,369 個分鐘股票日、2,982,036 個日 K 代理股票日、18,688 件 exact cash entitlements，並明列 76,615 個官方無一般交易股票日與 4,229 個 source-gap 股票日。官方無成交或來源缺口不會製造價格／成交；新單容量歸零，既有持倉仍依可驗證 mark、FIFO 與公司行動狀態延續。

本輪另修正兩個只有真實長路徑才暴露的錯誤：重複局部平倉的 entry fee allocation 會讓剩餘 `ENTRY_COST` 變負；physical FIFO artifact saver 仍要求 legacy T+2 cash queue。現在局部平倉依剩餘絕對股數保留成本，physical 稽核明列 `cash_settlement_queues_applicable=false`，artifact/report labels 也不再宣稱 T+2。

本機與 Vast 最終相關回歸均為 **921 passed、2 skipped**；Ruff、py_compile、`git diff --check` 通過。全新真實資料 smoke 完成 2014 train、2015 validation、2016～2026 full-horizon test；`progress.json` 為 `state=complete`、`failure=null`，2,605 個測試日每天恰有 270 點 float64 分鐘 NAV，期末 alive、0 default、0 open FIFO lots、0 outstanding claims。這證明工程就緒，不證明一輪模型績效；smoke 不得部署。

在 Vast 執行：

```bash
cd /root/stockAgent-daytrade-training-20260910
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_attention_training_vastai1t.yaml \
  --check-data-only
run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_attention_training_vastai1t.yaml
```

標準命令會依設定自動啟動兩張 GPU 的 DDP、1000 epochs 與 11 folds；不要帶 smoke 的 `--epochs 1`、`--max-folds 1` 或 acceptance output override。正式輸出仍位於 Vast 容器 overlay；完成 fold／模型須通過 lifecycle 後再送 bounded artifact ingress／cold store，否則不能把容器內檔案稱為耐久備份。
