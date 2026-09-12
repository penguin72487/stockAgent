# Vast Attention LayerNorm 訓練整合與測速

## 1. 執行進度

已在 vastai1T 建立 `/root/stockAgent-daytrade-training-20260910` 隔離 worktree，部署本輪程式、測試與候選部署設定，並在雙 RTX 5090 上執行真實 NCCL、BF16、AdamW、模型反向傳播及 physical FIFO 跨 batch 測試。

**正式訓練的工程與資料 gates 已就緒，但 1000 epochs 尚未啟動。** Accepted source、年度 train/validation/test/deployment caller、physical FIFO archive、跨 fold 狀態、checkpoint/resume 與正式報表已接入既有 `train.py`，並在 Vast 雙 RTX 5090 上完成一個全新的一折一輪真實資料端到端驗收。正式輸出目錄仍沒有 checkpoint 或 fold completion；只有 `--check-data-only` 的啟動測時紀錄。

沒有啟動 1000 epochs 正式訓練、切換網頁／Discord、改 live 帳戶、送券商委託，或把合成資料寫成正式來源。

## 2. 保留的實驗契約

- 沿用指定 `tw_day_trade_hybrid_minute_v12_attention_full_then_last_layernorm_commission20_capital10m_all_features_v1` 的模型與未另指定設定。
- 2014-01-06 起、11 個年度 expanding folds、matching-train-and-validation-years 預訓練初始化、BF16、DDP、1000 epochs、10M。
- 09:00 決策／sizing，歷史 09:01 執行，50% 分鐘量、整張容量、實體 FIFO 殘倉延續；既有研究假設全部可轉融資融券、零股按整張行情保留。
- 早於 canonical 分鐘起日的日 K 代理用官方 Open/Close，不加不利 tick，容量公式仍用 daily volume / 271 × 50%；不把後期缺分鐘自動 proxy。
- 單一成交價格遵守 dated tick；VWAP／NAV 與委託價格不是同一種量，不任意四捨五入會計 state。会計／分鐘權益維持 float64，模型運算 BF16。

## 3. 本輪實作

`stockagent/training/day_trade_carry_bridge.py` 是共用 trainer 的 execution adapter，不含另一套 optimizer、loss、報酬公式或 downloader。

- `PreparedDayTradeCarrySource` 固定來源 ID、有序股票與交易日；同股票順序直接重用張量，只有子集／排序需要 index-select。
- 尾批 padding 不重複交易、計息或企業行動。缺日、倒序、重複日期、非有限模型輸出與失敗帳戶不允許交給通用 skip-batch 邏輯略過。
- 未解的舊 corporate-action／forced-exit 欄位不可默默刪掉；來源 adapter 必須先轉成 physical 義務。舊權重／T+2 buffer 不冒充實體庫存。
- 單機 epoch 逐 batch 傳遞 detached FIFO state；DDP 在 global batch 聚合後維持共同帳戶，不讓各 rank 各自重設本金。仍按原 batch optimizer cadence 更新。
- 共用 eval 保存 float64 每日 270 點 NAV、股數及真正期末 state；新增明確 segment 初始 state 入口，驗證分段前一交易日，不用尾日權重反推庫存。
- 年度正式 orchestration 已自動提供上述 source/context/state，並用全新輸出目錄驗證訓練、全期回測、archive、結算稽核、fold marker、walk-forward 報表與 resume。任何來源缺口仍依 accepted-source contract fail closed 或給零新單容量，不能由 caller 靜默掩蓋。

## 4. 已執行的測試與效能範圍

本機最終較大回歸：490 passed、2 個需 opt-in GPU 的測試 skipped，30.61 秒；Ruff、py_compile、git diff --check 通過。另已在 Vast 實際執行兩個 GPU 測試；本機 skip 不表示遠端未測。

小型參考模型真實兩-rank 測試：跨 5 個合成交易日、尾批 padding、兩次 epoch／optimizer 更新，兩個 rank 的參數逐值一致，並與相同更新 cadence 的 CPU reference 相符。

指定 FinancialTransformer 架構測試：2330 檔、99 特徵、lookback 32、22 basis（PCA 用測試訓練 windows 擬合）、full_then_last、LayerNorm、BF16、共用 fused AdamW、共用 DDP static-graph wrapper 及 panel-slab。使用 **5 個合成交易日**、global batch 2，非正式預訓練移轉、非全歷史訓練、非完整 epoch 報表工作流。

| 測項 | 遠端觀察 |
| --- | --- |
| Eager 指定模型，rank 0 兩次 bounded training-loop | 1.76986 s、0.27489 s |
| Eager 指定模型，rank 1 | 1.76786 s、0.27193 s |
| Peak allocated，各 rank | 856099840 bytes（約 816 MiB） |
| Compiled panel-slab，rank 0（已有 compiler cache） | 啟動 10.79794 s、第二次 loop 0.24414 s |
| Compiled panel-slab，rank 1 | 10.79901 s、0.24392 s |
| Compiled peak allocated，各 rank | 764274688 bytes（約 729 MiB） |
| 參數／梯度 | 更新後有限、存在非零梯度，兩個 rank 參數逐值一致 |
| CPU 16 日 × 2330 檔 batch 準備，舊 identity-copy 參考 | 7 次穩態中位數 19.3577 ms |
| CPU 同批張量重用 | 7 次穩態中位數 0.3615 ms |

最後兩列只測被移除的冗餘複製，約 53.5 倍是該小段的比例，**不是整體訓練加速比**。合成來源各日可共享儲存，沒有測真實來源磁碟 IO、年度資料量、乾淨 compiler cache 的完整冷 epoch、validation/test/plots/checkpoints 全 epoch，也不據此修改正式 batch 16 或宣稱最佳配置。編譯版本僅有一個穩態 loop 樣本，不據此宣稱優於 eager 的穩定幅度。

Opt-in 測試最初暴露測試入口漏接 PCA covariance 和未使用 canonical DDP static-graph wrapper；已修測試裝配，沒有刪除 PCA、改為 find-unused 每輪掃描、或改變模型架構來過關。

編譯測試第一次因只檢查最後一批非零梯度失敗；空目標／容量約束下最後一批可以合法無梯度。改以共用 trainer 全 epoch 梯度統計驗證，而非關閉檢查：兩次 norm sum 為 72.31970、56.19322，zero batches 分別 2/3、1/3，兩個 rank 一致。參數有更新且保持有限。另加獨立 NaN gradient 注入測試；physical 訓練每批檢查有限性，遇到錯誤立即失敗，不讓帳戶前進後悄悄略過 optimizer 更新。

擴大遠端回歸首次為 471 passed／19 failed：隔離部署漏帶最新 replay／promotion scripts 與 corporate-action receipt helper，造成 13:24／13:25 時序及 helper 參數不一致。補齊這些程式依賴，未改 oracle、放寬比較或執行下載。**修復後遠端 490 passed、2 個 opt-in skipped／22.32 秒。** 失敗原始 log 保留為 `final_regression.log`，修復後重跑 log 為 `final_regression_dependencies_repaired.log`。

早期 machine-readable synthetic evidence 位於新 worktree 的 `artifacts/operations/daytrade_training_20260910/`，保留作為歷史測速資料；其 `formal-ready=false` 已由後續真實資料驗收取代，不是目前狀態。最新驗收產物位於 `artifacts/acceptance/physical-v8-contract-smoke/`。

## 5. 遠端準備與資料界線

- 新 worktree base：`2d399eccbab241ca8d57a6caf536456c0b60e27f`；主 worktree 已在較新 `0b77c84`，沒有 reset 或覆蓋主 worktree 的 dirty changes。先前 parity worktree 也保留。
- 程式透過檢查後的 Git binary patch 傳到隔離目錄；資料沒有用 SSH 複製，沿用 catalog／Syncthing／`run_data_cache.sh use`。
- Runtime：`/venv/fintech`、Python 3.12.14、Torch 2.11.0+cu128、雙 RTX 5090；strict CUDA environment 通過。沒有套件升級或服務重啟。
- 新部署檔 `configs/deployments/tw_day_trade_attention_training_vastai1t.yaml` 只更改隔離 output/cache 與預訓練絕對路徑；交易、模型、年度設定不變。
- 預訓練 11 個 checkpoint 均透過 canonical resolver 成功讀取，summary／checkpoint 年度相符，各 SHA-256 記於 `pretrained_sources.json`。這不是在新 physical objective 下跑過 epoch-zero validation guard。

公開資料固定 `tw-public-20260910T100453368085496Z-l0-penguin-234efa703efde4b2`：本輪完整驗證 115997 邏輯檔、9616112077 bytes；edge payload 驗證 1332 個檔、12070832007 bytes，lease 更新至 2026-09-17T15:43:33Z。Receipt：`/var/lib/stockagent-packed-edge/receipts/packed-edge-1789055075586150527.json`。

正常 index-only use 流程完成後，自動釋放 1332 個已驗證的本機冗餘 payload，共 12070832007 bytes；權威 cold、materialized 資料與 source 未刪除，可由權威節點再次 hydrate。未改變 Vast index-only 角色。這一版不等於本機稍後新修復的 action 來源全已發布。

分鐘固定 `tw-minute-train-20260910T121601063496311Z-l0-penguin-09bedd96a2f68c39`：本輪完整驗證 3189 邏輯檔、13817707306 bytes；900 個 payload 雜湊共 14015770970 bytes，lease 更新至 2026-09-17T15:51:30Z。Receipt：`/var/lib/stockagent-packed-edge/receipts/packed-edge-1789055522407028882.json`。流程完成後只釋放同量本機冗餘 payload，materialized/source/權威 cold 不受影響，仍可再次 hydrate。

完成後 canonical edge audit：`hydrating={}`、本機冗餘 payload 0；penguin 連線 `quic-server`、TLS 1.3；folder idle、所有本地／peer need byte/item/delete 都 0、folder/system/pull/watch errors 都空、completion 100%、remoteState valid。其後正式 source cache v8 另驗證 18,688 件精確現金權利、2,797,369 個分鐘股票日、2,982,036 個日 K 代理股票日、76,615 個官方無一般交易股票日及 4,229 個明列 source-gap 股票日。後兩類不捏造成交；既有持倉仍延續估值與公司行動狀態。

遠端 workspace 是 overlay；新正式產物的離機保存尚未驗收，不把容器內留存視為可靠持久備份。

## 6. 可重跑的整合測試與正式訓練指令

在 vastai1T：

```bash
cd /root/stockAgent-daytrade-training-20260910
export FINTECH_ENV_PATH=/venv/fintech
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict

OMP_NUM_THREADS=8 run_fintech_python -m torch.distributed.run \
  --standalone --nproc_per_node=2 -m pytest -q -s \
  test/test_day_trade_carry_training.py -k real_ddp

STOCKAGENT_TEST_ATTENTION_CARRY=1 OMP_NUM_THREADS=8 \
run_fintech_python -m torch.distributed.run \
  --standalone --nproc_per_node=2 -m pytest -q -s \
  test/test_day_trade_carry_training.py -k named_attention

# Optional compiled panel-slab integration; still synthetic, not formal training.
STOCKAGENT_TEST_ATTENTION_CARRY=1 STOCKAGENT_TEST_CARRY_COMPILE=1 \
OMP_NUM_THREADS=8 TORCHINDUCTOR_COMPILE_THREADS=4 \
run_fintech_python -m torch.distributed.run \
  --standalone --nproc_per_node=2 -m pytest -q -s --show-capture=no \
  test/test_day_trade_carry_training.py -k named_attention

run_fintech_python scripts/benchmark_tw_day_trade_inventory.py \
  --prepared-batch --device cpu --symbols 2330 --batch-days 16 \
  --repeats 7 --threads 8 \
  --output artifacts/operations/daytrade_training_20260910/identity_copy_cpu.json
```

正式啟動前的最後唯讀驗收與正式命令如下；標準 `train.py` 會依設定自動以兩張 GPU 重啟 DDP，不需手動包 `torchrun`：

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

不要加 smoke 的 `--epochs 1`、`--max-folds 1` 或 `--output-dir artifacts/acceptance/...`。正式設定本身固定 1000 epochs、11 個 expanding annual folds、resume、BF16/DDP 與獨立正式輸出目錄。

## 7. 最終正式就緒驗收

最終修復的兩個實際失敗點：重複局部 FIFO 平倉會重複扣完整 entry fee，最後形成負 `ENTRY_COST`；physical FIFO fold 的通用 artifact saver 又錯誤要求舊 T+2 cash/payable/receivable histories。前者改成依剩餘絕對股數保留未實現 entry cost，後者輸出 physical FIFO 專用結算稽核，明列舊 cash queues 不適用。另修正 artifact 與 console label，不再把 physical FIFO 帳戶標成 legacy T+2。

最終本機與遠端相關回歸均為 **921 passed、2 skipped**；遠端 dashboard 子集另為 **24 passed**，Ruff、py_compile、`git diff --check` 通過。本機與遠端 `trainer.py` 及候選契約測試的 SHA-256 相同。

全新的 `physical-v8-contract-smoke` 不是舊結果續用：它以雙 RTX 5090、BF16/DDP 實際完成 fold 1 的 2014 train、2015 validation、2016-01-04～2026-09-10 test，共 2,605 個測試 session、2,754 檔、99 features。`test_backtest.npz` 保存 `(2605, 270)` float64 分鐘 NAV、`(2605, 2754)` 股數、`(2605, 2754, 10)` FIFO cohorts 與 `(2605, 2754, 3)` dated claims。完成標記為 `state=complete`、`failure=null`、fold 1 complete；期末帳戶 alive、0 default、0 open FIFO lots、0 outstanding cash claims。

該 smoke 只證明整合可訓練、可回測、可續跑與可原子落盤，不證明一輪模型有可交易績效。這次一輪 test cumulative return 約 -89.63%，不得拿來部署或取代正式 1000-epoch 模型。

Vast worktree 與目前訓練輸出位於容器 overlay。可以開始訓練，但 instance 被 destroy/recycle 前，完成產物仍須依 lifecycle 驗收後發布到既有 bounded artifact ingress／cold-artifact 流程；容器內存在不等於異機耐久備份。
