# vastai1T 隔日沖訓練準備

本次使用獨立 checkout `/root/stockAgent-overnight-1325-v2`，程式基於
`b751c73526a32906f2b5b7acd7f386a2fe3e45d1`。原本 `/root/stockAgent` 的期貨
修改與既有訓練產物保留。兩個目錄使用各自的訓練輸出與日線 cache。

## 啟動

在 vastai1T 終端執行：

```bash
cd /root/stockAgent-overnight-1325-v2
bash start_training.sh
```

只驗證準備狀態而不訓練：

```bash
bash start_training.sh --check-only
```

啟動器讀取 fintech 環境、檢查 CUDA、核對已驗收的程式／設定雜湊與固定資料版本，
然後呼叫共同 `train.py`。預設 GPU `0,1`、BF16、全域 batch 16、每 fold 上限
1,000 epochs，沿用基準 early stopping 與每 epoch 報告，支援原有 checkpoint resume。
CPU thread budget 為 112、compile thread budget 為 16，兩者由既有 DDP 入口分配。
若指定 GPU 當時已有運算程序，啟動器會顯示 PID 並等待空閒；Ctrl-C 可取消等待。
同一個隔日沖 checkout 的啟動器有程序鎖，避免重複啟動。

正式結果位於：

```text
artifacts/markets/tw_overnight_1325_close_fallback_next_open_multi_basis_22_projection_l1_capital10m_v2
```

這次準備只執行有範圍限制的驗收訓練，正式 1,000-epoch 訓練由使用者啟動。
驗收輸出位於 `artifacts/operations/vastai1t_preparation`，與正式 checkpoint 分開。

遠端另有期貨實驗持續使用 GPU，因此本次驗收限制每張 GPU 的 PyTorch allocator
最多 8 GiB、CPU 8 threads、compile 2 threads，關閉驗收用 GPU tensor cache，
eval batch/chunk 為 8。訓練 batch 仍為 16，保留 BF16、DDP、模型與 loss 編譯、
全部 train/validation/test/報告流程。這是整合驗收，時間不能當成獨占 GPU 的效能基準；
正式啟動器不載入驗收專用的記憶體上限或小型評估設定。

本次實際 DDP 驗收另修正共同交割編譯器的短批次入口：batch 16 現在會重用既有
單日 compiled kernel，嚴格 probe 必須觀察到真正 compiled calls。32 日完整區塊
及其後的 eager tail 保留；不透過增加 batch 或關閉嚴格檢查迴避問題。新增 CUDA
測試核對 1／16／31 日的全帳本輸出、action gradients 與單一編譯圖，並保留 35 日
完整區塊＋尾段的回歸測試。

## 固定資料與交易假設

| 資料 | 固定 release |
|---|---|
| 日線與官方規則 | `tw-public-20260906T151104843336596Z-l0-penguin-246aab6e5c72e427` |
| 分鐘研究分區 | 已存在的 `/root/stockAgent/data_tw_minute/research_dataset_stats_v4`；manifest SHA256 `05aba286a2c619d9fea4dfc7b1a4466af4574075bbbb1533ffef37790a821449` |

曾嘗試以 `stockagent-data use` 展開 2026-09-08 日線 release，但發現其 117 個物件
具有 Syncthing 刪除紀錄，不能視為完整冷庫。這次保留缺件清單，改用遠端已存在的
2026-09-06 materialization，透過共同 inventory 與 `_verify_materialized` 重新核對
所有檔案 SHA256、完整 portable fingerprint 與原 READY。這只證明現存訓練資料可用，
不宣稱冷庫已修復。分鐘來源沿用本機已保存的 v4 研究分區，逐分區 SHA256 由共同
13:25 loader 驗證；它不是宣稱已 materialize 的 `tw-minute-train` 冷庫 release。

checkout 的資料連結指向上述確定版本，cache 與訓練輸出留在 checkout 的 `artifacts`。
日線選取版本另有 canonical pin，避免在使用者尚未開始訓練前被七日 cache GC 回收；
不因此宣稱 Vast 容器的檔案能跨 instance destroy/recycle 永久保存。

策略保留原本 22 組 temporal bases、FinancialTransformer、lookback 32、TWD 1,000 萬
與兩折佣金。13:25 決定收盤進場權重，下一交易日開盤強制沖銷；本版只做多。
缺 13:25 價格時依使用者指示用同日收盤價替代，並記錄每個 symbol-session 的來源。
這些替代樣本含相對於 13:25 的前視資訊；日線 OPEN/CLOSE 亦只是競價參考價。

無法在下一開盤退出時，帳本保留既有 absorbing execution failure；不延長持倉，
也不把失敗回測顯示成正報酬。這是研究契約的失敗懲罰，不代表真實市場當天損失全部本金。
完整方法見 [隔日沖訓練契約](tw_overnight_1325_training.md)。

遠端驗收確實觀察到此限制：首個 epoch 的第一批次遇到開盤事件失敗，14 個訓練
batch 中後續 13 個為零梯度；整股測試回測的 `final_alive=false`，報表正確保留
-100% 的吸收性失敗標記。`ready.json.learning_health` 與啟動器保留這項提示。
目前已驗收的是資料、計算、checkpoint 與報表流程；完整年度的有效學習仍受上述
失敗狀態影響。沒有重設失敗帳本或用未來退出可行性回頭刪除候選股票。

## 驗收證據

完整資料稽核已通過：日線為 2014-01-02 至 2026-09-04，共 3,093 日、2,753 檔、
24 個模型輸入。分鐘 manifest 宣告 1,567 個分區，其中與正式日線交易日曆交集的
1,565 個分區完成雜湊驗證；實際使用 13:25 價格 1,769,619 個 symbol-session，
收盤替代 3,960,180 個 symbol-session。保留完整 2014 起的訓練期間。

工程驗收已於 2026-09-10（台北時間）完成：遠端 499 項測試通過，雙 RTX 5090
以 BF16、DDP 與嚴格模型／交割 loss 編譯跑完第一個完整 fold，沒有縮短該 fold 的
日曆（train 2014、validation 2015、test 2016 至 2026）。先跑 2 epochs，再從
checkpoint 接續第 3 epoch；前兩筆曲線完全保留，optimizer step 從 28 增至 42。
共同 lifecycle 已驗證 fold 產物與必要圖表，`bash start_training.sh --check-only`
成功，正式訓練輸出目錄仍未建立。這些是工程驗收結果；上述梯度與退出限制仍存在。

`ready.json` 已保存完整資料稽核、GPU 訓練與續訓驗收結果；啟動器會拒絕缺少或
失配的驗收證據。其餘收據包含：

- `environment.json`：實際 Python、PyTorch、CUDA 與兩張 GPU。
- `pytest.log`：遠端 495 項相關測試結果。
- `short_batch_compile_tests.log`：另 4 項 CUDA 編譯與帳本／梯度回歸結果。
- `data_audit.json`：panel 日期／股票／特徵、13:25 觀察與收盤替代數量。
- `existing_data_verification.json`：完整既存日線 materialization 驗證與 READY。
- `missing_public_objects.json`：最新冷庫 release 缺件證據。
- `ddp_smoke.log`、`resume.log`：實際 DDP 編譯、訓練、回測與報告。
- `resume_check.json`：曲線不變、epoch 3 與 optimizer step 續接證據。
- `start_check.log`：正式入口的環境、固定來源與程式雜湊檢查。
- `ready.json`：正式設定、程式雜湊、資料 pin 與訓練驗收摘要。
