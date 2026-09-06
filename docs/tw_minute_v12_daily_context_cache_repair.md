# 分鐘 v12 日線 context 快取路徑修復

## 根因（2026-09-05）

`tw_minute_dataset_verify` 在模型建立前失敗：設定透過
`data_tw_public/stocks/panel_cache_v2/variants/fe882a5c...json`
尋找日線 context。`data_tw_public` 指向 packed release 的 materialization，
但 `panel_cache_v2` 是刻意排除於 release 的本機衍生快取。
因此重新 materialize 後，原始資料可以完整，這個快取路徑卻不存在。
這不是 Q/K normalization、CUDA 或 NCCL 錯誤。

## 修復與不變量

`configs/markets/tw_minute_daily_guided_v12_dual_5090_qk_norm_v3.yaml` 改用既有的
`artifacts/cache/tw_day_trade_hybrid_minute_v12_reference/` 內獨立保留的快取，
固定 snapshot `tw-public-20260820T015018219623218Z-l0-penguin-6716296ea30636ba`，
不跟隨 `data_tw_public` 的符號連結，也不在不可變 materialization 裡重建快取。
修復僅覆寫 v3：目前 checkpoint fingerprint 也包含路徑，
因此不能順手修改舊 control/throughput-v2 的設定，破壞其續訓相容性。
正式 v3 只有啟動失敗紀錄、沒有 checkpoint，不需要遷移 optimizer。

- Panel version：54。
- Generation：`3726b829b37d4d56833a80b20280e4a2`。
- Source hash：`31f3e4f1e31338bb9c0fd52494cc0d840a5081ffeab00a1fceddfa3412258fe0`。
- Metadata SHA-256：`bb5335429a2da10a36906a688ab64ae080e5ad02f50ab110b5a3e8faf51f922a`。

Metadata、股票及 feature 名稱檔與隔離保留的舊副本逐 byte 相同；
26 個陣列均通過 canonical `array_content_fingerprint` 的完整內容驗證。
沒有恢復隔離樹、切換資料 release、修改特徵或更換 benchmark。
`qk_norm: true`、BF16、雙卡 DDP、1000 epochs 與交易規則不變。

## 驗證

完整分鐘資料 index 可載入：1,565 個交易日、2,620 檔標的；
首日具備 32 日日線 context。分鐘分區沿用現有 SHA-256 receipt 驗證機制。
日線 context 缺失仍必須明確失敗，不自動選擇另一個 cache variant 或新版資料。

67 項相關測試與 strict CUDA 檢查通過；雙 RTX 5090 的 Fold 1、
1 epoch smoke 正常退出，包含訓練、validation、2023 第一測試年 loss、
完整 fold 測試與圖表。Smoke 目錄為
`artifacts/markets/tw_minute_daily_guided_v12_qk_norm_v3_cache_repair_smoke_20260905_1425`。
資料 fingerprint 與舊實驗一致：
`7ee78d3eafea979177f1a582a4b8ad063272b3fcd2de08a9c68fce125ca18f78`。
這是工程驗證，不是正式訓練完成或獲利證明。

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q test/test_tw_minute_mode.py test/test_tw_minute_daily_guidance.py test/test_panel_cache_v2.py
run_fintech_python train.py --config configs/markets/tw_minute_daily_guided_v12_dual_5090_qk_norm_v3.yaml
```

正式指令不變；smoke 必須另指定輸出目錄與 `--epochs 1 --max-folds 1`，
不得把 smoke checkpoint 放回正式訓練目錄。
