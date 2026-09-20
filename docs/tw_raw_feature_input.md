# 台股多基底原始值輸入實驗

`configs/markets/tw_day_trade_daily_multi_basis_raw_ohlcv_v1.yaml` 是獨立實驗；不改寫既有多基底基準的 24 個工程特徵或 checkpoint。新設定只選 7 個數值欄位：標準化日線檔的開、高、低、收、成交量，以及官方日線的成交金額、成交筆數。這 7 欄送進多基底前不做 log、log1p、asinh、報酬率、差分或 K 線比例。官方表還保留其原始成交量欄，可供獨立比對，但新設定不重複餵入。

「原始」指資料來源的數值語意，不等於逐位元未處理：日線讀取器仍沿用既有價格精度規則；缺值仍由 panel 的既有缺值政策處理；可交易、漲跌停、報酬標籤、成本及清算仍照原契約。市場在開盤後才公布的完成日線只可供下一個交易日決策。這個實驗不使用 `next_session_open_gap_logret`，所以 09:00 當下的開盤報價不進模型，但仍供執行和成交估值。要保留當下開盤資訊，必須另做因果安全的原始開盤通道與 live 同步驗證，不可把次日高低收直接放進輸入。

模型的 `temporal_basis_input=input_features` 仍把多基底係數與上述原始數值一起送進同一個 `CandleEncoder`。`RMSNorm`、SwiGLU 投影、後續 Transformer、BF16 AMP 都是模型內計算，未移除；fold 層的 `causal_feature_rms_normalization` 在此設定為 `false`。因此這是「原始欄位進入模型和多基底」，不是「完全沒有模型內正規化」。原始價格與成交量的量級相差很大，不能假設換成原始值一定提升泛化或績效，必須用獨立訓練、相同 fold 與完整回測比較。

研究候選宏觀帳 `artifacts/data_quality/tw_public_provisional_macro/events.parquet` 另提供 `_raw` 欄位，包括可為負的稅額。其歷史版本尚未符合嚴格 PIT，`strict_pit_eligible=false`；這些候選值**沒有**加入本訓練設定。準確發布時刻只解決可用日，不證明今天的歷史整包數值就是當年初版。

重建既有官方特徵表後，先檢查數值和資料可用性，再評估是否啟動獨立訓練：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/build_tw_public_training_features.py \
  --input-dir data_tw_public \
  --output-path data_tw_public/features/tw_public_stock_daily.parquet \
  --symbols-root data_tw_public/stocks
run_fintech_python train.py \
  --config configs/markets/tw_day_trade_daily_multi_basis_raw_ohlcv_v1.yaml \
  --check-data-only
```
