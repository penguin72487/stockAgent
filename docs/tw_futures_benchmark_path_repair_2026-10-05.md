# TX benchmark 搬移後的路徑修復

## 根因與範圍

`accepted-v5/source/train.py` 使用的 v5 設定，仍繼承 v2 中已不存在的
`artifacts/markets/tw_futures_v8_margin_preparation/verified_scope_2011_20260928/daily/continuous_daily.parquet`。
原 pin 為 `8f3f62da483b6b7ee1652b9c615be8f7b41b4305cfca84a459fa3aeb8e5ed551`。
搬移後保留的是 `data_tw_index_futures/preparation_sources/` 中的官方來源。

根目錄的 `data_tw_index_futures/day_session_contracts.parquet` 格式不同，且其
資料版本較舊；直接替換會改變資料截止日。主 margin-components release
不含 TX，也不能用它建立大台參考線。

本次只在 [v5 設定](../configs/markets/tw_futures_v8_margin_components_causal_account_capital100m_v5.yaml)
明確覆寫 benchmark 路徑與 SHA；保留一億元、lookback 32、年度 +32、1000
epochs、每輪驗證、100 次未改善早停、交易資料與模型。沒有啟動訓練。

## 恢復來源與驗證

新參考資料：
`data_tw_index_futures/benchmarks/tx_front_rolling_1x_gross_2011_20260904_v1/continuous_daily.parquet`。
SHA-256：`5285fd33e54977cddf34b730324871ed16d0ea3b225df43e883f7ba7dae1cabb`。

[重建入口](../scripts/build_tx_front_benchmark.py) 讀取保留的官方 observations，
核對其 manifest、normalized Parquet SHA 及 37 份 raw archive SHA，重用
`_contract_metadata()`、`_stable_rank_map()` 及
`load_tx_front_rolling_benchmark()`。不插值、補成交價或把遠月遞補為近月；
缺近月／前一日同合約收盤價時拒絕完成。

新檔包含 21,030 筆有效 TX 月契約收盤，2011-01-03 至 2026-09-04 共
3836 個交易日、188 次轉倉。截止日保留原版本，不擴到來源較新的
2026-09-24。轉倉日仍用新近月合約自己的前一日收盤價，benchmark 為
1 倍名目本金、未扣費稅的大台連續轉倉。

來源是官方價格；舊 `futures_benchmark_audit.npz` 只用於事後比較。兩份
舊稽核各有 3307 個日期，2013-02-25 至 2026-09-04，日期、每日報酬、
合約、轉倉旗標、當日與前一日同合約收盤、覆蓋旗標逐項完全相等。

這是重新產生的 benchmark 投影，**不是原 Parquet 的逐 byte 恢復**。
新路徑／內容身分改變 checkpoint fingerprint；原設定與舊產物保留，
不允許混用舊 optimizer resume。修復時 v5 正式目錄只有啟動計時檔，
沒有 checkpoint，因此仍可使用原 v5 正式目錄從 Fold 1 開始。

- [資料來源、37 份原件與逐日相等收據](../data_tw_index_futures/benchmarks/tx_front_rolling_1x_gross_2011_20260904_v1/manifest.json)
- [原 frozen v5 loader 的路徑／hash 驗證](../artifacts/analysis/tw_futures_benchmark_path_20261005/frozen_loader_verified.json)
- [原 1199 檔程式源與 bundle 驗證](../artifacts/analysis/tw_futures_benchmark_path_20261005/frozen_runtime_verified.json)
- [最終驗收](../artifacts/analysis/tw_futures_benchmark_path_20261005/acceptance.json)

8 項來源／轉倉語義測試通過。原 accepted-v5 的 `--check-data-only` 退出 0，
接受 3881 個 panel 日期、2754 個 context symbols、99 個特徵及從 Fold 1
開始的全部 14 folds；沒有建立模型、optimizer、checkpoint 或完成標記。

## 使用者啟動

既有 accepted-v5 程式不需要改寫；使用 checkout 中已修正的外部設定。

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
export STOCKAGENT_CODE_RELEASE_RECEIPT="$PWD/artifacts/runtime/tw_futures_causal_account_v5/code-release-v5/release.json"
export OMP_THREAD_LIMIT=1 OMP_WAIT_POLICY=PASSIVE KMP_BLOCKTIME=0

run_fintech_python artifacts/runtime/tw_futures_causal_account_v5/accepted-v5/source/train.py \
  --config configs/markets/tw_futures_v8_margin_components_causal_account_capital100m_v5.yaml \
  --start-fold 1
```

路徑修復與來源驗證不代表策略績效提升；既有模型研究結果見
[causal account policy 報告](tw_futures_causal_account_policy_2026-10-04.md)。
