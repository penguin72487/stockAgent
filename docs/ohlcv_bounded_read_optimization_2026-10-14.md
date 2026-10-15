# 分鐘行情最近窗口讀取最佳化

Binance／OKX 的最近窗口讀取已改為經驗證的 Polars slice／projection
pushdown，不先展開巨大的 Parquet 最末 row group。固定來源的完整請求輸出
逐位元相同，324 項相關回歸通過；沒有 provider 呼叫、來源覆寫或 GPU 工作。
這是 common reader 的改善，不是全市場下載已達官方上限的宣告。

## 根因與處理

既有 `ohlcv_hot_tail` 用 footer 選出最末幾個 row groups，再全部解碼、最後
才 slice。實際 BTC 檔每個 group 可有數十萬列、47～63 欄；只讀 32 列也會
暫時展開大量不要的歷史。指定 columns 時還是在解碼後才刪欄位。

現在對大於 8192 列且超過請求長度的候選 groups，在 reader 內把 slice 與
選欄交給 Polars；`glob=False` 避免檔名的括號等符號變成搜尋規則。小檔保留
Arrow，避免引擎啟動成本。巢狀、dictionary／extension 或不熟悉的 dtype
仍走既有 Arrow 路徑；native 結果 schema 不一致也回到 Arrow，不自行轉型。
來源 dev／inode／size／mtime／ctime 在 native 讀取前後不一致時拒絕結果，
避免 footer 與內容跨過一次 atomic replacement 而混用。

`read_logical_parquet` 的 base／hot-tail 優先序、同時間戳最後非 NULL 欄位、
物理末列、缺欄補 NULL、單位與所有來源欄位均不變。沒有量化、補值或重寫
原始檔。這是有界讀取，不會驗證未請求的歷史頁；完整來源 audit 仍是另一
責任，也不以 tail_rows 當成全歷史完整性證據。

本機使用 Polars 1.38.1、Arrow 25.0.0，沒有升級套件；使用已實測有效的
功能，而不是以套件年份代替效能證據。
[Polars scan_parquet](https://docs.pola.rs/api/python/stable/reference/api/polars.scan_parquet.html)

## 完整請求操作實測

先完整複製實際來源 base／hot tail，再保存修改前 reader。Binance base 為
3,728,175 列、735,177,444 bytes、63 欄；OKX 為 3,586,317 列、470,656,420
bytes、47 欄。不是合成的完整市場，也不是原始 HTTP payload。來源副本與
reader SHA 在 benchmark 前後均核對。

每版各三個獨立程序、交錯順序、兩個 CPU threads；計時含完整所需列的
解碼、base／tail 合併和完整輸出 hash。hash 包含列序、欄序、型別、NULL mask
與所有非 NULL 數值位元，包括負零與 NaN；不是只比 top-N 或容差。
完整程序啟動時間與受測操作時間分列於收據。未清 OS page cache，其他正式
服務仍運作；下列中位數不外推到其他主機或全庫平均。

| 請求 | 原讀取秒 | 新讀取秒 | 原 RSS MB | 新 RSS MB |
| --- | ---: | ---: | ---: | ---: |
| Binance 所有欄，最近 32 列 | 1.255 | 0.448 | 559.92 | 228.40 |
| Binance 所有欄，最近 4096 列 | 1.443 | 0.518 | 564.28 | 233.73 |
| Binance OHLCV，最近 32 列 | 1.658 | 0.614 | 543.94 | 206.25 |
| OKX 所有欄，最近 32 列 | 1.529 | 0.625 | 394.34 | 224.45 |
| OKX 所有欄，最近 4096 列 | 1.185 | 0.646 | 393.34 | 224.12 |
| OKX OHLCV，最近 32 列 | 0.903 | 0.449 | 403.73 | 211.50 |

RSS 是獨立 worker 的 VmHWM，包含 imports；MB 採十進位。每組完整輸出相同。
這個 reader 沒有改動每分鐘下載器的配額／分頁；不能把讀取收益直接稱為
provider 抓取吞吐提升。既有常駐程序不 hot reload，沒有因此重啟交易服務；
新程序使用新版，長駐 owner 是否實際載入仍須另驗。

收據：

- [修改前固定來源與 reader](../artifacts/benchmarks/running_services_20261013/ohlcv_tail/fixture.json)
- [原型 36 次對照](../artifacts/benchmarks/running_services_20261013/ohlcv_tail_prototype.json)
- [最終穩定來源版本 36 次對照](../artifacts/benchmarks/running_services_20261013/ohlcv_tail_stable_generation.json)
- [324 項最終回歸](../artifacts/operations/agent-workflow/runs/ohlcv-tail-final-regression-20261013T161527-268a9f13/run.log)
- [00:17 五種台股盤後正式快取再驗收](../artifacts/operations/running-services-optimization-20261013/postclose-after-reader-acceptance.json)

測試另覆蓋欄位投影、短／長窗口、巢狀及字典 fallback、NULL、NaN payload、
負零、損壞的請求頁、原子替換競態及原有 panel／collector／materialization。
Ruff 與修改範圍 git diff --check 通過；沒有宣稱跑完全 repository suite。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_ohlcv_tail_reader.py \
  --fixture artifacts/benchmarks/running_services_20261013/ohlcv_tail \
  --current --repeat 3 \
  --output artifacts/benchmarks/running_services_20261013/ohlcv_tail_retest.json
```

輸出須全新；新來源用全新 fixture 路徑及 `--prepare-only`，先保留至少 30 GiB
空間，再保存修改前 baseline。舊 fixture、失敗收據與來源都不刪除。

## 執行服務的另一項實際故障

00:01 的 registered intraday 追新中，Binance 575 檔約 31.88 秒，OKX 500 檔
約 59.53 秒；Bybit 在讀 API 前即因 `data_bybit` 指向不存在的 materialized
版本而失敗。PID 1 與 IDE 兩個 mount namespace 都找不到該 target，不是
把 shell 的不可見目錄誤當成正式服務缺檔。
[Bybit canonical observer](../artifacts/operations/agent-workflow/runs/bybit-catalog-readonly-20261013T160910-cb1e21a6/run.log)
回報 cold-only、索引 payload 約 24.18 GB；尚未進行完整 SHA 還原驗收。

儲存技能讓這次停止於查證，不建立空來源或把 downloader 寫進 immutable
cache。這仍是待修復事項：後續需以固定 release 恢復到獨立可寫 producer
workspace、核對原始值／收據，再沿原 owner 接續追新，不能把 cold-only
觀測說成 Bybit 已恢復。完整 all-service 目標仍 active。

盤後 GPU／RAM 與五種正式推論的驗收見
[盤後推論快取](completed_session_inference_cache_2026-10-13.md)；較早 FinMind／
OpenBB 改善與完整服務普查見
[本機效能報告](running_service_optimization_2026-10-13.md)。
