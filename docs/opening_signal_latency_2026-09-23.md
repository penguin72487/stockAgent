# 2026-09-23 開盤訊號延遲優化

## 結論與證據邊界

本輪已部署訊號產生端與當沖引擎更新，不更換 checkpoint、不重算今天訊號、
不變更交易規則或開盤行情覆蓋門檻。五個模式在固定真實收據的離線暖機測試中，
全部發布的中位數從 2,283.20 ms 降至 894.74 ms（降低 60.81%）。
這不是實際開盤總延遲，更不是券商成交延遲，也不是冷啟動測速。

今天原始記錄來自 `artifacts/live/tw_day_trade_simulation/opening_signal_latency.jsonl`：

| 模式 | 開盤至訊號 ready | 開盤至原子發布 |
| --- | ---: | ---: |
| Attention LayerNorm | 2,676 ms | 2,700 ms |
| Multi-Basis 22 | 3,581 ms | 3,600 ms |
| Projection L1 GELU | 4,671 ms | 4,698 ms |
| Multi-Basis | 5,700 ms | 5,737 ms |
| 100M | 6,673 ms | 6,706 ms |

共用行情第一個 callback 在 09:00:01.310；覆蓋門檻在 09:00:02.163 達成。
Multi-Basis 到 09:00:04.864 才輪到排程，模型 forward 本身只花 43.254 ms。
第一個行情請求的引擎佇列等待為 830.794 ms，來源 fetch 為 1,192.081 ms。
因此只改 GPU forward 無法解決主要等待；也不能用今天尚未到達的行情宣稱一秒內出訊號。

## 實作

1. 開盤 cache hit 不再 seed 同一份約 2,300 檔收據、覆寫 JSON 或刷新接收 TTL。
   真正的新來源仍走原本驗證與持久化路徑；交易日、覆蓋門檻、遮罩不變。
2. 五個已完成盤前準備的模式並行執行狀態檢查，錯誤各自交回既有重試路徑。
   需要補做盤前準備的模式保留原來的完整檢查；模型及其全域 runtime 仍序列化。
3. 先原子發布所有到期模式的 execution weights + summary + pointer，
   再完成 Parquet、Markdown、Discord 報表。沒有省略任何報表或完整訊號。
   `artifact_complete=false` 保留到實際完成；延後完成不得覆蓋更新的 pointer。
4. feature explanation 只計算／排序一次，再切出 top N；單一 float 的有限值檢查改用
   `math.isfinite`，陣列運算仍保留 NumPy。
5. 設定解析優先使用 `CSafeLoader`，未安裝 C extension 時回退原 SafeLoader。
   沒有加入可陳舊的 config TTL；每次仍讀取來源、解析繼承、拒絕重複 key、
   Python object tag、錯誤布林型別與未知設定。
   依據：[PyYAML Loader 官方文件](https://pyyaml.org/wiki/PyYAMLDocumentation)。
6. 每日 receipt 新增 `mode_dispatch_queue_ms`、`realtime_prepare_wait_ms`；
   後者區分準備總耗時與輪到該模式時真正剩下的等待，兩者不可相加。
   `execution_pointer_written_at` / `input_to_execution_pointer_ms` 在原子 pointer
   寫完後量測。儀表板已有相應中文階段名稱。

## 可重現測速

使用今天原始行情收據的副本、09-22 完成的特徵與全部五個正式模型；
來源網路函式被禁止，生成檔案與收據寫入均隔離到 temporary directory。
計算出的 weights、prices、decisions、model explanations 逐模式 SHA-256 完全相同。
正式訊號 pointer、交易帳本及 Discord channel 不作為測試輸出。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_opening_signal_pipeline.py \
  --receipt artifacts/live/tw_opening_snapshots/2026-09-23.json \
  --feature-date 2026-09-22 --repeats 5 --legacy-control \
  --output artifacts/benchmarks/opening_pipeline_control.json
run_fintech_python scripts/benchmark_opening_signal_pipeline.py \
  --receipt artifacts/live/tw_opening_snapshots/2026-09-23.json \
  --feature-date 2026-09-22 --repeats 5 --defer-reports \
  --output artifacts/benchmarks/opening_pipeline_optimized.json
```

`--legacy-control` 在隔離程序恢復原本的重複 seed、重複 feature summary、
Python SafeLoader 與 scalar NumPy 檢查；不啟用報表延後。每組先完整暖機，
之後測五輪，計時不包含驗證雜湊，終點是實際 pointer 寫完。
可以另外加 `--profile PATH` 產生 cProfile 檔；profile 數據不能與未插樁時直接比較。

| 模式（從整批開始累積至發布） | Control ms | Optimized ms |
| --- | ---: | ---: |
| Attention LayerNorm | 376.83 | 177.74 |
| Multi-Basis 22 | 852.26 | 334.51 |
| Projection L1 GELU | 1,338.35 | 511.61 |
| Multi-Basis | 1,819.79 | 678.64 |
| 100M／全模式完成 | 2,283.20 | 894.74 |
| 全部 rich artifacts 完成 | 2,396.52 | 1,314.13 |

正式配對證據：

- `artifacts/benchmarks/opening_pipeline_2026-09-23_paired_control.json`
- `artifacts/benchmarks/opening_pipeline_2026-09-23_paired_optimized.json`
- Receipt SHA-256: `04aaaed921ab87b7372626e25b75265ac3222042ccfbe689375497c1acf4bd32`

早期 `before/after` 測試還包含驗證雜湊且完成點包含最後一個報表，
保留供追溯，不混入上表。單獨四輪讀取現行五個 ModeSpec，
原解析器 685.52 ms → C safe parser 175.92 ms；這會減少引擎定期設定重載的阻塞，
但不能把差值直接當成下一次實際行情佇列必定節省的時間。

## 測試與部署驗收

- 592 個核心、報價、scheduler、config、checkpoint、simulation、rollover 測試通過；
  另 139 個 Discord 格式／啟動契約測試通過。Node components 9 個通過。
- 兩項既有問題仍公開保留：
  - 全設定掃描遇到 `tw_day_trade_last_last_only_training_vastai1t_cohort_only_candidate.yaml`
    等候選訓練設定的四個未知 training key。舊 Python SafeLoader 同樣失敗，
    未藉改解析器繞過驗證，也未修改不在本次範圍的候選設定。
  - `test_expensive_recovery_jobs_are_timer_only_and_staggered` 仍期待 minute backfill
    14:45；工作樹既有排程已改為其他時點。本次沒有修改該 timer 或放寬測試。
- 12:32 依序重啟 Discord、當沖服務以載入更新。Discord Gateway 已連線，
  22 個 global commands 同步成功；當沖資格預熱完成，Shioaji 2,075/2,075 合約預熱完成。
- 五個模式的 signal ID、processed signal digest/count、entry filled shares 在更新前後相同；
  正常 state revision 繼續前進，沒有重生今日訊號。
- 公開 IPv4 面板與 `/tw-day-trade/api/status` 均為 HTTP 200；
  12:34 的 API 證據為五模式 `active`、Discord `synchronized`、revision lag 0。
  這不代表 IPv6 或所有歷史資料健康問題已解決。
- 中午重啟後 startup model warmup 顯示 pending，是既有 07:00–08:15 時窗規則；
  不假稱新程序已盤前 final-arm。下一交易日仍須通過當日預備與 final-arm 證據。

## 下一次開盤驗收

比較相同五模式、同一交易日的 scheduler wake、mode queue、prepare wait、
broker request queue、首筆／quorum receipt、forward、實際 pointer 發布及引擎接收。
保留開盤價因果時間與行情門檻。尚未實測新的 live 開盤總延遲，
不能宣稱已達「總延遲一秒」或「物理極限」。
