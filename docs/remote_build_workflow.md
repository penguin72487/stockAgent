# 遠端建置：按機器實測後選擇

使用者 2026-10-03 要求所有遠端建置都依該機器實際條件選最快的方法，
除了經驗還要實測。歷史冠軍只作候選起點；每種 workload 各有自己的完整比較。

## 判定與量測

先讀遠端操作契約，核對程序／GPU owner，再觀測 CPU 型號、核心／SMT／NUMA、
affinity、cgroup-v2 可見祖先 CPU quota／memory max-minus-current、RAM headroom、
來源／輸出 mount、磁碟空間及負載／pressure。GPU 工作另核對 GPU／VRAM／
driver／CUDA 與 canonical 嚴格 preflight。CPU quota 是 CPU 時間額度；
有界執行緒超配也可以實測，不因此修改 quota 或占用其他工作的 GPU。

每輪量測使用者等待的完整 wall time，保留來源／程式前後校驗、全部資料、
summary／評估／curve／checkpoint、最終輸出與 parity。記錄 CPU-seconds、
peak RSS、I/O 和負載；不清全機 caches、不停止其他程序、不改 kernel/cgroup。
source SHA 校驗會暖 page cache，數字不能稱為完全冷磁碟效能。

TW source 搜尋依當下額度產生 Polars／Arrow 候選，再測合適的 NUMA physical／
logical affinity，也測每個 CPU socket 及全機一執行緒／實體核心的配置。
例如本次 EPYC 節點為 NPS4，每 NUMA 只有14實體核心，53-thread pool 需比較
同一 socket 的56實體核心或112 logical CPUs，不能假定一 NUMA 就是一 socket。
相同 CPU 集合只測一次。每輪是獨立 process、逐一執行；目前初次探索最多到 affinity
與 2 × ceil(CPU quota) 中較小者，這是有界搜尋範圍，不是全域最優證明。
前兩名以正反順序各再測一輪，每個 finalist 至少三次完整樣本，按完整均值
排序，再用 receipt 真正重建一次。區間重疊時只稱觀測均值最快，保留不確定性。
所有失敗／不相容 candidates 的 logs／輸出均保留。

## 重跑與套用（TW public 完整來源建表）

先以既有 cache owner 確認 selected materialization 的 object／READY 與 lease，
保留原 current consumer。工具呼叫原 `build_tw_public_training_features`，
不 hydration、不移動 latest、不刪除來源或改特徵算法。code receipt／source.zip
使用既有發布與 canonical extractor 交付到新私有根。

在遠端固定 code checkout 下：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_public_remote_derivation.py \
  --code-root FROZEN_CODE_ROOT --code-receipt RELEASE/release.json \
  --packed-root /srv/stockagent-packed \
  --source-root EXACT_MATERIALIZED_SOURCE --snapshot-id EXACT_SNAPSHOT \
  --end-date YYYY-MM-DD --reference PREVIOUS_ACCEPTED_REBUILD.json \
  --memory-budget-gib 64 --output NEW_PRIVATE_BENCHMARK_ROOT
```

`--reference` 是同一完整 workload 已驗收的 canonical 輸出 correctness oracle，
不把舊 timings 充作這次結果。來源／schema 改變時要建立相應的新 oracle。
更新 workload 可用 `--threads 8 16 32` 等上一冠軍與相鄰候選開始，仍實測完整
比較與 finalists。保留候選輸出的空間不足便不啟動，不自動刪舊資料。
timeout／取消只終止自己啟動的 process session。

完成留下全部 receipts／logs、node profile、不可變 `selection-candidate.json`
及經實際套用驗證的 `selection.json`：

```bash
run_fintech_python scripts/verify_tw_public_remote_derivation.py \
  --code-root FROZEN_CODE_ROOT --code-receipt RELEASE/release.json \
  --packed-root /srv/stockagent-packed \
  --source-root EXACT_MATERIALIZED_SOURCE --snapshot-id EXACT_SNAPSHOT \
  --end-date YYYY-MM-DD --build-count 1 \
  --tuning-receipt NEW_PRIVATE_BENCHMARK_ROOT/selection.json \
  --output NEW_PRIVATE_BUILD_ROOT
```

套用前重新核對 node／boot／CPU／NUMA／quota／memory limit／mount、當下 headroom、
exact code receipt／source manifest／runtime／cutoff，以及至少三次 selected
measurements 的 SHA、output、pool、實際 affinity／RAM budget 和完整均值。建完核對來源／程式／runtime／
quota／output SHA。身分變動、證據遺失或 RAM 不足便拒絕，重新量測。

目前自動化的是 TW source 全資料建表；安裝／編譯、其他資料與 GPU 研究需
沿用自己的 canonical benchmarks 與完整驗收，不能套用本表的 threads。
[Kernel cgroup-v2](https://docs.kernel.org/admin-guide/cgroup-v2.html)、
[Polars thread pool](https://docs.pola.rs/api/python/stable/reference/api/polars.thread_pool_size.html)、
[Arrow CPU pool](https://arrow.apache.org/docs/python/generated/pyarrow.set_cpu_count.html)
說明容量與初始化邊界，最快設定仍由完整實測決定。
