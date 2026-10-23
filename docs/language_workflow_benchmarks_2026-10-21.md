# StockAgent 多語言工作流實測：2026-10-21

本輪完成 CPU 工作流的工程比較：Python、NumPy、Numba、C++、Rust、JavaScript，
以及 Python 呼叫 PyArrow／Polars 的完整 Parquet 查詢。最有實測支持的方向是
提前篩選／投影、消除重複運算、整批傳入原生核心；不同工作沒有同一個語言贏家。
這是分析與候選程式交付，正式 collector、價格規則、訓練、帳本及服務沒有切換 backend。

## 第一性原理：哪些成本會受語言影響

固定輸入、輸出與正確性後，把使用者等待時間拆成：

```text
T = 來源等待 + 讀取/解碼 + Python逐筆操作 + 原生運算
    + 配置/複製 + 語言邊界/同步 + 寫入/持久化 + 獨立讀回驗證
T_new = 不受影響的時間 + 熱點時間 / 核心加速倍數 + 新增邊界成本 + 建置成本 / 重用次數
```

需要先問：是否做了不必要的工作？能否用相同順序／數值語意的更好演算法？
是否仍在 Python 逐筆解譯？資料是否已在原生庫或 GPU 裡？
若熱點只佔總時間 5%，把它加速十倍，理想總加速也只有約 1.047 倍。
加入跨程序搬運或每筆 FFI 呼叫後，還可能變慢。

Python 是介面語言，不等於底層運算語言。Numba 可把數值迴圈編譯為機器碼；
Polars 的 Rust 核心可直接從 Python 使用。[Numba 官方效能說明](https://numba.readthedocs.io/en/stable/user/performance-tips.html)、
[Polars 官方說明](https://docs.pola.rs/)。

## 測量範圍與條件

- 節點：penguin，AMD Ryzen 7 5800X3D，WSL2，16 個可見邏輯 CPU。
  Numba／Arrow CPU／Arrow I/O／Polars pool 都設為 1；來源及輸出在 ext4。
  背景服務持續運行，主測前後 load average 約 72／61；不是獨佔或磁碟冷讀測試。
- Python 3.12.15、NumPy 2.4.6、Numba 0.64.0、PyArrow 25.0.0、Polars 1.38.1、
  GCC 15.2.0、Rust 1.90.0、Node 26.10.0（canonical runtime 選定的版本）。
- 價格規則：64 個四位數台股檔案、262,821 個來源收盤參考價／原生日期。
  測量 regular-stock limit 函式，不把計算結果當成交或執行資格。
- 滾動統計：BTC 原始特徵檔的 `okx_mark_log_return_15m`，233,743 × 1，window=32；
  來源時間 2019-12-16 至 2026-08-16。這個固定快照不代表當前資料 freshness。
- 查詢：2,018,074 筆期貨日資料，選 2020 年起 TX 日盤，輸出固定 9 欄、9,730 筆。
  原始 session 保留「一般／盤後」；日盤篩選重用 canonical alias 集合。
- 主測六輪，完整候選順序交替正向／反向；保留每輪 wall time、CPU time、迭代數。
  以下列 wall time 中位數，範圍為實際 min–max，沒有把小差距當統計顯著結論。
- 原始資料先讀成固定快照，來源路徑／SHA／size／mtime 保留在 `sources.json`。
  核心輸出保留 NaN 位置、非 NaN 位元、signed zero；只不要求 NaN payload/sign 相同。
  無 fast-math、FP32/BF16 替換、資料抽減或歷史補值。

## 相同運算的核心與介面成本

單位 ms；包含輸出配置與各自的呼叫介面，排除讀寫檔案與建置／JIT。

| 實作 | 歷史日期價格規則 | 滾動平均／標準差 |
| --- | ---: | ---: |
| 純 Python 參考 | 345.776 | 489.731 |
| NumPy 現有函式 | 20.102 | 9.220 |
| NumPy 重用 prefix 候選 | — | 5.576 |
| Numba | 4.728 | 0.968 |
| C++ bulk C ABI | 1.483 | 1.861 |
| Rust bulk C ABI | 1.883 | 1.592 |
| JavaScript resident kernel | 12.978 | 11.528 |

價格 Numba 為現有 `panel_numba.tw_limit_price`，含日期轉換／wrapper；
滾動 Numba 為本輪候選，沿用 `_rolling_moments` 的 prefix 加總與差分順序。
此處 JavaScript 是已暖機的同程序 TypedArray 迴圈，沒有把程序啟動混進核心時間。
C++／Rust 接受同一組連續 FP64/int64 陣列，不轉成 Python list 或 JSON。

價格規則上，主測 C++ 比目前 Numba 入口約 3.19 倍快；不能全歸因於語言。
追加八輪 dispatch 核對，使用同一份主測快照及原生二進位，得到：

| 相同價格輸入，追加測量 | ms |
| --- | ---: |
| Numba 公開入口 | 8.554 |
| Numba 原始 parallel kernel，1 thread | 3.881 |
| Numba 原始 serial kernel | 4.367 |
| C++ bulk C ABI | 2.452 |
| Rust bulk C ABI | 3.396 |

這組測量顯示 wrapper／日期搬運也有成本；C++ 相對原始 Numba parallel kernel
的中位數差距降為約 1.58 倍，Rust 與其範圍重疊。
不同回合的絕對時間會受背景負載影響，不能混用兩組中位數計算新加速比。

滾動統計的 Numba 候選在主測最快；C++／Rust 沒有勝過它。
而 NumPy 單純避免重算三個 prefix，就由 9.220 降到 5.576 ms。
目前只驗證單欄候選，正式 `[T,R]` 場景還要保留 axis=0、商品隔離與順序後另測。

## 完整讀取、計算、寫入、fsync、獨立讀回

單位 ms，格式為中位數（min–max）；六輪皆保留相同輸出及驗證。

| 實作 | 價格規則工作流 | 滾動統計工作流 |
| --- | ---: | ---: |
| 純 Python | 428.8（354.3–664.0） | 682.3（648.8–793.7） |
| NumPy | 106.3（68.2–178.0） | 147.9（106.8–256.9） |
| NumPy prefix 重用 | — | 137.3（111.6–209.6） |
| Numba | 89.4（77.2–351.4） | 123.3（86.1–196.6） |
| C++ | 72.7（49.3–259.2） | 103.3（91.4–151.5） |
| Rust | 79.0（39.0–163.2） | 141.7（81.2–226.1） |
| JavaScript，含一次新程序／binary 搬運 | 337.5（298.6–472.1） | 378.6（351.5–479.5） |

價格 Numba 每輪 compute stage 中位數只有 3.88 ms，write/fsync 卻約 69.43 ms；
C++ 對應為 1.32／56.82 ms。C++ 工作流中位數少掉的約 17 ms，不能全部歸功於
只省約 2.6 ms 的計算。Numba／C++／Rust 完整時間範圍重疊，未建立穩定的整段贏家。
各 stage 中位數不必相加等於完整時間中位數，原始每輪明細才是加總依據。

JavaScript resident 核心與新程序工作流分開解讀。這組結果拒絕的是這種短任務
每次新開 Node／寫 binary 的接法；沒有測長駐 worker，也不據此宣稱 JavaScript 天生不適合。

## Parquet：先減少工作量，再比較 backend

相同查詢、排序、9 欄輸出、zstd 寫入／fsync、重新讀回與 oracle 比較，全數一致。

| 路徑 | 完整時間中位數 s | min–max s | CPU 中位數 s |
| --- | ---: | ---: | ---: |
| PyArrow：整檔解碼後篩選／投影 | 3.4937 | 1.8004–4.9923 | 2.8815 |
| PyArrow：讀取時篩選／投影 | 0.2833 | 0.2446–0.4259 | 0.2467 |
| Polars Lazy：篩選／投影 | 0.0986 | 0.0715–0.1328 | 0.0632 |

先在相同 C++ backend 提前篩選／投影，約改善 12.33 倍；Polars 對已投影 Arrow
再改善約 2.87 倍。合計相對「整檔先讀」約 35.43 倍。
這同時比較執行計畫、reader 與資料表示，不是 Rust 相對 C++ 的純語言倍數。
目前專案已提供 Arrow／Polars 路徑，應沿用來源責任入口，而非另建資料管線。

## FFI：整批與逐筆是不同工作流

相同 4,096 筆價格、相同 C ABI／輸出配置、精確相同結果：

| 原生核心 | 一次整批 ms | Python 逐筆呼叫 ms |
| --- | ---: | ---: |
| C++ | 0.0637 | 91.1862 |
| Rust | 0.0795 | 94.9445 |

此差距包含逐筆 slicing、ctypes 檢查、函式呼叫與小陣列配置，不能稱為硬體 FFI
指令成本。結論是把工作做成整批數值 buffer；不是換成 Rust 就自動避開 Python 開銷。
[PyO3 官方效能指南](https://pyo3.rs/main/performance.html)也區分型別轉換、Python 物件配置與 interpreter 邊界。

建置另計：C++ 1.400 s、Rust 0.781 s；本程序第一次 Numba 價格／滾動呼叫
約 0.740／0.802 s，保留原本 disk cache，因此不是清空快取後的完整冷 JIT 測試。
Rust 工具鏈下載／安裝未混入穩態數字；它隔離在本次 artifacts 下，未改 shell profile。

## 各工作流的採用方向與尚未驗證部分

| 工作流／目前責任 | 可用方向 | 本輪證據與下一個邊界 |
| --- | --- | --- |
| 資料取得、TEJ／API 等待：既有 downloader | 先處理冗餘請求、排程、quota／UI 等待，再考慮 parser 原生化 | 本輪沒有 provider 端 A/B；不能宣稱換語言能突破來源限速 |
| Parquet 篩選、join、聚合：[columnar_lake](../stockagent/data/columnar_lake.py)、[分鐘聚合](../stockagent/data/tw_stock_futures_minute.py) | Python 呼叫 Arrow C++／Polars Rust，提前篩選／投影 | 已有同輸出完整查詢實測；其他資料布局／join 需另測 |
| 台股分支／價格規則：[panel_numba](../stockagent/data/panel_numba.py)、[歷史價格規則](../stockagent/data/tw_price_rules.py) | 先降低 wrapper／轉換；仍占 critical path 時測整批 C++／Rust | 已驗證兩個歷史時點、上下限、非有限／非法值；未更換正式價格核心 |
| 因果滾動統計：[tw_all_futures](../stockagent/data/tw_all_futures.py) | 重用 prefix、Numba 融合迴圈；保留時間／商品軸 | 單欄 NumPy／Numba／C++／Rust 候選 bit parity 通過；未推定全 panel 加速 |
| recurrent ledger／回測：[inventory_scan](../stockagent/backtest/tw_inventory_scan.py) | 區分 CPU 狀態機與既有 ATen／CUDA 原生 scan；逐一保留帳務／梯度 | 滾動統計不是 FIFO／整數帳本；沒有完整 ledger 或 autograd A/B |
| 神經網路訓練／推論：[trainer](../stockagent/training/trainer.py) | 先量 H2D、kernel launch、fusion、同步，再選 C++／CUDA／Triton | 沒有本輪 GPU／DDP 完整 fold；C++／Rust launcher 不會自行換掉現有 GPU kernels |
| checkpoint／報表：[runtime](../stockagent/training/runtime.py)、[report](../stockagent/backtest/report.py) | 查序列化、配置／複製與 writer；保留必要持久化和全部產物 | 本輪只量 bounded Parquet 輸出；未宣稱 checkpoint 或繪圖加速 |
| Web／長駐服務 | 現有 JS／Python owner 下，量請求、序列化及跨程序粒度 | 只測 Node 短任務 bridge；Go、Julia、C# 沒有同工作量實測，不列速度排名 |

GPU 選型需遵守現有 [效能契約](agents/performance.md)與完整 epoch／fold 驗收。
`torch.compile` 改善的機制包含圖與 kernel 融合；只換呼叫語言並未自動採用這些改變。
[PyTorch 官方端到端範例](https://docs.pytorch.org/tutorials/intermediate/torch_compile_full_example.html)。

本輪優先順序：先在既有資料 owner 使用正確投影／篩選，其次消除 prefix 重算和
Python 小呼叫，再對可觀測的 CPU critical path 採用 bulk C++／Rust。
GPU 訓練、全量 panel、collector 與 live 服務的正式優化仍各自需要完整工作流證據。
本輪不宣稱系統優化全部完成或已達硬體極限。

## 程式、證據與重跑

- 可重用入口：[benchmark_language_workflows.py](../scripts/benchmark_language_workflows.py)。
  候選：[C++](../scripts/benchmarks/language_kernels.cpp)、[Rust](../scripts/benchmarks/language_kernels.rs)、
  [JavaScript](../scripts/benchmarks/language_kernels.mjs)。
- 主測：`artifacts/benchmarks/language_workflows_20261021/run2/result.json`，
  `state=accepted_bounded_language_benchmark`；主測 source copies 在 `run2/source_snapshot/`。
- 追加 dispatch：`artifacts/benchmarks/language_workflows_20261021/dispatch_probe.json`，
  同目錄保留可重跑的 `dispatch_probe.py`。
- 原始 run1 保留為失敗證據：日盤原生標籤與假設不符，被空輸出檢查拒絕；run2
  改用 canonical session aliases 後才通過。未把空資料速度計入結果。
- 針對性測試：8 passed（benchmark verifier、prefix 邊界、既有 panel Numba kernels）；
  另有主測各 backend／上下限／空資料／缺值／window 邊界及每輪讀回檢查。
- Git 只保留 benchmark source、測試與本報告；snapshot、二進位、工具鏈、JSON、log
  在既有忽略目錄，未加入 index，使用者其他 dirty work 保留。

使用已安裝的 GCC／Rust；`--rustc` 可以指定本輪隔離工具鏈中的實際 `bin/rustc`。
每次選新的 output directory，避免覆蓋舊收據：

```bash
source scripts/runtime_env.sh
OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 NUMBA_NUM_THREADS=1 POLARS_MAX_THREADS=1 \
  run_fintech_python scripts/benchmark_language_workflows.py \
  --output-dir artifacts/benchmarks/language_workflows_repeat \
  --stock-files 64 --repeats 6 --threads 1 \
  --cxx /usr/bin/g++ \
  --rustc artifacts/benchmarks/language_workflows_20261021/toolchain/rustup/toolchains/1.90.0-x86_64-unknown-linux-gnu/bin/rustc
```

其他節點／資料要重新觀察容量及實測，不能沿用本機數字作為默認 winner。
