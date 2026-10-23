# 工作流第八輪：逐操作計時、FIFO 與預讀批次

接續[第七輪](workflow_speed_optimization_2026-10-22_prefetch.md)，沿用
`workflow-speed-baseline-20261021`。本輪證據位於
`artifacts/benchmarks/workflow_speed_optimization_20261021/round8/`。
前輪證據及維護來源先以 SHA 核對並凍結，沒有改寫歷史樣本。

必要工作仍包括每份輸入的讀取、來源版本檢查、SHA、分類、必要解析、原文保存、
所有輸出及另一程序的讀回。先把這些成本分開，才能判斷可刪除的重複工作與
可重疊的等待；使用語言、執行緒或 CPU 使用率本身不構成加速證據。
完整時間由啟動、讀取／計算的重疊區、輸出與回讀組成；FIFO 減少淘汰查找，
batch 調整改變可重疊的工作量，兩者都不刪減必要運算。把單一操作降到零，也
無法消除其餘階段；未量到可持續頻寬、metadata 與計算下界前，不宣稱達到極限。

新增 `--profile-benchmark-inspection`，預設關閉。它在既有 `Evidence`、JSON
decoder 與欄位樹入口記錄操作次數、失敗次數、monotonic wall 與 thread CPU
nanoseconds。每個執行緒使用自己的 counter，不以跨執行緒 function tracing
歸屬成本。原文、數值、來源識別、錯誤、預算、分類順序與保留規則不變。
例外後會還原原 owner；紀錄不保留 producer 物件。
wall 採用 [perf_counter_ns](https://docs.python.org/3.12/library/time.html#time.perf_counter_ns)，
thread CPU 採用 [thread_time_ns](https://docs.python.org/3.12/library/time.html#time.thread_time_ns)；
兩者的差值定義不同，不把 wall 減 CPU 當成精確 I/O 等待。

`evidence_read_inclusive`、`classification_inclusive`、`read_batch_inclusive`
包含子操作，不能與子項相加；預讀與消費互相重疊，也不能相加成完整 wall。
時鐘與 counter 有成本，診斷資料只用於定位；優化比較全部關閉這個選項。
同步 `read_bytes` 包含開檔／讀取／關檔；預讀的開檔與 bounded read 分列，
關檔及 Python bookkeeping 留在 batch inclusive。SHA 項計 construction/feed，
hex formatting 留在上層。這些差異不隱藏在「純 I/O」標籤內。

沿用 **372,550 份、9,383,953,790 bytes、107 explicit receipts** 的同一份獨立副本。
輸入 manifest SHA 為
`f4bb9b16c7d423b7dbb85bc1f1c61cb5e990596de2c2fe881672572b0959044d`。
每次新程序都完成全部讀取、SHA、必要解析、六份輸出及獨立讀回，沒有改用
較小資料。705,929 個 reported 欄位、2,006 個正規樣本與 722 組摘要須同值。
reader 核對所有輸入識別及 3,739 份獨特保留原文；非 timing payload 的原文
沒有另存並讀回，仍明列這個邊界。

`instrumentation_protocol.json` 事前宣告 off/on/on/off。兩組都使用同一凍結
來源、單一預讀 worker、2 MiB batch。完整 subprocess wall 包括啟動、匯入、
input-plan 載入、原生 cache 初始化、完整處理、輸出與獨立 reader；編譯準備另列。
CPU 為計時區間的 worker 全執行緒，排除前置匯入／input-plan 與 reader 子程序；
RSS 為單一 worker high water。沒有清 OS cache、暫停背景 owner 或宣稱冷讀。

四次完整 off/on/on/off wall 依序為 238.269917、288.185172、397.667836、
305.551825 秒。off 平均 271.910871 秒、CPU 196.556956 秒；on 平均
342.926504 秒、CPU 272.833576 秒。觀測平均 wall 增加 26.1%、CPU 增加 38.8%，
同時有主機波動，不能全歸因於 observer；因此診斷維持預設關閉。六份輸出 SHA
逐一相同，兩份診斷 raw sample 都保留在 `operation_stage_summary.json` 的來源鏈。

| 操作／執行緒 | 每次呼叫數 | 兩次 wall 秒 | 兩次 thread CPU 秒 |
| --- | ---: | ---: | ---: |
| stat／main | 373,571 | 29.094 / 30.130 | 16.260 / 16.843 |
| stat／reader | 743,972 | 29.897 / 75.357 | 16.253 / 32.016 |
| open／reader | 371,529 | 33.110 / 30.279 | 13.439 / 14.540 |
| bounded read／reader | 371,529 | 35.598 / 37.425 | 9.591 / 13.657 |
| SHA／reader | 371,529 | 3.990 / 4.944 | 2.890 / 3.309 |
| SHA／main | 1,021 | 4.753 / 8.264 | 4.746 / 7.438 |
| JSON decode／main | 341,023 | 19.955 / 36.468 | 20.305 / 33.578 |
| metric tree／main | 19,076 | 17.083 / 25.233 | 16.809 / 23.993 |
| gzip／main | 3,739 | 4.347 / 6.623 | 4.264 / 6.319 |
| snapshot write／main | 3,739 | 1.355 / 3.752 | 0.677 / 1.572 |

讀取側的重複 stat 是預讀前後與消費時的來源版本檢查，不能因成本高就省略。
JSON parse 的 341,023 次也顯示總成本不只在 C++ 欄位樹。這些診斷含 observer
成本，不能推算未計時版本的絕對操作占比。凍結的早期 recorder 在 CPU 結束取樣
前含少量 counter 更新，因而部分小操作累計 CPU 可略高於 wall；最終候選改為
先取兩個結束時鐘再更新 counter，原診斷證據不改寫。

另一候選把負面分類 cache 的「尋找第一個 dict key 再刪除」改為
[`OrderedDict.popitem(last=False)`](https://docs.python.org/3.12/library/collections.html#collections.OrderedDict.popitem)。
保留 8,192 筆上限、相同插入／淘汰順序，
hit 不刷新年齡，仍只快取 key metadata 與原分類 provenance，不快取 producer 值。
另比較 2 MiB、8 MiB 與 256 MiB batch；只有原本的單一 worker，pending 及消費中的
raw buffer 上限為 `2 * (batch_bytes + 1)`，同步大檔與整體 RSS 另外處理。

大小盤點發現 38 份大於 32 MiB 的檔案合計 6,598,971,276 bytes，占總 bytes
70.3%。因此在任何候選效能測量前加入 256 MiB 大批次，針對這批主要搬運成本。
每次大批次前後要求可見 cgroup／MemAvailable 保守 headroom 至少 4 GiB。
`candidates_protocol.json` 在計算前宣告 reference、FIFO、FIFO+8MiB、FIFO+256MiB
及反序共八次。
對照為第七輪真正採用的完整來源，包含新增診斷關閉時的軟體成本。要求全部
輸入／六輸出／讀回同值，完整平均 wall 至少少 2%、CPU 不增加、RSS 增加不超過
2%；另接受 wall 在 2% 內、CPU 至少少 10%、RSS 增加不超過 2%。只從合格者
選完整 wall 最低者。256 MiB 額外占用記憶體，因此須完整 wall 至少少 10%、CPU
不增加、RSS 增加不超過 25%，另通過前述 headroom gate。這些門檻在測量前決定，
不依結果修改；CLI 預設仍為 2 MiB。所有慢樣本與負面結果保留。

前兩次相同的 serialization owner 分別耗時 38.49、20.68 秒，顯示背景環境變化
確實影響結果。因此另由 `confirm_candidate.py` 在初選完成後，對唯一初選勝者
執行全新的 reference／selected／selected／reference 確認；沿用原本門檻，
確認失敗就不採用，不轉而挑選未確認的其他候選。初選與確認皆保留全部樣本。

八次初選皆完成完整處理與獨立回讀，輸入識別及六份輸出 SHA 相同。

| 初選候選 | 兩次完整 wall 秒 | 平均 wall 秒 | 平均 worker CPU 秒 | 平均 worker 峰值 bytes |
| --- | ---: | ---: | ---: | ---: |
| 第七輪原版／2 MiB | 500.274280 / 174.090889 | 337.182585 | 220.222077 | 2,171,076,608 |
| FIFO／2 MiB | 417.314009 / 161.549043 | 289.431526 | 194.616574 | 2,181,885,952 |
| FIFO／8 MiB | 210.850881 / 188.518955 | 199.684918 | 168.580196 | 2,193,565,696 |
| FIFO／256 MiB | 199.251186 / 174.199264 | 186.725225 | 166.727354 | 2,185,158,656 |

原版兩次相差近三倍，初選平均差距不能直接視為可靠因果加速。256 MiB 為初選
平均最低者，仍須通過另一份 `confirmation_protocol.json` 的四次 ABBA。
2／8／256 MiB 的同步讀取次數分別為 1,021／426／130，預讀次數為
371,529／372,124／372,420；輸出資料量沒有因此減少。

確認 ABBA 完成，四次完整 wall 依序為 184.979618、192.052514、180.712029、
237.876645 秒，全部六輸出及 372,550 份輸入識別均通過獨立回讀。

| 獨立確認 | 平均完整 wall 秒 | 平均 worker CPU 秒 | 平均 worker 峰值 bytes |
| --- | ---: | ---: | ---: |
| 原版／2 MiB | 211.428132 | 175.646565 | 2,175,512,576 |
| FIFO／256 MiB | 186.382271 | 163.164692 | 2,189,312,000 |

本批完整平均 wall 下降 **11.846%**、CPU 下降 **7.106%**，RSS 增加 **0.634%**，
通過事前 10%／CPU／25% RSS 與每次前後 4 GiB headroom 門檻。各次環境仍有
波動，這是固定輸入的本機確認結果，不推論其他主機、所有服務或 GPU fold
同幅加速，更不是硬體極限證明。

採用來源逐 byte 等同已測試凍結候選，九份維護來源身分記於
`source_disposition.json`。本輪完整 collector 明確使用
`--metric-backend cpp --evidence-read-workers 1 --evidence-read-batch-mib 256
--training-discovery-threads 0`。通用 CLI batch 預設仍為 2 MiB，診斷預設關閉；
新的硬體／來源必須重新量測。FIFO 保留原 cache 上限、淘汰順序及 provenance。

採用後的 168 項 focused semantic tests、語法、必要 lint 與 diff 檢查通過。
隔離 wheel 大小 9,248,243 bytes；unpacked source/native 載入、原生 cache、
`10**70` 同值檢查通過，沒有安裝套件、重啟正式服務或啟動新訓練。

繼承第七輪所有完整 collector 引數，再加入本輪收據與明確 256 MiB batch。
實際完整 command wall 為 **423.462372 秒**，另一程序讀回 **25.948419 秒**；
235,866 筆正規樣本、26,583 組摘要、13 個階段、12 份輸出、6,552 份獨特原文
通過獨立核對，C++ 後端驗證 1，issue／traversal gap 皆 0。
這是目前正式目錄的全流程驗證；與固定副本的量測不同，也不和前輪不同環境的
完整 collector wall 作因果加速比較。歷史 producer timing 仍保留原來源與時鐘，
不是本輪重新執行每個 producer。

| 目前完整 collector 的主要階段 | wall 秒 | collector CPU 秒 |
| --- | ---: | ---: |
| benchmark evidence inspection | 269.602193 | 159.093578 |
| benchmark path selection | 37.246448 | 31.627763 |
| benchmark discovery | 32.114001 | 30.249341 |
| aggregation/output serialization | 28.113993 | 26.624444 |
| code inventory/source capture | 23.261823 | 22.461570 |
| training timing ingestion | 11.838672 | 10.327741 |

13 個 serial phase 的總和以 reader 獨立核對；上表 CPU 不含子程序 CPU，
各 phase 不能和前面的 nested operation clocks 相加。內容檢查仍為最大階段；
下一輪先針對其中版本檢查、JSON parse 與欄位樹的實際成本挑選候選，再以完整
輸出／回讀驗收。C++ 欄位樹已存在，換另一個語言本身不保證剩餘成本消失。

目前 registry 共 2,399 筆，1,361 筆有 stage timing，**1,038 筆仍未綁定**。
其中混有 group、product、credential 與 inventory view，不能解讀成 1,038 個
獨立 API endpoint 或算成來源 E2E 完成率；完整逐來源 E2E 驗收仍為 0。
沿用 architecture snapshot 的兩筆 missing-artifact config error 仍保留。
`remaining_workflow_coverage.json` 綁定當次 inventory／collection／reader SHA，
逐來源後續須補 canonical producer identity、operation grain、完成單位及耗時。

2026-10-22 **15:04:27 UTC** 以註冊 SSH policy 重新做唯讀 admission，仍為
`No route to host`；這不是 GPU 計算測速，沒有啟動遠端訓練。
逐來源完整 E2E、正式服務完整 loaded workflow、遠端完整 GPU fold、硬體下界
仍分別驗收；全局 `all_workflows_measured=false`、`hardware_limit_proven=false`。

本輪原始比較、兩組診斷、來源快照、完整收集及命令收據以
`optimization_acceptance.json` 封存，`optimization_acceptance_readback.json`
由另一程序重算 SHA、平均值、admission 及 operation 次數覆蓋。原始慢樣本全部保留。
完整收集引數在 `final_collection_arguments.json`；重新驗證時指定新的讀回收據路徑：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/profile_workflow_speeds.py \
  --output-dir artifacts/benchmarks/workflow_speed_optimization_20261021/round8/collection_full \
  --verify-only \
  --validation-output artifacts/benchmarks/workflow_speed_optimization_20261021/round8/user_readback.json
```
