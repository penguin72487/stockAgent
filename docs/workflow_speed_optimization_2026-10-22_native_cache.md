# 工作流第六輪：原生語意快取與目錄掃描

本輪接續[第五輪](workflow_speed_optimization_2026-10-22_shared_keys.md)，
沿用任務 `workflow-speed-baseline-20261021`。證據位於
`artifacts/benchmarks/workflow_speed_optimization_20261021/round6/`。
前輪封存來源保留為歷史版本；本輪先凍結八份現行來源，再實作與比較。

## 第一性原理與範圍

相同欄位名稱的可見性、時間欄位判斷與 pointer escaping，只依賴名稱及固定規則，
不依賴任何 producer 數值。因此可以直接在 C++ 命中既有判定結果，只有未命中
才呼叫原 Python 分類函式。這和第五輪未採用的「只繞過 Python wrapper」不同；
本輪命中時完整避開 Python callback。數值、有限性檢查與原 Python 錯誤仍保留。

快取綁定單一純函式，最多 4,096 個 key、每個最多 256 個 Unicode 字元；
滿額時清空，不宣稱是 LRU。只保留不可變的 `(bool, bool, str)` 語意，
不保留 producer 數值或子樹。重新設定 backend 會建立新快取；Python backend
會清除它。原四參數 `rows` API、deep/custom fallback 及 auto 不編譯的行為保留。
借用 reference 與容器 GC 依
[CPython dictionary API](https://docs.python.org/3.12/c-api/dict.html) 及
[GC 容器契約](https://docs.python.org/3.12/c-api/gcsupport.html) 處理。

第二個候選把 `os.walk` 加額外 Path/lstat 的組合改為單次 scandir 分類；
沿用順序、深度／目錄預算、排除根目錄、dotfile、副檔名與自訂 predicate 語意。
DirEntry 的型別可用於第一次分類，predicate 之後仍保留新鮮的 symlink 檢查，
避免掃描後被改指向的連結。開啟或迭代目錄失敗都保留 coverage gap，
不把未讀成功的目錄當成空目錄。

## 固定輸入與完整比較

原生快取比較沿用全部 **372,550 份、9,383,953,790 bytes** 的獨立副本及
107 份 explicit receipt；manifest SHA 為
`f4bb9b16c7d423b7dbb85bc1f1c61cb5e990596de2c2fe881672572b0959044d`。
每次完整讀取、stat、SHA、分類、必要 JSON 解析、保留原始證據、輸出六份檔案，
再由另一程序讀回全部輸入識別與保留原文。沒有刪減負面分類或驗證。

事前宣告八次 `A B B A B A A B`，每個變體四個新程序，沒有清 OS cache、
中止背景 owner 或平行執行 metadata 測速。編譯準備另列，載入／初始化包含在
完整 subprocess wall 中；worker CPU 不包含另一個 reader 的 CPU。
預先要求全部同值、完整平均耗時至少少 2%、CPU 減少、RSS 增加不超過 2%。
所有樣本都保留，主機壓力／cache 波動不以刪除慢樣本處理。

metadata 比較則使用完整 configured benchmark root、depth 32、directory budget
500,000 及原排除清單，序列為 `A B B A`。包含原 path selection、輸出及獨立
reader；它只測 metadata 流程，不假裝讀過 producer body。即時目錄四次的
路徑、順序及 gap 輸出 SHA 必須全部一致，否則不採用速度結論。
兩批流程不同，比例不可相乘，也不能投影為整個系統或 GPU fold 的改善。

第一批 metadata 在背景工作新增
`migration_v21_native_chunk_pipeline_abba_20261022.json` 時，selected 從 372,455
變成 372,456。四次完整結果保留，但這批輸入不同，不作同條件加速證據。
`metadata_live_drift.json` 列出逐筆新增／移除清單；沒有排除新檔或改寫原 protocol。
只針對這個輸入無效的批次，另外事前宣告反序 `B A A B` 確認；原有效批次
不會因速度不理想而重跑挑選。確認仍使用完整 configured root 與相同排除規則，
四份 metadata 輸出 SHA 相同才可採用。

### 原生快取：八次完整結果

`native_cache_protocol.json` 與 `native_cache_balanced.json` 保存全部條件、
node pressure、來源／binary 身分及原始樣本。所有六份輸出 SHA 與前輪相同。

| 次序 | 變體 | 完整 subprocess 秒 | worker CPU 秒 |
| --- | --- | ---: | ---: |
| 1 | 原 C++ | 255.643787 | 154.283815 |
| 2 | 原生語意快取 | 127.450710 | 106.740277 |
| 3 | 原生語意快取 | 142.042450 | 109.327210 |
| 4 | 原 C++ | 195.691494 | 163.353809 |
| 5 | 原生語意快取 | 201.522293 | 148.774793 |
| 6 | 原 C++ | 212.000591 | 177.863564 |
| 7 | 原 C++ | 185.149690 | 153.738139 |
| 8 | 原生語意快取 | 199.312378 | 159.438890 |

完整平均 **212.121390 → 167.581958 秒，少 20.9971%**；worker CPU
**162.309832 → 131.070293 秒，少 19.2469%**。peak RSS 平均
2,164,803,584 → 2,173,564,928 bytes，增加 **0.4047%**，通過事前門檻。
序列後半四次候選完整 wall 約多 0.93%、CPU 約少 7.05%；第一筆也明顯較慢。
不能分離 page cache 與背景負載的影響，因此不把本批 21.0% 說成穩定保證。
沒有為改善比例剔除任何一筆。

每次候選均有 **53,350,116 hits、1,002,259 misses**，合計維持同一
54,352,375 個名稱判定需求；約 98.16% 直接在原生命中。最後 cache size 2,835，
只含 key 語意。全部 705,929 個 reported 欄位與 2,006 個正規 benchmark 樣本
保留，CPU 降低沒有來自少做輸入、有限性檢查、壓縮、輸出或 readback。

### metadata：輸入一致的反向確認

`metadata_confirmation_protocol.json` 綁定原排除清單、worker/source SHA 及
輸入無效批次的 receipt。確認四次全部 **372,456 discovered / selected**、
gap 0，三份輸出及每筆獨立讀回全部相同。

| 次序 | 變體 | 完整 subprocess 秒 | 掃描秒 | worker CPU 秒 |
| --- | --- | ---: | ---: | ---: |
| 1 | scandir 候選 | 80.202799 | 36.168938 | 51.370464 |
| 2 | 原 os.walk | 128.320681 | 77.804351 | 92.237929 |
| 3 | 原 os.walk | 173.935726 | 101.767811 | 125.949443 |
| 4 | scandir 候選 | 105.877679 | 31.056282 | 57.306156 |

完整平均 **151.128204 → 93.040239 秒，少 38.4362%**；掃描平均
**89.786081 → 33.612610 秒，少 62.5637%**；worker CPU
109.093686 → 54.338310 秒，少 50.1912%。RSS 平均少 0.1466%。
只有這批一致輸入的結果用於採用，第一批輸入不同的 49.1% 平均不作加速結論。
path selection 本體未改，該段的耗時變動不歸因為另一項新演算法改善。

`metadata_operation_probe.json` 在同一個真實 fixture、相同 302 個輸出、0 gap
下，Python 層 `os.stat` 呼叫從 152 降到 1，`os.lstat` 維持 150。
這只計 Python 層 dispatch，排除 DirEntry 內部操作；不冒稱全部 kernel syscall
或磁碟頻寬下界。保留的 150 次 lstat 對應 predicate 後的新鮮 link 檢查。

## 正確性與採用證據

原生候選先通過 20,000 案例差異比較，保留數值物件 identity、順序、單位、
Unicode redaction、長 key 與負零。metadata 另有 104 組實際目錄 fixture
比較，涵蓋深度、預算、predicate、排除根、連結、權限失敗及相對／缺失根目錄。
維護測試另檢查參照生命週期、callback cycle 的 GC、錯誤重試、快取隔離，
以及 predicate 改指向連結和迭代中斷時的行為。

兩項入選修改已進入維護來源；初版 **79 項**聚焦測試、py_compile、lint、
diff 檢查通過。初版隔離 source wheel 為 9,207,866 bytes，來源 asset 與工作目錄
逐 byte 相同；以 `python -I` 解包載入、原生編譯並驗證 cache hit 與原數值物件
identity，沒有安裝套件到正式環境。

初版 collector source SHA 為
`527bf2d3efd241f6d0b998646e6e9249661a9e708e220d986838c68532c29ff6`；
C++ source SHA 為
`2bdf64c2f7ebd3aef2346ffe929d1bec03a5ebabe3090bde096bf8429c5fc411`。
初版 discovery 的 AST body 與入選 prototype 一致，其餘函式與原生比較的
frozen candidate 一致；共用 JSON reader、artifact writer 與 native loader 未改。

最後複查另重現相對根目錄 `.` 的排除回歸：字串 join 留下 `./`，原本的
`Path.absolute()` 會去除這個 prefix，導致 exclude_roots 無法比對。
已修正相對分支並移除不再使用的 getcwd；絕對 root 的 inner loop 保持不變。
修正版新增四組相對／絕對排除路徑測試，最終 **83 項**測試及編譯、lint、
diff 檢查通過；另以 100 組原 fixture 與四組 `.` exclusion 比較對照舊行為，
AST 讀回證明只有此相對分支修正及 unused getcwd 移除。
本輪完整測速均用絕對 root，原始樣本與 protocol 保留；沒有改寫測速來源。

修正版證據位於 `round6/relative_guard/`；collector 最終 SHA 為
`6eb11fba44b8d115cbab87f9234a11b313c4734acb1e5ff2c89a881b23e7a1e1`。
最終隔離 source wheel 9,214,627 bytes，再次驗證三份來源 asset 逐 byte 一致、
`python -I` 原生編譯／cache hit 與原數值物件 identity，環境安裝套件數為 0。
兩次 wheel 的 snapshot 時點不同，不把總大小差額歸因為單一分支修改。

## 最終維護來源的完整 configured collector

`relative_guard/final_collection_arguments.json` 保留完整 argv，延續第五輪的 root、depth、
directory budget、保留上限與既有 architecture／service 輸入，另加入本輪全部
原始比較收據。這是相同設定範圍的目前收集，不代表所有 producer 都重新執行。
collector 完整 subprocess 為 **550.117755 秒**，其後獨立 reader 為
**17.871346 秒**；13 個序列階段合計 547.313748 秒，包含最後輸出 hash，
不包含最終 collection.json 寫入、程序啟動與結束。兩種時計範圍分開保存。
初版 490.633145 秒加 24.394647 秒 reader 的完整收據仍在原 round6 根目錄；
本輪修正後全量驗證較慢，兩次完整觀測都保留，不從中挑選加速結論。

| 階段 | wall 秒 | collector CPU 秒 |
| --- | ---: | ---: |
| code_inventory_and_source_capture | 29.208161 | 25.680123 |
| environment_observation | 0.046976 | 0.030813 |
| scope_inventory | 0.159668 | 0.158149 |
| service_http_tej_storage_receipts | 0.224963 | 0.214182 |
| acquisition_receipts | 9.144245 | 5.068383 |
| engineering_command_receipts | 1.175819 | 0.908801 |
| training_timing_discovery | 21.474830 | 0.148404 |
| training_timing_ingestion | 13.245845 | 12.991845 |
| benchmark_discovery | 35.622945 | 35.220991 |
| benchmark_path_selection | 39.751543 | 39.795751 |
| benchmark_evidence_inspection | 361.275125 | 180.580206 |
| aggregation_and_output_serialization | 34.491594 | 27.457646 |
| final_output_hashes | 1.492034 | 1.491736 |

CPU 使用 collector process time，不包含子程序 CPU；wall／CPU 的落差不能單憑
此表歸因於磁碟或運算極限。主機仍有其他 owner，沒有為測速中止它們。
本批 live 路徑與負載不同於前輪及固定副本；不以 550.117755 秒和前輪總時間
作因果比較。`relative_guard/io_wait_observation.json` 另存本程序與主機的
唯讀 I/O／wait 觀測；單次觀測不建立頻寬下界或完整因果歸因。

本次保留 **235,851 個速度樣本、26,548 組彙整**，涵蓋 1,031 個入口、
2,144 個來源端點清冊、1,138 份 training timing 檔及 372,576 份 benchmark
候選。後者保留 706,719 個尚待 scope review 的 reported 欄位；
18,399 份 captured、354,177 份 not_timing_evidence，沒有把它們都當成獨立測速。
擷取 issue 0、遍歷 gap 0；沿用的兩個 deployment config 仍缺歷史 artifact，
因此保留 configuration_unresolved，沒有為讓 coverage 變綠而改寫設定。
來源端點中 1,361 個只有 recorded stage timing，另外 783 個尚未綁定
source-specific operation timing；完整逐來源 E2E 計數仍為 0。

初版清冊為 2,399 個端點；兩次凍結的 live monitor payload 時點不同，
後者本身少了 255 個 `tej:` id，沒有新增 id。`relative_guard/inventory_drift.json`
保存全部差異、兩版原文 SHA 與 snapshot，核對每版 inventory 都包含該版
payload 全部 id；不推測外部 producer 為何移除它們，也不把數量變動說成加速。

獨立 reader 重算 12 份輸出、6,535 份 unique retained originals、全部速度
樣本與 CSV grouping、13 階段及 18,160 個 classification reuse links，
結果為 verified。354,177 個負面檔案記錄識別，不宣稱其 payload 已全部凍結讀回。
原生 backend 身分驗證為 1，正式收集使用的 module SHA 為
`a0a731ca6e71328e85ab8c7a6f85629a7433d33c398d39a286f3154fde77d19f`；
來源、build receipt 與 binary 身分由 collection/readback 綁定。

## 目前 owner 與遠端界線

2026-10-22 11:31 UTC 再做 enrolled SSH 的唯讀 `uname -sr`，仍得到
`No route to host`，exit 255，傳輸嘗試 0.333562 秒。這是連線失敗耗時，
不是遠端硬體或 fold 速度；沒有啟動遠端 compute。

同時唯讀驗證現任服務 owner，InvocationID 為
`41880ed839024767af2e3dadc7dd39dc`，讀回前後不變。其 namespace 有 1,608
份來源檔及唯讀 mount 通過驗證；private_json、completed_inference_cache 與
tw_day_trade_dashboard 三份候選來源現在都與 release 相同。
這是其他既有 owner 的目前 release，這個任務沒有重啟服務。
receipt 仍明列 `loaded_service_revision_verified=false`，只證明啟動時凍結檔案
與 namespace，不把 import coverage 或完整服務 E2E 採用說成已驗收。

## 剩餘成本與下一個可驗證的改善

`remaining_costs.json`／`remaining_diagnostic.prof` 另以完整固定副本做一次
cProfile，frozen candidate 的 inspection／output 程式碼與採用版本相同；
不執行 metadata discovery，因此相對根修正不影響這個診斷。
全部 372,550 份、9,383,953,790 bytes、六份同值輸出及另一程序讀回通過，
reported 欄位與樣本數保持 705,929／2,006。這次 subprocess 為
539.023487 秒，明列 diagnostic_only，沒有混入八次正常測速平均。

| 診斷函式 | 次數 | self 秒 |
| --- | ---: | ---: |
| BufferedReader.read | 372,672 | 183.528096 |
| posix.stat | 764,193 | 83.106672 |
| msgspec JSON decode | 341,023 | 20.235273 |
| CSV writerows | 3 | 16.234664 |
| C++ metric rows | 19,076 | 11.209665 |
| openssl_sha256 | 374,594 | 8.611836 |
| zlib.compress | 3,739 | 5.321773 |
| Python _metric_descriptor | 1,002,259 | 2.345504 |

self 秒是插樁後的函式時間，包含可能的等待，不是純 CPU；cumulative 層級
不能相加。reader 等待另見 poll 約 20.072581 秒。這些數字定位工作類型，
不直接換算正常執行的比例或可省時間。53,350,116 次原生命中及
1,002,259 次 Python 呼叫相互核對，確認沒有少遍歷欄位。

依序改善的門檻如下；未通過完整同值測速前不採用：

1. **讀取與 metadata**：在既有 Evidence owner 內測 bounded prefetch，
   先以 1／2／4 workers 比較，限制 in-flight bytes 並保持原輸入處理順序。
   保留每份讀取前後的 dev／inode／size／mtime／ctime 與 SHA，故障與容量
   仍 fail closed。測全部輸入、輸出、原文 gzip 與獨立讀回，不以跳過負面檔案
   或 stale metadata cache 換速度；RSS／背景負載也要納入採用條件。
2. **彙整與序列化**：原 CSV writer 已使用 C backend，先拆量 Python 欄位
   轉換與 nested JSON，再評估批次介面；保留 BOM、欄位順序、空值、quote、
   浮點與原檔 SHA，不先預設換語言會更快。
3. **training timing discovery**：其 wall／collector CPU 落差很大，下一步
   應量 canonical rg 子程序的 CPU／I/O 與實際掃描範圍；目前沒有足夠證據把
   這個等待歸因為可被 C++ 消除的 CPU 瓶頸。
4. **遠端與完整 owner 流程**：先恢復既有 enrolled transport 的可讀連線，
   再按選定 config 量完整 epoch／fold、驗證、曲線、plot 與 checkpoint。
   source namespace 相同不能代替 loaded-code 與完整 owner workflow 收據。

第一性原理的硬體下界需要相同檔案顆粒、cache／負載條件下的讀取頻寬、
metadata IOPS 與必要 CPU 工作量。這輪保留完整工作，但尚未建立這些下界，
故 `all_workflows_measured=false`、`hardware_limit_proven=false`。

## 後續分析的紀錄入口

以下路徑皆相對本輪 `round6/`，原始慢樣本與失效批次一併保留：

| 檔案 | 用途 |
| --- | --- |
| native_cache_balanced.json | 八次固定副本的完整 wall、CPU、RSS、各階段及輸出同值證據 |
| metadata_confirmation.json | 四次輸入一致 metadata 測速與採用判定 |
| metadata_abba.json、metadata_live_drift.json | 四次輸入變動的原始失效批次及新增檔差異 |
| selected_round6_samples.json | 16 筆原始樣本的正規入口；失效批次有獨立 cohort_state |
| relative_guard/collection_full/speed_samples.jsonl | 含歷史 producer 收據的完整速度樣本與來源定位 |
| relative_guard/collection_full/speed_summary.csv | 按 workflow／stage 等 scope 彙整的速度紀錄 |
| relative_guard/collection_full/coverage.csv | 已知缺口，供下一次選擇實測範圍 |
| remaining_costs.json | 插樁診斷與未混入正常平均的原始時間 |

封存以 `optimization_acceptance.json` 綁定來源、binary、報告與各個獨立讀回
收據；`optimization_acceptance_readback.json` 為另一程序的封存讀回結果。
讀回會重算 16 筆原始樣本及 40 個平均欄位，其中 12 筆為輸入一致的正常
比較，四筆保留為 input drift；另驗證修正後來源的 83 項測試身分、完整收集、
診斷輸出與兩次 package。只有對應 state 為 verified 的範圍才視為通過。

逐來源完整 E2E、遠端完整 GPU fold、原生服務完整工作流採用與硬體極限，
仍需各自的目前證據；本輪不以清冊、階段時間或 source namespace 代替它們。
