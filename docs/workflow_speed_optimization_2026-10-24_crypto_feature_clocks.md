# 加密資產特徵下載：原生計時與請求排程下界

2026-10-24，延續 `workflow-speed-baseline-20261021`。本次補入既有下載器
留下的原生時鐘，沒有重新呼叫交易所或啟動訓練。先將耗時拆成來源請求、
本機讀取、衍生計算、比較、寫出、覆蓋檢查，再檢驗目前哪些證據足以決定
下一個實驗。這批資料與 R47 固定完整流程的輸入分開保存。

## 已完成的實測與讀回

R53 封存 Binance／OKX 的 `historical_feature_report.csv` 和
`download_summary.historical_features.json`，來源為 2026-10-23 06:00 起的
既有歷史特徵工作。CSV 的原生階段時鐘來自 `perf_counter`；run work 和
lock wait 分開保留，UTC 起訖估算也保留，但不混作同一種時鐘。

| 來源 | 有計時 worker | worker 階段與總計樣本 | 原生 run work | lock wait |
| --- | ---: | ---: | ---: | ---: |
| Binance | 511 | 8,176 | 1,019.588411 秒 | 2.510437 秒 |
| OKX | 442 | 6,188 | 859.858105 秒 | 3.016226 秒 |

加上兩筆 run work，合計 **953 個 worker、14,366 筆樣本**。現行 reader 與
D22 未修改的 original reader 各自完整讀回，六份輸出、14 份原始檔、全部
樣本和獨立 CSV regroup 結果相同；保留 2,399 個已知來源身分。每筆保留
來源 SHA、CSV logical-record 位置、symbol、階段及 outcome。

| 階段 | Binance p50／p95 秒 | OKX p50／p95 秒 |
| --- | ---: | ---: |
| mark price | 0.169／4.659 | 18.844／24.869 |
| index price | 0.176／5.063 | 4.817／18.579 |
| open interest | 9.430／9.619 | 0.510／0.573 |
| 衍生計算 | 0.019／0.031 | 0.018／0.030 |
| 讀取既有資料 | 0.009／0.288 | 0.007／0.158 |
| 寫出 | 0.034／0.100 | 0.025／0.101 |

Binance 的 basis、taker volume、兩種 top-trader ratio 的 p50 約 9.59 秒，
global ratio 為 9.538 秒。上述是來源工作階段 wall latency，包含請求排程、
網路與解析等工作，尚無獨立 wait／network／CPU 分項。多個 worker 會重疊，
其秒數總和不能當作整次工作 wall 或 CPU。輸出 row count 也不能當成本輪
處理量，這批樣本不據此計算 throughput。

本次封存／建立 delta 為 8.282234 秒 wall／2.577005 秒 process CPU；兩個
fresh readers 為 23.873752 秒 wall／6.385938 秒 descendant CPU。完整命令
33.469718 秒 wall／9.514373 秒 descendant CPU。這是量測與讀回的成本，
不是下載加速結果。R53 owner 已 succeeded、實際 inactive、MainPID 0。

## 相同請求量與排程政策下的條件下界

保留的計數顯示 Binance `futures_data` 有 3,166 次 local grants、間隔
0.3 秒；OKX `history_mark_price_candles` 有 6,781 次 grants、間隔 0.1 秒。
在既有 deadline 政策、unit-cost requests、全部 grants 位於該 work 區間、
中途不重設 limiter 的條件下，可以推導 request-start span 的下界。

初始 shared deadline 的相位未知。`_next_deadline` 保留未滿一個 interval
的排程遲到，第一個可見間隔可能較短，因此使用保守式
`max(0, N - 2) × interval`。多個獨立 bucket 可重疊，只取最大下界；同一
family 有 K 個 bucket、總 N 次 grants 時，先用 `ceil(N/K)` 作最忙 bucket
的保守次數，不假裝已知每個 instrument 的分布。

| 來源 | 原生 work | 保守條件下界 | 保留請求量／政策時的理想加速上界 |
| --- | ---: | ---: | ---: |
| Binance | 1,019.588411 秒 | 949.2 秒 | 1.0742 倍 |
| OKX | 859.858105 秒 | 677.9 秒 | 1.2684 倍 |

這是條件推導，尚無實際加速、純等待比例或硬體極限結論；完整歷史 loaded
source identity 與每次 grant timestamp 也未驗證。它說明後續應先量測請求
排程與頁面需求，才能判斷改寫運算核心的收益，不能直接刪除請求或提高
限流率。任何頁面重用／增量化候選仍須保留來源原值、缺漏、重試、覆蓋及
publication 語意，並以相同請求 fixture 和完整工作讀回驗收。

R54 原推導採 `N-1`，漏了未知初始相位；證據保留，由 R56 取代此推導。
R56 用現行 `_next_deadline` 的 120 個確定性案例核對，找到 32 個反例並
驗證保守下界，沒有啟動 limiter threads、等待或資料請求。R55 在建立
source archive 時缺少父目錄而停止，未執行案例；R56 只修復 archive 建立
及改用新的 artifact root，保留原來源、錯誤與收據。

## 原生頁面計數的續查

R62 只讀 R53 已封存的 OKX `index_acquisition_json`，442 個 worker 都有同一
acquisition fingerprint，outcome 均為 updated，未記錄 recent-route fallback。
recent index 共 **5,746 個 logical pages**、client local grants **5,873**；
history index 的 logical pages 和 grants 都為 **873**。

這兩種計數的粒度不同，recent 多出的 127 次 grants 尚未分辨為 transport
retry 或其他 client 呼叫，不能直接叫作可刪除的重複工作。最慢的 mark-price
階段未保留相同的逐 worker logical-page audit，也不能由 index 的結果代推。
後續需先沿用 canonical downloader 的請求／覆蓋規則補足這項歸因；目前沒有
足夠證據刪請求、改頁面大小或提高限流率。

分析為 0.118884 秒 wall／0.032604 秒 process CPU，完整命令 0.739058 秒
wall；owner succeeded、實際 inactive、MainPID 0，沒有重新呼叫來源。

## 尚待補齊的範圍

R53 當時沿用的 R29 固定來源清單有 752 個未綁定 stage clock 的身分，
其中 active 570、inactive 182；包括 family、product、credential 與來源入口。
後續 R87 完整流程已驗證 2,399 IDs／1,651 stage bindings，剩餘 748；
這次 CEX worker clocks 沒有足夠 endpoint 身分證據，沒有據此增加來源綁定。
不同身分粒度和既有缺口全部保留。

CSV 和 summary 分別原子寫出，stage aggregate 相同只能證明本批內容吻合，
不能證明跨檔原子快照或整段歷史載入程式身分。完整 workflow 仍須持續補測。

- [R53 完整雙讀回與來源範圍](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round53/readback.json)
- [各階段原生時鐘與來源 SHA](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round53/native_stage_diagnostics.json)
- [量測與讀回成本](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round53/completion.json)
- [修正初始相位後的下界、120 案例與完整假設](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round56/pacing_readback.json)
- [442 個 worker 的原生 logical pages 與 grants 對照](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round62/page_readback.json)
- [原生時鐘收集程式](../artifacts/benchmarks/workflow_speed_optimization_20261021/round53/collect_crypto_feature_clocks.py)
- [R56 原 owner](../artifacts/operations/agent-workflow/runs/workflow-r56-pacing-phase-readba-20261023T234501-6c474026/run.json)
- [完整收據封裝對照與正式整合範圍](workflow_speed_optimization_2026-10-24_inspection_diagnostic.md)
# 後續：標記價格分頁計數

R62 保留的舊資料缺少 mark-price logical-page audit，無法從 index-price
分頁數回推。現在已在既有 `_fetch_array_history` 呼叫接入 mark audit，
後續 worker 報告新增 `mark_acquisition_json`，使用
`okx_historical_feature_worker_report_v2` 標記報表格式。記錄邏輯分頁嘗試，
不冒充傳輸重試次數；失敗階段也保留已嘗試頁數。資料取得 fingerprint、
features schema、頁面大小、路由與請求內容維持原值。

對應測試已擴充完整八階段 enrichment，比較 audit 開／關的所有 feature
值、缺值與請求序列。R70 在 R69 實際 terminal 後完成四套測試，
**141 passed、0 failure／error／skip**，17.310572 秒 wall／19.124227 秒
descendant CPU；owner succeeded／inactive／MainPID 0。舊／新來源的
acquisition payload、全部 feature 欄位及 schema 1 完全相同，fingerprint
仍為 `7be94f558799c185d3159e492383c2e6fa3f6ec2d42d282c4d8571e76decf8c5`。
完整 fixture 中 mark 有 15 個 logical pages，失敗 fixture 也保留已嘗試頁數。
這是本機報表與請求 fixture 驗證，尚未重驗實際 live worker 的 loaded identity，
也沒有補回歷史缺失頁數或新增來源請求。
[四套測試、舊來源比較與完整計時](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round70/completion.json)
[原驗證 owner](../artifacts/operations/agent-workflow/runs/workflow-r70-mark-page-audit-che-20261024T003734-01b4d807/run.json)

## 正式收集入口：已通過語意與原始列驗證

先前 R53 的 14,366 筆原生時鐘僅由獨立分析腳本讀取。現在已將其配對讀取
接入 `profile_workflow_speeds.py` 的 acquisition 階段，沿用正式 producer 的
`FEATURE_STAGE_IDS` 及 `stage_latency_summary`，不啟動來源請求。報告契約為
`native_crypto_feature_csv_summary_v1`，使用新的 observation root 和 source SHA。

CLI 預設尋找既有 `data_binance/1m`、`data_okx/1m` 的 worker CSV 與專用
feature-run summary。若只剩一個檔案，也記錄配對缺口。可重複提供
`--crypto-feature-report PROVIDER CSV SUMMARY`，此時使用明確選定的範圍。
兩份原始 bytes 都先封存，再核對 provider、grain、原報告路徑、CSV shape、
worker 身分、合法階段、六位小數容差及完整 stage aggregate。錯配或非法輸入
保留來源與 issue，不留下部分樣本。

每筆保留原 report SHA、summary SHA、CSV logical record、instrument、outcome
及原始浮點耗時；空 stage clocks 不會把預設 total 零當作實測。相同報告的
重複 summary 匯出保留兩份來源，只計一次。所有樣本仍沒有 output-row
throughput，也不新增 endpoint binding、loaded-code 或 acquisition completion 證明。

目前 source SHA 為 `80b3304de9e28f5068b078515c8f0443c9d7878ab886ebc3604038908b74cc85`，
新增 26 個語意案例，涵蓋兩個 provider、負零、logical records、缺漏、錯配、
重複匯出及 CLI scope。三份來源的 AST 和 focused diff check 通過；
R90 已另行通過下列檢查，沒有沿用 R87 的驗收替代這次改動。

R90 在 R89 terminal receipt 加實際 inactive／MainPID 0 後執行，於
**04:35:34 UTC succeeded／exit 0**，實查 inactive／MainPID 0。六套共
**290 項測試、零 failure／error／skip**，單獨確認全部 26 個新案例。
另一個程序核對 R53 全 **14,366 JSON rows／953 measured workers**，完整
multiset、負零、原值、context、locator、outcome、source identity 及重複筆數
均一致。兩套 stage summary 也由全部原始 worker clocks 重建通過。

| 子命令 | wall 秒 | descendant CPU 秒 |
| --- | ---: | ---: |
| 六套語意測試 | 27.612147 | 12.943742 |
| 完整原始列獨立讀回 | 13.908912 | 4.932643 |

supervisor 整段 4,694.259482 秒含前序等待，不列為測試或 collector 耗時。
這次不證明完整 13 階段已通過、全域來源覆蓋已重算、服務部署或速度提升。
R89 的 19-file capsule 保留舊版 profile `2219c318…`，這次工作樹修改不影響它。

- [正式 CSV／summary adapter](../scripts/profile_workflow_speeds.py)
- [新增語意案例](../test/test_workflow_crypto_feature_clocks.py)
- [具前序結束檢查的完整驗證程序](../artifacts/benchmarks/workflow_speed_optimization_20261021/round90/check_crypto_clock_integration.py)
- [R90 原 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r90-cex-qualified-after-20261024T031713-13963df0/run.json)
- [R90 測試及原始列驗證結果](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round90/completion.json)
- [R90 14,366 列完整 multiset 與兩套 stage summary](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round90/golden_readback.json)
- [R87 完整驗收與最新覆蓋](workflow_speed_optimization_2026-10-24_native_batch.md#r85正式整合與完整驗收)

## R92 完整 13 階段驗收通過

R92 canonical owner `workflow-r92-full-cex-after-20261024T033455-66f09fa0`
於 03:35:02 UTC 啟動等待；R90 **succeeded／exit 0** 加實際 inactive／
MainPID 0 後，已建立 R92 protocol／admission 並開始正式 collection。
04:57:33 UTC 同一 owner 已 succeeded／exit 0，實查 inactive／MainPID 0。
等待不列作 collector 或測試耗時，沒有啟動另一份重複流程。

程序重用未修改的 R87／R85 完整驗收函數，只更換輸出 root 與已驗證 source
pins，並在原始完整 argv 後明列兩個 native CSV／summary pairs。保留全部
原有 receipts、training roots、三份原生日誌、2,399 個來源 IDs／1,651 個
stage bindings，以及 R45 四檔／226 列、R73 十四檔／6,655 列的完整舊版
parser oracles。CEX 部分另擷取未修改 R53 parser 的原始 AST 函數，使用
untouched original Ledger，對本輪封存的四份 native bytes 重新建立全部
JSON rows，檢查完整 multiset、來源、logical-record locator、outcome、
context、浮點值與重複筆數。舊 reader 的 14 輸出／13 phases 驗證亦保留。

R92 driver SHA 為
`5c8959d2fe8e52ab401108e7c00ff8d9313ee471c6d114532c425ddd4903ef80`。
最終 `completion.json` 狀態為 `verified_complete_canonical_native_crypto_workflow`，
已包含額外 CEX original-reader；沒有以中途的舊 `full_acceptance.json` 代替。
14 份輸出、6,569 份 unique originals、292,006 samples、44,376 groups、
13 個完整階段通過新舊 reader。原三份 journal／21 次 unique 原生操作、
R45 226 列、R73 6,655 列及所有舊來源／綁定均保留；額外 CEX oracle 重新
核對全部 14,366 JSON rows 與原始 parser 完整一致。

| 完整執行區間 | wall 秒 | CPU 秒及範圍 |
| --- | ---: | --- |
| collector child | 598.084971 | 379.103045 descendant |
| canonical reader child | 151.843585 | 37.673484 descendant |
| 實體重建 | 135.545237 | 17.033462 process |
| 未修改原版 reader 與舊 oracles child | 322.738562 | 69.894214 descendant |
| 額外原始 CEX oracle child | 78.210039 | 16.986796 descendant |

含段間控制的 collector＋全部 oracles 為 **1,287.066609 秒 wall**。
完整 supervisor 的 4,950.256759 秒還含前序等待與準備，不能當作 collector
或測試時間。R98 的短時計時測試及 R99 有界遠端 metadata 觀測與本次非受控
正確性驗收有重疊，本次耗時不作 A/B 速度結論。

R92 scope 的全量來源為 **2,399 IDs／1,651 stage-bound／748 unbound**；四項 FinMind
`latest_success_without_consistent_transport_clock` 問題原樣保留。這次沒有加入
後來 R96 的新 journal。後續 R107 已加入該完整 journal 並通過新的完整
13 階段與全部原始 oracles，覆蓋更新為 **2,399／1,653／746**，原 CEX
14,366 列完全保留；[完整範圍與計時](workflow_speed_optimization_2026-10-24_native_tw_public_clocks.md#r105r106r107發布時鐘共用讀取與完整範圍再驗收)。
本次成功不證明完整歷史 acquisition、服務 loaded identity、受控全流程加速
或硬體極限。

- [沿用完整舊驗收與原生 CEX oracle 的程序](../artifacts/benchmarks/workflow_speed_optimization_20261021/round92/run_cex_full_workflow.py)
- [R92 原 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r92-full-cex-after-20261024T033455-66f09fa0/run.json)
- [R92 原完整 argv、保留範圍與新 pairs](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round92/protocol.json)
- [R92 最終完整 13 階段與額外 CEX 驗收](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round92/completion.json)
- [R92 原版全量 reader、舊時鐘及來源集合](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round92/full_acceptance.json)
- [R92 原生 CEX 原始列獨立 oracle](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round92/native_crypto_original_readback.json)
- [R91 小型正式訓練完成 metadata 與未驗收範圍](workflow_speed_optimization_2026-10-24_remote_reuse.md#r91-0329-utc小型完成-metadata-與末端成本)

## R113：七個 ETF 發行商的既有原檔解析測速

從剩餘 11 個明確 registered endpoints 中，先核對七個 issuer 的實際輸入。
七份原檔及其原生 receipt 均存在；R113 使用 ETF／Dune 既有 `.download.lock`
共享鎖，封存 **23 份原始證據／481,643 bytes**，沒有呼叫外部 API。
receipt 的原始檔 SHA、gzip 儲存檔 SHA、觀察時間及完整 source specs 分開保留。

每次執行全部七個原生 parser，從封存檔讀取、SHA、解壓及解壓後 SHA 開始計時，
匯入、前置封存及結果寫出另計。結果擷取包含 dataclass 轉換開銷。
無 profiler 的整段為 **0.870102770 秒 wall／0.624597868 秒 process CPU**。

| 原生 issuer | 完整該檔處理 wall（秒） | process CPU（秒） | 原生輸出列 |
| --- | ---: | ---: | ---: |
| IBIT 2024 tax XLS | 0.184188193 | 0.153631202 | 1,086 |
| IBIT 2025 tax XLS | 0.271616070 | 0.172681313 | 1,095 |
| ETHA 2024 tax XLS | 0.099698579 | 0.080318376 | 573 |
| ETHA 2025 tax XLS | 0.136673470 | 0.121387504 | 1,095 |
| IBIT holdings CSV | 0.036733682 | 0.014411880 | 3 |
| ETHA holdings CSV | 0.016924116 | 0.004122203 | 3 |
| BITB fund page | 0.080245983 | 0.036097177 | 1,399 |

新程序核對 native 與 profile 兩次執行全部 **5,254 列的順序及完整欄位**，
再按照原 producer 的 upsert keys，與原始 normalized Parquet 中相同 source／
raw SHA 的全部列比對：**5,248 metrics、5 holdings、1 reserve**，原表重複
key 亦拒絕。日期、值、空值、來源、單位、available／observed clock 均相符。
來源檔與 receipt 前後全量 SHA／身分核驗通過，未寫入原資料目錄。

完整 native child 為 4.010127 秒 wall／2.092047 秒 descendant CPU；profile
child 為 4.225045／2.643055，新程序讀回 4.213543／1.731216。完整 owner
為 **17.178653 秒 wall／7.376672 秒 descendant CPU**，實查 succeeded／exit 0、
inactive／MainPID 0。這些是本機解析與觀測成本，不是 ETF acquisition E2E。

profile 顯示四次 XLS parser 累計 0.992931 秒，其中 1,293 次逐值日期解析
累計 0.290098 秒、`iterrows` 0.219283 秒、八次開啟工作簿 0.180459 秒。
累計時間有重疊且含 profiler 開銷，不可相加估算可移除成本。
目前安裝的 Pandas 2.3.3 `iterrows` 從 `self.values` 逐列再建立 Series；
R114 因而只比較直接遍歷**同一種 common array**，保留原日期與 float 轉換，
沒有改為另一種逐欄型別規則。Pandas 亦明列
[逐列 Series 的型別邊界](https://pandas.pydata.org/pandas-docs/version/2.3.3/reference/api/pandas.DataFrame.iterrows.html)
及 [values 的 common-type 規則](https://pandas.pydata.org/pandas-docs/version/2.3.3/reference/api/pandas.DataFrame.values.html)；
採用與否仍由實際完整輸出及雙順序比較決定。

維護中的 ETF／Dune 測試加入五個語意案例，涵蓋字串／Timestamp、無效日期、
日期順序、來源時鐘、空值、單位、大整數共同轉型、負零及缺表／短 schema。
原有 12 個 test function AST 完全保留。R114 baseline／candidate 均已
**17 passed、零 failure／error／skip**；完整八輪與採用判定見下節。

Dune 當前 summary 的 67 個結果為 66 個 `not_started_credits`、1 個
`blocked_credits`，三個 registered queries、100 個 registered partitions，
完成 0；這不是正常下載或解析速度。R113 當時尚未逐份重建舊 raw 工作；
後續盤點確認兩個 query 有 34 個完成 receipt 與 2,747,699 列，SQL SHA
均吻合現行設定。R117 的嚴格盤點發現 2,771 個保留原始頁面中，完成 receipt
引用 2,766 個，另五個屬於另一個 execution；兩者均保留，由 R119 接續驗證。
下方 R119／R120 已完成兩個 Dune query 和 SEC 原生 per-entity 的保留資料
重播與完整原始值 oracle；缺乏完成原檔的 Dune query 仍列缺口。
R113 當時的七個解析樣本尚未加入全量 canonical source binding，R107 的
**2,399／1,653／746** 數量保持其原範圍，不提前增加覆蓋。

- [R113 固定完整七個來源與原始證據](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round113/protocol.json)
- [R113 無 profiler 原生細項計時](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round113/native_timing.json)
- [R113 完整函式 profile](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round113/profile_calls.json)
- [R113 原 normalized 全欄位獨立讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round113/readback.json)
- [R113 完成與完整命令耗時](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round113/completion.json)
- [維護中的 ETF／Dune 語意案例](../test/test_crypto_dune_etf_history.py)

## R114：陣列逐列候選有局部收益，完整流程未通過而不採用

在 R113 實際終結後，重用其所有原始證據、七個完整 issuer specs、時鐘與
normalized oracle；只更換 `_parse_ishares_xls` 的逐列包裝及位置存取。
每一輪均包含七個原生 parser、完整結果擷取及**新的獨立程序**讀回，沒有
用重複相同工作多次來放大解析占比。完整命令用同一 monotonic 區間量到兩個
程序終結，不從子階段相加替代。profiler 關閉，前置 golden 準備另計。

全部八輪均與 R113 **5,254 列完整、有順序的輸出**相同，再核對原 normalized
Parquet 的全部匹配列；23 份原檔及封存 SHA／身分前後相符。兩半均須通過
完整命令和原生七檔流程 wall ≤ 0.98、CPU ≤ 1.02 的原門檻。

| 順序半組 | 完整命令平均 wall：原版 → 候選（秒） | 完整 wall 比值 | 完整 CPU 比值 | 七檔原生 wall：原版 → 候選（秒） | 原生 CPU 比值 |
| --- | ---: | ---: | ---: | ---: | ---: |
| ABBA | 6.904913 → 5.731437 | 0.830052 | 0.770579 | 0.567913 → 0.237475 | 0.392858 |
| BAAB | 5.628646 → 6.471670 | 1.149774 | 1.038594 | 0.485226 → 0.317972 | 0.604216 |

局部原生解析 wall 減少 **58.18%／34.47%**，CPU 減少 60.71%／39.58%；
但 BAAB 完整 wall 與 CPU 都未通過，因此 **`candidate_accepted=false`**。
這個結果保留為「局部收益、完整流程不接受」，不取平均抵銷負結果，也不以
不同工作量重跑來追求採用。還未證明完整 ETF acquisition 或穩定完整流程加速。

正式 ETF source 仍為
`b435b4d0e2e2541aa78a67f4aa89d8edc755c28ed9b41ee9affddc58cc8e6bfc`，
未套用候選。維護中的新增語意測試 SHA 為
`a5b12101bcb79700a3d3675923da05b733bc4fb41100f18f2384539872f30f60`。
R114 succeeded／exit 0，實查 inactive／MainPID 0；完整比較 owner 耗時
**62.842285 秒 wall／30.318194 秒 descendant CPU**，沒有 provider 或 GPU 工作。

- [R114 固定全量範圍與原採用門檻](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round114/protocol.json)
- [R114 八輪原始計時及拒絕判定](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round114/comparison.json)
- [R114 原版 17 項語意檢查](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round114/baseline_checks.json)
- [R114 候選 17 項語意檢查](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round114/candidate_checks.json)
- [R114 原 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r114-etf-array-rows-20261024T064941-be561207/run.json)

## R115：既有 issuer 解析時鐘接入共用紀錄

共用 collector 新增明確的 `--crypto-issuer-parser-report TIMING PROTOCOL OUTPUTS READBACK`，
採 `observed_retained_crypto_issuer_parser_v1`；預設不搜尋或執行 parser。
先封存四份報告，再逐份核對 protocol 的 23 份原始證據及程式封存 SHA。
receipt、設定中完整 issuer 順序、解壓前後原檔 SHA、原生觀察時間、所有
normalized keys／欄位／值與讀回列數均須一致，才一次匯入整份 bundle。
缺證據、錯配或超出上限會保留問題與原始證據，不留下部分成功時鐘。

七個 issuer 各保留 wall／process CPU 共 **14 筆**，另保留兩筆整段時鐘，
合計 **16 筆**。來源以完整 issuer ID 綁定；兩筆整段區間不綁個別來源，
也不與內含的子階段相加。奈秒原值與浮點秒數均保留；處理量使用本次
完整 parser 的 5,254 列與解壓 bytes，不用整個歷史表列數代替。
每筆明列沒有新增 HTTP、沒有驗證全段 acquisition 或正式服務載入身分。

R115 **417 passed、零 failure／error／skip**，包括新增 51 項異常時鐘、
完整來源集、缺失與損壞原檔、膨脹 gzip、normalized 值／負零及重複匯出案例。
其餘 366 項原測試保留；新的獨立程序驗證原 CEX **14,366 筆完整紀錄**、
四份台灣公開資料 journal 的 **43 筆完整紀錄**，以及 R113 全部 16 筆時鐘、
5,254 列解析輸出與 23 份原始證據，沒有改寫原始資料或 downloader。

測試命令為 29.800027 秒 wall／19.817352 秒 descendant CPU；CEX 原版讀回
16.733351／6.082422、TW 原版讀回 3.428038／1.465736、issuer 讀回
4.852031／1.494258。完整 owner **61.600373 秒 wall／30.093532 秒 descendant CPU**，
已 succeeded／exit 0、實查 inactive／MainPID 0。這些是整合驗證成本；
尚不構成完整流程加速。後續 R116 已完成原 13 階段與全部既有 oracle，
全域覆蓋驗收見下節；focused 與完整流程證據分別保留。

- [共用匯入實作](../scripts/profile_workflow_speeds.py)
- [維護中的 issuer 完整性測試](../test/test_workflow_crypto_issuer_parser_clocks.py)
- [R115 固定程式與全部測試範圍](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round115/protocol.json)
- [R115 全部原 issuer 輸出與時鐘讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round115/issuer_readback.json)
- [R115 整合完成收據](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round115/issuer_completion.json)
- [R115 原 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r115-issuer-parser-inge-20261024T071250-6a792b3d/run.json)

## R116：完整 13 階段與七個 issuer 的精確綁定

完整保留六個 training roots、239 份明確 benchmark receipts、兩個 CEX 原生
CSV／summary pair、四份原始 TW journal 與全部既有 oracle，另加入 R113 四份
明確的 issuer bundle。共用 reader、封存重建及未修改的原版 reader 均通過：
**14 份輸出、6,598 份原始證據、294,166 筆速度樣本、46,536 個分組及 13 階段**。

相對 R107，**2,399 個來源身分**全部保留，原有 **1,653 個綁定**無遺失，
僅新增七個 issuer 的已驗證離線 parser stage：目前 **1,660 個有階段時鐘，
剩餘 739 個**。這個身分集合包含不同粒度，不能解讀成 739 個尚未測速的
獨立下載器。16 筆新時鐘與 R115 獨立讀回的完整紀錄逐列 multiset 相同，
兩筆 aggregate 未綁來源；來源取得完整性與正式服務載入身分仍未驗證。

原 R45 的四檔／127 records／226 samples、R73 的十四檔／712 records／
6,655 samples、四份 TW journal 的 43 筆完整原生紀錄均通過原 oracle。
CEX 本輪封存內容已自然更新，**16,510 筆**完整紀錄由原生獨立 oracle 核對；
R115 的固定 R53 **14,366 筆**比較另外保留，沒有把這個資料變動當作速度收益。
四筆 FinMind `latest_success_without_consistent_transport_clock` 問題仍保留。

| 完整工作階段 | wall 秒 | CPU 秒與範圍 |
| --- | ---: | --- |
| collector | 486.710608 | 333.409503 descendant CPU |
| canonical readback | 217.059734 | 42.066217 descendant CPU |
| physical reconstruction | 213.960541 | 23.426562 process CPU |
| 未修改原版 reader | 367.396411 | 69.334089 descendant CPU |
| 原生 CEX 獨立 oracle | 60.570199 | 13.523626 descendant CPU |

collector 與全部原始 oracle 區間為 **1,347.234959 秒 wall**；整個受管命令含
準備與最後 issuer 比對為 **1,399.151702 秒 wall／492.767141 秒 descendant CPU**。
owner succeeded／exit 0，實查 inactive／MainPID 0。collector 的最大階段仍是
benchmark evidence inspection，257.007657 秒 wall／171.254522 秒 process CPU；
細項診斷的原子操作區間重疊，不能逐項相加。這次輸入與 R107 不同，
不是受控完整流程 A/B，也未證明硬體極限。

- [R116 全部來源與原始 oracle 的完整驗收](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round116/full_acceptance.json)
- [R116 七個精確 issuer ID、16 筆原時鐘與舊綁定驗收](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round116/issuer_full_acceptance.json)
- [R116 本輪 CEX 全部原生紀錄 oracle](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round116/native_crypto_original_readback.json)
- [R116 完整命令收據](../artifacts/operations/agent-workflow/runs/workflow-r116-full-issuer-parser-20261024T071553-0b3dc2bd/run.json)

R117 在原始頁面集合核對時拒絕，尚未執行 parser：34 個完成分區引用
2,766 頁，另外五頁不能當作完成分區證據。原失敗收據與 2,839 份封存均保留。
接續 R119 重播兩個 Dune query 的 34 個完成分區與 2,747,699 列，另保留五頁；
每份沿用封存必須重新通過原檔 stable FD／全部 bytes 相等核驗。
兩者以既有原檔與 receipt 時鐘重播，不呼叫 provider、不支出 Dune 額度，
不以重播代替實際 HTTP acquisition。

R119 無 profiler 的原生重播已返回 34 個分區／2,766 頁／2,747,699 列，
整段 **274.099765 秒 wall／76.717282 秒 process CPU**；整個 native child
含匯入、全部封存 pins 與前後核對為 **391.233353 秒 wall／89.175325 秒
descendant CPU**。兩份完整重播與 fresh readback 均已 exit 0，owner
`succeeded`、實際 inactive／MainPID 0。所有列順序、schema、null mask、
浮點位元、原生 receipt 欄位及原始頁面解壓 bytes 通過；歷史 migration
metadata 保留，只對明確的 `_dune_contract_id` 欄位位置作具名比較。
2,847 份原檔／125,636,499 bytes 的身分與 SHA 最終重驗相同。
readback 子命令 166.564286 秒 wall／28.938608 秒 descendant CPU。
R117 以此驗收標為 superseded，原失敗收據、log 與封存仍保留。

Profile 中 `atomic_write_bytes` 2,834 calls 累計 160.847562 秒，包含下層
呼叫，不能與它們相加。`mkdir` 2,950 calls 累計 49.509402 秒，
成功 replace 後的 `unlink` 2,868 calls 累計 23.358995 秒。這支持下一個
候選實驗：只在 open 回報缺少父目錄時建立目錄，成功 rename 後不再清理
已不存在的暫存路徑；仍須保留失敗清理、private O_EXCL、原子 publication、
fsync 與全部原資料 oracle，完整對照通過才採用。Profile 包含儀器成本，
上述數字不能直接當作可實現加速，wall 減 CPU 也不是純磁碟等待。

R118 在輸入盤點拒絕，尚未執行 native。原先把 SEC summary 的 33,112
誤當成 entity Parquet rows；逐項核對顯示 **33,072 筆 filings／companyfacts
加 40 個 mapping entity definitions** 才是 33,112。R120 保留這項失敗與
1,458 份封存，fresh stable FD／完整 bytes 重驗後沿用，逐一核對 40 個
entity 與原始 summary，再獨立記錄 mapping40。原始值、manifest、1,256
cached documents 與全部數值位元 oracle 保持。R120 已 succeeded、實際
inactive／MainPID 0，native／profile／fresh readback 均 exit 0；1,461 份
原檔／111,488,627 bytes 的原始身分和 SHA 最終重驗相同。
原生整段 **38.519285 秒 wall／7.493287 秒 process CPU**；整個 native child
含匯入、pins 與快取文件 staging 為 **109.045434 秒 wall／16.362939 秒
descendant CPU**。readback 子命令 124.188433 秒 wall／20.231096 秒
descendant CPU。81 次 adapter calls 是保留資料重播，79 次成功 metadata
requests 與兩個沒有 companyfacts 的原生分支分開，沒有實際 HTTP 404 觀測。
R118 以此驗收標為 superseded，原失敗和封存保留。

R121 只在獨立候選改動 `atomic_write_bytes`／`atomic_write_parquet` 的 metadata
呼叫；成功寫出仍用原子 replace，durable 的檔案與目錄 fsync 保留。
原版與候選各通過 **91 tests／0 failure／error／skip**，包含新增 25 個
publication 失敗、權限與目錄重建案例；資格驗證期間共用來源為原版 `da14a676…`。
八次完整 34 分區、全部原始資料 oracle 的 ABBA／BAAB 對照於 09:17 UTC
完成，兩個順序皆通過預先門檻。八次全部原始列、receipt／raw page 與原檔
身分 SHA 均相符；owner succeeded、實查 inactive／MainPID 0。
每次完整時鐘包括 native 子命令、匯入、pins 及 fresh original reader；
需兩個順序的完整及原生 wall ratio 都 ≤0.98，CPU ratio 都 ≤1.02 才採用。

| 順序 | 完整 baseline／candidate wall 秒 | 完整 wall 減少 | 完整 CPU 減少 | native wall 減少 |
| --- | ---: | ---: | ---: | ---: |
| ABBA | 411.864480／372.000804 | 9.6788% | 4.6242% | 21.6313% |
| BAAB | 572.460090／438.178333 | 23.4570% | 10.9439% | 39.1866% |

原始 trial 值另行獨立重算全部均值與 ratio，結果與 comparison 相同。
第二順序的主機競爭見下一段，不能把 23.46% 當成穩定預期收益；較早順序
的 9.68% 亦僅是此固定 34 分區、完整原始 reader 的觀測。未外推至 HTTP
acquisition、其他來源或硬體上限。後續 R129 資格驗證成功且實際 terminal
後，已精確套用此候選，正式來源 SHA 為 `aeefbe716ceac074872a40b31bcbbc8aafba68f30486a340afd15721a1c06ece`。

R121 第七次完整 baseline 仍通過原始資料 oracle，但完整 wall 為
755.542549 秒、descendant CPU 132.226715 秒，明顯高於前幾次；原值保留。
R126 於 09:11:23 UTC 另存唯讀主機快照，16 CPUs、load average
22.57／29.26／29.65、CPU PSI some avg10 13.23%、I/O PSI full avg10
6.63%，另一個既有 filesystem evidence test owner 同時執行。這是當下競爭
證據，不是過去各輪的完整監控，也不能單獨證明第七次變慢的原因。
最終比較須分開呈現兩個順序及此波動，不能只挑最快試次作加速宣稱。

正式收集器新增明確的 `--retained-dune-report TIMING PROTOCOL READBACK` 與
`--retained-sec-report TIMING PROTOCOL RESULTS READBACK`，預設不發現或啟動
下載器。沿用原 Evidence、Ledger、來源綁定及完整讀回，獨立重驗保留原檔、
原生輸出、每列數值、null／浮點位元、收據、mapping 與全部 raw pages／
cached documents。Dune 的 70 筆和 SEC 的 84 筆 native wall／process CPU
時鐘可各自綁定兩個 query／一個 SEC endpoint，四筆 aggregate 不作來源綁定。
mapping definitions 不算財報列，五個其他 execution 頁面不算完成分區。
新 ABI 為 `observed_retained_dune_partition_v1`／`observed_retained_sec_native_v1`，
仍不是外部 acquisition、live readiness、正式 worker concurrency 或歷史 loaded identity 證明。

原先新增 **86 tests passed**；涵蓋違規 clock／count、混用 row grain、正負零、
不實 readback flags、原始／native bytes 變動、重複匯出、缺少檔案及 gzip
展開預算。R122 的 **503 tests／0 failure／error／skip** 與舊 CEX／TW／issuer
oracle 皆通過，但完整 Dune 匯入在第一個原始分區拒絕，因此 R122 整個
owner 為 failed，不能使用其較早階段的 `completion.json` 代替最終驗收。
原失敗 log、503 測試、原始擷取及舊 source bytes 全保留。

拒絕原因是歷史 migration 的 original `raw_root` 保留舊 range 分區名稱，
而原生 writer 使用 `raw/query_id/partition_id/execution_id`；新 importer
誤把相對目錄要求為相同。修正沿用 canonical writer 的三個精確組件，仍保留
全部 original path／migration 欄位、所有 receipt 非路徑欄位、逐頁解壓 bytes
與完整數值 oracle。沒有改原檔、時間或 native 輸出，也不接受其他 query、
partition、execution 的相同頁面作替代。靜態 contract 拒絕原因現在會保留於
issue，其他例外仍不輸出原始 payload 或任意錯誤文字。

新增四項 migration／跨目錄拒絕案例後，針對性 **90 tests passed**。
完整唯讀診斷已通過 Dune 70／SEC 84 筆，但這個診斷沒有 Evidence 封存，
不能代替正式資格驗證。另一次有快取的診斷出現 `ENOMEM`，原輸出保留；
分開新程序、逐次有界讀取的 SEC 診斷通過，沒有把該失敗當作成功或性能收益。
R129 以新 source `879e8457…` 重跑 **507 tests** 及所有舊／新原始 oracle，
owner 已 succeeded／exit 0，實查 inactive／MainPID 0。507 項全數通過，
完整 Evidence 匯入 Dune 70／SEC 84 筆時鐘，並重驗 Dune 2,747,699 列、
SEC 33,072 財務列及獨立 40 個 mapping definitions；舊 CEX 14,366 列、
TW 43 筆及 issuer 5,254 列／16 筆時鐘均保留。Dune reader 為
215.336454 秒 wall／41.860975 秒 process CPU，SEC 為
149.269965／21.375681 秒；這是完整核對成本，不是下載或計算加速。
最終資格收據 SHA 為 `14b88431570893e76129c97fd100bb9eb5c6ef1a21ce5d4db3e373710f1729db`。
R122 以此後繼驗收標為 superseded，原失敗、log 與 503 項測試保留。

R141 已完成原始資料及完整 13 階段接續驗收；目前全域覆蓋更新為
**2,399 個 registry 身分／1,663 個有階段時鐘／736 個未綁定**。
R116 的 2,399／1,660／739 保留為歷史基準。

R123 於 09:49 UTC 啟動完整 13 階段整合，run 為
`workflow-r123-full-retained-cryp-20261024T094915-2ad6d55a`。啟動前獨立
核對 R129 protocol／release／最終收據 SHA、全部 50 份 source pins，
唯一允許的變化是 R121 已驗收的 atomic publication 來源。保留六個 training
roots、239 份 explicit receipts、兩組 CEX、四份 TW journals、全部舊 oracle
與新增 Dune／SEC 原始輸出讀回，未呼叫 provider 或 GPU。driver SHA 為
`327cd0a8d9db6870124d504a020fd312ee51fd05f9a505796f92c0318112cc97`。
R123 已 failed／exit 1，實查 inactive／MainPID 0；`profile=879e8457…`、
`artifact_io=aeefbe71…` 與全部原產物維持原樣。完整 collector、canonical
reader 與實體重建已通過，原始 reader 在額外來源清單成員檢查失敗。
包裝器要求 `downloader/artifact_io.py`，但 collector 的
`measurement_sources.json` 未列入該檔；原 collection 的 `code_manifest.json`
與開始收集前的 R123 input archive 均保留相同 **6,938 bytes／aeefbe71…**。
這是清單與包裝器的不一致，不能把失敗當作完整整合驗收。

| 已量測範圍 | wall 秒 | CPU 秒 | 結果 |
| --- | ---: | ---: | --- |
| 完整 13 階段 collector 子命令 | 1,032.227271 | 413.209448 descendant | 通過 |
| 其中 acquisition receipts／完整本地原始核對 | 479.498013 | 78.781252 process | 通過，沒有新 provider 呼叫 |
| 其中 benchmark inspection | 249.146044 | 170.279334 process | 通過 |
| 其中 serialization | 118.396183 | 28.839072 process | 通過 |
| canonical reader | 231.325050 | 41.571824 descendant | 通過 |
| 原始 reader | 677.245482 | 105.153623 descendant | 清單成員斷言失敗 |

reader 已驗證 **14 outputs／10,997 unique originals／294,313 samples／
46,683 groups／13 phases**；實體重建保留 10,997 gzip originals，輸出
1,259,976,266 bytes。上述 collector 子階段不是額外可相加的完整工作。
原 owner 的 launcher 單調時鐘為 **2,401.884583 秒**，包括重建及失敗 reader；
UTC 起訖差另為 2,401.951661 秒。R137／R141 舊收據中的
`measured_failed_owner_wall_seconds`／`reused_failed_owner_wall_seconds`
實際沿用後者，應讀作 UTC span，並非 launcher 的單調時鐘；原收據不改寫。
重建的獨立 monotonic 子時鐘尚未寫成收據便中斷，明確保留這項缺口，
不由檔案時間推算或偽造完整成功工作流的加速比例。

R137 新 owner `workflow-r137-retained-source-re-20261024T104052-5e793047`
原已接續原始 reader 及原 CEX／TW／training／issuer／Dune／SEC oracle。
只新增一份 IO source 的獨立證明：重讀原 R123 archive，核對其原 source pin、
原 collection code manifest 與當前精確 SHA，補足原成員檢查及逐來源 bytes
驗證；保留原 collection 的 **45 份來源清單與全部 14 份輸出**。
原 reader、全部既有 assertion 與原始資料檢查均保留，不改寫舊清單、
產物或失敗收據。新 owner 的獨立成本另記；R137 本身未完成驗收，
後續 R141 的完整驗收才更新覆蓋。

14:24 UTC 重新查核時，實際 boot 已從 `5a688359…` 換成 `cf9d28cb…`；
R137／R138／R139 原 PID／start-time／boot identity 均不再存在，systemd
unit 為 not-found／inactive／PID 0。已用 canonical `run-reconcile` 留下
**interrupted／recovery_verified**，沒有把舊 running 檔案當作活程序或成功。
R137 尚無原始 reader 完成收據，R138／R139 尚未建立 D benchmark protocol。
保留中斷時的空 log 與全部原產物，不覆寫重跑。

R140 在第一個專案 import 即 failed，**尚未寫入 D**；缺少 import root 的
修正由新 R141 bootstrap 提供，R140 source／失敗原樣保留。
R141 `workflow-r141-boot-source-reconc-20261024T143426-f18d545e` 已由同一
systemd owner 重新登錄當前 D mount：**守門器 bytes／SHA 仍為 `5010eb4b…`**，
保留正式 volume marker、8 KiB 9p、PID 1 namespace、持有 FD、mount tuple、
device 與 boot 的全部檢查；新 device 為 161，原 enrollment／device 149
與 boot guard 未修改。只在新的 `round141/location.json` 記錄當前身分。

R141 已以原 restore helper 將同一個 D packed collection 完整重建到
自己的 ext4 `physical_original_view`，再通過原始 reader 及全部原 oracle。
owner 已 succeeded／exit 0，獨立實查 inactive／MainPID 0。
事先保留 root 8 GiB + 重建預估 4 GiB、D 32 GiB 與 RAM
24 GiB headroom。D collection／舊 D 實體 view 仍保留；ext4 staging 是
精確原格式驗證的工作位置，不能當作 cold recovery 或全服務開機恢復證明。
物理位置、boot 與負載和舊 D reader 不同，沒有受控 reader／全工作流加速結論。

| R141 接續的完整範圍 | wall 秒 | CPU 秒 | 結果 |
| --- | ---: | ---: | --- |
| 重建 10,997 個原格式 gzip／1,259,976,266 bytes | 736.638751 | 51.525731 process | 完整重建 |
| 原始 reader 及既有 assertion | 123.499306 | 66.908165 descendant | 通過 |
| CEX 原始 reader | 24.675319 | 12.481686 descendant | 16,510 列 multiset 相同 |
| driver 整個接續流程 | 1,185.813844 | 個別階段如上 | 全部 oracle 通過 |
| launcher child-launch-through-wait | 1,188.450789 | 177.400947 descendant | 成功終結 |

UTC 起訖差另為 1,168.300878 秒，與 launcher／driver 單調時鐘不同；
沒有證據判定差異成因，效能比較使用相應範圍的單調時鐘。
完整驗收保留 **14 outputs／10,997 originals／294,313 samples／46,683 groups／
13 phases**、舊來源身分及綁定、TW 43 筆操作、issuer 16／Dune 70／SEC 84
筆完全相同的時鐘 multiset，以及全部原始列、數值位元、receipt 和額外五頁。
新增綁定恰為 `crypto-etf:sec-edgar`、
`dune-query:dune_dex_asset_activity_daily_v1`、
`dune-query:dune_stablecoin_issuance_daily_v1`；沒有 provider acquisition 推論。
原清單仍為 45 份，IO 的第 46 份是精確 archive 的獨立補充。
驗收 SHA 為 `41ac118541191334c7fb0e90b374ef9e4f03ce2fa818315276b916bff81016d1`。
R123／R140 的 failed run 以 R141 驗收標為 superseded，原失敗保留；
R137 仍為 interrupted／recovery_verified，沒有改造為 exit 0。

R142 已在 R141 實際終結後封存 28 份來源，完成兩組 smoke，開始
372,550 固定 inputs／107 explicit 的完整 ABBA／BAAB；完整輸出、原始 bytes、
獨立 reader 與採用門檻保留，目前尚無快取採用結果。
R143 原始核對診斷繼續等待 R142 **實際成功**後執行。

R146 在 15:39:58 UTC 以新的獨立程序重新核對目前全部 **736 個未綁定身分**。
使用已驗收 R123 inventory `3e393715…`、R141 acceptance `41ac1185…`，
逐項對照 R94 `7be1eb20…` 的明確 scope；後者是 **03:53:12 UTC 的歷史定義**，
沒有當成當前啟用狀態或權限。完整 736 項清單及獨立讀回已保存：

| 明確登錄粒度 | 尚未綁定身分 |
| --- | ---: |
| source_registry | 240 |
| logical_source | 258 |
| registered_endpoint | 1 |
| crypto_fact_family | 35 |
| physical_inventory | 55 |
| inventory_partition | 4 |
| storage_group | 50 |
| product_granularity | 48 |
| credential_gate | 45 |

R123 凍結清單的 active scope 是 554 項／其餘 182 項；這也不是當前健康或
API 權限判定。不能把 736 項稱為 736 次獨立下載，亦不能刪去 group／gate
使分母變小。這次 **沒有新增操作時鐘或改變 2,399／1,663／736 覆蓋**。

唯一 registered endpoint 是 `dune-query:dune_cex_labeled_flows_daily_v1`。
R146 以原穩定 FD／no-follow／有界 helper 封存 21,208 bytes 原始 Dune
summary，SHA `885d9128a09885b65315baaedb1f124a5f0bc47b5baa3253c6ea8371ed13eb2c`。
該原收據的產生時間是 **2026-10-23 22:30:14 UTC**，其 due scope 中 CEX
28 個分區全為 `not_started_credits`、本次 rows 0；15:39 的本機查核也確認
該 query 的 raw／receipts／normalized 目錄均不存在，三個父目錄存在。
這證明目前沒有該 query 的本機保留資料可重播，不證明當前帳戶額度，也不
將父層 blocked 時間分配成分區時鐘；未重試 API 或補造資料。

R146 owner 已 succeeded／exit 0、實查 inactive／MainPID 0；分析及獨立讀回
1.177797 秒 wall，launcher 完整 child 1.288427 秒 wall／0.495559 秒 descendant
CPU。這些是本機盤點成本，不是 CEX 來源操作時間。其餘 45 項 credential
gate 已由 R145 排定正式稽核程式的本機完整診斷，仍等待既有測速程序終結。
R149 的既有 collector 匯入草稿已通過 62 項合成語意檢查，R150 另以新程序
原版 reader 核對四份合成完整收集；實際 R145 時鐘尚未匯入，因此覆蓋不變。
來源包裝程式、exact binding 與資格邊界見
[本機 credential 時鐘匯入驗證](workflow_speed_optimization_2026-10-24_inspection_diagnostic.md)。

- [R119 完成分區與額外五頁的明確範圍](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round119/protocol.json)
- [R119 無 profiler 的完整原生分區時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round119/native_timing.json)
- [R119 native 整個子命令時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round119/native.command.json)
- [R119 全部原始列、數值位元、receipt 與頁面讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round119/readback.json)
- [R119 profile 原始資料，累計區間重疊](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round119/profile_calls.json)
- [R119 最終來源身分與驗收收據](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round119/completion.json)
- [R118 原盤點失敗收據](../artifacts/operations/agent-workflow/runs/workflow-r118-retained-sec-nativ-20261024T074821-8914046d/run.json)
- [R120 mapping 與 entity 計數修正 wrapper](../artifacts/benchmarks/workflow_speed_optimization_20261021/round120/reconcile_sec_mapping_and_entity_rows.py)
- [R120 全部財報資料、manifest、mapping 與快取文件讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round120/readback.json)
- [R120 原生 mapping 與 40 個 entity 的個別時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round120/native_timing.json)
- [R120 原始身分與完整驗收收據](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round120/completion.json)
- [R121 原版／候選的測試、完整對照範圍與門檻](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round121/protocol.json)
- [R121 八次完整原始資料對照與兩個順序的均值](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round121/comparison.json)
- [新的 Dune 匯入語意測試](../test/test_workflow_retained_dune_clocks.py)
- [新的 SEC 匯入語意測試](../test/test_workflow_retained_sec_clocks.py)
- [R122 完整資格驗證 wrapper](../artifacts/benchmarks/workflow_speed_optimization_20261021/round122/qualify_retained_crypto_replays.py)
- [R122 完整 Dune 拒絕的原始 log](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round122/retained_native_readers.log)
- [R129 保留失敗並重新資格驗證的 wrapper](../artifacts/benchmarks/workflow_speed_optimization_20261021/round129/qualify_migrated_retained_clocks.py)
- [R129 全部 507 項與原始資料資格驗收](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round129/retained_ingestion_completion.json)
- [R123 完整整合 driver](../artifacts/benchmarks/workflow_speed_optimization_20261021/round123/run_full_retained_crypto_workflow.py)
- [R123 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r123-full-retained-cryp-20261024T094915-2ad6d55a/run.json)
- [R123 原始失敗與完整 reader 耗時](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round123/untouched_original_reader_command.json)
- [R137 精確補充來源與原 oracle 接續](../artifacts/benchmarks/workflow_speed_optimization_20261021/round137/reconcile_retained_source_proof.py)
- [R137 原產物與失敗 owner 身分封存](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round137/protocol.json)
- [R137 補充 IO 來源的獨立證明](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round137/supplementary_source_proof.json)
- [原三個 owner 的新 boot／程序缺失證明](../artifacts/benchmarks/workflow_speed_optimization_20261021/round140/boot_observation.json)
- [R140 原開機接續 source 與完整復原方式](../artifacts/benchmarks/workflow_speed_optimization_20261021/round140/recover_after_boot.py)
- [R140 import 前置失敗收據](../artifacts/operations/agent-workflow/runs/workflow-r140-boot-source-reconc-20261024T143209-09df6825/run.json)
- [R141 精確 bootstrap 接續](../artifacts/benchmarks/workflow_speed_optimization_20261021/round141/recover_with_project_import.py)
- [R141 保留所有原檢查的新 boot 登錄](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round141/enrollment.json)
- [R141 當前 mount／device／namespace 綁定](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round141/location.json)
- [R141 原始驗證接續 owner](../artifacts/operations/agent-workflow/runs/workflow-r141-boot-source-reconc-20261024T143426-f18d545e/run.json)
- [R141 全部原始 reader 與既有 oracle](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round141/untouched_original_readback.json)
- [R141 完整 13 階段與新增三個綁定驗收](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round141/retained_crypto_full_acceptance.json)
- [R141 重建、readback 與整個接續成本](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round141/reconciliation_completion.json)
- [R146 736 個剩餘身分與精確登錄粒度](../artifacts/benchmarks/workflow_speed_optimization_20261021/round146/analysis.json)
- [R146 原收據、穩定擷取及三個本機目錄缺失證據](../artifacts/benchmarks/workflow_speed_optimization_20261021/round146/protocol.json)
- [R146 完整獨立讀回](../artifacts/benchmarks/workflow_speed_optimization_20261021/round146/readback.json)
- [R146 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r146-unbound-grain-read-20261024T153954-06852c10/run.json)
- [R126 當下競爭快照與範圍限制](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round126/observation.json)
- [R126 原始 systemd 狀態與啟動時間](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round126/systemd_conditions.txt)
- [R126 獨立 bytes 讀回完成](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round126/completion.json)
