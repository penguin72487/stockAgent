# 工作流第五輪：原生欄位配置、callback 與全量驗證

本輪接續[第四輪](workflow_speed_optimization_2026-10-21.md)的剩餘成本剖析，
採用共用結果欄位名稱的 C++ 修改。同批六次完整比較中，平均耗時
**158.736563 → 148.985211 秒，少 6.1%**；本程序 CPU 少 7.5%，
peak RSS 少 6.3%。這是完整 benchmark 證據檢查、輸出及獨立讀回的本機比較，
沒有把它與前輪 22.3% 相乘，也沒有宣稱所有工作流或硬體極限已驗收。

產物根目錄為 `artifacts/benchmarks/workflow_speed_optimization_20261021/round5/`。
沿用同一任務 `workflow-speed-baseline-20261021`。第四輪的 18 次樣本及
341 份證據已於改動前獨立封存核對；其當時 ROOT source hash 是歷史觀測，
對應 immutable source 仍保留於 `round4/canonical_source/`，不冒充目前版本。

## 第一性原理與量測範圍

結果每筆都只有四個相同的欄位名稱。原 C++ `Py_BuildValue` 的 `s` 會為每筆
建立新的 Python 字串，這些名稱與數值本身無關。改為每次樹狀走訪取得四個
interned 名稱，再用 `O` 建立字典，讓所有結果共用名稱，減少配置及保留的物件。
不改 producer 值、順序、單位、pointer、redaction、finite 判定或解析錯誤。

`field_name_allocation_probe.json` 在相同 10,000 筆輸出、相同輸出 SHA 下確認：

| 欄位名稱配置檢查 | 原 C++ | 共用名稱 |
| --- | ---: | ---: |
| 不同的欄位名稱物件 | 40,000 | 4 |
| 這些名稱的 Python heap bytes | 2,120,000 | 212 |

此表只計結果持有的欄位名稱，排除字典、數值、allocator arena 與其他工作，
不拿它替代完整程序 RSS。C API reference 處理由
[CPython Unicode API](https://docs.python.org/3.12/c-api/unicode.html) 的新 reference
契約及既有 RAII 持有；數值繼續保留原 Python 物件。

另一個假說是避開前輪觀測到的 54,352,375 次 Python descriptor wrapper：
實驗版在 C++ 判斷名稱長度，短名稱直接呼叫既有 bounded key cache，長名稱
用原 uncached 函式。這個候選通過語意驗證，但未通過完整時間／CPU 入場門檻，
**未進入維護來源**。維護模組仍保留原四參數介面及既有 callback 行為。

初版只測配置的等待命令，在尚未 dispatch 比較或 build 時取消，exit -15 與
`waiting_comparison_supersession.json` 保留。原因是完成的 residual profile
揭露 callback 熱點，於新 protocol 宣告三變體六次鏡像比較；沒有改寫測到一半的
protocol，也沒有停止正式服務。

## 固定全部輸入的六次鏡像比較

沿用第四輪的私有獨立副本，**372,550 份、9,383,953,790 bytes**，
107 份 explicit receipt；沒有重新挑選較小資料。輸入 manifest SHA 為
`f4bb9b16c7d423b7dbb85bc1f1c61cb5e990596de2c2fe881672572b0959044d`。
每次都讀取、stat、SHA、分類及解析原本需要的全部內容，保留原始證據與完整
CSV/JSONL/manifest，並由另一個程序驗證全部輸入識別與保留原始內容。
705,929 個 reported 欄位、2,006 筆正規 benchmark 樣本及六份輸出 SHA 全部一致。

`metric_callbacks_protocol.json` 在計算前宣告 `A B C C B A`：原 C++、
共用名稱、共用名稱加直接快取。各兩個新程序及輸出目錄，無 cache flush，
背景 owner 保留；每筆前後都保留 capacity、load、pressure 與 boot ID。
`metric_callbacks_semantic_fuzz.json` 在測速前另驗證 10,000 個案例，
包含 Unicode、長名稱、負零、單位、redaction 與原數值物件的 identity。

| 變體 | 兩筆完整 subprocess 秒 | 平均秒 | 本程序 CPU 平均秒 | peak RSS 平均 bytes |
| --- | --- | ---: | ---: | ---: |
| A 原 C++ | 190.234082、127.239043 | 158.736563 | 123.567359 | 2,340,450,304 |
| B 共用欄位名稱 | 168.783536、129.186886 | 148.985211 | 114.296197 | 2,192,885,760 |
| C 共用名稱及直接 key cache | 148.872575、258.123616 | 203.498095 | 141.825816 | 2,192,271,360 |

完整 subprocess 包含匯入、讀取固定清單、檢查、寫出、獨立讀回及最後樣本落盤。
CPU/RSS 為 worker，排除匯入前段及另一個讀回程序的資源；讀回 wall 已計入。
component、workflow 與 subprocess 互有包含，不相加。

事前入場規則要求全部輸入／輸出同值、CPU 平均不增加、RSS 平均至少降 5%，
且完整 subprocess 平均不得退步超過 2%；在合格者中選完整平均最低者。
只有 B 通過。B 的同批完整時間少 6.1431%、CPU 少 7.5029%、RSS 少 6.3050%。
C 同批平均時間多 28.1986%、CPU 多 14.7761%，因此不採用。

C 的反序樣本有明顯波動，`host_contention_during_direct_reverse.json` 另記錄
實際 CPU/I/O/memory pressure 及只含程序名稱／資源計數的觀測；沒有讀出或
記錄 command line、環境內容，也沒有改變任何背景 owner。
每變體只有兩筆，不能從這批證明 C 的演算法本質較慢，也不選其中 148.87 秒
的單筆宣稱改善。原始負結果與樣本全部保留；硬體上限仍未證明。

## 維護版本與驗證

維護修改只有 `stockagent/workflow_metric_tree.cpp` 的欄位名稱共用；
collector 主來源仍為 `7092035357ea6b60a96463e5cd0bae97e04acdaf99f69160a987b7228445c0cb`，
新 C++ SHA 為 `e969beea06f008cba50527dde356b887ee19169f0b573787c5320759107eef73`。
`adopt_round5.py` 在計算前核對維護 C++ 本體與入選 prototype，僅 module 名稱
及起首註解不同，並凍結所有維護來源。沒有把未通過門檻的直接快取或先前預讀
候選藏入正式實作。舊 cache 由 source SHA 自動分隔，沒有刪除。

同一組 **68 項**維護測試、compile/lint/diff 及隔離 source wheel build/import
均再次通過；不是再加 68 種不同測試。包含數值、單位、順序、Unicode、long key、
deep/custom fallback、refcount、建置失敗保留及 frozen native runtime 的拒絕測試。
wheel 內的來源與 canonical byte 相同，解包後以 `python -I` 載入／編譯；
沒有安裝套件到正式 runtime。`--metric-backend auto|python|cpp` 的行為與第四輪相同。

正式模組隨後在同一批 372,550 份固定輸入通過全部六個輸出 SHA 與另一程序的
讀回，見 `canonical_adoption.json`。首次初始化／編譯 1.031814 秒，計入該次
完整 subprocess 156.117568 秒；此單次採用驗證不另外推導速度比。
實際 binary SHA 為 `d14e4621c46c17a88116cb2a031b14eb6c4499fc0e0375a0f2684ed7bc7563a5`。

## 最新完整清冊、分段與讀回

`collect_round5.py` 沿用第四輪完整設定、所有 roots、深度與預算，只換新 output
root 及增加本輪收據。精確 argv 保留於 `final_collection_arguments.json`。
收集 **372,564 份 benchmark 候選、235,760 筆速度樣本、26,507 組摘要**，
另列 706,444 個未經語意審核的 producer 欄位。18,387 份來源 captured，
354,177 份負面分類仍保留識別；evidence 原始預算計入 1,836,922,189 bytes。
擷取問題與遍歷缺口都是 0，既有兩份 config error 仍留在 inventory。

`collection_full_readback.json` 獨立核對 12 份輸出、6,523 份不同原始內容、
18,160 條負面分類重用關係、所有樣本／摘要及 13 個分段。
`native_metric_backend_verified=1`，實際 source、binary、build receipt、
Python runtime 的凍結身分均通過交叉驗證。

| 最新完整收集的序列階段 | wall 秒 | 本程序 CPU 秒 |
| --- | ---: | ---: |
| 程式清冊與量測來源凍結 | 12.557833 | 11.685602 |
| 環境觀測 | 0.183918 | 0.015920 |
| 設定、來源、服務範圍清冊 | 0.129388 | 0.099573 |
| 服務／HTTP／TEJ／儲存既有收據 | 0.132401 | 0.108948 |
| 既有 acquisition 收據 | 3.539376 | 1.956726 |
| 工程命令收據 | 0.567922 | 0.464759 |
| 訓練計時發現 | 4.769567 | 0.102550 |
| 訓練計時擷取 | 8.994631 | 6.927806 |
| benchmark 發現 | 53.652073 | 52.241716 |
| benchmark 路徑篩選 | 21.093840 | 18.709948 |
| benchmark 證據讀取與解析 | 384.162488 | 180.099916 |
| 彙整及輸出 | 24.804163 | 19.981646 |
| 最終輸出雜湊 | 1.263665 | 1.234772 |

序列合計 515.851267 秒，完整命令實際 **518.426755 秒**，另加獨立讀回
13.394367 秒。這次比前輪 live collection 久，原值保留；兩輪即時輸入與
cache／background 狀態不同，不把它拿來估計同條件的因果速度比，也不以
前面固定資料的 6.1% 掩蓋這個觀測。`full_collection_progress_observation.json`
保存自己 engineering cgroup 的五秒進度：collector 持續增加 CPU 與輸出 bytes，
當時累計約 9.64 GB logical read、8.10 GB physical read。這是程序累積計數，
不是某個單獨 phase 的吞吐量；沒有只憑 `active` 認定完成。
機器可讀分段與比較為 `collector_phase_timings.csv`、`optimization_comparisons.csv`。

Registry 為 2,399 個來源，當輪 1,361 個有精確來源 ID 的階段時間；
逐來源完整端到端驗收仍為 0。stage timing、清冊涵蓋及來源完整執行是不同證據。
06:23 UTC 的 enrolled transport 再驗仍為 `No route to host`，沒有遠端完整
GPU fold 的速度；owner namespace 讀回核對 1,603 份 immutable source 及
readonly mount，PID 穩定，`loaded_service_revision_verified=false`。
private JSON 與 completed-cache source 匹配當前 release，dashboard history
工作目錄版本仍未匹配，不能據此宣稱完整 native service workflow 採用。

## 剩餘成本與驗收邊界

維護版再跑完整固定輸入 residual profile，保留同值輸出與獨立讀回。
`inspection_canonical_residual_diagnostic.json` 是診斷資料，instrumentation
時間排除於上述一般樣本及最新速度樣本投影。未通過門檻的直接快取仍保留為
未採用候選，因此既有 Python descriptor callback 成本也保留，沒有宣稱已解決。
此診斷的完整 subprocess 為 283.559306 秒，檢查 251.022221 秒／CPU
187.781577 秒。self time 主要為 read 85.302473 秒、Python descriptor
20.369942 秒、stat 15.813170 秒、native `rows` 15.072808 秒、decode
13.337537 秒、open 9.111422 秒及 SHA 6.344197 秒。descriptor 呼叫仍為
54,352,375 次，累積 31.709173 秒；累積值與子呼叫重疊，不相加，也不把
profiler 插入的 callback 開銷當成正常執行必然可省的時間。
後續需繼續在同一完整流程比較更粗的欄位批次處理／快取與 I/O 方法，
並保留對 mandatory read/hash、來源完整驗證、輸出及獨立讀回的成本。

本輪 `optimization_acceptance.json` 及 `optimization_acceptance_readback.json`
封存並獨立核對所有六筆原始樣本、均值、事前順序、入場規則、來源／binary
及最新完整清冊。前輪資料獨立保存，不混合成新平均。
正式服務重啟／部署及新訓練工作為 0。逐來源完整 E2E、遠端完整雙 GPU fold、
原生服務完整工作流、開盤到 broker fill 與完整 NAS 恢復仍未驗收；
`all_workflows_measured=false`、`hardware_limit_proven=false`。
