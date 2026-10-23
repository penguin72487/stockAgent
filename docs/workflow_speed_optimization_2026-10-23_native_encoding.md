# 2026-10-23：有型別與整數邊界的原生 metadata 編碼

延續 [Round 10 的未採用候選及常駐物件診斷](workflow_speed_optimization_2026-10-23_manifest_encoding.md)。
本輪已實作另一個候選，212 項語義測試與八次階段測速通過；原八次完整流程比對未通過原延遲門檻。
另行預登記的四次完整 CPU 成本確認、package、目前完整 collector 與獨立回讀均通過，已保留候選。
證據根目錄為 `artifacts/benchmarks/workflow_speed_optimization_20261021/round11`。

## 從觀測成本選擇修改

上一輪常駐物件診斷量到：資格檢查約 1.00 s、stdlib compact 編碼及 UTF-8 約 2.01 s、
原生排版約 0.37 s。繼續優化排版的收益有限，因此本輪讓已確認型別的 metadata 直接用
`msgspec.json.encode` 產生 UTF-8，再用原生 formatter 排版。大範圍的讀取、SHA、解析、
walk、CSV 與獨立 reader 都保留；本輪沒有把局部百分比套到整個工作流。

`stockagent/private_json.py::pretty_metadata_bytes` 的原生分支只接受 exact builtin
str／bool／null／dict／list，以及 `[-2**63, 2**64-1]` 的 exact int，深度有界。
這個範圍是本輪 eligibility 政策，不是宣稱目前套件無法編碼更大的整數。
大整數、浮點數、自訂容器或 key、深樹與 legacy 情況仍用原本 stdlib pretty encoder，
保留值、特殊值、錯誤及整數 digit-limit 行為。既有 loads／dumps／projection 函式 AST 不變。

只有 `scripts/profile_workflow_speeds.py::write_evidence_manifest` 接入這個 guarded helper。
`atomic_write_bytes(..., durable=True)` 仍由原 owner 執行；輸出格式、換行、fsync、atomic replace
與 parent sync 不變。C++ walker、source reads、資料特徵、交易／loss、模型及 optimizer 都未改。

## 語義與階段實測

三個受影響測試模組 **212 passed**，含 300 組固定亂數的巢狀／Unicode wire oracle。
額外明確驗證 signed64／unsigned64 邊界、超界整數與浮點數不進原生編碼，以及 stdlib legacy
fallback、錯誤、控制字元、深樹與耐久寫入 owner。native package 為本機已安裝的 msgspec 0.22.0；
沒有安裝新套件，也沒有把一般金融 JSON 編碼切換成 native encoder。

同一份封存的 202,684,792-byte manifest，完整 read／SHA／parse／durable write／獨立 reader，
按 A/B/B/A、B/A/A/B 執行。每次輸出都與原檔 SHA
`e204263d1bf4da025ede9dc8eb0492911b454c73d571e491864999dfd90ac4bc` 一致。

| 確認組 | 原版 write wall s | 候選 write wall s | 原版 write CPU s | 候選 write CPU s | 原版 RSS bytes | 候選 RSS bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A/B/B/A | 4.951686 | 2.127460 | 4.706717 | 2.059825 | 1084438528 | 768677888 |
| B/A/A/B | 7.038208 | 3.229056 | 6.605175 | 2.723035 | 1084049408 | 768823296 |

兩組皆通過預先固定的 gate：mean write wall／CPU 至少降低 20%、mean worker lifetime peak
RSS ≤102%、全部 bytes 與獨立 reader 相符。CPU 不含 child reader；RSS 非父子同時總峰值。
沒有清 OS cache、隔離其他主機工作或宣稱冷碟測量。階段樣本保留於 `encoding_comparison.json`。

## 原八次完整流程門檻與結果

`comparison_protocol.json` 在執行前固定八次 A/B/B/A、B/A/A/B。每次使用固定的 372,550 份
檔案、9,383,953,790 bytes、107 explicit receipts，保留完整讀取／SHA／分類／解析／walk、
六份輸出及獨立 input identity／原文回讀。read worker=1、batch=256 MiB、profiling 關閉，
前後 memory headroom 皆須至少 4 GiB。

兩組都須通過六份輸出相同與 reader，再同時滿足 mean worker RSS ≤102%、完整 outer wall ≤102%、
worker CPU ≤100%、序列化 wall 與 CPU ≤90% 原版。沒有採最快單次，也不沿用前輪被拒絕的結果
宣告採用。此 gate 容許總時間近乎持平的局部加速；只有實測支持時才報完整流程加速。

第一組的序列化 mean wall `24.409810 -> 24.414451 s`，CPU `22.241878 -> 19.560160 s`。
CPU 下降約 12%，原先要求的 aggregate serialization wall 降低 10% 未通過。反向組的完整 outer wall、
序列化 wall 與 CPU 門檻也未通過。因此原八次 `candidate_admitted=false`，結果完整保留。

| 次序 | 版本 | 完整 outer wall s | worker CPU s | 序列化 wall s | 序列化 CPU s | worker peak RSS bytes |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 原版 | 224.833282 | 177.238262 | 25.465781 | 23.886856 | 2192216064 |
| 2 | 候選 | 181.543767 | 158.777359 | 21.545552 | 18.497342 | 1872424960 |
| 3 | 候選 | 183.725914 | 155.006396 | 27.283349 | 20.622979 | 1882546176 |
| 4 | 原版 | 186.670241 | 161.055900 | 23.353838 | 20.596899 | 2195439616 |
| 5 | 候選 | 217.555205 | 164.073973 | 15.224420 | 14.537245 | 1881964544 |
| 6 | 原版 | 191.257548 | 159.484008 | 17.711214 | 16.765415 | 2194243584 |
| 7 | 原版 | 172.994378 | 150.103010 | 17.739486 | 15.954420 | 2191224832 |
| 8 | 候選 | 160.511353 | 142.548842 | 18.018988 | 16.366515 | 1879228416 |

這組 worker CPU 是 timed worker body 的所有執行緒成本，不含 imports／input-plan setup／child reader；
不能與下面完整程序樹 CPU 當作同一口徑。完整 outer wall 包含這些工作。原版耗時隨次序有明顯漂移，
不能挑最後候選與第一個原版宣稱加速。六份輸出每次均逐 byte 等價；每次獨立 reader 驗證 372,550 份
input identities 與 3,739 份留存原文。354,177 份 non-timing bodies 僅有 identity／classification 證據，
不擴大宣稱為全部原文已封存或冷還原。

## 另行登記的完整 CPU 成本確認

延遲與 CPU 成本是不同觀測量。第一組 CPU 已下降，aggregate wall 近乎持平，不能由這個差值
直接歸因為 I/O wait。為直接驗證計算資源效率，在原八次尚未完成、四個全新樣本尚未開始時，
另於 `2026-10-22T17:52:19.108090+00:00` 寫入 `cpu_efficiency_protocol.json`，預先固定新的 A/B/B/A 與門檻。
原八次作為新 CPU 假說的探索資料，保留原門檻拒絕結果；不把它們算進新四次的採用計算。

新四次仍完整讀取相同的 372,550 份檔案，保留全部工作、六份輸出與獨立原文／輸入回讀。
改量 **包含啟動、input-plan setup、cached backend init、worker、輸出、reader 與結束的 CPU**：
sole worker subprocess 前後的 `RUSAGE_CHILDREN` user+system delta，加上 controller spawn/wait
區間的 `process_time` delta。Linux 在中間各程序都等待子程序時，children accounting 包含已等待的
後代；RSS 仍使用 worker lifetime peak，不誤稱為父子同時總峰值。
定義依據為 [Linux getrusage 手冊](https://man7.org/linux/man-pages/man2/getrusage.2.html)。

新 gate：四次全新樣本 mean 完整 CPU 成本至少減少 2%、完整 outer wall 不增加超過 2%、
worker RSS 不增加超過 2%，並保持全部 output／reader equality 及前面已通過的 20% stage gate。
這是 CPU 效率確認，不以 CPU-only acceptance 宣稱延遲有同等收益。
四次結果全部 output／reader equality 通過，三個 mean gates 也通過；正式決策引用
`cpu_efficiency_comparison.json`，原 `balanced_comparison.json` 的八次 wall-gate 拒絕結果仍完整保留。

| 次序 | 版本 | 完整 outer wall s | 完整程序樹 CPU s | worker CPU s | worker peak RSS bytes |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | 原版 | 173.723747 | 172.849080 | 154.206430 | 2193014784 |
| 2 | 候選 | 163.413658 | 160.213559 | 143.081958 | 1882390528 |
| 3 | 候選 | 160.825889 | 160.942273 | 144.260274 | 1881772032 |
| 4 | 原版 | 174.293418 | 174.069590 | 151.031461 | 2186113024 |
| mean | 原版 | 174.008582 | 173.459335 | 152.618945 | 2189563904 |
| mean | 候選 | 162.119773 | 160.577916 | 143.671116 | 1882081280 |

在這四次固定輸入實測中，完整程序樹 CPU 減少 **7.426%**、完整 outer wall 減少 **6.832%**、
mean worker lifetime peak RSS 減少 **14.043%**。序列化 CPU mean `19.567965 -> 17.623473 s`，
序列化 wall mean `20.527664 -> 20.443584 s`，依然接近持平。
未修改的 inspection CPU、啟動／reader 等成本也有變動，不能把完整 CPU 差值全歸因於 encoder。
每個版本只有兩次全新樣本；ABBA 減少近似線性漂移影響，未清 cache、隔離主機或證明統計顯著性，
因此這是本機固定輸入的採用證據，不能外推其他來源、完整訓練或其他硬體的收益。

候選已保留在四個 owner／test 檔案，其餘五個受保護的來源識別不變；
`source_disposition.json` 記錄所選來源與決策證據，212 項測試只在全部九個來源 hash 相同時重用。

## 常駐物件拆項診斷

採用決策後，另外完整執行相同的 372,550 份 inspection 與先行 CSV 寫入，在實際常駐 metadata
物件上做 A/B/B/A shadow writes，再拆解一次編碼。這些額外工作標記為 diagnostic-only，
不進入採用門檻，也不與沒有 shadow writes 的完整流程耗時混用。

| 常駐物件次序 | 版本 | durable write wall s | thread CPU s |
| --- | --- | ---: | ---: |
| 1 | 原版 | 4.710547 | 4.644774 |
| 2 | 候選 | 2.449045 | 2.411505 |
| 3 | 候選 | 1.979521 | 1.802339 |
| 4 | 原版 | 4.909559 | 4.786960 |

| 拆項（各一次） | wall s | thread CPU s |
| --- | ---: | ---: |
| plain metadata eligibility | 0.751746 | 0.765432 |
| native JSON encoding／UTF-8 | 0.379199 | 0.386107 |
| native whitespace format | 0.310174 | 0.315834 |
| newline／canonical durable write | 1.839786 | 1.681399 |
| output SHA readback | 0.133564 | 0.135428 |

五份 shadow／component manifests 均為相同的 202,684,792 bytes 與前述原檔 SHA；component
亦經獨立程序回讀。計時界線各自保留，不用 wall−CPU 推論 I/O wait，也不以這一次拆項比較前輪
不同時間的主機狀態。證據為 `encoding_components.json` 與各自的 reader receipts。

## 套件、完整 collector 與回讀

從隔離 source snapshot 建立 9,278,581-byte wheel，核對 collector／loader／C++／private JSON／
atomic writer 五份 asset 與目前來源逐 byte 一致。從 unpacked wheel 獨立匯入，驗證 guarded metadata
與 legacy bigint wire equality、原 C++ descriptor cache 與任意精度 integer identity。沒有安裝套件。
受影響語義證據仍為 **212 passed**，另外 fatal lint 與 diff checks 通過；這不是全 repository suite。

沿用前輪完整 collection argv，只換本輪輸出與新增 receipts。於 `2026-10-22T18:23:06Z` 完成獨立回讀：
**235,864 samples、26,568 summary groups、13 phases、12 outputs、6,581 unique originals**；
native backend identity、CSV regrouping、throughput、來源連結與 classification reuse links 均通過。
collector issues／traversal gaps 都是 0。完整 collector subprocess **316.599795 s**，另行 reader
**17.152695 s**；兩者分開保留，沒有省略回讀。inputs 與 host 時點不同，不能與前輪 collector wall
作因果加速比較。

| 本輪完整 collector phase | wall s | process CPU s |
| --- | ---: | ---: |
| code inventory／source capture | 14.719999 | 14.458749 |
| environment observation | 0.028404 | 0.018353 |
| scope inventory | 0.101616 | 0.102715 |
| service／HTTP／TEJ／storage receipts | 0.130646 | 0.129072 |
| acquisition receipts | 3.801920 | 2.384493 |
| engineering command receipts | 0.388123 | 0.388732 |
| training timing discovery | 6.706040 | 0.101435 |
| training timing ingestion | 7.929757 | 7.492006 |
| benchmark discovery | 20.306228 | 20.473382 |
| benchmark path selection | 25.113667 | 24.475873 |
| benchmark evidence inspection | 214.578790 | 139.840197 |
| aggregation／output serialization | 19.530543 | 19.201940 |
| final output hashes | 1.772998 | 1.716871 |

完整 collector 的主瓶頸仍為 evidence inspection。process CPU 為各階段自身程序的成本，
不是下面每個服務／producer 的完整 CPU；本表也不能取代全新四次完整程序樹 CPU 的計時口徑。

`required_runs.json` 綁定八次 encoding、原八次完整比較、全新四次 CPU 確認與最後 verification 的
canonical supervised receipts。`optimization_acceptance.json` 封存來源、所有原始樣本、輸出與本報告；
獨立 `optimization_acceptance_readback.json` 重新 hash 封存檔案、重算階段／原八次／新四次的全部
平均與門檻，並保留原拒絕、新採用及 scope。最終結論應同時檢查這兩份檔案。

## 保留的工作範圍

原八次的四筆原版 mean worker CPU 為 161.970295 s，其中 inspection CPU 142.454523 s，
占約 87.95%；候選分別為 155.101643 s 與 137.395274 s，占約 88.58%。
因此即使把 inspection 以外的 worker CPU 假設成零，這組固定輸入的 worker CPU 加速上界也僅
約 1.137 倍（原版）或 1.129 倍（候選）。這是依本組階段成本計算的 Amdahl 理想值，
不是硬體極限，也不是包含 reader／啟動的完整流程上界。
下一個量測重點應回到 inspection 的 read／SHA／parse／metric walk，沿用 operation 診斷及固定輸入
重測，逐項減少重複配置與 Python 呼叫；來源讀取、checksum、未知欄位與完整 metric coverage 都保留。

本輪最新清冊為 **2,399** 筆來源登錄項，其中 **1,361** 筆具有來源階段計時、**1,038** 筆未綁定，
**0** 筆通過這份 collector 定義的完整來源工作流接受。登錄包含群組、產品、credential 與 inventory
view，不能當成 API 數量或整體完成率。`remaining_workflow_coverage.json` 保留逐 prefix 缺口與
兩份 inherited historical config 的 FileNotFoundError，沒有合成替代設定或把它們算成完成。

`2026-10-22T18:23:09.883513+00:00` 再次對 enrolled `vastai1T` 做 strict-host、read-only transport
admission，耗時 2.795099 s、SSH exit 255／`No route to host`。這是連線失敗證據，沒有遠端硬體／
完整 epoch／fold 測速，也沒有啟動訓練。後續仍須分別完成 producer、loaded service 與遠端訓練接受。
`all_workflows_measured=false`、`hardware_limit_proven=false`。
