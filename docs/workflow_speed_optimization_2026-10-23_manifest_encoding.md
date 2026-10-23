# 2026-10-23：完整 bytes 不變的 evidence manifest 編碼實驗

本輪沿用 [Round 9 的拒絕結果與測速](workflow_speed_optimization_2026-10-23_deferred_paths.md)，
針對已量到的 manifest 寫出成本實作候選。八次完整固定輸入比對的兩組序列化門檻都未通過，
候選已還原；原始程式與 200 項候選測試保留於凍結的 `optimized_source`。正式九份來源檔
與 Round 9 驗證版本完全相同，還原後另跑 **172 passed**。
產物根目錄為 `artifacts/benchmarks/workflow_speed_optimization_20261021/round10`。

## 成本與修改範圍

Round 9 診斷的 `evidence_manifest_json_write` 為 9.689845 秒 wall、7.515256 秒 thread CPU，
輸出 202,684,792 bytes。讀取、SHA、原文留存、JSON 語義與樹遍歷仍是必要工作；
這個編碼階段的加速不能直接換算成整體 collector 或來源工作流的加速。

`stockagent/private_json.py::pretty_metadata_bytes` 對有界深度、精確 builtin 型別的
字串／整數／布林／null／dict／list，先用 stdlib compact encoder 編碼，再用已安裝的
`msgspec.json.format(..., indent=2)` 排版。整數由 stdlib 編碼，沒有縮窄或轉成浮點數。
浮點數、自訂型別、深樹與 legacy 值走原本的 stdlib pretty encoder；既有解碼函式 AST 不變。

候選只有 `scripts/profile_workflow_speeds.py::write_evidence_manifest` 接入新 helper。
仍呼叫原本的 `atomic_write_bytes(..., durable=True)`，保留換行、fsync、atomic replace
與 parent-directory sync。`downloader/artifact_io.py`、C++ walker、財務與模型語義不變。

## 語義與階段實測

三個受影響測試模組共 **200 passed**。新增檢查涵蓋任意精度整數、控制字元、Unicode、
空容器、順序、浮點 fallback、自訂容器、深樹、cycle、bytes、unpaired surrogate、
原生 formatter 失敗，以及實際 manifest 的耐久寫入 owner 與 bytes oracle。

第一組校準誤將第二次完整 eligibility walk 放進候選計時，兩個 block 一通過、一不通過；
結果與八份原始樣本保留於 `encoding_comparison.json`，未用來宣告採用。
移除 benchmark 專用的重複 walk 後，候選程式、輸入、格式與 15% 門檻均不變。
新一組仍按 A/B/B/A、B/A/A/B 執行，結果為：

| 階段確認組 | 原版寫出 wall | 候選寫出 wall | 原版寫出 CPU | 候選寫出 CPU | 原版峰值 RSS | 候選峰值 RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A/B/B/A | 7.661056 s | 5.595796 s | 7.354922 s | 4.876485 s | 1,084,268,544 B | 768,694,272 B |
| B/A/A/B | 8.620542 s | 5.928586 s | 7.186710 s | 4.923567 s | 1,084,153,856 B | 768,634,880 B |

兩組均通過：mean write wall、CPU 均至少減少 15%，worker lifetime 峰值 RSS 不增加超過 2%。
每次都讀取／SHA／解析同一份 372,550 identities 的實際 manifest，完整耐久寫出後另啟 reader，
確認輸出長度與 SHA 與前輪封存的原檔一致。CPU 為 worker process 的所有 threads，
不含獨立 reader child；RSS 為單一 worker 的 lifetime peak，非父子程序同時總峰值。
host/cache 未隔離，沒有清 cache 或冷碟測速宣告。

## 完整流程採用門檻

`comparison_protocol.json` 在八次完整流程前固定 A/B/B/A、B/A/A/B。
輸入為固定的 372,550 檔案、9,383,953,790 bytes、107 explicit receipts；每次保留全部
讀取、SHA、分類、解析、walk、六份輸出與獨立 reader。profiling 關閉、read worker=1、batch=256 MiB。

兩個 block 都須同時滿足：六份輸出 bytes 與前輪 reference 相同、獨立 reader 通過、
平均 RSS ≤102%、完整 outer wall ≤102%、worker CPU ≤100%、序列化 wall 與 CPU 均 ≤90%。
此門檻容許完整流程近乎持平時採用已證實的局部加速，不會因此宣稱完整流程有同等百分比收益。
不得取最快單次或只取通過的 block；失敗時保留所有負面結果並還原候選。

完整結果（CPU 為 worker process、包含所有 threads；不含 imports/setup 與 reader child，
outer wall 則包含它們；RSS 是單一 worker 的 lifetime peak）：

| 次序 | 版本 | 完整 outer wall s | worker CPU s | 序列化 wall s | 序列化 CPU s | RSS MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 原版 | 269.434098 | 181.799177 | 19.884209 | 18.084660 | 2089.79 |
| 2 | 原生排版 | 214.553087 | 163.350135 | 35.144180 | 26.517721 | 1792.84 |
| 3 | 原生排版 | 221.093483 | 178.914396 | 22.123752 | 21.453994 | 1791.05 |
| 4 | 原版 | 245.684208 | 186.522601 | 23.103617 | 22.624395 | 2095.17 |
| 5 | 原生排版 | 200.979256 | 160.628757 | 20.838994 | 19.679640 | 1793.04 |
| 6 | 原版 | 184.515451 | 156.742064 | 20.601934 | 20.555281 | 2090.28 |
| 7 | 原版 | 174.022125 | 152.914276 | 16.588795 | 15.907781 | 2084.32 |
| 8 | 原生排版 | 155.768348 | 139.550077 | 19.285682 | 18.397390 | 1789.16 |

第一組序列化 mean wall `21.493913 -> 28.633966 s`、CPU `20.354527 -> 23.985857 s`；
反向組為 wall `18.595364 -> 20.062338 s`、CPU `18.231531 -> 19.038515 s`。
兩組的 RSS／完整 outer wall／worker CPU gate 都通過，序列化 wall／CPU gate 都失敗，
因此未採用。相同原版的完整時間由 269 秒降到 174 秒，不能把整體均值差直接歸因於編碼修改。
所有六份輸出 SHA 都與前輪 reference 相同；每個獨立 reader 驗證 372,550 份輸入身份、
3,739 份唯一保留原文、2,006 筆樣本與 722 組 summary。354,177 份 non-timing body
只保留身份與分類，不宣稱逐份原文已留存／回讀。

## 完整常駐物件的追加診斷

完整比對拒絕後，另外重新執行全部固定讀取、分類與 CSV 工作，在同一個常駐 metadata
物件圖上按 A/B/B/A 寫出四個 shadow manifest。各檔均為 202,684,792 bytes，SHA
`e204263d1bf4da025ede9dc8eb0492911b454c73d571e491864999dfd90ac4bc`。
四次 wall 為 `7.316528 / 4.608795 / 4.573937 / 6.148002 s`，thread CPU 為
`7.033397 / 4.527414 / 4.494931 / 5.818007 s`。主要六份輸出仍另由原候選 owner 寫出，
並完成完整獨立 reader；這些診斷不納入八次選擇樣本，也不改變拒絕結果。

另一次同物件的分段操作如下，時鐘沒有跨執行緒相加，也不把 wall 減 CPU 當成 I/O wait：

| 操作 | wall s | thread CPU s |
| --- | ---: | ---: |
| builtin metadata 資格檢查 | 0.995558 | 0.985599 |
| stdlib compact 編碼及 UTF-8 | 2.013034 | 1.998039 |
| 原生 whitespace 排版 | 0.365834 | 0.364887 |
| 換行與原 canonical 耐久寫入 | 2.026493 | 1.897363 |
| output SHA 回讀 | 0.135437 | 0.135290 |

局部 encoder 與整體序列化有不同觀測結果，尚未單獨證明主機負載、快取與配置成本各占多少。
下一個可驗證方向是 guarded 原生編碼；必須另外核對大整數、Unicode、fallback 與全部 bytes，
重新預先登記完整比對，不能沿用局部收益宣告採用。

完整 collector 與獨立回讀均已通過；目前的正式來源仍是還原後的 Round 9 版本。
本次完整 wall `326.244265 s`，獨立 reader `18.231578 s`；235,851 筆樣本、26,551 組 summary、
13 個 phase、12 份輸出、6,572 份唯一保留原文、native backend 身份驗證 1，issues／traversal gaps 均為 0。
這是目前輸入的觀測，不能和前輪不同輸入／主機狀態的完整 wall 作因果速度比較。
正式九份來源與舊 wheel 仍相同，因此重讀既有 wheel 及 unpacked collector／loader／C++／
private JSON／atomic writer 五個實際資產的 bytes，沿用既有 native build 證據；未安裝套件或宣告整包／服務驗收。
固定輸入的各次 output 與原文 readback、候選／還原測試及 package 資產會一併封存。
封存入口為 [optimization_acceptance.json](../artifacts/benchmarks/workflow_speed_optimization_20261021/round10/optimization_acceptance.json)，
另由獨立程序產生 [optimization_acceptance_readback.json](../artifacts/benchmarks/workflow_speed_optimization_20261021/round10/optimization_acceptance_readback.json)。

## 尚未解決的範圍

本輪不代表全部來源、已載入服務、遠端完整 epoch/fold 都測過。
目前清冊為 2,399 筆來源登錄項、1,361 筆綁定階段、**1,038 筆未綁定**；這些包含產品、群組、credential
與 inventory view，不能把數量直接當成 API 數量。完整來源工作流驗收仍為 0；兩個既有歷史 config
缺檔也保留，未冒充已修復。階段綁定會隨目前來源收據變化，不能由綁定數直接推出服務退步或完成率。
2026-10-22 17:30:14 UTC 的 enrolled SSH 檢查仍為 `No route to host`（exit 255）。
遠端連線與 GPU compute admission 分開，沒有啟動訓練、重啟正式服務或刪除來源資料。
`all_workflows_measured=false`、`hardware_limit_proven=false`。
