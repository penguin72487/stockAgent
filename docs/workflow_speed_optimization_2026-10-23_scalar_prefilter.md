# 工作流第十五輪：純量欄位預篩選未通過反序確認

本輪候選未採用。八次完整固定輸入執行與獨立讀回全部同值，但反序確認的
完整 wall 與 CPU 未達事前門檻。收集器及 C++ 核心已逐 bytes 還原到候選前版本；
保留一項通用的巢狀 JSON 語意回歸測試。證據位於
`artifacts/benchmarks/workflow_speed_optimization_20261021/round15/`。

接續 [Round 13 靜態盤點](workflow_speed_optimization_2026-10-23_static_inventory.md)
及 [Round 14 TEJ 覆蓋](workflow_speed_optimization_2026-10-23_tej_table_coverage.md)。
Round 13 的全量觀測中，證據檢查為最大階段，約 491.607 秒；這是選擇調查目標的
依據，不是本輪固定輸入的加速分母。

## 假說、範圍與門檻

原生 walker 仍對純量資料欄位呼叫分類器。候選在既有 C++ walker 的選用第五參數
傳入必要後綴 `seconds`、`_ms`、`_s`，省略不可能命中的純量欄位分類。
所有容器仍巡覽；可能的計時欄位仍由原 Python 判定匹配、redaction、escaping、
有限值與數值身分。一般四參數介面保留原分類呼叫與例外行為。
候選來源及測試完整保留於 `candidate_source/`，未留下未驗收的執行分支。

每次執行均保留 **372,550 份、9,383,953,790 bytes、107 explicit receipts**，
全部原有 read/stat/SHA、分類、必要解析與巡覽、原文保存、六份完整輸出與另一程序
的讀回。每次須有 705,929 個 reported fields、2,006 個正規樣本、722 組摘要；
reader 核對全部輸入識別及 3,739 份保留原文。354,177 份非計時 payload 只有
身分與分類紀錄，不宣稱其原文均已另存或冷還原。

輸入 manifest SHA 為
`f4bb9b16c7d423b7dbb85bc1f1c61cb5e990596de2c2fe881672572b0959044d`。
預讀 worker=1、batch=256 MiB、C++ backend、profiling 關閉。未清 OS cache、
預熱整份輸入或停止背景 owner。編譯準備另列，每次 cached module 驗證與初始化
包含於執行；來源、binary 與 build receipt 身分保留於各筆結果。

完整 outer wall 包含啟動、匯入、input-plan、工作、輸出、獨立 reader 與結束。
完整 CPU 是 worker 及其已等待 reader 的 `RUSAGE_CHILDREN` user+system delta，
加上 controller spawn/wait 的 process CPU；node probe、準備編譯與後續比較雜湊另計。
RSS 是單一 worker 生命週期峰值，不是父子程序同時總峰值。

原 ABBA 事前門檻：完整平均 CPU 至少下降 2%、完整平均 outer wall 與 worker RSS
均不增加超過 2%、六輸出與 reader 同值，前後保守 memory headroom 至少 4 GiB。

## 全部原始結果與採用決定

| 組別／次序 | 版本 | 完整 outer wall 秒 | 完整程序樹 CPU 秒 | Worker peak RSS bytes |
| --- | --- | ---: | ---: | ---: |
| ABBA 1 | 原版 | 339.127418 | 241.832636 | 1520787456 |
| ABBA 2 | 候選 | 200.178657 | 191.678535 | 1522581504 |
| ABBA 3 | 候選 | 175.593876 | 177.247543 | 1517932544 |
| ABBA 4 | 原版 | 183.274277 | 183.352559 | 1515380736 |
| BAAB 1 | 候選 | 167.929894 | 170.866817 | 1519910912 |
| BAAB 2 | 原版 | 189.839335 | 184.584291 | 1526452224 |
| BAAB 3 | 原版 | 184.126569 | 183.353406 | 1521201152 |
| BAAB 4 | 候選 | 234.039651 | 218.312255 | 1510129664 |

第一組平均 outer wall `261.200848 -> 187.886267` 秒、CPU
`212.592597 -> 184.463039` 秒，原門檻通過。然而原版首尾 wall 相差約 85%，
worker `ru_inblock` 分別為 15,135,024 與 266,640，主機 load 亦不同。
block counter 不是邏輯輸入 bytes，也不能把 wall 減 CPU 當成 I/O 等待。
這些差異使穩定收益仍有疑慮，不能直接宣稱候選加速 28.07%。

第一組完成後、四筆確認開始前，另行登記全新 BAAB；保留相同輸入、輸出與原
CPU/wall/RSS 門檻，另要求前後 scratch free 至少 16 GiB。原四筆不重用為確認
樣本，最終採用要求兩組各自通過。沒有修改第一組結果或放寬門檻。

確認組平均 outer wall `186.982952 -> 200.984773` 秒，增加 **7.488%**；完整 CPU
`183.968849 -> 194.589536` 秒，增加 **5.773%**；worker RSS 平均下降約 0.578%。
wall 與 CPU gate 均失敗，故 `performance_candidate_adopted=false`。
最後較慢的候選完整保留；沒有將兩組合併平均、挑最快樣本或追加比較直到過關。
本組結果也不證明候選在所有硬體或負載下一定較慢。

## 還原、驗證與剩餘工作

候選先通過四個相關模組 **297 passed**。撤回後 **291 passed**，包含新增的
300 組固定亂數巢狀 JSON／Unicode redaction／原始數值物件與 signed-zero oracle；
兩批測試重疊，不相加成獨立測試數。focused diff check 通過，未執行全 repo suite。
當前 `scripts/profile_workflow_speeds.py` 與 `stockagent/workflow_metric_tree.cpp`
各自等同本輪 `baseline_source/` 的原始 bytes。沒有重啟服務、呼叫來源 API、
改動 queue／來源資料、安裝套件或啟動 GPU 計算。

候選撤回後，執行程式沒有新的變更，因此未執行原先準備的候選 package／全量
collector。Round 13 的 **13 phases／12 outputs** 全量讀回與 Round 14 的
**135 個 producer task DB 獨立核對**，仍為各自範圍的既有證據，不冒充本輪重跑。

`remaining_source_timing_gaps.csv` 保留 Round 14 已封存清冊的全部 **902** 項未綁定
紀錄、active scope、service keys 與缺失原因。已有 stage timing 綁定 **1,497／2,399**；
尚待補齊包括 FinMind 122、TEJ 120、TW public 196、economic 93、OpenBB 20。
群組／產品／憑證／inventory 等紀錄粒度不同，不視為同分母 API endpoint。
TEJ 缺合格歷史計時不等於從未下載；FinMind 後續須分開直接 fetch/validate/publish
耗時與 cycle 按比例分攤的 calibration。缺失時鐘不回推或補零。

下一步優先在 canonical producer 補足直接計時與完成單位，並繼續拆解必要
read/stat/SHA、解析與寫出成本。完整逐來源工作流、遠端 GPU fold 與硬體下界仍需
各自驗收；`all_workflows_measured=false`、`hardware_limit_proven=false`。

原始契約與結果：`comparison_protocol.json`、`balanced_comparison.json`、
`reverse_protocol.json`、`reverse_comparison.json`。選用版本與測試收據為
`selected_source_disposition.json`；封存與另一程序的驗證為
`optimization_acceptance.json`、`optimization_acceptance_readback.json`。
