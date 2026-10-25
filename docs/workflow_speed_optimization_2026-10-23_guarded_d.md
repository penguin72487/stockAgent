# 工作流測速第二十輪：守護的 D 工作區全量比較

本輪承接第十九輪預設關閉的正例解析快取。八次交錯命令與跨版本語義核對均已完成，
第一半組未通過 wall 門檻，第二次完整獨立讀回也確認候選不接受。
快取變更已撤回，保留來源清冊修正；選回後 598 項回歸及完整 13 階段收集與讀回均通過。
這是 collector 的 benchmark-inspection 工作量，
不能外推成交易、訓練、資料來源完整 E2E 或整個 collector 的加速。

## 保持相同工作與完整驗證

原始輸入沿用第四輪凍結清單：372,550 個路徑、107 份 explicit receipts、
9,383,953,790 bytes。A 與 B 都逐檔讀取原始資料、驗證世代與完整 SHA，
保留相同 evidence 預算、全部六份輸出及另一個程序的獨立讀回。
B 僅在同一次收集中重用完整 SHA 相同的不可變 metric scalars。

順序固定為 A/B/B/A、B/A/A/B。每個 child 的完整 wall 包含 fresh interpreter、
來源 guard、讀取、處理、輸出與獨立 reader，CPU 包含等待完成的後代程序。
父程序的跨版本語義比較另行執行；不得把它混入某一版。
兩半組各自要求 wall B/A <= 0.98、CPU B/A <= 1.02，且六份輸出語義一致，
才可能接受。實驗預設快取為 0；拒絕後已移除候選旗標與實作，不從單次數字選版。

ext4 未達原容量 gate，所以沒有開始 ext4 全量 trial，也沒有刪除來源或舊證據。
本輪只將新輸出寫入已驗證的 D 私有 benchmark 目錄，保留 32 GiB，
每個 worker/reader 重查既有 volume guard 並持有 directory FD；缺盤不回退 C。
兩版都使用相同 D 位置，不能與以前 ext4 輸出的時間直接混算。

## 八次完整命令的原始觀測

| Trial | 版本 | 完整 child wall 秒 | 後代 CPU 秒 |
| --- | --- | ---: | ---: |
| 1 | A | 726.623619 | 283.838344 |
| 2 | B | 619.658904 | 238.311754 |
| 3 | B | 697.973224 | 257.182720 |
| 4 | A | 604.333684 | 235.549613 |
| 5 | B | 590.653166 | 229.839860 |
| 6 | A | 595.512194 | 229.982330 |
| 7 | A | 528.252965 | 209.554848 |
| 8 | B | 416.528676 | 169.346884 |

第一個完整 A/B/B/A 半組：A 平均 665.478651 秒、B 平均 658.816064 秒，
wall B/A = **0.989988**；CPU B/A = **0.953997**。雖然 CPU 約少 4.6%，
wall 只約少 1.0%，未達事前要求的 2%，因此此半組 **不接受**。
反向 B/A/A/B 的 A 平均 561.882580 秒、B 平均 503.590921 秒，
wall B/A = **0.896257**、CPU B/A = **0.908198**，通過此半組。
兩半組必須各自通過；前半組的失敗不因反向較快而抵銷，門檻未更改。
不可挑單次最快值，或把兩半組合併後重新決定接受。

既有 worker 時鐘可分割成以下不重疊的平均區間，沒有新增 performance trial：

| 半組／版本 | inspection 秒 | 獨立 reader 秒 | 其他 worker 秒 | worker 時鐘外秒 |
| --- | ---: | ---: | ---: | ---: |
| ABBA A | 343.534899 | 163.002004 | 146.379039 | 12.562709 |
| ABBA B | 389.132615 | 134.892227 | 125.853567 | 8.937655 |
| BAAB A | 249.657161 | 141.579819 | 162.865460 | 7.780140 |
| BAAB B | 233.096713 | 134.566175 | 127.481829 | 8.446204 |

inspection 包含原始讀取、SHA、凍結、解析與擷取。其他 worker 是其時鐘內扣除
inspection／reader 後的餘額，包含初始 backend／實作凍結及報表輸出／SHA，
不能把整個餘額叫作單一 writer 耗時。時鐘外含 fresh command、guard、輸入載入
與退出等餘額。四區間相加逐輪等於完整 child wall。

基準版的 inspection 占完整 wall 約 44.4%～51.6%、reader 約 24.5%～25.2%。
ABBA 的 B inspection 平均反而較長，完整 wall 略短同時伴隨其他區間變動；
不能把完整時間差全部歸因於解析快取。這些區間也不是硬體上限或 I/O 的純歸因。

第一個 B 重用 12,532 次解析，少解析 657,171,121 bytes，charged cache 峰值
6,413,989 bytes；native 讀取、完整 SHA 與原始凍結不減少。
首個 A 的 inspection 為 411.745 秒，獨立 reader 為 153.773 秒；
輸出與其餘處理仍占相當時間。這些是階段觀測，沒有排除 I/O、快取狀態或背景負載。

4.04 秒的被動共享主機樣本觀察到收盤快取、盤中資料整理、FinMind quota snapshot、
FinLab refresh、備份與 storage lifecycle 同時使用資源。這些是短暫資源計數，
不能當成各來源操作延遲、長期平均或某個程序造成全部 PSI 的證明；不扣除其耗時。
另有原生收盤快取工作回報 `CapacityDeferred: filesystem_headroom_exhausted`。
目前只凍結有限日誌與來源識別，尚未取得失敗 allocation 的完整 namespace 身分，
也未證明相同工作的反覆重試。後續應先補足該證據，再評估 receipt-only controller
能否提前略過仍不可能 admission 的工作，保留 cache hits、來源變更與恢復後的重試。

完整比較後，無 producer imports 的第二個獨立 reader 已重算兩組門檻、
核對八輪固定輸入、原始 gzip SHA、重用 anchor 及六份輸出，canonical 命令耗時
3,911.604186 秒。它沒有被 FD prototype 替換，也沒有因先完成部分輪次而中止。
選回後 8 份來源逐 byte 符合保留清冊修正的 `registry_source`，已保存選版清單；
598 項回歸通過，pytest 子命令 22.025633 秒，canonical launch-to-wait 22.723689 秒。

## 選回後完整工作量與剩餘瓶頸

完整 collector 保留原 205 份 explicit receipts、四個 training roots，另加八份這輪
驗證收據與既有 2,399 個來源識別的 inventory，不匯入舊計時或目前健康判斷。
13 個階段、獨立 reader 與另一個標準庫 scope／summary 核對均通過：
240,567 筆樣本、27,555 組 summary、6,811 份唯一 originals、2,399 個來源 ID、
1,646 個已有階段綁定、753 個待補測，traversal gaps 為 0。
目前 monitor 已包含全部舊 ID，因此此次歷史缺席補入為 0；沒有把舊計時帶回。

| 觀測區間 | wall 秒 | 邊界 |
| --- | ---: | --- |
| benchmark inspection | 400.282819 | 原始讀取／SHA／凍結／解析／擷取 |
| aggregation 與 output serialization | 157.655372 | 包含 manifest 等輸出，不是純 JSON encoder |
| final output hashes | 30.160295 | 最終各輸出的完整 SHA |
| 全部 13 階段相加 | 737.403737 | 不含最後 `collection.json` 寫入 |
| driver 內完整 collector 加獨立 reader | 1,071.938641 | 含完整子命令與中間 guard |
| canonical driver launch-to-wait | 1,083.011288 | 含 driver 初始化、後續接受核對及收據產出前工作 |

原 `collection_wall_seconds=707.426036` 在 final hashes 前取樣，不能當成完整
13 階段或整個命令的耗時。driver 與階段時鐘之差也不是純 reader 的時間。
這次讀入的檔案與樣本已有變化，輸出在 D，不能與前輪 ext4 收集直接算倍率。
目前 inspection、輸出與讀回仍值得優化；新增的完整 reader 操作診斷另列，
所有 inclusive 區間不可直接相加或從原八輪 clocks 扣除。

完整保留 A1 的診斷讀回已通過，含六份輸出及 3,740 份唯一原始 gzip：
instrumented reader wall **504.296455 秒**、process CPU **41.564526 秒**；
canonical launch-to-wait **505.846604 秒**。各範圍如下：

| 診斷操作 | 次數 | inclusive wall 秒 |
| --- | ---: | ---: |
| Path.resolve | 3,747 | 260.293891 |
| lstat，巢狀於 resolve | 33,715 | 211.289914 |
| gzip.open constructor | 3,740 | 93.061162 |
| gzip read 與解壓 | 7,749 | 30.470685 |
| json.loads | 2,009 | 1.311730 |
| Path.open constructor | 11 | 0.499355 |

Path.resolve 約占此診斷 wall 的 51.6%，其中 lstat 已包含在內，不可再相加。
constructor 時鐘不包含其後整份報表的串流讀取／SHA；未包覆區間不能任意命名為
單一瓶頸。這是帶觀測開銷、當次共享主機條件下的診斷，沒有當成接受候選的 A/B trial。
下一步優先測量完整 reader 的路徑／開檔減量，保留全部 bytes、SHA、世代與逃逸檢查。

## 額外讀回的小檔階段工程

完整八輪計時結束後，額外獨立 reader 的短暫系統呼叫觀察顯示部分時間在
9p `read` 等待。這些是有追蹤開銷的診斷，未修改原八輪 clocks；不能把短暫
比例當成整個 reader 的歸因。後續追蹤看到完整 gzip 小檔及 EOF 探測，
不足以直接判定緩衝區有缺陷。

另寫的標準庫 oracle 以 directory FD 固定 canonical `evidence/<sha>.gz` 樹，
拒絕 symlink，以見證的壓縮長度讀完全部 bytes，核對檔案與目錄世代，再驗證
完整 gzip、原始 SHA 與長度。13 項語義測試通過（0.89 秒），包含內容損壞、
名稱／SHA 不符、逃逸及讀取途中替換；兩版 D smoke 各 37 份完整原始檔均通過。
初版測試的例外範圍未涵蓋結束時的第二次目錄世代拒絕；原測試、失敗 run/JUnit
與修正後的測試均保留，未修改 reader 來放寬檢查。

128 份固定唯一快照、每份不超過 1 MiB 的 A/B/B/A、B/A/A/B 篩選，
包含 FD 初始化與世代檢查，兩半組 wall FD/path 為 **0.201492／0.207697**，
thread CPU 為 **0.337481／0.249108**。這只證明該小檔階段在當次條件下較快。
每個完整 trial 還有 651,007,138 bytes 的六份報表等工作；不能把階段倍率
套成整個獨立 reader 或 collector 的倍率。

觀察時原額外 reader 已有三輪明確通過標記，故沒有停止／替換它，也沒有再啟動
另一組完整 reader。FD prototype driver 未執行，canonical collector reader 未修改，
原來的八輪完整讀回已全數完成。此工程結果供下一次獨立完整比較使用。

## 證據位置

- [第十九輪工程與容量紀錄](workflow_speed_optimization_2026-10-23_positive_metrics.md)
- [D 位置與實際 namespace](../artifacts/benchmarks/workflow_speed_optimization_20261021/round19/d_continuation_location.json)
- [D harness 的逐項來源變換](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round20/harness_transition.json)
- [全量比較 protocol](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round20/comparison_protocol.json)
- [已通過的 trial 進度](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round20/comparison_progress.json)
- [八次完整比較與兩半組判定](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round20/comparison.json)
- [八輪第二次完整獨立讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round20/comparison_readback.json)
- [選回來源與完整 SHA](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round20/selected_source.json)
- [選回後 598 項回歸](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round20/selected_checks.json)
- [完整 13 階段與 reader 接受](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round20/full_collection_acceptance.json)
- [來源、所有樣本及 summary 獨立核對](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round20/full_collection_scope_readback.json)
- [剩餘 753 個來源的明細](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round20/remaining_source_timing_gaps.json)
- [完整 reader 操作診斷](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round20/reader_diagnostic.json)
- [八輪既有時鐘的完整分割](../artifacts/benchmarks/workflow_speed_optimization_20261021/round19/complete_trial_intervals.json)
- [第一半組不可變觀測](../artifacts/benchmarks/workflow_speed_optimization_20261021/round19/first_half_observation.json)
- [事前固定的後續驗證規則](../artifacts/benchmarks/workflow_speed_optimization_20261021/round19/followup_protocol.json)
- [獨立 reader 加強來源與讀取數的追加規則](../artifacts/benchmarks/workflow_speed_optimization_20261021/round19/followup_amendment_1.json)
- [共享主機被動觀測](../artifacts/benchmarks/workflow_speed_optimization_20261021/round19/shared_pressure_observation.json)
- [原生容量失敗的有限觀測](../artifacts/benchmarks/workflow_speed_optimization_20261021/round19/postclose_capacity_observation.json)
- [額外 reader 系統呼叫診斷](../artifacts/benchmarks/workflow_speed_optimization_20261021/round19/independent_reader_syscall_observation.json)
- [FD oracle 實際 D smoke](../artifacts/benchmarks/workflow_speed_optimization_20261021/round19/fd_snapshot_d_smoke.json)
- [FD 固定子集交錯比較](../artifacts/benchmarks/workflow_speed_optimization_20261021/round19/fd_snapshot_screen.json)
- [保留原完整讀回的決定](../artifacts/benchmarks/workflow_speed_optimization_20261021/round19/fd_subset_decision.json)

依原比較門檻不接受正例快取；完整八輪、兩次讀回、選版、回歸、13 階段 collector
及獨立來源／summary 核對、完整 reader 診斷均已完成。第十九輪的 538 個檔案與
13 份 terminal receipt 已獨立封存驗證；本輪的封存與獨立讀回分別記錄於同一 D
artifact root 的 `optimization_acceptance.json` 與 `optimization_acceptance_readback.json`，
以後者的實際狀態及 SHA 為準。
尚未證明完整 collector 加速、正式服務已載入、所有來源均完成 E2E，或已達硬體計算極限。
