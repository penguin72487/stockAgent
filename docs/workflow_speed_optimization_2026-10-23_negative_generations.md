# 工作流測速第十八輪：減少重複負例讀取

本輪接續完整收集器的 evidence inspection。前輪為實質進展；本輪保留
2,399 個來源的原範圍、全部既有 explicit receipts、四個訓練根目錄及完整輸出與讀回。
世代重用候選未通過接受門檻，已撤回本輪兩個 runtime/test 檔案的變更，逐 byte
回到本輪開始的狀態，保留所有先前與其他 dirty work。重啟後的完整基線已收集與
獨立讀回；清冊另經明確的歷史識別對帳，維持原任務範圍。

## 原理與實作

前輪完整 inspection 為 778.13 秒 wall、163.36 秒 CPU，並捕捉到讀取執行緒
等待 `folio_wait_bit_common`。該輪 354,155 個不含計時資料的候選檔案合計
7,757,297,503 bytes；主要可消除的工作是再次讀取、雜湊與判定相同負例。
改用另一種語言不會自行消除這些磁碟操作。

候選沿用 `Evidence`、`inspect_benchmarks` 與 `verify_collection`，曾加入兩個選項；
它們只保留在本輪 private candidate snapshot，現在的 canonical CLI 沒有這些選項：

- `--record-negative-generations`：完整讀取後，記錄一般檔案的
  device、inode、size、mtime_ns、ctime_ns，產生有界壓縮索引。
- `--reuse-negative-generations <既有完整收集目錄>`：先讀回驗證完整 seed，
  再檢查 collector、JSON parser、Python、native walker 與 recursion limit 身分。
  只有兩次 stat 都符合原世代的負例可以沿用分類；變動、遺失、類型或預算不符時
  回到原本讀取與失敗處理。新收集目錄凍結使用的 seed 索引，供獨立讀回核對。

候選預設關閉重用。所有明確指定及含計時資料的收據，仍重新讀取、雜湊、解析並凍結
完整原始 bytes。原收據沒有完整世代資訊時，不能補猜 inode/ctime 來重用。
重用負例依靠檔案世代與已驗證索引，並不宣稱每次重新雜湊其 payload；
這個觀測邊界會保留在 collection、索引與讀回收據中。

## 實測與接受條件

使用既有完整固定 corpus：372,550 個候選、107 個 explicit，原始 bytes 合計
9,383,953,790。先完整讀取建立新的 seed，然後以 A/B/B/A、B/A/A/B 各四次
fresh interpreter 執行，比較原實作與世代重用。每次包括匯入、seed 驗證、全部輸出、
獨立子程序讀回及所有後代 CPU，不能只報省去讀取的局部時間。

六份輸出核對完整語義；只排除負例分類的解析/重用旗標、世代欄位與不同實作的
凍結來源封套。來源路徑、SHA、bytes、分類、計時原值、樣本與 summary 均保留。
兩個平衡半組都要滿足完整 child wall <= A 的 98%、CPU <= A 的 102%，才接受。
seed 首次建置成本另列，不能宣稱是新節點冷啟動速度。

候選通過 166 項 focused tests；候選相關回歸共 507 項，16.19 秒。
包含同長度改寫後還原 mtime、inode 替換、symlink 轉向、檔案遺失、第二次 stat
變動、讀取預算，以及偽造索引/重用依據的拒絕。最初四個 mutation fixture 字串
長度寫錯，於執行產品邏輯之前失敗；修正後全部通過，失敗收據保留。

完整 seed 的 worker body 含輸出與獨立讀回為 281.072362 秒，inspection
229.997351 秒；不含匯入與 corpus plan 載入。canonical 收據內的
`performance.contract=waited_command_monotonic_resources_v1` 另測得完整 child launch
through wait 為 288.005452219 秒、後代 CPU 217.724928 秒；包含匯入與 setup，
不含 queue、最終 canonical receipt 寫入及 detached workers。
這筆首次建置成本不能省略成冷啟動加速。

八次 fresh interpreter 完整命令全部成功，固定 corpus 每次 372,550 檔、107 個
explicit，六份輸出語義全部相同。兩個平衡半組結果如下；數字是各兩次的平均：

| 順序 | A 完整 wall 秒 | B 完整 wall 秒 | B/A wall | B/A CPU | 接受 |
| --- | ---: | ---: | ---: | ---: | --- |
| A/B/B/A | 180.459393 | 213.248358 | 1.181697 | 0.959181 | 否 |
| B/A/A/B | 172.924689 | 140.651538 | 0.813369 | 0.799664 | 是 |

第二半較快不會取代第一半失敗。本輪接受規則要求兩半都通過，因此撤回候選，
原始八次結果、候選原碼、測試、seed 和逐檔依據完整保留。
outer 的 `peak_child_rss_bytes` 是 `RUSAGE_CHILDREN` 累積峰值，不能拿來比較
逐試次記憶體；worker 另有自己程序的 RSS，但 reader peak 沒有獨立分離量測。

每個 B 都避免 354,177 個負例 payload 的 7,757,382,750 bytes 邏輯讀取；
此值不扣除新增索引/seed 驗證，也不是實體磁碟流量減少量。
首組 A 的 inspection 120.377569 秒、reader 11.304235 秒；B 的 inspection
61.162218 秒，卻另需 seed 驗證 15.472738 秒、reader 28.178454 秒，
其餘 worker 階段 33.758759 秒。最後一項由 serial clocks 相減，涵蓋後端設定、
來源封存、序列化及輸出雜湊，不能只歸因於 gzip 或單一 writer。

共享主機負載也有波動。11:06:16.872900–11:06:27.898368 UTC 的 12 筆
被動觀測只捕捉到本工作程序 running/`do_wait`，沒有捕捉到 D-state；
全機 CPU/IO PSI 是當時負載背景，不能指認寫入者、證明硬體飽和，或拿來扣除 wall。
這不是新的一份 I/O 阻塞機制證明。

撤回後的來源已通過 497 項回歸，19.82 秒；這是 selected baseline 的檢查，
與上面的 507 項 candidate tests 分開，不相加當作同一套驗收。

第一次全量收集遭 Linux/WSL boot 中斷。前 boot
`48f1764b-4ab6-4c49-89b8-c995bc2d8b2a` 與現 boot
`5a688359-9220-4539-930c-d3ab52660954` 不同，舊 systemd unit 已 `not-found`、
MainPID=0，舊 boot/PID identity 已不存在。canonical `run-reconcile` 已將舊 run
記為 interrupted，沒有捏造完成時刻、exit code 或速度；`collection_full` 原樣保留。
這證明 Linux/WSL boot 變更，不證明 Windows 實體重啟或中斷原因。
八次比較、來源撤回與 497 項檢查都在中斷前完成，沒有重跑。

重新核對八個選定來源檔案 SHA 全部相同，並完成新 boot 的 headroom admission，
以新目錄 `collection_recovery` 補做 13 階段 collector。收集器 472.592532 秒，
收集加獨立讀回的 wrapper 為 497.476864 秒；canonical launch through wait 為
497.774643 秒。獨立程序驗證 232,724 筆樣本、6,798 份 unique originals、
13 階段及所有輸出，沒有 traversal gaps。inspection 為 331.653735 秒 wall、
208.288580 秒 CPU；aggregation 為 27.152797 秒 wall。恢復後的 wall 不能和
重啟前八次測試、或前輪不同輸入的收集，當作受控速度比較。

覆蓋檢查發現重啟後的即時清冊只有 2,144 個來源、1,511 個已有階段綁定，
比原範圍少 255 個 TEJ 來源識別。135 個來源的 342 筆原始階段樣本仍存在，
卻因識別缺漏而未綁定。第一個 phase 驗收正確拒絕範圍縮小，失敗 run 保留。
沒有刪除來源、補造時鐘，或將舊的服務狀態當作目前健康證據。

`registry_reconciliation` 是原收集之外的明確補充，不覆寫原清冊或 collection。
沿用 canonical binder，從前輪已凍結清冊補回這 255 個歷史識別，保留原快照時間，
標記 `historical_identity_missing_from_current_snapshot`，且 current visibility、
live readiness 均為 false。獨立、無 producer imports 的讀回逐列確認 232,724 筆
樣本的值、時鐘、單位、來源 SHA、context 與順序完全不變；只有這 342 筆新增既有
source ID annotation。數值 summary 逐 byte 相同。補充後原範圍仍為 2,399 個來源、
1,646 個已有階段綁定、753 個待量測；對帳沒有新增任何 duration。

因此 13 階段的單次 traversal 是完整的，單次即時清冊的原範圍完整性為 false；
原來源範圍是經公開標記的補充清冊保全。不能合併成單次全清冊成功或 live readiness。
後續應在 canonical inventory owner 處修正冷啟動清冊的持續識別，避免依賴人工補充。

下一個計算候選由固定基線的正例重複度決定：18,364 個含計時資料的路徑，只有
3,730 個 unique SHA；1,626,555,574 bytes 正例中，有 1,165,982,400 bytes 是
重複內容。這是可避免的 JSON 解析量上界，尚不是可省去的 wall 或實體 I/O。
下一輪會保留每個檔案的原始讀取、SHA、完整 receipt 與凍結，只研究在單次收集
內重用相同完整 SHA 的不可變 metric extraction，並限制記憶體與 metadata 大小。
此方向可避開本輪跨收集世代索引的驗證成本，仍需相同輸入、完整輸出、兩組平衡
比較和獨立讀回才能採用。

## 證據與未完成範圍

- [測量協定](../artifacts/benchmarks/workflow_speed_optimization_20261021/round18/protocol.json)
- [固定輸入基準讀回](../artifacts/benchmarks/workflow_speed_optimization_20261021/round18/fixed_seed.readback.json)
- [平衡比較協定](../artifacts/benchmarks/workflow_speed_optimization_20261021/round18/comparison_protocol.json)
- [八次完整比較](../artifacts/benchmarks/workflow_speed_optimization_20261021/round18/comparison.json)
- [八次階段原始速度表](../artifacts/benchmarks/workflow_speed_optimization_20261021/round18/stage_breakdown.csv)
- [Boot 中斷證據](../artifacts/benchmarks/workflow_speed_optimization_20261021/round18/boot_interruption.json)
- [恢復後完整收集](../artifacts/benchmarks/workflow_speed_optimization_20261021/round18/collection_recovery/collection.json)
- [恢復後完整讀回](../artifacts/benchmarks/workflow_speed_optimization_20261021/round18/collection_recovery_readback.json)
- [來源識別對帳](../artifacts/benchmarks/workflow_speed_optimization_20261021/round18/registry_reconciliation/reconciliation.json)
- [來源識別獨立讀回](../artifacts/benchmarks/workflow_speed_optimization_20261021/round18/registry_reconciliation_readback.json)
- [未綁定來源清單](../artifacts/benchmarks/workflow_speed_optimization_20261021/round18/remaining_source_timing_gaps.csv)
- [完整階段驗收](../artifacts/benchmarks/workflow_speed_optimization_20261021/round18/phase_acceptance.json)
- [下一輪正例解析重複度](../artifacts/benchmarks/workflow_speed_optimization_20261021/round18/positive_parse_duplication.json)

目前不能宣稱完整 collector 加速、正式下載服務已載入新碼、全部工作流已測完，
或已達硬體/計算極限。來源 E2E、資料完整性、遠端訓練與本機工程驗收仍是不同邊界。
