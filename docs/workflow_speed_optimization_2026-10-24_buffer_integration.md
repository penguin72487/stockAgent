# R28：串流輸出緩衝整合與完整驗收

R27 固定輸入、八次 fresh-child 比較與獨立四十檔 SHA 讀回全部完成。
五份報告的完整輸出子流程，兩組平衡比較耗時降低 **40.8%／46.0%**，
CPU 同時降低。R28 已套用兩處 `buffering=1 << 20`，**191 項回歸通過**；
新完整 collector、canonical reader 與原版獨立 reader 已全部成功，29 份
量測來源的 SHA 結束時均未改變；未宣稱完整 collector 的因果加速、所有來源
已測速或硬體計算極限。

## 實測機制與範圍

前輪完整流程中，inspection 為 **309.355206 秒**，aggregation／輸出
**106.681460 秒**，最後輸出 SHA **39.548669 秒**。先減少重複檔案操作，
再依剩餘 CPU／等待成本決定下一項實驗。上述階段並非全部都由 renderer
構成，因此不能把局部倍率直接套用到 collector 或訓練。

本候選只改 `write_csv`／`json_lines` 的開檔緩衝大小，CSV BOM、encoding、
newline、欄位順序、nested JSON、負零與 JSONL 原值均保留，沿用原 close
與 durability policy。沒有更換 Windows writer、刪除資料或縮減測量工作。

輸入固定為 R20 五份已驗證報告，共 **620,615,573 bytes**，沿用 R25
已 terminal、完整 SHA 驗證的 private input copy，未再建立 payload copy。
每個 child 包含 startup、輸入 SHA／parse、全部五個 renderer、close 與
輸出 SHA。CSV parse 後以 wire string 重建原 bytes，兩邊輸入相同；這不是
原 typed collector 的 CPU 測試。父程序另用 **226.790977 秒**完整讀回
五檔×八次，全部符合原始 SHA。

| 順序 | A whole child 均值 | B whole child 均值 | wall B/A | Linux CPU B/A |
| --- | ---: | ---: | ---: | ---: |
| ABBA | 171.941450 s | 101.808953 s | 0.592114 | 0.841818 |
| BAAB | 117.883625 s | 63.619562 s | 0.539681 | 0.751877 |

兩半均通過事先固定的 wall ≤ 0.98、CPU ≤ 1.02。A1／B2 renderer 的
Linux `syscw` 為 **72,842／596**，寫出字元 bytes 均為 **620,615,573**。
這個 counter 不是 9p RPC 數，也不能據此宣稱 122 倍工作流加速。
原 continuation 已 succeeded，實際 systemd unit inactive／MainPID 0。

## 來源與回歸

R28 使用新的明確 source cohort，不修改 R26／R27 封存來源或歷史 guards。
兩處 keyword 反轉後恢復 R26 profile 的全部 bytes，兩個 renderer AST 與
實際 R27 候選一致。profile SHA 為
`d3bf90fe5fc9bb795ee131519ded2fb35f0dd7b14fae40fcd1def87f3667bb36`，
preimage 為 `3301e6d58956dc96efd83d4f05a636e215bb195505d01b713906436f888360da`。
串流 helper／其測試及另外七份原來源均保留既有 SHA。

`test_workflow_speed_profile`、`test_workflow_snapshot_streams` 與
`test_tw_public_journal_ingestion` 共 **191 passed、0 skipped**，完整 pytest
child **6.324953 秒**。既有測試檢查 ragged CSV／nested value／負零、mapping
語意、來源變動拒絕、原生 journal 身分與串流回讀邊界；未增加鏡像測試。

## 完整流程結果

實際 owner 為 `workflow-r28-full-buffer-collect-20261023T202715-5ab2de65`。
admission D free **568,979,103,744 bytes**、root free **46,535,266,304 bytes**，
minimum memory headroom **24 GiB**。容量不足延期，不刪除來源或證據。

保留 R26 全部 **221** 個 explicit receipt 與四個 training root，加入兩個
本輪證據；全部 13 個階段、native journal、已知 2,399 個來源身分均保留。
沒有啟用診斷 profile、已拒絕的 metric-cache 或未宣告的 fallback。
依序執行完整 collector、canonical reader、封存原版 reader 與獨立
registry／native-clock oracle；結束時核對所有 measurement-source SHA。
本次輸入隨真實收據更新，並非固定全 collector A/B，完整秒數只能作新觀測。

三階段均 exit 0，原 owner succeeded／實際 inactive／MainPID 0：

| 完整階段 | wall 秒 | descendant CPU 秒 |
| --- | ---: | ---: |
| collector | 478.930210 | 279.106527 |
| canonical readback | 111.663914 | 27.480128 |
| frozen original reader＋獨立 scope／clock oracle | 180.177010 | 37.271157 |

三階段與結束檢查 **771.013923 秒**，canonical launch-through-wait command
**772.230498 秒**，含 admission／driver 成本，前置 queue 不在其中。
新觀測 aggregation／輸出 **42.601608 秒**、最後 SHA **31.699216 秒**，
不是固定全流程 A/B 的因果加速證據。

兩種 reader 除觀測時間外回傳欄位完全一致：**13 outputs、6,823 unique
originals、240,620 samples、27,608 groups、13 phases**。所有 **29** 份
measurement source 結束 SHA 與開始相同，native journal 的 PID／invocation／
spec／policy／原 clock 均由獨立 oracle 核對，沒有 lost source IDs、lost bindings
或匯入舊 timing。collection SHA 為
`75f2fd296063d4b53b513ac23d12e617424d0bf5be232a8e64e9d1b16450fb8b`。

stage timing 為
**1,647／2,399**，尚有 **752** 項未綁定。分母包含 group／credential／product／
inventory views，不是 2,399 個可執行 API，不能直接當作同粒度端點接受率。
完整逐來源 E2E acceptance 仍為 **0**。四筆 issue 與 R26 相同，皆為
FinMind 舊成功列缺少一致 transport clock：TaiwanStockKBar、TaiwanStockPriceTick、
TaiwanStockTradingDailyReport、TaiwanStockWarrantTradingDailyReport；保留原始
row ID 與缺口，沒有補造歷史 clock。traversal gaps 為 0；工程整合驗收與
上述 producer-clock／來源 E2E 缺口分別保留。

R29 診斷 continuation 已於上述同一原 owner 成功 terminal、完整 acceptance
通過後執行並完成驗收。保留全部 223 receipts／
四個 training roots／13 phases，以既有 opt-in clocks 拆開 inspection 成本，
多出的 `inspection_profile.json` 明確作第 14 個輸出；兩個 reader 與獨立
scope oracle 全部保留。timer overhead 與跨 thread／inclusive 重疊範圍不作
速度收益，也沒有為診斷更動正式來源碼。
成本拆解與後續唯讀路徑比較見 [R29 診斷報告](workflow_speed_optimization_2026-10-24_inspection_diagnostic.md)。

## 證據

- [完整比較與四十檔 SHA 讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round27/comparison_result.json)
- [原 R27 continuation 完成與決策](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round27/continuation_result.json)
- [兩處 patch 與可逆證明](../artifacts/benchmarks/workflow_speed_optimization_20261021/round27/buffer_integration_preparation.json)
- [新整合來源封存與前置 owner gate](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round28/preparation.json)
- [191 項測試驗收](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round28/checks.json)
- [canonical source adoption](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round28/canonical_adoption.json)
- [完整參數與 scope 契約](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round28/collection_full_input_contract.json)
- [完整流程原 owner 收據](../artifacts/operations/agent-workflow/runs/workflow-r28-full-buffer-collect-20261023T202715-5ab2de65/run.json)
- [三階段完整工程驗收](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round28/full_collection_acceptance.json)
- [原版獨立 scope／native-clock oracle](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round28/full_collection_scope_readback.json)
- [canonical 完整回讀](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round28/canonical_readback.json)
- [接續診斷原 owner](../artifacts/operations/agent-workflow/runs/workflow-r29-inspection-continua-20261023T203155-a0c24bbd/run.json)
- [R26 reader 全量驗收](workflow_speed_optimization_2026-10-24_reader_integration.md)
- [原生 writer 的負面完整實驗](workflow_speed_optimization_2026-10-24_native_output.md)
