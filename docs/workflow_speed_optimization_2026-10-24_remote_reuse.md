# 工作流測速續作：遠端既有證據重用與 SSH runtime 恢復

2026-10-23 17:54–18:00 UTC 的唯讀觀測確認目前登錄端點可連線，雙 RTX5090
由既有程序使用。獨立重算八個既有工程 case 的保留曲線、完整 epoch 計時及
queue elapsed，結果與原比較一致；沒有再啟動 GPU 測試、重建 cache 或修改訓練 owner。
本輪新增的是證據接續與控制流程修復，沒有新增訓練加速倍率。

## 已驗證可接續的測速

18 份 metadata 檔在各自讀取前後均保持同一 generation。重新核對 v2 request、
source pin、比較、選用路徑及 v3 parent/recovery request 連結，並由八個 case
各四 epoch 的保留列重算穩態 epoch 3/4 完整 MAX-rank 均值。

| 時鐘 | 完整穩態 epoch：A → B | 穩態倍率 | 四 epoch case 均值：A → B | case 倍率 |
| --- | --- | --- | --- | --- |
| 08:45 | 103.964667 → 27.827847 秒 | 3.735994x | 650.203570 → 522.327724 秒 | 1.244819x |
| 09:00 | 103.565679 → 27.720685 秒 | 3.736043x | 648.192904 → 395.265401 秒 | 1.639893x |

每個時鐘各為原有 A/B/B/A；完整 case 包含準備、cache 重用及原生命週期工作。
這些是既有資料的重算，不是本輪重跑、純裝置時間、新節點首次啟動或報酬改善。
本輪未重新開啟全部原始訓練 payload，也未逐檔驗證整個 loaded source tree。
單檔穩定讀取不等於跨檔 transaction snapshot。

v2 來源 pin 為 `def65564b57bf02375162793db633c8352ad3adffaa0ecabe70f02dff6d6707a`。
v3 的 request、fixed release 與既有 startup 驗證指向
`aed5c44399c0e68e8f8b8320d67a748bde42cff04aa266b20cfd236b3e58829d`；
startup 收據記錄 1,828 個檔案，並明列 imported-code coverage 未證明。
沒有把該舊收據轉述成本輪完整載入驗證。

## R23 觀測的 owner 與待測瓶頸

觀測時 NVML 顯示兩卡使用 31,546／31,944 MiB，GPU busy 99／98%，compute
PID 分別為 445816／445817。這是單次板卡／程序觀測，沒有以 busy 百分比推論
有效算力、Tensor Core occupancy 或計算極限。

v2 正式 queue 的保留狀態為 `formal_canonical_queue_exited`、returncode 1。
既有 calendar recovery 為 `recovered_formal_cases_running`，已列出六個正式 case；
v3 finite-guard owner 仍為 `waiting_for_calendar_recovery_lifecycle`，
`gpu_training_started=false`。成功工程比較與正式研究收斂分別處理；沒有重排既有
queue、取得另一份 GPU lease 或宣稱新 finite guard 已通過 CUDA 驗收。

原瓶頸收據的 candidate host 欄位顯示 finite check 約 587–612 ms/batch，
但 `timing_synchronized=0`，這些非同步區間包含等待，不能與 autograd 相加或
當成可完全移除的純運算。已有 v3 裝置端 finite guard／延後進度讀回候選負責
後續完整比較；此次沿用該 owner。待測仍包括實際 sparse transpose／窄投影
裝置時間、帳本 recurrence/VJP、host launch/sync 及完整報告成本。

## SSH runtime 修復

首次 canonical SSH 探測在本機缺少 `/run/stockagent-agent-ssh` 時失敗。
保留原失敗後，使用相同驗證設定並對單次連線停用 multiplexing，成功讀取完整
遠端操作指南及 owner metadata。沒有降低 host-key 驗證。

隨後呼叫既有 `scripts/prepare_agent_ssh_profile.py::prepare`，以私有候選 profile
及原 socket 路徑恢復 0700 目錄；候選與原 `/etc` profile 完整 bytes 相同，
canonical profile 未改寫。兩次 canonical SSH `uname -s` 均成功，完整子命令
wall 為 **0.310476／0.036303 秒**。這是恢復及連線重用觀測，沒有 ABBA 效能
驗收，也沒有宣稱新增開機自動恢復；`/run` 的重新準備仍依既有 runtime 流程。

## 證據

- [本輪位置與 admission](../artifacts/benchmarks/workflow_speed_optimization_20261021/round23/location.json)
- [首次連線失敗及完整耗時](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round23/enrolled_transport_observation.json)
- [操作指南完整擷取身分](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round23/enrolled_guide_observation.json)
- [目前 supervisor、GPU 及 owner 快照](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round23/remote_runtime_observation.json)
- [既有曲線／完整流程與 request 收據擷取](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round23/remote_performance_receipt_observation.json)
- [18 檔連結核對及八 case 獨立重算](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round23/remote_reuse_readback.json)
- [canonical socket 恢復、設定不變與兩次連線](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round23/enrolled_mux_recovery.json)
- [原訓練研究、比較與 finite-guard 排程](tw_stock_futures_conditioning_2026-10-23.md)
- [同輪原生日誌接入與封存瓶頸](workflow_speed_optimization_2026-10-24_native_journal_ingestion.md)

完整目標仍持續中。新的全量 reader 比較沿原 continuation 等待舊封存的獨立讀回；
受保護的十二份來源在本輪再次逐檔核對相符，未為恢復連線而變更計算或資料語意。

## R24 18:51 UTC 既有工程計時續讀

canonical SSH 再次唯讀擷取既有 metadata，完整子命令 **1.070830 秒**；
這是傳輸觀測，沒有 ABBA 連線加速主張。36 份 metadata 各自讀取前後 generation
穩定，另保留一份預期缺少的 `remote-failure.json`。不是跨檔 transaction
snapshot，也沒有重新 hash 原始 curve／workflow／NPZ 或 loaded source tree。

獨立核對既有 v2 request／source、v3 parent 與 calendar-recovery request 連結，
並重算六個成功工程 case 的 48 列 curve／workflow epoch 資料。每例完整四個
epoch、兩 rank、每 epoch 一次 optimizer update；steady 使用 epoch 3／4 的
完整 MAX-rank wall，與原 acceptance 一致。

| 工程 case | 原 owner 完整 case wall | 完整 steady MAX-rank epoch 均值 |
| --- | --- | --- |
| 08:45 control | 397.031 秒 | 28.145 秒 |
| 09:00 control | 418.550 秒 | 27.798 秒 |
| 08:45 notional | 435.285 秒 | 27.858 秒 |
| 08:45 Muon | 394.011 秒 | 27.757 秒 |
| 09:00 notional | 421.023 秒 | 28.335 秒 |
| 09:00 Muon | 432.319 秒 | 27.740 秒 |

原 owner 的非空執行收據有被觀察到，本次沒有重驗完整執行 payload。原 v2
五個未完成／不接受的觀測仍以 owner／case 身分保留，沒有被 recovery 同名
成功案例覆蓋。不同 clock／optimizer 與單次 case 的成本差不構成 ABBA
效能排名，沒有由表格選用 Muon 或推論研究收益。

同次板卡觀測為 31,742／31,820 MiB、兩卡 busy 均 100%，既有正式訓練仍執行。
v3 progress 在 18:51:41 UTC 仍為 `waiting_for_calendar_recovery_lifecycle`、
`gpu_training_started=false`；沒有以工程計時代替 finite guard 的 CUDA 驗收。
未新增 GPU 工作、重建 cache、修改正式 owner 或 source。

- [36 份穩定 metadata、目前 GPU／owner 快照](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round24/remote_followup_observation.json)
- [六 case／48 epoch 列獨立重算與舊失敗保留](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round24/remote_followup_readback.json)

## R33 21:23 UTC：原 owner 已推進至 finite-guard 比較

沿用 R24 已封存的唯讀 remote probe，透過 canonical SSH 再讀既有 metadata，
完整 SSH 子命令 **1.160770 秒**。36 份 generation 穩定的 metadata 中，34 份
SHA 未變，兩份變動均為原 owner 的進度。stdout、stderr、probe 原始碼、
SSH profile SHA 與獨立程序的讀回結果完整留存；本機 supervisor 已正常結束。

calendar recovery 的進度為 `recovered_formal_queue_exited`；v3 由原先等待
恢復流程推進至 `finite_guard_ABBA`／`guard__b2`，`gpu_training_started=true`。
正式 v3 supervisor 仍 RUNNING。`supervisorctl status` 整體 exit 3 也保留，
其清單同時含其他 EXITED／STOPPED 服務，沒有將整體 exit code 當成 v3 結果。
這次只驗證小型 metadata，完整訓練 payload、數值／gradient oracle、載入程式
身分與 finite-guard 效能採用，仍由原 owner 的完成收據驗收。

同點兩張 RTX5090 各 **798 MiB／32,607 MiB**、GPU busy 0%，功耗
19.20／11.67 W。這是當下硬體取樣；完整 epoch／fold 與後續 case 需另外核對。
本次沒有新增訓練、重啟 owner、調整 GPU 設定或變更任何遠端檔案。

- [R33 36 份 metadata、原 owner 及 GPU 取樣](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round33/observation.json)
- [34 份未變／兩份進度變更的獨立讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round33/readback.json)

## R35：完整 finite-guard A/B/B/A 未達採用門檻

21:41 UTC 再讀原 owner 的既有收據，新增 `guard-acceptance.json` 與
`guard-speed-retained-evidence.json`，generation 穩定的 metadata 共 38 份。
來源 request／parent request 指紋未變；原 owner 已轉入 `four_case_engineering`。
完整 SSH 子命令 **13.148016 秒**，保留較慢觀測，不將傳輸時間變動當成加速。

另一個本機 supervisor 命令獨立重算四個 case 的 **16 列 curve＋16 列完整
workflow epoch**；每個 case 四個 epoch、兩個 timing ranks、每 epoch 一次
optimizer update。steady 使用 epoch 3／4 的 `complete_epoch_wall_s_max_rank`，
未以較窄的 curve clock 取代完整 epoch。

| 完整量測範圍 | A 均值（秒） | B 均值（秒） | B/A |
| --- | ---: | ---: | ---: |
| steady 完整 MAX-rank epoch | 27.974207 | 27.879348 | 0.996609 |
| 完整四個 epoch case | 394.454980 | 393.955168 | 0.998733 |

分別僅減少 **0.3391%／0.1267%**。原 owner 的選擇門檻包含至少 3% steady
與 projected-100 改善，`selected=false`，維持不採用。projected-100 明列為
推估，本次未當成 100 個實際 epoch，也未把 host finite-check 的等待全部
換算成可消除的 CPU 運算。

每例保留的 test／deployment 收據均記有非空整口部位；本次只核對 retained
metadata 與其既有 clock／SHA 參照，沒有重新 hash 原 NPZ、curve 原檔或
載入程式，也沒有新增帳務／gradient 一致性驗收。原 metadata 回報 temporary
payloads 已依先前使用者要求退休、formal checkpoints 未刪；本次沒有執行
退休、恢復或新增訓練工作。原 v2 失敗與 recovery 成功仍各自保留。

- [R35 38 份 metadata 與原 owner 進度](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round35/observation.json)
- [16 個完整 epoch 獨立重算與負面採用結果](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round35/guard_readback.json)

## R39：四個後續工程 case 已完成，正式訓練沿原 owner 執行

22:25 UTC 仍使用相同、未修改的唯讀 probe 與 canonical SSH，完整 SSH
子命令 **1.035093 秒**，取得 40 份各自 generation 穩定的 metadata。
v3 新增 engineering acceptance／retained evidence；獨立重算其中四個
case 的 **16 列完整 workflow epoch＋16 列 curve**，每例四 epoch、兩 rank、
每 epoch 一次 optimizer update，與原 owner 的 acceptance 完全相符。

| v3 工程 case | 完整 steady MAX-rank epoch 均值 | 完整四 epoch case wall | case wall 減完整 epoch MAX-rank 總和 |
| --- | ---: | ---: | ---: |
| 08:45 AdamW lr3e-5 | 27.948068 s | 396.516525 s | 190.723787 s |
| 08:45 notional＋Muon | 27.976994 s | 395.007555 s | 189.215277 s |
| 09:00 AdamW lr3e-5 | 27.961773 s | 394.015760 s | 188.422689 s |
| 09:00 notional＋Muon | 28.002762 s | 395.271158 s | 189.157130 s |

steady 一律取 epoch 3／4 的 `complete_epoch_wall_s_max_rank`；未使用較窄的
curve clock 代替。最後一欄只是完整 case 與 epoch maxima 總和的差值，
尚未歸因為 startup、報告或等待，也不能視為可移除成本。下一項 GPU 流程
分析需沿既有 owner 讀取對應 preparation／startup／report 原生時鐘。

四例均為不同數值設定的單次工程驗證，不是 A/B/B/A 效能比較，也沒有用
數值軌跡不同的結果主張 bitwise parity、投資收益或新加速倍率。每例保留的
test／deployment metadata 各記有 145 日整口部位；本次沒有重新 hash 原
NPZ／curve／workflow 原檔或整個載入程式樹，也沒有新增財務／gradient 驗收。
原 v2 的失敗、calendar recovery 成功、R35 guard 未採用，仍分開保留。

原 v3 progress 在 22:02:03 UTC 已記為 `formal_cases_running`，四個設定均由
原 owner admitted。22:25 點觀測兩卡分別 **31,168／31,146 MiB**、busy
**92%／56%**，compute PID 為 734129／734130；這個觀測點的高 VRAM 對應
正在執行的正式訓練。沒有將單點 busy 當成算力上限或平時閒置負載結論，
也尚未驗證正式 case 已完整結束。本次沒有新 GPU dispatch、重啟、
排程改動或遠端寫入。

- [R39 原始 metadata、GPU／owner 觀測與傳輸時間](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round39/observation.json)
- [16 列完整 epoch／16 列 curve 獨立重算](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round39/engineering_readback.json)

## R42：保留完整時計，辨認會重複相加的區段

沿 R39 凍結 metadata 重算四例共 16 個 epoch，沒有增加遠端讀寫或訓練。
這些曲線每 epoch 為 23 batches、一次 optimizer update、兩個 timing ranks，
`timing_synchronized=0`。完整 steady train MAX-rank 均值為 **26.179787–
26.316874 秒**，完整 epoch 均值為 **27.948068–28.002762 秒**；兩種 MAX
可能屬於不同 rank，不能將差值當成一段獨立、可移除的工作。

將 host 的 `forward`、`backward_total`、`finite_check` 相加，會比同一列
`train_total_s` 多出 **12.496197–14.059119 秒**。這直接證明這組時鐘不能
作為互斥區段加總；其中的巢狀計時與 GPU 非同步等待，也不能全部歸為
可刪除的 CPU 運算。R35 完整 guard 比較未達採用門檻，仍維持原結論。

同一批完整 epoch MAX-rank 比較窄的 curve MAX-rank 多 **0.621757–
0.665922 秒**。後續驗收保留完整時計，不能以較窄分母主張加速；這個
差值本身也沒有進一步歸因為 plotting 或 bookkeeping。完整 case 與 epoch
總和之間約 188–191 秒的差距仍需原生 startup／preparation／report clocks。

本次重算命令由 canonical supervisor 實測 **2.088719 秒 wall／0.582191 秒
child CPU**，僅是 metadata 分析本身的成本，沒有新的訓練加速或算力極限
結論。保留原 metadata、所有時計與來源 SHA。

- [16 個 epoch、巢狀計時反證與各時計差值](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round42/phase_clock_readback.json)
- [重算命令的時鐘、CPU 與終結收據](../artifacts/operations/agent-workflow/runs/workflow-r42-phase-clock-readbac-20261023T224525-0f0e314a/run.json)

## R44–R46：取得正式訓練的原生準備時鐘與原始 bytes

R44 沿既有 canonical SSH 唯讀目前正式 `clock0845__adamw_lr3e5` 的時鐘，
不是新訓練，也沒有將已退休工程 payload 重建或覆寫。SSH 子命令
**0.683778 秒**，取得七份 generation 穩定檔案；兩份預先指定的 queue
summary 路徑不存在，原樣保留 missing，沒有據此判定正式 owner 已停止。
另一個命令核對同一 `run_id=1792792929-733671`、rank 0／world size 2，
逐列 `elapsed_s` 相加與各自 `cumulative_s` 相符。

| 原生時計範圍 | 原始紀錄數 | 該範圍 cumulative |
| --- | ---: | ---: |
| launcher 到 trainer handoff | 6 | 121.049435 s |
| trainer startup | 6 | 2.156286 s |
| train-group pre-epoch | 16 | 6.013550 s |

第一個範圍中，DDP launcher／child start **22.036615 秒**、arguments／
imports **22.227727 秒**、panel build／cache load **76.529523 秒**。
panel 紀錄為 3,353 dates、2,757 symbols、14,727 features。這是 rank 0
原生區段，包含 canonical builder、feature attachment 及 DDP 協調邊界；
尚未拆成互斥的 I/O、CPU、GPU 或等待成本，也不是 MAX-rank 準備時鐘。
三個時計範圍各自驗證，不將其加總冒充完整 case wall。

這讓下一個準備瓶頸有具體定位：先沿既有 panel builder 拆解來源／cache
驗證、載入、feature attachment 與 rank coordination，再比較完整冷／暖
case。這筆正式 case 的時計不能回填成 R39 四個不同工程 case 的 188–191
秒差距；首次 compile、正式報告與完整 case 收據仍須各自量測。

R44 同時取得 **92 列完整 workflow epoch**；R45 使用可完全反轉的衍生
probe，唯一內容擴充為攜回同一 bounded read 的 base64 原始 bytes。
原 R44 source 未改，SSH 子命令 **0.736058 秒**。R45 保留七份原始檔，
新鮮程序逐檔讀回 byte count／SHA／JSON 內容，共 **127 列 JSONL**：
6＋6＋16 個 startup／pre-epoch clocks，以及 **99 個完整 workflow epoch**。
讀回子命令 **0.388078 秒 wall／0.187235 秒 CPU**。這是當下觀測，尚未
證明正式 case、fold artifact 或所有四個 case 完成。

R46 隨後實際呼叫未修改的 canonical `collect_training`，並在另一個程序
以 frozen original parser 逐筆比較。**226 個 stage／epoch samples 完全
一致**；兩套原始讀回器均通過 **六份輸出、九份 unique originals、32 個
summary groups**。保留全部 **2,399 個既知來源身分**，不匯入舊時計，
也沒有把 training clocks 當成新增 source-endpoint binding 或全域涵蓋率。
此增量 collection 為 **1.079619 秒**，獨立雙 parser／reader 為
**2.583489 秒**；完整 supervisor 命令為 **5.048631 秒 wall／2.033558 秒
child CPU**。所有計時與負面觀測均保留。

下一次完整 13-phase workflow 須保留原 training roots，另外加入
`round45/remote_sources/artifacts/markets/tw_stock_futures_conditioning_history2013_20261023_v3`
作為 training root，並同時收錄 R32 原始 operation journal 與 R21 calendar
journal。R46 是已驗證的原始時計增量；完整來源身分、正式採用、全流程
加速與硬體極限仍需對應範圍的驗收。

- [R44 原生時計、限定目錄與 owner／GPU 觀測](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round44/observation.json)
- [原生 scope 的逐列累積核對與 92 個完整 epoch](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round44/native_clock_readback.json)
- [R45 七份原始 bytes／SHA 與 127 列紀錄讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round45/primary_readback.json)
- [R45 新鮮程序讀回時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round45/primary_completion.json)
- [R46 現行與原始 collector 的完整逐筆比較](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round46/training_delta_readback.json)
- [R46 增量 collection／reader 分段時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round46/completion.json)
- [R46 完整命令與資源收據](../artifacts/operations/agent-workflow/runs/workflow-r46-training-clock-delt-20261023T225526-ccce14f5/run.json)

## R48／R50：155 個完整 epoch 原始時計與停止條件核對

**23:20 UTC** 沿原 v3 Supervisor owner 唯讀取得新不可變快照；重用 R45
完整 capture／fresh-reader 流程，只可逆更換輸出 cohort。SSH **0.758607 秒**，
七份穩定原始檔、**183 列 JSONL** 全部 byte count／SHA／JSON 讀回成功。
四份原生時鐘分別為 6、6、16、**155 列完整 epoch**；三份準備時計 SHA
完全不變，新的 workflow timing bytes 保留 R45 全部 99 列的精確前綴，
增加 56 列。epoch 1–155 連續、timing ranks 均為 2。

原始 request 的停止條件為 `cap1000_no_improvement100_min_delta0.0001`：
最多 1,000 epoch，patience 100、min delta 0.0001。**100 是未改善的耐心值，
不是固定訓練輪數**。155 列時計或 Supervisor running 均不足以證明正式
case、fold finalization、全部四個 case 或其財務驗收完成。

| 同一 case、epoch 3–155 共 153 輪 | median | min | max |
| --- | ---: | ---: | ---: |
| complete epoch MAX-rank | 28.811775 s | 27.950755 s | 30.230825 s |
| complete train MAX-rank | 27.060057 s | 26.177728 s | 28.519729 s |

末輪 complete epoch 為 **28.620031 秒**、complete train 為 **26.896183 秒**。
兩列各自統計，MAX 可能屬於不同 rank，沒有相減成可移除的區段，也沒有
用 host clocks 推定 GPU device time。主要下一個量測範圍仍是完整 train
內的 forward／backward／ledger／communication 關係；保留全部 validation、
test、plot、checkpoint，準備流程則沿 R44 的 panel scope 拆解。

R50 實際重用 R46 canonical 增量流程，僅更換不可變來源及相應筆數。
現行與 untouched original parser 的 **338 samples** 全部相等；兩套 reader
各通過 **六份輸出、九份 unique originals、32 summary groups、2,399 個
既有來源身分**。collection **1.628696 秒**，獨立雙 reader／parser
**7.372980 秒**；完整命令 **10.922572 秒 wall／2.606446 秒 child CPU**，
owner succeeded／inactive／MainPID 0。這是既有正式工作的新時計保存，
不是新訓練或新的加速比較。

R49 原嘗試在建立 payload 前因來源替換次數的保護檢查寫成 3 而失敗；
實際 R46 固定來源是 2 次。R50 在新 cohort 修正該計數並通過完整增量
驗收；原失敗來源、log 和收據保留，另追加 superseded 處置，沒有覆寫。
R47 已凍結的完整驗收仍使用原 R45 99-epoch 快照及 226 samples oracle，
沒有在實驗中途更換輸入；本次後續 56 輪保持獨立來源與 SHA。

- [R48 七份原始 bytes 與 183 列時計讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round48/primary_readback.json)
- [R48 可逆重用 R45 capture 的來源衍生](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round48/controller_derivation.json)
- [R50 338 samples 與 untouched original 全部相等](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round50/training_delta_readback.json)
- [155 個 epoch、原 bytes 前綴與原停止條件](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round50/chronology_readback.json)
- [增量 collection／雙 reader 時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round50/completion.json)
- [R50 命令與終結收據](../artifacts/operations/agent-workflow/runs/workflow-r50-extended-training-c-20261023T232246-1ab0e649/run.json)

## R71–R73：補齊原始曲線、存檔與最終繪圖時鐘

**00:48:31 UTC** 的 R73 快照沿同一 v3 owner 唯讀取得 **21 份穩定原始檔、
6,117,877 bytes、712 列 JSONL**。另一程序逐檔核對 raw bytes、SHA、JSON
及 R48 七份舊來源的精確前綴；兩例完整 workflow 分別連續 227／101 輪。
擷取 **1.726346 秒 wall／0.152144 秒 descendant CPU**，獨立讀回
**3.878519／0.895097 秒**。沒有新增 GPU 工作或遠端寫入。

| 08:45 正式 case，steady epoch 3+ | 實際完整輪數 | complete epoch MAX-rank median | complete train MAX-rank median |
| --- | ---: | ---: | ---: |
| AdamW lr3e-5 | 227 | 28.779305 s（225 輪） | 27.037601 s（225 輪） |
| notional＋Muon | 101 | 28.920621 s（99 輪） | 27.116318 s（99 輪） |

這些是不同 optimizer 的正式單次訓練，不能當成 A/B 效能排名。時鐘均保留
兩 rank 原契約；MAX 可能屬於不同 rank，沒有相減成一段獨立工作。第一例
`fold_complete.json` 宣告完成，第二例當時仍有後續進度；本次沒有重讀及
hash model／checkpoint／backtest payload，故不宣告完整 fold 財務驗收、
全部四例完成或目前載入程式身分已重驗。

新增保存的生命週期時計包括第一例 `save_timing.json`、`plot_timing.json`
及兩例動態命名的 `epoch_curve_plot_timing_every1.json`：

| 原生計時範圍 | 秒 | 範圍限制 |
| --- | ---: | --- |
| 第一例 save total | 0.283028 | 含 checkpoint、NPZ、表格、audit 與 deployment 存檔 |
| 第一例 final plot total | 1.635113 | 最終曲線與年度圖 |
| 第一例 walk-forward refresh | 10.338656 | 原生 refresh 區間 |
| 第一例 post-plot total | 12.260505 | inclusive，不能再與上述 plot／refresh 相加 |
| 第一例末次 epoch-curve plot | 0.982663 | loaded／plotted 227 列；只代表這次繪圖 |
| 第二例當次 epoch-curve plot | 0.636070 | loaded／plotted 101 列；只代表這次繪圖 |

這組正式訓練的一次性存檔／最終繪圖成本遠小於數百輪 train 累積成本；
後續 GPU 調校仍先定位完整 train 的 forward、backward、ledger 與同步。
逐輪 plot 只保存當次原生 receipt，尚未逐輪重建其總成本。

兩份完整 epoch curve 也已原樣保存，含每輪約 400 個欄位。
`timing_synchronized=0`；host spans 巢狀且可能包含非同步等待，
`_cuda_ms_*` 的零值不證明裝置成本為零。帶負號的 `unaccounted` remainder
保留原值，不能截成零，也不能把 host finite-check 數值當成可移除的純 GPU
運算。R35 完整 guard 比較未達門檻的結論持續有效。

R71 先取得 12 份穩定檔／227＋91 輪，R72 擴充生命週期檔名後取得 20 份／
227＋97 輪，但第一例曲線超過原每檔 2 MiB 上限，該 unavailable 觀測原樣
保留。R73 在新 cohort 將每檔上限提高至 8 MiB、總量仍限制 8 MiB，取得
兩份完整曲線（4,118,317／1,832,964 bytes），沒有放寬 generation／SHA 檢查。
這些是分別穩定的檔案，**不是跨檔原子快照**。

R74 在 R69、R70 實際終結後，沿未修改的 canonical `collect_training`
匯入其中 **14 份原生時計**。新程序驗證器錯將 `EPOCH_PHASES` set 的跨程序
遍歷順序當成固定，逐列 list equality 失敗；原 failed owner、原輸出與失敗
log 保留。R79 在另一新程序重讀這批原輸出，不重寫或重新收集，對完整 JSON
列及其重複筆數逐筆核對 canonical／untouched original parser。

**6,655 samples 全部原值、locator、context 及重複筆數一致**；兩套 reader
各通過 **六份輸出、20 份 unique originals、175 groups、2,399 個既知身分**。
兩套 parser 的 wire list order 均與原檔不同，卻有完全相同的 row multiset，
直接驗證失敗來自非契約性的集合順序，沒有忽略數值差異。R79 為
**8.071277 秒 wall／2.551822 秒 process CPU**，已 succeeded／inactive／PID 0。
沒有增加 source-endpoint binding 或回填舊時計。

R75／R77 因前序失敗而未執行，R76 也因未取得有效 export 收據而失敗，皆
保留。新 R80 已從兩份完整 curve 匯出 **71,504 個原生 timing 值、872 個
case／regime／field／unit groups**，14.683157 秒 wall／3.969849 秒 process CPU。
R81 已在新程序獨立核對 **71,504 個值、原始單位、來源 SHA、浮點 sign bits**，
以及全部 **872 個唯一 summary groups** 的 count／min／median／max／零值與
負值數，**8.387229 秒 wall／2.685834 秒 process CPU**。本批沒有負零值，
仍保留逐值浮點 bit 比對；沒有把非同步 host spans 或零 CUDA counters
改稱純 GPU 時間。R80／R81 均 succeeded／inactive／MainPID 0。

R82 隨後實際完成寫出時 SHA 候選的 **16 tests，0 failure／error／skip**，
3.989434 秒 wall／2.842020 秒 descendant CPU。該候選的全量速度比較另由
R78 執行，這些語意測試不構成速度採用。動態 epoch-plot 檔名仍待接入
canonical discovery，整合後也仍須驗收全部 13 階段。

- [R73 原始擷取、來源 SHA 與逐檔 generation](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round73/observation.json)
- [21 份原始檔／712 列及舊來源精確前綴讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round73/readback.json)
- [擷取與獨立讀回完整計時](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round73/completion.json)
- [R74 canonical 匯入與原版比較的排程 protocol](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round74/protocol.json)
- [R75 原始單位與非同步範圍的匯出 protocol](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round75/protocol.json)
- [R76 數值／單位獨立讀回的排程 protocol](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round76/protocol.json)
- [R79 6,655 列原值／重複筆數及雙 reader 驗收](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round79/readback.json)
- [R79 原輸出重用、失敗保留與完整計時](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round79/completion.json)
- [R80 全部 71,504 個原生值及匯出時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round80/completion.json)
- [R81 獨立數值讀回 protocol](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round81/protocol.json)
- [R81 所有數值、原始單位與唯一 groups 的獨立驗收](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round81/completion.json)

## R91 03:29 UTC：小型完成 metadata 與末端成本

2026-10-24 03:29:30 UTC 沿同一已登錄 SSH profile、嚴格 host-key 驗證及原
v3 request/source 身分，唯讀擷取 **33 份穩定 JSON、2,031,144 bytes**。
完整傳輸子命令為 **1.339331 秒 wall／0.097149 秒 descendant CPU**；新程序
獨立讀回為 **7.294696 秒／0.962643 秒**。保留原始 bytes、SHA、單檔
generation、126 個原生數值欄位及浮點 hex。canonical child 整體為
**17.567855 秒 wall／2.436938 秒 descendant CPU**，另含 guard、封存及控制
成本，未只以 SSH 或 reader 時間代表整個工作。這些欄位包含計數，尚未全數判定
單位或重疊區間，不能直接相加為耗時。

| 正式 case | 此次保留的原生狀態 | save total | plot total | walk-forward refresh | inclusive post-plot |
| --- | --- | --- | --- | --- | --- |
| 08:45 AdamW | progress complete、fold 12 marker | 0.283028 s | 1.635113 s | 10.338656 s | 12.260505 s |
| 08:45 notional Muon | progress complete、fold 12 marker | 0.297554 s | 1.640768 s | 10.076404 s | 12.018731 s |
| 09:00 AdamW | progress complete、fold 12 marker | 0.342680 s | 1.672023 s | 10.541871 s | 12.560604 s |
| 09:00 notional Muon | training、epoch 86／最大 1000 | 未觀察到 | 未觀察到 | 未觀察到 | 未觀察到 |

`post_plot_total_s` 包含繪圖、refresh 與其他工作，不能再與子項相加。三份
fold marker 均宣告 145 列 test/deployment 範圍 2026-02-26 至 2026-09-24；
本次沒有重讀 NPZ、checkpoint、圖檔及完整 source tree，因此保留的是 owner
完成狀態，**不是新的完整訓練生命週期驗收**。最大 epoch 1000 與既有 early
stopping 設定不變，沒有把 patience 100 當成總 epoch 上限。

同一保留的原 owner `summary.json` 分別回報 case `elapsed_s` 為
**6,828.922150／6,923.049520／3,195.867413 秒**，三例皆 status succeeded、
returncode 0、完成 1／1 fold。本次驗證其 metadata bytes，未重新計時或
重算完整 curve。條件上若唯一改變是移除這一次 final post-plot 區間，
case wall 可減少的比例至多約 **0.18%／0.17%／0.39%**；此推導用來排序
瓶頸，沒有實際刪除報告或宣稱觀測到這個收益。三例的 epoch 數與 optimizer
軌跡不同，其完整耗時亦不能相除為方案加速倍率。

既有每 epoch 圖最後讀入 227／230／101／85 列，單次完整圖表工作分別為
0.982663／0.647701／0.642215／0.626804 秒；最後一例仍在更新。未重讀大型
curve/workflow JSONL，無新增穩態訓練加速比較。原始 JSON 欄位及來源 SHA
留在獨立讀回收據，沒有以最新 marker 回寫舊 R73 快照。

總 metadata 讀取上限 2 MiB、每檔 1 MiB。第四例 600,532-byte run manifest
因剩餘 budget 明確 deferred；缺少的兩份 ablation status/summary 也保留，
沒有把缺檔視為成功或零耗時。觀測時兩卡各使用 31,166／31,766 MiB、busy
98／100%，compute PID 953144／953145；這是當時的遠端訓練負載，不能外推
為本機休市常駐服務負載或硬體計算極限。

R91 canonical owner `workflow-r91-finalization-meta-20261024T032924-b9e68bf7`
已 succeeded／exit 0，實際 systemd inactive／MainPID 0。未修改正式 owner、
程式、設定或資料，未派發新的 GPU/provider 工作。本輪判斷是先處理主流程的
讀取、序列化與同步成本；十餘秒的末端工作仍保留量測，不刪圖或縮減驗證。

- [有界唯讀 probe](../artifacts/benchmarks/workflow_speed_optimization_20261021/round91/native_finalization_probe.py)
- [canonical 擷取與獨立讀回 driver](../artifacts/benchmarks/workflow_speed_optimization_20261021/round91/capture_finalization.py)
- [原始 metadata、逐檔身分及缺口](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round91/observation.json)
- [33 份原始檔與 126 個數值欄位獨立讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round91/readback.json)
- [R91 完成及各段完整計時](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round91/completion.json)

## R99 04:51 UTC：第四例仍在原 owner 訓練

沿用 R91 已封存的 observer 與完整獨立 reader，只將 probe 的 `CASES` 集合
縮至上次未完成的 `clock0900__notional_muon`；其餘 business AST 不變。
原四例與舊快照沒有刪除或重算，本次明列為單例增量觀測。嚴格 host-key 與
既有 profile、單檔 1 MiB／總量 2 MiB／96 檔案上限均保留。

04:51:19 UTC 取得 **7 份穩定 JSON／794,959 bytes／9 個原生數值欄位**，
包括上次因總預算 deferred 的 600,532-byte run manifest。傳輸子命令為
**0.921779 秒 wall／0.057023 秒 descendant CPU**；獨立讀回為
**1.480200 秒／0.411192 秒**。這些是觀測成本，不是訓練速度。
canonical 完整 child 另保留 **5.107082 秒 wall／1.189424 秒 descendant CPU**，
包含 guard、封存與控制成本，不由兩個子階段相加代替。

第四例 progress 仍是 **training、epoch 255／1000**；沒有觀察到該例完成
marker、save／plot finalization 或正式 case 完成收據。per-epoch 圖讀入
254 列，原生 total 為 **0.681291429 秒**，其中讀取 0.034026608、loss plot
0.155029989、timing plot 0.492132270 秒；子項包含於 total，不能重複加總。
缺少的兩份 ablation status／summary 仍明列為缺檔，不轉成零耗時。

同次遠端 GPU 使用 31,186／31,910 MiB，busy 98／97%，compute PID 仍為
953144／953145。這是既有訓練負載，不能當成本機休市常駐負載或計算極限。
本次未改訓練／設定、未停止 owner、未派發新 GPU 工作，也沒有重新驗證
checkpoint、curve payload、財務語意或載入程式；完整生命週期仍未驗收。

R99 owner succeeded／exit 0、實查 inactive／MainPID 0。與本機 R92 非受控
正確性驗收重疊的只有此有界 metadata 觀測，沒有新的本機速度比較。

- [重用 sealed R91、單例範圍與完整 SHA 驗證](../artifacts/benchmarks/workflow_speed_optimization_20261021/round99/capture_remaining_finalization.py)
- [R99 原始 metadata 與 GPU／owner 觀測](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round99/observation.json)
- [R99 原始 bytes、數值與缺口讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round99/readback.json)
- [R99 完成與子命令計時](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round99/completion.json)
- [R99 原 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r99-fourth-tail-20261024T045114-de6f082c/run.json)

## R109 06:17 UTC：第四例進至 epoch 433，仍未進入完成驗收

R108 實際結束後，重用 R99 原 observer／單例 probe 與獨立 reader，沒有
重新派發訓練。06:17:21 UTC 取得 **7 份穩定 JSON／794,960 bytes／9 個
原生數值欄位**；原始 bytes、SHA、float hex 與缺檔證據均保留。
傳輸子命令 **0.898030 秒 wall／0.097300 秒 descendant CPU**，新程序
讀回為 **1.226336 秒／0.387057 秒**。這是有界觀測成本，不能當作訓練耗時。

`clock0900__notional_muon` progress 為 **running、validation、epoch 433／1000**，
完成 fold 仍為 0／1。per-epoch 圖讀入／繪製 432 列，原生 `total_s`
為 **0.692213811**，其中 `load_s` 0.062450215、`sample_s` 0.000230610、
`loss_plot_s` 0.150115799、`timing_plot_s` 0.479410077；這些子項包含於
total，不能相加後再計入 total。數值欄位的單位與時鐘重疊語意仍未獨立驗證，
不拿單次 per-epoch receipt 推算全部 1000 輪成本。

兩張遠端 GPU 此時使用 **31,022／31,260 MiB**，busy 均為 **100%**，
compute PID 仍是 953144／953145。這是持續執行的既有訓練，不能將 VRAM
當成休市閒置占用而回收。兩份 ablation status／summary 仍缺少；沒有新 case
完成收據，也未重讀大型 checkpoint／curve、驗證載入程式或財務生命週期。

R109 succeeded／exit 0，實查 inactive／MainPID 0。保留 formal lifecycle
acceptance、跨檔原子快照、載入身分驗證與新加速判定均為 false；下一次只在
原 owner 出現新的完成證據時驗收末端，不以重複觀測代替完整訓練結果。

- [R109 原始 metadata 與 GPU 觀測](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round109/observation.json)
- [R109 原始 bytes 與數值獨立讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round109/readback.json)
- [R109 有界觀測驗收與子命令時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round109/completion.json)
- [R109 原 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r109-fourth-tail-after-20261024T055759-b687fb06/run.json)

## R124 08:55 UTC：第四例已有完成 metadata，接續核對完整時鐘

重用 R99 原封存 probe；其 business AST、嚴格 host-key、每檔 1 MiB／總量
2 MiB／96 檔上限皆不變。R109 epoch 433 快照及原失敗／缺檔證據仍保留。
本次取得 **13 份穩定原檔、1,024,493 bytes、39 個原生數值欄位**，由新程序
重新核對完整 bytes、SHA 與數值；傳輸為 **1.007602 秒 wall／0.050811 秒
descendant CPU**，獨立讀回為 **2.091360 秒／0.504758 秒**。

原 queue 現為 `formal_canonical_queue_exited`。第四例
`clock0900__notional_muon` 的 progress 為 `state=complete, phase=complete`，
fold 12 marker 為 `status=complete`。原 owner 的 summary 現列四例皆
`succeeded`、returncode 0、1／1 fold；第四例完整 case `elapsed_s` 為
**19,314.398880 秒**。它與其餘三例的 epoch 數、optimizer 軌跡不同，不能
用 case 耗時相除作加速比較。

第四例原生 save total **0.304120 秒**、plot total **1.641052 秒**、
walk-forward refresh **10.204276 秒**、inclusive post-plot **12.153419 秒**；
子項包含於 post-plot，不能重複相加。fold marker 宣告 test/deployment
皆為 145 列、2026-02-26 至 2026-09-24。保留的目錄清單也顯示第四例
`epoch_curve.jsonl` 已有 **11,935,052 bytes**，超出原 metadata 單檔上限；
需另立明確預算，完整擷取後才能重算全部 epoch 與階段時間。

此次仍未讀取完整曲線、checkpoint、backtest 容器及圖檔，
`formal_lifecycle_acceptance=false`、loaded-source identity 與跨檔原子快照
驗證亦為 false。兩卡仍各占用 **31,702／31,738 MiB**、busy 均 98%；
queue 結束不能證明 GPU 閒置，也沒有停止其他工作或回收 VRAM。

R124 owner succeeded／exit 0，實查 inactive／MainPID 0。此輪是有界唯讀
觀測，未新增訓練、provider 請求或加速結論。

- [R124 保留原 probe、R109 舊證據的 driver](../artifacts/benchmarks/workflow_speed_optimization_20261021/round124/capture_remaining_formal_case.py)
- [R124 原始 metadata 與目錄尺寸](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round124/observation.json)
- [R124 13 份完整 bytes 與原生數值讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round124/readback.json)
- [R124 完成與觀測成本](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round124/completion.json)

## R128／R131：四例完整 1,216 個 epoch 的原始時鐘驗收

R125 的 SSH 參數順序錯誤在連線前失敗；R128 以新 wrapper 修正順序，
沿用 R73 stable-FD observer、精確 source request 與嚴格 host-key，明確
預算為每檔 **16 MiB／總量 32 MiB／96 檔**。取得 **60 份穩定原檔、
25,167,891 bytes** 後，舊 reader 錯把可更新的 progress JSON 套用 JSONL
append-prefix 規則而失敗。完整擷取與失敗證據皆保留。

R131 未再次連線，重用 R128 原 bytes，以新程序核對 stable-FD、所有 SHA、
base64 原文、完整 JSON／JSONL、原封存檔、R73 的全部 JSONL prefix、
R124 完成 summary、source request、fold 12、epoch 連號及兩 rank。
四例 **227／230／101／658 個 epoch，合計 1,216**，皆與原 owner curve
和 median 精確吻合。去除 epoch 1／2 後有 **1,208 個 steady epochs**。

| case | 全部／steady epoch | 完整 epoch MAX-rank median 秒 | train MAX-rank median 秒 | 原 owner 完整 case 秒 |
| --- | ---: | ---: | ---: | ---: |
| 08:45 AdamW | 227／225 | 28.779305 | 27.037601 | 6,828.922150 |
| 08:45 notional Muon | 230／228 | 28.891974 | 27.119896 | 6,923.049520 |
| 09:00 AdamW | 101／99 | 28.830932 | 27.126178 | 3,195.867413 |
| 09:00 notional Muon | 658／656 | 28.884117 | 27.098995 | 19,314.398880 |

四例設定／軌跡／epoch 數不同，表格用來定位成本，不能相除作加速比較。
原生 val median 約 0.361–0.363 秒、test curve 0.734–0.735 秒、checkpoint
0.184–0.196 秒；parallel evaluation 與其他區間有重疊，不能相加。
1,208 個完整 epoch 累計 34,903.892612 秒，train MAX-rank 累計
32,764.641054 秒，約占 **93.8710%**。在 **train 保持不變** 的條件下，
即使其餘 epoch 開銷理想地歸零，wall 最多減少 **6.1290%**，throughput
上界為 **1.065291 倍**；這是本批 steady epoch 的條件界限，並非 GPU
算力極限。train 仍包含資料取得／準備、forward、loss、backward、step
及同步，需要繼續分析其原生分項。

R131 succeeded／exit 0、實查 inactive／MainPID 0，讀回成本為
40.485732 秒 wall／6.021428 秒 descendant CPU。完整時鐘收據 SHA 為
`5f2040f4c4780dace60b66b6061d04b7721f441e4bd992b52e225c2de5ae4c5b`，
completion 為 `fae3f1c318e0efbf2939f94be4a281d660f2a6310439c36a9fca2247f051ce8f`。
R125／R128 已由此後繼驗收標為 superseded，原失敗與完整原檔均保留。
沒有新訓練、checkpoint 反序列化、財務結果驗收或新加速宣稱。

- [R125 完整原生時鐘擷取與獨立 reader](../artifacts/benchmarks/workflow_speed_optimization_20261021/round125/capture_completed_formal_clocks.py)
- [R125 有界唯讀 probe](../artifacts/benchmarks/workflow_speed_optimization_20261021/round125/native_completed_clock_probe.py)
- [R125 原 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r125-completed-formal-c-20261024T090756-a16b545c/run.json)
- [R128 原始完整擷取](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round128/observation.json)
- [R131 原檔與全部 JSONL prefix 的獨立讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round131/complete_clock_readback.json)
- [R131 最終時鐘驗收](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round131/completion.json)

## R130：canonical 結構驗收與完整 checkpoint 語意分開

R127 在 SSH 前因缺少合格完整時鐘而拒絕。R130 等 R131 實際成功後，
保留同一個唯讀 probe，在遠端執行現有
`validate_completed_training_artifacts`，檢查四例 fold 12。執行前四份
checker 原始碼逐份 SHA 與本地相同，擷取原 bytes 後獨立讀回；沒有修改
遠端 source、反序列化 checkpoint、配置 CUDA 或重啟任何 owner。

四例 canonical structural gate 皆通過，每例 18、合計 **72 份必要 artifact**
皆有前後 stat fence，宣告大小合計 **376,935,134 bytes**。包含 root
manifest／progress／summary、最後與最佳 checkpoint、完整 epoch curve、
test／deployment backtest、mode contract、完成 marker、三張圖、年度報告
和 plot timing。這輪只取得七份小型原檔 **82,852 bytes**，沒有對全部
大型 artifact 做 SHA／載入／冷端重建，因此只驗收 canonical **結構**。

原生 gate wall 分別為 0.064705／0.053528／0.023613／0.156399 秒；
第一例 process CPU 為 3.344791 秒，其餘約等於 wall。此處未歸因第一例
短區間的 CPU 差異，不能將其比值當成可重現吞吐量。
擷取 8.386483 秒 wall，獨立讀回 13.877576 秒 wall／2.023081 秒
descendant CPU。R130 succeeded／exit 0，實查 inactive／MainPID 0；
R127 以此後繼標為 superseded，原先缺依賴的拒絕證據保留。

`canonical_structural_gate_passed=true`，但 full checkpoint／financial
semantics、cold recovery、原訓練 loaded-source 身分、跨檔原子快照及全部
工作流驗收仍未證明。結構 reader SHA 為
`1efca921af3d918b27aeded844e9d8c3ab0267ad8ceea42a0ec068f78b5507aa`。

- [R130 四例 canonical 結構 gate 與原始 metadata 讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round130/readback.json)
- [R130 擷取／讀回成本與完成邊界](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round130/completion.json)
- [R130 沿用原 gate 的 driver](../artifacts/benchmarks/workflow_speed_optimization_20261021/round130/observe_reconciled_completed_artifacts.py)

## R132：完整原生分項診斷，保留 lifecycle 粒度

重用 R131 已驗收的 60 份／25,167,891 bytes，不重新 SSH、執行 GPU 或
修改 trainer。另列 32 份原生 clock files 的所有 `_s`／`_s_max_rank`、
`_ms_per_batch`／`_ms_per_call` 欄位，共 **267,884 個值、2,124 組統計**。
記錄每組 count、min／max、median、nearest-rank p95、mean、零值及負值數；
保留來源 SHA、原路徑、行號、epoch 1／2 與 steady regime。每批與每次呼叫
的毫秒不混成完整工作 wall，負的 residual 也不偷偷裁成零。

| reported-rank host 分項，steady median 毫秒／批 | 08:45 AdamW | 08:45 Muon | 09:00 AdamW | 09:00 Muon |
| --- | ---: | ---: | ---: | ---: |
| transfer | 10.174 | 10.313 | 10.242 | 10.240 |
| inclusive forward | 568.343 | 570.731 | 569.397 | 568.009 |
| model forward | 20.948 | 21.648 | 21.089 | 21.441 |
| loss | 11.922 | 13.243 | 12.689 | 13.235 |
| inclusive backward | 595.986 | 596.398 | 598.738 | 599.230 |
| autograd | 446.612 | 447.951 | 447.842 | 450.015 |
| finite check | 607.244 | 606.516 | 605.367 | 603.650 |
| optimizer step | 0.074 | 0.620 | 0.076 | 0.629 |

每列原始曲線皆報 23 batches、一次 optimizer update、`timing_synchronized=0`。
分項互含／可能等待先前 GPU 工作，finite check 可大於某個包含其他工作
的單一分項，不能相加或視為純運算成本；零 CUDA-event 值也不代表沒有
裝置工作。此輪沒有驗證原訓練實際 imported-code coverage，未以現行本地
trainer 代替歷史載入程式身分。

診斷 7.648629 秒 wall／2.037880 秒 process CPU，新程序重讀
6.206374 秒 wall／2.058102 秒 descendant CPU；完整 epoch totals／medians
再次與 R131 精確相同。owner succeeded／exit 0、實查 inactive／MainPID 0。
沒有擴張 canonical 每 epoch 的 lifecycle samples 或來源綁定；這是明確
分開的診斷視圖，避免數百個重疊分項被當成獨立工作流。

- [R132 全部原生分項與完整 epoch 條件界限](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round132/native_phase_diagnostics.json)
- [R132 新程序原始 bytes 核對](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round132/readback.json)
- [R132 診斷與讀回成本](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round132/completion.json)

## R133：既有 finite guard 比較未達門檻，保留負結果

依原 request 找到既有 owner 的 `guard-acceptance.json`、retained evidence、
engineering acceptance、runtime launch 及兩 rank GPU oracle 收據。本次
僅用原 stable-FD observer 與嚴格 host-key 取得 **10 份穩定原檔、
891,120 bytes**；沒有新 GPU trial 或正式參數修改。新程序核對全部
bytes／SHA，再用原四個 case 與 queue elapsed 重算同一份採用式。

原 A1／B1／B2／A2 的 owner 工程檢查皆 accepted，但完整 steady
MAX-rank 均值只由 **27.974207 → 27.879348 秒**，減少 **0.3391%**。
完整四 epoch case 平均 **394.454980 → 393.955168 秒**，減少 **0.1267%**；
原 owner 的 100-epoch 推估 **3,079.978806 → 3,070.372552 秒**，減少
0.3119%，推估不是實際 100-epoch 計時。兩項必須至少快 3% 的採用條件
未成立，精確重算 `selected=false`；不能因機制檢查通過而採用。

這份負結果支持繼續量測其他 train 瓶頸，不能將約 600 毫秒 host finite
欄位當作可全部消除的算力。保留四例原時鐘／execution SHA 與未選用判定，
沒有重跑已結束的比較，也未對已回收的測試 tensor 重新驗證 GPU 語意。
原測試的 compact evidence 是此輪 bytes 來源，不宣稱完整原 tensor 復原。

擷取 0.710233 秒 wall／0.073862 秒 descendant CPU；讀回
3.322302／0.665974 秒。owner succeeded／exit 0、實查 inactive／MainPID 0。
policy readback SHA 為 `f0933016d59da40e9969131682b7ac5e8c8f19896a92af0587ed283ed23bbb03`。

- [R133 十份原始政策／比較 metadata](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round133/observation.json)
- [R133 原採用式與四例讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round133/guard_policy_readback.json)
- [R133 完成邊界與觀測成本](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round133/completion.json)
