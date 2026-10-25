# 工作流測速第二十三輪：原生來源日誌接入與封存等待定位

候選 collector 已能從完整 journald 匯出辨識台灣公開資料的原生呼叫時鐘，
沿用既有 `collect_source_operation`、來源帳本及原版讀回流程。500 項測試通過，
獨立讀回也確認真實日曆操作 **0.508543692 秒**，沒有重新發出來源請求。
這是封存輸入的接入驗證；canonical collector 尚未切換，全域覆蓋數尚未刷新。

## 接入與來源身分

新增候選參數 `--tw-public-operation-journal PATH`，逐檔凍結完整 journal bytes。
解析可處理進度列前綴及 MESSAGE byte array，保留原始證據、呼叫單調起點、
PID/thread、boot/invocation、完整 DatasetSpec 指紋與原生狀態。
只有目前 registry 身分、原生 ABI、時鐘邊界及 producer PID 相符才綁定來源。
不同匯出中的相同呼叫去重；同一呼叫內容衝突則保留耗時、撤下來源綁定並列出問題。
無效時鐘仍列為缺口，不轉成零秒。原生失敗狀態不轉為採集成功。

候選由八段明列的來源修改建立；反轉八段可還原編輯前完整 bytes，包括既有 dirty
work。前輪仍在封存及比較，受保護的八份 canonical 來源保持原 SHA。
候選使用額外 artifact root，沒有改動舊批次證據或正式服務。

## 實測與讀回

完整的 500 項測試包含既有 collector、共用時鐘、economic/OpenBB 回歸及 42 項
新接入案例。成功批次的完整 pytest 子命令為 **38.699819 秒**。
第一次有一項既有 cooldown 測試失敗：50 ms 期限在 SQLite 初始化之前設定，
測量起點卻在初始化之後，初始化成本吃掉了等待區間。已把期限移至同一測量起點，
保留原 50 ms 期限及至少 40 ms 斷言；OpenBB 生產來源沒有變更。
原失敗腳本、來源版本、JUnit、日誌及 run 收據均保留。

封存真實日誌的 delta 寫入與讀回保留 **2,399 個來源 ID**，只加入 **一筆**原生
操作樣本並綁定到既有 holiday schedule ID。原有歷史耗時、即時健康與 active 狀態
均未帶入此 delta；810 個歷史總行數未冒充本次完成工作量，吞吐分母保持未知。

delta 主體含來源清冊、journal 接入、輸出及原版完整讀回，wall **4.125521 秒**、
process CPU **1.073021 秒**；不含此前 imports、guard 與 admission。
它只有四份 root 輸出及五份原始證據，不能與前輪十三份輸出的全量 collector 比速。
另一個獨立程序直接重解原始 journal，逐項核對 PID/invocation、單調時鐘值及
DatasetSpec SHA，再用未修改的原版 reader 驗證所有輸出、summary 及壓縮證據。
第一次 oracle 漏寫共用 locator 的 `/operation_timing/seconds` 後綴；只修正預期
定位字串後通過，原失敗版本保留，候選及來源資料未變。

## 封存目前耗時的位置

17:45 UTC 對同一個既有封存程序做 10.104 秒的被動觀測，200 次時間取樣中，
167 次程序處於 D state，168 次 wait channel 為 `p9_client_rpc`；程序在此期間
讀取 2,399,552 bytes。這支持目前正在等待 D 掛載的 9p 檔案操作，而非單純
JSON 計算耗時。這些是時間駐留樣本，並非 syscall 呼叫總數或全封存階段分解。
沒有追蹤、發 signal、重啟或修改目標，也沒有據此宣稱到達磁碟或計算極限。

## 證據與下一步

- [admission、來源與固定輸入位置](../artifacts/benchmarks/workflow_speed_optimization_20261021/round23/location.json)
- [候選與可逆來源修改清單](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round23/candidate_manifest.json)
- [500 項測試、clock origin 修正及來源身分](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round23/checks_v2.json)
- [真實原生日誌 delta 寫入與讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round23/native_delta_acceptance.json)
- [原始 journal 與原版 reader 的獨立核對](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round23/native_delta_readback.json)
- [封存程序被動觀測](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round23/seal_proc_observation.json)
- [前輪原生時鐘與自然排程觀測](workflow_speed_optimization_2026-10-24_native_tw_public_clocks.md)
- [串流讀回候選、固定全量輸入與雙半比較門檻](workflow_speed_optimization_2026-10-24_streaming_readback.md)

既有 continuation 等待原封存完成後，會執行全量十三輸出的 A/B/B/A、B/A/A/B
比較及額外原版讀回。兩個半段均須通過 wall/CPU 門檻，才有串流 reader 採用依據。
原先規劃待其結束才合入日誌入口；[第二十四輪](workflow_speed_optimization_2026-10-24_canonical_journal.md)
已證明 archive execution 隔離、保留原比較條件，完成 canonical 接入與 696 項
回歸，並開始完整 collector 及來源覆蓋讀回。尚未宣稱所有來源已有實測、完整
工作流加速或達到硬體極限。
