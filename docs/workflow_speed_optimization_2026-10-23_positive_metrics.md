# 工作流測速第十九輪：保留來源識別與重用正例解析

接續第十八輪的負例世代候選拒絕結果。原收集器與測試已撤回至當輪起點，
47,138 份證據與 15 個 terminal runs 經獨立讀回封存。第十九輪的來源清冊修正
與預設關閉的正例解析候選已完成工程驗證。後續 Round20 的完整八輪比較與第二次
獨立讀回確認快取候選不接受，已只撤回該快取變更，保留來源清冊修正；
選回後 598 項回歸與完整 13 階段 collector／兩次讀回均通過。

## 來源範圍不能隨目前可見性縮小

重啟後目前快照只有 2,144 個來源。共用收集器新增 `--known-source-inventory`，
讀取並凍結既有 canonical inventory。現在快照的資料優先；缺席來源只補識別、
歷史快照時間與原服務欄位，不匯入舊計時、舊覆蓋或目前健康判斷。補入來源的
`active_scope=null`、`service_keys=[]`，舊值另列 historical 欄位，並明確標記
目前 visibility 與 live readiness 均未驗證。

此邊界有 133 項回歸通過。使用第十八輪全部 232,724 筆原樣本，canonical binder
重新核對後保留 2,399 個來源、1,646 個已有階段綁定、753 個待量測來源；
只為既有 135 個 TEJ 來源的 342 筆樣本補 source ID。無 producer imports 的
獨立讀回重建完整綁定樣本雜湊、核對所有原值與 annotation，數值 summary
逐 byte 相同。這是清冊 owner 的固定輸入整合驗證，不是新一次 13 階段 collector。

## 計算候選與記憶體邊界

固定正例有 18,364 個路徑、3,730 種完整 SHA。候選保留每份檔案的 native 讀取、
stat、完整 SHA、原始預算與 gzip 凍結，只在同一次收集中重用相同 SHA 的
metric extraction。明確指定收據與一般 full-payload API 仍完整解析。

實驗時的 `--benchmark-metric-cache-mib` 預設為 **0**，拒絕後旗標及快取實作均已撤回。
該候選有獨立的 charged bytes、2,048 個
entry、16,384 個 metric rows 上限；只收至少 4 KiB、最多 512 個 metric 的
原始文件，保存不可變 scalar tuples 與有界 observed-time projection，不保存
原 JSON tree。每次回傳新的 metric dict，避免來源裝飾或外部修改污染下一份結果。
parser、walker、native descriptor 或 recursion context 變更會清除快取。

JSON/parser、Python/C++ walker 與新語義測試共 **268 passed，7.93 秒**。
包含大整數、負零、毫秒、敏感欄位、同長度內容變更、原始 evidence 預算、
context 失效、entry/row/byte 上限，以及偽造重用 anchor 時獨立讀回拒絕。
這是正確性證據，不能代替效能接受。

完整輸入、全部六份輸出及獨立 reader 的最終比較見第二十輪。ABBA／BAAB 的
wall B/A 分別為 0.989988／0.896257；前半組未達 0.98，不能用反向較快抵銷。
依事前門檻不接受候選，撤回後 8 份共用來源逐 byte 回到 `registry_source`，
不是回退使用者或先前的其他 dirty work。

## 有界比較與容量處置

先以原 corpus 中 132 個固定路徑、4 份 explicit receipts 執行 A/B/B/A、B/A/A/B。
每次包含 fresh interpreter、所有六份輸出、完整原始凍結與獨立 reader。
八次全部成功、六份輸出語義相同；每次 B 重用 96 份解析、少解析
2,073,774 bytes，native 讀取與完整 SHA 數量不變。

| 半組 | 完整 child wall B/A | 完整後代 CPU B/A |
| --- | ---: | ---: |
| A/B/B/A | 1.065296 | 1.065192 |
| B/A/A/B | 1.063315 | 1.055969 |

這個小工作量的完整命令較慢；不能只取其中一次 inspection 變快宣稱成功，
也不能用此小 corpus 代替原 372,550 檔、107 explicit 的全量比較。預設仍關閉。

ext4 剩餘容量不足完整比較預算。依前輪逐檔 allocation 加 10% 與小檔餘裕，
八次比較及後續完整收集預估新增 7,733,488,852 bytes，另保留 8 GiB；
正式 gate 當時可用 14,724,923,392 bytes，小於所需 16,323,423,444 bytes，
因此沒有啟動任何 ext4 全量 trial。兩份最初 assertion 失敗與後續正式 deferred
收據都保留，沒有刪除資料或降低完整比較範圍。

後續唯讀檢查找到 D 槽約 605 GB 可用。依儲存契約，於 systemd 真正的 PID 1
mount namespace 驗證既有 D volume marker、9p 掛載及 8 KiB 設定，建立獨立的
private benchmark 工作區，與 cold CAS、heads、mutable PG 及既有證據分離。
沿用 `storage_layout.admit_output(..., "benchmark")`，保留至少 **32 GiB**，
每次 fresh worker 與 reader 重查 guard，持有 D directory FD，缺盤不回退 C。

新的輸出位置是：

```text
/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round20
```

132/4 的 D smoke 已通過：五份輸出逐 byte 相同、第六份 manifest 語義相同，
兩版獨立讀回與全部輸入身分均成立。Round20 全量比較維持同一組 ext4 原始輸入，
兩版都寫到相同 D 槽，保留 A/B/B/A、B/A/A/B 及每半組 wall <= 0.98、CPU <= 1.02
的原接受門檻。D 與 ext4 輸出是不同的量測條件，結果不能混合。

## 證據

- [第十八輪獨立封存讀回](../artifacts/benchmarks/workflow_speed_optimization_20261021/round18/optimization_acceptance_readback.json)
- [有界工程 admission](../artifacts/benchmarks/workflow_speed_optimization_20261021/round19/protocol.json)
- [清冊固定輸入獨立讀回](../artifacts/benchmarks/workflow_speed_optimization_20261021/round19/registry_roundtrip_readback.json)
- [候選來源與測試綁定](../artifacts/benchmarks/workflow_speed_optimization_20261021/round19/candidate_source_manifest.json)
- [有界八次完整比較](../artifacts/benchmarks/workflow_speed_optimization_20261021/round19/bounded_probe_comparison.json)
- [ext4 全量容量延後](../artifacts/benchmarks/workflow_speed_optimization_20261021/round19/comparison_deferred.json)
- [D 實際 namespace 與 admission](../artifacts/benchmarks/workflow_speed_optimization_20261021/round19/d_continuation_location.json)
- [D smoke 獨立核對](../artifacts/benchmarks/workflow_speed_optimization_20261021/round19/d_store_smoke_readback.json)
- [完整八輪、區間分割及後續收集](workflow_speed_optimization_2026-10-23_guarded_d.md)
- [完整比較第二次獨立讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round20/comparison_readback.json)
- [選回保留清冊修正的基準](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round20/selected_source.json)
- [598 項選回後回歸](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round20/selected_checks.json)
- [完整 13 階段與來源／摘要獨立核對](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round20/full_collection_scope_readback.json)

這次完整收集有 240,567 筆樣本、2,399 個來源、1,646 個階段綁定，仍有 753 個待補測。
這次 current snapshot 已包含全部已知來源，歷史缺席補入數為 0；前述 255 個缺席識別的
固定輸入 roundtrip 是另一份工程證據，沒有當成這次新增來源。
尚未證明整個 collector 加速、正式服務已載入、所有流程均已測完或已達計算極限。
剩餘來源 E2E、遠端訓練與當前服務 readiness 仍各自保留接受邊界。
