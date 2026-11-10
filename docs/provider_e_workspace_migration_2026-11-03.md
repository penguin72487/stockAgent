# 期貨補查所需 provider 狀態暫遷 E

2026-11-03 使用者明確要求「暫時把所有要用的到東西都移到 E 槽再繼續」。
這次授權包含期貨補查的原生佇列、配額／traffic SQL、鎖、收據與來源資料，
是先前 C 留下載控制資料之位置建議的局部例外。程式、Python、交易服務、
PostgreSQL、D 冷庫及 NAS 不因本次操作遷移或重啟。

## 唯一入口與範圍

操作沿原 `futures-full-period-study-20261030` task，以
`scripts/relocate_provider_workspace.py` 執行。原 FinLab／FinMind／TEJ
collector 路徑仍是 `data_finlab`、`data_finmind`、`data_tej`，實際寫入位置為：

```
/srv/stockagent-workspace/downloads/provider-state-temporary-20261103/{finlab,finmind,tej}/data
```

這是原生 E ext4 工作卷，不是 `/mnt/e` 上的 SQLite，也不以 WSL guest 的
virtual free 取代實體 backing admission。每個 provider 的完整清冊、複製進度、
SHA 讀回與容量准入證據在 E 的
`migration/provider-state-temporary-20261103/{provider}/`。
小型 node-local placement／交接收據位於
`/var/lib/stockagent/workspace-volume/`，不進 Git。

FinLab 原有 `intraday/objects/futures-workspace-v1` 續用原 E namespace，
不重複拷貝。FinMind 與本次台灣期貨無關的
`complement/{parquet,receipts,receipt_history}/USStockPriceMinute`
留在原 C 副本，透過固定唯讀 bind 保留可讀性；不能宣稱這些美股原件已遷 E。
其他 provider namespace 與本次需要的台股特徵、期貨、選擇權資料和控制狀態
都納入逐檔清冊。原本 disabled 的一般 FinMind history lanes 保持原狀。

## 切換與驗證

1. 保存原 timer／service 狀態，對相關服務設暫時啟動條件並停在原 owner
   的安全交接處。TEJ interactive helper 使用 PID/start/boot 身分與原 pidfd
   helper 正常 SIGTERM／drain，不終止其 `init.scope`。
2. 持有原 native coordination locks，檢查程序引用，沿共用
   `workspace_relocation` 清冊與有界 workers 複製、完整 SHA、獨立讀回。
   symlink 僅在明確指定時保留 link text；不遍歷其目標。
3. 原 C 名稱只在完整驗證後改為固定 `.data_*.pre-e-20261103` 回復副本，
   原資料不刪除。原路徑改用 E mount alias，逐一 pin namespace、root 及
   proc FD，沿現有 `transport_namespace_handoff.prepared_tree`／`attach_tree`
   交接 detached mount；一般跨 namespace MS_BIND 會回 EINVAL。掛載橋接先在
   authority 載入 stdlib，再進入目標 namespace，因為部分固定服務看不到新程式、
   Python 路徑或 `/proc/self`。保留原 namespace 的唯讀／security mount flags，
   不改服務 code mounts；另一個新程序獨立 stat 回讀每個資料入口。
4. registry 尚未 active、掛載缺失、UUID／copy proof 改變，都在原容量入口
   defer，拒絕回退空 C 目錄。子 bind 以 kernel mount metadata 辨識；同裝置
   bind 的 `Path.is_mount()` 可能為 false，不能據此當作未掛載。
5. 獨立回讀後安裝可恢復的 mount units，恢復原 timers／TEJ service；有限
   補查沿同一 request IDs 取得新的 agent run 收據。冷庫驗收與 source retirement
   是其他流程，這次沒有 source／D／NAS 刪除授權。

## 當次執行邊界

19:27 台北的[固定快照](../artifacts/markets/tw_futures_full_history_v22_official10_20261030_v1/provider-e-migration-progress-20261103-v1.json)
仍為搬移中，沒有整體完成收據。這輪三個原生來源清冊共 5,367,574 files、
299,121,192,775 logical bytes。當時已逐檔複製並讀回約 184.3 GB；FinLab 為
713,624／1,086,965 files，TEJ 817,762／1,617,244，FinMind
266,907／2,663,365。這些是儲存工作量，不代表金融資料覆蓋率或訓練進度。
E guest available 當時為 621,308,993,536 bytes，實體 E free 為
3,689,692,454,912；沒有只拿 Windows C 或只拿 WSL guest free 當成准入。

現在的複製 owner 為
`provider-e-temporary-v3-20261103-20261103T105227-80765d9c`，三個獨立 provider
processes 共用原 capacity reservation，保留其所有 native locks；每個 process
的 file workers 有界。最初的 V4 handoff 在等待原 operation lock 期間，因
發現 TEJ 的來源佇列更新而安全取消；目前接續 handoff 為
`provider-e-handoff-v5-20261103-20261103T124310-6bb4aaf0`，等待原 operation lock
釋放，接手完整 copy proof 後完成跨 namespace alias、掛載開機恢復及原服務復原。
它不會搶走仍在運行的 owner；若前一 run 在舊 namespace 的 code import 邊界退出，
保留其已驗證副本及 cutover journal，續做資料入口交接。

`continuation.json` 已從原 run argv／request IDs 產生，所有 alias 與 proof
通過、解除 fence 後才啟動原 FinLab 有限期貨分鐘補查、FinMind 逐產品 backlog，
以及等待該 backlog 的全市場核對。穩定 run name 加 exact argv readback 避免
交接中斷後啟動第二個相同 worker。原始交易／quota／desktop owner 不另建。

工程驗證為 194 個 focused tests 通過；另在 10 個現存 namespace 的隔離
clone 實測 E 原值可讀、保留子樹唯讀、masked proc 及 frozen code 可交接。
這些 probe 沒有修改原服務 namespace。ABBA 的 2,048 小檔完整流程由
6.102 秒降至 5.502 秒，約 1.109 倍，四輪內容 fingerprint 相同；不將
小清冊結果外推成正式百萬檔搬移或 GPU 訓練倍數。原件 SHA、獨立讀回及
完成資料落盤後才寫 verified state 的條件保留；批次採 Linux syncfs，
不對其他磁碟做全機 sync。SQL pending rows 改用 keyset 小批讀取，避免長
cursor 阻止 WAL checkpoint。測速 temporary payload 已刪除，正式數據保留。

先前失敗保留：初次 packaged CPython 沒有 optional pidfd Python bindings，
已改重用現有 glibc adapter；Linux pause 後原 Windows recovery 曾更新 TEJ
狀態，該未完成 payload／index 已移入 E 的 `replans/` 保留，原件未刪。
原 Windows helper 現在於任何 lock／狀態寫入之前檢查同一 fence 與持久化的
placement gate；重開機丟失 `/run` marker 時，未完成的切換仍不能產生 C 寫入。
切換開始前仍須通過完整 source namespace／generation 驗證，不能只憑上述
focused tests、部分 GB 已複製或程序存活宣稱完成。

更新的[進度快照 v2](../artifacts/markets/tw_futures_full_history_v22_official10_20261030_v1/provider-e-migration-progress-20261103-v2.json)
另列仍在原位置的其他歷史來源與 live service roots；本文件的三個 provider
收據不能當作所有 `data_*` 或整個 checkout 已遷 E。原交易服務的
`data_tw_public` 入口仍屬原 `service_data` owner。

完整 2011+ 訓練仍須通過既有歷史帳務、假日持倉估值、掛牌與成交時鐘、
以及大台一口持續轉倉基準的真實來源准入。本次搬移不放寬這些機制，
也不購買先前未獲授權的付費歷史 tick。

## 2026-11-03 20:42 台北：TEJ 測試交接修復

一次搬移測試錯誤地把 pytest 暫存 fence 寫入實際 systemd runtime drop-in，
造成已暫停的 TEJ history service 和 API timer 短暫啟動。已將 14 個 drop-in
恢復為同一正式 fence、重新載入 systemd，並只停止這兩個原本應暫停的 owner；
修復收據見 [test-fence repair](../artifacts/markets/tw_futures_full_history_v22_official10_20261030_v1/provider-e-test-fence-repair-20261103-v1.json)。
測試現使用可注入的暫存 runtime 目錄；回歸另在隔離 mount namespace 執行。

TEJ 原始 API queue 由 1,250 增至 1,278 筆任務；兩個世代的 request 總數同為
684，4 次受影響 API 執行紀錄均為 0 pages。已沿原生六個 TEJ locks、E 實體與
WSL filesystem 容量預留及同一 writer fence，將整個 `api_trial_v1` 來源子樹
完整 SHA／讀回後交換進尚未准入的 E 副本。舊 E 子樹與舊全量索引保留，
原始資料沒有刪除；[reconciliation receipt](../artifacts/markets/tw_futures_full_history_v22_official10_20261030_v1/provider-e-tej-api-reconciliation-20261103-v1.json)
紀錄交換前後的 queue 數量。這只是子樹修復，V5 仍須重新驗證全部 TEJ 原件、
全部 E 複本及 namespace，完成後才可解除 fence。API 命令現在在初始化 SDK／
資料庫以前先通過同一 placement／容量檢查，並持有原 `.api.lock`；等待鎖時
若路徑 inode 已因切換改變會明確 defer，不能拿著舊鎖寫新路徑。

完整 verified rows 重新核對原本逐一派工作業過多。第一次 2,048 小檔 ABBA
測試中逐檔 future 中位數 2.044 秒、順序版 0.719 秒，是負面結果，不宣稱加速。
改為每 64 檔一批後，對真實 TEJ E 副本的 2,048 檔抽樣，順序／4 workers／
12 workers 中位數分別 0.815／0.610／1.082 秒；這只支持當時取樣的 4-worker
完整 SHA 讀回選擇，不是百萬檔總搬移或訓練效能保證。紀錄見
[retained verification sample](../artifacts/markets/tw_futures_full_history_v22_official10_20261030_v1/provider-e-retained-verify-workload-20261103-v1.json)。
這一輪背景搬移與正式來源補查的狀態仍以各 owner 的當前收據為準。
