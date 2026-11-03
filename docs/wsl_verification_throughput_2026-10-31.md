# WSL 原值驗證吞吐與回收：2026-10-31

本次沿原 Penguin 儲存 owner、固定 D 版本與原 NAS 回執操作。沒有刪除
來源原值、冷庫物件、帳本、現行訓練輸入或備份。`factorized` 產生器維持唯讀。

## 11:21 的固定進度觀測

10 月 31 日上午的固定來源計畫共 61 項，其中 54 項已有完整 D 原值重建
證明；一項正在重建，其餘六項保留個別重試與錯誤證據。這不是全部歷史的
完整率，也不是 NAS 語義還原或來源可刪除的證明。

- FinMind 固定版本的 **11,849,443 檔／131,540,060,545 原值 bytes**，
  已在 06:01:34 通過完整重建，收據的 `independently_reconstructed_files`
  與來源檔數相同。這一固定驗證耗時 21,111.48 秒，不再列成仍待核對。
- 正在處理的是另一份 `local-tw-index-derivatives-ticks`：**2,912,360 檔**、
  **84,260,036,846 原值 bytes**，封存為 128 個物件，共 85,358,986,245 bytes。
  11:21 讀到的進度是 **1,102,921 檔（37.9%）**、31,809,041,623 原值
  bytes，屬進行中計數，並非可回收 ACK。
- 六項待處理中，CFTC 舊歷史與 OpenBB 是中斷後重試；期貨介面等待其原
  依賴完成；兩個 index-futures 來源均涉及指向 `/dev/shm` 的絕對連結；
  台股服務來源則曾在封存期間更新檔案。保留原始錯誤，不以排除資料或
  放寬符號連結規則假裝完成。

固定計畫：`477b7b54bf8730904be1973961e18f0389dcf05c3a259e75e92fcb75d6cb175f`。
本次逐檔重建的 manifest SHA：
`03f0ab43cd9630e101fc4175b95144e7fe9393523b88650be44bd9d4f70ef711`。

## 問題與實作

原 worker 的 stack 顯示它在逐筆序列化清冊並寫入 SQLite，沒有死鎖。
在這個階段沒有更新進度，容易把前次 attempt 的進度誤認為停住。

1. `MetadataRows.extend` 在 spill 後以同一 cursor 的 `executemany` 消費串流，
   保持重複鍵或 generator 失敗時已成功插入的前綴及精確計數；不建立
   另一份整批 Python list。
2. 固定 D 還原可依當前 `MemAvailable` 與可見 ancestor cgroup headroom
   保留清冊於 RAM，避免 SQLite JSON 往返。以清冊檔數及壓縮大小保守估算，
   預留一個 pack 的驗證空間；處理中重新觀測，餘裕不足時退回原 spill 路徑。
   一般發布者保留原預設，不新增 CPU、I/O 或服務記憶體占用上限。
3. 加入 `load-packed-inventory`、`validate-packed-inventory` 階段計數。
   階段切換與完成端點立即寫入，中途更新才依原五秒取樣；進度一律
   `acceptance: false`，不能當作原值驗收或刪除資格。
4. 對完整核對的一個約 1.9 GB pack 實測後，原生記憶體讀取可涵蓋至
   2 GiB；仍只持有一個 pack，且需要其四倍加 512 MiB 的實際餘裕。
   空間未知或不足時保留原 SHA／SSD／串流 fallback，不降低 reserve。
   SHA、ZIP CRC、每個原值 SHA／size 及來源前後 generation 檢查全部保留。
5. `.vscode/settings.json` 補上新 `service_data/` 的 watcher、搜尋及
   TypeScript 掃描排除。原本 IDE 曾重新掃描大量服務資料；後續五秒樣本的
   file-watcher CPU 為 0.4%，僅代表該次取樣，不當作受控的前後倍數測試。

## 實測範圍

| 實測 | 原路徑平均 | 候選平均 | 結果與範圍 |
| --- | ---: | ---: | --- |
| 10 萬筆完整合成清冊 ABBA | spill 9.57 秒 | RAM 5.01 秒 | 1.91 倍；全清冊 hash、計數及物件成員相同；不是 payload 還原 |
| 一個固定 1,912,555,133-byte pack ABBA | 原生 SSD 46.24 秒 | 原生 RAM 31.82 秒 | 1.45 倍；65,901 個成員全數 CRC／size／SHA parity；不是整個 release |

清冊實測的 spill 暫存索引為 48,824,320 bytes，RAM 模式未建立該 SQLite。
兩項倍數不可相乘，也不代表已達硬體極限；實測沒有清除作業系統 cache，
同機其他服務仍在運行。大型 pack 的記憶體模式減少 SSD 寫入，仍需從
D 完整讀取並驗證每個位元組。

證據保存在 `artifacts/storage_layout/inventory-abba-20261031-final.json`、
`native-large-pack-abba-20261031.json`。後者的私人實測腳本不納入 Git；
可重用的清冊實測入口為 `scripts/benchmark_packed_inventory.py`。

## 本次真正釋放的空間

原有 `metadata-scratch-plan` 對明確的
`/var/tmp/stockagent-packed-metadata-mqco9xsw` 完成 dry run：SQLite 的
已提交部分只有 8,192 bytes、零筆資料，其他是前次中斷留下的未提交頁。
完整檔案 SHA、schema、quick-check、私有權限、hardlink 及全程序引用均通過。
沿原 apply 的重新驗證、quarantine、unlink 與目錄同步，實際回收
**3,042,873,344 allocated bytes（2.83 GiB）**；原值與冷庫物件刪除數皆為零。

原 owner 正使用的 `stockagent-packed-metadata-xzzvfii9` 未刪除。
WSL 內釋放 blocks 與 Windows C 可用量不是等值關係，並行寫入也會改變淨空間。
回執是 `artifacts/storage_layout/empty-scratch-mqco9xsw-20261031.receipt.json`。

## 驗收與部署界線

核心儲存／遷移相關五個測試檔為 **790 passed**。擴充大型 pack 路徑後，
容量拒絕、原生讀取失敗、資料改變、fallback 及 metadata 的三檔回歸為
**129 passed**；兩次範圍有重疊，不加總為不重複測試數。三條持久 Temporal
工作流程的完整當前 run 歷史已重播，未執行 activity、重設 workflow 或
呼叫來源 API。

原交易已進入逐檔重建，因此交接採原 `rollout_lakehouse_source_policy.py`
的 code-only 自然邊界，不中斷它來重跑清冊。初次唯讀交接等待在新增
大型 pack 實測後明確取消；僅終止己方尚未取得鎖的 `flock` waiter，
原 storage worker、PostgreSQL、Temporal 與資料沒有停止或變更。
正式載入結果以後續 handoff 收據與新 worker 的 code identity 為準。
交接已用既有 agent launcher 的 systemd backend 監督：
`inventory-memory-handoff-20261031T031815-aee1478f`，11:18:21 開始執行。
它沿原三個 owner 邊界等待，部分鎖一律先釋放，不占著其他 owner 來等候；
不停止 PostgreSQL、Temporal 或當沖服務。等待上限一小時；若程式身分
期間改變則拒絕載入，不偷偷採用未驗收版本。
預期完成收據為 `artifacts/storage_layout/inventory-owner-handoff-final-20261031.json`，
這次等待已於 12:18:23 逾時結束，沒有重啟原 worker；該收據未產生，不能
宣稱新版已在正式 worker 生效。後續分工版本沿同一入口另排自然邊界交接，
不覆寫這次失敗的 run。

NAS 的來源 ledger 在 11:13:55 將目前 91,608 個物件、1,644,580,378,551 bytes
列為 archive-covered，待傳 delivery 與 bytes 均為零，沒有 receipt error。
這是該 ledger 的檔案交付範圍，不取代各來源版本的語義還原或來源退休 gate。

尚未對全流程給出固定完成時間。應分別觀測清冊、完整物件讀取／原值重建、
原有 scratch 回收及各來源重試；不能把清冊加速倍數外推成全資料倒數。
11:21 的觀測與空間／owner／NAS ledger 聚合保存在
`artifacts/storage_layout/inventory-live-progress-20261031T032000Z.json`。

## Vast 分攤驗證的實測

使用者提出使用 `114.34.26.236` 分攤驗證後，已沿既有嚴格 host-key SSH
登錄及 SFTP 設定連線、量測，沒有新增 mirror、payload owner 或遠端常駐服務。
WSL 重開機遺失了 `/run` 內的 SSH socket 目錄，已由原
`prepare_agent_ssh_profile.py` 重建；沒有改 key、host-key、遠端服務或帳號。

這次遠端 container 身分是 `57313961`、boot
`910a0cff-52d2-4043-a020-ca8a7fa8f0a4`，不能沿用其他 boot 的驗收。
觀測到 affinity 256 logical CPUs，但 `cpu.max = 6143999 100000`，
實際 CPU 配額約 **61.44 核**；`memory.max` 為 **241.75 GiB**，當時使用約
61 GiB。可見檔案系統可用約 **464 GiB**；`workspace_is_volume: false`，
因此仍是非持久計算節點。沒有更動 provider 配額或啟動 GPU 工作。

| 實測 | 結果 | 可以推論的範圍 |
| --- | --- | --- |
| 4 個各 16 MiB 不可壓縮合成檔，單路 SFTP | 6.82 秒，9.38 MiB/s | 單次小型傳輸，非持續 bulk 速率 |
| 同大小、全新名稱，4 路 SFTP | 3.77 秒，16.98 MiB/s | 每檔遠端完整 SHA 相同；不能直接套用到 TB 資料 |
| 一個固定 210,583,820-byte pack，本機 D 讀取及完整驗證 | 8.35 秒 | 7,080 成員，所有 CRC／size／SHA；含 D 讀取成本 |
| 同一固定 pack，遠端已落地後完整驗證 | 0.62／0.67 秒 | 全物件 SHA 與所有成員身分相同；不是純 CPU 公平倍率 |
| 同一 pack，候選 `128Ki` chunk／128 concurrency 的傳輸及遠端完整驗證 | 26.38 秒，其中傳輸 25.44 秒 | 本次整體慢於本機，沒有採入正式傳輸設定 |

第一個完整 pack 的遠端測試曾因測試腳本產生含 NUL 的 Python 程式而失敗。
修正後從同一精確 probe 接手，重新核對本機與遠端全物件及全部成員，沒有重傳
那份已到達的檔案。原先傳輸時間未持久化，收據明確記為 `null`，不拿它比較。
本次合成檔與固定 pack 的遠端工作副本均在精確集合／完整 SHA 再驗後回收；
原始資料、D 物件、訓練輸入與既有遠端內容刪除數為零。

固定版本的 128 個物件在遠端既有 cold 路徑中，**符合名稱及長度者為零**；
這是 metadata presence 觀測，不是遠端所有資料的完整搜尋或完整性證明。
若重傳其 85,358,986,245 bytes，即使用小型四路樣本的速率推算，光傳輸也
約需 80 分鐘，另有遠端核驗及收據成本；這是條件推算，並非完成 ETA。
目前本機已進入後半段，因此保留原交易繼續，沒有為測試重啟、遷移或重置進度。

判斷是否卸載要比較缺少的固定物件傳輸、完整核驗及收據的總等待時間。
逐 pack 傳輸與計算可以管線化，但仍受兩者較慢的一段限制；增加遠端 CPU
不能消除 D 完整讀取及上傳。適合交給 Vast 的是它已持有的精確版本，以及
後續依固定來源生成的研究／訓練工作集；既有 `use`、demand fence、全 SHA、
READY 與 completed-return 分工繼續適用。這次私人單 pack probe 的結果沒有
接入正式 D 回收驗收，不能把單 pack parity 當成整份來源可刪除的收據。
使用者後續指定的「本機／Vast 分工驗證」已另實作成原 verifier 的 opt-in
pack 佇列，含批次 D 讀取、記憶體 RPC、完整版本彙整與失敗回退；實測與
載入邊界見 [分工驗證](cooperative_packed_verification_2026-10-31.md)。

Vast 的可見剩餘空間也不足以接納整個約 2.66 TiB 的 WSL 已用量。要大量
釋放 WSL，仍須沿原 owner 完成 D 固定版本的完整還原、必要的 NAS 配對
驗收、來源 writer／reader 路徑切換與逐項退休。此次遠端實測本身沒有回收
任何既有 WSL 原值，也沒有把唯一冷正本改放 Vast。

原始小型證據位於：

- `artifacts/storage_layout/verification-offload-9223b863a1cc4d5286a7f8dc40e39b33/acceptance.json`
- `artifacts/storage_layout/fixed-pack-offload-fda5cdf9bd094e7f97496f825b0aef34/acceptance.json`
- `artifacts/storage_layout/vast-fixed-object-presence-20261031.json`

以上是實際 SFTP、全物件 SHA 與逐成員 parity 驗證，沒有使用來源 API 配額，
不是正式跨節點全資料驗收或 GPU runtime 證明。
