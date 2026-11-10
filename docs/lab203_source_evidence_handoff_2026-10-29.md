# Penguin → lab203：來源證據與背壓交接（2026-10-29）

## 2026-11-05 18:32 接續：確認 repository lock 與刷新原 readiness

這是來源端交接要求，不是 NAS ACK、鎖已失效或接收端已採用的證明。
Penguin 仍讀到 11/2 12:49:56（台北）的原 `readiness.json`，已超過原
900 秒有效期間；新鮮的 capacity／runtime／owner schedule 不延長它。
11/4 19:27:36 的固定 `stockagent.lab203_readiness_and_batch_progress_non_ack.v1`
診斷回報原 repository config preflight exit 11、`unable to create lock`／
`repository is already locked exclusively`，且明確保留其他 repository
clients 可能存在。這是當時診斷，不斷言今天仍為同一持有者。

請 lab203 的原 owner：

1. 核對目前 repository 身分、所有已知 clients、鎖持有者與對應備份／
   restore／prune 活動。保留原鎖與證據；不依本機 PID 不存在就強制 unlock。
2. 在原安全流程取得准入後，重新執行原 readiness 檢查並產生真正新觀測；
   沿既有反向回執通道回傳，不改寫舊時鐘或單純延長 lease。
3. 接續既有封閉 dispatch 的 NAS 獨立讀回與配對 ACK。Penguin 的 lifecycle
   snapshot 1061 尚在等 delivery
   `bd73586c2a4cfaed9b1b6674bf4b731f5334abb7cc6095eac1df820681d8e856`。
   請分開回報該 delivery 與私有備份 cohort 的進度、剩餘工作量、實際讀回
   吞吐量、下一次檢查／開始／預計完成；不可由送達或容量推定驗收。

Penguin 已保留服務、原 source journals 和 NAS 未 ACK 的副本；網站每分鐘
觀測，原候選 worker 仍待安全邊界載入 E metadata scratch 改善。無需為
刷新網頁另起 backup writer、重新封裝已驗收包或更改現有 reserve。

## 2026-10-30 00:55 更新：三個固定控制包已取得 NAS 檔案還原 ACK

以下更新取代本文件較早時間點「等待三包 NAS 驗收」的狀態。Penguin
來源 ledger 已逐一核對原 envelope、dispatch、repository、檔案大小及
回執；對應的 lab203 ACK 均記錄全部檔案 SHA 與獨立檔案還原通過：

| 固定控制包 envelope | ACK identity | 接受時間（台北） |
| --- | --- | --- |
| `1d1fb24558388997640f61fc459584bcf039ec9768e81be720d3cf211f164243` | `d15e2ced456341ca9a3a9cbb46da962c32e0c047a7653872f9fbfea8f772c158` | 00:37:25 |
| `7c5f0ab836251b1f88c24620bcbf3db941ee4598e5de4d1d127c83aa96ad5cca` | `d55bb48658bac02b7a6e1d04f467b75715f15a19342ec6f4cbfad800ed24159f` | 00:37:41 |
| `0f61755e8bc37e3f27f02a63f4300e494fdeba90ed49cefef3c4c5a568368b29` | `ff376de38cc70c167505eb39c0079e436e9572543e8f83d677393b8cbd068c1a` | 00:37:09 |

這些 ACK 的範圍是個別封閉交付的離機檔案備份，不表示完整 canonical
重建、TW runtime gate 或歷史缺件恢復。請 lab203 依已收到的固定控制包
和原 owner 接續下列只讀核對，並以原回執通道回報逐項結果：已固定
snapshot 的可續跑搜尋、25 版本／114 物件及 498 個新聞缺件的 SHA／
長度交集、TW 原 runtime contract 的固定 snapshot 讀回與六項差異。
未讀完的 snapshot 保持未知，不標為不存在。保留既有 pin、retry、原 owner
及實際容量 gate。

## 23:16 進度更新

lab203 的 `lab203-control-paired-nas-evidence-request-v1-20261029T141537Z-6e4aa5e8.json` 回報三個下列固定包已做接收端本機完整 SHA 讀回，但仍缺配對 NAS 派工與獨立 NAS 讀回。Penguin 已實作固定封套的原 owner 派工，並以 `lab203-paired-control-backup-202-20261029T151145-131197a0` 排隊，當時仍在等原 owner；**排隊不是已派工或 ACK**。請 lab203 原 owner 隨後沿原 Syncthing 收到各自 `batch-<envelope identity>` 與 `tools/dispatch/<envelope identity>.json` 後，按原派工、配對 repository 和容量 gate 完成獨立 Restic 備份／還原，並由原回執通道回傳逐包 ACK。請勿把此段或本機讀回視作 NAS ACK。Penguin 實作與測試範圍見 [備份 owner 交棒](backup_owner_fairness_and_control_backup_2026-10-29.md)。

22:53 的 `lab203-donor-target-ack-progress-v1-20261029T145307Z-ba9f5d85.json` 回報 452 個候選中有 160 個已被六個目標封套的嚴格 NAS ACK 覆蓋，另 292 個尚未被這六個 ACK 覆蓋；不是新 donor 讀回，也不能證明其餘不存在。請繼續原已固定 snapshot 的有界讀取，以及 25 版本／114 物件和新聞清冊的精確核對。

22:12 的 `lab203-ingress-recovery-progress-v1-20261029T141233Z-f887b0ba.json` 對原 498 個新聞缺件的更新是：122 個觀測到同長度，376 個當時未見；同長度尚未驗 SHA，更不是 NAS ACK。這是當時觀測而非目前全量驗收。該回執也指出 TW 原 runtime contract 檔案尚未確認接收，六項 runtime 差異及 strict gate 均保留；請依前述已固定來源證據確認原 contract 文件，不能把固定 verifier 的本機 SHA 讀回當成完整 runtime 驗收。

Penguin 23:19 重新以原 `rescue_binding` 核對 source supplement 成員的完整 SHA、來源封套與原配對 NAS file ACK。TW 原 runtime contract 是該既有 supplement 的一員，已有固定 snapshot 可供原 owner **只讀重新還原**，不必等三個新的控制包完成 NAS 派工：

- 原配對封套 `6b4d8362b475779d85dc1c6bf888812aebb056f66a56da57eb9bd813c7e9209c`；原 dispatch identity `96888d017938122d13490716c2d1a09ca39f32a342b3ed38faa9bf83c85a1a7f`；原 ACK identity `95e83d63d8d80f93e04d43e82728d3f183080e2133712b74a4d2ba134b3925dc`；原 snapshot `75d0dea8cd442522853c3af7a4423ad330c22934c5caaf49f47c948ecfa67da4`。
- 封套成員相對路徑 `traditional/private-rescue/965a8cd00fb77f3bc7ad4f0a28a89afd06bb2540a737a4643cbd3745facff2f0/evidence/storage-single-authority-20261025/dcb3f3dba24b25e9546369e3ce2045a7bad74bda01b7a4dfba6a91bc2326a754.json`；2,006 bytes；檔案 SHA-256 `a0cdf1afb17c9eebaa707e10a24ceb6f38c69cf496c27ef2b44902ed1d7d4db6`；內容 canonical identity `dcb3f3dba24b25e9546369e3ce2045a7bad74bda01b7a4dfba6a91bc2326a754`。
- 請先依原 owner 驗證配對 dispatch、ACK、repository 與 snapshot，獨立讀回該固定成員並核對兩種 identity，再依原 verifier 契約處理 TW runtime；保留六項差異及失敗 gate。Penguin 本次重驗來源與既有 ACK，不是新 NAS 讀回，也不代表 runtime 合格。

此記錄是來源端控制資訊，不是接收端 ACK，也不宣稱 NAS 完整備份或訓練續跑通過。2026-10-29 22:00 左右，使用者明確授權在硬體可支撐範圍內提高 in-flight ceiling，以便加速接收；這取代先前「不提高 ceiling」的交接條件。實際調整由 lab203 原 owner 依最新實體後端、WSL 與 NAS 空間及完整在途、還原暫存和並行寫入計算；維持實際容量保護、既有保留量與接收驗收，不能假設資料都已可回收。不要手動刪除來源、NAS 或 ingress 資料、解除 pin、改寫 retry，也不要用搬移、忽略或重新計數規避容量保護。通過逐批配對、獨立 NAS 還原及既有程序／FD／pin 等 gates 的副本，依原 owner 的已驗證回收流程處理。

## 22:00 使用者授權及原 owner 的接續事項

使用者希望接收額度盡量接近硬體實際可安全容納的上限，並以驗收後回收持續騰出空間。請原 owner：

1. 核對 21:56–21:58 曾由 96、136 變為 740 GiB 的 ceiling 是何種設定或動態結果，提供可重現的配置、目前實際 WSL／實體後端／NAS 可用空間，以及各自的保留量、動態 minDiskFree、現有在途與還原／並行寫入所需工作空間。這些容量取樣可能不同時，不能互相替代或相加。
2. 在不降低既有保留量、不改動計數及不破壞原 owner 驗收／回收契約下，以實際剩餘物理空間和仍在佔用的在途資料計算可接收上限；持續檢查可用容量，讓接收、NAS 獨立讀回、驗收與符合條件的回收流水化。先量測瓶頸與進度，完成後以原回執通道回報設定、證據及效果；不得把有預期回收的空間當成已釋放。
3. 收齊並核驗下列三個固定控制包，回報 envelope 身分與本機／NAS 讀回，不把 Penguin 的來源驗證或單純傳輸進度視為 ACK。據原 owner 固定清冊繼續比對 93 批、114 個歷史缺件與 498 個新聞缺件，執行指定 Restic snapshots 的可續跑只讀查找。

## 既有通道上的封閉控制包

以下路徑相對於原本配對的 Syncthing ingress，並非新路徑或新 owner。Penguin 已分別在發布後重新讀回並驗證各封套。lab203 的 14:00 回執指出前兩包均已進入來源索引，但因原 ingress 容量限制尚未接收；第三包於 15:58 發布，通知因既有 folder scan 而延後。本地讀回不代表遠端收到或驗收。

| 路徑 | 固定 envelope identity SHA-256 |
| --- | --- |
| `tools/storage-source-evidence-20261029-v1/` | `1d1fb24558388997640f61fc459584bcf039ec9768e81be720d3cf211f164243` |
| `tools/storage-capsule-binding-20261029-v1/` | `7c5f0ab836251b1f88c24620bcbf3db941ee4598e5de4d1d127c83aa96ad5cca` |
| `tools/storage-retired-scan-20261029-v1/` | `0f61755e8bc37e3f27f02a63f4300e494fdeba90ed49cefef3c4c5a568368b29` |

每包都有 `READY`、`backup-envelope.json`、`handoff-manifest.json` 和一個 JSON 控制資料。收到後請以本機已驗證的 `scripts/verify_backup_delivery.py`、上表固定完整 identity 及封包根路徑核對，並把新的讀回收據寫在封包外；先確認配對 producer、receiver 和 repository 身分。來源端驗證不代替接收端驗證。

## 請 lab203 的原 owner 接續

16:37:42 曾觀測到 lab203 透過 relay 重新連線，16:39 收到新的容量回執，16:41:20 再次斷線；16:51:39 又經 relay 連上。16:52:15 來源查詢為 `connected=true`、`remoteState=valid`，peer 資料夾完成度 72.40%、needBytes 32,743,896,719，但對端 16:52 的接收稽核仍記錄 admission budget 為零，兩者並不代表 NAS 驗收或背壓解除。請查明雙方 Syncthing 反覆中斷與接收端服務狀況；若可在現有網路安全政策下建立直連，量測穩定性與速率，勿改變配對身分。16:51:53 `storage-relay-audit.json` 同步時再次出現 `finishing: pull: no such file`，後來的版本已送達；請檢查此快照頻繁更新與同步的競態，以及可否使用不影響至少每分鐘觀察的節流或穩定版本，同時保持原有收據與完整性語義。

1. 第一個包有 12:52 的來源端 85 批待處理逐筆封套與派工身分，以及先前 11:20 的 83 批候選。後者由當前清單扣除 journal 中兩次較晚發布所重建，總筆數和 bytes 符合，但沒有當時的原子清單，**不能說已找回精確歷史清單**。與 12:00 前接收端的 81 批不同時點，交集及差集僅供查找。
2. 第一個包列出 10 個有嚴格配對 NAS 獨立還原 ACK 的 donor，以及 54 個私人冷庫目標中 1,382 個 SHA、bytes 與封套一致的相同內容候選，合計 5,341,935,007 logical bytes。這些是目標待處理封套所有成員的比對，**不保證它們是實際缺件，不是目標批次 ACK，也不是新一次讀回**。請先和接收端真正缺件清單交集，原 owner 依原契約判斷是否可從固定 donor snapshot 做獨立只讀核驗；不得繞過容量門檻。17 個待處理新聞批次在這批已 ACK 的增量冷庫 donor 中沒有相同候選；不能據此排除其他 NAS 快照存在副本。
3. 第一個包也固定了原 verifier 的兩支程式 SHA、原 source publication 封套、handoff、supplement、已配對 NAS file ACK、response index 與 runtime qualification contract。第二個包把現有 v7 固定清冊連到 TW 原始 commit／checkpoint、訓練設定、原始程式、兩份 prepared references 和 runtime。此為既有清冊的來源身分綁定，並非新 inventory 或 payload 驗收。checkpoint 的 `full_optimizer_state` 來自原始 commit 的語義記錄；RNG、strict runtime 與完整續訓仍未通過。請保留六項原 metadata 差異，依固定原契約進行獨立驗證。
4. 25 個歷史版本、114 個物件的身分仍在第一個包。Penguin 對原登錄冷庫路徑逐項查找與現存來源 ledger 全部檔案描述比對均無相同 SHA/bytes；這不是全域證明。請在原 owner 可用時對既有診斷列出的 live Restic snapshots 執行有界、可續跑的唯讀精確搜尋，分批保留進度並釋放 owner，記錄每個 snapshot 的完成、錯誤與缺件結果；未完成前不要標記不可恢復。
5. 對 30 批新聞保留精確聯集與獨立還原的驗收工作。13 批現有配對 ACK 不等於 30 批齊全；其餘批次不能僅憑接收索引判定完成。
6. 請回傳各包的接收與內容核對結果、候選與實際缺件交集、Restic 搜尋進度，以及在目前限制下的非破壞性處理方案。若接收空間仍不足，先對本機已有且可配對的派工及 NAS snapshot 做唯讀比對，不需等待補件；維持原有 admission 等待並回報。不要將上限／保留空間之外的其他目錄當成接收空間來繞過限制。

## Penguin 本地證據

- lab203 的 `lab203-news-missing-catalog-v1-8333d85af8e79131c1a5f06745529817f2d55ce432981c63a5b6a140cc776b3a.json` 已同步至 Penguin。其 canonical identity 是 `8333d85af8e79131c1a5f06745529817f2d55ce432981c63a5b6a140cc776b3a`，檔案 bytes 的 SHA-256 則是 `cef150994c047078ee1ad45225ef255c1414e9bc7779a9853f7f2768efd769b4`；兩者是不同的核對方式。固定清冊共 9 個封套、498 個檔案描述、1,391,545,896 bytes，接收端 15:32 的狀態均為 absent。Penguin 對 9 個來源封套的 READY、固定 envelope 及配對派工核對後，498 筆 SHA／長度描述均屬於對應封套；這一步沒有重新計算來源 payload 的完整 SHA。
- 15:37 以固定來源 ledger 的 1,839 筆配對 ACK、40,866 筆成員描述，直接與上述 498 筆做 SHA／長度預篩，交集為 0；另一份固定來源 evidence 的 114 個歷史缺件與 498 筆也沒有 SHA／長度交集。固定預篩回執為 `artifacts/operations/storage-single-authority-20261025/lab203-news-missing-498-ledger-prefilter-20261029T073743Z.private.json`，identity `780bafa752a4365b5adc152ef15028ab36464eae4309efaa500d57d49960ee3f`。這不是 ACK，也不是 NAS 不存在的證明；仍需 lab203 原 owner 對尚未完成的固定 Restic snapshots 繼續有界查找及實際讀回。
- 15:55 唯讀核對固定來源 ledger 發現 93 批冷傳輸在原 owner journal 已記為 `retired`、配對 NAS 檔案 ACK 均通過檢查，來源 batch 與退休暫存名稱均已不存在；合計派工 logical bytes 為 766,181,259，而 `deletion_scan_requested` 尚未記錄。固定診斷 `artifacts/operations/storage-single-authority-20261025/source-retired-unscanned-20261029.private.json`，identity `d0544bc08575bcf8f368a77ed0d5fef89792dad8e04ba30515a7b856e02786a5`，只含批次／派工／ACK／快照等身分，不含 payload 或原始檔案路徑。現行來源程式要先看到整個 paired folder idle、無錯及 peer 100% 才會處理這個通知；15:53 觀測為 scanning、peer 72.43%、needBytes 32,698,854,484，上一輪 cache 標示 deferred。journal flag 未更新不代表 Syncthing 實際尚未掃描，也不代表 receiver 尚有這 93 批。請 lab203 原 owner 依固定清冊逐批比對接收端當前名稱及索引、原 ACK／快照與 pin 狀態，回報交集和是否已有刪除事件；可回收與否須依原 owner 的逐批驗收和回收 gates 判定。這是可能的等待循環與可調查候選，不能推算可釋放容量或逕行要求同步刪除。
- 14:30 另對一份固定來源 ledger 做五種已配對 ACK 類型的 SHA-256／長度唯讀預篩：1,839 筆已驗收 delivery、40,866 筆 ledger 成員描述，與 17 個來源端完整封套核對過的待處理新聞批次共 5,962 個不同成員描述比對，沒有相同描述；25 個歷史版本的 114 個不同缺件描述也沒有相同描述。lab203 舊新聞回執的 17 個待處理封套均在上述來源待處理集合內。此範圍包含 code/control、incremental cold、private cold、private news 和 private Vast，不只先前的 incremental cold。固定 ledger SHA、回執 identity、分種類計數及未宣稱事項記在 `artifacts/operations/storage-single-authority-20261025/lab203-all-kind-acked-ledger-donor-prefilter-20261029T063033Z.private.json`。這是來源 ledger 的成員描述搜尋，未重新讀出所有 donor 的 sealed headers 或 NAS payload，也未窮舉 live Restic；結果不能證明 NAS 中不存在，也不是遠端接收或還原 ACK。
- 使用者轉交的 lab203 14:18:49 診斷指出：兩個控制包仍未收到；已保存 498 個新聞缺件描述，與 114 個歷史缺件 SHA／長度沒有交集；9 個固定待查 Restic snapshot 中 3 個逾時、6 個尚未查。這是使用者轉交的診斷，不代替機器回執、完整索引檢查或不可恢復的證明。
- [來源 evidence 產生器](../scripts/build_lab203_source_evidence.py)與[原始 TW capsule 綁定產生器](../scripts/build_lab203_capsule_binding.py)。
- `artifacts/operations/storage-single-authority-20261025/` 下的兩份固定 JSON、發布回執及 Penguin 發布後讀回回執。
- 第 9 輪 WSL 來源退休回執記錄釋放 7,653,269,504 allocated bytes，D 暫存另回收 1,557,876,736 bytes；第 11 輪退休回執記錄 WSL 釋放 13,404,483,584 bytes，16:41 的 `stage-pruned.json` 另記錄 D 暫存回收 2,501,287,936 bytes。其固定冷庫版本 15 個必要檔案，在來源端 Restic 和 immutable archive 已接受收據的聯集中均有對應檔案身分：詳見 `artifacts/operations/storage-single-authority-20261025/round11-combined-nas-file-coverage-20261029T074256Z.private.json`。這是既有 ledger 的檔案覆蓋對帳，不是新的 NAS 讀回或跨封套完整版本重建；原退休回執仍保留 `backup_verified=false`／`single_d_volume`。第 6、10 輪已於 16:42 通過啟動前檢查並平行開始，尚未完成內容及退休驗收；不代表所有 WSL 清理或 NAS 同步完成。細節見 [WSL 續跑](wsl_retirement_renumber_resume_2026-10-29.md)。
- `lab203-user-pinned-controls-pending-v1-20261029T060059Z-54d84bc9.json` 是 14:00 的接收未完成回執，不是 ACK；兩包各有四個成員尚未接收。勿以來源端發布證據推斷接收端已完成。
- 14:15 的 `storage-relay-audit.json` 顯示 admission 為 0：ingress `local_transport_logical_bytes` 85,904,587,016，高於 103,079,215,104 ceiling 扣 17,179,869,184 margin 的 85,899,345,920，超出 5,241,096 bytes；同分鐘的 `storage-status.json` 顯示接收端 WSL available 806,993,584,128，低於同期動態 required 807,453,851,648 bytes。兩份觀測並非原子快照；兩個門檻都需恢復，NAS available 12,410,023,215,104 bytes 不能替代 ingress admission。原 owner 仍回收已驗證的其他還原工作副本，這不是 ingress 已回收或接收完成的證明。

## 2026-10-31 17:23 給 lab203 Codex 的接續事項

以下是 Penguin 的來源端更新，不是新的 NAS ACK；沿既有 owner 與反向
回執通道處理，不新增接收端，也不改寫原 gate。

1. 17:15 的反向 audit 仍有約 592 GiB admission、`receiving_backpressure=false`，
   NAS 約 11.18 TiB free。請持續按實際後端容量、reserve、還原工作區與
   在途計算接收；此刻沒有為解除零 budget 而再提高 ceiling 的必要。
2. 私有備份已移除額外的 512-MiB latency target，保留來源／接收 8-GiB
   batch、512-file、pending、staging、實體容量及 envelope 空間等原限制。
   首兩批各 4 GiB 已封閉發布，請原 owner 正常接收、獨立 NAS 讀回，
   逐批回報原格式配對 ACK，另列讀回 bytes、耗時與尚未收妥的身分：
   `095d79cc48302c47864f8ef0634e2e85a9518974b6c5db81ea33e690cff1ec17`、
   `cbdb9dfd9624375a292981557e9938375025461a2835acf6e5e395e33fbcd83d`。
   來源正在將通知改為每個封閉波次與下一批複製重疊；17:21 單次通知
   因原 folder 尚在 scanning 而 deferred，不能當成資料已接收。
3. 最新公開固定 cohort 已有 103,578／103,584 個檔案在已接受 Restic／
   immutable archive 收據的去重聯集中覆蓋；剩 6 檔、1,942,320 bytes。
   這不是私有或未發布來源全量完成。請更新兩個仍列為 not-delivered 的
   靜態 audit 項目（skill、capsule inventory），依本文既有三包固定
   identity、已存在配對 ACK 與實際部署／runtime 證據判斷；檔案 ACK
   不代表 skill discovery、完整 inventory 關聯或 strict runtime 合格。
   保留 TW 六項 runtime 差異、25／114 歷史缺件與舊重試證據，回報本次
   觀測的 gate 和下一個執行者，勿只重貼歷史 retry 計數。

來源實作與驗證細節見
[10/31 分工驗證紀錄](cooperative_packed_verification_2026-10-31.md)。

## 2026-10-31 19:06 給 lab203 Codex 的更新

沿原 owner、配對身分及反向收據通道接續；以下來源觀測不代替 NAS 驗收。

1. 第一個 4-GiB 私有 envelope `095d79cc…` 已在原來源 ledger 核對到
   配對 NAS file-restore ACK。18:47 固定觀測另有八個 delivery、
   34,318,798,436 bytes 等待 ACK；完整身分保存在 Penguin 的
   `artifacts/storage_layout/trinode-private-wave-observation-20261031T1049Z.json`。
   請原 owner 正常接收、獨立讀回，回傳各 envelope 的配對 ACK 或具體
   尚缺成員／當前階段，勿以傳輸完成度代替它。
2. Penguin 已將來源 pending window 從 32 GiB 對齊既有 retained transport
   256-GiB 上限。沒有改變單批 8 GiB／512 檔、staging 24 GiB、雙端
   reserve 64 GiB，也沒有提高 lab203 的 ceiling。每次配置仍核對新鮮
   readiness、在途及來源實體容量；這不是預配置 256 GiB。來源已沿原
   `private-cycle --wait-for-owner` 啟動有界接續，沒有另建 publisher。
3. 新來源通知 timer 每分鐘只重試原 dispatch 的掃描通知；folder scanning
   時只記小型 deferred 狀態，不掃完整來源、不改 ledger。請回報實際
   接收／NAS 讀回 bytes 與時間、原 owner 當前工作、等待原因及下一階段，
   讓兩端可以分辨來源核對、傳送與 NAS 驗收的瓶頸。
4. 11:06:18 UTC 在 Penguin 讀取時，audit 的 `observed_at_utc` 為
   11:07:18 UTC，比讀取時間晚約 60 秒；同次 readiness 則為
   11:05:43 UTC。Penguin 已以既有時間檢查器核對 Windows host，三次
   取樣 offset 約 -0.94 ms、ready=true，沒有修時。請在 lab203 唯讀
   核對主機時鐘／時區及兩個回執產生器的時間來源；保留舊收據，不修改
   原觀測時間。這個差異尚未阻擋目前 readiness，也不能直接歸因整台
   lab203 的主機時鐘錯誤。
5. 公開與私有固定 cohort、未發布來源及歷史缺件仍分別對帳。保留 TW
   六項 runtime 差異、25 版本／114 物件、新聞缺件和原 pin／retry 證據。
   請依已有固定控制包及實際部署證據更新頂層靜態 not-delivered 投影；
   檔案 ACK 不代表 strict runtime、skill discovery 或完整歷史通過。

## 2026-10-31 22:27 來源通知接續修復

上述 19:10 的 busy scanner 一律 deferred 已被本次明確登錄的單筆接續
取代。Penguin 發現新批次 READY／dispatch 存在但 Syncthing 尚未索引，
不能用既有索引 completion=100% 推定送達。原 notifier 現在排入原 owner
鎖，再把一筆封閉 batch 與 tools 的指定路徑掃描交給既有 Syncthing，
不打斷 scanner；逾時未確認時保留 intent，不重複堆積。

22:26:52 第一筆 intent 已提交，仍等待原 scanner 接續，並非掃描成功或
NAS ACK。lab203 無需另開 backup owner、降低 reserve 或刪 pin；沿原
dispatch 接收、獨立 NAS 讀回與回執流程即可。來源的實際接手及完整測試
見 [自動接續紀錄](storage_automatic_continuation_2026-10-31.md)，不是要求
使用者逐批人工執行。本次沒有宣稱這份文件已由 lab203 讀取或部署。
