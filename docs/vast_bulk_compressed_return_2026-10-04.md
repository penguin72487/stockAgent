# Vast 全量壓縮回傳、D 冷保存與安全回收

這是 2026-10-04 明確授權的**一次性舊資料保存**：把 Vast 的整個
`artifacts/markets`、`artifacts/ablations` 串流壓縮回 penguin D；另保存已盤點且
不使用的 cache。不是日常增量資料重新打包，也不是訓練完成／可部署證明。
原有新完成成果 ingress、來源增量發布、七日使用續租與 GC 不變。

## 判準與資料流

資料價值取決於能否重建，不取決於名字是不是 cache。唯一模型、來源觀測、
結果與收據必須可恢復；正在訓練／服務使用的資料不能因磁碟壓力移除。

```text
Vast 原始目錄 → GNU tar PAX＋zstd-1（SSH 加密串流）
  → Windows 原生 FileStream → D 私人 incoming（只收壓縮檔，不解封）
  → 每個原始成員完整解碼、SHA／mode／uid／gid／奈秒 mtime／空目錄清冊
  → 原 ingress owner → canonical D packed content-addressed blobs
  → 獨立從正式 D blob 再解碼 → 當前 paired index 收斂
  → 精確、新鮮 SSH ACK → remote dry-run／程序／服務／pin／link／來源再檢查
  → 私人 quarantine 再驗證 → 只回收符合條件的遠端熱根
```

沒有在 Vast 建第二份完整壓縮檔；沒有 C 完整副本；沒有新增 Syncthing raw
artifacts folder。SSH bulk 傳输和 Syncthing QUIC index 是兩件事。
incoming、publish alias、正式 blob 共用同一 NTFS inode，不多寫一份巨大 payload。
release ID 是小型原子索引，不是原始目錄的完整 snapshot 複本。

一次性保存設定：[`vastai_bulk_preservation.json`](../configs/data_sync/vastai_bulk_preservation.json)。
至少穩定 12 小時才可回收；這不是把日常七日 GC 改為 12 小時。
不自動刪 D 冷物件、歷史、不完整片段或未知檔案。active/recent/shared/unsupported
與任何證據不齊的根都保留；全量壓縮傳完不等於全部來源可以刪。

## 全量回傳

從 penguin checkout 執行。所有寫入都先驗 enrolled D 掛載；D 至少預留 64 GiB。
每路 zstd 使用 8 threads，兩個目錄並行。會與其他 owner 排隊，不中斷訓練、
服務或舊發布交易。

```bash
cd /root/stockAgent
source scripts/runtime_env.sh

# 唯讀規劃
run_fintech_python scripts/receive_vast_bulk_archives.py

# 實際兩路壓縮回 D；請使用下方受監管背景入口，勿依賴 terminal 存活
stockagent-agent run --backend tmux --name vast-bulk-return \
  vast-all-artifacts-cold-return-20261004 -- \
  run_fintech_python scripts/receive_vast_bulk_archives.py --apply

# 收到後自動原始成員全驗證、登錄正式 D 冷庫，再逐根 dry-run／安全回收
stockagent-agent run --backend tmux --name vast-bulk-organize \
  vast-all-artifacts-cold-return-20261004 -- \
  run_fintech_python scripts/organize_vast_bulk_archives.py --watch --apply --retire

# 不刪來源：移除 --retire；只驗證／計畫：也移除 --apply
# 指定既有單批：加 --batch /mnt/d/stockagent-cold-primary/remote-artifact-incoming/EXACT_BATCH
```

固定 SSH 預設為 `root@114.32.64.6:40032`，私鑰從既有本機檔案讀取，不写進
同步庫、指令輸出或發布 metadata。沒有改遠端 Git worktree。

## cache：先清點、去重，再保存不使用的唯一內容

```bash
source scripts/runtime_env.sh

# 當前完整清冊；物理佔用跨目錄按 inode 只算一次
run_fintech_python scripts/inventory_remote_training_cache.py \
  --output artifacts/operations/vast-cache-inventory.json

# exact SHA 去重的 dry run／apply，只處理穩定 immutable generation .npy
# 每步重新檢查 current service/config/fd/mmap，並持有 canonical panel writer lock
run_fintech_python scripts/compact_remote_unused_cache.py \
  --inventory artifacts/operations/vast-cache-inventory.json
run_fintech_python scripts/compact_remote_unused_cache.py \
  --inventory artifacts/operations/vast-cache-inventory.json --apply

# 去重改變 inode／metadata：保存前重新取得當前清冊，不用舊 fingerprint
run_fintech_python scripts/inventory_remote_training_cache.py \
  --output artifacts/operations/vast-cache-inventory.json

# 不使用且穩定的 cache 先完整壓縮保存；不是因叫 cache 就直接刪
stockagent-agent run --backend tmux --name vast-unused-cache-return \
  vast-all-artifacts-cold-return-20261004 -- \
  run_fintech_python scripts/receive_vast_bulk_archives.py --scope cache \
  --cache-inventory artifacts/operations/vast-cache-inventory.json --apply
```

清冊須為兩小時內。預設保護兩個台股服務 cache，以及當前
`tw_futures_v8_margin_components_20261003` 訓練 cache；當前 consumer 會額外保護。
未知配置／consumer 解析失敗不放行。NPZ／Parquet／來源資料不套用 .npy hardlink
去重；相同 byte 的 immutable .npy 只合併物理 inode，邏輯路徑全保留。
不得原地修改共用 generation；產生新 generation。

2026-10-04 已套用 334 個精確重複檔，釋放 `22,541,832,192` allocated bytes；
刪除邏輯路徑 0、唯一 payload 0。新的跨根 inode 清冊量得 cache
`222,238,560,256` bytes；同輪可用空間會受仍在訓練的新輸出影響。
不能把每個根的大小直接相加，因為 hardlink 跨根共用資料。

## 進度、中斷與續傳

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/status_vast_bulk_return.py
stockagent-agent run-status EXACT_RUN_ID
stockagent-agent logs EXACT_RUN_ID --tail 20
stockagent-data automation-status --human --live

# 只可續接固定舊 batch；遠端重新生成壓縮 prefix，精確 SHA 相同才開始 append
run_fintech_python scripts/receive_vast_bulk_archives.py --scope cache \
  --cache-inventory artifacts/operations/vast-cache-inventory.json \
  --resume-batch /mnt/d/stockagent-cold-primary/remote-artifact-incoming/EXACT_BATCH --apply
```

`receiving` 是壓縮傳輸；`compressed_transport_received` 是傳輸完成；
`canonical_d_originals_verified` 才是正式 D 原始內容恢復驗證；
`cold_preserved_retirement_audited` 只代表逐根審查結束，仍需看
`all_sources_retired`、`deleted`、`reclaimed_allocated_bytes`。
failed prefix 是 retained evidence，不能交 organizer 當完整 archive。
已收完的獨立 batch 可另外以不帶 `--apply` 的 organizer 做原值全解碼；它只寫
`.verification.json` 與原始成員索引，不能覆寫 `.organization.json` 的正式入庫／
回收收據。`received_originals_verified_read_only` 不是 canonical D 恢復驗收。
狀態工具分別顯示原值解碼的已讀／總壓縮 bytes 與正式冷驗證／回收；失敗、完成
或來源變動的傳輸不顯示為正在 `verified_tail_append`。
cache 壓縮 prefix 不同即不 append、不覆蓋。來源持續變動或 tar 非零時保留資料，
不能拿整個 scope 名字作為刪除許可。
續接可重用私人收據內的預期 prefix SHA，但不是重用驗證：遠端重新生成並全 SHA
比對，Windows writer 在排他寫入 handle 下也全读現存 prefix 比對；兩端這次
真實位元組檢查均通過後才 append，避免第三次重讀同一 prefix。

現場 WSL `/mnt/d` 的 64 KiB 9p 大批寫入 ENOMEM；既有正式 8 KiB mount 不動。
新 byte adapter 直接用 Windows 原生 FileStream binary pipes，canonical locks、
inode／volume／marker 檢查不變。8 MiB 和 1 GiB 寫入、`Flush(true)`、獨立全讀回
SHA 通過；1 GiB 同輪寫入含 flush 21.35 秒、讀回 8.12 秒，只代表該驗收樣本。
失敗的約 57.59 GB 舊片段仍保留，並未因新串流啟動就刪掉。
私人測試檔在全 SHA 通過後已移除，可重新測試：

```bash
run_fintech_python scripts/verify_windows_cold_binary_io.py --bytes 1073741824 \
  --receipt artifacts/operations/windows-native-io-acceptance.json --apply
# 大於 Int32 的 prefix 與 exact append 驗收（只建立自己的私人合成測試資料）
run_fintech_python scripts/verify_windows_cold_binary_io.py --bytes 4294967296 \
  --append-bytes 1048576 --receipt artifacts/operations/windows-native-large-append.json --apply
```

續傳曾因 PowerShell `Math.Min` 選擇 32 位元 overload，在 7.17 GB prefix 失敗；
現在雙參數明確使用 Int64，writer ready handshake 通過才送任何新 bytes。
大型 JSON 清冊讀寫另外固定至原 8 KiB 掛載的同一 NTFS inode alias，不使用
`/mnt/d` 64 KiB 9p 做大量 JSON I/O。

4 GiB＋1 MiB 的真實不可壓縮測試已通過完整初寫／flush、讀回、Int64 prefix
再校驗、append、全檔讀回 SHA。完整流程約 420.25 秒（當時兩路大傳輸並行），
不是把續接階段的 332.10 秒誤稱為單次讀回速度。私人測試資料已驗證後回收，
收據：`artifacts/operations/vast_bulk_return_20261004/windows-native-4g-int64-append.json`。
最後一輪相關 storage／transport／packed／legacy／training-return 測試共 **154 passed**。
包含兩個真實 content-addressed blob 的發布／再解碼整合測試、來源變動／額外檔案／
root metadata／consumer／shared link／過期 ACK 等拒絕回收測試。工程測試通過
不代表 393.29 GB 遠端 `markets`／`ablations` 已全部冷驗證或回收。

2026-10-04 18:44 當輪觀察：三路均在背景運行。`markets` 已收約 16.91 GB，
`ablations` 約 15.83 GB；cache 已從舊 `7,170,292,944`-byte prefix 成功續接到
約 7.36 GB。這些是壓縮 bytes，不是完成比例；新批次整包尚未完成 D 原始解碼，
尚未因此刪任何遠端唯一來源。

| 當輪工作 | 受監管 run ID |
| --- | --- |
| markets（原兩路中的 ablations 啟動失敗，已獨立重試） | `native-compressed-markets-ablati-20261004T102031-7d8dc82b` |
| ablations | `native-ablations-retry-with-priv-20261004T102137-875a9577` |
| cache 精確續傳 | `cache-resume-fixed-int64-prefix-20261004T103540-1e9d264b` |
| 正式冷整理／安全回收（與原 owner 排隊，不中斷舊長交易） | `stable-metadata-native-bulk-cold-20261004T103514-4b20c338` |

### 2026-10-04 22:50 更新（不是全量回收完成）

以上 18:44 數字和 run ID 是歷史觀察。22:50 的實際狀態如下；壓縮傳輸、
私人收件解碼、正式冷庫恢復證明與遠端刪除分開計量。

| scope | 收到的壓縮 bytes | 私人收件原值驗證 | 正式 D 冷庫／本批回收 |
| --- | ---: | --- | --- |
| cache | 21,223,985,638 | 完成，2,887 檔；原值邏輯 167,293,069,861 bytes | 等既有 ingress owner；本批刪除 0 根 |
| ablations | 26,301,075,282 | 完成，39,512 檔；原值邏輯 156,321,337,719 bytes | 等正式入庫；本批刪除 0 根 |
| markets | 71,140,449,081 | 獨立完整解碼進行中 | 本批刪除 0 根；打包程序 code 1，來源保留 |

markets SHA 是
`505a0f006d303b01f6f5d0bafc9108055aba85a0aecb78c50b2017273b319746`。
來源在捕捉時仍有變動，因此不能把整包稱為完整傳輸成功；可解碼的原值仍保存，
只有逐根完整匹配**當前**來源且全部安全閘門通過才可回收。ablations 9 根中
2 根含 unsupported 成員，不能整根刪除；7 個 regular 根也仍須經服務與程序檢查。
上述原值 bytes 含 hardlink 邏輯名稱，不能用作實際可釋放空間。

唯一 bulk organizer 已在**未持鎖、無子程序、尚未發布**的安全邊界交接，
新 run 是 `verified-cache-first-bulk-retire-20261004T143603-389ee1ed`，依
cache → ablations → markets 順序。交接收據：
`artifacts/operations/vast_bulk_return_20261004/bulk-cache-first-safe-handoff.json`。
原 bulk run 的 `-15` 是這次有收據的替換，不是來源回收完成。
既有舊根入庫正在持共用鎖時不停止、不強行搶鎖，也不啟動第二個 bulk publisher。

已完成的 cache 去重 22,541,832,192 allocated bytes 與先前逐根回收為歷史成果，
不得重算成本批新刪除量。之後是否刪除，以 `.organization.json` 的每根
`deleted`、`reclaimed_allocated_bytes` 和遠端 retirement journal 為準。
私人 `.original-index.json` 或 `.verification.json` 都不能單獨授權刪除。

同輪另量測到 Vast 時鐘比 penguin 慢 0.41–0.80 秒（含 SSH 往返界限），
立即送出的 ACK 可能被原本的 `age >= 0` 條件誤擋。
共用 `acknowledgement_age` 現在只在驗證 ACK identity／authority 後，最多等待
5 秒讓接收端時鐘追上；**不接受負 age、不改原驗證 timestamp、不延長原
legacy 300 秒／bulk 1,800 秒期限，也不調整任一主機時鐘**。過大偏移、
停滯時鐘、過期或偽造 ACK 仍拒絕。這段控制程式沿用私人 SSH 傳遞，
無須改遠端 checkout 或重啟訓練／服務。
真實遠端唯讀驗收等待 0.384 秒後 age 非負，timestamp 未變，來源和冷物件
都沒有刪除；收據：
`artifacts/operations/vast_bulk_return_20261004/ack-clock-wait-live-verification.json`。
相關 archive／legacy／organizer 測試第一輪 **59 passed**；連同 cache
compaction／packed edge 的最終回歸為 **86 passed**，包含停滯時鐘與不延長
期限的拒絕測試；這仍不是本批來源回收完成證明。

22:55 再查：Penguin／Vast paired index 當前收斂檢查 `ok=true`；Vast 可用
空間 `106,129,244,160` bytes，但本批仍刪除 0 根，不能把其他工作釋放的
空間列為本批成果。markets 原值驗證已讀壓縮 bytes `48,964,304,896`／
`71,140,449,081`，只表示解碼讀取進度，不代表可回收比例。

## 明確恢復某一舊根

使用 organization/cold-proof 收據中的固定 dataset/release ID。只恢復到**不存在的
新路徑**，不覆蓋服務或訓練根，不自動部署。保留 D 冷檔。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/restore_vast_bulk_archive.py \
  --dataset EXACT_DATASET --release-id EXACT_RELEASE_ID \
  --relative-root ablations/EXACT_OLD_RUN --destination /root/restored-old-run
# 核對規劃後加 --apply；須為原生 Linux filesystem，實際主機空間足夠＋32 GiB reserve
```

此一次性 legacy restore 是顯式新路徑，尚未註冊成通用 materialized lease；
不能假設任意新目錄會自動七日刪除。日常 `stockagent-data use` 的受管理熱資料
才套用現有七日續租／GC 契約。需要上線模型另走原部署驗收。
