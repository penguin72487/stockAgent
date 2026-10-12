# Vastai1T 儲存盤點、冷保存與測試產物丟棄

執行起點：2026-10-06 21:34 台灣時間。2026-10-07 使用者補充：smoke tests、benchmark 等測試產物及其冷保存版本完全不留，直接刪除。這項最新指令優先於原先全部離線成果先封存的計畫。

2026-10-07 後續正式成果改為至少七天熱保留，並完成 `markets` 的 193 項分類。
原 scale-separated cash 訓練結果已固定還原，現況與驗證見
[訓練儲存分類與七天保留](vastai1t_training_layout_2026-10-07.md)；以下容量及
訓練進度保留原觀測時間。

## 第一性原理與資料責任

Vast 的磁碟是非持久訓練工作區，`workspace_is_volume=false`。磁碟容量應分配給正在使用的固定來源、當期 panel/tensor、checkpoint/resume 和編譯工作集。權威原始觀察及 provenance 由 penguin 持有，D 保存不可變冷版本，lab203/NAS 負責離機備份。正式成果至少完成七天熱保留，再於完整回 D、獨立重建及原回收門檻通過後回收；使用者不要的測試產物直接丟棄。

本輪未變更 instance、kernel/driver、Caddy、付費 volume 或正在訓練的 frozen code/config。GPU 執行與儲存維護分開持有工作。

## 已執行的回收

以下容量是檔案 `st_blocks`／最後硬連結所回收的實體 bytes，不能用檔案 logical size 或多個相同名稱相加。

| 操作 | 實體 bytes | 證據 |
| --- | ---: | --- |
| pip 支援的 cache purge | 36,564,992 | `pip-cache-plan/apply` |
| 六個未釘選 managed materialization：來源逐檔 SHA 與 D 全集合獨立重建後 edge-aware evict | 17,113,210,880 | 各資料集 `d-materialization-*`、`evict-*-apply.json` |
| 26 份相同工程 Parquet 完整 SHA 去重，保留所有路徑 | 32,932,761,600 | `derivation-compaction-apply.json` |
| 最新指令後直接丟棄測試目錄與 26 個 Parquet 名稱，三輪含唯一失敗項重排 | 33,895,755,776 | `testing-discard-remote-result.json`、`testing-discard-remote-final.json`、`testing-discard-last-retry.json` |
| 追加 20 個舊 market smoke／benchmark log 精確目標 | 1,044,480 | `testing-discard-v2-config-repaired.json` |
| 停止的測試回傳工作在 penguin 留下的對應測試子目錄副本 | 5,023,805,440 | `cancelled-test-private-scratch-discard.json`，不計入 Vast 的 df |

最後一列只刪除這個工作私有 staging 中的測試子目錄，保留正式觀察、transfer intent 和操作證據。Parquet 去重與最後刪除名稱是不同階段；後者只有最後連結回收的實體容量納入第二次計數。

2026-10-07 01:25 台灣時間觀測：Vast total **1,073,741,824,000 bytes**、used **748,604,563,456**、available **325,137,260,544**。與本輪起點 used **907,716,513,792** 相比，df 淨減 **159,111,950,336 bytes**。這包括本輪期間既有正式冷回收 owner 完成的其他目錄，不能只把上表當作所有 df 的來源；也不能把歷史已回收 100 GB 再計一次。訓練持續寫檔，數字是時間點觀察；證據為 `final-remote-storage-check.json`。

## 直接丟棄的固定範圍

現行 [`testing_artifact_disposition_20261007_v2.json`](../configs/data_sync/testing_artifact_disposition_20261007_v2.json) 固定 **123** 個目標，身分 `9ca7717dbc33ca7d5c09d32260385b2225b1563e5c39ff76509f8ec99c0fac1b`。原始 [103 目標清冊](../configs/data_sync/testing_artifact_disposition_20261007.json) 及身分 `f20a4c9b2380d1d75c43a9d17ba477d7c6d1fe45b493de7693016442654f46f2` 留作歷史。

- 明確列出的 `artifacts/smoke` 子目錄。
- 舊 `tw_day_trade_factorized_panel_20261004_v1` 的 16 個工程 validation 目錄。
- repair 工作內的 benchmark、DDP acceptance、checkpoint replay 和 minute smoke 子目錄；同目錄內 official raw/delivery 來源及工程 receipts 保留。
- 實測工作區內 26 個 `first.parquet`／`independent.parquet` 精確名稱。位置由現場清冊確認在 `/root/stockagent-remote-build-measurement-*` 或 `/root/stockagent-control-pilot-*`，不是 `/var/lib`。
- v2 加入舊 market smoke 空目錄及 benchmark logs，共 20 個目標。追加項目實際回收 1,044,480 bytes。

最後以清冊的 `artifact → /root/stockAgent/artifacts`、`engineering → /root` 對應逐名 `lexists`：**123 個目標全部不存在**。`artifacts/smoke`、`artifacts/benchmarks` 也已空，market 第一層没有剩餘 smoke／benchmark／acceptance 名稱。這是固定範圍驗收，不把名稱探測當作未知目錄可刪的證明。

流程先保存 dry run，再重查程序、active config、服務、FD/mmap、holds、pins 和 source generation，移到同一 filesystem 的私有 quarantine 後再次核對，最後逐名 unlink。外部硬連結不刪，只有最後連結才算容量回收。完全不要的測試不再要求 recovery、completion 或保存期。

Vast overlay 搬移某個目錄時 `st_size` 由 24 變 32，子檔集合／身分全相同；原判準保留 quarantine。修正為目錄以完整成員清冊、inode、mtime、mode/links 核對，檔案仍核 size；只允許同一 owner 的固定、未變動 quarantine 還原後做新 dry run。此失敗項已單獨重排完成，沒有略過安全检查。

`stockagent-legacy-return@reviewed-offline-20261006.timer` 已停用；其 worker 在不持共用鎖、無子程序的等待點結束。panel 正式保存選單從 52 個目錄縮為 36 個，原選單與 inventory SHA 保留。新 timer rounds 每項重讀丟棄清冊，不會重新保存已丟棄項目。

## 冷資料處理與同步邊界

`scripts/discard_cold_testing_artifacts.py` 使用現行 D mount guard 和共用 publication owner。完整圖把所有保留的 manifest（包含 restricted/historical/staged）當作引用。刪除僅限測試版本獨占的 CAS 物件；共用的物件保持原位。manifest/disposition 小型收據留作為何資料不再要求恢复的證據。

兩輪 D 冷資料直接丟棄，共 **126 個獨占物件、677,933,056 allocated bytes**（33 個／289,513,472 bytes；93 個／388,419,584 bytes）。有 **5 個共享物件**仍由保留版本引用，沒有刪除。證據為 `testing-cold-discard-apply-retry.json`、`testing-cold-discard-v2-retry.json`。

`backup_stream.capture_catalog` 明列 `discarded_testing_releases`／`discarded_testing_objects`，不將它們算成未完成歷史或待備份 payload。DuckLake 最新 `source_objects` 移除這些精確、沒有其他引用的物件，歷史 metadata 保留。控制 worker identity 同時綁定輕量 disposition 程式和清冊；控制環境不需要匯入訓練套件。

控制 worker 升級時曾因排除規則匯入訓練模組而缺少 NumPy、退出重試。已拆成標準函式庫的 `testing_disposition.py`，保留正式 Mamba 角色隔離。PG、Temporal server 及 GPU 沒有重啟；恢復與重新載入收據另記。此事件不能描述成全程未中斷。

最終新版在 source／catalog 既有 owner 邊界接手，worker PID `2767790 → 2898455`，載入程式身分 `26e7e3ef543d835a22e4996ee5d42ee71ee0650b4db8505056c83b4f3a7bbbb3`。兩條 Temporal 工作鏈保留相同 first-run 身分，PostgreSQL／Temporal server PID 及私有設定 SHA 未改；見 `final-lifecycle-queued-owner.json`。交接改用有期限的 kernel flock 排隊，避免持續來源波次讓非阻塞搶鎖一直錯過邊界。

新版成功提交 DuckLake snapshot **91**，842 個保留版本皆可讀、unavailable **0**。再獨立核對所有 immutable manifests 與 archive staging 引用：6 個測試版本及 126 個測試獨占物件均未留在現行 registry，126 個物件全部不存在，5 個共享引用保留；見 `final-catalog-disposition-verified.json`。此完整 capture 花 537.37 秒，包含 D／DrvFs 的 metadata I/O；不宣稱 metadata 吞吐改善。

另修正 catalog 擷取與平行發布的競態：初次 manifest 清單之后才發布的固定 head，會再讀取其指定 immutable manifest，驗證實際 bytes／SHA／dataset／snapshot 等身分。測試丟棄 head 也先完整驗證，不能藉排除規則掩蓋錯誤。這不停止 publisher，也不以 moving latest 取代固定來源。

本輪不把 D 的刪除收據當作 NAS 物理刪除收據。已存在的 Restic／immutable NAS 副本、含正式來源的混合 archive，必須依其各自的固定版本與引用政策處理。未對整個 NAS repository 啟用 prune，也未刪除會影響正式來源的共享 pack。全歷史備份是否完成仍看 catalog 與逐批 NAS 收據。

01:25 現場同步：packed 索引及收據回傳 folder 為 idle，need／errors 為 0；lab203 ingress 遠端 completion **87.01%**、尚待約 **5.54 GB／183 items／369 deletes**，來源仍在 scanning，不能稱全鏈收齊。NAS relay 新 heartbeat 為 `processing`，single owner／真實 NAS mount／runtime lock 門檻皆通過。來源持續發布新增正式版本、其新波次待 NAS 驗收約 **32.09 GB**，receipt errors 為空，publication 不等待前批 NAS ACK；待辦來自持續新增，百分比不能當作固定分母。見 `final-sync-status.json`。

## 保護中的資料與接續工作

兩張 GPU 的 DDP PID `2190325/2190326` 持續使用固定 `20261006T122817687210Z-14e94f045528` source/config，在 `scale_separated_cash_v1/training-bf16` 的 fold 11、epoch **150/1000**（上述觀測時間）。此資料及 checkpoint、20261005 gaprepair panel/values、當期釘選 materialization、活動 Torch/Inductor cache 保持原位。

追加測試清理前發現，現行配置的多層 `base_config` 指向已由正式冷回收 owner 退休的 `markets/tw_day_trade_no_basis_batch_powers_20261006_v1/large_batch_optimization_v5/code/configs`。舊檢查只看頂層 YAML 和解析後 dataclass 欄位，遺漏繼承檔案本身。當時 GPU 仍在執行，但日後 resume 會失敗，不能只用 PID 存活當作配置完好。

已為該根安裝 recovery hold，從先前已完成 D 獨立驗證的保留副本還原 **35 個配置檔**，逐檔核對固定冷 manifest 中的原始 SHA／size／mode／mtime。冷 snapshot 為 `legacy-vast-return-8254a220bb0b4d4449170-20261006T160946957878898Z-l0-penguin-af3993d57b8f8b2c`，manifest SHA `441a44c3525f31de954e1abb6d40244477640d02057185caa618d4078c83d23e`。沒有重啟 GPU 或更改配置內容；現行完整配置來源鏈 **38 檔可讀**。`remote_legacy_return` 與 `artifact_consumers` 共用 canonical `load_config(..., source_signatures=...)`，把每層繼承檔案納入程序／服務引用保護；缺檔依然拒絕刪除。證據為 `inherited-training-config-restored.json` 和最終遠端檢查。

原始 `data_tw_minute`、`data_yahoo`、`data_okx`、期貨來源／preparation_sources、未辨識 quarantine 和 explicit recovery holds 不因 cache/feature 檔名就清掉。三個 inactive 但仍釘選的歷史來源版本未擅自解除 pin。

main／partitions／reviewed-main／reviewed-panel 原有正式冷回收 owner 與 retry timer 繼續處理未使用、需保存的成果。單一來源已回收、服務 active 或若干 dry-run eligible，不代表整台 Vast 都已清理完。剩餘容量及每項原因用同一 task `vastai1t-storage-cleanup-20261006` 追蹤。

## 驗證與收據

操作清冊及逐項收據：`artifacts/operations/vastai1t-storage-cleanup-20261006/`。遠端原始清冊：`/var/lib/stockagent-storage-audit-20261006/`。這些操作收據不含 NAS/SSH 密碼或金鑰。

新增丟棄行為、backup catalog 與 lakehouse 相關檢查：**130 passed、1 skipped**。另 10 個 focused 配置／程序檢查通過，含真實子程序和多層 canonical config loader。涵蓋活動 config/程序/共享 inode、來源改動、symlink 跳脫、外部硬連結、quarantine 接續、cold pin、共用 CAS 保護、新 formal reference 拒絕舊方案、晚發布 head 擷取，以及正式來源在清除測試冷物件後完整 fetch 還原。skip 是現有 optional test，未當作通過。

同輪先前修改：193 個 legacy/remote/scratch/return 檢查、141 個 packed/native/retirement 檢查、後續 75 個 blob alias／packed native 檢查均通過。實際 67-member pack 三輪交错中位數：DrvFs 1.640227s、強制 native SSD 3.152907s、完整 SHA bounded-memory 1.436647s、private verification-only scratch 0.132292s。完整 1,540 檔／56,962,705 原值 bytes 的獨立 D 重建 38.170719s；未清 OS cache，未宣稱整個冷回收倍率。

verification-only scratch 保留全部 object/member/original SHA、CRC、size/metadata/space gate；不發布 managed target／READY、pin 或 lease。一般 fetch／正式還原仍 fsync。相同 CAS blob 的多個邏輯檔名可在同次 fetch 從第一份完整驗證的 SSD 檔案複製，每個檔案仍核 SHA 並保留各自 mode／獨立 inode。
