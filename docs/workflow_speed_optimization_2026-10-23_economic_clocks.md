# 經濟資料、OpenBB 時計與收據驗證優化：2026-10-23

本輪新增 **93 個來源的直接收據驗證階段計時**，原有 2,399 項來源清冊的階段覆蓋
由 **1,553 增至 1,646**，無遺失，剩餘 **753**。這些來源的粒度不同；這不是
1,646 條完整取得流程的端到端驗收，也不是全部工作流或硬體極限的完成宣告。

前輪見 [FinMind 時計報告](workflow_speed_optimization_2026-10-23_finmind_clocks.md)。
本輪不啟動下載、不重啟正式服務、不改配額／鎖／資料 schema，不啟動 GPU 工作。

## 第一性原理與實作

使用者等待時間包含排隊、供應者節流、取得、解析、驗證、寫入與結果提交。
UTC 時戳只描述事件時間，不能代替單調耗時；共享交易也不能平均分攤成每個來源的
真實耗時。因此沿用原有責任入口增加觀測，不改資料、帳務或請求語意。

- `downloader/operation_timing.py`：每次呼叫獨立的 `perf_counter_ns` 時計，保留成功與
  拋錯階段；各階段可能互相包含，不相加為總耗時。負值時計為未知，不補零。
- `download_public_economic_history.py::acquire`：紀錄舊收據驗證、原始資料重用、含節流與
  重試的 fetch、解析、遮蔽、修訂合併、寫檔與雜湊。耐久收據的 snapshot 結束於自身
  寫入之前；回傳／既有 status 中的時計涵蓋收據寫入。快取命中取得本次驗證時計，
  不繼承舊下載耗時。原始值、單位、發布時間未知狀態、fingerprint 與 quota 記帳保留。
- `OpenBBWorker`：分開觀察 semaphore 等待、adapter（仍含內層節流／取得／解析）、
  外層 pacing、正規化、過濾、寫檔與 backoff。排程器另記 executor 排隊、discovery、
  submit 至 reap 的重疊區間。整個 fallback 任務的總時間不綁成單一 provider 時間。
- `Manifest.complete`：在原任務交易的 `archive_meta` 保存每個 provider／endpoint／結果
  的最新樣本，不增加每筆獨立 commit、表或 schema migration；回滾會一起撤銷觀測。
  整批提交／quarantine 的時計沿既有 scheduler 狀態保存，不分攤成逐來源耗時。
- `profile_workflow_speeds.py`：支援上述版本化時計、唯讀有界 OpenBB 快照與經濟來源
  收據。OpenBB 最多選 4,096 筆，超量明示；SQLite 查詢沿 query-only 交易與 3 秒期限。

前輪只檢查 `data_public_economic`，5 個 Frankfurter 來源在該根沒有收據。
本輪核對正式 owner，確認其根為 `data_forex_frankfurter/official_v2`，已納入；
這修正了觀測範圍，不能把前輪的缺少 metadata 解讀成來源沒有資料。

## 真實來源測量與固定輸入比較

`scripts/benchmark_public_economic_receipts.py` 對原清冊的 **93／93** 個經濟來源
呼叫 canonical `verified_receipt`，93 份收據均通過，再分開逐檔核對
**11,618** 個原生成員、**1,434,130,428 bytes**。這證明當次收據成員的位元組，
不證明供應者全部歷史、來源更新完整度或歷史 PIT 安全。

首次原生觀測的函式內 wall 為 **15.441445 秒**，其中直接收據驗證合計
**3.636928 秒**，另有 **9.840836 秒** 的全成員雜湊讀回；函式 wall 不含 interpreter
匯入與最後報告寫入。這次原生觀測不是以下暖快取比較的 A 組，不能直接互相比速。

已實作的候選只減少重複工作：每次驗證只解析根目錄一次，保留逐成員的路徑解析、
大小、簽章與必要雜湊，返回前再次核對根目錄。根目錄改變即拒收，不做跨呼叫 TTL 快取。
路徑跳脫、symlink 跳脫、根目錄變更與損壞資料均有語意測試。

比較使用完整的 93 來源與 11,618 成員的私人固定副本，資料位元組、來源時鐘、
查詢 fingerprint 不變；只投影副本的本機 `verified_file_signature`。沒有 hardlink
原始檔案，也沒有改變原始 ctime。每次比較使用新 interpreter，保留輸出與全部
雜湊讀回，執行預先聲明的 ABBA 與反向 BAAB 共八次。

| 順序 | 驗證階段 A／B 平均秒 | 階段耗時變化 | 完整 child A／B 平均秒 | child CPU B／A |
| --- | --- | --- | --- | --- |
| ABBA | 3.559758／2.979327 | −16.3% | 8.482934／8.485753 | 1.00248 |
| BAAB | 4.943429／3.024679 | −38.8% | 12.009278／8.903540 | 0.71455 |

八次來源、檔案、原始值與輸出語意／雜湊一致，兩組都通過預定的階段改善與完整
耗時／CPU 不退步門檻，保留候選。**ABBA 完整耗時持平**，反向比較有共享負載變化，
因此只接受減少重複解析與階段改善，不宣稱穩定的整段加速幅度。這是私人副本的
暖檔案系統工作量，不是冷磁碟吞吐或正式服務已採用的證據。

原始數值與預定門檻見
[比較收據](../artifacts/benchmarks/workflow_speed_optimization_20261021/round17/comparison.json)。

## 完整整合與讀回

保留原四個 training roots、depth 15、benchmark depth 32、500,000 directory budget、
256 MiB 單檔／4 GiB 總額，以及原有明確收據；靜態清冊 worker 4、證據 reader 1／256 MiB、
C++ backend 沿用前輪已接受版本，沒有採用前輪撤回的 scalar prefilter。

本輪完成 **13 階段、13 輸出、232,547 個計時樣本**，collector wall **915.710147 秒**；
含前置封存、完整採集與獨立讀回的監督命令為 **944.463993 秒**。本次沒有搜尋缺口。
原有兩筆 FinMind local-derived 記錄仍缺有效 transport clock，保留未知。
共享主機與來源 corpus 有變動，不以這些 wall 數字比較前輪整體速度。

主要瓶頸是 benchmark evidence inspection：**778.133728 秒 wall／163.364253 秒 CPU**。
實際 cgroup 執行緒取樣 6 次，其中 5 次處於 `folio_wait_bit_common`。
取樣前曾讀到 host I/O PSI `some avg10=92.18`、CPU PSI `some avg10=0.79`；
封存的後續取樣 I/O avg60 約 51–58，仍包含讀檔阻塞。這證明當時存在 I/O 等待，
不證明是哪個外部 writer 造成，更不是 NVMe、GPU 或整個系統已到硬體極限。
原 run 完成前沒有因觀察逾時而重新啟動。

獨立 reader 重新核對原始與私人副本各 11,618 個檔案、八份完整比較輸出、
93 個來源與 186 個直接階段樣本、完整來源集合、零遺失的既有綁定，以及 32 份
所選程式版本。驗收見
[階段讀回](../artifacts/benchmarks/workflow_speed_optimization_20261021/round17/phase_acceptance.json)。

相關回歸 **469 項通過**；另有受影響的 Frankfurter／keyed catalog **103 項通過**，
兩組分開記錄。首次新 benchmark CLI 因漏加 repo 匯入路徑而在讀資料前失敗；已補正，
加上倉庫外工作目錄的 CLI 測試並完成原完整範圍。失敗回條與 log 保留並有修復處置。

## 尚未完成

本輪 OpenBB 正式 DB 的新時計樣本為 **0**；新 producer 程式與正式 owner 的載入、
自然請求、耐久觀測仍是獨立待驗收項。單元測試、私人副本與本機檔案 SHA 不代表部署。
也沒有逐來源完整 E2E、遠端完整訓練或所有瓶頸已達上限的證據。

下一輪優先拆解完整證據讀取中的必要流量、重複讀取與有身分證明的重用方式，並接續
OpenBB 未綁定來源及正式時計驗收。保留完整解析、原始 bytes、SHA 與讀回，不以少讀
來源或跳過驗證換速度。完整剩餘清冊保留在階段讀回的 `remaining_unbound`。

重跑收據驗證可使用新輸出目錄：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_public_economic_receipts.py \
  --source-inventory artifacts/benchmarks/workflow_speed_optimization_20261021/round17/collection_full/inventory.json \
  --output-dir artifacts/benchmarks/economic_receipt_validation_NEW
```
