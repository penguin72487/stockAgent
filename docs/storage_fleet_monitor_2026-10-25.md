# 全資料面板：跨機器儲存監控

面板入口：[全資料監控／儲存狀況](https://penguin72487.dpdns.org/data-monitor/#storage-inventory)。沿用既有成員／擁有者存取權限；公開入口不提供儲存 API。

## 目前交付範圍

| 位置 | 容量來源 | 資料分類 |
| --- | --- | --- |
| Penguin WSL | 本機 `statvfs` | 已登錄專案、服務、還原副本、工作樹及套件快取目錄；完成根目錄與部分用量分開顯示 |
| Windows C | Windows 原生磁碟資訊 | 顯示 WSL 位於 C 的關係；不重複計算 WSL，也不遍歷個人目錄 |
| Windows D | Windows 原生磁碟資訊 | 原生 D 掛載下的 StockAgent 目錄；與其他 D bind mount 不重複計數 |
| Vast | 已登錄 SSH、固定唯讀命令 | 已登錄專案、冷庫與還原目錄；首次實測已取得 |
| lab203 | 既有配對回執中的原生量測 | 已收到容量與分類；回執過期時保留原實測時間，不冒充即時資訊 |
| NAS | 既有配對回執中的守護掛載量測 | 已收到容量與分類；仍需 lab203 回傳新鮮稽核與獨立恢復證據 |

容量每 60 秒取樣。資料夾分類是較昂貴的工作，使用獨立單一背景工作者，每 6 小時重用／更新一次；逾時 30 分鐘，失敗後 15 分鐘重試。每個完成的磁碟可先發布，不必等整批。首次清點、失敗、舊觀測與未知均有獨立狀態，頁面不把未知顯示成零。

同一次分類掃描期間，每個容量週期也能讀取已完成的目錄記錄，顯示「已完成 n／m 個根目錄」。未讀完的上層目錄只是已量測下限，不算完成。沒有新增完整記錄時不更新分類時間；容量取樣時間與分類時間保持獨立。這個進度不代表原始資料下載、備份或退休完成。

資料分類可用蜂巢按鈕逐層打開、麵包屑返回及收起；需要大量明細時再展開數值樹。六張磁碟卡顯示總容量、已用、可用、使用率與量測時間。分類另有自己的完成時間，不能把一分鐘前的容量時間當作分類時間。

## 在 lab203 安裝

Penguin 已將 **389,956 bytes、16 個檔案**的封閉安裝包放到既有 ingress 通道。沒有新增遠端管理入口，也不會由 Syncthing 自動執行程式。先等下列路徑的 `READY` 和完整套件送達，再於 **lab203 本機**執行：

```bash
sudo bash -lc '
set -euo pipefail
set -a
source /etc/lab203-backup/backup-pipeline-v10.env
set +a
pkg=/srv/lab203-backup/ingress/tools/storage-observer-20261025-v1
source "$pkg/scripts/runtime_env.sh"
run_fintech_python "$pkg/scripts/lab203_storage_monitor.py" \
  --install \
  --manifest-sha256 d23108d9c5e9995ba7e7464d458f1906f057b8e0bb119b70b88ddf51ce08828c
'
```

安裝會核對完整檔案集合、SHA、固定 manifest 與既有配對身分，再建立獨立的 `lab203-storage-monitor.service`。既有備份服務不需重啟。套件不含金鑰，不變更 NAS 備份內容；只在既有反向回執目錄寫 `storage-status.json`。

確認新服務：

```bash
sudo systemctl status lab203-storage-monitor.service --no-pager
sudo journalctl -u lab203-storage-monitor.service -n 30 --no-pager
```

實際回執位置沿用 `/etc/lab203-backup/backup-pipeline-v10.json` 的 `receipt_root`。Penguin 收到簽章、配對身分與時間均有效的回執後，會自動接上 NAS／lab203 的總容量、已用空間及分類，不需再改網站。

若套件尚未到齊、manifest 不符、NAS 掛載不符或已有同名安裝，安裝器會停止並保留現場。不要自行刪除原安裝或修改封閉包；先看錯誤與固定身分。來源發布收據只證明 Penguin 已發布，不代表 lab203 已安裝或 NAS 完成原生量測。

## 計量與效能

- `total = used + free`；可用空間是 `available`，與 root／檔案系統保留的 free 不同。
- WSL 目前由 Windows 登錄資訊確認位於 C；WSL、C、D、NAS 不是可直接相加的獨立資料量。
- 目錄用量取 `du -B1` 的已配置位元組。同一檔案系統的所有不重疊根目錄放入同一次掃描，hardlink 只計一次；不跟隨 symlink 或跨檔案系統。分類名稱用於導覽，不是可刪除性判斷。
- NAS 整體使用量可含其他使用者。分類只限已登錄授權目錄；CIFS 顯示的配置量不能證明 NAS 底層壓縮、dedup 或快照的實體占用。
- 網頁每次只讀小型已發布 metadata；不觸發掃描、資料庫查詢、SSH、下載或 GPU。API 有欄位白名單，沒有刪除或管理端點。
- 觀測服務為低優先序、I/O idle、單一 census worker，CPU 上限一個核心的 30%，記憶體上限 640 MiB。初次 census 的 page cache 成本不能當成平時閒置 RSS。
- 5 秒 CPU quota 試驗未提供有效的端到端加速證據，維持 30%，不把取消限流宣稱為最佳化。
- 安裝包通知改為只掃 `tools`，不讀取或重掃所有待交付的資料批次。通知以 5 秒為限；傳輸正忙時記為待原通道處理，不借用原本資料掃描的 900 秒逾時，封閉包與發布收據保持有效。

## 本次空間處置及壓縮實測

沿用既有 `packed_snapshot metadata-scratch-plan/apply`，完整核對空索引格式、全檔 SHA、程序引用、quarantine 及 unlink 後 readback，安全回收 **2,684,395,520 配置位元組（2.50 GiB）**。來源、D 冷庫與 NAS 物件刪除均為零。

另一個 **9,911,521,280 配置位元組（9.23 GiB）**的 metadata index 仍有程序 FD 引用，清理門檻拒絕，已保留。既有 managed-cache 與 compiler-cache dry run 沒有可回收候選，未強制清理。不能把磁碟總可用空間的其他變化算成本次回收成果。

三個完整 FinLab Parquet 樣本使用相同 schema、原始值、null 與寫入選項，另驗來源 SHA 不變：

| 候選壓縮 | 三檔輸出合計 bytes | 寫入與 fsync 合計秒 |
| --- | ---: | ---: |
| ZSTD 3 | 6,191,059 | 1.952 |
| ZSTD 6 | 6,104,694 | 1.805 |
| ZSTD 19 | 5,968,894 | 5.458 |

19 相對 6 只多省 **2.2%**，本次寫入耗時約 **3.0 倍**。保留現有壓縮設定，沒有對原資料整批重編碼；以上只是三檔完整樣本，不能推成全庫節省率。

## 驗收證據與維護入口

實作：`stockagent/data_sync/storage_monitor.py`、`configs/data_sync/storage_monitor.json`、`scripts/audit_node_storage_roles.py --monitor --watch`、`services/data_monitor_dashboard/storage.{js,css}`。

固定證據位於 `artifacts/operations/storage-monitor-20261025/`：

- `receiver-handoff.json`、`receiver-handoff-verified.json`：封閉包與通知；不等於遠端安裝。
- `empty-index-zmpfe0ce.retirement.json`、`active-index-protection.json`：實際回收與引用保護。
- `compression-trial.json`、`census-quota-trial.json`：包含不採用的試驗結果。
- `gateway-acceptance.json`：授權邊界、metadata API 延遲及六個既有服務 owner 未變。
- `browser-v*/`：真實桌面／手機操作與獨立未知、過期、惡意標籤 fixture；fixture 不作為原生 NAS 已上線的證據。
- `acceptance.json`：最後 readback 與尚待本機安裝的範圍。

收據效能測試的 20 次成員 API 請求 P50 **17.7 ms**、最大 **50.2 ms**，測得頁面回應約 143 KB。這是當次本機 HTTP 測量，不是廣域網路或冷啟動的 SLO。六個交易／行情／FinMind 服務 PID 與啟動身分保持不變。

## 2026-10-26：背景進度與解析成本修正

舊掃描順序先遇到大型專案，WSL 逾時時仍有 31 個登錄根目錄未觸及。現在只用先前量測的 bytes 安排順序，先處理未知及較小的根目錄；這些舊數字不當成新量測。仍是單次 `du`，維持跨根目錄 hardlink 去重，解析使用原根目錄 ID，不因掃描排序改變身分。

進度使用觀測者自己持有的暫存 stdout，鎖定 FD 生命週期、以 `pread` 讀取最多 8 MiB metadata，不移動 `du` 的共享寫入位置、不增加第二次檔案掃描。NUL 尚未結束的記錄、掛載改變、格式錯誤及超出上限都不能產生新進度。重啟時未完成掃描會恢復排程，不能把中斷的 `scanning` 當作已完成。

實機進度又暴露了解析成本：每筆記錄對每個根目錄重建祖先鏈。修正為先拒絕重疊／重複根目錄，再以祖先索引尋找唯一歸屬，並共用一次深度排序。以實際 **12,575 筆、33 根目錄、1,125,510 bytes** 的固定 metadata，分別以新程序執行 A/B/B/A、B/A/A/B：

| 完整 metadata 處理階段 | 舊版平均秒 | 新版平均秒 |
| --- | ---: | ---: |
| 讀取及 SHA、完整解析、公開欄位檢查、JSON 序列化 | 14.5553 | 2.0165 |
| CPU 時間 | 14.5246 | 2.0148 |

八次完整公開輸出 SHA 相同；此階段約 **7.22 倍**吞吐、CPU 少約 **86%**，不代表整個 filesystem 掃描或全部服務同幅加速。沒有移除驗證、提高 CPU quota、使用 GPU、掃 source payload 或清除全機 cache。

**341 項**相關測試通過；真實 Chromium 桌面 1440／手機 390 及未知、過期、下限、惡意標籤、冷區狀態共 **10 項**驗收通過。桌面與手機也核對實際 API 的 WSL `32／33` 進度，並保存截圖。原唯讀掃描結束後才載入索引解析器，只重啟觀測者；七個受保護 owner（含網站）身分保持不變。

04:33 台北時間的 WSL 結果是 **32／33 根目錄完成、整體 partial**，33 個根目錄都有觀測節點，其中一個仍是下限；不能寫成全碟分類完成。lab203／NAS 當時最後容量實測仍為 10/25 18:56 台北時間，新的 `storage-relay-audit.json` 尚未回來。NAS 備份與 C 退休都沒有因此通過。

本輪固定證據在 `artifacts/operations/storage-single-authority-20261025/`：

- `storage-census-parser-ABBA-BAAB.private.json`：固定真實 metadata、新舊程式 SHA、八次測量及完整輸出 parity。
- `storage-census-progress-adoption.private.json`：進度與公開欄位首次載入、讀取／權限邊界、資料 owner 保護。
- `storage-census-index-adoption.private.json`：等待原掃描結束後的索引版本載入與七個 owner 核讀。
- `browser-native-progress-v6/browser_acceptance.json`：十項驗收與實際 WSL 根目錄進度；同目錄保留桌面／手機截圖。
