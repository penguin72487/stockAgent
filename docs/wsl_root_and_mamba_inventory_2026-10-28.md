# WSL 根目錄與 Mamba 空間盤點（2026-10-28）

這是一次作業期間的觀察，不是原子快照；有進程持續寫入，數字會變化。`du` 使用已配置區塊，單位以下為 GiB；硬連結在一次遍歷內只算一次。整理工作樹時發生的事故及其未解範圍見 [復原紀錄](wsl_worktree_recovery_incident_2026-10-28.md)。

## 容量與現況

2026-10-29 00:15 左右，WSL `/` 顯示可用約 314 GiB，D 槽約 385 GiB；重新量測 C 槽實體支撐空間為 76,921,626,624 bytes（約 71.6 GiB），高於既有 64 GiB 的一般下載保留門檻。此數字會變，虛擬檔案系統可用容量不能代替實體 C 槽容量；dispatch 預檢通過也不等於每筆寫入已預留容量。當沖服務按原優先級運作，保留門檻不應繞過當沖需要。

第 9 輪已凍結約 7.13 GiB、40,418 個檔案，當時仍執行 D 槽封存驗證，來源回收尚未完成。現有工作以固定清冊、封存與完整還原證據及進程引用檢查為刪除前提；不能以執行中或本機封裝成功認定 NAS 完成。第 7、8 輪先前已驗證回收約 25.63 GiB；本次另清理已完成的測試暫存約 0.86 GiB，未刪 D 冷庫物件或 NAS 快照。後續中止的測試另留下約 0.59 GiB 暫存，尚未核對清理；這些數字分別是毛回收量與新增暫存，不是實體 C 槽的淨釋放量。

2026-10-29 00:08 左右，新聞資料本機已封裝 21/30 部分；這是本機進度，還沒有 NAS 還原 ACK。私有來源備份清冊為 48,276 個檔案約 761 GiB，進度狀態仍明確標示全來源 NAS 驗證未完成。公開冷資料的一次去重對帳覆蓋 99,277/99,432 個清冊檔案，剩 155 個；該清冊不包含全部私有及未發佈來源，亦不能證明歷史缺件已復原。

## 已量測的 `/root` 主要目錄

以下不是可以直接刪除的清單。初次 `du` 遍歷期間發生工作樹事故，因此該次清冊不是原子或完整狀態；此表只列觀察到且仍需判別用途的主要目錄。

| 目錄 | 約 GiB | 判斷 |
| --- | ---: | --- |
| `/root/miniforge3` | 40.81 | 共享 Mamba／Conda 及環境；多個服務使用固定路徑。 |
| `/root/.shioaji` | 38.82 | 永豐 SDK 的合約及行情等資料；當沖相關，持續保留。 |
| `/root/finlab_db` | 9.87 | FinLab 本機資料；移動之前須確認 SDK 路徑與所有運行中使用者。 |
| `/root/.codex` | 13.72 | Codex 設定、工作及紀錄；用途與保留期限由使用者決定。 |
| `/root/Downloads` | — | 先前掃描約 6.00 GiB；本次檢查路徑已不存在，非本次移除，不能推斷原因。 |
| `/root/data` | 3.56 | 2004–2012 美國藥物不良事件季度檔、VAERS 疫苗不良事件檔、MIMIC-III 示範臨床資料；未刪除。 |
| `/root/syncthing-merge-backups` | — | 實體備份已搬到專案內，原名是相容連結；不因此釋放容量。 |
| `/root/.vscode-server` | 2.94 | 遠端編輯器環境，可能正在使用。 |
| `/root/OpenBBUserData` | 2.49 | OpenBB 資料及設定，與專案資料採集可能有關。 |
| `/root/security-build-ac88-20261024` | 1.69 | ARM／ARM64 的 OpenSSL、Unbound 等安全元件建置結果與 sysroot；保留待確認。 |
| `/root/.local` | 1.46 | 共用程式與設定，不能只憑名稱清除。 |
| `/root/.cache` | 1.36 | 應逐項檢查正在使用的快取後再決定。 |
| `/root/MVP` | 0.06 | 獨立 Git 工作目錄，研究新聞回看天數與本地 LLM 股票預測；保留待確認。 |
| `/root/syncthing-backups` | 0.24 | Syncthing 程式版本與升級前備份；保留待確認。 |

六個原有 StockAgent／Syncthing 合併備份及大小寫不同的 `/root/StockAgent` 空目錄樹已經核對並原子搬入專案；原路徑是相容連結，保護既有還原命令及 Git remote。完整清單、收據、餘下的執行中 SDK 目錄和效能實測見 [後續紀錄](wsl_root_backup_and_pack_prefetch_2026-10-29.md)。19 個事故後重建的工作樹目前位於專案內的具日期復原目錄，但這不表示事故前所有內容皆已復原。

### 其他根目錄項目的用途

2026-10-29 00:31 再以不展開內容的方式列出 `/root` 第一層；下列是命名與抽樣用途，並非刪除建議。憑證及設定檔只核對位置，不列出內容。各子目錄的即時寫入及共用連結仍可能改變容量或使用者。

| 項目 | 調查結果 |
| --- | --- |
| `Documents` | 約 2.8 MiB；NVIDIA Nsight Compute 的 sections。 |
| `canonical-host-backup-20261017`、`security-backups`、`security-expanded-audit-20261023-1550`、`wg-phase1-backup-20261017` | 主機憑證／私鑰、網路或安全設定與回滾備份，可能是共用基礎設施。 |
| `backup-pre-reboot-20261024.py`、`pre-reboot-wsl-status-20261024.py`、`verify-pre-reboot-pg-20261024.py` | 重開前備份、狀態與 PostgreSQL 還原驗證工具。 |
| `cf-recent-events.py`、`deploy-autoblock-ubuntu.py`、`final-compact.py`、`security_autoblock.py`、`test-autoblock-policy.py`、`security-autoblock-protected-special.json` | Cloudflare、主機防護、封鎖與稽核工具／設定；部分參照 StockAgent 的共用設定，不能由此推論為不用。 |
| `.claude`、`.claude.json`、`.copilot`、`.codex` | 不同程式助理的資料與設定。 |
| `.cupy`、`.nv`、`.triton`、`.dotnet`、`.npm`、`.duckdb`、`.ipython`、`.jupyter`、`.vscode-server`、`.vscode-remote-containers` | GPU、程式套件、資料庫、互動環境及編輯器的使用者環境與快取；`.npm` 約 194 MiB、`.nv` 約 85 MiB、遠端容器約 119 MiB。 |
| `.conda`、`.mamba`、`miniforge3`、`.local`、`.cache`、`.config` | 共用環境、套件、設定或快取，詳細空間與硬連結說明見下節。 |
| `.finlab`、`.openbb_platform`、`.shioaji`、`finlab_db`、`OpenBBUserData` | FinLab、OpenBB 與永豐 SDK 的使用者設定或資料；可能仍受服務或 SDK 使用。 |
| `.ssh`、`.gnupg`、`.pki`、`.gitconfig`、`.bashrc`、`.profile`、`.bash_history`、`.lesshst`、`.wget-hsts`、`.motd_shown` | SSH／憑證與終端、Git、使用者環境或歷史狀態。 |
| `stockAgent-test-link-*`、`stockAgent-test-safe-*` | 工作樹事故調查的兩個小型測試證據目錄，保留。 |

其餘明確列名的六個備份與 `StockAgent` 已移入專案並在原位置保留相容連結；`stockAgent` 本身是目前專案工作目錄。第一層清冊不代表已逐檔判定這些共用項目與專案完全無關。

## Mamba 環境與套件

- `/root/miniforge3` 實際配置約 40.81 GiB；`fintech` 環境單獨計約 24.50 GiB，套件快取單獨計約 28.31 GiB。兩者有約 13.10 GiB 同 inode 硬連結，故不能相加；同一次遍歷先計 `fintech` 再計快取時，快取新增約 15.20 GiB。
- 除主安裝外，實測所列服務角色環境的獨立配置合計約 1.90 GiB；全部所量測的 Mamba 樹合計約 42.71 GiB。使用不同次掃描得到的子目錄大小不能直接累加成跨掃描唯一實體大小。
- 實測控制面兩個環境的 Python 版本、SOABI、40 個 Conda 套件版本／build／checksum 與 Python 發行套件版本相同；另一組架構試驗環境亦有 167 個相同的 Conda 套件與相同 Python 套件版本。硬連結使重複空間遠低於目錄大小，這兩組後者各自的額外配置分別約 91 MiB、196 MiB。
- 進一步唯讀逐檔比對，控制面一組有 7,574 個已共用硬連結、450 個同名但內容不同的檔案，另有 674 個只存在於一邊；架構環境一組有 20,139 個共用硬連結、845 個同名但內容不同、2,093／1 個單側檔案。部分差異是含有各自絕對 prefix 的執行入口或 Python 快取；尚未證明所有差異均可忽略。比對結果分別保留在 `artifacts/operations/storage-single-authority-20261025/mamba-control-pair-content-20261028.json` 及 `mamba-architecture-pair-content-20261028.json`。
- 套件清單一致並不證明原生 library、固定 prefix、現行服務引用及歷史重現合約可替換。正式控制面使用其中一個固定環境，當沖及備份也依賴各自環境；本次沒有合併、刪除或改變服務執行環境。
- `conda clean --dry-run --all` 列出約 281 MiB 名目上可移除的快取項目，未實際清理；實際可釋放容量與對符號連結環境的影響仍須另驗證。

## 可量測的驗證優化

已將封存來源清冊的共用父路徑計算移到每個目錄一次。以實際 40,418 檔案、固定輸入交錯執行 A/B/B/A，兩版完整清冊雜湊相同，舊版平均約 7.70 秒、新版約 1.79 秒，約 4.3 倍；證據為 `artifacts/operations/storage-single-authority-20261025/source-plan-abba-20261028.json`。此結果只涵蓋清冊子階段，不代表 D 槽 I/O、完整還原、NAS 接收或整輪總時間加速相同比例。已在執行中的第 9 輪不會自動載入新的 Python 程式碼。

針對來源清冊名稱、連結拒絕、capture 穩定時限與封存還原執行的 focused tests 為 6 passed；四檔更廣泛測試在 79 passed 後為減少對當前回收作業及空間的干擾而中止，不能稱為完整通過。執行中工作採樣顯示第 9 輪正處理 packed snapshot 寫入與來源複查，並多次等待 WSL／Windows 的 `p9_client_rpc`；私人備份則正在平行核對封閉 envelope 的路徑及內容。這些採樣不能證明死鎖，也不足以估算整輪完成時間或宣稱已達硬體上限。

2026-10-28 23:42–23:43（Asia/Taipei）另觀察到 `stockagent-tw-public-source-events.service` 的特徵產生程序約 36.9 GiB RSS、在 Polars sort 中使用多核心；全系統 16 個邏輯 CPU 一次取樣有高系統 CPU 占用與少量換出。這是當沖優先資料鏈的正在進行工作，沒有為加速清理而停止或設置 CPU／記憶體上限；採樣不能證明該 sort 可直接移除，需另依資料排序與輸出等價合約分析。C 槽實體容量讀取曾逾時而安全暫緩；23:44 後重查取得 63,968,051,200 bytes，仍低於 64 GiB 的一般下載保留門檻。

23:44 時該特徵子程序以 0 正常結束，systemd cgroup 未記錄 OOM；子程序完成不代表後續整條當沖鏈已驗收。其固定 summary 顯示完整建表 9,652,279 列、耗時約 334.8 秒，增量規劃原因為 `non_dated_or_global_dependency_changed`：三個 `tpex_api_mopsfin` 表與 `twse_etf_fund_basic` 發生變化。若要減少這類完整重建，須先證明這些來源可按實際生效／發布時間與受影響標的切出精確變更，再用相同資料對照完整重建結果，不能直接取消全域依賴的安全邊界。
