# 全系統開機恢復修正與驗收（2026-10-05）

本輪已部署 penguin 的 Windows／WSL 持續存活、16 個正式 owner 的有界退避、
掛載成功後接續啟動及自動恢復稽核。正式稽核已退出 0，
`local_infrastructure_ready=true`；最新清單為 **74 services／57 timers／3 paths**。
lab203／NAS 持續回傳新備份驗收，Vast 同步仍在線。這些結果沒有取代四節點的
獨立冷開機測試，也沒有把業務資料缺口或全歷史備份宣稱為完成。

## 第一性原理與責任邊界

開機恢復需要四件事：程序的 owner 真的能從開機事件啟動、其執行環境持續存在、
依賴恢復後會再嘗試，以及能核對恢復的是哪一層。程序 active、網站可回應、
資料庫能讀到契約、資料最新、模型相容、可執行下單是不同的證據。

| 節點 | 正式責任與恢復方式 | 本輪取得的證據 |
| --- | --- | --- |
| penguin Windows | 既有 `StockAgent Public Caddy` S4U 工作；開機、登入及每分鐘 IgnoreNew 觸發 | 工作 Running、安裝檔 SHA 一致、獨立 foreground WSL child 存活 |
| penguin WSL | systemd 管理網站、資料收集、行情、paper engine、資料庫與同步 | 16 個重要 owner active、掛載守門與 PG 契約查詢通過、所有排程有效 |
| lab203 | 既有 WSL/systemd 備份與 Windows 喚醒／runtime holder；不開 SSH | 已配對接收端持續回傳 runtime lock、單一 owner、NAS guard 與新批次 accepted |
| QNAP NAS | 接收端以既有私有掛載與 Restic repository 身分保護備份 | 新鮮接收端收據；本輪沒有登入 NAS 管理端或重啟 NAS |
| Vast | 現有容器 supervisor 管理 Syncthing；來源與產物仍按原生命週期 | Syncthing autostart=true／autorestart=unexpected、RUNNING；舊正式訓練仍 STOPPED |

租用容器的 stop/start、provider 主機重啟、recycle/destroy 是不同事件；本輪沒有
替 Vast 購買持久 volume，也沒有以容器程序的存活證明 provider 主機的重啟策略。
USB 金鑰保管沿用使用者已確認的狀態；lab203 Windows 登出驗收維持取消。

## 實際修正與實測

### Windows 必須持續持有 WSL

2026-10-05 實際 Windows 開機為 01:38:38.5。原工作只派送短命的
`wsl.exe ... systemctl start --no-block` 子程序；首次後端健康出現在 01:39:31，
隨後又反覆停止／恢復，到 01:41:27 才保持健康。近似為首次 52.8 秒、最後一次
恢復 169.1 秒，不能把 systemd 的約 22 秒當成整台 Windows 的恢復時間。

[Microsoft 的 systemd 說明](https://learn.microsoft.com/en-us/windows/wsl/systemd)
明確指出 systemd 服務不會維持 WSL instance 存活。因此在原本
`start_windows_public_caddy.ps1` supervisor 中持有獨立的 foreground WSL sleep，
每輪檢查其退出並重建，與短命的 gateway 派送分開。既有 S4U 工作、Caddy 設定、
只重啟 read-only gateway 的健康恢復邏輯均沿用。

對唯一辨識的 idle holder 發送 SIGTERM：新 Windows child 實際在 **4.103 秒**
啟動，**4.420 秒**內觀察到完整接手。網站全程健康；當時 17 個常駐服務的
PID／InvocationID 均不變。這是 warm fault recovery，尚不能據此宣稱新配置的
Windows 冷開機或登入前恢復已完成實測。

### 持續重試必須有速度上限

原 gateway、行情與 Syncthing 等服務可能連續失敗後耗盡 StartLimit，停止自動
嘗試；PostgreSQL instance 與 backup transport mount 原本沒有 failure restart。
現在原 16 個 owner 使用獨立 drop-in：保留現有第一次重試及 always/on-failure
語意，採 systemd 原生 `RestartSteps`／`RestartMaxDelaySec`，取消會永久停住的
start limit。PostgreSQL 新增 10 秒、transport mount 新增 15 秒的首次重試。

| Owner 組別 | 首次重試 | 最長間隔 |
| --- | --- | --- |
| Discord／day-trade／overnight／Syncthing | 1 秒 | 30 秒 |
| Public gateway | 5 秒 | 30 秒 |
| TAIFEX dashboard／source events | 5 秒 | 60 秒 |
| Shioaji bidask／top200／TEJ history | 30 秒 | 120 秒 |
| FinMind 三個收集器 | 60 秒 | 120 秒 |
| D mount／backup transport mount | 15 秒 | 60 秒 |
| PostgreSQL 18 main | 10 秒 | 60 秒 |

有故障時仍保留 Result、退出碼、來源／業務收據；沒有縮短現有交易 watchdog，
沒有改動模型、查詢配額、訂單、帳本或訓練相容性。

另在隔離的原生 systemd units 測量三個依賴候選。Wants／Upholds 的相互依賴
在這個拓撲下繞過退避，六次重試只用約 0.28／0.29 秒；Upholds 還會拉起被
明確停止的 consumer，因此均未採用。選定 **ExecStartPost 非阻塞接續**：
parent 的檢查通過才請原 owner 啟動下一層。

選定候選在缺依賴時持續超過六次重試，花 **3.571 秒**且保持 consumer 未啟動；
放行依賴後 **1.034 秒**恢復，consumer crash **0.150 秒**恢復，明確 stop 仍
被尊重。這使用 100ms→1s 的隔離測試間隔，並非正式 NAS／行情的恢復時間。
正式 hook 只啟動已 enabled 的下一層，disabled／masked owner 保持停止。

TEJ trial timer 的 `OnBootSec=3min` 改為 `OnActiveSec=3min`，保留原
15 分鐘 completion-relative 週期及 API budget gate，使 timer 自身重啟也有
啟動錨點。只重啟 timer，沒有手動重新提交 trial 查詢。

### 恢復驗證必須在實際 owner 的掛載空間

IDE 的 WSL mount namespace 與 PID 1 不同：IDE 看到 D root 64KiB、舊 binds
8KiB；正式 backup stream／Syncthing 的 namespace 中三者均為 8KiB 且 guard
通過。本輪曾誤把 IDE 視圖當成正式掛載問題，造成一次不必要的 D owner restart。
正式依賴鏈已恢復、同步已收斂，沒有刪除來源；自動修復候選已移除。

現在整個稽核透過 `run_boot_recovery_audit.sh` 進入 PID 1 的 namespace，
掛載檢查、marker、檔案讀取與 PG 檢查保持相同視圖。不能只在另一 namespace
驗證 guard，再從原 namespace 使用資料。`--unmount` 仍只允許已識別的 D mounts，
使用原生 umount 的 busy gate；不使用 force／lazy detach，也不放寬啟動守門。

systemd 的 PATH 也不繼承 IDE 的 Windows 路徑；啟動器會在本機存在時加入
Windows PowerShell 目錄並沿用穩定 init interop socket。觀測將 Windows
wsl.exe launcher 與它的 wsl.exe worker 合併為同一 foreground owner，
保留來源 hash、實際退出時間與未知身分，避免把 wrapper 算成第二個 owner。

## 自動觀測與操作

新增 `stockagent-boot-recovery-audit.timer`：啟動後 90 秒執行，再每五分鐘
持續更新。使用原有 `audit_service_latency_coverage.py` 盤點所有服務與排程，
另核對 16 個 owner、兩個 D mount guard、PG read-only 契約版本查詢與 foreground
WSL holder。失敗退出 75，不能當作恢復成功；市場關閉或原有結算阻擋保持可見。

收據位於 `/var/lib/stockagent/boot-recovery/latest.json`。以 kernel boot_id 加
systemd userspace 起點保存完整的第一份觀測，因為 WSL userspace 可重啟而 kernel
boot_id 不變。第一份使用完整 temporary JSON 加 atomic no-overwrite link 發布。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/install_boot_recovery_policies.py \
  --apply --install-audit-timer --output artifacts/operations/boot-policy-install.json
systemctl start stockagent-boot-recovery-audit.service
systemctl status stockagent-boot-recovery-audit.timer
```

手動觀測使用 `bash scripts/run_boot_recovery_audit.sh`，讓整個檢查使用正式 owner
的 mount namespace；不要把 IDE 中單獨的 guard 失敗解讀為正式服務掛載失敗。

本輪原生自動稽核 02:38:17→02:38:25 成功退出 0；所有啟用 timer active、
無下一次觸發問題、無 failed project unit，`local_infrastructure_ready=true`。
五分鐘 timer 於 **02:43:26 自然再次觸發**，02:43:33 退出 0、同樣為 ready；
完整這輪 wall 7.759 秒、CPU 1.948 秒、memory peak 59M，未靠手動重啟冒充排程驗收。

## 驗收與剩餘邊界

- **441 個相關回歸通過（3.98 秒）**，涵蓋 boot contracts、稽核證據、holder
  身分、disabled owner、錯誤掛載拒絕 detach、cold primary／transport 與 binary I/O。
- 從 Vast 對 public origin 做 **22 個 HTTPS GET，全部 HTTP 200**。這是回應與
  首批最多 2KiB body，不是瀏覽器完整繪製、API 全資料正確性或特定 IPv6 路線驗收。
- 02:37 四個同步資料夾 idle；need、pull/watch error 全 0；lab203／Vast 連線、
  completion 100%。lab203 新批次 accepted 與 NAS guard 收據持續更新。
- TAIFEX 仍被既有未平模擬期貨避險部位阻擋結算；day-trade 有既有 partial entry
  警示；市場關閉時 capture waiting 保留。沒有製造平倉或把這些狀態改成 ready。
- 尚未對新配置做四台機器的獨立冷開機、Vast provider host restart 或新訓練
  checkpoint 的斷電還原。本輪不主張全歷史備份完成；receiver 仍明列 false。

完整清單、原始時序與收據在
`artifacts/operations/all-services-boot-recovery-20261005/`：

- `automatic-audit-acceptance.json`：74／57／3 完整清單與正式 namespace 的正向驗收。
- `runtime-holder-fault-test.json`、`windows-supervisor-deployment.json`：4.420 秒接手與 source/install SHA。
- `systemd-recovery-post_start.json`：選定依賴候選的失敗、恢復及明確停止測量。
- `recovery-policy-final-deployment.json`、`tej-timer-deployment.json`：部署前後與排程。
- `mount-namespace-evidence.json`：不同視圖與不必要 restart 的工程紀錄。
- `sync-and-nas-acceptance.json`、`vast-and-public-https-acceptance.json`：即時同步、NAS 收據及外部 GET。
