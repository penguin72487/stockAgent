# 工作流測速第二十一輪：閒置 GPU 歸屬與 Windows 量測修復

本輪被動觀測到 RTX 5070 Ti 曾使用 15,312 MiB（約 14.95 GiB）VRAM；
一個既有開盤價格敏感度工程 probe 自然結束後，後續板卡觀測 4,102～4,316 MiB
（約 4.01～4.21 GiB）。這是自然程序生命週期的
前後觀測，沒有把它記成新增快取優化的效果，也沒有停止或重啟工作。

## 實際觀測及歸因邊界

2026-10-24 00:34:55 的 canonical Linux FD inventory 核對 73 個受管程序，
唯一持有 GPU 裝置 handle 的是 `stockagent-code-probe-6450af71bc0b4b638714be246bbdc973.service`
中的 PID 492675，持有兩個 `/dev/dxg` FD；當時讀到的腳本名稱為
`analyze_opening_price_sensitivity.py`。00:36:04 再核對 71 個受管程序，GPU FD 為 0。
這個 sampled scope 沒有把所有 Windows、未受管 Linux 配置或板卡 bytes 都歸因給該 PID。

Windows 端樣本觀測到 Sunshine／Parsec 的 video-encode 活動，另有桌面及一般應用。
程序 memory counters 的值與板卡總量不能直接對齊；沒有相加、宣稱每個 PID 的精確
實體 VRAM，或將 Windows PID 當成 Linux service PID。非交易日仍可有工程與桌面工作，
來源 admission、程序生命週期、GPU 計算與 VRAM 保留須分別量測。

## 修正可重測入口

原 `audit_windows_gpu_ownership.py` 在受管環境的兩次 Windows 查詢均 unavailable。
保留的診斷顯示 PowerShell 啟動回傳 `Invalid argument`；同一 canonical 稽核明確
傳入仍有效的 caller WSL socket 後，兩次查詢成功，各取得 47 個 Windows 程序，
分別耗時 27.400926／12.525012 秒。這是不同時間的診斷，不是固定輸入速度比較。

共用稽核增加 `--wsl-interop-socket`：只接受絕對、現存的 socket，僅覆寫該查詢
child 的 `WSL_INTEROP`，不變更 parent 環境、不自動找別的 endpoint。
明確指定但失效時保留 unavailable，沒有回退或冒充零使用量。
輸出契約更新為 `windows_wddm_gpu_owner_observation_v2`，記錄 transport 選擇，
保留原程序身分、非原子樣本、non-additive counters 及 privacy 邊界。

13 項測試通過。初次有兩項 fixture 的 AF_UNIX 絕對路徑超過長度限制；原來源、
失敗 JUnit/log、修正前 driver 與 run 均保留。只修正測試在自己的 scratch 中綁定
相對 basename，沒有改正式邏輯來放寬 endpoint 檢查。

新 CLI 已在 canonical supervisor 中實際通過：一份 Windows 樣本有 46 個程序，
查詢 18.906213 秒，板卡前後 4,107／4,102 MiB。此觀測沒有初始化 CUDA、呼叫
provider、重啟服務或停止 Windows 應用，也沒有宣稱新版本加速或所有 bytes 已歸因。

```bash
source scripts/runtime_env.sh
stockagent-agent run --name UNIQUE_OBSERVATION workflow-speed-baseline-20261021 -- \
  python scripts/audit_windows_gpu_ownership.py --samples 1 \
  --wsl-interop-socket "$WSL_INTEROP" \
  --output artifacts/operations/NEW_PRIVATE_OBSERVATION.json
```

輸出路徑與 run 名稱必須使用新值。沒有可驗證的 caller socket 時保留缺口；不更動
共享 service runtime 的環境，也不藉量測操作去釋放別的工作持有的 GPU。

## 證據

- [原不可用的 Windows 查詢](../artifacts/operations/gpu_idle_20261024/workflow_speed_round21_wddm_v1.json)
- [工程 probe 尚存的 Linux FD inventory](../artifacts/operations/gpu_idle_20261024/workflow_speed_round21_managed_handles_v1.json)
- [自然結束後的 Linux FD inventory](../artifacts/operations/gpu_idle_20261024/workflow_speed_round21_managed_handles_v2.json)
- [明確傳入 live socket 的兩次 Windows 觀測](../artifacts/operations/gpu_idle_20261024/workflow_speed_round21_wddm_live_interop_v1.json)
- [13 項回歸與前後來源 SHA](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round21/gpu_observer_checks_v2.json)
- [正式 CLI v2 的實際讀回](../artifacts/operations/gpu_idle_20261024/workflow_speed_round21_wddm_cli_v2.json)
- [同輪原生台灣公開資料時鐘](workflow_speed_optimization_2026-10-24_native_tw_public_clocks.md)
