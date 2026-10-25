# 非交易日 GPU 常駐用量修復（2026-10-23）

本次範圍是 penguin／WSL2 的非交易日常態資源；不改模型、精度、輸入、
訊號或成交帳務。所有機器狀態與原始來源留在
[`gpu_idle_load_20261023`](../artifacts/operations/gpu_idle_load_20261023/)。

## 原因與修正

11:49 同機整卡三次觀察為 14,959／15,048／15,048 MiB，容量
16,303 MiB。Windows WDDM 的 vmwp 用量為 11,548,901,376 bytes；
WSL 唯一持有推論 GPU 裝置的 Python 是 Discord PID 1708813。
Windows 的各程序記憶體計數可能重疊，不可相加成整卡用量；WSL 的
`nvidia-smi` 未列出程序也不能作為沒有 CUDA 所有者的證據。

Discord 當次收據顯示六模式在 08:08:32–08:10:15 完成暖機，模型常駐旗標
為 true，當下佇列收據沒有進行中的互動推論、開盤工作或背景維護；
日誌仍反覆出現每五秒一次的六模式開盤資料重試。11:52 的八秒取樣仍有
兩個 `/dev/dxg` handle，RSS 4,915,146,752 bytes，平均使用 0.512 CPU core。

共用交易日判斷把 10/23 分類為一般平日，導致原有 08:05–13:30 模型保護
窗口生效。但這天是已公告的臨時全日休市：

- [土地銀行證券 10/22 公告](https://stock.landbank.com.tw/Index_CH/Index_Detail.aspx?ct=1&id=P0066&n=P0054&open=5025)。
- [兆豐證券 10/22 公告](https://www.emega.com.tw/emegaTran/bulletin.do?id=20261022173442560596)。

原始土銀 HTML SHA256 為
`83d8748d22e5bbc5cc914ae52f4776ee9086b3580a3f9dd2672f9ed434e709be`，
下載時間與長度見 `emergency_closure_evidence.json`。

既有每半小時日曆更新正常執行；
[TWSE 年度日曆 API](https://openapi.twse.com.tw/v1/holidaySchedule/holidaySchedule)
沒有這次臨時休市。相同 payload 去重後仍保留 9/16 快照；不能把更新
成功當成已涵蓋臨時公告，也不能偽造一列 TWSE API 來源。

因此只在六個目前啟用的 TW 當沖 `LiveMarketConfig.holidays` 加入
`2026-10-23`。暖機、GPU 釋放和執行端沿用同一交易日入口，既有日曆
與原始行情不變。執行端原生重讀設定；Discord 的設定為程序內快取，
需經既有 qualified reload 入口重載一次，才會讀到新日期。

本次重用原本已釘選的程式 release，不把工作目錄其他程式版本帶入服務。
reload 僅選 Discord；控制租約、現有工作、broker-disabled、精確 engine ACK、
帳本 prefix 與其他服務 owner 都由原工具核對。

## 驗證與操作

`calendar_after_config.json` 對六模式各核對 10/22、10/23、10/24、
10/26、10/27，共 30 個交易日分類：前一實際交易日不變，10/23 為設定休市，
週末與 10/26 原定假日休市，10/27 仍為預定交易日。
`configured_scheduler_acceptance.json` 另在 08:05、09:00、11:00、13:00
確認六模式停止暖機並允許原生 idle release；沒有載入模型或初始化 CUDA。

回歸涵蓋交易日判斷、暖機／釋放邊界、工作中的 producer／mutex 保護、
allocator 與 reload。第一輪 327 passed／12 setup errors：測試 fixture
仍引用已刪除的 `_LIVE_PANEL_SOURCE_KEY_CACHE`。移除該不存在的測試項目，
不改生產釋放函式；修正後 **338 passed in 31.47s**，完整指令含啟動與
匯入為 35.027 秒。兩輪結果均留在 agent workflow run 收據。

`discord_reload_plan.json` 只有 Discord 在清單內，未平倉與實際待執行
訂單均為零。第一次 apply 因其他 coordinator 持有控制鎖而拒絕，沒有重啟；
原失敗收據保留。第二次用既有有界等待取得租約，12:01:07 重載 Discord，
12:01:46 完成 `accepted_code_admission_only`，見
`discord_reload_apply_retry.json`。沒有更新程式 release，也沒有重啟執行引擎。

新 InvocationID `99ccca9612284564bd534de71d37ae1d`，Python PID 3466123；
`native_acceptance.json` 從此程序的實際 mount namespace 讀回六個設定 SHA，
確認今日休市、Gateway／commands 正常、精確 engine ACK lag=0、訊號 ID／
loaded code 不變、無新 warm-ready／fatal，GPU device handles=0。
原 events／orders／fills 前綴與完整長度一致，operator ledger writes=0。

| 同機短時間觀察 | 修改前 | 採用後 |
| --- | --- | --- |
| 整卡 VRAM 使用量（median） | 15,048 MiB | 3,115 MiB |
| Discord RSS | 4.58 GiB | 0.76 GiB |
| Discord 平均 CPU（1.0 = 一核心） | 0.512 | 0.0161 |
| Discord GPU device handles | 2 | 0 |

整卡釋放約 **11.65 GiB（79.3%）**；RSS 減少 83.4%，短樣本平均 CPU
減少 96.9%。前後 CPU 觀察各為 8.01／8.70 秒，整卡 VRAM 各為 3／8 筆；
原始資料與計算見 `baseline*.json`、`after_discord_process.json` 和
`resource_comparison.json`。這不是獨佔主機的控制實驗；背景工作與使用者
Windows 桌面仍在變動，不能將全主機效益或板卡功耗差異全部歸因於本次修正。

兩次 Linux managed-process FD 觀察分別完整讀取 100／100 與 58／58 個
程序，GPU owner／handles 均為零；這些是當時取樣，不是 Windows 全部
分配的精確歸戶。兩筆 WDDM 採用後觀察未列出原 vmwp 大額記憶體，
仍有 dwm／Sunshine 等桌面與串流用量；各程序 counter 不可相加。

qualified reload 的 protected owners 在其控制區間前後一致。較長的
11:49–12:04 觀察區間中，public dashboard 在重載區間外自行更換 owner，
見 `concurrent_public_dashboard_owner.log`；不宣稱整段觀察所有服務都沒變。
執行引擎、TAIFEX bidask／歷史資料、FinMind complement 與 TAIFEX dashboard
的初始／最終 owner 相同。

往後臨時休市仍須先核對公告，再更新目前啟用模式的既有 `holidays`，
驗證相鄰交易日並依同一 qualified Discord-only 流程採用。新的公告還沒有
自動匯入；週末、年度假日和原生 idle release 則沿用現有排程。
Windows 桌面、遠端畫面編碼及使用者應用程式另有 GPU 用量。

## 2026-10-25 凌晨實查：目前資源與 Windows 查詢邊界

本次以原 canonical audit 程式重新讀取 penguin／WSL2，沒有重載服務、
初始化 CUDA、啟動訓練或呼叫 provider。兩筆整卡讀值為：

| UTC 觀察時間（台北為 10/25 凌晨） | VRAM MiB | GPU 使用率 | 板卡瞬時功耗 W |
| --- | ---: | ---: | ---: |
| 10/24 16:44:39 | 3,992 | 2% | 41.56 |
| 10/24 16:45:41 | 4,026 | 6% | 48.37 |

16:45:54 的原 Linux managed-process audit 可見 191 個 `/proc` candidates，
其中 **99／99 個 StockAgent service 程序**的身分與 FD 讀取完整，GPU owners
及裝置 handles 均為 **0**。這是該次受管 Linux 程序的樣本，不含未受管
Linux 程序或所有 Windows allocations，也不代表未來暖機或 CUDA readiness。

Windows 歸屬 audit 的兩次查詢均為 **unavailable／TimeoutExpired**，分別
30.177344／30.261817 秒；沒有發布空 owner 清單作為成功歸戶。整個 audit
child 為 **63.630566 秒 wall／1.829150 秒 WSL reaped-child CPU**。Linux FD
audit child 為 **0.577643 秒 wall／0.398086 秒 CPU**；WSL rusage 不涵蓋
Windows PowerShell 本體的 CPU。兩個 run 的 succeeded 只表示收據已保存，
不把 Windows 查詢失敗轉為 ownership 或 VRAM 的完整證明。

R153 再用相同明確 caller socket 與原 `_query_environment`，執行一個只輸出
固定 literal 的 Windows console 控制測試，**沒有執行 GPU CIM queries**。
5 秒 deadline 亦得到 TimeoutExpired，實際等待 **5.349162 秒**。這顯示純
console roundtrip 的傳輸／PowerShell 啟動也存在未解延遲，不能僅由先前兩筆
30 秒結果判定 GPU query 內哪個步驟慢，亦不據此修改 CIM 收集語意。
Windows 分配仍未知；保留相同 endpoint，沒有自動搜尋或替換 socket。

採樣時 R142 第二組的第一個 candidate 仍在執行；這些是額外的低負載觀測，
未改其凍結程式、固定輸入、順序或門檻。原 10/23 修復前後比例保留為該次
歷史結果，不與這次不同 boot／桌面狀態的讀值組成新的受控加速比例。

來源 SHA：Windows audit `3272ddaa…`、Linux FD audit `acaf9300…`，既有程式
未修改。WDDM 收據 SHA 為 `d92b1738…`；Linux FD 收據為 `d7b14a06…`。

- [兩筆整卡資料與 Windows 原逾時收據](../artifacts/operations/gpu_idle_load_20261025/current_20261024T164400_wddm.json)
- [99 個 managed processes 的完整 FD 觀察](../artifacts/operations/gpu_idle_load_20261025/current_20261024T164400_linux_handles.json)
- [原 Windows 稽核命令、clock 與終態](../artifacts/operations/agent-workflow/runs/workflow-r151-gpu-idle-current-o-20261024T164432-d927d323/run.json)
- [原 Linux FD 稽核命令、clock 與終態](../artifacts/operations/agent-workflow/runs/workflow-r152-gpu-idle-managed-h-20261024T164548-6f4af457/run.json)
- [純 console 傳輸控制程式](../artifacts/benchmarks/workflow_speed_optimization_20261021/round153/diagnose_windows_gpu_transport.py)
- [原 console deadline 結果](../artifacts/operations/gpu_idle_load_20261025/current_20261024T164400_transport_only.json)
- [console 控制的 supervisor 終態](../artifacts/operations/agent-workflow/runs/workflow-r153-gpu-windows-consol-20261024T165000-1a055ac9/run.json)

## 2026-10-25 續查：完整可見 Linux 程序與唯一 handle 持有者

17:55 UTC 的兩次全 Linux 範圍樣本，完整觀察 **183／183、187／187 個
可見程序**。其中 StockAgent managed 都為 89 個，unmanaged 分別為 94／98
個，unknown rows 均為 0；兩次各只有一個 GPU owner／一個 `/dev/dxg` handle。
這補齊前段僅觀察 managed processes 的範圍，沒有推翻較早 99／99 個 managed
程序 handles=0 的原始結果。

同一個持有者為 **PID 507606／start ticks 1741555**。18:01:59 UTC 再以
before／after process identity 與 FD 核對，確認 comm=`nvtop`、執行檔
`/usr/bin/nvtop`、cgroup=`/init.scope`、parent PID=507445、FD 3=`/dev/dxg`。
未擷取 command line 或其他私密 FD 目標，也沒有停止或重設該監看程序。
身分讀回區段為 0.001187186 秒，僅涵蓋指定 proc 欄位及身分檢查，不含
Python 啟動與收據寫出。**FD 持有者不等於 VRAM bytes 的完整歸戶**；仍無
證據把整卡用量全部歸給 nvtop，Windows allocations 亦未驗證。

兩筆整卡 NVML 讀值如下，與 Linux FD 樣本為順序觀察，並非同一瞬間總帳：

| UTC | VRAM MiB | GPU 使用率 | 瞬時功耗 W |
| --- | ---: | ---: | ---: |
| 17:55:22.982 | 4,539 | 10% | 44.81 |
| 17:55:25.491 | 4,562 | 12% | 50.82 |

同一 canonical `audit_managed_gpu_handles.py` 新增明確的
`--include-unmanaged` 選項。預設 managed v1 範圍與欄位保持原行為，擴大
範圍另用 `stockagent_linux_visible_gpu_fd_sample_v1`；初始身分讀取失敗、
PID／cgroup 變動或 FD 不可讀都保留 unknown／partial，不能當成零 handle。
device targets 仍限原 allowlist，沒有建立第二支 GPU 掃描器。
R156 **16 tests／0 failures／0 errors／0 skips**，完整 child
**2.159053 秒 wall／1.951447 秒 reaped-child CPU**，且實際 inactive／PID 0。
目前 Linux audit SHA=`45b14a6c…`、test SHA=`4a09d1d3…`；前段舊收據與
`acaf9300…` 來源不變。

R157 由原 systemd workflow owner 執行兩次 CLI，第三個 fresh process 完整
重讀兩份原始 JSON、核對全部 PID、selected／observed／unknown 分母、managed
分類、每個已報告的 GPU FD 及 allowlist，通過後 succeeded／exit 0、inactive／PID 0。
source、tests 及原 R156 owner receipt 的完整 bytes／SHA 均封存。各區段時鐘為：

| 區段 | 完整呼叫 wall 秒 | 該呼叫 reaped-child CPU 秒 |
| --- | ---: | ---: |
| NVML before | 0.986151 | 0.305943 |
| 全可見 Linux CLI 1 | 0.558898 | 0.305008 |
| 全可見 Linux CLI 2 | 0.980600 | 0.510319 |
| NVML after | 0.910420 | 0.709821 |
| Fresh reader | 0.342549 | 0.257935 |
| R157 完整 child | 5.770836 | 3.316562 |

parent process CPU ns 另保留在各 clock 收據；完整 child 包含該 parent 與
其子程序，不能再與分段相加。這些是觀測與驗證成本，沒有量出 nvtop 本身
的 CPU／GPU 額外負載，也不是新的省電或整體加速對照。
WSL caller endpoint 仍為 `/run/WSL/355255_interop`，實際仍是 socket，relay
亦存活；本次未重試先前相同的 Windows console／CIM 逾時查詢。
R156 與第七次 baseline、R157 與最後一次 candidate 同時存在，並保留此條件；
未修改 R142 capsule、固定輸入或門檻，也沒有 provider／CUDA compute 工作。

- [Canonical Linux FD audit](../scripts/audit_managed_gpu_handles.py)
- [預設與擴大範圍、unknown／privacy 語意測試](../test/test_audit_managed_gpu_handles.py)
- [R156 16 項完整測試與時鐘](../artifacts/operations/agent-workflow/runs/workflow-r156-visible-gpu-fd-che-20261024T174900-2c1df781/run.json)
- [R157 精確來源、已驗證原測試與觀測 protocol](../artifacts/benchmarks/workflow_speed_optimization_20261021/round157/protocol.json)
- [183 個全可見程序原始樣本](../artifacts/operations/gpu_idle_load_20261025/visible_linux_20261024T175520_0.json)
- [187 個全可見程序原始樣本](../artifacts/operations/gpu_idle_load_20261025/visible_linux_20261024T175520_1.json)
- [兩份完整原始樣本的 fresh reader](../artifacts/benchmarks/workflow_speed_optimization_20261021/round157/readback.json)
- [同次整卡 NVML 原始讀值](../artifacts/benchmarks/workflow_speed_optimization_20261021/round157/board.json)
- [nvtop PID 身分、啟動 ticks 與 FD 獨立讀回](../artifacts/operations/gpu_idle_load_20261025/owner_identity_20261024T180159.json)
- [R157 全命令、完整時鐘與終態](../artifacts/operations/agent-workflow/runs/workflow-r157-visible-gpu-owner--20261024T175512-fd09b9ff/run.json)
