# TEJ 操作全流程加速：同範圍實測與部署

日期：2026-10-05，Asia/Taipei。工程任務：`tej-full-flow-speed-20261005`。

後續針對 6,544 列大表的讀回最佳化與完整驗證，見
[TEJ 大表讀回加速與完整鏈路驗證](tej_large_table_speed_2026-10-05.md)。
以下保留前一輪對照與當時部署快照；不同輪次的改善百分比不能相加。

## 結論與界限

已修改既有 downloader／Smart Wizard bridge，並恢復自動下載。沒有建立第二套
collector。真實來源的完整 AB／BA 對照如下；不是只測搜尋或單次按鈕。

| 對照範圍 | 原版完整時間 | 新版完整時間 | 時間減少 |
| --- | ---: | ---: | ---: |
| 原生查找／選取批次，4 種工作各測兩輪 | 163.96 秒 | 148.64 秒 | 9.34% |
| 再加原生群組辨識／單位選單短路，同樣工作 | 164.11 秒 | 134.01 秒 | 18.34% |
| 同優先級兩表、各兩個原始範圍，連續排程與額外完整稽核 | 117.12 秒 | 88.88 秒 | 24.11% |

前兩列包含 canonical `run_one` 的排程、launch、來源操作、查詢、完整讀回、
資料驗證、Parquet 與收據；第三列另外包含逐筆獨立稽核的 wall time。
三列是各自獨立的對照，**不能把改善百分比相加**。第三列同時包含操作修正與
table locality，不能把全部 24.11% 都歸因於少換表。

來源數值／欄位／鍵與要求範圍一致。測試包含真實非空日價格結果：4 家公司、
662 個來源日期、28 個 feature、2,648 資料列、30 個總欄位；另外包括稀疏事件、
明確空回和完整來源目錄規劃。非空結果沒有抽樣替代完整讀回。

這是本機、這些實際範圍與當時負載下的改善，**不是已證明所有表最快、官方吞吐
上限或全歷史完整**。目前主要剩餘成本是大表逐格跨程序讀回。不同介面、帳號或
硬體需要重新量測；不能從本機查詢開始間隔推導廠商每日額度。

## 第一性原理：成本分解與方法選擇

完整一次取得的時間為：

`T = 排程／claim + Windows admission／初始化 + 範圍準備 + 來源回覆 + 完整讀回 + 驗證／入庫／收據`

有依賴的同一 Wizard 條件與 Preview 不能當成獨立可並行狀態。批次化優先消除
「每個元素都重做相同跨程序搜尋／列舉」；每筆動作的 owner、modal、實際名稱及
已選目的清單仍須新鮮驗證。少換表只允許同一優先級有限連續工作，不得改變資料
價值先後、到期 discovery、來源隔離、配額或未知查詢保護。

修改前最近 24 筆回顧樣本：完整工作中位 15.64 秒，送出前中位 15.13 秒。
不同工作不能拿來推導加速比；這個快照只用來定位前處理的成本。

| 操作階段 | 本次採用／測試方式 | 保留的正確性與接受方式 |
| --- | --- | --- |
| 排程與來源切換 | 同 priority 最多連抓 4 筆，再輪換；原預設 1 保留 | 更高 priority、到期 discovery、cooldown／source isolation／retry window 優先；實際 queue AB／BA |
| Linux claim／Windows launch | 沿用 canonical locks、durable admission、互動 relay 與 immutable bridge release | 不改登入／來源配額，不把未收到回應當作未送出；每次 request 綁定 bridge SHA |
| PowerShell／C# 初始化 | 保留每次新鮮初始化，新增單調時鐘計時 | 大表樣本約 0.45 秒，不以持久快取或過期 handle 犧牲 owner 邊界 |
| 群組／來源控制項辨識 | 原生 caption、class、MSAA Grouping role 定位，再取得單一 UIA 元素 | 支援 WinForms user-painted GroupBox；PID、HWND、parent、caption、唯一性重新核對 |
| 單位選單定位 | 先檢查 ComboBox 有 5 項，再讀全值比對單位選單 | 不再無意義讀完整資料庫／產業目錄；重名或不符即拒絕，沒有改變金融單位 |
| 欄位查找／選入 | `ExactListIndices`／`SelectListBatch` 單次 managed 批次 | request 重複拒絕；每次 normal notification／BM_CLICK 後核對完整目的清單與順序 |
| 公司候選／查找 | 一批精確搜尋；移除逐公司全視窗列舉 | 原生搜尋後再做 ordinal 完整名稱與重複檢查；case／Unicode 不能混淆 |
| 公司選入 | 每次原生新鮮 guard、Select normal click、完整目的清單核對 | 按鈕與 list 原生矩形核對；不用每筆 UIA 幾何遍歷；不搶焦點 |
| 日期輸入／搜尋／選入 | 沿用既有已驗證背景輸入；日期選入同樣 managed 批次 | 讀回真正來源日期，不能將假日、別的日期或來源無觀測補成資料 |
| 查詢提交／回覆 | 一次來源 Preview default action，依真實狀態回覆 | 原 prepreview 證據、fresh-result transition、未知結果 barrier 不變；沒有重複提交 fallback |
| 全量讀回 | 保留完整 batched MSAA 與獨立樣本／結構檢查；另測 UIA bulk cache | 正規來源逐值一致；bulk cache 在隔離 fixture 超時，拒絕部署 |
| 序列化、驗證、Parquet、收據 | 原 canonical 程式、hash／schema／鍵／null 計數 | 完整工作計時包含以上全部；每個測試 trial 再跑獨立 receipt audit |
| 復原／進度 | 原 watchdog、exact retained response／安全 prequery 重試／有界授權 replay | 不省略恢復，不偽造 heartbeat／完成；額外 flow telemetry 失敗不重送查詢 |

Windows 原生精確搜尋是大小寫不敏感的，不能單憑其 index 認證來源名稱，故仍保留
ordinal 讀回；參見 [Microsoft LB_FINDSTRINGEXACT](https://learn.microsoft.com/en-us/windows/win32/controls/lb-findstringexact)。
正常 [BM_CLICK](https://learn.microsoft.com/en-us/windows/win32/controls/bm-click) 有對話框／
啟用狀態限制，送出成功不等於選取成功；本版仍獨立檢查實際目的清單。

## 大表：現在的真正瓶頸

第二輪候選，同一份非空價格範圍兩次完整取得：

| 分段 | 第一次 | 第二次 |
| --- | ---: | ---: |
| 初始化／編譯 | 0.460 秒 | 0.451 秒 |
| 範圍準備 | 7.005 秒 | 6.024 秒 |
| 來源查詢／回覆 | 1.480 秒 | 1.318 秒 |
| 完整讀回 | 29.478 秒 | 25.149 秒 |
| JSON 序列化／保存 | 0.117 秒 | 0.128 秒 |
| 整筆 canonical wall time | 40.086 秒 | 34.748 秒 |

剩餘 wall time 包含其他 Windows／Linux guard、admission、scope 驗證、Parquet、
metadata 與收據。分段之和不是整筆時間；Windows process CPU 也不等於 TEJ／
Excel／來源伺服器全部 CPU。上述兩次 Windows bridge CPU 約 12.08／10.72 秒，
沒有據此宣稱全系統 CPU 降低多少。

原版同範圍兩次為 39.894／47.720 秒，個別區間有交疊；不能宣稱每次都變快。
來源回覆與系統負載波動仍存在。大表消耗隨 `rows × columns` 的跨程序完整讀取
增加；即使把前處理降到零，也無法消除這部分成本。

另測官方 [UIA CacheRequest](https://learn.microsoft.com/en-us/dotnet/api/system.windows.automation.cacherequest)
與 [CachedChildren](https://learn.microsoft.com/en-us/dotnet/api/system.windows.automation.automationelement.cachedchildren)
路徑：隔離的 64 資料列 × 30 欄 fixture，MSAA 完整讀回約 0.995 秒，而 bulk UIA
階段觸發 60 秒外部 deadline。更大 fixture 也超時；**這是候選不可接受，不是
已證明 TEJ UIA 永遠不可用或快取不可能改善**。沒有對 TEJ 來源使用這條失敗路徑，
沒有資料查詢或資料採納。工具已限制只能使用本身 owned fixture。

Excel `Value2`／官方匯出可能減少逐格互動，但會變更結果精度／值表示與來源
證據流程；本次沒有證明大範圍匯出、全值等價及完整鏈速度，不冒充已測最快。
API 有可用資料表／額度時繼續由既有 API 管線負責，不因桌面慢而重複啟動另一個
API downloader。既有 trial API 與 Smart Wizard 分工未改。

## 測試設計、證據與來源壓力

每輪用 A→B、B→A 平衡順序；同樣原始範圍／欄位，獨立私有 trial root，走
原 `run_one → DesktopBridge → validate → Parquet → receipt`。保留所有來源
request、raw、Parquet、receipt、stage、bridge release；不 reset 正式 receipt 或
cursor。測試期間等待正式在途工作完成，再以 production `.download.lock` 和
本次唯一 gate 排他測試；失敗會保留 gate／原證據，不偷偷重送可能送出的查詢。

三輪合計 **40 次額外資料查詢、10 次 metadata 規劃**。這些是有界、明確允許的
重查試驗，不是省下的呼叫數；trial 資料沒有冒充正式庫下載完成。各次 source
範圍全值相同、完整 receipt audit 接受；這仍不代表原生歷史完整或發布時間可考。

- [修改前 24 筆快照](../artifacts/data_quality/tej_full_flow_speed_2026-10-05/baseline_latest24.json)
- [第一輪完整 AB／BA](../artifacts/data_quality/tej_full_flow_speed_2026-10-05/abba-native-batches/acceptance.json)
- [第二輪完整 AB／BA](../artifacts/data_quality/tej_full_flow_speed_2026-10-05/abba-native-groups/acceptance.json)
- [實際 queue 串接 AB／BA](../artifacts/data_quality/tej_full_flow_speed_2026-10-05/abba-queue-locality/acceptance.json)
- [81 項 Windows 原生操作接受](../artifacts/data_quality/tej_full_flow_speed_2026-10-05/native_groups_fixture_v2.json)
- [bulk cache 候選拒絕證據](../artifacts/data_quality/tej_full_flow_speed_2026-10-05/bulk_uia_fixture_v5.json)

原 bridge SHA：`fe5c592d2fe0af2bf0eef243a9ae7a7ce637082b136080f0b39100abf6980086`；
最終候選：`fc1bdf963852e053ed6875a29d81898c63bfc59b79368fbea4120b82ec928ae6`。
目前 `minimum_export_interval_seconds: 2` 是本機查詢開始間隔，不是官方 RPS。
沒有減小讀取範圍、變更 raw display strings 合約、價格／單位、歷史起點、
來源配額或官方授權。Smart Wizard 帳號的精確每日／並行限制仍未知，不填成無限。

## 部署接受與日常量測

13:26:35 恢復 `stockagent-tej-history.service`。初始一次桌面暫不可用與一次
metadata busy 都經原復原流程續行，沒有刪除舊證據。13:29:16 以索引唯讀快照
選出最新 8 次全流程完成工作；SQL 選取耗時 0.0013 秒，不掃舊 request blobs。
8 筆中 5 筆非空、3 筆明確空回，51 分片資料列、1,185 非空 feature 值；全部
raw／Parquet／receipt／source schema／key／計數檢查接受。

這 8 次中位 5.64 秒、送出前中位 4.74 秒。**不是與前 24 筆同範圍比較，不用來
宣稱另一個加速倍數**；它證明部署後確實持續取得並入庫，不只 systemd active。

- [部署前狀態](../artifacts/data_quality/tej_full_flow_speed_2026-10-05/deployment_before.json)
- [部署後完整分段快照](../artifacts/data_quality/tej_full_flow_speed_2026-10-05/production_profile_latest8.json)
- [部署後 8 筆完整收據接受](../artifacts/data_quality/tej_full_flow_speed_2026-10-05/production_receipts/audit.json)
- [13:32 運行接受](../artifacts/data_quality/tej_full_flow_speed_2026-10-05/deployment_acceptance.json)：同一 bridge release／request SHA、實際互動 relay、policy=4、34 次完成工作，無新 blocked 或 source barrier。完整逐筆稽核仍以上述 8 筆為範圍，不把 34 筆工作當作歷史完整。

現行程式 TEJ 相關完整回歸 **1,712 passed、1 skipped**；來源 UI／fixtures／事件、未知結果、
runtime policy、source isolation、API ownership、queue、dashboard 均在範圍內。
最後實驗工具保護也包含在這輪回歸；另外的受影響聚焦測試 **36 passed**，面板 JS **16 passed**。
81 項 Windows fixture 包括大小寫、Unicode、duplicate、錯 owner／parent／role／
geometry、途中停用、normal destination 完整讀回與不搶焦點。這不是 81 個資料表。

日常唯讀 timing；`--output` 使用新檔名，保留前次快照：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/profile_tej_acquisition.py \
  --root data_tej --limit 24 --output artifacts/data_quality/tej_timing_NEW.json
```

每次新增 `.flow.json` 記錄 initialization、scope、query/response、full readback、
save、Windows bridge wall／CPU；profile 僅接受同一 task／attempt 的有界數值。
遺失／無效 telemetry 不影響資料採納，不重送來源、不更新假的 source heartbeat。
現有 ETA 繼續學習實際完整工作時間，不硬乘本次實驗的加速率。

全流程測試工具：`scripts/benchmark_tej_full_flow.py --help`。真實重查必須顯式
給 `--allow-source-requeries`、保留的 baseline、完成的原始 task ID，以及新證據
目錄；`--compare-queue-locality` 要求兩表各兩個原始範圍。不能將含 credentials、
query 設定或原始值的私有 trial root 公開到下載頁。
