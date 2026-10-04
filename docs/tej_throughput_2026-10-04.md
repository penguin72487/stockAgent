# TEJ 下載吞吐量、並行可行性與實機接受

日期：2026-10-04（Asia/Taipei）。這是有界工程接受，不是全歷史下載完成宣告。

## 結論

**獨立實例有並行的可能；目前不能讓多個 worker 同時操作同一個 Smart Wizard。**
現有來源選單、公司／日期／欄位與 Preview 都屬於同一個查詢視窗。後一個工作改動
條件或結果，會使前一個工作的來源證據失效。這是目前的實作與共享狀態問題，
不是已證明的「TEJ 官方只允許一個工作」。

本日分兩輪：第一輪改善完整讀回；第二輪實測同一來源的操作成本、Windows 啟動
恢復與獨立空 Excel 實例，詳見文末。沒有啟動第二個 TEJ 查詢通道或調高來源頻率，
也沒有縮減欄位／歷史範圍。既有 API 與 Wizard 分工、價值排序與未知結果保護均保留。

## 從耗時組成定位瓶頸

一次完整工作的成本是：

`T = 啟動與來源條件準備 + 來源回覆 + 全量讀回 + 驗證／入庫`

多開只可能改善可獨立重疊的部分；不能把同一個 UI 執行緒及同一份可變 Preview
當成彼此獨立的資料來源，也不能把一次桌面查詢當成一次 provider HTTP request。

修改前的 60 次完整下載工作使用 SQL 索引與實際 `fresh_end_to_end` 計時：

| 指標 | 實測 |
| --- | --- |
| 非空工作 | 33 次；完整耗時中位數 35.72 秒 |
| 明確空回工作 | 27 次；完整耗時中位數 21.44 秒 |
| 提交查詢前的耗時中位數 | 18.00 秒 |
| 樣本分片列數 | 54,047 列 |

這些是不同來源／範圍的回顧樣本，不能拿後來另一張表的耗時直接宣稱加速比。
目前 2 秒限制是**查詢開始間隔**；一個工作已超過 2 秒時，不再額外等 2 秒。
本次沒有再修改這項既有排程。

## 已部署的讀回改善

`scripts/tej_smart_wizard_bridge.ps1` 新增
`shared_rows_batched_msaa_preview_full_v1`：

- 原本獨立樣本與完整讀回各枚舉一次，加上讀完後驗證，共三次完整列枚舉。
  新版共用第一次列物件，保留第二次完整枚舉作為結構穩定性驗證。
- 透過 [Microsoft AccessibleChildren](https://learn.microsoft.com/en-us/windows/win32/api/oleacc/nf-oleacc-accessiblechildren)
  批次取得列物件；仍逐一驗證每個 child 的角色、數量與界限，不採納部分枚舉。
- 每一格仍完整讀取；保留獨立 object-path 樣本、欄位／鍵／來源順序核對、
  字串容量界限、已知 HRESULT 的有限 fallback，以及最後的全量結構檢查。
- 釋放批次列物件及異常路徑的 COM references。逐列進度仍每兩秒有界發布，
  不新增逐格磁碟寫入。
- 原始結果增加枚舉、樣本與完整值／穩定性檢查的分段耗時；不新增下載框架。

來源／值合約仍是 v4、`editable_source_scope_v1`、
`native_msaa_preview_full`、`vendor_display_strings_not_underlying_excel_values`。
沒有新增 log、換算、四捨五入或跳過 null。**這仍是來源顯示字串，不因此宣稱為
Excel 底層完整精度或已驗證的歷史發布時間。** 原始檔、Parquet、收據與未知結果
不得重送的 barrier 均保留。

## 同一份實際 TEJ 結果的完整對照

基準 bridge 保留在私有 `data_tej/performance/throughput_2026-10-04/`，SHA：
`ae09071185786ad02f796fdfdc6f5f70cb9dbe6309b66101e8880bb9009023ff`。
候選 SHA：`eebbefacb5e08d7d81369c7bcd60940aa95b970fc6454ad809acb66f5cf33dab`。

對照的是已成功保存的 `Qfii/Dealer/Invest. Range Buy/Sell` 結果：
5,270 資料列、26 欄；含表頭共 137,046 格。取得既有 dataset lock 與 Windows
owner mutex 後，執行 AB／BA 四次完整讀回，逐格與原始保存結果比較，並核對
獨立樣本及前後來源 signature。**沒有送任何新查詢、採納結果、改隊列、切頁、
剪貼簿操作或變更前景。**

| 順序 | 舊版完整讀回 | 新版完整讀回 |
| --- | --- | --- |
| A→B | 62.31 秒 | 64.98 秒 |
| B→A | 86.57 秒 | 54.30 秒 |
| 兩次中位數 | 74.44 秒 | 59.64 秒 |

全部格與樣本一致、fallback 0、來源 signature 不變；中位數少約 **19.9%**。
但第一輪新版較慢、第二輪較快，波動明顯，只有一張來源表、兩次每方法量測。
這是讀回組件的有界結果，**不是穩定全流程加速 20%、官方最大吞吐量，或全表
等價證明**。全域 ETA 仍使用實際完整工作計時，不直接乘上這個比率。

自建不搶焦點 Windows fixture 另覆蓋負值、零、null、Unicode、引號與超過 double
精度的字串。它只證明軟體讀回路徑，不能替代上述 TEJ 來源接受。

## 部署、收據與網站接受

驗證完成後於 **00:20:32（臺北）**恢復 `stockagent-tej-history.service`，
沒有第二個 collector。新版首次實際成功工作於 **00:21:03**入庫：

- `FHC IFRS_Parent Financial_Security(Acc)-4`，task `514fba58f7e70c91dc7092b0`。
- 1,083 列、30,324 個非空特徵值；30 欄；原始結果記錄新讀回 contract。
- 枚舉 2.290 秒、樣本 0.437 秒、完整值／結構檢查 7.381 秒。
  這是新工作的分段耗時，不與另一個範圍混作同條件加速比。

**00:22:03 的收據快照**接受 454 次已完成下載工作、280,558 分片列、
5,475,191 個非空特徵值；原始／Parquet SHA、來源 schema／鍵、列數與欄位計數
不一致均為 **0**。明確空回也有收據，工作數不等於完整歷史數。
前後快照的 blocked 清單未新增；既有 19 個延期／失敗工作仍保留，不冒充完成。

TEJ 範圍的 **1,431 項 pytest 通過**。公開 [TEJ 頁](https://penguin72487.ddnsgeek.com/tej/)
桌面／手機 rendered acceptance 通過，使用頁面實際收到的同一份來源狀態核對。
隔離的瀏覽器回應 fixture 另驗證每五秒更新、讀回不冒充入庫、HTTP 錯誤保留前次
狀態以及 filter／focus 保留；fixture 不是一次真實 TEJ 下載。

## 並行路徑的條件與限制

| 路徑 | 本次判斷 |
| --- | --- |
| 同一 Wizard 多 worker | 共享條件與 Preview，不能安全並行；現有 Linux locks 與 Windows mutex 保留 |
| 同 Excel 的不同工作簿／Wizard | 尚未證明外掛 session／全域狀態隔離，不能只依不同工作簿名稱認定獨立 |
| 不同 Excel PID 的獨立實例 | 可以作為後續候選；先確認帳號並行政策、獨立登入／來源狀態、PID／HWND／工作簿 owner、工作 claim 與各自的未知結果 barrier，再作完整對照 |
| TEJ REST API | 可避開逐格桌面讀回；需要實際 API key、表權限與配額，不能從 Smart Wizard 登入直接推定 |

`desktop_parallelism: 1` 目前不是建立 worker pool 的參數；真正的序列執行是現有
watch loop 與所有權鎖。僅改成 2 不會新增安全通道。現在的 10,000 列、400,000 格、
128 公司是**本機已核對的有界設定**；Preview 的 30 總欄位須扣除每表 1／2／3 個鍵。
[Smart Wizard 官方手冊](https://www.tej.com.tw/TEJPLUS/TEJE3_TCHINESE.pdf)有 Preview／Excel
匯出說明，但本次查閱沒有取得目前帳號的桌面並行與每日流量上限，不能將未知
填成無限，也不能將本機設定說成官方上限。

**較早「尚未設定 API key」的檢查已不適用目前狀態。**第二輪沿用既有 API 管線，
只呼叫一次 key-info metadata，沒有新增資料查詢，也沒有輸出金鑰。09:36:45（臺北）
官方回報 `todayRows=50000`、`todayReqCount=73`、`multiConn=false`；試用帳號每日
50,000 筆、500 次，每頁 10,000 筆、單查詢分頁總量 50,000 筆。資料額度已耗盡，
不能透過多開增加額度；`multiConn` 不足以推導桌面外掛的授權或多實例政策。
[TEJ REST 文件](https://api.tej.com.tw/document_rest.html)提供 key 資訊與用量查詢，
實際限制以目前帳號回應為準，不能把 API 配額套用到 Smart Wizard。
現有 `.api.lock` 與 `.download.lock` 是分開的，可在各自權限／額度允許時運行；
本次沒有觀察到配額重置，因此也不宣稱已驗證重置後抓取。

## 可重跑的證據與工具

- [完整工作計時基準](../artifacts/data_quality/tej_throughput_2026-10-04/profile_before_v1.json)
- [同結果全格對照](../artifacts/data_quality/tej_throughput_2026-10-04/same_source_readback_v2.json)
- [自建 Windows fixture](../artifacts/data_quality/tej_throughput_2026-10-04/owned_readback_v1.json)
- [部署後收據接受](../artifacts/data_quality/tej_throughput_2026-10-04/receipt_audit_after_v1/audit.json)
- [公開頁 rendered 接受](../artifacts/data_quality/tej_throughput_2026-10-04/public_browser_v1/rendered_acceptance.json)
- `scripts/profile_tej_acquisition.py`：有界唯讀 SQL snapshot，不呼叫來源。
- `scripts/benchmark_tej_preview_readback.py`／`.ps1`：需明確選擇 fixture，或一個精確
  已完成非空 task；須停止 collector 並確認最後一個工作正常結束後才使用來源模式。
  不處理未知結果、不覆寫證據；報告只有計數、雜湊與耗時，不輸出原始值。
- 工程 task：`tej-throughput-20261004`。第一次 source benchmark 的預設雙鍵判斷
  與測試中錯把 assembly reference 當作按鍵輸入的問題已修正；失敗 run 證據保留，
  不冒充成功來源查詢。

## 第二輪：操作成本、並行測試與阻塞恢復

本節的工程 task 是 `tej-operation-throughput-20261004`。範圍是既有下載器的操作
吞吐量與安全恢復，不是承諾全歷史已完成或所有阻塞都能無條件重送。

### 起點與根因

本輪修改前最近 60 筆 `fresh_end_to_end` 完整工作：31 筆非空、29 筆明確空回，
共 322 分片列，完整耗時中位數 **31.28 秒**；送出前中位數 **29.09 秒**。
瓶頸主要在準備來源／公司／日期／欄位，而不是兩秒的查詢起始間隔。
這一批與前文第一輪樣本不同，不能混用分母。

當時唯一未知阻塞 task `1ad12432f0412802a399fa24` 在 Windows 互通啟動出現
`UtilAcceptVsock:273: accept4 failed 110`；沒有留下 stage／結果／進度。
**沒有檔案不等於沒送出查詢。**依使用者只對此筆的明確重排授權，保留原 unknown
attempt、原請求與診斷雜湊，產生一次性 operator authorization；新 attempt 於
09:43:01 完成，11 列。沒有把此例擴大為所有未知結果的自動重送許可。

### 已實作的操作改善

- 同一批選取只完整定位 Select 按鈕一次；每次操作仍重新驗證 PID、視窗、父子
  關係、類別、精確名稱、enabled／visible，最後再檢查控制項身份未變。
- 來源選單只讀目前選取文字，精確查找用原生 ComboBox 查詢，不為驗證單一
  binding 一再枚舉整張選單。保留大小寫、Unicode、重複標籤拒絕與完整欄位 schema。
  原生精確搜尋大小寫不敏感，另外加上 Ordinal 完整比對，不能用字首或相似字代替。
- 全公司／全日期查詢省掉 Select All 前的逐項重複 lookup；操作後仍完整核對
  每一個選取名稱／順序，數量相同但名字錯誤一樣拒絕。部分範圍維持逐項精確定位。
- 增加七段單調時鐘計時，無新增 GUI 檢查或逐格磁碟寫入；計時不會延長工作 deadline。
  唯讀 profile 報告升為 `tej_bounded_acquisition_timing_profile_v2_scope_stages`，
  僅輸出固定七段的有限非負數值，未知 key／文字／無限值均不公開。

實際同一個已保存的 `TDR Cash Dividends` 來源，AB／BA 對照：

| 組件 | 舊版中位數 | 新版中位數 | 接受範圍 |
| --- | --- | --- | --- |
| 控制項解析，24 輪 × 3 個 Select | 2.726 秒 | 0.951 秒 | 少約 65%；fresh ownership guards 保留 |
| 全軸重複 lookup／完整名稱驗證，37 公司、270 日期 | 1.349 秒 | 0.098 秒 | 少約 93%；沒有跳過完整目的地名稱核對 |
| 36 項小型來源選單，讀取目前項目＋精確 lookup | 0.0158 秒 | 0.0064 秒 | 絕對差很小且樣本少，不宣稱全流程收益 |

零新來源查詢、零選取操作、零隊列寫入；來源 schema／軸／前景與前後程式雜湊
均未變。這是唯讀組件測試，**不是 TEJ 官方最大吞吐量或整體加速 65%／93%**。

### 實際多程序與多實例測試

持有原 Wizard owner 時，真正啟動第二個 Windows PowerShell worker；它在接觸 UI
之前被 owner mutex 拒絕。這證明競爭不會同時修改條件，不代表雙抓性能測試成功。

另外實際建立獨立 PID 的隱藏、空 Excel 實例，結束後只關閉本次新建空實例。
原 Excel／Book2／Wizard 與既有 TEJ 外掛程序身份不變；沒有新登入或資料查詢。
新實例沒有已連線的 TEJ **COM** add-in，外部 TEJ Addin 程序仍只有一個；
尚未證明 legacy 外掛、獨立 Wizard session、來源狀態及授權隔離。
**所以可以多開 Excel，但本次不能據此安全啟用多個 TEJ 桌面 downloader。**

### 阻塞與自動恢復的邊界

新增 `downloader/tej_windows_transport.py`，仍由原 DesktopBridge 呼叫：

1. Windows helper 先寫入綁定請求 SHA 與唯一 token 的 readiness ACK；未取得
   controller 發出的 durable permit 前，不可執行任何 TEJ／Excel bridge 操作。
2. 優先使用 WSL instance relay；未授權啟動失敗時，才嘗試另一個既有 relay。
   每次等待有界，未拿到 permit 的 helper 會自行退出。
3. 兩條 relay 都在 permit 前失敗，保存可驗證的 negative proof，工作保持 pending、
   60 秒後再試，不消耗選取程式錯誤的 retry 預算，也不留下全球 unknown barrier。
4. **一旦 permit 已發出**，錯誤／timeout 必須保留 unknown outcome，不能假裝
   查詢未送出或自動啟動第二份。同樣沿用已有的 saved-result adoption／attempt reconciliation。

真正 Windows 無來源故障注入四種情況均通過：第一 helper 失敗後 fallback 成功；
兩次未授權失敗；授權後錯誤；授權後 timeout。後兩種都只有一次 launch／permit，
沒有重送。這是 transport fixture，不能冒充 TEJ provider 的實際超時接受。
Windows 軟體 fixture 53 項通過，包括錯 PID／父視窗／名稱、Unicode、大小寫、
重複選單標籤、相同數量不同軸標籤，以及不搶前景的控制項操作。

失敗候選 `transport_fault_injection_v1` 沒有成功誘發指定 relay 故障，
`control_resolution_v4` 與較早 list fixture 前景穩定性不通過，證據均保留；
不將它們算成成功或未有證據地歸因於使用者操作。

### 部署後的完整工作與收據

最終新版於 **10:08:46** 啟動 canonical `stockagent-tej-history.service`，只有一個
桌面 collector。10:14:13 的最後十筆全部含新版七段計時，5 筆非空、5 筆明確空回，
28 列，完整耗時中位數 **25.50 秒**。來源與範圍混合、負載會波動，不能與修改前
不同樣本計算因果加速比。七段揭露公司 universe／lookup 約 2–16 秒、來源 binding
約 2–13 秒，外掛重新載入仍是重要成本，沒有刪除必要的來源 freshness 證明來換速度。

**10:14:35 scoped receipt audit**：最終部署後 15 筆加上上述授權恢復 1 筆，
16 筆全部接受；58 分片列、1,369 非空值；原始／Parquet SHA、來源鍵、schema、
列數、欄位計數、attempt SHA binding 不一致為 **0**。這是指定工作批次，沒有將
全域 registry 分母拿來比較局部批次，也不是全庫資料品質／歷史 completeness 保證。

10:10 的公開 [TEJ 頁](https://penguin72487.ddnsgeek.com/tej/)桌面／手機渲染接受，
實際顯示「歷史資料下載中」，提交後的新收據與五秒 polling 連動，無 JavaScript 錯誤或
橫向溢出；瀏覽器不發 provider requests。既有未解來源隔離項目仍保留，沒有冒充完成。
當時全域 ETA 仍為多年級，待排查詢達數百萬且還有未量測表；這一輪操作改善
不能據以宣稱很快全部抓完，網站仍依完整工作實測重估。

最終範圍回歸 `run_fintech_python -m pytest -q test/test_tej*.py`：**1,525 項通過**，
39.34 秒，監督 run `tej-operation-regression-release-20261004T021451-5df4b1ee`
於 10:15:32 成功結束。沒有跑不相關訓練或變更其他 provider。

### 第二輪驗收證據

- [本輪完整工作基準](../artifacts/data_quality/tej_operation_throughput_2026-10-04/profile_before_v1.json)
- [同來源操作 AB／BA 與雙程序 owner 接受](../artifacts/data_quality/tej_operation_throughput_2026-10-04/control_resolution_v5.json)
- [獨立空 Excel 實例 probe](../artifacts/data_quality/tej_operation_throughput_2026-10-04/excel_instance_probe_v1.json)
- [Windows launch 故障注入](../artifacts/data_quality/tej_operation_throughput_2026-10-04/transport_fault_injection_v2.json)
- [53 項控制項 fixture](../artifacts/data_quality/tej_operation_throughput_2026-10-04/list_fixture_v7.json)
- [API 官方使用量 probe](../artifacts/data_quality/tej_operation_throughput_2026-10-04/api_usage_probe_v1.json)
- [最終部署後十筆完整計時](../artifacts/data_quality/tej_operation_throughput_2026-10-04/profile_after_final_v1.json)
- [本次16筆收據稽核](../artifacts/data_quality/tej_operation_throughput_2026-10-04/receipt_audit_final_v1/audit.json)
- [公開網頁接受](../artifacts/data_quality/tej_operation_throughput_2026-10-04/public_browser_v1/rendered_acceptance.json)
- [最終範圍回歸 run 收據](../artifacts/operations/agent-workflow/runs/tej-operation-regression-release-20261004T021451-5df4b1ee/run.json)

重跑工具：`benchmark_tej_selection_controls.py` 需要精確已完成 task、來源結果雜湊、
排他 owner 與閒置 collector；`verify_tej_windows_transport.py` 僅執行無來源
Windows fixture；`probe_tej_excel_instances.ps1` 僅建立並關閉本次新建空實例。
全部沿用現有服務／收據管線，不另建下載排程或略過來源驗證。
