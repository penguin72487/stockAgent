# TEJ Smart Wizard 本機匯出復原與驗收方法

本文件是維運 runbook，記錄 2026-10-01 的實際操作與有限樣本驗收。
本次已恢復 Smart Wizard 的歷史預覽、Excel 匯出及查詢設定存檔；
不代表已確認所有資料庫權限、全部歷史、無人值守下載或原始錯誤的永久修復。

## 已驗證的範圍

- 當時 Smart Wizard 版本為 `4.1.1.7`，Excel 為 64 位元、Smart Wizard 為 32 位元。
  這是環境觀察，不能單憑位元數不同判定故障原因。
- 資料庫為 `TSE/OTC Unadjusted_Price(Daily)`，公司為 `2330 TSMC`。
- 歷史樣本涵蓋 2014-01-02、01-03、01-06、01-07、01-08，合計 5 筆日資料、
  6 個數值欄位、30 個數值；Excel 實際儲存格與匯出前 Preview 逐格一致。
- Excel 擷取時間為 `2026-10-01T10:18:35.5251093Z`，即台北時間
  2026-10-01 18:18:35。這是取得時間，不是歷史發布時間。
- 先前的 ActiveX 與 File Manager .NET 錯誤本次未再出現，但完整根因尚未證實。

完整證據見 [復原收據](../artifacts/data_quality/tej_smart_wizard_repair_2026-10-01/repair_result.json)、
[逐格驗收收據](../artifacts/data_quality/tej_smart_wizard_repair_2026-10-01/verified_export/verification.json)
與 [樣本 CSV](../artifacts/data_quality/tej_smart_wizard_repair_2026-10-01/verified_export/historical_sample.csv)。
這些是本機驗收產物；Git 文件存在不代表其他主機已取得產物。

## 復原查詢視窗

1. 保留現有 Excel、使用者工作簿及 TEJ Pro。先確認正在操作的是自己的測試查詢。
2. 若匯出後查詢被最小化，或控制項雖然「可見」卻移到畫面之外，
   不要沿用舊座標、PID 或 HWND，也不要把按鍵送出當作操作成功。
3. 對自己的測試查詢使用正常關閉按鈕。若出現
   `Had been setting conditions. Do you close form?`，只有確認是可放棄的測試設定才選 Yes；
   不能擅自關掉使用者尚未保存的查詢。
4. 回到 Excel 的 TEJ 工具列，由 `Database Settings` 重新開啟 Smart Wizard。
   本次既有登入仍有效，不需要重啟 TEJ Pro。
5. 重新取得視窗與控制項。操作前檢查前景視窗、目標所屬程序、焦點、
   實際螢幕座標及點擊命中對象；任何一項不符就停止，不對其他程式送鍵。

`IsWindowVisible` 不保證控制項實際在畫面內。匯出也可能清空 Preview 或改變視窗狀態，
所以不能只用舊的視窗 handle 或「訊息已送出」作為完成證據。

## 選擇日期公司與欄位

日期輸入是有遮罩的 WinForms 控制項。本次直接寫入文字曾讓畫面看似正確，
但候選日期沒有更新。成功方法是讓日期框取得真實焦點，再用鍵盤
`Ctrl+A → Backspace → yyyyMMdd → Tab`，最後按 Search。

對上述歷史樣本，起訖輸入 `20140102`、`20140108`。Search 後必須讀回候選日期，
確認只有 01-02、01-03、01-06、01-07、01-08；Select All 後再確認已選日期相同。
不得把假日補成觀測，也不能因日期輸入失敗就斷言來源沒有該年份。

選擇 `LISTED&DELISTED`、`TEJEquity`、`TSE/OTC Unadjusted_Price(Daily)`。
對欄位清單使用實際可見選取並按 Select，讀回已選清單，確認以下六欄全部存在：

| 原始欄位 | 來源單位 | 匯入欄位及換算 |
| --- | --- | --- |
| `Open(NTD)` | 元 | `open_twd`，保持來源值 |
| `High(NTD)` | 元 | `high_twd`，保持來源值 |
| `Low(NTD)` | 元 | `low_twd`，保持來源值 |
| `Close(NTD)` | 元 | `close_twd`，保持來源值 |
| `Volume(1000S)` | 千股，不是股 | `volume_shares = 原值 × 1000` |
| `Amount(NTD1000)` | 千元 | `amount_twd = 原值 × 1000` |

Search 公司 `2330` 並選入，確認已選公司只有一筆 `2330=>TSMC`。
上述名稱是本次英文介面的觀察；版本或語系不同時，以實際標籤及單位重新核對。

原生 ListBox 的游標移動不一定更新 .NET 內部選取狀態，因此必須讀回已選清單。
原始欄位與換算後欄位均保留，不做 log、asinh、補零、插值或任意價格捨入。
單位換算不會恢復來源已捨去的精度，也不能套用到不同名稱或單位的 TEJ 欄位。

## 預覽匯出與設定檔

先按 Preview，確認實際預覽表的公司、日期、六欄及數值。必須在匯出前保存
市場儲存格的讀回證據，再按 Preview 頁的 Export to Excel。當時廠商在匯出後清空
Preview，匯出後讀到空表不代表原本沒有資料。

驗收用的 Preview 是 MSAA 讀回，不是 OCR 或畫面推測。目前可維護的工具包含
Excel 唯讀擷取與兩邊驗收；當時的視窗操作／MSAA 擷取工具屬於暫存診斷 harness，
尚不是跨版本通用的無人值守下載器。新的查詢仍需取得新的真實 Preview 證據，
不能沿用本次 JSON 證明別的查詢。原始 Preview 證據沒有保留精確擷取時間，僅知在匯出前。

File Manager 開啟的是查詢設定存檔。它保存的 TXT **不是市場資料**，可能包含帳號身分；
不要放入公開目錄、Git、面板或驗收輸入，也不要讀取 Excel 查詢註解來取得帳密。
本次設定檔已移到 Windows 使用者的私人 LocalApplicationData，未保留公開副本。

## 唯讀擷取 Excel

使用 [唯讀擷取工具](../scripts/read_tej_smart_wizard_workbook.ps1)。它要求明確指定
工作簿名與 Excel HWND，並限制從 A1 開始的八欄原始格式。
它只讀已匯出的 `Value2`、保留 Excel 日期系統及觀測時間；不登入、不請求 TEJ、
不執行巨集、不存檔、不讀查詢註解，也不關閉使用者的 Excel。

先在 WSL 的 repo 根目錄取得目前正確的 Windows 路徑：

```bash
wslpath -w "$PWD/scripts/read_tej_smart_wizard_workbook.ps1"
```

當時從 WSL UNC 路徑直接執行受到簽章政策拒絕，改為把已審閱的腳本逐位元複製到
本機私人目錄，核對 SHA256 後執行，沒有修改執行政策。
以下在 **Windows PowerShell** 執行；不要固定發行版名稱、PID 或 HWND：

```powershell
$ErrorActionPreference = 'Stop'
$tejSource = Read-Host '貼上 wslpath 輸出的腳本路徑，不加引號'
$tejRunDir = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) `
    ('StockAgent\TEJSmartWizard\readback-' + [Guid]::NewGuid().ToString('N'))
[void](New-Item -ItemType Directory -Path $tejRunDir)
$tejReader = Join-Path $tejRunDir 'read_tej_smart_wizard_workbook.ps1'
Copy-Item -LiteralPath $tejSource -Destination $tejReader
if ((Get-FileHash -LiteralPath $tejSource -Algorithm SHA256).Hash -cne `
    (Get-FileHash -LiteralPath $tejReader -Algorithm SHA256).Hash) {
    throw '腳本內容不一致'
}

$tejWorkbook = Read-Host '輸入已匯出的工作簿名，例如 Book1'
$tejExcel = $null
$tejBook = $null
try {
    $tejExcel = [Runtime.InteropServices.Marshal]::GetActiveObject('Excel.Application')
    $tejBook = $tejExcel.ActiveWorkbook
    if ($null -eq $tejBook -or $tejBook.Name -ne $tejWorkbook) {
        throw '目前 Excel COM 工作簿不是指定對象'
    }
    $tejWindow = [long]$tejExcel.Hwnd
    [pscustomobject]@{Workbook=$tejBook.Name; ExcelWindow=$tejWindow}
} finally {
    foreach ($tejObject in @($tejBook, $tejExcel)) {
        if ($null -ne $tejObject -and [Runtime.InteropServices.Marshal]::IsComObject($tejObject)) {
            [void][Runtime.InteropServices.Marshal]::ReleaseComObject($tejObject)
        }
    }
}
if ((Read-Host '確認以上是指定的 Excel 工作簿及視窗，輸入 YES') -cne 'YES') {
    throw '尚未確認擷取範圍'
}
$tejPayload = & $tejReader -ExpectedWorkbook $tejWorkbook `
    -ExpectedExcelWindow $tejWindow -Worksheet 'Sheet1' -MaxRows 100
$tejReadback = Join-Path $tejRunDir 'excel_readback.json'
$tejPayload | Out-File -LiteralPath $tejReadback -Encoding utf8
$tejReadback
```

這是重跑方式，並非本次文件更新重新操控了 Excel。工作表名稱與上限須依實際查詢修改，
工具最多接受 5,000 筆；不適用於任意 TEJ 資料表。多個 Excel 實例時 COM 可能指向另一個，
若範圍檢查失敗應先人工核對，不取消保護。若本機腳本仍被政策拒絕，保留錯誤交由
管理者處理，不使用 `ExecutionPolicy Bypass` 或降低 Office 巨集／ActiveX 安全設定。

## 重跑歷史樣本驗收

[驗收工具](../scripts/verify_tej_smart_wizard_export.py) 僅接受真實 Excel 與 Preview JSON。
以下重驗的是已保存的歷史樣本，不會向 TEJ 重新抓取；每次使用新的輸出子目錄，
不覆蓋先前收據：

```bash
source scripts/runtime_env.sh
tej_verify_root=$(mktemp -d /tmp/stockagent-tej-verify.XXXXXX)
run_fintech_python scripts/verify_tej_smart_wizard_export.py \
  --input artifacts/data_quality/tej_smart_wizard_repair_2026-10-01/excel_readback.json \
  --preview artifacts/data_quality/tej_smart_wizard_repair_2026-10-01/preview_readback.json \
  --symbol 2330 \
  --dates 2014-01-02,2014-01-03,2014-01-06,2014-01-07,2014-01-08 \
  --output-dir "$tej_verify_root/verified"
```

檢查項目包括公司／日期鍵恰好符合計畫、無重複與缺日、全部數值有限且非空、
OHLC 關係有效、Excel 日期系統正確、六欄與 Preview 相同、成交股數為非負整數、
金額非負，以及輸入／輸出 SHA256。成功產出 `historical_sample.csv` 與 `verification.json`；
這是來源內一致性驗收，不是與交易所獨立校驗。

針對工具的 [26 項語意測試](../test/test_tej_smart_wizard_export.py) 已於原修復通過：

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q -s test/test_tej_smart_wizard_export.py
```

## 錯誤判斷與尚未驗證事項

先前 `Cannot create ActiveX component` 的實際 ProgID 經唯讀 metadata 診斷為
`Excel.Application`。32 位元 COM 建立及 32／64 位元既有 Excel 存取均曾成功，
Excel 與 Wizard 當時也是相同的 medium integrity；因此尚不能歸因為單純位元數或權限衝突。
本次以正常重新開啟查詢及有效操作恢復成功，但不等同證明其中某一步就是原始例外根因。
先前 File Manager 的完整 .NET 例外未保留，仍需重現及完整 stack 才能定位。

未修改 Office／TEJ 安裝檔、註冊表、巨集／ActiveX 政策或帳密；也未啟動全市場下載、
定期排程、冷庫同步或訓練注入。桌面登入可用不等同獨立 TEJ API 授權。
全部欄位權限、各商品最早日期、全歷史覆蓋、歷史發布時刻、來源獨立校驗與
遠端／訓練授權仍須分別清點，不能由本次五筆成功外推。

官方流程參照為復原收據所記錄的
[Smart Wizard User Guide](https://www.tej.com.tw/TEJPLUS/Smart%20Wizard%20User%20Guide.pdf)；
本機可用性的證據以本文件連結的實際收據為準。
