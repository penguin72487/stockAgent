# TEJ Smart Wizard 欄位清冊與分階段取得計畫

此頁保留為當時不完整目錄的歷史快照。**現行完整清冊與下載器／面板請看
[2026-10-02 取得與監控文件](tej_smart_wizard_acquisition_2026-10-02.md)**；勿沿用本頁舊的表／欄位總數。

日期：2026-10-01（Asia/Taipei）。本次依使用者要求先整理清單，
沒有啟動全量市場資料下載、新排程或訓練注入。

## 目前交付範圍

已讀到 **175 個 SmartID／資料表、43,849 個資料表欄位項目**，
並對照 **9,083 個本機特徵證據項目**。這不是 43,849 個獨立經濟概念，
也不是 43,849 個已測試可下載的新特徵：同一概念可出現在不同產業、
母公司／合併、單季／累計或歷史分段資料表。

這份是**尚未完成全目錄的第一版**。Type 選單共 30 類，12 類已有欄位讀回、
4 類留下空選單記錄，另外 14 類未讀取；Bankstat 已讀 6 張表但尚未讀完。
兩份主要來源收據均明確標示 `complete=false`，不能把已讀到的表算成全帳號完整清冊。
空選單也不等於來源無資料、無權限或永久失敗。

清點期間既有查詢視窗已關閉；最後確認的 Smart Wizard 是 Book2 的 Preview 頁。
新工具在視窗身分改變或找不到 MainPage 的 Data Source 時拒絕繼續，
沒有關閉使用者工作簿、按預覽、按匯出或讀取帳密。
續查需要保持已登入的查詢開啟，切回 MainPage，再以當前 PID／HWND／標題精確指定。

## 完整逐欄清單

- [全部已讀欄位](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-01/plan_v2/all_fields.csv)：保留原始欄名、資料表、Type、分類、推定原生頻率、單位、階段、比對線索及待確認事項。
- [所有資料表](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-01/plan_v2/tables.csv)：逐表欄位數與階段數。
- [30 類目錄狀態](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-01/plan_v2/catalog_scopes.csv)：已讀／空選單／未讀分開標示。
- [完整表格報告](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-01/plan_v2/inventory.md)：所有已讀資料表，沒有 Top-K 截斷。
- [來源與限制摘要](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-01/plan_v2/summary.json)：原始目錄 SHA256、比對快照與驗證範圍。
- [本機既有特徵證據](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-01/plan_v2/local_feature_evidence.csv)：可追溯 FinLab 收據、FinMind 收據／佇列及全來源監測欄位。

每欄 TEJ 的真正最早／最新日期、非空筆數、成功下載權限與發布時程仍待實查，
未知欄位留空，不填成零。目錄只有欄名，不能拿表名中的年份作為歷史下限。

## 取得順序：兩個階段，最後再校驗

| 順序 | 清單 | 欄位候選 | 安排原則 |
| --- | --- | ---: | --- |
| P1 | [未找到明確概念對照](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-01/plan_v2/phase1_new_feature_candidates.csv) | 42,572 | 先核對同義欄位與來源定義，確認真正沒有取得，再下載新增資訊。未映射不代表其他程式一定沒有。 |
| P2 | [既有概念、歷史覆蓋待補](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-01/plan_v2/phase2_history_gap_candidates.csv) | 754 | 先核對頻率、標的範圍及缺口；同原生頻率與口徑內，已有可比較筆數者由少到多。 |
| 最後 | [跨來源校驗候選](../artifacts/data_quality/tej_smart_wizard_inventory_2026-10-01/plan_v2/phase3_cross_source_validation.csv) | 523 | P1、P2 的必要取得完成後，再下載獨立 TEJ 版本校驗，不直接覆蓋既有主來源。 |

P2 中 **72 欄有可比對的 FinLab 非空數值數、682 欄仍未知**。
這些未知項目包括頻率／還原口徑未對上、最新樣本只有欄名，或首期是季度標籤而不是 ISO 日期；
不能把它們說成 682 個確定資料缺口，也不能因為數值很多就宣布完整。
CSV 的 `local_non_null_count` 是 FinLab 單一 key 的目前非空儲存格數，
不是 TEJ 筆數，也不是本機所有來源加總。原始首末標籤另留在本機證據 CSV，方便查清季度期間。

比較起點預設 2014-01-01，沿用既有研究期間，**不限制之後 TEJ 回補只能從 2014 開始**。
實際取得仍應按來源可查歷史下限盡量向前。

日、週、月、季、年及事件表分開比較。季財報本來就比日行情少，
公司晚上市、已下市、沒有事件或沒有營業活動也不等於下載失敗。
真正缺口需以該標的的有效期間、原生觀測頻率與應有鍵集合確認。

## 已讀資料中的優先檢查方向

以下是值得優先查驗的候選，不是「其他來源絕對沒有」的結論。
每個實際欄位都在上述 CSV，這裡僅說明取得用途。

| 資料群 | 實際可見欄位／表例 | 先查什麼 |
| --- | --- | --- |
| 公告與可用時間 | `Announcement Date - SALE`、`Announcement Date - Earning`、Important Calendar | 是否為當期公告日、修訂日或現有版本日期；不能直接當成所有欄位的發布時刻。 |
| 月營收、盈餘與預估 | Monthly Sales & Earnings、`Forcasted Sale`、`Forecasted Pre-tax Income`、營收極值與達成率 | 原始值／比率可保留，預估與實際值分開，依版本與發布時間校驗。 |
| 子公司與部門 | Subsidiary Monthly Sales、Department Information(Qly) | 與本機公司合併層資料區分，不把子公司／部門軸混成母公司重複值。 |
| 財報及金融業明細 | 銀行／金控／保險／證券財報、資產品質、授信、到期日、地區／產業分布、BIS | 先取得通用表未涵蓋的附註與金融業欄位；同時保留合併／母公司、單季／累計與幣別。 |
| 查核報告 | Abstract Auditor Report | 可作查核意見與資訊風險的分類資料，需驗證歷史版次與公告時刻。 |
| 治理與投資 | 董監酬金、董監持股／質押、長期投資、轉投資、中國投資、持股結構 | 是否已有同概念歷史，而不是僅有最新快照。 |
| 公司事件與生命週期 | 股東會、股利政策、除權息、庫藏股、買回日、停牌／公司屬性 | 保留事件與生命週期，不用無事件零值掩蓋缺乏來源覆蓋；包含歷史下市標的。 |
| 市場結構 | 還原／未還原價格、下一日參考價與漲跌停、法人、融資券、借券、當沖 | 既有行情／法人先補真正缺口；TEJ 自算估值與官方估值仍是不同口徑。 |
| 期貨／選擇權 | 契約屬性、期貨日／夜盤、個股期貨契約調整、大額交易人 OI、選擇權歷史分段 | 與本機 TAIFEX／FinMind／永豐按商品、契約月份、交易日、盤別、單位逐鍵校驗。 |
| 總經／外匯／指數 | Macroeconomics、NewYork／Asia FX、國際股價指數、銀行統計部分表 | 系列代號可能放在公司／標的軸；目前欄位數不是宏觀系列數，還要補系列目錄。 |

未讀分類：Offshore Fund、Bond、PUB、Delisted、Foreign Exchange Rate- Spot、
Bond-RP/RS、Gbd、TDR、Bond Indicators Yield、Corporate Finance Events、GFund、
GOffshore Fund、FINST、WMIX。代碼式名稱在未讀資料表前不自行推斷內容。

## 避免重複取得與誤用的規則

1. 只有跨 Type 的 **SmartID、表名和完整欄位清單都完全相同**，才合併目錄項目；
   Type／標的範圍仍保留。不同表名、歷史分段、還原方式與會計口徑不直接刪除。
2. FinLab 寬表的股票代號不是獨立 feature，改用每個 dataset key 的目前收據。
   不把多來源行數或不同版本相加，假裝同一特徵有更多資料。
3. 英／中文別名僅為概念線索；分鐘線不等於日資料，還原價不等於原價。
   補資料前要確認相同市場、標的、頻率、單位與值定義。
4. `Volume(1000S)` 保留原始千股並明確換算成股，`Amount(NTD1000)` 換算為元；
   未標示單位的欄位待核對 UOM，不能一律乘 1,000。
5. 原始值與比率保留；目錄已辨識的 2 個 `ROI%-Ln` 欄位列入清冊，但標成
   `source_log_derived_do_not_use_as_raw_input`，不當作使用者要求的原始模型輸入。
6. 原始 TEJ 與其他來源分開保存；價格／數值差異先核對精度、盤別、修訂與調整，
   不以平均、插值或覆蓋主來源消除差異。

## 工具、驗證及後續續查

- [目錄擷取工具](../scripts/inventory_tej_smart_wizard_catalog.ps1)：指定 PID、HWND、精確標題；
  只操作目錄選單，逐表穩定讀回，可用 `-ResumeSource` 保留成功收據，
  `-TypeIndices` 分段續查。供應商內部目錄請求量未測量，不宣稱零 API 成本。
- [本機比對與清單工具](../scripts/build_tej_smart_wizard_inventory.py)：唯讀本機收據、
  Parquet 欄位及 SQLite 讀交易；沒有 API、GUI、來源寫入或排程操作。
- [操作及匯出復原手冊](tej_smart_wizard_export_runbook_2026-10-01.md)：記錄精確日期輸入、
  候選／已選欄位讀回、匯出前 Preview 保留、Excel 唯讀擷取、單位與驗收。

重建本次清單時使用新的輸出目錄，既有版本拒絕覆蓋：

```bash
source scripts/runtime_env.sh
tej_inventory_run_dir=$(mktemp -d /tmp/tej-catalog-plan.XXXXXX)
run_fintech_python scripts/build_tej_smart_wizard_inventory.py \
  --catalog \
    artifacts/data_quality/tej_smart_wizard_inventory_2026-10-01/catalog-resumed.jsonl \
    artifacts/data_quality/tej_smart_wizard_inventory_2026-10-01/catalog-remaining.jsonl \
  --output-dir "$tej_inventory_run_dir/plan" \
  --target-start 2014-01-01
```

Focused tests：

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q \
  test/test_tej_smart_wizard_inventory.py \
  test/test_tej_smart_wizard_export.py
```

本輪 56 tests passed；PowerShell Parser 語法檢查通過。
測試涵蓋不完整目錄、分段收據、schema 衝突、Type 別名、未知筆數、
不同頻率／還原口徑、欄位單位、log／比率區別、股票代號軸排除與禁止覆蓋。
實際產物驗收另外確認 43,849 個 feature ID 無重複、三階段聯集與逐欄清單完全一致、
逐表欄位合計一致、30 類目錄狀態保留、來源 SHA256 一致及 25 個本機文件連結有效。
驗收狀態是 `passed_partial_inventory_acceptance`，不是全目錄或全歷史下載完成。
另有上一輪 **2330、5 個歷史日期、6 個數值欄位**的匯出讀回驗收；
不能外推為所有資料表／所有股票／全部歷史都可下載。

官方手冊的資料庫與欄位選擇流程可參考
[Smart Wizard User Guide](https://www.tej.com.tw/TEJPLUS/Smart%20Wizard%20User%20Guide.pdf)。
本次清冊以實際可見目錄為證據；桌面可用不等於獨立 TEJ API 已授權，
資料表與歷史權限應在後續必要的有限樣本查詢中逐一確認。
