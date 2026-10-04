# 台股當沖特徵：研究用公布時間補齊（2026-09-28）

## 使用邊界

使用者本次明確允許：無法逐筆證明歷史公布時間時，以既有發布規律推估。
這授權的是**時間假設**，不是補造觀測值、把目前修訂數值宣稱為歷史原版，
或把今天的 OpenAPI 快照填回 2014 年。嚴格版
`artifacts/datasets/tw_day_trade_curated_20260928_v3` 保持不變。

新版使用獨立 `tw_preopen_release_schedule_research` 合約。只供接受這些
假設的研究訓練；不代表已通過實盤 PIT 閘門，也不含分鐘成交與報酬標籤。
房屋相關、SHA、URL、路徑、下載時間等仍不是模型輸入。原始值保留供稽核；
時間、來源與品質資訊另放收據和字典。

## 規則及證據

所有時間以台北 09:00 決策為界。只有公布日期、沒有可靠時刻時，不在該日
09:00 使用。申報期限遇休市先順延期限，再等下一個交易日，不能把「期限
順延到週一晚間」錯當「週一開盤前已公布」。日曆使用實際 TAIEX 交易日。

| 家族 | 新研究時鐘 | 狀態保留上限 | 注意事項 |
| --- | --- | --- | --- |
| 季報 | Q1 5/31、Q2 8/31、Q3 11/30、年報次年 4/30 的保守包絡，再比對已知上傳日取較晚者 | 200 日 | 包絡是研究假設，不是所有產業的法定期限；不套用 TDR 的海外申報制度 |
| 月營收 | 月份標籤對應次月 10 日；2026 起因缺歷史保險集團分類，統一保守用 15 日。已轉成發布月份的提供者索引不再加一個月 | 62 日 | 年月索引與已對齊日期分開解析，模糊的月底日期拒收 |
| 每日借券、外資持股、ETF 折溢價、鉅額成交 | 觀測日期之後第一個交易日 | 不補日缺口 | 前日資料不冒充當日新觀測 |
| 集保持股比例 | 每週資料日期加 7 個曆日，再於下一交易日使用 | 14 日 | 7 日是額外研究緩衝，官方資料只證明週末持有量統計語意，未證明每一筆歷史發布時刻 |
| PMI／NMI | 不早於發布月份第 3 個交易日，下一交易日使用 | 62 日 | 官方為前 3 個工作天；交易日是較保守的代理，不是精確歷史公布日 |
| 景氣指標 | 提供者發布月份月底後下一交易日 | 62 日 | 官方通常 27 日、可能另有預告日期；月底是研究包絡，修訂風險仍保留 |

來源（2026-09-28 查核）：

- [證券交易法第 36 條](https://twse-regulation.twse.com.tw/tw/law/DOC01_print.aspx?FLCODE=fl007009&FLNO=36)：一般財報與月營運申報時程。
- [公告申報特殊適用範圍辦法](https://twse-regulation.twse.com.tw/TW/law/DAT08_print.aspx?FLCODE=FL067432)：第一上市 Q2、海外制度及 115 年起保險業月營收特殊期限；不能用單一一般公司期限假裝覆蓋所有公司。
- [TWSE 111Q1 申報期限新聞稿](https://investoredu.twse.com.tw/FileSystem/FileUpload/f506f947-0f96-48d2-8248-b28f5a431c7a.pdf)：一般公司與金控期限不同，假日順延。
- [FinLab 財報發布日與截止日說明](https://finlab.finance/blog/taiwan-financial-statement-date-index)：季度標籤、電子書上傳日、統一期限具有不同語意。
- [國發會 PMI 發布時間問答](https://www.ndc.gov.tw/nc_337_2268)：每月前 3 個工作天發布上月指數。
- [國發會景氣概況資料說明](https://data.gov.tw/dataset/27543)：原則上每月 27 日下午公布前月景氣資料。
- [TDCC 集保戶股權分散表](https://original-www.tdcc.com.tw/portal/zh/smWeb/qryStock)：每週最後營業日營業結束後持有餘額的統計。
- [中經院 PMI 2026 年 4 月新聞稿](https://www.cier.edu.tw/wp-content/uploads/2026/05/PMI_NMI202604-NEWS.pdf)：部分季調指標會回溯修訂，推估時間不能消除此值版本風險。

上述一般規律不是逐公司歷史合規證明。若公司晚報、特殊展延未出現在現有
上傳日期表，仍可能有時序誤差。財報上傳表也只是提供者的日期證據，尚未
逐筆匹配原始文件內容版本。因此 manifest 永遠保留
`historical_point_in_time=false`、`publication_time_estimated=true`。

## 資料處理

- 沿用 `finlab_research_overlay` 的寬表數值解析和原始檔 SHA 驗證，新增可傳入
  明確發布日映射的接口；原有呼叫預設行為不變。
- 只從已分類且可適用家族規則的真實數值欄位建立研究通道，不用資料型別
  是數字就通通收進來。尚未明確定義粒度、單位、企業映射的來源繼續隔離。
- 有期限的後向狀態對齊只保留已知最近觀測；不向過去回填，不插值，不跨
  長期停更沿用失效科目。舊期財報晚到不得覆蓋已經可用的新一期。
- 保留有效的 0；缺資料仍為 NULL，availability 必須一致。品質異常不修成
  看似合理的數字，另留遮罩依據。
- 按年串流寫出，逐批比對原嚴格版每一個值與遮罩，驗證股票／日期鍵不變。
- 不在全歷史上估計標準化或預測效力；這些必須由後續每個訓練 fold 自己擬合。

## 重建方式

在 `/root/stockAgent` 執行，輸出目錄必須是新版本且不存在：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/build_tw_release_schedule_dataset.py \
  --accept-estimated-publication \
  --output-dir artifacts/datasets/tw_day_trade_release_schedule_research_NEW_VERSION
```

程式完成全量驗證後才寫 `dataset_manifest.json`；沒有該成功 manifest 的
目錄只是未完成候選。`feature_columns.json` 指定 X 的次序，日期／股票代號
只是 join keys。`verify_dataset.ipynb` 提供可重跑的全量讀回驗證。

## 本次執行結果

完成版本：`artifacts/datasets/tw_day_trade_release_schedule_research_20260928_v3/`。
合約：`tw_preopen_release_schedule_research_v3`。

| 項目 | 驗收結果 |
| --- | --- |
| 原嚴格版 | 46 個數值欄位與 46 個遮罩，逐批比對完全未變 |
| 新增研究特徵 | 206：財報／營收 159、總經 32、持股 8、借券 5、鉅額成交 1、ETF 1 |
| X 欄數 | 252 個數值 + 252 個 availability = 504；不含日期及股票 key |
| 模型輸入列 | 5,809,578 股票日；2,567 檔、3,107 個交易日 |
| 期間 | 2014-01-02 至 2026-09-24；不代表每個新欄位都有全期間非空觀測 |
| Parquet | 1,124,195,274 bytes |
| 原始候選清單 | 12,023 個來源欄位完整保留分類結果 |
| 行鍵／數值 | 重複鍵、非有限觀測、NULL 與遮罩不一致、全空欄、有效值全常數欄均為 0 |
| 時間稽核 | 94,563,975 個逐欄來源觀測；相對本研究規則的時間邊界違反 0 |
| 較晚上傳日 | 驗證 8,766,941 個有提供者上傳日的逐欄觀測；無提前使用 |
| 異常比例 | 17 個來源儲存格保留原始值、在派生層遮罩，未硬改為 100% |
| 回歸測試 | 86 passed，另有全量資料讀回與時間稽核 |

矩陣 SHA-256（僅作收據，**不在 X**）：
`7bdec9543ba27371035d8849a21c3f66596e4803a53d6ed4f33aa02202468332`。
原嚴格版 SHA 保持
`689e04f9cbc0cbc885927f043cce8c09516b5719ffce48ab83ca4fa6efd8c957`。

異常實例：`00663L` 的 2022-11-25 來源集保比例為 115.58。新規則映射到
2022-12-05 才可用，故 12/05–12/09 的該研究欄位均為 NULL／false；
12/12 下一份有效觀測到達後恢復。未用先前 83.95 的值掩蓋異常週。
其餘 16 個來源比例異常早於本次 2014 起的 X 範圍。

驗收檔案：

- `dataset_manifest.json`、`validation.json`：資料就緒、結構與數值驗證。
- `feature_columns.json`：可供模型使用的欄位順序。
- `research_feature_dictionary.csv`：新增特徵分類、來源、時鐘、保留期限與實際起迄。
- `release_schedule.csv`、`observations/*.parquet`：推估時間、逐股已知上傳日及數值追溯；不作 X。
- `quality_masks.json`、`quality_masks.csv`：異常原值及遮罩依據。
- `annual_coverage.csv`：每年每個特徵的觀測與缺值比例。
- `timing_readback_validation.json`、`verify_observation_clocks.py`：可重跑的全來源時間／遮罩稽核。
- `verify_dataset.ipynb`：可重跑的全矩陣與原嚴格版比對；本次已執行所有
  code cells，未宣稱使用 Jupyter UI／kernel；輸出保存在
  `artifacts/data_quality/release_schedule_notebook_execution_20260928.txt`。
- 測試輸出：`artifacts/data_quality/release_schedule_research_20260928_tests.txt`。

## 還沒有全部放行的部分

本次針對明確可適用規則的 208 個來源欄位執行：206 納入、2 個在選定來源／
股票範圍內全常數，仍排除。兩者為「合併前非屬共同控制股權」及其損益。

此外，原公布時間／版本隔離清單仍有 **1,278 個來源欄位未納入本版**：
FinLab 770、FinMind 423、Shioaji／TAIFEX 56、其他官方／派生表 29。
並不是宣稱它們永遠不能使用，而是本次沒有替它們完成相符的來源適配、
時間規則與數值版本驗收。所有項目在 `remaining_timing_worklist.csv`；
其他粒度／品質／識別／房屋排除項目在完整 `feature_classification.csv`。
不能把「允許估算公布時間」當成對全部未知單位、未知粒度、只有今日快照、
或缺數值來源的通行證。

未重新訓練、未更動線上服務、未同步／發布到遠端。這是可讀取的研究 X，
不是已完成分鐘執行標籤、walk-forward 設定、回測與部署的完整訓練工作。

過程保留 v1 候選，以及因遮罩日期 JSON 序列化停止的 v2 候選。v2 沒有成功
manifest。該錯誤已修正，新增端到端測試覆蓋「有異常遮罩 → JSON → 完整
矩陣 → manifest」，最終 v3 已實際走完此路徑。
