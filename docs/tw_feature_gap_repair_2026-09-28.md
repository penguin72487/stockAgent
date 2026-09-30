# 2026-09-28 當沖特徵缺值、更新週期與語意修復

## 1. 執行進度

本輪依使用者的質疑，重新區分「沒有數值」與「來源損壞」。前一版 worklist 的
`unmapped` 等狀態只是在描述**備援 adapter 是否完成**，不足以判斷原始資料品質。
核心修正不是把所有 NULL 填成 0，而是先找出每個欄位在該股票、日期是否應有值。

本輪已驗收資料為 `artifacts/datasets/tw_day_trade_semantic_research_20260928_v3/`，
以 `tw_day_trade_cross_source_research_20260928_v1` 為 parent，保留 252 個數值特徵、
252 個 availability mask、原日期與股票鍵。2026-09-29 台北時間完成（沿用本輪開始日的
20260928 artifact 命名）；**全量重建、獨立逐格驗收、201 項回歸均通過。**

- 5,809,578 筆股票日、3,107 個交易日、2,567 個代碼；範圍 2014-01-02～2026-09-24 不變。
- 逐格比對 **1,464,013,656 個數值格**；未記錄的數值變更、鍵／順序漂移、availability
  mask 錯誤皆為 0；157 個明示官方衝突格維持遮罩。
- 合計補 3,683,927 格；取消無效／過早值 2,439,948 格；另更新 36,444 格已公布狀態。
  缺值 416,893,786 → **415,649,807**，淨減 1,243,979。沒有把增加的 NMI 時點遮罩藏在補值數內。
- 集保獨立 NumPy 時間序列重建比對全部 5,809,578 列，數值／NULL 不一致為 0。
- 矩陣 1,128,105,432 bytes；SHA-256
  `d272d38dad702eb145debc5a59dd5cd90ffe1b97e777b9d4fedc86e9498f0b45`。
- 原來源、舊研究資料集、strict base 保留；沒有改訓練 config、遠端、checkpoint 或 live 服務。
- 27 欄在選定股票日範圍內無缺值。其餘逐格分原因，**不代表其餘 225 欄損壞／不能使用**。

已實作的修正：

| 問題 | 第一性原因與修正 | 全量矩陣獨立驗收 |
| --- | --- | --- |
| 集保歷史月資料被當週資料 | 2015/5 前為月報，原本一律 14 天到期會清掉仍有效的月報；改為歷史月制 62 天、改週制後 14 天的有界狀態延續 | 補回 222,359 格，該欄缺值 272,755 → 50,396 |
| 鉅額交易沒有事件被當缺資料 | 逐交易所、逐日核對四個市場總額與**完整來源所有代碼**明細；對得上才把未出現股票記為 0，不把每日事件 forward-fill | 補入 3,402,206 格有完整性證據的 0 |
| 面板首日失去前一天 | 原面板先裁到 2014，再做前日位移，遺失 2013/12/31；改用相同 pinned 股票檔與共用特徵公式讀取起始日前歷史 | 補 33,663 格；不改價格、不跨股票生命週期 |
| 可精確推導的財報科目 | 同公司、同期、同單位的總額恆等式；與原資料重疊驗證通過後才補，時間取最晚運算元公布日 | 補 638 筆原始期間觀測，展開補 25,699 格、更新舊期 carry 2,322 格 |
| ETF 折溢價異常哨兵值 | 正價格及正 NAV 不可能產生 ≤−100% 折溢價；`−999.99` 不可當有效數值或沿用更舊值 | 原來源保留，遮罩 12 格 |
| NMI 追溯序列被當成當時已公布 | 2014 年的回溯調查在 2015/2/2 才首次公開；已知首次公布日覆蓋通用月頻代理 | 取消 2,439,936 格過早可見值；最早 2015/2/3 可用 |

上表的「補格數」是日面板格數，不是新下載筆數。財報例子：股東權益 55、流動負債
91、非流動負債 492 個原始股票／季度鍵通過。營業毛利、營業利益等候選恆等式因
實際口徑含其他項目、一致性不足，**沒有硬補**。

集保制度沿革由[金管會資料開放簡報第 6 頁](https://data.gov.tw/api/front/file/download?uuid=50c4fb19-c6d0-4d88-8304-dfdf7338b732)
確認，並與本機 2330 的月底／每週日期序列交叉比對。62／14 天是研究版的**狀態有效期上限**，
不是額外公布延遲；公布後安全等待仍只有 1 個日曆日。
NMI 首次公開日由[中經院發布會紀錄](https://www.cier.edu.tw/conference-ch/16183/)
與[當期官方新聞稿](https://www.cier.edu.tw/wp-content/uploads/2025/03/upload/PMI%26NMI201501-NEWS.pdf)確認。

## 2. 為何理論上完整，日面板卻有 NULL

一個訓練格子能用，需要同時滿足：此欄適用該證券、相關報告／事件確實存在、數值定義
成立、在決策前已可取得、轉換和對齊正確。原始檔案沒損壞，不代表這五件事自動成立。

- **狀態資料**：月營收、季報、集保、景氣指標應從公布後沿用最近一期，不要求天天
  發布。既有月／季處理原本已採 as-of carry；本輪抓到的是集保制度曾由月改週的例外。
  仍不能向公布前回填，也不能跨過顯式無效觀測或無限延長已停用科目。
- **每日流量／事件**：有鉅額交易才出現一列是合理稀疏；完整市場報告能證明沒事件才
  能填 0。借券、法人每日流量不能拿昨天的量填今天。5 個借券欄位缺值鍵完全一致，
  是共同來源／適用範圍問題的線索，不是 5 次各自的數值解析損壞；未證實的格子仍保留未知。
- **適用範圍**：ETF 沒有一般上市公司那組合併財報，普通股票也沒有 ETF 折溢價。
  不能拿全 5,809,578 個股票日當成每個 feature 都應有觀測的分母。
- **會計科目**：同公司同期已有資產報告，而某細項仍未見於可用來源，可能是不適用、
  未單列、合併到別科目、分類制度不同，也可能還有映射缺口。這是「來源未見該科目」，
  **不是已證明來源損壞，也不是已證明數值為 0**。
- **公式所需欄位**：本地官方 TWSE 日行情 adapter 沒有週轉率所需的發行股數分母；
  因此大量上市股票週轉率為 NULL，而 TPEx 有值。先前用 FinMind 分母對比未通過
  同義一致性，不可換成另一種股數口徑強填。零成交量的對數變動也可能數學上無定義。
- **歷史時點**：檔案今天包含 2014 年資料，不等於 2014 年已公開；NMI 正是實際例子。
  這種修正可能讓空值增加，卻降低訓練偷看未來的風險。

所有原選定欄位保留，沒有因 NULL 比例高就整欄刪除。模型消費端仍需使用原有 availability
mask；張量的缺值佔位 0 不得解讀為「真實財務值等於 0」。這一輪沒有改模型輸入欄數、
訓練 runner、實盤服務或 broker 成交。

### 剩餘原因：不是一份「損壞清單」

新版 `remaining_feature_worklist.csv` 的 `dominant_reason`、`reason_counts` 解釋各欄；
另以 `missingness_causes.csv` 保存可加總的細項。以下大宗都是**診斷線索／適用性**，
不能直接宣稱來源壞掉或完全健康：

| 分類 | 剩餘格數 | 解讀與處理 |
| --- | ---: | --- |
| 同公司已有財報，該科目未見可用值 | 273,265,708 | 保留未知；區分科目適用、未單列、制度／mapping 差異，不任意填零 |
| 公司報表不適用 ETF | 85,003,626 | 不是公司財報下載洞，保留 mask |
| 來源庫最早觀測前 | 15,948,571 | 不能向前回填；不代表其他來源必定沒有 |
| 每日來源缺值或公式定義域 | 7,413,469 | 原始量、估值條件、數學分母需逐項檢查 |
| NMI 首次公開以前 | 5,325,034 | 包含本來即空與本輪撤銷；歷史當時不可用 |
| ETF 指標不適用普通股票 | 5,274,964 | 結構性不適用，不是空白下載 |
| 代碼不在來源 schema | 4,597,824 | 覆蓋／產品範圍待核對，不猜數值 |
| 當日日報未見該股票值 | 4,447,603 | 僅已核對完整市場報告的事件能推論 0；其他保留未知 |
| 該股尚無已公布觀測 | 3,890,810 | 上市／報告起始及來源覆蓋待核對 |
| TWSE 官方週轉率缺發行股數運算元 | 3,273,436 | 已辨識 adapter 原始欄位範圍；不能混用另一種股數口徑 |
| TDR 申報時鐘未納入 | 2,604,102 | 研究 adapter 範圍未就緒，不等於來源損壞 |
| ETF 不適用公司估值 | 1,589,517 | 保留 mask，不製造公司 PE/PB |
| 前一實際交易日未見該股行情 | 1,235,740 | 不偷用更舊一列，不插值價格 |
| 超過對應週期的有界有效期 | 970,566 | 不無限延長舊值，需核對後續報告／退休科目 |
| 本地來源未見該股任何有效觀測 | 799,936 | 覆蓋或欄位適用性仍待核對 |
| 技術指標前置運算元／guard 不足 | 8,884 | 保留公式 guard，不填虛構行情 |
| 顯式無效觀測阻隔 | 17 | 本輪 ETF 12 格＋原集保持股比例 5 格，禁止跨越 NULL barrier |

例：資產總額的 587,831 個缺值中，534,614 是 ETF 不適用；剩餘為未到首次公布
11,005、來源 schema 未涵蓋 17,995、TDR 時間規則未納入 16,378、過期狀態 7,839。
集保修正後的 50,396 缺格中，真正屬於有效期過期只剩 781；另有 44,655 尚無該股
已公布觀測、4,955 不在來源 schema、5 個原始比例無效。不能再說這整批是週期問題，
也不能把它們全說成下載損壞。

## 3. 證據、驗收與重建

新研究版的 canonical 程式：

- `stockagent/data/tw_public_release_schedule.py`：日期化週期、已知首次公布下限。
- `scripts/build_tw_release_schedule_dataset.py`：日後新建時使用相同規則；ETF 無效值保留 NULL 阻隔。
- `stockagent/data/tw_feature_semantics.py`：市場完整性、同期間恆等式、逐格原因分類。
- `scripts/repair_tw_feature_semantics.py`：可重現的版本化資料建置，不修改 raw 或 parent。
- `scripts/verify_tw_feature_semantics.py`：獨立逐格讀回；集保另外用 NumPy 重建 dated as-of。
- `scripts/audit_tw_feature_gap_repair.py`：只接受驗收通過且 SHA 相符的新版，再原子更新使用者清單。

本輪驗證收據：

- `artifacts/datasets/tw_day_trade_semantic_research_20260928_v3/dataset_manifest.json`
- `artifacts/datasets/tw_day_trade_semantic_research_20260928_v3/independent_acceptance.json`
- `artifacts/data_quality/tw_feature_semantics_20260928_tests.log`：201 passed。
- `artifacts/data_quality/tw_feature_semantics_20260928_notebook.txt`：伴隨 notebook 所有程式格
  已直接執行，不宣稱啟動過 Jupyter kernel／UI。
- `artifacts/data_quality/tw_feature_gap_repair_20260928/acceptance.json`：當前清單的資料集、
  manifest／矩陣／清單 SHA 與前版備份路徑；不再沿用舊矩陣的 acceptance。

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python scripts/repair_tw_feature_semantics.py \
  --output-dir artifacts/datasets/tw_day_trade_semantic_research_NEXT
run_fintech_python scripts/verify_tw_feature_semantics.py \
  artifacts/datasets/tw_day_trade_semantic_research_NEXT
run_fintech_python scripts/audit_tw_feature_gap_repair.py \
  --dataset artifacts/datasets/tw_day_trade_semantic_research_NEXT \
  --output-dir artifacts/data_quality/tw_feature_gap_repair_20260928
```

每步退出碼必須為 0。建置使用新的 output-dir，不重用已有 manifest；驗收未通過不能
把清單指向候選。`changes.csv` 分開記錄補值、取消無效／過早值、更新已公布的新期狀態；
`missingness_causes.csv` 的原因格數須等於各欄剩餘 NULL 數。原清單／驗收會保存在
`history/` 的雜湊命名檔，不誤把先前 adapter-only 驗收當成本輪矩陣的驗收。

候選 v1 因診斷階段 Float32／Float64 拼接失敗，沒有完成 manifest，已標記
`INCOMPLETE.md`；v2 是集保歷史週期修正前的版本。兩者保留追查，不是本輪最終選用資料。

仍維持 `historical_point_in_time=false`、`strict_training_eligible=false`、
`live_eligible=false`、`online_exhausted=false`。這是有估計公布時點與現修版數值的研究資料，
不是已證明每份歷史發布版本、也不是已備妥分鐘成交標籤的完整訓練任務。

## 前一輪紀錄：跨來源補洞（保留歷史，不代表本輪最新狀態）

### 當時執行結果

原始來源、嚴格版資料、服務、模型與遠端設定均未覆寫。此次只建立新版本研究資料。
當時資料集：`artifacts/datasets/tw_day_trade_cross_source_research_20260928_v1/`。
只有存在且通過讀回驗證的 `dataset_manifest.json` 才代表建置完成；不代表所有缺值已補完。

- 26 個研究欄位補入 **680,012 筆原始期間／股票觀測**。來源為既有、逐份收據／SHA 驗證的 FinMind 正式下載分區，不是以平均值、0 或價格反推補值。
- 另有每日估值／指數補值 **5,431 格**：本益比 1,041、股價淨值比 1,432、殖利率 1,453、前一交易日 TAIEX 1,505。
- 保留原本 252 個數值特徵與 252 個布林 availability mask，共 504 個輸入通道；沒有將 SHA、路徑、公布日期或房屋欄位加進 X。
- 範圍保持 2014-01-02 至 2026-09-24，5,809,578 筆股票日、3,107 個交易日、2,567 個代碼。這是原使用者選定訓練資料範圍，不宣稱新增之後的行情。
- 最終全量讀回：重複鍵、非有限值、缺值遮罩不一致皆為 0；最後每日補值沒有改變 parent 的任何既有非空數值。原始 strict v3 的檔案雜湊也保持不變。
- 相較舊 release-schedule v3，缺值由 419,284,443 降至 416,893,786，**淨減少 2,390,657 格**。這包含時間對齊修正與真實原始觀測補洞，不能全數宣稱是新下載筆數。
- 最終矩陣 1,127,409,002 bytes，SHA-256 `5888bef6918d4d98681fe5f0dcd5c9e4497b1e8efed83b8c32f8c361e3649586`。
- 182 項相關回歸通過；產生的 notebook 全部程式格已直接執行驗證，不宣稱啟動過 Jupyter UI／kernel。
- 另逐份雜湊核驗並檢查 95,235,908 筆股票觀測的**宣告時間規則**，其中已知上傳／觀察下限 8,801,608 筆，違規 0。這不等於證明推估公布日就是當年實際公布日。
- **尚未全部補完，也未證明所有有權來源皆查無資料。** 所有 manifest 維持 `all_missing_resolved=false`、`online_exhausted=false`；未完成語意對應或暫時額度不足，不能改寫成永久查無。

### 一天是額外等待，不是把月／季統計提前一天公布

新契約 `tw_preopen_release_schedule_research_v4_delay1d`：

1. 先決定來源的已知或推估公布日。
2. 額外安全延遲固定 **1 個日曆日**。
3. 取其後第一個實際交易日 09:00；週末、國定假日的閉市等待分開記錄。

`release_schedule.csv` 分別保存 `estimated_published_on`、`safety_ready_on`、
`extra_delay_days=1`、`nontrading_wait_days`。逐股票的已知上傳／來源觀察下限保存在
`observations/*.parquet`，不混成模型數字。

- 季報：已知公司上傳日優先，**不再等到較晚的月底**。沒有上傳日才採一般公司季報期限代理：5/15、8/14、11/14、次年 3/31；假日的法定截止順延另算。特殊申報制度仍是未證實的研究時間代理。
- 月營收：FinLab 的日期已是公布月份，不再額外加一個月；取消所有公司一律延到 15 日的做法。
- 集保：取消額外 7 天，來源週資料日期作為明示的公布日代理，再加一天；這不是證明當年該日確實已發布。
- NDC 景氣：取消月份最後一天的額外保守包絡，使用來源公布月份日期代理。
- PMI／NMI：官方說明是每月前 3 個工作日發布，因此仍以第 3 個交易日作為未知歷史公布日的估計；**其後安全延遲只有一天**。交易日不完全等於行政工作日，仍不是精確發布證據。

已知晚於代理的公布／上傳資訊不能被提前。程式另測試「已知早期上傳已落在資料範圍內、推估截止日卻在範圍外」的邊界，避免遺失可用季報。

### 跨來源怎麼補

先在 **相同股票、相同原始所屬期間** 比較，通過後才做公布時鐘與每日對齊。
不能拿已經 carry 的日面板誤認成新一期原始數字。

| 類別 | 單位／期間處理 | 結果 |
| --- | --- | --- |
| 財報 | 明確欄位 allowlist；FinMind 元轉 FinLab 千元；EPS 不除 1,000 | 部分通過，修訂／口徑不同的保留隔離 |
| 現金流 | 同年度精確前一季 YTD 差分；缺前季不作差、不跨年 | 多項無法通過跨來源一致性閘門，沒有硬補 |
| 月營收 | 相鄰月份／去年同月必須有精確期間；累計要求自 1 月起每月齊全 | 當月及上月營收通過，部分衍生百分比／累計因修訂差異未合併 |
| 集保 >400 張 | 僅加 400,001 以上四個級距；不加 total 或調整列；四級必須完整 | 通過並補入歷史觀測 |
| 外資持股 | 股數與百分點各自保持單位；比例超出 0–100 排除 | 七個欄位通過 |
| 成交量／額／筆數 | 不因日期代碼相同就假定成交範圍相同 | 一致率約 63.7%，拒絕混用；2330 比對無此差異，多數差異值集中在整千股官方記錄，原因仍待逐官方產品追查 |
| 融資融券 | 官方「張」及 asinh(x/1000) 原定義 | 一致性通過，但未找到額外可補的缺鍵 |
| 法人 | 外資、外資自營、投信、自營細分獨立；舊總量與細分同時非零不重複相加 | 約 98.5–99.49%，未達此補洞閘門，保留原值／原遮罩 |
| 週轉率 | 另外嘗試保留官方成交量、只補 FinMind 發行股數分母 | 約 88.47%，仍不混用；不能拿另一種流通／發行股數冒充原定義 |
| 估值／TAIEX | 相同原始單位、嚴格前一交易日 | 補 5,431 格 |

本次映射 QA 要求至少 100 個重疊鍵，且在固定數值容許誤差內一致率至少 99.5%。
這是單位／定義檢查門檻，**不是資料正確率保證**；不是拿報酬挑選，也沒有用測試集擬合換算係數。
既有有限原始數字一律優先，不投票、不覆蓋。顯式官方衝突遮罩禁止由備援解除。

無效百分比原本的 NULL 阻隔必須保留；否則稀疏 overlay 省略該筆後，as-of carry
會錯誤延續更舊值。此次補上專門回歸測試；只有實際合格備援觀測能解除該阻隔。

### 當時存取與尚缺的項目

- FinMind 本次可核實為 Sponsor，6,000 次／小時。使用原有正式分區、收據與共用資料規則；沒有建立另一組限流器或並行重抓已完成全市場歷史。
- FinLab 2026-09-28 22:29 台北取樣：已用 4,967.345／5,000 MB，剩餘 32.655 MB，低於原下載器 50 MB 保留額度。下個既定重置為 2026-09-29 08:00；未對這批缺值繞過額度重抓。既有排程的恢復不代表此資料集會自動更新，需重新建置新版本。
- FinMind 月營收 `create_time` 是 **入庫日**，不是公司公告；只自 2026-04-21 起記錄，該日的初始化值不能當歷史公告日。舊空日期沿用明示研究時程代理；較新真實入庫日僅作資料可用下限，不冒稱原始版本時間。
- ToAlpha 一般 API／查詢權限不等於批量匯出或 AI 訓練的書面授權。使用者表示只補小洞，未提供另行授權文件；此次未將 ToAlpha 值注入訓練資料，也未購買服務。
- 本機仍有 MOPS IFRS 54 個季檔、加上 GAAP 共 71 個本機匯入 archive。已確認可用檔案與欄位，但 **未完成剩餘研究財報欄位的逐項會計概念／合併個別／單季累計對應**；不能宣稱這些檔案已逐格查無。MOPS 來源狀態也區分本機匯入完成與線上自動下載未獲授權，沒有繞過限制下載。未映射的 124 欄也包含總經等非 MOPS 家族。
- 82 個研究欄位已嘗試 FinMind 映射，其中 26 個通過並有補值，56 個因口徑／修訂／一致性待釐清而不合併；其餘 124 個不是「網路沒有」，而是尚無驗證過的 adapter。
- ETF 不適用公司財報、未申報的會計項目、分母為零、沒有成交的 OHLC、尚未上市時期、年度／季別制度變動等，都不能以 0 或插值消掉。剩餘清單不把這些未辨識情況假裝成同一種下載失敗。

### 前一輪證據與重現

程式：

- `stockagent/data/tw_public_release_schedule.py`
- `stockagent/data/tw_public_cross_source_fill.py`
- `scripts/build_tw_cross_source_fill_bundle.py`
- `scripts/build_tw_release_schedule_dataset.py`
- `scripts/build_tw_daily_feature_fill_bundle.py`
- `scripts/apply_tw_daily_feature_fill_bundle.py`

證據：

- `artifacts/data_quality/tw_cross_source_fill_20260928_v2/`：原始期間補值、映射 QA、衝突、逐分區來源收據。
- `artifacts/data_quality/tw_daily_feature_fill_20260928_v1/` 至 `v3/`：每日欄位 QA 與稀疏補值收據，v3 沿 lineage 引用前版合格補值。
- `artifacts/datasets/tw_day_trade_release_schedule_research_20260928_v5/`：一天額外延遲＋原始期間補洞；嚴格 base 46 欄與遮罩全量保持。
- `artifacts/datasets/tw_day_trade_cross_source_research_20260928_v1/`：最後每日補值；`missing_values.csv` 列出每個數值欄位尚缺數量。
- `artifacts/data_quality/tw_cross_source_fill_20260928_tests.txt`：實際回歸結果。
- `artifacts/data_quality/tw_feature_gap_repair_20260928/history/`：本輪發布時保留前一輪 `acceptance.json` 與 `remaining_feature_worklist.csv` 的原始 bytes。前一輪 252 欄中，27 欄無缺值；其餘 37 欄已驗證映射但仍缺值、64 欄跨來源待核對、105 欄未完成備援對應、19 欄公式／觀測待辨識。**這些是 adapter 處理狀態，不是原始來源損壞數。** 最新同名清單改列語意原因。

新建置必須使用新 output-dir，不覆蓋已有 manifest。示例：

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python scripts/build_tw_cross_source_fill_bundle.py \
  --output-dir artifacts/data_quality/tw_cross_source_fill_NEXT
run_fintech_python scripts/build_tw_release_schedule_dataset.py \
  --accept-estimated-publication \
  --cross-source-bundle artifacts/data_quality/tw_cross_source_fill_NEXT \
  --output-dir artifacts/datasets/tw_day_trade_release_schedule_NEXT
run_fintech_python scripts/build_tw_daily_feature_fill_bundle.py \
  --output-dir artifacts/data_quality/tw_daily_feature_fill_NEXT
run_fintech_python scripts/build_tw_daily_feature_fill_bundle.py \
  --extend-bundle artifacts/data_quality/tw_daily_feature_fill_NEXT --market-only \
  --output-dir artifacts/data_quality/tw_daily_feature_fill_NEXT_market
run_fintech_python scripts/apply_tw_daily_feature_fill_bundle.py \
  --parent artifacts/datasets/tw_day_trade_release_schedule_NEXT \
  --bundle artifacts/data_quality/tw_daily_feature_fill_NEXT_market \
  --output-dir artifacts/datasets/tw_day_trade_cross_source_NEXT
```

這不是訓練命令。資料仍是現修版與推估時點的研究 ABI：
`historical_point_in_time=false`、`strict_training_eligible=false`、`live_eligible=false`，
也不包含分鐘成交模擬標籤。沒有改動原訓練 config、checkpoint 或產生策略績效宣稱。

參考：[FinMind 基本面與 create_time 定義](https://finmind.github.io/tutor/TaiwanMarket/Fundamental/)、
[FinMind 籌碼資料定義](https://finmind.github.io/tutor/TaiwanMarket/Chip/)、
[FinLab 財報發布日與截止日](https://finlab.finance/blog/taiwan-financial-statement-date-index)、
[NDC PMI/NMI 公布規律](https://www.ndc.gov.tw/nc_337_2268)、
[TDCC 股權分散表](https://original-www.tdcc.com.tw/portal/zh/smWeb/qryStock)、
[ToAlpha 使用條款](https://toalpha.tw/terms)。
