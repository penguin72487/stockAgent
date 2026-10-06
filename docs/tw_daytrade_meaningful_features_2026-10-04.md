# 台股當沖：全部有意義特徵清冊與逐項狀況

## 1. 執行結果

你說得對，有意義的財務科目確實超過幾千個。之前的 **251 個**是已接入混合頻率研究表的數值選集，不是全部資料來源的 feature 總量；這兩種數量必須分開。

本次已整理 **160,610 筆來源欄位／原生概念表示**，沒有 Top-K 截斷。每筆均有 Markdown 詳細表，依分類分成 **654 冊**。另外將 **2,342 個註冊來源項目**逐項寫成 Markdown；這些項目含群組與別名，不能另加到 feature 數量。

| 本次計數口徑 | 數量 | 正確解讀 |
| --- | ---: | --- |
| 全部清冊明細 | 160,610 | 原清冊 12,446＋TEJ 45,826＋實際來源概念展開 102,338 |
| 用途內、有經濟意義的候選表示 | 145,804 | 包含跨市場背景；不是已通過全部訓練驗收的維度 |
| 其中數值／比率／旗標候選表示 | 144,306 | 型別、單位、實體、公布時鐘仍須逐項驗證 |
| 其中類別／事件／文字候選表示 | 1,498 | 需要歷史類別或文字／事件編碼，不直接轉成任意數字 |
| 財報與營收候選表示 | 129,301 | 同科目在不同表、taxonomy、單位及版本下可能重複 |
| 財報與營收不同原始科目名稱 | 10,738 | 移除 XBRL 命名空間後按原名精確計數，仍不等於互不重複的經濟概念 |
| 已接入原私人研究表的數值 | 251 | 接線狀態；本次沒有修改 allowlist 或模型輸入 |

從 [完整總報告](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/README.md)進入所有分冊；只看財務科目可直接開 [財報與營收完整索引](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/fundamentals/index.md)。

這是**本機已盤點目錄與指定原檔展開範圍**的完整清冊，不聲稱列盡每個供應商 API 的所有可能指標，也不聲稱所有來源歷史都已下載完。

## 2. 第一性原理：到底什麼是一個 feature？

一筆觀測至少要保留：

`來源、經濟概念、實體、報告／觀測期間、合併／部門等維度、單位、值、可知時間、發布版本`

經濟概念回答「量測了什麼」。實體與期間回答「誰、何時」。公布時鐘回答「決策當時知不知道」。檔案 hash 回答「讀的是哪份檔案」。這些不是同一件事：

- `Assets`／資產總額是量測；每檔股票代碼不是一個新的資產 feature。
- FinMind 財報的 `type`／`origin_name`區分科目，`value`是數值容器，不能只算一個 value，也不能將容器和子科目雙算。
- MOPS 的同一個科目可能有不同歷史 taxonomy QName、單位與財報維度。清冊完整保留這些表示，尚未宣稱彼此等價。
- TEJ 表格 × 欄位數包含不同表、版本與市場。可見目錄、匯出非空值、完整歷史、可用於訓練，是不同狀態。
- `sha256`、檔案路徑、下載時間留在追溯層；未來報酬、執行價格及成交規則留在標籤／執行層，不因它們是數字就混入模型。

**分類方法**：先依來源家族與原生科目軸，再看欄名、已取得的數值／型別證據；不是單憑舊訓練名單決定有沒有意義。這是可重現的逐筆規則分類，不冒稱已人工驗證每一科目的公式或預測能力。未知定義保留為待核對，不直接刪除。

因此 **10,738 個不同名稱也不是模型必須設為 10,738 維**；獨立資訊量、SVD／有效秩、可預測性均未在本次測量。

## 3. 財務特徵分類

以下數量是用途內候選**表示**，不將版本重複或同名誤稱獨立資訊。

| 財務子類 | 表示數 | 訓練前要核對的基本差異 |
| --- | ---: | --- |
| 資產負債與資本結構 | 29,799 | 時點存量、幣別／尺度、合併／個別、企業行動 |
| 損益與營運 | 23,710 | 期間流量、單季／累計、一次性項目、原始／修訂版本 |
| 現金流量 | 1,537 | 營業／投資／籌資、累計期間、流入／流出符號 |
| 財務比率、每股與成長 | 6,455 | 公式與分母、每股／總額、百分點／小數、零與負分母 |
| 月營收與成長 | 383 | 所屬月份與實際公布時間不同，累計與當月不可混用 |
| 銀行、放款與信用風險 | 19,015 | 產業特有定義、放款／存款／逾放、減損與資本適足 |
| 部門與子公司 | 775 | 報告實體、部門軸、關係企業、重複加總 |
| 其他財務科目 | 47,627 | 保留原來源定義；子類規則尚不能可靠細分，不猜測公式 |

公司的投資性不動產、租賃負債或抵押貸款是財報科目，不能因「房屋」或「loan」關鍵字就當作住宅成交資料或股票融資券。房屋市場資料仍依你原先要求排除；這次只更正報告分類，沒有繞過原訓練的排除 gate。

## 4. 全部分類入口

以下每個索引都連到**全部明細**，不只是一張分類統計表。

| 分類 | 用途內候選表示 | 完整 Markdown |
| --- | ---: | --- |
| 財報與營收 | 129,301 | [全部科目](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/fundamentals/index.md) |
| 公司基本與治理 | 1,066 | [完整清冊](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/company_profile/index.md) |
| 公司事件 | 586 | [完整清冊](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/corporate_events/index.md) |
| 價量／技術與指數 | 764 | [完整清冊](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/price_technical/index.md) |
| 估值 | 38 | [完整清冊](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/valuation/index.md) |
| 流動性 | 126 | [完整清冊](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/liquidity/index.md) |
| 微結構 | 117 | [完整清冊](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/microstructure/index.md) |
| 法人籌碼 | 411 | [完整清冊](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/institutional/index.md) |
| 集保與持股 | 208 | [完整清冊](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/shareholding/index.md) |
| 融資券與借券 | 349 | [完整清冊](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/margin_lending/index.md) |
| 期貨與選擇權背景 | 2,034 | [完整清冊](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/derivatives/index.md) |
| 基金／債券／ETF | 1,089 | [完整清冊](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/funds/index.md) |
| 總經 | 8,256 | [完整清冊](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/macro/index.md) |
| 匯率與利率 | 143 | [完整清冊](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/fx_rates/index.md) |
| 跨市場背景 | 94 | [完整清冊](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/cross_market/index.md) |
| 加密市場背景 | 722 | [完整清冊](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/crypto/index.md) |
| 資格／制度與執行資料 | 32 | [完整清冊](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/execution_rules/index.md)；執行 mask 與 label 不混作輸入 |
| 待完整定義／細分類 | 468 | [完整清冊](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/unclassified/index.md)；有量測軸不代表定義已完全核對 |
| 房屋資料（用途排除） | 0 | [排除明細仍保留](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/housing/index.md) |

權證用途排除保留在各來源原分類中；鍵、管理證據、未知型別、執行與標籤也都留在完整明細，沒有為了讓數字漂亮而刪掉。

## 5. 每個 feature 明細實際包含什麼

每筆都有固定清冊 ID 與可跳轉 anchor，詳細表包含：來源／表／原欄名、分類與子類、經濟角色及判斷理由、台股或跨市場用途、原生概念軸、首末觀測標記及日期審查、儲存列／非空／檔數、數字／有效零／型別、缺值解讀、單位尺度、更新頻率與證據、公布與版本時鐘、驗檔範圍、來源權限、訓練接線、前次 admission 理由、同名表示核對、下一步、原檔與收據位置。

已接線的 251 個值另列所選研究網格的可用率及品質 barrier；TEJ 明細另列取得狀態與已累積的非空匯出 cells。**沒有統計證據的值寫未知，不假設為 0 或完整。**

## 6. 現在可以用哪些？還差哪些？

| 本次工程狀態 | 清冊項數 | 解讀 |
| --- | ---: | --- |
| 已接入原私人研究表 | 251 | 仍需獨立正式 data gate／訓練驗收 |
| 需轉換、單位與可知時間接線 | 100,460 | 經濟候選，不等於可以直接餵入訓練 |
| 原生語意或型別需確認 | 1,741 | 未解析概念不直接判為無意義或壞檔 |
| 文字／數值解析需 adapter | 331 | 不把沒有 numeric projection 的概念誤當數值 |
| TEJ 已匯出非空值但未接線 | 3,221 | 這是全 TEJ 清冊狀態，含非模型鍵；不是 3,221 個已就緒經濟通道 |
| TEJ 目錄可見但未證明取得非空值 | 42,605 | 不把目錄當已下載值 |
| 所選來源／檔未有觀測值 | 13 | 不是證明全來源或歷史都查不到 |
| 來源／衍生層需品質核對 | 19 | 保留候選與既有 barrier，不替它補造數字 |
| 所選研究期間無變異 | 2 | 在該期間不加通道，不宣稱科目永遠沒有意義 |
| 非本次選定輸入／用途排除 | 11,967 | 原始資料與明細保留 |

這張表與「經濟意義」是不同軸；總數合計 160,610，但不能將每個工程狀態都解讀成可用模型通道。

### 不同頻率怎麼處理

對某檔股票在決策時間 `t` 的候選值，只能使用 `available_at ≤ t` 的已知觀測。季報／月報可依原研究契約沿用最後已公布值，並提供 `available`、`age`、`update`；沿用不是生成一份新的財報。

財報期間、公布日、來源下載時間、下載器排程必須分開。事件型資料不能一律永久 forward-fill。沒有原始發布版或歷史公布時間證據時，要標成研究估計／未驗證，不倒推成嚴格 PIT。

FinLab 明確區分 actual disclosure date 與 deadline，詳見 [官方 FAQ](https://finlab.finance/docs/en/faq/)。本次逐欄保留已有 clock／carry 規則；未登記者不擅自填上每日頻率或任意 carry 天數。

### 缺值不是單一故障

合法低頻、上市前／下市後、不適用科目、稀疏事件、公式分母為零、有效零、未下載、映射失敗、原檔損壞需要分開。沒有預期 issuer × native-period 分母，不能把 `非空 cells ÷ 日期列數`當完整率。

本次沒有任意補零、插值、以現值填歷史，也沒有更改研究例外。`1000 NTD`與`NTD MN`分別標示千元／百萬元；來源單位與欄名證據衝突時兩者並列，不沿用錯誤尺度。

## 7. 覆蓋與驗收邊界

- 原清冊 12,446 筆與原 admission ID 一一對帳；TEJ 一次唯讀交易核對 253 張表的 45,826 欄，快照時間為 **2026-10-04 16:21:22 台北時間**。
- 對指定長表來源的 **2,833 個原檔**讀取必要欄位並驗證內容 SHA／讀取穩定性；FinMind 有原 SHA 的選定檔另對照收據。其他來源沿原 footer／收據界線，沒有宣稱全來源逐值驗收。
- MOPS 展開 87,749 個 QName × unit 概念表示、FinMind 財報 467、FinLab 事件表欄 756、BEA 300、Census 12,676、FRED 11、免費公開 metric 369、ETF metric 10，合計 102,338。
- 原監控盤點 88,762 個實體 dataset × field；股票代碼等寬表軸不當 feature。原 schema 覆蓋仍為 partial，詳見 [實體粒度對帳](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/physical_grain.md)。
- 全部來源狀態見 [註冊來源完整 Markdown](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/source_status/index.md)。仍有 1 個容器未展開全部原生概念，已在完整總報告逐項說明。
- 最後版只更正分類，從已驗收 v1 逐鍵核對 **148,164 筆原始來源 snapshot**後重用，不冒稱重新下載或更新全部來源。
- 聚焦分類、單位、投影、缺值、Markdown 覆蓋與證據改動拒絕測試共 **108 項通過**；完整明細、ID、分冊 hash 與連結驗收見 [報告收據](../artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2/report_acceptance.json)。

FinLab／FinMind 維持使用者確認的本人 vastai1T 私人非商業研究權限；TEJ 的本機可見目錄／匯出不擴大為跨主機發布許可。沒有公開受限資料或帳密。

**本次完成的是清冊與報告，不是將 145,804 個表示全部接入訓練。** 沒有改模型、下載、遠端寫入、同步、重算報酬或啟動 GPU；未證明所有歷史 PIT、無重複獨立資訊量、預測能力或正式訓練就緒。

## 8. 重現與程式

沿用 [既有報告入口](../scripts/report_tw_day_trade_feature_catalog.py)，新增全來源語意清冊模式；核心邏輯與測試為 [tw_feature_semantic_report.py](../stockagent/data/tw_feature_semantic_report.py)、[test_tw_feature_semantic_report.py](../test/test_tw_feature_semantic_report.py)。

```bash
cd /root/stockAgent
source scripts/runtime_env.sh

# 原檔不變時，只重分類已驗收觀測；輸出必須是新的版本目錄。
run_fintech_python scripts/report_tw_day_trade_feature_catalog.py \
  --all-source-catalog artifacts/data_quality/tw_daytrade_mixed_frequency_20261004/catalog \
  --admission-dir artifacts/data_quality/tw_daytrade_mixed_frequency_20261004/admission_v3 \
  --tej-root data_tej \
  --tej-inventory artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/plan_v3 \
  --reuse-source-report artifacts/data_quality/tw_daytrade_all_feature_report_20261004_v2 \
  --output-dir artifacts/data_quality/tw_daytrade_all_feature_report_NEW

# 若要重新讀取新來源觀測，移除 --reuse-source-report，仍使用新的輸出目錄。
```

清冊附件 `feature_catalog.jsonl`記錄全部 ID、分類與 Markdown 位置；`report_manifest.json`固定來源輸入、程式與分冊 SHA。這些附件用於驗收，**不取代全部 Markdown 明細**。
