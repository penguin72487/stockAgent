# 當沖特徵分類與乾淨資料集（2026-09-28）

## 1. 執行進度與交付範圍

新資料集目錄：`artifacts/datasets/tw_day_trade_curated_20260928_v3/`。
**特徵整理、完整來源驗收、品質遮罩、Parquet 全量回讀與 notebook 程式碼驗證均已完成。**
以 `dataset_manifest.json` 為最終版本收據，v1／v2 是保留的調查版本。

- 期間：2014-01-02～2026-09-24，3,107 個交易日。
- 5,809,578 筆股票日，實際包含 2,567 個代號；來源 panel 有 2,757 個代號，
  沒有在本期間產生 alive 列的代號不虛構資料補入。
- 46 個數值特徵＋46 個可用性標記，另有 date／symbol 兩個主鍵。
- 原始來源／時鐘驗收：0 critical、0 high、1 medium。
  medium 是下市與強制回補公告交叉覆蓋不完整，不是完整執行資料已就緒。
- 196 項相關測試通過；資料回讀的重複鍵、非交易日／未知代號、已觀測非有限值、
  NULL／mask 不一致均為 0；沒有全空或全期固定的數值特徵。
- Parquet 473,850,755 bytes（約 452 MiB），SHA-256
  `689e04f9cbc0cbc885927f043cce8c09516b5719ffce48ab83ca4fa6efd8c957`。

主要交付：[模型輸入](../artifacts/datasets/tw_day_trade_curated_20260928_v3/model_inputs.parquet)、
[特徵字典](../artifacts/datasets/tw_day_trade_curated_20260928_v3/feature_dictionary.csv)、
[全部分類](../artifacts/datasets/tw_day_trade_curated_20260928_v3/feature_classification.csv)、
[排除原因](../artifacts/datasets/tw_day_trade_curated_20260928_v3/excluded_features.csv)、
[逐年覆蓋率](../artifacts/datasets/tw_day_trade_curated_20260928_v3/annual_coverage.csv)、
[最終收據](../artifacts/datasets/tw_day_trade_curated_20260928_v3/dataset_manifest.json)。

本次整理的是 **09:00 盤前可用的模型輸入 X**，不是訓練好的模型，也不是
含 09:01 分鐘成交／費用／報酬標籤的完整交易訓練包。沒有啟動訓練、切換
網頁／Discord 模型、下單或發布資料至遠端。

`feature_audit.yaml` 只供來源與特徵時鐘驗收，繼承的 naive 標籤不是當沖
成交；**不要直接拿它執行 `train.py`**。當沖 eligibility、容量、委託續單、
企業行動及成交價格仍由既有交易模組負責，不能當成 X 的一部分。

## 2. 原清冊不是模型矩陣

輸入清冊：`artifacts/data_quality/downloader_integrity_20260928/feature_candidates_corrected.csv`，
共 12,023 個「來源 × 欄位」項目。這個數量包括：重複供應商表示、索引、
管理欄、未解析的數值字串、交易規則及尚未證明公布時間的研究資料。
因此不應直接把所有數字欄、或 FinLab 寬表中的股票代號當成模型特徵。

`feature_classification.csv` 對每一項保留分類、處理結果與原因。
`excluded_features.csv` 是未直接准入的完整清單，並不是要刪除的來源檔案清單。

| 處理 | 來源欄位數 | 解釋 |
| --- | ---: | --- |
| 房屋／不動產排除 | 3,599 | 內政部實價／租賃、住宅統計及房屋相關會計欄 |
| 管理資訊排除 | 1,429 | SHA、URL、路徑、下載資訊、錯誤與其他管理文字 |
| 識別／分類鍵不直接餵模型 | 1,544 | 日期、代號、名稱、幣別等；必要 join 鍵另存 |
| 僅限交易規則或標籤 | 51 | eligibility、未來價格／報酬、調整因子等 |
| 重複表示不重複加入 | 48 | 同名訊號使用權威股票來源，不拼入期貨衍生投影 |
| 會丟失合法零值的舊轉換 | 15 | 5 種 positive-only log1p × 3 個來源投影，改採原值 |
| 全空，排除 | 7 | 所選來源沒有已驗證觀測值 |
| 品質待修，隔離 | 120 | 來源不完整、舊衍生表需重建、TDCC 聚合異常 |
| PIT／歷史版本未證明，隔離 | 1,486 | 包括 FinLab 研究資料與快照型資料 |
| 缺語意／轉換／時間對齊，隔離 | 3,693 | 不能因為有值就自動跨市場、跨粒度合併 |
| 已選權威公共特徵 | 31 | 另加 15 個由股票 OHLCV 正規建置的技術特徵 |
| 合計 | 12,023 | 原始來源和舊清冊皆保留 |

「隔離」不代表這個經濟訊號沒有價值，也不等於下載失敗。舉例：財報
`decimal_value` 是待解析與匹配申報版本的事實，不能因為儲存為字串便丟棄；
同樣也不能只轉 float 就宣稱它在歷史 09:00 已知。

分類共有 20 類，包括微結構、公司治理、財報、期權、海外市場、基金、
加密資產等。仍有 395 個來源欄位的業務語意待確認，全部不進主矩陣。
完整分類數量見 `category_summary.csv`，沒有只列 Top-K 後省略其他欄位。

## 3. 這次可用的特徵

| 類別 | 數值特徵數 | 例子 |
| --- | ---: | --- |
| 行情、技術形態與大盤 | 18 | 開收盤相對報酬、上下影線、CLV、TAIEX |
| 成交量與流動性 | 6 | 量變化、成交量／金額／筆數原值、周轉率 |
| 估值與殖利率 | 3 | 本益比、股價淨值比、殖利率 |
| 融資融券 | 8 | 餘額、變化及買賣流量 |
| 法人籌碼 | 4 | 外資、投信、自營商、法人合計淨買賣 |
| 原始發布版本總經 | 7 | 外匯存底、M1B／M2 年增率、CPI、失業、GDP |
| 合計 | 46 | 另外 46 個 `__available`，模型輸入共 92 通道 |

主要資料 `model_inputs.parquet` 只含：

- `date`、`symbol`：股票 × 決策交易日主鍵，不送入模型的數值 X。
- 46 個 `float32` 數值欄。
- 46 個 `bool` 可用性欄。

沒有房屋、SHA、URL、檔案路徑、下載時間、財報申報期序號、執行標籤或
原始文字欄。`feature_columns.json` 是唯一的模型欄位／順序白名單；
`feature_dictionary.csv` 列出全部名稱、分類、單位、可用時鐘及缺值統計。

不可將 SHA 從原始收據刪掉：它不具備預測意義，但仍是驗證來源與快取
沒有被偷換的必要資訊。它只留在 manifest／receipt，不會成為模型通道。

## 4. 時間、缺值及篩選原則

1. 27 個日行情／當日完整統計欄位，往下一個**交易日**移動一次；週末、
   假日不當成交易日。同步移動其可用性標記。
2. 12 個盤後法人／融資券欄位，正規來源建置器已移到下一交易日，不能再移一次。
3. 7 個總經欄位按已驗證原始發布版本和可用時間對齊；只延續已發布狀態，
   不用現今修訂值倒填歷史。
4. NULL 保留，真實的 0 仍是 0。只有送入既有 tensor 路徑時才在保留 mask 的
   情況下填 0；本益比不適用等缺值，不以未來數值、內插或其他股票補齊。
5. 篩選依欄位語意與資料品質，沒有用全部年份相關係數、測試集報酬或
   整段歷史標準差挑特徵。標準化／缺值估計若有需要，只能在各 fold 訓練年擬合。
6. 保留年度 expanding walk-forward，不隨機拆 stock-day。
7. 當天開盤 gap 是另一個合法但須**收到開盤價後**才能用的 phase-aware
   特徵，不是垃圾資訊；本次 preopen 表不加入，也不將其偷偷倒填到前日。

房屋排除範圍是特徵，不是刪掉營建股或金融股。除了明確中文房屋欄，
Census `hv`、`mhs`、`mhs2`、`resconst`、`ressales` 也納入；混合住宅／非住宅
建設的 `vip` 保守整組排除。一般 GDP／CPI 總體指標不因包含住宅成分而
被重新拆解或偽造為「去房屋」數值。分類依據可查
[Census 住宅空置與自有率](https://api.census.gov/data/timeseries/eits/hv.html)、
[住宅建設統計](https://www.census.gov/construction/nrc/data/series.html) 及
[Census EITS API 說明](https://www2.census.gov/api-documentation/EITS_API_User_Guide_Dec2020.pdf)。

## 5. 此次攔下的實際問題

- TDCC 舊研究表大戶比例：17,510 筆非空值中 17,343 筆大於 1，最大為 2。
  未完成分級與合計列語意修復，整個相關家族不准入。沒有用 `clip(0, 1)`
  掩蓋錯誤，也沒有以該研究表更新任何模型。
- v1 的逐欄交叉檢查發現，既有 `_positive_log1p` 明確要求原值 `> 0`，
  導致零成交量、零成交金額、零筆數、零融資／融券餘額被丟掉。v2 將
  這 5 個輸入改為原值通道，不改寫舊模型、舊公共表或其 checkpoint ABI。
  v1 僅保留調查與相依來源用途，不是本次最終交付的 X。新模型若需要縮放，
  必須用訓練 fold 擬合，或採明確保留零值的轉換；不能重新套用舊的正值門檻。
  最終 v3 相對 v1 新增保留 **2,709,475 個可驗證真實零值欄位格**，不是補造
  2,709,475 筆交易。逐欄證據見 `zero_preservation_comparison.csv`：成交量、
  金額、筆數各 40,028，融資餘額 488,150，融券餘額 2,101,241。
- 保留零值後再逐筆核對，發現 `6201 / 2016-09-29` 的原值為 0、舊 log
  卻為 log(2)。追到合併前的 2016-09-26 TWSE／TPEX 融券餘額分別為 1、0；
  不能因為兩者都是官方檔便任意依列順序挑選。v3 重用正規解析器，核對
  融資券、法人與估值的合併前觀測，只將來源不一致的值遮罩，不更動原始檔。
  2014 起的日期範圍內有 53 個股票日期、157 個欄位衝突（法人 152、融資券 5）；
  估值沒有查到衝突。完整證據見 `quality_masks.csv`，實際輸出遮罩數以 manifest
  的 `quality_mask_summary` 為準：本次實際遮罩 157 格，另 93 格位於輸出期間／
  範圍之外，不新增資料列。NULL 與非空並存不算矛盾；相同值不遮罩。
  盤後籌碼沿用既有下一交易日時鐘；估值僅移一次，不會多落後一天。
- 舊相依資料綁定的 `tpex_basic_company.parquet` 及
  `taifex_daily_options.parquet` 已被下載器更新。第一次正式驗收拒絕舊
  symbol／feature 收據；透過既有官方建置器在 `source_inputs/` 完整重建，
  沒有直接修改 SHA 收據假裝通過。失敗證據保留於 v1 的 `audit_before_source_refresh/`。
  v3 重用 v1 已重建的 `source_inputs/`，實際相依路徑與 SHA 固定在 manifest，
  不另外複製一次原始公共資料；不要在未處理相依關係前刪掉 v1 來源目錄。
- v2 第一次驗收剛好遇到背景 CBC／DGBAS 更新：讀到暫時的 running 收據，
  且 DGBAS 檔案在驗收中換版，因此正確拒絕通過。更新結束後逐一驗證
  原始文件、收據及語意來源 fingerprint，確認特徵來源內容未變，再以既有
  `tw-public-refresh.lock` 保護驗收；沒有停止下載器或修改收據假裝通過。
  失敗證據保存在 v2 的 `audit_during_source_refresh/`。
  同時補上對 `state/*.json` 的前後穩定性檢查，避免只檢查 Parquet，漏掉
  發布完整性收據在驗收期間遭替換。
- 驗收器原先只接受數值欄，會把正規建置的 availability 欄誤判為額外欄位。
  已改為檢查精確的「值＋可用性」欄序，並檢查每個可用性值只能為 0 或 1。
  額外 SHA、錯誤欄序、重複欄位、非二元 mask 仍失敗。
- 通過標記最後才原子寫入；設定（含父設定）或正規輸入 tensor 與驗收時的
  fingerprint 不一致時，不能輸出 ready manifest。v3 另固定品質遮罩的 SHA，
  保留遮罩前／後 tensor fingerprint，不改寫原快取，然後對乾淨矩陣全量回讀。

## 6. 重現與讀取

不需要新增下載框架。新目錄的建置順序如下；不可使用已 ready 的目錄重建。

```bash
cd /root/stockAgent
source scripts/runtime_env.sh

run_fintech_python scripts/curate_tw_day_trade_training_dataset.py rebuild-sources \
  --output-dir artifacts/datasets/tw_day_trade_curated_20260928_v3 \
  --end-date 2026-09-24

flock --nonblock /srv/stockagent-live/.locks/tw-public-refresh.lock \
  bash -c 'source scripts/runtime_env.sh
run_fintech_python scripts/curate_tw_day_trade_training_dataset.py prepare \
  --source-bundle artifacts/datasets/tw_day_trade_curated_20260928_v3/source_inputs \
  --output-dir artifacts/datasets/tw_day_trade_curated_20260928_v3'

flock --nonblock /srv/stockagent-live/.locks/tw-public-refresh.lock \
  bash -c 'source scripts/runtime_env.sh
run_fintech_python scripts/audit_tw_public_data_layer.py \
  --config artifacts/datasets/tw_day_trade_curated_20260928_v3/feature_audit.yaml \
  --public-dir data_tw_public \
  --output-dir artifacts/datasets/tw_day_trade_curated_20260928_v3/audit \
  --strict --require-live-selected-features --build-panel \
  --panel-cache-root artifacts/datasets/tw_day_trade_curated_20260928_v3/panel'

run_fintech_python scripts/curate_tw_day_trade_training_dataset.py finalize \
  --output-dir artifacts/datasets/tw_day_trade_curated_20260928_v3
```

若來源鎖正由正式下載／更新持有，上述非阻塞命令會失敗；稍後重試，不要
繞過鎖。建置與驗收間若來源內容改變，依收據重建，不手改 fingerprint。

現有資料只讀重驗：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/curate_tw_day_trade_training_dataset.py validate \
  --output-dir artifacts/datasets/tw_day_trade_curated_20260928_v3
```

查看一批輸入，不載入整個歷史到 RAM：

```python
import json
from pathlib import Path
import polars as pl

root = Path("artifacts/datasets/tw_day_trade_curated_20260928_v3")
columns = json.loads((root / "feature_columns.json").read_text())["model_inputs"]
batch = pl.read_parquet(root / "model_inputs.parquet", n_rows=256)
keys = batch.select("date", "symbol")
x = batch.select(columns).cast(pl.Float32).fill_null(0).to_numpy()
assert x.shape[1] == 92  # 保留全部 availability；不把 keys/labels 放入 x。
```

完整逐筆回讀的檢查程式保存在 `verify_dataset.ipynb` 和建置腳本。
`source_inputs/` 與 `panel/` 是建置／驗收相依資料，不是要求使用者將它們的
所有數字欄都餵給模型；正式輸入僅限上述白名單。
特別是 `panel/` 保留遮罩前的正規來源 tensor 以供重驗，**訓練請讀取已完成
品質遮罩的 `model_inputs.parquet`，不能直接拿此驗收快取替代乾淨 X**。
