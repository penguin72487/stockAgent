# 訓練資料集組合方法

## 操作原則

資料集不是一個大型 panel 檔名，而是「固定來源版本＋固定轉換程式＋解析後設定＋
特徵／交易契約」。原始觀測、發布時鐘、交易資格、執行行情、標籤分開保存。
同一日期或欄位名稱不能證明兩個來源語意相同；缺行情不能用插值或另一商品價格代替。

penguin 保存原始來源、收據與服務必需的資料；Vast 按工作需求組合訓練 view。
資料抵達只產生新版**配方**，不自動開 GPU、不建立無人使用的大 panel、不切換正在訓練的
版本。試組合只留下驗證收據；正式訓練準備才保留工作使用中的 view／受管理快取。

逐項設定及已執行 run 清冊：

- [組合清單 Markdown](../artifacts/data_quality/minute_recipes_20261007/inventory/training_combinations.md)
- [完整 data／trading／walk_forward 組合 JSON](../artifacts/data_quality/minute_recipes_20261007/inventory/training_combinations.json)
- [組合 CSV](../artifacts/data_quality/minute_recipes_20261007/inventory/training_combinations.csv)
- [消融矩陣／GPU 工作定義](../artifacts/data_quality/minute_recipes_20261007/inventory/experiment_definitions.json)
- [歷史方法無法解析清單](../artifacts/data_quality/minute_recipes_20261007/inventory/errors.json)

2026/10/07 的清冊涵蓋可讀本機設定、現存本機 run，以及 vastai1T 現存 run 的只讀
metadata。303 份可解析設定、58 份 run manifest（42 本機／16 遠端）、167 種不同組合；
另保留 27 份消融／GPU 工作定義。run 包含正式、smoke、benchmark，不能全稱正式訓練。
6 份歷史設定仍有依賴缺失或已移除的 key，並未偷偷改成目前設定。
185 份紀錄的 path 已含明確 immutable release ID，已另外保留這種部分證據；
不能因為 manifest 沒有 `source_releases` 就說它們全都沒有固定版本，
也不能反過來宣稱其他 companion sources 的 SHA／可還原性都已證明。
封存 payload 本次沒有解封，**不宣稱已找回每次歷史訓練的全部來源**。

## 各家族的 canonical 組合方式

| 家族 | 原始／伴隨資料 | 組合入口 | 必須固定的差異 |
| --- | --- | --- | --- |
| 台股原始分鐘 | `tw-minute-source-cold`、`tw-public` 官方交易日 | `build_shioaji_tw_minute_dataset.py` → `audit_shioaji_tw_minute_dataset.py --all-partitions` | 右標已完成分鐘、數量單位、下分鐘研究成交／標籤、缺口與非交易日隔離 |
| 台股跨 provider 原生分鐘聯集 | FinMind current receipts、已驗證 Shioaji 原始欄位投影 | `merge_tw_minute_sources.py inventory/trial/build` → `report_tw_raw_source_merge.py` | 指數／個股、分鐘左右標、撮合機制、股份單位、來源分段／拒收／有效差異；不是直接可訓練的 research tape |
| 台股公開日特徵 | `tw-public`、股票原始日行情、交易規則／企業行動、所選分鐘 tape | `build_tw_public_training_features.py` → `stockagent.data.panel.build_panel` → 所選 execution loader | 公布時鐘、feature include/exclude/shift、日 K 代理 ABI、09:00/09:01／容量／收盤規則 |
| 混頻／寬覆蓋研究 | 經准入之來源包、FinLab／FinMind 原生觀測、官方股票／共用資料 | `stage_tw_day_trade_mixed_sources.py`／`stage_tw_day_trade_panel_sources.py` → `prepare_tw_day_trade_mixed_frequency.py` | 私人授權、feature_specs、原始 NULL、各頻率 carry／失效／report reset、公布時間研究近似 |
| 共用／個股分離 value-only | 同一受限原生來源包、公開規則／企業行動、分鐘行情 | `prepare_tw_day_trade_factorized_panel.py --model-channel-policy value_only` | 只有數值進模型；來源可用性與時鐘仍是內部准入證據，不新增 age／updated 模型通道 |
| 歷史四通道分離 panel | 同上 | `prepare_tw_day_trade_factorized_panel.py --model-channel-policy value_available_age_updated` | value／available／age／updated 是另一 ABI，不能接續 value-only optimizer |
| 台股分鐘＋日模型 OOF 指引 | 分鐘 manifest、相應年份之前已訓練／驗證的日模型結果 | `build_tw_minute_daily_guidance.py` → canonical minute loader | 同日期／商品、pre-year cutoff、fold owner；不把 in-sample 答案當 OOF；不自動打開 zero-fill |
| 私人 FinLab overlay | 准入的私人 FinLab 成品、官方規則／行情 | 所選 config 的 `tw_public_feature_path` → canonical panel | 私人包授權與來源 hash；不是允許公開原始 FinLab／FinMind；636／143 等歷史寬度不能互換 |
| 實體台灣期貨 | 官方 daily、product universe、合約生命週期、到期結算、乘數／保證金／費稅、公司行動、原生分鐘／tick/context | `prepare_tw_futures_physical_history.py` → `prepare_tw_futures_margin_training.py`；分鐘由 `build_tw_stock_futures_0900_entries.py` 等所選入口供應 | 實體合約身份、夜盤歸屬交易日、08:45/08:46、terminal price、rule delta／quarantine；期貨與台股假設分開 |
| Tick／BidAsk／秒級研究 | 原始 Tick、BidAsk、接收時間及 book capture、商品 universe | `build_shioaji_hft_dataset.py`／所選原生衍生品 loader | 粒度是 event／秒，不是分鐘；book age、receive clock、權限與合約身份；不能由 K-bar 猜深度 |
| 加密／外匯／一般日 panel | 對應 provider 原生 OHLCV、listing／session、funding 等同商品資料 | 對應 canonical provider builder／refresh → `build_panel` | date grain、時區、provider 數量單位、funding 時鐘、上市退市、方向／費率／fold |

以上是**家族入口**，不是一套能覆蓋所有股票／期貨商品的萬用價格或通用 loss。
每個實驗的真正 feature 清單、資金、容量、費率、切分及執行模式以完整組合 JSON
和 run manifest 為準；設定的預設欄位不代表全部實際成為模型輸入。

同義 row／來源分段合併方法另見 [完整方法](tw_raw_source_merge_2026-10-07.md) 與
[來源及試驗 Markdown](../artifacts/data_quality/tw_raw_source_union_20261007/index.md)。
FinMind 的 2005 年起分鐘是 TAIEX 指數，不能補成個股歷史；個股來源從 2019 年起。
跨 provider 的來源／補缺／衝突證據放在 sidecar，不新增 provider 模型特徵；
完整來源清冊與代表商品試合併，不等於全商品逐列合併或新版 execution tape 已准入。
此受限私人來源尚未寫進公開 `tw-minute-train`，也不冒充五種現有配方的已驗收 input。

### 最容易錯配的部分

1. 股份數量：原始 Volume、contract_unit、Amount 的單位證據先驗證，才能形成 shares／容量。
   不能把 lots 當 shares，也不因 simulation 登入就把行情 bar 描述為 simulation 成交回報。
2. 交易時鐘：一般台股日策略、下分鐘策略、期貨 08:45 策略、秒級 HFT 各有時鐘。
   分鐘價與日 K 代理、同收盤研究近似、即時 broker fill 各是不同契約。
3. 低頻觀測：原始未更新日維持 NULL；訓練 view 依既有發布／有效期／生命週期規則取得
   已知值。不能全欄 blanket forward-fill，更不能把真實公布 NULL 和外連接 padding 混同。
4. 共用特徵只儲存共用軸，個股特徵儲存日期×商品軸；`factorized` 在此指資料儲存表示，
   **不是重新啟用使用者已取消的基底分解／SVD／稀疏訓練實驗**。
5. 日 K 代理、同收盤特徵、借券／零股等例外只能沿用相應命名研究 ABI，不提升為實盤保證。
6. checkpoint 是否 resume 由 canonical lifecycle 和相容指紋決定。本次不修改正式訓練的
   年度重置、fold10、BF16/TF32、完整 feature 或 batch 設定，也不啟動正式訓練。

## 版本化配方與自動追新

模板在 [`configs/data_sync/training_dataset_recipes.json`](../configs/data_sync/training_dataset_recipes.json)。
目前有五種可呼叫模板：原始台股分鐘、公開核心、mixed-frequency、factorized value-only、
歷史 four-channel。其餘家族保留 canonical 方法與逐項設定，**未驗證的歷史來源綁定不會猜填**。

`training-dataset-recipes` 冷庫包只包含方法及版本頭，不包含訓練 tensor、optimizer 或帳密。
每個版本保存：各 input 的 dataset／snapshot_id／manifest SHA／inventory SHA、builder 與共享
Python／runtime script SHA、模板、語意及必要參數。base_config 的解析後 hash 與套件版本
另由每次組合收據記錄。五種模板的「方法就緒」不是「雙卡訓練驗收通過」。

資料發布成功後，既有 `run_data_release.sh` 呼叫 metadata refresh；
`stockagent-training-recipes.timer` 每五分鐘補做失敗／漏觸發工作。原始分鐘與官方日曆的範圍
不一致時，狀態是 `waiting_compatible_source`，保留上一個可用配方，而非造出完整新版。
同資料、同程式、同模板重跑不重寫版本頭；新資料或轉換程式才生成新配方。
方法包的發布沿用 canonical source lock；D 上的同步通知只持久入列，由既有 scan-retry
owner 重試。方法包提交不是遠端收齊證據，通知失敗亦不是 source 下載失敗。

本次已驗收的「抓完→內容驗證→入庫→新方法」入口是台股原始／研究分鐘。
非台股來源及封存歷史組合仍依各自 owner／clock／coverage gate，未驗收的範圍見
[逐項報告](minute_validation_and_recipes_2026-10-07.md#尚未放行的項目)，不把家族清冊當全系統完成證據。

## 在 vastai1T 按需組合

先依既有 code release 流程部署與配方 `code_files` 相符的 checkout；不能只複製本文件，
也不能拿舊 checkout 套新方法。原始包、配方與 cold index 走既有私人 Syncthing／edge 管線，
不需要 Vast 反向 SSH penguin。缺 payload、cold SHA 或 peer proof 時會阻擋，不改用本機舊資料。

在遠端工作目錄執行（先選定一個配方，不在中途追 moving latest）：

```bash
cd /root/stockAgent
source scripts/runtime_env.sh

# 先以既有 edge owner 取得 methods 包；這不是下載全部來源。
bash scripts/run_data_cache.sh use training-dataset-recipes
```

從 methods 包 `heads/tw-stock-minute.json` 讀取 `path`，固定對應的
`versions/tw-stock-minute/<recipe_id>.json`。其他家族同理。

```bash
# RECIPE 指向上述固定的版本檔，不是 mutable head。
run_fintech_python -m scripts.maintain_training_dataset_recipes trial \
  --recipe "$RECIPE" --materialize-inputs \
  --receipt artifacts/operations/minute-rebuild-trial.json
```

`--materialize-inputs` 是明確按需操作：只 hydrate 配方中的固定來源，沿用 canonical edge
peer／ignore／七天 lease／完整 hash；不在資料抵達時自動執行。來源 materialization 仍是
可重用受管理 cache，不是另存一份永久 panel。試組合的 derived view 成功／失敗都移除；
不刪使用者原始資料或既有訓練輸出。

需要正式準備 view 時明確使用 `build` 和新的 `--build-output`，不能覆写已有結果：

```bash
run_fintech_python -m scripts.maintain_training_dataset_recipes build \
  --recipe "$RECIPE" --materialize-inputs \
  --build-output artifacts/data_preparation/<新工作名> \
  --receipt artifacts/operations/<新工作名>-build.json
```

factorized／mixed-frequency 模板另須 `--parameters-json`，內容為
`{"base_config":"/root/stockAgent/configs/deployments/<指定實驗>.yaml"}`。
不重新選模型／假設；組合收據固定解析後設定 SHA。結果產生後仍須原本的環境／
資料閘門與雙卡 DDP 訓練驗收，才可啟動 canonical foreground resume。

## 保留與重現邊界

本次實際重建測試只覆蓋 0050、2330 的全部 1,608 個現有交易日，精確比對
848,123 筆原始價、容量、features 及 labels；不是 2,629 商品全 universe 重建，
也不是把所有 167 種組合都訓練一次。

歷史 run 若只有 dataset fingerprint／本機 mutable path，不能證明來源 bytes 可還原；
有些 run 的 materialized path 已包含明確 release ID，應沿此證據尋回，不可一律換 latest。
新配方會補上 source bindings；缺失的舊 cold bytes 或 checkpoint 仍需獨立恢復證據。

既有服務依賴的 `research_dataset` 不在本次刪除範圍。只移除了本次擁有的 trial 暫存，
既有分鐘來源、來源 gaps／unavailable、非交易日原始觀測、收據、歷史模型與快取均保留。
