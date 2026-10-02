# penguin 源本保留與遠端訓練分工

現行契約：[storage.md](agents/storage.md#penguin-source-host-and-remote-training-ownership)。
本頁是 2026-10-01 的分工落地與清點紀錄；容量、筆數及服務狀態是觀測，不是永久設定。

## 1. 從資料不可替代性決定保留位置

penguin 是資料與服務機，不是常態訓練機。保留不可重新取得的觀測及來源證明，
把能由確定輸入重新計算的訓練工作集放在遠端，不用每個模型版本各存一套。

| 類別 | penguin 常態位置／處理 | 遠端訓練節點 |
| --- | --- | --- |
| 整理過的原始觀測、歷史修訂、成交／報價／公司行動／規則 | 既有來源工作區與 D 唯一權威冷庫；保留時鐘、單位、權利與 hash | 按需取確定來源 release |
| 來源 receipt、首次觀測時間、發布時鐘、目錄／主檔 | 隨來源保留；不能靠重新下載現在版本取代 | 跟來源一起驗證，不改寫 |
| 訓練特徵、label、research view、fold／tensor／panel／tape | 不再常態新增每個研究版本的熱副本；舊檔逐項驗證後遷移 | 使用既有 builder 按需生成，記錄來源／程式／設定／ABI |
| 網站、Discord、收集器與模擬交易所需的最小投影／模型 | 具名服務依賴，維持相容與可用；不得一併清掉 | 不改變 penguin 的部署權威 |
| 完成模型、獨有研究成果、歷史冷 release | 既有冷庫保存；新分工不是歷史刪除授權 | 完成後沿用既有驗證／ingress／冷發布流程 |

「源本」允許無損型別化、壓縮及去重，不表示所有 HTTP 暫存、日誌與失敗回應都需永久熱存。
但不能只留下現修值而丟掉已觀測到的歷史版本，也不能丟掉無法重抓的 Tick／BidAsk。
冷庫用不可變 manifest 引用 content-addressed 物件；release ID 不是又複製整套資料，
因此不按 release 名稱或年代直接刪 D 歷史。

## 2. 這次清點的定義與範圍

- 檢查 29 個本機邏輯資料／訓練輸入族群、27,701 個 Parquet 檔；17 個族群完成精確鍵稽核。
  163 個市場 YAML 中 157 個能解析，
  6 個引用缺少的基底設定，錯誤另存 `config_errors.json`，未改動那些實驗。
- `datasets.csv` 是族群摘要，`files.csv` 是實體檔／inode／Parquet footer 明細；
  `source_registry.csv` 為既有監控的 1,888 項來源登錄，包含 aliases／投影，不能相加算樣本。
- 相同 inode 不重複計實體配置量；加密貨幣以每個交易所自己的商品 × date，
  對主檔和 `_hot_tail` 求精確鍵聯集。不同交易所、價格調整、決策時鐘及 ABI 不當成同一來源。
- footer 列數不等於鍵去重後列數，更不等於可訓練樣本。樣本還取決於有限值、可交易性、
  lookback、因果時鐘及 fold；未做該模型的 eligibility 驗收，不宣稱已全部可訓練。
- 正在下載的來源不是全域凍結快照；逐檔檢查 inode／大小／mtime 是否變動，發生變動就標記失敗，
  不偽造去重結果。觀測時間逐族群保留。
- 容量是清點範圍內 Parquet 檔的 bytes；不包含所有原始附件、logs、cache 或 D 壓縮物件。
  這不是整台磁碟的總量，也未證明全庫跨版本 SHA-256 去重。

清點程式：[audit_training_source_inventory.py](../scripts/audit_training_source_inventory.py)。
結果目錄：`artifacts/data_quality/training_inventory_20261001/`。

## 3. 已量到的主要族群

2026-10-01 本機觀測；GB = 10^9 bytes。鍵稽核尚未做的項目明列，不冒充「完全無重複」。

| 族群 | Parquet 容量 GB | 列數 | 去重／角色界線 |
| --- | ---: | ---: | --- |
| 台股官方日線，2,757 檔 | 0.261 | 9,000,859 | 商品 × date 鍵精確去重；整理過的行情來源 |
| 台股正式公開特徵 | 1.330 | 9,611,482 | date × symbol 鍵稽核；服務投影，不加到日線當新行情 |
| 台股一分鐘成品，1,602 分區 | 13.901 | 313,047,232 | footer 列數；遠端重建候選，未做全鍵去重 |
| 台股分鐘 developing-v5，1,567 分區 | 17.600 | 305,624,677 | 與正式分鐘史重疊；不同 ABI，不相加 |
| 台股微結構秒級成品，15 分區 | 2.018 | 44,257,395 | footer 列數；原始 capture 必須獨立保留 |
| 全商品期貨實體契約日線 | 0.149 | 2,940,809 | date × product × contract × tenor_rank × session 鍵稽核 |
| 個股期貨歷史分鐘 v2 | 0.224 | 25,574,357 | footer 列數；v1 與其它分鐘表不當成額外獨立史 |
| 台指選擇權月全鏈 | 0.0157 | 2,359,942 | footer 列數；ATM 選擇表是投影，不另外加算 |
| 台指選擇權週全鏈 | 0.0041 | 529,604 | footer 列數；週／月合約 grain 仍需完整鍵檢查 |
| Yahoo 美股日線，15,605 檔 | 1.415 | 38,595,013 | 商品 × date 鍵精確去重，子目錄 cache 不在 selector |
| Yahoo 外匯日線，875 檔 | 0.221 | 5,753,706 | 商品 × date 鍵精確去重 |
| ECB／Frankfurter 外匯日線，870 檔 | 0.229 | 5,985,556 | 不與 Yahoo 混算；不同提供者／價格定義 |
| Bybit 永續一分鐘，878 商品 | 19.992 | 788,620,218 | 主檔＋尾檔不同鍵；788,621,970 原列含 1,752 重複鍵 |
| OKX 永續一分鐘，495 商品 | 26.537 | 458,883,334 | 主檔＋尾檔不同鍵；458,884,324 原列含 990 重複鍵 |
| Binance USD-M 一分鐘，574 商品 | 128.071 | 732,533,174 | 主檔＋尾檔不同鍵；732,534,322 原列含 1,148 重複鍵 |
| Bybit 日訓練表，397 商品 | 0.0655 | 412,011 | 一分鐘來源的聚合投影，不當作獨立原始市場 |
| Pepperstone 歷史日線，72 檔 | 0.0327 | 1,125,347 | footer 列數；含多資產，不是全為外匯 |
| 台股 54 通道 preopen core | 0.224 | 5,809,578 | date × symbol 鍵稽核；原行情的訓練投影 |

台股研究 v1／v2／v3 各有 9,631,769 鍵列，占約 0.525／0.534／0.966 GB；
FinLab v4 約 0.996 GB、9,624,546 鍵列；636 通道研究表約 1.200 GB、9,626,904 鍵列。
它們是特徵 ABI／授權／覆蓋不同的投影，不是五套不重複行情；相同鍵也不代表所有欄位位元組相同。

三個加密貨幣交易所的完整鍵稽核已併入 `datasets.csv`，明細保留在 `crypto_key_audit/`。
來源末時點分別是 2026-10-01 03:35／03:36／03:37 UTC；每族群有自己的觀測時間，
不是同秒鐘凍結的整庫版本。重複時間鍵不代表整列數值一樣，沒有因此刪除主檔或尾檔。
第一次遇到字串 date 的失敗記錄另存 `pre_string_key_audit.json`，不當成精確結果。

## 4. 訓練層與 cache：量到容量不等於已能刪除

本輪一次低 I/O `du -B1 -s`，保持來源與服務不變：

| 精確範圍 | 配置量 GB | 處理方向 |
| --- | ---: | --- |
| `artifacts/cache` | 73.796 | 分清每個 panel／tape／來源混合內容；訓練 cache 改遠端 |
| `data_tw_public/stocks/panel_cache_v2` | 44.831 | 可能供線上推論；先確認目前與稍後服務依賴 |
| `data_yahoo/us_stocks/panel_cache_v2` | 7.568 | 分類為遠端訓練 cache 候選，保留原 Yahoo 行情 |
| `data_bybit/perpetual_daily/panel_cache_v2` | 1.098 | 遠端訓練 cache 候選，不能連根刪 funding／1m |
| `artifacts/datasets` | 33.204 | 訓練 dataset 多版本，需檢查其 `source_inputs` 是否有獨有源本 |
| `artifacts/research_features` | 2.496 | 遠端重建候選；FinLab 權利與時鐘限制仍有效 |
| 正式分鐘／developing-v5／HFT 成品三根 | 33.572 | 重建候選，原始 kbar／capture 不在刪除範圍 |

上述約 196.6 GB 是這些範圍的配置視圖，不是承諾能立即釋出 196.6 GB。
已有 hard link、冷庫未完整涵蓋的新增來源、服務未來重開與未驗證重建，都會減少安全回收量。
取樣當下未見這些 cache／成品被 `/proc` fd／mmap／cwd 引用；只證明該取樣，不證明未來不會開啟。
此階段是清點，沒有刪除訓練資料、原始資料、模型或冷庫物件；後續實際清理另見第 10 節。

## 5. 冷保存的阻礙不能靠新分工跳過

只讀頭資訊發現：

- `tw-public` 最新頭的 release 發布於 2026-09-29，來源 freshness 是 2026-09-24；
  本機行情／正式表已到 2026-09-30。不能拿發布日期當成涵蓋 9/30 的證明。
- `tw-minute-train` 頭仍為 2026-09-10 的 release，本機分鐘成品到 2026-09-24。
- `bybit` 頭仍為 2026-09-26 的 release，本機 1m 到 2026-10-01。
- canonical `okx`／`binance` 目前沒有 per-node head；不能宣稱整段歷史已在 D 可恢復。

以上是 manifest／head 中的元資料，不是本輪完整物件解碼驗證。
`tw-minute-source-cold`／`tw-microstructure-captures-cold` 的 head 仍為 2026-08-20 發布的 release，
也需獨立稽核最新來源是否已被保存；訓練成品與源本的證明不互相替代。
FinLab／FinMind 的 catalog 保留 `publish: false`／帳號授權限制，不能為了遠端訓練直接公開或跨權利分發。

## 6. 已落地與仍待遷移

已落地：

- README 與所有 agent 必讀的儲存契約已採用新的 source/service-host 分工。
- 清點輸出標示 `storage_class` 與 `eviction_authorized_by_inventory=false`，不按名稱推斷刪除。
- 台股來源 reconcile 的 unit template 及 penguin 的 systemd override 都去掉自動生成
  研究 v1／v2／v3 的參數；保留正式表、source lock、交易時段延後與成功後冷發布流程。
  已 `daemon-reload`，未重啟 Discord、當沖、網站或 Syncthing，也未強行啟動 rebuild。

仍待逐項遷移：

- Bybit 既有 `stockagent-crypto-training-refresh` 同時下載原始 funding、生成日訓練表、
  稽核並依目前 catalog 發布。直接停 timer 會連來源取得一起停，直接跳過成品也可能破壞
  既有發布 freshness 契約，故尚未整條停掉。應把純來源發布與遠端訓練生成分離後再退役舊鏈。
- 既有源本混合 feature 檔須先證明每個原始欄位／觀測時鐘／歷史版本可恢復，再做 lean source ABI；
  不把目前 builder 能執行當成所有歷史樣本已可精確重建。
- 舊 local view/cache 的逐項移除仍需 cold／rebuild receipt、服務消費者、pin／lease 與程序引用守門。
  一般 unmanaged `panel_cache_v2` 不會因新規則自動受七日 GC 管理。

## 7. 自助操作與遠端工作順序

清點（只寫分析報告，不觸發來源 API、不解封、不訓練、不刪資料）：

```bash
cd /path/to/stockAgent
source scripts/runtime_env.sh
run_fintech_python scripts/audit_training_source_inventory.py \
  --output-dir artifacts/data_quality/training_inventory_CURRENT
run_fintech_python scripts/audit_training_source_inventory.py --help
```

遠端：固定 Git／config 和精確 release，使用既有 `stockagent-data use` 驗證後才解封。
不要在一次訓練途中重新解析 `latest`。需要 CUDA 的正式工作先做既有 strict preflight。
下列 builder 是既有入口，不是一鍵通用訓練指令；先確認來源路徑、calendar、receipt 和 ABI：

| 遠端產物 | 既有入口 |
| --- | --- |
| 台股一分鐘 label／execution／features | `scripts/build_shioaji_tw_minute_dataset.py` |
| 秒級微結構成品 | `scripts/build_shioaji_hft_dataset.py` |
| 核心台股 54 通道訓練資料與品質 mask | `scripts/curate_tw_day_trade_training_dataset.py` |
| 寬研究／TAIFEX／all-observed 表 | `scripts/reconcile_tw_public_training_features.py` 的明確 research 參數 |
| Bybit 日表／單站 funding 特徵 | `downloader/materialize_bybit_perpetual_daily.py`、`scripts/build_bybit_venue_daily_features.py` |
| panel／tensor／fold／execution cache | 既有 trainer／panel lifecycle，使用獨立可寫遠端 root |

對應來源不是 materialized root 內的可寫輸出目錄；在 immutable 來源外建立 node-local 工作區。
精確準備結果、來源 hash、Git／config／ABI 和環境寫入既有 receipt／run manifest。
只有 managed materialization 由七日 lease／pin／引用 GC 回收；其他 cache 使用其明確維護政策。

## 8. 第一階段清點的驗證界線

24 項 focused 測試通過，覆蓋 date 字串／Arrow 時間型別、主檔＋尾檔鍵聯集、無序／重複／空鍵、複合鍵、
來源混合分類、缺少目錄與原有 reconcile 行為。沒有啟動訓練或全庫 hash／restore。
服務 `active` 只表示程序存活；本輪不宣稱服務資料完整或遠端已重建所有資料。

分析 companion：`artifacts/data_quality/training_inventory_20261001/review.ipynb`。
三個程式格已在 canonical Python runtime 執行、schema／計數界線驗證通過；
環境沒有 `ipykernel`，沒有宣稱完成 Jupyter kernel 或互動 UI 驗收。

## 9. 不只日線：公開資訊與 provider 來源清單

2026-10-01 13:20（台北）來源登錄觀測，並逐檔讀取 248 個台灣公開資料 Parquet footer；
所有 footer 稽核成功。分類是可讀導覽，不是內容去重或歷史完整性證明。
資料表來自 `data_tw_public/` 的頂層、supplemental 與 MOPS/XBRL，不包含全部 provider 表。

| 初步分類 | 表數 | Parquet GB | footer 列數，未做唯一鍵去重 | 內容例子 |
| --- | ---: | ---: | ---: | --- |
| 財報／營收及 MOPS 表 | 141 | 2.385 | 154,222,001 | XBRL 項目、季度財報、月營收；部分 MOPS 基本資訊仍需細分 |
| 法人／持股／融資券 | 24 | 0.379 | 15,908,867 | TDCC 持股分布、三大法人、融資券、借券 |
| 公司／交易規則／目錄 | 19 | 0.0526 | 4,668,524 | 公司主檔、成分股、注意股票、當沖資格 |
| 公司行動 | 10 | 0.0053 | 210,783 | 除權息、股利、換股、移轉調整與權益 |
| 總體原始發布 | 17 | 0.0019 | 305,647 | CBC 外匯存底／貨幣、DGBAS、MOF 初報與修訂時鐘 |
| 衍生品 OI | 1 | 0.0016 | 234,214 | TAIFEX 大額交易人未平倉 |
| 價格／市場統計 | 21 | 0.414 | 20,113,045 | 指數、官方行情及結算統計；與非價格類分開 |
| 仍需人工分類 | 15 | 0.0109 | 1,431,777 | SITCA、期貨／選擇權日表、TPEx 其它資訊；原樣保留 |

這 248 表合計約 3.250 GB。長表的財報項目、報表時點與歷史修訂不是 1.54 億個
獨立訓練樣本；報表日期、交易日、發布時間和本機首次觀測時間分欄保存，不能互相代換。
1,888 項既有來源登錄含 FinLab／FinMind、FRED、CFTC、OpenBB、SEC、Coin Metrics 等
aliases／投影；缺值與 receipt 計數基礎也保留，不能把登錄數當獨立資料集數。
FinLab／FinMind 仍服從本機授權與 catalog `publish: false`，未將其公開或重新分發。

| 來源庫實體範圍 | 配置 GB | 保存界線 |
| --- | ---: | --- |
| `data_openBB` | 145.331 | 多供應商來源封存，不按大目錄名稱整個刪掉 |
| `data_finlab`／`data_finmind` | 20.383／20.358 | 授權來源、歷史值與 receipt；FinMind 持續更新 |
| `data_coinmetrics_community` | 12.180 | 社群鏈上來源；不因衍生特徵可算就丟掉原始值 |
| `data_tw_public/raw` | 12.391 | 原始回應／附件、修訂及首次觀測證據 |
| `data_tw_public/mops_xbrl` | 7.044 | 原稿與解析表；包含上方已計入的表，不能重複相加 |
| `data_free_public`／`data_public_economic` | 2.203／1.855 | 公開非價格、總體與 original release 證明 |
| `data_crypto_reference` | 0.464 | 商品／鏈／提供者參考資訊 |
| `data_cftc_legacy`／`data_fred_crypto_macro` | 0.0632／0.0067 | 歷史 COT 與宏觀來源，非任意編譯 cache |

這些是整個來源庫的 `du` 配置量，**混有價格與非價格資料，也不是非價格資料的加總**。
另外保留 raw_failures 約 0.096 GB：失敗回應可能是 acquisition 缺口證據，這次沒有順手刪掉。
清點沒有發送供應商 API、下載、解封或訓練請求。

結果：`artifacts/data_quality/public_information_inventory_20261001/`。

- `public_tables.csv`：每表大小、列數、schema、各時間欄範圍與檔案穩定性簽章。
- `public_source_registry.csv`：提供者、發布／觀測時鐘、權利、服務依賴與 receipt 計數基礎。
- `public_categories.csv`：上表初步分類摘要。
- `provider_storage.csv`：15 個非重疊來源根的配置量；MOPS 解析表與上方表格仍有計算視圖重疊。
- `public_summary.json`：範圍與限制；`all_history_complete_claim=false`、`training_eligible_samples_claim=false`。

## 10. 本次後續安全清理

先核對 enabled Discord modes、model selection 的 resolved config、隔夜引擎、服務的未來重開
依賴與 `/proc` 引用，再以 exact hash／recoverability 決定是否可回收，不以「舊／cache」名稱刪除。
本輪沒有停或重啟既有服務，也沒有刪除唯一來源、模型、D 冷物件或解除 pin。

| 已完成動作 | 數量 | 回收配置 bytes | 方法／恢復方式 |
| --- | ---: | ---: | --- |
| 七個非服務研究 panel 的完全相同 generation `.npy` | 117 個冗餘 inode | 5,420,744,704 | 同 SHA-256 並重查穩定簽章，原子 hard link；所有邏輯路徑保留 |
| 不在用的 TorchInductor／Triton 編譯 cache | 37,014 檔 | 1,533,542,400 | 既有 storage-pressure 工具；需要時由編譯器重新生成 |
| 中斷解壓 staging 的已證明冗餘副本 | 100,752 檔 | 8,754,040,832 | 冷庫全 checksum／ZIP 校驗、逐候選獨立解碼與本機 SHA-256 比對後 unlink |

上述合計 **15,708,327,936 bytes（15.708 GB）**。編譯 cache 是本次使用者要求下的
手動立即清理，經 train／compiler process 與 fd／mmap 守門，使用 `--min-age-days 0`，
未使用 `--force`，**沒有改自動七日保留政策**。

`tw-public-20260916T165816562965668Z-l0-penguin-53527a472d53e98e` 的中斷解壓目錄
dry run 與 apply 都完成 2,197 個冷物件全 checksum／ZIP 校驗（16,717,240,735 object bytes），
以及 100,752 個候選檔的獨立解碼 SHA-256。apply 已回收 8,754,040,832 配置 bytes；
清理後逐一確認候選路徑不存在，沒有未知殘留，冷 manifest hash 相同、所有引用物件仍存在。
這不是把整個完整 materialization 標成 `READY`，也不是 metadata／大小比對而已。
2026-10-01 14:13 的 WSL 可用空間約 80.53 GB，仍低於系統 5% free-space guard；
收集器同時持續寫入，`df` 差值不是這次刪除量的精確度量，以 apply 收據為準。

收據位置：`artifacts/operations/source_host_cleanup_20261001/`。

- `cleanup_summary.json`：三項 apply 的配置量加總、來源／模型／冷物件刪除 0 及驗收界線。
- `service_dependencies.json`：服務設定、實際 model selection 與資料／cache 邊界。
- `panel_dedup/panel-dedup-apply-20261001T053000.459459Z.json`：完整 48 組 SHA-256、117 路徑及配置量。
- `panel_dedup/post_verify.json`：清理後重新核對每組 hash 與所有 alias inode。
- `compiler_manual/storage-pressure-apply-20261001T054547.266774Z.json`：process/open/changed skips 全部 0，錯誤 0。
- `partial/`：指定 staging tree 的 dry-run／apply 每檔選取與保留原因、冷校驗及獨立解碼證明。
  本次 apply 是 `partial-apply-1790835065864447002.json`，`post_verify.json` 是清理後確認。
- `materialized_gc_dry_run.json`：完整熱版本的 lease／pin 守門。
- `bybit_venue_rebuild.json`：因來源 identity 變更而拒絕重建／刪除的證據，不是假成功。
- `runtime_latest.json`／`syncthing_latest.json`：清理後程序、HTTP、既有 guardian 與冷庫同步觀測。

這是 WSL ext4 的配置空間回收，不代表 Windows C: 的 VHDX 已自動縮小。
這次沒有停止 WSL、做 VHDX compaction 或把 D: 單份冷庫稱為獨立備份。

## 11. 目前不能安全刪除的項目

- `data_yahoo/us_stocks/panel_cache_v2` 約 7.568 GB：舊 ABI v19，15,781 個 cached symbol 中
  **1,918 個找不到對應現存源檔**。不能只憑「現在 Yahoo 檔還在」宣稱所有舊觀測可重建。
- `data_bybit/perpetual_daily/panel_cache_v2` 約 1.098 GB 是舊 v54；兩個 v55 研究 cache 約
  0.529／0.137 GB 的 retained-input identity 已不同。新 builder 能跑也不等於位元組精確還原。
- 四個 managed 完整熱版本仍 pinned：部署中的模型／來源及既有 research pin。沒有擅自解除。
- 另一個 `tw-public-20260825T011559079640521Z-l0-penguin-d00c44591f4a85b7` 的中斷解壓約
  5.636 GB，對應冷 pack 缺失，保留等待恢復；絕不把新下載的不同內容偽裝成舊 release。
- 已 enrolled 的三個 Bybit 完成訓練 run 及 legacy crypto／US 成果仍受七日使用 lease 保護；
  最早的已登錄 lease 於 2026-10-02 到期，US 於 2026-10-08 到期。這不是源資料刪除許可。
- 13:44 的冷庫同步檢查：penguin 本地 need bytes/items 0、錯誤 0；vastai1T QUIC 已連線，
  completion 100%／need bytes 0，但 **need items 尚未歸零**。canonical artifact retirement
  因 `cold-peer-not-converged` 延後；沒有繞過 gate 或宣稱完整多機收斂。
  14:09 再檢查 vastai1T `needItems=240`，penguin `idle`／need 0／錯誤 0，仍維持延後。

依賴表包含四個當沖 mode、隔夜引擎、網站、即時推論 panel、v8 年化 cash panel、分鐘 parity
來源、execution tape 與 Shioaji／TAIFEX 來源。某檔此刻未被 fd 打開，不等於夜間 replay 或下次服務重開不需要。

## 12. 安全清理的自助命令與驗收

公開資訊清點（只寫報告）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_training_source_inventory.py \
  --public-information --output-dir artifacts/data_quality/public_information_inventory_CURRENT
```

研究 panel 無損去重（預設 dry run；只接受 allowlist，不接受任意 artifacts 目錄）：

```bash
run_fintech_python scripts/deduplicate_inactive_panel_caches.py \
  --panel tw_public_preopen_raw_v1 \
  --panel tw_public_preopen_raw_long_history_v1 \
  --panel tw_public_preopen_pit_v7 \
  --panel tw_public_preopen_pit_v7_649 \
  --panel tw_public_preopen_pit_pinned_20260916 \
  --panel tw_public_verified_features_20260917 \
  --panel tw_public_no_unvintaged_macro_20260917
```

看完收據、確認服務消費者未變，再在同一命令加 `--apply`。每個 panel 的既有 `.write.lock`
以非阻塞方式鎖定，apply 仍重新確認 hash／inode 穩定性；不刪邏輯路徑。
若新增服務使用某個研究 root，工具會拒絕，並應先移除該 allowlist 名稱。

精確重建驗證（目前刻意只支援 plain Bybit daily family；不訓練、不寫另一份 cache、不刪資料）：

```bash
run_fintech_python scripts/verify_panel_cache_rebuild.py \
  --config configs/markets/bybit_perpetual_daily_0005_historical_pit_trajectory_v1.yaml \
  --receipt artifacts/operations/panel_rebuild_proof_CURRENT.json
```

它用既有 canonical builder，在 cache load/save 邊界禁止重用／寫入，再逐 logical array digest
與 symbols／features／backend ABI 比較；輸入或 ABI 不符直接拒絕。即使 `exact_rebuild_verified=true`，
也不代表 consumer／pin／lease／cold gate 已通過，輸出始終 `eviction_authorized=false`。
中斷解壓回收命令及所有守門見 [README 的 prune-partial](../README.md#prune-partial安全回收中斷解壓的暫存副本)。

94 項 focused 測試通過，包含公開資訊／時間欄清點、去重、精確重建、變動／redirect 守門、
獨立解碼 hash、未選冷物件仍完整校驗、packed／cache／process-reference 回歸。
Ruff F 檢查及 `git diff --check` 通過。

14:11 程序觀測：Discord、四 mode 當沖引擎、公開網站、來源事件、三個 FinMind worker 與
Syncthing 都 `active`，原程序未重啟（`NRestarts=0`）；兩個 dashboard `/healthz` 返回 200，
當沖 status 持續更新且四 mode 均存在。這只證明程序與讀取入口仍可用。
既有 guardian 仍報 **09:00 causal live SLA 未達成**（此次 13:05 後清理之前的當日開盤問題），
以及磁碟低於 5% 門檻；另有 source-event／post-close maintenance 與部分下載器警告。
沒有改 gate 來消除告警，也沒有把 replay 稱為當日 causal live 成交。
沒有啟動訓練或宣稱全服務、全歷史、全冷庫驗收完成。

## 13. `artifacts/markets` 封存與退役，2026-10-01 晚間至 10-02

本輪只處理 penguin 的 market 成果與對應舊 hot mirror，不操作遠端機器。
23:53 的逐檔清冊有 54 個 root，regular file 去除相同 inode 後配置量
492,467,249,152 bytes；含目錄的 `du` 約 493.32 GB。收集器與其它工作仍在寫入，
這不是全機凍結快照或保證可回收量。

| 盤點分類 | Root 數 | 處理 |
| --- | ---: | --- |
| 服務設定／候選模型／隔夜消費者引用 | 5 | 保留工作路徑與受管理連結 |
| 有近期寫入，需另查用途 | 24 | 不做整個 root 的退役 |
| 至少七日未變動、尚無冷庫登錄 | 23 | 逐項 allowlist、封存、獨立解碼及來源 SHA-256 驗證 |
| 已有 legacy 冷存版本 | 2 | crypto／US；刪除前仍重新驗證，不以既有 head 代替恢復證明 |

23 個新候選共有 2,496 個邏輯檔，11,188,660,675 logical bytes，root 內去除
相同 inode 後共 8,098,611,200 配置 bytes；兩者不相加。最大的期貨 margin 準備
root 約 309 GB、632 萬個檔，包含原始規則證據及近期新版本；本輪不整包移除。
部分研究 root 的最新寫入只是當日生成的 deployment report，但不能因此偽造
七日穩定或使用租約已到期，也不能按 root 名稱推斷成果可以重建。

封存仍走既有 `legacy_artifact_archive`／content-addressed packed 冷庫，D 槽為唯一
冷庫；不在 C 槽新增另一套 archive。它保留唯一模型、未完成成果、原始路徑與 hash，
標示 `deployable=false`，不是訓練完成或可上線證明。
`stockagent-legacy-archive-stage` 在 D 槽，暫時保留恢復驗證／退役所需的可續跑工作集。

新增共享消費者守門，full-run 與 legacy 退役都會解析實際 model selection、候選
root／config、experiment input／initialization、Discord runtime enablement、隔夜設定與
受管理連結；legacy 也檢查 pin。其它收集／期貨準備用途仍須獨立審查，不能只看 Discord。
修正 legacy CLI 與排程誤用舊冷物件保留 peer 條件的問題：現行 hot 退役只要求本機
Syncthing 健康及 `artifact_retirement.json` 指定的 peers，這次該清單為空。
這不表示 vastai1T 的同步已完成，也沒有啟動舊 C 冷物件 retention。

退役 timer 已改為每小時一次，保留七日 lease，不倒填日期。未到期只做 cheap negative gate；
到期才再次完整核對 D 冷庫、原始內容、舊 mirror、pin、使用者與傳輸狀態。
單一 dataset 的唯讀恢復／mirror 證明失敗會保留該項並繼續其它計畫；apply 失敗仍停止，
不隱藏 quarantine 或未知刪除結果。這次尚未取得跳過七日等待的選擇，不能宣稱已回收熱資料。

發現的具體保留原因：

- 一筆微小 raw-L1 封存的 Syncthing scan request 曾逾時；保留 durable pending intent
  與來源，使用既有 retry 工具，不把未確認掃描叫做多機送達。
- `markets/tw` 舊 mirror 的 `walkforward_first_year_cumulative_returns.png`
  與 repository 版本大小不同。未知舊版本不當作重複檔刪除，需補完整冷保存證明。
- 三個已 enrolled Bybit 完整 run 的當前來源 portable SHA-256 fingerprint
  與各自冷 manifest 相同；這次 probe 沒有重做冷 payload／decode，刪除時仍須完整驗證。

本輪完整執行結果以 `artifacts/operations/market_artifact_cleanup_20261001/` 為準：
`inventory.json`／`roots.csv`、`archive_progress.json`／`archive_results.json`、
`hot_mirror_preflight.json`、`lease_observation.json`、`retained_roots.json`、
`service_dependencies_current.json` 與 `enrolled_source_preflight.json`。
其中 inventory 與 preflight 始終不是刪除許可；失敗項不列作已驗證封存。
93 項 storage／consumer／CLI／排程 focused 測試通過，JUnit 收據為 `pytest.xml`。
Ruff 檢查通過。這是分階段工作紀錄，不宣稱所有 market root 已清完。

### 本輪封存驗收與仍待處理的刪除

01:06 的小檔重試完成 source／cold 校驗，沿用原 release，沒有重做另一套完整副本。
最終 **23 個 root、2,496 個邏輯檔、11,188,660,675 source bytes** 已完成封存與
獨立解碼；各 release 的 `stored_bytes` 共 **6,761,391,931 bytes**，不包含 D staging，
也不是整個 D 磁碟新增量或跨 release content-addressed 去重後的增量。
原始一次通知逾時的收據保留；retry 時已無 pending intent，再執行 canonical
publish/verify 成功，不能把單純 `idle_no_pending` 當成恢復證明。

25 個 legacy root 的 mirror preflight 中 24 個通過；`markets/tw` 因上述不同版本圖表
保留且未加入自動退役清單。已將另外 22 個新封存 root 加到既有 allowlist，連同原有
crypto／US 共 24 個 legacy root，以及三個完整 Bybit run，由 hourly timer 重查。
新封存的使用 lease 於台北時間 **2026-10-09 00:07–01:03** 間到期；實際回收仍以
到期後所有重驗通過為準，不保證那時必然釋出全部盤點容量。
crypto 於 10-02 11:45、Bybit 三項於 10-02 10:54–10:57、US 於 10-08 09:25 到期。

**本輪 repository／hot mirror／冷物件刪除量皆為 0，沒有宣稱已釋出 C 空間。**
當前 dry run 27 個排程項目均因有效七日 lease 延後，沒有跳過等待或解除 pin。
`cleanup_summary.json` 是本輪量化摘要，`archive_retry_result.json` 記錄修復後的封存
校驗，`scheduled_retirement_dry_run.json` 記錄仍待使用租約到期的事實。
`runtime_current.json` 與 `syncthing_current.json` 是結束時的程序／容量／本機同步觀測，
不代表全資料完整或多機送達驗收。

## 14. 明確授權的單次立即回收，2026-10-02

使用者回覆「不等七天直接回收」後，本輪只略過熱資料使用 lease 的七日年齡。
不倒填 `last_used_ns`，不改 hourly timer 的七日政策、不解除 pin、不刪 D 冷庫物件。
`--manual-immediate` 必須同時存在於 dry run 與 apply，並納入計畫 fingerprint、
retirement state 與獨立操作收據；一般計畫不能冒用為立即刪除計畫。

延伸既有 full-run／legacy 退役工具，而不是另建清理框架。批次只選已 enrolled 且
列在既有 allowlist 的 27 個 root，較小項目先處理，US 最後。
每項重新驗證 D 冷庫所有 payload、獨立解碼／來源雜湊、舊 hot mirror、服務、程序、
pin 與本機 Syncthing 健康。rename 後、真正 unlink 前再次檢查引用、pin、舊 bridge、
D guard 與新的 Syncthing 收據；失敗保留 quarantine，不宣稱回收完成。

這次改用 inode 的 `st_blocks` 與 `st_nlink` 計算已移除的配置 file bytes。
repository 與 mirror 共用的內容只算一次；仍有外部 hard link 的內容不算回收量。
不把來源邏輯容量、目錄開銷、其它收集器寫入或 Windows VHDX 縮小混在同一數字。

證據放在 `artifacts/operations/market_artifact_manual_retirement_20261002/`：
`preflight.json`、每項 `.plan.json`／`.result.json`、`progress.json` 及最後的 `summary.json`。
初次唯讀檢查遇到 Syncthing 的定期掃描，未刪檔；中斷收據保留在
`attempt-1-interrupted.json`。重試的完整 plan/apply 收據在 `attempt-2/`，會短暫等待
掃描回到 idle 後才再次判斷，不降低傳輸守門；自動 timer 不使用此手動等待模式。
preflight 量到 27 個 root、146,391,342,703 source logical bytes，服務引用為零；
這仍不是刪除／恢復證明，實際結果以逐項 apply 收據為準。
109 項 focused 測試通過，`pytest.xml` 包含恢復、年齡 bypass、缺冷物件、引用、pin、
quarantine、計畫模式不相符、自動七日規則及逐項批次收據測試。Ruff 通過。

本輪仍保留 `markets/tw` 的未知舊圖表、約 309 GB 的混合
期貨原始證據／準備目錄與所有被當前服務引用的模型資產；不宣稱全 `markets` 已清空。

回收時發現多個 run 的 `loss_contract.json` 共用 inode；刪除已驗證的其它名稱改變
ctime，導致原先把歷史 signature 當內容證明的 legacy 守門誤判。失敗時仍保留來源。
已分開「原始內容／可恢復 metadata」與「當前穩定性觀測」：仍要求 exact SHA-256、
size、mtime、mode；當前 device/inode/ctime 在雜湊前後及 plan/apply 間必須穩定，
歷史觀測差異則保留在 `source_metadata_drift` 收據中。不改既有 D 冷 manifest，不因
ctime 相同就認定內容相同，也不因 ctime 不同就把相同內容重新複製進冷庫。
修正後新增 hard-link 刪除引起 ctime 變動、連續回收兩個共用 inode 的 run、恢復原始
兩個邏輯 root、雜湊期間寫入、內容／mtime／mode 變更仍拒絕等測試。
第二次執行完成 19 項回收後，在下一項唯讀驗證階段中斷，套用修正後只重查剩餘
8 項；已完成收據保留於 `attempt-2/`，其餘結果在 `attempt-3/`，不重複加總容量。

### 已回收結果與大型項目的監督執行

截至交接，一共 **24 個 root 已回收、3,280,912,384 allocated file bytes**（約 3.28 GB）。
已一起移除 repository 與舊 hot transport 的已驗證名稱；D 冷物件刪除數仍為零。
這不是 Windows VHDX 壓縮量，也不是把較早其它清理的 15.71／115 GB 重新加總。

剩餘三項是 `tw_public_lanten_market_candles_tw_cash_all_available`、crypto、US。
大型檔案需要完整雜湊／獨立解碼，不以發布成功或舊收據直接授權刪除。
前景工作只在下一項 **唯讀 planning** 階段中斷，沒有中斷 rename/unlink。
後續由一次性 `stockagent-market-manual-retirement-20261002.service` 執行；只挑原
`preflight.json` 已授權且未完成的 root，走相同 canonical plan/apply。
完成回收後會對所有已回收 release 再做一次完整冷物件／解碼檢查，最後觀察本機
Syncthing 與容量，才寫總結 `summary.json`。`state=complete` 才是全部 27 項的驗收。
在那之前不能把 service active 或單項成功稱為全量完成。

```bash
systemctl status stockagent-market-manual-retirement-20261002.service --no-pager
journalctl -u stockagent-market-manual-retirement-20261002.service -n 30 --no-pager
source scripts/runtime_env.sh
run_fintech_python artifacts/operations/market_artifact_manual_retirement_20261002/continue_and_finalize.py --report-only
```

`attempt-4/` 保存剩餘三項逐項收據，`background-job.json` 記錄 PID 與選定項目；
最後複驗進度為 `final-audit-progress.json`，每項為 `.post-unlink.json`。
一次性 unit 不啟用開機排程、不重啟既有服務、不替換七日 timer；WSL 關閉不屬於
完成證明。任何失敗保留 blocked/quarantine 收據，不可手動清空。
