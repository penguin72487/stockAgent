# 當沖特徵：應有值缺口、Provider 優先修補與新版來源

## 1. 執行進度

2026-10-05 續作：使用者已確認清理完成；新版 v4 已發布並經 Syncthing 完整交付，新的遠端 panel 正在重建。以下為 10/04 暫緩時的歷史驗收狀態，最新進度與固定版本見 [遠端恢復交付報告](tw_feature_gaprepair_remote_resume_2026-10-05.md)。

核對時間：2026-10-04 14:53 UTC（台北 22:53）。最後已完成台股交易日是 **2026-10-02**。本次沒有啟動訓練、改模型、覆寫舊 panel、變動網頁部位或刪除原始資料。

- 全來源目前清冊：12,452 筆欄位表示；當沖已選經濟量 14,720 項逐項列出。來源欄位表示、經濟量、股票×原生期別觀測與訓練通道不是同一個計數。
- 最終 v4 相較最初固定來源，原 NULL／不存在座標新增 **173,139 筆真實觀測**，原非空值消失 **0 筆**、有限值修訂 **0 筆**，已通過完整逐項比較。完成重查的 raw-current v3 曾少了 v2 的 **80,285 筆歷史非空值**；不能只比最早原始版本便宣稱新版沒有掉值。v4 已逐筆保留這 80,285 筆真實舊觀測，最新非空值與真實零值不被覆寫。
- 新版內 **7,821 筆營收替代來源補值已逐筆驗證真正寫回模型來源表**，不是只保存一份補值檔或宣告計數。既有真實零值與非空值保持不變。
- 交易量為正卻缺正數 OHLC 的必要價格缺口 **0 筆**。這只證明日 OHLC；不等於所有歷史 09:01 分鐘、企業行動或券源資料都完整。
- **377 個 FinLab key** 已插入既有 owner 的單次優先重查；14:21:44 UTC **377／377 全部完成真實 upstream check**。[逐項檢查狀態](../artifacts/data_quality/tw_feature_expected_gaps_20261004/priority-status-final.csv)。檢查完成不等於 provider 補齊所有 NULL。保留既有 quota、cooldown、授權和 owner lock。
- **195 項相關回歸通過（20.65 秒）**；包含原回應範圍縮減、歷史觀測保留與惡意修改補值後仍能拒絕。不是整個 repository 全套測試或雙卡完整 fold 驗收。
- 先前 `/srv/stockagent-live/data_tw_daytrade_panel_sources_20261004_gaprepair_v2` 已通過完整來源 SHA 及實際補值寫回驗收；14:00:42 UTC 冷發布退出碼 0。它仍是保留的歷史冷版本，不當成最終 latest。
- 遠端既有 `code-gaprepair-v14` 的核對發生在使用者暫緩交付之前，且它早於本次歷史保留修正。本機另封存包含新 consumer 的程式版本，**沒有把這個新版本送去遠端**；程式、資料、panel 及訓練驗收分開。
- Vast Syncthing 回報 `insufficient space on disk for database (/opt/syncthing/data/index-v2): current 0.83 % < required 1 %`。使用者已指示 **先不傳遠端，待自行清理空間**；不啟動新 hydrate／panel rebuild，不降低門檻或刪既有檔案。新版後續資料留在本機待交付。
- 最新本機版 `/srv/stockagent-live/data_tw_daytrade_panel_sources_20261004_gaprepair_v4`，source SHA `167190ad3e17d43aa590bdb4160e8f9c26e5ffe9951e7d9f9069683e0280811f`，包含重查後 **102 個變動 FinLab 來源**及 **100 個需要保留歷史的數量**，保留全部真實營收補值和既有 native source。完整 source／retention 驗證與缺口 audit 已通過；新 registry entry `publish=false`。

本次任務：`tw-feature-expected-gaps-20261004`。原始失敗／取消候選與記錄均保留。來源 v1 候選不允許發布，沒有發布 head；v2 是先前通過的冷發布版本，raw-current v3 只保留作回應縮減證據，v4 是最新本機驗收來源。不能用檔名最大版本號替代資料驗收。

## 2. 第一性原理：哪一種 NULL 才該向 provider 要資料？

原始 panel 是「日期、實體、經濟數量」的 nullable 座標表，不是要求每個股票每天都有每一個財務科目的實心立方體。

「應有值」至少需要同時成立：當時存在且適用該實體、科目或系列有定義、已到應公布時點、位於來源可提供歷史範圍，而且不是停牌／事件未發生／比率分母為零等結構性空值。只有真實觀測才能填入原始表；最近已公布值的延續、資料年齡及可用性屬訓練 view，不算新的原始觀測。

| 狀態 | 本次判定 | 採取動作 |
| --- | --- | --- |
| 已證實交易，但必要價不存在或不正確 | 本次日 OHLC 0 筆 | 若出現才走 canonical 官方價格修補；不插值成交價 |
| 同科目同實體在前、後原生期別有值，中間缺值 | 待核對提示；不是 optional 科目必須存在的證明 | 377 key 單次優先向原 provider 重查 |
| 已取得同義、同單位的真實替代觀測，重疊驗證通過 | 可做 missing-only 聯集 | 寫回新版來源，保留每筆來源與公布下界 |
| 月／季／週資料當天沒有發布 | 正常頻率／未觀測座標 | 原始 NULL；訓練依既定 TTL 使用最近公布值、available、age、updated |
| ETF 沒有營收、公司未披露選擇性科目、分母為零 | 不適用或未有存在證明 | 不補零、不照全股票全季度強制下載 |
| 上市前、下市後、停牌、來源起點之前 | 生命週期／可用歷史限制 | 保留 eligibility／lifecycle 與經證實的來源範圍 |
| 原始資料已經有值，但尚未接線單位／公布時鐘 | Adapter 工作，不是下載失敗 | 清冊單獨標示；不重複打 API 或把 period end 當公布日 |
| 新回應比已接收版本少了歷史非空值或整個實體欄 | 已證實回應範圍縮減，未證實原因 | 同數量、同原生軸與同授權重驗；僅保留真實舊觀測，最新非空值優先 |
| 讀取當下碰到原子收據切換 | 必須獨立重查，不先說壞檔 | 核對當前筆數與完整 SHA，保留原掃描證據 |
| Provider 已回覆全 NULL | 有來源回應，不是本機漏存 | 保留全 NULL 與正常 cooldown，不能偽造為零 |

## 3. 全部清單：來源、期別、實體都可以追查

以下清單不只展示前幾項；逐項 Markdown 與精確 Parquet 座標均已產出。

| 清單 | 完整證據 |
| --- | --- |
| 全部目前已選 14,720 項的缺值判定 | [148 份 Markdown 與索引](../artifacts/data_quality/tw_feature_expected_gaps_20261004/native-audit-v4/index.md)、[完整 CSV](../artifacts/data_quality/tw_feature_expected_gaps_20261004/native-audit-v4/selected_feature_status.csv) |
| 508 個 FinLab 來源的原生槽位、非空值、NULL、待核對數 | [source_period_status.csv](../artifacts/data_quality/tw_feature_expected_gaps_20261004/native-audit-v4/source_period_status.csv) |
| 377 項 provider 優先清單及全部已重查狀態 | [provider_priority_worklist.csv](../artifacts/data_quality/tw_feature_expected_gaps_20261004/native-audit-v4/provider_priority_worklist.csv)；每列 `gap_file` 連到全部 `period × symbol` 座標 |
| 全來源 12,452 個欄位表示的原因與動作 | [Provider 狀態索引](../artifacts/data_quality/tw_feature_expected_gaps_20261004/provider-status-v3/index.md)、[all_source_field_states.csv](../artifacts/data_quality/tw_feature_expected_gaps_20261004/provider-status-v3/all_source_field_states.csv) |
| 每一來源數量實際增加／修訂／消失多少值 | [Before/after](../artifacts/data_quality/tw_feature_expected_gaps_20261004/source-comparison-v4/index.md)、[全部變化 CSV](../artifacts/data_quality/tw_feature_expected_gaps_20261004/source-comparison-v4/finlab_source_changes.csv)；`new_observations_path` 是全部新增的原始鍵和值 |
| 新回應相較已收到 v2 的歷史缺值與修訂座標 | [縮減證據](../artifacts/data_quality/tw_feature_expected_gaps_20261004/source-v2-to-v3/index.md)、[逐來源 CSV](../artifacts/data_quality/tw_feature_expected_gaps_20261004/source-v2-to-v3/finlab_source_changes.csv)；`removed_finite_path` 包含每筆舊原始鍵和值 |
| 最終 v4 對已驗收 v2 的獨立不掉值檢查 | [完整比較](../artifacts/data_quality/tw_feature_expected_gaps_20261004/source-v2-to-v4/index.md)、[逐來源 CSV](../artifacts/data_quality/tw_feature_expected_gaps_20261004/source-v2-to-v4/finlab_source_changes.csv) |
| 舊缺口修好多少、為什麼提示總數反而變多 | [缺口前後比較](../artifacts/data_quality/tw_feature_expected_gaps_20261004/gap-transitions-v3/index.md)、[逐項 CSV](../artifacts/data_quality/tw_feature_expected_gaps_20261004/gap-transitions-v3/gap_transitions.csv) |
| 新來源不能沿用舊 alias 的 QA 結論 | [fresh_source_union_checks.csv](../artifacts/data_quality/tw_feature_expected_gaps_20261004/native-audit-v4/fresh_source_union_checks.csv) |
| 過去更廣的有意義／排除特徵、單位／context | [完整語意清冊](tw_daytrade_meaningful_features_2026-10-04.md) |

新版候選原生缺口 **1,407,557**；其中 **14,771** 有通過這個新來源重驗的替代聯集，另外 **1,392,786** 仍是待核對提示。它們不是 139 萬筆已證實 provider 漏下載，也不是 139 萬個獨立 feature。

### 3.1 最優先且仍需要核對的核心項目

| 來源 key | 新版原生期別待核對座標 | 說明 |
| --- | ---: | --- |
| `financial_statement:資產總額` | 3,126 | 資產總額通常應有；仍須核對公司／報表基礎／財年和合法適用期別 |
| `financial_statement:負債總額` | 3,126 | 不把未有來源回應的缺值當零負債 |
| `financial_statement:每股盈餘` | 1,569 | 需保留股數／單位與報表期間口徑 |
| `financial_statement:營業收入淨額` | 1,545 | 金融業報表呈現不同，不用全股票同一科目強制要求 |
| `monthly_revenue:當月營收` | 491 | 對有前後實際營收的來源期別重查；未到期、上市前或 ETF 不算每日缺口 |

較大提示如「發放現金股利」「收取之股利」「短期借款減少」亦完整列在清單並接受原 provider 重查。沒有發放／收取股利或沒有某項借款流量的季度，不能僅因前後曾有值便宣稱中間一定漏下載。

### 3.2 為什麼補值後，缺口提示反而增加？

逐筆比較原來的 1,350,228 個候選與新版：**12,124 個舊來源缺口不再缺值**；**1,338,104 個仍缺**。新觀測擴大許多實體「第一筆至最後一筆真實值」的範圍，又揭露 **69,453 個新的中間期別提示**，因此新總數變成 1,407,557。這是核對範圍擴大，不是新增資料把舊非空值刪掉；非空觀測消失的獨立核對值是 0。

這些 native-period 候選亦不同於 [舊 panel availability coverage](../artifacts/data_quality/tw_day_trade_factorized_panel_20261004_v1/metadata-v13/feature_coverage.csv) 的日期×全股票分母；後者不能直接拿來當「理論必須有值」的缺值率。新清單引用的 panel coverage 仍明示為舊固定 view，不冒充新版 view 已重建。逐項 CSV 的 `next_action` 與 provider 狀態已區分「已重查仍待核對」；Markdown 的待核對數不是要求再把同一批無限重新排隊。

## 4. 實際補回的資料與不得混淆的計數

| 來源家族（最初固定來源 → 最終 v4） | 舊 NULL／不存在座標新增真實觀測 | 原非空值的 provider 修訂 |
| --- | ---: | ---: |
| FinLab 原生財務期別 | 110,485 | 0 |
| FinLab 財務衍生基本面 | 54,833 | 0 |
| 營收，經 FinMind missing-only 交叉修補 | 7,821 | 0 |
| 其他來源數量 | 0 | 0 |
| 合計 | 173,139 | 0 |

例：資產總額及負債總額各增加 2,261；EPS 增加 924；當月營收補 3,477、上月營收補 4,344。這是來源表的精確原始座標計數，包含當前 provider 版本與歷史觀測，不是宣稱同數量的新可交易股票、獨立經濟量或已滿足公布時鐘的訓練樣本。部分觀測在舊 panel 的替代來源聯集已可用，不能再計一次模型新增量。

營收修補的單位／同義 gate 不變：至少 100 個重疊點，99.5% 一致，僅補 primary 沒有真實值的格子。當月營收一致性 99.8416%、上月營收 99.8743%。另外六項累計、年比較與增減率約 79%～92%，**未通過便不使用**；保留不一致鍵及 NULL，不用降低閾值或推測倍率補滿。

7,821 筆中，2014 年起有 4,821 筆：當月 2,624、上月 2,197；其餘是來源本身較早歷史。此處最新補入的營收 subject month 最晚 2026-08，沒有把尚未普遍發布的 9 月營收偽造為完整月。

### 4.1 有替代來源值，為什麼仍不直接補？

財務科目名稱相似，不代表報表基礎、單位、累計／單季、原始／重編版本都相同。FinMind 對原生 FinLab 的資產、負債、EPS、營業收入重疊一致性約 98.0%～99.1%，低於保留的 99.5% 門檻，因此不直接合併；revised FinLab 在這些科目雖然約 99.998%～100% 一致，卻沒有額外缺口座標可以補。精確重疊數、衝突數與 `filled_keys` 都在 [最終重驗清單](../artifacts/data_quality/tw_feature_expected_gaps_20261004/native-audit-v4/fresh_source_union_checks.csv)。

這屬「替代值存在、但來源口徑／版本一致性不足」；不是「FinMind 全無資料」，也不構成「FinLab 檔案損壞」的證明。這次不調低門檻、猜倍率或改掉既有非空 primary；FinMind 原生數值仍保留在 source 包，無法安全合併的觀測與真正沒有來源回應的缺口分開。

[完整補值／衝突與來源收據](../artifacts/data_quality/tw_feature_expected_gaps_20261004/monthly-fill-v2/bundle_manifest.json) 包含每筆原始期別、symbol、原始值、替代來源、wide projection SHA 及公布下界。非空修訂以最後捕獲的真實回應為準，不使用歷史保留器把舊修訂強蓋回去；舊版完整保留，不能以相同 fingerprint 續接舊 optimizer 或假稱新舊模型數值相同。

### 4.2 為什麼不能只取最新一個 response？

重查後 v3 相較已接收 v2，有 **80,285 筆原非空值不再出現**，涉及 **100 個來源數量**。其中 **28,535 筆**是整個股票欄不再存在，**51,750 筆**是欄位仍在但該期變空；涉及 45 個消失實體欄。這證明回應覆蓋縮減，不能據此斷言全是下市、授權變更或 provider 損壞。

v4 採 `tw_verified_same_provider_snapshot_missing_only_v1`：同 provider、同科目／單位／原生軸、同私人授權；沿用至少 100 重疊點和 99.5% 一致性，102 個變動數量皆通過，100 個實際有值可補。只在新回應缺少有限值時，保留先前 **確實收到** 的原始觀測；新回應的所有非空值與真實零值保持不變。沒有插值、倒推報表或補假零，也沒有新增 provider 請求。

每筆保留鍵和值、最新與先前 source manifests、兩份原始 projection 及 overlap QA 都被打進新 source 的 `source_retention/`；consumer 重新核對來源 SHA、經濟身份、實際舊觀測、最新非空不變、每筆寫回與總數。這是 current-revision 研究來源的歷史保留，不提升為 original-vintage PIT，也不假稱最新回應本身含有那些值。

最終獨立比較 v2 → v4：原非空值消失 **0 筆**、新增 **0 筆**、非空值變動 **756 筆**。這 756 筆以最新捕獲回應為準，值恰與最初來源相同；所以「最初來源 → v4」的有限值修訂數為 0，而「v2 → v4」為 756，兩個分母／起始版本不同，並不矛盾。80,285 筆保留恢復的是 v2 已有觀測，不再加進 173,139 的總增量冒充另外一批新值。[歷史保留 overlap QA](/srv/stockagent-live/data_tw_daytrade_panel_sources_20261004_gaprepair_v4/source_retention/agreement.csv)。

## 5. Provider 操作與沒有盲目重抓的項目

### FinLab

新增有限的 owner intent，消費者仍是既有 `stockagent-finlab-local-refresh.service`。開盤行情優先不變；本次研究重查在一般 backlog 前，並依核心科目與缺口量排序。沒有新增第二組下載器或繞過 `.sync.lock`。

真正 upstream check 的 `upstream_incremental`／`upstream_forced` 才能結束 intent；本機 cache read 不算。一次真實檢查即使結果 unchanged 也結束這次重查，避免同一 optional NULL 永久迴圈。反之 auth、quota、cooldown、expired intent 也不能被宣稱為已取得觀測。

可在不排新工作、不打 API 的情況下核對全部意圖；報告另外保留 expired／not-yet-due，而不是把 `total - pending` 一律當成功：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/queue_tw_feature_gap_priorities.py \
  --status-only \
  --receipt artifacts/data_quality/tw_feature_expected_gaps_20261004/priority-status-latest.json \
  --status-csv artifacts/data_quality/tw_feature_expected_gaps_20261004/priority-status-latest.csv
```

兩個已有 provider 全 NULL 證據的 key：`dividend_otc:權息`、`management_change_events:變更交易開始日`。保留來源證據和正常重試，不填假零。[清單](../artifacts/data_quality/tw_feature_expected_gaps_20261004/provider-status-v3/finlab_source_empty.csv)。

### FinMind

使用 Sponsor／Complement 的既有唯一 owner、SQLite queue、shared limiter 和收據。金融三家族的 canonical owner 2014 年以後季度 partition 已取得至 2026Q2；Q3 還沒到應完整公布的時間，不能以 2026Q3 檔空白要求重抓。Complement 重複財報歷史的待查不是 Sponsor 原始來源缺值。

新聞 10/03、10/04 初掃時筆數與收據遇到刷新切換；重讀當前收據並驗證完整 Parquet SHA 後兩筆皆正常，沒有把活躍來源停下重抓。[原異常與精確重查](../artifacts/data_quality/tw_feature_expected_gaps_20261004/provider-status-v3/exact_failure_rechecks.json)。此為這兩筆的完整驗證，不推論所有 FinMind 歷史都完整。

`deprecated_query_shape` 和 `outside_documented_range` 被分開列為舊 query／可提供歷史限制，不當成可以強迫取得的 numeric NULL。台股以外及權證 pending queue 不因本次當沖財務清點自動改優先或重新啟用。

### 已有數值、不是要再向 provider 要一次

- `twpub_mof_business_tax_log` 全 NULL：營業稅原始資料實際有帶符號值，native `twcad_mof_business_tax_raw` 已接線。負值（例如 2026-08 的 -5,628,183）無法取普通 log，不是 provider 沒給資料。保留 signed raw，不用正值過濾器或補零掩蓋。
- BEA／Census 有原始觀測，但尚缺各 program 的歷史公布時鐘 adapter；新私人 source 包保留 9／19 個來源表。不是把 period end 加一天就能解除 PIT 問題。
- 即期外匯的 `Trading_Volume` 沒有該 provider 的交易所全市場量定義；不算應有實際成交量。
- 未完整檢視的 19 個 legacy／Yahoo 欄位表示屬 inventory coverage 未證實，不是 19 筆已證實損壞資料。本次台股 canonical 股票來源另有完整 OHLC 檢查。
- 房屋、SHA、路徑、收據、純文字 metadata 和未有授權／歷史觀測證明的 TEJ 項目仍不混入模型數值輸入。

## 6. 修正實作、時鐘與驗收

本次三個整合根因有測試保護：

1. 補值後的寬表包含文字 `source_index`；原打包器把它也放進 numeric measure check，跳過 patched 表，後續 enrichment 又補回未修復原表。已將軸與數值欄分開；宣告的每筆 fill 必須在 actual model source 表有相同鍵和值，總數、bundle、files、SHA 全部相符，才可提交 `complete` manifest。失敗 candidate 不發布。
2. Provider 正常更新了 receipt head，但清冊 `source_path` 還指舊 SHA 物件。現在以捕獲的一份 current receipt 解析它對應的 immutable object；前後 rehash 同一物件，完整保存捕獲的 receipt body，不要求後來的 moving head 永遠不變，也不停止 owner。
3. 真實 upstream check 的最新回應不保證保留既有完整歷史。下載器新增欄位／非空數縮減警示，保留 immutable 原回應；新私人來源以具逐格證據的歷史保留修補，不修改 provider raw。Consumer 不能只檢查補值檔存在或宣告總數，也不能只與最早原始版本比較。新回應與已接收版本的新增、非空修訂和消失座標皆獨立保存。

新的缺口 audit 每次重新執行 missing-only overlap QA，不沿用舊 panel 的 alias `accepted=true`。新增必要價格檢查在一次檔案掃描中判斷所有 OHLC，語意測試包含無量／停牌、不正價格與 NULL；不以跳過檢查加速。

補值的已知公布下界一路保留到新 panel builder。FinMind `create_time` 是 provider ingest，不是公司首次公布時間；官方指出欄位自 2026-04-21 起才加入，bootstrap 當日不當成古早歷史公布證明。[FinMind 官方文件](https://github.com/FinMind/FinMind-Doc/blob/master/docs/tutor/TaiwanMarket/Fundamental.md)。FinLab 建議檢查時程也不是「值已完整」的證明。[FinLab 官方 API](https://finlab.finance/docs/en/reference/data/)。

此外，新 builder 不盲信 native macro 表內預先計算的 `effective_session`：以 `published_at_taipei` 對齊第一次能用於 09:00 決策的交易日；當天 09:00 或盤後資料不能回到同日開盤。保留原事件、目前修訂版本與 estimated-clock 標記，不因此提升為 original-vintage／strict PIT；舊 `prepared-v5` 沒有就地修改。

驗收範圍：FinLab history／admission／incremental quota、優先意圖、receipt-head 切換、missing-only 寫回、原生季度／月份缺口、替代來源重驗、macro 開盤時鐘、mixed/factorized synthetic preparation、view verification、publication 與 frozen code release，共 **195 項通過（20.65 秒）**。包含 expired intent 不能冒充成功、changed repaired primary 阻擋、原 native source 不被覆寫、已重查仍 NULL 與未查分開報告、真實零值保留、消失實體欄的真實歷史保留，以及改寫 latest primary 後即使重新計算 source files SHA 仍被語意驗證拒絕。這不是 GPU 效能／完整 fold、盤前服務或整個 repository 的驗收。測試輸出：[完整 run log](../artifacts/operations/agent-workflow/runs/final-provider-retention-regress-20261004T144508-6d358e1d/run.log)。

## 7. 新版來源與仍未完成的邊界

最新版僅在本機：`/srv/stockagent-live/data_tw_daytrade_panel_sources_20261004_gaprepair_v4`。manifest SHA256：`167190ad3e17d43aa590bdb4160e8f9c26e5ffe9951e7d9f9069683e0280811f`。

包含 2,757 股票來源檔、553 規格（508 FinLab）、54 季 MOPS IFRS、2,237 個 FinMind 原生財務 projections、13 個台灣原生總經量，以及原有全球共同來源與 US source-only tables。來源與 proofs 共 5,292,847,433 bytes、5,916 個受 manifest 約束的 members。7,821 筆跨來源營收補值與 80,285 筆同 provider 真實歷史保留分別驗收，不能相互冒充。

`status=complete` 表示這個已宣告私人來源組合完成打包驗收，不表示每一項 optional 科目都有值；不宣稱所有 NULL 已解決，`historical_point_in_time=false`、`training_ready=false` 不變。

最新 registry 名 `tw-daytrade-panel-sources-20261004-gaprepair-v4`，明確 **`publish=false`**，沒有啟動這一版冷發布、遠端同步、hydrate 或訓練。原始 sources 及歷史候選不刪除。

先前冷庫 dataset 名：`tw-daytrade-panel-sources-20261004-gaprepair-v2`。14:00:42 UTC、使用者暫緩交付之前成功發布，收據在 [source-publish-v2.json](../artifacts/data_quality/tw_feature_expected_gaps_20261004/source-publish-v2.json)。這個歷史 snapshot 不是最終 v4：

`tw-daytrade-panel-sources-20261004-gapre-20261004T140040050197103Z-l0-penguin-91817a7e7884440d`

Packed manifest SHA256：`f68c8b367728f910fe3dd6021c908e1a5e786316f8bd6dc762a71f32b3e67655`。包含 5,614 files、85 required objects；總 cold bytes 5,068,965,950，新增加 18 objects／1,052,456,547 bytes，其餘既有內容位址物件核對後重用。這是 penguin 發布證明，**不是 Vast 已收到此版本的證明**。資料仍走既有 Syncthing，沒有 SSH 傳原始來源。

先前遠端程式 fixed source SHA：`d1609d57b398e7d5436ead6cf6925b3995bd45c04f8733227cf3ae928d68fc2b`。Vast 程式根：`/root/stockAgent/artifacts/markets/tw_day_trade_factorized_panel_20261004_v1/code-gaprepair-v14`；1,250 members 與 wheel／ZIP 雜湊當時通過。不覆蓋 code-v13、舊 checkpoints 或 configuration。**v14 尚不含新的歷史保留驗證器，不可直接宣稱適用 v4 來源。**

包含新 consumer 的本機程式封存 source SHA：`be575fa5caabaf75f413438abfd6ff49e4eaa17a4658d3954f268d58c219b7bf`；ZIP SHA：`47b59450cb8c0ba554d0f498ae6b2b3a99380af9214208d6fe95135e334533d8`、1,230 frozen members，另有 wheel 與 runtime 收據，data/deployment flags 均為 false。[本機 release 收據](../artifacts/data_quality/tw_feature_expected_gaps_20261004/code-release-v4/20261004T144631018170Z-be575fa5caab/release.json)。沒有傳到遠端或宣稱新訓練設定已固定。

暫緩前的遠端檢視只有約 8.34 GiB free；新個別訓練區塊重建約需 17 GB，還要來源 materialization、準備與 checkpoint 空間。使用者要求自行清空間後再處理，本次未再啟動新的遠端操作，這不是其後持續監控的 free-space 證明。本次不自行刪掉舊來源／模型，也不在磁碟不足時啟動 full build。舊精度及完整雙卡 fold 限制亦未解除，詳見 [既有 panel 驗收](tw_daytrade_factorized_panel_2026-10-04.md)。**來源更新完成、Syncthing 交付完成、新訓練 view 完成、正式訓練完成必須分開交付證據。**

13:54 UTC 核對時，508 個已打包來源仍對得上當時 receipt；14:08 UTC 已有 41 個來源換新。最終 14:21:44 UTC 的 377／377 checks 全部完成後，固定捕獲 102 個變動 FinLab 來源為 raw-current v3，再以 prior v2 真實來源證據做缺值保留形成 v4。這個 latest 是本次固定捕獲的來源版本，不把後來可能移動的 receipt head 當成永遠不變。v2／v3 均未就地修改。

增量 refresh 僅更新原已准入、原 universe 的 FinLab projections，保留既有 native macro／MOPS／FinMind pinned members、來源限制與真實營收補值；不重新打 provider API。若 repaired primary 改變，必須重新驗證同義／單位 bundle，不能沿用失配的舊補值。原始來源刷新、補值、修訂與新增特徵仍分開記錄。

仍未解決的項目均有清單：剩餘 optional／適用性未證實期別、未通過單位／值版本 gate 的映射、provider 真實全 NULL、尚未到期報告、來源可提供歷史限制、US program 時鐘、原始 historical vintage，以及原有分鐘／企業行動未解部分。不能把已批准日 K 代理和安全遮罩解讀為已補回所有原始觀測。
