# 歷史缺價與 Factorized 盤後維護修復（2026-10-18）

本報告分為盤後維護與後續授權的歷史補段兩階段。前一階段不改正式模型、
訊號、股數／張數、費用與模擬成交契約，也沒有替換正式帳本。後續依使用者
授權，已從 2026 年第一個交易日補足五模式、明示目前模型反事實回放，並經
完整來源／帳務／分鐘驗收後原子切換；原帳本保留作 rollback。全程沒有券商
下單。來源完整、回放驗收與正式採用仍是不同邊界，後續證據見文末。

截至 16:32 的獨立驗收：Factorized 完整歷史／報表已到 10/16，原生盤後維護
job 已恢復 ready；五模式收盤快取 failures=0、deferred=0，823 個相關測試通過。
歷史首分鐘來源仍有真實缺口，正式舊帳本未替換，因此網站整體仍如實 degraded。
[最終原生驗收收據](../artifacts/operations/tw_history_maintenance_20261018/final_native_acceptance.json)。

## 根因與修正

### 開盤價不是第一分鐘 VWAP，也不是第一分鐘收盤價

`load_local_stock_0901_vwaps` 原本在讀取本機資料時丟掉 `Open`。
另一路回放為了取得該值，會先掃描整日 K 線並建立 Python 字典；
該路徑還可能把缺少的 `Open` 預設為 `Close`。兩條路徑既重複，又有語意差異。

- 保留共用本機讀取路徑，僅讀必要欄位、標的與右標籤 09:01 列。
- 分開輸出來源實際 `Open`、`Amount / volume_shares` 執行代理、`Close` 估值。
  缺少 Open、零量、未驗證量單位或超出有效 High/Low 的 Open，不當成首筆開盤價。
- `ts` 有明確時區時轉成臺北時間；沒有時區時保留既有臺北牆鐘契約。
  同時存在日期、時間與 `minutes_from_open` 時必須一致。
- 日期分區和按股票分塊使用同一驗證；不同 dtype／時區的來源不合併成同一 schema。
- 仍先讀已下載來源；未增加 API 請求，未把較晚 K 線移到 09:01。

### Factorized 的生產範圍與消費者契約不一致

盤後維護的真正失敗不是 Discord Gateway 斷線：完整 fold 推論依選定設定需要
2014-01-06 起的 BASE 行事曆；排程 updater 卻固定從 2025-12-01 生產。
10/16 的舊投影只有 212 個日期，缺 2,906 個必要歷史交易日。
「末日已更新」不能證明「完整歷史已齊全」。

- updater 改讀選定設定的 `panel_start_date`，未指定時使用固定訓練父來源起點；
  不改訓練、模型或 fold 範圍來迴避驗收。
- 維護 worker 現在也檢查已發布投影的原始請求範圍，能自動要求 canonical updater
  修復「末日雖新、歷史不足」；純收據 retry controller 仍不匯入模型。
- 另一個 canonical 投影建置者已持有 `prepare.lock` 時，只延後 Factorized 的來源與
  完整歷史工作，其他模式繼續；不排入重複 child、不白耗 activation timeout，
  不增加失敗重試次數。使用實際 exclusive kernel lease，未上鎖檔案或 shared reader
  不代表生產者；此探測不是資料驗收，後續仍要檢查完整發布收據。
- 收盤快取階段與完整歷史階段的結果不再互相蓋掉：真正的快取失敗保留為
  degraded；等待來源另計 deferred，不算推論失敗，也不能確認所有快取已完成。
  完整維護、輕量收盤 bridge 和 wakeup acknowledge 都遵守此區分。
- `LoadedImplementationChanged` 是整個已載入程序的失效，不是單一市場資料失敗；
  偵測到後提前退出並留下 worker degraded，由既有 timer 啟動新程序。
  不再繼續嘗試其他模型或消耗各市場的歷史重試／退避額度，也不重新標記舊程序版本。
- 完整範圍需要的 TTL 初始化年度種子曾未部署齊全。由原始 Vast 來源恢復
  2013～2026 共 14 個年度檔案，逐一通過既有父 manifest 的 SHA256；
  新複製約 1.87 GB，既有 2025／2026 檔未覆寫。
- 種子驗證提前到昂貴特徵準備之前；接受原始 `annual_events/YEAR.parquet`
  或已部署的同雜湊扁平布局，但不接受缺年、重複、逃逸或已變更的檔案。
- 保留 `prepare.lock`、來源讀鎖、精確 generation、不可變區塊重用與中斷進度、
  原子發布，以及最後一個已驗收版本。未完成 attempt 不是正式投影。
- 完整冷來源實測建置本體需要 3,811.64 秒，超過一般 1,800 秒 completed-session
  timeout。只有完整隔離維護 owner 的來源恢復使用 80 分鐘額度，保留原 120 分鐘
  systemd 上限中的推論／控制預算；一般互動、輕量 bridge 和市場設定的 timeout 不變。
- receipt-only controller 原可在休市日喚醒週五的失敗工作，但 full worker 使用
  日排程 key 後又跳過，形成無效喚醒。隔離 owner 現在只接續最新已驗收收盤日期、
  已存在的 failed／interrupted key，仍保留 fresh-data、收盤收據、退避、
  開盤／互動優先與單一 worker lease；不在週末建立新訊號日或改寫舊日期。
- 完整日期修好後，原生推論進一步揭露標的軸不一致：新價格面板有 2,761 檔，
  固定訓練投影／模型為 2,757 檔。`train.py --mode infer` 原在模型標的對齊前
  附掛特徵而失敗。現在對已驗收的 paper projection 先使用
  `checkpoint_contract.subset_panel_symbols` 同序對齊全部價格、mask 與執行資料，
  再走原本嚴格的附掛驗證；缺少的訓練 slot 永久 masked、價格 NaN。
  保留全部日期，不使用 live-tail 日期裁切，不改官方交易清冊分母；
  一般訓練與非 paper manifest 仍維持原本嚴格的軸一致性。
  manifest symlink 只解析一次，後續驗證綁定同一不可變 generation。
- 正式實跑再揭露另一個獨立阻擋：BASE 已到 10/16，實體 FIFO 清算仍繼承
  只覆蓋到 10/02 的固定訓練 archive；完整日期範圍驗證因此正確拒絕。
  新增明確的 `data.day_trade_inference_public_feature_path`，僅允許 `--mode infer`
  使用目前已嚴格驗收到 10/16 的 executor-only 官方檔，並把 live 分鐘來源綁回
  既有 `data_tw_minute/research_dataset`。原始訓練 archive、宣告路徑、checkpoint
  `data_schema`／模型／fitted RMS 都不改；實際來源仍進入 runtime config 與
  physical cache 的內容／收據 fingerprint。停用預設不改歷史訓練 config fingerprint。
- 新來源設定禁止 train／resume，在 cold restore／大面板配置之前拒絕誤用；
  live overlay 的預設執行模式明確設為 infer。推論入口在同一 canonical shared
  read lease 內準備 BASE、分鐘 tape 與實體來源，準備完成即釋放，不持鎖跑完整
  GPU 推論。生產者持有 exclusive lease 時非阻塞延後；例外與 DDP 失敗會清理 lease，
  原本每份來源的 SHA／coverage／price-grid 檢查仍保留。
- 實體來源完成後，第一次完整推論又在 `torch.compile(fullgraph=True)` 拒絕：
  外層整圖包住了刻意 `torch.compiler.disable` 的 Factorized 主機 I/O 排程。
  現在訓練與完整推論共用既有 eager I/O／compiled math 分段設定；
  dense 路徑仍保持原整圖編譯。未移除 I/O 邊界、關閉全部編譯、改精度，
  也未關掉 strict-no-fallback。新增分段／dense／停用及 compiler 不可用反例。
  [PyTorch 整圖契約](https://docs.pytorch.org/docs/stable/user_guide/torch_compiler/compile/programming_model.fullgraph_true.html)、
  [刻意不編譯區段](https://docs.pytorch.org/docs/main/generated/torch.compiler.disable.html)。

來源恢復證據：[seed_recovery.json](../artifacts/operations/tw_history_maintenance_20261018/seed_recovery.json)。

## 完整來源已發布與獨立驗收

- canonical updater 於 15:08 完成，實際歷史範圍 **2014-01-06～2026-10-16，
  3,118 個日期**，下一決策日 10/19。原本漏掉的 2,906 個必要歷史交易日已補入，
  不是縮短模型範圍或放寬完整行事曆檢查。
- generation `44030955ef8ec3e9ce0d563cda510d848b34cf6ac8bfc0658e5897fe5d980fdf`，
  manifest SHA256 `1df820537d6bae1cec2ee56d345ed6edef602320faaf298655edbcbc47765307`。
  建置本體約 63.53 分鐘；外層受監督工作含來源等待與收尾約 67.27 分鐘。
  這不是下方本機讀價的 39 倍加速數字。
- 發布後以另一程序重新雜湊全部 2,906 個 manifest 成員（包含 dictionary），
  並通過選定設定的完整行事曆 preflight；測前／測後 manifest SHA 相同。
  [獨立驗收](../artifacts/operations/tw_history_maintenance_20261018/full_projection_acceptance.json)。
- 從已保存的精確原始訓練區塊抽查 02/24、09/30，共 **81,166,080 Float32 cells**，
  **different=0，逐位相同**。只驗證兩日外部特徵，不含 BASE 價格，也不代表全期
  無缺價、完整 fold 推論已通過或已部署至交易引擎。
  首次誤指向只有 manifest／種子的 parent 目錄時因缺 common.npy 明確失敗；
  改指向保留的 original_shards 後驗證通過，沒有另抓或重建原始值。
  [特徵逐位驗收](../artifacts/operations/tw_history_maintenance_20261018/full_projection_row_parity_retained/parity_receipt.json)。
- 完整實體清算來源於 16:14 完成首次建置，本體 **1,144.5 秒**；當次 BASE／
  分鐘 tape／清算來源準備合計 **1,358.92 秒**，隨後才遇到上述編譯入口錯誤。
  新程序重新讀取同一內容定址快取，通過完整 SHA256 驗證，該步 **12.5 秒**；
  當次整個面板／來源準備仍需 **180.95 秒**，不能把局部快取驗證當成總啟動時間。
- 另一程序獨立驗證實體快取全部 **1,618 個成員、11,560,261,110 bytes**，
  3,118 日、2,757 個訓練 slots，18.81 秒通過成員大小與 SHA；
  manifest `9bdaf751525ee4fa9c1437d669769ebfc8bf78c161d266fe4c88b9c282aba2d1`。
  [獨立實體來源驗收](../artifacts/operations/tw_history_maintenance_20261018/physical_cache_independent_acceptance.json)。
- 實體來源的 `source_gap_symbol_days=0` **只在原有研究代理契約內成立**：
  2,841,236 個 symbol/day 使用分鐘來源，2,526,587 個為分鐘歷史以前的日線代理、
  468,724 個為分鐘歷史以後的日線代理；另有明確無一般交易的來源分類。
  後者缺口原因仍保留：缺歷史合法價格界線 297,214、缺少或空分鐘列 80,768、
  分鐘量超過官方日量 97,450。沒有新增代理政策、假造觀測分鐘或把代理當正式帳本成交。

## 本機讀取實測與重測

2026-10-16 留存 Parquet 的 2,305 個 symbol 用於這次讀取測試，**不是當日正式交易清冊分母**。
兩路都取得相同 1,332 個有效首分鐘 Open，價格逐值相等；來源 SHA 在測前／測後一致。
同程序 ABBA、不重置 page cache；最新一輪直接觀測 Polars 與 Arrow pool 各 2 條執行緒，
不是系統 CPU 配額。無 API、模型、CUDA 或帳本寫入。

| ABBA 輪次 | 原整日解析後取 09:01 | 共用 09:01 必要欄位讀取 | 加速比 |
| --- | ---: | ---: | ---: |
| 第 1 輪 | 24.834 秒 | 0.565 秒 | 43.93 倍 |
| 第 2 輪 | 26.298 秒 | 0.662 秒 | 39.74 倍 |
| 第 3 輪（記錄實際 pool 與測速工具 SHA） | 32.550 秒 | 0.821 秒 | 39.66 倍 |

負載不同會改變絕對耗時，三輪皆通過同來源／同價驗證並完整保留。
這不是冷磁碟測試，也不是開盤到模型／全部成交的端到端 SLO。

重測工具會在來源／價格不相等時拒絕產生「通過」收據，且拒絕覆寫既有結果：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_day_trade_0901_open_read.py \
  --trading-date 2026-10-16 \
  --symbols-from data_tw_minute/research_dataset/trade_date=2026-10-16/data.parquet \
  --output artifacts/operations/NEW_MEASUREMENT/local_open_read_abba.json
```

[完整量測與實際 pool 收據](../artifacts/operations/tw_history_maintenance_20261018/local_open_read_abba_observed_pools.json)。

## 歷史來源與隔離回放驗收

- 對既有 02/25～09/22、129 日的全市場缺口名單重新查本機：30,263 個 symbol/day
  中 16 個現在有來源，30,247 個仍沒有 09:01 列；來源讀取錯誤為零。
  這是全市場來源缺口，不是 30,247 筆策略未成交，也不證明沒有該分鐘的市場交易。
  [重新盤點](../artifacts/operations/tw_history_maintenance_20261018/history_0901_local_recheck.json)。
- Factorized 原六個具體缺口（03/05 0052、03/17 2485、05/27 2489、07/09 與
  07/15 00715L、08/21 7751）皆有官方日開盤價，但本機分鐘首列全部為 09:03。
  [六筆來源逐項讀回](../artifacts/operations/agent-workflow/runs/historical-six-price-source-evid-20261018T061714-b36bb59d/run.log)。
  交易所確有[暫緩開盤兩分鐘機制](https://www.twse.com.tw/zh/products/system/trading.html)，
  但這只能說明可能原因，不能在缺少當日撮合／逐筆證據時斷言這六筆皆因暫緩開盤。
- 另由 canonical 永豐歷史 helper 重新查六筆 09:00:00～09:00:59 逐筆及 09:01 K 線，
  六筆均未查得有效首分鐘資料，API 錯誤、缺合約與流量停查皆為零。
  同六日期的 2330 陽性對照全部成功；不是僅憑本機缺檔或整個歷史 API 失效下結論。
  這仍是「目前可查來源未提供有效首分鐘」的證據，不等於交易所認證當分鐘無交易。
  [六筆查詢](../artifacts/operations/tw_history_maintenance_20261018/six_gap_remote/summary.json)、
  [同日陽性對照](../artifacts/operations/tw_history_maintenance_20261018/six_gap_remote/control_2330.json)。
  首次監督程序缺少登入家目錄，SDK token pool 初始化失敗；重試只以官方支援的
  `SJ_HOME_PATH` 指向既有 SDK 狀態目錄，不更改 HOME、不新建另一套行情 owner。
- 10/16 以既有五模式原訊號固定 ID、已保存官方開盤資料與本機分鐘資料重建隔離候選，
  五模式均無 `official_open_price_unavailable`。其他缺量、價格界線不足及部分成交
  原因仍逐項保留，沒有把日開盤價當成 09:01 成交量。
- 同候選完成五模式各 270、合計 **1,350 分鐘點**驗證；799 列有揭露沿用估值，
  無線性插值，成交 SHA 不變。這不表示每分鐘都有新成交，也不是正式帳本已替換。
  [分鐘驗收](../artifacts/operations/tw_history_maintenance_20261018/candidate_20261016/minute_curve_receipt.json)。
- 全歷史重建遇到 10/06、10/07 Multi-Basis 22 與 10/14 Factorized 的原訊號／
  既有追加修復訊號並存。保留嚴格拒絕；不能用 first/last wins 選一筆覆蓋。
  尚未對其餘日期宣稱完成重算或 promotion。

## 回歸與執行狀態

- 最終相關聯集回歸：**823 passed in 122.22s**。包括完整範圍、年度種子來源、
  不可變重用／恢復、正式歷史契約、盤後維護／重試、分鐘價格、時區與測速反例。
  額外包含完整／輕量 worker 的快取失敗不被歷史階段掩蓋、等待不算失敗、
  不錯誤 acknowledge、既有 Discord helper、完整推論標的軸、infer-only 來源綁定、
  來源 shared lease 釋放／生產者等待、實體 carry、checkpoint、lifecycle，
  以及 Factorized 推論的分段編譯與嚴格 compiler 失敗回歸。
  [回歸 log](../artifacts/operations/agent-workflow/runs/history-factorized-partitioned-f-20261018T082118-a462132e/run.log)。
  不以重複執行的重疊測試相加，也未宣稱全庫測試通過。
- 新增休市日接續修正後，先通過相關 **106 個針對性測試**；包括不建立週末 key、
  不使用舊日期／未驗收來源、不越過 retry backoff／互動／開盤優先。
- 隨後新增完整推論標的對齊，另通過 **271 個**特徵、checkpoint、DDP 入口與
  lifecycle 測試；與上列測試重疊，不能相加。
  目前環境 Matplotlib 3.9.1／Pyparsing 3.3.3 在匯入時有第三方 deprecated API
  警告，第一次標準收集因此拒絕。此次指令僅對 Matplotlib 的
  `_fontconfig_pattern`、`_mathtext` 兩個模組增加 PyparsingDeprecationWarning
  相容例外，未改 dependencies、全域 warning policy 或專案警告。
  [入口與生命周期回歸](../artifacts/operations/agent-workflow/runs/factorized-formal-universe-warni-20261018T073435-b313b901/run.log)。
- 額外整個 config 掃描仍發現既有實驗 overlay
  `tw_day_trade_factorized_values_20261006_no_basis_flat_bf16_tf32_b128_v1.yaml`
  的 `base_config` 指向已不存在的歷史 artifact 內程式快照。這不是本輪選定的
  live／candidate config，也未透過改指另一份實驗設定或略過錯誤冒充全庫通過。
  最終聯集只選取本輪相關 config loader 節點，沒有宣稱全 repository 已通過。
  [完整失敗證據](../artifacts/operations/agent-workflow/runs/factorized-inference-source-cont-20261018T075007-3716944d/run.log)。
- 完整 Factorized 投影與實體來源均已發布驗收；16:27 完整 fold 實跑成功。
  原生維護仍須獨立 reconcile，不修改告警 JSON 為 ready。
- 隔離維護 owner 已因載入版本變更而獨立重新啟動；Gateway 與當沖引擎沒有因此
  一起重啟。已恢復 10/16 的 100m 收盤快取，Factorized 等待完整投影後再原生驗收。
- 14:46 原生完整維護讀回：其餘四個當沖模式收盤快取通過，Factorized 來源等待；
  真正失敗為 0、Factorized 原失敗 job 的 attempt 仍為 5，未白耗重試。
  原完整歷史告警仍保留，不以本輪 worker exit 0 宣稱已消除。
- 15:25 原生五模式收盤快取全部通過，failures=0、deferred=0，wakeup readback
  acknowledge=true。15:26 full owner 成功在週日接續 10/16 原 job，沒有建立週日 key；
  當次因上述標的軸錯誤拒絕，attempt=6 的真實失敗與原退避保留。
- 修正入口後，在相同 canonical worker lease 下呼叫既有
  `_run_artifact_backfill_sync` 補跑一次；嚴格 CUDA preflight 通過。
  操作不重寫 scheduler 失敗／退避紀錄，完成後必須由原生維護流程獨立
  reconcile 實際完整產物。15:39 該次因上述過期 physical archive 拒絕；
  15:51 新來源綁定通過針對性測試、嚴格 CUDA preflight 後再次透過相同 owner
  補跑；該次完成實體來源後於編譯邊界失敗。16:18 修正分段編譯入口後，
  再經針對性測試與嚴格 CUDA preflight 執行。
- **16:27:40 完整 fold 11 推論、曲線及報表成功完成**，工作範圍保持
  train 2014～2024、validation 2025、test 2026；完整測試末日到 10/16。
  `fold_complete.json` 為 `complete`、scope version 2、`full_horizon` 與
  `stitched_deployment`。canonical repair 本體約 **489.80 秒**，外層含 preflight
  約 527.16 秒。原 owner 的 `_artifact_backfill_is_current` 實際讀回通過，
  不是只驗證 returns 檔存在；沒有產生或回填新的開盤訊號。
  [完整修復收據](../artifacts/operations/tw_history_maintenance_20261018/formal_history_operator_repair.json)、
  [完整執行 log](../artifacts/operations/agent-workflow/runs/factorized-partitioned-formal-in-20261018T081847-0742379b/run.log)。
- 16:28 首次原生重新驗收遇到獨立 postclose-cache owner 正在更新，正確延後且
  未增加原失敗 job 的 attempt；等該 owner 自行完成後再驗收，不搶鎖或停止它。
- **16:30:34 原生完整維護自行 reconcile 為 ready**，既有 attempt 保持 6，
  error 與 next_retry_at 已清除，未建立週末 job。五模式快取 attempted=5、
  failures=0、deferred=0；unit invocation
  `c1cdfa4b2717420b826303bb377b67d9` 正常完成。16:31:59 另一程序再次驗收
  原生 job／run、兩份已獨立驗證的 manifest SHA、Discord 新鮮載入版本與連線、
  引擎 invocation 未變、原始帳本 byte-prefix，以及 HTTP 帳本 integrity ready、
  divergence=0 全部通過。不是手動把 scheduler JSON 改為正常。
  [最終獨立原生驗收](../artifacts/operations/tw_history_maintenance_20261018/final_native_acceptance.json)。
- 隨後獨立核對 Discord 載入版本並以 canonical plan → apply → verify 更新，
  `accepted_code_admission_only`：新程式版本、Gateway 連線、commands sync 與五模式
  引擎 ACK 通過，沒有 watchdog／fatal。**不是 GPU 已暖機**。
  當沖引擎未重啟，兩筆真正待處理委託仍保留。
  [隔離 Discord 採用收據](../artifacts/operations/tw_history_maintenance_20261018/discord_admission_apply.json)。
- 共用儲存／FinLab 程式由其他工作更新後，14:51 再獨立完成最後載入版本驗收，
  `accepted_code_admission_only`、Gateway ready，仍不等同 GPU 暖機或 Factorized 維護完成。
  [最後採用收據](../artifacts/operations/tw_history_maintenance_20261018/discord_final_admission_apply.json)。
- runtime 來源修正後，15:59 再經獨立 plan → apply → verify 完成 Discord-only
  code admission：新 run、Gateway／commands 與既有引擎訊號 ACK 通過，revision lag=0；
  當次引擎有四個已登錄訊號，沒有把尚未登錄的 Factorized 訊號算成已 ACK。
  GPU 暖機未驗證，當沖引擎 invocation 未改、兩筆待處理委託保留。
  [runtime 來源採用收據](../artifacts/operations/tw_history_maintenance_20261018/discord_runtime_binding_admission_apply.json)。
- 完整推論分段編譯修正後，獨立重算 Discord 的實際依賴閉包，211 個依賴的
  code SHA 仍與已載入程序完全相同；trainer 並不在該角色依賴中，無須再重啟。
  不因其他角色檔案更新而把正常 Gateway 判成失效。
- 正式事件／委託／成交原始 byte-prefix 全數通過獨立雜湊驗證；未改原始訊號或正式帳本。
  [runtime 修正後帳本前綴驗收](../artifacts/operations/tw_history_maintenance_20261018/ledger_prefix_after_runtime_source_binding.json)。
  完整推論結束與原生 job 恢復後再次驗證仍全部相同，正式 ledger 沒有新增或截短；
  [完整修復後前綴驗收](../artifacts/operations/tw_history_maintenance_20261018/ledger_prefix_after_completed_formal_repair.json)
  與上列最終原生驗收均有保留原始雜湊。
- HTTP health 與當沖 status API 目前可連線，帳本 integrity ready、divergence=0；
  status health 仍是 degraded，包含正式舊帳本 10/16 的缺開盤價與部分成交證據。
  已驗收的隔離候選沒有 promotion，不能用候選結果改寫正式帳本的健康狀態。

本節狀態是當次觀察，不是往後所有交易日的可用性或毫秒成交保證；
投影完成、完整 fold 推論與 native 維護 job ready 都須分別讀回收據。

## 2026 年起始顯示與五模式歷史補段（已部署，保留資料缺口）

使用者已明確同意：缺少的 2026-01-02～02-24 以所選模型重建並標示反事實回放，
包含 Factorized；完成驗收後恢復第 5 個公開模式。不是把新訊號冒充成當時即時訊號。

- 共用當沖／隔日沖日期控制預設 `2026-01-01`～最新交易日，保留手動起訖與
  最新單日切換。沒有資料的 1/1 不造點；絕對資金不因選擇區間歸零，僅百分比
  按所選第一個有效分鐘重設。完整分鐘不下採樣。
- 399 個相關 Python 測試與 47 個 Node 測試通過；內容雜湊版 JS 的公網／本機，
  當沖／隔日沖與筆電／手機合計 6 組原生瀏覽器測試通過，無整頁重載、JS 錯誤
  或橫向溢出。這份收據刻意標記 `missing_history_accepted=false`，不冒稱補歷史完成。
  [瀏覽器日期驗收](../artifacts/operations/tw_ytd_history_20261018/browser_five_mode_code/receipt.json)。
- 31 個官方交易日的漲跌停快照、五模式 155 份隔離回放訊號已完成。保留真正
  10/18 產生時間、官方開盤輸入、模型／checkpoint、權重與回放有效時間。
- 00883B、1589、4987、6806 已不在當前券商合約清單，但歷史查詢仍可取得。
  經 canonical KBar downloader 的明確歷史身分與量／金額驗證，4 檔共 8 個請求
  完成要求期間；原失敗與空合約結果仍保留，未把股票張數當股數。
  [歷史身分補源收據](../artifacts/operations/tw_ytd_history_20261018/minute_recovery_historical_identity/download_summary.json)。
- 回放分開「尚未成交的目標缺價」與「已持有部位缺估值」：前者保持未成交並
  留下缺口，不能阻止其他標的；後者仍 fail closed，不插值、不捏造成交或價格。
- `update_readiness` 曾只把成功載入的 spec 當成啟用清單，使 Factorized 設定失敗
  被誤寫成停用、原始錯誤也消失。當沖與隔日沖改用明確 configured roster，
  保留帳戶及錯誤、標示 `blocked_mode_configuration`；正式排除仍需設定真的移除。
- canonical strategy-switch 新增 `--history-extension --prepared-candidate PATH`：只接受
  缺少的較早期間，核對既有 signal ID、已留存 artifact pins、各模式原始資本與
  真實回放標記；不呼叫整段訊號替換。沿用 minute gate、原子交換與 rollback、
  Discord 暖機／引擎 ACK、公網 API 驗收，以及 `plan → apply → verify → status`。
  正式 journal 在規劃後改變即拒絕切換；過時 replay／ready receipt 不算新程序驗收。

完整範圍預期為 190 個交易日 × 5 模式 × 270 分鐘 = 256,500 個策略分鐘點。
首輪五模式重算完成 31 個新增交易日後，在 2/25 嚴格拒絕找不到的 V8 原訊號，
並未替換正式帳本。追查確認：加入 Factorized 的 additive receipt 保存了舊四模式的
原收據，但來源解析器沒有追溯 `prior_rebuild_receipt`／`new_mode_component`；前三模式
原檔則已依既有儲存政策退到固定冷庫。同 ID 的現行重新產生權重與原始雜湊不同，
不能替代原紀錄。

- 解析器沿明確宣告的 additive 證據圖追溯，使用現有 `evidence_path` 防止越界／
  symlink 逃逸，拒絕循環、非獨立新 component、變更的來源與不一致的模擬契約。
- 既有 archive restore 增加 `--snapshot-id`，可恢復精確不可變版本而非追隨 latest。
  此次來源為 `legacy-wsl-offline-9f3a6246e0f8c4c395ac7467`，只恢復原值，不刪除冷庫。
- replay 增加全期間訊號預檢 `--preflight-only --preflight-output PATH`；正式重算前
  核對全部選定訊號的 ID、policy mask、summary／weights／open-input 雜湊，並固定
  新發現來源，防止長時間重建後才發現較晚日期缺檔。`accepted_sources_only` 明確
  不代表成交、分鐘完整性或正式發布已通過。
- 證據圖、固定版本還原初次針對性測試 139 項通過；加入全期間預檢後，replay／
  strategy switch／promotion／maintenance 相關 190 項通過（兩組重疊，不相加）。
- 擴大相關回歸 464 項 Python、47 項 Node 通過；隨後來源路徑改善的 replay
  119 項通過。單日五模式完整 cProfile 比對：68.285 → 48.373 秒，其中分鐘
  來源載入 21.081 → 3.530 秒；移除每筆分鐘重複 `Path.resolve()`，但每個來源仍
  重驗路徑，拒絕讀取中 redirect，並維持完整 SHA／metadata 檢查。訊號、委託、
  成交、事件與 1,350 個策略分鐘 marks 的 journal bytes 全部一致；五帳戶除隔離
  candidate 絕對路徑前綴外逐值相等。這是帶 profiler 的單日工程比對，不是 ABBA
  穩態測速或全歷史已驗收。
  [逐值／逐 byte 比對收據](../artifacts/operations/tw_ytd_history_20261018/path_cache_parity.json)。
- 相鄰流程回歸發現：恢復已有但暫時未公開的模式，不同於重新加入「啟用曾中止、
  尚未建倉」的新模式。promotion 現在只對通過既有 aborted-addition 證據核對的
  新模式扣除 superseded roster；一般模式恢復仍保留原帳戶與非空倉禁止切換。
  promotion／addition／minute／benchmark 相關 128 項測試通過（與上述組合重疊）。
- 固定冷庫恢復已完整完成：6,528 檔、1,337,382,875 原始 bytes；恢復 marker 核對
  snapshot `legacy-wsl-offline-9f3a6246e0f8c4c395ac7-20261006T134610214535993Z-l0-penguin-0bf6bbd7ae42dd2c`、
  manifest SHA `8e8cb68a55ba69dbf3aa9236e855515613f7dff4fc76c07c40c884d65f485ffc`，
  scope 為完整 archive、`deployable=false`。此項只證明原訊號檔恢復，不是帳本發布。
  [恢復執行 log](../artifacts/operations/agent-workflow/runs/ytd-fixed-signal-source-recovery-20261018T095049-649a9b2e/run.log)。
- 恢復後第一次全期間預檢通過 946／950；10/14 四模式保留 producer 原始
  `execution_weights.json`，但完整 `target_weights.parquet` 尚未落盤。原 live pointer
  reader 已支援 compact 格式，replay 卻只接受 Parquet。現在共用 schema-1 reader，
  核對 market／signal ID／rows，replay 額外核對宣告的同目錄路徑、symbol count
  與原始檔 SHA。未重建權重、未寫出假的 Parquet；已釘選 Parquet 缺失仍拒絕，
  compact 經預檢釘選後也不因稍後出現 Parquet 而換來源。
  [原始拒絕收據](../artifacts/operations/tw_ytd_history_20261018/full_signal_preflight.json)。
  補上此格式相容後 replay／engine／switch／promotion 相關 391 項測試通過；
  最後擴大回歸為 580 項 Python、47 項 Node（組合重疊，不相加）。
  [最後回歸 log](../artifacts/operations/agent-workflow/runs/ytd-compact-union-regression-20261018T102643-9d0e83d1/run.log)。
- 第二次全期間預檢通過 950／950，failures 為空；其契約仍明示
  `execution_or_minute_coverage_accepted=false`。已啟動五模式完整重算，依序執行
  benchmark、保留既有 bracket-aware marks 的精確分鐘驗收、只讀 promotion
  驗證及 history-extension plan；此流程不自動 apply。每個完成 session 的
  `rebuild_wall_ms` 與各階段 shell wall time 留在執行 log，明示不是即時成交延遲。
  [全部訊號來源預檢](../artifacts/operations/tw_ytd_history_20261018/full_signal_preflight_compact.json)、
  [完整重算／驗收 log](../artifacts/operations/agent-workflow/runs/ytd-five-mode-complete-acceptanc-20261018T102602-63246d43/run.log)。
- 31 個新增 session 完成後，另外以 `cmp -n <原串流長度>` 核對保留前次結果與
  新候選的 signals／orders／fills／events／marks 五種 journal prefix，全數逐 byte
  相同。這證明該補段的優化沒有改寫交易數值，不代表其後全期間已驗收。
  [31 日串流比對 log](../artifacts/operations/agent-workflow/runs/ytd-31-session-journal-prefix-pa-20261018T103621-d9d82f99/run.log)。
- 瀏覽器驗收新增明確 `--expected-market`／`--expected-first-session`，同時核對
  status mode roster、實際卡片標題及可見歷史起點。函數驗證包含缺卡、重複模式、
  錯誤模型標題；該服務測試 7 項通過。切換前以真正 `/tw-day-trade/` 路由跑
  筆電／手機兩案，確認仍為四模式、最早 2/25。第一次誤用入口首頁的逾時收據
  保留，不視為服務中斷。切換後必須再驗收五模式與 1/2 起點。
  [切換前原生瀏覽器驗收](../artifacts/operations/tw_ytd_history_20261018/browser_mode_visibility_precutover_correct_route/receipt.json)。

五模式完整重算已完成 190 個 session（1/2～10/16），native wall time 51m23.417s。
逐 session 的 rebuild wall time 中位數 15.135 秒、最小 9.847 秒、最大 33.303 秒；
這是離線歷史重算時間，不是 09:00 訊號／模擬進場延遲，也不是全期間 A/B 加速證明。
三項 benchmark 來源重建 native wall time 5m28.629s，190 sessions、10 個 TX
實體月份、57,760 筆初步 benchmark marks，新增 Shioaji 請求為零；股票基準的
完整分鐘重建與策略分鐘驗收隨後以 native wall time 16m45.912s 完成。
候選已發布 256,500 個策略分鐘點、102,980 個股票基準點與 57,000 個 TX 點。
所需 118,077 個 symbol-day 全有本機來源，缺少配對為零；保留既有 bracket-aware
策略 marks、13:30 終點與成交 SHA，沒有線性插值或額外 API 下載。
135,653 個策略點含明確標記的沿用估值，不能解讀成每個標的每分鐘都有新成交。
[候選分鐘來源／輸出驗收](../artifacts/operations/tw_ytd_history_20261018/minute_curves_verified/minute_curve_receipt.json)。

包含瀏覽器可見性新測試的最後全組合回歸：582 項 Python、47 項 Node 通過。
[最後切換前回歸 log](../artifacts/operations/agent-workflow/runs/ytd-precutover-final-regression-20261018T105949-918de230/run.log)。

獨立的完整帳務／promotion 驗收與正式切換仍待完成；尚未替換正式帳本，
也未宣稱新增期間已公開。

第一輪完整 promotion 核對拒絕候選，未切換正式服務。根因已分開追查：

- `register_signal` 每日歸零 entry-completion 計數；驗證器錯當成 lifetime 計數，
  因而把較少補成交的次日判為回退。現在逐 session 核對 entry 次數、逐差額核對
  lifetime reduction 次數，並額外與每個 session／mode 的原始 fill rows 核對，
  不只是放寬總數。現有 journal 不改寫。
- 完整來源 audit 原先把同檔多日的已用來源解成「所有符號都用此來源」，混入
  未被選定的重疊 chunk。現在重用 replay 的 symbol／session owner 與 SHA pins，
  核對當時真正選用的 OHLCV。8354 10/12 09:48、6158 10/12 09:50、6649
  10/13 09:57 的被釘選原始來源各有 2,000 股，對應價格亦在 OHLC 內；未選用
  的重疊行情仍保留，legacy 沒有 symbol pins 時也不隱藏衝突。
- 原訊號中 42,373 筆首分鐘零成交嘗試不能自動當成下載缺漏，也不能自動算正常。
  使用既有 receipt-verified absence 驗證：其餘未發現正量矛盾，尚缺 3454 的
  1/21、1/30 兩組來源，正在 canonical collector 補查；保留失敗診斷。
  [首分鐘未成交來源診斷](../artifacts/operations/tw_ytd_history_20261018/absence_diagnosis.json)。
- 失敗的 promotion 現在也會保存完整 `failed_validation` 收據，明示
  `cutover.ready=false`；不再只有截斷至前 50 筆的錯誤訊息。

上述修正的 promotion／完整帳務 audit 針對性回歸 50 項通過。完整候選重新驗收中；
不以通過這些測試取代真實資料驗收或公開網站讀回。

後續實查確認 3454 1/21、1/30 **都有** 09:01 正成交量，不是已證實無成交。
canonical Shioaji downloader 以明確歷史身分、`simulation=true` 及原有本機憑證，
在獨立目錄取得 1/21～1/30 共 357 列（實際 9 次請求，來源缺口 0），兩個
目標日期的 09:01 各為 8,000 股。第一次未載入 `.env` 的零請求失敗有保留，
未把憑證放到命令列。這些新觀測會影響成交與後續 NAV／整張數量，不能只改
兩筆成交或沿用原候選的後續資金路徑。
[3454 原始補源收據](../artifacts/operations/tw_ytd_history_20261018/minute_recovery_3454_absence/download_summary.json)。

另啟獨立 `tw_day_trade_ytd_five_modes_sources_complete_20261018` 候選，沿用相同
950 份固定訊號、模型與正式來源 journal，納入新來源重新計算五模式完整 190 日，
再做 benchmark、270 點／日／模式的分鐘驗收、完整帳務及只讀切換計畫。
舊候選／正式帳本均未刪除；不以原候選被攔下的結果上線。
[補源後完整重算 log](../artifacts/operations/agent-workflow/runs/ytd-five-mode-complete-source-re-20261018T115835-b9e09c17/run.log)。
包含完整帳務 audit 的全組合回歸為 608 項 Python、47 項 Node 通過（90.66s）；
與先前組合重疊，不相加。
[補源／驗證器修正後回歸](../artifacts/operations/agent-workflow/runs/ytd-source-scope-union-regressio-20261018T115338-c840b244/run.log)。

舊候選的完整重驗 native wall time 9m12.180s，最後只剩上述兩組真實缺價；
沒有忽略該失敗。補源後新候選到達 1/21 以後，再獨立核對此前 13 個 session
的五種 journal prefix：signals 178,698 列／481,290,489 bytes、orders 61,750 列、
fills 29,183 列、events 364 列、marks 17,550 列，兩候選全數逐 byte 相同。
此證據只保證補源前前綴未改，不代表後續 NAV 或完整新候選已驗收。
[補源前前綴 SHA 驗收](../artifacts/operations/tw_ytd_history_20261018/prefix_before_3454_recovery.json)。

補源後完整重算已完成 190 sessions／950 mode-session，native wall time
48m17.240s（含來源預檢），五個帳戶原始資本不變、最後未平倉數皆為零。
rebuild receipt 的 producer SHA 與當前 replay 程式完全一致。此為候選檔案讀回，
不是完整來源／帳務或正式發布驗收；benchmark、分鐘核對與 promotion 繼續執行。
這次重跑與第一次的來源及 OS cache 狀態不同，不把兩次 wall time 差異當成 A/B 加速。

最後程式版本的相關全組合回歸再確認：608 項 Python（98.13s）、47 項 Node 通過，
不是全 repository suite。
[最後程式版本回歸](../artifacts/operations/agent-workflow/runs/ytd-final-ledger-consumer-regres-20261018T124405-bd076680/run.log)。

補源後 benchmark 建置 native wall time 5m37.318s。另修正訊號時間文案：新補段的
09:01 是模擬執行時鐘，畫面明示「目前模型反事實回放（非當時訊號）」；
既有回放若 source clock 只是相同的正規化時間，不再誤稱為原始訊號時間。
原始 summary／重建收據仍保留實際生成時刻，未改寫財務 journal 或計算中的候選。
此顯示修正通過 48 項 Node 與 33 項 dashboard Python 測試，更新 app.js 的
內容雜湊版本為 `13a9263a61d7`；與上述回歸重疊，不能加總為全庫通過。
[時間標示回歸](../artifacts/operations/agent-workflow/runs/ytd-counterfactual-ui-clock-regr-20261018T131103-e5df7023/run.log)。

補源後分鐘建置 native wall time 17m25.659s：190 日、五模式、256,500 列；
所需來源 symbol/day 118,079 組全數可讀，未發 API 請求。135,653 列含沿用估值，
明示為歷史 last-trade mark，沒有線性插值或宣稱每分鐘皆為新可成交報價。
此階段仍不是完整帳務驗收或正式切換；正由 canonical promoter 獨立驗證。
[補源後分鐘來源收據](../artifacts/operations/tw_ytd_history_20261018/minute_curves_sources_complete/minute_curve_receipt.json)。

21:19 補源後完整候選通過獨立 promotion 驗收（native wall time 9m2.215s）：
190 日／五模式、256,500 分鐘全部驗證，254,600 個歷史內部分鐘的未驗證數為 0；
完整範圍帳務 audit `full_requested_range_passed=true`、error_count=0，
五模式最後未平倉皆 0。來源證明驗證的首分鐘無價組數為 41,844，
不是把缺檔或官方日開盤價當成當分鐘成交。
[完整候選驗收摘要](../artifacts/operations/tw_ytd_history_20261018/full_candidate_acceptance_compact.json)
綁定原始完整收據 SHA256，仍明示 `promoted=false`。

Canonical plan 已為 `ready_to_apply`：保留 795 份既有決策、752 組原始 artifact pins，
只新增 155 份較早缺少的反事實訊號，950 份來源逐一通過。五帳戶原始資本不變；
Factorized 維持 fold 11、checkpoint `3132bca00eb5`。21:20 開始 canonical apply，
交換前會再次完整驗證、重驗來源 journal fence 與清倉狀態，尚未以此宣稱已部署。
[正式切換計畫](../artifacts/operations/tw_ytd_history_20261018/history_extension_plan.json)。

原生瀏覽器 verifier 增加可選的首日回放揭露驗收：同時核對實際載入的公開回放標記、
日期／反事實旗標與每一個已渲染時間欄，拒絕私有 ID 洩漏、空畫面、舊日期或冒稱原始訊號；
額外保存五模式卡片截圖。相關 dashboard 測試 35 項通過；切換後才執行真實頁面驗收。

包含上述瀏覽器驗收反例的最後相關全組合：**610 項 Python（100.03s）、
48 項 Node 全數通過**，不與前述重疊次數相加，也不是全 repository suite。
[五模式／瀏覽器契約最後回歸](../artifacts/operations/agent-workflow/runs/ytd-final-five-mode-browser-cont-20261018T132237-712b8a23/run.log)。

三個 benchmark 另外使用共用 `_validate_benchmarks` 獨立通過完整分鐘格線：
0050／2330 各 51,490 列（190 日 × 271），台指期 57,000 列（190 × 300）；
來源 SHA 測前／測後相同，核對耗時 4.20 秒。這份證據僅驗證基準分鐘覆蓋，
不把它誤稱為策略會計或即時開盤延遲驗收。
[基準完整分鐘覆蓋](../artifacts/operations/tw_ytd_history_20261018/benchmark_full_minute_acceptance.json)。

21:43 原子交換完成，正式引擎新 run `12d6fefee1f4447abae50223c202ee20`
已確認五模式與 ledger integrity ready；原帳本完整保留在
`artifacts/live/tw_day_trade_ytd_five_modes_sources_complete_20261018`（交換後此路徑是 rollback，
不是新版候選）。正式 rebuild receipt SHA 維持
`f4d2e61655738b5fc823be7c676c7deeba9017968ed88a196c315cc0f92c71c1`。

部署收尾遇到真正的 canonical 來源更新：`finalize_tw_public_completed_session.py`
持有 exclusive `tw-public-refresh.lock`，Discord 正確拒絕混版暖機並原生重試。
21:46:52 更新者已發布 `expected_date=2026-10-16`／`status=ok` 並釋放鎖，
Factorized 隨後以 `3132bca00eb5` 通過原生暖機；但第一次 apply 的 240 秒
消費者期限已到，留下 `failed_retryable`／`promotion_completed=true`，沒有隱藏失敗。
接續使用同一 canonical apply 的 `_recover_promoted_consumers` 收尾，
只處理 Discord／公開服務，不再 replay 或交換帳本；此恢復已成功，
`second_ledger_promotion_performed=false`。引擎確認五模式，Discord 新程序原生
暖機 5／5 ready，公網與本機 API 皆讀回 1/2～10/16 共 190 個交易日、
Factorized 首日 2,757 筆訊號；同步 revision lag 與 ledger divergence 均為 0。
Canonical 狀態仍為 `complete_with_data_gaps`，沒有把真實來源問題改成正常。
[第一次正式 apply](../artifacts/operations/agent-workflow/runs/ytd-five-mode-history-extension--20261018T132014-c248b41a/run.log)。
[消費者恢復 log](../artifacts/operations/agent-workflow/runs/ytd-five-mode-promoted-consumer--20261018T134851-e87f3797/run.log)。

正式切換後 canonical verify／status 已通過，但瀏覽器驗收先後抓到冷查詢等待
及回放文案問題，未將這些失敗算成完成。公網原本會移除內部 `signal_id`，
前端卻依賴它辨識新補段。現在在私有來源投影前核對明確回放旗標、ID 契約與
交易日期，只輸出 `replay_signal_origin=current_model_counterfactual`，並在
signals、嵌入估值、positions 與 status 保持一致；不公開 ID 或本機路徑。
既有訊號、錯誤日期或非回放不會被重新標成新補段；沒有修改財務 journal。
此修正的最後相關回歸為 **750 項 Python（123.57s）、48 項 Node 全數通過**，
包含公網 DTO、隱私、錯誤日期、非回放與輸入不變的反例；不與較早組合相加。
前端內容雜湊更新為 `95421d9778f1`。僅重新啟動 public gateway，不動引擎、
Discord 或帳本；公網／本機筆電、手機原生瀏覽器四組於 22:14 全數通過。
[公網回放標示最後回歸](../artifacts/operations/agent-workflow/runs/ytd-public-provenance-final-regr-20261018T141046-d1fc9ba1/run.log)。

### 正式頁面最後讀回（22:17）

- 公網／本機皆顯示五模式，預設 2026/01/01～10/16，第一個實際交易日為 1/2。
  每個模式 190 日 × 270 分鐘 = 51,300 點，策略合計 **256,500 點**；0050／2330
  各 51,490 點、台指期 57,000 點，完整 response 共 416,480 點，各日格線均完整。
  此為真實 `minute_columns_v2` response／頁面讀回，不以 sampled endpoint 代替。
- 四組原生瀏覽器均通過最新單日、手選日期、恢復年初範圍及絕對資金不變；
  首日實際渲染 100 筆訊號皆明示「目前模型反事實回放（非當時訊號）」。
  每組 document navigation 僅 1 次、page errors 為空、橫向 overflow 為 0。
  已人工查看筆電／手機模式卡片與實際 viewport 截圖。CPU-2D profile 不宣稱 GPU
  或 WebGL 驗收；延伸查驗也確認未聚焦 skip link 在 viewport 外，截圖沒有被誤當 UI 故障。
- 切換後重新執行 canonical verify／status：引擎、Discord、public 三服務 active；
  引擎 ledger integrity ready、divergence=0，五模式最後未平倉皆 0；Discord 五模式
  新 run 的 native warmup 5／5 ready。原帳本 rollback 完整保留，未二次交換。
- 狀態仍是 `verified_with_data_gaps`／`complete_with_data_gaps`，不是全資料正常：
  10/16 一億模式 3 筆合法漲跌停界線缺漏；官方日 OHLC 全空的無成交紀錄為一億
  模式 16 筆、V8 4 筆；一億與 Multi-Basis 仍有部分目標未成交的真實告警。
  不創造成交、不補假價，也沒有把工程回放結果視為券商回報、PIT 實盤結果或
  09:00 延遲 SLA 驗收。

[四組公網／本機瀏覽器收據](../artifacts/operations/tw_ytd_history_20261018/browser_final_five_modes_v2/receipt.json)、
[獨立正式驗收與完整點數核對](../artifacts/operations/tw_ytd_history_20261018/final_production_acceptance.json)、
[切換後 canonical verify](../artifacts/operations/tw_ytd_history_20261018/final_canonical_verify.json)。
