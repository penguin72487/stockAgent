# StockAgent 服務完整清單（2026-09-23 起逐次更新）

## 2026-10-02 22:24：網站 review 收束，再次依使用者要求暫停

使用者正在整理／刪除資料；本次僅核對既有程序與證據、留下
[最新暫停交接](SERVICE_OPTIMIZATION_PAUSE_2026-10-01.md)。目標為 **paused**，不是完成。
沒有新啟動修改、測試、測速、來源掃描、下載、重建或發布；沒有停止／重啟正式服務、
既有排程或其他 actor，所有 dirty changes 保留，系統既有資料寫入不因此停止。
協作清單只有 root，已知本輪 browser 稽核與 responsive benchmark 無存活程序。

21:22 暫停之後的網站 review 保留共用刷新／表格效能、未知值語意、導覽與響應式修正；
既有證據為 Node 68 項、Python 299 項，以及六種尺寸的 72 個頁面／模板案例。
其時間與涵蓋版本不同，詳見交接；本次沒有重跑，也不是完整現況或資料健康宣稱。
普通觸控／鍵盤完整補驗、當沖 signals 初次 15 秒 timeout、MOPS 非法日期與其餘
後端／全服務驗收仍未完成。等使用者說繼續後，先重查 Git、資料身分及最新 receipts。
下方各次清單與健康數量均為當時觀察，不沿用為資料整理後的現況。

## 2026-10-02 21:22：依使用者要求再次暫停

使用者正在整理／刪除資料，停止後續優化與新測試、測速、來源掃描、下載、重建及
發布作業。[最新暫停交接](SERVICE_OPTIMIZATION_PAUSE_2026-10-01.md)記錄保留的修改、
證據、未完成項目與恢復起點。協作清單只有 root，已知 data-monitor benchmark、
coverage audit／pytest 沒有存活程序；沒有停止／重啟既有正式服務或干預其他 actor。
所有 dirty changes 保留，目標改為 **paused**，不是完成；等使用者明確要求繼續。
下方 20:52 的數量、狀態及耗時是歷史觀察，不作為資料整理後的現況證明。

## 2026-10-02 20:52：恢復目標、正式增量欄位發布與故障恢復

使用者控制的 goal 已恢復為 **active**，10/1 暫停紀錄保留為歷史。
沿用 `user-working-preferences` 的完整輸出、canonical 共用與證據邊界：先重查
Git／資料位置／容量及全服務清單，不沿用整理前的來源數量或健康。
大量 TEJ、FinLab／FinMind、儲存及其他 actor 的變更均保留，沒有提交或回退。

[20:19 起始清單](../artifacts/benchmarks/service-coverage-20261002-resumed-baseline.json)及
[20:52:21 全清單](../artifacts/benchmarks/service-coverage-20261002-shards-final.json)
均為 **66 installed services、50 timers、3 paths**；timer findings 0。
TEJ 新服務已納入，而不是沿用昨天的 65。起始 root 可用約 **164 GB**，
`data_tw_public` 仍指向 `/srv/stockagent-live/data_tw_public`；容量是觀察，
不是所有 acquisition／cold publication 的容量或完成證明。

### 修掉不必要重建的根因，再沿用同一份精確計算

新 footer 的 schema pairs 原為 tuple，JSON cache 解碼則為 list；內容相同仍被
視為 schema 改變，造成共用 schema 的其他 owner 也重建。Writer 現在統一為
JSON-native list pairs，真正 schema 改變仍影響全部相關 owner。
Cache 外層維持 v10／membership v1／fast-index v4，沒有另建 legacy 分支。
初次新測試的 **2 failed／114 passed** 已保留於
[初次語意回歸](../artifacts/benchmarks/data-monitor-shards-semantic-20261002-initial.log)，
修正後重新驗證，不把先前失敗改寫為通過。

`FeatureInventoryScan`／`FeatureDataset.rows` 成為全量與增量共用入口：完整逐檔
身分／缺項核對保留，只有已證明不變的 owner 可以省略欄位聚合與 JSON 編碼。
Python native integer、NULL 下界、partial、欄位角色、公開排序及完整 rows 不變。
來源移動、schema／公開標籤變更、cache base 不符、未知 delta 或 ABI 變更均重建。

### 正式 per-dataset shards 已接入既有 producer

`data_monitor_feature_shards.py` 的 content-addressed rows／preview objects 是
**可重建的私有網站投影**，不是來源、訓練資料或 cold publication。
預設藏在完整 JSON 同目錄的 `.feature_inventory.json.projection-cache`；
objects 上限 **256 MiB**，current／previous manifest 各有 **2 MiB** 上限。
保留兩個 head，僅在 writer lock 下核對 marker、明列 digest、regular file、
單一 inode link、身分及 SHA256 後回收自己產生的 orphan objects；不遞迴刪除，
不碰未知檔名、來源、receipt、packed／materialized 或其他 actor 資料。

完整舊格式 JSON 仍是公開 authority。全部 shards 核對完才 durable atomic rename，
再發布原 v3 receipt／完整來源頁；不是把部分 rows 當完整資料。
已知 object 遺失／損壞會重新做 canonical cold build；容量／權限或其他 cache
問題則走原完整 publisher，保留 fallback event。這不是冷來源重建或主機冷啟動驗收。
同 output 的 JSON＋receipt 發布共用 kernel lock，避免兩個 local publisher 的
rename／digest 競態；程序死亡釋放鎖。**不是網站全域 rate limit 或訪客併發上限**。

### 同完整資料 ABBA，首次成本另列

[最終 publisher ABBA](../artifacts/benchmarks/data-monitor-shards-pipeline-abba-20261002-final.json)
釘住兩個真實 cache 世代與當時 stat／absence，四次均核對 **59,974** 個 source
references，保留 **88,740** 欄位及完全相同的 JSON bytes。
該次 delta 為 OKX hot tail；**全量中位數 2,656.915 ms、sharded 547.734 ms**。
首次 seed 的全部分片建置為 **2,841.804／3,461.921 ms**，不能當熱路徑或零成本。
另保留[較早三 owner delta 的 ABBA](../artifacts/benchmarks/data-monitor-shards-pipeline-abba-20261002.json)。

範圍是完整欄位核對／聚合、公開 JSON、原子檔案與 receipt；source stat 用捕獲的
真實觀察做固定重播。Imports、cache decode、discovery、record refresh、真實冷 I/O、
provider、公網與 browser-paint **不包含**。零 production source writes／broker calls，
不以這個數字宣稱整個 producer 或所有服務的端到端加速率。

[自然 timer journal](../artifacts/benchmarks/data-monitor-shards-natural-cycles-20261002.jsonl)
已看到正式 cache `published`，例如 20:54:59 重用 **236**、重建 **1** owner，
完整 feature stage **1,149.233 ms**；20:55:29 重用 **234**、重建 **3**，
feature stage **1,093.333 ms**。這兩次 record／feature roots 相符，完整 workflow
仍約 **8 秒**，record decode／掃描／寫入與 public status 仍待優化。
其他持續寫入輪次有 roots 不符及 partial；沒有強改成正常，也不把不同負載當 A/B。

[首次自然世代 shadow](../artifacts/benchmarks/data-monitor-shards-natural-delta-20261002.json)
是 `inconclusive_initial_generation`：取樣時 cache／feature／receipt 未對齊。
該收據保留，不宣稱正式環境兩個完整世代的 shadow 已通過。

### 驗收與仍未完成的全服務範圍

- [最終共用回歸](../artifacts/benchmarks/data-monitor-shards-final-shared-regression-20261002.log)：
  **1,185 passed／53.95 秒**，涵蓋 data-monitor／FinLab／FinMind／imports／benchmarks。
- [最終針對性回歸](../artifacts/benchmarks/data-monitor-shards-final-publication-lock-20261002.log)：
  **126 passed／3.14 秒**，包括 native integer、來源消失／恢復、quick frame 跨世代、
  標籤／ABI、壞 object、容量不足、rename 中斷、receipt／fallback、同 output 鎖及
  symlink 拒絕。與上一項有重疊，不加總。
- [Gateway／paging／updates／imports 回歸](../artifacts/benchmarks/data-monitor-shards-gateway-regression-20261002.log)：
  **160 passed／31.10 秒**；在最後 publisher lock 修改前完成，該修改另由上述兩項
  驗證。沒有執行全 repository、browser、多裝置或 reboot。
- 實際 localhost `/data-monitor/api/features/page?offset=0&limit=80` 回應 200／80 rows，
  完整 summary **88,740 fields、59,974 files**，取樣時 **59,467** 通過 schema，仍為
  **partial**；未驗證的檔案沒有被宣稱完整。

20:52 六個 localhost status GET 全 200，但 TAIFEX **blocked**、當沖／隔日沖／
Shioaji／data monitor **degraded**、OpenBB **stopped**。Daily 收據保留 Yahoo **212**
gaps（13 failed／199 lagging_skip）；OKX feature step 仍 failed，crypto training
`deferred_raw_writer`、TW cold `deferred`。OpenBB archive 與 legacy US cold unit 的
失敗亦未由本輪修復，不能用 HTTP 200 或 systemd oneshot completed 掩蓋。

本輪 gateway／day-trade／Discord PID **3988904／1551092／898** 與起始相同 invocations；
gateway 已由其他工作在前一天之後更新，不冒稱是本輪部署。沒有重啟核心、登入券商、
送單、改帳本／模型、刪來源、清 OS cache、全量 acquisition 或 cold publication。
新增 producer 由原自然 timer 載入，不另開輪詢 daemon。

下一步仍按完整服務清冊：record cache 的解碼／聚合／寫入、public status 各來源成本，
以及 OpenBB／TAIFEX／OKX／Yahoo 真實資料健康與可靠性；自然完整世代、全庫、
browser／公網 IPv6、Windows-WSL 冷恢復尚未全部驗收。**目標維持 active，未完成全系統。**

可重測（不登入、不改來源；output 必須是新檔，固定觀察重播不是冷 I/O）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_data_monitor_projection_pipeline.py --output artifacts/benchmarks/data-monitor-pipeline-rerun.json
run_fintech_python scripts/benchmark_data_monitor_feature_delta.py --wait-seconds 50 --output artifacts/benchmarks/data-monitor-natural-delta-rerun.json
run_fintech_python scripts/audit_service_latency_coverage.py --sample-seconds 2 --output artifacts/benchmarks/service-coverage-rerun.json
```

## 2026-10-01 16:28：依使用者要求再次暫停

使用者正在整理／刪除資料，先停止優化工作並保留所有既有修改。
[最新暫停交接](SERVICE_OPTIMIZATION_PAUSE_2026-10-01.md)記錄已驗收的來源一致性修正、
尚未實作的正式分片 publisher，以及恢復時需重新核對的資料位置／身分與 runtime。
本次沒有啟動新測試、測速、來源掃描、下載、重建或發布，也沒有停止／重啟正式服務
或干預其他 actor。協作清單只有 root，已知 data-monitor benchmark／pytest 沒有存活程序。
先前的 active、資料數量、健康及耗時均是有時間標籤的歷史證據，不是整理後現況。
目標改為 **paused**，不是完成；等使用者明確要求繼續後才續接。

## 2026-10-01 16:02：來源觀察一致性、暫時缺項恢復與真實增量驗證

使用者控制的目標已恢復為 **active**；不把上次暫停當作整個系統完成。
依 `user-working-preferences`，先補共享根因／正確性證據，再測速，不靠少讀來源、
少列欄位或把 partial 改成正常。[本輪驗收與 SHA256 綁定](../artifacts/benchmarks/data-monitor-source-coherence-validation-20261001.json)
保留測試、原始自然 journal、全服務清單與測速邊界。沒有重啟核心、登入券商、
送單、換帳本／模型、刪來源、清 OS cache、cold publication 或回退其他 actor 的工作。

### 修復會把缺項留住的共享競態

原本清單統計與欄位投影分兩階段核對檔案；相同 footer revision 不代表兩階段
看見同一份檔案或缺項狀態。新增 `source-observation-v1` 根指紋，沿用原來逐檔
stat，綁定 dev／ino／size／mtime／ctime 與已觀察的缺項，不增加另一輪來源掃描。
Cache 外層維持 **v10**、membership v1；fast index **v4** 加上來源／cache 一致性。

新增測試先[重現失敗](../artifacts/benchmarks/data-monitor-transient-source-red-20261001.log)：
資料夾在彙總時短暫移走又放回，child inode 不變，舊程式仍寫出可重用的 partial
索引。現在彙總觀察不符 cached identities 時不寫該索引，下一輪重算後恢復真實
計數。同 footer 的檔案替換也更換觀察根／revision；不能用數值看似相同跳過身分。

Producer receipt 另綁定實際欄位投影的觀察根；缺少、損壞或不同根時重建。
有要求來源綁定時，只把 JSON 再 parse 一次不能補成已驗證來源。自然 journal 的
`feature_source_observation` 區分 record／feature 是否相符、record 是否符合 cache；
不一致仍保留 partial，之後重新投影。這不是官方發布時鐘、PIT 或券商成交證據。

### 測速機制與沒有被消除的成本

將 ctime 全域排序提示換成 **`refreshed-priority-uniform-v1`**：上一輪實際 footer
刷新鍵優先，其他來源仍抽樣；提示通過後仍核對全清單與五部分身分，未抽到的
變動仍走完整處理。沒有新增網站全域限流或併發上限。

[完整 frozen-metadata ABBA](../artifacts/benchmarks/data-monitor-refreshed-hint-full-namespace-20261001.json)
有 **59,028** 條 cache paths、上一輪實際刷新 pool **703**、提示 **512**，12 trials
包含 unchanged、指定 refreshed-path 變動與 unhinted 變動。刻意選「新提示命中而
原 uniform 沒抽到」的控制情境，metadata calls **59,540 → 1**，中位數
**314.79 → 4.73 ms**。Unchanged／unhinted 仍完整核對；這是記憶體中身分重播，
**不是實際冷 I/O 或完整 producer 的加速率**，不拿它宣稱網站已達極限。

[自然排程 journal](../artifacts/benchmarks/data-monitor-refreshed-policy-natural-cycles-20261001.jsonl)
新提示命中時 preflight **23.92／9.78／3.99 ms**；一次未命中仍全核對 **2,860.07 ms**。
來源變動完整 workflow 分別 **27.29／21.45／21.05／8.84 秒**，來源量與其他程序負載
不同，不能直接算 A/B 改善。較早來源一致性 journal 還有 **36.34 秒**的輪次；
完整 cache 解碼／彙總／寫入與欄位全量建置仍是下一階段重點。沒有減少欄位來換速度。

### 終於取得兩個自然世代的一致性證據，尚非正式增量發布

第一個新 shadow 視窗為[no_new_generation](../artifacts/benchmarks/data-monitor-source-coherence-natural-delta-20261001.json)，
不是通過，也沒有把正常的外部無變動當故障。確認另有新 revision 後再測，
[07:55:42 → 07:57:43 UTC 兩個完成世代](../artifacts/benchmarks/data-monitor-source-coherence-natural-delta-20261001-new-revision.json)
全 **88,706** rows 重組與完整新版相同；**7** 個受影響資料集、**179** rows 的局部
footer 重建也相同，`moved_paths=0`、`cache_identity_mismatch_paths=0`。
重組 **71.82 ms**、局部 footer **22.24 ms**；完整世代讀取／discovery／綁定另列，
不能相加後冒稱已部署的端到端 latency。這次完成的是增量基礎驗證，
**正式 immutable per-dataset shards／atomic publisher 尚未啟用**。

此前 same-revision mismatch 與現場身分不符的 inconclusive artifacts 都保留。
15:55 的公開 schema summary 仍為 **partial**：59,029 files 中 59,010 通過欄位
驗證，沒有把尚未驗證的 19 個檔案改成完整。

### 回歸與全服務狀態

- [共用回歸](../artifacts/benchmarks/data-monitor-source-coherence-shared-regression-20261001.log)：
  **1,076 passed／90.59 秒**，涵蓋 FinLab、FinMind、data-monitor、import contract
  及相關 feature／hint benchmarks。
- [公開 gateway／updates／feature pages 回歸](../artifacts/benchmarks/data-monitor-source-coherence-gateway-regression-20261001.log)：
  **160 passed／34.16 秒**，與上項有重疊，**不加總**。Ruff／py_compile／diff
  whitespace 通過；未執行全庫、browser、多裝置或重開機驗收。
- 最初 [106 passed／3 failed](../artifacts/benchmarks/data-monitor-source-observation-resumed-initial-20261001.log)
  及暫停前不存在 test filename 的 no-tests log 保留，不改寫成 pass。

[16:02:13 全清單](../artifacts/benchmarks/service-coverage-20261001-source-coherence-final.json)：
**65 installed services／0 transient、50 timers、3 paths**，schedule findings 0。
六個 localhost GET 都 200，但 TAIFEX **blocked**、當沖／隔日沖／Shioaji／全資料
**degraded**、OpenBB **stopped**。OpenBB archive 與 registered daily unit failed；
既有 Yahoo **210** gaps、crypto deferred_raw_writer、TW cold deferred 保留。
OpenBB L1 精確 matched invocation `ee9e97f813724de4a3f4e16a370524ba` 的 stage samples
有 **7,143 memory-high events／0 OOM**，是該次歷史壓力，不是當下記憶體占用。

Gateway、當沖引擎、Discord 的 PID／invocation 均保持 **1526156／1551092／898**
及前次相同 invocation，沒有為了取樣或 cache 遷移重啟。常駐 gateway 不宣稱
已載入所有新版程式；短命 producer 已由自然 timer 載入並記錄新契約。
公網 IPv6、Windows／WSL cold recovery、資料健康根因與其他全服務優化仍未完成，
**目標維持 active，不宣稱系統全部修好**。

重測（沿用現有環境；output 使用新檔，不清 cache／不觸發 provider／帳本）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_data_monitor_hint_policy.py --output artifacts/benchmarks/data-monitor-hint-policy-rerun.json
run_fintech_python scripts/benchmark_data_monitor_feature_delta.py --wait-seconds 50 --output artifacts/benchmarks/data-monitor-generation-rerun.json
run_fintech_python scripts/audit_service_latency_coverage.py --sample-seconds 2 --output artifacts/benchmarks/service-coverage-generation-rerun.json
```

## 2026-10-01 11:01：唯讀監控依賴解耦、載入耗時與自然排程證據

全服務目標維持 **active**。沿用 `user-working-preferences` 的完整流程／現場量測與有界驗收：先查 recurring producer，而非把快速 HTTP projection 說成整個資料流程已快。本輪沒有登入券商、送單、修改帳本／價格／模型、刪來源、清 OS cache 或重啟服務。[本輪交接](../artifacts/benchmarks/public-reader-reliability-validation-20261001.json)綁定程式、測試與原始觀測；本節不替代其他服務的驗收。

### 減少依賴，而不是減少資料

原監控只需讀分類／重試規則，卻因 import 下載器而連帶載入 pandas、requests 和收集工作。將 FinLab `safe_stem`、date-window 排除與 per-key retry 原樣移入 `stockagent/data/finlab_acquisition_contract.py`；FinMind Complement 的全部目錄規則移入 `downloader/finmind_catalog.py`。收集器與唯讀讀者共用同一組物件，不維護第二份清單；Sponsor 時鐘／source specs 直接沿用既有 `finmind_scheduling`。

沒有變更 69 項 Complement 目錄的順序／成員、查詢粒度、授權、release 時鐘、retry deadline、receipt 檔名或資料健康。缺 SDK／收集器依賴不再阻止唯讀模組載入；缺選用 PyArrow 時可載入，不代表仍能驗證新 footer 或宣稱資料正常。新增 fresh-process import guard，避免後續又把 worker／SDK／pandas／requests 拉回讀取路徑。

### 完整原版／新版載入對照，不是完整工作加速率

[最後 ABBA](../artifacts/benchmarks/public-monitor-imports-abba-20261001-instrumented.json)使用[修改前五支模組的實際 frozen source](../artifacts/benchmarks/public-monitor-imports-baseline-20261001.json)，兩側使用相同 source loader；每個 target 各 8 次 fresh interpreter，共 **24 次**。比對完整有限目錄與命名／重試案例 SHA，結果一致；新讀者均未載入 worker、pandas、requests 或券商 SDK。中位數如下：

| 載入邊界 | 原版 → 新版 wall | 程序 peak RSS 原版 → 新版 |
| --- | --- | --- |
| 30 秒監控 producer 的模組載入 | 1,129.76 → 448.82 ms | 140.46 → 83.24 MiB |
| 全資料監控模組 | 1,111.08 → 455.51 ms | 140.28 → 83.01 MiB |
| FinLab 面板模組 | 732.04 → 23.18 ms | 126.69 → 29.79 MiB |

這是**載入邊界**，不含完整建置、provider、HTTP、browser paint 或 Windows/WSL 冷開機；OS cache 未控制。不同試跑受負載影響，保留所有 receipts，不把前一輪較慢值與最後新版值拼成更大的加速率。既有常駐 gateway 沒重啟，不宣稱它已載入新版或當下 RSS 減少上述幅度。

### 自然排程與仍然昂貴的來源變動

producer 已增加可持續記錄的 `dependency_import_observation`（nested schema v1）：shared import wall、範圍與 completion 時是否載入三支 worker。它不包含 interpreter／stdlib 啟動；既有 `total_ms` 仍是 main workflow，不改舊計時意義，也不把兩者相加冒充 systemd 完整 wall。

[自然排程原始 journal](../artifacts/benchmarks/public-monitor-natural-cycles-instrumented-20261001.jsonl)中，10:54:07–10:56:08 五個不同 invocation 的 shared import **198.37–284.87 ms**，三支 worker flags 全 false；來源 **1,888**、欄位 **88,703**。各輪仍逐檔核對並更新 footer，未減少來源範圍／公開欄位。不能由欄位數推論歷史或 PIT 完整。

**剩餘主要成本仍在**：即使 10:55 輪僅變動 1 個檔案，main workflow 仍 **9.65 秒**；其他變動輪 **9.36–14.40 秒**，[稍早 journal](../artifacts/benchmarks/public-monitor-natural-cycles-20261001.jsonl)亦有 **16.51 秒**。新增檔案／身份／schema／non-null count 變動會使整份特徵投影與持久 cache 重建，不能拿 unchanged fast-index hit 和 changed-source rebuild 直接算加速率。稍早一輪因來源動態觀測只有 88,670 欄，下一輪回到 88,703；沒有把動態缺項改寫成完整。下一階段優先 canonical per-dataset 增量投影／原子 manifest，保留完整身分核對、未驗證／缺項狀態及冷重建 fallback，而不是用 directory mtime 跳過原地改寫。

### 驗收與部署邊界

[最終相關回歸](../artifacts/benchmarks/public-monitor-contract-regression-20261001-accepted.log)：**979 passed／48.47 秒**，涵蓋全部 `test_finlab_*`、`test_finmind_*`、`test_data_monitor_*` 與 import contract；Ruff、py_compile、diff whitespace 通過。早期錯誤 test filename 的 no-tests 試跑保留，但不算成通過，也不把重疊的 165 項 telemetry 試跑加總。沒有執行全 repository suite。

[10:52:18 全服務觀測](../artifacts/benchmarks/service-coverage-20261001-public-reader-contract.json)為 **66 services＝65 installed＋1 transient、50 timers、3 paths**，schedule／當次 pressure findings 0。六個 localhost GET 為 200，但 TAIFEX blocked、當沖／隔日沖／Shioaji degraded、OpenBB stopped、全資料 degraded；HTTP 成功不代表 source／execution 已修好。Discord maintenance、OpenBB、registered daily／features 的 failed unit 狀態保留。

公開 gateway 仍 PID **1526156**、09:55:55 invocation `c12a00583bc844fc9060d8a808a2384a`；本輪短命 producer 自然載入，常駐讀者部署仍待安全窗口。引擎目前 PID **1551092**、10:14:35 invocation `ab27422100a04db99c6bfef872f9c863`，與前次記錄不同，本輪沒有做該重啟。沒有驗證新開盤、外部 IPv6、browser 或 Windows/WSL 重開機，也未宣稱所有服務已修好／達效能極限。

可重測（只讀／新程序，不清 cache、不執行下載／帳本；output 用新檔）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_public_monitor_imports.py --baseline artifacts/benchmarks/public-monitor-imports-baseline-20261001.json --output artifacts/benchmarks/public-monitor-imports-rerun.json --rounds 2
run_fintech_python scripts/audit_service_latency_coverage.py --sample-seconds 2 --output artifacts/benchmarks/service-coverage-public-reader-rerun.json
```

## 2026-10-01：Shioaji 查詢／回補可量測性、排程公平性與官方視窗修正

全服務目標仍 **active**；沿用 `user-working-preferences` 的現場證據與有界驗收，以及 Shioaji skill 的單一既有連線／callback／local-first 流程。先審查並保留工作樹中其他 actor 的背景回補候選，再做共用修正；本輪沒有另開券商登入、送訂單、換帳本／模型、刪來源或重啟核心。[本輪交接](../artifacts/benchmarks/service-reliability-shioaji-timing-validation-20261001.json)綁定最終程式、測試與測速 SHA256。

### 先把真正成本與未量測部分分開

今天 09:01 回補的 retained receipt 有 **820** 個已解析價格，09:16 才完成；舊 traffic ledger 同 consumer 的 **820** 個 query event，`duration_ms` 加總約 **279.89 秒**，不能解釋整段約 15 分鐘。舊計時不包含 before-usage 與 ledger 寫入，成功／失敗的 after-usage 邊界也不同。這只證明存在未量測成本，不能推論全部時間都花在某個 RPC 或 fsync。

- Canonical `shioaji_query` 現在保存 before-usage、request body、after-usage、pre-record 分段；可選的 caller-owned timing 另取得 ledger 寫入與完整 context，再由既有 remote-fetch receipt 聚合。沒有新增 network／usage calls，也沒有為了量自身寫入而再寫第二筆 event。
- 舊 `duration_ms` 的相容邊界保留；新量測／row-count contract 是 **`query-phases-v1-column-rows-v1`**。Request body 包含 context 內處理／callback 等待，不冒稱純券商傳輸。Ledger 失敗僅標記 `ledger_record_failed`，不能改動有效行情或原查詢例外；無法量測的 wrapper 保留 unmeasured／null，不補造 0 ms。
- Native Ticks／KBars 的 `len()` 可能回傳欄位數；現在先以 `ts`／`close` column 數計 observations，list-of-rows 仍照列數。820 個舊 event 的 rows 加總 **6,560** 不能解讀為 6,560 個 ticks；舊原始 ledger 不重寫。Summary 可能混合不同觀測契約，不能拿舊 rows 宣稱歷史完整。
- Remote fetch receipt 分出 API acquisition、配額 guard、contract／鎖等待、歷史 admission、query phases、resolved callback 與剩餘處理。Resolved callback 的每檔原子收據寫入包含在 fetch 中，另列其成本；外部 local scan、最後 outer receipt、NAV 與完整復原工作不包含。不能把 last-batch timing 當整個 820 檔回補時間。

### 修正會造成停止服務或無謂等待的排程

[永豐目前官方限制](https://sinotrade.github.io/zh/tutor/limit/)為行情總查詢 **50 次／10 秒**，不是舊版 50／5 秒。候選 background gate 的 40／5 秒與另一 entry-book 的 50／5 秒都有過快風險；改讀 canonical `downloader.common` 的相同 profile。09:01 ticks／KBar fallback、historical entry books、current-minute KBars 共用這個**程序內歷史視窗**，維持原候選 20% reserve，即目前歷史最多 **40／10 秒**，不再各函式／各 batch 重設或維護三套迴圈。這不是網站全域限流，沒有新增 32 concurrency cap。

另外修正高優先失敗標的的 retry starvation：先讓尚未查詢的標的前進，組內仍保留 mode priority；還有 fresh work 時不受其他標的的 5 秒 retry cooldown 拖累。只有純 retry／source-settling 才等 5 秒，quota stop 仍等 60 秒。沒有增加曝險、減少原訊號／分鐘點或改價格契約。

**這是局部防護，尚非全部券商規範驗收**：官方另列盤中 ticks 10 次／KBars 270 次及少量必要查詢／使用推播的要求；跨程序、snapshots、其他 clients 的總配額／盤中限制仍需追查，不能把程序內 40／10 秒當全帳戶保證。沒有實際重查今天 820 檔、放寬 provider ceiling 或用追加登入規避。

### 最終同資料、隔離 ABBA 與回歸

[最終 fixture-only ABBA](../artifacts/benchmarks/shioaji-recovery-fixture-abba-20261001-final.json)：相同 8 個標的、固定 native／usage replies、每 variant 2 次；canonical ledger fsync／atomic receipts／dispatch 是實作，broker latency 是替身。同步 main-call 最大值的中位數 **144.33 ms**，背景為 **0.42 ms**；完整回補 **144.33 → 148.28 ms**，**整批沒有更快**。改善是主迴圈可回應，而非宣稱 provider／今天開盤訊號已加速。四次完整價格 SHA 相同；32 個 events／64 observations／31,668 bytes journal，零 live broker requests／正式 source writes、promotion=false。Native callback 替身正確回送結果，不把 timeout=0 placeholder 當有效資料。Imports、OS cache control、完整 live job、browser／公網均未量測；8-symbol probe 未觸及 40-call 視窗，不用這個數字替代 admission 壓力測試。

[最終 15 模組回歸](../artifacts/benchmarks/service-shioaji-timing-regression-20261001-final.log)：**774 passed／12.99 秒**；Ruff、py_compile、diff whitespace 通過。涵蓋成功／失敗／interrupt timing、無額外 usage calls、ledger／reporting sink 失敗隔離、native column rows、callback 收據耗時、價格／張股與 09:01 邊界、原子分批恢復、fresh-work 公平性、官方 profile／跨 batch 視窗、當沖／隔日沖、Shioaji collector／panel／schedule、全服務 audit。先前的 callback fixture 與 module-cache 隔離試跑失敗已修正並重跑；保留失敗 log，不加總成更多通過測試，也未跑全 repository suite。

### 現場、版次與剩餘風險

[10:11:29 全清單](../artifacts/benchmarks/service-coverage-20261001-shioaji-timing-final.json)：**65 services／50 timers／3 paths**，schedule findings 0；比前一份清單多了其他工作流程安裝的 legacy-US cold-publication service／timer，尚未執行不算資料發布完成。六個 localhost GET 全為 200，但 TAIFEX **blocked**、當沖／隔日沖／Shioaji **degraded**、OpenBB **stopped**、data monitor **degraded**。當沖 status GET **5.08 ms** 是 projection transfer，不是開盤、券商、冷來源、browser-paint 或完整回補速度。

現場 Discord PID **898**、同 invocation；當沖引擎／公開面板由其他工作流程先後於 **09:37／09:55** 重新啟動，最新 PID **1526022／1526156**，invocations 已不同。本輪沒有重啟；`NRestarts=0` 不能證明沒有手動重啟。10:11 engine active、四 mode session_date 均為 10/1，仍不等於全行情或券商成交完成。10:13 的現場 stock snapshot 已看到新 query contract：before-usage **18.28 ms**、body **79.36 ms**、after-usage **30.05 ms**、pre-record **127.71 ms**；這證明早前共用量測有載入，**不證明 09:55 後的正確視窗／公平性／callback 計時等最終版本全已部署**。

本輪末次 root statvfs 一般 available **59,925,037,056 bytes**、含 reserved free **169,892,970,496 bytes**，與先前 available=0 是不同時間的觀察；本輪沒有刪除或搬移，不能由差值推論誰回收了哪些來源，亦不代表每一工作容量／publication preflight 都通過。重型 cold reconstruction 仍維持盤中保護。

後續需在安全部署窗口驗收最終版、下一次真實開盤與全工作 timing；追查跨服務 native account budget／intraday 查詢、仍存在的 source gaps／Discord maintenance／TAIFEX／OpenBB 健康、全庫、公網 IPv6／browser、Windows-WSL cold boot。**不宣稱所有服務已修好或效能達到極限，目標維持 active**。

可重測（固定 fixtures，不登入／不改 source；output 必須是新檔）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_shioaji_recovery_responsiveness.py --output artifacts/benchmarks/shioaji-recovery-fixture-rerun.json
run_fintech_python scripts/audit_service_latency_coverage.py --sample-seconds 2 --output artifacts/benchmarks/service-coverage-shioaji-rerun.json
```

## 2026-10-01 09:18：Yahoo 匯率重修與退出碼修正，開盤等待風險仍在

沿用 `user-working-preferences` 的現場證據與有界驗收流程，並以資料品質 skill 作為工程檢查的 companion：先確認來源身分、交易日／查詢邊界、code 粒度與本次／累積報告，再改共用契約，不將未解決來源問題改成正常。[本輪交接](../artifacts/benchmarks/service-reliability-yahoo-validation-20261001.json)保存程式／回歸／測速／來源報告 hashes 與現場邊界。沒有重新啟動核心、修改帳本／模型、下單、全量下載、刪來源、force GC 或放寬容量 floor。

### Yahoo FX：消除 producer／precheck 自相矛盾

58 個 canonical Yahoo 匯率檔案都有有效 `source=yahoo`、`asset_class=forex`、requested-start 與 checked-through metadata；現場最後價格為 9/30，查詢邊界為 10/1。Writer 原本會移除全零／null 的無實際意義成交量，precheck 卻把 `Trading_Volume` 設為所有資產的必填欄，每天判定全部 58 檔 `schema_mismatch`，觸發從 2000 年開始重抓與重寫。這不是行情缺漏，也不應補造 volume=0 來掩蓋。

- Canonical repair 現在按 asset 驗證欄位：forex volume 可省略，有實際數值仍保留；台／美股與 crypto volume 仍必填。缺 OHLC／adjclose、來源錯誤、日期或 asset metadata 錯誤仍不通過；真正 stale 仍安排 overlap 增量 merge，沒有跳過更新。
- Acquisition contract **v2**，FX fingerprint **`da9beafa7f8333cdea9fc53014f14a15272b422a3df72b11a09a41406ab0afe8`**。價格 schema／grain 維持 **v1／daily**；後續新檔／新 source summary 記錄 contract，不回寫舊來源 metadata、冒稱已有正式 v2 run 或改 training ABI。
- 同一 daily runner 的 `run_yahoo_incremental_assets` 用 `if ! ...; then rc=$?`，取得的是邏輯反轉後的 0，而不是原失敗碼。改成成功／失敗分支擷取原退出碼，且兩種結果都恢復 asset scope；0／1／7／42／124 均有 regression。保留原 source-gap gate，不把有 productive rows 的 exit=0 解讀為無缺口。

[58 檔、13,754,753 bytes 唯讀 ABBA 驗收](../artifacts/benchmarks/yahoo-forex-readonly-precheck-20261001-validated.json)：相同檔案／10/1 查詢邊界，legacy 安排 **58** 次全歷史 schema rebuild，candidate 為 **0**、58 current；四次 trial 的每檔 SHA256 與 repair-report hash 前後相同，**零 network／source writes、promotion=false**。Precheck 中位數 legacy **17.13 ms**、candidate **21.17 ms**，新版本身沒有更快；改善是避免錯誤觸發的 provider fetch／全歷史重寫。不能把前次正式 **16.666 秒**與這個唯讀 planning 的 **21.17 ms**當整批下載 A/B，imports／discovery／subprocess precheck timeout／provider／cold-start／全工作未包含；OS cache 未控制。

[最終 12 模組回歸](../artifacts/benchmarks/service-yahoo-regression-20261001-validated.log)：**642 passed／9.14 秒**；Ruff、py_compile、bash syntax、diff whitespace 通過。涵蓋 forex volume／incremental／source identity、台美股與 crypto 必填 volume、report preservation／leading-zero code、真實失敗退出碼、source run binding、shared transport／locks／limiter、public economic／monitor／service audit。沒有執行或宣稱全 repository suite。

### US 的 210 筆 source gaps 仍未解決，報告不能混算

精確正式 run **`registered-daily-20260930T223000400818411Z`**：US source **1,223.664 秒**、step 1,225 秒；**12 failed＋198 lagging_skip**，matched source summary 與 step receipt 的 unresolved=210。12 個 metadata-invalid code 都對到本次 failed；其中 CWAN 來源為 CBOE、WSR 缺 requested-start、其餘 10 缺來源／日期證據，沒有補 metadata 假冒 Yahoo，也沒有為了綠燈取消 lag 門檻。

`repair_report.csv` 是 **16,427 個唯一 code 的本次 precheck**；`download_report.csv` 是保留舊結果的 **12,046 個唯一 code 累積 report**，其中 14 failed 有 **RZAI／THRMV** 兩筆不在本次 metadata-invalid cohort。因此本次不能報成 14 failed，也不能把累積 99 history-extension-unavailable／47 retry 說成今天的新錯誤。歷史退市股／來源缺口仍保留，未做全 US 重抓或清理。下一個 eligible 正式 source run 才可驗收新版端到端。

### 開盤與全服務：可連線不等於完成執行

[09:12:39 全清單](../artifacts/benchmarks/service-coverage-20261001-yahoo.json)：**64 services／49 timers／3 paths**、timer findings 0。六個 localhost GET 皆 200，但 TAIFEX blocked、當沖 stale、隔日沖／Shioaji degraded、OpenBB stopped、data monitor degraded；舊 Frankfurter failed、Yahoo 210 gaps、OKX feature failed、crypto-training deferred_raw_writer 仍未清除。HTTP timings只包含 localhost response transfer，不代表公網／browser／uncached／成交延遲。

今天四模式 `opening_signal_latency.jsonl` 的 immutable-ready 延遲 **15.29／15.67／16.11／16.48 秒**；第一模式 realtime prepare 約 2.65 秒、quote fetch **12.06 秒**，不是 138 ms model inference 佔主因。四筆 signal-ready receipt 不等於 simulation fills。09:15 引擎 `status.json` 仍為 **09:01:01／waiting**、各 mode session_date 仍 9/30；journal 顯示當日 09:01 VWAP 回補持續進展到 **500／820**。應追查長時間同步資料取得與 control-loop／status 更新的串接，不能縮短價格證據窗口、插值或回填假成交來解決。此次未重啟正在回補的引擎。

**09:20:25 再查**：引擎更新至當下、health 為 **active**，四 mode session_date 均切到 **10/1**，沒有重啟。這不消除剛才的長時間等待，也不證明券商成交／全行情完整；09:20 stock-stream 記錄 requested **366**、subscribed **200**、available **193**、capacity_limited **166**，數值是各自的覆蓋／容量定義，不能簡單相加。交接保留此恢復後狀態，前面的全清單與 09:15 snapshot 是有時間標籤的恢復前證據。

[09:21:26 全清單再查](../artifacts/benchmarks/service-coverage-20261001-yahoo-post-opening.json)：仍為 **64／49／3**；公開當沖投影由 stale 回到 **degraded**，不是全正常。TAIFEX blocked、隔日沖／Shioaji degraded、OpenBB stopped、data monitor degraded；全部 localhost GET 仍 200。引擎 active、畫面已更新與資料／執行健康分開呈現。

三核心 PID **898／606482／715544**、原 invocation 與 NRestarts 0 未變。09:15 root 一般 available **0**、含保留區 free **72,459,042,816 bytes**；reserved 不是 acquisition budget。容量值有波動，不用兩次 sampling 推算是哪個 PID 的 volume 寫入或精確滿盤時間。正式大型歷史／source 重建、cold publication、全庫、公網 IPv6／browser／Windows-WSL cold boot 仍未驗收；**不宣稱所有服務正常或最佳化完成，goal 仍 active**。

可重跑（不修改 source；output 必須是未存在的新檔）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_yahoo_forex_precheck.py --end-date 2026-10-01 --output artifacts/benchmarks/yahoo-forex-precheck-rerun.json
run_fintech_python scripts/audit_service_latency_coverage.py --sample-seconds 2 --output artifacts/benchmarks/service-coverage-yahoo-rerun.json
```

## 2026-10-01 08:40：Frankfurter 歷史前段誤判修正，目標仍 active

沿用使用者工作偏好的「現場失敗 → 共用機制 → 同源驗證 → 明確驗收邊界」。[本輪交接](../artifacts/benchmarks/service-reliability-frankfurter-validation-20261001.json)包含 13 份程式／測試／正式失敗報告／實測 receipts 的 SHA256；沒有新增服務或下載框架、重啟核心、換模型／帳本、送訂單、刪來源、force GC、解除 pins、降低容量 floor 或終止其他 actor 的工作。

### 真正原因：沒有發布的前段，不能冒充檔案損壞或補造價格

Registered daily `registered-daily-20260930T223000400818411Z` 的 Frankfurter 不是整批未取得：**754 updated_incremental、116 failed_existing_read**。116 筆全部是 BRL／CNY／ILS／INR，各 29 pairs；皆在查詢 `1999-01-04..2000-01-12` 時收到 404，因而阻斷後續 tail，不是 Parquet 讀壞。本機四個 USD pairs 的第一筆是 **2000-01-13**，最後一筆仍 **2026-09-25**。

核對相同窗口的 **native EUR／ECB v1**：267 個發布日皆無這四種 base 的觀測；另行只讀查詢 2000-01-13 時四種幣別才出現。V2 的 currency metadata 起始日混合多個來源，不能拿來充當 ECB 的發布起點；[Frankfurter 官方文件](https://frankfurter.dev/)亦說明 v2 預設 blended、指定 provider 才是單一來源。本輪保留[既有 v1 ECB 契約](https://frankfurter.dev/v1/)，不切換成 blended feed。

- 只對 **404** 尋求 absence 證據：必須有完整、非空、amount=1、base=EUR、日期範圍完全一致、日期／幣別／數值有效的原生回應，且全窗口沒有該 base。若其實有 base 觀測、窗口偏移、壞資料、native EUR 自己 404，或其他 HTTP／認證／伺服器錯誤，仍保留失敗，不標正常。
- 每個窗口只保存一份共享 `.head_sources` parsed provider response；head proof 綁定來源 SHA256、實際 first date、查詢範圍與 acquisition **v2**。缺少／改變／損壞的證據不能沿用；先前只查短窗口的 proof 也不能覆蓋較長的 end。未發布日期保持沒有價格，**沒有計算 EUR cross-rate、補零、內插、造價或捏造 broker fill**。
- 真正 head source 錯誤改成 `failed_head_source`，保留可讀原檔的實際 row count；真正檔案錯誤仍是 `failed_existing_read`。源資料或交易問題不改成正常。

### 效率、復用與併行穩定性

移除 `.app` legacy 跳轉，直接使用同一個 `.dev/v1`；保留既有原生 shared limiter／有界重試與 base/date singleflight，不新增全域 32 concurrency／任意 throughput cap。手寫、只含 PID 的 temporary writers 改用既有 `artifact_io` 原子發布，保留 Snappy／schema／price dtype；主 collector 沿用 canonical `.download.lock`、finite nonnegative `--lock-timeout-seconds`，覆蓋來源讀取、所有寫入、summary 與 optional v2 子工作，例外後也釋放鎖。不另建 cache／fetch framework。

Acquisition v2 fingerprint **`897d988772250aff77ea335bae479d0b6bfb56150d753394f3483803bcb2c1e5`**；price ABI 仍 **v1**。下一次正常 collector invocation 才會寫新正式契約；沒有改寫舊正式 receipt 來宣稱已部署／已完整取得。

[固定已完成窗口的最新 ABBA 實測](../artifacts/benchmarks/frankfurter-head-isolated-native-20261001-validated.json)：每 variant 2 samples，舊 alias 中位數 **352.18 ms**、native v1 **59.92 ms**，減少 **82.99%**；四份完整 JSON payload 的 normalized SHA256 全相同。前兩次測量也保存為獨立 receipts，時間不同，不選最快當常數。這只是 GET＋JSON 的單端點比較，provider／OS cache 未控制、redirect HTTP legs 未逐個計數；**不是全 870 pairs、完整 daily job、首次 process cold start、browser 或 opening signal 的改善幅度**。

### 隔離驗收，不替換正式來源

同一可重跑工具只複製 **BRLUSD／CNYUSD／ILSUSD／INRUSD** 到有界 temporary workspace，沿用 canonical `_download_pair`，實際查到 9/30；三個原有 6,834 rows 成為 6,837，ILS 6,833 成為 6,836。**全部原有 observations／dtype 完整保留**，未發布前段仍空缺，四個正式來源 SHA256 前後不變。這不證明 2000～2026 全部 interior history 無缺。

第一個 evidence＋tail pass 約 **2.79／1.22／1.25／1.18 秒**；下一次無變更再檢查 **1.44／1.01／1.21／1.01 ms**、網路 calls **0**，不是把熱路徑當冷啟動。共享 source response 只有一份。工具的 **14 logical GET calls** 有 admission budget，benchmark 自己 max_retries=0，不改正式 retry policy。Parsed provider response、觀測時間、query、exact serialized-source SHA256 與每個 head proof 都保留於 benchmark receipt，temporary copies 清除但正式 source/report 沒有修改，**promotion=false**。

[9 個模組最終回歸](../artifacts/benchmarks/service-frankfurter-regression-20261001-validated.log)：**270 passed／4.53 秒**，包含共享前段、實際 tail 保留、壞／缺／改 source proof、各 HTTP 失敗、短窗口、源／檔錯誤分離、writer-lock 例外與 timeout、benchmark admission／不 promotion、public economic／keyed receipts、native limiter 與 data-monitor inventory。Ruff／py_compile／diff whitespace 通過；前面的 collection error 已修正 import 並完整重跑，不把試跑次數加總，不宣稱全 repository。

### 現場仍未完成的項目

[08:27:47 完整清單再查](../artifacts/benchmarks/service-coverage-20261001-frankfurter-final.json)：**64 services／49 timers／3 paths**、timer findings 0。六個 localhost GET 均 200，但 TAIFEX **blocked**、當沖 **degraded**、隔日沖／Shioaji **waiting**、OpenBB **stopped**、data monitor **degraded**。Registered daily 舊 Frankfurter failed／Yahoo **12 failed＋198 lagging_skip**、舊 OKX feature failure、crypto-training deferred_raw_writer 與 TW cold 未接受的 source-only release 都沒有清除；availability 不等於 source／execution acceptance。

08:40 核對三核心 PID **898／606482／715544**、原 invocation、NRestarts 0，沒有重啟或破壞開盤保護。Root 一般 available **0**、含保留區 free **83,602,890,752 bytes**；D available **2,529,952,428,032 bytes**。保留區不是 acquisition budget、D 空間不是搬移授權，沒有啟動正式全量來源／歷史重建。容量仍在消耗，是持續風險；**不宣稱系統正常、全服務已優化到極限或本輪 source promotion 完成**。

下一輪繼續安全容量／writer 現場、Yahoo 精確失敗、data-monitor 重掃／projection 與其他服務；正式 870-pair run／完整 source-history、全庫、公網 IPv6／browser、Windows／WSL cold boot 仍需各自驗收。目標維持 active。

可重測（只寫新 benchmark receipt，已有 output 會拒絕覆寫）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_frankfurter_head_repair.py --output artifacts/benchmarks/frankfurter-head-rerun.json
run_fintech_python scripts/audit_service_latency_coverage.py --sample-seconds 2 --output artifacts/benchmarks/service-coverage-frankfurter-rerun.json
```

## 2026-10-01 07:43：OKX 原生端點分流與保留區容量監測，目標仍 active

依使用者工作偏好的「現場證據 → 共用實作 → 同資料測速 → 逐項驗收」流程；本輪沒有新增下載框架／daemon、重啟核心服務、換模型／帳本、送出訂單、刪除來源、解除 pins 或降低容量 floor。完整 source hashes、回歸與現場邊界見[本輪交接](../artifacts/benchmarks/service-reliability-okx-reserve-validation-20261001.json)。

### OKX：減少端點配額等待，不刪分鐘或改資料語義

9/30 的 registered-feature run 不是整批未下載：481 個 feature 標的有 480 updated、1 partial；**WLD-USDT-SWAP 的 mark_price 因 RemoteDisconnected 失敗**。現行 transport 已有有界斷線重試，沒有再實作同一修正／增加 retry budget。當次 index history **7,218 grants、0.2 秒 interval**，aggregate worker seconds 含共用 limiter 等待，不能相加成全流程 wall。

[OKX 官方文件](https://app.okx.com/docs-v5/en/#public-data-rest-api-get-index-candlesticks)說明近期 index candles 保留最新 1,440 筆、20 requests／2 秒；歷史端點 10 requests／2 秒；**兩者每頁最多都是 100**，不是提高 page limit 到 300。

- 在 canonical historical-feature reader 中，只有整頁位於近期保留區內、再扣一頁安全 margin 時，才使用 recent endpoint；更舊區間仍走 history。空頁、短頁、非連續／未完成頁或 recent error 從**同一 cursor**退回 history，不提交不完整 recent page，也不反覆切回失效 recent endpoint。history 仍失敗就保留 failed／partial 與已有價格。
- 在原共用 native limiter 加入 recent endpoint 的獨立官方 profile，保留使用者較慢的 explicit interval；沒有全域速率／32 concurrency 限制。每個標的的 CSV 多記錄 index acquisition fingerprint、logical page calls 與 fallback reason；transport grant telemetry 仍包含實際 retries，兩種計數不混用。
- Array、funding object、Rubik 三種 history pager 共用 typed row／timestamp／cursor 驗證；重複頁或壞回應不再被靜默當成「歷史結束」。合法稀疏資料仍維持缺值，不補造或內插。
- Acquisition contract **v2／SHA256 `45b41fda489d0dcf529d374e8816d07f5e453e0dd46f9f5fd6be991557b53218`**；feature ABI 仍 v1、completed 1m／native 5m／causal funding 語義與欄位不變，保留相容 `data_okx/1m` root。Catalog／summary 記錄新 acquisition contract，不改原有 price files／receipts 來冒稱新正式 run 已完成。

| 固定已完成區間 | ABBA samples／variant | history-only median | hybrid median | 延遲減少 | 完整資料／dtype parity |
| --- | --- | --- | --- | --- | --- |
| [BTC-USDT，1,440 分鐘](../artifacts/benchmarks/okx-index-routing-btc-1440-abba-20261001.json) | 4 | 2.993924 秒 | 1.591330 秒 | 46.85% | 全部相同、全部 1,440 點 |
| [ETH-USDT，2,880 分鐘](../artifacts/benchmarks/okx-index-routing-eth-2880-abba-20261001.json) | 2 | 5.755572 秒 | 4.383307 秒 | 23.84% | 全部相同、全部 2,880 點 |

實際 anonymous public GETs，原生 shared buckets；BTC 每 variant 都是 15 頁，hybrid 改為 recent 13／history 2，**沒有少抓或減少總 HTTP 頁數**。只測 fetch＋normalize，排除 imports、其他七項 features、全市場、檔案冷啟動、browser／成交延遲；provider／OS cache 未控制。兩份 receipts 的程式 hashes 均再次與本輪最後程式核對。正式全市場下一次 eligible run 尚未驗收，因此 **舊 OKX failed receipt 未清除，也不推稱整批已快一倍／已修好**。

### 保留區：一般可用量降到 0，不能因此停止觀測容量變化

本輪開始一般 available 約 11.5 GB；07:23 核對已是 **0**，但含 reserved 的 `f_bfree` 約 **107.5 GB**，不是宣稱檔案系統完全沒有 free blocks。07:43:15 再查一般 available **0**、含保留區 free **102,555,701,248 bytes**；D 槽 **2,529,952,428,032 bytes**。保留區不是 acquisition budget，D 空間不是搬移授權；沒有削減保留量／floor、force GC 或終止另一項工作。

原 trend 只扣 available，在 available=0 後會漏掉繼續佔用的保留區。現在沿用**同一次 statvfs**，增加 free-including-reserved、used、reserved-or-unavailable-free 及配對 delta；仍每五分鐘一次、同個 bounded baseline／rotated journal，沒有 walk 或更多輪詢。未知 epoch、counter、geometry／clock 異常與舊 baseline 缺欄位均維持 null，不把缺值當 0。

既有 hardened worker **自然載入**新計量，未重啟／加權限。07:32／07:37 兩次 **64 services** 的事件，各四個 journal frames 已以 `journalctl --all -o json` 完整取回／重組／核對 SHA256。第一個 sample 對舊 baseline 的新增 counter delta 是 null；第二個 sample 以相同真實 systemd invocation anchor 證明 epoch，available delta **0**，含保留區 free delta **-406,310,912 bytes**、used delta **+406,310,912 bytes**；capture **27.059／17.770 ms**。這是 volume net used change，**不能歸因某個服務、當成檔案 growth 或與 parent／child／cgroup I/O 相加**。

### 回歸與剩餘服務狀態

[17 個模組最終回歸](../artifacts/benchmarks/service-okx-reserve-regression-20261001.log)：**964 passed／43.43 秒**。包含 recent／history 跨保留區、同 cursor fallback、壞頁／重複頁、完整八階段 synthetic enrichment＋實際 Parquet write/read 的全部 feature／缺值模式／coverage parity、原生配額、重試、有界測速、來源 run／report／receipt 串接、public dashboards、data inventory、storage-pressure 與新保留區 delta；早先 119／86／10 項試跑有重疊，不加總。Ruff／py_compile／diff whitespace 通過，不是全 repository／正式歷史／真實成交或冷開機驗收。

[07:32:06 的完整服務觀測](../artifacts/benchmarks/service-coverage-20261001-okx-reserve-final.json)：**64 services／49 timers／3 paths**，timer findings 0，六個 localhost GET 均 200；TAIFEX **blocked**、當沖 **degraded**、隔日沖／Shioaji **waiting**、OpenBB **stopped**、全資料監控 **degraded**，不改寫為正常。Registered daily 又有 Frankfurter failed，以及 Yahoo **12 failed／198 lagging_skip**；crypto-training-refresh deferred_raw_writer。TW cold 最新 source-only attempt 仍 deferred stale_derived_receipts（feature source_bytes／official lifecycle＋source receipts），沒有 release；不以 unmatched receipt 作驗收。OpenBB L1 matched invocation `2499b43ae877431b86ef754b65505a01` completed，但 memory-high delta **8,242**、OOM／kill 0；不能說已解決其重掃與 resource pressure。

07:43 三核心單元 PID **898／606482／715544**、原 invocation、NRestarts 0；HTTP／active 只證明可達與監督。下一輪優先安全容量／actual volume use、Frankfurter／Yahoo 精確來源失敗、OpenBB 重掃與 recurring data-monitor projection 成本，再在容量及 opening admission gates 下做正式來源／FIFO history 驗收。全市場 job 性能、全服務資料健康、全庫測試、公網 IPv6／browser 與 Windows／WSL cold boot 都仍未全部完成，**目標保持 active**。

**07:52:22 容量再查**：一般 available 仍 **0**，含保留區 free 已降至 **98,182,283,264 bytes**。沒有因為一般 available delta 為 0 而宣稱容量穩定；繼續保留大型作業 admission／opening 保護，不把保留區改算成可用額度。

可重測（public read-only probe 只寫新 benchmark receipt；已有 output 會拒絕覆寫）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_okx_index_routing.py --inst-id BTC-USDT --minutes 1440 --lag-minutes 30 --repetitions 2 --budget-seconds 60 --output artifacts/benchmarks/okx-index-routing-btc-rerun.json
run_fintech_python scripts/benchmark_okx_index_routing.py --inst-id ETH-USDT --minutes 2880 --lag-minutes 30 --repetitions 1 --budget-seconds 60 --output artifacts/benchmarks/okx-index-routing-eth-rerun.json
run_fintech_python scripts/audit_service_latency_coverage.py --sample-seconds 2 --output artifacts/benchmarks/service-coverage-okx-reserve-rerun.json
```

## 2026-10-01 06:45：程序寫入與容量風險監測，目標仍 active

沿用使用者工作偏好的「清單 → 現場量測 → 共用修正 → 驗收」流程。沒有新增常駐服務、放寬公開服務權限、重啟核心服務、刪除資料或啟動正式全歷史推論。

### 修正與實際運轉

- 原 all-service audit 只有一次 repo-context 程序清單，寫入計量主要來自 systemd cgroup，因此會漏掉獨立執行的修復工作。現在在既有取樣區間前後收集 PID／starttime／unit 與 `/proc/PID/io`，以啟動時間防 PID 重用；缺少權限、counter rollback、unit context 改變、時鐘／host epoch 無法證明均保留 null 與原因。出現／消失只代表進出可觀察範圍，不冒稱確定啟動／退出。
- 與既有每五分鐘的 runtime tracker 共用相同實作；一般每 30 秒狀態發布在未到期時不讀 `/proc`，不新增 polling daemon 或不受控歷史。單一 baseline 仍限 4 MiB，歷史交由既有 journald rotation 與有 checksum 的分段事件。程序權限或 optional baseline clock 異常不會抹掉 64 個 unit 的監測或阻止公開狀態發布。
- Hardened status worker 的 `ProcSubset=pid` 會隱藏 boot ID：只有真實 32-hex systemd invocation UUID／MainPID／restart count 及 active 狀態相符才可作 host epoch witness。沒有見證即不扣除 unmanaged process 或 filesystem counter；磁碟容量變更也不當成寫入成長。PID I/O 包含 waited-for children，**不可把 parent／child／cgroup 相加或當成容量淨增長**，依 [Linux man-pages 的 counter 定義](https://man7.org/linux/man-pages/man5/proc_pid_io.5.html)。
- 對 `/proc` 的可見性亦保留 failure counts，未增加 ptrace／公開服務權限。06:44:30 的 root CLI 觀察到 57 個 repo-context 程序（40 managed／17 unmanaged candidate），兩次 capture **29.027／39.011 ms**；06:44:55 的既有 hardened worker 只觀察到 19 個（18／1），capture **22.514 ms**，119 個 cwd read failures、17 個 matched process I/O unavailable。這是當次 metadata capture 耗時，不是全站延遲或全主機工作覆蓋率。
- 既有排程已自然載入新程式：06:34、06:39 的兩份五分鐘事件已由 journal chunk 完整重組並校驗 SHA256；06:39 以 Discord invocation 作 epoch anchor，程序間隔 **303.407 秒**。後續 06:44 baseline 已包含最終 capture timing／visibility。這證明監測執行，不證明任何 source／成交成功。

### 容量與尚未解決風險

本輪系統盤 available 從 06:13 的 **30,270,435,328 bytes** 降至最後核對的 **11,543,511,040 bytes**（約 11.5 GB；`df` rounded 100%）。D 槽仍 **2,529,952,428,032 bytes**，guarded cold bind `--check` 通過；D 是本機冷主副本，不冒稱獨立備份。

20.984 秒 read-only sample 的 volume available **-291,250,176 bytes**；主要 observed writers 是 unmanaged Python **291,012,608 bytes** 及其 bash parent **339,058,688 bytes**，parent 開啟 `artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/rebuild_v36.log`。這是另一項期貨修復工作的重要線索，**不是把 parent＋child 加總、不是證明全部容量下降都由它造成**。06:39 既有容量監測另外記錄五分鐘 **-4,857,126,912 bytes**。

06:44:47 重新執行 canonical `stockagent-data gc --dry-run`：只核對目前四個 managed registrations，全部 pinned，would_evict／would_renew／實際 evicted 均 **0**。沒有 apply／force、解除 pin、刪除 minute source／模型／帳本／cold object 或清掉使用中 caches。不能用這四項的結果聲稱全部 node-local derived caches 已驗收；亦不能把 D 槽容量當成搬移授權。已向使用者請求協調另一項修復工作之暫停／D 槽寫入路徑，本輪不擅自終止它；安全容量確定前不追加大型驗證／回補。

### 回歸與服務驗收邊界

[六模組最終回歸](../artifacts/benchmarks/service-process-io-regression-final-20261001.log) **666 passed／28.62 秒**；包含 PID reuse、same-capture race、hidden boot ID／有效 anchor、null counters、partial visibility、counter／clock rollback、巨大 optional clock、volume resize、journal framing、snapshot publication 與 public dashboard／data inventory／storage-pressure 整合。先前 414／662 項試跑是重疊集合，不加總。Ruff、py_compile、diff whitespace 通過；不是全庫、正式全歷史、Windows 冷啟動或公網 browser 驗收。

[06:44:30 最終現場 audit](../artifacts/benchmarks/service-coverage-20261001-process-io-final.json)仍 **64 services／49 timers／3 paths**，timer findings 0；六個 localhost GET 全 200，但 TAIFEX **blocked**、當沖 **degraded**、隔日沖 **waiting**、Shioaji **waiting**、OpenBB **stopped**、全資料監控 **degraded**。Crypto training refresh deferred raw writer、registered features 的 OKX step failed、TW cold publication failed 均保留，不以 HTTP 200 消除。

OpenBB L1 最新 matched invocation `8f0f3bb24c984d7ab799d077517eb964` completed，但 derivative footer scan **132.911 秒**、memory-high delta **4,941**、OOM／kill 0；約 2.55 GB file cache／0.116 GB anon 的 stage sample 顯示主要是讀取 cache 壓力，不能誤稱 Python heap 洩漏或宣稱已解決。正式 Discord history attempt 6 仍 failed，沒有解除 opening runway／backoff 或替換產物。三個 core unit PID／invocation 未變、NRestarts 0。

完整證據與後續安全邊界：[本輪交接](../artifacts/benchmarks/service-reliability-process-io-validation-20261001.json)。重測仍走既有路徑：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_service_latency_coverage.py --sample-seconds 10 --output artifacts/benchmarks/service-coverage-process-io-rerun.json
run_fintech_python -m pytest -q -s test/test_audit_service_latency_coverage.py test/test_track_service_runtime_trends.py test/test_data_monitor_inventory.py test/test_public_dashboards.py test/test_data_monitor_dashboard.py test/test_storage_pressure.py
stockagent-data gc --dry-run
```

## 2026-10-01 06:00 起：共享實體分鐘快取容量／解碼修復，目標仍 active

依共用訓練及使用者工作偏好流程，修正相同問題的 dense／packed／compact 三條來源路徑，沒有新增下載器、重啟核心服務、替換模型／帳本或改財務公式。這是本機程式碼與小型端到端驗證，**不是正式全歷史或全服務 ready**。

### 根因、修正與監測

- 舊程式只有 dense 依整份資料估算值決定是否保留；packed／compact 的 dictionary 無條件保留，包含明確設定 `STOCKAGENT_DAY_TRADE_SOURCE_CACHE_GIB=0` 時也保留。三種表示各自解碼同一份 NPZ，還可能保留重複 mark plane。這是原始碼與反例證明的風險，不冒稱它造成 attempt 6 的失敗；attempt 6 的已證明根因仍是上一節的 checkpoint 投影遺失來源。
- 改為來源模組內一個 byte-bounded LRU，三種表示共用 budget；按實際 CPU tensor backing-storage identity／bytes 計算，同一 storage／view 不重複計入。dense／compact 從同一 packed loader 取得資料，不再各設無界 dictionary。相同 key 的併發 miss 共用結果或原始錯誤，失敗可重試；不同日期的 I/O 不在全域鎖內，保留平行解碼。淘汰只放掉 cache 引用，已交給 batch 的 tensor／views 與 immutable source 檔均保留。
- 延用既有 GIB 設定；零值現在確實關閉所有表示的保留，拒絕 NaN／infinity／負值。`auto` 在原 host reserve／world-size 分配前，亦讀取可取得的 cgroup v2 ancestor `memory.high`／`memory.max` 減 `memory.current` 餘裕；沒有提高 systemd 限制。實際 formal owner 的 high **48 GiB**、max **64 GiB**，不能把整機 MemAvailable 當該 worker 的可用容量。Linux 文件區分 [high 的節流與 max 的 OOM 邊界](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html#memory-interface-files)；storage 計數依 [PyTorch storage API](https://docs.pytorch.org/docs/2.14/storage.html)，不只計算 view 的 logical cells。
- 每個表示保留 hits／loads／coalesced／errors／evictions／bypasses、load／wait 秒數及 resident／peak tensor bytes。prepared source 的獨立 runtime callback 跨 universe projection 共用，不混入 immutable audit、release ID、optimizer／checkpoint fingerprints。所有 canonical fold writer 呼叫端接上原本的 atomic JSON writer，輸出 `physical_source_cache.json`；標為 **rank-local shared-source lifetime，fold reporting 前 snapshot**，不是某一 fold／epoch 的獨占量測。`load_s` 含 nested load，不可相加假裝總 wall time；snapshot 附時間。
- budget 限制的是 **cache 保留的 tensor storages**，不含完整 source arrays、Python metadata、in-flight decode、consumer-held／projected batch、GPU workspace。啟動時 headroom 不是日後可用 RAM 的保證；cgroup v1／無法讀取的 limits 不在新 v2 headroom 證據內。因此未宣稱整個 worker 不會 OOM，亦未宣稱全域 replay 已做 bounded streaming。

### 驗證與可重測結果

[最終回歸](../artifacts/benchmarks/day-trade-source-cache-regression-final-20261001.log)：**496 passed、3 skipped／49.56 秒**，共 14 個選定模組；包括 source builder 三種 budget、storage/view byte 計數、跨格式 LRU、consumer 引用、同 key／nested 表示併發共用與錯誤後恢復、獨立日期平行、v2 ancestor reserve／DDP 分配，以及原 checkpoint／FIFO／270 點／fees／artifact／stitched deployment／訓練 lifecycle 回歸。實際小型 neural inference 仍產生全部要求的 NPZ、audit、報表／圖與新 cache diagnostic，checkpoint SHA 不變。Ruff、py_compile、`git diff --check` 通過；不是全庫測試、正式行情或冷開機驗收。

[最終 ABBA receipt](../artifacts/benchmarks/day-trade-source-cache-abba-accepted-20261001.json)及[原始 log](../artifacts/benchmarks/day-trade-source-cache-abba-accepted-20261001.log)固定 Git baseline `862fc3e08b156a411d1b64f54e925faed44247cc`，每 budget／variant 4 samples、每 sample 2 次 ordered access；資料為 receipt-backed **synthetic 2 sessions／3 symbols**，CPU eager。各表示完整 tensor／session digest、全部 FIFO state 與 **270 點／session**完全一致，panel input fingerprint、原始來源與 session NPZ hashes 不變。

| budget | 舊／新保留 tensor bytes | minute NPZ 讀取次數（兩次存取） | 舊／新首輪 accessor 合計 | 舊／新兩輪 accessor 合計 |
| --- | --- | --- | --- | --- |
| 1 GiB，足夠放整個小型資料 | 93,888／66,720 | 3／1 | 15.038／12.923 ms | 24.103／24.491 ms |
| 約 10 KiB，byte 上限 10,737 | 28,464／7,488 | 4／2 | 19.695／11.538 ms | 34.662／22.500 ms |
| 0，關閉保留 | 28,464／0 | 4／6 | 13.300／9.023 ms | 18.358／25.987 ms |

容量足夠時確認少一次以上解碼、少保留 **28.94%** 的 storage；但是最終這次兩輪合計沒有加速（約 +1.6%），不能挑先前較快試跑當結論。零容量的舊版違反關閉設定，新版確實少保留，但重複讀取變慢是誠實的取捨。獨立記錄完整兩-session FIFO replay（含取得 dense source），1 GiB 的 median **46.964／47.642 ms**，也沒有證明整段 workflow 更快。輸入建置、digest 驗證、replay 各自計時；accessor samples **不含 imports、source preparation、parity digest、模型、report／plots、CUDA 或 cold filesystem**。CLI 整個 process peak RSS **965,070,848 bytes**含 imports，不是各 variant 或正式全市場的 RSS 比較。前輪 1,036.813 秒真正來源建置量測未被改寫。

最後另固定測速器的預設 baseline commit，避免未來 commit 後 `HEAD` 自己變成新版、讓測速器回歸錯誤；[這項 CLI 修正的測試](../artifacts/benchmarks/day-trade-source-cache-benchmark-pinned-default-20261001.log) **5 passed／8.81 秒**，與上述 496 個測試有重疊，不加總成額外 coverage。[固定預設後的同指令重測](../artifacts/benchmarks/day-trade-source-cache-abba-pinned-20261001.json)仍有相同解碼／bytes／exact parity，但 1 GiB 兩輪 median 變成 **23.110／20.810 ms**、完整 FIFO **31.177／29.719 ms**；相鄰量測的 wall time 有明顯變動，因此沒有選其中一次宣稱穩態或正式全市場延遲改善。最新測速程式的 code hash 見這份重測，不以舊 receipt 的 CLI hash 冒稱最新版。

```bash
source scripts/runtime_env.sh
run_fintech_python test/benchmark_day_trade_source_cache.py --baseline-commit 862fc3e08b156a411d1b64f54e925faed44247cc --output artifacts/benchmarks/day-trade-source-cache-rerun.json
```

### 服務／容量與後續優先序

[05:53:15 Taipei 的服務觀測](../artifacts/benchmarks/service-coverage-20261001T0600-source-cache.json)仍 **64 services／49 timers／3 paths**；六個 localhost GET 200。TAIFEX **blocked**、當沖 **degraded**、隔日沖 **waiting**、Shioaji **waiting**、OpenBB **stopped**、全資料監控 **degraded**，不把 HTTP／systemd active 當成交證據或資料完整。Discord／gateway／當沖仍原 PID **898／606482／715544**、原 invocation、NRestarts 0，無核心 restart。新 source cache 程式需由下一個讀入新程式的 worker 執行，未宣稱現有常駐進程已熱載入。

timer findings 0；**resource findings 不為 0**。新獨立 OpenBB L1 invocation `cabf292c76404aff882d571dce89edd4` 在 05:39～05:41 completed、new segments 0，但 memory-high delta **4,459**、OOM／kill 0；matched step 的 stale-contract audit **123.771 秒**、whole unit **163.581 秒**。root 沒有手動觸發該輪、修改該 compactor 或把節流消成 green。

正式 job attempt 6 仍 `failed`、completed **04:17:44**、next retry **10:17:44**；沒有消除錯誤／重設 backoff，也沒有覆寫 fold10 archive。123 分鐘 runway／06:17～13:40 admission fence 保持。**06:00:16** filesystem available **35,774,357,504 bytes**（約 35.8 GB、99% 使用），較 05:29 的 44.9 GB 繼續下降，有其他活動，不憑差額判定 writer。OpenBB 仍低於原 100 GiB floor 保護停止；沒有降低 floor、實際 GC、刪除來源／checkpoints 或啟動正式全歷史 rebuild。

下一輪優先釐清持續磁碟成長與安全容量，再處理 L1 已量測的重掃／memory-high，以及正確來源的正式歷史驗收與跨 universe 全域 replay peak。全服務資料健康、公網 IPv6／browser、Windows／WSL cold boot、完整 repository suite 仍未全部驗收。此輪有實作、496 項 regression 與可重測 evidence，屬於實際進展，**目標保持 active，不標完成或 blocked**。

**06:09:16** 再查 filesystem available 為 **30,763,302,912 bytes**；持續下降，後續不能只把剛更新的 source cache 當成容量問題已解。完整 code／runtime／safety 與剩餘項目見[本輪交接](../artifacts/benchmarks/service-reliability-source-cache-validation-20261001.json)。

## 2026-10-01 05:03 起：正式推論來源遺失根因修復，目標仍 active

本輪使用共用訓練及使用者工作偏好流程，先確認實際來源／checkpoint／產物範圍，再修共享實作；沒有以放寬 FIFO 驗證消除錯誤。05:03:41 的[完整服務觀測](../artifacts/benchmarks/service-coverage-20261001T0510-fifo-inference.json)仍是 **64 services／49 timers／3 paths**。Discord、公開 gateway、當沖引擎的 PID／invocation 與前輪相同、NRestarts 0，沒有重啟、送出訂單、換模型或改紙上帳本。

### 根因與實作

- **更正前輪「legacy archive 相容性」的定位。** [20 份 NPZ 成員清冊](../artifacts/benchmarks/day-trade-inference-archive-census-20261001.json)證明 fold01～09 的 test／deployment 都是 schema 8、currency、270 點曲線與 FIFO 狀態；只有 attempt 6 新生成的 fold10 兩份是 schema 7、nav_ratio、沒有 minute_nav／carry fields。不是把舊檔強改標籤可以解決。原共用 `_subset_panel_symbols` 在 2,757→2,754 checkpoint 對齊時，漏帶 `day_trade_carry_source` 與 executor minute tape；原始 panel 保留物理來源、aligned panel 卻掉回舊帳務路徑，最後嚴格 stitched reader 正確拒絕。
- **共用標的投影不再遺失執行證據。** 為 prepared source 新增 lazy universe projection；投影前核對完整 ordered universe／calendar，保持 release ID 與源收據。dense／CSR／packed transport 各保留正確軸、整數 event identities 與 chronological minute tape，packed 投影不先製造 dense exit cells。缺少已驗證實體來源的 checkpoint-only 標的仍明確失敗，不用 masked feature 假裝有價格，也不回退舊帳務。原 source 不被改寫。
- **移除兩份重複推論報表流程。** neural／tree inference 改走既有 `_save_fold_output_artifacts`，重新接上 test symbol sidecar、settlement audit、schema/context 驗證、`save_timing.json`／`plot_timing.json`／`mode_artifact_contract.json`。推論不重寫 model／checkpoint。Taiwan surrogate／integer audit 的 metrics 仍區分；沒有移除圖、縮短日期或少算分鐘。
- **只移除已證明重複的完整前綴重播。** 完整 owned interval 有相同 split rows、來源最後日與真實 FIFO endpoint 時，複製已記錄結果與狀態；不再重讀每一天與重跑 270 opportunities。部分前綴或具 initial state 的續接仍走 canonical replay。合法 checkpoint 子集合／重排是 fresh deployment cache miss，轉標準全域 replay；壞或重複 sidecar 不會被吞成成功。

### 回歸、測速與邊界

[11 個模組的最終回歸](../artifacts/benchmarks/day-trade-inference-contract-regression-final-20261001.log) **464 passed、3 skipped／35.37 秒**；另[測速器回歸](../artifacts/benchmarks/day-trade-full-prefix-benchmark-regression-20261001.log) **5 passed／7.40 秒**，不重複加總之前試跑。新增反例包含來源／minute tape 遺失、calendar／universe 不符、缺少實體證據、zero eager decode、dense／CSR／packed exact projection、integer event dtype、full endpoint 不重算，以及真實小型 neural infer→schema 8→270 點曲線→報表／全部圖→checkpoint subset 擴展→同一 stitched account。source/context、checkpoint SHA 與新標的零 request／零 holdings 都核對；測試資料明確是 synthetic，不是官方交易證據。[端到端 log](../artifacts/benchmarks/day-trade-inference-e2e-20261001.log)。Ruff、py_compile、diff whitespace 另驗證；不是全庫、正式全部歷史或 Windows 冷啟動驗收。

[同一已評估物理帳戶、Git baseline 的 ABBA 測速](../artifacts/benchmarks/day-trade-full-prefix-abba-final-20261001.json)，每 variant 4 samples、10 個 synthetic sessions／2 symbols／270 點，每個財務陣列、canonical compact FIFO state、完整 input fingerprint 均相符；完整前綴處理 median **152.753→0.385 ms**。這是 CPU eager synthetic prefix-only 比較；**不包含 imports、source preparation、模型、NPZ／報表／圖、全域 stitched replay、provider、cold filesystem 或 CUDA**，不能拿它取代正式 inference 或 09:00-to-signal 延遲。完整來源第一次建置前輪仍 1,036.813 秒，這輪沒有改寫該量測。

### 現場與下一輪

正式 attempt 6 仍 `failed`，completed 04:17:44、next retry **10:17:44**；沒有手動清掉錯誤／6h backoff，也沒有把新測試標成正式資料 ready。既有 123 分鐘 runway／08:20 fence／13:40 保護保持不變；修正後正式歷史仍需在安全容量及排程條件下走隔離驗收。原 fold10 schema 7 檔未覆寫或刪除。filesystem available 從本輪較早的 **50,441,269,248 bytes**，到 **05:26:18 的 44,935,598,080 bytes**（約 44.9 GB），有其他 ongoing work，不能單憑差額歸因。OpenBB 仍低於 100 GiB floor 保護停止；沒有 GC/apply、刪除來源或降低 floor。

05:03 的六個 localhost public projections 都 200；TAIFEX **blocked**、當沖 **degraded**、隔日沖 **waiting**、Shioaji **waiting**、OpenBB **stopped**、全資料監控 **degraded**。這與 TCP／HTTP 可達分開。timer findings 0。獨立 OpenBB L1 matched invocation `7af189d767024961acf6dfe3da41399d` 的 memory-high delta **1,713**、OOM／kill 0，沒有因此聲稱記憶體無壓力。

下一輪需處理安全容量、正式最新完整來源／產物驗收，以及 physical source dense／packed／compact session cache 的目前無界保留與跨 universe replay 成本。後者是程式碼風險，這輪尚未量測修正後正式 peak RSS；不能先宣稱更快或更省記憶體。全服務資料健康、全庫測試、公網 IPv6／browser 及 Windows／WSL 冷開機仍未全部驗收。完整證據見[本輪交接](../artifacts/benchmarks/service-reliability-fifo-inference-validation-20261001.json)。

可重測（明確 synthetic microbenchmark，不碰正式產物或訂單）：

```bash
source scripts/runtime_env.sh
run_fintech_python test/benchmark_day_trade_full_prefix.py --baseline-commit 862fc3e08b156a411d1b64f54e925faed44247cc --output artifacts/benchmarks/day-trade-full-prefix-rerun.json
run_fintech_python -m pytest -q -s test/test_day_trade_inference_source.py test/test_benchmark_day_trade_full_prefix.py
```

## 2026-10-01 03:58：輕量盤後重試已部署，全服務目標仍 active

本輪依使用者工作偏好，以完整服務清冊及真實收據為準，不把程序存活當資料健康。[新清單](../artifacts/benchmarks/service-coverage-20261001T0400-discord-retry.json)實際觀測 **03:58:00、64 services／49 timers／3 paths**；增加的只有 Discord artifact retry controller 與 timer。沒有重啟 Discord Gateway、公開 gateway 或當沖引擎，也沒有改模型、真實訂單或帳本。完整交接見[本輪收據](../artifacts/benchmarks/service-reliability-retry-validation-20261001.json)。

### 實作與實際驗證

- **補上 retry deadline 到固定排程之間的空窗。** 第 5 次失敗的 `next_retry_at=02:08:36` 原本僅代表最早允許時間，不會自行啟動 worker。新 controller 每分鐘只查最新／啟用／已排程工作的收據；由 systemd 喚醒原本 `stockagent-discord-artifact-maintenance.service`，沿用既有模型、來源驗收、backoff、2h budget 與 kernel lock，不複製推論或資料下載邏輯。週末不輪詢；現場 owner `TimeoutStartSec=2h`、stop grace 90 秒，所以最後版派送前再次核對 **123 分鐘 runway**，06:17～13:40 不派送。沒有只用 start budget 擠到 08:20 fence；原 owner budget 沒有改動。
- **沒有工作就不載入模型。** 把 Discord／Torch import 移到真正 worker 路徑。只接受有時區 deadline；缺檔不建立空成功收據，壞 JSON／過大內容明確失敗。`running` 必須配合實際 kernel lock 判讀，不拿過期 PID 或時間戳當 owner。
- **來源未變時合併重複喚醒。** 相同 due jobs 與 canonical close receipt/config event hints，300 秒內不重複派送；新 source event、job attempt 或 config 可立即喚醒。stat 只是排程提示，不是資料驗收。獨立、原子寫入的 `/var/lib/stockagent-discord-artifact-retry/latest.json` 僅證明 systemd 接受派送，不能證明產物成功；未接受／狀態寫入失敗分別保留。時鐘倒退不會無限 suppress。控制器 `ProtectSystem=strict`、僅 AF_UNIX，只有自己的 StateDirectory 可寫。
- **同份真實 snapshot、fresh-process ABBA。** [最新碼可重測收據](../artifacts/benchmarks/discord-artifact-retry-probe-accepted-final-20261001.json)每 variant 4 samples、決策一致、原 receipt bytes 不變、沒有 dispatch／inference。whole process wall median **8.756124→0.187056 秒**；worker peak RSS **770,125,824→24,346,624 bytes**。baseline 包含原本 eager Discord／Torch imports；輕量 probe 沒有 Torch。這次背景 worker 正在推論，不能跨輪對比：[較早一輪](../artifacts/benchmarks/discord-artifact-retry-probe-abba-20261001.json)是 3.840153→0.100596 秒。本測速不是 full-history、provider、公網、p95 或 cold filesystem speed；OS cache 未控制。只比較最新內部 `probe_seconds` 則沒有加速（66.212→90.508ms），真正省掉的是不必要的 import。
- **自然 timer 已接通，但正式歷史第 6 次仍失敗。** 03:58:00 controller 無 Torch、probe 84.126ms、dispatch persisted；原 worker invocation `ffe5a5e962894aefb80c613ef044a10c`，03:58:06 最新 v8 job 進入 attempt 6，target 9/30。03:59 下一輪 kernel-lock fast path 76.226µs，`requested=false`，沒有第二個模型 worker。資料建置 1,036.813 秒後進入 CUDA 推論；04:17:40 stitched deployment 讀取 backtest archive 時拒絕 **`physical FIFO source context/state cannot use a legacy archive schema`**，04:17:47 unit `failed/exit 1`、MainPID 0。canonical job 保存 attempt 6／failed、worker degraded、next retry **10:17:44**（6h backoff）；04:25 timer 正確 `no_due_retry`、無 Torch、沒有再派送。錯誤在 archive 相容性，沒有把它放寬或改寫為 ready；10:17 位於保護區，最早仍需 13:40 的正常 gates。這不代表 Discord Gateway／當沖引擎中斷。[runtime log](../artifacts/benchmarks/discord-artifact-retry-runtime-20261001.log)。

本輪六個 canonical test 模組 **247 passed／6.83 秒**，包含 malformed/future/obsolete/disabled jobs、orphan recovery、kernel lock、時區邊界、大小寫一致的 market scope、runway/stop-grace recheck、重複派送合併、來源變動、時鐘倒退、dispatch／持久化失敗、CLI 無 Torch，以及 benchmark parity／budget／market-window guards。[最終 log](../artifacts/benchmarks/discord-artifact-retry-regression-runway-final-20261001.log)；沒有重複加總前幾輪，也不是全庫或冷開機驗收。Ruff、py_compile、shell syntax、systemd unit verify 與 calendar parse 均另行檢查。

### 容量與仍未解決的項目

完成低優先度 read-only [workspace](../artifacts/benchmarks/workspace-disk-inventory-20261001T0300.tsv)及 [artifacts](../artifacts/benchmarks/artifacts-disk-inventory-20261001T0330.tsv)清冊：不同時間樣本的 artifacts 約 948.5／955.2 GB；這不是精確增長量。markets 約 477.9 GB、replays 139.8 GB、live 117.6 GB、cache 62.2 GB。名稱、大小或 mtime 不能當刪除授權；正式來源、receipts、pins、leases 與可恢復契約保持不變。canonical compiler-cache [dry-run](../artifacts/benchmarks/storage-pressure-20261001/storage-pressure-audit-20260930T185728.049011Z.json) **eligible 0／selected 0／deleted 0**，未 force／apply。沒有取消 pins、降低 OpenBB 100 GiB floor 或刪正式資料。

03:58 的六個 localhost GET 200，產品仍是 TAIFEX blocked、當沖 degraded、隔日沖 waiting、Shioaji degraded、OpenBB stopped、全資料監控 degraded。timer findings 0；獨立 OpenBB L1 compaction matched invocation `5d2ddc3003434f2da3cbaff098a3811b` 有 memory-high delta 1,958、OOM/kill 0，不能改寫成整個系統無壓力。04:01 live filesystem available **88,636,489,728 bytes**，OpenBB 仍保護性停止；沒有只從磁碟變化歸因於本輪 worker。

待辦仍包括安全容量處理、OpenBB L1／全檔驗收、TW source/derived/cold publication、Discord legacy archive 相容性修復與正式歷史驗收、TAIFEX/Shioaji/隔日沖資料健康，以及全庫測試、公網 IPv6/browser 和 Windows/WSL 冷開機。目標保持 active，不宣稱所有服務正常或優化到極限。

可重測：

```bash
source scripts/runtime_env.sh
# 若需與正式 worker 相同的 market scope，先載入既有 .env；不要輸出其內容。
run_fintech_python scripts/benchmark_discord_artifact_retry_probe.py --output artifacts/benchmarks/discord-artifact-retry-probe-rerun.json
run_fintech_python scripts/audit_service_latency_coverage.py --sample-seconds 1 --output artifacts/benchmarks/service-coverage-rerun.json
```

## 2026-10-01：目標恢復 active、OpenBB 有界修復與真實測速

**目標已恢復 `active`**：全服務延遲／吞吐／資源效率，以及健壯性、穩定性；不是全部完成。依使用者偏好先盤點、維持服務與來源契約，再逐項量測／修正／驗收。[02:28:49 現場清單](../artifacts/benchmarks/service-coverage-20261001T0230-openbb-recovery.json)是 **63 services／48 timers／3 paths**，沒有把歷次清單相加；本節不把局部測試稱為全庫驗收。完整變更、失敗與驗收邊界見[本輪交接收據](../artifacts/benchmarks/service-reliability-validation-20261001.json)。

### 實作與現場證據

1. **消除每批近 900 萬筆的重掃。** 新增目錄子工作修復初版把 `active/plan_token` 和 `task_id IN` 放在同一 SQL；現場 SQLite 選了 scheduling covering index，每批掃大量 active tasks。已安全中止本輪自己啟動的 `--plan-only` 工作（PID 884286，精確 stdout log 核對，exit 130），保留 log 與已提交進度；改成主鍵查詢，再在 Python 套用完全相同的 active／plan 條件，不新增索引或修改正式 DB 的 durability。[同 snapshot 的真實 512 筆 ABBA](../artifacts/benchmarks/openbb-catalog-membership-abba-20261001.json)每 variant 2 samples，median **1.806468 秒→0.004512 秒**、結果相同、query-only、連線 total_changes 0。這只證明該查詢，不是整個 archive／公網／p95 的速度。
2. **接回缺漏的 catalog children，保持原資料。** 使用 canonical `discover_followup_tasks` 和 Manifest upsert，驗證本機成功 parent 的 footer／列數／metadata 後才建立缺漏工作；不重抓 parents、不重置 active accepted children，也不延伸固定 archive end date **2026-07-18**。coverage 的合法 `deferred` 動態端點現在與 `included` 一起保留；excluded／unavailable／not_enumerable 仍不接受。現場 SDK 是帶點名稱；新增無點相容查詢只是防禦，**不是現場缺漏成因**。
3. **成功修復與獨立驗收分開。** [修復收據](../data_openBB/_state/catalog_followup_repair_latest.json)version 2，**13,148 parents、1,133,281 per-parent children、743 restored、140.929 秒**，固定 plan `56e52165c96966261d74547b`。第二次同一 canonical `--plan-only` 成功退出，版本 marker 避免再做整輪修復，accepted child 不變。[獨立 canonical read-only followup audit](../artifacts/benchmarks/openbb-catalog-followup-audit-20261001.json)重建 **1,126,953 unique required scopes**，missing **743→0**、parent read failure 0、32.390 秒。兩種 child 數字的 grain 不同：前者逐 parent，後者跨 parents 去重。這些證據是「工作存在」，**不是新增 743 份資料已下載或 archive 全完成**。
4. **ownership audit 不再保留全量 Path set。** 在資料 filesystem 的私有 SQLite scratch index 串流 512 筆，每檔都掃，最多保留 50 個問題樣本；不放在 tmpfs、不改正式 Manifest 的同步設定。Manifest inactive-success 檔另列 retained；unknown／pending／failed／foreign-active outputs 仍拒絕，所有 active-success 的原 footer／wrapper／metadata 驗證保留。原 147,186 個被列 non-success 的檔，只查核了 50／50 樣本為 inactive-success；**不能推論全數都已合格**。retained 是所有權分類，不是重新驗證 inactive 檔內容。
5. **實際 source copies 的 ownership benchmark。** [100,000 真實檔案 ABBA](../artifacts/benchmarks/openbb-ownership-abba-accepted-20261001.json)使用複製檔，不是 hardlinks；來源 stat identity 與 SHA 前後相同。worker VmHWM median **182.25→91.77 MB（−49.64%）**。wall median 9.708→8.935 秒，但 samples 重疊且波動，**不宣稱穩定 latency／全服務加速或原 47.56 GB peak 已根除**。沒有裁掉來源檔案／分鐘或拿熱快取冒充來源重建。scratch index 本身也需要磁碟空間。
6. **冷發布明確指出阻擋項，但不放寬 gate。** `publish_tw_public_cold_release.py` 保留 exit 75，新增 `blocking_checks`／原始 findings 到失敗或 deferred 收據。現場 canonical audit 的失敗是 official symbols `source_receipts/lifecycle_source_receipts`、features `source_bytes`：官方 symbols build 到 9/30，整體 download summary 仍到 9/29，TPEx company 與多份公開來源在 derived build 後已變動。這是來源／衍生層一致性問題，不能只改日期或網站狀態；本輪沒有發布新的 cold release 或重寫 runtime pin。新診斷接線尚未經下一個自然 publish run 驗收。

### 反例、故障與回歸

保留首輪修復回傳 0 但只認 `included` 的失敗驗收（13,148 parents／1,062 expected／0 restored）；不能把它算成功。也保留後續錯誤 SQL 工作的 interrupt log。初次 ownership benchmark fixture 的 import 衝突，以及新增 lookup benchmark 原子 writer 參數順序錯誤（1 failed／1 passed），均在正式現場 benchmark 前修正，沒有把失敗 sample 混入 accepted timings。

本輪 canonical **10 模組 457 passed／8.90 秒**，涵蓋既有 downloader／contracts／supervisor／watchdog／dashboard、ownership 全 batches、外計畫與 pending 拒絕、read-only、scratch connect／insert 空間不足清理、成功資料不變、deferred scope、壞 parent 不標完成，以及首批已 durable 後中斷再續跑。Ruff、py_compile、scoped diff check 通過。這不是全 repository 或冷開機驗收；沒有加總各輪重複測試。[最終 log](../artifacts/benchmarks/service-reliability-regression-final-faults-20261001.log)。

可重測入口：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_openbb_catalog_lookup.py --output artifacts/benchmarks/openbb-catalog-membership-rerun.json
run_fintech_python scripts/benchmark_openbb_file_audit.py --sample-files 100000 --output artifacts/benchmarks/openbb-ownership-rerun.json
run_fintech_python scripts/audit_service_latency_coverage.py --sample-seconds 1 --output artifacts/benchmarks/service-coverage-rerun.json
```

### 正式服務與未解風險

已於 **02:22:47**沿原 systemd unit 恢復 OpenBB archive：invocation `c57d2961521e4140a79ee26afbb52569`、原 PID 923909、NRestarts 0，原 supervisor lock、100 GiB 磁碟 floor、開盤 resource gate、provider policy 全保留。02:41:28 的 canonical monitor 記錄 success **7,319,562→7,320,556**，不是 full acceptance，也不能由總數判定每個新 child 都完成。隨後 **02:44:36** 因 free **106,900,807,680 bytes < 107,374,182,400 bytes floor**，supervisor 保護性停止，unit `failed/exit 2`、MainPID 0；**不再是 running**。新 full source／catalog／compaction acceptance 仍未完成，沒有放寬 floor、reset-failed 或刪正式資料。

[02:48:09 停止後清單](../artifacts/benchmarks/service-coverage-20261001T0250-disk-guard.json)仍 63／48／3，六個 localhost GET 200，OpenBB 回到 **stopped**，其餘產品健康仍與下列 02:28 樣本相同；timer／resource findings 此樣本皆 0，不抹掉先前 matched compaction 的壓力證據。已執行 canonical [GC dry-run](../artifacts/benchmarks/storage-gc-dry-run-20261001.log)，**would_evict 0／would_renew 0**，既有重要資料被 pin 保護；沒有實際 GC。資料盤空間不足仍待安全儲存盤點或容量處理，不能用取消 pin／降低 floor 冒充已修好。

Discord／gateway／當沖仍是 PID **898／606482／715544**及原 invocation、NRestarts 0；本輪沒有重啟它們，沒有改真實訂單、模型／checkpoint 或當沖帳本，也沒有碰其他 actor 的 futures margin dirty work。

02:28:49 六個 localhost GET 都 200，但 TAIFEX **blocked**、當沖 **degraded**、隔日沖 **waiting**、Shioaji **degraded**、OpenBB **active**、data-monitor **degraded**。當沖單次 GET 3.505ms 只含 localhost projection／傳輸，不是開盤訊號、browser paint、公網或 uncached source latency。

timer findings 為 0；**resource findings 不為 0**：獨立 L1 compaction 02:01:24～02:03:10 雖完成、沒有新增 segments，該 matched run 記錄 memory-high counter 增加 2,019，OOM／kill 0，stale derivative metadata scan 82.745 秒。這是該次收據，不等於此刻仍有記憶體事故；root 本輪沒有觸發或重啟該 compaction，也沒有宣稱已修復這項瓶頸。

待辦維持可分別驗收：OpenBB 新下載及全檔 audit／L1 pressure；registered OKX 原 transport 失敗的自然輪；crypto-training source lease budget；TW 正確來源完成後 derived 重建與 cold publish；Discord formal-history 第 5 次失敗仍保留，02:08:36 是最早允許 retry，不是保證 worker 自動啟動，夜間觸發空窗仍需處理；TAIFEX／Shioaji／隔日沖實際資料健康；公網 IPv6／瀏覽器、Windows／WSL 整機冷啟動與全庫測試。不能因本節局部進展宣告全服務都正常或已到極限。

## 2026-09-30：來源恢復、分鐘曲線驗收與有界可靠性修正

本輪是全服務目標的局部進度，**不是全系統完成或全部服務加速到極限**。9/30 當輪結束的目標紀錄是 `blocked`，不是 `complete`；10/1 已依使用者要求恢復 `active`，見上節。本輪仍完成下列可安全驗證的工程修復，未改投資模型／checkpoint、真實券商訂單、全域 rate limit 或服務資源上限。完整命令、來源 SHA、失敗與驗收邊界見[驗證收據](../artifacts/benchmarks/service-reliability-validation-20260930.json)。

### 已修復與現場驗收

1. **還原被清掉的指定分鐘來源並保護部署。** 透過 canonical `stockagent-data use … --verify` 從既有 cold objects 還原指定 `tw-minute-train-20260910…09bedd96`，full verification 成功：3,189 檔、13,817,707,306 logical bytes；另外加入部署 pin，防止間歇消費者沒有持有程序 lease 時再次被自動回收。來源分割日期到 9/10，不能據此宣稱 9/30 價格或模型完整。
2. **修復模擬進場重試的事件種類。** 兩種 canonical paper entry 路徑，首次成交仍標 `entry`，追加成交改標 `entry_completion`；保持股數、價格、費用與雙帳本提交／復原契約。歷史 reader 僅在明確 paper contract、既有同一 position、嚴格 retry order ID 與無重複 order ID 下，把 36 筆舊 retry 用追加進場的數學估值。查核 203,690 筆 fills；**沒有修改原始 fills、orders、signals 或 replay receipt**。不把這些事件認成券商成交，不按券商 order ID 去重實際 fills。
3. **恢復完整分鐘曲線並消除漏日。** Reader 改從現有 canonical `replay_entry_books` 讀取檔案，但仍逐檔核對原始 receipt SHA、日期、schema／數值，拒絕外部 symlink；原 receipt 的舊絕對路徑只作 provenance。維護程式漏認 `unlimited_close_paper_settlement`，導致 9/29 完整資料被排除；現在只接受精確 assumption contract、整數剩餘數 0、正整數已清倉數的事件，仍保留所有模式端點驗證。
4. **發布前驗 scope、發布後錯誤留當輪證據。** Writer 與 promotion 共用日期／模式／270 點 scope validator；維護 owner 把精確日期與模式傳給 child，錯誤 scope 在寫正式帳本前拒絕。來源、NAV、價格來源、checksum、帳本未變與鎖檢查仍各自保留，不以 metadata gate 冒充完整驗收。Post-publication validation 若失敗會寫當輪 `failed_stage`、錯誤及耗時，不再殘留前一次錯誤。
5. **修復已下載尾段未接上歷史公司行動目錄。** Baseline 只到 9/17，但 `execution_actions` 尾段已通過 9/30 來源驗收。用既有 merger 先 dry-run、備份舊 table／receipt，再核對 12 個重疊事件與 58 份 raw response，合併 602→605 筆、coverage 2014/1/1～9/30，重疊 accounting revisions 為 0。沒有新抓 provider、編造條款或只改 receipt 日期。既有 margin-actions 排程已接上同一 merger；已有完整 catalog 時只驗證，沒有 accepted baseline／合併失敗則另留 `waiting_baseline`／`blocked`，不掩蓋錯誤也不阻斷已合格的當日 action receipt。新接線的完整自然排程尚待驗收。
6. **Crypto 資料下載修正與有界 transport 重試。** 將已測過的 shared reconciliation planner 接到 OKX／Bybit；以實際 timestamps 規劃缺頭／內部缺段／revision tail，不用 first/last 假裝連續；原 atomic merge、舊非 null feature 與 malformed fallback 保留。共用 transport 補進 truncated-body／disconnect 等可重試例外，保留既有 retry 次數、HTTP 永久錯誤與 JSON 格式失敗邊界。TW 分鐘來源 metadata 缺失先在建置大 panel 前拒絕；不把 metadata presence 當內容完整。
7. **Discord backoff 不再假完成。** 未證實 artifact current 的 retry-deferred 工作，保留其 market failure／attempt／deadline，worker 顯示 `waiting_source`；成功 reconcile 的路徑仍可 complete。沒有手動清掉 failure 或略過 backoff。

正式 minute-curves service 於 **23:22:57～23:23:07**，invocation `1fc5eb623f9c461283faf52804221a10`，以 `no_op_already_complete` 驗收 **148 交易日 × 4 模式 × 270 分鐘 = 159,840 點**，2/25～9/30，unverified interior rows 為 0。這約 10 秒是完整現有輸出的驗收，**不是完整歷史重建加速成 10 秒**。先前 child 成功寫出 148 日、舊 parent 卻只期待 147 日的失敗紀錄仍保留。

確認四模式空倉、無 pending entry 後，23:28:17 只重啟當沖服務，啟用 producer 修正：PID 132→715544、invocation `24576b9466794213b6b9157ba5ac45df`，`active/running`、`Result=success`、NRestarts 0，索引 ready／2,984.567ms；重啟後再查一次上述 159,840 點驗收仍成功，帳本執行來源 SHA 不變。這不是 Windows／WSL 整機重開測試。Discord PID 898 未變；gateway 由另一個 actor 於 21:57:33 換成 PID 606482，本輪 root 沒有重啟 gateway，不歸因於本 patch。

### 可重測效能與回歸

[OKX ABBA 收據](../artifacts/benchmarks/okx-reconcile-abba-20260930.json)每 scenario／variant 10 samples，mocked HTTP 分頁、真實私有 Parquet read／merge／atomic write，暖 OS／imports，輸出 bytes 相同；受測與 installed `_download_symbol_1m` AST SHA 相同。

| 同份 candle 工作 | HTTP calls 舊→新 | wall median 舊→新 |
| --- | --- | --- |
| 缺頭 | 28→2 | 104.791→42.395ms |
| 內部缺段 | 28→3 | 74.882→40.456ms |
| 普通尾段 control | 1→1 | 29.351→32.617ms |
| 明確 full refresh | 28→27 | 81.331→81.961ms |

尾段與 full refresh 沒有加速證據；不是 live provider／p95／公網數值。沒有為了 speed 刪分鐘、放寬驗收或把 cache hit 當來源重建。

最新 canonical **40 模組 1,141 passed／59.72 秒**，包含兩類 producer recovery、legacy long／short 完整 270 點 NAV parity、source paths、scope-before-write、post-validation failure、真實 merger 子程序、source receipts、crypto source／projection 鎖及 bounded retry。Ruff、py_compile、scoped diff check 通過。保留 pre-fix 6 個 producer／consumer 反例 fail、998-pass 中間輪，以及新增 test fixture 匯入衝突的一次 1 failed／1,140 passed；修正匯入後重新跑全部 40 模組，不隱藏前輪失敗、不重複加總，也**不宣稱全 repository 測試完成**。

### 全服務現況與未解決項目

[23:34:09 coverage](../artifacts/benchmarks/service-coverage-20260930T2340-after-reliability.json)實際觀測 63 個 installed services／48 timers／3 paths（檔名 2340 不是觀測時鐘）；timer findings 與該樣本 resource-pressure findings 為 0。六個 localhost GET 都 200，但產品仍分別是 TAIFEX blocked、當沖 degraded、隔日沖 degraded、Shioaji degraded、OpenBB stopped、data-monitor degraded，不能從網頁可連線推論交易或資料正常。

仍 failed 的 units：OpenBB archive、registered data features（OKX WLD mark-price transport 的 14:00 原失敗）。Discord 23:00 oneshot 雖正常退出，實際 worker 是 `waiting_source`／`artifact_retry_deferred`；最新 formal-history 第 5 次失敗在 22:08:36，下一允許 retry **10/1 02:08:36**。歷史 share catalog 已修復，但完整模型 panel／inference／artifact publication 尚未重新驗收，不把小型 physical-source horizon probe 成功改寫成正式產物完成。

接續優先驗收修復後的 Discord formal artifact 與 action 排程、OKX 真實失敗 window，再處理 OpenBB upstream／L1 memory pressure、TAIFEX／Shioaji／隔日沖實際資料健康；crypto source continuity、最新分鐘來源／PIT、全庫測試、公網 IPv6／瀏覽器與 Windows 冷啟動仍各需自己的 current receipt。其他 actor 的 TW futures margin preparation dirty work 未改動、未納入本輪驗收。

## 2026-09-27 06:31 起：歷史日線重建契約與來源互斥

**本輪 progress，原目標保持 active。** 沿57服務清單檢查長時間weekly backfill，沒有因journal暫無輸出就重啟。此輪修兩個production檔、新增三個測試檔；沒有手動訪provider、重啟服務、改排程／資源上限或寫正式來源、帳本、cold store。完整hash、命令與邊界見[驗證交接](../artifacts/benchmarks/daily-materializer-validation-20260927.json)。

### 已證實並修正

`materialize_ohlcv_daily --refresh` 原本只繞過mtime shortcut，之後仍讀舊daily並從最後一天−1重算。tiny actual-Parquet／真正main反例證實：早期補入、舊中段修正、來源已移除日期都可能留下錯誤投影。修前14-case版 **8 failed／6 passed**；不是宣稱抽查到的兩個現場symbol已缺頭。

現在full refresh不讀取／合併舊prefix，可從正常來源修復壞掉的derived target；若全來源沒有已完成日，仍保留舊bytes並明示skip，不把empty當刪除權限。三provider共用 `run_perp_daily_materialize`：只有source成功且daily enabled才執行，non-tail補歷史加`--refresh`，tail保持原bounded overlap。保留workers／native pools／Bybit外層source lease與失敗傳播，沒有加入第二套下載框架。

OKX／Binance原本download return後才跑projection，來源鎖已釋放。Materializer現在重用各collector的`input_dir/.download.lock`，先取得鎖才建立output與progress，完整持有至Parquet、summary、CSV、final progress。取得上限預設180秒，busy／錯誤會留下wrapper失敗證據並保留舊輸出；不是改全域rate limit。Bybit維持外層lease→內層dataset lock的順序。**互斥保證只涵蓋合作使用同鎖的writer；Yahoo writer尚無同等契約。** 多symbol仍是逐檔atomic，不是整個dataset transaction。

移除未使用的`adjclose`讀取（日線adjclose原本就由canonical close計算），selected columns七→六；filter dtype共用首次Arrow schema，不再重讀。Receipt新增`requested_reconciliation_scope=full_source/incremental`與`lock_wait_seconds`，不把incremental bootstrap實際full讀取誤寫成tail-only，也不把requested scope當資料完整證明。

### 測試與可重跑量測

受測candidate與installed兩檔byte-identical；獨立review後root canonical十模組 **102 passed／6.79秒**，其中46個新cases。覆蓋head／middle／removed dates、新mtime、corrupt target、empty保舊、UTC completed day／partial coverage／hot-tail、duplicate／off-grid、讀取／寫入／atomic replace失敗、鎖timeout零輸出、鎖內final receipts／finally release、三provider成功／失敗／disabled與scope接線。Ruff、compileall、bash syntax、scoped diff check通過；不是全庫驗收，也不將隔離測試重複加總。

[最終ABBA收據](../artifacts/benchmarks/daily-materializer-accepted-abba-20260927.json)每scope／variant20次，使用真實0GUSDT的538,728-row base（91,201,249bytes）與當輪hot tail；全部date／OHLC／volume／coverage輸出IPC SHA相同，來源parts＋stat＋SHA量測前後相同。只量`_daily_frame`讀取／logical merge／聚合，暖OS／imports、2個Polars threads；沒有HTTP或正式輸出寫入。基礎服務仍在執行，root測試與coverage不和accepted量測重疊。

| 同份來源工作 | 舊wall median | 新wall median | 舊／新CPU median |
| --- | --- | --- | --- |
| 完整來源聚合 | 384.246ms | 380.304ms | 434.666／423.896ms |
| 尾端篩選聚合 | 27.115ms | 24.859ms | 43.377／40.960ms |

完整聚合差約1%，**不宣稱穩定或全服務加速**。先前短量測428→395ms、加長394→385ms也保留；不同輪hot tail可能自然前進，只保證各輪內來源未變，不跨輪混成一份統計。中間`final-abba`曾與coverage同時啟動，不作最終acceptance。修正後的full投影比舊的錯誤tail-only多做必要歷史工作，不能沿用舊13秒daily step作加速證據；自然全量時間／peak RSS尚待測。

### 06:45:44 現況與仍待處理

[套用後coverage](../artifacts/benchmarks/service-coverage-20260927T0648-daily-materializer-installed.json)實際06:45:44（檔名0648不是觀測時鐘）：57服務／42timer／3path，當次timer與resource-pressure findings為0，六個localhost API200。Gateway／當沖／Discord維持PID1740524／653393／653541、原invocation及NRestarts0。當沖單次GET5.248ms只是當次投影／快取狀態，不能歸因於本patch。TAIFEX blocked、當沖degraded、隔日沖／Shioaji／OpenBB waiting、monitor critical不變。

Weekly仍原02:33 invocation `3e1199c9d5574855aa33e02dd499bf33`。同輪Yahoo step exit0；Binance574 symbols／728,921,996rows報告完成，但其舊daily13秒沒有full重算。Bybit864updated／3failed已持久，不因外層仍running而藏掉；OKX仍candles，06:46 page count126,183持續前進，123已完成symbol不是停止工作證據。尚不能宣稱weekly或全歷史修復完成。

**已啟動shell保留舊函式，這次OKX後續projection也不能當新版`--refresh`接線驗收。** 未強制重跑729M／763M級minute來源來冒充完成。接續需要安全驗收歷史daily投影、量全量成本，再以來源revision／分段delta避免無必要全掃；default incremental的mtime／last-two-day shortcut仍不是舊歷史修正證明。OKX大量head請求、Bybit3個失敗、CBC上游、TAIFEX blocked／自然輪、Windows冷開機、公網雙棧／瀏覽器與全庫測試繼續留在原目標。

## 2026-09-27 06:11 起：CBC 無效列表不再放大為整批請求

**上一輪分類 progress，目標保持 active。** 本輪沿57服務的既有清單修復CBC FX／Money失敗路徑，沒有以單一模組完成代替全系統目標。未訪provider、重啟服務、改排程／資源限制或寫正式來源、帳本與cold store；兩份CBC現場receipt SHA不變。完整驗證命令、source hashes與限制見[本輪交接](../artifacts/benchmarks/cbc-listing-failfast-validation-20260927.json)。

### 證實的問題與實作

FX原本先抓完所有頁面，再驗首頁raw rows／page identity／layout／total算術。隔離13頁反例的首頁只有1／60列，舊版仍抓13頁、save13次才失敗。這不是03:00官方upstream-error事件的已證實根因，而是另一個可重現的放大失敗風險。

FX現在擁有共用逐頁validator；Money委派它，移除重複算術。首頁在save與排程後頁前驗證；其餘頁在save前核對本輪首頁的metadata。FX legacy20／online60 layout仍分離，Money固定60；Money近期回補的舊尾頁仍各自驗歷史generation，沒有套本輪totals或推進舊full-scan時鐘。既有所有頁面／所有raw列數、Parquet與resume來源驗證均保留。

兩個listing pool的exception範圍涵蓋submit、future result與progress state write。任一不可恢復的discovery錯誤取消尚未啟動的工作，保留原exception；部分submit失敗也不再drain已有queue。已啟動HTTP仍正常等待，其他worker可能在主執行緒看到錯誤前開始新工作；**不宣稱瞬間中止HTTP或固定最多額外workers−1頁**。Detail階段個別article失敗／SourceAccessBlocked語意未改。

Money新增listing／detail／Parquet proof的stage_seconds。兩個collector原elapsed都在Parquet前計算，現改在Parquet proof後、final receipt前採樣一次，明示 `elapsed_scope=through_parquet_proof_before_final_state_write`。不含最後狀態寫入、process啟動或wrapper後續建置；各stage不是完整可直接加總的wall分解。

### 驗證與效能：分清失敗路徑與正常工作

隔離候選 **208 passed／7.43秒**，獨立review後一次套完整patch，canonical九模組 **208 passed／7.18秒**。新增30個測試涵蓋錯頁／短頁／layout／totals、transport／raw寫入／progress寫入／partial-submit、原exception、queue cancellation、legacy20／60、mixed-generation及注入2秒write clock。舊missing-page fixture改為真正有效的首頁，繼續檢查第二頁缺失，沒有移除測試。Ruff／compileall／scoped diff check通過；兩份installed source與受測候選byte-identical。非全庫或自然collector驗收。

[加長ABBA](../artifacts/benchmarks/cbc-listing-failfast-extended-abba-20260927.json)每scenario／collector／variant20次，使用synthetic 13頁／722 raw rows，實際跑parse、raw存檔、detail、Parquet與receipt；每次私有資料root為新建，暖imports／OS、零網路。載入同份已hash source bytes並核對量測前後無變；只固定fixture datetime做provenance byte parity，perf_counter／process CPU都是真實時鐘。

| 場景 | 舊版wall median | 新版wall median | 請求／結果 |
| --- | --- | --- | --- |
| FX首頁不完整 | 104.354ms | 1.231ms | listing 13→1、raw save 13→0；同樣拒絕不完整來源 |
| FX正常完整工作 | 131.694ms | 130.798ms | 各13 listing＋13 detail；Parquet SHA一致 |
| Money正常完整工作 | 155.662ms | 151.844ms | 各13 listing＋13 detail；Parquet SHA一致 |

正常路徑**沒有穩定整體加速證據**：保留[短樣本](../artifacts/benchmarks/cbc-listing-failfast-accepted-abba-20260927.json)，其中Money144.707→154.664ms略慢，加長後方向反轉。因此只確認故障請求放大被消除，不把它說成正常collector／公網／p95加速。早期候選與測試重疊輪的收據也保留，但不作final acceptance。

### 全服務觀測與下一步

[06:27:13 coverage](../artifacts/benchmarks/service-coverage-20260927T0627-cbc-listing-failfast.json)：57／42／3，該次timer與resource-pressure findings為0；六個localhost API200，當沖單次GET129.779ms。Gateway／當沖／Discord仍原PID1740524／653393／653541、原invocation與NRestarts0。TAIFEX blocked、當沖degraded、隔日沖／Shioaji／OpenBB waiting、data monitor critical仍忠實保留。

OpenBB新自然輪於06:15:41 completed，invocation `34703cbf62c342cc8b6e7af51552d868` matched，新增260 segments，pending1,580,023、compacted5,737,619；不是CBC patch的成果。Weekly backfill仍running，memory22.35GB／peak52.63GB。CBC仍03:00原failed invocation；觀察到下一timer為9/28 07:00，新版自然驗收與上游當下是否恢復尚未知。

同輪FX／Money共享listing仍未完成：full/full可省全部重複頁，正常Money recent只重複前2頁；跨subprocess安全重用需要本輪pinned receipt、原始觀測時鐘、同份SHA驗證bytes與各namespace證據，不能拿cached_list_pages冒充online。此項、CBC上游恢復、registered缺口／writer競爭、TAIFEX blocked／自然輪、Windows冷開機、公網雙棧／瀏覽器及全庫測試均繼續列入原目標。

## 2026-09-27 05:51 起：共用收據大小契約與重啟證據保存

**上一輪分類 progress，目標保持 active。** 本輪修復 CBC Money／FX 共用狀態寫入與讀取上限不一致；這是隔離反例證實的恢復風險，不是03:00現場上游錯誤的已證實成因。未重啟服務、手動抓provider、變更排程／資源上限，亦未寫正式來源、帳本、cold store或CBC收據。完整source hashes、命令與邊界見[驗證交接](../artifacts/benchmarks/release-state-size-validation-20260927.json)。

### 從實際反例修復，不裁掉恢復證據

原reader只讀4MiB，但writer未驗序列化後的UTF-8大小。真實邊界fixture的成功收據剛好4,194,304bytes；加上失敗狀態與inline checkpoint後變4,194,579bytes，下一次reader拒絕、失去恢復候選。新版一般路徑仍用原inline v1，**序列化一次**後直接交canonical durable atomic byte writer；沒有二次JSON轉換。

只有超限例外才保存舊bounded收據的**原始bytes**到 `state/release_resume_evidence/{dataset}/{sha256}.json`，latest改為小型degraded與schema v2 reference。Reader最多讀兩個各≤4MiB的檔案，核對SHA／精確bytes／dataset／schema，拒絕觀察到的symlink、外部路徑與遞迴reference。後續running／degraded沿用驗證過的v2，不再將大型proof嵌回latest或覆蓋最初超限診斷。舊v1與normal成功格式保留；真正新的合法線上成功才移除latest checkpoint。

超限claimed complete會先持久degraded再拋 `ReleaseStateTooLarge`，避免collector回傳／列印原來的green summary。超限非成功狀態不遮蔽原 `SourceAccessBlocked`。完整超限診斷保存在每dataset單一latest檔 `state/release_state_diagnostics/{dataset}.oversized.json`，而非無限追加；該檔可以超4MiB、不作resume來源。Evidence objects保留，本輪沒有新增GC或刪除。Checkpoint只是恢復metadata；collector原始來源／Parquet SHA驗證與dashboard／訓練不接受degraded的契約不變。

若連evidence storage都失敗，無法同時把唯一近上限proof留在單一bounded檔、又持久新狀態。此時**保留舊bytes並拋錯**，exception note明示 `new_state_not_persisted`；不能說新degraded已落盤，舊receipt也不是本次成功。最新檔replace失敗亦不能保證新狀態。Diagnostic單獨寫入失敗則記錄 `saved=false`，仍可發布bounded degraded。未宣稱能抵抗所有磁碟故障或已完成冷開機驗收。

### 測試與量測

隔離候選先 **178 passed／6.52秒**，獨立review後一次套完整production patch，再 **178 passed／5.86秒**；這是178個相關測試，不將兩次加成356或宣稱全庫通過。涵蓋UTF-8真實4MiB、舊inline、重複狀態、兩個CLI、完整性consumer、缺檔／hash／size／dataset／schema／symlink／遞迴拒絕、PermissionError／ENOSPC與未保存狀態。Ruff、compileall與scoped diff check通過，installed source與受測candidate byte-identical。

[可重跑ABBA](../artifacts/benchmarks/release-state-size-benchmark-final-20260927.json)：一般8KiB私有fixture，每樣本完整durable成功寫入＋失敗寫入＋resume讀取，baseline／candidate各30次，wall median **0.452／0.439ms**，輸出bytes一致；差異很小，**不宣稱已加速**。真實4MiB邊界latest由4,194,579bytes縮至736bytes且proof完整保留，單次例外處理＋resume成本68.43→96.01ms；新路徑多了證據／診斷持久化與hash核對，不能拿舊的「丟失恢復候選」當等價更快工作。這是暖imports／OS、synthetic metadata，不是完整provider、自然service、p95、公網或斷電證明。

### 06:07:08 服務觀測與未解項目

[最新coverage](../artifacts/benchmarks/service-coverage-20260927T0610-release-state-bounds.json)實際時間為 **06:07:08**（檔名0610不是量測時鐘）：57服務／42timer／3path，該次timer及resource-pressure findings均0，六個localhost API200；當沖單次GET119.22ms，不是p95。Gateway／當沖／Discord仍原PID1740524／653393／653541、原invocation與NRestarts0。TAIFEX blocked、當沖degraded、隔日沖／Shioaji／OpenBB waiting、全資料monitor critical仍真實顯示。

CBC維持03:00原failed invocation、兩份現場receipt SHA前後未變且無checkpoint；新版尚未由下一自然collector驗收。OpenBB仍上輪05:43 completed invocation、pending1,612,791，不冒充新完成。Weekly backfill仍原invocation執行、memory23.26GB／peak52.63GB。CBC上游、FX／Money重複listing取得、registered缺口／writer競爭、TAIFEX blocked與下一自然建置、Windows冷開機、外網雙棧／瀏覽器及全庫測試仍未完成。

## 2026-09-27 05:31 起：同輪來源共用與 CBC 分頁完整性修復

**上一輪分類 progress，目標繼續 active。** 本輪維持57服務範圍，沿 `extend-stockagent-derivatives` 的canonical建置路徑減少重複計算，同時修復另一個資料服務的錯誤成功風險。沒有重啟服務、手動觸發下載、改排程／資源限制，亦未修改正式資料、帳本或cold store。總交接：[source hashes、重測命令及邊界](../artifacts/benchmarks/run-reference-cbc-validation-20260927.json)。

### TAIFEX：同一次工作只讀一次完整期貨來源

前輪商品預篩選仍保留；此次再移除producer兩個重複load helper，由既有完整TX reader建立一份run-scoped `TaifexTxOpeningReference`，ATM候選／ATM輸出／所有full-chain shards／merge共同使用。內含factory自建、唯讀的日期→合約／開盤價及日期→開盤價mapping，沒有global或新增持久cache，不保留可變NumPy view。既有raw SHA、程式fingerprint、shard SHA與最後來源核對保持。

factory前後及各consumer驗resolved path＋dev／inode／size／mtime／ctime，hash→factory之間換代亦在任何建置前拒絕。ATM預算列核對相同合約／開盤價，shard fingerprint直接取同一reference；merge再核對每個shard對應日期的futures fingerprint。原本缺TX的空shard仍記錄解析日期，補到TX後只重建受影響shard；真正無weekly scope的空shard仍可重用，空shard日期重疊不被忽略。

ATM、full-chain及merge在replace前再次驗來源；失敗保留舊target並清理自己這次的temporary。沒有共同source lock，最後stat→replace仍存在極短TOCTOU；多輸出也不是單一transaction。本輪不宣稱整份來源／輸出完全原子快照。所有候選先在隔離副本測試，再一次套完整patch；canonical兩檔與已量測候選byte-identical。

使用真實2026-08與09兩份TXO來源（合計21,957,105bytes），每次給全新私有derived cache；保留完整producer的raw驗證／hash、ATM、shard、merge、quality與manifest寫入，只限制month iterator為明確兩份來源並禁止網路。ABBA每variant兩次，包含第一次reference建立，結果見[最終收據](../artifacts/benchmarks/taifex-run-reference-final-abba-20260927.json)：

| 量測 | 原流程 | 同輪共用 | 結果 |
| --- | --- | --- | --- |
| 完整期貨loader呼叫 | 8次 | 1次 | 去除7次同來源重讀 |
| 整個受測正規化wall median | 7.561秒 | 4.174秒 | -44.80% |
| process CPU median | 7.722秒 | 4.224秒 | -45.30% |

四份monthly／weekly ATM與full-chain Parquet的SHA在全部試次完全相同，原始來源身份及SHA前後不變。這是**暖OS頁面／imports、兩份來源、冷私有derived cache**，不是完整2001起歷史、自然服務總時間、公網、瀏覽器、冷開機或p95。保留[較早候選收據](../artifacts/benchmarks/taifex-run-reference-abba-20260927.json)，不把較早未補merge guard的結果當最終source。程式fingerprint會正常使舊projection一次性失效；下一自然完整TAIFEX工作仍待驗收，未手動重建正式cache來湊結果。

隔離78個相關測試通過後，root正式七模組 **101 passed／12.16秒**，涵蓋冷main恰一次load、全部top-level verified reuse零次、跨路徑／hardlink／symlink換指向、讀取途中及還原mtime改寫、ATM／merge混代拒絕、舊target保留、部分日期cache失效及既有期貨策略／backtest回歸。Ruff／compileall／scoped diff check通過；不是全庫或完整訓練fold驗收。

### CBC Money：不能把「有回應／hash吻合」當「分頁完整」

純記憶反例確認舊collector請求page1／2／3卻收到同一page1，廣告121筆／3頁但每頁只有1筆raw row，仍會回complete並被checkpoint metadata接受。這是可重現邏輯缺陷，**不是03:00現場upstream error的已證實成因**。

已重用FX的 `ListingPage` 型別與60-row layout常數，在同一次HTML parse驗實際頁碼、頁數／總列數算術、page size及每頁raw rows；比較本輪新抓頁的metadata一致性，在detail工作及Parquet發布前拒絕錯頁／短頁／多列／缺metadata。Money篩選後筆數不冒充raw筆數，公開parser既有回傳介面保持。新summary保留完整分頁metadata。

舊checkpoint即使SHA吻合，也會對每張原始listing從**同份hash-verified bytes**重新驗結構，包括未refresh尾頁；不重開path、不重讀來源、不跨混合代際硬比總數，也不推進舊full-scan時鐘。錯誤保留原Parquet／checkpoint與degraded狀態；20-row舊cache不能當60-row完整證據，未刪任何舊原始檔。root獨立七模組 **141 passed／5.25秒**；加TAIFEX共242個本輪相關測試，不重複加總隔離測試。[CBC完整驗證收據](../artifacts/benchmarks/cbc-money-listing-proof-20260927.json)。

新增HTML結構驗證有必要成本，不宣稱CBC加速：3頁私有fixture／7個唯一檔案，每個source只open一次，9次local resume驗證median25.50ms；不是完整歷史或前後A/B。正式resume phase已有獨立計時可觀察後續成本。現場兩份CBC receipt仍為官方HTTP200 upstream-error HTML造成degraded、沒有checkpoint；本輪未訪provider，不能宣稱來源已恢復。

### 最後服務觀測與下一個未解風險

[coverage](../artifacts/benchmarks/service-coverage-20260927T0550-run-reference-cbc.json)實際時間 **05:46:49**：57／42／3，timer與該次resource-pressure findings均0，六個localhost API都200；當沖單次GET120.281ms。TAIFEX blocked、當沖degraded、隔日沖／Shioaji／OpenBB waiting、資料監控critical保持真實。Gateway／當沖／Discord PID1740524／653393／653541及invocation不變、NRestarts0。

OpenBB另一自然輪invocation `3c208c8adcc844a882a4b818781e3965` 已完成且audit matched，pending1,612,791，較前輪再少32,768；此為新進度，不是本輪TAIFEX修正的加速成果。Weekly backfill仍原invocation執行中，memory24.69GB／peak52.63GB；CBC release archives仍failed，未reset-failed掩蓋。

後續仍包含CBC上游及正式恢復驗收、shared receipt writer可能寫超4MiB而reader拒絕的風險、FX／Money同輪重複listing取得、registered缺口／writer競爭、TAIFEX blocked與自然輪驗收、Windows冷開機、公網雙棧／瀏覽器環境及全庫驗證。本輪未改寫這些為完成。

## 2026-09-27 05:09 起：TAIFEX 減少無用轉換、OpenBB 日誌證據恢復

**目標仍 active，持續有實作進展，未宣稱全系統完成。** 依 `extend-stockagent-derivatives` 重用既有 canonical reader／builder；本輪不新增資料管線，不改交易／行情／帳本／來源檔／排程／資源限制，未重啟 gateway、當沖或 Discord。全部結果和重測入口見 [本輪驗證交接](../artifacts/benchmarks/taifex-loader-openbb-validation-20260927.json)。

### TAIFEX：只提前排除沒有要求的商品，不刪日曆或遠月

真實 futures parquet 為69,816列，TX34,994／MTX31,660／TMF3,162。原完整loader把全部商品轉成Python objects再逐列排除；現在只對標準string／large_string欄位，以原有Python `strip().upper()` 規則先做Arrow商品filter，再進相同完整loader。全來源date先驗證及保留，selected products所有tenors／duplicate／multiplier／metadata檢查不變；非標準dtype沿原路徑，三商品全集繞過新增unique／filter工作。沒有只讀近月、裁日期、補假值或以cache代替來源重建。

候選在隔離副本完成驗證後才一次套用完整patch。新增回歸包含未選商品的非法日期在顯式panel_dates下仍拒絕、其他商品唯一日期保留、全部遠月、Unicode空白、nullable／binary／dictionary、duplicate與原metadata錯誤。正式來源六模組 **78 passed／80.33秒**；獨立review未發現新增correctness問題。source SHA `c7588c7ad75392135bea2425336749e56265a47a0fb549338204935b53763782`，隔離候選與正式function AST相同。

最後一次[順序ABBA重測](../artifacts/benchmarks/taifex-futures-loader-sequential-cpu-20260927.json)在上述測試結束後執行，低優先權、暖imports／OS cache、同一真實來源；不是獨占主機、來源冷啟動或整個歷史服務速度。時間為median：

| 範圍 | 舊wall → 新wall | 舊process CPU → 新process CPU | 結論 |
| --- | --- | --- | --- |
| 完整TX loader | 528.57 → 440.70 ms | 544.17 → 456.45 ms | wall -16.62%，CPU -16.12% |
| 全三商品loader | 681.35 → 687.27 ms | 693.69 → 700.34 ms | 未證明加速；wall +0.87% |
| 一份來源monthly full-chain | 1,055.56 → 996.04 ms | 1,070.25 → 1,008.70 ms | wall -5.64% |
| 同來源weekly full-chain | 942.70 → 855.00 ms | 955.84 → 869.18 ms | wall -9.30% |

TX／全商品各每variant4次，monthly／weekly各每variant2次，不推論p95。所有dataclass arrays逐欄dtype與數值完全相同；monthly33,532列／weekly20,502列的Parquet SHA逐次一致，原始來源SHA與五元身份前後不變。最初profile2.685秒含profiler成本，不能與無profiler量測混算。保留全部前輪receipt，包括[同時執行測試的一輪](../artifacts/benchmarks/taifex-futures-loader-promoted-20260927.json)全三商品慢5.06%的結果；不能只挑最佳數字或由這次量測保證零退步。

**下次自然TAIFEX工作尚未驗收。** 兩個manifest與68份monthly／weekly shard receipts仍綁舊reader SHA，ATM抽樣同樣失效；原fingerprint機制會正確觸發一次性重建，首輪成本可能較大，未改寫receipt或繞過驗證。歷史兩組各34 rebuilt的紀錄與新shard cache首次冷建置相符，但不足以證明是反覆無謂重建；沒有因此拿掉cache驗證。上述微測速不能當下次整組服務加速幅度。

### OpenBB：大筆terminal journal現在可作真實的fallback證據

05:08:15原排程自然啟動，invocation `7078e9b02b0b4ac7b174ddfe257fdf45` 於05:10:32完成，exit0，新增263 segments／32,768來源檔；pending1,678,327→1,645,559，48endpoint合計與canonical／attempt一致。business136.522秒與systemd main137.026秒是不同時鐘邊界；不拿相較前輪縮短當受控加速。MemoryPeak2,463,752,192bytes，本輪high／max／OOM／swap均0；仍有pending及FRED query deferral，不是全部資料完成。[自然輪完整收據](../artifacts/benchmarks/openbb-natural-cycle-20260927T0508.json)。

實際terminal app MESSAGE有5,637bytes，原 `journalctl -o json` 未加 `--all` 會將大欄位變成null，造成fallback證據遺失。已在既有consumer加 `--all`，保留32筆／5秒／輸出1MiB和單行64KiB拒絕門檻，以及精確unit／boot／invocation／時間驗證。實際只靠journal恢復也已通過，沒有製造新completed狀態。五項新增回歸包括大訊息及損壞／過大拒絕；root相關四模組 **484 passed／2.11秒**。合計本輪兩組562個相關測試通過，非全庫驗收；Ruff、compileall與scoped diff check均通過。

### 最後全服務觀測

[coverage收據](../artifacts/benchmarks/service-coverage-20260927T0528-futures-loader-openbb.json)實際觀測 **05:27:22**：57服務／42timer／3path，timer與本次資源壓力findings均0，六個localhost API均200。當沖112.419ms只是單次GET；TAIFEX blocked、當沖degraded、隔日沖／Shioaji／OpenBB waiting、資料監控critical未改成正常。OpenBB匹配本輪completed與step timings；gateway／當沖／Discord原PID及invocation不變、NRestarts0。

CBC release archives仍failed；weekly backfill仍原invocation執行中，memory約24.78GB、peak52.63GB，不能宣稱完成。CBC upstream、registered歷史缺口、writer競爭、TAIFEX最新自然輪、Windows冷啟動、瀏覽器環境與全庫驗證仍有未完成項目。本輪未重測外網／雙棧、瀏覽器或冷開機。

## 2026-09-27 04:51 起：逐步耗時證據與跨重啟排序的保守修復

**目標仍 active，上一輪分類為 progress。** 本輪維持57服務範圍；依 `extend-stockagent-derivatives` 的重用邊界，只接既有TAIFEX日誌，不改行情、交易、模型、資料來源、排程或資源上限。沒有手動觸發下載或重啟gateway／Discord／當沖引擎。

### TAIFEX 三步耗時：已實作，但最終最新輪次驗收仍待證據

新增純consumer `scripts/service_stage_journal.py`，接入原全服務audit及五分鐘runtime tracker；只讀wrapper原有 `options_daily`、`recent_ticks`、`final_settlement` 三個固定stage。核對精確unit／boot／invocation、PID1 terminal、monotonic和wall clock、固定順序、退出碼及整數秒精度。缺筆、重複、未知stage、錯identity、回退時間、非終態、超時或損壞輸出均不補成功；lock skip明示deferred且無step timings。輸出明示 `completion_claim=recorded_wrapper_steps_only`、`source_completeness=not_checked`，不是資料完整或策略可交易的證明。

歷史invocation `80b242d9053342e1a6c9113e8c0dd6af`、boot `3ad2788e212f4bc38525bd6af6a07a68` 的原始journal確實記錄266／2／1秒，PID1 wall269.125075秒。來源clock是2026-09-25，不是今天新執行。內層既有log顯示monthly full-chain147.403秒、weekly114.927秒，兩者各重建34 shards；這是下一個可調查瓶頸，**不能未查cache失效理由就宣稱應該reuse或移除完整驗證**。

中途05:01 audit及05:03自然snapshot曾成功接出三步（完整57服務、3分片SHA核對）；最後獨立review發現更嚴格的跨boot邊界，因此**中途證據不作最終版本的latest-attempt acceptance**。目前boot `cd140071bfb440b4a960a125b3f002a7` 沒有此unit事件，14天PID1窗口含三個歷史boot。新版無法獨立證明哪個boot最新，故正式projection回 `unknown / matches_last_attempt=false / not_measured`。原始三步和先前receipt都保留，未刪歷史來製造單boot成功，也未縮小查詢窗口繞過檢查。下一自然timer是9/28 17:00；沒有為驗收手動重跑。

### 修復全服務共用的「舊成功冒充新工作」風險

純fixture重現兩項既有缺陷：同boot跨輪壁鐘倒退可讓舊run勝出；較新run缺terminal時會回落舊完成紀錄。共用PID1 helper現在先用同boot的start monotonic選最新invocation，再驗開始／結束pair；缺start或新工作未完成會阻擋舊成功。舊invocation延遲resource事件不能蓋過較新start；current boot有事件時優先current boot，否則多個歷史boot無可靠次序即unknown，不拿wall clock硬選。這是風險fixture，不是宣稱主機真的發生時鐘倒退或交易中断。

Root相關四模組 **479 passed／2.28秒**；獨立review其中三模組440 passed／1.04秒，Ruff、compileall通過。最終同程序10次read-only查詢，PID1 lookup median16.930ms；未知分支不查stage journal，median0.0097ms（首次含lazy import0.490ms）。先前34ms是中途成功stage-query單次樣本，不能拿未知分支成本當成功查詢加速。完整測試、source SHA、原始樣本與重測命令見 [telemetry validation](../artifacts/benchmarks/taifex-stage-telemetry-validation-20260927.json)。

### 檔案提示候選留在隔離環境，沒有足夠淨收益就不升級

以既有完整metadata的mtime選近期**實際檔案**，僅作提前negative miss提示；沒有positive fast-pass。50秒有界因果窗只有一個完整代際、零transition，故reader新增命中或遺失命中均未知，不能說收益已證明為零。固定57,463檔metadata，完整私有 `_write_quick_index`（含checksum／JSON／atomic write）經Path字串warmup控制後ABBA：baseline median63.084ms、candidate72.635ms，增加9.551ms；這不是整個producer速度。

候選**不升級**，所有新增實驗檔只在benchmarks；正式inventory SHA仍 `c4edfea8192527ba8004492e731e1a4827f3082b0a848d443f914da1b3299839`。11隔離測試root重跑0.14秒通過；加上述479為490個本輪相關tests，不是全庫驗收。保留首次warmup偏差樣本和無transition限制，[完整交接與重播命令](../artifacts/benchmarks/actual-file-preflight-handoff-20260927.json)。不繼續等待代際或改正式cache來湊收益。

### 最終服務觀測與未完成項目

[最終coverage](../artifacts/benchmarks/service-coverage-20260927T0507-final-ordering-gate.json) 內實際時間 **05:05:57**：57 services／42 timers／3 paths，timer findings零。六個localhost API均200；TAIFEX blocked、當沖degraded、隔日沖／Shioaji／OpenBB waiting、資料監控critical均未改成正常。當沖252.392ms是該次GET，較本輪前一次116.437ms高，沒有受控p95或整體加速宣稱。

Gateway／當沖／Discord PID1740524／653393／653541、invocation及NRestarts0保持不變。weekly backfill仍執行中，memory約22.42GB、peak52.63GB，不是完成。CBC upstream、registered資料缺口、writer競爭、Windows冷啟動、TAIFEX最新自然輪驗收及全庫驗證仍待完成。本輪增加可驗證性、攔下錯誤成功歸屬，沒有宣稱所有服務已最佳化到極限。

## 2026-09-27 04:30 起：正式 attempt 驗收、監測自復原與拒絕退步候選

**目標仍 active，上一輪分類為實際 progress。** 本輪維持全部57服務的範圍，不把完成單一telemetry修補等同全系統完成。未手動觸發OpenBB或snapshot工作，未改timer／資源上限，也未重啟gateway、Discord、當沖引擎或weekly backfill。

### OpenBB 新版已由原排程正式驗收

原timer於 **04:35:07** 啟動 invocation `6c2d2f6b3d7b48a3b185108035be84b6`，04:37:46正常結束。獨立attempt為 `370b12a2bb034ceb94a253dc1d9b7af5`，state `completed`、publication `status_published`、exit0，新增261 segments，failed／deferred／stale均0。Business elapsed **158.175573秒**，systemd main wall158.70902秒、CPU136.979293秒；兩種clock範圍不能混用。

本輪MemoryPeak **2,797,568,000 bytes**，swap0；exact attempt首尾及中間樣本high／max／OOM／OOM-kill增量均0，不把前輪high事件移植過來。03:33正式receipt到這次published總數的差額包含04:05已提交卻延期發布的進度，不能直接當作本次新source-file數。

04:38:28全服務觀測已正確產生 `matches_last_attempt=true`、`matched_run_step_timings`；04:41:09的自然長期樣本亦觀測同invocation completed，完整journal分片重組及SHA通過。先前04:25的「尚未驗收」是當時真實狀態，由本次後續證據補足，不改寫舊receipt。

最大階段仍是stale contract audit **60.195秒**（其中derivative metadata60.044），unassigned load30.960、segment build56.025、query publish5.833、status publish4.625。Nested timings不能直接相加；158秒比某前輪短也不是受控加速證據。保留來源／衍生驗證，後續應由實測瓶頸推進，不刪檢查來造快。[正式驗收與hash](../artifacts/benchmarks/openbb-l1-attempt-accepted-20260927T043746.json)、[全服務原始觀測](../artifacts/benchmarks/service-coverage-20260927T0438-openbb-attempt-accepted.json)。

### 全服務監測：損壞 baseline 不再造成永久重試失敗

隔離fault injection連續兩次證明：原baseline中一個service row若是list，後續每次採樣都 `AttributeError`，損壞baseline未被更新，可能永久失去所有服務測速。這是可重現故障風險，**不是宣稱現場baseline已損壞**；現場46.8KB／57服務baseline正常loaded。

沿用原tracker，不增daemon：限制baseline讀取4MiB、驗schema／finite clocks／row結構，壞資料重建完整新baseline並明示 `baseline_state`；跨缺口CPU、I/O、磁碟差值保持null。clock回退（包括admission後才發生）、負counter／free-space、NaN／infinity、深層JSON及過大檔均有測試。Current snapshot壞或太大不得覆寫舊baseline，不裁切服務清單；deactivating不冒充terminal，缺query timing不補假0。獨立review另抓出query_ms須在atomic replace前驗證，已修復。

最終tracker **54 passed**，加audit／OpenBB producer整合 **418 passed／2.30秒**，Ruff／compileall通過。暖同檔20次交錯舊parser／新reader median0.376／0.435ms，約增加0.059ms；這是健壯性改進，**不是加速宣稱**。原300秒採樣節奏保留，未到期不查systemd。[重現、測試、hash與自然恢復證據](../artifacts/benchmarks/service-runtime-baseline-validation-20260927.json)。

本輪也保留自身變更造成的incident：**04:35:04**，snapshot invocation `dcbab986e2c04d8a9a8cd8a7e5084988` 在短暫未完整的工作樹patch期間import到SyntaxError，該輪telemetry失敗；同worker仍成功發布面板資料。修正並編譯後，**04:35:33**下一自然worker成功輸出57服務樣本，04:41:09再次成功，兩次分片hash都已驗證。這暴露「正式timer直接import可變工作樹」的部署風險；不能據此宣稱零擾動或immutable deployment已解決。往後candidate應先在隔離副本完成驗證，再套用完整patch至live-imported檔案。

### 效能候選明確拒絕：directory hints 的局部收益不能替代整體驗收

真實snapshot日誌顯示少量Binance變動會造成87,563筆feature全量重建；原 `changed_dataset_ids` 只是診斷，沒有跨代際source proof，故未拿它繞過完整feature驗證。實驗改用最多64個parent directory hints提前拒絕已失效quick index，通過時仍全membership＋五元檔案identity掃描。

4096個metadata探針的局部negative miss確實更快，但正式自然worker出現 **refreshed_files=0／changed=[]** 仍落slow inventory rebuild，整輪約3.8～4.2秒，而原unchanged觀測約1.9秒；這些輪次仍成功reuse feature，本次退步在inventory而不是把feature又重建一次。非Parquet JSON／temp churn也會改變parent，隔離fixture已重現false-miss機制；每一正式輪次的具體觸發檔名未捕捉，不能宣稱逐一完成因果對位。**已撤回本輪全部production directory-hint hunks**，恢復檔SHA `c4edfea8192527ba8004492e731e1a4827f3082b0a848d443f914da1b3299839`，沒有加正式opt-in或降低驗證。候選封存在benchmarks作拒絕實驗證據，不是維護中的另一套服務。

撤回後自然workers正常完成；04:44～04:46仍持續有1～826個真正Binance／Bybit source更新，因此這段沒有可直接拿來比的unchanged樣本，不能宣稱正式median已量到回復1.9秒。恢復source的獨立inventory tests45 passed。後續可研究以**實際檔案**更新歷史改善既有512-file hint的選樣，但不可將目录時間或mtime相等當資料未變證明。

最終隔離probe：injected miss18.22→0.66ms、unchanged19.53→20.12ms，均不包含完整fallback；建立整份cache hints另需median24.29ms／首次99.72ms。因此局部倍率不作promotion依據。正式147 tests加封存候選24 tests，root回退後獨立重跑 **171 passed／2.57秒**；加前述tracker整合418，本輪兩组共589通過，不代表全repository驗收。[拒絕交接、自然樣本、各檔SHA與重跑命令](../artifacts/benchmarks/data-monitor-directory-hints-rejected-handoff-20260927.json)。Benchmark CLI僅載入SHA-pinned封存候選、寫私有index及實驗receipt，最多4096個不同來源路徑會被反覆stat，不會啟用正式路徑，也不讀Parquet內容或footer。

### 未完成的全系統項目

六個localhost API仍200，當沖113.758ms為04:38單次GET；TAIFEX blocked、當沖degraded、隔日沖／Shioaji／OpenBB waiting、資料監控critical均保持真實。CBC upstream／缺失舊resume proof、registered daily630 gaps、provider writer競爭、Windows冷啟動及完整repository驗收仍未解決。

服務測速下一候選已唯讀盤點：Binance public archive及margin readiness有步驟耗時但缺invocation ID，不能直接當exact-run proof；TAIFEX auxiliary已有可綁trusted journal invocation的三步耗時，尚未接入。本輪不以改名whole-process wall冒充operation coverage，也未刪掉必要資料或降低原目標範圍。

**04:50收尾：** gateway／當沖／Discord均loaded＋active，PID1740524／653393／653541與原invocation未變，NRestarts皆0。相關Ruff、compileall、scoped diff check及root兩份新增receipt解析／SHA驗證通過。這只確認本輪變更與指定執行邊界，不把仍degraded的資料或未做的冷開機驗收算完成。

## 2026-09-27 04:09 起：OpenBB 暫緩／故障路徑的可驗證執行紀錄

**目標仍 active。** 本輪以04:05自然輪的真實缺口為起點：archive scheduler恢復時，compactor已完成30個segments卻在正式發布前return，導致當輪階段耗時及資源觀測未持久化。未降低archive-idle、來源驗收、交易時窗、3GiB上限或原30分鐘timer；未重啟gateway、當沖、Discord或手動重跑正式compaction。

### 實作與正確性邊界

- 新增 `_state/l1_compaction_attempt_latest.json`，與正式 `l1_compaction_latest.json` 分離。只有持有原compactor lock的writer可原子寫start／terminal；鎖競爭者維持非零退出，只記有界journal event，不能覆蓋正在執行者的latest。
- 初期archive busy在開DB／清暫存前留下deferred；途中archive resumed保存本輪已提交segment數。audit-only、零batch仍需修復發布、隔離失敗／backoff、真正例外與中斷各自保留狀態。正式發布收據保留原五階段資源契約。
- 區分query publish開始／完成、status publish開始／完成。發布中或發布後故障不能被當成「完全未發布」或完成。例外只記類型，不把完整provider錯誤或endpoint catalog塞進event；event大小由固定欄位限制。
- 新增首尾資源樣本，沿用既有中間樣本／timings，不增加source／footer／DB掃描。正式status的計時結果也回填attempt。監測端新增精確attempt counter delta；尚未到達的stage不補零，實際存在但無效／倒退的counter不跨越。
- 嚴格核對schema、attempt ID、systemd invocation、時間及exit/publication語意。只在精確boot／invocation／attempt下接受bounded journal回補；較新的無效事件會阻擋舊成功，沒有回退到上一輪published receipt。
- 修復次生資源採樣OOM可能遮蔽原業務例外的問題。receipt寫入失敗不遮原traceback；若原工作成功但terminal receipt失敗，仍非零退出並保留「資料可能已發布」的階段證據。`SystemExit(0)`仍是interrupted，不是completed；SIGINT的shell 130與systemd signal 2分開驗證。

**崩潰邊界：** 為避免每segment fsync，只寫start／terminal兩次。SIGKILL／OOM kill可能僅留下running的開始快照；其中lock／not_attempted不是當下發布實況。consumer對running／unknown不輸出publication階段，也不假造terminal資源或完成。Python啟動前的ExecCondition skip仍屬systemd證據範圍，不能捏造producer attempt。

### 驗證與可重測成本

新增39個獨立attempt cases，包括真正tiny-Parquet producer→consumer串接、manifest／audit／build／publish／status故障、鎖競爭、磁碟寫入失敗、次生OOM與中斷退出碼；既有資料與query parity測試保留。最終相關七模組 **526 passed／17.15秒**，runtime trend另 **18 passed／0.54秒**，共544個；Ruff／compileall／scoped diff check通過。驗證結果與原始碼SHA見 [本輪驗收紀錄](../artifacts/benchmarks/openbb-attempt-validation-20260927.json)。這不是全repository測試通過的宣稱。

凍結最終程式碼後，隔離 `/tmp` 的15次小型量測：單次resource採樣median **0.455ms**（0.386–0.667）；完整synthetic deferred attempt，含原lock fsync、兩次durable receipt與首尾採樣，median **1.393ms**（1.146–1.620），event最大1,683 bytes。這是本機觀測成本，不是正式compaction加速、網路延遲或冷啟動SLA；未操作provider／正式DB。較早1.450ms為加入精確中斷退出語意前的樣本，以此最終版為準。重測程式碼及原始樣本保留於驗收紀錄。

### 正式服務仍待自然輪驗收

[本輪服務觀測](../artifacts/benchmarks/service-coverage-20260927T0427-attempt-telemetry.json)的實際時間為 **04:25:56**（以receipt內 `observed_at_utc` 為準，不用檔名推斷）：57 services／42 timers／3 paths，timer findings零。六類localhost API均200；TAIFEX blocked、當沖degraded、隔日沖／Shioaji／OpenBB waiting、資料監控critical，沒有改成綠燈。當沖137.736ms是單次GET，不是warm p95。

Gateway／當沖／Discord仍為PID1740524／653393／653541及原invocation，NRestarts零。weekly仍執行中，當次memory約38.67GB、peak52.63GB，未宣稱完成。CBC release archive仍是03:00原失敗，並未清掉失敗狀態；registered daily仍630個reported gaps。

04:25觀測時OpenBB尚未有新版正式attempt，`matches_last_attempt=false`是正確的未知狀態，不能用舊03:33 published receipt或04:05 exit0補綠。原timer下一輪04:35:07保持不變；**新版自然輪尚未驗收**。後續應以該輪exact invocation確認deferred／completed以及真實high事件、耗時，再選下一個瓶頸；不為了驗收額外啟動大型工作。

## 2026-09-27 03:46 起：CBC 續跑證據與真瀏覽器驗收

上一輪是有實作、併發修補、正式自然輪與 gateway 部署證據的 **progress**；本輪目標仍 active，沒有將全部57服務縮成單一面板優化。重新核對 CBC release job 仍為03:00失敗的原 invocation `9b421ea02d8f46e1ac31422c9f625216`；weekly 仍為02:33:18的原 invocation `3e1199c9d5574855aa33e02dd499bf33`、PID1538145，未因觀測等待或鎖衝突重啟。Gateway／當沖／Discord invocation與PID沿用上一輪，NRestarts零。

### CBC：失敗不再抹去上次成功的續跑依據

根因是 running／degraded state 覆寫上次成功 summary；money 的 `_money_recent_pages` 與 collector 都只認頂層 complete，故失敗後失去 Parquet hash、listing receipts及 full-index clock，原本2頁增量退回全索引。現在沿用 `downloader/release_archive_io.py` 與既有 durable atomic JSON writer，在當次 state 內保存**單層** `resume_checkpoint`；多次 running／failure 不遞迴增長、不改舊成功時間。

Current仍是 `running/degraded, complete=false`。不合格卻自稱 complete 的新summary直接拒絕，CLI照原例外路徑記失敗，而不是只改落地狀態卻回傳成功。完整線上成功需要正確dataset、hash、非空收據／counts與有效時間；cached/offline、缺hash、future/naive clock、壞metadata、超4MiB或深層壞JSON不能變成可用線上proof。FX與money共用保存機制，但**沒有新增FX增量掃描**。

排程前只讀metadata作規劃hint。money在原writer lock內重新讀取candidate，驗證 Parquet、所有listing raw與去重後detail raw的SHA／受限路徑／page identity／counts；before/after五元file identity拒絕讀取期間改寫、replace與symlink轉向，Parquet直接解析已驗證的同一份bytes。不因兩個metric共用同篇原稿而重複hash。原稿完整與value-history完整分開，raw-only／已知缺月份仍可合法續跑，沒有把缺值當已補齊。

增量完成保留原 `last_full_index_scan_at_utc`，只有真正full scan更新；money cached reparse改為degraded，與FX既有cached/offline契約一致。Sunday／explicit full仍走原full-index，retry／provider budget／canonical source lock／發布／訓練門檻未改。Money新增 `stage_seconds.resume_verification`，總elapsed也包含驗證成本；只讀receipt helper不再eager import Polars。

**root七模組111 passed／3.72秒**，含60個新resume案例及獨立consumer正／反控制：先證明完整fixture可通過dashboard／archive audit／training lineage，再轉為failed＋valid nested checkpoint，三者皆拒絕當作完成。覆蓋兩次失敗後restart recent、新Parquet replace但success state寫失敗、六種讀取期間變動、hash/路徑錯誤零API與零promotion。Ruff／compile／diff check通過，獨立review無剩餘blocker。

可重測synthetic fixture為3頁、3篇原稿、6個metric rows、7檔；9次驗證中位 **7.606ms**（5.202–8.454ms）。mock full實際3頁＋3篇，restart recent僅2頁＋1新篇；這是request範圍與本機驗證成本證據，**不是實網／全歷史速度倍數**。單次collector0.029／0.027秒不當受控加速率。[完整收據、SHA及重跑命令](../artifacts/benchmarks/cbc-resume-validation-20260927.json)。

**現存正式資料並未恢復：** live money仍為 `degraded/complete=false`、generated `2026-09-26T19:01:06.030066+00:00`，resume candidate為空；舊proof已丟失不能靠本修補補造。未寫live archive/state、觸發provider、重跑Sunday全歷史或reset-failed。下一合法full-index成功後才具备續跑基礎；新code由原排程下次自然載入。

### 瀏覽器：明示2D相容profile，不隱藏預設失敗

同機同版Playwright1.62.0／Chromium151.0.7922.34，原生timer、wall／monotonic clock會推進，但default可出現rAF0、ordinary click等stable逾時。控制ABBA default **0/4通過**；明示兩flags `--disable-gpu --disable-software-rasterizer` 的 `cpu-2d` **4/4通過**，500ms內30–31個原生frames。較早matrix有一次default成功，因此不宣稱每次必敗；GPU／software-GL初始化路徑被定位為相關條件，但精確Chromium／driver根因尚未證明。

既有 `scripts/probe_browser_runtime.py` 增加固定allowlist `--browser-profile default|cpu-2d`。預設不變，沒有env偷偷選擇、fallback、forceclick、dispatch、fake clock、manual beginFrame或延長timeout。parent核對worker回報profile與launch選項；失敗仍exit1。只讓FinMind單一2D browser test顯式使用同helper，其餘browser launches不動，不升降套件或改正式服務。

同時修復該測試過時的quota fixture：原固定9/25點已超出最近1日範圍，且兩點相差60分鐘超過12分鐘斷線門檻，實際是空圖，不能當rendering證據。只把獨立quota clock改成實際UTC now−2min/−1min，數值2／12和其他dataset日期不變；新增exact SVG series／points／empty-hidden斷言，保留所有原正常click、filter與overflow檢查。

**root public_dashboards＋probe＋data_monitor三模組234 passed／24.19秒，沒有排除browser。** 桌面1280／手機390實際quota曲線均在完整截圖驗得像素，Canvas2D及SVG控制像素亦正確；root已目視桌面曲線及手機整頁。手機單獨element screenshot仍得到單色clip，保留失敗圖，另以同次full-page及CTM對位的5個曲線座標證明實際頁面繪製；不能聲稱clip capture也修好了。僅HTML／SVG／Canvas2D驗收，**不包含GPU／WebGL／WebGPU或硬體影片加速**。

[完整瀏覽器交付與重測命令](../artifacts/benchmarks/browser-runtime-profile-handoff-20260927.json)、[控制ABBA](../artifacts/benchmarks/browser-runtime-software-raster-abba-20260927.json)、[最終真頁面及像素證據](../artifacts/benchmarks/browser-runtime-finmind-quota-final-cpu-20260927.json)。預設CLI最終仍frames0／timeout／exit1；cpu-2d frames30／normal click passed／exit0，兩種結果均保留。

### 正式服務觀測邊界

[本輪03:54觀測](../artifacts/benchmarks/service-coverage-20260927T0359-resume-work.json)的實際 `observed_at_utc=2026-09-26T19:54:23.656156+00:00`（以receipt內clock為準，不用檔名推測時間），維持57 services／42 timers／3 paths；六類localhost API都200，但當沖degraded、TAIFEX blocked、隔日沖／Shioaji／OpenBB waiting、資料監控critical。當沖單次149.146ms，未用warm小樣本替代。

OpenBB原timer於 **04:03:08** 自然開始新 invocation `337a927423594987b2a8f1b56cb52f9e`、PID1778277；04:04仍activating/start。沒有改timer或人工再跑；Result=success此時不是完成證明，保留原job繼續觀測，不因等待時間重啟它。本輪程式修改不需要重啟gateway／engine／Discord，三者均未重啟，也未改帳本、行情或訂單。

**04:05:02自然輪已終止，不再等待／重啟它：** 上述 OpenBB invocation exit0，但業務是 `deferred_publication=archive_busy`，因archive scheduler恢復活動，30個new segments後停止；failed_segments0、cleaned_temp_files0。CPU74.513秒、MemoryPeak2,953,252,864 bytes、swap0，不能把前輪1,390 high events歸給這輪。原 `l1_compaction_latest.json` 仍是03:33的已發布receipt，並未假造新完成。

此自然分支暴露下一個監測缺口：`after_segment_build` 的stage_resources已取得，但 `stopped_for_archive` 在詳細receipt發布前直接return0，當輪phase/resource證據未持久化。下一步應另存attempt-level deferred receipt，**不推進query/status publication，也不降低archive-idle gate**。[終止觀測原始證據](../artifacts/benchmarks/openbb-l1-deferred-observation-20260927T0405.json)。下一timer04:35:07保持原排程。

[04:07:42收尾全服務觀測](../artifacts/benchmarks/service-coverage-20260927T0408-resume-browser.json)仍57／42／3，六類localhost API200，產品health同前，當沖單次138.890ms。本輪完成的是CBC恢復機制及可實際運行的2D瀏覽器驗收；CBC上游、已丟失舊proof、GPU／WebGL／mobile clip capture、writer競爭、持久索引及資料健康等限制仍列待辦，目標保持active。

## 2026-09-27 03:08 起：同輪來源證據重用與完整 session facts

**目標仍 active。** 上一輪分類為有實作與正式驗證的進展，不是全系統完成；本輪仍覆蓋原本所有服務的效能、健壯性與穩定性。先驗證同輪已讀取的資料是否可安全重用，不更改成交、帳本、來源完整性或發布門檻。

[03:14 全服務觀測](../artifacts/benchmarks/service-coverage-20260927T0314-in-progress.json)涵蓋 **57 services／42 timers／3 paths**，timer findings 為零、六個 localhost GET 皆 200。當次產品狀態為當沖 degraded、TAIFEX blocked、隔日沖／Shioaji／OpenBB waiting、資料監控 critical；與 02:58 的 degraded 是不同觀測時點，不能合併成全綠。當沖 GET 約198ms 是本次單一觀測，不是端到端 p95 或冷來源重建測速。

Gateway／當沖／Discord PID **1633546／653393／653541**、invocation 分別 `4bff366b6d444c15884d6838117a8626`／`22a65c384b264bc89215233b2eebc8ad`／`128a3645f1bf485a94f52cf0e3e6131f` 保持不變，NRestarts皆零。週修仍是原 invocation `3e1199c9d5574855aa33e02dd499bf33`；03:14 cgroup約40.55GB，其中anon約25.32GB、file cache約14.46GB，MemoryPeak52.63GB，尚未達64GiB high。樣本內 high／max／OOM事件零，不代表沒有主機壓力；當次主機memory full avg60約0.46%、I/O full avg60約1.52%。未終止writer、刪鎖或縮小歷史範圍。

### 瀏覽器測試失敗與應用故障分開

FinMind正常click逾時的獨立實驗中，空白 `data:text/html` 單一button、無app／CSS／API，同樣 **rAF 0／500ms**，click等待stable超時；截圖也停滯。Headless shell／完整Chromium與四組compositor參數皆未建立可靠修復證據。自然wheel曾成功一次，但重跑連wheel也會停滯，因此不把它加入測試當作修復。保留正常click與原斷言，沒有force、dispatch、跳過或拉長timeout。

目前能定位到瀏覽器frame production停滯，但底層runtime／host原因仍未查明；**不是已證明FinMind UI有錯，也不是瀏覽器驗收通過**。互動例外不得被環境skip吞掉的三個故障注入回歸仍3 passed。後续需修復真實瀏覽器運行環境，不能靠DOM assertion取代實際互動驗收。

### 本輪候選與驗收邊界

- OpenBB：同輪L0 footer已在來源選取讀取一次，候選以完整file identity＋schema／task證據重用，省掉compaction前的第二次footer；仍保留L1衍生footer稽核、COPY後／publish前fence與三個輸出讀者。先以小型真實資料對照，不能把省一次footer宣稱整輪或網站已加速。
- 當沖：完整session facts必須保留所有raw欄位、nested值、missing／null、順序及晚到fragment。不能套用已刪欄位的曲線projection。候選只重用來源身份未變的完整解析結果，容量超限仍讀完整資料；需同尺寸改寫、ctime、replace、append及mutable alias故障驗證。尚未部署，不宣稱已改善正式延遲。

### 完整 facts 候選的有界量測

9/24 session 的 marks 1,620列／benchmark 825列／events 42列，僅使用已驗證的私有byte索引讀取，沒有全帳本掃描。原raw Python graph約5.95MB；重用同一process自行編碼的immutable pickle bytes合計約1.725MB，每次decode提供全新nested graph。**pickle不得落地或從外部輸入載入**。marks clone中位2.33ms對deepcopy19.20ms，benchmark3.92ms對23.80ms；這只決定快取內部複製方法，不是網站速度。[方法對照原始收據](../artifacts/benchmarks/tw-session-facts-copy-method-20260927.json)。

實作沿用原session cache，僅 `maximum_rows=None` 完整facts分支使用新entry；舊int讀者不增加deepcopy成本。每entry8MiB、合計32MiB、最多16entries；超界仍回傳完整資料，不截分鐘、不裁帳本。來源dev／inode／size／mtime／ctime在讀取、cache hit及發布前核對；同process觀測到mtime還原改寫時繞過舊persistent index重建。**舊磁碟index沒有ctime欄位，不能聲稱跨restart或兩份runtime觀測都被evict後仍能辨識先前的mtime還原**；此既有索引信任邊界未偷換成完整性證明，也未為遷移而強制掃描正式多GB帳本。

四個fresh worker的ABBA，以SHA-pinned實際dirty pre-change module為A，新cache為B；兩側均關閉不會公開的orders／fills display讀取。20次完整canonical payload皆相同，source及code前後fence皆通過。steady每側8樣本中位wall **63.074→43.969ms（約-30.29%）**、CPU **73.696→44.043ms（約-40.24%）**。fresh-module first每側只有2樣本，621.07→549.05ms，只保留觀測、不推論來源冷啟動收益。[完整ABBA收據及重跑argv](../artifacts/benchmarks/tw-session-facts-cache-abba-20260927.json)。這是builder／sanitize／encode／prepare範圍，不含HTTP、公網、cache queue或瀏覽器繪製；此刻尚未部署。

瀏覽器控制頁已另存[03:21失敗收據](../artifacts/benchmarks/browser-runtime-control-20260927T0321.json)：Playwright1.62.0／Chromium151.0.7922.34，可見且focus正常的純button，501ms內0frames，ordinary click在stable等待超時。可重跑入口為 `scripts/probe_browser_runtime.py --output <新收據路徑>`，失敗exit1、不覆寫既有收據。工具有worker deadline；Playwright的Chromium另建process group，若強制停止worker時無法独立證明browser清理，收據明示cleanup unverified，不能把診斷超時當通過或全子程序已清理。這個探針不取代正式頁面驗收。

### OpenBB：重用與加速是兩種不同證據

來源proof只在當輪存在，由一次PyArrow footer及前後五元身份觀測取得rows／bytes／uncompressed bytes／schema；不保存footer大物件、不作跨輪stat-only快取。COPY後、task callback後／replace前、DB registration交易內均保留對應file／task核對。task path已canonical且與proof一致時才略過重複resolve；symlink別名仍重新resolve。磁碟發布後DB gate失敗仍走原quarantine，不能留下有效member。未改原持久source_signature ABI、L1衍生footer完整稽核、三個輸出讀者、timer或記憶體上限。

三份ABBA負／中性結果全部保留：512檔synthetic **0.817793→0.844367s（+3.25%）**；真實SEC四個精確128-member segment共512檔／5,620,235 bytes／512列，首版 **1.128377→1.152241s（+2.12%）**；去除冗餘canonical path解析後 **1.140298→1.137695s（-0.23%，近乎持平）**。全欄位multiset相同、來源及code前後未變、PyArrow來源footer **1,024→512**，**沒有證明整體顯著加速**。這不是正式32,768批次／L1全稽核的測速；另一次FRED各檔schema不同的實驗在parity gate中止，不用失敗實驗宣稱等值。

[Synthetic原始收據](../artifacts/benchmarks/openbb-l0-proof-abba-20260927T0320/receipt.json)、[SEC首版](../artifacts/benchmarks/openbb-l0-proof-real-sec-abba-20260927T0323/receipt.json)、[SEC最終版及重測命令](../artifacts/benchmarks/openbb-l0-proof-real-sec-abba-20260927T0324-final/receipt.json)。03:24:41 source凍結，columnar SHA `4495b76c255d36931bfceee0d367fed924e8d74b5f26a1fec0aacab170cf9aef`、compactor SHA `d85f485e1d4cb9104bc119cfc46f597eb56915eacb60c199cef6d550464aa2e6`。保留03:28:51自然排程待實際驗收，未為測速人工重跑大型工作。

**03:29:59 OpenBB已依原timer自然啟動**，invocation `f5ae426035134cfe8f3819f4dd1dc563`、PID1722013；啟動前上述兩份SHA再次核對相同。03:28觀測的next已為03:29:59，以實際start為準，不把較早顯示的預計時間當未觸發故障。尚未完成時 `activating/start Result=success` 不是當輪業務成功。

**當沖候選繼續維持未部署：** 第二輪併發review找到首次read尚未附ctime觀測時，另一reader可能沿舊spans將空結果存成新signature的交錯。這不是已揭露的跨restart舊index邊界，而是必須修復的同process競態；前述ABBA的payload／測速仍是當時真實結果，**但不足以通過併發驗收**。正在把read-before觀測與index publication在同一既有鎖內原子核對，未重啟gateway載入未完成候選。

**另有正式資料工作失敗，不能漏報：** `stockagent-tw-public-release-archives.service` invocation `9b421ea02d8f46e1ac31422c9f625216`，03:00→03:03:40 exit1；CBC外匯存底及貨幣release collector遇官方upstream error HTML，被`SourceAccessBlocked`拒絕。DGBAS650 releases與CBC overnight6,076 dates另有各自完成收據，不將其成功當整組成功。目前只讀追查retry／來源共享與receipt邊界，未reset-failed、重跑整個Sunday歷史或寫入替代資料。當沖／Discord／gateway仍為原invocation。

**OpenBB自然輪完成驗收：** business receipt 03:30:00.340357→03:33:02.223710，**181.883秒**；systemd process wall182.830秒、CPU140.311秒，exit0。新增261segments，compacted **5,602,707**／pending **1,714,935**，較上輪精確推進32,768files；stale／failed／deferred_failed為零。41個view重用、SEC重建，FRED103,223個schema仍超4,096門檻而deferred，未降門檻或假造complete。

phase為L1 stale audit70.830秒、unassigned load41.083秒（內含query18.822、metadata22.258）、segment build55.790秒、query publish5.241秒。**304.099→181.883秒不是受控加速比**：未修改的L1 audit也135.458→70.830秒，來源batch與主機cache／負載均不同；固定SEC ABBA仍近乎持平。當輪MemoryPeak **2,953,740,288 bytes**、swap0、max／OOM／kill事件0，但觀測到 **1,390 memory.high events**（segment後151、query後1,390）；不能稱記憶體壓力已根除。仍保留原3GiB上限與30分鐘 cadence。

[正式原始收據](../artifacts/benchmarks/openbb-l1-source-proof-20260927T033302-f5ae426035134cfe8f3819f4dd1dc563.json)，SHA `e068b9f57d2f0ae82c4ff4bc6e964dfa9cf1513b734a97ffd8a67e11e63211c2`。下一timer當次顯示04:03:08，沒有人工再跑大型工作。

[03:35全服務驗收](../artifacts/benchmarks/service-coverage-20260927T0335-source-proof-completed.json)保持57／42／3、零timer finding、六個localhost GET皆200，且長期監測已精確匹配新OpenBB invocation並保留1,390 high events，不因service退出就遺失壓力證據。當次failed units有CBC release archives，以及同provider writer lock逾時後、等待下次timer的registered intraday；不能稱零failed unit。產品health仍當沖degraded／TAIFEXblocked／其餘waiting／資料監控critical。

公網名稱的本機直連小檢查（無proxy env）IPv4 HTTPS `/healthz`200，total1.819秒；WSL同機IPv6 TCP失敗。**同機hairpin失敗不是外部WAN IPv6失敗證明**：既有獨立Internet.nl receipt在9/26 08:15為passed，但本輪未重新觸發外部探針，也未改DDNS／路由器／Windows。上述單點耗時不是公網p95，舊外部receipt也不當現在雙棧驗收。

CBC純mock根因追查另外確認：一次running／failed state覆寫會丟掉最後成功的listing receipt／Parquet hash／full-index clock，令money下一次由2頁增量退回全歷史；FX detail的SourceAccessBlocked亦未像money一樣取消未開始任務。前者是恢復效率缺陷，後者是失敗傳播缺陷，但**本次事故仍發生於官方listing錯誤頁**，不是已證明WAF或漏重試。error HTML這條_fetch mock只1次request、0次defer；不應盲加重試。候選為在當次degraded／complete=false之外保存獨立且重新驗證的最後成功resume proof，不能把它供dashboard／audit／training冒充當輪完成；既已丟失的proof不可事後捏造。本輪只診斷、未修改CBC source或重跑。

### 最終併發修補與 gateway 部署驗收

上述當沖候選的同 process 競態已修復：首次讀取的五元來源觀測與 index publication 在既有 index lock 內交接，初始／最終 fence 拒絕同尺寸且還原 mtime 的改写；被拒絕的舊觀測仍保留為重試失效證據，不讓下一次 retry 將舊 spans 當成新資料。完整 JSON decode／clone 不持有新增全域鎖。跨 restart 的舊四欄 persistent index，以及違反 append-only 假設的 prefix rewrite＋append，仍是已揭露邊界，未宣稱本次一併修復。32MiB 上限只約束新增 serialized payload，非整個 process RSS。

**部署採用的唯一最終 ABBA** 是 [reviewed receipt](../artifacts/benchmarks/tw-session-facts-cache-reviewed-abba-20260927.json)：20 次完整 canonical payload 相等，source 五元及 code fence 均通過。steady 每側8個樣本中位 wall **53.014→34.170ms（-35.55%）**、CPU **62.471→34.168ms**；fresh-module first 每側僅2個樣本，**490.980→502.338ms（+2.31%，略增）**。不是 OS／來源冷讀、HTTP 或瀏覽器測速。前述 `cache-abba` 與檔名含 `cache-final-abba` 的舊收據都早於競態修補，保留原觀測但明確列為 concurrency rejected／superseded，不是部署驗收依據。

[最終 review／測試／重測命令收據](../artifacts/benchmarks/tw-session-facts-cache-validation-20260927.json)保留 child 獨立審查與生命週期邊界；其中 `production_enabled=false` 是 agent 交付時狀態，不覆寫成事後部署證明。相鄰回歸 **311 passed／1 deselected**，唯一排除的是先前已實測失敗的 FinMind 真瀏覽器互動；該失敗尚未修復。root 另驗 cache／benchmark／compact／session projection／serve 五個模組 **57 passed**，與上述有重疊，不相加宣稱唯一測試數。OpenBB 五個模組及 browser control probe 另組 **189 passed**；不是全庫通過。

**03:39:08 僅重啟公開唯讀 gateway**，新 PID **1740524**、invocation `f9c3ee81eefc4d0c9d1a0b750eec2dc5`，實際 source SHA `7fe0d88378da77df2241bc1821f9c413e62790990da19a8ce47950599b467010`。從發出 restart 到觀測 health ready **2.734秒**；期間5次短探針失敗，沒有宣稱零中斷。當沖／Discord／weekly 的 PID 與 invocation 前後完全相同，未改帳本或送交易。[部署原始收據](../artifacts/benchmarks/gateway-session-facts-deployment-20260927.json)。新 gateway invocation 初始 journal 僅正常 listening 訊息，沒有觀測到 fatal／watchdog。

部署後 localhost 四類 route 各 first＋10 次穩態樣本、concurrency1、共44次 GET 零錯誤；revision／當沖 status／OpenBB／data summary 的穩態 p50 分別 **1.120／1.408／1.082／1.543ms**，p95 **2.161／2.173／2.186／3.088ms**。當沖 first **2.175ms** 明確為 `cache;desc="fresh_hit"`，不能冒充首次來源重建。[HTTP 完整樣本](../artifacts/benchmarks/dashboard-session-facts-deployed-20260927.json)。這不是公網、UI 或資料健康已改善的證明。

### CBC 失敗傳播的有界修補

在前述診斷之後，僅修正 FX detail loop 的3行：與 money 既有行為一致，遇 `SourceAccessBlocked` 取消尚未開始的 futures 並重新拋出原例外，不把主機級來源失敗降格成單篇可繼續錯誤。已在執行中的 request 仍由原 executor 等待與 request timeout 約束，**不宣稱能即時取消 in-flight**。一般 `ValueError`／單篇 `ConnectionError` 維持原 failure／degraded，未變 retry、limiter、排程或發布條件。

新增 deterministic futures 回歸，先在舊碼重現「未拋出 SourceAccessBlocked」失敗，再驗證成功一篇後第二篇 blocked、第三篇 pending 被取消、同一例外傳出且零 Parquet 寫入。FX＋money＋refresh 三模組 **38 passed**，Ruff／diff check 通過。未手動觸發 provider、重跑 Sunday 全歷史或 reset-failed；本次正式 listing 錯誤仍未恢复。獨立 last-success resume checkpoint 保存（前述 A）仍待實作，不能把這個小修補稱為整個 CBC 恢復完成。

獨立 CBC review 無 blocker；root 重跑上述三模組為38 passed／0.79秒。生產端未為此重啟或觸發下載，等待原排程使用修補。Mock 的「2次 detail」只是控制交錯的測試證据，不能推論真實並行下只會發出兩次請求；已執行的工作仍可保留原raw供續跑。

**03:45收尾再驗：** gateway／當沖／Discord皆active且NRestarts=0，六類產品GET皆200，但健康仍依序TAIFEX blocked／當沖degraded／隔日沖waiting／Shioaji waiting／OpenBB waiting／資料監控critical。較晚這次當沖GET為 **122.202ms**，不是先前fresh-hit小樣本的1–2ms；此探針未記cache header，因此不猜測它就是cold/miss，也不把warm p50當所有request SLA。[本輪收尾證據及未解項](../artifacts/benchmarks/service-goal-round-20260927T0345.json)。目標保持active，繼續處理來源恢復、週修與即時writer競爭、瀏覽器runtime、持久索引及資源壓力；不宣稱所有服務已達極限或資料完整。

## 2026-09-27 02:37 起：週修阻塞、狀態頁無用讀取與特徵時鐘

**目標仍 active，以下是分項進度，不是全系統修復宣告。** 02:33:18 的正式 weekly invocation `3e1199c9d5574855aa33e02dd499bf33` 持續前進，但其 provider 級 writer lock 也阻塞 02:33:42 intraday invocation `d57e1d977b8a4ea79d63fd99dd166f7b`。Bybit 在 02:36:43 等待 source lease **180.021 秒**後失敗；OKX／Binance 舊版則無限等待 `.download.lock`，令同輪已退出的其他 provider 無法隨下一次 timer 重試。這不是 gateway／Discord／當沖中斷，也不應靠刪鎖或開放兩個 writer 解決。

### 資料一致性與恢復邊界

三個來源的 candle／feature 都有讀改寫同一 base／hot-tail 的路徑；full reconcile 還有 base replace 後移除 tail 的跨檔步驟。因此不能把 candle 與 feature 分成互不相認的鎖。停止整個 weekly 雖不會提交半個 Parquet，仍可能留下 partial symbols、暫存檔及 running receipt。本輪優先修正 **等待 deadline 與跨 provider 阻塞傳播**，不聲稱已解決同一 provider 的 full-history/live-tail 競爭；後者仍需完整 universe checkpoint 或具版本驗證的短提交批次。

02:46:50 Bybit **866／867**、2 failed，但最後 symbol 仍新增 request pages，非死鎖。平均 ETA 顯示一秒不適用此長尾。只讀已被 log 點名的三份 footer，BTCUSD／ETHUSD／XTZUSDT row count 與分鐘跨度差 **65／379／1**；這不是精確缺洞位置，但足以拒絕把它們當連續中段略過。仍等當輪 terminal report 說明實際 failed symbols，不能拿前輪報表或最後一行 log 猜測。

### 特徵的新鮮度不能跟著目錄重寫

資料品質 companion 檢查確認：candle-only 批次會重写 endpoint catalog 的 generated_at，但保留前次 feature report；原監控取兩檔最新時間，會把 12 小時前的 features 說成剛更新。改由 feature report 及專屬 `download_summary.historical_features.json` 保守取較舊完成時鐘；catalog 不再是取得證據，無／壞 summary 保持 unknown，僅尚未有專屬 summary 的舊報表沿用其 mtime。兩個 atomic 檔交替發布時不能將新時鐘套到舊證據。主 OHLCV 則單獨讀 `download_summary.json` 的 ended_at，缺失／未來時間不宣稱完成。

真實唯讀投影：OKX features 最近 **9/26 14:26:06 台北**、Binance **14:35:27**；它們不再沿用 9/27 02:30 的 candle-only 時間。逐商品成功 coverage **492／492、574／574**保留，但 freshness 正確為 stale；取得成功與時效是不同維度。修補時 scoped dashboard tests **72 passed**，新增兩 provider 的 catalog-touch、無報表／錯誤專屬 receipt、atomic 交替與未來時鐘回歸。這不是補齊歷史特徵，也尚未是 gateway 部署證明；正式啟用與後續驗收另記。

### 有界鎖等待已啟用，歷史錯誤不隱藏

Binance／OKX 共用窄 `downloader/dataset_lock.py`；仍持有原 inode 的獨佔鎖直到整輪工作／例外收尾，不分割 candle／feature、不刪鎖檔。預設等待 180 秒，`--lock-timeout-seconds 0` 可單次嘗試；負值／NaN／inf 拒絕。依 [Python flock 契約](https://docs.python.org/3/library/fcntl.html#fcntl.flock) 使用非阻塞嘗試與 [monotonic clock](https://docs.python.org/3/library/time.html#time.monotonic) 截止時間，每 30 秒輸出結構化等待證據；取得前不建立 client／API 或覆寫共享報表。timeout 為非零失敗，不能說該來源已完成。summary 加入 lock wait 與 work wall 的独立欄位，保留舊 elapsed 語義。

Root 獨立驗證涵蓋真實子程序競爭、兩入口 timeout 保全十種既有證據、失敗／成功釋鎖、原 terminal archive 及 Bybit retry／compact status，**160 passed／11.30 秒**；另一組 dashboard／API／projection **253 passed／37.44 秒**。首次較廣測試抓出兩個 OKX 舊 mock 缺新 CLI 欄位，已更新 fixture 後通過，未在 production 加靜默 fallback。仍有全庫既有退休 YAML 缺檔的測試，不宣稱全 repository 通過。

**02:52:12 僅停止舊 intraday 純鎖等待 cgroup**：事前兩個 child `1540634／1540653` 均為 `locks_lock_inode_wait`、CPU 不到一秒；停止後 MainPID0、兩 child 消失，舊 invocation 忠實留下 signal15 失敗，不改成成功。立即以原 unit 啟動新 invocation `0abc25f4f11d4e0783b60da7b953948b`／MainPID1612009；weekly invocation／PID1538145、當沖 PID653393、Discord PID653541 不變，timer 保持 active。

Bybit 新 intraday **02:52:13.437→02:52:36.616，867／867 updated、867 pages、23.179 秒**，證明其他 provider 的長歷史鎖不再永久扣住它的整輪恢復。OKX／Binance 的正式 180 秒終局與後續 timer 重排待觀察。這仍不是每個來源的 live-tail 隔離完成；同一 provider 在 weekly 期間仍會明確 timeout。

原 weekly Bybit 02:47:38 終局 **864 updated／3 failed、10,680 pages、step wall860秒**；實際失敗 BTCUSD／ETHUSD／XTZUSDT 均為 `10016 svc error: Get kline failed`，舊三檔保持未覆寫，失敗清單已進該 run 的 `bybit_source` archive。[Bybit 官方錯誤表](https://bybit-exchange.github.io/docs/v5/error) 將 10016 定義為 server error；既有 retriable set 漏了它，故 `--max-retries 8` 原不生效。現僅加入既有同頁、有上限、指數退避與 shared limiter 的重試集合；超限照樣 raise，參數／無效 symbol 錯誤不重試，沒有空資料冒充成功或跳頁。11 個新 mock cases 通過，未觸發額外全量下載，三份歷史缺口仍未解除。

02:52:30 正式 snapshot 經現有 timer 自動載入資料時鐘修補，localhost provider API 已顯示 features **stale／age 約44,784及44,223秒**，candles current；不必重啟資料 writer。OpenBB 自然新輪亦於 **02:53:07** 開始，invocation `4c4f0a5543d54f48902cace6c9c7f716`，其重用效果另依當輪完成收據驗收。

**正式等待 deadline 與排程恢復已驗證：** Binance／OKX 分別 **180.000158／180.000136 秒** timeout，02:55:13 整輪以 `completed_with_failures`／exit1 結束；原 `OnUnitInactiveSec=1min` timer 在 **02:56:14** 自動再啟動 invocation `f001ac3241a24073a2f8eeabe27b3c0e`。第二輪 Bybit 又於 02:56:33 完成 867 updated，沒有靠手動重啟才能繼續。provider 同源鎖衝突仍在、未放寬一致性 gate；新期限避免它變成其他 provider 的無限期停擺。

額外 reviewer 用故障注入找出並修正三個時鐘邊界：先逐一拒絕未來 feature report／summary 時間再取 min；candle `failed/repair_required` 即使有 ended_at 仍 degraded，只有 `updated/skipped_up_to_date` 完整數量證據才 complete；feature proof unknown 時必須 waiting／unknown ETA，不能用舊 stage 成功數宣告完成。相關 dashboard **80 passed**，與 compact benchmark 合跑 **96 passed**。正式 provider API 02:57:31 同時保留 feature stale／waiting_schedule 與 candle current，沒有把資料問題藏起來。

### Compact status：刪掉無用讀取，但不誇大速度

`build_dashboard_snapshot(include_order_fill_rows=True)` 保留原預設；僅公開 `tw_status` 選 False，略過原本會讀入後又被 sanitizer 清掉的 orders／fills。沒有調小 `maximum_event_rows`，沒有刪 signals、events、marks、latency、完整生命週期或歷史帳本。current／historical、ledger-date flag、缺失 ledger、健康及 payload counts 均有回歸，sanitized 內容完全相同。

固定 9/24 session／now／小型 metadata、私有已驗證索引、四個 fresh workers ABBA，每 worker first＋4 steady；禁止全帳本重索引，不 flush 正式或 OS cache。首次 raw JSON SHA 不同，追查只為 mapping key order；canonical SHA 保留所有欄位與 array 順序後 **20／20 相符**，來源 ledger／position metadata fence 也一致。第一份未驗 parity receipt 原封保留為 `accepted=false`，未被成功測試覆寫。

第二份已驗 parity 的結果反而 **first A714.14／B806.46 ms；steady A78.01／B102.62 ms**。不宣稱本修補已改善整體延遲。首次 orders/fills 約111–119ms 內含97–106ms 的 Polars 首次初始化，B 只是把初始化移到 marks；steady 原 orders/fills 實際約0.17–0.22ms，主要剩選定 session 的 marks／benchmark 解析。這個歸因避免把移動初始化成本冒充消除成本。

可重跑入口：[benchmark_tw_day_trade_compact_status.py](../scripts/benchmark_tw_day_trade_compact_status.py)；[輸入、方法、完整命令與 hash-pinned 原始結果](../artifacts/benchmarks/tw-compact-status-order-fill-reproduction-20260927.json)。正式重跑只需換新 output 路徑；既有 receipt 不覆寫，若索引失效直接拒絕，不偷偷退回全來源掃描。

**02:57:18 只重啟公開唯讀 gateway**，新 invocation `4bff366b6d444c15884d6838117a8626`／PID1633546；觀測 health-ready **4.502 秒**。當沖／Discord／weekly PID **653393／653541／1538145**不變，[部署收據](../artifacts/benchmarks/gateway-compact-status-deployment-20260927.json) 留前後狀態。4條 route×10次 localhost 0 errors，當沖 status steady p50 **1.474ms**；first observed **2.160ms**已可能被背景預熱，不是冷首請求，更不能與前輪495ms直接算加速率。[正式 HTTP 小樣本](../artifacts/benchmarks/dashboard-compact-status-deployed-20260927.json)。新 invocation journal 無 error／traceback／fatal／exception。

### OpenBB：已證明重用，但整輪來源成本仍待優化

自然 invocation `4c4f0a5543d54f48902cace6c9c7f716` 的 business receipt **02:53:07.859662→02:58:11.959000，304.099 秒**：41 個 `reused_verified_paths`，SEC `rebuilt_schema_groups` 1.803 秒，FRED仍 deferred；NPORT真正重用 **0.105 秒**，query publish由上輪 **83.490→7.086 秒**。新增260segments、compacted **5,569,939**／pending **1,747,703**，精確再推進32,768files，stale／failed／deferred_failed全零。

本輪各資源階段 high／max／OOM零；systemd MemoryPeak **2,744,274,944 bytes**、MemorySwapPeak **0**、CPU **187.718秒**。這證明本輪沒有先前的記憶體壓力事件，不保證所有未来輸入。全輪 **219.732→304.099秒**反而較慢：stale contract audit **135.458秒**、unassigned source load **76.467秒**、segment build **79.801秒**。部分 timing 是 nested，不能全部相加；本輪與 weekly重疊，不用不同負載算整體優化比。下一階段應量測這些來源稽核／載入瓶頸，保留source-fence與corruption fail-closed。

[原始完成收據](../artifacts/benchmarks/openbb-l1-20260927T025812-4c4f0a5543d54f48902cace6c9c7f716.json)，SHA `af3625b56aa40bb51968e5628c817b91c1dadcce2fc79c2dab3b8ca5f87b06e5`；原 timer下一輪 **03:28:51**，未手動重跑大型壓實或改資源上限。[正式部署後全服務清單](../artifacts/benchmarks/service-coverage-20260927T0258-compact-deployed.json)仍覆蓋 **57 services／42 timers／3 paths、零timer finding、6 localhost GET均200**；當沖 degraded、TAIFEX blocked、隔日沖／Shioaji／OpenBB waiting、當次資料監控 degraded，並非全綠。未驗證外部IPv6、Windows冷開機或全資料歷史完整性。

**03:01 最後 root 合併回歸：21 模組 490 passed／1 skipped，63.85秒；Ruff／範圍 diff-check通過。** 跳過的 browser條件與既有退休 YAML 缺檔均不視為通過，沒有全库／所有裝置瀏覽器驗收聲明。下一項已完成唯讀 ownership review：compact status 的 marks／benchmark 完整 facts 沒有現成同契約 rows cache；history曲線projection已丟欄位，不可硬套。可評估延伸既有 `_projected_ledger_session_frame`，但必須保留全部欄位、來源順序、fallback policy及同尺寸改寫／舊session追加／坏shard回退等證據。目前尚未實作，不新建平行帳本。

**03:05 瀏覽器驗收發現必須保留的失敗：** 單跑原3個browser曾全過，但原 FinMind／provider兩個測試把任何 `PlaywrightError` 都標成環境不可用。現收窄為僅 Chromium executable 缺失可跳過，新增3個導航故障注入案例確保互動例外不被吞掉。收窄後 5 passed／1 failed：FinMind 的「盤中歷史」正常 `.click()` 在 visible／enabled／stable 等待30秒後逾時；不是未安裝瀏覽器。其他兩個頁面互動與3個故障注入均通過。仍在診斷 layout／headless frame因素，未以 force、dispatch_event、拉長timeout或恢復skip偽造通過。前述490／1是當時歷史結果，**不能當作現在所有瀏覽器測試已通過**。

網站非browser回歸另驗 **105 passed／11.53秒**；核心三個服務仍 active／NRestarts0。後續 OpenBB 唯讀 EXPLAIN 顯示目前 query 已用 `idx_l1_tasks_compaction_order` 與 DeferredSeek，沒有 temp sort，不能把强制index或CTE materialize當未驗證解方。最小待測候選是把同輪已取得的 L0 footer proof 傳入既有 `compact_parquet_files`，在pre/post精確file identity fence下消除選中32,768檔的第二次PyArrow footer解析；不是跨輪 stat-only快取。全L1衍生footer稽核仍保留，source journal無變動不等於衍生檔未毀損。此候選本輪只有review、未實作。

## 2026-09-27 02:02 起：完成後資源證據、長期紀錄邊界與來源恢復清單

**目標仍 active。** 本輪沿用正式服務與既有五分鐘監測，沒有新增 daemon、重啟核心服務、放寬完整性 gate 或刪除歷史資料。資料品質流程只用於 Yahoo／Binance 的有界來源證據檢查，不把 process success 當來源完整。

### 完成後仍可追查資源壓力

OpenBB business receipt 現投影固定五階段／十二個整數資源欄；缺漏、bool、負數、非有限或越界值不轉成零。必須匹配 invocation、開始時間及完成時間；active／未完成／未來 receipt 不能冒充已完成工作。採樣最大值明確標為 `observed_sample_maxima`，不是全程峰值或當前 cgroup；只有所有五點合法且單調時才提供首末 counter delta，不相加 nested timing。

另列 `resource_pressure_findings`，與 business state 分開。[02:12:54 全服務收據](../artifacts/benchmarks/service-coverage-20260927T0216-resource-pressure.json) 精確匹配 OpenBB `5ec7aed9a4a64130b9f8fbe5064610b6`，同時記錄 **completed** 與 **17,130 memory.high events、採樣 swap 272,252,928 bytes**，不偽造失敗或健康。57 services／42 timers／3 paths，沒有 timer finding；既有長期 trend 直接保留這份投影。

### 防止長期 JSON 超過 journal 單行邊界

02:10:21 正式自然樣本新增上述欄位後，已是 **47,437 bytes**，完整 JSON 尚未破裂，但只距本機預設 48 KiB 邊界 **1,715 bytes**。[systemd 官方 LineMax 說明](https://raw.githubusercontent.com/systemd/systemd/main/man/journald.conf.xml) 明確表示 stdout stream 超長時會被強制切成不同記錄；因此不提高 journald 記憶體上限，而是在現有唯一輸出端作有界 framing。

原事件 ≤32 KiB 保留原格式；較大樣本輸出多筆 `all_service_runtime_sample_chunk`，每筆 ≤32 KiB，帶共同 `sample_id`、原始 payload SHA-256、零起始 `part_index`、`part_count` 與 `payload_fragment`。讀取時必須收齊同 ID 的全部唯一 index、依序串接 fragment、驗證 SHA，再解析原 JSON；缺片／重複／digest 不符不能當作完整或較小的 service inventory。所有欄位、中文與資源證據都保留，無截斷或新資料儲存層。序列化異常留明確 failure event，不讓測速失敗拖垮狀態頁產生器。

以真實 47,437-byte 樣本 SHA `a6c6505f74fa373cd8b40b319b24935aefc3940cca49211c019332ae23659a38` 做 100 次微測：原序列化 p50 **0.150 ms**，framing p50 **0.291 ms**、p95 **0.372 ms**；三筆長度 **17,856／17,973／16,390 bytes**，重組逐位元組一致。這是每五分鐘增加約 0.14 ms 的局部 CPU 成本以保護證據，不宣稱網站變快，也不是磁碟持久性測速。自然 journal 收齊驗證待下一樣本。

**02:15:25 自然 journal 已驗收：** `sample_id=fc9ac5f3919d4a4f960e882c0900310a` 的三片皆收齊，長度 **17,856／17,949／15,803 bytes**；重組原始 **46,896 bytes**，SHA `8020c4db951f51d71d1ecad98fc029c190d192ea1e75bc304d58acf2de84f588` 相符。解析後完整 **57 services** 與 OpenBB stage resources 均存在，沒有改 journald 或重啟 sampler。這證明本輪傳輸／重組，不保證無限保留；仍遵循既有 journal rotation。

### Yahoo／Binance：區分歷史失敗、現在成功與尚未證明

Yahoo 最近 630 個來源缺口為 **618 lagging_skip＋12 metadata_invalid**。daily stale gate 會略過超 14 日無新 bar 的標的，但系統仍有週修，不能稱「所有 618 永久未重試」；其中 52 個可見 9/20 checked-through。真正查到的恢復清單缺陷是週修 fresh listings 成功時沒有 union 本機保留歷史，並重寫 manifest，令日更從 local 檔重建時 617 個 name 退化成 code。既有精確-code 描述可分類 326 為股票／ETF 範圍外商品、206 supported、86 unknown；不把 unknown 當已下市、不刪本機檔，也不把 12 個上游空回覆改為不存在。修正與驗收另記下文。

Binance 9/20 weekly backfill 只保留 **573 updated／1 failed**，沒有該輪 symbol-level archive；無法從既有 log／step receipt 還原失敗 symbol。最新 574 candle／574 feature updated 與五份小報表 bytes／SHA 相符，但都為 `tail_only=true`，不足以解除歷史 head 完整性疑問。現行主流程已在 raise 前保存 archive；本輪只補 `main()` 層 mock 回歸，驗證失敗 symbol／message、五份 artifact hashes、後續 tail 更新不覆寫舊 archive，未假造現存執行故障、改 health 或觸發全量 backfill。

**Yahoo 修補已實作與測試：** 非 strict US repair 以 cached＋local tracked＋fresh discovery 保全恢復 universe，`--limit` 只限本輪 scheduled records、不縮完整 manifest；targeted 原分支的保留行為補回歸。缺名稱僅依 exact-code 的既有描述補入，不猜 `_DL` alias、不改 Yahoo symbol／market 或有效原名稱；daily cached／local 同步套用，避免被 canonical 商品政策排除的本機檔隔天以無名稱狀態重新入排程。所有 Parquet 保留，未以缺行情認定下市。`strict_no_fallback=true` fresh-only 契約保持不變，失敗仍 fail closed。

另補來源保護：`verify_us_history_head` 只可延伸 metadata／asset contract 正確的原檔，不能將 `source=cboe` 等檔案合併後改寫為 Yahoo；不符者仍走既有 metadata-invalid 的獨立重建流程，上游空回覆不產生成功資料。純 mock 測試重現／防止清單縮水、名稱退化、錯誤來源 merge、strict fallback 違約與 limit／targeted 破壞。Yahoo＋status **82 passed**，Ruff／diff-check 通過；完整 manifest 名稱補全＋filter 七次中位數 **80.90 ms**，不是下載測速。**630 gaps 並未宣稱已補齊**：12 筆上游空回覆、206 supported／86 unknown 歷史狀態與下一週修的實際 receipt 仍待驗收。保留原 02:25:38 weekly timer，未提前觸發下載。

截至本輪上述修改，15 個相鄰測試模組 **651 passed／23.10 秒**（不含另跑的 Yahoo／status 82）；不是全庫、全來源完整性、公網瀏覽器或冷開機驗收。Gateway／當沖／Discord PID **1332337／653393／653541** 保持 active／NRestarts=0。OpenBB SQL persisted-hash 修正仍待 02:18:58 自然輪；Crypto 批次 scan 仍待 02:30，自然重疊工作的鎖等待與資源影響另作觀察。

### OpenBB persisted SQL 遷移自然輪

InvocationID `abc754246d5042baa27b829e5985cbe9`，**02:18:58→02:22:41 exit 0**，process wall **222.321 秒**、CPU **171.258 秒**；261 新 segments、0 stale／failed，pending **1,813,239→1,780,471**／compacted **5,504,403→5,537,171**，精確推進 32,768 source files。舊 invalid SQL catalog 依原安全 gate 被一次重建，42 published views＋1 deferred；query **83.490 秒**，NPORT **68.323 秒**，SEC **8.615 秒**。

本輪仍有 **16,991 memory.high events**、MemoryPeak **2,953,314,304 bytes**、MemorySwapPeak **3,834,703,872 bytes**；max／OOM／kill 皆零。不能把 migration 成功當記憶體問題已消失。完成後另做純唯讀 exact SQL/catalog 驗證，**43 entries 全通過，0.172 秒**；這是現在持久化 signature 可重用的證據，不是下一輪已實際重用。下一自然輪應觀察 unchanged NPORT reuse、query 成本與 memory.high，未再手動跑一輪高壓工作。

[正式原始收據](../artifacts/benchmarks/openbb-l1-persisted-sql-migration-20260927.json) SHA `c8eb749ef105ec41a29ed909d6cb234513e4e620e5605be4f4fdafd4cd71be74`。[工作期間 localhost 小樣本測速](../artifacts/benchmarks/dashboard-under-openbb-20260927T0220.json) 四條 route、每條 10 次、concurrency 1 全無錯誤：revision／當沖 status／OpenBB status／資料 summary steady p50 **1.264／1.805／1.446／1.947 ms**。當沖 status 第一個觀測 **197.856 ms** 明確保留；這是 warm/local 小樣本，不是公網／冷快取／全程 peak-pressure SLA 或整體架構加速率。

### 有界 Bybit 修正進行中與暫時 timer 保護

原 weekly Bybit 在來源鎖內曾跑 **19,115 秒**、日表 **14 秒**；daily tail 約 24 秒，主要差異不是鎖本身。已查到 full reconcile 只要 earliest 晚於 launch，就從 launch 重新抓到今天，重複數年已保存的中段。候選將查詢限於缺頭＋尾端修訂，但不能直接套原 cutoff merge（會刪中間），也不能以 footer 的 rows==span 假定中間完整：RAM 反例 `[2,3,3,5,6,7,8]` 同時重複／缺一分鐘可騙過 footer 計數。

**02:22 暫停且僅暫停 `stockagent-registered-data-backfill.timer`**，仍 enabled、service inactive／PID0，避免原 02:25:38 自然輪載入未完成驗證的 full-history 改動；未停止 intraday、Crypto、核心服務或正在跑的資料工作。候選需 exact minute continuity、完整保留中段、fresh timestamp wins、空／失敗回覆不造成功與 tail-only 回歸，通過後由本輪恢復原 timer；恢復證據另記，不能把暫停中的計時器宣稱正常。

**02:33:17 已恢復原 timer，立即按 persistent overdue 排程啟動原 weekly service。** Timer 保持 enabled／active，service invocation `3e1199c9d5574855aa33e02dd499bf33`、MainPID 1538145，自 02:33:18 執行；沒有改 calendar、provider budget、source lease 或服務資源上限。程式 SHA `f02518d1a4118a105cc6bee102720b4b410e7b33cdb20844482ddd70bab35375`，新測試 SHA `8662f9cd47852692f8d2e9e42fde8e46c02f1ff366ac1fbf86ceecaedf4fa0cd`，恢復前 root 重跑 head-tail／hot-tail／terminal receipt／buffer **40 passed**，Ruff 通過。

正式修補只在原始 UTC/ns 日期**非空、無無效值、全部對齊整分鐘、每一相鄰差為一分鐘、首末與 metadata 一致**時跳過中段；同一次 logical frame 留給 merge，不重讀完整 OHLC。不能先轉成秒字串再檢查，否則 `.500`／nanosecond 錯誤會被抹掉。失敗退回原全段修復，空頭每次仍可重試，tail-only 路徑保留。合併改先 `unique(keep=last)` 再 sort，保留中段、同 timestamp 新資料勝出，也省一次無用排序；[Polars sort 文件](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.sort.html) 的預設並不保證相同鍵保序。[Bybit 官方 Kline 契約](https://bybit-exchange.github.io/docs/v5/market/kline) 的時間範圍、單頁 1,000 筆與未完成分鐘邊界維持。

`test_bybit_reconcile_windows.py` 用固定 **5,004 列**完整 truth tape 比對 full-refetch 與 split 的**每一列／欄位**，mock API **6→2**；另測實際小型 base＋hot-tail 合併、偏秒 launch、少量／無效 head、tail 故障零寫入、footer 重複抵銷缺口與次秒時間戳。不是實網 3 倍加速證明。中段證據是本機時間序列完整性，不是重新詢問上游所有歷史修訂；顯式 `--refresh` 仍完整重建。真正有洞的來源仍可能持鎖較久，沒有宣稱整個 weekly lease 已有 180 秒上限。初始正式 Yahoo precheck 已用 **16,398** records；整輪來源結果仍待完成。

### Crypto 批次通知自然輪

InvocationID `d0829d5d23804564813100f5fe189167`，**02:30:00→02:32:26 exit 0**，process wall **146.437 秒**、CPU **291.560 秒**、MemoryPeak **8,508,563,456 bytes**、MemorySwapPeak **0**。來源 lease **139.444 秒**，canonical publish **102.703 秒**仍是主要成本；沒有因同步通知改善就宣稱全流程變快。

本輪新發布 `bybit-20260926T183218064395883Z-l0-penguin-5c622919e29861b7`，changed files **877**、new objects **17**／**231,864,499 bytes**。來源鎖外 scan subprocess **3.226 秒**，內部 receipt **3.097915 秒**，精確證明 `batch_object_paths=true`、pending **1→0**、`scan_request_acknowledged`。這是新物件自然輪，不是 ABBA 同來源對照；不同 queue／主機與掃描內容不能拿前輪 34.949 秒直接算固定加速率，peer convergence／release verification 也未由這份 scan receipt 證明。

整體真實狀態為 **completed_with_report_deferrals**：core／publication 已 published，transport 已 request_acknowledged，OKX／Binance reports completed；coverage 因放鎖後新的 Bybit raw writer PID 1536497 活動而 `deferred_raw_writer`，沒有把混合快照當成完成。原始收據：[Crypto natural batch](../artifacts/benchmarks/crypto-refresh-batch-natural-20260927.json)，SHA `a808f511b9d30f737b3f3c7369b180307fffbf1bc7b839ed684150821ffff6f3`。這個分域使新的即時下載能繼續，不必等報表／通知結束。

**本輪最後合併驗證：22 個測試模組 839 passed／75.15 秒**，Ruff／範圍 diff-check 通過；另補 sampler `main()` 層的分段成功與序列化故障注入，測速失敗不影響既有公開狀態發布。較廣的 crypto-15m contract 檢查仍有既有「已退休 projection-L1 YAML 被刪除」測試失敗，未復活舊設定或隱藏此項，故不宣稱全庫通過。

[02:34:07 收尾服務收據](../artifacts/benchmarks/service-coverage-20260927T0234-restored-timers.json) 覆蓋 **57 services／42 timers／3 paths，零 timer finding**；weekly 正在執行而不是被遺留停止。六個 localhost GET 全 200，但 TAIFEX blocked、當沖 degraded、隔日沖／Shioaji／OpenBB waiting、資料監控 critical 保留。此時當沖 status 單次 **495.057 ms**，高於稍早 warm 小樣本，不能只引用 warm p50 宣稱延遲已好；重負載下的 cache miss／I/O 仍需後續拆解。核心三個 PID／NRestarts 不變，weekly cgroup 02:35 約 37.2 GB；這是當下總量而非 process RSS。下一 OpenBB timer **02:53:07**，需驗 unchanged endpoint 的真正重用；Bybit 新週修仍在跑，沒有全歷史完成或 lease 上限的證據。

## 2026-09-27 01:35 起：消除重複 footer 運算、資源故障分域與同步通知 A/B

**目標仍 active；不以局部改善宣稱全系統完成。** 本輪先用固定 1,024 個 SEC segments 拆解來源驗證成本，再做保留全部安全條件的 single-pass 修正。未刪 L0、來源、帳本或 cold objects，未改交易規則、資料完整性門檻、3 GiB hard limit 或 30 分鐘 cadence。

### 同來源 footer 歸因及修正

先前正式 source metadata audit 從 38.375 秒變成 67.122 秒，同時實際 read bytes 多了 186,040,320 bytes；來源數量、頁面快取與 I/O 都不同，不能將全部差額歸因於新增 proof。新的 [bounded attribution receipt](../artifacts/benchmarks/openbb-l1-footer-proof-sec1024-20260927.json) 實讀每個 footer，證實舊路徑在 ordinary audit、proof audit、physical identity 各序列化一次相同 Arrow schema。benchmark 實驗快取不是生產快取，未直接部署。

正式 `schema_identities()` 現在只從**同一已開啟 footer** 計算一次 Arrow digest，共用於 manifest integrity 與 physical identity；沒有跨檔快取，也不採信 manifest 傳入的 digest。原 physical tree、ordered leaves、全部 KV bytes、未知版本 fallback、rows／bytes／Arrow 檢查及 stat 前後圍欄皆保留。`_mark_stale_segments` 的 proof 分支不再先執行重複的普通 Arrow audit；非 SEC／未 opt-in 維持原路徑。

固定 ordered contract `fb1022d99d4e6a1735da49252c7e187b1d5cf6684e675643488610446268b706`，fresh worker **old→single→single→old** 的完整 footer 步驟 wall 為 **0.427576／0.403271／0.416936／0.456178 秒**；同組均值 **0.441877→0.410103 秒（−7.19%）**，CPU **0.444394→0.412628 秒（−7.15%）**。每輪 read_bytes=0，這是 warm page cache 量測，不是磁碟冷啟動。Diagnostic serialization 次數 **3,072→1,024**，logical-leaf 檢查仍各 **20,461** 次。所有 physical identity 與 hash-pinned 歷史基準 `a3b33abb0206b8012f509adc341cfc888607df7de3f59784063d13c496417275` 一致，沒有只跟自己比較。

收據：[single-pass ABBA](../artifacts/benchmarks/openbb-l1-footer-proof-singlepass-sec1024-20260927.json)，SHA `b8333c6c03c1445b3f22d60abc61d6e6071f2adbf27c7afd4b5cffeb7a01da5b`。不能將這個 7% 外推成整個 OpenBB 或整體網站改善；下一正式輪另驗收。正式五個 resource snapshots 也新增累計 `process_cpu_ns`／`monotonic_ns`，後續能用相鄰差值區分 CPU 工作與 wall 等待，不再僅憑整輪 CPU 猜測，也不把 nested 計時重複相加。

### 正式輪發現另一個 catalog 重用問題

原 registered service InvocationID `5ec7aed9a4a64130b9f8fbe5064610b6`，**01:43:50→01:47:37 exit 0**，process wall **227.234 秒**、CPU **175.309 秒**。260 新 SEC segments、0 stale／failed／deferred-failed；pending **1,846,007→1,813,239**，compacted **5,471,635→5,504,403**，仍精確處理 32,768 來源。完整原始收據已保存在 [single-pass 正式輪](../artifacts/benchmarks/openbb-l1-singlepass-formal-20260927.json)。

本輪 source audit **57.176 秒**（stage CPU 差 **46.358 秒**）、source load **25.527 秒**、build **49.734 秒**，但 query publish **89.886 秒**，其中 **etf.nport_disclosure=78.586 秒**、SEC 分群 **5.817 秒**。query 前 memory.high=0，之後 **17,130 events**；max／OOM／OOM-kill 仍為 0，cgroup peak **2,953,314,304 bytes**，query 結束 swap current 約 272 MB。查詢階段 CPU 差 **44.473 秒**，額外 read_bytes **3,947,634,688**。這輪有真實回收／交換壓力，不能宣稱記憶體根除或整輪加速。

唯讀 manifest 顯示 NPORT **24,792 段／1,592 Arrow schemas／34,133,672 rows**，本輪來源沒有變更，卻與其他 41 個 published endpoints 一起被標為 rebuilt。因此優先調查 catalog 完整性判斷／SQL reopen round-trip 導致的不必要全量重建，**不**直接擴大記憶體上限、不取消 SQL tamper 驗證，也不把 unchanged NPORT 當成新資料自然成本。下一節或後續收據另記其修正驗收；本次時窗保留原 30 分鐘排程。

**後續已用 12-schema 小樣本確認並實作根因修正：** DuckDB writer 看到的 UNION SQL，與 checkpoint／close／reopen 後的文字表示不同；2 組 schema 的原測試恰好穩定，12 組則連續三輪都誤判不可重用。正式 42 個 views 的唯讀 metadata 比對也只有 SEC SQL hash 不符，NPORT hash 本身正常。沒有把「增加括號」當成可安全刪除的字元；不能用 strip 或模糊比較放寬 SQL 完整性。

現在新 temporary catalog 先 checkpoint／close，再 reopen 單次讀取 persisted SQL，**只替本輪 CREATE 的 allowlist views** 記錄確切 SQL SHA；重用 view 維持舊的已驗證 signature，不能被重簽洗白。第二次 checkpoint／close 後，再以完整 catalog／view set／exact SQL signature 驗證結果對照本輪 expected catalog，最後保留來源 stat fence 才 atomic replace。新增連續三輪、12 群、未變其他 endpoint、每輪 footer audit、SQL tamper、缺 catalog row、final validation／checkpoint 故障及尾端 source race 回歸；失敗保留原 public DB／receipt。

這是修復「下一輪誤判全庫不能重用」，不是修改查詢語意或略過驗證。**舊 public DB 的失配 hash 不會被原地洗白，首次套用仍需一次正常重建；之後才應看到未變 endpoint 重用。** 本輪未再手動啟動高壓工作；下一自然 OpenBB timer 當時為 **02:18:58**，正式多輪重用與記憶體／延遲效果仍待驗收。[正式 process／code-hash sidecar](../artifacts/benchmarks/openbb-l1-singlepass-runtime-20260927.json) 另記 MemorySwapPeak **3,872,559,104 bytes**；它與 MemoryPeak 是不同時間的峰值，不能相加當同一瞬間 working set。

### 穩定性修正

獨立 code review 發現原本 footer audit 的 `except Exception` 會吞掉 `MemoryError`（包含 ArrowMemoryError），將健康 segment 誤標 stale 並解除 member association。現在資源不足直接中止該輪，保留原 metadata／catalog／receipt；新 segment build 的 Python／DuckDB OOM 同樣不記成來源 batch 的 durable failure backoff。真正缺檔、損壞、schema 漂移與一般可隔離來源錯誤仍維持原處理。新增六個 fault-injection cases 覆蓋普通／SEC proof 及 builder，沒有藉製造真實主機 OOM 測試。

### 同步通知候選與服務現況

`retry_packed_syncthing_scans.py --batch-object-paths` 新增 **default-off** CLI，receipt 的 `requested_scan_policy` 只表示請求策略；idle 不冒稱執行。Crypto timing validator 僅允許固定 Bybit／唯一 receipt 命令的單一尾端旗標。ABBA 前正式 caller 尚未啟用，後續接入情形如下。已核對本機 Syncthing v2.1.5 與其[官方多個 sub 參數實作](https://raw.githubusercontent.com/syncthing/syncthing/v2.1.5/lib/api/api.go)；實際 ABBA 必須量測 API 回應，不能沿用 mock 請求數當速度證據。

**01:49:33～01:49:37 真實 ABBA 已完成。** [Dry run](../artifacts/benchmarks/syncthing-scan-abba-dryrun-20260927.json) 後固定已發布 Bybit head SHA `334fe14818e71cc3384679e83b84157f581ab7d23cefc5e4f32dbb7dff8c0b0d`、manifest、18 objects＋inventory，共 19 個相同路徑；每輪實際 POST **21／3／3／21**，48 次全 HTTP 200。所有 head／manifest／source stat／code 前後一致，pending 未被新增或清除。[主收據](../artifacts/benchmarks/syncthing-scan-abba-20260927.json) SHA `9dc1089102ae6c3787365a0ca5e87f144451cc2f011fb3e4d69a5c946ce19624`；[runtime sidecar](../artifacts/benchmarks/syncthing-scan-abba-runtime-20260927.json) 留存前後相同的 Syncthing PID 399686／invocation／config digest，未記錄憑證。

| Warm notification 量測 | 單筆路徑 | 批次路徑 |
|---|---:|---:|
| 兩輪 API acknowledgment 秒數 | 0.553043／0.554205 | 0.496773／0.501090 |
| API 秒數中位數 | 0.553624 | 0.498932 |
| 含 benchmark guard／mount check 的 trial wall 中位數 | 1.066900 | 0.817120 |

API 部分約減少 **55 ms／9.88%**；trial wall 包含額外的逐次 head／pending 觀察成本，因此不能把其 23% 差異全部當作 production 效益。**沒有**把前次新物件 publication 的 34.949 秒拿來跟 warm scan 的 0.5 秒比較；未重新 hash payload、未測第一次掃描／queue overhead、未證明 peer convergence。工具有每 trial 90 秒及整個持鎖區 390 秒 deadline，外層可穿透內層失敗處理，逾時釋鎖與受控退出都有測試。

ABBA 後僅將 **Crypto `drain_transport`** 接上該旗標；其他 caller、CLI／library defaults 不變。回傳 receipt 必須精確證明 `requested_scan_policy={batch_object_paths:true}`，缺漏、false、數值 1 或額外欄位皆保留 `transport=pending`，不撤銷已成功的 `core/publication=published`。未重跑完整來源發布來製造熱路徑結果；**02:30 下一自然 Crypto 輪的 first-publication 實際效果仍待驗收**。

[01:39:11 全服務 receipt](../artifacts/benchmarks/service-coverage-20260927T0140-proof-review.json) 仍為 **57 services／42 timers／3 paths**、零 timer finding；Binance backfill failure、Yahoo 12 failed／618 lagging_skip 等缺口保留。Gateway／當沖／Discord PID **1332337／653393／653541** 持續 active／NRestarts=0，未由本輪重啟。這不是來源完整、Windows 冷開機或下一開盤成功的證據。

**01:55:31 收尾盤點：** [全服務收據](../artifacts/benchmarks/service-coverage-20260927T0156-singlepass-batch.json) 仍覆蓋 57／42／3、零 timer finding，OpenBB 精確匹配本輪 invocation 與七階段、最慢 `query_view_publish=89.886s`；六個 localhost API 均 HTTP 200，但 TAIFEX blocked、當沖 degraded、隔日沖／Shioaji／OpenBB waiting、資料監控 critical 仍忠實保留。來源缺口及正式新政策的下一輪驗收未完成。

所有本輪改動合併後，**14 個 test modules 共 503 passed／23.15 秒**，Ruff 與範圍 diff-check 通過；包含 resource failure、12 群 SQL round-trip／tamper、scan deadline／釋鎖、queue／transport policy 與 runtime evidence。這不是全庫測試、公網 browser 測速、Windows cold boot 或下一交易日驗收。保留 02:18:58 OpenBB 與 02:30 Crypto 自然排程，不新增 daemon、不停止主服務。

## 2026-09-27 01:17～01:28：OpenBB 安全分群正式驗收與逐輪測速串接

**目標仍 active；本輪確認記憶體壓力改善，沒有宣稱整輪加速或全系統完成。** 上節 default-off 候選先通過實際 production policy 的完整列驗證，再僅替既有 registered service 加上 `--schema-grouped-sec-views`。installed unit 與 repo template 逐字核對、`systemd-analyze verify` 通過；保留 30 分鐘 inactivity timer、32,768 source／輪、2 threads／1 GB DuckDB、2,816 MiB soft／3 GiB hard、40 分鐘 timeout、idle I/O 與交易／尾盤 guard。未重啟 Gateway、Discord 或當沖引擎，也未刪 L0、packed、來源或帳本。

### 真正 production policy 的完整資料列比對

`benchmark_openbb_l1_view_binding.py` 新增 `--candidate-policy production`，在同一批 footer audit 建立正式 `ViewSourceProof`，fresh worker 呼叫真正 `_schema_grouped_view`，不接受 fallback 冒充候選成功。candidate-only 必須提供 hash-pinned 原版 baseline receipt，且核對 endpoint、cutoff、37,429 個有序 segment contract、預期列數、DuckDB 版本與完整比對標記；來源或實作在前後改變則失敗。不能 candidate 與自己比較就稱等價。

固定 cutoff `2026-09-26T16:07:21.537870+00:00`、contract SHA `71b7e48ad4c0863b23f404f8db0ae51ea748763e605659664663433e65120677`，本輪 production candidate 形成 **12 個 physical＋KV 群**；**4,288,596 列／21 欄** 的名稱、順序、型別及 row-multiset SHA 均與原版相同，摘要 `8d50cedfefa717d2111839aa3ca38d0dfff3517222aa6c0e3fdd41f57176499f`。收據為 [production 全列驗證](../artifacts/benchmarks/openbb-l1-view-binding-sec37429-production-fullrows-20260927.json)，SHA `0196c78c8452cf4cdc9b73597f5bed07361f60dc2eec35f6944afd57b2d9bb8c`；明示 `comparison_mode=hash_pinned_historical_baseline`／`paired_timing_comparison=false`。

安全 footer proof **22.103 秒**在 parent；worker 的正式 group plan＋bind **1.961 秒**（其中 plan 1.822 秒），binding 累計 RSS peak **477,794,304 bytes**。完整列驗證另外 **51.470 秒**、整個 worker RSS peak **2,127,122,432 bytes**。這些不是整個服務或 3 GiB cgroup 的量測；保留 Nice 15、idle I/O、1 GB DuckDB／2 threads、單 worker／240 秒等待及 8 GiB address-space cap，寫入資料 bytes 為 0。未跑同時段原版速度 A/B，不能拿歷史 baseline 耗時算固定加速率。

### Registered service 首輪

InvocationID `ea10c3a1ee664c579501cfa9a4232f45`，**01:19:47→01:22:35 exit 0**；process wall **167.905 秒**、CPU **155.025 秒**。261 個新 segments、0 stale／failed／deferred-failed、42 published views、原有 1 個過多 schema 的 deferred view 保留；pending **1,878,775→1,846,007**、compacted **5,438,867→5,471,635**，均精確變動 32,768。SEC action=`rebuilt_schema_groups`；source audit 仍 `incremental/journal_verified`，不是跳過來源稽核。

| 完整步驟 | 前一輪原路徑 00:54 | 本輪安全分群 |
|---|---:|---:|
| Stale／衍生 footer 稽核 | 38.389 秒 | 67.149 秒 |
| 待分派來源載入 | 28.369 秒 | 33.895 秒 |
| Segment build | 55.777 秒 | 56.223 秒 |
| Query-view publish | 12.711 秒 | 7.358 秒 |
| 完整 process wall | 139.369 秒 | 167.905 秒 |
| cgroup MemoryPeak | 2,953,314,304 bytes | 2,663,292,928 bytes |

正式收據五個階段的 **memory.high／max／OOM／OOM-kill 全為 0**；query publish 前後 peak **2,318,249,984→2,663,292,928 bytes**。發布後 anon 約 206 MB、file 約 1.93 GB，是結束採樣，不能冒充峰值當下分布。相對之前可重現的 query-stage memory.high 壓力，本輪未再觸發；但來源不同、快取／主機負載不同，**不把這個表當受控 A/B，也不掩蓋整輪較慢**。新增 proof 的實際成本、冷／熱 footer 行為及剩餘重複 schema 計算仍需拆解，下一自然輪前不恢復 15 分鐘 cadence。下一 timer 當時為 **01:54:32**。

證據：[啟用前 unit／receipt](../artifacts/benchmarks/openbb-l1-safe-groups-before-20260927.json)、[正式輪完整 stage／resource／journal／hash](../artifacts/benchmarks/openbb-l1-safe-groups-formal-20260927.json)。回退只移除 unit／template 的分群旗標並 daemon-reload；下一次受保護的正式輪會依原 signature 規則重建原 reader，不需刪資料或擴記憶體上限。需要回退的條件包括新資料不等價、來源核對失敗、OOM／持續 high 或實測資源／延遲影響核心服務；單輪成功不是永久穩定保證。

### 長期證據與其他並行工作

OpenBB receipt 新增 actual start／finish、systemd invocation、requested policy/revisions、failed／deferred counts。統一 service audit 現要求**同時**匹配時間 ±10 秒與 exact 32-hex invocation，再投影 7 個不重疊主階段；nested timings 不合計成 wall，無效／負／非有限／超出該輪耗時的證據不產生測速。既有五分鐘 runtime sampler 直接保留此投影到輪替 journal，無新增 daemon；舊 receipt 或前輪成功不能將新工作改成正常。另補 completed label 與 failure/deferred 計數的一致性驗證；缺漏、型別不合法或矛盾計數顯示 unknown 並列 finding，不被完成字樣蓋掉。

Syncthing object scan 批次化已實作為 `batch_object_paths=False` 的**尚未啟用候選**：有界重複 `sub` 參數，全部 object requests 成功後才依序 manifest、head；64 paths／16 KiB encoded-query 邊界、>256 full-object fallback、pending generation CAS 與失敗留存皆保留。固定 18 blobs＋inventory＋archived head 的 mock 同路徑集合從 22 POST 減為 3 POST；只是請求數量證據，不是假造真實網路耗時改善。正式 CLI／Crypto caller 尚未接旗標，沒有用 mock 當上線驗收。

**01:24:29 全服務重查：** [coverage receipt](../artifacts/benchmarks/service-coverage-20260927T0124-openbb-safe-groups.json) 仍覆蓋 57 services／42 timers／3 paths、零 timer finding。OpenBB 已為 `matched_run_step_timings`，精確對應 invocation、requested policy、SEC `rebuilt_schema_groups` 與 7 階段，slowest=`stale_contract_audit=67.149s`。六個 localhost API 200；TAIFEX blocked、當沖 degraded、隔日沖／Shioaji／OpenBB waiting、全資料 critical 未改成正常。Gateway／當沖／Discord PID 1332337／653393／653541 仍 active／NRestarts=0；這是程序連續性，不是核心延遲沒有波動的證明，也不是公網／瀏覽器／券商端到端測速。補強矛盾計數驗證後的 [01:28 最終 coverage](../artifacts/benchmarks/service-coverage-20260927T0128-openbb-final.json) 再次確認同輪 7 階段與上述健康分界。

獨立唯讀風險盤點提出後續三項，不以重啟或放寬 gate 掩蓋：(1) TAIFEX 9/18 cycle 仍被 28 個未平 futures strategies 阻擋，須核對該日真實 flatten／價格證據後隔離 replay；(2) Yahoo 最近 daily 雖 exit0，仍 618 個超 14 日 lagging_skip＋12 metadata_invalid／empty-download 缺口，先分類歷史下市／可交易／非股票並做 canonical 小樣本 repair，不能刪歷史股票；(3) FinLab 01:13:23 unit 已 success，卻受 42.42 MB<50 MB reserve 與 required `broker_transactions` oversized gate 阻擋，required_pending=196；需有界來源方案，不能降低 reserve 或移除 required 定義。這些是不同健康層的未解事項，非 OpenBB 改動可解決。

本輪相關 12 個測試模組最終合併 **415 passed／11.98 秒**，包含新增 unit guard 與矛盾收據測試；Ruff、py_compile／範圍 diff-check 通過。不是全庫測試、冷開機或下一開盤驗收。

## 2026-09-27 01:04 起：Crypto 提交與同步通知分域；OpenBB 安全分群候選

**目標仍進行中。** Crypto v5 保留完整來源 lease／逐檔 hash／atomic head gate，但 canonical publisher 新增僅供明確單一發布、繼承來源 FD、唯一結果 receipt 使用的 `--defer-scan`。D-primary 的發布先 durable enqueue；來源子程序退出、owner lease 關閉之後，立即呼叫既有 retry 工具的 `--dataset bybit`，以獨立 300 秒 subprocess 等待預算送出通知，不再持來源鎖等待 Syncthing。非 D-primary 維持既有立即通知行為；沒有新增輪詢服務或停用即時下載。物件仍先於 manifest／head 掃描，API acknowledgement 不代表遠端 convergence。

短 queue metadata lock 與長 scan execution lock 分開；掃描完成後以 generation UUID、inode／ctime 與 receipt bytes 比對後才清除通知。掃描期間新到的通知保留待辦，不被舊掃描吃掉；錯誤、逾時、損壞的舊 receipt 仍走既有保守重試。獨立審查另發現 `queue_after_publish=False` 原本可能被忽略，現明確拒絕回報「durably queued」；head 若已提交，保留 outcome-unknown 與冷資料，不能反推沒有發布。一般 scan 失敗則保留 `core_state=published`／`publication_state=published`，另外標 `transport_state=pending`，不混同來源失敗。

正式驗收使用原 `stockagent-crypto-training-refresh.service`，InvocationID `f413fcd27fe942139a55fda122f51932`，**01:04:28→01:06:57 exit 0**；既有 CPU／I/O／memory caps、交易時窗、150 秒來源 subprocess 預算及下載器 180 秒等待皆未改。完整程序 wall **148.567 秒**、CPU **278.008 秒**、cgroup MemoryPeak **19,872,944,128 bytes**。服務退出後 cgroup 已不存在，因此沒有該輪終端 memory.events／峰值時 anon-file 比例證據；不能把 root cgroup 值當服務值，也不能宣稱記憶體或 CPU 成本改善。

| 同輪 v5 步驟 | 秒 |
|---|---:|
| Funding | 2.070 |
| 394 商品 daily materialization | 33.315 |
| Venue features／Bybit audit | 2.875／2.630 |
| Canonical cold commit＋durable enqueue | 60.574 |
| 來源鎖外 Syncthing scan subprocess | 34.949 |
| Coverage／OKX／Binance reports | 7.932／1.425／1.459 |

來源 lease **102.427 秒**，較上次 147.248 秒短；這兩輪更新量不同（changed files 2,063→877），**不是同來源 A/B 加速率**。因果驗收是：**01:06:11** core 記 published、owner 放鎖，registered Bybit 同秒取得來源鎖（wait **66,794 ms**），**01:06:44** 完成更新與整輪 exit 0，早於 scan subprocess 約 **01:06:46** 結束。下載器沒有被停止或改排程。此輪整體 `completed`、core／publication `published`、transport `request_acknowledged`，三份 reports 全 completed；保留 future busy／source-change／pending 的獨立狀態，不保證每輪跨來源報表都一致。

release `bybit-20260926T170609435190253Z-l0-penguin-6d73dcd3aa97c831`、inventory SHA `6d73dcd3aa97c831114e5cc58b1fe120333970e2e342a181b55edc093df1928f`，4,677 檔／22,372,806,444 邏輯 bytes、3,800 reuse／877 changed；新增 17 物件／230,039,351 bytes。輪後核對 head manifest hash、snapshot ID、inventory digest、建置 metadata 指紋一致，Bybit pending scan 已清除；沒有再把全部物件重 hash，也沒有宣稱 peer convergence。完整 receipt、程式 SHA、systemd、journal 與核對結果保存在 [正式分域驗收](../artifacts/benchmarks/crypto-refresh-separated-transport-20260927.json)。

後續仍可優化 scan 呼叫成本：本機 Syncthing v2.1.5 的[官方 API 實作](https://raw.githubusercontent.com/syncthing/syncthing/v2.1.5/lib/api/api.go)接收多個 `sub`，可另測「物件批次→manifest→head」的三階段請求；本次刻意沒有混入這項改動，先量測解耦本身，不把潛在效益算成已實作。

OpenBB 候選已接入 `--schema-grouped-sec-views`，**預設 false、正式 unit 未啟用**，僅允許 canonical 非 Hive 的 SEC filing_headers。分群證據取自同輪完整衍生 footer 稽核，包含 physical／logical tree、全部 KV bytes、Arrow schema、版本與實際 DuckDB reader policy；未知表示退回原 reader。所有來源在發布前後重新核對 stat identity，manifest／非首檔變更或毀損不能發布新 DB，reader policy／SQL 與來源證據也綁定重用簽章。安全候選的 111 個相關回歸通過，含 INT96、Geo、Hive、版本未知與 race；沒有拿前節 Arrow-only benchmark 冒充本版本正式測速。已開 footer 的 1,024 次小樣本 proof CPU 約 0.194 秒，舊 Arrow 指紋約 0.011 秒，表示安全 proof 有額外 CPU 成本；仍須完整正式階段 wall／memory.high／peak 驗收。01:00 前最後一輪原路徑自然服務 00:54:14→00:56:33 exit 0，wall 139.369 秒、CPU 139.733 秒、peak 2,953,314,304 bytes，0 stale／failed、pending 1,878,775，不能宣稱記憶體壓力已根除。

**01:08:52 全服務再盤點：** [coverage 收據](../artifacts/benchmarks/service-coverage-20260927T0110-separated-transport.json) 覆蓋 57 services／42 timers／3 paths，零 timer finding，Crypto 已精確匹配當次 invocation、分列 9 個步驟及來源 lease，no-op 只採納本次真正執行的 transport timing。六個 localhost API 皆 200；TAIFEX `blocked`、當沖 `degraded`、隔日沖／Shioaji／OpenBB `waiting`、全資料 `critical` 仍未消除，backfill Binance 失敗與 Yahoo 630 個來源缺口保留。Gateway PID 1332337（00:58:04 起，由本輪外變更）、當沖／Discord PID 653393／653541 均 active／NRestarts=0，本輪沒有重啟這三項核心服務。Intraday 下一輪 01:07:44 取得 source lock 只等 5 ms，01:08:56 exit 0，未見殘留 lease。

最終 10 個相關 test modules 合併 **336 passed／9.02 秒**；Ruff 與範圍內 diff-check 通過。包含來源 FD／commit 後故障、通知併行 CAS、D-primary 消失、transport timeout／pending、no-op 防舊測速與 OpenBB 分群失配／race；未跑全 repository、冷開機或下一個開盤驗收。下一階段是 OpenBB 安全候選的有界正式資源驗收，及 scan 批次化獨立量測；既有來源缺口、TAIFEX 歷史部位及整體資料健康仍列未解。

## 2026-09-27 00:11～00:36 Crypto 建置／發布共用來源交易與分域驗收

**目標持續進行中；這次修復來源競態，不代表所有服務已達極限。** 先前 Bybit refresh 只在步驟前掃描程序，下一秒啟動的分鐘下載仍可改寫建置輸入；不相關的 OKX／Binance writer 又會讓 Bybit 工作整體延後。現在沿用 catalog 的 `bybit_source_publish.lock`，funding、daily materialization、venue features、Bybit audit 與 canonical publisher 共用同一個來源 lease，沒有另建下載系統。跨交易所 coverage／OKX／Binance 報表移到 lease 外，仍保留完成／來源變更／失敗狀態，不把尚未完成的報表當成已完成。

父程序用 `pass_fds` 交接同一 open-file description；publisher 驗證 FD、regular file、device／inode 與 catalog path，避免重新取得自己持有的鎖而死鎖。owner 只 close、不提早 `LOCK_UN`；步驟逾時終止整個自有 process group，仍存活且持 FD 的後代會繼續阻擋寫入。核心 subprocess 共用 **150 秒等待預算**，registered writer 原 180 秒等待不變；這不是 metadata I/O／fsync 的硬上界，也不能保證所有主機負載下都完成。

共用 `scripts/bybit_refresh_inputs.py` 建立 Bybit 原始分鐘／funding 輸入的 path／device／inode／size／mtime／ctime metadata 指紋。canonical publisher 在初始來源盤點後、最終完整來源穩定檢查後及 atomic head 前驗證建置指紋；它是**額外代際契約**，沒有取代既有逐檔內容 hash、packed objects、完整來源穩定性與發布 gate。正常成功結果另寫唯一原子 receipt，保存 snapshot ID／inventory SHA；若 head 已提交但 scan 或 receipt 回報失敗，refresh 明確記 `publication_state=outcome_unknown`，不猜測「未發布」。發布後若來源又變更，保存實際 release 證據並標 `needs_reconciliation`。未變來源的 no-op 也在 lease 內判斷，重用 release，但不把舊步驟時間冒充本次測速。

### 正式單輪驗收

先通過 `scripts/mount_packed_d_cold.sh --check`、確認沒有 Bybit writer／OpenBB compaction，於 **00:32:19** 執行一次既有 `stockagent-crypto-training-refresh.service`；沒有重啟交易、Discord 或 gateway，也沒有停掉 intraday timer。InvocationID `955ba231842945d9baf3cab7738a5f37` 於 **00:34:54 exit 0**，完整程序 wall **155.316 秒**、CPU **235.633 秒**、cgroup MemoryPeak **18,796,761,088 bytes**。一次發布中途採樣的 anon 約 48 MB、file 約 16.18 GB，high／max／OOM／OOM-kill 當時為 0；這不是終端或峰值時的完整記憶體分類，不用它宣稱記憶體成本已改善。原 24 GiB soft／32 GiB hard 設定未改。

| 同次完整 subprocess 步驟 | 秒 |
|---|---:|
| Funding | 1.217 |
| 394 商品 daily materialization | 33.623 |
| Bybit venue features | 2.270 |
| Bybit audit | 1.770 |
| Canonical cold publish（含 scan） | 107.500 |
| 跨來源 coverage report | 5.161 |
| OKX／Binance reports | 1.203／1.392 |

核心 lease **147.248 秒**，receipt 為 `core_state=published`／`publication_state=published`；394 商品日物化與 funding 各 0 failed，`source_receipt_hashes_match=true`／`materialization_current_to_raw=true`。release 為 `bybit-20260926T163402958843835Z-l0-penguin-fde248f397dbb99a`，inventory SHA `fde248f397dbb99a46444ba26cdfdafb2d9a4cc67acc81cb5e70244d855a0ad4`：4,677 檔、22,372,100,509 邏輯 bytes、2,614 reuse／2,063 changed，新增 18 個同步物件／348,668,748 bytes。輪後核對 current head、manifest inventory SHA、manifest 的建置 metadata 指紋均與當次 receipt 一致；Bybit pending-scan receipt 已清除。這不是遠端 Syncthing convergence 或所有歷史 release 的完整重建驗證。

同時啟動的 registered intraday 工作只讓 Bybit 等待 **104,127 ms**，OKX／Binance 仍並行更新；Bybit **00:34:46** 取得 lease，**00:35:11** 完成分鐘更新，整輪 **00:35:12 exit 0**。沒有藉停用下載來製造穩定來源。但 coverage report 在 lease 外偵測到來源變更，因此總狀態保留 **`completed_with_report_deferrals`**；OKX／Binance reports completed。這是核心已發布、跨來源報表仍待重做，不能合併宣稱全部完成。

當次 receipt 保存在 [修復後單輪](../artifacts/benchmarks/crypto-refresh-shared-lease-20260927.json)；[修復前 receipt](../artifacts/benchmarks/crypto-refresh-before-shared-lease-20260927.json) 保留原 `deferred_source_changed`。publisher 的唯一結果路徑記在前者 `publication_result_path`，原冷 head 歷史／來源／帳本未刪。00:29 唯讀盤點 [service coverage](../artifacts/benchmarks/service-coverage-20260927T0029-crypto-review.json) 仍覆蓋 **57 services／42 timers／3 paths**、零 timer finding、六個 localhost API 200；TAIFEX blocked、當沖 degraded、全資料 critical 仍是未解業務狀態。

**00:41 監測串接驗收：** [新 coverage 收據](../artifacts/benchmarks/service-coverage-20260927T0041-post-crypto-lease.json) 已將 Crypto 分類為 `matched_run_step_timings`，核對當次 systemd invocation 後顯示 8 步驟，最慢 `bybit_cold_publish=107.5s`；安全白名單另外保存 core／publication／report 狀態，不暴露 command 或憑證。舊代際、空／no-op、未知腳本、非有限／負耗時與超出該輪時窗的步驟不產生測速宣稱，整體 `completed_with_report_deferrals` finding 仍保留。全清冊仍 57／42／3，六個本機產品 HTTP 200、零 timer finding；TAIFEX blocked／當沖 degraded／全資料 critical 及 backfill 失敗、Yahoo 630 個來源缺口未消除。Gateway 當時 PID 1276854（00:26:56 已啟動）、當沖／Discord PID 653393／653541 均 active／NRestarts=0；不把 gateway 與先前舊 PID 當作連續程序。Intraday 下一自然輪 00:36:12→00:37:25 成功，再下一輪 00:38:26 的 Bybit 鎖等待為 6 ms，沒有觀察到殘留 lease。

最終 8 個相關 test modules 合併 **185 passed**，Ruff、py_compile 與範圍內 diff-check 通過。回歸涵蓋借用 FD、來源前後變更、同 mtime 改寫、atomic-head 前 veto、no-op、子程序 timeout、已 commit 後回報失敗及逐步 timing 證據；全庫測試、冷開機與下一個開盤不在這次驗收範圍。

**剩餘風險：** 147.248 秒已接近 150 秒預算，且持鎖等待仍增加 Bybit 更新延遲；本輪不是延遲改善的 A/B。下一步要分離「來源交易已提交」與後續 transport scan，或使用可驗證 immutable input snapshot 縮短持鎖時間，而非放寬安全預算／停止來源／宣稱永不再失敗。coverage 的跨來源一致視圖與來源繁忙後有界重試也仍需另外處理。

## 2026-09-27 00:24～00:30 OpenBB query-view 分群候選：完整列等價，尚未進入生產

**此節只有唯讀來源的隔離 benchmark 與安全設計，沒有將分群候選接入正式 compactor。** 正式 `_publish_views` 仍使用原本的全檔 `union_by_name=true`，既有 30 分鐘 timer、32,768 檔／輪、2 執行緒／1 GB DuckDB 預算、2,816 MiB soft／3 GiB hard、交易窗口及來源稽核均未因本候選改動。00:21:36 的自然 compaction 已於 **00:23:49** 正常結束；00:24:26 確認 service `inactive`／MainPID=0 後才啟動完整資料列比對，沒有與該正式輪次重疊，也沒有啟動／重啟任何服務。

唯讀 SQLite manifest 顯示 `regulators.sec.filing_headers` 當時有 **37,429 段／12 種 Arrow schema**。候選將相同 schema 的路徑分群，各群用 `read_parquet(..., union_by_name=false)`，再以 `UNION ALL BY NAME` 合併；群按其第一個來源的原始順序建立。這減少 binding 時重複聯集 footer 的工作，並非把資料列刪掉或預先裁切。原／候選 SQL 分別約 **4,566,387／4,567,147 bytes**，字串尺寸幾乎不變；僅 CREATE VIEW 與 LIMIT 0 就能重現高峰，支持主要熱點是 metadata binding，而不是完整資料列 `fetchall`。[DuckDB schema 合併文件](https://www.duckdb.org/docs/lts/data/multiple_files/combining_schemas)也明示 `union_by_name` 會增加記憶體消耗。

先前 1,024 段／7 schema／117,591 列的 fresh-process ABBA 已通過完整欄序／型別與 SHA-256 列多重集合比對：原 binding **0.120／0.088 秒**、候選 **0.012／0.011 秒**。完整 37,429 段的 bind-only ABBA 亦欄序／型別相同，原 binding **10.305／5.568 秒**、候選 **0.347／0.394 秒**；該收據刻意標 `not_fully_verified`、`row_verification_performed=false`、列等價為 null，不能拿它當完整資料列證明。

本次再固定 `created_at <= 2026-09-26T16:07:21.537870+00:00`，並強制 ordered source-contract SHA 為 `71b7e48ad4c0863b23f404f8db0ae51ea748763e605659664663433e65120677`，確保自然輪次新增 segment 後仍比較**同一份 37,429 段**。每個來源 footer 的 row count／Arrow schema／byte size 均核對 manifest，所有檔案身分在每個 trial 後再驗證。原／候選兩個 fresh worker 各掃完整 **4,288,596 列**；21 欄的名稱、順序及 DuckDB 型別相同，canonical DuckDB row JSON 的 SHA-256／multiplicity 多重集合摘要均為 `8d50cedfefa717d2111839aa3ca38d0dfff3517222aa6c0e3fdd41f57176499f`，收據 `state=verified`。這驗證已選來源的列多重集合，不承諾自然列順序；canonical L1 本來不依賴任意 shard 排序。

| 同來源隔離量測（DuckDB 1.5.5） | 原全檔聯集 | schema 分群候選 |
|---|---:|---:|
| CREATE VIEW binding | 4.608679 秒 | 0.308549 秒 |
| binding 完成前程序 RSS 累計峰值 | 2,234,548,224 bytes | 267,288,576 bytes |
| 完整列雜湊驗證額外耗時 | 45.657467 秒 | 38.505394 秒 |
| 包含完整列驗證的程序 RSS 累計峰值 | 3,584,716,800 bytes | 2,039,844,864 bytes |

**binding 峰值與 full-row 驗證峰值不可混用。** 上表是隔離 in-memory DuckDB，不是完整服務、不是正式 catalog clone，也不是 cgroup 峰值；沒有把約 15 倍局部 binding 差異外推為整輪加速。Worker 並行數為 1、Nice=15、啟動使用 idle I/O、DuckDB 2 threads／1 GB、每 trial timeout 180 秒，另設 `RLIMIT_AS=8 GiB`；地址空間限制**不是** 3 GiB RSS cap，也沒有改動正式服務 hard limit。兩 worker 記錄 `write_bytes=0`，宿主 MemAvailable 前／後約 92.80／93.17 GB；這只是該次旁路負載紀錄，不是正式服務資源下界或長期保證。

收據與工具：

- [1,024 段 ABBA 與完整列比對](../artifacts/benchmarks/openbb-l1-view-binding-sec1024-20260927.json)。
- [37,429 段 bind-only ABBA](../artifacts/benchmarks/openbb-l1-view-binding-sec37429-bindonly-20260927.json)。
- [37,429 段完整列 AB 比對](../artifacts/benchmarks/openbb-l1-view-binding-sec37429-fullrows-20260927.json)，檔案 SHA-256 `a9cde96818c63fcc997d388861026c30209b814efbd6c07046d1f12fa3dea511`。
- [唯讀 benchmark](../scripts/benchmark_openbb_l1_view_binding.py)／[focused tests](../test/test_benchmark_openbb_l1_view_binding.py)：**4 passed**，涵蓋 scalar 欄位重排／缺欄／NULL、INT96 反例、來源 receipt 不符，以及 frozen selection cutoff／SHA gate；Ruff、diff-check 通過。

以下為本次完整比對的相同參數，輸出改用新檔避免覆寫既有收據。執行前須確認服務已完成，且下一次 timer 有足夠空檔；若歷史 segment 已失效／來源集合變更，SHA gate 應拒絕，不可刪除 gate 來製造通過：

```bash
systemctl list-timers stockagent-openbb-l1-compaction.timer --no-pager
if [[ "$(systemctl show stockagent-openbb-l1-compaction.service -p ActiveState --value)" != inactive ]]; then
  echo 'deferred: registered compaction is active' >&2
  exit 3
fi
ionice -c 3 bash -c '
  source scripts/runtime_env.sh
  run_fintech_python scripts/benchmark_openbb_l1_view_binding.py \
    --max-segments 37429 \
    --created-at-before 2026-09-26T16:07:21.537870+00:00 \
    --expected-contract-sha256 71b7e48ad4c0863b23f404f8db0ae51ea748763e605659664663433e65120677 \
    --sequence ab --trial-timeout 180 \
    --output artifacts/benchmarks/openbb-l1-view-binding-sec37429-fullrows-recheck.json
'
```

**尚未滿足正式 promotion gate；不能只依 Arrow fingerprint 或 primitive allowlist 上線。** 獨立 review 已重現：同一 `timestamp[ns]` Arrow 指紋下，INT96 與現代 Parquet timestamp encoding 被 DuckDB 讀為 `TIMESTAMP`／`TIMESTAMP_NS`；原 true 可保留 nanosecond，分群 false 可能截斷。另一例具有相同 Arrow 指紋與實體 leaf schema，卻因 footer `geo` metadata 讓 DuckDB 讀為 `BLOB`／`GEOMETRY`。另外，group field ID 不同仍可能通過 `ParquetSchema.equals()`；因此不能把 leaf equality 當完整 tree 相同。非首檔腐毀時，原 true 會在 CREATE 失敗，false 可能拖到 SELECT 才失敗；甚至 `COUNT(*)` 也不足以驗證 schema。這些是候選必須補齊的邊界，不因 SEC 目前完整列相等而忽略。

最小安全實作方向，留待後續獨立生產變更及正式資源驗收：

1. 復用現有 `_mark_stale_segments` 每檔已開啟的 `ParquetFile`，同次取得完整 physical／logical schema tree、ordered leaf signature、全部 metadata KV bytes 的保守 identity，以及 path／segment／row／Arrow contract 與 `(device,inode,size,mtime_ns,ctime_ns)`；只保留摘要，不持有每檔 metadata 物件。此方向不需要重讀舊 37k 段 footer；新建段可沿既有 validation 產生 proof，若暫不擴 receipt，僅補讀新建的有界批次。原全段來源／衍生稽核不可省略。
2. Identity 必須綁定 DuckDB／PyArrow 版本與 reader policy，包含會影響型別的 options；全部來源須證明是 canonical 非 Hive 路徑。PyArrow 完整 group tree 暫可由去掉 object-address 首行的版本釘定表示取得，但它不是穩定跨版本序列化；格式未知應退回原路徑，不可假定相同。保守 metadata key 必須包含 bytes 值，不能只比 key 名稱或忽略 GeoParquet。
3. 同 physical＋KV identity 的各群以首檔作代表，再用代表路徑的原 `union_by_name=true` 推導 expected merged schema，與 grouped SQL 的欄序／型別比對。所有 paths 的 proof 在發布前後重驗；missing／corrupt／stat drift 必須在 atomic replace 前 fail closed 並保留舊 DB／receipt，不能當一般 fallback 成功。無法建立可信分群 proof 才保留原 true 行為。
4. 將 SQL policy revision、reader contract 與分群 identity 納入 view-input/reuse signature，避免只因 paths 未變就沿用不相容 SQL；保留原子替換、完整 view／SQL 簽章核對及原有 `schema_variants > 4096` deferred 語意。補 INT96、GeoParquet、group tree、Hive、非首檔變更及故障恢復測試後，先對已完整驗證的 SEC 作有界正式驗收，再看連續自然輪的 `memory.high`／OOM／peak／wall，不能提前宣稱壓力已消失。

## 2026-09-26 23:55～09-27：資源預算、測速判讀與行情失聯邊界

**目標仍進行中**：所有已註冊服務的效能、健壯性和穩定性須逐項量測、修正及驗收；以下局部成果不代表全系統健康或已達效能極限。

OpenBB 新排程後已觀察到 32 輪終端紀錄，全部 0 stale／failed；首輪後 31 次皆 pending −32,768、compacted +32,768，待辦由首輪後 2,992,887 降為 1,977,079。收據為 `artifacts/benchmarks/openbb-l1-timer15-followup-20260926T2356.json`。最新 23:46:59～23:49:21 輪 wall 141.766 秒、CPU 158.357 秒，peak 2,954,686,464 bytes；同一 invocation 在 segment build 後 `memory.high=0`，query-view publish 後為 1,183，I/O full stall 5.568 秒。這是新增的資源壓力證據；舊收據未保存 max／OOM／peak 時 anon 與 file 分布，不能將未知填零。有一輪提前啟動原因未確定，因此不把全部 32 輪歸因於 timer 自然觸發。

依上一節已列的回退準則，00:00 左右將 timer 與監控 ETA **恢復 30 分鐘**，只部署 timer，沒有覆寫其他 dirty service 模板；`systemd-analyze verify` 通過、timer active/waiting，當時下一次排程 00:19:39。查得 `_publish_views` 的舊 catalog 與新 DB 連線均漏套 `--threads 2 --memory-limit 1GB`，現補齊 connect-time 設定，保留 atomic replacement、路徑／SQL 簽章核對和 3 GiB cgroup hard limit。[DuckDB 官方說明](https://duckdb.org/docs/current/guides/performance/oom) 指出部分配置可能繞過 buffer manager，因此 1 GB 設定不是整個程序或檔案快取的硬上限；仍須由正式階段收據及 cgroup 驗收，不預先宣稱 high 事件消失。

全服務測速工具原本將已核對同次 run ID 的逐步收據仍列為 `last_completed_process_wall_only`。現新增 `matched_run_step_timings` 及 `recorded_steps_only` 摘要，顯示有效步驟數與最慢步驟；不推算未記錄步驟、不把步驟總和當 wall，不接受過期／空／非有限／負耗時。相關盤點／監控／啟動測試 **113 passed**。00:01:37 的新盤點 `artifacts/benchmarks/service-coverage-20260927T0003-step-timings.json` 覆蓋 57 services／42 timers／3 paths，其中 3 個有同次逐步量測、35 個只有完整程序 wall、17 個只有資源採樣、2 個未量測。三項最慢步驟分別是歷史 backfill 的 OKX 65,726 秒、daily 的 Yahoo 1,293 秒、features 的 Binance 2,127 秒；它們是各自最後一次已核對工作，不是目前所有服務的即時延遲。

同次六個 localhost 產品 API 均 HTTP 200、零 timer finding；TAIFEX `blocked`、當沖 `degraded`、全資料 `critical` 仍保留。業務缺口包括 crypto refresh `deferred_source_changed`、歷史 backfill `completed_with_failures`，以及 daily 同次 Yahoo 630 個未解來源。核心 gateway／當沖／Discord PID 653543／653393／653541 自 14:35 起未變且 NRestarts=0；這是連續程序證據，不代表資料完整性或開機恢復驗收。

TAIFEX 唯讀追查已用 9/18 capture `25ca9ab737224b7da9b54f9c4f5cc40c` worker 0 的 323 個分片、528,492 筆 BidAsk 確認：TMFJ6 最後接收於 09:24:41.259184，整個 worker 最後於 09:24:41.290769，尾端缺口 15,637.740816 秒，舊 manifest 卻在 13:45:19 記 complete。28 策略合計絕對值 115 口 TMFJ6 尚未平；該期貨到期日 10/21，不能用已有且核對原始 SHA 的 9/18 選擇權結算價 47,116 代替平倉。保留 bootstrap settlement gate，沒有修改帳本。現有 180 秒停流 watchdog 已處理「收到後停流」，另查出「從未收到第一筆 BidAsk」漏洞；安全修補必須有同一必要期貨的有效即時 Tick 作開市證據，完全無行情與平日休市尚缺可驗證 TAIFEX 日曆，不得用 TWSE 假日檔推斷。

加密貨幣 refresh 的 9/26 22:30 輪，funding 13.663 秒、daily materialization 38.902 秒；Bybit intraday writer 在 22:30:38 取得既有 `artifacts/daily_downloader/bybit_source_publish.lock`，22:31:12 完成，與建置重疊，2,143 檔輸入 metadata SHA 前後改變。`deferred_source_changed` 正確阻止後續特徵和冷發布。02:30／16:30／22:30 timer 正常，9/26 02:58:14～03:00:23 仍有完整成功；但分鐘 writer 約 60 秒工作／60 秒空窗，全鏈曾需 80～129 秒，僅掃描程序會有反覆延後風險。refresh 前半段尚未共用此 source lock；後續修復需有界鎖定或 immutable input snapshot，且發布子程序重入同鎖會死鎖，不可直接讓父程序持鎖到 publish。22:30 的日表與 02:58 的 venue 特徵已不同代際，舊 audit 的 hash-match 不能當目前一致性證明。本次只定位根因，沒有重建或發布該資料。

**00:04:58～00:07:21 正式有界驗收：** 手動啟動一次既有 OpenBB registered service（InvocationID `2a55c281097d4d35a2d6486ed94e0291`），沿用 32,768 檔上限與所有 guard，沒有重啟核心服務。退出成功、261 個新 segments、pending 1,977,079→1,944,311，精確消化 32,768；stale／failed／deferred-failed 皆 0，L0 未刪。完整程序 wall 143.740 秒、CPU 150.011 秒；正式階段收據 stale metadata 41.431 秒、unassigned source 27.732 秒、segment build 62.442 秒、query publish 8.891 秒。新增 peak／anon／file／max／OOM／OOM kill 欄位已在正式收據出現，未知檔案保持 null。**query publish 仍 high 0→805，peak 2,953,904,128 bytes，沒有證明峰值改善；max／OOM／OOM kill 皆 0。** 查詢結束後 anon 約 336 MB、file 約 407 MB，不代表峰值時的分布。此輪資料與前輪不同，不能用 CPU／high 下降推導 A/B 加速率。保留 30 分鐘 timer；重啟 timer 的一次性 `OnActiveSec=20min` 仍使下一觸發為 00:21:35，並非 00:37。正式監控 API 已顯示 30 分鐘輪間隔、低信心 ETA 與交易窗口延後假設。後續需再定位 query bind 的瞬間記憶體配置，不能把補預算設定當作已根除壓力。

**行情 watchdog 實作：** 必要期貨依實際訂閱清單決定，包含 data-only worker 0，options-only worker 保留原有首筆等待。只有同一必要期貨的正價格／正量、非試撮／非暫停、當前 session／交易日且 exchange-to-receive 不超既有 `stale_ms` 的首筆 Tick 才啟動固定 BidAsk 期限；其他合約 Book 及後續 Tick 不重設期限，收到必要合約 Book 後只按最後 Book 計時。盤前／週末／到期 13:30／跨午夜夜盤與舊事件／未來事件均有測試，collector 單檔 **41 passed**；無新增 callback 同步 I/O。程式已接線，沒有 API login 或 capture 重啟；仍待下一次正式 capture 驗收。Tick、Book 均為空的情況及歷史 115 口未平繼續列未解，不用推測行情或修改帳本掩蓋。

本次 OpenBB 正式有界驗收另保存於 `artifacts/benchmarks/openbb-l1-resource-budget-20260927.json`，含正式收據 SHA、完整階段資源、journal、guard、程式 SHA、核心 PID 與排程證據，`resource_pressure_resolved=false`、`same_source_ab_benchmark=false`。本輪最終整合：`test_audit_service_latency_coverage`、`test_openbb_l1_compaction`、`test_data_monitor_dashboard`、`test_boot_recovery_contracts`、`test_shioaji_microstructure` 共 **188 passed／9.13 秒**，Ruff、py_compile、變更範圍 diff-check 通過。沒有執行全 repository 測試，也沒有宣稱歷史缺口、TAIFEX 帳本、下一個交易日或 Windows 冷啟動已驗收。

## 2026-09-26 14:40～14:51 OpenBB L1 待辦消化頻率

最新正式 L1 收據仍有 **3,025,655** 個待壓實來源；`source_audit.mode=incremental, reason=journal_verified`，本輪來源 JOIN 0 秒，但既有衍生檔 metadata 驗證 **28.301 秒**、待分派來源載入 **22.100 秒**（其中 metadata **17.650 秒**）、新 segment 建置 **45.065 秒**，皆是真實工作，不能刪驗證換取假速度。`journalctl -u stockagent-openbb-l1-compaction.service --since '2026-09-26 12:00:00' -o short-iso` 可重看最近五輪：每輪待辦精確減少 **32,768**、0 stale／failed、wall 約 **103.4～118.0 秒**、記憶體峰值 **2.3～2.7 GiB**，低於服務的 **2,816 MiB soft／3 GiB hard**；完成時間每隔約 33 分鐘，主要不是被單輪運算佔滿，而是原本 `OnUnitInactiveSec=30min` 的輪間等待。以 3.03 百萬待辦與相同每輪容量，單純排程容量約 93 輪；此估計假設無新來源、無來源回退／提供者限制、無交易時窗延後，**不是**完工承諾。

現只將 OpenBB L1 timer 模板的輪間等待 **30→15 分鐘**，保留每輪最多 32,768 檔、40 分鐘上限、3 GiB hard、`--archive-idle-only`、`Nice=15`／`IOWeight=10`、台股開盤至 13:35 的 ExecCondition 及既有完整來源／衍生驗證。同步更新全資料監控的 ETA 容量常數和文案，使 UI 不再沿用 30 分鐘舊排程；兩項相關測試及啟動契約 **74 passed**，Ruff／diff 檢查與 `systemd-analyze verify` 通過。只部署此 timer，**沒有安裝工作樹中其他 dirty service 模板**。14:48:10 timer 重新啟用時因舊輪完成已超過新等待時間而自然觸發既有 service；當時狀態為 `activating`，保護中的公開 gateway／當沖／Discord PID 分別 653543／653393／653541，OpenBB cgroup 記憶體高水位事件 0，宿主單次 I/O full PSI 10 秒約 3%。此時**尚未有新輪 terminal 收據**，仍須在新輪完成後驗證進度、失敗、資源及下次單調時鐘排程，並持續觀察夜盤與下載器競爭；不能將提頻宣稱為單輪加速或已完成全部 backlog。

新排程首輪已於 **14:50:10 正常退出**，wall **120.227 秒**、CPU **134.565 秒**、memory peak **2.5 GiB**；正式 L1 收據增建 **262** 段，待辦由 3,025,655 **精確減 32,768** 至 2,992,887，`stale_segments=0`、`failed_segments=0`、來源稽核仍為 `incremental/journal_verified`，L0 原始檔未刪。Timer 下一次為 **15:05:48**（該輪完成後約 15 分 38 秒），不是原 30 分鐘。輪後公開 gateway／當沖／Discord PID 與 NRestarts 均未變、gateway `/healthz` HTTP 200；宿主 I/O full PSI 10 秒觀察約 0.21%，這些是本次點狀證據，不能保證下一個交易日或夜盤不會競爭。若後續多輪出現來源下載落後、記憶體高水位、I/O full stall 或高優先服務延遲上升，應回退 timer 至 30 分鐘並重新量測，而非放寬 3 GiB 與交易時窗保護。

正式 `/data-monitor/api/status` 的 OpenBB L1 列已顯示 **15 分鐘輪間隔**、上一輪 120 秒、每輪最多 32,768 shard，`confidence=low` 並明示無新來源／交易窗口延後假設；不是保證完成日期。OpenBB、監控和冷啟動契約擴大回歸 **103 passed**。14:51 唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260926T1451-openbb-timer15.json` 仍為 57 service／42 timer／3 path、零 timer schedule finding、零 failed unit、六個 localhost 產品 HTTP 200；TAIFEX `blocked`、當沖 `degraded`、隔日沖／永豐／OpenBB `waiting`、全資料 `critical`，以及 backfill 失敗／Yahoo 同次 630 缺口均未由提頻消除。

## 2026-09-26 14:21～14:38 指定來源欄位首次請求與並行維護邊界

上一輪欄位位置索引降低了**已載入後**的篩選運算，但來源每約 30 秒變動時，指定來源的第一次請求仍須讀取／解析約 54.6 MB 快照並重建 87,626 列索引，HTTP 曾約 0.9 秒。正式快照現有 233 個資料集，其中 232 個各不超過 512 欄，合計 6,314 列。針對這些小來源，正式 producer 在完成完整 `feature_inventory.json` 原子寫入後，額外產生約 3.8 MB 的**可選、唯讀**來源列投影；投影與原快照 inode／大小／mtime／ctime、完整檔 SHA 及 producer 收據逐項綁定，並在收據原子寫入後才可被 gateway 接受。公開 gateway 只在指定來源且投影包含該來源時使用它，保留全域摘要與來源／分類篩選清單；任何缺漏、竄改、來源換代或超界都退回原完整解析。大來源及無指定來源查詢仍走既有路徑，不減欄、不讓投影成為交易或資料發布權威。

自然 `stockagent-data-refresh-status-snapshot.service` 已產生正式小來源投影，觀測時 3,817,254 bytes、232 來源／6,314 列；來源／投影收據驗證成功，另在同一穩定來源代際逐列比對完整快照，**完全一致**。改後自然輪仍 `Result=success`、87,626 完整欄位；當輪 `feature_stages_ms.receipt` 約 63～75 ms，先前約 37 ms，負載與來源不等，不能當精確淨增成本。新的 `scripts/benchmark_data_monitor_feature_pages.py` 對同一穩定正式代際交錯測未預載索引的完整／小投影路徑，回傳 JSON 相同：完整 **1,066／1,225 ms**、小投影 **85／78 ms**，收據 `artifacts/benchmarks/data-monitor-feature-source-pages-20260926.json`。這是本機 OS cache 未控制的單程序量測，不是公網 HTTP 或 p95；來源換代時工具標為 inconclusive。相鄰 gateway／監控／清冊／影子測試 **238 passed、1 skipped**，最終小範圍 **16 passed**，Ruff、py_compile 通過。

**當時正式 gateway 端到端驗收暫停。** 14:30～14:31 有另一項「當沖隔離帳本原子匯入」維護在 `/run/systemd/system/stockagent-public-dashboards.service.d/90-account-import-maintenance.conf` 加入 `ConditionPathExists=/run/stockagent-daytrade-import-allow`，條件檔當時不存在。Gateway 日誌為外部 SIGTERM 正常停止，14:31:43 的啟動因條件未滿而跳過，並非此投影程式異常退出；先前一次 HTTP 200／66 ms 和兩次約 2 ms 不能構成持續上線證明。沒有移除或繞過這道匯入保護，也沒有重啟交易、行情或匯入服務；維護解除後的驗收列於下段。

14:35:49 維護方解除臨時 drop-in 後，gateway 自行恢復 `active/running`（新 PID 653543），當沖服務亦 `active/running`（PID 653393）；本工作沒有覆寫維護條件。穩定同代際的 6 欄來源正式 localhost HTTP 連續三次為 **106.108／2.417／2.342 ms**、全為 200，版號相同、`matching_total=6`、`read_only=true`、`production_control_possible=false`。後續自然來源換代時，帶舊版號且 `offset=1` 的請求觀測為新 revision、`reset_required=true`、`offset=0`、仍 6 筆完整匹配；請求期間來源簽章穩定。大來源 81,312 筆篩選、無條件首 80 筆、經 Windows Caddy 的 IPv4 HTTPS 健康端點與隔日沖狀態端點另皆 HTTP 200；並行第一次探針約 1～2 秒受到 gateway 冷載與並行競爭，不當作新功能單獨延遲。以上仍是點狀檢查，非長期 p95、公網獨立外部探針、IPv6 或交易資料健康。最終相鄰回歸 **239 passed、1 skipped**；8 MB 投影上限故障會保留完整收據並回到完整解析。正式資料監控 producer 最新輪 `Result=success`、完整欄位 87,626、投影收據仍驗證成功。

14:38 唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260926T1438-source-pages-live.json` 再覆蓋 **57 services／42 timers／3 paths**，六個 localhost 產品皆 HTTP 200、零 failed unit、零 timer schedule finding；但 TAIFEX `blocked`、當沖 `degraded`、隔日沖／永豐／OpenBB `waiting`、全資料 `critical`，註冊 backfill `completed_with_failures` 和 Yahoo 同次來源 630 項（failed 12／lagging_skip 618）仍明確保留。可達性與實際資料／交易完整性繼續分開報告。

## 2026-09-26 14:10～14:18 全資料欄位輸出與篩選 A/B

先以正式、SHA 收據驗證過的完整欄位快照（87,626 列、約 54.6 MB）測試 per-dataset JSON shard：`scripts/benchmark_data_monitor_feature_shards.py` 保留原列順序，檢查 233 個資料集的列皆連續，交錯寫出整份與 shard 串流版本，四次輸出都與正式來源**逐位元組相同**。收據 `artifacts/benchmarks/data-monitor-feature-shards-20260926.json` 的整份序列化約 540～566 ms；已存在 shard 的串流組裝約 45～62 ms，但首次建 shard 自身需約 596 ms 編碼及 51 ms 寫入。**冷路徑沒有端到端收益**；目前只保留唯讀量測工具，沒有啟用持久 shard、略過來源檢查或修改正式 producer。要啟用重用仍需資料集級來源／顯示標籤綁定、跨代際不變證據與崩潰恢復測試。

欄位分頁 API 在已載入的 87,626 列索引中，舊版即使篩選只有 6 欄的來源，仍逐一掃完整 87,626 列。現於同一次 `FeaturePageIndex` 建立時記錄各來源及分類的列位置，篩選只掃較小的候選集合；無篩選的全文搜尋保持原直接掃描。頁面回傳欄位、順序、完整 `matching_total`、修訂版重設、安全唯讀契約不變。正式快照 A-B-B-A 唯讀收據 `artifacts/benchmarks/data-monitor-feature-pages-20260926.json` 中，六個篩選案例皆與舊版完整回傳相等：6 欄來源篩選由約 10.6～15.2 ms 降至約 0.001 ms，81,312 欄的大來源由約 18.6～20.9 ms 降至約 9.9～10.5 ms，小分類由約 7.5～8.4 ms 降至約 0.002～0.003 ms。新索引建立本身約 240 ms；這些是單程序已載入資料的局部時間，不能冒充首次索引建立、公網 HTTP 或瀏覽器 p95。`test/test_data_monitor_feature_pages.py` 新增所有來源／分類／搜尋／分頁交叉驗證；只有需要篩選的要求才載入完整索引，首 80 筆仍可由來源 SHA 綁定的預覽取得。

相鄰公開 gateway／監控／shard 測試 **182 passed、1 skipped**，Ruff、差異檢查通過。只重啟唯讀 `stockagent-public-dashboards.service` 載入新索引；14:17:29 gateway 開始監聽，`/healthz` 及 6 欄來源 API 均 HTTP 200，回應仍為 `read_only=true`、`production_control_possible=false`、`matching_total=6`。首次篩選讀取新 54.6 MB 快照與建索引時單次 HTTP 約 **0.9 秒**，已載入同代際索引後連續三次約 **2.0～2.9 ms**；來源持續更新時首次成本會重現，不能只報熱路徑。從此 WSL 主機經 DDNS／Windows Caddy 的 IPv4 HTTPS `/healthz` 另觀測 HTTP 200、憑證驗證成功、單次約 **422 ms**；這不是獨立外部網路或 IPv6 重測。14:18 的唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260926T1418-feature-filter.json` 仍列 57 services／42 timers／3 paths、六個本機產品 HTTP 200、零 timer finding、零 failed unit；TAIFEX `blocked`、當沖 `degraded`、隔日沖／永豐／OpenBB `waiting`、全資料 `critical`，註冊 backfill 歷史失敗與 Yahoo 630 同次未解並未因 UI 變更而消失。未重啟交易、行情、Discord 或下載服務。

## 2026-09-26 14:02～14:05 資料監控 producer 峰值記憶體

正式 `stockagent-data-refresh-status-snapshot.service` 每 30 秒另起程序；變動輪次要解碼約 **45.4 MB** 的清冊 JSON、重建 **87,626** 欄、寫約 **54.6 MB** 的公開快照。此前在公開欄位已建成後，`InventorySnapshot` 仍持有完整清冊 Python 物件，`raw_feature_inventory` 仍持有另一份 87,626 列，然後才做公開 JSON 預覽與原子序列化。兩份中間資料在這一點已沒有後續消費者。現於原始欄位建成後釋放清冊 payload／選檔清單，於公開欄位建成後釋放原始列；沒有改變檔案發現、Parquet 頁尾驗證、來源 metadata／feature revision 綁定、完整列數、JSON contract 或原子發布。仍為**完整重建**，不是增量索引。

自然 systemd 收據可由 `journalctl -u stockagent-data-refresh-status-snapshot.service --since '2026-09-26 14:00:00' -o short-iso | rg 'memory peak'` 重看：改前 14:00～14:03 的變動輪次峰值約 **766.1～797.8 MiB**；新版首次確定於 14:04:05 啟動，14:04:11／14:04:41／14:05:11 三輪為 **628.7／650.2／651.7 MiB**，皆 `Result=success`、欄位 **87,626**、新版 feature 與來源 metadata 收據俱在。整輪 wall 仍約 5～6 秒，來源變動及同機負載不等；這是峰值記憶體改善的多輪觀察，不宣稱固定延遲加速或全服務最優。相鄰清冊、監控、gateway 與增量驗證 **225 passed**，Ruff／差異檢查通過。14:06 唯讀全服務覆蓋 `artifacts/benchmarks/service-coverage-20260926T1406-monitor-memory.json` 仍為 57 service／42 timer／3 path，六個本機端點 200，但 TAIFEX `blocked`、當沖 `degraded`、隔日沖／永豐／OpenBB `waiting`、全資料 `critical`；註冊 backfill 失敗與 Yahoo 630 項同次未解仍獨立顯示。下一個主要成本仍是變動輪次的完整 45 MB 清冊解碼／再寫與完整欄位序列化；持久 shard 方案必須先通過來源競態與收據完整性驗證。

## 2026-09-26 13:55～14:00 永豐擷取收據的短命監控程序記憶體

正式 30 秒監控 producer 的永豐階段每輪約 0.4～0.7 秒，內含本機 systemd/journal、歷史回補收據、期貨與股票擷取 manifest；這些是**本機證據讀取**，不登入永豐、不增加 API 連線或行情額度。期貨擷取目前約 274 份 `worker=*.json`、13 MiB，每份又帶較大的合約 metadata；公開狀態實際只用擷取 ID 作內部去重及十多個時間／數量欄位。舊 `_latest_capture_receipt` 把每份完整解析結果放進程序級 JSON LRU，且再持有一份候選清單；但 systemd producer 每輪都是新程序，這種快取在該路徑沒有跨輪收益。

現對這一條路徑保留原本的**讀前／讀後 inode、大小、mtime、ctime 驗證**，但不把大型 manifest 放進全域 JSON LRU，候選清單也只保存聚合必需欄位。其他通用收據讀取仍使用原本有界 LRU，worker 同一 capture ID 的相容鏡像去重、輸出欄位及健康判定不變。實來源獨立程序單次樣本：修正前 `ru_maxrss` **18,104→46,904 KiB**、約 **92.522 ms**；修正後 **18,308→18,692 KiB**、約 **67.302 ms**、全域快取新增 0 筆。這是局部峰值記憶體及單次 wall，**不是**整個監控 service 的穩定加速率或 cgroup 峰值比較。新測試用大型未使用 payload、worker 相容鏡像與收據更新驗證輸出與不快取；永豐監控相鄰 **28 passed**、Ruff 與 diff 檢查通過。

13:57:31 新版檔案更新後，13:58:05 與 13:58:35 的自然 producer 輪次均 `Result=success`、完整欄位 **87,626**、Shioaji 公開投影保留 `read_only=true`／`simulation_only=true`／`production_order_possible=false`；最近兩筆 `capture_manifests` 階段 **67.362／74.646 ms**。先前同機負載的該階段約 88～276 ms，資料頁仍有業務健康缺口，因此不以這兩筆推論 p95 或上游修復。永豐／資料監控／公開 gateway 相鄰完整測試 **199 passed、1 skipped**。14:00 唯讀服務覆蓋 `artifacts/benchmarks/service-coverage-20260926T1400-shioaji-memory.json` 仍為 57 service／42 timer／3 path、六個本機產品 200；TAIFEX `blocked`、當沖 `degraded`、隔日沖／永豐／OpenBB `waiting`、全資料 `critical`，註冊 backfill 失敗與 Yahoo 630 項同次未解仍保留。沒有重啟交易、行情、Discord 或永豐服務。

## 2026-09-26 13:40～13:50 全資料欄位快照的增量驗證與成本邊界

正式每 30 秒資料監控輪次有來源變動時約 5～9 秒、未變動時約 1.9 秒；自然收據將清冊解碼／發現／簽章／聚合、永豐與 FinLab 來源、欄位投影／預覽／原子寫入逐段列出。來源每輪仍會有真實 OKX／Bybit／Binance hot-tail 更新，因此不能把穩定快取輪次冒充變動輪次，也不能以局部欄位重算毫秒數代表整輪延遲。

只讀 `benchmark_data_monitor_feature_delta.py` 的代際驗證原本用舊版四欄雜湊，正式 producer 已在收據綁入第五欄 `source_metadata_sha256`；因此新版正式代際一律被誤判 `inconclusive_initial_generation`。現將版本雜湊移到共用 `data_monitor_feature_receipt.feature_revision_binding`，producer 與 shadow 工具共用；shadow 同時拒絕不合法 metadata digest。新舊收據及竄改回歸已覆蓋，沒有重啟 producer 或公開 gateway。

修正後一次自然兩代驗證 `artifacts/benchmarks/data-monitor-feature-delta-20260926T1348.json`：OKX hot-tail **1 個資料集／47 欄**改變，重組後與正式 **87,626 欄**逐列相同，受影響 Parquet 頁尾重算 **3.417 ms**，來源檔案身分零差異。但補記完整成本的下一次自然輪次 `artifacts/benchmarks/data-monitor-feature-delta-20260926T1348-costed.json`，三家 hot-tail 共 119 欄，讀舊／新完整代際分別 **1,272／1,100 ms**、重做來源發現 **764 ms**；驗證期間 **64 個來源檔**已更新，故為 `inconclusive_source_signature`，不是通過或不一致。只讀工具在來源競態時維持 fail-closed；正式 producer **仍全量欄位重建**。若要啟用增量，須先設計與現有清冊版本、公開標籤、摘要、檔案身分及原子收據一致的持久 per-dataset shard；本輪不能把 3 ms 局部運算當作端到端收益。

相鄰欄位／清冊／公開 gateway 測試 **165 passed、1 skipped**；補上「正式 producer 寫新版 metadata 收據 → shadow 讀取」跨模組回歸後，該檔定向測試另為 **8 passed**。Ruff 與 `git diff --check` 通過。這是測量契約的穩定性修復，尚未宣稱正式輪次加速或解決上游資料缺口。

13:50 唯讀全服務覆蓋 `artifacts/benchmarks/service-coverage-20260926T1350-feature-delta-review.json` 仍列 **57 service／42 timer／3 path**、六個本機產品 GET 200、timer 排程 finding 為 0；業務健康依序是 TAIFEX `blocked`、當沖 `degraded`、隔日沖／永豐／OpenBB `waiting`、全資料 `critical`。另保留註冊 backfill `completed_with_failures` 與 Yahoo 同次工作 630 未解來源項目。單次 GET 耗時不是 p95、交易執行或外網路徑證據。

## 2026-09-26 13:29～13:40 永豐儲存監控與 OpenBB 深度稽核

先把資源指標拆開：執行中的 `stockagent-shioaji-tx-history-backfill.service` 在 13:38 的 cgroup 約 12.6 GB 是 **file cache**，匿名記憶體約 0.002 GB；不能把前者說成 Python 程序常駐吃掉 13 GB。`stockagent-shioaji-storage-monitor.service` 最後一次失敗是本機 07:11 重啟時收到 `TERM`，不是儲存掃描報錯；它沒有在台股時段自動追跑。此前每小時掃約 458 萬檔，30～190 秒的真實成本仍在。以正式 `stock_minute` 約 49.5 萬檔試 `find | awk` 聚合，首輪冷掃 9.94 秒、暖掃 1.39 秒；原 `find`＋Python 暖掃 1.41 秒，沒有穩定優勢，因此沒有替換完整掃描或省略 symlink／錯誤檢查。儲存摘要原本寫死「九個」群組，正式現有 10 個；文字現按實際資料集數生成，不改容量算法。

OpenBB `--audit-only` 深度稽核每個 L0 成員本來開啟同一 Parquet metadata **兩次**，雖不影響正常 compaction timer 的 `segment_build`，卻對大規模稽核造成冗餘 I/O，且兩次讀取可能遇到不同來源代際。現以單次 metadata 讀取保留原 row-count／未壓縮大小檢查。新增 `benchmark_openbb_l1_stale_scan.py --variant none --source-metadata-sample 1500` 的唯讀重測：正式 L0 1,500 份檔案 A-B-B-A，雙次 **0.362／0.371 秒**、單次 **0.198／0.195 秒**，四次完整 metadata 摘要 SHA 相同、零 manifest 不符。收據 `artifacts/benchmarks/openbb-l1-deep-audit-source-metadata-20260926T1340.json` 可重跑；這是局部深度稽核收益，**不是**正常 compaction 的 110 秒整輪改善或生產深度稽核完成。測試另外計數確認深度稽核每個來源只開一次；OpenBB、columnar lake、永豐儲存監控相鄰 **37 passed**，Ruff 與 diff 檢查通過。沒有重啟活躍的永豐、當沖、Discord、下載器或 OpenBB。

舊註冊 backfill 的 `binance_perpetuals` 失敗可從該輪日誌歸因為 574 商品中 **1 個分鐘來源失敗**；9/25 Binance 歷史特徵工作回報 574 個更新、9/26 增量分鐘工作也回報 574 個更新，但兩者都不能單獨證明 9/20 該商品的全歷史缺口已補齊。13:38 唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260926T1340-openbb-shioaji-review.json` 仍為 57 service／42 timer／3 path、六個本機產品 GET 200；TAIFEX `blocked`、當沖 `degraded`、隔日沖／永豐／OpenBB `waiting`、全資料 `critical`。註冊 backfill 的歷史失敗與 Yahoo 每日工作 630 個同次未解項目仍獨立列出，不能用 HTTP 200、當前增量成功或 systemd `active` 覆蓋。

## 2026-09-26 13:13～13:26 同次來源缺口分類與 OpenBB 候選重測

註冊資料每日工作在 9/25 22:30 UTC 的正式同次收據雖為 `completed`，Yahoo US stocks 步驟仍回報 630 個未解項目。以前全服務盤點只顯示總數，現在在核對 run ID、步驟名稱、來源模式與摘要欄位後，保留該次 `failed: 12`、`lagging_skip: 618` 的分類，並驗證分類和等於 630；若收據分類格式或總和不符，盤點會標成分類無效，不會顯示成可信拆分。`stale` 等可能與後續成功修復重複的預檢狀態仍不算未解。新盤點 `artifacts/benchmarks/service-coverage-20260926T-current-gap-breakdown.json` 已顯示上述兩類；這是**9/25 同次工作的歷史證據**，不是 9/26 仍缺同樣 630 檔的即時證明，也沒有替任何失敗來源補抓。相鄰盤點／來源收據測試 37 個通過，Ruff 與 diff 檢查通過。

同一 Yahoo 來源摘要 `run_id=registered-daily-20260925T223000446095714Z`，產生於 22:51:33 UTC；當時 `repair_report.csv` 的 618 筆 `lagging_skip` 均顯示來源落後超過該每日工作 **14 天**的補抓上限，因此它們是明確暫緩、不是網路請求失敗。`download_report.csv` 現有 12 筆 `failed` 的訊息均為 Yahoo 無回傳列，但此報表會保留先前標的狀態、沒有自己的 run ID，不能單憑目前報表宣稱這 12 個代碼**就是**該次失敗的全部身分。若要追查單一標的，須由同次可驗證的報表／原始日誌核對，不能把 aggregate 收據推導成完整逐標的證據。

13:12 自然執行的 OpenBB L1 compaction 約 110 秒、退出碼 0，實際仍待處理約 309 萬份；其中 segment 建置 45.782 秒、stale 衍生檔 metadata 34.184 秒、未分派來源載入 22.799 秒（含來源 metadata 18.742 秒）。對 1,500 份正式檔案測試四工作者並行 metadata，冷樣本有改善，但可重複熱樣本的串行為 0.760／0.294 秒，並行 0.842／0.821 秒；輸出雖逐項相同，熱路徑沒有穩定收益，因此**撤回並行候選**，保留原有完整性驗證與服務版本。32,768 列待辦查詢的強制 covering index 與原查詢 EXPLAIN 相同、速度無穩定優勢，也未修改。FinLab 候選清冊剖析約 171 ms，其中安全路徑檢查與 JSON 讀取各約 60 ms，未為微小收益取消 symlink 防護。這輪沒有重啟交易、行情、Discord、OpenBB 或公開 gateway。

同一份唯讀盤點仍為 57 service／42 timer／3 path、六個 localhost 產品 GET 均 HTTP 200；TAIFEX `blocked`、當沖 `degraded`、隔日沖／永豐／OpenBB `waiting`、全資料 `critical`，另有註冊 backfill 的 `completed_with_failures`。可達性、工作完成度、資料品質仍分開判讀；全部服務未宣稱達效能極限或健康。

## 2026-09-26 13:08～13:12 已註冊 Bybit venue-only 建置的真實耗時與失敗邊界

查 `stockagent-crypto-training-refresh.timer` 的實際步驟，16:30 的已註冊工作執行 `build_bybit_venue_daily_features.py`，並**不**執行跨 OKX／Binance 的完整公開特徵建置。先前 venue-only 摘要的 `started_at_utc` 和 `ended_at_utc` 都在運算完成後填入，無法代表檔案載入、投影、品質稽核及寫出耗時。現在從 `build()` 入口記起始時間，正式摘要新增 `elapsed_seconds_before_summary` 與 `stage_seconds.source_load`／`projection_and_quality`／`artifact_write`；外層 `refresh_crypto_training_dataset.py` 原有的每步 wall 收據仍記完整 subprocess 時間，兩者可互相核對，不能把 summary-before-write 冒充完整 subprocess wall。

同時把品質 frame／CSV 計算移到正式 Parquet 原子換檔**之前**。若品質計算失敗，原本的正式輸出保持原位；既有輸出 SHA、品質 SHA、receipt-last 與下游逐 SHA 驗收保留。相鄰 Bybit 範圍及日特徵 **73 個測試通過**，包含故意讓品質計算失敗而確認原正式輸出 byte-for-byte 不變；Ruff、`git diff --check` 通過。這只是程式與隔離回歸，未重啟或手動觸發註冊服務。下一次正常 venue-only 工作需確認新欄位及外層 subprocess wall 一致、零失敗、輸出 SHA 與下游報告；完整跨來源建置則另有前段所述受控全量驗收，兩者不能混為一談。

13:12 唯讀收據 `artifacts/benchmarks/service-coverage-20260926T1312-crypto-projection.json` 仍列 **57 service／42 timer／3 path**，沒有 timer 排程 finding 或已註冊 service 的 failed result，六個本機產品 API 皆 HTTP 200；業務健康仍是 TAIFEX `blocked`、當沖 `degraded`、隔日沖／永豐／OpenBB `waiting`、全資料 `critical`，另有兩項註冊資料工作的業務異常收據。這是現況分層，不表示上述缺口已修好或 16:30 新版建置已驗收。

## 2026-09-26 13:01～13:07 加密貨幣分鐘來源欄位投影

持續運作中的舊版 Bybit 公開日特徵建置約 25 分鐘時仍占多核心與約 12～15 GiB RSS；沒有中斷該程序，也不能把其狀態當作新版驗收。查實共用 `read_logical_parquet` 已支援欄位投影，但 OKX／Binance 日特徵原先每個商品都載入整份分鐘來源。現只讀因果日特徵實際用到的必要／可選欄；先從**基檔與 hot tail 的實體 schema 聯集**確認欄位存在，再交給原本的邏輯合併器。如此保留 tail-only 新欄、可選欄缺失判定、最後非空值覆蓋、分鐘去重、必要欄缺漏時的 fail-closed 與完整日門檻；沒有刪來源或減少正式輸出欄位。必要欄宣告與投影共用同一常數，避免日後維護分叉。

新增唯讀、可重跑的 `scripts/benchmark_crypto_public_source_projection.py`，用 `--source binance|okx --path SOURCE --output RECEIPT` 對同一實體檔案執行 full／projected／projected／full，逐列核對完整日輸出，並拒絕測試中基檔或 hot tail 身分改變。正式來源 90 MB 級樣本與收據：`artifacts/benchmarks/bybit-public-projection-20260926T1305-binance.json` 的 **63→20 欄**、357 個輸出日，full **0.886／0.574 秒**、projected **0.430／0.416 秒**，輸出 SHA `8874f49d...601`；`artifacts/benchmarks/bybit-public-projection-20260926T1306-okx.json` 的 **47→20 欄**、1,002 個輸出日，full **1.959／1.531 秒**、projected **1.389／1.199 秒**，輸出 SHA `8aae2f7a...867`。兩個 parity 均為 `equal`，但來源／OS cache／同機負載不同，這仍是兩個樣本的局部 A/B，**不是**全 394 商品或生產整輪的加速比例。

新增基檔缺少可選欄、hot tail 才出現的合併回歸；OKX／Binance 因果輸出測試亦呼叫同一 A/B 工具。相鄰 **64 個測試通過**，Ruff 與 `git diff --check` 通過。第一次直接啟動基準腳本暴露 repo 匯入路徑錯誤，修復後正式 CLI 實跑與持久收據皆成功。修正時舊版建置程序已在執行，未中斷它；下一次**受控全量建置**須依新 `stage_seconds`、輸出 SHA／品質／覆蓋收據、wall／CPU／RSS 驗收整體收益與完整性，不能拿上述局部測速冒充部署完成。

13:07:55 再查原 PID 已結束，舊版正式摘要成功寫出：04:41:05→05:07:55 UTC，約 **26 分 50 秒**，394 商品、OKX 224／Binance 361 對應、0 failed、445,752 列／128 欄、正式輸出 SHA-256 `f9651f907287903bb12f60f6fec5d07fb944ea0241c407b0ff1ab920f8b65e3f`；舊版摘要沒有新 `stage_seconds`，不能用它判斷各階段改善。查註冊排程後確認 `stockagent-crypto-training-refresh.timer` 的 16:30 工作**只跑 Bybit venue-only 建置**，不執行這個跨 OKX／Binance 的完整公開特徵腳本；不能把該 timer 當成新版全量驗收。這次未為了基準重跑 27 分鐘全商品工作，也未推動冷發布或改動輸出；新版端到端驗收仍待來源穩定窗口中的下一次受控全量建置。

## 2026-09-26 12:47～12:57 OpenBB 候選淘汰與加密貨幣日特徵熱點

最近一次自然 OpenBB L1 輪次約 118 秒，其中建立 segment 63.016 秒、stale 衍生檔 metadata 23.694 秒、未分派來源載入 19.535 秒（含來源 metadata 17.085 秒）。仍有約 312 萬待辦檔，`economy.fred_series` 因 103,223 種 schema 超過 4,096 上限而 deferred，不能因程序 exit 0 宣稱完成。唯讀正式 manifest 對相同 300 個 L1／L0 檔案交錯比較 `ParquetFile` 與 `read_metadata`，後者沒有穩定速度優勢；沒有放寬或移除衍生檔完整性檢查。四工作者並行讀 1,000 份來源的首批樣本較快，但在正式 SQLite 同一唯讀交易選出的 2,048 份來源 A/B 中，串行 metadata 首／末輪 2.317／0.787 秒、四工作者 1.959／1.854 秒，熱檔反而明顯退步；**並行候選已撤回**。來源選取 `LEFT JOIN` 和等價 `NOT EXISTS` 在同一 snapshot 的交錯 2,048 列查詢約 4.126／3.506 秒對 4.421／4.716 秒，結果逐列相同而後者未改善，也未替換。這些測試沒有重啟或寫入正式 OpenBB manifest。

同時資源快照顯示既有 Bybit 公開日特徵建置程序在高 CPU 下仍執行，先用實際檔案拆解其計算。Bybit 394 個商品對應的 OKX 224／Binance 361 個來源沒有重複對應，因此沒有加入無效的跨商品 cache。Binance 某份實際 **506,523 列、日期為 String** 的來源，把同一日期欄解析兩次約 0.432／0.334 秒；解析一次並在同一 frame 重用等值時間序列約 0.262／0.240 秒。OKX／Binance 日特徵現重用已解析的同一時間欄，保留原本區間推定、完成日門檻與輸出；Bybit 原本另讀 394 份檔案的 `date`（唯讀樣本合計 406,607 列、約 0.474 秒），現直接取其已建立特徵 frame 的日期。這些是局部 A/B，**不是**整輪服務加速比例。

日特徵成功／失敗摘要新增 `stage_seconds`：來源註冊、各商品的 Bybit／OKX／Binance 計算、全商品迴圈、逐個公開來源、最後投影與寫出均可分辨；總迴圈包含各來源子階段，數字不可相加。另修正全部 Bybit 檔案讀取失敗時原先先遇到 `no Bybit daily dates found` 而遺失失敗收據：現在仍拒絕發布、保留既有正式輸出，先寫 `failed_before_publish`、失敗商品數與耗時。當時相鄰 **63 個測試通過**，包含成功收據、全失敗不覆寫、OKX／Binance 因果日特徵；Ruff、`git diff --check` 通過。執行中的舊版建置程序當時未被中斷；下一次受控全量建置須確認新耗時欄、成功／失敗結果、完整輸出與整輪 wall／CPU，方可對生產收益下結論。

13:00 唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260926T1300-bybit-source-review.json` 仍覆蓋 **57 service／42 timer／3 path**，沒有 timer 排程 finding 或已註冊 service 的 failed result；六個本機產品 API 均 HTTP 200。但 TAIFEX `blocked`、當沖 `degraded`、隔日沖／永豐／OpenBB `waiting`、全資料 `critical`，以及既有兩筆註冊資料工作的業務異常收據仍在。此稽核不代表以上資料或交易缺口被修復，且沒有重新啟動任何交易、下載、Discord 或公開 gateway 服務。

## 2026-09-26 12:06～12:19 欄位首屏的完整來源驗證投影

上一輪分頁已避免把 87,626 列／約 54 MB JSON 全部送到瀏覽器，但 producer 每有新代際後，gateway 首位訪客仍需解碼／建索引整份來源，正式路由最初 7 筆的 p95 約 1.44 秒。現在沿用既有 `feature_inventory.json` 原子快照和 `.reuse.json` SHA-256 收據：producer 從**同一份完整、已驗證的公開 DTO**附上首 80 列、完整分類／來源選項和摘要；gateway 只對無搜尋、無篩選且位於第一頁的請求使用它。讀取前先驗證 root-owned 非群組可寫收據、來源身分、投影校驗值，並串流雜湊**整份 54 MB 原始快照**與收據 digest 對帳；收據缺失、代際不合、資料遭改或非首屏查詢都退回原本完整解析，絕不沿用未驗證的舊代際或省略來源核對。完整 API、篩選、後續頁面、資料健康與交易契約不變。

正式自然 producer 輪已寫出投影。從正式完整快照獨立核對：**87,626** 列中的首 80 列、全部分類／來源選項、摘要與 gateway 回應逐項相等，來源版次吻合；單獨串流驗證來源 digest 三次為 **39.672／35.608／36.552 ms**，同輪完整 JSON 解碼＋索引為 **1,178.987 ms**。自然變動輪新 `feature_stages_ms.preview` 約 **76.7～83.7 ms**，是 producer 額外成本，未省去原本約 2 秒欄位投影，也未改寫入週期。隔離 gateway 的正常新代際請求觀察 **37～44 ms**、同代際 **3～4 ms**；另有一筆初次隔離請求 **581 ms** 並建立完整索引，當時可能撞到來源／收據發布交界，未證明已根除所有冷路徑回退。

資料監控、producer、收據、公開 gateway 及增量 shadow 工具相鄰 **228 passed、1 skipped**；篡改投影、來源改動、過期版次、篩選回退及首屏不完整解析均有測試。390 px 瀏覽器實際渲染首 25 列可讀且無水平溢出；此機無頭 Chromium 的常規 screenshot 因 frame 等待逾時，CDP 捕捉約 30 秒後才成功，截圖速度不當作頁面速度。僅重啟唯讀 `stockagent-public-dashboards.service`，正式 localhost 初次首屏 **43.336 ms**、後兩次 **3.843／3.061 ms**；公開 IPv4 HTTPS HTTP 200、gzip **6,609 bytes**、該次約 **251 ms**，POST 仍為 405。正式 unit `active`／零重啟、當時記憶體約 **548 MB**；與先前不同負載時的記憶體樣本不可作因果效益宣稱。

部署後唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260926T1220-feature-preview.json`：**57 service／42 timer／3 path**、無已註冊 failed result 或 timer 排程異常、六個 localhost 產品 GET 200；資料／交易健康仍是 TAIFEX `blocked`、當沖 `degraded`、隔日沖／永豐／OpenBB `waiting`、全資料 `critical`，既有兩筆資料工作業務異常收據仍在。下一階段需解決 producer 反覆完整掃描與來源發布交界造成的偶發完整解析，並持續看新路由的真實長期 p50/p95；本次不能宣稱全服務達最佳效能或業務缺口消失。

## 2026-09-26 11:50～11:57 全資料欄位頁的按需傳輸

正式欄位清冊有 **87,626 列、54,588,335 bytes** 原始 JSON；舊 `/data-monitor/api/features` 一次傳給瀏覽器，雖在 localhost gzip 後約 1.41 MB，瀏覽器仍須解壓、解析、保存與搜尋全部列，畫面起初只呈現桌機 80／手機 25 列。保留舊完整唯讀 API 與 producer 原始清冊，另加 `/data-monitor/api/features/page`：同一原子快照只在首次請求時建一次搜尋索引，按版次、安全參數、搜尋／類別／來源回傳有界頁；版次在來源變更時要求從第一頁重取，不能把不同來源代際接在一起。未減少原始欄位、未改資料健康、下載或交易規則。

對實際 54 MB 清冊的隔離 gateway 量測：首次建立索引約 **1.14 秒**；同代際首 80 列約 **6.5 ms**，gzip **6,609 bytes**；`twpub_` 搜尋 419 列約 **16.2 ms**，下一頁約 **8.3 ms**。冷索引包含約 0.69 秒 JSON 解碼和完整驗證，不能以暖頁延遲冒充冷啟動。用 5,000 列一頁取回 18 頁、共 **87,626 列**，與原 JSON 按順序逐列相同，約 **2.98 秒**；這是完整性稽核，不是一般訪客的頁面成本。

無頭 Chromium 對此長頁的 `IntersectionObserver`／捲動未自動觸發（先前 FinLab 瀏覽器測試亦觀察到無動畫 frame），故瀏覽器整合測試顯式觸發進入可視範圍，沒有把它冒充真實人工捲動驗收。其後桌機 1440 px／手機 390 px 均得到 HTTP 200、無 JS error、頁寬等於視窗寬；初始各 80／25 列，載入更多到 160／50，搜尋 `twpub_` 回 419 符合列、全部 87,626 欄仍保留。Node 行為測試另覆蓋離開可視區不輪詢及回來補同步；相鄰 Python **215 passed、1 skipped**，Node **1 passed**，Ruff、語法、diff 檢查通過。仍須量正式 gateway 長期記憶體與持續請求延遲；新分頁未消除 producer 每 30 秒重建完整快照約 5～9 秒的慢輪。

11:58 僅重啟唯讀 `stockagent-public-dashboards.service` 以載入新路由，沒有重啟交易、行情、Discord 或下載器。正式本機第一次分頁 **781.78 ms**、同代際第二次 **3.44 ms**、`twpub_` 搜尋 **11.93 ms**；公開 IPv4 HTTPS 首頁首次觀察 **4.98 秒**，緊接三次為 **16～18 ms**，因此首筆是未歸因的冷路徑異常值，不可藏成常態或宣稱 p95。公開 HTTPS 分頁 HTTP 200、gzip **6,605 bytes／23.94 ms**；gateway `active`、零重啟，重啟後記憶體當時約 **669 MB**、峰值約 **803 MB**（這是整個 unit，非純分頁）。服務覆蓋收據 `artifacts/benchmarks/service-coverage-20260926T1200-feature-page.json` 仍為 **57 service／42 timer／3 path**、六個本機產品 GET 200、沒有 failed result 或 timer schedule finding；業務健康保持 TAIFEX `blocked`、當沖 `degraded`、隔日沖／永豐／OpenBB `waiting`、全資料 `critical`，且兩筆既有資料工作的業務收據仍有失敗／未解缺口。分頁部署不構成這些業務問題已修復，也不是正式長時間 p95 驗收。

部署後的互動回看補上兩個狀態邊界：同一篩選條件的來源代際更新，應重取**已載入的列數**並保留「已展開」狀態；只有切換篩選或載入更多遇到代際衝突才回第一頁。「載入更多」也改成成功收到下一頁後才增加畫面筆數，避免慢請求／連點讓數字跑在資料前面。Node 新增代際變更回歸；正式本機 Chromium 在 390 px 下**顯式注入過期版次**重驗，50 列在來源重取後仍是 50 列、無 JS error。這不是用不同代際拼接資料。

正式 `/traffic/api/status` 已把新路由逐請求分相記錄；最初 7 筆顯示 p50 **4.0 ms**、p95 **1,436.304 ms**、錯誤 0，慢端幾乎全在 `build`，與每次新快照首個請求需重讀／解析清冊相符。7 筆遠不足以宣稱長期 p95，卻明確指出下一個瓶頸：在 producer 可信的完整快照／收據之外，製作只供無篩選首屏使用的、與來源 digest 綁定的小型 projection；不改掉完整來源、不能無證據重用舊代際。近期再以 Chromium 檢查 390／768／1080／1440／2560 px，五種寬度均無水平溢出、無 JS error；這是自動化版面檢查，不是人工設備驗收。

## 2026-09-26 11:20～11:31 資料監控慢輪的來源歸因與拒絕候選

唯讀全服務盤點 `artifacts/benchmarks/service-coverage-20260926T1120-next-risk.json` 仍列 57 service／42 timer／3 path，六個本機產品 API 回 200；TAIFEX `blocked`、當沖 `degraded`、隔日沖／永豐／OpenBB `waiting`、全資料 `critical` 是另外的業務健康，不因網站可達而變正常。正式資料監控每 30 秒的變動輪約 5～9 秒，清冊約 2～4 秒、欄位投影與 53 MiB JSON 原子寫出約 2 秒。連續清冊前後比對確認 OKX `_hot_tail` 不是無效重建：一輪有 110 個 Parquet 檔案大小或 mtime 實際變動；因此不能無證據沿用舊欄位收據。

現在 `data_monitor_timing.changed_dataset_ids` 隨每輪正式 journal 記錄**可安全歸因的實體資料集**，來源 membership／舊版 cache 不可縮窄時為 `null`，快索引真正命中或完整掃描無變動時為空列。它只是下階段逐資料集增量設計的測速與驗證線索，**不允許跳過**原有 57k 檔案身分、Parquet footer、完整公開快照或收據。11:31:02 的首輪自然收據：完整清冊刷新 1,080 檔，歸因 `physical:binance:hot-tail`、`physical:bybit:hot-tail`、`physical:okx:hot-tail` 三個資料集，完整輸出仍為 87,626 欄／`feature_reused=false`；整輪 6.354 秒，並非加速宣稱。另修復快索引／無變動捷徑可能沿用格式錯誤的 `feature_revision`，改成拒絕並重建合法代際。清冊、監控與永豐相鄰 **135 個測試通過**，Ruff 與 diff 檢查通過。

同輪排除看似簡單但不合適的替代：隔離 `orjson` 對既有 44／53 MiB JSON 更快，但其大於 64 位整數解碼會轉浮點，破壞本專案任意大計數的精確性，沒有安裝到正式環境；標準庫 `json.dump` 直接串流及逐塊 `iterencode` 對同一 53 MiB 投影約 1.3／2.7 秒，原本 `dumps`＋一次寫出約 0.5～0.9 秒，未改寫入方式。永豐監控擷取清單熱讀約 9～10 ms、冷讀偶有百毫秒；不為此增加索引。清冊與永豐唯讀監控平行化的獨立程序 A-B-B-A 樣本，串行 wall 3.216／1.185 秒、平行 3.400／1.349 秒；來源變動與同機負載不能嚴格控制，既未顯示穩定收益又新增併發複雜度，因此未部署。下一步需把已驗證的逐資料集 shadow parity 擴充為有持久代際與崩潰回退的正式增量投影，並以相同全量 JSON／收據與自然慢輪對照，不能靠刪欄位、降低核對或熱快取偽裝首次成本。

## 2026-09-26 11:04～11:10 TAIFEX 策略行情靜默失聯與虛假 complete

盤點把「worker 有收據／總檔案有資料」與「策略必要的避險行情持續可用」分開。9/18 日盤的 Shioaji 多 worker 收據全部標 `complete`、合計約 375 萬筆 Bid/Ask；但唯一執行策略的 worker 0（含 TMFJ6）最後一筆 Bid/Ask 是 **09:24:41 台北時間**，距 13:45:19 收據結束 **15,637.741 秒**；worker 1、2 則持續到約 13:44:58。模擬帳本最後幾筆 TMFJ6 成交在 09:24，留下多個避險部位；後續到期結算按原契約拒絕在沒有真實可成交行情的情況下替它們假平倉，故 TAIFEX 網頁仍 `blocked`。這是策略 worker 的串流中斷／監督缺口，不是把 `health` 改綠能修好的顯示問題。永豐官方文件描述串流為非同步 callback，並有重連事件；服務必須另外以實際 callback 新鮮度驗收，不能只依賴程序活著。

因果時鐘：Bid/Ask callback 收到並入列後記單調時間；策略 worker 0 追蹤已解析的避險期貨代碼，其餘 worker 追蹤任一 Bid/Ask。至少收到一筆行情且在對應連續交易時段時，若 **180 秒**無必要 Bid/Ask，worker 以 `failed_bidask_stale` 寫收據退出；已收到其他標的但從未收到避險行情，也從首筆行情起計時，但盤前靜默時間不計入連續交易時段。監督腳本現在直接持有 Python worker PID，逐一觀察退出；任一 worker 失敗立即停止同批其餘 worker，原有 30 秒有界重試重新建連，不等到 13:45 才發現。完全初始零資料或休市不觸發此新靜默閘門，原有 `failed_no_bidask_events` 和夜盤／休市安排保持；失聯期間不生成訊號或成交。**合約到期月最後交易日是 13:30，而非平常 13:45**：watchdog 對該避險合約在 13:30 停止要求新報價，日終稽核也以同一到期時鐘計尾端缺口，避免正常到期被誤判失聯。此時間遵循期交所微型臺指期貨商品契約，不從跨 worker 總筆數推定。

使用真實 immutable capture 重驗：9/16 到期月 TMFI6 在 13:29:54 最後一筆報價後合法結束，對 13:30 的缺口只有 **5.680 秒**，整份 541 萬筆日盤稽核通過，收據 `artifacts/benchmarks/taifex-capture-audit-20260916-expiry-recheck.json`；9/17 非到期 TMFJ6 對 13:45 的缺口 **8.443 秒**，702 萬筆稽核仍通過，收據 `artifacts/benchmarks/taifex-capture-audit-20260917-recheck.json`；9/18 非到期 TMFJ6 缺口 **15,637.741 秒**，同一稽核以 exit 1 明確拒絕（沒有改寫原始 manifest）。相關 TAIFEX／Shioaji 模擬與面板測試 **86 passed**，Ruff、編譯、shell 語法與 diff 檢查通過。11:10 休市安全窗口確認無行情 worker 後，僅重啟 `stockagent-shioaji-taifex-bidask.service` 載入新腳本；unit 為 `active`，日誌顯示下一擷取排程 9/28 08:30，未登入券商或中斷其他服務，`systemctl --failed` 無項目。**這只驗收待機程序與程式載入**；下一個真實交易時段仍須觀察 callback、退出／重試收據和 13:24～13:45 尾端行情，才能稱重連在生產有效。9/18 既有未平帳本未改，歷史成交缺口不能藉此事後修復。

## 2026-09-26 10:48～10:57 同次業務失敗不得被 systemd 成功掩蓋

唯讀全服務盤點原本逐服務保存 `result` 與同次 `business_receipt`，但頂層沒有可直接檢索的業務異常清單；只看「57 個服務無 failed result」容易把程序 exit 0 誤認為資料完成。`scripts/audit_service_latency_coverage.py` 現額外產生 `business_receipt_findings`：只收**與最後一次 systemd 執行相符**的收據，並分列非完成狀態、失敗步驟、來源未解數及來源健康；較舊或缺失的收據不被當成成功。異常欄位型別也列為 finding，避免錯誤資料被默默轉成零。此變更只影響唯讀盤點收據，不改來源、排程、下載器、交易或公開 API。

最終程式的正式收據 `artifacts/benchmarks/service-coverage-20260926T1058-business-findings-final.json`：**57 service／42 timer／3 path**，已註冊 systemd result 無 failed、timer 無排程異常；新頂層清楚列出兩項不同的歷史業務證據：`registered-data-backfill` 最後一次收據為 `completed_with_failures`、失敗步驟 `binance_perpetuals`；`registered-data-daily` 的最後一次收據雖是 `completed`，其來源摘要仍報 **630 個未解項目**。這是相應執行的收據狀態，**不是**今天所有符號仍缺 630 筆的即時證明；後續來源修復必須用較新的逐符號／逐來源收據稽核。相鄰盤點測試 **25 passed**、Ruff、編譯及 diff 檢查通過。

另以唯讀正式 OpenBB manifest、可丟棄的 20,000 member SQLite 副本重測已部署的 task 變更日誌候選，收據 `artifacts/benchmarks/openbb-l1-journal-20260926T1053-sample20k.json`：2,000 筆更新的 A-B-B-A 基線 **0.095／0.105 秒**、日誌 **0.105／0.107 秒**；寫入量約多 0.33 MiB。這只是樣本寫入成本的補充證據，正式 OpenBB 日誌版先前已啟用並驗收安靜來源路徑；本輪沒有重新部署，也**未**驗收真實 task 變更後的端到端 stale 重建。FinLab 候選清冊局部剖析約 **182 ms**、其中約 69 ms 為安全路徑／檔案存在性檢查；替代小讀法僅省數毫秒，沒有為微小收益放寬防 symlink 的檢查。

## 2026-09-26 10:20～10:44 資料監控欄位聚合與 FinLab 瀏覽器驗證

正式 `stockagent-data-refresh-status-snapshot.service` 的變動來源輪次，`feature_inventory_stages_ms.aggregate_fields` 需要反覆聚合約 **373,245 個 schema 欄位**、其中約 **4,254,890 個已知 non-null 統計格**。原實作對每一欄都建 Python 篩選 list；正式 cache 中僅約 **1,005 格**為未知、沒有非整數值。現先以 `sum(counts)` 走整數常態，遇未知或異常型別則保留原本逐格 `isinstance` 篩選路徑；不跳過來源核對、未知值、任意大整數或檔案收據。

相同正式 cache 的交錯局部量測中，舊方法三次為 **331.339／245.952／237.175 ms**，新方法為 **149.283／123.654／121.587 ms**，結果的已知檔數和總和逐欄一致。正式自然排程較受同機下載負載影響：修正前 **10:20～10:33:30** 的 21 個變動輪 `aggregate_fields` 中位數 **625.458 ms**；修正後 **10:34 起** 15 輪中位數 **497.031 ms**。這是該階段約 **20.5%** 的觀察差額，不是整輪或其他服務的加速保證。可用 `journalctl -u stockagent-data-refresh-status-snapshot.service -o cat` 擷取 `data_monitor_timing.feature_inventory_stages_ms.aggregate_fields` 重測，並須分開比較 `feature_reused=true` 的快輪。

FinLab 瀏覽器測試先前在 `scroll_into_view_if_needed()` 超時；本機無頭 Chromium 的 `requestAnimationFrame` 三秒內 **0 frame**，Playwright 在「等待元素穩定」階段卡住，原測試其實只需驗證標籤可見、文字與進度值。已移除對滾動動作的前置依賴，保留暫時 503、恢復、資料格式錯誤與再次恢復的原流程，不改網站資料或重試程式。相鄰 **212 個 Python 測試通過**；Ruff、編譯和 `git diff --check` 通過。這是測試環境相容性修正，不表示使用者捲動體驗已全面驗收。

唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260926T1044-inventory-aggregate.json` 枚舉 **57 service／42 timer／3 path**，已註冊 service 無 failed result，timer 無排程異常，六個 localhost 產品 API 皆 HTTP 200。產品健康仍為 TAIFEX `blocked`、當沖 `degraded`、隔日沖／永豐／OpenBB `waiting`、全資料 `critical`；Windows Caddy 排程的非零結果仍須與實際 child 結果分別判讀，IPv6 外網驗證已過時。未動當沖／Discord／永豐交易連線、下載器、資料發布或公開 gateway；此輪尚未宣稱全部服務效能最佳化或完成業務健康修復。

## 2026-09-26 10:14～10:25 永豐監控讀取當沖狀態的成本與代際保護

正式資料狀態快照量到 `shioaji_build=629.939 ms`，其中 `pipeline_receipts=425.419 ms`；細分為收據檔 **29.720 ms**、擷取清單 **254.064 ms**、HFT 與當沖 Snapshot **141.135 ms**、組裝 **0.451 ms**。單次量測不是長期 p95；正式監控不登入永豐，也不發出報價或交易查詢。

當沖引擎目前每次以原子方式寫約 **9,537,985 bytes** 的完整 `state.json`；永豐監控僅為顯示模式數、行情基準數、永豐來源類型數及最新報價，仍每輪讀解整份狀態。在同機熱檔交錯局部量測各 20 次，完整狀態讀取＋解析中位數 **68.988 ms**，既有 **28,490 bytes** `status.json` 讀取＋解析再核對來源檔 stat 中位數 **0.172 ms**。後者目前尚無新增摘要，僅是成本代理測量，不能宣稱正式 API 已快 400 倍或整輪省下該差額。

引擎現於寫完正式 `state.json` 後，將四個監控所需值做成小型 `shioaji_monitor_projection`，附狀態修訂號及五項檔案身分，寫進原有 `status.json`；監控只有在模擬／非正式下單旗標、版本、修訂號、型別和當前狀態檔身分全部吻合時才使用，否則退回原完整狀態。這不更動帳本、訊號、成交、報價頻率或資料發布契約。另修正 JSON 收據快取在 cache hit 後原子換檔的競態：讀取前後身分不一致時不顯示舊一代。有效摘要與失效回退的整合測試、引擎真實 `_persist` 修訂測試及相鄰監控測試合計 **52 passed**；Ruff、diff 檢查通過。

**啟用邊界：**目前執行中的 `stockagent-tw-day-trade-simulation.service` 自 **07:19:15** 起未重新載入程式，當前 `status.json` 尚無摘要；監控仍正確走完整狀態。沒有為了這項監控成本重啟交易服務。待自然、安全的服務更新後，應驗證正式摘要與來源同代，再量 `pipeline.hft_and_snapshot`、整個 `shioaji_build` 的多輪 p50/p95，以及輸出四欄與來源逐輪一致。擷取清單約 **254 ms** 仍是另一項成本，未由此修正。

唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260926T1026-shioaji-projection.json`：**57 service／42 timer／3 path**、無註冊 service failed result、無 timer 排程異常；六個本機產品 GET 均 HTTP 200，當次永豐狀態 API 約 **3.373 ms**。產品健康仍為 TAIFEX `blocked`、當沖 `degraded`、隔日沖／永豐／OpenBB `waiting`、全資料 `critical`；IPv6 外部驗證收據已過時。這些只是服務與公開投影的點狀驗證，不能將健康問題或未測的公網路徑說成已修復。

## 2026-09-26 09:57～10:05 資料監控快索引失效的重複掃描

正式 30 秒清冊快照在來源變動時，原本先為快索引核對約 **57,527** 個檔案身分，失敗後慢路徑又重新掃同一批來源。這不是可以跳過的資料核對；但一旦少量可信的舊身分已顯示來源改動，前一次**完整**快索引比對的答案必然是「失效」，可以直接走原本完整慢路徑。快索引新增有 checksum 綁定的 **512 個固定抽樣檔案身分**作預檢；任一不符只會提前失效。樣本都符合時，仍執行原本完整 membership／57k 檔案身分指紋，未抽中的新檔、刪檔與改檔也不能被誤認為未變。舊版沒有此可選欄位的索引仍走原路徑，不要求中斷服務或重建來源。

先試的「最新 16 檔」因下載批次移動而沒命中自然變動，已**替換**為分散抽樣。正式自然慢輪 `data_monitor_timing`：舊索引失效輪在 **09:58:16／09:58:46／09:59:16／10:00:16** 的 `quick_index_probe` 約 **644～724 ms**；新版完成索引發布後的 **10:00:46／10:01:16／10:02:17／10:02:47／10:03:17** 為 **2.765～6.799 ms**，均仍 `fast_index_hit=false` 並執行完整慢路徑，`refreshed_files` 分別 **830／100／1,347／476／110**，欄位數維持 **87,626**。這是省掉失效前的冗餘完整指紋，不是整輪省 640 ms：首次目錄發現由原本預檢階段移到慢路徑，五個新版變動輪的 `discover` 為 **516～696 ms**，整個 inventory 仍 **2.19～3.33 秒**、整輪仍 **5.35～7.28 秒**，同機負載不同也不能把短樣本差額當長期 p95。未變動輪仍可命中快索引並重用欄位快照。

測試明確覆蓋樣本命中提前失效、**未抽中檔案變動仍由完整指紋抓到**、壞索引退回，以及既有清冊／公開欄位契約；相鄰 **106 個資料監控測試**與 **8 個公開 gateway 資料監控測試**通過，Ruff／編譯／diff 檢查通過。唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260926T1001-fast-index-preflight.json` 仍列 **57 service／42 timer／3 path**、無已註冊 failed unit、六個 localhost API 均 HTTP 200；TAIFEX `blocked`、當沖 `degraded`、隔日沖／Shioaji／OpenBB `waiting`、全資料 `critical` 仍是待處理的不同問題。後續主要成本仍在 cache 解碼／落盤、完整來源掃描、公開狀態組裝與約 54 MB 欄位快照寫入；不能靠此預檢宣稱全部服務已最佳化。

## 2026-09-26 09:45～09:54 資料監控欄位投影增量化的唯讀驗證

先把來源快照、完整公開欄位投影和發布收據視為同一代，才有資格談「只重算變動部分」。新增 `scripts/benchmark_data_monitor_feature_delta.py`，只讀連續兩個**已由正式 producer 完成並有相符收據**的代際；比對檔案 footer、schema、資料集彙總及選檔 membership，將前一代未變資料集的公開列與新一代變動資料集的公開列合成，逐筆對照完整新快照。它再用新一代 cache 對受影響資料集重新讀取檔案身分與欄位統計；來源在驗證中改變或與 cache 身分不符時只回報 `inconclusive_source_signature`，不把不一致硬判成通過。這個工具**不改正式 producer、公開 API、下載器、帳本或發布資料**，收據寫在 `artifacts/benchmarks/`。

自然排程收據 `artifacts/benchmarks/data-monitor-feature-delta-20260926T0950.json` 得到 `state=verified`：本次只影響 `physical:okx:hot-tail` 的 **47 列**，完整公開快照為 **87,626 列**；合成列與完整新快照逐筆相等，重新計算受影響資料集的原始欄位與公開新列一致，驗證前後變動檔案身分及 cache 身分不符數均為 **0**。合成排序 **45.407 ms**，受影響資料集 footer 投影 **4.103 ms**。這是一次自然增量的**可行性證據**，不是正式服務已省下完整重建時間；目前正式慢輪仍會解碼約 45 MB cache、掃約 57k 檔、重建／寫入約 54 MB 完整欄位 JSON。若實作正式增量索引，必須持久保存逐資料集原始列與計數、綁定來源 cache revision／選檔／公開來源 metadata／程式契約，於崩潰或 schema／來源競態退回全量，且保留完整公開快照與收據相容性。

後續一次旁路讀取撞上 producer 依序發布 cache／快照／收據的交界，暴露驗證器起始階段缺少短重試；現改為最多 3 秒等待完整代際，逾時只回 `inconclusive_initial_generation`。修正後另一輪來源沒有產生新 revision，收據 `artifacts/benchmarks/data-monitor-feature-delta-20260926T0954.json` 如實為 `no_new_generation`，不是誤判通過。測試覆蓋 footer／彙總／schema／membership 變更、故障退回、舊欄位刪除、公開 metadata 漂移、跨代收據綁定遭竄改及發布交界；資料監控相鄰 **103 個測試通過**，Ruff／語法檢查通過。唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260926T0948-feature-delta-shadow.json` 仍列 **57 service／42 timer／3 path**、無已註冊 failed unit、六個 localhost API 都 HTTP 200；但 TAIFEX `blocked`、當沖 `degraded`、隔日沖／Shioaji／OpenBB `waiting`、全資料 `critical`，且外部 IPv6 收據已過時，這些都沒有被欄位投影測速修復。

另以隔離安裝的 `orjson` 對現有約 45／54 MB JSON 測試，編解碼速度雖較快，但超過 64 位元範圍的 JSON 整數在其解碼路徑會轉為浮點值，與標準 `json` 的精確整數語意不同；沒有把它部署到正式環境或偷偷替換 cache 格式。先保正確性，再針對可證明等價的局部熱路徑優化。

## 2026-09-26 09:23～09:27 註冊加密貨幣下載的日誌寫入節流

`stockagent-registered-data-intraday.service` 執行同一組 OKX／Bybit／Binance tail 下載時，在 **09:22:58～09:23:58** 的 journald `-o cat` 文字共 **47,201 bytes**，其中三個 `download:` tqdm 標記合計 **454 次**。這些頻繁的 carriage-return 更新不等於 454 筆資料驗收；資料監控以各下載器原有的 `progress.json` 原子收據為優先，僅當結構化收據缺漏／不新鮮時才退回掃描 tqdm 日誌。不能直接把進度條關掉而失去故障備援。

`downloader.common.run_parallel_tasks` 現只在**非互動式 stderr** 把 tqdm 最小顯示間隔從預設 **0.1 秒**增至 **10 秒**；互動式終端仍維持 0.1 秒，每項完成、例外結果及原子進度收據的計算和寫入完全不變，完整階段結束仍顯示最終狀態。此為 tqdm [官方 `mininterval` 契約](https://tqdm.github.io/docs/tqdm/)所支援。下載器／資料監控相鄰 **143 個測試通過**，Ruff 與 diff 檢查通過。修改時 09:22:58 已在執行的程序載入舊版，不以它的日誌量作新版驗收；需在下一次**自然啟動**後量同長度日誌、確認 `progress.json` 前進及終態／來源資料不變，不能只用測試聲稱正式 journal 已下降。

**09:26:58 自然輪已完成驗收**：相同一分鐘窗 `09:26:58～09:27:58` 的 journald `-o cat` 為 **12,050 bytes**，三個 `download:` 標記合計 **10 次**；相較上述舊版同長度窗約少 **74%** 日誌位元組、約 **98%** 進度條標記。OKX **492/492**、Bybit **867/867**、Binance **574/574** 的各自 `progress.json` 均為 `complete`，服務 exit 0、約 **59.958 秒** wall；舊輪約 **59.961 秒** wall。這證明日誌量下降且觀測未斷，不證明下載吞吐穩定提升或歷史覆蓋完整。正式來源內容／發布收據仍須由其獨立資料健康契約驗證。

後續唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260926T0929-aggregate-log-throttle.json`：仍是 **57 service／42 timer／3 path**、沒有 failed unit result 或 timer schedule finding，六個 localhost API 都 HTTP 200；資料健康仍為 TAIFEX `blocked`、當沖 `degraded`、隔日沖／Shioaji／OpenBB `waiting`、全資料 `critical`。五秒主機資源樣本的 CPU／memory／I/O full PSI 皆 0%，只說明當次樣本，不能取代開盤、冷啟動、資料完整或長期 p95 驗收。

## 2026-09-26 09:18～09:23 欄位清冊聚合的重複物件配置

在仍持續寫入的 FinLab／OpenBB 工作旁，正式 30 秒資料監控慢輪約 **6～10 秒**，其中 footer 欄位聚合約 **0.6～1.0 秒**；因此不能只看整輪時間或熱快取。`build_feature_inventory` 對每個 schema 欄位使用 `dict.setdefault(name, {...})`，即使欄位已存在，Python 仍先建立預設 dict／set。現改為先查找，只有新欄位才配置。檔案身分重查、footer null 統計、完整欄位集合、公開 DTO／收據及 timer 頻率均不變。

對同一份已解碼 cache 與 **57,494** 個固定檔案身分，在同一程序交錯 A-B-B-A／A-B-B-A 執行原版與新版，各輪均輸出 **87,626 列**且完整 JSON SHA-256 完全一致；欄位聚合中位數原版 **632.690 ms**、新版 **447.414 ms**。這約省 185 ms 的單一聚合階段，並非整輪 30 秒快照或使用者端延遲的保證。未固定檔案身分的先前交錯量測因 live 來源變動造成每輪雜湊不同，已排除，不當作等價證據。`test/test_data_monitor_inventory.py` 與 `test/test_data_monitor_dashboard.py` 合計 **97 passed**，Ruff、diff 檢查通過。正式自然快照在新版檔案修改後持續 exit 0、欄位數仍 87,626；同時 OpenBB 壓縮與 FinLab 下載競爭，正式聚合時間波動仍大，尚不能宣稱穩定 p95 已下降。

此項僅消除一個多餘配置點；45 MB inventory cache 解碼、57k 個來源檔案身分稽核及 54 MB 欄位 JSON 重寫仍是主要成本。後續應從可信變更證據建立資料集級增量投影，保留完整來源身分與 footer 驗證，不能用調低刷新頻率或刪欄位假裝加速。

## 2026-09-26 09:10～09:15 全資料欄位頁的可視範圍刷新

正式 `feature_inventory.json` 當下為 **54,566,354 bytes**、87,626 欄位；相鄰兩輪只有 47 欄位列變動，但 producer 仍每約 30 秒重投影完整快照，欄位頁一旦啟用後原本每 60 秒持續抓取並解析完整清冊，即使使用者已捲離欄位區；資料更新時還把「載入更多」重設。這些是不同成本：本次只修**瀏覽器離開欄位區後的無效刷新與互動狀態**，沒有聲稱已消除 producer 的完整重投影或欄位區可視時的首次完整傳輸。

`services/data_monitor_dashboard/app.js` 現以持續觀測的 `IntersectionObserver`（提前 220 px）控制欄位同步；離開欄位區不再做定時欄位請求，重返時只在距上次請求達 60 秒後補同步，失敗時 10 秒後可重試。時距採瀏覽器單調時鐘，成功刷新保留已展開筆數與篩選。沒有改欄位 API、公開資料、ETag、來源收據、安全驗證、總覽的 10 秒刷新或交易系統。無 `IntersectionObserver` 的瀏覽器保留原持續刷新行為。

無搜尋／分類／來源篩選時，「載入更多」現在直接復用已有的列陣列，不再每次對全部欄位做一次 `.filter`。用當下 **87,626** 筆正式 JSON 在 Node 同一程序交錯執行各 100 次、比較回傳列數完全相同：原掃描 **226.347／223.475 ms**，無掃描 **0.094／0.004 ms**。這只是單機 JS 熱路徑微基準，對一次操作約省 2.2 ms，不是瀏覽器繪製、公網或完整頁面延遲的保證；有任何篩選時仍走原逐欄判斷。

新增 Node 行為測試覆蓋初次可視啟用、離開不輪詢、重返補同步、快速捲動不重複請求及保留展開數；**1 passed**。相關公開 API **4 passed**、`node --check` 通過；正式 localhost 與公網 HTTPS 均已送出含新版欄位控制邏輯的 JS，公網 `/data-monitor/` HTTP 200。未做獨立手機／平板瀏覽器性能測量，也沒有把合成測試等同實際使用者延遲。後續需設計可驗證的每資料集增量投影及欄位差量／分頁傳輸，不能刪除完整欄位或用較慢刷新掩蓋來源變化。

變更後的唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260926T0916-feature-viewport.json` 仍涵蓋 **57 service／42 timer／3 path**、`timer_schedule_findings=[]`，已註冊 service 沒有 failed result；六個 localhost 產品狀態 GET 都 HTTP 200。然而 TAIFEX 仍 `blocked`、當沖仍 `degraded`、隔日沖／Shioaji／OpenBB 仍 `waiting`、全資料摘要仍 `critical`。本次前端修正不能視為這些資料／交易缺口已修復。五秒資源樣本和單次請求也不是長期 p95 證明。

## 2026-09-26 08:50～08:57 FinLab cgroup 壓力與單鍵下載有界調整

OpenBB 第二輪後的唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260926T0850-openbb-journal.json` 再覆蓋 **57 service／42 timer／3 path**，沒有 failed unit 或 timer schedule finding；六個 localhost 狀態 API 均 HTTP 200，但 TAIFEX `blocked`、當沖 `degraded`、隔日沖／Shioaji／OpenBB `waiting`、全資料監控 `critical`，不能說所有業務已正常。當時 FinLab 本機研究下載器執行逾 50 分鐘，6 GiB `MemoryHigh` 下 cgroup memory.full 10 秒平均約 **90%**、`memory.high` 累積約 **98,000** 次，卻有主機約 **81 GiB** 可用；父層 64 GiB soft／76 GiB hard 未滿。這是子服務高水位回收競爭，不是主機實體 RAM 用盡。`inventory` 已在原 600 秒單鍵上限逾時，保持未完成，不偽造來源收據。

只將 FinLab 服務 `MemoryHigh` **6→8 GiB**，保留 **10 GiB hard、0 swap、600 秒單鍵超時、帳號額度和開盤讓路**；用 `finlab-only` installer 更新正式 unit，對同一 active cgroup 用 runtime property 生效，**沒有重啟服務或增加 API 請求**。調整後當前 `rotc_broker_transactions` 仍一度 memory.full 約 **97%**、匿名記憶體近 9 GiB，故不能說 soft-limit 單獨根治。該鍵於 **08:56:27** 回報 `unchanged`、**22,467,309** 列，08:49:31 起約 **416 秒**、低於 600 秒；整輪峰值約 **9.04 GB**、未達 10 GiB hard、0 OOM、0 swap。這是另一個鍵、同一次非受控執行，不能與 `inventory` 逾時作因果 A/B；後續需看同鍵多輪時間與高水位事件，若持續卡住應分區或資源暫緩，不盲目再抬 hard limit。FinLab 相鄰 **65 個測試通過**；其中一個「跨配額日重試」測試原本把模擬日期與真實 `checked_at` 混用，已在測試中顯式對齊模擬收據，**未改正式排程時鐘**。08:59:48 再次唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260926T0900-finlab-soft8g.json` 的主機 memory.full 10 秒平均為 **0%**、六個 API 仍 200，但重壓鍵已結束且同機負載變了，**不能把歸零單獨歸因於 soft limit**；產品資料健康仍不變。

## 2026-09-26 08:30～08:35 OpenBB L1 來源異動日誌：增量稽核候選

真冷輪的來源 JOIN 曾需 **574.924 秒**並讀取約 **17.6 GB**，暖輪仍重掃約 386.6 萬個已壓縮 member。這項工作要回答的是「已壓縮來源是否變更」，不是每輪重新計算相同答案；但不能僅靠 mtime、熱快取或省略來源驗證。新增 `scripts/openbb_l1_source_journal.py`：SQLite `tasks` 的 INSERT／DELETE／契約欄位 UPDATE 由同一交易內的 AFTER trigger 記錄 task ID，先做一次完整基線；後續只對變更 ID 走原 stale 判定 SQL，仍逐一驗證既有 L1 輸出 metadata。資料庫 inode／schema／SQL 契約變更、日常 24 小時完整稽核到期、日誌斷序或變更超過上限都退回完整掃描；只在完整 endpoint 宇宙可啟用，禁止過濾 endpoint 後推進全域 watermark。收據記錄模式、原因、變更數與分段耗時；原全量路徑保留。

**寫入成本先量後採用**：`scripts/benchmark_openbb_l1_source_journal.py` 從正式來源唯讀複製 100,000 筆真實 task/member/segment 到同 ext4 暫存庫，在可拋棄副本對相同 10,000 個 task 做 ABBA 更新及新增。收據 `artifacts/benchmarks/openbb-l1-source-journal-20260926T0830-update-insert.json`：更新基線 **0.503／0.553 秒**、日誌版 **0.563／0.543 秒**，程序寫入量約 **14.57 MB→16.22～16.25 MB**；新增基線 **0.666／0.764 秒**、日誌版 **0.656／0.834 秒**，寫入約 **39.64／75.28 MB→41.37／77.01 MB**。日誌版本樣本庫約多 **3 MB**，這不是正式 downloader 同時寫入或長期吞吐 SLA。之前的 covering index 候選明顯增加讀寫成本，未部署。

程式和 systemd 模板已部署；相鄰 OpenBB **387 個測試**、Ruff、編譯與 diff 檢查通過。正式 unit 保留 `--archive-idle-only`、每輪 32,768、3 GiB hard stop 和交易窗口守衛；沒有重啟 downloader、交易、行情、Discord 或公開 gateway。**08:37:08～08:45:39 手動啟動的首輪完整基線** exit 0、wall **511.363 秒**、CPU **209.036 秒**、峰值 2.7 GiB、約 33.5 MiB swap；來源 JOIN **347.389 秒**、衍生 metadata **61.536 秒**、來源稽核後程序讀取 **13.75 GB**、全輪約 **14.98 GB**。0 stale、0 failed、新增 261 段，已壓縮 **3,931,539**、待辦 **3,386,103**，相對上輪精確減少 32,768，L0 未刪。正式 `l1_compaction_latest.json` 記 `source_audit.mode=full, reason=checkpoint_missing`；checkpoint 已寫入，日誌 0 列，member 數與成功段逐 endpoint 對帳通過。這仍是**冷基線**，不能拿來宣稱增量加速。

**08:46:12～08:48:41 第二輪正式增量驗收**也 exit 0、wall **149.487 秒**、CPU **132.044 秒**、峰值 2.7 GiB、0 swap；收據明確為 `source_audit.mode=incremental, reason=journal_verified, changed_task_ids=0`。來源 JOIN **0.000 秒**，stale 稽核仍完整驗證衍生 Parquet metadata **64.478 秒**；全輪程序實際讀碟約 **2.928 GB**，對首輪約 14.98 GB。0 stale、0 failed、新增 260 段，已壓縮 **3,964,307**、待辦 **3,353,335**，又精確減少 32,768，L0 未刪，逐 endpoint member／segment 對帳通過。全輪 wall 相差約 **511→149 秒**，但兩輪 OS 快取和同機負載不同；可歸因的直接證據是來源 JOIN **347.389→0.000 秒**、保留原衍生稽核及相同批次進度，不是長期 p95 或每輪保證。後續再加入 fail-closed 欄位契約檢查：若 stale SQL 使用未列入同交易 trigger 的 task 欄位，下一輪拒絕增量稽核；相鄰 OpenBB 回歸此時 **388 passed**。

仍待驗收正式 downloader 產生真實 task 變更時 trigger 的端到端 stale 重建，以及 24 小時到期後的強制完整掃描；目前 `changed_task_ids=0` 只證明安靜來源路徑。第二輪衍生 metadata **64.478 秒**、待辦來源 metadata **17.550 秒**及 segment build **50.983 秒**成為下一批熱點，不能為了壓秒數跳過檔案與列數證據。[SQLite trigger 官方契約](https://www.sqlite.org/lang_createtrigger.html)及 [AUTOINCREMENT 序列契約](https://www.sqlite.org/autoinc.html)是此故障回退設計的依據。

## 2026-09-26 08:15～08:16 重開機後外部 IPv6 與全服務複核

第一次重新要求 Internet.nl IPv6 探針在 60 秒期限內仍 `done=false`，收據如實為 `inconclusive_timeout`；隨後重用結果雖顯示成功，但 `probe_request_issued=false`，不能當成當次新探針證據。再獨立發起的一次新探針於 **08:15:46** 回傳 `done=true, success=true, probe_request_issued=true`，DNS AAAA 同時為 `2001:b011:e610:35b4:f1f5:5ae6:7419:3f1d`，正式收據 `artifacts/benchmarks/dashboards/public-ipv6-audit.json`。同時 DDNS 強制 IPv4 HTTPS `/healthz` HTTP 200，單次約 **247 ms**。這證明觀測時點的雙棧可達，**不是**全天候、公網瀏覽器完整頁面或路由器 IPv6 長期轉發保證。

緊接唯讀服務覆蓋 `artifacts/benchmarks/service-coverage-20260926T0816-openbb-ipv6.json` 計 **57 service／42 timer／3 path**、`timer_schedule_findings=[]`、StockAgent 當時無 failed unit；六個本機產品 GET 均 HTTP 200，外部 IPv6 收據被判為 `external_pass_observed`。但 TAIFEX `blocked`、當沖 `degraded`、隔日沖／Shioaji／OpenBB `waiting`、全資料監控 `critical`；資料健康／成交證據並未隨網路與 OpenBB 壓縮驗收變綠。這是一次五秒資源樣本與點狀 HTTP 檢查，不能宣稱長期 p95 或所有服務功能已正常。

## 2026-09-26 07:40～08:05 OpenBB 真冷輪分解、索引候選排除與狀態聚合修正

重開機後的 **07:40:03 自然輪**於 07:53:35 exit 0，wall **811.217 秒**、CPU **263.061 秒**、記憶體峰值約 **2.7 GiB**、swap 峰值 **36.8 MiB**；新建 **259** 段、待辦精確減少 **32,768** 至 **3,451,639**，0 stale／failed，L0 未刪。持久收據 `data_openBB/_state/l1_compaction_latest.json` 顯示來源 JOIN **574.924 秒**、衍生 Parquet metadata **73.518 秒**、整段 stale 稽核 **648.786 秒**，來源稽核後程序已實際讀取 **17,606,316,032 bytes**。完成前 cgroup `memory.high` **46,958** 次、I/O full stall 約 **121.99 秒**。這是冷啟動與同機工作競爭下的一輪，不可以 06:45 暖輪 164.567 秒冒充重啟 SLA；索引／查詢問題是主要成本，但不能忽略磁碟和快取條件。

先用新唯讀工具 `scripts/benchmark_openbb_l1_covering_index.py` 從正式 manifest 複製 **100,000 筆**真實 task/member/segment 到同 ext4 的有界暫存庫，強制原 PK 與 covering index 計畫交錯 A/B，僅對暫存庫要求逐輪頁面快取清除。收據 `artifacts/benchmarks/openbb-l1-covering-20260926T0805-sample100k-ext4-cold.json`：兩計畫 0 stale、同結果 SHA；covering index 將程序邏輯讀取約 **116.47→69.73 MB**，但樣本實際讀碟僅 **142.59→134.12 MB**、冷查詢反而 **0.204～0.214→0.539～0.540 秒**；新增索引 **30.86 MB／十萬筆**，樣本千筆更新的實際寫入約 **1.37→4.42～6.53 MB**。因此**不在正式 17.8 GB manifest 建此索引**：本機樣本顯示查詢與寫入權衡不佳，且無法證明全庫收益。暫存庫已由工具清理，正式來源以唯讀開啟，沒有更動其 schema 或任務。

可重測命令：`source scripts/runtime_env.sh && run_fintech_python scripts/benchmark_openbb_l1_covering_index.py --sample-members 100000 --cold-cache --output artifacts/benchmarks/openbb-l1-covering-recheck.json`。`--cold-cache` 僅對暫存副本呼叫 per-file `posix_fadvise`，不清除正式 manifest 或全機快取；此樣本不能推定全量讀取收益。SQLite [官方查詢規劃說明](https://www.sqlite.org/queryplanner.html#_covering_indexes)解釋了 covering index 能免回表，但此處的實際磁碟與寫入代價必須以本機測速為準。

另找出無須額外索引的確定性冗餘：每輪狀態收據為了計算 `compacted_files`／`compacted_rows`，重掃近 **386.6 萬**筆 member，真冷輪這一步 **45.400 秒**。但每個成功 segment 的 `source_files`／`source_rows` 在來源檔數／列數驗證後與 member 列**同一 SQLite 交易**寫入。正式 43 個 endpoint 對照，segment 加總與現有 member 結果逐項完全相同；member 全域筆數與成功 segment 宣告檔數也同為 **3,866,003**。已改為單次 segment 聚合，仍每輪利用既有 endpoint covering index 核對**每個 endpoint** 的 member 筆數，不符即在寫出狀態前 fail closed；既有公開欄位與 L0／stale 規則不變。正式唯讀查詢的 endpoint member 計數 **0.234 秒**、segment 加總 **0.048 秒**；完整 OpenBB 相鄰回歸 **376 passed**、Ruff／`git diff --check` 通過，含人為刪除 member 與改錯 endpoint 後拒絕覆寫舊收據的測試。

08:04:47 於週六非交易窗口啟動既有受控服務，**08:14:17 exit 0**；wall **569.574 秒**、CPU **237.476 秒**、峰值約 **2.7 GiB**，新建 **259** 段、0 stale／failed、待辦減少 **32,768** 到 **3,418,871**，L0 未刪。來源 JOIN 仍 **390.414 秒**、衍生 metadata **69.673 秒**，而新 `status_member_count` **0.147 秒**、`status_segment_count` **0.072 秒**；前一冷輪的 member 群組掃描為 **45.400 秒**。這是持久正式收據對局部熱點的驗收；兩輪來源快取、I/O 壓力和批次不同，**不能把整輪 811→570 秒差額歸給狀態聚合**。這一進程在啟動時載入的是全域總數核對版本，稍後加強的 endpoint 級核對已通過回歸與正式資料唯讀查詢，仍待下一自然輪載入驗證。每輪完整來源 JOIN 仍是主要未解瓶頸；後續增量化必須有與 task 寫入同交易的變更證據、遷移全量基線和可回退的完整稽核，不能只根據檔案 mtime 或熱快取跳過來源驗證。

## 2026-09-26 07:45 重開機後唯讀覆蓋檢查

收據 `artifacts/benchmarks/service-coverage-20260926T0745-post-reboot.json` 盤點 **57 service／42 timer／3 path**，`timer_schedule_findings=[]`、StockAgent unit 當時無 `failed`。六個本機產品 API 均 HTTP 200，但 TAIFEX `blocked`、當沖 `degraded`、隔日沖／Shioaji／OpenBB `waiting`、全資料摘要 `degraded`；這是可達性與當下回應，不是資料完整或交易履約驗收。Windows Caddy task 為 `Running`，安裝的 launcher／Caddyfile SHA 均與來源一致；公網 gateway 恢復收據記本次 07:19 左右的 episode 約 **12.957 秒**（WSL dispatch 到 userspace **0.410 秒**、userspace 到 gateway healthy **12.032 秒**）。這是現有事件紀錄，非本輪主動重開機實驗；外部 IPv6 探針收據已過期，不能宣稱重開機後雙棧已驗收。Windows task 的非零 `0x800710E0` 與 `IgnoreNew` 重複排程、當時正在 `Running` 同時存在，須保留為歧義，不將單一 task 結果當故障或成功證明。

本輪 5 秒資源採樣的主機 I/O full PSI 約 **11%**；07:40 自然啟動的 OpenBB L1 工作尚未有 terminal 收據，當時其 cgroup `memory.high` 事件已逾一萬次，記憶體主要是檔案快取而非 Python anon，0 OOM。07:47 再看該程序已讀碟約 **11.28 GB**、累積 `memory.high` **22,283** 次，並在 I/O wait；這是冷啟動競爭狀態的風險觀測，不可把 06:45 暖輪耗時套用到這輪；須等待自然完成，再比對持久階段耗時、cgroup 壓力、來源完整性與待辦減量。完整 OpenBB 相鄰回歸 **372 passed**，不代表全庫通過。

## 2026-09-26 06:38～06:49 OpenBB L1 來源 JOIN 順序驗收

第一性成本是每輪必須驗證「已壓縮來源仍有效」，不能為求速度跳過 stale 契約或輸出檔 metadata。對同一約 17.8 GB SQLite manifest 做唯讀、交錯順序的完整來源 JOIN A/B：`abba` 四輪的 segment-first **29.456／35.701 秒**、member-first **22.299／21.738 秒**；`baab` 四輪的 segment-first **30.674／24.850 秒**、member-first **22.559／22.265 秒**。八輪皆為 **0 stale**、同一結果 SHA；member-first 每輪約 **33.475 GB** 程序邏輯讀取，segment-first 約 **37.716 GB**。收據為 `artifacts/benchmarks/openbb-l1-stale-20260926T0638-abba.json` 和 `...T0641-baab.json`。除首輪 segment-first 實際讀碟約 **290 MB** 外，其餘讀碟 0，故這是**熱快取查詢與邏輯讀取量**的證據，不是冷碟或服務 p95 證明。基準腳本可重跑順序與記錄 I/O。

正式 `compact_openbb_l1.py` 只把既有等價 SQL 的掃描順序選為 `member_first`，並在持久收據記錄 `stale_source_scan_order`；來源條件、stale 判定、衍生檔 metadata、批次限制、L0 保留與交易時段守衛不變。06:45:56 啟動的正式 systemd 輪於 **06:48:41 exit 0**，wall **164.567 秒**、CPU **168.696 秒**、記憶體峰值約 **2.7 GiB**；收據中的來源 JOIN **24.344 秒**、衍生檔 metadata **44.929 秒**、完整 stale 稽核 **69.286 秒**。新建 **261** 段、待辦降至 **3,484,407**、0 stale／failed、42 views，既有一個高 schema 變體 view 仍 deferred，`l0_deleted=false`。前一輪來源 JOIN **25.771 秒**；快取、I/O 負載和當輪來源量不同，因此**不能將 1.427 秒差值當穩定因果加速**。這輪 `memory.high` 事件 **648**、I/O full stall 約 **1.87 秒**，仍需追蹤冷啟動與負載下的尾延遲。相鄰 OpenBB 測試 **20 passed**、Ruff 與 `git diff --check` 通過；沒有宣稱全庫或所有服務已完成。

主機後續重新開機，07:40:03 自然啟動了另一輪；此段只記錄已完成的 06:45 輪，不以正在運作的輪次推論成功。下次應用同一收據對照 `stale_source_join_query`、`stale_derivative_metadata_scan`、整輪 wall、cgroup high／I/O stall、stale／failed、pending 減量；若資料規模或索引布局改變，重做交錯 A/B，不能把 `member_first` 視為永久最優。

## 2026-09-26 06:10～06:30 OpenBB L1 冷／暖查詢與 cgroup 壓力

最新 05:44 自然壓縮輪仍 exit 0，但 wall **518.754 秒**；完整 stale 契約稽核 **376.714 秒**，其中同一來源 JOIN **319.509 秒**、15.5 萬個輸出 metadata **57.034 秒**。cgroup 在 2.5 GiB `MemoryHigh` 下累積 **29,631 次 high 事件**、約 58 秒 I/O full stall，峰值 `2,686,709,760` bytes、swap 約 60 MB，0 stale。該輪和 D: 冷庫完整校驗重疊，不能單從 wall 將因果歸給 SQL、soft limit 或校驗中的任何一項。

冷庫校驗結束後，使用正式 `_stale_source_contract_sql`、同一 17.79 GB SQLite manifest **唯讀**重測兩輪：第一次 **130.083 秒**，緊接同查詢 **23.313 秒**；兩輪皆 0 stale，結果 SHA `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`。收據 `artifacts/benchmarks/openbb-l1-stale-20260926T0615-recheck.json` 與 `...T0618-warm-recheck.json`。這顯示快取／I/O 條件可造成約 5.6 倍差距，不能以暖快取 23 秒保證冷啟動。基準工具現會逐 variant 記錄 `/proc/self/io` 的邏輯與實際讀取量；這兩輪在修改前啟動，尚無新欄位。正式自然輪結束後，在其更新後的 17.81 GB manifest 上再跑新基準：**28.712 秒**、0 stale、`rchar_bytes=37,715,624,046`、`read_bytes=0`，收據 `artifacts/benchmarks/openbb-l1-stale-20260926T0629-profiled.json`。這證實該次是大量**邏輯** SQLite 讀取但完全由已熱的頁面供應，不能拿它充作冷碟 I/O 改善。先前同快照 member-first 與更大 SQLite cache 都沒有穩定收益，所以沒有更改正式 SQL、索引、並發數或完整來源／衍生檔稽核。

只將正式 L1 service 的 `MemoryHigh` **2.5→2.75 GiB**，保留 `MemoryMax=3 GiB`、32,768 來源上限、兩線程、40 分鐘 timeout、開盤保護及所有發布／校驗契約。19 個相鄰測試、Ruff、systemd 單元驗證通過並已安裝。06:23:30 **自然 timer** 輪 exit 0、wall **163.562 秒**、CPU **159.163 秒**、峰值 `2,954,391,552` bytes（接近新 soft limit）、swap 0、0 OOM；`memory.high` 事件 **3,603**、I/O full stall 約 **11.24 秒**。來源 JOIN **25.771 秒**、輸出 metadata **47.973 秒**、整個 stale 稽核 **73.757 秒**；新建 **261** 段、待辦精確減少 **32,768** 到 **3,549,943**、0 stale／failed、42 views，其中既有 `economy.fred_series` schema 超限仍明確 deferred，L0 未刪。這支持此輪在不犧牲驗證下減少 cgroup 壓力，但兩輪 OS 快取、來源量與並行 I/O 不同，**不能宣稱** 519→164 秒是穩定因果加速或全服務 p95。保留新 soft limit，不再貼近 3 GiB hard stop 上調；後續須重看冷／競爭條件下的自然輪與新讀取量收據。

部署後唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260926T0630-openbb-memory.json` 仍涵蓋 **57 service／42 timer／3 path**、`timer_schedule_findings=[]`；六個本機產品 API HTTP 200，但 TAIFEX `blocked`、當沖 `degraded`、隔日沖／Shioaji／OpenBB `waiting`、全資料 `critical` 均未因 L1 調整變綠。五秒樣本和一次自然輪不能驗收長期穩定性。

另複查 09-20 `registered-data-backfill` 的約 18 小時歷史失敗：當時 Binance 574 個符號有 1 個下載失敗，該服務的 last attempt 仍應如實標 failed；但 09-26 06:11 最新 Binance 1m 摘要 574／574 updated，09-25 14:29 歷史 feature report 亦 574／574 updated。這些較新收據顯示先前**符號層**缺口已有後續進度，不能因此倒改週任務的失敗紀錄，也不足以證明所有長期 feature 覆蓋或其它資料健康已完成。此輪未重跑 18 小時回補、未降低任何來源缺漏門檻。

## 2026-09-26 05:05 起：TW public 冷發布重試風暴與完整驗證讀取量

唯讀全服務快照 `artifacts/benchmarks/service-coverage-20260926T0505-ipv6.json` 仍可見 `stockagent-tw-public-cold-publish.service` 每五分鐘因 stale derived receipts 失敗後自動重試；每次約 11～12 秒 wall、16～18 秒 CPU，累積至少 62 次。這不是可用性恢復：相同 stale receipt 不會因重啟而變新。對官方 symbol 與 feature 的既有嚴格稽核分別證實前者的三項來源 receipt、後者的 `source_bytes` 不符，沒有降低任何發布門檻。

現在只有明確的 `StaleDerivedReceipts` 回傳 exit 75，仍保存 `failed`／`release=null` 收據；systemd `RestartPreventExitStatus=75` 停止對同一失敗不停重跑，其它運行錯誤維持原重試。新增兩份衍生收據的 `.path` 喚醒與 feature reconcile 成功後的 `OnSuccess`，並在冷發布開始前保留距 08:20 至少三小時的 runway、交易時段不啟動。安裝後實際觀察到 stale exit 75 停止重啟、PathChanged 喚醒、來源 lock busy 時維持 `deferred`；`OnSuccess` 已安裝並通過語法驗證，但尚未觀察下一次**自然**成功 reconcile 後端到端觸發，不能把它列為已驗收。公開、交易與行情服務沒有因此重啟。

在週六非開盤窗口，使用既有 canonical source lock 重建官方 symbol，`8,996,233` 列／`2,757` 標的／調整缺漏 0；其後正式 feature reconcile `9,606,617` 列／`143` 特徵、約 4 分 45 秒 wall／34 分 25 秒 CPU、記憶體峰值約 48 GiB、swap 峰值約 576 MiB，無 OOM。兩份衍生收據重新做精確來源位元組稽核皆 `valid=true`、零 findings。D: guarded mount 檢查、catalog `publish-status` 通過後，正式冷發布於 05:28:49 完成；新 immutable release 為 `tw-public-20260925T212755208193379Z-l0-penguin-b66184ee3b0076b7`，來源 `165,143` 檔／`15,344,322,690` bytes，其中 `162,319` 檔重用、`2,824` 檔變更、新傳輸物件 `75` 個／`1,852,204,937` bytes。該輪約 4 分 9 秒 wall／1 分 38 秒 CPU、記憶體峰值約 12.9 GiB；發布器現為 `inactive/success`、`NRestarts=0`，path 為 `active/waiting`。沒有更改運行時資料連結、materialized cache 或刪除來源。

同一 release 的**原驗證實作**已完整通過 `2,843` 個物件、約 `25,000,595,368` bytes 的 SHA、ZIP、inventory 和 manifest 檢查，wall 約 25 分鐘，結束前程序實際讀取量約 44.5 GB。原因是 pack 先讀一次算 SHA，再從 D: 再讀一次做 ZIP CRC。現在小於等於 64 MiB 的 pack 只從冷庫讀一次，在有界記憶體內對**同一份位元組**重算 SHA 與 ZIP CRC；大 pack 和 blobs 繼續舊 streaming 路徑。這沒有省略物件／成員或校驗步驟；新增 CLI `verify --profile` 留下程序 wall、邏輯及實際讀取量。小 pack／大 pack fallback／校驗輸出等價與 profile 測試在相鄰 48 個冷庫測試中通過。

新版對相同 snapshot 的**完整實跑也通過**：`2,843` 個物件、`25,000,595,368` bytes，manifest SHA `19c3b12644424265933d7074b27203bc3127336eab1851bb88350691ef290f65` 與 inventory SHA `b66184ee3b0076b7b2c982ae6b71f92ba62b797508c920ba58fd63033259ee97` 均和舊版一致；`--profile` 記 `elapsed_seconds=608.044`、`rchar_bytes=25,022,063,790`、`read_bytes=25,022,050,413`。對照舊版結束前約 44.5 GB，這一輪完整檢查的實際程序讀取量約少 **44%**。舊／新輪依序執行，OS 快取、Syncthing 掃描與其它服務負載不同；**不能把約 25 分鐘→10 分鐘當作已證實的穩定 wall 倍數**。`materialized_verified=false` 表示本輪只驗冷發布物件，並未宣稱某個使用端 hot cache 通過。

新版校驗完成後 06:07:51 再以既有 Syncthing 檢查器唯讀取得同時點狀態：本機 folder `idle`、need/error/pullError/watchError 全零；Vast peer 經 QUIC connected、completion 100%、remoteState valid。這證明該瞬間的傳輸狀態；Vast 是 index-only edge，不能從 100% 推論其 payload 已持久保存，也不能把本機 release 驗證稱為遠端 materialization。

06:08 再跑全服務唯讀收據 `artifacts/benchmarks/service-coverage-20260926T0608-cold-publish.json`：**57 service／42 timer／3 path**、無失敗的已註冊 service、`timer_schedule_findings=[]`；六個 localhost 產品 API 均 HTTP 200，但健康仍為 TAIFEX `blocked`、當沖 `degraded`、隔日沖／Shioaji／OpenBB `waiting`、全資料 `critical`。這是當下覆蓋與健康分層，不是所有業務資料已修好；同一次 5 秒資源樣本也不是長期延遲 p95。真冷啟動、`OnSuccess` 的下一次自然執行、Vast 端 payload/材料化驗證和未解資料缺口仍待後續驗收。

## 2026-09-26 05:00～05:05 公網雙棧證據與防誤判監測

本機 WSL 的 `curl -6` 連 DDNS 443 失敗，但 Windows `Ethernet 5` 當時持有 DNS AAAA `2001:b011:e610:5ac8:a519:a8c1:9c93:bef2`，Caddy 於 Windows `::`:443 監聽；Windows 對該位址及另一個偏好的全域 IPv6 位址直連 `/healthz` 都回 HTTP 200，已安裝 Caddyfile 的 SHA-256 也與版本庫一致。Windows 允許 Caddy TCP 80/443 的 inbound 規則仍啟用。**同機 WSL 失敗並不是外部 WAN 失敗證據**。

重新發起的獨立 Internet.nl IPv6 測試於 05:04:55 完成，`done=true, success=true`，當時 DNS AAAA 與 Windows 位址一致；機器收據為 `artifacts/benchmarks/dashboards/public-ipv6-audit.json`。同時本機經 DDNS 強制 IPv4 HTTPS `/healthz` 為 HTTP 200／單次約 47 ms。這只能證明**該觀測時點**的外部 IPv6 探針與 IPv4 路徑可用，不是全天候可用性或真正重開機恢復驗收；Windows 上該 AAAA 已標為 deprecated，有效期當時僅約 10 分鐘，因此 DDNS 輪換仍需後續持續觀測。

修正 `scripts/audit_public_ipv6.py`：外部探針在期限內未完成會寫 `inconclusive_timeout` 收據，而不是拋例外後失去當次證據；並記錄是否真的要求啟動探針，避免 `--reuse-latest` 冒充新一次測試。`scripts/audit_service_latency_coverage.py` 現在唯讀納入外部 IPv6 收據，只有近期、明確發起探針、目前 AAAA 與收據一致時才標 `external_pass_observed`；缺檔、過期、DNS 變更、重用舊結果及未完成探針均不會標綠。這不會自動修復網路，也不觸碰路由器／防火牆設定。相鄰 **29 個測試通過**、Ruff 與差異檢查通過。

新唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260926T0505-ipv6.json` 列 **57 service／42 timer／2 path**，`startup.public_ipv6_external.state=external_pass_observed`。六個本機產品狀態 API 都 HTTP 200，但健康仍是 TAIFEX `blocked`、當沖 `degraded`、隔日沖／永豐／OpenBB `waiting`、全資料 `critical`；HTTP 可讀不等於來源資料與成交正確。`systemctl --failed` 當下有一個 FinLab API 探針的 **transient** `run-p1510610-i1514526.scope` 失敗，並非正式 stockagent service；未清除、重啟或把它冒充所有服務正常。外部雙棧應持續定期重測；本輪未驗證 Windows 真冷啟動、長期延遲 p95 或所有資料缺口。

## 2026-09-26 04:51～04:55 公開訊號窄欄字典化與記憶體驗收

正式 gateway 上一版首次完整訊號請求後仍保留約 2.4 GB cgroup 記憶體。獨立進程對 2,004,142 筆同一快取與來源測試：`gc.collect()` 未回收物件，`libc.malloc_trim(0)` 只令 RSS 約 **2,041,856,000→2,041,516,032 bytes**（少約 0.34 MB）；這**不能證明記憶體洩漏**，也不能靠排程 glibc trim 根治 Polars／其它配置保留。`POLARS_MAX_THREADS=16/4/4/16` 四輪完整輸出 SHA 相同、峰值約 **1.875～1.915 百萬 KiB**，wall **1.31／1.31／1.88／2.54 秒**，不同時間點負載波動比候選差異大；**未**改正式 thread pool。

只在當沖全史窄投影的暫存 frame，將高度重複但**不參與排序**的 `signal_id`、`reason`、`status` 轉為 Polars Categorical；整頁輸出仍從已驗證 Parquet 回補原字串，沒有改帳本、shard、模型、成交或公開 DTO。未改排序鍵 `session_date／market／symbol`。隔離試驗首次與三欄轉換版的完整 SHA 相同；峰值 RSS 樣本約 **1.88～1.92 百萬 KiB** 對 **1.80 百萬 KiB**，wall 與同機負載混雜，不能稱穩定延遲改善。正式資料六種查詢（全期、offset 分頁、單模式、中文標的、blocked、短區間）對原完整寬欄路徑逐欄 JSON 相等；相鄰公開 gateway／全資料／當沖／OpenBB **386 個 Python 測試通過**，Ruff 與 diff 檢查通過。

04:55:14 僅重啟**唯讀公開 gateway** 載入該修正；新 invocation `c416de92eb994f3eb56b055cdcc86c84`、MainPID `2837480`、`NRestarts=0`。當沖引擎、Discord 與永豐 TAIFEX BidAsk 的 PID／invocation 完全未變。重啟後第一筆 localhost 全期訊號為 HTTP 200、2,004,142 筆、`scan_limit_reached=false`、相同完整公開 JSON SHA `8b79cc81f18e5f07e10c26965dcd63d49db15872b1b73f927ce9857a6730d1f9`，`Server-Timing build=1,093.468 ms`；當次 cgroup `MemoryCurrent≈2.193 GB`、`MemoryPeak≈2.194 GB`，對前一 invocation 的不同時點 2.39 GB **不是同負載穩定 A/B**。IPv4 公網 `/healthz` 與訊號 API 皆 200；後者已命中熱回應快取，不作冷延遲證據。健康狀態／IPv6／外網獨立客戶端／長期 p95 尚未因這項暫存欄位改動而驗收。

## 2026-09-26 04:44～04:50 OpenBB L1 stale 稽核：同快照驗證後排除假加速

正式 compaction 04:38 輪的 `stale_contract_audit=57.459s` 中，來源 JOIN 29.221 秒、逐個 Parquet metadata 檢查 28.212 秒；當時約 **155,011** 個 segment／**3,669,395** 個 member／**13,287,450** 個 task。用既有唯讀 `scripts/benchmark_openbb_l1_stale_scan.py` 在**同一 SQLite 讀交易**對完整來源稽核做 member-first／原 segment-first 比較，兩者皆 0 stale、結果 SHA `e3b0c442...b855`；前者 **22.690 秒**、後者 **21.752 秒**。這不是穩定加速，也沒有改正式 SQL 或 manifest。收據 `artifacts/benchmarks/openbb-l1-stale-20260925T2044.json` 保留查詢計畫、來源數與 RSS；現行 member-first 多出臨時排序，不能因其看似較少索引查詢就切換。

把**純唯讀**的 metadata A-B-C-C-B-A 量測加入同一腳本的 `--variant none --metadata-sample 10000 --metadata-workers 4`；每輪對相同 10,000 個正式 L1 segment 讀出完整列數和 schema fingerprint，逐檔有序 SHA 一致、**0 mismatch**，不只比較錯誤數或抽樣列值。正式收據 `artifacts/benchmarks/openbb-l1-metadata-20260925T2049.json`：原 `pq.ParquetFile` 單線程 **1.600／1.455 秒**，`pq.read_metadata` 單線程 **1.512／1.434 秒**，四線程原方法 **5.551／5.570 秒**。手動先前 10,000 檔的四線程約 5.61～5.84 秒、八線程約 6.69～6.77 秒，亦無吞吐收益。樣本和完整 155k 檔路徑不同；但併發在此負載反而約三倍慢，**未部署**；`read_metadata` 與原方法只有數十毫秒差、落在樣本波動內，亦不宣稱生產改善。新增壞檔故障注入回歸、相鄰 OpenBB L1 **18 個測試通過**、Ruff／編譯／diff 檢查通過。

這將下一步收斂到真正的資料結構問題：每輪不論任務是否改動，都對數百萬 member 做來源 JOIN、對十多萬個輸出逐一開檔。若建立 source-change journal／不可變衍生輸出證明來增量化，必須先證明所有 `tasks` INSERT/UPDATE/DELETE、段建立競態、崩潰重啟、manifest 代際與磁碟損毀檢出沒有失明，再用原查詢作 shadow parity；不能直接跳過 57 秒稽核。此輪未更改 downloader、正式 L1 compaction 或來源／冷發版契約，也未中斷 active OpenBB archive。

## 2026-09-26 04:40～04:42 部署後全服務資源基線與下一熱點

部署後唯讀 `scripts/audit_service_latency_coverage.py --sample-seconds 5` 收據 `artifacts/benchmarks/service-coverage-20260925T204050Z.json` 枚舉 **57 service／42 timer／2 path**、`timer_schedule_findings=[]`。五秒取樣中 `registered-data-intraday` 約 **2.994 cores／2.626 GB**、OpenBB L1 compaction 約 **1.341 cores／0.840 GB**，其餘當沖引擎約 0.040 cores、Discord 約 0.030、公開 gateway 約 0.0002；這是單次交錯負載，不是每服務長期平均或各功能延遲。六個 localhost 產品 API 都 200，但當時 TAIFEX `blocked`、當沖 `degraded`、隔日沖／Shioaji／OpenBB `waiting`、全資料 `degraded`；資料健康不能由 HTTP 200 推導為正常。`systemctl --failed` 當時只有另一個 FinLab 實驗的 transient scope，非已註冊 StockAgent service；保留其失敗證據，不清除旗標。

同輪 OpenBB L1 自然工作 04:38:36～04:41:02 exit 0、wall **146.415 秒**、CPU 約 **160.586 秒**、峰值約 **2.433 GB**；正式 `_state/l1_compaction_latest.json` 記 **260 個新 segment**、`failed_segments=0`、**3,648,247 個 pending files**，遠非完整壓縮。分段耗時為來源 stale contract **57.459 秒**（其中 SQLite JOIN **29.221**、既有 L1 輸出 metadata **28.212**）、segment build **56.824 秒**、unassigned source **21.304 秒**、query-view publish **5.618 秒**。唯讀 SQL plan 目前以 155,011 個 segment 為外層、約 3,669,395 個 member 依 `segment_id` 索引查詢，再逐筆 PK 查約 13,287,450 筆 task；換成 member-first 會多出排序暫存，不可未測即切換。下一階段需在同一只讀 SQLite snapshot 比較完整結果 SHA、冷／熱查詢與 I/O，才能更動此 57 秒契約稽核；不能跳過來源或衍生輸出驗證冒充吞吐改善。`registered-data-intraday` 仍在正式執行，未中斷或調高其下游配額。

## 2026-09-26 04:36～04:40 當沖窄查詢正式公開 gateway 載入

先對目前工作樹的公開 gateway、全資料監控、當沖與隔日沖相鄰 **368 個 Python 測試**做整合回歸，並以 `node --check` 驗證全資料前端 `app.js`／`provider.js`。在獨立 `127.0.0.1:18771` gateway 載入目前程式，六個產品狀態 API 全回 HTTP 200，資料健康仍如實為 TAIFEX `blocked`、當沖 `degraded`、隔日沖／Shioaji／OpenBB `waiting`、全資料 `critical`。正式 2/25～9/25 當沖訊號 API：臨時新版與原 8770 舊版各回 **2,004,142 筆、100 筆頁面、1,078,821 bytes**，完整公開 JSON SHA-256 都是 `8b79cc81f18e5f07e10c26965dcd63d49db15872b1b73f927ce9857a6730d1f9`；當次 `Server-Timing build` 約 **1,269／2,544 ms**。兩程序不同時序、cache 與負載，不以這一對數字宣稱穩定倍數；隔離 A-B-B-A 在下一節。

04:38:52 **僅重啟唯讀** `stockagent-public-dashboards.service`，新 invocation `79e685a743c74ec6ad1eafec26a53b81`、MainPID `2809065`、`NRestarts=0`；當沖引擎 invocation `3b313be6ed2d4258b0bb49f9a5d9d729`、Discord `7792ce6632e04efa86127ceb811ffc53` 和各自 PID 保持不變。載入後第一筆正式 localhost 全期訊號為 HTTP 200、`scan_limit_reached=false`、相同 1,078,821 bytes／相同 JSON SHA，`Server-Timing build=1,493.623 ms`，當次 cgroup `MemoryCurrent≈2.389 GB`、`MemoryPeak≈2.391 GB`，低於現有 4G／8G 控制線，但仍屬 GB 級記憶體。IPv4 公網 `/healthz` 與訊號 API 都回 HTTP 200；另一個 `limit=99` 非同頁快取請求 `Server-Timing build=445.252 ms`，**摘要快取已熱**，不能拿它冒充首次全期建置。這些是本機發起的 IPv4 路徑，未驗證外部獨立客戶端、IPv6 或長期 p95。

本輪臨時 gateway 的獨立測試程序與約 9 KB 的暫存匿名流量檔已停止／移除；正式效能收據及 canonical 帳本未刪改。後續仍需持續量測正式 gateway 冷／熱路徑及其它服務，並分開追查上述業務健康缺口。

## 2026-09-26 04:19～04:34 當沖全史訊號窄投影與有界回退

在**相同 canonical `signals.jsonl`、state、146 個逐日私有投影、2,004,142 筆**下，把「完整頁面」與「只算完整來源摘要」分開量測，避免用摘要速度冒充整頁速度。新增 `scripts/benchmark_tw_day_trade_signal_page.py --probe-narrow-summary`：先核對逐日索引 byte span、來源 dev/inode/size/mtime 契約、schema/receipt、Parquet SHA-256、列數及執行前後檔案簽章；只投影摘要必要欄位，並強制帶完整基線的摘要 SHA 與預期總筆數。若任一收據、hash、總筆數或來源簽章不符，非零退出。這是**唯讀實驗路徑**，不替代公開頁面，也不改帳本、交易或服務程序。

隔離進程正式資料基線：完整頁面 `complete=true`、100 筆、輸出 SHA `51a607dfd685a7c8fb516eb1b64a3b0a073e2f71ff88c6cfa5e00c09b3b5b7f9`，wall **2,343.216 ms**、峰值 RSS **2,345,316 KiB**，來源摘要 SHA `cb64c0624bd866002fb946adf74b7489d0a73ac58ca4f88e18c079c2698f5277`。獨立進程窄投影摘要使用同一總筆數、同一摘要 SHA，收據驗證 **34.878 ms**、窄載入 **125.974 ms**、完整摘要 **653.184 ms**，wall **915.988 ms**、峰值 RSS **1,690,156 KiB**；窄 frame 估計 **415,064,605 bytes**。此對照**只證明摘要可等價縮欄**，不含前 100 筆完整欄位回補、篩選或 overlay，故不能宣稱正式頁面已降到 916 ms 或 1.69 GiB。較早的逐 session Python 聚合雖低至約 336 MiB，卻要約 10 秒，不能直接採用；Polars streaming 聚合更快但末位浮點數不同，亦不能悄悄替換正式統計。

隨後把窄投影做成 `build_dashboard_signal_page` 的有界快路徑：每個逐日 shard 沿用相同來源／receipt／Parquet SHA 驗證，先只載入篩選、排序和完整執行稽核必需欄位，再按 top-K 的 `(session_date, projection_row)` 從命中的 shard 回補整筆資料；對跨日 schema drift 補齊型別與空欄，維持 `signal_source_path` 空值原本省略的 DTO 語意。僅 `offset+limit<=1000`、沒有形式化隔日沖訊號檔、逐日索引皆單一 span、所有快取驗證與來源前後簽章成功時使用；否則走原完整路徑。頁面回補中若 shard 變動，重新走舊路徑，**不回傳混合版或截斷資料**。沒有改模型、成交、canonical ledger 或衍生 shard 格式。

正式資料對照不只首分頁：完整頁面 offset 17、單模式、標的 `2330`、blocked 狀態與縮短日期範圍五種查詢，新舊路徑每一種**完整 JSON 逐欄相等**，總筆數分別 2,004,142／400,624／730／233,357／535,353。小型雙日／雙模式／舊新訊號 ID／中文標的／缺失欄位測試涵蓋分頁、模式、標的、狀態，另故障注入損壞一個 Parquet shard、把 shard 換成 symlink 及回補失敗，皆回退原正確路徑，不沿用不可信快取。完整 2,004,142 筆首分頁隔離程序 A-B-B-A：舊路徑 **2,346.725／2,143.914 ms**、峰值 **2,352,116／2,365,876 KiB**；新路徑 **1,281.233／1,236.353 ms**、峰值 **1,939,640／1,940,752 KiB**。四輪 `complete=true`、完整結果 SHA 相同；這是同機短樣本，不能宣稱長期 p95 或開盤／公網延遲同倍率改善。`scripts/benchmark_tw_day_trade_signal_page.py --legacy-full-projection` 可重跑舊路徑對照，預設量新路徑；另可加 `--probe-narrow-summary --expected-summary-sha <完整頁面輸出的 SHA>` 只量完整摘要，兩種都需同一日期、總筆數與快取目錄。

本節實作時正式公開 gateway **尚未重啟**，因此當時只宣稱程式回歸與隔離測速；後續完成相關整合驗收並載入的證據見上節。相鄰當沖／公開面板／benchmark **272 個測試通過**、Ruff／語法／diff 檢查通過，仍非全庫、IPv6 或真開盤日長期 p95 驗收。

## 2026-09-26 04:00～04:04 當沖全史訊號來源路徑去重（保留完整 200 萬筆）

上一節的逐 session 來源投影把 `signal_source_path` 從每個 frame 抽出後，仍在查詢期間保留**每一列的完整字串**，只為最後 100 筆頁面還原來源路徑。對已驗證的 146 個私有 Parquet session shard 唯讀計量：**2,004,142 列**的此欄位原始 Polars 字串緩衝估計 **446,957,668 bytes**；每 session 最多只有 **5** 個不同來源路徑。改為僅在 query 的記憶體索引使用 Polars Categorical，估計容量 **8,016,568 bytes**，頁面索引仍回原字串。沒有改 append-only ledger、shard 位元組、來源證明、排序、摘要、公開 DTO 或交易判斷。

正式資料隔離基準兩輪均 `complete=true`、**2,004,142** 筆、`scan_limit_reached=false`，完整結果 SHA-256 仍為 `51a607dfd685a7c8fb516eb1b64a3b0a073e2f71ff88c6cfa5e00c09b3b5b7f9`；wall **2,452／2,630 ms**、最高 RSS **2,317,460／2,342,432 KiB**。先前未去重的隔離樣本約 **2,363,084～2,379,788 KiB**；邏輯欄位少約 439 MB，**不代表**整個程序峰值會等量下降，因 Parquet 解碼、Polars 聚合、配置器保留與同機負載仍占大宗。觀察到的 RSS 差額約數十 MiB，尚需更多同負載樣本與正式 gateway 重啟後核對，不能稱為穩定倍率或根治 2.3 GiB 問題。

完整範圍嚴格 JSON／columnar 對照、摘要重用與訊號排序 **3 passed**，相鄰公開 gateway **110 passed**，Ruff 通過；目前正式 gateway **未重啟**，隔離基準已載入新版但公網程序尚未驗收。下一步仍是有界逐 session 摘要與 top-K：目前大宗來源投影、完整摘要和頁面排序仍各花約數百毫秒至數秒，不能透過刪歷史或隱藏部分訊號解決。

## 2026-09-26 03:44～03:57 當沖全歷史訊號：正確基線、分段測速與拒絕假加速

資源風險比每 30 秒約 2 秒的全資料 feature 投影更大：唯讀公開 gateway 當時 `MemoryCurrent≈1.76 GB`、本 invocation `MemoryPeak≈2.90 GB`，先前完整訊號請求後仍可能保留數 GiB 匿名記憶體。對正式 2026-02-25～09-25、100 筆首分頁做隔離量測時，**沒有**設定 gateway 的 `STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR`，建構器無法使用逐 session projection，耗時 **32.023 秒**、只掃 **100,000** 筆；這是明示 `scan_limit_reached` 的有界 fallback，**不是**可拿來對照完整歷史的基線。補上正式 gateway 同一索引目錄後，兩次原版隔離查詢完整覆蓋 **2,004,142 筆**、`scan_limit_reached=false`、100 筆輸出 SHA-256 均為 `51a607dfd685a7c8fb516eb1b64a3b0a073e2f71ff88c6cfa5e00c09b3b5b7f9`，wall **2.408／2.354 秒**、程序最高 RSS **2,370,040／2,363,084 KiB**。這是相同資料與索引目錄的兩筆樣本，不是長期 p95。

試過把多／空 target 與 actual 合併到單次 Polars select：小型嚴格 JSON／columnar 等價測試通過，完整輸出雜湊也相同，但候選兩輪 **2.687／2.765 秒**、記憶體無穩定下降；撤回候選後另一輪 **2.676 秒**，顯示同機負載／快取波動足以掩蓋微幅差異。**沒有保留或部署**這項未證明有收益的候選，也不把較少的邏輯掃描宣稱為真實加速。

為後續真正降低全歷史峰值，在 `build_dashboard_signal_page(..., timing_ms=...)` 加入純觀測分段，不改公開 DTO 或交易帳本；新增 `scripts/benchmark_tw_day_trade_signal_page.py` 以獨立程序輸出 wall、RSS、各階段、結果 SHA 與來源前後簽章。索引目錄缺漏、截斷掃描、來源改寫或指定總筆數不符時，benchmark 非零退出，不再把有界 fallback 誤報完整歷史。可重跑：`source scripts/runtime_env.sh && run_fintech_python scripts/benchmark_tw_day_trade_signal_page.py --cache-dir /var/cache/stockagent-public-dashboards --start-date 2026-02-25 --end-date 2026-09-25 --expected-total 2004142`；此路徑是本機已安裝 gateway 單元的觀測值，換主機須先讀已安裝單元，且工具可能更新**衍生的私有投影快取**，不改 canonical ledger。

03:56 正式資料的隔離腳本 `complete=true`、總筆數 **2,004,142**、同一輸出 SHA、來源簽章不變，wall **3,102.001 ms**、最高 RSS **2,370,696 KiB**；階段為日期 **112.249**、逐日來源投影 **1,228.571**、過濾 **22.760**、完整摘要 **1,036.373**、頁面排序／overlay **683.932 ms**。03:57 的另一次隔離輪把來源／state 五元檔案簽章一併輸出，仍為同一總筆數與輸出 SHA，wall **2,381.197 ms**、最高 RSS **2,379,788 KiB**；這反而更證明不可取單輪作 SLA。先前另兩輪顯示來源投影與摘要可能因同機負載波動約 **1.18～2.86 秒**與 **0.99～1.54 秒**。下一階段應保持同一 2,004,142 筆、摘要／排序／稽核雜湊等價，改為逐 session 有界聚合與 top-K，降低約 2.3 GiB 的隔離 RSS；不能只裁切歷史、跳過輸出欄位或以熱 HTTP 快取掩蓋第一次來源成本。相關公開 gateway **110** 個、benchmark **3** 個、完整訊號路徑 **3** 個測試通過，Ruff 通過；正式 gateway **未重啟**，本節沒有宣稱其生產記憶體已下降。

## 2026-09-26 03:38～03:44 feature 重投影成本拆解（觀測修正，非資料捷徑）

延續上一輪去除重複目錄發現後，為 30 秒正式 `stockagent-data-refresh-status-snapshot.service` 補上 disjoint 的 `feature_stages_ms`：快照重用檢查、footer／欄位投影、公開欄位投影、原子 JSON 寫入、SHA 收據。未改公開欄位、資料來源、刷新頻率、失效規則或交易服務。**03:39:43** 自然慢輪 exit 0、更新 78 個 footer、`feature_projection=2,257.839 ms`，其中 `reuse_check=0.119`、`footer_projection=1,193.869`、`public_projection=325.543`、`atomic_write=705.625`、`receipt=32.634 ms`；五階段與總時間相符。此輪共 87,615 個公開欄位，其中 `physical:finlab:downloaded-datasets` 佔 **81,301** 個；完整 JSON **54,576,289 bytes**。本機 API 在要求 gzip 時回 **1,404,227 bytes**、`Content-Encoding: gzip`，因此傳輸端已有大幅壓縮，不應把原始大小冒充實際公網傳輸量，也不應盲目再加一套 gzip。正式頁面只有欄位區啟用後才抓取，後續每 60 秒以 ETag 條件請求；資料變更時仍需對 87,615 欄解壓、解析與建搜尋索引，行動裝置的實際互動延遲尚未驗收。

同次唯讀 cProfile 顯示永豐監控建置約 **0.739 秒**、其中 `_history_manifests` 約 **0.314 秒**，短生命週期快照每次仍讀約 1,106 份 JSON manifest；這指出可驗證的後續增量索引候選，但不能據 profile 直接略過歷史 manifest，因回補覆蓋、配額時間線與去重仍依賴它們。`test_data_monitor_inventory.py`／`test_data_monitor_dashboard.py` **94 passed**、公開 gateway **105 passed**、Ruff 通過；正式 `/data-monitor/api/status` HTTP 200 但健康仍 `critical`，不是來源問題已解決。下一步應先設計每資料集可驗證的清冊增量投影與快照查詢分頁／差量契約，保留每檔身分及 footer 證明；不可把完整欄位從資料產品中刪掉來省時。

## 2026-09-26 03:25～03:35 全資料監控慢路徑去除重複目錄發現

`stockagent-data-refresh-status-snapshot.timer` 每 30 秒執行；57,296 個 Parquet 快取檔案一旦有變動，原流程先在可信 fast index 探測中發現所有目錄及檔案身分，接著慢路徑又對相同目錄做第二次 discovery。正式日誌顯示常見慢輪約 7～10 秒，資料清冊約 3.7～5.9 秒、feature 投影約 2.1～2.9 秒；快路徑約 2 秒。慢輪的數千個 footer 更新、42 MB cache 解碼與 87,615 個 feature 仍是真實成本，不能以快路徑成績取代。

現在只有在 fast index 本身通過版本、owner／mode、cache signature、checksum 驗證，且來源 fingerprint **確實變動**但 cache signature 仍穩定時，才把剛發現的路徑清單交給慢路徑；慢路徑仍逐檔重新 stat、只接受相符身分、讀取變動的 Parquet footer、檢查缺檔／in-flight 替換及重新建立公開結果。快索引損壞、快取不穩或檔案在 fingerprint 掃描中消失時仍重新 discovery，沒有信任舊的列數或欄位。增列 `quick_index_probe` 階段時間，避免把第一次 discovery 從測速中藏掉。

新程式由自然 timer 載入：**03:34:42** 慢輪 `refreshed_files=1,445`、`features=87,615`、`quick_index_probe=1,253.889 ms`、慢路徑 `discover=35.853 ms`（僅做 membership fingerprint，不再二次目錄發現；原同量級慢輪此階段約 0.7～1.1 秒）、inventory **4,677.445 ms**、全輪 **9,363.660 ms**，exit 0。03:30:41 的先前慢輪更新 1,542 檔、inventory **5,917.417 ms**、全輪 **10,616.748 ms**；來源變動量、OS cache 與並行負載不同，這兩筆**不是**穩定 A/B 加速倍率。清冊及相鄰公開面板／MOPS 測試 **212 passed**，包含「變更檔只 discovery 一次且列數更新」及原有缺檔、改組、競態、損壞快索引回歸。全庫、各種服務的長期 p95、來源健康與外網 IPv6 仍未因此驗收；下一階段應對 42 MB cache 解碼與 87,615 筆 feature 重投影設計可驗證的增量更新，而非跳過身份或 footer 檢查。

## 2026-09-26 02:58～03:22 Bybit 冷發布與分鐘下載協調（已驗證正式輪）

根因是 `refresh_crypto_training_dataset.py` 在發布前僅檢查當下的下載程序，無法阻止一秒後由 `OnUnitInactiveSec=1min` 啟動的下一輪分鐘下載。保留原有 source stat／SHA／全樹 fingerprint 的 fail-closed 驗證；在 catalog 的 `bybit` 項目登記 `source_coordination_lock`，正式 catalog publisher **取得鎖後重新檢查**活躍寫入者與收據，整個打包及 head 更新期間持有鎖。註冊的 Bybit 分鐘下載與其可選日物化共用同一鎖，最多等待 180 秒；超時仍失敗並留下原有 group／service 錯誤，不會假裝下載成功。鎖等待時間在 publisher stderr 與下載日誌可見。其他交易所的下載不持有此鎖，也沒有停掉 intraday timer、排除 `_hot_tail`、削弱冷發布驗證或改變交易服務。

正式驗收從 **02:58:14** 啟動 `stockagent-crypto-training-refresh.service`：394 標的日物化、特徵／稽核完成，publisher **02:59:08** 取得鎖；下一輪 intraday 於 **02:59:10** 啟動，Bybit 寫入直到 **03:00:23** 出版完才開始，而 Binance／OKX 仍可併行更新。正式服務 **03:00:23 exit 0**，持久 `refresh_receipt.json` 為 `completed`，全鏈約 **128.5 秒**、發布步驟 **74.474 秒**、峰值 **9.71 GB**。新 immutable head 是 `bybit-20260925T190010896981824Z-l0-penguin-798e3d129b95f6c1`，**4,677** 個來源檔、約 **22.35 GB** 邏輯資料，新增冷物件約 **359 MB**；下一輪 intraday 於 **03:00:53 exit 0**，且後續 03:01:54／03:03:52 正常輪的 Bybit 鎖等待各為 **6 ms**。本次撞期 Bybit 約等 **72 秒**才開始，沒有省略分鐘資料，但短暫的新鮮度代價仍需下一階段不可變來源快照才能移除；不能稱為零延遲。為了不讓舊實測誤讀，首次等待日誌 `wait_ms=72357180051` 是 `%3N` 在此系統輸出九位奈秒造成的單位錯誤，已改為奈秒除以 10^6，後續 6 ms 才是可信讀值。

`test/test_packed_snapshots.py`、catalog／鎖／刷新及掃描相關共 **38 項測試通過**，含「取鎖後重新檢查活躍寫入者」的回歸測試；Ruff、`bash -n`、`git diff --check` 通過。Syncthing 在 **03:02:36** 本機 `stockagent-packed` 為 idle／`needBytes=0`／錯誤 0，連線中的 `vastai1T` completion 100%／`remoteState=valid`；這只證明當時傳輸收斂，不證明遠端本機物化或訓練可用。新 exact release 以低 I/O 優先序做完整驗證，約 **18.5 分鐘**後 exit 0：**616 個冷物件／23,802,271,979 bytes** 均通過 SHA-256／ZIP pack 檢查，manifest SHA `8cc5f1e77294ba82b667c9e4b00b6b0a7e3581b4003737d5be9c637246133cba`、inventory SHA `798e3d129b95f6c17947de7f81be11436592bcb15a5a229d1327632e34d295ac`；`materialized_verified=false`，沒有驗證遠端物化。03:20:22 同步複核時本機 folder 正在掃描，雖 `needBytes/items/deletes=0`、所有錯誤計數為 0，且對端 completion 100%／`remoteState=valid`，**當下不能宣稱同步已 idle**，需等待掃描結束再複核。另有一輪 intraday 在修改其**正在執行**的 shell 腳本時於末尾報 `unexpected EOF`／exit 2；來源下載已完成，下一輪 02:54:55～02:55:52 自動成功。此部署風險應避免對執行中的 shell 腳本原地改寫；正式變更應在服務 idle 的間隔進行並驗收下一輪。協調鎖覆蓋註冊排程寫入；手動直接啟動個別 Bybit 下載器仍需避免與發布重疊，正式 publisher 的內容驗證仍會 fail closed。

03:06 再跑唯讀全服務覆蓋 `artifacts/benchmarks/service-coverage-20260926T0306-bybit-lock.json`：當下可發現 **57 service／42 timer／2 path**、`timer_schedule_findings=[]`，StockAgent 單元無 `failed`；六個本機產品 GET 均 HTTP 200，但健康仍忠實為 TAIFEX `blocked`、當沖 `degraded`、隔日沖／Shioaji／OpenBB `waiting`、全資料監控 `critical`。這是可用性／排程樣本，不是所有服務的來源品質、交易執行或外網雙棧通過。

## 2026-09-26 02:40～02:47 全服務稽核揭露 Bybit 發布與連續寫入衝突（未修復）

唯讀收據 `artifacts/benchmarks/service-coverage-20260926T0240-openbb-eta-gateway.json` 重新覆蓋 **57 service／42 timer／2 path**、`timer_schedule_findings=[]`。六個 localhost 產品 GET 都是 200，但健康仍為 TAIFEX `blocked`、當沖 `degraded`、隔日沖／Shioaji／OpenBB `waiting`、全資料 `critical`；**`stockagent-crypto-training-refresh.service` 是一個真實 failed 的 StockAgent unit**，不能因網站可連就宣稱全系統正常。

02:30 正式 Bybit 工作先完成 394 標的 funding、394 份增量日物化、公開特徵與品質稽核，約 02:30:55 進入既有 D: 冷發布。正式 publisher 02:31:56 因 `data_bybit/1m/_hot_tail/0GUSDT_features.parquet` 在打包期間變動而以 exit 2 **fail closed**；外層服務 exit 1、wall **116.743 秒**、CPU **326.567 秒**、峰值 **12 GiB**，持久 `refresh_receipt.json` 明確為 `failed`，沒有發布成功的 head 可由本輪宣稱。同期 `stockagent-registered-data-intraday.service` 02:30:56 自然啟動，Bybit 來源寫入至約 02:31:33；它是與發布重疊的連續寫入者。`active_raw_writers()` 在發布前檢查的是**當時 process**，02:30:55 沒有看見約一秒後即將啟動的下載器；單純將這次 systemd failure 清零或縮短驗證都會掩蓋競態。`packed_snapshots.py` 的來源前後 stat／SHA 與全樹 fingerprint 拒絕變動是正確保存契約，**不可移除**。

下一個真正修復需在 catalog 與正式發布入口下取得**可驗證的不可變來源視圖**，或讓下載器與發布共享一個有界協調鎖，且證明不犧牲連續分鐘擷取。僅移動 02:30 timer 會與每輪完成後約一分鐘再啟動的 intraday 排程重新競爭；只排除 `_hot_tail` 會漏掉尚未合併到 9/20 base Parquet 的已完成分鐘，因此都不是等價修復。此輪沒有停掉 intraday、重啟交易、忽略失敗或把 Bybit 冷發布標成完成；下次 crypto refresh timer 為 **16:30 台北時間**。正式失敗仍保留以便後續設計／驗證。

## 2026-09-26 02:29～02:35 正式公開 gateway 載入與 OpenBB 監控契約修正

重跑公開 gateway／全資料監控整合測試揭露真正的跨服務不一致：OpenBB L1 正式 systemd 每輪已改為 **32,768** 來源，但 `stockagent/live/data_monitor_dashboard.py` 仍寫 **2,048**，導致 ETA 顯示與安裝單元不符；同時舊 20 分鐘「最長單輪」少於正式 `TimeoutStartSec=40min`，且沒有算 `OnUnitInactiveSec=30min`／`RandomizedDelaySec=2min`。先前的 ETA 是錯的，不能因為 UI 顯示 `waiting` 就認為監控正確。現在監控採同一批次／排程／timeout 契約，每輪用 **32,768** 筆、30 分鐘等待、最多 2 分鐘排程抖動與 40 分鐘執行上限計算保守容量時間，明示「沒有新來源且沒有交易窗口延後」、`confidence=low`；不把它稱為實測完成時間或絕對上界。相鄰測試先 **1 failed／162 passed** 定位這項契約漂移，修正後 **163 passed**，並斷言 service、timer 模板與監控常數一致。

`stockagent-data-refresh-status-snapshot.timer` 於 **02:32:57** 自然觸發，正式 snapshot **02:33:35 exit 0**。持久 `public_status.json` 的 OpenBB L1 進度為 **3,538,323／7,317,642**、待辦 **3,779,319**；公開 `/data-monitor/api/status` 亦回相同數據與新版低信心 ETA。這只是無新增來源假設下的保守容量估計，實際供應商恢復、尾段不足 32 檔或交易窗口讓路都可改變完成時間；頁面整體仍如實為 `critical`。

隨後將同一份 `refresh_services.json` 的**下一次 systemd timer 觸發、上一輪 start／complete、timer active/result** 接入 OpenBB L1 ETA：等待中的正式 timer 才採用已觀測的上一輪執行秒數，工作正在跑顯示 `active/in_progress`，timer 停用則顯示 `deferred/timer_inactive` 而不捏造完成日期；缺少可信 runtime 時才使用上述明示條件的靜態保守容量估計。02:38:59 自然快照與公開 API 回報上一輪 **158 秒**、每輪 **32,768**、下一次真實排程，低信心剩餘 **240,211 秒（約 2.8 天）**；這不是穩定吞吐或完成承諾。新增執行中／停用／實際排程回歸測試後，公開及監控整合 **164 passed**，Ruff 通過。

在 30 個當沖 dashboard 測試、163 個公開／監控測試與真實舊版 HTTP JSON 完全相同的驗證後，**02:33:49 僅重啟唯讀 `stockagent-public-dashboards.service`** 載入上節的窄鍵連接修正。正式當沖全期間首分頁回 **200／1,078,821 bytes／2.569 秒**，請求前後 cgroup 記憶體約 **548→2,788 MB**、峰值 **2,789 MB**；與上一輪不同時間的舊 gateway 數值不是同條件 A/B，不能聲稱穩定延遲倍率。公開 gateway `/healthz` 200、IPv4 HTTPS 的當沖頁與全資料頁各 200；當沖仍 `degraded`、模擬 only。當沖、隔日沖及 TAIFEX dashboard 三個相依服務的既有啟動時間未變且 `NRestarts=0`，沒有重啟交易或行情。約 2.8 GB 的全歷史記憶體峰值仍是下一階段有界逐日聚合的工作，不能因低於 4 GiB soft limit 就算解決。

## 2026-09-26 02:03～02:18 公開訊號全歷史查詢的記憶體風險與局部修正

正式公開 gateway 在一次完整歷史訊號查詢後常駐記憶體逼近 4 GiB 的 soft limit。先區分 cgroup 與 HTTP 快取：舊程序約 **3.83 GB MemoryCurrent**，其中 **3.78 GB anonymous**，當時 HTTP response cache 僅約 **82 MB**；縮小回應快取不能解釋或根除主因。訊號 canonical JSONL 為約 **5.2 GB／2,004,142 列**。用相同持久索引，在僅綁 `127.0.0.1:18770` 的臨時 gateway 重現 `2026-02-25～09-25`、全部模式、100 筆首分頁：預熱後 RSS 約 **601,140 KiB**，請求後 **3,054,344 KiB**，HTTP 200／**1,078,821 bytes**／**5.635 秒**。只有呼叫不使用完整逐日 projection 的獨立建構器，峰值約 533 MiB，不能拿它冒充正式全歷史成本。

追到 `_summarize_columnar_signals` 原先把 200 萬列的**寬欄位**與每個交易日／模式的目前訊號 ID 做左連接，只為判定一個布林遮罩；這會複製大量與判定無關的欄位。現在只以 `session_date/market/signal_id` 三欄做保序連接產生遮罩，再套用於原本的摘要欄位。沒有裁切交易日、訊號或欄位輸出，也沒有更改 canonical 帳本、填單或回報健康。舊正式程序仍載入舊版時，對**同一真實查詢**做新版程式／舊公開 HTTP 結果比對：完整消毒 JSON **完全相同**，皆 **2,004,142 total、100 rows**。獨立新版量測峰值 **2,368,984 KiB**，舊版 **2,618,160 KiB**；相同端口型別臨時 gateway 新版預熱後 RSS **600,120 KiB**、請求後 **2,829,780 KiB**，HTTP 200／同樣 1,078,821 bytes／**2.932 秒**。兩次請求依序進行，OS page cache 與別的服務負載不同，**不能把 5.635→2.932 秒當穩定 A/B 加速倍率**；可支持的是等價輸出與約 220～250 MiB 的這次記憶體下降。仍約 2.8 GiB，後續應將跨日摘要改成有界逐日聚合，而非宣稱全歷史記憶體問題已完全解決。

相鄰 dashboard／columnar 測試 **30 passed**、Ruff 與差異檢查通過。臨時 gateway 已停止；`stockagent/live/tw_day_trade_dashboard.py` 的修正**尚未重啟正式 gateway 載入**，因同時有其它工作正在修改並多次重啟 `scripts/serve_public_dashboards.py` 與全資料頁，避免把其未驗收改動一起部署。待該工作穩定後，仍需正式重啟、重測同一查詢前後 RSS／延遲、公開回應與其它路由，再宣稱 production 生效。

## 2026-09-26 01:31～01:52 OpenBB L1 固定稽核成本攤銷：正式批次吞吐調校

先對**同一份 17.6 GB 正式 SQLite manifest** 做唯讀、結果雜湊一致的 SQL A/B：現行 `segment_first` 查 0 個 stale 約 **24.027 秒**，`member_first` 約 **27.191 秒**；只增 SQLite cache 到 64／256 MiB 分別約 **36.798／26.393 秒**，256 MiB mmap 一輪約 **22.388 秒**。後三者的順序、OS 快取和同機負載不同，微幅 mmap 差距不足以抵銷資源與不確定性，故**未改 JOIN、索引、cache 或 mmap**。重測收據位於 `artifacts/benchmarks/openbb-l1-stale-sql-20260926T0131.json`、`...T0132-cache64.json`、`...T0133-cache256.json`；mmap 試驗為唯讀臨時連線，未安裝到正式程式。

反而找到可避免重複固定工作的大宗：原 timer 每輪約 30 分鐘後才重試，而一次完整 stale 契約及衍生輸出驗證需約 50～65 秒，原 service `--max-source-files 2048` 對 **約 387 萬**未壓縮 shard 每輪只推進 2,048 筆。只調高**單輪來源上限**，保持每段 128 檔／16 MiB 來源／32 MiB 未壓縮／50 萬列、`--archive-idle-only`、開盤與延遲撮合保護、2 threads、DuckDB 1 GB、2.5 GiB soft／3 GiB hard、40 分鐘 timeout，以及 L0 不刪除。模板與已安裝 systemd 單元同步改為 **32,768**；無需更改 downloader 或來源／衍生校驗，亦未重啟 OpenBB 下載器、行情或交易服務。

| 正式輪次（台北） | 來源上限／實際新增 | wall／CPU | cgroup 記憶體峰值 | 完成後 pending |
| --- | ---: | ---: | ---: | ---: |
| 01:34:49 自然輪 | 2,048／2,048 | 81.3／88.2 秒 | 1,579,098,112 B | 3,869,431 |
| 01:41:51 受控輪 | 8,192／8,192 | 94.3／103.0 秒 | 1,759,756,288 B | 3,861,239 |
| 01:45:13 受控輪 | 16,384／16,384 | 109.4／116.0 秒 | 1,848,987,648 B | 3,844,855 |
| 01:49:32 受控輪 | 32,768／32,768 | 149.2／164.9 秒 | 2,215,620,608 B | 3,812,087 |

四輪均正式 systemd **exit 0**，待辦減少量與已壓縮檔增加量逐輪相等；最後一輪新建 **260 段**，`stale_segments=0`、`failed_segments=0`、`deferred_failed_segments=0`、`l0_deleted=false`，42 個 query view 發布且原有 `economy.fred_series` schema 超限的 1 個 view 仍明確 deferred。32,768 輪的 `memory.high` 事件 0、swap 0、memory full stall 0，距既有 soft limit 仍約 **469 MB**；來源選取 30.324 秒、建段 54.699 秒、完整 stale 稽核 54.389 秒。每輪壁鐘處理率由約 **25.2 → 219.6 檔／秒**，約 **8.7 倍**，但這是四個依序執行、資料與快取不完全相同的正式樣本，不是隔夜 p95 或供應商下載提速。若來源不再增加、下載器保持 idle 且 30 分鐘間隔能持續觸發，現有約 381 萬待辦的理想清空期由原配置約數十天降至約 **2～3 天**；條件一旦不成立就不能套用這個外推。

本輪 `test/test_openbb_l1_compaction.py` **17 passed**，systemd 單元語法、`git diff --check` 通過；公開 `/openbb/api/status` 仍如實回 `waiting`，compacted／pending 與正式收據相符，OpenBB downloader invocation 未改變。現保持 32,768，不再盲目上調：距 soft memory limit 只剩約 0.44 GiB，還須看下一次**自然 timer**、上游重新下載後的讓路、長期 memory／I/O 壓力與交易窗口前阻擋是否持續成立。

部署後唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260926T0155-openbb-batch32k.json` 覆蓋當時 **57 service／42 timer／2 path**，`timer_schedule_findings=[]`、沒有 `failed` 的 StockAgent unit；OpenBB 本機狀態 GET 約 **3.98 ms**／`waiting`。其它產品仍如實標 TAIFEX `blocked`、當沖 `degraded`、隔日沖／永豐 `waiting`、全資料 `critical`，故 L1 吞吐改善不等於整個服務群或來源健康完成。下一次自然 timer 預定 **02:22:34 台北時間**，其當時來源量、記憶體與錯誤仍須另驗。

自然 timer 已於 **02:22:34** 如期觸發，正式服務 **02:22:35～02:25:13** exit 0／Result=success，wall **158.879 秒**、CPU **166.021 秒**、cgroup 峰值 **2,256,429,056 bytes**，保持在 2.5 GiB soft limit 以下。正式摘要：`new_segments=261`、`compacted_files=3,538,323`、`pending_files=3,779,319`，相對前一輪待辦減少**恰好 32,768**；`stale_segments=0`、`failed_segments=0`、`deferred_failed_segments=0`、`l0_deleted=false`、42 views。`l1_compaction_latest.json` 的各步驟資源收據顯示 `cgroup_memory_high_events=0`、`cgroup_swap_bytes=0`；同時仍有約 **1.14 秒 memory full stall**、**5.16 秒 I/O full stall**，故不能說完全無壓力。這驗證一輪**自然排程**確實承接 32,768 設定，不足以證明供應商恢復下載後、長期 p95 或開盤前資源讓路都已驗收；仍保持目前上限而不再上調。

## 2026-09-26 01:26 現行熱點重驗：拒絕無收益並行與虛假的資料修復

目前公開全資料摘要有 **414 個啟用資料端點：224 complete、122 catching_up、68 unable**，完整性自檢的 DTO 契約違反數為 0；這不是「68 項都由程式效能引起」。明細中的台灣公開資料 31 項 `unable` 大多是候選歷史值缺少原始數值版本／發布時點驗證，另有 MOPS 申報時刻與更新授權門檻；crypto reference 16 項則含尚未建置的管線與部分場館歷史缺口。改 UI 分母、取消驗證或替候選值補上猜測時間，會把資料健康弄假，不能列為優化。實存 Parquet 清冊仍有 **19 個 footer 無效檔**（17 個 `data_yahoo/crypto`、2 個舊 `data_parquet`）；兩個 catalog 雖設定可發布，D: 冷儲存當前及歷史 head 均沒有 `yahoo-market`／`legacy-parquet` 可供直接恢復，故本輪沒有覆寫、刪除或宣稱已恢復來源。

永豐儲存監測最近正式輪 **4,587,114 檔／39.447 秒 wall／43.783 秒 CPU**，與早期 4,587,108 檔／183.376 秒不同，但同一新版 23:09 也曾花 **126.296 秒**，不能把單輪差距當穩定倍率。固定 `data_tw_microstructure/captures` **165,176 檔／7,250,320,592 bytes** 的唯讀測試，現有 GNU `find`＋Python 精確日期聚合為 **0.912 秒**，再串 `awk` 只做檔數／位元組加總為 **0.98 秒**；後者功能還少了 30 個台北日期桶，且沒有變快，因此**不採用**第二條原生管線。現有原生遍歷已避開舊版逐檔 Python stat 大宗；未有等價且穩定較快的證據前，不以省計算為由刪掉資料群組或日期精度。

OpenBB L1 壓縮最新自然輪於 **01:02:40～01:04:13** exit 0，約 **93 秒 wall／97 秒 CPU**、峰值 **1.63 GB、0 swap**、新建 20 段、0 stale；前次約 294.6 秒與其來源量／併行負載不同，不能將差額全歸單一變更。最新分段顯示大宗是 `stale_contract_audit` **73.661 秒**，其中 SQLite 來源契約查詢 **35.392 秒**、153,223 個既有 L1 輸出的 metadata 掃描 **38.254 秒**；曾較慢的 view 發布現為 **5.7 秒**，因此已不是第一熱點。對相同 **5,000 個成功段**做完整 row count＋schema 指紋唯讀重驗，單執行緒兩輪 **3.711／6.706 秒**，4 執行緒 **3.788 秒**、8 執行緒 **5.544 秒**、2 執行緒 **5.017 秒**，5,000／5,000 均有效；執行緒數沒有穩定收益，且 host/I/O 負載使重測波動，故未將 15.3 萬檔驗證並行化，也未跳過來源或衍生層校驗。下一個可檢驗的成本是來源契約 SQL 與 metadata 開檔的查詢／I/O 形狀，而非提高執行緒或以熱快取掩蓋完整性。

## 2026-09-26 00:57～01:00 OpenBB 等待上游狀態與公開頁驗收

OpenBB 正式下載器與 supervisor 均仍存活、invocation `c003edcbc0434efc98df60fb30f09a1b` 未變；但最新 `provider_scheduler.json` 為 `phase=waiting`、`wait_reason=provider_cooldown`，下一次**最早**可重試時間約 **09-26 04:05 台北時間**，本輪新嘗試仍為 0。原公開投影只因 PID 與活動心跳新鮮就標 `health=active`，頁面顯示「下載中」，且在兩份階段檔都新鮮時固定偏向 downloader 舊 `download`，可蓋過較新的 scheduler `waiting`。這會把供應商配額等待冒充成有效下載進度；進程活著與任務有產出必須分開。

`stockagent/live/openbb_archive_dashboard.py` 現依兩份可信階段檔的**實際更新時間**選最新、忽略超過既有 10 分鐘新鮮度界線的舊狀態；只有 supervisor 與 downloader 都活著、活動證據新鮮且最新階段確為 `waiting` 時才顯示新 `health=waiting`。只投影已消毒的等待原因與可解析的最早重試時間，不公開上游原始錯誤／憑證。`services/openbb_archive_dashboard/app.js` 將此狀態標為「等待上游」、使用警示色、顯示供應商配額冷卻與「最早重試」，不誤報成程序停止。**沒有**縮短配額冷卻、改任務規劃、提高請求速率或改變下載內容。

相鄰公開 gateway／OpenBB supervisor／dashboard **119 tests passed**，Ruff、Node 語法、`git diff --check` 通過。僅重啟唯讀 `stockagent-public-dashboards.service`，00:57:56 起 `active/running`；實際 localhost `/openbb/api/status` 回 `waiting/provider_cooldown`、兩程序仍活著。公開 IPv4 HTTPS 頁回 200，並透過 Chrome 檢查 **1366×768** 與 **390×844**：狀態文案正確、無主頁水平溢出與 JS console error，1h 曲線按鈕 API 成功；可重測收據與截圖分別在 `artifacts/benchmarks/openbb-browser-20260926-{desktop,mobile}/`。兩次按鈕至畫面量測約 **41.4／42.8 ms**，只是當次 browser path，不是對照組或全路徑性能提升證據。桌面稽核仍報 16 個低於 40 px 的觸控目標，手機本次為 0；資料完整稽核快照仍逾時，封存右界仍停在 **2026-07-18**，不得把狀態標示修正當成 8.9M 待辦已下載。

部署後全服務唯讀收據 `artifacts/benchmarks/service-coverage-20260926T0100-openbb-waiting.json` 對當時可發現的 **57 service／42 timer／2 path** 顯示 `timer_schedule_findings=[]`，OpenBB 產品探測已為 HTTP 200／`waiting`，全資料監控仍 `critical`。與 00:39 基線相比多出的三個 FinMind service、兩個 timer 是同一工作樹的其它並行維護產物，**不是**這次 OpenBB 修改造成的服務增加；測試與截圖只驗了 OpenBB 頁，不能概括成全部 57 項服務無故障。

## 2026-09-26 00:33～00:39 TAIFEX 正式輪恢復與新版本驗收

前一輪 TAIFEX 官方歷史資料發布在 D: head 更新後遇到 Syncthing 明確掃描逾時，留下 `failed` 的 systemd 結果；先前的掃描收據補償與舊版本本機校驗，不能代替下一次正式輪成功。離開交易時段後，執行原有 `stockagent-taifex-public-history.service` 一次；官方來源抓取、驗收、冷發布與掃描均走正式入口，**00:36:20 退出 0／Result=success**，systemd 記錄 wall **3 分 16.465 秒**、CPU **19.250 秒**、峰值記憶體 **207.1 MB**。沒有僅用 `reset-failed` 消除錯誤，也沒有重啟行情／交易服務。

本輪新 head 指向 `taifex-public-history-20260925T163533820866595Z-l0-penguin-ff468a5e561f191a`。以 `scripts/packed_snapshot.py verify` 對**該 exact release** 驗證通過：**298 個物件、230,590,817 bytes**，manifest／inventory 雜湊一致；`materialized_verified=false` 表示沒有驗證本機熱資料實體化，不可混用。待掃描收據為空。隨後本機 Syncthing `stockagent-packed` folder 為 `idle`、`needBytes=0`、待傳項目／刪除／folder errors／pull errors／watch error／system errors 全為 0；連線中的 `vastai1T` 回報 completion **100%**、`needBytes/items/deletes=0`、`remoteState=valid`。這證明當時冷位元組傳輸收斂，**不證明**對端已 materialize、在其持久磁碟驗證或能用於訓練。

唯讀全服務基線 `artifacts/benchmarks/service-coverage-20260926T0040-taifex-recovered.json` 涵蓋 **54 service／40 timer／2 path**，`timer_schedule_findings=[]`。六個 localhost 產品探測皆回 HTTP 200，但 TAIFEX 模擬產品仍 `blocked`（到期結算被未平模擬避險部位阻擋、即時來源停在 9/18），當沖 `degraded`、隔日沖與永豐 `waiting`、全資料監控 `critical`；TAIFEX 公開歷史下載恢復**沒有**修復獨立的模擬帳本與即時行情。全資料監控當時仍有 65 `unable`、90 `catching_up`、16 項需憑證處理與 2 項實體無效清冊，應逐一依正式來源與證據修復，不能改 UI 狀態掩蓋。

## 2026-09-26 00:10 D: 待掃描補償排程與 Binance 正式續跑

TAIFEX 的 D: 發布掃描逾時留下 `scan-pending/*.json`，原本只有同資料集再次發版才會自動重試；即使 300 秒整庫 rescan 最終送出位元組，也可能留下無人處理的待掃描收據與 failed 工作。新增 `scripts/retry_packed_syncthing_scans.py`，由 `stockagent-d-cold-scan-retry.timer` 每五分鐘執行一次：先用 canonical `mount_packed_d_cold.sh --check` 驗證 D:，只列舉安全的正式 JSON 收據、拒絕 symlink，每輪**最多一個資料集**，再呼叫既有 `scan_after_publish(..., retry_full=True)`。該函式持有原資料集鎖，仍依記錄先掃物件、再掃 manifest/head；API 失敗保留收據並使單元非零退出。成功的狀態只叫 `scan_request_acknowledged`，收據明示 peer convergence／release verification `not_checked`；不下載來源、不重建 head、不刪除冷物件，也不清掉原發布工作的失敗歷史。

安裝器 `scripts/install_packed_scan_retry_service.sh` 已渲染並驗證 systemd 單元；00:10:07 正式首次執行 exit 0、`idle_no_pending`，腳本內約 **0.095 秒**。00:15:07 自然 timer 第二輪亦成功、腳本內約 **0.085 秒**；00:25 第四輪後下一輪排在 **00:30:09**。這些輪次都沒有待掃描收據，不能把 idle 當成生產 pending case 已驗收；前輪 TAIFEX 的真實 18 路徑手動重試成功和新增的五項故障注入測試分別提供功能證據。Binance／Syncthing／重試／開機契約／限速相鄰測試 **66 個通過**，Ruff、編譯、shell 語法、已安裝 systemd 單元驗證及差異檢查通過；全庫未跑。00:25 全服務唯讀收據 `artifacts/benchmarks/service-coverage-20260926T0016-retry-deployed.json` 覆蓋 **54 service／40 timer／2 path**、`timer_schedule_findings=[]`；六個本機產品狀態仍有 TAIFEX `blocked`、當沖 `degraded`、隔日沖／Shioaji `waiting`、全資料 `critical`，不能由新 timer 成功推導其它服務健康。

Binance 新輪次完成 4,011 標的來源探索後，正式 `download_and_validate` 於 **00:21:33** 完成並 exit 0；最終 `download_summary.json` 記本輪待處理物件 **2,791／2,791** 已完成、`failed_objects=0`、`cycle_state=complete`，進度耗時約 **588.84 秒**。但 `state=partial` 仍是正確的：持久帳本有 **1 筆 `quarantined_source_invalid`**（`BTCUSDT_210326` 的 2021-02-03 官方日檔含 1 筆無效 OHLCV）與 **1,355 筆 `quarantined_repair_required`** 月檔。後者雖有日檔重建路徑，尚未對每個月做足以解除隔離的完整覆蓋證明；不得把成功退出、已下載物件數或來源隔離改寫成整段歷史完整。

## 2026-09-25 23:31 Binance S3 清單截斷的 fail-closed 重試

先前 23:12 啟動的正式 Binance 封存，在 `discover` **1,652／4,011** 後停住，23:31:38 以 exit 1 結束（wall **19 分 31.949 秒**、CPU **52.863 秒**、峰值記憶體 **417.4 MB**）。systemd traceback 定位在 S3 清單 `urlopen(...).read()`：chunked response 不完整，拋出 `http.client.IncompleteRead`；原 `_get` 只重試 `TimeoutError`、`URLError`、`OSError` 與指定 HTTP status，故一次可重試的位元組傳輸中斷直接終止全部 4,011 個標的探索。持久 `progress.json` 仍標 `running`，與 process exit 1 矛盾。這不是容量、安全保留線或資料檢查通過的證據。

現在對唯讀／可重試的 S3 GET，把 `HTTPException` 納入原有的限速、最多設定次數的退避重試；僅當**完整 response** 成功讀取後才解析 XML 或接受封存內容，不會用截斷位元組補資料。若某一標的在所有重試後仍失敗，取消尚未開始的其它清單 future，等待已執行的 worker 結束，將持久探索狀態標為 `failed` 並維持非零退出；不發布部分 plan。新增故障注入覆蓋「首輪 chunk 截斷、次輪成功」、「重試耗盡仍失敗」及「探索失敗收據」，連同相鄰限速測試共 **49 個通過**，Python 編譯、Ruff、`git diff --check` 通過。

23:51:52 正式單元以新 invocation `b2a9427fcbc2494881ccbe107bb23cc1` 重新執行；23:58 持久進度 `discover` **1,172／4,011**。這只證明新版能開始運作且如實回報進度，尚須等全探索、容量再檢、checksum/Parquet 驗證與最終 `download_summary.json`，才能驗收整輪結果。前一輪的錯誤沒有被改寫成成功。

## 2026-09-25 22:38～23:11 D: 冷儲存切換後的 OpenBB 恢復與 TAIFEX 掃描修復

新的儲存契約以 D: 作為唯一的本機冷儲存實體，不能再把先前 C／D 雙份備份的描述當成目前狀態。`scripts/mount_packed_d_cold.sh --check` 通過；根碟可用約 **522,906,947,584 bytes**，已高於 OpenBB 原有 **107,374,182,400 bytes** 安全線。OpenBB 前次 15:15 退出 2 的直接原因仍是當時根碟僅有 **103,404,191,744 bytes**，不是下載器已完成或 provider 健康。唯讀 preflight 確認此既有封存的右界仍釘在 **2026-07-18**，不會因今天重啟而冒充已封存至 9/25。22:38:48 啟動原 `stockagent-openbb-archive.service` 後，正式 supervisor 與下載器程序均在同一 cgroup，invocation `c003edcbc0434efc98df60fb30f09a1b`，原容量前置檢查已通過。22:40 的全服務唯讀稽核 `artifacts/benchmarks/service-coverage-20260925T2240-openbb-resumed.json` 覆蓋 **52 service／39 timer／2 path**；OpenBB 公開 API 為 HTTP 200／`active`，但資料監控仍 `critical`。下載器恢復既有 plan，回報 **8,909,579 個 active tasks**；後續 watchdog 為 `waiting/provider_cooldown`、新嘗試數 0，故本次只證明監督與續跑能力，**沒有**證明新資料已完成或歷史覆蓋足夠。

同一稽核揭露 `stockagent-taifex-public-history.service` 17:44:31 退出 2：當日官方公開資料抓取與來源契約檢查已完成，D: 冷發布完成 head 原子更新後，Syncthing 對 `head-history/...json` 的明確掃描請求逾時；不是官方資料抓取失敗。`/srv/stockagent-packed/.local-state/scan-pending/taifex-public-history.json` 保留了 18 個待掃描路徑。待 Syncthing 的大範圍掃描轉為 idle 後，使用既有 `scan_after_publish(..., retry_full=True)` 對**相同已發布路徑**安全重試，回傳 `True` 且待掃描收據消失；沒有重抓資料、改寫 head 或清除失敗紀錄。再以 `scripts/packed_snapshot.py verify` 驗證 exact release `taifex-public-history-20260925T094229676352787Z-l0-penguin-96544e211c1be2c6`：**282 個物件、230,262,193 bytes**、manifest 與 inventory checksum 通過。當時本機 folder `needBytes=0`、peer 回報 `completion=100`／`remoteState=valid`，但 folder 查詢時正處 `scanning`，依正式同步契約**不能宣稱已完整收斂**；系統服務也仍保留舊的 `failed`，下一次排程是 9/28 17:30。後續仍需在同步真正 idle、所有錯誤計數與 peer 證據通過後驗收；跨資料集 pending-scan 的有界自動重試已在上方下一輪實作。這輪未改動 Syncthing 掃描／發布語意，也未重啟行情或交易服務。

Binance 公開封存的上次 12:30 單元雖顯示 systemd `Result=success`，實際 `ExecMainStatus=75`；容量收據清楚標記 `accepted=false`、未執行遠端探索。D: 切換釋出根碟空間後，23:12:06 用原有鎖與保留線啟動正式單元；新 `data_binance_archive/capacity_receipt.json` 記錄 `accepted=true`、可用 **522,866,450,432 bytes**、10% 保留線 **216,336,110,387 bytes**。23:13 的持久進度為 `discover` **284／4,011** 個 instrument、`state=running`；只證明這次確實越過前置容量阻擋並開始來源探索，尚未證明所有壓縮檔下載或最終驗證完成。這項排程的 exit 75 必須繼續在營運稽核視為非零結果，不能因 systemd 接受它為正常停止就算資料健康。

23:20 再次完整唯讀稽核 `artifacts/benchmarks/service-coverage-20260925T2318-archive-recovery.json` 枚舉 **53 service／39 timer／2 path**、`timer_schedule_findings=[]`；本機六個產品 GET 均為 HTTP 200，但健康仍依序為 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、Shioaji `waiting`、OpenBB `active`、全資料 `critical`。Binance 封存仍在正式 `discover` 階段，23:20 的持久進度 **1,652／4,011**；OpenBB 正在 `provider_cooldown`，兩者皆不能由程序運轉推導資料完整。TAIFEX 掃描收據已修復，但該 systemd 單元的上次結果仍是 `failed`；下次正式輪次及同步 idle 驗收尚待完成。

## 2026-09-25 15:00 官方歷史重抓的相同日期快路徑與分鐘排程重掛

14:50、14:50:30、14:51 的正式 `institutional_initial` 收據都顯示 `twse_institutional_trades` 改變，TWSE 原始回應的 SHA-256 也依序變動；14:52 正式輪則 `changed_dataset_count=0`。因此不能把前幾輪一律當成下載時鐘造成的假更新，亦不能跳過來源修訂。下載器原本即排除 `_downloaded_at_utc` 比較穩定欄位，但**每次先解碼並合併整份約 350.7 萬列歷史檔**，才判定是否無變動。

現在只對一般按 `date` 覆蓋的 Parquet，在共同欄位 dtype 相同、沒有新欄、受影響日期所有非下載時鐘欄位與列序相同、且現有檔日期無空值並已排序時，從 Parquet metadata 返回總列數，不讀全量資料也不覆寫檔案。舊官方欄位可在當日原本全為 null 的前提下補 null 比對：實際 9/24 TWSE 原始回應 **27 欄**、歷史檔 **32 欄**，TPEx 為 **32／33 欄**。來源數值、網址／來源欄位、新欄／共同欄位型別、新日期、未排序檔、snapshot vintage、append-only payload、顯式 refresh 與 TWSE OHLCV 畸形日期修復仍走原完整合併；這是相同資料的局部加速，不改發布契約。當前約 350.7 萬列 TWSE 檔的舊同日期合併判定約 **1.109 秒／5,561,556 KiB 峰值 RSS**；使用真實官方 9/24 raw 重建 incoming，逐欄先確認完全相同後，新判定約 **0.095 秒／285,564 KiB 峰值 RSS**，原 inode／mtime 不變。約 258 萬列 TPEx 同法為 **0.037 秒**且原檔不變。這是單程序 hot-file A/B，不是整個 8 資料集排程提速 10 倍。

回歸測試另發現舊 TWSE OHLCV 修復路徑會先在記憶體移除可由官方日期替代的畸形列，卻被舊的「無變化」判定提前返回，導致回報列數與實體 Parquet 不一致。現只在確有畸形列修復時禁止該提前返回，確保原子寫入；未代替來源取得或補造價格。當前正式 OHLCV 檔唯讀掃描畸形日期為 **0 列**，故這是預防再次發生的修復，不是宣稱本機現有歷史已被改寫。全 `test/test_tw_public_*.py` **424 個 Python 測試**、Ruff 及 `git diff --check` 通過；這仍不是全庫測試。

正式整輪 A/B 亦界定加速的實際適用條件：15:18:29 在 schema-null 修正前、來源穩定的一輪為 **9.18 秒／峰值 6,466,884 KiB**，顯示最初要求 incoming 與多年歷史檔 schema 完全相同的快路徑**沒有命中**。加入舊欄 null 證明後，15:21:23 與 15:21:45 兩輪分別 **14.56／15.45 秒**、峰值約 **6.56／6.60 GB**，但兩輪 TWSE 正式來源 body SHA-256 與 Parquet SHA-256 都變動，正確地走完整合併。因此目前只證明**真實形狀且內容不變**的局部加速；尚未量到「官方來源穩定」的同負載完整排程速度，不能捏造端到端收益。

為避免日後再次用合成同欄資料誤判，新增唯讀 `scripts/benchmark_tw_public_historical_merge.py`：從已接受的當日 raw 重建 incoming，僅以現存檔的單一來源 URL 做同等比對，先證明 `fast_path_eligible`，再分別量快判定與原完整 merge，要求兩者都判定無變化，並檢查原檔 inode／mtime／大小均未動。15:25 在真實 9/24 TWSE 檔量得 **0.099 秒／277,808 KiB** 對 **2.583 秒／5,569,048 KiB**；TPEx **0.076 秒／238,876 KiB** 對 **1.249 秒／4,246,424 KiB**。各為單次、檔案可能在 page cache 且同機有其他服務負載；若官方 raw 已與 Parquet 不同，腳本會回 `fast_path_eligible=false`，不做虛假的 no-op A/B。可重測：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_public_historical_merge.py --dataset twse_institutional_trades --date 2026-09-24
run_fintech_python scripts/benchmark_tw_public_historical_merge.py --dataset tpex_institutional_trades --date 2026-09-24
```

15:00 僅用 `intraday-timer-only` 安裝已驗證的 `OnActiveSec=1min` 模板並重啟 timer，下載 service 及交易行程未更換；由 `active/elapsed` 無下一觸發改為 `active/waiting`。正式 15:01:42 與 15:03:43 **兩輪皆觸發並完成**，各約 60／59 秒、峰值 2.71／2.54 GB，三個分鐘來源的步驟收據均 `complete`；第一輪的 OKX／Bybit／Binance 分別完成 492／867／574 個標的，並排出下一輪 15:05:43。15:05 唯讀全服務稽核 `artifacts/benchmarks/service-coverage-20260925T1505-timer-recovered.json` 覆蓋 52 service／40 timer／2 path，`timer_schedule_findings=[]`。15:00:49～15:04:59 根碟可用 103,415,799,808→103,411,924,992 bytes，屬整個 filesystem 變動，不能只歸因這個下載器；沒有觀察到 GB 級突增。六個產品 HTTP 200，但業務健康仍為 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、永豐 `waiting`、OpenBB `stopped`、全資料 `critical`。15:15 OpenBB **自然重試已觸發**，前置檢查讀到 103,404,191,744 bytes，仍低於 100 GiB 安全線 107,374,182,400 bytes，缺口 **3,969,990,656 bytes**，依既有契約退出 2，未開始下載；不是 timer 失效。

15:30、15:30:30、15:31 的正式 `close_revision` timer 自然輪均 `Result=success`／exit 0，分別約 1.48／0.97／0.97 秒；收據都是 `status=skipped_verified_non_session`、`commands=[]`、`changed_dataset_count=0`，官方本機證據 `TWSE schedule as-of 2026-09-16: 中秋節`。14:02 的舊 failed 記錄因此由新的成功執行清除，但這只驗收休市分支，**不是**下個開市日收盤來源或開盤交易就緒。15:32 全服務唯讀收據 `artifacts/benchmarks/service-coverage-20260925T1531-holiday-acceptance.json` 覆蓋 52 service／40 timer／2 path、`timer_schedule_findings=[]`，六個本機 API 皆 200，但健康仍依序 `blocked`／`degraded`／`waiting`／`waiting`／`stopped`／`critical`；OpenBB 是唯一仍 failed 的 StockAgent service。

同輪 OKX `history-candles` 有 **504 次** rate-limiter grant、實際約 **59.50 秒**；[OKX 官方文件](https://www.okx.com/docs-v5/en/)目前仍列該 endpoint 每 IP **20 次／2 秒**，故僅配額的理論下限已約 **50.4 秒**，未計網路與處理。加大 16 個 workers 或取消限速不能在相同端點、安全契約下把這段壓成數秒；若要改用文件另列 **40 次／2 秒**的最新 `market/candles`，必須先證明其 1,440 根範圍、跨停機補抓、隨機快取新舊次序及逐標的實際時間戳不會遺漏，再做帶回退的灰度驗收。本輪未切換端點，也未以標的數縮減冒充提速。

## 2026-09-25 14:34 已註冊 artifact 退役：先做不可退役的廉價證明

為查明能否安全釋放根碟容量，休市時用 `timeout 180s nice -n 15 ionice -c3 ... scripts/retire_enrolled_artifacts.py` 執行**唯讀**計畫；它在 180 秒到期時仍在讀取 C 冷儲存物件（觀測約 8.7 GB read bytes、0 write bytes），退出 124，沒有完成任何 eligibility 判定，且沒有 `--apply` 或刪除。檢查四筆已註冊狀態才發現，三個完整 Bybit run 與一個 legacy crypto 檔案都是 9/25 剛建立／使用，明確受 7 天租約保護，最早到 **2026-10-02 02:54 UTC**；在到期前做 C／D 全量 hash 不可能讓它們符合退役資格。

排程退役入口現在先核對已註冊來源身分、`hot-enrolled`、schema 1 與 `last_used_ns`；只要有效租約未到期，就回 `deferred`／`seven-day-use-lease-active`、精確到期時點，並顯示 `verification=not_checked_active_lease`、`snapshot_id=null`，**不宣稱來源、D 備份或 Syncthing 已驗證**。異常未來時間、到期、狀態損壞或不符時仍走原完整 C／D／來源／引用／pin／peer 驗證才能 apply；沒有更改核准的刪除契約。與實際四筆來源相同的唯讀重跑 **0.373 秒**，全部是 deferred，沒有可用來修復 OpenBB 的核准回收空間。相關排程／完整 artifact／legacy 退役 **19 個測試**、Ruff／whitespace 通過。OpenBB 仍因根碟可用約 **103.4 GB** 低於既有 **100 GiB（107.37 GB）**安全線而停用；不得降低門檻或刪原始快取來假裝恢復。

14:40 再跑全服務唯讀覆蓋 `artifacts/benchmarks/service-coverage-20260925T1440-idle-lease.json`，仍有 **52 service／40 timer／2 path**；六個本機產品端點全為 HTTP 200，但健康依序 `blocked`／`degraded`／`waiting`／`waiting`／`stopped`／`critical`。`stockagent-registered-data-intraday.timer` 仍 `active/elapsed`、沒有下一次觸發；正式 OpenBB 仍 failed，14:02 的官方收盤掃描失敗旗標尚待下次定時服務成功清除。這些沒有因本輪閒置效能改善而被改成正常。當日 `stockagent-registered-data-features.service` 14:29:47 已完成約 29 分 47 秒工作、收據 `completed`；它與停擺的 intraday 計時器是不同工作。

## 2026-09-25 14:28 隔日沖歷史閒置行程的實際啟動成本

前一輪已使「無啟用模式」回 `idle_no_enabled_modes` 而非 failed，但正式 systemd 輪仍花 **8.034 秒**、**7.804 CPU 秒**、峰值 **436.1 MB**；單獨 `import scripts.maintain_tw_overnight_history` 約 **5.285 秒**，而只載入市場設定模組約 **0.089 秒**。瓶頸是進入閒置分支前先匯入完整重建／部署／訓練相依模組，不是讀取市場設定本身。

維護腳本現在先用 canonical `load_market_configs()` 判斷 `enabled && overnight_simulation_enabled`；只有真的啟用模式才延遲載入重建、來源驗證、歷史部署及日曆模組。設定讀取失敗仍報錯，啟用模式仍經原有 lineage／來源／部署檢查；停用時只更新 `latest_attempt.json`，不冒充已完成的歷史資料。新獨立程序匯入約 **0.108 秒**；正式 `stockagent-tw-overnight-history.service` 14:28 再跑成功，CPU **0.169 秒**、峰值 **13,787,136 bytes**（約 13.1 MiB）、同樣回 `idle_no_enabled_modes`。這是當下無模式的資源節省，**不是**已啟用策略的重建速度證明。相鄰發布／隔日沖 **122 個 Python 測試**及 Ruff／whitespace 通過。

## 2026-09-25 14:15～14:21 閒置心跳公開判活與休市工作修正

全服務稽核發現閒置引擎的 `service_sync.json` 心跳雖每約 10 秒更新，但當沖／隔日沖完整 dashboard 與首頁摘要仍以 60 秒一次的 `status.json.updated_at` 套用 30 秒 stale 門檻，故可能把正常運轉誤判為 `stale`。現保留「最後完整提交」時間與年齡，另用與 `state.json`／`status.json` 同一 `state_revision`、同一 `engine_run_id` 的心跳判活；任何缺失或修訂不一致均不得借用心跳變綠。首頁輕量摘要僅核對 status 與 compact receipt，不讀取大型 state；頁面將「帳本心跳」更正為「引擎心跳」，同時顯示最後完整提交年齡。新回歸涵蓋 45 秒閒置心跳、ID 錯配與首頁 fail-closed；當沖／隔日沖／公開網關相關 **413 個 Python 測試**、共用前端 **17 個 Node 測試**、Ruff／whitespace 通過。只重啟唯讀 gateway 與 8766 dashboard 子行程，兩個紙上交易引擎 InvocationID 未變。實測直接 8766 當沖 `degraded`、心跳年齡約 **9.5 秒**、完整提交年齡約 **60 秒**；公開 8770 當沖 `degraded`、隔日沖 `waiting`，IPv4 HTTPS 當沖狀態 HTTP 200（單次約 **260 ms**）。公開完整狀態仍有 55 秒快取／背景重建，回應中的年齡為建置時快照，不能將其當成每次 GET 即時計時；快速 `/api/revision` 與來源時間戳須分別解讀。

14:15 唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260925T1415-idle-heartbeat-fix.json` 覆蓋 **52 service／40 timer／2 path**；六個本機產品端點均 HTTP 200，但依序仍是 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、Shioaji `waiting`、OpenBB `stopped`、全資料 `critical`。另外發現隔日沖歷史維護 14:10 因目前**沒有啟用的隔日沖模式**而退出 1，以及 14:02 休市的收盤公開資料掃描對 9/25 執行 completed-session finalize 而退出 1；這兩個失敗不能歸咎網站或交易引擎中斷。

隔日沖維護現在先檢查 lineage：只有「確實沒有啟用模式」會寫獨立 `latest_attempt.json`、回 `idle_no_enabled_modes`／exit 0；設定錯誤、資料錯誤仍失敗，過去 `latest.json` 完成收據不被假 idle 覆寫。14:20 正式 `stockagent-tw-overnight-history.service` 重跑為 success，收據 `enabled_market_count=0`；因大型 Python 模組仍需載入，這輪約 **8.0 秒**，不宣稱啟動耗時已優化。收盤官方掃描僅在既有來源稽核確認 TWSE 官方休市時提前記 `skipped_verified_non_session`，不讀大檔、不下載、不執行當日收盤 finalizer；缺漏、未驗證或矛盾的日曆不能走跳過路徑。9/25 官方本機排程證據為 `official TWSE schedule as-of 2026-09-16: 中秋節`；14:21 手動同一正式腳本 `--phase close_initial` 約 **0.5 秒**完成，收據 `commands=[]`、`changed_dataset_count=0`。相關發布／隔日沖歷史 **96 個測試**與 Ruff／whitespace 通過。此手動驗證沒有清除 14:02 的 systemd failed 歷史狀態；尚需等下次 15:30 close phase timer 驗收實際排程。OpenBB 容量安全線、crypto intraday `active/elapsed`、盤後來源健康和下次開盤仍未解決。

## 2026-09-25 13:35～14:04 休市引擎寫入減量：提交與心跳分離

確認 9/25 官方休市期間，當沖引擎仍每約 10 秒執行 `update_readiness()`；它原本每次都增加 state revision，序列化／原子覆寫約 **9.5 MB** `state.json` 及 positions／status／service-sync。自然取樣 13:42:32 的 **302.068 秒**，當沖 cgroup block write **291,753,984 bytes**、CPU 平均 **0.0488 核**；隔日沖 **269,844,480 bytes**、CPU **0.0588 核**。此 I/O 是區塊寫入，不等於磁碟淨增容量。

新契約保留所有真正狀態／成交提交的 `state_revision`、原子檔與 append-only 帳本；只有離峰或已**確證**休市的閒置時段，完整 readiness 更新降至每 60 秒，且每約 10 秒只刷新 `service_sync.json` 的 `heartbeat_at`（原 `published_at` 仍是最後一次完整提交時間，不冒充新修訂）。Dashboard 與無人值守檢查以 heartbeat 判活，仍以原修訂碼對帳；服務收據 revision／engine run ID 不一致時心跳拒絕更新。08:10～09:10 的當沖開盤轉換、隔日沖 08:10～09:11 與 12:45～13:36 兩段撮合窗口保持原快速節奏；官方日曆缺漏／衝突不能走休市慢路徑。Shioaji 行程持有訂閱狀態，故只在證交所公告的休市日重啟紙上交易服務；下次開市仍須驗收登入、訂閱與 09:00 訊號。相關當沖／冷啟動／dashboard／guardian **230 個測試通過**；其中舊冷啟動測試把註冊資料集數硬寫成 156，正式註冊表已是 159，斷言改為比對實際 canonical 清單，未刪任何資料集。

當沖重啟後 invocation `916f2ea796e64e56a499b1b346fe9ee3`；原 9/24 **198 筆**持倉數與訊號／委託／成交／事件帳本檔案大小和時間戳未變，公網 `/tw-day-trade/api/revision` 仍 200 且能顯示獨立 heartbeat。公開 gateway 因自身持有舊版 dashboard 模組，另在 **119 個相關測試通過**後單獨重啟，驗收來源由舊 `engine_age≈29 秒、heartbeat 欄缺失` 轉為新的 heartbeat age 約 **8 秒**；IPv4 HTTPS 仍 200。13:52:33 完整自然輪（**300.478 秒**）當沖區塊寫入 **104,312,832 bytes**、CPU 平均 **0.0270 核**，對照 13:42 同一休市時段約減少 **64% block writes**、**45% 平均 CPU**；配置／背景負載不完全相同，不宣稱開盤熱路徑有相同增益。

隔日沖另發現較大的同型缺陷：目前 `enabled_markets=[]`，但舊 runner 用 `or not specs` 令每約 1 秒都重新載入設定並完整提交。13:53:45～13:54:06 實測 21 秒 **19 個修訂／18,989,056 block write bytes**。當沖與隔日沖兩個 runner 均改為「空模式也算已載入」，只靠時間到期或首次啟動重讀設定；新模式最遲於平常 30～60 秒被偵測，開盤／撮合窗口沿用 30 秒。隔日沖五個相鄰測試檔 **67 個測試通過**。休市重啟後 invocation `adbbd11ac4da4d48a01ad7a9d3183e67`，訊號／委託／成交／事件 SHA-256 全部未變；13:55:08～13:55:41 的約 33 秒 `state_revision=148105` 不動，heartbeat 持續更新。14:02:41 的正式自然輪（**306.806 秒**）隔日沖區塊寫入 **5,103,616 bytes**、CPU 平均 **0.0022 核**，對照 13:42 同樣休市空模式約 **269,844,480 bytes／0.0588 核**，這是約 **98% 寫入減量**與 **96% 平均 CPU 減量**；仍不是下次有啟用策略時的實測。當沖同輪區塊寫入 **83,546,112 bytes**、CPU **0.0159 核**，相對舊休市樣本寫入約減 **71%**。隔日沖沒有啟用模式且 Discord overnight ack revision 仍為 0，公開同步狀態為 `catching_up`；沒有把它說成交易就緒。

再檢查 `systemctl StatusText` 發現休市當沖可能說「等待今日訊號」、空模式隔日沖可能列歷史模式的 `waiting_13_00_switch`。已區分官方確認休市、來源未驗證及無啟用模式；兩個行程在完整五分鐘取樣後另受控重啟載入文字修正。14:04 正式 StatusText 分別為 `verified closed session; carried paper positions=198` 與 `running idle; no enabled modes`，兩個公網 status GET 仍 200；當沖訊號檔的大小／mtime、委託／成交／事件 SHA-256 及隔日沖四種 append-only 帳本 SHA-256 均與修正前完全相同，state／service-sync revision 對齊。合併當沖／隔日沖／冷啟動／dashboard／guardian 回歸 **313 個測試通過**，Ruff 與 whitespace 通過。這仍不等於下一開市日行情訂閱、開盤訊號或出場實測成功。

13:07 合併回歸：資料監控、FinLab、公開網站、服務趨勢、開機契約與備份／保留 **303 個 Python 測試通過**；全工作樹 `git diff --check` 通過。這是相關範圍回歸，不等於全庫測試，也不表示尚未重啟的 gateway／備份行程已載入新程式。根碟可用 **103,670,067,200 bytes**，仍低於 OpenBB 的 100 GiB 安全線；crypto intraday timer 仍 `active/elapsed`／下一觸發 `infinity`。

## 2026-09-25 13:25～13:30 備份 head 唯讀快路徑加固與唯讀 gateway 載入

備份 metadata 階段對已存在的 D: head 原用 `safe_path()` 後 `Path.read_bytes()` 比對內容；前一步雖拒絕當時可見的 symlink，兩步間仍有名稱被替換的時間窗，而且在 DrvFs 重做路徑解析。此唯讀探測已改用既有 `read_existing_metadata_nofollow()`，將目錄與檔案以不跟隨連結的 fd 開啟，讀取前後核對檔案身分及每層目錄名稱；真正缺檔才走原本新 head 的完整物件重驗，symlink／異常則維持 fail-closed。未更動冷物件校驗、D: head 寫入或刪除政策。新增測試防止退回跟隨路徑讀取，且 D: head 被 symlink 取代時 `degraded`、`current_heads_complete=false`；備份／保留 **40 個測試通過**、Ruff 通過。

依證交所當年公告，9/25 為休市，故只重啟**唯讀公開 gateway** 與**獨立 D: 備份**，不動交易／行情／Discord。Gateway 重啟前後本機 `/healthz`、`/api/overview`、`/shioaji/api/status`、`/data-monitor/api/status` 與 IPv4 公網 `/healthz` 全部 HTTP 200；gateway invocation 變為 `c2500d48386a40ee9bc2336a2a19b7a4`，交易、行情、Discord invocation 均保持原值。重啟前相關公開 gateway **119 個測試通過**。新的已載入 gateway 經可重跑工具 8 次／路由、單客戶端取樣，`/api/overview` 中位 **1.181 ms**、`/shioaji/api/status` **2.232 ms**、`/data-monitor/api/status` **11.269 ms**、`/tw-day-trade/api/summary` **1.536 ms**，各路由 0 錯誤，收據 `artifacts/benchmarks/public-gateway-20260925T1330-restart.json`。這是本機熱態取樣，不能推論所有公網路徑或冷啟動同速；Shioaji payload `health=waiting`，沒有改寫資料健康。

備份重啟後 invocation 為 `2be12792bdb24303bbc557903dc18989`。首次正式 current-head pass 為 `up_to_date`、**12,019 物件／125 heads／0 pending／0 error**，metadata **4,701.86 ms**，拆解 manifests **2,049.00 ms**、heads **2,642.69 ms**，destination trust **9,751.11 ms**；D: 可見的 current-head 完整收據未退化。這一輪與重啟前 metadata **6,277.14 ms** 的兩次自然取樣負載不同，不能單憑它宣稱加固提高效能；主要成本仍是每輪物件信任驗證。

載入後完整唯讀稽核 `artifacts/benchmarks/service-coverage-20260925T1330-post-restart.json` 覆蓋 **51 service／40 timer／2 path**。六個產品 probe 均回 HTTP 200，但健康依序仍為 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、Shioaji `waiting`、OpenBB `stopped`、全資料 `critical`；OpenBB 是唯一 failed StockAgent service，crypto intraday timer 仍有 `recurring_timer_without_next_trigger`。部署驗收只證明 gateway 與 D: 備份改動已載入並維持端點／備份可用，不等於所有資料與排程已修復。

## 2026-09-25 13:20 盤中交易服務寫入：監測欄位修正與成本歸因

短取樣曾看到當沖／隔日沖的 cgroup block write，但五分鐘趨勢一直為 `null`。原因是 `service_snapshot()` 已提供直接從 cgroup `io.stat` 取得的 `CgroupIOReadBytes`／`CgroupIOWriteBytes`，趨勢卻讀主機未提供的 systemd `IOReadBytes`／`IOWriteBytes`。已改為與短取樣同一來源；相同 invocation 才相減，缺失或重啟仍為 `null`。相關趨勢／排程／啟動 **42 個測試通過**，Ruff 通過。13:16:53 的正式自然輪（間隔 327.959 秒）首次可見當沖寫 **322,691,072 bytes**、隔日沖寫 **296,828,928 bytes**、Discord 寫 **6,717,440 bytes**；當輪整個 filesystem 可用量只少 **581,632 bytes**。這是區塊寫入，不是各服務新增容量；不能用這 600 多 MB 解釋 OpenBB 的磁碟容量缺口。

當沖 `state.json` 當時約 **9.5 MB**、100m 模式單獨約 **6.55 MB**；其 `carry_cost_ledger` 約 **3.33 MB**、`margin_exit_capacity_used` 約 **1.59 MB**。13:14:39 到 13:18:27 的 `state_revision` 增 **22**，`dashboard_content_revision` 保持 **4682**；至少一次相鄰修訂 **25817→25818** 在排除心跳／修訂欄位後完整狀態 SHA-256 相同。這證實整檔重寫有重複成本，但**不授權直接省略提交**：現行測試要求每次 heartbeat 新修訂碼，state／positions／status／service_sync 修訂對齊，margin cost 收據與累計必須同一原子提交。盤中沒有改交易引擎或帳本。後續要在離峰設計可驗證的狀態分片／心跳分離及崩潰恢復對帳，不能把「面板內容沒變」當成交易狀態沒變。

## 2026-09-25 13:04 FinLab 收據檔案驗證：減少重複路徑解析

全資料監控每輪從 FinLab 目錄與收據列出約 1,113 項，其中 1,104 份已下載收據各自解析 `datasets/<檔案>` 路徑，原 `_finlab_candidate_sources()` 同程序三輪約 **142.555／137.153／159.276 ms**。一般兩層相對路徑改為在已解析的 enrolled root 下，以 `O_DIRECTORY|O_NOFOLLOW` 開啟 `datasets` 並對單一葉檔用不跟隨連結的 `stat` 驗 regular file；回傳前另比對開啟中的目錄與目前路徑的 inode，若目錄換版就回退完整檢查。每筆仍重新觀測檔案，沒有只看目錄 mtime。符號連結、巢狀舊路徑或不支援這些旗標的平台回退原本的 `resolve()` 加根目錄包含檢查；來源外連結仍拒絕。單獨對 1,104 檔的初版原型約 **85.637 → 17.668 ms**；加上目錄換版防護後完整函式三輪 **115.396／114.379／141.253 ms**，不同負載下增益浮動，不能宣稱固定倍率。

FinLab／全資料／公開網站回歸 **188 個測試通過**，另補測檔案與目錄的內部／外部符號連結、缺檔、穿越及絕對路徑；Ruff／whitespace 通過。13:03～13:04 正式自然輪的 `finlab_sources_and_acquisition` 為 **115.683／128.386／121.754 ms**，首輪因程式 mtime 觸發欄位清冊重建使總耗時 **5.083 秒**，後兩輪約 **1.927／1.807 秒**。公開資料健康仍 `critical`、註冊 1,636 項、需注意 115 項；只是降低觀測運算，沒有冒充下載完成。

## 2026-09-25 12:58 盤中資料 timer：隔離安裝與雙時鐘競態修正（尚未部署）

正式 `stockagent-registered-data-intraday.timer` 仍 `active/elapsed`、下一次 monotonic 觸發 `infinity`。原模板同時有 `OnBootSec=2min` 與新的 `OnActiveSec=1min`；前者對已開機很久的 WSL 重新啟用會立即觸發，與「盤中先不要突然啟動下載」相衝突。兩個只執行 `/usr/bin/true` 的 transient timer 實測：含 `OnBootSec` 者剛啟用就產生 `LastTriggerUSec`；僅 `OnActiveSec=1min`／`OnUnitInactiveSec=1min` 者 `LastTriggerUSec` 保持空且 `SubState=waiting`。兩個 probe 測後都已停止／卸載，不涉及資料下載。

模板移除多餘 `OnBootSec`，保留 timer 啟用後一分鐘與每輪結束後一分鐘的時鐘。安裝腳本新增 `intraday-timer-only`，只渲染／驗證／安裝這個 timer，不覆寫下載服務及其他 dirty 模板，不修改無關腳本權限；因已 active 但 elapsed 的 timer 用 `enable --now` 不會重排，該模式明確 `restart` timer 並拒絕仍為 `elapsed` 的結果。`bash -n`、排程／啟動／趨勢 **41 個測試通過**、whitespace 通過。**未在磁碟低於安全線及盤中執行安裝**；正式服務已有較早的完成事件，`OnUnitInactiveSec` 仍可能讓重新啟用後立即觸發，隔離測試沒有排除這點。必須在可接受下載啟動的安全窗口載入，再實測下一觸發、實際工作收據與容量。

## 2026-09-25 12:51 排程失效納入長期服務趨勢

手動全服務稽核雖能找出「enabled／active、實際 elapsed 且沒有下一觸發」的循環 timer，但之前每五分鐘的自然服務趨勢只記 service 資源，故障可能長時間沒有連續證據。現重用相同的 `timer_schedule_findings()` 判定，每五分鐘以一次唯讀 systemd 查詢記 `recurring_timers` 的 `attention`／`observed_clear`／`unavailable`；systemd 查詢失敗或一個 timer 都沒觀測到，不能變成空 findings 的假正常。未改任何 timer、下載或交易行為。

12:51:13 正式自然 `all_service_runtime_sample` 已記 **40 timer**、`state=attention`，唯一 findings 為 `stockagent-registered-data-intraday.timer` 的 `recurring_timer_without_next_trigger`；相鄰趨勢／排程／開機契約 **40 個測試通過**，Ruff／whitespace 通過。這是偵測能力，不是排程恢復：timer 仍 `active/elapsed` 且下一觸發 `infinity`，須在容量與盤中資源安全後載入已修正的 timer 模板並實測執行收據。

## 2026-09-25 12:46 備份 metadata 子階段測速（尚未部署）

備份閒置對帳已量到 metadata 約 10.1 秒，但舊收據無法區分 manifest 與 head。`PackedBackup.metadata()` 現將兩段單獨計時，併入既有 `stage_timings_ms` 的 `metadata_manifests`、`metadata_heads`；校驗、D 槽寫入與 head 提交順序均未更動。備份／保留回歸 **38 個測試通過**，Ruff／whitespace 通過。**正式備份服務仍沿用舊行程（InvocationID `b1815b0439074309a0c43430af794182`）**，因此目前沒有實測新欄位，更不能先宣稱 metadata 耗時已下降；離峰載入後觀測實際子階段，才能決定是否有安全的後續優化。

12:47 唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260925T1247-current.json` 記 **51 service／40 timer／2 path**；crypto intraday 仍被正確報為 `recurring_timer_without_next_trigger`，OpenBB 為唯一 `failed` service。六個產品 probe 分別仍有 `blocked`／`degraded`／`waiting`／`waiting`／`stopped`／`critical`，不能因 HTTP 可達或本輪快取加速便宣稱系統全綠。正式公開 Shioaji probe 約 359.673 ms，符合尚未重啟舊 gateway 的部署邊界。

## 2026-09-25 12:35 全資料清冊：逐檔驗證後重用小型聚合索引

原 30 秒自然輪的清冊約 **1.38～1.48 秒**，其中完整 JSON 快取 **43,319,683 bytes／57,296 檔**的解析約 **0.57～0.60 秒**、發現約 **0.49～0.60 秒**、檔案簽章檢查約 **0.29～0.34 秒**。251 個資料集的公開聚合僅約 **112 KB**。直接跳過掃描或只看目錄 mtime 會漏原地覆寫，不能以此換速度。

現加 `record_inventory_fast_index.json`（約 110 KB）作**可丟棄**的衍生索引：記錄完整快取的 dev／inode／size／mtime／ctime 與每個資料集的精確檔案歸屬、逐檔五元身分指紋、聚合及修訂碼。每輪仍重新發現並 `stat` 所有 57,296 個選取路徑；只有指紋、索引內容 SHA-256 與完整快取簽章都相符時才略過 43 MB 的解析與重算。缺檔、原地更換、同大小同 mtime 的 inode 替換、跨分組搬移、索引損壞／變更都走原完整流程；欄位明細需要重建時仍可讀完整快取。完整快取另記只含資料集與成員路徑的指紋：即使兩檔交換分組後所有聚合數字剛好相同，也會更新 feature revision，避免沿用錯誤欄位清冊。索引寫入失敗只失去加速，不可讓資料清冊失敗或變成假 `verified`。

12:32 首次建索引使該輪額外重算並寫 43 MB 快取，清冊約 **2.54 秒**、欄位快照也因來源快取換版重建，總約 **5.83 秒**；不能把此 warm-up 隱藏。12:33 兩輪正式自然收據：清冊 **680.568／693.222 ms**、整輪 **1,744.328／1,781.394 ms**，`feature_reused=true`、`refreshed_files=0`，對照變更前同類穩定輪約 **2.45～2.49 秒**。12:34 一輪雖清冊 **686.686 ms**，欄位投影因來源檔／實作檔 mtime 變更而重建，總耗時 **4,939.11 ms**，不能宣稱每輪必快。唯讀當下完整快取與快索引的 **251 個 datasets 完全相同、feature revision 相同**；公開 API 仍 `critical`，57,296 選取檔中 19 個 invalid 仍明列。相同聚合不同分組修訂碼、索引損壞回退、相鄰網站及開機契約共 **230 個測試通過**，Ruff／whitespace 通過。最新自然輪 12:44:09 的 `inventory_fast_index_hit=true`：清冊 **689.861 ms**，整輪 **1,755.233 ms**，`feature_reused=true`、`refreshed_files=0`；12:40 的索引版次遷移亦曾造成慢輪，不拿來算穩態。

容量方面，OpenBB `_state/raw_cache` 約 **8.3 GB**，其中 SEC companyfacts 約 3.0 GB、BLS labstat 約 3.3 GB、SEC insider 約 2.1 GB；它是可續傳的請求原始資料，不能把「cache」字樣當成可刪證據。OpenBB SQLite 約 17 GB、journald 約 4.1 GB。核准的已註冊 artifact 退役 dry-run 在交易時段掃描約 40 秒仍產生較高磁碟 I/O；已只停止本次由代理啟動的**唯讀**計畫，退出 143，未執行 `--apply`、未刪來源或備份，也**沒有**可用的退役 eligibility 結論。待離峰再跑並核對 D／Syncthing／lease／進程引用與可回收 bytes；在此之前 OpenBB 仍不能因安全線不足而強行宣稱恢復。

## 2026-09-25 12:18 永豐公開狀態頁：重用已建置的唯讀快照

12:13 全服務唯讀稽核 `artifacts/benchmarks/service-coverage-20260925T1213-post-anchor.json` 列 **51 installed service／40 timer／2 path**，crypto intraday 仍是唯一 `recurring_timer_without_next_trigger`；六個 localhost 產品端點皆 HTTP 200，但永豐狀態回應約 **351.6 ms**、健康 `waiting`，當沖 `degraded`、TAIFEX `blocked`、OpenBB `stopped`、全資料 `critical`。HTTP 200 不是業務健康。

單獨對永豐公開 builder 做本機剖析：首輪約 **461 ms**，其中「本機 service／journal／manifest」約 **187 ms**、「pipeline receipts」約 **248 ms**；無券商登入、無行情查詢。相同程序稍後三輪直接建置約 **443.63／72.70／54.92 ms**。原本每 30 秒的全資料監控工作已為自己的分組重複建置這份 payload，造成公開 gateway 自己再讀同一批收據。

現在 30 秒監測工作只建置一次永豐公開狀態，原值同時交給全資料投影並以原子檔發布 `artifacts/live/data_monitor/shioaji_status.json`；`read_only=true`、`simulation_only=true`、`production_order_possible=false` 才能寫。公開 gateway 每次檢查產物的產生時間（不得逾 60 秒）、安全 regular-file／UID／權限、固定 1 MiB 上限、讀前後 inode／mtime／ctime 簽章及 JSON 有限值，任何缺失都回退原本的本機 builder，**不**以舊檔冒充新鮮資料。首頁與永豐獨立頁共用同一路徑；未加券商連線，也未改交易／報價服務。

12:17 自然監測輪已實際產出 root-owned 0600、47,767 bytes 的有效快照，`health=waiting`、10 項 pipeline；同機三次驗證讀取約 **0.49／0.52／0.61 ms**，和直接建置的健康與 pipeline 數相同。隔離的新程式 HTTP 入口三次約 **3.65／2.59／2.30 ms**，HTTP 200 且 `waiting`。公開頁、永豐、全資料和長期服務趨勢 **189 個測試通過**，Ruff／whitespace 通過。**正式公網 gateway 仍是舊行程，盤中未重啟；此數字不是公網端到端測速。** 離峰須只重啟唯讀 gateway、核對本機與 IPv4 HTTPS、回退情境及交易／行情行程 ID 不變；IPv6 與上游資料健康仍是獨立問題。

## 2026-09-25 12:10 根碟容量趨勢與歸因邊界

根碟 `df` 此刻可用 **103,271,596,032 bytes**，低於 OpenBB 既有 100 GiB（107,374,182,400 bytes）安全線；OpenBB supervisor 仍 `failed/exit 2`，crypto intraday timer 仍 `active/elapsed` 且下一次觸發為 `infinity`。`stockagent-legacy-us-stage.service` 是另一個目前運行的 transient 任務，11:47 啟動，12:08 cgroup 累積 `rbytes≈84.74 GB`、`wbytes=0`；這個樣本**不能**證明它造成先前數 GB 容量損失。journald 總占用約 4.1 GB，但沒有為了跨越安全線而清除可能唯一的稽核紀錄。

現有每五分鐘 `all_service_runtime_sample` 只有同 invocation 的 cgroup 讀寫量；它既不能代表容量增減，也無法判定某個來源樹是否變大。現補一次 `statvfs` 量測：記錄 StockAgent 所在 filesystem 的裝置 ID、總 bytes、可用 bytes、跨樣本可用 bytes 差。跨開機、換 filesystem 或觀測失敗時 delta 留空；事件明確註明這是**整個檔案系統的變動，不可歸因到單一服務，也不等於 cgroup I/O**。無須遍歷 2 TB 來源樹或另起常駐程序；唯讀本機 helper 回報同一掛載可用 **103,270,936,576 bytes**。新功能及相鄰排程／稽核 **39 個測試通過**、Ruff／whitespace 通過。12:20:35 正式自然輪的 `all_service_runtime_sample` 已記錄 51 個 service、`device_id=2096`、可用 **103,268,347,904 bytes**、比上個同機樣本少 **5,341,184 bytes**；這只是約五分鐘整個 volume 的變動，仍不能指定造成變動的服務。

## 2026-09-25 12:04 計時器重啟保證擴展

從 systemd journal 可定位 crypto intraday 最後一輪 9/24 08:19:38 成功完成、08:20 timer 停止、08:53 短暫啟停、09:10 再啟動；09:10 後沒有新工作，現為 `active/elapsed`、無下一觸發。這支持「重啟後缺少獨立錨點」的排程修正；它不表示來源下載、儲存容量或全部資料健康已恢復。

用只執行 `/usr/bin/true`、`ExecCondition=/usr/bin/false` 的獨立 transient timer 實測 `OnActiveSec=1s` 加 `OnUnitInactiveSec=2s`：12:04:13、15、17、20 均因條件跳過，timer 仍回到 `waiting` 並持續觸發；測後已停止。這驗證受保護窗口的 `ExecCondition` 跳過不會讓新錨點停止循環，未呼叫任何實際下載。

通用契約測試掃描所有 `deploy/systemd/*.timer.in`：凡有 `OnUnitActiveSec` 或 `OnUnitInactiveSec`，必須另有相對 timer 啟用的 `OnActiveSec` 或實際日曆 `OnCalendar`。除前述 crypto timer 外，為五個同類模板補獨立啟動錨點：公開資料狀態 5 秒、WSL 回補記憶體檢查 5 分鐘、儲存壓力 15 分鐘、遠端冷 artifact ingress 2 分鐘、未安裝的舊冷 artifact maintenance 5 分鐘；原完成後循環及服務內容不變。相關 boot-recovery／全服務稽核 **30 個測試通過**。這些**只有 repo 模板變更**，未重啟盤中服務，也未把 crypto timer 裝入 systemd；實際服務排程仍須在離峰、磁碟容量足夠後個別安裝並核對下一觸發及收據。

## 2026-09-25 11:58～12:00 systemd 計時器實測與誤報修正

用唯一 `codex-timer-bootstrap-probe-20260925.timer` 作隔離測試，目標只執行 `/usr/bin/true`，`OnActiveSec=2s` 加 `OnUnitInactiveSec=2s`、`AccuracySec=1s`；11:58:24～11:58:40 的 journal 有連續啟動／正常退出，測後已明確 `systemctl stop`，timer 為 `inactive/dead`。此實測證明 timer 啟用錨點可接續完成後循環，但不等於 crypto 下載工作已成功。

實測還發現本機 systemd 259 的 `NextElapseUSecMonotonic=infinity` 可與正常 `SubState=running`、反覆觸發同時存在，因此不能單憑該欄把 timer 判停。全服務稽核已把 `recurring_timer_without_next_trigger` 限定於 **`active/elapsed`、宣告 OnUnitActive／OnUnitInactive 循環、目標不在執行且無可辨下一次觸發**。新隔離回歸保證 `running` 不會誤報；`artifacts/benchmarks/service-coverage-20260925T1159-timer-substate.json` 仍只指認實際停擺的 crypto intraday timer。相鄰量測／稽核 **42 個測試通過**。

## 2026-09-25 11:55 動態 service 與可回收空間界線

全服務稽核的 `systemctl show stockagent-*.service --all` 會包括暫時建立的 systemd 單元，不能把 52 直接解釋為 52 個持久安裝的 service。新增讀取 `Transient`／`UnitFileState` 並在報表逐項註明來源；正式唯讀收據 `artifacts/benchmarks/service-coverage-20260925T1155-origin.json` 分成 **51 installed + 1 transient**，後者為當時仍在執行的 `stockagent-legacy-us-stage.service`。對每個 service 的 wall／CPU／記憶體／區塊 I/O 量測仍照常保留，不因 transient 就漏掉其負載。相鄰稽核／量測 **42 個測試通過**。

再次跑核准 `stockagent-data gc --dry-run`：只查到 3 個受控熱快取，2 個 pinned、1 個 lease-active，`would_evict=0`。C 冷 store 最近核准保留計畫僅約 **274,432 allocated bytes** 可回收；這與編譯快取 0 天 dry-run 的約 **182 MB** 相加仍遠不足根碟約 **4.10 GB** 安全線缺口。沒有解除 pin、縮短 lease、碰來源／帳本，亦沒有啟動滯後的 crypto 下載。公開 OpenBB API 實際列 `storage.free_bytes≈103.28 GB`、`minimum_free_bytes=107.37 GB`、`above_safety_floor=false` 且 `health=stopped`，UI 沒有把停機改寫為正常。

## 2026-09-25 11:47～11:49 磁碟再度低於安全線與跨服務實際 I/O 取樣

根碟可用空間降至約 **103,278,145,536 bytes**，低於 OpenBB 原定的 **107,374,182,400 bytes** 安全線約 4.10 GB；OpenBB 仍為 `failed/exit 2`，15:15 自動重試若容量不變會再次被 preflight 拒絕。核准的編譯快取清理器即使用 **0 天**門檻做唯讀 dry-run，也僅有 **771 檔／182,358,016 allocated bytes** 可回收，遠不足缺口，因此沒有為了顯示服務恢復而再刪資料或壓低安全線。`lsof +L1` 沒有發現大檔刪除後仍開啟；此段仍**未證明** 11:20 後約 5 GB 的所有新增占用來源。

全服務取樣增加 cgroup `io.stat` 的 block read/write bytes 增量，只有同一 service invocation 前後持續執行才計算，缺失或重啟保留 `null`；它是實體區塊 I/O，**不是**來源邏輯新增容量。11:48 收據 `artifacts/benchmarks/service-coverage-20260925T1148-io-pressure.json` 當時列 **52 service／40 timer／2 path**：比 11:22 多了其它工作新增、正在跑的 transient `stockagent-legacy-us-stage.service`，不可假設固定永遠 51 個。5 秒同程序寫入最多為隔日沖約 4.997 MB、TW public source events 約 1.294 MB，其餘採樣中的 service 很低；不能把這個短樣本倒推為先前 5 GB 的來源。編譯快取、來源樹、OpenBB preflight 及外部 Windows/D: 活動屬不同邊界。Crypto intraday timer 的模板修復若在此容量下直接啟用，補寫資料可能加深磁碟壓力；須先有容量或經核准的安全退役方案，並在離峰驗證。

## 2026-09-25 11:42～11:44 盤中資料 timer 無下一次觸發：偵測與待離峰載入

重新核對本機 **51 service／40 timer／2 path** 時，`stockagent-registered-data-intraday.timer` 雖為 `enabled/active`，實際 `SubState=elapsed`、`NextElapseUSecRealtime` 空、`NextElapseUSecMonotonic=infinity`，目標 service 為 `inactive` 且本次開機無執行紀錄。timer 9/24 09:10 啟動後仍記住 08:18 的舊觸發；`registered_intraday_runs.tsv` 最後一筆 9/24 08:19 完成。**不能**因 enabled/active 就宣稱盤中一分鐘資料仍有持續刷新。`OnBootSec` 與重啟／開機時鐘交互的精確內部原因尚未單獨重現；可以確認現有 `OnUnitInactiveSec=1min` 在沒有新的 service 完成事件時無法自行重新排程。

模板加入相對**timer 本身啟用**的 `OnActiveSec=1min` 作獨立啟動錨點，保留原完成後 1 分鐘循環及既有資源窗口 `ExecCondition`；不改來源抓取、配額或發布契約。全服務稽核現在同時讀 monotonic/realtime 下一觸發與所有（可重複）`TimersMonotonic` 規則；只對**有完成／啟動相對循環、無下一觸發且目標不在執行**的 timer 發現 `recurring_timer_without_next_trigger`。新收據 `artifacts/benchmarks/service-coverage-20260925T1141-timer-gap.json` 正好報這一條；三個 Shioaji 歷史回補 timer 因其目標仍 `active/running` 沒有被誤判。全資料監控的自動化判定亦改為將 `timer_unarmed` 顯示為無法執行，不再因 `timer_active=true` 或其它低頻工作排程就把停擺的分鐘尾端說成正常；11:44 自然輪本機 API 中 OKX、Bybit、Binance 與 crypto reference 群組均顯示 `unable/timer_unarmed`，頁面已載入 `app.js?v=40`。相鄰 **91 個測試通過**。**模板尚未裝入 systemd，也尚未重啟 timer**；盤中不啟動重型 crypto 下載。離峰須只更新這份 timer、`daemon-reload`、重啟 timer，再驗其下一次觸發、正式 service 收據及資料更新，不能只看 `active`。

## 2026-09-25 11:34 永豐儲存監測：原生串流掃描與錯誤隔離

正式 07:09 儲存監測快照掃了 **4,587,108 檔／107,078,171,233 bytes**，wall **183.376 秒**；其中 `futures_history` **1,903,379 檔／133.32 秒**。此服務只讀本機檔案，不連券商；成本主要是每檔 Python 目錄項與 stat 處理，不是 API 限速。固定同一個 `data_tw_microstructure/captures` 的 **165,176 檔**做唯讀 A/B：原 `_scan_dataset` **4.347 秒**，GNU `find -type f -printf` 串流到相同 Python 聚合為 **0.627 秒**；檔案數、總 bytes、30 個台北曆日分桶及最新 mtime 完全相等。這只是單一群組、單輪局部數字，不可推論整個 459 萬檔服務固定快 7 倍。

現正式掃描路徑改用受信任 `/usr/bin:/bin` 的 GNU `find` 只輸出數值 metadata、保留不跟隨 symlink、原有的 mtime 成長公式與所有資料群組；缺少 `find` 才回退 Python。對原生掃描的非零退出、錯誤輸出格式則**拒絕發布新快照**，不把部分結果標成 `ready`；來源在遍歷時消失由 `-ignore_readdir_race` 按原掃描容忍。回歸涵蓋檔案／目錄 symlink、單檔來源、台北午夜界線、原生與 Python 輸出逐欄相等，以及掃描失敗保留上一份原子快照。此改動不重啟券商、行情或當沖引擎，下一次 14:09 的自然正式 timer 才能驗收全群組 wall／CPU／記憶體、群組檔數與容量；本段不得提前聲稱全量提速或整個 Shioaji 資料健康恢復。

## 2026-09-25 11:20 核准編譯快取緊急回收

根碟停機時可用 **106,773,131,264 bytes**，低於 OpenBB 既有 **107,374,182,400 bytes** 安全線。預設 14 天編譯快取 dry-run 為 0；將**同一核准清理器**的最小檔齡設為 1 天後，唯讀計畫只在 `/root/.cache/torchinductor` 與 `/root/.cache/triton`（受限於 `/root/.cache`）找到 **35,705 檔／1,412,739,072 allocated bytes**；沒有匹配的訓練／編譯程序、開啟檔案或錯誤。已用 `scripts/maintain_storage_pressure.py --min-age-days 1 --apply` 執行，正式收據 `/var/lib/stockagent-storage-pressure/receipts/storage-pressure-apply-20260925T031959.026723Z.json` 記錄**恰好**刪除上述 35,705 檔、1,412,739,072 bytes，0 個變動／開啟中檔被誤刪、0 錯誤；來源、帳本、模型、D 備份、冷 store、materialized cache 均未碰觸。這些編譯快取可由下一次編譯重新產生，但重編譯有時間成本，不能把清理說成零代價。

刪除後同一根碟可用 **108,207,480,832 bytes**，高於安全線約 **833 MB**；`OPENBB_PREFLIGHT_ONLY=1` 的原入口已退出 0、未啟動下載器。OpenBB 服務仍保持 10:53 的 `failed/exit 2`，沒有在市場時段額外啟動；安全重試 timer 會在 **15:15** 自然嘗試。此容量緩衝很薄，可能隨其他寫入再次耗盡；要持續運作仍需擴充根碟或經完整 D 備份／程序引用／租約證據安全退役資料，不能降低 100 GiB 線。

清理後的唯讀全服務重測收據 `artifacts/benchmarks/service-coverage-20260925T1122-post-cache-recovery.json` 仍涵蓋 **51 service／40 timer／2 path**。六個本機產品 GET 均 HTTP 200，但 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、永豐 `waiting`、OpenBB `stopped`、全資料 `critical`；OpenBB service 仍是唯一 `failed` 的 StockAgent unit，另有非 StockAgent 的舊 FinLab transient 探測 scope failed。這些狀態與暫時通過的磁碟 preflight 不矛盾，不能在 15:15 前宣稱服務已恢復。

## 2026-09-25 11:15 實存清冊身分遷移驗收與獨立來源缺口

前輪的舊 Parquet 快取身分重驗已由自然 30 秒 timer 完成：持久清冊 **57,296 筆**均有 `file_identity`，公開 `/data-monitor/api/summary` 同樣顯示 `identity_unbound_files=0`、當輪 `identity_rechecked_files=0`；近幾輪 `feature_reused=true`、欄位數 **87,615**。11:12～11:14 的自然輪總 CPU／wall 隨宿主負載約 **2.87～4.53 秒**，包含每輪掃檔與永豐本機狀態工作；不能把身分遷移完成說成全服務延遲已最佳化。來源健康仍是 `critical`。

清冊另外如實列 **19 個無法讀 footer 的 Parquet**：17 個 `data_yahoo/crypto/*_features.parquet`、2 個舊 `data_parquet/*_features.parquet`。使用 PyArrow 逐檔獨立嘗試，19／19 都是 footer magic bytes 缺失，而非只有監控 UI 的分類錯誤。這些檔案不能當已保存可讀的資料；本輪沒有從其他來源猜補內容、覆寫來源、改成 `complete` 或繞過發布稽核。下一步須對這 19 檔逐一找原始來源或已驗證的 D 備份版本，再以正式來源／冷發版程序修復；與 CBC 清單完整度及 OpenBB 磁碟停機分屬不同健康域。

## 2026-09-25 11:08～11:15 央行清單頁面覆蓋驗證

前一輪的 20→60 筆改動揭露了既有完整度漏洞：原先只比較每頁宣稱的**總頁數**，沒有核對「此頁頁碼」、「公告總筆數」、「實際每頁原始列數」及選定頁寬。若 CDN 將同一頁 200 回給不同網址，可能在去重後少筆，卻仍標 `complete`。現在每個線上及快取頁均解析官方頁面自己的頁碼、總列數、實際含日期的原始列數與選定 20／60 筆；全輪要求頁碼精確對應 URL、全頁總列數相同、頁數等於總列數除頁寬向上取整、每頁列數符合首尾頁容量。任何不符即拒絕發布，不把來源錯頁／清單移動冒充完整歷史。

用正式現存 **393 份** 20 筆來源原文逐頁重驗：官方總計 **7,842 列**，逐頁實數相加也是 **7,842**，頁碼／頁寬／首尾列數 **0 異常**；其中有效 `cp-302` 文章連結比原始列少 38，主因為歷史空 href，這 38 個標題均未提到「外匯存底」，故不能錯把「可用文章連結數」當成官方總列數。60 筆版首／末頁現場抽樣分別有 **60／42** 個原始列，總數 7,842、131 頁；上一輪全部 131 頁與舊版的外匯存底文章 URL 集合仍一致。新增重複頁及缺列 fail-closed 回歸；CBC／歸檔／開機相鄰測試 **41 passed**。這是正確性與故障隔離修正，下一次正式 16:30 執行仍需查看完整稽核、耗時與記憶體，不以離線樣本替代。

## 2026-09-25 11:03 OpenBB 容量停機與安全自動重試

新唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260925T1103-cbc-page-size.json` 已列 **51 個 service／40 個 timer／2 個 path**；比 08:59 增加 `stockagent-enrolled-artifact-retirement.service/.timer`，因此原表的 50／39 是歷史快照。六個本機公開 API 仍 HTTP 200，但 OpenBB 產品健康為 `stopped`，不可說全服務正常。10:53 的 OpenBB journal 證實根碟剩餘 **107,127,414,784 bytes**，小於服務固定的 **107,374,182,400 bytes（100 GiB）**，supervisor 依安全契約退出 2，立刻一次重啟的 preflight 仍拒絕；目前 unit 明確 `failed`。這不是 CBC 分頁變動所致，也不是降低空間保留線就能安全修復的錯誤。

正式 compiler-cache 清理的唯讀 dry-run 為 **0 eligible／0 可回收 bytes**；同時刻核准的 C 冷儲存保留計畫僅 **274,432 allocated bytes** 可回收，遠不足容量缺口，故未刪除任何來源／帳本／快取，亦未強制重啟 OpenBB。原 timer 只在開機後與平日 09:12 觸發，這次 10:53 停機若空間稍後恢復會等到下個工作日；現在增加每日 **15:15（台北）** 安全重試，既有磁碟門檻與開盤資源窗口仍在。已更新 timer 模板與本機已安裝檔、`systemd-analyze verify` 通過，只重啟 timer，不啟動 downloader；下一次為今日 15:15，服務仍保留 `failed/exit 2` 真實狀態。容量需實際增大到安全線之上，15:15 自然嘗試與其來源收據才可證明恢復。相鄰開機／來源歸檔回歸 **39 passed**。

## 2026-09-25 央行外匯存底來源歸檔：同契約較大官方分頁

`stockagent-tw-public-release-archives.service` 07:04～07:10 的正式輪總 wall **369.982 秒**；其中 CBC 外匯存底來源抓取 **129.150 秒**。原來源清單使用每頁 20 筆，該輪須掃 **393 頁**；在既有每請求至少 0.25 秒的共享節流下，光順序請求間隔即約 **98 秒**，不能靠加 worker 解決。已存的官方頁面明列每頁 20／40／60 筆，並給出 `lp-302-1-1-60.html` 形式。直接請求 60 筆版第一頁為 HTTP 200、131 頁；只讀抓完整 131 頁、仍用原 0.25 秒節流後，與本機該正式輪的 393 頁來源逐 URL 對照：雙方均 **317 個**外匯存底公告，無新增／漏失，60 筆版只讀 wall **73.019 秒**。這是不同時間與網路狀態下的來源對照，**不是**正式 A/B，也不代表 369 秒總工時已縮短 56 秒。

程式現將正常線上掃描改為官方的 60 筆分頁，保留完整逐頁掃描、頁數一致性、HTTPS 原文 hash、所有公告 detail 驗證與共享請求節流。新 60 筆頁的本機原文前綴為 `page-0060-`，絕不與舊 20 筆頁混讀；離線／既有快取模式若沒有新首頁，仍可使用舊 20 筆完整快取；若新首頁存在而後續頁未齊則拒絕混合。正式來源摘要新增 `listing_page_size`、`listing_pages` 與 listing／detail／Parquet 三段秒數，便於 16:30 下一次自然輪次驗收；**本輪未**啟動正式來源／特徵重建，也未改動任何已發布來源檔。相鄰 CBC／歸檔稽核／協調器／公開進度與來源 catalog **32 passed**，Ruff 與 `git diff --check` 通過；下一輪還需比對正式 `complete`、317 筆或新增來源的合理變化、逐頁收據、官方審核及總 wall，不能僅用較少頁數宣稱業務完成。

## 2026-09-25 10:42～10:49 舊 Parquet 清冊有界重驗及公開欄位增量收據

上一輪加入新項目的 inode／ctime 綁定，但現存約 57,296 個舊快取項尚無此證據。現在由原本每 30 秒的 supervised 資料監控程序，每輪最多重讀 **1,024** 個舊 Parquet footer；新檔／真正變更仍保有獨立的既有 **4,096** 檔額度，不被遷移佔滿。讀前後檔案身分須一致。每輪公開 `record_inventory_progress.identity_unbound_files` 與 `identity_rechecked_files`；前者是**舊版大小＋mtime 快取尚未重驗數**，不是聲稱那些檔案遺失或歷史資料不完整。頁面同時顯示進度，不能把尚未重驗的舊項說成全數已綁 inode。

舊 footer 重讀若得到完全相同的欄位 schema、筆數、null 統計及資料集首末界線，清冊只更新來源身分證據；用持久 `feature_revision` 綁定 53 MB 公開欄位快照收據，**不重寫相同欄位清單**。任一 footer 統計、成員集合或資料集聚合改變即換 revision、重建欄位快照；收據缺失、revision 不符、來源內容 SHA 不符或程式／catalog 依賴更新仍退回完整解析／重建。現有完整 `/data-monitor/api/features` 契約未刪除，公開 gateway 仍驗快照來源與 sidecar。這個 revision 表示欄位投影的語意版本，**不是**Parquet 內容完整 SHA、資料發布或 PIT 合格證明。

自然 timer 10:46:58 一輪記錄 `identity_rechecked_files=1,024`、剩餘 **47,056**、`feature_reused=true`、總 **4.858 秒**；10:48:29 的一輪剩餘 **43,984**、總 **5.423 秒**（其中服務趨勢取樣 460 ms）。同批期間欄位快照保持 87,615 欄、54,576,257 bytes、SHA-256 `3ab2e0f120edd36b41499134ee2c7bc2d1573b6085b9b4667436afb9162a5445`，沒有每輪重新寫 53 MB；這是**來源統計相同期間**的增量收益，遷移中 footer 讀取與 38 MB 清冊原子更新仍令背景輪次高於遷移前約 2.5 秒。10:49 本機頁面 HTML 已載入 `app.js?v=39`，欄位進度 API 有剩餘 **42,960**、JS GET 200；未做外網瀏覽器跨裝置量測。另修正讀取路徑：若快取仍有舊聚合卻把 `files` 或 `schemas` 寫成非物件，不能沿用「已核實」聚合，必須回到逐來源 `scanning`。監控／閘道／服務測速相鄰 **204 passed**，Ruff、Node syntax、diff check 通過。重測可用 `journalctl -u stockagent-data-refresh-status-snapshot.service -o cat | rg 'data_monitor_timing'` 核對每輪重驗、剩餘、feature reuse 與耗時，並核對公開 API 的 `record_inventory_progress` 及完整欄位檔 SHA。

## 2026-09-25 10:34～10:36 實存清冊讀檔競態防護

欄位／筆數清冊的新增 Parquet footer 原先以讀取前的 `size+mtime` 記錄；若來源在 footer 讀取期間被原子替換，當輪可能把舊統計與新路徑配對。現在每次**新讀取** footer 後重查裝置、inode、大小、mtime、ctime；不一致或檔案消失即丟棄該筆快取、保留 `scanning`，下一輪再試。新寫入的快取項亦綁上述檔案身分，使同大小、還原 mtime 的後續替換會重新讀 footer；舊版約 57,296 項仍按原 `size+mtime` 規則，**不能宣稱歷史項已全部重驗**。若要全面升級須規劃有界 footer 重驗，不可在本輪把舊來源冒充已完成新驗證。

測試涵蓋同大小／同 mtime 但 inode 改變，以及 footer 讀取中被替換時不發布舊筆數；資料監控、公開閘道相鄰 **177 個測試通過**，Ruff／差異檢查通過。10:35:53 首次因程式依賴變動而重建欄位收據、`feature_reused=false`，下一輪 10:36:23 恢復 `feature_reused=true`、總約 **2.827 秒**、本機監控 summary HTTP 200。這是防止錯誤統計的健壯性修正，**不是**宣稱 2.5～6 秒背景輪次全面加速；OpenBB L1 仍受盤中 `ExecCondition` 保護，不為測速啟動，FinLab 仍在配額保留線等待。

## 2026-09-25 10:18～10:28 資料監控逐階段測速與日誌掃描

30 秒 supervised snapshot 現已在原有 `data_monitor_timing` 收據中記錄公開投影各階段，以及 Shioaji 的本機收據／journal、歷史管線等子階段；欄位清冊重建另拆成檔案簽章、欄位聚合及輸出列，這些診斷欄位**不加入公開 API**。10:27:50 一輪 `feature_reused=false` 的自然 timer 記錄總 **5,339 ms**（其中 service 趨勢取樣 601 ms、inventory 1,444 ms、public 1,016 ms、feature 2,154 ms）；feature 的原始檔案簽章檢查 **351 ms**、欄位聚合 **728 ms**、輸出列 **279 ms**。10:28:20 無來源變更、重用完整 feature 收據的一輪總 **2,530 ms**，inventory 1,452 ms、public 927 ms、feature 30 ms；仍完整掃描 57,296 個選定 Parquet 的簽章，不能因熱輪較快就冒稱完整來源重建也快。

公開投影中 Shioaji 狀態每輪約 **0.48～0.59 秒**，其中歷史管線／收據約 **0.30 秒**；監控的永豐頁只讀本機收據及有界 journal，沒有登入或消耗行情連線。日誌進度選取原先對 **11,040 個** intraday 舊 log 建立 Path 並全排序，現在只單次掃描找實際最新修改時間的檔案；獨立熱測 5 次約 **87～102 → 72～80 ms**。新舊測試均保留舊檔若續寫即成為最新的語意。自然 timer 的該階段曾量到 207～268 ms，新版兩輪為 66～69 ms，但受並行負載影響，不能把整段差額全歸因於這個局部改動。監控與永豐相鄰 **103 個測試通過**、Ruff 與 diff check 通過；本機 `/data-monitor/api/summary`、gzip `/api/features` 分別 HTTP 200（單次 11／205 ms）。根磁碟仍 95%，全資料健康與其他服務缺口未因此解決。

## 2026-09-25 TAIFEX ATM 單來源增量投影與 Yahoo 缺口核對

ATM 月／週全史原先在新來源使 manifest 失效時，各自重新解析全部 34 份官方選擇權收據；前次正式階段約 185／107 秒。現把每份收據在當時 TX 日盤合約與開盤價下的**每日候選**存成節點本機可替換快取，正式總表仍走同一個 `build_taifex_opening_atm_straddles` 選取、跨收據衝突和缺日補列規則。每輪先完整 SHA-256 驗所有官方來源，快取另綁來源 SHA、直接程式依賴指紋、該收據所有 TXO 日期的 TX 合約／開盤價指紋與投影內容 SHA；資料變動只重建受影響的收據。即使週選擇權該來源為空，仍記錄只有月選擇權的日期，維持 `no_weekly_txo_listing` 與 `missing_txo_daily_partition` 的區分。快取位於 `artifacts/cache/taifex_option_atm_sources`，不進正式 `data_tw_index_options_daily` 冷發布；正式輸出和 manifest 未被此隔離測試替換。

以正式 `manifest.json`／`manifest_weekly.json` 的各 34 份來源、相同 TX 期貨檔在本機隔離輸出測試：月度首次投影 **59.325 秒**、總表 **0.700 秒**，週度首次投影 **50.241 秒**、總表 **0.624 秒**；輸出 SHA-256 分別與正式檔逐位元組相同（`230ade09e2a8181e020e2c7691a64bb12436bf63601a3fe32151a47bcf8787f9`、`927d24092ad8ab768635e15e99f2f4a60651c7215051d1e99ab445207b243448`）。加入快取路徑符號連結檢查後，以最終程式版本重新預熱 34 份來源，月／週冷投影 **65.768／51.227 秒**、熱快取校驗 **0.943／0.608 秒**；再完整核對每份原始來源及 TX 檔 SHA，熱快取建置正式等價輸出分別 **1.552／1.284 秒**（不含來源 SHA 的預先核對），輸出雜湊仍完全一致。這些是隔離測速，並非下一輪正式 systemd wall，也不把不同負載的 185／107 秒當作嚴格 A/B。相鄰 ATM／全鏈／期貨及研究回歸 **76 passed**，Ruff 和 diff check 通過；正式輪仍需觀察新來源、記憶體峰值、發版、讀者與來源耗時。

Yahoo US 本輪 `daily_update_summary.us_stocks.json` 的 **12 failed** 對應 `repair_report.csv` 中同樣 **12 metadata_invalid**；本機 12 個 Parquet 都仍存在，但其中 11 個缺少可信 Yahoo 來源中繼資料，CWAN 標示 `source=cboe`。本輪對這 12 檔的 Yahoo 請求均回空資料，故不能把現存列直接冒充已核驗的 Yahoo 歷史、補寫來源標籤或把來源失敗改成成功。另有 **618 lagging_skip** 按既定延遲政策未補；`stale=12,219` 已對應同輪 `repaired=12,219`，不可重複計作缺口。約 12,219 次修補在現行 10 req/s 客戶端節流下有約 1,222 秒最簡請求下界，觀察到的 1,281 秒主要受此限制，增加 worker 不會突破該界線。本輪只核對證據，沒有改 Yahoo 請求率或未證實資料。

## 2026-09-25 TAIFEX 選擇權完整鏈增量分片

盤後因果時鐘不變：先驗官方日檔收據與 TX 日盤來源，再以同日 TX open 指定研究用相對履約價軸；各 TXO open／close 仍是該合約首／末筆成交價，不是同步 bid/ask 或券商成交。前一輪全量正規化約 740.978 秒，其中月／週 full-chain 約 228.220／170.658 秒；重複逐年解析是主要可減的計算。曾在單一 2025 年官方 ZIP 的 profiler 量到全鏈約 14.85 秒，其中 `_read_txo_rows` 約 9.97 秒。每系列到期日由同日數千個選擇權腿改為每個到期系列計算一次；同一 223,298 列年分區 A-B-B-A 輸出 SHA 完全相同，新舊測速為 7.427／6.860／7.113／7.036 秒，這只是局部小幅改善。

正式月／週完整鏈現在各按**一份官方來源收據**儲存節點本機可替換的驗證投影快取，並保留既有全歷史正式 Parquet 與 manifest。每輪仍對全部原始來源做完整 SHA-256 與品質驗收；分片重用另核對該來源 SHA、受其影響的實際交易日 TX open 指紋、直接程式依賴雜湊和分片 SHA。來源日包含因 TX open 不可用而沒有輸出列的日子，歷史 TX 修正會使相關分片失效；月與週都沒有有效列的單份來源可有空分片，但合併後的整條鏈仍必須非空。合併時拒絕跨收據交易日重疊、欄位契約漂移與分片在讀取時變動；輸出仍原子替換。來源檔完整 SHA 讀取前後及建置完的 inode／size／mtime／ctime 一致性亦在正式 script 驗證，同大小改寫或改連結會拒絕發版。正式發布、期貨／選擇權價格契約和缺漏 fail-closed 不變。

第一次實驗把分片放進 `data_tw_index_options_daily`，經 catalog 檢查發現這是會進冷發布的正式來源，已將**本輪產生的 136 個分片與收據檔、約 20.2 MB** 精確搬至 `artifacts/cache/taifex_option_full_chain_shards`；正式來源下已無 `full_chain_shards`。程式固定使用節點本機快取並拒絕其落在指定正式來源內，避免把可重建資料混入冷發布。這是同檔案系統搬移，沒有刪除原始來源、正式月／週 Parquet 或 manifest。

最終程式版本以既有正式 `manifest.json`／`manifest_weekly.json` 的 **34 份**官方來源、完整來源 SHA 與相同期貨檔唯讀重建：月鏈 2,358,194 列、週鏈 528,406 列；兩者的輸出 SHA-256 與既有正式檔**逐位元組相同**，品質摘要逐欄相同。一次性全分片建置為月 102.445 秒、週 75.886 秒；來源不變的下一輪 34 分片校驗與合併分別為月 0.032＋1.082 秒、週 0.012＋0.306 秒，且輸出 SHA 仍相同。這些是當下負載的唯讀實測，不能把原 228／170 秒與本次 102／76 秒直接當嚴格 A/B 加速比；現有月／週 ATM 全歷史建置約 185／107 秒仍未分片，來源真的變動時也要重新建相應分片。**尚未**在下一次帶新官方來源的正式 systemd 輪次驗收總 wall、記憶體峰值、發布及讀者；目前正式資料維持原樣，timer 下一次預定為今日 17:00 台北時間，屆時才會載入整合路徑。

## 2026-09-25 容量壓力：來源歸因與保留邊界

09:20 左右唯讀量測：根檔案系統 2,163,361,103,872 bytes，可用 108,627,632,128 bytes（約 101.16 GiB），使用率 95%；獨立 D 槽可用 2,613,418,995,712 bytes。根目錄中的 `artifacts` 實際配置約 592.24 GB，其中 `markets` 184.08 GB、`replays` 139.80 GB、`live` 103.28 GB、`cache` 56.03 GB、`audits` 38.69 GB、`maintenance` 24.79 GB。這些是分層佔用，不可相加成「可刪除」容量。現有 Binance archive 要保留檔案系統 10%（目前約 216.34 GB）與估計新資料峰值空間；9/24 最近一次在遠端探索前以 `filesystem_free_below_required_reserve` 暫停，systemd 因 `SuccessExitStatus=75` 顯示 `Result=success`，但業務結果不是資料更新成功。不能為了讓單元變綠而降低保留量。

`replays` 多個各約 5.4～5.5 GB 的 `signals.jsonl` 是主要佔用。既有去重服務明確排除 `.jsonl`、執行中的 `live` 及未完成 run；9/25 最近一次正式去重只回收 24,576 allocated bytes。`artifacts/cache` 的 12 個 `panel_cache_v2` 根下有 26 個 generation，逐一對照現有 `meta.json`／`variants/*.json` 均仍被引用，沒有可證明的孤兒 generation；獨立 cold materialized-cache GC dry-run 亦無可逐出的版本。這次沒有刪除、硬連結、搬移、降配額或更改冷備份政策。若要回收數十 GB，須先為回放／實驗輸出建立精確的 immutable 完成與使用者參照證據，再依獨立 D 備份、執行程序參照和既定退役政策規劃，或擴充根磁碟；目前不能把目錄名稱當成刪除證明。

同一候選樹的去重唯讀盤點原需完整 SHA-256 雜湊 3,717 個不同 inode，首次實測 28.696 秒；但只找到 4 組、5 個重複 inode，潛在實體回收僅 311,296 bytes。現先依不可變的大小、檔案身分／權限／擴充屬性分組，對仍可能相同的檔案各讀開頭與結尾最多 64 KiB 作**排除用**指紋；樣本相同者仍逐檔做完整 SHA-256，正式替換前原有的逐檔完整 SHA-256 重驗及原子硬連結不變。相同樹測得抽樣 3,647 inode、完整雜湊 9 inode，首次 13.808 秒；緊接著以 Git HEAD 舊程式與新版於同一進程做完整逐組 SHA／路徑對照，4 組完全一致，當輪舊／新耗時 19.428／13.695 秒。這是低優先級例行 I/O 減少，不等於釋出 21 GB 空間，也不推論系統所有服務同倍加速；去重／冷啟動相鄰 13 個測試、Ruff 與差異檢查通過。新版會由 9/26 約 03:33 的下次自然 timer 執行，不重啟交易、行情或公開閘道。

## 2026-09-25 Windows／WSL 公開閘道啟動測速腳本部署

先前約 274 秒的 WSL 後端恢復事件，已定位在 Windows 首次派送至同 VM 網路／Linux userspace 可用之前；但當時 Caddy supervisor 沒記每次 `wsl.exe` 子程序退出碼。先把非阻塞完成紀錄部署到 Windows 排程的安裝檔，受控重新啟動**公開 Caddy 排程本身**後，首次紀錄實際得到 `exit_code=-1`；同時後端已健康只表示舊服務仍在運轉，不能證明派送成功。

用與排程相同的 Windows `ProcessStartInfo` 重現：`--distribution "Ubuntu-26.04"` 回 `WSL_E_DISTRO_NOT_FOUND`／`-1`，而 `--distribution Ubuntu-26.04` 退出 0。Windows registry 的預設發行版名稱精確為 `Ubuntu-26.04`，互動 PowerShell 直接呼叫也退出 0；因此根因是這條啟動鏈的引數組法，不是 WSL 發行版不存在。現在對只含字母、數字、點、底線、連字號的發行版名稱不用多餘引號；不符時明確失敗，不靜默啟動其他發行版。新版亦分開記子程序 `StartTime→ExitTime` 與 5 秒 supervisor 輪詢造成的觀察落後，避免把輪詢延遲冒充 WSL 執行時間。相同的唯讀 `systemctl is-active` 呼叫在修正後 `exit_code=0`、回 `active`。

每次部署都先複製到同一 Windows 目錄的暫存檔、驗 SHA-256，再原子替換安裝檔並保留舊檔備份；最後一份備份為 `C:\Users\agari\AppData\Local\StockAgentPublic\start-caddy.ps1.pre-20260925T024547.bak`。排程 action 已核對指向該安裝檔，PowerShell 語法零錯誤，啟動鏈／覆蓋／趨勢 **31 個測試通過**。02:45:54 受控重啟排程載入最終版本，正式 `startup.log` 出現 `exit_code=0 elapsed_seconds=0.186 observed_lag_seconds=5.532`；這是**已運轉 WSL** 上的子程序執行時間，不是冷 VM 啟動延遲。Windows Task Scheduler 工作仍為 running，`caddy.exe` PID **5060、7172** 維持不變，本機與 DDNS 強制 IPv4 HTTPS `/healthz` 均 HTTP 200（各約 1／16 ms 單次樣本）；當沖、TAIFEX 擷取、Discord 的 PID／InvocationID 亦未變。最終唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260925T0246-wsl-argument-fixed.json` 列 **50 service／39 timer／2 path**、launcher 與 Caddyfile 安裝檔皆 `exact_match=true`，排程為 `Running`；`systemctl --failed` 為 0。尚未做 Windows 真冷開機驗收，也未解決外部 IPv6。Task Scheduler 的既存 `LastTaskResult=0x800710E0` 與工作正在執行並存，不能拿該欄單獨證明成功或失敗。

## 2026-09-25 註冊資料全量回補：避免假成功、保留逐輪證據

最近一輪完整收據 `registered-backfill-20260920T022411Z` 約 65,733 秒，以 `completed_with_failures` 結束。步驟收據顯示 Binance 1m 約 30,213 秒、574 檔中 1 檔失敗；OKX 1m 約 65,726 秒、482 檔價格列完成，但歷史特徵仍有 4 檔 `partial`。舊 OKX 程式只把 `progress.json` 標成 failed，程序仍退出 0，導致步驟被算成成功；Bybit 的 `failed`／`repair_required` 來源有同一類退出碼錯誤。Binance 那輪的逐檔報表已被後續 tail 任務覆寫，不能從現在的報表反推當時失敗標的，也不能用新 tail 成功宣稱完整歷史已修復。

現在 OKX／Bybit 在完成最新報表與進度收據後，若來源或特徵不完整即退出非零；完整結果仍退出 0。三家全量回補都把當輪來源報表封存到該次 `step_receipts/<run-id>/<provider>_source/`，後續 tail 任務不再覆寫這份逐輪證據。這是正確性與可追查性修正，**沒有把 18 小時下載變快，也沒有重跑全量來源**。離線回歸 131 個測試通過，包含完整／不完整兩種退出結果、封存後最新報表被替換的情形；下次實際全量回補仍須核對逐檔報表、步驟退出碼、完整特徵與最終收據，不可僅看 systemd 單元 inactive／Result=success。

## 2026-09-25 永豐公開狀態：收據時間一致性與小幅讀取優化

永豐公開狀態僅讀本機收據與有界 journal，不登入券商，也不在行情 callback 中做同步查詢。單獨行程 profiler 實測冷建置約 520 ms、同一行程第二次約 84 ms；769 份期貨歷史 manifest 是主要讀取群之一。原本每份 manifest 解析後再單獨 `stat` 兩次取得顯示時間，檔案若在中間換版，可能把新時間貼在舊內容。現在 JSON 與 mtime 從同一份前後簽章一致的讀取返回，遇到讀取中換版仍拒絕當輪使用。

同機熱讀、四組交錯舊／新函式的 manifest 清單中位數為 27.64／23.36 ms，769 筆輸出逐筆相同；這只是該子步驟的小幅改善，不能推論整頁冷請求同等加速。Shioaji／公開頁／測速相關 138 個測試通過。只重啟公開 gateway 後，本機與 IPv4 HTTPS `/healthz` 均回 `ok`；永豐 API 的 10 項 pipeline 狀態、`degraded` 健康值、回補／擷取狀態及已用流量在重啟前後相同。Shioaji 行情與交易行程未重啟；`degraded` 的來源證據未被 UI 修飾為正常。

公開路由後續三筆本機樣本約 329／2.9／4.3 ms，IPv4 HTTPS 一筆約 21 ms；因該路由有 8 秒新鮮期與背景更新，快取命中不代表冷建置已縮到數毫秒。夜盤服務 journal 明確記錄 `cannot cash-settle cycle while a shadow futures hedge remains open`，於 9/24 14:51 將策略 bootstrap fail-closed、行情改為 data-only；目前公開狀態的 `strategy_bootstrap_ready=false` 與持續行情擷取一致。這是需另查模擬避險腿與到期結算契約的實際策略阻擋，不在網站測速修正中自動清算或重啟交易行程。

## 2026-09-25 備份監看器：安全性與延遲追蹤

`stockagent-packed-backup.service` 的 `current_heads` 閒置對帳沒有複製物件，卻曾在 D 槽逐物件路徑驗證停留很久；這不是雜湊重讀或來源重建。相同 11,834 個物件、121 個當前 release 的三輪正式收據如下，單位 ms。原始值留在當輪 systemd journal；最新一輪亦在 `/var/lib/stockagent-packed-backup/status.json` 的 `stage_timings_ms`。不同時段 D 槽負載可能不同，不把這些差值當作保證加速比。

| 正式對帳 | 來源清單 | D 槽信任檢查 | metadata | 最後狀態前合計 | 結果 |
|---|---:|---:|---:|---:|---|
| 變更前，02:07 完成 | 1,346 | 246,856 | 12,253 | 260,484 | `up_to_date`，零待搬／零錯誤 |
| 變更後，02:11 完成 | 1,268 | 9,746 | 12,368 | 23,416 | `up_to_date`，零待搬／零錯誤 |
| 相同 metadata 略過重複掛載查詢後，02:13 完成 | 1,195 | 9,498 | 10,129 | 20,845 | `up_to_date`，零待搬／零錯誤 |

原因是舊路徑對每個物件在 Windows D 槽重複進行每層 symlink／`resolve` 查詢；80 個隨機物件的唯讀對照約 1,867 ms，單次 `lstat` 約 203 ms。新路徑對每個物件仍查 regular-file 身分、沿用既有 SHA-256 收據的 inode／大小／mtime／ctime 與 30 日重驗條件；只把同一輪的目錄層以 `O_NOFOLLOW` 描述符固定，結尾重查每層路徑是否被替換。未變動的 metadata 已讀比對相同後不用再逐檔重查掛載；每輪起迄與每次寫入前仍核對 D 槽身分。新增檔案、來源變動、checksum 回讀、head 原子提交不變。正式驗收目前為 `current_heads_complete=true`、`present_objects_complete=true`、`remaining_bytes=0`、`historical_completeness=not_checked`；不能由此聲稱歷史版本完整。C 與獨立 D 完成收據的時間、物件數、驗證 bytes 一致。服務重啟後也會立即標示 `checking` 與逐物件掃描進度，不再把前次 `up_to_date` 當成當輪狀態。相關備份／保留測試 36 個通過，D 槽 80 個隨機物件新舊身分結果一致。

`metadata` 最後一輪仍約 10.1 秒，是下一個已量到的成本；未在缺乏逐子階段證據前放寬 manifest、head 或 object 的完整性驗證。

02:15 的唯讀全服務重測收據為 `artifacts/benchmarks/service-coverage-20260924T181532Z.json`：50 service／39 timer／2 path、10 秒資源取樣，沒有 systemd `failed`。這不能替代產品狀態：六個 localhost API 均 HTTP 200，但 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、永豐 `degraded`、全資料 `critical`；OpenBB 為 `active`。這次 Shioaji 狀態請求約 336 ms，仍有 receipts／來源查核成本；備份優化沒有處理該服務。CPU／memory／I/O PSI 的 10 秒平均都為 0，只能代表取樣時段，不能推論日間尖峰。

本清單由實際安裝在 penguin WSL 的 systemd unit files 與當下 runtime 狀態產生，作為逐項測速與優化基準。`inactive/dead` 對一次性 service 是正常待命；`failed` 是失敗。資料新鮮度另看收據。

下表列出 02:14 再枚舉的 **50 service／39 timer／2 path**。狀態欄保留 9/23 13:10 的原始基線；當時尚未安裝的 FinLab 兩項明確標記，不把歷史狀態當成現在健康。即時完整狀態請看 `artifacts/benchmarks/service-coverage-20260924T0214-corrected.json`。

## 50 個已安裝 service

| 類別 | Unit | 安裝方式 | 13:10 狀態 | 對應 timer |
|---|---|---|---|---|
| 儲存／維運 | `stockagent-artifact-dedup.service` | static | inactive/dead | 有 |
| 加密／註冊資料 | `stockagent-binance-public-archive.service` | disabled | inactive/dead | 有 |
| 加密／註冊資料 | `stockagent-crypto-training-refresh.service` | disabled | inactive/dead | 有 |
| 儲存／維運 | `stockagent-data-cache-gc.service` | static | inactive/dead | 有 |
| 儲存／維運 | `stockagent-data-refresh-status-snapshot.service` | static | inactive/dead | 有 |
| Discord | `stockagent-discord-artifact-maintenance.service` | disabled | inactive/dead | 有 |
| Discord | `stockagent-discord-bot.service` | enabled | active/running | — |
| Discord | `stockagent-discord-postclose-cache.service` | disabled | inactive/dead | — |
| FinLab 帳號研究資料 | `stockagent-finlab-local-refresh.service` | disabled | 當時未安裝 | 有 |
| FinLab 帳號配額觀測 | `stockagent-finlab-quota-snapshot.service` | disabled | 當時未安裝 | 有 |
| 儲存／維運 | `stockagent-hot-artifact-sync.service` | disabled | inactive/dead | — |
| OpenBB | `stockagent-openbb-archive.service` | static | active/running | 有 |
| OpenBB | `stockagent-openbb-l1-compaction.service` | static | inactive/dead | 有 |
| 儲存／維運 | `stockagent-packed-backup.service` | enabled | active/running | — |
| 儲存／維運 | `stockagent-packed-retention.service` | static | inactive/dead | 有 |
| 公開網站 | `stockagent-public-dashboards.service` | enabled | active/running | — |
| 加密／註冊資料 | `stockagent-registered-data-backfill.service` | disabled | inactive/dead | 有 |
| 加密／註冊資料 | `stockagent-registered-data-daily.service` | disabled | inactive/dead | 有 |
| 加密／註冊資料 | `stockagent-registered-data-features.service` | disabled | failed/failed | 有 |
| 加密／註冊資料 | `stockagent-registered-data-intraday.service` | disabled | inactive/dead | 有 |
| 儲存／維運 | `stockagent-remote-cold-artifact-ingress.service` | static | inactive/dead | 有 |
| 永豐／行情 | `stockagent-shioaji-historical-market-data.service` | static | active/running | 有 |
| 永豐／行情 | `stockagent-shioaji-minute-backfill.service` | static | active/running | 有 |
| 永豐／行情 | `stockagent-shioaji-storage-monitor.service` | static | inactive/dead | 有 |
| 永豐／行情 | `stockagent-shioaji-taifex-bidask.service` | enabled | active/running | — |
| 永豐／行情 | `stockagent-shioaji-taifex-dashboard.service` | enabled | active/running | — |
| 永豐／行情 | `stockagent-shioaji-top200.service` | enabled | active/running | — |
| 永豐／行情 | `stockagent-shioaji-tx-history-backfill.service` | static | active/running | 有 |
| 儲存／維運 | `stockagent-storage-pressure.service` | static | inactive/dead | 有 |
| TAIFEX 官方資料 | `stockagent-taifex-auxiliary-daily.service` | disabled | inactive/dead | 有 |
| TAIFEX 官方資料 | `stockagent-taifex-futures-daily.service` | static | inactive/dead | 有 |
| TAIFEX 官方資料 | `stockagent-taifex-public-history.service` | disabled | inactive/dead | 有 |
| 時鐘 | `stockagent-time-sync-check.service` | static | inactive/dead | 有 |
| 當沖 | `stockagent-tw-day-trade-eligibility.service` | disabled | inactive/dead | 有 |
| 當沖 | `stockagent-tw-day-trade-margin-actions.service` | static | inactive/dead | 有 |
| 當沖 | `stockagent-tw-day-trade-minute-curves.service` | static | inactive/dead | 有 |
| 當沖 | `stockagent-tw-day-trade-multi-basis-22-history.service` | static | inactive/dead | 有 |
| 當沖 | `stockagent-tw-day-trade-preopen-gate.service` | static | inactive/dead | 有 |
| 當沖 | `stockagent-tw-day-trade-simulation.service` | enabled | active/running | — |
| 當沖 | `stockagent-tw-day-trade-unattended-guardian.service` | static | inactive/dead | 有 |
| 隔日沖 | `stockagent-tw-overnight-history.service` | static | inactive/dead | 有 |
| 隔日沖 | `stockagent-tw-overnight-simulation.service` | enabled | active/running | — |
| 台灣公開資料 | `stockagent-tw-public-0830-check.service` | disabled | inactive/dead | 有 |
| 台灣公開資料 | `stockagent-tw-public-cold-publish.service` | disabled | inactive/dead | 有 |
| 台灣公開資料 | `stockagent-tw-public-feature-reconcile.service` | static | inactive/dead | 有 |
| 台灣公開資料 | `stockagent-tw-public-official-catalogs.service` | static | inactive/dead | 有 |
| 台灣公開資料 | `stockagent-tw-public-publication-sweep.service` | disabled | inactive/dead | 有 |
| 台灣公開資料 | `stockagent-tw-public-release-archives.service` | static | inactive/dead | 有 |
| 台灣公開資料 | `stockagent-tw-public-source-events.service` | enabled | active/running | — |
| 儲存／維運 | `stockagent-wsl-backfill-memory-reclaim.service` | static | inactive/dead | 有 |

## 39 個已安裝 timer

| Timer | 安裝狀態 |
|---|---|
| `stockagent-artifact-dedup.timer` | enabled |
| `stockagent-binance-public-archive.timer` | enabled |
| `stockagent-crypto-training-refresh.timer` | enabled |
| `stockagent-data-cache-gc.timer` | enabled |
| `stockagent-data-refresh-status-snapshot.timer` | enabled |
| `stockagent-discord-artifact-maintenance.timer` | enabled |
| `stockagent-finlab-local-refresh.timer` | enabled |
| `stockagent-finlab-quota-snapshot.timer` | enabled |
| `stockagent-openbb-archive.timer` | enabled |
| `stockagent-openbb-l1-compaction.timer` | enabled |
| `stockagent-packed-retention.timer` | enabled |
| `stockagent-registered-data-backfill.timer` | enabled |
| `stockagent-registered-data-daily.timer` | enabled |
| `stockagent-registered-data-features.timer` | enabled |
| `stockagent-registered-data-intraday.timer` | enabled |
| `stockagent-remote-cold-artifact-ingress.timer` | enabled |
| `stockagent-shioaji-historical-market-data.timer` | enabled |
| `stockagent-shioaji-minute-backfill.timer` | enabled |
| `stockagent-shioaji-storage-monitor.timer` | enabled |
| `stockagent-shioaji-tx-history-backfill.timer` | enabled |
| `stockagent-storage-pressure.timer` | enabled |
| `stockagent-taifex-auxiliary-daily.timer` | enabled |
| `stockagent-taifex-futures-daily.timer` | enabled |
| `stockagent-taifex-public-history.timer` | enabled |
| `stockagent-time-sync-check.timer` | enabled |
| `stockagent-tw-day-trade-eligibility.timer` | enabled |
| `stockagent-tw-day-trade-margin-actions.timer` | enabled |
| `stockagent-tw-day-trade-minute-curves.timer` | enabled |
| `stockagent-tw-day-trade-multi-basis-22-history.timer` | enabled |
| `stockagent-tw-day-trade-preopen-gate.timer` | enabled |
| `stockagent-tw-day-trade-unattended-guardian.timer` | enabled |
| `stockagent-tw-overnight-history.timer` | enabled |
| `stockagent-tw-public-0830-check.timer` | enabled |
| `stockagent-tw-public-cold-publish.timer` | enabled |
| `stockagent-tw-public-feature-reconcile.timer` | enabled |
| `stockagent-tw-public-official-catalogs.timer` | enabled |
| `stockagent-tw-public-publication-sweep.timer` | enabled |
| `stockagent-tw-public-release-archives.timer` | enabled |
| `stockagent-wsl-backfill-memory-reclaim.timer` | enabled |

## 2 個已安裝 path unit

| Path | 安裝狀態 |
|---|---|
| `stockagent-discord-artifact-maintenance.path` | enabled |
| `stockagent-tw-day-trade-minute-curves.path` | enabled |

## 關聯的非 StockAgent systemd 服務

- `syncthing@root.service`：目前 active/running。本機程序活著，不代表所有 peer 已同步或冷資料可重建。
- Windows 排程 `StockAgent Public Caddy`：Windows 工作狀態 Running（TaskState 4），提供公網 HTTPS；`StockAgent Preserve Crash Dumps` 為 Ready；`StockAgent One-Time WSL Filesystem Repair` 與 `WSL Daily Backup - Ubuntu-26.04` 為 Disabled。
- 本機端口：公開 gateway `127.0.0.1:8770`，TAIFEX `127.0.0.1:8765`，當沖引擎面板 `127.0.0.1:8766`，Syncthing 管理介面 `127.0.0.1:8384`。Caddy 是 Windows 排程，未在 WSL systemd 以 unit 呈現。

## Repo 中有模板、但本機未安裝

- `stockagent-cold-artifact-maintenance.service/.timer.in`
- `stockagent-tw-mops-xbrl.service/.timer.in`

這些是可部署定義，不計入本機已安裝服務性能達標。

## 初始異常與測量邊界

- `stockagent-registered-data-features.service` 當下 `failed/failed`；不能把失敗工作算成零延遲。
- `stockagent-registered-data-intraday.service` 在首次盤點時 `activating/start`；完成／失敗需看終端收據。
- 交易、資料、儲存、Discord 的終端驗收須各依正式收據與 source/ledger，不因 unit `active` 或 HTTP 200 直接宣告正常。
- 本輪「所有」指這台 WSL 目前已安裝的 50 個 StockAgent service、39 個 timer、2 個 path，以及相關 Windows Caddy 與 Syncthing；最初 9/23 的 48／37 是 FinLab 兩項安裝前的盤點。遠端 Vast、路由器內部服務、其他 Windows 應用不能從本機清單推斷已驗證。

## 第一輪全服務與全路由實測

原始可重測收據：`artifacts/benchmarks/all_services_latency_2026-09-23.json`。`systemctl show` 對全部 48 個 service 查詢約 153 ms；最近一次完成的 job 中，`registered-data-features` 1,550.8 秒、exit 1，不能算作完成吞吐。其餘較久的成功 job 有 Binance public archive 1,544.7 秒、registered daily 1,312.5 秒、TAIFEX auxiliary daily 833.9 秒、TW public release archives 574.7 秒。這些是**不同工作量**的耗時，不可直接互相比快慢；持續型 daemon 的運作時間也不是請求延遲。

在 11.97 秒的 48-service 資源取樣內，TAIFEX dashboard 約 2.01 CPU cores、公開 gateway 約 0.64、registered intraday 約 0.38。這與同時進行的 HTTP 壓測重疊，不能外推為閒置常態。TAIFEX `marks.jsonl` 約 3.97 GB，mtime 為 9/18，取樣時服務仍按 55 秒 TTL 重建歷史；這是可避免的來源不變重算。

32 條公開 gateway 路由各有 8 個請求，HTTP 錯誤數 0。當沖完整 1m 歷史中位數約 2,294 ms，14.70 MB 解碼、約 3.83 MB gzip；TAIFEX 1d 歷史約 349 ms，其餘多數路由為個位數到百毫秒。這是 loopback 量測，不是 WAN、瀏覽器繪製或開盤訊號端到端延遲。

`registered-data-features` 的 9/22 收據有 574 個 Binance symbol，其中 569 個 partial，569 個均為 taker 統計分頁無前進；舊的根目錄 `data_binance/download_summary.json` 並非目前 1m 任務收據，最新收據在 `data_binance/1m/`。不能因新的分頁單元測試通過就宣告完整歷史資料健康。新的有界分頁先用官方最近三天 BTCUSDT 5m 資料驗證：865 筆、無中間缺口或重複；全量排程結果如下。

14:00 排程已於 14:25:47 完成，service `Result=success`、exit 0；不能只看 exit，另查特徵報表：Binance 574/574 `updated`、各階段 `ok`、0 個錯誤，原 569 個 taker 分頁錯誤為 0；OKX 486/486 `updated`、0 個錯誤。Binance 全步驟約 1,467 秒，整個 features cycle 約 1,547 秒；它是數千個外部資料項目的取得成本，不應拿來與毫秒 HTTP 路由直接比較。同步執行的 intraday job 於 14:24:56 覆寫 `data_binance/1m/download_summary.json` 的「本輪特徵啟用」欄位；本次特徵驗收使用有當次 mtime、574 個逐 symbol 階段及錯誤欄位的 `historical_feature_report.csv`，不把後來另一輪的旗標誤作本輪失敗或成功。

## 已實作並部署的延遲修正與重測

同樣的 32 條路由、每條 8 次請求重測收據為 `artifacts/benchmarks/all_services_latency_2026-09-23_after.json`。所有路由仍是 HTTP 200；當沖完整分鐘歷史中位數由約 2,294 ms 降至約 904 ms。這是相同機器與併發設定下的兩次樣本，**不是**每次請求的保證，也不是跨網路端到端時間。來源沒有變更時，TAIFEX 面板不再每 55 秒掃描 3.97 GB 的 marks；相同壓測下服務 CPU 取樣由約 2.01 cores 降至約 0.006 cores。這個改善是移除無效重算，不影響來源異動時的背景更新與失效檢查。

重測的當沖完整歷史約 904 ms 中位數，伺服器處理約 40 ms、回應體傳輸／解壓約 835 ms、JSON 解析約 245 ms（各分位數**不能相加**，樣本與併發重疊）。因此再微調 Python 迴圈已非主要優先級；後續要比較分區增量／二進位序列化的端到端收益，同時保留每分鐘點與完整來源證據。TAIFEX 1d 歷史約 212 ms 中位數，伺服器約 0.13 ms，主因同樣是回應體處理而非後端查詢。

當沖與隔日沖前端現在只在所選日期或歷史來源版本變動時，才重新下載大型分鐘曲線；狀態／訊號更新仍可個別刷新。完整歷史 gzip 壓縮使用實測較快的 level 3；傳輸位元組略增，壓縮 CPU 降低。當沖資格排程若已能以穩定的 TWSE／TPEx 官方收據驗證當日完整覆蓋，會略過不相關的全域 writer lock；來源異動或缺漏仍必須經原流程取得並驗證，不用舊資料冒充新資料。本機相同日期覆蓋檢查約 37 ms，09:15 排程後的實際耗時尚待次日驗證。

持倉與完整生命週期的歷史定位索引改為 schema v2：保留每份來源檔的原始定位列，僅解碼來源簽章變動的檔案，最後仍按原檔案順序合併並讓較後檔案覆蓋相同 position ID。即使原先覆蓋它的檔案被刪除，前一份檔案的紀錄會恢復。隔離副本基於 725 份實際來源、47,902 個定位列：單檔更動後持久索引增量建置 1.129 秒，同來源強制全掃 5.481 秒，結果逐筆一致；原始檔未被修改。`artifacts/benchmarks/tw_position_index_2026-09-23.json` 可重測。另按 profiler 找到 725 次 `Path.relative_to` 佔約 177 ms，改為受根目錄前綴檢查保護的相同相對路徑後，同一持倉頁 profiler 樣本由 322 ms 降至 65 ms。正式 gateway 已在收盤後套用，第一次重啟建置約 2.4 秒，後續一次不命中頁面快取的後端建置約 56 ms；這不是公網端到端保證。

公網瀏覽器驗收涵蓋 8 頁，1366×768 與 390×844 各一輪：兩者均無控制台錯誤、API 失敗或頁面橫向溢出；手機輪另無過小觸控目標。HTTPS IPv4 可連，但 WSL 對 DDNS AAAA 的 IPv6 連線仍失敗；Windows 本機 IPv6 443 listener 與防火牆規則存在，不能以 IPv4 結果宣稱外部雙棧完成。

追加完整日期區間測試後發現「快速的最新一天」不能代表完整帳本：當沖 2/25～9/23 的訊號／事件頁，無命中來源快取的本機端點分別約 3.62／4.78 秒。事件頁過去每次都重新讀取最多各 10 萬筆委託與成交，且未向 UI 告知截斷；現在加入依帳本與正式歷史檔 `device/inode/size/mtime_ns` 失效的 32 頁有界結果快取，以及逐來源掃描筆數、上限與截斷警示。來源不變時同一事件查詢約 0.24 秒、再一次約 0.19 秒；**首次不同查詢仍可能花數秒**，未冒稱冷查詢已完成最佳化。帳本追加即失效的單元測試已通過。兩個完整區間路由也列入持續測速清單；39 路由×5 次、HTTP 錯誤 0 的收據在 `artifacts/benchmarks/all_services_latency_2026-09-23_final.json`。該收據中完整區間事件頁的 first 僅 28 ms，是先前人工查詢已預熱同一 key 的結果，不可當成真正冷查詢。

當沖完整區間訊號原始投影是 100,000 列、約 34 MiB；頁面仍需套用即時狀態與持倉，不能只把整頁長時間凍結。新增最多 2 個、每個最多 64 MiB 的來源投影快取，僅在帳本 `device/inode/size/mtime_ns` 與所選日期均不變時重用；即時狀態版號仍使最後頁面重算。部署後新程序第一次相同完整區間查詢約 6.01 秒，帳本不變但改用「受阻」篩選約 0.288 秒，再查同一頁約 0.005 秒；來源追加失效與狀態版號改變時重用投影，均有測試。冷查詢數字在不同樣本間波動，不能把 0.005 秒當冷啟動性能。

最終修改後又在公網 Chromium 重測當沖頁：1366×768 與 390×844 都沒有橫向溢出或 JavaScript 例外，手機觸控目標風險 0；完整分鐘曲線保留 317,540 個點／8 個序列。桌面讀取與繪製約 1.078 秒，手機約 0.761 秒；這是一次端到端瀏覽器樣本，不是 P95 保證。報告在 `/tmp/stockagent-dashboard-browser-audit-2026-09-23-postfix*/`，重測時仍有 Permissions-Policy `web-share` 瀏覽器警告，無 JS 例外。

最後一輪手機瀏覽器驗收在切換日期時出現 3 個舊請求 `AbortError`，同一新選擇的端點後續均 HTTP 200；這是前端主動取消過期請求，不是資料服務健康證據，也不能把該輪表述為「完全沒有取消」。

串接驗收發現安全輸出白名單原本未傳出新事件截斷欄位，已補 `scan_limit`、`scan_limit_reached`、逐帳本 `source_rows_scanned`，並保留原始委託／部位 ID 遮罩。公開 HTTPS 完整區間回應已驗證 HTTP 200、`scan_limit_reached=true`、委託與成交各掃 100,000 筆；重啟後第一次事件冷查詢約 7.9 秒，後續同來源快取較快。最終修改後的手機公網 Chromium 再驗證：無橫向溢出、觸控目標風險 0、JS 例外 0，完整曲線仍有 317,540 點。

## 尚未達成與下一輪量測邊界

- `registered-data-features` 的 9/22 失敗已由 9/23 14:00 的完整排程驗收消除；但目前證據只涵蓋這次 574-symbol Binance 與 486-symbol OKX 特徵工作，以及各自報表列出的近期取得區間，不等於保證未來供應商都不缺資料或不再限流。
- TAIFEX auxiliary daily 最後約 834 秒，其中 option daily 會在每個交易日對多年 immutable 官方收據反覆解析，並分別重建月／週 ATM 與 full chain。本輪加入各階段 journal 耗時與同日重試的嚴格重用快路徑：官方收據、期貨檔、直接依賴程式、正規化輸出與 full chain 的雜湊都相同才跳過重算；它不會縮短**有新來源的每日完整建置**。下一步仍需不可變分區投影與增量合併，並以完整輸出逐欄一致、source provenance 與缺漏 fail-closed 驗收；目前**未**宣稱已修好此段。
- Registered daily 約 1,313 秒，其 Yahoo US 股票更新為主要工作；其收據仍含 failed symbol。不能只靠減少抓取量改善表觀耗時，必須先分開 provider 回應、已退市／不存在、更新與本地重算成本。
- `registered-daily` 的逐步收據確認 Yahoo US 股票約 1,294 秒（12,407 個 symbol，12,378 `repaired`、10 `new_symbol_repaired`、6 `not_found`、13 `failed`），CoinMetrics 約 349 秒，其餘主要步驟各低於 91 秒；並行執行使逐步耗時不可相加。現有 Yahoo `us_stocks` 約 8.8 GB、每個 symbol 的單一歷史 Parquet 在增量更新時仍會重寫，這是值得以不變歷史分區＋可驗證 tail 試驗的成本點，但不能在未保證退市股票、schema 與完整讀者相容前直接拆檔或刪除失敗 symbol。
- 09:00 五個模式的 ready gate 實測約 2.68–6.67 秒；第一筆永豐行情收據本身約 1.31 秒、覆蓋證據約 2.16 秒，現階段不能宣稱已達 1 秒端到端。盤前預備、行情到達、推論、持久化與通知仍要分段追蹤，不能用回放時間代替即時延遲。
- 9/23 的 opening latency 收據實際有**五**個模式：第一個 `attention_layernorm` 排程醒來約 6.7 ms，但需等行情覆蓋到 2.163 秒，訊號 ready 於 2.676 秒；其餘四個模式依序 ready 於 3.581、4.671、5.700、6.673 秒。模型推論本身各約 31–63 ms，後四個模式的等待與序列化比推論大得多。這是進一步設計共用來源快照與可驗證併發的證據，不能直接取消模型／帳本鎖或把第二個以後模式當成已完成。
- `tw-day-trade-margin-actions` 曾在今日 07:54 因衍生收據過期失敗而自動重試，後續 08:01 完成；當沖資格 09:15 一次耗 315 秒主要是等不相關 writer lock。服務最終 exit 0 不抹去該次延遲和失敗。
- `crypto-training-refresh` 今日 02:30 的計算已完成資料稽核，但發布時 `download_bybit_perp_1m.py` 正在寫入，正確的發布閘門拒絕了它；不能把這種失敗改為通過。現有 intraday 排程每次結束後 1 分鐘又啟動，單靠把訓練刷新改成另一個固定時刻仍可能競爭。需設計來源穩定快照／共同租約或可驗證的增量恢復，並確認不延緩盤中原始資料更新。
- 今日 systemd 錯誤日誌還有 08:30 官方資料閘門、09:05 盤前閘門與幾次 intraday 收集失敗。09:05 盤前閘門觀察到 2 個尚未套用的來源事件而 fail-closed；來源事件監測在 09:05:27 顯示未套用數回到 0。現在 `events/latest.json` 為 `ok` 不等於當時的失敗沒有發生，也不能由此證明明天盤前不會重演。
- 「全部服務到極限」不存在可從單日樣本證明的絕對界線；各個排程資料量、外部配額和硬體負載都會變。清單是完整作用域，這輪的修正與尚未消除的瓶頸分開記錄，不把其他服務的未量測情境稱為已最佳化。
- 14:14 的公開全資料摘要為 `critical`：326 個啟用資料端點中 221 complete、39 catching up、66 unable。14:25 全量特徵完成後重讀仍為 `critical`，數量變成 223 complete、38 catching up、65 unable。這是資料健康，不是 HTTP 或前端性能；不可因服務與面板正常回應就把其餘來源改成正常。
- `unable` 分散在臺灣官方公開資料、即時 Tick／五檔、TAIFEX Tick、台股分鐘／微結構、Yahoo、Binance、Dune、Pepperstone、免費市場脈絡與數個研究投影群組；這些需要逐一依 source/receipt、配額、憑證與實存覆蓋排障。不能從公開摘要推斷為同一個效能瓶頸，更不能藉刪除來源或改顯示狀態達成「變快」。
- 14:50、14:50:39、14:51、14:52 的 `stockagent-tw-public-publication-sweep` 四次實際嘗試均失敗：8 項中 6 項成功，但 TPEx 當日法人交易表為已驗證開市日無列，TWSE 法人交易回應非有效 JSON；摘要 `coverage_complete=false`、`data_status=incomplete`、exit 1。下次既有 timer 為 15:30；未把 6/8 當 8/8、未人工補造官方來源，也未因單次失敗去重啟交易程序。該工作每次約 15～39 秒、記憶體尖峰約 6.5 GiB；重複讀取多年的資料是下一個可測的成本候選，但先要保持官方來源的缺漏語義。

## 逐服務測速覆蓋與開機鏈追加（15:12～15:30）

新增可重複、唯讀的 `scripts/audit_service_latency_coverage.py`：一次列出全部已安裝的 StockAgent systemd 服務、timer、path，對每個服務區分「已完成 job 最近一次程序耗時」與「常駐程序取樣 CPU／記憶體」。後者**不是**一次功能請求的延遲。執行方式：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_service_latency_coverage.py --sample-seconds 10
```

本輪原始收據 `artifacts/benchmarks/service-coverage-2026-09-23T1512.json` 有 48 service、37 timer、2 path；實際取樣 13.38 秒。當時永豐分鐘回補約 2.37 CPU cores、註冊盤中資料約 1.48 cores、公開 gateway 約 0.03 cores。CPU 是該段負載與程序共享資源的觀察值，不代表各自的任務吞吐或瓶頸必然固定。`publication-sweep` 當時最近一次失敗程序約 22.50 秒；15:30 新一輪已開始，該時點的 `systemctl is-system-running=running` 僅因失敗狀態被新執行取代，15:30 的資料報表仍為 5 項成功、2 項失敗、`coverage_complete=false`，不可用 systemd 顏色取代資料驗收。

擴充 Windows 枚舉後再跑的收據 `artifacts/benchmarks/service-coverage-20260923T073151Z.json` 仍涵蓋 48／37／2、取樣 12.95 秒，另明列 4 個 Windows 排程。當時 `publication-sweep` 2.77 cores、`registered-data-intraday` 2.47、Discord artifact maintenance 1.02；程序樹指出前者正在用 8 workers 重建當日官方標的 Parquet，約 17 GiB RSS。新收盤日輸入發生變化，這次重建不能簡單跳過；若要縮短，須針對官方 panel 的歷史依賴、除權息與生命週期校正做可驗證的增量分區，而非刪除來源或略過正式稽核。此成本仍列為未完成優化。

官方標的建置原本只報整步耗時，無法知道成本在來源合併、symbol 分區、並行逐檔建置，還是 metadata 收尾。已在既有正式 summary 增加 `stage_elapsed_seconds` 四段與總計，保持原有資料／價格運算不變；56 個相關 builder 測試通過。15:30 已啟動的程序載入的是舊版程式碼，**不會**有這些新增欄位；待下次正式建置後，才能用其收據決定哪一段值得改造並驗證收益。

15:38 補跑那輪已載入新版標的 builder，其正式 summary 實測：來源校驗與合併 33.114 秒、symbol 分區 0.314 秒、8 worker 逐標的建置 56.618 秒、metadata 收尾 0.041 秒，寫收據前總計 90.088 秒。瓶頸不是分區或收尾；日後要先處理來源變動判斷及需要重算的 symbol／歷史區間，再比較 CPU 微調。

15:36 同時執行的官方特徵建置約 28 GiB RSS 與 Discord 歷史推論約 42 GiB RSS，WSL 記憶體 `full` pressure 60 秒平均約 8.64%，顯示並行工作已出現可量測的資源爭用。Discord artifact worker 原本只在整輪開頭查官方來源 writer／開盤／互動優先閘門；一輪會連續跑多個大模型，期間開始的官方建置就和下一個模型重疊。現改為**每個**昂貴模型開始前再查一次三個既有閘門，官方 writer 正忙時使用原有有界等待與 `waiting_source` 收據，逾時則明確 `deferred` 而非冒充完成。這不會中斷已在跑的模型，也不會把正式歷史的 1,800 秒推論 timeout 縮短；當前已啟動的 worker 不會立即套用。相關閘門測試 4 個通過，下一輪仍須比較來源發布與整輪 artifact 完成耗時，不能先宣稱總吞吐一定增加。

第三份全服務收據 `artifacts/benchmarks/service-coverage-20260923T074122Z.json` 加入 host PSI：當時官方 sweep 約 12.94 CPU cores（本機 16 cores），記憶體 `full` 60 秒平均 11.35%，I/O `full` 4.83%；這是宿主整體壅塞，不能把百分比全歸給某一個 unit。15:30 的官方 sweep 於 15:38:54 成功結束，systemd 記錄 wall 8 分 54 秒、CPU 累計 47 分 7 秒、35.2 GiB memory peak、7.2 GiB swap peak。它隨即因先前 30 秒排程點而再啟動；兩次來源報表皆為 5 ok／2 failed，且 `changed_dataset_count=0`。第一輪衍生重建收據細分股票 panel 107.7 秒、公開 feature panel 369.6 秒；在來源真正未改變時，重建這些下游不是有效吞吐。

追查發現 TAIEX 官方月資料下載器每次完成 overlap polling，會因 `_downloaded_at_utc` 改變而重寫**價格完全相同**的完整歷史 Parquet 和成功 summary；其 byte SHA 變動使股票 panel／feature 的正式來源依賴校驗失效。修正為：仍照原規則向官方查詢、驗證所有月分並在 `latest_attempt` 記錄此次觀測，但若已驗證的 canonical 收據、起訖範圍及除下載時間外的全部輸出欄位完全相同，就保留 canonical Parquet 和其成功 summary 的原始 bytes；只要任一價格或其他語義欄位變動，就按原流程原子寫入新版本。19 個 TAIEX 下載器測試通過，其中明確驗證「同值刷新不改 canonical／summary」及「收盤指數修正會更新」。目前已在跑的第二輪仍使用舊程式，下一輪才可驗證這項修正對正式下游耗時的實際效果；獨立官方資料的 2 項缺漏並未因此消失。

Windows 啟動鏈目前可唯讀確認 4 項相關排程：`StockAgent Public Caddy` Running，`StockAgent Preserve Crash Dumps` Ready；`StockAgent One-Time WSL Filesystem Repair` 與 `WSL Daily Backup - Ubuntu-26.04` Disabled。Caddy 任務含 Boot／Logon／Time triggers、S4U 登入，Windows 80/443 listener 屬於 Caddy 程序，已安裝 launcher 與 Caddyfile 的 SHA-256 均與 repo 來源一致；WSL `/etc/wsl.conf` 啟用 systemd、gateway `/healthz` 回 HTTP 200。這仍只證明**現在的設定與存活**；未執行會中止行情、交易與回補的 `wsl --shutdown`／實際 Windows 重開機測試，故「無人登入後的冷開機自行恢復耗時／成功率」尚未驗證。上述四個 Windows 排程現在也由測速腳本枚舉，不僅硬編碼 Caddy 一項。

永豐分鐘回補的 14:45 截止只約束新的 **API 查詢連線**。15:09～15:20:54 的高 CPU 實為不登入券商的 `--local-only` 日線重建，2,757 檔中 2,339 complete、127 合約不可用、291 在來源期外、失敗 0；後續混合資料建置到 15:23:54、正式稽核於 15:24:48 `status=ok`，source-gap fallback 保留 68 筆。之後服務為了 89 個歷史 source-gap 等候下個安全連線窗口；程序 `active` 並不表示仍佔用 API 連線。這一輪不能被新程式碼倒算成已提速。

造成 `--local-only` 每日成本的根因是日線 summary 的目標結束日每天變動，既有「同一 manifest SHA」快取無法命中，單檔重讀多年來的 83 個分鐘區塊，再聚合與校驗。已新增逐區塊來源簽章的增量契約：已驗證且未變的前段沿用原日線，從第一個變動區塊開始重新驗證、聚合；前段 source-gap 分類、標的名稱／市場、日線輸出雜湊或區塊身分不相符時，回退完整重建。每檔 summary 記錄重用區塊與完整來源清單，頂層收據記錄增量／全量檔數、重用區塊數及本機 materialization 耗時，供往後逐日比較。首次載入新程式碼的舊 summary 尚無逐區塊身分，必須先完整驗證建立證據；下一個來源日才可能看到增量收益。

在**真實現有分鐘資料**上唯讀抽樣，`2330` 全史校驗＋聚合 0.504 秒／變動尾段 0.024 秒，`0050` 為 0.381／0.020 秒，`6168` 為 0.314／0.025 秒。這證明尾段運算約快 13～21 倍，但不是全 2,757 檔 end-to-end 的倍數；後續約 3 分鐘混合建置與約 54 秒稽核尚未因本次改動縮短。新路徑的三個增量／更正／損毀回退測試，加上既有相鄰模組，合計 43 個 Python 測試通過；下一輪真實全量端到端耗時與輸出逐欄一致仍待排程觀察。

## 全服務持續測速及另一項實測邊界（16:01～16:06）

在既有每 30 秒執行的 `stockagent-data-refresh-status-snapshot.service` 中，加入 `scripts/track_service_runtime_trends.py` 的**每 5 分鐘**取樣閘門，不另建常駐監測程序或 timer。每次取樣對所有已安裝 StockAgent service 記錄最近一次已完成程序耗時、狀態與結束碼；僅對同一個 systemd invocation 計算跨次 CPU／I/O 差，服務重啟、計數器回退、首次取樣則保持 `null`。原子保存一份約 26 KB 的比較基線，長期事件交由既有 journald 輪替，不建立無限長自管檔案。這些都是**程序／資源**指標，不冒充每個 HTTP／模型／交易請求的延遲；逐功能耗時仍要看專屬收據。

正式 worker 首次執行於 16:01:26 辨識到 48 個 service。實跑即抓到 hardened systemd 的 `ProcSubset=pid` 看不到 `/proc/sys/kernel/random/boot_id`，令初版節流每 30 秒重取樣；已依單調時鐘回退與同一 `InvocationID` 約束修正，加入「無 boot ID 的沙盒仍限速」回歸測試。16:05:55 正式日誌的 `service_trend` 階段僅 1.905 ms、未產生第二次取樣，對照修正前取樣輪約 190～266 ms；下一個有效 5 分鐘跨次 CPU／I/O 差須待 16:10 左右的正式 worker 才能驗證。與監測、資料庫存及資料面板相鄰的 76 個測試已通過，Ruff 與 diff whitespace 檢查通過。

16:10:59 已收到下一筆正式樣本，間隔 329.409 秒、48 個服務全數列出，查詢本身 140.819 ms；非取樣輪附加階段約 0.7～1.9 ms。這也抓到初版對**閒置 oneshot** 保留的 `InvocationID` 計算出 `0.0 cores`，容易誤導成工作本身零成本；已改為只有連續執行中的服務才計算跨次 CPU／I/O，閒置工作只展示其最近一次程序耗時。此語意修正後相鄰測試為 77 個通過；16:10 的舊樣本不應拿其 idle CPU `0.0` 作績效判斷，下輪才會顯示 `null`。

16:16:01 正式再次取樣，間隔 302.814 秒、仍為 48 個服務。`stockagent-artifact-dedup.service` 與剛完成的 `registered-data-intraday` 已如契約顯示 CPU 差 `null`、並保留最近一次工作 wall；持續運行的 `public-dashboards` 則記錄約 0.0038 cores。這驗證節流與「閒置非零成本」邊界，而非證明所有 48 項業務操作均已量到 p95 或達最優。

## 下一個開盤瓶頸的精確追蹤

9/23 的第一個當沖模式在 09:00:02.676 訊號 ready，券商本機 callback 第一筆於 09:00:01.310、全必需標的覆蓋於 09:00:02.163；永豐共享報價請求發於 09:00:00.236，broker 約 09:00:01.067 才開始處理，隊列 830.794 ms，provider 擷取 1,192.081 ms。最晚第五模式才在 09:00:06.673 ready，主因是五模式順序執行；模型推論本身單模式約 31～63 ms。此處**不**把本機 callback 視為交易所撮合時間，也不以回放訊號冒充 1 秒即時達標。

共享報價服務設在當沖模擬引擎 loop 中；原有資料只量到「提交至 broker 開始」的 831 ms，無法分辨是同一圈的模式重載、交易日閘門、readiness 更新，還是上一圈的交易帳本工作佔用。已在原有 `shared_quote_request` 稀疏日誌加上上一圈工作、當圈 broker 前置、模式重載、交易日驗證及 readiness 四段毫秒值，只有真的處理請求才輸出，不增加每圈持久檔案或外部 API 查詢。165 個當沖與開盤 pipeline 測試通過；**目前持續執行中的引擎仍載入舊碼，未為純測速而重啟交易引擎**，所以下次正常重新載入後的實際開盤才有這些欄位。下一步先以量測證明 831 ms 等待的來源，再決定是否安全地調整 loop 內優先次序；今天不能從數據推定它全是某一函式造成。

16:30 之後的 5 分鐘全服務樣本中，常駐 `tw-public-source-events` 約 0.415 cores，是當時最大的常駐 CPU 來源；後面是永豐 TAIFEX bid/ask 0.075、Discord bot 0.047、當沖模擬引擎 0.043。前者收據實際為 `degraded`：159 個註冊／159 個已探測來源中，`tpex_daily_valuation` 有一筆尚未套用，官方已驗證開市日 9/23 的 TPEx valuation 回應無列；16 次下載重試仍不能補出官方資料。監測器每 60 秒探測同一來源表示、失敗重試會連帶驗證既有數百萬列歷史。這不是監測進程崩潰，也不能用「服務 active」改成資料健康。雖可拉長重試來省 CPU，但會延後來源探測故障時的補抓；本輪沒有以犧牲發布即時性或忽略缺列的方式製造提速。下一階段若要縮短此路徑，需保留 60 秒來源版本偵測並驗證日期清冊重用或有界負面快取的因果正確性。

目前合併相鄰回歸測試為 470 個 Python 測試通過；不是全庫 5,404 個測試已執行。TAIEX `twse_taiex_ohlc.parquet` 的現有成功摘要在 16:43 唯讀驗證 SHA 相符、1999-01-05～2026-09-23 共 6,870 列，因此下一輪有資格走語意不變路徑；是否真的命中須以實際官方回應與正式 `latest_attempt` 證實，不能提前當成測速成果。

進一步查 9/23 的 `tw_public_stock_daily.summary.json`，960 萬列、約 1.33 GB 的公開特徵表實際 `build_mode=full`、`reused_rows=0`；現有 `--public-feature-incremental-days=14` **不是**來源變動時就能沿用歷史前綴。其嚴格契約要求全部來源與既有輸出位元收據相同，來源新日資料或任何歷史修訂使之失效；這是避免歷史修訂／公告時點改動被舊前綴掩蓋的正確 fail-closed 行為。已把 `base_contract_not_verified` 退回原因與來源證明、逐股特徵、全市場特徵、組裝、Parquet 寫入與再驗證的分段耗時寫入**下一輪正式建置**的摘要；未改資料值或放寬證明。155 個公開特徵／盤後發布相關測試通過，含「來源變動必須全量」及新增的收據欄位測試。真要在新交易日將約 335 秒縮短，需先有能證明前綴未變的來源分片／日期指紋，再容許尾段計算；現有旗標本身不構成該證明。

完全同值的 `unchanged_verified` 路徑現在也會在 CLI 日誌顯示來源／輸出證明耗時，**不改寫**成功摘要；下輪若因 TAIEX 同值輪詢而跳過公開特徵表，就能區分「校驗 1.33 GB 輸出」與「重新計算 960 萬列」的真實成本。上述所有受影響模組合併為 557 個 Python 測試通過，Ruff 與 whitespace 檢查通過；仍未執行全庫 5,404 測試。

對 55,765 個 Parquet 檔的正式資料監測快照進一步 profiling：`inventory` 約 1～3 秒，`public_projection` 約 0.7～1.5 秒，若有來源變動則 6,314 項 feature 投影約 0.4～0.6 秒。一次直接執行腳本的 profile 漏設正式 wrapper 的冷 inventory 持久快取，額外讀取約 16 萬行壓縮 inventory 2 秒；**不能**用該錯誤 profile 宣稱正式服務的瓶頸。帶正式環境變數的 profile 確認主要成本是 55,765 檔發現／校驗及其他實際狀態投影，必須維持變動偵測與壞檔呈現；本輪沒有把刷新週期拉長、少看檔案或沿用可能已變的 footer 來製造表面提速。

15:38:54 第二次官方 sweep 已在 15:46:41 `Result=success`，但全資料 `waiting_publication` 仍含 TWSE、TPEx 兩項 valuation 上游缺漏；其 467.43 秒 wall、約 2,607 秒 CPU 與 71.4 GB cgroup 記憶體尖峰是舊 TAIEX 程式碼的結果，不能拿來評價新 semantic-no-op 修正。下次 17:30 正式執行及新摘要才是驗收點。16:04 `systemctl --failed` 為空、gateway `/healthz` 回 `ok`，但這不是全資料健康、冷重啟或外網雙棧已驗證。

## 17:30 正式排程與公告封存故障

17:30 的正式 `close_final` 先查詢並完整驗證 6,870 列 TAIEX，日誌確認 `semantic_noop=True`，原 canonical 與成功摘要沒有因觀測時間刷新而改寫。該輪仍因其他來源收據改變而需要重建 9,604,259 列公開特徵；正式摘要的分段耗時為來源證明 0.706 秒、逐股特徵 235.221 秒、市場特徵 1.975 秒、輸出組裝 6.699 秒、寫入／校驗 10.612 秒，寫摘要前總計 255.214 秒。完整 sweep 約 5 分 19 秒、32 分 41 秒 CPU、56.8 GiB cgroup 記憶體尖峰。17:35 因密集發布探測點再執行的一輪已重用完成的衍生層、沒有重建特徵，約 47.6 秒；仍取得官方資料，不能稱為零成本。`tpex_daily_valuation` 當日官方回應無列，來源狀態仍是 `waiting_publication`，不能把 `Result=success` 解讀成全部資料完成。

16:30 的宏觀公告封存曾在央行外匯存底第 164 頁遇到單次 HTTP 轉址，整個服務 fail closed。下載器現在對原始官方 URL 有界重試，**不跟隨**未知站點／錯誤頁；持續轉址仍報錯。隔離測試驗證暫時轉址恢復及持續轉址拒絕。修正後的人工正式執行已讓央行外匯存底 317/317 筆、710 個原始檔通過完整性稽核，證明這次 CBC 階段可完成；但服務接著在公開特徵對帳因另一問題失敗，不能宣稱整個 service 恢復。該問題是 9/23 收盤衍生層已被正式 close receipt 接受，舊的全資料下載摘要只到 9/22；TPEx 估值仍待發布，原對帳器卻只接受所有來源完整的 close receipt，故想用 9/22 重建 9/23 特徵並正確被防倒退閘門拒絕。

對帳器現改與完成交易日 finalizer 使用相同的必要條件：TWSE／TPEx 官方收盤兩源俱在、同一 live root、非阻斷失敗為零，才允許保留較新的收盤日期與 `allow_daily_publication_lag` 契約；原來源缺漏仍顯示待發布，沒有補造估值。若當日收盤證據不成立，仍拒絕倒退。對帳及公告封存相關 23 個測試通過；實際 root 的唯讀 dry run 已不再倒退，回報 `end_date=2026-09-23`、`allow_daily_publication_lag=true`。由於期間另有 10 個真實來源位元收據改變，仍須一次完整特徵重建，不能只憑 dry run 宣稱服務已恢復。

18:15 人工重新啟動同一正式 service 後，宏觀公告封存、317/317 筆央行外匯存底原始發表、9/23 官方特徵對帳、MOF 原始公告、研究表與 TAIFEX 附加研究表均跑到終點；systemd `Result=success`，wall 9 分 47 秒、CPU 48 分 57 秒、記憶體尖峰 48 GiB。公開特徵仍因十個來源收據確實改變而全量重建，這輪 `stock_build=289.639s`、總計 309.389 秒；沒有冒充尾段增量，也沒有把缺漏的 TPEx 估值寫成已完成。`cbc_money_release_vintages` 的 2000-06 原始值仍缺，稽核明示 `value_history_complete=false`，雖然此次服務成功但歷史數值覆蓋沒有變成完整。

逐股特徵建置本來只量整段 235～290 秒。已在下一次程式載入的摘要加上 16 個逐股及 11 個市場 feature builder 的獨立耗時，未改來源或欄位計算。另對已經位於驗證交易日的巨大 DataFrame 加入線性日期集合驗證，命中才略過重複排序／as-of join；任何假日或 null 日期仍用原映射。直接以正式 9,604,259 列特徵表的三欄做唯讀比對，快路徑 0.034 秒、舊映射 0.431 秒；排序後逐列相等、沒有遺失 key。這只證明此映射步驟約省 0.4 秒，**不足以解釋或解決 289 秒的主要瓶頸**。未以此小局部增益宣稱完整建置已大幅提速；要用下一次逐 builder 收據與實際下游合併測速判斷大宗成本。

18:48 再掃 48／37／2 個 unit，12.4 秒樣本沒有 systemd failed；常駐 CPU 最高為當沖模擬約 0.043 cores、Discord 0.039、TAIFEX bid/ask 0.033、來源事件 0.023。公網 IPv4 HTTPS 與本機 `/healthz` 成功，DDNS IPv6 HTTPS 仍無法連線。全資料摘要仍 `critical`：326 個啟用端點為 225 complete、35 catching up、65 unable；網站通並不代表資料可用。

最近一次當沖資格服務表面耗時 315.5 秒，但 9/23 09:15:06 已觀察兩個官方來源且本機當日資格最終可重用，直到 09:20:14 才完成；整輪 CPU 只有 5.5 秒。現有流程會在快取穩定驗證未命中時等候全域 TW-public writer lock，這是可能的長等待，但此筆日誌缺少鎖等待分段，不能把五分鐘全歸因給鎖。已在既有成功收據增 `probe_stage_elapsed_ms` 與 `coverage_stage_elapsed_ms`（穩定檢查、writer-lock 等待、持鎖檢查、需要時的官方下載、總計），只在一次完成時寫入，不另添每 2 秒日誌；原 fail-closed／鎖語意未改。下一次遇到同型長耗時即可判明來源、鎖或本地校驗哪段佔用，避免以縮短驗證或繞過寫入鎖冒充提速。

公開特徵整段逐股建置 289 秒仍未解釋清楚，因此在同一份下次建置收據再細分「逐來源 builder」、「交易日映射」、「稀疏特徵逐次 join」三層耗時；每個 join 仍使用原有全外連接與原有欄位衝突語意，沒有在欠缺逐列等價證明前改寫成另一個合併算法。81 個公開特徵測試通過；這一版計時程式還未經真實下一次全量執行，不把它當性能改善成果。

## 19:30～20:03 公開特徵完整來源路徑

細分收據證明 9,604,259 列全量建置的主要重複工作在巨大逐股輸入的 `_finalize_feature_frame`，而不是市場列：19:30 正式公告封存建置耗時約 267 秒，其中單一逐股 join 前的準備約 59 秒。每個來源在 join 前已統一鍵／值型別並去除重複鍵；唯一鍵關聯的 full join 結果仍唯一，因此移除結果上第二次逐欄 group-by。對至少 100,000 列的輸入，先線性檢查 `(date,symbol)` 是否有重複；只有確認沒有重複才免除原有 group-by。實際官方 OHLCV 的 9,746,178 列沒有重複鍵，這段準備由約 59 秒降至隔離全量測試的 1.379 秒。保留有重複鍵的原邏輯，並以重複鍵及欄位衝突的回歸測試比較舊／新輸出。

同一次隔離全量測試從真實 170 個來源收據重建 9,604,259 列、143 個模型特徵，寫入約 1.33 GB Parquet，`total_before_summary=173.727s`、`stock_build=145.221s`、`stock_merge=53.008s`、`parquet_write_and_proof=18.446s`；較先前同型隔離全量測試的 284.814 秒快約 39%，但兩次機器爭用與來源版本不是完全相同。輸出 SHA-256 與目前正式完整表一致；但有三份法人／當沖資格來源位元收據在正式表建置後變更，**相同輸出雜湊不等於來源收據已更新**，因此正式對帳 dry run 仍正確回報 `rebuild=true`。下一步以正式服務取得新來源收據與其實際 wall／CPU 證據。

若唯一變更嚴格限於五份只進入市場列的 TAIFEX 來源，且既有表、特徵 ABI、股票 universe、交易日終點與其餘全部來源收據皆通過位元驗證，新增 `market_only_rebuild` 僅重建市場列、重用舊股票列；任一證據不足即退回完整重建。隔離實表測試在 9,597,389 股票列＋6,870 市場列的樣本約 20.724 秒，對當時完整來源輸出做 9,604,259 列／162 欄逐值相等測試通過。這只縮短符合精確五來源條件的情況，不是任意官方來源更新均可走快路徑。

此輪修改後相關公開特徵、公告、當沖資格、資料對帳及研究表測試分兩批 106＋300 個通過，Ruff 與 `git diff --check` 通過。較廣測試最初被乾淨工作樹中過期的 `audit_cbc_release_vintage_contract` 測試匯入擋住；已將該測試改為現行 `audit_release_vintage_contract`，核對 CBC 公告版本仍 fail closed，然後 300 個測試完整通過。仍非全庫測試，也不代表全部 48 個服務延遲達下界。

20:03 實際啟動既有 `stockagent-tw-public-feature-reconcile.service` 驗收新版：全量 9,604,259 列核心特徵在 214.994 秒完成，後續暫定宏觀、研究表、TAIFEX 研究表及 all-observed 研究表均完成或通過既有相同來源重用閘門；整個正式 unit `Result=success`、wall 246.100 秒、CPU 1,637.236 秒、記憶體尖峰 48 GiB。這比 19:00 另一輪正式核心全量 365.066 秒短約 41%，但負載不同，不能當固定加速率或改善其它服務的證明。正式輸出 SHA-256 `bbe6532ee0ec6aca3ab515ebd6dce82899175ebf65b92d953513a1e1c7238ec5` 與隔離全量輸出一致；正式來源收據已刷新。後續唯讀 dry run 回 `rebuild=false`；獨立 `audit_feature_build_receipt` 六個檢查（schema、availability、來源位元、symbol universe、輸出位元）全部為真、無 finding。

20:15 重掃全服務，枚舉已從 48 service／37 timer 增加為 **49 service／38 timer／2 path**；新的是帳號授權範圍的 `stockagent-finlab-local-refresh` 及其 timer，當時工作正在跑，還沒有完成 wall／資料品質結果。收據是 `artifacts/benchmarks/service-coverage-20260923T1215Z.json`；這說明清單必須每輪自動重新發現，不能把早上的 48 項視為永遠的全部。當次 `systemd` 沒有失敗 unit，但 FinLab、OpenBB 壓縮與外部研究 backtest 同時存在，宿主 memory `full` PSI 的 10 秒平均約 48.85%；正在執行不等於其輸出已通過稽核。沒有為量測或優化而中斷這些其他工作。

## TAIFEX options 原始檔解析的下一個熱點

時鐘／可用性邊界：這裡讀取的是官方**已完成日盤**的交易日期、TX 當日開盤參考與 TXO 每契約開盤、收盤、成交量；ATM／full-chain 為歷史資料投影，既有輸出才決定後續研究可用性。它不把全日成交量挪到開盤下單、不把日線價格稱為 bid/ask 成交；缺少或相衝的官方原始列仍拒絕建置。任何解析優化只能保持每個來源 ZIP／CSV、交易時段、合約月份、買賣權、價格／量與 provenance 的逐列語意，並由來源及輸出收據驗證。

17:00 正式服務的既有分段日誌顯示：官方收據／下載 63.320 秒、monthly ATM 233.043、monthly full chain 218.155、weekly ATM 148.168、weekly full chain 146.737，options 腳本總計 809.982 秒；整個 unit 約 821.951 秒。四個 builder 都重讀官方 CSV，這是結構性重複；目前沒有取得跨年分片的逐值等價證據，不直接改寫 ATM／full-chain ABI 或省略歷史資料。

先在一份真實官方 2025 年 ZIP（18 MB、月契約解析後 243 交易日／223,298 筆）做單一步驟 profile：`csv.DictReader` 與日期解析都是熱點。試改整個 CSV 讀法只使無 profiler 的單次讀取從 11.534 秒變為 11.220 秒，增益小且增加欄位／短列相容性複雜度，已撤回；真正保留的是每次讀檔內 512 筆有界交易日期解析快取，來源日期欄原值不變，無 profiler 的單次讀取為 8.779 秒。這是單樣本約 24% 的解析改善、非正式每日服務 wall 的聲明。資料來源、成交價、可交易欄位及重複列檢查未改，options、下載器、衍生 tick 與舊 TX 策略共 53 個回歸測試通過。下次正式 options 重建仍要驗證輸出收據／前後雜湊與實際分段耗時；此更改的 builder fingerprint 會正確讓既有舊版快取失效。

另在隔離目錄用同一 2025 年官方 ZIP 和正式 TX 日盤期貨來源重新建置四份結果，逐欄／逐列比對正式完整資料的 2025 年切片均相等：monthly ATM 243 列、monthly full chain 223,298 列、weekly ATM 243 列、weekly full chain 107,524 列。這證明該年四個投影在這次解析微調後仍與舊版正式值一致；不是 2001～2026 每個年度均已重建驗收。21:00 左右 FinLab、本機 OpenBB 壓縮與註冊盤中資料仍在執行，沒有為量測強行增加一次完整 13 分鐘正式重建；下一次正常 TAIFEX timer 的分段 wall 與 hash 仍待觀察。

## Yahoo US 每日服務的外部節拍下界

9/23 06:30 正式 `registered-daily` 的 US 股票步驟實際要處理 12,221 個 repair task，20 多分鐘進度持續約每秒 9.5～10.2 項；完整 wall 1,294 秒。當次日誌明示目前本機 Yahoo chart 請求節拍為 0.100 秒／項（10 req/s，屬本機安全策略，**不是 Yahoo 官方保證配額**）。即使所有其它運算零成本，12,221 次請求在此策略下也至少約 1,222 秒；觀測值比這個條件下界多約 72 秒／5.9%。因此單純增加 16 個 worker、微調本地 Parquet 或省幾毫秒 UI 不會消除 21 分鐘；真正縮短得減少有充分 terminal／未變證據的請求，或另行量測較高外部節拍的失敗率與限制，不能擅自略過退市歷史。該輪仍有 13 `failed`，所以更高頻率絕非無風險「最佳化」。

該服務原本把 US 與 forex 分兩次程序跑，兩次都覆寫同一 `daily_update_summary.json`，最終只剩 forex；這是測速與錯誤追蹤的證據缺口，不是 US 沒有下載。現在保留舊合併摘要的相容路徑，同時原子寫出 `daily_update_summary.us_stocks.json`／`.forex.json`，各自含 mode、end date、狀態數量、完成時刻與實際該資產步驟耗時；不碰請求節拍、符號集合、下載或 Parquet 語意。兩次獨立呼叫仍保留前一資產收據的測試通過，整個 Yahoo state 測試 61 個通過。正式下一輪下載尚未執行，故持久收據的實際效益待下一次 timer 驗證。

21:45 左右的唯讀服務覆核：`systemctl --failed` 空；IPv4 公網 HTTPS `/healthz` 為 200、單次 34.844 ms，IPv6 同端點約 228 ms 即連線失敗，未解雙棧。新加入的 FinLab 服務 20:14 開始、21:10 收到 TERM 停止，最後 journal 顯示 55 分 40 秒 wall、8.9 GiB 記憶體尖峰；停止前的同步收據仍為 `partial`，例如 1,109 catalog keys 中只下載 82 個、約 1,027 待下載。後來 systemd 的 `Result=success` 是停止後的 unit 狀態，**不是資料完成證據**。未擅自重啟這個與本輪同時加入、配額及停止原因未明的服務。

## 22:24 後：OKX 歷史特徵的可量測下界與分段收據

新掃描 `artifacts/benchmarks/service-coverage-20260923T1424Z.json` 發現 `stockagent-finlab-quota-snapshot.service/.timer`，所以當時清單是 **50 service／39 timer／2 path**；`systemctl --failed` 仍空。14:00 的 `registered-data-features` 最近一次 wall 約 1,548 秒；並行子步驟是 OKX 1,547 秒、Binance 1,467 秒、Bybit 52 秒，故瓶頸確實在前兩者，不是 Bybit 或 shell 排程。

OKX 這輪 486 個合約、每合約 8 個特徵階段，共 3,888 個進度項。官方 [OKX history-index-candles 文件](https://app.okx.com/docs-v5/en/)列明每頁最多 100 根 1 分鐘 K 線、每 IP 每 2 秒最多 10 請求；現有客戶端維持相同 5 req/s 節拍。若所有 486 個合約都需補整整 24 小時的 1,440 根 index K 線，光此端點約需 `486 × ceil(1440/100) / 5 ≈ 1,458` 秒，接近實際 1,547 秒。這是**條件下界與瓶頸推論**，不是已證實本次每個合約實際都請求 15 頁；須以下輪真實授權數驗證，不能只因 wall 接近就宣稱已達上限。

在既有 OKX `historical_feature_report.csv` 增加每合約的八階段、讀檔、衍生、比較、寫入、覆蓋稽核與總耗時；成功與各階段失敗都保留時間。`download_summary.json` 新增每階段樣本數、P50/P95 與**並行 worker 秒數總和（非服務 wall）**；另外記錄現有跨程序 limiter 的**本程序**各端點 grant 數與 limiter 數，明列 funding 為每合約獨立 limiter，避免把它的總 grant 數錯當共享 5 req/s 下界。未改價格、時間戳、缺漏處理、API 頻率或資料檔語意；10 個相鄰測試通過。

隔離複製正式 BTC-USDT-SWAP hot tail，使用實際 OKX 公開端點跑單合約，沒有寫回正式資料：4,869 列、八階段皆 `ok`、總 5.413 秒，其中 mark 1.834、index 1.680、open interest 0.663、funding 0.337 秒，本地讀／衍生／比較／寫入／覆蓋合計遠小於外部階段。這只是一個 8 小時左右缺口的樣本，不能外推成所有合約或完整每日服務的 P95；下次正式 14:00 收據才足以檢查實際 grants 與階段分布。

## Binance 歷史特徵同源測速與收據保留

Binance 9/23 14:00 的 574 合約歷史特徵並行子步驟耗時 1,467 秒。官方 [USDⓈ-M Futures Market Data 文件](https://developers.binance.info/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)對 `futures/data` 統計資料列出 IP 1,000 requests／5 minutes；現有本機節拍為約 3.33 requests／秒，並未為了縮短 wall 擅自放寬。隔離複製正式 BTCUSDT hot tail、使用公開端點補一段約 8 小時的資料後，十個特徵階段全數成功、5,002 列、總耗時 4.239 秒。此單一合約發出本程序 15 個 grant，其中 `futures/data` 8 個、funding 1 個；本地讀檔 0.424 秒、衍生 0.013、比較小於 0.001、寫入 0.016、覆蓋檢查 0.042 秒。8 次統計端點請求乘 574 合約再除以 3.33 req/s 約 1,378 秒，與 1,467 秒相近，**但只是用單樣本推算的條件下界**，尚非實際全體 request 數或平均；全量下一輪的 grant 收據才可驗證。

已把 Binance 每合約各階段（含失敗）、讀檔、衍生、比較、寫入、覆蓋驗證與總耗時加入現有 `historical_feature_report.csv`，在 `download_summary.json` 記每階段樣本數／P50／P95／並行 worker 秒數，以及本程序統計與 funding 端點授權數。OKX 與 Binance 使用同一個有界統計器；額外將歷史特徵與僅 K 線更新的摘要各寫成 `download_summary.historical_features.json`、`download_summary.candles_only.json`，因為原本共用的 `download_summary.json` 會在稍後的盤中 K 線刷新覆寫 14:00 特徵證據。相容的舊路徑仍維持；新檔只在下一次正式執行產生。兩個交易所相鄰的 30 個測試及 Ruff／whitespace 檢查通過；沒有變更 API 節拍、下載資料粒度或特徵值。

22:56 的新唯讀全服務清單收據 `artifacts/benchmarks/service-coverage-20260923T145652Z.json` 仍為 **50 service／39 timer／2 path**。以上只是完成一項重點服務的瓶頸歸因與測速持久化；其餘服務、真實正式次日耗時及是否接近可達下界，仍列為持續工作，不宣稱「所有服務已到極限」。

## 22:30 Crypto training refresh 與一分鐘原始寫入器競爭

22:56 再查 `systemctl --failed` 發現 `stockagent-crypto-training-refresh.service` 失敗。原始日誌明確顯示它在 22:30:00 啟動、22:30:23 funding 階段完成，22:30:24 便遇到同時起動的 OKX／Bybit／Binance 一分鐘原始下載器；程式當下丟 `raw writer started during refresh`，將整輪標為 failed。過去 9/22 及 9/23 的 02:30 批次也曾在衍生建置或發布邊界遇到相同 writer 競爭。這不是 Discord、當沖引擎或網站掛掉；但它確實使 Bybit 訓練資料刷新／冷發布沒有完成，不能把 defer 說成成功。

已保留原本的「writer 忙時不讀／不發布」失敗封閉規則，把**執行中途**及發布前發現 writer 的情況改寫成含活躍程序、已完成步驟與結束時間的 `deferred_raw_writer` 收據，不再把預期讓路誤報為業務程式失敗。發生於實際發布動作內、不能證明僅由 writer 造成的錯誤仍維持 failed，不掩蓋其它發行問題。新增中途碰撞測試與相鄰 34 個測試通過；22:30 已失敗的 process 不會因修程式自動變成完成。**根治排程上的持續競爭**仍需要可驗證的來源快照／鎖協調或保留明確寫入空窗；目前 1 分鐘原始資料更新週期短於約 2～3 分鐘的衍生建置，單純改成功色或重試頻率不能保證完成，也不應無聲暫停來源蒐集。

23:05 的正式服務日誌更準確量到相鄰原始刷新每輪約 57～60 秒、結束後約 60 秒再次啟動，因此沒有可讓約 145 秒的 Bybit 日表建置獨占執行的既有空窗。唯讀實測真實 BTCUSDT 3,412,383 筆 base 分鐘列加 5,010 筆 hot-tail：全史 `_daily_bars` 3.076 秒，後續日表 funding join 0.077 秒；主要負擔在重讀多年分鐘資料、排序／合併／聚合，非每日 2,373 列的 funding 算式。這是單一高量標的，不外推成 394 標的全輪耗時。下一個有效性能改造應是帶來源版本與 32 日 lookback 證明的分鐘尾段增量建置；發布仍另需隔離持續變動的 `data_bybit` 原始來源，不能直接鬆開 writer gate。

為避免 writer 在日表建置期間完成並退出、使單靠程序掃描漏掉混合版本，已加入一個僅用檔案身分／大小／mtime 的低成本 Bybit 輸入邊界檢查：日表建置前與每一後續步驟／發布前重看 1m base、hot-tail、funding 及兩份來源清冊；改變時保留 `deferred_source_changed`、前後摘要與已完成步驟，停止發布。實際 2,143 個輸入檔一次 metadata 快照約 146.5 ms；它僅防一般原子替換造成的競爭，**不是**防同 metadata 惡意改寫或來源位元完整性證明，最終發行仍必須通過原有 SHA-256 稽核。三個針對中途 writer、原子替換及建置期間來源變動的測試通過；正式 02:30 執行尚未載入驗收。

## Bybit 日表尾段增量的正式接線與隔離驗收

`materialize_bybit_perpetual_daily.py` 現在能利用上一版輸出旁的 materialization 證明，僅重讀 hot-tail 最早受影響日期前 33 天的分鐘資料，再把已驗證的舊前綴與新尾段合併；funding 仍對**完整**日表重新計算。先驗證 contract、輸出 SHA-256、base 的 device/inode/size/mtime、來源 canonical 時間字串與 hot-tail 的日期統計，證據不足即走全量。來源在建置或 Parquet 寫入期間變動時，原子替換前先拒絕；發布前後的來源檢查仍保留。`materialize_report.csv` 與 summary 新增 `full`／`incremental`／`skipped_up_to_date` 方式計數，以免把快取命中冒充增量計算。

隔離測試只讀正式 BTCUSDT 的 3,412,383 筆 base 分鐘列與 5,010 筆 hot-tail，輸出寫在 `/tmp`：新版全量一次 wall 3.53 秒，修改隔離 funding 檔 mtime 後的增量一次 wall 0.90 秒；兩者皆 2,373 日列、2,372 個可執行報酬列，Parquet SHA-256 均為 `2374960d1bd2f6257a0802af6cd7d5d34299b850778d665d482dd44d174b83d3`。這是單標的、無真實 funding 值變化的同值重算測試，**不是** 394 標的正式全輪、來源發布或更新日跨日修訂的完成證據。0／5 分鐘執行契約、尾端修訂、損毀證據回退與替換前拒絕均有測試；擴至 hot-tail、來源範圍、crypto inventory 的相鄰 81 個 Python 測試通過。日表建置也會在載入後和每標的原子替換前後驗證 funding coverage 與 instruments 收據身分，以免來源清冊在批次中途改變而被混合使用。

剩餘主問題仍是原始一分鐘寫入每輪約 57～60 秒、間隔約 60 秒，而全部衍生與冷發布需要更久；增量日表只縮短其中一段，不能單靠這項修正保證 crypto training refresh 完成。不得為讓它看起來成功而放寬 writer gate、跳過完整來源收據或降低原始資料刷新頻率。22:30 的 service 仍顯示 failed，直到下次真正的正式刷新收據驗收前，健康狀態不得改成完成。

## 9/24 00:00 官方當沖資格監測的空窗修正

9/23 22:29 啟動的資格監測器並非掛起：截至 23:57 的 `latest.json` 每 30 秒更新，累積 2,539 次輪詢，官方 TWSE 主檔仍宣告 9/23，未達服務要求的 9/24，有據地維持 `waiting_source`。原本 90 分鐘期限在 23:59:58 到達，正式收據為 `publication_pending_timeout`；舊 unit 的 `RestartSec=5min` 令發布恰好落在期限後時有最長近五分鐘探測空窗。這不是可以用 9/23 舊資料充當 9/24 解決的問題。

已把**僅此** service 的重試間隔改為 30 秒，並用 `StartLimitIntervalSec=10min`／`StartLimitBurst=3` 防止初始化立即失敗時的無限快速重啟；既有輪詢 2 秒、90 分鐘有界等待及官方兩源與精確日期驗證不變。已對本機單一已安裝 unit 驗證並 `daemon-reload`，00:03:34 自動重新進入 `start`，新 `latest.json` 再次顯示 `waiting_source`；並未重啟當沖引擎。75 個官方發布／資格監測相關測試通過。這把**下一次等待期限後**的重試設定空窗縮短，未證明官方何時發布，也未宣稱 9/24 資格已取得。

9/24 00:04 開始的正式一分鐘原始資料輪次可分段核對：Bybit 867 個標的約 36.7 秒、Binance 574 個約 30.1 秒、OKX 491 個約 59.8 秒，三組並行，整輪 wall 約 61.0 秒。OKX 收據有 507 個 `history-candles` grant，現有本機端點節拍 0.1 秒／次；即便本地處理零成本，此設定下約需 50.7 秒。因此本輪 OKX 已接近其**目前節拍條件下**的下界，未經官方限制與失敗率驗證不能把 507 次請求全部無限制併發。timer 是上一輪完成後 1 分鐘再啟動，故實際 start-to-start 約 121 秒；「1m 資料粒度」不等於「每 60 秒完成全市場一次」。若要讓全量衍生與冷發布在此節奏下完成，仍需可證明一致的來源快照或協調機制，而非把必要 491 個標的請求刪掉。

Bybit 日表現也在正式 `materialize_report.csv` 增加每標的 `daily_bars`、`funding_join`、`write_and_proof`、`total` 秒數，summary 用同一套有界最近秩 P50／P95 計法，明示聚合 worker 秒數不是整輪 wall。隔離 BTCUSDT 再跑一次增量，分段為 0.443／0.070／0.012 秒、總 0.525 秒，輸出 SHA-256 仍與前述全量一致；這次重新量測的 105 個 crypto 相鄰 Python 測試通過。分段收據將在**下一次正式 394 標的工作**顯示分布，不能由單一 BTCUSDT 推估全體 P95。

為驗證全體分布，在 `/var/tmp/stockagent-bybit-fullbench-nfo4AbC7` 建立只讀原始來源的硬連結快照，輸出寫到**獨立**日表目錄，8 workers、低 CPU/I/O 優先權，不寫正式訓練資料。固定快照的首次全量為 394／394 成功、wall **189.87 秒**；逐標的 `daily_bars` P50／P95 是 3.194／7.634 秒。只把隔離**輸出**的 mtime 調舊、強制每標的走已驗證增量後，394／394 再次成功、wall **39.48 秒**；`daily_bars` P50／P95 為 0.367／0.603 秒，funding join 0.242／0.450 秒，輸出／證明 0.119／0.350 秒。兩輪均 405,819 日列、405,425 可執行報酬列；對同一 394 份 Parquet 排序逐檔 SHA-256 後的總指紋均為 `4c8d9786a6d047e3cc272fe4ce91dfc1de94de7291c3038efb9ec0a819d0679c`。全量對增量約 4.8 倍是**同一隔離快照、同一主機兩輪**的結果，不等於正式 02:30 service wall 或下一來源日的固定提速率。

這個結果推翻「日表本身一定放不進約 60 秒原始下載空窗」的假設，但**不**解決整條工作：正式 funding 下載、Bybit venue 日特徵、coverage 稽核、各交易所報表與 catalog-backed 冷發布仍需另量並持續保留 writer／來源變動 fail-closed。正式輸出的首次舊版 sidecar 缺失也可能使第一次部署仍需 190 秒左右全量，不能拿已預熱隔離快照冒充正式第一次成功。

另以各自的暫存輸出唯讀量下游：Bybit venue 特徵 2.48 秒，歷史覆蓋稽核 5.36 秒（17 資料集／5 finding，約 5.8 GiB 峰值 RSS），Bybit 訓練報表 4.74 秒，OKX 與 Binance 一分鐘來源報表約 2.68／2.82 秒。隔離 venue 特徵 Parquet 與品質 CSV 的 SHA-256 均和當時正式輸出一致；報表的 `completeness_verified=false` 仍維持原資料健康語義。這些單步在不同時間執行，不能直接把秒數相加當同輪服務 wall。

22:30 正式 funding 步驟曾耗約 23 秒，但原程式沒有分段證據。現於 `funding_coverage.csv` 加每標的 `same_day_proof` 或 `fetch_and_mark`／`merge_write_proof`，於 `funding_summary.json` 加 universe discovery 耗時、逐段 P50／P95 與寫摘要前總耗時。隔離複製當日 397 個既有 funding 檔，再用同一官方合約清冊跑 394 個標的：394／394 均 `skipped_current_snapshot`，instrument discovery 0.241 秒、每標的同日證明 P50 0.006 秒／P95 0.010 秒、整個程序 wall 1.58 秒。這說明 22:30 的 23 秒**不能**歸因為固定 394 檔本地證明成本；可能是當時外部回應或宿主負載，但尚無該輪分段證據，不定論。下一次正式服務載入新版收據後才能歸因。funding 與相鄰 crypto 模組的 70 個測試通過。

最後合併 crypto 來源／日表／歷史特徵、官方發布與當沖資格的 **180 個 Python 測試通過**，Ruff 及 `git diff --check` 通過，未把這 180 個稱為全庫測試。9/24 00:23 的本機 `systemctl --failed` 仍僅有前次 `crypto-training-refresh`，它尚未收到新版正式 02:30 輪驗收；資格監測仍是官方主檔宣告 9/23 的 `waiting_source`。公網 IPv4 `/healthz` HTTP 200，IPv6 連線失敗；全資料公開摘要 `health=critical`，327 個啟用端點為 225 complete、36 catching up、65 unable、1 streaming。這些是不同健康域，均未因隔離提速而改成綠燈。

## 9/24 00:34 FinLab 單鍵無界等待

正式 FinLab 本機刷新上次在 `broker_transactions` 整表 SDK 呼叫停留約 55 分 40 秒，接近 8.9 GiB 記憶體尖峰，最後由外部 TERM 結束；當時只完成 82／1,109 個目錄鍵。既有 4 小時 unit timeout 只能限制整輪，無法讓後續小鍵在同輪前進。該巨大鍵已單獨列為資源暫緩，不把它當成已下載。

現在排程預設每次只處理一鍵，透過 GNU `timeout` 對每個 SDK 子程序設 600 秒上限；超時後只憑相同 attempt ID 與 `runs/latest.json` 的進行中鍵寫入 `timed_out` 收據，保留 `partial`、真實錯誤計數及剩餘估計，接著處理下一鍵。每輪最多容忍兩個鍵逾時，避免長時間佔住重型資料 slice。若子程序已自行完成終端收據，恢復器不改寫其結果；無法辨識正確進行中鍵、非超時異常或超時後強制殺不掉時仍報錯，絕不把它們歸類為成功。手動覆蓋 `FINLAB_SYNC_BATCH_SIZE` 大於一時，上限作用於整個子程序而非單鍵；正式部署仍維持預設一鍵。

這是**有界故障隔離**，不是 SDK 下載加速，也未重啟帳號服務或消耗額度驗收。下次 16:10 正式執行仍須看逐鍵耗時、配額、`latest.json` 終態與私有研究發版守門；不能把 `partial` 說成資料已齊。`test_finlab_history.py`／`test_finlab_dashboard.py`、shell 語法及 Ruff 已完成隔離驗證；正式 run 尚未載入新版。

同輪重新枚舉維持 **50 service／39 timer／2 path**；收據為 `artifacts/benchmarks/service-coverage-20260924T0036-local.json`。它顯示 Crypto 訓練刷新最近一次仍 failed，FinLab 上次 3,340.7 秒程序結束但 `Result=success` 不代表同步完成。`registered-data-intraday` 與官方當沖資格監測當時正在執行，不能對進行中的程序套用「最近一次完成耗時」。

Crypto 訓練刷新新增逐步 `step_attempts`：每個子程序開始即記錄活動命令、結束寫實測 monotonic 秒數及成功／失敗；即使原始 writer 於步驟後出現或來源簽名變動而延後發布，已花的計算時間仍保留。既有 `steps` 仍只收錄通過後續檢查的步驟，發布亦有獨立耗時。這只補量測和中斷證據，不變更 raw-writer／來源位元守門，也未解決與約兩分鐘一輪的原始寫入競爭。FinLab、Crypto 相鄰 **39 個測試**、Ruff、shell 語法及 whitespace 檢查通過；下次正式 Crypto 02:30 的收據仍待驗收。

全服務量測另新增 `last_attempt_outcome`，把正在執行、從未觀測、非零退出、systemd 失敗與程序零碼結束分開；`process_exited_zero` **不代表資料完成**。00:41 的新收據 `artifacts/benchmarks/service-coverage-20260924T0041-local.json` 確認 `crypto-training-refresh` 為 `failed`，FinLab 雖然 systemd `Result=success`、但 `ExecMainStatus=15`，現明確列 `nonzero_exit`。這修復跨 50 個 unit 的監測誤讀，不改 service 本身。相關測速、追蹤、Crypto 與 FinLab **54 個測試**通過，Ruff、shell 語法與 whitespace 檢查通過。

## 9/24 00:46 Windows／WSL 非 systemd 路徑覆核

本機 root crontab 為空、`/etc/cron.d` 未見 StockAgent 工作；root 使用者層 systemd 沒有運行的 StockAgent unit。Windows 四項相關排程仍為公開 Caddy（Running）、crash dump 保存（Ready）、停用的一次性 WSL 修復與停用的 WSL 每日備份。Caddy 的程式與設定 SHA-256 都和 repo 原檔一致，本機 gateway `/healthz` 為 200；這仍不是外網雙棧或冷開機復原證明。

排程每分鐘觸發的 Caddy 工作顯示非零上次結果 `0x800710E0`，但現有實例仍在 Running；新增唯讀 Windows process 證據，記錄兩個 Caddy PID 的父程序與啟動時間，不讀命令列、憑證或設定內容。新收據 `artifacts/benchmarks/service-coverage-20260924T0046-local.json` 顯示 Caddy PID 5060 的父程序是 9/15 已啟動的 PowerShell PID 12128，另一個 Caddy PID 7172 由前者啟動；啟動日誌最後一次 dispatch 也是 9/15。排程設定為 `IgnoreNew`，依 [Microsoft 的正式語意](https://learn.microsoft.com/en-us/windows/win32/taskschd/taskschedulerschema-multipleinstancespolicy-settingstype-element)已有實例時後續觸發不另開實例，因此「重複觸發被忽略」符合觀測；但 Task Scheduler Operational 日誌目前停用，**不能以此單獨證明 `0x800710E0` 的確切成因或真正冷開機恢復時間**。健康判斷須同時看實例、Caddy 程序、backend 與 HTTPS，而非只看最後結果碼。相關唯讀稽核測試 25 個通過。

## 9/24 00:52 Shioaji 儲存監測與 OpenBB L1

`stockagent-shioaji-storage-monitor.service` 最近一次正式掃描 **4,494,363** 個本機實體檔，`scan_seconds=161.304`，CPU 約 161 秒；它不連永豐 API，也不應為了節省掃描成本刪除資料群組、漏算成長或跟隨 symlink。改為先計算最近 30 個台北完整曆日的精確午夜時間界線，每個檔案的 mtime 只做數值區間／二分搜尋；目錄堆疊也不再替每個目錄建立 `Path`。在同一份固定的 123,015 檔、約 5.05 GB 唯讀樣本，舊／新 `_scan_dataset` 回傳值**逐欄完全相等**，交錯三輪中位數 0.7757 → 0.5792 秒（約 25%）；這不是 449 萬檔正式整輪已改善 25% 的證明。新快照另含每資料群組 `scan_seconds`，下一次 01:20 正式排程才能核對完整 wall、CPU、檔數與各群組。相關 Shioaji 儲存及面板測試 25 個通過。

OpenBB L1 最近正式輪次 00:44～00:49 約 5 分 16 秒，新增 19 段、剩餘 4,008,695 個 L0 shard 待壓縮；cgroup 峰值約 2.50 GiB RAM 與 3.64 GiB swap。直接放大 batch／併發或禁用 swap 可能損害同機即時工作，不能因單次 wall 長就盲調。L1 收據目前只有新段與待處理數，已加入來源過期契約稽核、未指派來源載入、批次規劃、段建置、查詢 view 發布、隔離與狀態查詢的分段 monotonic 耗時；**沒有**改 L0、Parquet、SQLite、刪除或發版語意。下次 01:19 正式輪次才可歸因與選擇真正瓶頸；L1 加 Shioaji 儲存相鄰 32 個測試及 Ruff 通過。

00:54 再以 SQLite 唯讀連線核對，manifest 有 **13,287,450 tasks、3,308,947 L1 成員、151,945 segments**。`EXPLAIN QUERY PLAN` 顯示過期契約稽核逐 segment 掃已壓縮成員並依 task 主鍵查證；下一批未壓縮 shard 的查詢則用現有 `idx_tasks_schedule_age_v2` 篩 active/success，但 `ORDER BY endpoint,task_id LIMIT 2048` 需要 **TEMP B-TREE** 排序。這是數百萬列下的明確候選瓶頸，**還不能**僅憑 query plan 推斷其實際耗時或 swap 來源。直接為 1,329 萬 task 建大索引會增加資料庫與 I/O，且可能阻塞同一個 archive writer；先用下一輪分段實測決定是否值得，再採可中止的受保護建置或改寫方案。

另對兩項低優先級服務做現場證據分流，避免只盯長 wall：`stockagent-packed-backup` 00:48 最新 current-heads 收據為 11,787 個物件、121 個 head、464,027,907,190 位元組已驗證、待辦 0，最近無新複製，每次重新調和約 12～15 秒；**歷史完整性仍 `not_checked`**，不能擅自跳過驗證來縮短這段時間。`stockagent-artifact-dedup` 上次只對 3,706 個不同 inode 做內容雜湊，39.7 秒 wall、29.2 秒 CPU，精確重複組 3 組、回收實體配置約 24,576 位元組；其低優先級與無損重驗語意使它目前不是先於 OpenBB swap／公開來源遲滯的可證明主瓶頸。這只是優先序判斷，不是宣稱兩服務達到極限。

## 9/24 01:00 OpenBB L1 稽核單次開檔試驗

正式 L1 稽核對每個既有 segment 原先先開一次 Parquet 讀行數，再重開同一檔案取 schema 指紋。改成同一 `ParquetFile` 同時讀兩者；仍依原先順序檢查缺檔、行數、移除 schema metadata 後的 SHA-256，沒有跳過任何 segment，也不更改來源或發版。從目前 SQLite manifest 唯讀取得 1,024 個已成功 segment，交錯跑兩次：原路徑 6.815／5.542 秒，單次開檔 3.763／3.436 秒；1,024 個行數與 schema 指紋逐一相等。這是同機此樣本的局部 I/O 測速，不把差值線性外推到 15 萬段或宣稱整輪 wall 已改善。OpenBB L1 既有 7 個端到端、失效重建與稽核測試以及 Ruff 通過；下一次 01:19 正式輪仍要看 `stale_contract_audit` 與其它分段收據。

同一回歸測試另加入「衍生 Parquet 行數仍相同但 schema 不同」的實際寫檔破壞，正式 audit 回拒、正常 compact 恢復、後續 audit 通過。01:10 重新枚舉仍是 50／39／2，10 秒取樣的常駐 CPU 最高約為永豐 TAIFEX BidAsk 0.072 cores、Discord 0.044、台灣公開來源事件 0.027、當沖模擬 0.020；宿主 CPU／記憶體／I/O PSI `full avg10` 均為 0。這只界定當時沒有宿主壅塞，不等於每個排程已完成或開盤延遲達標；可重測收據為 `artifacts/benchmarks/service-coverage-20260923T171010Z.json`。

Crypto 發行的獨立現況核對：`data_bybit` 約 22 GB／4,306 個實存檔案，catalog 的 `bybit` source 正是整個 mutable `data_bybit`；冷庫 penguin head 最後是 2026-09-21 02:35 UTC。一次 `publish_data_releases.py status bybit` 在原始 writer 間隙曾顯示 `publish_ready=true`，但那只表示**當下無活躍 writer**，不是有足夠時間對 22 GB 做完整 catalog 發行，也不是新冷版已完成。現行來源發行不可繞過 writer gate；真正排除每兩分鐘反覆競爭，需要先建立可審計、跨整個 22 GB 來源的一致快照／producer 交接契約，並核對全部寫入器的原子性、發布與原始 freshness 的因果界線。尚未實作，不把短暫 ready 冒充成功。

01:19:30 OpenBB L1 正式 timer 載入新版完成：wall 294.633 秒、CPU 222.600 秒、19 個新段、0 stale／failed、4,006,647 個來源 shard 待壓縮，沒有刪 L0。前一輪約 316 秒；新舊輪有不同來源量及同時的 Shioaji 掃描，不能把約 21 秒差全歸因於單次開檔。新版正式收據 `data_openBB/_state/l1_compaction_latest.json` 的分段是來源契約稽核 120.976 秒、未指派 shard 載入 41.661 秒、建段 3.582 秒、DuckDB 查詢 view 發布 118.769 秒、狀態 task count 4.205 秒。cgroup 記憶體尖峰約 2.50 GiB、swap 尖峰約 3.76 GiB；`MemoryHigh=2.5 GiB` 附近仍有明顯換頁，不能稱為整體資源最佳化。下一步的大宗應是 view 發布及未指派查詢，而非增加建段執行緒。

01:20:59 Shioaji 儲存監測正式 timer 完成 4,497,460 檔、142.082 秒、`status=ready`；上次 4,494,363 檔、161.304 秒。此輪與 OpenBB 重疊、檔數多約 3,100，仍短約 19.2 秒／11.9%，但不能把它稱作無爭用的固定加速率。各資料群組：期貨歷史 1,903,284 檔／81.353 秒、FOP 串流 1,625,053 檔／31.577 秒、歷史市場 286,019 檔／16.528 秒、股票分鐘 489,920 檔／9.329 秒。此服務只掃本機儲存、不登入 Shioaji；未因測速重啟券商／行情行程。

DuckDB view 發布原本只量整段 118.769 秒，無法判定是 24,792 段的 ETF 視圖、20,843 段的 SEC filing headers，還是其它端點／CHECKPOINT。已在相同正式收據加入 `view_endpoint_seconds`，逐端點記錄既有建 view＋catalog 寫入耗時；不改任何 SQL、來源路徑、view 名稱、schema 或原子替換。含同筆數 schema 損毀回歸的 7 個 L1 測試通過；**01:19 已啟動的舊版沒有此欄**，下一次正常 timer 才可驗證逐端點瓶頸。

對同一正式 manifest 只讀拆分 2,048 個未指派 shard，原全域查詢一輪 44.741 秒、逐 Parquet metadata 驗證 3.043 秒；原查詢另一輪僅取 task IDs 為 37.354 秒，顯示 OS 快取／爭用會改變單次 wall。用現有 task 主鍵強制排序花 104.082 秒，已淘汰，沒有進入正式程式。另一個受控快路徑先用現有 `idx_tasks_active_plan` 找第一個未指派端點，再只排序此端點；若它不足以填滿要求的 2,048 筆，或指定了端點篩選／缺少索引，就退回原全域查詢。單獨 SQL 測得找首端點 5.070 秒、端點內排序 26.483 秒；與原查詢的 2,048 個 task ID SHA-256 **完全一致**。接線後再次對正式 manifest 唯讀重測為首端點 6.530、端點排序 33.734、metadata 0.707、總 40.971 秒；相鄰舊完整路徑 47.786 秒。這是不同時間的低優先權單輪樣本，不能聲稱固定加速率或下一正式輪已驗收。快路徑、首端點不足回退與缺索引回退的測試，加上原 L1 審計測試共 8 個通過；不新增 1,329 萬 task 的大型索引，也不變更來源集合或排序契約。

為避免把 view 118.769 秒誤歸因於 2 萬段端點的 SQL 本身，又在**隔離 DuckDB** 只讀正式 manifest 完整建 42 view／1 deferred：總 9.054 秒，其中 ETF 24,792 段 4.483、SEC 20,843 段 1.614 秒。單端點隔離重測也只有 6.913／1.047 秒。與正式輪的差距很大；正式輪同時有 cgroup `MemoryHigh` 附近約 3.76 GiB swap 峰值與 I/O 壅塞，較可能是狀態／資源交互，而不是單一 view SQL 固定耗時。下一正式輪的逐端點收據、cgroup 與 PSI 才能決定是否要做安全的 view 增量發布或記憶體生命週期優化。

低優先權、只讀的 20,000 段 Parquet metadata 稽核逐筆行數與 schema 指紋均通過；掃描中程序 RSS 約 95 MB、Arrow pool 0、swap 0，沒有隨段數持續累積。這否定了「單次開檔稽核本身線性洩漏 2.5 GiB」的猜測，卻不能解釋正式輪較後階段的記憶體；因此下一輪正式收據再增加每階段程序 RSS／swap、cgroup memory／swap、`memory.high` 事件及該 cgroup 的 memory／I/O full-stall 累計值。這些是唯讀診斷；欄位缺權限會是 `null`，不影響查詢、輸出、刪除或發版語意；8 個 OpenBB L1 測試通過。

Shioaji 儲存掃描另試了以一次 `DirEntry.stat(follow_symlinks=False)` 的 `st_mode` 判別目錄／檔案，取代三次 `is_symlink`／`is_dir`／`is_file` 判斷。固定 165,176 檔／7,250,320,592 bytes 的 Top-200 來源輸出相等；交錯單輪舊版 1.357、0.944 秒，新版 1.046、0.979 秒，第二對新版較慢，無可信的穩定收益，**未改正式實作**。保留目前已正式改善且等價的午夜界線掃描。

當沖引擎目前程序自 9/23 12:32 起執行；稍後新增的開盤 queue／loop 分段測速尚未載入該程序。9/24 01:42 的正式 `status.json` 唯讀顯示 5 模式中，`tw_day_trade_100m` 有 35 筆 open／margin carry、`execution_evidence_complete=false`，其餘四模式 open 0；ledger integrity `ready=true`／divergence 0 只代表狀態與帳本一致，不代表這 35 筆可成交或當沖紀律已恢復。因此未為純測速重啟持有 Shioaji 報價狀態的當沖引擎；要讓新開盤計時在明日 09:00 生效，仍需單獨完成離峰重啟前後的持倉、帳本、訂閱及模式 revision 驗收。不得以便宜的重啟抹掉留倉問題。

## 9/24 02:10 OpenBB L1 正式瓶頸與增量視圖

01:54:53 正式 L1 輪次完成，wall **282.104 秒**，新增 19 段、尚待 4,004,599 個來源 shard，0 stale／failed，未刪 L0。分段收據：既有段契約稽核 108.369 秒、未指派來源載入 41.910 秒（找首端點 5.427／端點內排序 35.928／metadata 0.555）、新段建置 3.735 秒、DuckDB 視圖發布 **117.414 秒**。逐端點顯示未變動的 `etf.nport_disclosure` 佔 **107.604 秒**，不是新段建立慢。建段後 cgroup memory 約 1.73 GiB／swap 0；視圖發布後 `memory.high` 累計增加到 19,527 次、memory full-stall 約 3.081 秒、I/O full-stall 約 26.428 秒，且服務觀測到約 2.5 GiB memory／3.62 GiB swap 峰值。這是正式當輪觀測；單靠總耗時不能把全部 117 秒歸因於 ETF SQL，cgroup 與主機 I/O 交互仍需後續正式輪分辨。

已改成複製上次原子發布的 DuckDB 資料庫到暫存檔，對每個端點用**有序 segment 路徑清單 SHA-256** 判斷是否需要重建；重用前檢查既有 catalog 的端點／視圖集合、每個視圖的 SQL SHA-256，以及目前來源路徑仍存在。只有簽章一致的視圖重用；變更端點重建，失效／缺舊簽章則完整重建；最後仍以 `os.replace` 原子發布。沒有更改 L0、Parquet 內容、端點命名、查詢輸出語意或原有 schema／行數稽核。收據新增逐端點 `view_endpoint_actions`（`rebuilt`／`reused_verified_paths`／`deferred`），可量測每輪實際命中而不是冒充熱快取。

在**正式 SQLite manifest 唯讀連線、隔離 DuckDB 輸出**上完整測 42 published／1 deferred：首輪建全部視圖 **12.610 秒**（ETF 6.775 秒）；同一份 manifest 第二輪驗證並重用全部 **1.645 秒**（ETF 0.199 秒）。這是無新 segment 的隔離對照，不能聲稱正式 117 秒已縮為 1.645 秒。新增舊版無簽章、視圖 SQL 被異動、兩端點只一端變更、端點延後時移除舊視圖及恢復後重建的回歸，加原有增量、stale 稽核共 **12 個 L1 測試通過**，Ruff 通過。正式 DB 目前仍是舊 schema：**下一次正常 timer 先做一次保守完整重建，之後才可正式驗證重用**；保留各階段 cgroup 與逐端點收據觀察剩餘稽核與來源查詢成本。

02:10 全服務唯讀重新枚舉仍為 50 service／39 timer／2 path，收據 `artifacts/benchmarks/service-coverage-20260924T0210-local.json`。已完成 oneshot 的 CPU counter 不應以取樣前後相同而顯示 `0.0 cores`：這只表示兩次觀察之間未執行，會誤導為工作本身零成本。共用測速現在僅對觀察區間**前後均持續執行且同一 invocation**的服務計算 CPU 差；閒置的最近一次 wall／exit 仍保留，CPU 顯示未知。25 個相關測速與追蹤測試、Ruff 通過；上述 02:10 舊收據的 idle `0.0` 不可作為優化成果。

02:14 新程式重新盤點的 `artifacts/benchmarks/service-coverage-20260924T0214-corrected.json` 再次列出 50／39／2：已結束的 L1 工作 CPU 為 `null`、保留 282.107 秒上次 wall；上次失敗的 Crypto refresh CPU 亦為 `null`、保留 failed／23.643 秒；仍在執行的 TAIFEX BidAsk 則有該次取樣的 0.042 cores。這是修正測速語意的現場驗收，不代表 L1／Crypto 業務功能完成。

此份全服務收據的量測覆蓋是 **34 項最近一次已完成程序 wall、14 項執行中資源取樣、2 項未觀測**；未觀測的是已停用的舊 hot-artifact sync 與 registered-data-backfill，沒有為了製造秒數而啟動它們。這些仍只是 unit 層量測，不能將每項業務步驟或外部供應商延遲宣稱已量到。

02:17 即時 HTTPS 再驗：本機 `/healthz` HTTP 200／約 1.5 ms，DDNS 公網 IPv4 HTTP 200／約 466 ms，強制 IPv6 連線失敗。這只是當次可達性；沒有因此把 IPv6、業務資料、帳本成交或冷開機恢復標記完成。

同時把 `stale_contract_audit` 再拆成來源契約 SQL、L1 衍生 Parquet metadata 掃描與真正 stale 狀態套用三段計時；不變更稽核路徑。下一正式輪可判定約 108 秒應處理 SQLite join、檔案 I/O 還是有錯誤才會執行的資料庫更新，避免僅憑總秒數改索引或放寬驗證。相關 L1 12 個測試與 Ruff 通過。

同時覆核高 CPU 的 `registered-data-intraday` 正式 02:12:56 輪：Binance 574 symbols 約 29.37 秒、Bybit 867 symbols 約 33.22 秒、OKX 491 symbols 約 56.15 秒，三者重疊執行，整輪 **57.11 秒 wall／約 120 秒 CPU／2.2 GiB memory peak**，各來源這輪 `status_counts.updated` 等於其 symbol 數。這顯示現有 1m tail 三家並行，不能把 29+33+56 秒相加當串接延遲，也不能僅因 2.42 cores 的瞬間觀測就再提高 workers。此輪刻意 `historical_features_enabled=false`，所以不證明歷史特徵已更新或所有資料完整。

## 9/24 02:35 正式雙排程驗收

02:30 Crypto refresh 與 02:30:12 OpenBB L1 同時啟動。Crypto funding 1.353 秒成功；Bybit 日表建置 **157.187 秒**後，394 標的中 383 完成、11 失敗，`build_mode_counts` 是 383 full／0 incremental。失敗的 GALA、GAS、GLM、GMT、GMX、GRASS、GRIFFAIN、GRT、GUN、HBAR、HEI 都明確回報 `Bybit source changed during daily materialization`；盤中分鐘原始更新於 02:30 前後及 02:32:50 又起動，使同一標的的讀取與寫入前證明不一致。正式 service **failed**，沒有進入後續公開特徵、稽核或冷發布。這不是前次單一 `raw writer started` 錯誤，不能把兩者混為一談。383 個成功輸出建立了逐檔 sidecar，但整體訓練資料仍未完成；下輪是否能命中增量與完成 11 檔要用收據驗證。服務這輪約 2 分 39 秒 wall、23 分 36 秒 CPU、6.9 GiB memory peak。`materialize_summary.json` 的 11 failed 是 fail-closed 證據，不能用已存在的舊 Parquet 冒充本輪完整。

OpenBB L1 於 02:35:36 完成：**323.859 秒 wall**、19 新段、4,002,551 待壓縮、0 stale／failed、0 L0 delete。因既有 DB 無新簽章，42 published view 全部按設計保守重建、1 deferred；ETF 佔 view 96.010 秒，整段 view 105.189 秒。新增分段顯示來源契約 SQL **75.800 秒**、既有衍生檔 metadata 掃描 **94.274 秒**、stale 狀態套用 0 秒；未指派來源載入 35.026 秒（找首端點 5.745／端點內排序 27.752／metadata 1.528），建新段 3.322 秒。服務峰值 memory 2.5 GiB／swap 3.6 GiB，`memory.high` 20,009 次、視圖階段額外 I/O full-stall 約 13.5 秒。與上一輪 282 秒的工作量／競爭不同，尤其本輪同時有約 8 核的 Crypto 建置；不可將 42 秒差歸因為程式退步。**下一正常輪才是正式視圖重用的驗收點**。既有檔案 metadata 稽核仍佔大量時間，接下來先用固定實檔樣本比較同一行數／schema 契約的開檔並行方案，不減少檢查或改資料。

完成後獨立唯讀開正式 DuckDB 驗 43 筆 catalog、42 個 published view，每個 published view 的 SQL SHA-256 均與新 catalog 相符；1 deferred 沒有視圖／簽章。這證實下輪已有可驗證的重用起點，但不預先保證下輪實際命中或壁鐘收益。

固定 8,192 個正式成功段的唯讀、低優先權交錯測速：單執行緒 4.364／1.991 秒，4 執行緒 4.449／4.950 秒；兩者 8,192 個行數＋schema 校驗皆相同，並行沒有穩定收益，未接線。另一組在相同 8,192 檔上比較每檔先 `is_file()` 再開 Parquet metadata，與直接開 Parquet、僅在開檔錯誤時補 `is_file()` 分類：舊 1.592／1.657 秒，新 **1.387／1.396 秒**，全部 8,192 筆逐一通過。已把後者接進正式衍生檔稽核，仍逐檔驗行數與 schema，缺檔、壞檔仍 fail-closed；新增實際刪除**測試暫存** L1 檔後 audit 拒絕、正常輪重建與原錯誤原因保留的回歸，OpenBB L1 **13 個測試及 Ruff 通過**。這是局部樣本約 13–16% 改善，正式 15 萬檔掃描仍待下一輪驗收；沒有降低稽核頻率或採信 mtime 代替 metadata。

對另一段 `OFFSET 70000` 的 8,192 個真實檔再交錯：舊 3.687／1.468 秒，新 1.247／1.252 秒；第一個舊樣本顯著受冷檔案快取影響，不能用 3.687 對 1.247 宣稱約三倍提速。同為較熱的後半組，新少約 0.216 秒／15%，8,192 筆輸出仍全相同，支持少一次 `stat` 在不同檔群也有穩定局部收益。

02:46 公開 gateway 39 條有限路由各量 `first_observed`＋2 次 HTTP 回應，`artifacts/benchmarks/all_services_latency_2026-09-24_recheck.json` **0 HTTP 錯誤**。當次首次完整 1m 當沖歷史約 975 ms，日期全區間訊號／事件約 1,770／1,814 ms，持倉約 1,889 ms；同 key 後兩次的低個位數 ms 多為已建立的服務快取，不能冒充冷來源重建。全資料 features 首次約 1,088 ms；TAIFEX 1 日首次約 619 ms。樣本只有每路由 3 筆且順序執行，非 p95、WAN、瀏覽器繪圖或資料正確性證明，也不直接拿來和不同日期／快取狀態的 9/23 基線作固定倍數比較。

OpenBB L1 的來源契約 SQL 原先對所有已壓縮成員逐筆呼叫 Python 路徑正規化。正式 manifest 抽樣 10,000 筆皆為「task 相對路徑／member 絕對路徑」的可直接拼接型態；已在 SQLite 先比對精確拼接，只有不相等時才退回原 `stockagent_resolve_path`，所以絕對、`.`、`..` 與非標準路徑仍沿用原判斷。對正式 330 萬筆成員以唯讀 SQL 順序跑原／新查詢，各回傳 0 個 stale 契約，結果 SHA-256 一致；wall **29.746 → 19.212 秒**。這是受不同 OS 快取／同機負載影響的單組只讀比較，不是正式 75.800 秒的下一輪結果。新增相對、帶點、絕對等價路徑及真正換路徑的回歸，OpenBB L1 14 個測試通過；下一正式 timer 要檢查 `stale_source_contract_query` 是否下降、所有稽核結果與資源壓力是否保持正確。

Crypto 日表的 11 個正式失敗標的都有相同的「來源在單標的建置中原子替換」錯誤。新增**僅對此明確版本競爭**的受限第二輪：其他 383 個結果先保留，第一輪結束後對發生競爭的標的重新擷取來源身分、重算並驗證，再將最終每標的結果只計入進度一次；持續變動或其它錯誤仍為 failed，不能發布。此變更不用暫停每分鐘寫入，也不放寬 Parquet 原子替換前後或整體冷發行的來源證據。相鄰 Crypto／OpenBB 74 個測試通過；**下一次正式 Crypto refresh 尚未執行**，單標的重試也不能根治整個 22 GB mutable `data_bybit` 來源與冷發布的交接問題，故服務健康仍維持 failed。

對當沖全期間事件明細另做一次 `cProfile`：查詢掃描 orders／fills 各 100,000 筆，約 20 萬筆事件中 `consume_batches` 的逐列 Python 欄位查表占約 1.34～1.39 秒；臨時 JSONL 投影寫入約 0.56 秒，另外約 0.32 秒是新程序第一次載入交易日曆。改成把 Arrow batch 的 8 個識別欄位順序轉為列並逐列解包，不改身份、去重、排序、筆數或 scan cap。隔離的同範圍三次結果均為 199,506 筆、前 100 筆 SHA-256 完全一致；後兩次非頁面快取建置 1.728／1.793 → 1.592／1.594 秒，屬不同時間、同機競爭下的樣本。當沖／隔日沖／rollover 173 個測試通過。

03:00:38 僅重啟唯讀 `stockagent-public-dashboards.service` 載入事件頁改動；當沖模擬、Discord Gateway、永豐 TAIFEX BidAsk 的 PID／啟動時間保持不變。本機 `/healthz` 與當沖 status、IPv4 公網 `/healthz` 均 HTTP 200。服務重啟後首筆廣範圍事件頁 `Server-Timing build=2653.643 ms`，之後三個不同 `limit` 的實際 build 約 2249／2250／1939 ms；**未能在現場證明正式 HTTP 延遲改善，也未達一秒**，與隔離 benchmark 不等同。後續應在固定資料版次及負載條件下重測，並優先考慮重用事件範圍投影／增量索引，而不是繼續微調每列 Python 幾十毫秒。

## 9/24 03:08 OpenBB L1 新版正式重用驗收

03:05:49～03:07:59 的既有 timer 正式執行：**129.959 秒 wall／114.386 秒 CPU**，新增 18 段、0 stale／failed、仍待 4,000,503 個 L0 shard、`l0_deleted=false`。對照 02:30 那輪 323.859 秒、19 新段，整體較短約 194 秒，但兩輪來源批次與同機負載不同，不能把全部差距視為程式單一改動的因果效果。正式收據 `data_openBB/_state/l1_compaction_latest.json` 的分段是來源契約 SQL **20.650 秒**（前輪 75.800）、衍生 Parquet metadata **60.719 秒**（前輪 94.274）、未指派來源 33.406 秒（前輪 35.026）、建段 3.128 秒（前輪 3.322）、DuckDB 視圖發布 **5.272 秒**（前輪 105.189）。來源 SQL 的唯讀同 manifest 比較與這次正式方向一致；metadata 仍是最大單段，沒有省掉行數或 schema 稽核。

正式 `view_endpoint_actions` 為 **41 `reused_verified_paths`、1 `rebuilt`、1 `deferred`**；舊輪是 42 rebuild／1 deferred。重建的 SEC filing headers 約 3.244 秒；沒有新段的 ETF 視圖本輪驗證重用約 0.231 秒，前輪在資源爭用下重建 96.010 秒。發布後以正式 DuckDB 唯讀重新驗證 catalog／實際 view SQL SHA-256，43 個端點中 42 published／1 deferred，整體校驗通過。cgroup 本輪至發布後觀察的 swap 為 0、`memory.high` 事件 1,346，前輪峰值約 3.6 GB swap、20,009 事件；仍有 2.5 GB memory peak 和 I/O stall，不能宣稱資源已達下界。下一個可量測大宗是約 60.7 秒的 15 萬既有衍生檔 metadata 校驗；除非建立具備內容與失效證明的增量稽核索引，不應以跳過檔案檢查換速度。

OpenBB 與唯讀 gateway 載入新版後，再跑完整 39 條有限 HTTP 路由各 `first_observed+2`，`artifacts/benchmarks/all_services_latency_2026-09-24_after_openbb_and_gateway.json` 為 **39/39 HTTP 成功、0 錯誤**。這一輪全期間當沖 events 首次約 1,556 ms、signals 約 1,735 ms，data-monitor features 首次約 1,030 ms；其餘同 key 的快回應多是服務快取。此順序式 n=2 測試不是無快取來源重建、併發吞吐、瀏覽器端感受或公網 p95，故仍把廣範圍明細與 features 首次顯示列為未達一秒的瓶頸。

03:10 再產生全服務唯讀收據 `artifacts/benchmarks/service-coverage-20260924T0310-post-optimization.json`：仍是 **50 service／39 timer／2 path**，33 項有最近一次完成程序 wall、14 項有連續執行資源取樣、3 項本輪未量到操作 wall／取樣。第三項是恰在 10 秒取樣中途啟動的 `registered-data-intraday`，按測速契約不能把局部樣本計成平均 CPU；另兩項仍為停用的 hot-artifact sync 與未曾完成的 registered-data-backfill。唯一 systemd `Result!=success` 為 Crypto refresh 的正式 failed；未以重試程式已通過單元測試把它改為成功。

FinLab 55 分鐘長輪再查日誌，20:14:59 最後可觀察的供應商請求是 `broker_transactions`，當時帳號 SDK 印出日用量約 4,720／5,000 MB；至 21:10:10 收到 TERM、沒有該 key 的完成收據。該 unit 的 `TimeoutStartSec=4h`、單 key 包裝 `timeout=600s`；單靠這段日誌不能判明是 SDK 網路、配額、記憶體壓力，還是外部停服務造成 55 分鐘，故不把它算作可藉提高 workers 縮短的 CPU 熱點。目前程式的 `AUTOMATICALLY_DEFERRED_KEYS` 已明列此未界定的大表及其需「有界分片」的條件，下一正常 timer 是 16:10；跳過它只避免阻擋其它 key，**不代表 1,109 項帳號資料全部下載**，目前最後收據仍只有 82 項 local download。此輪未擅自重啟付費帳號下載或改配額策略。

當沖 `margin-actions` 在 9/23 18:00 的來源 collector 已建出 34,573 列公司行動參考與 2,212 筆精確權益事件，但共用冷發布因上游 `stale_feature_build_receipt` 拒絕，`Restart=on-failure` 讓整個來源建置 18:09、18:16 又重跑；其約 2～4 分鐘每輪的資源耗費不是新的資料要求。現在僅在 **`run_tw_day_trade_margin_actions.sh`** 的冷發布呼叫加 `--defer-stale-derived-receipts`：`_check_training_receipts` 仍嚴格拒絕任何 stale 發版，收據明列 `status=deferred`、`reason=stale_derived_receipts` 和阻擋碼，但這個已完成來源工作的 unit 不再因無關上游收據重算三遍。獨立的 `tw-public-cold-publish` timer／預設命令仍嚴格失敗並會重試；其它發布錯誤在來源服務也仍失敗。相鄰公告／當沖來源測試 **72 個**與 shell 語法、Ruff 通過。**未觸發今天新的官方下載或正式冷發布**；下次正常 18:00 必須確認來源收據 `source_ready`、冷發布 `deferred` 而非 `ok`，以及沒有重複 collector 啟動。

03:17 再查公開健康：當沖 status 仍 `degraded`，全資料監控仍 `critical`（65 unable、36 catching up）；IPv6 HTTPS 仍 `curl` connect 失敗，沒有以 39/39 的 HTTP 成功把資料品質、交易證據或雙棧連線標成完成。此次程式與測速變更合併的相鄰回歸為 **361＋72 個 Python 測試**，Ruff、shell 語法與 `git diff --check` 通過；這不是全庫測試或開盤／冷啟動驗收。

## 9/24 03:19 之後的剩餘瓶頸排除

OpenBB 的 8,192 個正式成功 L1 檔逐階段唯讀取樣：`ParquetFile` 開檔 1.903 秒、Arrow schema 轉換 0.191 秒、讀行數 0.008 秒，總 2.144 秒；6,364 種 schema 讓單一 schema 快取也不太可能消去大量 CPU。相同檔案的 `pq.read_metadata` 與 `ParquetFile` 交錯重測約 1.17～1.22 秒，沒有穩定收益；2 執行緒約 2.21～2.25 秒、單執行緒 1.19～1.27 秒，並行更慢。152,012 個成功段已分布於 43 個目錄，相鄰列約 99.97% 同目錄，額外排序也無明確改善空間。這些結果排除幾項看似容易、實際增加複雜度或 I/O 壓力的方案；正式每輪約 60.7 秒的 metadata 完整稽核仍未壓至下界。

未指派 L0 查詢的首個端點 `regulators.sec.filing_headers` 有約 5,961,281 筆 active/success task；現行端點索引需以 temp B-tree 排序，唯讀單輪約 27.658 秒。改強制全域 `task_id` 主鍵的相同 2,048 個結果 SHA-256 一樣，卻約 29.372 秒；只取第一筆仍約 24 秒，因早期 task ID 大量已有 L1 成員。故不能靠簡單換查詢提示改善。新增 `(active,plan_token,status,endpoint,task_id)` 類覆蓋索引可能加速讀取，但會在約 1,329 萬 task、16 GB 正在使用的 SQLite manifest 上帶來建置 I/O、寫入放大與 live archive 競爭；未經隔離完整 benchmark 和安全建置窗，不直接在線上盲建。

當沖公司行動來源工作新增單步 `elapsed_seconds`、最後驗證與總耗時到原有 `readiness.json`；collector 成功／非零退出均保留原有結果與錯誤語意。這使下一次正常 18:00 執行可拆分三個官方來源 collector、Parquet／權益契約驗證與冷發布等待，而不是只看整個 unit wall。唯讀隔離 mock 的成功／失敗收據測試共 3 個及 Ruff 通過；沒有為產生秒數再次觸發官方資料下載。

Discord artifact maintenance 的最近 54.675 秒並非歷史推論變慢：23:00:04 明確寫出 `waiting_source/tw_public_refresh_in_progress`，直到 23:00:52 才繼續，最後 10 個市場均 `reconciled_current_artifact`、attempted 0／failures 0；該輪只用約 9.782 CPU 秒。前幾輪無等候時約 7.2～13.5 秒 wall。這是與 TW-public canonical writer 協調的來源等待，不能靠縮短正式歷史推論 timeout 或重啟 Discord Gateway 消去；目前不修改來源閘門，只保留等待與工作耗時的區別。下一 timer 13:40 的排程若再長，可用相同日誌判斷等待或真實工作。

## 9/24 03:36 當沖分鐘曲線重試成本與來源延遲

正式 9/23 18:00 分鐘曲線工作停於 `waiting_source/benchmark_source_preflight`，因為 `data_tw_index_futures/shioaji_history/TXFR1/receipts/trading_date=2026-09-23.json` 確實不存在；未執行基準或曲線重建。單次 `_completed_scope` 約 0.447 秒、對 221 MB／195,765 列 `marks.jsonl` 重掃不可缺的 09:01／13:30 端點約 1.601 秒、TX receipt 查詢不足 1 ms。這區分了空等的 CPU 成本與真正尚未取得來源的時間。

現已在分鐘曲線維護狀態收據附上端點預檢的原始檔 `device/inode/size/mtime_ns/ctime_ns`、交易日、模式集合與契約版號。只有上次確定 **0 個缺漏** 且以上全部相同，才略過 221 MB 的再次掃描；來源增刪改、模式或日期改變即全掃，且全掃前後簽章變動會拒絕發布。相同實帳本的隔離量測為首輪 **1.680 秒**、同源重用約 **0.011 ms**。正式 oneshot 連續啟動：首輪仍報 `waiting_source`、端點 `reused=false`，後輪 `reused=true`，最後一次收據分段 `completed_scope=0.463004s`、`accepted_endpoints=0.000069s`、`benchmark_source=0.000116s`；整個單位從前次約 6 秒降到約 2.6 秒 CPU，仍含 Python 啟動和狀態掃描。這些是同夜不同負載的樣本，不是固定倍數保證；相鄰分鐘曲線與排程測試 **65 個通過**（含掃描中來源變更拒絕），Ruff 與 whitespace 檢查通過。

**截至 03:36 的來源等待（後續已解除，見文末 04:23 續修）**：TX 歷史 runner 9/23 14:31 因 `live_connection_reservation` 寫入等待收據，下次 9/24 05:00:10 才允許登入。這保留夜盤 FOP 三條與股票行情兩條連線；直接取消等待會觸碰每人五條連線與即時行情契約。另 `prepare_query_calendar` 的共用期貨完整日界線是 16:30，即使在 14:31 開放連線，也不會把當日 TX 視為完整。曾考慮 **僅 TXFR1、官方日盤確定收束、在 14:45 前硬停止的優先查詢**；後續確認基準重建其實可使用已留存的完整 FOP 一秒報價，因此沒有修改正在運作的連線排程。05:00 歷史工作仍須依自己的收據驗收，不能因曲線已用本機來源補齊就宣稱 TX 歷史 Tick 已下載。

## 9/24 03:41 OpenBB L1 正式續測

03:39:14～03:41:10 的既有 timer 正常完成：**115.955 秒 wall／109.613 秒 CPU**、19 個新段、0 stale／failed、41 視圖重用、1 重建、1 deferred、4,000,503→3,998,455 個來源 shard 待壓縮、`l0_deleted=false`。正式收據的來源契約 SQL **20.536 秒**、既有 L1 metadata **43.010 秒**、未指派來源 **36.956 秒**（其中 endpoint 內排序 30.992 秒）、建段 **3.396 秒**、view 發布 **5.153 秒**。發布後以正式 DuckDB 唯讀檢查 catalog、實際視圖集合與 SQL SHA-256：42 published／1 deferred 全部通過。與 03:05 輪的 129.959 秒、18 新段相比，來源 SQL 20.650→20.536 秒幾乎不變；metadata 60.719→43.010 秒下降，但同機快取與工作量不同，不把這 17.7 秒全歸於改動。cgroup 分段讀取 `memory.high=0`、swap 0；systemd 記錄峰值約 2.05 GiB，不能推定每輪都無壓力。下一個真成本仍是完整 metadata 驗證與 13m-task SQLite 未指派查詢；前述並行、`read_metadata`、索引提示的負面測速說明不應盲目增加執行緒或略過證據。

03:44 的全服務唯讀重盤收據 `artifacts/benchmarks/service-coverage-20260924T0342-post-openbb.json` 仍是 **50 service／39 timer／2 path**；當時唯一 `Result!=success` 的 StockAgent unit 是 `stockagent-crypto-training-refresh.service`。宿主 CPU、memory、I/O 的 10 秒 `full` PSI 均為 0，這只表示那段時間沒有可觀察的宿主級阻塞，不等於各 job 來源完整。Windows Caddy 工作顯示 Running，但最新排程結果非 0；必須以 HTTPS 實際可達性與 Windows 工作日誌分開驗證，不能直接宣稱其冷啟動鏈健康。

03:45 HTTPS 實際驗證：本機 gateway `/healthz` HTTP 200／約 1.6 ms、DDNS IPv4 HTTPS HTTP 200／約 293 ms、強制 IPv6 HTTPS 仍無法建立連線。這證明當次服務對 IPv4 用戶可達；沒有證明 Windows 工作的非零結果無害、重開機後可恢復、IPv6 可用或業務資料健康。

Windows 排程唯讀檢查顯示 `StockAgent Public Caddy` 為 `Running`、`MultipleInstances=IgnoreNew`、每分鐘有下一觸發，而 `LastTaskResult=2147946720`；這**符合**已執行的長駐任務遇到重複觸發時拒絕新實例的型態，但 Task Scheduler Operational 日誌在本次時窗沒有可用事件，故不將原因定論。未重啟 Caddy 或改排程；目前只驗證既有實例的 IPv4 可達，冷開機路徑仍需另測。

對 1,313 秒級 Yahoo US daily 修復程式碼的唯讀檢查發現一個未修的超時契約缺陷：`_run_parallel_symbol_downloads()` 先用 `as_completed(futures, timeout=None)` 等待工作完成，再呼叫 `future.result(timeout=repair_symbol_timeout_seconds)`；後者對**已完成**的 future 不會提供逐標的 wall-clock 上限。底層每次 Yahoo 網路呼叫另有 `_fetch_with_hard_timeout` 與 socket timeout，但整個標的的重試／合併不受這個宣稱的外層數值約束。不能因設定了 `repair_symbol_timeout_seconds` 就把供應商長尾風險視為已修復；應在下一輪以可中止的工作邊界、完整來源／檔案原子性及實測供應商回應驗收，不直接關閉外層 executor 而留下仍寫檔的執行緒。

## 9/24 03:52 Crypto 正式重試與冷發布範圍

在既有交易開盤保護時窗以外啟動正式 `stockagent-crypto-training-refresh.service`，03:52:18～03:53:16 的日表階段 **55.973 秒**；`data_bybit/perpetual_daily/materialize_summary.json` 證明 394／394 標的完成、0 failed，其中 383 尾段增量、11 首次全量。相較 02:30 的 157.187 秒／383 完成／11 failed，這次確實驗收了單標的有界重試與尾段增量的組合，但不同來源狀態及負載下不能把秒數比值宣稱為固定加速率。正式服務 systemd `Result=success`，**業務收據卻是 `deferred_source_changed`**：日表完成前後的 2,143 個輸入 metadata 簽章不同，只記下 funding 為已完成步驟，未做其後特徵報表、稽核與冷發布。`Result=success` 在此只表示「有證據地讓路」，不等於刷新完成；全流程仍未修復。

隔離測試先前失敗的 11 個標的，在另一輪原始分鐘採集中 11／11 可完成、0 failed；這不代替正式 394 標的與冷發布的驗收。根因是約每兩分鐘的原始寫入持續替換檔案，55 秒日表後的全鏈路不能靠固定排程保證 22 GB mutable 來源持續靜止。此部署的 ext4 對測試檔不支援 `reflink`，因此不能假設零拷貝 CoW 快照可用；短暫 `publish_ready=true` 也不足以讓完整打包／雜湊跨越下一次 writer。後續需要 catalog 約束的、可逐檔證明且不阻塞原始採集的一致來源版本／發布交接機制，不會略過 writer 閘門或冒充已發布。

另依現有「可重建 cache 不進冷 release」契約，catalog 的 Bybit 發布現在排除 `perpetual_daily/panel_cache_v2`。正式來源唯讀盤點：原 4,716 檔／23,380,394,831 bytes；選定發布 4,674 檔／22,282,215,603 bytes，少掃／少傳 **42 個可重建檔、1,098,179,228 bytes（約 1.02 GiB）**，原始 1m、funding、正式日表與收據保留。隔離小型冷 release 及 materialize／checksum 驗證確定資料保留、cache 排除；相關 packed、publisher、Crypto 測試 **25 個通過**、Ruff 與 diff whitespace 通過。這降低下一次成功發版的成本，**沒有發布新的 Bybit head**，也不解決 writer 競爭。

全服務唯讀測速清單現在對 Crypto refresh 額外讀業務收據，並以 systemd 單調啟動時間及本次觀察時鐘驗證它屬於**同一次**執行；過期的 completed 收據不會套到新的一輪。實際 `artifacts/benchmarks/service-coverage-20260924T0410-crypto-semantic.json` 仍列 **50 service／39 timer／2 path**，明確同時顯示 Crypto `Result=success`、exit 0、最近程序 wall 57.886 秒，及相符的業務狀態 `deferred_source_changed`。這補上「程序成功不等於資料完成」的觀測盲點，不假裝改善實際發版延遲；相關稽核／發布／Crypto **30 個測試**與 Ruff 通過。

同一個相符收據欄也接到既有每 5 分鐘長期服務趨勢（不另開 daemon 或無限成長的記錄檔）。04:08:42 正式 `all_service_runtime_sample` 已在 50 項服務中寫下 Crypto 程序退出 0 與 `deferred_source_changed` 並列，未把 deferred 當 completed；1 秒唯讀人工重測同樣辨識成功。完整相鄰回歸現在 **36 個測試**通過，含過期收據不可冒用、長期趨勢與冷 release 排除 cache；Ruff、diff whitespace 通過。長期測速仍只量 unit 資源與最近工作 wall，沒有把每一項業務請求 p95 宣稱已蒐集。

高頻全資料快照的 55,873 個檔、60,280 項 feature 凍結輸入測速：25.7 MB 快取 JSON `read_text+json.loads` 中位約 388 ms，僅用標準庫 `read_bytes+json.loads` 約 336 ms；路徑發現約 610 ms、單次 stat 約 304 ms、完整 feature 投影約 657 ms。正式 30 秒 worker 在 04:12 的兩輪分別約 3.27／4.86 秒，後者刷新 780 個 footer；凍結輸入的局部耗時不可直接相加或外推成正式收益。試作「未變資料組重用彙總」後，55,873 檔／234 組輸出 SHA 一致，但交錯測速原全彙總中位 **80.8 ms**、新驗證重用 **47.9 ms**，只省約 33 ms；已撤回這項額外複雜度，正式仍逐組核對，不以 33 ms 冒充 3～5 秒瓶頸已解。

上述測速時另外發現一個原有的觀測競態：檔案在目錄發現後、footer 更新前消失時，`refreshed_files=0` 原先可直接重用先前 `verified` 總數。現在只要本輪 stat 遇到不可讀檔，即禁止零變更快取短路、重新彙總並顯示 `scanning`／`count=null`；沒有把缺檔變成零列或已完成。以檔案在兩階段之間消失的回歸驗證；資料監控、服務趨勢、Crypto 發行相關 **106 個測試**、Ruff 和 diff whitespace 通過。這是正確性修復，幾乎不影響正常快路徑；真正的大宗 30 秒監測計算仍待優化。

同一 25.7 MB JSON 再做 8 次交錯 `read_text`／`read_bytes` 標準庫解析：值的 SHA 相同，中位 **317.9／309.9 ms**，先前 388／336 ms 受取樣環境影響，較可信的本輪局部收益僅約 8 ms。保留單行 `read_bytes` 以避免一次字串解碼／配置，但不將它視為顯著服務加速；70 個資料監控測試與 Ruff 通過。

## 9/24 04:23～04:40 當沖分鐘曲線來源閘門與實帳本續修

分鐘曲線預檢原先只接受當日 `TXFR1` 歷史 Tick 收據，但正式基準重建器優先接受帶 `complete` manifest、實際開盤報價與完整分鐘覆蓋的本機 FOP 一秒報價。這造成 9/23 已有來源卻被拒絕，空等 9/24 05:00 歷史排程。現在預檢呼叫與正式重建器同一組讀取／檢查函式，先驗留存 FOP capture，必要時才驗 receipt-backed Tick，並將兩者根路徑明確傳給子程序。9/23 實檔為 `TXFJ6`，3 份完整 manifest、**300／300 個分鐘點都是當分鐘新鮮報價**；單次預檢約 **2.08 秒**，沒有新增永豐登入。這解除來源等待，但不代表全部基準及策略已修好。

第一次正式 `--no-fetch` 重建成功補出 **145 個交易日、44,080 筆基準標記**，卻在策略階段因 9/23 `tw_day_trade_multi_basis:6168` 的持倉 35,000 股與僅讀原始 `entry` 18,000 股不符而 fail-closed。帳本另有使用者先前授權、明示 **非券商成交** 的 `entry_completion` 17,000 股，同價 42.65；重建器錯把它當出場。現將原始進場與明示補單相加驗持倉與均價，分鐘估值及攜倉稽核也把它視為有揭露的庫存增加，不冒充交易所撮合；缺原始進場、價量不一致或稽核缺補單證據仍拒絕。相鄰分鐘／攜倉／稽核 **64 個測試**、Ruff、whitespace 通過；未改原始 append-only fills。

第二輪低優先權、`--no-fetch` 正式流程 04:40:05 收據為 `ready`：**145 日 × 5 模式 × 每日 270 點 = 195,750 個策略分鐘點**，09:01 開盤及盤中未驗證來源點均為 0；所需 **52,680 個標的／日期組**本機可用，缺口 0、API 請求 0。公用 gateway 的 9/23 `1m/v2` 實讀有 8 序列：台指 300、0050／2330 各 271、五個策略各 270 點，`historical_minute_missing_price_points=0`；持倉頁 6168 顯示 35,000 股、42.65 元，`entry_completion_broker_fill=false` 與補單契約仍在。這些證明資料顯示與來源覆蓋，不是券商成交證據。

**尚未完成的正確性與性能邊界**：重建收據記錄 9/23 五模式合計 **1,185 個**新舊分鐘權益差異點，最大約 **NT$48,513.82**；本輪未要求獨立估值 parity，故 `ready` 只代表指定的來源／點數／價格契約通過，不能宣稱舊曲線數值逐點相同或已釐清全部差異。`audit_tw_day_trade_margin_replay` 正式回放收據只到 9/22、即時 append ledger 已有 9/23，直接拿它審完整 live 根會因範圍不一致拒絕；需要分離正式回放與即時追加範圍才可得到可解釋的全鏈審計。基準重建第二輪即使 `projection_changed_sessions=[]` 仍花約兩分鐘並重寫總檔；策略全期重估從約 04:33:25 跑到 04:39:44，約 6 分鐘，峰值程序 RSS 約 3.8 GiB、無本程序 swap。這是下一階段增量化與熱點量測的明確目標，不能靠刪除分鐘點或跳過來源驗證。維護收據已新增 `benchmark_rebuild`、`minute_curve_rebuild`、`post_validation` 分段秒數供下輪正式執行驗收；04:40 這次程序在加欄位前已啟動，因此本次收據沒有該新欄。

後續在來源與帳本不變的 `--no-fetch` 正常 no-op 路徑分段量測：未修前端點檢查約 **1.76 秒**；`ready` 收據保存已驗證的 `device/inode/size/mtime_ns/ctime_ns` 與日期／模式指紋後，相同來源下約 **0.0001 秒**，來源任何可觀測異動仍全掃並檢查掃描前後簽章。又發現原流程先用完整 221 MB marks 帳本完成帶輸出 SHA-256 的 270 點／日檢查，隨後再讀它作開盤與盤中價格來源檢查；現在只在前者已驗證所有日期／模式、明確重估 09:01、全部點數齊且來源缺漏為 0 時重用其結論，否則仍走獨立逐筆掃描。相鄰正式 no-op 的 `strategy_price_provenance` 約 **3.15 秒 → 0.000021 秒**，整段 existing validation **10.71 → 4.67 秒**；它們是不同時刻單輪樣本，不能直接聲稱固定提升率。分段計時保留在 `artifacts/operations/tw_day_trade_minute_curves/latest.json`。

基準驗收過去每次反序列化約 **294 MiB** 的總檔；現在先核對總檔 SHA-256、分區 head 的來源 SHA、每個所選分區的 gzip 內容雜湊與行數，再對約 **7.9 MiB** 的正式投影執行相同三基準逐分鐘網格檢查；head／分區缺失或損壞則回退總檔，不會以未驗證快取放行。一次唯讀實檔量測總檔 SHA 約 **0.183 秒**、145 個分區載入驗證 **0.727 秒**；相鄰 no-op 的基準驗證約 **4.59 → 1.46 秒**，但單輪負載有變動。投影快路徑與損毀回退、策略及維護相鄰測試 **71 個通過**。正常 no-op 最近一次仍約數秒，其中完整策略帳本驗證約 3.2 秒、FOP 來源檢查約 3.5 秒，沒有宣稱已到極限。

基準建置器也不再因**僅** live `state.json` 的無關欄位 SHA 改變而重寫語意完全相同的總檔；市場來源、origin 或任何 mark 變動仍重寫。這避免獨立重試時的約 294 MiB 無效輸出與下游失效，但完整日間基準重建本身仍可能花數分鐘。注意維護流程的先建稀疏官方開收基準、再展開完整股價分鐘曲線是**真正不同**的兩份內容，該必要轉換仍會改總檔；不能把這項無效寫入修正說成已消除所有重寫。相關基準與投影測試 **54 個通過**。

04:58 再跑跨模組聯合回歸：當沖基準／分鐘曲線／攜倉稽核、全資料監控、服務測速、冷資料發布共 **175 個測試通過**，Ruff 與 diff whitespace 通過。同期服務盤點仍是 **50 service／39 timer／2 path**，systemd 最近結果均為 success；但 Crypto 業務收據仍為 `deferred_source_changed`，當沖公用 API `health=degraded`，所以不以程序結果宣稱全部資料或交易服務已正常。

## 9/24 05:00 歷史永豐共用登入與全掃成本

05:00 後同時有股票分鐘回補、通用期貨／選擇權歷史回補與 TX 連續期貨歷史回補。股票分鐘工作依五條帳號連線契約使用兩個 worker，並**沒有**持通用／TX 共用的 `login.lock`；TX 的 `history_login_slot_busy` 是通用歷史工作持鎖，不能把等待誤歸因於股票分鐘工作。通用歷史工作首輪發現 5,731 個合約、3,954 個 pending 查詢；其 500 筆有界批次於 05:00:15 啟動，05:08:43 才產生終端摘要，TX 每 60 秒仍留有等鎖收據。日誌中第 99→100、199→200、299→300、399→400、499→500 筆的間隔約 40～49 秒，與 `_write_summary` 每百筆對完整收據樹做驗證相符；實際單次稽核秒數需由新版分段日誌驗收，不能把全部空隙當作純 CPU。

已在通用歷史 runner 加**收據條件的批次公平讓位**：若 TX 的等鎖收據為最近 120 秒內的 `history_login_slot_busy`，通用歷史批次結束並釋放鎖後讓出 75 秒，足以跨過 TX 的一次 60 秒重試；沒有等待者仍只間隔 30 秒。每人五條連線、即時 FOP 與股票報價保留、07:45 歷史查詢截止及 90% 流量上限均不變。下載器另在達到 `--max-queries` 且必定立刻進終端稽核時，省掉重複的「中途」全掃；每百筆非終點與終端的正式收據稽核保留，並在日誌增加 `summary_audit elapsed_seconds`。這是排程／重複驗證的局部優化，不是減少市場查詢、放寬缺漏或假設 TX 資料完成。相關 Shioaji 排程／收據測試 **38 個通過**、Ruff、shell 語法與 diff whitespace 通過。

部署界線：在第一輪無下載子程序的 30 秒批次間，曾 `systemctl stop` 通用歷史 service；啟用中的 `Persistent` timer 隨即於 05:08:57 再啟動，並重新做一次庫存發現。這次切換未讓 TX 先取得鎖，且造成額外庫存成本；因此不再於活躍查詢中重啟／停用 timer。新的 runner 讓位邏輯由 05:08:57 新主行程載入；第二批 500 筆完成後，05:16 正式寫出 `yield_to_waiting_tx_history`，TX 於 **05:16:26** 開始自己的 64 日期批次，證明這項公平讓位在實際共用鎖競爭下生效。TXFR1 的 **2026-09-23** 45,197 筆 Tick 收據於 05:16:47 顯示 `complete`、`session_finalized=true`、實際 TXFJ6 合約，1,169,462-byte Parquet SHA-256 與收據逐位元一致；TXFR1 本機歷史顯示 1,586／1,586 日期已解析。這是該合約／日期的資料證據，**不**代表全期貨產品、整批 64 日期或冷發布已完成。下載器「不重複終端全掃」須等下一個 Python 批次子程序才會載入；其分段稽核秒數、總 wall 與 TX 全批終端收據仍待驗收。

同一 05:16 TX 批次在完成 64 次查詢後仍逐一稽核 773 個 alias × 最多 1,586 個日期，05:24 已掃到第 500 個 alias 且仍持登入鎖；這些剩餘掃描不會增加本批 API 取得量。新版在有界查詢額度耗盡時寫 `batch_partial`、`coverage_scope=scanned_contracts_only`、實際已掃與總合約數，隨即退出釋放登入；只有全目錄掃完才可寫 `batch_finished` 並進入正式發布門檻。下一批由已驗證的逐日收據續跑，沒有把未掃合約當作零缺漏。部分批次以 60 秒重檢；若通用歷史工作正等鎖則先讓位 75 秒，以避免反向飢餓。Contract V2 庫存刷新維持每小時一次，其間只讀已保存目錄，市場歷史查詢仍受 07:45 截止和 90% 流量上限保護。此 TX 變更**尚未載入目前正在執行的舊 Python 子程序**，需在其完成後再部署與驗收。

通用歷史 `_valid_receipt` 增加最多 131,072 筆的程序內驗證快取，以收據與資料檔的 `device/inode/size/mtime_ns/ctime_ns` 作失效條件；首次仍實際讀收據與核對資料 SHA，後續相同檔案重用，掃描中檔案變更拒絕，回傳拷貝避免呼叫端污染。2,000 個正式 futures KBar 收據的獨立唯讀樣本，首次 **1.089 秒**、再次 **0.047 秒**、兩次 2,000 筆狀態 SHA 相同，該程序最高 RSS 約 **106.5 MiB**；此樣本不等於 11 萬個收據的整批正式 wall 或記憶體保證。下一個通用歷史 Python 子程序才會載入快取與 `summary_audit` 分段計時。相關 Shioaji **39 個測試通過**，包括檔案改動、刪除、掃描途中替換與部分批次不可發布；Ruff、shell 語法、whitespace 通過。

## 9/24 05:28～05:42 新批次驗收與跨服務冷啟動

05:28:50 僅在 TX 舊批次子程序已完成、通用歷史持共用登入鎖時，重新載入 TX runner；沒有重啟即時 FOP、股票行情、Discord 或當沖引擎。新版 TX 05:33:56 取得登入、05:34:47 在 64 次查詢額度耗盡時寫 `status=batch_partial`、`coverage_scope=scanned_contracts_only`、**22／773** 個 alias 已掃後釋放鎖；`missing_dates_after_batch=0` 僅是這 22 個的部分視角，`current_query_sweep_complete=false`，不能拿來發布全目錄。通用歷史 05:35:41 重新取得鎖，雙向讓位都已現場觀察。TX cgroup 曾顯示約 7.2 GiB memory，但子程序退出後 `memory.stat` 約 6.7 GiB 是可回收 file cache，不是仍有 7 GiB Python heap；須分開記憶體來源與實際宿主壓力。

通用歷史新版 500 查詢批次的正式完整摘要稽核：query 1／100／200／300／400／500 分別 **13.946／21.697／11.220／11.996／10.758／14.162 秒**；第 500 筆已是 `kind=final`，沒有重複做中途摘要。下一批 query 1／100／200／300／400／500 則約 **12.577／19.621／12.882／14.851／19.575／12.755 秒**，表明冷熱快取與並行負載使絕對秒數浮動。唯讀 cProfile 對 5,731 個合約的完整摘要，一次約 62.141 秒（有額外 profiler 開銷與其他服務競爭）；其中 `_valid_receipt` 呼叫 305,022 次、`stat` 約 1,009,814 次，`build_tasks` 又重掃來源，正式非 profiler 的 10～22 秒也有相同結構。每筆查詢已有獨立 `PersistentProgress` 收據；因此後續新 Python 批次把昂貴的全樹中途摘要由每 100 筆改每 **250** 筆，仍在第 1 筆與批末做完整、fail-closed 稽核，不減少查詢或收據驗證。這項新 cadence 尚待下個子程序實跑；不能把 profiler 62 秒當成既有正式耗時。

另一個跨服務成本來自「僅需退佣時點規則」的執行設定卻連帶載入 PyTorch。現在把純字串／日曆規則置於 `stockagent.backtest.tw_commission_rebate_policy`，原 `tw_commission_rebate` API 仍原物件轉出，訓練的 Torch 計算函式與語意未改；正式基準重建器把只在 `main()` 使用的模型設定匯入延後，讓當沖分鐘曲線預檢不載入訓練 runtime。新 Python 程序的單次匯入量測：`tw_day_trade_simulation` **1.350 秒／約 529 MiB → 0.346 秒／約 68 MiB**；`maintain_tw_day_trade_minute_curves` **3.508 秒／約 577 MiB → 0.446 秒／約 111 MiB**。同一份實帳本以直接 `--no-fetch` 再跑顯示 `ready/no_op_already_complete`、195,750 個策略分鐘點，**6.87 秒 wall、約 399 MiB 峰值**；其中最新基準來源預檢 2.191 秒、策略驗證 2.403 秒、基準驗證 1.238 秒。這些不是與 05:30 嚴重冷 I/O 競爭下 67 秒 systemd 執行的同條件 A/B 比較，不能宣稱全程固定快 10 倍。交易、基準、曲線、退佣、Shioaji 相鄰 **286 個測試通過**，新增的「乾淨子程序匯入不可載入 Torch」與原 API 身分回歸亦通過（後續 57 個測試）。

05:40 全服務唯讀收據 `artifacts/benchmarks/service-coverage-20260924T0540.json` 仍為 **50 service／39 timer／2 path**、systemd 最近結果沒有失敗；10 秒取樣中 Shioaji 通用歷史約 0.595 core、packed backup 0.100、Discord 0.057、當沖模擬 0.054。宿主 memory `full` PSI 10 秒約 0.83%，這是取樣時的競爭，不是任何單一服務固定成本。Windows Caddy 冷啟動、外部 IPv6、Crypto 冷發布、當沖實際成交與 50 服務所有業務收據均未因上述 CPU 優化自動變為完成。

`build_tasks` 的 Tick 目標原先在 KBar 已驗證一次後，又呼叫 `observed_tick_dates` 逐塊重讀同一批收據。現在在原有 `_valid_receipt` 驗證迴圈直接收集 `observed_trading_dates`，原有官方活動日期聯集、來源缺漏與任務排序不變。正式 5,731 合約、79,143 個觀察日期的唯讀對照：新版規劃產生 1,057 個當下待查任務、單次約 **30.708 秒**（與其他 worker 競爭）；舊式**額外**重讀相同已驗證 KBar 日期另花 **2.124 秒**。這 2.124 秒是可刪除的獨立遍歷，不可和不同負載的正式摘要耗時直接相減。單次 KBar 驗證次數及 Tick 目標日期的回歸、Shioaji 排程共 **41 個測試通過**，Ruff、whitespace 通過；待下一個自然啟動的 Python 批次觀察正式耗時。05:45 前仍為舊程序，但 64-query TX 批次已再次正常讓位與續跑，未見共用登入鎖飢餓。

05:46:23～05:49:22 的下一個**自然啟動**通用歷史批次已載入新規則，正式只在 query **1／250／500-final** 寫出全樹稽核，分別 **16.612／10.592／12.226 秒**；仍完成 500 個查詢並保留逐查詢進度。相鄰上一批約 4 分 4 秒，這批約 2 分 59 秒，但合約／待查內容、快取與其他服務負載不同，**65 秒差額不是可歸因的固定加速保證**。05:45:46～05:46:14 的 TX 64-query 批次亦再次退出讓位，沒有重新登入即時 FOP／股票服務。

衍生品成本物件的另一處共用冷啟動耦合：`stockagent.config` 原為了讀 `FuturesCostSchedule`／`OptionDayCostSchedule`，直接匯入包含模型 Tensor 計算的模組，乾淨程序 **1.482 秒／約 563 MiB** 且載入 Torch。依既有衍生品契約只抽出純費率 dataclass 到輕量 policy，舊模組 re-export 同一類別並保留 pickle 舊路徑，原預設、驗證、交易時鐘與 checkpoint 語意不變。新版 `stockagent.config` 匯入 **0.311 秒／約 82 MiB**，`scripts.run_tw_day_trade_simulation` 匯入 **0.322 秒／約 111 MiB**，均未載入 Torch；這是冷匯入局部量測，不能套用為整個訊號延遲的下降。正式衍生品測試與舊類別序列化回歸仍在進行中，未重啟任何盤中引擎。

上述衍生品／配置／舊 class path 測試正式完成 **102 個通過**（74.56 秒）；新測試明確覆蓋兩種費率類別的原 API 物件身分、pickle 還原、預設與無效值，以及乾淨程序匯入 config 不得帶入 Torch。Ruff 與 whitespace 通過。這項抽離不改資料集、模型輸出、決策時間、收費公式或 checkpoint 指紋；技能流程的實資料完整分區與 fold 驗收適用於新增衍生品交易模式，本輪純載入邊界重構沒有宣稱完成新的訓練或策略表現。

05:51 本機公開 gateway `/healthz` 為 HTTP 200／約 1.8 ms、TAIFEX 面板 `/healthz` 約 44 ms；但公開全資料摘要仍是 **critical**（327 個啟用端點：225 complete、37 catching up、65 unable），當沖 `/tw-day-trade/api/status` 仍 **degraded**。這些讀取只證明本機服務可達與目前來源健康分級，不證明公網 IPv6、交易成交或缺漏已消除。

## 9/24 05:54～06:03 公開路由與冷請求續測

全公開閘道 **39 條路由**以單連線／每路由兩次穩態取樣，回應錯誤 0；收據 `artifacts/benchmarks/all_services_latency_2026-09-24_0554.json` 保留首請、Server-Timing、HTTP 傳輸與客戶端 JSON 解析的分離耗時。此小樣本不是 p95 SLA。最慢首次請求是完整當沖 1m/v2 歷史 **8,660 ms**，其中服務端 `build` **8,528 ms**、客戶端解壓後 body 約 129 ms、額外 JSON 解析約 286 ms；隨後同端點 4 次重測首次 83 ms、穩態中位約 98 ms。來源 04:39 重建後，分段投影 head 在 05:52 首位請求才更新，顯示冷建置被轉嫁給訪客，不能以熱快取速度冒充整體體驗。

離線測速腳本未設正式 `STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR` 時，雖聲稱可用 immutable session shards，實際退回完整來源掃描：**4,128 ms、317,840 點、8 序列**。修正測速收據，現在記錄實際索引目錄和分段投影可用性；以與公開服務相同的索引目錄重測、略過最終完整投影但保留已驗證分段，僅 **490 ms**，輸出 SHA-256 與全掃相同。這兩次不是同一 cold OS cache 條件，不能把差值歸於單一演算法；重點是工具不得誤標「分段命中」。

已把完整歷史的**盤後預熱**接在分鐘曲線與三基準通過正式驗證之後：只向固定 `127.0.0.1` 公開唯讀 gateway 讀取完整 1m/v2 曲線，不登入永豐、不觸動交易；最多等 20 秒，結果在維護收據記為 `warmed` 或 `not_warmed`，預熱失敗不會冒充曲線來源失敗。現有資料的直接整合呼叫 HTTP 200、14,720,478 bytes、約 **41 ms**（已熱）；尚未等到下一次**真正來源變更後的正式維護**來驗證它能否把首位訪客 8.66 秒轉移到盤後工作。67 個測速／曲線相關測試通過。

全資料欄位清冊首次 HTTP 約 **1,472 ms**，其中服務端建置約 1,343 ms；37,366,561 decoded bytes／774,195 gzip bytes，瀏覽器仍需解析及處理約六萬筆。凍結來源單步量測：讀檔 33 ms、JSON 解析 511 ms、對已精簡來源再序列化 **441 ms**、gzip 66 ms、SHA-256 18 ms，輸出與來源 bytes 相同。公開閘道現在維持完整 JSON 解析、唯讀／不可控制契約及非有限數拒絕，並要求開檔前後與路徑的 inode／size／mtime／ctime 一致；直接使用已驗證來源 bytes，刪去第二次 37 MB 序列化。隔離本機新流程首建 **414 ms**、熱命中 0.09 ms。公開 gateway 於 06:02 **單獨重啟**載入變更後的實際首次 HTTP 864 ms、後續 3 次中位 111 ms；這些受 60,280 筆客戶端解壓／解析及並行負載影響，不宣稱 414 ms 是公網端到端保證。公開／測速／分鐘曲線聯合 **159 個測試通過**，Ruff 與 `git diff --check` 通過。

部署後本機五條關鍵路由 `/healthz`、overview、欄位清冊、當沖完整曲線及 status 共 0 錯誤，當沖完整曲線首次約 90 ms（重啟預熱已載入既有投影）。DDNS IPv4 HTTPS `/healthz` 為 HTTP 200、約 0.42 秒；IPv6 同端點約 3.49 秒後連線失敗。全資料摘要仍 **critical**（225 complete、37 catching up、65 unable），當沖仍 **degraded**；此輪沒有修復外部 IPv6、來源缺漏或模擬／券商成交差距，也沒有重啟即時交易或報價服務。

06:04 完整重測 39 條 HTTP 路由再次 0 錯誤，收據 `artifacts/benchmarks/all_services_latency_post_deploy_2026-09-24.json`。其中跨 2/25～9/24 的訊號／事件首次仍約 **1,562／1,598 ms**，剖析顯示有界 100,000 筆帳本暫存欄式投影與事件累計為主要成本；這不是熱命中約 2～3 ms 能取代的冷路徑。正式 API 的 `scan_limit_reached=true`：訊號返回 `total=100000`、事件 `total=199506` 只是掃描範圍內統計，UI 已警告較早紀錄未計入。不能為降秒數再降低上限，更不能宣稱跨日明細已含所有歷史；下一步需要依日期分區的精確索引／分頁與真實總數契約。

公開 HTTPS 真正 Chromium 驗收分別以 1366×768 筆電與 390×844 手機跑完整 8 頁，原始報告 `artifacts/benchmarks/public-browser-20260924-1366x768.json`、`artifacts/benchmarks/public-browser-20260924-390x844.json`。兩種視窗全部 8 頁皆無全頁水平溢出、JS console error；筆電沒有表格溢出，手機觸控目標風險清單為 0。單次當沖筆電測得初次資料 request-to-paint opportunity 約 109 ms、完整 317,840 點歷史選取至繪出約 637 ms（API 本身約 275 ms，畫圖準備約 36.3 ms）；這是當下公網瀏覽器個例，不是每種裝置／網路的 SLA。桌面 audit 的自動觸控尺寸規則在一些密集按鈕仍報風險，但手機版對應規則為 0；後續如改善滑鼠密集控制，需保持筆電一眼可見而不增加橫向捲動。

06:09 全服務再盤點 `artifacts/benchmarks/service-coverage-20260924T0609.json` 仍為 **50 service／39 timer／2 path**，systemd 最近程序結果均非 failure；正在執行的註冊資料盤中服務與永豐通用歷史取樣各約 **1.047／0.992 CPU core**，屬當下真實來源工作而非可直接刪掉的背景忙等。Crypto refresh 最近程序雖退出 0，同輪業務收據仍是 `deferred_source_changed`。該短取樣宿主 memory full PSI10 約 0.32%、I/O full PSI10 約 0.15%，因此單端點冷耗時比較仍要標註並行下載負載。不能以 50 個 unit 的 `Result=success` 冒充所有資料、憑證、冷發布與開盤交易鏈健康。

## 9/24 06:12～06:16 高頻欄位清冊重用成本

30 秒一次的資料監控 worker 在 `refreshed_files=0`、欄位清冊可重用的正式輪次，`feature_projection` 仍約 **562～922 ms**；單獨對當下 37.4 MB／60,280 欄來源呼叫舊重用函式約 **533 ms**，主要是每輪重讀並建構整個 JSON 物件，只為取 `len(rows)`。現在每次正式欄位清冊原子發布後寫一份小型私有重用收據，記來源 `device/inode/size/mtime/ctime`、SHA-256 與實際欄數。重用輪先查依賴 mtime，再對來源串流核對完整 SHA-256 與開檔前後身分，只返回欄數；收據遺失、損毀、來源改動或依賴更新均回退完整 JSON 解析／重建，沒有把欄位缺漏視為零。自然排程已在 06:15 產生新收據；同來源直接重測重用路徑約 **23.8 ms**，不是整個 worker 加速量。資料監控／公開閘道相鄰 **163 個測試通過**，含收據損壞、來源替換、非法 JSON 與依賴更新；Ruff、diff whitespace 通過。活躍加密貨幣下載仍使每輪有數十至上千個 footer 更新，正式 `feature_reused=false` 時這條快路徑不會命中，不能宣稱每個 30 秒輪次都會省 0.5 秒。

後續自然排程的真正重用輪 06:16:43／06:18:44 正式收據分別顯示 `refreshed_files=0`、`feature_reused=true`、**60,280 欄不變**，`feature_projection` **20.478／21.297 ms**，整輪仍約 2.046／1.988 秒，主要留在 55,873 檔 inventory 檢查與公開投影，故不能把單階段節省冒充總輪降至 20 ms。私有重用收據後續另加入覆蓋欄數與來源雜湊的自身 SHA-256，以防欄數位元損壞但來源雜湊仍正確時報錯數；v2 收據已於 06:19 由正式排程產生，06:22:15／06:22:45 兩次自然重用輪同樣保留 **60,280 欄**，`feature_projection` **45.975／33.990 ms**，整輪 **2.591／2.193 秒**。凍結來源剖析顯示 `build_feature_inventory` 的 60,280 欄中 **53,966 欄**屬 FinLab 已下載寬表，且真正來源變動時仍需約 1～1.7 秒重建；下一階段考慮按資料集分區的精確投影，但不能用全域舊快取掩蓋其他來源的新 footer。

06:24 唯讀剖析全資料公開投影的首次呼叫約 **3.225 秒**（含其自行讀取庫存），最大子階段是冷 release 清冊重新驗證 **1.639 秒**，Shioaji 公開狀態約 **0.514 秒**，單獨讀取現有記錄庫存約 **0.347 秒**。正式 30 秒 worker 已傳入同輪庫存並使用有界、按 release 身分／每小時再驗證的冷清冊收據，因此不能將這個孤立首次呼叫直接套作正式每輪耗時。後續任何縮短都須保持冷清冊 SHA、head／manifest 身分及來源健康分級，不能只看程序的 `active`。

## 9/24 06:27～06:30 永豐監控讀檔與全服務續測

`build_shioaji_public_status` 的唯讀剖析顯示一次涉及約 1,091 份本地 JSON 收據；既有讀檔快取每份先 `Path.resolve()`，合計約 **194 ms** 花在重複解析實體路徑。改成以絕對路徑作快取鍵，仍每次核對 device／inode／size／mtime，另增 ctime、讀後再次核對；連結目標替換或同大小改寫會重讀，讀取中途來源變動則回報不可用，不把舊版當作當下狀態。修改後在同一程序內每次清空讀檔快取、連續五次真實本機監控投影為 **439、290、283、283、319 ms**，中位約 **290 ms**；修改前只有單次剖析 577 ms，負載不同，不能宣稱嚴格 A/B 的倍數。永豐／測速／資料監控相鄰 **92 個測試通過**，Ruff 和 diff whitespace 通過；公開閘道已單獨重啟載入新版，`/healthz`、永豐、全資料及當沖 status 本機 HTTP 皆 200。永豐仍 `waiting`、全資料仍 `critical`、當沖仍 `degraded`，沒有重啟券商連線或交易服務。

06:30 再次唯讀盤點仍為 **50 service／39 timer／2 path**、沒有 systemd failed unit；收據 `artifacts/benchmarks/service-coverage-20260924T0630.json`。五秒取樣中 OpenBB L1 compaction 約 **0.627 CPU core**，宿主 I/O full PSI10 約 **7.79%**，代表當時真有 I/O 競爭，不能以空閒快取測速推論盤中延遲。Windows Caddy Task Scheduler 的 `LastTaskResult=0x800710E0` 與 Caddy 程序同時存在；這個結果不能單獨證明冷開機復原成功，仍需真正重開機後驗收。重啟公開閘道後 DDNS IPv4 HTTPS `/healthz` **200／約 37 ms**，IPv6 同路徑仍約 **3.32 秒後連線失敗**；雙棧未完成。跨日訊號／事件首次約 1.6 秒且 10 萬筆上限未解，仍是下一個架構性改善目標，現有日索引不足以直接宣稱完整歷史查詢。

06:35 公開閘道載入新版後再次遍歷 **39 條路由、每條 2 次穩態請求，HTTP 錯誤 0**；逐路由原始分段測速見 `artifacts/benchmarks/all_services_latency_2026-09-24_0635.json`。高 I/O 競爭下跨日訊號／事件首個觀測分別 **2,694／2,650 ms**，欄位清冊 **1,252 ms**；各自後續小樣本穩態中位約 **6.55／4.36／131.74 ms**。這些 first-observed 不是清除 OS／應用快取後的 canonical 冷重建，也不能從兩筆樣本報真 p95。跨日查詢的精確總數及可訪問歷史仍受 10 萬筆限制，不能以熱命中掩蓋。

## 9/24 跨日訊號投影分片可行性

正式帳本唯讀盤點：`signals.jsonl` **5.096 GiB／145 個交易日**，每日日索引段中位約 **36.02 MiB**；抽樣三天的既有欄式投影各有 **13,727 行**，逐日讀取約 **50～120 ms**（首天受 OS cache 影響）。其中 9/23 的 31.18 MiB 原始段轉成欄式表後約 4.05 MiB，zstd Parquet **0.118 MiB**，隔離寫／讀約 **7.39／6.66 ms**，讀回逐值相等。這支持「依交易日 immutable 投影分片＋當日增量」方向，但目前**沒有**把一次實驗的臨時 Parquet 冒充正式持久索引，也沒有取消 10 萬筆 API 上限；需要先實作來源段位移／inode／欄位 ABI 驗證、原子發布、歷史精確總數與篩選語意、異常回退，再做新舊輸出逐欄比較與公網重測。

六組相鄰回歸（資料監控庫存、資料監控投影、公開閘道、永豐監控、測速器、Yahoo 狀態）合併執行 **267 passed／21.41 秒**，沒有把各次有重疊的 163、92、64 個測試直接加總成測試數。

## 9/24 06:43～07:00 跨日事件完整查詢與投影快取

先落地事件帳本（`orders.jsonl`、`fills.jsonl`）的**逐交易日精確分片**：只從既有 append-only ledger 的日期位元組索引讀取，將 API 所需欄位投影為可重建的私有 zstd Parquet；每片記來源 dev／inode、位元組 spans、觀測 size／mtime 與 Parquet SHA-256，原子發布。後續追加其它交易日不需重新讀舊日；同日追加、原檔替換、快取損毀則重建／回退帳本。多個日期按原始位元組順序合併；不連續日期段與正式 overnight overlay 仍退回原有逐列路徑，避免重排事件。無正式 overlay、無 Unicode symbol 搜尋時，以欄式去重、精確計數及只物化請求頁取代 39 萬次 Python 逐列轉換。這是衍生索引，不是新的交易帳本或成交證據。

正式 2/25～9/23 事件為來源實掃 **283,927 個 order／107,271 個 fill 行**，去重後 **282,935／107,113 個，合計 390,048**；原先每來源最多 100,000 行的 `scan_limit_reached=true` 不再代表完整資料。以原有無上限嚴格路徑作同時段對照，頁面 100 筆、總數、各事件數、來源掃描數、日期及 `has_more` 全部逐欄相等。低 I/O 優先權的離線新索引首次建置 **8,453 ms**（比原無上限掃描 5,691 ms 慢，沒有冒充冷啟動提速）；建好後逐次清空頁面記憶體快取，完整查詢 **599～844 ms**，對照無上限原路徑約 **4,794～5,691 ms**。145 天兩種事件共 290 個 Parquet 分片，私有 cache 占用約 **9.4 MiB**，原始事件約 0.303 GiB 保持不變。相鄰當沖／overnight 回歸 **157 passed**，涵蓋追加、同大小原檔替換、快取損毀、重複事件、0 數量與分頁；Ruff 與 whitespace 通過。

公開閘道單獨重啟載入新版後，本機首個完整跨日查詢 HTTP 200、服務端 `build` 約 **889 ms**，回應 `total=390048`、`scan_limit_reached=false`，下一次約 **5.5 ms**（頁面快取命中）；公網 IPv4 HTTPS 同路徑 HTTP 200／約 **191 ms**，仍非真 p95。5 次小樣本 route benchmark 中位約 **1.76 ms**，原始收據 `artifacts/benchmarks/event_projection_2026-09-24.json`；不能拿頁面熱快取取代首次 0.9 秒。當沖健康仍 `degraded`、全資料 `critical`、永豐 `waiting`，沒有重啟交易／報價／Discord。**跨日訊號仍維持 100,000 行上限**；其 5.096 GiB 來源需同樣的逐日精確索引，再處理當前訊號摘要與排序的完整歷史語意，不能用事件頁修好就宣稱全部明細完成。

後續檢查發現事件 API 本身仍限制 `offset≤100000`：雖然總數已正確，較深的頁無法查。已讓完整投影路徑把兩種事件在欄式表內排序後直接切出 `offset/limit`，不再把前 39 萬筆轉成 Python 字典；事件路由仍有明確 1,000 萬 offset 上限，訊號路由不變。對 390,048 個真實去重事件，第 0／100,000／200,000／390,040 筆起的頁面與完整來源掃描逐筆相同；欄式路徑約 **1,230／741／624／628 ms**，相同頁的原逐列路徑約 **2,972／8,310／14,757／29,327 ms**（同一輪但順序負載不同，不能宣稱固定倍數）。當沖、overnight、公開閘道 **249 個測試通過**，Ruff、whitespace 通過。此時程式及路由已修改，仍待公開閘道重新載入與實際 HTTP 深頁驗收。

07:05 後公開閘道單獨載入此事件修正，本機 `offset=200000&limit=2` 回應 2 筆、`total=390048`、`scan_limit_reached=false`，IPv4 公網同一路由 HTTP 200／約 **450 ms**。這是實際深頁可達證據；本輪沒有重啟交易、報價或 Discord。

## 9/24 07:06～07:36 跨日訊號完整來源與記憶體邊界

訊號帳本正式來源為 **145 日／1,990,415 行／5.096 GiB**，日期索引所有日期各一個連續 byte span。逐日私有 Parquet 投影在原 ledger 異動或同日追加時按既有收據失效；完整 145 日首次建立新版投影約 **12.41 秒**，不能拿後續熱分片速度冒充原始冷重建。直接把所有欄位拼接、逐筆 Python 計算摘要的原型雖能報真總數，但約 **19.45 秒、3.4 GiB RSS**，沒有部署。改由 Polars 對目前訊號 ID 做完整範圍篩選，以欄式彙總目標／實際多空、開盤價格覆蓋、未成交原因，只排序請求分頁前綴；保留 Python Unicode `casefold` 的標的／名稱搜尋契約，但只對兩個字串欄運算。三交易日實帳本的全部／blocked／單模式／深頁結果及中文搜尋，與原有逐列路徑的頁、總數、方向、開盤稽核逐項相同。沒有以少算來源換取速度。

比對公開 DTO 時發現原投影漏了 `target_unsubmitted_shares`，且省略僅供後端 feature-driver 查詢的 `signal_source_path`，使快路徑未能附上特徵解釋。兩欄已進入私有投影，但長路徑字串只在請求頁逐筆還原，避免完整框架膨脹；公開 sanitizer 仍不對外暴露來源路徑。三日實帳本的公開輸出與原始逐列路徑包括 feature drivers 全部相等；相鄰回歸加入直接來源路徑及未送出股數。完整期 `offset=200000` 的離線新版約 **2.23 秒、2.55 GiB RSS**；這是獨立程序，不是正式端到端 SLA。`offset=1900000` 仍可正確分頁，極深頁的記憶體較高。

公開 gateway 只單獨重啟後，本機 `offset=200000&limit=10` 第一次 HTTP 200／約 **3.89 秒**、回傳 `total=1990415`、`scan_limit_reached=false`、`opening_execution_audit_scope=complete_current_signal_rows_per_mode`，後續不同深頁 `offset=1900000` 約 **1.86 秒**；正式 cgroup `MemoryPeak≈3.44 GiB`，低於 `MemoryHigh=4 GiB`、`MemoryMax=8 GiB`。IPv4 公網相同已熱 key HTTP 200／約 **107 ms**；它只是熱命中，不代表全來源第一次速度。訊號／事件路由各有 1,000 萬 offset 公開上限，不再被 10 萬筆限制截斷；總數大於總行數仍只返回空頁，不會建立假資料。當沖、overnight、公開閘道 **249 個測試通過**、Ruff、whitespace 通過。

重新跑全部 **39 條公開路由各 2 次**，HTTP 錯誤 0，原始收據 `artifacts/benchmarks/all_services_latency_2026-09-24_post_projection.json`。跨 2/25～9/24 的訊號首次觀測約 **1,749 ms**、事件約 **693 ms**；同 key 熱回應約 **7／3 ms**。此二樣本不可當 p95、也未清 OS cache。全資料欄位清冊本次首次約 **560 ms**，包含大型 payload 的網路與解析成本；完整訊號首次多秒、同時多種深頁的記憶體壓力、正式 overnight overlay、外部 IPv6、Windows 冷開機 Caddy、Crypto `deferred_source_changed` 與各服務業務來源健康仍未因這項投影修復而完成。

## 9/24 07:37～07:45 全資料搜尋互動熱點

`/data-monitor/api/features` 為 **60,280 欄／約 37.36 MB 解壓後 JSON**。前端原來每輸入一字便對每列最多五個字串反覆呼叫 `toLocaleLowerCase("zh-Hant")`；真正瓶頸在這個同步 CPU 迴圈，不是初始桌面只渲染 80 列的 DOM 數量。保留同樣的欄位包含搜尋與來源／分類條件，改用預設 Unicode `toLowerCase()`；新版本由 `app.js?v=35` 讓既有瀏覽器更新腳本。可重跑 `node scripts/benchmark_data_monitor_feature_search.mjs`，正式本機清冊五個查詢 `t/tw/twpub/price/台股` 的結果索引完全相同，所有五欄 × 60,280 列的 locale 與預設小寫值差異 **0**；舊／新搜尋 CPU 約 **3,132／112 ms**，但這是 Node 隔離樣本、不可當瀏覽器端到端倍數。收據 `artifacts/benchmarks/feature_search_2026-09-24.json` 保留 JSON 解析與搜尋分段；搜尋以外的後端清冊建置和 37 MB 解壓並未因而縮短。

公網真 Chromium 1366×768 的 `data-monitor` 首頁無 JS 錯誤、全頁橫向溢出 0；功能清冊開啟到可用約 **1,429 ms**，其中 API Resource Timing 約 **868 ms**，五次輸入到兩個 `requestAnimationFrame` 的可繪畫機會約 **25～37 ms**，原始收據 `artifacts/benchmarks/feature_search_browser_2026-09-24.json`。390×844 手機版同頁無 console error、無全頁橫向溢出、無自動觸控目標風險；筆電版密集桌面控制仍有 10 個自動觸控尺寸警示，不等於筆電滑鼠不能使用。兩份畫面收據在 `artifacts/benchmarks/public-feature-20260924/`。這是單次實際瀏覽器證據，不是公網 p95。共享導覽的 Node 回歸原先仍期待 8 頁，與已上線的 FinLab 第九頁不符；測試契約已更新為 9 頁並重跑 **16/16 通過**，資料監控 JS 語法檢查通過。

八頁公網總驗收也已重跑：`artifacts/benchmarks/public-all-pages-20260924/report-1366x768.json` 與 `report-390x844.json` 分別含 8／8 頁，兩種視窗所有頁面皆無全頁橫向溢出、無 console error；手機自動觸控尺寸警示為 0，筆電密集控制合計 110 個警示，仍需依實際滑鼠／觸控裝置權衡。當沖完整分鐘曲線在該筆電樣本選取到顯示約 **1.92 秒**，服務端 build 約 1.05 秒、傳輸 body 約 372 ms、JSON 解析約 63 ms、圖表 prepare 約 80 ms；它是新量到的多階段長尾，不能以先前本機熱命中 98 ms 取代公網首次體驗。

另修正訊號頁的一個快取依賴：原帳本頁快取會把頁面的模型 feature drivers 一起凍住，即使 `summary.json` 後來更新也不變。現在只對同一頁涉及的 feature 來源做最多每秒一次核對；帳本／state 異動仍由既有 cache key 即時失效。單一 summary 即使同大小並把 mtime 設回原值，ctime 異動也能重讀；缺檔不沿用舊特徵說明。特徵摘要快取限制 256 項、每項 JSON 至多 256 KiB，不讓歷史瀏覽無限保留解釋物件。真實完整歷史頁第一次本機 HTTP 約 **1.98 秒**；同 key 超過一秒後第一次重新核對來源約 **82 ms**，緊接四次約 **2.7／1.5／1.7／1.5 ms**。這是「來源正確性」和「熱請求延遲」之間的明確折衷，特徵檔單獨更新最多延後一秒可見。再跑當沖／隔日沖／公開 **249 個 Python 測試**及共享前端 **16 個 Node 測試**皆通過，Ruff、diff whitespace 通過，公開閘道單獨重啟驗收 HTTP 200；未觸動交易／報價服務。

07:45 的全服務收據 `artifacts/benchmarks/service-coverage-20260924T0745.json` 再次確認 **50 service／39 timer／2 path**。`stockagent-finlab-local-refresh.service` 的上次程序 systemd `Result=success` 表面欄與實際 `ExecMainStatus=15` 不同，監控正確標 `nonzero_exit`；該次於 9/23 20:14 起、21:10 被 TERM，停在供應商 `broker_transactions` 呼叫後，不能算資料完整。其後加入的逐 key 600 秒 timeout 程式尚待新的自然批次驗證，不宜在 08:20～09:10 受保護開盤窗口中人工加開供應商工作。07:45 取樣的 OpenBB L1 compaction 與註冊盤中資料確有 CPU／I/O 工作，不應當作無效忙等刪除。

另以 2/25、2/26、4/13、5/26、7/08、8/20、9/23 七個相隔交易日對照 `status=blocked` 的**公開**訊號輸出；逐日投影與逐列原始來源的總數、頁面、方向與 feature-driver 輸出皆完整相等。單日兩條路徑約 0.35～0.72 秒／0.03～0.43 秒，不能宣稱逐日分片一定比直接讀源快；完整跨期的勝出來自避免一再解碼整個 5.1 GiB 帳本。最後一次 OpenBB L1 compaction 07:47 正常完成，該批 18 個新 segment、0 stale／failed、約 **6 分 33 秒** wall、2.5 GiB memory peak；仍有約 398 萬 pending 檔，這是另一個尚未完成的吞吐成本，不因程序退出 0 就宣稱資料已全壓縮。

該次 OpenBB 正式 `stage_seconds` 進一步顯示 `stale_source_contract_query` **194.537 秒**、`stale_derivative_metadata_scan` **56.849 秒**、未指派來源查詢 **72.783 秒**、狀態 task count **52.327 秒**、建新段僅 **3.594 秒**、view 發布 **4.985 秒**。前次唯讀 SQL 樣本約 19 秒與這次正式 194 秒差距很大，須在相同負載／查詢計畫下重測索引與 I/O；不能直接把它解讀為演算法退化，更不能盲目提高並發或跳過來源稽核。07:45 覆蓋表中 33 service 有最近完成程序 wall、15 個常駐服務有資源取樣、2 個停用／未執行服務無可比工作 wall；**並非 50 項都已有功能級 p95**。

只讀 SQLite `EXPLAIN QUERY PLAN` 確認 stale 查詢目前按 segment 主鍵掃描、用 `idx_l1_members_segment` 查每段成員、再依 `tasks` 主鍵逐筆查約數百萬來源任務，另需 `DISTINCT` 暫存 B-tree；`sqlite_stat1` 目前不存在。這解釋為什麼「沒有 stale 輸出」仍有全量核對成本，但**不**單獨證明 194 秒全是 SQLite CPU 或加索引即可解決；需在市場保護窗口外，用相同 DB 快照比較 I/O、查詢計畫、索引大小與寫入影響後才能改 manifest。此輪未執行 `ANALYZE`、建索引、變更 compactor 語意或中斷 OpenBB archive。

08:02 再核對公開路徑：IPv4 HTTPS `/healthz` 為 HTTP 200／約 **51 ms**，IPv6 同 URL 約 3.1 秒後連線失敗。當沖 `degraded`、隔日沖 `waiting`、永豐 `waiting`、全資料 `critical` 仍按真實來源狀態顯示；這些不是前端效能修正可以改寫成正常。Windows Caddy 冷重啟任務的前次結果仍需真重開機驗證，不能以當下 IPv4 可達推論自啟動或雙棧完成。

## 9/24 08:11～08:16 開盤前全服務取樣與欄位搜尋

開盤前唯讀收據 `artifacts/benchmarks/service-coverage-20260924T0811.json` 再列出 50 service／39 timer／2 path。5 秒程序資源樣本中，當時註冊盤中資料約 1.74 CPU cores、公開 gateway 約 0.015；盤中資料工作隨後在 08:11:42 完成該輪，systemd 記錄約 69 秒 wall／114 秒 CPU／2.3 GiB 峰值。這些是單次時窗，不是每筆外部資料的延遲。最近已結束工作中，FinLab 的 3,340.7 秒仍是 exit 15 的失敗，不因 systemd `Result=success` 當作有效完成；後加的每個 key 600 秒期限尚待下一次自然執行驗收。開盤保護時段前未重啟任何交易、報價或資料採集程序。

全資料欄位清冊 60,280 列在每次按鍵重複建立五欄小寫字串。現在只在取得新清冊時建立與原始列一一對應的 NUL 分隔搜尋索引；分類／來源篩選仍先判定，包含 NUL 的特殊查詢退回原逐欄語意。原始欄位和搜尋結果沒有被裁剪；每次新清冊刷新都一起替換列與索引。可重跑 `node scripts/benchmark_data_monitor_feature_search.mjs`：這次同一份約 37.36 MB 解壓清冊五個查詢，既有預設小寫法約 140.94 ms，新索引建置約 52.26 ms、五次搜尋合計約 10.84 ms，結果索引逐筆相等。這是隔離 Node CPU 數字，不代表整頁提速 13 倍；首次取得清冊須付一次建索引成本。

新 `app.js?v=36` 已由本機公開 gateway 和 IPv4 HTTPS 供應。真 Chromium 公網重測 `artifacts/benchmarks/feature_search_browser_index_public_2026-09-24.json`：60,280／60,280 列具索引、五次輸入同步處理約 5～8 ms、實際前端搜尋與逐欄原始結果連特殊 NUL 查詢都逐筆一致，無 JavaScript 例外或全頁水平溢出。不同單次樣本的清冊開啟約 1.35～4.98 秒，公網 API Resource Timing 約 0.90～4.69 秒；後一輪明顯長尾，故不能宣稱首次載入已獲穩定改善。相鄰 `curl` 重測同一壓縮 API，本機約 23 ms、公網 IPv4 約 77 ms，表示瀏覽器長尾需另分解主執行緒、解壓與請求排程；也不能直接推論是伺服器慢。繪畫機會受 60 Hz frame 限制約 10～34 ms，不把它解讀成同步搜尋時間。共享前端 16 個 Node 測試、JS 語法與 `git diff --check` 通過。這輪只修可隔離的互動熱點，未宣稱全部 50 項服務的功能級延遲已到極限。

## 9/24 08:18～08:29 OpenBB 壓縮與開盤資源保護

現場發現 `stockagent-openbb-l1-compaction.timer` 在 08:18:21 啟動低優先權壓縮；原 unit 雖設定 `CPUWeight=10`、`IOWeight=10`、40 分鐘上限，卻沒有開盤時間閘門，因此可一路運行到約 08:58。08:28 前程序仍只開啟 SQLite manifest／WAL／鎖、尚未開啟衍生輸出檔，cgroup 約 2.3～2.5 GiB，宿主 memory full PSI 10 秒均值約 15.72%、I/O full 約 8.64%。這不是把壅塞全部歸因於單一服務，但在開盤保護時段不應讓可重試的壓縮與行情、訊號競爭。

共用 `check_outside_tw_opening_resource_window.py` 新增可選 `--minimum-runway-minutes`，預設 0 保持其他服務原語意；OpenBB unit 專用 45 分鐘提前閘門，比其合法 40 分鐘最長執行時間多 5 分鐘緩衝。平日 07:35～09:10 不再允許**新啟動**，不縮減正式來源稽核、來源數或 L1 輸出。原輪已在新版條件安裝前開始；核對其開啟檔案僅有 DB／鎖後，於 08:28 有意停止這項可重試維護作業，未停 OpenBB archive、當沖引擎或公開 gateway。隨後以 `systemctl start` 唯讀驗證啟動條件：`Result=exec-condition`、exit 1、`ExecMainPID=0`；當時沒有重新啟動壓縮。最新完整 L1 收據仍是 07:47:18 版本，沒有把中斷輪次當成功。壓力再採樣的 memory full／I/O full 10 秒均值約 0.34%／0.04%，這是相鄰觀測而非單因素因果證明。

正式 unit 已 `daemon-reload` 載入新閘門；下一個 08:59 timer 若觸發，應被條件略過，須再現場驗證，09:10 後亦須驗證下一輪能續跑。19 個 guardian／開盤閘門測試、Ruff、whitespace 通過；目前沒有對 16 GiB DB 做會搶占開盤 I/O 的完整一致性掃描，也不能以原始收據未改動單獨證明所有私有暫存物件都已清理。

## 9/24 08:30～08:43 開盤前重工來源與 OpenBB 保序索引實驗

再查 timer 與正式收據，08:00 的官方資料驗收首次運行 **9 分 34.6 秒、63.1 GiB memory peak**，exit 1；其衍生資料由 stale 經重建後已 current，但最後仍因 publication watcher receipt 尚未 current 而失敗，08:10 下一輪才完成。首次主要步驟是官方 symbol panel 約 93 秒、公開 feature panel 約 160 秒、嚴格模型稽核約 291 秒；這些會處理真正變動的來源，不能透過假收據或跳過稽核冒充加速。07:50 官方 sweep 確有 `twse_institutional_trades` 位元變更，並於 07:50:48 完成。08:20 的獨立正式 feature reconcile 隨後重建 9,604,259 列，**3 分 49 秒 wall、48 GiB memory peak**；它不是 OpenBB 壓縮造成的同一項工作，也不能因守住 OpenBB 就宣稱開盤資源競爭全消失。未新增會與 08:00 嚴格稽核重疊的 08:00 feature timer。

永豐股票分鐘、通用歷史及 TX 歷史服務在 08:35 仍顯示 `active`，但其主行程為 Bash，子程序僅 tee／sleep，沒有歷史查詢 Python 子行程；TX 日誌記錄 07:45 後進入 live-priority wait。這一瞬間的 process 證據不等於整日資料回補完成，也不證明即時 Tick／BidAsk 健康；保留券商登入與資料收據的獨立稽核。

OpenBB L1 每輪未指派來源查詢的約 72.8 秒中，約 58.8 秒為第一個 endpoint 按 `task_id` 排序，既有 index 只到 `(active, plan_token, status, endpoint)`；直接用 task 主鍵掃描，在正式 DB 的 1,000 筆有界唯讀嘗試超過 2 秒，不能替代全局排序。新增**僅在索引存在時**才啟用的保序查詢路徑，候選部分覆蓋索引為 `(plan_token, endpoint, task_id, rows) WHERE active=1 AND status='success'`：同一 global `endpoint, task_id` 順序、不刪來源、不改 segment 資料契約；狀態 `COUNT/SUM(rows)` 也可由 covering index 執行。隔離 fixture 的結果與既有路徑逐筆相等、`EXPLAIN` 無 temp B-tree，完整 OpenBB L1 測試 **15 個通過**。正式 **16 GiB manifest 尚未建立此索引**，所以目前不宣稱 72.8 或 52.3 秒已縮短；需要在非開盤時段量索引建置 wall／大小／archive 寫入影響、正式收據與跨輪計畫一致後才部署。

## 9/24 Yahoo 每標的修復期限的真實邊界

每日 Yahoo repair 原本 `as_completed(futures)` 後才對**已完成** future 呼叫 `result(timeout=90)`；這個 timeout 幾乎不可能觸發，不能當作外部長尾已受控。現在單標的 worker 自開始執行起建立 monotonic 時間預算，網路包裝等待不超過剩餘期限；重試、限速排隊後與 Parquet 原子提交前都再次檢查。期限已過則回報原有 `failed`／`repair timed out` 形式，不再嘗試下一個請求或寫入來源。`0` 仍不限制。刪除已無效的完成後 future timeout／handler。更底層原先自稱 daemon 的 `ThreadPoolExecutor(wait=False)` 實際仍會在 Python 行程結束時 join 未歸還的工作；已改為有 **64 個在途上限**的 daemon 工作與包含准入等待的 monotonic 期限。這不是改 Yahoo 請求節拍；正常設定的 16 個標的 worker 不會因 64 上限等待，供應商全面掛起時才避免無限制累積背景線程。隔離子程序中讓背景函數等 30 秒、呼叫端期限 0.02 秒，含 Python 載入的程序 wall 約 **0.51 秒**而非被背景線程拖住 30 秒。**這不是強制中止**：已送出的供應商呼叫無法被 Python 線程取消，可能在背景短暫持續消耗連線／配額；已進入不可中斷的原子寫入也可能超時。不能把設定解讀為整個批次最長 90 秒，也不能拿失敗作完整來源。64 個 Yahoo 狀態／修復測試通過，含慢來源晚歸不得寫檔、預算傳入 worker、遺留 daemon 及並發閘門；Ruff、diff whitespace 通過。**06:30 已啟動的 `registered-data-daily` 行程不會因改檔自動載入新版，也未為此中斷它**；須等後續新行程的真實 US repair 收據證明長尾下降及失敗率不升，本輪未啟動額外 Yahoo 請求。

## 9/24 10:42～10:48 當沖事件與儲存掃描避讓

新唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260924T024231Z.json` 仍列出 **50 service／39 timer／2 path**，當時沒有 failed unit；這只是程序層，不代表交易或資料健康。五個模式今日都已持久寫入 09:00 訊號。開盤測速首個模式的 Snapshot 往返 2,170.373 ms，最後模式因序列排隊約 3,383.444 ms，09:00:03.650 才完成訊號；因此一秒目標尚未達成，也不能用後續紙上補成交冒充 09:00 執行。盤中改用有 `simtrade` 與交易所事件時間的 Shioaji Quote 推送後，10:34 的每分鐘 `quote_fetch_ms` 為 5.419 ms；292 個未平標的超過 200 筆同時訂閱限制，該分鐘僅 133 檔在 10 秒內有事件。完整故障及證據邊界見 `docs/tw_day_trade_live_quote_recovery_2026-09-24.md`。

儲存清冊服務每次 `scandir` 約 452 萬檔，最近一次 10:09 啟動的服務壁鐘約 100.7 秒；它提供容量與近 30 日 mtime 統計，盤中每小時重掃既不能縮短當沖訊號，也會和即時工作搶 CPU／I/O。將 timer 改為台北時間 00～07、14～23 時的每小時 09 分；取消開機 15 分鐘強制掃描與漏跑補執行，避免在 08:00～13:59 啟動。儲存網站仍讀最近的完整快照，盤中數字會延後到 14:09 才刷新，不能把快照當即時磁碟用量。只重載並重啟 timer，**未重啟清冊工作、當沖、報價或 Discord**；`systemd-analyze verify` 通過，下一次觸發顯示 14:09。開機契約／儲存快照相鄰 11 個測試通過。14:09 的正式壁鐘與盤中資源干擾改善仍待自然排程驗收，不能把避免盤中掃描說成掃描本身加速。

同一盤中檢查發現 Bybit 加密訓練刷新 10:30 自動啟動，雖正式收據為 `completed`、壁鐘 94.4 秒，但累計約 269 CPU 秒、記憶體峰值約 21.3 GiB；它不是 09:00 當沖的輸入。停用 10:30 timer 觸發與開機漏跑立即補執行，保留 02:30、16:30、22:30 的完整原工作及發行稽核。已重載 systemd 並只重啟 timer，下一次顯示 16:30；10:30 的**已完成結果不抹除**，若 02:30 後真的失敗，盤中不再重試，會等 16:30，資料健康應保持未完成。這是交易時段資源避讓，不宣稱 Bybit 本身 94 秒耗時縮短或整體市場延遲已降低；需下個開盤測量訊號與系統壓力才可歸因。

OpenBB L1 壓縮也在 10:40:58 盤中執行，約 147 秒壁鐘、111 CPU 秒、2.5 GiB 記憶體峰值；既有 07:35～09:10 開盤閘門不足以保護持續中的當沖。只對該可重試壓縮 unit 延長 `ExecCondition` 到平日 13:35，保留 45 分鐘開盤前 runway、原壓縮與收據語意；其它使用共用腳本的服務仍沿用原 09:10 截止。10:57 現場試啟動的條件回傳 `allowed=false`／`Result=exec-condition`／`ExecMainPID=0`，確認沒有啟動壓縮；原本 10:40 完成的正式結果保留。此修正保護盤中資源，不縮短壓縮本身執行時間；下個 13:35 後自然輪次還須驗收。

## 9/24 11:07～11:26 盤中實際健康與 FinLab 早失敗閘門

正式當沖單元名稱是 `stockagent-tw-day-trade-simulation.service`，11:16 為 `active/running`，不是不存在的 `stockagent-tw-day-trade.service`。本機公開 `/tw-day-trade/api/status` 為 `degraded`：五模式今日訊號齊，但 100m 與 GELU 模式尚有未達標模擬成交；`opening_gate` 仍為 `failed`，其中 09:00 後保留的 10:00 驗收也顯示 final-arm 未就緒。今早 08:54 的門檻檔曾證實五個 final-arm ready，但 08:56 後都消失；原因需結合 Discord 程序當時版本及後續 09:51 重啟再驗證，不以訊號已出反推開盤驗收通過。09:00 最慢不可變訊號為 **3,650.179 ms**，首次 Shioaji 開盤價覆蓋為 **2,514 ms**，一秒目標未達。11:16 股票即時訂閱約 200／291，當分鐘有可用事件約 91／291，不能把無有效非試撮行情的未成交股數補寫成券商成交。

全資料狀態快照每 30 秒重算：FinLab 正在連續新增 Parquet 時，清冊約 2.3～3.6 秒、公開投影約 0.9～1.3 秒、欄位投影約 1.9～3.0 秒，約 6～7 個新檔迫使欄位清冊實際更新。FinLab 停止後 11:23 自然輪次 `refreshed_files=0`、`feature_reused=true`，總壁鐘 **2,711.031 ms**，其中清冊 **1,554.299 ms**、公開投影 **973.741 ms**、欄位計數重用 **30.264 ms**。不能將活動中真變動誤判成「無用重建」而直接省略；仍需在不漏來源的條件下研究增量 per-dataset 投影。大型欄位 API 的一筆本機 gzip 首位元組約 **628 ms**、總 **636 ms**、壓縮下載 **1.16 MB**；單次樣本不代表 p95。

`stockagent-finlab-local-refresh.service` 11:12 失敗：先運作 **36 分 7 秒**、耗 **13 分 37 秒 CPU**，最後 Bash 回報腳本 `done` 附近語法錯誤，完整資料仍為 partial。現有腳本 `bash -n` 已通過；為避免再次讓這類可在啟動前發現的錯誤耗盡長批次，FinLab installer 現在安裝前語法檢查，正式 service 加上 `ExecStartPre=/usr/bin/bash -n ...`，已以 `finlab-only` 安裝、`systemd-analyze verify` 及已載入 unit 驗證。未重跑 FinLab、未抹除失敗；自然下一輪 16:10 的實際資料收據才是恢復證據。這道閘門不能防止執行中腳本被原地改寫，也不能證明供應商下載成功；須另處理穩定部署來源與長工作不中途變更的操作契約。

## 9/24 11:39～11:43 欄位 API 首次請求驗證成本

正式 `feature_inventory.json` 約 **49.20 MB／79,299 欄**；隔離 Python 同一檔 JSON 解析約 **733 ms**、gzip level 3 約 **161 ms**、SHA-256 約 **26 ms**。先前公開閘道即使讀取已完成的 producer 快照，第一次訪客仍重做完整解析，造成單次本機 gzip HTTP 首位元組 **628 ms**、總 **636 ms**。現在共用 `data_monitor_feature_receipt` 契約：producer 僅在唯讀 DTO、無非有限 JSON 及原子檔完成後發布同所有者、不可由群組／他人寫入的 SHA-256 收據；閘道逐位元組與檔案指紋核對後免重解析。缺收據、修改來源、或收據不符都回原來的完整 JSON 驗證路徑，不把快取當認證或業務資料健康證明。這是可信本機 producer 與唯讀 gateway 間的完整性優化；具有 root 寫入權者仍不在此收據的防護範圍。

相關 164 個 Python 測試、Ruff 通過；另將資料監控靜態頁版本斷言與現行 `app.js?v=36` 對齊。只單獨重啟唯讀公開閘道，當沖、Shioaji、Discord 均未重啟。第一次本機 gzip HTTP 200 首位元組 **182 ms**、總 **185 ms**、下載 **1,156,959 bytes**，同 key 熱命中約 **1.9／5.3 ms**；回應解析仍為 79,299 欄、`read_only=true`、`production_control_possible=false`、摘要狀態 `partial`，沒有冒充完整。這是兩筆相鄰樣本，不能當 p95 或瀏覽器載入／搜尋端到端改進；資料來源更新時仍需真實重建欄位快照。

安全複核後收據升為 **schema v3**：只在 producer 以 `allow_nan=false` 序列化並驗證唯讀 DTO 後才簽出；舊 v2 收據回完整解析，不可免除非有限 JSON 檢查。164 個相關 Python 測試再次通過，包含篡改來源／缺收據回退。再次只重啟公開閘道後，v3 冷 HTTP 樣本首位元組 **517 ms**、總 **547 ms**，當時的主機／檔案快取負載不同；同檔三次隔離拆解的讀取／收據 SHA／gzip／回應 SHA 約 **30／24～29／91～103／24 ms**。因此不能把前述 182 ms 當穩定 SLA，需累積多個自然換版的分位數；所有樣本仍保留完整 79,299 欄，沒有縮減內容。

公開 DDNS 的 IPv4 HTTPS 實際 `/healthz`、當沖 `/api/status` 及全資料欄位 API 均回 HTTP 200；其中一筆欄位 API 從本機經 DDNS 約 **738 ms**，連線階段約 **447 ms**，不可把此網路變動誤算為純應用建置。相同網域強制 IPv6 `/healthz` 於 2 秒連線逾時／HTTP 000，雙棧仍未修復。當沖正式 API 仍顯示 `degraded`，公開可達不等於交易或資料健康。

## 9/24 12:00～12:12 回收後的一次性服務測速證據

`systemctl show` 會在一次性 unit 被回收後清空 `ExecMain*` 時戳；原先的 22 項 `not_measured` 有相當部分其實已執行，不能把空欄解讀為未執行或零秒。`scripts/audit_service_latency_coverage.py` 現在只對閒置且缺目前時戳的 unit，讀取最近 14 日 systemd PID 1 的結構化 journal，按同一 boot ID／invocation ID 配對開始與終止事件計算程序壁鐘，並標註 `last_attempt_source=systemd_journal`。短工作可能沒有資源統計事件，因此成功／失敗終止事件亦可作結束時點；不能用更舊的失敗代替較新的短暫成功。未配對、無明確結果或 journal 遺失則保持未知。正式業務收據仍須與這次啟動時間相符，不能拿新舊不同工作的 `latest.json` 塗綠。

最新收據 `artifacts/benchmarks/service-coverage-20260924T1225-final.json` 保留 50 service／39 timer／2 path：**36 項有最近一次已完成程序 wall、13 項只有常駐資源取樣、1 項無近期可配對執行**（已退休且停用的 `stockagent-hot-artifact-sync.service`）。這不是「49 項功能延遲已最佳化」：程序壁鐘包含等待，業務資料須看正式收據，daemon 的 CPU 取樣也不是單次請求的 p95。相鄰測速／FinLab／開盤回歸最後合計 247 個測試通過。

配對後發現 9/20～9/21 的 `registered-data-backfill` 跑 **65,724.113 秒**後失敗，該輪 Binance 574 標的有 1 檔未完成；9/24 的日常 tail 收據另有 574／574 `updated`，但不證明整個歷史特徵回補已完成。`tw-public-cold-publish` 的**最近** systemd 呼叫其實在 9/24 00:00:48～00:00:49 **0.569 秒成功退出**，但資料工作為 `deferred/canonical_refresh_lock_busy`，沒有發布 release。前一版 journal 回收邏輯只取有資源統計的事件，誤把 9/23 23:55 的 19 秒失敗當最近一次；現已納入短工作終止事件修正。冷發布 `latest.json` 已被 08:03 另一條流程覆寫，不屬於午夜 systemd 呼叫；進一步按啟動時間配對其不可變 `runs/20260924T000048721178.json`，正式收據確認 `deferred`。下一次 23:50 timer 和來源完整性稽核仍需驗收。

## 9/24 12:16 FinLab 長工作版本固定

11:12 的 FinLab 腳本在已下載 581／1,109 個目錄鍵後才因 `done` 語法錯誤中止；僅在 `ExecStartPre` 對**當時**的來源做 `bash -n`，仍無法防止長 Bash 工作在執行中逐段讀到後來修改的檔案。正式 unit 現改由 `run_finlab_refresh_frozen.sh` 啟動：建立獨立暫存快照、比對來源位元、對**同一份快照**做 `bash -n`，再執行該快照；原腳本透過固定的 repo root 環境變數保持既有相對路徑與資料語意。installer 同時語法檢查兩份腳本；`finlab-only` 已重裝 unit，`systemd-analyze verify` 通過，下一次 timer 是 **16:10**。沒有重啟既有失敗工作、沒有盤中重新下載，也沒有清除失敗證據。26 個 FinLab／測速相關測試通過；真正完成度、配額與資料品質仍須 16:10 的正式收據驗證。此保護是腳本版本一致性，不是 FinLab SDK 或來源可用性的保證。

## 9/24 12:33～12:40 長期測速與快照熱點再核對

每五分鐘的正式 `all_service_runtime_sample` 現在也對 systemd 已回收的 oneshot 補查同次 journal，保留啟動 ID、boot ID、退出結果與程序壁鐘，再按該次啟動時間尋找業務收據；這使近期執行不再在長期趨勢裡變成空白。12:33:40 自然樣本包含 **50 個服務**，補查的 22 項 journal 約 **477 ms**（隔離呼叫），正式整個 service-trend 階段 **744.896 ms**，只在五分鐘週期發生、不是每 30 秒必跑。當中 `registered-data-backfill` 最近一次 65,724.113 秒／失敗取自 journal；`tw-public-cold-publish` 0.569 秒／程序退出 0、同次不可變收據 `deferred/canonical_refresh_lock_busy`，沒有把退出 0 誤報為來源已發布。67 個相鄰監控／公開狀態測試、Ruff 與 whitespace 通過。

每 30 秒全資料快照的 12:32～12:35 自然樣本總壁鐘約 **2.39～2.63 秒**：來源清冊約 **1.24～1.37 秒**、公開投影約 **0.93～1.11 秒**；沒有趨勢取樣的輪次該階段約 1 ms。用正式冷清冊快取環境進行本機剖析，公開投影約 1.43 秒的 cProfile 樣本中，永豐本機 JSON 收據讀取共 1,097 次、且路徑**全部不同**；它們不是同一收據反覆讀取，盲目加函式內 memoization 不會縮短一次性快照。最大單檔為當沖 `state.json` 約 8.9 MB，讀取／解析的一次樣本約 122 ms；即時監控未登入永豐、未新增行情訂閱。來源清冊的剖析樣本約 1.91 秒，其中檔案枚舉／排序約 0.80 秒、兩份 JSON 解碼約 0.38 秒、約 7.9 萬次檔案 stat 約 0.31 秒；它須核對約 5.6 萬份已登錄檔案的變動。這些是 CPU 剖析下的不同樣本，不能相加成正式延遲，也不能透過略過鮮度檢查或縮減來源宣稱最佳化。下一步應針對檔案發現與大型狀態投影做輸出等價的增量證據，而不是改變 Shioaji 連線或拿熱快取冒充完整來源。

12:47 的跨系統唯讀核對：Windows `StockAgent Public Caddy` 工作排程顯示 **Running**，含開機／登入／定時觸發；Caddy 在 Windows `::`:80/443 與 `192.168.50.211`:80/443 監聽，StockAgent Windows 防火牆允許 TCP 80/443 的 `LocalAddress=Any`。WSL 本機 `127.0.0.1:8770/healthz` 與公網 IPv4 均 HTTP 200。DNS AAAA 當時是 `2001:b011:e610:3a44:7ccc:5dfa:4c50:bcc0`，**不是** Windows `Ethernet 5` 上的 `2001:b011:e610:3a44:b0c6:b1a0:8768:74eb` 或其另一個全域位址；Windows 本機測其 IPv6:443 TCP 成功，但公網以 DDNS IPv6 連線仍逾時。這把當前雙棧阻點收斂到 DDNS 指向路由器 WAN 位址後的 IPv6 防火牆／轉送或 DNS 目標選擇；未取得路由器當前設定與外部 IPv6 對 Windows 位址的探針證據，不能單獨歸因於哪一項，也不能因排程顯示 Running 便宣稱冷重開機已驗收。

公開資料狀態投影原先把同一批 logical sources `_enrich_and_sort_rows` 算兩次：第一次供群組狀態彙總，第二次和群組／實體清冊合併。現在在**同一個快照輪次**重用首次計算結果，僅對群組／實體列新算，再按原排序鍵重排並重編 `sort_index`；來源發現、時效、發布與缺口檢查均不跳過。測試證明每個來源只富化一次、分拆再合併與原全量富化的欄位／排序相同；54 個資料監控測試及 Ruff 通過。在隔離、已提供清冊且把永豐／OpenBB 來源固定為 fixture 的四次相鄰樣本，原雙算約 **320／355 ms**、新單算約 **299／309 ms**；這只有數十毫秒，不可換算為正式完整快照的穩定百分比。自然輪次公開投影仍在約 0.95～1.1 秒範圍，來源清冊約 1.2～1.4 秒，是下一個較大的成本。

Windows 最近實際開機時間是 **9/15 07:31**，不是 9/24 08:50；後者是 WSL 不可達後的恢復事件，不能冒稱冷開機驗收。Caddy supervisor `startup.log` 從 08:50:18 到 08:53:58 約每 15～16 秒派送一次 `wsl.exe` 啟動要求，08:54:04 首次記錄 backend healthy。Linux `uptime -s` 為 08:53:57；systemd 在同秒啟動公開 gateway，08:54:01 開始監聽，因此這段約 3 分 39 秒的主要延遲發生在 Windows 派送 WSL 到 WSL 使用者空間可用之前，**不是** Python gateway 的 4 秒啟動，也不是 Caddy 設定未安裝。已安裝的 Windows launcher/Caddyfile SHA-256 與 repo 版本逐位元相同。需另取 Windows WSL 啟動事件或真實受控重啟測試，才可分辨 WSL VM／磁碟／Windows 資源延遲；本輪不在盤中重啟 WSL 或交易服務製造新的樣本。

## 9/24 13:01～13:15 開盤保護漏網與訊號健康誤判

正式 OpenBB archive 今日 08:58:52 由 `OnBootSec=5min` 啟動，在 09:01 左右才完成約 890 萬工作項的既有計畫載入；其低優先權配置不能保證零 I/O／記憶體競爭。這和 09:00 訊號延遲同時發生，但尚無單因素因果證據。既有 guardian 08:20～09:10 只暫停三個 registered-data 可續傳工作，漏了 OpenBB archive；開機後 08:58 的觸發也沒有 `ExecCondition`。現已在來源模板加入同一開盤閘門，將 archive 納入 guardian 的有收據暫停／恢復清單，並增加工作日 09:12 的 timer 補啟動，供開盤中因 `ExecCondition` 略過的開機觸發於時段後續傳。`Persistent=false` 避免漏跑補啟動直接撞進保護窗口。36 個開機、guardian、archive shell 契約測試通過，calendar 解析下一次為 9/25 09:12；**截至此記錄尚未安裝新版 systemd 模板**，13:20～13:30 出場前不會為此重啟當沖或 OpenBB 工作。

同一時段 guardian 把四個今日 09:00 已持久提交的訊號標成「非因果回放」，根因是 `_classify_session_signals` 只接受舊的 `causal_best_quote` 字串，而四個新模式的正式紙上執行契約是 `causal_market_full_target_at_best_quote`。加入兩者的明確允許集合，仍拒絕官方開盤價回放、非零價格 offset 與缺少訊號／完成時間。27 個 guardian／開機回歸通過；13:11 自然 guardian 收據已由 `failed` 改成 `degraded`、`failures=[]`，沒有改帳本或聲稱券商成交。`degraded` 的剩餘原因是根目錄磁碟 92% 使用率、約 154 GiB 可用，觸發原有早期警告，不是此誤判。

當沖公開狀態仍為 `degraded`：100m 與 GELU 模式各有同一檔 7547 的 2,000／128,000 股紙上進場保持 `waiting_fresh_regular_session_quote`；其餘三模式為紙上目標數量完成。現有 QuoteSTKv1 最多 200 筆同時訂閱，13:08 約 291 檔待估值，該分鐘只有約 116 檔有 10 秒內新事件。沒有正式盤中、非試撮且訊號後的最佳買賣價時不能從 Snapshot 或 1 分鐘 K 線偽造「市價成交」；目前沒有對 7547 做券商委託或新增永豐登入。13:20～13:30 紙上出場與 13:35 後服務部署尚待實際收據驗收。

## 9/24 13:20～13:38 尾盤故障驗收與背景服務隔離

13:20 首次看精簡 `status.json` 時，由其不含 `exit_limit_submitted_at` 誤推斷限價出場未執行；核對 canonical `state.json`、`events.jsonl` 與公開 `/tw-day-trade/api/summary` 後，五模式均為 **13:20:00 已送出**，公開摘要也有時間戳，故沒有為此增加重複投影或修改交易。13:24 五模式均進入市價強制出場，部分已平；13:25:01 均記錄極端合法限價尾盤委託。13:25 尚有 191／5／1／0／1 筆 `closing_auction_order_status=working`，但 `closing_auction_pending_count` 顯示 0：原引擎只在事後結算更新此計數。已在**尚未載入的來源程式**中改為委託送出時立即反映未平筆數；四個相關出場測試通過，沒有把掛單變成成交。

13:30 收盤驗收沒有證明成交，13:35 的原帳本仍有 **191／5／1／0／1，合計 198 筆未平**，逐筆標示 `unfilled_no_causal_auction_evidence`，舊契約將其轉成模擬融資／融券，非券商核准或實際成交。正式五市場 `day_trade_strict_intraday` 旗標仍為 false；9/10 已實作但未啟用的嚴格契約才會等候 13:33 延遲撮合至 13:35，且僅具證據的不利鎖漲跌停可例外留倉。因現有帳本含舊持倉，不能在未核對成本／股數與使用者選擇前把這 198 筆冒充平倉或直接替換策略；已向使用者詢問是否保留原帳本，或另建明示反事實清算新帳本。

現場重複的 `latest_quote` 來源已由共享 broker 請求檔 `requester_pid=773` 確認為**隔日沖**引擎，不是 Discord。舊引擎於 13:30～13:35 因 `close_hot` 每 0.1 秒迴圈重打同一批 424 檔 Snapshot；當沖 broker 日誌該五分鐘共有 1,185 次 `latest_quote`，其中 1,178 次為 424／424，同時 Discord 盤後快速訊號亦需較大的 snapshot。這是與尾盤行情的資源競爭，仍不能單獨證明每筆收盤缺價都由它造成。隔日沖 executor 已加 monotonic **最少 1 秒請求間隔**，保留訊號檔 event-driven 喚醒與最長約 1 秒的新報價輪詢；21 個隔日沖測試、Ruff 通過。13:35 觀察截止後 13:36:15 才重啟隔日沖，四模式未平股數皆為 0，100m 的 `critical_actual_close_print_missing` 保留；下一個實際開／收盤仍須驗收請求率與有效價格。Shioaji 即時訂閱與 Snapshot 是不同證據契約；此節拍不允許把 Snapshot 宣稱為當沖即時非試撮成交。

OpenBB archive 模板已於 13:36 透過正式 installer `--no-start` 套用，重建 3,943 列監控歷史約 4.74 秒；原 archive PID 5880／08:58:52 啟動時間未變，未重啟下載。systemd 已載入 `ExecCondition` 與工作日 09:12 補啟動 timer；離線評估 9/25 08:58:52 被拒、09:12 允許，37 個 guardian／開機／archive 回歸通過。這證明配置與模擬時鐘，不等於下一次真正 WSL 冷啟動及 09:00 資源避讓已驗收。當日根目錄磁碟約 92% 使用、154 GiB 可用，警告仍存在且沒有刪任何資料。

## 9/24 13:42～13:46 永豐歷史服務開機喚醒核對

三個歷史服務的 `active` 起點分別為期貨 Tick 09:03:51、一般歷史 09:05:51、台股分鐘 09:23:56；逐一核對 cgroup 與日誌後，當時都**只有 Bash、tee、sleep**，保護等待至 14:31，沒有盤中歷史 API 查詢。不能把常駐 `active` 推論成搶走報價額度或開盤訊號延遲的因果證據。真正多餘的是開機／啟用 timer 在 07:45～14:31 提早喚醒服務、佔三份常駐程序，且 `NEXT=-` 不易觀察正式 14:31 啟動時點。

三個 service 現共用 `ExecCondition`，交易日 07:45～14:31 不啟動歷史 runner，14:31 日曆 timer 保留。離線時鐘 9/25 09:03:51 拒絕、14:31:00 允許；27 個開機／guardian 測試通過。三份模板已用各自 installer `--no-start` 安裝；確認當時只有 sleep 後停止這三個等待程序，沒有停止 Top-200、TAIFEX、當沖或 Discord。systemd 顯示三個 timer 均 `active`，**下一次均為 9/24 14:31:00**，服務 `inactive` 且結果 `success`。14:31 的真實下載、額度及完整性仍待當次 receipt 驗收；此修正只避免盤中空等與啟動誤判，不把資料標為已完成。

其中期貨 Tick／一般歷史在平日 14:31 啟動後，仍可能依原有 FOP 夜盤連線預留規則等待到次日 05:00；因此 14:31 timer 到點並**不保證**這兩項開始歷史 API 查詢，須看 runner 日誌的實際批次與收據。台股分鐘下載另有 14:31～14:45 的限時視窗。

13:47 儲存壓力驗證：根檔案系統約 2.0 TB，剩餘 **147 GiB／使用率 93%**。Binance public archive 12:30 的 preflight 因 10% 保留空間門檻未達而退出 75；service 的 `SuccessExitStatus=75` 使 `Result=success`，但公開資料監控正確仍列為 `unable/degraded`，不能當成封存完成。正式 compiler-cache 清理在 13:09 的 receipt 是 `eligible_files=0`；同參數唯讀 dry-run 13:47 仍是 0。盤點主要本機目錄：`artifacts` 約 515 GiB、冷 store 約 387 GiB、materialized 約 37 GiB、live producer 約 67 GiB；`artifacts` 中 markets 約 172 GiB、replays 約 105 GiB、live 約 97 GiB、cache 約 51 GiB。這些名稱與 `du` 大小都**不是可刪證據**，本輪沒有清除來源、帳本、冷 release 或快取。04:26 的核准冷 store retention **已**回收約 2.36 GB，仍遠低於恢復約 10% 保留空間所需；需另做逐來源可重建／D 備份／程序引用稽核，不能降低預留門檻冒充有容量。

隔日沖收盤執行另有獨立且可重現的來源契約斷層：100m 模式今天 424 筆 `close_auction_entry` 均在 13:34 標 `expired_without_actual_close_print`，部位 0。共用報價代理的 `latest_quote` 只回 Shioaji Snapshot；目前安裝的 Shioaji 1.7.0 `Snapshot` 型別欄位沒有 `simtrade`，正式官方 Snapshot 欄位表亦沒有它。轉換器因此把 `simtrade_flags` 留成未知，隔日沖 `_auction_print` 必須有 `simtrade is False` 與 13:30～13:33 的交易所時戳才接受，故不能從此來源建立可驗證紙上撮合。10 Hz 舊輪詢造成的重複 Snapshot 已節流到下一交易日最多約 1 Hz，但**節流不會補上缺失的成交證據**。要修復須由現有單一永豐連線的 QuoteSTKv1 非試撮即時事件，提供具交易所時戳的收盤撮合收據，且處理 200 檔訂閱上限；這涉及與當沖共用行情代理協調，不應把 Snapshot `close` 或事後官方收盤價冒充即時可成交報價。今天隔日沖帳本未被重寫。

官方 Shioaji 使用限制還明示盤中不應反覆輪詢 Snapshot 作即時報價，應改用訂閱；因此 1 Hz 只是將原本約 10 Hz 的壓力暫時壓低，**不是合規或來源正確性的終態**。目前與另一位 agent 的當沖帳本工作並行，本輪沒有改共用報價代理／當沖執行器，也未另外登入永豐；要改接收盤事件需一起設計 200 檔上限、訊號優先序、記錄時戳和觀測缺口。14:04 唯讀冷 artifact 狀態顯示 4 個已登錄來源皆未達完整訓練 artifact 契約，故不能以 `artifacts/markets` 名稱或冷 release 存在為理由退役其熱副本。

## 9/24 14:00～14:50 盤後來源耗時與並發失敗

官方 close 初次於 14:00 尚未發布；14:00:30 後接受。盤後 `corporate_action_entitlements` 第一次查詢遇到 14 筆 MOPS HTTP 200 拒絕頁，保留為失敗收據並重試，不把拒絕頁當成功內容。後續正式 completed-session 收據為 `status=ok`，14:41:47 的最新版本確認 TWSE／TPEx close、公司行動、股票 panel 與公開 features 都標到 9/24。`status=ok` 只證明這組衍生來源驗收，不代表所有官方資料已完成或隔日沖成交成立。

股票配發的 MOPS 清單與明細原本各有約數千筆 Future，但只有整批結束才看得到計數。下載器現在每 500 筆（並含首筆與末筆）記錄完成／失敗、單階段 elapsed、實測 rate 與粗估剩餘秒數；正式 summary 增列兩段實際耗時，空候選為 0。它不放寬來源拒絕判斷、不增加請求率，也不冒稱這次 14:00 已使用新程式；下一輪自然執行仍需驗收新欄位與供應商長尾。137 個相鄰公司行動／發布／Shioaji 排程／開機契約測試通過。

隔日沖正式歷史工作 14:28:04 開始，146／146 日 replay 於 14:38:16 算完，隨後依既有 fail-closed 雜湊核對拒絕發布：`twse_daily_ohlcv.parquet` 在其運行期間 14:34:22 變更。服務因此 exit 1，14:38:21 標 `failed`；不能沿用 9/23 的最後完整部署冒稱 9/24 已更新。抽樣比對 9/23、9/24 的官方 OHLC 與舊逐日輸入逐檔相同，但**沒有證明全 146 日、模型 panel 與其他來源都等價**。既有逐日輸入與訊號快取只核對各自檔案雜湊，缺少對新來源版本的等價／失效驗收；直接重跑有混用版本風險。此處尚未修復，不重跑、不清除約 13 GiB 隔日沖歷史工作樹，須先設計來源版本綁定或等價證據，並把來源變動檢查提早到昂貴 replay 中途。

14:31 台股分鐘 K 線 runner 真正啟動查詢，一段現場速率約 4.6 request/s；其後在 14:45 視窗截止後進入既有分鐘資料集建置。一般 Shioaji 歷史與 TX 歷史同時觸發，但兩者只有 Bash／tee／sleep，日誌明記 `live_connection_reservation` 待至次日 05:00，不能標為已下載。14:49 根目錄仍約 140 GiB 可用／使用率 93%；未因空間壓力刪除任何來源或帳本。

## 9/24 15:00～15:15 隔日沖歷史來源版本防線

已在既有歷史重建入口加上來源版本與快取相容性驗收：同一 lineage 工作目錄的已存逐日輸入／訊號若對應不同 `twse_daily`、`minute_manifest` 等全域來源雜湊，**在寫新 plan 與計算前**明確拒絕重用。逐日輸入快取另核對該日漲跌停檔與分鐘 partition 的收據雜湊；同源檔案在重播途中每 20 日比對 metadata，只有變動才重新 SHA-256，末端原有全量 SHA-256 仍保留。這縮短可見的來源競態浪費，沒有跳過原交易、估值或來源語意。

用 9/24 的現存工作目錄只跑 `--stage plan`，約 6 秒即得到預期的 `cannot reuse this cache namespace: minute_manifest, twse_daily`，沒有重新跑 146 日或發布舊訊號。15 個隔日沖相關測試、Ruff、編譯和 diff whitespace 通過。**隔日沖 9/24 歷史仍未修好／未發布**：下一步必須以來源版本綁定的新、可稽核工作區重新產生受影響的逐日輸入與模型訊號，並設計避免每天累積龐大重播檔案的保留規則。現有一次失敗 replay 約 2.3 GiB，其中 `signals.jsonl` 約 2.1 GiB；盲目每天建立新 lineage 會加劇現有約 7% 的低空間壓力，因此本輪沒有這樣做，也沒有刪除任何既有歷史收據。

## 9/24 15:15～15:22 夜盤 FOP 服務分層核對

`stockagent-shioaji-minute-backfill.service` 的券商分鐘 API 段已離開，15:16 仍在本機 `download_shioaji_tw_kbars --local-only` 衍生處理，約 14.3 GiB 記憶體；這不是新增永豐登入，卻仍占 CPU／I/O。同期 `stockagent-shioaji-taifex-bidask.service` 三個 worker 已訂閱，15 點時段 `book_events/trade_date=2026-09-25` 近期 Parquet 持續新增；現場 memory full PSI 10 秒均值約 0、I/O full 約 0.35～1.02%。因此只可說**本次取樣看到行情捕捉在寫入**，不能以兩個 unit `active` 推論無掉包或長期延遲達標；本輪未停任何夜盤服務。

FOP 策略健康與行情捕捉需分開。14:51 的策略 bootstrap 拒絕現存 9/18 到期週期，明確原因 `cannot cash-settle cycle while a shadow futures hedge remains open`；後續 `capture=data_only`。唯讀 `state.json` 有 28 個模擬策略保留非零期貨影子部位，絕對口數合計 115，淨口數 -83；9/18 官方 TXO 結算表有 1 筆價格 47,116，但它**不是影子期貨的平倉或結算證據**。不會清零持倉或把資料捕捉成功冒稱策略啟用。後續需按各期貨實際合約身分、有效撮合／官方最終結算及策略帳本稽核另行修復；本輪僅診斷，沒有改 TAIFEX 策略程式或狀態。

## 9/24 15:29 分鐘補抓終端與本機網站抽樣

分鐘補抓的正式 `download_summary.json` 為目標 9/24、選取／回報各 2,757 檔、失敗與 partial 均 0、`resumable_collection_complete=true`，但 `selected_coverage_complete=false`，仍有 89 檔被分類為來源缺口，不能宣稱全市場完整。後處理 15:29:02 完成 `tw_shioaji_audit status=ok`，混合資料 2,339 檔、不可用 127 檔；runner 隨即進入 `historical_source_gap_retry`，因三條 FOP 夜盤連線加兩條股票保留連線而等待，不是仍在盤中查券商資料。正式收據／狀態的這些層級彼此不同。

15:29 單次本機公開 gateway：`/healthz` HTTP 200／約 1.3 ms，隔日沖 status HTTP 200／約 66.8 ms，當沖 status HTTP 200／約 5.8 ms。實際 payload 中隔日沖 `health=critical`（收盤實際撮合價缺失），當沖 `health=degraded`（未平模擬部位）；HTTP 可達不等於資料或交易正常。本輪遵守與另一位 agent 的工作邊界，沒有修或重算當沖帳本。當時根檔案系統剩餘約 133 GiB／使用率 94%，無安全刪除證據。

## 9/24 16:25 隔日沖同日來源修訂與模型快取範圍

再對 146 個歷史交易日做唯讀語意比對：目前官方 TWSE／TPEx 每檔日線的 OPEN／HIGH／LOW／CLOSE 與舊輸入相同，漲跌停檔亦相同；分鐘 partition 的位元雜湊在 9/15、9/16、9/17、9/18、9/21、9/22、9/23、9/24 共 8 日不同。以 9/24 14:28～14:30 的**兩個當日模型面板快取版本**與目前重新建置的 210 日面板逐日比較，99 與 24 特徵版本各只有 9/24 的 feature row 不同；本次兩次真來源面板建置分別約 35.4／32.3 秒。這只證明兩個 9/24 快取與現在的特徵差異，**不能倒推 9/16～9/23 已產生訊號時使用的所有模型輸入都等價**，也未比對每日日內決策價、其他 masks 與最終撮合。因此目前不沿用舊訊號、不發布 9/24 歷史，也不以 cache 名稱冒充可重用證明。

同日來源更正還可能被另一道捷徑遮蔽：原 `maintain_tw_overnight_history._deployment_current` 只檢查部署日期、lineage 及輸出 SHA。現先以廉價的日期／版本條件排除不相干部署，再核對發布 plan SHA 與每個官方全域來源的目前位元雜湊；任一來源修改就不能回 `already_current`。隔日沖歷史／來源相關 20 個測試、Ruff、編譯與 whitespace 檢查通過。這是**發現錯誤的閘門**，不是 9/24 歷史的重建方案；未重啟交易、行情或當沖服務。16:24 根目錄剩餘約 123 GiB；建立每日 13 GiB 新工作區仍未具備保留／回收證據。

## 9/24 16:10 自然排程後的區分

隔日沖歷史 timer 自然嘗試後仍 `failed/exit 1`，源於來源／舊快取不相容的安全拒絕；公開網站保留 9/23 最後完成部署，不能報成 9/24 最新。FinLab 16:10 工作則在 16:53:58 以程序 exit 0 結束，耗時約 43 分 59 秒／CPU 約 10 分 52 秒、記憶體峰值 6.6 GiB，但正式驗收 `catalog_current=false`：1,109 個目錄鍵中 802 verified、225 missing、82 stale，SDK 有 2 次 600 秒逾時，衍生資料與私有冷發版均未啟動。程序成功與資料完成是不同結論；其中 `rotc_broker_transactions` 逾時已有逐鍵失敗收據及原有短期重試退避，不應直接取消證據閘門或宣稱 all services 正常。16:59 根檔案系統約 115 GiB 可用／使用率 95%。

## 9/24 17:20～17:29 舊歷史訊號可沿用假設遭反證

現存 13 GiB 隔日沖工作樹的主要容量是五個約 2.2～2.3 GiB 的不同日期完整 ledger，並非一份新重建就要再花 13 GiB；訊號快取共約 1.3 GiB。RAM 可用約 84 GiB，當下 memory／I/O full PSI 10 秒均值近 0，FOP 夜盤捕捉仍優先。容量雖可容下一次數 GiB 完整重播，但若每天累積一份而不建立有驗證的保留／回收契約，仍會加速用盡目前 115 GiB 可用空間。

對 9/23 14:28～14:29 當時的 209 日 `panel_cache_v2` 兩種模型版本（99／24 特徵）與目前重新從來源建置的 210 日面板，要求相同 symbol 與 feature schema，按相同日期逐一比對**所有 26 個已存陣列欄位**。兩個版本皆顯示 feature 在 9/16、9/21、9/22、9/23 不同；`can_short_open_mask` 有 13 日不同，`day_trade_can_short_open_mask` 有 51 日不同，`unresolved_corporate_action_mask` 有 19 日不同，另有報酬／量與現金股利欄位差異。兩個真來源面板建置各約 29.3／27.6 秒。9/23 快取不等同每個歷史訊號原始輸入，但這些較早日期的差異已足以**否定**「只有 9/24 需要重新計算」的捷徑；不能把 9/23 已產生權重不加驗證直接搬入新來源版本。此為唯讀診斷，沒有改任何當沖或隔日沖帳本。

## 9/24 17:30～17:42 官方來源修訂與隔日沖模型來源防線

17:33 的正式 `close_final` 掃描有 **1 個真實位元變動**：`tpex_daily_valuation`，舊／新 SHA-256 不同；completed-session 衍生服務因此用約 165.7 秒重建公開特徵，17:36:34 的收據 `status=ok`，不能將這次重建當作重複空轉。當時服務記憶體約 44 GiB，等待其結束後才進行下一輪隔日沖診斷，未和它爭用模型重播資源。

隔日沖歷史 plan 原只綁官方 OHLC、calendar 與分鐘 manifest；模型實際讀取的逐股 Parquet、外部公開特徵、公司行動與配置來源未被釘選。現在沿用 panel 本身的來源解析規則，枚舉這批模型檔、source config 與公司行動收據，逐檔做 SHA-256＋讀取前後檔案簽章檢查，並把列表與雜湊放入隔日沖 plan。輸入準備結束、推論開始／各模式結束、重播開始／每 20 日／發布前都檢查新增、移除與變動；未變檔案的中途檢查只看簽章，末端再全量 SHA。現場為 **2,768 個模型相關檔案／約 1.48 GiB**；單次發現約 0.625 秒、全量雜湊約 1.088 秒，這是額外的真實完整性成本，不是零耗時快取。舊 namespace 缺這份模型來源憑證，`--stage plan` 約 7.37 秒即拒絕舊訊號重用，訊息列 `model_inputs_unversioned(2768)`，這個數字**不是** 2,768 個實際改變的檔案。隔日沖歷史／來源 59 個測試、Ruff、編譯與 diff whitespace 通過；未發布 9/24 歷史、未改當沖帳本。

## 9/24 18:14～18:19 隔日沖新來源版本完整重建與公網驗收

17:30 真實公開特徵來源修訂完成後，以 `nice 19`／idle I/O 在**獨立工作區**完整重建隔日沖 146／146 日、4 模式的價格輸入、訊號與同一反事實撮合帳本；舊 9/23 工作樹未覆寫，當沖帳本完全未觸及。新的正式來源計畫約 693,140 bytes，模型 2,768 檔加官方 4 檔於 plan 驗證約 1.727 秒；整輪完成 18:14:29 並通過部署閘門。`overnight_history.json` 為 `end_date=2026-09-24`、146 日、1,168 個開／收盤權益點、1,208,073 筆歷史訊號、87,520 個事件；`overnight_history_deployment.json` 的結果／plan／歷史／訊號／事件 SHA-256 已寫入。新完整工作樹約 3.6 GiB，18:14 根檔案系統仍約 110 GiB 可用；這是一次性修復，**不是**已解決每日重播與保留的長期容量成本。

歷史發布狀態是 `ready_with_stale_unresolved_position`，保留 1 筆早期未解決持倉與 144 次新收盤訊號被阻擋；即時隔日沖 API 仍為 `critical`，100m 今天缺真實收盤撮合價而未建立部位，且共用開盤驗收問題尚在。公開 gateway 本機 `/tw-overnight/api/status` HTTP 200／約 74 ms，已讀到 9/24；DDNS 強制 IPv4 同 endpoint HTTP 200／約 422 ms（連線 331 ms、TLS 346 ms、首位元組 391 ms），同樣回 `health=critical` 且歷史 9/24。強制 IPv6 `/healthz` 3 秒連線逾時／HTTP 000，雙棧仍未修好。外部網頁抓取工具本身無法存取該站，故以部署主機的實際 HTTPS 路徑測速，不冒稱全球各地可達或瀏覽器端到端驗收。

正式 `stockagent-tw-overnight-history.service` 18:19 再啟動只用約 6.07 秒回 `already_current`，沒有重算或重啟交易；`Result=success`，當時 `systemctl --failed` 為 0。`already_current` 現會驗證發布 plan、官方與全部模型來源檔／清冊，而不只檢查日期。**明天新資料版本的持久增量結構仍未完成**，原排程若直接落入舊 cache namespace 仍會安全失敗；不能把今天手動新工作區的成功當成冷啟動與次日自動恢復已驗收。

重建完成後又移除一個來源驗收自身的重複運算：`infer_signals` 原在四個模式後各自以空簽章重新雜湊約 1.48 GiB 的相同檔案；現在沿用第一次全量驗證的 `(device, inode, size, mtime, ctime)` 清冊，後續模式只重新雜湊真正變動的檔案，並保留重播末端的完整 SHA-256 稽核。這個改動**晚於本次正式完整重播**，已有 59 個相關回歸與 Ruff 通過，但下一次真實多模式工作仍須分段測速確認節省量，不能把 1.1 秒單次雜湊直接乘四當作已量到的服務改善。

## 9/24 18:30～18:45 Binance 每週歷史回補的假缺口

舊 `registered-data-backfill` 9/20～9/21 連續跑約 65,733 秒，Binance 574 檔中 1 檔失敗，因此整批為 `completed_with_failures`；失敗的逐檔報表已被後續尾端排程覆寫，不能倒推該檔已恢復。9/24 14:00 的另一輪尾端＋特徵收據為 574／574 `updated`，只證明其範圍，**不證明歷史頭完整**。

舊歷史頭判斷直接把 `exchangeInfo.onboardDate` 當第一根 K 線時間。現存 574 檔的上線時間與本機首根 K 線比較，有 107 檔差超過 1 分鐘、32 檔超過 1 天、9 檔超過 2 天；最長 ICPUSDT 約 423.81 天。用[官方 USD-M Kline API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data) 的 `startTime`、`limit=1` 唯讀實測這 9 檔，**9／9 官方第一根時間均與本機一致**，各請求約 75.9～103.1 ms。這反證「晚於上線時間＝本機缺頭」；只用上線 metadata 每週可反覆讀取大量已存在資料。

已讓非 tail 的 Binance 增量回補在 metadata 指出缺頭時先以官方首根 K 線探測：官方首根不早於本機首根，便只做尾端重疊更新；官方確有更早 K 線，仍從其實際首根開始補。空回應不當作完整證據，格式／時間不合法則失敗，不改資料列或限流契約。每週非 tail 工作另在來源鎖尚由本輪持有時，把逐檔報表、summary、receipt、symbol 清單與特徵報表寫入 `step_receipts/<run-id>/binance_source/`；後續 tail 輪次覆寫 `data_binance/1m` 最新報表也不會失去當次失敗標的。這只作用於未來 Binance 回補，不碰當沖、隔日沖帳本。53 個 Binance／熱尾端／限流回歸測試、Ruff、Bash 語法與 diff whitespace 通過；**尚未在下一次完整每週排程測得總 wall 改善或證明那 1 檔舊失敗已修復**。

## 9/24 19:00 Windows／WSL 恢復測速與取樣界線

讀取**現有** Windows Caddy `startup.log`，不觸發重啟，將每次 `gateway backend healthy=False` 到下一個 `True` 配對為「已觀察到的後端恢復」，另記第一個 WSL 派送到健康、派送次數、已完成與仍在失聯的狀態。新收據 `artifacts/benchmarks/service-coverage-20260924T1900-startup-recovery.json` 覆蓋 50 service／39 timer／2 path；近 7 天可配對 10 次，最近一次 9/24 11:43 為 **5.604 秒**，最慢一次 9/24 08:49～08:54 為 **274.030 秒**，其中第一個 WSL 派送到健康為 **273.496 秒／18 次派送**。Windows 本次實際開機仍是 9/15 07:31，故 9/24 的長事件是 WSL／gateway 恢復，**不是 Windows 冷開機驗收**；也不能僅憑日誌定位延遲在 VM、磁碟或 Windows 哪一層。

初版收據雖指定 5 秒資源取樣，卻將 Windows 查詢置於兩次 Linux CPU 快照之間，實際間隔 **16.015 秒**；分母是真實 16 秒，沒有捏造 5 秒速率，但不同次 Windows WMI 耗時會令取樣不可比較。現把排程／Windows 查詢移到 `before` 之前，並在啟動鏈收據記錄自己的觀測時間。相同 `--sample-seconds 5` 的 `artifacts/benchmarks/service-coverage-20260924T1905-controlled-sample.json` 為 **5.144 秒**、清單仍 50／39／2、Windows 恢復事件仍 274.030 秒；整體命令 wall 約 9.7 秒，區分了稽核器額外成本與資源觀察窗。17 個測速／趨勢回歸、Ruff、編譯、diff whitespace 通過。沒有停止 WSL、Caddy、Discord、行情或當沖帳本服務。

## 9/24 19:10～19:23 網站非帳本路由冷建置優化

正式 loopback 五條非當沖路由、各五次新連線的前測收據 `artifacts/benchmarks/dashboard-routes-20260924T1910-nonledger.json`：流量歷史首次 **1,113 ms**，其中伺服器建置 **1,109 ms**，熱請求中位 **2.039 ms**；TAIFEX 1 日歷史首次 **1,764 ms**，其中上游代理／淨化建置 **1,735 ms**；功能清冊首次 **323 ms**、後續下載中位 **105 ms**，約 49.9 MB 解碼後 JSON 的單次客戶端解析約 0.47～0.53 秒。不同資料量與快取狀態不可直接比較，最初取樣不等於服務啟動後冷快取保證。

流量歷史 24h 是 15 分鐘趨勢，但原始 store 對每次冷查詢都把約 1,434 筆分鐘紀錄與巢狀路由 histogram 完整 `deepcopy`，然後查詢再只讀聚合。真來源唯讀 profiler 中深拷貝佔約 **1.08 秒／1.59 秒**（profiler 本身會放大 wall），因此保留 `rows_since()` 原有的隔離複製契約，新增**僅供內部流量歷史查詢**的近期唯讀視圖；舊資料／跨最近 2 日仍走原本複製與磁碟邊界。凍結最近兩日匿名 JSONL 到臨時目錄、不修改正式資料，`scripts/benchmark_public_traffic_history.py` 交錯各六次：深拷貝中位 **373.759 ms**、唯讀視圖 **168.728 ms**，12 份完整輸出（去除每次生成時間）SHA-256 全相同，收據 `artifacts/benchmarks/traffic-history-view-20260924T1920.json`。94 個公開閘道測試、加上測速與 Shioaji 流量相鄰共 **118 個**通過，Ruff、編譯與 whitespace 通過。

只重啟**唯讀公開 gateway** 載入程式；當沖、隔日沖、TAIFEX、Discord 的 PID／起始時間前後相同。重啟後第一筆流量歷史 HTTP 200，`Server-Timing` 明記 `cache=build`／`build=172.210 ms`，curl loopback 總 **174.000 ms**；相對前測的 **1,113 ms** 是兩個時間點的現場樣本，不能宣稱穩定改善倍數或 WAN p95。後測同五條路由收據 `artifacts/benchmarks/dashboard-routes-20260924T1922-traffic-view.json` 均 HTTP 200／零錯誤；流量歷史熱中位 **1.150 ms**。`systemctl --failed` 仍 0、gateway `/healthz` 200。資料監控清冊的約 50 MB JSON／客戶端解析，以及 TAIFEX 真來源首次建置仍是未完成的延遲工作，不應用流量頁改善代替全部網站優化。

## 9/24 19:30 全資料欄位清冊的未變更輪詢

全資料頁啟用欄位清冊後每 60 秒會再次請求完整 `/data-monitor/api/features`。目前正式回應約 49.9 MB 解碼後 JSON，gzip 傳輸 **1,203,636 bytes**，瀏覽器解析曾測約 0.47～0.53 秒；資料不變時這些傳輸與解析都不會帶來新資訊。閘道原已提供依完整回應位元計算的 ETag 和 `If-None-Match` 驗證，不需改資料來源或另開快取真相。前端現只在已驗證完整清冊後保存 ETag；下次帶條件請求，304 時讀完空 body、保留既有 rows／搜尋索引／畫面，200 時仍完整解析、檢查與重建。共用 fetch 測速將 304 記成成功的未變更輪詢，不把它算成錯誤。

正式 loopback 以相同 gzip ETag 實測未變更回應 **304／0 bytes／1.496 ms**；故意給不同 ETag 則仍為 **200／1,203,636 bytes**。這是每個保持開啟且清冊不變的頁籤，每分鐘避免一次約 1.2 MB 傳輸和一次巨大 JSON 解析的條件式效益；首次載入與清冊真的變更時的成本沒有消失。`node --test test/test_dashboard_core.mjs` **17/17**、全 45 個 `.mjs` 測試與前端語法檢查通過。隔離 Chromium 的真頁面重測收據 `artifacts/benchmarks/data-monitor-etag-browser-20260924T1937.json` 顯示 80,375 個欄位、搜尋結果與原條件一致、無橫向溢出及 console 錯誤；立即再整理一次時記錄到 304／`outcome=ok`，rows 與搜尋索引維持同一物件，首列 DOM 未替換，計數文字未變。此實測只涵蓋本機瀏覽器與 gateway，不代表外網各設備的速度。測試用獨立瀏覽器與約 25 MB 臨時 profile 已停止／清除；這次只修改靜態前端與測速腳本，公開 gateway 立即讀到新檔，未重啟任何交易、行情或公開服務，沒有修改當沖帳本。

## 9/24 19:45～20:00 WSL 恢復邊界與全資料快照成本

已在唯讀服務覆蓋稽核器加入**目前 WSL boot** 的 systemd userspace 時點。僅當該時點落在 Windows Caddy 同一筆「不健康→健康」事件內，才拆出首個 WSL 派送→Linux userspace 與 userspace→gateway 健康；舊 boot 事件保留未知，不拿目前 boot 時鐘錯配舊紀錄。新收據 `artifacts/benchmarks/service-coverage-20260924T1945-userspace-breakdown.json` 仍為 50 service／39 timer／2 path，1 秒指定資源取樣實際 **1.145 秒**。9/24 08:49～08:54 的 **274.030 秒**失聯中，首派送→userspace **260.353 秒**，userspace→健康 **13.143 秒**；此區分將瓶頸放在 Windows 派送至 WSL Linux 可用的邊界，但**尚不能**在 Windows 排隊、VM 建立、磁碟或時鐘同步之間進一步歸因，也不是 Windows 冷開機測試。本次讀取 root crontab 為「no crontab」，root user systemd 沒有 StockAgent unit，Windows 相關工作名稱仍是 4 個；`supervisorctl` 與 `atq` 此主機未安裝，不能因此推斷遠端或所有其他使用者沒有背景工作。18 個覆蓋／趨勢回歸測試、Ruff 通過，未重啟任何服務。

每 30 秒的 `stockagent-data-refresh-status-snapshot.service` 是另一項持續成本。19:45～19:49 的正式 `data_monitor_timing` 多筆樣本約 **2.15～2.57 秒 wall**／**2.06～2.27 秒 process CPU**；例如 19:46:50 這次 2,344.841 ms 中，服務狀態 114.090 ms、56,696 檔的 record inventory 1,272.046 ms、1,632 個來源的公開投影 930.198 ms、已驗證重用的 feature projection 27.867 ms。inventory 當次 `refreshed_files=0`，代表仍逐檔確認來源簽章、不是重讀 Parquet footer。獨立 profiler 未帶正式 `STOCKAGENT_COLD_INVENTORY_CACHE_PATH` 時會重驗大份冷庫 inventory，**不能**拿其 1.7 秒直接當服務瓶頸；帶正式快取後，永豐監控的讀檔器單次仍呼叫約 1,097 次，其中歷史合約 manifest 目前實存 **768 份**。這些驗證與資料健康有關，尚無等價的失效證據機制前不刪掉或任意延長 30 秒新鮮度契約。OpenBB 服務因 provider cooldown 等到 9/25 04:05，`systemctl` 顯示 cgroup 約 **2.4 GiB** 記憶體，但 `/proc/5927/status` 的 Python RSS 只有約 **223 MiB**；`memory.stat` 中約 **2.24 GiB 是 file cache**、匿名記憶體約 **187 MiB**。cgroup 用量不能直接稱為行程 RSS，不能以此推論中途退出可省 2.4 GiB；本輪沒有中斷該活服務或改其冷卻流程。

為避免往後再將檔案快取誤判成行程堆記憶體，全部 service 的資源快照／5 分鐘歷史現同時保留 `memory_current_bytes`、`memory_anon_bytes`、`memory_file_cache_bytes`，後兩者來自同一 unit 的 cgroup `memory.stat`；這三欄不是互斥加總，kernel/slab 等仍占其餘部分。新收據 `artifacts/benchmarks/service-coverage-20260924T2010-memory-split.json` 的 **13/13 個 active unit** 都取得 anon/file 分拆，OpenBB 分別是 **196,161,536／2,403,622,912 bytes**，公開 gateway 是 **748,625,920／220,868,608 bytes**。正式 20:09 的 5 分鐘 `all_service_runtime_sample` 也已帶出相同 OpenBB 分拆；36 個相關回歸、Ruff、編譯與唯讀現場稽核通過。這是觀測修正，尚未降低服務用量；後續優化必須按 anon、file cache、服務輸出與資料健康分別驗收。

20:12 對 record inventory 的無變動分支做小型整理：56,696 個已選檔仍逐一核對 `stat`，但確認集合與檔案簽章未變後，不再重建 schema 引用圖；若新增／刪除檔案、檔案在核對時不可用或有新 footer，仍走完整重算。原實作在 `max_refresh_files=0` 且剛新增檔案時可能錯用舊資料集總計，現在新增的回歸要求該資料集 `files_total` 立即變 2、`count` 保持未知。實檔 5 次前測 1,254～1,360 ms，中位約 **1,308 ms**；後測 1,290～2,613 ms，中位約 **1,351 ms**（含同機並行排程干擾），**沒有可驗證的 wall-time 改善**，不能宣稱已解決 1.2 秒掃描成本。相關 Python **110** 個測試、Ruff 通過；其中一個既有全資料頁靜態測試還期待舊 `fetchJson("api/features")`，已改為驗證上輪的條件請求與 304 路徑。後續若要真正降到亞秒，需要在保留新增、刪除、同名改寫與資料健康語意的前提下，建立可驗證的變更索引或事件驅動失效，而非單純延長輪詢週期。

20:20 再補一個相同捷徑的邊界：**既有同名檔已改寫、footer 額度為 0** 時，不能因 `refreshed_files=0`、檔名集合不變就沿用舊的 `verified` 總數。現在只要掃描遇到簽章變動，即使因配額中止，也不走舊 aggregate 捷徑；資料集降為 `scanning`、`count=null`，待下一輪讀到 footer 才恢復精確值。舊版本快取也不得直接重用新版本 aggregate。新增實檔同名改寫回歸後，監控／測速相鄰 Python **111** 個測試通過。20:19 正式快照自然執行，`inventory` 約 1.21 秒；此修正是防止錯誤綠燈，**不是延遲已改善的證據**。唯讀 gateway `127.0.0.1:8770/healthz` 及 `/data-monitor/api/status` 均 HTTP 200（單次約 1.6／133 ms），`systemctl --failed` 仍為 0；這只證明可達性，不代表其他資料與交易健康。

## 9/24 20:20～20:28 TAIFEX 唯讀面板的雙來源健康狀態

盤中／夜盤的 TAIFEX `stockagent-shioaji-taifex-bidask.service` 行程仍在，日誌顯示 capture `data_only`；策略啟動的官方結算階段因「模擬期貨避險部位仍未平」而失敗。`state.json` 的 `updated_at_utc=2026-09-24T06:51:35Z`、`engine_status=blocked_subscription_bootstrap_settlement` 與明確原因是**較新的策略狀態**；`status.json` 最後是 9/18，不能拿前者時間當作今天有可成交行情或新估值。舊面板只用後者判健康，20:20 的 `/healthz` 因逾時回 503 `stale`，但沒有顯示今天的策略阻擋。

現在唯讀投影只在 `state.json` 的**明確訂閱啟動結算阻擋**比行情 `status.json` 新時採用其引擎狀態，公開文字僅顯示安全的原因類別，不把狀態檔中的任意例外字串／私有路徑直接公布；`source_updated_at_utc`、`source_age_seconds` 仍嚴格來自舊行情檔。前端同時寫出「策略阻擋」與「最後行情快照逾時，不能當即時估值」。這是來源／策略健康的分離，不產生、修補、結算或沖銷任何帳本與部位。TAIFEX 面板與策略相鄰 **67** 個回歸測試、Ruff、Python/JS 語法和 diff whitespace 通過；現場唯讀快照回 `health=blocked`、`engine_status_source=state`、`source_age≈542,447s`。只重啟 `stockagent-shioaji-taifex-dashboard.service` 載入唯讀程式；行情擷取 PID 149、當沖服務 PID 54773 均未變。重啟後 `/api/status` HTTP 200 且顯示上述狀態，`/healthz` 仍 **503 blocked**，這是正確的失敗訊號，不是策略已修復。正式避險部位／官方結算修復不在本輪；依衍生品契約不能為了消除紅燈自行沖銷或假造結算。

同一時間 20:25 排程的 `stockagent-tw-overnight-history.service` 自行失敗，原因是來源修訂與既有 `c275c5f...` cache namespace 不相容（`minute_manifest`、`twse_daily`、2,768 份未版控模型輸入）；`systemctl --failed` 因而不是 0。本輪沒有啟動它、改隔日沖或當沖帳本，也沒有清掉這筆失敗證據。這項與 TAIFEX 面板重啟不同，必須由該歷史工作流使用新的、完整驗收的 lineage 解決，不能把失敗標成健康。

## 9/24 20:30～20:45 WSL VM 啟動前等待的跨平台時點

對 08:49～08:54 這筆 **274.030 秒**後端失聯，前一個 WSL boot 的 systemd journal 在 **08:49:23** 明確進入 `poweroff.target`／`The system will power off now`，08:49:25 結束。這是正常關機路徑，不能寫成「已證明 VM 崩潰」；目前也沒有可驗證的呼叫者，不能猜是使用者、測試腳本或 Windows 排程。Caddy 首次派送 `wsl.exe` 約 08:49:30.647。Windows `System` 的 Hyper-V VmSwitch 事件 102 與**目前執行中的 `wslhost.exe --vm-id`**相符，最新網路驅動載入是 **08:53:50.5046395**；最早相同 VM 的 `wslhost.exe` 建立於 **08:53:51.480481**，systemd userspace 的秒級時點是 **08:53:51**，gateway 健康於 08:54:04。由此得到首派送→VM 網路驅動約 **259.858 秒**，驅動→userspace 約 **0.495 秒**，userspace→gateway 健康約 **13.143 秒**。`wslhost` 的亞秒建立時點略晚於 systemd 秒級時點，稽核允許至多 1 秒顯示精度差，不硬把它解讀為 Linux 先於宿主行程啟動。

`scripts/audit_service_latency_coverage.py` 現用唯讀 Windows WMI 與 VmSwitch event 102 取得**當前** VM 的相符時點；多個 VM ID、沒有事件或時序不符就保持未知，舊 boot 事件不借用新 VM 證據，也不公開原始 process command line。新收據 `artifacts/benchmarks/service-coverage-20260924T2043-vm-boundary.json` 與最終覆核 `artifacts/benchmarks/service-coverage-20260924T2052-vm-boundary-final.json` 都覆蓋 **50 service／39 timer／2 path**；前者資源取樣實際 1.157 秒，Windows 事件匹配 4 筆、同 VM 的 `wslhost.exe` 6 個。這將長等待定位在 Windows 派送之後、目前 VM 網路驅動可用之前，但仍不能在 `wsl.exe` 退出／重試、VM 建立、儲存或宿主資源競爭之間分出因果。當時 `Tcpip` 4227 與反覆 VmSwitch 285 warning 也存在，卻不是單憑相近時間就能定因；後者在事前事後也每分鐘出現。

Windows Caddy launcher 目前只記派送、不記每次 `wsl.exe` 的**結束碼與耗時**，無法解釋 18 次派送是快速失敗還是短暫成功但 VM 仍未可用。已在 repo 的 `scripts/start_windows_public_caddy.ps1` 增加非阻塞、只對已結束子程序寫出 `exit_code`／`elapsed_seconds` 的日誌，保留同一個 supervisor、10 秒重試、只操作唯讀 gateway 的邊界。**尚未重裝或重啟 Windows 排程**，最終收據的 `installed_caddy_files.launcher.exact_match=false` 如實表示目前仍執行舊版 launcher；下次安全安裝後的自然事件才能驗證新欄位。Windows PowerShell parser 通過，啟動鏈／覆蓋測試共 26 個、Ruff、Python 編譯與 diff whitespace 通過。沒有執行 `wsl --shutdown`、沒有關閉 Caddy、行情、Discord 或當沖服務；三個關鍵行程 PID 分別保持 TAIFEX capture 149、當沖 54773、公開 gateway 610597。此時 systemd 仍因隔日沖歷史工作失敗為 `degraded`，所以不能宣稱整體服務或 Windows 真正冷開機恢復已驗收。

## 9/24 21:05 systemd 以外的專案行程候選覆蓋

`systemctl list-unit-files` 的 **50 service** 是已安裝單元，不等於機器上所有可能的手動背景程序。唯讀覆蓋稽核器現在另外掃 `/proc`，只看目前程序的 cwd 是否在 repo 內，或前 64 KiB argv 是否含 repo 絕對路徑；**只輸出 PID、PPID、程序短名、配對依據、所屬 StockAgent unit 與單行程 RSS**，絕不輸出 argv／命令列、環境變數或開啟的檔案。稽核程序自己的父子鏈排除，避免把本次測速當成漏管服務；無權讀取、程序已退出、遠端主機、Windows 非 Caddy 行程、只持有 repo fd 而 cwd/argv 不含路徑者仍不在此掃描範圍。

收據 `artifacts/benchmarks/service-coverage-20260924T2105-proc-candidates.json` 量到 **50 service／39 timer／2 path**，掃描 103 個 `/proc` PID，47 個符合專案上下文，其中 **36 個行程歸屬 StockAgent systemd unit、11 個為未歸屬候選**。11 個短名組成為 `MainThread` 3、`bash` 3、`sh` 3、`htop` 1、`nvtop` 1，皆以 cwd 配對；其父子關係符合目前編輯器／終端／監看工作，但**未經命令來源與啟動器證明，不能稱 11 項獨立背景服務，更不能因這次沒看到其他名字就宣告全機無漏項**。行程數只是瞬間快照，與 50 個已安裝 unit 並非同一統計母體，也不能相加。相鄰 `/proc`／監控回歸 20 個、Ruff、編譯通過；全服務仍有單次操作 p95 與正式業務收據缺口，這項只補發現層。

## 9/24 21:15 永豐監控的冷／熱成本與優化邊界

單獨以正式資料建置 `build_shioaji_public_status`，同一行程連做五次：首次 **597.297 ms**，接下來 **64.853／57.779／68.712／66.146 ms**。這些是本機單次樣本，不是 p95；跨行程的每 30 秒資料監控快照仍會重新負擔冷讀，不能把同一行程熱值當成正式排程耗時。唯讀 profiler 的首輪約 0.556 秒，涵蓋 1,097 次 `_read_json`、約 768 份不同期貨歷史合約 manifest，以及 journal 的解析；`_history_manifests` 約 0.245 秒、其餘 pipeline 建置約 0.288 秒。現有 JSON 讀取依裝置／inode／大小／mtime／ctime 核對檔案，並在讀取後再確認簽章；不應為了省掉冷讀而無憑證地延長資料新鮮度、跳過修改或讓交易行程承擔監控計算。下一步若要壓縮正式冷成本，應先建立可驗證的跨行程收據變更索引與失效測試，逐項比對完整投影等價後再部署；本輪沒有修改 Shioaji 連線、配額、捕捉或監控來源語意。

## 9/24 21:58～23:21 排程覆蓋與當沖故障隔離

唯讀覆蓋器新增 `/etc/cron*`、root crontab、root user systemd 與 Windows 排程工作動作中的專案線索；只計數與列安全的工作名稱，不公開命令參數或憑證。`artifacts/benchmarks/service-coverage-20260924T2321-post-fault.json` 覆蓋本機 **50 service／39 timer／2 path**，13 個 cron 檔案均可讀、root crontab 為空、root user manager 的 54 個 unit 名稱沒有 StockAgent 匹配；Windows 查到 4 個相關工作。這不是其他 Windows 使用者、遠端節點或所有短命程序的全域證明。當前唯一 `failed` 的已安裝 unit 仍為 `stockagent-tw-overnight-history.service`；其來源版本／快取不相容尚未在此線解決。主機 23:21 的 10 秒記憶體 PSI 為 0，但 21:58 曾量到 `full avg10=35.84`，須區分時點。

9:37／9:39 當沖 unit 曾被 90 秒 outer watchdog 殺死；9:43 唯讀 dashboard 的 8766 port 衝突又使原 `wait -n` 包裝器連帶退出，systemd 連續重啟整個引擎。現已讓引擎為 critical child、面板為可獨立重試的 child，重試間隔 2／4／8／16／30 秒封頂；交易／帳本邏輯未改。22:34 收盤後僅重啟此 unit 載入包裝器；22:35 對**確認在相同 cgroup 的唯讀面板 PID**送 TERM，面板重新監聽 8766，而引擎主 PID `787958`、InvocationID `14def02ad64347fc97006a94bf6eba40` 沒變，`NRestarts=0`。測試前後當沖帳本指紋同為 `8ee9bca3f33088c18914d9e1ac2863dbcd496cb60bee04918894476492d4289f`；公開 API 仍 `health=degraded`、帳本完整性 ready、Discord 同步零落後。程序級故障隔離測試含持續崩潰退避、引擎退出傳遞，加上相鄰監控／守護／隔日沖測試共 **53 passed**。這證明**面板子程序故障不再直接重啟引擎**，不證明 9:37 的長時間 panel/模型建置停頓已根除，也不代表 198 筆當沖未平已結清。

23:21 從部署主機走 DDNS 強制 IPv4 的當沖狀態單次請求 HTTP 200、總耗時約 38 ms；這不是跨地區 p95 或資料健康證據。Windows 已安裝 Caddy launcher 仍是舊檔；repo 的新 WSL 派送完成碼／耗時日誌尚未部署，不能宣稱下次冷啟動已驗收。

23:45 再以同一 gateway 做 **39 條有限路由、每條 2 次、單一客戶端** 的可重測基線，收據在 `artifacts/benchmarks/dashboards/http-20260924T2345-system-baseline.json`；39 條皆 HTTP 成功。首次觀察（不保證冷快取）最慢的是日期範圍當沖訊號約 3.23 秒、完整當沖分鐘史約 0.984 秒、overview 約 0.877 秒、TAIFEX 單日史約 0.553 秒、全資料 feature 約 0.428 秒；同輪第二次分別約 6.3、95.5、1.6、25.5、153.9 ms。兩次樣本不足以估 p95 或承諾服務水準。面板 payload 仍如實為 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `critical`、Shioaji `degraded`、全資料 `critical`；OpenBB 顯示 `active` 亦不可推論每筆來源新鮮。使用中的 gateway、行情擷取與交易引擎未因測速重啟。

所有目前安裝的 StockAgent service／timer／path 經 `systemd-analyze verify` 無設定語法錯誤；這不是業務健康檢查。當沖子程序、守護器、服務監測、隔日沖歷史的相鄰回歸 **53 passed**，`bash -n`、Ruff 和 `git diff --check` 均通過；整庫測試與真正斷電冷啟動未驗收。

## 9/25 00:18～00:33 隔日沖來源版本自動隔離與真資料驗收

隔日沖歷史排程先前固定寫入模型 lineage 目錄；官方來源變了卻仍嘗試重用舊 `inputs`／`signals`，正確性檢查只能反覆拒絕。現在由官方全域來源、2,768 個模型輸入、逐交易日漲跌停檔的內容 SHA-256 與模型 lineage 決定 `source_generations/<sha256>`；同版中斷可續跑，變版不借用舊訊號。完成後 maintenance 只接受自己宣告的子目錄、相符的交易日／lineage／來源版本、完整逐日漲跌停清冊，重新核對來源才發布。舊部署缺逐日清冊時仍逐日核對原始輸入收據，不因版本缺欄而假裝來源未變；失敗或資料修訂保留為待重建。此版本隔離**沒有宣稱**跨來源版本的歷史訊號可安全增量沿用，也不自動刪除舊版本。

00:18:39 用正式 `stockagent-tw-overnight-history.service` 在原有低優先權設定下從新來源版本重建；00:30:32 成功退出，146／146 日、四模式均完成，wall **11 分 52.572 秒**、CPU **17 分 8.939 秒**、cgroup 峰值 **12.1 GiB**。階段時點：輸入最後一日 00:19:09、模型訊號四模式結束 00:24:47、回放 00:25:02～00:30:11；不能把 6 秒 `already_current` 當作來源重建延遲。發布結果為 1,208,073 筆歷史訊號、87,520 個事件，狀態 `ready_with_stale_unresolved_position`，仍保留 **1 筆歷史未解決持倉、144 次後續收盤訊號阻擋**。00:33:09 用新版程式正式再啟動同一個 history unit，6.583 秒回 `already_current`，`systemctl --failed` 暫為 0；這不代表即時隔日沖與其他資料健康已全綠。新版逐日漲跌停版本計畫在同一真來源上另做 `--stage plan`，146 個交易日都有 SHA，沒有再次重跑全史。隔日沖版本／拒絕錯誤路徑 **15** 個測試、Ruff、diff whitespace 通過。

容量仍是長期風險：舊固定 lineage 工作樹約 13 GiB，新版每次全來源修訂會產生新完整版本，這次開始前根檔案系統僅約 108 GiB 可用。未建立可驗證的逐 session 增量投影與保留／回收證據前，不應把每日全史重算稱作效能已最佳化，也不能自行刪除仍被部署或稽核引用的版本。正式 TAIFEX 夜盤行情、當沖引擎、Discord 都沒有因本次重建而重啟。

最終相鄰組合（當沖子程序／守護器、全服務覆蓋／長期測速、隔日沖來源／歷史／回放／模擬）**111 passed**；Ruff、shell 語法、`git diff --check` 通過。部署後本機隔日沖 API 在休市時回 `waiting`，歷史 9/24；DDNS 強制 IPv4 同 endpoint 單次 HTTP 200／約 343 ms。當沖仍 `degraded`、TAIFEX `blocked`、全資料 `critical`；這些不受隔日沖歷史排程成功所消除。

## 9/25 00:45～01:43 失敗證據、產品探針與全資料摘要快路徑

全服務覆蓋器仍列出本機 **50 service／39 timer／2 path**，但先前對已卸載的 oneshot unit 可能誤讀 `systemctl show` 預設的 `ExecMainStatus=0`。現以**相同 boot／invocation** 的 systemd `Process ... exited` journal exit 欄位校驗，長期測速趨勢同樣修正；`artifacts/benchmarks/service-coverage-20260925T0050-exit-proof.json` 因此將每週 `stockagent-registered-data-backfill.service` 正確記為 exit 1／failed，而不是假成功。再配對 `registered_{daily,intraday,features,backfill}_runs.tsv` 的起始與耗時收據，`artifacts/benchmarks/service-coverage-20260925T0100-business-receipts.json` 確認每週那次為 `completed_with_failures`、失敗步驟 `binance_perpetuals`、約 **65,733 秒**；後續尾端更新不能倒推全歷史已修好。一般執行時間、業務收據與資料完整性維持分開。

覆蓋器另加六個**本機、唯讀**產品 GET 探針；單次耗時含本機 HTTP 傳輸，但不是外網、瀏覽器、券商下單或上游冷來源延遲。最終 `artifacts/benchmarks/service-coverage-20260925T0145-after-summary.json` 六項均 HTTP 200，TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、永豐 `degraded`、OpenBB `active`、全資料 `critical`。這些健康值依原來源保留，不以成功回應取代。當次 Shioaji 約 **430.5 ms**、隔日沖約 **226.7 ms**；各只有一筆，不是 p95。Shioaji 狀態建置的獨立 profiler 冷約 **0.60 秒**、同一行程熱約 **0.12 秒**，主要是本機約 1,097 份 receipt／manifest、journal 與檔案核對；本輪沒有降低其來源檢查、延長快取新鮮度、使用券商連線或宣稱這兩條已最佳化。

全資料 `/data-monitor/api/summary` 之前每次建置先把約 **6.31 MB** `public_status.json` 整份 JSON 反序列化，然後只抽約 **152 KB** 摘要。現沿用既有每 30 秒快照工作，在完整快照原子發布後，同時寫 `public_summary.json`；摘要及完整檔的裝置／inode／大小／mtime／ctime 與 SHA-256 必須一致，且唯讀／不可控制正式服務的欄位必須有效。任何缺檔、版本錯配、內容改寫或摘要建置失敗都回退原完整路徑，不將失敗標為健康。正式檔單次本機函式比較：驗證並讀小摘要約 **6.0 ms**，原完整 JSON 抽取約 **78.6 ms**，兩者完整投影相等；這是同一時點一筆測速，不是 p95。只重新啟動**唯讀公開 gateway** 後，實際 loopback 首次 HTTP 200、`Server-Timing cache=build/build=8.083 ms`、curl 總 **10.517 ms**，熱請求 **2.081 ms**；重啟前一次 loopback 摘要約 **216.9 ms**。快照每 30 秒更新、閘道摘要 8 秒 TTL，跨世代瞬間可能讀到上一個已驗證版本，不能宣稱每一毫秒都和最新檔案相同；隨後現場比對生成時點及全部摘要欄位一致。健康仍 `critical`。相關公開閘道、資料監控、全服務稽核與趨勢 **195 passed**，Ruff、`git diff --check` 通過；`systemctl --failed` 為 0。未執行全庫測試、Windows 真冷啟動或外網瀏覽器驗收。

第二次 gateway 重啟後立即的第一筆剛好落入快照世代交替，安全回退完整 JSON，`build=75.995 ms`／curl 約 **90.580 ms**；當前 sidecar 再核對為有效約 **6.827 ms**，後續請求仍照原有 stale-while-refresh 契約返回。此例說明新快路徑不是 100% 命中保證；優化的是匹配世代的建置成本，沒有犧牲跨檔一致性。

## 9/25 01:48～01:55 持續快照成本與信任邊界

每 30 秒的全資料快照仍是持續 CPU 成本。唯讀拆解中，約 56,988 個已選檔的檔名發現／排序是其中一段；對所有固定、單層 `*_features.parquet` 來源改用一次 `os.scandir`，保留 `Path.glob` 的隱藏檔、同名目錄與檔案篩選語意，其他遞迴／混合樣式仍沿用原路徑。交錯十輪、同一實際來源集合的局部測速：舊發現中位 **466.8 ms**、新版 **284.7 ms**，兩份 56,988 檔的完整分組／順序相等。這只證明發現階段；正式快照的 inventory 階段仍約 **1.31～1.37 秒**（01:54:50、01:55:20 兩輪），不能將約 182 ms 局部差額當作已量到的端到端節省。36 MB 的 inventory 快取解碼、逐檔簽章核對和約 1,633 來源公開投影仍有成本；沒有跳過新增、刪除或同名改寫檢查。

調查期間兩次約 2.4～2.6 秒的 87,606 欄清冊重建，時間剛好與本輪監控程式碼修改吻合；現有重用契約要求程式碼變更後重建，後續連續四輪 `feature_reused=true`、每輪約 31～38 ms。故不能把這兩次判成無變更狀態下的週期性故障，也沒有為此放寬失效規則。對全資料公開投影做獨立 profiler 時，**未帶正式 `STOCKAGENT_COLD_INVENTORY_CACHE_PATH`** 的一次會重解約 175k 行冷庫 inventory，約 3.5 秒；帶正式快取才約 1.54 秒（仍含 profiler overhead），主要是 Shioaji 本機狀態約 0.57 秒、FinLab 來源約 0.27 秒與其他行列整理。不能拿漏掉正式環境變數的結果歸咎於實際排程。

小摘要快路徑再比照既有功能清冊的本機信任邊界：只讀同擁有者、不可由群組／其他使用者改寫的普通 sidecar，`O_NOFOLLOW` 禁止符號連結；不符時回退完整快照。安全／監控／公開面板相鄰 **200** 個 Python 測試、Ruff 和 diff whitespace 通過。僅重啟唯讀公開 gateway 載入新版；有界重試後摘要 HTTP 200、`Server-Timing build=19.225 ms`（包含重啟後首次來源檢查），熱請求約 **2.309 ms**，`/healthz` 200，`systemctl --failed` 為 0。重試的 curl wall 含一次連線被拒與 1 秒重試，不能當作實際請求延遲。行情、當沖、Discord、TAIFEX 擷取沒有因這次部署重啟，且資料健康紅燈未被消除。

## 9/25 02:50～03:04 跨日期訊號成本與非預期退出保護

當沖訊號頁 8/1～9/24 首次約 **655 ms**、2/25～9/24 首次約 **2,245 ms**（各兩次的正式 loopback HTTP 測速；後續同查詢命中頁快取約 **5 ms**）。完整範圍有 146 個交易日投影、約 **2,004,142** 筆訊號，成本主要在逐日投影的驗證／載入、跨日期統計與排序，不能把熱快取時間當冷建置或宣稱一秒內。若單獨呼叫 Python builder，必須帶服務正在使用的 `STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR` 與相同 `maximum_scan_rows`；漏帶投影快取時曾量到約 **21.65 秒**，那是不同執行路徑，不能用於正式服務前後比較。一次縮欄再回讀當頁的實驗使 100 筆結果跨 69 個日期重讀，未能證明更快或完整輸出相等，已撤回；合併兩次方向統計掃描也未量到可靠的端到端改善，已撤回。保留現有訊號與成交語意，後續需用同一資料版次做逐 session 輕量索引與完整欄位回補的等價驗證，而不是直接對正式帳本動手。

當沖 service launcher 的故障隔離已有引擎／唯讀面板兩個子程序；本輪補正引擎**意外以 0 退出**時，包裝器仍以非零結果退出，避免在 `Restart=on-failure` 等監督政策下被當作正常停機。現場 unit 是 `Restart=always`，故這是防止設定漂移與獨立執行時漏復原，不是假稱已修好既有中斷。新增乾淨退出的程序級測試，合計 **8 passed**；`bash -n`、Ruff、diff whitespace 通過。未重啟仍在運行的當沖引擎，只有下次正常重啟後才會載入新版 launcher。

03:03 唯讀覆蓋收據 `artifacts/benchmarks/service-coverage-20260925T0310-stability.json`：本機 50 service／39 timer／2 path，`systemctl --failed` 為 0，gateway `/healthz` HTTP 200；六個產品 GET 均 HTTP 200，但 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、永豐 `degraded`、全資料 `critical`，不能稱業務全部恢復。當沖資格 watcher 仍 `activating`，最近兩次各跑滿約 90 分鐘後因 TWSE 官方 master 宣告日仍為 9/24 而寫 `publication_pending_timeout`，30 秒後由 systemd 重試；這是**來源等待，不是交易引擎中斷**，但每輪約 2,600 次 2 秒輪詢的長期網路成本尚未優化，不能把 `activating` 當已取得 9/25 資格。Binance archive 最近一次 exit 75 是磁碟可用空間低於既定 10% 安全保留時在遠端發現前拒絕，非下載成功；registered backfill 最近的業務收據仍是 `completed_with_failures`，失敗步驟 `binance_perpetuals`。本輪未繞過磁碟閘門或重跑 18 小時回補。

## 9/25 03:05～03:20 跨日期訊號首建置剖析與記憶體邊界

用正式投影快取環境與相同 2/25～9/24／`offset=17`／`limit=100` 再做單獨進程剖析：完整輸出 SHA-256 `cd6fa9c9c1fb3b3e98ee9729c352bccc340f843042055eeede8c8df46083df42`、總數 2,004,142、wall 約 **2.63 秒**、峰值 RSS 約 **2.67 GiB**。`cProfile` 的另一個 offset 樣本約 **2.95 秒**，其中 146 個逐日投影載入約 **1.13 秒**、跨日期統計約 **0.81 秒**、首次行事曆檢查約 **0.34 秒**、`top_k` 約 **0.20 秒**；這些是同一 call 的累計成本，不是可直接相加的獨立服務延遲。再次嘗試持有全幅 shard、在窄欄上做統計與排序再按原 row index 回補，完整回應 SHA 相同，但 wall 變成約 **3.08 秒**，已撤回；不以較慢或記憶體較大的路徑換取看似簡單的欄位數下降。

對明確結束於過去的訊號日期範圍，現在不再查當前盤前顯示日期；今天及未來範圍仍保留原行事曆路徑，日期無效仍由原驗證器拒絕。相同真實輸入的冷建置樣本約 **2.47 秒**、完整回應 SHA 不變，峰值 RSS 約 **2.60 GiB**。這是約 0.15 秒的單次差值，沒有足夠重複樣本宣稱穩定端到端加速。歷史／當日行事曆分支回歸及當沖／公開 gateway／監督器完整相鄰測試 **252 passed**、Ruff、diff whitespace 通過。此程式變動尚未重啟唯讀 gateway 載入；正在運行的交易引擎未動。

當前公開 gateway 的 systemd `MemoryCurrent≈3.34 GiB`、`MemoryPeak≈3.38 GiB`，`MemoryHigh=4 GiB`、`MemoryMax=8 GiB`，本次 `memory.events` 的 high／oom 均為 0。部署模板原「峰值低於 1 GiB」註解已不符合現場，改為具日期的量測邊界；**未調高限制**，因單次／空閒值不足以推論併發安全。下一個具體效能工作應是逐 session 聚合索引與有界熱門頁，避免每種 offset／篩選都重新解碼全史；需同時驗證歷史訊號輸出等價、失效、併發 RSS 與來源修訂，不能只比一次快取命中。

## 9/25 03:20～03:27 公開安全淨化器的重複欄位判定

廣範圍訊號在內部頁快取已命中時，gateway 每 2 秒失效的 HTTP 回應仍須重新安全淨化及序列化約 1.12 MB JSON。實際 100 筆頁的淨化原約 **60～80 ms**，`cProfile` 顯示 31,247 次重複鍵名正規化、約 44,824 次遞迴呼叫；與舊的 1 秒模型解釋檢查不能混為同一成本。現在將不變的私有／憑證鍵集合與正規表示式移到模組常數，僅對長度不超過 128 的鍵名快取「丟棄／錯誤遮罩／保留」判定，LRU 最多 512 個鍵；長鍵仍完整檢查但不駐留。這**未省略任何欄位安全檢查**，真實整頁用舊演算法在同一程序重算，公開投影完整相等，SHA-256 `a7bbfb786584b21f5751c23e206e424cad4c099d20edbcc33762c6d2731bc402`。新增長鍵與重複鍵的憑證／路徑／錯誤遮罩回歸。

相同單獨程序、同一頁的新版淨化 10 次樣本首筆 **38.64 ms**、後九筆中位 **20.02 ms**。當沖／公開 gateway／監督器相鄰 **253 passed**、Ruff、diff whitespace 通過。03:17 僅重啟唯讀 gateway 載入新程式：首次正式完整範圍 HTTP 200，`Server-Timing build=2361.097 ms`／curl 約 **2.367 秒**；隔超過 2 秒的下一筆熱頁重新建置約 **139.133 ms**／curl 約 **144.6 ms**。上版現場同類熱頁單次約 177～198 ms，但不是交錯控制實驗或 p95，不應宣稱穩定改善幅度；**冷來源首次仍逾兩秒**。本機與 DDNS 強制 IPv4 `/healthz` HTTP 200，`systemctl --failed` 0，當沖引擎 PID `787958`／InvocationID 未變、`NRestarts=0`，訊號帳本 dev／inode／size／mtime 未變。產品資料健康紅燈未因此消失。

## 9/25 03:22～03:28 訊號頁持倉覆蓋與冷啟動驗收

當沖寫入端計算 `dashboard_content_revision` 時刻意排除 `positions`，避免每分鐘估值使訊號全史重建；但訊號頁在選出當頁訊號後，又會從目前 `positions` 覆蓋後續完成成交的股數、價格與狀態。既有頁快取只看帳本與該 content revision，因此**帳本不新增時，持倉完成成交或覆蓋移除都可能一直顯示舊頁**。先加失敗回歸（0→1,000 股仍顯示 0），再把快取條目明確分成原始當頁列、已覆蓋的呈現列、覆蓋欄位指紋與模型解釋檢查時間；只對實際用到的持倉欄位取指紋。覆蓋變更只重套保存的當頁原始列，不再讀完整訊號帳本；持倉移除時能回到帳本原始 0 股。與每分鐘 `last_mark_at` 無關，保留既有「不重掃」契約。這不建立、修改或沖銷成交，也不把帳本審計改成券商填單證據。

真實 2,004,142 筆廣範圍輸出的完整 SHA-256 仍為 `cd6fa9c9c1fb3b3e98ee9729c352bccc340f843042055eeede8c8df46083df42`；隔離進程首次約 **2,539 ms**，同查詢後兩次約 **9.01／6.97 ms**，樣本不足以聲稱 p95 改善。當沖／公開 gateway／監督器 **254 passed**，隔日沖模擬與歷史相鄰 **25 passed**；Ruff 通過。先只重啟公開 gateway，再對同 cgroup、非引擎 PID 的唯讀 8766 子程序送 TERM，監督器約 2 秒後恢復面板；當沖引擎 MainPID `787958`、InvocationID `14def02ad64347fc97006a94bf6eba40`、`NRestarts=0` 不變，帳本 dev／inode／size／mtime 不變。公網強制 IPv4 `/healthz`、公開當沖歷史訊號、公開隔日沖訊號均 HTTP 200；**沒有**聲稱實際持倉今天有新的完成成交，修正由故障注入測試證明。

子面板重新啟動後的第一次**內部** 2/25～9/24 訊號查詢，在 curl 15 秒期限內未收到 body；伺服器日誌稍後記 HTTP 200，但**客戶端已逾時，不能當成功回應**。相同查詢後續約 **277 ms**，新 offset 約 **1,670 ms**，說明第一次冷啟動仍有尾延遲，不能用成功恢復的 `/healthz` 掩蓋。其逐 session 快取已有 146 檔，獨立 warm-index 函式另測約 **1.06 秒**；目前沒有足夠證據把 15 秒歸因於單一來源或剛修改的覆蓋快取。該 cgroup 在完整歷史請求後峰值約 **5.75 GiB**，子面板 `smaps_rollup` 約 4.87 GiB 為 `LazyFree`、可由核心回收，`memory.events` 的 high／OOM 仍為 0；不能把 RSS 直接稱為無法回收的洩漏，也不能沿用部署模板舊「峰值低於 1 GiB」註解。模板註解已更正，但 8G／16G 限制未改。真正冷啟動／多請求並發與 9:00 服務延遲仍須另行量測。

## 9/25 03:30～03:36 子面板冷查詢分段測速

為追查上筆客戶端逾時，先在與子面板相同的 repo 投影快取目錄中做**獨立進程**試驗：順序執行 index warm 約 **1,092 ms**、其後廣範圍訊號約 **5,174 ms**；另一個新進程同時跑兩者時，訊號約 **3,188 ms**。兩者完整輸出 SHA 均與既有基線相同。這些是不同時間點、受 OS cache 與同機排程影響的樣本，並**未證明**預熱競爭就是先前 15 秒逾時原因；該時段亦有 data-refresh snapshot 與 guardian 排程重疊，但相近時點不能當因果。

內部唯讀 8766 JSON 回應現在提供 `Server-Timing` 的 `prepare`（路由解析與來源建置）、`serialize`（JSON 編碼）與 `compress`（實際 gzip 壓縮）耗時；**新增的測速標頭**不包含原始例外或私有路徑。`/healthz` 同時提供 `session_index_warm_status` 與 `session_index_warm_elapsed_ms`，預熱失敗只列 `failed`、不公開預熱錯誤字串；這不代表既有其他錯誤回應已完成全面敏感資訊審計。相關 HTTP、索引、訊號、監督器與錯誤遮罩回歸 **162 passed**，Ruff、Python 編譯與 diff whitespace 通過。僅讓已確認同 cgroup 的唯讀子面板重新載入；引擎 MainPID `787958`／InvocationID 未變、`NRestarts=0`。

正式重啟後 `/healthz` 先觀察到 `warming`，再觀察到 `ready`／預熱 **1,241.056 ms**。第一次廣範圍訊號 HTTP 200，`prepare=2102.869 ms`、`serialize=13.740 ms`、curl **2.121 秒**；前一次正式重啟首次為 `prepare=2207.561 ms`、`serialize=11.980 ms`、curl **2.225 秒**。同頁後續一筆 `prepare=98.692 ms`／curl **114 ms**，新 offset `prepare=1552.998 ms`／curl **1.569 秒**；gzip 熱頁 `compress=7.515 ms`、約 90 KB。主要成本明確在來源建置而非 JSON 或壓縮，但 15 秒異常這兩次未重現，仍保留為未解尾延遲。未改帳本、行情、Discord 或交易引擎，也未把 `/healthz` 200 當作資料完整性證據。

## 9/25 訊號頁跨 offset 範圍統計共用

2/25～9/24 的訊號查詢涵蓋 **2,004,142** 列。範圍方向統計與開盤執行稽核只依賴帳本版本、選取日期、篩選條件及各模式資本，不依賴 `offset`／`limit`；之前每翻一頁都重做完整 Polars group-by。現以最多 16 筆的進程內快取共用這兩項小型聚合結果。鍵包含來源 dev／inode／size／mtime／ctime、正式歷史簽章、日期、模式、標的、狀態、掃描上限及資本；當頁列、持倉覆蓋與模型解釋維持逐請求取得。每次複製聚合稽核再加當前 state 的預期筆數，避免修改快取原件。帳本 append 與資本改變的失效均有回歸測試。

修改前同進程 offset 17／18：**2,568.3／1,829.4 ms**；修改後另一進程：**2,148.5／933.5 ms**，兩頁完整 JSON SHA-256 逐頁與修改前一致。另在單進程不同 offset 量測：強制每頁重算 **4,839.1／2,212.8／1,622.2 ms**，可共用聚合的後三頁 **989.4／987.9／931.1 ms**；非交錯實驗，受 OS page cache 和同機工作影響，不能推成 p95 或百分比承諾。首次來源建置仍約 2 秒以上，剩餘約 0.9 秒主要要再分離投影讀取、串接與 top-k 才能定位。相關當沖／公開 gateway **256 passed**，Ruff 和 diff whitespace 通過。此項只減少翻頁重算，不更改模擬成交、來源帳本、Discord 或資料健康狀態。

03:52 只重啟公開 gateway、終止並由既有監督器拉起唯讀 8766 子面板；交易引擎 MainPID `787958`／InvocationID `14def02ad64347fc97006a94bf6eba40`／`NRestarts=0` 不變，訊號帳本仍為 dev:inode `2096:38156421`、5,504,935,660 bytes。新版內部 HTTP 首頁 `prepare=2453.822 ms`、curl **2.470 秒**；相鄰頁 `prepare=1123.602 ms`、curl **1.142 秒**。公網強制 IPv4 `/healthz` 200；完整訊號頁先前公網首次 **2.540 秒**、相鄰頁 **1.220 秒**，皆 HTTP 200。公開 gateway 與內部面板各有獨立進程快取；首次公網查詢不會因內部面板曾查詢而預熱。前次偶發 15 秒冷啟動逾時本次未重現，未定根因；IPv6 與各產品資料健康不在此次修正範圍。

本輪唯讀覆蓋收據 `artifacts/benchmarks/service-coverage-20260925T0354-summary-cache.json` 仍列出 50 service／39 timer／2 path；六個產品 API 均可 HTTP 200，但 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、永豐 `degraded`、全資料 `critical`。當沖公開狀態對 9/24 仍列 `intraday_residual_open`：1 億模式 191 筆、Attention 模式 5 筆、Multi-Basis 及 Projection-L1-GELU 各 1 筆，合計 **198** 筆；這是成交／強制退出證據未閉合，不能因網頁翻頁加速就宣稱交易服務健壯或平倉完成。本輪沒有改寫這些帳本或製造成交。

## 9/25 04:00 官方當沖資格 watcher 的空輪詢成本

現場 03:01 啟動的資格 watcher 於 03:56 仍在等官方 9/25 宣告日，`waiting_source` 已記 **1,591 次**、2 秒輪詢；這不是取得新資格。TWSE 官方 [OpenAPI 的 TWTB4U 端點](https://openapi.twse.com.tw/) 此刻宣告 9/24；一次正式 `_http_get` 取回 **86,473 bytes／1,232 列**，獨立樣本網路約 **93.7 ms**、JSON＋日期掃描約 **3.0 ms**、完整表解析約 **19.4 ms**。回應有 ETag／Last-Modified；帶實際 ETag 與 cache-buster 的條件請求回 HTTP 304、無內容。因每次空輪詢都重抓與重解析完整舊表，55 分鐘 1,591 次約對應 **137.6 MB** 的重複表內容；這是依本次內容長度的估算，不是網卡實測流量。

程式現保持 2 秒偵測週期與原有共用來源 rate limiter，在既有 `_http_get` 增加可選的 If-None-Match／If-Modified-Since。watcher 只沿用本進程曾讀到的官方完整 body：304 不傳輸、不重解；新版 200 仍核對宣告日、完整解析並等待 TPEx 同日回應，然後才執行原有 canonical downloader／覆蓋稽核。過期日期只做日期預檢，不能觸發就緒；每 **60 秒**強制無條件重取，以防來源端驗證器更新異常。若沒有本地 body／驗證器卻收到 304，維持 `PublicationPending`；等待、成功及逾時收據加入 `full_bodies`、`not_modified`、`body_bytes`，可量測日後實際節省而非假定。正式端點連續兩次試探為 200／86,473 bytes／約 1,245 ms，接著 304／0 bytes／約 18.3 ms；此二樣本受同機共用節流和網路變動影響，不能宣稱 p95 延遲改善，也**沒有**減少請求次數或保證官方確切發布時刻。相關資格與下載器 165 個測試、Ruff 與 whitespace 檢查通過；另外新增一次 stale 來源的完整 CLI 控制流程回歸，確認 `publication_pending_timeout` 不會被寫成成功。

04:01 活躍 watcher 仍為 03:01 啟動的舊 Python 進程；為保留其 90 分鐘等待與終端收據，**未中途重啟它**。更新程式會在本輪正常成功／逾時後的下一次既有 systemd 啟動才生效；須以新收據 `twse_transport` 出現及實際 304 計數驗收，不能先宣稱現場已省流量。當沖引擎、Discord、行情與帳本均未因本項改動重啟或改寫。

## 9/25 04:05 跨服務記憶體壓力的真實增量

先前覆蓋表把 cgroup `MemoryPeak` 與現在的 `MemoryCurrent` 同列，卻沒有 `memory.events` 的時間差，容易將舊的 48 GiB 尖峰當成當下持續壓力。現在保留每個 installed StockAgent unit 的 `MemoryHigh`／`MemoryMax` 與 high／max／OOM／OOM-kill 累計事件，只有在同一個活躍 InvocationID、PID 與 NRestarts 不變且計數器未倒退時，才計算採樣期間增量；重啟、完成的一次性工作或不可讀 cgroup 都顯示 unknown，不填零。新增 36 個服務測速／盤點測試通過，Ruff 與 whitespace 檢查通過。

正式 10 秒收據 `artifacts/benchmarks/service-coverage-20260925T0405-memory-events.json`：仍覆蓋 50 service／39 timer／2 path。`tw-public-source-events` 當前約 **1.20 GiB**、歷史峰值約 **48.00 GiB**、memory.high 門檻 **48 GiB**、累計 high **106,709 次**，但這 10 秒 high／OOM／OOM-kill **增量皆 0**；公開 gateway 約 **3.04 GiB**、high **4 GiB**、這 10 秒 high 增量 0。這只排除短樣本期間的 cgroup 壓力，不證明日後高負載無風險，也不表示 48 GiB 尖峰來源已根治；下一次重建需用同欄位長期樣本定位，不能直接降低上限或停掉來源驗收。

## 9/25 04:20 註冊下載服務的逐步驟瓶頸

原覆蓋表對四個 registered-data 工作只附「與 systemd 啟動相符的整輪 TSV」；18 小時回補的單一 wall 值不能指出哪個供應商工作耗時。現在只在整輪 `run_id` 已由開始／結束時點驗證後，讀取該**完全相同 run_id** 的 `step_receipts` 目錄；最多 128 檔、每檔最多 16 KiB，拒絕 symlink、錯 run_id、錯 step/schema、非有限或負耗時，且只輸出步驟名稱、狀態、耗時與退出碼，不公開命令或日誌路徑。`latest` 步驟捷徑不作為證據；並行步驟耗時不能相加成整輪 wall。相鄰 36 個稽核／測速測試、Ruff 與 diff whitespace 通過。

新唯讀收據 `artifacts/benchmarks/service-coverage-20260925T0420-step-timings.json` 仍覆蓋 50／39／2。最新 backfill 的 systemd wall **65,724 秒**，業務收據為 `completed_with_failures`；同輪 **OKX 1m 65,726 秒 complete**、**Binance 1m 30,213 秒 failed**、Bybit 1m 19,115 秒 complete。每日工作 1,312 秒中 Yahoo US daily 1,295 秒；features 1,953 秒中 Binance 1m 1,953 秒、OKX 1m 1,627 秒。這只確立下個量測／修復優先順序，不表示可以任意加併發或跳過來源完整性；Binance 失敗仍是失敗，服務 exit 0 不能覆蓋業務收據。

回補失敗屬 **9/20 同輪歷史收據**，不能套到較新的來源結果。9/24 features 的 `data_binance/1m/download_summary.json` 已另記 574／574 `updated`、0 failed、約 1,951 秒；OKX 同輪 491／491 `updated`、約 1,625 秒。這說明較新工作成功，不會回寫 9/20 回補那筆 `completed_with_failures`，也不代表全歷史缺口已審計完整。優化優先順序改為現行 features 兩個 27～33 分鐘階段的細分耗時，並獨立稽核舊 backfill 的未完成範圍。

9/24 同輪來源自身已有逐 feature 階段與 limiter 收據：OKX `history-index-candles` **7,359 grants × 0.2 秒 = 約 1,472 秒**的串列下界，該更新整體約 1,625 秒；Binance `futures_data` **5,947 grants × 0.3 秒 = 約 1,784 秒**，整體約 1,951 秒。這是以本地節流設定與實際 grants 推出的下界，不是逐請求真實服務時間相加；兩個工作尚有網路、解析、寫入與其他端點。[OKX 官方文件](https://app.okx.com/docs-v5/zh)目前對該端點列 **10 requests／2 秒、每 IP**，[Binance 官方文件](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)對對應統計端點列 **1,000 requests／5 分鐘、每 IP**，與本地節流相符。此時單純提高 worker 數或解除 limiter 不會合法地消除主要下界；可行方向是證明哪些請求為重複／超過來源粒度需求，再以同來源行數、版本及缺口稽核驗證減量，不能刪資料或把來源不完整標綠。

## 9/25 04:25 註冊資料工作快速重試的收據隔離

四個 `registered-data` 範圍原以 UTC **秒**產生 run ID。systemd 快速重試或人工重啟若落在同一秒，新的 `step_receipts/<run_id>` 與前次同名；尤其 `latest` 之外的「同一 run」證據會被覆寫，導致效能與失敗診斷混合。新工作改用 UTC 奈秒時間戳作 run ID；稽核器同時支援舊秒格式與新奈秒格式，只讀完整匹配的目錄。這不會重跑來源、改寫資料或改動交易引擎；既有工作直到下次正常啟動才會採用新 ID。測試覆蓋同一秒內兩次不同 ID、各自獨立的步驟收據、舊版兼容及非法日期，相關稽核／測速 **44 passed**，`bash -n`、Ruff 與 whitespace 檢查通過。

## 9/25 04:32～04:33 資格 watcher 自然換版驗收

03:01 啟動的舊 watcher 跑完原定 90 分鐘，於 04:31:32 寫下獨立的 `publication_pending_timeout` 收據（2,572 次輪詢，TWSE 仍宣告 9/24），沒有中途強制重啟或刪除記錄。systemd 於 04:32:02 自動啟動新版 PID `1122362`。新版 04:33:03 的 `waiting_source` 收據有 31 次輪詢、`full_bodies=2`、`not_modified=29`、`body_bytes=172946`；兩次完整回應包含 60 秒無條件重驗，其他 29 次 HTTP 304 沒有回應主體。相對每次都下載 86,473-byte 舊表，這個窗口少接收約 **2.51 MB 的回應主體**；不含 HTTP 標頭、TLS 或其他網路流量，亦不是長期 p95。官方宣告仍落後最低可接受日 9/25，**尚未取得 9/25 資格**，等待狀態正確。`systemctl --failed` 空；交易引擎仍為 PID `787958`、相同 InvocationID、`NRestarts=0`。這只驗收來源等待效率與監督器自動重試，沒有驗證今日開盤成交或修復資料健康。

## 9/25 04:30～04:35 磁碟壓力安全邊界

唯讀覆蓋收據 `artifacts/benchmarks/service-coverage-20260925T0430-resilience.json` 涵蓋 50 service／39 timer／2 path，10.15 秒內沒有觀察到任何 memory.high／OOM 增量；這不能證明長期無壓力。`stockagent-binance-public-archive.service` 最近一次 exit 75；根檔案系統當下約 **94.86% 已用**、可用約 **104 GiB**，不足既定 10% 安全保留。既有編譯快取維護工具只做 dry-run，判定 `under_pressure=true`，但 `eligible_files=0`、可選已分配位元組 0，沒有可安全清除的舊編譯快取。已批准的 packed rolling-retention 最近一輪只證明／回收約 **532 MB**，遠不足解決約 2 TB 檔案系統的 10% 門檻；沒有放寬保護、刪除來源／帳本或擴大清理範圍。後續需按來源 catalog、D 備份與 Syncthing／程序參照驗證具體大檔，再決定擴容或另行批准保留政策；`systemctl --failed` 空不代表這個業務工作成功。

## 9/25 04:40 TAIFEX 輔助資料的空轉冷卻

前一輪 `stockagent-taifex-auxiliary-daily.service` 約 **750.2 秒**；journal 精確分段顯示主要為選擇權正規化 **740.978 秒**，其中官方收據階段 **49.647 秒／34 份**，其後月 ATM **184.951 秒**、月 full-chain **228.220 秒**、週 ATM **106.929 秒**、週 full-chain **170.658 秒**。這四個衍生層共約 690.8 秒，是後續 per-year immutable 投影或單次解析研究的主目標；不得以移除歷史資料或略過來源雜湊／品質驗證縮短。

收據迴圈原來無論檔案是否已在本地，每份驗證後都 `sleep(request_interval=1s)`。共享下載函式現在可選「只有真正完成 HTTP 附件下載後才冷卻」；選擇權任務採用此選項，其他使用者維持原預設，現有檔案仍逐份進行原本的收據格式驗證。以現有 manifest 所列 34 份且全部已落地的附件做唯讀快取命中＋格式驗證，約 **0.011 秒**、零網路請求；上次正式輪次 34 份中約 33 份本地重用，因此理論上移除約 **33 秒**純睡眠，而真實來源請求仍保留 1 秒冷卻。相鄰 TAIFEX **18 passed**、Ruff、whitespace 通過。尚未執行完整來源／衍生層重建，也未宣稱 750 秒端到端已實測下降；必須比較下一次自然排程的相同來源範圍與階段日誌。

唯讀單檔剖析補充：2025 年官方 ZIP **18,743,862 bytes**，目前 `_read_txo_rows` 月序列 **6.846 秒／223,298 列**、週序列 **6.233 秒／108,238 列**。正式建置分別對 ATM 和 full-chain 重複讀取相同來源，故此年單是四次解析就可能約 26 秒；這是單檔、相鄰但非交錯樣本，不能外推為 34 份來源的總節省。下一階段若要處理約 690 秒的主瓶頸，應以雜湊／解析版本綁定的逐來源投影 shard 或單次解析多消費者架構，先建立完整輸出 SHA／列數等價及記憶體峰值門檻，再決定是否上線；不能用無界進程快取把 34 年選擇權鏈留在 RAM。

## 9/25 04:50 Yahoo 長工作「程序成功≠來源完整」收據

9/24 的 Yahoo US daily：正式步驟 exit 0、約 **1,295 秒**，但同輪 `daily_update_summary.us_stocks.json` 記 `failed=12`、`lagging_skip=615`；前者是實際失敗，後者依既定延遲政策未嘗試修補，兩者是 **627 個仍有缺口的標的**。`stale=12,208` 是修補前狀態且同輪另有 `repaired=12,208`，不得再把它加到未解缺口。`not_found_skip`／`delisted_skip` 另保留原狀態，不能任意改稱完整或改成失敗。現行宿主共用 Yahoo 節流預設每秒 10 請求，12,237 個本輪標的即使每檔只需一個請求，也有約 **1,224 秒**的最簡下界；1,294 秒的觀察值已接近它，單加 worker 不會突破這項客戶端節流與供應商風險界線。

現在排程把 run ID 傳入 Yahoo；每個資產的來源摘要保留該 ID。步驟完成或失敗時，只在來源摘要的 run ID、資產、模式、產生時點與有界結構全部相符時，將安全的狀態計數、來源耗時與 `failed`／`still_stale`／`lagging_skip` 缺口數複製進**同 run** 步驟收據；不複製 URL、命令、錯誤原文或憑證。覆蓋表仍分列程序 `state=complete` 與 `source_data_health=reported_gaps`，若摘要缺失或不符則列 `unverified`，不把零退出碼改寫成來源完整。既有 9/24 來源摘要沒有新 run ID，因此**不能倒填為精確關聯**；需等下一次自然 daily 排程驗收。相鄰 Yahoo／收據／服務覆蓋 **93 passed**，Ruff、`bash -n` 與 whitespace 通過；未重跑 12,000 檔來源或改變 Yahoo 節流、資料與交易狀態。

同一份 04:30 全服務覆蓋表的操作測速等級仍是 **35 個只有上次程序 wall、14 個常駐程序只有資源樣本、1 個退役 hot-sync 無現行測量**。因此「50 個 service 已盤點」不等於「50 個功能均有逐步驟延遲」，完整目標尚未達成；後續須逐一補可關聯到同一次執行的業務收據及長期分位數，而不是把 CPU／記憶體或 HTTP 200 當作功能反應時間。

修改後另執行唯讀整體收據 `artifacts/benchmarks/service-coverage-20260925T0500-yahoo-source-contract.json`，仍成功覆蓋 50／39／2。9/24 的舊 Yahoo daily run 與 systemd 啟動相符、16 個步驟可讀，但 `source_data_health` 留空；這是舊摘要沒有 run ID 的正確 fail-closed 行為，不會用當前可變摘要冒充舊同輪收據。

## 9/25 05:03 OpenBB L1 未指派來源保序索引

最近一次正式 L1 compaction 約 **86.98 秒**，其中來源契約／衍生檔稽核約 **43.30 秒**、未指派來源載入約 **29.14 秒**、實際新段建置約 **2.93 秒**。正式 16 GiB SQLite manifest 原先沒有 `idx_l1_tasks_compaction_order`；`EXPLAIN QUERY PLAN` 證實未指派來源查詢使用 `idx_tasks_active_plan` 後仍建立 `TEMP B-TREE FOR LAST TERM OF ORDER BY`。不應削減 L0／L1 來源、行數、schema 稽核來掩蓋排序成本。

將已由 L1 消費端支援的部分覆蓋索引 `(plan_token, endpoint, task_id, rows) WHERE active=1 AND status='success'` 納入下載器的 manifest 初始化契約。現場 provider scheduler 為 `waiting` 且 active／buffered／completed-pending 全部為 0；持有 L1 排他鎖、使用低 CPU／I/O 優先權、檢查可用空間及來源是否繼續等待後，為現有 manifest 原子建立索引，耗時 **54.566 秒**。建索引只改 SQLite 查詢結構，未改 L0 Parquet、L1 段、任務狀態、查詢 view 或交易帳本。

同一份正式資料庫唯讀比對 `LIMIT 2048`：新保序索引 **2.303 秒**、舊 `idx_tasks_active_plan` **29.486 秒**，兩者 2,048 列的 SHA-256 均為 `49efbb74ab7d4f132d602089f88da216779127901f4ba4b4fa39ebcea3aa12aa`；新查詢計畫無暫存排序樹。每 endpoint 的成功任務 COUNT／SUM，新 **0.901 秒**、舊 **3.689 秒**，48 列 SHA-256 同為 `cb7ce27628cd51995a1aa8c5a8eab41001dfc9215566b89a2f8b5dc1464ec483`。這是不同順序的單組熱度樣本，不能宣稱 p95 或整輪提升；新索引亦增加磁碟與未來任務寫入成本。OpenBB L1 全檔 **15 passed**、OpenBB archive downloader 全檔 **277 passed**，Ruff 與 diff whitespace 通過。下一次自然 timer 必須核對 `unassigned_source_index_order`、整輪 wall、來源稽核、pending 數、磁碟／WAL 成長與 archive 寫入延遲；不能因查詢變快就宣稱服務或所有資料已完整。

建索引後 SQLite WAL 仍保留約 **990,971,272 bytes**。在 provider 仍等待、L1 未運行且持有同一排他鎖時，用 SQLite 自身 `wal_checkpoint(TRUNCATE)` 回傳 `(busy=0, log=0, checkpointed=0)`，WAL 從該大小變為 0，檔案系統可用位元組由 **109,104,082,944** 增至 **110,095,040,512**；未直接刪除 WAL。OpenBB archive PID 仍為 5880、`NRestarts=0`，但剩餘容量仍低於既定 10% 保留，Binance archive 的 exit 75 問題未因此解決。

新的唯讀覆蓋收據 `artifacts/benchmarks/service-coverage-20260925T0510-openbb-index.json` 仍列 50 service／39 timer／2 path。六個產品端點均可 HTTP 200，但實際資料健康分別為 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、Shioaji `waiting`、OpenBB `active`、全資料 `critical`。這些狀態不因 L1 查詢最佳化自動改善；註冊資料 backfill 上次 exit 1 與 Binance archive exit 75 仍需各自處理。OpenBB archive 仍在供應商 cooldown，沒有因此重啟；下一次自然 L1 timer 才能驗證整輪效果。

05:09:32 依原 systemd unit 的開盤保護、CPU／I/O 權重及記憶體限制啟動一輪正式 L1 驗收，05:10:35 正常結束：**63.175 秒 wall／70.305 CPU 秒**，相鄰前輪 **86.983 秒 wall**；兩輪皆新增 **19 段／2,048 來源 shard**、0 stale、0 failed、0 deferred failure、0 L0 delete。`unassigned_source_load` **29.141 → 3.138 秒**，其中新 `unassigned_source_index_order` **1.893 秒**、metadata **1.245 秒**；同輪 `status_task_count` **3.783 → 1.004 秒**。來源契約稽核 **43.303 → 46.659 秒**，建段 **2.929 → 3.020 秒**，view **4.813 → 6.091 秒**。整輪改善方向與索引機制一致，但相鄰輪的 cache／I/O 負載不同，不能把 23.808 秒全部歸因於索引或推成 p95。cgroup 峰值本輪約 **1.8 GiB**、各分段取樣 swap／memory.high 事件皆為 0；剩餘 **3,922,679** 個 L0 shard 待壓縮，並有 1 個原本即 deferred 的 query view。原定自然 timer 的下一輪與 archive 重新寫入後仍須複測，尤其是新索引的長期寫入成本。

本機公開閘道 `/openbb/api/status` 隨後讀到同一筆 05:10:35 L1 收據：`pending_files=3,922,679`、`new_segments=19`、`active_segments=152,741`；OpenBB archive 主程序維持 PID 5880／`NRestarts=0`。這驗證了「服務完成 → 收據 → 本機 API」串接，不代表外部 IPv6 可連或所有 OpenBB 來源已獲取完畢。

## 9/25 05:17～05:34 TAIFEX 期權解析與輔助排程恢復

官方期權 CSV 原本對每個契約列建立 `DictReader` 字典；月／週 ATM 與完整鏈各讀一次同一來源。改為每個來源檔只建立一次標頭欄位索引，逐列仍保留重複標頭取最後欄、短列缺值、額外欄忽略及原本缺值／衝突判定。單一 2025 年官方 ZIP（18,743,862 bytes）月序列解析 **7.016 → 4.396 秒**、週序列 **6.192 → 3.940 秒**；兩邊列數（223,298／108,238）與各自結果摘要完全相同。2001 年 ZIP 及 2026-09-01～09-22 CSV 的月／週也與 `HEAD` 原解析器逐列摘要相同。這是解析局部測速，不等於四個衍生建置整輪的純收益。

05:17 用原 systemd unit 正式執行後，34 份來源收據通過，完整期權衍生建置 **252.630 秒**：來源收據 8.066、月 ATM 60.139、月完整鏈 80.111、週 ATM 49.366、週完整鏈 53.655 秒；相鄰較早正式輪次為 740.978 秒，但同時包含先前的空轉冷卻、日期解析快取及不同 OS cache／負載，**不可將差值全歸於本次 CSV 改動**。本輪月資料 6,083／6,096 可執行、週資料 2,920／3,382 可執行；不可把不足列解釋成解析器效能問題。原服務接下來因逐筆頁面只有 29 個共同日期而退出 1，且 `set -e` 令最後結算階段被跳過。

05:24 的兩個官方逐筆頁面各列 30 條連結，但包含本機當時仍屬未來的 **2026-09-29**；排除當日尚未完成及未來日期後，只有 8/17～9/24 的 **29 個共同已完成交易日**。新流程不把 9/29 或未完成日期算入「最近 30 日」。如果這 29 日**恰好**等於既有完整 30 日 manifest 的後 29 日，先逐一驗證既有 60 個原始 ZIP 的檔名／內容雜湊／成員及 60 個正規化 partition 的收據／內容雜湊，再保留該 manifest 和原有 8/14～9/24 視窗，寫 `state/recent_listing_status.json` 的 `waiting_next_publication`、頁面摘要與被排除日期；任何來源或收據驗證失敗則 `blocked_incomplete_listing` 並非零退出。這是在等待下一筆**可驗證的官方檔**，不是把 29 個新日期宣稱已更新成新的 30 日。

原 wrapper 現將期權、逐筆、最後結算當成獨立階段記錄退出碼與耗時；任一階段失敗仍讓其他階段嘗試，總退出碼維持非零。故下一次逐筆來源不足不會再次無故跳過結算。05:33 原 unit 重新執行 **2.779 秒 wall／1.615 秒 CPU／97.1 MiB 峰值**，三階段各退出 0：期權直接沿用相同來源且雜湊驗證過的產物（0.343 秒）、逐筆 `waiting_next_publication` 保留 SHA-256 為 `58e41c2a1b4af3126ef17fba406f14cb39f319468532e4eae5a26b43ed060ff5` 的完整既有 manifest、最終結算產出 777 筆官方 TXO 結算（2012-11-21～2026-09-23）。`systemctl` 最終為 `Result=success`；這只證明本輪三階段成功或明確等待，**不代表今天的新逐筆 30 日視窗已產生**。相關期權／逐筆／期貨整合 **64 passed**，另外 20 個逐筆鄰近測試、Ruff、`bash -n` 與 whitespace 檢查通過。自然排程在下一個新官方逐筆日期出現後仍要驗證：新完整 30 日 manifest、報表／partition 行數、接續狀態及服務 wall。

05:35 本機全資料監控仍將「TAIFEX 台指期權逐筆成交」群組列為 `blocked`，不是因新 wrapper 的 exit 0 就誤判為完整：tick 子端點為 `complete`／data-through 9/24，但已註冊的選擇權 1m materializer 尚未接入可執行管線，必要子端點為 `unable`。這是**不同的既有缺口**；本輪未製造 1m 資料，也未掩蓋它。

逐筆收據另在真正寫入新完整 manifest 後才從 `listing_ready` 改記 `complete`，並附新 manifest 雜湊；現場這輪仍是 `waiting_next_publication`，不能因原 systemd unit 退出 0 或完整的**舊** 30 日 manifest 而推論新日期已完成。追加修改後的逐筆／選擇權相鄰測試 **17 passed**。

## 9/25 05:47～05:51 全資料監控的實際週期成本

30 秒 supervised snapshot 的現場 `data_monitor_timing` 約 **2.3～4.3 秒／輪**，大輪會重建 **87,606 個欄位**；這是背景生產成本，不能用公開 API 的 9 ms 快取命中替代。對現有 `record_inventory_cache.json`（約 36 MB）、**56,988 個實體 Parquet** 做唯讀剖析，檔案發現約 163 ms、JSON 解碼約 548 ms、重讀各組聚合約 770 ms；cProfile 增加追蹤成本，因此只作歸因。自然 timer 採用新增的細分計時後，05:47:39 一輪實測總 **4,212 ms**：inventory **1,167 ms**（cache 解碼 478、發現 424、簽章掃描 262）；feature 階段 **1,964 ms**，其中來源欄位驗證與聚合 **968 ms**，剩餘時間含公開欄位投影、JSON 寫入與其雜湊收據。這些時間是單輪，不是 p95。

現在每輪 `data_monitor_timing` 都附 `inventory_stages_ms`；重建欄位時再附 `feature_inventory_stages_ms`。它們只測既有工作步驟，不改來源核對或公開輸出契約；相鄰 **77 個資料監控測試通過**。公開欄位投影另將同一 dataset 的來源標題、供應商及市場分類解析共用：回歸測試以同資料集 1,000 欄位證明分類從逐欄重算降為一次，仍保持逐欄輸出內容。現場整輪受其他負載和來源 JSON 寫入影響，尚不能宣稱這項小改動帶來可量測的 p95 改善。測試中的 MsgPack 對 36 MB 快取僅比 JSON 解碼少約 **0.1 秒**，因此沒有引入第二套持久快取格式。服務趨勢採樣失敗的日誌只輸出例外類型，不再原樣輸出可能含私有路徑的錯誤字串。

自然 supervised run 於 **05:49:43** 以新版欄位投影重建 snapshot，此後兩輪重用收據均顯示 `features=87,606`、`refreshed_files=0`、無 traceback；本機唯讀 `/data-monitor/api/features` 回 HTTP 200，gzip 約 **1,401,993 bytes／4.6 ms**。這只證明本機 API 快取命中與功能可讀，不是瀏覽器解析、公開 WAN 或長期尾延遲驗收。獨立的 1m materializer、Binance archive 磁碟保留及全庫資料健康仍未解，整體目標維持未完成。

補跑公開 gateway、資料監控、實存清冊與長期服務趨勢整合回歸 **181 passed／19.55 秒**，Ruff 與 diff whitespace 通過；此覆蓋仍不是全庫測試。

## 9/25 06:00～06:08 Windows／WSL 啟動鏈的非零任務結果

新唯讀覆蓋收據 `artifacts/benchmarks/service-coverage-20260925T0600-current.json` 仍列 **50 service／39 timer／2 path**；`systemctl --failed` 為空，但這只說明目前沒有 systemd 失敗單元。Windows `StockAgent Public Caddy` 排程為 `Running`、上次任務結果 `0x800710E0`，同時 Caddy 有兩個程序，Windows 對本機全球 IPv6 位址的 443 TCP 測試成功，WSL 內部 8770 `/healthz` 為 200，公開 IPv4 HTTPS `/healthz` 也為 200。**不能把非零 LastTaskResult 直接當成 Caddy 已停，也不能用網站現在可讀證明重開機自動恢復。**

查到該任務使用 `MultipleInstances=IgnoreNew`、有每分鐘重複觸發及 boot／logon 觸發；[Microsoft 的 Task Scheduler 文件](https://learn.microsoft.com/en-us/windows/win32/taskschd/taskschedulerschema-multipleinstancespolicy-settingstype-element)定義 `IgnoreNew` 在既有執行實例仍在跑時不啟動新實例。這使該非零碼與被拒絕的重複觸發**相容**，但 Task Scheduler Operational history 目前為 disabled，尚無同一事件的逐條證據，不能斷言此碼必定只由 IgnoreNew 造成。唯讀稽核現記錄 `multiple_instances_policy`、`repetition_intervals`，並把此組合標成 `ambiguous_nonzero_with_ignore_new_repetition`，保留非零原始結果，不把它改綠。現場新版稽核輸出符合預期；相鄰稽核測試 **21 passed**。

WSL 最慢的已記錄恢復事件為 **274.03 秒**：首次 gateway dispatch 到當前 VM 網路驅動載入約 **259.858 秒**、到 WSL userspace 約 **260.353 秒**，userspace 到 HTTP 健康約 **13.143 秒**；其間舊版啟動日誌記 18 次 WSL dispatch。這是 **9/24 的 WSL 恢復**，不是已重現的 Windows 全機冷開機，也不能據此認定重複 dispatch 是 260 秒延遲的原因。新版 launcher 會記錄每個 `wsl.exe` dispatch 完成的退出碼與命令耗時；唯讀稽核現在另保留最近完成紀錄及七日完成數，不把 `wsl.exe` 退出 0 當成 VM 或 gateway 已就緒。現有最近紀錄為命令退出 0／0.186 秒，而後端健康仍須分開驗證。

這台 WSL 的全球 IPv6 `/128` 恰與 DDNS AAAA 相同，但 WSL namespace 沒有 443 listener；因此從**這個 WSL** 對域名的 `curl -6` 立即連線失敗，而 Windows Caddy 對 `::`:443 監聽、Windows 本機 IPv6 TCP 測試成功。前者是同址本地測試，**不足以判定外部 IPv6 WAN 路徑**；仍須獨立外網 IPv6 節點檢驗路由器、Windows 防火牆及 Caddy。沒有調整 DDNS、路由器或防火牆，也沒有宣稱雙棧已完成。

啟動鏈／唯讀服務趨勢整合 **35 passed**，Ruff 與 diff whitespace 通過。Windows 任務 Operational history 未啟用、外網 IPv6 未獨立驗證、未做冷重啟；這三項維持明確未驗收。

06:09 本機全資料監控 `/data-monitor/api/summary` 仍是 `critical`：327 個必要資料端點中 **65 unable、58 catching_up、204 complete**。故網站與 Caddy 可連不等於所有資料服務健康；此輪未更改任何資料健康狀態或交易帳本。

## 9/25 06:23～06:30 公開特徵全量建置的合併成本與故障邊界

正式 9/24 特徵摘要記錄 9,606,617 列、143 個特徵；`stock_merge` 約 42.9 秒。19:00 與 19:30 兩次任務之間的官方來源收據確實變動，下一輪 dry-run 相對已建版本也有變動來源；**不可**只因同一交易日就略過重建。來源任意舊列可能修訂，現有完整來源雜湊、產物原子替換及建後重驗仍保留。

原實作對已去重的 16 個股票特徵表逐一做 full join，每次都重新建構、合併逐漸變寬的鍵集合。現對首表至少 10 萬列且至少 3 個有效表的情況，先取一次所有 `(date,symbol)` 鍵聯集，再做 left join；小型／兩表路徑保留原 full join。來源表仍逐一用原規則正規化與去重；第一個同名欄位優先、任何後續來源獨有的鍵都保留，最後的正式輸出仍按鍵排序。`stock_join_keys` 與每段 join 秒數進入既有階段收據。

以**同一份真實來源**、相同 `2026-09-24` 截止及相同當前 **2,757** 檔股票 universe，在獨立暫存目標依序建置原合併器與新合併器：兩者均為 **9,606,617 列**，Parquet 內容 SHA-256 均為 `40a9d2d98c16874dcc906539ab8273f11675e8877ece038fb1c9ab985b2f5d23`。原版 `stock_merge=45.898s`、整個 canonical 特徵建置 `146.298s`；新版為 **24.464s、112.446s**，分別少約 **46.7%** 與 **23.1%**。兩份 receipt 的 `name` 不同，故完整 receipt 物件不相等；檔案內容雜湊才是此處的位元組等值證據。暫存輸出在測試結束後移除，正式特徵檔與交易帳本未動。這是相鄰兩輪的觀察值，不是 p95、來源抓取、研究衍生層或整個 19:30 service wall 的承諾；後續仍須比較自然排程的記憶體峰值與完整收據。

本次先試的「少數重複鍵只局部聚合」在真實 margin 混合表**不適用**：盤後特徵與同日規則證據本來大量共鍵，該捷徑較慢，已撤回；沒有把退化實驗留在正式路徑。新增測試覆蓋大量鍵、後續來源獨有鍵、同名欄位優先及少量重複鍵的末筆非空值。公開特徵／reconcile 相鄰回歸 **94 passed**，PyCompile、Ruff、`git diff --check` 通過。

另一個獨立程序、相同來源的新版全量建置仍產出同一 SHA-256，但受同時其他工作負載影響，`stock_merge=37.058s`、整輪 `154.748s`，不能把前次 112.446 秒當穩定延遲或 SLA。該程序的 `ru_maxrss` 峰值為 **44.623 GiB**，低於目前服務 `MemoryHigh=48G`、`MemoryMax=64G`；這只是程序 RSS，不包含 systemd cgroup 的檔案快取與其他計費，也未量到自然排程的峰值。正式排程接續要追 `MemoryPeak`、memory.high 事件及完整 service wall；若長期尾延遲或記憶體壓力退化，需回查鍵聯集大小與回退門檻。

06:30 唯讀覆蓋收據 `artifacts/benchmarks/service-coverage-20260925T0630-resilience.json` 仍涵蓋 **50 service／39 timer／2 path**，5.18 秒樣本沒有已觀察到的 memory.high／OOM 增量，`systemctl --failed` 為 0。六個本機產品 GET 都 HTTP 200，但實際健康仍為 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、Shioaji `waiting`、OpenBB `active`、全資料 `critical`；Binance 公開 archive 最近 exit 75 的磁碟保留閘門也未解除。這項計算優化不會自動修復來源缺口、盤中成交證據、Windows 真冷啟動或 IPv6 外網路徑；整體健壯性目標仍未完成。

06:37 嘗試只啟用 Windows `Microsoft-Windows-TaskScheduler/Operational` 的既有 10 MiB 循環事件日誌，以便區分 Caddy 任務的 `0x800710E0` 是否真為 IgnoreNew 重複觸發；目前 WSL 呼叫的 Windows 使用者沒有提升權限，`wevtutil` 回 `Access is denied`。重讀設定仍為 `enabled: false`，**沒有啟用**、沒有重啟或修改 Caddy／WSL；需要 Windows 管理員另行啟用並用同一任務事件驗證，不能在缺證據下將非零結果改稱正常。

## 9/25 06:36～06:45 註冊 daily 真實失敗與新掛牌尾端修復

06:30 的 `stockagent-registered-data-daily.service` 於 06:36:09 以 exit 1 結束，正式同輪 `registered_daily_runs.tsv` 明記 `completed_with_failures`、`yahoo,okx_perpetuals`；`systemctl --failed` 因此由 0 變成 1，不能沿用較早的健康快照。Yahoo 群組在開始一秒內因腳本 `set -u` 引用未定義的 `repo_root` 而退出，與供應商資料速度無關。已改用既有 `ROOT_DIR`，並加入只載入腳本、覆寫 `run_step` 的無網路回歸測試，證明 Yahoo 摘要路徑在 nounset 下可解析；沒有重跑數萬筆 Yahoo 來源，因此**本次 Yahoo 資料未宣稱補齊**、原失敗收據保留。

OKX 同輪 492 個合約中只有新掛牌 `KII-USDT-SWAP` 失敗，訊息為 `No rows in requested date range.`。官方即時 instrument API 的上市時間為 **2026-09-24 11:00 UTC**，history-candles API 當場有已完成 1m K；問題在 `tail_only` 對**尚未到來的要求日期 UTC 23:59**倒推 24 小時，台灣清晨執行時起點落在目前已收盤 K 線之後。將新標的尾端起點改為「`min(要求終點, 最新已收盤 K 時間)` 減 24 小時」，並讓 Bybit 的分頁終點也不超過已收盤時間；OKX、Bybit、Binance 同型路徑與新掛牌回歸各自覆蓋。既有標的的增量起點及來源缺失的 fail-closed 狀態未改。

用 OKX 正式來源先在獨立暫存目標驗證 `KII-USDT-SWAP` 取得 **703 列**，再用原 OKX 下載器、原排他資料鎖與官方節流做正式尾端重驗：**492/492 updated、0 failed、59.506 秒**；新掛牌正式檔有 **704 列**（兩次觀察間新增一分鐘）。這個成功是新的一輪來源收據，不會把 06:30 的失敗歷史改寫成成功。相鄰 crypto／來源收據回歸 **88 passed**；Yahoo 尚待不干擾開盤的完整更新或下次自然排程驗收，`stockagent-registered-data-daily.service` 的舊失敗狀態仍可見，未執行 `reset-failed`。

合併本輪公開特徵、三家加密下載、資料收據的相鄰回歸 **182 passed**，PyCompile、Ruff、`bash -n` 與 `git diff --check` 通過。06:47 新唯讀覆蓋收據 `artifacts/benchmarks/service-coverage-20260925T0647-after-repair.json` 仍列 50 service／39 timer／2 path；`systemctl --failed` 的唯一項仍是上述 06:30 daily 執行，六個產品本機 GET 仍 HTTP 200，但其業務健康仍是 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、Shioaji `waiting`、OpenBB `active`、全資料 `critical`。未把 OKX 單來源修好等同整輪 daily 或所有產品恢復。

## 9/25 06:46～06:52 OpenBB L1 自然排程的分段驗收

本輪自然執行成功，wall **340.660 秒**、CPU **129.178 秒**、20 個新段、0 stale／failed、L0 未刪除、仍有 **3,916,535** 個來源檔待壓縮；`economy.fred_series` 因 103,223 種 schema 超過 4,096 的已知邊界而繼續延後查詢 view，不能把 42 個 view 已發布解讀為全來源可查。正式收據 `data_openBB/_state/l1_compaction_latest.json` 的 `stale_source_contract_query=250.491s`、`stale_derivative_metadata_scan=49.649s`、`unassigned_source_index_order=5.470s`、`unassigned_source_load=7.143s`、`segment_build=3.470s`、`query_view_publish=5.510s`、`status_member_count=20.990s`。新保序索引已在正式輪被採用，但與先前輪次的來源量和資源爭用不同，不能直接把整輪時差歸因於它。

該 cgroup `MemoryPeak=2,686,447,616 bytes`，收據末段有 `memory.high=24,588` 累計事件、約 5.30 秒累計 memory full stall，最終 swap 約 12.9 MB；當時主機尚有約 87 GiB 可用記憶體，但大量 high 事件本身**不足以**證明 250 秒來源契約查詢由記憶體限制造成。現場 `EXPLAIN QUERY PLAN` 顯示這條查詢按段主鍵掃描成功段、按 `segment_id` 索引找 member、再按 task 主鍵逐筆查來源，最後對失效結果用暫存 B-tree 去重；它仍逐一驗證來源 task 的有效性、路徑、行數與更新時間。後續應在非開盤時段針對正式 manifest 做唯讀、相同資料與熱度的索引／查詢候選測速，驗證失效集合與原因等價及寫入、磁碟代價後才改；本輪**沒有**因為慢而略過來源或衍生檔完整性稽核，也沒有盲目調高記憶體上限。

## 9/25 07:00～07:15 公開來源鎖與休市資格的跨服務修復

07:00 `stockagent-tw-public-release-archives.service` 的開盤保護檢查通過，但原本第二道 `flock -n` 單次預檢恰與正在重試的資格 watcher 碰鎖，unit 以 `exec-condition` **跳過整輪**；這既不是成功建置，也不會由 `systemctl --failed` 提醒。鎖持有者是自 06:02 啟動的 `stockagent-tw-day-trade-eligibility.service`，原本反覆呼叫 canonical 下載器。現在來源歸檔在 Python 生產者內有界等待同一把鎖最多 120 秒、取得後重新檢查距 08:20 至少 45 分鐘的安全緩衝；仍以同一全域鎖保護來源／衍生層，逾時明確失敗。已部署 unit 模板，未重啟當沖引擎或 Discord。

07:04:48 有監控補跑，實測來源鎖等待 **2.010 秒**，總 wall **369.982 秒**、CPU 約 **1,980.136 秒**、cgroup `MemoryPeak=50,693,111,808 bytes`（約 47.2 GiB，低於 48 GiB `MemoryHigh`）、swap peak 約 84 KiB，unit 成功。正式特徵依新來源收據重建為 **9,606,617 列／143 特徵**，`stock_merge=22.728s`、`total_before_summary=109.046s`、輸出 SHA-256 `cc932c65305f7c5d25aac3001e1c7ead14ca1fad202e24749a452c9e601ee8c2`；先前正式輪相應為 42.886s、127.693s。兩輪來源版本不同，故只能說正式環境觀察到約 **47.0%／14.6%** 的階段縮短，不能宣稱位元組等價或 p95；相同來源的隔離新舊輸出等價證據見前節。

資格 watcher 失敗的真正來源日曆原因是把 09-29 主檔之前的 09-25、09-28 當成必須有當沖歷史報表的普通平日；[TWSE 官方 115 年開休市表](https://www.twse.com.tw/holidaySchedule/holidaySchedule?response=html) 明列兩天休市。新規劃只從既有 TWSE OpenAPI 官方日曆、核對 `_dataset`／`_source`／`_as_of_date` 後，跳過同一 next-session 前方已證實休市的平日；無日曆或衝突時 fail closed，不略過任何未證實開市日。資格 watcher 的 canonical 下載命令也改為使用原有 TAIEX Parquet SHA-256／收據綁定的完整實際交易日曆，只要求驗到上一個已開市日再加上 09-29 的精確官方 TWSE／TPEx 規則；不是用明天不存在的指數 K 線補假資料。對直接以 `downloader/download_tw_public_data.py` 路徑執行的 Python import 邊界加了實測回歸，避免服務啟動方式不同而快速失敗。

watcher 在 07:12:57 先以精確會員覆蓋寫出 `status=ok`：TWSE 09-29 **1,231** 檔、TPEx **843** 檔，但當時 TWSE 舊式全史狀態仍有 223 個 weekday 假缺口。其後在正式全域來源鎖及原來源節流下，以 canonical 下載器重驗兩份來源，約 **4.12 秒**，摘要 `ok=2, failed=0, coverage_complete=true`；兩份狀態均為 `checked_through=2026-09-29`、`failed_dates={}`、`missing_dates_after=0`，記錄 `same_session_official_closed_bridge=[2026-09-25,2026-09-28]`。這是下一個開市日資格，不代表 9/25 或 9/28 有交易，也不證明 9/29 開盤成交。休市／鎖／直接啟動的相鄰 **42 passed**，Ruff 與差異檢查通過。

07:15 唯讀覆蓋收據 `artifacts/benchmarks/service-coverage-20260925T0715-holiday-bridge.json` 仍有 **50 service／39 timer／2 path**；六個本機 API 均 200，但 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、全資料監控 `critical`，這些不因資格修復而變綠。06:30 `stockagent-registered-data-daily.service` 歷史失敗仍保留，已於 07:15 起以原生 systemd unit 重新執行，尚待同輪完成收據。

07:24 在 registered daily 的 Yahoo 更新負載下另存唯讀 `artifacts/benchmarks/service-coverage-20260925T0724-under-daily.json`，仍涵蓋 50／39／2。六個本機 status GET 皆 200；同一順序的 07:15→07:24 單次耗時為 TAIFEX 37.265→36.293 ms、當沖 3.577→4.200 ms、隔日沖 32.317→146.006 ms、Shioaji 331.505→378.215 ms、OpenBB 3.388→3.351 ms、全資料 10.130→9.815 ms；主機 10 秒 I/O PSI `some` 為 0→5.13%。這是**兩個單次樣本**，不能推論因果、p95 或公網視覺完成時間。單獨的公網 IPv4 HTTPS `/healthz` 此時 HTTP 200／約 521 ms，本機 `/healthz` HTTP 200／約 1.45 ms；IPv6 外網路徑與真冷啟動仍未驗收。完整公開資料下載／資格／來源鎖相鄰回歸 **198 passed**，Ruff 與差異檢查通過。

07:30 的自然 `stockagent-tw-day-trade-eligibility.timer` 一輪即成功，systemd wall **923 ms**、CPU **794 ms**、記憶體峰值約 **116.7 MiB**；正式收據 `attempt_count=1`、`status=ok`、`trading_date=2026-09-29`、`reused_existing_exact_session=true`，精確覆蓋確認約 **52.647 ms**。TWSE OpenAPI 回 200／1,231 列、TPEx 回 843 列，兩份 SHA 與 07:12 的成功收據相同。這證明自然排程重用本地已驗證資料，不證明 09-29 市場開盤後的行情或成交。

07:16:47～07:38:27 的原生 `stockagent-registered-data-daily.service` 補跑正式成功：systemd `Result=success`／exit 0／`MemoryPeak=10,308,947,968 bytes`，同輪 TSV `registered-daily-20260924T231647202328080Z` 為 `completed`、wall **1,300 秒**、無失敗步驟，逐步驟收據 **16 complete／0 failed／0 running**。先前實際失敗的 OKX 1m 步驟本輪 **59 秒／exit 0**；Yahoo US stocks **1,281 秒／exit 0**，來源摘要的 run ID 精確匹配本輪，仍明列 **12 failed + 618 lagging_skip = 630** 個未解標的。`stale=12,219` 是修補前計數且同輪 `repaired=12,219`，不能再加到未解缺口。這次只證明 runner 與已修的尾端日期／腳本路徑正常，不證明 Yahoo 美股來源完整；06:30 的原失敗歷史保留，沒有 `reset-failed`。

補跑後唯讀 `artifacts/benchmarks/service-coverage-20260925T0740-post-daily.json` 仍覆蓋 **50 service／39 timer／2 path**，`systemctl --failed` 為 0；六個本機產品 GET 均 200，業務健康仍分別為 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、Shioaji `waiting`、OpenBB `active`、全資料 `critical`。全資料監控 327 個必要端點中 **65 unable、37 catching_up、225 complete**；磁碟仍約 95% 已用／可用約 102 GiB，Binance 公開封存的 10% 安全保留問題未解。補跑後 10 秒 I/O PSI `some=0`，不代表長期低延遲或所有來源就緒。

## 9/25 07:45～07:52 OpenBB L1 來源稽核的反證測速與正式觀測點

07:23 的自然 L1 輪次在 07:29:36 成功結束，總 wall **356.844 秒**／CPU **130.685 秒**、20 個新段、0 stale／failed，但 `stale_source_contract_query=267.209s`，另有衍生 Parquet metadata 掃描 51.249s。這是 17.6 GB SQLite manifest 的完整來源契約稽核；不能藉跳過稽核取得假速度。

新增唯讀 `scripts/benchmark_openbb_l1_stale_scan.py`，在同一 SQLite 讀取交易比對完整查詢及回傳列 SHA-256。候選「先順序掃成員、再查段」耗 **69.478 秒**，原「先掃段、依段索引取成員」耗 **20.458 秒**；兩者都是 0 個失效列，SHA 同為空集合雜湊。因候選更慢，**正式查詢沒有切換**。這組 A/B 候選先跑、原版後跑，cache 熱度不同，不是 p95；收據為 `artifacts/benchmarks/openbb_l1_stale_scan_20260925.json`。

以與正式 unit 相同 `MemoryHigh=2560M`、`MemoryMax=3G`、CPU／I/O 權重建立獨立 transient systemd unit，只做原版查詢：**19.264 秒**、wall **19.617 秒**、CPU **19.566 秒**、記憶體峰值 **51 MiB**。再測 SQLite `cache_size=128 MiB`：原版查詢 **21.925 秒**、wall **22.340 秒**、記憶體峰值 **182.8 MiB**，未見效益，因此未改正式 cache 設定。兩者回傳 0 失效列、同一 SHA。這個熱資料唯讀快測不能解釋自然輪次的 267 秒，更不能歸因於 memory.high；cache 冷熱及同時工作負載仍待正式輪檢驗。對照收據為 `artifacts/benchmarks/openbb_l1_stale_scan_lowmem_20260925.json` 與 `artifacts/benchmarks/openbb_l1_stale_scan_cache128_20260925.json`。

正式 L1 現將既有 `stale_source_contract_query` 再細分為「既有 stale 段查詢」與「來源契約 JOIN」，並在稽核前後記錄本程序實際讀／寫位元組、記憶體與 cgroup stall 事件；不變更失效判定、來源讀取或輸出。既有 stale 段查詢在當下熱資料單獨測為 **0.011 秒／0 列**。下一個符合開盤保護與來源空閒條件的自然執行，才可確認 267 秒是 JOIN 本身、冷磁碟讀取或環境負載；未完成前不提高記憶體上限、不換索引，也不將唯讀 20 秒當作正式服務已優化到此水準。

08:01:02 的下一個 timer 觸發被原有開盤保護 `ExecCondition` 正常跳過（`Result=exec-condition`、正式 compactor 未啟動）；即使今天官方休市，這道共用時間保護目前仍按平日作用。沒有為了跑測速修改或繞過它；下一筆正式稽核收據尚未產生。

## 9/25 07:53～08:00 Shioaji 面板快取上界與閘道就緒

Shioaji 公開頁讀既有本機收據與 systemd/journal，**不登入永豐，也不發 API 歷史查詢**。本機單次冷 HTTP GET 約 **392 ms**、立即重請求約 **1.5 ms**；獨立直呼的冷來源工作：合約 manifest 769 份約 54 ms、最近 capture receipt 約 87 ms、並行本機 service/journal 約 182 ms；各項熱讀為約 26／6／28 ms。這是不同進程、不同 cache 狀態的局部樣本，不能直接相加或宣稱 p95；數百毫秒冷建置並未因本次 LRU 修正消失。短期快取與 stale-while-refresh 已存在，沒有為降低首訪秒數而提高來源過期容忍或額外每秒輪詢券商。

發現程序內 JSON 收據快取以前隨歷史路徑無上限增長；同一輪狀態建置即讀入約 **1,100** 個條目。現用有鎖的 **8,192 條 LRU** 上限，命中仍驗 `dev/inode/size/mtime_ns/ctime_ns`，超限只逐出可重讀的快取，不刪原始資料，且回歸測試涵蓋逐出後檔案變更重讀。這是長期記憶體穩定性保護，**不是**已證明的當前 API 降延遲。

07:57:29 第一次僅重啟唯讀公開閘道時，`systemctl` 立刻回 `active`，隨後立即打本機 `/healthz` 一次得到連線拒絕，約 3 秒後 Python 才監聽；這暴露的是 **Type=simple 的啟動就緒競態**，不是交易或永豐連線中斷。新增固定 loopback `/healthz`、最長 60 秒的 `ExecStartPost`，配套 `TimeoutStartSec=70s`。`systemctl restart` 現在等到端點 HTTP 200 才回成功；07:59:35～07:59:37 的第二次正式重啟確認日誌為 `Starting`→`listening`→`Started`，其後本機健康 200、公開 IPv4 HTTPS 健康及 Shioaji API 均 200，未重啟行情或交易服務。這改善**啟動結果的真實性**，並非零停機切換；重啟期間仍可能短暫無法連線。

08:00 新唯讀覆蓋收據 `artifacts/benchmarks/service-coverage-20260925T0800-gateway-readiness.json` 為 **50 service／39 timer／2 path**，`systemctl --failed` 為 0。本機六產品端點皆 200，但業務健康仍是 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、Shioaji `waiting`、OpenBB `active`、全資料 `critical`。該輪 Shioaji 本機 GET **174.399 ms**，是單一不同 cache 狀態樣本，不能拿來宣稱從 392 ms 穩定減半。相關公開網頁／Shioaji／OpenBB 測試 **158 passed**、Ruff、Shell 語法與差異檢查通過。Windows 任務非零結果、外網 IPv6、真正冷開機及上述資料健康缺口仍未消除。

08:01 唯讀檢查獨立 D: 冷備份最新 `current_heads` 收據仍是 `current_heads_complete=true`、`remaining_bytes=0`，但當輪 `metadata=20.766s`、`destination_trust=11.757s`，與先前約 10／9 秒不同；DrvFs 與並行 I/O 負載未固定，不能把先前加速當成永久上限。這是下一個需要拆分 manifest 與 head 驗證成本的測速點；本輪沒有中斷正在運行的備份服務或跳過校驗。

## 9/25 08:04～08:18 D: 備份 metadata 與休市守護鏈

新增唯讀 `scripts/benchmark_packed_backup_metadata.py`，從當前 121 個 head 選出 head/manifest 共 **242 檔、3,452,489 bytes**，用 A-B-C-C-B-A 順序比較既有 `safe_path`、每檔 no-follow FD 與目錄 FD 重用，六次檔案內容摘要均為 `c2206b859fe7e98eb4f83100e3f64225973e4600336c564bc81c3a5025e0b4fd`。較接近正式程式的每檔 FD 路徑在一輪測得 **1.846／2.066 秒**，既有路徑 **4.352／4.048 秒**；更快的跨檔目錄 FD 重用 **0.912／0.929 秒**尚未進入正式服務，因其跨檔 pin/recheck 的維護與競態成本需要獨立驗收。這只是當前 head metadata 讀取，沒有測全備份 p95 或恢復。

正式備份只在**既存、完全相同**的 metadata 使用 no-follow FD 快路徑：從根到檔案逐級禁止跟隨符號連結、讀前讀後檢查檔案 signature、最後確認每級目錄仍是原 inode。缺檔、變動及寫入繼續走既有 volume guard、不可變版本檢查、head-history、原子寫入與 readback；不改冷備份保留或刪除語意。38 個相鄰備份／保留測試通過。重啟的只有獨立 `stockagent-packed-backup.service`，沒有重啟交易、行情或 Discord。正式 08:10 與 08:15 兩輪 `current_heads` 收據均 `up_to_date`、`integrity_state=verified`、121 heads／121 manifests／11,834 objects、463,950,911,986 已驗證 bytes、0 剩餘與 0 錯誤；最新 `metadata=5.870s`、`destination_trust=9.433s`、整個前段 `16.405s`。相對 08:01 的 20.766s metadata 為觀察改善，兩輪資源競爭不同，**不能**宣稱穩定 p95；歷史完整度仍 `not_checked`。

08:10 發現 `stockagent-tw-public-0830-check.service` 真實失敗：今天 09-25 為官方日曆已驗證的中秋節休市日，程式仍要求「今日」TWSE／TPEx 當沖資格、跑 >100 秒嚴格稽核，因精確來源實為下一開市日 09-29 而失敗。08:30 驗收改為先調用與開盤就緒檢查**同一交易日契約**；已驗證休市寫 `skipped`、資格／模型稽核欄位均為未通過，未知／衝突日曆仍失敗，不抓假資格也不跑昂貴稽核。08:15 自然 timer 留下 `session_state=closed`、`steps=[]`、同日收據，unit 成功、`systemctl --failed` 清零。守護程序原本只按「平日」判斷，因此每分鐘又重觸發資格及 08:30 驗收、把休市誤報為未就緒；現沿用同一官方契約，已驗證休市不觸發，不暫停背景工作，未知日曆明列失敗。08:17 自然 guardian 收據 `session_state=closed`、無上述兩個警報，只剩磁碟空間警告。相關公開資料／guardian 測試共 **94 passed**；這不代表下一開市日驗收通過。

前一次 08:08 嚴格稽核的真實 `model_safe=false` 仍保留在原 audit 目錄，`findings.csv` 有 **1 critical／1 medium**。critical 是 2026-05 TPEx 一則富邦基金受益憑證 `T1001Y` 的收益分配公告被誤當成未解析股票事件；下載器現在辨識明示的字母前綴證券代碼並把此類**有標記且不在股票／ETF 執行 universe** 的公告列入可追溯的 out-of-universe，而不是隱藏未解析資料。這條規則的 46 個相鄰測試通過。08:21 首次正式重驗因另一個研究特徵工作持有 canonical 全域來源鎖而在 60 秒上限退出，**未搶鎖、未中斷生產者**。待 08:20 正常啟動的特徵工作結束後，08:23 以原下載器、原兩層來源鎖與 10 req/s 共享節流重驗 2026 年：`announcements=1503`、`failure_count=0`、`unparseable=0`；原公告以來源 URL、文號和主旨留在 `out_of_universe_records`，沒有進入股票訊號。正式收據的 `coverage_complete=false` 仍忠實反映歷史交叉覆蓋限制。medium 是官方下市公告交叉覆蓋不足，屬真實歷史來源限制，仍須保留。

同輪發現 08:30 嚴格稽核快取指紋以前未納入 `tw_short_sale_download_report.json`：若該來源直接更新、全球下載摘要未變，可能錯誤重用舊的 `model_safe=true`。現在將放空規則及 TAIEX 官方摘要納入依賴指紋；實檔變更會讓下輪嚴格稽核重新執行，新增隔離回歸確認這一點。**這是快取正確性修復，不是稽核通過證明。** 08:25 使用與 08:30 相同參數的獨立嚴格稽核約 3 分 45 秒、退出碼 2；原 `unparseable` critical 已消失，但當天 08:20 的特徵檔早於 08:23 的放空規則來源修正，`stale_feature_build_receipt` 成為新的 critical，`model_safe=false`，原始失敗收據保留。它也觀察到全量 panel 冷建約 **109.937 秒**、大量記憶體與可見 PSI 壓力；非公開網站 API 的延遲。08:30 用原生 `stockagent-tw-public-feature-reconcile.service` 重新建特徵與研究輸出，`Result=success`、cgroup `MemoryPeak=51,543,433,216 bytes`、沒有 OOM；正式特徵 `build_mode=full`、`incremental_fallback_reason=base_contract_not_verified`、9,606,617 列。獨立的 `audit_feature_build_receipt` 六項檢查（來源 bytes、輸出 bytes、schema、availability、universe）全通過。這仍不是完整稽核；08:33 在 48G MemoryHigh／64G MemoryMax、低 CPU／I/O 權重的獨立 transient unit 跑同契約的完整複驗，待 `summary.json`／exit code 驗收。

08:33 的正式參數複驗已於約 **3 分 59.665 秒**完成，systemd exit 0；`artifacts/data_refresh/tw_public/0830/audit/20260925T0833_final_short_rule_recheck/summary.json` 為 `model_safe=true`、**0 critical／0 high／1 medium**，放空來源六項與特徵來源六項收據均通過。唯一 medium 仍是歷史下市公告交叉覆蓋不足，不能消去。這是**09-25 08:33 當時資料與稽核程式**的合格收據，不是 09-29 開盤成交或所有資料來源健康。複驗 cgroup 峰值約 **48 GiB RAM／8 GiB swap**，`memory.high` 事件逾七萬；panel 冷建約 **114.873 秒**，整輪另有約兩分鐘成本尚未分段定位。為後續精準優化，嚴格稽核下一輪的 `summary.json` 現會附每一主要階段耗時、程序 RSS 當前與最高水位；純觀測，不略過任何驗證。**新觀測欄位尚未經下一輪正式稽核產出**，不能把 08:33 既有 summary 當成有分段的新格式。

複驗後唯讀 `artifacts/benchmarks/service-coverage-20260925T0838-post-audit.json` 保持 50 service／39 timer／2 path，`systemctl --failed` 為 0，六個本機產品 API 均 HTTP 200；真實產品健康仍分別為 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、Shioaji `waiting`、OpenBB `active`、全資料監控 `critical`。從本機發起的公開 IPv4 HTTPS `/healthz` 為 HTTP 200／單次約 821 ms，不等於獨立外網或雙棧驗收。複驗負載中單次本機全資料摘要約 1.17 秒、結束後覆蓋探測約 3.455 ms；快取與資源狀態不同，不能直接當成穩定改善。併跑相關資料稽核／公告來源／公開驗收／guardian／備份回歸 **227 passed**、PyCompile、Ruff、差異檢查通過；仍非全庫測試或真冷開機。

08:42 另以**相同資料、同樣嚴格稽核參數**驗收分段觀測版：unit exit 0、`model_safe=true`、0 critical／0 high／1 medium；panel 命中對應來源的合法快取，因此是**熱 panel 對照**，總 wall **1 分 58.455 秒**、峰值約 **6.6 GiB RAM／0 swap**，不能拿來冒充失去 canonical cache 時的冷建速度。`summary.json` 的分段耗時：逐檔行情來源 **42.807s**、TWSE／TPEx tick 格線 **33.733s**、來源與特徵收據 **11.969s**、歷史來源 **9.692s**、標的／價格 provenance **8.522s**、公開特徵表 **3.464s**、panel 快取載入 **3.194s**。此輪驗證證明新觀測欄位可產出且不改 finding；後續建議先針對 42.8s 與 33.7s 各自優化。逐階段現在也輸出短日誌，讓被中止的執行仍有最後完成階段的證據；該新增日誌尚待下一次自然執行驗收。

價格格線的 33.7s 中，安全類型判定原本每列重複呼叫純函數。唯讀 A-B-B-A CPU 基準各取**真實 TWSE／TPEx 前 100 萬列**，逐列結果 SHA-256 在四輪皆相同：TWSE 直接 **0.802／0.806s** 對 bounded-cache **0.195／0.171s**（891 種鍵）；TPEx 直接 **0.940／0.993s** 對 bounded-cache **0.233／0.201s**（3,821 種鍵）。這僅是載入記憶體後的分類 CPU，**不代表完整格線或全稽核可同倍加速**。正式格線稽核已改為單檔 16,384 筆 LRU，鍵保留 `(symbol,name)` 以維持未來可能的名稱規則；價格數值、例外與檔案 SHA 的判定均未改。兩份**完整正式來源檔**再次唯讀執行：TWSE 5,329,567 列 **16.051s**、TPEx 4,419,006 列 **13.272s**，合計 **29.324s**，兩份回傳物件與修正前正式 `summary.json` 的格線欄位**逐欄完全相同**，皆 `quote_grid_valid`。相對前次 33.733s 階段觀察約少 4.4s／13%，但這不是同程序配對的完整 A/B 或 p95，不能全歸因於快取；CPU 前綴 A/B 才隔離了分類函數成本。格線相鄰測試 **59 passed**。

## 9/25 08:53～09:00 跨服務韌性與開盤資源保護

逐檔股票行情來源稽核另以全部 **2,757** 個真實來源檔做唯讀 A-B-B-A：64 workers 為 **43.640／45.579 秒**，16 workers 為 **43.522／43.314 秒**；完整 profile、摘要與 finding 的 SHA-256 四輪完全相同。降低到 16 workers 沒有可重現的吞吐改善，故**沒有**變更正式併發設定，也不以單次秒數推導全服務 p95。基準程式為 `scripts/benchmark_tw_public_quote_audit_workers.py`。

根檔案系統現約 **94.95% 已用、102 GiB 可用**。`scripts/maintain_storage_pressure.py` 的唯讀完整盤點為 `under_pressure=true`，但符合已批准的舊編譯快取檔案 **0 筆／0 bytes**；收據 `/var/lib/stockagent-storage-pressure/receipts/storage-pressure-audit-20260925T005514.349997Z.json`。packed retention 最近的已驗證實際回收為 **532,140,032 bytes**，相對 2 TB 檔案系統的 10% 保留缺口不夠。**沒有**強制清除資料、降低 Binance 封存空間閘門或修改冷備份保留政策；這個來源工作仍受容量限制，須以具體 catalog／D 驗證／程序參照及擴容計畫解決。

08:30 驗收原只在 **08:50** 後禁止*啟動*大型衍生重建／完整稽核；已量到這類工作約數分鐘，08:45 timer 若才啟動，就可能跨越 08:50 並與 09:00 訊號爭資源。現在將這四類非必要大型步驟的**啟動**保護提前到 **08:40～09:05**；同一份快速、已驗證的稽核仍可重用，當日精確資格修復不被此規則禁止，未完成的大型工作在收據標 `deferred` 並交由 09:10 timer 重試。這是針對已測工作時長留 20 分鐘開盤緩衝，不是對任意長尾任務的硬性截止；極慢已在執行的工作仍可能跨界，後續要以自然開市日測速與 cgroup 證據驗收。前置公開來源重抓的步驟另記真實 `elapsed_seconds`，避免把來源等待誤歸因於嚴格稽核。

08:59 唯讀服務覆蓋 `artifacts/benchmarks/service-coverage-20260925T0859-resilience.json` 仍為 **50 service／39 timer／2 path**，沒有 systemd failed 單元。六個本機產品 GET 皆 HTTP 200；單次傳輸時間依序為 TAIFEX **43.472 ms**、當沖 **4.066 ms**、隔日沖 **26.448 ms**、Shioaji **350.716 ms**、OpenBB **4.381 ms**、資料監控 **13.783 ms**。業務健康仍依序 `blocked`／`degraded`／`waiting`／`waiting`／`active`／`critical`。這是不同 cache 狀態的單次 loopback 探測，不是 p95、更不是券商／來源／外網延遲；HTTP 200 與 unit 未 failed 均不可宣稱所有業務正常。09:00 前相鄰公開來源／守護／稽核／備份測試 **235 passed**；PyCompile、Ruff、`git diff --check` 通過，並未執行全庫測試或真冷開機。

## 9/25 09:10～09:17 研究來源額度與公平排程

在執行中的 FinLab 研究同步實測：帳戶 5,000 MB 日額度，09:10～09:12 的 `after_market_fixed_price` 若干大欄位即使內容 `unchanged`，也各需約 23～28 秒並可消耗約 73～75 MB；09:14 的其他歷史小欄位有約 2～4 秒且僅約 0.1 MB 的例子。故每個「鍵」的 API／CPU／額度成本不等；不能從完成鍵數推算剩餘時間或宣稱可在一個額度週期完成全 1,110 鍵。SDK 的 `force_download=True` 是雲端重取，改成無條件本機快取雖快，不能作為發版閘門要求的上游檢查證據；保留此正確性界線。

真正的可修復故障是額度耗盡時的固定順序飢餓：之前所有到期的非精選既存鍵跟著 catalog 字母順序，下一個 08:00 週期又可能重訪同一前綴。現在排序為**精選鍵 → 未下載鍵 → 最久未做雲端檢查的已下載鍵**；不更改每鍵強制查來源、50 MB 明示餘量、逾時／失敗記錄、24 小時全目錄研究發版閘門或原始 Parquet。實際運行在修改後改訪前日 02:36 UTC 起的 `capital_reduction_otc`，不再只訪前日 14:25 UTC 的字母前段。相鄰 FinLab **44 passed**、Ruff 與 whitespace 檢查通過；正式輪尚在執行，最終額度／來源完整度待收據驗收，研究發版仍可能因帳號額度與未取得鍵而保持 blocked。

同時唯讀容量盤點得到 `/srv/stockagent-live` 約 **71.1 GB**、`/srv/stockagent-packed` 約 **414.2 GB**、`/srv/stockagent-packed-materialized` 約 **39.7 GB**；`/root/stockAgent` 項在 90 秒低優先權上限內未掃完，不能把這三個加總當成整個 1.8 TB 根磁碟的完整歸因。受管快取 GC dry-run 的 3 個版本為 2 pinned、1 lease-active，**0 個**可安全逐出；未刪除資料或降低封存的 10% 空間閘門。

## 9/26 全服務覆蓋與欄位快照序列化

唯讀覆蓋收據 `artifacts/benchmarks/service-coverage-20260926T1230-goal-next.json` 列出 **57 service／42 timer／3 path**，無 systemd failed unit；六個本機產品 API 均 HTTP 200，但 TAIFEX `blocked`、當沖 `degraded`、隔日沖／Shioaji／OpenBB `waiting`、全資料監控 `critical`。這些是不同層次的狀態，HTTP 與 unit 成功不能覆蓋業務缺口。資源分拆指出 Shioaji TX 回補 cgroup 約 11.8 GiB、OpenBB 封存約 9.4 GiB 主要是可回收檔案快取，而非等量匿名程序記憶體；沒有據此中斷或限速活服務。

資料監控每 30 秒正式輪在來源變更時約 5～9 秒，當中 87,626 欄位／約 54.6 MB JSON 的重建寫入約 0.55～0.97 秒。對當時同一份已完成快照，在同一 Python 行程做三輪純 `json.dumps` 對照：`sort_keys=True` 為 **508.6／472.7／433.0 ms**；`False` 為 **330.5／320.5／329.5 ms**，兩種輸出 UTF-8 位元組數相同。只對欄位快照停止物件鍵排序；JSON 的欄位值、列順序、嚴格非有限數拒絕、原子替換及整檔 SHA 收據維持原契約。這是約 0.1～0.2 秒的序列化 CPU 局部改善，不是整個 30 秒週期或公網 p95 的倍數改善。自然下一輪 `feature_stages_ms.atomic_write=624.130 ms`，仍有負載波動。完整快照 **87,626 列**與首 80 列的 SHA 綁定預覽驗證通過；本機 `/healthz`、欄位分頁、摘要 HTTP 200，單次約 1.6／34.2／10.4 ms。監控／公開閘道相鄰 **228 passed**，Ruff 與 diff whitespace 通過；非全庫測試。要顯著降低來源變更輪的 5～9 秒 CPU，仍須設計資料集增量投影與全域摘要／來源標籤的一致性證明，不能直接把唯讀影子測速工具升格為正式快取。

同日後續檢查指出另一個正確性風險：完整 feature 快照原本只綁實體資料 `feature_revision`，沒有綁來源清冊中實際顯示的 `source_title`／`provider`／`market_category`。資料檔未變而來源標籤改變時，舊快照可能繼續重用。現讓指紋與正式公開投影共用相同的來源標籤解析；只對 251 個 inventory dataset key 計算顯示標籤指紋，排除會每輪改變的健康／時間欄位。指紋納入原有 SHA 收據的 revision binding；舊收據、缺綁定或標籤改變會重新建置，未變則保留原快路徑。現場單次指紋計算約 **1.311 ms**；自然 04:38:15 UTC 輪資料未變、`feature_reused=true`、欄位階段 **31.330 ms**。當輪 87,626 筆欄位所屬 **233** 個資料集之顯示標籤與當前公開來源逐一比對，**0 差異**；這是當時快照的驗收，不是未來來源永遠不變。相鄰測試 **229 passed／1 skipped**、Ruff、PyCompile、diff whitespace 通過；本機欄位分頁 HTTP 200。此修正不改變資料擷取、PIT 判定、交易帳本或資料健康。

補上一個失敗邊界：若綁定收據遺失或損壞，只有完整 JSON parse 也**不能**證明舊列的顯示標籤來自本輪來源，因此正式帶標籤指紋的重用路徑直接拒絕並重建；不會藉 parse 舊檔順手寫出新綁定。缺收據／舊收據／標籤改變均有隔離回歸。

本輪亦以真實清冊拆開量測：81,312／87,626 欄位屬 FinLab 已下載寬表；在共用已解碼清冊下，該資料集欄位重算單次約 **521 ms**，其餘資料集約 **746 ms**（後者主要是 55,820 檔簽章驗證）。FinLab 1,105 份 receipt 單獨熱讀約 **27～35 ms**，故新增跨程序 receipt 快取不是現有 0.3～0.5 秒監控階段的主要解法；512 個 quick-index 預檢簽章熱掃約 **5～7 ms**，而完整檔案指紋約 **559 ms**，不能僅憑某輪 1 秒預檢時間就刪掉完整性預檢。這些反證排除了兩個表面上容易但效益／安全性證據不足的捷徑；真正的欄位增量建置仍須以原始統計、全域摘要及來源標籤同代驗證後再切正式路徑。

04:41 UTC 修正後唯讀覆蓋收據 `artifacts/benchmarks/service-coverage-20260926T1243-goal-metadata.json` 仍為 **57 service／42 timer／3 path**，六個本機產品 GET 均 200；業務健康為 TAIFEX `blocked`、當沖 `degraded`、隔日沖／Shioaji／OpenBB `waiting`、全資料監控 `degraded`。監控當輪 **414 個活躍資料端點**中仍有 **122 unable、25 catching_up、42 waiting_publication、225 complete**；從較早 `critical` 變 `degraded` 是當時讀到的健康狀態，**不是**欄位快照標籤修正治好了上游缺口。註冊歷史 backfill 的 Binance 失敗收據仍保留，最新 registered daily 雖 `completed`，Yahoo 的 **630** 個未解來源項目仍在；沒有改寫成正常。
