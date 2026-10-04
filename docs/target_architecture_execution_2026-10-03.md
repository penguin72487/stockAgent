# 目標架構實作與驗收（2026-10-03）

使用者本次明確要求啟動目標，按先前的五階段規劃實作、修正與優化。
工程任務為 `target-architecture-20261003`，接續已接受的
[現代化工程](architecture_modernization_2026-10-03.md)與尚在校驗的
[服務／來源修復](architecture_readiness_repair_2026-10-03.md)。舊驗收保持其原始
版本與範圍，不將新工作倒填成舊結果。

## 第一性原理與順序

目的是減少取得可信觀測、建立可重現研究、完成評估及保存／恢復證據的總成本。
來源資料與盤中帳本各有唯一 owner；工作控制只編排既有入口，不能改寫市場事實。
效率分別量測實際補進的分區／API 成本、完整 fold／GPU 小時、更新放大率、
前端等待及故障恢復人工介入。零失敗與相容身分是效能比較的前提。

| 階段 | 本次責任 | 交付證據 |
| --- | --- | --- |
| 1：版本與設定 | 延續已接受 code release、解析後設定、runtime 與來源 release 身分；重新核對可執行／歷史清冊 | 目前清冊、來源 SHA、啟動驗證、版本變動拒絕與新發布驗收 |
| 2：來源與遠端衍生 | Penguin 保存 authoritative observations／receipts；遠端按固定 release 重建版本明確的研究視圖 | exact release、ABI／config／runtime 與 canonical rebuild receipts；沒有自動 hydration 或資料刪除 |
| 3：共同工作控制 | PostgreSQL 先試行非盤中、唯讀的 canonical code-release 驗證；統一身分、依賴、claim、lease、attempt、資源准入與完成證據 | 競爭 claim、過期 worker fencing、中斷恢復、有界重試、資料庫備份／恢復與真實節點證據 |
| 4：模組與主成本 | 沿現有責任拆大型模組；依整段工作流的 profile 選擇監控與 GPU 候選 | 受影響模式／完整 lifecycle parity、冷啟動／穩態與資源消耗，原輸出保持完整 |
| 5：規模擴充 | 依多 writer 分析表／多引擎／長工作鏈的實際需求評估 DuckLake、Iceberg、Temporal | 相同工作流比較、採納或不採納理由、catalog／workflow 恢復與回滾證據 |

## 本次起點與隔離

Git 起點 `894e782dfde5e12d1f1c6a9d94393a8bc3383ba6`，分支 `twRule`。
TEJ、FinMind、期貨、採集頁面及儲存整理已有其他 active tasks；本工作保留它們的
dirty work、服務與 receipts。前一輪完整回歸及 US 冷校驗持續由原 run 執行。
目前可見 73 個 StockAgent services，舊 US 一次性 scan failure 仍保留；此數量是
本次觀測，不能代替健康／完整度。完整清冊為
`artifacts/operations/target-architecture-20261003/baseline-architecture.json`。

第一條控制試行選擇 `verify-code-release`：輸入是不可變 receipt SHA 與 code SHA，
工作仍呼叫 `stockagent.runtime_identity.verify_source_release`。
它不使用 provider 配額或 GPU，不產生訂單、不移動資料、不啟動正式 fold。
這使 lease／重試的故障注入可以在不影響盤中程序的情況下驗證。
既有 downloader 的分區續傳、shared limiter 及 GPU manager 的本機 lease 保持 owner。

PostgreSQL 的 row lock 只協調領取與完成；lease 使用 server clock，完成須比對
attempt／token 與尚未到期的 lease。這能拒絕舊 worker 的 late completion，
但不能宣稱任意外部副作用 exactly-once。試行限於可重試的唯讀 action；其後
每個有副作用的 action 必須接上原 owner 的 idempotency／outbox／恢復契約。
依據官方 [row locking](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE)
與 [transactions](https://www.psycopg.org/psycopg3/docs/basic/transactions.html)。

## 進度

本輪目標架構基礎與有界試行已驗收完畢：跨節點控制、完整遠端衍生、實測角色、
責任拆分、固定版回歸與發布皆有自己的 receipts。它沒有將每個 provider、
儲存層與盤中執行都改接新控制庫；原 source／GPU／帳本 owner 保持權威。

首份整合驗收為 `artifacts/operations/target-architecture-20261003/delivery-acceptance.json`。
其 source SHA 為 `53ce5c12121ae43043c98025c137a09147c85b6647d1a229e491a0e6d8e41949`，
依據相同 code 的包裝、兩節點工程核對、遠端完整重建，以及 full-suite／later-delta
的明列 lineage。工程驗收、研究相容、cold recovery 與 market readiness 各自保留範圍。

其後 workflow 結案補丁另固定、驗收與發布，最終整合收據為
`artifacts/operations/target-architecture-20261003/delivery-final-acceptance.json`，
綁定 `d3bc29700020…` code 的包裝、真實兩節點／故障接手、完整遠端重建與獨立
62 項 workflow 回歸。原整合收據與 full-suite 不變；新版的工程失敗處置見下文。

### 程式與部署身分補齊

來源 identity 原先未列出 systemd templates 與部分 provider 面板，會讓 installer
與它需要的部署輸入分離。本輪納入這些 Git 管理的公開資產，新增模板修改後
拒絕啟動的測試。最終固定樹有 2,268 files，明列原 full-regression 樹之後
12 個 delta files；新發布 `7c809a28681e…` 包含 1,091 個來源／設定檔、其中
126 systemd templates 與 48 services assets，未混入其他 active tasks 的新修改。
第一份 wheel／`source.zip` 的獨立重建、checkout 外安裝／import、CLI、startup
provenance 已通過（17.008 秒）。後續僅為唯讀比較工具補上拒絕覆寫輸入或既有
證據的 guard，另凍結新發布；第一份候選與其驗收保持原有版本，不倒填。
最終發布 `53ce5c12121a…` 同樣包含 1,091 檔，其新 wheel 相同 SHA 重建／
外部安裝／啟動 provenance 已通過（21.948 秒）；`final2-source-lineage.json`
保存這兩檔補丁與原 12 個累積 delta。兩個來源樹與原 full-regression 樹均保留。

控制角色增加最小 `control` dependency group，installer 由同一份 `uv.lock`
匯出 pinned／hashed requirements；不以未綁定的版本範圍再次解析角色。
本機實際鎖定安裝後，原角色的每個 package version 與 native runtime 都相同，
沒有 DB／服務重啟。`locked-role/control-role-installation.json` 保存 lock、
requirements 與 role runtime SHA。原生 prefix／非 venv 目錄會在副作用前拒絕。

### 控制角色與真實跨節點試行

新增 `stockagent/control/` 的不可變 WorkSpec、PostgreSQL schema／claims／attempts、
read-only worker，並接上 `stockagent-agent work`。工程 role 採 PostgreSQL 18.6、
獨立 psycopg 3.3.6 venv、loopback listener 與 `stockagent-control.slice`，沒有
替換 fintech／CUDA dependencies。private env 維持 0600；一次失敗測試洩出的
新試行密碼已撤銷，更換 receipt 保存於 `pilot-credential-revocation.json`，
fixture repr 也已改為不包含連線值；原失敗測試仍保留。

Penguin／Vast 同一份 915 檔 code release（`9714033544d1…`）真實試行：3 work、
4 attempts，含一次遠端領取後退出、server lease 到期、Penguin attempt2 完成。
原 expired attempt 仍可查，schema backup／restore 的 nodes、jobs、dependencies、
attempts 完全相同。第一次完整試行含 code delivery、role 安裝、SSH private
tunnel、三工作、故障等待、備份與還原，共 21.173 秒；這不是 verifier 或 API
throughput benchmark。詳細證據為 `cross-node-pilot-2/acceptance.json`。
後續 stored-contract guard 的真實跨節點重驗也通過，完整流程為 52.890 秒，
證據為 `cross-node-final/acceptance.json`。兩次執行的節點負載不同，不用這兩個
時長推論加速或退化；其後缺失版本表的 admission delta 再獨立驗收。
最終 `53ce…` 發布也完成同一組三工作／四 attempts、斷線接手與 schema exact
restore（28.861 秒），實際核對 1,091 個檔案；證據為
`cross-node-target-release/acceptance.json`，不是把舊 915 檔驗收改稱新版本。

每日 backup timer 已啟用，第一份完整 DB logical dump 48,589 bytes、0.734 秒；
這次 backup 僅驗 archive 可讀性，真正 round-trip 比較由上述 pilot 另驗。
其後另將實際整個 dedicated DB dump 還原到本次唯一的 disposable DB：
5 個使用者 schemas、20 tables、34 rows 全部一致，原 DB 前後 state SHA 相同，
核對 archive SHA 後移除自己的測試 DB。這份 proof 為
`control-whole-backup-restore-acceptance.json`；原 archive／receipts 保持 private。
第一次 helper 在切換最小 control venv 後誤匯入 native downloader dependencies，
已修正為只在 native launcher 讀入設定，原失敗 run 保留；沒有把原生套件
補裝進 slim role。離機恢復與 PG HA 仍未驗證。
DB／backup 分別有獨立預算，備份未啟用自動刪除與離機 disaster-recovery claim。
三個受保護程序 Discord、TAIFEX BidAsk、TW day-trade 的 PID／啟動時間與
本次 baseline 一致；這只是程序未被本次重啟的證據。

使用與恢復方式見 [control runbook](control_plane_workflow.md)。
CPU／RAM／scratch reservation、lease／公平性／失敗與 canonical verifier 的
實際 PostgreSQL 測試第一輪 30 passed；後續 unknown／empty stored-contract
admission 的真實 DB 回歸 32 passed，原工作與 nodes 不被拒絕動作改寫。
缺失版本表與已存在工作表的情況也完成獨立回歸，共 33 passed／11.55 秒，
證據為 `control-final-contract-regression.xml`，不倒填舊測試。
workflow／runtime／pilot 62 passed，architecture／release／identity／pilot
186 passed。母集合有重疊，不相加當成唯一測試總數。

### 模組與效率的選擇

OpenBB 下載入口的資料型別、JSON ABI、request-level checkpoint 續傳分成
`openbb_archive_types.py`、`openbb_archive_serialization.py`、
`openbb_request_checkpoints.py`，原入口保留 import aliases。
13 個移出的定義 AST 相同，其餘 orchestration／provider／manifest AST 相同，
四份拆分前的 engineering process payload 可讀回，task envelope 與 canonical
JSON bytes 相同。保留的 OpenBB baseline SHA 與原 915 檔發布相同。
602 項 downloader／contract／monitor／supervisor／compaction／benchmark 回歸通過。
第一輪 normalization 發現仍需 Decimal import，已修正，原失敗 log 保留。
這是責任與相容性改善，沒有宣稱 API 下載加速。

監控與 GPU 的選擇沿用原固定版實測：private record 完整 cache 子工作流的
msgspec 候選 wall time 少 13.97%；雙 5090 的 CPU16／48／112 warm epoch3
吞吐差距約 1%，保留 CPU16 的較低宣告預算，不宣稱更多訓練加速。這些數字
屬 [原版完整比較](architecture_modernization_2026-10-03.md)，本輪沒有修改
tensor loss／optimizer／execution 算法，也沒有把舊 fold 當成本輪新 GPU run。

### Lakehouse／workflow 的條件式決策

本輪真正的多節點需求是共同 work claim／lease／attempt／依賴，因此導入 PG
作 opt-in control trial。尚未發現要讓多個節點同時交易提交同一張分析表：來源
producer／immutable release 與 remote derived output 仍有自己的 owner。
因此繼續使用 Parquet／Arrow／Polars／既有 publication，沒有搬市場事實到新
catalog，也沒有新增 Spark／Flink／Trino 執行責任。

DuckLake 的官方選擇將 remote multi-user catalog 配給 PostgreSQL；當真正
需要此類共同分析表提交時，PG 基礎可重用，但 lake catalog 必須另驗 source／
snapshot 映射、restore 與 consumer ABI。[官方 catalog 選擇](https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database)
Iceberg 的 optimistic concurrency／retry validation 適合另有多引擎寫入
需求的評估；目前不增加其 catalog 與 engine。[官方 reliability](https://iceberg.apache.org/docs/latest/reliability/)
Temporal Activities 仍須可重試／idempotent；導入引擎不能取代 collector 的
durable partition resume 或帳本防重。[官方 Activities](https://docs.temporal.io/activities)
目前實測的三工作依賴／中斷接手可由有界 control trial 與原 supervisors 完成，
暫不增加第二套 workflow ownership。這是本專案現況的選擇，沒有比較未執行
的 engines，也不宣稱 PG 是所有 workload 的最快選項。

### 來源衍生與固定版本驗收

遠端衍生選定 exact `tw-public-20260913T030921278338896Z-l0-penguin-248d0869d7d53be4`，
以原 packed inventory／READY 與 canonical `_verify_materialized` 逐檔核對；
131,688 inventory entries、其中 131,332 檔逐一 SHA 已通過。全來源與 code 保持固定，兩份獨立完整
特徵輸出只寫新的 private remote root，不將它們升為服務／訓練預設或刪除源本。
兩份完整重建各有 9,584,859 rows／143 features、1,317,309,373 bytes，SHA 相同
（`85dab0305dc56281…`）。完整驗收含兩次 builder、summary、輸出與前後逐檔
校驗共 505.148 秒，證據為 `remote-derivation-acceptance.json`。

原保存表是 9,694,310 rows／135 columns；新表是 162 columns，新增 27 個
原始單位欄位，共同欄位型別未變。相同 canonical 2,755-symbol scope 與
2026-09-11 cutoff 下，舊表獨有 110,635 個主鍵、新表獨有 1,184 個，均無
重複主鍵。舊表日期始於 1974-03-17、新表始於 1999-01-05，早期市場列是
明確可見的 scope 差異；不能把它當成僅壓縮器變更。保存的來源可讓目前程式
重建穩定的新視圖，但這不是舊 checkpoint resume／歷史 PIT 相容的證據。
兩次 common-column positional comparison 正確拒絕不同主鍵；保留失敗 log，
獨立的 key-only anti-join 診斷為 `derived-scope-diagnosis.json`。原表、來源、
checkpoint 及執行服務沒有升版或刪除。

第一份 builder profile 的主要成本為股票來源組裝（約 203 秒），其中 margin
約 59 秒、OHLC 約 39 秒、merge 約 39 秒。以同一 exact source／915-file
code／native runtime 交錯執行 2／8／8／2 Polars threads；每輪都包含完整新表、
summary、所有來源前後校驗與輸出 bytes parity。採納門檻是兩個時長區間分離、
平均完整 wall 至少少 5%、peak RSS 不超過 32 GiB；未達則保留較小預算。
SHA 校驗會暖 file cache，因此不稱為完全冷磁碟比較，也不省略檢查來改進時長。
首輪 2 threads 的實際 peak RSS 已達 47.45 GiB，32 GiB 候選預算不足；不能
將較少 threads 說成符合 32 GiB。原比較保留其門檻與不符合結果。重建 driver
另增加 64／128／256 GiB 的明確 admission 與實測峰值 gate，預設雙重驗收
保守要求 128 GiB；每次 run 可選經驗證的預算，超限不產生 accepted receipt。
新角色預算必須在最終固定發布上再完成一次完整重建才能採用。

| 完整重建角色 | wall（兩輪，秒） | 平均 builder（秒） | 平均 process CPU（秒） | 最大 RSS（GiB） |
| --- | --- | --- | --- | --- |
| Polars 2 threads | 281.568／274.589 | 233.505 | 513.381 | 47.474 |
| Polars 8 threads | 131.324／128.143 | 85.995 | 587.635 | 47.578 |

四輪完整輸出與先前雙重重建的 9,584,859-row 表逐位元相同，來源、code、runtime
均固定。8 threads 的平均完整 wall 少 53.346%（約 2.143 倍），process CPU
約多 14.464%，memory 幾乎不變。這個遠端節點當時有 224 CPU affinity 與
約 489 GiB MemAvailable，8 threads 的時間收益適合縮短研究準備等待；它不是
最低 CPU-seconds 的選擇，也不是其他硬體的預設。原 32 GiB policy 仍記為
不符合，不將「保留2」說成適合32 GiB。新 64 GiB／8-thread role 在最終固定
code 上另驗完整 output／source／runtime／peak RSS 後才採用。
比較證據為 `derivation-benchmark-acceptance.json`；預先宣告的 5% 時間門檻與
兩個時間區間分離均成立，32 GiB memory 門檻則確實失敗。

最終 `53ce…` code 的 64 GiB／8-thread 完整重建已 accepted：132.288 秒、
peak RSS 47.578 GiB、實際 pool 8，輸出與前六份完整表 SHA 相同；原 131,332
source files／manifest／native runtime 前後不變。新的角色預算與門檻確實通過，
不是把原 32 GiB 結果改成通過。`remote-derivation-final-acceptance.json` 與
`remote-rebuild-role-final-acceptance.json` 綁定最終 code／runtime／source／參數。
本次已直接使用這個角色；沒有修改全域 Polars defaults，也沒有替換原服務 view
或 checkpoint。最初 final helper 的語法錯誤在啟動前被發現與修正，原失敗 run
保留；新的完整 run 是獨立的成功驗收。
另以 canonical lease identity 讀回該 selected materialization：目前 hot lease
有效至 2026-10-10T03:35:02Z，target／manifest 與所選 release 相同。這是
`remote-source-lease-observation.json` 的當下觀測，不倒填成各歷史 job 的准入。
往後短工作先使用既有 `stockagent-data use`／pin 與 object／READY gate；到期
不能只信 ready-marker 而繞過 cache ownership。source acquisition 與 cold edge
policy 仍由原 owner 管理。

固定測試樹為 `/root/stockAgent-target-architecture-20261003`，2,264 files，
基於原受驗樹加本目標 30 個 explicit delta files；不是把其他 active tasks 的
後續 dirty files 混進自己的 full-suite claim。新的完整 `pytest -q -s test`
受 systemd 監督，已成功完成：12,994 passed、78 skipped、0 failure／error，
耗時 3,437.83 秒；skip 保留，沒有將其改稱通過。schema admission、重建 driver 的執行緒／
資源 receipt、新 Parquet 比較工具與它的 tests 等後續 delta，以獨立 focused／
真實重建 evidence 及最終發布 lineage 明列，不改寫這份 full run。

後續 cumulative 12-file delta 的 focused 回歸為 28 passed，control role 的
真實 PostgreSQL／契約回歸為 33 passed。兩個集合與原 full suite 有重疊，
不相加成唯一總數。原 2,264-file 測試樹與兩個 2,268-file 發布樹的內容／
mode 全部再次核對無變動，證據為 `final-immutable-source-verification.json`。
本機 native runtime 與安裝前相同；Discord、TAIFEX BidAsk、TW day-trade 的
PID／啟動時間再次相同，分別記錄於 `native-runtime-final-comparison.json`、
`protected-services-final.json`，不把它解讀為載入新 code 或市場狀態已完好。

### 工程失敗的處置與最後補丁

結案時發現本機工作流程只認同名 run 重試：採用不同名稱的後續驗收，或正確拒絕
不相容候選，都會讓 task 永遠無法完成。新增 `task resolve-run`，以 active task
追加 `superseded`／`expected-rejection`，要求同 task 後續成功 run、理由與當次
證據；拒絕 queued／running／cancelled／未知結果。expected rejection 另須綁定
task、原 run、成功診斷 run 與判準的 JSON，不能以普通的成功檔略過。
原 failed／exit code／log 不變；結案重新核對原 run、log、後續 run 及 evidence
SHA，任何改動或新的未處置失敗仍拒絕。這是工程候選與替代關係的記錄，不會
消除原表不相容、來源缺口、SLO 或正式 artifact／deployment gate。
契約與命令見 [Agent workflow](agent_workflow.md)。

本任務追加五筆處置：兩次不同主鍵比較保持不相容拒絕；date serialization、
final helper syntax、slim-role native import 三次失敗，各由其獨立成功驗收取代。
紀錄為 `engineering-run-resolutions.json`，沒有把失敗 state 改為 succeeded。
新增行為與 workflow／runtime／pilot 回歸共 62 passed，接著在新固定樹再驗
62 passed／10.45 秒。原 full-suite 及 28／33 項補丁驗收保持原本版本，不加總。

第三份固定發布僅加 workflow、語意 tests、runbook 三檔，累積 15 個 later deltas；
`final3-source-lineage.json` 保存 2,268-file 來源樹與原 full-suite lineage。
新 code SHA 為 `d3bc29700020a6af25fb40a2ad4c7ea8a58b70d5235dbac448a16c56e04ec9e9`，
1,091-file wheel／source bundle 的相同 SHA 重建、checkout 外安裝與 provenance
通過，耗時 27.956 秒（`packaging-acceptance-final3/packaging-acceptance.json`）。
同一新版的真實兩節點試行亦通過：3 work／4 attempts、故障接手、schema exact
restore，完整 40.796 秒（`cross-node-final3-target-release/acceptance.json`）。
四份原 full／final 固定樹的逐檔內容與 mode 均保持不變，證據為
`final3-immutable-source-verification.json`。新版完整遠端重建以同一 64 GiB／
8-thread 角色另驗並通過：完整流程 130.534 秒、peak RSS 47.544 GiB、
9,584,859 rows／143 features；輸出與前七次完整重建 SHA 相同，131,332 source
files／manifest／runtime 前後不變。`remote-derivation-final3-acceptance.json` 與
`remote-rebuild-role-final3-acceptance.json` 綁定同一 `d3bc…` code，原 ABBA 測速
與 32 GiB 拒絕不變。最終整合收據綁定 21 份各有範圍的證據，沒有以新的
coordinator 補丁把舊 full-suite 或 benchmark 改稱新版本的量測。

## 後續擴充界線

使用者後續明確要求所有遠端建置都按當下機器條件實測最快設定。原 2／8
比較的結果保持原範圍，不作跨機器預設；新的 cgroup／NUMA／pool 搜尋與
實測套用見[遠端建置量測](remote_build_measurement_2026-10-03.md)。

新增具副作用的 work kind 前，須把它接上既有 collector／GPU／publication 的
idempotency、配額、lease 與完成 evidence；本 trial 的 row fencing 不足以
取代這些 owner。需要多 writer 分析提交、長 durable work chain 或多租戶時，
再依上面的條件採納 catalog／workflow／權限能力。

原 US cold-only 校驗仍在 `architecture-readiness-repair-20261003` 的監督 run 中，
該獨立任務未由本次整合驗收關閉。新表沒有舊 view／checkpoint 相容證據，
來源／模型／既有衍生物沒有被本次刪除，沒有把未驗的 PIT／broker／cold recovery
改成通過。新的研究輸入須使用自己的版本與 artifact root。
