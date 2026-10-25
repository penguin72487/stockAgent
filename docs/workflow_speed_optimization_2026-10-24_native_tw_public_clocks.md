# 工作流測速第二十一輪：台灣公開資料的原生呼叫時鐘

共用台灣公開資料下載器的 `download_dataset` 現在逐次輸出結構化計時，涵蓋
historical、delisted、snapshot、data.gov、GCIS 與 FSC 六種原生分派。
198 項離線測試通過；獨立 AST 核對確認原分派及其他業務程式保持一致。
這完成計時入口的工程驗證；後續已觀測 canonical owner 輸出的原生事件，
所有來源的實際樣本與整個程序載入模組的身分驗證仍待完成。

原生註冊共有 161 個資料集：historical 11、delisted 2、snapshot 133、data.gov 13、
GCIS 1、FSC 1。與保留清冊的 196 個 `tw-public:` ID 逐項比對後交集為 159；
37 個舊 ID 走發布檔案、XBRL 或衍生特徵等其他流程，另外兩個原生 ID 不在該舊清冊。
因此 161 是此入口的可計時範圍，不能當成 196 個來源都已有實測；衍生特徵 ID 也
不能冒充獨立網路請求或與資料集分母相加。

## 計時及來源綁定邊界

新增日誌 ABI `tw_public_dataset_operation_timing_v1`，每行前綴為
`[tw-public:operation-timing] `。沿用共用 `OperationTiming` 的 `perf_counter_ns`，
記錄單次原生分派呼叫的起點、耗時、原生狀態及 PID/thread。
不含 executor 排隊、計時日誌自身、父程序報表與後續 audit，不能稱為完整採集 E2E。
此版只有呼叫總時間，不把整段耗時誤稱為 HTTP、解析或寫入的純耗時。

只有完整 `DatasetSpec` 與目前註冊值相同、分派種類支援、回傳資料集相符、
時鐘有效時，紀錄才標示可供來源綁定。規格及請求設定以 SHA-256 識別；
日誌不包含 URL、憑證、輸出路徑或原生例外訊息。設定指紋沒有冒充實際已解析的
請求日期、快取狀態或完整工作量身分。

保留原 `DownloadResult`、CSV、parquet、raw receipts、PIT 與 parser 契約。
既有總行數可能包含快取和歷史資料，只記為觀測值；本次完成工作量分母保持未知，
不據此製造 rows/sec。`failed`、`incomplete` 等狀態仍照原值記錄，計時可綁定也
不代表採集成功或資料完整。計時／日誌故障不重跑下載、不改結果、不遮蔽原生中斷；
失去計時仍是覆蓋缺口。

## 驗證與監測成本

三份原來源在編輯前完整封存；候選與測試執行前後逐檔 SHA 相符。
198 項測試包括既有下載器回歸、六種分派、結果身分、例外及中斷、來源不相符、
時鐘倒退、無效工作數值與並行日誌。pytest 完整子命令 6.357915 秒；
canonical supervisor 的 launch-to-wait 為 7.051892 秒，兩個邊界分開記錄。

離線 no-I/O adapter stub 的 A/B/B/A、B/A/A/B，各 trial 500 次呼叫，保留實際
時鐘、指紋、JSON、日誌 write/flush，輸出至記憶體。新增平均 wall 成本分別為
**0.138224／0.139955 ms/次**。此數字不含主機日誌持久化或 provider I/O，
不是實際來源延遲或採集加速證明。

00:30 的既有日曆排程自然執行了一次：systemd 單調起迄差為 **20.934206 秒**，
原生日誌回報 writer busy，保留既有日曆後正常退出。該 invocation 的兩筆 journal
均完整留存，沒有下載分派計時事件。因此這是 admission／鎖等待的控制流程觀測，
沒有拿 service exit 0 冒充來源已更新或新時鐘已被正式 owner 採用，也未強制重跑。

01:00 的同一既有 timer 再次自然執行，invocation
`0793b9917bb84a53948c12e1f760bf97` 正常退出，整個服務程序為 **2.055193 秒**。
完整 journal 中有一筆原生時鐘：`tw-public:twse_api_holidayschedule_holidayschedule`，
狀態 `ok`，分派呼叫 **0.508543692 秒**。原生 `producer_pid` 與 journald `_PID`
相符；進度列導致 journal 的 MESSAGE 成為 byte array，讀回時保留原始 bytes，
再解碼並定位完整 JSON，沒有因只比對行首而漏算。

這證明該 canonical owner 的這次來源操作已實際輸出新 ABI。當下來源與先前
候選封存 SHA 相符，但沒有冒充整個程序每份載入模組的身分證明。完整工作量
分母仍未知；沒有把 810 個既有總行數換算吞吐率，也沒有推及其餘 160 個原生
資料集或先前 196 個不同粒度來源。第一次讀回工具誤用 `pid` 欄位而失敗；已依
實際 ABI 修正為 `producer_pid`，保留原腳本、失敗收據及後續成功讀回。

本輪新證據寫入既有 volume guard 保護的 D 私有 benchmark 目錄，pytest 使用
另行 admission 的唯一 root scratch。保留 D 32 GiB／root 8 GiB，沒有刪除來源、
舊證據或啟動 provider、GPU 工作。沒有為取得計時重啟正式服務。

## R32：自然排程後續樣本與實際 collector 綁定

唯讀擷取上次觀測點至 **2026-10-23 21:16:34 UTC** 的八個既有 TW-public
service journal，保留完整 **614 entries** 與原始 bytes；未達 4,097 筆／32 MiB
上限，無選取截斷或無效原生事件。查詢 wall **0.502137 秒**，不將 observer
自身的 CPU counter 當成 journald 子程序 CPU。

新窗口共有 **20 次原生操作、7 個來源**，以下每個耗時均保留其 outcome：

| 原生資料集 | 次數 | 原生狀態 | call wall 範圍（秒） |
| --- | ---: | --- | ---: |
| cbc_money_aggregates | 2 | ok | 3.555458–4.169845 |
| cbc_overnight_rate | 2 | empty、ok | 1.218363–2.080360 |
| fsc_open_data_catalog | 1 | up_to_date | 4.429771 |
| gcis_open_data_catalog | 1 | failed | 7.907659 |
| taifex_daily_options | 1 | ok | 3.050380 |
| twse_api_holidayschedule_holidayschedule | 7 | ok | 0.439764–0.722018 |
| twse_api_opendata_t187ap11_l | 6 | ok | 2.069405–2.568253 |

兩個獨立 supervisor 命令均 succeeded／inactive／MainPID 0，逐筆以正式 parser
核對 PID、boot、invocation、資料集規格 SHA、設定指紋格式及原生時鐘 ABI；
失敗、empty 與 up_to_date 的觀測完整保留。這些 call clocks 仍不包含父程序
報表、後續 audit 或來源採集 E2E，也沒有完整工作量分母。

再以現行 `d3bf90fe…` collector 接收此窗口及 01:00 的原始 journal，共
**21 samples／21 groups**。沿用 `collect_known_source_inventory` 保留原本
**2,399 個 ID**，未從舊 inventory 匯入耗時或宣稱當前來源可見。
六份完整輸出、五份 unique originals、所有 2,399 個身分鏈，均由另一程序
的 canonical reader 驗證。delta collection **1.543289 秒**；獨立 reader
**3.284107 秒 wall／1.693994 秒 descendant CPU**。

其中五個原清冊 ID 可綁定，包含原本已觀測的日曆，以及四個新取得時鐘的
既有缺口：`cbc_money_aggregates`、`cbc_overnight_rate`、`taifex_daily_options`、
`twse_api_opendata_t187ap11_l`。GCIS／FSC 兩個 ID 位於舊清冊之外，另列為
有效原生觀測。全量覆蓋目前仍以 R29 的 1,647 stage-bound／752 unbound 為準；
這份 delta 將供下一次完整 13 階段的來源與覆蓋驗收使用。

原 journal SHA `47852cb8c4c74327421656b7908677cd098f8509b9d405b9b08c0d6e7107873b`；
delta collection SHA `058ca56271fa344a1e6bbfd9722326b7834df0d783bff448495dbc72f4a6f2c0`。
沒有為此啟動下載、GPU 工作或重啟正式服務，R31 原始測速程序持續執行。

- [R32 原始窗口、owner 狀態與來源 SHA](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round32/observation.json)
- [完整窗口獨立讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round32/readback.json)
- [實際 collector 的來源綁定與 2,399 個身分鏈讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round32/delta_readback.json)
- [delta 命令時間與 CPU 邊界](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round32/delta_completion.json)

## 證據與後續

- [位置、admission 與原來源清單](../artifacts/benchmarks/workflow_speed_optimization_20261021/round21/location.json)
- [198 項測試與來源前後身分](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round21/native_checks_v1.json)
- [獨立 AST／JUnit／SHA 讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round21/source_boundary_readback.json)
- [八次離線監測成本觀測](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round21/clock_overhead.json)
- [原生入口與既有來源 ID 的逐項核對](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round21/native_registry_scope.json)
- [自然日曆排程的控制 deferral 與 journal 身分](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round21/natural_calendar_observation.json)
- [01:00 原生操作與完整 journal 讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round21/natural_calendar_native_observation.json)
- [前輪固定輸入全量比較](workflow_speed_optimization_2026-10-23_guarded_d.md)

[第二十三輪接入候選](workflow_speed_optimization_2026-10-24_native_journal_ingestion.md)
已通過 500 項測試及真實日誌 delta 的獨立讀回。
[第二十四輪](workflow_speed_optimization_2026-10-24_canonical_journal.md) 隔離固定比較的
archive execution 後，已合入 canonical collector、通過 696 項回歸並開始全量覆蓋
讀回。尚未把離線 fixture 當成來源覆蓋，也未宣稱所有流程已測完、完整工作流
加速或達到硬體極限。

## R94：剩餘來源粒度與發布檔案時鐘

2026-10-24 03:53 UTC，重新封存目前的原生 metadata，並以另一程序逐檔核對
24 份輸入的 SHA，共 13,935,933 bytes。canonical owner 正常退出，實際
systemd inactive／MainPID 0；整個子命令為 **8.398226 秒 wall／1.855950 秒
descendant CPU**。擷取與分析為 4.760994 秒 wall，獨立讀回為 3.098408 秒 wall。
這些是觀測程序成本，不是資料來源採集時間。

R87 的 2,399 個來源 ID、1,651 個 stage-bound 與 **748 個未綁定**全部保留。
逐項與當前 registry 的明確 `scope` 對照，未綁定項目的粒度如下；沒有靠名稱前綴
推測、刪除別名或重算綁定數。原 baseline 中 566 項屬 active scope、182 項不屬於。

| registry 明確粒度 | 未綁定項目 |
| --- | ---: |
| source_registry | 240 |
| logical_source | 260 |
| registered_endpoint | 11 |
| crypto_fact_family | 35 |
| physical_inventory | 55 |
| inventory_partition | 4 |
| storage_group | 50 |
| product_granularity | 48 |
| credential_gate | 45 |

其中 storage、product、credential 與 inventory 合計 202 項，不能直接解讀成
202 次獨立 API 操作；其對應的盤點、驗證、權限判定與採集工作仍須各自測量。
當前狀態、可排程或已啟用，也不能代替實際操作耗時。

R94 的 11 個 `registered_endpoint` 缺口是七個 ETF 發行商頁面／檔案、
一個 SEC EDGAR 端點及三個 Dune query contracts。其 canonical owners 分別為
`download_crypto_etf_history.py` 的 `_issuer_source`／`_sec_entity`，及
`download_dune_crypto_history.py` 的 `_run_partition`。後續需保留原始來源／SQL
與 partition 身分，量測各自完整呼叫；父層共享 upsert、排程等待、權限／額度
拒絕另外記錄。這份清單來自 R94 封存，沒有把宣告 `complete`／`waiting`／
`blocked`、父層 progress elapsed 或檔案時間戳換算為已測得的端點速度。

另封存八個發布檔案 producer 的狀態，保留兩個 CBC producer 已有的 **9 個原生
單調時鐘數值**及其 float hex、來源 SHA、JSON locator、outcome：

| 原生發布流程 | producer 總時鐘（秒） | 已有階段時鐘（秒） |
| --- | ---: | --- |
| 外匯存底公告 | 118.520043 | 索引探索 98.002；明細收集 20.262；Parquet 證明寫入 0.166 |
| 貨幣公告 | 24.610852 | 續傳驗證 14.950002；索引探索 7.136596；明細收集 2.435876；Parquet 證明寫入 0.027302 |

兩者明確的原生邊界都是 `through_parquet_proof_before_final_state_write`，因此
不包含最後狀態寫入，不能當成採集 E2E；階段包含於總時鐘，不能相加計算工作量。
現有 release 計數也沒有冒充本次處理分母，未產生 rows/sec 或新來源綁定。
這次只驗證保留的 metadata，沒有重新驗證 raw receipts、Parquet payload、PIT、
發布完整性或當次 producer 實際載入的程式身分。

六個計時缺口完整保留：DGBAS 有 229.596 秒原始值，但缺少明確原生範圍，暫不
納入同一測量邊界；CBC 隔夜、CBC 年度匯率及三個 MOF 發布流程缺少原生耗時。
不從 UTC 時戳推算耗時，也不把 `complete` 狀態當成完整資料證明。

下一個效能診斷針對貨幣公告的 canonical `_verified_resume_rows`：現行程式已將
archive root resolve 移到迴圈外，且已依 resolved path 去重內容 SHA，因此先測
剩下的路徑解析、檔案驗證與 listing 解析成本，再選擇修改。外匯存底的官方每頁
60 筆設定也已實作，不重做既有優化。診斷需等目前固定輸入比較與 CEX 全量驗收
結束，避免同時讀檔干擾測速；沿用 canonical resume 證明，不啟動 provider。

- [原始 24 份輸入與 SHA 清單](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round94/protocol.json)
- [748 項逐項對照、9 個時鐘與六個缺口](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round94/analysis.json)
- [獨立 metadata 讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round94/readback.json)
- [完成收據與觀測成本](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round94/completion.json)

目前仍未完成全部來源與完整原生採集流程的測速，也未證明硬體極限。

## R95：來源別名造成鎖路徑缺檔，保留延期並由 R100 修正診斷入口

04:08 UTC 已由同一 task 的 canonical owner 啟動等待程序，明確等待 R92
`completion.json` 驗收成功，且實際 systemd inactive／MainPID 0，才開始讀取
原始資料。R92 已通過，但 R95 於 04:57:42 UTC 以
`deferred_source_lease_unavailable`／`FileNotFoundError` 延期，沒有執行資料剖析。
owner 雖為 succeeded／exit 0，這是有收據的 deferral，不能算測量通過。

這次沿用 R94 的 bounded capture helper，新增實際原始檔案驗證，不呼叫
`collect()`。取得已存在的 TW-public 與 CBC money 共享讀鎖後，使用 canonical
`read_release_resume_state` 選擇當次有效續傳證明，保留 current／companion 的
原始 bytes，拒絕從鬆散檔案或 `complete` 字樣合成證明。鎖忙或不存在保留
deferral，不能解讀成測量成功。

五份原生模組逐檔凍結並直接從核對過的 source bytes 編譯載入；驗證函式本體
保持原樣。原始資料以單檔 16 MiB、總量 256 MiB、2,048 個 lexical paths
為上限，執行前 admission 保留 D 32 GiB＋512 MiB、root 8 GiB 及 2 GiB
可用記憶體。超限或來源世代變動會拒絕，不縮減輸入後宣稱全量測完。

依序另起程序執行一次不帶 profiler 的完整驗證、一次 cProfile，以及一次
完整 SHA／世代與所有回傳列的讀回；各自保留 call wall、process CPU、整個
child wall／descendant CPU、完整函式統計。cProfile 時間包含追蹤成本，
累計函式時間會重疊；前置封存可能暖快取，因此這是歸因診斷，不是冷啟動、
受控 A/B、完整採集 E2E 或套件／整台機器的不可變證明。未修改下載、限流、
排程或續傳驗證語意。

R95 driver SHA `6a32a65cdf65c6a6ee69cc82436d8fe2aaf75377f0c1b0b17afd318804a70f87`。
先核對這一個 owner 的結果，再進行下一個重型比較或發布流程的完整回歸檢查，
避免新的 CPU／I/O 競爭。R89 SHA 候選已依八輪負結果拒絕，沒有待套用的 SHA 優化。

- [R95 唯讀診斷與既有 helper 重用](../artifacts/benchmarks/workflow_speed_optimization_20261021/round95/profile_cbc_resume.py)
- [R95 實際等待 owner](../artifacts/operations/agent-workflow/runs/workflow-r95-cbc-resume-after-20261024T040812-dc82f350/run.json)
- [R95 原延期收據](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round95/deferral.json)

實查 `ROOT/data_tw_public` 是指向 `/srv/stockagent-live/data_tw_public` 的 symlink。
R95 在解析 root 之前以 `DATA.parent` 尋找外層鎖，落到不存在的
`/root/stockAgent/.locks/tw-public-refresh.lock`；正式 release service 使用的
`/srv/stockagent-live/.locks/tw-public-refresh.lock` 及 dataset lock 都已存在。
沒有建立替代鎖，也沒有跳過鎖。

R100 保留 R95 原程式、deferral 及 supervisor 收據，從同一原程式編譯執行，
只修改 `BASE` 與 `DATA` 兩個 top-level assignments；`DATA` 先做
`resolve(strict=True)`，與正式 producer 使用同一物理 root。所有函數 AST、
五個 module pins、容量上限、現有共享鎖、續傳證明與新程序讀回均保留。
R100 在 R98 完整回歸實際 terminal 後啟動，於 05:00:18 UTC 再次延期；
這次是正確來源鎖的 `BlockingIOError`，與 R95 的缺檔不同。兩次都沒有
執行資料剖析，也沒有建立或繞過來源鎖。

R101 沿用 R100 的 root 修正與 R95 全部原始函數，另外保留 R100 deferral／
收據／程式，最多等待既有共享鎖 30 分鐘；取得兩把鎖後，持有至整個唯讀診斷
與獨立讀回結束。原 `observe()` 也仍取得同一共享鎖，各 worker 核對 source pins
與所有原始 SHA／世代。R101 已於 05:09:54 UTC 成功完成，實查
inactive／MainPID 0；完整封存 **455 個原始檔／25,360,344 bytes**，三次原生
驗證均回傳相同的 **643 列／322 個 unique releases／131 頁索引**。
完整 child 為 322.712808 秒 wall／89.848772 秒 descendant CPU，包含前置封存、
各子程序與讀回，不能當成單次函式耗時。

不帶 profiler 的原生續傳呼叫為 **17.500778 秒 wall／16.717475 秒 process CPU**；
cProfile 呼叫為 39.148668 秒 wall，包含 profiler 成本。其累計統計顯示
`_parse_listing_page` 131 次共 38.168448 秒，其中 BeautifulSoup 建樹累計
19.975250 秒、97,690 次 `find()` 累計 9.504249 秒。這些時間有包含與重疊關係，
不相加。這支持優先減少 HTML 重複走訪，而非略過 raw SHA、Parquet 或世代驗證。

- [R100 精確重用與 root 修正](../artifacts/benchmarks/workflow_speed_optimization_20261021/round100/profile_canonical_cbc_resume.py)
- [R100 正確 root 與兩次來源身分的保留證據](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round100/root_path_amendment.json)
- [R100 原忙碌鎖延期](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round100/deferral.json)
- [R101 有界共享鎖等待、保留來源與同一診斷](../artifacts/benchmarks/workflow_speed_optimization_20261021/round101/profile_after_source_lease.py)
- [R101 完整診斷驗收](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round101/completion.json)
- [R101 原生完整函式統計](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round101/profile_calls.json)

## R102／R103／R104：減少 CBC HTML 重複走訪，八輪通過並正式採用

R102 於取得現有共享鎖前遇到 `BlockingIOError`，在任何 trial 開始前 failed／exit 1。
保留原始程式、admission、失敗日誌與收據；這不是候選效能負結果。R103 直接
重用完整比較程式，只加入已驗證的有界共享鎖等待，沒有停止或改寫採集器。

修改只在 money `_parse_listing_page`：保留原 BeautifulSoup／`html.parser`、
`li` 與頁數選取、日期／標題／URL 驗證及例外行為；先建立每個被選中 `li`
的第一個 descendant `time` 索引，避免逐個列表反覆搜尋整棵子樹，沒有時間標籤
的項目也不再搜尋連結。巢狀 `li` 的原有重複列與 raw-row 計數仍保留。
`_verified_resume_rows` 本體及所有原檔 SHA／續傳世代／Parquet 證明不變。

R103 的 **ABBA／BAAB 共八輪**均在新的完整子程序執行原生續傳，每輪另外由
新程序比對 **643 列及 131 個完整、有順序的 ListingPage**，不是只核對數量。
前後重新核對所有 455 份封存及原檔；既有 41 項 money 回歸與另 11 組
舊解析器樹狀結構／例外 oracle 全部通過。兩半各自需通過整個命令與原生呼叫的
wall ≤ 0.98、CPU ≤ 1.02 門檻，結果如下。

| 順序半組 | 完整命令平均 wall：原版 → 候選（秒） | wall 減少 | descendant CPU 減少 | 原生呼叫 wall 減少 |
| --- | ---: | ---: | ---: | ---: |
| ABBA | 16.925488 → 13.524729 | 20.09% | 21.80% | 23.58% |
| BAAB | 19.205835 → 17.127446 | 10.82% | 19.11% | 20.92% |

R103 已成功結束，實查 inactive／MainPID 0，候選已正式套用。
money source SHA 為 `6a59eaf03b3f3da215071e8a08c09dcecee995ffefb5741cd9f8b367cb6a76f0`，
與實測候選完整檔案相同。舊 driver／舊 source pins 及全部負結果保持原樣，
不再用舊 cohort 的 strict-code 門檻重跑已改版程式。

R104 使用新 source capsule、同一對既有共享鎖與同一批原始檔完成正式驗收：
money／FX／release IO 三套 **72 passed、零 failure／error／skip**，完整命令
9.614037 秒 wall／8.048997 秒 descendant CPU。兩次完整原生續傳及各自的新
程序讀回均匹配 R103 的全部列與列表欄位，來源 bytes 與世代再次一致。
owner 於 **05:37:22 UTC succeeded／exit 0**，實查 inactive／MainPID 0。

R104 不帶 profiler 的呼叫為 11.579173 秒 wall／11.271157 秒 process CPU，
這是採用後的驗收觀測，沒有拿它與另一輪相減宣稱新加速。整個 owner 為
233.612254 秒 wall，包含 **120.084376 秒來源鎖等待**。後續 cProfile 顯示
建樹累計 17.334020 秒、CSS 選取累計 6.465748 秒；用於決定下一個優化軸，
不能直接換算為未量測的加速率。

本輪加速範圍為固定輸入的完整唯讀 CBC 續傳命令，前置封存可能暖快取，
不包含 HTTP 採集及一次性 golden 準備。沒有宣稱完整 CBC 採集、全部工作流
加速或硬體極限。

- [R103 完整八輪計時、門檻及全部輸出判定](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round103/comparison.json)
- [R103 固定輸入與 source pins](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round103/protocol.json)
- [正式 money 解析器](../downloader/download_tw_cbc_money_release_archive.py)
- [巢狀列表、第一日期與錯誤行為回歸](../test/test_tw_cbc_money_release_archive.py)
- [R104 採用後驗收與完整命令時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round104/completion.json)
- [R104 採用後完整函式統計](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round104/profile_calls.json)

## R108：原生 HTML 查詢候選未達雙順序門檻，不採用

在 R107 完整驗收及實際 owner 結束後，以正式 R104 capsule 的
455 份原始檔／25,360,344 bytes 比較另一個候選：只將三種 CSS 查詢改為
同一個 `html.parser` 樹的 `find_all`／class／selected-descendant 查詢。
其餘原生續傳、原檔 SHA、世代與完整輸出檢查保持不變。

46 項現有 money 測試及 23 組樹狀結構／例外 oracle 全部通過。
八輪各自的新程序讀回均匹配全部 643 列及 131 個有順序的完整
ListingPage，原始與封存 bytes 前後相符。但兩個平衡順序的結果不一致。

| 順序半組 | 完整命令平均 wall：正式版 → 候選（秒） | wall 比值 | descendant CPU 比值 | 原生呼叫 wall 比值 |
| --- | ---: | ---: | ---: | ---: |
| ABBA | 9.672199 → 10.454042 | 1.080834 | 1.056683 | 1.046213 |
| BAAB | 11.015885 → 9.068634 | 0.823232 | 0.819432 | 0.805190 |

ABBA 同時未通過 wall ≤ 0.98 與 CPU ≤ 1.02，故 `candidate_accepted=false`。
不取兩半平均來覆蓋失敗，也不重跑相同候選追求正結果。正式 money source
仍為 R103／R104 的 `6a59eaf03b3f3da215071e8a08c09dcecee995ffefb5741cd9f8b367cb6a76f0`。
R108 succeeded／exit 0，實查 inactive／MainPID 0；成功的是比較與拒絕判定。
下一步先分開量測三種原查詢，確認成本再選新的修改軸。

- [R108 固定輸入與完整比較範圍](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round108/protocol.json)
- [R108 八輪原始計時與拒絕判定](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round108/comparison.json)
- [R108 原測試與額外 oracle 結果](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round108/checks.json)

## R110／R111：三種原查詢的完整原生細項診斷

R110 在執行查詢前停止：driver 將狀態物件的排序序列化 bytes 與舊 capsule
的縮排 JSON bytes 直接比較，兩者格式不同；尚未產生速度樣本。
保留 R110 程式、admission、protocol、完整失敗 log／命令計時及 run 收據。
R111 只改用 R104 原有的完整 decoded-state 值比較並切換新輸出目錄，沒有
放寬原檔 SHA、世代、來源身分或 golden-output 的任何門檻。

R111 在同一對既有共享鎖下，前後重算 **455 份原檔及封存檔**，執行未修改的
正式 parser／native resume。每個原查詢包一個 monotonic／process-CPU span，
按 131 個 listing 的順序保留全部 **393 個 span**；仍返回原查詢物件。
新程序重新計算每組統計並核對全部 **643 列／131 份完整 ListingPage**。

| 原查詢 | 次數 | 累計 wall（秒） | 累計 process CPU（秒） | 單次 wall 中位數（毫秒） |
| --- | ---: | ---: | ---: | ---: |
| `select("li")` | 131 | 0.663738088 | 0.660119727 | 4.293015 |
| `select_one(".total")` | 131 | 0.900704502 | 0.893672708 | 5.618119 |
| `select_one("#PageSize option[selected]")` | 131 | 0.393722447 | 0.391041669 | 2.322395 |

帶量測的完整原生呼叫為 **12.072557474 秒 wall／12.035258679 秒 process CPU**；
完整測量 child 為 27.706608 秒／14.783899 秒，新程序讀回 1.794157 秒／
0.492969 秒。整個 owner 為 **64.515714 秒 wall**，包含 guard、原檔核驗與
封存成本。R111 succeeded／exit 0，實查 inactive／MainPID 0。

這是含量測開銷的成本分解，與 R108 的未帶量測 A/B 不混算。
`li` 查詢約占這次原生呼叫的 5.5%；安裝套件對簡單名稱有直接遍歷分支，
因此 R112 只比較 `select("li")` → `find_all("li")`，保留另外兩種 CSS 查詢。
是否採用仍取決於同一完整原生流程的八輪雙順序門檻，不由細項成本推定加速。

- [R110 原失敗收據](../artifacts/operations/agent-workflow/runs/workflow-r110-cbc-query-spans-20261024T062715-c1de7924/run.json)
- [R111 固定來源及原失敗保留](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round111/protocol.json)
- [R111 全部原始查詢 span](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round111/query_spans.json)
- [R111 完整測量與分組值](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round111/measurement.json)
- [R111 獨立讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round111/readback.json)
- [R111 完成及完整子命令時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round111/completion.json)

## R112：只更換簡單 li 查詢仍未通過，不採用

R111 完整診斷後，再以新候選只將 `select("li")` 改為 `find_all("li")`，
兩種頁數 CSS、HTML builder 及其餘 parser／native resume 不變。
沿用原完整八輪比較與全部門檻；46 項現有測試、23 組額外 oracle、每輪
643 列／131 個完整列表及新程序讀回全部通過，455 份原始及封存檔前後一致。

| 順序半組 | 完整命令平均 wall：正式版 → 候選（秒） | wall 比值 | descendant CPU 比值 | 原生呼叫 wall 比值 |
| --- | ---: | ---: | ---: | ---: |
| ABBA | 8.072130 → 6.640853 | 0.822689 | 0.819785 | 0.824855 |
| BAAB | 9.645564 → 10.094174 | 1.046509 | 1.013483 | 0.982311 |

BAAB 的完整命令 wall 與原生呼叫 wall 均未達原 ≤ 0.98 門檻，因此
`candidate_accepted=false`。這不是對簡單查詢的普遍速度結論；結果不足以
採用於這個完整工作流。正式 source 仍為 R103／R104 的 `6a59eaf…a76f0`，
不套用 R108 或 R112，也不放寬門檻或平均掉失敗。
R112 succeeded／exit 0，實查 inactive／MainPID 0；整個比較 owner 為
126.355403 秒 wall／78.403882 秒 descendant CPU，包含完整原檔核驗與控制成本。

- [R112 單一修改軸與完整固定範圍](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round112/protocol.json)
- [R112 八輪全部計時及拒絕判定](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round112/comparison.json)
- [R112 原測試與額外 oracle](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round112/checks.json)
- [R112 原 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r112-cbc-simple-li-20261024T063333-35a5b105/run.json)

## R96／R97：自然排程補到兩個原有來源計時缺口

保留 **2026-10-23 21:16:34 至 2026-10-24 04:13:16 UTC** 八個既有 service 的
完整 journal 窗口，共 **824 entries／800,413 bytes**；未達 4,097 entries 或
32 MiB 上限。查詢為 **0.837262 秒 wall／0.345622 秒 descendant CPU**，沒有
啟動 provider、GPU 工作或重啟正式服務。另封存 26 份程式／既有證據輸入，
共 4,467,655 bytes。原生 parser、journal collector 與來源綁定函式的 AST，
均與 R32 已驗證版本相同；目前 profile SHA 保持 `80b3304d…`。

此窗口有 **22 個新操作、四個資料集**，與 R32 已保留的 operation identity
沒有重疊。每筆 producer PID、boot、invocation、規格 SHA、原生時鐘 ABI、
狀態與來源定位均經正式 parser 核對：

| 原生資料集 | 次數 | 原生狀態 | call wall 秒 |
| --- | ---: | --- | ---: |
| twse_api_holidayschedule_holidayschedule | 12 | ok | 0.436880–0.711454 |
| twse_api_exchangereport_twt84u | 8 | ok | 2.365514–5.248576 |
| taifex_daily_options | 1 | ok | 2.179811 |
| taifex_large_trader_futures_oi | 1 | ok | 0.854112 |

其中 **TWT84U** 與**期交所大額交易人期貨未平倉**在 R87 基準清冊中原本沒有
來源專屬時鐘；這次確認已取得有效的原生呼叫觀測。另兩項是新的操作樣本，
不重複計算成新的來源 ID。四項皆仍不含 executor 排隊、計時日誌、父程序報表
或後續 audit；沒有完整處理單位分母，保留 work 空值與 units/sec 未知。
`ok` 也不代表歷史完整、PIT、發布或整個 producer 的載入身分已通過驗收。

R96 首次獨立 oracle 漏做 `bind_source_stages`，預期列缺少 `source_endpoint_id`，
導致完整 JSON multiset 比對拒絕；原 failed／exit 1、原始輸出及日誌全部保留。
R97 只在原 oracle 的比較前，加入全部舊來源 ID 與**同一 canonical 綁定函式**，
完整 JSON equality、數值、context、locator、outcome 與 multiplicity 比對不變。
沒有修改 R96 已執行的程式、收據、輸出或重跑 collection。

R97 的另一個新程序已驗證 **六份輸出／三份 unique originals／22 samples／
22 groups／全部 2,399 個來源身分鏈**，以及從原始事件重建的完整列集合。
R97 owner succeeded／exit 0，實查 inactive／MainPID 0；整個命令為
**8.957837 秒 wall／2.437974 秒 descendant CPU**，其中獨立讀回
**7.673780 秒 wall／1.617000 秒 descendant CPU**。R96 失敗命令另保留
16.340587 秒 wall，不能稱作成功 collection 耗時，也不從差額補造未持久化的
細分時鐘。

這是兩個原有缺口的 delta 驗證，當時尚未重算 R87 的 **2,399／1,651／748**
全量結果。R92 已執行的範圍保持不變；後續 R107 已明確加入本次完整 journal，
保留所有既有來源與計時，通過新的完整 13 階段驗收，結果為
**2,399／1,653／746**，詳見末節。後續 R116 再完整驗收七個 issuer 的離線解析
綁定，當時為 **2,399／1,660／739**，詳見
[R116 全部原 oracle 與新綁定](workflow_speed_optimization_2026-10-24_crypto_feature_clocks.md#r116完整-13-階段與七個-issuer-的精確綁定)。
R141 完整驗收再加入 Dune／SEC 三個綁定，目前為
**2,399／1,663／736**，見同報告的 R141 接續驗收。
這仍不是所有流程、服務更新或硬體極限證明。

- [R96 完整原生日誌、事件與來源狀態](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round96/observation.json)
- [R96 六份 delta 輸出的 SHA 清單](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round96/native_delta/collection.json)
- [R96 原失敗收據](../artifacts/operations/agent-workflow/runs/workflow-r96-natural-clock-delta-20261024T041705-5accc6ec/run.json)
- [R97 oracle 修正與原收據 SHA](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round97/amendment.json)
- [R97 完整原始事件列集合與 canonical reader 讀回](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round97/readback.json)
- [R97 成功完成收據](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round97/completion.json)

## 六個發布流程的共用計時入口與 R98 檢查

已在既有 CBC 隔夜／年度匯率、MOF 發布日期／貿易發布值／原始發布檔案及
DGBAS 原始發布版本的六個 canonical `collect()` 加上共用 decorator。
沿用既有 `OperationTiming`；六份來源各僅增加 helper imports 與 decorator，
移除這些新增 AST nodes 後，完整 AST 與 R94 封存的舊來源一致。R98 檢查時
CBC money／FX 的驗證實作未改，R95 五個模組 pins 相符；後續 R103／R104
另外量測並採用了前節的 money 解析器優化。

新契約為 `tw_public_release_archive_operation_timing_v1`，stderr 前綴為
`[tw-public-release:operation-timing] `。計時涵蓋原 `collect()` 全部本體，
**包括最後 state 寫出**；不含 CLI admission、外層鎖等待、記錄本身與
CLI 後續 exception handler。原始回傳物件、例外／中斷與 raw／Parquet／state
檔案不變；計時或日誌失敗保留原結果與觀測缺口，不重做採集。

每列包含 dataset／endpoint ID、原 producer filename／function、時鐘定義
SHA、PID／thread、returned／raised、原生 status 和 monotonic clock。
只有原 canonical owner、matching returned dataset、合法時鐘及非 unsupported
status 才宣告可供綁定。work denominator 保留空值，不記 URL、根路徑、原始資料
或例外訊息，不將計時解讀成發布時間、loaded-code 或歷史完整性證明。

R98 於 04:39 UTC 由 canonical owner 啟動，在 R89 實際結束後已通過
**27 項 focused tests，零 failure／error／skip**：
**4.805335 秒 wall／3.310489 秒 descendant CPU**。涵蓋六個入口、signed zero／
原檔 bytes／原回傳身分、正常與 failed／interrupted、計時初始化／snapshot／
寫出／flush 故障、非法回傳／倒退時鐘及 32 個並行呼叫的完整 JSON lines。
source pins 與六份 business AST 再核對一致。這是本機工程驗證，沒有新 API／GPU 工作。

R98 接著等待 R95 terminal 加實際 inactive／MainPID 0，已完成既有 release
archive、provisional macro、TLS 及 refresh 等七套完整回歸：**80 passed，零
failure／error／skip，7.065323 秒 wall／6.618668 秒 descendant CPU**；這 80 項
已包含前述 27 項，不能相加當成 107 個不同案例。全部 pins／六份 body AST
再核對一致，owner 於 04:58:10 UTC succeeded／exit 0，實查 inactive／MainPID 0。
新契約已接入既有 profile journal owner，R105 工程驗收通過，詳見下節；
以 `producer_collect_total` 保留與 dispatch 不同的時鐘邊界。**尚未核對自然
排程的實際原生發出事件**，六個舊缺口沒有因此自動關閉。

- [共用計時入口](../downloader/release_archive_timing.py)
- [27 項語意案例](../test/test_release_archive_timing.py)
- [R98 封存、AST oracle 與順序回歸程序](../artifacts/benchmarks/workflow_speed_optimization_20261021/round98/check_release_clocks.py)
- [R98 source pins 與契約](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round98/protocol.json)
- [R98 focused 結果與計時](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round98/focused_checks.json)
- [R98 七套 80 項完整回歸與計時](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round98/release_regression_checks.json)
- [R98 最終工程驗收](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round98/completion.json)
- [R98 實際 owner 收據](../artifacts/operations/agent-workflow/runs/workflow-r98-release-clock-check-20261024T043908-149d5de0/run.json)

## R105／R106／R107：發布時鐘共用讀取與完整範圍再驗收

canonical profile `7ef8cd81…` 現在透過原有 journal ingestion owner 解讀
release collect ABI，共用原始 journal 封存、重複操作去重、衝突保存及來源綁定。
每筆需核對 PID／thread、boot／invocation、原 producer filename／function、
定義 SHA、時鐘範圍及 observation flags；身分不符但有效的時鐘保留為未綁定，
非法時鐘保留原始證據與 gap。原有 dispatch parser 的業務行為與全部既有測試不變。

新增階段 `producer_collect_total` 及 `native_release_archive_collect` input identity，
不混入原 `producer_call_total`／`native_dataset_dispatch`。新事件的 journal
inventory 明列 ingestion v2，報告另列支援的新契約；完整 collector source
evidence 加入共用 helper 與六個原 producer，仍只證明 observed source，
不聲稱服務載入程式已驗證。工作量分母仍為空值。

R105 已通過 **366 tests、零 failure／error／skip**，其中新增的 release
日誌案例為 **49 項**，同時重用 27 項入口案例與全部既有共用回歸。
完整測試命令為 **24.758003 秒 wall／17.805116 秒 descendant CPU**。
另外以原始 producer 格式測試六個 owner、MESSAGE bytes、身分不符、失敗狀態、
非法時鐘、觀測邊界、重複／衝突及不同計時粒度，沒有以這些 fixture 增加真實來源覆蓋。

以 R92 原 profile 的 source bytes 編譯另一個 reader，重讀三份既有 journal
及 R96 原始 journal，共 **43 個完整 dispatch rows**；全列 multiset、context、
locator、outcome、重複次數、完整 inventory 與 summary 均一致。CEX 原始 oracle
的 **14,366 列／953 workers** 也完整相等。兩個獨立命令分別為
**5.823034 秒 wall／1.783398 秒 descendant CPU**（TW），及
**25.097278 秒 wall／6.781228 秒 descendant CPU**（CEX）。
R105 succeeded／exit 0，實查 inactive／MainPID 0。

R106 在封存完整依賴時發現漏列 R90 歷史 driver，於任何 collection 開始前
failed／exit 1；**2.520905 秒 wall** 是失敗準備成本，沒有量測完整流程。
R107 保留其程式、部分 source capsule、admission、日誌與失敗收據，精確補回
同一 R90 driver／原 SHA `91925271…`，沒有刪除或放寬依賴。

R107 已啟動新的完整 13 階段驗收，保留全部六個 training roots、239 份 explicit
receipts、兩組原生 CEX report／summary 與三份舊 journal，另外明確加入 R96
完整原始 journal。保留原版三 journal／21 次操作 oracle，再以只改 cardinality
3 → 4 的同一 oracle 驗證四份 journal／43 次操作，全部欄位與 equality 規則不變。
完整 reader、14 份輸出、獨立物理重建、R45／R73 training clocks 與 CEX 原始列
oracle 已全部通過。R107 於 **06:13:08 UTC** 成功完成，實查
inactive／MainPID 0，完整輸出含 **6,577 份 unique originals／292,014 samples／
44,384 groups／13 phases**。所有舊 **2,399 個 ID／1,651 個綁定**均保留，
加入 R96 的真實事件後為 **1,653 個 stage-bound／746 個未綁定**。

| 完整命令或階段 | wall 秒 | CPU 秒及範圍 |
| --- | ---: | ---: |
| collector | 664.060591 | descendant 456.017403 |
| canonical 獨立讀回 | 197.276866 | descendant 40.637383 |
| 物理重建供舊 reader 讀取 | 250.729990 | process 28.934107 |
| 舊 reader 及原始 TW／training oracles | 391.861826 | descendant 93.605570 |
| 額外 CEX 完整原始列 oracle | 120.929384 | descendant 15.776134 |

collector 加全部讀回／重建／oracles 的完整邊界為 **1,627.150389 秒 wall**，
包含中間控制成本，不能只相加表中 CPU 或用 wall 減 CPU 當成磁碟時間。
R45 的全部 226 samples、R73 的全部 6,655 samples／712 JSONL records、三份
原 journal 的 21 次操作及四份合計 43 次操作均逐欄保留；兩組 CEX 的全部
14,366 列也與原 oracle 相等。四個 FinMind latest-success 缺少一致 transport
clock 的 issues 繼續保留，沒有補零或改寫成成功時鐘。

這是新 scope／新 snapshot 的完整工程驗收，沒有以 R92 的不同快照耗時推算
受控加速。六個 release collect 入口仍待自然排程的實際事件；沒有新增 provider／
GPU 工作或重啟服務，也沒有宣稱所有流程已測完或達到硬體極限。

- [49 項新增日誌案例與既有案例](../test/test_tw_public_journal_ingestion.py)
- [R105 全部工程檢查及原始 oracle 驗收](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round105/release_completion.json)
- [R105 完整 TW 舊 reader 對照](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round105/tw_original_readback.json)
- [R106 原失敗收據](../artifacts/operations/agent-workflow/runs/workflow-r106-full-release-reade-20261024T054357-4e7369d9/run.json)
- [R107 新完整範圍與失敗依賴保留](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round107/protocol.json)
- [R107 實際 owner](../artifacts/operations/agent-workflow/runs/workflow-r107-full-release-retai-20261024T054534-ac40bd78/run.json)
- [R107 完整 13 階段與舊來源／時鐘 oracle 驗收](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round107/full_acceptance.json)
- [R107 包含 CEX 額外 oracle 的最終驗收](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round107/completion.json)
