# 新聞啟動：鉅亨歷史範圍覆蓋索引

本輪只續辦初始化成本。前輪已完成的 CMoneyLearning 28 篇、報新聞 15 篇及
431 筆已結案診斷沿用既存正文與收據；鉅亨維持優先，CNA 暫緩。

## 先拆分原來的 190 秒

[原 owner 的時鐘與階段快照](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/planned_scope_cover_20261024/before.json)
記錄同一個 owner `1d4cb10a4793471f883a612f5482b018`：

| 邊界 | wall 秒 | 範圍 |
| --- | ---: | --- |
| owner 開始至首次已發布快照 | 190.200 | 完整初始化觀察，含後續維護 |
| `owner_initialization` | 108.787 | queue 建構與恢復 |
| `acceptance_recovery` | 107.481 | 核對 221,532 份既有 JSON 收據 |
| 首輪 `discovery_seed_maintenance` | 74.974 | 全部設定來源的排程範圍維護 |
| `cnyes_planned_scope_read` | 74.424 | 65 個鉅亨端點的範圍重建合計 |

這些是有巢狀關係的階段，不能相加成總時間。190.200 秒不能全歸因於收據 I/O。
階段 CPU 使用呼叫執行緒時鐘；receipt reader 的 process CPU 包含讀取執行緒，
也不能把兩個口徑混為一談。此前較慢的 4 KiB 分段、重用 buffer 及 pool 方案維持拒絕，未重做。

## 根因與實作

原 `planned_intervals()` 用 URL 主鍵尋找歷史分頁，仍需逐筆讀取資料表才能檢查
`kind='cnyes_news'`。原生計畫為 `sqlite_autoindex_discovery_1` range search；
大量分散的表頁讀取是可省掉的物理工作。

[範圍模組](../downloader/news_fulltext_cnyes_seed.py) 定義 partial covering index：
`discovery(kind,url) WHERE kind='cnyes_news'`。
[原 queue 初始化](../downloader/news_fulltext_queue.py) 使用 `IF NOT EXISTS` 安裝，
另記 `cnyes_planned_scope_index_initialization`。範圍 SQL、URL、歷史起點、
已拒絕／已拆分父頁、重複 query 參數的既有解析、priority、attempts 及 retry 時鐘均保留。
沒有增加下載 owner、來源端點或更改 provider 配額／body fingerprint。

## 同一快照的完整維護比較

[原生 AB／BA 結果](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/planned_scope_cover_20261024/native-abba.json)
包含 88 個已設定 provider、376,817 筆 discovery。
沿用[共用比較工具](../scripts/benchmark_news_discovery_maintenance.py)，新增
`--index-axis cnyes_planned_scope`；只有 TEMP discovery／settings／counters 可寫，
正式 WAL 快照唯讀，沒有 publisher 請求。

| 完整 `seed_discovery` 的中位數 | 原 URL 主鍵 | 覆蓋索引 |
| --- | ---: | ---: |
| wall 秒 | 2.727045 | 1.368288 |
| process CPU 秒 | 2.635107 | 1.315199 |

四次完整 discovery／queue_settings／counters 結果 SHA 全部為
`afd809a7ba15755fd441c6f5d4ccb5671181c80ca219353177532187c69a5cce`。
本機 resident 維護約 **1.993 倍**，wall 減少約 **49.8%**；兩次 candidate 均快於兩次 reference。
DISTINCT 的既有暫存 B-tree 保留，沒有以少查歷史範圍換速度。

TEMP 索引配置 49,917,952 bytes，建置最多 1.718 秒；建置時間不包含在維護中位數。
[完整 benchmark 命令](../artifacts/operations/agent-workflow/runs/news-planned-scope-native-abba-20261024T145634-f1e3ff55/run.json)
耗時 118.584 秒，包含快照準備、建索引、完整雜湊及四次維護；準備本身 22.455 秒。
這不是正式主檔首次建索引的時間、HTTP 提速、冷開機或所有資料抓完的證明。

## 驗證與接續

[323 項共用回歸](../artifacts/operations/agent-workflow/runs/news-planned-scope-shared-tests-20261024T145943-bcbe27f7/run.json)
通過，涵蓋原生範圍、完整維護比對、拒絕／archived／特殊 query、索引重建、
queue 收據恢復、派送、增量排程、生命週期與進度投影。測試前的 16 項聚焦結果有重疊，
不相加為另一個總數。

[正式服務與兩端面板驗收](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/planned_scope_cover_20261024/ready.json)
於 10/24 23:04 Taipei 通過：原服務新 owner
`3c1b2ede664b4c35b1a2fd78e7b490c9`／PID 429787，NRestarts 為 0。
65 個端點全部實際使用覆蓋索引，主檔索引 49,917,952 bytes；首次建置階段 4.272 秒，
另有 commit／其他初始化，不能用 TEMP 的 1.718 秒代替。
原正文 43 篇的 head／payload SHA、431 筆結案及交易／共用面板服務全部保持；
恢復 guard 已停止，正式 service／timer 繼續原排程。

| 新 owner 階段 | wall 秒 |
| --- | ---: |
| 收據恢復：221,541 份 | 70.991 |
| queue 初始化，含索引／恢復 | 81.006 |
| owner 開始至首次已發布快照 | 81.416 |
| 首輪全部來源範圍維護 | 1.563 |
| 其中 65 個鉅亨範圍讀取合計 | 1.286 |

本次首份快照與後續首輪維護是不同邊界，驗收另等維護完成才判定恢復。
兩次 owner 的收據數、快取、其他系統負載及快照發布位置不同，
不能把 190.200／81.416 或 74.424／1.286 當作配對加速倍率。
可靠的因果比較仍是上述同快照 AB／BA；本次未重開機或清除全機 page cache。

[接續清單](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/planned_scope_cover_20261024/handoff.json)
已將索引比較／部署移出待辦。鉅亨缺文繼續依原 retry／來源時鐘排程，
有新原始身分證據才處理舊路徑；先前已完成的 legacy probe 不再重做。
等待期間只續辦新正文證據及仍約 71 秒的收據恢復成本。完整 JSON 核對維持；
報新聞仍有 4 筆結構、56 筆署名／範圍未解，不重送已結案問題。
本輪沒有新增正文或重抓已完成正文；鉅亨全歷史、穩定最高來源速率及全球完成 ETA 尚未證實。

## 10/25 只續未完成：清點與收據讀取試驗

[完成項目讀回](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/unfinished_handoff_20261025.json)
確認同一正式 owner、43 篇既有完成正文、439 筆結案問題、來源身分及交易服務不變，
本機與公開面板均 fresh／ready；這輪 0 次新聞來源請求、0 次 queue 寫入、0 次服務重啟。
新清點只讀取收據與目前索引，沒有把已分類的空原生頁或媒體當成新文字重播。

為續辦 221,601 份收據的啟動成本，新增一項有界、未部署的實驗：
同一組跨歷史 8,192 份已提交收據，原本的完整恢復流程與兩個 reader 不變，
比較既有讀取與目錄 descriptor 相對開檔；API 語意參考 [Python 官方文件](https://docs.python.org/3.12/library/os.html#os.open)。
每種條件執行兩輪 AB／BA，完整 JSON 所有欄位及原件 SHA 一致，正式 SQLite 寫入與來源請求均為 0。

| 條件 | 原讀取 wall／CPU 秒 | 候選 wall／CPU 秒 | 決定 |
| --- | ---: | ---: | --- |
| [暖快取](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/receipt_directory_descriptor_screen_20261025/warm.json) | 0.584113／0.662879 | 0.582209／0.637287 | wall 僅約 0.3% 差異，不足以啟用 |
| [逐收據 page advice](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/receipt_directory_descriptor_screen_20261025/advice.json) | 4.458368／1.904675 | 5.071270／2.149295 | wall 約慢 14%、CPU 約多 13%，不採用 |

page advice 僅對指定新聞收據提出提示，核心可忽略；不是全機 cache 清除或真正冷開機。
此實驗僅包含合法已提交收據，也尚未驗證目錄替換與全部錯誤語意，不能視為可部署替代品。
保留負面結果，不重做已拒絕的 4 KiB、重用 buffer 或常態 pool 試驗，也沒有為此重啟正式服務。

[完整清點與規劃](../artifacts/operations/agent-workflow/runs/news-unfinished-current-scope-20261024T164536-5622ef1d/run.json)
另觀察到 232.080 秒的命令成本。原清點的兩個全資料聚合都使用 `dispatch_latest` 再回讀表頁；
這是獨立於收據恢復與來源 HTTP 的待辦。只比較同一 WAL 快照的完整結果，保留所有 provider／host、
日期範圍、缺文／實存計數和錯誤分類，不以 transactional counters 代替獨立的全資料核對。

[完整 AB／BA](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/article_inventory_plan_benchmark_20261025.json)
已完成：同一個 read-only WAL 快照、6,345,133 筆文章 head，兩個聚合的每一欄、NULL、排序與結果 SHA 全相同。
原 `dispatch_latest` 掃描加表頁回讀，改成兩個聚合各自的直接表掃描；沒有移除排程索引或改動選文查詢。
`NOT INDEXED` 的約束參考 [SQLite 官方文件](https://www.sqlite.org/lang_indexedby.html)，本機效能依配對實測判斷。

| 完整兩聚合，中位 | 原計畫 | 順序表掃描 |
| --- | ---: | ---: |
| wall 秒 | 44.335472 | 38.016339 |
| process CPU 秒 | 39.542823 | 33.459793 |

本次採用 wall 約減 14.3%、CPU 約減 15.4% 的方案；這是清點聚合成本，沒有跳過任何 provider 或原始資料列。
[清點器](../scripts/audit_news_providers.py) 的新輸出帶 `full_frontier_scan_contract_version=1`
與 `inventory_implementation_sha256`，新清單綁定該程式及完整輸入 SHA，不覆寫前一份清點。

[目前程式的實機驗收](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/article_inventory_scan_acceptance_20261025/accepted.json)
確認兩個 native query plan 均為 `SCAN articles`；完整清點本次 wall 47.285 秒、CPU 41.901 秒，
包住規劃、保留證據及網頁讀回的受監督命令為 54.951 秒。
前次 232.080 秒與本次有不同快取及命令邊界，**不以兩者比值宣稱整鏈路加速**；
可靠的因果比較是上表同快照 AB／BA。142 項來源清點、端點、優先順序、metadata、finite cohort 與授權 API 回歸通過。

正式 collector 程式與 owner 保持原樣，43 篇舊正文、439 筆結案、來源契約和交易程序不變，
本機與公開面板均 fresh／ready；沒有為這個唯讀工具變更重啟下載服務。
原服務兩次清點間另新增的兩篇 CMNews 正文，已用既有選文 verifier 核對原件／正文／批次收據與目前解析器，
沒有重抓或重算成舊文重查。鉅亨 187 個活動缺文仍待原站規則端點恢復，報新聞 4／56 筆證據缺口、
其他來源與收據恢復成本仍未完成；此次全資料聚合比較及已拒絕的 descriptor 試驗移出待辦，CNA 暫緩。
