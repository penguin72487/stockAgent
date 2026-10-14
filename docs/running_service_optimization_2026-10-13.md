# 本機執行服務效能清點與最佳化

目前目標仍在進行；已驗收的結果與尚待正式載入的部分分開列出：

- 五個台股模式的收盤完整快取，最新程式再次回讀通過：零模型前推、
  CUDA 未初始化。Discord 盤前常駐窗口改從預設 08:05 開始，正式冷 owner
  與 08:08 的自然暖機均已接受；成功暖機耗時 109.097 秒，早於 08:15。
  真正開盤端到端延遲仍待實際驗證。
- 自然暖機的三次股票尾端讀取合計 82.511 秒，是目前準備熱點。完整
  全市場對照已否決較小讀取門檻（牆鐘反增 27.3%、CPU 增 30.1%）；
  有界原始 Arrow 尾端共用的完整四輪對照通過：workflow 牆鐘少 27.3%、
  CPU 少 35.1%，peak RSS 增 4.6%；尚待 canonical 整合與正式載入。
- 永豐完整本機稽核的六程序對照：牆鐘少 20.2%、CPU 秒少 19.4%、
  峰值 RSS 少 35.1%，全待辦／三摘要／寫檔一致。選出的容量已成預設，
  未替換 memo 的完整正式函式回讀亦通過；不是 API 全批下載倍率。
- 主機清冊為 87 個已安裝服務、67 timers、3 paths；07:51、暖機前的
  79 個可讀 StockAgent Linux 程序均無 GPU 裝置 handle。這不歸屬所有 Windows
  顯存，也不證明所有服務已達最優。FinMind 新索引部署及原正式歷史失敗
  仍是未完成項目。

<details>
<summary>前序部署／來源世代／逐階段驗收紀錄</summary>

盤後 GPU 階段已完成目前五模式正式快取與零 CUDA 回讀，詳見
[盤後推論快取驗收](completed_session_inference_cache_2026-10-13.md)。
10 月 14 日 01:44 的再驗收正確拒用舊程式世代的快取；01:59 核對為
`stockagent/config.py` 新增訓練 optimizer 設定，並非已證實的行情資料缺口。
03:15 已用目前五個正式鍵、15 次獨立完整訊號讀取重新驗收，與原先獨立
控制的完整 SHA256 一致，CUDA 未初始化、GPU 配置量為零。
03:27 只重載 Discord 與唯讀網頁入口；03:32 再驗收通過，交易與行情
捕捉程序未重載。這不代表原有兩個正式歷史工作失敗已解決。
04:10 期貨唯讀展示已載入經完整對照的四執行緒與局部 allocator 回收設定；
04:12–04:13 主機取樣中該程序 RSS 約 171 MiB，原交易與行情捕捉 owner 不變。
04:15 台股五模式再次通過目前正式鍵、獨立完整輸出與零 CUDA 驗收。
04:39 只重載網站唯讀入口，04:47 四種當前資料分頁／搜尋／換代的 HTTP
內容逐 byte 與獨立原始解析一致；原交易、行情捕捉及下載器 owner 不變。
04:54 最後再次驗證五個目前正式鍵及獨立完整控制：零 model forward、
零 provider call、CUDA 未初始化，沒有交易寫入。
05:34 僅重載唯讀網站入口，日期目錄預熱已正式載入；05:35 完整歷史
341,952 點及最新單日 2,192 點的 HTTP bytes 均與重載前一致。
05:36–05:37 正式入口 RSS 約 769 MiB，詳見下方日期目錄階段；
不把有界流程的 36.4% RSS 降幅當作全機或整個 gateway 的降幅。
05:22 隔日當沖資格表更新，使 04:54 的摘要來源世代不再適用；
05:37 起五模式完整重播仍全數命中原 forward cache，CUDA 禁用且
完整訊號與獨立控制一致。來源世代與純模型輸入快取是兩個不同契約。
05:39 canonical cache-only 維護器在模型／CUDA／行情 API 禁用條件下
更新五模式摘要；05:41 再次正式讀回均命中原 forward key、零 GPU，
最新來源世代已接受，未啟動兩個失敗的完整歷史工作。
05:56 原生監聽器已補上兩個 canonical 資格檔；05:58 工程通知真實
喚醒既有 cache-only owner，五模式摘要／weights／forward key 均不變。
06:00 正常三來源 watch set 與停止後維護器讀回通過，沒有新增輪詢 daemon。
06:07 起對永豐歷史回補做非阻塞取樣；已定位每批三次全庫摘要內的
重複收據遍歷，單次遍歷修正與 256 項共享回歸通過。06:51 完整真實
清冊 ABBA 通過所有待辦與摘要／寫檔對照；中位牆鐘減少 48.7%、CPU
減少 36.9%，但首輪明顯冷讀、RSS 中位增加約 24%，不能當整體最佳化。
06:57 起同清冊的隔離快取容量 ABBA，完成後因牆鐘／CPU 變慢而未採用。
07:15 原歷史 owner 正常批次完成，正式單次遍歷載入已接受，未重啟下載器。
07:19 僅重載 Discord，修正早於準備窗口長時間常駐 GPU；新 owner 五模式
快取及零落後同步通過，無 GPU device handle。詳細原生前後讀回在下方。
07:33 完成新的主機取樣：87 個已安裝服務與一個工程 transient 分開列出；
Discord 匿名記憶體約 519 MiB、CPU 約 0.049 core，未混成全機最佳化。
完整永豐 audit profile 亦已通過原始全輸出 oracle；後續 owned-FD 雜湊
修正通過 80 項測試，正做獨立舊程式的完整平衡對照，尚不宣稱加速。
以下 FinMind／OpenBB 及 21 時 52 分主機數據保留原觀察時點，
不當成後續重載的現況；全服務目標仍待逐項 profile 與驗收。
10 月 14 日續作的 common OHLCV reader 有界完整對照與 Bybit 路徑診斷，
見 [分鐘行情讀取最佳化](ohlcv_bounded_read_optimization_2026-10-14.md)。
10 月 14 日 00:47 起，原全資料面板排程已載入專用清冊編碼器；
兩輪完整清冊重播降幅 15.3%／29.2%，300 項共享回歸與正式 reader 相容性通過，
詳見 [清冊編碼最佳化](data_monitor_record_encoding_optimization_2026-10-14.md)。
這是內部 metadata 編碼改善，不是整個狀態服務或全機的相同倍率。
10 月 14 日 01:34 的 FinMind 完整狀態查詢對照亦通過；牆鐘減少 45.3%、
CPU 減少 46.5%。正式 owner 尚未載入：連續歷史批次沒有合格空閒窗口，
本次安全重載觀察已明確延後，沒有強行中斷下載。細節見下方新階段。

FinMind 每分鐘預估的重複清冊掃描已移除，正式取樣程序已載入新版。
OpenBB 改為保留來源標記的字典編碼並分批驗證，完整抽樣檔案驗證通過；
已開始的舊版全庫驗證未被中斷，在 10 月 14 日 00:22 自行結束但未通過
歸屬檢查；下一次程序才載入新版。詳見下方全量 audit 結果。
行情擷取、交易帳本、checkpoint、特徵、精度及配額上限均未修改。

</details>

這是本機服務普查與已定位熱點的有界最佳化，不是所有服務、所有資料及遠端
訓練已達理論最快的宣告。效能、資料完整性與服務健康各自驗收。

目前收盤快取的版本驗收與載入證據：

- [目前五模式完整輸出與零 GPU 對照](../artifacts/operations/running-services-optimization-20261013/postclose-current-code-full-output-acceptance.json)
- [只重載兩個讀取服務及保留交易／捕捉身分](../artifacts/operations/agent-workflow/runs/current-cache-readonly-consumer--20261013T192732-168c570d/run.log)
- [重載後五模式正式回讀](../artifacts/operations/running-services-optimization-20261013/postclose-after-consumer-adoption-20261014.json)
- [03:33 全服務取樣](../artifacts/operations/running-services-optimization-20261013/census-after-readonly-adoption-20261014.json)
- [目前公開模式／訊號頁／原交易程序回讀](../artifacts/operations/running-services-optimization-20261013/postclose-current-public-owner-readback-20261014.json)

## 已驗收的改善

下列為每版三個獨立程序的中位數，交錯執行。主機上仍有其他服務；
不清空 OS page cache，不把本次倍率當跨日期或跨硬體保證。

| 完整受測操作 | 原版 | 新版 | 改善 |
| --- | ---: | ---: | ---: |
| FinMind 完整 ETA 更新牆鐘 | 29.746 秒 | 12.590 秒 | 減少 57.7% |
| FinMind 完整 ETA 更新 CPU | 28.466 秒 | 12.405 秒 | 減少 56.4% |
| FinMind 程序峰值 RSS | 163.76 MB | 163.50 MB | 大致不變 |
| OpenBB 40 檔完整檔案驗證牆鐘 | 0.969 秒 | 0.284 秒 | 減少 70.7% |
| OpenBB 同一驗證 CPU | 1.505 秒 | 0.395 秒 | 減少 73.8% |
| OpenBB 程序峰值 RSS | 376.21 MB | 224.64 MB | 減少 40.3% |

CPU 秒包含工作執行緒，與牆鐘秒不同。MB 採十進位；RSS 取 worker 自身
`/proc/self/status` 的 VmHWM，不採 fork 繼承的父程序 rusage。

FinMind 使用唯讀 online backup 的固定佇列／流量快照，保留 106 個資料集，
完整 workload、telemetry、secondary admission 及所有階段 ETA 的 SHA256
六次完全一致。各 owner 分別凍結，不宣稱跨 owner 原子發布。
OpenBB 使用 10,000 個成功候選中的 32 個前段樣本與 8 個大表，合計
40 檔、20,312,780 bytes、1,921,111 筆。這是刻意包含解碼重表的抽樣，
不是全庫平均分布，也不是全庫下載吞吐量。兩版沿用同一個 canonical
`_audit_success_files`，所有語意結果一致；只排除原有的兩個 stopwatch 欄位。
兩項 benchmark 均沒有 provider 呼叫、正式佇列寫入或原始來源修改。

收據：

- [FinMind 最終完整對照](../artifacts/benchmarks/running_services_20261013/finmind_eta_final.json)
- [OpenBB 最終完整抽樣對照](../artifacts/benchmarks/running_services_20261013/openbb_audit_final.json)
- [初次主機基線](../artifacts/operations/running-services-optimization-20261013/baseline.json)
- [包含主機服務的最後取樣](../artifacts/operations/running-services-optimization-20261013/final.json)
- [程式身分與正式取樣驗收](../artifacts/operations/running-services-optimization-20261013/acceptance.json)

## 計算成本與根因

端到端時間包含來源允許的等待、網路、解碼、驗證、落盤及發布。吞吐量受
配額、網路、處理與儲存中最慢的環節限制；多開執行緒不等於增加帳號配額。
本次先移除重複處理，再量測，不跳過驗證、不縮減模型輸入、不換近似公式。

FinMind 的初次 cProfile 中，完整 ETA 約 35.6 秒，
`build_finmind_workload` 被呼叫兩次：ETA 先掃清冊，次要校驗准入又重新掃一次。
telemetry 還重新計算 queue state counts 與已接受分區的 renewal counts。
profile 含量測額外成本，沒有混入上述未 profile 的比較。

現在同一次更新共用唯讀交易取得的暫存聚合。聚合只能用於完全相同的
佇列路徑與 UTC 觀察時點；缺表、讀取錯誤、時點／owner 不匹配保持原本的
unknown 或 fail closed。實際下載 dispatch 仍重查即時工作與共用配額，
沒有把 ETA 快照當成下載許可。

有限優先工作查詢從掃全部 tasks 加上逐筆 EXISTS，改為優先名單外層搭配
task 主鍵查找。DISTINCT 保留舊名單重複 key 時的 EXISTS 語意。
這沿用 SQLite 的明確 join 順序，沒有新增高成本寬索引，也沒有強制所有
GROUP BY 改走 NOT INDEXED；後者的實測沒有改善，未採用。
[SQLite query planner](https://www.sqlite.org/optoverview.html#manual_control_of_query_plans_using_cross_join)

OpenBB 需要核對第一筆來源標記，原版卻先把整個首 row group 的五個重複字串
欄位展開。現在先做原有 footer／schema 檢查，對大於 8,192 列的首 group
重用 footer 開啟 dictionary reader，以 65,536 列 batch 消費完整首 group。
所有原有 requested pages 都解碼，後段壞頁仍會失敗；不是只讀一筆略過驗證。
小檔不重開 reader；缺欄位仍分類為 metadata mismatch，而不是提前被 Arrow
誤分類成 unreadable。零列、encoded wrapper、擁有權、筆數與來源比對都保留。
[Apache Arrow ParquetFile](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html)

## 所有運行服務的清點

最後一次 20 秒級取樣於台北 21:52 完成：86 個已安裝 StockAgent services、
2 個工程 transient services、67 個 timers、3 個 paths。
已安裝服務中有 18 個 running daemon、4 個 activating 工作。
主機 system manager 合計記錄 60 個 active／activating service units，
其中 39 個 running／activating，其餘包含 active exited 的初始化工作。
兩個主機 CPU 計數無法形成有效 delta，保持未量測，不當成零。
基線是 85 個已安裝服務及 66 個 timers；期間其他工作新增了服務，兩份清冊均保留。

CPU cores 是取樣 CPU 秒除以取樣牆鐘，1 代表一個 logical core 忙碌，
不是全主機 100%。以下為最後取樣的主要本機服務，括號內以 core 表示；
完整每個 unit 的 PID、狀態、匿名記憶體、file cache、I/O、歷史收據與 timer
皆在上方最後取樣 JSON。inactive 與 failed 也保留，不會從清冊消失。

| 服務或責任 | 取樣結果與處置 |
| --- | --- |
| OpenBB archive | 0.911 core；正在收尾的舊版完整檔案驗證，非空轉輪詢；新版不強制中斷現有 audit |
| FinMind complement | 0.291 core；抓取與處理仍運行；共用帳號、日曆、release reserve、工作順序不變 |
| FinMind sponsor | 0.0002 core；沒有據此提高官方配額或判定已抓全 |
| FinMind free | 0.0001 core；等待／idle heartbeat 與來源完成證明保持分開 |
| FinMind quota snapshot | 每分鐘短工作；新版正式 health 已成功產生，實際更新約 12～18 秒隨主機負載變動 |
| Public dashboards | 0.021 core；資料頁與狀態投影可讀，HTTP 200 不等於 data health 合格 |
| Discord bot | 0.050 core，匿名記憶體約 2.53 GiB；重用今日已驗收的五模式暖機，不再重啟 consumer |
| TW day trade simulation | 0.0033 core；保留原 engine 與帳本，沒有改成交或會計語意 |
| TW overnight simulation | 0.0005 core；保留交易／特徵時鐘 |
| Shioaji TAIFEX bidask | 0.131 core；夜盤擷取受保護，未重啟或更改 broker session |
| Shioaji TAIFEX dashboard | 0.011 core；file cache 明顯多於匿名記憶體，不據此宣告記憶體洩漏 |
| Shioaji historical market data | 輕量 waiting wrapper；沒有據此宣告歷史回補已完成 |
| Shioaji minute backfill | 輕量 waiting wrapper；不額外登入、不製造分鐘值 |
| Shioaji top200 | 輕量 waiting wrapper；保留 canonical owner |
| Shioaji TX history backfill | 輕量 waiting wrapper；保留配額與既有排程 |
| TW public source events | 0.028 core；保留來源事件及發布時鐘 |
| TEJ history | 0.039 core；Windows Smart Wizard 的 UI／授權／輸出流程不以多執行緒強行平行化 |
| Storage lifecycle | 0.116 core；不刪資料、不變更 authority、lease、冷庫或發布流程 |
| Temporal | 0.022 core；等待與 durable orchestration，不當成無效 CPU 熱點 |
| Backup stream | activating，0.300 core；實際傳輸／備份成本，不以停止備份換取效能數字 |
| Legacy partition return | activating，0.513 core；保留回傳工作及恢復邊界 |
| Packed transport | activating；此取樣沒有有效 CPU delta，未量測而非零成本 |
| Data refresh status snapshot | activating；此取樣沒有有效 CPU delta，匿名記憶體約 900 MiB，仍需完整工作量 profile |
| News fulltext | 基線 0.126 core，最後取樣 inactive；新聞正有其他並行修改，未覆寫或歸為本次節省 |
| Syncthing | 0.198 core；記錄資源，不改同步或回收策略 |
| PostgreSQL | 0.0039 core；沒有清空 cache、VACUUM 或重啟資料庫 |
| OS 常駐服務 | systemd、DNS、cron、journald、chrony、snapd 等逐項記錄；沒有足以支持調整的熱點 |

GPU 三次取樣為 6～11%，已用記憶體約 2.8～3.1 GiB；是瞬時主機數字，
不是推論耗時，也無法據此把所有 Windows／GPU 工作歸屬某個模型。
主機最後 CPU PSI some avg10 約 3%、I/O some 約 3.42%；
不是全 CPU 或 GPU 全天滿載的證據。OpenBB 最後 cgroup file cache 約
15.93 GiB、匿名記憶體約 130.5 MiB，兩者不能都當成 Python RSS。
最後 OpenBB archive 記憶體 high／OOM 事件取樣增量均為零。
另有匹配到的 L1 compaction 歷史收據記錄 3,353 次 memory high 事件，
這是該次批次的壓力證據，不是當前 archive daemon 的 OOM。

## 推論與上線界線

重用今日既有的完整 live inference 驗收，而非另改模型：第五模式的熱推論
中位數為 0.571 秒，完整輸入與輸出 SHA 相等；五模式正式 consumer 暖機
10.925 秒、5／5 ready。這不是本次新做的加速，也不證明下一次開盤的
取價、下單或成交延遲。[完整推論驗收](tw_day_trade_live_inference_optimization_2026-10-13.md)

FinMind quota timer 仍每分鐘取樣，使用 official account 的動態上限；
21:57 的 SponsorPro 收據為 20,000 requests/hour。本次沒有改流量上限、
固定追新 reserve、失敗重試政策或歷史優先序，沒有額外消耗 provider 額度。
OpenBB 的現有 audit 不 hot reload；後續 canonical supervisor 的獨立 audit
會讀新版。不得拿抽樣通過冒充正式全庫驗證完成。

最後服務清冊仍有 boot recovery audit、Discord artifact maintenance、
registered intraday、Vast bulk return 四個 failed units；intraday 的匹配收據
指出 Bybit perpetuals failed step。六個本機公共狀態頁雖然可讀，仍有
blocked／degraded／starting 狀態。這些健康或資料缺口不因效能測試而消失。
正式 OpenBB 全庫收據、盤後維護恢復及下次真實交易時段的端到端驗收
仍是後續事項；本次沒有把它們改成 complete。

## 10 月 14 日全量 audit 結果

00:22:10，原先執行中的 OpenBB archive 自行退出，systemd 為 failed／exit 2，
不是本次終止或重啟。其 [唯讀收據核對](../artifacts/operations/agent-workflow/runs/openbb-terminal-audit-readonly-20261013T163114-14a44841/run.log)
保留全量 audit JSON 的 SHA256、寫入時點與原 invocation。
JSON 的 `checked_at` 為 10 月 13 日 20:48:34 的開始取樣時點，不能當成完成時點；
檔案於 10 月 14 日 00:22:09 發布，owner 隨後退出。

成功工作的 7,326,790 個檔案、334,814,433 筆的既有受測範圍中，缺檔、
不可讀、manifest 筆數不符、來源 metadata 不符皆為零。這不是所有 payload
欄位／所有 row groups 的逐值驗證。catalog follow-up audit 亦通過，
13,148 個 parent 檔對應 1,126,953 個必要 unique follow-ups，缺漏為零。

真正未通過的是來源檔案歸屬：共掃描 7,473,976 個 Parquet，
146,575 個 inactive manifest-success 歷史檔正確保留；另有 611 個檔案沒有
合格的 active／inactive-success owner，或屬 foreign-active 範圍。
目前 samples 沒有對這 611 個逐一提供細分類，不能直接說是毀損、沒有價值，
也不能改成忽略而宣稱 audit 通過。下一階段沿原 owner 核對 task／來源 metadata／
回收與保留契約，保留原檔，不重新呼叫 API 或建立空資料替代。

完整舊版 ownership index 建置花 1,031.03 秒，掃描 691.32 秒；這是另外的
全量瓶頸，不由先前 40 檔 metadata benchmark 證明已最佳化。
沒有啟動下一次幾小時級的全庫重跑、compaction，亦未清除 failed 狀態。

最新 [00:22 服務普查](../artifacts/operations/running-services-optimization-20261013/postclose-current-census-20261014.json)
仍涵蓋 86 個已安裝服務、67 個 timers、3 個 paths；OpenBB 已退出，不能再列為
目前正在消耗 CPU 的 audit。此取樣的主要活動為 registered intraday、
storage lifecycle、FinMind complement、歷史 partition return 及 backup stream；
仍需各自端到端 profile 與同輸入驗收，不以停止必要工作換取 idle 數字。

## 10 月 14 日 FinMind resident owner 狀態查詢

20 秒的 live stack 取樣把主要主執行緒活動定位到 canonical `_status`，
不是單純多開網路 worker 就能消除的成本。約 316 萬個 task 的聚合原先
使用 `(dataset,state)` 窄索引，再回查 table 取得 rows／bytes／日期。
採用涵蓋這七個聚合欄位的 covering index，減少 retained-history table 查找；
沒有改工作順序、交易日曆、release reserve、來源請求範圍或官方帳號配額。
依據 [SQLite covering index](https://www.sqlite.org/queryplanner.html#covering_indexes)。

固定唯讀 online backup 的 3,162,288 列，兩份 byte-identical 起點，凍結同一
觀察時間及 symbol 名單，交錯 ABBA 共 12 次。受測操作是完整 `_status`、
dispatch／history frontier／retry policy 與原子 JSON 發布，不只是 SQL microbenchmark。
所有狀態輸出的語意 SHA256 相同，沒有 provider 呼叫或正式佇列寫入。

| 同輸入完整操作 | 原版中位數 | 新版中位數 | 改善 |
| --- | ---: | ---: | ---: |
| 狀態查詢牆鐘 | 2.639 秒 | 1.444 秒 | 減少 45.3% |
| 狀態查詢 CPU | 2.623 秒 | 1.403 秒 | 減少 46.5% |

新增索引約 289,116,160 bytes（275.7 MiB），不刪原有索引。每輪也實測
64 個實際 task 的 canonical claim／release、128 次 WAL／full-sync transactions；
還原 task 內容並保留原有 unrelated in-flight claim。寫入牆鐘沒有觀察到劣化，
但不宣稱寫入 CPU、RAM、API 吞吐或整台電腦都有相同改善。

兩個負例保留：新增索引後未更新 bounded planner statistics 的完整操作變成
約 26 秒，強制 table scan 亦變慢，均未採用。新索引另曾搶走 fresh queue 的
dispatch plan；修正為保留既有 pending／due-refresh partial-index seeks，
不是移除失敗測試。最終 381 項受影響共享回歸通過。

部署仍待正式 owner 載入。01:40 起的 300 秒觀察沒有取得 coherent waiting、
零 active task／in-flight claim 及足夠 deadline 的批次邊界，收據為
`deferred_no_safe_idle_window`、`restart_requested=false`。FinMind 原 invocation
不變，交易與 TAIFEX 夜盤 owner 也沒有重啟。不要把 offline 測試通過說成
正式下載器已提速，也不要對連續忙碌程序重複盲目輪詢或清除 claim。

收據：

- [完整 ABBA、plan、寫入與程式世代對照](../artifacts/benchmarks/running_services_20261013/finmind_status_dispatch_final_20261014.json)
- [live CPU 取樣](../artifacts/benchmarks/running_services_20261013/finmind_live_cpu_20261014.json)
- [381 項回歸](../artifacts/operations/agent-workflow/runs/finmind-status-all-shared-contra-20261013T174041-1d214787/run.log)
- [安全重載延後](../artifacts/operations/running-services-optimization-20261013/finmind-status-index-rollout.json)
- [盤後拒用原因唯讀核對](../artifacts/operations/agent-workflow/runs/postclose-source-identity-diff-20261013T175922-9c48462f/run.log)

重現使用 `run_fintech_python -m scripts.benchmark_finmind_status --help`，指定新
output；不要直接修改 live queue 的 schema。正式重載沿
`scripts/reload_finmind_at_idle.py` 的接受收據／loaded-code／claim／deadline
檢查，沒有合格窗口就保留 deferred，不能把啟動成功當成部署驗收。

## 10 月 14 日 FinLab dispatch 恢復

02:01 的新普查有 88 個服務、67 個 timers、3 個 paths。數量含新工程 transient
及其他並行工作，不當作本次新增常駐服務數。FinLab quota 02:02 的官方收據尚有
1,839.262 MB，實際下載 unit 卻未啟動；其 relative timer 的 next_elapse 為 0，
下一個固定觸發要等 07:00。原 `OnUnitInactiveSec=1min` 缺少本次 boot 的
第一次 deactivation，不能靠上次開機留下的 calendar stamp 自行開始 relative retry。
本機唯讀 admission 也確認 `allowed=true / workload_unverified_recovery`，
不是已證實沒有工作或官方額度用完。

新增 `OnActiveSec=1min` 為 timer 啟動種子，保留所有原有固定時刻、完成後一分鐘
重試、開盤保護、實際官方 quota 與 sync lock。只有一個 canonical service owner，
已有 active 工作時 timer 不會另啟動一個並行 owner。
[systemd 官方 timer 定義](https://github.com/systemd/systemd/blob/main/man/systemd.timer.xml)

沿現有 installer 新增 `finlab-timer-only` 模式，只驗證、部署及重啟該 timer，
不覆蓋 FinLab worker／quota observer 或其他 dirty service template。
120 項相關測試通過，包含實際 installer 在隔離命令替身下的只改 timer 證明。
既有 boot audit 曾把 HEAD news timer 的合法 `OnBootSec` 排除；依官方定義
補入 OnBoot／OnStartup 獨立 anchor，沒有刪除或跳過原失敗測試。

02:13:10 原 timer 自動觸發，兩個原 ExecCondition 均通過，下載 owner 開始
執行。後續 real SDK 收據有 downloaded 更新且 provider_errors=0，pending
從 185、177、169、161 降至 153；這是 02:21 前的觀察，不是所有歷史已抓全，
也不是新增非商業授權、PIT 或冷庫發布權限的判定。當沖及 TAIFEX 夜盤的
原 invocation 未變。沒有以關掉必要抓取換取低占用，也沒有放寬額度或優先序。

收據：

- [02:01 全服務取樣](../artifacts/operations/running-services-optimization-20261013/current-census-20261014T0204.json)
- [本機 admission](../artifacts/operations/agent-workflow/runs/finlab-idle-eligibility-readonly-20261013T180334-d6d7196d/run.log)
- [120 項排程回歸](../artifacts/operations/agent-workflow/runs/finlab-timer-activation-contract-20261013T181009-c8584ef0/run.log)
- [只部署 FinLab timer](../artifacts/operations/agent-workflow/runs/finlab-dispatch-clock-only-deplo-20261013T181134-ae238937/run.log)

TAIFEX 展示在這次 20 秒取樣曾接近 0.97 core，但不是整小時持續滿核的證明。
普通 Python profiler 20 秒只有六個活動 samples；native 60 秒亦只有六個並有
81 個 sampling errors，不能用那些樣本百分比當作 CPU 秒或準確歸因。另有
約 4.0 GB 的 live marks 與 0.97 GB 的有效 replay；後續沿原完整曲線 projector
完成欄式 cache 的同輸入對照與唯讀展示 owner 重載，見下一節。

## 10 月 14 日 TAIFEX 唯讀曲線欄式快取

以原始帳本可驗證前綴及有效 replay 的私人副本，約 4.99 GB，執行完整
1d 曲線操作。原始／候選各四個獨立程序交錯 ABBA；最終程式世代的所有
金融欄位、排序、曲線、來源時計及 counts 的完整 JSON SHA256 一致。
新增完整 schema 的 Float64／Int64／nullable Parquet **派生投影**，不改帳本、
計算公式、價格／成交、資料單位或 quantization。這不是原始來源全集已壓縮成
35 MB：只有此既有 projector 消費的欄位，原始約 5 GB 全部保留。

| 完整 1d 曲線，最終 ABBA 中位數 | 原始 JSON reader | 欄式 reader |
| --- | ---: | ---: |
| CPU 秒，包含 native 工作執行緒 | 21.051 | 4.462 |
| fresh worker 峰值 RSS，十進位 MB | 4,250.42 | 163.28 |
| 牆鐘秒 | 4.026 | 4.115 |

CPU 約減少 78.8%、峰值 RSS 約減少 96.2%；牆鐘 **沒有改善**，小幅增加且
分布重疊，不能宣稱等待更快或全機等比例提速。首次建置另外需 10.39 秒，
CPU 29.08 秒／峰值 RSS 約 4.24 GB；真實捕捉尾端 256 KB 追加另外需
7.83 秒／CPU 10.53 秒。這些成本沒有省略，35.02 MB 的私有投影由原來源
核對後重建或更新。正式首次預建在獨立工程 process 完成，不把它保留在 server。

每個來源 generation 綁 dev／inode／size／mtime／ctime 與完整 SHA256；
來源改變先核對全部舊前綴，不假定只能 append。增量僅解析真實 tail，
舊內容被改寫則重新建立；payload 也驗證完整 SHA／schema。用 kernel flock
與 pinned FD 橫跨整次查詢、atomic／durable 發布；壞快取或 FIFO 回到原讀取器，
壞原始值保留原 parser failure，不報成功空集合。相同 process／generation 的
已驗證 source 與 payload proof 可重用，不每次掃描約 5 GB 的 hash。
底層沿 [Polars streaming Parquet sink](https://docs.pola.rs/api/python/stable/reference/api/polars.LazyFrame.sink_parquet.html)
及既有 canonical projector，沒有加裝 GPU 資料框架來消耗遊戲顯卡。

249 項共享回歸通過，另有 21 項 cache／runtime 證明及最後 14 項 runtime／
原嚴格時計測試（互有重疊，不加總為不同測試）。只有 canonical 唯讀 display
launcher 預設啟用，可用 `STOCKAGENT_TAIFEX_HISTORY_COLUMN_CACHE=0` 回原
reader。未修改 service unit／優先序、capture、provider 配額或 source publication。
02:37 僅重載展示 owner，invocation 改為 `841df5066b934760a2ef6a7b7ac1b4f0`；
原夜盤 BidAsk 與 TW 當沖 invocation 全程未變。後續收據確認新 owner 真的
更新私有 Parquet receipt，七個 API 區間的整體時計也已追上 source。

**整個策略資料就緒仍未通過。** 原 all-strategy 時計檢查保留並失敗於
`long_strip_1c2p` 的舊端點。02:30、部署前的原始 reader 固定副本已有同一
缺口，lag 約 1,926 秒；完整結果 SHA 仍等於原 ABBA 控制，證明不是新投影
丟掉資料。live strategy 仍 `alive=true`，目前新估值是 null，不當成已終止
策略或補造成交。新 runtime receipt 的 `deployed_physical_cache_verified`
只表示已部署且快取機制讀回，`api.strict_data_ready=false` 明列所有區間的
嚴格失敗原因，不能拿窄證明 supersede 原資料缺口。

02:42 的 30 秒主機取樣，展示服務為約 0.20 core，較 02:01 的 20 秒取樣
0.97 core 低；不同時段的工作量不相同，且另有並行工程與 FinLab 抓取，
不是受控全機倍率驗收。GPU telemetry 約 3–4% 是整卡即時值，不能證明
Windows 遊戲 FPS、某 process 的推論狀態或 CUDA 可用性。

收據：

- [最終完整來源 ABBA／冷建／真實追加／程式 fence](../artifacts/benchmarks/running_services_20261013/taifex_history_columns_final_balanced_20261014.json)
- [249 項共享回歸](../artifacts/operations/agent-workflow/runs/taifex-column-cache-shared-contr-20261013T183000-7244c6c7/run.log)
- [正式 cache 隔離預建](../artifacts/operations/running-services-optimization-20261013/taifex-column-cache-live-prewarm.json)
- [只重載唯讀展示](../artifacts/operations/agent-workflow/runs/taifex-display-cache-only-reload-20261013T183704-449500e6/run.log)
- [正式快取／所有區間／未變交易 owner 讀回](../artifacts/operations/running-services-optimization-20261013/taifex-physical-cache-data-boundary-readback.json)
- [原嚴格資料就緒失敗，未掩蓋](../artifacts/operations/running-services-optimization-20261013/taifex-column-cache-formal-readback.json)
- [部署前原始 reader 缺口獨立證明](../artifacts/operations/running-services-optimization-20261013/taifex-original-raw-gap-diagnostic.json)
- [後續全服務取樣](../artifacts/operations/running-services-optimization-20261013/current-census-after-taifex-cache-20261014.json)

### 02:49 追加競態與目前載入版本

前一版已降低重複解碼，但正式 source 在每分鐘分批追加時，generation proof
可能碰到正在寫入的帳本。不能把這當穩定 cache hit；立即退回原始 reader
又會產生多 GB 的瞬間工作集。現在只對明確的 source generation race 最多
重觀察三次，每次仍做完整 prefix／inode 核對；超過上限才沿原 reader。
未增加睡眠、永久錯誤重試或未驗證 prefix 的旁路。

最終 bounded-race 程式版另跑 238 項共享回歸與四加四 ABBA：完整輸出一致。
原始 CPU 21.658 秒／候選 5.420 秒，約減少 75.0%；峰值 RSS 4,255.70 MB
至 164.72 MB，約減少 96.1%。牆鐘 5.222 至 5.242 秒，仍無改善。
冷建 8.30 秒／CPU 27.73 秒，真實尾端追加 5.59 秒／CPU 9.05 秒，皆另列。
這才是目前 helper 程式 fence，前一版測量保留歷史，不改寫成新版結果。

另外在同一個常駐 process 對不變 source 各跑三次完整 1d builder：欄式版
第一次約 4.77 秒核對 source，其後約 0.241／0.245 秒、CPU 0.465／0.400 秒；
原 reader 第二／三次約 3.70／4.29 秒。所有完整輸出 SHA 一致。這是額外
resident reuse 檢查、不是交錯 ABBA 或追加工作量，也不宣稱整個 API 同倍率。

02:55 最新版在隔離 worker 重讀既有正式投影，無須再解碼全帳本；峰值 RSS
160.79 MB。02:56 再次只重載展示 owner；目前 invocation
`083d117663d8410db7d6d0f2d2eaee56`，兩個受保護交易／capture owner 仍未變。
最新版 readback 已確認 Parquet receipt 由新 owner 推進，原策略估值缺口
仍明列 `strict_data_ready=false`，沒有修改、跳過原完整資料就緒檢查。

- [目前 bounded-race 完整對照](../artifacts/benchmarks/running_services_20261013/taifex_history_columns_race_final_balanced_20261014.json)
- [238 項最新共享回歸](../artifacts/operations/agent-workflow/runs/taifex-column-bounded-writer-rac-20261013T184932-2f6b6082/run.log)
- [原 reader resident 完整曲線](../artifacts/benchmarks/running_services_20261013/taifex_resident_baseline_20261014.json)
- [欄式 resident 完整曲線](../artifacts/benchmarks/running_services_20261013/taifex_resident_candidate_20261014.json)
- [最新版本預讀](../artifacts/operations/running-services-optimization-20261013/taifex-column-race-live-prewarm.json)
- [最新版本正式讀回](../artifacts/operations/running-services-optimization-20261013/taifex-race-cache-final-runtime-readback.json)

### 七種區間、候選演算法與展示執行緒

03:38 起，對同一 SHA-verified 真實凍結來源，完整建立 `1h/1d/1w/1mo/1q/1y/all`
七種曲線，保留所有財務數值、時鐘、極值、重複時戳及 null／舊格式處理。
分組端點再 join 的候選雖降峰值記憶體，CPU 反增約 20%；改用 struct 集合
過濾的候選也沒有整體收益。兩者只保留為離線實驗，沒有正式啟用。
正式 query 的視窗選點規則原封不動，抽為可測試的純函式。

展示原本讓 Polars 使用本機預設 16 條執行緒。已核對
[官方 thread pool 說明](https://docs.pola.rs/api/python/version/1/reference/api/polars.thread_pool_size.html)：
此設定須在程序啟動前決定，官方不建議不經測試隨意覆寫。因此沒有改動
全域 downloader／訓練設定，而是實測 1／2／4／8／16，再交錯驗收這個角色。

第二輪 ABBA 沒有並行啟動本次 pytest；其他原服務仍在工作。每版四個新
程序，各完整建立七種區間、包含完整 JSON 對照編碼；共 56 個完整輸出，
每種區間 SHA256 都與未改寫的獨立完整控制一致，1d 也匹配先前原始 NDJSON
控制。沒有改變來源、重算模型、呼叫 broker 或 provider。

| 完整七區間操作中位數 | 原 16 執行緒 | 4 執行緒 |
| --- | ---: | ---: |
| 牆鐘秒 | 7.865 | 7.988 |
| CPU 秒 | 15.984 | 12.054 |
| worker 峰值 RSS，十進位 MB | 1,229.13 | 1,101.63 |

CPU 減少 24.6%、峰值 RSS 減少 10.4%；牆鐘增加約 1.6%，不宣稱更快。
較早一輪有牆鐘改善 8.6%，不能把它當最後一輪的速度結論。
這是同一硬體的完整展示工作流程，不是所有服務、所有 API 或整機 RAM
的相同降幅；未宣稱無欄式 cache 的第一次全帳本建置也更快。
218 項最新 reader／runtime／public 共享測試通過；原 strict 策略時鐘
validator 保持不變，部署與完整資料就緒仍各自驗收。

- [兩次 join 候選完整對照，未採用](../artifacts/benchmarks/running_services_20261013/taifex_seven_range_grouped_balanced_20261014.json)
- [struct 過濾候選完整對照，未採用](../artifacts/benchmarks/running_services_20261013/taifex_seven_range_predicate_balanced_20261014.json)
- [完整執行緒篩選，不是公平 ABBA 結論](../artifacts/benchmarks/running_services_20261013/taifex_seven_range_thread_screen_20261014.json)
- [四執行緒最終 ABBA 與 56 個完整輸出](../artifacts/benchmarks/running_services_20261013/taifex_seven_range_four_threads_final_balanced_20261014.json)
- [218 項目前 reader／runtime 共享回歸](../artifacts/operations/agent-workflow/runs/taifex-role-thread-runtime-regre-20261013T194958-351100c1/run.log)

03:54 的隔離預讀峰值 RSS 147.33 MB；03:54:39 之後只重載期貨展示
owner，invocation 為 `6521c3fc9c2b44618d4ca5f86178e797`。
已讀回實際 `POLARS_MAX_THREADS=4` 與 `polars-0` 到 `polars-3` 的執行緒；
03:55 七種 API 區間、實際 column receipt 推進及原交易／capture 身分
驗收通過。設定保留顯式使用者覆寫，只影響這個唯讀展示 launcher。
`strict_data_ready=false` 的來源估值缺口仍保留，沒有改變正常夜盤擷取。
台股五模式在此之後也再次通過零 GPU 正式快取讀回。

- [四執行緒正式預讀](../artifacts/operations/running-services-optimization-20261013/taifex-four-thread-live-prewarm-20261014.json)
- [只重載展示及保持原交易／capture owner](../artifacts/operations/agent-workflow/runs/taifex-readonly-four-thread-adop-20261013T195439-a648b4f7/run.log)
- [七區間與目前展示 owner 正式回讀](../artifacts/operations/running-services-optimization-20261013/taifex-four-thread-runtime-readback-20261014.json)
- [台股五模式再次零 GPU 回讀](../artifacts/operations/running-services-optimization-20261013/postclose-after-taifex-role-adoption-20261014.json)
- [03:59 一分鐘全服務取樣](../artifacts/operations/running-services-optimization-20261013/census-after-four-thread-adoption-20261014.json)

全卡 GPU 取樣由先前 3–4%／約 3.4 GiB 變為這輪 9%／約 4.85 GiB，
NVML compute-app 清單在 WSL 中仍為空；不能據此歸因某個程序，也不能把
五模式零 CUDA 的工程驗收說成整張卡或 Windows 遊戲已完全閒置。
這輪主機取樣含明列的隔離 allocator benchmark，不是無量測工作的整機基線。

FinMind safe reload 的直接 CLI 曾缺 repository import bootstrap；已修復並
在 repository 外、清除 PYTHONPATH 的程序測試。9 項 reload／status index
測試通過。最新只讀觀察仍無安全批次空閒窗口，沒有重啟目前 worker，
不能把 index 的離線改善當成正式下載器目前吞吐已改善。

- [FinMind 直接 CLI 與 index 回歸](../artifacts/operations/agent-workflow/runs/finmind-safe-reload-cli-contract-20261013T194645-ffff4c5e/run.log)
- [最新 FinMind 延後證據，沒有重啟](../artifacts/operations/running-services-optimization-20261013/finmind-current-idle-observation-20261014.json)

## 04:10 展示服務閒置記憶體回收

本機安裝的 Polars 1.38.1 初始化值仍為 `muzzy_decay_ms:-1`；
未使用頁面可以被保留，不能直接把較高 RSS 判定為資料洩漏。
[jemalloc 官方說明](https://jemalloc.net/jemalloc.3.html#opt.muzzy_decay_ms)定義
回收的是未使用頁面，而非目前仍需要的資料；
[Polars 目前官方原始碼](https://github.com/pola-rs/polars/blob/main/py-polars/src/polars/__init__.py)
已採 1,000 ms 的 muzzy 回收設定。本次沒有升級套件、全域改 allocator、
丟棄 OS page cache 或強制清除交易／下載器記憶體。

兩輪各八個新程序，固定四執行緒，依 ABBA 比較完整七區間、
十秒實際閒置及再次完整 1d 讀取；所有完整輸出與原獨立控制相同。
第二輪沒有並行跑本任務的測試或其他 benchmark，原正式服務照常執行：

| 同版本完整操作或資源 | 舊回收設定 | 局部 1,000 ms | 解讀 |
| --- | ---: | ---: | --- |
| 七區間牆鐘中位數 | 8.185 秒 | 7.815 秒 | 此輪少 4.5%，不宣稱普遍加速 |
| 七區間 CPU 中位數 | 12.115 秒 | 12.135 秒 | 約相同，增加 0.17% |
| 程序峰值 RSS | 1,054.7 MiB | 918.3 MiB | 少約 13% |
| 十秒閒置 RSS | 1,022.6 MiB | 155.0 MiB | 少約 85% |
| 十秒閒置 CPU | 0.016 秒 | 0.082 秒 | 回收有小幅非零成本 |
| 再次完整 1d 讀取 | 0.181 秒 | 0.179 秒 | 第二輪大致相同 |

第一輪再次讀取曾由 0.207 增到 0.267 秒；不能只選第二輪說暖讀必然更快。
接受條件保留完整輸出、資料與目前程式／安裝版本 hash，並限制全流程
退化與後續暖讀額外成本。啟動器只設定展示角色的 `_RJEM_MALLOC_CONF`，
保留明確 operator override；下載器、broker／paper owner 與訓練不受影響。

04:08:44 只重載展示，正式 owner 為 `de3573388d954aaeb797e3b7e731febe`。
第一次讀回尚未等到新來源 generation，因此原檢查拒絕宣稱新 column refresh；
等來源自然更新後 04:10 正式讀回七區間、實載四執行緒／allocator、
實際新 column receipt，原交易／capture invocation 均一致。
`long_strip_1c2p` 原來源估值缺口仍保留，`strict_data_ready=false` 沒有放寬。
117 項 cache／曲線／readback／零 GPU 相關回歸通過，與前述測試有重疊。

04:12–04:13 取樣中展示 process RSS 179,318,784 B，約 171 MiB；
cgroup MemoryCurrent 約 202 MiB，其中 anonymous 約 102 MiB、file 約 96 MiB。
這些指標不同，不把可回收 file cache 當成同量無法回收的程序 RAM。
主機 resource delta 清單有 61 個單元，其中 35 個為 active/running；
另有 activating 的單次更新／備份，不能把全部登錄服務都叫常駐程序。
全卡 GPU 此輪 6%→3%、4,990→3,463 MiB，屬不同時點的觀察，
不是 allocator 導致的 GPU 改善或 Windows 遊戲效能證明。

- [第一輪完整 allocator、閒置與暖讀對照](../artifacts/benchmarks/running_services_20261013/taifex_allocator_idle_balanced_20261014.json)
- [第二輪 ABBA 確認](../artifacts/benchmarks/running_services_20261013/taifex_allocator_idle_confirmation_20261014.json)
- [局部設定的隔離正式預讀](../artifacts/operations/running-services-optimization-20261013/taifex-allocator-live-prewarm-20261014.json)
- [新來源 generation 後七區間正式讀回](../artifacts/operations/running-services-optimization-20261013/taifex-allocator-runtime-after-generation-20261014.json)
- [部署後全服務一分鐘資源取樣](../artifacts/operations/running-services-optimization-20261013/census-after-allocator-adoption-20261014.json)
- [台股五模式再次匹配目前控制、零 CUDA](../artifacts/operations/running-services-optimization-20261013/postclose-after-allocator-current-controls-20261014.json)

## 全資料網站 feature index 長駐記憶體實驗

真實、SHA 固定的 55,797,215 B 清冊包含 88,865 欄；
每個新程序完整解碼、驗證、建索引、遍歷全部分頁、搜尋與換代重置，
1,130 個結果合計 83,794,398 B，八程序全輸出 SHA 一致。
不含網路、HTTP/gzip、producer 改寫或整個 gateway 的吞吐。

第一版共享每世代內相同的 immutable 標籤，保留原 JSON 數值解析器、
未知欄位、Unicode、負零、整數、NULL、所有行、搜尋和 revision 語意；
pool 每類最多 4,096 值，超過只不再共享，不截斷資料，也不全域 intern。
索引保留 RSS 約 183.1→128.4 MB，少約 30%，峰值少約 17%。
初步總耗時相近，但這版的乾淨第二輪總 CPU 約 1.605→1.788 秒，
增加約 11%；主要是新 generation 的建索引時間多約 0.18 秒。
沒有拿初輪時間當作新版本加速宣告，這個較慢版本沒有重載正式網站。

254 項分頁、側車、gateway、安全／唯讀與 dashboard 契約測試通過；
後續微調需要另留目前程式證據，不把舊測試或 prototype 當正式部署。

- [原 prototype 全分頁 ABBA](../artifacts/benchmarks/running_services_20261013/public_feature_residency_balanced_20261014.json)
- [整合版本、獨立 control 與完整輸出](../artifacts/benchmarks/running_services_20261013/public_feature_residency_production_balanced_20261014.json)

### 已正式載入的原生解碼版本

最後採用現有 msgspec 0.22.0 的 untyped metadata 解碼與有界標籤共享，
沒有使用會把整數轉浮點的型別轉換，也沒有換掉公開 JSON 編碼器。
缺少套件或不支援的合法舊格式走原 stdlib 備援；Infinity／NaN 仍拒用。
4,096 個浮點數含極小值、負零、極大值做 IEEE64 逐 bit 比對；
整數超過 2^53／2^64、Unicode、未知欄位與 pool 滿額後不丟列亦通過。
套件與原生 extension 本身的 SHA256 也納入當前 benchmark fence。
[官方 JSON 型別說明](https://msgspec.dev/supported-types)僅作介面依據，
實際相容性與效能仍以本次完整資料、測試及讀回為準。

| 完整清冊工作流程／資源中位數 | 原解析器 | 原生解碼與共享 |
| --- | ---: | ---: |
| 完整牆鐘 | 1.752 秒 | 1.750 秒 |
| 完整 CPU | 1.752 秒 | 1.733 秒 |
| worker 峰值 RSS，十進位 MB | 315.9 | 218.7 |
| 索引保留 RSS，十進位 MB | 184.4 | 160.3 |
| 建索引牆鐘 | 0.961 秒 | 0.789 秒 |
| 全部分頁／搜尋牆鐘 | 0.732 秒 | 0.884 秒 |

峰值 RAM 少約 31%、索引保留 RAM 少約 13%；全流程 CPU／時間相近。
建索引較快，但本輪分頁階段較慢，不宣稱每個子階段都變快；
完整 1,130 個結果仍與第一版獨立原始 control 的完整 SHA 一致。
上述為完整 feature-index 工作流程，不是整個 gateway、所有 HTTP 或全機降幅。
257 項目前分頁、側車、gateway、唯讀／安全與 dashboard 契約測試通過；
另 36 項目前 portable readback／完整來源與程式 fence／query 規則測試通過，
兩組有重疊，不相加當不同測試總量。

04:39:33 只重載 `stockagent-public-dashboards.service`，目前正式 owner
為 `5e76016bdd3f4ae49d3032bfb2cd74c6`／PID 1237926。
獨立控制使用當前 raw JSON 的原 stdlib 解析，不使用新 decoder 當 oracle。
原驗證腳本一度錯用內部 `search` 參數、且後續頁未帶 revision，回應 400；
保留網站原 `q`／revision 安全規則，增加 actual parser 的測試後再驗證。
04:47 當前 88,865 列的四種搜尋、分頁與換代重置 HTTP 內容逐 byte 一致，
交易、capture、FinMind、Discord、期貨展示五個 protected owner 均不變。
04:40 台股五模式正式鍵／獨立完整輸出也再次通過，零 model forward、零 CUDA。

04:41–04:42 的 gateway process RSS 943,792,128 B（約 900 MiB），
此時尚未完成 valid feature search 的新索引預熱；不能把整體高 RSS 歸因
feature index，也不能把角色重載後不同時點的數字當公平 A/B 因果證明。
全卡 GPU 仍約 4%／3.4 GiB，不是模型快取又在做 GPU 推論的證據。
下一步清點入口預熱、官方價格查詢及 native allocator 的長駐／閒置分布；
不先假定是 model、JSON、Arrow 或資料洩漏，也不對其他服務做全域回收。

- [最終原生解碼、完整 ABBA、原始 control 與安裝 extension hash](../artifacts/benchmarks/running_services_20261013/public_feature_native_residency_final_balanced_20261014.json)
- [當前原解析器 live oracle 預讀](../artifacts/operations/running-services-optimization-20261013/public-feature-live-oracle-prewarm-20261014.json)
- [實際新 owner 四種 HTTP 結果逐 byte 驗收](../artifacts/operations/running-services-optimization-20261013/public-feature-current-valid-http-readback-20261014.json)
- [入口重載後台股五模式零 GPU、目前完整控制](../artifacts/operations/running-services-optimization-20261013/postclose-after-native-gateway-adoption-20261014.json)
- [04:54 目前五模式完整控制與零 CUDA 最後讀回](../artifacts/operations/running-services-optimization-20261013/postclose-final-current-readback-20261014.json)
- [入口重載後全服務一分鐘取樣](../artifacts/operations/running-services-optimization-20261013/census-after-native-gateway-adoption-20261014.json)
- [36 項目前 readback 與 query 契約](../artifacts/operations/agent-workflow/runs/public-feature-real-wire-query-g-20261013T204705-d77a0e84/run.log)

## 日期目錄預熱：保留完整曲線，移除未使用的歷史常駐圖

正式 gateway 啟動預熱原本呼叫 `_benchmark_history_index`，載入
131,352 筆 benchmark marks。實際 chart reader 已採日期分區投影及
外層 bytes cache，啟動預熱需要的只是 156 個日期，不需要整份 Python
物件圖長期常駐。來源 `benchmark_history.json` 為 331,514,015 bytes；
已接受的日期 head 只有 36,112 bytes。

先以正式 owner 的數值執行緒／索引目錄環境重播完整 gateway 預熱，
不啟動第二個 collector 或常駐服務。舊版觀察 RSS 約 893 MB，
最後的「完整 benchmark index 預熱」額外留下約 366 MB。
另一輪一次性 Python allocation 診斷中，477 MB live Python objects
約有 386 MB 來自 stdlib JSON decoder。tracemalloc 本身有顯著開銷，
這些診斷牆鐘及 RSS 不當作正式服務效能。

OfficialDailyQuoteSource 已經採日期 predicate 與必要欄位投影；本次
預熱只留下三日 7,071 列。Arrow pool live bytes 為零、peak 約 19 MB，
不支持「Arrow 活資料吃掉近 1 GB」的歸因。一次性 allocator release
只釋出約 39 MB，因此未部署每次 request 呼叫 `release_unused()` 的
政策；[Arrow 官方文件](https://arrow.apache.org/docs/python/generated/pyarrow.MemoryPool.html)
也將其定位為可能昂貴、影響效能的 debug／benchmark 操作。

修改 `warm_dashboard_session_indexes`：有效 head 只讀日期目錄，
不把空選擇寫入完整列 cache。真正 chart read 的 shard SHA 驗證不變；
缺少／過期／壞 head 仍走原完整 reader。position locators、帳本 byte
索引、line counts、歷史持倉與任何圖表列都不刪除。

### 完整受測流程與正式讀回

八個獨立程序交錯 ABBA × 2，正式 gateway 使用的
`use_memory_cache=False` 設定，完整包含所有索引預熱、全歷史 8 條
曲線、三個指定交易日 8 條曲線、編碼、GC／閒置觀察及再次完整 warm
讀取。每個 worker 使用私人 derived index cache；沒有修改來源、
正式 cache、API 配額或 GPU。所有曲線、索引計數及再次讀取均 exact。

| 整個受測工作流程中位數 | 原完整列預熱 | 日期目錄預熱 | 降幅 |
| --- | ---: | ---: | ---: |
| 牆鐘 | 3.3043 秒 | 2.3609 秒 | 28.5% |
| CPU | 3.3541 秒 | 2.4102 秒 | 28.1% |
| 閒置 RSS，十進位 MB | 417.3 | 265.4 | 36.4% |
| 峰值 RSS，十進位 MB | 449.1 | 297.0 | 33.9% |
| 再次完整 warm 讀取牆鐘 | 0.6756 秒 | 0.6502 秒 | 未退化 |

全歷史 341,952 個點、8 條曲線；最早／中間／最新指定日各 2,192 點，
每條曲線完整 bytes 與全部索引計數一致，不是抽樣前 N 列。
這是上述完整有界流程，不包括 gateway status／其他 upstreams／
任意 range 組合，也不代表全主機的同倍率改善。

較早的兩次量測因整份 `state.json` 心跳 mtime fence 拒絕，舊失敗
收據保留。其中一次已定位唯一變動為 `state.json`，不是歷史檔或程式。
canonical history 計算只消費 `product`、`enabled_markets`，最新版
分開記錄這兩欄的計算輸入 fence 和整檔 metadata，仍 fence 全部歷史
來源、SHA256、四個程式檔；改變產品／市場／任何歷史來源的測試仍拒絕。
採用的八次量測是重新執行，不將被拒絕的舊量測改寫成成功。

05:34 的重載只涉及 `stockagent-public-dashboards.service`。
正式讀回證明全歷史 15,814,439 bytes 與最新單日 105,915 bytes
逐 byte 相同；protected trading／capture／FinMind／Discord／TAIFEX
display owners 在本次重載前後一致。05:30 原生 Shioaji SDK 維護
排程另行重載了自己的行情及交易 owners，不是本次操作，也不宣稱
這些 owners 整晚都未改變。

新版相同環境完整 gateway 預熱診斷觀察 RSS 753 MB、6.75 秒；正式
服務 05:36–05:37 取樣 RSS 806,248,448 bytes（約 769 MiB）。這兩個
數字屬不同流程／時點，不將其與有界 ABBA 的 265 MB 混在一起。
同時全卡 GPU 利用率 3–4%、占用 3,388 MiB；不是本次模型 cache
使用 GPU 的證據，也未識別所有 Windows GPU clients。

本版 207 項共享回歸與 22 項專用 fence／query／prewarm 測試通過，
兩組有重疊，未聲稱全 repo suite 完成。HTTP verifier 首次誤用
內部 `history_encoding` keyword，依真正 public parser 改用
`encoding=v2` 並加入正式 query 契約測試，未放寬 API。重載尚在
`activating` 時的一次 probe 亦正確拒絕，待服務完成預熱後才驗收。

- [正式設定八次完整 ABBA、全部 SHA／索引計數與資源中位數](../artifacts/benchmarks/running_services_20261013/tw_catalog_prewarm_gateway_settings_20261014.json)
- [正式重載前完整 HTTP 控制](../artifacts/operations/running-services-optimization-20261013/tw-catalog-valid-previous-reader-20261014.json)
- [正式新版完整 HTTP bytes 與 protected owners 讀回](../artifacts/operations/running-services-optimization-20261013/tw-catalog-ready-actual-http-readback-20261014.json)
- [舊版正式環境全 gateway 記憶體歸因](../artifacts/operations/running-services-optimization-20261013/public-gateway-owner-matched-residency-20261014.json)
- [新版完整 gateway 預熱診斷](../artifacts/operations/running-services-optimization-20261013/public-gateway-current-catalog-residency-20261014.json)
- [05:36–05:37 全服務與 GPU 取樣](../artifacts/operations/running-services-optimization-20261013/census-after-catalog-adoption-20261014.json)
- [207 項共享語意回歸](../artifacts/operations/agent-workflow/runs/tw-catalog-current-schema-regres-20261013T211704-8206c3da/run.log)
- [22 項目前 public query 與完整輸入 fence 測試](../artifacts/operations/agent-workflow/runs/tw-catalog-wire-contract-regress-20261013T213410-d56dae7c/run.log)

### 隔日資格發布後：更新摘要，不重算相同模型輸入

原資格 watcher 的不可變收據確認：05:22 將下一交易日（10 月 14 日）
資格表加入 live root，TWSE 1,226 檔／TPEx 838 檔。這是實際來源世代
改變，不是 SDK 維護造成，也不是交易引擎的 `state.json` 心跳。
因此 04:54 摘要在 05:35 後的驗證正確回報
`completed_source_generation_changed`；未回填舊 summary 的 fingerprint。

最新資料完整重播五模式，每模式三次完整 signal 操作，在模型、CUDA
及 quote provider 禁用條件下均命中原始 forward key，完整輸出與原
獨立控制一致。canonical worker 的 `signal_cache_only=True` 分支
沿用原有鎖、來源接受、訊號出版及 owned-cache 回收，05:39 只更新
五個使用者摘要／weights pointer，沒有交易帳本操作或正式歷史重跑。
05:41 新摘要再次接受來源世代，驗證五模式正式 keys 與完整控制一致。

這證明「來源更新需刷新摘要」不等於「相同輸入要重做 GPU forward」。
未改純 forward identity，也未將日曆或資格表的來源 fence 移除。
本次受控更新不當成未來每次事件已自動正確觸發的證明；事件連動的
部署與獨立原生工程通知驗收見下一節。

- [05:22 下一交易日資格來源不可變收據](../artifacts/data_refresh/tw_day_trade_eligibility/runs/20261014T043206043762.json)
- [五模式最新來源的完整輸出與零 GPU 重播](../artifacts/operations/agent-workflow/runs/postclose-new-source-five-cache--20261013T213742-6303c237/run.log)
- [既有 canonical cache-only 維護器，禁止 GPU／模型／行情 API](../artifacts/operations/agent-workflow/runs/canonical-postclose-source-refre-20261013T213941-d73be7a1/run.log)
- [05:41 五模式新來源正式讀回、原 forward key 與獨立完整控制](../artifacts/operations/running-services-optimization-20261013/postclose-after-source-generation-formal-readback-20261014.json)

### 原生事件連動：只在 committed source 變動時喚醒

舊 `stockagent-discord-artifact-maintenance.path` 只監聽收盤接受收據，
因此隔日資格表更新未自動喚醒 cache-only worker。現在同一個 path
加上 canonical live root 的 TWSE／TPEx 資格 parquet，仍指向既有
`stockagent-discord-postclose-cache.service --signal-cache-only`。
不監聽會定期改寫的等待／heartbeat `latest.json`，不新增輪詢 daemon，
不啟動完整歷史維護器。這兩個絕對路徑與 publisher 預設及目前五模式的
實際 resolver root 一致；自訂 rule root 時需重新配置並驗證 watch set。

[systemd 官方 path 說明](https://raw.githubusercontent.com/systemd/systemd/v255/man/systemd.path.xml)
描述原生 inotify 監聽與 `PathChanged` 的 close-write／多來源行為。
實際主機是 systemd 259；功能接受仍以本次原生 owner、atomic-replace
工程通知、終止及後續閒置讀回為準，不只靠 man page 或 unit 字串。
資料接受、來源 shared lock、opening guard、完整 forward identity 及
單一 cache-worker lease 全部沿用既有實作。

05:58 使用一次明確標示的 `/run` 工程通知，同一 native path 真實
喚醒同一 cache-only owner。沒有改任何市場原始資料／mtime／發布時鐘，
也不把工程通知當成真正的新資格發布。原生 worker 成功處理五模式，
6.154 秒後退出、CPU 6.096 秒、峰值約 512 MiB；這是完整新 process
啟動與所有 reuse gates 的成本，不是模型推論速度。native log 沒有
產生新 signal 的 `postclose_cache_ready`；前後五個完整 summary bytes、
weights bytes、來源 identity 與原 forward key 都不變，禁止模型／CUDA
的 canonical verifier 亦通過。

測試用 path drop-in 已先撤除並只重載 path owner，再移除通知檔，
避免在仍受監聽時刪除 marker 又觸發額外工作。測試通知保留工程副本。
06:00 正式讀回只剩三個必要來源、無 runtime drop-in；維護器仍是
05:58 同一個 terminal handle，沒有反覆啟動。六個 protected reader／
trader／capture／download owners 前後不變。真正下一次資格發布的
端到端時點尚未發生，仍保留 `real_new_publication_tested=false`。

147 項共享 cache／Discord／event-wire 測試通過。初版驗證工具用 dict
解析 `systemctl show`，把多次 `Paths=` 只保留最後一個，導致兩次工程
讀回錯判；已修為保留全部值、加上真實 CLI 格式回歸，舊失敗保留。
錯判發生在發送通知前，沒有因此改寫來源或重算模型。

- [原生目前完整 watch set 與五模式基線](../artifacts/operations/running-services-optimization-20261013/postclose-event-current-multivalue-control-20261014.json)
- [原生通知交付、terminal handle 與五模式完整 bytes／key 不變](../artifacts/operations/running-services-optimization-20261013/postclose-event-current-native-delivery-20261014.json)
- [撤除探針後三來源與閒置 terminal handle 讀回](../artifacts/operations/running-services-optimization-20261013/postclose-event-normal-idle-readback-20261014.json)
- [保留的非市場工程通知](../artifacts/operations/running-services-optimization-20261013/postclose-event-marker-proof-20261014.json)
- [147 項相關語意／多值 wire 回歸](../artifacts/operations/agent-workflow/runs/postclose-event-multivalue-wire--20261013T215711-359a7ec0/run.log)

全服務目標仍 active。FinMind 舊 owner 的 covering index 尚待安全空閒
窗口；新取樣最高 CPU 為原生 Shioaji 歷史回補，須用其完整 request／
驗證／persist 流程另行量測，不能中斷歷史下載或拿 index microbenchmark
宣稱全部改善。兩個正式歷史 GPU 工作失敗亦仍未解決。

### 永豐歷史回補：單次完整收據遍歷

06:07 的 `--nonblocking --rate 10 --duration 20` 取樣只有 18 個有效
stack samples，並有 5 個錯誤；不把比例解讀成 CPU 秒或完整查詢吞吐。
同時實際子程序 RSS 約 1.37 GiB、其中匿名頁約 1.22 GiB；這是當時
回補批次，不是所有服務的 RAM。原生日誌的每批 500 queries 包含首筆、
第 250 筆與最後三次 summary audit，近期每次約 47–52 秒。

舊 summary 再讀 KBar 收據建立 tick 日期，並呼叫完整 `build_tasks`
重建待辦；新 summary 在同一次既有 SHA／device／inode／size／mtime／ctime
驗證中計算覆蓋、官方活動缺口、tick 目標及待辦數。派工與摘要共用
retry predicate；不是用已完成數扣除待辦，也沒有跳過未知空回應或
合約不可查的覆蓋缺口。KBar/tick 轉換、順序、單位、日曆、窗口、
三次摘要頻率、既有修復冷卻、provider 請求和 publication gate 均未改動。

已直接核對[目前官方限制](https://sinotrade.github.io/tutor/limit/)：
行情查詢共用 50 次／10 秒，與本機現行 limiter 相符；沒有依 bundled
技能中較舊的 50 次／5 秒說明提高配額。

本階段量測包含固定清冊的 inventory、官方 activity、完整待辦、三次
全清冊 summary 與真實 atomic persist；不含 SDK 初始化／登入、網路
查詢與 publication，因此不能據此宣稱整個下載批次或全機同樣提速。
真實來源逐 receipt/data pair 做 signature fence 後獨立複製，不使用
hard link、更改原始資料／mtime、改名或清理歷史檔案。主機仍有其他
服務，OS page cache 不清空；比較使用獨立程序的 ABBA 次序。

第一次 literal filesystem snapshot 超過 300,000 檔安全上限而停止；
保留的私人部分快照為 300,000 檔、4,379,627,045 bytes，失敗收據未刪除。
改為目前清冊真正遍歷的 KBar chunk 與已驗證 KBar／官方 activity tick
聯集；過期的舊 query range 仍留在正式 archive，不列為這次工作量。
固定的來源檔案、完整 task/summary hash、實際節點/cgroup/runtime 與
寫檔讀回共同作為接受條件。06:51 的四程序對照已完成：

| 完整本機 audit 中位數 | 原先多次遍歷 | 單次遍歷 |
| --- | ---: | ---: |
| 牆鐘時間 | 387.85 秒 | 198.85 秒 |
| CPU 時間 | 317.38 秒 | 200.35 秒 |
| 程序峰值 RSS | 1,331,910,656 bytes | 1,651,480,576 bytes |

四次皆有 7,579 個合約、135 個真實待辦；完整排序 task SHA、三份
全部摘要 SHA 與 atomic 寫檔回讀逐欄一致。對照順序為舊／新／新／舊，
首輪舊版 physical read 約 7.16 GB，而最後舊版約 15.27 MB；不能忽略
磁碟冷熱差。最後暖讀舊版與新版中位比較約為牆鐘少 26.6%、CPU 少
26.2%，也不是受控 page-cache 或獨占主機的倍率。RSS 中位增加約
24% 是尚待修正的結果，不因牆鐘改善而宣稱記憶體改善。

有效清冊含 144,941 份收據及 117,470 份資料檔；原收據 LRU 為
131,072、SHA LRU 為 16,384，均小於其工作集。循環全庫遍歷仍大量
淘汰；新版四次 audit 的 logical read 約 11.33 GB，原版約 14.19 GB。
06:57 啟動隔離的容量 ABBA：原配置對照收據 4,096／SHA 131,072，
只在獨立 worker 重包原函式 decorator，使用相同清冊、官方活動、
完整待辦／摘要／persist，並以已接受的原版所有輸出 hash 為 oracle。
四次全輸出及來源 fence 均通過；中位牆鐘由 158.36 秒增加到 167.33 秒
（+5.7%），CPU 由 160.29 秒增加到 168.89 秒（+5.4%），峰值 RSS 由
1,627,254,784 降到 1,020,571,648 bytes（−37.3%）。小收據快取每次
都 miss，SHA 重用的收益不足以抵銷完整收據重讀成本；未採用這個容量
政策，原收據／SHA 上限保持不變。另以同清冊完整 cProfile 定位下一個
CPU 熱點，profile 已完成且全待辦／三摘要輸出一致：445,479 次
`Path.resolve()`、3,096,485 次 `stat`、4,900,269 次 `lstat`。解析路徑
累計約 72.74 秒；累計時間彼此包含，不能相加。profile 本身約 286.76 秒，
包含量測成本，不能直接當公平加速倍率。不把 hash cache 當
免驗證，device/inode/size/mtime/ctime 與 atomic replacement fence 全保留。

下一個候選保留原 `c343eed…` repair source 的獨立副本與已接受 oracle，
不從新版生成舊版控制。SHA cache lookup 改用不解析每層路徑的絕對名稱，
cache miss 必須開啟一個自己擁有的 regular-file descriptor，`fstat` 的
五欄身分在讀取前後一致，返回前再核對路徑身分。非 blocking 開啟防止
檔案被替換成 FIFO 時卡住；雜湊前設回 blocking，結束一定關閉 descriptor。
這使用 [Python 3.12 `hashlib.file_digest`](https://docs.python.org/3.12/library/hashlib.html#file-hashing)，
不是換雜湊算法或略過 SHA。來源競態／I/O 錯誤在外層收據 validator
fail closed 且不 memoize 暫時失敗，不中止整個 audit。

80 項共享回歸涵蓋完整計畫、逐欄摘要、SHA memo、同大小恢復 mtime 的改寫、
symlink ABA、cache-hit 期間切換、原子替換、刪除、FIFO、防 descriptor 洩漏，
另有 profile／獨立 baseline 身分與失敗收據測試。這不是全 repository 測試。
六個獨立程序以原版／compact FD／default FD／default FD／compact FD／原版
比較原容量與兩種候選容量；在對照完成前沒有採用容量變更、沒有重啟 owner。

07:50 六程序完整對照接受，CPU／牆鐘／RSS 的中位數如下。所有 task SHA、
三份完整 summary SHA、persist 回讀與來源／程式 fence 相同，provider calls
為零。OS page cache 未清空，其他原服務繼續工作，沒有全機獨占保證。

| 完整相同本機工作 | 舊 SHA／原容量 | FD／原容量 | FD／選出容量 |
| --- | ---: | ---: | ---: |
| 牆鐘秒 | 156.735 | 144.705 | 125.132 |
| CPU 秒 | 157.770 | 146.443 | 127.218 |
| 峰值 RSS bytes | 1,651,818,496 | 1,642,520,576 | 1,071,228,928 |

選出容量在三個資源面向都優於兩種控制：相對原版牆鐘少 20.2%、CPU 少
19.4%、RSS 少 35.1%。後者不是 06:57 的原 SHA 小快取負例；只有配合
移除重複路徑解析的 FD 身分驗證才觀察到此改善。不把兩輪資料混在一起。

07:51 先獨立保存受測兩模組原始 bytes／SHA／AST，再只改兩個 literal LRU
容量：收據 4,096、SHA 131,072。07:58 無替換 decorator 的 canonical 完整
計畫／三摘要／persist 回讀再次一致；全模組 AST 只允許那兩個容量不同。
這次單一回讀不是重新比較速度；上述倍率仍來自六程序完整平衡對照。
新預設通過 95 項共享測試，與此前 80 項重疊，不相加。

目前只證明程式預設與完整本機流程；沒有為此重啟下載 owner。原排程的
盤中保護仍保留，下一個自然 batch／正式 API owner 載入另驗，
`live_owner_adoption_verified=false`。沒有把 zero pending、來源空值或
不可查歷史改成 complete。

正式歷史 owner `e17122c4dd2c4242a3e00c71640519fd` 已自行進入
`waiting_source`；06:09 收據仍為 source gaps，pending=0 不是全資料完整。
07:09 因既有 TX history 持有 shared login slot，原生排程正常等 60 秒，
07:11 起自行取得槽位並載入 `summary_strategy=single_pass_receipt_v1`。
07:15 以首筆／最後 `coverage_passes=1`、canonical 摘要與原排程收尾讀回
接受正式載入；history owner 與其受保護 owner 在這次讀回都不變。
實際本批為 40 次原生來源查詢，摘要首筆 38.122 秒、final 75.175 秒；
同時有其他 CPU 工作，沒有匹配原版的 API／全批控制，不能把離線倍率
套到本批或稱原生 final 比原版快。工程 verifier／benchmark 的 provider
calls 為零，原生下載本身的 40 次查詢不混為零。仍有 66 次正活動空回應、
21 個官方活動缺日期與 1,540 個不可查合約，`all_source_data_complete=false`。

- [非阻塞原始 stack samples](../artifacts/operations/running-services-optimization-20261013/shioaji-history-nonblocking-20261014.txt)
- [第一個快照安全停止紀錄](../artifacts/operations/agent-workflow/runs/shioaji-full-local-audit-fixture-20261013T221256-f520920b/run.log)
- [有效清冊快照的原生執行紀錄](../artifacts/operations/agent-workflow/runs/shioaji-canonical-target-audit-f-20261013T222128-91619bdc/run.log)
- [四程序完整清冊 ABBA、全部輸出 hash、runtime 與 I/O](../artifacts/benchmarks/running_services_20261013/shioaji_canonical_audit_abba_20261014.json)
- [原生 ABBA handle 的完整紀錄](../artifacts/operations/agent-workflow/runs/shioaji-canonical-full-audit-abb-20261013T222851-d5a97b41/run.log)
- [256 項最後共享契約回歸](../artifacts/operations/agent-workflow/runs/shioaji-single-pass-final-contra-20261013T222637-ab5bdd68/run.log)
- [36 項含 fixture 競態／有效目標範圍的回歸](../artifacts/operations/agent-workflow/runs/shioaji-audit-target-safety-test-20261013T222238-ba6c4918/run.log)
- [隔離快取容量 ABBA 執行紀錄](../artifacts/operations/agent-workflow/runs/shioaji-complete-cache-capacity--20261013T225708-7cb9fab8/run.log)
- [快取容量全輸出、CPU／RSS 負例](../artifacts/benchmarks/running_services_20261013/shioaji_cache_capacity_abba_20261014.json)
- [完整 profile、全輸出相等與原始 syscall 計數](../artifacts/benchmarks/running_services_20261013/shioaji_compact_full_audit_profile_20261014.json)
- [原始 SHA 實作的獨立副本與 hash／oracle 綁定](../artifacts/benchmarks/running_services_20261013/shioaji_verified_hash_reference_v2_20261014/reference.json)
- [80 項 FD 身分、完整 audit 與 profile 回歸](../artifacts/operations/agent-workflow/runs/shioaji-owned-fd-final-full-sema-20261013T233208-0b390e36/run.log)
- [六程序原始 SHA／兩候選、完整 oracle、節點與 I/O](../artifacts/benchmarks/running_services_20261013/shioaji_verified_hash_full_balanced_20261014.json)
- [容量變更前保存的兩份受測原碼與 AST 綁定](../artifacts/benchmarks/running_services_20261013/shioaji_measured_capacity_reference_20261014/reference.json)
- [未打 memo 補丁的完整正式預設驗收](../artifacts/benchmarks/running_services_20261013/shioaji_unpatched_default_full_audit_20261014.json)
- [95 項正式預設、FD 與完整共享契約測試](../artifacts/operations/agent-workflow/runs/shioaji-default-capacity-full-re-20261013T235246-b4c83a9f/run.log)
- [正式 owner 自然載入的基線](../artifacts/operations/running-services-optimization-20261013/shioaji-audit-natural-control-20261014.json)
- [原 owner 自然完成、實際 marker／摘要／排程驗收](../artifacts/operations/running-services-optimization-20261013/shioaji-audit-natural-terminal-20261014.json)

### Discord 盤前 GPU 常駐窗口

07:03 原程序完成五模式暖機曾測得 15.338 秒，但原窗口硬編碼從準備
時間 08:15 提前 75 分鐘，釋放保護又獨立寫死 07:00。暖機完成不等於
盤後 forward cache 被使用，也不等於所有閒置服務已無 GPU 配置。
本次把啟動暖機與 GPU 常駐保護共用相同窗口：這台主機預設準備前
600 秒（08:05），保留 60／120／240 秒重試與完整五模式暖機餘裕。
`STOCKAGENT_STARTUP_WARM_LEAD_SECONDS` 可依其他主機實測設定，限制
60–4,500 秒；非有限／格式錯誤沿用預設。未知 schedule 仍保守保護，
正常開盤重試、live 其他市場、model mutex 與 postclose source gate 不改。

成功釋放 model cache 才清除 process-hot flag，不重設 permanent failure；
否則隔日同一程序會因舊的 warm-ready flag 跳過重新準備。health 另標示
`model_cache_resident_ready`，把歷史暖機完成與當前常駐狀態分開。
嚴格 HH:MM 負例發現舊 helper 接受 25:00，新 GPU 窗口自己拒絕越界
時鐘，沒有趁機改其他市場 schedule parser。

07:18–07:19 僅重載 Discord；原交易、行情捕捉、兩個相關下載／展示與
公開入口 owners 不變。新 Python PID 1,674,213（原 1,045,265）重新連線、
global commands sync 成功／或明確 defer 必須可讀，五模式 engine revision
與 signal IDs 零落後；目前所有收盤 summary SHA、forward key 及獨立完整
權重／決策／解釋 oracle 不變，GPU device handles 從 2 變 0，沒有提前
暖機 ready 日誌。這是正式閒置窗口採用，不代表 08:05 下一次暖機或
09:00 opening SLO 已發生及驗收，也沒有修復原兩個失敗的 formal history。

08:05 原生第一次暖機遇到來源發布寫鎖：五模式均在 `_build_panel` 的
`live_tw_public_source_read_guard` 非阻塞讀鎖返回 `BlockingIOError`。
既有 60 秒重試在 08:06:21.418 開始下一輪，08:08:10.515 完成五模式，
合計 109.097 秒；成功早於 08:15 準備時間，沒有強制暖機、重載或移除
一致性保護。08:10 唯讀接受確認同一 PID／InvocationID、五模式當前
resident-ready、GPU device handles、原生日誌及 engine acknowledgement
零落後；verifier 不呼叫 CUDA／provider、不寫交易。08:06 的早期探針
因尚未完成而正確拒絕，失敗保留並由實際成功讀回 supersede。

這次暖機不能沿用先前 15.338 秒當固定估計，也不能稱 109 秒全部花在
GPU。接受收據內的原生階段計時如下；`pre_quote_prepare_ms` 包含
checkpoint、面板讀建與對齊等取價前工作，尚不是獨立 panel profiler。

| 模式 | 完整暖機秒 | 取價前準備秒 | 模型階段秒 | 面板 tier |
| --- | ---: | ---: | ---: | --- |
| V8 Annual Log Cash | 40.837 | 37.494 | 3.062 | rebuilt |
| Multi Basis 22 | 35.193 | 33.897 | 1.126 | rebuilt |
| Multi Basis | 0.811 | 0.165 | 0.406 | memory |
| 100M | 0.481 | 0.150 | 0.125 | memory |
| Factorized Scale Separated Cash | 31.761 | 28.883 | 2.768 | rebuilt |

三個 rebuilt 模式的取價前準備明顯較重；其他兩模式命中 memory。
後續先依相同語意的 panel／來源鍵分析可共用範圍，不能直接混用不同
feature、checkpoint universe 或來源世代。08:15–09:05 不重載交易相關
owner、不重跑重 GPU benchmark；真正 opening SLO 仍未驗收。

原控制的 NVML 全卡顯存 15,130 MiB，新原生讀回另記錄當時數字；
07:19 人工讀回約 3,174 MiB。全卡仍有 Windows 桌面／Sunshine／其他
程式，不能把差值逐 byte 歸因給 Discord，也不強制停止非專案應用。
新的 WDDM 歸因探針只做兩次有界唯讀取樣，每次約 9.25 秒，沒有安裝
polling daemon；Windows PID 不映射成 Linux PID。Microsoft 亦記載
[GPU Process Memory counter 可能不準確](https://learn.microsoft.com/en-us/troubleshoot/windows-client/performance/gpu-process-memory-counters-report-wrong-value)，
同一共享配置不可把每個程序的記憶體加總當作整卡占用；不以 counter
較大就宣稱 source corruption／memory leak 或終止程序。

- [真正重載前的五模式、原 owner、GPU handle／NVML 控制](../artifacts/operations/running-services-optimization-20261013/discord-prewarm-reload-admission-20261014.json)
- [新 owner／零 handle／full cache／commands／zero-lag 原生接受](../artifacts/operations/running-services-optimization-20261013/discord-prewarm-native-idle-acceptance-20261014.json)
- [僅 Discord 的原生重載動作](../artifacts/operations/agent-workflow/runs/discord-only-idle-prewarm-clock--20261013T231836-c23c9d5f/run.log)
- [189 項暖機窗口、native verifier 及原共享回歸](../artifacts/operations/agent-workflow/runs/discord-idle-native-all-existing-20261013T231735-84439167/run.log)
- [五模式自然暖機、當前常駐、owner／engine 原生接受](../artifacts/operations/running-services-optimization-20261013/discord-natural-prewarm-terminal-20261014.json)
- [57 項自然暖機、日期／clock／resident 與原生讀回回歸](../artifacts/operations/agent-workflow/runs/discord-natural-warm-clock-final-20261014T000507-3cec0fef/run.log)
- [Windows WDDM／board 的分開取樣](../artifacts/operations/running-services-optimization-20261013/windows-gpu-process-observation-20261014.json)
- [FinMind 07:02 仍無 drained idle window，沒有重啟](../artifacts/operations/running-services-optimization-20261013/finmind-early-morning-idle-observation-20261014.json)
- [下游 FD 優化後，目前五模式仍完整零 CUDA 回讀](../artifacts/operations/running-services-optimization-20261013/postclose-after-shioaji-fd-defaults-20261014.json)

### 全市場股票尾端準備：先否決門檻，再測原始資料共用

同一 Discord InvocationID 的成功暖機日誌，三個 rebuilt BASE panel 的
`symbols` 階段分別為 29.379／28.921／24.211 秒，合計 82.511 秒。
相同三輪的 external／assemble_rules／total 也保留；這是原生階段計時，
不是把所有取價前時間誤歸給 GPU。另兩個模式命中面板 memory cache。
不能混用不同 feature、checkpoint universe、raw-input 或來源接受世代；
可以測試共用相同的物理股票原始資料，後續仍各走 canonical feature builder。

CPU-only 完整 BASE panel fixture 保存 2,760 個股票檔、共 2,768 個來源／
資格與權利參照／收據成員，1,601,263,137 bytes。四個獨立程序按
original／candidate／candidate／original 執行；每輪重建五個配方、保留
memory reuse、私有 disk cache 發布，逐一比較 `PanelData` 所有 fields、
日期／商品順序、dtype、mask、可選欄位及浮點 bits。沒有 provider 呼叫、
GPU 初始化、行情／訊號／交易寫入或原始資料改寫。

| 完整 BASE panel 工作的中位數 | 原門檻 8,192 | 候選門檻 256 | 判斷 |
| --- | ---: | ---: | --- |
| workflow 牆鐘秒 | 86.316 | 109.922 | 增 27.3%，不採用 |
| CPU 秒 | 140.453 | 182.675 | 增 30.1%，不採用 |
| process peak RSS bytes | 1,963,984,896 | 1,915,781,120 | 減 2.5%，不足抵消時間退步 |

較小八檔樣本的結果分布混合，不能替代此全市場結果。正式 reader 仍保留
8,192，不繼續用同一假說盲試更低值。workflow 包含面板建置、memory reuse、
disk publication 與完整 oracle；imports、完整 fixture fences 在該 stopwatch
之外。它不是 factorized attachment、checkpoint／model、quotes、signals、
opening SLO 或完整訓練 epoch 的改善證據。

新候選只共用同檔／tail 長度／全部 logical parts 世代的原始 Arrow table。
使用 64 MiB 上限、短鎖與 LRU，I/O 不持有全域 mutex；來源變動或 transient
error 不回傳舊 cache。clear 有 in-flight epoch fence。Arrow 的
[slice 是 zero-copy，而 buffer size 會計入整個被引用 buffer](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html)，
因此快取前以 `take` 壓成實際尾端 rows，再按 `get_total_buffer_size` 計額度；
不以 48-row 邏輯 nbytes 掩蓋整個歷史 row group 的常駐記憶體。
source read lease 仍由 canonical live caller 持有，stat 只是防線，不是
資料發布／接受或 owned-FD provenance 的替代證明。

新候選通過 80 項 raw-cache／完整 oracle／固定來源／原始 reader 回歸，
原先獨立保存並綁 SHA 的完整控制與四輪全市場對照亦通過。兩次候選均為
2,760 misses／5,520 hits、zero evictions，實際 cache buffers 27,542,765 bytes
（26.3 MiB），容量上限 64 MiB。每輪皆全欄位／完整來源及 code fence 通過。

| 原始尾端共用的完整 BASE panel 中位數 | 原版 | 候選 | 變化 |
| --- | ---: | ---: | --- |
| workflow 牆鐘秒 | 95.407 | 69.398 | 減 27.3% |
| workflow CPU 秒 | 155.311 | 100.817 | 減 35.1% |
| process 牆鐘秒（imports／fences 亦計入） | 105.531 | 79.434 | 減 24.7% |
| process CPU 秒（imports／fences 亦計入） | 165.410 | 110.834 | 減 33.0% |
| process peak RSS bytes | 1,965,694,976 | 2,055,501,824 | 增 4.6% |

這是同一 live host、nice 19、Arrow／Polars threads 2、panel workers 8 的
CPU-only BASE panel 實測；OS cache 未控制。不能把原門檻實驗的 baseline
與這一輪交叉當作倍率，不能當完整 GPU 暖機或 API／全機改善。額外 RSS
不全是 cache buffers；正式整合須沿 idle/deployment cache clear 釋放來源
快取，避免把盤前換取的重複解碼減量變成盤後長期常駐成本。
此時仍未整合進 production，沒有重載開盤保護中的 owner。

- [原生成功暖機的同 owner 階段日誌](../artifacts/operations/agent-workflow/runs/native-warmup-panel-stage-attrib-20261014T002627-45c6a857/run.log)
- [完整全市場 BASE panel fixture](../artifacts/benchmarks/running_services_20261013/five_mode_live_base_panel_cutoff_20261014/fixture.json)
- [已拒絕的門檻四程序完整對照](../artifacts/benchmarks/running_services_20261013/five_mode_live_base_panel_cutoff_abba_20261014.json)
- [獨立原始 controller／完整 oracle 的保存收據](../artifacts/benchmarks/running_services_20261013/live_base_panel_controller_reference_20261014.json)
- [80 項來源快取與獨立 oracle 回歸](../artifacts/operations/agent-workflow/runs/raw-cache-independent-oracle-tes-20261014T005411-30619270/run.log)
- [原始尾端共用四輪完整輸出與 process 資源對照](../artifacts/benchmarks/running_services_20261013/five_mode_live_base_panel_source_cache_abba_20261014.json)
- [有界原始資料快取](../downloader/live_tail_source_cache.py)、[完整面板對照入口](../scripts/benchmark_live_base_panel_cutoff.py)

## 07:33 全服務正常佔用再取樣

在六程序 benchmark 開始前，canonical 普查先完成 21.16 秒資源 interval。
87 個已安裝 StockAgent services、一個普查 transient、67 timers、3 paths
均留在清冊；CPU／memory／I/O 的數值與是否工作、失敗及資料完整性分開。
Discord 匿名記憶體 544,399,360 bytes（約 519 MiB）、CPU 0.0491 core；
公開 gateway 匿名記憶體約 679 MiB、CPU 0.0200 core，TAIFEX 展示無額外
高耗用判定。這不是程序 RSS、整卡 VRAM 或前後等負載的全機改善倍率。

較高 CPU 是正在工作的保證金 action（1.006 core）、partition return
（0.971）、TX history（0.879）和註冊 intraday 抓取（0.567）；不以停止它們
來改善取樣。FinMind complement 正常工作約 0.347 core，離線 covering index
仍因未有合格 drained idle window 而未正式載入。Windows、kernel file cache
與遠端工作另有邊界，WSL service 資源不能全部相加當作應用程式 RAM。

本次沒有 resource pressure finding；CPU PSI some avg10=1.8%、memory
some=0.02%、I/O some=1.99%，不是零延遲或全日無壓力的證明。清冊仍保留
OpenBB、Discord 正式歷史維護、crypto refresh、registered daily、cold publish、
release archive、Vast bulk return 的失敗，不把優化的健康／快取驗收套到它們。

- [所有目前可發現服務、主機與匿名／file cache 分拆](../artifacts/operations/running-services-optimization-20261013/census-after-native-gpu-window-20261014.json)
- [canonical 普查執行紀錄](../artifacts/operations/agent-workflow/runs/all-services-after-gpu-window-id-20261013T233234-596e059a/run.log)
- [79 個可見受管理 Linux 程序的 GPU FD／身分逐項取樣](../artifacts/operations/running-services-optimization-20261013/all-managed-gpu-handle-idle-20261014.json)
- [GPU FD 身分、unreadable／退出及多控制器 7 項測試](../artifacts/operations/agent-workflow/runs/managed-gpu-fd-inventory-portabl-20261013T234943-a21143c6/run.log)

## 測試與重現

較廣的 FinMind、准入、OpenBB monitor／ownership、服務清冊與 dashboard
量測回歸為 1,456 passed。最後程式版本另跑 572 passed focused tests，
涵蓋相同時計／owner reuse、NULL renewal、legacy 重複優先 key、後段
metadata 壞頁、缺欄位分類與零列分類；兩組有重疊，不相加稱 2,028 個不同測試。
修改範圍 `git diff --check` 通過；未宣稱 repository 全測試完成。
工程失敗紀錄保留並由成功收據 supersede，不刪除、不重寫成成功。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_service_latency_coverage.py \
  --sample-seconds 20 --include-host-services \
  --output artifacts/operations/running-services-optimization-20261013/retest.json
run_fintech_python scripts/benchmark_finmind_eta_runtime.py \
  --fixture artifacts/benchmarks/running_services_20261013/finmind_eta_v2 \
  --output artifacts/benchmarks/running_services_20261013/finmind_eta_retest.json
run_fintech_python scripts/benchmark_openbb_metadata_audit.py \
  --fixture artifacts/benchmarks/running_services_20261013/openbb_audit \
  --output artifacts/benchmarks/running_services_20261013/openbb_audit_retest.json
```

新的 fixture 使用 `--prepare-only` 和新路徑；舊 fixture 不覆蓋。先確認有
至少 30 GiB 安全剩餘空間加來源快照容量，再做 SQLite backup 或來源抽樣。
這些測試不需要新登入或 GPU 訓練；既有訓練與 source receipts 不改動。
