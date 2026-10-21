# 全網站與當沖策略的真正逐日增量

範圍：2026-10-20 使用者要求的**所有網站、所有當沖策略**，不只
Factorized。完整歷史、原始時鐘、NULL、金額及策略語意不縮減。

## 最新交付摘要（10/21 續作：研究 lineage、完整驗收與 freshness）

- 已實作相容 reader、精確 BASE suffix、opt-in 年分區 writer、manifest-aware
  個股價格讀取與 live／completed cache 來源 key。正式 writer 預設仍是 `file`。
- 同政策個股階段：full 4.645 s、增量 2.667 s；**不含上游全歷史 merge**，CPU
  未下降。年分區 8.156 s 反而更慢，故不啟用；不是「BASE 整段已加速」。
- 真實 2,761 檔來源的完整 completed key，30 次交錯測試 p50 **83.58→30.70 ms**、
  p95 **238.79→154.55 ms**。這是來源識別，不是模型推論或開盤成交耗時。
- v40 的完整 Factorized daily／full cell oracle 已完成且相同，但最後
  unchanged owner 被磁碟容量 admission 擋住，原 run 為 failed；v60 等待超時
  也保留。10/21 的 v89 已成功補跑 unchanged、全部 projection cells 與獨立 BASE
  配對；原失敗標記 superseded，沒有改寫原收據。這是原固定 code graph 的
  真實來源工程驗收，不是後續 freshness 新版本的正式服務准入。
- 最新 53 檔相依回歸 `daily-lineage-final-v98-20261020T182051-225e661f`：
  **1,957 passed / 146.55 s**，另有 Ruff／`git diff --check` 通過；不是全庫測試，
  不與較早重疊集合加總。
- 新增上游完整合併的精確 session projection，以及資料監控、公開／私有發布端
  的 manifest 相容與變動隔離。`--incremental-merge-cache` 明確 opt-in，既有排程
  不因有 parser cache 而自動切換。完整歷史與原始 SHA 收據不縮減。
- 新增日初版實測 **107.058 s，反而慢於 full 41.487 s**，已保留此負面結果；
  根因是先解析全歷史／建 parser cache，才判斷 suffix。已改成先驗證 prefix、
  只解析受影響價格，並行驗證獨立來源；新版真實每日測速見第五階段。
- v87 完整真實來源驗收通過：同輪全量 **57.287 s**、新增日 **54.188 s**、
  未變來源完整讀回 **22.516 s**。新增日 CPU 105.803→81.827 s（約減 22.7%），
  wall 只減約 5.4%，不宣稱大幅優於同輪全量。完整 9,030,856 列／41 欄、
  原始收據與診斷，均等於修改前 v79 的固定 oracle；沒有只測末日或熱快取。
- 本輪上游／監控／發布／還原相關回歸，最新為
  `parquet-catalog-scope-v88-20261020T102331-9a3c2584`：
  **702 passed / 71.36 s**；完整監控組另為 **319 passed / 16.01 s**。
  不是全庫測試，也不與其他重疊集合加總。
- 10/21 已補齊 wide／all-observed／FinLab stage、cold inventory 與 remote
  readiness 的分區來源身分：新 `source` 欄位保留每個原始檔案 SHA，舊
  `sha256` 欄位不改義。market freshness 已支援 logical source，且每次核對所有
  舊分區；1.34 GB 未變 research staging 的真實 ABBA，新版 1.618／1.673 s、
  原版 2.119／2.779 s，避免每次重寫 1.34 GB。詳見第六階段；正式 writer 未切換。

## 邊界與驗收

來源採集、來源差異判定、特徵／模型狀態、帳本、網站投影、瀏覽器傳輸是六個
獨立階段。重用壓縮檔、熱快取或預設只看一天，均不能證明 producer 真正增量。
首建允許完整掃描；普通新增日只轉換新分區；舊資料修訂必須使相應分區／有狀態
後綴失效。未知相依或損壞則保留最後已驗收版本，不能直接視為 append。

驗收至少包含：新增日、補舊日、同日修訂、刪除、原檔替換／截短、部分寫入、
重啟、並行讀寫、ABI／清單變更，以及與同來源全量重建的完整輸出比較。
每段分別記錄來源讀取 bytes、解析 rows、重建／重用 sessions、輸出 bytes、wall。

## 目前清冊與 canonical owner

| 網站／服務 | 入口／來源 | 現況與本輪目標 |
| --- | --- | --- |
| 首頁 | `serve_public_dashboards.py:build_public_overview` | 小型來源摘要；不能每次掃原始歷史 |
| 當沖（全部模式） | `tw_day_trade_dashboard.py`；signals/events/marks 帳本 | 已有位元組日期索引、逐日投影；消除多日補入的全量退路與重複解碼 |
| 隔日沖 | 同一 dashboard builder，獨立 state root | 與當沖同修；不混用資金、日期與出場規則 |
| 期貨／選擇權 | `taifex_volatility_dashboard.py`、`taifex_history_segments.py` | 已有 base/delta；仍需整段來源 prefix SHA，不能把它稱為 O(delta) I/O |
| 永豐 API／儲存 | `shioaji_api_dashboard.py` | 已有 receipt metadata cache；完整來源涵蓋與 freshness 另驗 |
| FinLab | `finlab_dashboard.py` | 狀態／每 key receipts；未變來源不重複解析，改動 key 獨立失效 |
| FinMind | `finmind_dashboard.py` | sampler／ETA／receipts；不在網頁請求呼叫 provider |
| 新聞 | `news_dashboard.py` | 讀 canonical 狀態，保留更新時鐘；不重掃新聞 payload |
| TEJ | `tej_dashboard.py` | 唯讀 queue SQL＋狀態；新增任務不重算舊歷史正文 |
| OpenBB | `openbb_archive_dashboard.py` | 已有 JSONL append reader；補強同 inode 修訂、截短及部分寫入 |
| 全資料監控 | `data_monitor_dashboard.py` | 完整註冊清單，未變 receipts 不重複解碼；不能以 HTTP 200 宣稱資料完整 |
| 流量／效能 | `PublicPerformanceHistoryStore` | 已有日期分檔與 resident rollup；保留真實測速與讀回，不另寫一份全歷史 |

目前啟用的當沖 market（由 `services/discord_bot/markets/*.yaml` 核對）：

- `tw_day_trade_100m`
- `tw_day_trade_multi_basis`
- `tw_day_trade_multi_basis_22`
- `tw_day_trade_v8_annual_log_cash`
- `tw_day_trade_factorized_scale_separated_cash`

前四者共用 `finalize_tw_public_completed_session.py` 與 canonical panel／public
feature builder；第五者另接 `update_tw_day_trade_factorized_live.py`。因此必須
同時處理共用特徵層與 Factorized 狀態續算，不能只優化第五模式或網站顯示。

## 分層實作順序

1. 來源狀態採有界、完整 metadata generation 綁定的共享 JSON snapshot reader；
   命中不重讀檔／重解析 JSON，單一檔案變動只失效自己，回傳值隔離防止呼叫者改壞快取。
2. 所有當沖／隔日沖歷史沿原逐日 shards；補入／修訂／刪除只處理對應 session，
   不因變更超過固定日數而全量重建。完整輸出合併成本與 producer 重算分開計時。
3. canonical public feature producer 的 changed-source partition proof：證明最早
   受影響可知日，保留足夠上下文；未知來源、日期解析或生命周期變更 fail closed。
   不能僅放寬既有 SHA gate 或固定重算 14 天。
4. Factorized 保存原始 Float64 value、觀察時鐘、report-period floors、NULL barriers
   及生命周期／候選欄位狀態，綁定來源前綴；普通新增日只續算新日，歷史修訂從
   相容 checkpoint 重建後綴。Float32 展示結果不能作為狀態 checkpoint。
5. 實際每日排程、網站傳輸與瀏覽器合併驗收；舊完整歷史仍可選取。冷首建與
   普通增量分別量測，未完成的層不列為已部署／已達極限。

本文件是範圍／設計與驗收清冊；各層完成狀態以後續測試、benchmark、原生
owner 讀回收據為準，不以本清冊宣稱全部已完成。

## 本輪已實作，但尚未提升正式服務版本

- 六種來源監控共用 `dashboard_updates.read_json_snapshot`；512 筆／32 MiB
  有界快取採每來源 single-flight、完整 dev/inode/size/mtime/ctime 圍籬。
  只由本程序已解析的 JSON 產生暫存 marshal packet，以 C 層複製隔離呼叫者修改；
  **不接受外部 marshal、不落盤**。這是 reader 最佳化，不是上游採集增量。
- 當沖五模式與隔日沖共用逐日投影不再因一次補入超過八天或刪除舊日而全建。
  lineage 防止同跨度修訂沿用舊 digest；同 inode 的「改舊日＋追加」先核對前綴 SHA。
  普通 append 只解析新列，但仍有前綴 I/O；未宣稱它已是 O(delta) I/O。
- OpenBB JSONL 完整前綴驗證後只解析新增完整列；修訂、截短、替換與半列另驗。
- 全模式共用 `build_tw_public_training_features --incremental-auto`，由逐日 typed
  Arrow proof 找最早新增／修訂／刪除日期，不只固定重建十四天。
  12 種股市來源與 5 種 TAIFEX 來源有明確的日期契約；不支援的日期／排序、
  schema、全域規則或來源清單變更仍全建。TAIFEX 的 `Date`／`最後結算日` 是
  特徵交易日；`date`／`_as_of_date` 可能只是下載日，不能互換。
- 保留長期停牌前的實際上一筆收盤價，不把補入上下文列誤算為當前資格清單。
  ISO 日期證明後才用字串比較；其餘保留原日期解析。全輸出包含 **162 欄**，
  其中 143 個 feature，另有鍵與規則欄；不是只驗證展示用欄位。
- builder 的 summary 綁定實際四個程式依賴與 Polars 版本；載入後或建置中
  相依改動會拒絕發布，不把舊程式的結果標成新版本。未納入無關新聞程式。
- Factorized 沿 canonical causal sweep 增加 lossless preparer checkpoint：
  Float64 value、int32 觀察時鐘、int64 availability count、report/event floors、
  NULL barrier 與候選欄位都保留。普通新增日重用已封存前綴，重算舊末日與新日：
  舊末日的 provisional 下一 session lifecycle 不能直接視為永遠不變。
  兩種 state 與所有 donor members 必須先完整驗證；遺失／損壞／舊資料修訂全建。
  無 optimizer/model checkpoint 變更，也無交易／帳本回寫。

`/traffic/api/status.daily_incremental` 增加來源解碼／命中計數、最近當沖與隔日沖
shard 的 load/rebuild/merge/wall 及重建／重用日期。沒有內部路徑；尚未觀察時是
空物件，不編造零延遲。它是程序內診斷，重啟歸零；既有 minute performance
history、逐次 producer summary／Factorized receipt 與 benchmark receipt 才是
持久證據。尚未將新工作樹版重新啟動到正式 gateway。

## 重測工具與結果

```bash
source scripts/runtime_env.sh
stockagent-agent run --name daily-delta-probe tw-opening-latency-20261015 -- \
  run_fintech_python scripts/benchmark_dashboard_daily_incremental.py \
  --output artifacts/benchmarks/dashboard-daily-incremental-FRESH-RUN \
  --sessions 640 --symbols 256
```

輸出必須是全新私有目錄。工具先做容量入場；上列預設命令全部為 synthetic
來源，無 provider 請求、GPU、正式資料發布、帳本修改、下單或服務重啟。
第三階段另加須明確指定 `--real-public-root` 的正式來源唯讀 stage，區塊與收據
仍只寫私有 benchmark 目錄。完整輸出 oracle 與其耗時另記，沒有以抽樣、刪欄
或少算交易日冒充相同工作。

v5／v6／v7 同形狀三次的 wall 中位數（毫秒）：

| 實際測量範圍 | 全量 | 增量 | 驗證與限制 |
| --- | ---: | ---: | --- |
| 64 來源、三次完整 JSON 讀取與序列化，包含首次填入 | 291.82 | 305.59 | 解碼 192→65；首次填入未證實較快，保留負面結果 |
| canonical public feature，640 日×256 檔再加一天 | 1700.65 | 1174.66 | 重用 163,840 列，只轉換新增 256 列；162 欄／所有 NULL 與值相同；仍重寫整個 Parquet |
| 32-channel causal sweep＋state 壓縮讀寫＋完整 dense 合併 | 885.49 | 182.71 | 重用 639 日、重建 2 日；所有 Float32 bits/count 相同；**不是完整 Factorized producer 耗時** |

原始 receipts 在 `artifacts/benchmarks/dashboard-daily-incremental-20261020-v5`、
`v6`、`v7`，不入 Git。v1／v2 的物件深複製／錯誤容量估算造成反覆淘汰，已拒絕；
未刪除那些負面 receipts。v3／v4 的小型 producer 也沒有穩定 wall 改善。

v8 起把來源首次填入與後續更新分開量測，均包含完整 JSON 序列化。v9 的後續
更新為 288.44→234.49 ms、解碼 192→1／命中 191；僅是該次合成負載，不是公網
端到端或每天固定延遲。v9 也修正舊 benchmark 的 `all_columns` 命名誤差：143
是 feature_count，完整檔案實際是 162 欄；舊 oracle 本身一直比較完整 DataFrame。

最後加上完整 code/ABI 圍籬後，v10 另行重測：public feature 全量 2294.70 ms、
增量 1348.12 ms；steady 來源讀取 363.17→278.01 ms；causal stage
969.83→196.65 ms。這是工作站當時負載下的單次最終版測量，不與前三次中位數
混算。receipt：`artifacts/benchmarks/dashboard-daily-incremental-20261020-v10/receipt.json`。

## 第一階段驗證

本輪分組回歸（組間重疊，不加總成全庫測試）：

- 305 個 producer／Factorized state、native panel、reuse/member 測試。
- 705 個網站／監控／共用 reader／排程整合測試。
- 新增前綴 proof 後 250 個曲線／索引／逐日投影回歸。
- 加入 gateway 診斷與 canonical `--incremental-auto` 排程後，293 個回歸。
- 55 個 Node 測試：共用 UI、預設最新日、日期切換、取消舊請求、保留完整資料。
- 最終 source/dependency ABI 圍籬與建置中版本變動測試加入後，351 個整合回歸
  通過（`public-daily-final-abi-20261020T033626-dcbb0750`，71.68 秒）。
- 聚焦 Ruff、`git diff --check` 通過。正式程序 invocation/PID 未變；本機 healthz
  與公網當沖 HTML 各讀到 HTTP 200，**只證明可達，不代表資料／交易／新版本已就緒**。

## 第二階段實作：BASE、原檔正規化與整代容量

本次在第一階段後繼續實作，仍未提升為正式服務版本：

- 個股 BASE 增加 `official_symbol_exact_prefix_state_v1`。同來源／程式版本下，
  驗證完整原始輸入前綴、既有輸出 SHA 與 state checksum，才延續原始 Float64
  recurrence。保存的是內部狀態，不從最後一列展示價格猜狀態；最後一列缺值
  也能正確續算。歷史修訂、刪除、checkpoint／輸出損壞及實作改變均失效。
  完全未變且 checked-through 相同的標的不重寫檔案；checked-through 改變則
  仍更新正式 metadata，不把舊 freshness 假裝成新日期。
- Yahoo 歷史回補區段有後來官方邊界使整段失效的依賴，目前仍完整驗證，不能
  套用官方純前綴的簡單續算。完全沒有 fallback 的標的略過無用途的 Yahoo
  歷史逐列搜尋；return quarantine 仍依完整序列重建，包含受下一日影響的前一列。
- BASE report／summary 持久記錄 proof、normalization、adjustment、assembly、
  Parquet write、output proof，並保留重建／重用列數。worker 時間加總不當成
  平行總 wall；上游原始檔驗收／merge 時間仍獨立存在。
- FinLab 日／週資料在寬表轉長表**之前**依原始可知日期篩選，並讓 Parquet reader
  下推原始 index 篩選。這只用於沒有 issuer publication bound 的獨立日期來源；
  季報、月營收的版本 floors 及延後發布資料仍保留完整上下文。
- 未變動的 FinLab 原檔重用同一 canonical generation 中的 sealed normalized
  events。完整 raw SHA、definition、symbol 軸、native context、normalizer／runtime
  與日曆都須一致；未決發布日期或未來 upload/bound 不能因換日而直接命中。
  原檔變更仍走原始 normalizer。NULL、負零、period 及 source-part precedence
  不變，每個新 member 仍納入完整 SHA／讀回驗證。
- Factorized progress receipt 改成首筆立即持久化、其後每 64 個 member 或下一次
  超過兩秒的更新批次提交，common／dictionary／最終 stats 強制 flush。
  130 個連續 member 的確定性測試由逐筆提交降為 4 次；硬中斷最多失去未提交
  batch 的重用機會，不會把未封存檔當成完整 generation。
- `prepared_capacity.json` 預設每 namespace 128 GiB、每新代預留 16 GiB，並共用
  原本 25 GiB filesystem／實際 Windows backing reserve。整次建置持有 kernel
  lease；所有 private generation 寫入都通過累積預算，超出先拒絕，再保留最後
  已驗證指標。舊嚴格 `cache_capacity.json` schema 沒加欄位，避免混版讀取失敗。
  不自動刪除來源、舊代或失敗工作；達容量上限是 `capacity_deferred`，不是 ready。

本輪容量唯讀觀察（2026-10-20）：prepared namespace 實體配置
7,795,421,184 bytes，bounded inventory 完整、filesystem 可用約 79.90 GiB。
這是容量觀察，不是實際 backing-volume reservation 或正式新代建置驗收。

第二階段重測工具另包含 BASE 與真實 canonical FinLab normalizer。v11／v12 是
640 個合成歷史日、256 標的、32 個 causal channels，非 09:00 provider／成交 SLA。

| 範圍 | v11 全量→增量 | v12 全量→增量 | 限制 |
| --- | ---: | ---: | --- |
| FinLab daily normalizer | 201.39→89.53 ms | 411.47→73.36 ms | 不包含 raw 全檔 SHA |
| BASE per-symbol build | 13.84→13.26 s | 28.60→14.78 s | 完整 proof／寫入；不含上游 merge |
| 未變 BASE 輸出 | 未獨立測 | 428.29 ms | 仍驗證原始值與輸出 SHA |
| Public feature producer | 1846.10→1951.37 ms | 1572.85→1135.27 ms | v11 是負面 wall 結果，未隱藏 |

v12 的 BASE 增量中，檔案寫入占 11.38 s、輸入 proof 1.67 s；recurrence 本身
只 0.15 s。CPU 則全量 3.65→增量 4.60 s，證明「少重算」不等於整段 CPU 也更快。
這兩輪有系統負載與 I/O 差異，不以最漂亮的一筆宣稱穩定兩倍改善。
receipts 位於 `artifacts/benchmarks/dashboard-daily-incremental-20261020-v11/receipt.json`
及同層 `...-v12/receipt.json`；保留完整值／NULL／位元 oracle 與實作雜湊。

最終程式凍結後另跑 v13，沒有與本輪回歸測試同時執行；工作站仍有其他正常工作，
不是隔離實驗室。Polars 與 Arrow 均觀察到 2 個執行緒，無 process CPU quota。
完整 code hashes／runtime 及各段 oracle 留在
`artifacts/benchmarks/dashboard-daily-incremental-20261020-v13/receipt.json`。

| v13 實測範圍 | 全量 | 增量／重用 | 結論 |
| --- | ---: | ---: | --- |
| 來源摘要後續更新，含完整 JSON 序列化 | 280.71 ms | 217.07 ms | 解碼 192→1；不代表 provider 變快 |
| Public feature 完整 producer | 1559.51 ms | 1173.83 ms | 完整 162 欄／NULL 相同，仍整檔重寫 |
| BASE per-symbol build | 7012.18 ms | 9729.57 ms | **負面整段結果**，不能宣稱 BASE 已加速 |
| 完全未變的 BASE 輸出 | — | 363.39 ms | 原始值／輸出 SHA 仍完整核對，不重寫 inode |
| FinLab daily normalizer | 170.28 ms | 59.40 ms | 不含 raw 全檔 SHA；所有 typed 值／NULL 相同 |
| Causal sweep＋state I/O＋完整 dense 合併 | 1025.87 ms | 153.62 ms | 所有 bits/count 相同，不是完整 Factorized producer |

v13 BASE 的 163,840 列 adjustment 前綴確實重用，只重算新增 256 列；但增量
CPU 是 4.36 s，高於全量 2.88 s。輸入 proof 1.73 s、整檔寫入 6.38 s，
recurrence 只有 0.14 s。這個 checkpoint 的正確性與「未變輸出快速重用」已驗收，
**新增日整段效能尚未驗收為改善**；不因前兩輪較漂亮的 wall 就忽略本輪退步。
下一步應先改來源分區證明／reader 相容的分區輸出，再重測整段，而非再疊加計算快取。

### 第二階段驗證

- 最終凍結版本以全新程序跑 24 個測試檔：**623 passed，93.73 秒**。
  收據：`daily-second-stage-final-v3-20261020T043107-30a1cb57`。
- 包含 BASE append／歷史修訂／刪除／損壞／缺值／episode、正規化日曆延伸／
  未決發布／issuer bound、原生 Factorized 全建對照、容量寫前拒絕／last-good
  pointer 保留、native serializer 包裝錯誤仍回傳 typed deferral，以及 panel／
  annual cache／source guard／live inference 回歸；不是全 repository 全測試。
- 較早一輪 2 個失敗來自測速工具寫死舊函式簽名，改為受限 AST 結構檢查；另輪
  8 個失敗因測試中相依程式仍在修改，版本防護正確拒絕混版。均保留原收據，
  已以 v3 追加 superseded 處置，未放寬 `LoadedImplementationChanged`。
- v13 合成測速成功且各段完整 oracle 通過；聚焦 Ruff 與 `git diff --check`
  通過。沒有 provider 呼叫、GPU 工作、正式發布、帳本修復或交易服務重啟。

## 第三階段：精確來源區塊證明與 canonical parser 重用

第二階段的負面結果顯示，應先減少資料搬移與持久提交，不能只再優化已經很短
的 recurrence。本階段依下列順序實作，沒有全面重寫前端或另建一條採集路徑：

1. 完整來源 SHA 仍照原契約驗證；另保存原始 Parquet 壓縮區塊及其解碼 metadata
   的精確證明，跳過已驗證前綴的重複**解碼**。這不是只比檔案大小／時間，也
   不用 footer 的 min/max 統計猜交易日。前綴最後一組及跨組的整個邊界日必須
   重讀；排序、schema、群組布局、日期時鐘或證明不相容則保守回到原路徑。
2. 把原本 TWSE／TPEx 的逐列解析函式抽出，供原完整 reader 與區塊重用 reader
   共用。只重用 row-local 的原始欄位解析；merge、去重、生命周期、Yahoo
   fallback、quarantine 與跨日調整仍由原 owner 完整處理，不能按區塊分別計算。
3. 每個新區塊仍有 typed roundtrip、完整 SHA、來源 generation 圍籬及封存檔案。
   writer lock 保護 inventory；全部必要區塊完成後才原子提交有界 index。
   底層沿用容量／backing-volume reservation 與逐次寫入預算；損壞只失效，不
   刪原始資料、舊區塊或失敗證據。命中不需要新的 payload allocation。
4. BASE 所有檔案仍逐一做 payload fsync／atomic replace；由 canonical batch
   owner 等所有 worker 結束後，只對共同輸出目錄 fsync 一次，完成後才能發布
   summary／state。例外時仍等待 worker 並處理目錄持久性；獨立單檔呼叫保留
   原本逐檔目錄提交。這不是取消資料持久性。
5. 小型 Arrow IPC／schema proof 改由 coordinator 先算，再交 worker 做檔案工作，
   避免多執行緒爭用 GIL；proof 自身耗時仍完整計入，不移出測速範圍。

編碼區塊及 row-group reader 的設計依據見
[Parquet column chunks](https://parquet.apache.org/docs/file-format/data-pages/columnchunks/)
及 [Arrow Parquet reader](https://arrow.apache.org/docs/python/parquet.html)。
來源 writer 重新分組會形成安全 miss；因此本階段減少的是重複解碼，不宣稱來源
I/O 已達 O(delta)。TAIFEX 會丟棄 null 日期的稀疏時鐘仍留在完整 proof 路徑。

### 第三階段測速與負面結果

CPU 診斷 `base-whole-profile-v14-20261020T044320-9d69827f` 的小型負載中，
390 次 fsync 占 4.12 s，其中 192 次共同目錄提交占 2.00 s。這是定位瓶頸的
診斷，不是批次提交改善倍率的正式對照。工具保留 `--profile` 與獨立 stage
選擇，正式量測不與這份 CPU profile 混算。

v16 改用八個 worker 後，BASE 全量 3646.66 ms、增量 5648.47 ms，仍退步；
因此再調整 proof 排程。最終程式凍結後的 v18，仍以相同八個 worker 比較，
來源與輸出都完整保留。640 日、256 標的、32 個 causal channels：

| v18 實際範圍 | 全量 | 增量／重用 | 完整性與限制 |
| --- | ---: | ---: | --- |
| 全來源前後 SHA＋所有 typed session proofs | 396.52 ms | 115.43 ms | 6 組重用 4 組、解碼 2 組／33,024 列；8.49 MB 全來源仍驗證 |
| 原 TWSE row-local parser＋前後 SHA＋新區塊寫入＋完整合併 | 508.92 ms | 59.61 ms | 重用 163,840 列、解析 256 列；完整 11 欄相同；不含整體 official merge |
| Public feature 完整 producer | 1699.24 ms | 1077.18 ms | 完整 162 欄相同；仍重寫完整 2.07 MB Parquet |
| BASE 全部個股 proof／轉換／完整檔案寫入 | 4002.90 ms | 4163.69 ms | **仍未證實新增日整段變快**；CPU 6.14→7.77 s，不隱藏負面結果 |
| 完全未變 BASE 輸出驗證 | — | 481.79 ms | 不重寫檔案，完整輸入／輸出 SHA 保留 |
| FinLab daily normalizer | 116.04 ms | 46.20 ms | 原始時鐘／typed 值／NULL 相同，不含 raw 全檔 SHA |
| Causal sweep＋lossless state I/O＋完整 dense 合併 | 1101.97 ms | 358.31 ms | bits/count 相同，**不是完整 Factorized producer** |
| 來源摘要後續更新＋完整 JSON 序列化 | 327.92 ms | 251.25 ms | 解碼 192→1，不代表外部 API 延遲降低 |

TWSE 區塊 cache bootstrap 為 1067.29 ms，與 59.61 ms 的後續新增日分開記錄。
BASE 重用 163,840 列 adjustment、重算 256 列，但變更的個股仍整檔寫入。
worker 階段時間加總包含平行等待，不當作整段 wall。Polars／Arrow 各觀察到
2 個執行緒，不是 CPU quota；其他正常系統工作未停用，這不是隔離實驗室。

收據：`artifacts/benchmarks/dashboard-daily-incremental-20261020-v18/receipt.json`；
run：`daily-third-stage-v18-all-20261020T044950-0df610b3`。以上均是合成來源、
同來源完整 oracle，不是正式每日排程、09:00 行情或模擬成交 SLA 的驗收。

### 正式來源唯讀驗證

另以 `/srv/stockagent-live/data_tw_public` 的固定原始來源執行完整 canonical
`_official_frame`，沒有傳入 legacy／Yahoo fallback 參數。TWSE 5,348,897 列、
TPEx 4,433,181 列，共 **9,782,078 列／81 個 row groups**。三輪輸出均為
**9,028,545 列、41 欄**；整份有序 typed 值／NULL／位元 SHA、原始 source
receipts 與 merge statistics 完全相同，結束時再次核對兩份原始檔完整 SHA。

| v19 同一正式來源 | 原完整 parser | 區塊首次建置 | 區塊後續重用 |
| --- | ---: | ---: | ---: |
| 完整 canonical 合併 wall | 101.493 s | 153.082 s | 59.480 s |
| 合併程序 CPU | 187.241 s | 225.917 s | 104.624 s |
| 獨立完整 typed 輸出 oracle wall | 18.772 s | 23.812 s | 25.375 s |
| 合併＋獨立 oracle wall | 120.265 s | 176.894 s | 84.855 s |
| 原始列重新解析 | 9,782,078 | 9,782,078 | 0 |
| 區塊重用／新建 | 0／0 | 0／81 | 81／0 |

後續重用的合併 wall 減少約 41.4%、CPU 減少約 44.1%；**首次建置更慢**，
與後續重用分開記錄。獨立全量 oracle 也較慢，沒有將其成本藏到測速之外；
連同 oracle 的完整驗證工作由 120.265→84.855 s，約少 29.4%。本次沒有
冷洗 OS page cache；三輪順序固定，沒有 ABBA 重複測量，改善幅度仍是本機
單次觀察。後續重用仍讀取／驗證 326,421,097 bytes 原始壓縮區塊、所有原始
來源收據與 cache members，**不是零 I/O、不是已驗收的每日 append**。

上述結果直接證明：正式大資料可以跳過重複解析，但完整 merge／lifecycle
仍需約 59 秒；若要接近逐日新增成本，仍須下一階段的 reader-first 分區遷移，
不能只把這個 warm reuse 當成全管線已增量。新區塊寫入有實際 filesystem／
Windows backing admission；正式來源、feature 檔、帳本與服務均未寫入／重啟。

重測命令：

```bash
source scripts/runtime_env.sh
stockagent-agent run --name native-official-source-merge tw-opening-latency-20261015 -- \
  run_fintech_python scripts/benchmark_dashboard_daily_incremental.py \
  --output artifacts/benchmarks/dashboard-daily-incremental-NATIVE-FRESH-RUN \
  --stages real_official_source_merge \
  --real-public-root /srv/stockagent-live/data_tw_public
```

收據：`artifacts/benchmarks/dashboard-daily-incremental-20261020-native-v19/receipt.json`；
run：`native-official-source-merge-v19-20261020T045050-b388f9c9`。
`accepted_captured_source_stage_oracles_not_daily_or_live_readiness` 只接受本次固定
來源 stage 的完整比對，不表示每日排程或盤中執行就緒。

### 第三階段最終回歸與操作邊界

- 程式凍結後，以全新程序執行 **28 個測試檔、822 passed，112.14 秒**。
  run：`daily-third-stage-final-v20-20261020T045752-6c37d72d`；包含前兩階段的
  BASE／public／Factorized／容量／panel cache 回歸，以及 source group cache、
  來源監控 snapshot、canonical 資料層與官方排程／發布整合。這不是全庫測試。
- 新增 Parquet layout／schema／實作變更、append／修訂／刪除、session 跨組、
  損壞 checkpoint、source 部分寫入、封存 cache member 損壞仍保留原檔、
  row-changing parser 拒絕發布，以及 batch 全部 payload fsync／單一目錄提交
  與目錄提交失敗拒絕返回可發布結果的測試。
- 聚焦 Ruff 與 `git diff --check` 通過。原 v14 測速工具缺少 `pyarrow.parquet`
  匯入的失敗保留，已由 v18 完整測速追加 superseded 處置；沒有放寬
  `LoadedImplementationChanged`、來源 SHA 或任何交易／容量防護。
- 最後唯讀核對 Discord、當沖、隔日沖、網站四個主程序的 invocation/PID，
  與本輪開始的觀察一致，沒有因本輪重啟。`active/running` 只是程序存活，
  不代表 artifact maintenance、minute curves、行情、完整資料或新版本已驗收。
- 新檔只增加共用來源 proof／row-local projection、可重測工具、語意測試與本報告；
  benchmark receipts／cache members 位於已忽略的 `artifacts/`，沒有納入 Git。
  原始來源、正式 feature、帳本及既有歷史沒有由本輪覆寫或刪除。

## 第四階段：相容 reader、BASE suffix 與分區成本

本階段先建立 reader ABI，再增加 **opt-in** writer；不把正式 `.parquet` 偷換成
目錄，也不保留一份舊檔讓不同 consumer 讀到不同日期。正式輸出預設仍為 `file`。

### 已實作的契約

- 共用 `stockagent/data/partitioned_parquet.py` 的
  `ordered_partitioned_parquet_v1`：顯式有序 manifest、Date32 年／月分區、
  原始 schema／metadata、每個不可寫 member 的完整 byte SHA 與 typed value SHA。
  即使查詢只選一天，也驗證整代全部 members；禁止 legacy 與 manifest 同時存在。
- canonical panel、Factorized preparer、邏輯 OHLCV reader、官方資料 audit／rebuild
  已接入相容 reader；panel 保留查詢起點前的持續性 lifecycle／rule 事件，
  不為少讀歷史而改變 masks。這不是所有 live consumer 的遷移完成證明。
- 個股歷史價格頁已支援 manifest 的代號解析、投影與完整 byte proof；cache hit
  仍驗證 immutable members，不以價格 map 的快取繞過損壞檢查。legacy cache key
  另外綁定 dev／inode／ctime，避免同大小、保留 mtime 的修訂仍顯示舊價格。
- public table audit 與 build receipt 已支援邏輯／實體 manifest 路徑，摘要仍位於
  同一 logical `.summary.json`；完整 pointer SHA 加上全部 member proof 才算有效。
- BASE writer 新增 `--output-layout year`。不可變 payload／generation archive
  留存；只在 payload／archive fsync 後切換 manifest，batch join 後提交父目錄。
  合併重複 durability barrier，不省略持久化。沒有清除任何舊 generation。
- 精確原始前綴、implementation、state checksum 與完整舊輸出 SHA 全部成立，
  且沒有 Yahoo fallback 時，才恢復 recurrence checkpoint，只轉換最後上下文列與
  新列；檔案模式仍完整序列化輸出，分區模式只重建邊界年。修訂／刪除／規則或
  schema 變更不能套用 append 捷徑；缺證據即回完整 canonical build。
- Factorized 的 `already_current` 與提交前驗證新增完整 BASE 檔案清單、行情、
  corporate-action reference／entitlements／raw manifest 來源指紋；避免只有
  public feature table 不變，卻漏掉價格或除權息修訂。沒有放寬來源驗收。
- 未變分區的統計只記本次零寫入，CSV 中 nested timing 以 scalar JSON 輸出；
  分區標的清單縮減必須明確退役，先拒絕發布並保留資料，不自動刪除不可變歷史。

### 實測：少寫 rows 不等於整段更快

`dashboard-base-partitions-20261020-v35/receipt.json`：3,200 sessions × 128 symbols、
8 symbol workers，包含 canonical per-symbol transform、寫入與來源／輸出 proof，
**不包含上游 `_official_frame` 全歷史 merge**。不是正式每日排程測速。

| 同來源案例 | wall | 結論 |
| --- | ---: | --- |
| 獨立完整 per-symbol 建置 | 3.054 s | 比較基線 |
| 檔案模式精確前綴＋新增日 | 2.632 s | 本次 wall 約少 14%；CPU 未減少，不能外推整段增益 |
| 檔案模式未變、完整 proof | 0.650 s | 明確是未變命中，不冒充冷建／新增日 |
| 年分區新增日 | 8.618 s | **負面結果，不切換正式 writer** |
| 年分區未變、完整 proof | 1.856 s | 小檔案 proof／footer／manifest 仍有成本 |

v35 的完整基線使用原逐檔目錄提交，不能把差異全歸因於 suffix。後續
`dashboard-base-partitions-20261020-v45/receipt.json` 增加**相同批次提交、workers、
完整 input／output proofs 及 checkpoint 建構**的全量基線：全量 4.645 s、
新增日 2.667 s；CPU 分別 4.590 s／5.343 s，沒有 CPU 節省證據。這是在共享主機
上的單次 wall 觀察，不外推穩定百分比或整段 BASE 的加速。年分區仍為 8.156 s。
相同政策的完整全量、新增日與年分區均有全部 rows／欄位的獨立比對。

年分區新增日寫 128 個 partitions／35,840 rows，重用 1,024 個 partitions／
373,888 rows，所有欄位、NULL、Float bit 與排序均與完整建置相同。但「標的 × 年」
仍產生很多小檔案，hash／footer／serialize／fsync 的固定成本高過省下的列運算。
單 worker profile 曾量到 3,342 次 fsync 累計 44.509 秒；合併重複提交後仍未勝過
原檔模式。下一步必須測跨標的月／年分區與共享 snapshot，而不是逕自增加 workers。

### 測試與真實 Factorized 每日驗收

- 最新相依回歸：`daily-source-fences-v36-20261020T053358-ebf46834`，
  **362 passed / 83.18 s**。包含 reader ABI、BASE canonical／增量、完整 panel
  masks、public audit／rebuild、Factorized identity／causal state／normalization／updater。
  不是全庫測試，也不是正式服務部署。
- BASE 分區退役／統計／CSV 修正後，
  `daily-base-safety-v37-20261020T053705-5686b760`：**240 passed / 42.37 s**。
- 擴大至 33 個相關測試檔：
  `daily-broad-regression-v38-20261020T053840-9891d806`：**981 passed / 133.45 s**。
  其後 fixture 修正由 `real-daily-fixture-v39-20261020T054133-839e971a` 驗證
  **44 passed / 7.59 s**；public manifest audit 由
  `public-manifest-audit-v41-20261020T054234-050013b2` 驗證 **174 passed / 18.67 s**；
  個股價格相容與既有 live helpers 由
  `stock-history-reader-v43-20261020T054406-edb91915` 驗證 **138 passed / 18.02 s**。
  這些集合互相重疊，不相加冒充更多獨立測試。
- 完成本階段 reader／audit／個股歷史頁後，以新程序再跑 34 個相關測試檔，
  `daily-reader-final-v44-20261020T054550-5868c189`：**1,076 passed / 125.83 s**。
  公平 BASE probe 另外由 `base-fair-probe-test-v46-20261020T054828-d95a76d5`
  驗證 **48 passed / 8.97 s**。
- 新增離線 `scripts/benchmark_factorized_daily_owner.py`，直接呼叫原生 `prepare()`，
  不複製核心演算法。捕捉完整 BASE、公用 features、350 個 FinLab receipt-bound
  原檔與原始 dictionary／annual events，再釋放正式來源 read lease。
  接續使用私有副本測前日全建、新增一交易日、獨立全建、所有 BASE arrays／masks、
  所有 individual／common cells 的位元比對，以及未變完整驗證。
- v34 已保留可獨立 SHA 讀回的完整來源；發現 BASE dependency fence 缺漏後，
  只停止本輪 benchmark，修正後由 v36 重新驗收並重用同一份不可寫來源，避免換樣本。
  來源是 2026-10-16／2026-10-19 的**回溯子集 fixture**，不聲稱為歷史 PIT release。
- 真實 v36 run：`factorized-real-daily-v36-20261020T053402-a4a6e989` 已在前日
  BASE 冷建期間停止，只保留診斷證據。原因是 cutoff fixture 的 `stock_rows`／
  `market_rows` 與每檔 `last_date` 仍沿用最新日期／筆數，驗收工具本身需要修正。
  原始價格與 capture 沒有修改；這一輪沒有產生完整驗收 receipt。
  已修正 fixture 每檔實際首末日期、空檔範圍與 stock／market 筆數核對；最新真實
  run 為 `factorized-real-daily-v40-20261020T054153-d122eaff`。
  **完整每日 oracle 尚未完成，不能列為已驗收。**
  私有來源捕捉／fixture 成本與 producer 成本分開紀錄；禁止 provider／CUDA 呼叫，
  容量 admission 與每代 write budget 均保留，沒有重啟服務／寫入帳本。
- 真實前日 BASE 冷建在 v40 量到 96.353 s：symbols 30.444 s、external rules
  40.692 s、assemble rules 19.352 s、cache save 5.644 s。這不是新增日的耗時。
  v40 的全量 Factorized oracle 可重用同來源 BASE cache，**不可把兩次命中相同
  BASE cache 當成獨立 BASE 重建**。因此另用同一測速工具的 `real_factorized_base`
  stage，以原封存最新來源、全新 cache 執行原生 `build_panel`／`subset_panel_symbols`：
  `factorized-independent-base-v47-20261020T055126-edb36c70`。它的全部 BASE arrays／
  masks 指紋必須與 v40 每日結果獨立一致，才完成這一部分驗收。
  這份獨立 BASE 已完成：**119.843 s wall／197.608 s CPU**，3,119 dates／
  2,757 symbols／6 BASE features，全 dataclass arrays／masks 指紋
  `b34b6ed06f1a250dcba7eeba02b27bf66b578aa5ce5c0d10dba1f4006e91d7b3`。
  receipt 的 `paired_daily_receipt_still_required=true`，不單獨代表 Factorized
  daily acceptance。離線 probe 與 fixture 測試由
  `base-oracle-test-v49-20261020T055259-fbf0a35e` 驗證 **52 passed / 14.01 s**。

### Live 快取來源識別補齊

- `completed_inference_cache.source_fingerprint` 與 live panel key 已改成相容
  legacy／explicit manifest；包含 pointer 與全部當代不可寫 members，不 glob 歷史
  parts／archives。外部 feature 的 summary 仍用 logical `.summary.json`，不誤找
  `*.manifest.summary.json`。
- 移除 `_LIVE_PANEL_SOURCE_KEY_CACHE` 的目錄時間捷徑；原地同大小修訂即使保留
  mtime，也必須以 inode／ctime 等 generation 變動失效。只列實際輸入，不再因
  無關 `status.json` 的目錄 churn 丟掉正確 panel。
- 舊分區損毀（即使位於 live tail 日期之前）、缺檔、legacy／manifest 並存皆拒絕
  讀取，不能讓 warm memory cache 掩蓋錯誤。
- `live-source-manifest-v50-20261020T055826-9da4cd99`：
  **210 passed / 25.37 s**；包含 source fingerprint、推論 cache／loaded-code
  identity、live helpers 及 partition corruption tests。與 1,076 集合重疊，不相加。
  來源 key 的額外 stat／路徑解析成本另行量測，不能用正確性修正冒充效能改善。
- 第一版完整檢查有顯著退步：真實 2,761 檔，live key p50 559.16 ms，completed
  key p50 305.40 ms；負面記錄為 `live-key-latency-v51-20261020T055858-a72a818b`。
  逐項 profile 證實多數 CPU 是 Path hash／排序／重複祖先解析，而非必要 stat。
  改共用單次 `scandir`＋每檔 stat，legacy 名稱保留原字串，只對 manifest 啟用原生
  全 member SHA／schema 驗證；不增加另一條 reader／collector。
- `live-key-string-v55-20261020T060243-c8433c11`：同樣實際來源，30 次，
  完整 live key p50 **34.22 ms**、p95 **47.05 ms**；completed p50 **36.33 ms**。
  舊 directory-only warm key 約 1.08 ms，但不能發現原地修訂，**不作為相同完整性
  的效能基準**。完整規則測試由
  `live-generation-typed-v56-20261020T060246-79e0ee13` 驗證 **211 passed / 26.61 s**。
- 公平比較改用 HEAD 原本就完整逐檔 stat 的 completed key（非上述錯誤快徑）：
  `completed-key-fair-v57-20261020T060404-b6127f4a`，30 次交錯執行，保留
  baseline AST SHA 與同 run current code guard。p50 **83.58→30.70 ms**、p95
  **238.79→154.55 ms**；兩版在固定來源上各自 identity 穩定。
  主機同時有原生全量 oracle 等工作，不能把單次結果當獨占硬體理論極限。

### 真實日流程的自動驗收鏈

既有 `scripts/benchmark_dashboard_daily_incremental.py` 增加 pair-verification mode：
讀取指定 canonical run 的 durable receipt，等待它實際 `succeeded / exit_code=0`，
失敗或超時立即拒絕，不重啟／繞過原 owner。

主流程完成後必須同時通過：

1. 原始 `prepare()` 前日、新增日、最新全量與未變完整驗證均完成；
2. 新增日／全量的每個 BASE dataclass field／array／mask，等於另一個 fresh cache
   冷建的 v47 reference，不只比較 digest 字串或最後一列；
3. 兩條流程使用同一份 retained capture，所有原始 byte SHA／readonly／ownership
   再驗，原生 owner code ABI 相同且沒有變更；
4. 再呼叫原完整 projection comparator，重驗所有 members 與 individual／common
   每個 Float32 bit，且結果等於主流程先前的 all-cell oracle；
5. 所有相依收據再讀回一致後，才寫入
   `accepted_real_source_daily_and_independent_base_not_pit_live_or_inference_readiness`。

配對與逐 cell 再驗的耗時另列 `verification_wall_ms`，不偷偷扣掉驗證以美化 producer。
此成功收據仍是離線真實來源副本／回溯 fixture 的工程驗收，不是歷史 PIT、模型品質、
即時訊號、paper fills、正式服務 promotion 或券商成交證明。

驗收鏈目前由 canonical run
`factorized-pair-watch-v60-20261020T060756-e343d4da` 觀察 v40。它不啟動另一份
producer、不改原 run；每 15 秒檢查 durable state，四小時仍未完成就明確超時失敗。
若全部通過，成功收據才出現在
`artifacts/benchmarks/factorized-daily-independent-pair-20261020-v60/receipt.json`。
**18:10 回查：v60 已因四小時上限退出，exit_code=1，沒有成功收據**；
v40 仍執行完整逐 cell oracle。此失敗保留，不能把等待工作失敗說成配對完成，
也不能以延長截止或重跑取代原 producer 的成功證據。鏈的正向／失敗案例由
`pair-acceptance-contract-v58-20261020T060653-8f822500` 驗證
**223 passed / 31.98 s**（包括相關 reader／cache 回歸，不與其他集合相加）。

本輪沒有執行 production apply／restart。14:06 唯讀回查時，Discord、當沖與
隔日沖 owner 的 PID／InvocationID 保持原值；gateway 則在 13:57、14:05 有本輪
命令以外的正常 stop/start，現在 active/running。這只能證明當下 liveness，
不能當作本輪部署、資料健康或 paper ledger acceptance。

## 剩餘工作與部署邊界

仍不能宣稱「所有網站／所有 producer 已完成真正逐日增量」或「已到極限」：

1. BASE 已加入精確 recurrence checkpoint、未變輸出重用、區塊 parser 重用及
   批次目錄提交；上游 `_official_frame` 已加入明確 opt-in 的精確 session suffix，
   正式預設仍是既有路徑，來源全 SHA、部分 lifecycle 與 Yahoo archive 正規化
   邊界仍完整處理。精確 suffix 已擴及檔案模式；預設仍重寫變動個股完整 Parquet，
   年分區 writer 只開放明確 opt-in／新 root。v35 的 per-symbol wall 改善不包含
   `_official_frame`，不將 suffix recurrence 稱為全管線 O(delta)。
2. public feature 已可重用完全一致的壓縮前綴、跳過歷史解碼；變動原檔完整 SHA
   與完整輸出重寫仍存在。reader-first 支援已擴至 research lineage、stage／cold／
   remote readiness 與 market freshness；public writer 尚未分區，正式 consumer
   的同版本 runtime admission 也未完成：
   - manifest 固定有序 typed 資料、schema／metadata、原始 receipts 與每個 member；
   - 先覆蓋 panel／loaders／preparer／publisher／admission 的 file/is_file／完整 SHA
     消費者，不能直接把 `.parquet` 檔換成資料夾，或一面給舊 reader 舊資料，
     另一面讓新模型讀新資料；
   - 普通新增日只寫受影響的跨標的月／年分區，歷史修訂重建對應分區及有狀態
     後綴，避免「標的×每天」產生大量微小檔案；
   - 新 ABI／manifest 綁定後再開 writer opt-in，以全歷史 oracle 驗證新舊 reader。
   原有 byte SHA 不能偷換成 logical digest，沒有確認相容前維持原檔案契約。
3. 未納入明確日期契約的 monthly/report／lifecycle／rule 來源仍可能觸發全建。
   每個來源必須依實際可知時鐘擴充，不能按欄名猜交易日。
4. Factorized 已加入正規化來源重用、日／週 horizon pruning 與整代容量 reservation；
   變更原檔的完整 SHA、季報／營收 normalization、報表 resets、issuer lifecycle
   及完整 member hashes 仍非全為 O(delta)。大型真實 producer 冷首建／每日
   end-to-end 已由原 v40 與 v89 恢復收據、v47 獨立 BASE 配對驗收；backing-volume
   新代 reserve 及長期 retention／回收仍須各自驗收。
   沒有容量不能刪檔換取成功；現行行為是保持上一代並明確延後。
5. 未提升工作樹為正式服務版本。本輪維持原生交易 owner 與帳本；後續須
   canonical plan→flat admission→apply→verify，完成新版本同 run 來源／暖機／引擎
   ACK 與 ledger-prefix 驗收。隔日沖原有 disabled YAML 不因本輪變成有交易。

分區遷移不是只有 producer 加一個輸出目錄。下一階段的最小 consumer 清冊：

| Canonical consumer | 必須保留的契約 | 遷移驗收 |
| --- | --- | --- |
| `stockagent/data/panel.py` | Arrow schema／欄位與日期下推、panel 起點前的 PIT／持續性規則、完整來源指紋 | 新舊 reader 的全部值、availability／execution masks 及完整歷史規則相同 |
| `prepare_tw_day_trade_factorized_panel.py`／`prepare_tw_day_trade_factorized_live.py` | 同一固定 source release、physical public path／SHA、原始 Float64 events | 新 manifest 不能繞過原 source identity；causal state／全量 tensor oracle 相同 |
| `stockagent/data/panel_cache.py` | 所有 source members 的完整 byte proof、generation 與失效傳播 | 同大小修訂／刪除／部分發布／重啟均拒絕舊 masks |
| `rebuild_tw_public_data_layer.py`／`audit_tw_public_data_layer.py` | source／summary／output 收據與整代 promotion／rollback | 不混用新分區與舊 summary；失敗保留 last-good |
| `publish_data_releases.py` 與 live admission | release members／code ABI／runtime ACK／ledger-prefix | 固定 release 獨立讀回、平倉後切換，不能以 writer exit 0 取代原生驗收 |

上表是經目前程式入口核對的遷移清單，**整體尚未完成**。panel／preparer／部分
audit／rebuild 與 `signal_engine`／`completed_inference_cache` 來源 key 已接入相容 ABI；
本次已補 `data_monitor_inventory`、publisher／migration 的 logical members admission；
10/21 另完成 `market_status` 與研究 release 的雙 ABI lineage／cold 讀回測試。
仍須完整 runtime admission 及正式 public writer 遷移，不能把 fixture 支援當成部署。
特別是 panel 的持續性規則會跨查詢起點，
不能只讀選定日期的月分區；本輪 reader 的完整規則測試已保留這項邊界。

原有 artifact maintenance／minute-curves 的失敗也未藉網站快取改寫為正常；
本輪不是它們的恢復驗收。以上殘餘工作仍保留在原 agent task，沒有標記完成。

## 第五階段：上游 session merge、監控與發布端

### 根因與資料流

舊合併先讀全歷史 TWSE／TPEx，解析所有價格，建立 lifecycle、套用來源優先序、
重算 reference／前一交易日，再回傳完整約 903 萬列。個股 recurrence 已有增量，
但沒有減少前面這一段。初版把 merge projection 接在 parser cache **後面**，
來源每日整檔重寫／重新分組時，parser cache 全 miss，還加了一份中間層寫入。

目前 opt-in 路徑：

1. 保留完整來源 byte receipts／日曆／lifecycle 證據；先完整證明原始時鐘，已
   證明的 ISO／Date32 用相同日期值的直接轉換，不重跑全部 ROC／文字猜測解析。
   相同原始 parser 的 lazy 投影只讀 lifecycle 所需欄位，不先解析歷史價格。
2. 未變原檔 SHA 重用已證明的 session proofs；改變原檔對所有 typed 欄位逐日
   驗證，獨立來源最多兩路並行。舊日修訂／刪除／換 schema、日曆或 lifecycle
   改變均使相應 suffix 或整代失效，不能拿最大日期當成 append 證明。
3. 價格讀取以**實際解碼驗證過的** chronological group bounds 選原始 row groups，
   不拿 footer min/max 當證明；未知日期仍走原 parser／全建。無 statistics 的
   Parquet 測試也必須能正確選出所需 groups。只解析受影響日期的價格並執行
   原本來源優先序、合法標的、fallback、reference、
   lifecycle 與診斷。TPEx 全歷史 next-reference 重複鍵仍驗證；長停牌標的使用
   prefix 中真正最後被選中的日期／episode，不能固定往前抓一天。
4. 跨標的年分區只寫受影響年、保留其他 immutable members。commit 前後重新
   核對原始來源；完整歷史仍讀回。已排序 prefix／suffix 用有序合併，不再次對
   全歷史排序；沒有刪欄位或分鐘。

`official_merge_projection.py` 沿用 `partitioned_parquet` 的 typed／byte 驗證與
原容量 reservation；不另建 collector／交易 owner／資料發布管線。空間不足延後，
不自動刪歷史或退役失敗代。parser cache 的舊功能與預設排程不變。

### 真實完整來源測速

重測工具：`scripts/benchmark_official_daily_merge.py`。在 canonical source read
lease 下固定原始 bytes，再建立明確標示的前日資料副本；恢復真正最新來源後
執行新增日與獨立全量 owner，比對**全 41 欄、全部列／NULL／浮點位元、順序、
所有來源收據與所有診斷**。不是只比較新增的 2,311 列。

- v68 固定 2026-10-19 → 2026-10-20：9,028,545 → 9,030,856 列。
- 已淘汰初版：cold seed 162.354 s；新增日 107.058 s（CPU 275.007 s）；
  原始 full 41.487 s（CPU 221.927 s）；不變來源 48.578 s。
- 初版新增日雖僅重建 2,311 列、重用 23 年分區，卻仍解析 9,784,477 原始列；
  一年 boundary 重寫 438,210 列。這說明少寫 rows 不能取代整段測速。
- v74 同一 2-thread native policy：新增日仍為 150.883 s（CPU 267.334 s），
  full 96.507 s（CPU 171.522 s）；unchanged 85.555 s。完整輸出 oracle 通過，
  **效能仍失敗，沒有部署**。其中 lifecycle 約 47.113 s、current decode
  約 52.654 s、prefix read/sort 約 19.945 s；僅回傳 2,399 原始列不代表 parser
  沒跑歷史。不得把 v68 的不同 native pool 數字混成同政策加速比。
- v79 加入原始 clock 證明前移、certified group read、直接日期轉換與有序合併，
  已通過完整 oracle，收據位於 `artifacts/benchmarks/official-real-daily-20261020-v79/`。
  同一 2-thread native policy：cold seed 132.435 s；新增日 **94.412 s**
  （CPU 140.129 s）；原始 full **99.114 s**（CPU 171.414 s）；unchanged
  75.748 s。新增日價格只讀 81 個 row groups 中的 2 個，共 76,957 列，
  current decode 降到 2.389 s；但 lifecycle 32.091 s、prefix read/sort
  29.275 s 仍佔大宗，僅約 4.7% wall 改善，不能稱為數量級加速。
- 已進一步將 lifecycle 查核的 quote 集合按實際上市／下市／轉板事件鍵縮小，
  在 collect／unique／sort 之前過濾；完整價格輸出與原始來源驗證不減少。
  延遲建立 metadata，無事件就不額外掃市場歷史；保留來源順序與 keep-last。
  v82 的 **129 passed / 21.53 s** 包含 eager／lazy、空歷史、轉板與重複名稱。
- v83 固定使用 v79 的完整原始 source bytes 與舊版全值 oracle，除了新版本
  daily／full／unchanged 互相比對，還必須等於修改前完整 41 欄／所有診斷收據。
  工具新增 `--reference-receipt`：來源不同或 reference 途中被更換即拒絕。
  `artifacts/benchmarks/official-real-daily-20261020-v83/receipt.json` 已接受：
  新增日 **90.371 s**（CPU 120.875 s）、原始 full **61.465 s**
  （CPU 112.851 s）、unchanged 48.047 s。lifecycle 降至 **9.172 s**，
  但 daily 仍慢於 full，不啟用新預設。冷 seed 183.728 s，期間與 reader probe
  重疊，不拿它宣稱冷建加速；所有數字均是共用主機上的單次觀察，不是 SLA。
- v85 私有完整 reader probe：Arrow decode＋sort 47.520 s、相容 Polars reader
  27.124 s；全部 9,030,856 列／41 欄的 typed hash 相同，也等於 v79 舊 owner。
  不是只驗日期邊界／末列。已將 private merge prefix 接到現有 canonical
  `ParquetSource.scan()`，讀前全 member SHA、讀後 generation／pointer fence
  不變；共享 `partitioned_parquet.py` 沒有改動，不破壞 v40 固定 code graph。
  v87 再以同一原始來源與 v79 固定 oracle 驗證整段，收據位於
  `artifacts/benchmarks/official-real-daily-20261020-v87/receipt.json`，canonical
  run `official-real-daily-v87-20261020T102117-056b8b52` 已 succeeded / exit 0。

v87 最終測速（同一 2-thread policy；合併 owner 包含自身驗證、cache 寫入與
完整歷史組裝；額外獨立等價性校驗另列，沒有從工作流程取消）：

| 範圍 | Wall | CPU | 完整等價性校驗另耗時 |
| --- | ---: | ---: | ---: |
| 前日首次 cache 建置 | 124.275 s | 178.440 s | 分區 typed roundtrip 包含於 owner |
| 新來源 session 增量 merge | 54.188 s | 81.827 s | 19.514 s |
| 獨立原始全量 merge | 57.287 s | 105.803 s | 23.662 s |
| 未變來源完整輸出 | 22.516 s | 34.738 s | 18.560 s |

新增日只解析 2／81 個官方原始價格 groups，共 76,957 列，最後合併仍提供全部
9,030,856 列。source proof 15.291 s、lifecycle 9.122 s、prefix read/sort
11.741 s、current decode 2.577 s、partition commit 5.420 s、full output
assembly 6.067 s。較早 v83→v87 是同一封存來源／政策的不同輪次觀察，不能把
90.371→54.188 s 宣稱固定倍率 SLA；v87 的公平同輪比較僅約 5.4% wall 改善。
額外獨立 oracle 及來源擷取／前日 fixture 的時間均保留在原始收據。

下一步若要再降低上述完整成本，必須先遷移仍要求完整單檔／DataFrame 的讀取
契約，再由 canonical 原始 producer 發布可驗證的 immutable session generations。
不能只拿 merged cache 的尾部幾列或 metadata 取代完整原始來源 byte proof；
也不能刪掉歷史輸出來宣稱整段 O(delta)。

這是完整上游 merge（包含驗證、快取寫入與歷史輸出組裝），**不是完整 BASE
個股寫入、Factorized、推論、網站或開盤成交耗時**。當原始資料仍每日整檔改寫，
全來源 SHA／session proof 有不可忽略的全歷史成本，不能宣稱整段 O(delta)。
本次真實來源正式 summary 沒有 legacy／Yahoo 參數；兩種 archive 與 CSV、
優先序／無效官方列／非交易日 fallback 已由完整 owner parity 測試另覆蓋。

### 其餘 consumer 與發布相容

| 消費端 | 本次變更／尚存邊界 |
| --- | --- |
| `data_monitor_inventory` | 同次單一目錄列舉辨識 file／manifest，一個 logical source 不重複計數；掃描全 member SHA，匯總所有列與 non-null／日期。任一舊 member 損毀，不得仍命中 complete 快取。舊檔不額外逐檔探測不存在的 manifest。 |
| `publish_data_releases` | 在原 source locks／rights／freshness gates 內加入完整 manifest/member 關係與格式／成員集 fences，不更動 catalog 發布權限。 |
| `dataset_migration` | 私有 prepared controls 不可把新 head 重新定義成可變舊單檔；新 manifest 與 retained 舊單檔 override 相衝突時拒絕，要求 fresh preparation。 |
| 固定 cold release | 已用 canonical packed publisher 發布測試副本，再固定 snapshot 獨立還原、讀取全表及比對原 manifest bytes；沒有發布正式來源。 |
| `market_status` | 原 v40／v89 固定 code 驗收結束後才修改；已完成 logical inventory／日期與完整 member proof。新版本回歸通過，正式 runtime admission 尚未執行。 |
| 研究 overlay／stage／remote readiness | 已完成全鏈雙 ABI、exact cold inventory 與 source generation fence；單檔 hash 欄位維持 raw bytes，分區採新 source 欄位。FinLab 權限不變。正式 writer 分區與整代部署另行准入。 |

新增負面測試包含發布途中 member 損毀／成員新增／混入舊 ABI：原 cold head 不
前進，最後已接受 fixed release 仍可精確還原。原 publisher／packed 的 inventory、
來源鎖、exact bytes、同步與 delivery acceptance 邊界不變。
發布與 private migration 也以 catalog `dataset=tw-public` 辨識來源；搬移／還原後
即使根目錄不再叫 `data_tw_public`，仍需完整 member guard。未登錄的其他私有
資料集不會因內含相近檔名而被自動納入；權限、source catalog 和 collector 不變。

### 驗證與版本邊界

- v69：**174 passed / 22.10 s**，上游 archive parity 與監控 freshness／membership。
- v71：**91 passed / 25.33 s**，lazy admission／suffix 與原 official builder。
- v75：**641 passed / 54.37 s**，上游、發布、prepared migration、packed 還原與
  中途失敗防護；Ruff 與 `git diff --check` 通過。這些集合不相加。
- v78：**127 passed / 25.81 s**，包含不靠 footer statistics 的 certified group
  價格讀取、未知時計退路、完整 owner parity。
- v80：**699 passed / 68.23 s**，最新核心整合；v77：**319 passed / 16.01 s**，
  全資料監控組。最新 Ruff 與 `git diff --check` 通過，集合不相加。
- v84：**701 passed / 65.37 s**，包含 lifecycle scoped lazy 查核、跨版本
  fixed source oracle 與原 publisher／migration／packed 還原回歸；取代 v80
  作為這一組最新程式的驗證，不與先前集合加總。
- v88：**702 passed / 71.36 s**，最新完整 scoped 組，包含 canonical prefix
  reader 與搬移後的 catalog guard；較早 v84／v86 為重疊集合，不相加。
- 原 v40 完整 Factorized daily／full cell oracle 保留原 code、source capture 與
  輸出；v60 等待配對已超時。10/21 的 v89 已完成原 graph 的恢復與全值／獨立
  BASE 配對，見第六階段；原兩筆失敗保留為 superseded，未取消或改範圍。
- 所有新數字是私有副本／離線驗收，正式 source writes、ledger writes、service
  restarts 均為 0。沒有以 HTTP 200 或服務 active 宣稱來源／模型／交易已就緒。

## 第六階段：10/21 研究發布身分與驗收恢復

### 原因、修正與 ABI

剩下的問題不是缺一個讀 Parquet 的函式，而是來源身分仍被假設為「一個路徑
等於一個 byte SHA」。直接把 manifest digest 填進 `base_feature_sha256`，會讓
研究建立、發布、冷庫及遠端准入對相同欄位產生不同解釋。

共享 `stockagent/data/parquet_lineage.py` 沿用 `ParquetSource`，區分：

| 來源 | 收據 | 讀回／發布驗證 |
| --- | --- | --- |
| 原單檔 | 原有 byte SHA 欄位保持不變 | 完整 SHA 與 before/after file generation |
| 分區來源 | `parquet_byte_lineage_v1`，獨立 `source`／`base_source`／`base_feature_source` | 原 manifest bytes 與全部 ordered member bytes、列數、schema、權限及同次 generation fence |

相對成員路徑允許固定版本搬移還原，不能擴張到來源 namespace 外。新舊欄位
同時出現、同一 logical source 同時有舊檔與 manifest、舊 member 缺失或同尺寸
修訂，都會拒絕。不是 glob 所有 `.parts` 世代，也不只驗最新年。

- wide research：單檔 receipt 維持 v5，分區來源 v6。
- all-observed：單檔 receipt 維持 v2，分區來源 v3；join 採穩定 left order，
  build strategy 升為 `bounded_annual_range_join_preflight_keys_stable_left_v4`，
  避免不同 reader 的 hash join 隨機排序造成不必要的 output hash 差異。
- 三種 stage：舊單檔 release schema v1；分區 schema v2 並列出完整來源身分。
  formal action companions 不混入 feature member；FinLab 私有／非商用／不可
  公開原值條款維持，沒有改 catalog 或發布權限。
- cold audit 比對每個 base member，並核對實際 staged receipt 與冷庫中的原始
  receipt SHA，不能只有「冷庫裡存在一份 receipt」。remote readiness 核對相同
  來源身分，股票根也接受 explicit manifest。

### 消除未變 staging 的整檔重寫

`_stage_copy` 在來源與既有 private target 的完整 SHA 都相同、generation 穩定、
mode／owner 正確時不再重寫。變動檔仍經 temporary、完整 SHA、file fsync、
atomic replace、directory fsync。不是依賴檔名／mtime 判斷資料相同。
FinLab stage 同一交易內共用 formal snapshot，避免新增重複的完整 base hash。

實際保留的 **1,337,372,819 bytes** 來源，以原 Git `_stage_copy` AST 為 baseline，
執行 A-B-B-A；最新一輪
`research-stage-abba-v96-20261020T181849-04f78431` 將原始 Git revision 與函式
AST SHA 固定在收據，避免之後的 HEAD 漂移改變 baseline：

| 版本 | 完整呼叫 wall seconds | 本次寫入 payload |
| --- | --- | --- |
| 原版 A | 2.119、2.779 | 每次 1,337,372,819 bytes |
| 新版 B（未變來源） | 1.618、1.673 | 0；完整來源／目標 SHA 仍驗證 |

收據 `artifacts/benchmarks/research-stage-copy-20261021-v96/receipt.json`。
前一輪 v94 的原版為 2.045／3.319 s、新版為 2.045／2.243 s，也保留。
這證明避免完整重寫，不證明全 research pipeline 已 O(delta) 或固定倍率；僅各
兩次樣本，且同機有其他驗收。原 baseline 無 explicit fsync，新寫入版本反而
補足該 durability barrier，沒有為測速降低耐久性。

### Factorized 的獨立恢復驗證

`recover_factorized_daily_acceptance.py` 只接受已結束、精確 argv 綁定、原 log
確認 `filesystem_headroom_exhausted`、且僅最後 unchanged stage 缺少的 run。
原完整 code graph、capture 原始 bytes、owner／readonly、原 progress 都必須仍
相同。不能復用其他失敗、跳過來源、取消原 task 或改寫 v40 收據。

v89 已補跑原 `prepare()` unchanged owner：**26.413 s wall / 23.587 s CPU**，
結果 `already_current`，manifest generation 不變。接著重新驗每個 projection
cell 並與 v47 全新 cache 的完整 BASE arrays／masks 配對。

原 v40 完整 owner 階段也保留在恢復收據，不以 26.413 s 代表新增日：

| 原真實來源階段 | Wall seconds |
| --- | --- |
| 前一完整交易日冷首建 | 5,660.916 |
| 新增完整交易日（含原 owner 全步驟） | 503.902 |
| 同最新來源的全新 cache 全量重建 | 6,303.400 |
| 未變來源完整 integrity owner（v89 恢復） | 26.413 |

這是盤後資料準備工程驗收，不是 09:00 訊號／市價成交延遲；各階段只跑一次，
不宣稱穩定倍率，也沒有把長時間的獨立 oracle 從驗收證據刪除。

原 all-cell comparator 用 512 MiB cache，而一個日期區塊解壓後最高
625,713,976 bytes，會反覆逐 symbol 驅逐。新驗收器保留相同 4-date dense slabs、
uint32 bit 比較、原有完整 dense SHA 與所有 source member 驗證；僅採每邊
2 GiB 有界 decoded cache，hash memoryview 而不額外複製 `tobytes()`。
不縮短 3,119 dates × 2,757 symbols × 14,720 individual/common channels。

v89 已成功，paired receipt 為
`artifacts/benchmarks/factorized-daily-recovery-20261021-v89/pair/receipt.json`：

- 全部 3,119 dates × 2,757 symbols × 14,720 individual/common channels
  的 uint32 bits 相同，dense SHA 仍為
  `4e7f1b781574251d0feb505c6e98c523ffebc4e72e2d11efcb4c4711218c9551`。
- 與 v47 全新 cache 的完整 BASE arrays／masks 配對，BASE SHA 為
  `b34b6ed06f1a250dcba7eeba02b27bf66b578aa5ce5c0d10dba1f4006e91d7b3`。
- 保留的 3,492 個原始來源 member 全部驗證。完整 pair 耗時 **1,037.383 s**，
  包含全 projection、BASE、來源與 code proofs；不是模型推論延遲。
- 原 v40 純 all-cell comparator 是 5,093.360 s，與本輪 pair 的範圍、負載不同，
  不能拿兩數直接計算同輪加速倍率。

接受狀態是
`accepted_real_source_daily_and_independent_base_not_pit_live_or_inference_readiness`。
這補完原固定 code graph 的完整工程驗收；不是歷史 PIT、後續新 code graph、
正式服務或即時推論就緒。未用 unchanged 單獨成功冒充整段驗收。

### Market freshness 的完整分區檢查

`market_status` 在 v89 完成後才修改，保留輕量 calendar import 與既有每日
benchmark/session sampling 政策；沒有改成每個請求掃全市場全部 payload。
logical inventory 將一份 manifest 計為一個來源，原檔與 manifest 同時存在則拒收。
manifest 的每個歷史 member 都重新 byte-verify，之後才讀可重用的 scalar max-date；
不能因最新年日期足夠新而隱藏舊年缺失。跨代更新只作兩次有界重試，不快取失敗。

新增舊 member 缺失、同尺寸／同 mtime 損毀、mixed ABI、讀取中 atomic generation
切換與真實 `data_freshness()` 的測試；最新整合 v98 包含這些檢查。
唯讀 legacy scalar cache ABBA（每段 1,000 次）另記在
`freshness-scalar-v99-20261020T182225-e837b564/run.log`：

| 版本 | p50 microseconds | p95 microseconds |
| --- | --- | --- |
| 原版 A1／A2 | 27.406／22.549 | 64.987／63.846 |
| 新版 B1／B2 | 53.189／54.602 | 152.770／204.947 |

新增 ABI 探測有微秒級成本，本輪沒有把這項退步藏起來。這只是 legacy 單次日期
快取呼叫，非整段 freshness、網站、推論或交易延遲；不以此推算整頁變快或變慢。
原來源 bytes 與日期在前後一致，未變更正式資料。

### 本階段驗收與剩餘工作

- `daily-lineage-final-v98-20261020T182051-225e661f`：**1,957 passed / 146.55 s**，
  共 53 檔，涵蓋 dashboard daily／BASE／panel／Factorized、上游 merge、監控、
  research producer／stage／remote／cold fixtures、完整 packed restore、publication／
  migration／partitioned ABI、completed cache、market freshness 與 calendar。
  不是全庫測試；v95 的 710 項等較早集合不相加。Ruff 與 diff whitespace 檢查通過。
- 新增 source 搬移、舊分區損壞／缺失、mixed ABI、manifest 全清單、三種 stage
  的單檔／分區雙版本、原位 target no-op 及來源改變拒收測試。
- 原 v40／v60 失敗保留；以實際成功且完整配對的 v89 設定 superseded，不刪證據。
- reader-first 遷移及真實 owner 配對已完成上述範圍；新 freshness code 的正式
  admission／同 run 暖機與引擎 ACK 尚未執行，不因離線測試通過自動切換服務。
- 仍需 canonical source 原生 immutable session generations、full-history return
  的 lazy output、所有 consumer 的正式 deployment／admission。原始來源改變後的
  full SHA、部分 lifecycle 與整段輸出成本尚在，不能宣稱已到硬體極限。
- 沒有正式 publication、service restart、writer 切換、帳本修復或 broker 下單。

下一個最小可驗證里程碑依序為：

1. 由 canonical collector 發布原生 immutable session generation／append delta，
   綁定原始完整 receipts 與修訂時鐘，才有依據減少來源全檔重掃；parser cache
   不可替代來源完整性證明。
2. 將完整結果 materialization 與查詢拆開，以受控 lazy return 減少未變前綴讀回；
   完整歷史請求、舊日期修訂與全量 oracle 都必須保留，不能靠刪分鐘或只讀末日。
3. 固定新 code／source release，完成 reader 與新 writer 整代 admission，再依
   canonical plan→flat admission→apply→verify 切換與 rollback 驗收。沒有這些
   runtime 證據前，正式 writer 維持原契約。
