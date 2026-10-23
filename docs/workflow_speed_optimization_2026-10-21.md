# 工作流補測與同值優化：2026-10-21

本次補完目前六個啟用模式的完整離線推論、大型歷史查詢、瀏覽器實際繪圖、
本機 inventory、D 磁碟完整版本往返及更多來源階段紀錄，並實作推論格式化與
歷史解析的同值優化。已測改善不代表所有流程已測完，也沒有證明已達計算極限。
完整歷史仍有 Python 逐列投影與物件配置成本；遠端訓練節點目前連線不通。

後續已依規劃實作固定欄位歷史點、批次原生解析與推論相依監測優化，
本次比較與第一批分開，詳見文末「第二輪實作與驗證」。第二輪最新歷史批次
沒有重現速度改善；記憶體改善已驗證，歷史速度維持未驗收，不以早期較快批次代替。
2026-10-22 第三輪另固定當時來源進行新的八次完整比較，原生曲線點建構與 tuple
傳遞改善已通過；前述第二輪的負結果仍保留，詳見文末第三輪紀錄。
第四輪針對清冊最大的證據檢查區段，完成 372,550 份固定檔案的三組、18 次
新程序比較。最終十次鏡像比較中，完整檢查、輸出與獨立讀回平均
168.705213 → 131.043917 秒，少 22.3%；這是該完整區段的本機觀測。

本次產物根目錄為 `artifacts/benchmarks/workflow_speed_optimization_20261021/`。
前次入口清冊與量測契約見 [工作流測速基準](workflow_speed_baselines_2026-10-21.md)，
語言比較見 [語言工作流測速](language_workflow_benchmarks_2026-10-21.md)。

## 已驗證的完整工作改善

| 完整工作與同條件比較 | 原路徑 | 本次路徑 | 證據與邊界 |
| --- | ---: | ---: | --- |
| 全歷史重建 → canonical 公開投影 → JSON/gzip/ETag → 完整解壓/JSON 讀回 | 22.126943 秒 | 13.089279 秒 | `history_projected_abba.json`；新程序 ABBA 各兩次；1.6905 倍吞吐量、時間少 40.85% |
| Multi-Basis 完整離線訊號，暖機後 | 390.693484 ms | 280.171547 ms | `inference_components_abba.json`；新程序 ABBA，每程序 9 次 warm；以各程序 warm 中位數再取同變體平均；1.3945 倍吞吐量 |

歷史輸入固定為 537,535,491 bytes，包含 358,132,962 bytes 的 marks ledger。
輸出保留 372,946 個實際點、9 條曲線；四次 canonical payload、17,312,615 bytes
HTTP body 與 5,523,452 bytes gzip 各自 SHA 完全一致。這個時間不含真實網路或瀏覽器，
helper／OS page cache 可能已暖。沒有抬高 256 MiB columnar cap；超過門檻的 ledger
仍逐行有界讀取，改用原生 JSON 欄位投影，減少不使用欄位與重複字典查詢的配置。

推論比較固定實際正式 source release，只替換已核對的兩個 helper：
`_build_decision_rows` 與 `cumulative_recent_returns`。原 helper AST 與該 source
基線一致；權重、逐股決策、解釋及近期報酬的完整驗證 SHA 全部相同。這是完整
canonical 離線推論，保留所有模型 aux；不含開盤取價、持久發布或 broker 成交。
已保留每次階段、CPU/GPU/RSS、來源與子收據 SHA；未將巢狀區間加總。

本機為 Ryzen 5800X3D、16 個可用 CPU affinity、約 94 GiB RAM、RTX 5070 Ti。
背景服務與 GPU owner 持續運作；ABBA 可降低順序偏差，仍不構成獨佔硬體的普遍速度保證。
不同測試批次的倍數不能相乘。較早 `history_native_abba.json` 是另一批比較，
不與最終投影批次合併平均。

## 六模式當日推論

| 模式 | 輸入 shape | 程序首次呼叫 | warm 中位數 |
| --- | --- | ---: | ---: |
| `tw_day_trade_100m` | 32 × 2744 × 99 | 7.206798 秒 | 318.618 ms |
| `tw_day_trade_multi_basis` | 32 × 2745 × 99 | 9.590193 秒 | 321.570 ms |
| `tw_day_trade_multi_basis_22` | 32 × 2746 × 24 | 7.965453 秒 | 493.886 ms |
| `tw_day_trade_v8_annual_log_cash` | 32 × 2754 × 99 | 4.716618 秒 | 342.205 ms |
| `tw_day_trade_factorized_scale_separated_cash` | 32 × 2757 × 14726 | 16.234840 秒 | 1592.236 ms |
| `tw_day_trade_unit_mgda` | 32 × 2757 × 14726 | 14.765712 秒 | 1121.483 ms |

對應收據為 `inference_100m.json`、`inference_fresh.json`、`inference_mb22.json`、
`inference_annual.json`、`wide_fresh_inference.json`、`unit_mgda_exact_inference.json`。
每個模式均驗證首次與後續完整輸出一致，panel 日期為 2026-10-20。
Multi-Basis 首次樣本含 profiler；其首次值不能與未剖析模式比較載入效率。
其他 source/runtime cohort 也分開保留，這張表不是六個模型的公平算法排名。

兩個 14,726-feature 模式保留全輸入與完整 aux，GPU reserved 峰值均為
11,049,893,888 bytes。容量入場保留原先 12,132,024,320 bytes 的 free 門檻，
等待其他 owner 自然釋放，沒有停止服務或調低預留。大型模型的 logical dense
工作量為 5,196,746,496 bytes；實際 factorized host transport 為 515,449,704 bytes。

Unit-MGDA 最初被模型契約檢查拒絕；原因為工程程序缺少正式 owner 的
`STOCKAGENT_SPARSE_ADJOINT_BACKEND=cusparse_csr_transpose` 與 algorithm `6`。
只讀這兩個非機密旗標並沿用同一 runtime 後通過；沒有略過模型、ABI 或 checkpoint 檢查。
namespace launcher 亦補上結構化 PATH 傳遞，保留有空格的路徑，沒有複製整份環境。

另測既有 completed-session cache：`wide_completed.json` 與原 forward 的輸出 SHA
一致，warm disk hit 不重跑 GPU forward；此次完整 warm 中位數 2.353004 秒，
仍有來源證明與準備成本，且背景負載不同，不能宣稱比 1.592236 秒的另一批更快。

## 新補的呈現、資料與儲存邊界

| 流程 | 本次實測 | 完成與限制 |
| --- | --- | --- |
| owner 瀏覽器 `:8771/tw-day-trade/` | first paint 280 ms；ready 1148.4 ms；全歷史載入 1344.2 ms、重畫 119.8 ms | 372,946 點、9 series、實際 Canvas；像素重畫一致、SSE 8、錯誤 0 |
| 公開瀏覽器 | first paint 2044 ms；ready 3428.9 ms；全歷史載入 1898.8 ms、重畫 28.4 ms | 全部點/series/Canvas 與像素檢查、SSE 10、錯誤 0；含 WAN 與轉址，現行 server cache 可能已暖 |
| 完整本機 footer inventory | 72,556 files、89,169 fields；CPU replay 中位數 10.166379 秒 | decode 4.264444 秒、discovery 2.712399 秒、含 partition proof 的 stat 0.937462 秒各自保存；frozen CPU replay 不冒充來源重建 |
| private packed 新舊版本往返，fixed 8 MiB | 完整平均 4.475578 秒；變更版本新增 42,547,366 bytes | baseline/update/no-op、完整 verify、新舊版本 durable restore/readback 均保留 |
| 同一 private fixture，fastcdc max 8 MiB | 完整平均 4.170925 秒；新增 4,080,939 bytes | 寫入量少 90.4%；小樣本、ext4 fixture，尚非 D/NAS 正式 policy 驗收 |
| 同一 private fixture，fastcdc max 1 MiB | 完整平均 6.095347 秒；新增 811,677 bytes | 寫入量更少但全流程變慢，未因小 chunk 看似省空間而採用 |
| 真實 D 磁碟 private fixture，portable / batched native | 完整平均 45.694745 / 49.165802 秒 | 完整 ABBA，新舊版本 SHA/還原一致；native 反而較慢，保留原路徑 |
| 四檔耐久 publication | compact 兩次 profiled 樣本 287.479 / 256.159 ms | 保留 file/directory fsync、完整公開值及 monitor binding；compact 已是現行實作，不重算成本次新改善 |
| TEJ 既有 256 任務 | end-to-end 中位數 10.547352 秒；before-submit 9.356953 秒 | 既有紀錄回溯、provider query 0；本批全為 EMPTY/0 rows，不換算非空下載速度 |

瀏覽器使用 Chromium 151.0.7922.34 與 CPU 2D Canvas；沒有宣稱 WebGL/GPU
繪圖極限。重畫確實呼叫目前 uPlot renderer 並等待兩個 animation frames，
memoized 0.1 ms 另列，沒有拿沒有執行 renderer 的空操作當成重畫速度。
目前網站的觀測不證明已載入本次後端程式。

儲存修訂 fixture 由原 2,018,074-row Parquet 私有複本製作：僅第一列 close
加一個 ULP，逐欄讀回其餘值一致；壓縮後檔案長度多 3 bytes。這是明確的人造
工程修訂，沒有改寫 provider 資料或充當原始觀察。完整版本往返比單獨 chunk discovery
更適合判斷採用；D 測試保留原生 cgroup、來源、code、owner 與 inventory fences。
測試沒有發布正式冷資料、切換 chunk policy 或產生任何 provider request。

HTTP 分開保存 owner、www 入口轉址及 canonical 公開站三個 audience：
owner 43 routes 共 172 次皆為 200；www 入口 215 次皆為 308，Location 指向無 www 網址。
canonical 公開站 43 routes 共 215 次，其中 23 routes/115 次成功，20 routes/100 次為 403。
403 原樣保留，沒有用 owner 的 200 補成公開站通過。公開當沖全歷史 API 後續樣本
中位數 1006.892 ms；樣本不足以宣稱長期 p95/SLA 或服務容量上限。

## 程式、測試與紀錄

- `stockagent/private_json.py`：可選原生投影、任意精度整數及浮點/非有限/legacy
  fallback；wire encoder 不改。相關測試涵蓋巨大整數、負零、subnormal、重複 key、
  surrogate、UTF-16、缺欄位、非物件與格式錯誤。
- `stockagent/live/market_status.py`：近期報酬的有界 immutable-identity cache；每次
  重解 preferred integer artifact，讀前/讀後 identity gate，失敗不快取，回傳副本。
- `stockagent/live/signal_engine.py`：同一逐股列重用已轉換的價格、權重與資格 bool。
- `stockagent/live/tw_day_trade_dashboard.py`：大 ledger 原生欄位投影，保留來源、
  時鐘、財務/provenance 欄位、完整點數、數值算法與有界 streaming。
- 測速器補上 cold/warm profiling、真實 Canvas 點數/重畫驗證、frozen inventory
  identity、新舊版本 chunk 候選，以及 TEJ/FinLab/解釋與瀏覽器計時的明確單位。
- 相關集中回歸 295 項通過；namespace 41 項通過；最後測速器/collector 回歸
  120 項通過；補充原生數值 fast path 與未使用欄位格式錯誤檢查後，private JSON
  31 項通過。各批部分重疊，不能相加當獨立測試數；未宣稱全 repo suite 通過。

`collection_v3/collection.json` 為本輪最終索引，`collection_v3/speed_samples.jsonl`
保存每個區間、單位、來源 SHA、locator、context、outcome；
`collection_v3/speed_summary.csv` 可供後續加速分析；
`collection_v3/coverage.csv` 保留逐項清冊與未測邊界。
`collection_v3_readback.json` 是獨立 SHA/凍結內容/單位/吞吐量/CSV regrouping 讀回。
`optimization_comparisons.csv` 保存完整工作 before/after；
`optimization_acceptance_final.json` 綁定本次比較、六模式、瀏覽器及證據 SHA。
失敗、等待、guard 拒絕及被否決候選收據均保留。

本輪最終 collection_v3 保存 233,075 筆計時欄位、24,606 個情境群組，
7,647 個來源登錄、1,546,924,676 bytes 證據、5,648 份不同原始內容；
12 個輸出與全部可擷取原始內容讀回通過。2,399 個 registry 來源中，
1,362 個已有精確 ID 綁定的階段紀錄，完整端到端逐來源驗收仍為 0。
四份原先超過 64 MiB 的 dictionary/catalog 現均在 256 MiB 單檔、2 GiB 整批
預算內完整擷取。這些是歷史區間及欄位數，不是新執行次數。
一般 benchmark metadata 深度仍為 2，196 個深度/目錄預算缺口原樣列明；
訓練計時另以深度 15 遍歷四個明確 roots，這部分沒有深度缺口。

markets/ablations/operations 三個 roots 本輪可見 676 份 training timing；
另補收 benchmarks 深層的 362 份，共 1,038 份。前一基線為 1,072 份；
與本輪共同路徑 675、前次獨有 397、本輪獨有 363。
`training_cohort_membership.json` 已把 397 份前次原始內容從舊凍結證據重新解壓並驗 SHA；
這些原路徑目前均不存在，沒有推定原因，也沒有刪除或改寫來源。
前次 `workflow_speed_20261021/collection_v3/` 與 retention 都保留；
不同路徑也可能是相同原始內容，不能相加當成新執行次數。

## 尚未完成與後續量測

1. 第一批全歷史 cProfile 仍見 `add_values`、字典配置、原生 decode 與 Python
   逐列組裝。本次已實作固定欄位物件與有界批次投影，見下方第二輪比較。
   各批 `history_final.prof` 都帶剖析開銷，不能混入未剖析 ABBA；目前沒有
   「剩下只能靠更快硬體」的證據。
2. 各來源仍缺下載 → 正規化 → 持久寫入 → 獨立讀回的逐來源完整 wall time。
   FinLab unchanged receipt 保留的是舊 bytes/rows，未製造吞吐量；共享 cycle
   不重複歸到每個端點。TEJ cache/fingerprint/scope-preparation 已可索引，
   非空/OCR/UI 及實際配額上限仍須對應工作實測。
3. 所有訓練設定的當日完整 fold、來源冷重建、跨節點與全容量 NAS 恢復尚未重跑。
   `remote_speed_admission.json` 使用目前登錄的 SSH 端點與既有 host verification，
   結果為 `No route to host`；不以連線失敗耗時冒充遠端 GPU 計時。
   lab203/NAS 依既有收據通道觀測，未猜測登入或另建 owner。
4. 開盤行情 → 訊號 → 紙上執行／broker 成交、Windows 物理冷啟動及長期尖峰分位數
   尚無本輪整鏈測值；離線 forward、HTTP 200 和 active 都不補這些證明。
5. 第一輪觀測時，工作目錄修正尚未切換到正式 immutable service release。live namespace
   獨立讀回另因 `.env` 與 PID1 當前 inode 不一致被拒絕，保存於
   `native_owner_source_readback_failed.json`；沒有宣稱目前 owner 的完整輸入
   namespace 已驗收，亦未重啟或改寫其環境。正式採用仍須固定經審閱 source、
   重做當前 admission，沿 canonical plan/apply/verify/status 完成。

任務 `workflow-speed-baseline-20261021` 保持 active。可改善但尚未實作、尚未測到
或遠端暫不可觀測的項目均保留在清冊；沒有將這次階段交付標成「全部完成」。

## 第二輪實作與驗證（台北時間 10/21–10/22）

使用者要求依規劃實作後，先凍結當前實作作為本批控制組；本批證據位於
`artifacts/benchmarks/workflow_speed_optimization_20261021/round2/`。
第一性原理上的改善是減少每個真實觀察點的配置、跨語言呼叫與重複查驗，
完整觀察、財務計算、來源證明與公開輸出均保留。

| 工作與比較口徑 | 控制組 | 本批候選 | 結果 |
| --- | ---: | ---: | --- |
| 完整歷史重建、公開投影、JSON/gzip/ETag、解壓與 JSON 讀回，最新 ABBA 平均 | 13.272984 秒 | 15.470900 秒 | 本批慢 16.56%；速度未驗收 |
| 上述四個新程序的 peak RSS，同變體平均 | 1,151,096,832 bytes | 936,726,528 bytes | 少 18.62% |
| 完整離線暖推論，同程序 6 組 ABBA，全部 warm 樣本中位數 | 255.772902 ms | 214.492742 ms | 時間少 16.14% |
| 同一批推論，平衡樣本平均 | 246.912564 ms | 222.907648 ms | 時間少 9.72%，6 組候選平均皆較快 |
| 完整程式身分查驗，12 組交錯 CPU 樣本中位數 | 19.308882 ms | 3.358587 ms | 子階段少 82.61%，不是整體推論倍數 |
| 兩份相同的大型實際字典，完整讀取、SHA、分類、索引寫入與讀回，ABBA 平均 | 34.289896 秒 | 7.305895 秒 | 時間少 78.69%，不是全樹掃描或下載速度 |

歷史點改用內部固定欄位物件；只在需要公開逐列結果時建立字典。
同一時間戳重用既有時區與策略時段判定，保留 09:01–13:30 及隔夜時鐘。
超過既有 columnar cap 的來源仍有界 streaming，以約 4 MiB 的行群組呼叫
原生投影；單一超長來源行仍依原 parser 處理。多物件同一行、跨行物件、
非物件、巨大整數、非有限值、surrogate 及無效 JSON 保留逐行 fallback。

`history_whitespace_final_abba.json` 的四次比較固定同一份 537,535,491-byte
來源；372,946 點、9 series、全部摘要、17,312,615-byte HTTP body、
5,523,452-byte gzip 及 ETag 完整一致。控制組使用第二輪開始時的現行程式，
不能把不同批次的改善倍數相乘。時間不含網路與瀏覽器；
本次工程批次串行，背景服務照常運作，未主張磁碟冷快取或獨佔主機。

早期 `history_compact_batch_abba.json` 為 14.671986 → 10.600402 秒；
後來補上「無效初始資本＋巨大整數 equity」的原先短路順序，以六個案例驗證
fallback、略過及錯誤次序，再重測最終實作。曾與全量索引工程程序重疊的
`history_final_order_abba.json` 為 19.137181 → 19.862019 秒，沒有加速；
原始結果完整保留，後續工程批次改為串行。這些批次分開呈現，
不混合平均，也不以財務邊界變更換取速度。

串行的 `history_final_serial_abba.json` 曾測得 12.485356 → 9.514882 秒，
時間少 23.79%。最終檢查另發現批次解析會錯誤去掉非 JSON 空白：改為僅移除
JSON 規定的四種空白，保留 VT、FF、NBSP、EM SPACE 的原先拒絕行為及純空白行
的略過規則。修正後重新完整比較得到上表的負結果；此結果不隱去，不將差異
全歸因於背景工作，也沒有據此認定歷史速度已改善。較低 RSS 可重現，速度原因
尚未定位；本任務沒有執行正式服務部署。稍後獨立觀測到外部流程切換部分候選
來源，詳見本節最後的新 owner 讀回，不能當成本任務已完成正式效能驗收。

推論身分查驗改監測實際程式與既有候選父目錄：每次 stat 的成員由
1,583 個減為 231 個，完整相依身分仍為相同的 218 份程式。
新增缺失套件、namespace、刪除相依、修改後還原 mtime，及外部符號連結
目標出現仍會觸發重建；原先頂層 dangling symlink 的拒絕條件亦保留。
已載入程式與磁碟不一致仍要求全新 owner，沒有放寬來源或 cache admission。

完整模型 oracle 位於 `inference_identity_abba.json`：四個程序各保留
10 次完整模型輸出，所有 tensor 的 shape、dtype 與完整 bytes 均一致，
權重、逐股決策、解釋及近期報酬亦同值。這批 oracle 暫存所有模型輸出，
不能當成正式記憶體用量；整體時間平均 302.280795 → 314.291817 ms，
沒有加速，前後控制組分別為 352.402284 與 252.159306 ms，原結果保留。

`inference_identity_paired_comparison.json` 另用正式輸出生命週期，在同一個
模型與 panel 程序中做 6 組 ABBA，兩變體各 12 次 warm；沒有額外保留模型
輸出。其訊號、shape、checkpoint 與完整程式身分和上述 oracle 一致。
兩種程式相依圖均先準備，故不將本批首次值當冷啟動改善；同一 canonical
來源讀鎖涵蓋整批，network、訊號發布與 orders 均為 0。樣本不足以推定 p95/SLA。

第一個只改 stat 呼叫寫法的候選未測到加速，已撤回；原始樣本與變更描述
保留於 `code_identity_abba.json`、`rejected_stat_spelling.patch`。
首次完整推論亦因官方來源 writer 持鎖被拒絕，未執行模型；後續等待自然
完成後才取得讀鎖，等待沒有混入推論耗時。

固定欄位與批次解析實作的 `round2/history_final.prof`／`history_residual_profile.json` 仍見
373,771 次逐列 `add_values`：profiled self 3.459 秒、含子呼叫 7.630 秒；
原生批次 decoder 86 次、self 1.241 秒，JSON encoder self 1.122 秒。
這些區間有剖析開銷且部分相互包含，不與上表未剖析時間混算。
此診斷早於最後的資本解析順序修正；完整真實資料輸出仍與最終版本相同。
它們是下一輪候選的依據，尚不支持硬體頻寬／算力已飽和的結論。

修正空白後另做完整 `history_after_whitespace_diagnostic.json`：仍為 86 次原生
批次解碼、逐列 projection fallback 為 0，輸出及完整 wire SHA 仍一致。
`add_values` profiled self 3.312 秒、含子呼叫 7.813 秒；原生批次解碼 self
1.200 秒。這排除本批退回逐列 decoder 的解釋，尚不能解釋未剖析批次的差異。

回歸包含 private JSON、completed cache、來源相依、歷史投影、公開 gateway、
測速器與索引器：`focused_regression.log` 共 382 項通過。
最後以 `history_parse_order_regression.log` 驗證 19 項歷史財務案例，包含報酬
重設基準、累積資產、槓桿參考跌破零、時鐘、模式退場、磁碟投影恢復與上述短路。
索引器與測速器後續合併回歸 58 項通過；加入分類重用關係的獨立讀回後，
索引器 35 項再次通過，見 `collector_final_readback_regression.log`。
最終 JSON 空白修正以 `private_json_whitespace_regression.log` 的 57 項驗證。
各批部分重疊，不能相加作為獨立案例數；未跑全 repo suite。

第二輪一般 benchmark metadata 最終搜尋深度為 32，目錄預算 500,000；訓練
深度為 15。走訪前排除歷次本工作凍結證據樹，不可讀目錄仍明列為缺口。
新完整歷史階段保留 ms/seconds 原單位與來源穩定性，變動來源不當成功樣本。
最終索引與獨立讀回放在 `round2/collection_v3/` 與
`round2/collection_v3_readback.json`；此為歷史計時欄位清冊，不是新增執行次數。

第一次擴大掃描將非計時字典也凍結，造成 4 GiB 證據預算耗盡及 50,342 項
問題；`round2/collection/` 保留為未採用嘗試。第二次改為先分類，剖析卻發現
每個非計時純量都建立路徑和執行正規表達式，且相同的 217,315,593-byte
字典在不同世代反覆解析；該工程掃描中斷證據保留於 `collection_v2/` 與
`collector_interruption_diagnostic.json`，沒有終止正式服務。

最後只對容器與真正計時候選建立路徑；非計時相同內容先完整讀取、讀前讀後
stat 及 SHA，再重用分類。只快取有界的負面分類，不常駐大型解析字典；跳過
解析的檔案明確標為 `json_parsed=false` 並綁定來源路徑與 SHA。顯式收據優先
凍結，含計時欄位的內容仍完整保留；escaped keys 與 UTF-16 仍走完整 parser。
`collector_inspection_abba.json` 的四個新程序各讀取兩份真實字典的固定副本，
434,631,186 bytes 全部重新讀取／SHA，輸出分類與索引完整一致。最終讀回會
重驗分類重用的來源身分關係；未保留的非計時 payload 不冒充已凍結讀回。

最終全量索引掃描 372,519 份候選，分類為 18,342 份已擷取及 354,177 份
非計時內容；有 18,160 次完整 SHA 相同後的負面分類重用。保存 233,932 筆
計時、24,841 個情境群組、705,774 個待審閱計時欄位，計入預算的原始證據
為 1,836,066,759 bytes。在設定的 roots／深度／預算內，擷取問題與遍歷缺口均為 0。
獨立讀回通過 12 個輸出、6,474 份不同凍結原始內容及全部分類重用來源關係。
2,399 個 registry 來源中，1,363 個有精確 ID 綁定階段紀錄，逐來源完整端到端
驗收仍為 0；上述計時筆數不是新執行次數。

全量索引完成後才進行最後的 JSON 空白修正比較；新收據另由同一維護中的
collector 收入 `final_correctness_receipts/`，並以
`final_correctness_receipts_readback.json` 獨立讀回。補充批次僅以顯式收據及
頂層搜尋更新，不重跑全量清冊；兩份索引存在重疊，不能相加。
`round2/optimization_comparisons.csv` 保留正負 before/after，
`round2/optimization_acceptance.json` 綁定最新原始收據、完整輸出與未驗收邊界。

16:14 UTC 的 `remote_speed_final_admission.json` 再次回覆 `No route to host`；
完整遠端 fold 與 GPU ceiling 仍無本批測值。16:15 UTC 的舊 Discord owner
獨立查驗因 `.env` 與 PID1 視圖 inode 不同被拒絕，保留於
`collection_v3_inputs/native_owner_readback.json`。

最終驗收發現 Discord 已在台北 00:24:13 由外部流程切換：原 MainPID
2944796／Invocation `459883376ef2484fbe65512ee75074ac` 改為 3558213／
`3ccd6ed045214d4d8c618f4c988a70af`，Python PID 為 3558289。
本任務沒有執行重啟。16:35 UTC 的 `native_owner_final_readback.json` 對新 owner
重新查驗：1,599 份 immutable source、readonly mounts、live-input inode 均通過，
owner 在讀回期間穩定；舊 `.env` 拒絕不再當成新 owner 的狀態。

新 release `20261021T162054Z-121a0c4854ca` 的 namespace 內，completed-cache
與 dashboard 檔案 SHA 符合本輪候選；`private_json.py` 仍為空白修正前的
`ff0a4f3…`，工作目錄最新為 `49bb321…`。這是部分 mounted source 的證明，
不是完整 imported-code／原生推論工作流驗收；最後的 decoder 修正仍未進入該
release。公開 dashboard 的 PID／Invocation 未變，亦不推定已載入工作目錄更新。

量測結束後另觀測到並行的零目標訊號顯示修改，涉及
`scripts/serve_public_dashboards.py` 與 `build_dashboard_signal_page`／
`_detail_query_lock`。原始量測版本完整凍結；AST 查驗顯示其餘模組定義、完整
歷史路徑及離線 `_prepared` wire 依賴未變，保留這些非本任務變更。工程驗收
不涵蓋新訊號顯示功能，也不當成目前所有 HTTP 路由或完整服務的重新驗收。

來源逐項端到端、新 owner 完整工作流與最後修正採用、開盤到成交及完整 NAS
恢復仍依原驗收邊界保留待辦；本批未證明整套工作流已達計算極限。

## 第三輪：2026-10-22 持續實作

本輪沿用 `workflow-speed-baseline-20261021`，原始目標與前兩輪原始證據保留。
來源凍結與新收據位於 `artifacts/benchmarks/workflow_speed_optimization_20261021/round3/`。

### 完整歷史曲線的 Python 配置與參數搬運

既有 profiler 已證明 JSON 的 86 次原生批次解析正常，沒有退回逐行解析。
剩餘熱點是逐列投影：原生解析先產生 tuple，呼叫端再展開成 20 個 Python
參數，並以 Python dataclass 初始化每個點的 23 個欄位。修改保留 tuple 傳遞，
在可用時以既有 msgspec 的 C 實作初始化固定欄位物件；仍維持 Mapping 介面與
可變的顯示報酬欄位，沒有原生模組時沿用 slotted dataclass 備援。
金融算式、解析次序、時間與去重規則均未更動。

| 本輪相同來源的新比較 | 原 slotted 物件與參數展開 | 原生物件與 tuple 傳遞 | 邊界 |
| --- | ---: | ---: | --- |
| 完整重建、摘要、公開投影、JSON/gzip 與獨立讀回 | 16.925471 秒 | 12.967892 秒 | 耗時少 23.4% |
| 相同流程的 CPU 時間 | 16.138126 秒 | 13.016790 秒 | 少 19.3% |
| 含匯入與 CLI 的完整子程序 | 19.268268 秒 | 15.260036 秒 | 另計程序開銷 |
| peak RSS 平均 | 933,367,808 bytes | 936,956,928 bytes | 約多 0.38%，不宣稱記憶體改善 |

`history_native_records_abba.json` 預先固定兩組 ABBA，八個全新程序，各變體四次。
537,535,491 bytes 原始輸入前後 SHA 不變；所有 372,946 個點、9 條曲線、
摘要、17,312,615 bytes HTTP 與 5,523,452 bytes gzip 的 SHA 均與既有 golden
一致。各組候選平均均較快，沒有與不同批次的倍數相乘；正常背景 owner 保留，
helper 與 OS cache 可能已暖，不含實際網路或瀏覽器。

最終九份相關測試檔共 541 項通過，涵蓋完整歷史、session projection、JSON、
HTTP 介面、benchmark 與清冊。日內／隔夜及 sampled／minute-v1／minute-v2
六種組合均核對原生與無 msgspec record 的備援，包含完整輸出和 session shard
capture。其後只再修改清冊的兩個 metadata 函式；最後 42 項清冊測試另保留
`metadata_final_regression.log`。這些批次部分重疊，不能相加；未跑全 repo suite。

### 速度清冊本身的成本

canonical `scripts/profile_workflow_speeds.py` 加入序列化的 monotonic wall／CPU
分段計時，區分程式清冊、環境、範圍、既有服務與來源收據、工程命令、訓練
發現與解析、benchmark 發現、路徑篩選、證據解析、輸出彙整及最終雜湊。
完整命令仍由 agent-workflow 的 child-wait 收據計時，最後 collection.json
落盤與獨立驗證不混入較早結束的內部分段。

原路徑篩選對每筆長路徑重跑 `Path.resolve()`。第一版單純快取父目錄沒有改善：
完整篩選及寫出／讀回 127.297804 → 129.129360 秒，負結果保留於
`path_selection_abba.json`。第二版遞迴重用目錄前綴有小幅改善，但 profiler
仍指出反覆建構／雜湊 `Path` 的成本；該中間版本保留於 `path_prefix_abba.json`。

最終版以字串處理已解析的前綴，保留每筆檔案符號連結檢查、父目錄 generation
失效、失敗時原有 resolve 行為及排除規則。快取最多 128 個目錄；並測試直接與
祖先連結換向、失效連結、相對路徑、無法讀取父目錄及連結迴圈。

`path_string_abba.json` 固定 372,519 個候選，以四個新程序 ABBA 比較。
完整篩選、全部結果寫出及解壓／JSON 讀回為 82.248156 → 17.404004 秒；
包含匯入、輸入讀取及收據寫出的完整子程序為 **84.130998 → 19.107955 秒，
耗時少 77.3%**。四次均選出相同的 372,445 個路徑，完整內容與 gzip SHA 一致。
其餘 74 個名稱依原有 workflow-speed 排除規則處理；完整收集仍另加入顯式收據，
沒有刪除原應擷取的證據。這是路徑階段的比較，不是整個清冊的 77.3% 加速。

程式清冊另跳過非入口 AST 中不會產生結果的第二次參數／計時呼叫走訪；仍完整
讀取、雜湊及解析每份來源。benchmark 發現保留走訪、連結與預算規則，改為
一次篩選子目錄，並先檢查檔名再為 JSON 建立 Path；`.json` 的 dotfile 語意與
自訂 predicate 路徑均保留。`metadata_preparation_abba.json` 的四次新程序使用
完整固定的 2,293 份 Git-visible 來源與實際 benchmark 目錄，均得到相同的
1,029 個入口、372,445 個路徑、零遍歷缺口及完整輸出 SHA。

此組完整準備流程含實際私有 Git index、發現、篩選、寫出與讀回，完整子程序
平均 98.661968 → 92.171121 秒；CPU 97.329254 → 91.197094 秒。
這是本機各兩次的觀測，未修改的篩選階段也有快取／背景波動；不把 6.6% 當成
穩定的全系統加速率，也不與前面不同批次的倍數相乘。

### 最終完整清冊與逐段時間

`collection_full/` 在設定 roots／深度 32／目錄預算 500,000 內掃描
372,547 個候選，保留 **234,106 筆速度樣本、24,949 組摘要**。
18,370 份候選有保留內容，354,177 份為非計時內容，18,160 次完整 SHA
相同後重用負面分類；原始證據計入預算 1,836,347,270 bytes。
擷取問題及遍歷缺口均為 0。`collection_full_readback.json` 獨立重驗 12 個
輸出、6,502 份不同原始內容及所有樣本、摘要、吞吐量和分類重用來源關係。

2,399 個 registry 來源中，**1,362 個**有精確來源 ID 綁定的階段紀錄；
逐來源完整端到端驗收仍為 **0**。前輪為 1,363，當輪數值各自保留。
另有兩份既存設定錯誤保留於 config inventory；零遍歷缺口不代表全流程已驗收。

| 本次完整收集的序列階段 | wall 秒 | 本程序 CPU 秒 |
| --- | ---: | ---: |
| 程式清冊與量測來源凍結 | 18.232318 | 17.917871 |
| 環境觀測 | 0.038480 | 0.017577 |
| 設定、來源、服務範圍清冊 | 0.119367 | 0.117986 |
| 服務／HTTP／TEJ／儲存既有收據 | 0.127336 | 0.125616 |
| 既有 acquisition 收據 | 2.729433 | 1.693924 |
| 工程命令收據 | 0.412478 | 0.399263 |
| 訓練計時發現 | 2.057689 | 0.091294 |
| 訓練計時擷取 | 8.800380 | 8.188969 |
| benchmark 發現 | 72.942095 | 72.485354 |
| benchmark 路徑篩選 | 15.109314 | 15.102603 |
| benchmark 證據讀取與解析 | 206.981262 | 129.709628 |
| 彙整及輸出 | 25.986519 | 24.802057 |
| 最終輸出雜湊 | 1.682166 | 1.577277 |

以上序列區間合計 355.218838 秒，不含最後 collection.json 落盤；完整命令
357.286312 秒，另加獨立讀回 19.842906 秒。本程序 CPU 不含 subprocess CPU。
機器可讀逐段資料見 `round3/collector_phase_timings.csv`，比較值與正負結果見
`round3/optimization_comparisons.csv`。沒有用前輪不同清冊的總耗時推算因果加速。

完整收集時來源為 `d7e38e9a…`，之後才修改 metadata 兩個函式；最新來源為
`2180046d…`。AST 核對證明僅 `visible_code` 與 `discover` 的函式本體不同。
完整 metadata ABBA 證明輸出同值；最終版另用同一 canonical collector 跑
`collection_scoped_final/` 及獨立讀回，收錄新比較與收據。此補充批次刻意保留
depth-zero 缺口，不能當成另一次全量掃描，也不能與完整清冊的樣本數相加。
補充批次的 10,144 筆樣本、3,714 組摘要及 2,330 份不同原始內容均獨立讀回
通過，13 個 collector 分段也通過核對。

### 尚未解除的瓶頸與採用邊界

目前清冊最大區段為證據讀取／解析的 206.981262 秒；下一步應在原有 collector
細分 stat/read/SHA、分類、解析、壓縮與樣本擷取，再依完整輸出同值的比較決定
是否原生批次化或重用。wall 與 CPU 的差額還含等待及排程，不能全歸因於磁碟。
完整歷史的診斷 profile 仍有逐列投影、JSON 編碼及公開欄位清理成本；
`history_residual_diagnostic.json` 保留細目，profile 計時不混入一般比較樣本。

17:34 UTC 的 `remote_speed_admission.json` 仍為 `No route to host`，完整
遠端雙 GPU fold 與 GPU ceiling 沒有本輪測值。同期
`native_owner_readback.json` 驗證目前 owner 的 1,599 份 immutable source、
readonly mount 與輸入 inode；completed-cache 符合工作目錄，新歷史 record
與最後 decoder 修正尚未進入該 release。這是 mounted source 證明，不代表
完整 imported code 或原生工作流採用。本任務重啟／部署及新訓練工作均為 0。
17:52 UTC 封存前再次查驗，結果相同，見 `remote_speed_sealed_admission.json`
及 `native_owner_sealed_readback.json`；兩次讀回期間各自 owner 穩定。

本輪驗收與獨立證據讀回見 `round3/optimization_acceptance.json` 及
`round3/optimization_acceptance_readback.json`。逐來源端到端、原生 owner
完整工作流、遠端完整 fold、開盤到成交及完整 NAS 恢復仍是未完成的獨立邊界；
本輪沒有證明所有工作流已測完或達到硬體計算極限。

## 第四輪：完整證據檢查與正式 C++ 模組（2026-10-22）

本輪沿用原任務及 canonical collector。產物位於相同根目錄的 `round4/`，
控制來源為 `2180046d36e29084aa78ccc3f43ce3c0133759786b503d3598d45ab64b906d57`。
初次完整 cProfile 命令遇到主機重開機，收據保持 `interrupted`、exit code 為 null，
部分輸出及 `recovery_observation.json` 保留；沒有把中斷當成完成。
隨後從完整候選清單重新執行證據檢查診斷，並由另一個程序讀回。

### 第一性原理與完整輸入

此段工作必須讀取每份候選、核對 metadata 與 SHA、分類、解析需要的 JSON、
保留原始證據、擷取欄位、寫出全部報表及獨立讀回。時間下限同時受檔案系統
metadata、實際讀取、Python 物件配置、必要計算與輸出約束；更換語言只可能
改善其中一部分。先減少語意上可證明不必要的重複工作，再比較原生化及並行。

`inspection_profiled_diagnostic.json` 記錄完整 372,550 份候選的 cProfile。
帶 instrumentation 的檢查時間為 364.309919 秒，完整工作為 400.598198 秒，
只用來診斷，不混入一般速度平均。主要 self time 包含 read 125.918351 秒、
`reported_metrics` 66.346866 秒、regex 23.407319 秒、`isinstance` 16.776489 秒、
decode 14.817780 秒、stat 14.507134 秒與 SHA 6.249450 秒。
`reported_metrics` 累積 124.782800 秒，與子呼叫重疊，不能與其相加。

所有候選含非計時檔案都複製為獨立私有 fixture，保留原相對路徑與順序；
沒有以 live source hardlink 冒充固定資料。總量 **372,550 份、9,383,953,790 bytes**，
其中 107 份為 explicit receipt，與診斷輸入相比改變數為 0。
準備花費 448.828031 秒，明確排除於比較，正式 collector 不增加這個複製步驟。
完整 manifest SHA 為 `f4bb9b16c7d423b7dbb85bc1f1c61cb5e990596de2c2fe881672572b0959044d`。
失敗的首次準備與 CLI 參數錯誤也保留，成功 v2 不覆寫它們。

每次比較均重新啟動程序與輸出目錄，讀取全部固定候選，保留 18,373 份 captured
來源及 354,177 份負面分類的識別，擷取 705,929 個原始欄位與 2,006 筆正規化
benchmark 樣本。六份輸出逐 byte SHA 相同；另一個程序重讀 3,739 份不同的
保留原始內容，並核對所有 372,550 筆輸入 SHA/bytes。負面 payload 沒有重複
壓縮保留，但每次實際讀取及計算 SHA，核對固定 manifest；不是略過這些檔案。

### 完整比較、改善與負結果

最後 protocol 先宣告 `A B C D E E D C B A`，每個變體各兩個新程序。
包含讀取、SHA、分類、解析、壓縮、全部 CSV/JSONL/manifest 輸出與獨立讀回；
完整 subprocess wall 也包含匯入、載入輸入 manifest 及最後樣本落盤。
本程序 CPU、RSS 不包含另一個讀回程序的資源；其 wall 已含在 subprocess。

| 最終同批完整變體 | 完整 subprocess 平均秒 | 本程序 CPU 平均秒 | 與原始版的耗時差 |
| --- | ---: | ---: | ---: |
| 原始 Python | 168.705213 | 139.785752 | 基準 |
| 欄位快取、必要字元證明及 CSV 優化，Python 循序讀取 | 138.632992 | 119.038937 | 少 17.8% |
| 相同軟體優化，C++ 走訪、循序讀取 | 131.043917 | 104.483231 | 少 22.3% |
| Python 加四執行緒預讀 | 266.630498 | 326.808363 | 多 58.0% |
| C++ 加四執行緒預讀 | 267.569967 | 328.490719 | 多 58.6% |

採用循序讀取的 C++ 方案，未採用預讀。預讀試驗有八筆 pending、最大 2 GiB raw
buffer 上限，開始前檢查記憶體 headroom；沒有無界讀取。即使資源充足，完整
工作仍退步，因此保留為負面證據，不只引用單一局部較快數字。
C++ 相較同批優化 Python 的完整時間約少 5.5%；其 peak RSS 平均
2,343,372,800 bytes，Python 為 2,173,466,624 bytes，約多 7.8%。不是記憶體改善。

先前四次欄位快取 ABBA 為 302.979278 → 275.285654 秒，但樣本呈現強烈暖快取
與 I/O 波動，逆序原始版也比前一筆候選快；只保留探索觀測，不宣稱穩定的 9.1%
因果加速。另一組四次 C++ 走訪 ABBA 為 196.210837 → 185.501925 秒，約少 5.5%，
本程序 CPU 134.399180 → 116.614977 秒，RSS 約多 7.8%。三組 cohort 分開保留，
不用不同批次速度相乘，也不用 cProfile 的時間比較一般執行。

沒有 flush OS cache 或停止背景 owner；節點為 Ryzen 5800X3D、16 個 affinity，
無 cgroup CPU quota，背景負載、pressure、headroom、boot ID 在 protocol 及每次
樣本前後保留。各變體只有兩筆觀察，沒有獨佔主機、disk-cold、統計顯著性或
硬體極限的宣稱。最終比較見 `inspection_final_pipeline_mirrored.json`，
其餘為 `inspection_key_cache_abba.json` 與 `inspection_native_walk_abba.json`。

### 維護實作與正確性

共用 `inspect_benchmarks` 仍由 canonical collector 呼叫；沒有建立另一條正式清冊路徑。
欄位名稱快取最多 4,096 筆，過長名稱走原判定且不進快取；快取不保留 producer 值。
解析仍執行原先需要的所有 JSON 驗證。只有可由相同的時間關鍵字集合證明
不會產生欄位、沒有 escape/UTF-16/32 疑義且結構淺的內容才省略樹狀走訪；
保留原來分類、`json_parsed` 狀態、深層錯誤與 explicit receipt 行為。
CSV 保留欄位順序、BOM、CRLF、巢狀 JSON、引號、空值與負零；自訂 mapping
仍走既有行為。所有完整比較輸出皆驗證，不以減少輸出換速度。

`stockagent/workflow_metric_tree.cpp` 使用 CPython C API 一次走訪整棵已解析樹，
沿用 Python 的欄位可見性與 finite-number callback；值保留原 Python 物件，
不轉換整數或浮點。字典順序、陣列 index、Unicode/pointer escaping、敏感欄位
及單位不變。深層或非 plain JSON 容器完整回到原 Python，數值溢位等錯誤照常
傳出。借用 reference、GIL 及遞迴處理採用
[CPython dict API](https://docs.python.org/3.12/c-api/dict.html) 與
[exceptions API](https://docs.python.org/3.12/c-api/exceptions.html) 的契約。

`stockagent/workflow_metric_tree.py` 綁定 C++ source SHA、Python 完整版本、
SOABI/cache tag、architecture 及 module SHA，原子發布 build cache，失敗保留
來源、compiler log 與 failure receipt。無額外 Python 套件或自動安裝。
預設 `--metric-backend auto` 只重用已驗證快取，沒有快取時用 Python；
`--metric-backend cpp` 明確編譯，編譯成本包含在該次 workflow；
`--metric-backend python` 可直接對照。快取在既有 `workflow_speed_` 排除範圍。
完整收集另凍結實際 C++ source、binary、build receipt，獨立讀回交叉驗證來源、
二進位、receipt 與記錄的 Python runtime。

維護來源 `7092035357ea6b60a96463e5cd0bae97e04acdaf99f69160a987b7228445c0cb`
通過 **68 項**聚焦測試及 compile/lint/diff 檢查，包含 large-int、signed zero、
NaN/Inf、單位、順序、Unicode redaction、deep/custom fallback、refcount、
失敗/timeout 編譯保留與 frozen build/runtime 的負面讀回測試。
三份 10,000-case 差異檢查分別核對欄位快取、原生走訪與最終完整 capture 分類/
錯誤；另保留 CSV、字元證明及預讀語意測試，數目不與 68 重複加總。
`pyproject.toml` 宣告 C++ source asset。`wheel_backend_readback.json` 驗證隔離
source snapshot 可離線建 wheel，三份新來源與 canonical byte 相同，且解包後
以 `python -I` 編譯及載入成功；沒有向正式環境安裝套件。
`implementation_mapping.json` 另核對五個 hot-path 函式的 AST：只有
`list(reported_metrics(...))` 換成正式 dispatch；C++ 本體只有 module 名稱及
起首註解不同。這項結構證明與正式模組的全量輸出同值共同綁定採用版本。

### 正式採用驗證與完整清冊

`canonical_adoption.json` 使用正式來源、正式 loader 與實際編譯的 binary，
對全部固定輸入重跑，再獨立讀回；六份輸出與比較批次完全相同。
首次初始化及編譯 1.548939 秒，含在該次完整 subprocess 294.580319 秒內。
該次期間的 node readback 顯示 CPU/load 與 I/O pressure 上升，與先前鏡像批次
條件不同；原值保留，不用單次驗證推算另一個加速率。採用 binary SHA 為
`8a487699306432474f280d3a025190826935dfe358755d117fa1eda50442dec1`，
C++ source SHA 為 `028983edd1162c8c3f7cf84d904b4725147b9f0957ec7d6140ea11318e51d067`。

隨後以同一正式 collector 的 `--metric-backend cpp` 重新收集完整設定範圍，
沒有縮小 roots、深度或目錄預算。`final_collection_arguments.json` 保存精確命令。
本輪 **372,559 份 benchmark 候選、235,715 筆速度樣本、26,465 組摘要**，
706,255 個未經語意審核的 producer 欄位仍與正規樣本分開；兩份既存 config error
仍留在 inventory。擷取問題與遍歷缺口均為 0，不代表所有來源端到端已完成。

獨立讀回核對 12 份輸出、6,518 份不同原始內容、所有正規樣本與摘要、18,160 條
負面分類重用關係及 13 個 collector 分段；`native_metric_backend_verified=1`。
registry 仍為 2,399 個來源，當輪 1,361 個有精確來源 ID 的 stage timing，
逐來源完整端到端驗收仍是 0。前輪的 1,362 與其他樣本數不覆寫或相加。

| 正式完整收集的序列階段 | wall 秒 | 本程序 CPU 秒 |
| --- | ---: | ---: |
| 程式清冊與量測來源凍結 | 12.211942 | 11.283340 |
| 環境觀測 | 0.042032 | 0.012281 |
| 設定、來源、服務範圍清冊 | 0.112332 | 0.099578 |
| 服務／HTTP／TEJ／儲存既有收據 | 0.109897 | 0.098456 |
| 既有 acquisition 收據 | 10.116509 | 4.802581 |
| 工程命令收據 | 0.494622 | 0.468322 |
| 訓練計時發現 | 7.112569 | 0.077693 |
| 訓練計時擷取 | 7.926332 | 7.484375 |
| benchmark 發現 | 72.381215 | 71.994862 |
| benchmark 路徑篩選 | 18.117556 | 18.055695 |
| benchmark 證據讀取與解析 | 210.959282 | 117.568482 |
| 彙整及輸出 | 18.220702 | 17.584594 |
| 最終輸出雜湊 | 1.066082 | 1.064001 |

序列區間合計 358.871072 秒，完整命令 360.071644 秒，另加獨立讀回
11.804274 秒。不同輪完整清冊的即時收據、數量、cache 及背景負載不同，
不能拿這個總時間與第三輪作因果速度比較；22.3% 只來自固定輸入的同批比較。
機器可讀資料見 `collector_phase_timings.csv` 與 `optimization_comparisons.csv`。

05:41 UTC 的 readonly admission 仍為 `No route to host`，沒有遠端硬體測值。
同期 owner namespace 核對 1,603 份 immutable source、readonly mount 及穩定 PID。
當前 `private_json.py` 與 completed-cache 已匹配 release，dashboard history 的
工作目錄版本仍不匹配；`loaded_service_revision_verified=false`，不能把 mounted
source 當成所有 imported code 或完整 native owner workflow 採用。這輪正式採用
證明只針對新啟動的 canonical collector。重啟／部署與新訓練工作仍為 0。

### 剩餘成本及驗收邊界

完整清冊仍以證據讀取／解析 210.959282 秒及 metadata 發現 72.381215 秒最大。
wall 與 CPU 差額同時可能含 I/O、排程或 reclaim 等等待，不能全標成磁碟速度。
優化後另跑完整固定輸入 cProfile，保留全部輸出及獨立讀回；其 instrumentation
時間不投影為正常速度樣本。診斷見 `inspection_canonical_residual_diagnostic.json`。
此診斷的完整 subprocess 為 251.928880 秒，檢查 213.954193 秒／本程序 CPU
164.282990 秒，六份輸出與同批固定輸入仍相同。read self time 66.174394 秒、
`_metric_descriptor` self 20.370808 秒／累積 30.941235 秒，後者被呼叫
**54,352,375 次**；C++ `rows` self 15.563656 秒、decode 12.911438 秒、
stat 10.973422 秒。這些是帶 instrumentation 的診斷，累積值不可重複加總。
因此後續除了共用 C++ 結果欄位名稱，也應單獨比較直接使用既有 bounded key
cache、避開數千萬次 Python wrapper callback 的完整效果；長名稱須保留原判定。

本輪驗收與另一程序的證據、原始樣本、平均值及完整清冊讀回，分別為
`optimization_acceptance.json`、`optimization_acceptance_readback.json`。
仍未證明逐來源完整端到端、遠端完整雙 GPU fold、原生服務完整工作流、
開盤到 broker fill 或完整 NAS 恢復，也未證明所有工作流已測完或已達硬體極限。
