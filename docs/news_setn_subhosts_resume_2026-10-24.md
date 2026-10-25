# 三立財經／房產已知網址接續

本輪接續全部新聞來源目標，先處理已有首頁／REP 審查、仍缺文章證據的財經相關來源。
既有來源共 951 鍵；這是本機清冊，並非全網媒體或完整歷史。

**最新驗收（台灣時間 10/24 07:16）**：富房網固定 83 個已知網址中，
56 篇原生正文均已入庫，剩餘 27 個原範圍排除獨立保留，待修原生正文為零。
本輪只續辦 v3 剩六篇，詳見下方 v4；先前各階段及其封存請求保留作為歷史證據。

## 執行前方案

已完成的 `setn-subhosts-review-20261023` 保留首頁和各自主機 REP：
`stock.setn.com` 的首頁明示 301 至 `http://inews.setn.com/`；
`fuhouse.setn.com` 首頁為 200。這些證據未證明文章轉址或正文，因此不重跑首頁審查，
也不把它們直接當成已允許整站下載。星聞的官方 10 秒間隔仍保留，這輪先辦財經及房產。

1. 由原唯讀 queue transaction 固定兩個來源現有網址、狀態與一個代表；不猜文章 ID、
   不重排已有正文、不複製其他 provider 的正文。
2. 重用原 private primary-review HTTP root 與 `NewsHTTP`；各來源僅一筆原網址，
   保留原 REP、公開位址、流量、拒絕及專有 owner checks。先核對容量與既有拒絕期限，
   不重啟正式服務、不清除節流／robots cache。獨立主機可並行，每個主機串行。
3. 文章請求不自動跟隨轉址。3xx 只保存實際 Location／狀態與請求證據；
   後續要逐跳核對，不由相同 ID／標題推定別名，也不將 HTTP 任意改成 HTTPS。
   200 只在無會員／付費／挑戰訊號時私人保留原件，供離線核對，尚非正文入庫。
4. 根據實際原件決定沿用既有 adapter 或擴充共同實作，保留完整文字、表格、署名缺值與
   原始時鐘精度。發布日、更新日、觀察時間分欄；未聲明時區／角色不補造。
5. 接線後才由原下載 owner 下載，驗證 raw／body／批次／耐久收據與兩端面板；
   對已完成正文做離線 parity，對缺文只提交一次相容 repair。失敗留證，不改成完成。

範圍與允許的觀察會綁定固定 request／原件 SHA，不能從這份文件取得存取、訓練或分享權利。
來源拒絕時保留其結果，下一步依實際限制找公開原始來源或授權介面。
沒有完整穩定窗口前，不宣稱官方或理論最大速率；樣本也不能推出 246 篇的完成 ETA。

## 已有文章證據與接線方案

只沿用[兩次相異文章觀察](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/setn-article-witness-20261024/observation.json)：
財經原網址明示 301 至同篇的 `http://inews.setn.com/` 路徑，尚未跟隨、未視為正文別名。
富房網的原網址 200，已私人保存 44,283 bytes、完整原件 SHA 與 terminal trace；
不是另外抓一次來補正文。

富房網使用獨立原生版型。新增解析沿用共用新聞管線：逐一核對 literal URL、
canonical／OG／dable ID、頁標題及完整 `articleBody` 段落，保留文字圖說、負值、比率與風險提示；
圖片 alt 不是第二份圖說。實際原件印「財經中心／綜合報導」，頁首作者空白，
所以保留團隊署名及人名缺值。原件 meta 時間明列 `Z`、可見時間只有分鐘且沒有時區，
兩種宣告分欄保存；沒有推定兩者為同一瞬間，沒有捏造歷史首發時間。

[離線原件與共用 writer 驗收](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-native-20261024/offline-readback.json)
確認 11 個段落與獨立 lxml 結果完全一致；shadow 入庫一次、重複執行新增零筆、重開後可讀，
其他 81 個 head 不變，額外來源 HTTP 為零。
原 HTML 與 trace 只轉為既有診斷原件格式，轉格式時間另外記錄，來源觀察時間、原授權內容與
原 provider ID 不變；未把轉格式時間冒充當時的抓取或發布時間。

接線前須通過共享規則測試與既有 SETN／iNEWS 完成正文比對，才進行一次有恢復 guard 的新聞 owner 交接。
保留原 HTTP provider ID、pacing 與拒絕狀態，移入同一主機的原 REP，先由共用 writer
直接採納這一份原件，再讓原 owner 接續其餘本機已知缺文；不開另一個爬蟲、不啟用全站探索。
在同一次已規劃交接的獨佔 HTTP 窗口，只觀察財經 301 實際寫出的下一跳一次；
沿用已啟用 iNEWS 的正式 root、已保存容量間隔、REP 與拒絕期限，不另外重啟 owner，
不自動追下一跳，不把診斷觀察當正文或舊網址別名。
上述交接及 pilot 正式入庫現已完成；其餘缺文仍按實際狀態接續，不能把接線成功當成全部完成。

## 正式入庫與未完成範圍（10/24 05:16）

[正式耐久資料及本機／HTTPS 讀回](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-native-20261024/complete-readback.json)
已驗收同一個 fresh／ready 原服務 owner、951 個來源鍵與富房網 **1 篇正文**。
這篇直接採納上述原件，沒有重發來源請求；raw／body SHA、實際批次及原觀察時間均成立。
原已知 82 網址在正常增量期間增加一筆 metadata，當時分母為 83：完成 1、待抓 53、
進行中 1、身分／版型失敗 17、範圍排除 11。這是當時快照，不是靜態全歷史總數。
既有 SETN／iNEWS 1,286 篇、知新聞 664 篇、遠見 137 篇與 309 個已結案問題保留。

沿用已通過的 303 項語義、771 項共用及 1,286 篇原件比對，不重跑整批。
首次交接因完整載入器補入預設說明、造成候選指紋不同而拒收；保留失敗收據，補上明確說明，
以 102 項包含完整載入器的檢查確認後才正式採納。
[來源正規化及相容性讀回](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-native-20261024/normalization-readback.json)
保留原 HTTP provider、流量桶、拒絕期限及十秒工程間隔。
[實際載入版本稽核](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-native-20261024/runtime-audit.json)
與雙端面板通過後已解除臨時恢復 guard；沒有更動其他服務。

財經只沿既有原網址的明示 Location 續觀察一次：
[`http://inews.setn.com/news/1681579` 實際 301](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-native-20261024/stock-hop-readback.json)
至 `https://inews.setn.com/news/1681579`。沒有重播第一跳、跟隨最後一跳或接受別名；
兩次 301 的證據各自保留，尚非 164 個 stock 網址都有相同轉址或正文。

接續只處理真正未完：富房部分原件的 `articleBody` 有直接非 `p` 子節點，v1 尚未證明能完整解析，
因此拒收而非靜默遺漏。先重用已保存原件及兩份失敗 metadata，再取得必要的新原件建立新版 grammar；
不把 parser 失敗說成來源資料錯誤。第三方轉載／範圍排除與缺文分開，財經最後一跳另做原生身分驗證。
完整歷史、穩定上限與完成 ETA 仍未驗證；一篇的入庫時間不能外推整站完成時間。

清冊註記更新後，[156 項清冊／正式設定檢查](../artifacts/operations/agent-workflow/runs/news-fuhouse-review-https-ledger-20261023T212203-7b307b4e/run.log)
通過；第一次檢查拒收 `evidence_urls` 中的 HTTP 字串，改為在實際轉址欄位保存原字串，
沒有任意替換協定或放寬清冊驗證。
[05:22 本機／HTTPS 再讀回](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-native-20261024/post-review-web-readback.json)
確認富房註記已出現在兩端、951 來源及同一 ready owner；這次只讀檢查沒有來源 HTTP、queue 寫入或服務重啟。
當時仍 1 篇正文，正常 owner 已處理至 44 筆身分／版型失敗、20 筆範圍排除、1 筆進行中、17 筆待抓。

## 下一個未完步驟：失敗原件樣本

保留上述已驗收原件、303／771／102／156 項檢查及 1,286 篇比對。
正常 owner 已留下兩類 metadata-only 失敗，沒有可重放的 raw：`1907073` 的正文結構、
`1703006` 的署名。先核對這兩筆仍缺文及既有 evidence；若沒有原件，在一次 guarded
canonical HTTP 獨佔窗口，各觀察原網址一次，只存符合原私人公開原件範圍的回應。
沿用原 provider／root／REP／已保存間隔及拒絕期限；不重抓 pilot、不重送 stock 兩跳，
不清除每日診斷額度，不跟隨轉址，不修改 queue；finally 交回原服務。
先把兩份原件離線定位拒收根因，再決定是否需要新版 parser；尚未證明的結構不放寬為接受。

此步驟已完成：[兩筆原網址觀察](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-remaining-evidence-20261024/observation.json)
各回傳一次 200，48,054／43,088 bytes；沒有接受正文、改 queue、重播 pilot 或 stock 兩次 301。
[固定原件離線重現](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-remaining-evidence-20261024/offline-failure-summary.json)
確認 `1907073` 是 `figure > picture／figcaption` 與頁首記者＋明示 profile 的組合；
`1703006` 是「財經中心／姓名」的印刷署名，頁首仍空白。新版不得從後者推定記者職稱，
也不能漏掉 figure 的文字圖說、把 alt 再當圖說，或任意修改來源時區。

[原服務及雙端網頁恢復讀回](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-remaining-evidence-20261024/owner-restored.json)
在台灣時間 05:28 驗收 ready／fresh、951 個來源及恢復 guard 解除。
首次稽核讀到服務交接前的舊 owner 快照，保留為未接受的結果；只重新唯讀核對當前 owner，
沒有再次重啟或重送兩篇。
此時 83 網址已完成當輪一般嘗試：正文 1、身分／版型未完 46、署名未完 9、範圍排除 27；
所以目前真正需要 parser／署名修復的是 **55 筆**，範圍排除另列，不能直接當資料錯誤。
兩份新原件現已可重用；下一輪從離線 v2 grammar 開始，不再執行上述原件觀察。

## v2 續辦方案

固定上述 83 網址及 55 缺文，擴充原 `setn_fuhouse_native_article`，版本與來源指紋分開；
v1 的已完成正文仍保留。原生 figure 結構驗證與 iNEWS 共用，僅讀文字圖說，
不下載媒體、不把 alt 再當圖說；頁首明示記者與 profile 原字串分欄，
財經中心／姓名保持「印刷署名、職稱未明」與缺失 profile，不由 meta publisher 補人名。
兩種來源時鐘繼續原樣保存，不推定時區或歷史精確發布時間。

先做三項互相獨立的驗證：合成語義／共用契約、兩份既有原件與獨立 lxml 完整文字、
全部 1,286 篇 SETN／iNEWS 加 1 篇富房舊正文 parity。
原件轉成既有 reviewed-native 格式時保留 HTTP terminal trace 的當時觀察時間，轉格式時間另列。
shared writer 在 shadow 只採納兩篇一次、重播新增零篇、重開可讀，其餘 heads 不變。
通過後才在一次 guarded 原 owner 交接中採納這兩篇，再各續抓其餘 53 筆一次；
不改 27 筆範圍排除、不重抓 pilot、不重播 stock 兩跳、不重置容量或拒絕期限。
最終以 raw／body／收據／實際批次／同一載入版本及雙端網頁驗收；來源最大速率與全歷史 ETA 另計。

### v2 已驗收部分（10/24 05:52）

[候選驗收](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v2-20261024/qualified.json)
通過 340 項語義、788 項其他共用檢查與 58 項聚焦檢查；
[1,287 篇既有正文比對](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v2-20261024/retained-parity.json)
保留完整 raw／body／署名／原時鐘，耗時 58.05 秒。共用回歸原先唯一失敗是新增升級測試的
設定 fixture 不完整；補齊 personal-research、ABI 與明確私人輸出設定後，58 項聚焦檢查通過，
沒有把原失敗 run 改寫為成功，也沒有為 fixture 修正重跑全部原件。

升級檢查發現原通用 parser migration 會把署名失敗直接歸零為待抓。
FuHouse v2 改沿用既有 native-body repair：保留原 state、attempts、due、錯誤與排除項目，
由明確 repair mailbox 入列。這個 metadata 變更與 body parser 分開核對，
[兩個實際失敗 head 的 shadow 演練](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v2-20261024/failed-shadow-readback.json)
確認共用 submit／consume／retained writer 只採納一次，其他 81 個 head 不變。
獨立文字 oracle 初次誤丟來源空段落，ABI1 approval 初次缺少既有顯式審核欄位且排序不符；
均保留失敗證據，修正驗證／manifest 後才採納，沒有修剪正文或改寫來源時間。

[正式交接](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v2-20261024/materialized.json)
已直接採納兩份 05:25 保存的原件，額外 publisher HTTP 為零。
[新 owner 載入稽核](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v2-20261024/runtime-audit.json)
與[資料／批次及雙端網頁讀回](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v2-20261024/initial-readback.json)
已驗收 **3 篇正文、83 個 head、951 來源鍵**，同一個 fresh／ready owner，恢復 guard 已解除。
第一次網頁讀取遇到兩次原子快照替換，回傳不可驗證而拒收；保留該快照，僅重做唯讀驗收，
沒有第二次重啟、重新下載或放寬資料一致性。

[剩餘 53 筆明確 repair](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v2-20261024/remaining-repair-request.json)
已交原 owner，3 篇完成資料與 27 筆範圍排除未入列。這批已在 06:01 完成全部實際嘗試：
**新增正文 0、署名未驗證 42、文章身分／版型未驗證 11**；沒有把失敗算成完成。
從 request intent 到最後一筆終態為 532.994 秒，53 次來源 HTTP；正文解析合計 4.785 秒 wall／
2.554 秒 CPU，請求 slot 等待合計 449.318 秒。這是目前十秒工程 pacing 的單輪實測，
不是官方最大速度或修復完全部未知格式的 ETA；目前有效正文產出主要受未解 grammar 限制。

[06:08 最終原件／批次及本機、HTTPS 讀回](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v2-20261024/final-readback.json)
確認 3 篇正文、53 個未完成 head、27 個原範圍排除、951 來源鍵及同一個 fresh／ready owner。
最終 guard 檢查修成只讀確認已解除狀態，避免對早已卸載的臨時 unit 再 stop 而誤判失敗。
原失敗驗收收據與快照保留，沒有重送 53 筆或重啟服務。

期間其他來源的既有六小時追新使原先「head 完全不變」驗證拒收：
[遠見一篇](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v2-20261024/unrelated-gvm-incremental-readback.json)
只升級原有署名／grammar 註記版本，獨立重解析新舊原件確認正文、署名與來源時鐘相同；
[iNEWS 14 篇](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v2-20261024/unrelated-setn-incremental-readback.json)
正文、印刷姓名與原時鐘不變，其中三篇的作者 profile 連結由來源實際改為 `/author/reid`。
保留兩次觀察及兩份原件，不改寫舊 profile、不請求 profile 或推定人的真實身分。
所有其他 head 與原結案問題仍逐項比較，沒有以正常追新為由放寬無關差異。

[清冊與正式配置 116 項檢查](../artifacts/operations/agent-workflow/runs/news-fuhouse-v2-review-ledger-ch-20261023T220936-9e508678/run.log)
已通過；目前註記保留 v1 歷史快照，獨立列出 v2 的兩篇原件採納與 53 次零新增的修復結果。
[06:11 清冊更新後的本機／HTTPS 讀回](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v2-20261024/post-review-web-readback.json)
確認新審查時間與 3／42／11／27 分類已出現在兩端，仍是同一個 fresh／ready owner。
網頁沿用既有公開 schema，沒有公開原文、私有規劃問題、金鑰或控制介面；本次唯讀驗收沒有來源 HTTP、queue 寫入或重啟。
[未完成清單](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v2-20261024/remaining-work.json)
固定 42 筆署名、11 筆身分／版型，後續只對仍缺原件的代表做有界觀察。
這三篇已完成原件、53 筆已完成的 v2 嘗試、27 筆範圍排除、stock 兩次已證實 301 均不整批重播。

## V3：只續辦剩餘原生格式（10/24）

沿用 v2 已驗收的 3 篇正文及 53 筆終態，沒有重播該批次。
[四個新代表的原始回應](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v2-residual-evidence-20261024/observation.json)
只取各失敗類別最早／最新 metadata 的未完成網址：1682273、1856883、1710011、1789959。
每篇一次，保留實際 HTTP trace、raw SHA 與原觀察時間；沒有改 queue 或跟隨其他網址。
[代表觀察後的服務與雙端讀回](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v2-residual-evidence-20261024/owner-restored.json)
先確認原服務恢復，才開始 v3 接線。

新增格式為首段 `記者姓名／地點報導`、沒有地點的頁首記者加相符首段署名、
`財經中心／姓名報導`、直接 `h3` 小標，以及能逐項對應正文標題的原生目錄。
保留署名原字串與地點；「報導」本身不作為「記者」職稱證明。
目錄重用既有 iNEWS 驗證，要求唯一 ID、順序、純頁內連結及標籤等於正文標題，
只排除已證明重複的導覽文字，正文標題保留一次。
未匹配、隱藏、未知巢狀或署名矛盾仍拒收；未推定時區、人身分或歷史首發時間。

[四份原件的獨立 lxml 全文比對](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v3-20261024/offline-readback.json)
全部成立；[舊 3 篇原件／署名／時鐘比對](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v3-20261024/retained-parity.json)
完全一致，非 FuHouse 的 SETN 模組 AST 不變，因此重用既有 1,287 篇驗收，不再掃整批。
[366 項語義及 814 項共享檢查](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v3-20261024/qualified.json)
通過；兩組有重疊，不相加當成不同測試數。
[安裝設定後的 83 項檢查](../artifacts/operations/agent-workflow/runs/news-fuhouse-v3-installed-final--20261023T223514-0455b9c1/run.log)
另核對 v3 選擇器、來源範圍及原 HTTP 桶。

[失敗 head 的影子入庫驗收](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v3-20261024/failed-shadow-readback.json)
沿同一 mailbox 游標續辦：首次 100ms 僅建好表，不能把一個 slice 當成整份工作完成；
另修正私人證據 JSON 的二進位 ID 編碼，沒有重送原請求。
四篇入庫、再次執行新增零篇、重開可讀，其他 79 個 head 不變。

[正式採納四份保留原件](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v3-20261024/materialized.json)
已完成，3 → 7 篇，沒有再次請求來源，保留實際來源觀察時間。
首次交接碰上既有設定監測器正常 exit75 重載，停止快照屬上一代，故在提交前拒收；
先等同一新程序 ready，再沿原 guard 接續未提交的採納，沒有重排四篇或放寬停止檢查。
之後的交接應把正式設定切換納入受監督窗口，避免設定自動重載與預定交接競爭。
[遠見另一篇正常六小時追新](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v3-20261024/natural-change-readback.json)
亦獨立核對新舊 raw／正文／署名／時鐘與耐久批次，不重抓遠見。
其重試次數依原 `schedule_recent_rechecks` 重新計算，不能要求跨追新週期單調遞增。

[最終逐篇原件／獨立全文／批次及雙端讀回](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v3-20261024/final-readback.json)
於台灣時間 **10/24 06:49** 通過，同一個 fresh／ready owner、951 來源鍵、富房網 **50／83 篇**。
本輪共新增 **47 篇**：上述四份保留原件，以及原排程 49 個相異缺文目標產生的 43 篇。
原 3 篇不重抓，27 個既有範圍排除不入列；53 筆 v2 嘗試及 49 筆 v3 嘗試均已封存。
每篇均有 raw／body SHA、獨立 lxml 完整文字、來源署名／原時鐘及耐久批次證據。

[本輪實測](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v3-20261024/metrics-readback.json)：
49 次來源請求均為 HTTP 200，43 篇成功入庫、6 篇解析失敗；有效產出為 **43／49（87.8%）**。
由封存意圖到最後終態為 **323.457 秒**，回應共 2,228,839 bytes；
解析共 2.974 秒 wall／2.495 秒 CPU，請求 slot 等待共 268.290 秒。
原自適應控制器在實際缺文工作上由 10 → 5 → 2 秒逐步升速，HTTP 同時請求峰值為 1，
最小實測起始間隔 2.000102 秒。2 秒仍屬 `ramping`，不是已驗證穩定上限；
不另抓已完成文章做校準。不同目標與格式的前後兩輪不可直接推算倍數加速或全歷史 ETA。

[獨立未完成清單](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v3-20261024/remaining-work.json)
在 v3 驗收時只留下下列六篇，另列 27 個既有範圍排除及 50 篇已完成正文；
六篇現已由下列 v4 接續完成，不能將此歷史表再次派送。

| 網址尾碼 | 現在狀態 |
| --- | --- |
| 1713394 | 署名待驗證 |
| 1717776、1718690、1723983、1725381、1740023 | 身分／原生版型待驗證 |

v4 沿此未完清單執行：先找可重放原件，再對確實無原件的六篇各觀察一次；
未重抓 50 篇、未重播封存批次，也未重跑原 stock 兩次 301。

## V4：剩六篇原生正文已完成（10/24）

[六個未完成網址各一次原始回應](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v3-residual-evidence-20261024/observation.json)
均為 HTTP 200，共 276,705 bytes；沒有跟隨轉址、派送 50 個已完成或 27 個排除網址。
當時只保留原件而未入庫；[原服務恢復及雙端讀回](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v3-residual-evidence-20261024/owner-restored.json)
先確認 50／83、原 owner 已恢復，才開始解析修復。

實際缺口為整段純 `strong` 署名、直接 `h2` 小標、目錄連到原生 `p > strong[id]`，
以及 `ul > li` 內的粗體、文字與換行後續文。
目錄直接重用既有 iNEWS v8 驗證；共享函式不變，仍須逐一證明標籤、唯一 ID 與順序相符。
清單逐項保存全部原文字，不補造條列號、不排除換行後續文；未知或隱藏結構仍拒收。
粗體署名只接受首段唯一無屬性、無巢狀 `strong`，並使用既有明確署名 grammar，
不把「報導」推定為記者職稱，也不從 meta 補人名或歷史發布時區。

[六份原件的獨立 exact lxml 全文驗收](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v4-20261024/exact-text-readback.json)
包含文字、段落順序及換行；[既有 50 篇原件／正文／署名／時鐘比對](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v4-20261024/retained-parity.json)
完全一致。非 FuHouse AST 不變，因此沿用已完成的 1,287 篇共享證據，沒有再掃整批。
[404 項原生語義、852 項共享檢查](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v4-20261024/qualified.json)
通過，兩組有重疊，不相加當成互異測試數；
[正式設定 120 項檢查](../artifacts/operations/agent-workflow/runs/news-fuhouse-v4-installed-catalo-20261023T231339-aee38071/run.log)
另核對完整載入器與原 HTTP 身分。

[六個實際失敗 head 的影子驗收](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v4-20261024/failed-shadow-readback.json)
沿 canonical mailbox／writer 完成六篇、再次採納零篇、重開可讀，其他 77 個 head 不變；
版型升版不清除原失敗、重試次數或期限。
[正式採納](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v4-20261024/materialized.json)
亦新增六篇，來源追加 HTTP 為零，沿用六份原件的實際觀察時間與原始回應。
這次先由獨立恢復 timer 保護、排空原 owner，再切換正式 catalog，避開 v3 的重載競爭。
一次過早啟動的唯讀 ready 檢查遇到採納收據尚未生成而失敗；待採納完成後核對同一新程序，
沒有重啟程序、重排請求或重複入庫。

[正式 runtime、56 篇 raw／body／exact lxml 全文、耐久批次及雙端頁面](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v4-20261024/final-readback.json)
於台灣時間 **10/24 07:16** 驗收通過；同一 fresh／ready owner、951 來源鍵，恢復 timer 已解除。
原 HTTP 流量／拒絕身分、Knews664／GVM137、309 個既有結案問題均保留。
沒有以完成文章重測速率，原 2 秒／1 slot 仍是 `ramping` 觀察，不是已證明的穩定上限。

| 目前固定範圍 | 筆數 | 後續操作 |
| --- | ---: | --- |
| 已完成原生正文 | 56 | 沿原增量 owner 接續新 metadata，不重抓此批 |
| 未完原生正文 | 0 | 六個 v3 失敗均已修復，不重播舊請求 |
| 原範圍排除 | 27 | 保留原證據與限制，不改列解析待修 |

[完整剩餘清單](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/fuhouse-v4-20261024/remaining-work.json)
將 56 篇完成與 27 筆範圍排除分開；此結果不代表整站歷史已完整或全 951 來源已完成。
Anue 優先、CNA 暫緩；後續只處理其他未完來源與真正新增資料，不重播 V2-53／V3-49，
也不重跑 stock 兩次已完成的 301 觀察。

## 文章原件結果與首次接線方案

原 `NewsHTTP` 與 private primary-review root 的兩個獨立主機觀察已完成，正式 queue 不變：

- `stock.setn.com/news/1681579` 實際 301 至 `http://inews.setn.com/news/1681579`，
  回應零正文；沒有跟隨、改協定、接受別名或借用既有 HTTPS 正文。
- `fuhouse.setn.com/news/1656200` 實際 200，44,283 bytes；canonical／OG URL 與原網址一致。
  原件為單一標題、頁首分鐘標籤、meta `article:published_time` UTC 標籤與完整 editor，
  首段明列 `財經中心／綜合報導`，11 個段落保有圖說與投資警語。

[固定範圍](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/setn-article-witness-20261024/before.json)、
[逐來源 HTTP 證據](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/setn-article-witness-20261024/observation.json)。

接線先擴充原 SETN pure native validator，不複製 collector 或 HTTP 排程。
新增房產站精確 host／path／標題／meta／editor 驗證；編輯單位與個人角色分開，
保留原始兩種時鐘及精度，meta UTC 不等於已驗證的實際發布時區或歷史 PIT。
首次接線只接受 source-proved 財經中心署名；後續 v2／v3 增補上述逐一驗證的本頁署名格式，
沒有 profile 的印刷記者仍保留缺值，不從 publisher meta 猜姓名。
整個 editor 要能說明每個 written 節點；新結構、第三方／付費／挑戰保持失敗證據。
先以 synthetic 語義與共同生命週期測試，再對固定原件離線核對文字／原值與時鐘；
通過才加入當前來源清冊並交回原服務，一個 pilot 入庫及讀回後才派其餘已知缺文。
首次接線不改 Anue 優先、其他來源 scope、流量桶或拒絕期限，不重播已完成的知新聞請求。
