# 全來源新聞：下載器、CPU、故障範圍與完整性清冊

## 原生網址與單主機故障修復：2026-10-14 23:03，Asia/Taipei

**原先鎖定的 18 個工程失敗 URL 已全部真正補入正文，獨立原件／收據讀回通過。**
23:03:54 的公網快照為台灣好新聞 **2,835／2,853 個已知 URL 已存正文**，另有
13 個連線／重試與 5 個網址身分／正文待修；這些分類互斥且合計等於分母。
數字是 URL head，不是獨立新聞篇數，也不是全站歷史完成率。面板已同步
[正式下載進度](https://penguin72487.dpdns.org/news/)，頁面不抓來源、不輸出原文。

最小修復集中在既有 7 個共用模組，沒有新增 HTTP client、SQLite writer 或平行
管線。`taiwanhot_visible_article_v2` 保留原 v1 作者、原文、分鐘發布精度與付費／
身分檢查；只接受原本本機已知的裸網域 HTTPS 原生頁與帶 query 的原生頁。
**送出的仍是原網址**，不去掉 query、不改主機、不在失敗後猜替代頁。回應的
同一新聞 ID、無 query 的 OG／canonical 與當篇 JSON-LD 聲明仍須一致；匹配
當篇的付費聲明不能藉裸網域或 query 變體漏過。JSON-LD 身分／權利判斷維持
線性掃描，兩種 parser 對三份既存原件的正文 SHA、作者與時鐘完全不變。

舊失敗只能在精確 v1 capture／parser fingerprint 下遷移：25 個可確認的網址
缺陷進入既有有界 retry lane，其中原始鎖定 18 個全部補齊並逐筆讀回。attempts、
due、原錯誤與觀察時間保留；未知身分、正文不足、403／429、租約與既存正文
不會一律重設或算完成。這是當次修復的證據，不是未來任意失敗的自動解禁。

實際舊 `web.taiwanhot.net` 故障發生在 robots TLS 憑證驗證，沒有 HTTP 回應或
正文。新版獨立收據確認失敗當時 `taiwanhot.net`、`www.taiwanhot.net` 有近期
完整成功回應，所以只隔離故障 origin 的 REP 負向檢查點，以 30→60→120→240→300
秒有界退避並在到期後重試。原始請求／TLS 錯誤統計保留，容量學習不把這類
已證明的單 origin 失敗當成整個 provider 壅塞；沒有虛增 HTTP 回應或正文數。
證據過期、路由錯誤、非 REP TLS 故障以及來源 403／429／Retry-After 仍保守處理。
無法驗證 robots 的 origin 保持禁止抓正文，沒有關閉憑證驗證或換身份／代理；
這也符合 [RFC 9309](https://www.rfc-editor.org/rfc/rfc9309.html) 的 unreachable
處理與 [Requests 憑證驗證契約](https://requests.readthedocs.io/en/latest/user/advanced/#ssl-cert-verification)。

完整新聞回歸 **10,063 passed／182 files，283.84 秒**；原 9,991 個 nodeid 保留，
新增 72 個自包含案例。第一輪完整回歸正確發現啟用 TaiwanHot 後的預設來源集合
斷言未更新；只加入這個已審查來源，原測試其餘內容及其他測試檔 SHA 未變，
沒有移除權利、付費或時鐘檢查。另有部署後 **388 passed／13.68 秒**。
測試用凍結 monotonic 時鐘與超過 16 個控制樣本的驗證器呼叫均曾被中止／拒絕；
原失敗 run 留存，修正 fixture／呼叫後重驗，不改正式 pacer 或放寬驗證上限。

正式 owner 於 22:59:24 正常 drain、exit 75，22:59:35 自動重載。啟動的收據恢復
期間仍顯示舊心跳，未被當成功；23:03:29 驗收確認新 owner、載入身分與心跳一致。
台灣好新聞、CMMedia、PeoPo 合計 **3,389 個既存正文 head** 的 SHA、Parquet 與
擷取時間不變，19 份原 cohort 收據與 43 份舊 TLS 故障原件不變；18 個新修復
正文及 16 個舊正文另作獨立讀回。當時尚未到期的 CMoney 拒絕期限保留，新
owner 在驗收前對該來源零 HTTP 請求。這不是全湖逐筆 integrity 驗收。

全來源公網快照仍是 **936 個來源鍵、66 個 recipe、6,304,465 個已知 URL、
3,826,736 個已存正文 URL**；來源鍵不等於獨立新聞網站，870 個未登錄鍵尚待
分群接線與權利／端點查核。CNA 按使用者要求暫緩。台灣好新聞剩餘 5 個工程
案例需檢查無 slug、舊 focus 順序、當篇身分與正文，而憑證故障也未宣稱根除；
保留完整證據後續修，不將它們草率標成永久不可取得。因尚有未解來源、正文與
近期有效吞吐不足，`all_history_complete=false`，面板不填虛構的全來源完工時間，
也沒有宣稱官方或理論最高速度。私人讀取不自動等於轉載／訓練／匯出授權。

證據：[完整回歸／精確斷言變更](../artifacts/data_quality/news_goal_stage01_20261009/taiwanhot_identity_recovery_20261014/regression2_after.json)、
[原件雙 parser 重播](../artifacts/data_quality/news_goal_stage01_20261009/taiwanhot_identity_recovery_20261014/offline.json)、
[發布前 guard](../artifacts/data_quality/news_goal_stage01_20261009/taiwanhot_identity_recovery_20261014/guard.json)、
[18 個正文／TLS 隔離／期限保護](../artifacts/data_quality/news_goal_stage01_20261009/taiwanhot_identity_recovery_20261014/after.json)、
[公網面板完整對帳](../artifacts/data_quality/news_goal_stage01_20261009/taiwanhot_identity_recovery_20261014/handoff.json)。

## 台灣好新聞原生接線：2026-10-14 22:14，Asia/Taipei

**已由唯一正式下載 owner 真正新增 1,197 個台灣好新聞正文 URL head；不是
診斷原件或 HTTP 200 計數。** 22:14:27 的獨立公網讀回為 2,713 個已知 URL、
1,197 個已存正文、1,498 個待抓／重試與 18 個解析／網址身分待修；分類互斥，
合計等於分母。網頁維持唯讀，不輸出原文或呼叫新聞網站。
這是當次快照，後續進度以[新聞面板](https://penguin72487.dpdns.org/news/)為準；
全來源仍為 `all_history_complete=false`，沒有把未接線或來源拒絕當作完成。

這輪集中擴充既有 downloader 的 `taiwanhot_visible_article_v1` 契約：一般頁、
focus 頁、已知 `?p=` 網址的實際同 ID 跳轉共用一個 adapter。核對真正 H2、
OG／canonical 身分、唯一正文容器、原署名與分鐘精度發布時間；只排除經版型
證明的照片和尾端延伸閱讀。實際 HTML 的不合法外層 `<p>` 包住區塊元素，
兩個 parser 會形成不同 DOM；修復結構判斷，不刪原文、不退回全頁文字。
理財周刊等原署名保留，不能把轉載標成台灣好新聞原創。

來源 recipe 使用既有私人 `known_url_only` 增量契約：原 2,666 個待核實 URL
全數仍在，22:07 的讀回另確認 41 個後續本機已知 URL 正常納入；不是凍結三篇
樣本，也沒有啟用全站 discovery、猜新聞 ID 或另開 HTTP／SQLite writer。
原文只供既定私人本機用途；版權與逐篇權利檢查不變，不據此宣稱對外重用、
轉載、模型訓練或跨主機匯出授權。Anue 最高優先與其他來源設定／身分未變。

正式 owner 先 drain 再以 exit 75 自動重載一次；新程序於 21:56:34 啟動，
舊心跳的 warming 讀回沒有被算成接線成功。當次 579 個既存正文 head 的 SHA、
時間、Parquet 與身分欄位、19 份原 cohort 收據不變；另獨立抽樣 8 個新正文
及 8 個原正文，核對原件、作者、時鐘、耐久收據與兩 parser 的讀回。
這不是全湖逐筆 integrity 驗收，也不宣稱這輪晚於到期的讀回證明了期限前零請求；
未到期退避的獨立證明仍見下方 v6 驗收。

完整新聞回歸 **9,991 passed／180 files，404.08 秒**，原 9,882 個 nodeid／測試
檔 SHA 保留；新 109 例為自包含 fixture，沒有依賴忽略目錄的原件。另有 478 項
聚焦與部署後 503 項清冊／面板語意測試通過。候選驗證器最初漏併 catalog 預設
欄位，正確拒絕身分不一致；改為完整 canonical 設定後重新核對三份原件，原文
SHA／作者／時鐘不變。原失敗 run 保留，沒有改已抓資料或放寬斷言。

尚待根治的 18 個工程問題是 17 個裸網域 HTTPS URL 被過窄的身分 gate 拒絕、
1 個原生網址帶額外 query；不能據此宣告文章不存在。另外舊 `web.taiwanhot.net`
的連線錯誤沒有 HTTP 回應，來源等待可能拖慢正常主機，需要保留拒絕／退避
邊界後做最小共通修復。尚未量測到官方或理論最高速；近期測量過期時，面板
正確不填保證完工時間。CNA 仍按使用者要求暫緩，870 個未登錄來源鍵待處理。

證據：[完整回歸與原測試 pin](../artifacts/data_quality/news_goal_stage01_20261009/taiwanhot_native_20261014/regression_after.json)、
[canonical 原件重播](../artifacts/data_quality/news_goal_stage01_20261009/taiwanhot_native_20261014/offline_deployed.json)、
[正式 owner／新舊正文讀回](../artifacts/data_quality/news_goal_stage01_20261009/taiwanhot_native_20261014/after.json)、
[公網完整 JSON handoff](../artifacts/data_quality/news_goal_stage01_20261009/taiwanhot_native_20261014/handoff.json)。

## 共同派送修正：2026-10-14 21:17，Asia/Taipei

**派送 v6 已上線，但來源拒絕仍是未完成項目。** 這輪只修改既有
`NewsQueue.claim` 的候選順位及派送版本，不新增下載管線、HTTP owner、SQL
或索引，也不放寬正文／作者／時鐘／逐篇授權。完整新聞回歸 **9,882 例通過**，
保留原 9,870 例及原斷言；新 owner、574 個既存正文 head 與 19 份原批次
收據對帳成功。全來源目標維持 `all_history_complete=false`。

根因是較新的 `source_access_denied` 文章，在每次來源退避到期後又以
latest-first 壓過較舊、未嘗試的文章。因此 v6 在已有可派送首次下載的 regular
lane 中，延後這種已知拒絕；它仍保留原 oldest-due retry lane 的有界服務。
stored／undated 的原份額、502／逾時的 latest-first、future due、有效 lease、
所有來源設定、capture／parser fingerprint 與拒絕期限不變。來源等待仍在
claim 前檢查，沒有換身分、繞過拒絕或提前清空退避。

20:48:26 正常重啟唯一正式 `stockagent-news-fulltext.service`；初始化期間仍是
舊心跳的那次讀回被正確拒絕，沒有把 `active` 當更新成功。
本輪新 owner 的有效讀回為 **20:51:11（UTC 12:51:11）**。
當時 PeoPo 的新期限 21:01:50 完整保留，新 owner 在期限前送出 **零** 個
PeoPo HTTP 請求。574 個正文與 19 份原收據不變，另抽樣 16 個既存正文核對
原件、作者、時鐘、SHA／Parquet／耐久收據；這不是全湖 integrity 驗收。

到期後，正式 owner 真正選中原 `attempts=0` 的另一篇文章，新增
`frontier_access_refusal_fairness_deferrals` 可見計數，證明沒有再被同一篇舊
拒絕餓死。但該網址也回 **403**：21:07:52 的獨立讀回為 98 個 complete、
84 個 pending、2 個 retry、1 個 retry_cooldown、77 個逐篇授權未核實，
**新增正文仍是零**。21:16 的下一次首次下載也受拒絕，新等待約 21:31:53。
這不是 CSS 修復失敗或 API 官方配額證據，不能填保證抓完時間，也不能把
嘗試數、重排、缺授權或拒絕算成正文完成。後續以面板當下時間為準。

台灣好新聞另核對三個真實原件：一般頁、focus 頁與一個原 `?p=` 網址的
正常 HTTP→HTTPS→原生頁跳轉。它們使用 H2 標題、`name="og:url"`、唯一
`.main_content .content_wrapper article > .news_content`、真正 reporter 與
分鐘精度時間；舊樣本實際標示 **2021-02-23 13:25**，不是 metadata 入列日。
理財周刊署名必須保留，不能標成台灣好新聞原創；延伸閱讀也不能假定是原文
替代源。原件／收據 SHA 已核對，但 **這三份是診斷原件，接受正文數仍為零**；
原生 parser 接線、來源範圍與其餘已知 URL 仍待完成，不把可訪問網站永久放棄。

公網完整讀回亦通過：21:11:37 快照為 **936 個來源鍵、66 個 recipe、
6,303,984 個已知 URL、3,823,219 個已存正文 URL**，不是獨立媒體或去重篇數。
逐來源數字及互斥處置總數對帳，API 維持唯讀／no-store，頁面讀取不呼叫來源，
不輸出原文。第一次未壓縮 HTTP/1 公網讀回以 `IncompleteRead` 失敗，保留
原 run；本機／公網 HEAD 顯示正常 builder，而普通 HTTP/2＋壓縮完整 JSON
讀回成功。只為驗證器加上整次 20 秒／傳輸 15 秒／8 MiB 邊界，沒有更改
來源 collector、身分或 gateway。未改 UI，不拿此輪宣稱新增瀏覽器尺寸驗收。

證據：

- [完整回歸與碼／測試 pin](../artifacts/data_quality/news_goal_stage01_20261009/access_retry_fairness_20261014/attempt2/regression_after.json)。首次回歸的 paid-cache `None` 案例尚未產生盤點 checkpoint；受限正文沒有被接受。相同測試檔單獨重跑及第二輪完整回歸通過，原失敗保留，沒有放寬測試。
- [正式 owner／原件讀回](../artifacts/data_quality/news_goal_stage01_20261009/access_retry_fairness_20261014/attempt2/after.json)、[尚未到期來源退避驗收](../artifacts/data_quality/news_goal_stage01_20261009/access_retry_fairness_20261014/attempt2/after_http.json)。
- [實際首次下載派送與真實 403](../artifacts/data_quality/news_goal_stage01_20261009/access_retry_fairness_20261014/attempt2/dispatch_progress.json)：只驗收派送，不宣稱成功下載。
- [台灣好新聞三種版型／原件 witness](../artifacts/data_quality/news_goal_stage01_20261009/access_retry_fairness_20261014/taiwanhot_native_layout_witness.json)、[完整公網／本機 handoff](../artifacts/data_quality/news_goal_stage01_20261009/access_retry_fairness_20261014/handoff.json)。

下一步保留 Anue 最高優先及來源獨立並行，優先完成台灣好新聞的共用原生
接線與可接受範圍，再依已知 URL 清冊續處理；PeoPo 等待來源期限，不反覆
重啟或改成假成功。CNA 依使用者要求暫緩，870 個未登錄來源鍵仍未完成。

## 最新增量修正驗收：2026-10-14 20:08，Asia/Taipei

已依前序「共用修正、集中設定接線、保留拒絕／原件，再驗證正式產出」方案部署。
**這輪額外釋放 225 個原來卡在舊批次之外的已知 URL，其中 137 個已真正新增正文；
不是只重排、拿 HTTP 200 當完成，或放寬全文／時間／作者檢查。**
[新聞面板](https://penguin72487.dpdns.org/news/) 已用全部已知 URL 顯示進度，
不再以原一次性 cohort 完成率冒充目前來源完成率。全來源目標仍未完成。

### 這輪根因與共同實作

一個有限下載批次是歷史證據，不應永久限制已核對來源的後續本機 metadata。
但移除批次 gate，也不能自動授權全站 discovery。此次在既有 policy／queue／owner
新增明確、具型別檢查的 `known_url_only` 契約：

- 僅接受已核對的私人原文或逐篇 CC 範圍；必須有確切文章 path、完整正文容器與
  作者歸屬。PeoPo 必須由這篇文章自己的 license link 通過，不能拿頁尾或其他文章
  的 CC 標誌替代。
- 本機正常 metadata 的已知 URL 可以持續入列；所有 feed、sitemap、index、
  WordPress、鉅亨 API 與其他 discovery 設定均被拒絕。不猜新聞 ID，也不啟動新的 HTTP owner。
  已存在的 dormant discovery job 保留證據但不再 dispatch；`WHERE 0` 路徑不強制
  使用不相容的 SQLite partial-index hint。
- capture fingerprint 有版本化變更；**正文 parser-repair fingerprint 沒變**。
  排程或審核文字的修改不會把舊拒絕當成 parser 修復重抓，亦不改來源 pacing、
  原始 cohort、有效 lease、attempt、拒絕／重試期限或既存原件。
- 只修改信傳媒、PeoPo 兩個既有 recipe 的增量模式；其餘來源設定及 Anue 的
  API／parser／容量身分保持不變。原四個私人增量來源也使用相同 scheduling ABI，
  不新增平行實作。初始化顯示的 parser repair 遷移數為零。

正式 collector 偵測 catalog 變更後正常 drain，以 exit 75 請求重載，19:57:25 由
同一 systemd 服務重新啟動。新 owner、queue policy、心跳與載入程序時間均對帳；
沒有為了提速強制清空重試、改身分或另開寫入服務。

### 固定 225 個已知 URL 的實際進度

下表是本次固定增量目標，不是全站或去重後的文章總數。
20:08:51 的工程收據保留 queue 20:08:39 的觀測時間；後續更新以面板為準。

| 來源 | 本次釋放 URL | 已新增正文 | 其餘處置 |
| --- | ---: | ---: | --- |
| 信傳媒 | 123 | 122 | 1 個轉載權利範圍排除；本批已無待抓工作。全來源已有 476／478 個 URL 的正文，另 2 個舊／新權利排除，不是 100% 全文 |
| PeoPo | 102 | 15 | 85 待抓、1 真實 403 重試、1 缺本篇 CC 授權；已有 98／262 個 URL 的正文。先前未核實授權及拒絕紀錄保留，不重標成功 |

PeoPo 本 owner 實際送出 17 次文章請求：16 次 HTTP 200，其中 15 篇可接受正文、
1 篇未通過逐篇授權；另 1 次 HTTP 403。**這是來源拒絕，不是新增 CSS 失敗或整個
服務停住。** 原 provider 退避／下次嘗試約為 **20:16:46**，舊拒絕期限約 20:17:52
也保留；由同一 owner 到期自動繼續，其他來源不受其等待阻塞。
條件式 ETA 沿用來源各自的有效入庫吞吐與實際等待，未證明可恢復／未接線範圍
不填保證完工時間。沒有將本次 2 秒 client floor 說成官方或理論上限。

### 整鏈路驗證與剩餘範圍

- 新聞相關完整回歸 **9,870 passed／178 files，274.16 秒**；原 9,783 個案例保留。
  套用後另有 296 項、審核清冊 70 項聚焦測試通過。最初的測試 fixture／診斷訊息
  失敗保留於原 run，並由後續完整成功收據 supersede，沒有刪測試掩蓋失敗。
- 兩來源原 437 個歷史 body heads 的受保護欄位、19 個原 cohort header／receipt SHA
  與一個未到期拒絕完全不變。每來源各抽 8 個新正文與 8 個舊正文，核對原件、
  canonical 身分、正文、作者、發布／修改時間、SHA、Parquet 與耐久收據；不是全湖逐筆驗收。
- 正式 Chromium 四種尺寸通過數字／分母、來源、篩選、歷史清單／正文區別與
  更新中斷恢復。公網唯讀 API 200／no-store，逐來源正文及互斥處置分類均對帳；
  沒有正文、金鑰外送或 page-view publisher call，未宣稱 GPU/WebGL 驗收。
- 前次尚待到期的 Anue `5887163`、`6012730` 已由正式 owner 完成；四個 H1 個案均
  有當下來源短文與作者／時間／原件／收據的獨立讀回，不臆造較長的歷史正文。
- 全清冊仍為 **936 個來源鍵、66 個 recipe、870 個未登錄鍵**；20:08 公網快照為
  6,303,883 個已知 URL／3,822,806 個已存正文 URL，不是獨立媒體數或去重新聞篇數。
  匯流 75、鏈新聞 15、金門日報 3 個既有批次之外目標仍需核對增量範圍，未自動開啟。
- 台灣好新聞使用共用正常 client，robots／首頁／已知原文實測 200，舊 CSS 不存在；
  已有一份帶 SHA／收據的私人原件供離線核對，正文候選、原生 H2 標題已定位。
  作者／發布時鐘、原創／轉載與來源範圍尚未驗完，故仍未啟用，不假稱已實作完成。
  CMoney 的真實拒絕、其他來源未核實的端點／範圍、全部替代路徑與全湖 integrity
  是分開的未完成項目。CNA 依使用者要求暫緩。

目前持續維持 Anue 優先及其他 provider 獨立並行；`all_history_complete=false`。
下一步先核對 PeoPo 到期後的原排程恢復，再依已知 URL 數處理未接線來源，
集中完成來源版型／時間／作者接線，不對每篇另寫 crawler。

證據：[updated_handoff.json](../artifacts/data_quality/news_goal_stage01_20261009/scope_growth_20261014/updated_handoff.json)、
[雙來源新／舊正文讀回](../artifacts/data_quality/news_goal_stage01_20261009/scope_growth_20261014/after_latest.json)、
[完整回歸](../artifacts/data_quality/news_goal_stage01_20261009/scope_growth_20261014/regression2_after.json)、
[正式畫面驗收](../artifacts/data_quality/news_goal_stage01_20261009/scope_growth_20261014/browser/acceptance.json)、
[台灣好新聞原版型診斷](../artifacts/data_quality/news_goal_stage01_20261009/scope_growth_20261014/taiwanhot_layout.json)。
下方 18:27 驗收及更早規劃保持原觀測時間，不與目前增量進度混算。

## 前次實作驗收：2026-10-14 18:27，Asia/Taipei

使用者確認依前序方案實作後，已恢復同一個
`news-adaptive-capacity-20261009` 任務。**共用下載器修正已部署，正常來源持續抓取；
全來源新聞尚未完成，沒有把未接線或無法存取改成成功。**
目前 [新聞追蹤面板](https://penguin72487.dpdns.org/news/) 顯示正文覆蓋、
排程待辦與可展開的原因分類；網頁唯讀，不提供全文、金鑰或控制下載器。

### 這批實際修正

- **鉅亨標題／正文所有權：** 原判斷把正文內的 H1 小標也當作文章標題，造成
  合法正文被拒絕。改成兩次線性結構檢查，保留正文內 H1；重複容器、外來標題、
  巢狀文章、身分、作者、公開／付費範圍與時間檢查仍保留。HTML/parser fingerprint
  已更新，API、授權及來源 pacing 身分不變。
- **原件免重抓回收：** 同一 owner 可在明確有限診斷範圍內重播 SHA 已核對的原始
  回應；每個 parser 版本只檢查一次，限制筆數、位元組與時間，不增加 publisher call。
  新 HTTP 結果、已存在正文、有效 lease 或未到期重試均不可覆蓋。保留原觀測時間，
  不把重播時間當成歷史發布／觀測時間。當前四份原件因已有較新 HTTP head 被 guard
  拒絕舊回應回填，不是原件損毀，也沒有為展示進度而移除 guard。
- **全部來源共用互斥分類：** 已取得狀態、可排程、未接線、範圍／存取待核實、
  新增待納入批次、解析待修、非文字、非新聞、空模板、原站不存在與未知原因分開。
  404 不等於所有可用替代都已失敗；尚未有完整替代路徑證據者不假造「永久不可得」。
- **正文計數與進度：** 沿用交易式 `stored_body_metrics`，一次有界 provider 查詢，
  不在網頁輪詢掃描 630 萬 URL。正文保留於重查／撤文狀態時仍計入「已存」；
  complete 狀態另列。每來源進度用全部已知 URL 作分母，原有限 cohort 另列；
  cohort 完成但仍有新 URL 時顯示等待納入，不顯示全部完成或零秒 ETA。
- **正式畫面發現的額外 decoder 缺陷：** 一個 canonical 清單結果為
  `inconsistent_source_snapshot` 時，舊唯讀 decoder 隱藏整段歷史清單。
  現已涵蓋 producer 的成功、等待、來源矛盾與頁面上限狀態，嚴格數字／未知狀態 gate
  不變。畫面分別顯示 4,071 個已驗證範圍、3 個等待與 1 個來源矛盾；
  清單範圍不是唯一文章，也不是全文完成。此修正只重載網頁，沒有再次重啟下載 owner。

### 實際資料與驗證

| 項目 | 當次讀回結果 |
| --- | --- |
| 清冊 | 936 個來源鍵；66 個 recipe，870 個仍未登錄，不是 936 家獨立媒體 |
| 已知 URL／已存正文 URL | 6,303,538／3,822,002；不是跨來源去重的篇數或全網總量 |
| 發布前快照至驗收的新增存儲 heads | 122；其中鉅亨 +14、Global Voices +105。包含正常抓取進展，不全是本次缺陷修復 |
| 四份真實 H1 原件 | 兩種 parser 均修復 4 份；另外六份原件仍保留未取得原因，不放寬條件 |
| 已實際修復入庫 | `5886694`、`5969783`；來源當下正文為 40／37 字短文，已核對 raw、正文、作者、時間、SHA、收據與 head，不承諾原始版本更長的內容存在 |
| 尚待原到期工作的兩篇 | `5887163`、`6012730`；原重試期限約 19:01:37／19:03:21。由同一 owner 依來源拒絕／限流繼續，不強制提早重排 |
| 既有成功正文控制組 | 16 筆原正文與時間／身分仍可重播；另加兩筆新修復，共 18 筆耐久讀回通過，非全湖驗證 |
| 整合新聞回歸 | 9,783 passed，289.35 秒；原 9,673 語意案例全保留 |
| 額外唯讀顯示修正回歸 | 399 passed，63.12 秒；下載 collector 未更動。原第一筆正式 browser 失敗證據保留 |
| 正式 Chromium | 四種尺寸全部通過：數字／分母、來源身分、篩選、手機版、清單／正文區分與更新中斷恢復；CPU-2D，未宣稱 GPU/WebGL 驗收 |
| 公網讀回 | 新入口 `/news/api/status` HTTP 200、no-store、逐來源數字與分類總數對帳；匿名 Python 預設 client 一次 403 另外保留觀察，以具名自有面板驗證 client 取得成功，不修改 ingress 或新聞網站限制 |

核心實作位於 `downloader/news_fulltext_publishers.py`、`news_fulltext_replay.py`、
`news_fulltext_failure_focus.py`、`news_fulltext_queue.py` 與原 owner loop；分類共用
`stockagent/data/news_source_catalog.py`，網頁沿用 `stockagent/live/news_dashboard.py`
及 `services/news_dashboard/`，沒有平行 collector 或新寫入服務。

驗收主收據：[final_acceptance.json](../artifacts/data_quality/news_goal_stage01_20261009/implementation_20261014/final_acceptance.json)。
全 936 列的分類與下一步：[classified_plan.csv](../artifacts/data_quality/news_goal_stage01_20261009/implementation_20261014/classified_plan.csv)
／[JSON](../artifacts/data_quality/news_goal_stage01_20261009/implementation_20261014/classified_plan.json)。
這份工作表仍是 16:48 規劃快照；即時數量以面板／驗收快照為準，不混用時鐘。

### 還沒完成與既有自動排程

Anue 優先、背景原順序與獨立限流維持；CNA 仍依使用者要求暫緩。
CMoney 的真實 403、其他來源的付費／原創範圍、870 個未登錄來源鍵、
318 個新 URL 的 finite-cohort 成長，以及圖片／嵌入資料仍是分開的工作。
已有通用 adapter 不等於這些來源已驗證接線；不得由 hostname 猜 API、擴張範圍或
另寫一份 client 宣稱修復。正常來源不等待這些缺口，既有到期 feed、正文與重試繼續。
下一批依清冊、已知端點與代表性原件核對來源後，集中設定接線或補真正缺少的協定。
`all_history_complete=false`，全湖串流 integrity 與全部可用途徑的不可得結案尚未完成。

## 前序規劃：先完整規劃，以最少修改批次完成可取得的目標

2026-10-14 **規劃快照：16:48:51，Asia/Taipei。** 當時使用者要求先停下逐項實作，改為全範圍分析，原自動推進目標及 `news-adaptive-capacity-20261009` 任務曾設為 **paused**；既有新聞服務繼續運行，沒有停服務、重排、清除重試期限或更動來源設定。其後使用者已要求依方案實作；最新完成與剩餘範圍以上方驗收為準。以下保留前序方案，下方 v170 是更早的工程驗收紀錄。

### 結論與本輪證據

最少修改不是「每個網址修一次」，也不是「把所有拒絕改成成功」。先把來源、失敗與 HTML 結構分組，重用目前共用下載器；能以設定接線的集中接線，真正缺少的協定才寫 adapter。**目標是一個涵蓋已知可修復事項的整合候選版，測完後一次部署；新增證據若揭露不同機制，才開下一批。** 目前不能證明所有未知網站都能在一個版本解決，也不能承諾全網歷史都可取得。

已使用現有只讀清冊與規劃入口，沒有向新聞網站送出本輪診斷請求，也沒有修改正式 queue：

- [全部 936 個來源鍵的清冊 CSV](../artifacts/data_quality/news_goal_stage01_20261009/planning_20261014/inventory.csv)／[同一 WAL 快照 JSON](../artifacts/data_quality/news_goal_stage01_20261009/planning_20261014/inventory.json)。每個來源都有已知網址、已存正文、狀態、錯誤、metadata 首末日、最後正文取得時間及接線資訊；metadata 日期不是已核實發布時間。
- [全部 936 個來源鍵的逐列規劃 CSV](../artifacts/data_quality/news_goal_stage01_20261009/planning_20261014/plan.csv)／[含輸入 SHA 與工作順序的規劃 JSON](../artifacts/data_quality/news_goal_stage01_20261009/planning_20261014/plan.json)。沿用原規劃 contract v7；啟發式分類不是來源授權、實際下載或新增端點驗收。
- [已登錄端點／網址命名空間清冊](../artifacts/data_quality/news_goal_stage01_20261009/planning_20261014/inventory.endpoints.csv)。網址 pattern 不是捏造的 API，已知 metadata 端點不等於提供全文。
- 原 owner `db7db51cd82441b8bdb6c812fd10e5a6` 與清冊的 status owner 一致；本輪查詢服務為 active、PID 2916518、NRestarts 2。這只能證明當時程序與清冊歸屬，不能證明全部歷史完整。

| 清點項目 | 本輪結果與解讀 |
| --- | --- |
| 來源鍵 | 936；不是 936 家獨立媒體 |
| 已登錄來源 recipe | 66；其中 24 開啟正文、另 LTN 只有 14 個 metadata RSS。recipe 存在不代表 adapter 已實測成功 |
| 未登錄來源鍵 | 870；680 有 queue URL，190 只有本機解析出的原始連結 metadata。不能因未登錄就直接判為無法抓取 |
| 已知網址 | 6,303,175；不是跨網址去重的新聞篇數 |
| 已存正文網址 | 3,821,374；其中 21 筆目前不是 complete 狀態，舊正文與重新檢查結果必須分開 |
| complete 狀態網址 | 3,821,353；只是狀態 head 計數，不是本輪全湖正文驗證 |
| Google News 轉址 | 1,285,020 個已知網址；先回到已核實原文，不能另抓一套相同新聞或當成 128.5 萬篇新缺口 |
| 來源角色 | 910 個媒體／其他待核實鍵、11 個政府鍵、9 個聚合鍵、2 個轉址鍵、4 個社群／影音鍵；角色不是權利或可取得性判決 |
| 已知網址等待範圍核實 | `awaiting_authorization` 2,135,560；不是已證明來源拒絕的篇數，也不是可直接全數放棄的篇數 |

清冊共耗時 86.76 秒／CPU 53.65 秒；這種全量掃描只用於規劃／里程碑，不應放進網頁輪詢。平常進度沿用現有交易式計數、索引與增量事件。

### 第一性原理：先固定目標，再修失敗機制

一篇目標新聞必須經過「已知來源 → 可取用端點 → 正確文章身分 → 完整正文 → 耐久寫入 → 讀回驗證」。HTTP 200、RSS 標題、空正文或重排成功不能替代任一步。優先最大化**新增且可驗證的正文**，再減少修改／部署次數與重複請求；不能靠省略網站、驗證或解除安全 gate 達成數字。

先固定本次歷史目標快照，追新另列；新發現 URL 保留增量清單，避免「所有新聞」因每天新增而沒有結案邊界。同站別名以已核實的 provider／原生 ID／canonical 身分合併，無原生 ID 時保留已核實網址。相同文字不自動等於相同新聞；翻譯、轉載、改版與 source receipt 保留各自 provenance。聚合／轉址解析工作與原文下載不重複計入篇數。

每篇採下列**互斥的處置分類**；分類是待實作方案，不覆寫目前狀態或原收據：

| 分類 | 動作與結案條件 |
| --- | --- |
| 已取得並驗證 | 保留 raw／正文／身分／作者與時間證據、source contract、SHA 與寫入收據；後續追新按原排程 |
| 已有接線、錯誤可修 | 按失敗階段與結構指紋分群；一個共用修正覆蓋同類，不逐篇加 URL 特例 |
| 尚未接線 | 先驗證現有 RSS 全文、WordPress JSON、公開正文 API、JSON-LD／HTML、官方檔案等已知入口；能設定化就不另建下載服務 |
| 暫時阻塞／待核實 | 403、429、timeout、robots／登入／付費及未核對範圍分開；保留實際拒絕和期限，健康來源繼續。一次 403、空白或 parser 失敗不能宣稱文章永久不存在 |
| 已有證據的非正文 | 測試帳號、行情 stub、影像／外嵌限定正文等獨立列出，不能以標題或摘要冒充全文；需要 OCR／影音者是不同 scope，不等於整篇不可取得 |
| 本次可用途徑已證明無法取得 | 原站明確刪除／不存在，且已知可取用的官方替代、同 ID 正式原文／實際原創來源或合法檔案也無法取得；逐篇記錄放棄原因與證據，不刪舊收據，不冒充完成 |

「放棄」記錄至少含原網址、provider、原生 ID（若有）、失敗階段、官方回應／可用替代嘗試、觀察時間、receipt／SHA、是否可以在新來源證據出現時重開。無合法可用入口時停止自動嘗試，清楚保留缺口；未核實與永久不可得分開。只有整個 provider 的已知可用途徑都具備證據，才採來源級停用；不得由一篇失敗推論全站。個人、非商用、不公開用途已確認，不再反覆詢問；用途與實際端點可取用範圍仍是不同事實。

### 集中修改的實作順序

| 階段 | 集中處理內容 | 責任入口／通過條件 |
| --- | --- | --- |
| 0：範圍與根因盤點 | 本輪 936 列清冊與原順序已留存；下一步按來源／協定／正文結構／錯誤階段整理代表性的既有原件 | 沿用 `audit_news_providers.py`、`plan_news_sources.py` 與既有有界診斷。待核實來源只標記下一步，不猜 API 或文章 ID |
| 1：共用修正＋缺少接線的整合候選 | 標題／正文所有權、來源特定作者／時間、正常原件重播、終止與待修分類；共用協定使用同一 adapter，來源差異用已驗證設定 | 延伸 `news_fulltext_policy.py`、既有 publisher／page adapters、queue／catalog；不是重寫 936 個 downloader。capture／parser 行為改變須配套 fingerprint 與相容性 |
| 2：候選測試後一次發布 | 先對所有已知失敗群做 focused tests、真實原件與既有成功正文回歸，再跑一輪相關新聞回歸、一次載入 | 無原件證據不得放寬接受條件；保留 lease、拒絕期限、配額與原收據。不對每篇新結果重跑全套測試／重啟 |
| 3：持續取用、增量驗收及結案 | 可取用 provider 獨立並行；各批自動驗證、未成功者分群；確證不可取得逐篇記錄並退出重試 | 既有 HTTP owner、batch writer、receipt verifier 與 dashboard。僅真正新協定／新失敗機制再開下一批；下載量低的來源不另開常駐服務 |

這是**降低重複工作的批次設計，不是已證明的全域最少修改次數**。未知版型、未核實來源與未提供的 API 能力，不可能在看到證據前承諾一版全修完；也不應為等所有未知網站而停住可正常抓取的來源。

具體優先清單：

1. **Anue 最高優先。** 清冊已有正文 3,681,338 個網址、216 個 active missing-body 工作；另有來源空模板、行情 stub、測試、來源不存在與 unsupported namespace，不能全部當成下載錯誤。四份真實原件證明共用 header 判斷把正文內 H1 小標一併計數；應建立標題／正文歸屬的結構規則並保留內文，連同 malformed／nested／hidden／foreign heading 反例一次驗證。[原件結構證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v170/body_layout_observations.json)不等於四篇已修復。合法短文、表格、引用與各種圖說變化按共同結構歸類，不再以單一網址追加特例。
2. **原背景順序保持獨立並行。** CMoney → UDN → Global Voices → 中時／工商 → MoneyLink → CMNews → MoneyDJ → LTN；優先來源有實際阻塞時回來處理。CMoney 有既有接線但真實回應為 403；不能再寫一份 client 當成修復。Global Voices 已存 55,066 個正文網址、尚有 55,522 個 active missing-body 工作；CMNews 已存 64,422，來源 paid／不存在／namespace 要另處置。LTN 現有 14 個 RSS 只取得 metadata，不能說正文已接線。
3. **把缺少的來源分成設定接線與新協定。** 870 個未登錄來源鍵全部留在 CSV；不是每個都需要專用程式。目前登錄但仍需進一步核實的例子包括 ETtoday（17,271 URL）、台視（10,841）、永豐原文（7,980）、台灣好新聞（2,656）、太報（470）、數位時代（376）、上報（143）。這是待核實優先清單，不是七家已通過自動抓取授權或已完成全文實測。其餘所有來源按同一清冊處理，CNA 依使用者最新決定仍暫緩，不從總清冊消失。
4. **修有限 cohort 的假完成／假等待。** 原小量一次性下載仍保留 provenance；有新已知網址的 ABMedia、信傳媒、匯流、金門日報、PeoPo 等須把範圍外待處理另列。原 cohort 100% 不代表目前所有已知 URL 已完成。僅在核對後的來源範圍內增量加入，不能以新 cohort 清除預算或把未核實 namespace 自動開啟。

### 吞吐量、驗證與網頁的共同契約

- **請求效率：** 已驗證的全文批次端點優先，只有缺正文的已核實目標才回源 HTML；RSS 標題不是全文，第三方快取也不能不經身分／來源規則就替換。依來源實際共同限額分組，不把不同 provider 都串行，也不將共享來源限額拆開規避。計算新增已驗證正文／call 與正文／秒，而不是只看 HTTP 數。
- **處理效率：** 先重播既有 raw，正文解析與檔案 SHA 盡量單次計算、重用同批結果；按 structure／error 指紋去重調查，不反覆對同一已知失敗做全量掃描。索引式 dispatch、增量 counters、原 batch 寫入與有界併行已存在，應先延伸，不另建平行框架。
- **恢復：** timeout／5xx／429 依既有期限與來源政策恢復；確認的 parser 缺陷集中列待修，不拖住其他 provider。owner 重啟後接續 lease／原 checkpoint，不能重排抹掉來源拒絕或把未知是否已送出的請求當成未花配額。
- **每批資料驗證：** 身分／canonical、正文不是摘要／導覽／別篇、原始值與結構、作者／時間來源、raw／body／Parquet SHA、row count、manifest 與 receipt、queue head 讀回。發布時間不可因下載時間而重建成不存在的精確歷史。來源本身缺資料明列，不能補造。
- **正式結案：** 對固定目標集合，每列都要是「已驗證正文」或「有證據的本次不可取得／非正文處置」，尚未接線、待核實或使用者暫緩仍是缺口。全湖收據完整性需另外做有界串流驗收；最近批次成功不能替代全量歷史驗證。
- **網頁：** 原頁面共用模型顯示已驗證正文、可抓剩餘、暫時阻塞、未接線／待核實、已記錄不可取得／非正文；正文覆蓋與處置結案分開。所有缺口可展開看原因，不能把放棄項目混入正文完成數或隱藏分母。ETA 按可排程 provider 的實測有效吞吐、原配額與拒絕期限估算；獨立並行的總尾端由最慢階段決定，存取／未知來源顯示原因與「無法估算」，不能把全域平均速率除進所有網址製造完工日期。

**本輪交付範圍：只讀清點＋完整批次方案＋原任務暫停交接。未寫新 downloader、未部署上述方案、未驗證 382 萬筆正文的全湖完整性，也未增加任何 publisher 請求。** 原 v170 的測試與收據保留，沒有因規劃重新跑長回歸。需繼續實作時，從已固定清冊、代表性原件和第一個整合候選接續。

## 前輪 v170 工程驗收（歷史快照，原證據保留）

2026-10-14 **目前摘要（v170；16:37:40 正式快照，Asia/Taipei）**：沿用個人研究、非商用、不公開用途，不再重問。鉅亨最高優先，原八家背景順序與 CNA 暫緩不變。**兩個共用取證阻塞已修正並實際生效：正文／標題版型不再是私人原件診斷的前提，舊 metadata-only 項目可在同一有限預算內追加一次正常回應原件。172 檔／9,673 項新聞回歸通過，四份真正升級收據、最近 8 批／62 個正文版本及網頁 936 個來源鍵均已驗證。這不是四篇全文修復；全新聞目標仍 active／progress。**

### 本輪已完成

- **先證明公開文章身分，再診斷渲染。** 共用私人取證新增 v3 證明：既有允許範圍內、正常收到的同網址 HTTPS 主站回應，通過挑戰／付費 gate，並核對唯一 canonical、同 ID current 宣告、整數 payment=0、current URL／非空標題／發布 epoch、唯一 JSON-LD 與一致 aware clock。它不依賴正文 selector 或可見 H1，也不接受隱藏／hydration 內容作為正文。正文 parser、作者、時鐘、公開／訓練匯出與 66 個 source capture／authorization／API／parser 身分不變。
- **修正永久停止取證，但不重設已花用預算。** 原 `metadata_saved` 即使之後能證明公開身分，也會永久被 duplicate guard 擋住。現在只有明確啟用的原 23-URL cohort 可在下一次正常回應身分證明成立時消耗第二次寫入；同 URL 全域最多兩次、原 46 次／256 MiB 上限不改。v8 收據绑定第一份 metadata 收據 SHA／路徑，舊 outcome、Parquet、收據與累積 bytes 不覆寫。身分不成立不花第二次；換 cohort 不准升級／重置，已存 raw／未知部分寫入／兩次用盡均抑制。原一般每日診斷預算不變，沒有額外 HTTP、提前 source due 或清除拒絕／限速。
- **測試與原件。** 337 項聚焦測試、完整 172 檔／9,673 項通過（378.04 秒）；原 171 檔／9,601 個 case 精確保留，新增 72 個公開身分、付費／衝突、顯示與接受分界、原件追加、預算及中斷恢復反例。原 29 份實際回應在兩種 parser 下的正文結果不改，同一批 16 篇既有正文的 raw、作者、發布時鐘與收據重播通過。最初一個測試命令引用不存在的檔名而 exit4；原 log 保留，更正後以較晚成功收據處置，沒有改成來源成功。
- **受測後自動載入。** 16:30:20 原 watcher 請求 reload，16:30:21 正常以 exit75 退場，16:30:31 systemd 啟動新 owner `db7db51cd82441b8bdb6c812fd10e5a6`／PID2916518；16:32:55 已有 running 心跳與實際新取證寫入。本輪沒有人工 stop／restart、停 timer、改 gateway 或另開 publisher client。原 70 份有限原件及 12,690,452 B 記帳完全保留，原未到期 due／拒絕期限不變。設定 reload 的重啟計數現為2；初始化仍花數分鐘，未宣稱啟動效能已優化。
- **真實收據，不把取證當完成。** 16:34:45 讀回 `6012730`、`5886694`、`5969783`、`5887163` 四個正常回應的 v8／v3 原件與第一份 metadata SHA 綁定。16:37:40 狀態累計29次／4,628,097 B，outcome 為17 metadata與12 raw；這是取證嘗試／預留回應 bytes，**不是29篇新聞或4.63 MB正文**。最近8個實際batch／62個正文版本收據通過，936個來源鍵全部state／complete網址計數及鉅亨index／manifest進度與唯讀gateway对帳；本輪沒有UI或新外網驗收，頁面不抓publisher、不傳raw／原文或憑證。

### 剩餘正文問題與下一步

四份新原件仍在兩種 parser 下得到 `body_not_found`，沒有接受為文章。結構檢查已找到第一層原因：每份都有唯一正文容器及唯一且符合 current 標題的**正文外 H1**，另1至3個H1屬於正文內；共用 `_cnyes_visible_article_scope` 卻計算整個 ARTICLE 的所有 H1，誤將正文標題視為多個文章標頭而提早拒絕。部分來源 markup 還有巢狀 H1；兩種 parser 的結構並非每處相同。**尚未修正正文接受契約，不能宣稱只排除正文 H1 就能完整取得四篇。** 下一步須先以原件建立標頭／正文所有權、可見全文與 current／schema 一致性反例，再修共用解析及相應 ABI；作者／時鐘、未知或隱藏文字與媒體仍須驗證。較早五份原件的可見及 schema 正文皆空，也不補造成新聞或直接宣稱來源永久缺文。

| 來源 | complete狀態URL head | 16:37:40狀態／邊界 |
| --- | ---: | --- |
| Anue鉅亨 | 3,681,320 | pending151、retry43、cooldown22；原30個未知正文網址仍待修，不是已驗證的唯一缺文章數 |
| Global Voices | 55,012 | pending55,575、inflight2，獨立並行 |
| CMNews | 64,402 | 付費616、来源不存在84等保留，不算全文完整 |
| 民視／Genet | 1,037／3,471 | 原署名、身分、scope、非正文與來源不存在狀態保留，不是全站歷史完成 |

以上是同一快照的完整狀態**網址head**，不是跨網址去重故事數。CMoney 的實際403仍未解除；UDN、中時／工商、MoneyLink、MoneyDJ等原全文scope未核實，LTN沿用14個RSS metadata端點。用途聲明已記錄，不會反覆詢問，也不將其當作解除來源拒絕、批次授權或合理使用裁決。全來源完成時間與未公布的可持續速率上限仍未知；本輪依工作偏好技能延伸共用管線、重要狀態放前面並保留接受分界，沒有另造下載器。

本輪證據：

- [原正常失敗與有限預算](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v170/before.json)、[原29份回應／16篇正文／受測世代](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v170/candidate.json)、[完整9,673回歸](../artifacts/operations/agent-workflow/runs/v170-all172-original9601-and-met-20261014T082215-e5f8ae74/run.json)。
- [正式啟用意圖](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v170/activation_intent.json)、[實際自動載入／66身分／原期限與原件驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v170/loaded.json)、[四個正常回應追加原件](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v170/normal_diagnostics_2026-10-14T083445051318_0000.json)、[真正H1／正文結構觀察](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v170/body_layout_observations.json)。
- [最近8批／62個正文版本](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v170/recent_actual.json)、[936來源網頁對帳](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v170/gateway_all_source_projection.json)、[16:37:40正式狀態與剩餘工作](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v170/completion_current.json)。原件留在私人root，不附全文、帳號或金鑰。

## 前輪 v169（原證據保留，不代表v170現況）

2026-10-14 **本輪工程摘要（v169；15:51:44 正式快照，Asia/Taipei）**：用途沿用已確認的個人研究、非商用、不公開，不再重問；鉅亨最高、八家背景順序與 CNA 暫緩不變。**共用私人診斷的已知正文版型前置條件已拆開，23 個去重後的既有未知網址正在正常排程取證；171 檔／9,601 項新聞回歸通過，936 個來源鍵的實際網頁對帳通過。剩餘 30 個未知正文網址未補齊，四筆新回應仍只取得診斷資訊，不能宣稱全文問題已解決。目標維持 active／progress。**

### 已完成的工程與驗收

- **循環診斷的第一層修正。** 舊 `_public_cnyes_scope` 先要求已知 `#article-container`／ARTICLE 版型，才驗證免費 current 宣告、正式網址、標題及來源發布時鐘；未知正文版型因此無法留下可檢查原件。共用診斷新增獨立的 v2 身分證明，限正常收到的同網址 HTTPS 主站回應，重用挑戰／付費 gate，再要求唯一 DOM 標題、同 ID／payment=0／current URL、唯一 JSON-LD／aware clock 等一致。正文 parser、接受條件、author、source clock、訓練／公開邊界均不改；不從 hydration、隱藏 comment 或猜測 selector 補正文。未能證明者仍只保存固定原因與 metadata。
- **不重複取證，不以換 cohort 重設預算。** 原 70 個 finite-focus 項目全部 `raw_saved`，其 reservation／12,690,452 B 預算記帳及每份 raw／receipt SHA 精確保留。原未知清冊有 30 個 URL，其中 25 個主站 URL 尚未進過 focus；再排除一般診斷已有原件的 `1257870`／`165936`，只把 23 個既有、未保留 raw 的網址放入新 scope。沒有猜 ID、新端點、額外 HTTP、清除原 source due／refusal 或重新取證已記帳網址；每URL最多兩次寫入嘗試，僅已證實寫入失敗才允許一次後續正常回應重試，metadata_saved／未知寫入不得重置。
- **真正原件及原測試保留。** 307 項針對性測試、完整 171 檔／9,601 項通過（456.93 秒）。原 170 個測試檔／9,513 個 case 精確不改，新增 88 個 private identity／付費／隱藏標題／衝突／錯時鐘／重複預算反例。28 份真實舊回應在 `lxml`／`html.parser` 下與已接受 v168 結果完全一致；同一批 16 篇既有正文的 raw、本文、作者、clock、receipt 和目前 parser 重播通過。三個 helper／新 fixture 失敗 log 保留並逐筆以較晚成功 run 處置，未改成來源成功。
- **受測後才啟用。** 完整回歸成功後才套用精確受測的 23-URL 設定。既有 watcher 於15:40:11輸出 `configuration_reload_requested`，15:40:12以預定 exit75退場，15:40:22由systemd自動重啟；本輪沒有人工stop／restart／停timer或開第二個HTTP owner。新owner `93b5ec90b5224a62b0ad9dcb4e131635`／PID2806330已出現新鮮心跳，66個capture／authorization／API／parser身份不變，原未到期重試及拒絕期限保留。至首次觀察新心跳仍約三分多鐘，未宣稱啟動效能已改善；正常的這次設定重啟計數為1，不冒充零重啟。
- **真入庫與網頁連動。** 15:44:30的最近8個實際batch／59個正文版本收據通過；同一新owner的936個來源鍵、全部state／complete URL計數及鉅亨index／manifest進度與既有唯讀gateway對帳通過。不是全湖重驗、新外網／響應式UI驗收或所有歷史完成；本輪沒有改UI／重啟gateway。私人診斷不是文章、不公開原文、不傳raw或憑證。依工作偏好技能沿用共用管線及原件／接受分界，不另建爬蟲。

### 實際剩餘問題，不能略過

四個新scope回應 `5889350`、`6014067`、`6014899`、`5890546` 已於15:43–15:48的**原正常排程**保存耐久 metadata／receipt。四個 v2 的第一失敗都是 `diagnostic_heading_identity_not_verified`，raw均未保留；四次收到的回應大小共631,482 B仍占原儲存預算，**不是631,482 B正文已入庫**。此證據只證明目前標題身分條件未通過，不能區分缺少／多重／隱藏標題、新版型、未渲染頁面或來源缺文，更不能宣稱原站資料錯誤或不存在。下一步應先在既有允許scope內獨立核對免費current／schema身分及有界結構資訊，再決定可用的私人原件取證證明；不得為展示進度清除這四個已花用 reservation，或把放寬診斷條件用於接受正文。

| 來源 | complete狀態URL head | 15:51:44狀態／邊界 |
| --- | ---: | --- |
| Anue鉅亨 | 3,681,297 | pending151、retry31、cooldown34；216個active missing-body URL仍分為186主文候選及30未知，不是已驗證的唯一缺新聞數 |
| Global Voices | 54,791 | pending55,795、inflight3，獨立並行 |
| CMNews | 64,398 | 付費613、來源不存在84等仍獨立列出，不算完整全文 |
| 民視／Genet | 1,037／3,471 | 當前已知清冊無ready backlog，舊署名／scope／身分及非正文狀態保留，不是全站歷史完成 |

以上是同一正式快照的complete **網址head**，不是跨網址去重故事數；重查中的既存正文可暫時不計入complete。CMoney原實際403仍未解決；UDN、中時／工商、MoneyLink、MoneyDJ等全文scope仍未核實，LTN原RSS metadata持續處理但不算全文。用途已沿用，沒有把用途聲明冒充解除實際來源拒絕，也不再重問。全來源全文完成ETA及官方未公布的可持續速率上限仍未知。

本輪證據：

- [原owner／30個未知URL與70個舊原件](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v169/before.json)、[精確原件重播／16篇正文／23-URL去重與受測世代](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v169/candidate.json)、[完整9,601回歸](../artifacts/operations/agent-workflow/runs/v169-all171-original9513-plus88--20261014T072920-645adb40/run.json)。
- [正式啟用前意圖](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v169/activation_intent.json)、[自動重載／66身份／原期限及70原件驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v169/loaded.json)、[四筆新正常回應的實際診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v169/normal_diagnostics_2026-10-14T075005827695_0000.json)。
- [936來源網頁對帳](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v169/gateway_all_source_projection.json)、[8個實際batch／59版本](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v169/recent_actual.json)、[15:51:44正式現況與失敗收據處置](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v169/completion_current.json)。原件仍在私人root，本文不附全文、帳號或金鑰。

## 前輪 v168（原證據保留，不代表v169現況）

2026-10-14 **本輪工程摘要（v168；14:56:52 正式快照，Asia/Taipei）**：沿用已確認的個人研究、非商用、不公開用途；鉅亨最高優先，八家背景順序與 CNA 暫緩不變。**三筆鉅亨測試帳號內容已由正式下載器按原期限正確分類，不再當成可恢復的正文缺口；170 檔／9,513 項新聞回歸全部通過。目標仍 active／progress，沒有宣稱全網站、全歷史或理論速率上限已完成。**

### 已實作、驗證與正式生效

- **根因與修正。** `160783`、`1903822`、`251164` 的原站全文是三組精確測試帳號字串，舊流程一律落入 `body_not_found`，因而每小時重試。共用 parser 新增 `source_article_test_only`，必須同時核對唯一 JSON-LD、正式網址、free/current/visible 的完整內容與發布時鐘；短文或標題提到測試本身不構成排除證據。原站多段 SECTION／空 P 分隔與既有完整廣告 skeleton 使用精確結構驗證，不刪未知文字、媒體或區塊，不把測試字串接受成新聞正文。
- **真正原件與回歸。** 全部28份保留的原始回應在 `lxml`／`html.parser` 下重播；只有三筆改分類，其餘原失敗維持。四個來源共16篇既有正文的 raw／本文／作者／發布時間／收據與目前 parser 重播通過。1,053項聚焦、170檔／9,513項完整新聞測試及另外130項既有公共gateway整合測試通過；JavaScript語法與diff空白檢查通過。原9,414個case保留，168個舊檔精確不變；僅一個舊spacing測試的歷史指紋mock補三行，以移除新增且獨立的測試內容ABI，原斷言未刪除或放寬。首次完整回歸的9,512過／1失敗及其餘六個planner／fixture／真raw讀回失敗原log保留並逐筆處置，不改寫成成功。
- **指紋、容量與恢復。** 只有鉅亨HTML capture／parser-repair身份改版；65個其他來源capture、API、HTTP capacity、authorization與設定不變。14:48:36僅請求重載原新聞service，原程序正常停止、無強制kill，timer未停、gateway與其他下載器／交易服務未由本輪重啟。新owner `5d3a192014984a6bbda044811a0cd4f4`／PID2684589有新鮮心跳，66個capture／parser身份與正式受測code對上；原未到期重試與來源拒絕期限保留。初始化／第一輪等待超過三分鐘才觀察到新心跳，沒有把systemd active冒充已恢復或宣稱啟動速度已改善。
- **實際三筆結果。** 正式程序於14:51:31（251164）、14:51:54（160783）、14:56:14（1903822）寫入新非正文狀態，body仍為空；第三筆原14:56期限未為展示結果提前清除。鉅亨active missing-body URL從219降為216：186仍有同ID正式主文候選、30仍無主文候選。三筆是非新聞排除，不是三篇新增正文；候選與未知URL都不是經驗證的唯一缺新聞數。
- **網頁與入庫。** 前後936個來源鍵的所有狀態／complete計數與來源快照對帳；新狀態顯示「來源測試帳號內容，非新聞正文」，列為阻塞／非正文而非完成。頁面不抓publisher，也不傳原文、raw、帳密。最新8個實際入庫batch／58個正文版本通過收據驗證；這是有限最近寫入驗收，不是重驗全湖或重新做外網／UI尺寸驗收。依工作偏好技能沿用共用pipeline與原件／時鐘證據，不另造下載器。

### 現況與仍未完成

| 來源 | complete狀態URL head | 其他現況 |
| --- | ---: | --- |
| Anue鉅亨 | 3,681,246 | pending151、inflight5、retry59、cooldown19；另三筆test-only。complete會隨已存正文重查而波動，不等於資料刪除 |
| Global Voices | 54,517 | pending56,069、inflight3，獨立並行 |
| 民視／Genet | 1,037／3,471 | 當前已知清冊無ready backlog；民視署名80、身分18、scope排除4,892等仍獨立列出，非全站歷史已完成 |
| 理財周刊／NOWnews | 566／722 | 原權限、付費、署名／身分及來源不存在狀態保留 |

以上是14:56:52同一正式快照的**完整狀態網址head**，不是跨網址去重文章數。鉅亨尚有真實HTML版型／身分／transport及AMP-origin拒絕問題；本輪未以推測selector、隱藏comment或作者姓名同音字補正文。CMoney仍保留實際403；CMNews已有64,380個complete head；LTN仍更新原14個RSS metadata端點而不是全文。UDN、中時／工商、MoneyLink、MoneyDJ等原全文範圍問題不能因用途聲明冒充解除，亦不再詢問同一用途。全來源完成時間與官方未公布的可持續上限仍未知。

本輪證據：

- [原始失敗與28份保留回應](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v168/retained_failure_inventory.json)、[精確原件重播與受測世代](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v168/candidate_v2_validated.json)、[完整9,513回歸](../artifacts/operations/agent-workflow/runs/v168-all170-original9414-plus99--20261014T064142-af99d638/run.json)。
- [16篇既有正文重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v168/retained_bodies.json)、[唯一原服務重載意圖](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v168/reload_intent.json)、[正式66身份與期限／日誌驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v168/loaded.json)。
- [936來源網頁前後對帳](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v168/gateway_all_source_projection.json)、[三筆正式結果後的網頁對帳](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v168/gateway_final_source_projection.json)、[最近8批原件與58版本](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v168/recent_actual.json)、[七個失敗收據處置](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v168/failure_resolutions.json)、[14:56:52正式進度](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v168/completion_current.json)。

## 前輪 v167（原證據保留，不代表v168現況）

2026-10-14 **本輪工程摘要（v167；13:59:04 正式快照，Asia/Taipei）**：沿用個人研究、非商用、不公開用途；鉅亨最高、原八家背景獨立順序、CNA 暫緩不變。**民視／Genet 的本機清冊阻塞已修正且有實際新正文；完整新聞回歸 169 檔、9,414 項通過，936 個來源鍵的網頁計數與正式快照對帳通過。目標仍 active／progress，未宣稱全歷史或所有網站已完成。**

| 來源 | 當前 complete 狀態 URL head | 本輪改變／現況 |
| --- | ---: | --- |
| Anue 鉅亨 | 3,681,240 | 已核對全部 187 個舊 AMP 待辦：186 有同 ID 正式原文且全部通過 raw／receipt／作者／clock／目前 parser 重播；另 1 的正式網址為來源不存在。AMP 原狀態未冒充成功 |
| Global Voices | 54,199 | 獨立並行，pending56,386、inflight4；不是全歷史已齊 |
| 民視 | 977 | 原本 `waiting_cohort` 的 3,407 個網址納入持續已知網址處理；原來源署名／身分／本文／時鐘／轉載排除不變 |
| Genet | 1,881 | 原本 `waiting_cohort` 的 2,124 個網址納入持續已知網址處理；實際外部署名與日期精度保留，不冒充自有作品或重用授權 |
| 理財周刊／NOWnews | 566／722 | 上輪增量設定持續沿用；各自無效／付費／署名／來源不存在狀態保留 |

數字是同一快照的 **complete 狀態網址 head**，不是跨網址去重故事數；正文已存而正在重查的 head 可暫時不在 complete 計數。鉅亨另有219個 active missing-body URL，186屬同ID主文候選、33尚無主文候選；33也不是經證實的唯一缺新聞數。這兩項分類原本已有面板欄位，本輪核對其實際內容與收據，沒有把網址版本合併、複製正文或偽造 AMP 觀測。

### 修正與驗收

- **根因一：清冊範圍與來源可取得性混在一起。** 舊一次性 frozen cohort 讓後來在本機觀察到的民視／Genet URL 永遠留在 `waiting_cohort`。使用原共用 continuous known-URL 模式，保留 frozen membership／收據作為歷史證據，不再拿舊範圍當本次增量門檻；沒有新增猜號、feed／index／API／sitemap、來源搜尋或容量設定。
- **根因二：歷史 supplement descriptor 被當成禁止增量的設定。** 舊驗證拒絕 continuous 模式保留 `allow_reviewed_supplements`。現在僅這兩個明列私用來源允許保留歷史旗標；新增 supplement／materialization 仍獨立要求 finite 模式，不因保留旗標擴張權限。其他64個完整來源指紋及全部66個 parser-repair 身份不變。
- **真實資料而非派工即完成。** 13:54的獨立回補清冊中，原等待網址已有民視85、Genet311個新完整head；各抽8篇，共16篇新正文驗證 raw／body／作者／來源clock／新capture指紋／收據。另16篇舊原文在新scope下精確重播；原2,224個完整head、全部舊實體batch／收據與frozen membership均未因scope改變重抓或回寫。新網址中轉載、身分不符及來源不存在仍分列，5,531個納入網址不是5,531篇已取得正文。
- **恢復證據。** 正式設定的既有watcher於13:46:20留下 `configuration_reload_requested`／exit75，systemd十秒後自動載入owner `2a2a4ba305164f729ffe957772484505`。本輪沒有人工stop／restart，也沒有另開HTTP owner。完整9,414回歸是在這次自動reload後才完成；最終讀回證實同一程序載入確切受測code與全部66個capture指紋，不能把它描述成「完整回歸通過後才reload」。
- **保留限制與誠實證明。** 未到期重試／refusal cooldown、原資料與作者／clock保留。兩份歷史supplement共5個URL的原receipt／membership／raw hashes由canonical validator逐一重驗；額外的pre-reload table快照因自動reload未完成，因此不宣稱supplement table的前後逐欄相等。七個planner／reader／新fixture驗證失敗原log保留、各以成功收據註記處置，沒有改成來源抓取成功。
- **網頁／用途。** 最新8個真實入庫batch與既有唯讀gateway的936個來源鍵、每個state／complete計數、鉅亨index／manifest進度均通過同快照對帳。頁面不下載publisher資料，也不輸出原文／raw；本輪沒有改UI、重啟gateway或宣稱新外網驗收。依工作偏好技能延伸共用管線、重要資訊放前面、保留可核查證據，不另造下載器。[民視隱私政策](https://www.ftvnews.com.tw/privacy)與[Genet官方聯絡頁](https://www.genetinfo.com/contact.html)本輪重新核對，但不把隱私／頁尾或個人用途推論成整站批次授權、合理使用裁決或訓練匯出許可。

**仍未完成：** 鉅亨實際原文／HTML解析與ReadTimeout工作仍由原排程處理；AMP主機的502／robots-origin冷卻不繞過。其餘來源依原背景順序持續處理，明列的403、robots、付費、條款或未知權限不因用途聲明被清除。全來源歷史完成時點與各來源真正速率上限仍未知；不能用批次API的新head速率保證所有舊HTML網址的完成時間。

本輪驗收入口：

- [全部186個正式原文的私有收據驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v167/counterpart_bodies.json)、[兩來源原清冊與計畫](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v167/scope_plan.json)、[完整9,414回歸run](../artifacts/operations/agent-workflow/runs/v167-all169-production9414-no-ov-20261014T054502-6ee743e7/run.json)。
- [實際自動reload與受測身份](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v167/restored.json)、[原head／cohort保留](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v167/loaded.json)、[歷史supplement獨立重驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v167/historical_supplements_revalidated.json)。
- [16篇真正新回補正文](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v167/new_bodies.json)、[最新8個batch](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v167/recent_actual.json)、[936來源網頁對帳](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v167/gateway_all_source_projection.json)、[失敗收據處置](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v167/failure_resolutions.json)、[13:59正式現況](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v167/completion_current.json)。

## 前輪 v166（原證據保留；當時的 finite／count 敘述不代表 v167 現況）

2026-10-14 **前輪工程摘要（v166；13:21:12 獨立讀回，Asia/Taipei）**：個人研究、非商用、不公開用途沿用，不再重問。鉅亨最高優先，八家背景順序與 CNA 暫緩不變。**修正兩個來源的本機一次性清冊阻塞與共用重排根因，已正式載入；目標仍 active／progress，沒有把所有用途宣稱為來源批次授權或把所有網站算成完成。**

### 本輪實際結果

| 來源 | 已保存正文 URL head | 本輪原等待網址中已取得正文 | 現況與邊界 |
| --- | ---: | ---: | --- |
| Anue 鉅亨 | 3,681,194 | 不屬本次兩來源 scope 變更 | pending151、inflight5、retry83、cooldown17；仍在抓，不是全史已齊 |
| Global Voices | 54,001 | 不屬本次兩來源 scope 變更 | pending56,586、inflight2；獨立並行 |
| 理財周刊 | 566 | 14／1,295 | 原等待網址已放入持續處理；未知／第三方作者、付費、轉址、空頁仍分開，body_not_found1未算完成 |
| NOWnews | 722 | 194／251 | 原等待網址已放入持續處理；identity3、scope175、4043、unsupported7為全來源當前各類狀態，不是只剩57篇待抓 |

上述數字是同一正式快照的 URL head，不是跨網址去重故事數；1,295／251是原本被本機 `waiting_cohort` 擋住的工作數，不是新授權數、正文數或全網歷史分母。各抽查8篇新納入網址的實際正文，共16篇，原件、作者、來源發布／修訂時鐘、收據及目前解析器重播均通過。原 frozen cohort／membership／收據保留，不回寫成擴大後的清冊。

**來源範圍。** [NOWnews著作權聲明](https://www.nownews.com/copyright)區分個人非營利用途與第三方／其他用途，[使用條款](https://www.nownews.com/terms)另有不當 BOT 操作與干擾限制；[理財周刊官方公告](https://www.moneyweekly.com.tw/ArticleData/Info/%E7%BD%AE%E5%BA%95%E6%96%87%E7%AB%A0/7)亦區分合理私人使用與第三方作品。因此只對這兩個既有 reviewed source 加上明列的私人已知網址 scope，使用原共用 continuous downloader；保留各自原 body／author／canonical／clock 規則，後來在本機 metadata 觀察到的新網址也可增量處理。只有兩個來源的完整 capture／authorization 指紋變更，其他64個保持；沒有新增猜號、feed、index、API、sitemap、訓練匯出或數字速率上限。其他來源的真實403、robots、付費及權限範圍不因用途聲明被繞過。

**共用重排根因。** 原程式把完整 capture 指紋改變一律視為解析器修正；排程模式、備註與審查日期也會改指紋。第一次scope載入因此多重排39個既有理財周刊 `redirect_not_article`，結果仍為轉址，並非39篇新正文。保留原失敗與較晚檢查的證據，不倒改觀察。新增版本化 parser-repair identity，綁定完整的前／後 canonical capture，只排除排程／說明欄位；真正 selectors、path、author、機器可執行的scope／rights、parser版本改動仍觸發原修復。缺少、損壞或 legacy identity 無法證實相同解析器時，保留原升級路徑。XML與mixed-page修復也保存當前 discovery ABI 的不重排證據，後續重啟不會又重抓；真正 discovery ABI 升級仍可修復。這次root修正不改66來源的完整 capture／authorization／endpoint／HTTP capacity 指紋，不改正文收據 ABI，不宣稱已測得下載倍速或官方極限。

**工程驗收。** 168個新聞測試檔、**9,376項通過（261.19秒）**，新增100項；以本輪前半段167檔世代為基準，166個檔不改，另1檔只把已批准的理財周刊排程斷言 `finite` 改為 `continuous`，其餘正文／作者／日期／權限邏輯完全保留。先前完整回歸精確抓到這個舊設定斷言（9,375過／1失敗），並非parser破壞；失敗log保留後重跑完整正式設定，未跳過舊測試。

正式新owner `e08bd87eabdf488daea10c19fa4a6f5d`、PID2456107、NRestarts0；root修正的單次有序載入含啟動 **82.027934秒**。另一次先前scope載入屬本輪前半段，驗收wrapper曾失敗但source已運行；只修唯讀驗收，不重啟同一handle。66個parser-repair身份、兩來源原始batch／收據與frozen清冊、原未到期限制、最新8個實際正文batch通過；既有唯讀gateway的**936個來源鍵、每個狀態與正文計數及index／manifest階段進度**對上同一正式快照。網頁／gateway／其他下載器與交易服務未重啟，正文不送瀏覽器，page-view不呼叫publisher；UI樣式與外網未重做驗收。原owner的19次實際transport失敗均找到較晚正常入庫、receipt及目前parser重播，但這不表示之後不再有新錯誤。

**全清冊與未完成。** 936鍵工作清單已重建，沿用11:48的不可變inventory，沒有把其舊總量冒充13:21最新全湖數字。ETtoday／TTV仍待完整來源規則查證；EBC與既有受限來源分開列出原因。來源鍵不是媒體家數；清單／adapter存在不代表正文已取得。FTV、Genet等其他finite scope沒有自動擴張。依工作偏好技能沿用共用pipeline、獨立並行與可核查原證據，不另造下載器，也不新增無證據的保守延遲或重試配額。

本輪私人驗收入口：

- [兩來源原清冊、scope與指紋](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v166/scope_plan_v3.json)、[先前scope真正載入與39次額外重排披露](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v166/scope_loaded.json)、[936鍵工作清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v166/all_source_plan.json)。
- [root修正計畫與唯一舊斷言更新](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v166/parser_repair/before_v2.json)、[完整9,376回歸收據](../artifacts/operations/agent-workflow/runs/v166-all168-production9376-curre-20261014T051224-765e431c/run.json)、[正式載入](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v166/parser_repair/restored.json)、[66身份與原件保留](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v166/parser_repair/loaded.json)。
- [16篇真正新納入正文](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v166/parser_repair/known_bodies_after.json)、[最新8個batch](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v166/parser_repair/recent_actual.json)、[936來源網頁同快照對帳](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v166/parser_repair/gateway_all_source_projection.json)、[13:21現況](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v166/parser_repair/completion_current.json)。

2026-10-14 **前輪工程摘要（v165；12:14:34 獨立讀回，Asia/Taipei）**：沿用已確認的個人研究、非商用、不公開用途，不再重問；鉅亨最高優先，背景仍依 CMoney→UDN→Global Voices→中時／工商→MoneyLink→CMNews→MoneyDJ→LTN，CNA 暫緩。**已修正共用 HTTP 對已登記 RSS 的格式宣告缺漏、正式載入並取得原排程結果；全新聞目標仍 active／progress，不是全來源已齊或已達理論最快。**

主要結果：

- **當前 936 個來源鍵逐列重建清單與工作順序**；66 個 configured provider、24 個 enabled、870 個 unregistered key 是不同層次，不是936家可抓全文的媒體。11:48 的同一唯讀 WAL 清冊：**6,302,030 個已知 URL／3,811,737 個保存正文 head**，190個來源鍵只有 metadata；881個來源鍵尚未完成來源審查。目錄、轉址、RSS及正文獨立列出，規劃不等於已下載。這次一次性全清點實測84.692秒wall／47.599秒CPU，並非每次網頁刷新成本或下載提速證據；不重複執行全掃來輪詢。
- **166 個新聞測試檔／9,250項完整回歸通過（292.05秒）**，196項聚焦通過，新增21項。原165檔／9,229項保留不改；全部HTTP AST除了單一已登記feed的Accept賦值外相同，66來源capture／authorization／API／index及實際HTTP capacity指紋不變。
- 原12:00固定診斷槽已由正式程序使用：**12:00:29**、鉅亨ID5974083、159,557bytes的正常回應仍 `body_not_found`，私人固定原因為 **`visible_article_layout_not_verified`**。receipt／Parquet同值及SHA核對；未保存無法證實scope的raw，也未接受為正文。這能定位本次判斷失敗的檢查，**不是取得原件來重播、證明正文永久不存在或倒推舊失敗**。原診斷父契約6／預算3、70項有限focus與原收據保留，不新增來源請求或重置儲存預算。
- 一次有序載入原news service／timer；新owner `9e9418a3903c4d43895075f3c1f5d377`、PID2312962、NRestarts0，保存的planned pause含啟動為**72.774999秒**。原未到期工作、拒絕期限及配額保留；其他下載器、交易服務、gateway／UI未由本輪重啟。
- 同一新owner **138.112244秒**的正式窗：鉅亨新增**155個正文URL head／174個耐久版本**，Global Voices獨立新增**11個head／11個版本**。同窗鉅亨仍有4次transport、1次server error；LTN的5次請求屬metadata而不是5篇全文。這是正常落盤量，不是RSS修正使Anue變快、所有錯誤已消失或publisher最大速率的因果證明。
- **215份不可變原版本／收據、215個目前head、最新8個正式正文batch**通過；既有唯讀gateway的**936個來源鍵、每個狀態與正文數、清單／階段進度**對上同一正式快照，原11,003頁清單證據保持。這是資料／backend連動，不是重做四尺寸UI或外網驗收。正文／raw不傳到瀏覽器，page-view不呼叫publisher。

**RSS共用根因與實際結果。** 原default Accept只有HTML／XHTML／`application/xml`／plain text，沒有RSS／Atom MIME；`application/xml`不自動匹配`application/rss+xml`或`application/atom+xml`。只在既有 `feed`／`rss_fulltext`／`metadata_feed`端點，加入已支援的RSS／Atom／text XML宣告；保留原格式、PDF獨立宣告、conditional validators、UA、來源登記、request reservation、robots、拒絕／冷卻、尺寸與正文驗證，不新增端點、identity或立即重試。這是[HTTP Accept協商契約](https://www.rfc-editor.org/rfc/rfc9110.html#name-accept)的客戶端修正，不以406單獨推定來源只有這個原因。

Fed的[官方RSS目錄](https://www.federalreserve.gov/feeds/feeds.htm)已登記在原管線。`testimony.xml`原失敗為406、原due **12:10:19**，正式新版於 **12:10:38.704667**正常收到**304**並成功沿用相容的原XML SHA／ETag：新owner只有這一筆feed request、無新增XML bytes；queue error清除、attempts歸零，下一原週期**13:10:38.704665**。**這證明本次正常條件式確認恢復，不是重新下載／解析XML、新聞正文取得，或證明406的唯一根因。** 另外 `press_all.xml`舊404、`speeches.xml`舊406的原due仍保留，尚未到本輪正常驗證時間；沒有直接重抓來展示成功。

Fed其餘7個正文缺口也沒有被RSS結果冒充解決：4份PDF只證實原文字解析失敗，**未取得PDF原件重驗，不能斷言都是掃描檔**；3項是document landing page，不是文章全文。CMoney仍沿用實際403與原恢復證據，其餘未核實全文範圍保留；已確認的用途不等於實際拒絕已解除，也不再重問同一用途。

**12:14:34的已知正文工作狀態。** 鉅亨complete **3,677,406**、pending **3,913**、inflight19、retry48、retry_cooldown28；Global Voices complete **53,645**、pending **56,940**。其餘source空模板、quote/image／嵌入類型、404和unsupported狀態獨立保留；這不是全網歷史分母或跨來源事件去重數，也沒有逐份重驗全部367萬篇。

依工作偏好技能採共用根因修正、原件／時鐘與量測分界，沒有另建下載器。新PDF fixture兩次錯配來源契約，原policy正確拒收；只修新fixture，原fail/log保留，由196／9250成功驗證處置。全936 planner最初漏 `--inventory` 的operator錯誤亦保留，改正CLI後對同一清冊成功；都不是來源服務失敗。下一步仍優先補Anue已知正文、依正常失敗證據修根因，背景處理可取得替代渠道及原週期結果；全目標不結案。

本輪證據：

- [936來源清冊CSV](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v165/inventory.csv)、[已登記端點CSV](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v165/inventory.endpoints.csv)、[逐列優先序／缺口CSV](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v165/all_source_plan.csv)、[完整清冊及CLI處置](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v165/all_source_plan_verified.json)。
- [原Fed錯誤與due](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v165/fed_current.json)、[原Accept缺漏](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v165/feed_diagnosis.json)、[原12:00正式診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v164/normal_scope_observation.json)、[21項新案例與精確generation](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v165/implemented.json)。
- [9250項完整回歸](../artifacts/operations/agent-workflow/runs/v165-all166-news-feed-negotiatio-20261014T040414-7efb2230/run.json)、[唯一正式載入](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v165/restored.json)、[原due正常304／原SHA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v165/fed_normal_result.json)、[215原件／目前head](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v165/formal215_preserved.json)、[最新8個batch](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v165/recent_actual.json)。
- [同owner真正落盤與錯誤](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v165/same_owner_progress.json)、[936來源網頁對帳](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v165/gateway_all_source_projection.json)、[本輪有限驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v165/completion_current.json)。原件與診斷僅在私人root，本文不附正文、帳號或金鑰。

2026-10-14 **前輪工程摘要（v164；11:41:02 獨立讀回，Asia/Taipei）**：沿用已確認的個人研究、非商用、不公開用途；鉅亨最高優先、原八家背景順序與 CNA 暫緩不變。修正的是既有失敗診斷的資訊盲點；下載服務已載入，**當時新正常失敗的原因收據尚待原定時段產生，現已由v165讀回**。全新聞目標仍 active／progress，沒有宣告全史、所有來源或理論 HTTP 上限已完成。

主要結果：

- **165 個新聞測試檔／9,229 項完整回歸通過（255.77 秒）**。新增 35 項；原 164 檔測試不改，219 項聚焦通過。18 份真實原回應比對新舊 scope 函式，全部布林值與例外輸出相同；原件 SHA／收據分別核對。離線 HTML MIME 是明列的 adapter 提示，不冒充舊收據未保存的 wire headers；不是 HTTP 速度比較。
- 只一次有序載入原 news service／timer；新 owner `083f16e161dd43bdbacfb2ca4f22d86a`、PID2228096、NRestarts0。保存的 planned pause 含啟動為 **76.258289 秒**，不宣稱零停頓。其他下載器、gateway及 UI 未由本輪重啟。66 個 provider 的 capture／authorization／API／index 指紋、來源設定、全域及逐來源 HTTP capacity 指紋與原未到期工作保留。
- 同一新 owner 的 **217.545030 秒**窗：鉅亨新增 **235 個 URL head／243 個耐久版本**，Global Voices 新增 **20 個 head／20 個版本**；CMNews 的 1 個版本是更新，不算新文章。鉅亨仍有 7 次 transport／3 次 server error，不能與其他窗口混成倍速證據。
- 另以原有唯讀工具核對 **9 筆實際傳輸失敗 → 嚴格較晚的正式新正文**：9 筆均驗證原回應、作者、發布時鐘、耐久收據及當前重播。不是只看連線重置或舊正文；也不是全來源所有錯誤都已恢復。這兩個工具沒有 publisher 請求、queue 寫入或限流重置。
- **215 份不可變原版本／收據、215 個核對時 complete 的目前 head、最新 8 個正式正文 batch**通過。既有唯讀網頁的 **936 個來源鍵**、全部狀態／complete URL數及清單進度，對上同一正式快照；這是 backend／資料連動證據，不是新四尺寸或外網驗收。110 個已登記端點能力／pattern 的清冊另有唯讀證據；**110端點、66 provider、936來源鍵不是同一分母**。

**根因與共用修正。** 當正文解析失敗且私人 raw-scope 證明也失敗時，舊 bounded 診斷正確不保存正文，但沒有留下是哪個 invariant 失敗，因而無法區分 identity、canonical、visible layout、payment、來源 clock 或 schema。既有 `news_fulltext_diagnostics` 現在於同一私人收據／Parquet 新增 version1 的 `raw_scope_observation`：只有固定 enum、布林、第一個失敗原因／證明依據，沒有正文、標題、cookie、憑證或例外訊息。保留原函式判斷順序、布林與例外語義；成功 raw-scope 只允許原私人診斷留存，**不算文章接受或授予來源權限**。父診斷契約仍6、儲存預算契約仍3；沒有新增 archive、HTTP owner、來源呼叫或 queue key。預算耗盡、重複 finite-focus 已在原解析前返回，新增 leaf 不造成額外解析。

**尚未取得的新正常證據。** 原 UTC 日 general 2份及 UTC03小時 supplemental 1份已使用；03:29:30 的獨立觀察沒有新 leaf。原6份診斷收據 SHA、完成的70項有限 focus與已保留數量／bytes完全保留，預算不減少、不換 cohort 重用。**下一個原固定小時槽在12:00（Asia/Taipei）；僅當原正常排程再發生合格失敗時才會產生新收據**，不是12:00保證取得文章，也不是鉅亨 API 流量額度更新。此輪未保存的舊回應不能倒推成已知精確原因。下一步先讀新原因，再修真正可重播的剩餘 `body_not_found`。

**11:41:02 的已知 URL工作狀態。** 鉅亨 complete **3,675,460**、pending **5,829**、inflight38、retry43、retry_cooldown27；Global Voices complete **53,467**、pending **57,118**。這不是全網歷史分母、重新驗證全部367萬份或跨網址去重故事數；其餘原來源空白／媒體類型狀態仍獨立保留，不填造正文。

**背景來源與原失敗保留。** CMoney在本輪正常排程實際再次403，正文0；UDN／中時工商／MoneyLink／MoneyDJ／LTN的全文可取得範圍仍未核實。LTN的13次 metadata請求不是13篇全文。用途聲明已沿用，不把它當作實際拒絕已解除，也不重問用途。直接啟動唯讀 transport audit曾因 repository import path失敗；沿用既有 `run_fintech_python -m scripts.audit_news_transport_observations` 入口成功，下載服務未受影響，沒有改來源工具或新增請求。新測試fixture最初誤把原native wrong-ID例外期待成False，只修fixture並補明確None案例；兩件原失敗run／log保留，由較晚成功驗證處理。依工作偏好技能採既有共用根因修正、原件／時鐘與完整證據保留，不另造下載管線。

本輪私人證據：

- [原現況與失敗收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v164/before.json)、[原 scope 診斷盲點](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v164/diagnosis.json)、[18份真原回應同值](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v164/original_scope_parity.json)、[精確實作／35項新案例](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v164/implemented.json)。
- [9229項完整回歸收據](../artifacts/operations/agent-workflow/runs/v164-all165-news-private-scope-d-20261014T032030-be9237f0/run.json)、[唯一正式載入](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v164/restored.json)、[215原件與目前head](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v164/formal215_preserved.json)、[最近8個正式batch](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v164/recent_actual.json)。
- [同owner真實並行進度／錯誤](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v164/same_owner_progress.json)、[936來源鍵網頁對帳](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v164/gateway_all_source_projection.json)、[110已登記端點清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v164/current_endpoint_inventory.json)。
- [原正常失敗事件／owner](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v164/transport_current.json)、[9筆失敗後嚴格較晚的新正文](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v164/transport_later_bodies.json)、[原診斷槽的具體等待證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v164/scope_observation_wait.json)、[11:41:02綜合讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v164/completion_current.json)。原件仍在私人root，本文不附全文、帳號或金鑰。

2026-10-14 **前輪工程摘要（v163；11:00:35 獨立讀回，Asia/Taipei）**：沿用已確認的**個人研究、非商用、不公開**用途，不再重問；鉅亨最高優先、原八家背景順序及 CNA 暫緩不變。這輪沿用共享入口修正可避免的 CPU 重複計算，**沒有新增下載管線、快取過期資料或宣稱所有網站全文已取得**。全新聞目標仍 active／progress。

主要結果：

- **164 個新聞測試檔／9,194 項完整回歸通過（266.07 秒）**；新增 43 項，原 163 檔測試未改。117 項聚焦測試包含原值、原例外、快取容量、日期規則改變、並行讀取、交易回滾、最新缺口分母，以及完整快照逐 bytes 比對。
- 新舊完整進度快照，在同一真實唯讀 WAL 世代及固定時鐘下，**全部序列化 JSON 的 SHA 一致**。載入前比較的 CPU 中位數 **0.431497→0.382280 秒（減少 11.4%）**；載入後另一個實際快照比較為 **0.311872→0.239876 秒（減少 23.1%）**。兩次是不同快照，不合併成固定效益；每次均有 5 組／variant、交替順序，第一個新版樣本包含 cold cache。量測不含正式 metrics 鎖及檔案發布延遲，**不是 HTTP／整站下載提速或理論上限證據**。
- 只一次有序載入原新聞 service／timer；owner `9392a96a4bb84bafa94bf4620514403c`、PID2164038、NRestarts0。原收據初始化仍有成本，保存的 planned pause含啟動為 **102.440217 秒**，不是零停頓。其他下載器、gateway及 UI 未由本輪重啟。
- **66 個 provider 的原 capture／authorization／API／index 指紋、來源設定、HTTP 全域及逐 provider capacity指紋全部保留**；原未到期 jobs／refusals亦保留。只有純函數計算實作改變，不重置配額、限流、診斷預算或重試期限。
- **215 份不可變原版本及收據、215 個核對時 complete 的目前 head、最新 8 個正式 batch**均獨立驗證。本輪沒有沿用前輪「214+1等待追新」當作現況：前輪等待的正文其後已由原正常排程取得；舊原件與新來源版本仍分開。
- 既有唯讀網頁的 **936 個來源鍵**、每個狀態／complete URL數及清單階段分子分母，對上同一正式快照。來源鍵不是936家已驗證provider；正文／raw不傳到瀏覽器，瀏覽頁面不呼叫publisher。UI未改，未重做四尺寸或外網驗收。

**根因與修正。** 剖析發現每次進度更新都重新解析約1,518個相同歷史查詢網址；`urlsplit`原生小快取不足以容納這個工作集。共享 `scope_key`加入 **4,096筆、每網址最多2,048字元**的有界純計算快取，日期偏移規則也納入key。原完整parser body的 AST只把日期偏移改成明列參數，其他驗證／來源修改函數AST全相同；異常型別、超長輸入及原例外仍走原解析，不裁剪或拒收。返回值為不可變tuple／None，不快取例外。**缺口資料列、來源設定、scope分母、head、發布時鐘、抓取結果及due都不快取**，每次仍讀當前DB；新增／刪除缺口、設定改變與回滾立即反映。剖析工具中的deepcopy是離線adapter成本，不冒稱正式服務根因。

**真正並行進度。** 新owner同一 **132.205574秒**的實測窗，鉅亨新增 **48個URL head／54個耐久版本**，Global Voices新增 **10個head／10個版本**。鉅亨仍有 **1次transport／2次server error**；保留原恢復機制，不說全部錯誤消失，也不把新舊不同工作窗拿來宣稱下載倍速。11:00:35讀回：鉅亨complete **3,672,723**、pending8,575、inflight18、retry30、retry_cooldown41；Global Voicescomplete53,249、pending57,335。這是目前已知URL工作，不是全網歷史分母或跨網址去重故事數。

**背景查證與未解邊界。** 等待測試時重查 [CMoney官方GetStockRssNews文件](https://www.cmoney.tw/cm-service/CMService.asmx?op=GetStockRssNews)：文件有date／lastDate／fetchSize及commkey／memberPk，回應列識別碼、時間、標題、連結、來源等，**沒有完整正文欄位**；仍未證明匿名權益、最早歷史、fetchSize或數字頻率上限，未叫用資料操作或猜憑證。CMoney真正403／零正文未被修好；UDN、MoneyDJ等仍分開保留未核實全文範圍，沒有以個人用途聲明授予來源權限。[UDN官方聲明](https://udn.com/page/topic/495)、[MoneyDJ服務條款](https://www.moneydj.com/z/ABMoneydj/aboutTermsService.djhtm)保留查證入口；公開文件可讀、會員權益、全文保存、模型用途與實際取得不是同一證明。本輪不改這些範圍、不重問用途，也不把RSS目錄算全文。其餘可取得provider仍由原排程獨立執行，CNA繼續暫緩。

本輪私人證據：

- [原9151驗收世代／現況及計畫](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v163/before.json)、[根因／原parser](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v163/diagnosis.json)、[載入前完整JSON／CPU比較](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v163/whole_status_parity.json)、[載入後另一真快照比較](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v163/live_status_parity.json)。
- [確切實作／新43案例指紋](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v163/implemented.json)、[9194完整回歸收據](../artifacts/operations/agent-workflow/runs/v163-all164-news-pure-scope-cach-20261014T024845-18f3f0d4/run.json)、[唯一正式載入／原期限保留](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v163/restored.json)。
- [215原件及目前head](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v163/formal215_preserved.json)、[最新8個真batch](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v163/recent_actual.json)、[同owner正式並行進度／錯誤](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v163/same_owner_progress.json)、[936來源網頁對帳](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v163/gateway_all_source_projection.json)、[11:00:35最新綜合讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v163/completion_current.json)。

2026-10-14 **前輪工程摘要（v162；10:28:20 獨立讀回，Asia/Taipei）**：維持個人研究、非商用、不公開用途、**鉅亨最高優先**、原八家背景順序及 CNA 暫緩。修復真實英文長文的正文漏抓，已由正式正常排程取得，不新增來源請求、不拿診斷檔冒充入庫，也不重問同一用途。**全新聞目標仍 active／progress，尚未全史完成。**

本輪主要成果：

- 鉅亨 `4888694` 於 **10:21:50.847999** 正式取得 **2,239字元／365個來源原有單字**；正文 SHA `30e955d8ab529593b6734f56e7547fd1af6473f8a0ff1abf52ee7688dd313f40` 與舊診斷原件的完整可見文字一致。原作者「林羿君」、原發布時間 **2022-06-10T03:05:45.000Z**、實際新回應與耐久 batch皆獨立重播通過。診斷觀察、離線修復與新正式抓取分開保留。
- **163個新聞測試檔／9,151項完整回歸通過（260.54秒）**，新增75項；其餘162檔原測試不改。508項聚焦回歸包含舊短文、正常長文 hot path、錯誤數值／日期／ID／付費／隱藏正文／表格媒體，以及既有429期限與配額狀態保留。
- 只一次有序載入原 news service／timer；正式 owner `02ca9c21f82b4e9e8d6c83cbb031a710`、PID2085279、NRestarts0，計入初始化的暫停 **103.642550秒**。不是零停頓提速；其他服務／唯讀 gateway／UI不重啟。**真實全 provider HTTP配額／拒絕政策指紋、配額 provider鍵均與載入前相同**；只改 Cnyes HTML解析 ABI，不改API、公開清單、授權、設定或原重試期限。
- **215份原版本／收據保留**；核對時214個當前 head complete，1篇 `6624565` 保留原正文但進入正常六小時追新，不能宣稱215篇此刻全已追新。最近8個正式 batch通過。原嚴格驗證的失敗留存，再以既有 stored-versus-freshness驗證對最新已接受基線獨立核對，沒有改正式 queue、日期、正文或舊驗證器。
- 原網頁 **936個來源鍵**、全部狀態／完成 URL計數與同一正式快照一致，清單階段分子／分母亦一致；不輸出 raw／正文，不因瀏覽網頁呼叫 publisher。這是現行本機 gateway資料連動，不是新四尺寸或外網驗收；原v161四尺寸驗收另留在下面。

**根因與共享修正。** 鉅亨原件的英文 JSON-LD `articleBody` 有1,875字元但去掉所有空白，因而被既有「非CJK至少8個單字」判為 `body_not_found`，每次正常重試都拒收。可見正文與當前同ID、payment0的完整來源內容有相同2,239字元／365單字，去空白後逐字一致；不是描述、摘要、其他文章或猜測的斷詞。將既有短文的 canonical／標題／同ID／免費／有時區發布時鐘／整個可見容器／完整當前內容比對，抽為**同一共用證明**；只有本來失敗且具有實際來源單字的長文分支，才返回原本可見文字。保留全部數字、符號與真正空白；沒有降低8單字 guard、刪正文、忽略表格／媒體、取隱藏內容或補造作者／時間。`165936` 原回應的可見容器確實空白，即使 hydration有文字也仍未算完成或全文不存在的永久證明。

**ABI與HTTP責任分開。** 新增 `verified_native_prose_spacing_contract_version=1` 到 Cnyes HTML指紋，bulk／公開清單指紋保持。新增HTTP capacity來源指紋只排除此一新解析ABI，保留每個既有政策／範圍／授權／端點／其他解析版本與設定欄位；其他政策變更仍讓不相容配額狀態失效。避免「修正文解析」順帶清掉來源既有退避、重新試速或重置診斷儲存額度。真實已落盤的全 provider capacity指紋保留，不只是synthetic測試或服務active。

**實測與不能宣稱的事情。** 16份不同已驗原件、ABBA共 **512次完整正常解析**保持全部輸出同值；整條 claim→原回應讀取→解析／失敗→成功→fsync／收據→獨立讀回，在私人臨時根目錄測量，舊版16篇入庫、新版17篇（多修復一篇）通過。完整健康解析每組128篇的原版／新版平均 CPU為 **2.690608／3.137761秒**，不是加速證據；整個18回應的durable流程 CPU為 **0.603356／0.533485秒**，但新版做了更多有效工作，亦不拿它宣稱同工作量提速、全站上限或整個來源理論極限。舊收據沒有原HTTP headers，離線MIME明列為格式提示，不冒充wire證據。主要提升是修掉每次請求0篇有效正文的錯誤拒收，後續不再對該成功head無效重試。

**正式並行進度（同一owner、168.525850秒窗）。** 鉅亨新增 **114個新URL head／130個版本**，Global Voices新增 **16個新head／16個版本**；原版本與新文章分開，不能相加成去重總文章。鉅亨仍有 **3次transport／4次server error**，由原恢復機制處理，不宣稱零錯誤。10:28:20的鉅亨狀態：complete **3,670,732**、pending **10,556**、inflight7、retry64、retry_cooldown18；這是目前已知URL工作，不是全網歷史分母。CMoney仍有真403，UDN／中時工商／MoneyLink／MoneyDJ／LTN的HTML全文範圍仍未驗證，沒有把用途聲明當成來源存取已解除。LTN官方[RSS清冊](https://service.ltn.com.tw/RSS)仍列 `people.xml`，該原端點404保留；不自行猜網址、提前重試或把RSS摘要算全文。

本輪可檢查的私人證據：

- [原始失敗、基線及計畫](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v162/before.json)、[真原件逐字重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v162/actual_original_replay.json)、[整條鏈路／完整解析ABBA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v162/whole_actual_benchmark.json)。
- [確切實作／75項新案例指紋](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v162/implemented.json)、[9,151項完整回歸收據](../artifacts/operations/agent-workflow/runs/v162-all163-news-long-prose-exac-20261014T021518-eac1a42b/run.json)、[唯一正式載入](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v162/restored.json)。
- [正式正常期限後新全文／raw／耐久收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v162/normal_recovery_actual.json)、[215原件與214+1追新邊界](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v162/formal215_preserved.json)、[最近8個真batch](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v162/recent_actual.json)。
- [同owner實際並行進度／錯誤](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v162/same_owner_progress.json)、[原936來源網頁對帳](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v162/gateway_all_source_projection.json)、[10:28:20獨立最終讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v162/completion_current.json)。

2026-10-14 **前輪工程摘要（v160→v161；09:52:55 獨立讀回，Asia/Taipei）**：維持**個人研究、非商用、不公開**用途、**鉅亨最高優先**、CMoney → UDN → Global Voices → 中時／工商 → MoneyLink → CMNews → MoneyDJ → LTN 背景順序及 CNA 暫緩。不重問同一用途，不將實際 403／429／robots、登入或付費限制當成已解除。**全新聞目標仍 active／progress，沒有宣告全史／所有來源下載完成。**

本輪的重要結果：

- 鉅亨原卡住的第26頁與後續第27頁，已由正常排程取得並重播原始 JSON／不可變收據；空標題 `newsId=901293` 的**真正全文**也已獨立驗證：1,245字元、作者、原發布／修改時鐘、原回應與正文 SHA、目前 head及耐久 batch。清單空字串保持原值，没有拿 metadata 補造全文。
- 已封閉的 **232個日期／11,003頁**清單階段完成、觀察326,776筆 metadata；只是這一明確清单範圍，不是全部鉅亨全文、跨媒體去重事件數或全網歷史完成。
- 修復 RSS 原值處理及**新版進度被網頁隱藏**的獨立問題。正式唯讀頁現在顯示13／14個已驗 RSS、520筆最新 feed 觀察、XML流量、1個錯誤與最近／下次核對時間；全文數仍0，未把摘要算成全文。
- **162個新聞測試檔／9,076項完整回歸通過（254.26秒）**，四種尺寸原網頁的實際 RSS數字、篩選重設及可讀性驗證通過。不是全 repo、GPU或外網 HTTPS驗收。

**RSS修復的範圍與證據。** XML1.0不接受部分 ASCII控制字元；字元範圍以 [W3C規範](https://www.w3.org/TR/xml/#charsets)為準。v160先走原嚴格解析；只有失敗時，才允許最多256個原生 UTF-8 literal控制碼，在明確登記的 item `title／description` 文字值中以唯一 token進行嚴格 XML解析後逐字還原。沒有刪字元、改原回應、URL、日期、作者、標籤／屬性或寬鬆 recover模式；欄位外、其他結構、DTD／entity、錯誤編碼及過量控制碼仍拒收。原 raw與還原值分別留私人收據，metadata ABI2顯式相容相同政策的舊 strict-v1原件，舊指紋／收據不重標，304及既有 HTTP限流／拒絕身分保持。

新增87項邊界測試，235項聚焦及161檔／9,045項回歸（239.12秒）通過。13份真實舊 RSS原件的AB／BA共 **1,664次完整解析**保留原欄位、URL及發布時鐘；新版最慢組平均 **1.3154毫秒／feed**，不主張整條 HTTP鏈路或理論上限。正常成功的 hot path不掃控制碼；失敗診斷由建立全部匹配清單改為常數額外記憶體的計數。重播 MIME是明列的離線格式提示，原收據沒有原HTTP headers，不冒充傳輸證據。

**正常重試不是人為提前補抓。** `life.xml` 原9次失敗及 **09:49:32.466597**期限跨載入保留；正式owner於 **09:49:32.910281**成功。`local.xml`在本輪基線時09:14已恢復，之後新版正常追新也驗證成功。兩份本次真實回應都**沒有非法控制字元**，因此本輪能證明正常取得、原值／時鐘／SHA及解析實作通過，**不能宣稱已重播或定位原壞 XML中的控制碼欄位，也不能把來源自行恢復冒稱為 fallback已在該壞原件生效**。`people.xml`原登記網址仍實際404，保留錯誤與期限，不將其稱為整個來源歷史不存在。14個 RSS只含官方已審閱的標題／摘要／連結／pubDate範圍，沒有取得LTN全文權限或正文。

**網頁根因與有限載入。** 實際原頁的 RSS進度是 `null`，原因是 backend只接受契約1，但正式來源快照已是2。v161只改共用 projection的兩行，接受原生整數的已知版本1／2並保留實際版本；所有其他函式AST及原161個測試檔保持。新增31項，203項聚焦通過；未知版本、字串／布林／浮點版本、錯誤數量、正文量、全史完成或假ETA仍拒收。完整9076回歸後，只一次載入原唯讀 gateway（10.904秒）；**原下載owner／PID1982829沒有因此重啟**。936個來源鍵與實際同一heartbeat一致，再逐欄對帳RSS數量、bytes、錯誤、版本及原due時鐘，頁面不呼叫publisher、不匯出正文／raw。

原頁以真實gateway payload固定的同一視圖，於 **1440／768／390／320px**瀏覽器實測，正常展開原折疊清冊、搜尋LTN並清除／回到全部936來源，數字與原值一致、無runtime error或頁面水平溢出；四份RSS元件截圖逐一檢視，原版面／樣式／篩選保留。這是本機原頁＋實際快照的render proof，**不是即時所有數字都凍結不變、四尺寸全部網站重驗或外網連通證明**。沒有另建Data app／網站、下載管線或修改API key。儀表板技能促使本輪補上逐欄與實際顯示對帳，而不是只看来源數；工作偏好技能沿用共享根因、原值／時鐘與收據保留。

**耐久資料、實測速度與剩餘工作。** 215份原不可變版本、215個目前head及最新8個正文batch再次嚴格驗證；13個舊 native RSS原件／收據 SHA載入後保持。09:36:21.701624–09:39:17.889379同owner的 **176.187755秒**窗新增鉅亨 **116個耐久正文版本／104個新URL head**、Global Voices **15個版本／新head**；同窗鉅亨仍有4次transport／5次server error，不把其隱藏或算成正文。本輪只一次新聞service有序載入（planned pause含drain／啟動94.127秒），後續只載入唯讀頁。09:52:55當前鉅亨 **3,668,660 complete URL head**、Global Voices **52,875**、CMNews **64,297**；不是逐份重驗全部366萬份正文，亦不能混加版本與URL數。

CMoney於 **09:45:42.798367**原正常排程仍 `source_access_denied`，保留原恢復／拒絕證據；其他未取得全文渠道及未知歷史也不冒稱下載成功。九件TDD／fixture／operator launch／browser-helper失敗的原run、log與SHA保留，均有同task較晚成功驗證及 `superseded`註記；第一次browser失敗是未先正常展開刻意折疊的清冊，不是刪除折疊功能或強制改DOM。下一步維持鉅亨已知正文回補／追新最高優先，等待時處理CMoney及其餘既有來源、安全公開替代渠道與LTN已登記404；全目標不結案。

證據：[v161當前有限驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v161/completion_current.json)、[9076項精確generation](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v161/implemented.json)、[實際RSS新版原頁空值根因](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v161/before.json)、[唯一gateway載入與逐欄對帳](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v161/restored.json)、[四尺寸真實數字及重設](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v161/browser_actual.json)、[九件原失敗的精確處置](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v161/original_failure_resolutions.json)、[v160原件與實際來源驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v160/completion_current.json)、[原Anue26／27頁重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v160/priority_actual_source_success.json)、[901293原生全文驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v160/priority_actual_body_success.json)、[兩個RSS正常重試](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v160/actual_feed_normal_results.json)、[13原RSS1664次同值](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v160/actual_feed_original_parity.json)、[同owner新增落盤與錯誤](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v160/same_owner_progress.json)、[215原版及目前head](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v160/formal215_preserved.json)、[最新8個正式正文batch](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v160/recent_actual.json)。原件僅在私人root，本文不附正文、帳號或API key。

2026-10-14 **前次工程摘要（v158→v159；06:37:29 最終獨立讀回）**：用途仍為**個人研究、非商用、不公開**；不重問同一用途，保留鉅亨最高優先、八家背景順序及 CNA 暫緩。已實作並正式載入鉅亨清單空標題根因修正；來源實際的 403／429／robots、登入與付費限制不因用途聲明而消失。**全新聞目標仍 active／progress，沒有宣告全史抓齊。**

**鉅亨的真正原因，不是請求太慢。** 原排程於 **06:13:54.569149** 正常重試 2014 年歷史清單第26頁，原件儲存額度雖已用完，v157 的精簡 witness 仍成功記下：第22筆、`newsId=901293`，`title` 是原生空字串；分類846、免費旗標0、原生整數發布 epoch1397116408及精確請求日期範圍都有效。舊程式因此拒收整頁，阻止後續頁。沒有把失敗 JSON 冒稱為已保存原件，亦未人工提前來源請求。

**從粒度修正：可發現 ID，不等於已取得正文。** v159 只移除清單 metadata 的「原生字串必須非空」限制；空字串／空白保持原值，不補造標題。非字串／缺欄位、錯誤 ID、分類、發布時間、免費／付費旗標、未知可能正文欄位與分頁驗證仍拒收；付費 ID 不進正文工作。原正文、作者、原始與修改發布時鐘及 source／HTTP 失敗函式 AST 不變，清單接受的正文量仍是0。合格 ID 由原 article queue 獨立取得與驗證正文，不能以空標題清單補正文。

**獨立 metadata ABI2，不重抓健康歷史。** 新 metadata 指紋明列 optional-native-string-title 契約2；端點登記仍是原已驗契約1，66個 source capture／authorization／bulk 指紋及四份來源設定 SHA 不變。只讀相容指紋由**相同設定、相同授權**的較嚴格 v1 精確推導，綁定目前指紋；不能跨任意 parser 或授權套用。舊原件、指紋、觀察時間與收據不重新標籤。同頁新舊 head 並存時只算一次；新 head 優先，進度與 forecast 仍區分 source total 和去重正文。304 仍須對上其原生舊指紋與同頁原證據，不把舊證據改寫成新版本。舊 global 拒絕／退避也跨這個相容 ABI 保留，精確 request scope 維持原證據，鄰頁成功不能解除原壞頁。

**完整驗證與測量邊界。** 新增 **31項**獨立正反例，三個舊測試檔只更新這個明確空標題語義，保留錯誤型別、日期、免費旗標與分頁拒收；其餘原標準不變。**194項聚焦、160個新聞 test 檔／8,958項完整回歸通過（236.56秒）**，不是全 repo／GPU 驗收。8頁真實已取得清單的 **AB／BA共1,024次完整解析**保留全部原欄位、source clock及付費排除，最慢新版組平均 **0.4555毫秒／頁**；重播 MIME 是明列的離線 JSON 提示，舊收據沒有 HTTP headers。正式完整清單 snapshot 的 **256次完整 progress 呼叫**，所有計數、phase、clock及 forecast 與舊版完全一致；新增版本標記除外。相容 head 去重有額外成本，最慢新版組平均 **80.752毫秒／完整 progress**，不冒稱查詢提速、HTTP 上限或整條鏈路已達理論最快。

**正式載入及真正新落盤。** 只一次 news service／timer 有序載入，planned pause含啟動 **46.918秒**；owner `90e37087c3604a4f82d085a7604581d6`、PID1529290、NRestarts0，實際 process clock、ABI2及完整測試 generation核對。**10,738個舊 strict-v1 head 與10,738個原未來健康 job 全欄位保持**，舊 scope annotation 逐字保留，沒有重抓健康清單或更改原壞頁的 attempt5／due／clock。06:29:11.886340–06:30:28.924826同 owner 的 **77.038486秒**窗新增鉅亨 **72個耐久正文版本／64個新 URL head／9頁已驗歷史清單**；Global Voices 獨立新增 **6個版本／新 head**。該小窗鉅亨 transport／server各0，不等於整天無錯誤。

原 **215份不可變版本、215個目前 head、最新8個正式正文 batch**再次分別嚴格驗證。06:30唯讀 gateway 的 **936個來源鍵**、每個狀態／complete URL數與來源清單、分階段進度，全部對上同一 source snapshot；沒有 UI／gateway 重啟、正文／raw匯出或 page-view publisher HTTP。這是 backend／projection proof，不是四尺寸瀏覽器或外網 HTTPS 驗收。06:37:29當前計數為鉅亨 **3,657,378 complete URL head**、Global Voices **51,731**、CMNews **64,292**；不是逐份重新驗全部365萬份正文或跨來源去重事件數。

**原重試仍保留，實際解卡尚待證明。** 空標題原頁下一次正常來源重試仍 **2026-10-14 07:13:54.569146（Asia/Taipei）**。本輪證明程式／測試／正式載入，不冒稱該頁已以 ABI2 返回、ID901293 已取得正文或後續頁已完成。目前仍由同一 canonical owner 追新／回補，最高優先來源需要介入就切回；不為展示完成而清掉 source due。

**等待時已處理的背景問題（v158）。** LTN 的14個已登記 RSS 只取得官方聲明的 `title／description／link／pubDate` 元資料，不是新聞全文。13個舊成功 head仍保留，另有3個目前錯誤。兩個 `life.xml／local.xml` 經原正常排程實測均 HTTP200、XML類型，但各含 **1個 XML1.0 禁止的 ASCII控制字元**，ET code4、位置分別 **[24,45]／[22,45]**；所在欄位及控制碼值沒有保存，**尚未修 parser、不直接刪字元或放寬解析**。`people.xml` 為官方已登記網址實際404，不能宣稱該來源全史不存在。v158 的 typed witness 最多4 KiB／目前指紋最多14個 latest key，僅 error stage、code／位置、布林、原 SHA／大小與原 due；無標題／摘要／正文或失敗 raw儲存、額外請求／配額、scope grant或發布 clock。

v158 **23項新例、141聚焦、159檔／8,927完整回歸（216.86秒）**；13份真實原RSS **1,664次完整解析**與舊版四欄／raw／發布 clock同值，最慢新版平均 **1.8875毫秒／feed**。v158正式同 owner 的 **338.160562秒**窗曾新增鉅亨 **244版本／224新URL head／33清單頁**，Global Voices **34版本／新head**、CMNews **8版本／1新head**；鉅亨仍有 **6次transport／4次server error**，與v159不同窗、不相加為去重篇數。CMoney 在 **06:30:31.605242** 原正常排程再次得到 `source_access_denied`，原恢復與拒絕證據保留，沒有取得正文；UDN／中時工商／MoneyLink／MoneyDJ／LTN全文渠道與 metadata／個人用途分開處理。

**交接。** v158三件及v159四件原 TDD／fixture／驗證器失敗 run／log 保留，均由同 task 的後續成功驗證加 SHA／`superseded` 註記處理，不把失敗原 run 改成成功。沿工作偏好技能採共用根因、原值／時鐘與收據保留、完整驗證及實測邊界，沒有另開下載管線、修改金鑰／其他下載器／交易／模型。下一步優先核對原鉅亨正常重試與真正新正文；等待時繼續依背景順序找可驗證取得渠道，LTN需定位非法字元後做不丟原值的解析修復，全目標不結案。

證據：[v159最終06:37:29當前驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v159/completion_current.json)、[v158正常來源失敗的真實空標題原因](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v158/priority_actual_failure.json)、[8958項精確generation](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v159/implemented.json)、[10738舊head／job逐字保持](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v159/compatible_live_readback.json)、[256次完整實際進度同值](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v159/actual_progress_parity.json)、[8頁原件1024次同值](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v159/actual_original_parity.json)、[唯一正式載入](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v159/restored.json)、[真新增落盤](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v159/same_owner_progress.json)、[215原件及目前head](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v159/formal215_preserved.json)、[最新8個正文batch](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v159/recent_actual.json)、[936來源進度頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v159/gateway_all_source_projection.json)、[v158正式RSS錯誤細節](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v158/actual_feed_failures.json)、[v158最終驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v158/completion_current.json)。原件僅在私人root，本文不附全文、帳號或API key。

2026-10-09，Asia/Taipei。工程任務 `news-adaptive-capacity-20261009`；保留原任務與既有 dirty work。
本次只修改新聞取得、私人正文保存、唯讀清冊與進度頁，沒有修改金鑰、其他下載器、交易或模型。

2026-10-14 **最新工程摘要（v156 → v157；05:38:17 獨立驗收）**：**鉅亨最高優先**、CMoney → UDN → Global Voices → 中時／工商 → MoneyLink → CMNews → MoneyDJ → LTN 背景處理順序、**個人研究／非商用／不公開**用途及 CNA 暫緩不變。這輪修復的是「來源清單失敗後，診斷儲存額度用完，連具體欄位原因也遺失」；不是宣稱來源壞資料已修好，也不是將用途聲明當成登入、403 或來源限制已解除。原全新聞目標維持 **active／progress，尚未全部完成**。

**根因與兩階段修正。** v156 在原嚴格 `parse_index` 的錯誤分支附上欄位類型、違規原因、原請求範圍及已收到的回應；沿用唯一 queue writer、原 general 診斷儲存預算，私人保存來源原件與 SHA。實際 page=26 於 **05:13:41.925214** 正常重試，仍是 `cnyes_index_unverified_metadata`、attempt4；原 **每日2份／每 UTC 小時1份 general 診斷儲存額度**已用完，因此沒有成功保存原回應，**具體欄位原因仍未知**。這是私人診斷原件的儲存限制，不是鉅亨 API 請求配額，不應借此新增來源呼叫或重置額度。

v157 從源頭把兩件事拆開：最多 **4 KiB** 的欄位類型／原因、回應 SHA、精確 request 與當前指紋，附在**本來就寫入的 failed-request scope witness**；原始回應保存仍受原預算限制。正常失敗會留下可診斷的精簡事實，即使原件預算已用完；超大錯誤數值改記精確值雜湊並明列省略，不杜撰數值。沒有新增 queue key、archive、HTTP 呼叫或授權；不保留未知欄位內容、標題文字或正文。啟動時保留原 annotation／第一次觀察時間；scope guard、metadata／正文接受條件、來源時鐘、403／429／robots、真正 global refusal、source due 和作者／付費排除均不放寬。此刻 **protocol2 已載入，但正式新欄位 witness 尚未觀察到**；不回填過去沒保存的回應。

**測試與同值驗證。** 最終 **16項新增反例、224項聚焦、158個新聞 test 檔／8,904項完整回歸通過（209.47秒）**。原 v156 的30項診斷案例保留，只更新兩個明確 protocol version 斷言；其餘157檔原標準不刪減。全部 **66個來源指紋、4份設定 SHA** 保持；原 validator、期限、scope guard 與 general raw 預算的關鍵函式 AST 同值。**8頁真實已驗清單、AB／BA共1,024次完整解析**保留全部原欄位、發布時間及付費 ID 排除；最慢新版組平均 **0.362毫秒／頁**，只證明離線解析，不宣稱來源速度極限或整條下載提速。舊收據沒有 HTTP headers，重播只使用明列的 JSON adapter 提示。

**唯一正式服務與並行干擾分開驗收。** v157 只一次有序載入 news service／timer，owner `5fa48326d1a04994b3d0baa9dea1ae9f`、PID1385237、NRestarts0。原載入 run 的「gateway 程序不變」檢查確實失敗：journal 顯示 **05:34:39** 唯讀 gateway 另一次正常停止／啟動，PID 由1237926變1386506；本 run 的命令只有 news service／timer，**沒有再次啟動下載器或重啟 gateway**。原失敗收據／log 保留；另獨立讀回**同一 new news PID、fresh process clock、所有載入指紋與原未到期拒絕**，再逐一驗新 gateway 的全部936來源。不能宣稱 gateway 沒變，也沒有把未通過的載入 run 改成成功。

**真正落盤進度。** v156 正式 owner 的 **305.510737秒**窗新增鉅亨 **153個新 URL head／172個版本／22頁已驗歷史清單**、Global Voices **28個新 head／29個版本**；鉅亨仍有 **6次 transport、5次 server error**，沒有冒稱無錯誤。v157 正式同 owner 的 **30.712398秒**窗再次新增鉅亨 **5個新 head／6個版本／1頁清單**、Global Voices **4個新 head／版本**，鉅亨有2次 transport error、0次 server error。兩窗不合併為去重文章總量，不以重新驗收版本冒算新文章；CMoney等沒有新正文的來源仍如實列出，不以背景服務活著冒稱成功。

**原件、目前 head、網頁各自證明。** 本輪嚴格重新驗證 **215份不可變原始版本＋215個目前 head＋最新8個正式正文 batch**，沿用原正文檢查器；先前兩筆例行追新已由原 owner 於04:41正常完成，沒有改 claim／recheck／attempts。新唯讀 gateway 的 **936來源全部 key、狀態、complete URL head、清單與分階段進度**和同一最新下載快照精確一致，沒有傳正文、原件、金鑰或 page-view publisher 呼叫；這是 backend／projection 驗證，**不是全瀏覽器 UI 驗收**。05:38:17 queue 計數為鉅亨 **3,654,591 complete URL head**、Global Voices **51,387 complete URL head**；不是宣稱本輪逐份重新驗了全部365萬份正文，亦非完整歷史已完成。

**保留的下一步。** 原壞頁全部 job 欄位、due、attempt4、兩份 scope annotation 逐字保留，原 raw 儲存預算不清空／不減少；下一次正常來源重試 **2026-10-14 06:13:41.925211（Asia/Taipei）**。屆時才取得這頁具體欄位原因並據實修正；本輪只是讓診斷不再因 raw 額度耗盡而失明。健康清單／正文與可取得背景來源繼續，403、登入與來源明列限制不靠提高頻率繞過，CNA 不納入。

證據：[05:38:17獨立當前驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v157/completion_current.json)、[8904項精確 generation](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v157/implemented.json)、[原正常重試與已耗盡預算](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v156/failure_budget_blocked.json)、[8頁真原件同值解析](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v157/metadata_original_parity.json)、[同一正式啟動的獨立讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v157/restored.json)、[215原件與目前head](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v157/formal215_preserved.json)、[最新8個正文batch](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v157/recent_actual.json)、[v156真落盤窗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v156/same_owner_progress.json)、[v157真落盤窗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v157/same_owner_progress.json)、[936來源網頁快照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v157/gateway_all_source_projection.json)。原件僅在私人 root，本文不附全文、帳號或 API key。沿用工作偏好的共用根因修復、原件／時鐘保留與實測邊界；沒有另開下載管線。

2026-10-14 **前次工程摘要（v155；04:47:33 獨立驗收）**：**鉅亨最高優先**、八家背景順序、個人研究／非商用／不公開用途和CNA暫緩保持。修正第二個清單停頓根因：**後來的短暫傳輸錯誤，把前一頁request-local metadata的一小時期限錯當成全端點期限**。沿用唯一queue writer與HTTP owner，沒有提高速率、清除拒絕或提前重查壞頁；原全新聞目標仍 **active／progress**，並未全部完成。

**真正的期限因果。** 原page=26已於 **04:13:37.030398**由正常排程重查，仍是 `cnyes_index_unverified_metadata`，第三次失敗、下次期限 **05:13:37.030396**；具體不符欄位仍缺原回應診斷。另一日期page=18於 **04:14:20.832251**發生 `transport_error`，自身真正重試期限為 **04:15:20.832246**。原 `max(previous_not_before, new_not_before)` 保留了前一metadata的一小時，但又覆寫成新transport的scope／error，令全部清單等待至05:13。這不是來源給了新的「一小時全端點限流」，也不能任意將所有cooldown縮短。

**修復與自我恢復。** request-scope證據現在必須與該頁自己的due相符（只容許原兩次取時的≤1秒差），不能替借來的global floor背書；既有endpoint證據之外，按當前指紋與精確請求保存每個壞頁的衍生lineage，未變值不重寫。metadata失敗發生於真正global cooldown時，不再覆寫原global錯誤／URL／期限。對舊錯誤，只修**transport + 精確仍有效metadata donor**：核對來源／URL／typed scope／job／aware clock及時間因果，完整納入其餘同端點未到期原因；超過128原因、壞／未知證據或scope不符均不降低期限。原circuit、被排除的request原因與保留的global原因先在同一交易保存不可變journal，再更正這個錯誤聯集；所有discovery job的原due、attempts、錯誤及來源時鐘不改。403／429／robots／server／未知錯誤本身不適用這個舊transport修復，HTTP owner的來源限流／Retry-After也保持。

**正式部署與實際source證據。** 最終 **33項新增正反例、127項聚焦**及 **156個新聞test檔／8,858項完整回歸通過（198.56秒）**。全部66個source capture／authorization／API／RSS／discovery指紋與四份設定SHA不變，metadata／正文解析、作者、發布時鐘和接受原件的函式AST不變。只一次news service／timer有序載入，planned pause含drain／startup **19.499秒**；owner `f74ac180c2c6401e86e2f9c457c2bcef`、PID1222467、NRestarts0。正式新owner的修復計數為 **1**，journal精確保留原借錯期限，修正後端點期限 **04:15:21.832246**（含保留的1秒取時差）；原壞頁仍維持 **05:13:37**，沒有operator queue改寫或新增publisher HTTP。

真正正式全discovery索引的一致唯讀WAL＋該connection的TEMP原因聯集，AB／BA **128次完整選取**確認舊版擋住 **16個真正到期頁**、修正後可選，原未到期壞頁不被選；正式main資料與全部job欄位不動。TEMP原因修復 **121.581毫秒**，新版最慢一組平均 **0.281毫秒／完整選取**；不含正式WAL fsync，也不是來源／整機速度極限或CPU百分比提升。早期比較原件保留，最後pin採最終版本，不混用不同code測量。

新owner的 **108.235436秒**metric窗內，鉅亨新增 **142個耐久版本／126個新URL head**、**21頁已驗歷史清單**；Global Voices新增 **9個版本／新head**。鉅亨本窗仍有 **3次server error、0次transport error**，真正的來源失敗與恢復照常；清單、版本、URL head不相加為去重文章數。16個原到期候選中已有 **1頁**由新owner取得完整source JSON／Parquet／receipt及逐筆metadata重播證據，其後續頁也前進；不是16頁全部完成的宣告。清單raw收據未保存HTTP headers，離線重播只用JSON adapter提示，不冒稱原始headers或正文。

**原件與追新分開驗收，沒有放寬原檢查器。** 嚴格「215個目前head都complete」的原檢查器實際拒收；獨立診斷找到 `6628829`、`6628845` 兩筆因原 **6小時、最近3天**的例行追新被排為retry／due=0／attempt=0／error=NULL。不是傳輸失敗或正文遺失，也不代表最新重查完成。全部 **215份不可變原件**的raw／body SHA、作者與發布／captured clock再次逐件核對；**213個目前complete head**通過原canonical parser／batch重播，另兩筆保留的目前正文、指標與值精確匹配v154已驗版本，並由canonical selected-head batch驗證。最新8個committed正文batch也通過。證據明列 `current_head_freshness_complete=false`；沒有把retry改complete、改due或以舊正文冒稱最新。本輪三個失敗run均保留，原基線／TDD有成功替代；嚴格freshness拒收有明確 `expected-rejection`，不是吞掉錯誤。

**網頁／當前數字與未完成邊界。** 04:34:35的唯讀gateway **936個來源鍵**、每個projected state／complete URL數、manifest分母分子與精確snapshot clock對齊，原935鍵全保留；沒有UI／gateway變更、重啟、正文匯出或page-view來源HTTP，不主張四尺寸browser或外網HTTPS驗收。04:47:33再次確認同process／owner、完整測試generation及fresh（快照04:47:20.990863、age12.146秒）：鉅亨 **3,652,329 complete URL head**、Global Voices **51,099**；不是跨來源去重事件數。232個overflow日期清單階段 **10,095／11,003頁（91.75%）**、**299,767筆metadata**，剩 **908頁**；metadata來源總數與去重全文數不同。近期300秒的21頁仍有兩個零進度子窗，條件中情境為 **10/14 08:23:32**，慢情境尚不可估；這只是此清單階段，不是全新聞／全文／全史完工ETA。

沿工作偏好技能採共享根因、原件／來源時鐘保真及完整實測；沒有改金鑰、其他下載器、交易或模型。下一步補**壞頁具體欄位的有界私人診斷**（本輪尚未實作），核對兩筆例行追新的實際選取／回應，並繼續鉅亨最高優先的清單／正文與背景來源；CMoney403、其他來源全文渠道及全部歷史仍有缺口，全目標不結案。

證據：[最終獨立當前驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v155/completion_current.json)、[實際混合期限基線](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v155/before.json)、[完整正式索引最終AB／BA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v155/actual_selector_final.json)、[156檔8858項最終generation](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v155/implemented.json)、[唯一正式owner](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v155/restored.json)、[原期限與真正修復journal](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v155/journal_actual.json)、[實際原到期清單頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v155/scope_actual.json)、[215原件與213完整／2待追新](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v155/formal215_preserved.json)、[原freshness拒收診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v155/preservation_diagnosis.json)、[精確拒收處置](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v155/strict_freshness_expected_rejection_final.json)、[同owner真新增正文／清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v155/same_owner_progress.json)、[936來源進度頁核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v155/gateway_all_source_projection.json)。原件僅在私人root，本文不附全文、帳號或API key。

2026-10-14 **前次工程摘要（v154；04:05:09 獨立驗收）**：保留**個人研究、非商用、不公開**用途、**鉅亨最高優先**、CMoney → UDN → Global Voices → 中時／工商 → MoneyLink → CMNews → MoneyDJ → LTN背景順序，以及CNA暫緩。本輪修正鉅亨公開歷史清單的**單頁資料驗證錯誤阻擋整個端點**；沿用唯一下載owner與既有共用queue，沒有提高來源限速、解除拒絕或修改正文／metadata驗證規則。**原全新聞目標仍 active／progress，全史尚未完成。**

**根因與隔離邊界。** 原page=26請求因 `cnyes_index_unverified_metadata` 已失敗兩次，原正常重查期限為 **04:13:30**；端點級circuit令另外 **17頁真正到期**的清單也無法派工。原失敗raw沒有留下，所以**究竟哪個metadata欄位不符仍未證明**，不能宣稱已修好該頁。現在只為符合當前來源／metadata指紋、精確URL／錯誤／attempt／已檢查時鐘與原期限的失敗，建立既有 `queue_settings` 中的衍生request-scope證據；沒有另建writer或重寫原失敗紀錄。衍生註記與原失敗同交易commit／rollback；重啟核對後可重建，布林／整數及壞JSON均嚴格區分，不符合即保留原端點隔離。403、429、robots、transport／server、未知欄位與無效分頁等**不縮小拒絕範圍**，不是繞過來源存取限制。

**成功回收也一起修正。** 原程式任一鄰頁成功就刪除整個端點circuit，可能丟失壞頁的原隔離證據；現在精確request隔離**只有該頁自身真正通過原驗證才解除**。鄰頁成功仍完整保留原壞頁、錯誤及期限。04:01:04正式讀回證實原circuit與job均未被鄰頁成功清除，未提前重查壞頁或把metadata當正文。

**驗證與完整選取實測。** 新增 **32項**獨立正反例，**181項聚焦通過**；最終 **155個新聞test檔／8,825項回歸通過（229.86秒）**，不是全repo或GPU驗收。66個來源capture／authorization／API／RSS／discovery指紋及四份來源設定SHA保持；metadata與正文validators AST不變，接受頁的AST只有上述circuit回收尾段有意改動。真正正式全discovery索引的一個一致唯讀WAL snapshot，AB／BA共 **128次完整選取**：舊版選不到頁，新版精確選出原17頁，原壞頁／期限保持。新版最慢一組平均 **0.579毫秒／選取**；這是調度正確性與成本，不是HTTP上限或整機CPU提升。比較只在該唯讀connection建立TEMP衍生註記，沒有operator publisher HTTP、正式lease或queue改寫。

**新版確實落盤，而非只有active。** 僅一次原news service／timer有序載入，planned pause含drain／啟動 **55.444秒**；owner `69d3113277454d32b809c9bcb95b54d0`、PID1122814、NRestarts0，當前process clock、完整測試generation及scope契約1均核對。03:58:20.326654–04:00:52.366261同owner的 **152.039607秒**metric窗：鉅亨新增 **109個耐久正文版本／109個新URL head**、**15頁已驗歷史清單**；Global Voices獨立新增 **16個版本／新head**。鉅亨同窗仍有 **2次transport與2次server error**，原恢復繼續；CMoney1次請求、LTN5次資訊請求沒有取得正文，不冒算全文成功。這些量是不同粒度，不能相加為去重新聞篇數。

原17頁中**一個原到期鄰頁**已由正式owner成功取得，完整source JSON／SHA、精確請求、private Parquet／receipt及30筆metadata逐筆重播通過，其後續頁也正常前進；**不是17頁全部下載的宣告**。重播使用最小JSON MIME adapter提示，原收據沒有保留HTTP headers，沒有把提示冒稱原始headers。原215份不可變版本、215個目前head及最新8個committed正文batch分別重驗，作者、原文、發布／captured clock保持。

**進度頁與當前數字。** 04:04:37唯讀gateway的 **936個來源鍵**、每個projected state及complete URL數與owner snapshot逐項一致，原935鍵全保留；manifest頁／筆數、planned／verified／remaining分母分子及精確snapshot clock也核對，`manifest_phase_progress`與`public_index_progress`分開，不以清單冒算正文。沒有UI／gateway變更或重啟、正文／raw匯出、page-view來源HTTP；本輪不主張四尺寸browser或外網HTTPS驗收。04:05:09再次獨立讀回，當前快照04:04:58.781653、age10.186秒、同process／owner且fresh；鉅亨 **3,650,492 complete URL head**，Global Voices **50,880**，不是跨來源去重事件數。

當時鉅亨已封閉的232個overflow日期清單階段為 **9,952／11,003頁（90.45%）**、**295,481筆已觀察metadata**、剩 **1,051頁**；來源宣告總metadata為326,776，不能保證去重全文數。若近期300秒實測的27頁速率維持、剩餘頁可取，中情境預估 **10/14 07:19:36**完成**這個清單階段**；仍是條件情境，不是統計信賴區間、全部鉅亨正文或全新聞／全史完工時間。原壞頁、其他歷史發現與正文缺口仍須繼續驗證，CMoney403及其他來源的全文渠道問題分開追蹤，不因個人用途聲明冒稱來源已開放全部下載。

**交接。** 六件TDD／驗證器失敗的原run及log完整保留，均有同task後續成功run、精確證據SHA與 `superseded` 處置；未部署的早期pin也保留、不冒算正式generation。沿工作偏好技能採共享根因修復、保留原件／時鐘、完整驗證和實測邊界，沒有另開重複管線、修改金鑰、其他下載器、交易或模型。下一步仍由原最高優先owner依原期限重查壞頁並持續清單／正文，背景來源獨立處理；全目標不結案。

證據：[最終獨立當前驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v154/completion_current.json)、[原端點阻塞與17個到期頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v154/before.json)、[正式全索引AB／BA選取](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v154/actual_selector_final.json)、[最終8825項generation](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v154/implemented_final.json)、[唯一正式owner載入](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v154/restored.json)、[實際鄰頁與原失敗隔離保留](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v154/scope_actual.json)、[215原件與目前head](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v154/formal215_preserved.json)、[最新8個正文batch](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v154/recent_actual.json)、[同owner真新增落盤](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v154/same_owner_progress.json)、[全部936來源與進度頁精確比對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v154/gateway_all_source_projection.json)。原件僅在私人root，本文不附全文、帳號或API key。

前次獨立讀回（v153）：2026-10-14 **03:26:26**，仍是同一已驗收 owner，精確 process clock／generation／freshness 通過；鉅亨 **3,648,640 complete URL head**、Global Voices **50,663 complete URL head**，均比03:24交接時增加。這次讀回沒有 publisher HTTP 或 queue 寫入；[獨立當前收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v153/completion_current.json) 明列 `all_history_complete=false`，原全新聞目標維持 active。

2026-10-14 **前次工程摘要（v153；03:24:10 十項有限驗收接受）**：保留**個人研究、非商用、不公開**用途，**鉅亨最高優先**、八家背景順序及CNA暫緩不變。本輪修正共用新聞清冊匯入阻塞派工的根因；不是改來源速度、用標題補正文或把 `active` 當下載成功。已完成實作、完整同值測量、新聞回歸、唯一服務載入與實際新正文／網頁核對；**全新聞目標仍 active／progress**。

**根因與恢復設計。** 原 owner 匯入 FinLab 一份 **442,407 筆**的已驗收新聞 Parquet 時，以整份 SQLite 長交易佔住主執行緒。實際整次清冊更新最長 **217.990 秒**，下載派工停頓、心跳過期；程序沒有死亡，後來自行完成並恢復。因此沒有將過期觀察當停機重啟，也沒有放寬45秒 freshness。現在沿用同一 queue writer 與 HTTP owner，依原 **4,096 筆 logical batch** 正規化／去重；每個短交易最多合併4個原批次，另有 **0.75秒 cooperative 寫入窗口**。每批之間檢查停止，原URL／首筆重複標題／跨批更新／日期與股號關聯不變；組內資料、invalid數與 durable cursor 一起 commit／rollback。只在全部讀完後更新 `receipt_heads`，未完整及 superseded 的版本不冒算完整 metadata。停機後重新驗完整 source SHA，按原批次邊界續跑；匯入未完時，原dispatcher每秒獲得一次下一量子機會，不等待下一個180秒維護輪次。

**完整測量與拒收候選。** 所有比較均使用真實 **442,407 筆、442,039 個目前 warm URL head**及原不可變 source receipt，私有 SQLite/WAL/`synchronous=FULL`副本，沒有額外 publisher HTTP 或正式queue改寫。最初單4k逐批提交候選雖然把派工停頓降到約1秒，但平衡測量的整份 wall／CPU反而增加，**未部署、原失敗run/log保留**。另實測2MiB／8MiB／32MiB，選較小8MiB；只於 active import 借用同一connection的快取，最後reader完成／失敗／關閉即還原原設定，保留使用者已明定的較大快取，沒有永久放大 RAM。再以實際合併提交程式完整執行 **AB／BA四次**，全六張資料表的全部輸出SHA一致：

| 完整真實清冊；兩次／版本 | 原實作 | 合併短交易＋暫借8MiB |
| --- | ---: | ---: |
| 整份匯入 wall 中位數 | 53.661秒 | **45.109秒** |
| 整份 CPU 中位數 | 49.409秒 | **41.784秒** |
| 最長連續佔住 owner；四次中各版本最大值 | 64.377秒 | **1.607秒** |
| 六表、所有head／title／date／association／完整receipt | 完整442,407筆 | **完整同值** |

這是完整本機匯入元件的約16% wall／15% CPU改善；不含正式3.6百萬head的全部索引大小或每秒實際HTTP派工間隔，只有兩組平衡樣本，不外推成整機CPU／來源極限。正式217.990秒與私本53.661秒是不同資料庫工作量，**不能聲稱正式大檔已從218秒變成1.6秒**。原32個真實metadata維護head／8個原批次的另一完整量子也同值：checkpoint **32→8次commit**，wall中位0.2377→0.0727秒；只是該量子，與整份或前幾輪百分比不相加。正式metadata replay的0.25秒預算及64-head上限保持。

**部署後真正落盤。** 同一新owner的03:21:39.171008–03:23:11.666901 metric窗（**92.495893秒**），鉅亨新增 **106個耐久版本／95個新URL head**，Global Voices獨立新增 **10個版本／新head**，CMNews有6個既有head更新；LTN有8次資訊請求，不冒算全文。鉅亨本窗0次transport、**1次server error**，原隔離及恢復照常。03:24:10讀回，新owner實際使用 **416筆／141次 retained metadata checkpoint**，不是只看到檔案已修改；原215個不可變版本、215個目前head及最新8個committed body batch重新驗證通過。

| 來源／優先序 | 03:24:10讀回；URL head不是去重事件數 |
| --- | --- |
| 鉅亨（最高） | **3,648,509 complete**；18,108 pending、22 inflight、65 retry、34 retry_cooldown。12個image-only、346個quote-only、4,617個空模板、1,269個not-found分開；inflight含尚待耐久commit，不等於HTTP併發數。 |
| CMoney | 272,811 pending、461 retry；原403與拒絕期限保留，本觀察窗0新請求／正文，不把舊403當本窗重新測量。 |
| UDN | 169,753 awaiting_authorization；候選保留，精確全文取得渠道仍未驗證。 |
| Global Voices | **50,649 complete**、59,937 pending、2 inflight；本窗10個真新head，原robots floor保持。 |
| 中時／工商 | 108,155 awaiting_authorization；背景渠道／候選與取得全文分開，不由用途聲明冒算完成。 |
| MoneyLink | 103,214 awaiting_authorization；金融資料與編輯全文範圍分開。 |
| CMNews | **64,288 complete**、609 paywall_or_login；本窗6次既有文章更新，原追新保持。 |
| MoneyDJ | 48,108 awaiting_authorization；原URL／證據保持，不用標題冒充全文。 |
| LTN | 31,127正文候選 awaiting_authorization；資訊追新持續，RSS與全文／全史分開。 |

前v152目標`2026844`已於 **02:54:17**由原正常排程向來源重查，正式分類為 `source_body_image_only`、due=0且沒有body，**不再錯誤重試，也不是取得文字全文**。`1257870`仍為真正未知正文／`body_not_found`，沒有將單獨減號補成文章或提前改狀態。

**驗收邊界與後續。** 新增31個獨立正反例，**158項聚焦、154個新聞test檔／8,793項最終回歸通過（198.97秒）**；原失敗、拒收效能候選及修正驗證都保留，不冒稱全repo／GPU測試。僅一次原news service／timer有序載入，**planned pause含啟動58.145秒**；owner `770469eeb437495ea02d35a9b67459a4`、PID1025286、NRestarts0，實際process clock、完整測試generation、metadata contract2及全部66個loaded capture核對。全部capture／authorization／API／RSS／discovery fp、四份來源設定、未到期拒絕、保留job及future重試期限不變；其他下載器、交易、金鑰、網頁服務不重啟。

唯讀gateway當前 **936個來源鍵**（原935全部保留，已知來源動態增加；不是936個獨立API owner）、每個既有projected state、complete數、精確snapshot clock及fresh／consistent一致；無正文／raw匯出或page-view來源HTTP。本輪無UI、四尺寸browser或外網HTTPS變更驗收。正式大清冊沒有為展示加速而被重新匯入；下一份真正改版receipt依原刷新與新durable cursor接續，不能用私本測量冒稱正式新大檔已完成。沿工作偏好技能採共享根因、保留原件與時鐘、完整同值及拒收候選實測；**全史發現、剩餘正文與來源取得渠道仍有未完成工作，沒有封閉全史ETA或來源最高速承諾**。

證據：[十項有限交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v153/handoff.json)、[原清冊與32個真head](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v153/before.json)、[原217秒匯入及同owner恢復](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v153/ingest_before.json)、[32-head完整量子同值](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v153/whole_quantum_benchmark.json)、[未部署的拒收候選](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v153/cache_single_commit_rejected.json)、[真442407筆完整AB／BA與cache還原](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v153/whole_receipt_scoped_cache_final.json)、[154檔最终generation](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v153/implemented_group.json)、[唯一正式owner](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v153/restored.json)、[215原件與目前head](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v153/formal215_preserved.json)、[同owner真新增落盤](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v153/same_owner_progress.json)、[唯讀進度頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v153/gateway_actual.json)。原件僅在私人root，本文不附全文、帳號或API key。

2026-10-14 **前次工程摘要（v152；02:08:21 十項有限驗收接受）**：**鉅亨仍最高優先**，八家背景順序、個人研究非商用不公開用途及CNA暫緩保持。這輪修正真實同ID圖片文章被誤列為`body_not_found`而反覆重試的根因；**圖片型是可驗證的非文字結果，不是下載到全文**。只有鉅亨非正文capture契約3→4，其他65個capture、全部authorization、API／RSS、discovery指紋及四份設定SHA不變，沒有新建下載管線、放寬全文範圍、抓圖片或OCR。

**實際原始回應、推導與反證。** 原耐久診斷中的`2026844`，當前完整宣告與整個可見正文圖片森林精確相符，資產來自同ID的`cimg.cnyes.cool/prod/news/2026844/l/...jpg`，來源原欄位為600×358及比率`1.675977653631285`；原分類器卻限定`/o/`及固定600×546／比率，因而漏判。現在只接受既有明確host、同ID、`/[ol]/`和32位hex JPG；來源字串尺寸必須是正整數、各≤32768，比率必須精確等於原尺寸除法字串。整個可見森林及完整宣告仍精確比對，沿用node／深度／尺寸預算，拒絕文字、未知attrs、hidden、混合媒體、query／fragment、身分／free／作者／時鐘不符。原attrs和raw不改寫、不把圖片ALT補成正文。

實際9份保留回應逐件重播：`2026844`的1份`body_not_found`→`source_body_image_only`且不重試；`1257870`的8份只有單獨減號，**仍為`body_not_found`**，沒有擅改為空模板或完整正文。這是同一份過去raw的新舊分類，不冒稱此次已重新向來源取得目標。原215份HTML／JSON／RSS以各自真正adapter完整結果重播，正文、作者及發布／修改時鐘精確相同。最初純HTML重播的1份RSS和1份JSON在新舊兩邊都不成立，原紀錄保留，後以正確adapter確認**215份全部可接受**；這兩件是驗證器表示法問題，不是來源授權／正文錯誤。離線比較僅凍結原captured clock作parity，沒有改動實際歷史時鐘或覆寫資料。

**真落盤與部署後驗證。** 新owner的02:02:24.466502–02:03:25.600706 metric窗（**61.134204秒**）中，鉅亨新增**62個耐久版本／60個新URL head及8頁已驗清單**，Global Voices獨立新增**7個版本／新head**；CMNews有2個既有文章更新、0個新head。鉅亨本窗0次transport、**1次server error**，原恢復仍持續；不能由短窗口宣稱來源零故障或速度極限。原215個不可變版本與當前head分開重驗，最新8個committed batch的**334個正文版本**驗證通過，其中**27份**是當前owner啟動後、實際新capture4的鉅亨正文，不是只讀到新版設定。版本／head／清單不相加為去重文章數。

| 來源／優先序 | 02:08:21讀回；URL head不是去重新聞總數 |
| --- | --- |
| 鉅亨（最高） | **3,643,832 complete**；22,457 pending、29 inflight、30 retry、35 retry_cooldown。11個image-only、346個quote-only、4,617個空模板、1,269個not-found分開。inflight包含尚待耐久commit的lease，不等於HTTP併發數。 |
| CMoney | 272,785 pending、456 retry；原403與拒絕期限保留。本61秒窗0請求，不以用途聲明或舊拒絕推定新一次來源結果。 |
| UDN | 169,731 awaiting_authorization；已知候選保留，精確批次全文渠道尚未驗證。 |
| Global Voices | **50,224 complete**、60,360 pending、3 inflight；本窗7個新head，既有robots floor保持。 |
| 中時／工商 | 108,155 awaiting_authorization；背景處理候選與可用渠道，不冒稱已取得全部全文。 |
| MoneyLink | 103,213 awaiting_authorization；金融資料／新聞全文的實際範圍分開。 |
| CMNews | **64,286 complete**、609 paywall_or_login；本窗2個既有head更新，原週期追新保持。 |
| MoneyDJ | 48,108 awaiting_authorization；原URL與證據保留，不用標題／摘要冒充正文。 |
| LTN | 31,119正文候選 awaiting_authorization；原14個RSS仍13份已驗、1份錯誤feed，資訊追新與全文／全史分開。 |

**完整驗收與仍未完成的部分。** 新增60個正反例先在原碼重現16 failed／44 passed，原失敗log保留並連到修正驗證；修正後**1,368項聚焦、152個目前新聞test檔／8,762項完整新聞回歸通過（180.22秒）**，不是全repo／GPU／並行dirty頁面驗收。只一次原news service／timer有序載入，**planned pause含啟動37.972秒**，owner `2419f9e0e2254985a1e687a81592059f`、PID816645、NRestarts0；實際進程clock、所有66個loaded capture及測試generation核對一致。原未到期拒絕、4個保留job及兩件body缺口期限保持，沒有operator publisher HTTP、SQL queue改寫、清除冷卻、改金鑰或其他服務重啟。來源capture契約改變後會重新驗證原容量測量，舊完整校準保存，沒有為提高顯示速率而重設上限或把warming判成故障。

目標`2026844`截至交接仍為`retry_cooldown/body_not_found`，原正常重查機會約**02:52:40**；`1257870`約02:45:13。**前者尚未以新契約重新取證、後者仍是真未知正文**，不提前改狀態，不保證指定秒一定取得。唯讀gateway的**全部935個當前來源鍵、既有unknown→other狀態映射、每個complete數、精確snapshot clock及fresh／consistent**對齊，無正文／raw匯出或page-view來源HTTP；本輪無UI／gateway變更、四尺寸browser或外網HTTPS驗收。沿工作偏好技能採共享根因修復、保留原件與來源時鐘、實測與未完成範圍分開。**全目標仍active／progress**；歷史清單仍發現新URL，不能由pending或短速率保證全部抓完ETA，也未宣稱所有provider已能取得全文、全部歷史完成或整體CPU／吞吐提升。

證據：[十項有限交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v152/handoff.json)、[935鍵原清冊與原分類器](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v152/before.json)、[9份實際耐久診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v152/actual_diagnosis.json)、[同raw分類與原HTML比較](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v152/actual_same_raw_replay.json)、[215份HTML／JSON／RSS真正adapter parity](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v152/canonical_original215_parity.json)、[152檔與來源版本pin](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v152/implemented.json)、[唯一正式owner與原重試期限](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v152/restored.json)、[215原件與當前head](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v152/formal215_preserved.json)、[同owner實際新增落盤](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v152/same_owner_progress.json)、[唯讀進度頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v152/gateway_actual.json)。原件僅在私人root，本文不附全文、帳號或API key。

2026-10-14 **前次工程摘要（v151；01:41:36 十項有限驗收接受）**：**鉅亨仍最高優先**，八家背景順序、私人非商用不公開用途及CNA暫緩保持。本輪修正共享文章選取的空工作浪費：真WAL快照上，24個可取得來源的完整claim輪次CPU中位數 **0.2138→0.1022秒，減少52.20%**，所有回傳欄位及公平序列完全相同。這是選取工作本身，不是整台CPU減半、HTTP吞吐翻倍或已達來源極限；與前v150的狀態CPU48.94%不能相加。

**根因、方案與反證。** 原owner約12分鐘已做5萬餘次article dispatch，大量沒有到期工作的來源仍查多個完整排序通道。現在先用**既有covering `dispatch`索引**精確判斷`pending／retry／retry_cooldown AND due<=now`是否存在；沒有工作才直接返回，有工作仍走原host／origin／finite scope、最新優先、missing／stored／undated／retry公平通道與lease。**不增快取、索引或表，不加等待／延遲，不改原due、attempts或資料**；新增工作、clock邊界及rollback下一次選取立即可見。空選取也不消耗公平credit或冒算一次來源請求。

最初檢查見到鉅亨`mixed`升速窗口，後續讀回已恢復`stable_pipeline_capacity`，不能由局部狀態斷言控制器卡死。因此**沒有放寬混合窗口／改來源調速**；真正傳輸錯誤仍可轉為recovering，原floor、429／403、Retry-After與來源互不連坐規則不變。先把確認的多餘計算移除，不以猜測配額取代有效正文與穩定窗口證據。

**完整選取成本與輸出。** 20個無到期工作的真實來源，32輪的原2,464次SQL→1,184次，CPU0.1631→0.0373秒（約減77%）。再以同一真WAL、凍結due clock、6次平衡比較測所有24來源的混合完整輪次：3,269→2,021次SQL、所有768次回傳序列SHA一致，兩邊各96次模擬lease選取。只抑制原lease UPDATE，正式DB唯讀、沒有取得生產lease／來源HTTP；因此這不是96篇下載、完整run_cycle／fsync／HTTP吞吐測試。正向公平與真正lease更新另由語義測試驗證。所有66個來源capture／authorization／config、正文／作者／發布時鐘及原排程保持精確指紋。

**正式落盤與進度頁仍一致。** 新owner的01:36:28.903637–01:39:33.431469 metric窗（**184.527832秒**），鉅亨新增 **124個耐久版本／110個新URL head、17頁已驗清單**，Global Voices獨立新增 **19個版本／新head**；CMNews有 **16個版本、0個新head**，是既有文章更新，不冒充新文章。鉅亨同窗有 **4次transport及1次server error**，原恢復繼續，不能稱零錯誤或已修好全部正文。版本／URL／清單不相加為去重文章數；持續新發現的pending與封閉歷史分母、全史完成時間分開。

| 來源／優先序 | 01:41:36當前owner讀回；不是去重文章總數 |
| --- | --- |
| 鉅亨（最高） | **3,641,598 complete URL head**；19,872 pending、32 inflight、92 retry、28 retry_cooldown及1個terminal body_not_found。inflight包括已回應、尚待批次耐久commit的lease，不等於HTTP併發數；空模板、圖片／quote-only、not-found等原結果分開。 |
| CMoney | 272,787 pending、454 retry；既有403／期限保留，本184秒窗0新請求／正文，不把舊拒絕算成本窗測速。 |
| UDN | 169,731 awaiting_authorization；原候選保持，精確批次全文渠道尚未驗證。 |
| Global Voices | 50,069 complete、60,515 pending、3 inflight；本窗19個新head，既有robots floor保持。 |
| 中時／工商 | 108,155 awaiting_authorization；私人用途不冒稱來源已開放全部自動取得。 |
| MoneyLink | 103,213 awaiting_authorization；新聞全文與金融產品範圍分開。 |
| CMNews | 64,285 complete、608 paywall_or_login；本窗16個既有head的新版本，原週期追新保持。 |
| MoneyDJ | 48,108 awaiting_authorization；原URL／證據保持，未以標題冒充正文。 |
| LTN | 31,118正文URL awaiting_authorization；14個RSS仍13份已驗、1個錯誤feed，資訊與全文／全史分開。本窗尚未到資訊追新，0請求不是被停用。 |

**驗收與部署邊界。** 23項新語義案例、280項聚焦通過；**151個目前新聞test檔／8,702項完整新聞回歸通過（178.44秒）**，不是全repo／其他並行dirty頁面／GPU驗收。原215個指定不可變版本與最新head分開重驗，最新8個committed batch的153個正文版本通過。只一次原news service／timer有序載入，**planned pause含啟動38.816秒**；owner `4158da4552f043ea8d3b4ec34f19d491`、PID747637、NRestarts0。實際新進程、metric epoch、測試generation與18,644次無到期工作選取已核對；原未到期拒絕及4個保留工作不降低／清除，無operator queue改寫、額外來源HTTP、金鑰修改或其他服務重啟。

唯讀gateway的**全部935個當前來源鍵與每個complete數、精確snapshot clock、fresh／consistent**核對一致；沒有raw／正文匯出或page-view來源呼叫。本輪無UI／gateway改動、四尺寸browser或外網HTTPS驗收。沿工作偏好技能採共享根因修正、保留原件與時鐘、完整混合工作量實測；**全目標仍active／progress**，403、全文渠道、native parser／傳輸及歷史發現仍有未完成工作，不把局部加速或測試通過當所有來源／歷史完成。

證據：[十項有限交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v151/handoff.json)、[935鍵原始清冊與原selector](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v151/before.json)、[所有24來源真資料完整選取parity](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v151/whole_round_benchmark.json)、[151檔測試pin](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v151/implemented.json)、[唯一正式owner與無到期工作探測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v151/restored.json)、[215原件與最新head](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v151/formal215_preserved.json)、[同owner真新增落盤](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v151/same_owner_progress.json)、[935鍵唯讀頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v151/gateway_actual.json)。原件僅在私人root，本文不附全文、帳號或API key。

2026-10-14 **前次工程摘要（v150；01:15:41 十項有限驗收接受）**：保留**私人研究、非商用、不公開**用途，**鉅亨最高優先**、八家背景順序與CNA暫緩不變。這輪已部署共用進度狀態的精確交易式彙總；在實際資料上，完整狀態計算加JSON序列化的CPU中位數 **0.8222→0.4198秒，減少48.94%**，完整JSON SHA逐字一致。這不是整台CPU減半、HTTP吞吐翻倍、已達來源上限或全部歷史完成；沒有降低發布頻率、省略進度清單或略過正文驗證。

**根因與設計。** 原heartbeat為五個discovery計數反覆全掃約37萬列。現在由同一SQLite交易的insert／delete／observed-field update trigger維護靜態計數及精確`(due, body_capable)`群組，查詢只讀真正到期群組；01:15:41原表 **373,099列、到期群組16個**，獨立原全掃結果與正式彙總完全一致。保留原float／NULL／錯誤／archive語義，沒有近似時間bucket；rollback跟原交易一起撤回，遺失衍生generation由原始列重建，舊唯讀DB只fallback原查詢、不取得寫入權。原資料、正文、時鐘、所有66個capture／authorization／config指紋及source deadline不變；新增的是狀態可觀測版本1。

先測過覆蓋索引，完整狀態CPU僅改善約2.2%，**沒有採用**。選定方案部署前完整JSON測試約省44%，部署後同一真WAL snapshot的6次平衡比較約省49%；兩者是不同資料時點，不當成下載速率比較。373k實際metadata的完整影子測試中，彙總建置CPU0.324秒，512次更新的邊際CPU約 **13.88微秒／次**；正式兩張衍生表約 **5.09 MiB**。這些成本分開呈現，影子更新不含正式磁碟／WAL fsync、主dispatch索引或HTTP，不宣稱整條I/O成本相同。

**真正落盤仍在前進。** 同一新owner的01:07:44.801211–01:10:24.181571 metric窗（**159.38036秒**）中，鉅亨新增 **69個耐久版本／62個新URL head、9頁已驗清單**，Global Voices獨立新增 **15個耐久版本／新head**。鉅亨本窗仍有3次transport及2次server error，保留原隔離／重試；LTN12次資訊請求沒有增加正文。版本、URL head、清單頁與RSS不相加為去重文章數；歷史清單仍在發現URL，不能由pending增加推論服務卡住，也沒有封閉全史完成ETA。

| 來源／優先序 | 01:15:41 owner讀回；不是去重文章總數 |
| --- | --- |
| 鉅亨（最高） | **3,640,510 complete URL head**；16,370 pending、12 inflight、71 retry、16 retry_cooldown。原空模板、圖片／quote-only及真正not-found分開。 |
| CMoney | 272,752 pending、453 retry；原403與拒絕期限保留，159秒metric窗0新請求／正文，不把舊403當本窗結果。 |
| UDN | 169,713 awaiting_authorization；候選保留，批次全文實際渠道尚未驗證。 |
| Global Voices | 49,920 complete、60,665 pending、2 inflight；本窗15個新head，既有robots floor未更動。 |
| 中時／工商 | 108,154 awaiting_authorization；沒有由用途聲明推定來源已提供全部自動取得權限。 |
| MoneyLink | 103,213 awaiting_authorization；編輯全文與其他金融產品範圍分開。 |
| CMNews | 64,284 complete、608 paywall_or_login；本窗0新增，原週期追新保持。 |
| MoneyDJ | 48,108 awaiting_authorization；原URL／證據保留，不把標題當全文。 |
| LTN | 31,117正文URL awaiting_authorization；14個RSS仍13份已驗、1個錯誤feed。資訊追新和正文／全史分開。 |

**測試、原件與部署接受。** 95項聚焦、**150個目前新聞test檔／8,679項完整新聞範圍回歸通過（218.73秒）**；不是全repo、並行dirty public dashboard或GPU驗收。原215個指定不可變版本與最新head分開重驗，最新8個commit batch的171個正文版本通過。直接benchmark CLI原先缺repo import path已修正，外部cwd的CLI測試也通過；原失敗run／log留存並附成功處置。只一次原news service／timer有序載入，planned pause含啟動 **36.155秒**，owner `84daa9b006604d669333b5550022f63d`、PID673290、NRestarts0；原未到期拒絕及4個保留job不降低或清除。其他服務、金鑰、來源端點、parser及排程不動，沒有operator publisher HTTP／SQL queue改寫或第二collector。

**面板與未完成邊界。** 當前唯讀gateway的 **935個來源鍵、所有complete數、精確snapshot clock及fresh／consistent**逐項對齊；不傳raw／正文，page view不呼叫來源。這輪沒有UI／gateway改動、四尺寸browser或外網HTTPS驗收。前v149的FTV分類修正繼續正常運行，原候選`2022706W0028`後來已於00:49:53判為scope excluded、沒有保存body，不仍稱該URL pending；分類接受不等於取得轉載正文。沿工作偏好技能保留共享根因修正、原件、來源時鐘及實測／剩餘範圍分離。全新聞goal仍 **active／progress**，403、精確全文渠道、正文／傳輸與歷史發現缺口尚未全部解決，沒有冒稱完成。

證據：[十項有限交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v150/handoff.json)、[正式完整狀態parity與效能](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v150/live_benchmark.json)、[完整metadata維護成本](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v150/maintenance.json)、[150檔測試pin](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v150/implemented.json)、[唯一正式owner](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v150/restored.json)、[215原件與最新head](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v150/formal215_preserved.json)、[同owner真進度](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v150/same_owner_progress.json)、[真更新後彙總／全掃一致](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v150/post_progress_summary.json)。原件僅在私人root，本文不附全文、帳號或API key。

2026-10-14 **前次工程摘要（v149；00:40:16 十項有限驗收接受）**：保留**個人研究、非商用、不公開**用途，**鉅亨最高優先**與八家背景順序不變。這輪修正民視文章內明列「中央社」轉載，卻被標為作者未驗證的分類根因；不是開啟中央社下載、判定所有網站均授權，或把第三方正文算成取得。CNA仍按使用者指示暫緩，既有來源、作者、原值、發布時鐘與配額不變。

**規則有原生證據，且已由正常服務驗收。** [原先的民視候選頁](https://www.ftvnews.com.tw/news/detail/2022706W0028)在web reader的較舊快取中明列中央社來源；這只用來提出修正候選，不當成本次HTTP或該URL已修復的證明。共享FTV adapter現在只辨識可見、文章正文內、整段獨立的`（中央社）`／`(中央社)`；引用、帶後續正文的第一行、圖片、caption、aside、頁面footer、身分不符及付費／登入不會因此變成可取得正文。原scope仍排除第三方轉載，FTV capture版本**4→5**，所有authorization、其他65個capture指紋、API／RSS、端點、頻率與工作順序精確不變。

本次同一新owner在正常排程中已計到**57次精確native CNA來源註記分支**；最新scope head樣本有本次核對時鐘、沒有body SHA，不保留／公開第三方正文。不把所有scope head都假定為中央社，亦不把分支計數當去重文章數。原候選`2022706W0028`在00:40:16仍為pending、舊錯誤保留，沒有聲稱該例已重新取證；原1,536個作者parser缺口也不是全部解決。FTV新指紋由原佇列正常reconcile重新排parser failure，來源拒絕、付費牆、已保存正文與finite cohort不變，沒有操作員SQL改狀態或另外的HTTP owner。

**整段來源鏈路仍在前進。** 同owner的00:38:02–00:40:05 metric窗（**123.206535秒**）中，鉅亨新增**119個耐久版本／119個新URL head、15頁已驗清單**，Global Voices獨立新增**10個耐久版本／新head**；LTN8次RSS、FTV61次終端請求各自分開，不相加為正文篇數。本窗鉅亨transport／server error均0，但不能推論整個歷史零故障、最高速或來源上限。歷史清單持續發現URL，pending增加不等於服務卡住；全史分母與完成時間仍未封閉。

| 來源／優先序 | 00:40:06對齊狀態；不是去重新聞總數 |
| --- | --- |
| 鉅亨（最高） | 3,639,188 complete URL head；12,704 pending、4 inflight、36 retry、35 retry_cooldown。實際落盤持續，短body／傳輸／空模板各別追蹤。 |
| CMoney | 272,755 pending、450 retry；之前native403與拒絕期限保留，本metric窗0新請求／正文，不把舊403當本窗結果。 |
| UDN | 169,713 awaiting_authorization；已有候選保留，尚無本帳號已驗的批次全文渠道。 |
| Global Voices | 49,726 complete、60,859 pending、2 inflight；本窗10個新head，官方robots的10秒floor保持。 |
| 中時／工商 | 108,154 awaiting_authorization；用途與來源實際存取範圍分開，不猜造帳號或API。 |
| MoneyLink | 103,213 awaiting_authorization；新聞與金融產品渠道分開。 |
| CMNews | 64,281 complete，608 paywall_or_login；本窗0新增，原1h追新仍保留。 |
| MoneyDJ | 48,108 awaiting_authorization；原URL與來源證據保留，沒有把標題／摘要冒充正文。 |
| LTN | 31,113正文URL awaiting_authorization；14個官方RSS仍13份已驗、1個錯誤feed，最新520筆資訊不是全文／全史。300秒是本機追新週期。 |
| 民視（既有獨立來源） | 869 complete；1,441 pending、1 inflight、30 author attribution及13 identity未驗、267 scope excluded、3,404 waiting_cohort。57個新分支沒有增加complete。此列SQL讀取為00:40:16，與上面metric快照分開。 |

**原件與測試完整保留。** 所有**869個當前已保存FTV head、57個Parquet檔**的immutable收據／raw SHA均驗證；同一份raw用精確原FTV4 credit parser與FTV5重播，正文、作者、原發布／修改時鐘、身分及目前歸屬證據欄精確相同，沒有已保存正文命中新規則而需改判排除。**137份歷史歸屬證據metadata**因較早capture格式或原canonical-resolution附加證據而不同，與本次parser差異分開保留，不改寫原件；正文／作者／時鐘仍逐件精確。不要用新parser產生的附加證據覆蓋歷史metadata。

25項新正反例與既有測試合計**308項聚焦通過**；**149個目前news test檔／8,670項完整新聞範圍回歸通過（199.90秒）**，不含並行修改中的`test_public_dashboards.py`，不是全repo／其他頁面／GPU驗收。原215個指定原始版本與最新head分開重驗，最新8個commit batch也通過；6個工程失敗run／log保留並追加後續成功處置，沒有消除來源403或舊證據，最初匯入錯誤不冒稱語義TDD紅燈。

只做一次原news service／timer有序載入，**planned pause含啟動48.143秒、start到fresh44.692秒**；owner `eac60e8fd7e54f7b9fff1734c5cedc64`、PID596908、NRestarts0，實際新進程／metric clock／input1／各來源capture指紋都讀回一致。原未到期拒絕及4個保留discovery job未降低或清除；無force kill、重複collector、其他服務重啟或金鑰修改。新聞唯讀API在00:39:35與原狀態快照935來源鍵、完成數、fresh／consistent、正文不輸出及page-view零來源HTTP核對一致；本輪沒有UI／gateway修改、四尺寸browser或外網HTTPS驗收。沿工作偏好技能的**共享根因修正、原件保存、實測與未完成邊界分開**；不把正文分類修正當CPU加速或下載速度極限。

全新聞goal仍active，本輪為`progress`；未聲稱八家全文、所有935來源鍵或全部歷史完成。證據：[十項有限交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v149/handoff.json)、[所有869份原件與metadata分離重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v149/stored_ftv_replay.json)、[149檔測試版本pin](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v149/implemented.json)、[8,670項run](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v149/regression_run.json)、[唯一FTV5正常owner](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v149/restored.json)、[215原件與最新head](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v149/formal215_preserved.json)、[實際同owner進度與native分支](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v149/same_owner_progress.json)、[935鍵唯讀頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v149/gateway_actual.json)。原件僅在私人root，本文不附全文、帳號或API key。

2026-10-13 **前次工程摘要（v148；23:58:42 十項有限驗收接受）**：**鉅亨持續下載，其他可取得來源獨立並行**；私人研究、非商用、不公開的用途已保留，不再重問。鉅亨優先、八家背景順序、既有66個來源設定與CNA暫緩不變。這輪修正**失敗診斷看不到實頁結構**及**共同收據讀取的多餘字串轉換**，不是八家全文均已取得或全部歷史完成。實際唯讀面板此輪列出935個來源鍵，不把舊934鍵快照當成現在的固定總數；沒有因此另啟來源或重寫原清冊。

**實際進度，不以服務active代替落盤。** 同一新owner的23:54:16–23:58:08 metric窗（231.904秒）中，鉅亨新增**160個durable版本、145個新URL head、19頁已驗清單**；Global Voices新增**22個durable版本／新head**。LTN有13次RSS請求，但不增加正文計數。鉅亨同窗仍有4次transport error與1次server error，既有重試／隔離繼續，不能稱零故障或已測出來源上限。不同URL可指向同篇文章，版本數、URL head、清單頁與RSS不能相加為去重篇數。

| 來源／優先序 | 23:58:08對齊快照與剩餘範圍 |
| --- | --- |
| 鉅亨（最高） | 3,636,780個complete URL head；4,651 pending、10 inflight、46 retry、24 retry_cooldown。清單仍會發現新URL，這不是封閉全史分母；短正文、空模板與真正404各別保留。 |
| CMoney | 272,758 pending、447 retry；已觀察native403，本metric窗零新請求／正文，不把前窗拒絕混成此次速率或成功。來源原拒絕期限保留。 |
| UDN | 169,713 awaiting_authorization；原候選與官方途徑保留，仍沒有已驗證、本帳號可用的批次全文渠道。 |
| Global Voices | 49,503 complete、61,082 pending、2 inflight；本窗22個新head。官方robots的10秒floor不變。 |
| 中時／工商 | 108,154 awaiting_authorization；未由個人研究用途推定來源已允許全部自動取得。 |
| MoneyLink | 103,213 awaiting_authorization；編輯內容與金融資料產品範圍分開，沒有猜造全文API。 |
| CMNews | 64,277 complete、608 paywall_or_login；本窗零新增，原1h首頁追新仍在，不因正常未到期強制重抓。 |
| MoneyDJ | 48,108 awaiting_authorization；正式自動取得範圍及歷史端點尚未驗證。 |
| LTN | 31,096正文URL awaiting_authorization；14個RSS仍為13份已驗原件、1個錯誤feed，300秒是本機追新排程而非官方配額。RSS摘要不是全文／全歷史。 |

**診斷盲區已修，兩筆正文缺口尚未修好。** 原診斷在native identity不成立時提前返回，連JSON欄位形狀與article-container都無法看到。工具現在額外記錄有界的欄位型別／長度、same-id JSON-LD、容器與header結構；不留正文、HTML、作者值或script原文，不以診斷取代正式parser／授權。一次exclusive owner有序取證只發出2個原本已到期、無lease、未保存正文的請求：`5889350`當時25秒ReadTimeout；`5887163`回200，但metadata內容362字、schema正文45字、容器可見53字，native scope不唯一，正式結果為`body_not_found`。不能因此斷言45字就是完整原文、略過身分／body驗證或把HTTP200列為成功。23:58:42後續真head讀回兩筆均為`body_not_found`／retry_cooldown、沒有body SHA；第一筆的後續狀態不是原探測的transport結果，兩者分開保留。兩次前置檢查沒有到期工作時均為0HTTP／0重啟。

**共同I/O優化不刪驗證、不漏收據。** 原復原逐件`read_text`後再交給native JSON解碼，ASCII收據會產生不必要的中間字串。新共享`read_acceptance_receipt_input`仍每件完整讀取一次、完整解碼全部欄位；ASCII／UTF-8 locale走bytes，非ASCII、NUL或其他locale在同一份已讀bytes上使用原text語義，保留universal newline、錯誤位置、BOM／UTF-16／invalid UTF-8拒絕、任意精度int、重複key、NaN及孤兒收據的contract／digest／row驗證。沒有按檔名／mtime跳過已commit JSON、沒有新增I/O執行緒或第二writer。僅增加input版本1供live讀回；native decode版本2、所有HTML/API/RSS/auth capture指紋、端點／頻率／來源用途精確保持。

**速度依相同分母報告。** 8,192份真immutable收據（3,014,263 JSON bytes）、3組ABBA共12次，在read-only來源及in-memory recovery索引比較完整值；每次仍64組covering lookup。full JSON parity、前後SHA、0來源queue寫入均通過。初次prototype為wall 0.4420→0.3791秒／CPU 0.4401→0.3757秒；現行共享實作確認為wall **0.4764→0.4440秒（省6.8%）**、CPU **0.4765→0.4332秒（省9.1%）**，採現行結果作保守結論，不挑最大一次。這是受控收據處理測試，不是外網下載吞吐量或冷開機。實際reload觀察recovery wall 34.6778→20.7080秒、owner初始化35.5525→21.1796秒；啟動時負載／cache不同，另列observational結果，不能當ABBA因果提升40%的證明。

**十項工程交接通過，資料與啟動成本分開。** 240項診斷、318項共享讀取／parser聚焦，以及148個目前news test檔／**8,645項完整新聞範圍回歸**通過（224.02秒）。不含其他角色正在修改的`test_public_dashboards.py`，不是全repo／其他頁面／GPU驗收。正式215個舊原始版本逐件核對raw／正文／作者／捕捉與發布時鐘；最新head另外驗證，可合法指向更新版本。最新8個commit body batch再獨立重播通過。loopback唯讀API935鍵、fresh／consistent、input1、正文不輸出及page-view零來源HTTP皆已核對；本輪沒有UI／gateway重啟、四尺寸browser或外網HTTPS驗收。

本輪一次bounded診斷取證的planned pause為81.581秒；測試全部在健康服務繼續抓取時完成，其後只載入一次已測試的新聞版本，planned pause35.411秒、start到fresh26.647秒。新owner `adea4f9690fc4cb5a96085224156c32a`、PID469840、NRestarts0，啟動晚於已pin source mtime並實際回報input1；原future拒絕deadline及4個reserved discovery jobs保留。沒有force kill、其他服務重啟、配額重置、操作員queue重寫或重複啟動同一handle。沿用工作偏好技能的**共用根因修正、原值與收據保留、有限驗收與未完成邊界分開**，沒有複製另一套管線。全新聞目標仍active；短正文、拒絕或未接入渠道未冒充全部完成。

證據：[十項有限交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v148/handoff.json)、[兩筆真失敗結構及限制](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v148/actual_failure_probe.json)、[原失敗與scope讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v148/probe_readback.json)、[現行8,192真收據ABBA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v148/receipt_shared_benchmark.json)、[148檔測試與版本pin](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v148/implemented.json)、[8,645項run](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v148/regression_run.json)、[測試後input1真owner](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v148/restored_input1.json)、[215原件及最新head](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v148/formal215_preserved.json)、[同owner並行真進度](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v148/same_owner_progress.json)、[唯讀935鍵面板](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v148/gateway_actual.json)。原件只在私人root，本文沒有原始全文、API key或授權判定。

2026-10-13 **前次工程摘要（v147；22:58:32 九項有限驗收接受）**：保留使用者的**私人研究、非商用、不公開**用途，不再重問用途。**鉅亨仍最高優先**，原八家背景順序、934來源鍵與CNA暫緩不變。這輪根治兩筆2026-10-10留下的mobile `article_identity_not_verified`：不是來源拒絕、缺失正文或登入失敗，而是native parser尚未支援兩個實際公開query形式。

**兩筆已由正常佇列真正取得，不是把主網址舊正文複製過來。**

| 原始mobile網址／query | 完整來源正文 | 原始發布時鐘 | 真正抓取時間（Taipei） |
| --- | --- | --- | --- |
| `/news/id/4948791?amp;utm_medium=RSS&amp;utm_campaign=relate` | 420字 | `2022-09-08T04:59:23.000Z` | 2026-10-13 22:54:11 |
| `/news/id/4002424?defer_action=auth` | 408字 | `2017-12-29T08:56:34.000Z` | 2026-10-13 22:54:04 |

先有界、匿名、原路徑檢查兩個實頁：皆HTTP200，visible/current/schema三份正文的whitespace comparison SHA一致，同ID、free、作者與原發布clock宣告成立。診斷只保存metadata，沒有因HTTP200接受正文或保存未知identity的raw。**不刪query、不執行auth action、不換host、不改credentials／robots／403／429限制。** HTML capture **42→43**只支援兩個實測的精確query形式，仍須通過完整native身份、免費、可見性、作者、時鐘與正文三方驗證；近似、額外token、未知參數、付費或互相衝突的宣告仍拒絕。API bulk、來源授權、其他provider及所有四份config的指紋／SHA保持。

唯一canonical queue依原恢復機制，只將這兩個unleased、missing-body、舊identity outcome排入正常重查，保留原attempts1與2026-10-10失敗clock；到期與refusal不由operator清零。真正完成後逐件重播native43 raw、完整body／作者／aware發布clock、原query requested/final/source-request URL與耐久batch收據。**同一份真正入庫的raw，舊query guard重現identity拒絕，新guard接受**，但不宣稱與先前未保存的診斷bytes相同。兩個主網址本來就有資料，因此新增的是**2個已驗證原URL view，不是2篇之前全無的unique新聞**；正式有限清冊由213→215 views，不是全站驗收。

**持續追新的current pointer與不可變原件分開核對。** 操作前第一次helper錯把當前URL head當成永久不能變：15筆已有合法新版，該核對失敗沒有停服務。修正後，213份原始版本逐欄與原batch/receipt獨立驗證，213個當前head另做完整source replay，最近8個committed batch另驗。原始body、raw、作者、發布與capture clock沒有因latest pointer更新而被覆寫；原失敗run留存並附後續成功處置，不凍結健康增量或把原件與latest混成同一證明。

**新版與進度網頁已連動；健康來源仍獨立進展。** 22:53:17.287642–22:57:56.270121同owner **278.982479秒**窗口，鉅亨新增 **117個durable版本、106個新URL head、51頁已驗清單**；Global Voices獨立新增 **25版本／新head**。鉅亨仍有6次server-error observation，故障隔離／重試保留，沒有宣稱零錯誤或理論／官方最高速。CMoney在該窗口零請求／正文，不能抹掉較早403；CMNews尚無到期工作，LTN的13次RSS terminal request不是全文。22:58:32鉅亨complete為**3,632,887個URL head**，仍有pending3,270、retry45、retry_cooldown35及來源empty/notfound/image/quote等不同outcome，不是全史完整。唯讀gateway確認934鍵、同新owner heartbeat、鉅亨真進度、LTN13/14資訊與1個feed錯誤分開，0publisher calls／0raw正文匯出；本輪未改UI或重啟gateway，不宣稱重新做過四尺寸或外網browser驗收。

**測試與交接成本明列。** 原診斷安全218項、query/body/lease/deadline聚焦359項通過；新版完整**146個新聞測試檔／8,595項，256.79秒**通過。本輪完整範圍是所有`test_news*.py`與`test_cnyes_news_bulk.py`，**不含另有並行修改的`test_public_dashboards.py`**，不能與前v146的8,675項（含該public模組）直接比較成退步／漏測。先恢復健康owner再做工程與全部回歸，測試跑完才短暫交接native43；診斷交接含啟動核對**74.4118秒**、新版交接**77.6003秒**（start→fresh50.9632秒），不把planned pause算成吞吐提升。沒有重啟交易、其他collector或修改`.env`。本輪為2個實頁修復的`progress`；全來源／全歷史goal仍active，CMoney403、四家未確認的精確全文渠道、LTN失效people RSS及其他正文／傳輸缺口仍未宣稱已解。

證據：[九項有限交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v147/handoff.json)、[兩份真原query全文與同bytes舊guard重現](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v147/native_actual.json)、[213舊原件與最新head分開驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v147/formal213_preserved.json)、[原路徑有界metadata診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v147/probe_readback.json)、[HTML43 pin與測試範圍](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v147/implemented.json)、[正常queue migration與新版owner](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v147/restored_native43.json)、[對齊同owner進度窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v147/same_owner_progress.json)、[934鍵唯讀頁讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v147/gateway_actual.json)。私人原文／JSON不新增到瀏覽器，其他來源的個別存取與用途限制不因使用者研究用途而偽造成來源許可。

2026-10-13 **前次工程摘要（v146；22:10:45 對齊計數、22:11 原頁讀回）**：使用者的用途保留為**私人研究、非商用、不公開**，不再重問用途。**鉅亨最高優先**，背景順序仍為 **CMoney → UDN → Global Voices → 中時／工商 → MoneyLink → CMNews → MoneyDJ → LTN**。已取得來源各自並行；不因一個 RSS、單篇文章或面板故障停下健康來源。原934來源鍵、全部已知URL、原作者／時鐘／原值與CNA暫緩保留。

**這次有兩項真正落盤的進展，不是只有放寬設定。**

- 鉅亨三個舊 `exp=a` 變體按原期限成功取得 **929、806、880 字原文**，raw／body SHA、來源作者、aware發布時間與耐久batch收據重播一致。與先前823字那篇合計，**四個指定變體全部已驗**；原210正式head保留，新增3個正式head。不複製四篇早已存在的API主文，不把URL變體算成三篇原先完全沒有的新聞。兩個原identity failure的本次原件在舊visibility guard語義仍會被純空隱藏連結誤拒；另一筆原本為transport error，不能把傳輸失敗都歸因parser。
- 接入自由時報官方列出的 **14個RSS資訊端點**；實測 **13個成功、1個人物feed回404**。獨立逐件核對13份實際XML、Zstd Parquet、SHA及私人耐久收據；該輪共 **520筆feed資訊、484個不同連結、277,302 bytes解碼後XML**。這是當次各feed的最新快照，會重疊、會追新，不是全文篇數、累計下載流量、線上wire bytes或全歷史容量。官方仍列出失效的`people.xml`，錯誤與正常重查期限保留，未猜新路徑／冒稱14個全成功。[官方RSS說明](https://service.ltn.com.tw/RSS)

**RSS能力與全文權限在共用入口分開。** 只有精確已註冊feed與同origin `/robots.txt` 可用這個資訊能力，不能取得首頁、article HTML或轉址到另一feed。沿同一新聞owner的robots、DNS檢查、限流、冷卻與writer；完整XML及標題／摘要／link／原始與aware `pubDate` 私人保存，沒有作者則保留NULL。XML只在canonical writer解析一次，內容定址檔案可重用；收到304須先核對原收據。未通過XML、receipt、fsync或queue commit不算完成；body/API/RSS/auth既有指紋精確保持。新增資訊不增正文計數、不啟用LTN HTML、不公開摘要或原件、不注入遠端訓練。

**面板已連動，錯誤、排程與全文分母不混用。** 唯讀API顯示RSS **13／14、520筆、最新XML大小、1個錯誤端點、最近核對／下次排程**；300秒是本機選定的追新週期，不是官方配額。來源冷卻優先於RSS追新狀態；有錯誤端點顯示需注意。原queue的`due=0`顯示「已到期，等待排程」，不是1970年預約。LTN全文完成仍為0、歷史完成ETA未知，不把持續RSS追新標為全部新聞已抓完。桌機1440、平板768、手機390及320的實際Chromium/CPU-2D頁面均核對相同render generation、無橫向溢出／JS錯誤／外部來源HTTP；這不是GPU/WebGL或外網HTTPS browser驗收。

| 來源／優先序 | 22:10:45對齊觀測與剩餘範圍 |
| --- | --- |
| 鉅亨（最高） | 同owner **693.835秒**新增 **622個durable版本、551個新URL head、87頁已驗清單**；12次transport error仍保留並恢復，不能稱零故障／官方或理論極限。complete為3,630,166個URL head，不是去重文章數。 |
| CMoney | 新owner先前正常到期請求仍403；該693.835秒窗沒有新的請求或正文，不能把前一窗口403混入分母。既有拒絕metadata、冷卻與URL保留，不換host／偽裝身份。 |
| UDN | 169,696個awaiting_authorization；現有候選及官方途徑保留，精確帳號／歷史／全文API範圍與schema尚未驗證。 |
| Global Voices | 同窗獨立新增 **68個durable版本／新head**；原native與已註冊RSS繼續，官方10秒robots floor保留。 |
| 中時／工商 | 108,153個awaiting_authorization；候選來源、全部URL與官方用途證據保留，不用另一個集團host代替未驗scope。 |
| MoneyLink | 103,211個awaiting_authorization；不由會員產品或其他金融API推定新聞全文接口。 |
| CMNews | 64,274個complete；同窗新增 **5個版本／3個新head**，1h首頁追新延續；付費／登入結果仍單獨保留。 |
| MoneyDJ | 48,106個awaiting_authorization；目前原件取得範圍尚未驗證，不把標題或第三方索引當全文。 |
| LTN | 14個官方RSS，13個有真收據、人物feed404；每300秒資訊追新。31,049個awaiting_authorization是正文URL工作，不是13個RSS失敗；全文未啟用、沒有全史完成時間。 |

**測試與交接成本如實區分。** 365聚焦、146檔／**8,675項完整collector/news＋public回歸**通過；其後RSS錯誤／due=0面板修正與當時共享public契約另通過 **306項**。共享台股報價／paper-report程式與fixture之後仍有使用者修改，保留不回退、不因此重新載入；不能聲稱後來整個共享public程式與先前306項為同一版本。新聞自己的code/config／body指紋、UI及獨立路由另有 **192項目前範圍回歸通過**，其他頁面的變動不擴成新聞重啟需求。最終 **15／15有限handoff檢查通過**，不是全repo或其他頁面的驗收。

原news service／timer只做一次有序恢復，owner `d06f022ca08d4768a0c75fcacba1b3c3`、PID197884。**planned pause到fresh讀回987.822秒（含工程修改與測試），start到fresh讀回79.123秒**，不是速度提升，也不是實測cold boot；後續應先完成隔離測試，再短暫載入，避免把最高優先來源停在測試等待。面板另一次唯讀gateway更新，並未因此再重啟news或向交易服務發出重啟。四個工程失敗run／log保留並追加後續成功處置，不消除來源403／404。最新8個已提交正文batch另讀回通過，與693.835秒metric窗不是同一分母。

沿用使用者工作偏好與dashboard技能的**共用入口、重要狀態優先、來源收據支持數字、摘要／正文分母分開**；這些技能沒有另建來源管線或新網站。全新聞目標仍active：已新增一段可驗資訊能力，並有3個真正恢復的舊原文；**未宣稱八家全文均已取得、934來源全部啟用或全部歷史完成**。

證據：[15項有限交接驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v146/handoff.json)、[13份真RSS與失效feed](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v146/rss_actual_receipts.json)、[三份新native原文重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v146/native_actual_newly_accepted.json)、[原210件保留](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v146/formal210_current.json)、[對齊進度窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v146/same_owner_progress.json)、[面板API讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v146/gateway_current.json)、[四尺寸真browser](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v146/browser_current.json)、[完整測試generation](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v146/implemented.json)、[192項目前新聞範圍回歸](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v146/news_scope_regression_run.json)、[共享其他角色的並行變更邊界](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v146/other_public_role_drift.json)。私人XML、摘要與正文不新增到瀏覽器；本文只有摘要和本機證據。

2026-10-13 **前次工程摘要（v145；21:16並行觀測、21:18原件驗收）**：**鉅亨仍最高優先**，等待時沿 **CMoney → UDN → Global Voices → 中時／工商 → MoneyLink → CMNews → MoneyDJ → LTN** 處理問題。可取得來源獨立並行；只有鉅亨發生可驗證的操作阻塞才切回，不因單篇失敗停止其他健康來源。原934鍵、全部已知URL、工作順序與CNA暫緩保留。使用者再次確認用途為**私人研究、非商用、不公開**；不把用途誤標為商業，也不由此捏造來源登入憑據、解除403或把RSS摘要標成全文。

**已根治一類正文誤拒絕，並有真實原件證明。** 舊鉅亨`exp=a`原頁的canonical／OG／schema／作者／aware clock及三種正文表示已相符，卻在最終可見性檢查被一個`display:none`、零文字的`a`拒絕，錯誤標成`article_identity_not_verified`。三次有限metadata取證逐步定位到原visibility guard；沒有保留失敗頁raw或正文，也未重設排程。共享native parser現在僅略過**純空連結**：無非空白文字、無子元素／script／comment／圖片、無ARIA／title／itemprop／content類替代資料。隱藏金融文字、圖片、未知子節點與權限／作者／時鐘／三正文一致性檢查仍拒絕或照常驗證；不靠刪新聞數值修復。僅兩個production modules改動，鉅亨native capture **41→42**；所有授權、API批次、RSS、其他provider capture、端點／頻率與工作順序精確不變。

**21:15:29真正下載成功，不是只有測試或重排。** `https://news.cnyes.com/news/id/505729?exp=a`按原到期順序取得**823字原文**，新native42指紋、raw／body SHA、原始發布時間`2015-04-22T06:40:06.000Z`及耐久batch收據均重播一致。來源宣告作者「鉅亨網預設編輯」與可見署名「鉅亨網記者胡薏文  台北」分開保留，沒有互相替換或猜人名。原raw確有一個無文字、無子元素、無替代資料的隱藏空連結；對**同一份本次成功保存的bytes**重建舊guard語義，仍得到`article_identity_not_verified`，現行guard則保留同823字body SHA而成功。這不是聲稱未保存的舊失敗HTTP回應與新回應完全相同。

**四篇主要原文早已存在，剩餘三個變體不冒稱修好。** 四個default URL在10/08已由鉅亨API取得原文與完整收據，本輪再次逐件重播／讀回保留，不複製到`exp=a`、不把網址變體算成缺四篇新聞。21:16讀回另外三個variant的原next due為**21:58:40、22:05:21、22:10:41**；最後一個之前是`transport_error`，與parser失敗不同。保留原期限、去重優先序與負面證據；實頁未接受前不宣称四個variant全部修復。

| 來源／優先序 | 21:16本輪實際狀況 |
| --- | --- |
| 鉅亨（最高） | 同owner230.646秒新增260個durable正文版本、226個新URL head計數與37頁已驗清單；仍有1次傳輸錯誤，不稱零故障／理論上限。當時complete為3,627,942個URL head，不是去重文章數。 |
| CMoney | 原到期native請求仍403，標頭metadata與21:27:58退避保留；0個新正文，不清冷卻、換host或把拒絕推論為數值速率上限。 |
| UDN | 169,684個awaiting_authorization；官方歷史匯出／分析途徑已清點，但本帳號介接範圍與資料schema仍待提供。 |
| Global Voices | 同窗獨立新增22個durable正文版本／21個新head；原native／RSS繼續，官方10秒robots floor不改。 |
| 中時／工商 | 108,149個awaiting_authorization；保留原來源及已知URL，不用同集團host代替缺少的介接範圍。 |
| MoneyLink | 103,211個awaiting_authorization；公共footer與不同產品權利已分開清點，不推定其他產品API提供新聞全文。 |
| CMNews | 64,269個complete；本次metric窗尚未到追新首頁的原1h期限，所以0個新請求不是服務故障。前owner此前另已取得2個新正文，不能混入本窗分母。 |
| MoneyDJ | 48,106個awaiting_authorization；candidate adapter、來源用途證據與全部URL保留。 |
| LTN | 30,559個awaiting_authorization；官方RSS有標題／摘要／link／pubDate，已驗無fulltext／creator；RSS範圍與HTML全文介接分開，不把兩者混成已抓全。 |

**工程、來源、進度三個證明分開。** 756項聚焦與145檔／**8,632項完整news＋public回歸通過**（251.32秒；不是全repo、GPU或browser／外網HTTPS測試）。原209個正式正文的raw、作者、原始時鐘、capture與收據精確保留；最新八個已提交batch另核對267個body versions，與上述230.646秒metric窗不是相同分母。唯讀gateway維持934鍵、fresh／consistent、不輸出正文、page-view零publisher calls；沿原狀態快照連動，沒有為查進度呼叫新聞來源或重啟gateway。

單次測試後有序載入native42，實際新owner`234ed53d30a2416c89d03c6ad14ce29b`，drained→fresh snapshot **31.126秒**，其他／交易服務未重啟。三次較早metadata診斷的intent→fresh readback成本分別**47.848、99.527、161.643秒**，包含各自取證／等待／原件核對，不與這次drained→fresh混比或隱藏成吞吐提升。其間曾觀測WSL boot ID改變，並非本輪發出的全機重啟；不推定原因或當作cold-boot驗收。兩個早期diagnostic fixture失敗及一個私人工程helper欄位拼錯的run／log保留，追加後续成功驗證，不改寫歷史成成功。

**12／12有限handoff與6／6同raw機制驗證通過，全新聞目標仍active。** 沿用共用owner、保留原值與時鐘、按實頁修共享guard，並把正文接受與網址／metadata計數分開，是工作偏好技能對本輪的影響。剩餘variant與八家缺口仍按優先序處理，未宣稱全部啟用、全部歷史完成、法定使用範圍已判定或達到官方／理論最大速率。

證據：[九來源與12項handoff](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v145/handoff.json)、[真native原件及同bytes舊／新guard](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v145/native_same_raw_guard_readback.json)、[測試generation](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v145/implemented.json)、[完整回歸run](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v145/regression_run.json)、[原件／期限保護與實際載入](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v145/restored_native42.json)、[原四篇API原文](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v145/four_primary_body_readback.json)、[private失敗site／結構metadata](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v145/native_identity_metadata_v3.json)。私人原件／JSON不新增到公開瀏覽器，本文僅記摘要和本機證據連結。

2026-10-13 **前次工程摘要（v144；20:06原件及九來源驗收）**：**鉅亨仍最高優先**，背景沿 **CMoney → UDN → Global Voices → 中時／工商 → MoneyLink → CMNews → MoneyDJ → LTN** 處理。已啟用來源獨立並行；工程注意力只在鉅亨真正阻塞時切回，不因其他來源等待而停止它。原934鍵、全部已知URL、既有收據及CNA暫緩保持，沒有另開正文管線。

**Global Voices的錯誤暫停已由真原文修復。** 一次canonical metadata-only取證證明「St. Vincent & the Grenadines, Haiti: Access Denied」是正常公開新聞的真標題，canonical、header/footer同一作者、來源aware發布時鐘及當篇CC均成立；舊guard只認標題開頭的`Access denied:`，錯把結尾詞組當來源拒絕。第一版HTML8→9仍在正常重試中失敗，原11:48:32錯誤、attempts4及20:03:32重試期限保留，沒有把測試通過等同下載成功。再讀七個已保留的native原件，全部都有正文外`#respond > .respond-content > form#commentform > h-captcha`留言驗證；新版全頁`[data-sitekey]`檢查誤把留言驗證當閱讀封鎖。最終HTML10只在完整文章身分先成立、留言元件單一且精確綁定同一`#page-content`、正文之外時排除這個誤判；explicit challenge、未知／重複／正文內captcha、member、403／429、robots仍拒絕。沒有提交、破解或操作captcha。

**真正落盤與原件重播。** 原網址在20:06:03正常取得237字完整來源正文，保留50字來源標題、同一作者、`2010-08-23T17:31:59+0000`原發布時鐘、CC、raw SHA／body SHA及耐久batch收據，逐件重播相符。全部208個前次正式原件／head／capture clock精確保留，另核對最新八個已提交batch；這是指定原件與有限批次驗收，不是所有歷史已完整。原handoff內兩個`GV9`字樣是操作檢查標籤未同步更名，其實際predicate核對的是已pin的HTML10 fingerprint和新owner；後續最終讀回附正確GV10標籤，原證據不覆寫。

**同owner分開計數，沒有堵塞其他來源。** 230.938秒metric窗中，鉅亨新增235個durable版本、222個新URL head計數、31頁已驗清單；Global Voices新增11個durable版本／11個新head計數。原文逐件驗收、batch集合及metric窗的分母不同，不能相加成總篇數。鉅亨仍有5次傳輸錯誤，既有隔離／重試繼續，未宣稱零故障或理論上限。20:06有限metadata phase為7,002／11,003頁（63.64%），剩4,001頁；不是鉅亨全部正文進度或全史ETA。

| 背景來源 | 20:06可驗證狀態／下一步 |
| --- | --- |
| CMoney | 前owner的native403及原期限保留，新owner尚未到20:10:30重試，不能把本metric窗零請求說成已恢復；不換host、清冷卻或推定速率上限。 |
| UDN | 169,684個awaiting_authorization；正式匯出scope／schema仍缺，metadata不當全文。 |
| Global Voices | 上述Access Denied缺口真修復；48,378個complete，62,208個pending，1個inflight；官方robots的10秒間隔不改，並非全站完成。 |
| 中時／工商 | 108,149個awaiting_authorization；官方爬蟲／用途限制及候選保留。 |
| MoneyLink | 103,211個awaiting_authorization；本輪補查登入之外的公共頁footer，來源明列編輯內容權利與重製限制，不再只依會員產品條款。 |
| CMNews | 64,267個complete；20:00:52首頁成功、原1h next due為21:00:52。本metric窗零請求／新版本符合正常排程，不強制重抓；607個member項目另列。 |
| MoneyDJ | 48,106個awaiting_authorization；正式自動取得scope／端點仍待確認。 |
| LTN | 30,559個awaiting_authorization；原官方RSS摘要／連結不冒充HTML全文或歷史授權。 |

MoneyLink的[公共資料聲明](https://tweb.money-link.com.tw/policy/declaration.aspx)與編輯內容footer，是本次新增primary證據；金融資料產品的書面允許條款不直接等同全部新聞契約。未找到授權feed不代表不存在，不能憑排名推定免費、配額、歷史範圍或本帳號已授權。只更新planning review為`permission_required`，面板已反映19:38:15檢查時間，沒有啟用來源、新聞HTTP、修改queue／金鑰或為review重啟collector。

**12／12有限handoff通過。** 最終673項聚焦、144檔／8,535項完整news＋public回歸通過（196.12秒），另195項current planning-review／面板測試通過；不是全repo、GPU或新UI／browser／外網HTTPS驗收。唯讀gateway核對934鍵、fresh／consistent、無正文輸出、page-view零來源HTTP。原native metadata取證drained→fresh48.011秒，HTML9與10各次測試後有序載入分別18.327及14.298秒，成本分開記錄；同一stop／start handle等待期間未重複重啟，拒絕deadline及reserved API／RSS工作精確保留。七個TDD／工具／回歸／v9原重試失敗run與log不改寫，追加真正source成功後的工程處置。沿用共用owner、保留原值／時鐘、按實頁修共享guard與必要分項驗收，是工作偏好技能對本輪的影響。全新聞目標保持active，八家／全部歷史仍未完成。

證據：[真標題／作者／時鐘metadata](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v144/native_metadata_probe.json)、[七個原件的留言驗證範圍](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v144/original_public_gate_shapes.json)、[12項原文／九來源／面板驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v144/handoff.json)、[HTML10載入與原208保護](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v144/restored_v3.json)、[最終144檔generation](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v144/implemented_v3.json)、[完整回歸run收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v144/regression_run_v3.json)、[公共MoneyLink review驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v144/review_verified.json)、[七個失敗run後續處置](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v144/failure_dispositions.json)。

**20:11後續讀回，不覆寫20:06快照。** 對同一份已落盤raw重建v9的全頁sitekey guard，仍得到`access_challenge`；現行v10對同raw、同237字body SHA得到complete，真頁只有一個正文外留言hCaptcha。這直接隔離判斷邏輯差異，不把沒有保存的v9失敗回應說成同一份bytes。新收據附正確GV10欄位名稱，原handoff標籤及失敗證據保留。同owner當時已累計鉅亨564／GV38個durable版本，不能取代前述230.938秒固定窗。CMoney在20:10:31原到期請求仍403，保留新的20:25:31退避期限；没有宣稱該來源已修好、換路或變更actor身分。證據：[同native原件舊／新guard、GV10及當前owner](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v144/completion_current.json)。

2026-10-13 **前次工程摘要（v143；19:00原件驗收）**：**鉅亨仍最高優先**，背景沿 **CMoney → UDN → Global Voices → 中時／工商 → MoneyLink → CMNews → MoneyDJ → LTN** 處理；可取得來源獨立並行。**前次七篇Global Voices空標題缺口已全部真正下載並逐篇原件重播通過**，但八家／全部歷史未完成，原934鍵清冊、工作順序、授權與CNA暫緩不變。

**從實頁定位根因，不以fixture代替來源。** 一次canonical metadata-only取證確認：舊模板的標題及作者credit在`.post-container`／numeric post之外，正文、footer作者與當篇CC則在numeric post裡。原untitled guard誤要求header在post內。現在只在canonical、numeric URL／post-id、單一`#page-content`、同一外部header、`single-post`／matching body postid、單一完整entry、同post內footer／license、相符公開作者及來源aware clock全部成立時接受。來源標題仍是空值，保留明確`title_basis`，不從URL猜標題、作者或發布日期。metadata診斷有界且不保存正文，不跟作者profile、不寫queue；登入、付費、403／429和未知錯誤不取得探測授權。

**真正來源資料與版本邊界。** 僅兩個production modules改動，Global Voices HTML capture版本7→8；其他capture、所有RSS、授權、來源端點與頻率、工作順序精確不變，沒有重抓健康RSS或改寫舊capture。原7網址於18:57:47–18:58:46全部取得完整正文；raw response、正文SHA、作者、原始aware發布時間、當篇CC、空標題basis及實際DOM範圍逐件重播相符。全部201個前次正式原件／head／capture clock亦精確保留，最新八個提交batch的原件SHA與耐久收據另通過，不能把這些選中原件說成全歷史驗收。

**同owner真進度，分母不混用。** 258.670秒metric窗中，鉅亨新增243個durable版本、221個新URL head計數、36頁已驗清單；GV新增9個版本、8個新URL head計數。七原件驗收、八batch及metric是不同選擇邊界，不能相加成新文章總數；鉅亨仍有6次傳輸錯誤。19:00有限metadata phase為6,383／11,003頁（58.01%），剩4,620頁，不是全鉅亨正文進度或全史ETA。

| 背景來源 | 19:00狀態／下一個問題 |
| --- | --- |
| CMoney | 一次正常到期native請求仍403，保留錯誤、標頭及19:10:25期限，不換路或推定速率上限。 |
| UDN | 169,669個awaiting_authorization，正式匯出scope／schema未取得；不把metadata當全文。 |
| Global Voices | 原7空標題已修好；另一個含「Access denied」的網址觸發獨立`access_challenge`，19:14:17正常到期，是否標題誤判仍待實頁證據，不憑slug宣稱已修復。 |
| 中時／工商 | 108,141個awaiting_authorization，原官方用途限制及候選保留。 |
| MoneyLink | 103,195個awaiting_authorization，原創／轉載及匿名全文用途尚未確認。 |
| CMNews | 原1h首頁排程保持，19:00:50正常到期；本metric窗零新版本，不因此認定停工或強制重抓。 |
| MoneyDJ | 48,103個awaiting_authorization，正式授權缺口保留。 |
| LTN | 30,553個awaiting_authorization，RSS摘要／連結不當全文。 |

**完整工程驗證與恢復成本。** metadata工具406項、source-focused 594項、142檔／8,492項news＋public回歸全部通過（231.79秒），另35項共用browser-component語義測試通過；不是全repo或GPU測試。五個工具／TDD／回歸失敗run與log保留並追加後續驗證，不改寫成成功。兩個全回歸失敗來自共用public test對既有使用者chart cache與函式簽名的過期斷言，只修兩個測試斷言，原renderer／UI未改。metadata取證與測試版本載入各只做一次有序planned pause，drained→fresh分別95.740及62.745秒；成本分開記錄，不冒稱網路加速、cold boot或「理論上限」。新owner/PID2373093實際載入version8、NRestarts0，舊原件、reserved API／RSS jobs及拒絕deadline保留。

**10／10有限handoff通過。** 當前唯讀gateway顯示完整934鍵、fresh／consistent、不輸出正文、page-view零來源HTTP；沒有新browser／外網HTTPS／UI重設計驗收宣告。沿用共用owner、保留原值／時鐘、依真模板修共享guard且不複製健康RSS，是工作偏好技能對此輪的影響。全新聞目標保持active；鉅亨實際阻塞先切回，其餘依法源、到期時點及背景順序繼續。證據：[實頁header／post／entry結構](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v143/identity_metadata_probe.json)、[原七篇及全部來源10項驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v143/handoff.json)、[測試版本載入與原件保護](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v143/restored.json)、[最終142檔generation](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v143/implemented_v3.json)、[完整回歸run收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v143/regression_run_v3.json)、[保留使用者renderer的兩處測試修正](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v143/shared_test_repair_v3.json)。

2026-10-13 **前次工程摘要（v141／v142；18:12驗收）**：**鉅亨仍最高優先**，背景沿 **CMoney → UDN → Global Voices → 中時／工商 → MoneyLink → CMNews → MoneyDJ → LTN** 分工處理。可取得的來源獨立並行；工程順序不授予來源權限。原934鍵清冊、全部已知URL、工作順序與CNA暫緩保持，鉅亨真正阻塞才切回。**本輪是診斷工具、錯誤分類與受保護的局部parser修正；不是七篇GV原頁或全部來源已修復。**

**直接找到了誤報原因，但未跳過原件驗收。** v141沿原canonical root／exclusive HTTP lock有序取證一次，原URL `https://globalvoices.org/2010/09/24/164959/` 返回200；公開header／footer有同一作者，而設定的標題元素為空。原parser把「要求的標題缺失」與「作者缺失」合併成 `article_attribution_not_verified`，因此這一筆不是已證明缺少作者。診斷工具新增明選作者失敗的唯讀前置檢查：只准現存、無已保存正文、無lease、已到期、aware且非未來觀測的失敗head；403／429／登入／付費、已完成與未知failure code均不能取得重試授權。作者診斷只留有界結構／公開署名／hash，不留正文、HTML、不跟profile、不改queue。351項相關測試通過；原7個失敗head保持，201個前次正式原件重播及capture／作者／發布時鐘未改，v141的9／9有限驗收通過。

**拆開標題與作者狀態，網頁分類已載入。** v142新增 `article_title_not_verified` 及「標題未驗證」UI；不再把缺標題視為作者／授權問題。局部untitled HTML adapter只嘗試驗證numeric URL／post-id、canonical、單一完整entry、來源aware clock、相符header-footer作者及當篇CC；標題保留空值與明確basis，不以URL、摘要或metadata編造標題。僅Global Voices的HTML capture版本6→7；原66來源授權、其他capture、全部RSS指紋、API／feed端點及頻率精確不變，不重抓健康RSS。原201正文與歷史capture指紋不重寫，publisher拒絕及重試期限不清除。

**實頁結果仍有缺口，沒有用測試代替source接受。** 原7篇由版本化reconciliation重新排入，18:12讀回為**6篇 `article_title_not_verified`、1篇待重試，0篇新正文接受**。這證明新的身分／時鐘／DOM約束仍未與該批真實模板吻合；不是來源完全錯誤或七篇無作者的結論。下一步要先取得canonical、出版時鐘和header／post／entry的真實結構證據，辨認失敗的具體約束，再修正；不能憑synthetic fixture放寬作者、clock、授權或body完整性。本輪9／9工程handoff明列 `all_seven_native_untitled_articles_repaired=false`、`all_eight_sources_repaired=false`，全新聞目標仍active。

| 來源／優先序 | 18:12實際狀況與剩餘工作 |
| --- | --- |
| 鉅亨（最高） | 同owner139.177秒metric窗新增189個durable版本、172個新URL head計數、21頁已驗清單；6次傳輸錯誤仍保留，正常管線續抓，不稱零故障或全史完成。 |
| CMoney | 新owner一次正常到期native請求仍403，原access metadata保留；next attempt為18:25:24，不清除期限、換路或把403推定為速率上限。 |
| UDN | 169,669個awaiting_authorization；已整理正式歷史匯出途徑，仍缺本帳號scope／schema，未啟動未授權全文。 |
| Global Voices | 同窗新增11個durable版本／11個新head；正常原文／已註冊RSS繼續。原7篇無標題缺口如上，不能宣稱已恢復全文。 |
| 中時／工商 | 108,141個awaiting_authorization，原官方用途限制與全部candidate保留。 |
| MoneyLink | 103,195個awaiting_authorization；匿名原文／原創範圍尚待精確確認，不從會員試用或其他產品API推定權利。 |
| CMNews | 18:00:50首頁正常成功，新owner已有3個durable新head計數；next due為19:00:50，1h追新修復延續。此計數不是本輪逐篇正文重播樣本或全站完成。 |
| MoneyDJ | 48,103個awaiting_authorization，正式授權缺口與URL完整保留。 |
| LTN | 30,553個awaiting_authorization；已查RSS摘要／連結不當全文，原正文權限缺口保留。 |

**測試、部署與資料證據分開。** 481聚焦測試、140檔／8,445項news＋public-dashboard回歸通過（202.96秒，不是全repo／GPU測試）；原false-author TDD失敗和本地helper／fixture失敗log保留。原新聞service／timer單次有序載入測試版本，正常exit0；新owner `976a3551b05a448caf9715a7e93c31db`、PID2212903、NRestarts0，實測drained到fresh heartbeat為93.587秒，不是故障或cold boot證明。此前v141單次metadata取證有自己87.126秒的正常恢復成本，不能藏入吞吐提升。唯讀gateway另一次載入新分類，沒有因此重啟collector或交易服務；934鍵、fresh／consistent、靜態JS SHA精確吻合、不輸出原文、page view零publisher calls。全部201原正式heads再次重播保留；最新8個已提交batch的raw／SHA／耐久收據另驗通過，與139.177秒metric窗不是同一分母。

18:12鉅亨的有限metadata phase為**5,918／11,003頁（53.79%），剩5,085頁**；這不是全鉅亨正文比例或全史ETA。沿用共用owner、分開可觀測狀態與資料接受、保留原值及已證明的舊capture，是工作偏好技能對本輪的影響。證據：[v141原頁metadata與公開署名](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v141/author_metadata_probe.json)、[v141取證／原件／復工9項驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v141/handoff.json)、[v142實頁仍拒絕及全來源工程handoff](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v142/handoff.json)、[HTML-only generation與完整測試範圍](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v142/implemented.json)、[140檔回歸收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v142/regression_run.json)、[同collector的新面板分類](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v142/gateway_restored.json)。未宣稱八家均啟用、未解限制已根治或取得理論速度上限。

2026-10-13 **前次工程摘要（v140；17:13驗收）**：維持**鉅亨優先**；等待時沿 **CMoney → UDN → Global Voices → 中時／工商 → MoneyLink → CMNews → MoneyDJ → LTN** 處理問題。已啟用來源各自並行，不讓拒絕來源阻塞健康來源；只有鉅亨有可驗證的操作阻塞才切回。原934鍵清冊、工作順序、全部已知URL、授權與CNA暫緩都保持。

**根治CMNews追新首頁被排成隔天的問題。** 原設定`discovery_interval_seconds=3600`，成功首頁工作的原`last_checked`到`due`卻相隔86,400秒；共用success路徑把全部HTML index硬編碼成24小時。不能全改為每小時：另一個啟用的連續HTML index來源Fed同時有近期與歷史archive。新增有界、明確的`incremental_index_pages`，只宣告已註冊的CMNews首頁；歷史索引不加速輪詢，feed、RSS歷史、bulk歷史與拒絕退避不改。排程ABI／SHA獨立於正文、授權和discovery parser，全部capture／authorization指紋精確不變，不因新欄位重抓幾百萬舊正文。

**舊成功工作的校正有原時鐘與耐久收據。** canonical queue owner用主鍵查明選URL，只校正「同parser、成功、零attempt、aware檢查時間、due確為舊24h公式」的工作：新due是**原last_checked＋1h**，不是現在、`due=0`或偽造觀測。校正與一次性schedule收據在同一交易；etag、last-modified、原response SHA、原檢查時間不改。未知、naive／未來時鐘、重試、403／429、停用／有限來源及非舊公式deadline均不提前。原設定更新由既有watcher在16:58:38有序返回75，systemd16:58:48自動重啟原canonical服務；17:00:47原owner完成一次校正。保留實際journal，沒有另做手動重啟、雙HTTP owner或重設來源拒絕期限；不是OOM／cold boot驗收。

**真正首頁及正文已取得，不是僅重排。** 17:00:49首頁HTTP200，隨後12個文章HTTP200中，**7個新URL的完整正文已落盤並重播原parser通過**，作者、發布／修改原時鐘、raw SHA和耐久batch收據一致；另5個仍依原`paywall_or_login`guard排除，不能把13個HTTP200都算成已取得全文。成功首頁next due為18:00:49，與原檢查相差3,599.999928秒。這證明本輪追新修復，不宣稱全站歷史或付費資料已抓齊，也沒有繞CMoney論壇403。

| 來源／優先序 | 本輪實際狀況 |
| --- | --- |
| 鉅亨（最高） | 同一新owner121.767秒metric窗新增172個durable版本、163個新URL head計數、21頁已驗清單；保留3次傳輸錯誤，不稱零故障或所有正文完成。 |
| CMoney | 17:10:20原到期重試仍403，新deadline17:25:20；標頭／錯誤與待辦保留，未提早重試、換host、讀拒絕正文或從403推論速率上限。 |
| UDN | 169,649個已知URL仍awaiting_authorization；正式匯出／分析途徑已有review，但本帳號scope／schema未提供，正文不虛列為已啟動。 |
| Global Voices | 同窗新增11個durable版本、10個新URL head計數，native／已註冊RSS繼續；WP作者嵌入權限及個別`article_attribution_not_verified`缺口未冒稱修好。 |
| 中時／工商 | 108,133個awaiting_authorization，原官方用途限制與candidate保留。 |
| MoneyLink | 103,162個awaiting_authorization，匿名公開全文／原創範圍仍待精確確認，不從會員合約或免費試用推定歷史API權限。 |
| CMNews | 首頁24h→明選1h修復，新增7個已驗完整原件；同一觀察窗沒有再輪詢首頁，符合新next due，不為製造進度重抓。 |
| MoneyDJ | 48,100個awaiting_authorization，已知URL及授權缺口保留。 |
| LTN | 30,550個awaiting_authorization，原RSS只有標題／摘要等metadata，不當全文或訓練語料。 |

**驗證完整且分母不混用。** 219項聚焦測試通過；138檔／**8,265項新聞回歸**全部通過（211.80秒，不是全repo test suite）。原24h錯誤的200／304兩個失敗重現log保留，後續成功驗收追加。全部194個前次選中heads精確保留並重播，原八batch再次核對；另選最新64個已提交batch，1,905個版本的raw／SHA／耐久收據驗證通過（鉅亨1,740／GV158／CMNews7），其中7個CMNews原文另通過完整parser replay。64批與121.767秒metric窗不同，不能把1,905當作該窗新文章數；不是全部歷史的抽樣外推。

**11／11有限handoff通過，面板狀態已連動。** 唯讀gateway核對934鍵、fresh worker、consistent snapshot、沒有正文輸出或page-view publisher請求；沒有新增UI、browser或外網HTTPS驗收宣告。17:13鉅亨有限metadata phase為**5,390／11,003頁（48.99%）、剩5,613頁**，不是全鉅亨正文或全史比例／ETA。本輪新owner的`status_write`最大19.660秒已如實留在telemetry；沒有把前次status重播效益當成這次網路加速或偶發停頓已根治。沿用共用owner、只修明確因果錯誤、保留原值及完整證據，是工作偏好技能對本輪的影響。證據：[11／11來源／排程／面板驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v140/handoff.json)、[原24h工作與完整scope](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v140/before.json)、[194原head及7新CMNews原件](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v140/body_readback.json)、[完整138檔新聞回歸](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v140/regression_run.json)。全新聞目標仍active，授權、CMoney403、鉅亨variant身分及GV作者缺口分開保留，未宣稱八家皆已修復或理論上限已達。

2026-10-13 **前次工程摘要（v139；16:38驗收）**：維持**鉅亨優先**，等待時按 **CMoney → UDN → Global Voices → 中時／工商 → MoneyLink → CMNews → MoneyDJ → LTN** 處理。可取得的來源各自排程並行；鉅亨真正阻塞時先切回。原934鍵清冊、已知URL、權限及CNA暫緩保留，不把工程排序當成授權或八家都已修復。

**已部署共用面板計算優化，沒有改來源頻率或正文。** 原owner曾有78.25秒的`status_write`，但仍提交正文並自行恢復，不能從舊心跳推定程序死亡。查出每次狀態投影都掃描1,292,372筆`bulk_item_errors`的多餘工作，改為同一來源交易內INSERT／DELETE trigger維護精確計數，僅初始化重建一次；新增來源清冊、清單進度、診斷計數、容量及JSON發布的分項wall／CPU計時。status投影版本3、計數版本1，沒有刪除診斷或重設來源deadline。

**完整投影重播wall／CPU約減27%，不是WAN加速或78秒因果證明。** 真實live計數與同一唯讀WAL上的`COUNT(*)`精確相等；五次平衡順序的整個status建構及JSON序列化，legacy wall中位1.079秒→0.785秒，CPU1.076秒→0.785秒，輸出bytes及SHA完全相同。範圍不包含live metric lock、檔案發布或來源下載；不宣稱網路吞吐提高27%或已定位舊78秒的全部原因。新owner截至16:38記錄133次`status_write`、最大2.498秒，仍只是這個owner的觀察。舊cgroup記錄swap／MemoryHigh壓力，不足以定位舊卡頓責任點，未因此擴張資源或操作GPU／交易服務。

**一次有序新聞重載及真正正文證據。** 66項聚焦、136檔／8,218項新聞回歸通過後，僅重載canonical新聞服務一次；全部120個既有head、作者／來源時鐘／原件、四個停用WP工作及原退避期限保留，drained到新的running心跳100.408秒如實記錄。新owner152.114秒固定metric窗，鉅亨新增134個durable版本／121個新URL head計數／18頁已驗清單，GV新增6個版本；CMoney有1次原到期403，CMNews該窗零新版本，不能把不同分母相加成篇數或宣稱其他來源停工。

**驗收發現並修好RSS讀回格式錯誤，沒有放寬授權。** 選中的八batch／194個版本，其原件SHA、耐久收據與當前heads原先全部通過，但canonical讀回器把一筆GV的`rss_content_encoded_v1`原始XML送入HTML parser，誤報`article_license_not_verified`。首次失敗handoff與單測重現保留；修共用唯讀audit版本8，以已註冊RSS parser重播同一原件、同一shared envelope只解析一次，仍拒絕mixed restricted／foreign envelope、身分歧義、摘要替代正文及作者／發布／修改時鐘／授權範圍變更。314項相關回歸通過，沒有額外新聞HTTP、改來源資料或第二次重啟。後續**194／194全部重播通過**，含鉅亨185、GV9；不把這批樣本說成全歷史完成。

**故障恢復與尚未修復分開。** 抽查的GV3筆傳輸故障都已核對嚴格後續capture及正文／作者／時鐘／SHA／收據；鉅亨10筆中9筆真正恢復。剩下`522903?exp=a`從傳輸逾時轉為`article_identity_not_verified`，16:33正常重試後進入原bounded-burst冷卻；不帶參數的primary早已有2026-10-08 capture，但不能複製成此variant的新觀測或後續恢復，guard不放寬。目前沒有這次identity失敗原回應的可用證據，根因尚未定位；單篇缺口沒有停掉主下載器。

| 背景來源 | 本次核對的操作與剩餘問題 |
| --- | --- |
| CMoney | 原到期重試持續403，保留標頭、錯誤及deadline，未讀拒絕正文；Owl RSS metadata不是已驗全文或本帳號權限。 |
| UDN | 正式歷史匯出／資料分析途徑已清點；實際授權scope及schema尚缺，正文未啟用。 |
| Global Voices | native／RSS正常入庫；本次3筆故障真正補回。WP作者嵌入`rest_cannot_access`仍未解，WP停用工作原值保留。 |
| 中時／工商 | 官方授權限制仍在；原candidate、URL及review保留，不因優先順序啟用正文。 |
| MoneyLink | 精確公開原文／feed用途scope尚待確認，不能從會員合約推定匿名正文或歷史API的權限。 |
| CMNews | 既有collector／排程保留；本次固定metric窗零新版本，不宣稱全站歷史或付費項目完成。 |
| MoneyDJ | 既有授權缺口仍在，停用候選及全部已知URL保留。 |
| LTN | 已實測的RSS僅標題／摘要／連結／pubDate，非全文；正文仍未啟用，不新增重複metadata管線。 |

**12／12有限handoff通過，面板已讀到當前狀態。** 唯讀gateway重新核對934鍵、fresh worker、consistent snapshot、不輸出私有正文及page-view零publisher請求，沒有新UI、browser或外網HTTPS驗收宣告。16:38的有限metadata phase為**5,118／11,003頁，剩5,885頁（46.51%）**，這不是全鉅亨正文比例或全史ETA。沿用共用管線、保留來源原值與可重播證據，是工作偏好技能對本輪修改的影響。證據：[12／12追加驗收及全部194原件](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v139/handoff_forward.json)、[真實live計數與完整status精確parity](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v139/status_live_parity.json)、[一次有序重載及全部120原head](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v139/restored.json)、[原失敗驗收，保留不覆寫](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v139/handoff.json)。全部來源／歷史目標仍active；CMoney403、variant身分與授權缺口未冒稱已修。

2026-10-13 **前次工程摘要（v138；15:17–15:31背景拒絕層與官方替代途徑）**：鉅亨優先與八家背景順序保持，已啟用來源獨立繼續。本輪沒有重啟、額外新聞HTTP、queue寫入或重設deadline；只更新CMoney／UDN的planning review、相稱測試及完整934鍵工作表，保留原12:19:02全清冊snapshot，不把來源聲明換成本機取得。

**CMoney原拒絕不是selector錯誤，也沒有證據可當限速。** 原owner保留的前三筆14:40／14:55／15:10native403，標頭為`Microsoft-IIS/8.5`、`via:1.1 kong/3.9.0`，未讀拒絕正文。15:25:10正常原到期重試仍403，原件與新的15:40:10期限核對保留，沒有重排提早打。這些只能證明該路徑被拒，不能指認特定WAF、限速或Owl token過期；也未證明所有CMoney產品不可取得。

**補齊兩個獨立Owl新聞端點的清冊，但不冒稱全文API。** 新查[官方23頁規格](https://www.cmoney.com.tw/owl%E6%95%B8%E6%93%9A%E8%B2%93%E9%A0%AD%E9%B7%B9/%E6%95%B8%E6%93%9A%E8%B2%93%E9%A0%AD%E9%B7%B9API%E8%A6%8F%E6%A0%BC.pdf)，第19／21頁列出帶產品、則數、可選股票代碼的兩種`/OwlApi/api/rss/`，需AppID／AppSecret換Bearer token。宣告的`Title`／`Data`是編號、標題、摘要、連結、建立時間、類別，含UDN原始連結，沒有可驗證全文／作者欄。舊2017／2018樣例和成功範例，不是本帳號權限、當前端點健康、最大則數／歷史／每小時quota；沒有使用公開樣例密鑰、呼叫auth或繞論壇403。與原CMService五個metadata方法分開列於planning工作表，不是啟用API，也不新增已重複取得的週期摘要管線。

**UDN找到正式歷史匯出途徑，不再停在舊404。** 沿現行[授權頁](https://udndata.com/authority/)頁尾，讀到[資料分析授權說明](https://udndata.com/faq/authority.html)及[原始資料庫授權匯出／回溯授權](https://udndata.com/faq/marketing.html)。閱讀會員／IP定址權限與大量下載、語料分析、第三方文章權利分開；資料分析申請的表格在web reader失敗，不當作來源404或通路不存在。仍需實際授權scope、export／feed schema及配額才能接既有collector，沒有登入、購買、提交申請或抓原始新聞。

[全文庫官方FAQ](https://www.udndata.com/faq/fulltext.html)另外補上歷史與時程：聯合報1951-09-16起、經濟日報1967-04-20起；聯晚1988-02-22–2020-06-01、Upaper2007-03-26–2018-04-26、民生1978-02-18–2006-11-30、星報1999-09-01–2006-10-31。歐洲／世界日報是部分收錄；1999-09-01之前回溯只有全國／台北版，不能說全地方版完整。新聞約04:30、報版07:00是入庫時程，不是每篇精確發布或歷史PIT；400／5,000年閱讀則數也不是API calls/hour、本帳號或免費全庫配額。沒有將這些聲明填成已下載首末日期、筆數或訓練可用資料。

**有實際下載進展與原件驗收。** 同owner約185.97秒metric窗，鉅亨增加54個durable版本／52個新URL head計數／56頁清單，Global Voices增加16個版本；保留鉅亨3次傳輸錯誤，不能稱零故障。另選最新八個已提交batch，全部120個正文版本／當前heads核對原件、SHA、耐久收據、作者與來源時鐘，含鉅亨74／GV21／CMNews25。兩種窗口不同，不能將樣本120稱為該窗新文章數。15:31有限metadata phase4,199／11,003頁（38.16%）、剩6,804頁，仍不是全鉅亨正文完成。

**173項相關測試、9／9有限handoff通過。** 正式capture／authorization／code／config generation、PID／timer與工程順序都保持；唯讀gateway已讀到兩個新review日期，不傳私有診斷文本或在page view打publisher，沒有宣稱新UI／browser／外網HTTPS驗收。沿用共用管線、保留原值與整鏈證據，是本輪工作偏好技能的作用。證據：[9／9驗收及全部120筆原件](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v138/handoff.json)、[原到期403與同owner進展](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v138/after.json)、[完整934鍵工作表](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v138/work_order.csv)。下一步沿中時／工商、MoneyLink等既有背景缺口繼續；鉅亨真阻塞先切回，CNA仍暫緩，全新聞目標active，並未把權限或尚未取得當作修復完成。

2026-10-13 **前次工程摘要（v137追加；15:10原件證明背景網路自行恢復）**：鉅亨仍優先；等待時沿 **CMoney → UDN → Global Voices → 中時／工商 → MoneyLink → CMNews → MoneyDJ → LTN** 處理，鉅亨真正阻塞先切回。背景啟用來源仍各自排程，不讓其中一家故障停住其餘來源；工程注意力排序不等於自動取得權限或八家皆已修復。

**Global Voices不是只有HTTP恢復，而是先前失敗文章真正補回。** 原正常owner後來持續成功，15:10:13獨立驗收 **8／8通過**：保留的37個傳輸事件中，33個是文章事件、涉及24個不同URL；**24／24都有嚴格晚於該URL最後一次故障的完整capture**，並再核對原始回應、正文／檔案SHA、耐久收據、作者與來源發布／修改時鐘。另選15:00–15:03最新八個已提交batch，**298個正文版本／當前heads**全部通過canonical收據驗證，包含鉅亨281與Global Voices17；17個GV正文也全部重播原parser。33事件、24網址、17版本是不同分母，不相加當新增篇數。

**恢復與剩餘缺口分開。** 沒有第二次重啟、額外新聞請求、DNS／hosts／代理修改、權限或頻率放寬；原transport circuit／到期重試延續，source／authorization／code／config世代與PID都未變。14:58「尚未恢復」收據原值保留，追加真正後續證據，不改寫歷史。這證明這批native連線及文章已恢復，仍不能定位先前網路故障的責任點，不能說全GV歷史完成。**WP批次100筆嵌入作者的`rest_cannot_access`仍在，入口仍停用**；native正文恢復不是修好API權限。CMoney15:10正常到期仍403，新owner累計三次，下一次15:25:08；不清除拒絕或強制提前重試。其餘正文授權／coverage缺口及CNA暫緩保持。

鉅亨15:10:12正常owner已保存1,435個版本，新URL head計數1,221，成功清單314頁；這些是新owner累計，不和舊owner相減，也不是皆首次全文。有限metadata phase為3,491／11,003頁（31.73%），剩7,512頁；不是全站或正文完成比例。此刻有新的有效速率窗口，metadata基準約4.78小時、台北19:56:54，最短約2.09小時、較慢62.6小時，均是近期速率情境而非保證或信賴區間；沒有把它冒稱全鉅亨抓完時間。

證據：[追加8／8恢復驗收、24失敗URL的完整原件](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v137/forward_recovery.json)、[最新八batch全部298版本／heads驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v137/recovered_recent_batches.json)、[15:15追加8／8整合handoff與934來源工作表](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v137/forward_handoff.json)。追加review後重跑同一套285項聚焦測試，9.81秒全部通過；最初組裝命令誤列不存在的test路徑，exit4、沒有執行測試或來源工作，原log保留，依原成功收據的明確清單修正後同名重跑通過。唯讀面板已讀到最新review日期，沒有外傳私人診斷物件或page-view publisher呼叫。追加正文讀回不更動原下載器ABI或把未啟用來源列成完成。全新聞目標與原工程任務繼續active。

2026-10-13 **前次工程摘要（v137；14:32–14:58批次作者與網路根因診斷）**：**鉅亨仍優先；等待時沿 CMoney → UDN → Global Voices → 中時／工商 → MoneyLink → CMNews → MoneyDJ → LTN 交錯處理，鉅亨真正阻塞先切回**。工程注意力順序與下載端獨立配額／冷卻是兩件事，不把八家改成串行，也不宣稱它們已全部能抓。這次只對新聞 owner 做一次有序診斷／恢復；交易、其他下載器和金鑰未操作，CNA仍暫緩，全新聞目標 active。

**Global Voices批次缺作者的真正機制已實測。** 沿原canonical root／exclusive HTTP lock／原客戶端及REP，對舊WP page1的`_embed=author`請求只重測一次：HTTP200、100個post、1,831,689 response bytes；來源宣告104,414 posts／1,045頁，只是該端點當下的分母。**100筆作者嵌入全部為`rest_cannot_access`，零可用姓名**；四個有界schema樣本也沒有Yoast姓名或額外credit欄位。不是補一個selector、再加一次embed就能修的問題。沒有請求拒絕的profile、由ID推測名字、保存mixed raw或把post／摘要列成已驗全文；WordPress入口仍停用。English1,127／ZHT624個RSS身分頁及下一頁實際invalid_feed／404工作，連同兩個舊WP job的attempts、due與錯誤全部原值保留，不是全網站最早日期或完整歷史。

**共用診斷器修正，而非另造爬蟲。** [原診斷入口](../scripts/probe_news_sources.py)現在有有界WP public-metadata結構：每頁最多100列、只樣本四個post，保存原宣告與嵌入錯誤，不印正文、私有profile資料或認證欄位、不自動跟隨profile網址。第一列不是object、錯誤embed型別或JSON壞回應，都明確記錄並繼續後面的明選端點；decode錯誤不洩漏拒絕文本。**285項測試通過**，包含privacy、owner、deadline、byline、quota合作、端點清冊與正常下載器回歸。這是診斷健壯性修正，不冒稱提高publisher最高速、修好了來源權限或實際修復所有壞回應。保留原值、共用入口及把工程／來源證據分開，是工作偏好技能對本輪修改的作用。

**恢復要算真成本，也不能只看PID。** 正常drain原owner後取得唯一HTTP鎖；診斷關閉才恢復原service／timer。新owner`0f579d913d194fa4b6ca119c023212ca`／PID1598819，NRestarts0，原12個native正文、作者、發布／修改／capture時鐘、hash／路徑與source／authorization／code generation核對保持。stop intent到新有效running heartbeat **165.86秒**，已確認drained到該心跳 **141.27秒**；不是宣稱幾秒重啟或把停抓算成來源故障。之後才用新owner自己的metric窗對帳，不將reset後的counter減去舊owner值。

**恢復後主動發現第二個、獨立的故障。** Global Voices正常文章／discovery連線持續失敗，不能被API的HTTP200掩蓋。只觀測既有owner的connect／getsockopt **35秒**，不額外打新聞、不記正文或HTTP headers，診斷結束已detach且原collector繼續。kernel原件顯示對`185.88.142.47:443`的`EHOSTUNREACH`；本機DNS與一次[獨立公共DNS診斷](https://developers.google.com/speed/public-dns/docs/doh/json)都回相同IP。這證明當時TCP不可達，不證明是哪一段網路／防火牆或整個publisher宕機；沒有改resolver／hosts／代理或推定權限403。原transport circuit／deadline仍生效，**觀察窗口沒有新的GV正文，網路尚未恢復**。官網[AI政策](https://globalvoices.org/about/global-voices-policy-on-ai/)屬供稿寫作／翻譯／圖像規範，不混成另一項爬取授權或撤銷[獨立CC-BY原創文本範圍](https://globalvoices.org/about/global-voices-attribution-policy/)。

**鉅亨沒有被背景故障拖停。** 14:41:51–14:53:01，同新owner約**669.22秒**的metric窗，鉅亨durable版本增加**436**／新URL head計數增加**384**／清單增加**58頁**；保留14個傳輸錯誤，沒有稱零錯誤。另選最新八個真正提交batch，全部**116個鉅亨正文版本／當前heads**再核对原始回應、正文／檔案SHA、耐久收據、作者與來源時鐘；這個選樣不是同一metric分母，不能稱116篇皆首次下載。14:58獨立核對，有限清單為**3,267／11,003頁**、96,810／326,776筆metadata，活躍缺文**667個URL**。未知原件、純報價、空模板、source404、primary候選及未探索頁仍分開；不是全鉅亨完成。當下清單最後一分鐘無新頁，forecast如實為`no_recent_progress`、沒有有效ETA，不借用較早速率製造完成時點。

**14／14有限handoff、全部934鍵工作表保留。** 完整plan仍沿12:19:02原全庫snapshot，各provider原counts／source role／enabled及上述八家顺序不改，只更新GV實證review；沒有重掃全庫或啟用其餘未授權來源。唯讀gateway已讀到新的review日期與GV冷卻狀態，未傳私人觀察、raw、作者診斷物件或正文，也沒有page-view publisher呼叫；本輪不主張新UI／browser／外網HTTPS驗收。CMoney新owner兩次到期仍403，下一次台北15:10:06，未用作者／網路修復清掉該拒絕。UDN、中時／工商、MoneyLink、MoneyDJ、LTN原正文範圍／授權工作保留；CMNews保持原正常維護，本窗沒有新版本，不能虛報新增。

證據：[原批次100筆嵌入錯誤](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v137/wp_metadata_probe.json)、[有序恢復與原12筆正文保護](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v137/restored.json)、[既有程序的TCP診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v137/connections.json)、[独立DNS對照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v137/dns_evidence.json)、[完整934鍵工作表](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v137/work_order.csv)、[14／14驗收與全部116筆原件讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v137/handoff.json)。下一步先監測鉅亨真落盤與source錯誤，再按八家順序處理；GV網路恢復必須有後續真正原文／收據，不能只靠連線或PID。

2026-10-13 **前次工程摘要（v136；14:10–14:17背景 RSS 範圍實測）**：**鉅亨持續下載；等待時沿 CMoney → UDN → Global Voices → 中時／工商 → MoneyLink → CMNews → MoneyDJ → LTN 交錯修復，焦點真正阻塞時先回鉅亨**。八家的工程清單與 provider 獨立配額／冷卻保持。這次未重啟新聞、交易或其他下載服務，也沒有把來源限制算成修復完成；所有新聞目標仍 active，CNA 暫緩。

**有實際並行進展。** 原 owner／PID1331683 在約213.20秒的 metric 窗，鉅亨保存版本增加355／新增URL head計數增加314／清單驗證增加48頁；Global Voices增加17個正文版本。另選最新八個已提交batch，全部274個正文版本及目前heads都再核對原始回應、正文／檔案SHA、耐久收據、作者與發布／修改／capture時鐘，包含鉅亨261及Global Voices13。兩種選樣窗口不同，不互換分母，也不宣稱全部都是首次下載。14:17驗收讀到鉅亨有限清單3,022／11,003頁、目前活躍缺文2,094個URL；後續探索會增加工作，這不是全網站分母或全歷史完成。

**LTN找到可用RSS，但不是全文替代源。** 從官網頁尾進入[著作權聲明](https://service.ltn.com.tw/copyright)與[官方RSS訂閱說明](https://service.ltn.com.tw/RSS)，後者另列14個RSS網址及非商業、標明出處的使用範圍。沿共用 `NewsHTTP`／robots／rate／circuit、僅對未啟用的獨立host做兩個有限RSS診斷；[財經](https://news.ltn.com.tw/rss/business.xml)與[即時](https://news.ltn.com.tw/rss/all.xml)都回200、各40個item，只有title／description／link／pubDate，沒有content:encoded全文或creator。40+40是回應資料列，不是80篇唯一新聞；沒有把摘要列為全文。真正REP允許原私人RSS客戶端的wildcard範圍，亦另外禁止列名AI／訓練bot；未更換身份或推論訓練權限。共3次來源HTTP含1次robots、兩次RSS，實測最小請求間隔2.0003秒，這不是官方最高速或quota。只保留最小REP快取與結構／SHA，不保存RSS原始全文、HTML文章或拒絕body；沒有新增重複的週期metadata管線。

**MoneyLink分清會員合約與匿名公開新聞。** [官方合約](https://ww2.money-link.com.tw/MemberCenter/MemberTerms.aspx?term=4)的產品／會員重製與保存限制、第三方新聞供應者分開記錄；不能只憑付費會員條款就把所有匿名公開頁套成同一權限，也不能由免費試閱推論歷史全文API。匿名原創新聞的精確範圍仍未確認，既有SN／PU身分與metadata保留，沒有自動啟用、抓HTML或換host。CMoney正式14:08重試仍403，原deadline排到14:23:16；UDN、中時／工商、MoneyDJ授權工作不被重試清除。Global Voices作者guard、未驗證WP入口停用與原robots10秒下限保持；CMNews現有正文維護，所抽到的unsupported樣本是tag／onlynotecontent，未放寬成文章路徑。

**182項相關測試、12／12 handoff通過。** 只修改兩個planning review與測試，正文／授權ABI、正式config、HTTP owner、冷卻與頻率未變。沿用共用管線並保留來源原值，是本輪工作偏好技能的作用。完整934鍵plan保留12:19:02原全清冊的事實與時間，不是重掃後的新全庫統計；首次報表指令用錯inventory檔名，已依原成功收據定位正確檔名重跑，原失敗留存。唯讀gateway已讀到MoneyLink／LTN新review狀態，不傳私人觀察、正文或金鑰、不呼叫publisher；未修改UI或聲稱新browser／外網HTTPS驗收。

證據：[兩個官方RSS結構與真實請求路徑](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v136/ltn-rss-probe.json)、[原owner與背景實際進展](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v136/after.json)、[完整934鍵工作表](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v136/work_order.csv)、[12／12驗收與全部274筆正文原件](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v136/handoff.json)。

2026-10-13 **前次工程摘要（v135；13:54–13:57背景來源介面與原件核對）**：沿用上一輪的選定工作順序：**鉅亨仍優先；等待時交錯處理 CMoney → UDN → Global Voices → 中時／工商 → MoneyLink → CMNews → MoneyDJ → LTN，真正阻塞則先切回鉅亨**。這是工程注意力順序；原下載端的provider獨立配額與冷卻繼續，不是讓八家排成串行，也沒有啟用未授權來源。

**這次補上可驗證的介面邊界，而非提高未證明的流量上限。** 實際讀完整[CMoney官方CMService清單](https://www.cmoney.tw/cm-service/CMService.asmx)及五個新聞方法描述：清單是**30**個操作，不是31，WSDL連結不能算作一個操作。三個新聞方法的Article／StockRssArticlePkgObject與另外兩個StockRssSimpleInfo保留不同欄位名稱；五者都未列全文欄位，兩者有commkey／memberPk參數。沒有參數不等於匿名可用，官方範例HTTP200也不是實際API成功；批次筆數上限、配額、最早歷史都保持未知。帳號、訂閱、部位與點擊計數方法沒有呼叫。WSDL在web reader的失敗不冒稱來源403或API消失；目前真正文章403仍由canonical owner持有，13:53重試後排到台北**14:08:13**，本輪沒有清除拒絕或deadline。

**Global Voices仍以可驗證的原文／RSS前進。** 只讀queue核對English1,127頁、ZHT624頁的RSS身分checkpoint；下一頁各保留實際invalid_feed／404，不將分頁尾端當成全網站最早歷史。舊WP page1／2在2026/10/8各有100筆缺作者錯誤，且請求已包含`_embed=author`；本輪沒有找到可重讀的該舊原始回應，不能宣稱重新實測API或從ID推測姓名。通用WordPress100筆分頁文件不證明Global Voices byline可用。維持停用該批次入口、正文作者guard與原robots10秒下限，後續需取得實際原件才能修parser；CC-BY原創文本及第三方媒體權利仍分開。

| 背景來源 | 現在的處理分界 |
| --- | --- |
| CMoney | 原到期重試仍403；五個官方新聞方法只確認metadata schema，不冒稱全文替代源 |
| UDN | 既有官方條款的授權／實際產品API工作保留；沒有啟用未確認正文範圍 |
| Global Voices | 原文持續補；RSS邊界與舊WP作者缺口分列，沒有以摘要或推測署名補齊 |
| 中時／工商 | 上輪已核官方未授權爬蟲限制；保留原清單與候選，不用另一集團host迴避 |
| MoneyLink | 公開用途與原媒體／第三方權利仍待核對，不能判定所有新聞均不存在 |
| CMNews | 既有64,228個complete URL heads維護；本次metric窗沒有新請求或版本，不冒稱新抓進展 |
| MoneyDJ | 官方網站自動取得／AI用途的授權工作保留，不能用重試消除來源限制 |
| LTN | 精確私人取得範圍、原媒體與namespace仍待review；沒有從頁尾推論批次全文權限 |

**實際並行與耐久原件驗收。** 同一正式owner／PID1331683、零restart，在13:54:09–13:55:57的107.88秒metric窗，鉅亨durable版本增227／新增URL head計數增220／清單驗證增34頁，Global Voices版本增12；CMoney冷卻沒有拖住它們。另獨立讀回最新八個batch的全部448個正文版本／當前head，原始回應、body／file SHA、收據及逐篇parser的作者、發布／修改／capture時鐘核對通過，其中鉅亨424、Global Voices24。這個batch選樣與metric窗不同，不能互換分母；未宣稱448篇皆首次下載或全歷史完成。13:57:43核對鉅亨有限清單2,819／11,003頁，活躍缺文2,702個URL；探索會繼續新增工作，不以此當全鉅亨分母。

**168項相關測試與10／10handoff通過。** 只更新兩個planning review及對應測試，沒有修改生產collector、source／正文／授權ABI、HTTP頻率或重啟服務。完整934鍵工作清單保留原12:19:02全清冊的每個來源事實與日期；不是再全掃或現在的全庫統計。正式唯讀gateway已讀到CMoney／Global Voices新review狀態，沒有傳新聞正文、金鑰、私人觀察文字或呼叫publisher。本輪未修改UI，也未宣告新的browser／外網HTTPS驗收。共用架構、保留原值與把規劃／實測／正文驗收分開，是工作偏好對本輪修改的影響；原所有來源目標仍active，CNA暫緩。

證據：[完整工作清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v135/work_order.csv)、[官方30操作／5新聞schema](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v135/cmoney_official_interfaces.json)、[原owner／背景工作與RSS邊界](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v135/after.json)、[本輪10／10與全部448筆原件讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v135/handoff.json)。

2026-10-13 **前次工程摘要（v134；13:35–13:42優先工作與等待時背景修復）**：依最新指令改為**鉅亨仍是優先焦點；等待時依 CMoney → UDN → Global Voices → 中時／工商 → MoneyLink → CMNews → MoneyDJ → LTN 的順序交錯處理問題，不必等鉅亨抓完**。鉅亨真正停止、耐久寫入失敗或出現未能自動恢復的阻塞時，工程注意力切回鉅亨；一般尚未到期的重試仍由原排程持有。全goal仍active，**全鉅亨全文尚未完成**，也沒有宣告八個來源全修好。

**兩種並行分清楚。** 更新既有`configs/news/work_order.json`為work-order v2及共用planner：工程背景順序／返回焦點條件可持續接手，v1保持相容；這份設定不是HTTP配額、來源授權或強制停止其他collector的開關。下載端繼續沿用原單一queue writer、16個workers、provider獨立pacing／冷卻，不增加第二個HTTP owner。完整934鍵的plan保留原12:19:02清冊時間，沒有將舊完整清冊冒稱現在重新全掃；排序、原來源事實、未啟用來源與CNA暫緩均保留。此輪沒有改collector code、source config／正文／授權 ABI，也沒有要求重啟新聞或其他服務。

**有真實背景正文，不只修改清單。** 同一正式新聞owner／PID1331683在13:35:09–13:39:22選出的全部8個已提交batch均核對原始回應、正文SHA與耐久收據；其中全部431個當前complete URL heads（鉅亨369、Global Voices20、CMNews42）再以原parser逐件核對作者、正文及發布／修改／capture時鐘。已知舊URL也納入；CMNews這42個是既有正文重查，不能稱為42篇首次下載。另13:34:58–13:40:19完整metric wall321.46秒，鉅亨已驗清單增64頁、提交版本增513／新增URL head計數增500，Global Voices版本增29、CMNews版本增42；metric與capture窗口不同，不能用這些數字替代431個實際原件驗收或宣告全歷史完成。

**八個背景來源各自處理，不因一個來源失敗全停。** CMoney原到期排程再次收到403，保留原來源拒絕，新retry時點為台北13:53:12；該窗口零正文接受，不能把metadata API或換hostname當恢復。Global Voices／CMNews與鉅亨繼續，Global Voices保留官方robots的10秒Crawl-delay。UDN／MoneyDJ仍有既有官方條款的授權工作；MoneyLink／LTN仍待精確公開用途、原媒體與端點檢查，沒有一概判定來源資料錯誤。這次也實際查[旺旺中時集團官方條款](https://member.want-media.com/member-term-of-service.htm)：第七節含未授權爬蟲抓取使用限制，覆蓋中時／旺得富／觸mii／工商；補齊原缺少的planning review，沒有因排名而啟用。唯讀頁已顯示這個檢查狀態，不輸出條款原文、新聞正文或授權金鑰。

**296項相關回歸及8／8handoff通過。** 新整鏈fixture確實讓背景HTTP等待到優先來源完成fsync／收據後才釋放；優先／背景任一來源403都只暫停自身，另一來源保存正文；停用來源沒有HTTP或limiter配置。首次兩個同名run因fixture誤以為停用來源也應有limiter而失敗，修正測試後107項及擴展296項通過，原log保留。另驗收發現同期共享gateway從PID1193917變為1420472，故原「gateway必須同PID」斷言拒絕；本輪未要求重啟它，改為分開核對新聞同owner與當前唯讀gateway，沒有假稱gateway不變。重新讀回全934鍵、planning review、無頁面來源呼叫與無正文輸出均通過；沒有新的UI改動、browser或外網HTTPS驗收宣告。沿用既有架構、分清工程排序與來源權限、以實際整鏈原件驗收，是本輪工作偏好對修改範圍的影響。

證據：[完整934鍵背景工作清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v134/work_order_reviewed.csv)、[全部431個實際正文及收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v134/parallel_body_proof.json)、[321秒同owner／獨立來源觀察](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v134/after.json)、[當前8／8驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v134/handoff.json)。

2026-10-13 **前次工程摘要（v133；13:21／13:22鉅亨優先、真正恢復與缺文原因核對）**：工作順序當時為**先補鉅亨剩餘正文與歷史清單，之後按原始媒體已知網址數降序開發／修復**。沿用原下載器；本輪沒有更改生產程式、設定、來源頻率、正文／授權 ABI，沒有重啟服務，也沒有額外呼叫新聞來源。完整goal仍active，**全鉅亨全文尚未完成**。

**前次13筆標頭逾時事件已全部有真正後續正文。** 對同一正常owner `134bd29fe7714fc7aab51c9b023ec984` 的原不可變故障樣本，重新讀回每筆嚴格晚於故障的原始正文、作者、發布／修改／capture時鐘、SHA與耐久收據，13／13接受，對應12個不同網址。前次未恢復的兩筆都是`5932126`，已於台北13:00:46取得正文。本輪不是把連線池重建、HTTP 200或舊正文當恢復，也不代表其他來源所有故障都已解決。首次驗收指令因直接執行script缺少package路徑而失敗，改用canonical runtime的`-m scripts.audit_news_transport_recoveries`後通過；原失敗log保留，不曾發送額外來源請求。

**308913不是文字漏抓，也不是空模板。** 重新核對發布於2015年、2026/10/12實際觀測並保存的文章原件：JSON-LD未提供`articleBody`，但當前資料與可見頁面有完全相同的圖片-only森林；current／schema作者相符，可見署名卻與該姓名不同。兩種parser均確認相同圖片結構與署名衝突，且改圖、隱藏圖片、插入文字三種負例均拒絕。既有正文guard因此保留`body_not_found`，沒有以圖片說明、推測姓名或OCR冒充來源正文；沒有下載圖片或證明永久無法取得。這是原件支持的表示型態／署名問題，不稱為網路限速或宣稱放寬guard就能取得全文。

**13:21:06有限清單phase為2,435／11,003頁（22.13%）、72,169／326,776筆metadata，剩8,568頁。** 已知鉅亨URL3,612,257，狀態complete3,600,472；狀態增量不是本輪全部原文讀回的篇數。13:22:37單一唯讀交易再對帳：4,848個活躍缺文URL，以及57個已有正文、正在重查的URL；全部狀態與小型progress bins精確相符，沒有aggregate漂移，不能把`state != complete`全當成缺文。清單仍會發現新URL；當下清單情境最短11.9／基準14.0／較慢20.4小時，**只估這個有限metadata phase，不是全鉅亨全文ETA或保證**。

全934鍵及原12:19:02清冊snapshot日期保留，鉅亨後原始媒體依序CMoney273,015 → UDN169,619 → Global Voices110,643 → 中時／工商108,121 → MoneyLink103,129 → CMNews65,016 → MoneyDJ48,089 → LTN30,531個已知URL。聚合／轉址先解析原媒體，不重複下載正文；既有collector繼續，CNA仍暫緩。完整8／8handoff、實際雙parser反例4／4、單交易對帳3／3接受；這些是本輪唯讀驗收，前次8,162項回歸保留但不冒稱本輪重跑，沒有新增UI或外網驗收。此輪沿用既有架構、保留來源值與分清驗收範圍，避免為排序重掃全庫。

證據：[本輪8／8驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v133/handoff.json)、[13故障事件／12個真正後續全文](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v133/recoveries_after.json)、[實際圖片原件與雙parser負例](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v133/actual_image_gap.json)、[缺文與已存正文重查精確對帳](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v133/work_reconciliation.json)、[全部934鍵降序工作清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v131/work_order.csv)。下一步繼續鉅亨有限清單與新增缺文，從來源／傳輸根因修復；未知正文、署名衝突或來源拒絕不算完成。

2026-10-13 **前次工程摘要（v132；13:01鉅亨優先、共用恢復判斷修正與正式入庫驗收）**：仍是**鉅亨剩餘優先，再按原始媒體已知網址數降序開發／修復**。沿用原共用下載器、原文與來源時鐘，不新增平行管線。只平順重載新聞collector一次；原timer續跑，同一唯讀gateway、其他collector與交易均未重啟，CNA仍暫緩。完整goal仍active，**全鉅亨全文尚未完成**。

**根治健康判斷與恢復選擇的錯誤分母。** 即時健康窗口已排除有獨立證據的局部host／robots-origin故障，但`recent_transport_resume`又以原始錯誤率複查，導致可接受窗口被錯誤排除。本輪先重現一個失敗負例，再修共用selector：要求原始計數、已證明隔離計數與完整pressure分母精確對帳；缺欄、負值、布林、矛盾、暫定隔離、真正拒絕或超過原1%預算仍拒絕。隔離故障仍保留為原始失敗，不變成成功或存取許可。selector contract2→3；measurement contract11、全部66來源正文／bulk／授權、獨立清單指紋與頻率設定全部不變。160項聚焦測試及**134個維護新聞／FinMind檔案、8,162項回歸通過**，不是全專案測試。

**不把潛在修復冒稱已測得WAN加速。** 以同owner保留的128個真實窗口及原代碼SHA做舊／新ABBA重播，確認5個健康窗口曾被原始錯誤率錯誤排除；但這批歷史最後選出的容量仍同為0.2秒／7槽。原程序在修正前也曾自行恢復，沒有完全停住。本輪證明選擇邏輯修復，不宣稱官方或理論最高速。13:01正常owner已載入selector3，但尚未觀測到新的live contract3恢復事件；原contract2歷史證據如實保留，並未改標版本。

**發布與恢復事件、文章數分開保護。** 重載前保留的62筆傳輸失敗中，59筆已驗證嚴格後續全文，對應58個不同文章網址；同一網址可以有多次故障，不能要求59個不同heads。最初發佈前檢查因此拒絕，原失敗log保留，未進行任何service action；修正事件／URL對帳並以6個反例檢查後才通過。12:55:14原程序正常exit0，12:56正常新owner`134bd29fe7714fc7aab51c9b023ec984`／PID1331683載入驗收5／5通過。58篇原文、作者、發布／修改／capture時鐘、hash與路徑，以及2,148個已成功清單工作的原期限，全部原值保留；沒有為重載而重抓成功頁或重置退避。

**正式工作與故障均有實際讀回。** 新owner約245.32秒完整metric wall取得43頁／1,261筆清單資料列，耐久新增URL head計數295；按capture時鐘選出的整個已提交窗口271個正文版本／271個目前complete heads，全部再核對原文、作者、來源時鐘、SHA與耐久收據，包含以前已知但這次才下載的網址。295與271是不同窗口邊界，不互換為新文章數。owner CPU51.56秒，約單核心21.0%；不是略過驗證或全歷史性能驗收。另有22個實際新清單原件／660筆metadata獨立重讀來源ID、發布時鐘、payment、SHA、耐久收據及canonical queue；metadata不算全文。

13:00的有界故障樣本有13次`awaiting_headers`逾時，13次失敗origin連線池均成功重建、故障證據全保留；5個局部502及1個AMP robots-origin故障仍按原規則處理。後續13筆逾時事件中11筆已驗證真正後續全文，其餘2筆是同一網址`5932126`，觀察時仍inflight，不算已恢復。不能將25秒標頭逾時直接說成限速，也不能將connection reset當全文恢復。

**13:01:44當下進度：** 已知鉅亨URL3,609,711，狀態complete3,598,951；活躍缺文3,929個URL，包含190個尚待驗證的primary候選；來源空模板、純報價、圖片-only、查無及不支援路徑等結果另留原狀，未計成全文成功。有限清單phase為2,215／11,003頁（20.13%）、65,681／326,776筆metadata，剩8,788頁。當下清單情境最短10.46／基準14.65／較慢20.92小時，基準時點台北2026/10/14 03:40；會隨新觀察重算。**僅此有限metadata phase，不是全鉅亨全文完成ETA、信賴區間或保證**；新增清單仍會找到更多缺文，因此活躍缺文數可增加。

全934來源工作清單仍沿用12:19:02原snapshot並保留日期，沒有為相同排序再掃全庫。鉅亨後依序CMoney→UDN→Global Voices→中時／工商→MoneyLink→CMNews→MoneyDJ→LTN；聚合／轉址走原媒體路由，未授權來源不因排名啟用。證據：[本輪11／11正式驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v132/handoff.json)、[128真實窗口舊新重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v132/actual_history_comparison.json)、[新程序與原件保護載入收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v132/loaded.json)、[整窗口271正文驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v132/throughput/window_body_proof.json)、[全部934鍵工作清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v131/work_order.csv)。本輪只讀回正式唯讀API，沒有修改UI或宣稱新四viewport／外網HTTPS驗收。

2026-10-13 **前次工程摘要（v131；12:38鉅亨優先、完整端點報表與實際新增缺文驗收）**：仍是**鉅亨剩餘優先，再按原始媒體已知網址數降序開發／修復**。沿用共用清冊與原collector，保留原文、發布時鐘、來源拒絕及重試證據；不另開下載管線。新聞owner、gateway、timer和其他collector／交易沒有重啟，CNA仍暫緩。完整goal仍active，**全鉅亨全文尚未完成**。

**修正端點漏列與直接執行入口。** 兩個實際負例證明完整source inventory與implementation plan漏列獨立`cnyes_public_index`。現在兩種清冊共用canonical endpoint builder，端點清冊contract2／plan欄位contract2單獨列出該metadata API；不複製分類API的30頁限制，不把清單摘要當全文，不把100,000頁工程guard當來源限制。保留分類API原觀測與其他端點，來源權限、正文／metadata ABI與正式source code35項SHA均未改。直接`run_fintech_python scripts/audit_news_providers.py`和`scripts/plan_news_sources.py`不再依賴外部`PYTHONPATH`；九個新增語義case連同受影響既有測試，**145項通過**，原兩個失敗log保留。

**只需查端點時，不掃6百多萬URL。** 新`audit_news_providers.py --endpoints-only`只以唯讀SQL取一次owner key，再整理已註冊端點與匹配owner的實測counter；實際輸出110筆端點／namespace描述，程式量測0.0565秒。沒有provider/body完整清冊、沒有把未讀筆數補0、沒有新聞來源請求或queue寫入。它不替代v130的完整唯讀WAL清點（當時71.60秒），兩種工作範圍不同，不能把比值宣稱為全庫盤點加速。

**按新清單帶來的真缺文量檢查排程。** 12:29–12:36同owner的約430.79秒metric wall內，56個新保存清單頁／1,678筆metadata全部重讀原件、來源ID／發布時鐘／payment、SHA及耐久收據；1,678個唯一公開URL均在canonical queue。讀回當下1,223個已有complete正文；455個是在這個窗口新見且仍缺正文的URL。它是窗口觀測，不宣稱全由此端點獨佔造成insert。同期正文請求443次、耐久新增URL head計數432，活躍缺文2,712→2,736；平均約8.13個新見缺文URL／清單頁，接近原8:1正文／清單分工。目前沒有量測證明另一比例能縮短總完成時間，因此先維持公平排程，不稱現值為理論最優。7次transport錯誤保留，自適應與原重試接續；觀察者不額外呼叫來源、沒有重啟collector。完整原件讀回耗wall0.568秒、CPU0.569秒。

**12:38:00當下進度：** 已知鉅亨URL3,606,690，狀態complete3,597,052；活躍缺文2,819個URL，另有來源空模板、純報價、圖片-only、查無、namespace及身分待驗等結果，未改算全文成功。232／232實際超限日期首頁已齊，清單1,952／11,003頁（17.74%）、57,863／326,776筆metadata，仍有9,051頁。當下清單條件情境最短15.09／基準20.95／較慢30.17小時，會隨新觀測重算；**只是有限清單phase，不是全鉅亨全文ETA或保證**。來源原始清單總數不是唯一新聞數，不能用99%以上已知URL complete比例遮住剩餘探索。

**完整934來源工作清單保留原snapshot日期。** v131從v130於12:19:02的不可變全庫清冊重新產生plan，只修正漏列端點；934鍵、每鍵原URL／正文／metadata count及順序全部保持，不假裝重新掃了全庫。鉅亨之後的原始媒體依序仍為CMoney273,015 → UDN169,619 → Global Voices110,643 → 中時／工商108,121 → MoneyLink103,129 → CMNews65,016 → MoneyDJ48,089 → LTN30,531個已知URL。Google／Yahoo等轉址與聚合工作走原始網址解析，不另下載重複正文；未授權／未review者不因排名自動啟用。其他既有collector繼續，工程下一步仍聚焦鉅亨剩餘清單與缺文。

可用指令：`source scripts/runtime_env.sh`後執行`run_fintech_python scripts/audit_news_providers.py --endpoints-only --output artifacts/data_quality/news_endpoint_inventory.json`；這是本機唯讀報表，不是額外來源下載。證據：[本輪9／9驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v131/handoff.json)、[110端點描述](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v131/endpoint_inventory.endpoints.csv)、[全部934鍵降序工作清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v131/work_order.csv)、[56頁原件與真正缺文讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v131/actual_link_yield.json)。新聞頁仍由同gateway／同UI顯示持續更新的清單phase；本輪沒有修改或重新驗證四viewport畫面，前次v130瀏覽器證據保留，不冒稱新UI發布。

2026-10-13 **前次工程摘要（v130；12:21同程序運行、實頁恢復與剩餘排序驗收）**：仍是**鉅亨剩餘優先，再按原始媒體已知網址數降序開發／修復**。這次只修正原新聞頁的快照失敗顯示；新聞owner、唯讀gateway、原timer及其他collector／交易都沒有重啟。完整新聞goal仍active，不把這次工程驗收當成全部歷史下載完成。

**斷線時只保留計數，不保留有效估時。** 真正的瀏覽器定時刷新負例重現：讀取失敗後health已stale，但新清單phase仍顯示`recent_measured`與舊完成時點。現在清單顯示contract2以同一快照時鐘檢查有效性，改標`snapshot_stale`並撤回估時；原計數、兩條清單bar、搜尋與展開狀態保持。文案明確區分畫面更新失敗與下載器停止；其他provider的attention不會單獨撤回鉅亨證據，成功重試自動恢復來源當下forecast，不修改原payload、發布時鐘或正文／metadata ABI。

**驗證不是只讀程式。** 五個相關news／gateway測試檔342項通過；正式本機gateway的1440／768／390／320四種viewport全部通過原生控制項、API數量對帳、無溢出與無新聞來源請求。Desktop真的中斷唯讀status傳輸、等原定時刷新失敗，核對清單估時撤回／bar與篩選保留，再等原重試恢復；實際斷線與恢復原圖已檢視。正式gateway讀回的app／HTML／CSS SHA等於目前檔案，仍是原PID1193917；新聞仍是原PID1191582／owner`53699de701e44e14ae313825be284a8d`。沒有宣稱外網HTTPS或全專案測試通過。

**量測有效工作，不額外對來源壓測。** 約244.13秒完整metric wall內，原正式owner驗證94頁／2,753筆清單資料列，啟動902次請求，其中753次單篇HTML；耐久提交新URL head計數增750。獨立按capture時鐘選出的整個已提交窗口，673個鉅亨正文版本／673個目前complete URL heads全數再核對原文、作者、來源時鐘、SHA與收據，包含以前已知但現在才下載的URL。750按commit窗口、673按capture窗口，邊界不同，不互換或宣稱都是首次看見的文章。此窗口約23.10清單頁／分、184.33新增URL heads／分，owner CPU68.53秒（約單核心28.1%）。平行transport wall不可相加當總elapsed；HTML transport合計1,330.89秒，正文解析30.10秒，清單解析0.052秒，主要等待在網路而非清單解析。

**混合排程與自適應限制仍由同一owner持有。** 實際約8.01次正文請求／清單頁，符合保留已知缺正文的8:1公平排程；這是實測分工，不先假設它一定該刪。窗口原控制器由6槽升到8槽、短暫試到0.1秒間隔，再因實際暫時故障退到7槽／0.2秒；局部AMP robots-origin暫停保留，其他端點繼續。未改來源頻率設定、未新增HTTP owner，也沒有把client設定或這一個窗口稱為官方／理論最高速。接下來應以清單新增真正缺文量、正文積壓與穩定窗口調整混合比，不以單純call變多判定最佳。

**12:21:49有限清單快照：** 232／232日期首頁；1,804／11,003頁（16.40%），剩9,199頁；53,441／326,776筆metadata資料列。當下五個完整分鐘實存182頁，清單情境最短1.53小時、基準4.21小時、較慢10.22小時，基準時點台北2026/10/13 16:34。這是此有限清單phase的條件情境，非全鉅亨全文ETA、統計信賴區間或保證；當下活躍缺正文2,399個URL亦另外保留，不以99%已知URL狀態條掩蓋未知歷史。下載仍持續，後續網頁會用新觀測重算。

**重新清點全部934來源鍵後的後續實作順序：** CMoney273,015 → UDN169,619 → Global Voices110,643 → 中時／工商108,121 → MoneyLink103,129 → CMNews65,016 → MoneyDJ48,089 → LTN30,531個已知URL。來源鍵含不同hostname／角色，不等於934個獨立媒體；Google／Yahoo轉址／聚合工作不另做重複正文管線，CNA仍暫緩，未取得權限的來源不因排名自動啟用。這是單次完整唯讀WAL清冊及canonical plan，不是每次頁面刷新掃整庫。此舊清點工具的端點列表尚未獨立列出新`cnyes_index_api`，因此本輪不宣稱「所有端點欄位已清點完整」；已有該清單的正式來源／分頁／原件證據，後續需補報表漏列，而非另建重複下載器。

證據：[當前運行9／9](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v130/handoff.json)、[正式四viewport與斷線恢復](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v130/browser_current/acceptance.json)、[完整wall量測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v130/throughput_acceptance.json)、[673正文與整窗口耐久讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v130/window_body_proof.json)、[全部934來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v130/provider_inventory.json)、[已知URL降序工作清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v130/work_order.csv)。負例及直接script入口的`ModuleNotFoundError`原log保留；後者在任何清冊／來源工作前失敗，改用正確module入口後實際清點成功，沒有重送來源請求。342項及原始run身分綁在不可變handoff收據，不綁之後可能修改的測試檔當失敗處置證據。

2026-10-13 **前次工程摘要（v129；11:47正式連動、11:54持續保存、11:57實頁及整鏈驗收）**：工作順序仍是**先補齊鉅亨剩餘，再按其他原始媒體已知網址數降序開發／修復**；其他collector／交易照常、中央社暫緩。這次主要把已驗證的有限工作量接回原`/news/`，沒有修改來源HTTP頻率、抓取／重試順序、parser、發布時鐘或正文／metadata ABI，也沒有宣布全鉅亨完成。

**進度必須是同一工作、同一單位、真正來源分母。** 從原分類gap的實際超限發布日集合，逐日取已保存首頁回報的頁數／資料列；每個首頁都齊且各頁來源總數一致才給分母。缺首頁、來源總數異動或型別／對帳錯誤時顯示未知，不補0、不外推未見日期、不把metadata當正文。11:57:54正式唯讀API的分階段快照：

| 階段 | 已驗證 / 有限分母 | 可宣告的範圍 |
| --- | --- | --- |
| 已知超限日期首頁盤點 | 232 / 232日 | 此集合的首頁原件均核對；不是日期全文完成 |
| 同集合清單分頁 | 1,471 / 11,003頁，13.37% | 已保存來源清單頁；仍有9,532頁 |
| 同集合metadata資料列 | 43,589 / 326,776筆 | 來源資料列；不是唯一新聞或可取得全文篇數 |
| 全鉅亨歷史正文 | 整庫分母及完成ETA未知 | 已知URL正文、尚未探索清單及來源結果分開計算 |

**清單估時是條件情境，不是全歷史期限。** 只用本程序最近3至5個完整60秒wall區間的耐久清單頁，等待、失敗、正文工作及零進展區間全部算入；不借前程序速度、不挑最快窗口。最新區間為0便撤回倒數；低樣本先暖機、停止／過期不給有效forecast；任何零區間都不編造有限慢情境。11:57:54的實際5分鐘20頁得到最短／中間／較慢皆約39.7小時、條件中間完成2026/10/15 03:40台北時間；這一刻各分鐘恰好皆4頁，三種情境才會相同，不是統計信賴區間、官方配額、保證或全文ETA。11:37候選曾約36.6小時、11:50約18.8小時，保留各時點，不以後值回填；網頁持續更新當下情境。

**原值及正式運行驗證。** 發布前原130檔加2個phase檔，共132檔／8,136項通過；全部162實際原件、雙parser和10段耐久鏈保持。僅一次journalled平順重載新聞service／原enabled timer，舊owner正常exit0；新PID1191582、owner`53699de701e44e14ae313825be284a8d`，11:47載入6／6。881個先前正式heads的正文／作者／時鐘／SHA／路徑原值保持，1,395個已成功metadata job整列及due保持。唯讀共享gateway另單次重載，PID1193917、3／3；未重啟其他collector／交易，未增加來源HTTP owner。

**11:54跨兩輪正常維護6／6，真正正文仍在保存。** 26次唯讀觀察跨約380秒，正常seed examined增464，全部1,395個受保護metadata整列不變；新增40個驗證清單頁。此程序窗口全部15個已提交batch／432個正文版本逐批讀回，其中全部**370個鉅亨正式complete URL heads**另逐件核對原文、作者、source/capture時鐘及收據。只有1個URL是此程序開始後首次看見，其餘369個是先前已知URL，不能因first-seen早就漏掉下載驗收；也不將370宣稱為全部首次下載。432包含其他provider／版本，不能換算成432篇新鉅亨全文。

**實頁和驗收工具分清三種progress。** 原工具只取卡片第一條`progress`，新增首頁100%後誤當正文比例；失敗log保留。只把工具改成既有正文mini-progress selector，加四種實際viewport的正文／首頁／分頁獨立斷言；後續160項通過，沒有因此修改或再重啟正式collector／gateway／served assets。實際1440／768／390／320四種畫面、API計數、native展開／鍵盤／搜尋／排序、無溢出及讀頁不呼叫來源驗收通過；已人工檢視實際desktop和small-mobile原圖。這是本機正式gateway，不是外網HTTPS驗收或全專案測試。

**尚未解決／下一步：** 全文及全歷史仍未齊；來源空模板、純報價、圖片-only、查無、身分待驗和來源拒絕保留，沒有改算正文完成。清單有9,532頁的實際剩餘，需進一步衡量正文／探索混合排程的有效吞吐，而非提高未證明的來源上限。實頁測試已驗證browser transport失敗後原始網址ETA撤回／自動恢復，但新增清單phase尚未驗證該client-side斷線分支；程式閱讀顯示`refresh`保留的舊phase forecast仍可能顯示，需要下一輪補上局部失效和負例，不宣稱這個邊界已解。來源快照過期／程序停止的backend phase失效已由語義測試核對。其餘媒體的排名沿用10:22全934來源清冊，不因網址多自動授予存取權限。

證據：[原132檔8,136回歸](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v129/regression_run.json)、[162原件不變](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v129/all162_index_body_invariance.json)、[232首頁／分母獨立讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v129/phase_candidate.json)、[正式新聞載入](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v129/loaded.json)、[唯讀gateway載入](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v129/gateway_loaded.json)、[兩輪維護與全部正文batch](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v129/maintenance_acceptance.json)、[全部370個鉅亨本窗口正式heads](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v129/whole_written_Anue_heads.json)、[locator修正後160項](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v129/render_focused_run02.json)、[實際四viewport](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v129/browser_current02/acceptance.json)、[最終當前運行7／7](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v129/handoff.json)。首輪stale fixture、模組入口及舊locator失敗均保留，後續實際成功run另綁定處置；首輪fixture的既有證據移到SHA／bytes完全相同的不可變副本，原failure／log／verification／history不改。

2026-10-13 **前次工程摘要（v128；11:00正式載入、11:16全部232日期首頁與本輪正文全cohort驗收）**：維持**鉅亨剩餘優先，再依原始媒體已知網址數降序開發／修復**。其他金融／交易／新聞服務照常、中央社暫緩，完整goal仍active；本輪沒有宣布鉅亨或全新聞完成。

**先把真正未知的工作量查清，不讓單日深層分頁擋住其餘日期。** 10:46完整剩餘清冊與實際metadata佇列確認：232個真正超限日期中，24個已觀測、208個尚未取得首頁；當時829頁、24,529筆metadata，沒有清單端點錯誤。隔離原共用selector精確重現：新日期page2先於舊日期page1。只修改既有`news_fulltext_cnyes_index`的排序；每個實際日期首頁先查，再按新到舊續抓來源真正宣告的分頁，沒有生成未宣告頁／猜ID／增加端點。共用RSS、追新、有效bulk正文與已知缺正文公平排程仍保留；來源頻率仍由原自適應owner控制，不宣稱官方最高速。

**排序遷移只動priority，不重抓成功清單。** 已存在的首頁以有界來源／kind核對改排序；due、403／429／transport拒絕、attempts、ETag、原始SHA與發布／capture時鐘保留，停用來源或獨立端點不遷移，foreign URL collision亦不覆寫。重複seed完整列不再異動。66來源正文／授權／bulk及獨立metadata ABI、原設定與網頁檔案均未改；私人進度另標明manifest schedule contract1。

**發布前130個維護news／FinMind／bulk檔8,090項、88聚焦項與162原件全鏈通過。** 162份實際原始回應、兩種parser及10段claim→解析→接受或保留失敗→Parquet／fsync／收據→獨立讀回，正文、全部作者、原時鐘及各失敗結果與v124完全相同。首次launcher兩個不存在的測試路徑、disabled fixture誤用`enabled`（實際欄位為`allowed`）與首個全回歸單一fixture失敗均保留；修正後原130檔全保留，沒有刪測試或放寬來源來掩蓋。根因負例及上述三個failed run均已綁定真正後續成功run作工程處置，原log不覆寫。

**只平順重載正式新聞service／原enabled timer一次。** 10:59舊owner正常exit0，新PID1124113、owner`645996a6c1744e8b96327d7671b11131`；11:00載入驗收6／6。全部369個既有正式heads原正文／作者／發布／修改／capture／SHA／保存路徑保持；1,009個先前成功metadata job只對首頁排序遷移，其餘整列原值保持。原唯讀934來源gateway、news UI及其他collector／交易未重啟，沒有另一個來源HTTP owner。

**11:05當前部分清冊驗收5／5，與完整232日期phase分開。** 151個已觀測日期的首頁全部逐件讀回真ID、原發布日、公開旗標、原始Parquet／SHA與耐久收據；該單一快照尚有81個日期未驗證首頁。新程序此窗口的全部13個已提交batch、228個正文版本亦逐批讀回；其中全部36個本輪新first-seen且complete的Anue URL heads另逐件核對原文、作者、source clock及收據。228包含其他來源及先前已知URL版本，不是228篇新鉅亨正文；151日期只有首頁證據，不是151日期全部分頁／全文完成。全部1,009個受保護成功清單完整列再核對保持，原分類gap仍1,518列，不把占位符或沒有留存原件的4個身分錯誤改算取得。

**11:16首輪完整驗收5／5：232日期首頁全部原件核對，512篇新鉅亨正文逐件讀回。** 原Workflow handle`v128-observe-all232-source-manif-20261013T030103-a60de4db`已實際exit0，59個觀測跨895秒及兩次以上正常維護，沒有額外HTTP、重排或restart；全部1,009個受保護metadata列保持。232個實際超限日期的首頁全部核對真ID、發布日、payment旗標、Parquet SHA與耐久收據；來源首頁合計回報**11,003頁、326,776筆metadata**，這是此有限日期集合的來源分頁量，不是獨立新聞篇數、可取得全文量或全歷史分母。11:15:44同程序全部39個已提交batch、823個正文版本全數讀回，另對全部512個本輪新first-seen且complete的Anue URL heads逐件核對原文、作者及來源時鐘；823包含其他來源與既有URL版本，不能替代512篇新正文。觀察期限不是來源服務失敗或全歷史ETA。

**下一階段仍由同一正式owner續抓實際分頁與原正文。** 11:16驗收末端清單為1,219頁、36,162筆metadata、27日期來源回報全部頁均已觀測；其餘日期尚未把全部分頁跑完，category gap仍保留。首頁的來源總量可用來盤點這232日期的metadata phase，不把清單列當全文或用短暫burst外推全庫完成日。全歷史完成ETA仍未知。其他provider順序仍沿用10:22全934來源清冊：CMoney、UDN、Global Voices、Wantmedia、MoneyLink、CMNews、MoneyDJ等；網址排名不授予來源存取權限。

首輪證據：[全部232日期與本輪正文驗收5／5](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v128/manifest_acceptance.json)、[全部232首頁原始回應核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v128/all_planned_manifest_originals.json)、[全部512個新complete Anue heads](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v128/all_new_complete_Anue_heads.json)、[整個程序39個已提交正文batch](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v128/whole_new_owner_body_batches.json)。此前11:05部分驗收保留，沒有以後來結果回填其時點。

證據：[10:46全部剩餘與原件](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v128/before.json)、[實際首頁／分頁前沿](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v128/frontier.json)、[130檔8,090回歸](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v128/regression_run02.json)、[162原件全鏈](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v128/all162_index_body_invariance.json)、[正式載入6／6](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v128/loaded.json)、[11:05當前部分清冊5／5](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v128/current_snapshot01/acceptance.json)、[151首頁完整原件](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v128/current_snapshot01/all_current_observed_first_page_originals.json)、[全部36個新complete heads](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v128/current_snapshot01/all_new_complete_Anue_heads.json)、[全部13個正文batch](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v128/current_snapshot01/whole_new_owner_body_batches.json)、[原failed log處置](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v128/failure_resolutions.json)。源本文本仍私人保存；沒有新外網HTTPS驗收或全專案測試宣告。

2026-10-13 **前次工程摘要（v127；10:28維護、10:29正式全文與全部剩餘URL讀回；v126新增網頁連動）**：仍先把**鉅亨剩餘補齊，再按其他原始媒體已知網址數降序開發／修復**。既有金融／新聞下載器繼續、中央社暫緩；整個goal仍active，沒有宣布鉅亨或全新聞完成。

**根治已驗證清單反覆重抓的排程錯誤。** 原`seed_discovery`以正文／HTML discovery指紋比對獨立public-index metadata指紋，兩者本來不同，因此每180秒維護把成功清單誤設`due=0`。10:06實際345份原始清單中116個已驗證job又到期，224個未取得job仍待查。隔離fixture精確重現：僅due從一年後變0、另12個欄位不變。修正後依實際端點的獨立指紋判定是否重解析，正常HTML清單保留原語意，停用metadata註冊仍保存原期限；403／429／robots／transport錯誤、attempts、SHA與來源時鐘不重設。只改共用新聞seed檔，沒有更改66來源的正文／授權／bulk ABI、獨立metadata ABI、來源頻率或工作順序。

**正式程序已跨兩次正常維護驗收6／6。** 10:17只平順drain／重載新聞service／原enabled timer一次，舊程序exit0；新PID1054543、owner`3d9d95bfbe594d138bf34ee0a9f1d13b`，NRestarts0。10:22至10:28的379秒、26個唯讀觀察中，374個已驗證清單job的完整列、到期時間、SHA與原時鐘逐次全相同；實際維護examined計數464→928，正常源頁繼續推進。10:28獨立讀回128份此前未取得頁的原始metadata、真ID／發布日與耐久收據；當刻清單577頁、17,092筆metadata、16個日期，15個日期已取得來源回報的全部頁。該128頁找到94個新的正文網址、94個都已有正常正式正文；此處只有16個全文抽樣核對，下一段另有整個新程序全文cohort驗證，不能把清單或抽樣當成全文全驗收。原分類gap仍1,518列，整庫分母與完成ETA未知。

**10:29新程序全部26個正式batch、315個正文版本讀回，219個新完整鉅亨URL heads逐件驗收4／4。** 從新程序時間窗的已提交batch選取URL，再用實際first-seen與capture確認，不掃描全部360萬heads或以HTTP 200／summary代替。219篇全數核對原始正文、全部作者、發布／修改／capture時鐘、SHA、來源重播與正式收據；全部150個前次正式heads原值亦在載入與維護後保持。315是此程序窗口的正式正文版本，包含其他來源與既有版本，不是315篇新鉅亨文章。

**10:29全部未存正文URL為7,033＝214個活躍缺口＋6,819個其他來源／路徑結果。** 活躍缺口含151個AMP pending、18個AMP 502、18個AMP robots-origin cooldown、7個未解正文、3個身分待驗及其他pending／inflight／transport重試。其他結果是空模板4,617、查無1,269、純報價346、圖片-only11、路徑574、身分2；保留原始證據，不算全文完成，也不一律推定永久不可取得。這是10:29:05單一WAL快照；後續正常清單仍會發現新URL，不能把7,033當固定整庫分母。下一步仍是232個實際發布日的合法清單→原正文取得，以及來源／身分缺口修復。

**網頁已沿用原`/news/`與唯讀API，新增鉅亨清單細項。** 全文與清單頁數／metadata筆數／已觀測日期／來源異動／歷史分類gap分開顯示。metadata不冒充正文或唯一篇數；沒有整庫分母時不畫假的完成比例或完成日。清單細項預設可收合、鍵盤可操作，重繪／搜尋／排序保留開關狀態；未知或舊版欄位顯示未知，不改成0。API只投影白名單計數，讀頁不開生產SQLite、不呼叫鉅亨、不暴露金鑰或原文。v126的306項新聞／gateway回歸及1440／768／390／320四種實際viewport、API數值／控件驗收通過；這是本機原正式gateway驗證，不是外網可達性證明。公共HTTPS一次15秒連線逾時，未宣稱外網驗收通過。v126五檔廣回歸另有3個未修改的FinLab／shared-layout舊斷言失敗（4卡vs實際5卡、v3vsHEAD v5），沒有修改其他provider來掩蓋，也不聲稱全專案回歸完成。

**工作清單已用10:22全934來源唯讀快照重排**，6,207,333個已知URL heads，日期欄仍是metadata排序日，不是發布證據。CMoney272,931 → UDN169,609 → Global Voices110,643 → Wantmedia108,116 → MoneyLink103,110 → CMNews64,979 → MoneyDJ48,075等；完整清單另含聚合／轉址與metadata-only鍵，這些沿用原來源解析，不另建重複正文管線。網址排名不授予存取權限，來源條款／robots／拒絕／body驗證保留。此清冊是一次72.34秒wall／44.67秒CPU的單一完整唯讀快照，不在網頁或15秒輪詢中反覆執行。

修正前負例、第一次launcher路徑錯誤、載入驗收helper缺少snapshot-only欄位的失敗均保留；後續用實際相同fixture／同一已啟動新聞程序驗證處置，不重啟第二次。10:19:55其他網站工作實際重載共用gateway，早期same-PID gate因此拒絕；另讀當前news assets／白名單API與原150heads接受，明列gateway變動，不冒稱其程序從未異動。**發布前129個維護news／FinMind／bulk測試檔、8,080項通過；162份原件的雙parser與10段完整耐久鏈完全保持**。後續共享網頁測試有其他工作新增；10:34當前共享世代的129檔8,080項再次通過、四種實際viewport與API／控件再次接受。各次handle獨立保存，不覆蓋發布前紀錄，也不混算網路最高速。

最新共享世代證據：[129檔再次回歸](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v127/full_regression_run02.json)、[當前gateway四viewport讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v127/browser/acceptance.json)。10:34:39後續正常心跳為667頁／19,774筆metadata／20個已觀測日期，17個日期清單齊；這只是後續心跳，不重寫10:29完整正文cohort或其全部剩餘清冊。

證據：[根因與真正負例](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v127/before.json)、[發布前129檔回歸](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v127/full_regression_run01.json)、[162原件完全相同](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v127/all162_index_body_invariance.json)、[同一新程序載入讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v127/loaded03.json)、[兩個維護週期](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v127/maintenance_acceptance.json)、[原始128頁與新URL](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v127/index_originals_after_maintenance.json)、[新程序全部219個完整heads](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v127/all_new_current_owner_complete_heads.json)、[26個正式batch](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v127/current_owner_whole_body_batches.json)、[10:29全部剩餘驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v127/current_acceptance.json)、[全部剩餘URL](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v127/cnyes_remaining.csv)、[934來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v127/provider_inventory.json)、[已知URL降序清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v127/work_order.csv)、[v126網頁連動](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v126/handoff.json)、[v126四viewport](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v126/browser/acceptance.json)。

2026-10-13 **前次工程摘要（v125；09:35／09:37正式來源與全文讀回）**：維持**鉅亨剩餘先抓，之後按原始媒體已知網址數降序實作／修復**。CMoney、UDN、Global Voices、Wantmedia、MoneyLink、CMNews、MoneyDJ等的順序仍依01:11全934來源快照，不冒充本刻重新排序；其他金融下載器照常、中央社暫緩，完整goal仍active。

**找到可補漏的不同公開清單，已接到原下載器，不是繞過分類API上限。** 全部1,518個歷史gap含704個舊父scope、814個實際單發布日scope；後者是407個分類／日期候選，合併排程只需232個真正日期。候選同日不等於來源頁ID相等，原gap、分類頁證據與重試時間均保留。核對原HTML實際連結的一份23,823-byte公開JS及SHA，讀到`/media/api/v1/newslist/all`的page／limit／epoch日期參數；只用原唯一NewsHTTP、robots、來源退避及正常drain／恢復測試。另一次v2/search回422，明列日期須Y-m-d且開始時間限近一年，立即停止，searchAllMatch未送出，不拿該搜尋端點補2014年。

all清單在2014-09-10的第1、2、31頁皆回200，90個實際ID不重複、真publishAt皆在指定發布日；其中16個primary網址原先不在本機。limit=30與100的首頁原始回應SHA完全相同，都只有30筆；不虛稱100筆或官方最高速。來源宣告10,844行／362頁，**不是已驗證362頁、唯一文章數或全文**，實際欄位只有標題／摘要等metadata。原分類API仍限制30頁，兩種端點、證據及指紋分開。

新共用`news_fulltext_cnyes_index`以獨立metadata ABI註冊此實測端點，只針對真超限日期，每日一份清單、每次依來源pagination接下一頁。每頁核對真ID、原始發布epoch、公開payment旗標、完整頁形及重複頁；付費ID不送正文隊列，summary永不接受成正文，未知可能全文欄位不保存。原metadata Parquet／收據先耐久保存再註冊URL；中斷在Parquet與收據之間可由精確原件恢復，不重寫原時鐘。parser circuit只影響這份清單，正文與分類API照常；停用註冊時舊清單job保留但不再呼叫。正文仍由原佇列完成同篇ID、作者、原生全文、發布／修改時鐘及正式保存驗證，已取得的正文不重抓。

**09:35實際程序工程檢查6／6接受**：276份正常清單原始envelope及Parquet／收據逐件讀回，8,183筆metadata，10個日期已觀測、6個日期已取得來源回報的全部清單頁。清單找到並註冊**94個此前沒有的正文網址，94個皆已由正式程序保存全文**；09:37再把這94個全文全數分組核對原文、全部作者、發布／修改／capture、SHA及收據，4／4接受。94個first-seen皆在對應原始index觀測後一秒內，維持實際註冊時序，不拿關聯當因果或改寫發布時間。169個近期正式body版本另有batch讀回，全部56個先前正式heads的8個原值亦保持。09:37心跳仍running，清單311頁／9,233筆metadata／12個日期，6個日期清單頁齊；**清單完成不是該日全部正文完成，更不是全鉅亨完成**。原分類gap仍1,518列，整庫完成ETA未知，不以重試資格、抽樣日期或平均頁數冒充。

全部**162份既有實際原件、雙parser與10段完整claim→解析→接受或保留失敗→fsync／收據→獨立讀回**保持與v124完全相同；66個來源的正文／permissions／bulk指紋均未改，新增的只有獨立清單指紋。完整**127個維護news／FinMind／bulk測試檔，8,018項通過**，不是全專案或網路最高速驗收。首次聚焦的2個fixture斷言問題修正；首次全回歸8,016通過／1失敗，是把新增鉅亨專用註冊一起複製到other-provider的舊fixture，不是放寬來源限制。fixture移除這個專用欄位，新增外來源不得取得index的負例；原失敗log與第一次run保留。

**正式重載是既有設定watcher自動執行，沒有額外人工再重啟。** 09:27舊owner記錄configuration_reload_requested、正常退出75/TEMPFAIL；systemd按原設計重新啟動PID961191、owner`b07f0d5d85f94a8392ae98cd0c1ccfd6`、NRestarts1。設定改動觸發的自動重載先於完整回歸，本報沒有倒置成「完整測試後才部署」。其後以同一程序驗證上述測試世代與實際來源／全文；原enabled timer、同一gateway、其他collector與交易未重啟。新清單細項目前寫入私人status／收據；既有網頁正文數會隨新heads更新，**公共projection尚未顯示這個新增清單細項**，不宣稱其UI已完成。

證據：[1,518 gap全部清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v125/before.json)、[原頁公開JS](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v125/bundle_source.json)、[v1/all與v2搜尋實際回應](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v125/published_query_observations.json)、[all第1／2／31頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v125/all_pagination/published_query_observations.json)、[唯一HTTP與正常恢復](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v125/all_pagination/published_query_operation.json)、[162原件完全相同](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v125/all162_index_body_invariance.json)、[127檔完整回歸](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v125/index_regression_run02.json)、[09:35實際程序接受](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v125/index_current01/acceptance.json)、[清單原件與94個新網址](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v125/index_current01/index_originals_and_new_urls.json)、[94個全文全數讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v125/index_current01/all94_new_article_bodies.json)。下一步持續232個日期的合法全清單→原正文取得，完成後再依已知網址數推進其他原始媒體；來源拒絕／未知分母及舊分類gap保留，沒有把局部工程接受當作整庫完成。

2026-10-13 **前次工程摘要（v124；08:53正式載入與剩餘清冊驗收）**：保持**鉅亨剩餘優先，之後依原始媒體已知網址數降序開發／修復**。接續CMoney、UDN、Global Voices、Wantmedia、MoneyLink、CMNews、MoneyDJ等；排序沿用01:11全934來源快照，不冒充此刻重新掃描。其他金融下載器繼續、中央社暫緩；完整goal仍active。

**釐清並修正來源非正文誤重試，不把它們冒充下載成功。** 全部27份剩餘原件中，14份是字面報價、4份是圖片-only、2份是空模板；7份仍未解，包括占位字串、未宣告標題的報表、任意數字及308913的來源作者矛盾。完整核對current／可見原生DOM／schema、同篇ID、全部作者與真發布／修改時鐘；保留每個數字、空白分界、空節點與結構差異。圖片頁只辨識兩種已觀察的精確GPT loading宣告，不憑廣告class忽略未知文字／控件，不請求圖片或做OCR。空模板比較整棵AST，只省略來源真正宣告的一個空尾節點；未知、隱藏、腳本、media與多餘結構仍拒絕。

全部**162份實際原始回應**（70／12／18／35／27五個完整cohort）與兩種parser、完整claim→解析→保存或保留失敗→Parquet／fsync／收據→獨立讀回接受**10／10**。原135份的所有成功正文、作者與原時計保持，非正文只更改已逐件核對的分類，其餘未解結果保留；本輪27份沒有產生任何新文章正文。原件證明的20個非正文不是20篇新下載，另7份與4個沒有留存原件的身分錯誤不推定完成。**126個維護news／FinMind／bulk測試檔、7,976項回歸通過**，不是全專案或官方最高速驗收。

08:49只平順重載原新聞service／enabled timer一次，舊owner正常exit0；新owner`0037abc402424bd6a48eb55e4fc142c8`、PID899126、NRestarts0。首次驗收helper誤向snapshot-only runtime讀取不存在的`accepted`欄，原failed log保留；修正驗證後，**同一程序5／5載入與6／6當前工程檢查接受**，沒有第二次restart。全部56個原正式heads的正文／作者／發布／修改／capture與保存路徑未改；66來源permissions／bulk、其他65capture、配置與診斷配額保留，Anue空模板8／報價7／圖片3的capture指紋明確變更。其他collector、交易與同一934來源唯讀gateway未重啟。

**08:53仍有7,018個未存正文URL，其中217個活躍缺口；歷史gap表1,518列。** 20個已證明的非正文目前只有2個已由新owner正常排程分類；不強制提前重試或把其餘18個的離線證明當正式結果。全庫有1,116份歷史缺口退休收據，本輪獨立驗證最新32個與32個leaf，遞迴共110份完整ID／日期證明、1,329份觀測及1,047份不可變原始envelope／收據；不是全部1,116份已在本輪逐件驗收，亦不是新owner又取得1,116範圍。新owner此窗口沒有新清單頁；9個正常批次／29個正文版本已讀回，不稱29個新增鉅亨網址。全來源runtime仍有`healthy_sources_make_independent_body_progress=false`，不宣稱所有媒體正常。清單、URL heads、非正文與正文版本分開計算，全鉅亨完成ETA仍未知。

下一步仍先處理鉅亨：核對真正單發布日超限的來源每頁筆數／合法查詢能力、缺頁與原生正文／身分障礙；只沿用canonical HTTP／排程／收據，不重抓已證明完整的範圍、不把秒／小時切片當有效來源filter、不繞過來源拒絕。未取得資料與分類完成是兩種結果，後續其他provider的實作順序不會因此提前算完成。

證據：[162原件與完整耐久鏈](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v124/originals02.json)、[126檔完整回歸handle](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v124/regression_run02.json)、[同一新owner正式載入驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v124/loaded02.json)、[08:53當前工程驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v124/current01/acceptance.json)、[原始ID／日期／退休收據讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v124/current01/gap_proof_readback.json)、[20個非正文的實際排程結果](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v124/current01/normal_nonbody_outcomes.json)、[全部剩餘網址清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v124/current01/cnyes_remaining.csv)、[29正常正文版本讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v124/current01/recent_current.json)、[原失敗及後續真實驗證處置](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v124/verified_failure_dispositions.json)。以下摘要是各自當時的舊快照，不是最新狀態。

2026-10-13 **前次工程摘要（v123；03:26正常原件、逐頁ID、父缺口與正文收據驗收）**：保持**鉅亨先補齊，之後依其他原始媒體已知網址數降序開發／修復**。接續CMoney、UDN、Global Voices、Wantmedia、MoneyLink、CMNews、MoneyDJ等，沿用01:11全934來源清冊，不把舊排名冒充此刻重新掃描；其他金融下載器繼續、中央社暫緩。全部goal仍active，沒有聲稱全新聞或鉅亨全歷史完成。

**正常owner已實際完成並獨立驗證16個歷史父缺口，原gap與觀測時鐘另存退休收據。** 核對全部必要子頁、實際來源ID與發布日、跨子集合不重疊，以及子total加總等於父原始total；不是queued、首頁成功或HTTP 200就刪缺口。此窗口另讀回32個完整leaf清單、共53個完整證明、496份逐頁觀測及474份不可變原始envelope／收據；其中446份是新owner正常來源觀測，其餘保留原來的source capture clock，沒有把舊件改成新抓。正常批次70個正文版本及56個原正式heads的原文、作者、真發布／修改／capture時計與路徑核對通過；70版本不是70個新鉅亨網址。當前工程7／7接受，全來源runtime仍有`healthy_sources_make_independent_body_progress=false`，不能據此宣稱其他網站全部恢復。

**去重以實際逐頁證據為準。** 同一實測来源日可有不同秒數查詢；只有當原頁的total與完整ID hash都吻合，才引用真正原請求、原bytes與原時計，不複製／改名正文或捏造alias capture。每個必要頁都要有證據；同一清單的證據分散在不同查詢也可拼合驗證。不吻合／缺證據則回查原網址，保留其失敗與source cooldown。來源total變動、超過30頁或ID／日期矛盾時，舊完成狀態會撤回；真正單來源日超過30頁仍是明確來源缺口，不改用未驗證的秒／小時切片或page31。清單完整不等於正文完整、唯一新聞總量或歷史PIT正確。

最終隔離驗證涵蓋**全部21,400個實際coverage scopes、290,501筆原頁及294,866筆原discovery jobs**；5,232筆待補metadata整批處理，192原件恢復、79原查詢／bytes證據不相符、4,961沒有可用原件；後兩種不是空資料／來源永久錯誤，透過正常owner排3,409次每有效日／頁一次的成功頁補證據。全部原頁ID／時計、失敗、deadline與19 endpoint／1 adapter circuits保持；重新開啟隔離佇列不重排。12項檢查接受。初始索引1.466秒，maintenance52片共2.526秒wall／2.679秒CPU；包含完整read／clone／decode／驗證23.880秒wall／23.962秒CPU。正常維護改為每輪最多128筆／0.05秒soft切片，而不是固定8筆後等2秒；單次檔案IO／decode仍可能超出soft切片，本次最大0.235秒。這是本機metadata效率與完整性，不冒稱WAN倍速／官方最高頻率。

176項相關語義測試及**125檔／7,524項news＋FinMind回歸**通過；最終34個source file hashes與正常owner實載一致，66來源body／bulk／permissions指紋及原config不變。只對原新聞service與enabled timer做一次平順drain／start，owner`b3cc6756c8ad4137b03ed6716de53bf8`／PID349215／NRestarts0；交易、其他collector、同一934來源唯讀gateway未重啟。初次載入驗收因一列在重載前早已真實更新的gap時計而false；完整舊快照及該次source page witness保留，同一程序重新驗收接受，沒有第二次restart。早期父缺口尚未齊的current01 false也保留；readonly observer等到真正16範圍完成才驗收，沒有額外publisher call或重排。

**03:26全鉅亨仍有7,018個未存正文網址＝219活躍缺口＋6,799其他來源／路徑結果；歷史缺口表1,846列。** 網址、查詢窗口、逐頁觀測和正文版本分開計算，未把不能取得的內容算成完成。16範圍是此窗口已獨立驗證的退休，不是所有1,820舊缺口已解；其後正常心跳可能再結案，但不冒充已在這次驗證集合內。全鉅亨完成ETA仍未知；後續繼續補當前未觀測子頁／metadata、處理真正來源上限及219活躍正文缺口，然後才按已知URL排名修下一provider。

證據：[全部實際metadata與保存／重啟驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v123/full_metadata_verified04.json)、[同代125檔完整回歸handle](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v123/regression_run05.json)、[同一正常owner正式載入驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v123/loaded02.json)、[03:26實際資料驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v123/current03/acceptance.json)、[16父缺口全部原始ID／日期／receipt讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v123/current03/gap_proof_readback.json)、[70正常正文版本收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v123/current03/recent_current.json)、[全7,018未存正文逐筆清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v123/current03/cnyes_remaining.csv)、[父scope唯讀觀察與無重啟證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v123/parent_stage_watch.json)、[9項原失敗及後續真實驗證處置](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v123/verified_failure_dispositions.json)。以下都是各自當時的舊快照。

2026-10-13 **前次工程摘要（v122；02:27當前正常子頁與正文收據）**：工作順序仍是**鉅亨優先，之後依其他原始媒體已知網址數降序開發／修復**；全934來源排序沿用01:11快照，不假裝重新掃描六百萬筆。其他金融下載器照常、中央社暫緩。不是全部新聞或鉅亨歷史完成。

**已正式載入共用切窗calendar2／dispatch3。** 原UTC日可能涵蓋兩個來源發布日；依前輪10個超限類別、40次真實來源對照的UTC16邊界分割，精確保留每個父查詢的全部請求秒數。真正同一來源日期不再切成無效的秒／小時查詢。這是實測查詢行為，不冒稱官方時區規格。直接重用全部**1,820個既有首頁證據，排出3,640個子窗口**，不必先額外重抓1,820個父窗口；4個子窗口沿用原失敗期限。只更新排程ABI，全部66來源的正文／bulk／授權指紋、原設定及原生正文parser不變。

**02:27正常owner已實際驗證40個新子範圍、806頁；其中38個範圍的表定頁數全部已觀測，1個真正發布日仍超過30頁。** 這不是806篇新正文，也不是完整唯一ID或全歷史證明。10個真正正常耐久批次／57個正文版本獨立讀回接受，56個既有正式正文、作者、真發布／修改／capture時鐘與路徑原值保留。此窗口8項工程檢查及全來源runtime的可執行進度接受；既有拒絕仍留在傳輸／恢復證據，不代表所有網站都可取得或已完整下載。

**仍有7,018個未存正文網址＝219個活躍缺口＋6,799個其他來源／路徑結果；歷史缺口表共1,821列。** 原1,820列保留，另加1個實際單發布日超限；不是因子頁已排程就刪除舊缺口。下一階段需逐父範圍核對全部子頁、來源ID與發布日期證據，再以版本化收據結案；相鄰子請求可能落在相同發布日，要有完整等價證據後才去重，不能假裝所有3,640次都是唯一來源日。全鉅亨完成ETA仍未知，來源查詢窗口數與正文網址數分開計算。

135項相關語義測試、**124個檔案／7,483項news＋FinMind回歸**通過；不是全專案測試或官方最高速驗收。隔離canonical佇列完整重播54,600筆原頁及原job，全部舊total、ID hash、錯誤、cache、時鐘、嘗試與來源退避保留；純遷移4.137秒wall／4.137秒CPU，包含讀取、隔離建置及驗證共20.114秒wall／15.674秒CPU，不宣稱WAN下載倍速。02:20:48單次平順重載、02:22:07載入驗收接受，新owner`664de0ae66204e0db33a2d40ca991e0b`／PID265759、原enabled timer續跑，沒有重啟交易、其他下載器或唯讀gateway。

證據：[完整1,820範圍重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v122/migration_verified.json)、[124檔回歸handle](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v122/regression_run.json)、[正式載入收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v122/loaded.json)、[正常子頁明細](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v122/calendar_child_progress.json)、[當前資料驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v122/current_acceptance.json)、[正常正文耐久驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v122/recent_current.json)、[全部未存正文CSV](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v122/cnyes_remaining.csv)、[缺口狀態與口徑](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v122/cnyes_remaining.json)。完整goal仍active。

2026-10-13 **前次工程摘要（v121；01:53當前收據與完整鉅亨缺口快照）**：工作順序仍是**先補齊鉅亨，再依其他原始媒體已知網址數降序開發／修復**；接續CMoney、UDN、Global Voices、Wantmedia、MoneyLink、CMNews、MoneyDJ等。沿用01:11的全934來源清冊／排序，沒有為相同排名重掃六百萬筆；其網址數有自己的快照時間，不冒充此刻重新清點。既有金融下載器繼續、中央社暫緩，聚合／轉址仍走原媒體管線。

**上一輪7個修復網址已全部正式補回，01:45:51驗收接受。** 最後一筆`m2218358`於原重試資格後正常取得；完整正文、作者、來源發布與修改時鐘、capture41及原始耐久收據核對通過。49個原有正式heads加這7個形成56個已驗證正式網址；它們的真正capture時間與原保存路徑不因本輪重查而改寫。01:53現任正常owner的50個正文版本再用原始批次與收據獨立驗證，全部56個舊正式heads原值保留。此階段工程7／7接受；其他來源的失敗仍可見，全來源runtime另有`healthy_sources_make_independent_body_progress`未滿足，**不能以本輪局部工程接受宣稱所有網站成功**。

**找到歷史缺口的共同根因：UTC「一天」並非來源的一個發布日。** 沿用全1,820超限窗口清冊，逐一選取全部10個實際超限分類的控制窗，以原查詢、UTC16:00前／後的兩個無洞且不增加請求秒數的子窗、單秒控制，共做**40次正常canonical來源查詢，全部200／既有pagination驗證通過**。10類皆呈現兩個子窗的來源回報total相加等於parent、兩邊首頁ID不重疊、發布時鐘位於UTC16分界兩側；此次選出的20子窗全部各自不超過30頁。以announcement為例，901／31頁變為496／17頁＋405／14頁。

單秒控制仍回同發布日總數與相同完整首頁，不是秒級filter；來源表現與UTC+8曆日正規化一致，這是實際回應的推論，**不是已找到官方時區聲明或所有年份的完整ID證明**。原`cnyes_window_partitions`及`migrate_cnyes_window_dispatch`把每個UTC日判成不能再分，是下一階段要根治的過時假設。本輪已實作有界清單／metadata-only測試、輸入前驗證與唯一operator鎖、拒絕停止及正常服務恢復；**新發布日切窗尚未套入正式下載排程，1,820窗並未因診斷而算補齊**。全部子頁、來源異動／重複SQL行、真實單發布日仍超限及跨分類ID覆蓋仍需逐項驗證，source total不是唯一文章分母。

01:46只短暫graceful drain共用新聞service，在已完成原7筆observer後才由單一NewsHTTP owner測試；測試完成立即恢復原service／enabled timer。PID208629、owner`3bba0d10aceb4338b960faf5cb845c5e`的來源代碼33檔、66來源fingerprint／授權／grain、原配置及同一唯讀gateway均未改，不重啟其他金融下載器或交易，不重置來源拒絕／重試期限。108項聚焦測試與完整**123個維護news／FinMind／bulk測試檔、7,464項回歸通過**，不是全專案測試，也不宣稱網路／官方最高速度。

**01:53全部7,018個未存正文URL＝219活躍缺口（190個未驗證primary候選＋29個無已知primary）＋6,799個其他來源／路徑結果，另有1,820個歷史查詢窗口。** 其他結果仍逐筆保存，不算正文完成，不一概判為永久不可取得；窗口數不是文章數。既有全934降序工作順序保留，下一階段優先把上述實測日期邊界整合為版本化共用排程、保存舊parent／合法頁收據與退避，真正取得並驗證子頁。**全鉅亨完成ETA仍未知**，不把本輪40個診斷頁或舊重試時間當整庫預估完成日。

證據：[7個正常正式網址與完整收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v120/normal_seven_watch.json)、[10分類原始保留回應的日期與來源收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v121/retained_date_bound_sources.json)、[40個有界清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v121/history_query_preparation.json)、[實際40回應metadata](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v121/history_query_observations.json)、[獨立日期／total／ID比較](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v121/history_query_analysis.json)、[唯一HTTP測試及正常恢復](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v121/history_probe_operation.json)、[123檔完整回歸工作](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v121/regression_run.json)、[01:53當前完整工程驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v121/current_acceptance.json)、[鉅亨全部未存正文逐筆清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v121/cnyes_remaining.csv)、[歷史窗口與其他結果](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v121/cnyes_remaining.json)、[既有全934降序工作清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v120/work_order.csv)。

2026-10-13 **前次工程摘要（v120；01:16正式資料快照，鉅亨仍優先）**：維持**先補齊鉅亨，再依其他原始媒體的已知網址數降序開發／修復**；既有其他下載器繼續運行、中央社暫緩。01:11完整清冊包含934個來源鍵、6,206,004個已知URL heads；接續順序為CMoney272,717、UDN169,532、Global Voices110,642、Wantmedia108,094、MoneyLink103,026、CMNews64,965、MoneyDJ48,074等。網址數不是去重文章數，也不代替來源存取權限；聚合／轉址沿用原媒體路由，不另建重複正文管線。

**修復7個正文網址，已有5／7經正常排程正式補回。** 完整清點35份最新失敗原件後，找到來源把真正敘事段落放進`figure`、清理器卻連同圖框刪掉的共用根因。新解析只接受同篇ID、公開current內容、可見正文、完整schema、全部作者及來源時鐘一致的有界原生結構，按原順序保留所有段落與數值；明列圖說與圖片參照另存，不讀取圖片／OCR、不把社群標籤或占位字串冒充正文。28份其他原件仍未解，另4個身分問題沒有保留原件，不推定空白或已完成。

全部135份原始回應（70／12／18／35四個完整cohort）、雙parser、獨立7篇原文／作者／發布與修改時鐘／圖說／順序核對、四次claim→解析→接受或保留失敗→Parquet／fsync／收據→獨立讀回耐久流程**10／10接受**。原100份輸出及失敗結果保持原值，28份未解原件沒有產生正式正文。**122個維護news／FinMind／bulk測試檔、7,425項回歸通過**，不是全專案測試。最初聚焦與完整回歸揭露巢狀圖組／聲明邊界及24個社群分類回歸，修正後才發布；原失敗工作與log保留。完整135原件ABBA，關閉9.481／9.871秒、開啟10.899／10.201秒，開啟多保存7筆，**不宣稱同輸出加速、網路或官方速度上限**。

00:57僅一次正常drain／重載新聞service；00:58新capture41實載**7／7接受**。全部49個既有正式heads的正文／作者時鐘／capture／保存路徑分組獨立核對不變，66來源的permissions／bulk、其他65capture、原配置、70份診斷預算及來源重試資格不變；沒有重啟交易、其他金融下載器或唯讀gateway。01:16當前工程**7／7接受**，234個正常正文版本與5個新正式網址皆驗證，其他來源的實際失敗仍保留；不把工程runtime接受當成所有網站成功。

**01:16鉅亨全部7,020個未存正文URL逐筆列出：221活躍缺口＝192個未驗證primary候選＋29個無已知primary；另6,799個來源／路徑結果分開保存。** 後者含空模板4,615、查無1,269、純報價332、圖片-only7、身分2、路徑574，不是正文完成，也不能一概認定永遠不可取得。7個修復網址有同owner的唯讀observer持續核對正式收據，不額外發HTTP或重排。最後一筆既有重試資格為01:45:33，**不是整個鉅亨完成ETA或保證成功時間**。

**歷史查詢缺口另有1,820個單日分類窗口，不能漏在正文URL分母之外。** 完整SQLite清點10個分類及所有54,600筆頁面證據：每窗前30頁均有紀錄，尚未觀測／未排程的合法前30頁都為0；剩下是來源回報31至225頁但目前只驗證過最多30頁。主要為announcement723窗、tw_bull586窗、anue_live147窗、cn_stock137窗等，請求邊界最早2013-01-08 UTC；此日期是API查詢邊界，不是已證明的最早發布日。分類可重複收錄同篇，回報總筆數也不等於唯一文章數，所以**1,820窗不是1,820篇，缺幾篇與全部完成ETA仍未知**。不假裝小時切片必然有效、不刪缺口或重抓已驗證30頁；下一步優先驗證來源支援的窗口／查詢能力及跨分類article-ID覆蓋。

證據：[135原件與獨立完整鏈](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v120/implemented02.json)、[122檔完整回歸工作](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v120/regression_run02.json)、[capture41實載驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v120/loaded.json)、[01:16當前工程驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v120/current_attempt03/acceptance.json)、[5個新正式網址核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v120/current_attempt03/formal_repaired_heads.json)、[49個既有正式heads保存](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v120/current_attempt03/prior_bodies_current.json)、[全部鉅亨未存正文URL](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v120/current_attempt03/cnyes_remaining.csv)、[7網址正常排程觀察工作](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v120/formal_watch_run.json)、[完整歷史窗口與頁面驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v120/bulk_gaps.json)、[1,820窗口逐筆清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v120/bulk_gaps.csv)、[全934來源工作順序](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v120/work_order.csv)、[全來源唯讀清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v120/inventory.json)。全goal仍active；下列舊摘要是各自當時快照。

2026-10-13 **前次工程摘要（v119；00:22正式資料驗證，鉅亨仍優先）**：工作順序維持**鉅亨 → 其他原始媒體按已知網址數降序**，既有下載器繼續運行、中央社暫緩。00:19完整清點934個來源鍵、6,205,974個已知URL heads；鉅亨之後依序CMoney272,717、UDN169,532、Global Voices110,642、Wantmedia108,094、MoneyLink103,026等。這是網址數，不是去重文章數或已取得存取權限；聚合／轉址仍走既有原媒體路由，不新增重複正文管線。

**v118的11篇已全部正式補回，本輪另修正並正式補回1篇英文短訊。** WSL在10月12日23:38實際重啟；新聞服務23:41自動恢復，舊驗收observer中斷，沒有把它改寫成成功。以新的boot／owner證據核對10篇，其中8篇在重啟前取得、2篇在新owner取得；最後一篇於原00:00:19重試資格後正常取得，00:01:06驗收接受。隨後整批11篇重讀正式原文／作者時鐘／current-source重播／耐久收據，連同原37篇形成48個已驗證正式heads，不混算重啟前後吞吐。

本輪完整診斷剩餘27份原件，找到**JSON-LD把英文單字空格移除**的真實漏抓原因。來源可見正文與完整current內容保有相同原始空格、單字及數值；只在同篇ID／free payment0／原發布時鐘／整段可見與current內容一致、實際正文仍符合至少8個觀察字詞的既有門檻時恢復空格。不推測單字或數值分界，不忽略圖片／表格／figcaption，不把缺標題的報表、空模板、純報價或占位字串改作全文。100份原始回應的70／12／18三個完整cohort、兩種parser與四次claim→解析→保存或拒絕→Parquet／fsync／收據→獨立讀回流程**12／12接受**；目標標題／全部作者／發布及修改時鐘再用真正原始JSON-LD另行**6／6核對**。前43個成功結果、其餘26份未解結果與另30份歷史控制全部維持原值；相關**1,116項**與完整**121檔7,253項**回歸通過，不冒充全專案測試。

完整100原件ABBA耐久流程，關閉修正7.159／7.649秒、開啟7.492／7.564秒；開啟多保留1篇，時間區間重疊，**不宣稱同輸出加速、WAN或官方速度上限**。00:16只有一次平順重載新聞service／原timer，舊owner正常exit0、沒有force kill；00:18新capture40實載**7／7接受**。原48個正式heads分16／16／16獨立唯讀快照保留正文／作者時鐘／capture／保存路徑，不冒充單一原子快照。66來源的permissions／bulk、其他65capture、原配置、固定70份診斷預算、重試資格及同一唯讀gateway全部保留。

**本輪英文短訊在00:20:35由正常排程實際取得，00:21:02驗收正文71字、作者時鐘、capture40及原始耐久收據。** 00:22當前工程**7／7接受**，正常批次29個正文版本與48個既有heads皆驗證；同owner工程runtime接受不代表每個網站都成功或全歷史完整。鉅亨完整未存正文清冊目前**7,025個URL＝226活躍缺口（197個未驗證primary候選＋29個無已知primary）＋6,799其他結果**，151個AMP仍pending。其他結果包括空模板4,615、查無1,269、純報價332、圖片-only7、身分2與路徑574，不能算全文完成，也不能一概宣布永遠不可取得。來源／身分障礙仍在，全鉅亨完成ETA保持未知；本輪單一observer已完成，不再寫成仍等待重試。

證據：[實際boot與原觀察中斷核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v119/boot_reconciliation.json)、[v118全部11篇正式正文重讀](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v119/all11_final_formal_heads.json)、[100原件與完整耐久流程](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v119/implemented.json)、[獨立原始作者時鐘核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v119/fidelity.json)、[121檔完整回歸工作](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v119/regression_run.json)、[capture40實載](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v119/loaded.json)、[本輪單篇正式正常下載驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v119/normal_one_watch.json)、[00:22當前完整工程驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v119/current_attempt01/acceptance.json)、[鉅亨全部未存正文逐筆清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v119/current_attempt01/cnyes_remaining.csv)、[全934來源工作順序](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v119/work_order.csv)。

2026-10-12 **前次工程摘要（v118；23:20正式資料驗證，先完成鉅亨剩餘）**：已按使用者新順序重新清點全934個來源鍵；目前工程唯一優先是鉅亨，之後按原始媒體已知網址數降序接續CMoney、UDN、Global Voices、Wantmedia、MoneyLink、CMNews、MoneyDJ、LTN、ETtoday等。既有其他下載器繼續運行，中央社暫緩；聚合／轉址沿用原媒體路由，不另建重複正文管線。23:17:45的完整唯讀清冊中，CMoney有272,652、UDN169,520、Global Voices110,642個已知URL heads；這些不是去重文章數，也不代表來源已取得授權或全部已實作。

上一輪11個修復網址已於**18:34:49全數正式驗收**，不是仍有2筆卡住。本輪完整檢查原70份cohort中剩餘38份原件，另保留12／18份歷史控制，共100份實際回應；新增有界圖片葉節點、正文段落、未配對figcaption、來源署名、小標題、比較表標籤與風險聲明的完整順序核對。實際多層figure內的段落不再誤拒；完整文字、正負數、股數與連結按來源原位置保留，來源署名不變造文章作者，沒有圖片的figcaption不捏造配圖，圖片alt與OCR不供給正文。只有精確同篇可見DOM／current／完整schema及來源作者時鐘全部一致才接受，未知或隱藏結構仍保留缺口。

**本輪11份原件恢復可解析；23:20其中2／11個網址已由正常排程正式補回並獨立驗證**原始回應、完整正文、圖說／獨立附文、原作者時鐘、capture39與耐久收據。原70份已可解析43份；另外27份原件結果保持不變，不拿alias-only檢查不適用或原文短小當作全數不可取得。前32份成功輸出及另外30份歷史原件逐項保持原值，來源在正文其他真實位置重複「比較表」等文字也不刪除。兩種parser、全部100原件與四次完整claim／解析／拒絕保留或保存／Parquet／fsync／收據／讀回驗證**12／12接受**；**1,052項相關測試、7,189項完整news／FinMind／bulk回歸（原119維護檔全保留＋新檔＝120）通過**，不是整個專案全套測試。

相同100原件的完整耐久流程，修正開啟為6.390／6.104秒、關閉為5.896／5.832秒；開啟多保存11筆且維持所有原結果，**不宣稱同輸出倍速、WAN上限或官方最大頻率**。23:16僅一次平順重載既有新聞service／原timer；舊owner正常退出，23:18新capture39實載驗收**7／7接受**，沒有force kill或第二次重啟。全部37個既有正式heads以16／16／5的獨立唯讀快照核對，原文／作者時鐘／capture／原保存路徑未變，不冒充單一原子快照。所有66來源的授權／bulk與其他65capture、固定70份診斷預算、原重試資格及同一934來源唯讀gateway均保留。

**23:20鉅亨全部7,036個未存正文URL已逐筆列出，其中237活躍缺口＝207個未驗證primary候選＋30個無已知primary。** 其餘6,799個空模板、查無、報價／圖片-only、身分與路徑結果分開保存，不能算正文完成或一律不可修復；151個AMP pending仍有robots／502來源障礙，不以其他host的許可代替。當次正常批次15個正文版本與2個新正式網址、原37個正式heads、24啟用來源傳輸、全934清冊與當前runtime驗收接受；全部網站成功仍為false，不把局部接受當所有網站恢復。

11個新修復網址使用原single-owner唯讀observer持續驗收，不額外發HTTP或重排。原排程的最後一筆重試資格約在**10月13日00:00:19**，那不是完整下載ETA，也不是保證該秒成功；來源／解析／身分障礙未全部解除前，全鉅亨完成ETA保持未知。工程清冊保留所有未解問題及後續順序，沒有宣稱全歷史完成。

證據：[剩餘38份完整原件基線](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v118/before.json)、[100原件與全部角色／完整耐久流程](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v118/implemented02.json)、[120檔完整回歸工作](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v118/regression_run02.json)、[37個原正式heads重載前驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v118/prior_bodies_at_release.json)、[capture39實載驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v118/loaded.json)、[23:20實際工程驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v118/current_attempt01/acceptance.json)、[2個新正式網址完整驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v118/current_attempt01/formal_repaired_heads.json)、[鉅亨全部剩餘逐筆清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v118/current_attempt01/cnyes_remaining.csv)、[11個正常正式網址背景觀察工作](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v118/formal_watch_run.json)、[最新全934來源工作順序](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v118/work_order.csv)、[23:17全來源唯讀清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v118/inventory.json)、[前輪11個正式正文終局驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v117/normal_eleven_watch.json)。下列舊摘要都是各自當時快照，不是最新狀態。

2026-10-12 **前次工程摘要（v117；18:05正式資料驗證，鉅亨優先）**：工作順序保持「先鉅亨全部剩餘，再依其他原始媒體已知網址數降序」；已啟動的獨立來源繼續運行，中央社暫緩。接續正文下載器順序為CMoney、UDN、Global Voices、Wantmedia、MoneyLink、CMNews、MoneyDJ等；聚合／轉址沿用原媒體解析，不另建重複正文管線。前輪8個圖表原件已於17:40:52全部由正常排程取得，完成原始回應、正文、作者、來源時鐘與耐久收據的獨立驗收。網址heads不是去重文章數。

本輪不是抽樣檢查：原70個失敗網址cohort中尚未解析的49份原件全部逐筆核對，並另檢查27份標準canonical頁面，沒有把alias-only witness不適用誤說成來源不可取得。共用擷取器新增有界圖片／段落結構核對：完整保留一般段落、股數、正負數與連結的原順序；明列「圖文」及同篇可見span／alt圖說則分開保存，alt不供給正文。只在已失敗的原生頁面比較路徑增加處理，沒有圖片下載、OCR、作者／發布時鐘推測或新平行下載管線。

**11個原件恢復完整可解析；18:05其中9／11個網址已由正常排程正式補回並獨立驗證。** 其他38份原件結果保留。原70份現在32份可解析，這不是鉅亨全庫完整率。全部100份實際原始回應分70／12／18三cohort保留，不跨cohort去重相同URL；兩種parser、作者時鐘、圖說、全部參照及四次完整claim／解析／失敗保留或保存／Parquet／fsync／收據／讀回驗證接受。前21個已成功輸出與全部30份既有回歸原件未變。完整回歸抓出社群連結拒絕fall-through與未宣告chart標籤的誤判；條件已收窄，舊測試與失敗證據保留，最終**774項相關、6,997項news／FinMind／bulk完整回歸（原118維護檔全保留＋新檔＝119）通過**；不是整個專案全套測試。

同樣100原件整條耐久流程的兩次修正開啟為7.535／7.234秒，關閉為7.096／7.915秒；開啟多保存11筆，時間區間重疊，**不宣稱同輸出倍速、WAN上限或官方最大頻率**。17:56開始只平順重載原新聞service／原timer；capture38的新owner已實載且7／7載入檢查接受，26個既有正式heads、固定70原件診斷配額與全部66來源授權／bulk及其他65capture均已核對保留。18:05再核對17個正常批次、71個正文版本及9個新修復網址的實際原文、原作者時鐘、capture38與耐久收據，7／7當前工程檢查接受；全部網站成功仍為false，不因這輪接受清掉其他來源錯誤。

**18:05鉅亨全部7,040個未存正文網址逐筆列出，其中241活躍缺口＝211個未驗證primary候選＋30個無已知primary。** 其餘6,799個來源空頁、查無、純報價／圖片及仍需核對的身分／路徑分開保留，不能算正文完成或全部不可修復。11個修復網址的同owner唯讀觀察器已在背景運行，會等待完整正式資料驗收；剩餘兩筆當時只有原排程的重試資格，觀察器期限與重試時間都不是全庫完成ETA。來源／解析／身分障礙未全部解除前，完成ETA保留未知。

證據：[完整49原件基線](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v117/before.json)、[27標準頁面原件](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v117/standard_original_shapes.json)、[最終100原件與四輪完整流程](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v117/implemented03.json)、[完整119回歸工作](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v117/regression_run03.json)、[26既有正式heads重載前核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v117/prior_bodies_at_release.json)、[capture38實載驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v117/loaded.json)、[18:05當前工程驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v117/current_attempt02/acceptance.json)、[9個正式修復網址驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v117/current_attempt02/formal_repaired_heads.json)、[鉅亨全部剩餘逐筆清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v117/current_attempt02/cnyes_remaining.csv)、[11個正式網址背景觀察工作](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v117/formal_watch_run.json)、[全934來源工作順序](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v113/work_order.csv)、[前輪8個正式正文驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v116/normal_eight_watch.json)。下列舊摘要都是各自當時的快照，不是最新狀態；全部goal仍active。

2026-10-12 **前次摘要（v116；16:52正式資料驗證，鉅亨優先、中央社暫緩）**：工程順序仍是先處理鉅亨全部剩餘，再依其他原始媒體已知網址數降序；轉址／聚合沿用原媒體路由、不重複建立正文下載器。v115修正的13篇已於16:27全部通過正式驗收。本輪完整分析57份尚未解析的原件，修復宣告型日K／走勢圖、多層figure及空白占位造成的schema誤拒，**8篇原件恢復完整可解析，其中3／8篇已由正常排程正式補回並獨立核對原文／圖說／原作者時鐘／capture37／耐久收據**。其他49份原件失敗結果保留，不代表已解决；沒有下載或解讀圖片數值。

508相關測試及 **6,855完整news／FinMind／bulk回歸（118維護檔）**通過；全部100份實際回應分三個不去重cohort、雙parser、四輪完整解析／接受或保留失敗／Parquet／fsync／收據／讀回驗證。只平順重載既有新聞service／原timer；新owner16:40啟動、16:44已running，原18篇正式正文及全部66授權／bulk／其他65capture不變，沒有重啟交易、其他金融下載器或gateway。

**16:52鉅亨全部7,055個未存正文URL逐筆列出，其中256活躍缺口＝226個未驗證primary候選＋30個無已知primary。** 其餘6,799個來源空頁、查無、純報價／圖片，以及仍需核對的身分／路徑，不能算正文完成或一律不可修復。新修正的5篇仍待原排程，最晚17:40:00只是重試資格，不是整個鉅亨完成ETA；共用唯讀observer繼續驗到8篇。詳見[宣告型圖表參照與完整正文回補](#宣告型圖表參照與完整正文回補)、[最新全部鉅亨剩餘清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v116/current_attempt02/cnyes_remaining.csv)及[全934來源工作順序](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v113/work_order.csv)。全goal仍active。

2026-10-12 **前次摘要（v115；15:52當時，鉅亨優先、中央社暫緩）**：固定70個失敗URL的原件已全部保存並獨立核對，沒有新增診斷HTTP。修正同篇舊圖片註解殘片造成的schema誤拒：完整可見原文、作者、發布／修訂時鐘不變，檔名僅記為來源參照，不捏造圖片內容。全部70份重播有13篇恢復可解析；其中 **10／13篇已由正常排程正式取得並通過原件／正文SHA／capture36／作者時鐘／耐久批次驗證**，另外3篇仍待原重試資格，沒有提前清期限。434相關測試及 **6,699完整news／FinMind／bulk回歸**通過，全部66授權／allowed／bulk與其他65capture契約不變。

15:31只平順重載新聞service／原timer，15:36新owner正式running；啟動的本機驗收恢復／metadata等工作耗時，觀測逾時保留，不再次重啟或把逾時說成服務失敗。15:52核對正常批次的225正文版本；鉅亨已知3,595,357個URL heads、已存3,588,295個正文heads，**全部7,062個未存正文URL逐筆列出，其中263活躍缺口＝233個尚未驗證primary候選＋30個無已知primary**。其餘6,799個來源空頁、查無、行情／圖片及身分／路徑結果不能冒充正文完成，也不是全數不可修復。13篇共用唯讀observer持續核對原排程產出；最晚16:27:31僅是既有重試資格，**不是全部完成ETA**。

新owner在15:39基線以前補回8篇；15:43的288.195秒DB窗口鉅亨新增0／Global Voices27個正文URL heads，原runtime false保留。15:52相同基線的782.950秒DB窗口鉅亨新增20／Global Voices70個正文URL heads，13項runtime接受；這不是所有網站已恢復，CMoney真403／0正文仍保留，未達期冷卻來源由原共用契約區分。兩個窗口都不能把基線以前8篇再加進去宣稱速度提升。詳見[舊圖片註解殘片、完整原文與正式回補](#舊圖片註解殘片完整原文與正式回補)、[最新逐筆鉅亨剩餘清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/current_attempt02/cnyes_remaining.csv)。其他網站仍依[全934工作順序](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v113/work_order.csv)的已知網址數降序，只有鉅亨是目前工程優先；既有下載照常執行，全goal active。

2026-10-12 **前次摘要（v114；14:40資料驗證、14:51服務仍運行）**：工程工作維持鉅亨唯一優先；沒有新增其他網站下載器，既有其他來源照常追新，中央社暫緩。根治已收到失敗回應卻因診斷額度耗盡而沒有留下原件的缺口：對固定70個已知URL新增耐久、有界的原件保存，沿用正常owner的既有回應，**不增加HTTP請求、不重排、不清來源拒絕期限，也不把原件叫正文**。22份正常新原件的SHA、收據、原URL、存取證據已獨立核對；這22份仍是`body_not_found`，下一步用實際原頁修共用解析器。14:42心跳已保存26／70份，只有22份在本次獨立驗證集合內。

**14:39鉅亨仍有273個活躍缺文URL，全部7,072個未存正文URL逐筆列出。** 另外6,799個包括來源空頁、查無、報價／圖片，以及需核對的身分／路徑；不是6,799篇正文完成或全部不可修復。新聞service於14:30平順恢復並載入修正，14:51仍active／NRestarts0。15個正常批次／96個正文版本、五篇先前正式正文及作者／時鐘驗證；同owner資料窗口鉅亨新增3個正文URL heads。**6,608完整news／FinMind／bulk回歸通過**，30份既有原件雙parser／完整耐久鏈及全部66來源契約保持。

全來源當窗驗收仍是false：CMoney有真正403且沒有新正文，保留原失敗，不因鉅亨保存修復宣稱所有網站恢復。全部完成尚無可信ETA。詳見[Finite診斷配額與鉅亨原件回收](#finite診斷配額與鉅亨原件回收)、[全部鉅亨未存正文逐筆清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v114/current_attempt02/cnyes_remaining.csv)。鉅亨處理後，仍按[全934來源工作順序](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v113/work_order.csv)的已知網址數降序開發／修復，排名不代替來源存取與用途核對；全goal仍active。

2026-10-12 **前次摘要（v113，13:44當時；鉅亨優先、中央社暫緩）**：依最新要求調整工程開發／修復順序，先完成鉅亨全部已知剩餘URL的正文取得與逐筆來源結果核對，再按其他來源的**已知網址數由多到少**接線或修復。完整934來源鍵仍保留；Google／Yahoo等轉址、聚合來源另走既有原始媒體解析，不另建重複正文管線。既有下載與追新不中斷，沒有重啟服務、改HTTP配額、清來源拒絕期限或啟用中央社。共用planner讀取[工作順序設定](../configs/news/work_order.json)產生version6；未指定新順序的歷史version5口徑仍保留。
**13:44鉅亨3,588,238個正文URL heads；7,072個未存正文URL＝276個活躍缺口（243個未驗證primary候選＋33個無已知primary）＋6,796個其他結果。** 其他結果含來源空頁／查無／純報價／純圖片，也含仍需核對的身分與路徑問題，不代表6,796篇正文已抓完或永遠不可修復。[全934工作清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v113/work_order.csv)及[鉅亨全部未存正文逐筆清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v113/cnyes_remaining.csv)已輸出，沒有用抽樣替代完整剩餘集合。
85項順序／來源規則回歸與[9項當前資料驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v113/acceptance.json)通過；同owner13項runtime、24來源傳輸觀測、243個新正常正文版本及原始耐久收據接受，五篇既有正式正文／作者／時鐘不變。這是實際正常產出與順序驗證，不是所有來源故障已消失。v111六筆quote已於13:09全由正常排程分類；v112共享image outcome2已載入，6,465完整／1,026相關回歸通過，五筆image案例目前3／5正常分類接受，另兩筆仍等原期限。分類不是正文下載成功。全goal仍active，詳見[鉅亨優先與按已知網址量的工程順序](#鉅亨優先與按已知網址量的工程順序)。

2026-10-12 **前次摘要（v111，12:27當時；中央社先不處理）**：修正6個實際鉅亨舊頁只有報價字串、卻被當成未知缺文反覆重試的共用分類根因。可見／current／schema、同頁身分、免費狀態及來源時鐘必須一致；附有敘事、媒體／table、付費、不同身分或不明video宣告仍不接受。12個保存原件完整重播，僅這6個變為`source_body_quote_only`，不是下載到6篇正文；其他6個待查原件及35個無原件URL不假裝解決。
581相關及**5,925完整news／FinMind／bulk測試**通過；12個既有正文、作者與時鐘完全不變，兩種parser的正／負完整耐久鏈接受。12:19平順重載新聞service／原timer，僅Cnyes HTML capture的quote outcome5→6，native正文parser35／bulk／全部66授權／其他65來源不變，既有收據不重標；沒有重啟交易或唯讀gateway。後續同owner13項runtime及24來源傳輸證據接受，15個新正常批次／41個正文版本獨立讀回。
**12:27鉅亨3,588,213正文URL heads、仍290缺文＝243個未驗證primary候選＋47個無已知primary。** 六筆正式分類尚等原排程於12:46–13:09具備重試資格；已啟動不發HTTP／不改queue的有界觀察，不把觀察期限或重試時間叫完成ETA。449.708秒DB窗口鉅亨＋1／Global Voices＋40正文heads，HTTP另449.806秒鉅亨0.367req/s，含索引304與真失敗，非WAN倍速或官方最高速。AMP502、CMoney／PeoPo拒絕與其他缺口保留；全來源goal仍active。詳見[報價片段分類與正式排程分開驗證](#報價片段分類與正式排程分開驗證)。

2026-10-12 **前次工程摘要（v110；中央社先不處理）**：修正「AMP robots 已按入口隔離，容量控制卻仍把其502算成整個鉅亨負載錯誤」的共用根因。只有同來源其他登錄主機在120秒內真正完成回應，才隔離該REP502／504的容量壓力；原錯誤、blocked入口與重試期限照留，429／Retry-After／拒絕／未知或相關故障仍保護整個provider。5,780項新聞／FinMind／bulk及308項唯讀gateway測試通過；11:38:45載入量測ABI11，全部66來源capture／授權／bulk不變，沒有重啟交易服務。
正常45.749秒窗口完成31次請求、4個新正文URL／12個耐久版本；含真AMP502，健康入口保留3路並行，原錯誤不算成功。後續12批／77個完整正文版本獨立讀回接受；這不是相同WAN工作量的倍速證明，也不是舊缺文已補完。11:43:39鉅亨存3,588,202個正文URL，仍290缺文（243個已有桌面正文候選、47個無已知primary）；另一個25.10秒標頭前ReadTimeout仍保留、自動重試，當時正以2路做有界恢復，不冒稱所有錯誤已修好。934鍵／39頁的四寬度真瀏覽器驗證接受，ETA支援新11及既有10，沒有頁面觸發publisher請求。詳見[REP故障範圍與正常入口容量修復](#rep故障範圍與正常入口容量修復)。

2026-10-12 **前次工程摘要（v109；中央社先不處理）**：新增兩類鉅亨共用解析修正：社群連結前的NBSP占位段落、包在外層圖組裡的圖片／圖說。兩篇已由正常排程於 **11:00:02／11:03:34**正式取得 **1,054／1,081字**，原作者、秒級發布／修訂時鐘、圖說與所有原連結保持來源值；沒有提前清重試期限。上一輪三篇也已於10:21:58／10:48:05／10:56:52全部正常回補，native34舊收據不重標為35。
18份相同原件、兩種parser與六次完整寫入／收據／獨立讀回接受；新增122項語義測試，2,441項native相關與 **5,747項完整news／FinMind／bulk回歸**通過。10:58僅平順重載新聞service／原timer，新owner載入capture35；其他65來源及全部66授權／allowed／bulk契約不變。全934鍵進度頁四寬度驗收接受，沒有頁面觸發publisher呼叫。
**11:01清冊：鉅亨3,595,249個已知URL＝3,588,176個正文URL＋291個待處理缺文＋6,782個其他來源結果**，不是幾百萬篇都尚未下載，也不是全網唯一文章完整率。後續只讀診斷確認151個pending位於AMP主機，受robots端點502阻塞；其他缺文有解析／身分／傳輸失敗或尚未到期。338.470秒DB窗口鉅亨＋7、Global Voices＋30正文URL；341.761秒HTTP窗口鉅亨0.363req/s，含大量索引304與失敗重試，不能當成最高速度或跨窗口倍速。其他provider的403／502等原拒絕與缺口保留，全 **934鍵／66設定／24啟用／880未個別審查**及全歷史goal仍active。詳見[占位圖組回補與鉅亨缺口分母](#占位圖組回補與鉅亨缺口分母)。

2026-10-12 **前次工程摘要（v108，10:30當時；中央社先不處理）**：修正鉅亨三類實際正文結構：連結圖片／巢狀圖說、含22段完整敘事的圖組，以及已知風險聲明搭配NBSP占位圖組。不是把`figure`一律忽略或放行：原生頁身分、整棵DOM、可見正文與當前schema要一致，保留所有原始財務數字、署名、發布／修訂時鐘、圖說與連結參照。16份原件兩種parser完全一致，三個缺文原件恢復 **1,310／2,961／1,400字**，其他七篇原正文與六個未接受結果不變；六次完整耐久測試鏈接受，沒有HTTP或正式queue寫入。
**176新增語義／906相關／5,625完整news含FinMind與bulk回歸**通過，capture33→34，其他65來源及全部66授權／allowed／bulk契約不變。10:12:40僅重載新聞service／原enabled timer；新owner已載入34與原status2，實際心跳1,993,736 bytes、934鍵唯讀頁正常。四寬度local頁各934鍵／39頁、0JS錯誤及0publisher requests接受。
**`4841220?exp=a`已於10:21:58由正常排程正式補回1,400字**，原署名Anne陳巧諭、2022來源秒級時鐘、capture34、raw／SHA／所屬64版本耐久批次與current replay全部接受。另兩篇仍由正常排程依原期限追蹤，**10:48:03／10:56:50是最早重試資格，不是完成ETA**。10:21同owner的324.699秒DB窗口鉅亨新增548／Global Voices34個正文URL heads；HTTP另335.657秒窗口鉅亨2.118req/s，不是同cohort WAN倍速或官方最高速。
首個新owner觀測窗仍真有CMoney403，原false runtime／failed run完整保留；後續窗口依既有判準排除基線已冷卻來源，不宣稱CMoney已恢復。兩個標頭前transport failure sample中一個有真正較晚正文、另一個仍未解；amp robots502、PeoPo403與其他短內容／圖組／權限／歷史缺口保留。全 **934鍵／66設定／24啟用／882未個別審查**及全歷史goal仍active。詳見[敘事圖組完整正文與原重試期限驗證](#敘事圖組完整正文與原重試期限驗證)。以下摘要保留各自當時的觀測，不是目前owner或速率。
10:29:41最新 **19項工程closure全部接受**：後續481.208秒DB窗口鉅亨＋817／Global Voices＋33正文URL heads，獨立473.688秒HTTP窗口鉅亨2.139req/s；最新128批／3,108版本。實際failure sample擴為23個，其中17個已有較晚正式正文、6個尚未驗證恢復，原10:21的2個sample／1恢復證據不改寫。另兩個目標watch的實際process身分與新鮮觀測已核對，仍不算正式下載完成。

2026-10-12 **前次工程摘要（v107，09:42；中央社先不處理）**：前輪三篇鉅亨正文已全部由正常排程正式補回：`232219` 786字於08:57:19、`190469` 1,193字於09:11:39、`m4937652` 888字於09:20:07；原件／作者／來源發布與修訂時鐘／capture33／完整耐久批次與當前解析器驗證接受，沒有提前清重試期限。另根治本輪發現的進度頁故障：原新聞心跳超過未變更的 **4 MiB讀取界線**，並非下載停止。沿用原owner心跳，改為compact UTF-8 JSON、最新64個連續容量窗口與64筆缺口樣本＋精確分組總數；完整128窗口與所有缺口仍在原calibration／SQLite權威資料中。
同一owner、同一WAL快照、交錯五輪完整心跳實測：**4,351,402→2,244,821 bytes**，934個來源的所有公開數量／狀態／時鐘／ETA完全相同；CPU中位0.693322→0.670689秒，僅約1.034倍的本機心跳差異，不宣稱下載倍速或最高源端速率。**287相關／5,449完整news含FinMind與bulk回歸**通過；09:31:40僅重載新聞service／原timer，source33及全部66 capture／authorization／allowed／bulk指紋不變。09:33實際心跳2,216,003 bytes、934鍵恢復；四寬度local頁各39頁、0 publisher requests通過。09:35的94.358秒DB窗口新增鉅亨 **80**／Global Voices **10**／CMNews **2** 正文URL heads；13項runtime與128批／3,178正文版本接受。
CMoney來源403的原證據及 **09:43:39起可重試**期限完整保留，新owner本窗口沒有請求CMoney、更沒有取得全文。官方五個RSS／頭條操作描述已記入私人清冊：兩個列commkey/memberPk，另外三個未列憑證參數，**都沒有全文欄位**，不能用目錄替代缺失正文或從文件可見推斷匿名權限／完整歷史／配額。本輪UI原失敗與診斷helper的空窗口誤設均保留失敗收據並附較晚成功驗證，未弱化驗收。09:35時1個真失敗尚無較晚正文；**09:42最新17項closure接受**，449.341秒DB窗口鉅亨＋336／Global Voices＋45，另461.906秒HTTP窗口鉅亨1.126req/s，最新128批／2,825版本。7個真失敗sample已有6個較晚原文與耐久收據，1個仍未解；標頭前逾時／amp robots502與其他圖組仍保留。全 **934鍵／66設定／24啟用／882未個別審查**及全歷史goal active。詳見[新聞心跳有界投影與正常正文正式驗收](#新聞心跳有界投影與正常正文正式驗收)。

2026-10-12 **前次工程摘要（v106，08:59當時；中央社先不處理）**：修正鉅亨兩類有原件可驗證的正文拒絕：圖組裡的來源署名，以及舊式表格／圖片參照標記。16份原始回應的兩種parser重播、六次完整耐久管線驗證接受；其中三個新修復原件分別為 **1,193／786／888字**，保留原作者、來源發布／修訂時間與參照，不捏造圖片或表格值。105項新語義測試、362相關與 **5,439完整news含FinMind／bulk回歸**接受；capture升為33，其他65來源與全部66授權／allowed／bulk契約不變。08:42:29僅重載新聞service／原timer，正常owner已載入；**`232219` 的786字正文已於08:57:19正式落盤，08:59原件／作者／來源時鐘／body SHA／capture33／所屬耐久批次驗證接受**；另外兩篇當時仍待原重試期限，不能把原件重播成功當已下載。
08:53的386.296秒DB窗口新增鉅亨 **377**／Global Voices **37** 正文URL heads；鉅亨0.976head/s，獨立397.431秒HTTP窗口1.402req/s。128近期批／3,774正文版本獨立驗證；全24啟用來源的傳輸／capacity及10個真故障sample中7個較晚正文恢復均保留。首次08:47 runtime僅12／13：**CMoney實際403／0正文，原false收據不改寫**。後續13項僅驗證基線可執行來源，依原共用契約排除已有未到期來源冷卻，絕非CMoney或全provider已修好。934鍵／66設定／24啟用／868未註冊／882未個別審查及四寬度local唯讀頁保留，0額外publisher requests。
08:55的17項工程closure接受；三個目標維持原期限，由既有正常排程與有界唯讀watch核對，**08:53／09:06／09:16只是最早重試資格，非完成ETA**。CMoney403、真正25秒標頭前ReadTimeout／502、其他圖組／空頁證據與混合capacity窗口仍未解，全934來源／所有歷史／理論最高速未完成，總goal active。詳見[原始來源署名與舊式參照標記恢復](#原始來源署名與舊式參照標記恢復)。以下摘要保留各自當時觀測。

2026-10-12 **前次工程摘要（v105，08:07；中央社先不處理）**：修正全部provider共用的來源閘門退避：`provider_cooldown`、`host_gateway_cooldown`、`robots_origin_policy_cooldown`及`stopping`，只有HTTP原生明確標記的等待，才不消耗真正下載失敗的burst；保留來源期限／原錯誤與原計數，不再因等待閘門而另加一小時。429／403、Retry-After、真正timeout／502、解析失敗及既有legacy期限不重設、不繞過。**226相關、5,334完整news含FinMind／bulk回歸**及16次相同受控來源的新舊整鏈耐久驗證接受，全部66 capture／authorization／allowed／bulk指紋不變。
07:59:35只重載新聞service與原enabled timer；新owner載入request-deferral contract1。08:05的273.092秒DB窗口新增鉅亨 **163**／Global Voices **19** 正文URL heads；鉅亨0.597head/s，獨立277.924秒HTTP窗口1.371req/s。128近期批／2,874正文版本、13項runtime與08:07階段closure接受；新owner的5個傳輸失敗sample都有真正較晚正文收據，**不是全部舊失敗已修復**。本次正式owner尚未觀察到gate-deferral事件，不能把受控modelled等待改善宣稱為WAN速度提升或實際已省一小時。
08:02全來源清冊及934鍵計畫保留66設定／24啟用／868未註冊／882未個別審查，已知6,202,692個URL／已存3,699,007個正文heads／2,130,658個等待授權URL；它們不是去alias唯一篇數或所有歷史。四寬度local進度頁934鍵／各39頁、0publisher requests接受。真正25秒標頭前逾時、502與舊`body_not_found`仍有未解；metadata／body混合capacity窗口與其他來源規劃繼續，總goal active。詳見[來源閘門等待不消耗下載失敗次數](#來源閘門等待不消耗下載失敗次數)。以下較早摘要保留其當時觀測。

2026-10-12 **前次工程摘要（v104，07:36；中央社依指示先不處理）**：修正鉅亨社群連結圖組被當成正文差異而拒絕的原生版型；正常下載器已於07:09:29正式補回 `4778922?exp=a` 的 **1,756字正文**，另保留128字來源附註／三個原連結、原署名 `Anne陳巧諭` 與來源秒級發布／修訂時鐘。另一頁 `1673421` 的26字行情片段不是新聞正文：原件重播接受正確分類，**正常排程也已於07:26:30正式分類為 `source_body_quote_only`**，正文仍為null，不再做缺文重試；07:36交接readback才觀察到此較晚結果，原07:24待重試證據不改寫。
定位到11.5萬項快取目錄盤點堵住主抓取迴圈，改為可續接的有界切片，已存在項目不再逐檔stat／重寫；另用精確partial index移除metadata日期修補的無效全表掃描。同一 **115,616項目快照** 的ABBA中，盤點最長單次堵塞由 **1.43–1.68秒降至12.5–14.8毫秒**、索引內容完全一致；正式owner完成29個切片，最長 **29.4毫秒**。這是本機metadata工作量測，**不是WAN下載倍速、完整啟動時間或官方速率上限**。
**838解析／時鐘、272恢復／效能相關、5,307完整news含FinMind／bulk回歸**通過；07:21:49只重載新聞owner與原enabled timer，07:28的13項階段驗收接受。07:25觀測146.940秒DB窗口新增鉅亨86／Global Voices12個正文URL heads；鉅亨0.585head/s，獨立138.228秒HTTP窗口1.317req/s。128近期批／2,727正文版本與四寬度local唯讀頁934鍵／39頁接受、0publisher requests。來源仍有標頭前約25秒ReadTimeout、mobile斷線與502；未清拒絕證據、未宣稱全錯誤解決或達到最大速度。全來源規模仍 **934鍵／66設定／24啟用／868未註冊／882未個別審查**，總goal active。詳見[鉅亨社群圖組原文恢復與主迴圈有界維護](#鉅亨社群圖組原文恢復與主迴圈有界維護)；以下v103及更早均為當時觀測。

2026-10-12 **前次工程摘要（v103，06:42；中央社依指示先不處理）**：台中38篇／彰化1篇既有原始連結，已全部由正常新聞owner正式取得，39筆原件、完整正文共34,087字、原始日期、機關／聯絡人角色及耐久批次逐筆重播驗證通過。補原生機關版型、台中canonical缺省但三組分享指標一致的已觀測變體，保留來源拼字，沒有把聯絡人當記者、把期間終點當發布時間，或下載照片。共用明確enqueue／fetch改為整批網址寫入後才初次凍結；既有收據及1篇原始台中範圍不重寫，另以16／16／5篇明確追加37篇。**257項相關回歸、5,181完整news含FinMind／bulk回歸**通過。
06:27:59恢復單一新聞owner與原enabled timer；13項runtime與128近期批／4,054版本驗證接受，24啟用來源有傳輸觀測，兩個故障取樣都有真正較晚正文收據。934鍵／66設定／24啟用／868未註冊／882未個別審查完整保留，既有64來源capture／authorization／bulk未變。台中500筆RSS與彰化10筆RSS僅為已實測回傳量，**不是完整歷史、全文或官方上限**，未啟用為批次正文來源。四寬度local唯讀頁包含兩個新provider、934鍵，0publisher requests。
鉅亨仍有實際502／傳輸恢復與兩筆正文表示差異：06:03觀測36.6分鐘新增2,418個URL heads／1.101head/s；新owner的06:34五分鐘窗口新增141／0.473head/s，處於`recovering_transport`，不能宣稱已達最大速度或沒有錯誤。中央社維持暫緩，全來源／全歷史與全域ETA仍未完成，總goal active。詳見[官方市政新聞原生接線與初次凍結競態修復](#官方市政新聞原生接線與初次凍結競態修復)。以下v102及更早摘要為當時觀測，不是目前owner或速度。

2026-10-12 **前次修復與實際下載摘要（v102，05:34；中央社先不處理）**：兩篇先前因版型誤判失敗的鉅亨文章，已由正常排程正式補回：`4631268?exp=a` 的671字／四組圖說於 **05:14:15**取得，`3947637?exp=a` 的762字／一組圖說與一個空圖說圖表參照於 **05:20:15**取得。原件、完整正文、原署名／時鐘、當前解析器及所屬60／18版本耐久批次皆驗證通過，沒有提前清重試期限或另開來源請求。**450解析回歸、95清冊／review回歸、5,072完整news含FinMind／bulk回歸**及16項階段closure通過。
05:19:36只重載新聞服務至HTML31；05:34最新13項runtime及128近期批／**5,173正文版本**接受。811.534秒DB窗口新增鉅亨 **1,088**／Global Voices **78** 個正文URL heads；鉅亨平均 **1.341 head/s**，另811.675秒HTTP counter窗口 **1.860 req/s**，不是去alias篇數、同cohort倍速或官方上限。本輪5個傳輸故障取樣全部有真正較晚正文收據；其他缺文、來源拒絕及歷史範圍仍未解。
同步修正全來源planner漏算「本機已有原始連結但未入佇列」：report **4→5**將 **192個來源鍵／946,017個literal alias關聯**獨立列為審查／去重工作，不當作正文或零請求工作。台中38個／彰化1個既有原始連結已有主站政策、精確文章與官方RSS/API目錄原件，但本輪只有審查註記，**未啟用正式抓取**。934鍵／64設定／22啟用／870未註冊／882鍵未個別審查仍保留；全來源完成估時未知，總goal仍active。詳見[空圖說圖表參照正式補回與未入佇列來源清冊](#空圖說圖表參照正式補回與未入佇列來源清冊)。

2026-10-12 **前次工程摘要（v101，04:54當時；中央社仍暫緩）**：從正常下載器留存的 `4631268?exp=a` 原件，確認128字原敘事段落被包在圖說內，舊圖片排除規則誤刪它；另修正一般圖說與巢狀圖說混用時的圖片配對順序。只接受同篇visible／current完整結構與全schema一致的已觀測形狀，671字原正文、四組原圖說、署名及來源發布／修訂時鐘均保留。**535針對解析測試、103共用驗收／observer測試、5,005完整news含FinMind／bulk回歸**通過，12項實物guard與六次隔離耐久重播接受。
04:43:38只重載新聞服務至HTML30；13項runtime與128近期批／**5,724正文版本**接受。341.583秒DB窗口新增鉅亨217／Global Voices32個正文URL heads；另338.090秒HTTP窗口鉅亨1.062req/s，不是同cohort倍速或官方上限。三個新逾時樣本已有兩個真正forward正文收據，第三個仍缺正文。**那篇671字仍未正式重新下載**，保留原 **05:14:13起可重試**期限；共用唯讀observer已在04:51啟動，正常owner取得後自動驗原件／正文／圖說／時鐘／耐久批次，不插隊或另開HTTP。04:54:51全14項**工程**closure通過，不宣稱正式補回；`1580057`缺文、其他舊故障與全934來源工作繼續。詳見[圖說包裹原敘事段落、完整順序與自動正式驗收](#圖說包裹原敘事段落完整順序與自動正式驗收)。

2026-10-12 **最新完成摘要（v100，中央社仍暫緩）**：鉅亨 `m4898771` 的來源將整篇正文與基金警語放在一個外層 `<figure>`；舊圖片排除規則誤刪全文，並非網路拒絕。沿用共同解析器增加有來源證明的外層容器比較視圖，保留全部13段、原數值、基金警語、作者與發布／修訂時鐘；**273針對測試、4,867完整news含FinMind與bulk回歸**通過。只重載新聞服務至HTML29；正常排程已在 **04:15:52.789704** 實際取得 **2,630字**，原件／所屬24版本耐久批次／current parser／精確queue head接受，沒有提前清重試期限。552.288秒DB窗口新增鉅亨 **401**／Global Voices **26** 正文URL heads，最新128批 **5,958版本**無誤；這是當窗觀測，不是倍速或官方上限。新owner的一個標頭前逾時也有實際forward正文收據，前owner其他未解樣本不撤銷。
另清點Wantgoo452、Wearn537、財訊快報551個既知URL的來源規則與歷史識別：保留等待權限／審查，不把Big5解碼錯誤當無法存取，也不把舊AJX群組頁回傳的最新文章倒填為歷史正文。三個provider共 **1,540** 個URL均未冒充已取得；私人診斷設定已關閉，原64來源授權、其他63來源capture、CNA暫緩與前次closure保留。全 **934** 鍵／64設定／22啟用／870未註冊，**884鍵仍未個別審查**；總goal active。詳見[鉅亨敘事圖片容器與財經來源歷史識別](#鉅亨敘事圖片容器與財經來源歷史識別)。

2026-10-12 **前次完成摘要（v99，中央社仍暫緩）**：政大唯一已知文章經canonical robots／原件診斷，修正generic容器造成的`body_not_found`，已由正式共同owner於 **03:32:26.082032** 補回 **525字**；完整原文、林亭／秘書處署名、`2021-03-20`日期精度、raw／56版本所屬耐久批次與current replay全部接受，不下載SSU照片或補不存在的發布時分。既有63來源指紋、16個原有限cohort與補充集合不變；目前 **934來源鍵／64設定／22啟用／870未註冊**，政大單篇有限進度在四寬度共用頁實測顯示 **1/1**，不是政大全歷史完成。
新增診斷owner scope防呆：已啟用的相同host不能藉另一root另開HTTP／限流額度；真正CLI拒絕發生在HTTP構造前，0請求／0主佇列／0診斷額度寫入。**277針對測試、4,771完整news含FinMind與bulk回歸**通過。新owner248.384秒DB窗口新增鉅亨 **191**／Global Voices **25** 正文URL heads；另244.981秒HTTP窗口鉅亨1.233req/s，不能混成同一cohort或宣稱官方最高速。兩個新傳輸失敗樣本皆有真正forward正文收據；前次四個`body_not_found`與更早未解證據仍保留。全934計畫仍 **887鍵未個別審查**；總goal active。詳見[政大原生正文與診斷owner分工](#政大原生正文與診斷owner分工)。

2026-10-12 **前次完成摘要（v98，中央社仍暫緩）**：投信4564769?exp=a於 **03:08:22.231435** 按原03:08:17期限正式補回281字，原件／作者／發布及修改時鐘／58個所屬耐久版本／current replay全部接受；精確空P另算1個來源無正文狀態。受監督唯讀接續自動完成8個驗收步驟，**41項正式交接＋12項最新closure**於03:09:53接受，沒有額外publisher HTTP、queue mutation或重啟。
最新2,113.433秒DB窗口新增鉅亨 **2,182**／Global Voices **210** 正文URL heads；13項runtime／128最新批 **4,726版本**接受，四寬度934來源／各39頁通過。27個傳輸失敗樣本有 **23個真正forward正文收據**，另外4個目前body_not_found仍保留，不撤銷前owner的未解證據、不宣稱全來源或官方最高速。政大審查仍未啟用；全934來源／全歷史目標active。詳見[投信正常回補與自動正式驗收](#投信正常回補與自動正式驗收)。

2026-10-12 **較早觀察摘要（v98，02:56）**：source28 正常 owner／正式 observer 都持續執行，沒有提前改 core、重啟或清除原重試期限。02:56 的 1,313.938 秒 DB 窗口實際新增鉅亨 **1,392**／Global Voices **130** 正文 URL heads；13 項 runtime／128 最新批 **4,683 版本**接受。21 啟用來源傳輸樣本中，鉅亨 15 次逾時已有 **12 個實際 forward 正文收據**，3 個未解仍保留，不把 pool reset 叫全部恢復。
政大唯一已知來源新增官方授權／署名／日期的**審查註記**，尚缺 canonical robots 與 immutable raw，**未啟用、未取得正式正文**。12 項 review-only guard／70 項 maintained 測試接受，公開頁自動呈現審查狀態；所有63來源 capture／授權 fingerprint、934來源鍵、owner與精確queue列不變。全934計畫重新產出，887個鍵仍未有個別來源審查，沒有把規劃當實作完成。當時投信文章仍待 **03:08:17 起正常派送**，不是已補回或保證完成時間。詳見[政大來源審查與不中斷的實際下載觀察](#政大來源審查與不中斷的實際下載觀察)。

2026-10-12 **前次續修摘要（v97，中央社仍暫緩）**：source27 的單行聲明正文已在 **02:19:11** 正常補回；符號 stub 與空白版型分別在 **02:27:44／02:26:06** 正確分類，只有 1 篇實際正文，另 2 筆不算文章。**39 項正式交接＋11 項最新稽核**接受；128 最新批／5,046 版本／0 errors，42 個 transport 樣本中 40 個有真正 forward 正文收據，2 個仍未解。
再從正常 owner 留存的原件修正「投信」買賣超清單被外資-only heading 誤拒，以及精確空 `<p>` 被無效重試；數字、U+FEFF 原格式、作者與來源發布／修改時間均保留。**103 隔離測試、545 針對回歸、4,712 全 news 含 bulk、26 原件／25 URL／78 耐久版本 ABBA**接受；整鏈沒有明顯倍速，不宣稱官方上限。02:34:05 僅 graceful 切換 news 至 HTML28／empty7；32 項載入、40 項**工程**交接及四寬度進度頁接受。
新 owner 的 186.039 秒窗口新增鉅亨 **162**／Global Voices **18** 正文 URL heads；空 `<p>` 已於 **02:34:42** 正確分類。4564769?exp=a 保留 **03:08:17 起可重試**的原期限，尚未冒充正式補回；有界唯讀 observer 持續核對且 core／owner 變更即拒絕混用。中央社不查 API、不改金鑰、不啟用；全 **934** 來源／全歷史目標仍 active。詳見[投信清單、空段落與上一版正式驗收](#投信清單空段落與上一版正式驗收)。

2026-10-12 **前次續修摘要（v96，當時尚待正式案例，結果已由下方 v97 更新）**：前輪两篇已全部由正常排程正式補回，source26嚴格36項與更新後9項收據接受。再修鉅亨單行聲明圖框：1,474字正文、125字獨立附文與兩個原連結完整保留；只有股票代號及精確NBSP空白版型明確分類來源無正文，不算全文完成。**113新增語義測試、549針對回歸、4,640全news含bulk、24份原件／74耐久版本ABBA通過**；不是來源速率上限或WAN倍速證明。
01:46:01僅graceful切換news至HTML27／empty outcome6／quote outcome4；30項載入與38項**工程**交接接受。273.057秒新owner窗口新增鉅亨227／Global Voices28正文URL heads；128批／5,018版本與四寬度934来源清冊通過，兩個新transport失敗已有實際forward正文收據。5125945?exp=a待**02:18:52**、符號stub與空版型待**02:24:23／02:26:04**原重試期限，有界唯讀observer持續核對，不重排、不把私人回放當正式完成。診斷工具另開HTTP owner被拒絕的問題改成明確deferred／0請求，67語義測試接受；1600512／4778922／1924802仍**沒有原件**，更正前次任務誤述，不推測根因。其他來源繼續獨立抓取，中央社不查API、不改key、不啟用；全934来源／全歷史目標仍active。詳見[單行聲明、來源無正文與診斷owner分界](#單行聲明來源無正文與診斷owner分界)。

2026-10-12 **前次續修摘要（v95，當時一篇正式補回、另一篇待原期限）**：共用鉅亨解析器補上image／p／URL附文、anchor／image參照及完整caption-only gallery；不把圖片alt、像素或schema-only文字當正文。整份current／visible／schema、逐圖對應與完整DOM順序皆需相符，所有數字、重複圖說、作者和原發布／修改時鐘保留。
**432針對測試／4,524全news回歸、22份不同來源捕獲（21網址）的ABBA／66耐久版本通過**；圖說文章兩個時點的原件都驗證，不以URL map覆寫舊失敗原件來縮小範圍。舊15篇健康輸出及其餘4個負面控制不變，candidate多接受原連結附文與完整圖說資料型態；整鏈wall中位11.8453→11.7949秒、範圍重疊，不宣稱WAN倍速或官方上限。
00:54:37只graceful切換news至HTML26；27/27載入／期限檢查及35/35**工程**交接通過。4795333?exp=a已於**00:58:30.472139**按原排程正式補回587字、8組原圖說／圖片對應；原件、作者、clock、所屬64版本與current parser核對。4902745?exp=a僅3,004字正文／47字獨立附文的私人原件重播通過，仍保留**01:38:22.972714正常重試期限**；不冒充兩篇均已正式補回。
291.498秒同owner窗口新增鉅亨374／Global Voices25個正文URL heads；鉅亨1.283029head/s，獨立292.226秒HTTP counter為1.964236req/s。runtime13/13、128近期批次／3,760正文版本／errors0及四寬度934來源／各39頁通過；這不是全部唯一文章或全歷史完整性證明。21啟用來源核對保留amp-news robots 502拒絕授權的證據，沒有用其他host許可替它放行。
啟動有界唯讀觀察者：每20秒僅查兩個URL的索引列，最多每分鐘留心跳，不發HTTP、不改queue；兩篇真正補回後自動執行正式正文／raw／clock／收據核對。來源程式或owner變更就拒絕混用證據。中央社維持暫緩；本機頁／API正常，外網DDNS從本機仍connect timeout。934來源／全歷史目標active，不因工程交接通過宣稱所有來源已修好。詳見[原始連結圖框與完整圖說的獨立資料型態](#原始連結圖框與完整圖說的獨立資料型態)。

2026-10-12 **前次續修摘要（v94，正式回補驗證）**：沿用正常owner已保留的4733427?exp=a原件，修復圖片、段落、CTA與文字-only風險聲明子圖框混排時被誤判缺文的共用解析規則。全文、數值、圖片及連結參照均保留；完整DOM雜湊另核對圖／段位置，不用各自文字／圖片清單冒充完整順序證據。
**290針對測試／4,395全news含bulk、相同20原件ABBA的58耐久版本通過**；舊有14篇健康正文與5個未解控制不變，候選可多重播1篇147字正文及373字獨立圖框全文。原件／作者／2021-10-01秒級發布與修改時間不變，private replay仍不是正式下載。單輪整鏈wall中位10.6940→10.6991秒，範圍重疊，不宣稱WAN倍速或官方上限。
00:00:39僅graceful切換news至HTML25；**25/25載入及原期限驗證**接受。4733427於**00:11:17.081381**按原00:10:53期限實際自動補回，147字正文與373字圖框全文、原件／作者／clock／54個所屬批次版本及current parser核對；**34/34嚴格正式交接、runtime13/13**接受。
670.493秒同owner窗口新增鉅亨497／Global Voices39個正文URL heads，鉅亨0.741245新head/s；獨立658.318秒HTTP counter為1.159015req/s。不是去alias唯一篇數、官方上限或同cohort WAN倍速。128近期批次／4,533正文版本／errors0，四寬度934來源／各39頁通過；21啟用來源傳輸樣本19筆有18筆真正forward正文恢復，1924802仍留到期重試，不拿pool reset冒充19/19。
四份後續原件仍有不同短內容／多圖／圖說／image-anchor問題，不以這次mixed分支通過宣稱全部已修。從本機讀public progress API200／934keys，外網DDNS連線逾時／未取得HTTP，因此只接受本機頁面，不宣稱外網已驗證。中央社維持暫緩，不查API、不改憑證、不啟動；全部來源／全歷史目標仍active。詳見[混合巢狀圖框的完整順序與正常回補](#混合巢狀圖框的完整順序與正常回補)。

2026-10-11 **前次續修摘要（v93）**：中央社依使用者「先不要管中央社」暫緩，清冊保留，不查API、不改憑證、不啟動下載。鉅亨4470101已在**23:23:06.792920**由既有正式owner按原23:23:05期限自動補回341字；原件／正文／張欽發署名／2020-05-01發布秒數／current parser及42版本所屬批次收據通過。v92獨立嚴格handoff **33/33**接受，不再只有私人原件重播。
同owner21啟用來源傳輸巡檢接受；55筆鉅亨article傳輸失敗樣本，49筆有真正後續正文、原件、作者、clock及耐久收據，剩6筆目前是body_not_found，不能用重連成功當補回。128近期批次／4,209版本／errors0，僅代表所選近期批次，不是全歷史。富果161個已知網址（133法說會slug候選／28其他研究）全數保留；三個網址兩輪共六次有界私人metadata診斷，兩個會員預覽、一個未見登入門檻的候選，**0正式正文**。
定位富果新版完成串流DOM、發布／修改meta混用與真正footer服務條款；不讀禁止的/api/、不登入、不把會員預覽／hidden Flight當全文。16項診斷／scope tests及17項唯讀交接接受；Fugle未新增到正式授權／producer。另重用正常owner既有原件4733427?exp=a，零新增HTTP定位巢狀figure內373字被正文排除的下一個鉅亨parser缺口，仍是待修復而非已完成。全934來源／全歷史目標仍active。詳見[正式正常重試與新來源完整診斷範圍](#正式正常重試與新來源完整診斷範圍)。

2026-10-11 **前次續修摘要（v92）**：4470101的真實到期原件定位到單一NBSP空白figure；它與既有NBSP空白figcaption是兩種不同、依序存在的來源placeholder。只在同頁visible／current的精確結構、順序、唯一性與整份schema全部核對後作comparison clone，原正文、圖說、數值、作者與clock不改。
**492 focused／4,287全news含bulk、同19份原件ABBA54耐久版本通過**；22:41:28只切換news owner至HTML24，24/24載入與32/32工程交接接受。199.545秒窗口新增鉅亨127／Global Voices19正文URL heads；鉅亨0.636head/s，另一HTTP窗口1.033req/s。這是現場觀測，不是matched WAN倍速、官方頻率上限或全部來源健康。
128批／3,847正文版本、四寬度934來源頁面接受。真實transport樣本最初1/2補回，22:48再核對已2/2有故障後的新正文／收據，不拿舊正文或pool reset冒充恢復。截至22:49，4470101仍無正式body，保留22:23:05傳輸失敗後的**23:23:05正常重試期限**；當時341字私人原件重播不算正式回補。後續正常重試及嚴格資料交接結果見v93。
依使用者本輪「先不要管中央社」指示，中央社暫緩，清冊保留；不再查API、不改憑證、不啟動下載。全934來源／全歷史目標仍active。詳見[精確空白圖框與不增加請求的驗證](#精確空白圖框與不增加請求的驗證)。

2026-10-11 **前次續修摘要（v91）**：正常到期失敗原件確認「盤後速報」連結標題的單一U+FEFF格式字元造成schema誤判。補上已觀測prefix，仍核對相同current／visible連結與完整schema；保留所有原始字元、數值、作者和clock。共用連結核對改為一次索引，避免每個連結重搜DOM。
**373 focused／4,241全news含bulk、同18份原件ABBA50耐久版本、runtime13/13及handoff32/32通過。** 22:05:44只切換新聞服務至HTML23，其他62來源、授權、bulk／routing、原重試期限、private originals與diagnostic配額保留。5276212於22:08:07由正常排程正式補回431字，原件、body SHA、作者、clock、耐久收據與current replay接受。
233.403秒新增鉅亨95／Global Voices20個正文URL heads；鉅亨0.407head/s，另一HTTP窗口0.879req/s。HTML transport累計501.391秒對解析3.226秒，仍以源站等待為主；不是整體CPU百分比。合成的相同400-link核對由2.084秒降為0.253秒（5次重複的median）；整鏈18份實物的median僅9.711→9.669秒，不宣稱網站下載8.2倍或官方上限。
128批／4,419正文版本及四寬度934來源進度頁接受。當窗7個header前逾時皆精確重建origin pool，5/7已有真實forward正文收據；4465689?exp=a仍502、4470101仍transport error，未冒充已恢復。1507405的current／visible／schema標題都是空字串，未放寬raw scope或捏造文章標題。既有5007990已於21:53:25補回、1305045於21:56:13正式分類圖片-only；舊capture fingerprint不改成23。
全934來源／全歷史目標仍active。詳見[盤後速報連結格式、線性核對與正式補回](#盤後速報連結格式線性核對與正式補回)。

2026-10-11 **前次續修摘要（v90）**：新兩份hourly原件又定位到圖片內p/em/strong來源署名，以及完全只有price／signed change／signed percent的頁面。保留完整來源署名、原作者與全部數字；前者精確對齊同圖、current／visible／完整schema後接受，後者只在三個完整表示與free／clock一致時分類quote-only，不再當正文缺失反覆重試。
**550 focused／4,222全news含bulk、同17份原件ABBA46耐久版本、runtime13/13及handoff29/29通過**。21:31:33 news-only載入HTML22／quote-outcome3，其他62來源、授權、bulk／routing、原期限、原件與diagnostic額度保留。5309063已於21:34:42由正常排程正式補回1,620字，body／raw／作者／clock／收據與目前parser皆接受；圖片內來源不是文章作者。
368.802秒新增鉅亨295／Global Voices36個正文URL heads；鉅亨0.800head/s，另一HTTP counter窗口1.354req/s。HTML transport923.51秒對解析12.803秒，主要仍是源站等待；新owner8筆header前逾時有7筆真實forward正文恢復，剩1477728 HTTP已回來但正文未接受，不算8/8。21:44正常capacity步進至4 slots／0.2秒client target，不代表實際5req/s、官方上限或matched WAN倍速。
128批／4,231正文版本收據與四寬度934來源頁面接受；934keys仍63設定／21啟用／871未註冊。保留既有5164089原址1,054字／Oct8 capture及舊fingerprint，唯讀current replay可核對，**不把它冒充?exp=a觀測已補回**。5007990／1305045仍留21:53／21:56期限，1693999留22:29期限；source衝突、權限、拒絕與全歷史仍active。
詳見[圖片內來源署名與完整報價片段的分流](#圖片內來源署名與完整報價片段的分流)。

2026-10-11 **前次續修摘要（v89）**：兩份真實原件定位到圖片figure的編號strong小標題，以及沒有文字的圖片-only文章。只接受相同current／visible圖、編號、樣式與完整文字的comparison witness，小標題另存而不刪來源文字；圖片-only須同ID／free／作者／clock及完全相同圖片DOM，不用alt、OCR或不同步schema偽造正文。
**602 focused／4,141全news含bulk、同15份原件整鏈ABBA四phase及runtime13/13接受**。正常controls10篇／3個真正缺文保持不變，新parser可重播1,437字正文；42耐久版本／每phase17 HTTP皆核對。21:12:44只重啟news，HTML capture20→21，其他62來源、授權、bulk／routing、有限cohort、原diagnostic收據與額度保留，19/19 loaded-owner checks通過。
216.686秒同owner新增鉅亨180／Global Voices22個正文URL heads；鉅亨0.831head/s，另一HTTP窗口1.381req/s。源站HTML等待308.04秒對解析6.165秒，仍有一次header前逾時；不是去alias文章篇數、同cohort WAN倍速或官方上限。128批／5,150正文版本原件收據接受，四寬度934來源頁面接受。
格式修復目標保留當前21:53:22／21:56:08重試期限，**尚未正式重新capture，不把原件重播算正式補回**。新小時又有5309063的schema／圖段格式、1693999短內容及4898771只有圖但schema仍宣告長文，已留三份原件逐項定位，不宣稱全部source errors修復。全934來源／歷史目標仍active。
詳見[同圖編號小標題與來源圖片-only的精確分類](#同圖編號小標題與來源圖片-only的精確分類)。

2026-10-11 **前次續修摘要（v88）**：根治每日失敗證據再次耗盡，令後來不同模板無原件可查的觀測缺口。原 general／五個固定stage的每日額度與收據不動，用完後每固定bucket／UTC小時至多多留一份已收到的回應；重啟、URL或parser fingerprint不重置，儲存失敗仍先扣額度，沒有增加HTTP或擴大raw權限。
**101 focused／3,991全news含bulk通過、整鏈owned TCP ABBA及runtime13/13接受**。20:37:02只重啟news，63-source契約、授權、bulk／routing／unknown-query、原收據與額度均保留；三份真實hourly原件已核對。5007990定位到同圖figure內strong小標題；1305045實為來源圖片而非文字正文，4898771仍缺body representation，不冒充已補回。
239.788秒新增鉅亨162／Global Voices25個正文URL heads；鉅亨0.676head/s、HTTP另一窗口1.097req/s，當窗源站HTML等待626.0秒對解析5.00秒。仍有3次標頭前timeout、502／robots來源隔離；3筆傳輸故障有2筆實際forward正文恢復。不是同cohort提速、官方上限或全部來源健康。四寬度934-source頁面接受；全部歷史仍active。
詳見[每日證據耗盡後的固定小時有界原件與新模板定位](#每日證據耗盡後的固定小時有界原件與新模板定位)。

2026-10-11 **前次續修摘要（v87）**：持續觀察鉅亨之外的缺文來源，新增台灣事實查核中心已知金融查核的原生正文適配；修正正文混入UI、CMS帳號被當成作者，保留記者／責任編輯／CMS原標籤與來源發布／修訂時鐘。這篇1,486字已由正式owner保存，原件／正文／當前parser／收據與四phase離線耐久重播通過，不把diagnostic當下載完成。
新增共用故障後正文驗證，9筆鉅亨傳輸錯誤有8筆實際forward recovery；剩餘1574038已恢復HTTP取得但仍無正文接受，不標完成。修正診斷交接被producer先重試時的expected cancel：記錄0請求skip，不能清新的deadline或重複抓取。**3,975全news含bulk通過、runtime13/13、handoff26/26**。
20:04:41新聞owner恢復後253.361秒新增鉅亨683／Global Voices25個正文URL heads，約2.696／0.09867head/s；不是去alias篇數、同cohort倍速或官方上限。已知全934來源仍63設定／21啟用／871未註冊；新增小cohort不是全網站完成。上下游實際robots200、文章403／未讀正文已留證，不繞過；其他授權、格式及全歷史缺口仍active。
詳見[原生查核正文及共用故障後資料恢復證明](#原生查核正文及共用故障後資料恢復證明)。

2026-10-11 **前次續修摘要（v86）**：正常owner原件5117411的visible/current/schema全文一致，失敗原因是nested table被無條件拒絕。原simple grid／短正文邊界不動，只在native nested分支增加linear、nonmutating表格森林witness，逐列／格／巢狀位置／混合文字／span與header attributes核對，全部原數字與clock保留。
**396 focused／3,921全news含bulk通過**；同13份真實原件整鏈ABBA驗證38版本，新增恢復592字表格正文，九個既有正文與三個真正缺文負例保持原判定。HTML capture19→20，其他61 capture／所有授權／bulk／routing／unknown query／原件及診斷額度不變。
18:50:34只重啟news owner；必須等fresh queue/status owner與新capture吻合才接受部署，不能用active新PID代替ready。10/10 deployed、runtime13/13、四寬度934-source進度頁通過；18:56:55／19:03:53兩個格式修復目標已由正常owner實際補回，body／raw／author／clock／收據current replay全部通過。本輪另8個header前逾時URL也全部有故障後的真實新正文與收據，不用重連或舊body冒充回補；最新工程／資料handoff30/30。
最新又有amp-news robots來源失敗，已隔離該origin，其他host繼續下載；正常capacity於18:56由2→4 slots、0.5→0.2秒client target階梯量測。這是自適應測試中的壓力，不是官方上限或實際5req/s／5篇/s；空來源正文、不明scope及全部歷史仍未完成。
詳見[完整巢狀表格的逐格來源森林核對](#完整巢狀表格的逐格來源森林核對)。

2026-10-11 **前次續修摘要（v85）**：由v84正常owner的新原件找出`p > em`單一前導U+FEFF格式缺口；只在相同且唯一current段落作證後於comparison copy比對，原字元／數字／全文／作者／clock皆保留。新helper只於mismatch lazy建一次段落Counter，避免逐candidate再掃所有current段落。
**332 focused／3,851 news含bulk通過**；同11份真實原件的owned TCP／parser／fsync／收據四phase共34版本接受，僅5157359修復為2,117字完整正文，另外兩份來源缺文仍失敗。只有鉅亨HTML capture18→19，所有授權／其他61 capture／bulk／routing與未知query／診斷額度保留。
18:19:24新owner正常下載，runtime13/13、四寬度進度頁及19/19工程handoff接受；236.354秒窗口新增鉅亨296／Global Voices22个正文URL heads，1.252新head/s是當窗觀測，不是同cohort WAN倍速或官方最大速率。5157359仍保留原18:58:36 retry deadline，**尚未正式重新capture，不把原件重播冒充正式補回**。
兩個provider又有header前逾時／502，已納入同owner傳輸證據並維持origin隔離／回補，不因其他來源有進度說全部健康；新table-grid／空正文原件仍需逐份修復或判明來源邊界，全934來源／歷史目標仍active。
詳見[原生em強調段落格式比較的完整正文修復](#原生em強調段落格式比較的完整正文修復)。

2026-10-11 **前次續修摘要（v84）**：修正共用每日兩份失敗原件被凌晨耗盡、後來不同模板只剩計數的觀測缺口；保留原 general key／舊原件，五個既有鉅亨 native failure stage 各有兩次私人取樣，先扣已知舊收據、儲存失敗也先扣額度，重啟不重置，不增加 HTTP 或接受失敗正文。
**150 focused／3,814全news含bulk及完整owned TCP配對通過**；新owner正常留下三類、三份真實新失敗原件，SHA／原件／當前negative replay皆核對。241.808秒窗口新增鉅亨513／Global Voices25個正文URL heads；鉅亨2.122新head/s是當窗觀測，不是官方上限或同cohort publisher提速。
來源、授權、配額與原時鐘均保留；934來源清冊、20啟用來源傳輸核對與四寬度進度頁核對。原件已定位下一步 `p > em` 前導U+FEFF模板缺口，另有可見正文缺段與空模板；本階段沒有把diagnostic當全文，全部來源／歷史／錯誤仍未完成。
詳見[失敗原件按固定類別有界保留與不重置額度](#失敗原件按固定類別有界保留與不重置額度)。

2026-10-11 **前次續修摘要（v83）**：根治已有正文的失敗追新被普通 `due=0`／最新日期追新餓死；只在既有 stored-body 份額內，每八輪普通服務後給最早到期失敗一輪，不增加請求、不借用 missing-body 配額，不清 source deadline／lease／scope。
**8/8實際逾期文章由正常owner補回，正文／作者／clock／raw／收據current replay皆通過**；另外兩筆新連線／短idle的真實逾時也正常補回。3,800全news含bulk、195focused與664版本完整管線配對接受。
453.441秒同owner窗口新增鉅亨720／Global Voices45個正文URL heads；鉅亨1.588新head/s、另HTTP窗口2.253req/s，不是去alias獨立篇數、官方上限或同cohort publisher倍速。
62-source身分／未知query／完整性guard保留；934清冊、20啟用來源故障觀測與四寬度實際provider cards／進度／恢復皆核對。來源逾時、502、未授權、無正文及其他parser backlog仍存在，**不是所有來源完成**。
詳見[有正文的失敗追新不再被普通追新餓死](#有正文的失敗追新不再被普通追新餓死)。

2026-10-11 **前次續修摘要（v82）**：共用傳輸加入真正checkout／connect觀測，不改TLS、重試、來源FP、內容或配額；區分未觀測、沿用連線及實際重新連線，不再把所有ReadTimeout猜成idle keep-alive。
現場鉅亨一筆僅閒置0.73秒、一筆已完成新連線，均在回應標頭前逾時；Global Voices另有59.24秒idle後RemoteDisconnected。**三筆均由正常owner自動補回，正文／作者／clock／raw／收據重播接受**。
**3,773全news含bulk、176 focused、owned TCP／TLS與12-phase完整管線ABBA**通過；832.192秒新增鉅亨1,301／Global Voices82個正文URL heads，平均CPU0.164 core。不同cohort不當作publisher倍速或官方上限。
20啟用來源故障核對與62個capture／authorization／bulk／routing均保留；16:26窗含5次鉅亨transport、14次502，16:37再監測累計已19次transport；CMoney／PeoPo403亦真實可見，不因保留舊正文便宣稱追新已完成。
詳見[實際連線重用與標頭前故障的源頭觀測](#實際連線重用與標頭前故障的源頭觀測)。

2026-10-11 **前次續修摘要（v81）**：根治同篇可見／current／schema與同圖皆一致、但figure只有單一`p.warning`時被拒絕的模板缺口；精確三行風險聲明仍另外保存，不刪字或放寬全文／作者／時鐘／權限。
原件四份重播僅真實模板一篇恢復，另外三頁4～20字仍保留失敗。15:37:13正常owner已自動補回**1,242字正文**，與原件body SHA一致；**10/10正文／作者／clock／收據current replay**接受。
**3,728全news含bulk、532 focused、loaded capture8/8及runtime13/13**通過；374.484秒新增鉅亨253／Global Voices32個正文URL heads，該窗仍有2次transport及7次502。
只有鉅亨HTML capture17→18，其他sources、rights、bulk與routing scope不變，既有`done`游標沒有重掃。20啟用來源／934清冊／四寬度頁面核對；全歷史、全部sources及外部錯誤仍未完成。
詳見[原生單段風險聲明模板的完整正文恢復](#原生單段風險聲明模板的完整正文恢復)。

2026-10-11 **前次續修摘要（v80）**：找出HTML多段CSS反覆掃整頁的CPU熱點；先用terminal tag／class／id／attribute縮小候選，再由原SoupSieve完整matcher核對，未支援語法及XML保持原CSS路徑。
同一批36份原件、精確v79前版ABBA八phase，完整解析／落盤／raw／收據／驗證中位wall **3.161→2.825秒（降低10.63%）**、CPU **2.126→1.879秒（降低11.63%）**；較慢phase亦保留，不當作publisher倍速或官方上限。
**3,706全news含bulk、1,170 focused、正式載入7/7與runtime13/13**通過；212.403秒新增鉅亨126／Global Voices19個正文URL heads，仍有鉅亨6次transport及1次502。
四個到期失敗的原件／收據重播定位到一個風險聲明段落模板缺口，另外三頁只有4～20字；這四篇尚未補回，沒有把診斷當成成功下載。20啟用來源／934清冊／四寬度網頁皆核對，全目標仍未完成。
詳見[HTML選擇器候選縮小與當前失敗原件重播](#html選擇器候選縮小與當前失敗原件重播)。

2026-10-11 **前次續修摘要（v79）**：原網址routing與正文parser版本分開，不再因parser／pacing變動重掃全部literal proofs；legacy原游標有實際current-policy證據才續接，不猜舊清冊、不快轉。
保留逐alias原順序metadata upsert，只把目標存在性查詢合併成每批最多400個bind；rights／state／body／due／lease／source links不變，801筆跨bind邊界、停止與rollback測試通過。
**3,138全news含bulk回歸、262 focused、7/7正式migration**通過；固定WAL三個256-alias窗口、36個ABBA phases與60份原正文／raw／收據recover相同，routing與完整replay有快有慢，不主張publisher倍速或官方上限。
正式runtime **13/13**通過，204.379秒新增鉅亨116／Global Voices16個正文URL heads；同窗仍有6次transport與6次502。20啟用provider與四viewport／934來源網頁核對通過；934來源、未授權與全歷史目標仍未完成。
詳見[原網址路由按清冊權限增量與有界批次查詢](#原網址路由按清冊權限增量與有界批次查詢)。

2026-10-11 **前次續修摘要（v78）**：原站gateway負面checkpoint與正文parser版本分開，v1-v3有證據的bounded期限／失敗次數能由v4恢復，parser升版不再清掉technical pause，也不把舊negative FP改成新FP。
同一檔案的單筆malformed host／row不再丟失其他合法host期限；當前授權、robots、Retry-After與publisher配額仍獨立檢查，restart／expiry不當成健康證據。
新四份2013原件中兩份只有「ticker／price／signed change」，已修正行情片段的分類缺口，**4/4來源negative原件核對、9/9既有正常正文重播**通過；不產生偽正文、不重開bulk歷史窗口。
**3,110全新聞含bulk回歸、369 focused**通過；配對loopback八phase保留真TCP／raw／收據與錯誤，最新wall／CPU仍較慢，不宣稱publisher提速或官方最高頻率。
正式runtime **13/13**通過，203.433秒新增鉅亨94／Global Voices19個正文URL heads；該窗仍有3次transport與2次server error，不把復原能力當來源無錯。
20-provider與四viewport／934來源網頁核對通過；全來源／授權／全歷史仍保持未完成。
詳見[解析器升版不清除原站故障證據與帶漲跌額片段分類](#解析器升版不清除原站故障證據與帶漲跌額片段分類)。

2026-10-11 **前次續修摘要（v77）**：單一原站連線故障不再關閉同執行緒其他健康來源的池；每worker最多十個origin／每池一條連線，TLS、cookies、robots與publisher配額仍走原共用管線。
擁有來源的實際TCP／原文／收據配對中，六次故障的總新連線20→8；最新wall與CPU反而較慢，**只接受故障範圍／連線復用改善，不主張publisher倍速或官方最高頻率**。
四份2013原件只提供ticker／價格片段，已核對可見／current／schema／ID／free／clock，獨立列為`source_body_quote_only`，不偽造新聞、不再無限重試；四筆正式查詢／當前queue皆通過。
**3,057新聞含bulk回歸、459 focused、125 gateway**通過；正式runtime **13/13**，202.303秒新增鉅亨194／Global Voices19個正文URL heads，鉅亨0.95896新head/s。
本機進度頁1440／768／390／320四種寬度、934來源／各39頁與新提示均rendered驗收；來源502／逾時／403、未授權或無原正文的缺口仍留存，**沒有全來源完成或全部錯誤已根治的宣稱**。
詳見[原站連線池故障隔離與行情片段的獨立分類](#原站連線池故障隔離與行情片段的獨立分類)。

2026-10-11 **前次續修摘要（v76）**：修正所有provider共用的discovery空／稀疏到期佇列掃描瓶頸。
鉅亨277,015筆active discovery在沒有到期任務時，固定WAL／ABBA中位查詢56.67毫秒→13.77微秒，保留同列／同序／同LIMIT／所有存取guard。
少量到期工作走due索引，最多257列lookahead；密集工作保留priority索引，不把probe的前256列當成完整候選。
剩餘第九筆原生identity已由正常排程補回，九份原件／當前parser重播 **9/9**通過；指定fetch **0 attempted／0 captured**，因已有耐久正文，不是下載失敗。
**2,946新聞回歸、163 focused（含69 bulk）**與正式runtime **13/13**通過；186.048秒新增鉅亨363／Global Voices17個正文URL heads，鉅亨1.95111新head/s。
該窗口仍有鉅亨3次transport與5次502；20來源錯誤context核對不等於來源全健康。934來源／全歷史目標仍未完成，不宣稱官方最高速或不同cohort的倍速。
詳見[到期工作索引與剩餘原生失敗的正常恢復](#到期工作索引與剩餘原生失敗的正常恢復)。

2026-10-11 **前次續修摘要（v75）**：修復啟動恢復反覆掃描620萬筆歷史、全量Path物件排序與原生identity失敗永久停抓。
同一批189,363份收據的完整AB／BA啟動，中位時間降低約17.7%、峰值RSS降低約56.2%，每份原始JSON仍讀取驗證。
四篇鉅亨 **4/4實際補回**，另四篇自動續抓；八篇耐久head與目前parser重播 **8/8**通過，九筆舊negative仍有一筆重試，沒有算九篇完成。
Global Voices真實圖片／YouTube-only頁已原件重播與正式GET確認為非文字內容，沒有偽造正文或下載第三方影音。
**2,920新聞回歸、69 bulk、214 focused**通過；正式runtime **13/13**。120.166秒新增鉅亨136／Global Voices4個正文URL heads，
鉅亨1.13177新head/s；該窗口仍有一次AMP robots502，較晚交接再次觀測到連線／gateway失敗。
來源拒絕與原期限保留、健康端點繼續，934來源與全歷史仍未完成，不宣稱外部故障已根治。
詳見[全歷史掃描、收據路徑與可恢復的來源失敗](#全歷史掃描收據路徑與可恢復的來源失敗)。

2026-10-11 **前次續修摘要（v74）**：修復真實原件 5418903 的清單 headline 格式字元比較，
兩筆原生 mobile 文章 **2/2重新下載、2/2耐久head與目前parser重播**通過；206筆到期舊失敗只重排一次，不算已抓完。
網頁的條件ETA改用同一owner近期連續完整視窗，保留失敗、等待、零進度與間隔；程序平均另列，舊owner速率不挪用。
**2,879新聞回歸、69 bulk、245 owner／capacity／panel**通過；正式runtime **13/13**，20啟用來源transport證據核對通過。
175.177秒WAL窗口鉅亨新增109／Global Voices13個正文URL heads，鉅亨 **0.622228新head/s**；
另167.778秒HTTP時計 **1.14437req/s**。服務約0.370289 CPU core／258.289 MiB cgroup memory。
目前仍在來源連線恢復與重新量測，這不是同cohort提速或官方最高速證明；AMP502及CMoney／PeoPo403仍保留。
934來源／39頁清冊與四個viewport的真實畫面核對通過；仍不宣稱全歷史或所有來源完成。
詳見[原生清單比較與同owner近期估時](#原生清單比較與同owner近期估時)。

2026-10-11 **前次續修摘要（v73）**：找出鉅亨公開文章「可見署名 span 明確空白、但同篇 schema／current data 作者一致」的真實解析缺口。
保留空白署名與來源作者的不同依據，正文／ID／free 宣告／發布時間／可見性仍須完整交叉核對；九個舊失敗已 **9/9重新下載、9/9耐久head／目前parser重播通過**，不是借用其他頁的作者或正文。
唯讀網頁分開顯示 provider 冷卻與局部 origin 暫停／最早重新核對時間；過期負面期限不是恢復存取的證據。
**2,802新聞回歸、69 API/bulk、108 selected-report tests**通過；新 owner 正式 **13/13**。
185.316秒WAL窗口新增鉅亨233／Global Voices16個正文URL heads；鉅亨 **1.25731新head/s**，另182.867秒HTTP時計 **1.91396req/s**。
新聞service約0.513835 CPU core／257.172 MiB cgroup memory；這是實際窗口，不是同cohort提速倍數、整台主機測量或官方上限。
CMoney／PeoPo仍有可驗證的來源403，deadline與收據保留，其他健康來源繼續；全934來源與所有歷史尚未完成。
詳見[原生空署名恢復與局部端點的公開觀測](#原生空署名恢復與局部端點的公開觀測)。

2026-10-11 **前次續修摘要（v72）**：修正 robots 初始 origin 與 provider 共同限速混用，
單一 origin 的無 Retry-After 502／504 不再借用其他 host 的成功證據決定整個 provider 是否停抓；
原 origin 仍 fail-closed，實際重試 30→60→120→240→300 秒，文章／discovery 選取及閒置喚醒使用一致的暫停範圍。
保留 401／403／429／503、任何 Retry-After、challenge、route／TLS 與共同配額保護；成功期限到期不等於取得許可。
**2,742 新聞回歸、69 API/bulk**通過；新 owner 正式 **13/13**，152.950 秒新增鉅亨143／Global Voices12個正文網址。
該窗口鉅亨0.934945新head/s，新聞服務平均0.375718 CPU core／323.83 MiB cgroup memory；
這是觀測窗口，不是配對全來源benchmark、整台主機節能證明或官方速率上限。
真實 AMP robots 持續502，11:27交接觀測耐久負面狀態已到第五代300秒；這不是其他正常請求的抓取間隔。
原錯誤保留、其餘端點持續入庫，不宣稱來源故障已消失。
詳見[Robots origin 故障範圍與排程喚醒](#robots-origin-故障範圍與排程喚醒)。

2026-10-11 **前次續修摘要（v71）**：沿同一collector實作「已核對原件直接重驗入庫」，
Genetinfo四份canonical實測原件 **4/4耐久正文、4/4當前解析與原始作者／日期一致**；
重驗／入庫0.574秒、**這一步新增HTTP請求0**，含owner初始化／恢復／狀態的整個指令21.148秒，不能混成全鏈路提速倍數。
發現2019 metadata指到目前2023另一標題的正文，兩者原值保留，沒有回填或捏造2019原文。
已存正文1,355、待核對2,108；原固定1,356網址與收據不變，新scope有獨立fingerprint，62份授權不變。
**2,697新聞回歸、69 API/bulk、140 focused**通過；四尺寸39頁／934來源與Genetinfo追加4/4對帳。
正式運行 **12/12**，122.160秒新增鉅亨117、Global Voices12個正文網址；較近傳輸樣本仍有3次等待標頭失敗／3次session重建、
4次robots的502（1個host隔離／3個publisher保護決策），沒有抹除失敗、強開拒絕或宣稱官方上限／全來源完成。
詳見[已核對原件零重抓、原生日期衝突與恢復保護](#已核對原件零重抓原生日期衝突與恢復保護)。

2026-10-11 **前次續修摘要（v70）**：補回已解析、但未進清冊的 **192個原始來源鍵／2,546條metadata連結**，
全來源網站由742增至 **934鍵＝62設定＋872未註冊**，原有鍵全部保留，授權與20啟用來源不變。
不是新增192個全文下載器，也不把多條轉址算成多篇正文；僅有線索的卡片顯示「未入佇列」、速率未量測與未知完成時間。
來源摘要改為同交易增量維護；同一WAL、相同946,017連結／616主機的三次核對，
全索引oracle中位123.17ms、增量摘要8.98ms，**摘要生成約13.72倍**，不是正文HTTP提速或官方極限。
**2,669新聞回歸、69 API/bulk、177 focused**通過，最終四尺寸39頁與934鍵實際渲染／過濾／復原驗收。
新owner正式 **12/12**；142.304秒新增鉅亨137、Global Voices15個正文網址，
該窗口仍有1次等待標頭的傳輸失敗／1次session重建；較晚交接仍有17次連線失敗及7次gateway錯誤，
沒有掩蓋失敗或宣稱已根治外部延遲。
只重載新聞owner與唯讀面板，timer恢復，既有失敗證據與來源拒絕／due保留；完整goal仍未完成。
詳見[原始媒體清冊遺漏與低成本增量摘要](#原始媒體清冊遺漏與低成本增量摘要)。

2026-10-11 **前次續修摘要（v69）**：沿完整742來源清冊補齊PanSci既有4個待核對公開網址，
**4/4原件重播、4/4正式下載與當前版本驗證**；新增12,108字／32,963 UTF-8 bytes，已知本機網址合計5/5。
原固定批次1篇、membership digest和收據不變，追加4篇有獨立immutable scope；不是全站歷史或CC／訓練授權。
兩篇原生日期為2017年，舊metadata為2018年；保留兩種原值，日期粒度不捏造成時區／分秒。
鉅亨958337已在原due自然重查後正確標為source-empty，無正文，當前source fingerprint核對通過。
**2,649新聞回歸、69 API/bulk、105 focused**通過，四尺寸既有頁面對帳；正式運行 **12/12**，
146.151秒新增鉅亨145、Global Voices15個正文網址，此短窗口未觀測到新的502或transport failure。
前次錯誤證據保留，不將此窗口或一個已補齊來源宣稱為全來源完成／最高速。
詳見[PanSci既有缺口、原生發布日與有限批次追加](#pansci既有缺口原生發布日與有限批次追加)。

2026-10-11 **08:25 前次續修摘要（v68）**：修正傳輸故障恢復只看all-time峰值、忽略近期其他穩定設定的排程根因。
同一份固定歷史，舊選擇沒有候選；新選擇找到 **7個不重疊健康窗口**支持的0.2秒／7並行，
按可比較的耐久新正文吞吐中位數排序；不重新發請求，不重設來源拒絕，不把歷史速度當作當前ETA。
同時修正鉅亨買超清單第一家公司前的格式字元；一篇已依原due自然補回，正文SHA、作者、發布時計和新指紋均相符。
另一篇原頁只有`<div><br></div>`空白模板，原件驗證為來源空白，不計正文完成，仍等待原定09:00重查。
**2,648新聞回歸、69 API/bulk**通過；四尺寸既有面板742來源鍵驗收。
較長正式窗口 **12/12**：265.146秒新增鉅亨 **346**、Global Voices **24** 個正文網址，
鉅亨 **1.3049新網址正文／秒**，仍 **6次502、2次header逾時／2次session reset**，沒有宣稱錯誤消失或官方極限。
詳見[鉅亨近期穩定容量恢復與原件格式修復](#鉅亨近期穩定容量恢復與原件格式修復)。

2026-10-11 **07:57 前次續修摘要（v67）**：6個VOA粵語追加網址的真實公開頁均宣告另一個中文canonical，
不是HTTP拒絕；已逐篇核對當前OG／schema／標題／作者／免費標誌／原生發布時計，修正精確對應與已觀測署名。
**6/6原件重播、6/6正式下載與耐久版本驗證**通過，原固定cohort1篇與收據不變；另1個未驗證網址保留。
保守私人研究v2不把出版社署名或受保護文字自動標成公共領域，不啟用再發布、商用或訓練匯出。
**2,611新聞回歸、69 API/bulk、238 focused**通過，不把重疊測試相加；四尺寸既有面板742來源鍵與追加6/6對帳。
較長正式窗口 **12/12**；556.218秒新增鉅亨 **264**、Global Voices **55** 個正文網址，
鉅亨 **0.4746新網址正文／秒**，實際仍 **4次502、10次傳輸失敗／10次session reset**。
這個較慢窗口保留，不用上一輪峰值宣稱本輪提速或官方上限；header等待、自動host隔離與回復仍是待優化重點。
詳見[VOA精確語系宣告與私人追加正文修復](#voa精確語系宣告與私人追加正文修復)。

2026-10-11 **07:25 前次續修摘要（v66）**：修正既有候選排程漏掉桌面 `?exp=a` 的分工問題，
沿用同一分類器、公平性與持久 cursor，不把主文已存的網址版本直接算 complete。
完整當前桌面待辦 **52,954** 筆均已有正確候選 ID；正文可用性提示 **0** 不一致。
網頁新增「未找到主文／待分類、主文已存但網址版本待驗證、本網址正文已存待重查」，三者合計核對待回補；
交易式小表每次只讀最多8組，兩次實測約 **8–16微秒**，不掃數百萬篇歷史。
**2,582新聞回歸、69 API/bulk、213 focused**通過；新增唯讀稽核細節另有 **73項**通過，不把重疊測試相加。
兩個正式窗口均 **12/12**：後一個 **458.290秒**新增鉅亨 **1,176**、Global Voices **48** 個正文網址，
鉅亨 **2.566新增正文網址／秒**，不是 matched-source 倍數或官方極限。
同窗口仍有 **4次502、7次header傳輸失敗／7次session reset、36次body_not_found**，均不計完成；
自動升速曾到8並行，遇失敗已退至7並行／0.2秒 client gap，仍繼續抓。
四尺寸面板與本機HTTPS路由通過，對公開IP的兩次回讀仍8秒連線逾時，不能宣稱外部連線已修復。
詳見[桌面候選分工與常數成本階段計數](#桌面候選分工與常數成本階段計數)。

2026-10-11 **06:57 前次續修摘要（v65）**：修正買超股票清單格式字元及直接圖片框補充文字的誤拒收，
兩份同原件 **2/2**通過，兩筆也已由正式 owner 依原 due 自然補回；原件／正文SHA、作者、時計及新指紋相符。
**2,563項新聞回歸、69項API/bulk、431項focused**通過；正式運行 **12/12**、最近128批收據無錯，
同 **148.705秒**資料窗口新增鉅亨 **103**、Global Voices **15**個正文頭。
文章HTTP平均 **1.876秒**、解析平均 **30.4毫秒**，仍有 **3次502／4次header逾時及4次session reset**；
不是 matched-source 提速證明或來源最高速。新增唯讀重複工作診斷：最早到期512個pending URL全是原生別名，
且本機有同ID主網址正文；但兩個已修案例的原始body SHA不同，不能因此直接跳過、複製收據或算別名complete。
本機四尺寸仍保留742鍵；完整goal active。詳見[買超清單與直接圖片補充文字修復](#買超清單與直接圖片補充文字修復)。

2026-10-11 **06:33 前次續修摘要（v64）**：再取得四份當前到期的公開失敗原件，
修正「短訊正文在段落外」與「巢狀圖片框」兩個共用解析根因；相同原件 **4/4**通過，
四筆均由正式常駐 owner 依原 due 自然補回，逐筆正文／原件／圖說／補充文字／作者／發布時計及當前指紋相符。
**2,529 項新聞回歸、69 項 API/bulk、397 項 focused**通過；focused已包含在新聞回歸中。
正式運行 **12/12**，同 **450.986 秒**資料窗口新增鉅亨 **330**、Global Voices **43**個正文頭；
文章 HTTP 平均 **2.499 秒**、解析 **33.9 毫秒**，主要等待仍在來源而不是正文 CPU。
同窗口仍有 **11 次 502、5 次傳輸失敗／5 次連線重建、58 次 body_not_found**；
不同來源 cohort 不能拿前輪速度直接算提速倍數，亦未達成「全部來源根治／官方最高速」證明。
後續06:38交接同owner **1,113.376秒**新增鉅亨 **1,204**、Global Voices **105**，
鉅亨 **1.0814新頭／秒**；同窗口仍有 **21次502／19次傳輸錯誤（19次重建）／170次body_not_found**。
本機四尺寸進度頁已讀到當前快照；清冊保留 **742 鍵／62 設定／20 啟用／680 未註冊**，goal仍active。
詳見[短訊全容器與巢狀圖片框修復](#短訊全容器與巢狀圖片框修復)。

2026-10-11 **06:00 前次續修摘要（v63）**：取得四筆當前到期缺正文的真正來源回應；
其中一筆 HTTP 502 沒有原件可接受，另外三筆確定是空白圖說／股票清單格式字元的比對問題。
三份相同原件從 `body_not_found` 修到可完整驗證，三筆也都已經正式下載入庫，
逐筆原始正文 SHA、作者、發布／修訂時計、當前 parser 指紋與 SQLite／Parquet／receipt 相符。
只改比較 clone，不清洗原始值、不刪正文、不補造時鐘、不重置來源冷卻或 2/day 自動診斷帳。
完整新聞回歸 **2,477 項／55.04 秒**、API/bulk ABI 回歸 **69 項／2.41 秒**通過；
最終同 owner **817.489 秒**新增鉅亨 **1,034**、Global Voices **82** 個正文頭；
鉅亨 **1.265 新頭／秒**，獨立 **809.325 秒**HTTP 窗口 **1.818 請求／秒**，正式驗收 **12/12**。
實測文章傳輸平均 **1.748 秒**，正文解析平均 **39 毫秒**；不是 matched-source 提速比或官方上限。
來源端逾時、502 與其他 schema／table 缺口仍存在，保留為後續工作，不稱全部根治。
全清冊仍為 **742 鍵／62 設定／20 啟用／680 未註冊**，本機四尺寸進度頁通過；全歷史 goal 仍 active。
詳見[鉅亨空圖說、股票清單與到期補漏驗收](#鉅亨空圖說股票清單與到期補漏驗收)。

2026-10-11 **05:19 最新續修摘要（v62）**：鉅亨仍有真實來源失敗，不是未排程。
最終正式 owner 同一 **145.592 秒**新增鉅亨 **161**、Global Voices **13** 個正文頭；
鉅亨 **1.106 新頭／秒**，另一 **138.477 秒**HTTP 窗口 **245 starts／1.769 請求／秒**。
窗口仍有 **4 次 ReadTimeout／4 次 session reset／25 次 body_not_found**；4 次傳輸失敗皆在
`news.cnyes.com`等待 response headers，不是可逕自忽略的其他 gateway 故障。
沒有以放大併發、放寬作者／時計或把請求數當正文數來主張「理論極限」或已根治全部來源錯誤。
新增兩個 VOA 原創文字來源：修復 `.wsw` 混入作者簡介、真正 byline DOM 與粵語跨語言 canonical 身分；
各一個已知原件已經共用下載器落盤、驗證，保留舊收據與原始文字 SHA。
完整新聞回歸 **2,428 項／58.35 秒**、最終正式運行 **12/12**、最近128批收據與本機四尺寸742列通過。
清冊維持 **742 鍵／62 設定／20 啟用／680 未註冊**；粵語另有1篇新增 URL 待核對，沒有藏成已完成。
正文、發布／修訂時鐘、來源許可與當前執行身分各自核對；不加入訓練、不傳到瀏覽器。
詳見[原創新聞身分、原件與正式續抓驗證](#原創新聞身分原件與正式續抓驗證)。

2026-10-11 **04:49 前次續修摘要（v61）**：正式新 owner 同一 **128.487 秒**窗口新增鉅亨 **240**、
Global Voices **12** 個正文頭；鉅亨 **1.868 頭／秒**，HTTP 獨立窗口 **2.542 請求／秒**。
仍有 **3 次 502、4 次 transport error、35 次 body_not_found**；既有 session 重建／主機隔離持續運作，
不是所有來源錯誤已消失，也不是同來源匹配基準或理論極限。
修正 UDN／MoneyDJ 來源審閱原因：現行主要條款明列需取得同意，不能當成解析錯誤反覆重試。
私人清冊與進度頁共用審閱 loader；**需取得同意、審閱未完成、未知、技術失敗**分開；
review 檔內容更新只使唯讀 cache 失效，不改來源權限、指紋、排程或重啟 HTTP owner。
**2,475 項／52.52 秒**完整新聞回歸、最後改動範圍 **141 項／15.68 秒**及共用 gateway **125 項／27.42 秒**通過；
正式續抓 **12/12**、最近 128 批 **7,363 版本／1,356,015,364 bytes 原件**核驗無錯。
本機四尺寸、742 列／31 頁、審閱時計與 11 來源的原生篩選通過；頁面不呼叫 publisher。
全清冊仍保留 **742 鍵／60 設定／18 啟用**；**704 未審閱、8 審閱未完成、11 需同意**，全歷史 goal 仍 active。
本輪只交接新聞服務與唯讀 gateway；交易 PID 未變。本機通過，本機對 WAN HTTPS 仍連線逾時。
詳見[來源審閱原因與正式續抓分離](#來源審閱原因與正式續抓分離)。

2026-10-11 **03:52 前次續修摘要（v59）**：已修正 gateway 主機失敗後固定每 30 秒反覆探測、
成功回應仍被舊 pause 擋住，以及晚到的失敗回應重開冷卻的問題。
只對已驗證的故障主機作 **30／60／120／240／300 秒**有界退避；這不是健康鉅亨端點的請求間隔。
成功的完整回應立即解鎖，重啟保留失敗代數，官方拒絕、Retry-After、robots 與來源配額不變。
**2,411 項／69.64 秒**新聞回歸通過，正式 owner 同一 **226.675 秒**窗口新增鉅亨 **382**、Global Voices **19** 個正文頭；
鉅亨約 **1.685 新頭／秒**，不是匹配基線的提速比例或官方上限。
最近 128 批 **6,172** 個正文版本核驗無錯誤，運行／程式身分／資料保存 **12/12** 通過。
17 個啟用來源的傳輸觀測核對通過，鉅亨已觀察到 **8 次失敗 session reset、1 次主機自動恢復**，
AMP 主機確實依序退避至 240 秒；來源 502／headers timeout／部分 schema 缺證仍未全部消失。
本機四尺寸、742 列進度驗收通過；僅重載新聞 owner，沒有重啟其他 collector 或交易。
全來源清冊／規劃仍保留 **742 個來源鍵、59 個設定來源、17 個啟用來源**，全歷史 goal 未完成。
詳見[故障主機有界退避與成功即恢復](#故障主機有界退避與成功即恢復)。

2026-10-11 **03:24 前次續修摘要（v58）**：正式新聞 owner 已載入完整收據解碼改善，
**2,385 項／64.38 秒**回歸通過；同一 owner **94.236 秒**新增鉅亨 **134**、Global Voices **12** 個正文頭，
全部 **12/12** 運行／收據／程式身分檢查通過。128 批 **4,123** 個正文版本 SHA／收據無錯誤。
鉅亨仍有來源端 502；17 個啟用來源的觀測通過，新 owner 的 **9 個 host 決策**均保留，沒有強迫重試或重置每日診斷上限。
固定 8,192 份收據的完整恢復改善約 **6–14% wall／CPU**；有界 thread pool 反而增加 CPU／暖延遲，
已移出正式 owner，只留在原測速工具，不把「平行」當作「更快」。
本機四尺寸、742 列進度核對通過；只重載新聞，其他 collector／gateway／交易 PID 都未變。
實際初始化 **68.930 秒**、收據恢復 **56.539 秒**仍有可改善空間；和上一輪不同 cache，不冒稱等比例加速。
來源 schema／作者／授權與未知歷史缺口仍分列，全來源 goal 未完成，詳見[完整收據解碼與實際初始化取樣](#完整收據解碼與實際初始化取樣)。

2026-10-11 **02:50 最新根因修復摘要（v57）**：真正佔住主排程的大頭，是每 15 秒的狀態更新，
其中稀疏非正文結果查詢採到錯誤 join order。已修正而非降低刷新頻率：同一 WAL、742 來源鍵的七輪**全欄位／整份 JSON 寫入**對比一致，
wall 中位 **6.940 → 1.059 秒（−84.74%）**、CPU **6.865 → 1.046 秒（−84.77%）**。
全部原／追加批次、來源身分／時鐘／fingerprint、錯誤和進度資訊保留。
最後新聞及批次回歸 **2,349 項／73.30 秒**通過；新版已載入，同一 owner **388.707 秒**新增鉅亨 **310**、Global Voices **40** 頭，
正式 **12/12**驗收通過，最近 128 批 **2,873** 個版本無收據／SHA 錯誤。
當前窗口約 **0.798 鉅亨新頭／秒、1.211 HTTP／秒**；不同錯誤負載窗口不拿來冒稱匹配下載速度提升或官方上限。
來源仍有 502／timeout，新 owner 已留 **13 次傳輸失敗／13 次 session reset、19 個 host 隔離決策**；沒有用假正文吞錯。
本機四尺寸進度再次通過，其他 collector／gateway／交易程序未重啟。
本次冷啟動 **168 秒**、其中收據恢復 **120 秒**仍需優化；原就緒 timeout 保留，沒有反覆重啟或跳過恢復。
詳見本節[主排程堵塞的稀疏非正文查詢](#主排程堵塞的稀疏非正文查詢)。全來源 goal 仍 active，下面各時間的快照／失敗仍保留。

2026-10-11 **03:02–03:08 續修（v58，啟動／資料驗收尚在進行）**：
全清冊仍為 **742 個來源鍵、59 個設定來源、17 個已啟用來源**，不是縮成鉅亨單一來源。
03:02 的固定 WAL 有 **6,201,943 已知 URL、3,571,295 已存正文頭**；鉅亨 **3,461,378** 頭、
**127,981** 個缺正文工作及 **397** 個已有正文重查，原值／空原文／作者／權限問題分開保留。
完整收據恢復改為重用既有 private JSON decoder，不省略任何 JSON 欄位、錯誤或孤兒 SHA／列數驗證。
8,192 份相同真實收據（3,014,263 bytes）、三輪 ABBA 的全恢復比較一致，
wall 中位 **0.3170 → 0.2957 秒（−6.73%）**、CPU **0.3157 → 0.2957 秒（−6.34%）**；
較小 4,096 份約 17% 的測試亦保留，不選較漂亮的比例冒稱完整冷啟動改善。
**2,367** 項新聞／批次回歸及最後 **235** 項恢復／失敗隔離測試通過。
只重載新聞 owner；本次尚須核對新 owner 的初始化、不可變收據及持續入庫，不能以 active 宣告完成。
原 UTC 日 **2/2** 失敗診斷預算未清除，沒有擴大請求／拒絕／finite scope 或修改其他 collector。
詳見[完整收據解碼與實際初始化取樣](#完整收據解碼與實際初始化取樣)。

2026-10-11 **02:15 前段驗收摘要（v57）**：鉅亨仍在自動抓取，但來源 502／headers timeout 仍是實際瓶頸，
不能宣稱全部故障已消失或已達官方最高速。本次根治廣告 ID-prefix 排除造成的每篇 HTML 重建／二次解析；
相同 11 份真實回應、相同 10 篇正文與 1 個來源空模板，七輪重播的解析 CPU 降低 **14.74%**，
含正式大小批次、壓縮、持久寫入和收據核對的鏈路 CPU 降低 **15.87%**、wall 降低 **9.13%**。
另修復監測自身長掃描造成的 false stale：不提高 45 秒門檻，分開初讀與完成 heartbeat、PID／owner 驗證；
精確統計沿用共用交易式 head 計數，同一 WAL 的兩輪完整索引 oracle 得到相同 **3,569,010** 個頭、**307** 個已有正文重查，
計數 wall 中位數 **31.010 → 4.315 秒**。正式驗收全部 **12/12** 通過；同一 owner **1080.744 秒**
新增鉅亨 **434**、Global Voices **91** 個正文頭，最近 128 批 **4,375** 個版本無收據／SHA 錯誤。
期間鉅亨 **27 次 502、7 次傳輸失敗／7 次 session reset**；端點隔離後仍持續入庫，診斷／checkpoint 寫入錯誤為零。
當前 **0.5 秒／3 並行**是恢復控制值；實際為 **0.402 新頭／秒、0.805 HTTP／秒**，不是設定值換算的保證速度。
最後新聞回歸 **2,268 項／79.56 秒**通過。本機四尺寸進度對上快照；本次未重新驗收 WAN，先前對外 HTTPS timeout 證據保留。
詳見[鉅亨廣告排除與只讀監測優化](#鉅亨廣告排除與只讀監測優化)。以下有日期的舊摘要和原始失敗收據保留，不是當前數量。

2026-10-11 **01:29 持續驗收摘要**：鉅亨同一程序的 **574.609 秒**窗口新增 **951** 個正文 URL 頭，
約 **1.655 新頭／秒、2.317 HTTP／秒**；Global Voices 新增 **57** 頭。12 項持續運行檢查全部通過。
來源仍出現 502 與傳輸失敗；本次核對 17 個已啟用來源，鉅亨保留 **16 次傳輸失敗／session reset**、
**28 次 host 故障隔離決策**，無診斷／checkpoint 寫入錯誤。這是已驗證窗口，不是官方最高速或全天無錯誤。
較早 173 秒窗口的 2.563 新頭／秒不能當作持續速度；當前 controller 以 0.2 秒／6 並行有界恢復，不放大成 512 秒等待。
另外修復有限清冊的非正文分類與網頁：**2 個原址實際 HTTP 410、1 個行政區域頁不是新聞**，
有不可變觀測收據且不計入已存正文；原批次 1 篇、追加 3 篇正文完整保留，待核對／待排網址由 3／3 正確顯示為 0／0。
新聞回歸 **2,174 項／66.81 秒**、新聞頁面專屬 **80 項／16.71 秒**通過；
最新 128 批 **5,087** 個正文版本無收據／雜湊錯誤。本機四種尺寸通過，對外 HTTPS 仍未通過。
共用網頁整份測試另有 1 項 FinLab 舊標籤期待失敗（124 項通過），保留證據、不改動其並行工作。
詳見[範圍外網址分類與持續抓取](#範圍外網址分類與持續抓取)；先前台南新增 3 篇與各時間的實測收據仍保留。

2026-10-10 **23:49 持續驗收摘要**（同 owner 長窗口與固定失敗清單均為 23:49 快照）：
鉅亨原始 40 筆失敗網址仍為 **23 已存正文、16 重試、1 身分未確認**；原 23 筆的原文與不可變收據證明保留。
先前字面 `<…>` 解析修復的一篇 **1,516 字元**正文已在 22:55 自動補回，通過當前 parser 與收據驗證。
新修復讓公開失敗回應沿用下載器的身分／作者／時鐘驗證，保留經確認的 `exp` 變體，
並以五個固定階段計數正文失敗；健康文章不重複診斷、不增加 HTTP。
「排空後沒有到期工作」改為零請求的明確跳過收據，不再中斷診斷流程；不清除來源拒絕、未來 deadline 或日診斷預算。
完整新聞回歸 **2,153 項／49.15 秒通過**。最新同 owner **971.311 秒**新增鉅亨 **1,343**、Global Voices **87** 個正文 URL 頭，
全部 12 項持續運行檢查通過；抽查最近 128 批、**5,807** 個正文版本，收據／雜湊無錯誤。
該窗口鉅亨為 **1.383 新頭／秒、1.969 HTTP／秒**；原 23:38 的 397／27 新頭及 5,967 版本證據仍保留，
「最近 128 批」是滾動集合，不同快照的版本數不能相減當作新增量。
三個失敗編號的正式網址已有 **1,452／1,054／378 字元**正文，已逐筆當前重播驗證；
這不等於其查詢變體已驗證，不改變原 40 網址的完成量。
較早 22:12 至 22:37 窗口新增鉅亨 2,483、Global Voices 131 頭的證據仍保留；不同窗口不能當成匹配工作量速度比較或官方最高速證明。
Global Voices 的完整 40 字短文及新發現的 82 字影片旁正文均已入庫，照片／影片不冒充文字正文。
來源 502／timeout 及 CMoney／PeoPo 真正 HTTP 403 仍有發生；各自保留拒絕／恢復證據，
未註冊／未授權來源及中央社 API 缺少憑證也沒有算成完成。
詳見[鉅亨排程與格式修復](#鉅亨失敗頁與到期工作餓死修復)及
[Global Voices 分類與短文回補](#global-voices-缺正文分類與短文回補)、
[鉅亨正文表示與來源故障驗收](#鉅亨正文表示與新來源故障驗收)及
[最新鉅亨失敗診斷與已有原文驗收](#鉅亨失敗診斷與已有原文驗收)。

本文較早 v7 快照保留原範圍。2026-10-10 凌晨最新資料見[正式續抓與 Fed 文件修復](#凌晨正式續抓與-fed-文件修復)：
該凌晨清冊為 741 個來源鍵；22:10 status 為 **742** 個來源鍵、59 個設定來源、17 個已啟用來源。
來源鍵包含聚合器／轉址，不代表 742 家獨立新聞社；較早清冊仍保留當時分母。
後續來源誤判與 RSS 恢復見[來源誤判與 RSS 修復](#來源誤判與-rss-修復)；歷史快照不覆寫成當前狀態。
2026-10-10 01:57 後的清冊計算、重試排程及繁中 RSS 修復見[清冊同步與到期重試](#清冊同步與到期重試)。
2026-10-10 02:11–02:52 的鉅亨短原文、API 快取與狀態修正見[鉅亨原文補漏與快取修正](#鉅亨原文補漏與快取修正)；
較早的「metadata-only 頁數」包含混合頁，不能視為全部缺正文，詳見本節的統計更正。
2026-10-10 下午最新的全來源清點、鉅亨標題邊界空白修復與實際續抓，見[鉅亨標題修復與全來源錯誤核對](#鉅亨標題修復與全來源錯誤核對)。
2026-10-10 17:11 的續抓驗收與本機重複統計掃描修正，見[鉅亨耗時分解與狀態統計優化](#鉅亨耗時分解與狀態統計優化)。
2026-10-10 晚間的真實失敗頁、CMNews 補漏與 oldest-due 排程根因，見[鉅亨失敗頁與到期工作餓死修復](#鉅亨失敗頁與到期工作餓死修復)。

## 結果先看

新聞服務與 timer 已恢復，仍使用原來的單一 owner、共享 HTTP 限流與不可變 Parquet 收據。
新增 44 個來源設定，總共 55 個；6 個啟用，49 個仍是待核對的候選設定。
**寫了來源設定不等於該網站已通過實際正文下載驗收。** 尚有 683 個未註冊 hostname 鍵，
包含原始媒體、聚合器、轉址服務、社群、公司公告與其他來源；不能將它們一律當成不同新聞社。
目前仍不能宣稱「所有網站已實作驗收」「全部問題已解決」或「全部歷史已抓齊」。

完整資料與下一步逐列保留，不只列熱門網站：

- [738 個來源鍵的 CSV](../artifacts/data_quality/news_all_source_repair_20261009/inventory-v7-final.csv)：URL、已存正文、既有正文重查、缺正文待辦、metadata 首末日、來源類型、設定、限制、錯誤與下一步。
- [一致 WAL 清冊](../artifacts/data_quality/news_all_source_repair_20261009/inventory-v7-final.json)與[全部已登錄端點](../artifacts/data_quality/news_all_source_repair_20261009/inventory-v7-final.endpoints.csv)。
- [新增來源設定](../configs/news/publisher_catalog.json)與[共用下載設定](../configs/news_fulltext.json)。
- [新聞進度頁](https://penguin72487.ddnsgeek.com/news/)：本機 gateway 已驗證；本次對外 HTTPS 連線逾時，**未完成外部入口驗收**。

10:20:13（02:20:13 UTC）一致快照：3,774,545 個已知 URL、868,450 個已存正文；
其中 2,137,833 個 URL 尚未啟用。這些數字是固定快照，不是全新聞歷史總量。

| 已啟用來源 | 已知 URL | 已存正文 | 剩餘情況 |
| --- | ---: | ---: | --- |
| 鉅亨 | 1,177,342 | 853,339 | 323,984 個缺正文就緒工作；2,111 個已有正文重查 |
| Fed | 13,678 | 13,599 | 75 個缺正文、4 個影像 PDF／無可抽文字待修；不是全完成 |
| Global Voices | 109,533 | 1,500 | 108,031 個缺正文就緒工作、1 個缺正文錯誤、1 個來源撤稿 |
| 環境資訊中心 | 70 | 12 | 58 個第三方／書摘等排除範圍，不算完成 |
| CMoney | 271,360 | 0 | 來源 403 退避；12 個網址路徑未支援 |
| CMNews | 64,729 | 0 | robots 回傳首頁 HTML；來源 circuit，不猜規則 |

## 第一性原理：真正的工作與瓶頸

輸入是 FinLab／FinMind 已接受 metadata 與來源實際提供的 feed、API、索引頁；不是猜新聞 ID。
流程是唯一 URL 註冊 → 來源准入／robots／限流 → 公開回應 → 原文與來源時鐘辨識 →
去重 → 壓縮／fsync → 檔案與正文 SHA → 原佇列提交 → 唯讀快照。
候選 URL、HTTP 200、正文完整性、合法用途與全歷史覆蓋是不同證據。

| 根因 | 已實作修正 | 保留的正確性條件 |
| --- | --- | --- |
| 每次派送掃描、排序整個歷史 discovery 清單 | 新增符合真正 ORDER BY 的部分索引；暫停端點在 LIMIT 前過濾 | 工作優先順序不變；舊工作與失敗證據不刪除 |
| 一頁鉅亨缺正文／署名，整個 provider 的批次都冷卻一小時 | Cnyes 解析 circuit 改成逐註冊分類端點；舊失敗 deadline 一次遷移，其他 64 個分類可繼續 | 真正 403／429／Retry-After 仍共同退避；不將解析失敗當成限速 |
| DOM 被序列化、再解析與重複巡覽 | 已解析節點直接抽取；簡單排除規則使用有界快取與 native lxml 路徑；複雜 selector 保留相容 fallback | 凍結原始回應逐篇比較正文、日期、署名與 SHA，不少抓、不改原文 |
| 每個 URL 遍歷所有 provider／ASCII 也做 IDNA | hostname suffix 索引，ASCII 快路徑 | Unicode 域名、精確／最長 suffix 與未知來源行為保持 |
| UTF-8 正文重複 encode 以算 bytes／SHA | 同一次編碼供兩者使用 | 原始值與 SHA 契約不變 |
| robots 合法轉址被當成非法路徑 | 同來源、公開地址與有界轉址核對 | HTML／登入／challenge 不能當 robots 規則；不能拿另一 hostname 繞過拒絕 |
| 無效 robots 回應每篇文章都重試 | 來源 circuit，其他 provider 獨立處理 | robots_disallowed 仍是逐 URL 的永久結果 |
| 全來源清冊反覆為沒有錯誤的 hostname 做查詢 | 一次 GROUP BY，僅實際有錯誤來源查樣本 | 完整保留全部來源；不縮成 Top K |
| 本文已存、只是追新失敗，進度卻像未曾下載 | 既有正文、complete 狀態、新增正文及重查分開顯示與驗證 | 已存正文不是當前已最新；失敗不是完成 |

hostname 匹配由逐 provider 搜尋改為依 host 標籤數搜尋。
DOM 處理仍需讀取原文，整體仍為 O(N)；優化的是重複解析、物件建立與掃描常數，
不是宣稱所有正文處理都能變成 O(1)。派送索引避免反覆全掃描／臨時排序，實際代價仍受就緒工作位置與被暫停端點影響。
沒有新增無界正文快取、GPU 工作或多套重複爬蟲。

## 各來源的實際修復

### Anue 鉅亨

65 個已核對分類共用來源限流。每次 API 實測最多 30 則，日期窗遇到來源 page cap 會分割；
付費／公開文字混合頁不保存受限制的共用原始全文，保留合法 HTML 連結與可見缺口。
既有正文及作者不因 API 缺署名而覆寫；不同分類有重疊，先用 URL／正文 SHA 去重。

v5 正式觀測曾出現 205 秒 **0 新正文**：一個 `mag` 頁面的解析失敗把全部分類冷卻。
修正後仍保留 `mag` 端點的一小時期限，健康分類不再被拖停。
v6 同 owner 150.827 秒新增 **4,934** 個正文 URL（32.71／秒）；HTTP counter 的獨立 151.037 秒窗口
為約 **4.62 request／秒**，不是 32.71 個 HTTP request／秒。
兩個時鐘與新增正文／修訂版本不能混用。這不證明來源最大速率或最早完整歷史。
v7 最新正式觀測的 126.996 秒 WAL 窗口新增 **11,606** 個鉅亨正文 URL（91.39／秒），
HTTP counter 的獨立窗口約 **9.81 request／秒**；仍沒有將兩者混成「呼叫成功率」。

### Federal Reserve Board

延伸原抽取器，不抓整頁。v4-v7 依來源實際 DOM 核對：明示 release date、直屬 font、
兩格 release header、同表後續正文列、聯合新聞稿、White House transcript、FOMC `#leftText`
及 policy implementation note 的精確正文區塊。
保留列表、數值、統計／正文表格；不把頁尾 Last update 當發布日。

[v6 兩篇](../artifacts/data_quality/news_all_source_repair_20261009/selected-v6-body-proof.json)與
[v7 三篇](../artifacts/data_quality/news_all_source_repair_20261009/selected-v7-body-proof.json)
均經真正來源 HTTP、canonical capture、Parquet／原始回應／正文 SHA／佇列 head 核驗。
2007-10-31 FOMC minutes 的內文明示的是開會日，頁尾另有更新日；沒有明示發布日，
`published_at_raw=null`，不得自動當成 2007-10-31 已可交易的 feature。
部分版型／影像 PDF 仍需後續處理，不將失敗全部怪給來源。
v7 已在正式服務自動取得另 130 篇正文；剩餘 75 個 HTML 版型及 4 個影像 PDF 缺口保留待修。

### 環境資訊中心

只接受 [原創授權範圍](https://e-info.org.tw/copyright) 的公開原創報導：
核對 Next `Post` id、published state、同頁 h1、記者 metadata 與可見署名。
舊文章只有「記者 — 姓名」而沒有「環境資訊中心」字樣，現在可正確辨識。
本機 70 個已知 URL 中 12 篇已驗證正文，58 篇為第三方、書摘／電子報等排除範圍；
排除不算下載成功，也不是可藉重試解決的 parser error。
網站原創預設政策與逐文章授權標記分開；沒標記就不偽造 `per_article_license_verified=true`。
此 CC 政策不是自動轉載、商用或模型訓練的全面許可；本次僅私人原文保存，沒有注入訓練。

### Global Voices、CMoney 與 CMoney 投資網誌

Global Voices 既有具正文／作者的 RSS 與 HTML 路徑沿用。
一篇明示因安全原因撤下的文章改成 `source_body_removed`；不找快取或翻譯頁覆寫撤稿。
另有一篇缺正文仍待查，不將它假定為來源錯誤。
CMoney 仍收到來源 403；CMNews robots 轉回首頁 HTML，因此 `robots_not_plain_rules`。
兩者仍保留各自待辦與來源退避，不算成功、不因重啟繞過，也不讓它們堵塞其他來源。
私人研究用途與資料再散布／訓練權利分開；[來源條款](https://www.cmoney.tw/member/tos.aspx)未被當成全面重用授權。

## 新增來源的狀態

新增設定包括自由／自由財經、ETtoday、東森、台視、FTNN、今周刊、台灣好新聞、理財周刊、
民視、UAnalyze、Vocus、三立、風傳媒、TVBS、Genet、鏡週刊、中央社、壹蘋、財訊、CTWant、
Newtalk、今新聞、NOWnews、好房網、商周、太報、數位時代、信傳媒、區塊客、鏡報、中天、
遠見、PeoPo、上報、公視、匯流、新浪、蘋果歷史網址、永豐研究、TradingView 與 Yahoo 台灣等。
另有本次已啟用的環境資訊中心及 CMNews；完整 44 列以 catalog 為準。

候選使用同一套 declared JSON-LD／itemprop／精確 CSS 抽取與保存流程；
**其 selectors 尚未全部實測，不應將共用解析器或註冊欄位當成逐站成功下載器。**
不得把來源不明的摘要、相關文章、付費正文或全頁導覽當成本文。
剩餘 49 個未啟用設定與 683 個未註冊鍵需要逐站核對來源用途、正文／時鐘、存取規則與歷史端點。
Google News／MSN／LINE／社群轉址尤其不能當原文作者或默認授權者。

## 可重跑的效能與驗證

[最新 v7 凍結回放](../artifacts/data_quality/news_all_source_repair_20261009/cpu-v7-replay.json)
保留輸入檔案／回應 SHA、程式／設定 SHA、所有重複輪次、profile、慢例與驗收，而非只挑最好一次。
這個固定 cohort 有鉅亨／Fed／Global Voices 各 32 個回應，共 96 個。

| 固定工作 | 參考版 CPU 中位數 | 優化版 CPU 中位數 | 解讀 |
| --- | ---: | ---: | --- |
| 32 個已保留鉅亨 API 回應，915 篇正文；解析、去重、壓縮／fsync、SQLite、獨立核驗 | 2.1247 s | 1.1355 s | CPU 減少 46.6%；同樣 915 個接受 head、0 hash 差異 |
| 96 個混合來源回應的正文抽取 | 4.5914 s | 3.1099 s | CPU 減少 32.3%；不是所有網站同幅改善 |
| 57,142 筆一致 WAL frontier；六來源派送查詢，7 輪交錯 | 0.09646 s | 0.004032 s | 查詢 CPU 約 23.92 倍；排序及選出的 URL 完全一致 |

鉅亨上述全流程 wall 中位數 5.4174 → 4.3022 秒，減少 20.6%。
第三列只是選工作查詢，不能推論整個爬蟲快 24 倍；前兩列不含來源網路等待，不能推論官方速率上限。
[較早 v6 cohort](../artifacts/data_quality/news_all_source_repair_20261009/cpu-final-indexed-replay.json)
的鉅亨 CPU 減少 32.7%、混合 HTML 減少 6.6%、53,494 筆清單查詢約 52 倍，也完整保留。
輸入構成與就緒工作位置改變會影響結果，不能只挑更好的一項或將跨 cohort 差異當成單一演算法效果。
基準可能與正常背景服務重疊，沒有宣稱整機隔離的性能極限。

v7 全來源新聞相關回歸 **526 項通過**（51.88 秒），run `news-v7-all-regression-20261009T021635-88425f13`；
新 fixture 的 focused 186 項通過。較早 v6 正式總驗收仍為 false，
因那段窗口 Fed 的剩餘待辦沒有新增正文，沒有把驗收要求改低或用其他來源的進展遮掩。
[最新 128 個近期批次](../artifacts/data_quality/news_all_source_repair_20261009/recent-v7-body-proof.json)
共 **3,153** 個正文版本通過檔案／原始回應／正文 SHA 與 head 核驗，0 錯誤；不代表整個 lake 已驗證。
[v7 正式工程驗收](../artifacts/data_quality/news_all_source_repair_20261009/runtime-v7-final.json) 9 個條件通過：
同一 PID／owner、來源政策／程式碼一致、就緒且未被來源暫停的 Cnyes 與 Global Voices 分別增加正文、
近期收據通過、限流間隔及不假稱全歷史完成。這段 127 秒 Global Voices 新增 11 篇。
Fed 此時已清掉可以修復的本輪就緒工作，仍剩 79 個錯誤；CMoney／CMNews 來源冷卻，Einfo 有排除範圍，
因此不在該「就緒佇列要有進展」條件中。排除理由逐來源記錄，**不是全 provider 成功證明**。

初始化亦有成本：一個新 FinLab news snapshot 的 442,075 筆 metadata 需要去重、保留來源關聯與版本。
v6 該次 ingest 79.3 秒 wall／61.7 秒 CPU，已與 HTTP 解析成本分開。
沒有透過略過新 metadata 或驗證來美化效能數字。

## 網頁與操作邊界

[最新本機頁面驗收](../artifacts/data_quality/news_all_source_repair_20261009/dashboard-final/acceptance.json)
測試 1440、768、390、320 像素：原生篩選、搜尋、分頁、折疊與 API。
738 個來源鍵分 31 頁，55 個設定來源可看流量，沒有橫向溢出或頁面呼叫新聞網站。
採有限狀態與數字投影，不傳正文、raw HTML、帳號、金鑰、任意錯誤字串或私人檔案路徑。
端點暫停、來源拒絕、缺正文、原創／授權未驗證、來源撤稿與未啟用分開。
已知 URL 的進度不等於媒體全歷史；unknown ETA 留 unknown，不顯示假倒數。

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.audit_news_providers --output artifacts/data_quality/news_inventory.json
run_fintech_python -m scripts.benchmark_news_cpu --responses-per-provider 32 --repeat 3 --output artifacts/data_quality/news_cpu.json
run_fintech_python -m scripts.verify_news_recent_batches --maximum-batches 128 --output artifacts/data_quality/news_recent.json
run_fintech_python -m scripts.verify_news_dashboard --base-url http://127.0.0.1:8770/news/ --output-dir artifacts/data_quality/news_dashboard
```

上述皆為唯讀／隔離私人回放，不占 production queue owner。live probe／selected fetch 必須先
確認新聞 owner 已 drain，使用自動恢復保護，不能一邊跑正式服務一邊另開 HTTP 來源管線。
沒有改變既有 403／429／robots／Retry-After 或 paid/login wall 限制。
剩餘工作依清冊來源、有效規則與可用端點續做；不把解析候選數、service active 或私人保存當成訓練／用途／全歷史驗收。

## 凌晨正式續抓與 Fed 文件修復

2026-10-10 00:21:55 Taipei 的一致 WAL 清冊有 **741 個來源鍵、59 個設定來源、17 個啟用來源**，
共 5,270,352 個已知 URL、2,472,445 個有保存正文的 URL；另有 2,130,117 個 URL 尚待授權。
來源鍵含 hostname、轉址與聚合器，不等同 741 家新聞社。所有來源仍逐列保留，沒有將拒絕或未審核來源算成完成。

- [全清冊 CSV](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v18/providers_final.csv)、
  [一致快照 JSON](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v18/providers_final.json)及
  [端點清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v18/providers_final.endpoints.csv)。
- [741 列後續規劃](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v18/source_plan_final.json)：
  接線、來源條件、錯誤分類及下一步，不是逐站已驗收證明。

### Fed 的根因與資料界線

從 2026-10-09 23:37 Taipei 的 13,679 個已知 URL／13,600 個正文 head，
收斂到本次 **13,868 個已知 URL／13,861 個正文 head**。
已知 HTML 的 `body_not_found`／署名驗證失敗為零；四份 PDF 維持無可抽取原生文字狀態。
`fed_table_release_v10` 僅更新 Fed 的 capture fingerprint，其他來源仍沿用原合約、bucket 與排程。

| 根因 | 修復 | 保留的界線 |
| --- | --- | --- |
| 聯合公告的發布宣告分成兩個儲存格、日期前後有空白或為西文 | 只抽發布列之後的實際正文；保留兩欄時刻與時區、`Fecha`／西文日期 | 不納入機關 masthead、聯絡資料或頁尾更新日 |
| 空白置中區被當成標題；直接發布列夾帶翻譯連結 | 空白區回退到原生頁面標題；只從發布列排除翻譯連結 | 不自造標題、作者或發布時刻 |
| 會議紀錄以 `A joint meeting` 或分離前言呈現 | 捕捉核對過的正文欄與前言 | 會議日期與頁尾不是發布觀察，未宣告的發布時間仍為未知 |
| 預測表以 h4／h5 呈現；早期可讀表格未列發布宣告 | 保留原生數值、表頭與註解；有宣告才保存發布字串 | 無宣告的表格仍可保存，但不把標題、會議日、URL 或 HTTP Last-Modified 回填成發布時間 |
| 實施公告的日期前導零、全寬／雙欄差異 | 比較日期意義，保留原寫的 `May 04`；只取正文欄 | Related Information 側欄不入正文；無效或不一致的宣告不被吞掉 |
| 2011 影片入口頁含真正逐字稿 PDF 連結 | 從已存在的官方索引連結登錄 `/mediacenter/files/FOMCpresconf…pdf` | 影片入口不是正文，PDF 另有原回應與保存收據，不猜檔名或下載影片 |

現仍有 **四份影像型 PDF**（2000-03-21、05-16、11-15、12-19）未取得原生文字，
以及 **三個 2011 影片入口頁**標記 `source_document_landing_page`。
入口頁實際連出的官方逐字稿已保存；已接入命名空間共有 91 份逐字稿 PDF，91 份都有正文。
基線之後新登錄的 189 個 URL 為 126 份官方 PDF（包含上述逐字稿）與 63 個 HTML 文件，全部都有正文。
這是已知清冊收斂，不是 Fed 官網所有文件或所有年份的完整性宣告。
未新增 GPU／OCR、未修改用途許可、未注入訓練；[官方資料用途聲明](https://www.federalreserve.gov/disclaimer.htm)的原創資料與第三方例外仍適用。

### 實際保存與恢復驗收

[24 個真實來源回應重解析](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v18/fed_v10_probes_reparsed.json)
全部通過原始 Parquet／response SHA、來源身分與正文或明確入口頁結果驗證。
[16 筆已入庫 HTML／PDF](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v18/fed_v10_selected_heads_verified.json)
另從 canonical raw response 重播當前解析器，驗證正文、標題、發布字串、作者、batch／head 收據；不是只驗 HTTP 200。

修補中曾誤改舊分支，12 筆公告以 `worker_NameError` 進入有限重試，原證據保留。
修正後[12 筆實際重試的完整保存核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v18/fed_v10_incident_retries_verified.json)
全數通過，不再有該錯誤或 HTML 缺正文狀態。維護要先停止唯一新聞 owner／timer、保留 leases 與原收據，
在 focused tests／原始回應驗證通過後恢復；來源拒絕、付費、robots 與既有冷卻不清除。

[正式同 owner 驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v18/runtime_v10_accepted.json)
九項條件通過。157.910 秒的 DB head 窗口鉅亨新增 13,683 筆、Global Voices 新增 13 筆；
Fed 已無可派送的缺正文工作，以剩餘狀態和實際文件收據另驗，而不是要求 idle 來源憑空新增。
HTTP counter 另有自己的 151.620 秒窗口；鉅亨約 7.572 requests/s，包含本機回收的總 head 增長不代表純線上容量。
[最新 128 批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v18/recent_batches_v10.json)
共 1,853 個正文版本核驗零錯誤。26 個相關測試檔共 **1,006 項通過，31.49 秒**，未宣稱全專案 suite、官方上限或全來源成功。

本機進度 API 在 00:26:46 Taipei 回報 `health=running`、`read_only=true`、`snapshot_consistent=true`，
共有 741 個來源鍵，不傳金鑰、原始正文或操作控制。
鉅亨和 Global Voices 仍持續回補；CMoney／PeoPo 的來源拒絕與其他來源的付費、下架、第三方權利或原文身分問題仍在清冊上。
全來源目標維持進行中，本次不宣告全部修復完成。

### 後續 HTTP 500 的實際恢復

00:31:51–00:34:29 Taipei，鉅亨六個 API 分類共回傳 41 次 HTTP 500，
不是單一分類解析例外；沒有 `Retry-After` 宣告。[原來源診斷與隊列觀察](../artifacts/operations/agent-workflow/runs/cnyes-v10-http500-access-recover-20261009T163948-cca8bc68/run.log)
保留每個失敗 URL、來源狀態及時鐘，不讀取錯誤頁正文或輸出 cookie。
既有 client 在單次錯誤時保留單筆重試，連續錯誤時開 provider circuit，按期限恢復，其他 provider 仍繼續。

00:43:25 的[同 41 個 URL 精確恢復檢查](../artifacts/operations/agent-workflow/runs/cnyes-v10-exact-error-url-recove-20261009T164325-9438b11a/run.log)
驗證診斷檔 SHA，以及每筆後續已通過來源驗證的 discovery 時鐘與 retained response SHA，**41／41 已恢復**；
沒有該錯誤仍停留在 current jobs。這是傳輸／來源頁工作恢復，不額外宣稱來源內部故障已根治、每頁都允許取得全文或未來永不出錯。

[00:39:52 的後續正式驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v18/runtime_v10_final.json)
仍為相同 PID／owner、九項通過；包含這次退避的 **1,048.199 秒 DB 窗口新增 113,979 個鉅亨正文 URL（108.738/s）**，
Global Voices 另新增 90 筆。HTTP counter 是獨立的 1,056.374 秒窗口，約 6.433 requests/s。
head 增長包含本機回收，不能拿來宣稱純線上／官方容量。
此時鉅亨有 2,765,649 個已知 URL、2,503,000 個已保存正文 head、262,617 個缺正文待辦，仍未完成全歷史。
[後續 128 批核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v18/recent_batches_v10_final.json)
另驗證 2,674 個正文版本，零 SHA／收據錯誤；不與前一輪重疊樣本相加成全庫覆蓋量。

## 來源誤判與 RSS 修復

2026-10-10 01:30 Taipei 再次恢復原新聞 service／timer。仍為唯一 queue writer；
維護中停止／排空原服務，測試及限定原網址恢復後才交回正式 owner，不在執行中改下載器。
本輪沒有提高來源流量、繞過會員／付費／拒絕，也沒有新增平行管線。

| 已確認根因 | 修正與實際結果 | 尚未取得的部分 |
| --- | --- | --- |
| Global Voices 新聞標題含 `Access denied`，共用 substring 判斷誤開全 provider 的 900 秒冷卻 | 僅在已核對的新聞模板、請求 canonical、署名、本文及文章授權一致時排除這個標題誤判；原新聞實際保存 **7,370 字元**，正文／原回應／檔案／head SHA 與當前解析器重播通過 | 真正 challenge、HTTP 拒絕和 robots 限制不豁免；不把這一篇當全站完成 |
| RSS 本文提到外部專欄有「paywall」，誤將整頁判成受限制正文，歷史停在 page33 | 改驗真正付費控制 DOM／結構化宣告，不以正文或外部連結的字眼判定；原 page33 從 **14 可用／1 誤判**變成 **15 可用／0 錯誤**，正式排程已向後續頁推進 | RSS 沒有官方全歷史總筆數；有效篇數不等於新增或保存篇數，已有正文照常去重 |
| RSS 沒有 `total`，成功重抓後未更新舊單篇錯誤投影 | 成功的 HTTP200 解析更新當前 item errors；304 不擦掉錯誤。精確已修復的舊投影保留在 queue audit，再從當前錯誤清單退場 | 後續新錯誤與未成功重抓的項目仍保留，不把錯誤全部清零 |
| Global Voices 2019 原頁只剩帶日期的安全撤文公告，被當缺正文 | 原文明示 2026-01-21 撤下；精確原網址重查後變成 `source_body_removed`，沒有正文 head | 不從快取、翻譯或別篇回填已撤文本文 |
| 理財周刊五篇 HTTP200 回應只有會員登入遮罩，卻報 `body_not_found` | 在當前 canonical 的實際 articleBody 中檢查遮罩，早於預覽／JSON-LD fallback；五筆均改成 `paywall_or_login`，原有 **552** 個正文 head 不改 | 三個真正空模板、39 個非文章轉址及排除範圍分別保留；未取得付費原文 |

只有 source capture contract 改變時，才對精確註冊的 RSS 解析失敗排一次重查；
原錯誤、deadline、attempts 和變更契約記錄保留。同版重啟不重排，不清除 HTTP 拒絕、provider 冷卻或 article leases。
RSS HTML fragment 只解析一次，供正文及存取標記檢查共用；沒有縮減正文範圍或自造發布時間。
本輪未做新的隔離 CPU benchmark，不以這個機制推論所有來源的加速百分比。

### 可核對證據

- [一致全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v19/provider_inventory.csv)、
  [JSON](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v19/provider_inventory.json)及
  [端點清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v19/provider_inventory.endpoints.csv)：
  01:20:24 Taipei 固定快照，741 個來源鍵、59 個設定、17 個啟用；**不是最後續抓後的即時數值**。
- [理財周刊精確隊列結果](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v19/source_queue_outcomes_verified.json)：
  七個原始回應 SHA 與當前原網址／policy／非正文狀態核對；五筆會員限制與兩個空模板，不是七篇成功正文。
- [Global Voices 原來源診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v19/globalvoices_source_verified.json)、
  [已恢復正文的獨立保存核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v19/globalvoices_recovered_head.json)及
  [撤文的精確隊列證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v19/globalvoices_final_outcomes.json)。
- [原 RSS page33 重測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v19/globalvoices_feed_after.json)：15 個來源明示正文、作者及時鐘通過；沒有保留混合受限制的 feed 回應。
- [唯讀面板核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v19/dashboard/acceptance.json)：
  本機 gateway 的 1440／768／390／320 寬度通過，要求核對 Cnyes／Global Voices／Moneyweekly。
  這不等同外部 DNS／HTTPS 入口已驗收，後續即時內容仍由正式 owner 快照提供。

最終 25 個相關測試檔 **1,041 項通過（34.03 秒）**，
run `v19-complete-relevant-regression-20261009T172844-e79f47cb`；不是全專案 suite。
CMoney／PeoPo 的來源拒絕、ZHT RSS 的 `invalid_feed`、民視部分署名／原文身分，以及未接入的授權來源仍需後續處理。
這些問題獨立列出，不用單一來源恢復來宣告所有 provider 成功或全歷史完成。

### 恢復後鉅亨實際續抓

[01:32:05–01:33:26 Taipei 同 owner 觀測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v19/runtime_restored_progress.json)：
**81.291 秒 DB 窗口新增 7,694 個鉅亨正文 URL head（94.648/s）**。
HTTP counter 的獨立 75.831 秒窗口為 605 次請求（7.978/s），604 個已收到的回應為 HTTP200；
有請求跨越邊界，不能用這個分母算全歷史成功率。
該 counter 窗口新保存 head 為 6,793，其中 6,638 來自線上 bulk、155 來自本機原回應回收；
DB 與 counter 時鐘不同，不能直接要求兩個增量相等，亦不宣稱官方／可持續速率上限。
正式 PID `2287156`、同 owner、政策與 source code 一致，沒有 restart。

Global Voices 在相同 counter 窗口驗證八個 RSS 頁、120 個正文身分，**沒有新增 head**，
因所遇文章已有正文而去重；不要把有效 RSS 篇數當新增保存。
01:34:22 的 queue 讀取證明英文 RSS 已前進 page83、該頁錯誤為零，
但仍有 104,607 個已知缺正文待辦。這只證明卡住的歷史分頁已恢復，不算全 provider 新正文收斂驗收。
上述個別 2021 新聞的成功恢復由限定 canonical fetch 與獨立 head 收據另證，沒有混入相同 owner 的吞吐窗口。

[最新 128 批保存驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v19/recent_batches_final.json)
核對 **2,098 個正文版本**，0 檔案／原回應／正文 SHA 或 head 錯誤；仍不是全庫稽核。
[最後面板核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v19/dashboard_final/acceptance.json)
四個視窗寬度再次通過。來源撤文、會員限制和非文章路徑不改成完成，所有新發現與既有拒絕仍列在清冊上。

## 清冊同步與到期重試

2026-10-10 01:57:43 Taipei 恢復原 service／timer。清冊同步已減少無效 CPU 計算；
批次歷史下載仍為主要工作，到期錯誤與 HTML 補漏新增固定處理機會。
維護期間停止並排空原 owner；沒有改來源流量、金鑰、會員權限或原始觀察。

### 清冊同步的實測

一次沒有新增分區的清冊同步，也會每 100 份收據重算整份狀態，
將 coordinator 擋住約 53 秒。改成每 15 秒及最終完成時更新面板；
所有收據仍經原本的版本與路徑核對；新增或變動且尚未接受的分區檢查 SHA、筆數與 schema，
已接受的相同版本不重讀全文。來源變更仍沿用原註冊流程。

| 本機完整清冊同步 | 修正前觀測 | 修正後實跑 |
| --- | ---: | ---: |
| FinMind 接受收據及已檢查份數 | 5,664 | 5,664 |
| 新增 metadata 分區 | 0 | 0 |
| 階段 wall time | 52.861 秒 | 2.131 秒 |
| 階段 CPU time | 51.605 秒 | 1.799 秒 |
| 完整狀態投影 | 程式推得 57 次，非逐次 trace | 實測 1 次 |

此完整清冊階段 wall 減少約 96%、CPU 減少約 96.5%；不是全服務吞吐量提高 25 倍。
修正前是[正式 owner 的已記錄遙測觀測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v20/inventory_before_observed.json)，
不是另存的原始 status 快照；修正後為[完整 CLI 清冊結果](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v20/inventory_after.json)。
新索引首次建立及 owner 初始化另計；有變動的大分區仍是同步匯入，不能將此次零變動耗時當其上限。

### 不中斷歷史下載的補漏排程

鉅亨修正前仍有 6 個 `http_502`、23 個 `transport_error`、1 個 `stopping`
缺正文重試；已過期限的錯誤可能被較新的 pending 文章和大量 bulk 工作擠在後面。

- 每個 provider 的歷史 bulk 最多連續派送 16 個工作，之後有可用文章時讓出一個 HTML 補漏位置；即時發布 feed 保留優先權。
- 文章派送累積 8 個非錯誤缺正文工作後，啟用到期錯誤的固定處理位置；既有重查／無日期的公平輪次可先處理，之後接續到期錯誤。新增小型部分索引按到期時間排序。
- 原先既有正文重查及無發布日文章的公平處理規則仍在，各來源獨立計數。
- 空補漏通道不佔空位；未到期、在 worker 租約中、來源退避中的工作不提前抓。

`article_dispatch_contract_version=3` 僅改派送契約，不改已保存正文、作者、發布時鐘或 source capture fingerprint。
上述比例是派送公平性的上界，不是文章完成時間保證或官方請求額度。

### 繁中 RSS 來源錯誤與原文補抓

[原 feed 實測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v20/zht_feed_controls.json)
發現 `zht.globalvoices.org/feed/?paged=12` 的正文 CDATA 有兩個 `U+0008`，
不符合 [XML 1.0 字元規則](https://www.w3.org/TR/xml/#charsets)，因此整頁原本無法解析，下一頁也未排入。

僅在非法 ASCII 控制字元全部位於明示 `content:encoded` CDATA、暫時 metadata view 能嚴格解析時，
救回未修改的 URL、標題與發布字串。所有正文都改從原文章頁取得，
不使用 `recover=True`、不保存改寫正文，也不將 metadata-only 頁標成正文完成。
若字元在 URL／日期／結構，或仍有其他 XML 錯誤，維持錯誤，不能猜修。

[Canonical 端到端結果](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v20/zht_metadata_pipeline.json)：
10 個連結已註冊、page13 已排入、從損壞 feed 接受的正文為 **0**；
各 item 及原 response SHA 保留，頁面狀態為 `rss_invalid_xml_body_requires_html`。
原 discovery ABI 失敗只重排一次，與文章抽取契約分開，沒有重抓所有既有正文或清除拒絕期限。

[第一篇原頁補抓及獨立保存核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v20/zht_original_body_proof.json)
真正保存 **2,531 字元**；署名、原發布字串 `2020-02-12T10:31:27+0800`、
原回應／正文／檔案 SHA、queue head 與當前解析器重播通過。
這是限定原網址驗收，不計入恢復後正式 owner 的吞吐量，也不是十篇全部完成。

本輪 25 個相關測試檔 **1,074 項通過，30.01 秒**；不是全專案測試。
仍有來源拒絕、付費／署名／撤文與未接入來源，全部保留原分類，不把 retry 或 metadata 當完成。

### 恢復後的正式驗收

[01:58:26 至 02:01:35 Taipei 的相同 owner 窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v20/runtime_after_progress.json)
通過九項工程檢查，包括實際新增正文、政策與執行碼一致、近期耐久收據，以及未越過來源流量規則。
PID `2350367` 沒有 restart；服務、timer 及每 15 秒刷新的一致唯讀面板均恢復。

| 來源 | DB 188.986 秒新增正文 URL head | DB head 每秒 | 獨立 HTTP 181.373 秒窗口 |
| --- | ---: | ---: | --- |
| 鉅亨 | 24,150 | 127.787 | 1,743 次開始請求，9.610 requests/s；收到 1,742 個 HTTP200 |
| Global Voices | 167 | 0.884 | 18 次開始請求，0.099 requests/s；收到 18 個 HTTP200 |

DB 與 HTTP counter 是不同時鐘；有請求跨越窗口，不能拿回應數除請求數冒充全歷史成功率。
這一 HTTP counter 窗口鉅亨新增 head 中，22,812 來自 bulk、70 來自 HTML、本機回收為 0。
另有 10 個既有正文版本更新，不能算成新網址；此結果不是官方最大速率或所有歷史已完成。
固定來源與分母均會隨探索改變，也不能僅由兩次不同時段的觀測宣稱排程造成全部速度差異。

同一正式窗口已派送九個到期缺正文重試與 102 個 bulk 公平補漏工作，沒有留待 bulk 全完才處理。
新出現的 `body_not_found` 與後續網路錯誤仍保留；沒有把它們改成成功。
[02:05:18 的完整鉅亨錯誤分組及 36 個缺正文 URL cohort](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v20/frontier_errors.json)
留下下一步診斷入口，須再核對實際原回應與既有正文，不能先斷言都是來源故障。
相同證據顯示英文 RSS page141 再遇正文非法字元，已自動轉 metadata／HTML 補漏並繼續到 page146；
不是遇錯便停住整條來源。

[128 批保存驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v20/recent_batches.json)
在 02:00:55 Taipei 固定 WAL 快照核對 **2,871 個正文版本**及約 20.3 MB 私人原回應，
0 檔案／正文／原回應 SHA 或 head 錯誤；不是全正文庫核驗。
[唯讀面板驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v20/dashboard/acceptance.json)
在 1440、768、390、320 寬度通過，實際讀到 running、最新筆數與比例；
未呼叫來源網站、沒有 JS 錯誤。這只證明本機 gateway，不代替外部 DNS／HTTPS 驗收。

## 鉅亨原文補漏與快取修正

2026-10-10 凌晨 v21–v23；仍沿用唯一正式 downloader、HTTP owner、共享限流、原始檔案與收據。
修正並非放寬到「HTTP200 就算下載成功」：正文、來源身分、發布時鐘、保存及全歷史分開驗證。
v23 於 **02:52:26 Taipei** 恢復 service／timer；當下 PID `2472171`、owner `95a9ec9321d94eeda07aa7cba38e1a97`。
這些是時間限定的執行證據，不是永久服務狀態。

### 真實短訊被誤判缺正文

[八個原網址的受控實抓](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v21/body_probes_after_cooldown.json)
均收到 HTTP200；三篇有真正的短原文，卻被原本通用的 100 字元門檻拒絕：

| 原新聞 ID | 原文長度 | 原來源發布字串 |
| --- | ---: | --- |
| 1056465 | 35 字元 | `2014-05-22T12:32:11.000Z` |
| 298494 | 61 字元 | `2015-01-30T15:13:12.000Z` |
| 6356615 | 78 字元 | `2026-02-28T07:50:40.000Z` |

僅對鉅亨、已核對的原網址、公開文章、來源明示身分／標題／發布秒數一致，
且 JSON-LD、可見正文與同篇公開頁面資料一致的短文啟用例外；不取鄰篇、摘要或隱藏付費欄位。
一般短文開關仍關閉，空值／`null`／身分或正文不一致仍拒絕。
同篇資料由 JSON 解碼，沒有執行來源 JavaScript。

[三篇真正保存與獨立核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v21/short_original_body_proof.json)
核對原回應、正文及檔案 SHA、queue head 與當前解析重播。
來源 JSON-LD 署名照原宣告保存；前兩篇正文內的翻譯署名與 JSON-LD 名稱不同，
原始證據均保留，不能將兩種宣告偷偷改成同一個作者。

其他五篇 `6356435`、`6172709`、`6205694`、`6421902`、`6314407` 的原頁正文為空。
[原頁與官方日期分類 API 對照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v21/empty_source_comparison_v2.json)
查到相同 news ID、發布秒數及公開標記，API 的正文也全部為零字元。
其中 `6356435` 連來源標題也空；診斷器僅將這個可驗證的空原始紀錄獨立列出，
沒有改正式下載器的正文門檻或把空紀錄當文章完成。
此證據不表示永久不存在，不能借用摘要／相似文章或造正文補齊。

### 不因 HTML 修正重驗未變動的 API 快取

HTML 短訊修正原先連帶改變鉅亨整體 fingerprint；
v21 的[正式窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v21/runtime_after_progress.json)
因此出現 460 個 retained API 頁面重驗、零新增 head。
這是本機快取重解析，不是 460 次新的來源呼叫，也不能把當時全部吞吐量差異都歸因於它。

新增 API／快取專用 fingerprint，**僅移除這次 short-HTML ABI**；
授權、端點、API parser v8、其他設定及 capture 規則都保留。
新的正文仍依原 capture fingerprint 保存，不覆寫來源／作者或正文。
只沿用相同 API 契約已檢查的 checkpoint，保留未檢查工作、原驗證失敗與既有目的結果；
沒有批次清空未來期限、拒絕狀態、租約或原檔案。
v22 正式 owner 已沿用 **1,265** 個 examined checkpoint，避免重做這部分解析。

成功解析、確定整頁沒有任何有效 API 正文的回應，另記 `no_replayable_body`，
保留原 envelope、錯誤與待抓 HTML，避免立即讀回同一份快取再解析。
這是快取工作排除，不是正文接受、檔案完整性或全歷史完成宣告；
含有效正文的混合頁、付費／權限頁與其他驗證錯誤不使用此捷徑。
API／授權／設定改變後可重新核驗。
此 no-body 捷徑通過 canonical 語意測試；v22 觀測窗口沒有新整頁適用案例，
不能將已沿用 checkpoint 的 1,265 份誤算為新 no-body 捷徑的實測節省。

[唯讀原快取證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v22/cached_source_proof.json)
於 02:47:44 Taipei 顯示已知 50,753 份 response envelope：16,859 份檢查完、33,894 份待檢查。
「檢查完」不是同數量的正文，仍有合法的舊未檢查快取需回收；不跳過以美化速度。

### 修正來源時鐘與頁面統計

首次清冊同步在第一個 `run_cycle` 之前發布狀態，原先沒帶已載入的 HTTP cooldown／capacity，
讓 CMoney 看似立即可抓、造成 v21 runtime 驗收失敗。
現在服務於第一次 metadata callback 前綁定共同觀測器；清冊、idle 及正式下載狀態都顯示相同來源期限，
不由面板額外呼叫 provider，不因顯示刷新清除來源退避。
原失敗證據保留，後續用新的同 owner 窗口驗收，不把原失敗改寫成成功。

另發現舊 `bulk_metadata_only_html_fallback_pages` 僅看 item errors，
會把「15 個有效正文＋45 個來源精確重複列」也算成 metadata-only。
因此 v21 的 662 個該計數不能推論為 662 頁全部沒有正文。
改為共用分類器，只有 **有效正文為零** 且錯誤全部為來源未提供正文／精確重複，才計入純 metadata；
混合頁另外計入 `bulk_valid_body_pages_with_item_errors`。
新增報告契約 `cnyes_bulk_page_classification_version=2`，沒有改 API／正文 capture ABI。

[兩份實際凍結原始回應](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v23/cached_page_classification.json)
均重新核對 SHA／公開標記／分頁及當前 parser：

| 實際 retained 頁 | 有效正文 | item 結果 | 新分類 |
| --- | ---: | --- | --- |
| `mag` 首頁 | 0 | 30 個來源未提供正文 | 純 metadata，原 HTML 補漏仍待抓 |
| `tw_bull` 2015-08-31 第 30 頁 | 15 | 45 個來源精確重複列 | 含有效正文的混合頁，不是缺文頁 |

該核驗是唯讀原回應重播，沒有增加 provider 呼叫或重新接受正文到正式 queue。
v23 owner 隨後已觀測到 25 個混合頁被正確分類，沒有誤算為純 metadata。

### 保存、正式速度與剩餘來源問題

[v22 的 02:43:58–02:47:45 同 owner 驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v22/runtime_after.json)
通過九項工程檢查；這是修正後真實保存，不是只看 service active。

| 來源 | DB 226.739 秒新增正文 URL head | DB head 每秒 | 獨立 HTTP 226.255 秒窗口 |
| --- | ---: | ---: | --- |
| 鉅亨 | 28,856 | 127.265 | 2,314 次開始請求，10.227 requests/s；2,312 個 HTTP200、4 個 HTTP502 |
| Global Voices | 1 | 0.004 | 23 次開始請求；22 個已收到 HTTP200；既有最小間隔 10 秒 |

跨窗口會有在途請求／延後正文批次提交；DB 與 HTTP 是不同時鐘，不能相除當全歷史成功率。
鉅亨 HTTP counter 窗口另證明 28,459 個 bulk 新 head、120 個 HTML 新 head，
有 136 個 bulk 公平補漏與 2 個到期缺正文重試派送；回收快取新增 head 為零。
本次樣本吞吐量不是官方最大速率，也不能由與 v21 不同頁面／重疊程度的樣本宣稱固定加速倍數。

[128 批保存核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v22/recent_batches.json)
在固定 WAL 快照核對 2,768 個正文版本及 24,824,967 bytes 私人原回應，零 SHA／head 錯誤。
[本機唯讀面板](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v22/dashboard/acceptance.json)
在 1440／768／390／320 寬度通過，仍不等於外部 DNS／HTTPS 驗收。
v23 最終相關回歸 **1,124 項通過，37.73 秒**；不是全專案測試。

[741 個來源鍵的完整當前清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v22/inventory.json)
保留全部來源、59 個設定、17 個啟用、終止／追新／正文缺口與錯誤，沒有縮成 Top K：

- CMoney 於 02:43:29 再回應 403，來源共同冷卻到 02:58:29；其他來源繼續。不能將 403 改成功或繞過拒絕。
- 鉅亨本窗口的 502、後續 owner 的短暫連線錯誤，依原重試期限重新排程；已保存正文與原錯誤不丟失。
- Global Voices 保留一篇來源已撤正文；損壞 RSS 仍走原頁補抓並繼續分頁，不接受改寫 RSS 正文。
- Fed 仍有 4 個影像 PDF、3 個文件入口；CMNews／民視等仍有署名、登入、第三方權利或不支援路徑。
- PeoPo 尚有來源拒絕及缺少明示文章授權；未啟用來源及授權 API 接線另列，不當作已抓齊。

服務存活、工程修正、選定正文驗收與全部新聞歷史是四個不同層級。
本輪沒有宣稱所有 provider 都已成功、所有錯誤已根治或全部歷史完成；原目標維持 active，
剩餘工作須繼續按來源回應／授權與保留的完整清冊逐一核對。

### v23 最終新 owner 驗收

[02:52:56–02:57:37 Taipei 的同 owner 窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v23/runtime_after.json)
再次通過九項工程檢查，包含新正文進度、來源期限、未越過流量規則、當前政策／執行碼、近期保存證據；
PID `2472171` 沒有 restart。這一窗口將基線時已存在的 CMoney cooldown 明確排除於健康來源進度驗收，
但沒有將其 271,598 個缺正文待辦當成完成，也不代表該來源能正常取得。

| 指標 | v23 最終結果 |
| --- | --- |
| DB 窗口 | 281.680 秒 |
| 鉅亨新增正文 URL head | 35,276，125.234 heads/s |
| Global Voices 新增正文 URL head | 1 |
| 獨立 HTTP counter 窗口 | 285.980 秒；鉅亨開始 2,793 次請求，9.766 requests/s |
| 鉅亨回應 | 2,725 個 HTTP200、65 個 HTTP304，另 3 個短暫連線錯誤進重試 |
| 正確識別含正文混合頁 | 74 個，不再算純 metadata |
| 到期缺正文重試派送 | 3 個；不提前跨越重試／租約／來源期限 |

HTTP counter 與 DB head 窗口仍分開：counter 中 35,721 個 bulk 新 head、123 個 HTML 新 head，
不硬湊成 DB 的 35,276，亦不將在途請求算成功率；快取回收新增 head 為零。
這一窗口沒有適用新 no-body checkpoint 的整頁案例，該捷徑仍只有語意測試驗證，不能宣稱其已提高正式吞吐量。

[v23 最新 128 批保存核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v23/recent_batches.json)
於 02:56:35 固定 WAL 快照核對 **2,788 個正文版本**，零原回應／正文／檔案 SHA 或 head 錯誤。
[v23 唯讀面板驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v23/dashboard/acceptance.json)
再次通過 1440、768、390、320 寬度；仍只驗證本機 gateway，不代表外部入口及全歷史完成。
本文 60 個本機文件／證據連結均存在；v23 最終 1,124 項相關測試通過，不取代全專案測試。

03:00 的[來源追蹤快照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v23/provider_followup.json)
又發現 PeoPo 於 02:58:06、CMoney 於 02:58:30 回應 403；
各自來源共同期限為 03:13:06、03:13:30 Taipei，原錯誤仍可見，鉅亨持續抓取。
這兩個拒絕沒有因工程驗收通過就消失；須在來源允許的 API／授權範圍內繼續查因，不能繞過拒絕。

## 既有原文連結回補與短暫錯誤後的容量恢復

2026-10-10 凌晨 v24–v25。仍是同一正式 queue／HTTP owner，沒有另開 Google 爬蟲、第二套正文庫或訓練注入。
修正重點是**少做無效等待、延續已驗證的容量、只把真正保存的正文算完成**，不是把失敗或轉址改成成功。
v25 service／timer 於 **04:11:20 Taipei** 恢復，PID `2637639`、owner `c1e145dcd4974189bd1db9e80274c66c`。
這些識別碼及下列筆數、速率都是時間限定快照。

### 所有本機 Google News 連結的清點與原站分工

[完整唯讀清冊 v2](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v24/google_original_inventory_v2.json)
與[重跑 v3](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v24/google_original_inventory_v3.json)
都清點 **1,285,019** 個已保留的 Google URL，涵蓋所有日期／狀態，不是最新頁或 Top K。
兩次 input cohort SHA 完全一致；完整結果為：

| 本機 metadata 結果 | 筆數／狀況 |
| --- | --- |
| 欄位內直接明示原站 URL | 946,017 個轉址連結 |
| 較新、不透明的 `AU_yqL` token | 339,000；保留未解，不算正文 |
| 其他未解 | 2；不安全／另一個轉址來源 |
| 唯一原站 URL | 835,692；499 個目的來源鍵，不是 499 個已驗收 publisher |
| v2 清冊時已啟用來源的未入列原站 URL | 48,722；其中鉅亨 41,365 |

解析只讀既有 `/rss/articles/…`、`/articles/…`、`/__i/rss/rd/articles/…` 的本機內容，
沒有向這些路徑發請求，也沒有使用私有 RPC、簽章、cookie、標題猜網址或 AMP 備援抓取。
這是已觀測的舊 envelope 版本契約，不是 Google 官方解碼 API。
原站必須經既有 source policy、robots、來源拒絕及正文驗證；未啟用／未審查的目標只保留 proof，不直接放行。

原 redirect metadata、股票關聯 `source_links` 保留原位；target 以 canonical URL 去重。
原站已完成正文、重試期限、租約及有限 cohort 閘門不重設，也不把 redirect 的股票關聯複製到原站。
實際 retained 樣本有相同原站 URL 卻不同 metadata 標題，故 **URL 指標不等於標題、股票身分或發布時鐘驗證**；
`metadata_identity_verified=false`，本次不新增訓練 feature。
清冊日期只是 retained metadata 的排序日期，不能當成歷史發布日期／PIT 證據。

### 不依賴網路工作的續跑與游標恢復

新增解析使用 host／state／rowid 的索引 keyset，所有 metadata 狀態輪流參與；
每片預設最多 256 筆、40ms soft slice，每 250ms 留出排程空間，沒有反覆全表掃描或單筆 publisher 呼叫。
soft slice 在筆與筆之間檢查，交易提交／fsync 不是可硬中斷的 40ms 保證。
沒有 HTTP future 時，只要仍有本機連結待辦就繼續有限切片，不提早退出再等待最長 60 秒的 service idle。
同時先提交待落盤正文，15 秒 heartbeat 與可中斷等待保持不變；磁碟不足、stop、診斷時間界限仍生效。

proof、target 入列、游標在同一交易提交；id／resolver-version 唯一鍵避免重跑重複計數。
malformed JSON／游標型別會先把**原值、SHA、原因、恢復時間**原子保存到私人
`original_url_checkpoint_recoveries`，再從 metadata seek 起點重跑，重用既有 proof。
不能藉此清除 HTTP 限制、正文、租約或 source failure；儲存失敗仍回報，archive／替換會一起 rollback。
這項損壞注入恢復有測試，正式資料本輪沒有刻意製造損壞，不宣稱發生過實際腐敗修復。

三份[原始驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v24/original_urls_native_before_restart.json)、
[重啟後驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v24/original_urls_native_after_restart.json)、
[v25 續跑驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v25/original_urls_continuity.json)
的已檢查數依序為 **364,988 → 436,147 → 516,033**；各自核對完整 proof table 的增量 counter，
並抽驗 129 筆 native URL／目標 proof，均通過。這不是所有正文或 metadata 身分驗收，解析剩餘工作仍在進行。

### 進度／估時不混入正文，也不偽稱 CPU 加速

既有新聞頁新增可收合的「Google News 轉址清點」，獨立顯示目前本機連結分母、已檢查數、
literal／opaque／未解及指向已入列 target 的**轉址數**；最後一項不是唯一原文篇數。
估時只依新 owner 實際檢查數／完整 elapsed，且至少 60 秒、256 筆；停止、過期、刷新失敗或快照不一致就撤下估時。
正文仍只算實際 body head／落盤證據，沒有把 opaque、解析成功、HTTP200 當正文成功。
[本機面板驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v24/dashboard/acceptance.json)
通過 1440、768、390、320 寬度與全部來源分頁，使用原生展開／搜尋／排序控制；
另實測 browser status request 中斷後估時撤下、正常定時重試恢復。沒有 publisher／Google 呼叫。
這仍只證明本機 gateway，不等於外部 DNS／HTTPS 驗收。

控制字元掃描改為 compiled regex，保留相同嚴格條件及 resolver version；全 cohort 結果完全一致。
但完整唯讀稽核 v2 **186.181s wall／184.744s CPU**，v3 **198.869s／193.931s**，後者反而較慢。
稽核包括暫存 SQLite 去重、現有 target 查詢及當時負載，不是純解碼微測速；**此改動未證明端到端效能收益**。
保留這個負結果，不以局部 C-level 掃描取代整段實測，也不宣稱達到理論最高速。

### 鉅亨反覆降速的根因與 v25 修正

[v24 正式窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v24/runtime_final.json)
已通過九項工程驗收，但 432.029 秒只新增 9,268 個鉅亨 head（21.452/s）；
獨立 HTTP counter 有四個 ReadTimeout，controller 又回到 `recovering_transport`。
程式當時在恢復期排除了「單一錯誤補足分母」機制，且恢復成功只回到低速 baseline，重做已測過的 ladder。
502／timeout 是傳輸失敗，不是官方 rps 限制的證據；一個錯誤／20 個結果是 5%，分母很小，不能直接當來源容量。

v25 保留相同 error budget、source fingerprint、measurement contract 與正式 HTTP 閘門，修正兩處：

- 恢復期也能在同壓力延長單錯誤樣本。1% 預算至少需要 100 個完成結果才容得下一個錯誤，
  不是統計保證或官方上限；不立即再降速、不先升速，也不跳過最多 600 秒的觀測期限、第二個錯誤或來源拒絕。
- 重複健康恢復窗口後，若同 source／authorization／measurement fingerprint、同工作類型、
  exact cadence／slots 在最近一小時已有足夠健康 online body 窗口，就恢復該容量再複驗。
  mixed／metadata／快取、缺新正文、過期／未來證據、corrupt counter、429／403／robots floor 或在冷卻中的來源不放行。
  沒有足夠 proof 仍走 reviewed baseline；不重用舊吞吐量做新 ETA。

[v25 的 04:12:04–04:16:09 Taipei 原生驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v25/runtime_after.json)
再次通過九項工程檢查；不是只看服務 active。
正式 owner 實際記錄一次 `transient_recovery_prior_capacity_restored`，有 **12 個**近期同 workload／capacity 健康窗口支援，
回到 8 並行、client target 間隔 0.033333s，且後續已達 `stable_pipeline_capacity`、至少四個健康複驗窗口。
target cadence 不是實際 rps，更不是官方最大速率。

| v25 同 owner 指標 | 正式結果 |
| --- | --- |
| DB window | 245.000 秒；新增鉅亨正文 URL head **15,276**，62.351 heads/s |
| 獨立 HTTP counter window | 241.651 秒；鉅亨開始 **1,562** 次，6.464 requests/s |
| HTTP 回應 | 1,526 個 HTTP200、34 個 HTTP304、2 個 HTTP502；錯誤保留重試，非空正文成功 |
| 獨立來源 | Global Voices 同窗口新增 22 個正文 head |
| service／policy／source code | current owner、policy 一致，NRestarts=0；沒有跨過正式限流／冷卻 |

DB、HTTP 及 controller window 有不同邊界及在途工作，不硬湊 head／response 成全歷史成功率。
v24、v25 讀到的歷史頁面與重疊程度不同；上述速率差**不當成同工作量固定加速倍數**。
controller 的原生恢復事件及後續持續落盤是本輪機制驗收；來源最大值與 WAN 韌性仍未證明。

[128 批保存核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v25/recent_batches.json)
檢查 2,069 個正文版本、19,558,527 bytes 私人原回應，零檔案／正文／原回應 SHA 或 head 錯誤。
99 個 capacity／performance focused tests、**1,145 個新聞／限流回歸**通過；不是全專案測試。

### provider 錯誤繼續保留與分流，不宣稱全部完成

[v25 完整清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v25/inventory.json)
仍有全部 741 個來源鍵，59 個設定、17 個啟用；10/10 04:14:56 的 WAL 快照有
5,939,600 個已知網址、3,219,393 個保存正文及 2,130,159 個待權限網址。
所有來源／端點、狀態、錯誤、metadata 日期與下一動作仍在清冊，不只列鉅亨。

CMoney、PeoPo 於本輪再回報 403；各自原 shared deadline **04:20:56 Taipei** 在 v25 重啟後保持一致，
不影響鉅亨／Global Voices。到期依原來源規則續試，不換 cookie、出口或隱藏 API 繞過拒絕。
鉅亨仍有來源缺正文／404／不支援路徑，新增 Google 原站連結也仍有未審查與 metadata 身分缺口；
沒有把它們改成完成或當成來源全錯就丟掉。後續按原回應、既有 API／HTML 與授權範圍核對、修復後續抓。
全來源目標仍為 active；本輪完成的是上述工程與選定資料保存驗收，不是全部新聞歷史或所有 provider 的成功。

### 04:24 續抓／再遇錯誤的原生追蹤

[04:24:48 Taipei 的同 owner 續抓驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v25/runtime_final.json)
仍通過九項工程檢查；自 04:12:04 的固定基線累計 764.255 秒新增鉅亨 **51,569** 個正文 head，
平均 **67.476/s**，Global Voices 另新增 71 個。不是全來源總成功率或官方最高速率。
後續又有傳輸錯誤，當下降為 7 並行／0.066667s，仍保留同 owner、既有 body 與進度，沒有回到低速 baseline。
這份快照狀態為 `recovering_transport`，不能用前一份 stable 快照冒充當下無錯誤。

CMoney、PeoPo 於 04:20:56 到期續試再得到 403，新的獨立 source deadline 到 **04:35:56 Taipei**；
錯誤仍未解除，沒有因其拒絕而停住鉅亨或 Global Voices。
[此時最新 128 批保存核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v25/recent_batches_final.json)
驗證 **1,887** 個正文版本及 16,963,926 bytes 私人原回應，零 SHA／head 錯誤。
[連結回補追蹤](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v25/original_urls_followup.json)
已檢查 **712,170 / 1,285,019**，剩餘 572,849；完整 proof counter 與 129 筆抽樣再次通過，沒有算作全部正文完成。

[04:31:09 Taipei 的再次恢復驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v25/recovery_followup_v2.json)
已保存**第二次**自然傳輸錯誤後的 prior-capacity restore；同 owner 回到 8 並行、六個健康複驗窗口，九項工程檢查再次通過。
自 04:12:04 基線的 1,145.189 秒累計新增鉅亨 **80,471** 個正文 head，平均 **70.269/s**；不是無錯誤或全歷史完成。
較早一份[追蹤拒收結果](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v25/recovery_followup.json)
僅因沿用超過 300 秒的保存核驗被拒收，沒有放寬近期證據閘門；重做 fresh body receipt 後才接受 v2，舊結果保留。

## 2026-10-10 鉅亨串流 HTML 短訊漏抓與來源錯誤時鐘修復

鉅亨沒有停止下載，但短訊存在**間歇性解析失敗**：來源以 React 串流輸出 HTML，側欄載入標題 `...` 有時先到，正文則在 `S:0` 區塊稍後到。原短訊驗證取全頁第一個 `h1`，因此同一篇、相同正文與發布時鐘，會因 DOM 排列而被錯算 `body_not_found`。這不是 429，也不是缺少發布日期，降低整個 provider 的速率不能修復它。

[原始問題頁面診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v26/body_probes.json)
與[正式 fetch 入口的回應診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v26/canonical_fetch_probes.json)
保存了同一篇的兩種原回應：一種標題先到，另一種側欄 `...` 先到；92 字正文 SHA 均為 `262b206dbddfa8a0f5c2b91dd0bd33b6ee4adef1f94c99049fb4d95406ac7410`。沒有用頁面整體文字、旁欄、摘要或隱藏資料替代正文。

### 共用修正

- `news_fulltext_publishers.py` 改由唯一 `#article-container` 所屬的 `article` 核對唯一文章標題；不依串流到達順序，也不靠容易變動的 CSS 雜湊。缺少文章範圍、重複正文容器或同範圍多個標題仍拒收。原有 canonical URL、newsId、payment=0、發布時鐘、可見段落與完整宣告相符的驗證保留。
- 鉅亨短訊 HTML 契約升為 **v2**，更新 source fingerprint，沿用既有 policy reconciliation 重新排程舊解析失敗；源站拒絕、付費內容與 404 不因此重置。API adapter 契約、每頁上限和保存的原值不變。
- 空正文也必須由同一文章範圍、宣告 ID／URL／發布時鐘、空可見容器及空的免費內容宣告交叉證明，才標成既有的 `source_article_empty_template`。影片／圖片、矛盾宣告、無法確認的短摘要仍不是可接受正文；不任意把全部 `body_not_found` 換標籤。
- `NewsQueue.failed` 現在為**所有 provider** 寫入實際 `last_checked_at_utc`。失敗觀測契約為 **v2**；不改 `captured_at_utc`、歷史發布日期、已存正文或收據，也不為舊空時鐘捏造日期。
- 診斷可用 `inspect_cnyes_news --canonical-fetch`，沿用現有 queue row 與正式 `fetch_article`，只保存收到的回應、不修改 queue。仍須停用正式 HTTP owner 後才能執行，最多 8 個選定問題頁，保留 robots、限速和冷卻。

### 原生驗收與續抓

[8 個問題頁的完成後核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v26/problem_page_acceptance_v5.json)
通過：正式共用下載器保存 **1 篇 92 字短訊**；另外 **6 篇來源空正文、1 篇 404** 保留實際源站結果與新的檢查時鐘。短訊的 Parquet／原回應／正文 SHA／SQLite head 及目前 parser replay 一起通過，不把另外 7 篇當作完成。
較早的[串流差異拒收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v26/problem_page_acceptance.json)
及[下載尚未全部 commit 的快照拒收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v26/problem_page_acceptance_v2.json)
仍保留；沒有放寬 gate，只有在 canonical fetch 完成後的新快照才接受。

測試：新聞／共享限速回歸 **1,180 passed**，另補跑鉅亨 bulk adapter **69 passed**、問題頁證據一致性 **4 passed**，合計 **1,253** 個不重複的相關測試；文章範圍／實際失敗時鐘／穩健排程 focused suite **249 passed** 是其中的子集。包含側欄先到、標題後到、空模板、付費與非整數宣告、重複或矛盾資料、未知發布時鐘、圖片／影片及不屬於正文的標題。只讀驗收也拒絕缺少 native row、原回應 SHA 不符，以及兩次快照之間正文 head 改變的情況；不是整個 repository 的全測試完成聲明。

[05:03:24 Taipei 的續抓驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v26/runtime_after.json)
九項工程檢查通過。自同 owner 的 04:59:40 基線起，約 224.5 秒新增鉅亨 **9,696** 個正文 head，平均 **43.19/s**；Global Voices 同時新增 **20** 個正文 head，沒有被鉅亨或 CMoney 的冷卻阻塞。此時鉅亨仍在新 HTML 契約的量測升速階段，6 並行、0.1 秒客戶端目標間隔，**不是新的穩定最高速證明**，也不能與前一 owner 的 70.27/s 當作固定工作量的效能 A/B。

[05:13:53 Taipei 的穩定續抓驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v26/runtime_stable.json)
仍為同一 owner，九項工程檢查通過；自 04:59:40 基線起 **853.57 秒**新增鉅亨 **42,023** 個正文 head，平均 **49.23/s**，Global Voices 同時新增 **78** 個正文 head。鉅亨達到本輪 `stable_pipeline_capacity`，**8 路並行、0.05 秒客戶端目標間隔、7 個穩定樣本**。區間內實際鉅亨 HTTP 速率為 **5.64 次/s**，不能把 20 次/s 的客戶端目標或每秒正文筆數當成已證明的官方請求上限；新增正文也包含既有原回應的快取重解析。

[05:12:06 Taipei 的最新 128 批保存驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v26/recent_batches_stable.json)
通過 **1,667** 個正文版本與 **15,818,789 bytes** 原回應，零 SHA／head 錯誤。採最新已 commit 批次的只讀快照驗證，不包含之後的寫入或全歷史。
[原生網頁驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v26/dashboard/acceptance.json)
通過 1440／768／390／320 寬度、正常讀取及原生失敗／重試行為；原有空模板狀態已可由共用狀態投影顯示。這是 localhost 的驗證，不是公開 DNS／WAN 證明。

[05:02:15 Taipei 的全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v26/inventory_after.json)
仍有 **59 個設定 provider、17 個啟用、682 個未註冊來源鍵**，合計 **6,100,961** 已知 URL、**3,365,807** 已有正文 head；不是 682 個已審核或可任意抓取的 publisher。Google 本地原文連結 proof 已檢查 **1,107,437 / 1,285,019**，剩 **177,582**；此進度不等於原文正文完成。
CMoney 仍有來源 403 及持久冷卻，授權／署名、圖片 PDF、真正空正文與無原文 URL 的缺口維持可見。服務與 timer 恢復後繼續由既有 owner 自動抓取；本輪沒有宣稱所有 provider 或全歷史已完成。

## 2026-10-10 混合權限批次分頁續抓與獨立來源驗收

Global Voices 的 RSS 歷史曾被兩個單篇權限例外卡住：繁中第 46 頁、英文第 539 頁都含一篇不符合全文保存範圍的文章。原下載器在排下一頁前拋出 `bulk_response_contains_restricted_text`，使後續頁面根本沒有進入排程，其餘文章只剩逐篇 HTML 回補。這是分頁控制流程錯誤，不是該 provider 整個歷史無法取得，也不能靠提高請求頻率解決。

[四頁實際來源診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v27/feed_boundary_probes.json)
確認兩個混合頁分別仍有 9／14 個可用正文候選，下一頁則有 10／15 個候選且沒有權限例外。診斷只保存 URL、SHA、作者及原始發布字串，不保存混合回應或正文，也不改動佇列。

### 共用下載器修正

- RSS、WordPress、鉅亨現在共用混合批次續抓契約 **v2**。先驗證來源、頁數、筆數、身分與防重複分頁；若個別項目受限，該整包回應只處理允許的網址中繼資料，受限項目的錯誤保留，其餘項目走原文 HTML，後頁正常入排程。**混合原回應和任何其中正文都不寫入正文庫或 quarantine**。
- WordPress 的 `paywall_or_login` 納入同一混合原回應防護，修復僅有單篇 protected、其餘免費時可能保存整包的缺口。這不是放寬登入或付費內容限制。
- 更新 discovery fingerprint，不更動既有正文 source fingerprint。一次性的 policy reconciliation 只重排舊解析器造成的 `bulk_response_contains_restricted_text` 頁面，保留原錯誤、deadline 與 marker 證據；不重置 403、429、來源冷卻、未宣告端點、文章 lease、有限批次或已存正文。無效歷史頁碼不使整個 owner 啟動失敗。
- 所有 provider 的 `fail_discovery` 現在寫入實際 `last_checked_at_utc`，失敗觀測契約 **v1**。既有 response SHA、ETag、發布／保存時鐘保留；不回填捏造的舊檢查日期。
- 原生回補使用 `recover_news_feed_boundaries`，沿用唯一 `NewsQueue`、`NewsHTTP` 和正式 accept／fail 路徑。最多四個已診斷、已註冊且已到期的頁面，仍遵守 robots、來源 pacing 及持久冷卻；執行前停用正式 service／timer，完成後恢復，沒有另開平行 HTTP owner。

### 實際回補與正文證據

[原生四頁回補](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v27/feed_boundary_capture.json)
通過；繁中第 47 頁新增 **9 篇**，另外一篇繁中及英文第 540 頁的 15 篇已在本機保存，沒有因 RSS 空白格式或作者欄位不同而覆寫既有 HTML。兩個混合頁維持錯誤可見、接受正文零筆，但成功建立後頁。

[只讀分頁與保存驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v27/feed_continuation_acceptance.json)
確認 **25 個正文 head**：9 個新增、16 個既有；Parquet、原回應、正文 SHA、收據與 head 一致。新增 RSS 正文還核對實際來源頁 response SHA、原始發布字串及作者；兩個混合回應沒有 quarantine，受限項目錯誤保留，接續第 48／541 頁存在。每次檢查有限範圍、使用只讀快照；兩次快照之間 head 改變會拒收，RSS 沒有官方歷史總量也不捏造分母。

新聞、鉅亨 bulk、共享限速與新驗收回歸 **1,278 passed**；含混合頁／全受限頁、protected WordPress、304、重複分頁、一次性恢復、不合法頁碼、實際錯誤時鐘、跨快照 head 改變、混合原回應落盤拒收與精確 provider ID。這是相關子系統測試，不是全 repository 測試或全來源完成。

### 正式程序的獨立續抓與速度

正式 service／timer 於 **05:36:04 Taipei** 恢復。[05:42:36 的穩定窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v27/runtime_stable.json)
與 05:40:19 的同 owner 基線相比，**136.56 秒**新增：

| 來源 | 新增正文 head | 正文 head／秒 | 實際 HTTP／秒 | 客戶端設定 |
| --- | ---: | ---: | ---: | --- |
| Anue 鉅亨 | 9,236 | 67.63 | 8.69 | 8 並行，0.05 秒目標間隔 |
| Global Voices | 127 | 0.93 | 0.095 | 1 並行，10 秒間隔 |

九項工程檢查全通過。RSS 的改善來自恢復分頁及每次取得多篇正文，**沒有提高 Global Voices 的頻率**；鉅亨當時為 `stable_pipeline_capacity`、10 個穩定樣本。數字是該窗口的新增 head／實際請求，不是官方最高速率、整段歷史的 A/B 或全市場完成時間。

[05:42:14 的最新 128 批正文驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v27/recent_batches_v2.json)
通過 **1,756 個正文版本、16,461,496 bytes** 原回應，零 SHA／head 錯誤。CMoney、PeoPo 的自然到期續試仍回覆 403，獨立來源期限到 **05:51:35／05:51:34 Taipei**；沒有讓其拒絕阻塞兩個健康來源，也沒有用 cookie／代理或強行重排來源 deadline。

較早的[開機暖身快照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v27/runtime_after.json)
正確拒收整體續抓驗收：初始基線還將尚未續試的 CMoney、PeoPo 列為需有正文進度的來源，隨後卻收到 403。保留該拒收結果；待實際拒絕／冷卻已形成的新基線後才驗收健康來源，沒有改掉原閘門、宣稱被拒來源完成，或拿不同 owner 比速度。

### 網頁與完整清冊邊界

[四種寬度的原生網頁核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v27/dashboard_v3/acceptance.json)
通過 1440／768／390／320，鉅亨、Global Voices、CMoney 的顯示數字與同一 render generation 一致，所有來源頁可正常翻頁，唯讀失敗／定時重試及過期 ETA 失效正確。驗收改用精確 provider ID，因「CMoney」模糊搜尋也會找到「CMoney 新聞」；沒有更改 UI 的正常模糊搜尋或只接受第一張卡。這是 localhost 證據，不是 WAN 驗收。

[05:41:31 的全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v27/inventory_after.json)
共有 **59 個設定 provider、17 個啟用、682 個未註冊來源鍵**，**6,193,980 個已知 URL、3,464,094 個已有正文 head**；仍有 **2,130,180** 個等待授權的 URL。
[全部來源鍵的後續工作表](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v27/source_plan.json)
已逐鍵分類，但 `all_sites_live_reviewed=false`、`plan_is_acquisition_acceptance=false`。
鉅亨仍有需逐篇確認的 `body_not_found`、舊 502／傳輸重試、86 個 404 與 18 個已驗證空模板；抽查的公告 API 有 `&lt;p&gt;null&lt;/p&gt;` 來源佔位內容，不能當正文，也不足以在未核對 HTML／官方原文前將整批未找到正文改稱來源確定缺失。圖片 PDF、有限批次以外新增、署名／授權與未解析原文連結也維持可見。本輪修復分頁、保護原回應與恢復抓取，不宣稱全來源／全歷史抓齊。

### 較晚窗口的傳輸降速仍未完全解除

[05:48:51 的同 owner 續抓驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v27/runtime_final.json)
九項工程檢查仍通過；自 05:40:19 基線累計 **511.81 秒**，鉅亨新增 **13,571** 個正文 head、平均 **26.52/s**，Global Voices 新增 **464** 個、平均 **0.91/s**。鉅亨本 owner 累計收到 **8 次 HTTP 502、14 次傳輸錯誤**，當下為 `recovering_transport`、5 並行、0.4 秒目標間隔；不是先前 stable 的 8 並行／0.05 秒，也不能用先前窗口冒充現在速度。這是不同工作量及自然錯誤下的累計觀測，不是固定批次 A/B。

正式 PID／owner 未更換、沒有服務重啟或正文丟失，Global Voices 的 10 秒 pacing 及獨立增量保持。[05:48:25 的新 128 批保存核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v27/recent_batches_final.json)
通過 **806** 個正文版本、**95,685,340 bytes** 原回應，沒有 SHA／head 錯誤。傳輸降速仍需繼續確認錯誤來源、工作類型及穩定容量恢復，不宣稱 502 根因已消除或現在維持峰值速度。

## 2026-10-10 傳輸恢復計數和端點錯誤歸屬修正

鉅亨的慢速不是單一問題：已到期的批次工作、單篇 HTML 回補和來源傳輸錯誤有不同成本；排程器也有一個使恢復計數失效的錯誤。本輪修正共用控制器與量測路徑，並由正式程序驗證跨工作類型的自動恢復。來源仍有零星逾時／502，沒有把恢復機制驗收說成來源錯誤消失或全歷史完成。

### 已重現的根因

[06:03 的只讀診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v28/capacity_before.json)
顯示當時鉅亨沒有已到期的宣告批次 discovery 工作，不能拿 HTML 回補與先前每次取得多篇正文的 API 窗口直接比較。當時有 7 個連續達標傳輸窗口；其中最後 3 個同為 **4 並行／0.5 秒目標間隔**，工作類型依序為混合、HTML、混合，卻仍只有 `stable_samples=1`。控制器把「同類正文吞吐量的可比較性」與「同壓力下的傳輸恢復」共用一個計數，工作類型切換就將兩者一起歸零。

[固定重現測試](../test/test_news_transport_feedback.py)
在舊實作確實失敗：兩個足夠的健康傳輸窗口由 HTML 切到混合後，沒有恢復到基準。另發現 HTTP 200 標頭先到、正文之後讀取超過期限時，`response_deadline` 原本沒有納入傳輸錯誤率；請求開始與較晚完成的錯誤也可能分屬不同量測窗口。

### 共用修正與效能界線

- 容量量測契約更新為 **v5**。新增獨立的 `transport_stable_samples`；相同壓力下達標的傳輸窗口可以跨 HTML／RSS／API／混合工作累積。真正的失敗窗口、來源拒絕、壓力／官方或已學習下限改變，以及新 owner 都會中斷此計數。
- 跨工作恢復不授予跨工作容量：只有目前同類工作、近期反覆驗證、相同容量與當前 fingerprint 的正文證據，才可恢復舊測得壓力。缺少該證據時只回到既有設定基準，再沿原本有界升速路徑測量；混合／中繼資料／快取重解析不能冒充較高速的正文證據。
- HTTP 終止觀測契約 **v1**：每個已送出的請求恰好記錄一次完成／失敗，計入真正端點、狀態碼、錯誤型別、牆鐘／CPU 時間與 in-flight 減量。容量的錯誤分母與工作類型改用完成結果；429／403／Retry-After 仍在標頭到達時立即生效，不等待正文。
- 未完成的正文期限錯誤納入錯誤率；過大回應及內部未完成回應不能成為健康容量證據。端點名稱只來自既有有界分類，不含網址、文章 ID、查詢 token 或正文。
- 請求開始、標頭與終止這三段的計數／時間更新，各合併為一次鎖操作，取代原本每段兩次；沒有增加 publisher 呼叫、重解析正常正文或另開 HTTP owner。這是 hot-path 鎖次數的結構性減量，不是不同工作量下的 CPU A/B 提速宣告。
- 新版不修改正文 source fingerprint、queue／收據或原始發布時鐘，也不清除持久來源冷卻、已學習的 429 pacing 或既有 robots／官方下限。舊量測契約的容量估計不冒充新版證據。

相關新聞、鉅亨及共用限速回歸 **1,293 passed**；含新 owner 的部分恢復計數失效、真正失敗窗口重置、晚到的 HTML 錯誤不算到新 API 工作、HTTP 200 正文期限、403／429／502 標頭先處理、讀取前／後逾時、恰好一次終止計數，以及只讀診斷不呼叫來源、不改佇列。

### 正式程序遇錯與自動恢復

service／timer 於 **06:10:36 Taipei** 恢復。正式 owner 與 PID 在下面的窗口維持不變，沒有為了取得恢復證據重啟或手動重排文章。

[06:17 的自然錯誤定位](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v28/capacity_natural_error.json)
確認本 owner 的 **4 次 ReadTimeout、2 次 HTTP 502** 都屬於 `article_html`，不是 Cnyes 批次 API；當時以 3 並行／0.5 秒恢復，第一個達標窗口為混合工作。來源回覆 502 與未收到標頭的逾時，不足以證明是本機網路、CDN、來源伺服器或未公告頻率限制中的哪一項。

[06:19:41 的恢復核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v28/capacity_recovered.json)
確認後三個窗口維持 **3 並行／0.5 秒**，依序為混合、HTML、HTML，完成請求分母為 **89、84、101**；錯誤為 **0、0、1**，均符合當前 **1%** 的窗口預算。切換到 HTML 後計數沒有歸零；第三個窗口結束，程序自行轉為 `warming`，理由為「恢復既有設定基準，正文容量仍需重測」。此時累計 5 次 ReadTimeout，沒有把允許的小比例錯誤說成零錯誤，也沒有採用不同工作類型的舊峰值。

### 保存進度與目前仍有的問題

[06:21:33 的同 owner 正式驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v28/runtime_final.json)
九項工程檢查通過；自 06:12:24 的實際拒絕／冷卻已形成基線起 **549.32 秒**：

| 來源 | 新增正文 head | 正文 head／秒 | 實際 HTTP／秒 |
| --- | ---: | ---: | ---: |
| Anue 鉅亨 | 880 | 1.60 | 1.96 |
| Global Voices | 520 | 0.95 | 0.10 |

此窗口主要是 HTML 回補與自然錯誤，不是固定工作量 A/B，也不是之前批次 API 的數十正文／秒。鉅亨到 06:21 又累積到 **7 次 ReadTimeout、2 次 HTTP 502**，再次進入 `recovering_transport`、3 並行／0.4 秒；自動恢復已驗證，不代表之後不會遇到新失敗。Global Voices 維持 10 秒 pacing，沒有被鉅亨錯誤或其他來源拒絕阻塞。

端點計時進一步顯示：鉅亨 API 共 **130** 次終止觀測、總牆鐘 **15.09 秒**，HTML 共 **1,040** 次、總牆鐘 **1,275.92 秒**；這些是重疊呼叫的加總，不是程序經過時間。HTML 抽取共 **1,029** 次、CPU **24.68 秒**；仍需針對 HTML 的成功／失敗延遲與來源或網路成因繼續診斷，不能以盲目縮短讀取期限、重置 deadline 或增加重複呼叫換取表面速度。

[06:21:21 的新 128 批保存驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v28/recent_batches_final.json)
通過 **458** 個正文版本與 **26,248,502 bytes** 原回應，零 SHA／head 錯誤。
[四種寬度的原生網頁驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v28/dashboard/acceptance.json)
通過 1440／768／390／320 的唯讀頁面、數字一致、失敗／重試與 ETA 失效；不是公開 WAN 驗收。

CMoney、PeoPo 的正式續試仍回覆 403，持久期限到 **06:26:34／06:26:35 Taipei**，未清除或繞過。啟動初期的快照仍可能引用舊 owner；待新 owner 時鐘與來源拒絕實際形成後才取得正式比較基線，沒有放寬驗收條件。
[全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v28/inventory_after.json)
仍有 **59 個設定 provider、17 個啟用、682 個未註冊來源鍵**；
[逐來源後續工作表](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v28/source_plan.json)
不等於逐來源下載驗收。署名／授權、圖片 PDF、真正空模板、未確定原文與鉅亨的剩餘 `body_not_found` 仍維持可見；沒有將來源 null 佔位內容當正文。全部新聞目標仍為 active。

## 2026 年 10 月 10 日 快取回補續跑與未完成進度修正

本次修復快取在暫時阻塞後不再接續的程式錯誤，正式服務與 timer 已於 **06:44:25 Taipei** 恢復。鉅亨目前仍有 HTML 的 502、ReadTimeout 與正文抽取失敗；本輪沒有證明來源錯誤已消失或整體吞吐量提升。

### 回補停止的原因與修正

原本已保存的 Cnyes API 回應即使含有可用正文，只要文章還在 `inflight` 或未到期的 `retry`／`retry_cooldown`，回補就跳過該文章，卻把整個回應頁記成 `checked`。文章變成可處理後，該快取頁不會再回來，必須等待其他路徑重新取得。三種狀態的固定測試均在原實作重現失敗。

[共用回補器](../downloader/news_fulltext_replay.py) 的 checkpoint 契約更新為 **v2**：

- 有暫時阻塞文章的頁面改記 `deferred`，保存文章 ID 與原來源 fingerprint 的等待關係。只有文章狀態變更、正文已由其他路徑取得或原重試到期後，才喚醒頁面；等待期間不反覆讀取、SHA 或解析原回應。
- 部分成功頁保留已提交正文，後續接續只補缺少的 head，頁面的 `accepted_heads` 累加而非覆寫。既有正文、lease、付費／來源拒絕終態和原 retry deadline 不被重置。
- 舊 schema 的 `checked` 標記為 v1，依原 API fingerprint 限量重新檢查一次；失敗驗證、其他來源及已確認無正文的頁面不整批重開。未改寫來源資料 fingerprint、權限或原始觀測時鐘。
- pending 與舊 checked 各用有序 covering index、各自 LIMIT，再最多合併兩倍批次上限的 compact metadata，不以 OR 掃描／排序全部原回應。沿用每次 64 頁、0.75 秒、8 MiB 的回補工作預算及 10 秒維護節拍；初次 schema／檔名 inventory 的成本另計，這不是所有呼叫絕不超過 0.75 秒的硬期限保證。
- 正文仍由原 canonical SHA／收據、授權與時鐘驗證後提交。儲存或 fsync 失敗不把頁面標成完成；已提交正文與 checkpoint 的兩段提交可重啟接續。

[只讀快取稽核](../scripts/audit_news_cached_replay.py) 也改為把 deferred 與需複查的舊 checked 計入未完成，分開顯示新 pending、暫緩、舊 checkpoint 和已檢查檔案。頁數不是新增正文／下載 bytes，也不是全歷史完整率；抽樣使用同一 pinned WAL 及原始 SHA、權限、時鐘，不呼叫 publisher、不建立 queue owner。

新聞、鉅亨與共用限速回歸 **1,311 passed**。新增測試包含 lease／retry 到期接續、等待中零 raw 重讀、部分成功與重啟、累加 head、終態拒絕保留、舊 schema 一次升級、來源失敗不重開、索引查詢計畫，以及只讀稽核的未完成判定、授權／時鐘與標題片段拒絕。

### 原始資料與正式服務驗收

[06:41 的原 schema 快照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v29/cache_baseline.json)
有 **39,450** 個舊 checked、**18,307** 個 pending、**12** 個 other-source，共 **57,769** 個本機原回應 checkpoint。重新檢查舊頁是在驗證未知缺口，不表示這 39,450 頁都漏了正文。

[兩次抽樣的 16 個原回應重驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v29/cache_rejection_diagnosis.json)
每頁都有 29 個可用正文且其 head 已保存，另有 1 個 `body_not_found`。其中有原文欄位重複標題、或只有股票代碼／價位的短片段；當前抽取器拒絕這些內容，沒有把片段補成文章、也沒有因此宣告整個來源不可取得。兩個原稽核的全項 acceptance 仍為 false，原失敗收據保留。此抽樣未證明實際漏抓的 cache head，也不能拿 fixture 的成功回補當成本機新增量。

[06:50:22 的正式同 owner 驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v29/runtime_final.json)
九項工程檢查通過。自 06:47:12 的來源拒絕已形成基線起 **189.89 秒**：

| 來源 | 新增正文 head | 正文 head 每秒 | 實際 HTTP 每秒 |
| --- | ---: | ---: | ---: |
| Anue 鉅亨 | 32 | 0.17 | 0.57 |
| Global Voices | 180 | 0.95 | 0.10 |

鉅亨該窗口有 **3 次 HTTP 502、2 次 ReadTimeout**；來源工作由啟動追新／304 回應轉到 HTML 回補，不能和批次 API 峰值或前輪窗口做相同工作量的 A/B。回補器另檢查 **642** 個快取頁、**新增 cache head 為 0**：這是去除舊 checkpoint 的不確定性，不是下載提速證據。新版暫緩接續已由固定測試與重啟驗證，當時未形成實際 deferred 頁的原生到期恢復樣本。

CMoney、PeoPo 正式續試各回覆一次 **403**，分別持久冷卻到 **07:00:16 Taipei** 左右；它們在本次比較基線已受來源冷卻，沒有被算進健康來源進度要求，也沒有被算成完成。Global Voices 以原 robots 的 10 秒 pacing 持續增加正文；沒有被鉅亨或其他來源拒絕阻塞。

[06:48:36 的最新 128 批正文驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v29/recent_batches.json)
通過 **556** 個正文版本、**18,593,208 bytes** 原回應，零 SHA／head 錯誤。
[正式唯讀網頁驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v29/dashboard_live/acceptance.json)
通過 1440／768／390／320 寬度、四個來源的數字連動、原生定時刷新與失敗／恢復；這是 localhost，不是公開 DNS／WAN 驗收。

### 剩餘問題與接續工作

[全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v29/inventory_after.json)
於 06:48:37 仍有 **59** 個設定 provider、**17** 個啟用、**682** 個未註冊來源鍵；已知 **6,201,434** 個網址、保存 **3,479,189** 個正文 head、**2,130,180** 個網址待授權。來源鍵、有限 cohort、已存正文、已抽取／未抽取及全網歷史是不同分母，並未全數完成。

下一步依[本 owner 的 16 個 HTML 失敗樣本](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v29/html_failure_sample.json)檢查原頁與 canonical alias，分清可修 parser、來源空模板與真正不可取得；HTTP 502／逾時繼續依端點與實際傳輸觀測追查。缺少原始回應時使用共用獨占診斷路徑，不另開 crawler、清除冷卻或虛報修復。全部新聞目標仍 active，服務／timer 保持續抓。

## 2026 年 10 月 10 日 鉅亨空值與重查排程修復

本輪找到兩個不同問題：鉅亨來源把 `null` 當成正文欄位，以及既有 parser 失敗的重查搶占首次下載。前者修正分類、後者修正共用排程，沒有靠降低正文／授權驗證、清除來源冷卻或只看 HTTP 200 宣告成功。正式新聞服務與 timer 最後於 **07:22:16 Taipei** 恢復；其他金融下載服務未更動。

### 原始回應與資料正確性

[五個失敗頁的原生診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v30/source_probes.json)
全部 HTTP 200，原抽取器全部回 `body_not_found`。原回應以 private Parquet 及 SHA 收據保留，沒有新增 HTTP 管線或將診斷當成已存文章。

| 原頁 | 原始狀況 | 修正後結果 |
| --- | --- | --- |
| 鉅亨 5560450、6542938、5814058 | JSON-LD、可見正文與當篇免費宣告均呈現字串 `null` | `source_article_empty_template`，不是正文，不再由一般佇列反覆重試 |
| Global Voices 2007 年 Brunei 短文 | 完整正文只有 83 字元，原本被 100 字元門檻拒絕 | 原正文、Preetam Rai 署名、2007 年原發布時鐘與文章 CC 授權均保留，正式收據已通過 |
| Global Voices 2016 年 Justice4Morocco | 正文只剩該站的嵌入內容已移除告示 | `source_body_removed`，不將告示當全文或擅自補字 |

[正式下載器重查紀錄](../artifacts/operations/agent-workflow/runs/v30-canonical-recovered-pages-an-20261009T231115-1d399b6d/run.log)
實際重查五篇、保存一個新正文 head。[四個來源結果的 native queue 驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v30/source_outcomes_native.json)
及[短文的原始回應重解析與 immutable 收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v30/recovered_body_native.json)
均通過；這不是整個 provider 的歷史完整驗收。

鉅亨的空值分類須同時符合當篇 canonical、newsId、標題、aware 發布時鐘、字面 `payment=0`、可見容器與原內容宣告，且沒有圖片／影音正文。`None`、大小寫變體、混合正文、相關文章、衝突宣告、錯誤時鐘、付費宣告或未證實空頁不被推定為來源空值。真正的 `null` 不送進短文驗證器重複解析 hydration。

Global Voices 的短文例外只接受單一當篇容器、canonical、標題、header/footer 一致的具名作者連結、aware 原發布時鐘及文章自身 CC 標示。不放寬一般 100 字元限制，不用頁尾全站授權、摘要、URL 日期、隱藏內容或關聯作者補足證據；長文不額外執行短文驗證。

HTML 契約已更新；鉅亨既有 API/cache fingerprint、Global Voices RSS discovery fingerprint 維持不變，避免 HTML 修復重抓健康歷史批次。只讀鉅亨清點工具也修正為零 probe 時不建立 HTTP owner、不改寫冷卻檔；探測先驗證來源，僅保留允許的公開回應，診斷 header 不保留 Cookie／Authorization。

### 原本排程為何沒有產生新正文

[07:13:27 至 07:16:05 的第一個正式驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v30/runtime_final.json)
**accepted=false**，原紀錄保留。鉅亨在 **157.90 秒**內新增正文 head 為 **0**，293 次文章嘗試中有 231 個來源空模板、44 個仍未解的 `body_not_found`、2 次 ReadTimeout。Global Voices 同窗口新增 151 個正文 head。服務 active、來源政策／owner 一致、收據通過，都不能取代鉅亨的正文產量驗收。

根因是 parser 修復將舊錯誤放回 `pending`，而一般排程在 pending／retry 間只比較 metadata 日期。較新的已知空頁會壓過較舊但尚未首次取得的資料，重試通道又只有單向公平性。

[共用 queue](../downloader/news_fulltext_queue.py) 的 article dispatch ABI 改為 **v4**：

- 有真實舊解析失敗時鐘、無 lease 的 pending 重查，依來源 capture／dispatch ABI 一次遷入 retry；保留原 error、last_checked、attempt、due、既有正文。未觀測的首次下載、來源拒絕／付費及停用來源不混入此遷移；unchanged owner 不重掃歷史。
- 一般下載在有首次 missing-body 工作時，不讓較新的已知 parser repair 搶占；原有每八次首次工作後的重試機會保留，舊重查、undated 與 stored-body recheck 各有自己的公平時計。沒有刪除或跳過任何重查。
- 最新網路逾時／502 保留原 latest-first 恢復順序，並遵守個別 due 和來源冷卻；不是將全部 retry 降成低優先。
- 使用小型 `dispatch_nonparser_retry` partial index，避免每次 lookup 過濾掃描幾千個較新 parser error；索引查詢測試無 TEMP B-TREE。正文提交仍通過原 SHA／Parquet／收據及來源時鐘驗證。

[正式遷移證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v30/repair_lane_native.json)
有 **1,979** 個鉅亨既有重查移到 retry，其中 **7** 個既有正文 recheck 未丟失；新索引在此快照只有 **102,400 bytes**。這是工作分工，不是新增下載量或整體加速百分比。

全部新聞、鉅亨與共用限速回歸 **1,370 passed**。新增測試覆蓋來源 null／媒體／付費／時鐘／身分衝突，短文與移除告示，HTML 修復不重排 RSS，以及首次下載、parser repair、網路 retry 的雙向公平、原 due／lease／正文保留及索引 lookup。

### 修正後的正式觀測

[07:23:55 至 07:27:06 的同 owner 驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v30/runtime_fair_lanes_final.json)
九項工程檢查均通過。DB head 窗口為 **190.49 秒**；已發布 HTTP counter 的獨立窗口為 **200.84 秒**，兩個時計沒有混用。

| 來源 | 新增正文 head | head 每秒 | 實際 HTTP 每秒 |
| --- | ---: | ---: | ---: |
| Anue 鉅亨 | 4 | 0.021 | 0.538 |
| Global Voices | 187 | 0.982 | 0.100 |

鉅亨該窗口另外處理 **92** 個已核實來源空模板，**11** 次 due missing-body retry dispatch；仍有 **2** 次 ReadTimeout、**1** 次 502。這是雙向公平與獨立續抓的原生證據，不是來源錯誤全部消失、整體 A/B 提速或已達來源最高速。修正前後 owner、佇列內容及工作組成都不同，不能拿「0 到 4」換算加速倍數；鉅亨可用正文產量仍低，需接續核對剩餘原頁與 transport 恢復條件。

[07:27:04 的最新 128 批收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v30/recent_batches_fair_lanes.json)
通過 **1,017** 個正文版本、**23,241,742 bytes** 原回應，零 SHA／head 錯誤。
[修正後正式唯讀網頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v30/dashboard_live_fair_lanes/acceptance.json)
通過 1440／768／390／320 寬度、四來源數字連動、原生刷新與失敗／恢復，控制路徑維持 405。此為 localhost 驗收，非 WAN／公開 DNS 證明。

[07:30:10 的接續驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v30/runtime_latest.json)
仍為同一 owner、NRestarts=0，九項工程檢查通過。自 07:23:55 起 **375.06 秒**新增鉅亨 **98**、Global Voices **358** 個正文 head；鉅亨同窗口仍有 **2** 次 ReadTimeout、**3** 次 502、**1** 個新 `body_not_found`，沒有將它們藏起來。CMoney 冷卻到期續試仍回 403，獨立再排到 **07:42:20 Taipei**，其他來源持續取得正文。
[此時最新 128 批收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v30/recent_batches_latest.json)
通過 **1,117** 個正文版本、**41,850,699 bytes** 原回應、零驗證錯誤。此窗口鉅亨 head 約 **0.261／秒**、HTTP 約 **0.802／秒**；仍非同資料集的 A/B 或來源吞吐量上限證明。

### 全來源範圍與剩餘工作

[07:13 的全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v30/inventory_after.json)
仍有 **59** 個設定 provider、**17** 個啟用、**682** 個未註冊來源鍵；**6,201,436** 個已知網址、**3,479,938** 個已存正文 head、**2,130,180** 個網址待授權。一次只讀清點耗 21.33 秒、21.07 CPU 秒，不放入每次下載的熱路徑。

CMoney 的真實 403 仍保留來源冷卻，前一個 deadline **07:27:20 Taipei** 到期後續試再次拒絕，最新延至 **07:42:20 Taipei**；PeoPo 的個別未到期 retry 也保留。授權／署名、純圖 PDF、來源真正刪除、未確定原文與鉅亨剩餘 `body_not_found` 仍可見，不算完成。全部新聞目標維持 active；後續依實際正文產量、錯誤、來源拒絕與下一次可重試時鐘接續。

## v31 鉅亨短表格正文與有界失敗診斷

**2026-10-10 07:52:21 Taipei** 正式新聞服務與 timer 恢復，PID 3148344、owner `39bbc6064e9d4c90acb171908211d933`。本輪修復的是可重現的短篇營收表格解析問題，並在共用 queue 加入失敗證據；不是所有來源或歷史資料已完成。其他金融下載服務未更動。

### 真實失敗原因與正式正文驗證

[兩個原生失敗頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v31/failed_page_probes.json)
均為 HTTP 200，原抽取結果均為 `body_not_found`。鉅亨 6626337 的 JSON-LD 正文只有 75 字元，但原頁確有完整營收表格；原驗證只比較段落，遺漏表格。當篇免費宣告的 `content` 又是 `$3f` 文字框架引用，舊解析把它當成內容字串，無法核對已顯示正文。

[React 官方送出端](https://github.com/facebook/react/blob/main/packages/react-server/src/ReactFlightServer.js)
與[接收端](https://github.com/facebook/react/blob/main/packages/react-client/src/ReactFlightClient.js)
以十六進位 UTF-8 位元組長度界定文字框架，不能按換行或中文字元數截斷。共用鉅亨解析器改為有界單趟讀取，最多 8 MiB、4,096 個框架；文字內看似 JSON 的資料不成為另一篇文章宣告，缺失／衝突／截斷引用不能放寬正文驗證。不執行 JavaScript，也不由隱藏內容提供正文。

短篇正文仍須核對 canonical、newsId、標題、aware 原發布時鐘、字面 `payment=0`、可見容器及當篇完整內容宣告。表格逐格比對，防止 `22｜184` 與 `2｜2184` 因串接成相同字串而通過；輸出保持可見原值的分隔，不把兩個數值黏成 `22184`。額外實際正文、隱藏格、巢狀表格、影音、付費或衝突宣告不被當成完整短文。HTML fingerprint 更新為 v4，健康 API/cache fingerprint 不因這個 HTML 修復重抓。

[正式下載與 immutable 正文收據驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v31/recovered_tables_native.json)
已通過兩篇：

| 原頁 | 正式正文長度 | 保留的原始發布時鐘 | 結果 |
| --- | ---: | --- | --- |
| 鉅亨 6626337 | 93 字元 | 2026-10-08T18:28:18.000Z | 原表格、署名、原回應與 head SHA 均驗證 |
| 鉅亨 6558570 | 95 字元 | 2026-08-04T05:42:21.000Z | 原表格、署名、原回應與 head SHA 均驗證 |

### 自動失敗證據與工作邊界

[共用失敗診斷](../downloader/news_fulltext_diagnostics.py)
只處理已取得 HTTP 200、但抽取回 `body_not_found` 的回應，**不新增 HTTP 呼叫**。工作執行緒只交回既有回應，SQLite 配額與檔案寫入由原 queue owner 執行。每個 provider 每個 UTC 日最多兩次診斷嘗試；重啟、parser 升版及診斷儲存失敗均不能重置這個額度。先保存原失敗，再嘗試診斷；診斷磁碟／Arrow／SQLite 錯誤有計數，不把來源失敗改成成功，也不讓它中斷其他來源。

只有能獨立核對當篇免費鉅亨身分、標題與原發布時鐘的 HTML 才保留 private 原回應。其他來源或未能證實範圍的鉅亨頁只保存 hash、長度與去 query／fragment 的回應身分資訊；Cookie、Authorization、Location 不保存。診斷另存 `data_news_fulltext/diagnostics/`，有 SHA 收據，永不計為 accepted article，不送進瀏覽器、發布或遠端訓練。健康文章不新增診斷解析、雜湊或檔案寫入。

[完整新聞回歸](../artifacts/operations/agent-workflow/runs/v31-final-complete-source-and-di-20261009T235152-73961421/run.log)
**1,405 passed，37.78 秒**；涵蓋框架／引用、逐格比對、權限負例、UTC 日額度、重啟／fingerprint 不重置、儲存失敗、worker 不寫 SQLite 及健康路徑不做診斷。前兩輪測試錯誤來自測試 fixture 誤用 queue context manager，另有一個 literal／引用負例錯誤；原失敗收據保留，由完整成功回歸 supersede，不將它們描述成來源故障。

截至[08:01 的原錯誤 provenance](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v31/remaining_error_provenance.json)
尚有 **1,951** 個原 `body_not_found` 留在 retry 通道。本 owner 尚無符合條件的新解析失敗診斷樣本；自動診斷分支的配額／權限／儲存隔離已由上述測試驗證，不能把沒有新樣本說成已完成實際原回應保存，也不能把舊錯誤重新排入當成已修好。

### 正式續抓產量與全來源缺口

[07:53:57 至 07:56:48 的同 owner 驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v31/runtime_final.json)
九項工程檢查全部通過。DB 窗口 **171.45 秒**、HTTP 計數窗口 **169.92 秒**，兩者各用實際觀測時鐘，不混用 heartbeat 與收據完成時間：

| 來源 | 新增正文 head | head 每秒 | HTTP 呼叫數 | 實際 HTTP 每秒 |
| --- | ---: | ---: | ---: | ---: |
| Anue 鉅亨 | 56 | 0.327 | 71 | 0.418 |
| Global Voices | 82 | 0.478 | 17 | 0.100 |

鉅亨該窗口有 **3 次 ReadTimeout、6 個已核實 source_not_found、9 個來源空模板**；窗口沒有新增 502，但此 owner 在此前有一次 502。逾時仍拖累實際吞吐量，既有 controller 依健康 transport 窗口調整並行，不將它推定為官方限速或清除持久拒絕。此為實際續抓產量，不是同資料集 A/B 提速或來源上限證明。

[最新 128 批收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v31/recent_batches.json)
驗證 **1,367 個正文版本、98,844,175 bytes 原回應**，零錯誤。原回應 bytes 不是壓縮後 wire 流量，也不是完整歷史證明。
[正式唯讀網頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v31/dashboard_live/acceptance.json)
通過 1440／768／390／320 寬度、四來源數字連動、原生刷新與錯誤／恢復；控制路徑維持 405。驗收範圍為 localhost，不主張 WAN 或公開 DNS 已驗證。

[07:56:47 的全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v31/inventory_after.json)
有 **59 個設定 provider、17 個啟用、683 個未註冊來源鍵**，**6,201,477 個已知網址、3,481,678 個已存正文、2,130,202 個待授權網址**。來源鍵不是獨立 provider 數量。CMoney 仍回真正 403，下一次來源可重試為 **08:07:54 Taipei**；本文驗收窗口內維持冷卻，其他健康來源仍有獨立正文產量。其他來源的署名／授權、來源純圖、缺失原文與未確認授權 API 仍在清冊；全部新聞目標維持 active。

## v32 鉅亨行動版欄位與已完成串流修復

**2026-10-10 08:45:30 Taipei**，最終修復的正式新聞服務與 timer 恢復，PID 3280521、owner `fbd2472bea724093971b1c04d3958d3f`。行動版 canonical 欄位匹配及公開 React 串流誤判已修復，正文實際續抓；來源逾時／502、舊欄位缺口與其他來源的拒絕仍未全部解決。其他金融下載服務、原始收據及遠端訓練權限未更動。

### 為什麼請求數不等於正文產量

上一個 owner 的 315 次 HTML 請求累計傳輸等待約 **961 秒**，297 次抽取累計約 **8.8 秒**；傳輸時間是重疊請求的累計值，不是單一程序經過時間。當時已有 **10 次 ReadTimeout、8 次 HTTP 502**。每秒開始請求的設定不能消除來源回應延遲；吞吐量同時受來源間隔、可用並行槽／平均回應時間、抽取速度與入庫速度限制。

[08:32:13 至 08:33:46 的同 owner 窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v32/runtime_final_coherent.json)
有 82 次鉅亨請求，但其中 59 次 HTTP 304、18 次 HTML 請求，新增正文 head 僅 7 個。大量追新分類回應未變、重查或原頁本來空白，不能把 API 呼叫率當作新正文速度；這個混合工作量也不能與其他窗口直接當成相同資料的 A/B 提速實驗。

### 同一原回應的兩個解析根因

[行動版實際回應](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v32/cnyes_and_original_probe.json)
的 `m.cnyes.com/news/id/5464945` 保留完整公開正文，但 canonical、og、NewsArticle 與當篇資料均宣告同 ID 的 `news.cnyes.com`。原通用 matcher 因 hostname 不同忽略其原發布時間與作者，留下 CSS 正文、空時鐘及空署名。

[共用抽取器](../downloader/news_fulltext_publishers.py)
現在只核對實際回應中宣告的同 ID canonical、og、單一當篇 schema、標題、字面 `payment=0`、當篇完整內容、原發布秒數及來源署名。可見時間、aware schema 時鐘與當篇 epoch 必須一致；署名原文與 schema／當篇宣告分開保存。短文及營收表格仍逐格驗證，僅排除已登記廣告骨架，不對真正正文全域移除字元。沒有生成 URL、追加 HTTP、借用鄰篇或猜發布日期。

續抓中另發現行動版 `article_identity_not_verified` 增加。
[三個原生樣本](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v32/mobile_identity_failures_native.json)
中，5456724 與 5456507 的公開文章暫存在 `hidden#S:0`，另有 `B:0` 的可見目的邊界及已完成 `$RC("B:0","S:0")` 顯示指令。這是伺服器串流交付格式，不是可以任意擷取的隱藏內容；[React 的邊界完成實作](https://github.com/react/react/blob/main/packages/react-dom-bindings/src/server/fizz-instruction-set/ReactDOMFizzInstructionSetShared.js)
將已完成區塊的子節點移入目的邊界。通用 hidden 檢查誤拒此原生格式。

修復只辨認兩份原生回應中獨立核對的精確顯示指令 SHA，要求唯一 ID、原可見 template、完整 Suspense 邊界及正確文檔先後順序。Flight 腳本及其他 segment 可以穿插，但完成指令不能早於目的／正文，也不能截斷、帶錯誤參數或追加任意程式；未知 renderer 仍是待修解析缺口。只有這個已證實完成的暫存 wrapper 得以視為顯示，article、段落、署名、時間或格子自身的 hidden／CSS 隱藏仍拒絕。既有免費、身分、時鐘、作者與完整正文核對不放寬，不執行 JavaScript、不啟動瀏覽器來解析文章。

HTML fingerprint 更新至 mobile contract **v3**；此鍵從健康 Cnyes bulk API fingerprint 移除，所以沒有因 HTML 修復重新下載健康 API 歷史。[共用 queue 的限定重排](../downloader/news_fulltext_queue.py)
僅重查已知舊版 fingerprint、行動版同 ID 路徑、未存正文的 terminal identity 失敗。原 v1 有 **150 個候選**重新排程，attempts、error code、原檢查時鐘與收據保留；後續 v2 轉 v3 的對應候選為零。已存 head、inflight、403／429、付費、来源不存在與其重試期限均不重置，重排也不算正文完成。

### 正式正文與反例驗證

[最終原生原回應重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v32/mobile_streamed_source_replay_final.json)
及[正式三篇 immutable head 收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v32/recovered_streamed_cnyes_native_final.json)
均通過，正文、原回應 SHA、當篇署名與發布時鐘可獨立核對：

| 鉅亨原頁 ID | 正文字元 | 原始發布時鐘 | 來源宣告作者 |
| --- | ---: | --- | --- |
| 5456724 | 401 | 2024-02-19T03:34:46.000Z | 鉅亨網新聞中心 |
| 5456399 | 420 | 2024-02-19T01:13:20.000Z | 鉅亨網新聞中心 |
| 5456507 | 402 | 2024-02-19T01:49:56.000Z | 鉅亨網新聞中心 |

其中 5456724 在前次正式下載遇真實 HTTP 502，仍留在 retry；期限過後只續抓缺的那篇，沒有重抓已保存的兩篇或清除原失敗。[前兩個正式正文樣本](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v32/recovered_cnyes_native.json)
亦已核對：6523842 的完整短訊 44 字元，5464945 的完整正文 541 字元、張欽發署名與 2024-02-27T03:16:54.000Z 原時鐘。

[最終完整新聞回歸](../artifacts/operations/agent-workflow/runs/v32-final-interleaved-stream-ful-20261010T004407-0d3ade23/run.log)
**1,455 passed，38.17 秒**。涵蓋 canonical／ID／發布時間／作者衝突、付費、隱藏內容、完整表格、錯誤／截斷／先後順序不對的顯示指令、穿插 Flight frame、未知舊 fingerprint、來源拒絕不重置及重啟不重排第二次。固定測試曾通過、但兩份真實串流回應仍失敗的中間結果也保留：實際完成指令不是緊鄰 S:0，不能拿 synthetic adjacency 假設當成來源驗收；校正後使用同批原回應重新驗證。

### 舊欄位清點與其他來源問題

[08:32 的舊行動版欄位清點](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v32/mobile_metadata_inventory_final.json)
有 **4,912 個已存 URL head、1,203 個不同批次**，其中 **3,815 個缺發布時間、4,533 個缺來源署名**；**3,811 個**具同 ID canonical 的原回應修復候選尚未批次重播／重寫。空署名可能就是來源沒有宣告，不能全部當成可以修復的 parser 錯誤。

[欄位清點工具](../scripts/audit_cnyes_mobile_metadata.py)
採 pinned read-only WAL、每批一次投影 metadata 欄位，這次耗時 2.26 秒、CPU 1.93 秒；不載入正文／原回應，不取得 writer、不追加 HTTP、不修改 queue，也不把存有正文視為 metadata 或完整歷史已驗收。

[08:50:31 的後續同範圍清點](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v32/mobile_metadata_inventory_after_stream.json)
已有 **5,078 個行動版 head、1,222 個批次**；缺時鐘、缺署名與 3,811 個舊修復候選數仍相同，metadata join 零錯誤，耗時 1.72 秒、CPU 1.58 秒。這是新 cohort 的欄位盤點，不是舊候選已全部修好。

[其他來源實際回應](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v32/source_probes.json)
及鉅亨附帶的 FTV 樣本仍列為問題：CMNews 的 snowbaby 原頁有 profile 連結，但沒有可見作者名；FTV 部分頁沒有原 newsroom 署名，另一篇明列《今周刊》轉載及原作者。不能將 schema editor 當原作者、猜 profile 名或把第三方轉載改算 FTV 原創。CMoney 真實 403 與 PeoPo retry deadline 仍按來源規則處理，不拖停健康 provider。

### 最終續抓量與網站連動

[最終程式同 owner 驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v32/runtime_final_code.json)
的九項工程檢查全通過。DB 窗口 **138.22 秒**，HTTP 計數窗口 **136.87 秒**，分開使用各自觀測時鐘：

| 來源 | 新增正文 head | head 每秒 | HTTP 次數 | HTTP 每秒 |
| --- | ---: | ---: | ---: | ---: |
| Anue 鉅亨 | 77 | 0.557 | 151 | 1.103 |
| Global Voices | 188 | 1.360 | 14 | 0.102 |

鉅亨窗口仍有 **6 次 ReadTimeout、1 次 502、21 個已核實空模板**，沒有新增 `article_identity_not_verified` 或 `body_not_found` 計數。失敗連線重建六次，其他請求和 provider 繼續入庫；0.5 秒是當前 client 基準間隔，不是已查證的官方上限。上表是當前混合工作量的實際產量，不主張 A/B 提速倍數、所有錯誤已消失或已達來源最高速度。

[最新 128 批獨立收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v32/recent_batches_final_code.json)
驗證 **1,439 個正文版本、67,546,274 bytes 原回應**，零錯誤；bytes 不是壓縮 wire 流量。前個 baseline 混入重啟前 owner 的狀態，原失敗證據保留，沒有採用該窗口速度；新 baseline 的 DB、status、PID 與開始時鐘一致，驗收未放寬。

[正式唯讀網站](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v32/dashboard_running/acceptance.json)
於 1440／768／390／320 寬度均顯示 running，五來源數字、分頁／刷新及錯誤恢復驗證通過；沒有 JavaScript／資源錯誤或從瀏覽器請求新聞 provider，控制路徑仍唯讀。驗收範圍為 localhost，不主張公開 DNS 或 WAN 已驗證。

[08:45:27 全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v32/inventory_final.json)
有 **59 個設定 provider、17 個啟用、683 個未註冊來源鍵、6,201,496 個已知 URL、3,484,265 個已存正文、2,130,202 個待授權 URL**。鉅亨當時已存 **3,386,197** 個 URL head，仍有 **207,775** 個未存正文的 pending／retry／inflight 工作；既存正文與完整 metadata／PIT／授權是不同驗收。08:48:58 的 CMoney 下一次來源重試為 **09:01:11 Taipei**，PeoPo 為 **09:02:28 Taipei**。全部新聞目標維持 active，舊原回應修復、未可取得正文、授權 API 與未知歷史均未算完成。

## v33 原回應零 HTTP 補件與圖說粒度修復

2026-10-10，針對「鉅亨慢、過程中其他 provider 錯誤也須發現並修正」繼續檢查同一正式下載管線。修復沒有增加另一個 crawler、放寬付費／身分／來源時鐘核對，或將修復版本灌入新篇數。**09:40:38 Taipei** 最終 mobile HTML **v5** 的服務與 timer 恢復，PID 3404085；載入版本與正式續抓的最後驗收另列下節，`active` 本身不是正文／歷史完整證明。

### 原生錯誤及精確修復

| 實際問題 | 根因與修正 | 邊界 |
| --- | --- | --- |
| 5454987 的 `body_not_found` | 可見正文與當篇免費 content 都保留段落首 U+FEFF，schema 省略它。僅 schema 比較視圖移除每段首一個 marker，保存正文仍保留原字元 | 字內／重複 marker 不刪；可見正文仍須與 current content 原樣匹配 |
| 舊行動版缺時鐘、作者及未核實容器 | 同 ID canonical 的已保留回應可重新解析；只補獨立核實的當篇資料，原觀測時鐘／授權／來源回應不改 | 真實既有標題、時鐘、作者衝突仍拒；API 本來沒有署名不算 HTML 錯誤 |
| 舊標題是 `...`，補件仍拒 | 原生記錄是 `css:#article-container`，原合成 fixture 卻是 `css_article`；限定原生 selector、未核實容器、空時鐘／署名的載入標題 | 舊載入標題另存，不將其他 selector 或真正不同的標題一律豁免 |
| 七篇 schema 多出圖說 | 正文抽取排除 figure，schema 卻包含當篇 content 的圖說。現在核對唯一、同序的可見 figure、字面 image src、圖說、完整 current/schema 對照 | 圖說另存 `verified_image_captions_json`，不混入原正文；首圖、隱藏、重複、別圖、未知文字、遺失正文仍拒，不下載圖片 |

共用 [publisher matcher](../downloader/news_fulltext_publishers.py) 保持健康正文相符時的快路徑；只有 schema 差異才做有界圖說比較。HTML capture fingerprint 已更新，健康 bulk API fingerprint 仍排除這個 HTML 版本鍵，故沒有為這次修復重抓 API 歷史。共同 queue 只重排已 pin 的 v3／v4、尚未存正文的 terminal `body_not_found`；已存、lease、403／429、付費及原重試期限都不重置。

[5454987 原回應重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v33/new_parser_failure_replay.json)
及[正式收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v33/recovered_new_parser_failure.json) 均通過：正文 363 字元，來源宣告時間 `2024-02-16T02:02:26.000Z`，署名鉅亨網新聞中心，marker 原樣保留。

### 同一 owner 的零 HTTP 舊資料補件

[canonical retained metadata 維護](../downloader/news_fulltext_metadata_replay.py) 使用按 parser fingerprint 固定的 current-head cohort、每批一次 metadata 投影、有界回應解壓及持久 checkpoint；不每輪掃整個 lake。常駐服務每十秒最多檢查 32 個 head／0.25 秒工作片，原回應 materialization 預算 32 MiB。單次 parse／fsync 可跨過牆鐘預算，這不是硬 RAM／即時中斷保證。已觀測的格式失敗可給最多 64 個明確 ID，仍走相同完整來源、receipt、舊 head compare-and-swap 與 acceptance 守門，不優先重解析不相關歷史。

每次新增 immutable 派生版本，保留原檔／receipt／raw SHA、原抓取與來源觀測時間、最後 HTTP 檢查、attempts、due、validators、拒絕及 lease。新解析時鐘另存；版本數不加入 HTTP attempts、線上吞吐樣本或新 URL head。中斷在 receipt／commit 之間由既有啟動 recovery 接手；若 head 已更新、被 lease 或新拒絕，不覆寫它。

來源授權不升級：舊的已 pin 個人本機 newsroom review 只有在 scope、kind、evidence 及限制相同／更嚴時才可重解析，原授權內容與 SHA 原樣保留；另記目前重解析 policy。不是 Hao、授權 API、商業許可、合理使用判決、PIT 或遠端訓練授權。

實際原回應維護量測：

| 固定維護片 | 檢查 head | 修復版本 | 維護牆鐘／CPU | 新 HTTP |
| --- | ---: | ---: | --- | ---: |
| [初版原生補件](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v33/retained_metadata_native.json) | 1,048 | 36 | 18.16／7.53 秒 | 0 |
| [原生 selector 修正後](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v33/retained_metadata_native_selector.json) | 302 | 297 | 20.10／13.62 秒 | 0 |
| [七篇當篇圖說定向補件](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v33/retained_seven_captions_durable.json) | 7 | 7 | 1.135 秒維護；另有約 52 秒 owner 初始化 | 0 |

以上是不同局部工作量，**排除 owner 初始化的維護量測，不是整條 crawler 的 A/B 提速倍率或官方上限**。原 HTTP attempts counter 均未增加；selector 維護窗口 head 篇數沒有因補件增加。

[載入標題 6601479 的原回應／正式 head 核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v33/repaired_loader_native.json)
通過：原本 `...` 的正文 1,100 字元，來源時鐘 `2026-09-09T05:38:09.000Z`，原署名陳于晴。另有[三篇舊欄位補件](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v33/repaired_metadata_selected.json) 已獨立核對。

[七個原生圖說誤判的原回應重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v33/retained_body_caption_replay.json)
及[正式 immutable 批次／當前 source 重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v33/seven_captions_source_and_body_proof.json) 均通過，零錯誤：6571351、6584161、6577814、6618507、6560464、6620688、6610557。七個 stored head 的原捕獲／HTTP 欄位未改，正文和圖說分開，沒有補猜歷史時鐘。

[最終完整新聞回歸](../artifacts/operations/agent-workflow/runs/v33-final-caption-target-full-re-20261010T013708-04b2a100/run.log)
**1,524 passed，49.56 秒**，涵蓋上述正例、marker／圖說反例、快路徑、不明 selector、未知舊 fingerprint、定向 cohort 邊界、HTTP 拒絕／lease／deadline 保護、原回應 digest、commit 中斷與相容啟動 recovery。早期合成 fixture 通過而原生 selector 仍失敗的結果保留，不把 synthetic success 當成原生驗收。

### 實際續抓、其他 provider 與仍未完成的範圍

[09:26:25 至 09:31:07 的同 owner 實測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v33/runtime_verified_before_caption.json)
九項工程驗收全通過：鉅亨新增 **207 個正文 URL head、0.734 head/s、1.176 HTTP/s**；Global Voices 新增 **399 個 head、1.415 head/s、0.099 HTTP/s**。這是 mobile v4 的混合工作量，不能與另一窗口當同資料 A/B；最終 v5 的續抓另驗。API 分類追新 304、舊 head 重查、來源空模板及 transport timeout，不是新正文。來源等待是全鏈路成本，提高清單呼叫率不等於提高清潔正文產量。

更早[09:15 至 09:17 的檢查](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v33/runtime_final.json)
如實失敗：冷卻已到期的 CMoney／PeoPo 新請求回 HTTP 403，CMNews 新候選也沒有接受正文，故不能宣稱所有原本 ready provider 成功。原失敗收據保留；後續 baseline 依已觀測的來源／article deadline 區分健康工作、真正拒絕及沒有 runnable missing-body work，不修改驗收條件或重設錯誤。當窗口仍有鉅亨 **85** 個、Global Voices **193** 個新 head，不代表其他來源已修好。

[09:15:22 全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v33/inventory_after.json)
仍有 **59 個設定 provider、17 個啟用、683 個未註冊來源鍵、6,201,522 個已知 URL、3,485,769 個已存正文、2,130,211 個待授權 URL**。此為該時鐘的全範圍盤點，不是最終當下數字或 full-history claim。

[09:26:26 欄位盤點](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v33/mobile_metadata_final.json)
有 **5,608 個行動版 current head**，缺發布時鐘 **3,370**、缺來源署名 **4,088**、同 ID alias 待補候選 **3,366**。metadata-column join 零錯誤，牆鐘 3.53 秒／CPU 2.53 秒，HTTP 0；這份快照早於七篇圖說補件。缺署名亦包括來源 API 本來沒有署名，不能一律補造；candidate 數不等於已核實成功。源頭解析、舊資料修復、HTTP 續抓、完整歷史、PIT 與授權／訓練可用性各自保留界限。

全新聞目標仍為 active。CMoney／PeoPo 的來源拒絕、CMNews／FTV 部分原署名或第三方 scope、CNA 授權 API 的實際接線驗證、未註冊 source keys、來源不存在／撤文、剩餘 metadata 及未知歷史仍在工作清冊，未計為全部完成。

### 最終 mobile v5 正式服務與網頁驗收

[09:41:45 至 09:43:59 的最後程式窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v33/runtime_final_mobile_v5.json)
九項工程檢查全部通過，DB／status／PID／開始時鐘與載入 policy 一致，source files 沒有在 owner 啟動後更動。DB 窗口 **134.03 秒**，HTTP counter 窗口 **121.75 秒**，各自使用正確觀測時鐘：

| 來源 | 新正文 URL head | head/s | HTTP 次數 | HTTP/s |
| --- | ---: | ---: | ---: | ---: |
| Anue 鉅亨 | 186 | 1.388 | 272 | 2.234 |
| Global Voices | 158 | 1.179 | 13 | 0.107 |

鉅亨的 runtime controller 當時調到 **0.2 秒 client 間隔／5 個並行槽**，實際所有 start gap 仍符合該 owner 已允許的區間；不是已驗證的官方數字上限。這是當前混合工作量的真實產量，不能宣稱與前個不同窗口相比的因果提速倍數。窗口內無新增 ReadTimeout／403／429／502、`body_not_found` 或作者誤判計數，但有 **46 個已核實來源空模板**，並不冒充正文；既有和未來的 transport／拒絕風險仍存在。

[最新 128 批 immutable 收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v33/recent_batches_mobile_v5.json)
驗證 **1,661 個正文版本、157,670,532 bytes 私人原回應**，零錯誤。這不是 wire 流量、整個 lake 或完整來源歷史的證明。當時鉅亨已存 **3,387,295** 個 URL head，仍有 **206,403** 個未存正文的 pending／retry／inflight 工作；剩餘工作不能全部保證來源仍存在、免費或有完整文字。

[正式唯讀網頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v33/dashboard_mobile_v5/acceptance.json)
在 1440／768／390／320 寬度全部顯示 running，五個來源數字、分頁／刷新、錯誤恢復與控制路徑唯讀均驗證。瀏覽器沒有請求新聞 provider、原文／金鑰／帳號不入公開資料；驗收限 localhost，不主張 WAN／公開 DNS 已實測。09:43:59 的 CMoney deadline 仍是 **09:45:42 Taipei**；PeoPo 沒有當時可派送的 missing-body work，不等於其舊 403 已修復或歷史完成。CMNews 的 terminal 問題同樣不因排程 idle 算完成。

## v34：鉅亨實際 `exp=a` 頁面的身分驗證與續抓

2026-10-10 的正式抓取又出現 `article_identity_not_verified`。這不是全部新聞消失：排程中的原始 URL 帶有 `?exp=a`，先前 mobile v5 只接受不帶 query 的手機版。既有 URL 不能任意去除參數，也不能只因路徑 ID 相同便宣稱內容等價。

mobile **v6** 現在接受 `https://m.cnyes.com/news/id/<numeric-id>` 的空 query 與實際觀測的精確 `exp=a`，仍逐次要求同 ID 的 canonical／Open Graph／NewsArticle／current free data、可見正文、標題、時鐘及署名互相符合。requested／final URL 和 query 原樣保存；未知 query、付費、隱藏內容、不同正文及時鐘衝突仍拒絕。來源 fingerprint 為 `2c69e752ee3632e83fa4bde770b6c833b03ce4985596f4eea51cfc68a6fe3c05`；批次 API 的 contract 沒有因此改版或重新下载歷史。

[原生 canonical 抓取與 immutable 收據核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v34/exp_a_native_source_proof.json)
確認 `5437242?exp=a` 正文 **447 字**、原發布時間 `2024-01-25T01:19:21.000Z`、署名「鉅亨網新聞中心」及相同原回應 SHA；新版本重讀來源 bytes 也通過。這是保留原 URI 的實測，不是用舊內容填入新 capture。

[一次性精準重排](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v34/query_recheck_cohort.json)
只重排 **20 個**舊 v5 終止的相同 query／身分錯誤；原 attempt、錯誤與查詢時間保留，已有正文、來源拒絕及有效 lease 不重置。候選重排不等於取得正文。
[唯讀 query 清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v34/query_view_inventory.json)
發現 7,390 個帶 query 的手機 URL：`exp=a` 7,345 個、`exp=b` 43 個及另外兩種 query。v6 沒有把未驗證的 `exp=b` 或其他參數一併視為已修復；URL head 也不等於去重後的原始文章數。

本版 **1,533 項測試通過／41.22 秒**。但
[10:05:47 至 10:09:18 的正式同 owner 窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v34/runtime_final.json)
顯示速度仍偏低：DB 窗口 211.20 秒／HTTP counter 窗口 217.51 秒，鉅亨 **64 個新正文 URL head／0.303 head/s**、142 次 HTTP／0.653 次/s；Global Voices 同時新增 284 個 head／1.345 head/s。鉅亨有 63 次 304、2 次 ReadTimeout、1 次 502 和 10 個来源空模板，沒有新增 query 身分錯誤，也沒有 403／429；runtime 已降為 **0.5 秒／單路**。這個較慢的窗口保留，不能只引用先前 v5 的 1.388 head/s 當目前速度。

[最近 128 批收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v34/recent_batches.json)
核對 1,463 个正文版本、零錯誤；九項 runtime 工程檢查及
[四種寬度的唯讀網頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v34/dashboard/acceptance.json)
均通過，範圍仍限此 owner／窗口／最近批次與 localhost。當時 CMoney 的實際 403 下一次期限是 10:18:21 Taipei；PeoPo 無當時可派送的正文工作，並不表示舊 403 已恢復。

## v35：傳輸恢復不能反覆降並行，測完再恢復正式 owner

[修正前共用 controller 診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v35/capacity_before.json)
揭示另外的速度根因：偶發 timeout／502 的不健康窗口會逐次把並行數 **4 → 3 → 2 → 1**；1 路時仍有 2 個錯誤／22 個完成請求（79.28 秒），因此沒有證據證明繼續序列化可以根治來源問題。304 只能證明傳輸可達，不能當歷史正文吞吐；錯誤預算也不是來源公布的 RPS 配額。

在 `news_fulltext_capacity.py` 的 **measurement contract v6** 共用實作中，每次傳輸恢復 episode 最多降低一次 pressure。後續不健康窗口維持已降低的 capacity、重置健康 streak，不升速也不再逐次減槽；此 episode 跨 workload 與 owner 重啟保存，部分健康 streak 不跨 owner 重用。連續健康窗口才回復 reviewed baseline 或近期同 workload 的已驗證 capacity，重新量測而非沿用舊吞吐預估。健康恢復後才開新 episode。403／429、Retry-After、503 過載、路由／TLS 故障及連續錯誤斷路器仍有最高優先權，既有拒絕與來源 deadline 不重置。

新的 `transient_recovery_pressure_reduced_once`／`transient_recovery_pressure_held` 計數與唯讀 audit contract v2 可核對此行為；不是增加發送配額。來源/body fingerprint 沒有因 controller 行為改變，所以不重抓 API／正文歷史；校準 fingerprint 更新，以免把舊 controller 的 capacity 證據當成新測量。

[全新聞回歸收據](../artifacts/operations/agent-workflow/runs/v35-full-news-regression-20261010T022436-841081a7/run.json)
為 **1,542 passed／39.73 秒**，包含跨 workload／重啟維持單次恢復、representation 錯誤不開新減速 episode、健康後恢復、壞 checkpoint，以及 403／429／503／Retry-After 不能被繞過。曾有測試 argv 的不存在檔名及工作流程 option 位置錯誤，後續正確命令驗證並標記 superseded；原失敗收據保留，沒有冒稱 publisher 錯誤已因此消失。

正式 timer／service 在 **10:25:33 Taipei、PID 3504392** 恢復。此刻只證明啟動，下一節以新 owner／PID／開始時鐘一致的原生續抓、獨立收據及網頁檢查建立本版實際效果；不能把單元測試或重新啟動算成提高了正式吞吐或全部來源都修好了。

### v35 正式續抓、別名候選與不通過的長窗口

[10:27:03 至 10:31:22 的同 owner 檢查](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v35/runtime_final.json)
九項工程檢查通過，DB 窗口 259.30 秒／HTTP counter 窗口 260.94 秒：鉅亨新增 **179 個正文 URL head／0.690 head/s**，243 次 HTTP；Global Voices 同時新增 **369 個 head／1.423 head/s**。鉅亨仍有 3 次 ReadTimeout、1 次 ConnectionError、2 次 `body_not_found`、30 個來源空模板和 5 次來源不存在。當時保有 3 個槽／0.5 秒派送間隔；不能宣稱上游錯誤消失，亦不能拿不同工作量窗口計算因果提速倍數。

[兩個手機版失敗 ID 的既有主站收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v35/failed_mobile_existing_canonical.json)
確認 `5430496`／`5432092` 已有 **371／626 字**的 canonical API 正文及原發布時間，批次與當前 head hash 核對通過，原 capture 保留。API 原回應沒有作者，`authors_json=[]` 原樣保存。這證明兩個 ID 並非完全沒有資料，不證明目前手機頁已成功，亦沒有把 API 內容冒充新的手機 HTTP 回應。

新增 `scripts/audit_news_cnyes_alias_candidates.py`，用 host/state 索引取最多 4,096 個 pending 手機 URL，逐一精確 lookup 既有 canonical head；沒有 publisher HTTP、第二個 writer、重排、內容複製或接受別名。
[正式 512 項樣本](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v35/alias_candidates.json)
在 **0.138 秒 wall／0.081 秒 CPU** 完成：**505 個**有完整主站 head 候選，但 **489 個 metadata 標題不同**、只有 16 個完全相同，另 7 個沒有完整 counterpart。這是索引順序的小樣本，不是隨機抽樣、全剩餘範圍或 98.6% 已證明正文相同；沒有據此刪除／合併或宣稱來源完整。下一步要把真正缺文章、缺時鐘／署名的補強，以及可選的頁面版本校驗分開，先證明 publisher story grain／來源權限／版本再減少重複抓取。

[所有 provider 的唯讀清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v35/provider_inventory.json)
於 10:27:04 核對 59 個設定來源、17 個 enabled、683 個未註冊來源鍵；已知 6,201,543 個 URL、3,491,844 個有正文 head、2,130,211 個待授權 URL。此 inventory 單次花 26.97 秒 wall／26.60 秒 CPU，不能塞入每次 dispatch 或網頁輪詢。各來源的 terminal／scope／署名／paywall／404 錯誤逐列保留，不把 pending=0 当整個新聞網站歷史完整。

[當版全新聞回歸](../artifacts/operations/agent-workflow/runs/v35-full-news-plus-alias-auditor-20261010T024247-811e512d/run.json)
為 **1,550 passed／51.22 秒**，
[四種寬度的正式唯讀頁面](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v35/dashboard/acceptance.json)
也通過。但
[10:27:03 至 10:46:48 的長窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v35/runtime_final_extended.json)
**沒有通過**：鉅亨雖新增 807 個 head／0.681 head/s、Global Voices 新增 1,289 個，九項檢查只有八項成功。`Global Voices` 的最低派送 gap **9.996969 秒**小於 owner 的 10 秒 floor（檢查 tolerance 1 ms），不得改寬門檻或隱去這一筆。

[同時期真實 transport 恢復證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v35/capacity_final.json)
顯示 19 次 transport errors、11 次 502，兩個恢復 episode 各只降一次、**10 個不健康窗口維持已降低 pressure**，仍保有 3 個槽；無鉅亨 403／429。CMoney 又有一次 403、PeoPo 又有兩次，當時 retry deadline 分別是 10:48:23／11:00:47 Taipei。即使健康來源繼續出正文，這些來源拒絕仍沒有修復，不把窗口驗收當所有 provider 成功。

## v36：限速與監控使用同一個實際派送事件

長窗口的 gap 不符不是靠放寬浮點門檻修正。原 `NewsHTTP._reserve_request()` 先寫 `last_calls=time.monotonic()`，再由 `NewsMetrics.request_started()` 另取時間；第一個執行緒若在兩者間被 CPU 排程或 telemetry lock 延後，下一次限速仍從較早時間起算，監控／派送的 gap 就可能較短。

現在 **request dispatch clock contract v2** 在 metrics lock 內取一次真實 monotonic 時間並返回，pacer 的 `last_calls`／下一個 deadline 使用同一個事件。發送鎖只保護派送承諾；網路 response 仍可平行，不把整段 HTTP 等待塞入發送鎖。cooldown／目前 floor、授權期限、停止狀態及最後 commitment 在同一 HTTP lock 內檢查，避免回應退避與下一筆 commitment 的競速。`measurement contract v7` 更新校準 fingerprint；terminal outcome contract v1 及 body／API fingerprint 不變。

3 ms／200 ms 的確定性排程延遲測試，證明下一筆 commitment 仍與前筆相隔完整 10 秒，而非改寫已觀測時間戳。這個證明是 **程序派送承諾**，不是 kernel socket 送出或 WAN 到站間隔；新的 runtime audit contract v3 明列此範圍，增加 shared-clock contract 檢查，仍保留原 1 ms tolerance。

[最新全新聞回歸](../artifacts/operations/agent-workflow/runs/v36-complete-news-regression-20261010T025609-53760187/run.json)
為 **1,552 passed／52.88 秒**。正式 timer／service 在 **10:58:12 Taipei、PID 3572249** 恢復，parser／body fingerprint 不因計時修復改動，既有資料與來源拒絕期限不重置。前述不通過的長窗口保留；本版另以新 owner 的基線、十項工程檢查、原始收據及唯讀網頁驗收，不能沿用前個 PID 的成功數字。

### v36 最新同 owner 續抓與十項工程驗收

[11:00:43 至 11:05:24 Taipei 的正式窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v36/runtime_final.json)
**十項工程檢查全部通過**，owner `dfc3ac41b2d14324af3e3ff83429c2b0`／PID 3572249／開始時鐘／policy／source code 一致。DB 窗口 **280.95 秒**，HTTP counter 窗口 **282.00 秒**：

| 來源 | 新正文 URL head | head/s | HTTP 次數 | HTTP/s |
| --- | ---: | ---: | ---: | ---: |
| Anue 鉅亨 | 193 | 0.687 | 352 | 1.248 |
| Global Voices | 301 | 1.071 | 28 | 0.099 |

鉅亨仍有 **14 次 502、5 次 ReadTimeout、3 次 `body_not_found` 與 49 個來源空模板**；沒有新增鉅亨 403／429。單次 pressure reduction 後，4 個不健康窗口沒有重複減槽，仍維持 3 個槽／0.5 秒 commitment 間隔。這是持續正常派送並處理失敗，不是源站已零錯誤或已實測到官方最大 RPS；不同窗口不可宣稱因果提速倍數。此刻已存 3,389,729 個鉅亨正文 URL head，仍有 203,431 個未存正文的 pending／retry／inflight，另有 terminal、頁面版本與來源範圍問題；這些數字不是去重後完整文章清冊。

[最近 128 批 immutable 收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v36/recent_batches.json)
驗證 **1,300 個正文版本、零錯誤**。
[唯讀網頁驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v36/dashboard/acceptance.json)
在 1440／768／390／320 寬度全部通過，限 localhost；沒有 browser publisher requests 或原文／帳號／金鑰公開。source scopes 與 private/export flags 不變。

[當版來源錯誤／capacity](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v36/capacity_final.json)
仍列 CMoney、PeoPo 的真實拒絕。11:05:24 的下一次期限分別是 **11:18:25／11:15:50 Taipei**，之後若再拒絕，以最新源站回應更新，不回填成功。新版時間戳修復取代的是前個長窗口的派送 conformance 失敗，沒有取代或隱去這些 provider 拒絕。timer 保持 enabled／active，正式 owner 繼續抓；下一階段優先查 API／article identity／metadata augmentation 的分工以避免無用重抓，再依清冊處理其餘 provider 的正文、署名、端點與授權缺口。全部新聞／全部歷史的原目標仍 active，沒有完成宣告。

## v37 正文候選分工與正式續抓驗收

鉅亨的有效工作不能只以 URL 數衡量。本輪最新可跑 512 個 pending URL 樣本，有 389 個手機版已存在主站正文候選，123 個是待正文的主站 URL；候選不代表相同正文或可替代觀測。
[兩組主站與手機版原始回放](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v37/story_identity.json)
及 [API 與 `exp=a` 手機版原始回放](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v37/story_identity_api.json)
共驗證三組來源宣告的 story identity，全部正文 SHA 不同，API 那組的作者也不同。主站 6621852／6621989 分別有 964／398 字，手機版是 986／447 字；相同發布瞬間仍不能複製正文、補作者、改 capture vintage 或刪除其中一份。這些回放是零 HTTP 的原始回應與 immutable receipt 驗證，不是全清冊的重複率估計。

`news_fulltext_cnyes_work` 的 contract v1 只作**排程候選分類**：bare／原樣 `exp=a` 手機 URI 可建立候選主站 ID；未知 query、不安全 host、fragment 或 HTTP 仍不正規化／合併。共用 owner 使用有界背景切片、rowid checkpoint 和 ready-state partial index，優先處理尚無已知正文候選的缺口；每八次這類派送後，仍給已有候選頁面一次服務機會。儲存正文追新、未標日期及 due-error 的原獨立 fairness／deadline／lease 仍有效，所以總派送比例不是固定 8:1。新主站正文接受時，只有派送提示隨 indexed target 更新；所有來源觀測、原始內容、時鐘、署名、拒絕和 attempts 都保留，不因分類標成 complete。

來源 body／API fingerprint 仍為 `2c69e752ee3632e83fa4bde770b6c833b03ce4985596f4eea51cfc68a6fe3c05`，既有歷史不重置。只有鉅亨的 capacity 校準 fingerprint 加入新派送契約／burst；其他 provider 的校準與 source cooldown 不重置。
[全新聞回歸](../artifacts/operations/agent-workflow/runs/v37-complete-news-regression-20261010T033252-e0de355f/run.json)
為 **1,579 passed／49.48 秒**；新增唯讀 auditor 與原始回放的
[補充測試](../artifacts/operations/agent-workflow/runs/v37-readonly-value-auditor-tests-20261010T033939-9be1e794/run.json)
為 **55 passed／2.78 秒**，範圍有重疊，不相加為全套測試數。

正式服務在 **11:34:04 Taipei、PID 3652619** 啟動。新索引的首次建置、既有初始化與 scope seeding 仍有成本：11:37:32 才出現同新 owner 的 inventorying 快照，11:38:10 已進入 running。這是服務啟動到快照的觀測，不是精確分拆的純索引時間，也不是 Windows／WSL 冷開機驗收。初始化中的兩次過早唯讀 audit（索引尚不可見／尚無 HTML 派送）保留失敗，不因服務 active 回填成功。

[11:40:53 的索引與實際派送核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v37/work_dispatch_live.json)
通過：六個 ready lane 都使用 partial-index search，無 full table scan／temp sort；每次唯讀 lookup 約 **0.070–0.189 ms**。單次完整 candidate-ready count 花 **60.9 ms wall／57.8 ms CPU**，僅供稽核，不加入每次派送／網頁輪詢。背景分類當時有 232 個切片，合計 13.93 秒 wall／6.92 秒 CPU；最慢一次 0.201 秒。40 ms 是逐列協作停止目標，不是能打斷單筆 SQLite 操作、查詢或 commit 的硬 wall deadline；分類仍在增量進行，0 類包含尚未分類的工作。

[11:38:15 至 11:43:23 的同 owner 正式窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v37/runtime_final.json)
**十項工程檢查全部通過**，308 秒新增鉅亨 **243 個正文 URL head／0.789 head/s**，HTTP counter 窗口 304 秒、430 次／1.414 req/s。窗口內 264 次 HTML 派送沒有已知正文候選、59 次已有候選；另外完成 83 個既有 raw metadata 版本補強，不算新增 URL 或 HTTP。Global Voices 獨立新增 30 個正文 head。與上一版的 0.687 head/s 是不同來源工作窗口，不宣稱因果提速倍率、唯一文章速率或官方最高頻率。

[最近 128 批收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v37/recent_batches.json)
驗證 **992 個正文版本、零錯誤**。
[唯讀網頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v37/dashboard/acceptance.json)
在 1440／768／390／320 寬度通過，限 localhost，瀏覽器未呼叫 publisher API，未公開原文／金鑰／帳號。
[目前 provider 錯誤](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v37/capacity_live.json)
仍有鉅亨 3 次 502、3 次 transport error（另有一個正文失敗、62 個來源空模板）；CMoney、PeoPo 各又有一次 403。鉅亨維持 3 槽／0.5 秒 client commitment 間隔，恢復 episode 沒有反覆降槽。這些來源錯誤未宣稱根治；原目標仍 active，下一步持續核對 candidate 分類、來源／欄位缺口與合法端點替代，並降低首次／每日 scope seeding 的重複計算。

## v38 鉅亨歷史範圍重建與空維護成本

本輪修正兩項本機重複工作，保留全部鉅亨分類、歷史起點、原始正文、署名、發布／觀測時鐘及拒絕後的重試時間。歷史範圍是「已排程」的範圍，不是下載完成；受限頁面及已拆分父範圍仍由原 retry／child owner 接續。

[原生 65 端點 AB／BA 比較](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v38/scope_maintenance.json)
使用同一份只讀 WAL 快照，包含 **284,957 個既有分頁網址**，兩輪前後反序測量。所有端點的已排範圍與待建立缺口 SHA 完全一致。僅對確切的原生成分頁前綴去掉 page 成分後，在 SQLite 中去重查詢；非原生成前綴、重複參數、特殊拼寫等仍走原解析，不刪除原 URL 或更動其 outcome。SQL 使用 endpoint URL 索引；冷維護的 DISTINCT 使用暫存 B-tree，不放進逐篇派送路徑。

| 本機範圍重建 | 原逐分頁解析 | 新按查詢範圍解析 |
| --- | ---: | ---: |
| 中位 wall 時間 | 5.3074 秒 | 1.8922 秒 |
| 中位 CPU 時間 | 5.2962 秒 | 1.8876 秒 |
| 每輪需解析的查詢數 | 284,957 | 20,958 |

這是本機範圍重建約 **2.80 倍**、wall 成本減少約 **64.3%** 的配對測量，不是 HTTP、唯一文章或全歷史下載的加速倍率。既有分頁的 priority 只在維護契約／端點集合變更時遷移；新範圍、拆分子頁及後頁仍由原插入入口帶入相同 priority。每日／新 owner 不再無條件重算全部舊頁優先序。維護契約 v1 納入鉅亨的 capacity fingerprint，其他 provider 校準、拒絕 ledger 與來源 body fingerprint 不變。

候選分類掃完後，不再每次寫回相同 cursor；空背景 seek 改為每 **5 秒**，有工作時維持 **0.25 秒**。新匯入仍即時分類，舊列轉狀態仍在 claim 時有界 lazy classify；空 seek 不延後實際下載。已設定 stop 時不先執行三個 seek，受損 cursor 仍保留原證據並由共用恢復入口修正。`40 ms` 仍是逐列協作目標，非中斷 SQLite／commit 的硬 deadline。

[聚焦測試](../artifacts/operations/agent-workflow/runs/v38-scope-maintenance-focused-co-20261010T035813-e89710e0/run.json)
為 **155 passed／4.17 秒**；[全新聞回歸](../artifacts/operations/agent-workflow/runs/v38-complete-news-regression-20261010T035858-e8ad5d20/run.json)
為 **1,594 passed／53.54 秒**，範圍重疊不相加。最初的測試命令誤用不存在的測試檔，沒有執行測試或碰來源；原 exit 4／log 保留，更正後通過並追加 superseded 工程處置。

### 官方替代入口的實際能力

[CMoney GetStockRssNews 文件](https://www.cmoney.tw/cm-service/CMService.asmx?op=GetStockRssNews)
確實提供日期區間與筆數參數，也要求 `commkey`／`memberPk`；回應是標題、原文 URL、來源頻道等 RSS 中繼資料，沒有正文。不能拿這個介面當成解決 CMoney 全文 403 的免費替代。
[官方資料串接說明](https://app.cmoney.tw/official/product/valued-data)
列出授權 HTTP／批次服務；本輪只核對官方文件，未對這些介面發送查詢或宣稱已驗證連線資格。

[PeoPo 使用規範](https://www.peopo.org/about/regulation/P1-1.htm)
說明作者可以選 CC 授權，但平台或其他權利人的內容權利不因此全域放棄。維持逐篇原授權核對，不能以公視品牌或另一個網址覆蓋既有 403／權限證據；本次未新增猜測的 API、換 IP 或登入繞過。這兩個來源仍是未解範圍，不計成已下載。

### 正式 owner 續抓驗收

只安全停啟新聞 service／timer；**12:00:45 Taipei、PID 3710738、owner `888d0902041941e987494af508db641f`** 啟動，NRestarts 為 0，timer 維持 enabled。首個確認 running 快照為 12:02:05；此時已存在 v37 的索引，因此不能與 v37 首次建索引的啟動耗時當成配對加速測試。正式 scope read 合計 **1.822 秒**；一次 priority migration 合計 **6.632 秒**，後續以耐久契約標記避免每日／新 owner 重複遷移。

[正式派送與索引核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v38/work_dispatch_live.json)
通過；六個 ready lane 仍是 partial-index search，無 full scan／temp sort，當時已有 61 次無已知正文候選派送及 7 次既有候選派送。已耗盡分類的 21 次背景檢查合計 **4.58 ms wall／4.33 ms CPU**；這個 idle 工作量不同於 v37 的首次有資料分類，不將兩個窗口的總耗時相除當作優化倍率。

[同 owner 正式窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v38/runtime_final.json)
在 **12:08:53 Taipei** 通過十項工程檢查。資料庫窗口 **383.721 秒**新增鉅亨 **225 個正文 URL head／0.586 head/s**、Global Voices **33 個正文 head**；HTTP counter 窗口 **389.458 秒**有鉅亨 **423 次／1.086 req/s**。另有 **119 個 retained metadata 版本補強**，不算新 URL 或 HTTP。這是不同來源工作窗口，沒有證明網路或唯一文章吞吐量高於 v37。

[12:04:53 的最近 128 批只讀快照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v38/recent_batches.json)
驗證 **718 個正文版本、零錯誤**；不是後續所有寫入或全庫證明。
[localhost 唯讀網頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v38/dashboard/acceptance.json)
在 1440／768／390／320 寬度通過，連動新 owner；沒有 publisher API 呼叫、原文／帳號／金鑰公開，也不是 WAN 驗收。

[全部 17 個已啟用 provider 的即時錯誤核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v38/capacity_all_enabled.json)
與 owner 一致。鉅亨本輪有 **6 次傳輸錯誤**，終端計數均為 article HTML 的 `ReadTimeout`；本輪沒有鉅亨 502／503／403／429。恢復 episode 只降低一次壓力，其後保持有界 3 槽／0.5 秒間隔並繼續處理正文，不把讀取逾時說成測得的官方限速。CMoney 的舊拒絕冷卻沒有被重啟清掉，到期後又有 **1 次 403**，仍保留 waiting_source；PeoPo 本輪未出現新的 HTTP error 樣本，既有拒絕／未取得內容不因此變成已修復。其他 provider 的無錯誤窗口亦不是全部成功或歷史完整證明。

原新聞目標仍 active。下一步沿用這個 owner，細分鉅亨 HTML 讀取逾時的 host／連線／回應階段及來源空模板，保留原請求與合法替代端點證據；繼續補清冊缺口，不重置拒絕 ledger、不開第二個下載 writer。

## v39–v40：主機／傳輸階段診斷與單主機 gateway 故障隔離（2026-10-10）

### 根因與原始證據

[v39 唯讀正式快照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v39/transport_before_isolation.json)
於 **12:35:10 Taipei** 核對全部 17 個已啟用 provider，owner 為 `8f89fccd0f1242bdb2daed9a79d3ebda`。
鉅亨 `amp-news.cnyes.com` **16 次全是 502**；同一 owner 的桌面主機 **83 次成功／1 次 502**，手機主機 **9 次成功／1 次讀取逾時**，API 主機 **65 次完成（5 次 200／60 次 304）**。
這些是 HTTP 交換，不是正文筆數或全歷史完成證明。原 publisher 熔斷器將單主機 gateway 故障連帶套用到健康主機，是可修的排程／故障域耦合；無法宣稱修好了來源的 AMP 伺服器。

手機版逾時的私有耐久事件已獨立驗證：**25.0976 秒、awaiting_headers、未取得 HTTP 狀態／正文位元組**，例外鏈為 `ReadTimeout → ReadTimeoutError → TimeoutError`；本次故障連線已重建，未開整個 provider 的傳輸熔斷。事件在後續重試成功後仍保留。
「尚未收到標頭」包含客戶端／連線／TLS／等待回應等階段，不能當成已分離的 DNS、TLS 或伺服器 TTFB。
[Requests 官方說明](https://requests.readthedocs.io/en/latest/user/advanced/#timeouts) 的讀取逾時也不是整條工作鏈的絕對時限；本次沒有為改善數字而縮短既有 timeout。

### 共用修正與邊界

- `NewsMetrics` 在同一終端計數鎖內，增加每 provider 最多 16 個 hostname 加 overflow 的統計；標籤沒有路徑、查詢、帳號或正文。標頭前／標頭後為 wall-only 計時，不把未量測的階段 CPU 偽報成 0。
- `NewsHTTP` 只在傳輸失敗時寫入私有、按 UTC 日分桶的 `transport_problems` 事件，綁定 owner、來源／授權 fingerprint、時鐘與例外類型；不存部分正文、headers 或例外訊息。目錄 0700、事件 0600；成功請求不增加診斷寫入。診斷磁碟錯誤不蓋掉原始重試與連線重建。
- `GatewayHostCircuit` 只隔離 **無 Retry-After 的 502／504**，且須同 provider **另一個 hostname 在最近 120 秒實際完成成功交換**。該主機自上次成功後累積三次符合的故障，暫停 30 秒；期限耐久保存，重啟仍遵守，到期解除 host 排程限制，仍遵守原項目 retry／lease／授權條件。未知／過時的健康證據、503、403／429、Retry-After、路由／TLS 故障仍沿用保守的 provider 級處理。
- 原 provider 共用 limiter、robots floor、429 學到的較慢速率與授權／拒絕紀錄不變；沒有另一個身分、代理、登入、別名正文或第二個 queue writer。HTTP 真實 502 與不可取得狀態不會抹掉。
- 所有 article dispatch lane（含鉅亨候選、缺正文、undated、retry、stored recheck）在 LIMIT 前，使用原索引和綁定參數過濾暫停主機。沒有暫停主機時 SQL／參數維持原樣；不更新暫停新聞的 due、attempt、正文、作者或發布時鐘。
- 容量量測升為 **v8**，不用舊 v7 capacity 樣本接續新校準。原始請求／回應／錯誤／全工作 wall／每請求正文量仍保留；另外列 `isolated_gateway_host_errors` 與非隔離結果的壓力分母。`healthy` 明確是非隔離結果的壓力健康，`all_observed_hosts_healthy=false` 仍揭露故障主機，不是全來源成功／限額證明；只有隔離錯誤且沒有健康樣本不能升速。
- 唯讀 auditor 把 hostname 終端計數與全 provider 終端量核對，並檢驗最近兩個 UTC 日、最多 128 個事件的檔案 SHA／物件 SHA／來源契約／權限；不呼叫來源、不輸出 URL／正文／例外訊息，不冒充所有歷史事件驗收。

### 測試、恢復與待驗收

v39 corrected focused **134 passed / 3.15s**，新增唯讀事件 auditor **16 passed / 1.15s**；v40 最終 focused **243 passed / 4.56s**。
[全新聞回歸收據](../artifacts/operations/agent-workflow/runs/v40-complete-news-regression-20261010T044331-c2ec5cea/run.json)
為 **1,627 passed / 42.46s**。涵蓋部分正文不能冒充成功 200、失敗證據不洩漏、正常成功不寫診斷、磁碟錯誤保留重試、短暫隔離跨 owner 恢復與到期、全部 article lane、原索引、403／429／503／Retry-After 保護及原始／壓力分母分開。

開發中發現的計時漏參數、測試凍結 monotonic、auditor 對 `http_capacity=null` 不相容均修正；失敗／終止 log 保留，以上述成功 focused／全回歸收據解消，不部署失敗版本。首次 v39 baseline 仍是舊 owner 的 stopped 快照，沒有拿它做有效跨 owner 速度比較。
新聞唯一服務與 timer 已恢復，PID **3814937**、NRestarts **0**；這只是程序恢復，不代替新 owner 的正文／收據／網頁驗收。接續同 owner 量測，原全新聞目標仍 active。

### v40 正式整鏈路驗收（12:57:54 Taipei）

[同 owner 正式窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v40/runtime_final_fresh.json)
通過全部十項工程檢查；PID **3814937**、owner **`c2e7dd103db34dccbcb74b0a32c44677`** 未變、NRestarts **0**，載入的 policy／來源程式／時鐘／實際派送間隔一致。
資料庫窗口 **550.120 秒**新增鉅亨 **751 個正文 URL head（1.365 head/s）**、Global Voices **47 個 head**；HTTP counter 窗口 **549.996 秒**的鉅亨實測 **1.898 req/s**。保留回補、重試、解析與耐久提交，不是只有 request／解析局部時間。此來源工作 cohort 不同於 v38，不拿兩個觀察窗口相除宣稱整體加速倍率／官方最高速／唯一新聞篇數。

[主機仍在隔離時的正式派送核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v40/work_dispatch_final.json)
包含 `amp-news.cnyes.com` 的 filter；六個 ready lane 皆保留 partial-index seek，無 full scan／temp sort，64 次重複查詢的最慢平均 **0.08265 ms／次**。這是索引尋找的局部時間，不是整鏈路性能倍率。

[最新獨立正文收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v40/recent_batches_fresh.json)
於 **12:56:19 Taipei** 驗證最近 **128 批／1,275 個正文版本**，錯誤為 0。這是該只讀快照的最近批次驗收，含先前批次，不代表全庫或後續每筆 751 head 都已逐筆重驗。
[localhost 網頁核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v40/dashboard/acceptance.json)
在 1440／768／390／320 寬度通過，實際保存 head／原網址進度與 canonical 唯讀資料一致，無來源 HTTP、原文／金鑰／帳號公開、JavaScript／資源錯誤；不是 WAN 驗收。742 是已設定或 metadata 觀察到的來源鍵，非 742 個啟用網站；正式設定 59 個 provider、其中啟用 17 個。

[同 owner 全部 17 來源錯誤清點](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v40/capacity_all_enabled.json)
在 **12:53:37** 保留鉅亨 **11 次傳輸錯誤／17 次 502**，CMoney／PeoPo 各 **1 次 403**。
[12:53:38 的耐久失敗快照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v40/transport_final.json)
實際驗證鉅亨 **15 個新 owner 傳輸事件**：13 個 awaiting_headers、2 個 reading_body；15 次皆重建失敗 session，診斷／host checkpoint 寫入錯誤均為 0，publisher `http_circuit_opened=0`，17 次 gateway 錯誤以隔離類別保留。正文中途失敗包含讀取逾時與 chunked 協定中斷；已收到 200／部分位元組不會被當成接受正文。AMP 原站仍 502，CMoney／PeoPo 原站仍拒絕，沒有宣稱修復原站或替它們偽造內容。

全來源核對改用正式 CLI，預設全部啟用 provider（上限 32），保留指定 provider 選項，避免 wrapper 的 str／Path 型別錯誤；報告契約升 **v3**，明列原始與壓力分母／健康範圍。
[最終全新聞回歸收據](../artifacts/operations/agent-workflow/runs/v40-final-corrected-news-regress-20261010T045614-8b1b1184/run.json)
為 **1,628 passed / 46.49s**。兩個仍斷言舊 v2 報告版本的測試已同步 v3；原失敗紀錄保留。
第一次正式驗收因正文收據超過五分鐘而正確拒絕，沒有降低新鮮度門檻；重新獨立核驗最近批次後才取得十項 acceptance。所有實際來源錯誤仍保留；服務持續自動續抓，原全新聞目標仍 active。

後續保留 v40 唯一 owner，繼續讀取主機／階段事件與未取得正文的既有 raw／metadata 證據，優先處理可修的端點／解析缺口；對拒絕來源只核對合法、已授權或公開替代端點，不清掉拒絕、冒充另一觀察或開第二個下載 writer。

## v41：全來源重新清點、非文字正文辨識與新 owner 續抓驗收（2026-10-10）

### 清冊與問題分界

[13:10:11 Taipei 的完整只讀清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v41/provider_inventory.json)
保留 **59 個設定 provider／17 個啟用 provider／683 個未註冊來源鍵**，已知 **6,201,611 個 URL、3,499,800 個已保存正文 URL head**，另有 **2,130,235 個 URL 等待授權**。來源鍵不是網站數，URL head 不是去重新聞篇數；metadata 最早／最新日期不是歷史發布時鐘或全歷史涵蓋證明。單一固定 WAL 快照成本 **32.118 秒 wall／25.125 秒 CPU**，沒有 publisher 呼叫或佇列寫入；不把這類全量清點放進每次 UI 更新。

鉅亨的主機／gateway／等待標頭問題沿用 v40 的故障域隔離，沒有用改短逾時、忽略正文驗證或清掉拒絕紀錄來提高數字。這一輪另從清冊的 `body_not_found` 檢查 Global Voices 六個既有 URL，先核對實際頁面而不是盲目反覆重試。

### 六篇原生頁面與 canonical 修復

[六個實際來源的初次診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v41/globalvoices_sources_before.json)
全部 HTTP 200，但 configured native `.entry` 的有效文字為零：四篇只有圖片／caption，兩篇只有 Piktochart iframe 或 Playbuzz widget 宣告。caption、標題、作者介紹及全站 footer 都不能冒充新聞正文；沒有呼叫／執行外部 widget，也沒有猜測它現在仍可用、讀取媒體或做 OCR。

共用原有 Global Voices 的可見文章身分核對：精確 requested URL／canonical、單一原生容器、非空標題、具時區發布時間、原頁作者與文章範圍的授權線索；隱藏／模糊身分、混合正文、未知嵌入宣告仍拒絕套用特殊分類。只在沒有有效原生文字時，辨識已驗證的 image-only／embedded-only representation。一般文字、既有短正文、付費／登入／拒絕路徑維持原契約。

- 四篇標成 `source_body_image_only`，兩篇標成新增的 `source_body_embedded_only`；**都不是取得正文**。
- Global Voices HTML 契約 fingerprint 升至 **2**；RSS fingerprint 不變，沒有重抓健康 feed、重置其他來源或清掉拒絕 ledger。
- 開發時可見性核對曾誤將無作者文字、`aria-hidden=true` 的 avatar 連結當成作者證據；已先排除空 avatar，再嚴格驗證具名字的作者連結。三種 representation 都加入此回歸，不放寬真正的隱藏作者／正文檢查。
- [同六個 retained raw 的修正後證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v41/globalvoices_sources_corrected.json) 通過 SHA、receipt、policy 與實際分類核對；原失敗 replay 與 log 保留，已用此成功結果解消。

[唯一 canonical CLI 的實際重查收據](../artifacts/operations/agent-workflow/runs/v41-live-canonical-nontext-check-20261010T052009-c98ad9e6/run.json)
六次來源 HTTP 200，最短 dispatch 起始間隔 **10.0001235 秒**、峰值 inflight **1**，captured body **0**。
[逐 URL 正式佇列驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v41/globalvoices_queue_acceptance.json)
確認六筆實際檢查時間、狀態／error、current fingerprint 及沒有既有正文；不是新增六篇全文。這六筆原先已是 terminal，分類修正不等於消除了六個持續 polling loop，也不當成性能增益。

### 測試、服務與唯讀頁面

修正後 focused **229 passed / 15.50s**；包含 UI label 的
[最終全新聞回歸](../artifacts/operations/agent-workflow/runs/v41-final-all-news-with-ui-label-20261010T052010-5ece0135/run.json)
為 **1,660 passed / 44.66s**。這是新聞子系統回歸，不是整個 repository 的全套測試。

只停／恢復新聞 service 與 timer；正式新聞程序於 **13:21:55 Taipei** 啟動，PID **3889718**、owner **`ea2585d82af646a6b6e08ff39039fe81`**、NRestarts **0**。為載入新狀態 label，另重載唯讀 public dashboard gateway；永豐期貨、台股當沖及隔夜模擬三個 child PID 核對均未改變，沒有重啟交易程序。

[13:23:32 的 localhost 頁面驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v41/dashboard/acceptance.json)
在 **1440／768／390／320** 寬度通過，Global Voices 的 **4 筆 image-only／2 筆 embedded-only** 都有明確 label，provider card 的 URL head／原網址進度來自 canonical 唯讀資料。沒有 JavaScript／資源錯誤或 publisher HTTP，原文／帳號／金鑰不送往瀏覽器；不是 WAN 驗收或授權／正文完整性證明。

### 正式量測與錯誤後繼續下載

[13:22:53 baseline 至 13:28:18 的同 owner 窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v41/runtime_final.json)
通過十項工程檢查。資料庫窗口 **324.862 秒**新增鉅亨 **476 個正文 URL head／1.465 head/s**、Global Voices **23 個 head**；HTTP counter 窗口 **322.773 秒**有鉅亨 **367 次／1.137 req/s**。鉅亨其中 **283 個 head 是已保存 API response 的本機重建**，另 **185 個 retained metadata 版本補強**不計新 head。不能把 476 說成 476 篇新網路下載、唯一故事數，或拿不同 cohort 宣稱加速倍率／官方最高速。

[13:27:46 的獨立固定快照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v41/body_verification_final.json)
驗證最近 **128 批／533 個正文版本、零錯誤**，包含早於量測窗口的批次；不代替全庫、後續寫入或每一個新增 head 的逐筆驗收。收據在 runtime acceptance 時仍符合原五分鐘門檻，沒有降低新鮮度要求。

[13:27:48 全部 17 個啟用來源的 feedback 核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v41/capacity_final.json)
與當前 owner 一致：鉅亨 **14 次 502／4 次傳輸錯誤**，Global Voices **1 次傳輸錯誤**，CMoney／PeoPo 各 **1 次 403**。鉅亨當時有效間隔 **0.5 秒／2 槽**，傳輸恢復 episode 只降一次壓力；AMP 短暫 pause 沒有熔斷其他健康主機。這不是來源零錯誤或限速上限驗收。

[同 owner 主機／階段事件與恢復核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v41/transport_final.json)
通過耐久事件、權限、SHA 及 hostname 終端計數核對。鉅亨四次皆為 awaiting_headers 的 **約 25 秒 ReadTimeout**，四個失敗 session 都重建，沒有開全 provider circuit；14 次 502 分別來自 AMP **11 次**、桌面主機 **3 次**。Global Voices 一次是 **0.242 秒、未收到標頭／正文**的 `ConnectionError → ProtocolError → RemoteDisconnected`，已重建 session；不把連線中斷當成來源正文錯誤或官方限速。

[錯誤後正文繼續入庫的 bounded readback](../artifacts/operations/agent-workflow/runs/v41-corrected-post-failure-proof-20261010T053116-48d1a0ac/run.log)
在上述已驗證 128 批中，鉅亨最後一個傳輸錯誤後仍有 **33 個 URL id 的正文版本**、Global Voices 錯誤後仍有 **9 個 URL id 的正文版本**，最後 capture 分別為 **13:27:17／13:27:19**。它證明來源工作仍前進，不代表同一個失敗 URL 已重試成功或所有後續正文已驗收；舊版 row 使用 capture 時鐘，未把缺少 `acquisition_transport` 的欄位猜成網路請求。初次只讀 wrapper 對七批舊版 optional metadata 欄位不相容而失敗，修正成 schema-aware 讀取後才接受，沒有改寫原資料或追加來源請求。

### 13:35:15 的追加同 owner 驗收

[追加 runtime acceptance](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v41/runtime_handoff.json)
仍通過全部十項檢查，PID／owner 不變、NRestarts **0**。相對原 baseline 的 **741.151 秒**共新增鉅亨 **1,198 個正文 URL head／1.616 head/s**，其中 **657 個為快取重建**；**205 個 retained metadata 版本補強**另計，沒有冒充新 URL。HTTP 窗口 **737.200 秒**為 **961 次／1.304 req/s**，Global Voices 新增 **60 個 head**。當時鉅亨 controller 是 **0.5 秒／5 槽**，是有界 client 狀態，不是官方上限或達成理論最高速的證據。

[13:34:40 的新鮮正文核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v41/body_verification_handoff.json)
再次驗證最近 **128 批／478 個正文版本、零錯誤**；這是不同 sliding cohort，478 比前次 533 小並不表示刪除了正文。
[同時的耐久傳輸核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v41/transport_handoff.json)
已累計鉅亨 **9 次 awaiting_headers 傳輸錯誤／9 次失敗 session 重建／29 次隔離類 gateway 錯誤**，全 provider circuit 仍為 **0**，診斷／host checkpoint 寫入錯誤為 **0**，沒有因錯誤停止正文進度。
[全部啟用來源的追加 feedback](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v41/capacity_handoff.json)
是較早 **13:33:28** 的快照，仍只出現鉅亨、Global Voices、CMoney、PeoPo 這四個已明列錯誤的來源；不把不同時間的計數相減當成 ledger 不一致。
[六個派送 lane 的只讀索引核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v41/work_dispatch_handoff.json)
通過 partial-index seek、無 full scan／temp sort。此時暫停主機已到期，沒有 active exclusion，因此不能冒充新的 host-filter 實際派送證明；有 active pause 的正式證據仍是 v40。

原全新聞目標仍 **active**。鉅亨 AMP 的實際 502、CMoney／PeoPo 的拒絕、民視未核對權利／身分的原文、其餘授權／端點清冊及全歷史缺口仍明列；不將非文字分類、連線恢復、最近批次通過或 UI 正常當成全部來源已修好。下一步保留此唯一 owner，持續觀察故障恢復與正文進度，優先核對可修的既有 raw／metadata／合法端點缺口，不以重啟取代量測。

## 2026年10月10日鉅亨原生網址與啟動恢復修正

本階段 v42 修復三類實際解析缺口、舊失敗漏排，以及啟動恢復中的重複 SQL 查詢。正式服務繼續下載；源端 502、等候回應標頭逾時、部分原文無法驗證及存取拒絕仍存在，不能宣稱全部來源或歷史已完成。

### 實際來源回應與正文修復

[八筆問題網址的實際回應](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v42/cnyes_native_sources_before.json)
於 **13:47:41 Taipei** 均取得 HTTP 200，最小實際請求起始間隔 **0.867 秒**；原解析器其中只有三篇成功。
七筆符合當前公開身分、發布時鐘及免費宣告的回應，保存為私人 SHA 收據；`6210558` 仍無法建立此證據，只保存結構資訊。早上的兩份舊失敗收據沒有原始 HTML，不能倒推其當時一定是哪種 renderer／正文錯誤。

三項修正沿用同一 canonical extractor，沒有另建下載管線：

- **手機精確 `exp=b`**：`5255792` 的原站 canonical、Open Graph、同 ID 的 NewsArticle／current data、可見正文、時鐘、署名與表格都能互證。只新增這個實際觀測的 query；其他未知、附帶 token 或編碼變形仍不承認等價。
- **桌面精確 `exp=a` 短文**：`5238694` 是完整的 **81 字**公開原文。原通用 schema 身分比對將 query 與 queryless canonical 視為不同文章，長度門檻接著拒收；改為要求原回應的完整身分／可見正文／時鐘證據。普通桌面長文保留原快路徑。
- **原站格式字元比較**：`5415376` 的可見及 current free 正文完全相同，JSON-LD 卻省略兩個段首 U+FEFF，及一個原站清單連結 `盤中速報 - ` 後的 U+FEFF。後者還必須與唯一的 current-content 連結及文字完全一致；只改比較視圖，**413 字原文、格式字元、負值及所有數字原樣保存**。任意字內／重複字元、隱藏連結、不同數字或 href 仍拒收。

[同批私人回應的修正後重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v42/cnyes_native_replay_after.json)
驗證六篇正文及一筆真正的 `source_article_empty_template`，沒有把空模板當作正文；未保存的 `6210558` 不在此原回應核驗範圍內。

[八筆正式 canonical fetch](../artifacts/operations/agent-workflow/runs/v42-canonical-native-fetch-20261010T060302-f0b1f7c5/run.log)
首先入庫五篇；另有一筆 transport error、一筆原站空模板、一筆未驗證正文。第一筆逾時的 `5390923` 於 **14:05:31** 被正式服務自動重試並成功入庫，並非改寫失敗為成功。
[六篇目前 head 的獨立核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v42/cnyes_six_native_heads_verified.json)
於 **14:58:05** 重播當前 source，驗證 **6 個 current head／18 個所在批次正文版本**及 **3,151,063 bytes** 私人原回應，零錯誤；不是六篇以外的全庫驗收。

### 舊失敗的版本對應及恢復

[獨立 policy fingerprint 重建](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v42/cnyes_policy_fingerprint_proof.json)
確認真正的原生 v6 是 `2c69e752…`，不是更早失敗 metadata 的 `6ee14742…`；目前 v7 為 `7fd7e72c…`。
只升級 HTML contract，**API bulk fingerprint 不變**，未重新下載正常的 API 歷史或重設權限。

首輪舊版本 pin 未對上，雖有新正文入庫，仍留下 13 筆舊手機身分失敗。修正必須同時核對已保存的 v6 rollout 證據與首次 v7 retry-lane 證據，只選 v7 rollout **14:03:35** 前、已實際檢查、無正文、無 lease 的指定 parser 失敗。正常 v6 升級與漏排恢復都只執行一次。

[canonical queue 的唯讀恢復收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v42/pinned_repair_marker_verified.json)
證明 **14:17:36** 將 **13 筆**排回 `retry`，保留原 attempts／error／檢查時鐘及 URL digest。
403／429、付費、來源不存在、既有正文、其他 worker lease 及較新失敗均未重置。這只是恢復排程，不是十三篇正文已全部接受的證明。

### 啟動恢復的固定收據比較

啟動程序的實際 stack 顯示仍在讀取歷史 commit receipts；當時已有 **183,388 批**。
原程式逐張解析 JSON，並對每張收據查一次 SQLite。改為 **128 張一批、covering index** 查詢；仍逐張解析，包括已提交但損壞的 JSON。真正未提交的 receipt 照原順序重播，完整保留 **contract／Parquet SHA／row count／metadata head witness** 驗證與重播後的 idempotence。

[第一版 AB／BA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v42/receipt_recovery_ab_ba.json)
在同一個 **4,096 張**已提交收據 cohort 上反而較慢：中位數原版 **0.1956 秒**、批查版 **0.3551 秒／0.551×**。這個版本沒有部署，原失敗結果保留。

確認直屬同一 `receipts` 目錄後，避免每個檔案重算祖先相對路徑；非標準明確輸入仍保留原路徑檢查。
[父目錄快路徑比較](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v42/receipt_recovery_ab_ba_parent_fastpath.json)
及[獨立再驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v42/receipt_recovery_confirmation.json)
使用**相同 cohort SHA**，兩次 AB／BA 各四對樣本，收據步驟分別 **1.221×／1.155×**。
每個 phase 仍讀全部 4,096 張 JSON；SQLite lookup **4,096 → 32**，zero publisher HTTP／zero production DB write，收據未更動。
這是在隔離 SQLite 中的儀表化固定收據步驟，不是整個開機、實際 publisher 吞吐量或官方速率上限。

正式 owner 的 `acceptance_recovery` 記錄為 **25.27 秒 wall／12.99 秒 CPU**。
沒有相同完整 live cohort 的冷啟動對照，故不能把這個數字與前一次不同 cohort／快取條件的啟動相除，宣稱整體加速倍率。

### 回歸及正式服務量測

[最終新聞子系統回歸](../artifacts/operations/agent-workflow/runs/v42-final-news-regression-with-f-20261010T063706-57056013/run.log)
為 **1,703 passed／49.04 秒**，包括付費／未知 query／署名及時鐘衝突、格式字元與表格數值、漏排恢復證據、128 張批查、損壞收據、孤兒 SHA／rows／version、同批先恢復再遇壞收據及 readonly benchmark。
較早 focused／full run 的測試 fixture 零計數鍵及未移除 constructor marker 問題已修正；原失敗 run 保留，沒有把它們冒充測試通過。

只重載新聞 service／timer 以載入實際修正；未重啟交易服務。最終程序於 **14:42:35** 啟動，PID **4053755**、owner **`a91b5c74f9784b02baec14e127678490`**、NRestarts **0**。
[14:47:59 至 14:56:59 的相同 owner 驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v42/runtime_final.json)
通過十項工程檢查：DB 窗口 **540.073 秒**新增鉅亨 **1,614 個正文 URL head／2.988 head/s**、Global Voices **47 個 head**。
另一個 HTTP counter 窗口為 **549.504 秒**，鉅亨 **820 次／1.492 req/s**；workload counter 為 **1,137 cached-source reparse new heads／501 article-HTML new heads**。
兩種時鐘窗口不同，不能將 workload 相加當作 DB 同期總數；也不能把本機重建、網址 head、正文版本說成新網路下載或唯一故事。

[14:55:23 的近期批次核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v42/body_verification_final.json)
驗證 **128 批／562 個正文版本／55,542,490 bytes** 私人原回應，零 SHA／head 錯誤；符合 runtime 的五分鐘新鮮度限制，不代替整個歷史或每個新增 head 的逐筆核驗。
[14:56:02 的 localhost 網頁驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v42/dashboard_final/acceptance.json)
於 **1440／768／390／320** 寬度通過，原始網址進度沿用 canonical 唯讀資料；不是 WAN 或正文／授權完整性驗收。

### 持續發生的錯誤與下一階段

[14:55:23 全 17 個啟用來源的 feedback](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v42/capacity_final.json)
與當前 owner 一致：鉅亨累計 **10 次 transport error／32 次 502**，CMoney **1 次 403**；PeoPo 較早的 403／冷卻保留，沒有為顯示正常而清掉。
當時鉅亨有效間隔 **0.4 秒／4 槽**仍在自適應恢復，不是官方或已驗證的最高速。
[host／phase 恢復核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v42/transport_final.json)
保存十份失敗證據並重建十次失敗 session，zero diagnostic／checkpoint write error；**28** 次符合 host-isolation 的 gateway error、**19** 次 host circuit 開啟／延長，另仍有 **2** 次 HTTP circuit 開啟。不能說本階段完全沒有 provider 層停歇。

後續優先核對少數 gateway error 為何仍升為 provider circuit、未驗證的原生短文／身分，以及失敗回應的安全取樣範圍，再做可證實的共享修正。包括 `5419221` 的身分失敗、`6210558` 及新發現的桌面 `body_not_found`，都不是已證明「來源永遠沒有正文」，也不能直接改成完成。
原新聞目標保持 **active**；source 403／429／Retry-After／付費／robots gate、原值、原始時鐘與私人收據繼續保留。

## 2026年10月10日 v43 故障範圍與正式服務驗證

v42 後的 [15:17 feedback](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v42/capacity_handoff.json)
仍有鉅亨累計 **34 次 transport／54 次 HTTP 502**、Global Voices **4 次 transport**、CMoney **2 次 403**。
[15:19 的唯讀故障核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v43/transport_before.json)
進一步驗證 Global Voices 四筆均卡在 `awaiting_headers`，四次 failed-session reset、無 transport circuit；鉅亨當時已累計 **7 次 HTTP circuit**。
這些是前一 owner 的實際錯誤，不能因重啟後 counter 歸零而說它們已解決。

### 把故障證據與影響範圍分開

依 [RFC 9110 的 502／503／504 語意](https://www.rfc-editor.org/rfc/rfc9110.html#section-15.6.3)
及 [RFC 6585 的 429 說明](https://www.rfc-editor.org/rfc/rfc6585.html#section-4)，普通 gateway 錯誤本身不是精確速率上限的證明。
標準也不保證每個站台只用 429／503 表示壓力；因此未知錯誤仍保留在自適應壓力及原始錯誤計數中，不能藉分類宣稱來源健康或無限升速。

原程式要求其他主機在 **120 秒內**成功，否則同一壞主機的 502 重新進入全 provider circuit；「較久沒有請求另一個主機」被混成「另一個主機也失敗」。另有對稱風險：其他主機剛剛失敗，較早的成功卻仍可能充當健康證據。新共享恢復分類如下：

| scope | 已有證據 | 操作與限制 |
| --- | --- | --- |
| `host` | 同 provider 的其他主機近期成功，且沒有後續未恢復失敗／有效 host pause | 只冷卻失敗主機；原始 gateway error 仍保存，不把失敗當成功正文 |
| `provisional_host` | 其他主機曾成功但已逾 120 秒，沒有觀測到其他主機的未恢復失敗 | 暫定局部隔離；所有這類錯誤繼續計入 pressure，**不能 ramp／宣稱健康** |
| `publisher` | 沒有其他成功證據，或只有過時成功而另有未恢復失敗 | 保留原全 provider breaker；不假設其他端點可用 |

後續真實成功才清除該 host 的 failure streak；單純 timer 到期／重啟不會把失敗變成功。
transport timeout 也會使較早的 host 成功不再可作 corroboration，**route／TLS 及連續 transport breaker 語意未放寬**。
仍使用同一 provider 的總流量桶與 worker 上限；局部隔離不是新增獨立額度、切換身分或繞過拒絕。

局部 pause 維持 **30 秒／三次 failure threshold**，沿用原 indexed dispatch 的 before-LIMIT host filter，各 lane 的 URL、attempts、時鐘及正文 head 不改。
`gateway_host_isolation_contract_version` 升為 **2**，舊 v1 相同資料指紋的未到期 pause 可讀回；`measurement_contract_version` 升為 **9**，不沿用不相容的升速證據。

### 明示拒絕與耐久診斷

403／429／503、route／TLS 與 Retry-After 保護保留；**空白或無法解析的 Retry-After 也不當作未提供來局部化**。
另依 [Cloudflare 官方 challenge 識別文件](https://developers.cloudflare.com/cloudflare-challenges/challenge-types/challenge-pages/detect-response/)
將明示 `cf-mitigated: challenge` 優先視為存取拒絕，含 HTTP 200／redirect／502；429 同時保留學到的較慢 pacing。
不讀取／解題 challenge body，不改 User-Agent、IP、cookies 或登入身分。

同一張私人 `access_problems` receipt 升為 v2，增加 owner 與 gateway decision：scope、basis、其他成功的 age、未恢復 host、failure count、pause deadline、source／authorization fingerprint。
沒有增加正常成功請求的診斷 I/O，也沒有另一個 collector／queue writer。
唯讀 auditor 驗證 exact fields、SHA、權限、當前 owner、來源／授權指紋及分類證據；**即使 SHA 合法，多出 body 或空證據卻宣稱 host 健康仍拒收**。
輸出不含原始 URL／query、headers、exception message 或正文。

### 回歸、載入及真正入庫

[凍結版本回歸](../artifacts/operations/agent-workflow/runs/v43-frozen-news-regression-20261010T073824-1fd2fc39/run.log)
為 **1,723 passed／48.33 秒**，包括 unknown／stale／共同失敗、較後 timeout 使舊成功失效、v1 pause 恢復、provisional 不能升速、空／無效 Retry-After、challenge、合法 SHA 的錯誤隱私／健康證據及唯讀 owner 隔離。
早先不存在的 focused test 路徑、fixture 對不存在 attribute 的直接取值及舊 measurement 8 斷言失敗均保留，修正後才做本次正式載入。

只 [重載新聞 owner](../artifacts/operations/agent-workflow/runs/v43-load-reviewed-fault-scope-20261010T073928-5c4bf9a7/run.json)，沒有動交易服務；timer 保持 active。
新 PID **4170501** 於 **15:39:33** 啟動，owner **`6d1d71ff657248ebb723020becf56205`**、NRestarts **0**。
啟動 receipt recovery 本次為 **53.19 秒 wall／26.64 秒 CPU**；不是 v42 的匹配 cohort／冷快取對照，**不能由 v42 的固定收據 15–22% 改善推論本次完整開機更快**。

[15:41:42 baseline 至 15:47:11 正式驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v43/runtime_final.json)
通過十項工程檢查：相同 owner、持續正文、目前 policy／source code、來源 pacing 與新鮮獨立收據均一致。
DB 窗口 **328.989 秒**新增鉅亨 **1,075 個 URL head／3.268 head/s**，Global Voices **30 個 head**。
HTTP counter 窗口 **322.696 秒**，鉅亨 **431 次／1.336 req/s**，work counter 是 **778 cached-source reparse／270 article-HTML new heads**。
它們不是同一窗口的互斥分割，不能把 1,075 都說成新網路下載或唯一故事，也不能將這個不同 cohort 與 v42 相除宣稱整鏈路加速倍數。

[15:45:11 的近期正文核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v43/body_verification_final.json)
驗證 **128 批／638 個正文版本／38,932,475 bytes** 私人原回應，零錯誤；不代替全歷史或每一個本窗口新 head 的核驗。
[網頁讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v43/dashboard_final/acceptance.json)
在 **1440／768／390／320** 通過原始 URL 進度與所選來源呈現，範圍仍是 localhost，非 WAN／授權驗收。

### 仍存在的來源錯誤及續作邊界

[15:45:09 的全 17 啟用來源 feedback](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v43/capacity_final.json)
與當前 owner 一致；[同時的故障收據核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v43/transport_final.json)
驗證鉅亨 **13 次 gateway decision**，全部有其他近期成功主機作 corroboration，採 `host` scope；**本實測未出現 provisional scope**，該分支目前只有工程回歸證據。
四次 headers timeout 的私人收據與四次 failed-session reset 均保留；這個窗口 **zero diagnostic／checkpoint write error、zero provider HTTP／transport circuit**。
這是有界恢復證據，不是「上游 502／逾時已消失」或「以後不會全 provider 暫停」。

當時 client 為 **0.5 秒／3 槽**，仍在 recovering transport，並非官方或全歷史實證上限。
CMoney／PeoPo 的舊拒絕與 baseline cooldown 保留，不能把新 owner 尚無 403 誤當修好；沒有 ready work／未啟用的來源也未標成全部成功。
原生身分／桌面正文未驗證項目、來源授權、全部端點清冊與完整歷史仍在原目標續作；新聞 goal **active**，正式服務繼續下載。

最後 [15:55:44 全來源讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v43/capacity_handoff.json)
及 [15:55:43 耐久故障核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v43/transport_handoff.json)
仍為同一 owner：鉅亨累計 **34 次 502／19 次 transport**，34 張 gateway decision 均為有近期其他主機成功證據的 `host` scope，19 份 transport failure 與19 次 session reset 均保留，尚無 provider HTTP／transport circuit 或診斷寫入錯誤。
這個更長窗口仍未實際觸發 provisional，不能把單元測試說成其線上實證。
CMoney／PeoPo 各新增 **1 次 403**並回到 `waiting_source`；不是靜默丟棄或已完成。當時鉅亨為 **0.5 秒／4 槽**；Global Voices 在本 owner 尚無 transport error，但這不抹去上一 owner 的四筆錯誤。

## 鉅亨標題修復與全來源錯誤核對

鉅亨仍有來源 502、連線逾時與少數解析錯誤，不能只靠增加併發或重啟清零判定解決。
有效取得速度必須分開看派送頻率、每次請求的有效正文、已保存原回應重放，以及實際新增的正文 URL head。
本輪只處理新聞服務，保留所有原始值、發布時鐘、拒絕與重試證據，未改動其他下載器或交易服務。

### 固定清冊及已自動補回的正文

[16:04:27 的全來源固定 WAL 清點](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v44/provider_inventory.json)
包含 **59 個設定 provider、17 個啟用 provider、683 個未註冊來源鍵、6,201,712 個已知 URL、3,525,230 個已有正文 head**；
仍有 **2,130,263 個待授權 URL**。來源鍵包括不同 hostname、聚合／轉址及非新聞資料，不能當成唯一新聞社數目；上述數值是本次固定快照，不是全歷史總量。
完整逐來源、端點資料仍在[CSV 清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v44/provider_inventory.csv)
及[端點 CSV](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v44/provider_inventory.endpoints.csv)。
這次全清點花 **33.77 秒 wall／26.26 秒 CPU**，只作一次診斷，不加入每次網頁輪詢或文章派送。

先前尚未核驗的桌面 `6210558、6163556、6146926、6137716、6136131、6135216`，
已由原 owner 的官方批次端點自動補回。
[六篇固定 head 核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v44/recovered_native_heads.json)
通過原始 JSON 重放、正文 SHA、標題、作者、發布時鐘、不可變批次及當前捕捉契約，零錯誤；
正文長度分別為 **88、92、87、46、45、43 字元**。這六篇不是本輪另外發六次文章請求取得，也不代表其他歷史缺口已補齊。

### 原生標題邊界空白及確實的來源空模板

安全排空原服務後，沿用 canonical HTTP 做
[四篇有界原入口診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v44/cnyes_failed_four.json)。
`m/5419221` 的公開 article ID、免費宣告、正文範圍與發布時鐘一致，但標題比對失敗；
沒有先保存尚未通過公開身分範圍的整份 HTML。
[只保存雜湊與碼位的標題核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v44/cnyes_title_comparison.json)
證明宣告標題為 31 字元、可見 `h1.get_text(strip=True)` 為 30 字元，唯一差別是末尾 **U+00A0 不換行空白**。

共用原生 parser 現在只對宣告標題採相同的外側空白比較；
保留原始標題、內部空白、隱藏控制字元、原正文、JSON-LD／current 完整宣告及發布時鐘。
不使用 NFKC、全字串空白刪除、猜測標題或換來源補身分，標準 desktop 長正文路徑不變。
HTML native contract **7 升為 8**，實際 fingerprint 為
`067ea836cf4f4b858bb0c225f68aae054c66eb21c3e652978ebc8eb6e039ec2d`；官方批次端點 fingerprint 不變。
只將 pinned v7、無正文／lease 的原生身分解析失敗重排一次；**12 個重排不是 12 篇已取得正文**。
舊 attempts、error、最後實查時鐘及拒絕／既有 head 保留，未清空 403、429 或來源不存在。

[正式原入口取得](../artifacts/operations/agent-workflow/runs/v44-native-fixed-canonical-fetch-20261010T081619-2b61f865/run.json)
對四個明確 URL 實查：**2 篇入庫、2 篇來源空模板**。
`5419221` 是本輪實際修好的 **372 字元**正文；`6625483` 再次取得 **104 字元**正文，先前狀態不等於永久缺失。
[兩篇獨立入庫核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v44/fixed_native_heads.json)
在 **16:18:43** 通過原回應重放與不可變收據，保留當前 HTML contract。
`6166703` 與 `5562672` 的 visible／current／schema 三個版本均為字面 `null`；
正式 queue 記為 `source_article_empty_template`，不算正文、不捏造歷史內容，也不推定來源永遠不能修復。

### 全來源拒絕核對及測試

全來源 feedback 與私人 transport／gateway 收據均使用同一 owner 判定，不把上一 owner 的錯誤歸到新 owner，
也不把新 owner 尚無錯誤當成修好。
[16:20 的全 17 啟用來源讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v44/capacity_baseline.json)
仍有鉅亨 **3 次 502**，CMoney／PeoPo 各有新 **1 次 403**，兩者仍在 `waiting_source`；
[同時的私人故障收據核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v44/transport_baseline.json)通過，拒絕 body 未讀取、未保存。
保留來源冷卻、robots、Retry-After 與共同 provider 節流；其他來源按各自排程續抓。

[CMoney 官方串接說明](https://app.cmoney.tw/official/product/valued-data)
列有授權 HTTP／批次服務；[GetStockRssNews 官方介面](https://www.cmoney.tw/cm-service/CMService.asmx?op=GetStockRssNews)
仍要求 commkey／memberPk，提供的是 RSS 中繼資料，不能充作新聞全文的已驗證替代。
[PeoPo 使用規範](https://www.peopo.org/about/regulation/P1-1.htm)
仍由作者逐篇選擇 CC 授權，不是全平台無條件重用授權。
本輪核對文件並保留當前實際 403，未換身份／IP、繞登入或猜測受拒絕來源的替代端點。

[修正後完整新聞回歸](../artifacts/operations/agent-workflow/runs/v44-frozen-news-regression-20261010T081620-b7b5cdce/run.json)
為 **1,742 passed／61.15 秒**；範圍為 `test/test_news*.py` 與 `test/test_cnyes_news_bulk.py`，不是全 repo 測試。
覆蓋外側空白、禁止改動內部空白／U+200B／U+FEFF／其他文字、來源時鐘／署名、原拒絕保護及重排只執行一次。
193 項聚焦測試與此全新聞回歸範圍重疊，不相加。

原正式 service 已於 **16:18:40 Taipei** 恢復，PID **59047**、owner **`56b156f0ff8643fc96911bf720a4d703`**，timer active、NRestarts 0。
一筆診斷 CLI owner 初始化花 **80.03 秒 wall／49.27 秒 CPU**，其中收據恢復 **46.82 秒 wall**、policy reconciliation **4.82 秒 wall**；
這包含 parser 升級、不同快取與同時回歸負載，不與前一輪直接相除宣稱提速，也不是穩態逐篇下載耗時。
目標維持 active，來源錯誤、尚未授權範圍及全歷史完成度仍在原清冊續作，不因上述成功核驗改成全部完成。

### 近期核驗的歷史版本誤判修復

續抓中，[最初的近期核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v44/recent_batches.json)
報 `benchmark_queue_head_mismatch`，原失敗保留，不以換一批檔案或降低 freshness 門檻消除。
[相同 128 個批次的只讀版本診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v44/historical_head_diagnosis.json)
包含 **1,384 個正文版本、1,383 個文章 ID**；兩個舊版本已被合法新版本取代，沒有缺失 head。
一個新 head 位於這批檔案內，另一個位於後續已提交批次；歷史版本不能都被要求仍是當前 URL head。

近期核驗 contract **2** 保留每個原始版本的 Parquet／receipt SHA、行數、原回應／正文 SHA 及 canonical-resolution 證據；
對這些版本的 article ID，所有指向核驗 cohort 的當前 head 都必須有相符正文見證。
指向外部 cohort 的 head 仍需存在並參照安全、已提交的批次，但其正文及其他 queue ID 不算已核驗。
缺失 head、未提交／越界指標、損毀的舊正文／raw，以及核驗 cohort 內的錯誤當前 SHA，均仍拒絕。
原 benchmark 預設的 all-rows-current 契約不變；版本模式需明確啟用，沒有改寫來源資料或正式下載器。
SQL 每個獨立 ID 只查一次，不為有界核驗增加全湖掃描。

[原失敗固定清單重驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v44/recent_batches_fixed_cohort.json)
通過 **128 批、1,384 個全部版本、1,382 個 cohort 內當前 head 見證、零錯誤**；
外部 cohort 的一個 head 正文不計入核驗。保留原失敗報告的 SHA，批次清單完全相同；在新的固定只讀 WAL snapshot 核驗，不宣稱重建舊 WAL generation。
`--batch-snapshot` 可重驗原報告的同一個有界檔案清單，不丟掉舊版本。

[版本修復聚焦測試](../artifacts/operations/agent-workflow/runs/v44-version-aware-receipt-focuse-20261010T083508-524f1003/run.json)
為 **36 passed／2.50 秒**；第一次命令引用不存在的 runtime 測試檔，exit 4、零測試，原 log 保留，更正後未觸碰來源。
[最終完整新聞回歸](../artifacts/operations/agent-workflow/runs/v44-validation-contract-news-reg-20261010T083627-10d71e46/run.json)
為 **1,755 passed／58.61 秒**。這是前述 1,742 項修正後追加版本核驗案例的完整回歸，不與聚焦測試相加。
這些只有驗證工具／報告 contract 的變更，正式 source code 未更動，**不再為此重啟 producer**。

### 正式續抓進度及仍存在的錯誤

[16:37:19 正式續抓驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v44/runtime_final.json)
十項工程檢查全部通過，仍是同一個 PID／owner。
資料庫窗口 **1,027.629 秒**新增鉅亨 **1,067 個正文 URL head、1.038 head/s**，Global Voices **99 個 head**；
HTTP counter 窗口 **1,026.105 秒**的鉅亨請求率為 **1.671 req/s**。
URL head 包含不同網址及已保存原回應重放，不等於唯一故事或全部新網路下載；這個不同工作窗口不與先前 3.267 head/s 相除當作性能回退或提速倍數。
相同 HTTP counter 窗口的 work counter 為 **1,036 個 article-HTML new head、13 個 cached-source new head、4 個 API new head**；
其 counter 時鐘與資料庫窗口不同，不能把三者相加當成 1,067 的互斥分割。

[16:36:28 近期原回應核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v44/recent_batches_final.json)
通過 **128 批、2,176 個正文版本、389,513,642 bytes 私人原回應、2,174 個當前 head 見證、零錯誤**。
所有 2,176 版本均驗證，兩個被後續版本取代的原版本保留，不算新 head；這不是全歷史或所有新 head 的核驗。
[原進度頁讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v44/dashboard/acceptance.json)
在 **1440、768、390、320** 寬度通過所選來源及原始 URL 進度，仍限 localhost，不是 WAN 或來源正文授權驗收。

[16:37:13 全 17 啟用來源讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v44/capacity_handoff.json)
保留本 owner 的鉅亨 **38 次 502、14 次 transport error**；CMoney／PeoPo 各 **2 次 403**，仍是 `waiting_source`。
[16:37:21 耐久故障核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v44/transport_handoff.json)
通過：38 張 gateway 決策都是有其他近期健康主機作證的 host 範圍；14 份 transport error 與 14 次失敗 session reset 相符。
此窗口無 provider HTTP／transport circuit、無故障診斷／host checkpoint 寫入錯誤；不等於上游錯誤消失，也不是 provisional 分支的線上驗收。

自適應排程曾有界試到 **0.2 秒／5 槽**，遇連線壓力後回復；16:37 時是 **0.5 秒／3 槽**，不是宣稱官方最高速。
下階段優先檢查 gateway 冷卻到期的重試派送是否造成多個失敗請求同時佔槽，再核對其餘原生解析與 provider 存取缺口；
保留上限、拒絕／Retry-After 證據與其餘來源獨立續抓，不以縮小正文範圍、偽造空內容或忽略錯誤換取速率。

## 鉅亨耗時分解與狀態統計優化

2026-10-10 17:11，Asia/Taipei。鉅亨與 Global Voices 已在同一新 owner 持續增加正文；
本次修正每 15 秒產生面板狀態時的重複 SQL 掃描，沒有更改來源、正文、發布時鐘、授權、重試上限或配額。
上游 502／連線逾時仍存在；**完整狀態生成較快，不等於所有新聞取得較快相同比例，也不等於所有來源已完成。**

### 全來源清冊與故障耗時

[17:03 的一致清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/provider_inventory.json)
包含 **59 個設定來源、17 個啟用來源、683 個未註冊來源鍵、102 個已登錄端點**；
**6,201,721 個已知 URL、3,528,810 個已存正文 URL 紀錄、2,130,263 個待授權 URL**。
這是來源 URL／有限 cohort 清冊，不是獨立文章去重數、全部歷史或全部媒體的清冊。
[逐來源 CSV](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/provider_inventory.csv)及
[端點 CSV](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/provider_inventory.endpoints.csv)
保留其餘來源的筆數、metadata 首末日、錯誤、排程、授權與下一步，不只檢查鉅亨。
這次一次性只讀清冊耗 41.87 wall 秒／32.48 CPU 秒，不放入面板輪詢。

[16:53 全啟用來源 HTTP 核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/transport_baseline.json)
驗證當時 owner 的 **71 次鉅亨 502、22 次 transport error**；另有 Global Voices 一次逾時。
CMoney 3 次及 PeoPo 2 次 403 留在
[相同 owner 的容量證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/capacity_baseline.json)，不是正文成功。

| 鉅亨主機與觀察 | 數量 | 累計佔用工作槽時間 | 判讀 |
| --- | ---: | ---: | --- |
| AMP 的 502 | 62 | 31.64 秒 | 其他同 provider 主機仍成功，既有 host 冷卻隔離有效 |
| 正文主機等待 headers 逾時 | 20 | 502.83 秒 | 本機 HTTP wrapper 尚未收到 headers；不能因此判定 DNS、TLS 或來源伺服器是哪個根因 |
| 行動主機等待 headers 逾時 | 2 | 50.24 秒 | 失敗 session 已重建，工作依既有退避重排 |

上表是平行呼叫的加總時間，不是可直接從總 wall time 扣除的時間。
62 次 AMP 502 的 60 次 pause-open 決策，相鄰觀察間隔 **最短 30.42 秒、中位 31.67 秒**，
沒有小於 30 秒的相鄰 pause-open 觀察。此窗口不支持「冷卻到期大量重試同時佔槽」是主要瓶頸；
沒有為這個未成立的假設新增半開閘門，也不把此結果說成已證明所有時候都不會發生。
HTTP 502 的標準語義是 gateway／proxy 收到無效上游回應，不直接證明官方速率上限；
來源的 Retry-After 與拒絕依原政策保留。[HTTP 官方規範](https://www.rfc-editor.org/rfc/rfc9110.html#section-15.6.3)

### 同一狀態輸出的精確單掃描

[本機完整狀態 profile](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/status_profile_baseline.json)
及[SQL 耗時分解](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/status_query_profile.json)
顯示狀態生成與原樣 JSON 序列化每次約 **1.18 CPU 秒**；五個 discovery 統計反覆掃描同一批寬列。
`summarize_discovery_status` 將當前待抓、當前錯誤、被拆分取代、封存錯誤和所有就緒 bulk 頁合併為
一次精確聚合，due 統計共用一個時間點。NULL、空字串及空 JSON 清單仍遵循原來的封存判定；
沒有暫存過期數量、隱藏封存錯誤、調低面板更新頻率、增加索引或修改來源列。
面板多了 `discovery_status_projection_contract_version: 1`；來源／正文 fingerprint 不變。

[最終同 WAL 完整狀態 AB／BA 七次比較](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/status_paired_projection_verified.json)：

| 完整狀態生成與相同 JSON 序列化 | 舊五次掃描 | 新單次掃描 |
| --- | ---: | ---: |
| wall 中位 | 1.1562 秒 | 0.8500 秒 |
| CPU 中位 | 1.1561 秒 | 0.8494 秒 |
| 狀態輸出大小 | 1,613,655 bytes | 1,613,655 bytes |
| 完整輸出 SHA | `96fb4bca…605e4419e` | 相同 |

完整狀態階段約 **1.36×、少 26.5% CPU 時間**。固定 SQLite generation、metrics、capacity 和時鐘，
所有欄位逐 byte 一致；不是只比較一個 SQL、換小清冊或刪進度欄位。
profile 使用明確 process CPU 時鐘且不納入計時中位；最初 profile 的函式分解使用預設 profiler 時鐘，
以這份最終明確 CPU 時鐘證據為準。讀回重播不含正式 metrics lock 或檔案發布延遲，不能冒充整體服務 CPU 改善比例。
原始測量及較早五次／七次比較保留，沒有以最快單次取代完整結果。

新增三項測試覆蓋空佇列、due 邊界、所有 error／封存 literal 組合、更新／刪除／rollback、
固定 owner 與零 HTTP／零來源寫入、舊 owner 拒絕及有界重播。
[新聞回歸](../artifacts/operations/agent-workflow/runs/v45-frozen-news-regression-20261010T090331-e257c42e/run.log)
為 **1,758 passed／58.85 秒**；之後明確 profiler CPU 時鐘修正的
[專項測試](../artifacts/operations/agent-workflow/runs/v45-final-status-replay-tests-20261010T090907-dd4a8ab1/run.log)
為 **3 passed／2.47 秒**，不將重複測試相加，也沒有宣稱整個 repository test suite 通過。

### 正式續抓與尚存的來源問題

[正式服務載入收據](../artifacts/operations/agent-workflow/runs/v45-deploy-shared-status-project-20261010T090522-22885402/run.log)
先排空原新聞 owner，再於 **17:05:36** 啟動 **PID 147139**、owner
`83497f8f3a5c468ca636cbc01e8bdc95`，timer active、NRestarts 0。
沒有另開下載器、清理資料、重設來源拒絕／限速或修改其他服務。

[17:07:44 起點](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/runtime_post_deploy_baseline.json)至
[17:11:50 工程驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/runtime_final.json)
同 owner 的 **246.68 秒 DB 窗口**：鉅亨 **+530 個有正文 URL head，2.149 head／秒**，
Global Voices **+21 個**。10 項工程檢查通過，包含新程序／owner／policy／code 一致、
基線就緒健康來源獨立增加正文、原共享派送時鐘及新鮮正文收據。
CMoney 等基線仍在冷卻或未就緒的工作保持缺口，不因排除其窗口進度要求便算完成。
HTTP counter 窗口為 **241.77 秒、鉅亨 739 次派送／3.057 req／秒**；
此計數與 DB 的採樣時鐘不同，也不能用派送數替代取得正文數。
當時為 **0.2 秒／7 槽**，保留 transport recovery；不是官方或長期穩定最高速，也不是與 v44 不同 cohort 的因果加速比較。

[128 批正文耐久核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/recent_batches_final.json)
通過 **4,509 個版本、816,725,946 bytes 原始回應、零錯誤**；
核驗各版本／receipt／原回應／正文 SHA 與該 cohort 的 head 見證，沒有宣稱核驗了全湖、未來寫入或每篇來源語意。
[既有進度頁讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/dashboard/acceptance.json)
通過 **1440、768、390、320** 四個寬度與所選來源／原始 URL 進度，仍限 localhost；本次沒有 UI 重設計或 WAN 驗收。

[17:09 全啟用來源容量核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/capacity_handoff.json)與
[相同 owner 的故障核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/transport_handoff.json)
保留 **9 次鉅亨 502、8 次 transport error／8 次 session reset**；有界原始故障收據、host 範圍與 terminal 計數相符。
這不是上游故障已消失。重啟後的零 403 計數也不消除舊 owner 或持久佇列的 CMoney／PeoPo 拒絕；
尊重已有 deadline，健康來源續抓，沒有對 403 改換身分、繞過或把空內容當正文。

[中央社本機授權接線](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/cna_setup.json)
沿用使用者已確認的授權，但 `CNA_API_KEY` 仍未提供，沒有呼叫 HTTP 或文章工具。
已提示填入 `.env` 既有 slot，或提供另一個已授權 API 的文件及變數名稱；不重問是否已有授權，也不索取聊天中的金鑰值。
17 個啟用來源其餘的 image-only、來源撤稿、內容／作者身分未核實、轉載及授權範圍問題仍逐列保留。
下一階段按原全來源目標核對鉅亨健康 canonical 入口能否在有來源證據下減少失敗 AMP／重查請求，
再修有實際原文證據的 parser 缺口；不先假定別名必然可互換，不刪獨有版本或原失敗紀錄。

### 故障紀錄與進度取樣時鐘

[17:18 容量交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/capacity_final_handoff.json)及
[原故障交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/transport_final_handoff.json)
保留當時的差異：較早 counter generation 有 **26 次 isolated gateway error**，
舊 auditor 卻用較晚的 status 發布時間納入 **27 份 gateway decision**。
同窗口有 **18 次 transport error／18 次 session reset**；鉅亨已退回 **0.5 秒／2 槽**，
所以前面 0.2 秒／7 槽及 2.149 head／秒只代表其明列驗收窗口，不代表最新或持續最高速度。

根因是 `NewsQueue.status()` 先取得 metrics，再執行 SQL／組裝 status，最後記錄 `updated_at_utc`。
HTTP worker 可能在兩者之間新增故障；以發布時間切故障收據，就會混用不同計數世代。
新增 **metrics snapshot clock contract 1**：在原 metrics lock 內同時複製 counter、
取得 `snapshot_at_utc` 與 owner elapsed／CPU clock；沒有新增 thread、HTTP 或另一個 queue writer。
故障 auditor **contract 3** 改用此 cutoff，仍以發布時間檢查 heartbeat 新鮮度。
runtime auditor **contract 4** 用相同 counter clock 計算 HTTP 分母及 owner epoch，
並在工程驗收中要求已核實的新時鐘；舊證據明列 unverified，不偽裝成新格式。
有界故障樣本、尚未 terminal 的請求及累積 counter 粒度仍不同，不能強迫三者一一相等或刪除差異。

新增回歸重現「取樣後、發布前」的真實次序：舊 generation 排除該筆，
下一代 counter 及故障收據都包含它；另驗證鎖內複製、延遲發布不能使舊 owner 通過、
未知／布林／空值版本、缺欄位、無時區、非 UTC 及未來取樣時鐘拒絕。
[完整新聞回歸](../artifacts/operations/agent-workflow/runs/v45-locked-metrics-clock-full-ne-20261010T093116-f60c50ee/run.log)
為 **1,773 passed／54.24 秒**，與前面結果不重複相加。
[新時鐘正式載入](../artifacts/operations/agent-workflow/runs/v45-deploy-locked-metrics-clock-20261010T093250-774d8332/run.log)
先確認舊新聞程序 inactive／MainPID 0，再於 **17:32:54** 啟動 **PID 207174**、timer waiting。
此收據只證明排空與啟動；同一新 owner 的正文增量、錯誤觀測及耐久收據另行驗收。

[17:34:57 新起點](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/runtime_clock_baseline.json)至
[17:37:38 新時鐘工程驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/runtime_clock_final.json)
為同一 owner `d8ed114021474c19a5a2d08e50157df7`；11 項工程檢查通過。
**160.82 秒 DB 窗口，鉅亨 +60 個正文 URL head、Global Voices +18 個**；
**153.47 秒 counter 窗口，鉅亨 0.814 次派送／秒**。
新窗口鉅亨 **0.373 head／秒**，低於先前窗口，不能將狀態 SQL 的 CPU 改善當成取得正文的因果加速。
來源／cohort／回應延遲與 recovery pressure 不同；沒有移除驗證或強行升速以掩飾較慢結果。
[128 批新鮮正文收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/recent_batches_clock_final.json)
核驗 **4,813 個 immutable 版本、零錯誤**，並綁定新 owner；仍非全歷史或全來源語意驗收。

[新時鐘的 17 來源故障核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/transport_clock_final.json)
通過；取樣時間 **17:37:25.546**、發布時間 **17:37:26.106**。
兩者之後新增的 **2 份 access diagnostic** 正確排除於舊 counter generation，原檔未刪除。
窗口內保留 **8 份鉅亨 gateway decision**：5 份有當時另一健康 host 的證據，3 份冷啟動時無此證據，
仍走 publisher 保護；另有 **1 次 transport error／1 次失敗 session reset**。
host terminal counter 對帳通過，沒有診斷寫入錯誤。
[相同狀態的容量交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/capacity_clock_final.json)
為 **0.5 秒／2 槽、recovering_transport**，仍有 **CMoney 及 PeoPo 各 1 次新 403**。
故障時計修好不等於上游故障消失，也不等於其餘 provider 全部取得正文。

[有界原文優先派送核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v45/cnyes_dispatch_clock.json)
顯示新 owner 的 64 次 HTML 派送中，58 次優先沒有已知正文候選者，6 次核對既有候選；
六個 pending／retry／retry_cooldown lane 都使用索引，查找約 **0.064–0.104 ms**，沒有 full scan／sort。
這排除目前候選查找是主要瓶頸的說法，但數字 ID 與候選正文不等同來源版本可合併。
下一階段保留 warm owner，按來源證據核對失敗 canonical／AMP 原文入口與 provider parser 缺口；
不因觀測 error 而重啟全部服務，不把 403／權限／撤稿誤列成已修好。

[17:44 既有進度 API 讀回](../artifacts/operations/agent-workflow/runs/v45-locked-clock-public-projecti-20261010T094433-da2586d8/run.log)
核對 localhost 的 `/news/api/status`：唯讀、running、fresh、snapshot consistent、
有已儲存正文及目前 owner 的新增鉅亨 head，且沒有虛構全歷史 ETA。
這是資料／既有網頁接線讀回，不新增 UI 功能、不重做前面四寬度測試，也不宣稱 WAN 驗收。

## 鉅亨 AMP 工作分工與發布時鐘追新

2026-10-10 18:29 重載前，同一新聞 owner 累計出現 **122 次鉅亨 502、56 次連線錯誤**，
CMoney 與 PeoPo 各有 **4 次 403**。這是該 owner 的累計，不是每秒錯誤率。
[17 來源傳輸核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v46/transport_pre_deploy.json)
通過，取樣時計正確且原故障證據保留；
[容量交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v46/capacity_pre_deploy.json)
沒有鉅亨 429，因此不能把 gateway／連線故障解讀成已測得的官方 RPS 限額。

### AMP 與一般新聞維持獨立觀測

[固定唯讀清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v46/cnyes_amp_candidate_baseline.json)
包含全部 **187 個當時 pending／retry／retry_cooldown AMP URL**，未被 256 筆界限截斷。
其中 **185 個同 ID 的一般新聞 URL 已有正文 head**，另外 **2 個一般新聞 URL 尚未註冊**。
同 ID 是查詢候選提示，不是兩個 URL 的正文、發布時間或版本相等證據。

[8 個一般新聞 head 的原始來源回放](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v46/cnyes_amp_canonical_head_proof.json)
通過，但 [原始來源宣告核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v46/cnyes_amp_declared_alternates.json)
沒有取得可支持 AMP 版本互換的連結。因此不複製正文、不把 AMP 改成 complete、不合併其日期或來源版本。
這 8 個樣本不是全部 185 個的語意驗收。

[共用工作分工](../downloader/news_fulltext_cnyes_work.py)升至版本 3。
已知一般新聞 head 的 AMP URL，和既有 mobile 候選一樣，走保留服務機會的次要查核輪次；
真正沒有已知正文候選的工作仍優先。全部 AMP 的原錯誤、日期、期限、租約和正文狀態保留。
維護最多每次 256 列／40 ms，以 host／state 索引和各自耐久游標續做，閒置時不反覆寫 checkpoint。

缺少一般網址時，可在既有公開 `news.cnyes.com/news/id/ID` 命名空間註冊**獨立未驗證查詢**。
只接受精確 HTTPS AMP 路徑，不擴張 query、其他 host、來源權限或有限 cohort；
不從 403、429、付費／登入拒絕或下載中的 parent 建立替代工作。
原網址已有任何狀態都不重排；新網址不繼承 parent 的標題、日期、正文、capture clock。
查詢與 `cnyes_native_lookup_candidates` 私有來源提示 journal 同一交易提交，並保留當時資料及授權 fingerprint。
實際 HTTP 仍由唯一 owner 經原 robots、publisher bucket、cooldown、解析和 immutable receipt 流程取得。

### 無清單日期仍可追新

[接受收據更新](../downloader/news_fulltext_queue.py)新增發布日排序契約 1。
只有**已接受正文**具有 timezone-aware ISO 或 RFC 發布時間，且原排序日為空時，才以該來源時間的 UTC 日補排序提示。
原 `metadata_date_raw`、正文發布時間和 capture clock 不變；既有非空 metadata 排序日不覆寫。
舊文章不因今天下載而變成今天發布，只有日期或 naive／未知 clock 也不推測時區。
這修復無清單日期的新增原文在成功後無法進入近期複查的缺口，不建立回溯發布證據。

容量量測契約升至 **10**，把工作分工與追新排序契約納入 fingerprint；
不把舊工作配置的 throughput 校準冒充新配置實测，也不重設持久的拒絕／Retry-After 限制。
[候選來源測試](../test/test_news_cnyes_native_lookup.py)、
[發布時間複查測試](../test/test_news_source_publication_rechecks.py)涵蓋原始證據、獨立取得、拒絕、租約、
有限 scope、回滾、cursor 續做和未知時計。
[最終全新聞回歸](../artifacts/operations/agent-workflow/runs/v46-final-full-news-regression-20261010T102816-b20dd827/run.log)
通過 **1,825 項／47.47 秒**；這是工程測試，不是所有來源或所有歷史已抓完。

### 重載後的實際下載驗證

18:29:45 正常重載唯一新聞服務，PID **325554**，新 owner
`99dc2b1d50f545d882c938914cfbd7d0`；沒有重啟交易、訓練或其他抓取服務。
18:31:44 的新快照確認工作契約 **3**、獨立查詢契約 **1**、發布日排序契約 **1**、量測契約 **10**
已載入且新增 **2 個獨立一般網址**。初始化期間的舊 stopped 快照不當作新服務失敗。

[獨立來源驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v46/native_lookup_source_acceptance.json)
確認兩個 lookup 中 **1 個已取得正文並通過原始來源、作者、發布時鐘與 immutable 收據回放**，
另 **1 個保留 `source_not_found`**，不是偽造 complete。
兩個 native row 的原 metadata 日期仍為空，兩個 AMP parent 仍沒有正文；一般新聞成功沒有覆寫 AMP 版本。
獨立查詢能修復遺漏的原文工作，不能保證來源不存在的內容重新出現。

[同 owner 持續進度](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v46/runtime_final.json)
通過 11 項工程核對，包括 fresh 時計、實際 source-code 載入、來源 fingerprint、間隔規則與正文進度：

| 來源 | 固定資料庫窗口 187.81 秒的新增正文 URL head | 窗口內 head／秒 |
| --- | ---: | ---: |
| 鉅亨 | 425 | 2.263 |
| Global Voices | 20 | 0.106 |

HTTP counter 窗口獨立為 **182.42 秒**；鉅亨實際 **2.544 次派送／秒**，不是設定 cadence 的倒數。
這個新窗口比前一階段的 0.373 head／秒高，但來源 cohort、回應和槽數不同，
**不能宣稱是修正造成 6 倍加速、官方上限或全歷史 ETA**。
[最新 128 批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v46/recent_batches_final.json)
核驗 **3,645 個 immutable 正文版本、664,131,919 bytes 原始回應、零錯誤**，綁定同一 owner。

[工作分工讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v46/cnyes_dispatch_live.json)
確認 loaded 契約相符；所有六個 ready lane 維持索引 seek，沒有全表掃描或排序。
[17 來源故障核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v46/transport_final.json)
和 [容量回饋](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v46/capacity_final.json)均通過。
同 owner 已保留 **12 次鉅亨 502、4 次連線 timeout**，失敗 session reset **4 次**，
CMoney、PeoPo 各又出現 **1 次 403**，原 HTTP 與來源期限保護仍有效。
可修的分工、排程與回復契約已部署，不將仍存在的來源拒絕或 gateway 故障寫成消失。
`CNA_API_KEY` 在本機仍未填入；既有授權 API 接線尚不能開始能力與配額核對，沒有再次要求使用者證明授權。

[既有進度 API 讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v46/public_projection_acceptance_v2.json)
確認 localhost `/news/api/status` 為 running、fresh、唯讀、snapshot 一致，
已顯示目前 owner 的新增正文 head，總 URL 清冊與各 provider 合計一致；
瀏覽面板不會向來源送出請求，也沒有虛構全歷史 ETA。
這次沿用原頁面，沒有 UI 重設計，不把 API 讀回當成新的瀏覽器版型或 WAN 驗收。

## 鉅亨失敗頁與到期工作餓死修復

2026-10-10 晚間，Asia/Taipei；v47。這輪修正的是來源解析與共用新聞排程，
不是藉由跳過收據、改寫原文、減少來源範圍或把 HTTP 200 當成完成來提高進度。
保留先前的來源拒絕、原失敗觀察與所有中間測試結果；沒有變更其他下載器、交易或訓練服務。

### 已取得、只是核對與尚未完成必須分開

[CMNews 選定 head 與來源回放](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v47/recovered_sixteen_heads.json)
核驗 16 個 URL：其中 **14 個 CMNews head 是這輪於 19:16:00–19:16:30 新補回**；
另兩個鉅亨 head 的 capture clock 是當天 **08:22:01／08:22:03**，早已有正文，
只做核對，**不算新增、也不算本輪加速效果**。檔名中的 sixteen 是核驗樣本數。

後來以真正缺正文的失敗頁重新取樣，
[來源診斷回放](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v47/current_failed_cnyes_corrected.json)
確認三個有保存原始回應的樣本：兩個可解析正文，一個來源空模板。
第四個樣本仍是 `http_502`，沒有原始正文或成功證據；診斷報告的 accepted 不表示它也被修好。

[新補回的兩個鉅亨 mobile head](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v47/recovered_factset_heads.json)
已通過 immutable batch、正文／回應 SHA、來源作者與 clock、目前來源 parser 回放：

| 實際新補回 URL | 新 capture clock（台北） | 原始正文字符數 |
| --- | --- | ---: |
| `https://m.cnyes.com/news/id/5437568` | 20:02:49 | 329 |
| `https://m.cnyes.com/news/id/5432092` | 20:02:59 | 632 |

它們是兩個先前缺正文的 **URL head**，不是證明兩篇故事從未出現在其他表示或 canonical URL。
這兩個成功發生於解析修復之後、排程契約 5 重載之前，不能歸因為後來的排程修正。

### 解析根因：來源格式與來源缺值，不是任意清洗

鉅亨兩個 FactSet 頁面的公開正文與同 ID、免費的 current-content 宣告一致，
但 JSON-LD 省略了公司名稱前的單個 U+FEFF。
[來源解析器](../downloader/news_fulltext_publishers.py)只在比較副本中處理實際觀察到的
FactSet／綜合評級固定前綴；必須同時有唯一、相同的 current-content 段落與股票連結。
原始正文、回應、公司文字、股票代碼、負值、表格格線及發布 clock 不變。
任意詞內／重複格式字元、數值衝突、付費、隱藏正文或其他文章資料仍拒絕，沒有全域 strip 或 NFKC。
current-content 段落只在需要時解析一次並建立索引，正常已匹配正文不做這段額外工作。

`784083` 的 current-content 是編碼後的精確空 `<div></div>`，schema 與可見正文也空，
同 ID、標題、免費旗標、aware 發布 clock 與空 video 宣告吻合。
新增的窄分類不接受任意「沒有文字」的 HTML、隱藏內容、script、媒體或未知值。
[實際佇列讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v47/current_failed_cnyes_queue_acceptance.json)
確認它是 `source_article_empty_template`、沒有正文 head，不再混成未知解析故障或成功。
這是目前來源的無正文證據，不是假造完整歷史。

CMNews 的 14 筆是已核對 profile 的當前 UUID 文章，來源可見署名節點實際空白。
以前的必要作者欄位讓它們誤判失敗；現在保留 `authors=[]`、
`source_visible_byline_name_empty` 與實際 profile／空署名的出處，不從網址或相鄰文章杜撰作者。
公開正文容器、UUID、canonical、profile、可見 clock、隱藏／VIP 遮罩仍核對；
發布與更新兩個可見分鐘 clock 分開保存，不補來源沒有宣告的秒數或時區。
Breadcrumb schema 不當成另一篇 NewsArticle 的署名依據。
這不是允許其他未知作者或會員文章繞過原政策。

捕捉契約分別升為 CMNews adapter **6**、鉅亨 native HTML **9**、空模板 outcome **4**；
HTML 修復不更動鉅亨 bulk adapter **8**，也不因這次 HTML 改動重抓健康 API 頁。

### 故障診斷本身也不能重複下載已完成工作

[診斷入口](../scripts/probe_news_sources.py)新增至多 16 筆的唯讀 preflight：
必須仍是到期、無正文、真實已有 clock 的缺正文失敗，且沒有租約、來源拒絕或未到期限制。
曾有一組選定 URL 在正式服務 drain 時已被重新檢查；preflight 正確取消，沒有送出來源請求。
來源未證明無正文的 `body_not_found`，不是永久不存在的證據。
其恢復契約 **2**只涵蓋已註冊、啟用、持續型 scope 的已核對 native URL 與既有 `exp` 表示。
不生成任意 query／新來源，不改有限 cohort、403／429、付費、已存 head 或下載中的工作。
單 URL 未知缺正文至少隔 300 秒再試、耗盡 burst 後冷卻；這不是所有文章的請求節奏。

修復 journal 保留原 error、HTTP check clock、attempts、due；
一般 policy rollout 不得先把這批工作重設成 due=0／attempts=0。
[唯讀 cohort 核對](../scripts/audit_news_body_rechecks.py)與 immutable body 驗證分開；
重新排程或看到 head hash，都不是全文下載完成。

### 續跑才發現的共用根因：regular retry 清掉 oldest-due 信用

第一次重載後的
[553.73 秒同 owner 窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v47/runtime_factset_final.json)
雖有鉅亨 **268 個新增 URL head／0.484 head 每秒**、Global Voices **53 個**，
[原 40 筆新恢復 cohort](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v47/body_rechecks_v2_final.json)
仍全在 retry_cooldown、沒有新增 head。不能用其他工作有進展替代這批已被服務的證據。

根因是 regular lane 選到較新的 network retry，就清除 oldest-due lane 的服務信用。
只要較新的 retry 不斷出現，真正最早到期的舊工作便可能永遠等不到獨立輪次。
[8 個反例](../artifacts/operations/agent-workflow/runs/v47-reproduce-oldest-due-starvat-20261010T120254-3f799959/run.log)
在舊版全部重現；涵蓋 retry／retry_cooldown、parser／502、burst 2／8。

[共用 queue](../downloader/news_fulltext_queue.py)的 dispatch 契約升為 **5**：
只有**真正服務 oldest-due lane**才能清除其信用。
較新的 regular retry 仍可按日期恢復，但不能冒充最早到期工作已獲服務。
已有正文重查、無日期工作與 publisher 各自的公平輪次維持；
來源期限、原 clock／error、正文與租約不重設，查詢仍使用既有 partial index seek。
新增 `frontier_oldest_due_retry_dispatches`，與一般 retry counter 分開。
此為共用工作選擇變更，容量 fingerprint 納入 dispatch 5；量測定義仍 **10**，
不把舊工作選擇下的 body throughput 冒充新排程校準。

[236 項針對性回歸](../artifacts/operations/agent-workflow/runs/v47-final-oldest-due-fairness-se-20261010T120535-b840a282/run.log)
及 [完整新聞回歸](../artifacts/operations/agent-workflow/runs/v47-final-oldest-due-all-news-re-20261010T120536-6481d6d6/run.log)
均通過；後者 **1,991 項／44.18 秒**。
原先測試只覆蓋新的 pending，不足以抓到持續較新 retry 造成的餓死；這輪補的是該反例，不是放鬆驗收。

20:07 重載後 PID **534085**，owner `0ac482c8d0d34586b513a7f350132ad7`；
20:08:58 已讀到 running 與 dispatch **5**。
[新 owner 起始核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v47/runtime_fairness_baseline.json)
確認目前 source policy／source-code 與實際 owner 一致，
[ready 工作讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v47/cnyes_work_fairness_initial.json)
維持所有六個 ready lane 的索引 seek，不用掃遍所有已存正文來挑下一筆。

### 部署後持續進度與未解決的舊失敗頁

[20:17:59 的同 owner 持續驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v47/runtime_fairness_final.json)
通過 12 項工程檢查，包括已載入 dispatch 5、來源／程式 fingerprint 一致、
服務期間未改動下載程式、獨立來源持續入庫，以及 HTTP 起始間距符合當時政策。
與 20:09:17 起始快照相比，資料庫窗口 **521.775 秒**、HTTP counter 窗口 **521.809 秒**：

| 來源／量測 | 這個窗口的實際結果 |
| --- | --- |
| 鉅亨新增已存 URL 正文頭 | **397**，**0.7609 筆／秒** |
| 鉅亨 HTTP 請求起始 | **679**，**1.3012 次／秒**；含追新、重查與非正文結果 |
| 鉅亨最早到期 lane | **55** 次；全部到期 missing-body retry 為 **92** 次 |
| Global Voices 新增已存 URL 正文頭 | **46**，**0.0882 筆／秒** |
| 鉅亨窗口末的容量狀態 | client interval **0.2 秒**、parallelism **4**；不是每秒 5 篇正文或官方上限 |

這是原始 URL 頭增量，不是跨 URL 去重後的獨有文章數，也不是全歷史完成。
前一 owner 的 553.73 秒窗口為 0.4840 頭／秒，但工作組成、錯誤與容量不同，
不能拿兩窗口直接宣稱同來源配對的加速倍率。

[最近 128 個 immutable batch 的獨立驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v47/recent_batches_fairness_final.json)
核對 **4,617** 個正文版本與 current head，私有原始回應合計 **832,655,169 bytes**，無驗證錯誤。
其 cohort 不等於上表 397 筆，不把整個驗證樣本當成這輪新增。

[40 筆原始失敗 cohort 的後續讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v47/body_rechecks_fairness_final.json)
為 **38 retry、1 inflight、1 article_identity_not_verified，已存正文頭 0**。
此報告通過的是 migration／owner／原始證據保全，不是正文驗收。
新 owner 的 55 次專屬 oldest-due counter 證明工作已獲服務，但不能把所有 40 筆狀態改變都歸因於 dispatch 5，
更不能將「已重試」寫成「已修復」；剩下的來源表示仍需拿原始回應核對。

[17 個 enabled provider 的傳輸核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v47/transport_fairness_final.json)
保留鉅亨 **8** 次 awaiting-headers 失敗與 **8** 次 session reset、**22** 次獨立 gateway-host 決策，
以及 Global Voices **1** 次 timeout／session reset，診斷寫入無錯誤。
這些是同 owner 的累積數，與上表窗口增量不可混用；gateway 決策也不是正文成功。
來源 502／逾時尚未根治，恢復後持續入庫只證明沒有整個 provider 永久停擺。

### 網頁與剩餘邊界

[既有進度頁的瀏覽器核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v47/dashboard_fairness/acceptance.json)
通過 desktop、tablet、mobile、320 px 四種 viewport 與鉅亨／CMNews／CNA 的精確 provider 卡片。
正文統計、流量、原始 URL 處理進度與已授權但尚未接通的 CNA API 角色皆讀既有唯讀快照，
不是向來源呼叫來補顯示。這是 localhost 的 cpu-2d Chromium／資料與互動核對，
不是 WAN、GPU/WebGL 或全網站歷史完成證據；沒有重新設計 UI。
截圖仍顯示手機的來源注意清單很長，後續應保留全部資訊但把摘要與詳細清單分層。

來源 502、連線 timeout、CMoney／PeoPo 403、不能核對的署名／範圍，
以及 Global Voices 的少量影像／caption／外部嵌入頁仍分開留證，不混成完整正文。
最早到期工作獲服務不代表 parser 已能處理所有剩餘表示；未知缺正文仍要逐樣本診斷。
這輪沒有證明官方最高速、整批不可修復、所有 provider 已完成或全歷史 ETA。

### 後續追查：圖說與無圖說圖片混合造成整篇拒收

20:31 的有界診斷不是又抓四篇已完成的新聞。
[原始來源回應與到期授權讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v48/current_due_native_failures.json)
在正式 producer drain 後，以目前 WAL 選到 **2** 筆仍缺正文且無 lease 的到期失敗：
`5142712?exp=a` 與 `5008397?exp=a`。最多四筆不是必須送滿四次；
原先的 due、最後失敗 clock 及既有來源拒絕都沒有覆寫。
診斷結束的 EXIT handler 恢復既有 service／timer，兩次 HTTP 回應及 raw Parquet 私有保留。

兩頁的 visible narrative 與 current free content 完全相符；
JSON-LD 額外帶當篇圖說，但兩頁同時包含另一張 **沒有 figcaption、沒有文字的圖片 figure**。
舊 `_cnyes_caption_schema_view` 要求每個 current figure 都有一個非空 caption，
因此一張正常無圖說圖片就讓另一個有效圖說的核對失敗，整篇被誤判 `body_not_found`。
[舊程式的合成反例](../artifacts/operations/agent-workflow/runs/v48-reproduce-mixed-caption-figu-20261010T123410-b2e345f3/run.log)
在 native desktop query／mobile、完成串流與正常 HTML、前後不同圖片順序共 **12** 種情況全部重現失敗。

修正仍沿用同一個共用 parser，只在三個文字表示不一致時做 figure 比較，
正常正文 fast path 不增加解析。無圖說 figure 必須恰好一張圖片、無文字、可見，
並與 current content 的 image src、唯一性與整體 figure 順序一致；
有圖說的 figure 仍核對原始 caption，不把封面、相鄰內容、隱藏文字或財務數字借成正文。
圖說另存 `verified_image_captions_json`，真正無圖說的圖片不產生假的空 caption 紀錄。
HTML capture fingerprint 更新為 **10**，bulk **8**／empty outcome **4**／dispatch **5** 保持不變，
不因這個 HTML 修復重抓健康 API 批次。

[針對性回歸及真實 raw 回放](../artifacts/operations/agent-workflow/runs/v48-mixed-caption-semantic-valid-20261010T123449-4f649c75/run.log)
通過 **448 項／4.42 秒**；
[兩筆實際原始回應驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v48/mixed_caption_source_replay.json)
得到完整 narrative **1,376／1,186 characters**，來源發布時間分別保留
`2023-04-12T07:44:44.000Z`／`2022-11-13T05:00:06.000Z`，作者與 response SHA 同步核對。
這裡先證明實際來源回應可解析，不能把離線回放冒充已補回正式正文頭。

[三筆較早 retained native 回應的另一次回放](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v48/existing_native_failures_replay.json)
亦可解析；它們是已有私有證據的離線診斷，沒有額外 HTTP、沒有新增正文收據宣稱。

### 正式補回、跨 parser 原始失敗清單與持續驗收

HTML capture 10 於 **20:37** 由原 service 重載，PID **600247**、owner
`6aa15d5c5c364fd08f2cf31d96be95d8`，`NRestarts=0`。
[實際 probe 的第二篇 `5008397?exp=a`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v48/second_mixed_caption_head.json)
於 **20:45:08** 補回；
[原始失敗清單的三篇正文](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v48/three_original_cohort_heads.json)
為 `5037772?exp=a`（20:38:28）、`5125760?exp=a`（20:44:34）及
實際 probe 的第一篇 `5142712?exp=a`（20:45:03）。
四篇均通過 selected head、immutable batch、正文／原始 response SHA、作者、發布 clock
與 current parser 回放，且 capture fingerprint 與目前 source 一致。
`5008397?exp=a` 不在原始 40 筆 migration cohort 內，不能把這四篇寫成「40 筆已修四筆」。

重載後另外發現唯讀驗收的分母問題：auditor 只讀「目前 source fingerprint」的 migration journal。
新 parser 建立的新 journal 為 0 筆，因為舊失敗已在 retry lane；若只看新 journal，
就會把仍待修的 40 筆變成 0。
保留 [舊 auditor 的原始讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v48/body_rechecks_mixed_caption_initial.json)
作為反例，沒有覆寫成新結果。

修正只涉及唯讀 auditor 與其測試：同一 repair ABI 的歷次 private journal 持續追蹤，
以原始 URL 合併並保留最早原始觀測，不因 parser 指紋變更而重置分母。
每個 journal 的 fingerprint／contract／原始 outcome／拒絕未重設聲明皆核對，
目前 source 與 owner 仍須正確載入；舊 journal 壞掉也不能被新的正常 journal 蓋掉。
settings namespace 有界索引查詢及每 URL 的 current-state seek，不掃正文庫、不呼叫來源、不改排程。
報告 contract 更新為 **2**，加入實際 current check clock 與 due，
body-head observation 與 immutable durability 仍是不同證明。

[原始失敗清單回歸及讀回](../artifacts/operations/agent-workflow/runs/v48-preserve-original-failure-co-20261010T124222-5a5adcd8/run.log)
**61 項通過**；
[最後完整新聞回歸](../artifacts/operations/agent-workflow/runs/v48-final-complete-news-regressi-20261010T124257-f42d9fdf/run.log)
為 **2,018 項／49.50 秒**。
[20:45:18 的原始 40 筆讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v48/body_rechecks_mixed_caption_final.json)
為 **3 complete、36 retry、1 article_identity_not_verified**。
其中三個 complete 的獨立耐久證據為上述 selected-head 報告；
其餘 37 筆沒有偽造完成或改寫來源發布時間。

[20:45:09 的同 owner 持續驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v48/runtime_mixed_caption_final.json)
通過所有 12 項工程檢查。與 20:38 baseline 比較，資料庫窗口 **418.655 秒**：
鉅亨新增 **392** 個已存 URL 正文頭，**0.9363 頭／秒**；
HTTP 起始 **1.3702 次／秒**，oldest-due lane **48** 次；
Global Voices 另入庫 **43** 個正文頭，沒有因鉅亨診斷／來源錯誤永久停住。
窗口末 runtime interval 為 **0.5 秒**、parallelism **4**；
[10 秒後的容量回饋](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v48/capacity_mixed_caption_final.json)
已升至 **5** 個 slot、仍為 ramping。
兩份快照 clock 不同，不能拿後者的 slot 當前者整個窗口的容量，更不能宣稱官方最高速。

[最近 128 個 batch 的實際驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v48/recent_batches_mixed_caption_final.json)
核對 **3,692** 個正文版本與 current head、**661,678,343 bytes** 私有原始回應，無驗證錯誤。
[17 個 enabled provider 的傳輸／恢復讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v48/transport_mixed_caption_final.json)
保留同 owner 鉅亨 **2** 次 awaiting-headers 失敗／**2** 次 session reset、
**18** 次獨立 gateway 決策，診斷寫入無錯誤。
amp host 的暫時 circuit 不會把正常 `news.cnyes.com`／API 一起停掉；
這是有界恢復與持續入庫證據，不是來源 502 已被消除。

剩餘優先工作仍是 37 筆原始失敗的未涵蓋表示及獨立來源的拒絕／署名／範圍問題，
再延伸 Global Voices 影像／外部嵌入與 CNA 已授權 API 設定。
有 retry 不等於有正文、原始網頁沒有內容不等於程式漏抓；
服務繼續抓取與目標未完成可同時成立。

## Global Voices 缺正文分類與短文回補

2026-10-10 晚間。沿用原 queue owner、source 時鐘、robots、有限重試與私有不可變收據；
只修文字擷取與來源結果分類，沒有下載圖片、執行第三方 widget、OCR 或提供瀏覽器正文。

### 真正文與非正文分開

字數只能是篩選啟發式，不能證明文章完整。一篇作者真的只寫 40 字，和照片旁的
「詳見另一篇」或「校對」欄位，不應同樣視為失敗，也不能把三種都當完整正文。
本輪用實際已留存的回應、唯一 canonical、可見標題、header／footer 一致作者、
含時區的發布 clock 與文章內 CC license 核對四筆原始失敗。

| 原始失敗頁 | 已驗證的來源內容 | 處理方式 |
| --- | --- | --- |
| 英文 2014 年 5 月 27 日澳門照片頁 | 照片、被排除的圖說及單一舊報導導覽連結 | `source_body_image_only`；不拿另篇報導補成本篇正文 |
| 繁中 `16502` | 照片、圖說及譯者／校對兩欄 | `source_body_image_only`；署名不是正文 |
| 英文 2014 年 1 月 18 日入學題目頁 | 圖片以外另有作者的完整 **40 字**文字 | 保存原始文字；註明圖片未下載／未 OCR，圖說不放進正文 |
| 繁中 `16251` | 照片、圖說及單一校對欄 | `source_body_image_only`；不再把八字校對欄當缺正文反覆抓 |

[四筆原始回應的完整離線核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v50/all_four_globalvoices_replay.json)
全部通過來源完整性或明確非文字結果驗證。這裡的三個 image-only 是「沒有可保存的文字正文」，
不是三篇新聞已下載完成；HTML 與非正文欄位仍在原私有診斷證據內。

修正以 Global Voices HTML contract **5** 版本化。健康 RSS 的資料指紋與 due 不變，
不為 HTML 小修重抓既有 feed；普通長正文不走新增的微型文章檢查。
短文例外只接受完整目前文章容器裡可見的作者文字。靜態圖片須位於既有排除的 caption 容器，
caption 不轉成 narrative；不明 media／script、hidden 文字、相鄰文章、credit 或導覽不能取得例外。
少於八個英文單字的文字也必須先通過上述來源核對，並非全域關閉短文檢查。

來源主站與繁中站目前留存的 robots 都明列 `User-agent: *`、`Crawl-delay: 10`；
本輪維持 **10 秒有效間隔、1 個 slot**，不以平行化繞過來源的共同限制。
本輪新診斷兩次共取得 **兩個**原始失敗頁，各一次；第一次設定最多兩筆，實際只有一筆符合到期條件。
取樣透過原 HTTP／robots／限流與唯一 HTTP-owner lock，service／timer 都有 EXIT 恢復，
沒有直接改 queue 成功狀態。

### 回歸與正式落盤

保留[修正前六個反例](../artifacts/operations/agent-workflow/runs/v49-globalvoices-before-countere-20261010T130127-25c82b2c/run.log)、
[圖片旁短文的兩個反例](../artifacts/operations/agent-workflow/runs/v50-captioned-short-body-before-20261010T131522-b663ac6d/run.log)及
[單欄校對反例](../artifacts/operations/agent-workflow/runs/v50-review-credit-before-counter-20261010T132211-2cfbe869/run.log)。
另保留一次 navigation／未知 credit 防護未通過的中間測試，不把 failed run 改寫成成功。
最後[相關 195 項與完整新聞 2,050 項回歸](../artifacts/operations/agent-workflow/runs/v50-final-news-regression-and-so-20261010T132256-9a301604/run.log)
分別在 **3.48／52.30 秒**通過；測試內的 mock 速率不是網站速率實測。

[40 字短文正式 head 的獨立驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v50/short_body_head_initial.json)
確認於 **21:23:49** 入庫，正文、原始 response、不可變 receipt、作者與
`2014-01-18T09:51:53+0000` 發布 clock 均可用目前 parser 重現。
該筆是在 HTML contract 4 入庫，而 contract 5 只再修另一頁的校對欄分類；
因此 capture fingerprint 與目前 source 不同，**current replay 與不可變驗證仍通過**。
沒有為了讓 fingerprint 顯示一致而重抓健康正文、偽造新觀測或改掉 capture 時間。

[前兩筆照片頁的正式 queue 結果](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v49/globalvoices_owner_outcomes_final.json)
已由原 owner 真實查驗並分類，不是離線解析後直接改資料庫。
[最後 contract 的三筆非文字結果核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v50/all_four_globalvoices_owner_outcomes.json)
確認三頁在正式 owner 下都是 `source_body_image_only`，載入的 source contract 亦一致；
全 Global Voices 84,606 個候選缺正文工作仍是 20:54 的清冊範圍，不能因四筆已解就稱全站完成。

### 鉅亨續抓與全來源未解項目

[原始鉅亨 40 筆清單的 20:54 快照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v49/body_rechecks_baseline.json)
有 **11 complete、19 retry、9 retry_cooldown、1 identity 未確認**；
[全部 11 筆的逐筆耐久驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v49/original_cohort_heads.json)
均通過目前來源與 immutable batch。21:07 的[同分母讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v49/body_rechecks_current.json)
為 **11 complete、28 retry_cooldown、1 identity 未確認**，尚未補回的仍是 29 筆。
失敗後的既有 due 不會因重啟被歸零；五分鐘是未知正文重查的最短值，耗盡即時重試 burst 後仍可有一小時 cooldown，
不是所有工作都每五分鐘成功，也不是已抓全。

[21:06 至 21:11 的同 owner 持續驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v49/runtime_final.json)
在 **332.451 秒**新增鉅亨 **161**、Global Voices **24** 個 URL 正文頭，全部 12 項工程檢查通過。
鉅亨 **0.4843 頭／秒**、HTTP **0.9691 次／秒**是該窗口的混合工作量，
低於較早窗口；source timeout、容量調整與工作類型不同，不能宣稱本輪全鏈路倍數加速。
[最近 128 個 batch](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v49/recent_batches_final.json)
驗證 **3,722** 個正文版本，沒有 hash／receipt 錯誤。

[17 個 enabled provider 的當前 owner 傳輸讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v49/transport_final.json)
保留鉅亨 **14** 次傳輸錯誤及 **14** 次 session reset、**18** 次獨立 gateway 決策，
相關診斷寫入沒有錯誤；同快照未見其他 provider 新傳輸失敗，不代表其歷史錯誤已不存在。
`amp-news.cnyes.com` 的 502 使用獨立 host circuit，正常主站與 API 仍能續抓。
client slot 上界 **8** 是工程上界，不是官方上限證明；容量仍依目前完成請求、錯誤與入庫成效調整。

[20:54 全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v49/all_sources_baseline.json)
有 **59 個設定來源、17 個啟用來源、683 個未註冊來源鍵**，共 **6,201,807 URL／3,543,114 正文頭**。
其中 **2,130,285 URL** 尚待來源授權／用途核對。清冊讀取同一 WAL、只掃一次，
實際耗時 **20.15 秒／19.68 CPU 秒**，不把這種全量清點放到每次派送或每個 provider 重複執行。
CMoney 的來源 403、民視未確認原作者／轉載範圍、影像 PDF、其他付費與未註冊來源仍需逐項處理。
[中央社本機設定檢查](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v49/cna_local_setup.json)
確認 `CNA_API_KEY` 缺少，送出 **0 HTTP／0 tool call**；已宣告的授權保留，不重問授權，
也不拿公開 OAuth client ID 或轉載內容冒充已接入授權 API。

### 最後版本持續驗收

HTML contract 5 於 **21:25** 交由原 service 載入，PID **699348**、
owner `af20c96dbe564cd28eec2372c89edb9b`、`NRestarts=0`。
保留部署剛啟動、舊 heartbeat 尚未替換的初始讀回，不把它當 ready；
[21:26 的 ready baseline](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v50/runtime_ready_baseline.json)
才確認實際 PID 的 start clock、WAL owner、status owner 與 loaded source／code 一致。

[21:30 的同 owner 驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v50/runtime_final.json)
通過全部 **12 項**檢查。資料庫窗口 **217.088 秒**新增鉅亨 **179**、Global Voices **16** 個正文頭；
鉅亨 **0.8246 頭／秒**、HTTP **1.4637 次／秒**。
[最近 128 個 batch 的不可變驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v50/recent_batches_final.json)
有 **4,473** 個正文版本、無錯誤；版本集合不等同本窗口的新增 URL 數。

[容量回饋](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v50/capacity_final.json)
在該快照為鉅亨 **0.5 秒有效間隔、5 個 slot**，engineering maximum 仍為 8；
同 owner 有 **4** 次 transport error、**15** 次 502，沒有 403／429。
個別 host circuit、session 恢復與自適應容量仍保留；錯誤不會被清除來美化吞吐量。
此窗口比前一個窗口快，但 work mix、網路狀態與 slot 不同，不能宣稱是同源同工作量的因果倍數提升，
也不是已達官方最高速度。

[原始鉅亨失敗清單最後讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v50/body_rechecks_final.json)
仍為 **11 complete、28 retry_cooldown、1 identity 未確認**。
本輪四筆 Global Voices 的分類／正文問題已修復；鉅亨剩餘表示、CMoney 拒絕、民視署名、
其他來源權利／註冊與中央社憑證仍是後續工作，整體新聞目標保持 active。

## 鉅亨正文表示與新來源故障驗收

2026-10-10，v51–v52。只延伸既有新聞 collector、來源驗證與不可變保存，不新增重複管線。
正式 owner 使用鉅亨 HTML contract **11**、Global Voices HTML contract **6**、
failure diagnostic contract **2**；鉅亨 bulk adapter **8**、健康 RSS 指紋與既有正文來源 clock 不變。

### 來源根因、修復與驗證邊界

| 根因／樣本 | 修復 | 實際驗收 |
| --- | --- | --- |
| 鉅亨 `4949007?exp=a` 的圖片 figure 內有 `p.warning` 三行免責聲明，正文排除 figure 後與 schema 不同 | 只辨認已觀察到的精確聲明模板；相同 current／visible 圖片、可見聲明、順序、完整 schema 都須一致。聲明保存為 `verified_article_notices_json`，不是圖說，也不刪金融數值 | 22:18:33 真正入庫 **1,465 字元**；notice 1 筆、當前重播／指紋／不可變批次全部通過 |
| 鉅亨 `4968333?exp=a` 的 schema 含字面角括號文字，HTML stripper 會刪掉真正原文字詞 | articleBody 是 [Schema.org 的 Text 型別](https://schema.org/articleBody)。比較完整 literal／HTML 表示，仍須與 current 免費資料及可見正文完全一致；不是無條件信任 JSON-LD | 私有原始回應離線重播通過 **1,516 字元**。22:18 選定雙筆讀回時仍在既有 retry cooldown，不能以離線成功當正式入庫 |
| Global Voices `2013/12/16/video-hong-kong-intertidal-marine-life/` 有完整短正文及單一靜態 Vimeo 宣告 | 只接受實際觀察的精確 HTTPS player／ID／query 結構及完整作者文字；不下載、播放、轉錄影片。無正文的 embed 頁另列非文字結果 | 21:49:12 真正入庫 **82 字元**；來源 CC、作者、aware clock、當前重播與不可變批次通過 |
| 正文判定失敗後缺乏可檢查的來源證據 | 對實際已收到的回應保存有界診斷；增加符合文章內 CC／作者／clock 的 Global Voices 範圍。未知／付費／無權利證據仍只留 metadata | 每 provider／UTC 日最多 **2 次**，失敗寫入也計數。預算 key 維持 v1，格式升到 2 不重設配額；沒有額外 HTTP、worker SQLite 寫入、正文接受或公開輸出 |

鉅亨 literal 與 parsed schema 各只計算一次，再重用於格式／圖說／聲明比對；
健康正文不做額外 figure 重解析。圖片、影片、未知 figure 裡的真正文或金融數字不能被上述例外消除。
免責聲明的保存與當前 source replay 也有獨立反例，不能只驗證正文 SHA 而漏掉新增 metadata。

[兩筆鉅亨實際已收到的失敗回應](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v52/cnyes_current_due_failures.json)
與[修正後離線驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v52/offline_repaired_source_diagnostics.json)
保留前後證據，不修改 probe 的原失敗結果。
[1,465 字正文正式驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v52/notice_head_final.json)及
[82 字正文正式驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v51/video_head_final.json)才是入庫證明。
[雙筆初始讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v52/selected_heads_initial.json)的未完成結果仍保留。

### 固定清單與完整新聞回歸

[22:21 原始 40 筆清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v52/body_rechecks_final.json)
為 **23 complete、16 retry、1 identity 未確認**，不是整個鉅亨的完整歷史。
該排程 audit 的 `body_durability_verified=false` 保持原義，不事後改成它沒做過的檔案驗證。
獨立用[新補回的 12 筆驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v52/recovered_original_heads_01.json)及
[原 11 筆當前重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v52/old_original_heads_current.json)
核對全部 **23** 個已存頭。新 12 筆 capture 指紋全部為當前 HTML contract 11；原 11 筆保留原捕獲指紋，
其原文、作者／時間與當前 parser 一致，不為報表一致性重抓、改寫或回填捕獲時間。

v51 的[定向 233／完整 2,072 項測試](../artifacts/operations/agent-workflow/runs/v51-video-diagnostics-focused-an-20261010T134414-a2efdf4e/run.log)
通過；v52 的[296 項來源定向測試](../artifacts/operations/agent-workflow/runs/v52-cnyes-corrected-focused-20261010T140814-deb63bd6/run.log)與
[109 項 metadata／不可變定向、完整 2,102 項新聞回歸](../artifacts/operations/agent-workflow/runs/v52-news-notice-and-regression-20261010T140939-709c896d/run.log)
通過，完整新聞套件 **53.35 秒**。
這是 `test/test_news*.py test/test_cnyes_news_bulk.py`，不是整個專案全套。
修正前的反例／中間 fixture 編碼失敗也保留，不把所有測試 fixture 的失敗都算成來源缺陷。

### 持續落盤、速度及其他來源

v51 [21:52 同 owner 驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v51/runtime_final.json)
在 **206.983 秒**新增鉅亨 303、Global Voices 20 個正文頭。
v52 保留部署後 stale heartbeat 的初始讀回；
[22:12 ready baseline](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v52/runtime_ready_baseline_v2.json)
才確認 owner、PID start、status、loaded policy/code 一致。
[22:18 同 owner 驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v52/runtime_window.json)
全部 **12** 項通過：資料庫窗口 **339.768 秒**新增鉅亨 **704**、Global Voices **30** 個頭；
鉅亨 **2.072 頭／秒**。獨立 HTTP counter 窗口 **350.234 秒**為 **2.727 requests／秒**，
兩種 clock／工作量不能混成同一分母。
[最近 128 個批次](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v52/recent_batches_window.json)
有 **6,195** 個正文版本，檔案／原始回應／正文 SHA／收據驗證無錯誤；版本數不等同窗口新 URL 頭。

[22:37 較長窗口驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v52/runtime_final.json)
仍全部 **12** 項通過，同 owner **1,495.280 秒**新增鉅亨 **2,483**、Global Voices **131** 個正文 URL 頭，
鉅亨 **1.661 頭／秒**、HTTP **2.271 次／秒**。較長窗口包含更多網路退避，不能只選較快短窗口代表全天速度。
[最後 128 個批次](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v52/recent_batches_final.json)
為 **5,937** 個有效版本、無錯誤；它是另一個較新的有限批次集合，不與前一集合做增減相減。
[17 個來源的最後傳輸核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v52/transport_final.json)
通過；鉅亨 **27** 次傳輸錯誤與 **27** 次 session reset、**61** 次 gateway 範圍決策均留有可核對證據，
沒有診斷寫入錯誤。這證明在這些錯誤中持續落盤，不是網路問題全部消失或來源全歷史完成。

解析規則變更會使舊容量的「有用正文吞吐量」證據失效；沒有直接沿用舊最佳率。
正式 controller 已自行重新量測／ramp，曾觀察至 **0.05 秒** cadence，之後因 timeout 又退回較穩定容量。
這是客戶端控制值，不是實際達成 20 requests／秒或官方限制。
來源 metadata、網路、slot、重查比例與工作量都變動，不能把前後不同窗口當因果倍速證明。

新 owner 中的 CMoney `forum/article/185366579` 與 PeoPo `news/587660` 在 22:20 收到真正 **HTTP 403**，
不是正文中「拒絕」字眼誤判。
CMoney 的[拒絕證據](../data_news_fulltext/access_problems/cmoney/1791642024078343040-e0ef7ce724a93e2e1cf463b1f4ebfb71e91b0e252201b0ccd3d99d344be7c827.json)及
PeoPo 的[拒絕證據](../data_news_fulltext/access_problems/peopo/1791642026090208325-7796a28302408e065a73afd7070f0f96d0728f3c2c08e7d770e5a453a2d7520d.json)
確認 `body_read=false`；保留 900 秒來源拒絕重試，不用換身分／挑戰繞過／摘要來假裝取得全文。
[CMoney 官方 CMService 文件](https://www.cmoney.tw/cm-service/CMService.asmx)列有股票 RSS／日期區間操作；
這只是候選正式入口，未證明具本篇論壇全文、歷史權限或批次授權，因此未當作已修復或自動啟用。

目前原固定清單仍有 17 筆未完整取得，其他 provider 的拒絕、權利、有限範圍、未註冊與 CNA 憑證也仍分開保留。
新聞 service／timer 與全來源 goal 繼續執行。下一步針對已到期、無正文的實際失敗做有界診斷，
驗證剩餘表示及候選官方入口；不以服務 active／HTTP 200／離線 parser 成功宣告全來源完成。

## 鉅亨失敗診斷與已有原文驗收

23:49 狀態仍在實際續抓。來源的 502、逾時與正文表示不一致是不同問題；
提高請求目標不能解決宣告全文與可見圖文不一致，解析成功也不能代替原文落盤。
本輪修復失敗證據缺口及診斷排程的合法無工作分支，保留既有正文接受、來源拒絕與重試規則。

### 公開失敗回應與解析階段

`_cnyes_native_article_identity` 共用同一套 native 身分見證：已觀察的精確 URI 形狀、
同 ID canonical／og／當前資料、免費標記、當前與宣告作者、可見署名及有 offset 的發布時鐘。
完整正文仍須另外通過可見全文／當前 content／宣告全文、圖文順序、表格欄位及可見性檢查。
身分見證不等於正文通過；失敗回應只進私人診斷資料，不進已取得數量、瀏覽器或訓練匯出。

診斷契約先升至 3，再由失敗階段欄位升至 **4**。只有身分見證通過的公開 native 變體保留原 `exp=a`／`exp=b`；
其他 query 一律移除並留 SHA，cookie、authorization 等 header 不保存。
原有預算鍵仍是 `failure_diagnostic_attempts:v1`，每來源每日最多兩次嘗試，儲存失敗及舊 metadata-only 嘗試也計入；
新版不清空當日已用預算。兩份先前真正收到的公開回應通過[離線身分與全文證明](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v53/offline_native_identity_proof.json)，
沒有新增 HTTP 或修改 queue。該證據保留當時診斷契約 3，不回寫成契約 4。

新增五個固定、無正文／個資的失敗階段。既有 `body_not_found`、deadline、attempt 與接受條件不變。

| 計數階段 | 意義 |
| --- | --- |
| `missing_body_representation` | 缺少可見、當前或宣告全文表示 |
| `visible_current_mismatch` | 可見全文與當前 content 不一致 |
| `schema_mismatch_without_verified_figure_view` | 宣告全文不一致，亦沒有可驗證的整體圖文比較視圖 |
| `schema_comparison_mismatch` | 圖文視圖已驗證，整篇文字仍不一致 |
| `table_grid_mismatch` | 純文字可能相同，但表格欄位／數值邊界不一致 |

計數來自當次解析分支，沒有額外請求、再解析或逐失敗寫檔。
23:43 正式快照已觀察到四種階段，分別為 **3／0／48／3／3** 次；它們是此 owner 的失敗事件，不是去重文章數。
未知標記、其他 provider 或其他錯誤碼不能建立任意 metric key，日診斷預算用盡仍能留下階段計數。
表格測試含相同緊縮文字、不同金融欄位邊界的反例，仍拒絕接受。

### 合法無工作與服務恢復

23:25 排空服務時，先前選中的兩個舊失敗網址已被正式 owner 在 23:24:40、23:25:19 處理，
並正常設定下一次 deadline。當時沒有到期的缺正文，舊 CLI 卻拋出 `no_due_failed_missing_body_to_probe`。
原失敗 run 與 trace 保留，EXIT 保護已恢復 service／timer；沒有額外來源請求。

新版只處理這一種已驗證的無工作情形：輸出 `skipped_no_due_failed_missing_body`，
明列零 HTTP、零來源修改及 `all_history_complete=false`。無效 provider／範圍／上限仍報錯，
不把拒絕、租約、已存正文或未來 deadline 強制改成可抓。
[正式資料的無工作 CLI 證明](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v54/no_due_live.json)
是在 Fed 沒有此類到期工作時取得，沒有建立第二個 HTTP owner；不是 Fed 全歷史完成證明。

原固定清單中的身分失敗 URL 又做了一次[有界原網址診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v54/original_identity_probe.json)。
該次發生真正傳輸逾時，沒有正文／原始回應可重播；不能據此推定身分錯誤已修好。
精確新聞 service／timer 在 EXIT 恢復，其他下載器、交易、GPU 或金鑰沒有重啟／改動。

### 已有正文與本輪測試

`4968333?exp=a` 在 22:55:16 真正入庫，**1,516 字元**，
[當前 parser、發布時鐘與不可變收據驗證通過](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v53/literal_head_initial.json)。
原 40 網址清單仍是 **23 complete、16 retry、1 article_identity_not_verified**，
見[23:49 固定清單核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v54/body_rechecks_long.json)；此清單核對本身不是耐久正文驗證。

三個失敗編號的正式網址 `5034797`、`5164089`、`5145624` 在台北時間 10 月 9 日 01:56 至 02:04 已取得，
分別有 **1,452／1,054／378 字元**正文。
[三筆原文的當前重播與不可變收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v54/existing_primary_heads.json)
全部通過。這只證明那些正式網址已有可用原文；不偽造查詢變體的回應或 alias 等價關係。
三筆舊 capture fingerprint 均原樣保留，沒有改稱本輪新抓或當前 capture。
三筆的實際原始 envelope 均為 `cnyes_api_content_v1` 批次 API JSON，而非該查詢變體的 HTML；
只能重播其原 API adapter，不能將 API JSON 假扮成 native HTML 以推定剩餘圖文差異已解決。

共享身分重構初版漏傳 byline 區域變數，被 61 個測試攔下且未部署；補齊 frozen identity context 後同一套 **390 項／5.11 秒**通過。
失敗階段及表格反例集中測試 **405 項／6.78 秒**通過，合法無工作與診斷集中測試 **115 項／3.04 秒**通過，
最後完整新聞回歸 **2,153 項／49.15 秒**通過。這是新聞範圍測試，不是全專案測試或來源最高速證明。
下載器保留單一 queue writer；沒有新增重複 collector、全市場掃描或健康文章的診斷工作。

### 正式吞吐與其他來源缺口

[23:16 正式窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v53/runtime_window.json)
在 **118.342 秒**新增鉅亨 **142**、Global Voices **6** 個正文 URL 頭，12 項驗收全部通過。
重啟診斷後的[23:38 正式窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v54/runtime_final.json)
在另一個同 owner **287.022 秒**窗口新增鉅亨 **397**、Global Voices **27** 頭，12 項仍全部通過。
後者鉅亨為 **1.383 新頭／秒、1.888 HTTP／秒**；不是官方最高速，也不以不同工作量的前後窗口主張因果倍速。
[最近 128 批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v54/recent_batches_final.json)
共 **5,967** 個正文版本，檔案、回應與正文 SHA、收據均無錯誤；版本數不等於窗口新 URL 頭。

[17 個已啟用來源的傳輸核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v54/transport_final.json)
通過，該快照中鉅亨有 **4 次等待 headers 的傳輸錯誤、4 次 session reset、20 次 gateway 範圍決策**，
沒有診斷寫入錯誤。23:43 後續 status 已是 8 次傳輸錯誤與 8 次重建 session；不能把短窗口零故障寫成全天正常。
CMoney／PeoPo 真正 403、其他來源的權利／作者／有限範圍缺口仍獨立保留，不拖住正常來源，也不算完成。

同一正式 owner 的[23:49 長窗口驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v54/runtime_long.json)
為 **971.311 秒、鉅亨 1,343 頭、Global Voices 87 頭**，鉅亨 **1.383 新頭／秒、1.969 HTTP／秒**，
12 項檢查全部通過。[該次最近 128 批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v54/recent_batches_long.json)
共 **5,807** 個正文版本無收據／SHA 錯誤；是另一個滾動集合，不能與前一個 5,967 版本直接相減。
[同截點的 17 來源傳輸核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v54/transport_long.json)
仍通過：鉅亨 **16 次等待 headers 的傳輸失敗、16 次 session reset、45 次 gateway 範圍決策**，
診斷寫入錯誤為零。這證明故障有留下證據且健康工作續抓，不表示來源網路錯誤已消失。

長窗口快照的鉅亨容量是 **0.4 秒派發間隔、5 個並行請求**，`recovering_transport`，
其工程並行上限 8 不等於官方允許值或已實測最佳值。控制器只維持一次有界恢復壓力，
不因反覆逾時無限倍增至 256／512 秒；連續健康窗口後才重新驗證容量。
同 owner 的正文解析計數是 1,810 次、63.138 CPU 秒，平均 **34.9 毫秒／次**；
派發等待則為 2,135 次、1,025.596 wall 秒但只有 0.885 CPU 秒。
這些是累計階段計數；多請求等待可重疊，不能將其 wall 秒相加當作整體耗時或 CPU 用量。
未以提高設定值、跳過完整正文／表格驗證或不同來源混合量，宣稱已到官方速度極限。

23:08 核對的 CMoney 官方
[GetStockRssArticles](https://www.cmoney.tw/cm-service/CMService.asmx?op=GetStockRssArticles)回應欄位是日期、標題、來源、URL、標籤、瀏覽數、股票索引與文章 ID；
[GetStockRssNews](https://www.cmoney.tw/cm-service/CMService.asmx?op=GetStockRssNews)同樣是 RSS 名單欄位，沒有全文欄位。
它們不能直接取代被拒絕的論壇全文，也沒有為既有已抓名單新增一條重複管線。
[中央社本機設定](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v53/cna_local_setup.json)
仍為使用者已聲明授權、`CNA_API_KEY` 缺少；此次零 HTTP／零 tool call，不再次詢問已確認的授權，也不以公開 OAuth app ID 代替金鑰。

全來源 goal 仍 active。下一步利用已收到的公開來源與既有原始收據診斷剩餘表示；
來源逾時、未驗證的 alias、未來 deadline、權利與缺少憑證各自保留，不將候選、離線解析或 active 當成全歷史完成。

## 台南來源格式修復與正式續抓

2026-10-11 00:05 的[一致全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v55/inventory.json)
有 **59 個設定來源、17 個已啟用來源、683 個未註冊 hostname 鍵**；
**6,201,907** 個已知 URL、**3,560,190** 個已存正文，**2,130,311** 個 URL 等待來源範圍／授權確認。
來源鍵仍不等於獨立新聞社。該次只讀 WAL 清點使用 19.875 wall 秒／19.588 CPU 秒，沒有 provider 呼叫；
後續驗收只讀有界 URL、批次與增量指標，不重複掃整份清冊。

### 實際來源格式與修復範圍

[三份原始公開 HTTP 200 回應](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v55/tainan_probe.json)
因大小寫不同的 ASP 頁面路徑被誤判為身分未確認。第三篇同時將發稿單位與時間放在一個段落，
其發稿單位為社會局；另外兩篇時間包含日期與時刻間的空白或全形冒號。

共享 `tainan_visible_news` adapter 現在只對已核對的 ASCII ASP namespace 作來源內大小寫比對，
保留原 requested URL，不全域 lowercase，也不接受 Unicode 仿字或編碼路徑當作別名。
`n`／`s` 身分、可見標題、實際正文、可見發稿欄位仍須一致；隱藏署名、圖說欄位、第三方單位、
錯誤日期及重複衝突欄位仍會被拒絕。只拆前段明列的發稿欄位，不把敘事內文改成作者資訊。
原始 ROC 發布時間與修訂字串保留，沒有推測缺少的秒或時區。來源資料 adapter 版本為 2。

來源限定新聞及國際關係處與本次實際核對的社會局；
[市府開放資料使用宣告](https://www.tainan.gov.tw/cp.aspx?n=13612)的署名要求、個資、第三方權利與特別限制仍適用。
沒有把政府網域下的報紙、影像或所有單位概括視為自由授權，也沒有新增訓練匯出或對外全文服務。

| 原文編號 | 名單排序日 | 原文實際發布字串 | 正文字元 |
| --- | --- | --- | ---: |
| 7803187 | 2021-10-03 | `110年10月3日 12：40` | 1,040 |
| 7874525 | 2022-04-07 | `111年02月23日 09:20` | 1,198 |
| 7920395 | 2022-09-24 | `111年9月16日15:55` | 1,026 |

名單排序日不等於發布日；兩種來源值各自保留，不能用名單日期回填精確發布時間。
來源頁的發稿人原始欄位保存在私人來源證據中，不傳到公開進度頁。

### 正式下載與收據驗收

[原始回應 SHA 與當前 parser 回放](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v55/tainan_replayed.json)
3／3 通過後，沿用既有單一 owner、共用下載器及
[獨立補充範圍](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v55/tainan_supplements.json)
排入這三個明列 URL。批准階段沒有 HTTP，也沒有將診斷回應直接冒充新抓取。
正式下載器在 **00:25:25–00:25:27** 重新取得原文並正常寫入 Parquet。
[三筆新頭與原有一筆的當前重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v55/tainan_heads.json)
全部通過；原有一筆保留舊 capture fingerprint，不改成當前新抓。
[清冊完整性驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v55/finite_cohorts_accepted.json)
確認原始 1 URL 清冊沒有改動、補充 3／3 入庫，仍有 **3 個範圍外 URL** 等待核對，沒有宣稱全歷史完成。

### 鉅亨吞吐與其他來源故障

[正式同 owner 窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v55/runtime_accepted.json)
在 122.791 秒新增鉅亨 229、Global Voices 12、台南 3 個正文 URL 頭，12 項工程檢查全部通過。
容量控制器仍使用已驗證的有效正文窗口逐步調整，不以 HTTP 200、304 或名單頁替代已存正文。
台南來源修改不重置其他來源的容量證據，不新增第二個 HTTP owner。
[最近 128 批驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v55/recent_batches.json)
共 6,350 個正文版本無收據／雜湊錯誤；這是滾動抽查範圍，不是窗口新增正文量或全庫完整性。
[17 個已啟用來源的傳輸觀察](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v55/transport_accepted.json)
通過；該次鉅亨有 **5 次等待 headers 失敗、5 次 session reset、9 次獨立 gateway 故障隔離決策**，
無診斷寫入錯誤。這證明故障有恢復／隔離且正常工作繼續，不代表來源網路故障消失。

同程序的[00:33 長窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v55/runtime_final.json)
為 **422.812 秒、鉅亨 653 頭、Global Voices 38 頭、台南 3 頭**，12 項驗收仍通過，
鉅亨 **1.544 新頭／秒、2.130 HTTP／秒**。短窗口的 1.865 新頭／秒不是持續最高速。
[該次最新 128 批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v55/recent_batches_final.json)
有 **6,398** 個正文版本，收據與雜湊無錯誤；前一批集合的 6,350 仍保留，兩者不能相減當作新增量。
[同時段傳輸觀察](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v55/transport_final.json)
核對 **19 次傳輸失敗、19 次 session reset、19 次獨立 gateway 決策**，
其中 17 次等待 headers、2 次讀取正文，診斷／checkpoint 寫入錯誤為零。
當前容量為 0.4 秒派發間隔／7 個並行請求；這是當時已退回的恢復壓力，不是官方最高速。
正式 PID 與 owner 未變、重啟次數為零，記憶體約 **287.4 MiB**；CPU 累計 **155.087 秒**。
這些資源讀數是該次程序快照，不是最大值、匹配工作量效能改善或整臺主機負載。

[民視兩筆公開原頁診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v55/ftv_original_probe.json)
皆 HTTP 200，schema 為社群中心編輯，頁面可見署名為「工商好朋友 報馬仔」，沒有可核對的內文 newsroom 作者。
社群編輯不自動等於原文作者；本輪未更改其 attribution 缺口或將這兩筆計入自有 newsroom 完成量。
CMoney／PeoPo 的拒絕、中央社憑證及未核對的來源範圍也沒有被抹除。

[本機唯讀進度頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v55/dashboard_local/acceptance.json)
於 1440、768、390、320 四種寬度通過：鉅亨／台南卡片對上後端數量、原網址進度分開、無 provider HTTP，
不提供正文或下載器控制。唯讀狀態 GET／HEAD 成功，控制 POST 被 405 拒絕。
[對外 HTTPS 失敗紀錄](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v55/dashboard/acceptance.json)
為 30 秒 timeout；本機驗收不替代 WAN 入口驗收。
全來源目標仍 active，保留原始失敗網址、未來重試時刻與權利／來源缺口，繼續依實際回應逐項修復。

## 範圍外網址分類與持續抓取

### 實際原址與非新聞頁

[00:39 的有界來源診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v56/tainan_remaining_probe.json)
沿用共用 robots、來源限流及唯一 HTTP owner，核對先前剩餘的三個台南網址。
`News_Content.aspx?n=13371&s=7908346` 與小寫路徑變體各自回傳 **HTTP 410／3,338 bytes**；
兩個原 requested URL、名單排序日 **2022-07-22／2022-12-28** 都保留。
沒有將 `n=13371` 猜改成 `n=13370`，也沒有將今天的來源不存在觀測推回歷史發布日。
410 只有已核對的傳輸觀測，不宣稱保留了原始正文或證明全歷史不可恢復。

`cp.aspx?n=13293` 回傳 **HTTP 200／127,807 bytes**，標題是「行政區域」，不是新聞文章。
其名單排序日 **2018-12-20** 保留；私人診斷原始回應、Parquet 與側邊收據的 SHA 均核對，
不把行政區域敘述、HTTP 200 或缺少署名的內容算成新聞全文。

### 共用分類入口與恢復邊界

`NewsQueue.add_articles` 先前先將有限範圍外網址放入 `waiting_cohort`，
使非文章路徑被誤顯示成等待排程。本次將既有來源 namespace 判定放在等待批次之前：
新匯入的非文章路徑直接留下 `unsupported_article_path`，不送 HTTP；
既有 upsert 仍只更新原本允許的名單欄位，不覆寫正文、原始時鐘、狀態或未來重試時刻。
相容的 metadata admission 合約為 **版本 1**，正文／訓練 ABI 沒有變更。

舊清冊的校正使用 `enqueue --reviewed-source-outcomes`，只接受 **1–16 個明列 URL**、
一小時內有時區的來源診斷、匹配的正式傳輸 trace，以及未租用、未存正文、已到期的 `waiting_cohort`。
原始／追加批次成員、已保存的頭、既有失敗、拒絕和未來 deadline 都不能被此入口改寫。
403／429／5xx、重新導向、權利缺口或一般 parser 錯誤不能被偽裝成不存在。

更新前先保留內容定址的原始診斷、非文章原始回應及批准收據；既有同址證據有衝突便拒絕覆寫。
私人來源文件限 10 MiB、收據限 64 KiB，Parquet 必須只有一列／一個 row group，
並在 materialize 前檢查邏輯大小上限。SQLite 更新與 `finite_source_outcomes` 觀測帳在同一交易，
提交時再核對狀態、租約、deadline 和兩種批次成員；任一筆衝突則整批 rollback。

[01:10 正式分類收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v56/tainan_outcomes.json)
為 **2 筆 source_not_found、1 筆 unsupported_article_path、0 HTTP、0 新正文**，沒有擴大來源 scope。
[最終唯讀重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v56/tainan_outcomes_final.json)
驗證不可變 SHA、來源 fingerprint、原 URL、來源觀測時刻與當前 queue 狀態。
HTTP 410 的正文 integrity 明列為 false；非文章 200 另有原始回應 integrity，兩者都不是已取得文章。
[原批次及追加收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v56/finite_cohorts_final.json)
確認原始 **1 URL** 分母與追加 **3 URL** 範圍未變；
[四篇正文的當前 parser 重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v56/tainan_heads.json)
與來源雜湊仍通過，沒有把舊捕捉 fingerprint 改成新抓取。

### 網頁與常態成本

有限清冊進度另列已核對非正文結果，只有當前狀態、來源 fingerprint、觀測時刻及範圍仍匹配的觀測帳列才納入。
原／追加正文的分子、分母保持獨立；校正後頁面顯示 **待核對 0、待排 0**，
另列 **來源不存在 2、非文章路徑 1**，不加到已存正文或全歷史完成量。
非法或互相矛盾的非正文計數不會減掉未知網址；私人文件、原始正文、發稿人資訊和收據路徑不進瀏覽器。

新增 admission 成本隨本次匯入的名單量增加；明列校正最多 16 URL。
常態分類聚合使用 provider 索引與觀測帳，已知量沿用 `state_metrics`，
不在每次刷新重掃約 620 萬 URL、不重讀正文，也不因網頁顯示追加 provider 請求。
唯讀投影合約 `nonbody_source_outcome_progress_contract_version` 為 **1**。

[本機網頁驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v56/dashboard_local/acceptance.json)
於 **1440、768、390、320** 寬度通過：鉅亨與台南卡片對上同一後端快照，
非正文 3 筆與待核對 0 的文字／計數一致，原網址探索仍分開顯示，無 provider 請求、無溢出或 JS 錯誤。
唯讀 GET／HEAD 成功，控制 POST 被 405 拒絕；重新載入公開唯讀 gateway 前後，
期貨／當沖／隔夜服務 PID 分別保持 **208／152／982**，未重啟其服務。
[對外 HTTPS 驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v56/dashboard_wan/acceptance.json)
仍失敗；本機成功不替代 WAN 入口驗收。

### 正式吞吐與驗證範圍

[01:22 的正式窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v56/runtime_accepted.json)
同一 owner 在 **172.841 秒**新增鉅亨 **443**、Global Voices **17** 個已存正文 URL 頭，
鉅亨 **2.563 新頭／秒、3.406 HTTP／秒**，12 項持續驗收全部通過。
請求速率使用獨立、鎖定的 **168.545 秒** metric 窗口，不能與資料庫窗口混算。
當時 controller 為逐步升速，**0.05 秒派發間隔／8 並行**只是工程控制讀數，
不等於來源允許 20 req/s、官方最高速或全部歷史已完成。

[17 個已啟用來源的傳輸核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v56/transport_accepted.json)
通過：鉅亨 **1 次讀取正文失敗、1 次 session reset、8 次 host 隔離決策**，
所有診斷／gateway checkpoint 寫入錯誤為零。502 及串流讀取故障仍存在，但沒有堵住正文續抓或其他健康來源。
來源故障不是正式限速證明；既有有界退避、壞 session 重建、host 級隔離與各 provider 的獨立派發仍保持。

本次程序快照為 **約 284.8 MiB** 記憶體／**166.917 CPU 秒**累計，非峰值或匹配工作量性能改善。
鉅亨正文解析 **529 次／28.831 CPU 秒**，派發等待 **575 次／28.884 wall 秒／0.138 CPU 秒**；
多請求等待可重疊，不能相加當成整體耗時，也不能把等待 wall 秒當作 CPU 用量。
[最近 128 批驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v56/recent_batches.json)
有 **5,172** 個正文版本，收據與雜湊無錯誤；是滾動抽查集合，不能與先前 6,398 版本相減當作增量。

同一 owner 的[01:29 長窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v56/runtime_final.json)
為 **574.609 秒、鉅亨 951 頭、Global Voices 57 頭**，12 項驗收全部通過。
鉅亨 **1.655 新頭／秒、2.317 HTTP／秒**；HTTP 計數窗口獨立為 **570.100 秒**。
此窗口包含後續來源故障，不能只將前段 2.563 新頭／秒當作全程持續值。
[17 來源的長窗口傳輸核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v56/transport_final.json)
通過：鉅亨 **16 次傳輸失敗、16 次 session reset、28 次 host 隔離決策**，
其中 **15 次等待 headers、1 次讀取正文**；診斷及 checkpoint 寫入錯誤仍為零。
控制器在 **0.2 秒派發間隔／6 並行**維持有界恢復壓力，正常窗口後才重新驗證更高容量，
不把不穩定網路當作已證明的正式限速，也不無限倍增等待。
[長窗口最近 128 批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v56/recent_batches_final.json)
有 **5,087** 個正文版本、零收據／雜湊錯誤；不同滾動集合的 5,172 與 5,087 不能相減當作新增量。
PID／owner 未變、非預期重啟為零；該截點記憶體 **約 293.7 MiB**、CPU 累計 **423.359 秒**。
程序資源快照不是整機負載、最高值或匹配工作量性能比較。

最終新聞回歸 **2,174 項／66.81 秒**通過；新聞網頁／身份專屬 **80 項／16.71 秒**通過，JS 語法檢查通過。
共用網頁整份測試為 **124 通過、1 失敗**：FinLab 已將文案更新為「一般表可估範圍（分鐘另列）」，
既有測試仍期待舊文字，不是來源 API 或恢復逾時；該失敗保留，未改動不屬於本次新聞工作的 FinLab 實作／測試。
測試期的一筆 synthetic 時序 hook 與一次空 `-k news` 選擇失敗也保留原工程 run，沒有變成正式成功收據。

全來源 goal 仍 active。59 個設定來源、17 個已啟用來源與 683 個未註冊 hostname 的完整清冊仍沿用前一節，
不以已核對三筆非正文、服務 active 或頁面進度改變宣稱全部來源／全歷史完成。
下一步繼續沿用正式 owner 的有效正文、到期失敗及已保留原始回應修復其他來源；
CMoney／PeoPo 的拒絕、未知原文／作者、中央社實際 API 憑證與未核對授權範圍各自保留。

## 鉅亨廣告排除與只讀監測優化

### 因果分解與原始故障

v57 保留所有來源目標，不增加第二條下載器、不提高來源上限、不縮減正文驗證。
先區分 HTTP 派發等待、來源傳輸、HTML 解析、壓縮／fsync、收據／頭驗證及唯讀監測；
重播沒有真實網路，不能拿 parser 速度當成下載速率。第一性原理的工作單位是**已驗證且持久化的唯一正文頭**，
不是 HTTP 200、設定 parallelism、服務 active、某個失敗狀態消失或已解析的連結數。

[17 個已啟用來源的舊 owner 診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/transport_before.json)
證明鉅亨有 **34 次傳輸失敗／34 次 session reset**，其中 **33 等待 headers、1 讀取正文**；
有 **86 個 host 隔離決策、54 次 host circuit、3 次 transport circuit**，不是統一的官方限速。
diagnostic／checkpoint 寫入錯誤均為零。這些是舊 owner 的累計，不與新 owner 的計數相減。

[原 40 筆失敗清單的當前唯讀核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/body_rechecks.json)
為 **23 complete、15 retry_cooldown、2 article_identity_not_verified**。
`5063045?exp=a` 與 `5034797?exp=a` 身分未確認，不能用同編號另一 URL 已存正文冒充這兩筆成功。
這份狀態核對本身不是正文耐久證明；原 23 篇的正文與收據及後續最近批次驗證分開保留。
既有 raw 診斷 corpus 沿用三份明列報告：v42 `cnyes_native_sources_before.json`、
v48 `current_due_native_failures.json`、v52 `cnyes_current_due_failures.json`，沒有追加 HTTP、猜 ID 或清空當日預算。

### 一次 DOM 走訪，保留完整語義

`downloader/news_fulltext_policy.py` 的 `_native_exclusions` 原本只認單一 `.class`／`#id`；
鉅亨 `[id^="div-gpt-ad-cnyes_news_article_"]` 使每次 existing-node extraction 先 serialize 再 parse。
真實保留回應的 cProfile 觀測到 **132 次 extraction、564 次 BeautifulSoup constructor**；
本次只將非空、引號包住、無跳脫的 ASCII ID prefix 接到原本的非破壞單次走訪。
依 [Soup Sieve 官方 attribute-selector 規則](https://facelessuser.github.io/soupsieve/selectors/basic/#attribute-selectors)，
這個受限形式使用區分大小寫的 `startswith`；空前綴、跳脫、flags、無引號、Unicode、群組與組合 CSS 留在原引擎。
不修改原樹、不併掉被排除節點周圍的 tail、不吃掉字面金融符號／負值、不省略署名、標題、時鐘或身份檢查。

`scripts/benchmark_news_cpu.py` 擴充原有重播，不另建管線：
最多 4 份 explicit probe 報告／16 份連續來源回應，先核對原始 SHA、Parquet／側邊收據與來源 parser proof；
限單列單 row group，檔案／邏輯／raw 大小均有界。有限清冊不能被 benchmark 擴展，JSON API 不能假裝 HTML。
基準只還原**前一版 ID-prefix fallback**，不是拿更早的全部慢路徑放大改善。
temporary queue 使用正式 `fetch_article`、精確 URL claim、大小受限 owner batch、`queue.accept`、壓縮／fsync、收據／頭 SHA 驗證。
前期逐篇 fsync 的重播保留；最後量測已改為與正式設定一致的批次及結束排空，10 篇在一批接受。

[最終七輪固定來源重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/html_prefix_final_benchmark.json)
兩邊同為 **11 個原始回應、10 個正文頭／10 個已驗證版本、1 個 `source_article_empty_template`**，
完整正文、title、author、published／modified raw clock、time basis、method、identity、captions／notices 的比較無差異。
失敗回應、慢輪次、完整量測、設定／實作／來源 SHA 都保留；首輪有 profiler 的 extraction 不混入正常中位數。

| 相同工作階段 | 前版中位秒 | 新版中位秒 | 降低 |
| --- | ---: | ---: | ---: |
| extraction wall | 0.478920 | 0.408268 | 14.75% |
| extraction CPU | 0.479251 | 0.408595 | 14.74% |
| 大小受限耐久鏈路 wall | 1.184145 | 1.075993 | 9.13% |
| 大小受限耐久鏈路 CPU | 0.566333 | 0.476435 | 15.87% |

重跑使用同一工具與三份明列輸入，另選新的 output 路徑，不覆寫本次收據：

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.benchmark_news_cpu --id-prefix-only --repeat 7 \
  --diagnostic-probes artifacts/data_quality/news_goal_stage01_20261009/remaining_v42/cnyes_native_sources_before.json \
  --diagnostic-probes artifacts/data_quality/news_goal_stage01_20261009/remaining_v48/current_due_native_failures.json \
  --diagnostic-probes artifacts/data_quality/news_goal_stage01_20261009/remaining_v52/cnyes_current_due_failures.json \
  --output artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/html_prefix_rerun.json
```

只讀 benchmark 報告合約為 **v3**；正文／來源／訓練 ABI 不變，不搬動舊頭、receipt、deadline、捕捉 fingerprint 或授權。

### 修復監測自身的 false stale 與重複全歷史掃描

[第一次長窗口失敗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/runtime_final.json)
原樣保留：heartbeat 為 18:06:39 UTC，18:06:44 讀入資料庫快照時僅約 5 秒舊；
舊 audit 在歷史聚合結束時才計 age，變成 **58.188 秒**，只有 `status_owner_coherent` 不通過。
同一窗口實際新增 **鉅亨 287／Global Voices 49 頭**，不是 collector 停機；不重啟正式 owner，也不重排失敗文章來掩蓋這筆監測錯誤。

`scripts/audit_news_runtime.py` 升為只讀報告合約 **v6**，保留獨立資料庫／HTTP metric 時鐘。
初讀 heartbeat 在初讀時判斷；完成時另讀 heartbeat、service PID／active／NRestarts／owner UUID／process start clock。
兩端的 **−5 至 45 秒**門檻不變：真正 stale、future clock、停止、重啟、舊程序時鐘或 owner 改變仍拒絕。
報告保留初讀／完成時刻、兩端 age、實作 SHA 與整份 audit 耗時；body receipt 仍需五分鐘內且無錯誤。

計數沿用同一交易的 `stored_body_metrics_version=1`，不將 `complete` 狀態或版本數當成存正文頭；
已有正文但 recheck 失敗的頭仍保留。只針對有頭 provider 的非 complete 狀態，用原 dispatch 索引計精確 recheck；
舊 queue 缺少 v1 表／marker 時明列 full-index fallback，不改寫 queue、不新增 schema 或 owner。
[同一 pinned WAL 的兩輪全索引比較](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/runtime_counts_benchmark.json)
兩邊皆為 **3,569,010 頭／307 個 recheck**，全 provider／state 結果一致、零寫入／零 HTTP。
wall 中位 **31.010 → 4.315 秒**，CPU 中位 **11.743 → 1.269 秒**；
冷／暖輪次原值分別保留（舊 wall 49.947／12.073 秒，新 7.484／1.146 秒）。
這是有界只讀計數效能，不是整個 owner、WAN 下載或全部歷史收據的改善倍數。
共用交易計數不是獨立 lake integrity proof；全來源 inventory 與最近正文／SHA 驗證仍各自執行。

新增 focused 測試涵蓋慢 audit、真 stale／future、service 停止、PID／owner／restart／process clock 改變、
無時區時鐘、cache／legacy oracle、跨 provider／修訂／body 缺失與零資料庫寫入。
focused **114 項／6.86 秒**、最後新聞整份 **2,268 項／79.56 秒**通過；先前 **2,248 項／57.46 秒**證據仍保留。

### 部署、網頁、正式續抓與尚未解決範圍

CPU 版於 **01:56:35**只 graceful restart 新聞服務；PID **1287421**、owner `0e233f1667004637a1bfdfb89b62e0dd`。
timer 停止後恢復，source／authorization／capture fingerprint 和 failed-diagnostic 當日 **2／2**計數均不重置。
共用 gateway／期貨／當沖／隔夜 PID **1198418／208／152／982**保持；本次沒有重啟其他 collector 或交易。
後續修復只是 audit code，正式 source owner 不必再重啟。

[修復後正式長窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/runtime_repaired.json)
全部 **12/12**通過：資料庫 **1080.744 秒**新增鉅亨 **434**、Global Voices **91** 頭；
HTTP 鎖定 counter 窗口獨立為 **1076.695 秒**，鉅亨 **867 次／0.805 HTTP 每秒**，正文 **0.402 新頭每秒**。
初讀 heartbeat **5.578 秒**、完成 **15.151 秒**舊，整份 audit **9.639 秒**，owner／PID 無變／NRestarts 0。
當前 controller 為 **0.5 秒／3 並行 recovering_transport**，0.5 秒不等於來源承諾 2 req/s；
不拿較早 v56 的不同窗口、不同失敗負載當成本次 CPU 修復的下載速度對照。

[最近 128 批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/recent_batches_repaired.json)
有 **4,375** 個正文版本／當前頭 witness、零收據／雜湊錯誤；rolling set 與較早 4,558／4,564 版本不能相減當作新增量。
[17 啟用來源的當前故障／恢復核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/transport_repaired.json)
鉅亨 **7 headers timeout／7 session reset、27 host 隔離決策／23 host circuit**，所有 diagnostic／checkpoint 寫入錯誤零。
同一 owner 還有 **27 次 502、81 個 body_not_found、2 個 identity 未確認**觀測；
失敗公開回應仍按五個階段辨識、保留原 URL／時鐘／重試時刻，不偽造正文、不把空模板與 schema 不一致吞成成功。
傳輸階段可重疊（868 次傳輸累計 1243 wall 秒／3.960 CPU 秒，588 次解析 29.963 wall／22.990 CPU 秒），
不能把並行 wall 累計加總為整體工期或 CPU 負載。

[01:57 全來源 inventory](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/inventory.json)
及 [CSV](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/inventory.csv)、
[端點](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/inventory.endpoints.csv)、
[逐來源計畫](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/plan.json)更新全部 **742 個來源鍵**：
**59 設定／17 啟用／683 未註冊 hostname**、**6,201,941 已知網址／3,568,551 已存正文／2,130,323 待授權網址**。
這次完整 inventory 花 **23.283 wall／23.039 CPU 秒**、零 HTTP；不是每個 dashboard heartbeat 的常態掃描。
來源鍵包含聚合器及轉址，不等於 742 家媒體，更不是全網歷史正文總量。

[本機四尺寸驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/dashboard_local_correct/acceptance.json)
於 **1440／768／390／320**寬度對上每次快照的已存頭、分母、比例及台南非正文分類；
完整 31 頁清冊可達，無 JS 錯誤／橫向溢出／publisher 請求，GET／HEAD 200、控制 POST 405。
先前本次診斷誤用未監聽 **8894**得到 ECONNREFUSED，
[原錯誤收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/dashboard_local/acceptance.json)保留；
沿用實際 **8770**通過，沒有為操作指令錯誤重啟 gateway。這也不替代 WAN 驗收；先前 HTTPS timeout 仍未由本次證明修復。

task 對 wrong-port run 與 false-stale run 留下 `superseded`、原 log／SHA、驗證 run 與收據，並未刪掉失敗或改成來源成功。
全來源 goal 仍 **active**：其餘來源原文／作者／時鐘、schema 失敗、CMoney／PeoPo 拒絕、CNA 授權 API 實際接線和未核對 scope 仍逐項保留。
下一步沿用原 owner 的到期工作與 retained response，修復有可重現證據的 parser／endpoint 缺口、監測恢復後的穩定速率；
不能重置診斷／拒絕預算、猜正文／歷史發布日、將沒有 ready work 當成歷史完成，或以此局部 CPU 修復結束全部來源目標。

### 主排程堵塞的稀疏非正文查詢

前述 CPU 修復後繼續檢查正式程序，並未以一個通過窗口結束。
02:22 遙測的 **96 次 status_write 累計 811.932 wall／673.328 CPU 秒**，正文解析約 23 CPU 秒；
主程序每 15 秒的狀態更新占了主要計算，期間不能派發／接受其他 future 的結果。
[同一資料庫的唯讀 canonical status profile](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/status_before_profile.json)
證明整份寫入 **7.445 wall／7.211 CPU 秒**，其中 12 次非正文結果查詢為 **5.821 wall／5.706 CPU 秒**。
測量使用唯讀 WAL、原 `NewsQueue.status`、當前 metrics 與私人臨時 status 輸出，沒有建新的 owner／鎖或寫入正式狀態。

[實際執行計畫](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/status_query_plan.json)
顯示舊 SQL 先 `SEARCH a USING INDEX dispatch_latest (provider=?)`，
對每筆候選再巡批次成員、查 outcome；資料庫有 **6,201,942 文章列**，但 verified outcome 只有 **3 列**。
原批次成員 **7,983**、追加 **4**；不能把全 frontier 每個 provider 的文章巡過一遍才找這 3 筆。
新版先由 `finite_source_outcomes_provider` 找少量 outcome，再以文章主鍵查目前狀態。
使用 [SQLite 官方明定的 CROSS JOIN join-order 控制](https://www.sqlite.org/optoverview.html#manual_control_of_query_plans_using_cross_join)，
保留原本全部 predicate，沒有做乘積展開或放寬分類。

修改限於 `downloader/news_fulltext_cohorts.py` 的共用 `reviewed_nonbody_progress` 與
`downloader/news_fulltext_queue.py` 的報告標記 `nonbody_scope_progress_query_version=2`。
來源 provider、body 缺失、state／error、原觀測時鐘、source fingerprint、**任一 provider 原／追加批次**的排除仍完整保留。
不新增索引、不更新舊列、不重置 scope；分類／正文／來源捕捉 ABI 仍是原版本，僅查詢計畫和 sideband 標記改變。
新測試逐一變更 provider、body、clock、error、fingerprint、原／追加／其他 provider 批次成員，
與前版 SQL oracle 完全一致；plan 測試確保 outcome 在文章主鍵 lookup 之前，不從完整文章 frontier 起跑。

原 benchmark 增加 `--status-only`，以一個 pinned WAL 和固定讀取時鐘重播**整份 canonical status + JSON 原子寫入**，
參考端只換回舊 JOIN；沒有略去 disabled provider、有限清冊、未知來源、錯誤、scope、deadline、進度、ETA 或長表。
全輸出 JSON SHA 比較，不只比對總數；隔離 process 的時鐘與 SQL 在例外後還原，正式 status bytes／queue settings 不變。
[七輪正式大小狀態重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/status_canonical_benchmark.json)
為 **742 來源鍵、6,201,942 網址、3,569,746 頭**，全欄位一致、零 source HTTP／queue 寫入／正式 status 覆寫。
台南仍是 **2 source_not_found／1 unsupported_article_path／0 新正文**，沒有把非正文算進完成量。

| 整份相同 status 工作 | 前版中位秒 | 新版中位秒 | 降低 |
| --- | ---: | ---: | ---: |
| wall（含臨時 JSON 寫入） | 6.940453 | 1.058901 | 84.74% |
| CPU | 6.865296 | 1.045548 | 84.77% |

所有輪次／慢例保留；這是固定資料量整份狀態更新改善，不是正式 HTTP、整機 CPU 或來源官方速度上限。
**15 秒刷新不變**，不以少更新／少顯示換速度。重播命令（另選 output，不覆寫收據）：

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.benchmark_news_cpu --status-only --repeat 7 \
  --output artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/status_canonical_rerun.json
```

針對性 **294 項／5.94 秒**、含時鐘／SQL 例外還原的 **253 項／3.95 秒**通過。
最後 `test/test_news_*.py test/test_cnyes_news_bulk.py` **2,349 項／73.30 秒**通過，先前所有回歸證據仍保留。

02:39:09 新聞服務第二次 graceful reload，新的 PID **1445944**、owner `6bbfeca0bbfc480e8e6ca61228d926d8`。
60 秒就緒檢查曾逾時，沒有標成成功、再重啟或略過 recovery；
實際初始化 **168.198 wall／54.883 CPU 秒**，其中 acceptance recovery **119.648 wall／36.860 CPU 秒**。
這是此次冷讀取仍未解決的啟動耗時，不能以查詢已加速聲稱冷啟動已最佳化。
02:42:42 正式 status 已為 running／query marker 2；
[02:44 readback](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/runtime_status_baseline.json)
核對 actual PID／DB owner／process clock、已載入 source code；日 diagnostic 仍 **2／2**，gateway／交易 PID 未變。
原 timeout run 留 log／SHA，以後續實際 readback 收據 superseded，不提高 liveness 的 45 秒門檻。

[第二版網頁驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/dashboard_status_local/acceptance.json)
四尺寸都對上進度，已存頭隨刷新 **3,569,962 → 3,570,035**，來源分母為當時 **6,201,943**；
無 JS／溢出／publisher 請求，GET／HEAD 200、控制 POST 405。
這是連動的新狀態證據，尚不替代對外 HTTPS、全部歷史及正式持續 throughput 驗收。

[第二版正式持續窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/runtime_status_final.json)
以獨立 **129.556 秒 DB／130.396 秒 HTTP**時鐘得到 **113 鉅亨／16 Global Voices**新頭，全部 **12/12**通過。
鉅亨為 **0.872 新頭／秒、0.943 HTTP／秒**；初讀 heartbeat **13.941 秒**、完成 **8.691 秒**舊，audit **9.770 秒**。
來源這個 counter 窗口仍有 **4 次 502、6 次 timeout**；
[該 owner 累計恢復證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/transport_status_final.json)
為 **8 次傳輸失敗／8 次重建 session、7 host 決策／5 circuit**，17 個啟用來源核對通過、診斷／checkpoint 寫入錯誤零。
窗口與 owner 累計範圍不同，不能相減／混算；沒有聲稱源端錯誤被完全修好。
[最新 128 批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/recent_batches_status_final.json)
有 **2,851** 個正文版本／零收據／SHA 錯誤；這個 rolling set 不能與先前 4,375 相減當作新增量。
02:47:41 遙測為 **20 次 status_write／28.202 wall／18.743 CPU 秒**，平均 **0.937 CPU 秒／次**；
含第一次冷 I/O 的最大 wall **10.982 秒**仍保留，沒有只列最快樣本。
兩個正式窗口的資料、source refusal／header failure、冷 cache 與初始化負載不同；只讀七輪匹配輸出驗證才是 84.77% 改善的依據。

[02:50 的同 owner 長窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/runtime_status_long.json)
仍 **12/12**通過，**388.707 秒**新增鉅亨 **310**、Global Voices **40** 頭；
counter 窗口獨立為 **391.441 秒**，鉅亨 **474 requests／1.211 HTTP 每秒、0.798 新頭每秒**。
這個窗口包括 **15 次 502、12 次傳輸失敗**，比只取較早 130 秒的速率更完整，但仍不證明最高速。
[17 啟用來源的長窗口恢復證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/transport_status_long.json)
該 owner 累計 **13 次傳輸失敗／13 次 session reset、19 次 host 決策（18 個 isolated errors／12 次 circuit）**，
所有 diagnostic／checkpoint 寫入錯誤仍為零。
[長窗口最近 128 批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v57/recent_batches_status_long.json)
核對 **2,873** 個正文版本／零收據／SHA 錯誤。PID **1445944**、owner UUID 不變、NRestarts 0；
初讀 heartbeat **12.138 秒**、完成 **4.903 秒**舊，audit **7.922 秒**。
全來源 goal 仍 active；下一步以實際保留回應修復來源 schema／identity 與 endpoint 缺口，
並在不略過收據／孤兒恢復正確性的前提下分析啟動冷 I/O。源端拒絕／逾時、未知歷史與未核對來源仍不算完成。

## 完整收據解碼與實際初始化取樣

v58 的入口仍是正式 `NewsQueue.recover_acceptance()`，沒有另外開下載器或把受阻文章標成成功。
固定 WAL 的[全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v58/inventory.json)、
[逐來源 CSV](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v58/inventory.csv)、
[完整端點](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v58/inventory.endpoints.csv)及
[742 列計畫](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v58/plan.json)保留：
59 個設定來源、17 個已啟用來源、683 個未註冊鍵，當時 6,201,943 URL／3,571,295 正文頭。
鉅亨當時 3,461,378 頭、127,981 個缺正文工作、397 個已有正文重查。
清冊是一次性的 RO 掃描（45.965 wall／25.532 CPU 秒），沒有塞回每 15 秒的狀態迴圈。

### 採用：重用既有完整 JSON decoder

`stockagent/private_json.py` 已有可選原生解碼及 stdlib fallback；本次重用它，沒有新增套件或改寫 JSON encoder。
原始 `read_text()`、每份 JSON 的全部欄位、malformed 尾端、重複鍵、Unicode／孤立 surrogate、
大整數、負零、NaN／Infinity／overflow 的既有行為和原順序均有測試；缺套件也能走 stdlib。
已提交收據仍讀取完整 JSON；未提交／重複收據仍查既有 path，孤兒仍驗 SHA、contract、列數並串行提交。
新增 `acceptance_receipt_decode_version=2` 是狀態 sideband，不重寫來源資料、發布日期、body fingerprint 或 receipt ABI。
本次亦把共用 decoder 檔案加入 runtime 的程式身分／owner 啟動後變更檢查。
原生解碼的產品能力參考[官方原始碼與文件入口](https://github.com/msgspec/msgspec)；本報告的改善比例來自本機實測，並非引用該專案宣傳倍數。

[交付版 8,192 份、三輪 ABBA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v58/receipt_decode_delivery_benchmark.json)
實際共 3,014,263 JSON bytes、msgspec 0.22.0；全欄位語意 SHA 相同，原文件與程式 SHA 未變、SQLite changes／HTTP 為零。
完整固定收據恢復 wall 中位 **0.333096 → 0.286984 秒（−13.84%）**、CPU **0.333053 → 0.286955 秒（−13.84%）**。
[較早相同 8,192 份測試](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v58/receipt_decode_final_benchmark.json)
的 **−6.73% wall／−6.34% CPU**及[4,096 份測試](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v58/receipt_decode_benchmark.json)
也保留。可說本機固定暖批次改善約 6–14%，不能挑最佳輪次當整個冷啟動或來源 HTTP 提升。

### 不採用：常態 receipt thread pool

實際 owner 初始化期間的[30 秒 stack 原證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v58/startup_stack.raw)
共 749 個樣本，706 個含 `read_text`、13 個含 private decoder；含 idle 的 stack 樣本不是精確 CPU／wall 歸因。
取樣沒有讀 locals／憑證／正文，也沒有改變來源節流或全系統快取。
該次實際初始化 62.511 wall／25.819 CPU 秒，收據恢復 55.010 wall／19.682 CPU 秒；
和上一輪 168 秒有不同 cache／負載，**不把這個差值全歸給解碼優化**。

只讀實驗仍呼叫原來的完整恢復：reader 只預取文字，最多八份／每份一 Mi 字元；
超長原件回到 owner 完整讀取，不截斷、不拒絕合法舊件。JSON／SQLite／digest／列數／commit 全留在 owner，
先前 valid orphan 在後續壞收據前仍提交；錯誤與 reader 關閉、原函數恢復均有測試。
測後已將候選移到既有 benchmark；正式 `recover_acceptance()` 沒有 thread pool／read-ahead 設定。

| 同一 8,192 份、兩輪 ABBA | 串行 wall／CPU 中位 | 兩個 reader wall／CPU 中位 | 決策 |
| --- | --- | --- | --- |
| [暖快取](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v58/receipt_io_final_warm.json) | 0.3460／0.3457 秒 | 1.8247／2.2769 秒 | wall 慢 5.27 倍、CPU 6.59 倍，不啟用 |
| [逐收據 DONTNEED 提示](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v58/receipt_io_final_page_advice.json) | 2.5817／0.9279 秒 | 2.1078／2.1576 秒 | wall 快 18.36%，CPU 2.33 倍，不作常態預設 |

第二種只對同一組新聞收據提出 Linux page advice、未動其他來源／global drop_caches；
核心可忽略提示，不能證明完整 cold boot。所有原輸入 SHA、慢輪次及原較早原型結果都保留。
最後[完整新聞／批次回歸](../artifacts/operations/agent-workflow/runs/v58-final-serial-news-regression-20261010T191708-6d98bad3/run.json)
**2,385 項／64.38 秒**通過。沒有用加倍 CPU、少讀原件、放鬆驗證或縮小來源範圍換取吞吐。

可重測的既有入口（輸出使用新的私人路徑，別覆寫上述基線）：

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.benchmark_news_acceptance_recovery \
  --json-decode-only --maximum-receipts 8192 --cycles 3 --output PRIVATE_NEW_JSON_PATH
run_fintech_python -m scripts.benchmark_news_acceptance_recovery \
  --read-ahead-workers 2 --maximum-receipts 8192 --cycles 2 --output PRIVATE_NEW_IO_PATH
```

### 故障邊界與續抓

鉅亨仍有真實 502／headers timeout、source schema／財務表格／caption 不一致及身分缺證。
native failure stage 已由 extractor 分類；缺少新的已驗證 raw witness 時，不放鬆對正文、作者與時鐘的檢查。
CMoney／PeoPo 403、CMNews 付費／舊址、民視作者／第三方轉載、Fed landing／image-only、
Global Voices image／embed 等都在全清冊分別列出。這些不是都已修復，也不是都無法修復。
UTC 日 cnyes 的原 `failure_diagnostic_attempts:v1` **2/2** 未清除或改名，沒有強迫重試、擴大 finite scope 或額外 HTTP。
服務活動、已寫設定、沒有 ready work 均不等於全部歷史完成；全來源 goal 仍 active。

### 新 owner 整鏈路驗收

最後[新聞專屬切換](../artifacts/operations/agent-workflow/runs/v58-final-news-only-cutover-20261010T192005-4325fcba/run.json)
後 PID **1538146**、owner **677800965dd64d3ea04297a14f85bb52**、NRestarts **0**、service／timer active。
初始化 **68.930 wall／27.524 CPU 秒**，收據恢復 **56.539 wall／20.713 CPU 秒**；沒有略過原件或啟用預取。
[03:24 的同 owner 驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v58/runtime_final_window.json)
全部 **12/12** 通過，資料庫窗口 **94.236 秒**新增鉅亨 **134**、Global Voices **12** 頭。
鉅亨為 **1.422 新頭／秒**；HTTP counter 採另一個獨立 metrics clock，約 **1.853 HTTP／秒**，
其 counter 新頭 94 不等同 SQLite 兩個查詢快照的 134，不能混用 clock 作速度／完成時間推算。
這是有 6 個 HTTP 502／22 個 body-not-found 的短窗口續抓證據，不是官方最高速、全天穩定或全歷史完成。

[最新 128 批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v58/recent_batches_final.json)
驗 **4,123** 個正文版本／零 SHA／收據錯誤；滾動批次不與另一份不同集合相減當新增量。
[17 個已啟用來源的恢復核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v58/transport_final_window.json)
通過，鉅亨 **9 個 host 決策／7 個 isolated gateway errors／4 次 circuit**，
本 owner 該快照未見 headers timeout，不以製造錯誤重測；較早 owner 真實 session resets 證據原樣保留。
diagnostic／checkpoint／access 寫入錯誤為零，UTC 日 2/2 診斷值不變。

[本機四尺寸進度驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v58/dashboard_final_local/acceptance.json)
通過，1440／768／390／320、742 來源鍵可完整分頁，GET／HEAD 200、控制 POST 405，
數值對上當次 backend，無 JS／overflow／來源 HTTP。這是 `127.0.0.1:8770/news/` 證據，
本輪沒有重新證明 WAN HTTPS，先前對外 timeout 仍保留。
其他 public dashboard **1198418**、Shioaji dashboard **208**、day-trade **152**、overnight **982** PID 均不變。

## 故障主機有界退避與成功即恢復

### v59：真正慢在哪裡

這輪先重播既有鉅亨私人失敗原件，沒有發現尚未覆蓋的新 parser witness：可修的既有原件仍可解析，
真正空模板仍保留。民視原件中也有中央社／今周刊等第三方轉載，不能為了提高成功率刪除署名與來源限制。
因此不先改正文、作者、發布時鐘或憑空切換來源，而修復現在有證據的等待／恢復成本。

[修前傳輸觀測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v59/transport_before.json)
在舊 owner 保留 45 個 gateway 範圍決策、5 次傳輸失敗／session reset，
43 次主機隔離錯誤／28 次主機 circuit。AMP 的 robots 回應持續 502，但原來每次 pause 都固定 30 秒。
晚到的同批失敗可再次推遲 deadline；完整成功雖清掉 failure streak，舊 pause 卻沒刪除。
這會浪費請求、佔住下載工作槽，且真正恢復後仍等待，不能靠一味增加並行或降低全域 interval 修復。

### 共用修復與不變的邊界

- `downloader/news_fulltext_host_circuit.py` 沿用同 publisher 的其他健康主機證據，
  只有 **502／504 且沒有 Retry-After**才可能隔離故障主機；未知／相關多主機故障仍保留來源級保護。
- 每次實際失敗的恢復探測將故障主機退避到 **30 → 60 → 120 → 240 → 300 秒**，上限五分鐘。
  同批晚到的失敗不延長已開的 pause、不增加探測代數；健康主機仍依原限速獨立派送。
- 完整成功回應立即移除該主機 pause／代數，寫入既有 durable checkpoint。
  正常成功路徑不增加磁碟 I/O；若恢復 checkpoint 寫入失敗，保留計數與舊盤證據，但不丟棄已成功取得的原回應或重抓。
- checkpoint 格式升至 **v3**，保留原 v1／v2 尚未到期的合法 deadline，不虛構其歷史失敗代數。
  v3 到期後可再探測，但不把「時間到了」當健康證據；重啟也不清零失敗代數或恢復舊成功證據。
- `downloader/news_fulltext_http.py` 的個別 retry 時鐘對齊主機 deadline，成功解除與寫入錯誤都有計數。
  `scripts/audit_news_transport_observations.py` 報告升至 **v4**，核對 v3 的開始時間／期限／代數／有界秒數。
  診斷 metadata 不含正文、完整 URL、headers、金鑰；原 source/body/capture ABI 與 fingerprint 不變。
- 401／403／429／451、503、challenge、任何 Retry-After、robots、來源配額與 UTC 日原 **2/2** 失敗原件預算均未放鬆。
  沒有 force retry、重設診斷日、增加 publisher 請求、另開重複管線或修改其他下載器。

這是所有共用新聞 HTTP owner 可用的恢復修復，不代表每個來源當前都發生符合條件的 gateway 故障，
更不表示能修復源站伺服器或用其他路由繞過拒絕。

### 測試、正式載入與可證明的效果

[最後專屬測試](../artifacts/operations/agent-workflow/runs/v59-gateway-final-focused-20261010T194357-7f8a07ed/run.json)
**171 項／5.94 秒**通過；包括重啟、過期後探測、成功即解鎖、late response、畸形 checkpoint、
慢 fsync 時鐘、故障 checkpoint I/O，以及健康成功不增加寫入。
控制測試的同一 **660 秒**持續主機失敗窗，固定 30 秒基線須探測 **24 次**，新版 **7 次**，
健康主機 **660 次**檢查均未被擋。這是排程控制測試，不是實際來源下載的倍數改善。

[全部新聞與鉅亨批次回歸](../artifacts/operations/agent-workflow/runs/v59-all-news-regression-20261010T194358-fa382876/run.json)
**2,411 項／69.64 秒**通過；[來源審查／清冊回歸](../artifacts/operations/agent-workflow/runs/v59-review-policy-tests-20261010T194543-7f8022a6/run.json)
另 **89 項／3.63 秒**通過。專屬測試包含在新聞回歸中，不相加宣稱唯一測試數。
僅一次[新聞服務載入](../artifacts/operations/agent-workflow/runs/v59-news-only-cutover-20261010T194542-ca5b8dd1/run.json)，
新 PID **1597536**／owner **a2506f7183d54c0095363f4f6bbc4a7f**、NRestarts **0**；其他已核對的 collector／gateway／交易 PID 不變。

[03:52 的同 owner 資料與運行驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v59/runtime_final_window.json)
全部 **12/12** 通過，SQLite 窗口 **226.674553 秒**新增鉅亨 **382**、Global Voices **19** 個正文頭。
鉅亨約 **1.685 新頭／秒**；另一個 metrics clock 窗口 **227.495104 秒**為約 **2.202 HTTP／秒**，不混用分母。
該窗口仍有 **10 個 502、2 次傳輸失敗、14 個 body-not-found**，不是所有錯誤已消失。
各輪工作集合／錯誤負載不同，不把上一輪 1.422 新頭／秒與此輪相除當修復加速比或理論最高速。

[03:51 的 17 個已啟用來源傳輸核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v59/transport_final_window.json)
通過：鉅亨新 owner 有 **16 個 gateway 決策、8 次傳輸失敗／8 次 session reset、1 次主機自動恢復**；
AMP 已實際開 **30／60／120／240 秒** pause，主站另開 30 秒。checkpoint／diagnostic 寫入錯誤為零。
8 次是 owner 累積，與前述資料窗口新增 2 次不是同一計量窗口。未看到其他 provider 新傳輸失敗不等於其全部資料已完整。

[最新 128 批私人正文驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v59/recent_batches_loaded.json)
核對 **6,172 個正文版本／1,122,268,907 bytes 原回應**，零 SHA／收據錯誤。
這是最近不可變版本集合，不將兩次不同 rolling cohort 相減當新增篇數，也不是全庫全歷史核驗。
[本機進度頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v59/dashboard_local/acceptance.json)
四尺寸 **1440／768／390／320**通過，742 列／31 頁可完整訪問，GET／HEAD 200、控制 POST 405，
數值對上當次 backend，沒有 JS error、頁面 overflow、資源失敗或瀏覽器 publisher HTTP。
只有 `127.0.0.1:8770/news/` 的本機證據，先前 WAN HTTPS timeout 仍未重新證明恢復。

### 全來源繼續抓與仍缺的證據

[03:45 的全庫固定快照清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v59/inventory.json)
保留 **742 來源鍵／59 設定／17 啟用**、**6,201,978 已知 URL／3,575,119 已存正文頭**、
**2,130,338 個待授權 URL**。鉅亨當時 **3,464,966 已存頭／124,290 缺正文工作／342 已存重查**。
這是切換前的 pinned read-only coverage 快照，不是新 owner 全庫驗收；來源鍵含聚合與 redirector，不是 742 個獨立媒體或全部網路歷史。
[742 列計畫](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v59/plan.json)保持全範圍，沒有縮成只抓鉅亨。

仍需依真實原件處理鉅亨 schema／圖說一致性及作者／身分缺證、民視轉載作者、來源空模板／404、
CMoney／PeoPo 403、付費頁、Fed 圖像 PDF／landing，以及 Global Voices 圖像／embed。
這些維持分列、retry/due 與私人證據，不能用摘要冒充正文、關掉驗證或把零 ready work 算全歷史完成。
中央社的既有使用者授權確認保留；[只讀 API 設定核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v59/cna_setup.json)
仍為 `credential_missing`，沒有對來源發網路請求或輸出任何 token，需已授權 endpoint／credential 才能接入。

另外補了 source review，不擴大執行授權：[Newtalk 官方條款](https://newtalk.tw/terms)要求擷取前通知並取得同意，
716 個已知 URL 留在 `permission_required`；[TaiSounds 條款入口](https://static.taisounds.news/termsofservice)
的網頁閱讀工具 502、Upmedia 的公開條款未核實，分別保留 459／142 個 URL 在 `review_incomplete`。
網頁閱讀工具失敗不是 canonical collector 的實測失敗、資料不存在或來源拒絕；沒有據此虛構 API、刪除工作或修改來源權限。
全來源 goal 仍 active。下一階段以新原件／端點／正文覆蓋證據修復尚未解析的問題，保持正式服務續抓與跨 provider 獨立觀測。

## 新政府來源的原件解析與全來源續抓驗證

### v60：區分閱讀工具、來源傳輸、正文解析與保存

新的既有來源 `unregistered:www.gov.mo` 只有一個勞動市場新聞 URL，不是新增猜測 ID 或另開爬蟲。
[官方使用條款](https://www.gov.mo/zh-hant/terms-of-use/)允許註明來源與取得日期的非商業原文複製，
但排除第三方／特別限制作品；修改、商業用途及公開非首頁連結有另外的授權要求。
本輪只接私有、已審閱的勞工事務局 DSAL/entity-301 既有原文，不下載媒體、不公開正文或文章深連結、不啟用訓練匯出。
`.gov.mo`／`.gov.cn` 的來源分類不是所有部門、第三方文章、訓練用途或批次全歷史的授權判決。

網頁閱讀工具曾逾時，但[canonical 診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v60/macau_probe.json)
的 robots／正文皆 200；正文回應 **0.472 秒**，實際整個流程 **3.558 秒**含來源 pacing／DNS／解析。
原共用候選 selector 回報 `body_not_found`，根因不是不可訪問，而是未接上當前原生 DOM。
單次回應速度不是官方上限；後來正式正文回應 **4.700 秒**，不能把不同網路窗口宣稱固定倍數加速。

`downloader/news_fulltext_taiwan_pages.py` 的 `macau_visible_news` 沿用共用 `fetch_article`／HTTP／queue／store：

- 同時核對當前 canonical、og URL、可見標題、當前 NewsArticle 身分、原生部門連結和署名。
- 僅准已審閱的原生 HTTPS `/zh-hans/news/<id>/`；不猜另一語系或別名，也不把跳到另一則有效新聞算原 URL 成功。
- 原生直屬 `article` 保存完整段落、表格原值、可見原始圖片說明；排除圖庫 UI、計數圖片及控制按鈕，不取得媒體。
- 可見欄位或正文被隱藏、第三方／其他未審閱部門、付費旗標、標題／身分不符和時鐘衝突仍拒絕，不退回側欄日期或隱藏 schema 全文。
- 可見時間是無時區的 `2022-04-29 10:17:00`，schema 為同一牆鐘加 `+00:00`，沒有證據可確定可見時區。
  保存原無時區字串及明確的 `no_timezone_inferred` basis；完整 schema 原件保留，不改寫成 UTC／澳門時間，也不用 metadata 日期當發布時間。
- 主 policy 必須是該 provider／domain／具證據 license、finite、要求原生容器與署名；共用來源／正文／capture ABI 與鉅亨 fingerprint 不變。

[同一原件的正式 policy 離線重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v60/macau_main_replay.json)
核對 **1,702 字、4,626 bytes**、部門署名與原發布時鐘，沒有追加 HTTP。
正式 owner 隨後透過既有 metadata 遷移與 finite 建立路徑凍結 **1 個 URL**，實際取得 **1 個正文頭**；
原請求／final URL 一致，重新解析正式 Parquet 原件的正文 SHA 與 raw SHA 都一致，取得日期保留。
[該正文所在的既有不可變批次核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v60/macau_durable_receipt.json)
包含 34 個版本，全數無錯；其中只有 1 個是澳門來源，不把整批當該 provider 的篇數。
此前診斷原件不是正式文章 acceptance，所以正式的一次正文取得不是重複建立已存 queue 頭或另開管線。

### 回歸、正式運行與進度頁

[完整新聞回歸](../artifacts/operations/agent-workflow/runs/v60-news-regression-complete-20261010T201724-b50f9af0/run.json)
**2,452 項／73.47 秒**通過，包含原生 identity／隱藏內容／來源署名／raw clock／redirect／policy 邊界。
[共用 gateway 回歸](../artifacts/operations/agent-workflow/runs/v60-shared-dashboard-current-con-20261010T201816-b104042a/run.json)
**125 項／32.09 秒**通過。原聯合回歸僅有 FinLab 的舊精確文案 assertion 失敗，隔離重現後更新為既有前端的
「一般表可估範圍（分鐘另列）」；保留一般表與分鐘分母區別，沒有改 FinLab 生產程式或放寬為模糊匹配。
本輪本機診斷 config 必填、錯測試路徑、decorator、registry base／main config 和 inventory CLI 的失敗記錄都保留，
並以後續成功 canonical run 登錄 `superseded`，不把工具啟動錯誤冒充來源拒絕。

catalog 的變動觸發既有新聞 owner 自動安全重載，沒有清除 queue、收據、故障主機 checkpoint 或每日 raw 診斷預算。
新 owner `1555063f712b4c5abc30491b15cd56ce`／PID **1646551** 在[正式同 owner 驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v60/runtime_final.json)
通過 **12/12**：程式／provider policy／鎖定時鐘／來源獨立進度／限速與收據一致。
資料庫 **133.059327 秒**窗口新增鉅亨 **232**、Global Voices **11** 個新頭；
鉅亨 **1.7436 頭／秒**，獨立 HTTP 計數 **138.141667 秒／384 請求，2.7798 請求／秒**。
該窗有 **2 次 502、3 次 transport error、34 次 body_not_found**，沒有將失敗的 HTTP 或未驗證正文算有效吞吐。
這是 current-owner 真實進度，不是同來源匹配前後基準、理論速度極限或全來源完成時間保證。

[最近 128 批核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v60/recent_receipts.json)
**6,746 版本／1,228,525,722 bytes 原件**、正文／raw SHA、current heads 與收據全部無錯；不是全庫歷史完整性證據。
[18 來源傳輸觀測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v60/transport.json)通過，
該觀測快照鉅亨 **4 次 headers phase 失敗與 4 次 session reset、1 個故障主機範圍決策**均核對，診斷／checkpoint 無寫入錯誤；
當時沒有新增主機恢復，不能把 pause 到期或 owner 重開當主機已健康。

[本機 UI 驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v60/dashboard/acceptance.json)
**1440／768／390／320**、742 列來源頁、鉅亨／澳門 provider card、中央社授權 API 尚待設定狀態與原 URL 進度核對通過。
唯讀 gateway 重載後 PID **1198418 → 1664320**，澳門來源顯示 `government_origin`／`finite_complete`，只表示已審閱的 1/1 cohort。
新聞 owner **1646551** 和 TAIFEX dashboard／當沖／隔夜 PID **208／152／982**未再次重啟。
本機對 WAN HTTPS 的 3 秒 connect 檢查仍逾時（HTTP 000），與本機 backend／頁面 acceptance 分開；沒有宣稱對外服務恢復。

### 全來源範圍與下一階段

[04:16:50 pinned WAL 全清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v60/providers.json)
保留 **742 來源鍵／60 設定／18 啟用**、**6,201,979 已知 URL／3,578,355 已存正文頭**、
**2,130,337 待授權 URL**。只做一次有界全清冊（wall **43.941 秒**／CPU **33.039 秒**），不在熱排程反覆掃全庫。
[742 列計畫](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v60/plan.json)顯示 **705 未審閱來源**，
這不是 705 個證實無法取得的網站，也不是隱藏／刪除工作。
全來源 goal 仍 active，延續既有來源缺證、空模板、404／403、第三方／付費／圖像與授權 API 工作清單；
以原件／合法端點修復可驗證的問題，不擴大原 finite cohort、不取消拒絕／robots／官方配額，不用假正文抹掉錯誤。

## 來源審閱原因與正式續抓分離

2026-10-11 04:49，v61。不是因為頁面可讀、私人用途或其他 provider 授權就推斷所有新聞可批次保存。
本輪核對真正適用的主站條款，再讓清冊與進度頁沿用同一原因；沒有另外開抓取管線。

### 原因、範圍與可修復邊界

[UDN 主要著作權聲明](https://udn.com/page/topic/495)明列下載／重製的授權要求及法令例外，
另列第三方權利人的書面許可與軟體／ML／AI 使用需事前書面同意。
不是把博客、課程或其他平台條款當成全新聞規則。現有 **169,300 URL**全保留；canonical adapter 不因此啟用。
footer 的[新聞授權連結](https://udndata.com/udnauthority.html)在當次 web reader 回 404，
只記該連結結果，不推論全部付費授權／歷史服務不存在。
[MoneyDJ 主站使用條款](https://www.moneydj.com/z/ABMoneydj/aboutTermsService.djhtm)第五節明列自動擷取需公司同意，
第六節另列其定義 AI 用途與大量自動擷取限制。原論壇／會員權益證據不適用整個新聞網站；
將 **48,033 URL**的原因由 `review_incomplete` 改為 `permission_required`，沒有刪除待辦或冒充 HTTP failure。
上述均為現行來源聲明的工程分類，不是對使用者個別法定權利作法律判決，也沒有推斷某人的實際授權。
MoneyLink 的信用卡／產品退費條款仍不能證明全文取得權限；既有工作與審閱缺口保留。

兩份鉅亨當日 diagnostics 沒有可用 `raw_response_body`；它們是 metadata-only 診斷，不是可供重新解析的 HTML。
本機檢視時錯把 `None` 丟給 parser 的 TypeError 是檢視指令錯誤，不是生產下載器或來源新故障。
保留當日 **2 次**自動診斷上限、既有原件、來源身分／署名核對及不明結果；沒有補造原件或重置配額。

### 共用實作與低成本唯讀頁

`stockagent/data/news_source_catalog.py`新增共用、最多 2 MiB 的 local JSON 審閱 loader，
驗證版本／枚舉／alias；精確 provider review 優先，路徑必須在 catalog 目錄且不得藉 symlink 讀 `.env` 或外部檔案。
`scripts/plan_news_sources.py`沿用這個 loader，不再維護第二份驗證／alias 合併邏輯。
catalog 的 `planning_review_ledger` 是規劃觀測指標；`configuration_revision`不加入 review 檔的內容／mtime，
而 `news_snapshot_revision`加入該檔 generation，所以後續審閱更新不重啟下載 owner。
public backend 只投影原因枚舉和有時區、非未來的審閱時鐘；不傳 observation、金鑰或 evidence URL 原文。
壞檔／缺檔只讓審閱顯示 unavailable，不讓成功進度消失或把缺證變成零；review 永不自動授權或撤銷既有授權。
各列原因以一次 `Counter`彙總；UI 沿用既有 lazy renderer／24 列分頁，只讀 bounded status/config，不掃正文或開 queue writer。

`services/news_dashboard`新增來源審閱／核對時間和「需來源同意」原生篩選，與 HTTP、解析、停止、ETA 分開。
[重載後四尺寸驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v61/dashboard_after_reload/acceptance.json)
核對 **1440／768／390／320**、全部 **31 頁／742 列**、11 來源篩選後回到 All、原 URL 進度與中央社 API 尚未開始抓取。
GET／HEAD **200**、控制 POST **405**；0 JS error／publisher request／失敗資源，窄頁無水平溢出，實際截圖已檢視。
沒有引入新 framework、公開正文或推估未測的配額。

### 真實交接、錯誤與驗收

[完整新聞回歸](../artifacts/operations/agent-workflow/runs/v61-news-regression-20261010T203748-9085a29a/run.json)
**2,475 項／52.52 秒**，[最後受影響測試](../artifacts/operations/agent-workflow/runs/v61-source-review-final-focused-20261010T204006-ecbc4e9b/run.json)
**141 項／15.68 秒**，[共用 gateway 回歸](../artifacts/operations/agent-workflow/runs/v61-public-gateway-regression-20261010T204101-bef4e6e4/run.json)
**125 項／27.42 秒**。第一個新測試錯把沒有 inflight 的待辦期待成 `running`，修為實際 `backfilling`；
失敗 log 保留並以後續成功 run supersede，沒有改生產 queue state 迎合測試。

只重載 gateway **1664320 → 1712254**，並讓新聞服務 **1646551 → 1717196**正常排空交接。
交接早期仍讀到舊 owner 的 stopped 快照，[早期 runtime 觀測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v61/runtime_reloaded_start.json)
的 process clock 正確未通過；[早期 transport](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v61/transport.json)也因 status／queue owner 不一致拒絕。
沒有拿 `systemctl active`當 ready 或再重啟整機；待初始化後取[新 baseline](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v61/runtime_initialized_start.json)。
新 owner `e55a7bfbc258414a9da08092ac74dbfc`在[正式驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v61/runtime_final.json)
**12/12**通過：policy／程式／時鐘／限流／來源獨立進度與近期收據相符。
資料庫窗口 **128.486928 秒**新增鉅亨 **240**、Global Voices **12**，鉅亨 **1.867894 頭／秒**。
HTTP counters 是另一個 **136.898554 秒**窗口、鉅亨 **348** starts／**2.542028 請求／秒**；
窗口仍有 **3 次 502、4 次 transport error（4 session resets）、35 次 body_not_found**。
失敗沒有算成正文、沒有把升速與根治來源混同，也沒有以降低校驗換吞吐；不是 matched-source 提速比例。

[最近 128 批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v61/recent_receipts_after_reload.json)
**7,363 版本／7,363 current heads／1,356,015,364 bytes 原件**、SHA／來源／receipt 核對無錯；這是 bounded sample，不是全庫證明。
[更晚傳輸觀測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v61/transport_final.json)
18 來源通過；該不同時點鉅亨 **5 次 awaiting_headers failure／5 session resets／5 主機決策**，
包括1次隔離主機 circuit；診斷與 checkpoint 無寫入錯誤，尚無新的 gateway recovery，不能稱故障來源已全健康。
TAIFEX dashboard／當沖／隔夜 PID **208／152／982**未變。本機對 WAN HTTPS 的 3 秒 connect 檢查仍 HTTP 000 timeout；外部連線未驗收。

### 全清冊與續作

[04:41:14 pinned 全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v61/providers.json)
保留 **742 鍵／60 設定／18 啟用／682 未註冊**、**6,201,980 已知 URL／3,581,852 已存正文／2,130,337 待授權 URL**。
只做一次清冊，不塞進每15秒面板或熱派工路徑；[同範圍計畫](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v61/plan.json)
仍有 **704 not_reviewed、8 review_incomplete、11 permission_required**，不是704個已證實無法取得的網站。
完整 goal 仍 active：繼續既有未知原件／作者／來源時計／合法端點／API 設定的工作清單，
追蹤鉅亨及各 provider 真實失敗，按證據修復，不擴大授權、不把部分頁面或 liveness 當全歷史完成。

## 原創新聞身分、原件與正式續抓驗證

v62，2026-10-11 05:19 Asia/Taipei。沿用私人共用 HTTP、SQLite frontier、有限 cohort、不可變 Parquet／receipt
與唯讀進度頁，沒有新增另一條下載管線，亦未重設鉅亨每日2份的自動失敗診斷預算。

### 鉅亨慢的已證實原因與尚未解決部分

先核對現有容量控制：健康 transport 窗口恢復原 reviewed baseline 時，`next_search_epoch`已經清為0；
沒有新的「舊一小時冷卻鎖住重測」程式錯誤，沒有為此修改既有正確邏輯。
過期、不同 workload 的先前高併發樣本不能直接恢復成當前的已驗證上限。
05:18面板顯示鉅亨仍在 `running`，3,473,698個正文頭、115,533個可排／重試 URL，
當時0.4秒／5slots、`recovering_transport`；最近45秒窗口約1.344個新頭／秒。
這是那個窗口的觀察，且已有新的502，不能保證未來速度或條件 ETA。

[最終正式資料驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v62/runtime_final.json)
綁定 owner `b6bf488ca0e549809b6a5e94cd483623`、PID1793308、當前程式／設定／時計與保存收據，**12/12通過**。
同 owner 資料庫窗口145.592394秒新增鉅亨161、Global Voices13；HTTP counters 的138.476976秒窗口鉅亨245 starts。
鉅亨分別 **1.105827新頭／秒、1.769247請求／秒**；這兩種量、兩個窗口不混算。
有4次ReadTimeout、4次失敗session重建、25次body_not_found，其中17次 native stage 是
`schema_mismatch_without_verified_figure_view`。未知正文仍待取得可驗證原件，不捏造內容、不當成永久無資料。
原始失敗預算已用完，未強迫重抓、清帳或把metadata-only診斷檔當可重播原件。

[當前傳輸觀測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v62/transport_final.json)
驗收通過，4次失敗都是 `news.cnyes.com`／`awaiting_headers`；4次session reset都有證據、診斷寫入錯誤0。
沒有觀察到新 gateway recovery，這個窗口也沒有任何可安全「排除壞gateway後當整體健康」的證明。
主站headers timeout、後來的502與部分schema/body gap仍未全修好；本輪不宣稱鉅亨匹配基準提速或來源的理論上限。

### 兩個真實可取得原創來源的解析修復

主來源規則為[VOA中文](https://www.voachinese.com/p/3874.html)及[VOA粵語](https://www.voacantonese.com/p/3889.html)：
自行製作內容與第三方／AP內容權利分開。先只納入本機已知、已核對 VOA 作者的原創文字；
不抓圖片／音訊，不推論所有作者、所有第三方作品或全網站歷史均已准許。
中文terms在後續web reader重開逾時；這是外部reader的觀察，不能寫成canonical新聞下載器失敗或已證明拒絕存取。

實際 canonical public robots／article皆200。
[第一輪](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v62/voa_probe.json)與
[byline結構核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v62/voa_byline_probe.json)
指出舊候選`.authors li`沒有匹配真正credit、`.wsw`命中正文與作者簡介兩處。
只輸出bounded公開標頭／DOM形狀，不把未知權利正文印到診斷畫面；byline scope失敗的回應沒有保留raw。
[修正選擇器後的原件](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v62/voa_scoped_probe.json)
私下保留，receipt／raw SHA 經[原件重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v62/voa_raw_replay.json)核對通過。

在 `news_fulltext_public_originals.py`／共用 policy 增加純 native metadata adapter `voa_original_article_v1`：

- 只認 exact provider／domain／primary rights evidence、finite範圍、已核對作者、正文與作者 selector。
- 檢查 requested／og／schema URL、visible／og／schema headline與exact visible／schema作者。
- 粵語5358735的canonical實際指向中文5357662；只認原件證明的這一對，保存宣告、不follow或替換當前觀察；任意跨域／其他ID不放行。
- 唯一`#article-content > .wsw`為正文；去掉media embed／control，不混作者簡介、navigation時計或相關新聞。
- schema出版秒數／offset與native `<time datetime>`等值才放行；另外保留visible文字精度、修訂時間。
  粵語visible只有2020年4月3日，但原件datetime明確是2020-04-03T21:32:04+08:00、schema是2020-04-03 13:32:04Z；
  這是來源宣告、不是補猜的盤中時鐘。

中文林枫原創2087字元／5634 UTF-8 bytes，粵語鍾辰芳原創1831字元／5265bytes；
本文SHA仍分別 `d2dc2cec22398a46d3a1afcbdca9b755f8fe8d0004db56cd52ea42a6d4f45413`、
`73f24f59150ca7090d266dbc95d8c01a04b1fadc5b4438ca088c5a004e00f6c6`，native修復沒有刪減原創敘述。
只兩個已知原件，不是VOA全歷史完成；未知作者／AP／媒體權利與新URL維持可見待核對。
鉅亨原資料指紋仍 `2dbf616eb87e63c9d73b57c32bb561d5ce920620dedac0422004a928cedeac41`，有獨立回歸斷言。

### 正式保存、重載與失敗證據

首次只停service，timer仍可自動拉起owner；後續配置更新觸發正常`configuration_reload_requested`／exit75交接，
不是不明服務crash，也不能拿這一段的舊status當最終native proof。
早期有限原件曾以generic metadata合法保存；保持其不可變原件／收據，沒有事後改寫宣稱早已native驗證。
最後依序 **停新聞timer與service→等待inactive/MainPID0→canonical fetch --refresh兩個exact cohort URL→恢復新聞service與timer**。
提前啟動的refresh被writer lock正確拒絕、未送出請求；保留失敗，確認drain後才重排，不繞過exclusive owner。
[成功重驗紀錄](../artifacts/operations/agent-workflow/runs/v62-voa-drained-current-refresh-20261010T211333-478157f5/run.log)
attempted2／captured2／outcomes空；這是新版本，不是兩個額外唯一新聞頭。

[當前native head metadata](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v62/voa_current_native_heads.json)
唯讀核對兩個現在Parquet head的data fingerprint、body SHA、native身分與clock detail均相符；HTTP0。
[最終有限批收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v62/finite_cohorts_current_native.json)
兩個1/1 cohort membership／receipt／raw與current head SHA通過，原始cohort沒有重凍或擴成整站。
曾誤用只驗證非正文拒絕／缺件的`--verify-queue-outcomes`來驗證成功正文；空非正文選集正確拒絕，
保留[此未驗收結果](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v62/voa_source_and_head_replay.json)，
不放寬verifier或把空集合改成成功。以真正raw重播＋cohort／native head證據取代。

[最近128批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v62/recent_receipts.json)
**6534版本／6532 current heads／1,199,325,420bytes私人原件**核對無錯，兩個superseded版本仍保存。
僅此bounded sample，不是全庫或全歷史驗收。
最後新聞service PID1793308、NRestarts0；gateway PID1712254與交易PID208／152／982未變。

### 測試、進度頁與全清冊

[最終354項／3.60秒針對性測試](../artifacts/operations/agent-workflow/runs/v62-voa-focused-final-20261010T210845-4e4c1799/run.log)
包含新21項：原始負值／股數／比率、文章／作者衝突、錯clock、任意canonical pair、multi-body拒絕、scope gate與鉅亨指紋。
初次test decorator語法及synthetic作者bio位置與原件不符的失敗保留，只修test，不改來源證據來迎合測試。
[最終新聞回歸](../artifacts/operations/agent-workflow/runs/v62-news-final-regression-20261010T211101-e898f11a/run.log)
**2428項／58.35秒**通過（`test/test_news*.py`，非整個repository test suite）；
先前一項固定啟用來源集合斷言需納入VOA，另加finite／public-domain／byline範圍驗證後通過。

[本機四尺寸UI／API驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v62/dashboard/acceptance.json)
1440／768／390／320、742列、鉅亨與兩個VOA各自進度、CNA capability-only、原始URL與審閱狀態通過。
已觀看VOA粵語native截圖：固定1/1保存之外，新增1篇待核對明列「非抓取失敗」，未藏成已完成。
無JS／publisher requests；GET／HEAD200、POST405。沒有改UI／server程式或重啟gateway；沒有將文章正文或金鑰公開。
此瀏覽驗收時間05:10–05:11，最後news owner交接後另由runtime與當前API核對，不把前次browser owner冒充新owner。

[05:11 pinned完整清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v62/providers.json)：
**742鍵／62設定／20啟用／680未註冊／6,201,981已知URL／3,583,907已存正文／2,130,335待授權URL**。
[相同來源全計畫](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v62/plan.json)保留全部742鍵，
**702未審閱、8未完成、11需同意**；新增兩個source是既有鍵重新註冊，不是刪除難抓網站使分母變小。
完整goal維持active：優先追鉅亨可驗證失敗原件、server timeout／schema差異與合法替代表示；
VOA新增2023 budget原件、MoneyLink公共新聞／付費服務規則仍待分開核對，不能推論無法取得或全部完成。

## 鉅亨空圖說、股票清單與到期補漏驗收

v63，2026-10-11 06:00 Asia/Taipei。只修改共用 native extractor、Cnyes HTML ABI 與測試；
沿用唯一 HTTP owner、robots／來源限制、host circuit、SQLite frontier、不可變批次及既有進度頁。
工程偏好與資料品質流程使本輪分開驗證「實際取得」「正文／身分」「原始時鐘」「當前頭與收據」，
沒有為成功率改原始文字、隱藏失敗、放寬權限或縮小742個來源鍵。

### 真正失敗原件與修正邊界

[四筆當前到期缺正文診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v63/cnyes_current_failures.json)
使用既有 `probe_news_sources --current-failure-provider cnyes --maximum-failure-probes 4`；
先停止並等待新聞 service／timer 完整 drain，再由同 HTTP owner／robots／pacing 路徑取得。
未更改文章 due、attempt、refusal、lease、原始觀測時計或自動 2/day 診斷帳；維護後恢復常駐服務。
`4733427?exp=a`為15.789秒後的502，沒有接受／保存錯誤正文，原來源結果仍保留。
另三份200回應已獨立核對 payment=0、同ID visible/current/canonical/schema、byline與精確發布時計，
保存為私人診斷，不直接當成功文章。

- `4636131?exp=a`：current/visible均有一個同位置 `figure > figcaption` NBSP 空白框。
  舊規則要求每一個current figure都必須有image，誤拒真實圖說與這個空框的組合。
  新規則只認無attribute、無image、單一textless NBSP caption的已觀察模板，
  並要求current/visible唯一順序相符；缺件、重複、隐藏、未知文字／數字或圖片仍拒絕。
- `4700323?exp=a`、`4699586?exp=a`：外資股票清單的公司名稱前保留 `)、U+FEFF`，JSON-LD沒有。
  只在確定的清單header、分隔符號、唯一current paragraph、前後同source quote/href/label下改比較clone；
  原body與所有數值保留。實际前一個quote可能為ETF `00715L`，不能只接受純數字股票ID。
  in-word/repeated marker、未知段落、錯link/label、不同current值、錯schema數字、隐藏內容仍拒絕。

新增current-paragraph索引只在已發生schema差異後懶建立一次，避免每個marker重掃整份content；
健康正文不進入新比較分支。caption與stock-list規則可組合，未知figure/table/schema仍保留缺口。
`mobile_canonical_html_contract_version` **11→12**，新capture fingerprint為
`4fa13e18d49a3eb24a9fef45b1963dfa5c4ff0cd574d42286078f159e9be9acb`。
Cnyes API/cached bulk fingerprint原本就排除HTML-only ABI，沒有因此重置健康API cache／checkpoint。
也沒有重用過期或不同source fingerprint的歷史容量best作「最高速」證據。

### 同原件重播與真正入庫

[三份原件SHA／parser重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v63/cnyes_retained_replay_final.json)
沿用 `verify_news_source_diagnostics`的明確三個`--url`，原四筆報告與502不刪除。
同三份原件修正前都是`body_not_found`、修正後 **3/3**；正文為350／488／492字符。
`4636131`由常駐排程於05:39:33捕獲；其餘兩筆已到原期限，
[既有下載器兩筆驗收](../artifacts/operations/agent-workflow/runs/v63-two-due-native-head-capture-20261010T214429-2c68930d/run.json)
於05:44:41／43完成，沒有refresh／due override／同時兩個HTTP owner。

[當前native三個正文頭](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v63/cnyes_current_native_heads.json)
驗證共同批次中的 **30版本／5,399,554 raw bytes**、三個精確SQLite頭及其全部原件／正文SHA。
actual raw經current canonical parser再驗，body、作者、發布／修訂時計、identity basis、新fingerprint一致；
三個body SHA與先前原件重播一致。這次是實際新HTTP下載及成功耐久入庫，不是只把診斷flag改成功。
三個URL頭不代表三個全新不同新聞事件，也不是全站歷史已完整。

[最終focused](../artifacts/operations/agent-workflow/runs/v63-cnyes-format-focused-final-20261010T213510-7d68ec55/run.json)
**307項／4.60秒**；[完整新聞回歸](../artifacts/operations/agent-workflow/runs/v63-news-regression-20261010T213611-c2b197a7/run.json)
**2,477項／55.04秒**；[API/bulk ABI](../artifacts/operations/agent-workflow/runs/v63-bulk-abi-regression-20261010T213822-10aa88a5/run.json)
**69項／2.41秒**。focused包含在新聞回歸中，不重複加總成另一個測試總數；不是全repo測試。

### 整鏈路成本、剩餘錯誤與網頁

[階段成本快照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v63/cnyes_stage_costs.json)
是同正式owner聚合的已完成stage：文章傳輸平均 **1.748秒**，正文解析平均 **0.039秒**；
整體transport包含啟動／metadata discovery，不等於每筆正文傳輸。
並行stage wall time不能直接加成elapsed時間；未宣稱matched-source速度比例或官方數值上限。
主要等待仍在來源HTTP，修CPU比較規則不能消除來源的502／超時。

最終常駐 owner `c3634236113742f6967eec96f48f91fc`／PID `1871567`／NRestarts0。
[正式持續抓取](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v63/runtime_handoff_final.json)
**12/12**；同一 **817.489秒**資料窗口新增鉅亨 **1,034**、Global Voices **82** 個正文頭，
鉅亨 **1.265頭／秒**。另一 **809.325秒**HTTP窗口 **1.818請求／秒**，不把call當正文。
[最新128批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v63/recent_receipts_handoff.json)
**5,324版本／5,322當前頭／973,721,714 raw bytes**無錯，保留兩個superseded版本。
早期長窗口驗收因舊收據證明超過freshness期限只過11/12；
未放寬gate，重新驗證新批次後才取得上述12/12。

[全部20啟用來源的當前傳輸證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v63/transport_handoff.json)
accepted，寫入／checkpoint錯誤0。鉅亨仍有 **10個真實transport failure／10 session resets**：
`news.cnyes.com` 7、`m.cnyes.com` 3；9個awaiting_headers、1個reading_body。
另有11個isolated gateway error／3次host circuit，健康來源仍獨立取得正文。
其證據時點比runtime略晚，不能把這些counter混成同一測量窗口。
較長runtime窗口仍有99次body_not_found，其中53次為未找到verified figure/schema view，
另有missing representation、schema comparison、table grid問題；本輪只根治兩種有原件的表示差異。

[本機1440／768／390／320追蹤頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v63/dashboard/acceptance.json)
accepted，GET／HEAD200、POST405，無publisher request／JS錯誤。
已觀看鉅亨native卡片截圖：已知／已存正文、回補數、正文bytes、本owner速度、ETA與502／其他缺件原因分列。
沒有修改UI／gateway或將私人正文送到瀏覽器；不以本機檢查宣稱WAN可用。
TAIFEX／day-trade／overnight／public gateway PID仍為208／152／982／1712254，未重啟其他服務。

[05:39 pinned完整清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v63/providers.json)
保留 **742鍵／62設定／20啟用／680未註冊**、6,202,014已知URL、3,586,340已存正文、2,130,359待授權URL；
[同範圍全計畫](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v63/plan.json)
保留702未審閱、8未完成、11需同意及1個授權API待設定，不更改全goal分母。

失敗測試、早期原件重播及輸入格式問題都保留，由真正後續驗收supersede。
`agent_workflow run -- python -`沒有轉送caller stdin，零exit不證明artifact已建立；
自建選集缺clock被verifier正確拒絕，最終直接沿用原始報告與既有`--url`能力，未補造觀測時鐘。
全goal仍active：優先取得當前Cnyes剩餘schema/table原件、保留主站逾時／502恢復證據，
再續VOA新增budget的byline／來源權利及MoneyLink公共新聞／付費服務差異；尚無全來源完成證據。

## 短訊全容器與巢狀圖片框修復

v64，2026-10-11 06:33 Asia/Taipei。沿用唯一 HTTP owner、robots／拒絕／deadline、
SQLite frontier、共用 extractor、不可變原件與 receipt，不新增另一條抓取管線。
工程偏好及資料品質流程使本輪把「來源取得」「原件重播」「真正耐久入庫」「運行／網頁」分開驗證。

### 原件、第一性原理與實作邊界

[當前四筆到期失敗診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v64/cnyes_current_failures.json)
均為真正200、已獨立證明同篇公開原件。維護時先停止 timer、排空 service，再由既有
`probe_news_sources --current-failure-provider cnyes --maximum-failure-probes 4 --retain-reviewed-response --trace-access`
取得；診斷結束立即恢復常駐服務。未更改文章 due、attempt、lease、refusal 或每日2份自動診斷帳。
第一次原件重播如實失敗，保留原收據，由[同四份修正後原件證明](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v64/cnyes_retained_replay_final.json)
**4/4**及後續真實入庫取代工程失敗，沒有補造 source clock 或把 private diagnostic flag 直接改成正文。

- `5154231`：官方宣告與 whole visible container 的完整99字符短訊一致；
  native renderer將部分文字放在 paragraph之外，原本只拼接 `p/table`誤判缺文。
  新規則核對完整可見 container／current free content／canonical／headline／source clock，
  並在同一樹遍歷排除原有非正文／廣告範圍、驗證每個正文節點的可見性。
  只有已證明完成的原生串流 wrapper可視為公開；隱藏節點、缺少／錯誤completion、
  重複wrapper、摘要、支付／身份／時計差異仍拒絕。表格原始 cell邊界仍須匹配，沒有合併數字。
- `4790747?exp=a`：同篇圖說外框中另包一個image-only figure；
  不能把外框／內框當成兩張不同圖或忽略圖說。現在只認已觀察的一層透明image leaf，
  匹配source image／caption／唯一順序；圖說獨立保存，不混入新聞 narrative。
- `5041298?exp=a`、`4852826?exp=a`：外框包含數個image-only figure、完整風險聲明，
  後者另有來源補充文字及連結。新規則核對完整ordered forest的文字、每個image URL與link/href/label，
  要求已觀察的結尾三行風險聲明；整個 schema/current/visible全文仍須一致。
  以 `same_article_nested_figure_text`獨立保存全部補充文字／image references／source links，
  不把任意財務文字假稱風險聲明、不下載第三方影像／連結、不更改原始 narrative。

共用[_cnyes_caption_schema_view](../downloader/news_fulltext_publishers.py)只在真實 schema mismatch後
比對root forest一次；保留全樹32／33個figure的上限，不能靠 nesting規避 materialization bound。
隱藏／未知層級、duplicate image、移序、錯圖、改值、錯href、注入註解及malformed URL均有負例。
不是刪除 figure或忽略schema差異來提高成功率。
短訊 ABI **4→5**、native HTML ABI **12→13**，新data fingerprint為
`4116f14ca47c02a4ca1eed407734d1ee33ca30ef697ea2b3c1cb7db24dec141f`；
既有API/bulk fingerprint `f0941d48fc292342baaad6030342722b47887108d4ee92eeff11e70dfde983f4`保持不變。
因此不重掃健康 API cache／metadata checkpoint，也不回刷已補回的v63三筆。

### 真正下載與逐筆驗收

正式 owner `bc09d7def7b44ae59df8de0b7fbd63e0`／PID1965551依原到期時間自行處理四筆，
沒有人工 `fetch --url`、refresh、重設attempt或額外停啟服務去強制捕獲。

| 原 URL尾段 | 捕獲時間（Asia/Taipei） | 正文字符／bytes | 結果 |
| --- | --- | --- | --- |
| `5041298?exp=a` | 06:21:19 | 1,541／3,939 | complete、matching原件重播body SHA |
| `4790747?exp=a` | 06:23:17 | 713／1,895 | complete、matching原件重播body SHA |
| `4852826?exp=a` | 06:23:37 | 1,157／3,051 | complete、matching原件重播body SHA |
| `5154231` | 06:26:48 | 99／213 | complete、matching原件重播body SHA |

[四個當前頭與native raw／metadata證明](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v64/cnyes_current_heads.json)
沿用 canonical `benchmark_cnyes_acquisition.verify_batches`，驗證相應批次全部 **79版本／14,102,143 raw bytes**，
再逐筆比對當前SQLite頭、Parquet、body／raw SHA、source data fingerprint與current parser重播的
正文、作者、發布／修訂時計、identity basis、figure caption及補充文字欄位；0新HTTP、0queue write。
四個URL頭不是四個全新不同事件，也不是全站歷史完整。

短訊一度到期尚未派工，實際讀取claim SQL及現行 max_attempts=6後，
確認沒有「attempts=5永遠不能重試」條件；後續自然進入第六次嘗試並成功。
維持最早到期重試的公平credit與原due，沒有為未證明的排程bug重設計數。

[最後focused](../artifacts/operations/agent-workflow/runs/v64-short-stream-final-focused-20261010T221957-0e3885e9/run.json)
**397項／4.07秒**；[完整新聞回歸](../artifacts/operations/agent-workflow/runs/v64-completed-stream-regression-20261010T222100-cbf094f1/run.json)
**2,529項／51.10秒**；[API/bulk回歸](../artifacts/operations/agent-workflow/runs/v64-cnyes-bulk-regression-20261010T221613-802525df/run.json)
**69項／2.02秒**。focused已包含在新聞回歸，不重複加總；未執行無關GPU／全repo訓練驗收。

### 速度、所有來源錯誤與目前進度

[06:27正式運行](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v64/runtime_final.json)
**12/12**通過；同owner資料庫窗口 **450.986228秒**新增鉅亨 **330**、Global Voices **43**個正文頭，
鉅亨 **0.73173新頭／秒**。另一 **443.265244秒**HTTP counter窗口 **1.28817requests／秒**，
仍有 **11次502／5次transport errors（5次session resets）／58次body_not_found**，
其中27次native stage為 `schema_mismatch_without_verified_figure_view`。
前輪cohort、捕獲契約與失敗組成都不同，不能拿前輪1.265頭／秒聲稱本輪提速或直接歸因退步。
本輪證明的是相同四個錯誤原件已修復並真正補回，未證明全站速度上限。

[同owner階段成本](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v64/cnyes_stage_costs.json)
文章HTML transport平均 **2.498746秒**、article extraction **0.033857秒**；
slot wait平均0.448583秒。全部transport含discovery，與每篇HTML傳輸不同；
各worker wall sums重疊，不可直接相加成elapsed時間。主要等待仍是來源HTTP，不是CPU正文解析。

[最近128批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v64/recent_receipts.json)
**4,799版本／4,799當前頭／869,451,102 private raw bytes**、receipt／SHA驗證無錯；是bounded sample，非整個lake。
[全部20個啟用來源傳輸證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v64/transport_final.json)
accepted、寫入與checkpoint錯誤0。鉅亨五個header timeout為主站4／行動站1，均有session reset，
另有14個host gateway決策；它們比formal runtime稍晚、不是同一個counter窗口。
沒有觀察到其它啟用來源在這個owner產生新transport failure，不等於它們已完整、沒有舊403或均有可抓正文。
Global Voices窗口內另有1個image-only來源，正確保留非文字分類，不偽造文字、OCR或算成功正文。
CMoney／PeoPo先前403、民視未知作者／第三方轉載、CNA實際API contract設定及未審閱來源仍各自保留。

本機 `GET /news/api/status`讀到fresh、running、snapshot_consistent的當前抓取快照；
唯讀面板沿用原先連動，不呼叫publisher補畫面，不發送正文／金鑰、不改UI或gateway。
[最後瀏覽readback](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v64/dashboard/acceptance.json)
在1440／768／390／320px核對742列、Cnyes／兩個VOA卡片、CNA setup與來源審閱及原文連結進度，
全部accepted、0publisher requests／0JavaScript errors。已查看desktop鉅亨卡片：running、已知／已存、
正文bytes、本owner速度、條件ETA與錯誤分列；不同viewport是不同snapshot時計，計數持續增長不強制相等。
未重做WAN驗收，不能把本機readback稱為外網已部署；沒有新UI設計或對外傳送文章原文。
news timer active；TAIFEX／day-trade／overnight／public gateway PID仍208／152／982／1712254，未重啟。

[06:38同owner交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v64/runtime_handoff.json)
仍 **12/12**；同 **1,113.376292秒**資料窗口新增鉅亨 **1,204**、Global Voices **105**個正文頭，
鉅亨 **1.081395新頭／秒**。另一 **1,115.034823秒**HTTP窗口 **1.597260requests／秒**；
仍有 **21次502／19次transport errors（19次session reset）／170次body_not_found**，
其中96次native schema gap。沒有將body_not_found或API304算成正文，也沒有宣称所有錯誤已修復。
[最新128批交接證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v64/recent_receipts_handoff.json)
**4,600版本／4,600當前頭／830,739,590 private raw bytes**、SHA／receipt無錯，非全lake或未來寫入的證明。

[06:22 pinned全清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v64/providers.json)
保留 **742鍵／62設定／20啟用／680未註冊**、**6,202,015已知URL／3,589,306已存正文／2,130,359待授權URL**。
[同742鍵全計畫](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v64/plan.json)
仍有702未審閱、8未完成、11需同意及1個授權API待實際設定；沒有把難抓來源移出goal分母。
完整goal維持active：續Cnyes其它schema/table缺口與來源header／502問題、
VOA新增budget原創署名／權利、MoneyLink免費新聞／付費服務差異及其餘待接線來源。

## 買超清單與直接圖片補充文字修復

2026-10-11（Asia/Taipei），續修 v65。仍是同一完整新聞 goal，不把未接線來源移出分母。
本輪沿用工程偏好及資料品質的 scoped companion：先核對真正失敗原件與來源身分，
修共享比較入口，保留正文、數值、發布時計、原失敗收據及 source access 邊界。

### 根因與修正

[当前到期失敗樣本](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v65/cnyes_current_failures.json)
在 timer／服務真正排空後，沿用唯一 HTTP owner、原 due、robots、截止及來源拒絕檢查；
上限4但實際只有2個合格 URL，沒有湊數、重刷 v64 已補回四筆或改寫 queue。
兩份都是公開200回應，但舊 parser 都回 `body_not_found`：

- `4644102?exp=a`：正文／current 都保留買超股票清單中的 U+FEFF，schema省略。
  共用清單比較規則原本只認「賣超」；現在同時辨認來源原樣的買／賣超清單，
  仍要求相同唯一 current 段落、完整文字、兩個相鄰股票連結與標籤，以及完整 schema agreement。
  只改 comparison clone，原始股票、數值及 U+FEFF 不清洗。
- `4822498?exp=a`：同篇圖片框有作者附註、原始連結和末尾風險聲明，卻沒有子 figure。
  共用結構 helper 接受這個已觀察的直接圖片形狀，仍限制一個實際圖片、1–4個 plain 段落、
  精確連結／標籤、末尾原樣三行聲明、可見性及整篇 current／visible／schema 比對。
  補充文字獨立保留為 `same_article_image_figure_text`，不冒充圖說、巢狀框或風險聲明正文。

實作為 `downloader/news_fulltext_publishers.py`、`downloader/news_fulltext_policy.py`，
測試為 `test/test_news_cnyes_stock_list_markers.py`、`test/test_news_cnyes_nested_figures.py`。
HTML native ABI **13→14**，data fingerprint
`aaf8e1b9f55cdee94e4c630aa6e12c744bb3cfa2d9cbdbf7c1c8bc2037fec693`；
bulk fingerprint仍是 `f0941d48fc292342baaad6030342722b47887108d4ee92eeff11e70dfde983f4`，
不重建既有健康批次快取／API歷史。未知結構、隱藏／衝突數值、網址憑證及整篇缺字仍拒絕。

### 同原件與回歸證據

[修前原件重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v65/cnyes_replay_before.json)
保留兩個實際負結果；[修後相同原件](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v65/cnyes_replay_after.json)
**2/2 accepted**、0額外HTTP，保留正文SHA、作者與原發布時計。
正文分別525／455字符；發布來源分別2021-05-12T00:49:21.000Z／2022-03-02T09:25:01.000Z。
這一步是解析接受，不自行推定已經正式入庫。

[focused](../artifacts/operations/agent-workflow/runs/v65-source-shape-focused-correct-20261010T225011-9edba26b/run.json)
**431項／4.49秒**；[完整新聞回歸](../artifacts/operations/agent-workflow/runs/v65-news-regression-20261010T225012-0096d668/run.json)
**2,563項／54.17秒**；[API/bulk](../artifacts/operations/agent-workflow/runs/v65-cnyes-api-regression-20261010T225013-72fc7717/run.json)
**69項／2.67秒**。focused已包含在新聞回歸，不重複加總。
第一次focused命令引用不存在的測試路徑，未跑任何測試；改用實存五檔後通過，原 failed receipt 保留並明確 superseded。

06:51完整測試後排空重載，正式 news PID **2054184**。剛重啟的第一份audit仍讀到舊owner stopped快照，
不是新部署通過或來源數據損壞；原報告保留，不把舊 transport metadata 的 fingerprint mismatch算來源新錯誤。
[初始化後正式基準](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v65/runtime_initialized_start.json)
06:53:07 owner **66c0c888f3bf455c8fb5d5a6b70aaee3**、程式／policy／DB／process時計相符。
[同新owner20來源transport初查](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v65/transport_current.json)
accepted，checkpoint／access／diagnostic write errors為0，已有4個gateway host決策。
它只是本輪早期窗口，不能稱全部來源／全歷史已根治。

### 實際補回、全鏈路與下一個效率根因

沒有對這兩筆另開fetch、改due、重置attempts或擴增2/day自動診斷帳。
常駐owner自然捕獲 `4644102?exp=a` **06:54:55**、`4822498?exp=a` **06:55:04**，
兩筆complete、無error／lease，正文SHA都與先前同原件重播一致。
[當前頭與原件證明](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v65/cnyes_current_heads.json)
沿用 canonical batch verifier，核驗其所在不可變批次全部 **62版本／11,058,284 raw bytes**，
再逐筆對照SQLite頭／Parquet／raw重播之正文、作者、發布／修訂時計、identity basis、圖說及補充文字；
**2/2 accepted**、0HTTP／0queue write。不是單純unit test或來源200就說成功。

[正式運行窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v65/runtime_final.json)
06:55:35 **12/12**通過，owner仍66c0；資料窗口 **148.705164秒**新增鉅亨 **103**、Global Voices **15**個正文頭，
鉅亨 **0.692646新頭／秒**；獨立 **152.553940秒**HTTP窗口 **1.127470 requests／秒**。
同窗口仍有 **3次502／4次transport failures（4次session reset）／13次body_not_found**，其中9個native schema gaps；
沒有把304、失敗、同篇其他URL或parser接受當成全新新聞事件。
此owner pinned stage中，168次文章HTML傳輸平均 **315.159383559÷168＝1.875949秒**，
162次解析平均 **4.922923524÷162＝30.39毫秒**；worker wall sums重疊，不能相加成elapsed。
來源等待仍占主要時間；本輪兩個相同失敗原件修復及真正補回有因果證據，整體提速比例／最高速沒有matched-source證明。

[最近128批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v65/recent_receipts.json)
**5,169版本／5,169當前頭／926,069,048 private raw bytes**核驗無錯；bounded sample，不是全lake。
[最後20來源transport核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v65/transport_final.json)
accepted、errors空；鉅亨4次awaiting_headers逾時分別主站3／行動站1，4次失敗連線重建，
7個gateway host決策。寫入／checkpoint錯誤0；其他來源本owner沒有新transport failure，不代表舊403／權利缺口已解。

[四尺寸實際網頁readback](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v65/dashboard/acceptance.json)
1440／768／390／320px、742列、Cnyes／兩個VOA／CNA setup、原文與審閱進度通過；0publisher requests。
已查看desktop鉅亨卡片，且06:58本機API仍fresh／running／read_only／snapshot_consistent。
只有news服務受控重載；public gateway／TAIFEX／day-trade／overnight仍PID1712254／208／152／982，未重啟。
未重做WAN或冷開機驗收，不稱外網已部署或全來源／未來故障都已解決。

[別名與同ID主網址的唯讀候選盤點](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v65/alias_primary_candidates.json)
最早到期的 **512個pending缺頭URL**，全部是已觀察形狀的原生別名，且本機已有同ID主網址的complete頭。
這提示目前剩餘工作可能大量是「另一網址的獨立驗證」，不是完全沒有取得的新聞正文。
這是有順序偏差的512筆樣本，非全frontier統計；相同ID不是相同內容／作者／時計／權利的證明。
兩筆已修案例的主網址body SHA也與別名不同，尚須核對raw、版本、格式及補充文字，
因此沒有刪除或跳過任務、複製正文／收據或把別名虛報complete。
下一步完整核對canonical entity／representation分工及原件，分離真正正文缺口與追加校驗工作，
再改善排程／進度與呼叫效率；其他native schema gaps、502/header等待與待接線來源仍保留在原goal。

本輪只讀確認CNA_API_KEY仍未設定，不印出值或更改已有授權。
富聯網現行[著作權](https://member.money-link.com.tw/AccountCenter/Copyright.aspx)與
[付費會員產品條款](https://member.money-link.com.tw/AccountCenter/ServiceCaption.aspx)範圍不同；
沒有把後者一律套用到免費公開新聞，也沒有以重開條款頁當成取得正文或接線進展。

## 桌面候選分工與常數成本階段計數

2026-10-11（Asia/Taipei），續修 v66；同一 `news-adaptive-capacity-20261009` task／完整 goal。
工程偏好、資料品質及儀表板技能影響本輪做法：先區分文章、網址版本和未驗證提示，
沿用 canonical classifier／owner／頁面，讓各階段和真實總數對帳，不用分類變化製造完成率。

### 根因、共用修正與界線

[修改前同 owner 稽核](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v66/dispatch_before.json)
顯示 ready missing-body work 中 pending 有61,978個無已知主文提示、45,509個已有主文提示。
v65最早到期512筆樣本雖有順序偏差，卻全部已有同ID主網址正文；從程式回查發現
`news_fulltext_cnyes_work.canonical_candidate_id`只處理mobile／AMP，
原 parser 已支援的桌面 `https://news.cnyes.com/news/id/N?exp=a` 仍落入首次正文 lane。

- 只延伸既有 `cnyes_supported_native_alias_view`，不接受桌面 `exp=b`、編碼／額外參數、fragment、假host或HTTP。
  import當下分類，舊資料沿 `host_state` 索引及獨立desktop cursor每輪最多256筆、迴圈40ms預算續補；
  SQLite查詢／WAL fsync算在實測成本，不宣稱整個交易有硬性40ms保證。mobile／AMP舊cursor不重置。
- 保留既有8個首次正文工作後輪到候選版本、oldest-due retry及其他lane公平性。
  沒有改due、attempt、拒絕、租約、Retry-After／host冷卻，也不複製主網址正文、時計或receipt。
  桌面版本不建立AMP lookup work；只有獨立實際capture才可完成。
- `work_dispatch` **3→4**，新增 `body_work_progress` **v1**；HTML native **v14**及bulk資料指紋不變。
  `cnyes_body_work_metrics`一次以ready partial index及inflight provider/state索引建立，之後insert／update／delete
  與主文可用性變化在同一交易維護，rollback同步恢復。heartbeat只讀最多8組，不掃歷史或原件。
- 既有唯讀頁新增三種待辦分項，安全投影核對合計；不存在或不一致的舊快照顯示未知，非零或完成。
  「未找到已存主文」含尚未分類，不能說是唯一文章缺口；候選主文存在也不是正文、作者或版本等價證明。

實作在 `downloader/news_fulltext_cnyes_work.py`／`news_fulltext_queue.py`、
`stockagent/live/news_dashboard.py`、`services/news_dashboard/app.js`；
延伸 `scripts/audit_news_cnyes_work_dispatch.py`、`scripts/verify_news_dashboard.py`及原測試。

### 分類、計數、收據與實際網頁

[第一次正式分工稽核](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v66/dispatch_final.json)
在同一唯讀WAL snapshot核對完整當前 **52,960**個未租用桌面候選：全部候選ID正確、52,586已有主文，
可用性提示不一致0。這不是已接受52,960篇新正文，也不是全歷史；分類本身不接受來源。
[07:23後續分工稽核](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v66/dispatch_latest.json)
仍一致，桌面候選52,954個；全部active missing-body網址106,019，其中97,837有主文提示、8,182無提示。
active含inflight／尚未到due的retry；每個版本仍保留。不能把8,182當作獨立唯一文章數。

增量小表逐組對實際文章表相符；兩次64輪小表讀取約8.28／16.41微秒，
首次owner的indexed ready統計約11.67毫秒。不同predicate與inflight另列，這是計數成本，非下載提速倍數。
測試涵蓋import／bounded backfill／source拒絕／租約／公平性／主文增加移除／claim／retry／acceptance／
rollback／provider-state變動／reseed／restart／頁面未知狀態及秘密不外露。
原新聞回歸 **2,582／50.79秒**、API/bulk **69／2.76秒**、focused **213／16.88秒**；
新增稽核細節後 **73／2.37秒**，測試組重疊，不相加。

[四尺寸實際渲染](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v66/dashboard/acceptance.json)
1440／768／390／320px：742來源鍵、Cnyes分項逐個數字、兩個VOA追加／非正文界線、CNA API setup、
原始網址清點及來源審閱、篩選／reset通過，0publisher requests。
已實際查看320px的鉅亨卡片，分項對同一次render generation，標籤／數字不溢出。
唯讀gateway API也保持fresh／running／read_only／snapshot_consistent，沒有正文或金鑰傳到瀏覽器。

[最新128批收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v66/latest_receipts.json)
**5,261版本／5,261目前頭／937,302,485原始bytes**，errors空；仍是bounded最近cohort，不是全庫完整性。

### 全鏈路持續進展及尚未解決的錯誤

只受控排空／重載新聞owner與唯讀gateway，news PID **2116043**、owner
`9a732c2c551f46e48b0adc0dd1ca94ff`，gateway PID **2116102**。
timer active／NRestarts0；TAIFEX／day-trade／overnight仍PID208／152／982，未重啟。
[已初始化基線](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v66/runtime_initialized_start.json)
政策／程式mtime／process clock與DB owner一致，沒有用舊draining快照當新owner驗收。

[第一個正式窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v66/runtime_final.json)
**12/12**；189.512秒DB窗口新增鉅亨237、Global Voices18，鉅亨1.2506新網址正文／秒；
183.720秒HTTP窗口1.7146請求／秒。同窗口仍4次502／21次body_not_found。
[升速後第二窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v66/runtime_ramped.json)
再次 **12/12**；458.290秒DB窗口新增鉅亨1,176、Global Voices48，鉅亨**2.5661新網址正文／秒**；
469.936秒HTTP窗口**3.1238請求／秒**。HTML／正文／入庫都保留，沒有少抓驗證或跳過失敗。
cohort組成和clock不同，不能用兩窗口直接算matched-source倍數；0.033秒client設定亦不代表30個實到請求／秒。

第二窗口實際仍 **4次502、7次transport／7次session reset、36次body_not_found**。
API/bulk和desktop不被壞AMP host一律拖慢；既有獨立source/host退避及重建連線繼續工作。
自動ramp曾到8並行，07:25snapshot為7並行、0.2秒client/effective gap；這不是官方上限或永遠穩定保證。
[20個enabled provider傳輸／恢復原件稽核](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v66/transport_latest.json)
07:23的較早snapshot accepted、errors空；6次主站awaiting_headers全部6次session reset，6個isolated gateway決策。
不同snapshot不把6和7相加；checkpoint／診斷寫入錯誤0。其他來源沒有本owner新transport failure，
不表示舊CMoney／PeoPo 403、未驗證作者、非文字或授權缺口都已解。

對公開DNS/IP的正式回讀兩次均8秒TCP connect逾時，原failed runs保留；
固定相同SNI／hostname到127.0.0.1的HTTPS回讀成功，返回當前742來源與新分項。
不把本機TLS路由成功當作WAN／冷開機／外部瀏覽器已修復。
完整goal仍active；下一步優先已保留native schema失敗的真實原件與可安全驗證來源，
所有到期工作持續由既有daemon續抓，未接線／被拒絕來源不移出清冊。

## VOA精確語系宣告與私人追加正文修復

2026-10-11（Asia/Taipei），v67；同一原task與完整新聞goal，沒有縮減來源。
工程偏好與資料品質技能用於分清URL身分、版本、來源時計和權利；驗證技能只驗收既有面板與實際資料，
沒有新增另一套網站、重新發布正文或用服務active取代收據。

### 實際根因與共用修正

[第一次有界公開中繼資料診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v67/voa_metadata.json)
6/6 HTTP200，卻均為`article_identity_not_verified`。實際可見署名是「美國之音」、「鍾辰芳」、「松仁」，
並非原本只有鍾辰芳的v1清單；網站標示的組織署名保留原文，不偽造成個別記者。
[第二次私人原件診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v67/voa_originals.json)
證明每篇粵語頁的canonical是另一個實際中文URL，而og/schema/current request的URL及文章標題一致。
因此只加入6組逐篇觀測的對應；不猜同ID、不跟隨中文網址、不複製中文正文或收據。

- 延伸`news_fulltext_public_originals.voa_original_article`與`PolicyRegistry`，只有粵語使用
  `voa_reviewed_private_article_v2`、adapter ABI3；中文v1維持原窄範圍。
  新資料指紋`ef50bbea8cd5536b3a6339c52ea7536428fb977dc23e6cf777bbe4e5de984a2c`。
  Cnyes native14與bulk指紋未改；新增VOA scope不能迫使鉅亨重抓健康窗口。
- 保留每篇原生`time datetime`、schema發布／修訂值、可見日期精度和時區，三者獨立核對。
  不拿本機新聞metadata日期或URL年份推回發布時鐘，也不回填成歷史PIT。
- 私人v2要求來源明確免費、唯一正文容器、當前schema與可見署名一致；隱藏／付費、
  未審署名、另一篇／錯誤canonical及時鐘不一致仍拒收。圖片、圖說及影音不在此次文字scope。
- canonical probe補上有界來源宣告診斷，以及獨立於身分解析成敗的原件保留閘門。
  若解析先卡身分，不能因此把未核對、隱藏、付費或schema署名不一致的正文誤存成診斷原件。
  同一HTTP／robots／pacer／cooldown路徑，沒有第二個並行collector或繞過拒絕。

[VOA粵語官方條款](https://www.voacantonese.com/p/3889.html)區分自行製作公共財產、
受保護AP內容及其個人非商業電腦儲存例外，不能一概說AP私人儲存禁止，也不能當作公共領域／再發布許可。
本輪只把已觀測三種當前署名列入有限私人scope；沒有為未觀測AP署名、整個入口或所有第三方作品授權。
`rights_status=not_cleared_for_reuse`，publish／commercial use／training injection／remote export維持false。

### 原件、追加批准與實際下載

[最終零HTTP原件重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v67/voa_replay_final.json)
6/6診斷SHA／來源身分／正文／署名／發布與修訂時計驗證。
有限追加沿既有`add_reviewed_supplement`與正式`download_news_fulltext fetch`，
先停止timer、排空單一owner，批准6個明確網址；保留immutable原件與scope收據，再各自重新實際下載。
命令實際attempted6／captured6／outcomes空，隨即恢復原service／timer，沒有操作其他交易服務。

[獨立耐久驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v67/voa_durable_acceptance.json)
6份正式版本、6個現行頭及正文UTF-8 **24,747bytes**，逐篇新指紋／原件重播／作者／時計／正文SHA相符。
追加scope與membership SHA、content-addressed原件SHA、原cohort收據均核對；原cohort仍1個URL，未改原digest。
診斷成功不是入庫；正式capture也不代表全portal歷史或可遠端訓練／發布。
目前粵語有7個已存正文頭，另ID-only`6424871.html`仍未在新scope通過；沒有把它假裝完整或刪除舊證據。

測試：最終完整新聞 **2,611／50.37秒**、API/bulk **69／2.45秒**、focused **238／5.05秒**，互有重疊。
涵蓋6組真實精確對應、錯誤ID／域名／schema／時鐘／署名、hidden／付費、原始數值、獨立診斷保留與scope。
初期schema型別斷言、pytest iterator警告及舊授權分類斷言的失敗run均保留；最終全組通過後標為superseded，
不是來源拒絕，更不是略過失敗測試。Git diff／本輪untracked檔案的顯式尾端空白檢查均通過。

### 全來源、實際頁面與仍慢的鉅亨

[一次完整唯讀清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v67/provider_inventory.json)
保留 **742來源鍵＝62已設定＋680未註冊**，其中20啟用；當前6,202,042個已知URL、3,595,957已存正文頭。
不是742個已完成provider；2,130,374個awaiting-authorization URL不算成功。
清點wall18.109秒／CPU17.864秒是本次單一pinned snapshot，並非每次heartbeat成本；0publisher calls。

[四尺寸實際渲染](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v67/dashboard/acceptance.json)
1440／768／390／320px通過：原cohort1/1、追加6/6、未核對新增1、待排0分開對帳。
仍顯示「需注意」，不能用固定批次100%冒充全站完成；已實際查看320px粵語卡片的換行、標籤與數字。
鉅亨三類工作、全部742來源鍵、CNA授權API setup、原URL清點、source review、filters/reset與失敗後native重試仍可用。
本機既有gateway快照fresh／running／read_only／snapshot_consistent；沒有重新啟動健康web或交易服務。
本次沒有新增WAN成功證據；v66公開IP的TCP逾時仍保留，不能以本機渲染宣稱外部入口已修。

[初始化基線](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v67/runtime_start.json)
source程式、registry指紋、process clock、DB和status owner一致，PID2209716、NRestarts0。
[第一正式窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v67/runtime_final.json)
12/12；217.277秒新增鉅亨55、Global Voices22；2次502、5次transport／5次session reset。
截至第一正式快照累積的HTML HTTP平均4.117秒，正文解析平均25.45ms；CPU解析不是主要等待。
[較長窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v67/runtime_latest.json)
再12/12；556.218秒新增鉅亨264、Global Voices55，0.4746新頭／秒；實際HTTP0.9107請求／秒。
仍4次502、10次transport／10次session reset，全部保留；不是較v66提速或官方極限。
獨立provider capacity checkpoint已按自身fingerprint重用；程式顯示持有bounded transport recovery，
不能把未知網路故障、2並行／0.5秒client gap或之前8並行的短窗口當作官方限速。

[全部20啟用來源傳輸證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v67/transport_final.json)
07:55較早快照accepted／errors空，7次當前header失敗全部7次session reset，診斷／checkpoint寫入錯誤0。
其他啟用來源在這個owner窗口無新傳輸失敗，不代表舊403、未核對作者、非文字與授權缺口已消失。
[最近128批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v67/receipts_latest.json)
4,973版本／4,973現行頭／887,502,953原始bytes，errors空；只證明此bounded近期cohort，不是全庫。
完整goal與task仍active；下一輪優先鉅亨主站header逾時、host隔離／健康參考與自動回復的現況，
以及當前到期但沒有正文的來源原件。服務自然續抓，不重設拒絕、Retry-After、future due、租約或學習歷史。

## 鉅亨近期穩定容量恢復與原件格式修復

2026-10-11（Asia/Taipei），v68；原task與全來源goal不變。
工程偏好與資料品質技能將請求、來源身分、正文、版本及發布時計分開核對；
驗證技能核對實際落盤和既有面板，沒有新增另一套collector或用active替代完成。

### 慢速根因與恢復選擇

[修復前傳輸證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v68/transport_before.json)
20個啟用來源中，鉅亨當前owner累積1,414個終結HTTP請求；主站1,066次中有17次transport失敗、8次502。
17份傳輸診斷均在主站，其中16次等待headers、1次讀body；17次失敗session均已重建，不能說來源沒有錯誤。
`awaiting_headers`涵蓋client／TLS／network／server等待，不是單独測出的來源server處理時間。

`PublisherCapacity._transport_resume_candidate`原本只檢查`best_by_workload`的歷史最高峰。
最高峰只有單一窗口或已過期時，其他較快但反覆穩定的近期設定被遮住，導致回到baseline而非恢復已有證據的壓力。
新增共用純函式`recent_transport_resume`，恢復選擇contract v2；measurement ABI10不變：

- 每次選擇最多128個既有窗口，依相同workload／client gap／HTTP slots分組；不同設定不可湊成穩定證據。
- 要求未過期、無重疊、足夠實際responses及新正文／版本、無不可用回應、錯誤比例符合既有規則。
  401／403／429／451、來源cooldown、official floor與資源上限繼續否決，不能因想升速而清掉。
- 合格群組按可比較耐久新正文速率的中位數選擇，不按單一最高峰。
  當前仍先取得fresh recovery streak，再進入live verification；歷史候選不是新的配額、成功或ETA。
- 工作量有界`O(128 log 128)`，不掃全庫、不增加測速HTTP或每次請求的額外網路工作。

[修復前固定快照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v68/resume_before.json)
與[相同歷史零HTTP重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v68/resume_after.json)
保留舊選擇null；新選擇0.2秒／7slots，由7個獨立健康窗口支持，中位數2.2389新正文頭／秒。
這只證明選擇缺陷與修復，`current_live_throughput_improved=false`；不是當前實測速度或官方上限。
新parser資料指紋仍依既有契約讓不相容舊容量歷史失效，沒有為取得較漂亮速度而硬接舊checkpoint。

### 兩份真實失敗原件與資料邊界

[當前原件獨立重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v68/cnyes_originals_final.json)
2/2診斷通過，兩篇結果不同，不都算正文成功：

| 原始網址 | 根因、修正與實際結果 |
| --- | --- |
| `news/id/4663341?exp=a` | 「買超股票清單」第一個BR之後、第一家公司前有U+FEFF；原比較器只認得第二家公司以後的分隔形式。限定精確已知heading、唯一目前paragraph、直接第一家公司、下一個已核對股票URL／標籤才移除比較用字元。474字／730 UTF-8 bytes完整正文已於08:21:35依原due自然補回。 |
| `news/id/958337` | 當前內容是encoded `<div><br></div>`，JSON-LD／可見正文同樣空白，來源確實沒有文字。只延伸精確空白BR模板；多BR、隱藏、script、影音、未知非文字markup仍不放行。原重查時間09:00:12不改，尚未把此新判定寫成正式queue outcome；沒有製造正文。 |

首家公司規則不全頁去U+FEFF、不修改原始數值、不把另一篇canonical或隱藏文字收進來。
native HTML fingerprint contract **15**、source-empty contract **5**，新鉅亨資料指紋
`ac0ca35513f4d69bf658f6c127833b8274e2bc634f4875695981ef6ac556c824`。
bulk和authorization指紋不變，VOA及其他provider原scope／收據保留。

[正式單篇耐久驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v68/selected_durable_acceptance.json)
核對新版本及當前頭，正文SHA
`56a6e81f218112c01db92d2b09ebd996a00c53735a8faed99b235ddacf98363d`，
作者保留「鉅亨網新聞中心」，發布／修訂原值均`2021-06-21T00:49:25.000Z`；
當前來源重播及capture fingerprint相符。不拿URL年份、本機抓取時間或舊metadata代替發布時間。

部署先停timer、排空唯一owner再reload；[當次targeted fetch](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v68/selected_current_fetch.json)
因兩篇仍未到due，attempted0／captured0，保留這個no-op而不稱為已下載。
恢復service／timer後，正式owner自行在原到期時間補回第一篇，未重設due、lease、拒絕或診斷每日預算。

### 測試、實際完整鏈路與網頁

最終完整新聞回歸 **2,648／55.26秒**、獨立API/bulk **69／2.84秒**；
focused **487／6.39秒**及容量／回饋 **140／2.17秒**有重疊，不相加當作更多測試。
測試包含第一家公司、hidden／未知heading／錯誤股票URL／schema數值、精確空白與非空白模板，
以及非峰值穩定候選、過期／拒絕／錯誤workload、重複或重疊窗口、不同壓力與restart相容性。

[較長正式運行驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v68/runtime_final.json)
**12/12**，同一owner `f06a3d0cddf041889beae22eeb201b20`、PID2283177、NRestarts0。
265.145799秒資料窗口新增鉅亨346、Global Voices24個已存正文網址，鉅亨 **1.30494新頭／秒**；
另275.51195秒HTTP counter窗口為 **2.45361請求／秒**，兩種時計與指標不混算。
同窗口仍6次502及2次header逾時／2次session reset。快照當下已自動升至8slots／0.2秒client gap；
這是有界當前觀測，不宣稱較v67的因果提速、全來源完成或官方最高速。
截至這個owner快照，679次HTML HTTP平均1.727秒、665次正文解析平均25.014毫秒；是owner累積階段平均，
不是該265秒窗口的增量，也不是相同文章的matched-source benchmark。等待仍主要不在CPU解析。

[最早21.892秒窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v68/runtime_first.json)
沒有觀察到新的DB正文頭，11/12而未通過；保留負證據，沒有改弱驗收或以HTTP成功數代替。
延長正常觀測、取得fresh獨立落盤證據後，既有相同12項檢查通過。
另三個workflow argv選項位置錯誤的run以exit127保留；沒有執行來源HTTP／修改queue，
更正命令入口後同一組唯讀驗證真正執行並通過，不把工具啟動錯誤誤算provider拒絕。

[最後20啟用來源傳輸回讀](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v68/transport_final.json)
鉅亨841個終結HTTP請求中，2份header逾時診斷與12份502 host決策均驗證；API／mobile／主站的近期健康證據分開。
其他啟用provider在該owner窗口沒有新傳輸失敗；idle、舊403、授權和正文身分缺口仍保留，非「所有來源已修完」。
[最近128批獨立收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v68/receipts_final.json)
3,910版本／3,910當前正文頭／683,528,659原始bytes，errors空，只證明此pinned近期cohort。

[全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v68/provider_inventory.json)
仍 **742鍵＝62設定＋680未註冊**，20啟用；6,202,050已知URL、3,597,135已存正文、2,130,374待授權。
[四尺寸實際頁面](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v68/dashboard/acceptance.json)
1440／768／390／320px均通過31頁來源清單、鉅亨三類工作、VOA原cohort／追加、CNA授權API setup、
controls／reset、空狀態與資料回讀，JS錯誤、資源失敗與publisher請求均空；已目視mobile鉅亨卡片。
公開瀏覽器仍不帶金鑰／原始正文、不呼叫publisher；沒有重開健康gateway或其他交易服務。
沒有新增WAN成功證據，v66公開IP逾時仍未宣稱修復。

完整goal與task仍active，正式owner持續回補。下一步保持當前吞吐與來源錯誤證據，
核對空白模板自然到期後的queue outcome，並針對新的未解析正文／其他provider可執行缺口續修；
不清掉未知、拒絕、Retry-After或未核對來源來宣稱全部抓完。

## PanSci既有缺口、原生發布日與有限批次追加

2026-10-11（Asia/Taipei），v69；全來源goal與task維持active，沒有縮減742來源鍵。
v68有真正的排程修復、正文落盤與驗證，是progress；本輪從完整清冊處理另一個可執行的既有缺口。
工程偏好與資料品質技能用於分清metadata／原頁日期、診斷／入庫和scope／全站；
驗證技能核對獨立原件、收據、實際頁面與當前owner，不新增另一套collector或dashboard。

### 清點、來源規則與實際缺口

PanSci原固定cohort有1個已存網址，另4個既有本機網址處於`waiting_cohort`，不是HTTP失敗或資料不存在。
目前[官方aboutus](https://pansci.asia/aboutus)仍將內容權利歸各作者，轉載需詢問作者或編輯部；
不能採用舊的全站CC假設，政府／研究機構的署名也不自動等於公開領域。
沿用已設定私人研究範圍，保留原作者／來源、實際robots和拒絕；不啟用再發布、商用或自動訓練匯出。
未找到官方數字速率配額，此處只做4篇有界取得，不做飽和測速或最高頻率聲明。

[有界當前原件診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v69/pansci_originals.json)
4/4 HTTP200，4篇均有自己`load_post_1`的ID／URL、作者和日粒度發布值。
5個HTTP請求＝1個robots條件重查304＋4篇原頁200，共2,437,547 response bytes；正文不是整頁所有嵌入文章。
先停timer、排空唯一source owner，再沿canonical HTTP／robots／pacer／circuit取得；trap恢復原service／timer。
[獨立零HTTP原件重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v69/pansci_originals_replay.json)
4/4通過，但診斷成功尚不是文章入庫、全站完整性或訓練授權。

### 發布日、作者與原始值

[逐篇日期與UTF-8獨立對帳](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v69/pansci_date_byte_reconciliation.json)
從已驗證的當前Parquet頭重新計算正文SHA、12,108字／32,963 UTF-8 bytes，沒有讀取或匯出整庫正文。
原始日期與本機索引時間分開保留，沒有重寫metadata或回填成當年版本：

| PanSci原始文章ID | 原頁日粒度發布值 | 既有metadata原值 | 當前署名 |
| --- | --- | --- | --- |
| 333349 | `・2021/10/23` | `2021-10-23 09:00:19` | Mia |
| 320279 | `・2021/05/05` | `2021-05-05 04:00:26` | 科技大觀園 |
| 127993 | `・2017/10/15` | `2018-07-06 10:25:30` | 研之有物│中央研究院 |
| 122920 | `・2017/07/26` | `2018-07-05 22:27:59` | 研之有物│中央研究院 |

兩篇calendar-date衝突不以索引時間冒充發布時間；機構署名不捏造成個別記者。
原頁只有日期，沒有可核對的分秒／時區；不指定午夜UTC，也不把當前正文宣稱為2017年的歷史PIT版本。
第一篇之外的嵌入文章日期和署名不借給目標篇，原有解析與缺日期測試維持不變。

### 共用追加、正式下載與恢復

只為PanSci加入既有`allow_reviewed_supplements`與明確追加說明；不新增feed／sitemap／全站輪詢。
新資料指紋`4d79163e495312cf3c47c66ea386b3707310dd1de1b9e2ef26bec060076324b8`；
authorization指紋仍`2c1d809af55ff9a01349e6e5e8bbf43469d8b62597b49e48ebb4a651fd25c138`。
鉅亨native15／bulk、VOA指紋與授權scope均不變；不是為了追加科學新聞而讓健康來源重抓。
新增真實PanSci registry／queue語義測試，證明這個opt-in不允許直接或`refresh`抓未審metadata增長，
不改原cohort，不自動建立supplement membership。

[正式CLI下載收據來源](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v69/pansci_capture_receipt.json)
核對successful run的原始JSON stdout與log SHA，attempted4／captured4／outcomes空；
4篇原位新增scope SHA `d8e5547169223115cf4a499d2da0e17a11c4c779f03b620f8d23b7ebfb8a630b`。
v67當時的`fetch --output`不屬於該CLI已文件化的報告用途，因此另保留有來源SHA的console收據，不捏造未寫出的檔案。
此為歷史觀測；[v73已正式支援selected fetch原子報告](#原生空署名恢復與局部端點的公開觀測)，不再作為現行CLI限制。
[正式當前版本與原件再解析](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v69/pansci_durable_acceptance.json)
4/4新指紋／正文SHA／作者／日期相符，capture時間為09:42:49–09:42:55；不是把診斷檔直接冒充正式capture。

[所有15個固定scope與4個追加scope收據核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v69/finite_receipts.json)
scope integrity通過；PanSci原1篇的URL digest
`118af5821c05e7c2a740ad3c5af8d4293bdbe4af1affe8fa506683b5169bc9d3`及收據不變，追加4/4均有immutable來源證據。
其日期UTC bounds仍NULL；有作者／日期不代表有精確UTC發布時刻。整份稽核不把其他來源缺口算完成。

### 鉅亨自然重查、整條鏈路與網站

[source-empty原件與當前queue回讀](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v69/cnyes_empty_queue_outcome.json)
958337在09:00:17依原due自然完成重查，`state=error_code=source_article_empty_template`、body SHA仍NULL。
原件重播、當前loaded Cnyes fingerprint與現行狀態相符；是正確的來源空白，不是成功文章或人工清除retry。

最終新聞回歸 **2,649／53.03秒**、API/bulk **69／2.53秒**、focused **105／3.64秒**；互有重疊不相加。
一個不存在的focused test路徑exit4和一個把驗證輸出錯當probe輸入的run均保留，
更正來源檔／入口後實際通過，沒有修改source拒絕或用工具錯誤當成來源故障。
[新owner基線](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v69/runtime_start.json)
PID2472157、NRestarts0、owner `1fa692c766a9452aa072b4b7152d5584`，DB／status／source政策／process clock一致。
[正式完整鏈路驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v69/runtime_final.json)
**12/12**；146.150679秒資料窗口新增鉅亨145、Global Voices15個正文網址，鉅亨0.99213新頭／秒。
HTTP另一時計為1.62567請求／秒；未觀測新的502／transport失敗，不消除此前失敗或宣稱官方上限。
[當前20啟用來源傳輸證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v69/transport_current.json)
source owner當時鉅亨184、Global Voices11個終結請求，傳輸／gateway診斷錯誤空；idle或冷卻來源仍非完成。
[最近128批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v69/receipts_current.json)
5,234版本／5,234當前頭／806,036,845原始bytes核對，errors空；不是全歷史驗收。

[當前全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v69/provider_inventory.json)
仍742鍵＝62設定＋680未註冊、20啟用；6,202,082已知URL、3,605,011已存正文、2,130,386待授權。
PanSci既有本機已知5/5，不宣稱所有可能歷史文章已下載。
[四尺寸實際頁面](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v69/dashboard/acceptance.json)
1440／768／390／320px均對帳：PanSci原1/1與追加4/4、未核對新增0、鉅亨三類工作、VOA追加、CNA API setup、
原URL／source review／filters與reset、31頁完整來源清單；JS錯誤、資源失敗與publisher請求均空。
已目視320px PanSci卡片；只有唯讀狀態進瀏覽器，不帶金鑰或原始正文，也沒有重開健康gateway與交易服務。
外部WAN仍沒有新的成功證據，不以本機渲染宣稱外部入口已修。

原goal和task保持active，正式owner繼續鉅亨與Global Voices回補。
下一輪從完整清冊核對其他有限來源的未審新增與未註冊來源的原始公開規則，
把可取得的真正正文沿共用下載器補入；仍不能以PanSci5/5或服務active冒充742來源全部完成。

## 原始媒體清冊遺漏與低成本增量摘要

2026-10-11，v70，Asia/Taipei。沿同一task／goal修復全來源清點的共同缺口，
不是另開新聞抓取管線、放寬來源規則或重設鉅亨失敗。

### 原因與範圍

[修正前獨立唯讀清點](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v70/original_source_coverage_before.json)
在01:55:54 UTC的一致WAL中，946,017條literal metadata連結指向616個原始hostname。
742鍵的舊清冊只合併實際`articles` frontier與設定來源；尚未獲准建立正文工作、
但已由本機literal envelope精確解析的 **192個來源鍵／2,546條連結**因此消失。
例如`maxfinanciallife.com`有1,886條線索、`www.stheadline.com`75條、`www.famitsu.com`65條。
這不是來源HTTP失敗，亦不代表2,546篇不同文章；同篇可能由多條alias指向。
hostname來源鍵可能是同一出版社的不同子網域，不能將934鍵稱為934家媒體。

保持literal decoder contract 1、正文／授權fingerprint及metadata原值不變，
沒有解析opaque Google token、使用未公開RPC、以標題猜URL或自動授權這些來源。
原有339,000個opaque、2個unresolved結果仍保留；metadata解析完成不是正文完成。

### 一次回建、增量維護與失敗語義

[共用URL與來源摘要](../downloader/news_fulltext_urls.py)新增獨立`original_source_inventory` contract 1：

- 唯一queue owner維護`original_url_host_metrics`，粒度為`resolution contract／hostname`的alias連結次數。
  INSERT／DELETE／hostname變更與原證據同交易更新；`target_queued`變更不重計連結，亦不創造正文。
- 舊庫只做一次covering-index回建，完成marker與回建同交易提交。
  中途失敗回滾，重開可再完成；總數不一致只修復derived摘要，保留articles、source_links、proof、收據與leases。
- heartbeat只讀616個host摘要，按現有PolicyRegistry分類；不在每次status／網頁重掃近百萬proof。
  沒有建立原始URL工作、複製股票關聯或修改來源允許範圍。
- [清冊](../scripts/audit_news_providers.py)、[owner status](../downloader/news_fulltext_queue.py)
  共用同一摘要。JSON／CSV保留原frontier的URL／正文計數，另列alias線索及host。
  未初始化或不一致標為不可用，不將未知解釋為0／完成。
- [公開投影](../stockagent/live/news_dashboard.py)獨立校驗typed counts與合計；不輸出原始目的URL、
  query、金鑰、作者個資或正文。僅有metadata者進度未知、正文工作未建立，不能顯示0/0成功。

[獨立SQL對帳與成本](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v70/original_source_conservation_and_cost.json)
在同一固定WAL內，三次完整proof索引oracle與materialized摘要逐來源／host／alias次數完全相等。
499個已解析原始來源鍵、616主機、946,017連結一致；舊742鍵無遺失、192個遺漏全部已列，
62個既有設定來源的authorization與前次報告完全相同，audit publisher calls=0。
相同輸出的中位wall／CPU：oracle **123.17／115.22ms**，摘要 **8.98／8.99ms**，
wall比值 **13.72倍**。這是新增來源清點的全掃描與增量讀取比較，
不是舊production heartbeat已做這個掃描，也不代表鉅亨網路或整個系統快13.72倍。
修正前首次冷讀1.700秒另存原件，不拿不同cache條件算改善倍數。

### 鉅亨錯誤沒有被清掉或當成完成

[重載前20啟用來源傳輸診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v70/transport_before.json)
02:04:20 UTC：鉅亨35次transport failure，均在awaiting_headers，35次failed-session reset；
18次502被記錄為isolated gateway host error、4次host circuit開啟，沒有變成整個provider禁抓。
實際成功回應中，mobile標頭等待最大24.22秒、desktop24.30秒；正文讀取通常遠短於標頭等待。
這是HTTP wrapper邊界，包含連線／TLS／網路／server等待，不能只憑這些數據歸咎來源server或CPU。
這些immutable事件仍保留，沒有清除403／429、Retry-After、原due、冷卻或未知結果。

只gracefully重載canonical新聞owner及唯讀dashboard，恢復timer；
PID **2518017**、owner **`76b6956712e44e6cae57024edf6415e0`**、NRestarts0，
唯讀網站PID2518052，其他下載器、交易、GPU工作沒有重開。
[新owner基線](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v70/runtime_start.json)
與[完整鏈路正式驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v70/runtime_final.json)
政策／code／process clock／status／DB與收據一致，**12/12**。
142.304278秒DB窗口新增鉅亨 **137**、Global Voices **15** 個正文網址；
鉅亨 **0.96273新頭／秒**，另一個139.875153秒HTTP時計為 **1.59428req/s**。
短窗口仍有 **1次awaiting_headers transport failure／1次session reset**，502增量0。
不將與前輪不同cohort的數字當成因果提速，不宣稱所有來源錯誤消失或已達官方上限。

[新owner最終20來源錯誤檢查](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v70/transport_final.json)
accepted、errors空，該header failure原件回讀與reset數據相符；idle／來源拒絕／冷卻的provider仍未證明完成。
[最近128批耐久正文收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v70/receipts_current.json)
4,999版本／4,999當前頭、829,916,897 private raw bytes通過；不是全部幾百萬正文的驗收。

[較晚交接的20來源診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v70/transport_handoff.json)
02:20:29 UTC，同一owner仍正常：鉅亨1,201個terminal requests、**17次awaiting_headers failure／17次session reset**，
**7次isolated gateway host errors／2次host circuit**，原件與統計回讀accepted、errors空。
這才是較晚的累積失敗證據，不用02:10較短窗口的502增量0宣稱外部錯誤消失。
02:20:57的原生heartbeat仍running、新owner累積新增正文URL heads792；這個counter不是不同媒體文章數。

### 網站、回歸與仍待完成的事

[當前JSON清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v70/provider_inventory.json)
與[CSV](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v70/provider_inventory.csv)：
**934鍵＝62設定＋872未註冊，20啟用，192鍵只有原始線索**；
6,202,084 frontier URLs、3,606,541已存正文、2,130,386個frontier URL仍待授權。
新增線索不加進URL／正文分母；該收據只描述02:07:58 UTC快照，不保證後續歷史範圍已全。

最終新聞回歸 **2,669／61.85秒**；API/bulk **69／2.61秒**；focused **177／19.10秒**，
後者是subset，不相加。新增測試涵蓋legacy回建、重啟等價、交易失敗回滾、摘要異常修復、
重複alias／多host同provider、read-only無mutations、禁止heartbeat讀proof表、typed/private快照與未知進度。
第一個focused run因測試誤用不存在的NewsQueue context manager和SQL trace誤計CREATE TRIGGER而失敗，
修正測試後實際通過，原失敗run保留；沒有把測試工具錯誤當source failure。
實際owner關閉／重開測試另核對新owner ID、整份canonical rows與summary完全保留；
初次測試誤用`owner_id`而非實際`owner`屬測試工具錯誤，修正後177項及2,669項回歸均通過，原失敗保留。

[最終四尺寸實際網頁驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v70/dashboard_latest/acceptance.json)
1440／768／390／320px，**每個39頁完整934鍵**；純線索卡片1,886與來源摘要／原URL時計對帳，
「未入佇列」、速率未量測、未知progress／ETA及權限分界均可見。
既有鉅亨工作分類、PanSci／VOA追加、CNA API待接線、native filters／reset、timed刷新失敗與恢復皆通過；
JS／資源錯誤及publisher requests均空，已目視320px最大metadata-only卡片。
首個UI run誤用不存在的`acq-sources`測試anchor，失敗保存在`remaining_v70/dashboard`；
核對現有shared shell的`acq-catalog`後才實際驗收，不改production DOM假裝成功。
外部WAN沒有本輪新驗證，不以localhost通過當作公開入口完整驗收。

goal／task保持active，這輪是有證據的進展，不是934個來源均有下載器或全部完成。
後續先從完整清冊核對有價值的尚未註冊原始網站與有限來源新增（如Genetinfo），
保留原站規則／robots／發布日／作者／正文範圍，沿同一canonical collector產生真正耐久正文；
鉅亨與Global Voices繼續按學習容量、失敗隔離及公平due排程回補。

## 已核對原件零重抓、原生日期衝突與恢復保護

2026-10-11 v71，工程證據：[完整清冊的優先來源對帳](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v71/local_priority_sources.json)。
Genetinfo的2,112個waiting網址，只有2,092個不同item ID；與原已存1,331個不同ID沒有交集。
這是清冊／固定scope待核對，不是2,112次網路抓取失敗。尚未註冊的maxfinanciallife有1,886條不同原始網址線索，沒有當成已抓正文。
重新讀[Genetinfo官網](https://www.genetinfo.com/)及[聯絡／footer頁](https://www.genetinfo.com/contact.html)、
[MAX官網](https://maxfinanciallife.com/)；僅這些頁面未找到數字配額或完整批次授權，不據此宣稱官方最高速／全文訓練權利。
MAX目前首頁內容與metadata歷史不能直接視為同版本；其下載器尚未啟用。

第一性原理的修正：實測原件已取得，效能目標應是「每份唯一來源回應能形成多少可驗證正文」，
不是把診斷與正式下載各抓一次。原流程追加scope後仍做第二次HTTP，來源取得的工作重複；
新增明確CLI模式沿同一解析器與NewsQueue.accept直接使用immutable原件，並把原始觀察與重解析時間分開。

```text
run_fintech_python -m scripts.download_news_fulltext fetch \
  --reviewed-supplement <canonical-probe-report.json> \
  --materialize-reviewed-response --url <explicit-original-url>
```

這不是新crawler或自動許可：最多16個直接網址、來源finite明確opt-in、逐筆成功公開診斷與獨立scope。
整批原件先驗證，再claim；沒有HTTP物件、補抓fallback、refresh、due／lease／拒絕重置或自動metadata擴圈。
要求scope／原件SHA、實際request/final URL、current source fingerprint、原author／native clock／body SHA、
當時authorization與headers、aware取得時間全部成立。舊診斷若缺真實時計／headers，只能作review證據，不能捏造正式觀察。
FTV等需要另一份canonical原文的流程不能用一份subsite回應假裝完成。所有本文仍私人、publish=false、remote_training_export=false、historical_point_in_time=false。

共用程式：[canonical正文驗證與record組裝](../downloader/news_fulltext.py)、
[不可變追加scope／原件materialize](../downloader/news_fulltext_cohorts.py)、
[同一owner、壓縮／收據／SQLite恢復](../downloader/news_fulltext_queue.py)、
[明確CLI](../scripts/download_news_fulltext.py)。replay contract=1；cached_source_reparse不充當線上HTTP容量。
新body receipt帶缺正文head witness；寫檔後、SQLite提交前崩潰，僅同一未變head／owner_restart釋放可恢復。
若有較新的來源檢查、拒絕、metadata、授權或body head，不能由舊replay覆蓋。

實測：[四筆canonical probe](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v71/genetinfo_canonical_probe.json)
全部200，HTTP到HTTPS是實際來源redirect，不是403後換入口。
73219的metadata是2019-01-28／另一標題，來源目前正文標成2023/8/22；只證明目前URL版本，不能恢復當年原文。
33611／33688／33732原生日期分別2019/12/31、2020/1/6、2020/1/7，日期粒度沒有被改成精確時區／開盤可用時間。
metadata原標題／日期、native正文／作者／日期、實際取得／再解析時間各自保留；日期衝突欄位明確標示，非UTC排序或PIT推論。

[正式materialization](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v71/genetinfo_materialization.json)
4/4、282,541原始response bytes、11,346字正文，1筆日曆日衝突；重驗／耐久入庫0.573516秒，這一步HTTP=0。
**整個指令21.148192秒、CPU17.049517秒**，包含初始化／恢復／scope／最後status；沒有把0.574秒冒充整條鏈路或來源下載速度。
[四個當前head獨立重驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v71/genetinfo_selected_acceptance.json)
4/4、response/body SHA、native作者／日、capture contract全一致。
[scope版本／原固定批次證明](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v71/postdeployment_scope_transition.json)
原1,356網址、membership digest、receipt SHA不變；Genetinfo scope變更指紋
`8389e7b4e8da1a74835905beb458e83c8e48de21a877735738509c824a2a97ed` →
`3f1500a09973f58952705ccbf3384f5cf784f4435548e029fe3add60bc4ef4d8`，四份原件的新approval／capture匹配新contract，舊正文／fingerprint保留。
62份authorization與20個enabled來源不變；不是略去fingerprint的silent scope擴張。

[完整清冊刷新](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v71/full_provider_inventory.json)：
934鍵＝62設定＋872未註冊，192 metadata-only仍可見；6,202,093已知URL、3,608,718已存正文head、2,130,386 awaiting_authorization。
Genetinfo原批次正文1,351、非正文5，新增scope正文4；總已存1,355、待核對2,108，不能說固定批次99.63%就是全來源歷史完成。

驗證：最終`v71-final-shared-news-config-reg-20261011T024108-7fc8da36`，2,697 pass／70.42秒；
API/bulk `v71-correct-cnyes-bulk-api-regre-20261011T023956-1488b997` 69 pass；
focused `v71-retained-replay-cli-batch-cl-20261011T023917-74867f6f` 140 pass（subset，不相加）。
涵蓋缺／naive／future時計、授權／MIME／digest／contract變動、完整批次preflight、persisted due／拒絕／lease、
zero-HTTP CLI、idempotence、durable receipt中斷恢復與較新拒絕不覆蓋。
[128個最近批次收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v71/recent_body_acceptance.json)
4,120 versions／4,120當前heads、712,793,310 private raw bytes接受；不是全lake歷史驗證。
[四尺寸rendered acceptance](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v71/dashboard_latest/acceptance.json)
1440／768／390／320，39頁／934來源，各尺寸顯示Genetinfo追加4/4、待核對2,108，
獨立Cnyes缺正文工作、metadata-only未知進度、API setup、日期／規則與唯讀失敗恢復；JS／resource／publisher requests空。
主agent實看320px Genetinfo截圖；CPU-2D browser不證明GPU/WebGL或WAN。

[同owner正式runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v71/runtime_after.json) 12/12。
owner `8926cda687d447a4b49a4c48e6885018`／PID2597553、timer active；唯讀gateway PID2594093，無手動更動其他下載器／交易。
[時計分開的實測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v71/measured_progress.json)：
WAL頭122.159739秒新增Cnyes117／Global Voices12，Cnyes0.957762新head/s；HTTP counter窗口116.575868秒，兩者不混用。
[本owner傳輸證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v71/transport_current_owner.json)
Cnyes 249terminal requests，3 awaiting_headers失敗／3 failed_sessions_reset，4 gateway決策（1次host／3次publisher層），
502都來自robots，不是假正文或文章body取得成功；沒有把原件重播或健康端點200當官方極限。

失敗／更正證據保留：第一輪日曆日衝突測試誤用了「只接受aware時計」的UTC排程hint，
改為literal day比較、不賦予時區／intraday後回歸通過。
API測試路徑與8000網頁port的兩個命令錯誤改用實際bulk測試／8770/news/後通過，不歸為來源故障。
最初scope報告錯把新增opt-in當成fingerprint不變，保留failed報告，改由實際新／舊fingerprint及四份原件一致性驗收；沒有修改原收據。
第一個runtime沒有previous，不能證明progress；另一個diagnostic要求新owner必須已有failure樣本而當時尚沒有，
不強迫製造錯誤或清除證據，後續真實觀察已完整保留。

完整goal仍active；本輪是可驗證的根因修正，不是934來源均已實作或全部下載完成。
下一步先釐清robots的origin級5xx是否錯擴為provider級長停（保留robots fail-closed、401/403/429與共同配額），
再續查Genetinfo2,108待核對原件與MAX1,886原始線索；不能用加速之名略過原站規則／身份／歷史時計。

## Robots origin 故障範圍與排程喚醒

2026-10-11 v72；只重載 canonical 新聞服務。原請求「Anue 太慢、發現 provider 錯誤後修復再繼續」仍保留，完整 goal active。

### 根因及控制邊界

[舊 owner 傳輸診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v72/transport_before.json)
核對20個啟用來源：鉅亨21個 awaiting_headers 傳輸失敗／21次 session 重建，17筆502 gateway決策，
其中開機後 AMP robots 的前三筆因沒有其他 host 成功證據而落在 publisher scope，
後續14筆為有獨立成功證據的 host scope；不能把這些概稱為429限速或正文解析失敗。
一般 robots_* 異常又會被 get() 擴成 provider 300秒暫停，原始 origin 規則的未知範圍因此混入了共同配額。

[RFC 9309](https://www.rfc-editor.org/rfc/rfc9309.html) §2.3／§2.3.1.2／§2.3.1.4 指定REP的scheme／authority、
重新導向後的初始authority及不可達時的拒絕抓取。據此分離權限範圍與共同pacer；
不把robots當內容授權、不套用4xx可選放行來弱化既有拒絕，也不宣稱本實作完整符合所有RFC redirect選項。
不同端點必須各自通過當前授權、robots、實際正文／身份／作者及耐久收據驗證；沒有替換故障文章的URL或模擬成功。

| 觀測 | 許可／暫停範圍 | 排程處理 |
| --- | --- | --- |
| robots 502／504，無任何 Retry-After／challenge | 初始 scheme＋authority；仍無許可 | origin負面快取30→300秒，期滿真實重驗；其他origin核對自己的規則 |
| robots 格式／重新導向異常，非存取 challenge | 初始origin；不把錯誤HTML當空Allow | 有界負面狀態，保留舊可信規則／validator作證據，不作失敗後許可 |
| 401／403／451／429／503、任何 Retry-After、challenge | 既有provider保護及已學得pacing floor | 原拒絕／期限不被owner重啟、origin期限到期或調速清除 |
| 一般文章502／504及route／TLS／transport異常 | 原gateway／route／transport契約 | 不借robots origin隔離偷偷放寬；失敗session仍自動重建 |

### 共用實作與恢復

[REP控制](../downloader/news_fulltext_robots.py)、[HTTP](../downloader/news_fulltext_http.py)、
[scope SQL](../downloader/news_fulltext_host_circuit.py)、[queue](../downloader/news_fulltext_queue.py)、
[Cnyes候選](../downloader/news_fulltext_cnyes_work.py)與[service](../downloader/news_fulltext.py)延伸原共用管線。
新增`robots_origin_policy_contract_version=1`，未增加另一個下載器／pacer，也未修改`.env`、來源授權、資料fingerprint或其他服務。

負面狀態使用原robots cache、durable atomic write；原source／authorization fingerprints、generation、觀測／not-before clocks均保留。
重啟或parser/config變更不清除有效負面期限。最多32個active origins/provider，內存過量或checkpoint寫入失敗時保守回退provider保護。
期滿會重新發出真實robots請求；不能重用過去Allow。有效200／304／404／410才清除負面狀態；
304只重驗原可信cache，錯誤HTML的新ETag與空body不能變成可重驗的許可。
首次 malformed policy redirect 缺少Location由原本6次同URL請求減為1次，仍拒絕文章HTTP。

所有文章lane及discovery在LIMIT之前排除active origins，保持partial index／UNION綁定與既有公平性；
blocked文章的lease／attempt／due／body不被選取本身改寫。
discovery只重寫外層WHERE，保留Cnyes相關子查詢的字面問號、endpoint cooldown及參數。
next_wake_delay使用同一origin／host期限：只有blocked due=0工作時睡到最近deadline，
不反覆啟動空cycle；有其他ready來源則不延遲，provider期限仍具優先權。

重新導向的robots請求從start／headers／terminal始終計為robots，不因最終路徑是首頁而混入文章吞吐量。
[傳輸診斷v5](../scripts/audit_news_transport_observations.py)分別驗證原gateway與新origin事件，
source／authorization fingerprints、私有檔案SHA／mode、owner、metrics snapshot cutoff與無Retry-After／challenge均需一致；
網路失敗也保留初始authority與role，不輸出正文／完整文章URL／headers。
舊快照無新marker只列legacy，不被[v7 runtime](../scripts/audit_news_runtime.py)當成已載入新控制的正式驗收。

### 測試、真實故障與驗收

新增[45個origin／恢復測試](../test/test_news_robots_origin_recovery.py)：獨立origin、初始redirect scope、角色時計、
任何拒絕／Retry-After／challenge、negative cache重啟與fingerprint變更、expired failure、304可信性、
checkpoint I/O、corrupt state、SQL LIMIT／index、correlated endpoint guard、閒置喚醒及新marker型別。
既有CPU／access診斷測試改為核對新的origin暫停，不降低「沒有文章HTTP／不能把HTML當Allow」的斷言。

最終`v72-final-compiled-news-regressi-20261011T031512-e871ae20`：**2,742 pass／69.87秒**；
`v72-cnyes-bulk-regression-20261011T031135-fd724488`：69 pass／2.84秒。Focused287是subset，不相加。
完整回歸中舊provider-wide斷言、fixture缺少cooldown必要欄位、測試time import與synthetic endpoint pattern的失敗run全部保留；
最終已更正並通過，不把命令／fixture問題歸為來源故障。

新owner `3a91b9929571442f817c280afc656b50`／PID2672525，手動只重載新聞服務；timer active，NRestarts=0。
最初重載後仍讀到舊owner的stopped heartbeat，沒有當成新程序運行證明或再重啟；
後續[current loaded baseline](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v72/runtime_loaded_baseline.json)
已綁定新PID／owner／載入marker。Baseline沒有previous，所以progress驗收false是預期，不偽造驗收。

[正式runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v72/runtime_after.json) **13/13**；
[時計分開的實測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v72/measured_progress.json)：
WAL窗口152.950232秒新增鉅亨143／Global Voices12正文URL heads，鉅亨0.934944643新head/s；
HTTP counter窗口151.938744秒為1.553257542req/s，兩個窗口不混用。
news cgroup CPU增加57.466229秒，平均0.375718 core，末值323.832 MiB；不外推整個WSL或GPU，也不拿前後不同cohort算提速倍數。

[目前owner故障收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v72/transport_current_owner.json)
20啟用來源核對通過：該取樣鉅亨212 terminal，3 awaiting_headers／3 session重建、
3次AMP robots502精確歸初始origin，另1次一般請求gateway host隔離；http_circuit_opened=0，原錯誤未刪除。
稍後[耐久cache實證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v72/robots_policy_cache_proof.json)
已到generation4／240秒，Cnyes sourceFP與authorizationFP不變；這是仍在故障及有界續試，不是健康證明。
較晚[11:27交接快照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v72/handoff_latest.json)
已到generation5／300秒，原origin仍無許可；owner仍持續新增正文、timeout仍會發現並重建失敗session。
此時計晚於上述formal window／immutable event取樣，不能倒灌進舊窗口的計數或SHA。

[最近128個批次](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v72/recent_body_acceptance.json)
核對**4,028版本／4,028當前heads／697,455,202原始response bytes**，全部通過；不是全湖3百多萬heads的逐檔驗證。
[四尺寸實際網頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v72/dashboard/acceptance.json)
1440／768／390／320px、39頁／934來源、native search／pagination／failure恢復及Cnyes／Genetinfo卡片對帳；
無JS錯誤、無水平溢出或browser publisher requests。主agent檢視320px Cnyes卡片；CPU-2D，不是GPU／WAN驗收。

### 全來源剩餘範圍

[完整清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v72/inventory_after.json)
62設定／20啟用／872未註冊／192 metadata-only鍵，6,202,122已知URL、3,610,953已存正文heads，
2,130,408 awaiting-authorization；未註冊鍵包含轉址／聚合，不是872家可隱式啟用的出版商。
該WAL鉅亨3,498,296已存heads／3,594,991已知URL、90,042 missing-body工作及88個已存重驗；known frontier不是全站歷史。
CMoney仍有source_access_denied、271,836待正文；PeoPo1個拒絕續試及76個來源授權未核對；
其他已登記的來源刪除、付費／登入、圖片／embedded-only、轉載作者／身份與非文章URL仍分別保留，不冒充已下載正文或來源數值錯誤。

完整goal未完成。下一步沿現有清冊檢查剩餘鉅亨正文與身份線索、其他provider的來源更新／合法API／原站替代、
有限來源未核對追加工作，以及面板的局部origin暫停說明；繼續觀察真實成功／故障窗口，不能強清官方拒絕或以沒有HTTP錯誤代替資料完整性。

## 原生空署名恢復與局部端點的公開觀測

2026-10-11，v73；沿用原goal／唯一news writer／HTTP owner／public gateway。
此階段是已驗收進展，完整goal仍active；沒有擴張來源授權、重開其他下載器／broker／訓練，沒有公開正文或credentials。

### 根因與來源證據

[開始清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v73/inventory_before.json)
有鉅亨9個 `article_attribution_not_verified`、195個文章identity失敗，以及CMoney／PeoPo存取拒絕。
獨佔HTTP的[有界實測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v73/current_identity_probe.json)
保留兩個不同結果：mobile樣本仍受既有 `host_gateway_cooldown` 保護，未送出請求、未取得可診斷正文；
`4955510?exp=a`實際200、公開free宣告／ID／canonical／標題／發布秒數吻合、三個正文表現的比較digest一致，
但唯一可見 `.signature` span 是明確空白，而current-ID data與同篇NewsArticle schema都宣告同一個具名作者。
舊parser把「可見credit空白」誤當「無法核對任何作者」，因而丟掉完整公開正文。

修正[共用native extractor](../downloader/news_fulltext_publishers.py)，不是另開HTTP／browser／mobile資料管線：

- 仍要求canonical、og、目前ID／free flag、標題、 aware原始clock與可見正文完整相符；正文／表格／caption比較與paid／hidden／challenge拒絕不變。
- 只有明確空白、無未知child／comment／title／ARIA代用credit的可見 `span.signature`，且兩個目前具名作者宣告一致，才接受此已觀測layout。
- `authors_json`保存來源真正宣告的作者；`visible_byline_raw`仍是空字串，
  `attribution_status=source_declared_author_visible_signature_empty`，不宣稱名字可見、不借用頁尾／推薦文章、不捏造作者。
- [capture fingerprint](../downloader/news_fulltext_policy.py)的native contract由15升16，原始response、clock及過去收據不改寫。

[原失敗response重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v73/retained_author_replay.json)
通過原件／收據SHA與新parser核對；診斷原件不是文章成功收據。
[真正重抓報告](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v73/recovered_author_fetch.json)
九個explicit native URL均實際attempted／captured **9/9**，outcomes為空；
[九個head的獨立驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v73/recovered_head_acceptance.json)
全部耐久batch、current head、原始正文／作者／clock與目前parser重播 **9/9通過**。
這是九個來源URL觀察，不因相同numeric ID推定其他頁正文等價，也不把現在取得的正文倒填成歷史capture。

### 自動恢復與取得報告

[佇列](../downloader/news_fulltext_queue.py)只對獨立驗證的v15 capture fingerprint做一次pinned重排：
due、未存body、無lease、曾實際HTTP核對的native author-parser terminal failures。
保留原attempt／error／check clock；stored heads、403／429／其他identity結果、未知query與未來deadline不重設。
marker與URL集合digest存於既有queue settings；restart不會無限再次重排。
新parser直接處理以後的同layout文章，不靠每天掃描全新聞history修補。

[canonical fetch CLI](../scripts/download_news_fulltext.py)的既有 `--output` 現在真正原子保存selected fetch／retained-materialization報告，
讓attempted、captured與每個失敗outcome可由檔案核對；entrypoint exit0、source200或服務active都不代替body acceptance。
首次部署wrapper的SQL quote錯誤被shell process substitution隱藏，未產生selected report／未送targeted HTTP；
保留[原run log](../artifacts/operations/agent-workflow/runs/v73-deploy-and-recover-pinned-au-20261011T034240-ad966dec/run.log)，
核對後改用已確認的九個exact URL argv，明確 `test -s` 所需報告，並用上述獨立head verifier判定結果。
原helper另有 `load_config(str)` 的本機Path型別錯誤；保留failed run並改為Path，沒有把它稱為source故障或下載成功。

### 同一網頁的局部故障範圍

[唯讀public projection](../stockagent/live/news_dashboard.py)只從canonical heartbeat取值，
以已載入的origin-policy contract v1、live clock、≤32個valid origin／有限deadline產生count與最早policy probe clock；
來源URL、headers、私人reason／正文不下送。legacy、bool版本、NaN、越界／過期快照與stopped狀態保持unknown，不能顯示「0個故障／已恢復」。
expired deadline只是不再active，不是permission或provider recovery。

[現有頁面](../services/news_dashboard/app.js)明列「整個來源最早再排」與「局部端點暫停／最早重新核對」，
後者寫明其他可用端點照常排程、到期不代表恢復存取；需注意filter／頂部alert涵蓋部分origin暫停而不誤改整個provider狀態或ETA。
保留原responsive shell、934來源、分頁／搜尋／原始連結清冊與自動更新；沒有以展示頁呼叫publisher補數字。
[實際render acceptance](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v73/dashboard/acceptance.json)
1440／768／390／320四尺寸皆通過，四個required cards（Cnyes／Genetinfo／CMoney／PeoPo）與數量／source clock對帳，
鉅亨的1個origin暫停及重新核對時間實際可見；39頁／934來源與搜尋／filter／API retry恢復、無overflow／JS error／publisher browser requests。
已實看320px的[鉅亨卡片](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v73/dashboard/small-mobile-required-cnyes.png)。
本環境無in-app browser opener，採既有canonical本機HTTP頁與Playwright／實際截圖驗證，沒有另建Data app或重新託管網站。

### 正式驗收、速度與仍存在的source故障

focused作者／origin／UI等 **360 passed**；final full news **2,802 passed /74.26秒**；
API/bulk **69 passed /3.41秒**；selected report／retained replay **108 passed /5.33秒**。
新增33項空署名／pinned恢復及27項public-origin數值／clock／privacy／unknown語意測試；browser test亦實際核對attention篩選與局部暫停文字。
工程run保留於同一task，沒有把mock benchmark日志中的速率當成實際source上限。

[新owner baseline](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v73/runtime_baseline.json)
與[正式after](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v73/runtime_after.json)
是同一owner `73debb26f7984a7ca973264d86e1b8d9`／PID2725647，loaded policy／source SHA／WAL／heartbeat／完成owner對帳 **13/13通過**。
03:45:18.586640→03:48:23.902814 UTC的WAL窗口 **185.316174秒**新增鉅亨 **233 heads /1.257310654 new-head/s**、Global Voices16；
另鎖定的HTTP counter窗口 **182.866726秒**為鉅亨 **1.913962193 req/s**，不可混用分母。
after runtime設定0.2秒間隔／5並行是client當時的自適應設定，不等於已量得5 req/s／官方上限。
新聞cgroup CPU delta95.221871秒／同WAL牆鐘約 **0.513835 core**，after memory **257.171875 MiB**、NRestarts0；
不是整個WSL／GPU／主機負載或相同cohort causal benchmark。沒有為提速跳過compression／fsync／正文或receipt驗證。

[最近128個committed batches](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v73/recent_body_acceptance.json)
**4,105版本／4,105 current heads／712,584,378 private raw bytes**全部通過，限於該batch集合，不是whole lake／whole history。
[新owner全20啟用來源transport核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v73/transport_after.json)
accepted，取樣鉅亨406 terminal、2 awaiting_headers失敗、3gateway決策／1origin-policy決策；失敗／session恢復證據保留。
前一owner的[independent source refusals](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v73/current_source_refusals.json)
另證明CMoney 1次403；PeoPo 1次304及1次403，拒絕body未讀、來源deadline沒有被強清。
第一份all-provider transport snapshot在改parser而owner未重載時拒絕Cnyes fingerprint，保留該negative結果；
不能把它當已部署成功，後述formal acceptance使用真正重載後的新owner。

[盤點after](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v73/inventory_after.json)
03:48:25.684787 UTC：62設定／20啟用／872未註冊host keys、6,202,125 known URL、3,612,735 stored heads、
2,130,408 awaiting-authorization。鉅亨 **3,499,941 stored heads /3,594,994 known URL**、88,401 missing-body工作、53已存重驗；
9個author terminal failures已清，仍有其他identity／正文比較／空模板／原站不存在／未支援網址問題。
部分origin恢復、九個新正文、active service或這份正式acceptance都不代表934來源全抓完。
後續沿完整清冊續查可修identity／正文、原站／授權API替代、未審閱有限追加來源，並區分owner啟動成本與近期觀測的ETA分母；
來源403／429／Retry-After／REP拒絕與unknown證據始終保留，不以強行重試、空正文或backdate湊完成。

## 原生清單比較與同owner近期估時

2026-10-11，續修v74。原目標與canonical task仍active，前次v73列PROGRESS，不重做或縮小全來源範圍。

### 先盤點、再分辨來源與本機問題

[before清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v74/inventory_before.json)
04:03:19.979731 UTC：62設定／20啟用／872未註冊host keys，6,202,127 known URL、3,614,113 stored heads、
2,130,408 awaiting-authorization、192 metadata-only keys。
鉅亨3,501,237 stored／3,594,996 known，87,102 missing-body工作、5已存重驗；201筆identity terminal negatives。
`maxfinanciallife.com`的1,886筆metadata不是已確認政府／公有來源，沒有把它誤開成public-domain下載器。
CMoney271,831 pending／retry缺正文，PeoPo一筆來源拒絕；paid、第三方權利、圖片／嵌入專文與未授權資料不當成普通重試。

[實際兩筆probe](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v74/current_identity_probe.json)
透過同一NewsHTTP，先排空新聞owner並用EXIT trap恢復service／timer；其他下載器未重啟。
兩頁均真實200與公開free原件：5463919現在通過原生identity／正文；5418903的visible／current內容相符，
JSON-LD對清單標題少一個U+FEFF。保留私人Parquet／SHA收據，未把diagnostic当成已入庫文章。

`_cnyes_remove_verified_anchor_markers`只加入實際觀測的literal prefix
`鉅亨速報 - Factset 最新調查：`，必須有唯一同current-content清單連結、相同href與完整文字、單一位置的marker；
只改比較clone，不改原始正文、數字、作者或日期。未知prefix、內文marker、重複marker、不同文字／href、hidden／非清單仍拒收。
capture contract **16→17**；原v16 fingerprint被精確固定。一次性負面重排只涵蓋due、無body、無lease的支援native
identity／body declarations，保留原attempts、error、HTTP觀測clock。206筆是重排數，不是206筆成功或歷史完成。
403／429、paywall、未到期deadline、已有head與其他路徑都不重置。

[原件離線重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v74/retained_parser_replay.json)、
[真正兩筆下載](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v74/recovered_native_fetch.json)、
[獨立耐久head驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v74/recovered_head_acceptance.json)
皆通過：479／1,667 characters；原發布時間分別2023-12-25T01:05:26.000Z、2024-02-26T11:20:05.000Z保留，
作者、source/raw SHA、當前capture fingerprint及current-source reparse均驗證，不借另一頁或推測日期湊資料。

### ETA分母、重啟與顯示一致

程序平均仍是current owner新增URL heads／整個owner wall；條件ETA不再把它當近期下載能力。
capacity snapshot新增owner-window contract，counter snapshot與每個完整window攜帶canonical owner ID。
唯讀backend要求owner、aware counter clock、measurement v10與獨立window一致；計數／時間重算，
不相信冗餘rate、不挑healthy最快window、不把舊owner history挪到新owner、也不按比例切開window計數。
使用最近約300秒的整窗，最長跨度600秒；至少兩窗／90秒／10個新head。零head是實測零，但ETA未知；
缺樣本、stale、future、重疊、超量、owner不一致或長重置間隔也不推算完成時間。
小間隔、失敗與等待都計入分母；cached重播不充當online新head。
不啟用來源不建立無意義forecast object，保留全部934來源但避免每次多產生872份空估時物件。
有限cohort與不可執行網址的完成界線、舊快照明示的owner proxy、provider／origin負面期限均不放寬。

[畫面驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v74/dashboard/acceptance.json)
1440／768／390／320四種width各保留934來源／39頁；核對Cnyes／Genetinfo／CMoney／PeoPo實際counts、rate與forecast狀態，
無JS錯誤、資源失敗、publisher browser request或overflow。主agent實際檢視320px鉅亨card：
近期兩窗117.119585秒、58個新head，**29.71/min**；程序平均**32.7/min**另列。
兩者是不同分母；畫面約2.01日是當下已知可排網址的條件推算，包含大量「主文已有、網址版本待核對」工作，
不是尚缺同樣數目的獨立新聞或全媒體歷史完成保證。其他provider未量測／被拒絕仍明示未知。

### 正式窗口與失敗證據

最終exact-code回歸`v74-final-exact-edited-news-regr-20261011T041540-2726b94b`：**2,879 passed／71.54s**；
69 bulk／2.84s，245 owner／capacity／dashboard／19.02s另驗。首次fixture總數不一致的1 failed／317 passed、
首次選錯不存在test路徑的exit4、離線helper錯誤入口／欄名均留原run，修正後重跑，不抹掉negative結果。
改parser但舊owner尚未重載的[transport-before](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v74/transport_before.json)
拒絕Cnyes context fingerprint，不能叫部署成功。

新owner `a7675d44d7834c459efa7b0c93d44bb3`／PID2793207／NRestarts0；
[baseline](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v74/runtime_baseline.json)至
[formal-after](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v74/runtime_after.json)
04:20:56.370169 UTC，正式**13/13**：175.176889秒WAL新增鉅亨109／Global Voices13 heads，
鉅亨0.622228198 head/s；另167.778193秒counter window為1.144368029 req/s。
service CPU delta64.866041秒，即0.370288806 core；MemoryCurrent258.289063MiB，不是whole-host或GPU使用。
相較v73的1.25731只是不同時間／cohort／恢復階段，**不主張提速倍率**；source maximum仍未驗證。
[近期128批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v74/recent_body_acceptance.json)
4,884版本／4,884 current heads／862,519,435 private raw bytes全部驗證，限於該集合，不代表整個lake。
[全20啟用來源transport](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v74/transport_after.json)
通過觀測結構／context核對；來源502／403仍是失敗，不因audit accepted而變成取得許可。

[after清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v74/inventory_after.json)
04:20:57.417905 UTC：6,202,128 known／3,615,500 stored heads，awaiting-authorization仍2,130,408。
鉅亨3,502,544／3,594,997，85,995 missing-body／8 stored-rechecks，identity negatives由201降至2；
減少的negative可能轉為retry／其他分類，不代表199筆全部正文已成功。
尚有324 retry與91 retry-cooldown body_not_found，AMP robots502、CMoney／PeoPo403及其期限保留。
後續仍需處理實際來源失敗、可驗證正文與原站／授權API缺口；profile本次capture-contract重載的佇列初始化及capacity重新量測成本，
只在source/auth/request scope一致且有實測證據時考慮沿用capacity seed。保持單一writer、全來源清冊與unknown／拒絕邊界；目標未完成。

[12:30固定交接快照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v74/handoff_0430.json)
04:30:10.625382 UTC：同owner／PID仍running，829次請求／584個新增正文URL heads，保留9個transport與6個502觀測。
此時近期完整視窗359.634957秒為0.912035923 head/s；85,172個主文候選已存、網址版本待核對，
372個尚未找到主文／待分類，另1個已存網址重查。候選提示不是正文相同證明；此快照也不是新的whole-lake驗收。
[20來源交接transport核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v74/transport_handoff.json)
再次accepted，來源拒絕、負面deadline及失敗觀測保留。

## 全歷史掃描、收據路徑與可恢復的來源失敗

2026-10-11，v75；仍沿用 `news-adaptive-capacity-20261009`，不是另一條下載管線。
使用 `user-working-preferences` 保留完整來源與驗證；資料品質流程把可重查的未知宣告、真實非文字內容、來源拒絕分開。
只 drain／恢復新聞service與timer；其他下載器、帳號、交易、訓練與既有dirty work不更動。

### 啟動的根因與完整對照

[基線profile](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v75/owner_baseline.json)
實際canonical初始化19.5797秒；189,337份原始收據皆經過恢復入口，Path比較約307萬次。
`UPDATE ... WHERE state='inflight'` 的plan是 `SCAN articles`：即使沒有中斷工作，也掃全部約620萬列。
這份有cProfile開銷、較早cohort的數字只用來找原因，不拿來與新程序算提速倍率。

新增 `abandoned_work` 小型partial index，精確涵蓋所有inflight列，不依賴provider清單或狀態摘要猜測是否有工作。
保留獨占owner後才能恢復、清除舊lease、完整收據重播／SHA／row／contract驗證的順序與原語意；包含未註冊provider。
實際第一次建索引耗2.40755秒，只掃一次；往後初始化是IF NOT EXISTS及小型索引查詢。
[最終單一WAL對照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v75/final_fixed_wal_selection_benchmark.json)
6,202,128列、同一個0-inflight集合，AB／BA：full-scan中位3.38264秒，partial-index約0.03868毫秒；
SQLite VM work由至少18,606,000步降到小於1,000步，canonical UPDATE使用covering partial index。
這是查找步驟，不是整個啟動／HTTP提速；實際有中斷工作、未知provider、已存head與429 deadline由semantic tests覆蓋。

原收據流程先建立所有Path再排序；改為同一目錄的原檔名排序、128筆一批才建立Path。
隱藏JSON、symlink、失效symlink、.json目錄與原錯誤順序不被過濾；非JSON排除規則與POSIX sibling排序一致。
每份原JSON仍完整讀取解析，committed lookup與orphan驗證不變。沒有啟用前次已量測較慢的read-ahead或跳過收據。
[最終完整AB／BA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v75/paired_startup_comparison.json)
使用最終queue程式、同189,363份收據／同狀態集合／同partial index，不開cProfile、不發HTTP，每份收據四次皆經原恢復入口。
一次性schema／policy工作先用warmup排除，四次再量完整canonical初始化，而非只量sort函式。

| 完整warm初始化指標 | 原glob／Path排序中位 | bounded Path中位 | 本次差異 |
| --- | ---: | ---: | ---: |
| wall秒 | 10.371992 | 8.531360 | 約降低17.7% |
| CPU秒 | 10.327233 | 8.498373 | 約降低17.7% |
| peak RSS，KiB | 301,040 | 131,932 | 約降低56.2% |

所有樣本仍保留：原法9.254／11.490秒，新法7.317／9.745秒，背景負載與I/O仍有變化；
不把單個最快樣本或此warm-start效果外推成穩態下載率、官方上限、cold boot或整台WSL記憶體改善。
較早189,350份的獨立對照也保留，其wall改善只有約4.2%，不挑掉較小效果。

### 原生未知identity不等於永久不存在

[四篇目前來源probe](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v75/current_identity_probe.json)
四篇mobile／原生exp=a網址原先 `article_identity_not_verified`，重新取得後全部通過**未放寬的原parser**。
首次失敗沒有足以重播該回應的原件，不能反推它當時一定是CDN／來源壞資料；可確認的根因是排程把這類未知宣告永遠終止。
新增 `cnyes_unknown_identity_recheck_contract_version=1`，重用既有原生scope、負面觀測及有界retry／cooldown。
至少300秒後重查；burst耗盡至少一小時，原source delay更長就照原期限。每次仍要重新通過free／ID／body／author／clock／可見性。
只限已註冊允許、ongoing、沒有body的原生路徑；不擴張到未知query、AMP、finite、source-not-found、空模板、paywall、401／403／429或來源拒絕。
九筆舊negative保存attempt／error／last_checked／原due後只遷移一次；這是排程證據，不是九份正文成功。
兩筆未核可的tracking／auth-action query仍保留negative，不任意去掉query或改走其他host。

[正式補回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v75/recovered_native_fetch.json)
4 submitted／4 attempted／4 captured，4次實際200；
[耐久head與目前來源重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v75/recovered_head_acceptance.json)
4/4通過，正文長度634／241／324／271，來源原發布時計及body SHA均保留，不把diagnostic視為成功收據。
後續獨立[八篇耐久head與原件重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v75/eight_recovered_head_acceptance.json)
**8/8**通過，其中四篇由自動排程續抓；第九篇desktop exp=a仍在 `retry_cooldown`，不偽造成功。

### 同時處理其他provider的真實錯誤

[Global Voices原件重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v75/globalvoices_nontext_replay.json)
確認2012-10-21 documentary頁：native canonical／作者／時區時計／同篇CC宣告可驗，排除第三方caption後原生文字為0；
只有一張captioned photo及一個可見的 `www.youtube.com/embed/<11字ID>?feature=oembed`。
新增極窄非文字分類版本1，source fingerprint由`f3a7f593…`更新成`ed10701c…`；不改短正文驗收，不抓影片／字幕／圖片／OCR。
未知host／query／fragment／hidden／srcdoc／多重widget／未標示圖片等仍拒絕分類，真實短正文旁的未知media仍不能被接受。
[同網址的正式GET](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v75/globalvoices_current_native_fetch.json)
1 attempted／0 captured，已更新為 `source_body_embedded_only`，不再誤列parser missing body；這不是文字完整或全站完成。

### 回歸、真實入庫及仍待處理範圍

最終新聞回歸 **2,920 passed／65.28秒**，既有bulk **69 passed**，source／recovery focused **214 passed**。
新owner `b3cec51525884cf8af6f967e582942ac`／PID2872399，timer與service均恢復。
[正式同owner runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v75/runtime_after.json)
**13/13**：120.165721秒WAL窗口，鉅亨136／Global Voices4個新正文URL heads；鉅亨1.13177035head/s。
另121.680926秒HTTP-counter窗口，鉅亨167次請求／1.37244189req/s，仍有1次AMP robots502。
新聞service CPU37.410399秒／約0.311323core，期末227.214844MiB cgroup memory；不是整台主機或每秒恆定值。
[近期128批完整原件驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v75/recent_body_acceptance.json)
4,662 body versions／4,662 current heads／828,683,232 private raw bytes，限於此批，不推稱全lake完整。
[20來源當前transport核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v75/transport_handoff.json)
保留AMP502、CMoney／PeoPo403與原期限；audit accepted是觀測context完整，不是所有來源已可存取。

[全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v75/inventory_after.json)
04:56:31.900643 UTC：62 configured／20 enabled／872未註冊來源鍵，合計934；192 metadata-only keys。
6,202,128 known URLs／3,617,511 stored heads，2,130,408 awaiting-authorization URLs。
鉅亨3,504,383 stored／3,594,997 known；84,156 missing-body work含已存primary待核對的網址版本，不等於84,156個全新獨立正文。
本輪沒有UI程式變更；本機production `news/api/status`仍顯示934來源、當前owner近期速度與條件ETA，不用HTTP 200當完整性證明。
[13:02固定交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v75/handoff_20261011T050235275579Z.json)
同owner／PID running，300個本owner新正文heads，已保留7次transport與8次502觀測；
較長近期視窗324.788秒為0.855942新head/s，與較早1.13177窗口不同，不挑只快／無錯誤的窗口。
[13:05較晚20來源錯誤核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v75/transport_latest.json)
05:05:18.061603 UTC，鉅亨10個已留存failure皆在等待標頭階段，9個gateway決策／2個robots-origin決策context可驗。
來源HTTP502、CMoney／PeoPo403與cooldown都不被改成成功；這不是DNS／TCP／TLS原因或官方限速的證據。
[交接快照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v75/handoff_latest.json)
與[文件／最終檔案核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v75/documentation_check.json)
保留原失敗、較早較慢對照與每次實際程序變更的證據。
仍需續查標頭等待／持久連線、合法替代／授權API、原站與未核可query等缺口；完整目標繼續，不宣稱全934來源或全歷史已完成。

## 到期工作索引與剩餘原生失敗的正常恢復

2026-10-11 v76；持續原任務，未改正文／作者／source clock／rights／robots／pacing／拒絕deadline。
沿用「工程與研究偏好」的共享根因與完整證據原則，資料品質技能只作原件與既有失敗的scoped companion。

### 根因：優先序索引不等於到期索引

[原owner錯誤](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v76/transport_before.json)
05:10:42 UTC有13個Cnyes ReadTimeout／ReadTimeoutError／TimeoutError，全部在awaiting_headers；news host 2、mobile host 11，約25秒。
這只能定位到request wrapper在收到標頭前失敗，不證明官方限速、DNS、TCP、TLS或伺服器是哪一層。
[Requests官方文件](https://requests.readthedocs.io/en/latest/user/advanced/#timeouts)也區分connect／read timeout與wall clock；
現有thread-local session、完整stream read／with-close及失敗reset仍保留，沒有盲目縮短合法慢回應期限。

[20秒、49Hz原程序取樣](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v76/live_owner_stacks.txt)
共1,172個active stack samples，其中584個含discovery_candidates（含一個query-builder stack），約49.83%；583個在執行SQL的原方法。
當時Cnyes285,029筆discovery中277,015筆active；277,015筆沒有到期工作，舊priority索引仍每輪逐列檢查due。
優先序索引provider→kind→priority→due無法在不給定前綴時直接range seek due，網路以外還有確定的本機空掃描。

### 共用修復：空、稀疏、密集三種物理計畫，結果不變

[NewsQueue](../downloader/news_fulltext_queue.py)新增partial index
`discovery_due_frontier(provider,due) WHERE superseded_by_windows_json IS NULL`。
先用同一實際cutoff讀最多257個常數列：零列立即返回；1–256列用due索引篩出完整到期集合後按原ORDER排序；257列則保留原priority索引早停LIMIT。
probe不含權限判定，也不授予權限：原endpoint白名單／bulk adapter與endpoint circuits／robots origin／finite scope／所有tie及LIMIT都留在完整selection。
沒有快取negative、沒有借用狀態gauge；新插入／due更新立即可見。密集模式不只挑lookahead的256筆，遠端高優先序仍能被選中。
`discovery_readiness_probe_contract_version=2`公開在owner status；discovery_dispatch成本與idle／sparse／dense計數持續記錄。
索引／query plan不改source data fingerprint、發布clock或已儲存head；不重設校準、source refusal或配額。

[固定真實WAL、20啟用來源ABBA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v76/fixed_wal_discovery_dispatch_v2.json)
每來源2輪，共160個有界選取，實際row內容／順序與完整reference逐項相等；只讀，不建立第二writer、不請求publisher。
Cnyes277,015列／0 due：reference wall中位0.056672705秒、CPU0.056687915秒、VM steps至少1,662,000；
candidate wall0.000013767509秒、CPU0.0000156195秒、VM steps低於1,000。
這是約4,116倍的空佇列查詢差異，不是來源吞吐量、整機CPU、cold boot或全歷史完成倍數。
Global Voices真實密集snapshot保留：candidate比reference多約0.169毫秒；沒有刪掉慢案例或說所有來源都提速。

[v1稀疏慢案例](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v76/fixed_wal_discovery_dispatch.json)
Cnyes12 due時只做existence guard仍要掃優先序frontier，reference39.13毫秒、candidate42.57毫秒。
所以未以v1「空佇列成功」結束；v2增加稀疏due plan。
[語意測試](../test/test_news_discovery_readiness.py)涵蓋0／1／256／257／2,048 raw due、負due、未到期、superseded、同priority tie、LIMIT、已移除endpoint、bulk-disabled、origin排除、endpoint冷卻與finite不允許discovery。
SQL query使用明示partial index時，SQLite在finite的constant WHERE 0會回no query solution；已保留finite無hint的no-work查詢並完成回歸，未改為允許finite discovery。
[readonly配對工具](../scripts/benchmark_news_discovery_dispatch.py)保留query_only、固定WAL、完整row-order parity、bounded cycles、無publisher request與code SHA。

### 九筆舊identity終於全數有原件，不把診斷算下載

第一次probe因真實provider cooldown未送出，證據保留。
[到期後native probe](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v76/remaining_identity_probe_v2.json)
4761357?exp=a實際200；canonical／當前article ID／payment0／作者／原始2021-11-04T07:56:02.000Z時計／可見正文與宣告吻合，原parser已可驗。
05:19:53已由正式服務正常排程保存754字正文；這不是新parser放寬，也不能由此倒推初次失敗的回應內容或原因。
[指定canonical fetch](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v76/recovered_identity_capture.json)
0 attempted／0 captured，因drain後重新核對已有head而未重抓，不把0 counts誤稱來源失敗。
[九份完整原件／當前source replay](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v76/nine_recovered_heads_acceptance.json)
9/9：長度634／241／324／271／568／411／253／1,104／754；request／final URL、raw SHA、body SHA、capture contract與原發布時計皆核對。
兩筆未核可tracking／auth-action query依舊negative，沒有去掉query、改host或借用另一篇的body／作者。

### 部署、進度、資源與剩餘問題

全部新聞 **2,946 passed／65.12秒**；discovery／finite／bulk／robots focused **163 passed／4.21秒**，含69 bulk。
原Prototype的兩項focused失敗、finite partial-index回歸、artifact helper未以module啟動的ModuleNotFoundError，以及第一次readonly benchmark的finite錯誤都保留原failed run；
已由後續語意／全部回歸及正式操作驗證取代，不將原失敗改寫成功。
[索引／probe操作](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v76/apply_index_and_probe.sh)
與[指定capture核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v76/capture_recovered_identity.sh)
只排空news service+timer、核對MainPID=0、唯一owner實作、EXIT trap恢復兩者；其他下載／交易／訓練服務未停止。
首次restore的正常owner已建立index，後續profile只驗證既有index；不把微秒IF NOT EXISTS時間說成首次建索引時間。

[正式同owner runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v76/runtime_after.json)
13/13，owner83d583c1a6e7454996a7f759afab4520／PID2921761／NRestarts0。
186.048021秒WAL窗口新增Cnyes363／Global Voices17個正文URL heads；Cnyes1.95110917新head/s。
另182.694562秒HTTP-counter窗口Cnyes464 requests／2.53975813req/s，仍有3次transport與5次502。
baseline→after service CPU48.909772秒，約0.262888core；期末279.25MiB cgroup memory，非整台主機資源或同來源工作量的speedup比較。
[最終20秒／49Hz取樣](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v76/final_owner_stacks.txt)
229個active samples中2個含discovery方法；同取樣設定但不同publisher workload，不把它當整鏈路倍速證據。
[近期128批原件](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v76/recent_body_acceptance.json)
4,389 versions／4,389 current heads／779,544,178 private raw bytes全部核對；僅此cohort，不宣稱整lake完整。

[20來源當前錯誤context](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v76/transport_after.json)
05:24:55 UTC：Cnyes3個awaiting_headers ReadTimeout、5個host-scope gateway決策、1個robots-origin決策。
來源502／逾時／CMoney與PeoPo403仍留存並按原deadline重試或等待，不以HTTP200／audit accepted偽造全健康。
[全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v76/inventory_after.json)
05:21:50 UTC：62 configured／20 enabled／872未註冊來源鍵，共934；192 metadata-only。
6,202,184 known URLs／3,619,582 stored heads；2,130,443 awaiting-authorization URLs。
Cnyes3,506,335 stored／3,595,001 known，82,208 missing-body work是工作狀態而非不同文章的完整全站總數。
本輪不改UI／存取權限；既有唯讀production API／934來源列表繼續連動current owner進度與含錯誤／等待的conditional ETA。
[只讀交接核對工具](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v76/verify_v76_handoff.py)
檢查code SHA／文件links／九份head／20錯誤context／13 runtime checks／current heartbeat與公開全清冊，沒有provider requests或DB writes。
[13:26固定交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v76/handoff_20261011T052637163793Z.json)
同owner／PID仍running：本owner已新增595正文heads／885 requests，7 transport errors／10次502；保留較晚錯誤，沒有只挑先前較快窗口。
公開current-owner近期320.339秒為1.857406新head/s，conditional ETA約43,969.9秒只適用已知81,670個missing-body工作，不是934來源全部／全站歷史完工時間。
後續仍須研究來源標頭等待與connection/pool隔離、其餘native body failure／合法原站與授權API缺口；保留全934來源目標，不宣稱全完成或最高頻率已驗證。

## 原站連線池故障隔離與行情片段的獨立分類

2026-10-11，工程續修v77。沿用同一新聞queue／HTTP owner／限速器、原件、收據、錯誤證據與唯讀網頁。
`news-adaptive-capacity-20261009`仍active；934來源的全量實作／授權／全歷史目標尚未完成。

### 原因與共享修正

[初始20-provider transport核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v77/transport_before.json)
05:32:39 UTC的既有owner有17個Cnyes標頭前ReadTimeout（mobile13／news4）、15個host gateway決策與2個robots-origin決策。
每次transport exception關閉該thread的整個Session：同thread之前服務的其他Cnyes原站、其他provider之健康池也被discard。
這是可從實作與實際TCP配對驗證的本機故障範圍；不是來源502或25秒標頭等待的根因證明。
Requests官方[Session／HTTPAdapter契約](https://requests.readthedocs.io/en/latest/api/#requests.Session)
明載Session.close關閉其adapters、adapter.close清理池；因此整Session reset具有跨origin的連帶影響。

`OriginConnectionSession`仍保留Session的cookies與標準送出／TLS行為；以canonical scheme＋authority作connection key，path／query不建立新池。
每serial worker最多十個LRU origin adapters、每池一條可復用connection、`max_retries=0`；滿額關閉被淘汰池，shutdown關閉剩餘池及Session預設adapters。
**不重送來源請求、不提高配額、不關閉TLS驗證、不導入proxy／.netrc／browser cookie**。
RequestException只discard該origin；其他健康池保留。連線cleanup錯誤有獨立counter，不能遮蔽原始source exception。
未知的注入／legacy transport沒有可驗證origin池，保留舊整Session恢復，另計`failed_sessions_reset`，不謊稱已隔離。
route／TLS／連續未恢復的publisher circuit、host gateway與robots拒絕／deadline等原規則仍生效。
connection隔離marker為1；transport audit artifact升為6，明列舊owner marker缺失及`failed_origin_pools_reset`／cleanup errors。

### 實際TCP、同原件與完整capture步驟配對

[可重跑工具](../scripts/benchmark_news_connection_recovery.py)
只使用三個**自有synthetic loopback**來源：同樣27個article attempts、21個正文、6次真實peer斷線、3個robots請求。
每phase包含metadata入queue、canonical請求／解析、失敗入queue、Parquet壓縮／fsync、收據SHA、SQLite commit與原件／head驗證。
八個ABBA phases固定URL／原始body／title／作者／發布時鐘／source fingerprint／request量／retry30秒，沒有production queue／rate-state writes或外部publisher requests。
不包含正式owner啟動、WAN、TLS handshake、全部歷史或所有來源；不得把此benchmark當publisher throughput或官方上限證明。

| 六次故障下的新建TCP | 整Session重置 reference | origin隔離 candidate |
| --- | ---: | ---: |
| 健康原站publisher.test | 7 | 1 |
| 故障原站m.publisher.test | 6 | 6 |
| 另一健康provider other.test | 7 | 1 |
| 合計 | 20 | 8 |

[最終全程原件配對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v77/paired_connection_recovery_accepted.json)
八phase均raw語意／請求數／失敗期限／正文收據parity通過，連帶新建connection減少60%。
**最終wall中位140.467→198.859毫秒、process CPU129.265→210.629毫秒，candidate較慢**；原始八phase完整保存，不能只選快的phase。
[較早同fixture測量](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v77/paired_connection_recovery.json)
及[中間測量](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v77/paired_connection_recovery_final.json)曾較快，來源parser版本／背景工作及短窗口不同。
因此目前只接受「健康connection不被故障連帶銷毀」與bounded recovery，不宣稱CPU／wall全鏈路已提高或比v76來源窗口快。
這輪沒有以取消驗證／body proof、少抓URL或清除來源等待期限來美化速度。

### 原站行情片段，不是可補造的新聞正文

[四個到期失敗的正式原件probe](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v77/remaining_native_failure_probe.json)
是同路由的4個HTTP200原件：news/1766727、1766169、1606199、1605445，2013年發布。
可見／current／schema三種representation對應同一ID、headline、免費payment0與aware clock，但內容只剩ticker／價格或價格／變動百分比／公司名稱。
其中schema可省略visible的空白；同原值的符號邊界足以分隔price與signed change，不能假定必有空白。

共用parser沿用既有短文identity／full-native-content validator，僅對完整匹配的bounded行情語法、可見且無另一image／video／iframe／table representation的原件回報`source_body_quote_only`。
未知短文、正常快訊、付費、hidden、current/schema/visible差異、wrong ID／clock／canonical與另一media representation皆不能套用。
這不是新聞body成功；metadata與私人raw保留，不合成敘述、不虛構歷史發布時間、不抓第三方media。
新的HTML outcome fingerprint為`verified_native_quote_only_outcome_contract_version=1`，既有HTML capture version17保持；此欄從bulk fingerprint移除，**沒有重開健康API歷史窗口**。
以前已存的正文／FP不改寫不刪除；僅將當前真實來源結果獨立列出。

[原件重播4/4](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v77/native_quote_source_acceptance_final.json)，
[正常canonical指定取得](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v77/selected_quote_source_outcomes.json)
為4 submitted／4 selected／4 attempted／**0 captured正文**，4個source outcome；claim保留原retry due，沒有強制deadline歸零。
[當前exact-URL queue／新source FP核對4/4](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v77/native_quote_queue_acceptance.json)
都無stored head，state/error同為source_body_quote_only，沒有把raw probe當accepted article。
[九篇先前正常原件9/9 current-parser重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v77/nine_native_positive_acceptance.json)
正文／title／author／clock仍相同；原始capture FP仍是舊ABI，`capture_contract_matches_current_source=false`正確保留，沒有回填新FP到舊收據。

### 正式運行、網站與完整清冊

[baseline](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v77/runtime_baseline.json)
05:45:17.990481 UTC至[正式runtime acceptance](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v77/runtime_after.json)
05:48:40.293104 UTC，同owner `322c29885be042f8bcc3dfc6fd2ecaa0`／PID2972797：**13/13** engineering checks通過。
WAL窗口202.302623秒新增Cnyes194／Global Voices19個stored URL heads；各0.958959／0.093919新head/s。
HTTP counter窗口198.785317秒，Cnyes323 requests／1.624869req/s，包含1個transport error與6個terminal502。
transport audit在稍後另一cutoff保留5個gateway decision；這是不同clock／計數單位，不能說只有5次HTTP502。
來源／workload不同於v76，這是現在實測而非paired publisher倍速。

服務CPU17.793981→54.768170秒，窗口CPU36.974189秒約0.183 core；期末systemd MemoryCurrent約253.855MiB、MemoryPeak約287.488MiB，並非process RSS。
這只是該news systemd service的同owner窗口，非全機／WSL／GPU用量，也不是正式memory節省比例。
[最近128 batches](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v77/recent_body_acceptance_final.json)
驗證5,484 body versions／5,484 current heads／982,080,732私人raw bytes。
[20-provider當前transport核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v77/transport_after.json)
確認loaded isolation1，該owner實際Cnyes一次failed origin pool reset、零整Session reset、零cleanup errors；1個awaiting_headers失敗仍保留，沒有推斷是DNS／TCP／TLS／publisher quota。

[全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v77/inventory_after.json)
05:48:41.806450 UTC：62 configured／20 enabled／872 unregistered，合計934；192 metadata-only source keys。
6,202,186 known URL／3,622,415有stored head／2,130,443待啟用／授權；不能把configured／可見清冊當已實作全正文。
Cnyes3,595,003 known／3,509,032 stored／79,496 missing body work；current states17個quote-only、130retry／323cooldown等仍分列。
stored head與complete state不相同：保留舊正文的recheck或失敗不會刪掉head；candidate／alias URL不等於全站不重複文章數。
來源502／ReadTimeout／CMoney與PeoPo403仍存在；既有robots、授權API尚缺實際interface／scope的來源仍不能宣稱取到全文。

進度頁原位沿用來源card／排序／分頁與只讀API；新增「原站只提供行情片段，未取得新聞正文」，不算成功、不把不可取得誤列排程倒數。
125共享gateway regression通過後只reload其只讀service；未重啟交易／其他取得服務。
[本機rendered驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v77/dashboard_acceptance/acceptance.json)
1440／768／390／320四viewports、每個934來源／39頁、Cnyes新label／數值、原始URL保守分母、搜尋／排序／reset與無publisher requests均通過。
採cpu-2d Chromium，不證明GPU/WebGL／WAN／真實browser冷啟動；無in-app opener／Data dependency tool，沒有另造Sites或receipt平台。

### 回歸、負例與接手範圍

最終完整news＋69 bulk合計**3,057 passed／62.08秒**；focused **459 passed／21.45秒**，gateway **125 passed／26.44秒**。
已覆蓋header／body transport失敗、另一host／provider健康池復用、真route／TLS circuit、cookie原樣、隱藏重試0、LRU上限、thread-local／close、cleanup error與original exception；
quote identity／free／clock／body三方、可見media負例、HTML FP變／bulk FP不變，以及公開狀態／原件不外流。
完整失敗run也保留：benchmark先用了不存在的`original_url`（canonical欄為requested_url）、最初diagnostic測得3/4因schema價格間空白、指定測試檔路徑錯誤、FP測試錯用不存在bulk屬性。
皆由相應實際修正／source4/4／459＋3,057回歸取代，不抹除最初負例、較慢phase或過程服務drain。

接續：追查其他native mismatch／標頭等待、縮減冗餘計算，且已發現host checkpoint restore仍以whole data FP判定，應分開body parser版本與尚未到期的負面連線證據，避免日後parser更新清掉technical pause。
此既有行為尚未在v77改寫；不得聲稱所有negative checkpoint在任意FP更新下都已驗收。
授權／來源拒絕和872未註冊source不因local acceptance解決；全934來源目標保持active，完成與blocked均需對應整體證據。

[固定交接13:54:46](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v77/handoff_20261011T055446108147Z.json)
13項交接checks、20份manual files／16個local report links通過；同owner／PID仍running，timer與readonly gateway正常。
較晚owner已記6個transport failures／8次502（比正式窗口更多，完整保留）；公開最新342.842秒／7個連續窗口有474個新head，1.382562新head/s。
conditional ETA57,133.05秒約15.87小時只指當時已知78,990個remaining body work，含錯誤／等待，不是全934來源或全站歷史完成保證。
[可重跑交接核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v77/verify_v77_handoff.py)
只讀queue／localhost status，不呼叫publisher、不改deadline；每次另寫immutable timestamp，原snapshot保留。

[較晚13:56:57交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v77/handoff_20261011T055657516581Z.json)
同owner已新增884正文URL heads／1,335 requests，10個transport failures／8次502；13checks、20manual files／18local links通過。
這份更晚的錯誤數同樣保留，不能只用較早6個timeout或較快／較乾淨窗口；沒有來源端逾時已全部根治的宣稱。

## 解析器升版不清除原站故障證據與帶漲跌額片段分類

2026-10-11，v78；沿用canonical news owner、HTTP limiter、raw／Parquet／SQLite receipt與唯讀進度頁。
工程與研究偏好技能要求修共用故障邊界、保留失敗與較慢phase；資料品質技能僅作本輪原件的scoped companion，檢查同篇identity／body／clock，不另建notebook、分析平台或出版surface。
Data dependency loader不可呼叫；已使用既有local authoritative diagnostics、queue與canonical auditor。此輪沒有改金鑰、交易、GPU、其他下載器或冷庫。

### 慢與錯誤的現有證據

[修正前全部20啟用來源的transport核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v78/transport_before.json)
是owner `322c29885be042f8bcc3dfc6fd2ecaa0` 的1,918個鉅亨terminal requests，18份awaiting_headers失敗／8份gateway決策；sample／counter／HTTP status不是同一單位，不互換計數。
later owner telemetry另有22次header transport failures與13次terminal502，保留更晚的錯誤，不能只用初始較低數。
news／mobile的sum header wait遠大於收到headers後的body處理；這是wrapper phase、可重疊的worker時間，不能說量出了DNS／TCP／TLS／server各自延遲。
ReadTimeout本身也不證明來源限流；[Requests timeout文件](https://requests.readthedocs.io/en/latest/user/quickstart/#timeouts)
說明socket等待與完整下載時間不同。本輪未縮短25秒read timeout、改45秒response檢查或解除robots等待來製造吞吐。
502代表gateway／proxy收到上游無效回應，不等於全部article被禁，亦不證明哪一層壞了；參見[HTTP semantics 502](https://www.rfc-editor.org/rfc/rfc9110.html#name-502-bad-gateway)。

[修正前完整source inventory](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v78/inventory_before.json)
維持62 configured／20 enabled／872未註冊source keys＝934，192 metadata-only keys。
鉅亨3,595,003 known／3,510,178有stored head／78,268 missing body work；這是candidate URL grain，不是全站distinct article或去重訓練文章數。
其他provider包括CMoney／PeoPo拒絕、民視attribution、paywall、404、media-only／rights-scope缺口仍明列。
沒有使用替代host、identity或擴大授權來忽略真正的來源拒絕，也沒有將此清點說成934來源都有成功正文。

### 根因與共享checkpoint修正

原`GatewayHostCircuit` restore要求`row.source_data_fingerprint == current source.data_fingerprint`。
whole FP包含正文parser／classification版本，parser升版即丟掉同provider／exact host未到期negative，甚至丟掉v3已到期但仍應保留的progressive失敗證據。
原`_checkpoint`又一律寫current FP，會把未重新觀測的old negative重新標記。兩者都混淆「正文能否按本版接受」與「原host尚未恢復」。

共用checkpoint升到4、另留gateway decision contract3，已發表舊events不改寫；owner metrics新增loaded checkpoint marker，transport auditor contract7分別核對loaded／legacy absence。
只恢復當前仍allowed且同provider的exact normalized hostname，期限／generation仍按30、60、120、240、最多300秒驗證。
v1／v2僅保留未到期的原bounded期限，不虛構舊generation；v3／v4保留已觀測generation、failure_count，expiry只允許下一次真實probe，不恢復positive health。
originating negative FP另存，unrelated host checkpoint不重標、不延長；真正的新failed probe才給新的negative其當時FP，真正的completed HTTP成功才清該host。
bad row以單筆隔離，userinfo／port／path／query／fragment／case alias／wrong scope／invalid hash／nonfinite或unbounded deadline不承接負證據。
當前授權expiry、robots、publisher-wide refusal／Retry-After仍在send前執行；ordinary healthy success沒有新增checkpoint I/O，新增negative-map cleanup只在實際recovery發生。

[focused recovery測試收據](../artifacts/operations/agent-workflow/runs/v78-exact-host-and-clock-regress-20261011T060551-1ec28e1e/run.log)
為184 passed；[最後source與checkpoint focused](../artifacts/operations/agent-workflow/runs/v78-signed-quote-original-and-id-20261011T061005-8bdb15ad/run.log)
為369 passed，含v1-v4跨FP重啟、原期限與FP不變、expired legacy與progressive區別、bad first row不消掉good second row、provider reassignment、actual before-send cooldown及最新authorization拒絕。
最初7項失敗保留：六項fixture混用真／虛擬時鐘，一項抓到normalize URL後仍須核對netloc＝exact host；不是把測試放寬或修改source deadline。
本輪讀取的當時production gateway checkpoint為v3空providers，不能聲稱有實際live nonempty跨FP migration；各legacy migrations是明確的controlled語意驗證。

### 四份原件，不把行情stub當新聞

[新到期missing-body原件probe](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v78/remaining_native_failure_probe.json)
四個同路由HTTP200；news/1587857、1587836的visible／schema／current只剩ticker、價格、signed change與colon。
schema可省略這些數值邊界的空白，current free0、sameID、headline與aware published clock均一致。
news/1454190、1454141原本已能分類quote-only。最初[重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v78/native_source_replay.json)
誠實回報2/4仍body_not_found，不把raw200當成功。

行情語法只增加ticker／price之後的可選單一signed change，仍須完整anchored match、少於100字、existing canonical／visible／current／schema identity與body一致，且無另一media/table representation。
中文敘述、英文正文、雙漲跌額、壞sign、percentage未驗證變體、different clock／canonical／paid／hidden內容皆不套用。
HTML `verified_native_quote_only_outcome_contract_version`＝2；這個HTML-only欄位仍從bulk FP移除，**健康API歷史窗口身分不變**。
raw／title／body／author／發布時間不改、不合成原文；舊body capture FP不回填成新parser版本。

[目前parser原件重播4/4](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v78/native_source_replay_final.json)
全部source outcome verified／diagnostic integrity verified，parser_verified false正確表示不是accepted article。
[既有九篇真正正文重播9/9](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v78/nine_native_positive_acceptance.json)
body／headline／author／clock保留，舊capture FP仍不冒充當前版本。
正式selected fetch保留原due／lease與300秒recheck條件；已到期才實際送出，絕不為了驗收把deadline清零。

### 效能、部署與接手邊界

[最終完整news與bulk回歸](../artifacts/operations/agent-workflow/runs/v78-final-all-news-regression-20261011T061103-e137b23f/run.log)
為**3,110 passed／74.39秒**；pre-classification回歸3,099／61.37秒亦保留，不把不同case數／同機負載的wall當產品速度比較。
[新code owned-TCP全capture配對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v78/paired_connection_recovery_final.json)
八個ABBA phase／同27 article attempts＋3 robots／21accepted bodies／6transport faults；body／author／clock／raw SHA與耐久收據相同。
健康publisher／other的TCP新連線各7→1、故障host仍6，總20→8；這是v77故障池隔離持續有效的proof，不是v78降低官方publisher latency的測量。
reference中位wall0.110569秒／CPU0.105970秒，candidate0.123835／0.124069秒，**仍較慢**。
[較早同輪配對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v78/paired_connection_recovery.json)
較接近且candidate微快，同樣保留；不挑最快phase、不給wall／CPU／WAN／TLS／publisher倍速或「理論極限」結論。

only news owner／timer按EXIT trap drain及恢復；只讀gateway、其他下載器、交易服務未重啟。
[正式selected查詢](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v78/selected_signed_quote_outcomes.json)
為4 submitted／4 selected／4 attempted／**0 captured正文**，四個source_body_quote_only；不是以先前diagnostic raw代替正式取得。
[四筆current source policy與exact URL落地](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v78/native_quote_queue_acceptance.json)
4/4符合state／error、沒有previous stored body，當前FP正確；
[部署前gateway checkpoint副本](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v78/pre_final_gateway_checkpoint.json)
為v4空providers，與前述controlled migrations的界限相同。

[正式baseline](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v78/runtime_baseline_final.json)
06:15:48.734080 UTC到[runtime acceptance](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v78/runtime_after.json)
06:19:12.167291 UTC，owner `9520e91acd1b4c4b90cca81e2d9e855c`／PID3038843／NRestarts0：**13/13** engineering checks。
WAL窗口203.433211秒，鉅亨新增94／Global Voices19個stored URL heads，0.462068／0.093397新head/s。
HTTP counter窗口215.325225秒，鉅亨208requests／0.965981req/s，含3transport／2server errors；兩種clock不能互換分母。
服務CPU窗口33.422006秒，平均0.164290 core，期末systemd MemoryCurrent292.875MiB、Peak298.570MiB；非process RSS、全機／WSL／GPU數據，也非paired resource reduction。
本輪有20秒／49Hz bounded py-spy observer；窗口包含普通排程、failure、idle與diagnostic觀察，沒有用不同cohort證明速度提高。

[128個最近raw batches](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v78/recent_body_acceptance.json)
驗證5,600 body versions／5,600 current heads／1,009,487,922私人raw bytes，沒有以HTTP200代替raw SHA／receipt／head驗收。
[20-provider transport核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v78/transport_after.json)
在06:18:32較早cutoff接受loaded checkpoint4／connection isolation1、全host counters reconciliation與private events。
該cutoff保留2個awaiting_headers失敗／1份gateway決策，實際failed origin pool reset2、全Session reset0；較晚正式窗口已有3transport／2server error，兩份證據都保留。

[完整最新inventory](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v78/inventory_after.json)
06:18:34.916965 UTC，6,202,187 known URL／3,624,561 stored heads，2,130,443待啟用／授權；62／20／872與192 metadata-only source keys維持。
鉅亨3,595,004 known／3,511,019 stored／77,375 missing work，當時152個quote-only、100retry／218retry cooldown等均分列。
source outcome terminal不等於歷史完整；現在約7.7萬missing body work，不包括2.1百萬awaiting authorization或未知全站歷史。

[唯讀網頁四viewport驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v78/dashboard_acceptance/acceptance.json)
1440／768／390／320皆934來源／39頁、來源搜尋／排序／reset、counts與quote label／origin暫停clock正確，實際desktop／small-mobile畫面已檢視。
新owner近期sample未足時顯示未知／蒐集中，不借舊owner的ETA；沒有重啟readonly gateway或做新app。
Chromium cpu-2d／localhost讀取，不證明WAN、GPU／WebGL或真正user browser冷啟動。

[live active-Python profile](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v78/live_python_profile.txt)
20秒49Hz共201 active samples、零observer errors，107不同stack。
leaf位置有25個`NewsQueue.select_candidates.lookup`、12個original URL routing、10個status、8個discovery status samples；
這是後續SQL／routing／重複維護工作的定位線索，不是27%wall節省證明，不替代同一WAL／完整capture的paired效能驗收。
接續沿共享candidate／routing與header恢復做精確工作量測試，保留所有guards、完整原件和publisher pacing，再決定優化。
934來源、其他native representation缺口、header failures與實際授權介面繼續；本輪局部修復／tests／raw replay不等於全來源／全歷史完成。

[當前owner交接核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v78/handoff_20261011T062222964792Z.json)
於14:22:22臺北時間通過**11/11**，包含本輪13個手動修改檔、文件連結、正式runtime／原件／配對證據、四寬度網頁、未知query保留與current PID／owner一致。
當時新owner已新增191個鉅亨stored URL heads、送出382個requests；5個transport errors與2個502仍明列。
近期同owner155個新增heads／318.035628秒，0.487367新head/s；158,511秒的條件ETA僅涵蓋當時77,253個已知待補正文URL，不包含未授權、未知全站歷史或將來新增資料，亦不是完成承諾。
此核對沒有呼叫publisher、寫queue、重排未知query或重啟其他服務；goal／原工程任務保持active，下一輪按相同工作量驗證candidate lookup／original URL routing開銷及標頭失敗恢復。

## 原網址路由按清冊權限增量與有界批次查詢

2026-10-11，v79。前一輪有code／收據／migration修正與formal資料驗收，是progress而非status重述；此輪仍沿`news-adaptive-capacity-20261009`及934來源原目標，沒有改寫成功條件。
工程偏好技能促使本輪保留全原文／metadata／發布時鐘／收據與較慢測量，沿canonical owner共享入口修復；沒有另建下載器、移走權限限制或使用其他服務配額。

### 根因、範圍與版本

[修正前934來源inventory](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v79/inventory_before.json)
為62 configured／20 enabled／872未註冊，192 metadata-only；6,202,187 known URLs／3,625,055 stored heads／2,130,443待啟用或授權。
[20啟用provider transport](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v79/transport_before.json)
核對接受、沒有auditor errors；鉅亨884 terminal requests中有9份awaiting-headers private failures、6個gateway decisions、3個robots-origin unreachable／暫停決策。
news host3／mobile host6個header failures，failed origin pools reset9、whole sessions reset0；其他enabled provider該observer窗口沒有private transport failure，不代表它們沒有rights／404／image-only等缺口。
最初兩個audit直接script argv因repo import root失敗，保留run；改用canonical `python -m`後才取得上述證據，沒有向publisher重送請求。

原`initialize_original_urls`以整個`policy_fingerprint`判定routing失效；此值包含各source的`data_fingerprint`，正文parser升版也會重排保留literal proofs。
修正前正在重掃的proof metrics有693,622個未linked target、252,395個linked target的literal alias links，合計946,017；還有339,000 opaque links與兩個unresolved。
這些是metadata proof／alias粒度，並非946,017篇可取得正文。舊routing每一個proof都另外查目標是否存在，即使target_queued早已證實或原站根本未准許。

`ORIGINAL_URL_ROUTING_CONTRACT_VERSION`＝2：專用identity只納入resolver version、provider／domain映射、allowed、authorization evidence FP、article namespace與continuous／finite模式。
不納入正文selectors／parser／data FP、pacing、parallelism或API端點；新metadata revision仍走原本同步upsert，不因路由完成而忽略後續標題／日期。
原`google_literal_url_routing_v1`key維持resolver-v1命名，**內含routing checkpoint contract2**；不是把URL解析本身升到v2，也不改body acceptance／bulk FP。
legacy整個policy仍能證明當前才保留原`after`，原checkpoint與replacement記錄為私有upgrade evidence；不符合就從metadata routing起點重新核對，不能猜已完成或清掉HTTP拒絕。
v2既有cursor只在上述routing scope改變時重新評估。checkpoint／upgrade evidence同transaction，storage failure不發布半個migration。

本輪只合併存在性seeks：原順序逐alias `add_articles`保持最大raw date、最後非空title、first／last seen更新及finite／namespace gates。
對已證實target_queued的proof省掉冗餘存在性query；其餘每400個bind查一次，UPDATE只由0→1，沒有create missing unreviewed targets、copy stock associations、接受body或宣告permission。
SELECT keyset、原有row上限／time slice／stop與transaction仍有效；grouped seek額外工作亦受最多2,048-row的既有slice上限限制。

read-only status projection沒有acquisition owner cache；full回歸發現兩處`AttributeError`後，共享progress改成被動讀durable marker。
legacy顯示version1、malformed／absent顯示unknown／invalid，而非因Python較新就說已升版；唯讀觀察不修cursor或啟動owner。

### 配對與正確性，不挑較快結果

[初次三窗口配對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v79/paired_original_routes.json)
每窗256 retained aliases、52份真sealed body版本，table／raw／receipt recovery皆一致；其中current keyset的routing wall與CPU反而較慢。
[最終配對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v79/paired_original_routes_final.json)
是另一個pinned WAL，三個256-alias窗口，每窗12 phases、共36，**同窗old／new使用完全相同來源**；本次sealed batch為60版本，不拿52與60的cost做前後比較。
三窗article／source_links／literal proofs／metrics／finite cohort全表digest一致，60份正文／作者／發布與capture clock／raw／Parquet SHA一致；temporary receipt-before-SQL replay後canonical verifier同樣接受。
沒有改寫歷史日期或capture FP，local metadata clock僅在isolated benchmark固定以比較有順序的upsert；沒有操作live collector的時鐘或queue。

| 固定WAL窗口 | routing wall reference→candidate | routing CPU reference→candidate | 完整route／sealed replay／receipt／recover／verify wall reference→candidate |
| --- | --- | --- | --- |
| first literal prefix | 24.91→28.71 ms，較慢 | 14.57→14.99 ms，較慢 | 622.32→596.95 ms |
| current keyset | 32.06→24.27 ms | 14.23→14.14 ms | 468.87→447.53 ms |
| last keyspace | 27.91→28.93 ms，較慢 | 15.41→14.76 ms | 447.69→357.85 ms |

完整replay CPU第一窗508.37→531.36 ms亦較慢；其他兩窗412.13→400.87、404.22→315.39 ms。全部phase與原較慢值留在artifact。
此cost排除temporary fixture seed；有同工作量的ordered routing窗口與完整durable replay窗口，**不是官方HTTP／TLS／WAN／整個daemon啟動的速度proof**。
SQL query-plan只能協助定位，非可依賴的永久格式或效能保證；採用[SQLite官方EXPLAIN QUERY PLAN說明](https://www.sqlite.org/eqp.html)。此輪沒有新增hypothetical partial index來冒充production測量。

[最新完整news含bulk回歸](../artifacts/operations/agent-workflow/runs/v79-last-full-news-bulk-and-bind-20261011T064200-cc3c9f5b/run.log)
**3,138 passed／68.87秒**；[最後focused](../artifacts/operations/agent-workflow/runs/v79-final-bounded-routing-and-re-20261011T064052-8b5f5c70/run.log)
262 passed／4.01秒，包括801 targets分成400／400／1 binds、duplicate targets逐序metadata、unreviewed／due／lease／stored body／association不變、stop不前進與rollback。
pre-bind-bound full3,137／60.61秒亦保留；首個full回歸的兩個readonly cache失敗由共享durable-marker修正及新full驗證，不刪失敗、不單靠focused說full通過。

### 正式續接、正文與進度網頁驗收

只drain news owner／timer，EXIT trap恢復；公開readonly gateway、其他provider collectors、交易、GPU與冷庫未重啟或更動。
[drained原checkpoint](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v79/deployment_before.json)
保留原`after=895ec547…`、whole policy FP及兩個protected unknown queries；沒有改due／lease以求快轉。
[實際migration 7/7](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v79/deployment_acceptance.json)
14:42:37臺北時間，新owner `79a0e265ff2b4cb8961dab6e0bf17dcf`／PID3094457，raw old checkpoint與upgrade evidence完全一致。
upgrade replacement仍從原`895ec547…`續接，觀測current cursor已正常到`8d07bebb…`，loaded routing2／新scope FP正確，body whole-policy FP不變、unknown query evidence不變。
[正式新owner baseline](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v79/runtime_baseline.json)
06:42:38.761668 UTC到[正式runtime acceptance](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v79/runtime_after.json)
06:46:03.140655 UTC，**13/13**通過，同owner／PID3094457／NRestarts0、source code未於啟動後修改。
WAL窗口204.378987秒，鉅亨116／Global Voices16個new stored URL heads，分別0.567573／0.078286新head/s。
HTTP窗口199.959933秒，鉅亨178個terminal requests／0.890178req/s，6次terminal transport errors、6次terminal502，均保留；不能拿兩種clock互換分母。
systemd服務CPU delta29.908855秒／平均0.146340 core，MemoryCurrent269.046875MiB、Peak275.683594MiB；不是process RSS／全機／WSL／GPU數字，也不是配對CPU下降結論。

[最新128 batches](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v79/recent_body_acceptance.json)
06:45:29.226296 UTC WAL，4,645 body versions／4,645 current heads、843,186,925 private raw bytes，raw／Parquet／receipt／head均接受，非全湖稽核。
[同一九篇native原件控制集合](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v79/same_nine_native_acceptance.json)
9/9真正文／author／發布clock重播通過，沒有回填historical capture FP。
第一次readback誤選非既有control的九個URL，missing結果留存；從先前接受artifact讀回原URL後才按同集合重播，未為誤選網址送HTTP或宣稱原文不存在。

[最終20啟用source transport核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v79/transport_after.json)
06:45:30.222570 UTC接受各host／terminal counter與private事件；較早cutoff有mobile5次awaiting_headers、3個gateway decisions，failed origin pool reset5、whole Session reset0。
當時actual host circuit開啟1次，robots-origin unavailable headers／暫停1次，診斷／checkpoint寫入失敗0；較晚正式窗口6次transport／6次502也保留，不說來源已無錯。
[最新934來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v79/inventory_after.json)
06:45:31.197292 UTC，6,202,187 known URLs／3,626,235 stored heads／2,130,443仍待啟用或授權。
鉅亨3,595,004 known／3,512,556 stored heads，當時75,701 active missing body work（含inflight）；298 quote-only與4,613 empty template等並未當成已取得正文。

[四viewport正式頁面](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v79/dashboard_acceptance/acceptance.json)
1440／768／390／320皆934來源／39頁；counts、進度分母、獨立origin暫停clock、source scopes／全來源ETA未知與search／sort／reset接受。
已檢視desktop summary與small-mobile鉅亨卡；沒有改UI或重啟read-only gateway，CPU-2D Chromium／localhost證據不等於WAN、真正使用者browser冷啟動或GPU／WebGL。
來源headers／502、未准許來源、民視attribution、原文缺失或非文字representation仍逐列保持；此輪工程局部修正並未完成934來源／全歷史。

[當前owner與完整交接證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v79/handoff_latest.json)
**12/12**接受，涵蓋本輪9個手動檔SHA與文件連結、實際migration／current checkpoint、同owner正式runtime、三窗完整配對／body／raw／receipt recovery、20啟用source errors、四viewport及兩個protected unknown queries。
首次14:49:44臺北核對時，新owner已新增301個鉅亨正文URL heads、562 requests；7次transport與9個502仍可見。
本輪`compile`／shell syntax／`git diff --check`及untracked手動檔whitespace檢查通過；沒有操作其他下載器或交易、修改金鑰、clear deadline／refusal、丟棄慢phase或改寫舊source receipt。
goal與原工程任務保持active；下一輪沿actual header／remaining native-body failure證據與candidate/status熱路徑，繼續完整934-source inventory／端點／權限／原件接線，不把這次局部progress改成全目標完成。

## HTML選擇器候選縮小與當前失敗原件重播

2026-10-11，v80。前輪routing／cursor／原文／收據／網頁修正已實際接受，是progress而非status重述；繼續原`news-adaptive-capacity-20261009`及934-source goal，不縮小成功條件。
工程偏好技能促使本輪保留相同來源、全正文與較慢測量，沿共享selector／publisher caller修復；沒有另建下載器或鬆綁rights／robots／refusal。

### 根因與修正邊界

[原934來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v80/inventory_before.json)
06:55:54 UTC為62 configured／20 enabled／872未註冊，192 metadata-only；6,202,187 known URLs／3,627,025 stored heads／2,130,443待啟用或授權。
[原20啟用provider transport](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v80/transport_before.json)
核對接受；鉅亨1,267 terminal requests有15份awaiting-headers private failures（mobile14／news1），各約25秒，origin pool reset15、whole Session reset0。
另有9個gateway與3個robots-origin502決策，診斷／checkpoint寫入失敗0；這是來源等待，不是HTML計算。API的192份304是條件追新回應，不是192篇正文或歷史抓取失敗。

[最初36原件profile](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v80/parser_before.json)
較舊all-CPU oracle僅用於定位：candidate解析profile約6.194秒，212次`_select_nodes`約2.237秒，124,639個SoupSieve matcher calls。
完整CSS多段selector先從整頁訪問大量不可能的tag；既有simple-selector hint沒有涵蓋chain，鉅亨canonical／og／article-container三個caller還直接走CSS。
source parsing約2.710秒與status／routing／dispatch其他熱路徑仍存在，不宣稱全部CPU根因已解。

`HTML_SELECTOR_CANDIDATE_CONTRACT_VERSION=2`是**載入效能觀測版本**，不是source DATA／parser acceptance／routing／bulk identity升版。
bounded ASCII compound／descendant／child／adjacent／general-sibling selector，先由最後一段tag／id／class／attribute找到候選，再用原compiled SoupSieve整段`match`核對。
selector最大4,096字、static hint cache最多1,024 entries；不cache DOM／Tag，不改順序／object identity／limit，不因attribute值含`.class`、`#id`、逗號或`>`誤判。
groups／pseudo／`:scope`／escapes／namespaces及XML保持原CSS matcher；先compile避免invalid selector被空候選藏掉，XML大小寫語義不借用HTML shortcut。
依據[SoupSieve官方API](https://facelessuser.github.io/soupsieve/api/#soupsievematch)保留完整匹配與HTML／XML差異；不是自己重寫一套CSS語義。
三個鉅亨caller沿同一helper，current／visible／schema／ID／free／authors／clocks／figures／tables與paywall拒絕不變。所有source capture FP、whole policy與routing scope均需正式readback相同。

### 同來源完整鏈路與反例

[精確前版最終配對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v80/parser_exact_pair_final.json)
固定鉅亨／Global Voices／Fed各12份、共36份原body，八個ABBA phases；reference為精確v79 selector-v1，鉅亨三個caller回到當時direct CSS，不混入更舊多項CPU修正。
兩種模式都經canonical worker、36-record bounded batch、private raw／Parquet／fsync、immutable receipt與head verification；各36版本／36 heads／36 responses，0 source rejection，正文／title／author／原clock／stable metadata皆一致。
new publisher HTTP為0、live queue writes為0；沒有縮小正文、跳過耐久驗證或注入proxy作者／日期。原件與code/config SHA綁定。
[中間配對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v80/parser_exact_pair.json)
亦接受，但尚未回復三個native caller的精確前版狀態，保留它，不用其中倍率替代最終測量；最初profile的舊all-CPU reference也不混入v80效果。

| 相同36原件工作量 | reference中位 | candidate中位 | 邊界 |
| --- | --- | --- | --- |
| extraction wall | 1.726046 s | 1.469966 s | 降低14.84%；各首個profiled sample不納入extract中位 |
| extraction CPU | 1.726575 s | 1.470531 s | 不等於host總CPU下降 |
| 完整解析／落盤／收據／驗證wall | 3.160944 s | 2.825002 s | 降低10.63%，四次完整pipeline皆納入 |
| 完整pipeline CPU | 2.125892 s | 1.878606 s | 降低11.63%，非network／TLS／boot測量 |

保留candidate較慢extract phase：1.966007 versus 1.726046秒；亦保留pipeline CPU反例：1.853166 versus 1.782577秒。
中位改善不代表每篇／每phase都快，source端25秒等待亦未由此消失；官方最高頻率、整個daemon啟動與全主機節能仍未證明。
[最終focused](../artifacts/operations/agent-workflow/runs/v80-final-selector-oracle-native-20261011T070600-c8112758/run.log)
**1,170 passed／8.65秒**；涵蓋兩種HTML parser、三種root context、30 selectors、三種limits、XML／namespace大小寫、invalid CSS、attribute值誤hint、no-DOM-cache、caller rollback與2000無關nodes的candidate matcher bound。
[完整news含bulk](../artifacts/operations/agent-workflow/runs/v80-full-news-and-bulk-regressio-20261011T070602-404920d0/run.log)
**3,706 passed／69.92秒**；正文／表格／figures／作者日期及negative rights／visibility皆保留。初版1,158 focused亦保留，沒有新full回歸失敗。

### 原失敗重播、來源問題與正式服務

僅drain news owner／timer，EXIT trap恢復；未重啟gateway、其他資料服務、交易、GPU或冷庫，未改金鑰。
[drained checkpoint](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v80/deployment_before.json)
保留routing原游標、全source capture FP、whole policy、HTTP負面checkpoint SHA及兩個protected unknown queries。
[新owner載入7/7](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v80/deployment_acceptance.json)
15:12:01臺北附近接受owner`17bc4b5c901a4c01a099f6089ac851b8`／PID3147112，loaded selector2；routing2原scope續走，source／whole policy及未知query／due原值皆未被清除。

[最多四個已到期missing-body failure的實際查詢](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v80/current_due_native_failure_probe.json)
經共用exclusive owner／pacing／robots／cooldown，沒有負載試探、猜ID或清due。四份皆200且public-scope raw合法私存，但**仍body_not_found，不當新正文**。
[私人原件／receipt精確重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v80/private_failure_replay.json)
逐份核對Parquet與raw SHA、同網址、權限與private flags；新增HTTP0、queue write0。1600512／1587433／1903822可見只有11／20／4字，不能虛構完整新聞。
4954707?exp=a可見／current為1242字，identity／free／author／clock一致；JSON-LD只多出精確三行投資風險聲明。
真實同圖figure只有一個`p.warning`，既有matcher要求兩段（先空p再warning），因此comparison view沒有成立，stage=`schema_mismatch_without_verified_figure_view`。
這是**已定位但本輪尚未修改的source body template缺口**；下一輪依同原件補bounded模板／metadata證據，不能把schema不一致任意略過、刪warning或說四篇補回。

[正式baseline](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v80/runtime_baseline.json)
07:12:02.531757 UTC到[正式runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v80/runtime_after.json)
07:15:34.934316 UTC，**13/13**，同owner／PID／NRestarts0／source code未於owner啟動後修改。
WAL窗口212.402559秒：鉅亨新增126正文URL heads、0.593213新head/s，Global Voices19、0.089453新head/s。
另199.149679秒HTTP counter clock：鉅亨224 terminal requests／226 starts、1.134825 starts/s，6 transport及1 terminal502仍保留；不拿兩種clock互換分母。
systemd新聞service CPU delta21.925946秒／平均0.103228 core、MemoryCurrent324.1875MiB／Peak327.421875MiB；不是process RSS、WSL／host總RAM／GPU或同cohort resource提速證明。

[128最近batches](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v80/recent_body_acceptance.json)
07:13:55 UTC核對4272版本／4272 current heads、778,013,261 private raw bytes與Parquet／receipt／head接受，非全lake稽核。
[同一九篇control最終驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v80/same_nine_native_acceptance_final.json)
9/9正文／作者／clock current replay通過；source原capture FP不回寫成新FP。
[第一次control指令結果](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v80/same_nine_native_acceptance.json)
漏寫4992968的`?exp=a`，read-only verifier正確拒絕not_registered；保留失敗／run log，從v79實際accepted artifact讀回原網址後才重驗，同task已追加superseded處置，沒有向誤選URL送HTTP。

[20 enabled source transport](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v80/transport_after.json)
07:13:55 UTC核對接受，當時鉅亨3 awaiting-header failures／3 origin resets、whole Session0，3 gateway／1 robots-origin決策；稍晚formal窗6 transport仍保留。
[新934來源inventory](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v80/inventory_after.json)
07:13:56 UTC為6,202,218 known URLs／3,627,933 stored heads／2,130,464待啟用或授權；鉅亨3,595,005 known／3,514,106 stored、74,130 missing-body work、37 stored-body rechecks。
鉅亨body_not_found、AMP502與header timeouts、CMoney／PeoPo403、民視attribution、cmnews付費／圖片及Fed PDF等仍逐來源列出；已觀測恢復不當全provider健康，未准許來源也不改成抓完。
[四寬度readonly網頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v80/dashboard_acceptance/acceptance.json)
1440／768／390／320皆934來源／39頁，counts／進度分母／error／origin暫停clock／ETA與source scopes核對接受。
已檢視desktop summary與320-width鉅亨卡；CPU-2D Chromium／localhost／不呼叫publisher，不等於WAN或GPU／browser冷啟動驗證。本輪沒有UI改版。

[當前owner交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v80/handoff_20261011T071941210894Z.json)
15:19:41臺北時間**13/13**，11個手動檔SHA與文件links、正式資料／同來源完整pipeline、loaded selector2／routing2及保留兩個unknown queries核對。
當時新owner鉅亨已新增453個stored URL heads、784 requests，12 transport與500七次／502八次仍明列；條件ETA只涵蓋當前73,904個已知missing URLs，不是全站／授權／未發現歷史的承諾。
本輪`compile`／shell syntax／`git diff --check`及手動untracked檔whitespace通過，沒有修改其他下載器、交易、金鑰或source限速保護。
原goal／工程task保持active；下一輪先根治上述實際一段warning模板拒絕，再按934-source backlog追蹤各provider的端點／作者／原clock／授權／歷史與共享status成本。這是局部性能修正與新失敗證據，不是全歷史／全來源已完成。

## 原生單段風險聲明模板的完整正文恢復

2026-10-11，v81；接續v80的真正CPU優化、正式資料進度與四份新failure evidence，是progress而非重述service active。原934-source goal與task不改；工程偏好技能促使保留完整原值／慢測量／拒絕／due與source分工，不為提速改寫數據。

### 源頭模板、版本與驗證

[prechange快照與保留v80 code](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v81/prechange_snapshot.json)
核對仍載入v17 source FP與原owner；保存舊policy／publisher code的exact SHA，前輪36原件ABBA performance仍綁定當時code，不把本輪semantic change冒充配對提速。
4954707?exp=a的原始回應有完整1242字narrative，visible／current／JSON-LD／ID／free／author／clock都能核對；只有同圖投資風險聲明在schema內，而一般narrative extraction會排除figure。
舊matcher只認「空白p、p.warning」兩段；這份真實source只有一段p.warning，所以整個figure comparison view返回None，落入body_not_found重試。
新matcher只增加**已觀察的沒有空白p**情況；1或2個direct p、末段精確`class=warning`、兩個BR與完全相同三行聲明，原same-image／current-content membership／order／visibility／whole schema proof全保留。
不接受真正額外段落、未知財務數字、不同圖片、隱藏warning／BR、任意span或額外caption。正文保留原值；聲明保存於`verified_article_notices_json`，不是偽caption、補造narrative或來源授權。

`mobile_canonical_html_contract_version`17→18；鉅亨capture FP由`c3ddcbe7…`到`6d6f75ae…`，whole policy因此更新。
其他61個source capture與全部authorization FP不變；bulk FP／registered endpoints／API clock／routing2 FP不變，selector performance2延續。這輪沒有調快pacing、parallelism或清掉negative checkpoint。
[相同四份failure source replay](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v81/four_source_replay.json)
四份原Parquet／raw SHA／receipt均確認不變；僅目標1242字完整恢復並保留1個同圖notice，另11／20／4字仍body_not_found。
這個離線step新增HTTP0、queue write0、committed heads0；不把原件可解析當正式下載，沒有虛構三份極短頁的內容。

[精確focused](../artifacts/operations/agent-workflow/runs/v81-exact-single-warning-and-sou-20261011T072351-8ff6e297/run.log)
**532 passed／6.25秒**，含old/new兩個figure模板、ordinary／completed-stream、mobile／desktop exp=a、unknown-content／rights／clock／visibility以及routing／robots恢復。
[全news含bulk](../artifacts/operations/agent-workflow/runs/v81-full-news-and-bulk-regressio-20261011T072352-57e12483/run.log)
**3,728 passed／68.13秒**；首次focused命令誤用不存在host／robots測試檔名、exit4且no tests，原run保留，rg定位後更正argv再驗證，已追加同task superseded，沒有publisher請求或來源重排。

### 自動恢復、耐久正文與獨立進度

[drained rollout snapshot](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v81/deployment_before.json)
只stop／restore news owner與timer，EXIT trap恢復；其他服務／gateway／交易／GPU／冷庫／金鑰未操作。
[指定canonical fetch](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v81/selected_capture.json)
**0 attempted／0 captured**：舊owner於15:25:06又觀察到parser失敗，既有due到15:30:06，claim_url正確沒有發HTTP。
沒有以這份exit0宣稱下載成功，也沒有reset due／attempt／lease；新normal owner繼續已知正文及oldest-due retry公平排程。
[loaded source rollout8/8](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v81/deployment_acceptance.json)
owner`2bb815cc14284ce4abf601ad93fb112a`／PID3187817接受，其他capture與protected兩個unknown queries不變，routing原`after=done`保留、不為HTML修正重掃literal proofs。
本輪HTML rollout的bulk parser重查marker為0 jobs，沒有重排來源HTTP拒絕；source依自身負面期限／robots／pacing繼續運行。

[實際恢復一篇＋九篇control](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v81/ten_native_acceptance.json)
**10/10**source／current parser／body／author／clock／raw／receipt核對接受。目標於15:37:13.938028由normal owner真正取得並入庫，第三次attempt，原due不變、error清除由成功accept產生。
正文1242字、SHA`ce1b6228…`與15:08原診斷response的完整body完全一致；新source response SHA不同仍另保留，沒有拿相同body誤稱相同HTTP回應。
來源署名為`Anne陳巧諭`，發布原值`2022-09-15T00:30:03.000Z`；捕捉時間是2026-10-11，兩者不同，不回填成historical capture。
一份same-image investment disclaimer獨立保存、不是caption；new head用capture18，舊九個controls保留各自capture FP，沒有重寫舊收據以假稱最新。

[正式baseline](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v81/runtime_baseline.json)
07:28:39.369289 UTC到[正式runtime13/13](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v81/runtime_after.json)
07:34:53.853364 UTC，同owner／PID／NRestarts0／source files未於owner啟動後修改。
WAL窗374.484075秒，新增鉅亨253個正文URL heads／0.675596新head/s、Global Voices32／0.085451新head/s；是網址head，不把同ID aliases混成獨立篇數。
另HTTP時計370.135603秒：鉅亨415 terminal requests、1.123912 request-start/s，仍有2個transport及7個terminal502，失敗／等待含於窗口。
CPU delta45.024531秒／平均0.120231 core，MemoryCurrent399.636719MiB／Peak430.253906MiB為systemd news cgroup；不是process RSS／全host／WSL／GPU或same-cohort提速證明。
[128最新durable batches](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v81/recent_body_acceptance.json)
07:34:38 UTC核對4342版本／4342 current heads及786,669,397 raw bytes；scope不是全lake，目標晚到的raw／receipt另外由selected verifier核對。

### 各provider錯誤、網站與剩餘邊界

[final20 enabled transport audit](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v81/transport_final.json)
同owner核對接受，當時鉅亨802 terminals／7 awaiting-header transport failures／7 origin resets，whole Session0；6 gateway／2 robots-origin決策與原negative checkpoint保留，所有診斷／checkpoint寫入error0。
gateway／readtimeout仍是可觀測source failure，不宣稱已消失；正常origin與其他provider繼續，403／rights／nontext／404亦不冒充成功。
[final934-source inventory](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v81/inventory_final.json)
07:39:43 UTC為62 configured／20 enabled／872 unregistered、192 metadata-only；6,202,231 known URLs／3,629,277 stored heads／2,130,468仍待啟用或授權。
鉅亨3,595,005 known／3,515,317 stored、72,918 active missing body work及83 stored-body rechecks；後續追新／新發現網址繼續改變分母，不以此保證全站最早歷史／所有文章已取得。
[恢復後四寬度readonly頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v81/dashboard_final/acceptance.json)
1440／768／390／320皆934來源／39頁，counts／進度分母／origin暫停／錯誤與條件ETA核對，只有metadata傳到瀏覽器。本輪沒有UI改版／gateway restart或publisher browser calls。
[當前owner完整交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v81/handoff_20261011T074342858854Z.json)
15:43:42臺北時間**12/12**，涵蓋10個手動檔SHA／報告links、正式runtime、loaded source18／routing2不重掃、同原件恢復／實際一篇與九個control、raw／receipt、20-source錯誤與四寬度934-source頁。
當時新owner已新增597個鉅亨stored URL heads、1042 requests；12 transport與502十次仍可見，remaining72,881的條件ETA不是未知全站／權限／未發現歷史的承諾。
已檢視恢復後320-width鉅亨卡；compile／shell syntax／git diff --check／手動untracked whitespace均接受，未啟動GPU、訓練或重啟其他來源服務。
全目標與task仍active，剩餘native schema／table failures、transport／AMP gateway、其他provider scope／permission以及未知歷史持續按清冊處理；這不是全934來源／全歷史完成或官方最高速證明。

## 實際連線重用與標頭前故障的源頭觀測

2026-10-11，v82。接續原934-source goal與task；工程偏好技能促使保留失敗、慢phase、來源FP與既有due，不為提速刪掉觀測。這輪先補足根因證據，沒有宣稱所有publisher問題已根治。

### 共用傳輸、真實連線與不改變的邊界

[原生connection observer](../downloader/news_fulltext_connection_observation.py)在原urllib3 `_get_conn`及HTTP／HTTPS `connect`之後／前後記錄socket、同connection object的terminal count、idle與connect attempts／completed／wall；不另發HTTP、替換原dropped-socket判斷或放寬TLS。
每個worker仍只有一個serial Session、最多十個origin、每池一條連線、adapter `max_retries=0`；正常回應只增加有界bucket counters，不另寫每-request診斷檔。
原生keep-alive需正確完成／關閉response，pool checkout與connect仍委派library實作；參照[Requests官方keep-alive](https://requests.readthedocs.io/en/latest/user/advanced/#keep-alive)與[urllib3 connection pool](https://urllib3.readthedocs.io/en/stable/reference/urllib3.connectionpool.html)。實裝依本機Requests2.34.2／urllib3 2.8.0與真TCP／TLS測試核對。
`connection_reuse_observation_contract_version=1`與transport auditor8；unsupported／injected adapter明列unobserved，不能把缺值說成connect耗時0。connect wall包含DNS／TCP／TLS整段，沒有虛構三個分段。
counter屬**connection object**；同object可重新連線，不是「同一TCP已送67次」。host／provider bucket總和須對應terminal requests；failure event與metrics各自時間戳保留，跨snapshot事件不硬湊一對一。
[before5/5](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v82/before.json)與[部署10/10](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v82/deployed.json)確認62 capture／authorization、whole policy、bulk、routing與兩筆protected unknown queries不變。沒有重掃歷史／reset attempts或due。
只drain並恢復news owner／timer；新owner`e6eac5d93e6b4a159e9de172def652f6`、PID3277248、16:11:43啟動，NRestarts0；其他collectors／gateway／交易／GPU／冷庫／金鑰未操作。

### 同來源完整管線、TLS及錯誤測試

[最終focused](../artifacts/operations/agent-workflow/runs/v82-final-hook-and-clock-focused-20261011T080914-e01b3955/run.log) **176 passed／6.30秒**；含45個新測試、native pool map不改全域、warm／fresh／idle／失敗恢復、型別／finite clock、exact bucket accounting，以及受信self-signed TLS與hostname verification。
[全news含bulk](../artifacts/operations/agent-workflow/runs/v82-final-all-news-and-bulk-regr-20261011T080915-07802c15/run.log) **3,773 passed／78.51秒**；不是全repo training或全934來源驗收。
[同owned完整管線ABBA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v82/connection_observation_pair_final.json)比較**相同origin isolation、hooks關／開**；12 phases、每phase39 bodies、共468個版本，canonical fetch／parser／raw／Parquet fsync／receipt／head verification／recovery一致。
reference中位wall／CPU0.151476／0.144377秒；observed0.143449／0.138878秒，含reference0.615秒慢phase。先前eight-phase observed0.760秒亦保留在原benchmark，沒有以此宣稱observer零成本或真站倍速。
首次pool map import、TLS fixture錯用forbidden ephemeral source port、snapshot猜不存在field及Tuple／JSON list比較共四個工程失敗，都保留原exit／log並以後續證據標記superseded；沒有重送未知來源查詢或放寬source URL grammar。

### 真實故障、自動補回與當前吞吐

[最先三筆原生故障](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v82/transport_after.json)：鉅亨兩筆25.10／25.40秒都沒有回應標頭／body bytes。
一筆warm socket只idle0.727秒；另一筆socket已關、同object重新connect完成0.305秒後仍等待標頭。這排除了「所有逾時都因長idle」；不能進一步把未分段的等待歸咎DNS／TLS或publisher計算。
Global Voices另有idle59.243秒後0.209秒RemoteDisconnected；**單例不足以證明官方60秒TTL**。原失敗origin pool已隔離，whole Session沒有reset，正常來源繼續工作。
[三筆正式正文恢復](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v82/three_failure_recoveries.json)均由normal owner捕捉：鉅亨651／680字於16:17:20／16:17:14入庫，距失敗30.79／30.66秒；Global Voices204字於16:14:08入庫、距失敗40.60秒。
**3/3 current source、完整正文SHA、作者、原發布／修改clock、capture FP及raw／receipt接受**；共兩個batch的107 immutable版本全部核對，沒有用舊body當新的HTTP成功。
[正式runtime13/13](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v82/runtime_final.json)在16:12:26→16:26:18 WAL窗832.192329秒新增鉅亨1,301／Global Voices82個stored URL heads；1.563341／0.098535新head/s是**網址head**，不是去alias後獨立篇數。
另HTTP counter窗838.523842秒：鉅亨平均2.132319 requests-start/s，仍含5次transport與14次502；adaptive pacing會升降，不把窗平均當固定官方限制。
CPU delta136.534135秒／平均0.164066 core；systemd news cgroup MemoryCurrent653.558594MiB／Peak724.824219MiB，不是process RSS／全WSL／GPU，也不是同cohort性能對照。
[128最新durable batches](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v82/recent_batches_final.json)於16:25:19核對**3,972 immutable body versions／3,972 current heads**與716,928,674 raw bytes；這是bounded recent-write cohort，不是全lake。

### 其他provider、清冊與尚未解決項目

[20 enabled最後故障audit](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v82/transport_final.json)於16:25:13接受：鉅亨1,652 terminals／5次標頭前transport／5 origin resets／whole Session0；Global Voices77 terminals／1次disconnect／1 origin reset。
所有diagnostic／pool reset／gateway與robots checkpoint write errors0；CMoney、PeoPo各有一次新連線403，保留source_access_denied及官方等待，不偽裝登入／換identity或把拒絕變正文。
後來兩筆鉅亨新失敗亦已正常補回；另6624734已有01:34舊body、但16:23的追新逾時仍是retry，**有舊正文不等於本次追新完成**。這些後到狀態不冒充上面3/3的獨立原件驗證。
[當輪934-source清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v82/inventory.json)16:13:33固定WAL：62 configured／20 enabled／872 unregistered、192 metadata-only；6,202,235 known URLs／3,630,613 stored heads／2,130,468待啟用或授權。
完整inventory本身wall32.105／CPU23.424秒，故本輪不高頻重掃；即時進度使用原indexed counters／bounded snapshot。鉅亨設定歷史搜索起點其實1999-01-01，已知metadata最早2009-06-17；兩者都不能證明所有更早文章已取得。
16:26 runtime鉅亨尚有70,543個missing body work／139個stored-body rechecks；未知全文／權限／新發現網址不由有限分母ETA保證完成。
[四寬度readonly頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v82/dashboard_final/acceptance.json)在16:30～16:31接受；1440／768／390／320皆934-source／39頁、summary counts／分母／native controls一致，JS與publisher browser requests0。這輪沒有UI改版／gateway restart；此驗收沒有另加require-provider，provider detail cards不冒充重新逐項驗證。
[後續20來源監測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v82/transport_handoff_current.json)16:37:25仍同owner、accepted、write errors0；鉅亨2,481 terminals／19 transport（含2個newly-connected）、Global Voices131 terminals／7 transport（含2個newly-connected）。不把16:26的較低故障數說成後續錯誤已消失。
工程[完整handoff](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v82/handoff.json)核對當前來源／時鐘／hash與原件三筆恢復，不把source拒絕、未發現歷史或872個未註冊來源說成已完成。
下一階段依新的native failure witnesses優先核對真實正文schema／figure／table拒絕與已存正文追新排程，保留原故障deadline、作者、發布時鐘和所有934-source backlog；沒有盲目縮短read timeout、臆造idle TTL或跳過驗證。

## 有正文的失敗追新不再被普通追新餓死

2026-10-11，v83。沿原 `news-adaptive-capacity-20261009` 任務；只修改共用新聞 work selection／capacity measurement identity，沒有修改來源內容、金鑰、授權、TLS、其他服務或訓練。

### 根因：服務正常不代表每種工作都得到服務

[固定的修前證據與三份原程式SHA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v83/before.json)於17:00觀測：6624734有01:34舊正文，16:23追新ReadTimeout後應於16:23:56再試，但已**逾期2,172.852秒**、仍retry且沒有lease；host／origin都沒有暫停。
既有 missing-body retry 已有最早到期公平服務；stored recheck卻只按metadata日期取最新，普通追新的 `due=0`／相同日期可以一直先被選到。增加threads／縮短退避不會消除此排序餓死；保留舊body也不是追新成功。

### 共用修正與成本邊界

[NewsQueue](../downloader/news_fulltext_queue.py)加獨立 `stored_body_retry_fairness_contract_version=1`，generic dispatch仍5；每provider的八次stored服務累積後，**只替換原已分配给stored工作的那一輪**為最早到期失敗。無可用失敗時credit保持、普通工作繼續，不製造空轉。
due、state、lease、host／origin exclusions、finite cohort和missing／undated／missing retry份額仍走同一claim；regular fallback只剩stored工作時也保持公平。只有特別的oldest-stored lane重置它自己的credit，普通失敗不會清掉。
`dispatch_stored_retry`只索引body存在、error存在且retry／retry_cooldown的active failures，按provider／due／metadata排序，不熱掃或重排全部完整正文。
首次安裝確實掃既有table：本機**wall20.584／CPU10.005秒**，初始9個active failures、index一頁4,096 bytes；完整收據恢復另**wall70.275／CPU31.619秒**。兩項分開，不把首次啟動或現有完整receipt成本說成零；後續IF NOT EXISTS機制不代表已實測另一次正式reboot。
[collector三個claim入口](../downloader/news_fulltext.py)統一傳同一burst；[capacity fingerprint](../downloader/news_fulltext_capacity.py)記錄新的fairness／knob，舊吞吐證據不冒充相同工作配置的新測量；原[calibration證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v83/rate_calibration_before.json)保留，source capture／rights／bulk／routing FP不變。
在一個最早到期失敗持續eligible、有持續missing及普通stored工作的fixture，預設8／8給**81個article dispatch內**的服務界限；這是同owner的選取次數，不是81秒、全排隊失敗的統一deadline或publisher quota。

### 先驗證，再讓正常owner恢復

[195 focused](../artifacts/operations/agent-workflow/runs/v83-corrected-exact-fairness-lea-20261011T091230-e12a50a8/run.json)與[3,800全news含bulk](../artifacts/operations/agent-workflow/runs/v83-all-news-bulk-and-fairness-r-20261011T091257-4e3d35f1/run.json)通過；持續新工作、兩類份額、普通失敗不清credit、future／lease／host／origin、兩provider獨立與invalid config皆涵蓋。
先前選錯不存在的test path，以及fixture把另一失敗due=0設得更早／用了不合法0.001秒capacity，原failed runs保留；修fixture而非放寬正式eligibility。
[完整管線同源ABBA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v83/owned_selector_abba.json)沿[既有owned transport及canonical capture／receipt工具](../scripts/benchmark_news_stored_retry_fairness.py)，原snapshot queue對目前queue、八phase。
每phase84次owned HTTP、81次article dispatch、72個missing／9個stored slots、83份immutable body versions／74個distinct URL heads；總**664版本**全部raw／body／作者／clock／fsync／receipt核對。
舊版始終挑普通fresh、原失敗維持；新版在既有第九個stored slot實際fetch／accept原失敗。continuous ordinary recheck是明列stress fixture，沒有reset正式queue或呼叫外站。
中位wall0.790430→0.621544秒、CPU0.798630→0.664814秒；原wall samples 1.161／0.655／0.670／0.911、新0.620／0.623／0.836／0.572都保留。這是owned bounded workflow，不把noise、warmup或不同publisher cohort當官方最高速／實際網路倍速。

[正式載入9/9](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v83/deployed.json)：17:16:20只平順重啟新聞owner／timer；新owner `f33916e1f530496b93ea519b7838336f`、PID3399566、NRestarts0，62 capture／authorization／policy／bulk／routing與兩筆未知query完全保留。
17:17過早audit正逢初始化，只看到了前owner最後stopped快照，未接受；原[未ready部署核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v83/deployed_initialization_not_ready.json)與[未ready傳輸核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v83/transport_initialization_not_ready.json)保存。等待新owner正式heartbeat後才重驗，不再重啟或假稱ready。
[八篇current source replay](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v83/actual_target_replay.json)全部接受：6626483、6626481、6625132、6624899、6625560、6625884、6624876、6624734均由正常deadline／claim補回；沒有手動重排、clear due或強迫來源回報成功。
6624734於**17:24:40.489**重新capture108字，原body SHA相同但新raw／Parquet／receipt與HTTP clock實際前進，attempt1→2、lease解除、error清掉；不是304借用舊capture或偽造歷史時間。
[當輪新連線／短idle逾時兩篇](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v83/current_socket_failure_recoveries.json)亦由正常missing retry補回487／579字，current source／作者／clock／raw均接受；仍不推論所有網路逾時都由idle socket造成。

### 現場吞吐、其他來源與下一步

[runtime13/13](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v83/runtime_final.json)：17:18:52→17:26:26同owner WAL窗453.441415秒，新增鉅亨**720**／Global Voices**45**個正文URL heads；1.587857／0.099241新head/s，不是去alias的獨立篇數。
另HTTP counter窗444.375675秒，鉅亨2.252599request-start/s，仍含5次transport與5次502；adaptive會升降，17:29:33實際client間隔0.5秒／parallelism4，不把窗平均混成固定頻率或已證明的官方上限。
news cgroup CPU delta103.450963秒、窗口平均0.228146 core；MemoryCurrent943.269531MiB／Peak1,025.675781MiB，非process RSS／全WSL／GPU，不聲稱整機記憶體已優化。
[128最近committed batches](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v83/recent_batches_final.json)核對**3,808 immutable versions／3,808 current heads**與697,749,064 raw bytes；不是全lake、不是128份全由本owner製造，也不是全部來源歷史完整。
[20啟用來源傳輸核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v83/transport_final.json)包括當前failure／pool reset／gateway／robots scopes；沒有HTTP或terminal的provider不是這個窗口已證明健康。健康origin繼續，source逾時／502仍依證據退避，不跳過robots、Retry-After、403、TLS或quota。
[934-source固定WAL清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v83/inventory.json)17:22:39：62 configured／20 enabled／872 unregistered、192 metadata-only；6,202,279 known URLs／3,634,576 stored heads／2,130,487待啟用或授權。
本次全清冊wall53.934／CPU39.464秒，單次核對、不拿高频全表掃描做即時進度。當窗鉅亨68,172 missing body work中68,112只有已知canonical-body候選、不是原網址全文已驗收；另207 stored rechecks，分母、alias等價、歷史發現與來源權限仍各自獨立。
原始來源仍有CMoney／PeoPo拒絕、民視署名／identity、圖像／影音、付費、失效網址與未註冊來源；清冊列出既有failure，不冒充全是本owner新HTTP。這些不是本輪fairness可消除的來源問題，也不靠改state或無限重送冒充抓齊。
[四寬度實際provider cards／全清冊／原網址進度／恢復](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v83/dashboard_final/acceptance.json)1440／768／390／320皆934-source／39頁，明確加require-provider核對鉅亨與Global Voices計數、分母、條件ETA、origin pause／probe time，source reviews及browser實際timed retry亦核對；無JS error／外站browser request／failed resource。
下一阶段優先針對當前鉅亨 `schema_mismatch_without_verified_figure_view` 原件拒絕與其他provider新錯誤逐份重播，擴充確實缺失的模板／端點；來源negative、授權、原時鐘及全部934 backlog保持，目標仍active、不宣稱全來源完成。

## 失敗原件按固定類別有界保留與不重置額度

2026-10-11 v84，Asia/Taipei。前輪v83確實補回8篇逾期追新，本輪先確認owner仍持續下載；不把新錯誤只算成其他provider或來源不穩定。

### 為什麼有計數卻無法查晚間錯誤

[原budget及不可變收據快照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v84/before.json)核對：原[diagnostics v4](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v84/reference/news_fulltext_diagnostics.py)每provider／UTC日僅兩次，鉅亨於UTC00:00:00與00:00:12已耗盡。
之後`schema_mismatch_without_verified_figure_view`、`schema_comparison_mismatch`、`missing_body_representation`與`table_grid_mismatch`只有static counter，沒有新回應原件；不是尚未發送HTTP、刪除當天額度或parser自動成功。
兩份旧原件仍有私人Parquet／SHA／收到時間：4663341已記錄schema-figure類，958337的舊stage未知，不能逆猜分類。舊`failure_diagnostic_attempts:v1:cnyes:20261011=2`不退款、不改key。

### 固定類別而不是每網址無限取樣

[共用私人失敗sink](../downloader/news_fulltext_diagnostics.py)升payload5／budget2；general allowance仍2/day。
五個既有native stage各有獨立persisted兩次：missing-body、visible/current、schema/figure、schema/comparison、table-grid。只有已有`FailureResponse`且來源／ID／自由存取／clock／query scope獨立核對的已收到回應可留raw；未驗證權限仍僅metadata，headers／query照舊清理。
第一次用stage桶只讀最多兩份≤64KiB當日legacy receipt metadata並扣已知stage；不重播猜舊stage、不接受正文、不改原件。未知或缺少的舊witness保留general charge；receipt malformed／超額／錯provider／naive clock會一次關閉新桶並留下seed錯誤，不反覆讀壞檔。
reserve先於HTML parse、hash、Arrow、fsync；storage outage消耗reservation，重啟／parser fingerprint升版都不重置。已初始化的桶走indexed setting讀取，不每篇讀舊receipt。
最大Cnyes allowance **general2＋五類各2＝12 attempts/UTC day**，每回應raw≤8MiB，理論raw上限96MiB/day；不是保證保留12份、不是每次HTTP額外抓取，也不是磁碟總保留政策。此次舊known類已先扣1，未知那份仍占原general，不重複給額度。
[queue status marker](../downloader/news_fulltext_queue.py)回報loaded budget2；[capacity](../downloader/news_fulltext_capacity.py)只使鉅亨measurement identity更新，其他provider的measurement FP與全部62 capture／authorization／bulk／policy／routing身分保留，不把舊吞吐估計冒充新measurement。

### 測試與完整鏈路

[150 focused](../artifacts/operations/agent-workflow/runs/v84-valid-client-cadence-and-ide-20261011T095525-0563ee9b/run.json)／[3,814 news含bulk](../artifacts/operations/agent-workflow/runs/v84-current-all-news-source-and--20261011T095526-e935e922/run.json)通過；[新stage額度測試](../test/test_news_failure_evidence_budgets.py)覆蓋12總上限、舊分類／未知、malformed evidence、storage outage、restart、fingerprint、其他provider不借桶、新UTC日及只改鉅亨measurement。
最初新v5 general receipt被legacy seeder拒絕，測試實際失敗後修正：v5只接受budget2／provider_general的合法先行紀錄，不能用未初始化native receipt開額度。其餘早期fixture錯誤包含未解escaped newsId、設定暫存根下不存在的endpoint inventory、0.001秒controller非法cadence、把generic非retry source outcome誤判；保留原failed runs，修fixture／判準，不放寬正式identity／容量／negative語意。
[owned TCP整鏈ABBA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v84/owned_evidence_abba.json)用[canonical capture／receipt工具](../scripts/benchmark_news_failure_evidence.py)：四phase各24次owned HTTP、相同8份正常正文；共32份版本全部raw／body／作者／clock／fsync／收據核對。
原版各2份native private witnesses，新版各4份、恢復第二類可觀测證據；其他provider仍2份metadata。原失敗state／attempts／error／無body與healthy head signature完全相同，queue重建後額度不重置。非retry的generic `body_not_found`及native `retry_cooldown`皆保留。
完整wall中位0.429997→0.311116秒／CPU0.493950→0.373103秒；舊wall0.592251／0.267743、新0.296082／0.326151，樣本交錯有快有慢。這是bounded owned workflow，不把noise／額外兩份診斷保存當publisher提速或理論最高頻率。

### 真正載入與新原件，不只看service active

[有界部署](../artifacts/operations/agent-workflow/runs/v84-loaded-bounded-failure-evide-20261011T095707-af2ce305/run.json)17:57:14只平順重啟新聞owner／timer；新owner `1c966b677bce48499a69fb7873af9b8f`，PID3479616，NRestarts0。
[部署讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v84/deployed.json)核對fresh heartbeat／loaded budget2／全部來源身分／unknown query／舊quota／兩份legacy raw及SHA；完整receipt恢復wall36.958／CPU15.266秒、owner initialization wall37.507／CPU15.557秒。未把初始化期間前owner stopped快照當ready，也未重啟其他服務。
[正常owner新原件重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v84/observe.json)：17:58:03的5125945、17:58:35的5157359、17:58:49的4898771分別來自三種native failure。全部raw、Parquet、receipt SHA與當前原失敗類別重播接受；raw仍diagnostic-only／不公開／不匯出／不算文章成功。
[私人表示差異](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v84/private_component_differences.json)定位：5125945的visible/current相同，schema另外有121字連結段落，不能補造缺段；4898771 visible/current空而schema有正文，不能用宣告取代完整可見原文；5157359 captions核對後僅差`p > em`開頭單一U+FEFF，原helper只看p第一個直接文字節點，留待下一阶段精確模板修復，仍保留原始字元／數字。

### 進度、其他來源與剩餘邊界

[runtime13/13](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v84/runtime_final.json)：241.808057秒same-owner WAL窗新增鉅亨513／Global Voices25個正文URL heads，2.121517／0.103388新head/s；另245.806962秒HTTP窗鉅亨2.746057request-start/s，仍有4transport／1次502。不是去alias独立篇數或官方最大速率。
news cgroup CPU delta51.153183秒，按database窗口約0.211545 core；MemoryCurrent545.0078125／Peak621.51171875MiB。這是正式news cgroup，不含工程test／audit、不是全WSL或GPU，也不推論與前輪不同cohort構成同源效能比較。
[128最近committed batches](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v84/recent_batches_final.json)核對5,004 immutable body versions及current heads，907,815,984 raw bytes；這個bounded cohort不全是本owner製造，也非整個lake／全歷史。
[20 enabled origin核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v84/transport_final.json)捕捉四個m.cnyes.com標頭前故障與四次origin pool reset，write/reset errors0；沒有重設整个Session／其他健康origin。沒有新HTTP的provider不被說成此窗已證明健康；既有CMoney／PeoPo拒絕、民視attribution／identity、授權／圖像／影音等缺口仍保留。
[934-source清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v84/inventory.json)18:02:42：62 configured／20 enabled／872 unregistered、192 metadata-only，6,202,288 known URLs／3,638,214 stored heads／2,130,487待授權或啟用；清冊wall39.746197／CPU30.064765秒，單次fixed-WAL audit而非高頻全表掃描。
[四寬度進度頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v84/dashboard_final/acceptance.json)1440／768／390／320皆934-source及39頁，actual鉅亨／Global Voices cards、原網址進度、source reviews、條件ETA／pause／probe time與timed retry皆核對；無JS error／外站browser request／failed resources。
下一階段從5157359的em-format原件缺口開始，明確測試正例、in-word／repeated／hidden／current mismatch負例及整鏈source原件；全部934-source目標仍active，本階段沒有宣稱所有provider錯誤已根治、全部正文已齊或官方吞吐上限已證明。

[v84工程handoff31/31](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v84/handoff_20261011T100608463492Z.json)保留；下一階段由下方v85接手，而非重啟或把該原件觀測範圍當整站完成。

## 原生em強調段落格式比較的完整正文修復

2026-10-11 v85，Asia/Taipei。沿用934-source原目標及v84已驗收的私人失敗原件；只修有來源證据的parser缺口，不把實際缺文／來源拒絕換成成功。

### 從新原件到精確且線性的比較

[修改前snapshot](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v85/before.json)保留5157359原URL／state／error／attempts／due／無body／last check，全部62 capture／授權／bulk／routing及v18原程式SHA。
實際raw `7a85032536ce5d71b12bc6f862cd8a5a688eb4af462d1241ef2e0e712883887e`的marker在attribute-free`p > em`第一個、唯一文字節點，對應current content有唯一且完全相同的markup／text；不是字中的marker、另一篇文章或付費／隐藏body。
舊[paragraph helper](../downloader/news_fulltext_publishers.py)只看`p.contents[0]`直接文字，因此在figure比較後仍丟出schema comparison mismatch；不是網路沒有下載原件，也不能靠加HTTP concurrency修這個判準缺漏。
新`_cnyes_remove_verified_em_paragraph_markers`只處理原件觀察到的single-p／single-em／single-text、無attributes、單一前導U+FEFF，current有且只有一個exact paragraph才在comparison copy比較。
visible與完整current仍先全篇一致、同ID／canonical／author／clock／free scope仍保留；schema全篇／每個數字仍一致才成功。retained body及raw完全不去marker，不log／縮放／刪除任何真實值；比較provenance獨立記為`source_current_em_paragraph_format_marker_comparison_v1`。
只有mismatch且有candidate才parse一次current；一次Counter把各current paragraph markup計數，之後indexed lookup，**O(P＋candidate markup)**而非每candidate重掃全部P段。沒有candidate或普通exact native body不parse額外current／marker view。
[HTML source fingerprint](../downloader/news_fulltext_policy.py)18→19；不是generic parser／permission／bulk／routing升版。[當前entry用原v18 policy重建](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v85/deployed.json)得到exact修改前全部62 FP，證明除這個HTML contract外沒有悄悄更改source entry或其他capture；舊資料指紋不改寫。

### 驗證原字元、負例與整條來源鏈

[332 focused](../artifacts/operations/agent-workflow/runs/v85-original-marker-retention-an-20261011T101333-2afddeb5/run.json)及[3,851 news含bulk regression](../artifacts/operations/agent-workflow/runs/v85-all-news-source-and-marker-r-20261011T101334-d85e4eda/run.json)通過。
[source contract測試](../test/test_news_cnyes_em_paragraph_markers.py)包含html.parser／lxml、desktop exp／mobile alias、stream completion、caption combination；in-word、double marker、strong／nested wrapper、attributes／hidden／comment／duplicate、current缺marker／markup不同與schema數字不同都拒絕。
原測試只錯在把lxml fragment自動加的html/body wrapper誤判成原件修改；保留failed run，改核對實際原fragment與前後Soup snapshot，不放寬source guard。
[同11份真實原件整鏈ABBA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v85/original_replay_abba.json)由[可重跑private replay](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v85/replay_full_chain.py)送到owned TCP、canonical capture／parser／失敗commit／fsync／Parquet／完整receipts；沒有publisher HTTP或正式queue writes。
固定三份v84 failed原件與八份v83已驗證正文原件，四phaseHTTP序列與八篇正常title／body SHA／raw SHA／authors／raw clock完全相同。reference只換成pinned v18 comparison function、沿同current scoped registry；不宣稱兩種production capture ABI相同。
舊phase各8正文／3原失敗，新phase各9正文／2原失敗，共**34 versions**全部驗證。5157359恢復2,117字、body SHA`7154939b4d4b539383f037c6e786df2f61370302640487892fef7a0952c5d454`，原U+FEFF仍在，作者／2023-04-27T04:30:55.000Z raw publication clock不變。
5125945缺宣告連結段落、4898771 visible/current空白，仍保留原body-not-found／stage；新helper沒有以schema替代可見正文，也未將diagnostic本身改為accepted article。
完整wall中位6.171831→6.130894秒、CPU0.652310→0.597462秒；舊wall6.241408／6.102254、新6.141971／6.119818，全部samples保存。多接受一個body、網路為owned且保留client cadence；這是完整正確性／成本證據，不推論12.5%或其他數字的production提速／官方上限。

### 正式部署、後續回補與最新錯誤

[只重啟新聞owner／timer](../artifacts/operations/agent-workflow/runs/v85-loaded-source-proven-em-temp-20261011T101917-fb10c99a/run.json)18:19:24載入新capture19；owner`a5a8a265885f4f818e07ce1a8e9e0c6d`、PID3524296、NRestarts0。
[9/9 deployed scope readback](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v85/deployed.json)核對fresh single owner、exact v18重建、新19已載入、其他source與所有授權／bulk／routing／unknown query／diagnostic reservations及seed proofs；未改其他下載器、金鑰、交易、GPU或儲存權限。
[runtime13/13](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v85/runtime_final.json)：236.354415秒same-owner WAL窗新增鉅亨296／Global Voices22個正文URL heads，1.252357／0.093081新head/s；230.234006秒HTTP窗鉅亨1.485445request-start/s，鉅亨有1transport／3次502，Global Voices有1次502。不同cohort與WAN狀況不當publisher倍速。
news cgroup CPU delta33.733506秒、按database窗約0.142724 core，MemoryCurrent955.34375／Peak1,025.921875MiB；不含工程audit/test，非全WSL／process RSS／GPU，也不把低平均CPU說成所有性能問題解決。
[128最新committed batches](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v85/recent_batches_final.json)核對5,255版本／current heads、951,892,199 raw bytes，範圍仍是bounded最近cohort，不是全lake或都由新owner製造。
[最新20 enabled傳輸觀測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v85/transport_final.json)再發現Cnyes四個header前故障、news／m各2，四次origin pool reset；gateway兩個以other近期成功host為證只隔離失敗host。Global Voices亦有四個header前故障、四次origin reset，gateway三個沒有其他成功host可供隔離，保留publisher-level pause及重試，不能憑空認定另一host健康。診斷write errors0、健康源有進度；不是所有publisher已無source error，沒有用重啟消除source refusal／Retry-After。
[四寬度actual provider進度頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v85/dashboard_final/acceptance.json)皆934-source／39頁，required Cnyes與Global Voices cards、原網址分母／進度／source reviews／條件ETA與timed retry核對；raw、作者全文與金鑰不傳到browser。
[工程handoff19/19](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v85/handoff.json)當時5157359仍是原retry_cooldown／無body，原deadline **2026-10-11 18:58:36.011 CST**、attempt／last check／lease完全不變。正式collector回補需等deadline正常claim，再核对真实新raw／body／收據／current parser；**原件重播的2,117字不是正式補回，pending不是無限期failed，也不因HTML契約升版清future deadline**。
下一阶段核對這篇正常回補，並重播新table-grid原件5117411與空representation4795333；持續追蹤Global Voices／Cnyes的新來源故障與自動恢復。全934-source、授權／歷史範圍與未知query證據仍在，目標active，沒有全來源／全歷史完成或所有錯誤均可本機根治的宣稱。

## 完整巢狀表格的逐格來源森林核對

2026-10-11 v86，Asia/Taipei。由v85仍在工作的正常owner自動保留的新原件繼續修復，未額外呼叫publisher、掃新ID或放寬付費／權限／未知query／來源拒絕。

### 來源事實與修正機制

[两份當前原件重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v85/new_native_failure_triage.json)先驗收diagnostic Parquet／receipt／原raw SHA與current source。5117411的visible/current各592字且完整SHA相同，schema538字去空白後亦全篇相同。原表格為outer table的一列、一格包inner table的六列；style對齊差異不是格子內容差異。兩個完整representation都有相同巢狀結構，舊`_cnyes_table_grids`卻一律回None。
4795333的visible/current均0字、schema550字，仍保留`missing_body_representation`，不能以schema孤證供應完整正文。這不是所有失敗都屬於同一種template，也不是取得metadata便已抓完全文。
[修改前v19快照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v86/before.json)保留兩個修復URL的原state／attempt／deadline／last check／無body，62 capture／授權／bulk／routing／transport SHA、unknown query與當日診斷額度。原失敗snapshot因來源自然進入`retry`而非假定`retry_cooldown`被拒絕；其舊JSON另保留，新snapshot只按真實兩種queue狀態核對，原v19兩份程式副本SHA不變。

[共用canonical native extractor](../downloader/news_fulltext_publishers.py)新增`_cnyes_nested_table_witness`：

- 只在原simple grid得到None的native巢狀分支執行；普通表格及`cnyes_verified_short_body`原規則不動。
- 單次iterative DOM walk記錄table／row group／tr／th／td開關順序，每格文字、巢狀表格位置、前後混合文字都在結構事件間保留。
- `rowspan`／`colspan`／`headers`／`scope`／`abbr`／`summary`原attributes直接比對，不推算或改寫市場數字；22|184不可能被2|2184冒充。
- 只允許已知文字排版wrapper；unknown／comment／script／excluded／hidden／inert、無列／無格或結構ownership錯誤都拒絕。來源完整visible/current/schema、identity/free/byline/raw clock仍須先一致。
- 不遞迴、不重掃每個outer cell的完整nested subtree、不改Soup或原raw/body；新增witness成本為**O(DOM＋文字bytes)**，普通exact/simple來源不建立森林。

HTML table的格子／跨列跨欄語義可對照[WHATWG table model](https://html.spec.whatwg.org/multipage/tables.html#table-model)。本修正驗證同來源表格森林及原文字，不宣稱重新建立所有瀏覽器layout或將扁平新聞正文當財務structured table。
[source capture](../downloader/news_fulltext_policy.py)19→20並保存`source_current_nested_table_cell_forest_comparison_v1`。來源授權／generic parser／bulk／routing契約不變；[原v19 registry重建](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v86/deployed.json)與修改前全部62 fingerprints完全相同，新owner只載入Cnyes新HTML身分。

### 同原件的完整鏈與負例

[396 focused](../artifacts/operations/agent-workflow/runs/v86-native-nested-forest-and-ori-20261011T104637-a4f5526e/run.json)、[3,921全news含bulk](../artifacts/operations/agent-workflow/runs/v86-all-news-including-nested-fo-20261011T104659-5f328161/run.json)通過。
[新增70 tests](../test/test_news_cnyes_nested_tables.py)涵蓋兩種parser／三種native aliases／普通及streamed頁／outer cell前後原文字、style-only差異；格內numeric collision、搬列、攤平／複製、th/td不同、各span/header attribute、隱藏不同位置、inert/comment/script/未知markup、空table／格外文字與schema数字不同皆拒絕。原simple-grid hot path及nonmutating witness亦驗證。
[13份真實原件owned TCP-to-receipt ABBA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v86/original_replay_abba.json)由[private replay](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v86/replay_full_chain.py)沿canonical HTTP、raw capture、parser、failure commit、fsync、Parquet與全部receipts。它是工程replay，沒有正式queue writes或新publisher HTTP，不把replay capture當歷史來源capture。
原v19各9篇／4失敗，新v20各10篇／3失敗，共**38 versions**全部核對。九個已接受control的title／body SHA／raw SHA／author／raw clock完全相同，包括v85的2,117字原U+FEFF。新592字body SHA`dd27ff831ec862ea0b3395350879d099984099a5f051f2e8d743071c2e205357`與source可見原文完全相同；作者林羿君及raw publication`2023-03-20T05:57:09.000Z`保留。
四phase的HTTP原URL序列一致。舊wall8.334265／7.256689、中位7.795477秒，新7.249445／7.745659、中位7.497552秒；舊CPU1.941717／1.349165、中位1.645441，新0.829487／1.745834、中位1.287660秒。samples交錯且多恢復一篇body，這是成本／正確性證據，不當作固定publisher提速百分比或理論最大速率。

### 正式服務、速度瓶頸與各來源繼續抓取

[通過全部gates後只重啟news](../artifacts/operations/agent-workflow/runs/v86-source-proven-nested-table-l-20261011T105030-c55fc366/run.json)，18:50:34起PID3582387、NRestarts0。首次readback新PID雖active，status尚屬舊owner，故拒絕該工程驗收；等fresh source registration後，[10/10 deployed](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v86/deployed.json)owner`d2c1f0f5e17b4d3495b64d61babfc66f`、queue/status/capture20/core SHA及mtime全部一致。舊owner負snapshot保留，不用active代替ready。
[同新owner runtime13/13](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v86/runtime_final.json)與[128 committed batches](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v86/recent_batches_final.json)接受；後者核對4,569版本／heads及829,530,958 raw bytes，只是最近cohort，不是whole lake或全部由本owner抓到。
新owner的初始2-slot窗146.862837秒新增鉅亨94／Global Voices15個body URL heads（0.640053／0.102136head/s），HTTP154.024494秒鉅亨0.759619request-start/s。初始22個article HTTP累計57.216218秒，平均約2.600737秒，解析累計0.694088秒，平均0.031549秒；來源網路等待比解析高約82倍，不能靠刪正文guard或縮短parser來宣稱消除WAN延遲。
18:56後正常capacity進入ramping，4 response slots與0.2秒client target；maximum8是client資源上限，不是官方上限。新壓力必须有實際terminal/body/receipt windows驗證，target5req/s不等於actual5req/s，更不等於5個去alias獨立新聞/s。別把初始warm低速當整段最終容量，也不能把不同WAN/cohort比較為同源倍速。
[當前20 enabled origins傳輸／恢復](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v86/transport_final.json)保留新gateway及amp-news robots來源失敗；origin隔離、服務pause/retry及正確headers/Retry-After繼續有效，healthy news/mobile與Global Voices不等其他host修好才抓。沒有新觀測的來源不當健康或全部完成，也不讓restart清除source refusal。
[四寬度真實provider進度頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v86/dashboard_final/acceptance.json)仍934 sources／39 pages、Cnyes與Global Voices卡片、URL分母／条件ETA／來源reviews／retry與blocked states全部核對，無JS錯誤／publisher fetch／失敗資源，原件／全文／金鑰不傳browser。
初次20/20工程handoff保留兩個原URL的正常due與pending邊界；後續正式回補另驗收，不更改原截止日或把owned replay冒充新capture。

### 兩個格式目標及八個實際逾時的正式回補

[兩個原URL的真實新正文](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v86/actual_two_target_recovery.json)均由正常owner取得，selected-article current source重播、所有原raw／正文SHA／authors／raw clock／不可變批次收據及current fingerprint20核對通過：

| 修復目標 | 正式取得時間（CST） | 原截止日及結果 |
| --- | --- | --- |
| 5117411 `exp=a`、完整巢狀表格 | 2026-10-11 18:56:55.114 | 18:52:50.685後正常oldest-due輪次取得；592字原正文SHA相同 |
| 5157359 `exp=a`、`p > em`前導格式 | 2026-10-11 19:03:53.426 | 原18:58:36.011截止日完全保留；2,117字及原U+FEFF的正文SHA相同 |

clock、作者與body全部來自實際原件，不把發布時間改成現在，也不把source captured_at倒填成replay時間。這是兩個URL的正式資料接受，不是完整歷史。
[19:02最新runtime13/13](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v86/runtime_final_current_accepted.json)595.416215秒同owner窗新增鉅亨491／Global Voices59個body URL heads（0.824633／0.099090head/s），鉅亨actual1.187867request-start/s；cohort含暖機／source故障／capacity變更，不能當相同WAN來源倍速、去alias獨立篇数或官方最大值。
這次重查先因body receipt snapshot超過300秒而拒絕；[同128不可變路徑重新驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v86/recent_batches_final_refreshed.json)在新WAL snapshot重查4,569版本後才接受，沒有延长freshness gate或假裝旧收據是新觀測。
[19:04當前20 enabled origin故障](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v86/transport_final_current.json)新owner有8個header前timeout（news6／mobile2），8次origin pool reset、reset／diagnostic write errors0；其中4個idle少於1秒、3個少於5秒、1個少於60秒，**不能把全部timeout歸因長閒置stale connection**。另5個gateway皆有其他成功host證據而隔離該host，3個amp-news robots-origin失敗保留source pause。Global Voices79個terminal當窗未見fault，未發請求的provider仍不是健康或完成證據。
[八個逾時URL的真實forward recovery](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v86/actual_eight_transport_recoveries.json)逐一綁定private failure object/file SHA、故障時間及**故障後**新captured_at，8/8已complete，再核對原raw／body／authors／clock／current parser／批次收據。這不是reset計數、active或故障前舊正文代替恢復；也不是來源未來不再逾時的保證。
[最新30/30工程／實際source handoff](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v86/handoff.json)綁定原scope、新owner、兩個原格式目標、八份逾時回補、當前runtime／raw receipts／20-origin診斷及四寬度頁面SHA，兩個原目標pending皆false。剩餘來源缺文、source refusal／權限、parser backlog、全歷史與934-source目標仍active，不宣稱全來源都根治或達到官方速度上限。

## 原生查核正文及共用故障後資料恢復證明

2026-10-11，v87。這一階段不再只修鉅亨：以全來源清冊挑選仍無正文且能先核對來源規則的既有金融原文。
[20:09清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v87/inventory_final.json)保留全部934來源鍵、6,202,364個已知URL、3,647,512個stored heads；63設定、21啟用、871未註冊，2,130,525個URL仍待授權。
粒度是normalized source URL，不是去重新聞篇數，也不是已知URL之外的全網歷史。

### 根因與實作

- `downloader/news_fulltext_fact_checks.py`沿用共用HTTP／queue／Parquet／收據，沒有第二條下載管線。來源原件顯示CMS schema署名`hwtseng`，原生側欄才是查核記者曾慧雯與責任編輯陳偉婷；不能把CMS帳號映射成人名或把編輯冒充記者。
- 確認requested／canonical／og／Article+Report schema、本篇post ID與publisher identity，再取原生Kadence第二列第二欄完整查核報告。排除metadata控制／第三方圖片與說明；拒絕hidden/inert、衝突、缺段、未審查模組和付費宣告，不用過寬`.entry-content`補值。
- `2026-07-28T07:05:01+00:00`發布、`07:07:55+00:00`修訂均保存原精度；原生visible日期另存，不用本機連結metadata的`07:06:09`冒充發布時間，不推測offset。原始股數、價格、比率與全文段落不改值。
- [官方版權聲明](https://tfc-taiwan.org.tw/copyright-notice/)與[已知金融查核原文](https://tfc-taiwan.org.tw/fact-check-reports/0050-no-stock-split-2026/)支援這個private、具來源與署名的非營利原文範圍，不虛構CC版本或擴及外部著作／商用／模型輸出。凍結cohort只含既有1個URL，不新增feeds、API、ID猜測、媒體或全站抓取。
- `scripts/audit_news_transport_recoveries.py`共用所有已註冊provider的immutable failure／當前parser／raw、正文、署名、clock與批次驗證。只有故障後更晚的complete head且通過新證據才算恢復，不能用pool reset、service active或故障前旧body替代。
- `scripts/probe_news_sources.py`補explicit選取的交接取消。producer先重試、已完成、仍有deadline／lease或已遇403／429時，不取得HTTP owner、不發重複請求、不清狀態；留下明確skip。壞clock、重複選取與超界仍拒絕。
- `scripts/benchmark_news_fulltext.py`修正isolated finite replay在metadata後沒有freeze cohort，導致claim0筆而空驗證看似成功；現在使用同一`sync_finite_cohorts`，不轉continuous或繞過membership。原始未接受結果與失敗run均保留，後續有實际1筆／phase驗證。

### 真實資料、吞吐及剩餘邊界

[原件重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v87/tfc_current_original_replay.json)驗證334,579byte原回應SHA；[正式正文](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v87/tfc_actual_capture.json)為1,486字／4,088 UTF-8 bytes，body SHA `bfdf8614bc54b01a1bf8546fba26d025f2a3910ee82a48921dddc22c3ec7a743`，19:43:53實際capture，19:44:21批次落盤。
Reporter、editor、CMS帳號、發布／修訂與原件分開保存；**不是歷史發布當時版本，也沒有自動訓練注入**。
[相同原件四phase](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v87/tfc_parse_durable_abba_corrected.json)使用既有canonical claim／fsync／Parquet／收據與獨立驗證，html.parser／lxml全文、署名、時鐘相同且每phase1筆接受；median0.749秒是離線完整管線，不是WAN速度。

[同owner runtime13/13](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v87/runtime_final.json)於20:09接受：20:04:41恢复的owner在253.36109秒DB窗新增鉅亨683／Global Voices25個正文URL heads（2.695757／0.098673head/s）；獨立244.052508秒HTTP counter窗鉅亨3.314860request-start/s。
兩個snapshot有不同clock，不能把metric版本數當WAL新增head數；來源、capacity、暖機與資料cohort不同，**不能當因果倍速、去重篇數或官方最高速**。Client target0.1秒／6slots不代表實際10req/s。
[最近128批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v87/recent_final.json)獨立重查5,398個原始正文版本、974,055,900byte私人原回應，hash／current-head witness／receipt errors0；這不是整個資料湖的全歷史驗證。

[先前9個當場傳輸錯誤](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v87/current_observed_error_recoveries.json)有8筆真正故障後的新正文／原件／署名／clock／收據接受。第9筆`1574038`在20:04:29已恢復HTTP取得，但仍`body_not_found`且無body；新的21:04:29截止日保留。
預排診斷在drain時遇到這次普通重試，最初安全拒絕；[修正後0請求skip](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v87/cnyes_1574038_current_skip.json)證明不把安全取消當新傳輸故障或偷偷清deadline。
下一步仍需該representation的原件證據，**不能僅因HTTP恢復就宣稱第9筆正文已解決**。
[新owner21來源傳輸核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v87/transport_final.json)與[因果恢復核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v87/final_observed_error_recoveries.json)分開：當窗尚無transport failure sample，但仍有502、amp-news robots失敗與parser backlog；沒有sample不是全來源健康證據。

上下游[5篇診斷清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v87/newsmarket_actual_probe.json)實際robots200／public path允許，第一篇原URL返回Cloudflare403，未讀／未存錯誤body，後4篇不再送出；同root cooldown保留。[官方條款](https://www.newsmarket.com.tw/terms-of-service/)的自身著作範圍與這個存取拒絕是不同問題，不更換UA／Cookie／身分／host／API繞過，也不宣稱五篇已下載。
苦勞網8篇的現行原創／轉載權利仍待核對；RTI、Gov、RFI的web-reader失敗不冒充正式NewsHTTP拒絕或資料缺失。CNA已有使用者授權，但本機`CNA_API_KEY`仍缺，當前capability inspection0 HTTP／0 tool calls，不重問授權或改走HTML抓取。

[四寬度唯讀頁面](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v87/dashboard_current/acceptance.json)核對934來源／39頁、Cnyes／Global Voices／TFC實際cards、原URL進度與來源review；瀏覽器不請求publisher、不接收正文或金鑰。[handoff26/26](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v87/handoff.json)保證原62來源契約、15原cohort、supplements、unknown-query及deadline與main config保留，只有TFC新增。
最後全news含bulk **3,975項／67.00秒**通過；原來源分類斷言、有限cohort benchmark、synthetic endpoint分類及初次交接取消的失敗證據都未刪除。
**全部934來源／歷史／所有错误尚未完成，goal仍active。**

## 每日證據耗盡後的固定小時有界原件與新模板定位

2026-10-11 v88，延續同一新聞目標。讀取3991-test前的Git dirty state並保留全部非新聞工作。
舊owner於20:36已有229次`failure_diagnostic_budget_exhausted`、118次schema-stage缺口。HTTP有回應並不代表正文可接受；無原件時也不能判成不可修復或增加盲目請求。

[既有診斷sink](../downloader/news_fulltext_diagnostics.py) payload5→6／budget2→3，原每日general2與五個static stage各2不重置。
兩個既有可獨立證明raw範圍的provider，daily bucket用完後，每UTC小時／固定bucket最多預留1份。鉅亨最多`(2+24)×6=156`attempts/day、Global Voices最多26；其他provider仍general2/day。每份原回應上限8MiB，因此鉅亨極端未壓縮上界1248MiB/day，不是宣稱實際流量；本輪三份合計小於1MiB，且都是原下載已收到的bytes，**新增source HTTP=0**。
不按URL、FP、重啟或任意錯誤字串開bucket；已耗盡的小時在public-scope parser／hash／fsync之前返回。舊receipt migration無法驗證時新bucket維持關閉；reservation在解析與寫入前durably由同queue owner記帳，I/O故障也不退款。paid／hidden／clock／canonical／byline／article-license guard不變，metadata-only不變正文成功。

[精確v5來源快照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v88/diagnostic_reference.json)與[owned TCP整鏈ABBA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v88/owned_evidence_abba.json)有四phase，每phase24次owned HTTP、8份相同正常正文與收據、相同失敗state／attempts；舊Cnyes4份對新6份private witnesses，其他source2份metadata不變。新舊時序有冷啟動／cache差異，**不能把中位0.468→0.233秒稱publisher倍速或證明最大速率**；接受的是可觀測性、同請求量、正常輸出與重建後quota一致。
101focused核對UTC hour／timezone、24小時有限上界、restart／FP不重置、corrupt seed fail-closed、儲存故障先扣額度及spent-hour不重解析。全news含bulk **3,991項／69.29秒**通過。

[第一次candidate](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v88/candidate.json)因dataclass tuple對JSON list產生報告假差異，安全gate停止而沒有重啟service；證據保留。
[修正同JSON representation後的candidate](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v88/candidate_accepted.json)與[部署14/14](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v88/deployed.json)核對63個原capture／authorization／policy／bulk／routing／unknown query、九份原不可變證據、八個原reservation。只更換新聞owner為`99778d1612c24c74a267ae7ca98f577d`，20:37:02正式啟動budget3；只有Cnyes／Global Voices的measurement identity更新，不能把舊速度重新貼新版本標籤，其餘provider calibration不動。

[真實hourly原件16/16](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v88/observe.json)已核對三份private Parquet／receipt／raw SHA及當前negative replay：

- `5007990?exp=a`於20:37:27，1437字visible/current完全相同；JSON-LD差25字，小標題實際位於同image figure的兩個strong子節點，既有risk-warning figure helper不涵蓋。仍`body_not_found`，尚未偽造修復。
- `1305045`於20:37:28，visible/current無文字、schema無articleBody；current原件有單一置中p／來源image及alt，不是整頁空模板。沒有OCR或下載image，下一步需嚴格來源image-only分類，不能反覆當網路失敗。
- `4898771`於20:39:36，仍`missing_body_representation`，無成功正文。

[原件／figure／empty-markup結構定位](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v88/figure_empty_boundaries.json)全程0HTTP／0queue writes、未改raw或原publication。不同類別各有修復或来源邊界，不以診斷收據冒充文章接受。

[同owner整體runtime13/13](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v88/runtime_final.json)：239.787851秒新增鉅亨162heads（0.675597/s）、Global Voices25（0.104259/s）；另一232.539771秒HTTP counter窗口鉅亨1.096587req/s。HTML transport累計626.024秒、204次解析5.004831秒，主瓶頸仍是來源回應／等待而非盲目增加CPU；不是matched publisher speedup、無alias去重後篇數或官方rate proof。
[128批目前耐久版本](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v88/recent_final.json)核對7242版本、1315635782private raw bytes、errors0，只是bounded cohort不是全lake完整。
[21 enabled source故障觀測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v88/transport_final.json)有鉅亨3次awaiting-headers timeout、3次精確origin pool reset，reset／diagnostic write errors0；2次gateway-host與1次amp-news robots-origin pause保留。[真實forward body／receipt proof](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v88/current_observed_recoveries.json)為2/3，不把HTTP重連或舊body當第三筆已補回。

[全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v88/inventory_final.json)仍934keys／63設定／21啟用／871未註冊，6202365knownURLs、3653846storedheads、2130525awaiting-authURLs、192metadata-only sourcekeys。
[四寬度進度頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v88/dashboard_current/acceptance.json)1440／768／390／320均核對934來源39頁、三個實際provider cards、原URL進度與review；publisher requests／JS errors0。
**全934來源、所有parser／權限／source拒絕與全歷史仍active，沒有完成宣稱。下一步先修已定位的strong figure與image-only誤重試，再按source證據繼續。**

## 同圖編號小標題與來源圖片-only的精確分類

2026-10-11 v89。原件是上一個正式owner已收到並依hourly固定bucket保存的兩份失敗回應，不另抓正常文章；[結構定位](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v89/structures.json)與[修正後原件重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v89/candidate_original_replay_corrected.json)都沒有publisher HTTP／queue writes。

- `5007990?exp=a`：相同圖片figure內是兩個具有相同已觀測樣式的strong，第一個為編號、第二個為小標題。新增nonmutating comparison identity，仍核對完整current／visible／schema文本、同圖、唯一membership與順序，hidden／unknown／改圖／改段落照舊拒絕。原1,437字正文保留，26字小標題另外保存在`verified_article_notices_json`，kind是`same_article_image_figure_numbered_heading`，不冒充caption或風險警告，也沒有圖片HTTP。
- `1305045`：NewsArticle沒有articleBody，current／visible是同一p／img，source作者與clock完整、free資料同ID；可見署名實為「鉅亨網新聞中心」，不宣稱原作者名字可見。只有精確的center-p、source-owned同ID圖片、空schema／video、textless wrapper及可見性核對後，分類`source_body_image_only`。沒有假body、alt補文、OCR／圖片下載、accepted正文收據或遠端training export。

兩份原件仍保留失敗時的capture20 fingerprint與不可變SHA，不能改成capture21。新parser的source／comparison契約只升Cnyes HTML20→21，bulk／checkpoint不重開；其他62來源、全部授權、routing、未知query、finite cohorts、原diagnostic收據／budget seed與累計額度保持原狀。[candidate12/12](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v89/candidate.json)及[新owner19/19](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v89/deployed.json)接受。第一個預檢腳本誤讀上輪summary而不是state-schema，保留KeyError run；發生在stop之前，沒有service mutation，修正後才執行正常news-only graceful handoff。

[同15份真實原件的owned TCP全鏈ABBA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v89/original_replay_abba.json)：HTTP → source extraction → raw／body／author／clock → fsync／receipt → 關閉／重建queue → 全版本核對。10篇原本正常controls及3個unfixed negatives保持相同；每phase17 HTTP不變，舊parser10版本、新11版本，四phase共42耐久版本；新的image-only沒有body receipt。Reference SHA固定，兩次reference與两次candidate的wall／CPU快慢全部保留，不把loopback的微小差異說成源站倍速。

602 focused／4,141全news含bulk回歸通過。21:12:44新正式owner`7d20e3dd9abd4c3383c4316b7ff4c35b`載入candidate：五個目標均保留原期限或有真正forward check，沒有因版本升級清掉cooldown。[runtime13/13](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v89/runtime_final.json)的216.686111秒窗口新增鉅亨180heads（0.830695/s）、Global Voices22（0.101529/s）；另一214.279959秒HTTP counter窗口鉅亨1.381370req/s。HTML transport230次308.040336秒、解析229次6.165167秒，主瓶頸仍是來源等待。不是unique去alias篇數／matched WAN speedup／official ceiling。

[目前128批收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v89/recent_batches_final.json)驗證5,150版本、936,824,411私人raw bytes、errors0，只是bounded recent scope。[21 enabled transport盤點](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v89/transport_final.json)新owner有一次headers前逾時；[實際forward recovery](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v89/forward_transport_recoveries.json)核對m4485583的新raw／body／作者／clock／receipt與current replay，1/1真實補回，不拿origin pool reset或舊body当恢復證明。其他provider的付費、權限、404與原站拒絕不是這個格式修正能消除的問題。

[全來源snapshot](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v89/inventory_final.json)934keys／63設定／21啟用／871未註冊，6,202,422 known URLs、3,655,569 storedheads、2,130,553 awaiting-auth URLs；不把原metadata日期當publication。[唯讀進度頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v89/dashboard_current/acceptance.json)1440／768／390／320四寬度均934來源39頁、Cnyes／Global Voices／TFC cards、原URL進度／source reviews核對接受，browser publisher requests／JS errors0。

**正式格式修復目標仍等21:53:22／21:56:08正常重試，replay不是正式下載。** [三份後來小時原件](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v89/later_hour_originals_triage.json)又定位5309063的schema／圖片內來源文字、1693999的短文字與4898771的images-only current／visible對長schema。維持negative／原件與原因，逐項修復或證明來源邊界，不能把不一致的schema宣告直接補成可見原文。全934來源及全歷史goal繼續active。

## 圖片內來源署名與完整報價片段的分流

2026-10-11 v90，沿用單一新聞owner與共用HTTP／queue／fsync／receipt／唯讀dashboard，不另建crawler，不修改其他下載器、交易、訓練或授權。先封存source21代碼與63-source scope，兩份原件維持source20 capture的不可變SHA，不重新標記成22。

- `m5309063?exp=a`：三個current圖片figure與visible順序一致；最後一個是精確的img＋p/em/strong，宣告「文章來源」而非caption／風險警告。新增nonmutating結構identity，仍要求同图／唯一membership／順序／可見性／完整current與schema相等；unknown shape、hidden、變圖、變段落或schema數字不一致皆失敗。1,620字正文不變，來源署名完整存於`verified_article_notices_json`，kind`same_article_image_figure_source_credit`，不推論或替換原作者`Betty鍾麗結`。正常controls不增加全域重解析。
- `1693999`：current／visible只有price、signed change及signed percent，schema表示省掉兩個格式空白。新精確grammar容許這個無空白表示，但仍先核對整份current／visible／schema、同ID、free與aware publication clock；只有price、少了percent、其他句子／數字或額外media不能判quote-only。不捏造正文、不丟棄原始數字、不改quote單位、不因升版重開bulk歷史。初candidate replay的quote失敗留存；修正格式grammar並重跑全部測試後才部署。

[修正後两份原件核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v90/actual_original_replay_accepted.json)接受：前者完整body、原作者與2023-08-31T11:20:06Z，後者`source_body_quote_only`非正文outcome，不是body下載成功。HTML capture21→22、quote outcome2→3；bulk fingerprint／所有授權與其他62個來源不變。

550 focused及4,222全news含bulk通過。[同17份實物整鏈ABBA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v90/original_replay_abba.json)保留owned TCP → extraction → raw／body／author／clock → fsync／receipt → queue關閉重建與全版本核對。11篇既有正文controls、3個尚未修復的negative與既有image-only outcome不變；每phase19 HTTP，舊11／新12正文版本，共46耐久版本；quote/image-only沒有假body receipt。Reference SHA固定，快慢phase全部保留；不宣稱publisher速度倍數。

[candidate13/13](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v90/candidate.json)、[載入20/20](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v90/deployed.json)、[工程／資料／進度handoff29/29](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v90/handoff.json)接受。新owner`bb5e4e12f0ff4a21923eac8988bdc1d0`於21:31:33啟動，source22已由queue/status及原代碼SHA／mtime核對；所有target原due或真正forward check、原private diagnostics／budget seed與累計額度、finite cohorts／routing及unknown queries均保留。

**5309063是正式補回，不只是重播。** [指定head／immutable batch／原件及current parser](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v90/formal_source_credit_recovery.json)驗證21:34:42的1,620字正文、原作者、source clock、1筆獨立來源署名，SHA`77fc7d82e5cdcf247ed76fedea3964666923e8fbfc6248cd420e597252864d7b`，capture契約符合22。另[已保存的canonical 5164089](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v90/existing_canonical_body_only.json)1,054字可由當前parser重播，但原capture為Oct8／舊fingerprint；沒有新HTTP、alias等價推論或?exp=a回補宣稱，沒有為了補顯示改寫歷史收據。

[runtime13/13](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v90/runtime_final.json)：368.802436秒同owner新增鉅亨295heads（0.799886/s）、Global Voices36（0.097613/s），另一369.262132秒HTTP counter窗口鉅亨1.354052req/s。507次HTML transport累計923.508342秒、496次解析12.802790秒；並行wall累計不能當單一程序耗時，也不是官方上限／去alias後文章速度／同cohort提速證明。

[21啟用來源故障觀測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v90/transport_final.json)有鉅亨8次awaiting-headers timeout、8次精確origin pool reset、1次gateway-host及1次robots-origin policy pause；[故障後正文與收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v90/forward_transport_recoveries.json)為7/8實際forward recovery。剩1477728不是傳輸沒回來，而是後續body_not_found，不能用重連或舊body說8/8健康。其他來源的權限、付費、404與拒絕仍獨立列入清冊，不清source cool-down、不繞過robots／403／429；healthy publishers繼續。

[128批耐久收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v90/recent_batches_final.json)4,231版本、767,128,975私人raw bytes、errors0，範圍只是recent batches。[934來源inventory](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v90/inventory_final.json)仍63設定／21啟用／871未註冊，6,202,424 known URLs、3,657,069 storedheads；[1440／768／390／320四寬度](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v90/dashboard_current/acceptance.json)全部39頁、三個provider cards／原URL進度／source reviews接受，publisher requests／JS errors0。

21:44 capacity正常升到4 slots／0.2秒client target；這只是已量測provider控制階梯，不是已達實際5req/s或官方ceiling。**全來源／全歷史仍active；當時5007990、1305045與1693999各保留21:53、21:56與22:29正常重試期限，後續正式結果記於v91，不改寫本階段的歷史收據。**

## 盤後速報連結格式、線性核對與正式補回

v91沿用canonical collector，只修正經原始HTTP證據支持的格式與共用查找。
本階段沒有增加provider、API、有限cohort、raw權限或來源查詢範圍。

[兩筆到期失敗的有界觀測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v91/current_failed_originals.json)
在新聞owner優雅排空後才選當前到期、沒有body、沒有lease的head；只探查兩筆並自動恢復service／timer。
不是以過去due授權重複健康請求，沒有清除cooldown或重置diagnostic額度。

- `m.cnyes.com/news/id/5276212`：同頁visible與current各431字，schema省略格式字元。
  原有「盤中速報」連結prefix可核對，兩個「盤後速報」連結prefix未涵蓋。
  只補上精確prefix；每條仍須唯一current href／單一文字節點／來源HTTPS新聞路徑／ul-li結構，
  再核對整份schema。只修改comparison clone；原正文保留7個U+FEFF（5個連結、2個段落）。
  不抓連結文章、不改數字、不把格式清理當成來源文字修改。
- `news.cnyes.com/news/id/1507405`：HTTP200但來源的current、visible、schema標題皆為空。
  `raw_public_scope=false`，沒有保留未通過來源scope的原始回應，沒有借其他文章標題。
  仍為可見缺口，不能因HTTP200就接受成完整新聞。

原先核對每個候選anchor時執行一次`current.find_all`；A個候選核對N個current DOM節點，掃描為O(A×N)。
現在懶建立一次href→links索引，再逐個核對，掃描／查找總量為O(N+A)，額外空間O(M)，M為current連結數。
當N隨連結數等比例增加時，這段工作由平方成長改為線性成長。
沒有候選仍不解析current DOM；重複href、不同文字、不同marker、混合節點或非列表仍拒絕。
既有兩種prefix與新prefix共用同一個實作，沒有平行下載路徑。

373 focused及4,241全news含bulk通過；[原件重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v91/actual_original_replay_accepted.json)
431字、SHA`b6d9f1f5696f497ef1c24126c17488b4e422b991063befad57cd609c5f53dec5`、
作者`鉅亨網新聞中心`、source clock `2023-08-07T01:06:15.000Z`皆核對。

[修正後的同18份原件整鏈ABBA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v91/original_replay_abba_corrected.json)
包含owned TCP、request／identity／body／raw／byline／clock、fsync、immutable receipt及queue重建核對。
12篇既有正文controls及5個真正negative保持不變；每phase20HTTP，舊12／新13正文版本，共50耐久版本。
[初次失敗證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v91/original_replay_abba.json)
完整保留：驗證腳本錯把5個link marker當作正文全部marker，未計原有2個段落marker。
修正驗證witness後完整重跑，不改production parser或捨棄較慢phase。

合成相同舊prefix的20／100／400-link核對，各5次重複、ABBA四phase；
400-link median wall為2.084320→0.253282秒，僅此helper的相同工作量約8.23倍。
真實18份整鏈median wall為9.710999→9.668799秒，CPU median 0.931432→0.895239秒；
樣本、快慢phase與warm-up差異保留，不宣稱matched WAN提速或理論最大頻率。

[candidate13項](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v91/candidate.json)、
[載入23項](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v91/deployed_current.json)、
[交接32項](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v91/handoff.json)接受。
新聞service於22:05:44載入source23，owner`00ddb5b4721a43c8958ded3af8bd65ab`。
最初owner交接期間status尚屬前owner，初檢拒絕接受並保留；待queue／status／code一致才重新驗證。
沒有因初檢失敗反覆重啟服務或改寫原始期限。

[正式target補回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v91/formal_postclose_marker_recovery.json)
證明22:08:07的431字新capture，immutable batch、current replay及capture contract23全部接受。
[前兩份正式body的current replay](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v91/prior_formal_bodies_current_replay.json)
也接受：5007990於21:53:25為1,437字、5309063於21:34:42為1,620字。
兩者是原capture契約，`capture_contract_matches_current_source=false`如實保留，沒有把歷史fingerprint改成23。
1305045於21:56:13正式分類source_body_image_only，沒有合成假正文。

[同owner runtime13項](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v91/runtime_final.json)：
233.403099秒新增鉅亨95heads（0.407021/s）、Global Voices20heads；
另一231.011133秒HTTP窗口鉅亨0.878746req/s。
143次HTML transport累計501.390673秒對133次解析3.225763秒；並行累計wall不是單一程序耗時。
本窗口比v90更慢，包含新的header前逾時與adaptive recovery；保留負向結果，不稱整體已加倍或達官方上限。

[21啟用來源傳輸巡檢](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v91/transport_final.json)
只有鉅亨有本owner新增傳輸失敗：7個ReadTimeout，7次精確origin pool reset，
1次robots-origin headers不可達與來源policy pause；reset／diagnostic write error為0。
[故障後資料證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v91/forward_transport_recoveries.json)
為5/7真實forward body recovery；4465689?exp=a仍http502、4470101仍transport error，交由原重試排程繼續。
此report的accepted表示證據有效，不表示7/7來源皆健康。

[128批收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v91/recent_batches_final.json)
驗證4,419正文版本、errors0；只證明recent batches。
[934來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v91/inventory_final.json)
仍63設定／21啟用／871未註冊，6,202,425 known URLs、3,658,889 storedheads、2,130,553 awaiting authorization URLs。
CMoney拒絕、會員付費、民視作者／來源scope、PeoPo license及不存在來源皆有獨立錯誤清冊；
未改權限、未繞過拒絕、未把image／embedded／quote-only當body failure反覆刷。
[四寬度唯讀頁面](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v91/dashboard_current/acceptance.json)
1440／768／390／320皆934來源、39頁及三個provider cards接受，publisher requests／JS errors／failed resources為0。

22:12仍為adaptive recovering_transport，client interval0.5秒、2 slots；
是實際壞回應下的有界恢復，不是解析失敗誤算限速，也不代表官方ceiling。
全934來源／所有歷史的原目標持續active，未將本次工程驗收標為全量完成。

22:16的[後續forward資料核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v91/forward_transport_recoveries_later.json)
已由5/7提升為6/7：4465689?exp=a於22:11:00正式capture，其raw／body／byline／clock／receipt及current replay接受。
4470101的HTTP已回來，但仍body_not_found，沒有body SHA或capture，不算正文已恢復。
其正常重試due為22:19:36；1507405仍來源空標題、due23:08:20。沒有清due或重複探查未到期URL。
下一步是核對4470101的正常到期回應／有界原件及其他剩餘來源缺口，新聞owner與timer保持運行。

## 精確空白圖框與不增加請求的驗證

v92只有鉅亨HTML capture23→24；其他62個來源的設定、授權、API bulk、routing、有限cohort、
診斷預留額度與舊私人原件保持不變。中央社依使用者本輪指示暫緩，保留待辦，不要求金鑰或新增API。

[到期單筆觀測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v92/current_failed_original.json)
於22:20:55取得4470101的真實HTTP200：三個表示同篇、literal free0、341字visible／current正文、
作者張欽發、source publication `2020-05-01T02:59:35.000Z`。
只保留既有來源scope允許的私人原件；canonical owner排空後探查一次，自動恢復service／timer。
沒有重排未到期URL、清cooldown、增加worker SQLite寫入或把診斷當正式文章。

來源依序有日K圖／「華通日 K 線圖」圖說、`<figure>NBSP</figure>`及既有空白figcaption figure。
原parser把沒有img的空白figure視為未核對結構，使全文schema比較失敗。
新增`empty_figure_placeholder`只接受沒有attributes、唯一非Comment文字節點且精確單一NBSP；
與`empty_caption_placeholder`分開，仍須current／visible的順序、唯一性、完整figure witness及整份schema一致。
image、數字、隱藏、class／style、comment、nested、混合whitespace、重複或額外placeholder都拒絕。
不刪原始正文、不把圖說混入正文、不下載圖像、不修來源數字或推測文章clock。
來源ad slot內的U+200C仍由既有精確selector排除；沒有新加全頁零寬字元strip。

[真實原件重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v92/actual_original_replay_accepted.json)
接受341字、SHA `814ee70c24913257beefb7aa3b2265b4079deea490dcde666c7aba0a58cd64f7`，
作者／publication／圖說皆保持來源值；這仍是私人診斷驗證，不是正式下載或歷史PIT證據。
492 focused及4,287全news含bulk通過。

[同19份原件整鏈ABBA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v92/original_replay_abba.json)
固定13篇健康正文、5個真正negative與這1個修復目標。
每phase21個owned TCP HTTP、extract／raw／byline／clock／fsync／收據／關閉再重建驗證；
舊13／新14正文版本，共54耐久版本，controls與negative判定全部不變。
median wall舊10.189382／新10.166167秒，CPU舊0.960985／新0.914207秒；
所有快慢phase保留，不把loopback數字當WAN加速或官方上限。

[24項正式載入核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v92/deployed.json)
確認22:41:28的新owner `80e8429d3348495c8d5f39028d8f2d33`已載入capture24，queue／status／code一致。
[32項工程交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v92/engineering_handoff.json)
只接受部署、runtime、近期正文與唯讀頁面；**沒有包含4470101正式回補條件**。
獨立`stage_receipt handoff`仍強制要求`formal_bare_figure_recovery.json`、current head與實際batch證據，沒有降低資料完成門檻。
4470101於22:23:05又發生真實transport error，22:49觀測的正常due仍23:23:05；沒有清期限或把舊observed clock改成新HTTP。後續正式結果見下節。

既有`materialize_reviewed_supplements`是finite、已核准私人原件且保留due的入口，
不能直接當作rolling鉅亨歷史主線的回補機制。既有cached bulk及metadata replay照常運行；
本輪沒有另造HTTP／SQLite owner，沒有把已審核診斷JSON當作任意正式正文。

[同owner runtime13項](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v92/runtime_final.json)
在199.544516秒新增鉅亨127heads（0.636449/s）、Global Voices19heads；
另一199.338920秒HTTP窗口鉅亨1.033416req/s。
165次HTML transport累計362.921037秒、161次parser累計5.145237秒；並行累計wall不是CPU百分比，
仍以網路／源站等待為主，沒有宣稱已根治外部逾時或全部來源錯誤。

[21啟用來源傳輸巡檢](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v92/transport_final.json)
看到鉅亨2次ReadTimeout／2次精確origin pool reset、1次孤立502、1個robots-origin標頭不可達。
pool reset與diagnostic write沒有錯誤；該origin隔離，其他host／provider繼續。
[故障後正文核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v92/forward_transport_recoveries.json)
證明2個transport樣本有1筆真正新capture；6624617仍只有08:39:43舊正文，不能當14:42故障後已恢復。
1693999於22:32:23已由正常source23 owner分類quote-only，沒有合成假新聞或繼續body_missing重試。

[128批耐久驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v92/recent_batches_final.json)
核對3,847正文版本，errors0；只證明近期提交批次，不是全歷史coverage。
[四寬度唯讀頁面](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v92/dashboard_current/acceptance.json)
1440／768／390／320都能看934來源與39頁，沒有publisher requests／JS errors／failed resources。
[全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v92/inventory_final.json)
仍63設定／21啟用／871未註冊；其他授權、作者、正文缺口與未接入來源尚未完成，原全新聞目標持續active。

22:48的[後續forward資料核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v92/forward_transport_recoveries_later.json)
已2/2有真實故障後的新capture、raw／body／作者／clock／收據與current source replay；
6624617不再只有舊正文。初次1/2證據保留，這僅證明同兩筆樣本，不代表最新所有傳輸故障或全部歷史完成。
22:49的同owner仍在寫入正文，累計鉅亨198／Global Voices38個新URL heads，程序重啟次數0；
沒有以active或累計heads取代近期收據／源站完整性證明。22:49時4470101仍保留23:23:05的正式重試期限。

## 正式正常重試與新來源完整診斷範圍

2026-10-11，Asia/Taipei；續原全934來源目標，不是另一個新聞collector。
中央社依使用者「先不要管中央社」暫緩，保留清冊，不再查API、修改憑證或啟動該來源。
本輪沒有修改正式下載core、provider授權、source catalog、重試期限或既有私人原件。

4470101於**23:23:06.792920**由既有source24 owner按23:23:05.192015期限正式取得。
[所選正文／原件／作者／clock／批次證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v92/formal_bare_figure_recovery.json)
接受341字、作者張欽發、來源發布`2020-05-01T02:59:35.000Z`、修改時間、實際raw SHA及current parser重播；
batch proof核對其所屬42版本與本次current head。這是新HTTP capture，不是diagnostic原件改時間。
[33項嚴格正式交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v92/strict_formal_handoff.json)
全部接受；原先32項engineering-only交接和未完成狀態證據保留，不降低正式資料門檻。

[21啟用來源傳輸證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v93/current_transport.json)
接受同owner的55筆鉅亨失敗（53次標頭前、2次讀body中）、55次精確origin pool reset，
reset與diagnostic write錯誤0；另14次孤立gateway decision、7次robots-origin decision。
這些計數不是55篇去重新聞或全部網站健康判定。
[真正故障後正文](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v93/forward_recoveries.json)
49/55有新capture／raw／正文／作者／clock／收據／current replay；其餘6個樣本尚未接受正文，
當次current head為body_not_found。不得把舊正文、pool reset或HTTP200當新正文完成。
[近期128批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v93/recent_batches.json)
核對4,209版本、errors0；不是全lake掃描或歷史完整性證明。

富果原先只有`unregistered:blog.fugle.tw`清冊，不能把既有行情API、其他publisher授權或三篇成功HTTP當作正文下載資格。
[全部161已知網址JSON](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v93/fugle_full_known_worklist.json)
及[CSV](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v93/fugle_full_known_worklist_idhex.csv)
保留133個法說會slug候選與28個其他研究；slug只作排序hint，並不證明全部可用／原創／全站歷史。
SQLite BLOB article ID以lossless hex輸出；既有metadata原日期／sorting day與正文發布clock分開，不回填成真實發文時間。

[第一輪三網址](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v93/current_fugle_structure.json)
和[第二輪完成串流／門檻／clock結構](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v93/fugle_completed_render_projection.json)
共六次有界文章GET；只存metadata與DOM結構，不保存原始新聞頁、完整正文、隱藏Flight payload或登入cookie。
實查robots允許公開頁，禁止`/api/`及`/search`；不呼叫這些端點，也沒有造數值官方配額。
三個HTTP200頁面的`main article`是0字loading shell，真正current article在新版React完成串流的`S:0`。
845字completion instruction的SHA在三頁一致；仍需精確B:0/S:0、完整boundary、唯一性與可見性證據，不能揭露任意hidden節點。
兩頁有會員預覽／欄位登入門檻，一頁法說會備忘錄未見該門檻；158頁尚未逐頁實查，**0正式全文**。
三頁OG published欄位都等於modified，但文章header另有原發布日期／literal datetime。
發布顯示是date-only，部分literal為UTC午夜；不得當成已驗證的intraday發布秒數或前開盤可用性。

讀的是blog footer真正連結的[服務條款](https://www.fugle.tw/service)，不是無關的富果線上學院條款。
[公開條款實際readback](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v93/fugle_public_service_review_assets.json)
記錄一般智慧財產權／複製限制；沒有宣稱找到明文scraping條款、已取得bulk原文grant或作合理使用法律判決。
第一次只允許同origin資源讓頁面空白，另一次wait_for_function碰CSP拒絕；保留失敗run與readback，
修正為原生locator讀取，只開頁面實際引用的單一hashed desktop JS/CSS CDN，不禁用CSP、不讀登入／captcha／tracking／市場XHR。
已讀到真正5067字公開政策；這是政策診斷，不是新聞原文授權。Fugle沒有加入正式producer。

16項metadata／routes／worklist／source-view測試通過。
[17項範圍與證據交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v93/diagnostic_handoff_corrected.json)
接受上述診斷與既有source24正式補回；**不接受Fugle adapter／正文grant，也不接受全新聞目標完成**。
初次16/17的[未接受交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v93/diagnostic_handoff.json)
保留：SourcePolicy tuple與JSON array直接比較造成假差異；改為lossless JSON view，測試確認改domain／allowed仍拒絕。
建構子順序錯誤及BLOB JSON輸出錯誤亦保留原失敗run；均為本輪診斷程式問題，不冒充production故障或刪除證據。

最後以正常owner已保留的4733427?exp=a原件，
[零新增HTTP定位下一個鉅亨缺口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v93/next_cnyes_retained_native_gap.json)：
一般排除figure後current／visible compact均132字，但schema compact478字；current另有巢狀figure內373字與文字-only子figure，
既有完整figure witness拒絕該結構。下一階段須逐圖／文字／順序／可見性／數字及全schema驗證，不直接放寬或標已修復。
原全934來源目標持續active；中央社暫緩，其餘下載及已核准增量正常繼續。

23:36的[持續運行13項驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v93/runtime_current_fresh.json)
全部接受，同owner／原code／scope／queue／status／dispatch clock一致。
3,027.180045秒DB窗口新增鉅亨2,848正文URL heads（0.940810/s）、Global Voices265（0.087540/s）；
獨立HTTP counter窗口鉅亨1.435844req/s。這是當窗觀測，不是去alias唯一篇數、matched WAN倍速或官方上限。
[更新的近期128批收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v93/recent_batches_fresh.json)
核對4,680正文版本、errors0，仍非全lake／全歷史。
初次[runtime未接受結果](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v93/runtime_current.json)
12/13：正文批次快照超過既有300秒新鮮度門檻；程序、owner、policy及獨立正文進度並未失敗。
重新核對最新批次才接受，原失敗report保留，不改時鐘、清deadline或放寬驗收條件。

## 混合巢狀圖框的完整順序與正常回補

2026-10-12，Asia/Taipei，v94。中央社仍依使用者指示暫緩；不擴大授權、不另開producer。
本階段只改共用鉅亨HTML解析分支與capture contract24→25；未更動其他62個來源、bulk／routing、
未知query、finite cohorts、診斷預算、原件、既有正文或原retry deadline。

根因由既有正常owner的4733427?exp=a失敗原件建立，沒有為診斷增加HTTP。
原件187,579bytes，來源capture `2026-10-11T15:00:02.131774+00:00`，
raw SHA `e5e5fdc90e103a93fed80347470d265c69927445db0711fa431d2f1422262fcd`。
free current-ID／visible／canonical／schema及aware clock皆已核對；private diagnostic仍不算正式body。
正文預設排除figure後current與visible compact132字；完整schema compact478字。
真正混合圖框另有373個含換行字元、346個compact字元，並非缺網路回應或可任意捨棄的空圖。

保留的來源結構為leading image、兩段正文、image-only子圖、再兩段正文、image／雙span CTA子圖、
社群link／精確三行warning文字-only子圖及空尾段。既有規則只接受所有子圖為image-only、
正文全在它們前面、warning位於root末尾；因此拒絕這種真實混排。
新的typed分支僅於原image-only檢查不匹配時才執行，舊健康流程沒有多一次DOM重解析。
圖框數量、深度、paragraph類型、presentation style、CTA與warning、HTTP(S)連結／圖片、
唯一性、所有節點可見性及整份schema仍逐項核對；unknown／hidden／comments保持缺口。

current與visible的整棵parsed DOM實際完全相同，雜湊
`388ec05537bec464da50a2bb4f445bbe17dc8ff0d5a0f71e0de09dc37d70fb14`。
僅比較文字、image-order與link-order，無法發現把一張圖移到另一段之後的變化；
新comparison key另含完整ordered DOM SHA，以線性序列化／雜湊補足圖／段位置與屬性證據。
雜湊不是可見性或權限證明，這兩項仍由原canonical helpers獨立檢查。
不改DOM、不抓圖片、不借schema-only文字，原signed values與literal角括號文字原樣保存。

結果沿用現有分量ABI：147字正文body與373字完整圖框分開保存，
後者為`same_article_nested_mixed_figure_text`，保留四個image sources、一個source link與全部原文；
**它包含財經敘述，不是只有risk warning，也不冒稱圖說或作者**。
`body_completeness`明示圖框文字存於`verified_article_notices_json`，
使用者／下游不能把147字body欄位誤讀為所有文字已合併。
原作者`Anne陳巧諭`、發布及修改`2021-10-01T09:52:44.000Z`保持不變。

290項focused及4,395項完整news含bulk回歸通過。
新增測試涵蓋兩個HTML parser、mobile／實際desktop query、完整stream、數字與正負號、
段／圖移位、連結／圖片更換、重複、隱藏、未知層次、warning／CTA變化與完整schema。
同樣改current／visible／schema也不能讓unknown／hidden模板成為授權正文；
移圖後文字／圖片清單不變的負例亦被完整forest witness拒絕。
[原件diagnostic完整核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v94/actual_original_replay_accepted.json)
與[相同20份原件整鏈ABBA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v94/original_replay_abba.json)
接受14個舊健康控制與5個真實未解控制，source24每phase14個耐久版本，source25為15個，共58個；
每phase22個owned HTTP（20原件、2 robots fixtures），沒有publisher HTTP或images。
流程保留真正TCP／parser／source envelope／fsync／close／DB重建／逐版本與current head收據檢查。
wall中位10.694035→10.699099秒、CPU1.131203→1.046387秒；保留所有原samples，範圍重疊，
不宣稱WAN加速、官方上限或以新增一篇推估全網站成功率。

[候選交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v94/candidate.json)
接受後才停止news timer、gracefully drain owner、確認PID0並啟動service／timer；exit trap保留恢復責任。
00:00:39新PID4186963，fresh queue／status owner
`d8190d3ff6134c6a9610ff184df56459`，Cnyes HTML25 fingerprint
`49d5770f0091291d23156c8d19c649bdeae4d80331f73247e7f623151c0d0f29`。
[25項載入／範圍／deadline證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v94/deployed.json)
接受；既有原件與diagnostic reservations未清除，目標保留00:10:53.302629正常retry。
當時「成功replay」仍不等於新HTTP capture；正式資料交接須另附實際forward正文收據。

[全934來源快照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v94/inventory_final.json)
與[本機四寬度頁面](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v94/dashboard_current/acceptance.json)
接受owner／status一致、63設定／21啟用／871未註冊，四個viewport各934來源／39頁，
0 publisher requests／JS errors／failed resources。這不是外網驗證、全部871已接線或全歷史完成。
[零新HTTP的後續四原件缺口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v94/next_retained_gaps.json)
保留1462347、4795333?exp=a、1089439、4902745?exp=a當前失敗。
其中4795333文字在多圖／多caption森林而一般visible body空；4902745另有image／p和image-anchor圖框。
這些尚未實作／驗收，不能拿本輪mixed模板修復通過當全部figure已支援。

### 正式到期回補與34項交接

4733427在原00:10:53.302629期限後，於**00:11:17.081381**由正常owner實際取得，
`bodies/20261011/20261011T161130903612Z-1bb99797.parquet`耐久保存。
body SHA `5c1218dc22f4f0efd6ec4f9e76dd3668485282e023e47797a56fdd32dad45b2f`與私人原件replay相同；
新capture timestamp是這次HTTP觀測，不把15:00Z diagnostic改成正式capture。
[正式所選正文核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v94/formal_mixed_figure_recovery.json)
接受147字body、原作者及發布／修改clock、全部notices與current parser／capture fingerprint25，
batch proof亦核對所屬54個正文版本、10,512,548bytes私人raw與本次current head。
正文147字不包含另存的373字圖框全文；不以body單欄位宣稱所有文字已flatten。
[嚴格正式handoff](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v94/strict_formal_handoff.json)
**34/34**接受，正式body證據為必要條件；source權限／scope、原證據／reservations、deadline、
loaded owner、兩個test runs、完整ABBA、21來源transport及934頁面均核對，沒有降低guard。

[持續runtime13項](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v94/runtime_final.json)
全部接受；670.493380秒DB窗口新增鉅亨497／Global Voices39個正文URL heads，
鉅亨0.741245head/s。另一658.317819秒HTTP counter有鉅亨763次start，即1.159015req/s。
URL head並非跨alias去重的獨立篇數，HTTP rate與正文rate有不同分母，不能混用預估ETA。
[近期128批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v94/recent_batches_final.json)
核對4,533正文版本、827,382,432bytes私人raw、errors0，並非全lake驗證／全部歷史。

[全21啟用來源傳輸](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v94/transport_final.json)
同owner接受19筆標頭前timeout（鉅亨16／Global Voices3）、19次精確origin pool reset，
reset／transport diagnostic write errors0；鉅亨6次gateway及2次robots-origin decisions，
Global Voices2次gateway decisions亦留證。尚有502／503，不把isolated失敗計入成功正文或全站健康。
[真正forward恢復](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v94/forward_recoveries_final.json)
18/19具新capture／raw／作者／clock／收據／current replay；1924802仍transport_error，
有界重試burst耗盡後保留**01:05:19.138526**正常期限，未強制重排、刪deadline或宣稱19/19。
這19筆是所選新owner失敗事件，不代表全部時間的去重文章總數。

[本機與外網進度transport](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v94/progress_transport_readback.json)
分開記錄：本機API200／schema1／running／934keys；從這台主機連DDNS的4秒connect上限內逾時，
curl28／HTTP000，沒有取得外網HTTP。另一次15秒有界嘗試亦逾時，不能推導所有讀者都無法存取。
正式下載與本機四viewport持續正常，沒有因這個不同層的診斷重啟collector、改DNS或繞過TLS。
全934來源／全歷史目標仍active；下一階段以四份已留原件修多圖、image-anchor及短內容，
維持其他provider獨立下載與錯誤／真正forward recovery核對。中央社維持使用者指示的暫緩。

## 原始連結圖框與完整圖說的獨立資料型態

2026-10-12 v95；UTC觀察／捕獲與Asia/Taipei排程分開記錄。
本輪沿用user-working-preferences的取捨：修共用責任入口，保留原始文字與來源時鐘，
不減少來源範圍、不把liveness、私人原件重播、正式回補或全歷史完整性合為同一個完成狀態。
中央社依使用者指示暫緩：清冊保留，不查API、不改憑證、不啟動下載。

### 根因、資料型態與完整來源核對

[source25及兩份原件釘選](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/before.json)
在修改前核對v94嚴格交接、原診斷收據／raw SHA及當前公開來源scope。
[實際原件索引](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/actual_probes.json)
保留兩個來源捕獲的request URL、raw SHA和capture時鐘，零新HTTP／queue writes：

- `4902745?exp=a`有6個圖片root。第五個為image加plain paragraph／公開URL連結；
  舊規則僅接受p/em/strong來源署名或固定風險聲明，因此47字真實附文被拒絕。
  第六個則是textless anchor／image，不能只核對image src卻丟掉href。
  新typed grammar分別保留`same_article_image_figure_link_text`與
  `same_article_linked_image_reference`，完整DOM SHA另證圖片位置與屬性。
  原3,004字財經正文不改；47字附文與textless圖片連結另存notices，
  不冒充圖片caption、投資風險聲明或文章作者，不開啟表單／圖片。
- `4795333?exp=a`的文字全部位於一個完整gallery內，含outer caption段落、
  image／figcaption葉節點及有界葉群；8張不重複圖片，8段原圖說有真實重複。
  舊narrative排除figure後為空，不代表來源沒有文字。新規則須逐圖匹配caption、
  全部node可見、完整current／visible DOM與schema相符，才保留**587字純圖說body**。
  8組caption／image依序另存，原重複不去重，alt不當正文、沒有下載pixels或OCR。
  `body_completeness`明列`verified_current_visible_caption_gallery_text`、
  `original_captions_in_source_order`及`image_pixels_not_downloaded_or_OCRed`，不冒充一般敘述。

兩個新grammar仍拒絕隱藏／未知markup、comment、credential／非法URL、缺圖／重複圖、
單邊文字／數值正負號／href／圖片順序變更及不完整schema。
普通健康文章保留既有fast path；不將schema-only正文、缺文或圖片-only舊控制放行。
只有鉅亨HTML capture25→26；quote-outcome3／image-outcome1、其他來源授權、bulk、
routing、protected queries、有限cohorts、原件與diagnostic reservations均保留。
訓練注入和跨主機正文發布沒有開啟，發布時鐘不改成這次capture時鐘。

### 同來源版本的耐久全鏈路比較

針對測試run `v95-typed-links-gallery-focused-20261011T164513-00db3d39`：**432 passed／3.70秒**；
全news含bulk `v95-all-news-source26-links-gall-20261011T164538-eb35684b`：
**4,524 passed／68.74秒**，其中129個新測試覆蓋兩種HTML parser、native／query variants、
完成串流、上述反例、nonmutating DOM witness與變更圖群位置的順序核對。
[共享私人原件驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/actual_original_replay_accepted.json)
接受兩份SHA、正文、原作者和秒級publication／modification clock；不是正式HTTP下載。

最初驗證工具漏帶probe snapshot的`observed_at_utc`，失敗run與原輸入保留，
另生成完整輸入，不改source publication或舊receipt。ABBA也先揭露URL map會覆寫gallery
另一時點原件，以及single-host fixture只有1次robots，不能當2次；兩份失敗比較收據保留。
修正的是比較方法，不是降低parser guard或更改production queue。

[最終全鏈ABBA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/original_replay_abba_source_versions_accepted.json)
run `v95-two-gallery-generations-full-20261011T165312-a3603627`：
原20份raw全保留，另加入當前兩份raw，其中gallery重複URL但raw不同，
因此為**22份來源捕獲／21網址**，不是22個不同網址。
每phase用21網址queue及舊gallery獨立queue，避免強制同URL越過due再抓第二個版本。
經canonical HTTP／parser／validation／Parquet／fsync／收據／reopen／全部版本讀回；
reference兩phase各15版本，candidate各18版本，合計**66耐久版本**。
每phase25個owned requests包含22份原件及三次相應robots，0 publisher／圖片／表單請求。
原15篇健康output signature相同；其餘4個負面控制仍失敗／原source outcome，
兩份gallery候選均為同587字、8caption原件，而非以新原件取代舊失敗。

整條所選鏈路wall median **11.845310→11.794941秒**，CPU median
**1.167283→1.030292 CPU秒**；wall範圍11.814–11.877對11.776–11.814有重疊。
新增三個耐久版本的資料可用性被驗證，沒有聲稱publisher下載倍速、官方頻率上限或全歷史完成。

### 正式交接、真實回補與持續觀察

[候選guard](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/candidate.json)
接受，`v95-safe-news-only-source26-hand-20261011T165433-5810f62b`只graceful
停止news timer／service、確認PID0、恢復兩者，EXIT trap亦確保恢復。
新owner `c9794b1aab7b4aafa90e78e6b5f2b026`於**00:54:37**啟動，
[載入／期限guard](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/deployed.json)
**27/27**接受；舊原件、reservation、scope與原期限保留。

[4795333正式正常回補](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/formal_gallery_only_recovery.json)
接受**00:58:30.472139**實際capture、587字body SHA
`8f1d43893df1c7b798fb4931334c8e1b4f29cd03c03422e265e6f93d8385f220`，
8caption對應、原陳蕙綾署名及2022-01-01T07:15:26Z發布／修改秒數均保留。
所屬`20261011T165837871719Z-3872aa09`批次64版本／11,497,053bytes私人raw、
current head與當前HTML26 parser／fingerprint全核對，不是把舊diagnostic改成正式capture。
4902745此時仍沒有body，原**01:38:22.972714**有界重試burst後期限沒有強制清除。

[13項runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/runtime_final.json)
與[35項工程handoff](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/engineering_handoff.json)
接受同owner／current loaded core、實際進度、原scope／deadlines及資料／頁面證據。
本次工程handoff明確沒有將待回補第二篇當正式成功；預設嚴格formal guard另外要求
兩個確切target都出現在正式收據且current row／capture fingerprint核對。
291.497665秒DB窗口新增鉅亨374／Global Voices25個正文URL heads，
鉅亨**1.283029head/s**；獨立292.225651秒HTTP counter有574次鉅亨start，
**1.964236req/s**。不是跨alias去重篇數；HTTP rate與正文rate不同，不混用ETA分母。

[128近期批次](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/recent_batches_final.json)
核對3,760正文版本／686,391,594bytes私人raw／errors0，不等於整個lake或全部歷史。
[934來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/inventory_final.json)
同owner一致，63設定／21啟用／871未註冊；快照鉅亨known3,595,044／stored3,551,517，
`missing_body_work`36,752仍是待補工作，不把4,613空template、316quote-only或1image-only當正文。
最早2009-06-17是metadata日期，不是全部文章已獲得的最早body日期。
[四寬度頁面](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/dashboard_current/acceptance.json)
1440／768／390／320各934來源、39頁、0 publisher request／JS error／failed resource。
[本機／WAN讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/progress_transport_readback.json)
本機200／schema1／934keys；外網從本機4秒connect逾時（curl28／HTTP000），
未取得WAN HTTP，不能推導所有使用者都無法存取，也不因此重啟健康collector。

[21啟用來源transport](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/transport_final.json)
本窗沒有新的article transport失敗事件，但有amp-news.cnyes.com robots 502；
該origin不授予permission，其他已核對來源仍獨立進行，不繞過robots或用另一host規則替代。
[早期forward recovery取樣](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/forward_recoveries_observed.json)
0事件不代表故障恢復驗收完成，亦不拿舊owner恢復冒充這個owner的證據。

`v95-normal-deadline-formal-captu-20261011T165818-dcd46379`啟動
[有界唯讀觀察者](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/observe_formal_recovery.py)：
20秒只查兩個索引列，row變更或最多每分鐘留一次心跳，90分鐘觀察上限。
不發HTTP、不取得HTTP owner／queue lease、不更動due、重試次數、source clocks或fingerprints。
[待正式捕獲狀態](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/formal_linked_gallery_recovery.pending.json)
在兩篇真正complete時自動呼叫共享selected-article驗證；來源程式／loaded owner改變即拒絕混用。
這是有界觀察，不是另一條下載管線；90分鐘到期未補回就留未完成證據，不製造正文或成功。
雙target正式收據尚未成立，後續須再核對fresh same-owner runtime／128批與嚴格formal guard。
全934來源／全歷史目標仍active；其他未知模板、短內容、來源權限與Fugle清冊缺口持續保留。

### 後續持續故障核對，不以早期零事件宣稱全部健康

上述初始transport快照之後，[同owner最新21來源樣本](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/transport_latest.json)
已觀測24筆鉅亨標頭前傳輸失敗、24次精確origin pool reset，reset／diagnostic write errors0，
另5次gateway及3次robots-origin decisions；source失敗沒有藏掉，也不是全部來源皆健康。
[真實forward正文恢復](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/forward_recoveries_latest.json)
**22/24**有故障後的新capture／raw／正文／作者／clock／耐久收據及current replay。
1600512與m.cnyes.com/4778922已不再是transport錯誤，但正文仍`body_not_found`，
因此沒有宣稱24/24資料恢復。舊owner的1924802在01:05正常到期後也仍是正文缺口，
下一正常期限由來源實際失敗／queue產生，不是人工清零。

[最新runtime13項](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/runtime_latest.json)
接受；832.270177秒DB窗口新增鉅亨942／Global Voices79個正文URL heads，
鉅亨1.131844head/s；獨立829.694651秒HTTP counter有1,392次鉅亨start。
[最新128批次](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/recent_batches_latest.json)
核對4,142正文版本／756,717,151bytes私人raw／errors0。
這些是另一時點的持續觀察，沒有覆寫原工程handoff或未完成雙target正式驗收。
4902745仍保留01:38:22正常期限，有界唯讀觀察者仍running；沒有因錯誤樣本就停掉其他來源。

## 單行聲明、來源無正文與診斷owner分界

2026-10-12，v96；沿用使用者工作偏好技能的共用根因、完整原件與分層驗收流程。
範圍仍是所有934來源鍵，不是縮成鉅亨幾篇；中央社依本輪指示暫緩。
沒有新增API金鑰、授權、來源、獨立下載管線、原文外傳或資料清理；既有dirty work保留。

### 先完成source26正式驗收，再升版

原觀察者 `v95-normal-deadline-formal-captu-20261011T165818-dcd46379`
未重啟、未修改source26 core／owner／重試期限；01:38:24.652042正常抓到4902745?exp=a。
[兩篇正式正文](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/formal_linked_gallery_recovery.json)
核對3,004字／47字附文與587字／8圖說、作者、原發布／修改時鐘、raw、102個所屬批次版本及current parser。
4795333仍是00:58:30.472139原capture，不回填新的時間或fingerprint。
[嚴格36項](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/strict_formal_handoff.json)
和[更新9項收尾](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/strict_formal_completion.json)
均接受，才允許隔離候選升至canonical core；前輪35項工程收據沒有被改名成正式證明。

[source26更新runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/strict_runtime.json)
13項通過；2,584.716399秒DB窗口新增鉅亨3,459正文URL heads，1.338251head/s；
另一2,580.181959秒HTTP窗口4,674 starts，非同一分母。
[128批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/strict_recent_batches.json)
5,017版本／909,927,740 bytes私人raw／errors0。
[21來源傳輸](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/strict_transport.json)
49次鉅亨失敗（48標頭前、1讀body）、49精確origin pool reset，reset/write errors0；
[實際forward正文恢復](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v95/strict_forward_recoveries.json)
45/49，另4筆仍正文缺口，並非全部修好或所有host皆健康。
四寬度934來源／39頁仍接受；沒有用HTTP 200或服務active冒充資料完整。
第一次fresh收尾helper誤用batch的owner/clock欄位而KeyError；保留failed run，
修成真正的`owner_id`／`verified_at_utc`後才接受，沒有改寫任何舊驗證時間。

### 三份原件、三種不同結果

[隔離準備](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/prepare_candidate.py)
只讀已留存、免費同ID原件並綁SHA／receipt；source26正式observer仍運行時不碰production。
[原件清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/actual_probes.json)
保存其真正來源捕獲時間；先前engineering acceptance與稍後strict formal acceptance明確分開。

- **5125945?exp=a：可修的parser gap。** Visible/current各1,474字，schema還有同篇圖框文字。
  單一figure的結構為`img,p,p,p.warning`；前兩段是兩個既有prefix及原連結，最後一段是精確的單行風險聲明。
  舊grammar只識別三行聲明，並非缺少原文。新版要求整個visible/current DOM、圖、連結、順序、可見性及完整schema相符。
  不捨棄附文、不只比warning、不從schema補造正文；1,474字body SHA
  `4d8a1df0874209a951515bec3718cd2ba53cfbc3e7c5818ceb31e8c0258c65d5`，
  125字附文以`same_article_image_figure_one_line_warning_supplement`另存；兩連結與圖片參照保留但不跟訪。
  作者Anne陳巧諭，發布／修改`2023-03-24T09:10:08.000Z`不改。
- **1462347：來源只有代號。** 三個完整表示都是5字的股票代號，沒有價格或新聞敘述。
  只在免費、同ID、完整visible/current/schema／clock與無影片證明成立時分類`source_body_quote_only`。
  此family涵蓋symbol/price stub，不聲稱有價格，更不是保存了一篇新聞；真正的短中文敘述仍保留為正文。
- **1089439：來源空白版型。** Visible/schema沒文字，current是精確的NBSP段落＋div＋NBSP段落。
  只識別該確切entity/decoded結構；任意空HTML、註解、hidden、script、圖片、未知屬性仍不放行。
  結果為`source_article_empty_template`，沒有虛構歷史正文。HTML契約27、empty outcome6、quote outcome4；
  其他來源、authorization、bulk/checkpoint、routing與來源原時鐘不改。

[shared原件驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/actual_original_replay.json)
一份可解析正文、兩份可驗證來源無正文；不是三篇下載完成。
第一輪隔離tests有6個失敗：部分synthetic預期太廣／錯把合法短中文敘述當缺文，且漏查stub來源影片。
補上影片核對及精確NBSP解碼，不放寬任意HTML；113新tests全通過。
第一次canonical全news另有兩項仍assert quote契約3；更新為4並保留bulk/authorization不變的核對，
不是移除負面控制。549focused／4,637全news接受；新增診斷owner修復後最終4,640全news接受。
所有失敗run保留，部署前沒有以失敗測試作成功證據。

### 同原件整鏈與正式排程是不同的驗收

[隔離ABBA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/isolated_original_replay_abba.json)
相同24個不同捕獲／23網址（兩個gallery時點都保留），各phase27個owned fixture requests，74個耐久版本。
17個原健康控制及舊gallery全文／作者／clock／附文輸出不變，其他3個負面控制不變；
候選多1個可用body，不把兩個無正文結果算body。原tcp→fetch→parse→queue→Parquet/fsync→receipt→回讀全鏈核對。
舊／新wall median 12.777259／12.756681秒，CPU 1.105451／1.017019秒，範圍重疊；
沒有WAN倍速、官方頻率或理論最大值證明。
[promoted core綁定](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/original_replay_abba.json)
明確是已驗證isolated報告和精確canonical SHA的後續綁定，不謊稱又測一次或正式下載。

[graceful news-only部署](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/deploy_source27.sh)
候選13項先過，停止news timer、等待service MainPID0後切換，trap恢復news service/timer；
沒有重啟FinLab／FinMind／TEJ／交易或其他collector。
01:46:01新PID211125／owner `00e78cb29e9445b3bbee2eebca1cd49f`，
[30項loaded與期限核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/deployed.json)
及[38項工程交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/engineering_handoff.json)
接受；這兩個收據不代表三個新target已正式到期驗證。

[有界唯讀observer](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/observe_formal_recovery.py)
run `v96-normal-forward-body-and-abse-20261011T174901-6bbd09fb`，每20秒查3個索引列，最多每分鐘留心跳，90分鐘上限。
5125945?exp=a正常**02:18:52.260370**、1462347正常**02:24:23.619586**、1089439正常**02:26:04.344759**。
[pending狀態](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/formal_recovery.pending.json)
等1篇實際body＋2個post-owner來源無正文狀態，再呼叫共用selected article與queue-outcome驗證。
0HTTP／0queue writes／不改due、attempts、lease、來源時鐘或fingerprints。
core/owner改變即拒絕混用；來源無正文只算classification，不製造2個body，
也不聲稱latest無正文raw已保留（較早私人原件與最新queue observation分開）。

### 診斷不能另開owner、不能憑錯誤碼猜原件

1600512／m.cnyes.com/4778922／1924802尚**沒有**留存原件，已更正舊任務next_action的「已有原件」誤述。
獨立`inspect_cnyes_news --canonical-fetch`初始化被正常的exclusive HTTP owner拒絕，0 publisher requests；
不是網站封鎖，也不為取樣停止健康collector、關閉exclusive flag或提前重排。
[scripts/inspect_cnyes_news.py](../scripts/inspect_cnyes_news.py)現在對該特定busy結果保存
`deferred_active_canonical_http_owner`，原URL、既有快照及0請求／0queue mutation明確保留；其他初始化錯誤仍拋出。
[實際deferred證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/missing_original_probes_owner_deferred.json)
不冒充原件取回；等正常owner與原有診斷配額保留證據再定位版型／正文根因。
新增3項owner/no-mutation/other-error tests；與既有診斷／probe selection共67項通過。

[新owner runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/runtime_final.json)
13項接受，273.057127秒DB窗口新增鉅亨227／Global Voices28正文heads，鉅亨0.831328head/s；
獨立277.731481秒HTTP counter有367次鉅亨start。含啟動與故障窗口，不能與前owner的不同cohort直接宣稱提升倍數。
[128批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/recent_batches_final.json)
5,018版本／909,220,649bytes raw／errors0。
[21來源transport](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/transport_final.json)
抽樣2次鉅亨失敗、2次origin pool reset；
[forward正文收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/forward_recoveries_final.json)
2/2核對新raw／body／作者／clock／收據及current replay，不是僅重連成功。
[四寬度頁面](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/dashboard_current/acceptance.json)
各934鍵／39頁／0 publisher request／JS error／resource error。
[全清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/inventory_final.json)
63 configured／21 enabled／871未註冊來源鍵，2,130,591 URLs待授權。
鉅亨3,595,046已知URL／3,555,455有保存body／32,816尚缺正文工作；不是所有不同文章、年份或來源都已抓齊。
總目標仍active，中央社維持暫緩，原件待取樣與其他來源缺口繼續保留。

**01:57 Taipei**的持續核對（不混用工具執行時間與來源clock）：
[更新runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/runtime_handoff.json)
13項接受，621.765031秒DB窗口新增鉅亨577／Global Voices64正文heads，鉅亨0.928003head/s；
809個HTTP starts是另一個HTTP counter窗口，不當成文章量。
[同128批再次回讀](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/recent_batches_handoff.json)
重新核對5,018個原immutable版本，0 errors，未把較早WAL generation改稱現在；
[更新8筆故障](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/forward_recoveries_handoff.json)
8/8有當前owner故障後的實際正文、raw、作者、clock、耐久收據與current replay。
news PID211125仍active／NRestarts0，observer child217676仍live，480秒心跳確認三target仍等原期限。
這是8筆抽樣的恢復，不撤回先前source26的4個未解樣本，也不宣稱全來源已修好或達到官方上限。

## 投信清單、空段落與上一版正式驗收

2026-10-12，Taipei，v97。中央社依使用者最新指示繼續暫緩：不查 API、不改 key、不啟用、不跟進轉載原文。
遵循共用管線、完整原件與分層驗收的工作偏好；不另開下載程序搶 HTTP owner，也不把已到期當成已下載。

### 先封存 source27 真正的正常回補

[原有 observer](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/observe_formal_recovery.py)
`v96-normal-forward-body-and-abse-20261011T174901-6bbd09fb` 已 succeeded，不重開。
它在 core／owner 全程不變、無 HTTP／queue mutation 的條件下等正常派送：

| 原網址 | 正常處理時間（Taipei） | 實際結果 |
| --- | --- | --- |
| 5125945?exp=a | 02:19:11.725148 | 1,474 字正文、125 字獨立附文及原連結，完整 raw／作者／clock／receipt／current replay 接受 |
| 1462347 | 02:27:44.504227 | source_body_quote_only，0 body；只是符號 stub，不是新聞全文 |
| 1089439 | 02:26:06.957429 | source_article_empty_template，0 body；精確來源空版型 |

[正式正文證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/formal_warning_recovery.json)
與[兩筆非正文狀態](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/formal_nonbody_outcomes.json)
分開。最新 absence raw 未另留存，不拿較早原件冒充最新 raw，亦不製造兩篇假文章。
1462347 晚於最早重試時間，是較早到期 retry 的公平派送順序；無手動改 due／priority 或額度。
[39 項嚴格正式交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/strict_formal_handoff.json)
及[11 項 fresh completion](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/strict_formal_completion.json)
於 02:28:46 接受。保留較早工程快照的時間，不覆寫成現在。

[source27 最新 runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/strict_runtime.json)
13 項接受：1,653.888959 秒 DB 窗口新增鉅亨 1,616／Global Voices 111 個正文 URL heads，
鉅亨 0.977091 head/s；另外 1,655.7143 秒 HTTP counter 窗口有 2,379 次 start。
[128 最新批次](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/strict_recent_batches.json)
5,046 個 immutable 正文版本，0 errors，不是 5,046 篇去重新聞。
[21 來源傳輸稽核](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/strict_transport.json)
與[forward 正文恢復](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v96/strict_forward_recoveries.json)
核對 42 個暫時故障事件，40 個有真正後續正文／raw／作者／clock／receipt／current replay；
1613451、4804047 仍未恢復，不因這個抽樣接受就把來源錯誤全清零。

### 用既有原件修真正的新解析根因

[隔離準備與精確原件清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/actual_probes.json)
使用 canonical owner 正常保留的兩份公開回應，0 新 HTTP、0 queue mutation。
source27 正式 observer 跑完之前只修改 candidate；之後才升級共用 decoder。

- **4564769?exp=a**：visible／current 都有 281 字，schema 只省略空白與一個股票清單分隔 U+FEFF；
  同段落的 `投信累計買超`／`投信當日買超` 被原本外資-only heading 拒絕。
  [共用 helper](../downloader/news_fulltext_publishers.py) 延伸至已實際觀察的「投信」heading；
  仍要求唯一 current 段落、兩側 native 股票連結／標籤、完整 visible/current 及 schema 核對。
  比較副本可忽略那個經證實的格式字元，**保存正文仍保留原 U+FEFF、全部數字和順序**。
  281 字正文 SHA `903fc47abde383ca363fe067a5aee2a3e2c70ef0ecae882ef343f27dd2474942`，
  作者鉅亨網新聞中心，發布／修改皆 `2021-02-02T00:43:39.000Z`，不改成抓取時間。
- **3716992**：current 精確宣告編碼的 `<p></p>`，visible 空白、schema 無正文、video 空字串。
  只新增這個精確模板，且仍核對 free／same ID／title／aware clock／無媒體。
  不接受任意 hidden、comment、attributes、空白變體、script 或有影片頁面；不捏造全文。

HTML capture contract **27→28**，empty outcome **6→7**；quote4／image1／short5 不變。
其他 62 來源 fingerprint、授權、有限 cohort、supplemental 範圍、原始 query、bulk fingerprint、
routing／checkpoint、daily／hourly 診斷預留與原件均維持原樣。
[正式共用 parser 原件重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/actual_original_replay.json)
為 1 個可解析正文＋1 個來源 absence，仍不是正式下載收據。

[隔離候選語義測試](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/test_candidate.py)
103 項／1.70 秒接受；[維護測試](../test/test_news_cnyes_institution_list.py) 及既有 regression
545 項／5.12 秒、全 news 含 bulk **4,712 項／71.52 秒**接受。
保留首輪兩個失敗 run：`v97-promoted-maintained-institut-20261011T183003-bd40fb6f`、
`v97-current-core-all-news-regres-20261011T183004-1edf12cb`。
兩個失敗僅是舊測試仍期待精確空 `<p>` 回 `body_not_found`；依實際原件改成 source absence，
並增加 schema 有／無與 encoded／decoded 正例，未知空殼和微短英文仍失敗；不是略過驗證。
最終 receipts：`v97-current-source28-correct-emp-20261011T183200-46f3e2ff`、
`v97-source28-final-all-news-regr-20261011T183201-008cd268`。

[同原件完整 ABBA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/isolated_original_replay_abba.json)
共 26 distinct captures／25 URLs，包括 gallery 的兩個原始時點。
四輪 canonical fetch→validate→fsync→重開 queue→全版本 raw／body／clock／receipt reconstruction，
合計 **78 durable versions**；18 個既有健康控制＋舊 gallery 及其他 5 個負面結果均保持。
整鏈 wall median 13.760294→13.791613 秒，CPU median 1.088988→1.129167 秒；
沒有明顯單次倍速。候選多取得有效清單並停止空頁的無效重試，不以少驗證、不同 corpus 或相同 retry 數冒充速度提升。
0 publisher／image／linked-page requests；不證明官方上限、WAN 吞吐量或全部歷史完成。
[promotion binding](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/original_replay_abba.json)
僅綁定 replayed candidate 與 canonical SHA，不是第二次 benchmark，也不覆寫來源時間。

### 新 owner、頁面與正式待驗案例

[13 項切換前 guard](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/candidate.json)
接受後，只 graceful 重啟 news service／timer，MainPID=0 後才啟動。
**02:34:05** source28 owner `44d2664ddf3d4e49957318b2134f4151`、PID313759／NRestarts0；
[32 項 loaded/deadline guard](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/deployed.json)
與[40 項工程交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/engineering_handoff.json)
接受；不是正式 target 文章補回證明。其他下載器、憑證、授權與 API 流量參數未改。

[新 owner runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/runtime_final.json)
13 項接受：186.039233 秒 DB 窗口新增鉅亨 162／Global Voices 18 正文 URL heads，鉅亨 0.870784 head/s；
HTTP counter 183.437842 秒內 268 starts／1.460985 req/s。含啟動且 cohort 不同，不與前 owner 宣稱 matched 倍速。
[128 最新批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/recent_batches_final.json)
4,853 immutable versions／0 errors；[21 來源傳輸](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/transport_final.json)
在這個新窗口尚未有失敗事件，不把零樣本叫全部恢復，也不撤銷 source27 的 2 個未解樣本。
[四寬度本機頁面](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/dashboard_current/acceptance.json)
1440／768／390／320 各934鍵／39頁，0 publisher request／JS error／resource error；這不是外網 DDNS 連通性驗證。

[目前全清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/inventory_final.json)
02:36:18 快照：63 configured／21 enabled／871 unregistered source keys；
6,202,519 known URL heads、3,675,851 stored bodies、2,130,591 URLs awaiting authorization。
鉅亨 3,595,046 known URL heads／3,558,356 stored bodies／29,910 missing-body work。
這是不同 URL 頭、版本與待作業範圍，不是去 alias 唯一篇數，也不把授權未知或未註冊來源隱藏。

3716992 已於 **02:34:42.021335** 正常分類來源空正文，未存成 fake body；
4564769?exp=a 原期限仍是 **03:08:17.295019 起可派送**，尚未正式補回。
[有界唯讀 observer](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/observe_formal_recovery.py)
run `v97-normal-forward-institution-b-20261011T183451-5b64d85f`；child315312，
每20秒讀兩個 exact URL 索引列，最多每分鐘心跳，90分鐘上限，0 HTTP／queue write。
[pending 狀態](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/formal_recovery.pending.json)
仍明確 accepted=false；只要 core／owner 改變就拒絕混用，**在正式新正文核對前不要再切換 core**。
完成時須驗 raw／作者／clock／batch receipt／current source replay，不只看 state=complete。

### 其他來源仍保留根因證據，不冒充全部已修

[有界既有原件稽核](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/other_source_retained_diagnosis.json)
0 新 HTTP／queue mutation：兩份民視 raw／file SHA 完整，但 current parser 仍無可核對的 own-newsroom credit；
schema 作者不是可見原創署名，未藉此放寬轉載／工商內容範圍，也沒有跟進中央社。
Fed 四筆 `pdf_image_only_or_body_not_found` 只核對目前 queue 狀態，沒有重新取得四份 PDF 原件，
所以**尚不能斷言全是 scanned/image-only PDF**；後續需原件才能區分文字層／短文／OCR 問題。
CMoney 的實際存取拒絕、Fugle membership／metadata-only stream、未註冊來源、缺原件的鉅亨樣本
都繼續保留 backlog；不換身份、清空 cooldown、猜隱藏 endpoint 或把權限未知改成成功。
總目標 **所有934來源／全歷史仍 active**，這一輪不宣稱全來源完成或官方最高速。

## 政大來源審查與不中斷的實際下載觀察

2026-10-12 Asia/Taipei，沿用同一個 `news-adaptive-capacity-20261009` 任務。
中央社依使用者「先不要管中央社」持續暫緩，沒有查 API、改憑證、調整其權限或啟動下載。
本次只改獨立 planning ledger 的政大註記；source28 core、owner、配額、due與leases均不動。

### 官方來源審查是準備，不是已下載

[政大主站著作權頁](https://www.nccu.edu.tw/p/426-1000-30.php?Lang=zh-tw)
明列並連到 [CC BY-NC-ND 2.5 TW](https://creativecommons.org/licenses/by-nc-nd/2.5/tw/)；
不把其他子站、不同媒體或 SSU 大專學生運動網照片納入同一許可。
[唯一已知文章](https://www.nccu.edu.tw/p/406-1000-9212,r17.php?Lang=zh-tw)
在官方 web reader 有校訊記者署名與2021-03-20日粒度日期，沒有可證的時／分／時區或修訂時間。
不採用球賽時間作發布時鐘，不把日期補成午夜，也不以研究用途推定任意再散布或訓練許可。
[校園網路規範](https://www.nccu.edu.tw/p/426-1000-25.php?Lang=zh-tw)針對校園網路使用者，
宿舍頻寬／每日傳輸量不是外部新聞爬蟲的官方 request quota。

web reader 未讀到 robots，只代表該reader未完成；不是 canonical NewsHTTP 的拒絕、404或allow-all證據。
所以 [planning ledger](../configs/news/source_reviews_20261009.json) 記為 `review_incomplete`，
正式robots／response、精確article identity／container／署名／原始日期、耐久raw與收據仍待驗。
本機公開頁沒有讀入私人審查敘述／金鑰／原文，只有審查狀態與可驗證review時間；未啟用來源沒有假ETA。

[原始審查context](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v98/review_before.json)
與[12項審查-only驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v98/review_only_acceptance.json)
確認只新增NCCU註記、collector config revision／bytes與63個授權和capture fingerprints不變，
source28 core與同owner不變、精確NCCU queue列仍awaiting_authorization／無正文／無lease，
934個來源鍵完整保留；0 publisher request、0 queue write、0 accepted article。
第一次誤用非executable腳本、接著缺module import root的launcher失敗均保留；
以canonical runtime的 `-m artifacts.data_quality.news_goal_stage01_20261009.remaining_v98.capture_review_context`
成功，不用chmod、硬編碼Python路徑或掩蓋失敗。
第一次test路徑不存在也保留；改用實際 maintained source-review／catalog／original-inventory 三份測試，
run `v98-valid-maintained-review-test-20261011T185431-0f615ca5`：**70 passed／2.04秒**。

[全934來源計畫JSON](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v98/all934_source_plan.json)
與[完整CSV](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v98/all934_source_plan.csv)
依固定02:36清冊與當前config／review SHA重新生成；934鍵均有下一步，但887鍵尚無個別審查。
6 ongoing／15 finite既有pipeline、667小群候選、38長歷史候選、191無本地queue URL、
11轉址／聚合、4社交／影片、1已關閉來源與1已授權API待接線，後者中央社仍依user指示不執行。
排序是工作分工，不是預測alpha、許可或實際取得的完整性證明。

### 實際正文進度與未解故障分開驗證

[當前runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v98/runtime_current.json)
02:56觀察，同source28 owner，13/13 engineering checks接受。
DB窗口1,313.937887秒新增鉅亨1,392 heads／1.059411head/s、Global Voices130 heads；
HTTPcounter另為1,318.105959秒／鉅亨1.546917req/s。
不是去alias唯一文章、不把不同cohort拿來宣稱WAN倍速或理論最高速。
[128最新批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v98/recent_batches_current.json)
4,683個immutable正文版本／raw／receipt／current head核對，errors0。

[21來源傳輸證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v98/transport_current.json)
留15次鉅亨timeout：14次header前、1次HTTP200讀body時，約25.05–25.82秒；
15次failed-origin pool reset／0 reset error是恢復機制，不是15篇已取得。
[實際forward收據稽核](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v98/forward_recoveries_current.json)
02:57:35有12/15文章真正補回；4841220?exp=a及1507405仍body_not_found，1487274仍transport_error。
這份有界樣本接受，但 `all_sample_article_failures_recovered=false`；此前owner的未解樣本也不撤銷。

02:56–03:06時，source28正式observer繼續唯讀等待4564769?exp=a的原03:08:17.295019到期，不提前重排。
3716992已正常分類來源空P；只算1筆無正文狀態，不叫下載成功。
當時已準備 [嚴格正式closure guard](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/close_formal_stage.py)，
尚未執行或產生completion；必須實際forward正文與raw／署名／clock／批次／current replay接受、
刷新128批／13runtime／21transport／934清冊及四寬度本機頁面後才能通過。
所有934來源／全歷史目標仍active；不能由這份審查或bounded telemetry宣布全來源完成。

## 投信正常回補與自動正式驗收

2026-10-12 03:09:53 Asia/Taipei，沿用 source28 同 owner；中央社仍暫緩。
上述較早03:08待重試狀態已由實際正常回補與新收據取代，不覆寫較早私人診斷原件或版本。

[真正forward正文證明](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/formal_institution_list_recovery.json)：
4564769?exp=a於 **03:08:22.231435** 取得281字正文，body SHA
`903fc47abde383ca363fe067a5aee2a3e2c70ef0ecae882ef343f27dd2474942`；
raw／署名／來源發布及修訂時鐘／58個所屬immutable版本、精確current head和source28 replay全部接受。
先前回放不是這次capture，沒有把原diagnostic時間改成新的來源capture。
[空P結果](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/formal_empty_p_outcome.json)
只算3716992一筆post-owner來源空模板，0 accepted全文、無舊正文；最新absence raw未留存仍明列false，
不把較早raw驗證冒充最新source absence raw。

[受監督接續](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/supervised_formal_continuation.json)
run `v97-supervised-fresh-formal-cont-20261011T190602-be2b3312` 已succeeded／exit0，
以原observer成功作前提，自動完成4項並行readonly稽核、2項dependent稽核、formal handoff和closure，共8個步驟。
0額外publisher HTTP／queue writes／collector restart；使用canonical runtime與既有auditors，不另開collector。
任何command錯誤、core變更、observer失敗或有界等待超時，都留失敗結果而非重寫成功。
[41項正式handoff](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/strict_formal_handoff.json)
及[12項fresh closure](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/strict_formal_completion.json)
接受；core、63來源scope、授權、bulk／routing／unknown-query、diagnostic額度及原期限證據均保留。

[最新runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/strict_runtime.json)
03:09:47，13項接受：DB窗口2,113.433146秒新增鉅亨2,182 heads／1.032443head/s，Global Voices210；
HTTPcounter另為2,106.524807秒／3,125 starts／1.483486req/s，不拿兩種窗口或grain混算。
[128最新batch](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/strict_recent_batches.json)
4,726 immutable正文版本／errors0，不等同4,726個唯一URL或測試數。
[完整934鍵清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/strict_inventory.json)
03:09:04：63 configured／21 enabled／871 unregistered，6,202,520 known URL heads、3,678,089 stored bodies；
鉅亨3,595,046 known／3,560,399 stored／27,867 missing-body work。
比前一清冊少的缺口不一定都變成功正文，合法無正文分類亦須獨立保留。

[21來源transport](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/strict_transport.json)
與[實際恢復證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/strict_forward_recoveries.json)
接受；27個文章失敗樣本有23個真正forward正文收據，
4801463?exp=a、4841220?exp=a、1487274、1507405目前都仍body_not_found。
1487274已由先前transport_error進到收到response後的body_not_found，不能當作全文已恢復。
較早owner的1613451／4804047等未解證據仍保留，不由新的樣本窗口自動撤銷。
[四寬度本機網頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v97/dashboard_strict_formal/acceptance.json)
1440／768／390／320各934keys／39頁，0publisher requests／JavaScript errors／failed resources；
不是當前外網DDNS連線驗證。

這個stage完成的是1篇真實正常回補、1筆來源無正文與限定範圍的整鏈驗收，
不是所有934來源、所有歷史、所有故障或理論吞吐上限。
下一步才可解除本stage的core／owner凍結，先驗政大唯一已知URL的canonical robots與immutable raw，
再擴充共用adapter；其他未解正文／缺原件／來源拒絕仍在原任務繼續，不啟動中央社。

## 政大原生正文與診斷owner分工

2026-10-12，Asia/Taipei；中央社依使用者最新指示暫緩，不查API、金鑰、憑證、端點或轉載原文。
本階段根目錄為[`remaining_v99`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v99)。
原source28真實回補／正式closure與v98僅審查證據保留，不以新owner重寫舊收據。

### 來源、原件與根因

唯一既知URL為`https://www.nccu.edu.tw/p/406-1000-9212,r17.php?Lang=zh-tw`。
[政大主站聲明](https://www.nccu.edu.tw/p/426-1000-30.php?Lang=zh-tw)明列
[CC BY-NC-ND2.5TW](https://creativecommons.org/licenses/by-nc-nd/2.5/tw/)；
這不是子網域、第三方SSU照片或全部校園作品的概括授權。
校園宿舍5GB規則不是外部新聞HTTP配額，未找到可驗證的數值官方抓取上限。
只保存本篇私人非商業研究原文，保留標題／來源／記者與授權；不開放正文重刊、媒體下載或自動訓練注入。

原21啟用來源的registry沒有這個host；與全部63設定來源的domain也無重疊。
[`before.json`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v99/before.json)
先以唯讀驗證真實source28已完成、同owner與精確缺失列，再依既有Macau新host診斷模式，
用canonical`NewsHTTP`／exclusive lock／robots在單host私人診斷root取一篇，不停其他來源。
診斷於 **03:23:10** 結束並釋放owner：robots200明列`User-agent:*`／`Allow:/`，文章200無redirect；
原件48,156bytes，raw SHA`89320f4bc4e463e651e58e4dddd080d35cdded78f892cc9a8dff071bd6650cb2`。
[`probe.json`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v99/probe.json)
及private diagnostic／receipt保存精確request、final、授權、抓取時間與raw，不把HTTP200叫正文成功。

generic `[itemprop=articleBody]` 不存在，因此舊提取只報`body_not_found`。
真正原生容器是`.module-detail`內的`.mpgdetail > .meditor`；
其他同名editor位於navigation／footer，SSU兩個重複圖庫則在正文之外。
本頁沒有canonical／og:url／NewsArticle schema；不能要求憑空補出這些欄位。
新的[`news_fulltext_nccu.py`](../downloader/news_fulltext_nccu.py)
驗證精確既知URL、CMS9212雙圖庫ID與`a`+32hex統計ID、native heading對page title，
再核對本篇日期面板、秘書處單位與`【校訊記者林亭報導】`。
只保留六個非空原始text runs（署名＋五段正文）及全部數字／正負號；
不使用隱藏schema、圖說、頁尾作者、別篇日期或事件時間。
`2021-03-20`維持date-only，modified unknown；HTTP Last-Modified不是文章修訂時鐘。

[`original_replay.json`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v99/original_replay.json)
8項原件／語義／既有63指紋／共用耐久replay guard接受，正文 **525字**／SHA
`f2ba9f1ff05480e0e563abf125358739412ae7322e79438ce92d73fe79142959`。
三次isolated cached pipeline沿用原claim／capture／batch／fsync／receipt／readback實作，0publisher requests；
這是candidate耐久驗證，不是正式下載、WAN速度或source最大值。

### 正式排程與網站

只新增`nccu_visible_news_v1`／finite one-URL，原63capture／authorization fingerprints全部相同。
診斷owner已關閉才註冊進[`publisher_catalog.json`](../configs/news/publisher_catalog.json)。
既有配置變更流程於 **03:31:53** graceful排空，exit75要求reload；systemd **03:32:03**正常啟動新owner
`73b0eaf8f9184adaa5a87259fbbd9150`，不是手動清deadline或另開daemon。
正式cohort於 **03:32:16.999187**凍結一個既知URL，原collector於 **03:32:26.082032**正常取得525字。

[`formal_article.json`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v99/formal_article.json)
核對目前raw／body／作者／date-only／source fingerprint／current parser，並驗證所屬56個正文版本與精確current head。
[`activation_acceptance.json`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v99/activation_acceptance.json)
13項guard接受：原16個有限cohort、members與所有approved supplements不變，
沒有把diagnostic原件冒充正常抓取、沒改中央社review或任何金鑰。
原unregistered key的review alias自動指向`nccu`，歷史metadata不刪。

[`inventory_after.json`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v99/inventory_after.json)
清點 **934鍵／64設定／22啟用／870未註冊**，6,202,569個known URL heads／3,681,440個stored body heads；
鉅亨3,595,046known／3,563,584stored／24,682missing-body work。
這些是03:36的快照和URL-head粒度，不是alias去重後唯一文章、全網完整性或發布日期。
[`全934個別後續清單`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v99/all934_source_plan.csv)
6ongoing／16finite／666小量審查候選／38大歷史候選等均保留，887鍵仍未個別審查。

[`四寬度共用進度頁驗收`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v99/dashboard_after/acceptance.json)
1440／768／390／320各934鍵、39頁，政大實際displayed counts `[1,1]`／ratio1，
0publisher requests、0JavaScript errors、0failed resources。
finite完成後不輪詢、無新feed／sitemap／猜ID；新metadata在凍結範圍之外仍須核對，
「1/1」不是政大全網站歷史已完成。這是local gateway驗收，沒有宣稱當前DDNS外網部署驗證。

### 診斷owner防呆與測試

新host診斷獨立root只在沒有已啟用domain overlap時可用；註冊後須回canonical root與原exclusive HTTP lock。
[`probe_news_sources.py`](../scripts/probe_news_sources.py)在任何HTTP構造前核對權威live registry，
相同、父／子domain重疊都拒絕，不用變更root取得第二份robots／cooldown／rate budget。
真正disjoint host與尚未啟用的候選不會無故停止其他provider。
[`diagnostic_guard_acceptance.json`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v99/diagnostic_guard_acceptance.json)
實際重跑NCCU singleton CLI在HTTP前拒絕；0請求／0主佇列／0診斷額度寫入，原completed head與live owner相同。
往後對此source診斷用`--config configs/news_fulltext.json`，不再用新host時期的isolated probe root。

測試保留原失敗，不更改原run outcome：初版CMS前綴誤當32hex，11failed／25passed，
原件指出真正`a`+32hex後36passed；原生shared focused298passed。
初次4743回歸未包含FinMind的16個既有news測試；改回先前完整pattern後，
4758passed／1failed明確指出default enabled名單少了新nccu，補入本次真實scope後4759passed。
加入owner guard後277focusedpassed；最終相同完整pattern **4771passed／71.79s**，沒有刪測試或降低驗收。

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q -s test/test*news*.py test/test_cnyes_news_bulk.py
```

初次activation helper匯入名稱打錯也保留failed run；改用已存在`load_source_review_ledger`後13項實際guard通過。
這些parser／helper／名單錯誤沒有當作來源錯誤，更沒有寫入無效正文。

### 其他來源仍在推進

[`runtime_after.json`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v99/runtime_after.json)
13項工程驗收通過；同new owner248.384秒DB窗口新增鉅亨 **191**／Global Voices **25** 正文heads，
鉅亨0.769head/s；244.981秒獨立HTTP counter窗口1.233req/s。
窗口／cohort不同，不計下載倍速或理論上限。
[`22來源transport稽核`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v99/transport_after.json)
及[`實際forward recovery`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v99/recoveries_after.json)
核對3897167?exp=a／3897146?exp=a兩個new傳輸失敗，皆有後續正式正文／raw／作者／clock／receipt，不只pool reset。
前owner的4801463?exp=a、4841220?exp=a、1487274、1507405仍body_not_found；
4841220按原排程03:17重試仍未解，下一次04:17可排，其他原期限也沒有提前清除。
這個新的2/2窗口不能撤銷前次23/27及更早未解證據。

[`本階段closure`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v99/stage_closure.json)
把真實525字取得、scope保護、fresh receipts、13runtime、22transport、全934清單、四寬度UI與matched regression分開核對。
於 **03:45:58.054459**全部 **17項guard**接受；最後最新128批／**6141個正文版本**／0errors通過，
同owner、當下狀態時鐘與body驗收時鐘均核對，沒有額外publisher HTTP／queue mutation。
全來源／全歷史goal維持active；中央社暫緩，來源拒絕／待審權限／正文缺原件／其他解析缺口繼續保留。

## 鉅亨敘事圖片容器與財經來源歷史識別

2026-10-12 v100。中央社依最新指示暫緩：不查API／文件／端點／key、不改憑證、不啟用、不跟轉載原文。
沿用原934來源鍵、共同collector／registry／queue／NewsHTTP／immutable batch／readonly dashboard，
沒有建立另一條鉅亨HTTP或擴充診斷預算。

### 從真實錯誤到正式正文

正常source28 owner於 **03:51:23.238422** 留存 `https://m.cnyes.com/news/id/4898771` 的實際失敗原件：
`body_not_found`／`missing_body_representation`，不是403、robots拒絕或付費內容。
原件raw SHA `16aa41f78b8595fd061735d8bf61d9b0d6ddc550f83d7e22b386527e53d765f3`；
其entity-encoded current content和可見原生DOM完全相同，整篇13個段落與基金警語包在一個外層`figure`。
其中只有兩個leaf figure是圖片，外層則是實際敘事容器；generic figure排除規則抹掉全部正文。
先前「圖片-only或visible/schema不一致」的猜測因此撤回，保留原錯誤證據而不是重寫舊收據。

[`共同原生解析器`](../downloader/news_fulltext_publishers.py) 的 `_cnyes_narrative_figure_views`
只在普通body為空、current全文存在且來源有外層figure時執行。
逐一核對唯一visible/current外層DOM、已觀察的13段／兩個image葉／4個警語換行／1個底線、
精確NBSP空段、來源visibility、合法公開image與link、未知attributes／comment／深度。
只有comparison copy解開外層，圖片葉繼續排除，不能從schema、隱藏hydration、圖片OCR或其他網址補正文。
最後仍須整篇可見文字與完整schema相符；任何單邊數字／正負號／段落變動都拒絕。
這是目前有真實原件證明的版型，不宣稱所有任意figure文章都已修復。

[`原件與耐久重播`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v100/original_replay.json)
8項guard與兩種parser的六次完整claim→parse→batch→fsync→receipt→readback通過，
保留全文 **2,630字／7,022 UTF-8 bytes**、Anne陳巧諭、
`2022-06-22T02:27:20.000Z` 的來源發布／修改時鐘，以及全部基金警語；
沒有補歷史時分、改原數值或把diagnostic原件直接升為正式body。
初版synthetic測試267pass卻在真原件NBSP空段失敗；依真DOM修正後273pass，這個差異已記錄。
[`新回歸測試`](../test/test_news_cnyes_narrative_figure.py)96項涵蓋兩種parser、native URL變體、
streamed Fizz、單邊變動、hidden／未知DOM、完整schema和source nonmutation。
完整相同pattern **4,867passed／77.77s**，沒有刪掉FinMind或bulk測試：

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q -s test/test*news*.py test/test_cnyes_news_bulk.py
```

鉅亨HTML capture **28→29**，其他63 capture／全部64 authorization與bulk API指紋不變。
04:06:58僅graceful重載新聞daemon，舊owner排空15秒；**04:07:13** PID508607／新owner
`a0a0db300b594506b5d9f595c0c5dc93` 正常啟動，timer保持active，未改其他provider服務。
原缺文重試期限 **04:08:53.432246** 保留；普通fair queue於 **04:15:52.789704** 正式取得全文。
04:16:18耐久批次`bodies/20261011/20261011T201618902883Z-453978b6.parquet`才完成batch落盤，
不能把HTTP取得時鐘與batch commit時鐘當成相同事件。
[`正式精確文章驗收`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v100/formal_article.json)
核對raw、作者、原時計、source29、current replay和所屬24個正文版本；正文SHA
`e0e9a04aa166b9b62e291708d584505e223a6b0453f4dc9fe58bdf55ad6e1bf6`與重播相同。
這次沒有第二次重啟、selected HTTP插隊、重排或提前清deadline。

### 真實吞吐、來源隔離與恢復

[`新owner runtime`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v100/runtime_postcapture.json)
13項工程guard接受；同owner **552.287526秒DB**窗口新增鉅亨 **401**／Global Voices **26** 正文URL heads。
鉅亨0.726新head/s；另外 **553.165149秒HTTP counter**窗口1.133req/s，
兩個時鐘不能混算、bulk新heads不能當獨立篇數，不宣稱同cohort倍速或官方最高速。
額外的wrapper核對只在空body分支執行，沒有擴大健康頁的full-tree掃描或增加來源HTTP。
[`最新128批驗證`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v100/recent_postcapture.json)
核對 **5,958個正文版本**、0errors；這不是全湖每個歷史版本完整稽核。

[`22啟用來源transport觀察`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v100/transport_after.json)
獨立保留鉅亨04:08:22.335069標頭前25.10秒ReadTimeout；
[`forward正文驗證`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v100/recoveries_after.json)
證明 `3734182?exp=a` 在04:08:54.679726有真正較晚正文、raw／署名／clock／receipt，
不是只換connection或用舊body冒充恢復。
這個新窗口1/1不撤銷前owner未解樣本。
AMP origin的robots502保留該origin的期限與後續probe；不能因此說mobile主機也被封，
更不能把512秒／篇之類跨來源或冷卻快照當成鉅亨穩態吞吐。

四個較早缺文 `4801463?exp=a`、`4841220?exp=a`、`1487274`、`1507405`仍按各自due重試，
沒有完成收據就不計入完成。正常owner又留存`4631268?exp=a`的schema/figure mismatch和
`1580057`的缺文原件，列為下一次逐份解析修復，不拿HTTP200或容器猜測冒充正文。
FTV第三方署名、Fed原PDF、CMoney實際拒絕與更早缺原件樣本也保留。

### 三個財經來源不是「有回應就有歷史」

[`診斷前基準`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v100/before.json)
保留三provider的精確queue counts、授權／capture指紋與原NCCU closure。
新host私人診斷先驗證與已啟用來源無domain overlap，兩個policy只透過共用NewsHTTP、
robots、singleton writer及2秒pacer，無新聞discovery、ID列舉或吞吐壓力測試。

| 來源／既知URL | 實際證據 | 決策 |
| --- | --- | --- |
| Wantgoo 玩股網／452 | [官方使用條款](https://www.wantgoo.com/terms-and-policies)明列未經書面同意的重製／改編／轉用等限制；沒有新聞HTTP或數字配額證據 | 等待權限，不擅自啟用全文archive |
| Wearn 聚財網／537 | [服務規則](https://www.wearn.com/servicepolicy.asp)實際200／Big5；[原創解釋](https://www.wearn.com/cre.asp)的原創標記也可能是授權轉載，不能等同自有授權 | policy可讀不代表所有新聞可批次保存；沒有news.wearn文章HTTP |
| Investor 財訊快報／551 | [Copyright](https://www.investor.com.tw/onlineNews/Copyright.asp)實際200／Big5；一個既知舊AJX網址回傳最新群組headline，無September日期或canonical原文證據 | 歷史映射／使用範圍尚待審查，不回填最新文章或猜歷史API |

[`兩個實際policy診斷`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v100/policies_probe.json)
及其私人raw／receipt保留。Web reader的Unicode decoding error不是源站不可訪問：
真正Wearn、Investor政策頁都是Big5／200，要區分解碼工具、源站存取和正文語義。
[`Investor精確歷史URL metadata診斷`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v100/historical_identity_probe.json)
對`AJX=2026/9/2110410`只做一次既知URL請求，200／100,736bytes／無redirect；
其title為當日雙碩headline，visible只有2026/10/12、2026/10/8，不能據metadata backdate。
初次byline matcher使用【，原生credit則〖；artifact的credit判定和「疑似latest」布林未成功驗證，
不是來源沒有作者或確定551筆皆壞。正式registry與review ledger保留這個unknown，
沒有保留新聞raw／正文或宣稱歷史取得；獨立官方首頁的latest title只支持此單篇映射待核對。

更新[`來源審查ledger`](../configs/news/source_reviews_20261009.json)以供原readonly頁顯示；
私人[`financial_probe.json`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v100/financial_probe.json)
已明確disabled／awaiting_authorization，避免後續誤啟用。三source共 **1,540URL／0正文**原隊列不變。
沒有把版權或身份unknown當source irreparable，也不繞過member／付費／第三方原文。

### 全來源、網頁與驗收邊界

[`全934鍵工作清單`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v100/all934_source_plan.csv)
保留 **934／64設定／22啟用／870未註冊**，目前884鍵未個別review。
[`全來源快照`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v100/inventory_after.json)
於04:08清點6,202,569個known URLs／3,684,607 stored heads，後續正常取得會繼續變動，
不能把快照數字講成當下總數或全市場完整率。
[`四寬度readonly UI`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v100/dashboard/acceptance.json)
1440／768／390／320各934鍵、39頁、所有審查與來源進度皆可讀，0publisher requests。
只驗local gateway，不宣稱當前DDNS／WAN已重驗。

[`本階段closure`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v100/stage_closure.json)
分別核對真實錯誤／六次耐久candidate、正式2630字文章、13runtime、fresh128批、
source契約、三來源尚無正文、CNA暫緩、全934scope與local UI。
previous v99 closure immutable保留，不用舊owner strict proof冒充source29全域驗收。
第一次readonly closure helper誤用不存在的`SourcePolicy.enabled`而退出，沒有完成宣告或修改producer；
改回原configuration的enabled與registry.allowed，保留failed run，再重新取得fresh128批與runtime收據驗收。
**04:24:05.517388** 全17項closure guard接受，當下body／runtime驗收分別僅47.236／46.639秒，
同source29 owner與程序／metric clock再次核對。
[`最後128批`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v100/recent_final.json)
**5,970個正文版本**全部接受；
[`最後runtime`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v100/runtime_final.json)
910.269677秒DB窗口新增鉅亨 **1,392**／Global Voices **58** 正文URL heads，
鉅亨1.529head/s；903.477755秒HTTP窗口2.021req/s。
與552秒快照不同窗口，不用當成同cohort提速因果證據或來源理論最高速。
**工程修復與這一篇正式補回成立；全來源／全歷史／所有provider故障／理論最高速未完成，總goal仍active。**

## 圖說包裹原敘事段落、完整順序與自動正式驗收

2026-10-12 v101，Asia/Taipei。範圍是共用Cnyes原件解析、caption完整性驗收與有界唯讀交接，
不另開下載管線或改來源HTTP速率。中央社依最新指示暫緩，不查API／key／端點或轉載原文。

### 兩份實際失敗原件不是同一種問題

[`原件基準`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v101/before.json)
保留正常owner已收到的Parquet／receipt／raw SHA、原source28、source29重播錯誤與精確queue heads。
這階段沒有publisher診斷HTTP、query variant切換或新增raw額度。

| 原件 | 完整來源證據 | 處置與仍未完成的邊界 |
| --- | --- | --- |
| `4631268?exp=a` | 五段原敘事，其中128字段落位於含巢狀figure的外層figcaption尾部；四組圖說／圖片、visible與current全DOM、完整schema均可對齊 | 沿用共用解析器保留671字正文；仍等待原05:14:13期限起的正式重試，不能把原件重播當已下載 |
| `1580057` | visible文章容器只見`(RH) :`；沒有可接受的原敘事表示 | 保留`body_not_found`與後續正常重試；不以schema-only或推測文字補全文，也不宣稱來源永久無法取得 |

新的caption分支只接受已觀測的`p → nested figure → p`完整形狀；第一個p是原圖說，
最後p是有完整跨表示證明的原敘事。未知attributes、hidden、事件、comment、重複圖片、
單側DOM／數值／順序改動與不完整schema仍拒絕。
沒有巢狀leaf的既有單圖rich caption維持舊語義，不能為了negative test擅改它的資料角色。
原來源五段順序、負值／數字、作者張欽發與發布及修訂
`2021-04-19T07:17:00.000Z`保留；缺修訂時鐘的fixture仍是未知，不填成發布時間。

### 共用順序問題必須由實物驗證揭露

第一次[`真原件候選驗證`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v101/original_replay.json)
只在「四組圖說／圖片的原順序」一項失敗。原因是原共用實作先收一般caption，
再加巢狀gallery caption；混合版型會改變來源順序，而正文SHA仍可能一致。
已改成依完整current figure keys單次順序收集，不在每張圖重新搜尋／解析整篇。
只有出現經證明的敘事尾段時才複製已解析comparison tree，保留原128字於原位置；
健康非此形狀頁面不增加這份複製或來源請求。

[`修正後12項原件驗證`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v101/original_replay_after_order.json)
核對五段、128字恰好一次且在最後原敘事之前、四組caption/image全順序、完整schema、
署名與原時計；前次m4898771的2,630字原正文仍同SHA。
lxml／html.parser各三次，共六次claim → capture → fsync → immutable batch → readback，
每次只接受兩個真正文、兩個版本，缺文1580057仍拒絕。
body SHA是`71fb0e804016cd8049dc337a1115626bebaed3438055c858b507e20daa1cce96`，
但這是**候選原件重播身分，不是新的正式capture收據**。

[`共同精確文章稽核`](../scripts/audit_news_selected_articles.py)report contract **6→7**：
除了正文、title、作者、published／modified、canonical與notice，現在比對
`verified_image_captions_json`的整組caption/image/basis及list順序。
JSON object key的序列化順序不是資料差異；caption list順序才是。
錯配即`current_source_replay_changed_verified_image_captions`，不更寫舊immutable batch或capture fingerprint。

驗證：535項解析回歸、103項共用auditor／observer測試與完整維護pattern
**5,005 passed in 74.77s**。第一次測試錯把未知修訂時間當已知、又誤判合法舊rich caption；
保留原33failed收據，修正判準而非偽造source clock。
第一次實物候選的caption順序失敗也保留，後來才修共用收集算法。

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q -s test/test*news*.py test/test_cnyes_news_bulk.py
```

### 正式owner、原重試時鐘與低成本自動交接

HTML capture **29→30**，其他63 capture／全部64 authorization與bulk指紋不變。
04:43:38僅graceful重載新聞服務，PID587203／owner
`ac44f3f76bd945db867b6a44ea6b69a2`；source fingerprint
`7031c776879c5ceb66807128742946c68e5e47ff7d35259e4bd7346b33b008c5`。
[`新owner基準`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v101/runtime_baseline.json)
核對PID start、owner metric epoch、configuration及source code，不能用舊owner心跳宣稱切換完成。

4631268保留04:14:13失敗後的 **05:14:13.086390起可重試**期限、attempts與原last-check；
這不是保證完成時間，也沒有提前清due或增加diagnostic budget。
[`共用唯讀交接`](../scripts/watch_news_formal_articles.py)
只觀察1–16個精確URL／SHA，20秒indexed read、每分鐘heartbeat、有界90分鐘。
pin全部producer／configuration及驗收code；owner／code變更拒絕混代，讀側SQLite contention繼續觀察，
不重啟producer。只有當前owner以當前capture契約落盤、原expected SHA符合才呼叫共同auditor，
再次驗原件、作者／時計、caption pairs與所屬耐久批。
deadline是observer結束，不是把來源或producer判失敗；0publisher requests／0queue writes。

04:51:07啟動受監督run
`v101-scheduled-normal-forward-67-20261011T205106-013c987f`，PID602247，
[`待正式驗證快照`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v101/formal_article_observation.pending.json)
由observer更新；正式結果將寫`formal_article_observation.json`。
這個共用工具取代再複製一份stage-specific polling架構，並不接管canonical下載器的重試／配額。

### 實際下載、逾時恢復與全來源頁

[`13項runtime驗收`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v101/runtime_final.json)
接受；**341.583106秒DB窗口**新增鉅亨217／Global Voices32個正文URL heads。
另一 **338.090375秒HTTP counter窗口**鉅亨1.061846req/s；不同時計不能混成倍速比較。
[`128近期批收據`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v101/recent_final.json)
驗證5,724正文版本、0errors；不是全湖每個歷史版本驗完。

[`22啟用來源傳輸觀察`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v101/transport_after.json)
保留三個新標頭前timeout。
[`真正較晚正文驗證`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v101/recoveries_after.json)
749029?exp=a於04:47:55、753643?exp=a於04:47:46正常取得原文，
current source／raw／作者／原時計／caption及33個所屬耐久版本接受。
第三個4726655?exp=a仍`body_not_found`，不能用HTTP恢復或pool reset把3/3說成已修好。
這個新owner樣本不撤銷其他owner仍未解的正文或傳輸證據。

[`全來源清冊`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v101/inventory_after.json)
於04:51:34保留6,202,573個known URLs、3,689,644個stored heads；數字隨正常下載繼續增加。
[`完整934鍵計畫`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v101/all934_source_plan.csv)
仍是64設定／22啟用／870未註冊、884鍵未個別review。
既有Google literal original resolver已檢查1,285,019個metadata連結；
946,017literal links／339,000opaque／2 unresolved不是正文完成或存取授權，不能另開重複全文管線。
[`四寬度readonly頁`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v101/dashboard/acceptance.json)
1440／768／390／320各934鍵、39頁、0publisher requests；local gateway接受，不主張已驗證WAN。

[`本階段14項工程closure`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v101/stage_closure.json)
於**04:54:51.983607**接受，清楚記錄`formal671_article_accepted=false`、observer仍執行、原deadline未改。
第一次readonly closure helper誤用前stage的config hash key而退出，沒有producer修改或成功宣告；
改讀本stage真實`config_sha256`後接受，原failed trace保留。
source／review／CNA暫緩與v100正式closure保留；較早四個缺文、1580057、4726655?exp=a、
CMoney拒絕、FTV第三方範圍、Fed PDF與全934來源未解工作繼續。
**共享解析修復與工程部署成立；671字正式補回、全部來源、全歷史及理論最高速尚未完成，總goal保持active。**

## 空圖說圖表參照正式補回與未入佇列來源清冊

v102，2026-10-12 05:34，Asia/Taipei。仍使用原任務
`news-adaptive-capacity-20261009`，不是把全934來源目標改成兩篇文章。
中央社依使用者最新指示先不處理：不查API／金鑰／端點、不啟用CNA，既有授權註記保留。

### 先確認上一版真正補回，再修新原件

[`交接前快照`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v102/before.json)
保留source30 owner、正在等待的共用observer及原05:14:13.086390 retry due。
上一版的671字文章於 **05:14:15.639034** 由正常下載器取得，並於 **05:14:30** 完成
原件、正文、四組原圖說／圖片、張欽發署名、`2021-04-19T07:17:00.000Z`來源發布／修改時計，
以及所屬 **60個耐久版本** 的核對。
[`七項正式交接`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v102/formal_source30_handoff.json)
與[`原observer正式結果`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v101/formal_article_observation.json)
另存，不重寫v101於04:54記錄「尚未補回」的immutable closure。
它當時確實未完成，後來正常重試才完成；不是用diagnostic重播冒充下載。

[`source30新失敗原件`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v102/source30_chart_failure.json)
是`3947637?exp=a`，可見／current有完整762字正文，但完整schema比較被另一個空圖說圖表阻止。
舊generic figure規則正確拒絕未知empty caption；問題是未識別這個實際可證的來源形狀，
所以把整篇文章當`body_not_found`再次排程，不是這份原件沒有收到HTTP正文。

| 精確來源 | 原因／本輪動作 | 真正結果 |
| --- | --- | --- |
| `4631268?exp=a` | 先前圖說包敘事及混合圖說順序修復；本輪只觀察正常到期重試 | source30正式取得671字、四組caption/image |
| `3947637?exp=a` | 同篇chart image＋NBSP figcaption的精確獨立參照 | source31正式取得762字、一組caption、一個空chart參照 |
| `m4898771` | 保留先前已接受的figure內完整敘事 | 同原件2,630字SHA不變，沒有再宣布一篇新文章 |
| `1580057` | 真正缺文負例 | 原件重播仍拒絕，不用schema／OCR／alt文字補造 |

### 共用修復的邊界與計算成本

[`共同publisher解析器`](../downloader/news_fulltext_publishers.py)新增
`_cnyes_empty_chart_caption_reference`：figure無attributes、直屬子節點恰為img與figcaption，
img恰有`src`／`data-cnyeschart="cnyeschart"`，caption恰為一個NBSP。
URL須為實際`https://cimg.cnyes.cool/prod/news/<id>/o/<32hex>.jpg`，無query／fragment。
完整DOM hash、image URL、唯一性與原順序必須在同篇current／visible表示中匹配，
並保持整篇schema、free、identity、visibility與clock的既有guard。
未知形狀、單側改動、額外文字／attributes、hidden、comment、重複圖及不完整schema仍拒絕。

空圖表另存為`verified_article_notices_json`中的
`same_article_empty_chart_caption_reference`，`text=""`且保留實際image來源；
它不是正文、不是真實文字圖說，也沒有下載圖片、OCR或把alt／財務數值填入敘事。
另一個真正caption仍獨立保存。只在原mismatch comparison分支增加這個有界shape判定，
沿用已解析tree與一次ordered figure收集，健康非此版型頁面不重新parse全篇，沒有新增HTTP。

原件的作者表示確有差別：current／schema為 **陳蕙綾**，visible header為
**鉅亨網記者陳慧菱 台北**。原始兩種表示保留於`author_attribution_detail_json`，
沒有宣稱三者完全相同或自行更正姓名。原發布／修改秒級時計
`2017-10-25T03:53:24.000Z`照存；本文接受與來源署名品質仍是不同邊界。

[`十項實物guard與六次整鏈重播`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v102/original_replay.json)
使用相同四份immutable原件，lxml／html.parser各三次claim→extract→validate→fsync→durable commit→readback，
每次只接受三個真實正文／三個版本；762／671／2,630字SHA與四組舊caption順序維持。
[`53項新chart案例`](../test/test_news_cnyes_empty_chart_caption.py)覆蓋兩parser、canonical／query／mobile、
stream與圖順序，以及visible membership、未知兩側形狀與完整schema負例。
450解析回歸、95 metadata／review回歸及完整維護pattern
**5,072 passed in 76.95s**。初次focused只有舊fingerprint斷言仍預期30而失敗；
改為新31／前30後通過，原449passed／1failed收據保留並以新驗證supersede。

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q -s test/test*news*.py test/test_cnyes_news_bulk.py
```

### 新owner的正式收據與實際速度

05:19:36只graceful重載`stockagent-news-fulltext.service`至HTML **30→31**，PID662048／owner
`40ab64adc4924780aa2c89bafa49161d`；FP
`72e92c8cda6098980f2f3ece84550a70011f3e7ea90a6f12b0fe8dbbb9367e29`。
其他63 capture、全部64 authorization／allowed、Cnyes bulk及canonical configuration不變。
共用readonly observer不請求publisher、不改queue，正常owner於 **05:20:15.495605**
真正取得762字，**05:21:07**完成
[`正式原件／正文／18版本耐久批次驗證`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v102/formal_article_observation.json)。
沒有清deadline、forced retries或另一root重複使用已啟用host的額度。

[`05:34 runtime`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v102/runtime_closure.json)
13/13成立；**811.533853秒DB窗口**新增鉅亨 **1,088**／Global Voices **78** 個正文URL heads。
鉅亨1.340671head/s；另一 **811.675377秒HTTP counter窗口**為1.860350req/s。
這是完整wall-time觀測，包含等待、source circuit與排程，不是獨立去alias文章數，
也不是同cohort WAN倍速或官方最高速；Global Voices實際robots間隔繼續遵守。
[`最新128批收據`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v102/recent_closure.json)
核對 **5,173正文版本／0errors**，不是整座歷史資料湖全部核對。
[`22啟用來源transport`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v102/transport_after.json)
與[`實際forward正文恢復`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v102/recoveries_after.json)
驗證本owner取樣的5筆文章故障全部真正補回；不撤銷前owner尚未解的body／來源證據。

### 官方來源審查與被漏算的未入佇列工作

既有literal original resolver已完成1,285,019個metadata連結的檢查；沒有重掃整個集合或新增duplicate collector。
本輪indexed原址查詢得到台中38個／彰化1個已知literal link；它們尚未形成queued article heads。
canonical NewsHTTP只對兩個與active provider不重疊的host做有界私人診斷，仍共用owner隔離、
robots、pacer、circuit與相同private-root狀態；兩份主站政策及兩篇精確原文／兩個官方目錄都200，
[`六份raw／receipt`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v102/stage_closure.json)
獨立驗SHA，均`diagnostic_only=true`、`accepted_as_article=false`、`publish=false`。

依[臺中市政府主站開放資料宣告](https://www.taichung.gov.tw/10179/10185/13599)與
[彰化縣政府主站著作權聲明](https://www.chcg.gov.tw/ch2/copyright.aspx)，
審查範圍只包括可歸屬政府的公開文字，保留署名、第三方／媒體／個資與限制資料邊界。
不能借其他department政策推定全網站開放。web reader timeout／JS提示並非canonical HTTP存取失敗：
實際台中有`#cpArticle`／2022-10-19日期，彰化有`article.content`／經濟暨綠能發展處及
`2023/11/08~2023/12/08`發布面板；日期-only／來源interval照存，不補午夜秒數或改用Google metadata日期。
generic extractor仍未核對原生identity／byline／clock，所以本輪未把診斷轉成正式正文。

台中官方目錄宣告RSS node9962及GetData／GetOpenData；`rows=500`只是實際query範例，
未測明最大筆數、最早歷史、配額或full-content batch形狀。彰化宣告HTTP rssnews1／2／2b／3／4，
未擅改為未觀測HTTPS、抓robots禁止的圖片檔或將目錄當正文。後續延伸共同native adapter與原router，
再驗RSS／API batch、歸屬、日期精度及歷史範圍；本輪只有兩筆review annotation，沒有producer activation。

[`source planner`](../scripts/plan_news_sources.py)report **4→5**修正重要漏項：
`known_urls=0`但`original_metadata.literal_metadata_links>0`不是「沒有本機URL」。
全 **192個未入佇列來源鍵／946,017個literal alias關聯**獨立列示；literal關聯不是唯一文章數，
須source審查、literal target去重與原生驗證後才能估請求，`minimum_html_article_requests_if_no_bulk=null`，
不是0或把alias數當article數。兩個官方小cohort排stage1，仍未設定／未啟用。
純planner以既有inventory作O(provider) join，不新增SQL／網路／queue寫入。
[`14項新grain測試`](../test/test_news_metadata_link_plan.py)保留platform／redirector／aggregator與API配置gate優先，
拒絕invalid counts，legacy無欄位與既有queued計數語義不變。

[`全934鍵最新版計畫`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v102/all934_source_plan_final.csv)
及[`05:27清冊`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v102/inventory_final.json)
仍64設定／22啟用／870未註冊、**882鍵未個別審查**；known6,202,619／stored3,692,505隨正常下載變動。
[`四寬度read-only頁`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v102/dashboard_final/acceptance.json)
1440／768／390／320各934鍵、39頁，0publisher calls；是local gateway驗收，不宣稱WAN接受。
來源審查自動出現在既有共用頁，未另建監控或重複爬蟲。

[`16項immutable階段closure`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v102/stage_closure.json)
於 **05:34:52.594750** 接受。closure helper首次以檔案路徑啟動造成package import失敗，
未寫closure或動producer；改用`run_fintech_python -m artifacts.data_quality.news_goal_stage01_20261009.remaining_v102.close_stage`
完成，原failed run/log保留。原件、前closure與測試失敗證據均未刪除。
最終文件核對第一次錯設「至少20個連結」判準，實際19個連結全部存在卻被判失敗；
保留原false收據，改為逐一核對實際連結及錨點、不要求虛構最低篇數。
第二次核對也保留「檢查尚未產出的自身receipt連結」失敗證據；文件改連既存檢查程式，
不把即將產出的receipt當作已存在。
[`八項最終唯讀交接檢查`](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v102/verify_handoff.py)
將結果另存`handoff_final.json`，核對當前owner／兩篇正文／原closure與來源bytes／文件SHA，
不改既有immutable結果。
較早4801463?exp=a／4841220?exp=a／1487274／1507405，以及1580057／4726655?exp=a，
本次精確readback仍`body_not_found`／retry_cooldown；不因HTTP恢復或兩篇新修復就冒充全數完成。
FTV第三方、Fed PDF、CMoney拒絕、其他來源存取／歷史缺口繼續留在總goal。
**兩篇正式回補、共享parser與report5工程驗證完成；全934來源／所有歷史／理論最高速仍未完成，總goal active。**

## 宣告型圖表參照與完整正文回補

2026-10-12 v116，Asia/Taipei。本階段仍只修鉅亨，中央社不處理；其他既有來源照常追新。先前[13篇正常正式驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/normal_thirteen_watch.json)於16:27:53完成，同owner／同code／同config，整篇原件、作者時鐘、正文SHA與耐久批次獨立驗證。直到這個pinned observer terminal成功才改producer，沒有中斷觀察來冒充完成。

### 全原件分析及共用結構修正

[全部57份未解原件基線](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v116/before.json)接受4項檢查；原件與收據SHA、雙parser舊輸出、全部66契約及同owner核對。不是只挑會成功的樣本。[完整頁面差異](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v116/source_shapes.json)和[figure結構清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v116/figure_models.json)沒有HTTP或queue寫入，也不是正文驗收。結構清冊的`recognized_structural_kind=null`只表示該次兩個helper沒有給出kind，不是整個parser已宣告不可抓。

鉅亨current與可見DOM將一張宣告型日K／走勢圖包在至多两層figure，或一個真實圖說p與兩個精確NBSP p內。原parser排除figure作正文是對的，但不足以解釋schema仍保留的圖說。共用[parser](../downloader/news_fulltext_publishers.py)新增有界型別：完整有序DOM SHA、原img來源與所有實際圖說必須在current／visible一致，整篇schema、可見性、同篇身分、作者與來源時鐘仍獨立核對。圖片URL只記為參照、不發HTTP、不OCR、不捏造金融數值；空圖說不發明caption，原有直接caption契約不改。未知隱藏、額外文字／數字、不同順序／圖片、重複／越深結構、未知屬性、錯圖表端點及schema衝突仍拒絕。

普通成功正文仍走原快路；新辨識只在既有figure/schema比較分支，深度與節點有上界，不對每張圖重parse整篇。真正圖說依來源順序獨立保存，NBSP及圖片參照不混入正文。[新156項測試](../test/test_news_cnyes_typed_chart_references.py)含兩種parser、原生／streamed、手機／`exp=a`、順序及負面控制；508相關測試通過。沿用全部117個既有檔再加新檔，[118檔完整回歸](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v116/regression_run.json) **6,855 passed，141.74秒**，没有縮小原維護範圍。HTML capture36→37，其他source授權、allowed、bulk及65個capture不變。

[100原件完整語義／耐久鏈驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v116/implemented.json)接受12項：全部70固定原件＋既有12來源結果＋18正文控制，各自保留不同capture，不用URLmap覆蓋後去重。8篇正文、原作者時鐘、圖說／來源參照逐一核對；原13成功、其他49失敗、既有30結果完全不變，兩種parser一致。四輪ABBA完整解析／claim／接受或失敗／Parquet／fsync／收據／讀回：相同100份輸入，關閉新見證有25個正文版本，開啟有33個；單輪9.35–10.12秒，這是本機cached原件工作流程、不是WAN或官方上限實測。新見證增加8個正確輸出，沒有省略失敗驗證來改善時間。

### 發布與正式回補

[發布意圖](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v116/reload_intent.json)先保存舊owner、18篇既有正式heads、來源契約與原finite ledger。首個prepare run因canonical單次最多16 URL而失敗，发生於stop／start／寫入意圖之前；原run `v116-tested-Anue37-release-prefl-20261012T083855-8c265e85`保留。改成16＋2個有界唯讀驗證，不刪除兩篇也不放寬單次界線；明列分開snapshot，不能叫同一原子快照。[18篇舊正文](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v116/prior_bodies_at_release.json)及發布前flight全部接受。

16:40:48只啟動原新聞service／timer；同一新process於16:44已正常running，[7項實際載入證明](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v116/loaded.json)接受。原18篇正文、作者／發布修訂／capture／raw／路徑未變，全部70原始保存及140次／256 MiB預算没有清除，沒有增額診斷HTTP或重設來源期限。唯讀934来源gateway的process不變，瀏覽器不呼叫publisher。這次observer放寬的是啟動觀察時間，以容納已測到的本機初始化，不是放寬資料驗證或重試來源；沒有第二次restart。

[16:52三篇正式新正文](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v116/current_attempt02/formal_repaired_heads.json)的完整raw、圖說／參照、作者時鐘、body SHA、capture37、current replay及所属批次接受：

| 原URL | 正文字數 | 正式取得時間，台北 |
| --- | ---: | --- |
| `m.cnyes.com/news/id/3748731` | 821 | 16:43:25 |
| `m.cnyes.com/news/id/3989011` | 679 | 16:44:47 |
| `news.cnyes.com/news/id/4545945?exp=a` | 473 | 16:50:52 |

原[16:45一篇驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v116/current_attempt01/acceptance.json)保留。[16:52階段7項驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v116/current_attempt02/acceptance.json)、[100正常正文版本耐久收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v116/current_attempt02/recent_current.json)、原18篇、24來源傳輸觀測及934鍵唯讀頁核對。當期13項runtime工程check為true，`all_configured_providers_successful`仍false，不能說所有來源已恢復；真拒絕與冷卻仍見[傳輸觀測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v116/current_attempt02/transport_current.json)及[恢復核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v116/current_attempt02/recoveries_current.json)。

剩餘3666458／4357680／4427985／4484038／3933212仍由[共用唯讀observer](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v116/formal_watch_run.json)等原17:10:47／16:55:47／17:30:30／17:38:45／17:40:00重試資格；90分鐘是觀察窗口，不是完成期限。只沿同一watch handle驗真實新head，不發HTTP或改queue、不把offline原件直接算正式body。live observer pin的producer／config保持到本批完成。

[全部7,055未存正文URL](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v116/current_attempt02/cnyes_remaining.csv)逐筆列出，其中256活躍缺口＝226未驗證primary候選＋30無已知primary，其餘6,799個來源／身分／路徑結果保留。這些是URL heads，不是已證明不同的新聞篇數；primary hint不是等價body的驗證。完整[剩餘狀態](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v116/current_attempt02/cnyes_remaining.json)沒有捏造全部完成ETA。

下一步仍分析49份未解原件與全部鉅亨剩餘URL；同时保留「已修好parser，ongoing未知缺文仍沿原retry due」的排程原因，後續若優化要走單一owner的精確修復lane及來源拒絕保護，不能用第二個SQLite writer清due。再按[全934工作順序](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v113/work_order.csv)的已知URL降序處理其他原始媒體，轉址／聚合不複製正文管線、中央社暫緩。**本階段8原件修復、3正式正文與回歸已接受；鉅亨全部剩餘、全來源歷史及理論最高速仍未完成，goal active。**

## 舊圖片註解殘片、完整原文與正式回補

2026-10-12 v115，Asia/Taipei。本階段只修鉅亨共用正文取得；工作順序仍是鉅亨剩餘全部先處理，再按其他正文來源的已知URL數降序。沒有新增其他provider、改授權、加API配額、清來源拒絕期限或查中央社。

### 同篇原件證明與解析根因

[初始45份完整原件清點](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/before.json)均被舊parser判為`body_not_found`。其中7篇的current內容只有`article_image`圖片檔名註解；原生頁把同一註解放在相同敘事段落之間，但JSON-LD的純文字`articleBody`含有檔名與`-->`殘片。真正可見正文及current原文一致，並不是缺少正文，也不是應將檔名當圖片文字補入。

[共用parser](../downloader/news_fulltext_publishers.py)新增只在既有完整schema比較失敗後才進入的見證路徑：核對current／原生頁註解的數量、順序、精確檔名、原生parent版型與可見性；將參照放回原位置後，仍要求兩組整篇比較視圖與全部schema文字完全一致。拒絕未知註解、額外詞、移位、重複、隱藏、paid、錯身分／時鐘／作者；不採全域字串刪除或只看局部相似。正文仍是原本完整可見文字，來源參照另存`same_article_schema_legacy_comment_reference`，明列沒有重建圖片或其值。正常schema已吻合的頁面不走新分支。

[75實際原件／完整耐久鏈驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/implemented.json)包括45份新原件與30份既有控制，不用同URL去重丟棄不同capture；兩種parser、四輪ABBA解析／claim／接受或保留失敗／Parquet／fsync／收據／獨立readback均接受，初始7篇原文字數395–1,498。修正只增加可取得正文，沒有修改原始作者、來源時鐘、負值、比率、既有30份結果，沒有宣稱WAN倍速或官方上限。

後續25份原件也保存完成後，[全部70份延伸見證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/all70_extended_comments.json)發現另外6篇同一版型；13篇整篇可見文字、作者／時鐘與分離參照逐一核對，停用新見證時全數回到原`body_not_found`。其餘57篇舊失敗輸出完全不變。70份原件／收據SHA、原capture35與固定cohort保留，總原始回應12,690,452 bytes，仍是私人診斷而非正式正文。首輪假設只有7篇的[失敗驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/all70_originals.json)保留；新的延伸驗收補正實際集合，不覆寫舊收據。

### 測試、正式載入與當前下載

新增91個精確／負面語義case；434針對解析測試通過。沿用全部116個已維護回歸檔並加入新檔，完整117檔 **6,699 passed**，見[同範圍完整回歸handle](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/regression_repair_run.json)。首次完整run的6,697通過／2失敗是舊測試固定期待HTML35；只更新到實際36，原[失敗handle](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/regression_run.json)不刪、不縮測試範圍。[capture契約](../downloader/news_fulltext_policy.py)35→36；全部66授權／allowed／bulk、其他65capture及固定70保存設定不變。

15:31:44舊owner正常exit0，15:31:45只啟動原新聞service／enabled timer。新process的本機啟動工作遲於180秒observer期限才出現running；原失敗run `v115-tested-schema-comment36-sin-20261012T073138-45222108`保留，沒有第二次stop／start。實際metrics中`owner_initialization`為221.707秒，其中包含`acceptance_recovery`139.257秒；`retained_metadata_replay`最慢18.134秒也是已觀察到的阻塞，不能把整段5分鐘都歸因於這一個cohort，巢狀時間也不能相加。15:36原process正式running後，只續做驗收，見[實際載入7項證明](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/loaded.json)。固定70筆ledger、140次保存嘗試／256 MiB cap沒有重設，唯讀gateway process不變；五篇既有正式正文／原作者與時鐘不變。最初prepare的BLOB序列化失敗也保留；修復read-only ledger的JSON表達，沒有改主DB資料。

[15:43正式8篇原件／正文／作者／時鐘／current replay與所屬批次驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/current_attempt01/formal_repaired_heads.json)接受，capture36、原始SHA與全可見正文逐一一致：

| 原URL id，全部為`?exp=a` | 正文字數 | 正式取得時間，台北 |
| --- | ---: | --- |
| 1964474 | 917 | 15:37:50 |
| 2044216 | 1,498 | 15:37:49 |
| 2050961 | 1,670 | 15:37:49 |
| 2062261 | 715 | 15:37:41 |
| 2062370 | 801 | 15:37:47 |
| 2068838 | 622 | 15:37:52 |
| 2070546 | 1,422 | 15:37:45 |
| 874309 | 908 | 15:37:47 |

其餘1886231／1926885／1825858／1487609／1620772在該快照仍等原15:46:31／15:46:48／15:56:49／16:17:07／16:27:31重試資格；**資格時間不是完成ETA**。13篇都交由[共用唯讀formal observer](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/formal_watch_run.json)在同owner／同code／同config下核對實際新正文，再逐筆驗raw／作者時鐘／body SHA／耐久批次。90分鐘只是觀察界限，逾時不代表producer失敗，也不重排或增加來源HTTP。

[15:43 Anue階段7項驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/current_attempt01/acceptance.json)接受；[21正常批／129版本驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/current_attempt01/recent_current.json)及五篇舊正文、24啟用来源傳輸證據、934鍵唯讀頁一致。當前[全部7,064個未存正文URL清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/current_attempt01/cnyes_remaining.csv)有265活躍缺口：235未驗證primary候選與30無已知primary；另外6,799個來源／身分／路徑結果仍保留，不是正文完成也不是全部不可修復。

[全來源runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/current_attempt01/runtime_current.json)仍false，`healthy_sources_make_independent_body_progress`不改寫。15:39基線以前的8篇不可加進後續288.195秒DB窗口：該窗口鉅亨新增0、Global Voices新增27；獨立276.038秒HTTP窗口鉅亨0.398req/s，不是相同工作集合／純正文請求，更非最大吞吐量。CMoney在本owner已有真403且0正文；[傳輸證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/current_attempt01/transport_current.json)與[後續恢復核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/current_attempt01/recoveries_current.json)保留，不稱所有網站恢復。

15:52追加[同一owner的10篇正式正文驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/current_attempt02/formal_repaired_heads.json)：`1886231?exp=a`的654字於15:46:37、`1926885?exp=a`的395字於15:46:51正式取得；同樣核對全raw／來源署名時鐘／body SHA／capture36與所屬耐久批次。原15:43的8篇驗證不改寫。另三篇仍待原15:56:49／16:17:07／16:27:31資格，不當成完成ETA。

[15:52新階段7項驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/current_attempt02/acceptance.json)、[正常225正文版本耐久收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/current_attempt02/recent_current.json)及[13項runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/current_attempt02/runtime_current.json)接受。782.950秒DB窗口鉅亨新增20／Global Voices70，獨立785.792秒HTTP窗口鉅亨0.322req/s；不是同cohort倍速／纯正文請求或最大速率，且不包含基線前8篇。CMoney仍真403／0正文，13項工程runtime只按既有契約檢查當期可執行來源，未解除拒絕或宣稱全來源成功。最新[全部7,062未存正文URL](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v115/current_attempt02/cnyes_remaining.csv)仍有263活躍缺口、6,799其他來源／身分／路徑結果。

下一步仍先處理鉅亨57份未解原件、其他全剩餘URL及啟動驗收恢復／metadata工作的實測原因；這些不是本次已驗收的修復或速度提升。其後才按已知URL量依序處理其他正文網站；Google／Yahoo等轉址／聚合不另做重複正文管線，中央社暫緩。**本階段parser／資料驗證及10篇正式回補接受；鉅亨全部剩餘、全934來源／所有歷史與理論最高速仍未完成，goal active。**

## Finite診斷配額與鉅亨原件回收

2026-10-12 v114，Asia/Taipei。鉅亨先處理全部剩餘URL，再依已知網址量安排其他網站的工程順序不變；本階段沒有新增其他provider adapter，也沒有查中央社API。以下是有界原件保存與實際下載的工程證據，不是全來源或鉅亨歷史完成驗收。

### 原因與共用保存修正

[修正前清點](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v114/before.json)顯示正常owner的257次缺文失敗，都碰到`diagnostic_budget_exhausted`；這是本機診斷保存限額，不是來源HTTP配額或成功正文。從全部剩餘URL選出固定70個沒有可重播原件的原生網址，保留[固定manifest與SHA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v114/focus_spec.json)，不隨重啟或換cohort重新給額度。

[共用保存ledger](../downloader/news_fulltext_failure_focus.py)接入原HTTP／queue owner的失敗回應路徑。只使用原本正常請求已收回的bytes；單原件上限8 MiB、全cohort上限256 MiB與140次保存嘗試。每個精確URL最多一個耐久原件，僅在兩個final檔確實都未寫出的儲存失敗後，才允許下一次正常來源回應再試一次。部分寫入／未知結果不刪檔、不覆寫、不另發HTTP；啟動以固定ledger核對完整收據與SHA再恢復。原件放進獨立有限namespace，舊daily quota掃描不把它當成新的一天配額。

免費原生scope／作者／原發布與修訂時鐘／原URL／headers仍由既有共同檢查器驗證；沒有scope證據時只能保存metadata。所有原件明列`diagnostic_only=true`、`accepted_as_article=false`，不公開、不輸出訓練、不假裝是文章。非70集合仍遵守原診斷限額；所有66來源capture、授權、allowed、bulk指紋未改，只有鉅亨capacity的工作量識別加入這個保存模式，HTTP限流規則不變。

### 驗證與僅新聞owner的恢復

[30份真實保存原件的完整鏈驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v114/implemented.json)10項接受：12個負面／18個既有原件，雙parser的正文、作者、時鐘及來源結果精確一致；正面鏈12個完整正文、負面鏈0篇偽文章，寫入、fsync、SQLite收據與獨立讀回全核對。535項focused回歸、兩次6,600項完整news／FinMind／bulk回歸、最後含恢復guard的**6,608項完整回歸**通過。71項focus測試包含持久額度、部分寫入、原件／收據損毀、symlink範圍及8種退出75恢復判準。

同原件ABBA整鏈wall依序3.75424／3.37706／3.19908／3.39046秒；診斷保存內容刻意不同，文章輸出精確相同。只接受本機保存成本与正確性，不能把它換算為WAN倍速、官方RPS上限或整體來源完成率。

首次部署helper只接受inactive，錯把正常configuration reload的退出75與MainPID0停在failed狀態；原失敗run保留，沒有force kill或另啟HTTP owner。補足「原owner＋已宣告配置重載＋退出75＋MainPID0＋timer inactive」的精確判準，對未知退出碼、仍在跑或不明查詢仍拒絕。核對後[只恢復這個已證明退出75的部署](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v114/reload_resumed.json)，14:30:06新owner載入；原新聞timer恢復active／enabled，交易與gateway未重啟。[8項載入驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v114/loaded.json)通過，五篇原正式正文與AMP拒絕期限保留，934鍵唯讀頁正常。

### 正常產出、完整剩餘集合與仍然失敗的來源

[14:39原件獨立核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v114/current_attempt02/normal_focus_originals.json)確認22份全部位於固定70集合，原件、Parquet檔SHA、收據、原生scope及來源時鐘正確，errors0；22份current parser仍`body_not_found`。這是失敗原件保存完成，不是22篇正文成功。14:42心跳已保存26份／4,668,591 bytes；後四份不冒稱已通過這次獨立驗證。剩餘原件仍只隨正常到期請求保存，不改due或為診斷另發HTTP。

[完整未存正文清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v114/current_attempt02/cnyes_remaining.json)／[7,072筆CSV](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v114/current_attempt02/cnyes_remaining.csv)逐URL去重核對：pending151、retry42、retry_cooldown80共**273個活躍缺文**，其中243只有未驗證primary候選、30無已知primary。另6,799＝來源空頁4,615、查無1,269、未支援路徑574、純報價332、純圖片7、身分未驗證2。候選不等於alias正文、來源空頁不等於已取得或永遠無法修復；禁止用標題、圖片alt或另一URL的正文填空。

[同新owner資料窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v114/current_attempt02/runtime_current.json)432.603373秒，鉅亨新增3／Global Voices35個正文URL heads；[15個正常批次](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v114/current_attempt02/recent_current.json)96個正文版本的原件與完整耐久收據接受。HTTP窗口423.690883秒是另一口徑，61個鉅亨durable版本／4個metric新head不與DB窗口的3個head混稱。這些版本並非全是鉅亨，也不是22個focus原件變成新文章。

[全来源當窗驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v114/acceptance.json)保留**false／8項通過、1項不通過**：全來源runtime12／13是因CMoney真403且沒有正文進展，當時原重試資格14:47:34不是已恢復或完成ETA。24來源transport觀測完整只代表證據可讀，不代表24來源全成功；AMP502、讀取逾時與Global Voices503原證據照留。首次current helper誤讀transport欄位的失敗也保留，修正後結果另存`current_attempt02`，沒有覆寫前次partial artifacts或弱化CMoney的失敗。

下一步仍只做鉅亨：繼續70個缺原件URL的正常回應回收，從已保存可見／current／schema不一致的實際DOM修共用正文規則；任何正文語義變更需新capture契約、精確原件負面控制與正常正式落盤驗證。來源只有報價、圖片、空頁或身分不足者逐筆記錄，不靠RPS放行。**尚無可信全鉅亨完成ETA，全部來源goal仍active**；這輪有限保存驗收與全來源成功分開，不啟動其他網站的新工程。

## 鉅亨優先與按已知網址量的工程順序

2026-10-12，13:44 Asia/Taipei。最新要求覆蓋先前「小cohort／研究topic優先」的工程排序，不改來源權限或HTTP配額。這是**開發、修復與驗收的工作順序**，不是先停其他provider、搶它們的配額或開第二個HTTP owner。

### 順序與完整清冊

[共用planner](../scripts/plan_news_sources.py)新增明確的`--work-order`，讀取[目前選定設定](../configs/news/work_order.json)。新規劃contract6保留原stage、topic、strategy、權限、錯誤與計數；另外列`work_phase`、`implementation_rank`、`implementation_state`及正文下載器候選順序。未提供新設定時保留歷史contract5，舊收據不重標。資料已全存者只留maintenance；來源不明或只有literal link者完整保留於後續review，不把redirect關聯數加成已知獨立URL。中央社留在清冊、明確`deferred_by_user`，不入實作順序。

| 正文管線順序 | 來源鍵 | 已知URL（13:35單一快照） | 已存正文URL | 下一步類型 |
| --- | --- | ---: | ---: | --- |
| 1 | cnyes／鉅亨 | 3,595,305 | 3,588,233 | 集中修剩餘解析、身分與傳輸；全部結果逐筆核對 |
| 2 | cmoney | 272,312 | 0 | 修既有管線與來源拒絕，不另建重複下載器 |
| 3 | udn | 169,425 | 0 | 先核對來源存取／用途，再補共用adapter；排名不代表已有權限 |
| 4 | globalvoices | 110,641 | 38,982 | 既有管線繼續抓，保留原Crawl-delay與正文授權 |
| 5 | wantmedia | 108,058 | 0 | 依原來源規則核對後接共用adapter |
| 6 | money_link | 102,537 | 0 | 依原來源規則核對後接共用adapter |

後續CMNews、MoneyDJ、自由時報、ETtoday等按同一已知URL降序列入，無Top-K截斷。[完整934鍵JSON](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v113/work_order.json)／[CSV](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v113/work_order.csv)包括所有其他來源與缺少正文數。Google、Yahoo、LINE等轉址或聚合來源會出現在全工作清單，但正文下載器順序排除其重複正文抓取，沿用既有原始媒體路由；不是抹除已知連結。

更新清單仍使用原入口與新的output；不直接覆寫上述歷史快照：

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.plan_news_sources \
  --inventory artifacts/data_quality/news_goal_stage01_20261009/remaining_v113/inventory.json \
  --config configs/news_fulltext.json --reviews configs/news/source_reviews_20261009.json \
  --work-order configs/news/work_order.json --output artifacts/data_quality/news_work_order_next.json
```

### 鉅亨剩餘分母與實際正常產出

[13:44全部未存正文URL](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v113/cnyes_remaining.json)／[逐筆CSV](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v113/cnyes_remaining.csv)由唯讀WAL快照及provider／state索引讀出，7,072個URL逐筆保留，276活躍缺口與共用progress表完全核對。243已有primary候選不是alias認定；33無已知primary也不是33篇必定存在的獨立文章。另6,796結果＝4,615來源空頁＋1,269查無＋574未支援路徑＋332純報價＋4純圖片＋2未驗證身分。身分／路徑、來源空頁的可用原始版本仍需個別核對，不能整批叫「已下載」或「不可修復」。

完成鉅亨這一階段的判準是：可取得的完整正文有原件、作者／時鐘、SHA與耐久收據；無文字／不存在的來源有逐筆來源證據；未解解析／身分／阻擋仍顯示未完成。不能靠把來源空白填成標題、把候選正文複製到另一個URL、清拒絕期限或提高RPS冒充抓齊。目前剩餘混合解析與來源阻擋，**尚無可信全部完成ETA**；原重試期限不是完成時間。

[9項當前驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v113/acceptance.json)全通過：相同正式owner、13 runtime guard、24來源觀測、[62個新正常batch／243個正文版本](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v113/recent_current.json)原件與完整收據獨立讀回，五篇既有正式正文／作者／發布與修訂時鐘／capture未改。[同owner資料窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v113/runtime_current.json)1,563.621秒新增鉅亨12／Global Voices140個正文URL heads；不是243篇全是鉅亨，亦非相同cohort的WAN速度極限試驗。934鍵唯讀頁沒有觸發publisher請求。此次85項相關回歸通過，沒有更換正式producer、重啟服務或中斷其他追新。

### 前一輪分類修正已部署，但不算新正文

v111原六筆quote觀察已[13:09全部正式完成](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v111/outcome_watch.json)。v112接著把五種實際純圖片森林納入[共用image分類器](../downloader/news_fulltext_cnyes_images.py)：單圖、三圖、精確圖表尺寸與原桌面資料中心署名，全部仍要求同篇visible／current／schema、免費狀態及發布時鐘。12個實際原件變為6 quote／5 image／1仍缺文；18個既有原件的12個完整正文精確保持，正／負耐久鏈接受，沒有抓圖、OCR或拿alt當正文。

[實作原件驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v112/implemented_replay.json)、1,026相關／6,465完整news含FinMind與bulk回歸及93個唯讀observer測試通過；image outcome1→2，其他65來源capture與全部66授權／allowed／bulk不變。13:17僅載入新聞owner／原timer，[7項載入證明](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v112/loaded.json)接受，交易及gateway沒重啟。五個image案例13:44已3／5按原排程正式分類，另外兩筆仍由原observer觀察，不重設due。原47個cohort的35個未留原件URL仍是歷史診斷缺口，不因此分類或排序變更假裝已有原件。

[16次ABBA完整本機原件鏈量測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v112/candidate_chain_benchmark.json)中，lxml非正文cohort CPU中位0.59670→0.38058秒；12篇完整正文cohort wall 1.70395→1.70759秒，沒有提速。這只是本機分類成本，**不是鉅亨WAN提速36%、官方最大RPS或全歷史完成**。接下來工程精力先集中鉅亨，未留原件、來源拒絕、候選身分與路徑的證據仍保留。全來源goal active，中央社暫緩。

## 報價片段分類與正式排程分開驗證

2026-10-12 v111，台北時間。上一輪v110的REP壓力修復有原件、完整回歸、正常資料與不可變handoff，是有進展；本輪不重寫前次收據。中央社維持停用，不查key／文件／API或原文。

### 範圍、根因與實作

[47個無已知primary的全部active缺文清點](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v111/before_verified.json)保留每筆URL、state、error、原due、attempts與HTTP檢查clock。65份既有私人診斷收據中，有12筆屬於這個集合且保存原始回應，另35筆沒有可重播原件；沒有另發HTTP來補顯示，也沒有從候選桌面正文推定alias等價。

以下6個原件的可見／current／schema及原生同頁身分、payment0與aware來源時鐘都一致，文字卻只是報價或股票代號。舊literal grammar沒有涵蓋這些順序／括號／冒號，因此誤留在未知`body_not_found`的重試lane；加快RPS無法讓它們變成敘事。

| 原URL id | 來源實際完整字串 | quote6分類 |
| --- | --- | --- |
| `1650218` | `(AMTG)22.20-0.48(-2.11%)` | 股票代號在有符號報價前 |
| `1632466` | `19.28+5.28+37.68%：(ECOM)` | 非括號百分比，尾端股票代號 |
| `1612338` | `7.08-2.89(-28.99%)：AtlanticPower(AT)` | 原公司名稱與代號尾綴 |
| `1638863` | `((COSH)46.72:` | 原件額外左括號；不修寫原字串 |
| `1632430` | `((SQI)22.38:` | 原件額外左括號；不修寫原字串 |
| `1580057` | `(RH):` | 只含代號／冒號，另需來源video明確為空 |

共用`cnyes_verified_quote_only_body`只新增小於100字的整串literal fullmatch。既有短文三表示一致、免費及同頁身分guard保持；不得去詞／刪數字強迫匹配，不猜未帶符號數字邊界。另有`img`／`video`／`audio`／`iframe`／`object`／`embed`／`table`、附加敘事、paid、schema／visible不一致或video未知，全部繼續拒絕這個分類。行情結果是有證據的非正文來源結果，不是parser成功抓到一篇文章。

`verified_native_quote_only_outcome_contract_version`由5升6，產生新的Cnyes HTML capture指紋；**native正文parser仍35**。全部66來源authorization／allowed與Cnyes bulk／checkpoint、其他65來源capture完全不變。既有Parquet、raw、作者、發布／修訂時鐘、body SHA與data fingerprint不重標；沒有新欄位取代原始數值。

### 測試、載入與真實資料

[原件驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v111/original_replay.json)12 guard接受：12個實際原件／兩種parser只有上表6個改為`source_body_quote_only`，其餘6個圖表／空文／占位等仍待查。v109的12篇原正文、notice／caption／author／clock全部一致，兩種parser各走一次正／負claim→extract→queue→耐久批次／receipt→獨立讀回；非正文鏈有0個article batch／0正文版本，所有lease清除。全47筆原分母及35未知URL保留。原件重播HTTP0、正式queue writes0。

`581 passed`相關語義及**`5925 passed in 127.72s`完整news／FinMind／bulk**回歸，包括同頁身分、附加敘事、媒體、source outcome、dispatcher、unknown retry、load/recovery、capacity11及唯讀dashboard；不是整個repository或GPU訓練驗收。另有20個精確observer clock／state／lease／missing-key語義檢查，觀察器不建立HTTP client。

[12:19限定重載](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v111/reload.json)先正常drain新聞service／原timer，再啟動同一canonical入口。沒有重啟交易、其他下載器或唯讀gateway，也沒有清AMP robots或其他來源冷卻期限。首次load驗收少了同owner `--previous`比較，原[false載入收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v111/loaded.json)及failed run保留；補[原判準的同owner前後比較](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v111/loaded_verified.json)後，7項載入及13項runtime全部接受，不再重啟、不降低資料進度判準。另一個觀察helper的錯誤import也保留failed run，改用既有`process_identity`核對boot／PID／start_ticks後重跑，只讀驗收接受，未修改producer。

[12:27正常前進驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v111/acceptance.json)8項接受：同owner13 runtime、24個已啟用provider的真transport／recovery證據、15個新正常批次／41個正文版本完整原件及收據、5篇既有正式正文不變、全47初始缺文逐列accounting、934鍵唯讀頁及ABI11正常。[449.708秒DB窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v111/runtime_after.json)新增鉅亨1／Global Voices40正文URL heads，HTTP另449.806秒鉅亨165 requests／0.366825req/s；這包含索引304、真解析失敗與等待，非相同WAN工作量的加速比例。

### 六筆正式分類、未解缺口與下一步

12:27鉅亨有3,588,213個已存正文URL，active缺文仍290＝243個尚未驗證正文等價的primary候選＋47個無已知primary；**本輪6個quote原件不能直接扣成已下載全文**。其原下次資格為12:46:13–13:09:17，保留原attempts／due／transport error，這是最早重試資格、不是完成ETA。

已啟動精確只讀觀察run **`v111-six-normal-owner-quote-outc-20261012T042501-29d2173f`**：沿當前owner，10秒一次讀6個indexed URL，producer code／config／verifier SHA與PID／boot／start time固定，不改queue／clock或發publisher請求。狀態在[有界pending觀察](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v111/outcome_watch.pending.json)，6筆全由目前owner正常分類且來源原件／當前WAL驗證後才寫不可變final；4,500秒是觀察期限，不是抓完ETA。若期限到仍遇來源故障，保留`observation_incomplete`及確切剩餘狀態，不判整個producer失敗或冒充完成。

AMP robots502、標頭前timeout、CMoney／PeoPo403與尚未確認身分、圖組、來源空值、權限或歷史範圍，仍各保留原錯誤與既有自主恢復。本輪沒有新增／全啟用臺中或彰化的RSS／歷史範圍；既有39篇有限scope照舊，正式RSS索引與摘要不是完整原文。下一步先核對上述正常分類、再從35個無原件與6個待查原件釐清真正共享缺口，續做尚未審查來源／正式公開歷史及追新，不做中央社。

**quote6共享分類、部署與正常前進已驗證；六筆正式queue結果、全934來源、所有歷史與官方最高速仍未完成，總goal active。**

## REP故障範圍與正常入口容量修復

2026-10-12 v110，台北時間。中央社不接入、不查key、文件、API或原文。仍沿既有新聞owner、queue、來源存取及不可變收據；本輪不是全部新聞來源完成。

[修正前實證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v110/before.json)封存native35 owner、原重試分母與真容量窗口。HTTP的REP負面cache已按初始scheme／authority隔離，但容量ABI10只扣除已佐證的普通gateway錯誤，未區分REP故障；AMP每次正常政策探測502仍可能連帶把健康desktop／API並行3降為2。沒有其他主機的近期成功，不能反過來推定錯誤一定局部。

共用`GatewayHostCircuit.corroboration`統一「完成回應、120秒內、沒有後續失敗或有效pause」證據；REP轉址同時排除初始與實際失敗主機，headers-only、舊成功、expired pause均不作成功證明。只有真正被佐證的REP502／504負面結果加入terminal隔離子集合；原HTTP請求／回應／server error總數、wall、正文heads及每call正文量完全保留，純錯誤不能當健康分母。未知／相關錯誤、503、任何Retry-After、401／403／429／451及challenge的publisher保護不變。

這是錯誤壓力歸屬修正，不是准許訪問blocked入口。`robots.txt`不可達時，仍不得直接抓該入口文章；初始authority與轉址的處理依[RFC9309 2.3.1](https://www.rfc-editor.org/rfc/rfc9309.html#section-2.3.1)，不以另一主機或舊Allow繞過。負面cache的30–300秒真探測退避、generation及原期限保持；私人access event另存有界corroboration，唯讀auditor驗版本、hosts、年齡、scope與原SHA。

新增33項語義測試，296項相關、**5,780項完整news／FinMind／bulk**、**308項唯讀gateway**接受。[首輪兩個過時測試預期的處置](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v110/failure_resolution.json)保留原failed run；維持原`insufficient_useful_data`行為，新增ABI11與既有10使用完全相同窗口ETA數值，未支持12仍拒絕。沒有改鬆正文／時鐘／拒絕判準。

[11:38限定重載](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v110/reload.json)只重載news service／原timer及唯讀public dashboard；交易、其他下載器、金鑰不動。[原REP cache](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v110/negative_policy_drained.json)及[載入驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v110/loaded.json)接受，全部66來源capture／authorization／allowed／bulk指紋不變；五篇既有正式正文、raw、作者、發布／修訂clock及收據exact readback不變。量測ABI升11重新校準，不把舊10的不同錯誤分母重標成11；原來源限流／拒絕期限不因此清除。

[正常下載驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v110/acceptance.json)於11:43:39接受八項：

- 45.749秒真窗口31個terminal requests／responses（21個200、9個304、1個502），4個新正文URL／12個耐久版本；error總數1保留，隔離REP1，其他30個結果的壓力健康，`all_observed_hosts_healthy=false`。並行3保留，不因AMP502降速；混合API／HTML不宣稱最高正文吞吐。
- 12個正常新批次、77個正文版本的raw／Parquet／fsync receipt及current-head witness逐批驗證，不把HTTP200或counter當正文收據。
- 本輪原290缺文沒有一筆被上述12批直接補回；這些是正常增量／修訂進度，不能冒充歷史缺文修復。11:43:39正文URL3,588,202；缺文仍290＝243個已有primary候選＋47個無已知primary。候選不是已驗證alias或唯一文章。
- 全24啟用來源的[傳輸實證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v110/transport_after.json)、[恢復核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v110/recoveries_after.json)及[runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v110/runtime_after.json)接受的是證據／健康來源進展，不是所有來源已恢復。鉅亨11:42:06尚有真正25.099秒`awaiting_headers` ReadTimeout；failed-origin connection recovery／自動重試繼續，當時回到2路有界transport recovery。AMP仍未取得有效政策，不能算已修好。
- [四寬度實際頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v110/dashboard_final/acceptance.json)1440／768／390／320皆934來源、39頁、0 JS錯誤、0 publisher requests；新11窗口不足時顯示未知／採樣中，不從舊owner或最快窗口猜ETA。這是local gateway驗收，不是DDNS WAN驗收。

本輪未改原文parser、數字、來源clock／byline、資料權限、tick／訓練或正文用途；健康HTTP多一些不等於官方最高速。臺中／彰化仍為已驗證有限cohort，declared RSS／JSON完整歷史接線尚待逐項驗證。880個未個別審查來源及其他權限／歷史／正文／傳輸缺口繼續留在總goal，下一步優先逐一重播47個無已知primary的實際原件，再接可公開取得的正式來源；中央社維持暫緩。

## 占位圖組回補與鉅亨缺口分母

2026-10-12 v109，台北時間。中央社不接入、不查key或文件、不請求API／原文。上一輪是有正式正文與收據的進展，非總goal完成；本輪沿用原task和新聞owner責任。

### 根因、修正與同原件驗證

[修改前證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v109/before.json)先保存當前native34單一owner、13項runtime、128近期批與兩份未被接受的真實來源回應；不額外抓publisher做benchmark。
`4825496?exp=a`在一張圖片後有exact NBSP段落與四組明列社群標籤／連結；原三連結版型不適用，不能把占位段落當缺失正文，也不能刪除連結label或fbclid參照。
`5164089?exp=a`的第二個圖說多包一層figure並帶exact NBSP段落；兩個相同「資料來源」文字各自對應不同原圖片，不能去重圖說或把兩者都配給第一張圖片。

共用`news_fulltext_publishers.py`新增有界typed forest，仍要求無未知屬性／事件／隱藏文字／comment／任意巢狀內容，完整有序DOM、當前可見內容與整個schema一致。正文所有原數值保持不動；社群附註、四組href／label與圖片參照單獨保留，巢狀圖說保持caption→原圖配對和原順序。圖片不下載、不OCR；alt不是正文，來源連結不跟隨。

[18份原件重播與六次完整耐久鏈](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v109/original_replay.json)接受：兩種parser相同，12篇正文；既有10篇正文／作者／時鐘／metadata及六個拒絕結果完全不變。兩份新原件恢復1,081／1,054字，獨立從current原文驗證完整敘事、附註與圖說。每次鏈都包含claim、解析、驗證、Parquet、fsync收據及獨立讀回；0 HTTP、0正式queue寫入，不能把這個私人驗證當正式回補或WAN倍速。

`test_news_cnyes_placeholder_forests.py`新增122項語義測試，含兩種parser、三個實際命名空間、完成stream、重複圖說原圖配對、hidden／event／unsafe URL／未知結構／不一致schema反例；native相關2,441項與完整news／FinMind／bulk **5,747項**通過。手動版本欄位縮排造成的首個collection失敗已修正，原失敗收據完整保留並[追加較晚驗證處置](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v109/failure_resolution.json)，未上線錯誤版本、未弱化斷言。

### 正常下載與進度頁，不改寫舊成功

先完成[上一輪三篇native34正常正式收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/target_bodies_final.json)，再以[有證據的新聞限定重載](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v109/reload.json)於10:58載入capture35。其他65 captures、66授權／allowed／bulk不變；既有三篇raw、SHA、作者與時鐘、capture34保留，不重標為35。[新owner讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v109/loaded.json)接受，compact status2為2,005,862 bytes，原4 MiB界線不變。

[本輪正常正式兩篇收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v109/target_bodies_final.json)於11:04:06接受：

| 原URL id | 正常抓取時間 | 正文字數 | 原署名 | 資料版本 |
| --- | --- | ---: | --- | --- |
| 5164089?exp=a | 11:00:02.173830 | 1,054 | 張欽發 | capture35 |
| 4825496?exp=a | 11:03:34.219652 | 1,081 | Anne陳巧諭 | capture35 |

原最早資格為11:00:00／11:03:32，未清due、未強迫claim或另開HTTP；正常producer的raw、秒級來源發布／修訂時鐘、body SHA、完整批次與當前parser重播全部接受，與私人原件body SHA相同。
[四寬度頁驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v109/dashboard_final/acceptance.json)在1440／768／390／320均934來源／39頁、無JS錯誤／publisher請求；是local同origin，非DDNS外網驗收。
[13項正常runtime與進展](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v109/runtime_current.json)接受；338.470秒DB窗口鉅亨新增7、Global Voices新增30正文URL heads。HTTP另341.761秒窗口鉅亨0.363req/s，包含索引200／304與真解析失敗，不能拿舊817-head窗口宣稱本窗口的速度。[128批讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v109/recent_current.json)驗證2,515個正文版本，不代表全lake掃描。

### 分母與剩餘故障

[11:01全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v109/inventory_final.json)是已知URL粒度，不是全網或去alias後的唯一篇數：

- 鉅亨3,595,249已知URL；3,588,176個已存正文heads，291個仍在pending／retry／cooldown。這些缺文才是需要回補的工作。
- 其餘6,782為來源空版型4,615、行情片段321、單圖片1、找不到來源1,269、不支援路徑574、未驗證身分2；不當正文成功，也不把真正來源無文捏造成文章。
- 後續只讀分組顯示151個pending在AMP主機，受其robots端點502阻塞；尚有amp502、原生解析／身分／傳輸失敗及未到期重試。提高整個provider的壓力不能修復這些來源結構和存取問題。
- 全934鍵／66設定／24啟用；6,203,351已知URL、3,708,406正文URL、2,131,113等待授權URL。[更新計畫](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v109/all_source_plan_final.json)880未個別審查；兩個校園來源新增primary規劃觀察，均`review_incomplete`，不是批次存取授權或新增下載器。

銘傳一週的[原始頁](https://www.week.mcu.edu.tw/43881/)有來源日期與編輯credit，編輯不是推定記者；中興大學的[媒體彙編頁](https://www2.nchu.edu.tw/news-detail/id/54287)有多個原媒體／作者／日期區段，不能合併成校方自有單一稿件。web工具不能讀兩站robots不等於來源403、無robots或允許；保留未知與原媒體歸屬，本輪未接入其capture，未改來源授權。

下一步沿原正常排程追剩餘解析／傳輸／AMP robots故障、保留並驗證真正較晚正文恢復；再續做尚未審查來源的端點、權限、歷史與增量工作。capture parser變更與HTTP容量證據是不同邊界；舊正文容量指標不直接作為新parser的最高速／ETA。所有來源、所有歷史、所有錯誤及numeric official maximum仍未完成，總goal **active**。

## 敘事圖組完整正文與原重試期限驗證

2026-10-12 v108，台北時間。沿原`news-adaptive-capacity-20261009`任務；中央社不接入、不查key或文件、不請求API／原文，既有停用清冊保留。

### 從原件區分內容、參照與未知缺文

[09:53修正前證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/before.json)先凍結新舊owner、九個私人來源原件／收據SHA、兩種parser結果及完整DOM。兩個parser都拒絕，不能據此認定來源不存在正文。
本輪可驗證的共同根因是：原生container與目前schema都有正文，但`figure`結構尚未被正確分類，或整個敘事圖組被當成圖片移除。
HTML的`figure`可承載完整flow content，亦可巢狀；這支持「先檢查實際結構」而非「所有figure皆非正文」的解析設計，不代表新聞來源授權。[WHATWG HTML官方規格](https://html.spec.whatwg.org/multipage/grouping-content.html#the-figure-element)

| 原失敗URL | 實際原件結構 | 共同修正與保留 | 私人原件重播正文 |
| --- | --- | --- | ---: |
| `4951809?exp=a` | 外層a／img＋figcaption內plain p／nested figure／a／img | 精確兩張不同image、兩個相同公共href；圖說只配原第一張圖，第二張不補圖說，所有參照獨立保留 | 1,310字 |
| `4957610?exp=a` | image-only葉節點與plain p交錯，末段原風險提示 | 整個22段敘事在原位置進入主要正文；所有數字、比率、原連結與聲明保留，不當作僅警語附註；5張圖片／8個原連結另存參照 | 2,961字 |
| `4841220?exp=a` | img／figure／p／p／figure／p，已知三行警語＋NBSP占位 | 六節點完整DOM、兩張圖片、原warning三行精確比對；只接受實際單一NBSP占位，任意短文／空白／未知markup不豁免 | 1,400字 |

[共用原生parser](../downloader/news_fulltext_publishers.py)新增三種有界、typed、可獨立驗證的figure grammar，**只在實際需要structured comparison的路徑進入**，健康的普通文章不做額外通用深層掃描。
當前schema、可見完整DOM、URL／canonical／OG／文章識別必須一致；hidden、事件attrs、comments、script、重複圖片、錯連結／caption、陌生markup及單側更動仍拒絕，不移除不懂的數值或推補缺失參照。
敘事只unwrap外wrapper並保留原文，image-only葉才分離；caption及敘事不在notice內重複儲存。原警語、圖說、連結與image URL為來源資料，沒有抓外站或照片、沒有將URL可見視為請求權限。

[完整原件重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/original_replay.json)12項接受：16份原始response與receipt SHA未變；`lxml`／`html.parser`所有成功與拒絕完全一致。原七篇的body SHA／字數／作者／來源時鐘／圖說／notice metadata完全不變；另外六個原來短內容／空正文／quote-only結果仍拒絕，沒有捏造終態或正文。
三個修復結果另由獨立來源文本／paragraph order比較，22段全部在primary body，原作者與來源發布／修訂時鐘一致。
再以兩種parser交錯各三次沿原claim→原件回應→extract→validate→Parquet／耐久receipt→獨立readback，每次10個正文版本接受；**0publisher HTTP、0正式queue寫入**，這六次是私人測試，不是正式下載完成。

| 完整16原件／10正文測試鏈 | 三輪wall秒 | 三輪CPU秒 | 精確輸出 |
| --- | --- | --- | --- |
| lxml | 1.873581／1.873718／1.942229 | 0.727773／0.778523／0.817224 | 同10份body SHA、署名、來源clocks與完整耐久收據 |
| html.parser | 2.114316／2.235629／1.815006 | 0.892714／0.841186／0.855435 | 完全相同 |

仍用既有production lxml，不從單一快輪次宣稱parser上限。新接受工作量由7→10正文，沒有相同舊版可接受工作量的對照；**不是整個下載器／WAN倍速或官方速率上限**，亦非單純HTTPbenchmark。

### 語義回歸、版本與實際部署

[176新增語義測試](../test/test_news_cnyes_retained_figure_forests.py)覆蓋兩種parser、desktop／mobile／exp variants、streaming、原文／caption／warning／refs順序、數值、來源clock、forest-only body、非破壞DOM及結構／可見資料單側變造；906相關／**5,625完整news含FinMind與bulk回歸**通過。
完整命令為`run_fintech_python -m pytest -q -s`搭配當時全部實際`test/test_news*.py`、`test/test_cnyes_news_bulk.py`與`test/test_finmind_news.py`；這不是全專案training測試套件。
兩個明列「目前capture33」的測試pin依本次正式ABI升34，只改版號與測試名稱；原授權／bulk隔離斷言不移除。
[原兩次測試失敗與append-only處置](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/failure_resolutions.json)保留不存在test path的exit4／0tests與最初5,623pass／2fail的收據／logs，後續完整5,625通過，沒有把舊failure改成功。

[capture契約](../downloader/news_fulltext_policy.py)33→34，新FP`8210122a544f86e861da75b01afb987cabe85a790a7d1d6880c2a2960a1229f9`；其他65來源capture及全部66授權／allowed／bulk契約不變。
沒有改來源頻率、並行限制、授權、TLS、policy reviews、其他provider或網頁UI；原quote／empty-body／request-deferral／status2契約不變。
[耐久重載意圖](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/reload_intent.json)先凍結三個新缺文原due與前輪三個正式body heads；
[10:12:40新聞-only重載](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/reload.json)只在舊PID完全drain後啟動原service與enabled timer，不提前重試、不重啟其他服務。
[10:14載入驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/loaded.json)7項接受，新owner`735c8c81a4df498d980d6d32446f6ccb`、實際心跳1,993,736 bytes及原4 MiB讀取界線、status2、934鍵gateway一致。
[前輪三個正式正文的新版重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/prior_three_body_replay.json)body SHA／署名／來源clocks／captured_at與舊capture33保留，**沒有把舊資料重標成34**。

### 真正續抓、來源拒絕與網頁

[第一個新owner的實際runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/runtime_final.json)故意保持false／12項通過：基線CMoney就緒，當窗真的發1個request取得403、零新正文，所以共用independent-progress判準正確拒絕。
[來源HTTP證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/transport_final.json)與[不可變診斷](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/runtime_expected_rejection.json)保留；
[workflow綁定criterion](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/runtime_rejection_binding.json)及[原false run處置](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/runtime_refusal_disposition.json)只是明確記錄正確拒絕，**CMoney403沒被修好或變成成功**。
首次分類helper少了workflow必需的task_id／run_id／verified_by欄位而失敗，原診斷與失敗log保留，另建綁定receipt通過，沒有覆寫原false runtime。

[10:21後續runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/current/runtime_final.json)以已觀測來源冷卻作基線，原共用判準只要求就緒鉅亨／Global Voices，13項接受。324.699073秒DB窗口新增548／34正文URL heads，鉅亨1.687717head/s；另335.657324秒HTTP counter窗口鉅亨2.118232req/s，兩種clock不混用。這是包含實際錯誤／升速的窗口，不是去alias唯一新聞或相同WAN/cohort倍速。
[128近期批收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/current/recent_final.json)2,565正文版本、0errors；非全lake、非全歷史。
[全24啟用來源傳輸](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/current/transport_final.json)、[capacity](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/current/capacity_final.json)及[真正forward恢復](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/current/recoveries_final.json)可核查：兩個標頭前transport sample中`1603579?exp=a`有較晚正式原文，`1271151`仍沒有；amp robots502、CMoney／PeoPo403及未知缺文仍保留。
當時capacity`ramping`、7 slots／client target0.2秒，是沿原有界升速階梯的觀測，並非實際5req/s、穩定最高速或官方上限。

[第一篇正常正式收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/target_first_body.json)：`4841220?exp=a`於 **10:21:58.296952**取得1,400字、Anne陳巧諭署名、原2022-03-25T12:20:05.000Z秒級發布／修訂時鐘、capture34及body SHA；當前parser重播與完整所屬64版本耐久批次接受，正文SHA和私人原件重播相同。
另外兩篇仍由[唯讀正常期限watch](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/watch_targets.py)核對：`4957610?exp=a`最早 **10:48:03.739**、`4951809?exp=a`最早 **10:56:50.569**。這是原重試資格，不是完成ETA；watch只讀SQLite／原件／收據、不發HTTP、不改due或claim，觀察到期限也不自動重啟producer。

[全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/inventory_final.json)與[934鍵計畫](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/all_source_plan_final.json)保留66設定／24啟用／868未註冊／882未個別審查；當時6,203,335已知URL／3,706,111已存body heads不是全網獨立篇數。
[四寬度實際頁驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/dashboard_final/acceptance.json)1440／768／390／320各934鍵、39頁、0JS錯誤及0publisher requests；是local同origin，不是DDNS外網驗收。
全來源完成時間維持未知，不因這三個parser修正，將所有權限／真來源空文／失效／歷史範圍算成已完成。

[10:29:41工程closure](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/stage_closure.json)19項接受；測試／原件／指紋／新聞-only重載／原false來源拒絕／四寬度頁／當前正常正文／13項runtime／原始完整calibration128權威／未解watch全部具各自證據。
[後續runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/runtime_closure.json)481.207921秒DB窗口新增鉅亨817／Global Voices33個body URL heads，鉅亨1.697811head/s；HTTP另473.688350秒窗口2.138537req/s，不宣稱最高速或所有獨立新聞完成。
[後續128批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/recent_closure.json)3,108版本接受；
[全24傳輸](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/transport_closure.json)及[真正較晚正文恢復](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/recoveries_closure.json)此時23個failure sample／17個較晚正式正文，6個仍未解；它們不是清冊中所有舊故障，也不能以reset／HTTP200代替正文驗收。
[不可變watch交接觀測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v108/target_watch_at_closure.json)記錄背景run與實際PID／start ticks，mutable watch不被當作固定hash證據；第一篇已正式接受，另外兩篇仍待原最早重試資格。
總goal繼續active：先接受另外兩個正常正式body receipts，續做其他有原件的實際figure／短內容缺口、header timeout／502與CMoney的官方替代；保留Fed PDF、FTV第三方、全934來源個別review及mixed-workload capacity排程。中央社依指示暫緩。

## 新聞心跳有界投影與正常正文正式驗收

2026-10-12 v107。中央社不接入、不查key或文件、不請求API／原文；保留唯讀停用清冊。

### 正常排程實物，不是私人重播

前輪單一owner正常派送三篇原失敗URL，未改原due、未強制claim、未另開publisher HTTP：

| 原始URL識別 | 正式取得時刻（台北） | 正文 | 正式驗證 |
| --- | --- | ---: | --- |
| news/id/232219?exp=a | 08:57:19.538939 | 786字 | 原署名鄭淑雲、2016來源秒級時鐘、raw／SHA／完整所屬批次 |
| news/id/190469?exp=a | 09:11:39.543482 | 1,193字 | 原署名張大仁、2015來源秒級時鐘、raw／SHA／完整所屬批次 |
| m.cnyes.com/news/id/4937652 | 09:20:07.789779 | 888字 | 原署名余孟軒、2022來源時鐘、獨立來源署名附註、raw／SHA／完整批次 |

[09:20全部三篇正式收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v106/target_bodies_final.json)接受，原有界watch成功結束，沒有重開或重設期限。
[第二篇獨立readback](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/target_second_body.json)及
[心跳修正重載後三篇再核對](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/formal_three_bodies_after_reload.json)也接受。
這三篇是正式資料；其他未驗證原件不因此被算成下載成功。

### 進度頁根因與共用修正

本輪四寬度驗收最初真正失敗於`status.snapshot_consistent`。唯讀公開API回傳unavailable／未知數量；同時新聞owner仍在接受新正文。
定位為原pretty JSON心跳超過`news_dashboard.MAX_SNAPSHOT_BYTES=4*1024*1024`，保留了
[原超界狀態檔](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/oversized_status_before.json)。
容量測試歷史佔大部分，另有1,820筆逐日缺口每15秒全數轉成Python字典再序列化。

共同修正，而非改成網頁即時掃SQLite或放寬4 MiB界線：

- `PublisherCapacity.snapshot()`仍保留完整128窗口給原`rate_calibration.json`、重新啟動與容量排序；只有owner心跳明確用`for_status=True`，先取最新連續64個再deepcopy。10秒以上的有效窗口足以保留整段近期ETA範圍，包含失敗、零進度與等待，不挑最快窗口；最末窗口、best／current capacity、所有原始數值不變。
- `summarize_bulk_window_gaps()`在原SQL權威資料上計算精確provider／error總數與首末觀測，僅發布固定64筆有序樣本。原1,820筆沒有刪除或改寫；`sample_truncated`、`total_gaps`及detail authority明列。SQL聚合仍是一次O(G)掃描；Python逐筆物件與輸出明細由O(G)降為有界樣本＋分組，不宣稱SQL零成本。
- 原`atomic_write_json`新增可選compact編碼，預設仍為原pretty輸出；只有新聞兩個心跳寫入點使用compact。沿用同一atomic rename、durable選項與單owner，沒有第二份權威資料或瀏覽器下載器。`status_projection_contract_version=2`明列報告契約，資料capture與授權指紋不變。
- 共用完整status benchmark同步識別compact格式，報告契約升為3；沒有縮減正文、驗證、重試、清冊或宣稱空值為成功。

[同一WAL完整心跳基準](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/status_budget_benchmark.json)：五輪交錯新舊相同資料，舊4,351,402／新2,244,821 bytes；934來源整份公開投影的SHA相同，所有數量、狀態、clock、forecast相同。CPU中位0.693322→0.670689秒，wall0.693672→0.671085秒，僅約1.034倍；不是整個下載器／WAN倍速或官方速率上限，且不包含檔案寫入與live metrics lock。
[候選心跳](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/bounded_status_candidate.json)僅為私人測試輸出，不直接覆寫正式狀態。

新增10項compact值不變／缺口界線／空資料／1,025缺口／10、30、60、600秒窗口的forecasts及完整diagnostic非alias測試；287相關與5,449完整新聞回歸通過。
[重載意圖](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/reload_intent.json)先持久記錄原owner、正式三篇heads與全部66指紋；
[09:31:40新聞-only重載](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/reload.json)確認舊PID完全drain才啟動原service／timer，不重啟其他下載器／gateway、不清來源期限。
[09:33載入驗證](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/loaded.json)7項接受：新owner `205acf843fc24bc0971b8af80de72afb`、status2、實際2,216,003 bytes、全部934鍵、三篇正文與原capture33都通過；全部66 source capture／authorization／allowed／bulk契約不變。

[恢復後四寬度頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/dashboard_restored/acceptance.json)1440／768／390／320各934鍵與39頁，無JS錯誤、0publisher requests；這是local同origin，不是DDNS外網驗證。
[原始失敗的append-only處置](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/failure_resolutions.json)保留兩個failed receipt與logs：原UI真的超界；診斷helper則誤認每個provider必有last_window，已修正可合法缺省而沒有捏造窗口。兩者用各自較晚成功證據supersede，未把原failure改成功或弱化驗收。

### CMoney：目錄API與完整正文分開

已查看官方CMService操作描述，沒有叫用資料操作。下表只記文件明列形狀；不是匿名權限、免費配額或完整歷史驗證：

| 官方操作文件 | 明列查詢／分頁參數 | 文件列憑證參數？ | 回應角色 |
| --- | --- | --- | --- |
| [GetStockRssNews](https://www.cmoney.tw/cm-service/CMService.asmx?op=GetStockRssNews) | date／lastDate／fetchSize | commkey／memberPk | StockRssSimpleInfo：標題、連結、來源、識別碼等目錄 |
| [GetMoreStockRssNews](https://www.cmoney.tw/cm-service/CMService.asmx?op=GetMoreStockRssNews) | articleId／lastDate／fetchSize | commkey／memberPk | 同上，不含全文 |
| [GetDailyHeadlineArticles](https://www.cmoney.tw/cm-service/CMService.asmx?op=GetDailyHeadlineArticles) | oldest／num／newsType | 未列 | Article：Date、Title、SourceName、Url、Tags、Views、StockIndex、ArticleId |
| [GetStockRssArticles](https://www.cmoney.tw/cm-service/CMService.asmx?op=GetStockRssArticles) | stockId／fetchSize／condition／fromDate／beforeDays／baseArticleId | 未列 | StockRssArticlePkgObject目錄，不含全文 |
| [GetStockRssArticlesWithFilterType](https://www.cmoney.tw/cm-service/CMService.asmx?op=GetStockRssArticlesWithFilterType) | 上列stock RSS參數＋filterType | 未列 | 同上，不含全文 |

`Date`／`CreatTime`也未證實等於原媒體的發布時間。官方[Owl金融資料API](https://owl.cmoney.tw/)與[WCFAdox／CMTrans](https://www.cmoney.com.tw/product/valued-data)是金融資料產品／授權整合，沒有因此證實歷史新聞正文。
本次未找到這五個操作的官方數字頻率、fetchSize／num上限、最早日期或全文權益，不猜空憑證、不清403、不從另一端點繞過原文拒絕。
[私人來源review](../configs/news/source_reviews_20261009.json)加入`cmoney:review_incomplete`，只改read-only planner／公開審查enum，不授予collector權限；公開頁不傳這些私人註記、證據URL或任何key。
[934來源計畫](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/all_source_plan_final.json)仍保留882未個別審查；[全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/inventory_final.json)的66設定／24啟用／868未註冊不變。

### 實際續抓與未完成

[09:35最新runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/runtime_final.json)：13項接受，94.358137秒DB窗口新增鉅亨80／Global Voices10／CMNews2正文URL heads，鉅亨0.847834head/s；另91.652428秒HTTP窗口鉅亨1.309294req/s，不混成同一窗口或唯一故事數。
[128近期批收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/recent_final.json)3,178正文版本、0errors；有限source沒有當窗ready work不表示所有provider歷史完成。
CMoney保留先前實際403與未到期 **09:43:39.915409**來源冷卻，當窗0HTTP／0正文；不是修好了。Peopo與其他未就緒工作仍獨立保留。
[全24來源傳輸](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/transport_final.json)、[capacity](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/capacity_final.json)、[實際forward恢復](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/recoveries_final.json)可核查：該次新owner有1個`1905112?exp=a`標頭前transport_error，仍無較晚正式正文；amp來源robots仍真502。不拿pool reset、HTTP200或工程通過當作錯誤已全部解決。
[本輪原始缺口清點](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/before.json)保留九個有原件可驗證的拒絕／短內容觀察；其中`source_body_quote_only`是既有正確非正文分類，不再冒充解析故障。其他包括linked image／nested caption、已知風險聲明與NBSP占位組合、較長敘事圖組以及尚未證實根因的來源短內容；兩個parser一致不代表來源全文不可取得，需繼續修正有證據的表示差異。

[09:42最新17項工程closure](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/stage_closure.json)接受；實際狀態2,274,326 bytes、全部934鍵gateway與不變4 MiB界線再次核對。
[後續runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/runtime_closure.json)的449.341001秒DB窗口鉅亨＋336／Global Voices＋45，鉅亨0.747762head/s；另461.905925秒HTTP窗口1.125770req/s。
[後續128批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/recent_closure.json)2,825版本、0errors；[後續全24傳輸](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/transport_closure.json)及[真正恢復收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v107/recoveries_closure.json)顯示7個失敗sample有6個較晚正式正文證明，1個未解仍保留，不改寫09:35當時的0／1。原生解析器／66資料與授權指紋未再修改，不宣稱全舊錯誤或全provider已完成。

09:48:17心跳唯讀交接再次確認同一owner仍持續運行；CMoney原冷卻期限之後，正常排程真的送出1次HTML請求並收到403。這是新owner實際來源拒絕，不改寫09:35／09:42窗口的零請求，也不能宣稱該provider已恢復。

整體goal仍active：全934來源／全歷史／全錯誤／理論速率上限尚未完成，中央社明確暫緩。

## 原始來源署名與舊式參照標記恢復

2026-10-12 v106，台北時間。沿原 `news-adaptive-capacity-20261009`；中央社不接入、不查key或文件、不請求API／原文。

### 根因、修正與原始值

[修改前實際原件清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v106/before.json)先驗證原正常owner、24啟用來源與最近兩個UTC日期的12份鉅亨診斷，再按requested URL選最新原件、加入先前原件，並核對來源bytes／收據SHA。沒有對publisher重抓來做benchmark，也沒有把source-empty或尚未驗證的圖組當作成功正文。

兩類可修問題在[共用原生parser](../downloader/news_fulltext_publishers.py)實作，不另開下載管線：

- `4937652` 的圖組只有文字來源署名 `文．理財周刊`。精確接受已觀測的純文字strong與style、無巢狀／連結／圖片，要求visible與current有相同有序結構、唯一身分及全篇schema一致。888字原敘事與作者 `余孟軒` 不變；來源署名獨立存為 `same_article_figure_source_byline`，不冒充作者。
- `190469?exp=a`／`232219?exp=a` 的實際current comments含舊式 `<article_table>`／`<article_image>` 參照；visible表示多出對應標記及literal ` -->`，導致原完整比較拒絕。只接受1–16個精確、同序、無屬性、plain text、相同p位置且可見的來源參照；比對用clone移除已核對的標記與精確suffix後，剩下全文須與current完全相同，visible原文仍須與自身整篇schema一致。原1,193／786字正文保留，表格／圖片標記另外存 `same_article_legacy_template_reference`；**不下載或重建被參照的圖片／表格數值**。

這不是全域刪標記或放寬短文：未知形狀、隱藏／重排／新增參照、任意suffix、作者／schema衝突仍拒絕。legacy比較只在原完整visible／current不相等時執行；健康文章不增加第二次clone工作。`html.unescape` 的官方職責是解碼named／numeric character references，不是一般HTML解析（[CPython官方文件](https://docs.python.org/3/library/html.html)）；此處current原件本來外層entity escaped，不能為速度直接刪掉該步。修正依據是以上具SHA的具體回應，不將Python文件當作publisher版型證據。

[來源契約](../downloader/news_fulltext_policy.py)把鉅亨native capture32→33，指紋為 `cc1acacae184b21a988d23a2de4896faae3fe8e43b9a828c32083eb29cf471ee`；不是新授權、批次端點、範圍或限速。其他65 capture、全部66 authorization／allowed／bulk指紋不變，舊資料身分和原始觀測時鐘不回填新版。

### 實物整鏈、測試與部署

[來源署名61項測試](../test/test_news_cnyes_figure_source_byline.py)及[舊參照44項測試](../test/test_news_cnyes_legacy_template_references.py)涵蓋兩種parser／alias、streaming、值與角色保留、錯誤結構拒絕及正常fast path。相關6檔 **362 passed in 3.35s**；最終維護中的完整news pattern **5,439 passed in 72.76s**，包括FinMind／bulk，不是全repository測試或正式訓練驗收。

[最終原件重播／耐久證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v106/original_replay_metadata_corrected.json)接受10項檢查：16份原始回應、兩種parser的成功與拒絕結果一致，三個修復原件加四份先前正文，作者／來源時鐘不變。六次相同原件的claim→cached fetch→完整parse→validate→耐久Parquet／原件／收據→獨立readback，每次接受同7個正文版本。是7份原件的六次隔離流程，不是42篇新文章；0publisher requests／0production queue writes，隔離流程的benchmark clock不作新的正式HTTP觀測或訓練資料。其他零文字／一字／15字頁、未知巢狀圖組仍保留拒絕證據。

首次相關測試用了不存在的檔名，exit4／no tests；首個原件helper猜錯先前JSON欄位，雖六次isolated pipeline已執行，組收據時KeyError，未產出成功結果。原failed run/log都保留；核對既有檔名與實際欄位後以不同run成功驗證。第一份重播收據的legacy-only附註被不精確標為figure-notice；原收據不重寫，修正精確proof metadata後重跑最終5,439測試及上列新10項實物proof，**部署只使用修正後的source bytes與收據**。

[news-only部署收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v106/reload.json)先核對三個成功run與實際source SHA，drain原service／timer至inactive／MainPID0，**08:42:29**恢復單一news owner與原enabled timer；其他金融服務／gateway／交易／金鑰均不更動，未顯式重設provider、host、robots或文章due／attempt／error。[新owner載入證據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v106/loaded_owner_observation.json)於08:43:55核對 `a36f2329118942978fd0e216efc747da`，SQLite與status capture33、request-deferral1、PID／start clock及新鮮heartbeat一致。

### 正常下載、真正403與進度頁

[首個正常runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v106/runtime_final.json)在08:47仍為false／12項通過：基線CMoney尚未持久冷卻，實際發出一個HTTP得到403／`source_access_denied`、零正文，後續來源冷卻到08:58:36；[傳輸原始計數](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v106/transport_final.json)同時確認 `www.cmoney.tw` 的403。不能略掉窗口內新失敗、稱全部健康；**原false收據／failed run不改成功，也沒有解掉來源拒絕**。[綁定判準的工程處置](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v106/runtime_expected_rejection.json)只確認runtime正確拒絕全required-provider宣告；原CMoney缺口不被清除。

[後續08:53 runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v106/current/runtime_final.json)以該已觀測快照作下一個窗口基線，沿既有共用判準僅要求當時可執行的鉅亨／Global Voices增長；CMoney／PeoPo已有未到期source cooldown而排除，backlog仍不完整。13項runtime檢查通過的範圍**不是所有provider成功**。386.296085秒DB窗口新增鉅亨377／Global Voices37個URL heads，鉅亨0.975935head/s；397.431113秒獨立HTTP counter窗口1.401501req/s，窗口不能混用。首次08:47的201.195秒窗口鉅亨267／Global Voices21也保留；不同cohort、不作新舊WAN倍速或官方maximum結論。

[128近期批次](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v106/current/recent_final.json)獨立驗證 **3,774正文版本**，是最近committed批次、非全部lake或唯一正文篇數；[全24來源transport](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v106/current/transport_final.json)、[capacity](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v106/current/capacity_final.json)與[forward recovery](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v106/current/recoveries_final.json)維持readonly：10個實際文章傳輸failure sample中7個已有更晚正文驗收，3個仍未驗證恢復；不是所有歷史故障修復。真正25秒標頭前ReadTimeout、502、CMoney403、未知figure／缺文及混合metadata／body capacity仍在清冊中。

[全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v106/inventory_final.json)與[934鍵計畫](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v106/all_source_plan_final.json)保留66設定／24啟用／868未註冊／882未個別審查，不把規劃或literal alias當正文。[四寬度local網頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v106/dashboard_final/acceptance.json)1440／768／390／320各934鍵／39頁、0JS errors／0publisher requests，沿既有/news共用進度頁；未宣稱外網WAN或全歷史驗收。

[08:55的17項工程closure](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v106/stage_closure.json)接受tested／loaded bytes、source contracts、當前正常body progress、失敗保留與全清冊；它明確仍為 `production_three_target_bodies_complete=false`，保留當時三者尚無正文的snapshot。[08:59的正式786字驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v106/target_first_body.json)另行證明第一篇已由正常owner於08:57:19補回，作者 `鄭淑雲`、原發布／修訂 `2016-02-02T03:33:57.000Z`、兩個原圖片參照、capture33与SHA一致；所屬34版本耐久批次及當前selected head也獨立驗證，未重寫08:55的closure。三個指定URL的最新可考狀況為：

| 目標URL | 原最早重試資格，Asia/Taipei | 原件修復結果 | 正式收據，08:59 |
| --- | --- | --- | --- |
| `232219?exp=a` | 10/12 08:53:15 | 786字／兩個原圖片參照 | 08:57:19落盤；08:59獨立驗證接受 |
| `190469?exp=a` | 10/12 09:06:37 | 1,193字／兩個原表格參照 | 未到原due |
| `m.cnyes.com/news/id/4937652` | 10/12 09:16:25 | 888字／來源署名另外保留 | 未到原due |

[有界唯讀target watch](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v106/observe_target_bodies.py)的確切run為 `v106-normal-target-deadline-read-20261012T004546-49215c35`，08:55已核對同boot／PID／start ticks仍在執行；每15秒只讀原queue/status並寫private觀察。三者真正complete後才用共用selected-body auditor核對原件重播、作者／時鐘／body SHA、capture33及部署之後的實際收據，產出 `target_bodies_final.json`。觀察界線09:31:25不是完成ETA或下載failure，未等到不授權額外restart／強制query；重試資格也不是全域ETA。當前 mutable watch不作immutable stage input，closure只固定其當時readback。

總goal繼續active：先觀察這三個正式收據，再處理其他實際圖組／空頁、25秒／502及CMoney的可用官方替代與授權邊界；保留全934來源清冊、Fed PDF七個舊缺口、882未個別審查與混合capacity工作。沒有清403／429／robots證據、沒有聲稱全provider無錯／全歷史抓完／理論最快。

## 來源閘門等待不消耗下載失敗次數

2026-10-12 v105，台北時間。沿原 `news-adaptive-capacity-20261009` 工作；中央社不接入、不查key或文件、不請求API／原文。

[當前原始快照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v105/before.json)及[24來源傳輸觀測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v105/transport_before.json)先確認舊正常owner仍在下載。原12個傳輸失敗sample有[10個真正forward正文驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v105/recoveries_before.json)；兩個剩餘目標中，一個後來HTTP成功但仍`body_not_found`，另一個當時還沒到重試期限，不能把兩者稱為服務停住。原始「最早due」查詢含robots暫停origin，並非全部可執行工作；未清source期限以製造排程公平性。

### 共用根因與限制

原 `NewsQueue.failed` 將所有recoverable結果都加上同一個指數退避；已達burst上限的工作即使只是provider／host／robots閘門或停止中的等待，也變為 `retry_cooldown`、額外一小時。等待來源規則與真正下載失敗不是同一事件。

[原生HTTP](../downloader/news_fulltext_http.py)現在只在四個已核對的request gate／stop路徑明確標記 `request_deferred=True`；[NewsError](../downloader/news_fulltext_policy.py)要求型別為bool、retry必須True且code屬於四種已知分界，任意timeout、403、429或未知code不能冒充。每個標記是「當前request未送出」；先前redirect／robots請求仍由原HTTP telemetry與provider配額計數，沒有退款或清除。

[共用queue](../downloader/news_fulltext_queue.py)對有該證據的等待保持原burst次數、照原閘門delay再排，釋放原lease；原error、last-attempt觀察、article失敗／完成計數均保留，另加 `frontier_source_gate_deferrals` 說明分界。真正HTTP／讀取／解析失敗保持既有上限、退避、Retry-After與refusal處理。啟動不改任何舊row的due／attempts／error，沒有追溯推定舊等待一定未用HTTP；只新增queue setting及status中的 `article_request_deferral_contract_version=1`，未加全表回寫或新資料索引。本文的嘗試／閘門時間不是成功下載證據。

### 整鏈驗證及實際載入

[27項新測試](../test/test_news_request_deferrals.py)涵蓋四種code×三種已有burst、owner重開保持原期限、真實／legacy錯誤保持原冷卻、無效標記拒絕、四種真正HTTP gate不發request／不造正文，恢復後由受控loopback取得並耐久commit。226相關語義測試及維護中的完整news pattern **5,334 passed in 76.59s** 接受；含FinMind／bulk，不是全repository或正式訓練測試。首次新測試誤把沒有context-manager的NewsQueue當作`with`使用，並猜錯HTTP stop參數，留下23failed／23teardown errors；改用既有close與stop介面後驗證通過，未為測試改production生命週期。首個baseline helper也誤讀未要求body-verification的runtime結果，原輸出保留為觀察；另以fresh128收據生成13項verified baseline，沒有把原failure改成功。

[相同來源的16次ABBA整鏈](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v105/owned_gate_chain.json)核對原queue snapshot SHA，兩舊／兩新×四種gate，全部完成相同claim→HTTP→完整parse→validate→耐久body/raw/receipt→獨立readback。所有正文、原件、作者、發布時鐘及capture signature完全相同；關閉閘門時兩側皆0HTTP。已達5次burst的受控情境中，舊版gate到期後又多3597／3599秒、新版為0。**這是明示logical clock推進的deadline正確性，不是實際等一小時、WAN倍速或官方上限**；兩側都完成正文，未省略較慢側的驗收。

[news-only部署收據](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v105/reload.json)記錄原service與timer正常drain至inactive／MainPID0，07:59:35恢復單一owner；其他服務、金鑰、來源設定、閘門期限沒有改動。[coherent新owner觀察](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v105/baseline_owner_observation.json)確認 `b1622dfb59934605b2844315317330c0`，不是原停止中的heartbeat。

### 正常下載、全清冊與剩餘工作

[08:05 runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v105/runtime_final.json)13項成立，273.092秒DB窗口新增鉅亨 **163**／Global Voices **19** 個正文URL heads；獨立277.924秒HTTP窗口鉅亨1.371req/s。不同workload／窗口不可當matched倍速，正文heads也不是去alias唯一文章或HTTP calls。[128近期批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v105/recent_final.json) **2,874版本／0errors**；[新owner的24來源傳輸觀察](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v105/transport_final.json)及[5／5真正較晚正文驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v105/recoveries_final.json)接受。原owner還未恢復的舊body／其他拒絕不撤銷。正式owner本輪gate-deferral計數為0；載入新handler與受控整鏈證明不等於已在WAN觀察到該分支。

[08:02全清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v105/inventory_final.json)、[934鍵計畫](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v105/all_source_plan_final.json)及[逐列CSV](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v105/all_source_plan_final.csv)保留全部規模：66設定／24啟用／868未註冊／882未個別審查／190個metadata-only來源鍵。known6,202,692／stored3,699,007／等待授權2,130,658是當時literal URL head粒度，不是全網所有新聞或去alias篇數。原3個Fed landing及4個image-only PDF仍列在before，尚未宣稱完成OCR／替代全文；全來源歷史及global ETA未知。

[四寬度local共用進度頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v105/dashboard_final/acceptance.json)1440／768／390／320，各934鍵／39頁與required來源、原網址進度及review正常，0publisher requests；非WAN驗收。[08:07 immutable階段closure](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v105/stage_closure.json)13/13接受，全部66 capture／authorization／allowed／bulk不變，原v104 handoff／所有失敗證據保留。

仍有真正25秒標頭前ReadTimeout、502與較早parser缺文。接著需分清metadata／body混合capacity窗口，減少不能產生新正文的重複discovery工作而保留必要追新／更正；同時續做其餘來源的端點、政策、歷史及free／author／clock驗證，不以提高RPS或只剩這一小類為完成條件。原所有來源／全部歷史目標不縮小，中央社維持暫緩，總goal **active**。

## 鉅亨社群圖組原文恢復與主迴圈有界維護

2026-10-12 v104，台北時間。中央社依最新指示不接入、不查文件／金鑰、不請求原文或API；既有註記保留。

### 原始錯誤、修復與正式下載分開驗收

[原始快照](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v104/before.json)保留原件、queue head、來源指紋及前次closure SHA。兩個實際錯誤有不同原因：

| 原始目標 | 來源事實與共用修復 | 正式結果 |
| --- | --- | --- |
| `4778922?exp=a` | 原文1,756字；一個figure含圖片、四個純文字段落及三個社群連結，整篇schema表示不同。新增精確原生圖組identity，只接受已觀測的有序形狀及同篇current／visible／完整schema、free／clock證明。附註128字與原始連結獨立保存，不下載圖片或請求社群服務。 | 正常owner在 **07:09:29.155675** 真正補回；body SHA `842c3b3ad54dcf02cb549a7192c62508be1554a8b3f69f1dc2779ae6ca09ba0f`，原件、耐久批次、當前parser與queue head全部接受。 |
| `1673421` | visible／current僅26字、schema22字；是 `(GLT) 20.19  1.08(5.65%) :` 行情片段。只從真實可見分隔符辨識無符號正變動，不猜schema連寫數字如何拆分；沿用整篇identity、clock、free與短文證明，分類 `source_body_quote_only`，不生成正文。 | 07:24仍是舊 `body_not_found`／retry；07:36精確queue readback確認正常排程在 **07:26:30.716492** 已正式分類為 `source_body_quote_only`，body為null、due為0，不再重試。沒有直接SQL改成成功、強制HTTP或宣布來源空稿。 |

[共用解析器](../downloader/news_fulltext_publishers.py)保留健康正文fast path；未知／隱藏／異篇／額外文字或attributes／paid／不完整schema仍拒絕，只有完整證據的圖組才能宣稱已驗附註。Cnyes HTML capture **31→32**、quote-only contract **4→5**；全部66來源authorization／allowed／bulk及其他65 capture不變。新Cnyes capture FP為 `dfea2e146ab9ed9e08f0dc778958dee3753a78640d71021c2a9bfdebd0dce99d`。

[六份真實原件／六次整鏈耐久重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v104/original_replay_corrected_source_attribution.json)接受10/10 guard；兩種parser各三次claim→extract→validate→耐久commit→完整readback，每次只接受四篇真正正文。既有762／671／2,630字SHA不變，缺文負例仍拒絕。初次重播錯把來源作者預期成新聞中心，原拒絕保留；實際為 **Anne陳巧諭**，發布／修訂為 **`2021-12-01T09:42:56.000Z`**，依原件另出驗收，不更改原件。[正式單篇current-source驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v104/formal_article_current_source.json)確認正常owner真正取得；[行情片段觀察](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v104/formal_quote_outcome.json)明列 `formal_source_outcome_confirmed=false`，未冒充正式成功。

### 不刪驗證的有界盤點與精確索引

現場有界stack觀測顯示主迴圈停在 `_inventory` 的 `lstat`／`stat`，目錄含 **115,616項**。原路徑完整盤點、對已知envelope重查檔案並嘗試重寫索引，不能在中途讓健康HTTP結果回到主迴圈。[共用replay](../downloader/news_fulltext_replay.py)現在保留每owner的scandir游標，每片最多4,096項／合作式50毫秒目標、metadata批最多256名稱，整輪完成才更新hourly clock；停止／關閉釋放游標，API FP變更重新盤點，精確已知job不再stat／INSERT。新檔仍核對symlink、來源／指紋／digest；正文replay的原件、授權、SHA、clock與耐久驗證不刪。filesystem單次阻塞不能硬性搶占，50毫秒不是絕對deadline。

[queue](../downloader/news_fulltext_queue.py)新增 `metadata_order_repair` partial index，僅包含未修補日期且raw date符合原predicate的rows；UPDATE predicate、值與語義不變，已修補的數百萬rows不再每次掃描。首次安裝仍須掃表，必要acceptance recovery仍保留。中間owner實際啟動97.808秒，其中acceptance recovery65.395秒、policy reconciliation24.932秒；**不能把全部啟動歸因於盤點，或宣稱啟動降至15毫秒**。

[同快照ABBA](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v104/inventory_benchmark.json)使用相同115,616項與唯讀current-API checkpoint，只在隔離in-memory SQLite操作，沒有producer DB寫入或publisher請求。前後全部job metadata／priority／state／clock hash一致；舊版兩次wall／CPU為1.679／1.678秒及1.428／1.427秒，新版整輪0.286／0.286秒及0.292／0.292秒，29片，最長單片 **12.53／14.78毫秒**。兩側同樣排除快照列舉成本，只證明metadata工作減少／主迴圈讓步，**不是同cohort WAN或全正文鏈路倍速**。

[15項新切片／索引測試](../test/test_news_cached_inventory_slices.py)涵蓋續接／完成、stop／owner restart、API變更、無重複stat／寫入、symlink／無效raw不當正文、參數guard與SQL plan／row-value一致。838解析／時鐘、272恢復／效能相關及維護中的完整news pattern **5,307 passed in 73.73s** 通過；包含FinMind／bulk，不是整個repository或訓練／GPU測試。初次錯預期fixture的modified clock及猜錯test檔名造成的失敗也保留，以正確source語義與完整回歸另行supersede，不刪失敗證據。

### 實際owner、網頁與剩餘問題

只重載 `stockagent-news-fulltext.service` 與其timer，沒有重啟金融／模型服務。07:21:49最終owner `e4035613fe934f079caa085de2af61b1` 載入source32與有界maintenance，timer enabled／active。較早07:06:41仍讀到舊owner heartbeat，保留為啟動觀察，未當成驗收；[07:23 coherent baseline](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v104/runtime_baseline_final.json)與[07:25 runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v104/runtime_final.json)才確認新owner／source bytes／capture FP及13/13 guard。正式1,756字在前一個source32 owner取得，head與原件不變，不因效能reload重複下載。

[07:28不可改寫階段closure](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v104/stage_closure.json) **13/13接受**：正式owner完成115,616項全輪盤點、57,808已知envelope跳過、0新索引、29片，最長 **29.384毫秒**，總wall／CPU0.560／0.554秒；live SQL planner使用精確partial index。146.940秒DB窗口新增鉅亨 **86**／Global Voices **12** 正文URL heads，鉅亨0.585head/s；另138.228秒HTTP窗口鉅亨1.317req/s。正文heads和request counters不同，也不是去alias唯一篇數、同cohort倍速或官方最高速。

[128近期批驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v104/recent_final.json)2,727正文版本／0errors；[四寬度local唯讀頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v104/dashboard_final/acceptance.json)1440／768／390／320、934鍵／39頁，進度與review正常、0publisher requests。頁面不另打來源API填顯示；這是local gateway，非WAN或全歷史驗收。

[07:36唯讀交接驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v104/handoff_final.json)9/9通過，同一正常owner仍active，正文與source bytes不變；新鮮quote head已是 `source_body_quote_only`、last_checked為07:26:30，無正文，不再輪詢此非正文目標。這項較晚結果取代「尚待正式分類」的工作狀態，原07:24觀察與07:28closure中的舊quote快照仍保留，不改寫為事後已成功。

[07:38正式分類與文件交接](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v104/handoff_final_quote_confirmed.json)另10/10接受：原件的current parser分類證明、正常queue較晚nonbody結果、source32 bytes、既有正式正文、服務時鐘與全部文件連結一致；只作唯讀核對，0publisher requests／0queue writes，沒有把行情片段計入已取得正文篇數。

仍有實際標頭前約 **25秒ReadTimeout**、mobile `RemoteDisconnected`／連線錯誤與較早502；自適應仍 `recovering_transport`、0.5秒間隔／parallel2，未清拒絕或強行30RPS。下一步追transport恢復正文收據及最舊parser retry輪候時間，續做934來源尚未審查的端點／存取／歷史工作。66設定／24啟用／868未註冊／882未個別審查規模未變；未產出新版全來源known／stored inventory，前stage數字不得當成最新。所有raw與failed runs保留，中央社暫緩，總goal **active**；全錯誤、全provider、所有歷史及numeric official maximum仍未完成。

## 官方市政新聞原生接線與初次凍結競態修復

2026-10-12 v103，台北時間。中央社不查文件、key、原文或API，既有授權註記保留；所有工作仍屬原`news-adaptive-capacity-20261009`。

### 從來源事實到可接受的正文

| 分界 | 實際根因與修正 | 不改動的界線 |
| --- | --- | --- |
| 台中機關署名 | 原通用parser無法確認原生聯絡欄位。由`main#center`、新聞9962文章ID、標題、發布日及已核對機關／事務所驗證；明確分開單位與聯絡人，含無空白的`民政局張小姐`及來源原寫`台中政府運動局`。 | 來源拼字不更正，聯絡人不當記者；聯絡電話／QR／圖說不插入正文，原HTML完整保留於私人原件。 |
| 同篇HTML變體 | 後續37篇HTTP200原件均不宣告canonical。不能因這個缺省就判來源不存在；新增只接受原生市政新聞麵包屑、同篇OG ID／標題及Facebook、Twitter、LINE三組精確分享URL指標一致的版型。 | 只解析本站宣告的指標，完全不請求社群URL；有canonical時仍須一致，缺省不偽造，重複／異篇／隱藏／外站指標均拒絕。台中native capture1→2，鉅亨仍HTML31。 |
| 彰化發布期間 | 實際原文無canonical／OG URL／NewsArticle；以原生form、breadcrumb、print三組bull_id、標題、發布單位及完整正文驗證。 | `2023/11/08`依可見header保留；`2023/11/08~2023/12/08`另存來源期間，沒有補秒、推時區、以期間終點替代發布日或下載受robots限制的照片。 |
| 初次範圍凍結 | timer在服務停止後又啟動owner；停止／drain前有1筆台中原始URL已被正常router入列。舊CLI constructor在39筆明確網址寫入前先把該1筆凍結。 | 凍結收據是事實，不能重設成38或抹除。現在queue僅對明確enqueue／fetch延後「新範圍」初次凍結；既有凍結、拒絕、重試期限及lease保持原樣。 |

兩站的原件／實際頁型與政策已在前stage取得；[臺中主站說明](https://www.taichung.gov.tw/10179/10185/13599)、[彰化主站說明](https://www.chcg.gov.tw/ch2/copyright.aspx)只作各自範圍的依據，不繼承至其他網站、第三方作品或照片。新provider只接這39個已知原始連結，不靠猜文章ID擴充。既有新聞服務、queue、robots、配額、failure與私人收據共同處理，沒有另開常駐爬蟲。

### 實際執行、失敗證據與恢復

先保留[v103原始owner／64來源／39 literal連結](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v103/before.json)與[5個真實原件的六次耐久重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v103/original_replay.json)。首輪275相關測試及5,154完整news回歸接受，但這些不代表所有38種實際頁型已測完。正常HTTP review後37筆canonical缺省拒絕全部保留於原report；修正後[完整37篇六次重播](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v103/all37_original_replay_native2.json)一致接受31,088字，沒有縮小集合或用摘要填補。

本次短暫停掉且還原的只有`stockagent-news-fulltext.timer`及`stockagent-news-fulltext.service`。只停service不足以保證無owner；第一次catalog檢查證明timer在06:06:53再啟動，第二次讀到尚未drain的`deactivating`也正確拒絕。後續確認timer inactive、service inactive／MainPID0才作明確入列。因1篇已在drain前被router加入，原始台中1篇及彰化1篇凍結收據保留，其餘37篇以16／16／5篇建立三張不可變追加收據。

直接materialize舊review原件的路徑也保留失敗：修訂後的授權scope快照與原HTTP取得時的`authorization_json`不同，共用guard於claim／正文accept前拒絕；**沒有改原件授權、降低guard、回填成成功，或再標示零請求重用**。改為[共用明確scope核准](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v103/scope_approval.json)，由正常owner在目前設定下重新HTTP取得。失敗只屬當時路徑，不是永久放棄37篇；初始凍結收據及舊觀察時間未變。

[39筆正式收據逐筆驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v103/formal39.json)：38台中＋1彰化，34,087字；全部在06:27:59新owner開始後由正常下載取得。分16／16／7選取，沿用共用原件、完整body／title／author／clock／caption／notice與所屬完整batch verifier；當前capture fingerprint全部一致，不是只讀私有probe作正式成功。[所有有限範圍驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v103/finite_after.json)另確認台中1原始＋37追加、彰化1，三追加scope均37／37正文complete，沒有把其他來源仍缺文說成完成。

修復後257項相關語義回歸與**5,181完整news含FinMind／bulk，74.07秒**通過；包含缺省canonical的異篇／隱藏／重複／外站指標、單位／聯絡人、日期精度、整批初次凍結與既有收據不可改。不是整個repository或正式訓練測試。[四寬度local唯讀頁](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v103/dashboard_final/acceptance.json)1440／768／390／320，934鍵／39頁與兩個新provider正常，0publisher calls；不是WAN或新建另一個dashboard。

### 速度、剩餘錯誤與下一階段

[新owner runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v103/runtime_final.json)13 guard通過；[128近期批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v103/recent_final.json)共4,054版本／0errors，[24來源傳輸觀測](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v103/transport_after.json)與[2／2較晚正文恢復](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v103/recoveries_after.json)接受。這兩個sample不是所有舊失敗都解決。新owner有真正502／transport錯誤，正由既有隔離／退避／逐窗校準恢復；不清reset拒絕或宣稱只提高RPS就能使來源正常。

鉅亨速度不可混用前後owner：06:03的2,195.970秒DB窗口新增2,418個URL heads／1.101head/s，HTTP另2,200.112秒窗口1.612req/s且無403／429；重啟後06:34的298.273秒窗口新增141／0.473head/s，HTTP0.858req/s，三個最近capacity窗口有502及其他transport errors，狀態`recovering_transport`。這是不同窗口／負載，**不是倍速實驗、官方上限或無錯誤聲明**；Global Voices同期新增30正文heads，來源獨立仍在前進。

[兩筆真實鉅亨殘餘原件](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v103/residual_cnyes_evidence.json)已保留完整私有SHA、當前head、原期限及版型診斷：`1673421`的visible／current26字與schema22字空白正規化雜湊相同；`4778922?exp=a`的visible／current1,756字與schema1,676字不同。接著優先修這兩種表示／圖說或notice語法與實際傳輸故障，再量測完整成功正文鏈路；不得把缺文宣告成來源空稿、插入標題冒充正文、刪數值以達成一致，或改出歷史發布時間。

06:53的[不可改寫階段驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v103/stage_closure.json)14項全部通過：原64來源fingerprint／授權／allowance／bulk不變，39正式heads和原始兩份1篇cohort收據未改，timer恢復enabled／active，當前owner與載入原始碼一致。[新鮮128批讀回](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v103/recent_closure.json)及[runtime](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v103/runtime_closure.json)另接受；1,452.207秒DB窗口新增鉅亨973正文heads／0.670head/s，獨立HTTP窗口1.071req/s，Global Voices新增144。這是含錯誤與等待的當時實際吞吐，不是固定最大值。原7筆失敗run各自追加工程處置，未把失敗改成成功或接受舊授權原件。

[全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v103/inventory_final.json)與[934鍵工作清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v103/all_source_plan_final.json)：66設定／24啟用／868未註冊／882未個別審查、190個metadata-only來源鍵完整保留。觀測known6,202,662／stored3,695,369，仍隨正常下載變動。官方RSS一次500／10筆只證明批次回傳；台中JSON歷史／分頁／完整正文／日期／配額契約未驗證，不能直接全啟用。全來源全部歷史、global ETA、numerical official maximum仍未知；中央社維持暫緩，原總goal active。
