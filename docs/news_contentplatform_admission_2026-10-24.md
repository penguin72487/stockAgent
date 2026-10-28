# 報新聞：已知網址的正文接線

## 10/26 晚間續辦：75／75 個已知網址的正文補齊

沿用下節已完成的 68 篇報新聞、40 篇 CMoneyLearning 與 497 筆結案問題，只處理原來 7 個缺文。
v6 新增 **3 篇**、v7 新增 **2 篇**、v8 新增 **2 篇**；目前 **75／75 有正文，這份已知清冊剩餘 0**。
下節的 68／75、7 個缺口是較早快照，不再是目前待辦。

- v6 支援書寫包裝中的圖片，保留父節點與前後文字；v7 依實際原件補上 `hr` 與無 WordPress class 的圖片。
  CSS class 不作文章身分證據，完整書寫內容、literal canonical／mainEntity、作者與發布／修改時鐘仍需驗證。
- 最後兩個缺口只提交一次新的有界結構問題 55；均通過原生身分、作者與時鐘，失敗位置分別是空元素與 script。
  兩題已結案，合計 **499 筆結案問題**，沒有重送舊 43／44／45／53 或重新啟用 55。
  新問題僅保留有限結構資訊，不把診斷算成正文或保存新私有原件。
- v8 只排除無文字／子節點且沒有資源、事件、自訂資料屬性的空葉元素，實際原件是空 `aside`；
  script 按 [WHATWG 的元素語意](https://html.spec.whatwg.org/multipage/scripting.html#the-script-element)排除程式碼，
  不執行、不抓 `src`、不解碼資料區塊。書寫父節點與兄弟文字保留，未知的書寫、隱藏或嵌入內容仍拒絕。
  完整性明確限定 **靜態 HTML 書寫內容**，不宣稱已取得執行 JavaScript 後產生的內容。
- **898 項相關測試通過**；與之前 785／816／845／860 的重疊測試不相加。
  v8 上線前以兩個 parser 離線讀回全部 **73 份既有原件**，正文與既有值保持，只有完整性說明明確化，**0 次來源 HTTP**。
  正式版本、owner、PID、DB／status 與程序時鐘核對後，只提交最後兩個缺文。
- v8 的兩篇保留 **496,285 raw bytes、7,253 UTF-8 正文 bytes**。
  原始回應、正文、Parquet 與耐久收據 SHA、完整書寫 DOM、作者及發布／修改原字串均逐項讀回，兩個 parser 的全部欄位一致。
  在同一份新入庫原件上重現 v7 的失敗，**0 次額外 HTTP**；原有 **113 篇正文與檔案、499 筆結案**保持。
  來源授權、容量身分、原 REP／冷卻與每日扣額保持，交易／報價服務未重啟。

公開狀態 API 已讀回報新聞 **75／75、known_queue_clear、此已知範圍 ETA 0**，CMoneyLearning **40／40**。
來源審查與本次收據已更新；90 個已註冊 provider 與 951 個來源列是不同分母。
本階段補齊這 75 個已知網址，**全站歷史範圍、最大穩定 RPS 與全歷史 ETA 仍未證明**；本次沒有新的全鏈路速度比較。
鉅亨仍優先，其 **187 個活動缺文 URL**沿原 AMP robots HTTP 502 的五分鐘恢復時鐘續辦；
186 個已有其他主頁候選，不能據此把不同網址合併成同一正文。CNA 延後，整體新聞目標維持 active。

證據：[v6 新增 3 篇](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_wrapped_media_v6_20261026/readback.json)、
[v7 新增 2 篇](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_written_html_v7_20261026/readback.json)、
[最後兩個結構問題](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_failure_shape55_20261026/readback.json)、
[73 份原件離線比對](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_static_document_v8_20261026/offline.json)、
[v8 正文與收據讀回](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_static_document_v8_20261026/readback.json)、
[原失敗重現](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_static_document_v8_20261026/root-cause-readback.json)、
[正式載入](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_static_document_v8_20261026/runtime-admission.json)、
[階段結案](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_static_document_v8_20261026/completion.json)、
[公開審查與交接](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_static_document_v8_20261026/handoff.json)。
fixture 初始化及 v7 診斷清單遺漏曾被測試擋下，修正後才上線；負向收據保留，沒有清除失敗來製造成果。
最後交接驗證器曾將 JSON 清單與 Python tuple 直接比較而誤判設定變更；型別比較已修正，
實際設定 SHA、revision 與 owner 均保持，同一服務的後續公開讀回通過，原失敗收據保留。

## 10/26 18:28 續辦：正文 15 → 68，只剩 7 個缺口

先沿用 CMoneyLearning 40 篇、報新聞 15 篇及 497 筆已結案問題；既有正文、原件及收據未重抓。
前述「4 個結構／署名、56 個範圍」是舊版觀測，現在以 **68／75 篇有正文、7 篇解析未通過** 為準。

- 重新核對[官方著作權說明](https://www.contentplatform.info/copyright/)的個人非商業下載條款。
  平台帳號署名、文內記者、公司與來源聲明是不同的原始宣告，不能因未使用單一「記者／綜合報導」格式就排除正文。
  v4 仍驗證 literal canonical／mainEntity、原生標題、可見作者／Person／profile、發布與修改時間及存取條件；
  只重排 60 個當時缺文項目，新增 **9 篇**。這不是著作權歸屬、開放授權或自動訓練／匯出的判定。
- v5 依兩份實際保留原件支援可見巢狀 `div` 與 WordPress 圖片／圖說結構，保留所有書寫文字，
  圖片和圖說明確排除；正文容器內原有的相關連結文字仍保留。未知結構、隱藏內容與存取限制維持失敗。
  **2 份原件沿共同耐久入庫流程重解析，0 次 HTTP、沒有另複製 raw**；
  只有其餘 49 個無原件缺文交給原下載服務，取得 **42 篇**，本階段共新增 **44 篇**。
- 保留原件 ABI 5 限定精確 v4 → v5 同授權轉換，核對舊 head、唯一新增失敗 attempt、owner、原件與 sidecar SHA。
  HTTP 收到回應與稍後寫入解析失敗是兩個時間；兩者皆保留，不能回寫成同一個時間或覆蓋後來的拒絕／租約／重試。
  原件未保存 redirect chain，紀錄明確標為未保存；沒有捏造 transport 證據。
- **785 項整合測試通過**，涵蓋既有原件、較新拒絕、租約、deadline、收據中斷恢復與舊版解析；重疊測試不相加。
  v5 的 44 篇逐項讀回：**10,840,660 raw bytes、176,997 UTF-8 正文 bytes**，含兩份重用原件，不能算成新增網路流量。
  原始回應／正文／Parquet／耐久收據 SHA、完整書寫 DOM、作者與發布／修改原字串、兩個 parser 的全部欄位均核對。
  24 篇既有報新聞、40 篇 CMoneyLearning、497 筆結案及交易／報價服務保持；來源容量身分、REP、原冷卻與每日診斷扣額未重設。

剩餘 **386719、418473、477100、494106、512881、538855、547111** 皆為 `article_identity_not_verified`；
這是尚未解決的解析／身分驗證結果，不宣稱文章不存在、永久損壞或已抓齊。
後續只處理這 7 個缺口，不重排 68 篇完成正文，也不重送已結案的 43／44／45／53 問題。
公開網頁已核對 68／75；此分母是目前已知網址，非全站歷史。最大穩定 RPS、全歷史範圍與全部完成 ETA 尚未證明。
鉅亨仍優先，其 187 個活動缺口依原 AMP robots HTTP 502 恢復時鐘等待；CNA 延後，整體目標維持 active。

證據：[v4 新增 9 篇](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_native_scope_v4_20261026/readback.json)、
[v5 原件入庫](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_native_geometry_v5_20261026/retained-admitted.json)、
[44 篇正文與收據讀回](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_native_geometry_v5_20261026/readback.json)、
[正式載入與時鐘](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_native_geometry_v5_20261026/runtime-admission.json)、
[公開進度](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_native_geometry_v5_20261026/public-status.json)、
[完成及未完成清單](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_native_geometry_v5_20261026/completion.json)。
測試曾誤用 fixture 預設網址、混淆全域 User-Agent 與來源指紋、要求兩件耐久操作一定在同一個 0.1 秒切片完成；
修正測試後才准入。失敗收據保留，准入被擋時沒有停止服務或發出文章請求。

## 10/26 續辦：只查 4 個未完成署名關係，不重抓完成正文

既有 CMoneyLearning 28 篇、報新聞 15 篇及 480 筆已結案問題已核對保持。
CMNews 的 28 個身分缺口另按確切來源連結清點，15 個宣告原站已有正文、13 個仍缺確切連結；
這與 CMoneyLearning 的 28 篇正文不是一對一計數，沒有接受 CMNews alias 或重新下載原件。

本次只向原新聞 owner 提交 386719、418473、475631、538855 的固定新問題 evidence 53：
第一個書寫段落是否以相符原生記者署名開頭，包含署名和正文同段的情況。
只允許穿過有界透明 div、圖片與無文字的單圖 figure；不跨過前文、圖說、未知包裝、隱藏內容或來源存取條件，
不在後文搜尋方便匹配的姓名。舊 43／44／45 保持結案，正式正文 parser 與來源身分不變。

[實際結果](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_prefix_20261026/readback.json)
為 4 次 HTTP 200、980,039 response bytes；三篇到達首個 p，一篇停在 figure，均未確認相符前綴。
這些新問題已全部回答並結案，合計 484 筆，不再重送 53；新私有原件與正式正文均為 0。
仍是 **15／75 篇有正文、4 筆結構／署名及 56 筆範圍未解**，不判定來源永久損壞或不存在。
後續只能取不同的原生署名／figure 宣告或可驗證原文來源關係，不能把這次的否定答案改算成功。

517 項相關測試通過，涵蓋同 owner、重啟不重送、前置收據再驗、原冷卻／robots、完整原件獨立讀回與舊資料保持。
首輪測試模擬重啟時未關閉舊 fixture owner，修正測試後通過；控制器另誤把成功 log 中的 fixture 名稱當成失敗，
在尚未重啟前被擋下，已改用 exit receipt 與最後精確 pytest 統計行。失敗紀錄均保留。
只實際重載新聞一次；[正式 owner／資料與公開進度](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_prefix_20261026/completion.json)
已核對，原正文 SHA、診斷扣額、來源政策與交易服務未變。此階段沒有全鏈速度或最高 RPS 的新宣告；鉅亨優先，CNA 延後。

## 10/25 00:15 接續：完成項目不重排，診斷共用一次解析

先核對 CMoneyLearning 28 篇、報新聞 15 篇與原 432 筆結案；上述 43 篇正文與檔案 SHA、
每日診斷扣額及交易服務保持。只提交尚缺正文的新問題，沒有重做覆蓋索引或 legacy 身分探測。
報新聞仍為 **15／75**，缺 **4 筆結構／署名證據、56 筆目前範圍未通過**；本輪沒有新增正文。

- 538855 的[首圖後書寫段落診斷](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_image_credit_20261024/readback.json)
  已結案：原生身分／作者／時間框架通過，首圖之後是 `p`，但整段署名未符合現有規則，沒有保留原件。
  evidence 45 沿用同一 owner／精確網址／一次性 key，不重開已完成的 43／44。
- 其餘三篇從未提交過原件問題，沿既有 evidence 43 各查一次，均停在原生 header。
  [初次讀回](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_remaining_originals_20261024/readback.json)
  與[不同根節點問題的讀回](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_remaining_roots_20261025/readback.json)
  均保留：386719 首個候選為 `div/div/figure`；418473、475631 為 `div/p`，署名仍未符合現有規則。
  這些是結構／署名未解，不能判定來源永久錯誤；三篇均未保存原件或正文。
  本輪共七次新文章請求，合計 **439 筆已結案診斷**，不重新送出。
- 修正診斷本身的重複工作：44／45 原先先解析 shape、再重新解析同一份回應確認 permit；
  現在由同一次解析回傳 facts 或附在拒絕上。私人原件寫入時的獨立驗證仍保留。
  初次 header 失敗也可在同一 DOM 留下有限的前導節點、可見性與署名前綴關係；
  不輸出姓名／正文／未知 HTML tag 字串，不把前綴匹配當作准許，不增加來源請求或重開舊問題。
- [460 項相關測試](../artifacts/operations/agent-workflow/runs/news-cp-single-parse-verified-te-20261024T161201-1af8e87a/run.json)
  通過，包含雙 parser、一次解析、隱藏內容、首段不得跳過、已完成問題與原件採納保持。
  最初測試命令的檔名錯誤紀錄保留並已更正，沒有列為成功。
- [已保存的 239,555-byte 原件離線 AB／BA](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_single_parse_20261025/offline-cost.json)
  使用原 canonical metadata path，reference 額外執行原本獨立的同回應 probe；完整 metadata 相等。
  evidence 44 wall 中位 **0.136908 → 0.062959 秒**，45 為 **0.151524 → 0.073808 秒**；
  CPU 分別 **0.136530 → 0.062914**、**0.151300 → 0.070887 秒**。
  這是離線診斷邊界成本，0 次來源 GET／0 次 queue 寫入，不是 HTTP 吞吐、冷啟動或全鏈路極限的證明。
- [原服務與本機／公開狀態 API 驗收](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_single_parse_20261025/ready.json)
  於 10/25 00:23 通過，同一 owner `51e95f832fd24aa1b1f947102b39fdc1`／PID 565030、NRestarts 0。
  載入 policy、owner／資料庫、快照時鐘及程式版本均相符，兩端 fresh／ready。
  本次首份快照為 **208.433 秒**、核對 **221,601 份收據**；條件不同，不用它與前次 107.840 秒直接比較提速。
  初次唯讀驗收超過外層 45 秒而失敗，未因此重啟下載器；
  [受監督的後續核對](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_single_parse_20261025/audit-retry-accepted.json)
  20.143 秒完成，開始／結束同 owner、PID 與 fresh 時鐘另外驗證。原失敗紀錄保留。
  面板當時鉅亨有 3,685,828 個正文網址，187 個活動缺文仍依原來源政策截止時間暫停；
  這是索引／快照計數，不是本輪對數百萬份正文重新做完整 payload 驗證。

接續只處理鉅亨原缺文與來源恢復、報新聞尚缺的結構／署名證據及收據恢復成本。
正式文章 parser、source／authorization fingerprint、每日扣額與舊結案 key 均未擴張；CNA 維持暫緩。

## 23:34 接續：只查新根節點問題，完成資料與舊診斷保持原狀

沿用已完成的 CMoneyLearning 28 篇、報新聞 15 篇、431 筆結案與鉅亨覆蓋索引。
只檢查上次 576 筆鉅亨不支援路徑清冊後新增的兩個網址：兩者都是 `twstock/.../company/...`，
沒有新的新聞身分證據，**0 次來源請求、0 個 alias、0 次 queue 寫入**；不重跑舊 32 筆 legacy probe。
這份增量清冊及原資料／source identity／每日診斷扣額基準保存在
[本輪基準](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_nested_root_20261024/before.json)。

538855 的舊原件問題已結案，但未保留正文。本次另問「第一個巢狀書寫節點是否有相符署名」：
新增 evidence 44，只有同一 head、來源授權與已結案的未保留 header refusal 才能提交及預留。
新 key 不依理由、parser hash 或重啟重開；不是重送舊 evidence 43，也不補回原每日扣額。
同時修正新安裝尚無診斷表時的 SQL 例外。最新
[430 項相關測試](../artifacts/operations/agent-workflow/runs/news-cp-root-verified-tests-20261024T152807-2ffc805a/run.json)
通過，包含其他來源共用流程、原問題保持結案、保留原件與非原 header 拒絕不得再次送出。
初輪測試的 fixture／缺表失敗紀錄保留，沒有當作成功結果。

- [唯一 owner 與兩端頁面驗收](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_nested_root_20261024/ready.json)
  為 `6c368a3641ed471bb26edc0221a2afe2`，載入版本、快照時鐘、資料庫 owner、程式檔身分均通過；
  本機及公開 HTTPS 均 fresh／ready。新程序起至首份快照約 116.618 秒，不宣稱冷啟動提速；收據恢復仍待改善。
- [實際診斷及讀回](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_nested_root_20261024/readback.json)：
  只由原 owner 請求缺文網址一次，HTTP 200、247,239 bytes、HTTP 4.913 秒、parse 0.208 秒。
  原生 canonical／作者／時間框架通過，但正文直接子節點為一個 `div`，其首節點為 `img`，
  因此這次沒有確認第一個書寫段落的署名，**沒有保留原件或新增正文**。
  這是尚未支援的來源結構證據，不能判定來源本身永久錯誤，也沒有接受後面碰巧相同的名字。
  舊 431 筆結案、43 篇已完成正文與 payload SHA、每日扣額及交易服務均保持；新增的這一筆也已結案，合計 432 筆。

報新聞維持 **15／75**，仍缺 **4 筆結構、56 筆署名／範圍**。
後續只續辦首圖後書寫節點的新證據、其餘缺文與收據恢復；不重送已結案的 evidence 43／44。
鉅亨原缺文重試維持最高優先，本輪讀回時面板顯示 3,685,822 個有正文網址，仍有 187 個活動缺文受來源暫停限制。
計數是目前索引／快照，不是全歷史完整或每個新批次已獨立讀回的宣告；來源最大穩定速率與全部完成 ETA 仍未知，CNA 暫緩。

## 22:41 接續：重用已取得原件，正文 14 → 15，不重排完成項目

先核對已完成的 CMoneyLearning 28 篇、報新聞 14 篇及 431 筆已結案診斷。
原有 head、正文檔案 SHA、每日診斷扣額均保留；只有正常發現的 `last_seen_utc` 可單調前進。
本次只採納仍缺正文的 `378180/jaya378180/`，目前為 **15／75 篇正文、4 筆結構及 56 筆署名／範圍未解**。
不是整站歷史完成，也沒有把排程佇列為零當成沒有缺文。

- [兩筆 resident 原件問題的既存收據](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_resident_originals_20261024/readback.json)
  已完成，不再送出。378180 保留了原生署名、明確發布／修改時間及精確同網址原件；538855 在正文沒有直接 `p` 的身分檢查處失敗，沒有原件可重播，仍須另一份具體結構證據。
  同一問題的 key 不隨理由、parser 版本或重啟改變，不新增第二個 HTTP／SQLite owner。
- [離線完整比對](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_retained_h3_v3_20261024/offline.json)
  找到一張原生 `wp-image-*` 圖片包在純圖片 `h3` 中。v3 只新增這個已觀察版型；含文字、未知包裝、多圖或隱藏內容仍拒絕。
  七段來源文字依既有「文字節點間換行」規則與獨立 lxml 完全一致，913 字元、2,325 UTF-8 bytes；兩套解析器的全部欄位相同。
  原先以段落合併比較的唯讀驗收失敗另保留並更正，沒有改變原始文字輸出規則或略過差異。
- [正式採納及耐久批次讀回](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_retained_h3_v3_20261024/readback.json)
  沿用 resident materialization ABI 2、原始 parquet 與 sidecar／job SHA，**0 次來源 GET**。
  保留原件實際觀察時間、原始 bytes、署名與發布／修改值；未保存的授權 JSON 與 redirect hop 陣列仍如實標示未保存，沒有重建成歷史 PIT。
  舊 14 篇、CMoneyLearning 28 篇、431 筆結案及其餘 60 筆失敗 head 均未重排。v3 不會在啟動時整批重試其他 identity 失敗。
- [唯一服務、載入版本及本機／公開 HTTPS 驗收](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_retained_h3_v3_20261024/ready.json)
  為同一 owner `1d4cb10a4793471f883a612f5482b018`，均 fresh／ready，顯示正文 **15／75（20%）**、尚缺正文 **60**，其中待修結構 4、署名／範圍 56。
  owner 開始至首份已發布快照為 **190.200 秒**，其中收據恢復 221,532 份為 107.481 秒、後續首輪範圍維護為 74.974 秒；
  本次本機 materialization 完整命令耗時 **191.109 秒**。這些巢狀階段不能相加，190 秒不能全歸因於收據 I/O。
  後續只針對尚未完成的初始化成本加入覆蓋索引，正文仍為 15／75；實測及新 owner／兩端頁面驗收見[啟動範圍優化](news_startup_scope_optimization_2026-10-24.md)。
- 最新修改分別通過
  [262 項針對性測試](../artifacts/operations/agent-workflow/runs/news-cp-v3-exact-upgrade-tests-20261024T142623-db303f30/run.json)、
  [1,183 項共用回歸](../artifacts/operations/agent-workflow/runs/news-cp-v3-shared-current-tests-20261024T142738-849c42b6/run.json)
  及 [213 項正式設定檢查](../artifacts/operations/agent-workflow/runs/news-cp-v3-installed-scope-tests-20261024T143525-6898f38f/run.json)，範圍重疊不相加。

接下來只續辦上述 4／56 筆缺文的新證據及收據恢復成本；覆蓋索引已完成，不再下載 378180、重做純圖片段落／標題修復或重送 431 筆已結案問題。
鉅亨保持優先並已恢復正常 owner 追新／回補，CNA 暫緩；全來源最大穩定速率、全歷史與完成 ETA 仍未證明。

## 18:25 接續：診斷分類已修正，完成正文保持不變

只續辦未完成項目。CMoneyLearning 28 篇與報新聞 14 篇的既有正文／SHA 全部保留；
報新聞仍為 **14／75**，剩下 **5 筆版型、56 筆署名／範圍未通過**。
正常發現流程會更新 `last_seen_utc`，因此驗收允許它單調前進，其他 head 欄位與正文檔案仍逐項相等；
原錯誤的全欄相等驗收及當時 baseline 保留，沒有重設基準來隱藏差異。

- 修正私人失敗診斷把範圍檢查誤列為寫入失敗的問題：現在分別記錄 `scope_rejected`、
  `storage_error`、`internal_error`。每個既有預留另保存一筆不可覆寫、無原文的結果；
  不新增流量、不增加額度、不修改原文章失敗狀態。**210 項相關測試通過**，包含重啟後額度不變。
- 舊兩筆錯誤缺乏分階段證據，原因仍未知；不能可靠追溯成寫入故障或來源拒絕。
  原每日 2 次扣額不變，不另開補充額度，也不把沒有保存的原件算成已取得。
- 既有原件實際宣告 WordPress REST root 及 posts 路徑。沿共用 HTTP／REP／原冷卻，
  一次請求查詢 4 個**已知且仍缺文** ID，使用 `include`、`_fields`、`_embed=author`，
  只回傳 metadata，HTTP 200／8,298 bytes／2.229 秒。原件宣告與來源回應均有 SHA；
  欄位投影與關聯合併依 [WordPress 官方文件](https://developer.wordpress.org/rest-api/using-the-rest-api/global-parameters/)，
  posts 過濾依[官方端點定義](https://developer.wordpress.org/rest-api/reference/posts/)。
  四筆 permalink 均符合原生精確路徑，三筆只有百分號十六進位大小寫不同；沒有新增 URL alias。
  帳號名稱、作者數字 ID、naive 時鐘仍是來源宣告，不能代替原文署名或歷史 PIT 證據。
  **沒有接通 API 正文下載，也沒有擴張全站發現範圍。**
- [載入版本與雙面板驗收](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_remaining_v3_20261024/ready.json)：
  唯一 owner `4850843a11214af89e71496eec09b740`，本機與公開 HTTPS 均 fresh／ready。
  新 owner 初始收據核對實測 73.232 秒；首次 ready 驗收早於新快照，原失敗保留，等實際快照後才通過。
- [本輪讀回與交接](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_remaining_v3_20261024/handoff.json)：
  上次交接後的 35 個耐久批次、690 個正文版本通過獨立讀回，其中鉅亨 610 個。
  這是**版本數，並非新增文章數**；原完成正文沒有為本次驗收額外發出 GET。

後續只針對新取得的四筆 metadata 與上述 5／56 筆缺文建立下一份原生結構／署名證據。
初始化重讀既有收據的成本已量出，尚未優化；來源最大穩定速率、全歷史與完成 ETA 仍未證明。
鉅亨持續優先，CNA 暫緩，不重新執行已結案的圖片段落或 CMNews 身分問題。

## 17:54 現況：只續辦缺文，沒有重抓已完成正文

本機 75 個已知網址已走完 v1 與這次 v2 有界修復：**14 篇有正文、5 篇原生結構尚未支援、56 篇署名／目前範圍尚未通過**。
後兩類不是來源永久缺文，也不能算全文完成。原生 canonical／mainEntity、可見署名／Person profile、明確發布與修改時間分別驗證；沒有回推或假造歷史發布日。

- [全範圍正文讀回](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_media_v2_20261024/scope-observation.json)：9 個耐久批次、14 個正文版本及目前索引通過；原 11 篇完整 head／payload SHA 不變，0 個已完成正文請求。
- v2 原件有一張放在獨立段落內的原生 `wp-image-*` 圖片。只支援實測的純圖片段落，保留全部文字、負值、比率與表格；帶未驗證內文／外框的媒體仍拒絕，沒有取用圖片檔。
- 一份仍缺文原件只補取一次；其後採納原件 0 GET，再由原 owner 重試 55 個尚無正文的 identity error，實際新增 2 篇，加上原件共由 11 → 14。8 筆原署名／範圍錯誤、HTTP 拒絕、來源截止時間與既有診斷額度沒有重置。
- 加入同回應的私人失敗原件保留，沿用既有每 provider／UTC 日 2 次預留，不依網址、parser 或重開機補額度。這輪 2 次診斷失敗已扣額，尚需分辨是範圍檢查或寫入錯誤；不能將計數當成已保留原件。
- [服務與兩端網頁驗收](../artifacts/data_quality/news_goal_stage01_20261009/planning_resume_20261021/contentplatform_media_v2_20261024/ready-recovered.json)：同一新 owner、載入版本一致、fresh／ready。清理已到期且消失的恢復 timer 時，以實際 `LoadState` 區分已不存在與仍須停止，原失敗驗收保留。
- 回歸分別通過 910 項註冊檢查及最新 575 項修復檢查；覆蓋有重疊，不相加。獨立 XPath 完整文字及兩套 parser 的所有欄位一致。

剩餘工作只針對上述 5／56 筆的新結構／署名問題，並核對診斷失敗原因；不重送已結案的純圖片段落問題。正式來源沒有加入發現端點，最早歷史、全站範圍、官方數值配額、穩定最高吞吐與全部完成 ETA 均未證實。
實際有用正文不足，因此面板不硬填完成時間。鉅亨維持優先，CNA 暫緩。

## 先行方案

承接新聞全來源目標，保留鉅亨優先、中央社暫緩。前輪已清點
`www.contentplatform.info` 的 74 個本機網址，並記錄官方 copyright／service
頁面的個人非商業下載條款；前輪沒有取得文章，不能視為全文或歷史完成。

1. 由現有 queue 的唯讀交易取得確切已知網址、原始 metadata 時鐘與狀態，
   不猜文章 ID、分頁或 WordPress API。
2. 沿現有 disjoint-source 私人診斷入口、原 HTTP root 與誠實 client 身分，
   保留 REP、來源冷卻、公開位址與容量檢查；初始為 10 秒／單槽。
   先觀察唯一已知的 `articles/533649/jaya533649/`，最多一次文章請求。
3. 未見會員／挑戰／拒絕時，才私人保留可重播原件。獨立核對 canonical、
   原生署名、完整可見正文、發布與修改時鐘；不同角色分欄，不猜時區或當年發布日。
4. 若有足夠原件，優先擴充共用 adapter，保留原數值、段落、表格與 null。
   來源範圍與讀取規則需明確版本；先離線驗證，再接原唯一正式下載 owner。
5. 由耐久批次與正文 SHA 讀回確認實際取得，並核對本機／公開面板。
   不重做其他已結案文章，不將 REP 或個人用途擴張成全站授權、全文完成或匯出權限。

本階段不能證明官方數值限速、最早歷史、全站總量或穩定最大吞吐；
只有實際完成的正文與剩餘可工作量才能用來估時。
