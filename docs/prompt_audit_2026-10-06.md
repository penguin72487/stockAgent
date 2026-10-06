# Prompt audit：penguin／Vastai1T 對話與既有技能

本輪已依使用者要求，把 StockAgent 的 `check-prompt-audit` 預設範圍改為
penguin 與 Vastai1T 的主對話及自訂 skills：合併有來源的偏好與後續修正，更新
既有技能，再同步。兩邊現有 7 個技能／30 個成員內容相同，Codex 均可載入。

## 範圍與證據

日期為 Asia/Taipei 2026-10-06；UTC artifact 為
[`20261005T161247Z`](../artifacts/prompt_audit/20261005T161247Z/)。

| 來源 | 檔案盤點 | 實際串流／角色抽取 | 候選 |
| --- | --- | --- | ---: |
| penguin | 185 檔，96 主對話／89 子代理 | 核對上輪 prefix／tail hash，只讀 10 份主對話新增區段，119,876,277 bytes | 22 |
| Vastai1T | 35 檔，30 主對話／5 子代理 | 首次索引 30 份主對話，681,421,284 bytes | 527 |

合計 549 則候選，精讀 55 則，採用 47 則短引文作偏好、範圍或取代證據。
其餘 494 則沒有全部語意精讀，不能稱為逐字稽核所有歷史。支援 `event_msg`
與 `response_item`，排除子代理、AGENTS／環境、plugin 推薦、app 導航和 goal
注入；IDE／附件只保留實際要求，問答只採使用者的 `answer`。

所有 47 個採用來源的 record SHA、timestamp、user role／短引文均回核成功；
Vast 的回核在來源節點執行。相同原話的 event／response、繼承與跨機拷貝合併
來源，不提高偏好信心；轉貼 traceback／助理建議仍需語意判斷。
機器推薦／附件前綴的篩選修正只重處理本輪私人候選，未重讀完整 raw history。

## 發現與修改

| 位置／原指引 | 問題 | 處置與來源 |
| --- | --- | --- |
| audit 只提本機可讀對話、遠端同步另行要求 | 不符合本次指定的持續範圍 | entry、UI prompt 與 references 改為 penguin／Vast 雙節點；當次只讀／限縮優先。R0054、R0055 |
| 偏好要求 panel 輸入 availability／age／update | 被後續 value-only 要求取代 | 新 panel 模型只收數值；原始 NULL、時鐘、TTL／生命週期及資格證據仍保留。R0041、value-only 報告 |
| 策略 gate／滿倉或額外 cash gate 可被誤當預設 | 使用者希望模型在既定環境中自行學習 | 補明市場／帳務規則與策略選擇的邊界；有請求的風控／架構實驗仍依當次範圍。R0006、R0016、R0020、R0031、R0033 |
| 正式 output 已存在便拒絕啟動 | 阻止相容 checkpoint 的正常續跑 | 既有訓練技能補 canonical resume、artifact gate 及所屬子程序停止；不接工程 optimizer。R0027、R0049 |
| 期貨優化 skill 的固定順序、2 的冪次、epoch2+ | 將歷史候選／測速變成永久方法限制 | 以實測瓶頸選候選；當前 Vast TW 當沖升級沿完整 fold／epoch3+，舊數字限日期／工作量 |
| 不相容 basis 便擬合 PCA/KLT | 可覆蓋後續不分解模型選擇 | 僅選定 learned-basis 設計才擬合，不重新導入已取消的分解。R0044–R0047 |
| 圖表、技術記錄與小修回覆 | 原有偏好缺少 Vast 原話的細節／例外 | 比較保留 baseline、名稱、原圖配色、逐 epoch／fold 圖與原目錄；一般小修直接對話交代。R0002、R0008、R0010–R0017、R0021 |

修改既有 `check-prompt-audit`、`user-working-preferences`、
`stockagent-training-reuse`、`stockagent-exact-futures-optimization`；沿用原 18 組
偏好，不逐對話新增 skill。其他 skills 的內容透過聯集合併，未為此改交易或訓練契約。
相關文件查閱 value-only、不分解、完整搬運流程、runtime／performance 契約；
本輪沒有重新掃描上輪全部 319 份 Markdown，未聲稱完成新一輪全部文件全文稽核。

## 驗證與可復原性

- focused tests：`test_prompt_history.py` 與 `test_skill_sync.py` 共 **24 passed**。
  涵蓋角色／來源排除、雙記錄／跨機去重、增量／半行／替換、錯誤節點基線、
  原話 locator，以及三方合併、衝突、備份、來源變動與同步範圍。
- 7 個 skill 的官方 `quick_validate.py` 通過；23 個相對 Markdown 連結可達。
  兩邊 `skills/list(forceReload=true)` 都列出 7 個 enabled skills，errors 為空。
- 增量再讀沒有新使用者候選：penguin 只讀 3 份尾段／3,256,628 bytes，Vast
  串流 0 bytes，沒有重新掃描前輪 681 MB 的主對話。
- 先建立共同基線，再更新導入的 Vast 既有 skill，最後同步 3 個改動成員；
  兩邊 30 個成員的內容／hash／mode 完全相同。再預覽新增／修改／衝突皆為 0。
- manual before／hash 及同步 rename 備份可回復；保留同工作區其他人的 dirty
  工作與既有 Git 變更。原始對話只讀，私人候選全文交付後移除；不同步 auth、
  sessions、memory 或 plugin cache。

格式與 discovery 證明技能可發現；不是模型品質、策略收益、GPU 加速或服務營運
驗收。本輪沒有啟動歷史任務中的下載、部署、刪除或訓練。

## 後續入口與限制

使用 `$check-prompt-audit`／`/check-prompt-audit` 即沿此流程。由 penguin 協調：
`scripts/inventory_prompt_history.py` 讀取兩邊角色候選，各節點使用最近的有效
`*_session_inventory.json` 做基線；`scripts/sync_codex_skills.py` 在整理後合併。
當前明確只讀或指定路徑仍優先，沒有常駐背景同步排程。

若在 Vast 觸發且無法使用 penguin 協調入口，應明列缺失覆蓋並繼續可獨立工作，
不能把 Vast 本機稽核稱為完整雙節點稽核。其他平台／帳號歷史未存取。
詳細同步／回復方式見 [技能同步](codex_skill_sync_2026-10-06.md)。

可核對收據：

- [歷史索引摘要](../artifacts/prompt_audit/20261005T161247Z/history/summary.json)
- [語意審閱與覆蓋](../artifacts/prompt_audit/20261005T161247Z/dialogue_review.json)
- [短引文與來源](../artifacts/prompt_audit/20261005T161247Z/reviewed_preference_evidence.jsonl)
- [原話回核](../artifacts/prompt_audit/20261005T161247Z/source_validation.json)
- [Codex 發現](../artifacts/prompt_audit/20261005T161247Z/skill_discovery.json)
