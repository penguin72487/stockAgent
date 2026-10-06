# Prompt audit 與偏好增量整理 — 2026-10-05

接續 [10 月 4 日基線](prompt_audit_2026-10-04.md)，本輪釐清新儲存 skill 的
唯讀／回收預演差異，並將新對話合併到既有 18 組工作偏好。修改 7 份既有文件，
另新增本報告；根 `AGENTS.md` 保持 6,767 bytes，沒有增加全域必讀規則。

任務：`prompt-audit-20261005-023929`。
證據根目錄：[20261005T023929Z](../artifacts/prompt_audit/20261005T023929Z/)。
起始 Git HEAD 為 `e480264a6a0bb6d4772d63eb3979b60d4724d2a9`，
297 個 dirty paths；本輪以修改前的工作樹為基準，不覆蓋其他工作。

## 從目的、行為到處置

| 原文／位置 | 可能誘發的行為 | 證據與處置 |
| --- | --- | --- |
| [儲存 skill](../.agents/skills/stockagent-storage-operations/SKILL.md) 將 `gc --dry-run` 列在「唯讀起點」，operations reference 亦將它併入進度查詢 | 只查進度卻取得 edge 操作鎖、寫入新的操作收據 | `run_data_cache.sh` 的 edge routing、`manage_packed_edge.py` 的 lock／`write_edge_receipt` 與 `materialized_cache.py` 的 dry-run 分支確認：不刪檔、不實際續租，但不是完全唯讀。兩處改為需要評估回收時另用預演；進度入口只列 status |
| 工程偏好只說 panel「保留不同更新頻率…造成的空值」 | 把原始 NULL 延伸成訓練日也一律 NULL，忽略後續明確選擇 | Q0001 的 answer 指定「原始 NULL；訓練使用最近已公布值與年齡」。改為原始觀察／訓練視圖分開；可用性、更新、過期與新 NULL 屏障沿既有 panel 契約 |
| 既有服務／架構偏好尚未納入本批開機、正式運行及跨 agent 接手要求 | 把候選工具測試、文件存在或 service active 當作使用者要求已完成 | Q0013–Q0018 與開機／架構文件支持：選定服務要有運行及業務證據，登入後恢復不冒充冷開機；專案 skill 要可攜且附完整引用，接收端發現與營運驗收分開 |
| 新增的大量歷史資料壓縮、誤刪恢復及暫停／續作原話 | 將一次性方案固定成日常架構，或把舊暫停與「其他不見的資料」無限泛化 | Q0003→Q0004 保留同任務續作；Q0005 bulk 限當次；Q0007–Q0010 的清理／恢復限原範圍。它們不是本輪執行營運命令的授權 |

根指令與新儲存 skill 的其餘主要契約保留：按任務讀取、單一 owner 下的有界並行、
精確來源與恢復、程序引用、PIT／帳務／checkpoint 相容性及全工作量效能驗收。
沒有證據顯示它們只是舊模型補救；不能以「模型更新了」或篇幅較長為理由移除。

也有已正確更新而無需重改的項目：

- [應有特徵缺口報告](tw_feature_expected_gaps_2026-10-04.md) 已在開頭連到
  [10 月 5 日恢復交付](tw_feature_gaprepair_remote_resume_2026-10-05.md)，舊暫停明確屬歷史。
- [儲存契約](agents/storage.md) 已指定 preparation 原值歸入
  `data_tw_index_futures/preparation_sources/`，保留 immutable receipts／相容引用與恢復保護。
- 新儲存 skill 已區分完整 skill 目錄、frozen code bundle、NAS 備份與接收端實際啟用；
  根 README／冷庫 runbook 的 GC 範例亦未把預演稱為完全唯讀，無需擴大改寫。

## 偏好合併與範圍

更新 [user-working-preferences](/root/.codex/skills/user-working-preferences/SKILL.md)
及三份按需 references，維持原 18 組偏好，不為每段對話建立新 skill。

| 直接原話 | 寫入的選擇原則 |
| --- | --- |
| Q0001：「原始 NULL；訓練使用最近已公布值與年齡」 | 原始觀察不填造，具名訓練視圖使用有界、可追溯的已公布狀態 |
| Q0002：「理論上有值但是實際上沒有值…全部整理…優先插入」 | 完整列出應有觀察，核對來源／發布條件，優先排入既有 provider 佇列；排程與實際補齊分開 |
| Q0010、Q0015：原始資料另置、Vast 只留當期程式／資料／產物 | 合併既有四節點分工；具體目錄及回收條件由 repo 儲存契約維護 |
| Q0013、Q0014、Q0018：重開機後恢復正常服務 | 依清冊、啟動依賴、業務收據驗收，區分登入後、登入前與實際冷開機證據 |
| Q0018：「減少滑頁，直接用搜的最快」 | 有可靠原生搜尋時優先實測；保留目標身分與選取讀回，不當作所有 UI 的速度保證 |
| Q0016：「寫成skill，教其他agent使用」 | 操作流程與必要 references 一起交接，連回可維護的專案契約 |
| Q0017：「直到所有設計的服務正在運行」 | 選定架構要完成實作、測試及正式服務驗收；不強制部署每項評估過的候選工具 |

問答只採使用者 `answer`，不把助理提供的問題選項算成原話。原話、文件佐證與編寫者
歸納分開保留；例如 panel 的 TTL／NULL 屏障來自專案契約，不聲稱使用者逐字指定。
未知配色、字型或人格傾向仍不推論。

## 實際覆蓋

- 起始 Markdown 索引 **319 份**：14 新增、9 變動、296 與前次最終 hash 相同。
  本輪聚焦新儲存 skill、偏好 skill、相關契約與變動文件；全文、相關段落、關鍵字
  篩查和 metadata 比對分列於 [逐檔清單](../artifacts/prompt_audit/20261005T023929Z/markdown_inventory.json)。
  不是 319 份全文語意審查；大型研究報告的數值與營運結果未逐項重新驗收。
- 本機 **185 份 session**：96 主對話、89 子代理。179 檔邊界未變，6 檔驗證為新增尾段，
  沒有新 session。只串流 6 份主對話增量，共 **118,280,999 bytes**，沒有重讀全歷史。
  prefix／tail fingerprints 核對通過，解析錯誤及未完成半行均為 0。
- 31 筆 user records 排除 13 筆注入上下文，得到 **18 則候選**，均閱讀完整候選內容。
  採用 **14 則**作偏好、修正或當次範圍證據；另 4 則為兩次進度詢問、一則路徑回答及本次 skill 呼叫。
  不以同一任務多則訊息增加跨情境重複信心，不以子代理文字佐證使用者偏好。
- [來源邊界](../artifacts/prompt_audit/20261005T023929Z/session_inventory.json)、
  [候選分類](../artifacts/prompt_audit/20261005T023929Z/dialogue_review.json)、
  [選用短引文](../artifacts/prompt_audit/20261005T023929Z/reviewed_preference_evidence.jsonl)
  保留時間、來源、固定 byte offset 與 hash。生成的完整候選暫存副本已移除，原始對話未改。
  只涵蓋本機可讀來源，沒有其他平台全部歷史的存取／審閱聲稱。

## 載入與驗證

- 本機 `codex-cli 0.160.0`，`config/read` 回傳 `project_doc_max_bytes=32768`、
  fallback filenames 空陣列；global `AGENTS.md` 為空，repo 入口為 6,767 bytes。
  這是本機當次觀察，不是所有版本的永久上限。
- **6 個 skills** 的 `quick_validate.py` 通過；本機
  `skills/list(forceReload=true)` 發現唯一、正確路徑且 enabled 的 5 個 user skills
  與 1 個 repo skill，無 discovery errors。沒有變更觸發 metadata。
- **14 筆**選用證據回到原 session 固定 byte 範圍核對 SHA、時間、user 身分與引文；
  回覆記錄的問答只驗 answer。對話存在不等於其中營運結果已在本輪驗收。
- 檢查受改文件的 Markdown 連結／anchors、引用路徑、格式、結尾換行與本輪 diff，
  核對新儲存 skill 的命令例子與實際 parser／routing／dry-run 分支。
- 人工核對只讀、只分析、既有授權續作、原始 NULL／訓練視圖、原值保護、開機證據、
  搜尋驗證及候選技術／正式服務的預期選擇。這是文件語意檢查，沒有做模型行為評測，
  也沒有測 IDE slash 選單。

收據：[本機設定](../artifacts/prompt_audit/20261005T023929Z/instruction_config.json)、
[skill 格式](../artifacts/prompt_audit/20261005T023929Z/skill_format_validation.json)、
[discovery](../artifacts/prompt_audit/20261005T023929Z/skill_discovery.json)、
[原話驗證](../artifacts/prompt_audit/20261005T023929Z/source_validation.json)、
[最終驗證](../artifacts/prompt_audit/20261005T023929Z/validation.json)。

本輪沒有執行訓練、provider 查詢、服務重啟、搬遷或資料回收；沒有寫入私人 memory、
套件 cache、交易程式或模型設定。先前期貨 benchmark 的程式／現金流待核項，以及
panel 完整訓練、四節點冷開機／備份完成度，仍需各自任務的實際驗收。
指令整理不宣稱提升模型品質、速度或資料完整性。

## 修改與復原

既有修改檔：偏好 skill 入口／三份 references、新儲存 skill 入口／operations reference、
`docs/README.md`；本報告為新增文件。

[before manifest](../artifacts/prompt_audit/20261005T023929Z/before_manifest.json)
與 `before/` 保存 7 份原文、SHA、bytes／行數；`existing_git_diff.patch` 與 staged diff
保留原有重疊工作。[本輪差異](../artifacts/prompt_audit/20261005T023929Z/changes.diff)
只比較本輪 before／after，各檔最終 SHA 及入口大小見最終驗證。
復原前須核對後續修改，不能用 Git HEAD 或整檔還原覆蓋其他人的工作。
