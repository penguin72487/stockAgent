# 使用者品味、修改偏好與 prompt audit skill 整理

2026-09-30（Asia/Taipei）。本輪把可讀的既有文件、使用者原話與後續修正整理成
可重用技能，並擴充既有 `check-prompt-audit`。交付的是偏好基線與維護流程；
沒有把歷史聊天中的操作指令重新執行，也沒有修改交易、下載器或訓練程式。

## 已安裝的兩個入口

| 技能 | 用途 | 入口 |
| --- | --- | --- |
| `user-working-preferences` | 日常套用有證據的工程、研究、介面及報告偏好 | [SKILL.md](/root/.codex/skills/user-working-preferences/SKILL.md) |
| `check-prompt-audit` | 整理提示規則，從文件與可讀對話維護偏好、範圍及後續修正 | [SKILL.md](/root/.codex/skills/check-prompt-audit/SKILL.md) |

使用 `$check-prompt-audit`，或在接受該文字指令的對話介面輸入
`/check-prompt-audit`，即可提出稽核要求。只想套用偏好時可用
`$user-working-preferences`。本輪確認的是 Codex 的 skill discovery，沒有操作
IDE 的 slash 選單，因此不把文字觸發約定宣稱為新增原生 slash command。

偏好技能只在適用任務載入；工程與研究、呈現與互動、來源與例外分開引用。
目前明確要求優先於歷史偏好；不因套用偏好就每次重掃所有對話。

## 整理出的取捨

| 類別 | 已有證據的偏好 | 避免過度泛化 |
| --- | --- | --- |
| 第一性原理 | 從目的、資料流、責任、實際運行與驗收條件定位問題 | 不把長篇推導或無限探索當成果 |
| 架構 | 重用既有元件、減少硬編碼和重複框架、機制簡單 | 不禁止有理由的新模組或明確要求的架構實驗 |
| 任務交付 | 在授權範圍實作到可用，相關根因一併修正 | 「先分析」時不自動實作；不順手改無關系統 |
| 完整性 | 全部先清冊，逐項交付；可解釋性保留完整覆蓋與圖說 | 摘要可以短，但不偷偷用 Top-K 代替完整結果 |
| 效能 | 測實際等待、整段工作及可重測 baseline | 純加速不靠減參數、少驗證、少出圖改善數字 |
| 資料 | 盡可能補齊歷史與特徵，缺值先辨認週期、稀疏和來源問題 | 研究近似要有範圍，不當成已證實歷史真值 |
| 排程 | 減少重複抓取、配額按工作分配、保留必要增量 | 網站的併發意見不等於放寬 provider 官方限制 |
| 狀態 | 分清程序可達、資料有效／完整、可訓練和已部署 | 不拿 HTTP 200、少量 smoke 或文件說法代替實際驗收 |
| 介面 | 一致、可讀；筆電寬度看完；手機和平板適配；原位更新 | 不靠藏欄位或極小字體壓縮畫面 |
| 圖表與文件 | 圖例可互動、每張圖有解讀、完整 Markdown 報告和可用指令 | 未指定固定配色、字型、深淺主題 |
| 進度 | 用流量、筆數、速度、等待原因與 ETA 表達剩餘工作 | 不只計算已完成幾個 dataset |
| 維護 | 能自行操作、可恢復、處理同一問題的相關模組 | 不把既有服務、唯一資料或未提交工作當整理垃圾 |

共 16 組 P 編號，具體原話與範圍見
[來源參考](/root/.codex/skills/user-working-preferences/references/evidence.md)。
這份表是有來源的歸納，不是聲稱使用者逐字講過每個句子。

## 來源與覆蓋

證據目錄：[preferences_20260929T171450Z](../artifacts/prompt_audit/preferences_20260929T171450Z/)。
它是本次讀取邊界的快照；其他對話與工作仍可能繼續更新。

| 來源 | 已做的處理 | 實際覆蓋界限 |
| --- | --- | --- |
| 本機 Codex sessions | 串流抽取 173 檔，86 主對話／87 子代理檔；JSON 解析錯誤 0 | 全量抽取使用者角色，不是全文審閱所有助理／工具內容 |
| 使用者角色候選 | 初次去重 1,489 則；排除僅出現在子代理的任務、移除 IDE 前綴並再去重後 1,486 則 | 仍可能有引用助理回答、錯誤堆疊；不能依 role 自動當偏好 |
| 採用的原話 | 精讀並保存 64 則相關主對話訊息，附 U ID、原路徑、行號、時間 | 作為本版偏好、反例、限域例外與使用者授權證據 |
| 專案 Markdown | 索引 232 檔：227 份 docs 加 5 份其他入口／README；保留大小、行數及 SHA | 文件標記篩選與相關段落精讀，不宣稱 232 份逐字審核 |
| 既有記憶摘要 | 作為搜尋線索，重要偏好回查原話及現有文件 | 未寫入或修改私人記憶 |
| 其他聊天平台／帳號 | 本輪沒有取得這些來源 | 不宣稱整理了不可讀的歷史 |

主要文件交叉核對包括 `AGENTS.md`、專案架構、完整工作量效能、可解釋性、公開
dashboard，以及近期 OCR、缺值修復、特徵整理、全商品期貨和未成交留倉文件。
技術細節以原專案的契約和實作為準，沒有把整份文件複製進個人偏好技能。

來源角色、文件與覆蓋證據：

- [初始索引](../artifacts/prompt_audit/preferences_20260929T171450Z/inventory.json)
- [主對話／子代理區分](../artifacts/prompt_audit/preferences_20260929T171450Z/origin_filter.json)
- [額外 Markdown 索引](../artifacts/prompt_audit/preferences_20260929T171450Z/additional_markdown_inventory.json)
- [64 則已讀使用者證據](../artifacts/prompt_audit/preferences_20260929T171450Z/reviewed_user_evidence.jsonl)

## 沒有固化的舊設定

雙卡 DDP、某一輪 epoch 測速口徑、BF16／TF32、注意力方式、資本額、開收盤
時鐘、可否跨日留倉、tick 優先序和保守延遲天數都保留其任務／研究版本範圍。
新要求只取代與其重疊的舊設定，不覆蓋其他模式。

同時接受「研究中用發布規律推估」與「區分估計和已核實時點」；不把前者刪成
絕對禁止，也不把後者丟掉。助理寫下的建議、子代理收到的任務、轉貼的文字和
新產生的偏好摘要，都不能冒充新的使用者原話。

## 驗證與復原

兩個 skill 均通過 Skill Creator 的 `quick_validate.py`。本機 Codex app-server
實際執行 `skills/list(forceReload=true)`，均只有一份同名技能、`enabled=true`、
`scope=user`、路徑與 UI metadata 正確，未回傳載入錯誤；沒有呼叫模型服務。
[發現機制驗證收據](../artifacts/prompt_audit/preferences_20260929T171450Z/skill_discovery.json)。

另檢查本輪技能的相對引用、來源 ID、UI 描述與前後 diff。人工對照以下情境：

| 情境 | 技能要求的判斷 |
| --- | --- |
| 一般 Markdown 小改 | 不啟動全歷史稽核 |
| 「只出報告／只分析」 | 不修改受查對象或進入實作 |
| 明確要求新模型設計 | 舊速度建議不禁止實驗 |
| 只要效能優化 | 保留指定語意、能力與工作量 |
| 手機頁面太寬 | 改布局，不把資料欄位刪掉 |
| 對話貼入助理或子代理文字 | 不依 user role 自動提升成偏好 |
| 新交易時鐘或研究近似 | 只更新相應模式與版本 |
| 偏好基線已存在 | 核對增量及關聯修正，不每次全量重讀 |

以上是文件語意對照與格式／載入檢查，不是獨立模型的端到端行為評測，也不
宣稱能保證每次自動選中技能。沒有執行與文件修改無關的 GPU 或訓練測試。
完整檢查結果與本輪檔案 SHA 見
[validation.json](../artifacts/prompt_audit/preferences_20260929T171450Z/validation.json)。

既有 `check-prompt-audit/SKILL.md` 與 `agents/openai.yaml` 的原文保存於證據目錄
`before/root/.codex/skills/check-prompt-audit/`；新增的偏好技能與參考檔另有清單／SHA。
復原時只處理本輪檔案，先核對是否又有後續修改，不用 Git HEAD 覆蓋既有工作。
