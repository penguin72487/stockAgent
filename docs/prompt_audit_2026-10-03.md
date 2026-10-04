# Prompt audit 與偏好增量整理 — 2026-10-03

已完成本輪提示文件稽核：修正 5 份專案文件、8 份自訂 skill 文件，並保存原文、
差異與驗證。根 `AGENTS.md` 維持既有 6,767 bytes；沒有修改交易、訓練、下載器
程式或實驗設定，也沒有重啟服務。文字整理不代表模型能力或執行速度已提升。

任務：`prompt-audit-20261003-070139`。
證據目錄：[20261003T070139Z](../artifacts/prompt_audit/20261003T070139Z/)。

## 發現、證據與處置

| 發現 | 問題與依據 | 本輪處置 |
| --- | --- | --- |
| 根目錄指引過時 | `docs/agents/runtime.md` 仍寫 repo root 是「this file」所在目錄；移至 `docs/agents/` 後已不成立 | 改以 checkout root、根 `AGENTS.md`／`train.py` 定位 |
| 工作狀態範例載入過多歷史 | `status` 包含每個 task 的 Git 基線、歷史與全部 run；原摘要直接保留 `.tasks` | runtime 提醒按需讀取；workflow 範例只列任務身分／狀態與按時間排序的最近十筆 run，原紀錄仍可查 |
| epoch 驗收口徑衝突 | performance 入口與 runtime 要求該遠端工作用 epoch 3+；正文仍概括寫 second epoch or later | 正文依所選 benchmark，明確保留 vastai1T epoch 3+／maximum-rank 要求與獨立冷啟動量測 |
| 平倉要求過度泛化 | 兩份訓練／衍生品 skill 把 terminal close 零部位當共通條件；現有 execution 契約及 `test_margin_forced_close_remains_reduction_only_with_residual_carry` 明確保留未成交部位 | 區分 forced-flat 與 carrying contracts；各自驗收終止／延續語意，保留破產、到期與缺來源邊界 |
| smoke 清單混入分鐘模式假設 | 通用清單強制 `minute_setup_timing.jsonl`、年分 group 命名，並對所有 vectorized minute runner 套 independent-day optimizer 次數 | 用 `TrainingArtifactLayout` 與模式契約定位 group；分鐘 setup 僅限該 runner；獨立 session 與 recurrent trajectory 分別核對 |
| 預設模型建議變成實驗禁令 | models 入口允許明確選定實驗，正文卻禁止所有新的 legacy-policy stock experiments；與根指令和近期架構實驗要求不一致 | 保留 learned-cash 預設；明確選定其他 policy 時仍須相容 ABI、artifact root 與驗證，不默默替換預設 |
| 歷史 review 被稱為「最近一次」 | 文件目錄把 2026-09-05 review 稱為最近一次，容易遮蔽後續架構文件 | 改為具日期歷史快照，加入本次稽核與偏好基線入口 |
| 自動 goal 續作可能被當成使用者反覆要求 | 本次增量抽取有 18 則 `codex_internal_context`，即使帶有 user role 也不是新原話 | 排除該批內容，並把此實際案例加入偏好抽取流程 |

上述都是指令位置、適用範圍或閱讀成本的修正；PIT、來源／價格真實性、費稅、
checkpoint 相容性、完整指定工作量、冷庫恢復及服務驗收沒有被移除。
現有測速數字保留為具工作量／硬體範圍的觀察，沒有為了縮短文字刪除契約。

## 偏好更新

[user-working-preferences](/root/.codex/skills/user-working-preferences/SKILL.md)
保留原 16 組偏好並新增 P17、P18；細節按工程／研究、呈現與證據分開載入。

| 新證據 | 更新後的取捨 |
| --- | --- |
| N0100、N0103：少量易懂、重要資訊在前，清單在後 | 抓取頁摘要優先、完整明細後置；共用版型保留來源機制必要差異 |
| N0070、N0088：分階段時間、下載與網站連動 | ETA 依實際排程和進度更新；停滯與未排程狀態不能假裝倒數 |
| N0042：只整理剛修正的部分，最後再全面檢查 | 修復採增量與相稱測試；最後做要求範圍的整合驗收，共用契約風險仍需即時驗證 |
| N0105：維護一個最完整規則版本 | 單一可變工作目錄；原始證據、不可變 release、checkpoint 和恢復紀錄仍分開保存 |
| N0106：遠端建置依條件，不只經驗、還要實測 | 按節點 quota／拓樸／記憶體／儲存與負載比較完整工作流 |
| N0086、N0087：現代化方案實作與實際比較 | 重用偏好不禁止受請求的新架構；用實際工作結果挑選技術 |
| N0044、N0015、N0022：結構來源、發布窗口、每 call 資料量 | 同語意資料優先重用；FinMind 的預留與追新按當次來源時程，不無條件占整天配額 |
| N0041、N0107：本機源本、遠端生成訓練資料 | 納入目前 StockAgent 分工，沿用資料版本、服務引用及安全清理契約 |
| N0082、N0083、N0109、N0111：新工作流程、Mamba、原啟動方式 | 採已建立的 task/run 流程及工程環境角色，同時保留熟悉入口 |

沒有把單次清理／重啟授權寫成永久許可，也沒有推測配色、字型或人格。
「所有商品」之後明確允許先跑可驗證子集、FinMind 排序之後把美股分鐘移到最後，
都保留原話的時間與範圍，不能只選一句最強硬的舊要求。

## 覆蓋與界限

- 基線合併上次 `inventory.json`、額外 Markdown 清冊與最終 `validation.json`，
  避免把上一輪最後建立的偏好報告再次誤算為新文件。
- 本輪開始時索引 **266 份 Markdown**：33 份新增、17 份變動、216 份未變。
  這是 metadata／內容雜湊比對，不是 266 份逐字語意審查。
- 5 份自訂 skill 入口全文審閱；相關 references、根指令、runtime／performance／
  models／execution／training 及新工作流程、介面、儲存文件按主題精讀。
  其他主題指令做引用及候選文字篩查，套件／系統 skills 沒有全面重寫。
- 本機共 **184 份 session**，95 主對話、89 子代理。只串流新檔／成長區段，
  16 檔共約 1.57 GB；子代理檔只核對來源。JSON 解析錯誤 0。
- 112 則增量候選排除 18 則自動續作內容，**94 則原話候選經閱讀，27 則選入
  偏好與例外證據**。工具提問、貼回的助理表格與歷史操作命令只作上下文。
- 舊記憶摘要中「大型 AGENTS 尚未重構」與當前檔案不符；本輪以當前檔案、
  原話與引用為準，未改寫私人記憶。不可讀的其他帳號／平台歷史不在範圍內。

來源清冊：
[Markdown](../artifacts/prompt_audit/20261003T070139Z/markdown_inventory.json)、
[對話讀取邊界](../artifacts/prompt_audit/20261003T070139Z/session_inventory.json)、
[角色與閱讀分類](../artifacts/prompt_audit/20261003T070139Z/dialogue_review.json)、
[採用的原話與來源位置](../artifacts/prompt_audit/20261003T070139Z/reviewed_preference_evidence.jsonl)。

## 驗證

- 5 份自訂 skills 通過 `quick_validate.py`；Codex app-server
  `skills/list(forceReload=true)` 對每份都發現唯一入口、正確路徑及 `enabled=true`，
  無載入錯誤。未呼叫模型服務，也未測 IDE slash 選單。
- 檢查本輪修改的 Markdown 引用／anchors、前後 diff、skill metadata 與證據 ID。
  指令文件中 114 次程式／設定／測試路徑引用皆可找到；不代表各命令都已執行。
- 實際執行更新後的 `status | jq` 範例：同一份狀態 JSON 為 556,842 bytes，摘要
  8,304 bytes；保留 20 個任務摘要、最近十筆 run。這是閱讀量差異，沒有宣稱
  status 程式執行更快，也沒有刪除原操作紀錄。
- 人工核對只分析、只讀稽核、新 policy 實驗、純加速、carry／flat、不同 runner
  產物、跨裝置完整明細、增量驗證與自動 context 排除等情境。
  這是指令語意檢查，不是獨立模型端到端評測。

收據：[格式](../artifacts/prompt_audit/20261003T070139Z/skill_format_validation.json)、
[Codex 發現](../artifacts/prompt_audit/20261003T070139Z/skill_discovery.json)、
[驗證與檔案 SHA](../artifacts/prompt_audit/20261003T070139Z/validation.json)。
沒有執行與文件改動無關的 GPU、訓練或全庫測試，未把檔案／載入檢查說成
交易策略、資料完整性或生產 runtime 通過。

## 復原

證據目錄內 `before_manifest.json` 對應每份修改前原文與 SHA；`existing_git_diff.patch`
保存原本重疊的 Git 變更，`changes.diff` 只對照本輪 before 與 after。
復原前仍需檢查後續修改，不能以 Git HEAD 覆蓋其他工作。本輪報告是新增文件。
