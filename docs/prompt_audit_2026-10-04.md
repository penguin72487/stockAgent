# Prompt audit 與偏好增量整理 — 2026-10-04

接續 [10 月 3 日基線](prompt_audit_2026-10-03.md)，本輪修正 2 份專案文件、
6 份自訂 skill 文件，另建立本報告。偏好維持原 18 組，合併新增原話與後續修正。
根 `AGENTS.md` 保持 6,767 bytes；本輪交付是指令與偏好整理，不是訓練、下載、
備份或清理任務的執行驗收。

任務：`prompt-audit-20261004-090138`。
證據根目錄：[20261004T090138Z](../artifacts/prompt_audit/20261004T090138Z/)。
盤點開始時 Git HEAD 為 `e480264a6a0bb6d4772d63eb3979b60d4724d2a9`，
有 140 個 dirty paths；其他任務仍在進行，修改依當前原文增量套用。

## 從目的、行為到修正

| 原文／位置 | 會誘發的問題 | 核對依據與處置 |
| --- | --- | --- |
| `stockagent-training-reuse/SKILL.md` 的 `train_<years>` 與固定年度曲線路徑 | 可能要求 day-count runner 改用年度群組，或找不到其正確 checkpoint／curve | `TrainingArtifactLayout` 接受 canonical `train_` 群組；tick runner 實際使用 `date_training_group_name`。入口改用 layout 的 group 路徑，保留年分與日期範圍各自的身分 |
| 同 skill 的 `canonical-training-map.md` 把年分 helper 當所有模式的路徑 owner | 主入口、驗收清單與 map 的範圍不一致 | 標明它們是 annual-fold wrappers；所有模式仍用同一 layout，沒有新建路徑契約 |
| map 將 phase 寫為 `train`／`test`／`report` | 照抄成 JSON enum 會與 runtime 拒絕條件衝突 | 對照 `lifecycle.py::TRAINING_PHASES`，改為 `training`／`testing`／`reporting` 等實際值 |
| [四節點文件](four_node_storage_architecture_2026-10-04.md) 表格的「Restic 單一 writer」 | 容易誤把一個協調 owner 解讀為所有工作必須串行 | `backup_relay_pipeline.py` 使用有界 worker pool、backup／restore semaphore，完成後才進 repository check barrier；改為單一 owner 下的受限並行並連到現行管線文件 |
| 偏好 evidence 中概括「歷史…允許…不構成現在的授權」 | 適合阻止稽核重播命令，但可能誤傷同一任務的有效持續授權，造成重複詢問 | 區分被引用到新稽核的命令與同一延續任務。已確認事項／有效授權承接；撤回、範圍變更或新證據衝突另行處理 |

原文與 hash 保存在 [before manifest](../artifacts/prompt_audit/20261004T090138Z/before_manifest.json)，
可對照 [本輪差異](../artifacts/prompt_audit/20261004T090138Z/changes.diff)。
這些修正處理適用範圍、可執行名稱及工作成本；不以「模型更新了」或字數多少作刪規則的依據。

## 偏好與後續修正

更新 [user-working-preferences](/root/.codex/skills/user-working-preferences/SKILL.md)，
工程、呈現與原話證據仍按需載入，不把全部原話塞進入口。

| 直接證據 | 本輪合併的偏好／範圍 |
| --- | --- |
| O0050：「保管好了不要再問了」 | 同一持續任務承接已確認事項；不是永遠略過新風險或目前證據 |
| O0052、O0070：日常同步／驗證／備份全自動 | 沿既有排程與收據接續，減少逐批人工轉貼 |
| O0071：「全部整條鏈都要同時平行處理」 | 獨立批次／階段可並行，以實測設定 worker；同批發布、恢復、回收仍有先後依賴 |
| O0063、O0064、O0068：自行恢復、重試到上限就記錄 | 有界重試、保留原錯誤與缺口，健康工作繼續；耗盡不是成功 |
| O0001、O0003、O0004：混合頻率、全部特徵清冊、共用／個股特徵 | 邏輯 panel 為日期 × 可交易目標 × 特徵；保留合法空值，同義合併先核對語意、粒度、單位與可知時鐘 |
| O0059、O0060：API／Smart Wizard 分工，先取得缺少且高價值資料 | 依已驗證的等價範圍扣除重複工作，不能把目錄可見等同已取得 |
| O0011：自己啟動、每 epoch 實際 loss 圖與指定工作目錄 | 遵循所選實驗 output policy；不以 JSONL 代替圖檔，不把指定路徑變成所有專案的絕對路徑 |
| O0054：penguin／lab203／NAS／Vast 四節點責任 | 源本與服務、備份中繼、持久備份、訓練與產物回傳各有責任；回收沿用 repo 儲存契約 |
| O0066、O0069：日曆、calls、分階段進度 | 併入既有 P12／P15，保留候選、有效工作、失敗與完成的不同口徑 |

幾組不能混成永久通則的修正已寫回證據索引：

- O0002 僅允許本人在 vastai1T 的 FinLab／FinMind 私人研究資料包，不延伸為 TEJ 或公開發布許可。
- O0023／O0024 明確允許具名期貨研究凍結估值與研究結清；不能稱作官方結算或真實成交。
  O0025 後來暫不交易早期 CPF，取代重疊範圍內較早的全缺口要求，原來源與缺口仍保留。
- O0037 取消 SSH 接手方向，O0052 後來要求日常自動化；先前人工貼交接文字不是每批永久流程。
- TEJ O0062 的單筆重排授權後來由 O0065 擴為同一恢復任務的持續授權；核對目前政策與
  持久預算後承接，不把舊單筆限制當成每輪必問，也不授權本次稽核實際重送。
- O0026 指定「大台一口無限轉倉，補滿保證金」的期貨 benchmark。已記錄新需求，
  本輪沒有驗證其模式接線、轉倉、補款與收益分母；既有 fully collateralized 1x TX
  比較基準不能僅憑名稱視為等價。這是後續契約實作的待核項，不藉文件稽核改程式。

## 實際覆蓋

- 起始 Markdown 索引 **304 份**：24 新增、11 變動、264 與可用基線 hash 相同、
  5 份既有自訂 skill 文件缺前次 hash。最後一類有前次 skill 發現／報告證明存在，
  因此不誤算為新建 skill。本輪索引補齊它們的 hash，供下次增量使用。
- 5 份自訂 skill 入口與相關偏好／訓練 map、根指令已讀；資料、模型、儲存、重試、
  備份與新工作要求的相關段落按需審閱。其餘文件分為 keyword／引用篩查或 metadata
  比對；不是 304 份全文語意審查。逐檔層級見
  [Markdown inventory](../artifacts/prompt_audit/20261004T090138Z/markdown_inventory.json)。
- 本機 **185 份 session**：96 主對話、89 子代理。177 檔邊界未變、7 檔驗證為新增尾段、
  1 份新主對話；只串流 8 份主對話增量共 **377,876,350 bytes**，沒有重讀全歷史。
  邊界使用前次 prefix／tail hash 核對，解析錯誤與未完成半行均為 0。
- 107 筆 user records 排除 30 筆注入上下文、5 筆重複記錄，得到 **72 則候選**。
  候選分類後採用 **28 則直接要求／回答**作語意證據；轉貼部署摘要只作間接線索，
  混合訊息只採本人新增部分，問答只採 `answer`。
- [來源邊界](../artifacts/prompt_audit/20261004T090138Z/session_inventory.json)、
  [候選分類](../artifacts/prompt_audit/20261004T090138Z/dialogue_review.json)、
  [選用短引文](../artifacts/prompt_audit/20261004T090138Z/reviewed_preference_evidence.jsonl)
  保留時間與 byte offset；臨時完整候選文字在驗證後移除，原始 session 不變。
  沒有其他平台或帳號全部歷史的存取／審閱聲稱。

## 載入與驗證

- 本機 `codex-cli 0.160.0` 的 `config/read` 回傳 `project_doc_max_bytes=32768`，
  fallback filenames 為空；根 `AGENTS.md` 為 6,767 bytes，global AGENTS 為空。
  這是此版本、此設定的量測，不寫成永久上限。分題文件仍按任務載入。
- 5 個自訂 skills 的 `quick_validate.py` 通過；本機 app-server 的
  `skills/list(forceReload=true)` 發現唯一、正確路徑且 enabled 的入口。
  描述／觸發 metadata 未擴張；一般 Markdown 小修改不需要完整偏好稽核。
- 初始 29 份指令／相關文件的 117 個本機連結皆可達；88 個明確程式／設定／測試
  路徑可找到。最終修改檔另檢查連結、anchors、前後 diff、Git whitespace 與結尾換行。
- 28 筆選用證據回到原 session 的固定 byte 範圍驗 hash、時間、user 身分與引文；
  這證明原話位置，不把貼來的部署結果升為本機驗收。
- 人工核對只讀、只分析、既有授權、日常並行、有界重試、混合頻率空值、研究例外、
  date-count group 與實際 progress enum 的指令語意。沒有呼叫模型服務或做獨立模型
  行為評測，也沒有測 IDE 的 slash 選單。

收據：[skill 格式](../artifacts/prompt_audit/20261004T090138Z/skill_format_validation.json)、
[本機設定](../artifacts/prompt_audit/20261004T090138Z/instruction_config.json)、
[來源驗證](../artifacts/prompt_audit/20261004T090138Z/source_validation.json)、
[最終驗證](../artifacts/prompt_audit/20261004T090138Z/validation.json)。
未執行無關訓練、provider 查詢、服務重啟或資料回收；沒有更新私人 memory、套件 cache
或交易／訓練程式。指令整理不宣稱提升模型品質、速度或資料完整性。

## 修改與復原

修改清單：`docs/README.md`、四節點架構文件，以及自訂 skills 的 preferences 入口／
三份 references、training-reuse 入口／canonical map。本報告為新增文件。

`before_manifest.json` 與 `before/` 保留 8 份目標修改前的原文、hash、bytes／行數；
`existing_git_diff.patch` 及 staged diff 保存原有重疊變更，`changes.diff` 僅比較本輪
before／after。各入口前後 bytes／行數及最終 SHA 見 `validation.json`。
同時進行的其他工作可能更新文件，復原時必須核對後續差異，不能用 Git HEAD 或
整檔還原覆蓋他人的變更。
