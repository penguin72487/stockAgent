# Prompt／偏好與雙節點技能稽核：2026-10-13

完成 penguin／Vastai1T 主對話增量整理、既有技能更新與合併同步。這輪主要修正
「已允許數值／梯度實驗，仍被舊逐位相同比較擋住」及「機器附加訊息被當成
使用者偏好」；保留來源、因果、帳務、恢復與相容性契約。

## 範圍與證據

證據根：[`artifacts/prompt_audit/20261013T143821Z/`](../artifacts/prompt_audit/20261013T143821Z/)。
主對話基線沿用 2026-10-06 訓練優化稽核的兩個節點索引，只讀新檔／新增區段；
Markdown 與最近可用的 2026-10-05 文件 hash 索引比較。沒有重播歷史操作指令。

| 項目 | 本輪覆蓋 | 不能據此宣稱 |
| --- | --- | --- |
| penguin sessions | 193 檔；104 主對話、89 子代理；串流 17 份主對話新增區段，1,276,254,128 bytes | 全部歷史助理／工具訊息逐字語意審閱 |
| Vastai1T sessions | 36 檔；31 主對話、5 子代理；串流 4 份主對話新增區段，19,001,419 bytes | 其他機器或其他平台全部聊天已取得 |
| 使用者候選 | 原角色抽取 280 則；排除 114 自動 goal＋1 skill 附件，165 則全文精讀 | 使用者 role 的每一段都是本人意見 |
| 採用證據 | 42 則短引文；全部 280 source record 的 SHA／時間／角色／過濾結果回核，42 引文回核 | 轉貼助理文字或子代理任務增加偏好信心 |
| repo 文件 | 395 份文件做路徑／hash 索引；116 份新增索引或 hash 改變，依主題精讀相關段落 | 395 份文件全部全文審核或新增索引等於本週新寫 |
| 自訂技能 | 7 個入口全文核對；33 個目錄成員盤點、frontmatter／連結／UI metadata 檢查，相關 references 精讀 | 系統／plugin cache 全文重寫、模型端到端品質已改善 |

索引、精讀與排除見 [對話分類](../artifacts/prompt_audit/20261013T143821Z/dialogue_review.json)、
[來源回核](../artifacts/prompt_audit/20261013T143821Z/source_validation.json) 及
[短引文](../artifacts/prompt_audit/20261013T143821Z/reviewed_preference_evidence.jsonl)。
S 編號只指本輪，不與較早偏好索引的 N／R 編號混用。

## 發現與處置

| 原位置／行為 | 目的與問題 | 證據及處置 |
| --- | --- | --- |
| `prompt_history.request_text` 只辨認無屬性的 internal tag | role 不能證明原話；帶 `source="goal"` 的續作提醒與 `<skill>` 正文會自我強化舊指令 | 114＋1 筆實際誤判。修正開頭 tag 過濾，保留本人要求稽核／引用 tag 的文字；增加 collect 級回歸 |
| `runtime.md`「require exact artifact/metric parity」及 exact skill 的 identical／equality 要求 | 純工程優化需要相同語意；已選數值／optimizer 實驗則須有明確比較，不能被歷史 bit comparator 永久否決 | S0002／S0005／S0206。新增 [數值實驗契約](agents/performance.md#numerical-and-optimizer-experiments)，runtime 引用同一來源；exact skill 分類並按本次容差驗收 |
| 上架 skill 只有 replacement 流程 | 新增獨立 paper 模式要保留舊帳戶與成交；replacement 身分規則不能直接套到 addition | S0226；現行 CLI 已有 `--add-mode`／`--candidate-preflight`。更新 description、UI、plan/apply 及全 mode union 驗收，候選 preflight 仍不能 apply |
| 歷史 fold10／舊加速設定 | 編號、硬體、shape 與測量分母是觀察；最新實驗選擇才決定範圍 | S0047–S0051／S0251。training reuse 記錄最後 fold 依日期解析、沿 baseline 配方、保留 d128／lookback256、OOM 才改容量，失敗留證據後繼續 |
| 偏好只含舊版工程取捨 | 重複提出的新要求應合併到原偏好；不能再造每段對話一個 skill | 沿用 18 組 P 偏好：來源 row／單位／sidecar、方法可重建、provider 排程與恢復、每項消融圖、全鏈延遲、盤後 cache、價格用途與救援目的機 |
| 兩邊 skills 各有未同步修改 | 覆蓋單邊會丟失既有知識；hash／共同基線及整個目錄才是同步單位 | 先合併既有三份內容及兩份 mode 差異，再更新技能；兩次 apply 均無衝突、保留 backup，最終預覽零差異 |

數值契約保留固定 source/actions 下的離散帳務與金融 recurrence 檢查、來源原值／
單位／時鐘、fees／settlement、整股、masks 與吸收式破產。不同模型軌跡不要求
交易清單或 checkpoint 逐位一樣；不相容的精度／batch／梯度版本仍另建 artifact，
不能接回舊 optimizer。這輪沒有修改訓練、交易或數值計算程式。

遠端兩份 topic 契約只發布本輪修改段落，保留其其他內容；沒有用整份本機檔案
覆蓋遠端版本。見 [遠端段落差異](../artifacts/prompt_audit/20261013T143821Z/remote_doc_delta.patch)
與 [備份／讀回收據](../artifacts/prompt_audit/20261013T143821Z/remote_doc_install.json)。

## 偏好與技能更新

既有 [user-working-preferences](/root/.codex/skills/user-working-preferences/SKILL.md)
入口只補當次數值允許與核對日期；細節放工程、呈現及來源 references。
採用原話／取代範圍見 [證據索引](/root/.codex/skills/user-working-preferences/references/evidence.md)。
另外更新 check-prompt-audit、training reuse、exact futures optimization、day-trade
strategy switch 及 storage operations；extend derivatives 維持其現有產品／事件契約。

| 入口 | before → after bytes／行數 |
| --- | --- |
| 根 `AGENTS.md` | 6,767／97 → 6,767／97 |
| user preferences | 3,903／53 → 4,006／54 |
| exact futures optimization | 7,678／99 → 8,599／103 |
| strategy switch | 4,010／43 → 4,570／45 |
| 其餘四個入口 | 正文未改；本輪知識按需存放 references |

沒有把七日租約、正式成果七日保留、smoke discard 或 Stop 混成同一回收許可。
救援由使用者指定並確認新 SSH；價格的內部模型用途、來源公開許可與本人模擬
交易報告分開；毫秒目標、研究 benchmark 與盤後 cache 都不當成已驗收結果。

## 驗證與可復原性

- `run_fintech_python -m pytest -q -s test/test_prompt_history.py`：**9 passed**。
  實際 280 個候選 source record 回核亦通過；不需重讀 1.3 GB 的其他 payload。
- 7 skills 的 Skill Creator frontmatter 檢查通過；Markdown 連結／anchors、
  UI metadata 及相關新增／不觸發情境另行檢查。人工指令情境核對不宣稱模型行為 eval。
- [最終同步](../artifacts/prompt_audit/20261013T143821Z/sync-apply/receipt.json)：
  7 skills／33 members、兩邊完整成員及 mode 相同、零衝突。
  [冪等預覽](../artifacts/prompt_audit/20261013T143821Z/sync-idempotency/receipt.json)
  的 local／remote changes 皆為空。
- [實際載入](../artifacts/prompt_audit/20261013T143821Z/skill_discovery.json)：
  penguin `codex-cli 0.162.1`，Vastai1T `0.162.0-alpha.17.2`；
  app-server `skills/list(forceReload=true)` 各 7 個 enabled、解析錯誤 0，沒有啟動模型推論。
  `config/read` 的當前 model 為 `gpt-6.1-sol`，兩邊 `project_doc_max_bytes=32768`，
  fallback filenames 為空。這是當前設定，不寫成所有未來模型的永久載入限制。
- before 原文、hash、相關 Git diff 與每次同步 backup 保留在證據根；從當前未提交
  內容存副本，沒有以 HEAD 還原。`docs/agents/services.md` 有其他工作並行變更，
  本輪未修改；其變化不算本次成果。

完整驗收與修改清冊見 [validation.json](../artifacts/prompt_audit/20261013T143821Z/validation.json)。
私人候選全文在短引文回核後移除；原始 sessions／archived sessions 不修改／不同步，
不讀憑證；私人 memory 僅只讀導航，未寫入。

## 保留的缺口

penguin 一筆紀錄含 **789 NUL bytes＋換行**，原始 SHA 與 byte offset 已獨立讀回，
仍無法解析。保留 [缺口紀錄](../artifacts/prompt_audit/20261013T143821Z/history/parse_error_readback.json)
及下輪重讀邊界，不推測其 role／內容；history command 的 exit 2 如實保留。
其他可讀候選與技能同步的驗收已完成。

本輪不重新認證文件中的策略收益、GPU 性能、服務部署或所有資料完整性，也不
宣稱縮短文字必然提高模型品質。這是指令、證據抽取與技能一致性的工程驗收。
