# StockAgent prompt audit — 2026-09-28

本次已整理專案指令與三個自訂 skills。主要問題是入口超過載入上限、
歷史實驗被誤寫為通用基準，以及過時／過度擴張的工作流程。
沒有改變交易、訓練、服務或資料程式的執行行為，也沒有啟動訓練或部署。

## 範圍與證據

- 完整閱讀：原始 `AGENTS.md`，三個 StockAgent 自訂 `SKILL.md`、其四份 references
  與三份 UI metadata；檢查相關程式／config 以判斷指引是否過時。
- 專案指令檔搜尋包含隱藏目錄；本工作樹未找到其他 `AGENTS*.md`、`CLAUDE*.md`、
  `SKILL.md` 或專用 `.instructions.md`。`~/.codex/AGENTS.md` 為空。
- 套件／系統目錄遞迴盤點到 79 個 skill metadata，包含未列入本輪可用清單的內部
  skills；僅做 metadata 篩查，**不是 79 份正文已完整審核**。
- `.env`、憑證值、私人記憶與一般研究 Markdown 不屬於本次改寫範圍。
- Git 原本已有大量未提交修改；原始 `AGENTS.md` 也已修改。本次以當時工作樹為
  基準，完整保留其新增的 futures funding-recovery gradient 契約。

稽核原檔、SHA-256、修改前 Git diff、逐段搬移對照與檢查結果位於
[`artifacts/prompt_audit/20260927T181209Z`](../artifacts/prompt_audit/20260927T181209Z)。
目錄名稱為 UTC；本報告日期為 Asia/Taipei。

## 主要發現與已做修改

| 優先度 | 問題與證據 | 處理 |
| --- | --- | --- |
| 高 | 原 `AGENTS.md` 2,197 行／151,637 bytes；32 KiB 截點在原第 499 行的 `prior-completed stock feat`，與本次實際注入截點一致。後面的訓練、因果、驗證等規則未完整自動載入。 | 入口改為共通契約與任務索引；詳細規則移至 13 份按需查閱文件。 |
| 高 | 原第 563–568、657 行附近同時稱 `financial_transformer` 為預設、舊 Transformer-base lookback-32 為 active/latest；原第 806 行附近又通用要求 forced L1，與前面的 `learned_cash` 衝突。 | 舊 Transformer 設定明確標為特定歷史實驗；forced-L1 段落限於相容控制組。保留產品專用 ABI 與新台股 learned-cash 契約。 |
| 高 | derivatives reuse map 要求在 `train.py` 加 dispatch 分支；現行 `train.py` 已呼叫 `dispatch_specialized_training_mode`，`stockagent/training/mode_adapter.py` 擁有 `TrainingModeSpec` registry。 | 改為新增 registry entry，移除過時的頂層分支指引；checkpoint 指引對齊共用 writer／contract。 |
| 中 | training skill 的驗證清單以 `minute_run_manifest.json` 或 mode equivalent 為主，但 lifecycle 已統一 `run_manifest.json`／`progress.json`／`summary.json`。 | 修正 canonical 名稱，保留 legacy manifest 只能作相容鏡像的限制，加入既有 completion validator。 |
| 中 | `Epoch-Level Timing And Throughput` 中相同 compiled-settlement 契約和測量結果出現兩次。 | 移除第二份完全相同的 17 行／1,288 bytes；第一份及其數字保留。 |
| 中 | skills 要求每次完整讀取 reuse map、跑全部搜索，以及無條件建所有分區／做訓練 smoke。 | 改為按受影響契約選擇閱讀與驗證。新 mode、共享 lifecycle／resume 變更仍需相應 smoke；使用者要求完整歷史或完整 epoch 時仍須完成全部範圍。 |
| 中 | training skill 用 epoch 2+ 作通用效能驗收，與專案指定 vastai1T 台股需雙 GPU DDP、完整 fold、epoch 3+ maximum-rank wall time 不一致。 | 將指定硬體的驗收條件寫清楚，epoch 2 在該場景只屬診斷證據。 |
| 中 | skill descriptions 以檔案路徑／普通 config 修改作廣泛觸發，可能把小改動升級成完整重構或部署流程。 | 收窄三個自訂 skills 的觸發條件；一般 config／文件編輯不自動觸發訓練重構或策略切換。 |
| 中 | 自演進段落要求把每個反覆失敗的規則追加到入口，易持續累積歷史事件與重複指引。 | 改為更新對應主題文件／日期報告，保留證據與適用範圍，入口只放跨專案共通規則。 |

`learned_cash` 判定另核對了
[`tw_day_trade_last_last_only_training_vastai1t.yaml`](../configs/deployments/tw_day_trade_last_last_only_training_vastai1t.yaml)
與其 v6 繼承設定。這不代表本次驗證了線上部署狀態。

## 整理後的載入方式

入口 [`AGENTS.md`](../AGENTS.md) 現為 **95 行／6,575 bytes**，相較原入口縮小
**95.66%**。這是自動載入入口的位元組差異，不是推論 token、速度或模型品質提升的測量。
整套文件因新增任務範圍說明而稍微增加；必要契約沒有靠大量刪除而消失。

| 任務 | 詳細文件 |
| --- | --- |
| Runtime、GPU 與測試 | [runtime.md](agents/runtime.md) |
| Storage、Syncthing、清理與復原證明 | [storage.md](agents/storage.md) |
| Discord／TAIFEX 服務 | [services.md](agents/services.md) |
| 模型預設與精度 | [models.md](agents/models.md) |
| 舊 Transformer-base 實驗／架構 | [transformer_reference.md](agents/transformer_reference.md) |
| Panel、特徵、crypto 資料 | [data.md](agents/data.md) |
| 交易／回測／loss／TW replay | [execution.md](agents/execution.md) |
| Walk-forward 與 benchmarks | [walk_forward.md](agents/walk_forward.md) |
| Training lifecycle／DDP／resume | [training.md](agents/training.md) |
| Compile／cache／效能 | [performance.md](agents/performance.md) |
| TW 公開來源／parser／價格與資格規則 | [tw_public.md](agents/tw_public.md) |
| Explainability／報告覆蓋 | [explainability.md](agents/explainability.md) |
| 指定 v2–v15 股期實驗歷史 | [futures_history.md](agents/futures_history.md) |

移動的詳細契約仍在其適用範圍內有效。日期、版本、quarantine、研究例外與
replay 條件保留；不同版本的近似段落若範圍不同，沒有當成重複任意合併。
特別保留 PIT、fees/masks、checkpoint、真實資料來源、270 點分鐘曲線、儲存復原與
程序引用等驗收條件。

三個自訂 skills 位於本機 `~/.codex/skills/`，不在本專案 Git 內：

| Skill | Description 字元數：前 → 後 |
| --- | ---: |
| `stockagent-training-reuse` | 499 → 212 |
| `extend-stockagent-derivatives` | 418 → 195 |
| `stockagent-day-trade-strategy-switch` | 366 → 206 |

共修改 6 個既有檔案：專案入口、三個 skill 入口，以及 training verification checklist
和 derivatives reuse map。既有 skill 的 invocation policy 與 UI 設定保持原樣。

## 套件 metadata 待評估項目

`shioaji` description 為 1,020 字元，`google-docs` 為 988；另有 investing routers
與 Data index 的長描述。長度是需要檢查的訊號，不能單憑長度認定功能過時或必須刪除。
若觸發誤判確實出現，應在技能來源套件中收窄條件並驗證功能，再透過正常更新流程安裝。
本次沒有直接改寫會被套件更新覆蓋的 cache，也沒有宣稱其他 skills 已完成語意審核。

## 驗證與界線

- 三個自訂 skills 均通過 Skill Creator 的 `quick_validate.py`。
- `git diff --check` 通過。
- 搬移檢查逐段還原 5 項已記錄的文字整理，確認 13 份來源區段與修改前內容一致；
  原檔全部非空行均有搬移或共通規則整併歸屬。
- 原先未提交的 futures funding-recovery penalty 規則完整保留。
- 21 份修改後入口／契約／skill Markdown 的 code fence 與 20 個相對連結通過檢查。
  新契約中可直接辨識的 literal repo 檔案引用沒有本機缺失。
- 這是文件／流程稽核，沒有跑訓練、金融回測、服務重啟或品質 A/B；不宣稱
  新指令一定提升報酬、模型品質或工具成功率。
- 開新 Codex 工作階段後才可驗證完整的新自動載入鏈；本輪既有對話仍含舊注入內容。
- 如需復原，只對照本次保存的 before 檔案與當前 diff 還原這些文件；不要用
  `git reset` 或 HEAD 版本覆蓋使用者原先的未提交修改。

## 官方依據

Codex 專案指令預設合計上限為 32 KiB，skills 先載入名稱／description，再按需要
讀取正文；官方也建議限制觸發範圍、按任務讀取文件，區分真正契約與過度流程化要求。
見 [AGENTS.md discovery](https://learn.chatgpt.com/docs/agent-configuration/agents-md)、
[Build skills](https://learn.chatgpt.com/docs/build-skills) 與
[Rethinking skills and prompts](https://developers.openai.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra)。
