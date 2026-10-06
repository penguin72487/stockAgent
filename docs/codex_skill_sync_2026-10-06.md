# Codex skills：penguin／Vastai1T 合併同步

已完成使用者指定的兩節點同步。penguin 原有 5 個 user skills／1 個 repo skill，
Vastai1T 原有 1 個 user skill；聯集後兩邊都是 **7 個 skills／30 個成員**，
內容、hash 和 mode 一致，Codex 實際可發現且啟用。雙節點對話與偏好更新見
[本輪 prompt audit](prompt_audit_2026-10-06.md)。

## 清冊

| Scope | 名稱 | 原來源 |
| --- | --- | --- |
| user | check-prompt-audit | penguin |
| user | user-working-preferences | penguin |
| user | stockagent-training-reuse | penguin |
| user | extend-stockagent-derivatives | penguin |
| user | stockagent-day-trade-strategy-switch | penguin |
| user | stockagent-exact-futures-optimization | Vastai1T |
| repo | stockagent-storage-operations | penguin checkout |

user root 為 `/root/.codex/skills`，repo root 為
`/root/stockAgent/.agents/skills`。這是本輪兩個實際 checkout 路徑，不是其他
機器的永久絕對路徑要求。沒有同步其他節點。

## 入口與共同基線

在 penguin 的 checkout 執行；output 必須為新的收據目錄：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/sync_codex_skills.py \
  --output artifacts/operations/skill_sync/NEW_PLAN

run_fintech_python scripts/sync_codex_skills.py --apply \
  --output artifacts/operations/skill_sync/NEW_APPLY
```

SSH、identity、port 及 roots 可用 CLI 指定，重用 `stockagent.remote_ssh`。
共同基線目前位於 `/root/.codex/skill-sync/vastai1T/base.json`；它綁定 peer 與
root mapping，改 peer／scope 須用另一 state root。不是依 mtime 挑較新的版本。

- 獨有 skill／成員取聯集，傳完整自訂目錄的 entry、UI、references、scripts／assets。
- 有共同祖本時接受單邊修改，獨立文字差異用 Git 三方合併；同一區段衝突、
  無祖本的同名差異及二進位衝突停止 apply，保留 `before.json` 供審閱。
- 不傳播刪除；來源 snapshot 變動則拒絕覆蓋。完整 staging 後逐 skill rename；
  舊目錄保存在對應 root 的 `.skill-sync-backups/<run-id>/<skill>`。
- 只有兩邊全部成員一致才推進基線。中斷後看 receipt 的已完成階段、重新盤點；
  回復按 before／backup 精確合併，保留後續修改，不整根覆蓋 skills。
- 排除 `.system`、plugin cache、hidden 暫存、sessions、auth、memory；symlink、
  非普通檔、私人 key／憑證成員拒絕同步。SSH key 僅供連線。

`check-prompt-audit` 已把這次指定的雙節點歷史／技能整理及同步納入 StockAgent
預設流程；技能同步不搬 raw history。只有按需入口，沒有新增 timer／常駐 writer。

## 驗收

第一輪 `establish-base` 安裝聯集並建立共同祖本：penguin 加入 Vast 的 6 個
成員，Vast 加入 penguin 的 24 個成員。之後改既有期貨優化 skill 的 3 個
文件，`audited-apply` 成功同步回 Vast，保留該 skill 原版備份；未出現衝突。

7 個 skill 格式驗證及 23 個相對連結通過。兩邊實際 Codex CLI 均為 0.160.0，
各自 `skills/list` 指定 `/root/stockAgent` 並 force reload，均回傳 7 個 enabled
skills、6 user／1 repo，errors 為空。Vast 使用實際 VS Code extension 的 binary，
沒有因非互動 PATH 無 codex 而誤判不可載入。

`idempotency` 再預覽 local／remote changes 和 conflicts 都為空。focused history／
sync tests 共 24 passed；skill format／discovery 不能替代服務、資料或訓練驗收。

收據根目錄為
[`20261005T160029Z`](../artifacts/operations/skill_sync/20261005T160029Z/)：

- [共同基線建立](../artifacts/operations/skill_sync/20261005T160029Z/establish-base/receipt.json)
- [整理後同步](../artifacts/operations/skill_sync/20261005T160029Z/audited-apply/receipt.json)
- [零差異重跑](../artifacts/operations/skill_sync/20261005T160029Z/idempotency/receipt.json)
- [兩邊實際技能發現](../artifacts/prompt_audit/20261005T161247Z/skill_discovery.json)
