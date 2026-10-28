# lab203 啟動保全綁定讀取器接手

沿原 ingress 接收 **`tools/storage-relay-20261027-v5/`**，核對發布收據的
envelope identity、`READY`、完整成員及 SHA。此包包含完整原儲存 skill、
離線 verifier、input-recipe、runtime identity 與 bootstrap binding validator；
只提供公開程式和操作文件，不含 runtime、checkpoint、來源 payload 或私鑰。
讀取完整 `.agents/skills/stockagent-storage-operations/SKILL.md` 及 references。
保留 v3／v4 原 bytes、原 NAS worker／Restic／paired ACK，不啟動任何 GPU。

新增 `stockagent.training_bootstrap_binding.v2`，為原 checkpoint-only 登錄綁定
既有 D 環境與 frozen code。原 `spec.json`、checkpoint、config、輸入 revision
及訓練軌跡維持原身分；新 binding 的 runtime lock 讀自已提交的原
`run_manifest.json`，不是目前控制機的套件版本。離線驗證必須帶上同包的
`stockagent/training_rescue_bootstrap.py`、`stockagent/runtime_identity.py`、
`stockagent/training_rescue_worker.py` 及 input-recipe 模組，不能靠 lab203
另一路 checkout 或現有 Python 安裝補 import。

先完成舊有當期 v6 五批 plan
`c239d6cfbe6f242cad670106fe518e4cc2e9cafcd135800af3d7e1037c7e75ff`
的所有 paired ACK 和完整 union 還原；該封閉 plan 不包含此後新增的 bootstrap。
新的私有清冊必須沿既有 encrypted Restic 路由單獨凍結、分批、驗所有 ACK，
再按其新 plan 身分完整還原。新工具包送達不等於新私有資料已備份。

隔離還原的命令維持原入口：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/verify_training_rescue.py \
  --restored-root /path/to/part0 \
  --restored-part /path/to/part1 \
  --restored-plan /path/to/recovery-plan.private.json \
  --plan-id EXACT_CLOSED_PLAN_ID \
  --output /path/to/new/private-readback.json
```

每根保留該 part 原 payload 與同一份 plan。新 binding 的原 provenance manifest
也是必需物件；不得省略、替換成新版 manifest 或改寫原 enrollment。未知 binding
版本拒絕，舊 bootstrap 維持原 file-list 完整等值驗證。

沿原 `storage-relay-audit.json` 回報實際採用的 v5 envelope／成員 SHA、skill
discovery、新舊 plan、parts、Restic snapshots、完整離線結果與未完成 gate。
若尚未採用 v5，明確回報缺少工具，不把 import 失敗當成來源資料已損壞。
此流程不載入 Torch、不改 CUDA、不證明異機 optimizer 續訓成功，也不授權刪原件。
