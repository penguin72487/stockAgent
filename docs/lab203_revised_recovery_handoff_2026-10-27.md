# lab203 接手當期恢復版本與原件封裝

沿用 `storage-single-authority-20261025`、既有 ingress、Restic、NAS worker
及反向回執。新工具包為 **`tools/storage-relay-20261027-v4/`**；舊 v3
保留。此包只提供原始碼、完整儲存 skill 與文件，不含私有權杖、帳號、來源
payload、runtime 或 checkpoint，不自動替換服務或啟動訓練。

先以 Penguin 發布收據的固定 envelope 身分驗證 `READY`、封閉成員集合及
每檔 SHA，再讀本包 `.agents/skills/stockagent-storage-operations/` 的完整
`SKILL.md`、`agents/`、`references/`，核對實際 skill discovery。不要因收到了
工具包就回報已採用或已完成 NAS 備份。

相較 v3，此包完整包含 current input-recipe 的離線解析依賴，以及 named
`preserved_file_set_v1` clock 原件還原與失敗日誌處理。原版來源 enrollment、
checkpoint、當期恢復輸入與已核准執行目標必須分開核對；不得用原 enrollment
的舊 source proof 冒充新版已驗證，也不得省略原始 clock bundle、financial
representation bundle 或 stock auxiliary bundle。`preserved_file_set_v1`
要求原件逐位元組相同，不改為數值容差，也不宣稱 raw clock 重新運算可重現。

Penguin 的 `scripts/inventory_training_rescue.py` 凍結 v2 私有清冊。後續 payload
仍由既有 `private_vast_recovery_backup` encrypted Restic route 發布；每批
最多 8 GiB，保留 24 GiB staging、32 GiB pending、64 GiB reserve。新版清冊
需要自己的 plan identity、所有 part IDs 與 NAS snapshots；**不可沿用舊 v1
清冊的 1,275 檔／19,465,967,303-byte 完成數作為新版本驗收**。

請在隔離 NAS 還原目錄，以相同 plan 的所有批次還原內容執行包內原驗證器：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/verify_training_rescue.py \
  --restored-root /path/to/isolated/restored/private-root \
  --restored-plan /path/to/recovery-plan.private.json \
  --plan-id EXACT_CURRENT_PLAN_ID \
  --output /path/to/new/private-readback.json
```

多批次以 `--restored-part /path/to/next/part` 追加；每根需含該批 payload 與相同的
`recovery-plan.private.json`，不可混入另一 plan 的控制檔或缺少一批。
完整檔案 SHA／recorded bindings 通過後，仍按原 runtime 與 checkpoint loader
另驗 model、optimizer、scheduler、scaler、RNG、config、source 及 ABI。
離線檔案驗證不載入 Torch、不執行 builder，也不代表新的 CUDA 續訓成功。

沿原 `storage-relay-audit.json` 回傳實際採用的包、envelope、成員 SHA、skill
discovery、當期 plan／parts／Restic snapshots、原始離線驗證 JSON 與仍待驗項。
不改 paired ACK ABI、不刪原 Restic 歷史、不擴大公開來源分享權限，不把 native
NAS 壓縮率的 CIFS 不可觀測狀態寫成通過。

## Penguin 已固定的當期身分

此段在 v4 工具包封存後補入，不改已發布 v4 的原 bytes。v4 envelope 為
`62ff930acdee9ea908bd53c8a123954e8c18c0849c2eaadff8ab07a6ce6e0f5d`；
Penguin 的另一個程序已完成全檔 SHA 讀回，這不等於 lab203 已採用。

2026-10-27 02:42 UTC 凍結的私有 v2 plan 為
`c239d6cfbe6f242cad670106fe518e4cc2e9cafcd135800af3d7e1037c7e75ff`，
包含 **1,422 檔、23,581,982,125 bytes**，保護一個未提交 partial。
三個 run 各自帶匹配的 source proof：08:45 epoch 265、09:00 epoch 402、
TW embedding epoch 121。這是原已保存 checkpoint 的 epoch，不是新訓練進度。
本 plan 已排為 5 個有界 parts，必須取得各自原 paired NAS ACK，再做全 union
還原；原 v1 的五批 ACK 不能替代。請將實際驗證結果沿原 audit 通道回傳。
