# lab203 固定底本與私有增量還原接手

從既有 ingress 取得 **`tools/storage-relay-20261027-v7/`**，以 Penguin 發布
收據核對 envelope identity、READY、完整成員及全部 SHA。讀取內附完整
`.agents/skills/stockagent-storage-operations/`。此包包含純標準函式庫的
`training_rescue_backup.py` 及新版離線 verifier；保留 v3～v5 工具原 bytes。
工具送達不自動執行，不更換 NAS／Restic worker、不連 GPU、不帶私鑰。

## 本次固定範圍

「工具 v7」與「資料 v7／底本 v6」是不同的版本範圍。工具 v6 在實際發布
預檢發現 JSON 欄位順序會改變 manifest bytes，未傳出增量 payload；保留該包
作為失敗證據。本包固定新 manifest 的完整遞迴 key 排序；舊 envelope 繼續使用
原 writer 的 insertion-order 格式，不改寫舊收據或既有封裝。

| 項目 | 固定身分與範圍 |
| --- | --- |
| 舊資料底本 | `c239d6cfbe6f242cad670106fe518e4cc2e9cafcd135800af3d7e1037c7e75ff`；1,422 檔／23,581,982,125 bytes／5 批 |
| 新資料 v7 | `7e4a6dae0900470394f55ec5e88453caefb78d5aff81ee98db55fa7333137e45`；1,448 檔／23,587,819,110 bytes |
| 增量 manifest | `eb00914c12668e2a46382b22e003a6c2c76890e1a35585699f9288a89f25eecf` |
| 新傳 payload | 26 檔／5,836,985 bytes；封裝另含 3 個控制檔、envelope 與 READY |

Penguin 已於 2026-10-27 核對五份原始 paired file-restore ACK。這些舊 ACK
只允許不重傳底本；新 v7 仍須從 NAS 取得五個原 Restic snapshot 及一批增量，
將完整聯集逐檔讀完、比對 SHA。凍結後的新 journal 另屬後續增量，不能宣稱
本清冊涵蓋所有未來的活動控制檔。舊底本缺失或 SHA 不符時拒絕還原，保留證據，
回報需要補傳的固定檔案，不能改寫底本／ACK 以略過問題。

## 以原管線還原

沿原 encrypted Restic、既有 paired dispatch／ACK、原 NAS 容量檢查與 owner
執行。先驗每個封閉 envelope 的全部檔案，再保留每批的
`private-rescue/PLAN_ID/` 作為下列 root；不要把多批混在同一根或移動 payload。
舊五批的 ordinal 與 envelope identity 依序為：

1. `e429a696b32afd0590c2320c572d038ed5573f9677a9259e832afc67ad9a5aff`
2. `b70836605a1e2f0d2641dbd24ac82316455f1f7124bad789c11e8d73d1de98f5`
3. `792b0d4691e4672f038041e5256965d3f1cddabb5cdf37a4a1adba39d2dfd2aa`
4. `fa9462c0c637265f5d41af61f91cbff892060100de0d04ea5df70669c533245a`
5. `81d99491108d7a880b0c8ee4d49b3b3a2bcabf3da3fe8f7eaa63e1ff78049375`

每個舊 root 保留其原 `recovery-plan.private.json`。增量 root 必須保留新版
`recovery-plan.private.json`、原 `base-recovery-plan.private.json` 與
`incremental-backup.private.json`。兩份 plan 的 raw SHA、paired identities、
所有舊 dispatch／ACK、分批歸屬與增量 manifest 都由 verifier 重新核對。
不得用新的 plan 覆蓋舊 root 內的原 plan。

在核對過的工具包內執行；把下列六個 root 換成 NAS 實際隔離還原位置，
output 使用尚不存在的私有檔名：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/verify_training_rescue.py \
  --restored-root /restore/delta-0/private-rescue/7e4a6dae0900470394f55ec5e88453caefb78d5aff81ee98db55fa7333137e45 \
  --base-restored-part /restore/base-0/private-rescue/c239d6cfbe6f242cad670106fe518e4cc2e9cafcd135800af3d7e1037c7e75ff \
  --base-restored-part /restore/base-1/private-rescue/c239d6cfbe6f242cad670106fe518e4cc2e9cafcd135800af3d7e1037c7e75ff \
  --base-restored-part /restore/base-2/private-rescue/c239d6cfbe6f242cad670106fe518e4cc2e9cafcd135800af3d7e1037c7e75ff \
  --base-restored-part /restore/base-3/private-rescue/c239d6cfbe6f242cad670106fe518e4cc2e9cafcd135800af3d7e1037c7e75ff \
  --base-restored-part /restore/base-4/private-rescue/c239d6cfbe6f242cad670106fe518e4cc2e9cafcd135800af3d7e1037c7e75ff \
  --restored-plan /restore/delta-0/private-rescue/7e4a6dae0900470394f55ec5e88453caefb78d5aff81ee98db55fa7333137e45/recovery-plan.private.json \
  --base-restored-plan /restore/delta-0/private-rescue/7e4a6dae0900470394f55ec5e88453caefb78d5aff81ee98db55fa7333137e45/base-recovery-plan.private.json \
  --incremental-manifest /restore/delta-0/private-rescue/7e4a6dae0900470394f55ec5e88453caefb78d5aff81ee98db55fa7333137e45/incremental-backup.private.json \
  --plan-id 7e4a6dae0900470394f55ec5e88453caefb78d5aff81ee98db55fa7333137e45 \
  --incremental-id eb00914c12668e2a46382b22e003a6c2c76890e1a35585699f9288a89f25eecf \
  --output /restore/private-union-readback-new.json
```

驗收結果契約為 `stockagent.private_training_rescue_offline_readback.v3`，
須包含完整 1,448 檔與 23,587,819,110 bytes，底本和增量都完整重讀。此命令
不載入 Torch、不解封執行 runtime、不反序列化 checkpoint，也不啟動 GPU。
沿原 `storage-relay-audit.json` 回報工具 envelope／成員 SHA、skill discovery、
六份 Restic snapshot、完整離線結果與尚缺的語義恢復階段。NAS file ACK、
全聯集驗證、來源語義重建、checkpoint CPU、新 GPU 續訓各自回報；都不授權刪原件。
