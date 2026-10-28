# lab203 接手：固定私有續訓救援備份

工程任務沿用 `storage-single-authority-20261025`。此版本補交完整儲存 skill
與續訓清冊，沿用既有 ingress、Restic、owner 與反向收據；不替換服務、不自動
執行，不新增公開入口。目前應接手 **`tools/storage-relay-20261026-v3/`**：
它已包含完整 skill 與原離線驗證器，下方「補交原離線驗證入口」提供命令。
v3 的固定 envelope identity 為
`a423732c409e35fe2a56ce49ffbbc6c4d458d8196402baad1a3d00cbeb827d2b`；
先驗其封閉 envelope、`READY` 與全檔 SHA，再讀包內文件。舊 v2 原件保留，
不覆寫其內容；本文件後續的入口說明不改變已發布 v3 包的原始 bytes。
不能把 Penguin 已發布視為 lab203 已收到。

2026-10-26 固定清冊的身分為
`647174630a7b03d5d0d882ad89ed64e532255cfa5226fbe6949da58d7d37d2d3`。
這是 canonical JSON 內容身分，不是檔案 byte SHA。它包含 1,275 個成員，
19,465,967,303 bytes：1,139 個私有 CAS 物件及 136 個不可變 JSON 控制檔。
一個零位元組的未提交 partial 留在 Penguin，不計入可恢復集合、也不刪除。
各批 envelope 額外帶同一份 `recovery-plan.private.json`，其實際 byte SHA 由
envelope 固定；需從所有批次還原並驗證 exact union，不能只驗摘要數字。

| 已保存的 run | 最新固定 epoch | committed context | 維持的訓練設定 |
| --- | --- | --- | --- |
| futures-history-v3-capacity-0845-attempt00 | 265 | `41947b935534963d86cf98bef4cf77ae901b5b3019a4fb30873ad99282e2f855` | global batch 128、2 ranks |
| futures-history-v3-capacity-0900-attempt00 | 402 | `b55a4f2aed1944ddbc17473fdd30d62ce739d7a2f239e260103513a4693053fe` | global batch 128、2 ranks |
| tw-v6-embedding-d128 | 121 | `4c93329e9f53ebedcc90092d6756b70be8fdafacca485ed6cdab88d86b682c66` | global batch 256、2 ranks |

請交給 lab203 Codex：

> 先讀本包完整 `.agents/skills/stockagent-storage-operations/` 的 `SKILL.md`、
> `agents/`、`references/`，核對實際 skill discovery，並沿既有
> `storage-relay-audit.json` 回報包的 envelope 身分及實際成員 SHA。
> 不要更改舊 frozen 包，也不要把「已讀取」當成服務已部署。
>
> 私有來源為 Penguin D 的 `.local-state/training-rescue`，不在公開 immutable
> lake/export 範圍；本次只沿已存在的 `private_vast_recovery_backup` 加密 Restic
> 路徑備份。以 dispatch 的 `catalog_identity_sha256` 等於上述固定清冊身分來
> 選批，驗 `delivery_kind`、paired producer/receiver/repository、envelope/READY、
> 每檔 SHA 與實際 complete bytes。禁止從來源掃描或動態 latest 猜範圍。
>
> 各批 payload 在 `private-rescue/<清冊身分>/` 下；其
> `recovery-plan.private.json` 必須 byte-for-byte 相同。驗計畫身分、1275 個
> 成員的 exact union、每檔 byte SHA／長度、無重複衝突或額外成員。分批控制檔
> 副本另計，不把它們加進 1275 個來源成員。批次尚未全部到齊就回報 pending。
>
> 沿既有 Restic owner 備份並做獨立 NAS 還原驗證，保留原嚴格 paired ACK ABI。
> 另外以版本化 audit 記錄 plan 身分、part 身分、NAS snapshot、還原檔數/bytes/
> 全 SHA、驗證時間與 scope。不要另開 backup owner，不將私有模型、optimizer、
> RNG、source/runtime capsules 或 run controls 明文送進 immutable lake。
>
> 提交檔是 canonical JSON 的內容 digest；檔案 byte SHA 可能不同。以原本 rescue
> verifier 驗各 run 的 commit/spec/context 綁定及 model/optimizer/RNG/config/
> source/runtime 引用閉包，不能用 Restic exit 0 代替這層驗證。若目前 worker 包
> 未含原 verifier，先回報「file restore verified、semantic restore pending」並
> 索取對應固定版本，不能改造另一套 parser 產生通過結果。
>
> 本次不啟動 GPU 或訓練、不更改 enrollment、不刪來源及 NAS 歷史。檔案備份、
> 語義還原、目的地 CUDA／記憶體 admission、舊 worker fence、真正跨主機續訓是
> 不同證據。原指定 full financial builder 的 160 GiB admission 仍未通過，不能
> 因備份成功宣稱原 scope 已能重建。NAS 原生壓縮若只有 CIFS 可觀察就維持 unknown。

Penguin 的 publication receipt 為私有操作產物
`artifacts/operations/storage-single-authority-20261025/private-rescue-nas-publication.private.json`。
該檔記錄每批 envelope 身分及是否收到嚴格 NAS ACK；工程工作完成或資料已放入
本機 ingress 不等於 NAS 驗收完成。精確中繼回收仍須原還原、消費者、同步與
quarantine gates；本交接不授權刪除唯一資料。

## 補交原離線驗證入口

`tools/storage-relay-20261026-v3/` 是獨立封閉的補充包，包含原
`scripts/verify_training_rescue.py` 與其標準函式庫依賴
`stockagent/training_rescue_worker.py`；舊 v2 包不覆寫。先以既有配對通道的
envelope、READY、exact set 與逐檔 SHA 驗包，確認實際到齊再執行。完整 skill
與交接文件也包含在 v3，不需要另一份 checkout 或從 moving latest 取程式。

以下路徑由 lab203 依實際獨立 Restic restore 選定，**不要指向 ingress 原本
傳輸副本冒充 NAS 還原**。每個 part root 都是還原目錄內的
`private-rescue/647174630a7b03d5d0d882ad89ed64e532255cfa5226fbe6949da58d7d37d2d3/`。
使用既有 Python 3.10+；此入口只有標準函式庫，不需 Penguin enrollment、
API 金鑰、GPU、服務連線或安裝另一套套件。`-S -B` 禁用 site 與 bytecode
寫入。範例中的絕對路徑是待替換的實際 restore 路徑：

```bash
python3 -S -B /absolute/verified-v3/scripts/verify_training_rescue.py \
  --restored-root /absolute/NAS-restore/part-0/private-rescue/647174630a7b03d5d0d882ad89ed64e532255cfa5226fbe6949da58d7d37d2d3 \
  --restored-part /absolute/NAS-restore/part-1/private-rescue/647174630a7b03d5d0d882ad89ed64e532255cfa5226fbe6949da58d7d37d2d3 \
  --restored-part /absolute/NAS-restore/part-2/private-rescue/647174630a7b03d5d0d882ad89ed64e532255cfa5226fbe6949da58d7d37d2d3 \
  --restored-part /absolute/NAS-restore/part-3/private-rescue/647174630a7b03d5d0d882ad89ed64e532255cfa5226fbe6949da58d7d37d2d3 \
  --restored-part /absolute/NAS-restore/part-4/private-rescue/647174630a7b03d5d0d882ad89ed64e532255cfa5226fbe6949da58d7d37d2d3 \
  --restored-plan /absolute/NAS-restore/part-0/private-rescue/647174630a7b03d5d0d882ad89ed64e532255cfa5226fbe6949da58d7d37d2d3/recovery-plan.private.json \
  --plan-id 647174630a7b03d5d0d882ad89ed64e532255cfa5226fbe6949da58d7d37d2d3 \
  --output /absolute/private-evidence/rescue-offline-audit-NEW.json
```

直接驗五批 exact union，不組裝另一份 19.5 GB payload。各批計畫副本須逐位元
組相同；缺少、衝突、重複來源、額外檔案、symlink／FIFO、資料／目錄變動均
拒絕。完成每檔完整 SHA／長度，再以原 rescue `digest`、`validate_spec` 及
reference validators 核對 spec／state／commit／context、記錄的 checkpoint
epoch／world size／batch、bootstrap、input capsule 與原小型控制引用。輸出
必須在還原集合外，已有結果不能覆寫。

這個 audit 不反序列化 checkpoint、不解封 runtime，也不啟動訓練。它明確回報
`semantic_restore_pending`、`fresh_checkpoint_CPU_verified: false` 及每個 run
的缺項；不能把舊 checkpoint attestation 當本次 CPU 或 GPU 驗證。目前兩個
期貨 run 的固定控制檔沒有 bootstrap／source readback，應照實保留 pending，
不能拿其他 run 的 capsule 或新版來源替代。需要進一步 CPU 驗證時，仍由已固定
的原 source／runtime 與 `checkpoint_summary`／canonical loader 驗證；本補交
不降低原 scope 的 CUDA、160 GiB builder admission 或舊 worker fence。

沿既有 audit 回執附上固定 plan／part／snapshot IDs、實際驗證入口 source SHA、
原 JSON 結果、檔數／bytes／時間與 pending，不改 paired batch ACK ABI。
本機成功試跑不會寫 NAS ACK；真正 NAS restore 的檔案、recorded bindings、
checkpoint CPU 與實際遠端續訓仍各自驗收。
