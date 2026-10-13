# Vast 訓練救援：先保全，故障後由使用者指定新機

## 使用者入口

現在不換機、不租機、不停止現有 GPU 實驗。發生故障時，告訴 penguin 的
agent：「確認救援 `RUN_ID`」，並提供新機的 SSH 主機、port、使用者即可。
agent 當時才產生新的臨時 **SSH 公鑰**；私鑰留在 penguin，不傳給使用者、
新機、Git、Syncthing 或 Discord。這不是 Vast 帳號 API key。

平常查看：

```bash
cd /root/stockAgent
bash scripts/run_training_rescue.sh status
systemctl status stockagent-training-rescue.timer --no-pager
journalctl -u stockagent-training-rescue.service -n 30 --no-pager
```

`recovery_prepared=true` 表示固定程式／環境、完整 checkpoint 和已驗證的冷資料
依賴備妥；**不表示已在新 GPU 續訓**。只有 `resume_progress_verified=true`
表示目的機產生了比救援基線更後面的完整 checkpoint，且已再次回讀保存於 D。

## 第一性原理與責任分工

恢復訓練需要的是「精確的輸入與程式」和「最近提交的全部訓練狀態」，不是
整台機器的檔案樹。服務、下載器、Git 工作區、可重建 cache 不一起搬走。

```text
Vast canonical train.py
  └─ group-last checkpoint + 同 epoch 曲線 + 設定／normalizer／best weights
       └─ 短暫、封閉的傳輸副本 → SHA 驗證 → D 回讀 → commit ACK
                                                 ↓
使用者給新 SSH + 確認 → 舊 worker 停止證明 → 固定 runtime／source
  → strict CUDA → 固定冷資料按需還原 → 遠端重建 prepared views
  → 原 checkpoint 全狀態還原 → Supervisor + 短租約 → 新 checkpoint 回傳 D
```

| 內容 | 保留位置與方式 | 是否可當作完成／刪除證明 |
| --- | --- | --- |
| 原始資料 | 既有 D immutable cold releases；綁定 manifest SHA，不追 `latest` | 救援不新增刪除權限 |
| prepared views、panel／compile cache | 遠端按固定 builder 重建 | 不在 penguin 常駐另一份 |
| 訓練恢復點 | D 的私有 content-addressed objects；同 SHA 共用 | 未完成 checkpoint 不是完成模型 |
| frozen source ZIP／receipt／config | D 私有救援區；保留原始位元組 | 不改目前 Git HEAD、不改 frozen source |
| 固定執行環境 | D 壓縮 runtime capsule，還原到原 prefix | 須符合 OS／arch／glibc 與 runtime lock |
| journal、租約、SSH 私鑰 | penguin SSD，0600／0700 | journal 不替代 D payload 證明 |

本次新增 D payload 約 8.5 GB runtime，加上約 93 MB 正式恢復點和小型程式包。
三份固定冷資料本來就在 D，約 20.97 GB cold objects，**不再複製進救援區**。
這不是整份資料或磁碟 snapshot；短暫傳輸副本只包含 bounded optimizer context，
有 D ACK 後才回收該副本，不刪原 run。保留歷史恢復點，不自動 GC 冷資料。

私有根目錄 `/srv/stockagent-packed/.local-state/training-rescue` 不加入 Syncthing
payload mirror，也不冒充 lake release head。沿用 D primary mount guard；D 不可用
或剩餘空間低於 64 GiB 時拒絕寫入，不回退到 C。此救援點尚不宣稱獨立 NAS 備份。

## 已登錄範圍與尚未驗證的邊界

本次只明確登錄 `tw-v6-embedding-d128`，不是自動涵蓋全部實驗：

- 正式 output：`artifacts/markets/tw_scale_cash_annual_last_fold_selected_20261008_v6/embedding__d128`。
- 原訓練已在 epoch 122 的 Broken pipe 後停止；可恢復的完整 checkpoint 為 epoch 121。
- 固定 world size 2、global batch 256，原 fold 11／epoch 上限和 launcher 參數不變。
- 保存 Adam、model、scheduler、scaler、每個 rank 的 RNG、normalizer、early-stop 計數與曲線。
- 不自動重跑完成的其他 v6 cases，也不停止／接管當前 frontier kernel benchmark。

預先驗證包含真實 canonical CPU checkpoint 冷還原後的 optimizer／RNG 續算，
完整權重與 loss 逐值一致；還有 corruption、確認、SSH key、source／runtime
還原、lease replay、子程序退出與 fencing 等測試。CPU 一致性不能推論跨 GPU
硬體 bitwise 一致。新 Vast 的 CUDA、原 prepared 全資料重建、第一個新 epoch
與 WAN RTO，須等使用者提供新機後實測；現在沒有做假故障或啟動新訓練。

### 本次驗收證據（2026-10-13）

- 救援、checkpoint、training lifecycle 與 runtime 相關測試：**213 passed**。
- D 上 25 個獨立 payload objects 已重新逐位元組回讀核對 SHA；真實 epoch 121
  checkpoint 已經冷還原並以 CPU 檢查完整 optimizer、world size 2／batch 256。
- 最新恢復 context 共 16 檔、93,682,072 bytes；closed runtime 壓縮後
  8,514,862,629 bytes。固定三份來源合計 **9,117 原檔**已解碼核對原檔 SHA。
- [本機驗收 receipt](../artifacts/operations/training-rescue/acceptance-20261013.json)
  明確記錄 `real_new_vast_resume_verified=false`；這份 receipt 不是目的 GPU 的完成證明。

需要重新驗收時，使用新的 output 路徑，避免覆蓋既有 receipt：

```bash
sudo nsenter -t 1 -m -- /bin/bash -c 'cd /root/stockAgent; source scripts/runtime_env.sh; run_fintech_python scripts/verify_training_rescue.py --run tw-v6-embedding-d128 --output /root/stockAgent/artifacts/operations/training-rescue/acceptance-NEW_DATE.json'
```

這會完整回讀 D payload、短暫 CPU 還原 checkpoint，並確認 timer 啟用；不啟動訓練。
若 timer 正在保全，驗收等待同一 owner lock，最久 120 秒後拒絕而不是並行寫入。

舊的 preparation build-source 和 code-training 路徑已退役，因此保全原 preparation
release ZIP 的 builder 與 selected_config，不能拿目前 working tree 代替。
`prepared_reconciliation` 逐欄比較 manifest，僅忽略重建耗時；所有 artifact SHA
必須相同後才恢復原 manifest bytes，另留當次重建 receipt。這不是回填來源時間，
不提升 PIT、live eligibility 或 CUDA acceptance。

目前 captured runtime 和歷史 run metadata 的兩個非數值差異是 `soupsieve`
版本與 StockAgent distribution registration；已記錄於 enrollment receipt。
Python／Torch／NumPy／Arrow／Polars／Triton／FlashAttention／xFormers 版本
不允許默默漂移，目的機須與保全的 runtime lock 一致。

## 故障時的流程與指令

### 1. 發出本次救援公鑰

```bash
bash scripts/run_training_rescue.sh key --hours 24
```

輸出 `key_id`、`public_key`、`authorized_keys_line` 和 expiry UTC。
以 `authorized_keys_line` 加到 **選定新機**的 `authorized_keys` 可立即由 OpenSSH
強制 `restrict,expiry-time`。如果 Vast UI 只接受裸公鑰，首次 bootstrap 會加上
相同限制；若尚未 bootstrap，帳號級裸公鑰不會因本機 key journal 到期就自動
從 Vast 消失，取消救援時要在 Vast UI 移除它。程式不自行修改 Vast 帳號級 keys。

新機需標準 Linux x86_64 Vast Ubuntu/CUDA image、root 或等效管理權限、
Supervisor owner、至少原 world size 的正常 GPU 和足夠儲存空間。
本次 cheap preflight 要求至少 128 GiB 空間；GPU／RAM 還須容納原 batch／模型，
不能因新機較小便默默降低 batch。缺少 userspace C++ build tools 時可在已確認的新機
安裝 `build-essential`，不重裝 host driver。
本次 runtime 是 glibc 2.39、原 prefix `/venv/fintech`；不覆寫新機已有的不同環境。
不安裝／重置 host NVIDIA driver；不支援從 CPU image 默默退回 CPU 訓練。

### 2. 固定目的地主機身分並產生方案

取得新機 ed25519 host-key line，透過可信的控制台／指紋渠道核對。
`ssh-keyscan` 只是取得候選 key，不等於認證。不要使用 `StrictHostKeyChecking=no`。

```bash
bash scripts/run_training_rescue.sh plan \
  --run tw-v6-embedding-d128 \
  --host NEW_HOST --port NEW_PORT --user root \
  --key-id ISSUED_KEY_ID \
  --host-key-file /absolute/private/verified-new-vast-known-hosts
```

方案綁定目的 SSH、host key、key ID、原 spec、D checkpoint／bootstrap、來源身分、
source generation、destination generation。此步不登入／修改新機；有效期一小時。
用戶確認綁定的 `plan_id` 後才執行；不租機、不選 offer、不更動帳單或 Destroy。

### 3. 取得舊 worker 的真正停止證明

目前原 run 是 legacy launcher，不能把失聯或 GPU fault 當作已停止。
若需要停止整個 Vast instance，先取得使用者對該 instance 的確認並由正式管理入口
停止；救援程式本身 **不呼叫 Stop**。下列命令只 GET 舊 instance，必須看到
相同 instance ID 的 `actual_status=stopped`；`intended_status=stopped`、offline、
Stop accepted 和 SSH timeout 都不通過。

```bash
bash scripts/run_training_rescue.sh fence-provider \
  --run tw-v6-embedding-d128 \
  --api-key-file /absolute/private/vast-read-api-key
```

API key 檔案須 0600；也可使用已安全注入程序的 `VAST_API_KEY`，不要將 token 放在
命令列、log 或聊天。這個帳號 API token 與給新機的 SSH 公鑰是不同東西。
輸出私有 receipt 路徑，只保存 allowlisted readback，不保存 provider `extra_env`。
之後若舊 run 已由本救援 guardian 管理，可以只停止該 run：

```bash
bash scripts/run_training_rescue.sh fence-guardian --run tw-v6-embedding-d128
```

它停止續租，只向符合 boot／PID start-time／generation 的本程式 guardian 發訊號，
並驗證所有 owned DDP 子程序已離開；不影響別的 job。無證明就停在等待確認。

### 4. 明確確認後恢復

```bash
bash scripts/run_training_rescue.sh recover \
  --plan-id EXACT_PLAN_ID --confirm EXACT_PLAN_ID \
  --old-node-stopped-receipt /absolute/private/generated-fence.json
```

先驗證確認與新鮮來源停止 receipt，再持久化 generation fence，才接觸新機。
bootstrap 很久時，launch 前再次讀回舊機停止狀態；不是重播初始 Stop ACK。
恢復順序：固定 runtime → 固定 source → runtime lock → strict CUDA
allocation／kernel／synchronize → 只取三份固定來源 → 原 builder 遠端重建
→ 完整 SHA 與原 prepared ABI → 完整 checkpoint／mode → Supervisor launch。

新機仍呼叫原 `train.py --resume`，沒有另一套 trainer／loss／帳本。
已驗證的 closed objects 可重用；半套 transfer／prepared build 不發布。
已開始的 recovery 不能重播 bootstrap 去覆蓋正在前進的 run。

```bash
bash scripts/run_training_rescue.sh cycle
bash scripts/run_training_rescue.sh status
```

`target_restored_guardian_launched` 只證明已 launch，不是續訓驗收。要看到新的完整
checkpoint epoch 超過基線，而且 D 逐檔回讀成功，才標示 `resume_progress_verified`。
成功後撤銷 incident bootstrap 公鑰；run-scoped forced-command control key
只可 lease／status／health／checkpoint read／owned stop，不能執行 shell 或轉送 port。

手動取消／撤銷 incident key：

```bash
bash scripts/run_training_rescue.sh revoke-key --key-id ISSUED_KEY_ID
```

不刪除其他 SSH authorizations，也不因 bootstrap key 到期而中斷正常續訓。

## 平常保全與增加保護範圍

```bash
# 立即保全最近閉合的正式 epoch，不啟動訓練
bash scripts/run_training_rescue.sh capture --run tw-v6-embedding-d128

# 固定 source、config、reference manifests 與 closed runtime capsule
bash scripts/run_training_rescue.sh seal-bootstrap --run tw-v6-embedding-d128

# 固定來源 full decode／SHA，沒有 C 熱還原副本
bash scripts/run_training_rescue.sh verify-sources --run tw-v6-embedding-d128
```

長操作可用既有 `stockagent-agent run` 監督；同一 rescue owner lock 的工作順序執行。
`owner_busy_no_duplicate_operation`／exit 75 表示保留現有作業、稍後重試，不是第二套
並行寫入。長保全期間仍維持已啟動健康目標的 lease，避免 WAN I/O 阻塞續租。

要保護另一個 existing TW factorized value-only 正式 run，先確認 source release 和
runtime capsule 對應該 run，再執行：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/enroll_training_rescue.py \
  --run UNIQUE_RUN_ID \
  --output-root /absolute/artifacts/markets/FORMAL_RUN \
  --config-file /absolute/FORMAL_RUN/generated_config.yaml \
  --source-receipt /absolute/code-release/release.json \
  --runtime-capsule /absolute/private/closed-runtime.tar.gz
```

此 enroller 用已登錄 SSH profile，推導 world size／batch／原 launcher／builder 和固定
三類來源；不啟動訓練。其他產品須先審查 spec，再用 `register --spec /absolute/spec.json`，
不可猜它們的期貨原始資料、單位、因果時鐘或 feature dependency。

runtime capsule 在原機用 **該 run 的** runtime 建立，輸出必須是新路徑：

```bash
# 在已授權的原 Vast shell；adapter 由 register 安裝在 frozen tree 外
source /root/stockAgent/scripts/runtime_env.sh
run_fintech_python -c 'import sys; from pathlib import Path; sys.path.insert(0,"/opt/stockagent-training-rescue"); from worker import freeze_runtime; print(freeze_runtime(Path(sys.prefix),Path("/var/lib/stockagent-training-rescue/runtime-capsules/NEW_ID.tar.gz")))'
```

不執行 `conda update`、`pip install -U` 或把 environment.yml 當作已凍結執行環境。
seal 時再比對 capsule 和現行 prefix 的完整檔案 SHA；不符合時須先釐清漂移。

## 監控、租約、失敗與延遲

- Penguin timer 在作業結束後 30 秒再巡檢；不是保證每 30 秒產生 checkpoint。
- 來源用低侵入 NVML／recovery-action、SSH、canonical lifecycle 狀態監控；NVML
  visible 不是 CUDA compute proof。新機先做 strict CUDA，再以新 checkpoint 驗收。
- 目的 guardian 租約 120 秒，綁定 run／spec／boot／generation、遞增 sequence，
  用 monotonic deadline 防 replay。中斷續租後終止其 owned process group，
  清掉 descendants 才釋放既有 GPU lease locks，不啟動第二個替代 trainer。
- D 掛載失效、controller 長時間不可達或 GPU fault，受管理的目的 run 會 fail closed；
  這是避免雙跑／無保全前進的取捨，不等於永不中斷。
- RPO 是「最後成功提交並 D ACK 的 epoch」，不是故障前每個 optimizer batch。
- RTO 含 runtime／固定來源傳輸、prepared 重建、strict CUDA、編譯及首個新 epoch。
  不搬整盤、不跑 solver，減少可避免工作；但尚未測得新 Vast WAN 的恢復秒數。
- 若 state 是 `awaiting_user_confirmation`，只保存已有證據；不選機、租機、自動
  restart 或 Destroy。半成品和歷史 receipt 留下，不刪唯一冷資料。

工程實作：`stockagent/training_rescue.py`、`stockagent/training_rescue_worker.py`；
測試：`test/test_training_rescue.py`。這是 active checkpoint preservation owner，
不取代 completed-training return／NAS 備份 owner，沒有提供模型部署或遠端清理 ACK。

依據：[Vast instance readback](https://docs.vast.ai/api-reference/instances/show-instance)、
[OpenSSH authorized_keys](https://man.openbsd.org/sshd.8#AUTHORIZED_KEYS_FILE)、
[PyTorch reproducibility boundary](https://docs.pytorch.org/docs/2.11/notes/randomness.html)。
