# 訓練資料生命週期

Vast 保存本期工作集，penguin 保存不可重新取得的源本與必要結果。先辨認實際
job／config／source release／output root，沿用 `docs/agents/training.md`、
`docs/agents/data.md`，調整硬體效能另讀 `docs/agents/performance.md`。

## 取用與在遠端重建

1. 解析 `configs/data_sync/packed_datasets.json` 和實際發布 manifest，固定 dataset／
   snapshot ID、來源 schema／PIT／cutoff。不能以 `data_*` 名稱或 moving latest
   替代版本，不能把有 source originals 的 `features` 根當可丟衍生物。
2. Vast 藉 index-only cold store 取本次 source；下列命令會寫入本機工作集並續租，
   只在準備本期 job 時執行。這個共同參數範例適用 canonical CLI／edge wrapper：

   ```bash
   source scripts/runtime_env.sh
   stockagent-data use DATASET --snapshot-id EXACT_RELEASE_ID --ttl-days 7
   ```

   缺 CLI 用 `bash scripts/run_data_cache.sh`；在 Vast 先確認 edge enrollment。
   penguin 的 `stockagent-packed-transport.timer` 是唯一 payload owner；私有
   rclone policy 登錄後，Syncthing 只搬 index，payload 走 bounded SFTP staging、
   全 SHA／current demand fence／no-overwrite promotion。不要另跑 full mirror，
   不手改 edge state／ignore 或清當期 leases 來加速。無需求時只做 no-op 確認。
   僅採用返回的已驗證 materialized path 與 READY 身分。不要把 penguin 專用的
   `--verify`／`--path-only` 當成所有 edge 都支援；以本機 `use --help` 核對參數。
3. 交付固定 Git／code receipt、config、source fingerprint、ABI／runtime lock，
   由原 canonical collector／builder 在 Vast 建立 panel、labels、split、tensor、
   execution tape 和 compiler cache；不要在 penguin 重建整份訓練 view 再同步。
   model／ABI／source 改變要換 matching fingerprint 與相容 output root，拒絕不相容 resume。
4. 使用中的 materialization 由原 lease／pin／consumer gate 保護；續租不表示
   原始資料歷史完整，READY 不表示模型完成。服務真正需要的 bounded projection
   與部署模型仍由 penguin 保留，不能為 source-only 理念破壞網站／下單依賴。

## 遠端硬體實測

先核對當前 CPU／SMT／socket／NUMA、affinity、可見祖先 cgroup quota、RAM
max-minus-current 與 MemAvailable、mount／容量／pressure、GPU owner。兩個
socket 可以列為候選；host 核心／RAM 數不等於租用實例可用額度。

TW public 完整來源建表的現有入口為
`scripts/benchmark_tw_public_remote_derivation.py` → correctness／重測 finalists →
`scripts/verify_tw_public_remote_derivation.py --tuning-receipt ...`。
精確 argv 與 oracle 條件讀 `docs/remote_build_workflow.md`，用該機當前 RAM budget
和完整來源；不要複製文件裡某次 64 GiB 或 53 threads 的值。

其他 preparation／GPU training 使用各自 canonical benchmark，測端到端 wall time、
CPU seconds、peak RSS、I/O、parity 和必要輸出。epoch／fold 必須包含設定要求的
validation、test、曲線繪圖與 checkpoint；不清全機 cache、不占其他 job 的 GPU。
expensive GPU job 前執行：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
```

用 Miniforge／Mamba 管理角色環境、平台各自 explicit locks；不覆寫正在用的
CUDA 環境或默默 CPU fallback。selection receipt 的 boot／hardware／source／
code／runtime 身分過時或 headroom 不足，沿原工具重新量測。

## 完成產物與工作集回收

penguin 的 `stockagent-remote-cold-artifact-ingress.timer` 按
`configs/data_sync/training_return.json` 選取 lifecycle-complete、穩定且有界的
精確 run；既有私有 SSH／rsync ingress 返回結果、canonical D 封存與獨立原值
重建、新鮮 ACK、再次檢查目前程序／consumer／source／同步後才回收其遠端來源。

不把 `checkpoint.pt`、optimizer 停止、同步到 penguin 暫存目錄或 service exit 0
當完整成果。未完成 run／unique trial results／來源不明 cache 保留。回傳模型
不自動 promotion，penguin 不永久保留當期 tensor／panel／compiler cache。

資料 materialization 的七日 lease、compiler cache 的壓力／十四日 allowlist、
completed output 精確退休、舊歷史一次性保存是不同交易。釋放工作集請先做
`stockagent-data gc --dry-run`，再遵循 [回收判準](recovery-and-retirement.md)；
不要因成果已回傳就直接遞迴刪 source、所有 `artifacts/cache` 或有效 checkpoint。
