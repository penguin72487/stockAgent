# 磁碟壓力維護

## 目的與邊界

這個維護器只回收可重建的 GPU 編譯快取，不碰：

- `/srv/stockagent-packed` immutable 冷庫；
- catalog-resolved canonical 資料來源；
- `/srv/stockagent-packed-materialized` 的 lease/READY 管理資料；
- `artifacts`、checkpoint、receipt、Git working tree 或執行中服務輸出。

CLI／常態 wrapper 預設只有檔案系統使用率達 89% 才啟動，並只選擇 14 日內沒有讀寫過、不是
symlink/hard-link、且未被任何程序 fd 或 mmap 引用的 TorchInductor、Triton 與 CUDA
cache。候選依最久未使用排序，最多清到 88%；每個檔案在 unlink 前重新檢查
device/inode/size/atime/mtime/ctime/mode/nlink，並重查 regular file／單一 link；變動中的檔案會保留。這些檔案刪除後最壞情況是日後重新
編譯，不會失去訓練資料或模型成果。

自動排程只要觀察到 `train.py`、`torchrun`、`torch.distributed.run` 或 TorchInductor
compile worker，就會整次延後並在 receipt 記錄 `protected-process-active`。這比只看當下
fd/mmap 更保守，避免訓練稍後重用的編譯物件被清掉。只有人工 `--force` 可以繞過這個
程序級保護；常態排程禁止使用它。

排程使用 `--apply`：若磁碟仍低於高水位，或一開始已觀察到受保護程序，會在掃描龐大
cache tree 前直接留下 `scan_skipped_reason` receipt 並結束。若清理開始後才出現訓練程序，
維護器每 128 個候選重新檢查程序與 fd/mmap，並停止後續 unlink。人工不加 `--apply` 的 audit 仍會完整走訪，
因此可能需要數十秒。

## 使用方式

```bash
cd /path/to/stockAgent
source scripts/runtime_env.sh

# 唯讀盤點；仍會留下 audit receipt
run_fintech_python scripts/maintain_storage_pressure.py

# 套用相同安全規則
run_fintech_python scripts/maintain_storage_pressure.py --apply

# 明確手動處理 /tmp/torchinductor_root；不是清全部 /tmp
# 先 audit，確認 receipt，才 apply；不得配 --force 或縮短保留期
run_fintech_python scripts/maintain_storage_pressure.py --tmp-torchinductor-only
run_fintech_python scripts/maintain_storage_pressure.py --tmp-torchinductor-only --apply

# 安裝 systemd timer；無 systemd 的 Vast container 自動改裝 cron
sudo ./scripts/install_storage_pressure_service.sh
```

`--tmp-torchinductor-only` 僅 root 可用且不能與 `--cache-root` 並用；目錄須由 root
持有、非 symlink／redirect、與 `/tmp` 同一檔案系統且不是 mount point。這個 scope
本身不會自動加入既有 timer／cron，預設仍只檢查 cache home 下的明確 allowlist。
PyTorch 的預設 TorchInductor 磁碟快取可能在系統暫存目錄，而非 `~/.cache`；
刪除後下一次需要時重新編譯，不是重新生成訓練原始資料。
參見 [PyTorch 編譯快取文件](https://docs.pytorch.org/tutorials/recipes/torch_compile_caching_configuration_tutorial.html)。

2026-10-04 後續明確授權自動清理後，Vast 可用 root-owned 本機
`/etc/stockagent/storage-pressure.json` 選擇固定 `compiler-home-and-root-tmp`
profile；原 wrapper 自動讀入，仍使用原 lock／hourly cron。手動選項與這個
明確註冊是兩回事；profile 不允許 force、自訂根、程序覆寫或短於 14 日。
所有 scope 在任何 unlink 前先驗證，拒絕不明 schema／redirect／mount／重複 key／NaN。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/maintain_storage_pressure.py \
  --policy "$PWD/configs/data_sync/vastai_storage_pressure.json" --check-policy
run_fintech_python scripts/maintain_storage_pressure.py \
  --policy "$PWD/configs/data_sync/vastai_storage_pressure.json"
run_fintech_python scripts/maintain_storage_pressure.py \
  --policy "$PWD/configs/data_sync/vastai_storage_pressure.json" --enroll-policy
```

註冊只寫固定本機設定與 receipt，不掃描／刪除 cache，也不建立另一個排程；
不同既有 policy 不覆寫。完整日誌、安裝／停用與自助命令見
[自動冷儲存與安全回收](automatic_cold_storage.md)。

Receipt 位於：

```text
/var/lib/stockagent-storage-pressure/receipts/
```

常態參數可透過 `/etc/environment` 設定：

```text
STOCKAGENT_CACHE_MIN_AGE_DAYS=14
STOCKAGENT_STORAGE_HIGH_WATERMARK_PERCENT=89
STOCKAGENT_STORAGE_TARGET_PERCENT=88
```

不要用 `--force` 當排程；它只供人工驗證測試使用。cold store 的 unreferenced object
仍只能先用 `run_packed_snapshot.sh objects` 做 reachability 報告，不能由本維護器刪除。
