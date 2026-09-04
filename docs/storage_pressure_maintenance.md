# 磁碟壓力維護

## 目的與邊界

這個維護器只回收可重建的 GPU 編譯快取，不碰：

- `/srv/stockagent-packed` immutable 冷庫；
- catalog-resolved canonical 資料來源；
- `/srv/stockagent-packed-materialized` 的 lease/READY 管理資料；
- `artifacts`、checkpoint、receipt、Git working tree 或執行中服務輸出。

預設只有檔案系統使用率達 95% 才啟動，並只選擇 14 日內沒有讀寫過、不是
symlink/hard-link、且未被任何程序 fd 或 mmap 引用的 TorchInductor、Triton 與 CUDA
cache。候選依最久未使用排序，最多清到 92%；每個檔案在 unlink 前重新檢查
device/inode/size/atime/mtime，變動中的檔案會保留。這些檔案刪除後最壞情況是日後重新
編譯，不會失去訓練資料或模型成果。

自動排程只要觀察到 `train.py`、`torchrun`、`torch.distributed.run` 或 TorchInductor
compile worker，就會整次延後並在 receipt 記錄 `protected-process-active`。這比只看當下
fd/mmap 更保守，避免訓練稍後重用的編譯物件被清掉。只有人工 `--force` 可以繞過這個
程序級保護；常態排程禁止使用它。

排程使用 `--apply`：若磁碟仍低於高水位，或一開始已觀察到受保護程序，會在掃描龐大
cache tree 前直接留下 `scan_skipped_reason` receipt 並結束。若清理開始後才出現訓練程序，
維護器每 128 個候選重新檢查並停止後續 unlink。人工不加 `--apply` 的 audit 仍會完整走訪，
因此可能需要數十秒。

## 使用方式

```bash
cd /path/to/stockAgent
source scripts/runtime_env.sh

# 唯讀盤點；仍會留下 audit receipt
run_fintech_python scripts/maintain_storage_pressure.py

# 套用相同安全規則
run_fintech_python scripts/maintain_storage_pressure.py --apply

# 安裝 systemd timer；無 systemd 的 Vast container 自動改裝 cron
sudo ./scripts/install_storage_pressure_service.sh
```

Receipt 位於：

```text
/var/lib/stockagent-storage-pressure/receipts/
```

常態參數可透過 `/etc/environment` 設定：

```text
STOCKAGENT_CACHE_MIN_AGE_DAYS=14
STOCKAGENT_STORAGE_HIGH_WATERMARK_PERCENT=95
STOCKAGENT_STORAGE_TARGET_PERCENT=92
```

不要用 `--force` 當排程；它只供人工驗證測試使用。cold store 的 unreferenced object
仍只能先用 `run_packed_snapshot.sh objects` 做 reachability 報告，不能由本維護器刪除。
