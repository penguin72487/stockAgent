# 即時 artifacts 同步（penguin 為衝突權威）

這個模式不建立使用者可見的資料快照。新的穩態 transport 使用
`/srv/stockagent-artifacts-hot`；`stockagent-hot-artifact-sync.service` 會將
penguin 的 hot 檔案送入 transport，並把新收到、penguin 尚未擁有的路徑立即
放進 `/root/stockAgent/artifacts`。舊 `/srv/stockagent-artifacts-live` 只在
lab203 遷移完成前保留。

## 第一性分層：hot 路徑與 cold 小檔

Syncthing 的索引成本由「路徑數」決定，內容 hard link 去重只能節省磁碟，
不能縮小已建立的 Syncthing index。因此新的穩態架構是：

```text
可變檔案與大檔
artifacts ──hard link──> /srv/stockagent-artifacts-hot
                              │ Folder ID: stockagent-artifacts-hot
                              ▼
                         peer artifacts

已完成 run 的小檔
artifacts ──SHA/ZIP packs──> /srv/stockagent-packed
                              │ manifest + object verify
                              ▼
                  直接驗證寫入 peer 的 artifacts 原路徑
                  然後才加入該節點的 .stignore-cold-local
```

既有 `stockagent-artifacts-live` 已經索引過的路徑不會因為事後加入 ignore
而從歷史 index 消失，所以採平行 Folder ID 遷移，不能原地假裝已縮小。
在 lab203 的新 folder 完成度達 100% 前，舊 folder 不退役。

冷資料登錄表是 `configs/data_sync/cold_artifacts.json`。只有通過明確 completion
contract 且超過穩定時間的來源可以發布；目前首批只包含通過完整 training
lifecycle gate 的 `feature_input_lookback32_v4`。另外兩個標示 complete 的舊 run
因 epoch/summary contract 不完整而保持 hot，不會被誤封存。

penguin 發布與啟用：

```bash
./scripts/manage_cold_artifacts.py status
./scripts/manage_cold_artifacts.py publish \
  artifact-tw-public-feature-input-v4-small --node-id penguin
./scripts/manage_cold_artifacts.py activate \
  artifact-tw-public-feature-input-v4-small --conflict-policy fail
sudo ./scripts/install_hot_artifact_sync_service.sh
```

`activate` 先驗證所有 packed objects，再直接安裝或核對原本的 `artifacts` 路徑；
不留下使用者可見的 snapshot tree。每個節點的 cold ignore 都是本機生成，不能
直接複製別台機器的「已完成」狀態。

lab203 必須先讓舊 folder 完成，再依序執行：

```bash
cd /root/stockAgent

./scripts/manage_cold_artifacts.py \
  --live-sync-root /root/stockAgent/artifacts \
  activate artifact-tw-public-feature-input-v4-small \
  --conflict-policy packed-wins

install -m 0644 \
  deploy/syncthing/stockagent-artifacts-live.stignore \
  /root/stockAgent/artifacts/.stignore

./scripts/manage_cold_artifacts.py \
  --live-sync-root /root/stockAgent/artifacts \
  rebuild-ignore
```

記錄輸出的 `conflicts_detected` 與 `replaced`；`packed-wins` 會用已驗證的 penguin
release 覆寫這些 cold 衝突。完成後再暫停並移除舊 `stockagent-artifacts-live` 的
Syncthing 設定（不刪實體目錄），接受：

```text
Folder ID:   stockagent-artifacts-hot
Folder Path: /root/stockAgent/artifacts
Folder Type: Send & Receive
Ignore file: /root/stockAgent/artifacts/.stignore
```

新 folder 必須達到 `idle`、`needBytes=0`、`needItems=0`、`remoteState=valid`
且保持 QUIC/TLS 1.3，才可在 penguin 移除舊 Folder ID。

衝突規則：

- penguin 已有同一路徑時，永遠保留 penguin 內容，並回寫傳輸目錄。
- penguin 沒有的路徑才接收 peer 內容。
- 不傳播刪除；避免任一 peer 誤刪後擴散。
- `data_locks`、`*.lock`、`*.pid`、Syncthing 暫存與 conflict copy 不同步。
- 同檔案系統使用 hard link，所以工作樹與傳輸樹不重複占用資料空間。

## 穩定檔案內容去重

`scripts/deduplicate_artifacts.py` 的正式排程只會處理超過 24 小時未變動，且
最近一層 lifecycle `progress.json` 明確為 `state=complete / phase=complete` 的
普通檔案；running、failed、沒有 lifecycle owner 的舊資料，以及 `live/`、
capture、repair、log、lock 等可變資料都不會自動處理。候選先依大小分組，再以
SHA-256 驗證完整內容；套用前會重新驗證，最後以原子 hard link 取代重複 inode。
所有既有相對路徑都保留，因此 artifact contract 與讀取程式不需改動。hard link
只能節省實體內容空間，不能減少 Syncthing 的路徑索引筆數。

安裝每日低優先權維護：

```bash
sudo ./scripts/install_artifact_dedup_service.sh
```

稽核 receipt 寫入：

```text
/var/lib/stockagent-artifact-dedup/receipts/
```

penguin 的 Syncthing folder：

```text
Folder ID:   stockagent-artifacts-live
Folder Path: /srv/stockagent-artifacts-live
Folder Type: Send & Receive
Watch delay: 1 second
Ignore deletes: enabled
Versioning:  disabled
```

peer 接受相同 Folder ID，但 Folder Path 使用該機器的
`/root/stockAgent/artifacts`。peer 必須套用
`deploy/syncthing/stockagent-artifacts-live.stignore`，並設為 Send & Receive。

安裝本機橋接服務：

```bash
sudo ./scripts/install_live_artifact_sync_service.sh
systemctl status stockagent-live-artifact-sync.service --no-pager
cat /var/lib/stockagent-live-artifact-sync/status.json
```

第一次建立 folder 會索引既有 artifacts；之後由 filesystem watcher 觸發，
通常在 Syncthing 收件完成後約一秒內原位出現。Syncthing 的傳輸完成仍應以
`needBytes=0`、`needTotalItems=0`、`errors=0` 與實際 peer connection 驗證。
