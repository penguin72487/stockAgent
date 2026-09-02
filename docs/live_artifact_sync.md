# 即時 artifacts 同步（penguin 為衝突權威）

這個模式不建立使用者可見的資料快照。新的穩態 transport 使用
`/srv/stockagent-artifacts-hot`；`stockagent-hot-artifact-sync.service` 會將
penguin 的 hot 檔案送入 transport，並把新收到、penguin 尚未擁有的路徑立即
放進 `/root/stockAgent/artifacts`。舊 `/srv/stockagent-artifacts-live` 與
`stockagent-artifacts-live` Folder ID 已退役並刪除，不得重新建立。

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

舊 folder 已完成平行遷移。現在只有 `stockagent-artifacts-hot` 承擔低延遲
operational artifacts；已完成且可驗證的 cold artifacts 走 `stockagent-packed`。

冷資料登錄表是 `configs/data_sync/cold_artifacts.json`。只有通過明確 completion
contract 且超過穩定時間的來源可以發布；目前首批只包含通過完整 training
lifecycle gate 的 `feature_input_lookback32_v4`。另外兩個標示 complete 的舊 run
因 epoch/summary contract 不完整而保持 hot，不會被誤封存。

登錄項目的 `maximum_file_bytes` 設為整數時，只發布不超過該大小的穩定檔案；設為
`null` 時發布通過 lifecycle gate 的完整 run。完整 run 中小於
`loose_file_threshold_bytes` 的檔案進固定 hash buckets，較大的 checkpoints、回測與權重
檔案成為 content-addressed blobs，不得因大小而漏傳部署必要檔案。

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

lab203 第一次加入新架構時依序執行：

```bash
cd /root/stockAgent

./scripts/manage_cold_artifacts.py \
  --live-sync-root /root/stockAgent/artifacts \
  activate artifact-tw-public-feature-input-v4-small \
  --conflict-policy packed-wins

install -m 0644 \
  deploy/syncthing/stockagent-artifacts-hot.stignore \
  /root/stockAgent/artifacts/.stignore

./scripts/manage_cold_artifacts.py \
  --live-sync-root /root/stockAgent/artifacts \
  rebuild-ignore
```

記錄輸出的 `conflicts_detected` 與 `replaced`；`packed-wins` 會用已驗證的 penguin
release 覆寫這些 cold 衝突。完成後接受唯一的 hot folder：

```text
Folder ID:   stockagent-artifacts-hot
Folder Path: /root/stockAgent/artifacts
Folder Type: Send & Receive
Ignore file: /root/stockAgent/artifacts/.stignore
```

新 folder 必須達到 `idle`、`needBytes=0`、`needItems=0`、`remoteState=valid`
且保持 QUIC/TLS 1.3，才算完成。

`vastai1T` 不加入 `stockagent-artifacts-hot`。它的 `artifacts` 主要是大型訓練與
ablation 工作集，直接加入會把 penguin、lab203 與 Vast 的完整輸出做聯集，造成數百 GB
額外同步與索引。Vast 的執行中產物保持 node-local；完成且通過 lifecycle gate 的 run
選擇性封裝到 `stockagent-packed`，由各節點驗證後 materialize。這是角色分工，不是
漏配 folder。

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
Folder ID:   stockagent-artifacts-hot
Folder Path: /srv/stockagent-artifacts-hot
Folder Type: Send & Receive
Watch delay: 1 second
Ignore deletes: enabled
Versioning:  disabled
```

peer 接受相同 Folder ID，但 Folder Path 使用該機器的
`/root/stockAgent/artifacts`。peer 必須套用 hot artifact ignore 規則，並設為
Send & Receive。

安裝本機橋接服務：

```bash
sudo ./scripts/install_hot_artifact_sync_service.sh
systemctl status stockagent-hot-artifact-sync.service --no-pager
cat /var/lib/stockagent-hot-artifact-sync/status.json
```

第一次建立 folder 會索引既有 artifacts；之後由 filesystem watcher 觸發，
通常在 Syncthing 收件完成後約一秒內原位出現。Syncthing 的傳輸完成仍應以
`needBytes=0`、`needTotalItems=0`、`errors=0` 與實際 peer connection 驗證。
