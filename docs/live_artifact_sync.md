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

具名冷資料登錄表是 `configs/data_sync/cold_artifacts.json`。只有通過明確 completion
contract 且超過穩定時間的來源可以發布；表內保留需要穩定部署名稱的 market run。
`artifacts/ablations` 則由下述自動維護器建立 path-hash dataset ID；未通過
epoch/summary contract 的舊 run 仍保持 local，不會被誤封存。

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
額外同步與索引。full-replica producer 可以使用 `stockagent-cold-artifact-maintenance`
逐一封裝完成 run；index-only Vast 因本機不保留 packed payload，不得使用該發布路徑，
而必須使用下一節的 durable-node ingress。兩條路徑都只接受 lifecycle-complete、無程序
引用的 immutable run，且每次最多發布一個，避免填滿本機或遠端傳輸佇列。

full-replica 維護器的發布與來源刪除至少跨兩次獨立執行：只有 exact release 再驗證
成功、七日使用租期已過、來源在驗證期間未變、沒有程序引用，而且指定 peer 對整個
packed folder 的 items/bytes/deletes/errors 全為零、completion 100% 且
`remoteState=valid`，才刪除該 run 的本機來源。任何檢查失敗都保留來源；需要時再由
冷庫驗證後 materialize。

安裝與唯讀預覽：

```bash
sudo ./scripts/install_cold_artifact_maintenance.sh

source scripts/runtime_env.sh
run_fintech_python scripts/maintain_cold_artifacts.py --scope ablations
```

預設 peer 名稱是 `penguin`，也可使用 `--peer-name` 或
`COLD_ARTIFACT_PEER_NAME` 明確指定；工具會由目前 Syncthing config 解析 Device ID，不把
某台機器的憑證身分硬編碼進程式。

### Index-only Vast 的完成產物入口

Vast 進入 index-only 後不得直接在缺少 payload 的 packed tree 發布 head。需要跨機
保留的完成產物改由 durable penguin 定時做受限 ingress：只允許明確列入 allowlist 的
相對根目錄；遠端必須是 lifecycle `complete` 且沒有程序引用；`rsync` 只落到 node-local
quarantine。penguin 會再次執行完整 artifact contract、確認傳輸前後的檔案數、bytes 與
最新 mtime 未變，再封裝、逐物件驗證並原子發布到 `stockagent-packed`。Syncthing watcher
只會看到已完成的 immutable release，不會看到半成品。

此處的「即時」邊界是：五分鐘內發現已完成且穩定的 allowlisted run；驗證與原子發布
完成後，Syncthing filesystem watcher 立即開始複製 cold release。傳輸中的 staging、
執行中的 checkpoint 與未完成 run 永遠不直接同步。

在 durable penguin 安裝五分鐘 ingress（私鑰路徑與 SSH endpoint 只寫入本機 `/etc`，
不進 Git）：

```bash
sudo ./scripts/install_remote_cold_artifact_ingress.sh \
  root@114.32.64.6 40032 \
  /root/.ssh/stockagent_vastai1t_ed25519 \
  ablations/tw_day_trade_hybrid_minute_v12_reference_architecture_checkpoint_finetune_ofat_v2/layernorm
```

這不是把 Vast 的整棵 `artifacts` 直接同步。新增另一個必須即時冷藏的完成產物時，先
評估容量與權限，再把其 exact relative root 加入 node-local allowlist；不得把 `ablations`
整個目錄當成萬用 allowlist。Vast endpoint 改變時重跑 installer 即可，既有 packed
release 不受影響。同一 relative root 若在發布後又改變 inventory identity，ingress 會
fail closed，要求 producer 以新 relative root 發布，不能靜默覆寫既有 immutable release。

hot transport 衝突規則：

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
