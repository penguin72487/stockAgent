# 指定模型 Syncthing 隔離接收驗收（2026-09-10）

## 1. 執行進度與邊界

使用者同意新增「僅同步指定模型封裝、接收後先進隔離區」的通道。
已完成 vastai1T → penguin 的實際 Syncthing 傳輸、完整隔離驗收、penguin
canonical cold publication，以及本機 artifact materialization。
**尚未切換正式 selector、重啟交易服務、改寫帳本或回補歷史。**

本次使用 strategy-switch skill 的隔離／持倉保護，以及 training-reuse skill
的既有 lifecycle 與 checkpoint 驗證，不另建訓練器。這些驗收不代表新訓練
已與 paper 跨日持倉契約一致；舊 checkpoint 的訓練語義沒有被改寫。

指定模型：

```text
artifacts/markets/tw_day_trade_hybrid_minute_v12_attention_full_then_last_layernorm_commission20_capital10m_all_features_v1
```

## 2. 傳輸與發布證據

| 證據 | 實測結果 |
| --- | --- |
| 原始產物 | 493 files、22 directories、0 symlinks |
| 原始邏輯大小 | 1,248,531,869 bytes |
| 封裝 | canonical helper 產生 32 buckets 的小檔 packs，加大檔 blobs；合計 54 objects |
| object 傳輸量 | 882,270,992 bytes；加 envelope 後共 55 files、882,409,422 bytes |
| 雙端同步 | 10:14:34–10:14:36 +08:00：idle、needBytes/items/deletes = 0、completion = 100、remoteState = valid |
| 錯誤 | 雙端 folder/pull/system errors 皆空／零，watchError 空 |
| 實際連線 | penguin quic-client、vastai1T quic-server；TLS1.3-TLS_AES_128_GCM_SHA256 |
| lifecycle | training-lifecycle-v1；fold 1–11 及對應 train groups 完整 |
| 本機安裝 | 493 added、0 replaced、0 conflicts；再次完整 hash 驗證通過 |

Package SHA-256：

```text
635db77571919ee396e56713a67b1c8a273f645099f8edd873c323457944568c
```

fold 11 checkpoint SHA-256：

```text
12a46eb8d1ceac2a54a3bcfa15250f6ca6eb63d39ffbacb8977772b767a1d012
```

由 penguin 發布的 exact cold release：

```text
artifact-auto-tw-day-trade-hybrid-minute-20260910T021522179736047Z-l0-penguin-693b08aca38c44bf
```

Manifest SHA-256：

```text
1d0443c7420e84ca3236892eb96e5dd2958178cef91397aee81cb31a1cb6b7a3
```

冷庫 54 個 objects（882,270,992 bytes）、inventory SHA、manifest SHA、隔離
重建與本機安裝後的逐檔內容皆重新驗證。這不是整個 canonical cold fleet 的
全量同步／歷史完整性驗收，不宣稱其他資料集或 lab203 已完全同步。

所有 operational receipts 位於 `/var/lib/stockagent-artifact-ingress/`：

- `accepted-<package>.json`：雙端 transport acceptance 及 quarantine。
- `published-<package>.json`：penguin head、manifest、inventory、objects 驗證。
- `materialized-<package>.json`：本機安裝、零覆寫及 lifecycle。
- `checkpoint-forward.json`：本機 checkpoint 載入／前向 smoke。
- `promotion-gate.json`：正式切換被持倉 gate 拒絕的證據。

隔離副本、原始 vastai1T 產物及本機安裝均保留；沒有自動刪除任何來源。

## 3. 首次實測發現與修正

`NamedTemporaryFile` 預設 0600；vast 的 Syncthing 使用不同本機使用者，導致
envelope 可以同步，但 54 個 objects 出現 permission denied。接收端一度顯示
100%／idle，僅表示「已公布到 index 的一份清單」同步完成，並不表示模型齊全。

修正為：在 object 原子完成前設定 0644；重跑時僅對已驗證相同 SHA 的 transport
object 修復讀取權限。包裹 identity 不因這項傳輸權限修復而改變，source 本身
不動。外層接收命令同時檢查兩端掃描／拉取錯誤；內層仍必須通過 pinned envelope、
每個 object、每個檔案與 lifecycle，不能以 Syncthing 的百分比取代完整性。

憑證處理：早期遠端程序診斷曾把 command-line GUI API key 帶入工具輸出；後續
工具改為程序內部解析並使用，絕不輸出原始參數／憑證。本文不保存該值，亦未
自行重啟遠端 supervisor 或輪替憑證。建議另行輪替這把 localhost GUI API key。

## 4. 實作與防護

- `configs/data_sync/artifact_ingress.json`：唯一允許 root、checkpoint SHA、2 GB／
  2,000 entries 上限、來源與接收者。擴大 scope 需要新的明確授權。
- `stockagent/data_sync/artifact_transport.py`：重用 canonical `_write_pack`、
  blob hash、portable inventory、safe extraction；不是新的 cold publisher。
- `scripts/configure_artifact_ingress_syncthing.py`：僅新增指定 folder；source
  sendonly、receiver receiveonly、watcher on、僅兩個 exact device identities。
- `scripts/transfer_selected_artifact.py`：SSH 僅傳 repository code／控制參數／
  小型驗收結果；不開啟、複製或經 SSH 傳輸模型物件。artifact bytes 全走 Syncthing。
- cold publication／activation 仍使用既有 `cold_artifacts.py`，新增 registry row；
  vast 不發布 canonical head、不改 index-only ignore／node ID，也不加入 artifact-hot。
- no-op 包裹不改 identity；來源 active／變動／未完成、缺檔、毀損、symlink、
  路徑穿越、重複 entry、非法 mode、超額物件、scope 不符均拒絕；不覆寫已存在隔離資料。

REST 操作依 [Syncthing config API](https://docs.syncthing.net/rest/config.html)，
只 POST 指定 folder，不 PUT 整份 config。驗收欄位參照
[folder status](https://docs.syncthing.net/rest/db-status-get.html) 與
[peer completion](https://docs.syncthing.net/rest/db-completion-get.html)。

## 5. 可重跑指令

在 penguin repository 執行；環境檔只提供既有 SSH control target／identity path，
不要輸出其內容。GUI API key 由各端在程序內讀取，不放入這些指令。

```bash
set -a
source /etc/stockagent/remote-cold-artifact-ingress.env
set +a
source scripts/runtime_env.sh

run_fintech_python scripts/transfer_selected_artifact.py configure
run_fintech_python scripts/transfer_selected_artifact.py build
run_fintech_python scripts/transfer_selected_artifact.py status
run_fintech_python scripts/transfer_selected_artifact.py receive \
  --package-id 635db77571919ee396e56713a67b1c8a273f645099f8edd873c323457944568c
```

`receive` 在未收齊時拒絕；不降級、不猜 latest，也不自動發布或切換。只有 source
byte-for-byte 相同時，上述 pinned package 才仍適用。新的 checkpoint 必須重新
授權／登記 digest。這是一項明確執行的接收工具，不是已安裝的自動發布排程。

## 6. 測試與部署阻擋

66 項 transport／Syncthing scope／既有 cold artifact／maintenance／remote ingress／
packed snapshot 測試通過（2.30 秒）；Ruff 通過。未跑完整 repository 測試。
最後加入既有價格格點、共用當沖契約及 strategy-switch 回歸，合計 109 項通過
（2.79 秒），Ruff 與 `git diff --check` 亦通過。10:18:34–10:18:37 +08:00
再次查核此 ingress 的兩端仍 idle、55 files、need = 0、零錯誤、peer valid。

本機 RTX 5070 Ti，嚴格 checkpoint model contract + state_dict 載入：
missing/unexpected/adapted keys 均空。BF16、lookback 32、2,746 symbols、99 features
的單批合成零輸入前向輸出 `[1, 2746]` 且全部有限；載入加第一次 forward 3.151 秒。
這不是行情推論、穩態 latency benchmark、完整 epoch 或策略績效驗證。

接收後查核正式 ledger 的 `updated_at` 仍是 `2026-09-10T09:01:00+08:00`：
100m 有 82 個非零持倉、多基底 8、多基底22 2、待替換 v12 2；共 94。
canonical `_flat_live_state` 明確拒絕切換四個仍有持倉的 modes。
不能把這項 ledger 讀取稱為服務當下健康／持倉即時同步的證明。

正式 selector 仍是原本的 OFAT `layernorm`，沒有切換成新模型。既有 selector SHA：
`96f050689d05482c0c6c95c0a47dddcbac714bb58db7b75b096430149f389383`。
未強平、未清空帳本、未改歷史／minute curves、未重啟服務。

原產物 fold 11 測試期間仍為 `2026-01-02`–`2026-08-19`（151 rows），不冒充已更新
到 9/10。完整新版 training/paper 跨日持倉契約、逐筆對帳及新訓練指令仍待實作；
詳見 [訓練執行一致性紀錄](DAY_TRADE_TRAINING_EXECUTION_PARITY_2026-09-10.md)。
