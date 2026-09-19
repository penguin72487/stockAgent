# lab203 同步退役紀錄（2026-09-16）

## 決策與邊界

penguin 是目前冷資料權威。依使用者指示，lab203 自此不再是 penguin 或
vastai1T 的 Syncthing peer，不接收它後續的 release 或 hot artifacts。退役只變更
Syncthing 配對／分享設定，沒有刪除 penguin C 槽、D 槽、舊 hot transport 或
lab203 自己磁碟上的任何資料。不能把「解除配對」解讀成所有歷史版本都已完整備份。

## 退役前資料證據

- lab203 唯一現行 cold head：`stockagent-migration-core`，release
  `stockagent-migration-core-20260812T030055076520750Z-l0-lab203-a4d130cbc3aacb1b`。
- C／D 的 head 內容與 manifest SHA-256 一致：
  `d2c97c13d2b9e04a30a2f2ac119f815f6a7d5939bd83fb3e795c20346f9cce56`。
- penguin C 槽對此 release 的完整 `verify_packed_snapshot` 通過：1,181 個
  pack/blob，合計 136,687,037,830 bytes。這證明當時的本機冷庫可重建該 release。
- D 槽所有參照物件都存在，head／manifest 相符。原本使用 8 MiB 讀取區塊的
  完整驗證在讀取約 91 GB 後因 WSL `ENOMEM` 中斷，**不能宣稱 D 已全量雜湊通過**。
  64 KiB 區塊的低記憶體重驗也遇到相同 `ENOMEM`，未觀察到 checksum mismatch。
  需另排查 WSL／D 槽讀取問題或由 Windows 端完成讀回；不能把中斷當作通過。
- D 整體備份另有歷史 backlog。2026-09-17 00:02 TST 觀測為 `degraded`，
  約 345.5 GB 待核對、41 個 head 和 108 個 release 待處理；錯誤包含舊
  `tw-public` manifest 參照的 C 冷庫缺失 pack。這些錯誤不是 lab203 配對變更
  所造成，且不能由上述單一 release 推論整個冷庫已備齊。

## 實際操作與驗收

- lab203 Device ID：`TLOI2HH-6EZOSBC-H6YMGVW-APQOGFW-P2A7FZT-EKX6IUP-2HMRKS6-LBDHOQM`。
- penguin 與 vastai1T 先暫停該 Device，接著從各自 `devices`、
  `stockagent-packed` 共享清單移除，加入 `remoteIgnoredDevices`。兩邊均保留
  penguin／vastai1T 的共享與歷史 lab203 cold head。
- `stockagent-artifacts-hot` 原本只供 penguin／lab203 使用，已從 penguin
  Syncthing 設定移除；`stockagent-hot-artifact-sync.service` 已停止並停用。
  舊 `/srv/stockagent-artifacts-hot` 磁碟樹保留待獨有檔案稽核。
- 兩邊 Syncthing 都回報 `restart-required=false`；退役後 lab203 不在任何共享
  folder、沒有 lab203 連線，且被列為 ignored device。
- 退役後 penguin ↔ vastai1T 的 `stockagent-packed` 雙向觀測均為 connected、
  QUIC、completion 100%、need bytes/items/deletes 皆 0、`remoteState=valid`；
  兩邊 folder 為 `idle`，errors/pullErrors 為 0，watchError 空白。
- penguin 的 `configs/data_sync/packed_retention.json` 現僅要求現役
  `vastai1T` 收斂；這不繞過 D checksum receipt、head、pin、程序引用或
  其他 cold-retention 保護門檻。本次沒有執行 cold-retention apply。

## 尚待完成／不可推論

- D 全量讀回與整體 backlog 要分別追蹤；在完成前不得稱 D 是完整災難備份。
- 舊 hot transport 有少量與 repository `artifacts` 不共 inode 的路徑；未逐檔
  核對冷庫前不得刪除整棵 transport。
- 這次控制的是 penguin 與 vastai1T 的配對。沒有登入 lab203 本機修改它的
  UI 設定；即使它保留舊邀請，也無法與已移除並忽略它的上述兩台機器同步。
- 兩台剩餘裝置的 `introducer` 與 `autoAcceptFolders` 都是 false；目前沒有
  自動重新引入 lab203 的 Syncthing 設定。未檢查 lab203 與其他第三方的配對。
- 其他歷史 release 的完整性不能由本次 lab203 head 驗證外推。
