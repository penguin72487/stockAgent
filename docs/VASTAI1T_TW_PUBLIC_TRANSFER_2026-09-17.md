# vastai1T tw-public 傳輸診斷（2026-09-17）

## 可重測的現場證據

- `tw-public-20260917T022610857716059Z-l0-penguin-7e9a08bb7548fd88`
  相對上一版僅有約 1.554 GB 新封裝物件，但 vastai1T 的 index-only edge 在上一版
  materialize 後清掉約 16.7 GB packed payload；因此新一輪需再次收約 17 GB，
  並非 Syncthing 把來源資料目錄當作增量直接傳。
- Syncthing 連線為直連 QUIC，沒有設定 send/receive 限速；開始時四條連線，
  15 秒樣本 2.61 MB/s，後續 20 秒樣本 3.33 MB/s，接收端 `pullErrors=0`。
- 用 SSH 只送 64 MiB 測路徑、遠端丟棄（未用 SSH 同步資料），花 119.63 秒，
  約 0.56 MiB/s。當時 Linux TCP 顯示 `cwnd=5`、約 55.8 MB 發送中
  1.38 MB 重傳。50 次 ping 對 Vast 位址有 3 次逾時（6%）；對本機路由器
  50 次則 0% 遺失。這支持「本機網段以外的路徑／Vast 主機端」有壅塞或遺失，
  但樣本不足以定位到哪一家 ISP 或哪一跳。
- 500/500 Mbps 是各自接取線路規格，不等於兩端之間必有 500 Mbps。若全程可達
  500 Mbps，17 GB 的純傳輸下界約 4.5 分鐘；這裡的實測不符合該假設。

## 修正與驗收邊界

1. `manage_packed_edge.py use --retain-payload` 為指定資料集保留經 SHA-256 驗證的
   exact-release packed payload，最多七日。下次同資料集發布時，讓 Syncthing
   只接收新封裝物件；新版物化並完成 READY 後才清理舊版專有物件。
2. 中斷後的同一 `dataset/snapshot_id` hydration 可以在 `syncing` 狀態接續，
   但只放行需要 bytes/items 尚未歸零的暫態；peer connected、remoteState valid、
   無 pull/system/folder errors 等條件仍需通過。
3. 曾把遠端 penguin 裝置的目標連線數由 4 暫調至 8，但觀察期內實際仍只有
   4 條連線，吞吐波動也不能歸因於該設定；已回復原本的 4。官方文件指出增加
   連線可能改善吞吐，但不是保證，這次沒有保留未驗證的調參。
4. 不能因為資料已送達就稱可訓練。仍需 exact cold object 驗證、READY、
   `--check-data-only`、雙卡 DDP 完整 fold 生命週期與等價性驗收。

此操作未建立新的同步資料夾、未用 SSH 搬運資料集，也未將 vastai1T 改成 cold publisher。
