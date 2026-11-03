# Vast 封閉壓縮包暫存與預驗證

2026-11-01。Vast 利用率低的主要原因是工作尚未送出：原流程先完成整份
來源清冊與封存，之後才分配 pack 驗證。`local-tw-index-futures` 本輪有
4,684,151 個來源檔；提高遠端程序數不能縮短這段等待。

現在在原發布程序安裝好不可變 CAS pack 後，就可以交給 Vast 暫存、解壓與
計算原值承諾。完整清冊提交後，原驗證器再綁定這些計算。沒有新增發布、備份
或刪除權威，現行來源與原失敗收據全部保留。

## 分工與資料流

| 位置 | 工作 | 保留內容 |
| --- | --- | --- |
| Penguin／D | 原封存、來源世代檢查、固定清冊、最終驗收；單一路徑順序讀取 D | 原本冷庫正本及小型收據 |
| Vast | 接收已封閉壓縮包、全 encoded SHA、每個 stored member 的 CRC、原值 SHA／長度 | SHA 定址壓縮包及私有計算收據 |
| lab203／NAS | 沿既有 owner 接收、獨立還原及配對 ACK | 原有備份；此次不變更接收條件 |

流程：`D 安裝封閉 pack → 私人 SSH 上傳 → Vast 全內容預驗證 → 回傳小型收據
→ 原固定清冊提交 → 原驗證器重新核對 D／Vast 完整 encoded SHA 與清冊承諾
→ 原完整還原／備份／回收 gates`。

Vast 回傳的是計算結果。Penguin 已有相同壓縮包，將它再傳回不會增加還原
證據，只會多用傳輸及 WSL 空間。此次沒有建立解壓出的 raw 資料樹。

## 正確性與失敗處理

- 預驗證結果明確是 `inventory_bound=false`；還沒有證明任何資料集完整。
- 正式重用要匹配固定清冊的有序原檔 SHA／長度承諾、pack 全 SHA、所有 CRC、
  worker/helper 程式 SHA、遠端 boot 及新的來源世代檢查。清冊只選部分 member
  時，仍驗證所有 stored CRC，並核對該精確子集合。
- Native D reader 必須完整 EOF、原子世代、mount 與子程序成功；釋放 reader 後
  才接納遠端回覆。釋放後可以傳下一包，與上一包的解壓重疊。
- cache 不存在、遠端中斷或證據不匹配時，原正式驗證保留失敗並走既有本機
  exact fallback。預驗證佇列逐物件記錄失敗，其餘物件繼續。
- 近期 mtime 只用於便宜的候選搜尋，不是發布時間、授權或驗收。舊物件未進入
  預驗證候選也不會被算作完成；原完整清冊仍負責所有物件。
- Vast cache 在 `/root/stockAgent/service_data/pack_compute_cache`，0700 根目錄、
  0600 檔案、逐物件鎖、不跟隨連結、32 GiB 實體容量保留，預設七天後才可回收
  自己的已封閉 cache pair。未知或忙碌物件保留，D／NAS／原始來源不動。
  容器磁碟不是持久備份；快取遺失只影響速度。

## 排程與監控

私有設定 `/etc/stockagent/pack-compute-spool.json` 明確啟用這個範圍；可攜範本
在 `configs/data_sync/pack_compute_spool.example.json`，預設關閉。原正式驗證設定
`/etc/stockagent/pack-verification.json` 的 `remote_spool=true` 是另一個必要條件。

```bash
bash scripts/install_pack_compute_spool.sh
systemctl status stockagent-pack-compute-spool.timer
```

2026-11-02 更新：timer 在工作清空或受阻後每 60 秒再檢查。只要本輪確實
完成新工作且仍有候選，就立即接續下一個最多 32 物件的回合，避免非空佇列
每回合空等一分鐘；每回合重新核對私有登錄。同一個 queue lock
防止手動工作與排程重複。D 順序 reader 為一個，Vast 工作者為
兩個，讓傳輸與解壓重疊。這是工作分派數，不是 CPU／GPU／磁碟限速。

舊私有 checkpoint `service_data/pack_compute_spool/state.json` 保留；新增範圍
各用 `scope-{context_sha256}.json`。現在明確分開正式與私有 D root，兩者各自
綁定 dataset、開始時間、遠端 boot、worker、來源世代與 cache SHA。
observer 只讀取
獨立小型 `status.json`，不解析完整逐物件紀錄、不讀 payload、不呼叫 SSH。
全資料面板每分鐘顯示「Penguin → Vast · 封閉壓縮包預驗證」、候選 bytes／packs、
已完成量及當前階段 ETA。來源還在新增 pack，候選分母可能增加；這個 ETA
不代表整份來源、NAS 備份或 WSL 清理的完成時間。網站排程清冊已加入新 timer，
總計 29 個排程／常駐 owner 項目。

原常駐 worker 正在封存數百萬檔案，因此另外使用原 rollout 的
`--after-publication-dataset local-tw-index-futures`：等固定 snapshot／manifest
可從 D 精確解析，再載入新版正式驗證器。等待時程式身分或原 operation 改變
就拒絕重啟。這個邊界僅代表來源提交，不是完整還原驗收；原 journal 和持久
workflow chain 保留。Vast 的預驗證已先執行，不需等待此交接。

以上是首輪交接的固定觀測；2026-11-02 已進入正式來源 `tw-index-futures`
封存，新的 ZIP 緩衝修正排在該來源提交邊界載入。雙範圍工作、實測及目前
限制見 [來源封存 I/O 與雙範圍驗證](storage_publication_io_and_vast_scopes_2026-11-02.md)。

## 實測與驗收範圍

- 首輪 8 包、7,139,941,524 bytes 已由 Vast 預驗證，失敗 0。
  18:20 台北時間的後續觀測為 27／172 包、21,822,184,821 bytes；這是當時
  近期封閉物件的快照，最新數字看面板。
- 原先逐個 WSL/P9 metadata 搜尋超過一分鐘；改為 NTFS 單次 metadata 列舉、
  對選定物件再做 canonical lstat，本輪 discovery 為 4.96 秒。沒有省略全內容 SHA。
- 約 1.1 GiB 的包，native 讀取及上傳約 47 秒，傳完後回覆約 3.6 秒。
  流水線需要約 `ceil((47+3.6)/47)=2` 個遠端工作槽。當前限制以 D／傳輸為主，
  不是遠端缺 CPU；不為提高 GPU 顯示利用率而新增不必要工作。
- 同一固定 Crypto ETF release（1,794 原件／126 packs）完成 warmup 及
  1／4／4／1 遠端工作槽的完整原值還原。四輪全 proof 相同；1 槽均值
  50.42 秒、4 槽均值 56.04 秒，背景封存與傳輸仍在執行，cache hits 不同。
  這是四槽沒有穩定獲益的負面證據，不能宣稱全流程加速倍數，也不能外推
  到全部大型資料集。兩槽的選擇來自上述流水線量測，尚無兩槽的全歷史速度驗收。
- 定向 pack／cache／reader／monitor 回歸 269 passed；來源遷移與恢復回歸
  360 passed、8 skipped；來源提交交接 4 passed。各自保留完整 run 收據，
  不把重疊測試數累加成不同案例數。
- 公開 HTTPS 網站的 Chromium 桌面／手機 27 項通過，包含兩種寬度的真實 Vast
  進度卡；fixture 異常案例另有標示，沒有 provider API 呼叫。
- 最終固定驗收快照：兩槽 systemd 自動工作已接手，完成 36／177 包、
  約 27.09 GiB，失敗 0；正式 worker 的固定來源提交交接仍在等待。之後數字
  持續變動，以網站與原收據為準。
- 此次沒有刪除原始資料、D 或 NAS 物件，也沒有啟動 GPU 訓練。七個受保護
  原服務的 PID／啟動身分於 observer 部署前後均保持一致。

實測原件與固定驗收收據：

- `artifacts/storage_layout/vast-spool-fixed-replay-v2-20261101/acceptance.json`
- `artifacts/storage_layout/vast-pack-staging-qualification-20261101.json`
- `artifacts/storage_layout/vast-spool-publication-boundary-rollout-20261101-waiting.json`

尚存限制：全來源清冊仍在封存；大型 pack 的正式本機 adapter 在無足夠 RAM 時
仍可能需要既有容量閘門下的暫存。預驗證沒有移除所有 D 讀取與網路成本；
後續應按真實大包全流程量測調整工作分派，不能只看 CPU 百分比。
