# D 冷庫新舊封裝與 E 分批周轉

後續實作：本文件保留 11:33–11:45 的清點數字。16 時起已依使用者新指示新增
「常用封存留 E、冷資料留 D」的精確實體位置分流，並修復期貨舊 E 編碼回收；
新功能與當前證據見 [本輪實作](storage_warm_archive_placement_2026-11-10.md)。
下文「尚未實作」與當時容量是該清點時間的觀察，不代表後續功能仍未部署。

觀測：2026-11-10 11:33–11:45（台北）。這是本機封裝／檔名存在性清點，
不是 NAS 驗收或全檔內容讀回；本次沒有搬移或刪除資料負載。

## 最新清點

掃描 public／private 的 1,303 個資料集命名空間、2,555 份固定清冊，
再獨立查驗 101,011 個被引用的封裝名稱。只讀取清冊和 filesystem metadata，
沒有解壓、讀取或重新雜湊封裝內容。清冊掃描約 453 秒；檔名查驗約 189 秒。

| 格式 | D 仍存在的封裝名稱 | 封裝檔案長度 |
| --- | ---: | ---: |
| 已確認新的 dataset/source 格式 | 2,125 | 1,929.93 GiB |
| 舊格式 | 78,258 | 928.90 GiB |
| 增量／混合格式，尚未直接確認 | 20,525 | 162.75 GiB |

以上依 authority 與相對檔名計量；hardlink 別名不能當成額外實體資料。
這批存在的封裝 hardlink 去重後共 98,598 個 inode，實際配置 2,690.51 GiB。
不含未引用物件、inventory、控制檔、其他 D 目錄及 NTFS 自身占用。
D 全磁碟約使用 3.55 TiB、可用 86.61 GiB，不能直接與封裝名稱長度相減推算可刪空間。

就現行 head 的格式而言，1,167 個為新格式、114 個為舊格式、22 個待確認。
這是 head 數，不是完成下載／備份的百分比；同名的 public／private authority 分別計數。

另有 103 個舊引用名稱已不存在，共 606,522,554 bytes。當中 67 個仍被 head 引用的
名稱，已核對所屬四個資料集均在原測試資料廢棄清單；不可因此重建已明確捨棄的測試負載。
其餘歷史引用保留在逐項報表，不能把歷史 metadata 的存在當成負載仍在。

原網頁 D catalog 的來源觀測仍是 11/6；它的「舊 catalog 已追上」僅表示當時的
未引用物件波次完成，不能代替這份 11/10 全部封裝格式清點。

## 分批順序與原執行者

使用者指定 C 留服務、E 為工作區、D 留整理後冷包。不能一次把全部 D 搬入 E：
11:45 時 E 工作卷 available 為 768,404,086,784 bytes（715.63 GiB），
與 Windows E 實體磁碟尚餘 1.38 TiB 是不同量測；周轉須同時通過兩者。

1. 已有完整 E 準備的固定資料集，先由原安裝器處理，不再複製一次相同舊包。
2. 已有新 D commit 的舊名稱，由原退休器檢查引用、pin、程序、hardlink 和 journal 後回收。
3. 其餘資料集逐批在 E 整理；E 只保存本批所需資料及未完成交易，完成後返還周轉空間。
4. D 放不下時，原 E-first 容量 owner 只可移除已有完整 E 固定世代保障、且通過原引用與
   容量計算的精確舊 D 名稱；同一資料集返還 D 的 writer 具有優先權。
5. 舊的 public／private head 仍有研究／資料集依賴時，不以「格式舊」為由刪除。
   FinLab／FinMind／TEJ 沿目前另有視窗維護的原搬移程序，不新增並行 writer。

這是現有 owner 已支援的 E-first 路徑。**任意仍被引用的舊 D 封裝先整批撤空、
再於日後才建立新 E 封裝，尚未實作成可恢復的通用交易。**本次沒有以移走檔案、
臨時 symlink、改 pin 或放寬來源檢查假裝完成該步驟。

| 原排程 | 已安裝狀態 | 接續方式 |
| --- | --- | --- |
| `stockagent-d-cold-e-handoff.timer` | active | 未引用的精確舊物件波次，完成後 20 秒再檢查 |
| `stockagent-prepared-stream-install.timer` | active | 已封閉 E 世代安裝，完成後約 1 分鐘再檢查 |
| `stockagent-dedup-candidate.timer` | active | 有界內容去重候選，完成後約 1 分鐘再檢查 |
| `stockagent-source-local-repack.timer` | active | 同資料集舊來源串流整理至 E，再交原 D 安裝器 |
| `stockagent-source-local-retirement.timer` | active | 精確換代及歷史舊名稱回收 |

今日受原當沖優先時段 08:00–13:30 保護，13:30 是最早可嘗試大量 I/O 的時間，
不是已承諾的開始或完成時間。timers 與每個原 owner 仍會重查容量、鎖和來源。

## 當前最大待處理項

`local-tw-index-futures` 固定原 SHA
`74477024a4fcdc60c1a11f5a37afc9faa405155402f0217a6c0eae864b2b1835`：

- 舊 D 封裝 188,865,896,622 bytes（175.90 GiB）。
- 已封閉 E 準備 4,684,161 個原始檔案、476,509,512,310 logical bytes。
- 一般 E 新包為 421,113,684,735 bytes（392.19 GiB），原本 D 所需餘量另加 32 GiB。
- 所以即使立刻騰出該資料集全部舊 D 長度，也不能直接放入這份膨脹新包。
  原安裝器已具有轉交內容去重候選的支線；仍須等實際內容盤點、來源依賴與容量檢查。
- 原固定版本另被 `local-tw-futures` 依賴，不能只依封裝年齡移除。

沒有實際去重候選大小與吞吐率前，不提供虛構的全庫完成時間。

## 可重跑的報表

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/report_storage_migration_inventory.py \
  --layout-only --output artifacts/data_quality/d_pack_layout_20261110
run_fintech_python scripts/report_storage_migration_inventory.py \
  --presence-only --output artifacts/data_quality/d_pack_layout_20261110
```

- [格式摘要](../artifacts/data_quality/d_pack_layout_20261110/layout_summary.json)
- [存在性與 hardlink 配置摘要](../artifacts/data_quality/d_pack_layout_20261110/layout_presence_summary.json)
- [逐資料集與固定版本](../artifacts/data_quality/d_pack_layout_20261110/layout_manifests.csv)
- [逐封裝格式與資料集關聯](../artifacts/data_quality/d_pack_layout_20261110/layout_objects.csv)
- [逐封裝存在性](../artifacts/data_quality/d_pack_layout_20261110/layout_presence_objects.csv)

新增的唯讀 collector 有界平行讀取 metadata，保留原 dataset 鎖與固定 head SHA 檢查；
存在性查驗對每個物件目錄只做一次祖先解析，避免每個小 blob 都重複查詢 DrvFS 祖先。
測試涵蓋新舊／私有分類、busy 與變動 head、不讀負載、hardlink、缺檔、變長及路徑轉向。
與既有報表測試共 **10 passed**。原搬移、來源與 NAS 驗收狀態未被改寫。
