# C／E／D 封存排程、進度與回收輪替

本輪延伸既有搬移、封裝、發布與回收 owner；沒有另建資料正本或新的清理器。
目標分工與每日增量語義見
[每日去重封存契約](storage_daily_deduplicated_archives_2026-11-10.md)。

## 網頁與排程

[全資料監控的儲存區](https://penguin72487.ddnsgeek.com/data-monitor/#storage-inventory)
每 60 秒讀小型 owner 收據。前景先顯示 C 服務、E 中轉／備份、D 壓縮備份，
以及七個本機階段：首次封存、D 舊包移 E、E 去重封裝、D 新包提交、舊版本回收、
每日合併、C 非服務副本回收。完整來源、排程、鎖與錯誤留在展開區。
網址錨點會直接展開儲存區。

規則投影由 `archive_plan.py` 讀取現行設定，經 gateway 白名單輸出，
不公開本機路徑、來源身分或任意設定字串。每天 03:00–07:00 為日常合併開始時段；
初次遷移 catchup 另計，保留原交易時段優先權與執行器停止規則。
網頁列出的下一個合併時段是可開始時間，不是整庫完成時間。

各階段只使用實測工作量與速度預估。本機總預估排除 NAS 與暫緩的獨立還原，
仍有未知階段時明示「總完成時間待估」。NAS 驗收與傳輸暫存回收繼續獨立列出；
本次遷移不新增獨立解壓還原前置，也不恢復已取消的 Vast 備份驗證服務。

## 解決回收飢餓與重掃

- D→E 每批加入原 `retirement_turn` 排隊。已接納的波次完成後，已預約的回收先取得
  下一輪；仍由原 publication、backup、pin、process 與 journal 檢查授權每次刪除。
  不移除鎖、不強停 active owner。
- 每次回收排程最多選一個普通資料集做大型清冊核對，再讓其他有界回收候選輪轉。
  上一來源的錯誤保留在該來源的 issues，不能誤標下一個正在處理的來源。
- 普通歷史以已驗證的固定清冊作覆蓋錨點，保護該錨點與所有實際 current heads。
  長掃描期間若只有不可變 manifest 新增，最終在原 owner 內重讀新增參照，
  最多 128 份／32 MiB；既有 manifest 更改或消失仍拒絕。新研究 pin 或依賴仍可否決回收。
  journal 記錄 `fixed-validated-anchor-and-all-current-heads-v2`。
- 覆蓋索引只存 path／kind，舊清冊串流比較，保留來源壓縮 SHA、路徑、類型與排序檢查。
  不將整份 SHA／chunk extents 再序列化到臨時 SQLite；原 guard 也在逐段解碼期間執行。
- 搬移器在挑選歷史清冊前先寫 checking 狀態，掃描時最多每 15 秒更新一次心跳。
  尚未開始複製時沒有 byte counter 或完成預估，不再沿用上一輪等待鎖的顯示。
- 目前 E 保存量與歷來 D 配置回收量分開。E 的舊副本之後按原 journal 回收，
  不會讓已釋放的 D 空間從累計數字消失，也不重複計算同一物件。

## 已固定的進度與測量

11/10 21:02 的格式盤點仍是本輪最近一次完整分類結果：新包 1,848 個名稱／
1,646.94 GiB，舊包 60,903 個名稱／773.44 GiB，混合待確認 20,783 個名稱／
445.99 GiB。FinLab 與新聞訓練兩個忙碌來源未納入；名稱長度含 hardlink，
不能相加當作 D 實體占用。盤點的 174 個未定位歷史名稱保留逐項證據，
不當作已恢復或推論為 174 份原始資料遺失。

23:21 的位置索引有 3,422 個已交接 E 的名稱、106,847,877,506 bytes，
累計 D 名稱配置回收 104,028,602,368 bytes。另有 1 個 D unlink pending，
不計入已交接數量。普通歷史回收已實際移除 `legacy-parquet` 的三個中間版本，
回收 D 配置 173,568,000 bytes；這是個別交易，不代表全庫回收完成。

50,000 路徑的合成 metadata ABBA 實測，完整紀錄索引 29,278,208 bytes，
path／kind 索引 9,760,768 bytes，約減少 66.7%。兩次平均建立時間約 7.08 秒與
4.77 秒，CPU 約 1.79 秒與 1.12 秒。這只量到索引建立，不是整個 GC 或整庫搬移速度。
證據：`artifacts/data_quality/archive_coverage_projection_benchmark_20261110.json`。

本輪核心與監控 611 項測試通過，共用前端 47 項 Node 測試通過；各次驗收範圍有重疊。
最後的清冊心跳、當前／累計分離與公開投影另通過 447 項相關測試。
桌面 1,440 px 與手機 390 px 的正式網站讀回、七個本機階段、三個角色卡、
直接錨點展開與無頁面例外，證據在
`artifacts/data_quality/storage_archive_plan_ui_20261110/`。
worker 的原流程、PostgreSQL／Temporal 與 journal 保留，程式載入證據為
`artifacts/data_quality/archive_plan_projection_worker_reload_20261110.json`。

## 未完成範圍

全庫遷移仍在進行。FinLab／FinMind／TEJ 有使用中的原始工作檔，尚未全部接成
封存後自動按需還原；面板明列此差距，不啟用通用 raw 刪除以製造完成率。
Yahoo、Pepperstone、OKX、Binance 的不同來源世代合併、兩個已改變來源根的交接、
舊物件來源歸屬、原續訓缺件與遠端 NAS readiness 各自保留原問題和 owner。
本機搬移、D 回收、研究固定版本完整性與 NAS ACK 分開驗收。

冷庫物件或原始資料仍有唯一內容、研究／服務引用、來源變更或固定缺件時，
繼續保留並公開阻擋原因；不是「沒有研究版本」就可全部刪除。
