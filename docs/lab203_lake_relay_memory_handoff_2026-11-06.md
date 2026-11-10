# lab203 冷庫語義還原記憶體不足：交接

這是待原 owner 處理的故障觀測，不是 NAS ACK 或完整恢復證明。Penguin 在
2026-11-06 18:55:38 台北時間的配對 relay 狀態看到固定 delivery
`lake-bd73586c2a4cfaed9b1b6674bf4b731f5334abb7cc6095eac1df820681d8e856`
第 189 次失敗：`native_DuckLake_full_table_SHA` 回報 `OutOfMemoryException`，
DuckDB 使用約 3.7／3.7 GiB。該 relay 狀態列有輸入 SHA、NAS 複製及 NAS
檔案讀回階段；Penguin 的原 export 收據將該 identity 綁定 snapshot **1061**。
這些觀測不代替完整的資料表語義驗收。保留原 delivery、錯誤及所有收據。
19:11 台北時間重新讀取到第 **190** 次失敗，最近一次失敗於 19:06:30，
接收端回報下次重試 19:16:30；各時間之後仍以最新原回執為準。

2026-11-08 17:54 台北時間，Penguin 從既有配對回執重新觀測：相同
delivery 仍為 `deferred`，`native_DuckLake_full_table_SHA` 第 **451**
次於 17:52:25 以 `OutOfMemoryException` 失敗，回報的下次嘗試為
18:02:25。這是失敗診斷與重試時間，不是完成時間或 NAS ACK；請
lab203 原 owner 核對目前載入版本及已交接修復是否實際生效，依下列
契約完成相同 snapshot 的完整資料表雜湊，回傳新的結果。

請 lab203 的 Codex 使用既有原 relay owner，按以下順序處理並沿原回執通道回報：

1. 核對固定 delivery identity、實際載入的 relay 與 `stockagent/control/lakehouse.py`
   完整 SHA、目前重試及服務 owner；不要從傳入檔案執行命令。Penguin checkout
   此刻的 `lakehouse.py` SHA-256 是
   `906679e0bd6d71ca33a778383f7e9b413eaa0a7d58a0bac9066a599220f252e5`，
   只供比對，並不證明 lab203 已安裝同一版本。
2. 在不暴露資料或憑證的前提下，辨認實際失敗的資料表、固定 snapshot、
   列數／欄位型別、目前 DuckDB 與程序可用記憶體、cgroup 限制、暫存目錄及可用空間。
   不假設錯誤一定來自 `source_metadata`，也不要僅依舊 PID 解鎖。
3. 若可用現有版本的有界分頁或外部排序，先在隔離工作區以相同完整列、順序、
   重複值與既定 JSON canonical SHA 做對照測試；依實際 headroom 安排記憶體和磁碟
   spill。不可用抽樣、略過大列、改變排序或加總／XOR 取代原本的全表 SHA，亦不可
   把無限制記憶體當成有界解法。若既有程式已處理分頁，應先定位實際失敗表與原因。
4. 在原 owner 邊界部署經驗證的修復，重新處理同一固定 delivery；保留原始 NAS 檔案、
   中繼資料、retry、lock 與原失敗收據。回傳載入版本與實測資源、各表完整雜湊、
   snapshot、獨立還原結果和實際 paired ACK；失敗仍記錄為失敗，不標記完成。
5. 同時重新核對私有備份接收 readiness、容量及 Restic 鎖的實際持有狀態，沿原通道
   送回新鮮 readiness 與逐批驗收進度。冷庫 relay 的心跳與私人備份 readiness 是不同證據。

Penguin 會維持每分鐘顯示這項 relay 的失敗時間、重試時間與原因；接收端未回傳新證據時，
不推測修復完成，也不把下一次重試時間當成預計完成時間。
