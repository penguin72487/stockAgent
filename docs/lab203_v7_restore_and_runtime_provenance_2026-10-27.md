# lab203 固定私有備份與復原證據：2026-10-27

## 已確認的範圍

lab203 於 2026-10-27 21:18 台北時間透過既有回執通道回報：固定 v7 的
五個底本及一個增量，共 1,448 個檔案、23,587,819,110 bytes，從六個
NAS Restic snapshot 還原後，以原離線驗證器核對 exact union、逐檔完整
SHA 及大小。Penguin 以既有 `readback_private_increment_v7_ack.py` 重核
六個原配對 ACK：其 part identity、snapshot ID、檔數及大小均與此回報吻合；
原驗證結果檔的實際 SHA 也與回報相同。這是 **lab203 報告的該固定私有
集合**，不是所有資料版本或資料庫的完整備份，也不是清除其他來源的授權。
原離線驗證結果的 `NAS_backup_verified: false` 表示該驗證器不自行辨認
後端；NAS 來源證據另來自逐批配對 ACK 與接收端的完整聯集回報。

兩個期貨 run 的已還原 checkpoint 分別在 CPU 以原 canonical loader
讀取成功，並通過原 runtime lock；台股 run 的單獨 loader 診斷可讀取
checkpoint，但 runtime lock 不符，嚴格 CPU 驗收仍是失敗。這三個 run
都沒有新 GPU 執行、舊 worker fence 或新 epoch 的驗收。

## 台股舊版準備資料參照

以 v7 固定計畫中原 spec CAS 的原始 bytes/SHA、canonical spec identity，
以及原 bootstrap identity 讀回比對後，台股舊版的兩個 reconciliation
參照雖未在參照本身填 `reference_sha256`，原 spec 的 `bootstrap_files`
已各自固定角色、原始路徑、目標路徑、長度與 SHA，且原 bootstrap 的
檔案清冊與 spec 完全相同；兩個物件也都在固定 v7 計畫內。這能識別
當時登錄的原參照，不表示已在新機重新組裝或比較完整訓練資料。
Penguin 另以受保護的檔案描述元完整讀取固定 D 的這兩個物件，核對
5,705,214 及 3,418,386 bytes 的長度、原 SHA 與前後 inode generation；
這次本機有限讀回不是新的 NAS 還原驗收。

已在既有 `scripts/verify_training_rescue.py` 加入
`legacy_original_bootstrap_reference_binding_v1`，只對原 spec 自身、
驗證過的 bootstrap、唯一目標路徑、相同原始路徑、指定 prepared role
及 exact CAS bytes/SHA 都一致的舊參照回報綁定。缺 bootstrap、版本不符、
同目標有歧義或欄位錯誤時仍留待驗；已有明確 `reference_sha256` 的
路徑維持原驗證。此改動不修改 spec、checkpoint、factorized 產生器或
接收端既有證據，也不單憑這份證明啟動訓練。離線驗證、增量備份和
bootstrap 相關測試 **118 項通過**；接收端尚未收到新封閉程式包，因此
目前不能宣稱 lab203 已採用新檢查或完成新的聯集驗收。

## 尚未解決的 runtime 差異

lab203 從固定 runtime capsule 重新解碼並驗證成員 SHA 後，Python 與
Torch 版本符合，但 `charset-normalizer`、`click`、`cuda-pathfinder`、
`typing-extensions`、`urllib3` 的套件版本及 `stockagent` distribution
metadata 與原 lock 不同。原始 source wheel 已在該備份中；這仍不足以
證明其餘依賴的原始安裝材料完整。不可藉過濾 metadata、選別版本、
改寫原 lock／spec 或採用另一 run 的環境來製造通過結果。下一步須
追查原始執行環境與各套件的來源、固定可還原的安裝材料，再沿原
enrollment／版本化補充與既有私有加密路徑完成獨立驗證。

對應接收端私有回執與 Penguin 六批 ACK readback 保留在既有私有作業目錄；
本報告不含金鑰、checkpoint payload 或 runtime 內容。讀寫或刪除原始資料
仍需各自的固定冷恢復、使用者／程序及 retirement owner 關卡。
