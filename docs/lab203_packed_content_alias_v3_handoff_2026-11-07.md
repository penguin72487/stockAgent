# lab203：來源封包 v3 讀取契約交接

Penguin 已把 `tw-index-futures` 固定公開 head 封裝為
`stockagent-content-alias-zip-v3`，manifest SHA-256：
`65fde55c914999af9a57589fc149e5215fe853aee660e632fbf5983f0da547dc9`。
此固定版本有 4,601,679 個邏輯檔案，但相同內容可能指向同一 ZIP
member；每條路徑仍各自保有模式，還原後必須是各自獨立可寫的檔案。
新資料亦可能混合共用 member、一般 blob 與 v2 形態的分塊。

本封閉工具包只含程式、測試與本文，沒有資料 payload、憑證或原 owner
設定，不會自動安裝。請在既有 ingress 以原 v7 delivery verifier 核對
envelope、READY 及所有檔案 SHA，再在獨立目錄執行包內合成測試：

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest -q test/test_packed_alias_v3.py
```

請先確認 Python 與 pytest 可用，使用僅供測試的暫存目錄；這些測試不
讀取正式資料、不建立 NAS 備份，不能當作實際資料驗收。舊 v2 reader
應拒絕 v3，不可將多路徑的 member 當成同名檔，或把不同原路徑做成
可互相影響的 hardlink。

後續在使用者恢復備份時，請由原 lab203 owner 依既有容量、鎖、派工、
版本與 NAS 原則整合該 reader，保留錯誤狀態及既有回執；不要由此
工具包自行覆蓋現行 worker、改收件路徑、刪除資料或聲稱 NAS 已驗收。
目前 Penguin 只確認新 D manifest 與物件名稱、長度，沒有額外 D payload
讀回或新的 NAS 驗收；原清冊中相同內容以一份代表來源 SHA 串流核對，
未逐一重讀每個舊重複路徑。正式接收與可恢復狀態需由原流程另行回報。
