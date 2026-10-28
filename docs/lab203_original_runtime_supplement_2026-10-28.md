# TW 原始 prepared reference 與 runtime 補件：2026-10-28

這份補件回應既有反向同步通道的兩份固定請求。原 spec、bootstrap、
checkpoint、runtime lock、runtime capsule 及兩份 prepared reference 都不修改；
兩個已驗證的期貨 run 也不重傳。私有補件沿原加密 Restic 備份與配對回執
通道交付；`tools/storage-relay-20261028-v1/` 僅含可檢查的程式與文件。

## 固定綁定

| 原 step / role | SHA-256 | 原始 bytes |
| --- | --- | ---: |
| 0 / `parent_prepared_reference` | `e7be0c72fe6394ecf01140d4587c7c412de370e502f51fa05bb97cd2b4d746f3` | 5,705,214 |
| 1 / `value_prepared_reference` | `33bed11d08665f5578b6ef11f3689ee80c99b22b260f661683d3e35eb21d9f50` | 3,418,386 |

新版原離線驗證器的 `legacy_original_bootstrap_reference_binding_v1` 僅接受
未改動的原 spec，以及另外驗證過的原 bootstrap。角色、來源路徑、唯一
目標路徑、完整 SHA 與長度都必須一致；不根據檔名、moving latest 或另一個
run 猜測。這是原始參照的識別證據，重新產生 prepared view 後的完整比較
與續訓仍各自驗收。

## Runtime 來源與診斷合約

`stockagent.original_runtime_restore_qualification.v1` 私有合約固定原始
spec、bootstrap、runtime proof、source receipt 與 wheel 的原始檔案 SHA／
長度，並固定原 runtime lock。原 prefix 是 `/venv/fintech`，來源程式根目錄
沿用原 enrollment。接收端使用已還原的固定 v7 聯集，不另下載新套件。

六項 metadata 差異有兩種可觀察的原因：

- 原 capsule 同時含有五組有爭議套件的新舊 `.dist-info`；原觀察器先記錄
  全部 distribution，再使用 `importlib.metadata.version()` 的實際查找結果。
  完整檔案 SHA 相同不能保證重建後的目錄列舉順序相同。保留全部版本；
  不挑選某版本或過濾 metadata 來湊出原 lock。
- 原 prefix 沒有 StockAgent distribution metadata，原 source wheel 則含有
  真實的 `stockagent-0.1.0.dist-info/METADATA`。診斷將完整 SHA 驗證過的
  原 wheel 加入 metadata 查找路徑；這不安裝套件，也不取代原 frozen source。

`scripts/diagnose_training_rescue_runtime.py` 會先完整重讀原 runtime proof
的每個檔案與連結，核對額外檔案、原 source release、兩個參照與所有固定
控制檔，再以原 Python、原 frozen observer 觀察全部 distribution 及順序。
最後仍使用未改動的原 lock 嚴格比較，並留下完整實際觀測。它不載入
checkpoint、Torch、GPU 或訓練，也不宣稱完成 resume。

接收端先驗證新 tools 包及私有補件的配對封閉 envelope，依原 Restic
恢復流程讀回固定補件。新版 evidence-only plan 的
`stockagent_private_rescue_evidence_supplement_plan_v1` 只列 evidence JSON，
不包含 runtime、checkpoint 或 prepared payload，且固定連回已驗證的 v7
plan。它不能作為完整訓練救援集合交給舊的 whole-plan 驗證器通過。

在原完整 v7 的離線驗證中沿用原本五個底本與一個增量的 roots；不要把
evidence-only root 併進原完整清冊。新的 runtime 診斷則明列原物件所在的
各個 disjoint restored roots，並使用補件中的固定合約路徑與 identity：

```bash
python3 -I -B /verified/tools/storage-relay-20261028-v1/scripts/diagnose_training_rescue_runtime.py \
  --contract /verified/private-supplement/evidence/SESSION/CONTRACT.json \
  --contract-id EXACT_CONTRACT_ID \
  --restored-part /verified/original-base-part-0 \
  --restored-part /verified/original-base-part-1 \
  --restored-part /verified/original-base-part-2 \
  --restored-part /verified/original-base-part-3 \
  --restored-part /verified/original-base-part-4 \
  --restored-part /verified/original-v7-increment \
  --output /private-evidence/tw-original-runtime-readback.json
```

實際 roots 是各批還原後的 `private-rescue/<該計畫 identity>/` 目錄；必須
逐一明列，不能使用萬用字元或自動選 latest。上面的路徑是格式範例，
`EXACT_CONTRACT_ID` 與相對路徑以封閉私有補件的 response index 為準。

原 lock 相符時結束碼為 0；仍有差異時，寫出
`original_runtime_strict_lock_pending` 與所有差異，結束碼為 **75**。
原始 bytes、來源、連結或合約錯誤會直接失敗。禁止將 75 改為成功，或
把這項診斷通過當成 optimizer/GPU 續訓驗收。原失敗證據繼續保留；
五項重複 metadata 的查找順序仍須由接收端實測，不預先宣稱修復。

## 歷史物件與新聞

私有 evidence 另附 25 個 tw-public 版本／114 個物件的本次搜尋範圍、
逐版本原來源修復規劃與未恢復物件清單。僅接受完整原 SHA 的物件或
能逐成員還原原始 bytes 的來源；目前資料重抓後的新版本不能冒充舊版本。

`data_news` 與 `data_news_fulltext` 的存在性、固定 manifest identity、
part 清冊及既有驗證時間另有索引。原新聞的 `publish`、
`remote_training_export` 與 training injection 設定維持原值；補件不包含
新聞原文或 live SQLite。新聞 part 可跨 generation 共用，未來若有允許的
交付範圍，必須依固定 manifest 清冊解析，不能只複製最新版本的目錄。

檔案送達、NAS 完整還原、原 runtime 嚴格相符與真正續訓是四項不同結果。
實際發送 identity、檔數、bytes 與當時驗收狀態由本次 publication receipt
記錄；未收到配對讀回之前不稱接收端已採用。
