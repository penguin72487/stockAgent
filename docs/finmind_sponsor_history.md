# FinMind Sponsor 全市場歷史回補（2026-09-26）

帳號權限以官方 `GET /v2/user_info` 的實際回報為準。本機 2026-09-26 重新驗證 `.env` 的 `FINMIND_TOKEN`：`Sponsor`、每小時 `6000` 次；此前同一環境曾顯示 `Free`、`600` 次，故不以「已付款」或 `.env` 有值推定可用權限。金鑰、電子郵件與原始資料均不送入公開頁；`data_finmind/account_status.json` 只含層級、額度、用量與取樣時間。每分鐘的帳號取樣由獨立 timer 執行，工作站本地滾動 60 分鐘流量另由 `request_traffic.sqlite3` 量測，二者不是同一個數字。

下載器 `downloader.download_finmind_sponsor` 依[官方完整資料集契約](https://finmind.github.io/llms-full.txt)建立 59 種可用 `data_id` 空值取全市場的日期分區。先以必要的全市場日／月／財報資料回補，再取低優先級來源；Free 補充下載器對同一 FinMind dataset 的逐檔任務，在 Sponsor 狀態新鮮且無權限阻擋時暫時讓渡，不刪除舊任務或既有收據。Sponsor 狀態失效超過 15 分鐘時 Free 任務回復候選。原本 Free 的全球匯率、海外市場、快照等非重疊任務繼續運行。

實測 `TaiwanStockPrice` 不帶 `data_id` 取 2026-09-23 單日回傳 47,827 列；2026-09-22 至 2026-09-23 的回傳只含 9/22，證實 `end_date` 為排他上界。因此收據保存半開區間 `[start_date, end_date)`，回應日期落在區間外會拒絕入庫。大表用單日／兩日，小表用月／年，避免把幾十年的 JSON 一次放進記憶體。最多四個並行網路工作共用與 Free 工作相同的主機限流鍵，間隔由官方帳號實際額度計算；402／429 延後全部工作，權限／參數 4xx 不自動重試以免 IP 被封。磁碟剩餘量低於 25 GiB 或開盤保護時段不派新工作。SQLite 佇列、內容雜湊 Parquet 與原子收據允許中斷後續傳。

這 59 種**是已排入下載器，不是宣稱已抓完整**。目前佇列目標、各來源非空／空回／失敗／受阻、首末日期、列數與容量見 `data_finmind/sponsor/status.json`、`/finmind/` 及總資料面板。空回只是查驗過，不算有數值；一個 200 回應也不證明供應商沒有缺列。每筆原始資料的正式發布時刻、歷史 PIT、訓練可用性和與 TWSE／TAIFEX／FinLab 等其他來源的欄位口徑都尚需獨立稽核。`configs/data_sync/packed_datasets.json` 仍是 `publish: false`，不會自動進冷庫或模型訓練。

以下類別目前**沒有**可在 Sponsor 層級高效率抓取全市場歷史的已驗證查詢形狀，已列入狀態檔 `unscheduled` 而非假裝完成：個股 Tick、個股分鐘 K、券商分點、期貨／選擇權全產品 Tick／分鐘 K、可轉債／資產交換逐債資料、海外分鐘線和即時快照。官方文件把對應全日 Parquet `storage_objects` 明確標為 `SponsorPro`，Sponsor 不可假定有權使用。前述 tick 也遵照先前要求維持最低優先級；若要全市場逐筆回補，需另確認 SponsorPro 資格、磁碟容量與保留期。[FinMind 官方資料契約](https://finmind.github.io/llms-full.txt)、[IP 封鎖政策](https://finmind.github.io/en/BanIPPolicy/)。新聞依使用者要求維持停用。

本機不消耗額度的交叉檢查 `scripts/audit_finmind_sponsor_overlap.py` 每日 18:10 比對 FinMind 原始股價與 TWSE／TPEx 官方股價，只比較相同日期／代碼的未還原收盤價和「股」單位成交量；將差異及無法對齊的界線記入 `artifacts/data_quality/finmind_sponsor_source_audit/latest.json`，**不會自動選值或覆蓋來源**。更多跨來源比對要逐資料集補上相同粒度與口徑的契約。

操作：

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.snapshot_finmind_quota
run_fintech_python -m downloader.download_finmind_sponsor --max-requests 4 --workers 2
sudo bash scripts/install_registered_data_refresh_services.sh finmind-only
systemctl status stockagent-finmind-sponsor.service stockagent-finmind-quota-snapshot.timer
```

排程分工仍以來源和任務粒度為界：官方 TWSE／TPEx／TAIFEX 與 Shioaji 各自保持原有權威資料、盤中捕獲和服務；FinLab 以其原始鍵庫保留獨立收據；FinMind 的 Sponsor 批量結果先作補洞候選，不能直接覆蓋官方值。跨來源校驗只在主資料可用後對齊交易日、標的、價格調整、成交量單位及發布時點；不一致報告差異，不能自動投票選值。保留重疊來源用於驗證，不代表常態重複向同一端點抓取。
