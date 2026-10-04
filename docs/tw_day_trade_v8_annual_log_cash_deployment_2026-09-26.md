# V8 Annual Log Cash fold 10 當沖候選部署（2026-09-26）

## 已驗證的產物與路徑

- vastai1T 原始完整 run：`artifacts/markets/tw_day_trade_v8_combined_annual_log_cash_sub_lot_first_session_all_folds_v5`，10 個 fold 均有完成的訓練生命週期；選用 `fold_10/checkpoint_best.pt`，本機重新計算 SHA-256 為 `25d52616598046c7983adaf1285adc0f4efc6662c2c2e6e037e22536f213e5a2`。
- 模型檔只經授權的 Syncthing ingress 隔離傳輸，package ID 為 `7a44aeb10dcb8b2342f1e8cde1d8311d729154043e02b5dbd213f4093dc09318`。接收端通過完整來源檔雜湊與訓練生命週期驗證；沒有透過 SSH/rsync 複製模型檔。
- penguin D 主冷庫固定 release：`artifact-tw-day-trade-v8-combined-annual-20260926T035701385846195Z-l0-penguin-86d740018476d823`。420 檔、2,679,630,822 邏輯 bytes；63 個冷物件與隔離原件完成 SHA 驗證。`/root/stockAgent/artifacts/markets/tw_day_trade_v8_combined_annual_log_cash_sub_lot_first_session_all_folds_v5` 是指向該 release 的 `READY` materialized cache 連結，另有七日租期及待部署 pin。
- D 掛載曾因 `/srv/stockagent-d-volume` 為 `msize=65536`、canonical mount 為 `msize=8192` 而拒絕發布；停 Syncthing、確認無活躍冷庫寫入後重新掛載，兩者現在均為 `8192`，`scripts/mount_packed_d_cold.sh --check` 通過。發布後掃描一度因 Syncthing 全庫掃描逾時，但 pending 收據已由重試清除。其併發重試競態已修正；最後重試收據為 `idle_no_pending`。當時本機 folder `idle`、need bytes/items/deletes、errors、pull errors 均為 0，vastai1T completion 100%、remoteState `valid`。這些是運輸證據，不宣稱 Vast 已 materialize 此 release。

## 隔離驗收

- 新推論 mode `score_entmax_log_cash` 已與 checkpoint 的 99 特徵 ABI 對齊；正式模型連結上的 GPU 推論使用 9/24 官方開盤價與 9/23 已完成特徵，開盤報價覆蓋 99.17%。CPU FP32 與 GPU BF16 的目標總曝險分別約 34.89% 與 34.80%，是計算精度差，不是面板不同：48 與 256 日 panel 在模型使用的 32 日特徵逐格相同。
- [候選訊號收據](../artifacts/candidates/tw_day_trade_v8_annual_log_cash/open_inputs/tw_day_trade_v8_annual_log_cash/backfill_receipt.json)涵蓋 2026-02-25～2026-09-24 共 146 個交易日，146 個 summary SHA 均相符，最低開盤 quote 覆蓋 98.79%，均明確標為反事實紙上訊號，不能冒稱當時即時產出。
- [隔離重播帳本](../artifacts/candidates/tw_day_trade_v8_annual_log_cash/simulation/rebuild_receipt.json)用原本的 09:01 起逐分鐘 50% 量續單與 13:20～13:30 出場規則，146 日各有 270 筆 09:01～13:30 分鐘權益標記，共 39,420 筆、無非有限權益。最後標記約 TWD 12,167,340；這是研究假設下的紙上結果，不是券商成交。
- 14 日有個別持倉缺少分鐘來源，帳本明示 last-trade carry，沒有補造價格。106 日因依原研究假設保留融資融券殘倉而非 flat，故 `execution_evidence_complete=false`；最後日仍有 10 筆非零紙上部位。不得將此標記解讀成全部實際成交。
- 相關推論／checkpoint 測試 322 項、Discord／重播介面測試 180 項、Syncthing 掃描測試 10 項通過；另有正式路徑 GPU 推論與 146 日隔離重播實測。

## 正式服務切換與未合併的歷史（同日 14:08 後）

使用者明確決定放棄兩個舊紙上帳戶的殘倉。已刪除 `tw_day_trade_attention_layernorm` 和 `tw_day_trade_multi_basis_projection_l1_gelu` 兩份日間策略入口設定，並將新 `tw_day_trade_v8_annual_log_cash` 改為 `enabled: true`。三個服務（當沖引擎／Discord／公用網頁 gateway）重啟後均為 active；Discord Gateway 連線及 app-command 同步成功。正式引擎、Discord 與網頁 revision 的啟用集合均為 `tw_day_trade_100m`、`tw_day_trade_multi_basis`、`tw_day_trade_multi_basis_22`、`tw_day_trade_v8_annual_log_cash`，revision lag 為 0，ledger divergence 為 0。網頁 status、曲線、訊號／部位／事件明細已按啟用集合過濾；歷史快取也以啟用集合失效。舊兩個模式不再運行、估值或出現在正式頁面，`/signal_now` 不再能解析已刪除的市場 ID。

切換前兩個舊模式分別有 5 與 1 筆非零紙上部位。這六筆沒有被虛構成平倉交易；其既有 state 與 append-only ledger 仍留在本機作稽核／復原證據，不是活動帳戶。其他保留模式當時另有 192 筆非零紙上部位，完全沒有被移除。切換前 `state.json` SHA-256：`ec7727ff6afc21904a537268534b293528efe9abafc673bc21eb1544fc71ebf5`。因此這是活動策略退役，不是刪除跨帳戶混存的原始稽核檔；不能聲稱舊紙上融資／融券真的在市場平倉。

新模型的獨立 TWD 10M 帳戶已透過 `scripts/add_tw_day_trade_paper_account.py` 匯入正式帳本，替換了引擎自動建立但從未有訊號、部位或現金變動的空白占位帳戶。候選重播先完成獨立持倉重估：3,616 個必要 symbol-date 全有來源、0 缺口、原重播權益差異點 0；正式 replay validator 通過。匯入在帳本 writer 鎖下建隔離後繼目錄，驗證舊五模式狀態完全相同、所有舊 ledger 位元組前綴不變後才做同檔案系統原子交換。第一次嘗試因外部監督者重啟引擎而被鎖閘門拒絕，沒有交換；第二次用暫時 `/run` 啟動條件保護維護窗後成功，條件已刪除並驗證服務恢復。正式 [帳戶加入收據](../artifacts/live/tw_day_trade_simulation/account_addition_receipt.json) 記錄回滾目錄 `artifacts/live/account-addition-wr5bvwna/ledger`，仍保留完整舊帳本。

正式新帳戶 09/24 結尾權益為 TWD 12,167,339.85，尚有 10 筆按既有研究假設保留的紙上殘倉；這些是新模型自己的部位，不是被刪除的兩個舊模式。正式網站已驗證 2026-02-25～09-24 共 146 個交易日、每日日間 09:01～13:30 恰好 270 點，合計 39,420 個新模型分鐘點，`resolution=1m&encoding=v2` 載入完整；9/24 訊號、部位、委託／成交明細也能按新 market 查得。舊模式的同類查詢總數為 0。合併後的六個歷史帳戶共驗證 236,520 個分鐘點、0 個無來源 interior row；正式啟用集合仍只有四個。

合併收據的 `strict_joint_parity=false` 是保留的明示差異：舊正式帳本 09/24 即時估值與事後分鐘重估不相等（1,160 點，含已退役模式），不能稱全舊帳本與重估逐點相同；其餘日期沒有此差異，新模型自身的獨立重估則逐點通過。舊帳本原位元組未改，沒有為追求「綠燈」去改寫其他保留模式的歷史成交或部位。

新 checkpoint 的無寫入正式推論測試成功：最新完成資料日 `2026-09-24`、fold 10、checkpoint fingerprint `0b4e5810007f`，面板快取命中，推論約 992 ms、推論前至準備完成約 2.37 s；價格使用當日官方 close 的 panel proxy，非開盤即時訊號、亦非成交。週六 09/26 引擎正確顯示 `waiting_trading_day`。服務 `active` 不保證下個交易日 09:00 成功，仍須以當日訊號耐久提交與後續因果行情證明驗收。
