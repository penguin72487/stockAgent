# 鉅亨抓取頻率重測與 512 秒退避修復

2026-10-09，時間使用 Asia/Taipei。任務 `news-adaptive-capacity-20261009`。

鉅亨的 **512 秒／1 並行是客戶端退避缺陷，不是已查證的官方限制**。
已修復、完成固定頻率重測，並恢復正式下載；目前採用 **0.1 秒開始間隔、8 個並行槽**。
三個完整驗證窗口實際為 **6.26–8.89 req/s**，均無限速或傳輸錯誤。
這是目前已驗證可運行的設定，不是鉅亨官方上限或所有歷史已下載完成。

## 512 秒的原因

[修復前快照](../artifacts/data_quality/news_cnyes_retest_20261009/before.json)記錄
512 秒開始間隔、1 槽；官方／robots／已學到的限速間隔硬下限為 0，沒有 429 證據。
[舊容量 checkpoint](../artifacts/data_quality/news_cnyes_retest_20261009/calibration-before.json)
保留了異常狀態及歷史窗口，未刪除原始資料或收據。

舊控制器把 transport／5xx 錯誤視為永久速度限制，反覆倍增開始間隔。
256 秒再遇一次錯誤就成為 512 秒。健康窗口又要求至少 20 個請求及回應，
但最多觀察 600 秒：`20 × 256 = 5,120 秒 > 600 秒`，因此即使其餘回應都是
200／304，也收不到足夠恢復樣本。持久化 checkpoint 讓重啟後繼續卡在低速。

## 共用控制器修復

- 區分暫時傳輸故障、端點／內容協商錯誤及真正限速。406／422 等端點問題不再靠減速修復。
- transport 退避有上界。鉅亨設定 `capacity_transient_backoff_max_seconds=0.5`，
  連續故障不再把客戶端間隔倍增至數百秒；來源 circuit 的實際等待期限仍保留。
- 多個健康窗口可以恢復已審核基準；304 可證明傳輸恢復，但不能證明新增正文吞吐提高。
- 429、Retry-After、robots 與官方間隔仍是硬限制，不因基準為 2 req/s 而覆寫來源限制。
- 容量量測契約升為 v3，不再載入不相容的 v2 退避狀態。來源 cooldown 與共用 limiter
  的 deadline 分開保留；這輪也確實等到舊預約期限後才開始送出請求。
- 固定頻率 benchmark 禁用自適應控制器，避免 checkpoint 或在線調整悄悄覆蓋測試頻率。
  調整 HTTP 控制不更動來源資料 fingerprint，不為測速重建另一份下載工作。

實作：[容量控制器](../downloader/news_fulltext_capacity.py)、
[固定測速](../scripts/benchmark_news_providers.py)、[正式設定](../configs/news_fulltext.json)。
面板新增「暫時故障恢復驗證，非限速」與「端點／回應結構待修，非限速」狀態。

## 固定頻率實測

正式 run `cnyes-fixed-ladder-retest-20261009T004525-07d4500f`，08:45–08:55。
暫停正式新聞 owner，透過原佇列與 exclusive HTTP owner 依序測速；沒有疊加第二條同源流量。
每級執行 45 秒再 drain，完整 wall 包含解析、去重、壓縮、fsync、提交及獨立收據核驗。
鉅亨每級最多 8 槽，僅既有批次正文工作，沒有另外送負載測試後丟棄資料。

| 目標 req/s | 實際 req/s | 請求數 | 新保存正文 URL |
| ---: | ---: | ---: | ---: |
| 2 | 1.92 | 94 | 33 |
| 5 | 4.81 | 226 | 317 |
| 10 | 6.92 | 330 | 3,082 |
| 20 | 4.67 | 220 | 2,162 |
| 30 | 4.58 | 216 | 1,849 |
| 50 | 4.81 | 230 | 1,975 |
| 75 | 5.63 | 265 | 1,837 |
| 100 | 5.70 | 268 | 1,993 |

以上全部回應為 200／304，沒有 429、拒絕或 transport／5xx 錯誤。
**100 是本次有限客戶端設定上界，不是實際持續送出 100 req/s，也不是來源上限。**
較高目標沒有增加實際輸出，因此未為等到 429 而無限增加客戶端並行或壓力。

選擇這輪新正文吞吐最高的目標 10 req/s，另外驗證三個不重播原頁的窗口：

| 驗證窗口 | 完整 wall 秒 | 實際 req/s | 新正文 URL | 新正文 URL/s |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 46.99 | 7.77 | 2,996 | 63.76 |
| 2 | 46.56 | 8.89 | 3,040 | 65.29 |
| 3 | 47.00 | 6.26 | 2,639 | 56.15 |

三個窗口全部健康且核驗通過後才寫入正式容量 checkpoint；相同候選若被拒絕，
或重複驗證的錯誤率超過 1%，不會升為穩定設定。

[原始測速證據](../artifacts/data_quality/news_cnyes_retest_20261009/fixed-rate-retest.json)
包含每級目標與實際 HTTP 控制、回應狀態、階段 CPU／wall、raw／body／batch SHA 核驗。
全流程共 **603.74 秒**，其中包含首次來源等待；CPU 433.32 秒，程序 peak RSS 570,320 KiB。
共 **2,922 個請求、21,923 個新正文 URL／核驗正文版本**。
不同分類、日期與重複率仍影響窗口，因此這不是公平重播的全域最佳化證明。

## 吞吐瓶頸

目標 20 req/s 的窗口 CPU 為 46.33 秒、wall 47.09 秒；其中正文提取消耗 33.00 CPU 秒。
目標 30 同樣約用滿一個 CPU 核心。網路回應與並行槽以外，正文解析及正式保存也限制吞吐。
堆高目標頻率不能讓已飽和的本機管線變快，也不能據此說鉅亨在 20 或 30 req/s 限速。
後續若要再提高實際送出率，應先量測並優化解析；不能省略正文驗證、原始回應或耐久保存。

## 正式服務與資料驗收

正式服務在 08:55:30 恢復，PID `179033`、owner `32c1a9c3040c4f41b8cf579058c71725`。
[恢復後基準](../artifacts/data_quality/news_cnyes_retest_20261009/production-before.json)與
[同 owner 進度](../artifacts/data_quality/news_cnyes_retest_20261009/production-after.json)
在 110.93 秒 DB 窗口獨立新增鉅亨 **5,940** 個正文 URL。
HTTP 快照窗口另為 122.02 秒，835 個請求，約 **6.84 req/s**；兩種時計不混算。
程序、queue／status owner、載入設定及原始碼均一致，沒有重啟迴圈。

[近期 128 批核驗](../artifacts/data_quality/news_cnyes_retest_20261009/body-verification-recent.json)
驗證 **3,136** 個正文版本，收據、raw/body/file SHA 及 pinned queue heads 無錯誤。
它只證明該時間點的近期寫入，不是全庫或所有歷史的驗收。

`production-after.json` 的廣義 `accepted=false` 保留：舊多來源驗收條件要求 Fed 與鉅亨
都新增正文，但該窗口 Fed 僅有 304、無新增。其他工程檢查通過，不能把這個判定改成
「所有來源正常」，也不能用它否定本次已證實的鉅亨進度。

[最終回歸](../artifacts/operations/agent-workflow/runs/cnyes-fixed-rate-final-regressio-20261009T005709-cf1f7605/run.log)
為 **430 passed in 50.97s**，包含無界退避、真限速持久化、固定頻率不受 adaptive 干擾、
429 中止升速及回到健康候選、驗證失敗不升級、端點錯誤、時間戳、正文核驗與面板隱私。
這些合成案例不是來源實測速度。

新聞 timer 已重新啟動且保持 enabled；測試期間的額外恢復保險 timer 已停止。
來源狀態、原始資料、收據與未完成工作均保留，沒有重啟交易服務或增加 GPU 工作。

## 面板驗收與剩餘邊界

本機 [四種寬度的瀏覽器驗收](../artifacts/data_quality/news_cnyes_retest_20261009/browser-loopback/acceptance.json)
通過 1440／768／390／320 像素，包含全部清冊分頁、篩選、排序、進度計數與行動版版面。
沒有 JavaScript 錯誤、正文／金鑰外傳或因看面板而發出的媒體請求。
本機 API 已顯示 0.1 秒／8 槽、目標 10 req/s 與獨立實際窗口速度。

[公開網址的驗收](../artifacts/data_quality/news_cnyes_retest_20261009/browser-final/acceptance.json)
則因本機連線逾時失敗；本機通過不能當作外網已通過，也不證明其他網路都無法訪問。
公開地址仍為 [新聞監控](https://penguin72487.ddnsgeek.com/news/)。

官方最高頻率、全天穩定性、單日頁數截斷、完整歷史與其他媒體的覆蓋仍未證明。
`publish=false`、`remote_training_export=false`、`training_injection_enabled=false` 維持不變；
個人研究 capture 不代表已取得再散布或模型訓練授權。
