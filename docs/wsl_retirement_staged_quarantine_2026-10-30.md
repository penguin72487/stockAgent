# WSL 退休：分段隔離稽核與 2026-10-30 觀測

## 2026-10-31 01:33 退休批次精確掃描

01:32:53 的原 migration owner 有效持鎖，`finmind-free` 正在獨立還原
1,875,703／11,849,443 檔、15,252,450,346／131,540,060,545
logical bytes；84 個物件完成，進度是進行中工作而非驗收。舊的 44 分鐘
是已完成的第 6 輪估算，不是此工作的剩餘時間。53 個來源已驗證、
7 個等待重試、1 個正在還原；這不是所有歷史或 NAS 完成的證明。

另外觀察到原 private backup cycle 的 owner 在 `retire_transport_cache`
等待 Syncthing 掃描整個 ingress；01:30 原 ledger 有 112 個已退休但尚未
記錄刪除掃描的批次。原設定單次掃描可等待 900 秒，不能把這些同步
通知算作 NAS 新驗收。Syncthing 官方文件與 2.1.7 handler 允許對已刪除
路徑指定多個 `sub`：<https://docs.syncthing.net/rest/db-scan-post.html>。
現改成在原 owner 下驗證每個封套與 `batch-<identity>`，以最多 64 個
精確路徑送一次通知，並對每個成功群組保存原 ledger；失敗群組與
後續群組保留待重試，既有完整刪除前 gate 不變。

隔離的 Syncthing 2.1.7、2,040 個小測試檔的 A/B/B/A：完整資料夾
0.544／0.466 秒，指定路徑 0.017／0.029 秒；驗證已刪除的兩個 `sub`
都被索引、未指定的另一個刪除不會被誤標記。這不是正式大資料夾的
整體速度，也不是 NAS ACK。Fintech 備份與退休測試 175 項通過，
另一組備份交接、原生傳輸及 lakehouse 恢復測試 60 項通過、12 項
依既有條件跳過，Ruff 通過；實際 backup role 的 Python 3.12 可載入
且 py_compile 通過，該 role 沒有 pytest，未冒稱在該 role 跑了完整測試。

01:26 啟動的 backup cycle 仍用舊碼等待原掃描，不中斷其 owner；
服務正常下次啟動才會載入精確掃描。修改亦改變 lakehouse code identity，
因此新候選的原 owner 自然釋鎖交接已排入
`lakehouse-scoped-retire-scan-dra-20261030T173235-b0b6dabb`；
之前等待舊 identity 的交接將在身分核對時拒絕，並非已部署。此交接
也包含先前實測的 1 GiB verified-memory pack reader；正在執行的冷還原
仍是舊 worker，未因候選測試而加速。

01:31 compiler-cache 原維護器實際回收 0 bytes，沒有合格項目；
上一次已登錄退休計時器因 cold peer 尚未收斂而安全暫緩。約 01:33
WSL 可用 226.7 GB、C backing 可用 15.34 GB、D 可用 347.9 GB；
不可將 WSL 與 C 的數字相加。C 實體仍低於 scratch 預留，當前活躍
SQLite 和尚無完整還原證據的暫存繼續保留。當沖模擬服務仍在執行，
其 activity 只代表服務狀態，不代表新的交易已成交。

七個 migration 等待重試中，`tw-index-futures` 與其 local complement
因同一來源樹的絕對 symlink 失敗：`build_inputs` 下 v128、v153、v157、
v165、v175 共五個連到 `/dev/shm/stockagent-universe725-source-build-*`，
本次讀取時五個目標均不存在。目標目前缺席不能證明其 provenance 或
其他版本可丟棄；尚未改寫連結、來源與歷史 journal。另有會變動的
runtime JSON、live panel 世代消失，以及相依來源尚未遷移完成的重試；
必須由原 owner 及相應來源契約處理，不能一律視作無用資料刪除。

TW public 23:50 冷發布因官方 symbol 與 feature 收據過期拒絕；
01:30 只讀 reconcile 預檢仍需要重建，原 08:00 交易日 owner 與
來源收據觸發的 cold publisher 均保留。沒有以修改收據繞過檢查，
也沒有在本次掃描工作強制重建 9 百萬列或重啟當沖服務。

## 2026-10-31 01:16 FinMind 冷還原與大包讀取

先前 44 分鐘是第 6 輪的階段估計；該輪的 WSL 退休與驗收已完成，
不能套用到現在的 FinMind。原 worker 的 `finmind-free` 固定版本來源
共 11,849,443 檔、131,540,060,545 logical bytes，來源雜湊及封包
處理已完成；01:16:20 的冷庫原檔還原核對為 1,028,535 檔、
9,098,293,430 bytes。當時目前大包讀到 8,192／32,768 成員；
42 個 cold 物件通過完整檢查。這是逐檔進度，尚未取得完整還原收據，
不是 NAS 驗收或來源退休授權；較小和較大的包費時差異很大，不能用
單一短時間每秒檔數可靠外推全程完成時刻。

實際清冊中有 64 個 256 MiB 以上的 pack，共約 50.45 GiB；其中當時
尚有 57 個未完成。C: 實體剩餘低於既有 scratch reserve，大包超過
舊 256 MiB 記憶體上限時會完整 hash 後退回 DrvFs 逐成員讀取。
在正式服務的 mount namespace，對一個固定 772,989,351-byte 大包
完整 native 讀取並核對 SHA 與來源 generation 後，抽查 512 個分散
成員的 A/B/B/A 時間：DrvFs 為 9.665／12.204 秒，記憶體為
0.370／0.306 秒，每次 CRC 與成員 SHA 摘要相同。完整物件載入和 SHA
另需 14.161 秒，峰值 RSS 為 826,960 KiB。這是局部抽樣，不是整個
release 的 benchmark、完整重建或 NAS 收據；原始紀錄見
`artifacts/operations/storage-single-authority-20261025/finmind-verified-memory-benchmark-20261031.private.json`。

候選將單包記憶體上限由 256 MiB 提至 1 GiB，仍要求量到的有效 headroom
不少於包大小四倍加 512 MiB；資訊不足或不符合時保留原本的完整 SHA
與逐筆讀取 fallback。仍須逐一核對 ZIP CRC、原始檔 SHA、檔案長度、
清冊與前後 generation，沒有改寫冷資料或刪來源。fintech 環境相關
測試 468 項通過；正式 lakehouse role 環境的 native pack 相關測試另有
56 項通過。
原 worker 沒有中斷，因而仍使用舊邏輯；新候選已用原 owner 自然釋鎖
交接排入 `lakehouse-memory-buffer-drained--20261030T171646-23fb4be7`，
24 小時是等待上限，不是預估完成時間。新候選改變 worker 身分，先前
等待舊程式的 handoff 會在交接核對時拒絕，不得當成已部署。

互動 shell 的 `/srv/stockagent-d-volume` 為另一 mount namespace，看到
`msize=65536`；正式 PID 1 與 worker namespace 均為 `msize=8192`，
正式 namespace 的 D primary 完整檢查通過。此次沒有重掛載；不能把
互動 shell 的預檢失敗當作正式服務 D 掛載故障。現行 worker 的 DuckDB
目錄註冊曾因舊 512 MB 設定反覆失敗，已存在的新程式亦須等安全交接，
尚未證實現場錯誤消除。

01:10 WSL 檔案系統約 211.16 GiB 可用、C 實體約 14.6 GiB、D 約
321.5 GiB，WSL 與其 C backing 不可加總。上一個大 metadata 索引已
由原工作移除；當前索引約 10.29 GiB，仍由 PID 929 開啟，不能刪除。
materialized cache 的 dry-run 有四個 pin、一個有效 lease，無可回收
項目。OpenBB 約 18.50 GiB 暫存依賴的完整恢復尚未完成；歷史 partial
及 quarantine 仍可能是缺件 donor，均保留。本次沒有符合 gate 的新增
大批來源回收；WSL 釋放量與 Windows C 實際釋放量仍分開報告。

01:18 採用原 peer probe 時本地資料夾已從先前掃描轉為 idle；不過
排定退休清單的 3 個 artifact 與 24 個 legacy 資料集目前都是
`cold-only`，沒有新的 `hot-enrolled` 對象。故沒有為了重跑計時器
而重複啟動完整冷檔核對；下次有新候選仍由原定時器和逐項 gate 處理。
01:10:37–01:18:22 原檔核對由 896,683 增至 1,036,727，約 301 檔／秒；
用這段短樣本直接外推尚餘檔數約 10 小時，但大小組成和來源讀取差異
極大，這只是一個不含後續階段的粗略量級，不能作為完成承諾。

## 22:07 FinMind 封包階段與面板背景清點讓路

22:12:21 更新：目前完成 hash 10,753,025／11,849,443，尚餘
1,096,418 檔；bucket 完成 9／64。22:07:24–22:12:21 完成 72,332 檔，
約 244 檔／秒，按此短時間速率外推剩餘打包約 75 分鐘；先前 57 分鐘
的外推已過時。這仍不是全流程 ETA，受資料大小與並行讀寫影響。

第 6 輪原退休及驗收已於 11:08 完成；舊的「約 44 分鐘」是該輪
當時的單階段估算，不是目前 FinMind 的 ETA。現行 PID 929／原 dataset
owner 在 22:07:24 回報 `packing_changed_small_files`：來源 cohort 共
11,849,443 檔已完成路由，10,680,693 檔已有 hash，還有 1,168,750
檔；64 個 bucket 完成 5 個。這是封包製作的進度，不是整體驗收、可
退休、NAS 備份完成或所有後續增量的進度。以 22:04:40–22:07:24 的
55,907 檔外推，剩餘打包約 57 分鐘，僅為短取樣估計，工作大小及
磁碟競用會變；之後仍有完整 storage 清冊檢查、冷庫還原及其他回收
gate，不能把此數當作總 ETA。

分流完成後，原 worker 已自行關閉並移除舊 metadata SQLite 暫存，
最後觀察的配置大小為 7,872,233,472 bytes（約 7.33 GiB）；WSL
可用空間亦受其他並行寫入影響，並非 C 槽實際等量回收。另一個
`stockagent-packed-metadata-7wak773g` 索引仍由 PID 929 使用，保留。
22:07 時 WSL 可用約 211.4 GiB、C 槽實體可用約 14.95 GiB，低於原
25 GiB 緩衝；WSL 位於 C 的 VHDX，兩者不能相加。原 non-sparse VHDX
回收 ext4 blocks 沒有保證在線返還 Windows 實體空間；沒有停 WSL 或
當沖服務。官方 compact 要求虛擬磁碟已卸載或只讀掛載。

儲存面板先前對 D 的只讀 `du` 掃描持續超過 21 分鐘並與 cold 工作
共用硬碟。現已讓 WSL／D 的可選分類清點觀察既有 dataset-version
owner lock：工作忙碌或無法觀測時暫緩，正在掃描時只收回自身唯讀
子程序，保留已完成子樹與原觀測時間；每分鐘容量採樣照常，並在
owner 釋放後重新排入。重啟只涉及 `stockagent-storage-monitor.service`，
PID 929 保持運行；22:07 的面板快照保留 D 部分結果
111,453,970,432 bytes，新的投影函式可顯示暫緩原因。原 gateway
仍是 09:20 啟動的舊程序：22:09 實際 member HTTP 尚無新欄位。
重啟它還會載入其他工作中較新的新聞程式，本輪未一併推送；靜態
頁面維持部分量測的正確文案，特定暫緩原因待原 gateway 安全交接。
短取樣中
D 佇列由先前約 2.1–2.2 降至約 0.4–1.0，但並行工作及時間不同，
不能據此宣稱因果或整體吞吐達硬體上限。8 秒 profiler 亦抽到冷庫
pack 寫入及既有 backup metadata 讀取，不能代替完整工作基準。

針對鎖觀測、不建立 owner lock、活動中讓路、部分觀測保留、重啟後
續排、容量持續更新與公開欄位白名單通過 67 個 monitor 測試；較早的
monitor／dashboard 組合 256 項通過，JavaScript 語法檢查通過。原
lakehouse worker 的程式更新仍在自然交接佇列，並未宣稱載入現有工作。
22:06 的 cache GC dry-run 有 4 個 pinned、1 個 lease-active，沒有
安全可刪除的項目。私人固定清冊最後一次 21:49 記錄 13,276 個已配對
NAS ACK 的檔案及 765,139,950,474 bytes 尚無配對 ACK；不是 NAS 缺檔
總量，也不能推定所有資料已完成備份。

## 17:50 舊清冊讀取進度與安全交接

17:46 實際取樣 worker 堆疊顯示，它已進入 `_iter_inventory`／
`MetadataRows.extend`，正在讀取舊版本壓縮清冊並建立第二份 metadata
索引，而非仍在來源 census。先前狀態檔最後的 11,845,512 檔只是來源
census 最後一次回報，不是已確認的完整總檔數；狀態檔沒有階段轉換
造成誤判。新程式在舊清冊讀取、驗證時分別回報階段、已處理及清冊
所載的總項目數，皆明示尚未驗收。新程式尚在原 owner 交接佇列，沒有
載入正在執行的 worker；現場讀取進度仍依其開啟的原檔 descriptor。

17:46:49 至 17:49:34，舊清冊 descriptor 的壓縮讀取位置由
22,155,264 增至 106,827,776 bytes；原檔共 759,613,076 bytes。
短時間外推約需再 21 分鐘讀完此階段；速度會變，不包含後續完整清冊
驗證、來源 hash、冷庫還原、NAS 讀回、退休與清理，並非整體 ETA。
第二份 metadata 索引同時成長，17:49:34 已為 1,270,411,264 bytes；
兩份 active indexes 仍由原 worker 持有。17:49 Windows C 實體可用
21,928,701,952 bytes，低於既有 25 GiB 緩衝，仍須監控，不能把 WSL
虛擬容量當作額外物理空間。

針對 metadata 插入進行 60,000 筆 A/B/B/A 局部試驗：直接批次版本
約 1.13 倍，但它改變批內失敗時的進度與例外語義；維持逐筆失敗語義
的生成器版本原版 1.299／1.203 秒、候選 1.538／1.118 秒，波動不
支持加速結論，因此沒有套用。這只是本機合成索引測試，不是完整資料
驗證基準。新階段報告及原掃描修正合計通過 365 項 packed 測試與 441
項遷移／metadata 測試；另有私人／一般備份 163 項通過、2 項跳過。
正式 worker 已重新排入原有自然釋鎖交接，run ID 為
`lakehouse-census-progress-draine-20261030T094934-22e3adbc`；需要等
交接收據，不能將測試速度當成現場已實現的速度。

17:48:25 私人冷庫原 cycle 已完成，4 批已在來源端封閉並發布，合計
311 個物件、2,131,574,644 bytes；這些批次仍等待 lab203 的配對 NAS
獨立讀回。周期狀態顯示固定私人清冊尚有 790,619,371,240 bytes 未有
確認的配對 ACK；此數不等於證明 NAS 無副本，也不包含尚未登錄來源。
原 backup controller 正在擷取公開目錄；其 timer 在 service 結束後
會續跑，保留原 owner 和配額。沒有刪除新的 source 或 NAS 物件。

如需 lab203 原 owner 協助，請沿現有通道核對下列封套是否已完整接收、
與派工配對，並逐批執行 NAS 原檔獨立讀回及完整 SHA 驗證後回傳 ACK；
同時回報目前接收容量與在途佇列。此清單是已發布封套，不是接收 ACK：

- `305522390a1d930457e43dcbbccffe8091b85943cb51bd278a2aa2b2727c3a43`
- `bd82176bc9a1919cba7b0d7b9569d38b176df98896b57df20c925088714ab33e`
- `df87ea37e6be1d6ce0ba05dc150c8c5388acbc0b3258967bf8c30cfdeca45cdc`
- `695e8d643aaa6eb1828c3491d427db0401e88f308c67ddb63983fb4f5d8b54f9`

## 17:46 備份啟動與本地可重建快取清理

17:45:11 FinMind 的狀態檔記錄 source census 已處理 11,845,512 檔，
但之後的程序堆疊顯示已進入舊清冊讀取；沒有可信的整個流程完成 ETA。metadata
SQLite 索引 17:44 約 6.11 GiB，仍由原 worker 開啟，不是可刪的垃圾。
本次最佳化的 worker 交接仍在等待既有 transaction 釋鎖，尚未載入。

私人冷庫修正已進入原 private-cycle：17:43 的第一批選擇 75 個檔案、
533,137,002 bytes；17:45:55 的狀態已列出兩個新封閉發布的批次，
下一批 79 個、534,777,303 bytes 正在複製與獨立 SHA 核對。這證明
先前因裝置號重新編配而卡住的實際路徑已能送出批次；目前尚無這兩批
配對的 NAS 獨立讀回 ACK，不能稱為 NAS 備份完成。固定清冊中仍受
其他錯誤或接收容量限制的項目也不能憑這兩批推定全部已修復。

以原 `cleanup_windows_wsl_temp_swap.ps1` 唯讀盤點使用者指定的
Windows Temp：零個符合條件的孤兒 swap；約 31.29 GiB 的 swap 有目前
活躍 WSL VM ID，因此予以保留。Windows 審計回執為
`wsl-temp-swap-audit-20261030T094428.8683982Z.json`，沒有執行刪除。

另查得 `/root/.npm/_cacache` 使用 477,593,600 allocated bytes，
沒有 npm 安裝進程、可見開啟的快取檔或多重 hardlink；以 npm 11.20.0
的 `npm cache clean --force` 移除這個可重新下載的套件快取，指令成功且
該快取目錄已消失。這不是正式資料或安裝環境。WSL 隨後的淨可用量
包含並行寫入，C 槽未因此等量釋放：17:46 Windows C 實體可用
22,200,311,808 bytes，仍低於 25 GiB 緩衝；WSL 本身可用
231,700,942,848 bytes，不能把兩者相加。

## 17:43 清冊效能與私人備份重試修正

先前「約 44 分鐘」所指的第 6 輪已在 11:08 完成 WSL 回收；該估算
不能套用到目前工作。15:20 的 OpenBB activity 在心跳逾時後中斷，當時
已處理 2,865,177／8,013,131 個檔案；現行 worker 隨後切到 FinMind。
17:42 FinMind 還在原 owner 的 `censusing_source` 階段，已掃過
11,511,982 個檔案，總數尚未建立，不能以舊版本的檔數推算目前 ETA。
目前不是第 6 輪仍未完成，也不能把持鎖直接認作死鎖。

對清冊路徑處理，改為每個目錄只算一次相對前綴，再以 `os.lstat` 讀取
各項原 metadata；不改變逐檔清單、篩選、符號連結、指紋或後續原值
驗證。在 tmpfs 人工樹的 A/B/B/A 測試，10,000 檔、2,525 個目錄、
24 個 exclusion 與 FinMind staging 規則，加上實際 SQLite spill 索引，
原版 2.864／2.909 秒，候選 1.668／1.429 秒，平均約快 1.86 倍；
兩版每筆記錄及清冊摘要一致。這只測此小型、暖快取清冊，不是完整
生產備份或硬體上限，也沒有宣稱目前仍執行中的舊 worker 已加速。

私人冷庫備份固定清冊建立於 10 月 28 日。17:39 的唯讀抽樣顯示重試
清單中 100 個物件的來源識別都只有 Linux `st_dev` 改變：122 變為 135，
inode、大小與 mtime／ctime 均一致。另選三個實際物件完整重讀，SHA
均符合固定清冊。修正後，只有私人冷庫原 authority 的磁碟與私有標記
檢查通過時，才接受舊裝置號重新編配；整個本次交付仍固定目前完整
signature，執行途中再發生裝置號或其他屬性變動立即失敗，並保留來源、
複製 SHA、獨立目的端 SHA、封套與逐批 NAS 讀回要求。單一真實物件在
隔離 tmpfs 測試完成 7,356,257 bytes 的來源／目的 SHA 核對；這不是
交付或 NAS ACK。原固定清冊、retry、失敗 staging 與既有 ledger 均保留。

清冊與儲存測試 364 項、資料遷移回歸 441 項、私人／一般備份回歸
163 項通過，另 2 項跳過。已取消本工作先前等待舊候選的工程交接，
重新排入 `lakehouse-census-and-backup-drai-20261030T094214-bab94b92`：
等待原 dataset/source/catalog owner 自然交接，並驗證實際載入身分與
workflow；12 小時僅為等待期限，並非完成 ETA。FinMind 舊 worker 保持
執行，尚未載入以上改動。私人備份另沿原 `private-cycle --wait-for-owner`
啟動 `private-backup-device-renumber-f-20261030T094203-fe0f6589`，17:42
已取得既有 owner 並進入傳輸空間盤點；尚須看到新封套與配對 NAS ACK，
不可預先稱為同步成功。

17:42 Windows C 實體可用 22,255,497,216 bytes，低於 25 GiB 緩衝；
WSL 虛擬檔案系統尚有 231,393,058,816 bytes，兩者不可加總。進行中
FinMind metadata 索引已占約 5.7 GiB，仍被 worker 持有；OpenBB 先前
約 18.50 GiB SQLite 準備副本仍待其對應 cold 還原等條件，不得逕刪。
本次測試自己建立的 tmpfs fixture 已在程序引用、外部 hardlink 檢查後
清除，約 250.6 MiB；不是正式 WSL 源資料回收，也不能視為 C 槽等量
淨空間增加。原 cache-pressure 維護當時沒有符合條件的檔案可回收。

17:31 固定 retained canonical inventory 的 NAS 檔案覆蓋中，2,664 件、
5,821,024,081 bytes 尚缺機器可驗證的配對讀回；此數不涵蓋全部私人
來源。私人固定清冊最後一次 17:19 記錄尚有約 790.6 GB 未獲 ACK，
並非證明 NAS 未有任何副本。25 個歷史版本／114 個物件仍未完成來源
重建；不能以傳輸佇列零或本地 SHA 通過宣稱 NAS 全部完成。
當沖模擬服務仍運行，但 guardian 持續顯示 6 個當日 signal commit
缺失；盤後分鐘曲線另外於 16:16 因 10 月 29 日 `open_position_count`
帳務不一致失敗，與上述清理是否有關尚無證據。

## 13:40 第 6 輪結果、NAS 缺口與後續交接

下方各節保留各時點的歷史觀測；本節是截至 13:40 的更新。第 6 輪已由
原 owner 寫入 `retirement.json` 和 `acceptance.json`：11:08:43 從 WSL
退休 **9,881,169,920 allocated bytes（約 9.20 GiB）**，11:59:30 完成
D 槽編碼暫存清理並釋放 **2,074,517,504 allocated bytes（約 1.93
GiB）**。固定 cold 版本還原並核對了 56,320 個原始檔及 3 個符號連結；
冷庫原物件並未因暫存清理而刪除。WSL 與 D 的回收量分別計算，不能把
它們當成 Windows C 槽淨可用空間增加量。

先前所說的約 44 分鐘只是當時針對逐檔核對的估算，現在已過時。這輪
隔離稽核約 32 分 25 秒，WSL 退休已於 11:08 完成；之後 D 槽暫存清理
的計畫及執行約 50 分 47 秒，直到 11:59 收據產生。大量小檔跨 WSL 與
D 槽的操作延遲是已觀察到的成本，尚無證據說整機已達硬體極限。
這兩段不是下一輪可以直接套用的完成時間。

目前已為往後的 D 槽暫存清理加入子階段與節流後的進度記錄，包含檔案
指紋、收據、全量冷庫／原始檔重驗和清理；執行中訊息明示不是驗收
證據。完成仍以正式收據和既有 generation、同步、程序／FD 與 pin gate
為準。此修改改善可觀測性，本身不是實測加速。原有隔離續跑在同步只剩
健康掃描時可重疊唯讀稽核的優化，最多能消除該路徑 300 秒串行等待，
既有刪除 gate 不變，已執行的本輪沒有動態載入此變更。新的小檔滑動
佇列在 D 槽 512 個人工收據讀取的 ABBA 為原版 0.879／1.222 秒、候選
1.054／0.775 秒；波動不支持實際加速結論，因此未投入正式流程。

本輪 manifest 與 head 有 NAS 獨立讀回收據；逐一對照固定清冊的 16 個
物件及 1 個壓縮清冊，5／17 有配對的 NAS 原檔讀回證明，剩餘 12 個、
931,160,631 bytes 在已查台帳中尚無該證明。這不等於證明 NAS 沒有
副本；也不能用傳輸佇列為零推論本輪全數備份完成。整體 source 目錄
最後成功刷新為 10 月 29 日 00:47，舊 worker 近期 catalog 更新曾出現
DuckDB 512 MiB 記憶體不足；dataset version owner 仍在處理資料，13:32
回報 53／61 已驗證。原 worker 不是等待第 6 輪持有的鎖，不能因此判作
死鎖或強制終止。13:38 對原 PID 的開啟檔案位置做兩次 8 秒取樣，確定它
正在處理一個固定 19,862,511,616 bytes 的 OpenBB SQLite snapshot，位置由
18,966,642,688 增至 19,042,140,160 bytes，約 95.87%、9.0 MiB/s。
按這八秒速度外推，該檔讀取剩約 87 秒；這不是整筆資料集、後續重驗、
釋鎖、程式交接或 NAS 備份的 ETA，速度可能變動。此觀測也支持 owner
仍在執行工作而非僅憑一段時間持鎖判為死鎖。
13:39:54 該快照的原讀取 FD 已結束，worker 堆疊進入後續 controls
準備。其 WSL 暫存檔 `stat` 為 19,862,540,288 allocated bytes、單一
hardlink；原 migration 啟用了 owned scratch 退休，安排在該資料集與
固定 SQLite 依賴完整冷庫還原，以及 SHA、程序引用等檢查通過後回收。
這是尚未完成的條件式候選，非已回收數量，也沒有已知全流程 ETA。

13:39 Windows 只讀查詢顯示 C 的實體磁碟是 NVMe SSD，D 是 SATA HDD；
一次效能取樣中 D 寫入約 7.97 MB/s、disk time 約 80%、佇列長度 3，
C 約 11% disk time。原 worker 的多條活躍執行緒同時在 WSL 的 p9 I/O
等待；這些觀測支持 D 隨機小檔／metadata 和跨系統操作是限制因素，但
單點快照不能證明硬體已達極限，也不能替各輪建立可信的整體 ETA。

現有磁碟程式的 catalog 修正和批次操作已在正確的 lakehouse 環境通過
190 項測試，並成功唯讀重播目前 3 條 workflow 共 374 個歷史事件。
已啟動原 `rollout_lakehouse_source_policy.py --reload-code-only` 的
受監督交接，等待 owner 自然釋鎖後才載入新版本；最長等待六小時是
逾時保護，不是完成 ETA。交接、目錄刷新及這 12 個物件的配對 NAS
讀回尚未完成，仍須查看後續收據。測試及 workflow 重播不等同生產
環境完整驗收。

13:32 量得 WSL 檔案系統可用 253,927,821,312 bytes、Windows C 可用
64,489,951,232 bytes、D 可用 366,010,658,816 bytes。WSL VHDX 位於
C，兩個可用量不可相加；回收 ext4 空間也不保證立即還給 Windows。
本次另在無程序引用檢查後清除本次測試自己產生的暫存，回收約 65.57 MiB；
未把未知來源或尚缺 producer 證據的測試副本當作可刪資料。
13:35 原本的 materialized cache GC 檢查 5 個已登錄 lease，4 個 pinned、
1 個仍有效，故沒有新刪除。除上述條件式 OpenBB 暫存外，本次尚未凍結
新的合格大批退休清冊，也不能聲稱下一輪已排完或估算其完成時間。

13:35 從既有部分清點另發現兩個歷史 materialization：2026-08-25 的
`.partial` 約 5.25 GiB，2026-08-21 的 quarantine 約 9.13 GiB。兩者均
對應 lab203 待修復的 25 個歷史版本之一。對前者的 manifest 進行 metadata
預檢，405 個引用物件中有 31 個目前在本地冷庫路徑不存在，合計
291,042,215 bytes；該候選目前不符合完整冷庫還原條件，不得逕行刪除。
針對 08-21 版本既有稽核列出的 16 個未解物件，各取先前第一個不符的
原始成員並對照其固定原始清冊：quarantine 有 8 個樣本的 size／SHA 符合，
partial 有 1 個符合。這只證明單一成員匹配，並不代表任何完整 pack
已可重建。read-only 候選預檢對這 16 個物件比較 quarantine、partial 和
現行來源：有 11 個連大小相符的完整成員集合都不存在；另外 5 個雖有
完整大小候選，但依原始清冊逐成員 SHA 比對均不完整。這只涵蓋 16 個
物件，不能推論其他歷史缺件完全無法修復。仍需完整成員與固定 object
SHA 重建及 D 獨立讀回後，才能主張該物件已修復；以上暫存皆持續保留，
避免回收時丟失可能有價值的歷史版本。逐物件結果保存在
`tw-quarantine-donor-union-preflight-20261030.private.json`。

本次精確收據、交接與測試紀錄位於
`artifacts/operations/storage-single-authority-20261025/`；先前各輪收據和
本輪結果也不代表尚未清點的歷史資料已全數入庫或完成 NAS 備份。

## 10:25–10:30 冷庫還原完成與掃描等待優化

第 6 輪的固定版本獨立原值還原於 10:24:55 完成：收據記錄 56,320 個
原檔、3 個符號連結、9,735,706,066 bytes，cold 與解碼原始檔均通過
核對。這份收據不是 source quarantine 的退休證明，也不是 lab203 的
NAS 驗收；此時 retirement／acceptance 收據仍不存在。

原程式在還原後先等待 Syncthing 收斂最多 300 秒，再開始唯讀 quarantine
audit。10:25 至 10:27 觀測顯示待傳項目和 bytes 為零、peer 已連線且
100%，唯一未完成條件是本地資料夾掃描。原 owner 於 10:29:56 開始
`full-quarantine-resume-audit`，10:32:04 正驗證 cold objects，15／16
個物件及 784,616,181／986,551,199 bytes；這只是其子階段，後續仍有
編碼／原檔、鏡像、cold resilience、最終 generation 及同步核對。
10:33:52 已進入編碼檔 SHA 子步驟，13,565／56,321 個檔案；檔案大小與
跨系統延遲不同，不能以此比例視為整輪進度或推定最終回收時間。
`retirement_options` 現在僅在
隔離續跑已完成冷庫還原、同步檢查除了掃描外全部正常時，讓唯讀 audit
與掃描重疊；peer 中斷或其他異常仍沿用原等待流程。實際刪除前仍須用
新鮮 proof 確認同步收斂，及通過原有 source／cold、consumer、FD、pin、
owner、generation 和有效時間 gate。此修改最多消除這個路徑上 300 秒的
串行等待，不能保證整輪縮短同樣時間；原本已在執行的 owner 不會動態
載入修改，未重啟它，也沒有宣稱本輪已享有這項加速。

相關 27 項測試通過；在隔離 fixture 重跑新增的六個參數化／流程案例亦
通過，覆蓋健康掃描先稽核、異常等待、未收斂時保留 quarantine、恢復
收斂後才可退休。這是測試資料，不是正式資料已刪除的證據。

10:31 的當沖 guardian 持續報六個模式未提交當日訊號；實際模擬引擎
回報等待 09:01 來源價格。該日恢復收據記錄 1,047 個候選來源價格未
解析、本地查詢零個，永豐歷史查詢因盤中保護延至 13:35；這是程式
排程狀態，非官方 API 已拒絕。官方盤中歷史逐筆次數限制與當沖優先
需共同考量，不能透過大批重試繞過。FinLab 既有分鐘有限查詢在
09:46 顯示等待一般資料條件，亦未驗證可直接作為此引擎輸入；沒有
憑等待狀態編造價格或重啟當沖服務。

## 10:24 逐檔驗證進度與加速試驗

第 6 輪仍是原 scheduler／原 owner；沒有啟動第二個退休交易。固定版本的
獨立冷庫重建已經重建 56,321 個編碼檔。第一輪完整逐檔 SHA 是
09:28:41–09:50:34，共 **21 分 53 秒**，先前單階段估計 44 分鐘偏高；
隨後的第一次整樹核對為 6 分 06 秒。原始 56,320 檔和 3 個符號連結的
解碼完成，解碼後另一輪逐檔 SHA 為 10:11:57–10:22:50，共 **10 分
53 秒**。10:24:25 的解碼後整樹核對觀測為 35,673／56,578 項，仍是
metadata 進度，不是驗收。下一步還有隔離資料的完整 audit、冷庫與同步、
活躍程序／FD、pin、原 owner、新鮮度及 unlink 前 gate；此時沒有第 6 輪
的 retirement 或 acceptance 收據，不能把候選 9.20 GiB 寫成已回收，也
不能從一項完成比例推算整輪 ETA。

現場四個驗證執行緒多次等待 WSL D 槽 `p9_client_rpc`；此批 56,320 個
原始檔中，35,340 個小於 4 KiB、20,820 個介於 4 KiB 和 1 MiB，僅
160 個至少 1 MiB。這支持跨檔操作延遲是重要成本，但不是整條流程或
實體硬體已達上限的證明。保留跨來源與解碼後完整 SHA、完整樹及來源變動
檢查；新增工作一律保持低優先權，不中斷當沖或原退休 owner。

真實 D stage 的 512 個小檔只讀測試，使用 Linux 端持有 no-follow FD、
前後 generation、目錄 pin 和全 SHA，比較 Linux 多執行緒雜湊／Windows
原生批次讀取，ABBA 為 **11.167／18.198／14.343／20.076 秒**；後兩組
的波動大且平均未證明原生批次較快。這不是完整驗收，不把實驗讀取加入
正式證明，也不改用原生 batch。另一個 4／8 workers 的同等保護測試，
4 workers 完成 512 檔用 36.968 秒，但因與正式作業共用 D 槽而主動停止
只讀測試，未完成對照；不能推論 8 workers 較快或較慢。

另做「最多 64 項在途、按清冊順序回報，不等整批末檔才補下一項」的滑動
佇列候選。既有 10 項 verifier 語意測試通過，隔離測試中的毀損 SHA 與
額外檔案均遭拒絕。`/dev/shm` 的 512 檔**人工延遲**完整 verifier
ABBA 為原版 **1.311／1.307 秒**、候選 **1.075／1.086 秒**，不代表
真實 D 的改善；真實 D 的 128 小檔 SHA／大小微量 ABBA 為原版
**3.041／2.196 秒**、候選 **2.437／2.333 秒**，未包含整樹與整個退休
流程，快取和其他 I/O 未控制。原版末次反而最快，尚不足證明正式流程
獲益；候選只在獨立 process 中試驗，未修改或替換正式 verifier。
細節與樣本收據在同目錄的 `r6-guarded-small-hash-abba-20261030.private.json`、
`r6-sliding-verifier-fixture-abba-20261030.private.json` 和
`r6-sliding-d-sample-abba-20261030.private.json`。

程式檢查另發現目前隔離續跑先獨立完整恢復一次，再以相同固定版本進行
隔離來源與冷庫的完整 audit；有機會把兩個驗證的讀取合併於同一 transaction，
但必須保留全量 cold reconstruct、解碼後 stage 重驗、每個原始 SHA、
source／hot／cold generation 前後綁定、同步及 consumer gates，以及獨立
可稽核的 recovery／retirement 證明。這只是後續設計方向，目前沒有實作或
宣稱等價，不能用來跳過本輪驗證。

10:22 左右量得 WSL 檔案系統可用 270,955,474,944 bytes，Windows C
實體可用 82,691,444,736 bytes，D 可用 380,414,754,816 bytes；WSL
空間位於 C 的 VHDX 內，不可把兩者相加。10:24 lab203 接收觀測為 pending
0、無背壓，但該稽核明示全歷史備份尚未驗證。當沖 guardian 10:22 為
failed，列出六個當日 signal commit missing；這不是已確認成交，也不能
因 worker 正在運行就宣稱盤中正常。

## 09:26 Windows 實體容量恢復

使用者明確表示不使用休眠並同意停用後，以 Windows 原生提權執行
`powercfg.exe /hibernate off`，程序 exit code 為 0。重新查詢 `powercfg /a`
確認休眠停用、S3 一般睡眠仍可用。C 槽在操作前觀測可用
27,839,303,680 bytes，操作後觀測 82,773,942,272 bytes，淨增加約
51.16 GiB；其間仍有其他寫入，這是兩次量測差，不宣稱全部為單一檔案配置量。
原第 6 輪在 09:25:55 的上一次容量檢查仍看到舊值；後續須由原 scheduler
重新測量、取得相應 owner 並完整驗證，尚不能記為第 6 輪已回收。
原 scheduler 於 09:26:59 的新計畫實際量得 C 可用 82,755,821,568 bytes，
容量 deferral 清空並啟動第 6 輪；09:27 原 owner 進入固定 D 發布的獨立
原值還原與核對，回收量仍為零。進入驗證階段本身不是冷庫驗收或退休收據。
此輪 12 個 cold blob generation 的紀錄顯示約 0.41 秒完成；隨後進入
16 個 cold 物件、986,551,199 bytes 的驗證步驟，09:27:55 觀測為
1 個／15,468,783 bytes。這只是該子步驟，不是 9.20 GiB 回收總進度。
09:28 固定 packed release 的封存編碼樹重建子步驟回報 56,321／56,321
項；09:29 進入同一編碼樹的逐檔核對，紀錄為 382／56,321 regular files，
執行緒堆疊確認四個 worker 正在讀取、雜湊或取得 metadata。其後仍須解碼
原始檔、重核整棵樹與所有退休 gate；不同階段不能相加當成總百分比。

這是 Penguin 台北時間 00:37–00:45 的觀測和程式測試，不是全部資料
退休或 NAS 獨立驗收的證明。既有任務和不相關的 dirty work 均保留。

第 6 輪已在 10 月 29 日 23:12 完成 56,320 原檔、9,735,706,066 bytes
及 3 個符號連結的固定 D 獨立重建與解碼。23:30:09 的完整 source／mirror／
D 稽核在 23:56:45 最終 gate 超過 1,500 秒時限；程序立即保留 quarantine，
沒有 unlink。phase history 顯示共用鎖取得很快，鎖內核對耗時 1,589 秒，
因此不能將該紀錄解讀為等待鎖 1,589 秒。此前第 7、8、9、11 輪收據記錄
48,574,013,440 allocated bytes；這些是先前結果，非本次新回收。

## 09:00–09:20 後續：第 10 輪完成、C 實體空間與可回收暫存

第 10 輪原 owner 的 retirement 收據記錄已回收 **11,910,537,216 allocated
bytes（11.09 GiB）**，其 acceptance 已完成；另在 D 回收的編碼暫存
1,880,563,712 bytes 不算入 WSL。第 7、8、9、10、11 輪原退休收據合計
60,484,550,656 allocated bytes（56.33 GiB），並非本次 C 槽淨增加量。
第 6 輪依原 scheduler 每 60 秒重測容量。約 09:17 的計畫需求為 Windows C
38,050,725,888 bytes、觀測可用 28,012,793,856 bytes；planner 的 32 GiB
保留及逐檔控制預算是估算，不是空間鎖，也不能以 WSL 內部可用量替代。
原 quarantine 仍保留，沒有新的第 6 輪退休收據。先前較快的分段 owner
修改已通過測試，仍沒有本輪真實回收驗收，無法據此給完成時間。

R17 的七份完整 builder 輸出先前已逐欄獨立讀回。此次以完成 run 的執行鎖、
原 build／readback 收據、完整檔案 SHA、inode／hardlink 及 process reference
檢查重新稽核，對相同 SHA 的六份輸出先寫入固定清冊，再逐項移至原 WSL
檔案系統內隔離並回收。B7 一份和七份 summary、來源、逐輪建置及讀回
證據保留。隔離測試覆蓋成功回收、檔案變動前置拒絕及 hardlink 拒絕。
正式稽核 34.89 秒、apply 含重新全檔核對 30.50 秒；回收收據記錄
**8,042,176,512 allocated bytes（7.49 GiB）**。另一程序再次完整讀回 B7
SHA、核對六個名稱已消失及七份 summary 都在，耗時 6.39 秒。它們是此次
單機、單次完整工作觀測，不是硬體極限。清冊、專用程式及收據位於本目錄
對應的 `r17-duplicate-outputs-20261030.*` 和
`retire_r17_exact_duplicate_outputs.py`；讀回工作為
`r17-retirement-readback-20261030T011856-e257b928`。沒有刪冷物件或 NAS。

約 09:19 的 WSL ext4 可用 268,661,997,568 bytes，Windows C 可用
27,969,146,880 bytes，D 可用約 381,576,982,528 bytes。手動觸發既有
`fstrim.service`，09:18:46–09:18:51 回報 17,078,620,160 bytes discard，
但前後 C 可用量沒有量得增加；discard 量不是 C 的淨回收量。Windows 原生
`GetCompressedFileSizeW` 同時量得 Ubuntu VHDX 長度與已配置量均為
3,049,160,441,856 bytes，`fsutil sparse queryflag` 回報非 sparse。
這是目前 backing 實測，不證明 ext4 內所有 free blocks 都可安全收縮，
也不授權對線上 VHDX 進行離線 compact 或關閉交易服務。

另有 `/var/tmp/stockagent-packed-metadata-k8est0pn/index.sqlite` 占
7,344,398,336 bytes，其 SQLite 可見已提交結構為空且僅 8,192 bytes；
但是原 lakehouse worker PID 929 仍持有 FD 153 與寫入鎖，安全 plan 明確拒絕
回收。當下 worker 堆疊顯示既有版本活動仍在完整 packed inventory 讀回，
另一活動正在註冊 catalog；Temporal status 為 47／61 版本已驗證、仍在
processing。這不代表整體 NAS 備份完整，也不是結束時間。08:41 曾有
`register_versions` DuckDB 488.2 MiB 限額下的 OOM；目前活躍 owner 未因
清理被中斷，不能把已修改的 source 當作該行程已載入版本。

09:22 再查時，先前的 `k8est0pn` 暫存目錄已不在原路徑，PID 929 也不再
持有其 FD；同一 worker 已持有另一個 `5r6u6s97` 活動索引，當時約 50 MiB。
這是時間點觀測，沒有單獨的回收收據，不能把 6.84 GiB 算成本次確定的
淨空間增加；新的活動索引也不能按舊清冊清理。09:22 的 WSL、C 和 D
可用量分別為 273,920,155,648、27,934,482,432 和 381,576,232,960 bytes。
持續使用原 owner 與原第 6 輪每分鐘重試，不另啟清理程序競爭 worker。

09:23 亦檢查到當沖活動 `opening_source_repair_latency.jsonl` 已有約
3.14 GB（十進位），首筆 session 為 10 月 19 日；抽樣的近期紀錄約每
1–2 秒寫入完整 70 KiB 左右的來源狀態，包含重複的上市櫃清單與來源細節。
檔案仍在寫入，且它保存逐次觀測證據，不能僅因內容重複而截斷或刪除。
未來若調整產生方式或封存舊段，需要保留事件順序、原始值與可驗證的還原；
此次盤中未修改產生器或移動活動檔案。

09:04 的來源 backup ledger 有 1,996 筆 NAS 檔案還原 ACK 及 1 筆等待 ACK；
約 09:18 lab203 接收稽核為 pending 0、沒有接收背壓。這些不同時間和不同
層級的證據不代表所有歷史物件均已恢復。09:17 當沖 guardian 仍報六個
策略當日 signal commit missing；09:12 的 09:01 歷史價格回執記錄 1,047
個 union candidates 受盤中歷史查詢政策延後到 13:35，並非來源已證實無價。
交易服務維持 CPU／I/O 優先，以上觀察不推論 broker 訂單、成交或策略績效。

第 10 輪本次獨立冷庫還原耗時 1,606.9 秒，完整 source／mirror／D 稽核
耗時 1,175.2 秒。00:43 完成稽核後，冷庫 Syncthing 仍在掃描，狀態檢查
其 need bytes／items 為零，但 folder idle 為 false，因此等待同步收斂。
00:44 的掃描事件顯示當時確有一個 107,595,045 bytes 子項正在處理；
該事件不表示整個資料夾完成比例。不能用單個子項速率外推總 ETA。
00:53:45 原 owner 重新檢查到 folder idle 且各傳輸項目為零；第 10 輪
隨後進入 `verify-and-retire-hot-source`；00:55 狀態為 `retiring`，原來源
已移至該交易 quarantine，且尚無 retirement 或 acceptance 收據，因此仍
不得計為已回收。該既有程序仍載入舊流程與原證明時效。

已將第 6 輪以原 scheduler 和 guarded-D owner 重新排入隔離區恢復；第 10 輪
另有原 scheduler 的受保護後續。兩者均重新檢查固定版本、容量與完整 gate；
排隊不代表完成。第 10 輪現有執行程序已載入舊程式，不中斷目前的交易。
00:37 的只讀預檢顯示第 6 輪 Windows C 需求估算為 38,050,725,888 bytes，
當時實際可用 41,451,212,800 bytes；這是非保留的瞬時觀測，執行前須重查。
WSL 虛擬空閒空間位於 C，不可與 C 的實體餘量加總。
00:56:43 再取樣時，WSL 可用 261,732,151,296 bytes、Windows C 可用
40,826,499,072 bytes、D 可用 374,035,558,400 bytes。以既有 planner 的
兩輪檔案控制預算、固定 D 最大單物件暫存及 32 GiB floor 加總，並行兩輪
的 C 需求估算為 40,879,994,366 bytes，高於當時 C 實際可用；此預算是
heuristic 而非硬性保留或全程峰值證明，因此不繞過現行單 owner 去並行。
00:58 的 C 可用量回升到 41,859,788,800 bytes，比同一估算多
979,794,434 bytes；這也不是保留的空間，且同一時間其他程序持續寫入。

## 修正與驗證

只針對明確的 guarded-D 手動退休模式，現有完整稽核與原所有 preflight
通過後，先由既有 owner 移動精確來源到其 journaled quarantine，釋放共用
mutation lock。原 cohort owner 接著在鎖外進行原有 packed、archive 和逐檔
原值完整 byte 稽核，再以同程序的 v2 quarantine resume 契約重取共用鎖，
重新檢查固定版本、全部檔案 generation、冷庫、process／service／pin／
lease 及 transport，通過後才 unlink。中斷、變動或失敗仍保留 quarantine。
原 1,500 秒證明時效不延長，也不從 metadata 重新計時；一般自動退休路徑
未變。這避免完整 byte 稽核占用共用鎖並讓它自己的初始證明過期，但尚未
以真實大輪次證明總耗時或回收完成。

測試：退休與隔離相關 focused 測試 32 通過；新的分段流程、來源及冷檔
改動、pin、等待、重試與 wave 排程另一組 43 通過。測試使用隔離 fixture，
不代表任何真實 cohort 已退休。程式及測試的 `git diff --check` 通過。
兩組測試結束後，僅清除此輪在 `/var/tmp` 建立的兩個固定 fixture 根；先
確認無程序引用及外部 hardlink，符號連結不追蹤目標，精確檔案占用合計
5,279,744 bytes。收據為 `test-fixture-cleanup-20261030.private.json`；沒有
刪除正式來源或將瞬間 filesystem 餘量變化當成此數字。

另對含絕對、相對、失效及非 UTF-8 目標的四個符號連結，實測新的
隔離後完整稽核、原路徑冷還原，以及隔離後連結被變更時保留資料；連同
原退休／還原路徑共 4 個 focused 測試通過，完整符號連結測試檔則為 19
個通過。這些測試另外建立的兩個固定 fixture 根
在相同的程序引用與 hardlink 檢查後清除，檔案占用為 462,848 bytes，
及 2,904,064 bytes，收據分別為 `test-symlink-fixture-cleanup-20261030.private.json`
及 `test-symlink-suite-cleanup-20261030.private.json`。本次測試 fixture
回收合計 8,646,656 allocated bytes，仍非正式 cohort 回收。

對第 10 輪固定 D encoding stage 的 512 個檔案做只讀 stat A/B/B/A，4／8／
8／4 workers 為 0.826／0.608／0.701／0.941 秒；8／16／16／8 為
1.021／0.890／0.839／1.194 秒，四次檔案 signature 一致。這只測當時的
少量 metadata、未控制快取及其他 I/O，不是完整稽核、原值解碼或硬體極限
的測試；目前不因此調整正式 worker 數量。私有聚合收據在
`artifacts/operations/storage-single-authority-20261025/d-stage-stat-sample-abba-20261030.private.json`
及同目錄 `d-stage-stat-8v16-abba-20261030.private.json`。

Penguin 三個固定控制包的 envelope identity、dispatch identity、檔案
SHA 和配對來源 ledger 均已對照 NAS 回執：00:37 分別完成 4 個檔案的
獨立讀回 ACK，範圍是個別封閉交付，不代表歷史缺件或完整 runtime 已恢復。
00:54:11 的來源 ledger 顯示 1,935 筆有 NAS 讀回 ACK、30 筆等待 ACK，
後者共 6,748,537,027 bytes，其中有 2 筆**其他** code/control、4 筆
新聞、10 筆私有不可變冷庫與 14 筆冷庫增量物件派工；這是該 ledger 的
時間點，並非全資料範圍的完整性證明。
00:50 接收端稽核的傳輸佇列為零且沒有接收背壓，這與逐批 NAS 獨立讀回
是不同證據。00:52 當沖模擬程序仍 active，CPU／I/O weight 均為 10000、
nice 為 -5，CPU、記憶體、swap 和 tasks 無 cgroup 上限；revision、summary
及 status 可讀，但 guardian 為 degraded：一筆 TWSE 資料集的最近探測失敗、
磁碟位於預警區。00:53 對同一 TWSE URL 的一次無條件 GET 回 HTTP 200 且
JSON 有 4 筆，帶既有 validator 的一次 GET 回 HTTP 304；兩者僅為當次
觀察，沒有覆寫 watcher 的失敗狀態或宣稱盤前驗收成功。00:55 guardian
曾因 Windows 時鐘橋接握手逾時標記時間未驗證；00:56 下次 guardian 採樣
已回復時間檢查，獨立使用既有不修時檢查得到 WSL 對 Windows 約 0.08 ms
偏差。00:57 watcher 對原失敗資料集的正常排程重試成功，來源事件監測
失敗數回到零；00:58 guardian 只剩磁碟早期預警，時間檢查正常。這不
代表 chrony 自己已有同步來源，也不是盤前驗收。
