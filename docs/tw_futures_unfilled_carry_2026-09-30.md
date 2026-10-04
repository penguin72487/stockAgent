# 期貨未成交平倉的延續與帳戶計算

2026-09-30 使用者指定：當沖未全部沖銷，剩餘部位隔日繼續，不增設處罰。
目前已修正剛才出錯的保證金留倉帳本；全商品歷史規則准入仍是獨立的未完成工作。

## 已確認的錯誤

舊帳本把風控平倉單因容量不足而留下的部位，直接列為違約，清除次日帳戶。
訓練反向傳播及報表檢查也有相同假設。這不等於真實虧損，更不等於未成交部位消失。

舊 2,816 槽位 TX／MTX、fold 10、3-epoch 控制測試在 2024-08-06 留下
43 口未成交風控減倉，套用歸零規則前的帳戶權益為 161,224,864 元。
舊結果與失敗收據保留，不覆寫成成功。

## 本次執行規則

- 風控減倉只能按真實可用容量成交；未成交口數留在原實體契約。
- 剩餘部位繼續盯市、占用適用保證金，次日重新檢查保證金及減倉條件。
- 費用及交易稅依實際成交計算；未成交本身沒有額外處罰，也沒有額外梯度懲罰。
- 真實權益不大於零仍終止帳戶；到期交割、未驗證資料、法定部位限制另依原契約檢查。
- 帳戶版本改為 `6`、梯度版本改為 `10`；新輸出目錄拒絕舊 optimizer／checkpoint resume。

期交所當沖減收保證金作業指出：收盤後若權益足以負擔一般原始保證金，
未沖銷部位無須代為沖銷；不足時需依期限補足，否則次日繼續代沖銷。
本次沒有新增當沖保證金折扣，也沒有把 2022 年公告回填成全部歷史規則。
[官方作業說明，第 7～11 頁](https://www.taifex.com.tw/chinese/11/attach/111年7月25日台期結字第1110002405號.pdf)。

## 驗證範圍

新增長／短方向、零成交／部分成交、隔日損益及續平倉、跨 batch 帳本一致、
真實破產不復活、報表不得抹除殘留部位、未成交通知不改變相同現金流梯度的測試。
遠端控制資料維持 TX／MTX、1 億元、2,816 槽位，未冒充全部 765 個歷史代碼。

既有 `tw_stock_futures_day_trade_0845_minute` 的 physical-carry 執行器已支援
未沖銷部位隔日續算。舊 `tw_futures_v8_intraday.yaml` 的全商品分鐘 adapter
仍是 flat-only 契約；不能只關掉違約旗標，就假裝其缺少的隔夜估值、舊月續接與
到期證據已齊全。這個 adapter 的全面准入目前仍未完成。

修正後的遠端控制設定：
[`tw_futures_v8_margin_account6_slots2816_regression_fold10_20260930.yaml`](../configs/historical/tw_futures_v17_20261003/markets/tw_futures_v8_margin_account6_slots2816_regression_fold10_20260930.yaml)。
此歷史工程設定在 2026-10-03 連同原始繼承鏈與 SHA-256 歸檔；它依賴舊版程式
ABI，目前程式會拒絕執行。完整範圍見
[archive manifest](../configs/historical/tw_futures_v17_20261003/manifest.json)。

遠端兩張 RTX 5090 已完成 fold 10 的三個 epoch、validation／test、推論、曲線、
checkpoint 與帳戶驗收，合計 286.95 秒。實際訓練決策日期為
2011-01-03～2020-12-31，validation 為 2021，test 為 2022～2026。
本機相關測試 157 項、遠端 158 項通過，另有共享生命週期／梯度測試 54 項通過。
[遠端驗收收據](../artifacts/markets/tw_futures_v8_margin_preparation/remote_account6_slots2816_margin_smoke3_receipt_20260930.json)。
補跑可成交性與 checkpoint 相容性：本機 170 項、遠端 182 項通過。
遠端 `--resume --no-retrain-completed-folds` 已通過，耗時 122.51 秒；
三份 checkpoint 與 epoch curve 的 SHA 均未改變。
[已完成 fold 續跑收據](../artifacts/markets/tw_futures_v8_margin_preparation/account6_slots2816_completed_resume_20260930/receipt.json)。

新舊回測的所有要求部位完全相同，2024-08-06 以前的帳戶紀錄也完全相同。
該日兩者皆有 43 口減倉未成交、權益 161,224,864 元；舊版隨後清成零，
新版保留 43 口，次日計入隔夜損益 891,000 元並繼續按模型目標及資金限制交易。
因此已直接定位這一筆人造違約，並非靠換一組較有利的交易指令隱藏問題。
[逐日對照及 SHA](../artifacts/markets/tw_futures_v8_margin_preparation/account6_unfilled_carry_comparison_20260930.json)。

修正後完整 test 的期末權益為 3,849,388,800 元，但最大回撤仍為 92.02%；
獨立部署區段的期末權益只有 19,906,286 元。兩者期間不同，不能混用或只挑
較佳數字宣稱策略成功。本輪是帳戶／工程驗收，`profitability_validated=false`。

正式全商品訓練狀態仍由
[`active_preparation_scope.json`](../artifacts/markets/tw_futures_v8_margin_preparation/active_preparation_scope.json)
記錄；不能用本次控制測試代替全商品准入。
