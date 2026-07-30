# 股票橫截面驅動的台指期日盤策略

## 交易契約

這個策略把台股橫截面視為市場狀態，不直接交易個股。模型每天只輸出一個
`[-1, 1]` 的台指方向曝險；執行器再用同方向的 TX、MTX、TMF 整數口數逼近
目標名目本金。三種商品的乘數分別為每點新台幣 200、50、10 元。

預設時序如下：

1. 期貨交易日 `t` 的 08:45，只讀取截至股票交易日 `t-1` 收盤已完成的資料。
2. 以期貨日盤開盤價建立部位。
3. 同一日盤收盤前全部平倉，不承擔夜盤曝險。

模型不讀取交易日 `t` 的股票開盤、最高、最低、收盤或成交量。期貨日盤的
開盤價、可交易狀態與實際到期月份也只屬於執行器，不是模型特徵。

## 資料

下載器只接受臺灣期貨交易所官方 `futDataDown` 收據，保留原始 ZIP/CSV、
SHA-256 與 manifest，然後：

- 只保留 `一般` 交易時段；
- 只保留 TX、MTX、TMF；
- 排除週選式月份與跨月價差；
- 每日每商品選擇最近的未到期月契約；
- 產出 `data_tw_index_futures/day_session_front_month.parquet`。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/download_tw_index_futures_day_session.py \
  --start-year 2005
```

## 訓練與評估

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python scripts/train_tw_index_futures_day.py \
  --config configs/markets/tw_index_futures_day.yaml
```

訓練目標使用連續曝險的費後 log utility；測試輸出則使用 TX/MTX/TMF 實際
乘數、整數口數、期交稅、交易/結算費與設定中的券商手續費、滑價重算。
`broker_fee_per_side_twd` 與 `slippage_points_per_side` 的預設值為零，只是因為
它們屬於帳戶條件；正式解讀前必須改成實際值。

這條路徑目前只建立研究、訓練與整數執行預覽，不會送出 Shioaji 正式委託。
未來接實盤時，商品月份必須由 Contract V2 查詢具體契約，不得自行拼接代碼。

