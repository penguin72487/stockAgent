# 台股純日頻日當沖、T+2 收盤清算、無違約訓練契約

正式設定為 `configs/markets/tw_day_trade_daily_no_default.yaml`。它從
2014-01-01 建立日頻 panel；第一個具有點時正確買進先資格且可執行的交易日
是 2014-01-06，賣出先資格自 2014-06-30 起才納入。模型每天只決策一次，
完全不載入分鐘 K，也沒有 13:20--13:30 的分鐘訂單狀態機。

## 同一套訓練 loss 與回測 forward

每個交易日以當日開盤時已知資訊產生 signed target weight，損益標籤是同日
`open[t] -> close[t]`。執行器先套用當日可交易、買進先／賣出先、開盤買賣
方向與收盤方向遮罩，再以日成交量預算的一半限制進場；一趟日當沖會在同一
日使用相同股數進、出場，因此容量預算不能在兩腿各用一次完整日量。

正常完成沖銷時收一般手續費與日當沖賣出稅。如果收盤方向受限但正式收盤價
存在，研究假設改列融資或融券一定成功且容量無限，仍在該日收盤價做會計結
清，只改收一般賣出稅費、融資或融券壓力成本。股票部位不跨日；但沖銷後含
費稅的淨損益會進入 T+2 應收／應付佇列。缺正式收盤價時該檔交易 fail
closed，不用 stale mark。

經濟 NAV 與可下單現金分開：T 日 loss 立即承認損益，但該淨差額在 T+1
仍待結算，T+2 開盤也不可使用；執行器在 T+2 當日訂單完成後才更新現金，
所以 T+3 開盤才第一次影響新部位 sizing。佇列只放淨差額，不把買進與賣出
總額各記一次；會計恆等式為

`economic_equity = cash + receivables - payables`。

多空組合先套權限與容量，再只縮減成交較好的那一側以保存模型要求的 long/
short gross mix；任一要求方向完全沒有可執行量時，整個雙邊組合 fail closed
為空倉。forward 的日損益、費稅、cash/claim queue 與 equity recurrence
就是 `log_utility` loss 使用的值，沒有額外代理報酬公式。跨 batch 會傳遞
`final_cash`、`final_payables`、`final_receivables` 與
`final_equity_scale`，不會在每個 batch 重設交割。

## 每日清算順序

1. T 日開盤先用「當下已入帳現金」換算模型 target；待收獲利不算購買力。
2. 套用點時正確的當沖資格、開盤買賣權限與先賣後買資格。
3. 用前一完整交易日成交股數、以 T 日開盤價重估容量；設定要求整趟進出兩腿
   合計最多使用前日量的 50%，因此進場股數上限實際是前日量的 25%。
4. 用 `open[t] -> close[t]` 日頻標籤結算正常反向沖銷；日頻版沒有分鐘內
   掛單、滑價、排隊順位或整張取整。
5. 收盤腿受限時，殘部以正式收盤價改列無限融資／融券並補收壓力成本，然後
   股票庫存歸零；不留下隔夜股票，也不進追繳／違約狀態。
6. 計算含手續費、稅、折讓與融資融券成本後的單一淨 claim，T 日立即計入
   economic NAV。
7. 當天訂單全數完成後，才支付／收取佇列最前端的 T+2 claim，再把 T 日
   claim 放到佇列尾端；因此新現金只能從下一交易日 T+3 開盤使用。

## 成本壓力設定

- 券商牌告手續費率 0.1425%，假設二折，故每一買賣腿最終有效率為
  0.0285%。執行器先收 0.1425%，再於當日損益沖回 80% 手續費折讓；交易稅
  不打折。整筆淨損益仍依 T+2 queue 入帳。不模擬最低手續費與整數
  元四捨五入。
- 多單未正常沖銷：融資成數 60%，年率 16% 計一天。16% 是研究壓力值，
  不是交易所規定的全市場最高利率。
- 空單未正常回補：一次融券手續費 0.10%，融券費以年率 20% 計一天，並補
  一般融券與當沖賣出稅費的差額。
- `tw_short_capacity_limit_enabled: false` 代表依使用者假設券源無上限；仍保
  留當日的點時正確賣出先資格，不能把今天的可放空名單回填至歷史。

## 正式訓練

```bash
cd /path/to/stockAgent
source scripts/runtime_env.sh
CUDA_VISIBLE_DEVICES=0,1 STOCKAGENT_STRICT_NO_FALLBACK=1 \
  run_fintech_python train.py \
  --config configs/markets/tw_day_trade_daily_no_default.yaml
```

設定保留 1,000 epochs、BF16 AMP、雙 RTX 5090 DDP、每 fold 獨立新程序與可
恢復 artifact。輸出在
`artifacts/markets/tw_day_trade_daily_tplus2_close_commission20_v3`；報表標籤是
`daily-tplus2-close`，不能標成 `exact-minute`。canonical backtest contract
已升為 v17，舊的 no-settlement optimizer/checkpoint 不可直接續訓。

完整多基底版本使用完全相同的日頻成交、二折費率、T+2 收盤清算與 T+3
可用現金，只在 FinancialTransformer 的普通輸入層加入 18 組基底、每組
4 個分量。其設定與指令為：

```bash
cd /path/to/stockAgent
source scripts/runtime_env.sh
CUDA_VISIBLE_DEVICES=0,1 STOCKAGENT_STRICT_NO_FALLBACK=1 \
  run_fintech_python train.py \
  --config configs/markets/tw_day_trade_daily_multi_basis_tplus2_close.yaml
```

多基底輸出位於
`artifacts/markets/tw_day_trade_daily_multi_basis_tplus2_close_commission20_v1`。

## 法規與研究假設邊界

- [TWSE 當日沖銷交易專區](https://www.twse.com.tw/zh/products/system/day-trading.html)
- [TWSE 款券交割作業](https://www.twse.com.tw/zh/clearing/clearing/operations.html)
- [TPEx 當日沖銷制度](https://www.tpex.org.tw/zh-tw/mainboard/trading/day-trading/rules.html)

無限融資與券源是「收盤殘部改列」的刻意研究假設，不是券商履約保證。交割
法規仍是 T+2；「T+3 才可用」是本模型把入帳放在 T+2 當日下單之後所得到
的保守決策時序。這個契約適合比較日頻訊號在保守成本下的結果，不適合宣稱
已重播真實委託簿、分鐘成交、追繳或隔夜風險。
