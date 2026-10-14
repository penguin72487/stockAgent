# 免費 Cloudflare 公網入口驗收：2026-10-14

`https://penguin72487.dpdns.org/` 已接上 Cloudflare Free zone 與正式 named
Tunnel。新入口使用既有的唯讀公開 gateway，不建立另一份面板或帳本。
這份驗收只涵蓋網站入口；不表示行情、訊號或成交資料問題已修復。

## 入口與責任

```text
訪客 IPv4／IPv6 HTTPS
  → Cloudflare edge／Universal SSL
  → named Tunnel（主機主動連出）
  → http://127.0.0.1:8770
  → 既有公開 DTO、頁面、API 與 SSE
```

DigitalPlat 委派 `brad.ns.cloudflare.com`、`mimi.ns.cloudflare.com`；
Cloudflare zone 狀態為 `active`，apex 是 proxied Tunnel CNAME，沒有寫入
家中 WAN 的 A／AAAA。這個新入口不需要跟著家中 IPv6 prefix 改寫 DNS。
路由器原有的 Dynu DDNS 更新權責保持不變。

沒有變更路由器、WAN 重連排程、Windows Caddy、當沖／隔日沖引擎或帳本。
舊 `penguin72487.ddnsgeek.com` 仍可用，所以家中 IP **尚未完全隱藏**；
應另行確認舊入口使用者與用途後，才關閉直連轉發及公開 DNS。

## 實測證據

時間為台灣時間 2026-10-14 約 02:16–02:22。主要收據在
`artifacts/operations/public-cloudflare-wan-20261014/`，不是公開 HTTP 路徑。

| 驗證 | 結果 | 收據 |
| --- | --- | --- |
| DigitalPlat NS、Cloudflare Free zone／Tunnel／SSL | NS 一致、active／healthy／active；重跑新增操作 0 筆 | `cloudflare-tunnel.json`、`authoritative-readback.json` |
| 外部雙棧 HTTPS | 台灣、日本、新加坡，IPv4／IPv6 共 6／6 為 HTTP 200 且 TLS 驗證成功；恢復測試後再次通過 | `external-dual-stack.json`、`external-dual-stack-after-recovery.json` |
| 公開頁面 | 13／13 為 HTTP 200 | `network-checks.json` |
| 當沖、隔日沖 SSE | 2／2 收到有效 revision，保留 no-store、no-transform | `network-checks.json` |
| HTTP → HTTPS | 301 導向同主機 HTTPS | `network-checks.json` |
| 機密與管理路徑 | `.env`、credentials、admin、metrics 皆 404；OPTIONS 為 405 | `network-checks.json` |
| 真實 Chromium | 1366×768 與 390×844，兩頁共 4／4；無 JS page error、整頁橫向溢出 0，原生更新按鈕不重載 | `browser/acceptance.json`、截圖 |
| 新 connector 異常退出 | systemd 自動重啟，公網連續兩次 HTTP 200；原 gateway PID 不變、舊入口仍 200 | `connector-recovery.json` |
| 設定器與 unit | 17 個聚焦測試通過，systemd unit 語法驗證通過 | `test/test_public_cloudflare_tunnel.py` |

瀏覽器使用明確的 `cpu-2d` profile，沒有宣稱 GPU／WebGL 驗收。
螢幕上原有的當沖資料／執行告警、隔日沖等待資料仍忠實保留。

### 恢復不是零中斷

測試只對新 `stockagent-public-cloudflared.service` 的主程序送 SIGKILL，
沒有重啟其他服務。第二次完整量測：本機 `/ready` 約 4.31 秒，公網首次
HTTP 200 約 5.75 秒，約 6.23 秒完成連續兩次成功讀回。
本機 ready 後仍觀察到一次 502，不能用 ready 代替端到端正常。
第一次過早讀回的失敗收據保留為 `connector-recovery-first-observation.json`。

unit 已 enabled，具有 3 秒自動重啟及不耗盡的啟動重試。這只證明本次程序
退出恢復；本次沒有實際重開 Windows／WSL，也沒有測試 WAN 斷線或主機停電。
單一主機、免費上游與用戶端網路仍不是「任何情況、永不中斷」的保證。

## 安全設定

- Tunnel 只導向既有 loopback 唯讀 gateway，未公開其他私有服務；未知 hostname 為 404。
- Cloudflare 啟用 HTTPS redirect、最低 TLS 1.2、IPv6 與 strict SSL zone 設定；
  Tunnel origin 是本機 HTTP，不將 strict 設定誤稱為 origin HTTPS 憑證驗收。
- 既有 CSP、nosniff、frame deny 與 no-store API 契約保持不變。
- connector 使用 DynamicUser、唯讀系統與 `LoadCredential`；token 檔案為 0600，
  不放在 argv、Git、公開目錄或收據中；metrics 只綁 loopback `127.0.0.1:20241`。
- 安裝來自 Cloudflare 官方簽章 APT repository；本次 cloudflared 為 `2026.10.0`，
  沒有進行全系統或 Python 套件升級，也沒有建立付費訂閱。
- 本次沒有新增全域 rate limit、登入牆或 bot challenge，避免改變既有公眾瀏覽用途。

## 維護指令

憑證留在本機 `.env`，勿貼入對話或命令列。
設定器預設只讀；`--apply` 才配置本次明確指定的 hostname。
遇到不同的 DNS／Tunnel 設定會拒絕覆寫，不自動重送結果不明的 mutation。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/configure_public_cloudflare_tunnel.py \
  --hostname penguin72487.dpdns.org \
  --receipt artifacts/operations/public-cloudflare-wan-20261014/cloudflare-tunnel.json

run_fintech_python -m pytest -q test/test_public_cloudflare_tunnel.py
systemd-analyze verify deploy/systemd/stockagent-public-cloudflared.service
systemctl status stockagent-public-cloudflared.service --no-pager
curl --fail http://127.0.0.1:20241/ready

run_fintech_python scripts/audit_public_ipv6.py penguin72487.dpdns.org \
  --provider globalping --url-path /healthz --timeout-seconds 45 \
  --output artifacts/operations/public-cloudflare-wan-20261014/external-dual-stack-recheck.json
```

首次安裝的 unit 來源是 `deploy/systemd/stockagent-public-cloudflared.service`；
安裝至 `/etc/systemd/system/` 後 daemon-reload 並 enable。恢復時不能只複製
unit：還需要正式 cloudflared binary 與 root-private Tunnel token。
不可把 token 檔內容輸出到一般維運 log。

## 後續邊界

1. 完全隱藏 origin 前，需要另行確認並關閉舊 DDNS／直連入口；本次刻意保留退路。
2. Windows 開機到 WSL／gateway／Tunnel 的完整冷啟動需要獨立驗收。
3. 每日 WAN 重連仍是另一項作業，沒有因為綁定網域而執行。
4. Cloudflare 邊緣雙棧不代表家中本身多線備援；主機或 ISP 停止服務仍會中斷。
5. 免費網域的續用、到期與供應商條件應定期確認；不能把免費服務當作沒有 SLA 風險。

官方參考：[Tunnel 設定](https://developers.cloudflare.com/tunnel/get-started/)、
[執行參數](https://developers.cloudflare.com/tunnel/reference/run-parameters/)、
[套件來源](https://pkg.cloudflare.com/)、
[DigitalPlat NS 委派](https://github.com/DigitalPlatDev/FreeDomain/blob/main/documents/tutorial/platform/1.3-connect-nameservers.md)。
