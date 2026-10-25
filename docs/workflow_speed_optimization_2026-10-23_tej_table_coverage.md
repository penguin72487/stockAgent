# 工作流測速 Round 14：TEJ 逐表選樣與串流索引

本輪接續 [Round 13](workflow_speed_optimization_2026-10-23_static_inventory.md)，
把既有 producer 的真實計時接回更多來源。已實作 canonical 唯讀 profiler v7，
135 個資料表各一筆最新完成任務、342 筆階段 samples 通過新程序獨立讀回。
對已封存 registry 的 stage timing 聯集由 1,367 增至 1,497，未綁定由 1,032 降至 902。
這是來源測速覆蓋與查詢演算法改善，不是新下載、完整歷史或投資結果。

完整紀錄位於
`artifacts/benchmarks/workflow_speed_optimization_20261021/round14/`。

## 從選樣偏差回到工作量

Round 13 的最近 256 筆任務集中在五個資料表；其他表的既有計時沒有進入清冊。
v7 新增 `--selection latest_per_table`，在同一 read-only transaction 內，選每表
最新一筆符合原有 scope、complete/download、fresh end-to-end 的任務。
相同完成時間以 task ID 降冪決定；NULL 完成時間不補值。
預設仍是 `latest_tasks`，原本全域最近任務的選法保留。

`--limit` 仍為 1..256。逐表模式明確回報 eligible tables、selected tables 與
`selection_truncated`，不把限制內的樣本說成全部來源。
原有準備、Windows flow、native helper cache、prebridge、transport 的 exact task /
attempt / request-SHA 驗證及各自時間邊界保持不變；不存在的細階段計時不填造。

## 實測拒絕兩種排序方案

三個唯讀 SQL admission probes 均保留同一 2 秒期限，不更動資料庫：

| 選法 | 實測，秒 | 結果 |
| --- | ---: | --- |
| GROUP BY / window，SQLite 自選索引 | 2.049345 | deadline 中止 |
| 強制既有 covering index 的 GROUP BY / window | 2.004820 | deadline 中止 |
| 串流 covering index，再查每表入選任務 | 1.253471 | 完成 135 表 |

第一組 query plan 選了不適合此條件的 metadata index；第二組雖已使用 covering
timing index，仍產生 GROUP BY / ORDER BY 的 temp B-tree。
採用方案沿既有完成時間排序掃描 index，只在 Python 字典保留每表第一個有效時點，
再以完整 scope + 完成時間 + table ID 查少量入選任務。
保留原本的 scope、完成時間與 task tie 選取語義，避免大量排序與未入選任務的 payload lookup。
沒有建立索引、執行 ANALYZE、更新 queue、重新送查詢或更動 GUI。

這些 probes 使用持續更新中的唯讀 producer snapshot，前兩組被 timeout，
不是固定 snapshot 的完整 ABBA。不能用 2 秒除以 1.25 秒宣稱完整速度倍率，
也不將不同階段的改善乘到交易／訓練吞吐量。

## Canonical 實跑與獨立核對

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/profile_tej_acquisition.py \
  --root data_tej --limit 256 --selection latest_per_table \
  --output artifacts/benchmarks/workflow_speed_optimization_20261021/round14/tej_profile_tables.json
```

正式 SQL selection 0.922993 秒，掃描 113,474 個 covered index rows，
eligible/selected 都是 135，沒有截斷。完整 owner command 1.353078 秒，含 imports、
SQL、可用的既有階段檔案讀取與報告輸出。
135 個任務中 64 個非空、71 個空回，保留 109,751 個 exported rows 的原始計數。
它們涵蓋不同資料表、欄位、公司與日期範圍，不拿其中位數與前輪不同 cohort 比速度。

`bind_readback.py` 重用既有 `collect_tej` / `bind_source_stages`，以 Round 13 的
已封存 inventory 為基礎產生 delta。新程序獨立驗證每份輸出 SHA、原始 profile gzip、
135 個 task ID 在唯讀 DB 的 table/attempt/完成時間/seconds/rows/scope/state/kind/basis，
以及每筆來源 ID 與 summary regrouping；`binding_readback.json` 為 verified。

135 個表得到 135 筆完整任務時間，另有 207 筆可用細階段，共 342 筆 samples。
其餘細階段沒有歷史紀錄，保持未知。原有五個表仍在新選樣內，新增精確對應 130 個表。
TEJ registry 尚有 120 項沒有符合本次選取契約的計時樣本，不能推成未曾下載或零成本。

全 registry 聯集為 1,497/2,399 有 stage timing，902 項未綁；分母仍包括 credential、
group、product 與 inventory views，不是 2,399 個可執行 API。
完整逐來源 E2E acceptance 仍為 0；一個表的一筆任務不能證明其原生歷史完整。
這是接續 Round 13 的 delta 讀回，沒有再收集整份 13 階段清冊，亦不冒稱已重測全部流程。

兩個受影響測試模組 167 項通過，包括舊表覆蓋、相同時點的 task tie、來源 scope /
timing basis 過濾、明確截斷、原預設與 invalid selection 不進行 I/O。
七個相關來源／測試檔案與全部正負 probes、正式 profile、獨立讀回保留於本輪 artifact。

下一步仍是完整清冊最大成本 inspection，以及其餘 producer 的 exact ID、計時、
工作量與接受證據。優化 read/stat/hash 或 query 時不得刪去來源、驗證、費用、
模型特徵、curves、plots 或 checkpoint。
`all_workflows_measured=false`、`hardware_limit_proven=false` 持續保留。
