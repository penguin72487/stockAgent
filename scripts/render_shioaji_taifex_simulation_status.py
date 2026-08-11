#!/usr/bin/env python3
"""Render the current Shioaji TAIFEX simulation state as Markdown."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
from typing import Any


DEFAULT_STATE_DIR = Path("artifacts/live/shioaji_taifex_volatility_simulation")
DEFAULT_API_RECEIPTS = Path("artifacts/orders/shioaji_futures_simulation")


def _json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root is not an object: {path}")
    return payload


def _line_count(path: Path) -> int:
    if not path.is_file():
        return 0
    with path.open("rb") as handle:
        return sum(1 for _line in handle)


def _service_state() -> dict[str, str]:
    command = [
        "systemctl",
        "show",
        "stockagent-shioaji-taifex-bidask.service",
        "-p",
        "ActiveState",
        "-p",
        "SubState",
        "-p",
        "UnitFileState",
        "-p",
        "MainPID",
        "-p",
        "NRestarts",
        "--no-pager",
    ]
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        return {"error": completed.stderr.strip() or f"exit={completed.returncode}"}
    return {
        key: value
        for line in completed.stdout.splitlines()
        if "=" in line
        for key, value in [line.split("=", 1)]
    }


def _latest_receipt(root: Path) -> tuple[Path | None, dict[str, Any] | None]:
    paths = sorted(root.glob("*-futures-simulation-lifecycle.json"))
    if not paths:
        return None, None
    path = paths[-1]
    return path, _json(path)


def render(*, state_dir: Path, api_receipt_dir: Path, output: Path) -> Path:
    status_path = state_dir / "status.json"
    state_path = state_dir / "state.json"
    status = _json(status_path)
    state = _json(state_path)
    service = _service_state()
    receipt_path, receipt = _latest_receipt(api_receipt_dir)
    cycle = status.get("active_cycle")
    pending = status.get("pending_targets") or {}
    rows = []
    for strategy_id, mark in status.get("strategies", {}).items():
        rows.append(
            "| {strategy} | {equity:,.0f} | {position} | {fees:,.0f} | {tax:,.0f} | {valid} |".format(
                strategy=strategy_id,
                equity=float(mark.get("net_equity_twd", 0.0)),
                position=int(mark.get("futures_position", 0)),
                fees=float(mark.get("fixed_fees_twd", 0.0)),
                tax=float(mark.get("transaction_tax_twd", 0.0)),
                valid=(
                    "yes"
                    if mark.get("option_books_valid") and mark.get("future_book_valid")
                    else "no"
                ),
            )
        )
    service_text = "/".join(
        filter(None, (service.get("ActiveState"), service.get("SubState")))
    ) or service.get("error", "unknown")
    api_text = "尚無 receipt"
    if receipt is not None:
        api_text = (
            f"{receipt.get('result')}；{receipt.get('logical_contract')} → "
            f"{receipt.get('resolved_contract')}；baseline/final position "
            f"{receipt.get('baseline_position')} / {receipt.get('final_position')}"
        )
    lines = [
        "# 永豐 TAIFEX 七策略模擬交易即時狀態",
        "",
        f"- 產生時間：{datetime.now().astimezone().isoformat(timespec='seconds')}",
        f"- systemd：`{service_text}`，enabled=`{service.get('UnitFileState', 'unknown')}`，PID=`{service.get('MainPID', 'unknown')}`，restarts=`{service.get('NRestarts', 'unknown')}`",
        f"- 引擎：`{status.get('engine_status')}`；blocked=`{status.get('blocked_reason')}`",
        f"- 安全邊界：simulation_only=`{status.get('simulation_only')}`；production_order_possible=`{status.get('production_order_possible')}`",
        f"- Bootstrap：只在 `{status.get('bootstrap_after_date')}` 結算後開啟新的完整週期，不補建已開始的舊週期。",
        f"- 合約／行情：TX=`{status.get('underlying_contract')}`、MTX=`{status.get('hedge_contract')}`、option contracts={status.get('option_contract_count')}、latest books={status.get('latest_book_count')}",
        f"- Active cycle：`{cycle if cycle is not None else 'flat'}`；pending targets={len(pending)}",
        f"- 永豐期貨 API round-trip：{api_text}",
        "",
        "## 七個獨立理想可成交價帳本",
        "",
        "| 策略 | 即時淨值 TWD | MTX 部位 | 累計手續費 | 累計稅 | Book 有效 |",
        "|---|---:|---:|---:|---:|:---:|",
        *rows,
        "",
        "理想帳使用收到當下的一口對手價：買進用 best ask、賣出用 best bid。永豐模擬帳戶會把七策略委託淨額合併，因此 broker callback 只做下單／成交／對帳證據，不拿來拆分各策略 P&L。",
        "",
        "## 紀錄檔",
        "",
        f"- `status.json`：即時快照（每 5 秒更新）",
        f"- `state.json`：可重啟狀態與 inflight order gate",
        f"- `ideal_ledger.jsonl`：{_line_count(state_dir / 'ideal_ledger.jsonl'):,} 筆理想 Bid/Ask 成交帳",
        f"- `marks.jsonl`：{_line_count(state_dir / 'marks.jsonl'):,} 筆每分鐘七策略 mark",
        f"- `calibrations.jsonl`：{_line_count(state_dir / 'calibrations.jsonl'):,} 筆模型校準與下一日 MTX target",
        f"- `events.jsonl`：{_line_count(state_dir / 'events.jsonl'):,} 筆決策、委託、成交與錯誤事件",
        f"- 期貨 API receipt：`{receipt_path if receipt_path else 'missing'}`",
        "",
        f"State schema={state.get('schema_version')}；strategy count={len(state.get('strategy_ids', []))}。",
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.replace(temporary, output)
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE_DIR)
    parser.add_argument("--api-receipt-dir", type=Path, default=DEFAULT_API_RECEIPTS)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()
    output = args.output or (args.state_dir / "live_status.md")
    rendered = render(
        state_dir=args.state_dir,
        api_receipt_dir=args.api_receipt_dir,
        output=output,
    )
    print(rendered, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
