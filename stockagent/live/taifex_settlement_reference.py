"""Read-only, explicitly incomplete intraday TXO settlement reference."""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import math
from pathlib import Path
import threading
import time as time_module
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo
import json

TAIPEI = ZoneInfo("Asia/Taipei")
INDEX_URL = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_t00.tw&json=1&delay=0"
INDEX_REPORT_URL = "https://www.twse.com.tw/exchangeReport/MI_5MINS_INDEX?response=json&date="


class SettlementReference:
    """Sample official index disclosures without claiming a complete tape."""

    def __init__(self, final_path: Path, partial_snapshot_path: Path | None = None) -> None:
        self.final_path = final_path
        self.lock = threading.Lock()
        self.latest: tuple[datetime, float] | None = None
        self.samples: dict[datetime, float] = {}
        self.report_date = None
        self.report_samples: dict[datetime, float] = {}
        self.closing_index: tuple[datetime, float] | None = None
        self.started_at = datetime.now(timezone.utc)
        self.failure: str | None = None
        self.retained_partial: dict | None = None
        if partial_snapshot_path is not None:
            try:
                saved = json.loads(partial_snapshot_path.read_text(encoding="utf-8"))
                first = datetime.fromisoformat(saved["sample_first_at"])
                last = datetime.fromisoformat(saved["sample_last_at"])
                count = int(saved["sample_count"])
                average = float(saved["partial_average_price"])
                now = datetime.now(TAIPEI)
                if (now.time() >= time(13, 25) and first.tzinfo is not None
                        and last.tzinfo is not None
                        and first.astimezone(TAIPEI).date() == now.date()
                        and last.astimezone(TAIPEI).date() == now.date()
                        and time(13, 0) < first.astimezone(TAIPEI).time()
                        <= last.astimezone(TAIPEI).time() <= time(13, 25)
                        and count > 0 and math.isfinite(average) and average > 0
                        and saved.get("closing_index_included") is not True):
                    self.retained_partial = saved
            except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
                pass

    def observe(self, payload: dict, now: datetime) -> None:
        rows = payload.get("msgArray") or []
        row = next((r for r in rows if r.get("ch") == "t00.tw"), None)
        if row is None:
            raise ValueError("missing TAIEX observation")
        ts = datetime.strptime(f"{row['d']} {row['t']}", "%Y%m%d %H:%M:%S").replace(tzinfo=TAIPEI)
        value = float(row["z"])
        if not math.isfinite(value) or value <= 0 or ts > now.astimezone(TAIPEI):
            raise ValueError("invalid index observation")
        with self.lock:
            if self.latest is None or ts >= self.latest[0]:
                self.latest = (ts, value)
            if time(13, 0) < ts.time() <= time(13, 25):
                self.samples[ts] = value
            self.samples = {k: v for k, v in self.samples.items() if k.date() == ts.date()}
            self.failure = None

    def observe_close_report(self, payload: dict, now: datetime) -> None:
        """Validate the exchange's regular-session 5-second report, including close.

        A 13:30 quote alone does not establish the final index after a delayed
        auction. The exchange report's final row is used only once published;
        partial or non-standard sessions stay explicitly unverified.
        """
        local_now = now.astimezone(TAIPEI)
        if local_now.time() < time(13, 35) or payload.get("stat") != "OK":
            raise ValueError("official post-close report not ready")
        if payload.get("date") != local_now.strftime("%Y%m%d"):
            raise ValueError("official report date differs from today")
        fields = payload.get("fields") or []
        if "時間" not in fields or "發行量加權股價指數" not in fields:
            raise ValueError("official report missing index columns")
        time_col, price_col = fields.index("時間"), fields.index("發行量加權股價指數")
        rows: list[tuple[datetime, float]] = []
        for row in payload.get("data") or []:
            ts = datetime.combine(
                local_now.date(), datetime.strptime(str(row[time_col]), "%H:%M:%S").time(), TAIPEI
            )
            value = float(str(row[price_col]).replace(",", ""))
            if not math.isfinite(value) or value <= 0 or ts > local_now:
                raise ValueError("invalid official index report observation")
            rows.append((ts, value))
        if not rows or len({ts for ts, _ in rows}) != len(rows):
            raise ValueError("empty or duplicate official report timestamps")
        if rows != sorted(rows):
            raise ValueError("official report timestamps are not ordered")
        closing = rows[-1]
        if closing[0].time() < time(13, 30):
            raise ValueError("official report has no closing index")
        selected = {ts: value for ts, value in rows if time(13, 0) < ts.time() <= time(13, 25)}
        first = datetime.combine(local_now.date(), time(13, 0, 5), TAIPEI)
        expected = {first + timedelta(seconds=5 * i) for i in range(300)}
        if set(selected) != expected:
            raise ValueError("official report is not a complete regular 5-second grid")
        with self.lock:
            self.report_date = local_now.date()
            self.report_samples = selected
            self.closing_index = closing

    def run(self, stop: threading.Event) -> None:
        next_report_attempt = 0.0
        while not stop.is_set():
            now = datetime.now(TAIPEI)
            if now.weekday() < 5 and time(8, 59) <= now.time() <= time(13, 35):
                try:
                    req = Request(INDEX_URL, headers={"User-Agent": "stockAgent-settlement-reference/1"})
                    with urlopen(req, timeout=5) as response:
                        self.observe(json.load(response), datetime.now(TAIPEI))
                except (OSError, ValueError, KeyError, TypeError) as exc:
                    with self.lock:
                        self.failure = type(exc).__name__
            if (now.weekday() < 5 and now.time() >= time(13, 35)
                    and time_module.monotonic() >= next_report_attempt):
                next_report_attempt = time_module.monotonic() + 60
                try:
                    request = Request(INDEX_REPORT_URL + now.strftime("%Y%m%d"),
                                      headers={"User-Agent": "stockAgent-settlement-reference/1"})
                    with urlopen(request, timeout=15) as response:
                        self.observe_close_report(json.load(response), datetime.now(TAIPEI))
                except (OSError, ValueError, KeyError, TypeError, IndexError):
                    pass  # Keep the partial observation and retry on the next interval.
            stop.wait(2.0)

    def snapshot(self, now: datetime | None = None) -> dict:
        now = (now or datetime.now(timezone.utc)).astimezone(TAIPEI)
        with self.lock:
            latest, samples, failure = self.latest, dict(self.samples), self.failure
            report_date, report_samples, closing = self.report_date, dict(self.report_samples), self.closing_index
        today = {k: v for k, v in samples.items() if k.date() == now.date()}
        report_ready = report_date == now.date() and closing is not None
        if report_ready:
            today = report_samples
        age = (now - latest[0]).total_seconds() if latest else None
        fresh = latest is not None and latest[0].date() == now.date() and 0 <= age <= 15
        prices = list(today.values()) + ([closing[1]] if report_ready else [])
        average = sum(prices) / len(prices) if prices else None
        retained = (
            self.retained_partial
            if not report_ready and not today and self.retained_partial is not None
            and datetime.fromisoformat(self.retained_partial["sample_first_at"]).astimezone(TAIPEI).date() == now.date()
            else None
        )
        if retained is not None:
            average = float(retained["partial_average_price"])
        estimate = (
            int(Decimal(str(average)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
            if average is not None else None
        )
        official = None
        try:
            import polars as pl
            frame = pl.read_parquet(self.final_path).filter(pl.col("product") == "TXO").sort("settlement_date")
            if frame.height:
                row = frame.tail(1).row(0, named=True)
                official = {"date": str(row["settlement_date"]), "series": row["option_series"], "official_price": row["final_settlement_price"]}
        except (OSError, ValueError, KeyError):
            pass
        return {
            "contract": "txo_observed_partial_settlement_reference_v1",
            "index_price": latest[1] if latest and latest[0].date() == now.date() else None,
            "index_observed_at": latest[0].isoformat() if latest else None,
            "index_age_seconds": round(age, 1) if age is not None else None,
            "index_state": "fresh" if fresh else "stale_or_unavailable",
            "partial_average_price": average,
            "partial_estimate_price": estimate,
            "sample_count": int(retained["sample_count"]) if retained else len(today),
            "closing_index_price": closing[1] if report_ready else None,
            "closing_index_at": closing[0].isoformat() if report_ready else None,
            "closing_index_included": report_ready,
            "average_sample_count": int(retained["sample_count"]) if retained else len(prices),
            "observation_source": ("twse_postclose_5second_report" if report_ready
                                   else "retained_partial_summary" if retained else "twse_mis_partial_poll"),
            "regular_grid_verified": report_ready,
            "sample_first_at": retained["sample_first_at"] if retained else min(today).isoformat() if today else None,
            "sample_last_at": retained["sample_last_at"] if retained else max(today).isoformat() if today else None,
            "capture_started_at": self.started_at.isoformat(),
            "complete_tape_verified": False,
            "official": official,
            "source_error": failure,
        }
