#!/usr/bin/env python3
"""ABBA responsive-row microbenchmark in an explicit, private CPU-2D browser.

Measures DOM update through the MutationObserver checkpoint, not API latency,
paint, GPU/WebGL, real financial data or overall dashboard acceptance. Both
implementations receive the same complete synthetic table and changes.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.probe_browser_runtime import browser_profile_launch_options  # noqa: E402

SOURCE = ROOT / "services/public_dashboards/dashboard-core.js"
SOURCE_PATH = "services/public_dashboards/dashboard-core.js"


def _positive(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def measure(browser, source: str, *, rows: int, columns: int, updates: int) -> dict:
    page = browser.new_page(viewport={"width": 1366, "height": 768})
    try:
        page.set_default_timeout(10_000)
        page.set_content('<div class="table-wrap"><table><thead><tr></tr></thead><tbody></tbody></table></div>')
        page.evaluate("""({rows, columns}) => {
          document.querySelector('thead tr').innerHTML = Array.from({length: columns},
            (_, column) => `<th>Column ${column}</th>`).join('');
          document.querySelector('tbody').innerHTML = Array.from({length: rows},
            (_, row) => `<tr>${Array.from({length: columns},
              (_, column) => `<td>${row}:${column}</td>`).join('')}</tr>`).join('');
          window.labelWrites = 0;
          const set = Element.prototype.setAttribute;
          Element.prototype.setAttribute = function(name, value) {
            if (name === 'data-label') window.labelWrites++;
            return set.call(this, name, value);
          };
        }""", {"rows": rows, "columns": columns})
        page.add_script_tag(content=source)
        result = page.evaluate("""async ({rows, columns, updates}) => {
          await Promise.resolve();
          const initialWrites = window.labelWrites;
          window.labelWrites = 0;
          const samples = [];
          for (let index = 0; index < updates; index++) {
            const row = document.querySelector('tbody').rows[index % rows];
            const cell = row.cells[index % columns];
            const start = performance.now();
            cell.textContent = `update:${index}`;
            await Promise.resolve();
            samples.push(performance.now() - start);
          }
          const cells = [...document.querySelectorAll('tbody td')];
          const labelsCorrect = cells.every((cell, index) =>
            cell.getAttribute('data-label') === `Column ${index % columns}`);
          return {samples_ms: samples, initial_label_writes: initialWrites,
            update_label_writes: window.labelWrites,
            rows: document.querySelector('tbody').rows.length, cells: cells.length,
            labels_correct: labelsCorrect,
            output: cells.map(cell => [cell.textContent, cell.getAttribute('data-label')])};
        }""", {"rows": rows, "columns": columns, "updates": updates})
        output = result.pop("output")
        result["output_sha256"] = hashlib.sha256(json.dumps(output, ensure_ascii=False).encode()).hexdigest()
        result["median_ms"] = statistics.median(result["samples_ms"])
        result["maximum_ms"] = max(result["samples_ms"])
        return result
    finally:
        page.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-ref", default="HEAD")
    parser.add_argument("--rows", type=_positive, default=1000)
    parser.add_argument("--columns", type=_positive, default=8)
    parser.add_argument("--updates", type=_positive, default=50)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    baseline_commit = subprocess.run(["git", "rev-parse", "--verify", args.baseline_ref + "^{commit}"],
        cwd=ROOT, check=True, capture_output=True, text=True).stdout.strip()
    baseline = subprocess.run(["git", "show", baseline_commit + ":" + SOURCE_PATH],
        cwd=ROOT, check=True, capture_output=True, text=True).stdout
    candidate = SOURCE.read_text()
    sources = {"baseline": baseline, "candidate": candidate}
    trials = []
    from playwright.sync_api import sync_playwright

    launch = browser_profile_launch_options("cpu-2d")
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(**launch, timeout=10_000)
        try:
            browser_version = browser.version
            for variant in ("baseline", "candidate", "candidate", "baseline"):
                trials.append({"variant": variant, **measure(browser, sources[variant],
                    rows=args.rows, columns=args.columns, updates=args.updates)})
        finally:
            browser.close()
    code_stable = SOURCE.read_text() == candidate
    complete = all(row["labels_correct"] and row["rows"] == args.rows
                   and row["cells"] == args.rows * args.columns for row in trials)
    parity = len({row["output_sha256"] for row in trials}) == 1
    accepted = code_stable and complete and parity
    result = {"schema_version": 1, "observed_at_utc": datetime.now(UTC).isoformat(),
        "accepted": accepted, "scope": __doc__, "browser": browser_version,
        "browser_profile": "cpu-2d", "launch": launch,
        "baseline_commit": baseline_commit,
        "source_sha256": {name: hashlib.sha256(value.encode()).hexdigest()
                          for name, value in sources.items()},
        "candidate_source_stable": code_stable, "complete_table_parity": complete and parity,
        "rows": args.rows, "columns": args.columns, "updates": args.updates,
        "median_ms": {name: statistics.median(row["median_ms"] for row in trials if row["variant"] == name)
                      for name in sources}, "trials": trials}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    print(json.dumps({key: result[key] for key in ("accepted", "complete_table_parity", "median_ms", "rows", "columns", "updates")}))
    return 0 if accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())
