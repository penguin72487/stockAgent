from datetime import date
import json

from scripts.refresh_tw_day_trade_margin_actions import commands


def test_new_execution_source_does_not_claim_current_year_is_baseline(tmp_path):
    output = tmp_path / "execution_actions"
    output.mkdir()
    planned = commands(tmp_path, output, 2026, date(2026, 9, 10))
    assert planned[0][planned[0].index("--start-year") + 1] == "2000"
    assert planned[1][planned[1].index("--retained-source-dir") + 1] == str(tmp_path)
    assert all(command[command.index("--output-dir") + 1] == str(output) for command in planned)
    (output / "tw_corporate_action_reference.summary.json").write_text(json.dumps({"baseline_established": True}))
    planned = commands(tmp_path, output, 2026, date(2026, 9, 11))
    assert planned[0][planned[0].index("--start-year") + 1] == "2026"
    assert all(command[command.index("--end-date") + 1] == "2026-09-11" for command in planned)
