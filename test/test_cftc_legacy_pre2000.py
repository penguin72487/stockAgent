from __future__ import annotations

import pandas as pd

from scripts.download_cftc_legacy_pre2000 import _normalize


def test_cftc_normalize_keeps_only_gap_and_conservative_availability() -> None:
    source = pd.DataFrame(
        {
            "As of Date in Form YYYY-MM-DD": ["1999-12-28", "2000-01-04"],
            "CFTC Contract Market Code": ["001602", "001602"],
            "Open Interest (All)": [100, 101],
        }
    )
    frame = _normalize(source, report_mode="legacy_futures_only", source_sha256="a" * 64)

    assert len(frame) == 1
    assert frame.loc[0, "date"].date().isoformat() == "1999-12-28"
    assert frame.loc[0, "conservative_available_date"].date().isoformat() == "2000-01-04"
    assert frame.loc[0, "cftc_contract_market_code"] == "001602"
    assert str(frame["cftc_contract_market_code"].dtype) == "string"
