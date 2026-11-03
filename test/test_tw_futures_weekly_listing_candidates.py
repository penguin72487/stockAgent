from datetime import date

from scripts.audit_tw_futures_weekly_listing_candidates import classify, nominal_expiry


def test_2013_typhoon_on_nominal_listing_day_is_not_classified_as_ordinary():
    expiry, listing, first_print = date(2013, 8, 28), date(2013, 8, 21), date(2013, 8, 22)
    assert nominal_expiry("201308W4") == expiry
    assert classify("201308W4", first_print, expiry, {first_print, expiry}) == {
        "state": "holiday_or_calendar_unverified", "scheduled_listing": listing}


def test_ordinary_2013_week_uses_one_week_schedule():
    listing, expiry = date(2013, 8, 7), date(2013, 8, 14)
    assert classify("201308W2", listing, expiry, {listing, expiry}) == {
        "state": "ordinary_schedule_candidate", "scheduled_listing": listing}


def test_2022_two_week_transition_and_prior_one_week_contract():
    old = classify("202211W2", date(2022, 11, 2), date(2022, 11, 9),
                   {date(2022, 11, 2), date(2022, 11, 9)})
    new = classify("202211W4", date(2022, 11, 9), date(2022, 11, 23),
                   {date(2022, 11, 9), date(2022, 11, 23)})
    assert old["scheduled_listing"] == date(2022, 11, 2)
    assert new["scheduled_listing"] == date(2022, 11, 9)
    assert old["state"] == new["state"] == "ordinary_schedule_candidate"


def test_holidays_launch_day_and_impossible_week_are_not_admitted():
    assert classify("201308W1", date(2013, 7, 31), date(2013, 8, 7), set())["state"] == "outside_reviewed_scope"
    assert classify("201308W4", date(2013, 8, 22), date(2013, 8, 28), set())["state"] == "holiday_or_calendar_unverified"
    assert classify("201308W4", date(2013, 8, 22), date(2013, 8, 29),
                    {date(2013, 8, 21), date(2013, 8, 28)})["state"] == "official_expiry_differs_or_missing"
    assert nominal_expiry("201308W3") is None
    assert nominal_expiry("201302W5") is None
