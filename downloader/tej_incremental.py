"""Incremental source queries without replacing TEJ's unfinished history plan.

The daily clock is a check window, not an invented publication timestamp. A
checked cycle means its native queries finished, including explicit empties;
it never certifies that the source published all expected observations.
"""
from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
import json
from zoneinfo import ZoneInfo

CONTRACT = "tej_separate_incremental_plans_v1"
PRIORITY = -1000
TAIPEI = ZoneInfo("Asia/Taipei")
SUPERSEDED = (
    'superseded_exact_api_scope_v1', 'superseded_selector_geometry_v1',
    'superseded_company_hint_v1', 'superseded_company_membership_v1',
    'superseded_query_tiling_v1', 'superseded_capacity_v1', 'superseded_key1_layout_v1',
    'superseded_key3_layout_v1', 'superseded_month_period_grid_v1',
)


def _settings(config: dict | None):
    if not config or config.get("enabled") is not True:
        return None
    if config.get("contract") != CONTRACT:
        raise ValueError("Unreviewed TEJ incremental scheduling contract")
    if type(config.get("check_hour_taipei")) is not int or not 0 <= config["check_hour_taipei"] <= 23:
        raise ValueError("Invalid incremental check clock")
    if type(config.get('api_snapshot_days', 7)) is not int or not 1 <= config.get('api_snapshot_days', 7) <= 31:
        raise ValueError('Invalid API snapshot cadence')
    for days in config["overlap_days"].values():
        if type(days) is not int or not 0 <= days <= 730:
            raise ValueError("Invalid incremental source overlap")
    return config


def _overlap(settings: dict, frequency: str) -> int:
    frequency = 'quarterly' if frequency.startswith('quarterly') else frequency
    return settings['overlap_days'].get(frequency, settings['overlap_days']['default'])


def _due_day(now: datetime, config: dict) -> date:
    local = now.astimezone(TAIPEI)
    return local.date() - timedelta(days=local.time() < time(config["check_hour_taipei"]))


def _schema(con):
    con.execute("CREATE TABLE IF NOT EXISTS tej_incremental_cycles ("
                "cycle_id TEXT PRIMARY KEY,table_id TEXT NOT NULL,generation TEXT NOT NULL,"
                "start_day TEXT NOT NULL,end_day TEXT NOT NULL,state TEXT NOT NULL,"
                "discovery_id TEXT NOT NULL,plan_json TEXT,next_query INTEGER NOT NULL DEFAULT 0,"
                "total_queries INTEGER NOT NULL DEFAULT 0,checked_at_utc TEXT,"
                "UNIQUE(table_id,generation))")
    con.execute("CREATE INDEX IF NOT EXISTS tej_incremental_active "
                "ON tej_incremental_cycles(state,table_id)")
    con.execute("CREATE TABLE IF NOT EXISTS tej_incremental_tasks ("
                "task_id TEXT PRIMARY KEY,cycle_id TEXT NOT NULL,ordinal INTEGER NOT NULL)")
    con.execute("CREATE INDEX IF NOT EXISTS tej_incremental_tasks_cycle ON tej_incremental_tasks(cycle_id)")
    con.execute("CREATE INDEX IF NOT EXISTS tej_incremental_tasks_frontier ON tej_incremental_tasks(cycle_id,ordinal)")
    # Native selector/API delegation can split a pending rectangle. Follow all
    # children in that same transaction without teaching every repair path a
    # second planner. The original query's cycle tag survives compact_request.
    con.execute("CREATE TRIGGER IF NOT EXISTS tej_incremental_task_lineage AFTER INSERT ON tasks "
                "WHEN json_extract(NEW.request_json,'$.incremental_cycle') IS NOT NULL BEGIN "
                "INSERT OR IGNORE INTO tej_incremental_tasks(task_id,cycle_id,ordinal) "
                "VALUES(NEW.task_id,json_extract(NEW.request_json,'$.incremental_cycle'),"
                "COALESCE(json_extract(NEW.request_json,'$.incremental_ordinal'),-1)); END")


def _finish_cycles(con, now: datetime):
    # The native selector and receipt commit own task state, never this planner.
    con.execute("UPDATE tej_incremental_cycles SET state='checked',checked_at_utc=? "
                "WHERE state IN ('discovering','downloading') AND plan_json IS NOT NULL "
                "AND next_query=total_queries AND EXISTS(SELECT 1 FROM tasks d "
                "WHERE d.task_id=discovery_id AND d.state='complete') AND NOT EXISTS("
                "SELECT 1 FROM tej_incremental_tasks i LEFT JOIN tasks t ON t.task_id=i.task_id "
                "WHERE i.cycle_id=tej_incremental_cycles.cycle_id AND (t.state IS NULL OR "
                "(t.state<>'complete' AND t.state NOT IN (" + ','.join('?' for _ in SUPERSEDED) + "))))",
                (now.isoformat(), *SUPERSEDED))


def seed_wizard(con, config: dict, now: datetime) -> int:
    settings = _settings(config.get("incremental"))
    if settings is None:
        return 0
    from downloader.tej_history import SOURCE_SCOPE_CONTRACT, stable_id, task_request, compact_request
    _schema(con)
    _finish_cycles(con, now)
    previous = con.execute("SELECT value FROM meta WHERE key='tej_incremental_last_seed_utc'").fetchone()
    if previous and (now - datetime.fromisoformat(previous[0])).total_seconds() < 60:
        return 0
    day = _due_day(now, settings)
    cutoff_row = con.execute("SELECT value FROM meta WHERE key='cutoff'").fetchone()
    if cutoff_row is None or day <= date.fromisoformat(cutoff_row[0]):
        return 0
    generated = 0
    for row in con.execute("SELECT * FROM tables WHERE discovery_path IS NOT NULL ORDER BY phase,table_id").fetchall():
        table = dict(row)
        if (con.execute("SELECT 1 FROM tej_incremental_cycles WHERE table_id=? "
                        "AND (state<>'checked' OR generation=?) LIMIT 1", (table["table_id"], day.isoformat())).fetchone()):
            continue
        original = con.execute("SELECT * FROM tasks WHERE table_id=? AND kind='discover' AND state='complete' "
                               "AND scope_contract=? ORDER BY completed_at_utc DESC LIMIT 1",
                               (table["table_id"], SOURCE_SCOPE_CONTRACT)).fetchone()
        if original is None:
            continue
        request = task_request(None, dict(original), connection=con)
        checked = con.execute("SELECT MAX(end_day) FROM tej_incremental_cycles WHERE table_id=? AND state='checked'",
                              (table["table_id"],)).fetchone()[0]
        anchor = date.fromisoformat(checked or cutoff_row[0])
        overlap = _overlap(settings, table['frequency'])
        start = min(anchor + timedelta(days=1), day - timedelta(days=overlap))
        start = max(date.fromisoformat(config["history_search_start"]), start)
        cycle = stable_id([CONTRACT, table["table_id"], day.isoformat()])
        identity = stable_id([cycle, "discover"])
        request.update(action="plan", start=start.isoformat(), end=day.isoformat(),
                       incremental_cycle=cycle, incremental_contract=CONTRACT)
        # Current snapshots have no historical date axis. Discovery retains the
        # original native controls, and their actual key mode remains authoritative.
        con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,scope_contract) "
                    "VALUES(?,?,'discover',?,?,?,?)", (identity, table["table_id"], table["phase"], PRIORITY,
                    json.dumps(compact_request(request, table), ensure_ascii=False), SOURCE_SCOPE_CONTRACT))
        con.execute("INSERT INTO tej_incremental_cycles(cycle_id,table_id,generation,start_day,end_day,state,discovery_id) "
                    "VALUES(?,?,?,?,?,'discovering',?)", (cycle, table["table_id"], day.isoformat(),
                    start.isoformat(), day.isoformat(), identity))
        generated += 1
    con.execute("INSERT OR REPLACE INTO meta VALUES('tej_incremental_last_seed_utc',?)", (now.isoformat(),))
    return generated


def install_wizard_plan(con, table: dict, request: dict, payload: dict, config: dict) -> int:
    from downloader.tej_planning import apply_scope_budget, build_plan
    from downloader.tej_key_layout import KEY1_CONTRACT, KEY3_CONTRACT
    cycle = con.execute("SELECT * FROM tej_incremental_cycles WHERE cycle_id=? AND table_id=?",
                        (request["incremental_cycle"], table["table_id"])).fetchone()
    if cycle is None or request.get("incremental_contract") != CONTRACT:
        raise ValueError("Unregistered incremental discovery")
    if cycle["plan_json"] is not None:
        return cycle["total_queries"]
    mode = payload.get("source_key_mode")
    if mode in (1, 3):
        request = {**request, "source_key_mode": mode,
                   "key_layout_contract": KEY1_CONTRACT if mode == 1 else KEY3_CONTRACT}
        if mode == 1:
            request["frequency"] = "snapshot"
    request = apply_scope_budget({**request, "max_rows": config["max_rows_per_export"],
                                 "max_cells": config["max_cells_per_export"]}, config)
    plan = build_plan(request, payload["company_labels"], payload["date_labels"], config, [])
    con.execute("UPDATE tej_incremental_cycles SET state='downloading',plan_json=?,total_queries=? WHERE cycle_id=?",
                (json.dumps(plan, ensure_ascii=False, separators=(",", ":")), plan["total_queries"], cycle["cycle_id"]))
    return plan["total_queries"]


def refill_wizard(con) -> int:
    if not con.execute("SELECT 1 FROM sqlite_master WHERE name='tej_incremental_cycles'").fetchone():
        return 0
    from downloader.tej_history import SOURCE_SCOPE_CONTRACT, compact_request, stable_id
    from downloader.tej_planning import next_request
    from downloader.tej_key_layout import key_count
    inserted = 0
    for row in con.execute("SELECT * FROM tej_incremental_cycles c WHERE state='downloading' AND next_query<total_queries "
                           "AND NOT EXISTS(SELECT 1 FROM tej_incremental_tasks i JOIN tasks t ON t.task_id=i.task_id "
                           "WHERE i.cycle_id=c.cycle_id AND i.ordinal=c.next_query-1 "
                           "AND t.state<>'complete' AND t.state NOT IN ("
                           + ','.join('?' for _ in SUPERSEDED) + '))', SUPERSEDED).fetchall():
        table = dict(con.execute("SELECT * FROM tables WHERE table_id=?", (row["table_id"],)).fetchone())
        query = next_request(json.loads(row["plan_json"]), row["next_query"])
        query['incremental_ordinal'] = row['next_query']
        identity = stable_id([table["table_id"], query])
        expected = len(query["company_labels"]) * (1 if key_count(query) == 1 else len(query["date_labels"]))
        con.execute("INSERT OR IGNORE INTO tasks(task_id,table_id,kind,phase,priority,request_json,expected_rows,"
                    "scope_contract,work_expected_rows) VALUES(?,?,'download',?,?,?,?,?,?)",
                    (identity, table["table_id"], table["phase"], PRIORITY,
                     json.dumps(compact_request(query, table), ensure_ascii=False, separators=(",", ":")),
                     expected, SOURCE_SCOPE_CONTRACT, expected))
        con.execute("INSERT OR IGNORE INTO tej_incremental_tasks VALUES(?,?,?)", (identity, row["cycle_id"], row['next_query']))
        con.execute("UPDATE tej_incremental_cycles SET next_query=next_query+1 WHERE cycle_id=?", (row["cycle_id"],))
        inserted += 1
    return inserted


def seed_api(api, config: dict | None) -> int:
    settings = _settings(config)
    if settings is None:
        return 0
    from contextlib import closing
    from downloader.tej_api import connect
    from downloader.download_tej_api import _task_id
    path = api.root / "scope_inventory.json"
    if not path.exists():
        return 0
    scopes = json.loads(path.read_text())["tables"]
    day = _due_day(api.now(), settings)
    generated = 0
    with closing(connect(api.root)) as con, con:
        con.execute("CREATE TABLE IF NOT EXISTS api_incremental_cycles ("
                    "cycle_id TEXT PRIMARY KEY,table_id TEXT NOT NULL,start_day TEXT NOT NULL,"
                    "end_day TEXT NOT NULL,state TEXT NOT NULL,UNIQUE(table_id,end_day))")
        con.execute("CREATE TABLE IF NOT EXISTS api_incremental_tasks (task_id TEXT PRIMARY KEY,cycle_id TEXT NOT NULL)")
        con.execute("CREATE INDEX IF NOT EXISTS api_incremental_tasks_cycle ON api_incremental_tasks(cycle_id)")
        # A partitioned parent is complete only through all of its native child
        # queries. Blocked/error/unknown outcomes keep the cycle outstanding.
        con.execute("UPDATE api_incremental_cycles SET state='checked' WHERE state='pending' AND NOT EXISTS("
                    "SELECT 1 FROM api_incremental_tasks i LEFT JOIN tasks t ON t.id=i.task_id "
                    "WHERE i.cycle_id=api_incremental_cycles.cycle_id "
                    "AND (t.state IS NULL OR t.state NOT IN ('complete','superseded_disjoint_partition')))")
        for table in scopes:
            if table["access_state"] not in {"verified_nonempty", "verified_empty_query"}:
                continue
            identifier, frequency = table["table_id"], table["frequency"]
            if con.execute("SELECT 1 FROM api_incremental_cycles WHERE table_id=? AND (state<>'checked' OR end_day=?)",
                           (identifier, day.isoformat())).fetchone():
                continue
            last = con.execute("SELECT MAX(end_day) FROM api_incremental_cycles WHERE table_id=? AND state='checked'",
                               (identifier,)).fetchone()[0]
            observed = table.get("last_observed")
            # A previously empty source still needs future checks. Its check
            # date is a cadence anchor only; an unfiltered native query makes
            # no invented historical date/coverage claim.
            anchor_text = last or observed or table.get('last_verified_utc')
            if not anchor_text:
                continue
            anchor = date.fromisoformat(anchor_text[:10])
            snapshot = frequency in {"current_snapshot", "reference"}
            cadence = settings.get("api_snapshot_days", 7) if snapshot else 1
            if (day - anchor).days < cadence:
                continue
            overlap = _overlap(settings, frequency)
            start = min(anchor + timedelta(days=1), day - timedelta(days=overlap))
            if snapshot or not observed:
                start = day  # Check-cycle metadata, never a snapshot history date.
            params = {} if snapshot or not observed else {"mdate.gte": start.isoformat(), "mdate.lte": day.isoformat(), "opts.sort": "coid.asc"}
            cycle = _task_id(identifier, {"contract": CONTRACT, "end": day.isoformat()})
            identity = _task_id(identifier, {"cycle": cycle, "params": params})
            con.execute("INSERT INTO tasks(id,table_id,params_json,priority,state) VALUES(?,?,?,?,'pending')",
                        (identity, identifier, json.dumps(params, sort_keys=True), PRIORITY))
            con.execute("INSERT INTO api_incremental_cycles VALUES(?,?,?,?,'pending')",
                        (cycle, identifier, start.isoformat(), day.isoformat()))
            con.execute("INSERT INTO api_incremental_tasks VALUES(?,?)", (identity, cycle))
            generated += 1
    return generated


def partition_child(con, parent_id: str, identifier: str, params: dict, default_id: str) -> str:
    """Keep capped page splits in the original incremental cycle and quota."""
    if not con.execute("SELECT 1 FROM sqlite_master WHERE name='api_incremental_tasks'").fetchone():
        return default_id
    row = con.execute("SELECT cycle_id FROM api_incremental_tasks WHERE task_id=?", (parent_id,)).fetchone()
    if row is None:
        return default_id
    from downloader.download_tej_api import _task_id
    identity = _task_id(identifier, {"cycle": row[0], "params": params})
    con.execute("INSERT OR IGNORE INTO api_incremental_tasks VALUES(?,?)", (identity, row[0]))
    return identity


def summary(con, *, api=False) -> dict:
    table = 'api_incremental_cycles' if api else 'tej_incremental_cycles'
    if not con.execute('SELECT 1 FROM sqlite_master WHERE name=?', (table,)).fetchone():
        return {'contract': CONTRACT, 'cycle_states': {}, 'all_history_complete': False}
    states = dict(con.execute('SELECT state,COUNT(*) FROM ' + table + ' GROUP BY state'))
    latest = con.execute('SELECT MAX(end_day) FROM ' + table + " WHERE state='checked'").fetchone()[0]
    return {'contract': CONTRACT, 'cycle_states': states, 'last_checked_cycle_end': latest,
            'all_history_complete': False, 'checked_is_source_query_completion_not_observation_completeness': True}
