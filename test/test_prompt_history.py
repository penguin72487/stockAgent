import json

from stockagent.prompt_history import collect, deduplicate, digest, request_text


def session(root, name, source="vscode", events=()):
    root.mkdir(parents=True, exist_ok=True)
    path = root / name
    lines = [{"type": "session_meta", "payload": {"id": name, "source": source, "cwd": "/root/stockAgent"}}, *events]
    path.write_text("".join(json.dumps(item) + "\n" for item in lines))
    return path


def event(text, timestamp="2026-10-05T16:00:00Z", response=False):
    payload = {"role": "user", "content": [{"type": "input_text", "text": text}]} if response else {
        "type": "user_message", "message": text}
    return {"type": "response_item" if response else "event_msg", "timestamp": timestamp, "payload": payload}


def test_roles_context_and_subagents(tmp_path):
    root = tmp_path / ".codex/sessions"
    session(root, "main.jsonl", events=[event("原始資料保持 NULL"), event("# AGENTS.md instructions for project"),
                                      {"type": "response_item", "payload": {"role": "assistant", "content": "false preference"}}])
    session(root, "child.jsonl", {"subagent": {"thread_spawn": {}}}, [event("delegation is not user opinion")])
    result = collect("penguin", [str(root)])
    assert [r["text"] for r in result["candidates"]] == ["原始資料保持 NULL"]
    assert result["counts"]["subagent_files"] == 1
    assert result["counts"]["excluded_context"] == 1


def test_incremental_boundary_and_partial_line(tmp_path):
    root = tmp_path / ".codex/sessions"
    path = session(root, "main.jsonl", events=[event("old request")])
    first = collect("penguin", [str(root)])
    partial = json.dumps(event("new request", "2026-10-05T17:00:00Z"))
    with path.open("a") as stream:
        stream.write(partial[:30])
    second = collect("penguin", [str(root)], first)
    assert second["candidates"] == []
    assert second["counts"]["partial_trailing_records"] == 1
    with path.open("a") as stream:
        stream.write(partial[30:] + "\n")
    third = collect("penguin", [str(root)], second)
    assert [r["text"] for r in third["candidates"]] == ["new request"]
    assert third["sessions"][0]["start_offset"] == first["sessions"][0]["snapshot_bytes"]


def test_replaced_source_rescanned_without_counting_known_message(tmp_path):
    root = tmp_path / ".codex/sessions"
    path = session(root, "main.jsonl", events=[event("old request")])
    first = collect("penguin", [str(root)])
    path.write_text(path.read_text().replace("old request", "new request"))
    second = collect("penguin", [str(root)], first)
    assert second["sessions"][0]["start_offset"] == 0
    assert second["candidates"][0]["text"] == "new request"


def test_event_response_and_copied_histories_keep_provenance(tmp_path):
    root = tmp_path / ".codex/sessions"
    path = session(root, "main.jsonl", events=[event("same request", "2026-10-05T16:00:00.900Z"),
                                              event("same request", "2026-10-05T16:00:01.001Z", response=True)])
    session(root, "inherited.jsonl", events=[event("same request", "2026-10-05T16:00:00.900Z")])
    result = collect("penguin", [str(root)])
    assert len(result["candidates"]) == 1
    assert len(result["candidates"][0]["sources"]) == 3
    locator = next(s for s in result["candidates"][0]["sources"] if s["path"] == str(path))
    with path.open("rb") as stream:
        stream.seek(locator["offset"])
        assert digest(stream.read(locator["length"])) == locator["record_sha256"]


def test_cross_node_copies_do_not_raise_confidence():
    records = [{"timestamp": "2026-10-05T16:00:00Z", "text": "request", "text_sha256": "hash",
                "sources": [{"node": node, "path": f"/{node}", "session_id": node}]} for node in ("penguin", "Vastai1T")]
    result = deduplicate(records)
    assert len(result) == 1
    assert {s["node"] for s in result[0]["sources"]} == {"penguin", "Vastai1T"}


def test_question_answer_and_ide_prefix():
    text = '<send_user_message_question_reply>[{"question":"tool suggestion", "answer":"只同步 Vastai1T"}]</send_user_message_question_reply>'
    assert request_text(text) == ("只同步 Vastai1T", None)
    assert request_text("# Context from my IDE setup:\n.env\n## My request:\n一起整理既有skill") == ("一起整理既有skill", None)
    assert request_text("<codex_internal_context>continue goal</codex_internal_context>")[0] is None
    assert request_text("<recommended_plugins>available apps</recommended_plugins>")[0] is None
    assert request_text('<external_codex_apps_open_page>{"page_id":null}</external_codex_apps_open_page>')[0] is None
    assert request_text("# Files pasted by the user:\nattachment\n## My request for Codex:\n不改交易計算") == ("不改交易計算", None)


def test_wrong_node_baseline_rejected(tmp_path):
    import pytest
    with pytest.raises(ValueError, match="another node"):
        collect("Vastai1T", [str(tmp_path / ".codex/sessions")], {"node": "penguin"})


def test_interior_parse_failure_and_credential_redaction(tmp_path):
    root = tmp_path / ".codex/sessions"
    path = session(root, "main.jsonl", events=[event("API_KEY=private_value keep this principle")])
    with path.open("a") as stream:
        stream.write("broken JSON\n")
    result = collect("penguin", [str(root)])
    assert "private_value" not in result["candidates"][0]["text"]
    assert result["parse_errors"][0]["reason"] == "invalid_json_record"
    assert result["sessions"][0]["processed_bytes"] < path.stat().st_size
