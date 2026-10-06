"""Read bounded Codex user-history snapshots without modifying source sessions."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys

CONTRACT = "codex-primary-history-v1"


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fingerprints(path: Path, size: int) -> dict:
    prefix_size = min(4096, size)
    tail_offset = max(0, size - 4096)
    with path.open("rb") as stream:
        prefix = stream.read(prefix_size)
        stream.seek(tail_offset)
        tail = stream.read(size - tail_offset)
    return {"snapshot_prefix_bytes": prefix_size, "snapshot_prefix_sha256": digest(prefix),
            "snapshot_tail_offset": tail_offset, "snapshot_tail_sha256": digest(tail)}


def verified_baseline(path: Path, old: dict, size: int) -> bool:
    previous_size = old.get("snapshot_bytes", -1)
    if not isinstance(previous_size, int) or not 0 <= previous_size <= size:
        return False
    actual = fingerprints(path, previous_size)
    return all(actual[key] == old.get(key) for key in actual)


def user_text(item: dict) -> str | None:
    payload = item.get("payload", {})
    if item.get("type") == "event_msg" and payload.get("type") == "user_message":
        value = payload.get("message")
        return value if isinstance(value, str) else None
    if item.get("type") == "response_item" and payload.get("role") == "user":
        parts = payload.get("content", [])
        if isinstance(parts, str):
            return parts
        return "\n".join(p.get("text", "") for p in parts
                         if isinstance(p, dict) and p.get("type") in ("input_text", "text"))
    return None


def request_text(text: str) -> tuple[str | None, str | None]:
    """Remove machine context; retain only answers from structured question replies."""
    text = text.strip()
    if text.startswith(("# Context from my IDE setup:", "# Files mentioned by the user:",
                        "# Files pasted by the user:")):
        marker = re.search(r"## My request(?: for Codex)?:", text)
        if marker:
            text = text[marker.end():].strip()
    if text.startswith("<send_user_message_question_reply>"):
        body = text.split(">", 1)[1].split("</send_user_message_question_reply>", 1)[0]
        try:
            answers = json.loads(body)
            text = "\n".join(a["answer"] for a in answers
                             if isinstance(a, dict) and isinstance(a.get("answer"), str))
        except (ValueError, TypeError):
            return None, "invalid_question_reply"
    if (text.startswith(("# AGENTS.md instructions", "<environment_context>",
                         "<INSTRUCTIONS>", "<skills_instructions>", "<recommended_plugins>",
                         "<external_codex_apps_", "<permissions instructions>", "<collaboration_mode>"))
            or "<codex_internal_context>" in text):
        return None, "injected_context"
    if not text:
        return None, "empty_or_nontext"
    # Candidate excerpts remain private; obvious credential assignments are still removed.
    text = re.sub(r"(?im)\b(?:api[_-]?key|access[_-]?token|password|secret)\s*[=:]\s*[^\s,;]+",
                  "[credential assignment redacted]", text)
    text = re.sub(r"\bsk-[A-Za-z0-9_-]{16,}\b", "[token redacted]", text)
    return text, None


def timestamp_seconds(value: str) -> float | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except (ValueError, TypeError, AttributeError):
        return None


def deduplicate(records: list[dict], seen: list[dict] = ()) -> list[dict]:
    """Coalesce event/response and inherited copies, keeping every source locator."""
    by_text = {}
    for old in seen:
        by_text.setdefault(old["text_sha256"], []).append(old | {"known": True})
    result = []
    for record in sorted(records, key=lambda r: (r["timestamp"], r["sources"][0]["path"])):
        seconds = timestamp_seconds(record["timestamp"])
        matches = by_text.setdefault(record["text_sha256"], [])
        match = next((x for x in matches if
                      (seconds is not None and timestamp_seconds(x["timestamp"]) is not None
                       and abs(seconds - timestamp_seconds(x["timestamp"])) <= 3)
                      or (seconds is None and record["timestamp"] == x["timestamp"]
                          and record["sources"][0]["session_id"] == x.get("session_id"))), None)
        if match is not None:
            if not match.get("known"):
                match["sources"].extend(record["sources"])
            continue
        record["id"] = digest((record["timestamp"] + "|" + record["text_sha256"]).encode())
        matches.append(record)
        result.append(record)
    return result


def collect(node: str, roots: list[str], baseline: dict | None = None) -> dict:
    baseline = baseline or {}
    if baseline.get("node", node) != node:
        raise ValueError("history baseline belongs to another node")
    old_files = {r["path"]: r for r in baseline.get("sessions", [])}
    sessions, records, errors = [], [], []
    counts = {"files": 0, "primary_files": 0, "subagent_files": 0, "streamed_primary_files": 0,
              "streamed_bytes": 0, "user_records": 0, "excluded_context": 0,
              "partial_trailing_records": 0}
    paths = set()
    for root_value in roots:
        root = Path(root_value)
        if root.name not in ("sessions", "archived_sessions") or root.parent.name != ".codex":
            raise ValueError("history root must be a Codex sessions or archived_sessions directory")
        if root.is_symlink() or (root.exists() and root.resolve() != root):
            raise ValueError("symlinked history root is not supported")
        if root.exists():
            paths.update(root.rglob("*.jsonl"))
    for path in sorted(paths):
        if path.is_symlink() or path.resolve() != path:
            raise ValueError(f"symlinked history member: {path}")
        initial_stat = path.stat()
        size = initial_stat.st_size
        before = fingerprints(path, size)
        with path.open("rb") as stream:
            first = stream.readline(size)
            try:
                meta_event = json.loads(first)
                if meta_event.get("type") != "session_meta":
                    raise ValueError("missing session metadata")
                meta = meta_event["payload"]
            except (ValueError, KeyError):
                errors.append({"path": str(path), "offset": 0, "reason": "invalid_session_metadata"})
                continue
            source = meta.get("source", "unknown")
            subagent = (isinstance(source, dict) and "subagent" in source
                        or meta.get("thread_source") == "subagent"
                        or isinstance(source, str) and "subagent" in source.lower())
            old = old_files.get(str(path), {})
            verified = verified_baseline(path, old, size)
            start = old.get("processed_bytes", old.get("snapshot_bytes", 0)) if verified else 0
            if not isinstance(start, int) or not 0 <= start <= size:
                start = 0
                verified = False
            entry = {"path": str(path), "session_id": meta.get("session_id", meta.get("id")),
                     "cwd": meta.get("cwd"), "source": source, "subagent": bool(subagent),
                     "baseline_bytes": old.get("snapshot_bytes", 0), "snapshot_bytes": size,
                     "start_offset": start, "processed_bytes": size,
                     "boundary": "verified_incremental" if verified else "new_or_replaced", **before}
            sessions.append(entry)
            counts["files"] += 1
            counts["subagent_files" if subagent else "primary_files"] += 1
            if subagent or start == size:
                entry["review"] = "subagent_metadata_only" if subagent else "unchanged"
                continue
            entry["review"] = "user_role_extraction"
            counts["streamed_primary_files"] += 1
            stream.seek(start)
            while stream.tell() < size:
                offset = stream.tell()
                raw = stream.readline(size - offset)
                if not raw.endswith(b"\n"):
                    entry["processed_bytes"] = min(entry["processed_bytes"], offset)
                    counts["partial_trailing_records"] += 1
                    break
                counts["streamed_bytes"] += len(raw)
                try:
                    event = json.loads(raw)
                except ValueError:
                    errors.append({"path": str(path), "offset": offset, "reason": "invalid_json_record"})
                    entry["processed_bytes"] = min(entry["processed_bytes"], offset)
                    continue
                original = user_text(event)
                if original is None:
                    continue
                counts["user_records"] += 1
                text, exclusion = request_text(original)
                if exclusion:
                    counts["excluded_context"] += 1
                    continue
                normalized = " ".join(text.split())
                records.append({"timestamp": event.get("timestamp", ""), "text": text,
                                "text_sha256": digest(normalized.encode()),
                                "sources": [{"node": node, "path": str(path), "offset": offset,
                                             "length": len(raw), "record_sha256": digest(raw),
                                             "session_id": entry["session_id"], "kind": event.get("type")} ]})
        if path.stat().st_ino != initial_stat.st_ino or path.stat().st_size < size or fingerprints(path, size) != before:
            raise ValueError(f"history snapshot changed during read: {path}")
    candidates = deduplicate(records, baseline.get("seen", []))
    seen = baseline.get("seen", []) + [{"id": r["id"], "timestamp": r["timestamp"],
                                      "text_sha256": r["text_sha256"],
                                      "session_id": r["sources"][0]["session_id"]} for r in candidates]
    counts["candidates"] = len(candidates)
    return {"contract": CONTRACT, "node": node, "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "sessions": sessions, "seen": seen, "parse_errors": errors, "counts": counts,
            "candidates": candidates}


def rpc() -> None:
    request = json.load(sys.stdin)
    if request.get("contract") != CONTRACT:
        raise ValueError("history contract mismatch")
    result = collect(request["node"], request["roots"], request.get("baseline"))
    print(json.dumps(result, ensure_ascii=False))
