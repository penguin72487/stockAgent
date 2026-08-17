import assert from "node:assert/strict";
import test from "node:test";

import timeAxis from "../services/public_dashboards/time-axis.js";

const end = Date.parse("2026-08-15T04:07:00Z"); // 12:07 Asia/Taipei

test("one-hour axis marks every fifteen minutes", () => {
  const axis = timeAxis.buildTimeAxis({range: "1h", timestamps: [end]});
  assert.deepEqual(
    axis.ticks.map((tick) => tick.label),
    ["11:15", "11:30", "11:45", "12:00"],
  );
  assert.ok(axis.ticks.every((tick) => tick.kind === "regular"));
});

test("one-day market axis retains every hour and adds open-close markers", () => {
  const axis = timeAxis.buildTimeAxis({
    range: "1d",
    timestamps: [Date.parse("2026-08-14T08:00:00Z")],
    sessions: [
      {label: "開", minute: 9 * 60},
      {label: "收", minute: 13 * 60 + 30},
    ],
  });
  const regular = axis.ticks.filter((tick) => tick.kind === "regular");
  const sessions = axis.ticks.filter((tick) => tick.kind === "session");
  assert.equal(regular.length, 25);
  assert.ok(regular.every((tick, index) => (
    index === 0 || tick.timestamp - regular[index - 1].timestamp === 60 * 60 * 1000
  )));
  assert.deepEqual(sessions.map((tick) => tick.label), ["開 09:00", "收 13:30"]);
});

test("one-week axis uses one Taiwan calendar-day tick", () => {
  const axis = timeAxis.buildTimeAxis({
    range: "1w",
    timestamps: [Date.parse("2026-08-15T08:00:00Z")],
  });
  assert.ok(axis.ticks.length >= 7);
  assert.ok(axis.ticks.every((tick, index) => (
    index === 0 || tick.timestamp - axis.ticks[index - 1].timestamp === 24 * 60 * 60 * 1000
  )));
  assert.ok(axis.ticks.every((tick) => /\d{2}\/\d{2}/.test(tick.label)));
});

test("all-history axis adapts to monthly ticks for a long span", () => {
  const axis = timeAxis.buildTimeAxis({
    range: "all",
    timestamps: [
      Date.parse("2025-01-01T00:00:00Z"),
      Date.parse("2026-08-15T00:00:00Z"),
    ],
  });
  assert.ok(axis.ticks.length >= 18);
  assert.ok(axis.ticks.length <= 21);
  assert.ok(axis.ticks.every((tick) => /\d{4}\/\d{2}/.test(tick.label)));
});

test("collapsed axis removes globally empty intervals and maps ticks to observations", () => {
  const friday = Date.parse("2026-08-13T16:00:00Z"); // Friday 00:00 Taipei
  const monday = Date.parse("2026-08-16T16:00:00Z"); // Monday 00:00 Taipei
  const axis = timeAxis.buildTimeAxis({
    range: "all",
    timestamps: [friday, monday],
    collapseEmptyIntervals: true,
  });
  assert.deepEqual(axis.observedTimes, [friday, monday]);
  assert.equal(timeAxis.position(axis, friday, 0, 100), 0);
  assert.equal(timeAxis.position(axis, monday, 0, 100), 100);
  assert.deepEqual(
    axis.ticks.map((tick) => tick.observedTimestamp),
    [friday, monday],
  );
  assert.ok(axis.ticks.every((tick) => (
    tick.observedTimestamp === friday || tick.observedTimestamp === monday
  )));
});
