"use strict";

(function installTimeAxis(root) {
  const TAIPEI_OFFSET_MS = 8 * 60 * 60 * 1000;
  const MINUTE_MS = 60 * 1000;
  const HOUR_MS = 60 * MINUTE_MS;
  const DAY_MS = 24 * HOUR_MS;
  const RANGE_MS = {
    "1h": HOUR_MS,
    "1d": DAY_MS,
    "1w": 7 * DAY_MS,
    "1mo": 31 * DAY_MS,
    "1q": 92 * DAY_MS,
    "1y": 366 * DAY_MS,
  };

  const timeFormatter = new Intl.DateTimeFormat("zh-TW", {
    timeZone: "Asia/Taipei",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  });
  const dayFormatter = new Intl.DateTimeFormat("zh-TW", {
    timeZone: "Asia/Taipei",
    month: "2-digit",
    day: "2-digit",
  });
  const dayWeekFormatter = new Intl.DateTimeFormat("zh-TW", {
    timeZone: "Asia/Taipei",
    month: "2-digit",
    day: "2-digit",
    weekday: "short",
  });
  const monthFormatter = new Intl.DateTimeFormat("zh-TW", {
    timeZone: "Asia/Taipei",
    year: "numeric",
    month: "2-digit",
  });
  const yearFormatter = new Intl.DateTimeFormat("zh-TW", {
    timeZone: "Asia/Taipei",
    year: "numeric",
  });

  function finiteTimes(values) {
    return values
      .map((value) => value instanceof Date ? value.getTime() : Number(value))
      .filter(Number.isFinite)
      .sort((left, right) => left - right);
  }

  function localFloor(timestamp, unit, step = 1) {
    const shifted = new Date(timestamp + TAIPEI_OFFSET_MS);
    if (unit === "month") {
      const month = Math.floor(shifted.getUTCMonth() / step) * step;
      return Date.UTC(shifted.getUTCFullYear(), month, 1) - TAIPEI_OFFSET_MS;
    }
    if (unit === "year") {
      const year = Math.floor(shifted.getUTCFullYear() / step) * step;
      return Date.UTC(year, 0, 1) - TAIPEI_OFFSET_MS;
    }
    if (unit === "week") {
      const midnight = Date.UTC(
        shifted.getUTCFullYear(), shifted.getUTCMonth(), shifted.getUTCDate(),
      );
      const mondayOffset = (shifted.getUTCDay() + 6) % 7;
      return midnight - mondayOffset * DAY_MS - TAIPEI_OFFSET_MS;
    }
    const interval = unit === "minute" ? step * MINUTE_MS
      : unit === "hour" ? step * HOUR_MS
      : step * DAY_MS;
    return Math.floor((timestamp + TAIPEI_OFFSET_MS) / interval) * interval
      - TAIPEI_OFFSET_MS;
  }

  function localDateKey(timestamp) {
    const shifted = new Date(timestamp + TAIPEI_OFFSET_MS);
    return `${shifted.getUTCFullYear()}-${shifted.getUTCMonth() + 1}-${shifted.getUTCDate()}`;
  }

  function addCalendar(timestamp, unit, step) {
    if (unit === "minute") return timestamp + step * MINUTE_MS;
    if (unit === "hour") return timestamp + step * HOUR_MS;
    if (unit === "day") return timestamp + step * DAY_MS;
    if (unit === "week") return timestamp + step * 7 * DAY_MS;
    const shifted = new Date(timestamp + TAIPEI_OFFSET_MS);
    if (unit === "month") {
      return Date.UTC(
        shifted.getUTCFullYear(), shifted.getUTCMonth() + step, 1,
      ) - TAIPEI_OFFSET_MS;
    }
    if (unit === "year") {
      return Date.UTC(shifted.getUTCFullYear() + step, 0, 1)
        - TAIPEI_OFFSET_MS;
    }
    return timestamp + step * DAY_MS;
  }

  function tickSpec(range, spanMs) {
    if (range === "1h") return {unit: "minute", step: 15, format: timeFormatter};
    if (range === "1d") return {unit: "hour", step: 1, format: timeFormatter, rotate: true};
    if (range === "1w") return {unit: "day", step: 1, format: dayWeekFormatter};
    if (range === "1mo") return {unit: "week", step: 1, format: dayFormatter};
    if (range === "1q") return {unit: "month", step: 1, format: monthFormatter};
    if (range === "1y") return {unit: "month", step: 1, format: monthFormatter, rotate: true};
    if (spanMs <= 14 * DAY_MS) return {unit: "day", step: 1, format: dayWeekFormatter};
    if (spanMs <= 90 * DAY_MS) return {unit: "week", step: 1, format: dayFormatter};
    if (spanMs <= 2 * 366 * DAY_MS) return {unit: "month", step: 1, format: monthFormatter};
    if (spanMs <= 6 * 366 * DAY_MS) return {unit: "month", step: 3, format: monthFormatter};
    return {unit: "year", step: 1, format: yearFormatter};
  }

  function regularTicks(startMs, endMs, spec) {
    const ticks = [];
    let cursor = localFloor(startMs, spec.unit, spec.step);
    if (cursor < startMs) cursor = addCalendar(cursor, spec.unit, spec.step);
    let guard = 0;
    while (cursor <= endMs && guard < 400) {
      ticks.push({
        timestamp: cursor,
        label: spec.format.format(new Date(cursor)),
        kind: "regular",
        rotate: Boolean(spec.rotate),
      });
      cursor = addCalendar(cursor, spec.unit, spec.step);
      guard += 1;
    }
    return ticks;
  }

  function sessionTicks(startMs, endMs, sessions, activeDates) {
    if (!sessions.length) return [];
    const ticks = [];
    let day = localFloor(startMs, "day", 1);
    if (day > startMs) day -= DAY_MS;
    for (let guard = 0; day <= endMs && guard < 4; guard += 1, day += DAY_MS) {
      const weekday = new Date(day + TAIPEI_OFFSET_MS).getUTCDay();
      if (weekday === 0 || weekday === 6) continue;
      if (!activeDates.has(localDateKey(day))) continue;
      for (const session of sessions) {
        const timestamp = day + Number(session.minute) * MINUTE_MS;
        if (timestamp < startMs || timestamp > endMs) continue;
        ticks.push({
          timestamp,
          label: `${String(session.label)} ${timeFormatter.format(new Date(timestamp))}`,
          kind: "session",
          rotate: true,
        });
      }
    }
    return ticks;
  }

  function nearestObserved(times, timestamp) {
    let low = 0;
    let high = times.length;
    while (low < high) {
      const middle = Math.floor((low + high) / 2);
      if (times[middle] < timestamp) low = middle + 1;
      else high = middle;
    }
    const candidates = [];
    if (low < times.length) candidates.push(low);
    if (low > 0) candidates.push(low - 1);
    if (!candidates.length) return null;
    const index = candidates.reduce((best, candidate) => (
      Math.abs(times[candidate] - timestamp) < Math.abs(times[best] - timestamp)
        ? candidate : best
    ));
    return {index, timestamp: times[index], distance: Math.abs(times[index] - timestamp)};
  }

  function tickToleranceMs(spec) {
    if (spec.unit === "minute") return spec.step * MINUTE_MS / 2;
    if (spec.unit === "hour") return spec.step * HOUR_MS / 2;
    if (spec.unit === "day") return spec.step * DAY_MS / 2;
    if (spec.unit === "week") return spec.step * 7 * DAY_MS / 2;
    if (spec.unit === "month") return spec.step * 31 * DAY_MS / 2;
    if (spec.unit === "year") return spec.step * 366 * DAY_MS / 2;
    return HOUR_MS / 2;
  }

  function mapTicksToObserved(ticks, observedTimes, spec) {
    const tolerance = tickToleranceMs(spec);
    const seen = new Set();
    const mapped = [];
    for (const tick of ticks) {
      const nearest = nearestObserved(observedTimes, tick.timestamp);
      const allowedDistance = tick.kind === "session"
        ? Math.min(tolerance, 30 * MINUTE_MS) : tolerance;
      if (nearest === null || nearest.distance > allowedDistance) continue;
      const key = `${tick.kind}:${nearest.index}`;
      if (seen.has(key)) continue;
      seen.add(key);
      mapped.push({
        ...tick,
        observedTimestamp: nearest.timestamp,
        observedIndex: nearest.index,
      });
    }
    return mapped;
  }

  function buildTimeAxis({range, timestamps, sessions = [], collapseEmptyIntervals = false}) {
    const times = finiteTimes(timestamps);
    if (!times.length) return null;
    let startMs = times[0];
    let endMs = times[times.length - 1];
    if (range in RANGE_MS) startMs = endMs - RANGE_MS[range];
    if (endMs <= startMs) endMs = startMs + 1;
    const spec = tickSpec(range, endMs - startMs);
    let ticks = regularTicks(startMs, endMs, spec);
    if (range === "1d" && sessions.length) {
      const activeDates = new Set(times.map(localDateKey));
      const important = sessionTicks(startMs, endMs, sessions, activeDates);
      ticks.push(...important);
      ticks.sort((left, right) => left.timestamp - right.timestamp);
    }
    if (!collapseEmptyIntervals) return {startMs, endMs, ticks};
    const observedTimes = [...new Set(
      times.filter((timestamp) => timestamp >= startMs && timestamp <= endMs),
    )];
    if (!observedTimes.length) return null;
    return {
      startMs,
      endMs,
      ticks: mapTicksToObserved(ticks, observedTimes, spec),
      observedTimes,
      collapseEmptyIntervals: true,
    };
  }

  function position(axis, timestamp, left, right) {
    const value = timestamp instanceof Date ? timestamp.getTime() : Number(timestamp);
    if (axis.collapseEmptyIntervals && Array.isArray(axis.observedTimes)) {
      if (axis.observedTimes.length === 1) return left + (right - left) / 2;
      const nearest = nearestObserved(axis.observedTimes, value);
      const ratio = nearest.index / (axis.observedTimes.length - 1);
      return left + ratio * (right - left);
    }
    const ratio = (value - axis.startMs) / (axis.endMs - axis.startMs);
    return left + ratio * (right - left);
  }

  const api = Object.freeze({buildTimeAxis, position});
  root.StockAgentTimeAxis = api;
  if (typeof module !== "undefined" && module.exports) module.exports = api;
})(globalThis);
