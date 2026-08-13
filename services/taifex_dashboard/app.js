"use strict";

const REFRESH_MS = 5000;
const money = new Intl.NumberFormat("zh-TW", { maximumFractionDigits: 0 });
const percent = new Intl.NumberFormat("zh-TW", { style: "percent", minimumFractionDigits: 2, maximumFractionDigits: 2 });
const ratio = new Intl.NumberFormat("zh-TW", { maximumFractionDigits: 2 });
let selectedStrategy = "";
let selectedCategory = "";
let selectedDirectionalExposure = "";
let selectedVolatilityExposure = "";
let selectedHedgeType = "";
let selectedSort = "fixed_capital_return";
let sortDescending = true;
let lastSnapshot = null;
let cachedHistory = [];

function byId(id) { return document.getElementById(id); }
function setText(id, value) { byId(id).textContent = value ?? "—"; }
function formatTwd(value) {
  if (value == null || !Number.isFinite(Number(value))) return "—";
  return `${money.format(Number(value))} TWD`;
}
function formatPercent(value) {
  if (value == null || !Number.isFinite(Number(value))) return "—";
  return percent.format(Number(value));
}
function formatRatio(value) {
  if (value == null || !Number.isFinite(Number(value))) return "—";
  return `${ratio.format(Number(value))}×`;
}
function localTime(value) {
  if (!value) return "—";
  return new Intl.DateTimeFormat("zh-TW", {
    month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit",
    hour12: false, timeZone: "Asia/Taipei"
  }).format(new Date(value));
}

function healthMessage(snapshot) {
  if (snapshot.health === "blocked") return `引擎已阻擋：${snapshot.blocked_reason || "原因未記錄"}`;
  if (snapshot.health === "stale") return `狀態已過期：最後更新距今 ${snapshot.source_age_seconds.toFixed(1)} 秒`;
  if (snapshot.health === "waiting" && !snapshot.source_fresh_expected) {
    return `排程監控中：${snapshot.current_market_phase} 為交易所休市空窗；下一個合法收單／撮合時段會自動恢復行情與策略。`;
  }
  if (snapshot.engine_status === "waiting_for_bootstrap") {
    return `正常等待：安全規則禁止補建已開始的週期，需等 ${snapshot.bootstrap_after_date} 結算後的新完整週期。`;
  }
  if (snapshot.health === "active") return "週期已開啟；策略、行情與理想帳持續更新。";
  return `引擎正常：${snapshot.engine_status}`;
}

function renderHeader(snapshot) {
  const dot = byId("live-dot");
  dot.className = "live-dot";
  const unhealthy = ["blocked", "stale"].includes(snapshot.health);
  dot.classList.add(unhealthy ? "blocked" : "active");
  setText("live-label", unhealthy ? snapshot.health.toUpperCase() : "LIVE");
  setText("last-refresh", `更新 ${localTime(snapshot.source_updated_at_utc)}`);
  const liveCount = snapshot.strategy_counts?.live_ideal ?? snapshot.strategies.length;
  setText("dashboard-title", `TAIFEX ${liveCount} 策略模擬交易即時面板`);
  setText("strategy-table-title", `${liveCount} 個獨立策略帳本`);
  const alert = byId("alert");
  alert.className = `alert ${unhealthy ? "blocked" : "ready"}`;
  alert.textContent = healthMessage(snapshot);
}

function renderMetrics(snapshot) {
  const market = snapshot.market;
  const cycle = snapshot.active_cycle;
  const broker = snapshot.broker;
  setText("engine-status", snapshot.engine_status);
  setText("blocked-reason", `blocked: ${snapshot.blocked_reason || "none"}`);
  setText("book-coverage", percent.format(market.book_coverage_ratio || 0));
  setText("book-count", `${market.latest_book_count} / ${market.expected_book_count} contracts`);
  setText("cycle-status", cycle ? "OPEN" : "FLAT");
  setText("cycle-expiry", cycle ? `expiry ${cycle.expiry_date || "—"}` : "目前無持倉週期");
  setText("broker-status", broker.order_failures === 0 ? "0 failures" : `${broker.order_failures} failures`);
  setText("broker-detail", `${broker.inflight_order_count} inflight · enabled=${broker.orders_enabled}`);
  setText("source-age", `${Number(snapshot.source_age_seconds).toFixed(1)} 秒`);
  const safe = snapshot.simulation_only && !snapshot.production_order_possible;
  setText("safety-state", safe ? "SIM ONLY" : "UNSAFE");
}

function renderCycle(snapshot) {
  const market = snapshot.market;
  const cycle = snapshot.active_cycle || {};
  setText("underlying-contract", market.underlying_contract);
  setText("hedge-contract", `${market.hedge_product || "—"} · ${market.hedge_contract || "—"} · ×${market.hedge_multiplier_twd_per_point || "—"}`);
  setText("option-risk-margins", `${formatTwd(market.option_risk_margin_a_twd)} / ${formatTwd(market.option_risk_margin_b_twd)}`);
  setText("capital-buffer-multiple", market.strategy_capital_buffer_multiple == null ? "—" : `×${market.strategy_capital_buffer_multiple}`);
  setText("cycle-series", cycle.series || "flat");
  setText("strategy-mode", snapshot.strategy_mode || "—");
  setText("catalog-entry-policy", snapshot.catalog_expansion_entry_policy || "next_cycle");
  setText("current-session", snapshot.current_market_phase || snapshot.current_session || "—");
  setText("runner-mode", snapshot.runner_mode || "—");
  setText("day-official-session", "08:30–08:45 盤前 · 08:45–13:45 連續");
  setText("intersession-gap", "13:45–14:50 休市監控");
  setText("night-official-session", "14:50–15:00 盤前 · 15:00–05:00 夜盤");
  setText("current-trading-date", snapshot.current_trading_date || "—");
  setText("decision-interval", snapshot.intraday_decision_interval_seconds == null ? "—" : `${snapshot.intraday_decision_interval_seconds} 秒`);
  setText("day-session-times", `${snapshot.intraday_entry_cutoff || "—"} / ${snapshot.intraday_flatten_time || "—"}`);
  setText("night-session-times", `${snapshot.night_entry_cutoff || "—"} / ${snapshot.night_flatten_time || "—"}`);
  setText("cycle-strike", cycle.strike == null ? "—" : money.format(cycle.strike));
  setText("detail-expiry", cycle.expiry_date || "—");
  setText("pending-targets", `${snapshot.pending_targets.length}`);
  setText("bootstrap-date", snapshot.bootstrap_after_date || "—");
  const receipt = snapshot.api_round_trip;
  setText("api-round-trip", receipt ? `${receipt.result} · ${receipt.logical_contract}→${receipt.resolved_contract} · final=${receipt.final_position} · ${localTime(receipt.finished_at_utc)}` : "尚無 receipt");
}

function renderStrategySelector(strategies) {
  const select = byId("strategy-select");
  const ids = strategies.map((row) => row.strategy_id);
  if (!selectedStrategy || !ids.includes(selectedStrategy)) selectedStrategy = ids[0] || "";
  if (select.options.length !== strategies.length) {
    select.replaceChildren();
    for (const row of strategies) {
      const option = document.createElement("option");
      option.value = row.strategy_id;
      option.textContent = row.label;
      select.appendChild(option);
    }
  }
  select.value = selectedStrategy;
}

function compareValues(left, right, key) {
  const a = left[key], b = right[key];
  if (a == null && b == null) return left.label.localeCompare(right.label, "zh-Hant");
  if (a == null) return 1;
  if (b == null) return -1;
  if (typeof a === "number" && typeof b === "number") return a - b;
  return String(a).localeCompare(String(b), "zh-Hant");
}

function sortedStrategies(strategies) {
  return [...strategies].sort((left, right) => {
    const compared = compareValues(left, right, selectedSort);
    return sortDescending ? -compared : compared;
  });
}

function matchesExposureFilters(row) {
  return (
    (!selectedDirectionalExposure || row.directional_exposure === selectedDirectionalExposure)
    && (!selectedVolatilityExposure || row.volatility_exposure === selectedVolatilityExposure)
    && (!selectedHedgeType || row.hedge_type === selectedHedgeType)
  );
}

function filteredStrategies(strategies) {
  return strategies.filter(matchesExposureFilters);
}

function populateFilter(id, rows, codeKey, labelKey, emptyLabel, selectedValue) {
  const select = byId(id);
  const labels = new Map();
  for (const row of rows) {
    if (row[codeKey]) labels.set(row[codeKey], row[labelKey] || row[codeKey]);
  }
  const options = [
    ["", emptyLabel],
    ...[...labels.entries()].sort((a, b) => a[1].localeCompare(b[1], "zh-Hant"))
  ];
  const signature = JSON.stringify(options);
  if (select.dataset.signature !== signature) {
    select.replaceChildren();
    for (const [value, label] of options) {
      const option = document.createElement("option");
      option.value = value;
      option.textContent = label;
      select.appendChild(option);
    }
    select.dataset.signature = signature;
  }
  select.value = labels.has(selectedValue) ? selectedValue : "";
}

function renderExposureControls(catalog) {
  populateFilter(
    "exposure-direction-filter", catalog, "directional_exposure",
    "directional_exposure_label", "全部方向曝險", selectedDirectionalExposure
  );
  populateFilter(
    "exposure-volatility-filter", catalog, "volatility_exposure",
    "volatility_exposure_label", "全部波動曝險", selectedVolatilityExposure
  );
  populateFilter(
    "exposure-hedge-filter", catalog, "hedge_type",
    "hedge_type_label", "全部避險型態", selectedHedgeType
  );
}

function renderExposureSummary(summary) {
  const grid = byId("exposure-summary");
  grid.replaceChildren();
  const dimensions = [
    ["directional_exposure", "方向曝險"],
    ["volatility_exposure", "波動曝險"],
    ["hedge_type", "避險型態"]
  ];
  for (const [key, label] of dimensions) {
    const block = document.createElement("div");
    const title = document.createElement("strong");
    title.textContent = label;
    const values = document.createElement("p");
    const rows = summary?.live?.[key] || [];
    values.textContent = rows.length
      ? rows.map((row) => `${row.label} ${row.count}`).join(" · ")
      : "尚無即時策略";
    block.append(title, values);
    grid.appendChild(block);
  }
}

function renderPerformanceSummary(summary) {
  summary = summary || {};
  setText("performance-count", `${summary.valid_return_count ?? 0} / ${summary.strategy_count ?? 0}`);
  setText("performance-reserve", formatTwd(summary.independent_strategy_reserved_capital_twd));
  setText("performance-median", formatPercent(summary.median_fixed_capital_return));
  const best = summary.best_strategy;
  const worst = summary.worst_strategy;
  setText("performance-best", best ? formatPercent(best.fixed_capital_return) : "—");
  setText("performance-best-name", best?.label || "—");
  setText("performance-worst", worst ? formatPercent(worst.fixed_capital_return) : "—");
  setText("performance-worst-name", worst?.label || "—");
  setText("performance-cost", formatTwd(summary.independent_strategy_explicit_cost_twd));
}

function renderTable(strategies) {
  const body = byId("strategy-body");
  body.replaceChildren();
  const visible = filteredStrategies(strategies);
  setText("strategy-table-title", `${visible.length} / ${strategies.length} 個獨立策略帳本`);
  for (const row of sortedStrategies(visible)) {
    const tr = document.createElement("tr");
    const title = document.createElement("td");
    title.textContent = row.label;
    const id = document.createElement("span");
    id.className = "strategy-id";
    id.textContent = `${row.strategy_id} · ${row.implementation_level}`;
    title.appendChild(id);
    tr.appendChild(title);
    const category = document.createElement("td");
    category.textContent = row.category;
    tr.appendChild(category);
    for (const value of [
      row.directional_exposure_label,
      row.volatility_exposure_label,
      row.hedge_type_label
    ]) {
      const td = document.createElement("td");
      td.className = "exposure-cell";
      td.textContent = value || "—";
      tr.appendChild(td);
    }
    const exposureRatio = document.createElement("td");
    exposureRatio.className = "exposure-ratio-cell";
    const designRatio = document.createElement("span");
    designRatio.textContent = `設計 ${row.design_option_ratio_label || "—"}`;
    const liveRatio = document.createElement("span");
    liveRatio.textContent = `實際 ${row.live_option_ratio_label || "—"}`;
    exposureRatio.append(designRatio, liveRatio);
    tr.appendChild(exposureRatio);
    const values = [
      formatTwd(row.reserved_capital_twd),
      formatTwd(row.one_unit_net_pnl_twd),
      formatTwd(row.one_unit_net_pnl_abs_twd),
      formatPercent(row.fixed_capital_return),
      formatPercent(row.compounded_return_to_live_mark),
      formatTwd(row.explicit_cost_twd),
      formatRatio(row.net_pnl_to_explicit_cost_ratio),
      formatTwd(row.margin_required_twd),
      formatPercent(row.margin_utilization),
      formatTwd(row.total_equity_twd),
      formatTwd(row.margin_excess_twd),
      formatTwd(row.open_liquidation_value_twd),
      money.format(row.option_position_count),
      money.format(row.futures_position)
    ];
    for (const value of values) {
      const td = document.createElement("td"); td.textContent = value; tr.appendChild(td);
    }
    const valid = document.createElement("td");
    const pill = document.createElement("span");
    if (!row.alive) {
      pill.className = "pill invalid";
      pill.textContent = "RUINED";
      pill.title = `absorbing ruin · margin calls=${row.margin_call_count}`;
    } else if (row.forced_liquidation_pending || Number(row.margin_excess_twd) < 0) {
      pill.className = "pill invalid";
      pill.textContent = "MARGIN";
      pill.title = `保證金不足；margin calls=${row.margin_call_count}`;
    } else if (!row.valuation_available) {
      pill.className = "pill invalid";
      pill.textContent = "UNAVAILABLE";
    } else if (row.valuation_stale) {
      pill.className = "pill stale";
      pill.textContent = "CARRIED";
      pill.title = `缺報價，延用上一筆完整估值；age=${Number(row.valuation_age_seconds || 0).toFixed(1)}s`;
    } else {
      pill.className = "pill valid";
      pill.textContent = "FRESH";
    }
    valid.appendChild(pill); tr.appendChild(valid); body.appendChild(tr);
  }
}

function appendGuideField(card, label, value) {
  const block = document.createElement("div");
  const term = document.createElement("strong");
  term.textContent = label;
  const detail = document.createElement("p");
  detail.textContent = value || "—";
  block.append(term, detail);
  card.appendChild(block);
}

function renderStrategyGuide(catalog, counts) {
  catalog = Array.isArray(catalog) ? catalog : [];
  counts = counts || {
    live_ideal: catalog.filter((row) => row.availability !== "blocked_contract").length,
    blocked_contract: catalog.filter((row) => row.availability === "blocked_contract").length,
    catalog_total: catalog.length
  };
  const filter = byId("strategy-category-filter");
  const categories = [...new Set(catalog.map((row) => row.category))].sort();
  const expected = ["", ...categories];
  if (filter.options.length !== expected.length) {
    filter.replaceChildren();
    for (const category of expected) {
      const option = document.createElement("option");
      option.value = category;
      option.textContent = category || "全部分類";
      filter.appendChild(option);
    }
  }
  filter.value = selectedCategory;
  const rows = catalog.filter((row) => (
    (!selectedCategory || row.category === selectedCategory)
    && matchesExposureFilters(row)
  ));
  const grid = byId("strategy-guide-grid");
  grid.replaceChildren();
  for (const row of rows) {
    const card = document.createElement("article");
    card.className = "strategy-card";
    const head = document.createElement("div");
    head.className = "strategy-card-head";
    const title = document.createElement("h3");
    title.textContent = row.label;
    const badge = document.createElement("span");
    badge.className = `pill ${row.availability === "live_ideal" ? "valid" : "stale"}`;
    badge.textContent = row.availability === "live_ideal" ? "LIVE CURVE" : "CONTRACT GAP";
    head.append(title, badge);
    const meta = document.createElement("p");
    meta.className = "strategy-card-meta";
    meta.textContent = `${row.category} · ${row.family} · ${row.broker_monitoring}`;
    card.append(head, meta);
    const exposureTags = document.createElement("div");
    exposureTags.className = "exposure-tags";
    for (const value of [
      row.directional_exposure_label,
      row.volatility_exposure_label,
      row.hedge_type_label
    ]) {
      const tag = document.createElement("span");
      tag.textContent = value || "未分類";
      exposureTags.appendChild(tag);
    }
    card.appendChild(exposureTags);
    appendGuideField(card, "設計選擇權多空口數比", row.design_option_ratio_label);
    if (row.design_futures_target_index_equivalent != null) {
      const futureTarget = Number(row.design_futures_target_index_equivalent);
      const direction = futureTarget >= 0 ? "多" : "空";
      appendGuideField(
        card,
        "固定期貨目標",
        `${direction} ${Math.abs(futureTarget).toFixed(2)} 指數等價`
      );
    }
    appendGuideField(card, "核心做法", row.summary);
    appendGuideField(card, "進場", row.entry_rule);
    appendGuideField(card, "出場", row.exit_rule);
    appendGuideField(card, "風險與帳務", row.risk_note);
    if (row.blocker) appendGuideField(card, "尚缺契約", row.blocker);
    grid.appendChild(card);
  }
  setText(
    "strategy-guide-summary",
    `目前顯示 ${rows.length} / ${counts.catalog_total}；${counts.live_ideal} 個實際理想帳曲線 · ${counts.blocked_contract} 個契約缺口 fail-closed`
  );
}

function svgNode(name, attrs = {}, text = "") {
  const node = document.createElementNS("http://www.w3.org/2000/svg", name);
  for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, String(value));
  if (text) node.textContent = text;
  return node;
}

function renderCurveWall(strategies, history) {
  const grid = byId("curve-wall-grid");
  grid.replaceChildren();
  const visible = filteredStrategies(strategies);
  const byStrategy = new Map(visible.map((row) => [row.strategy_id, []]));
  for (const point of history) {
    const value = Number(point.fixed_capital_return);
    if (byStrategy.has(point.strategy_id) && Number.isFinite(value)) {
      byStrategy.get(point.strategy_id).push(value);
    }
  }
  const allValues = [...byStrategy.values()].flat();
  let minY = Math.min(0, ...allValues), maxY = Math.max(0, ...allValues);
  if (!Number.isFinite(minY) || !Number.isFinite(maxY) || minY === maxY) {
    minY = -0.001; maxY = 0.001;
  }
  const range = maxY - minY;
  minY -= range * 0.06;
  maxY += range * 0.06;
  const width = 240, height = 76, inset = 6;
  const y = (value) => inset + (maxY - value) * (height - inset * 2) / (maxY - minY);
  for (const row of sortedStrategies(visible)) {
    const values = byStrategy.get(row.strategy_id) || [];
    const card = document.createElement("button");
    card.type = "button";
    card.className = "curve-card";
    card.title = `切換上方大圖：${row.label}`;
    card.addEventListener("click", () => {
      selectedStrategy = row.strategy_id;
      byId("strategy-select").value = selectedStrategy;
      if (lastSnapshot) renderChart(lastSnapshot.history);
      byId("equity-chart").scrollIntoView({ behavior: "smooth", block: "center" });
    });
    const head = document.createElement("span");
    head.className = "curve-card-head";
    const title = document.createElement("strong");
    title.textContent = row.label;
    const current = document.createElement("b");
    current.className = Number(row.fixed_capital_return) >= 0 ? "positive" : "negative";
    current.textContent = formatPercent(row.fixed_capital_return);
    head.append(title, current);
    const meta = document.createElement("span");
    meta.className = "curve-card-meta";
    meta.textContent = `${row.directional_exposure_label} · ${row.volatility_exposure_label} · ${values.length} 點`;
    meta.title = `${row.hedge_type_label} · 設計 ${row.design_option_ratio_label} · 實際 ${row.live_exposure_label}`;
    const svg = svgNode("svg", { viewBox: `0 0 ${width} ${height}`, "aria-hidden": "true" });
    svg.appendChild(svgNode("line", { x1: inset, y1: y(0), x2: width - inset, y2: y(0), class: "mini-zero" }));
    if (values.length >= 2) {
      const x = (index) => inset + index * (width - inset * 2) / (values.length - 1);
      const points = values.map((value, index) => `${x(index).toFixed(2)},${y(value).toFixed(2)}`).join(" ");
      svg.appendChild(svgNode("polyline", {
        points,
        class: Number(values.at(-1)) >= 0 ? "mini-line positive" : "mini-line negative"
      }));
    }
    card.append(head, meta, svg);
    grid.appendChild(card);
  }
  setText(
    "curve-wall-note",
    `顯示 ${visible.length} / ${strategies.length} 條曲線；共用 Y 軸（${formatPercent(minY)} ～ ${formatPercent(maxY)}），以固定預留資金正規化；排序與下表一致。`
  );
}

function renderChart(history) {
  const svg = byId("equity-chart");
  svg.replaceChildren();
  const rows = history.filter((row) => (
    row.strategy_id === selectedStrategy && row.decision_ts_ns > 0
    && row.total_equity_twd != null
    && Number.isFinite(Number(row.total_equity_twd))
  ));
  if (rows.length < 2) {
    setText("chart-note", "資料點不足；至少累積兩個每分鐘 mark 後才繪圖。");
    return;
  }
  const width = 900, height = 300, left = 76, right = 22, top = 22, bottom = 42;
  const values = rows.map((row) => Number(row.total_equity_twd));
  const baseline = Number(rows.at(-1).initial_capital_twd);
  const bounds = Number.isFinite(baseline) ? [...values, baseline] : values;
  let minY = Math.min(...bounds), maxY = Math.max(...bounds);
  if (minY === maxY) { minY -= 1; maxY += 1; }
  const pad = Math.max((maxY - minY) * 0.08, 1);
  minY -= pad; maxY += pad;
  const x = (index) => left + index * (width - left - right) / (rows.length - 1);
  const y = (value) => top + (maxY - value) * (height - top - bottom) / (maxY - minY);
  for (let i = 0; i <= 4; i += 1) {
    const value = minY + (maxY - minY) * i / 4;
    const yPos = y(value);
    svg.appendChild(svgNode("line", { x1: left, y1: yPos, x2: width - right, y2: yPos, class: "chart-grid" }));
    svg.appendChild(svgNode("text", { x: left - 10, y: yPos + 4, "text-anchor": "end", class: "chart-label" }, money.format(value)));
  }
  if (Number.isFinite(baseline)) svg.appendChild(svgNode("line", { x1: left, y1: y(baseline), x2: width - right, y2: y(baseline), class: "chart-baseline" }));
  const points = rows.map((row, index) => `${x(index).toFixed(2)},${y(Number(row.total_equity_twd)).toFixed(2)}`).join(" ");
  svg.appendChild(svgNode("polyline", { points, class: "chart-line" }));
  const first = new Date(Number(rows[0].decision_ts_ns) / 1e6);
  const last = new Date(Number(rows[rows.length - 1].decision_ts_ns) / 1e6);
  svg.appendChild(svgNode("text", { x: left, y: height - 14, class: "chart-label" }, localTime(first.toISOString()).slice(6)));
  svg.appendChild(svgNode("text", { x: width - right, y: height - 14, "text-anchor": "end", class: "chart-label" }, localTime(last.toISOString()).slice(6)));
  const changed = Math.max(...values) !== Math.min(...values);
  const carried = rows.filter((row) => row.valuation_carried_forward).length;
  const pnl = rows.at(-1).cumulative_pnl_twd;
  setText("chart-note", changed
    ? `${rows.length} 個每分鐘點；${carried} 點延用上一筆完整估值；最後總權益 ${formatTwd(values.at(-1))}，累積損益 ${formatTwd(pnl)}。`
    : `${rows.length} 個每分鐘點；${carried} 點延用上一筆完整估值；總權益維持 ${formatTwd(values.at(-1))}。`);
}

function renderCounts(counts) {
  setText("count-trades", money.format(counts.ideal_trades));
  setText("count-marks", money.format(counts.marks));
  setText("count-calibrations", money.format(counts.calibrations));
  setText("count-events", money.format(counts.events));
}

function render(snapshot) {
  snapshot.history = cachedHistory;
  lastSnapshot = snapshot;
  renderHeader(snapshot); renderMetrics(snapshot); renderCycle(snapshot);
  renderPerformanceSummary(snapshot.portfolio_summary);
  const compatibilityCatalog = snapshot.strategy_catalog || snapshot.strategies.map((row) => ({
    strategy_id: row.strategy_id,
    label: row.label,
    family: row.family || "legacy_live_strategy",
    category: row.category || "既有即時策略",
    summary: row.summary || "既有即時策略帳本；完整說明會在下一個自然 capture 週期載入。",
    entry_rule: row.entry_rule || "沿用目前 active cycle 的既有因果進場契約。",
    exit_rule: row.exit_rule || "沿用目前 active cycle 的既有出場契約。",
    risk_note: row.risk_note || "目前為新舊 schema 自然切換期間。",
    broker_monitoring: row.broker_monitoring || "legacy_runtime",
    directional_exposure: row.directional_exposure || "adaptive",
    directional_exposure_label: row.directional_exposure_label || "動態／訊號決定",
    volatility_exposure: row.volatility_exposure || "signal_dependent",
    volatility_exposure_label: row.volatility_exposure_label || "多空波動由訊號決定",
    hedge_type: row.hedge_type || "contract_pending",
    hedge_type_label: row.hedge_type_label || "執行契約待完成",
    design_option_ratio_label: row.design_option_ratio_label || "尚未載入",
    availability: "live_ideal"
  }));
  renderExposureControls(compatibilityCatalog);
  renderExposureSummary(snapshot.exposure_summary);
  renderStrategySelector(snapshot.strategies);
  renderChart(snapshot.history);
  renderCurveWall(snapshot.strategies, snapshot.history);
  renderTable(snapshot.strategies);
  renderCounts(snapshot.record_counts);
  const compatibilityCounts = snapshot.strategy_counts || {
    live_ideal: snapshot.strategies.length,
    blocked_contract: 0,
    catalog_total: compatibilityCatalog.length
  };
  renderStrategyGuide(compatibilityCatalog, compatibilityCounts);
}

async function refresh() {
  try {
    const response = await fetch("api/status", { cache: "no-cache" });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    render(payload);
  } catch (error) {
    const alert = byId("alert");
    alert.className = "alert blocked";
    alert.textContent = `面板資料讀取失敗：${error.message}`;
    byId("live-dot").className = "live-dot blocked";
    setText("live-label", "OFFLINE");
  }
}

async function refreshHistory() {
  try {
    const response = await fetch("api/history", { cache: "no-cache" });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    cachedHistory = Array.isArray(payload.history) ? payload.history : [];
    if (lastSnapshot) {
      lastSnapshot.history = cachedHistory;
      renderChart(cachedHistory);
      renderCurveWall(lastSnapshot.strategies, cachedHistory);
      renderCounts(payload.record_counts || lastSnapshot.record_counts);
    }
  } catch (error) {
    setText("curve-wall-note", `曲線歷史暫時無法更新：${error.message}`);
  }
}

byId("strategy-select").addEventListener("change", (event) => {
  selectedStrategy = event.target.value;
  if (lastSnapshot) renderChart(lastSnapshot.history);
});
byId("strategy-category-filter").addEventListener("change", (event) => {
  selectedCategory = event.target.value;
  if (lastSnapshot) render(lastSnapshot);
});
byId("exposure-direction-filter").addEventListener("change", (event) => {
  selectedDirectionalExposure = event.target.value;
  if (lastSnapshot) render(lastSnapshot);
});
byId("exposure-volatility-filter").addEventListener("change", (event) => {
  selectedVolatilityExposure = event.target.value;
  if (lastSnapshot) render(lastSnapshot);
});
byId("exposure-hedge-filter").addEventListener("change", (event) => {
  selectedHedgeType = event.target.value;
  if (lastSnapshot) render(lastSnapshot);
});
byId("exposure-filter-reset").addEventListener("click", () => {
  selectedDirectionalExposure = "";
  selectedVolatilityExposure = "";
  selectedHedgeType = "";
  if (lastSnapshot) render(lastSnapshot);
});
byId("strategy-sort").addEventListener("change", (event) => {
  selectedSort = event.target.value;
  if (lastSnapshot) {
    renderCurveWall(lastSnapshot.strategies, lastSnapshot.history);
    renderTable(lastSnapshot.strategies);
  }
});
byId("sort-direction").addEventListener("click", () => {
  sortDescending = !sortDescending;
  setText("sort-direction", sortDescending ? "由高到低 ↓" : "由低到高 ↑");
  if (lastSnapshot) {
    renderCurveWall(lastSnapshot.strategies, lastSnapshot.history);
    renderTable(lastSnapshot.strategies);
  }
});
refresh();
refreshHistory();
window.setInterval(refresh, REFRESH_MS);
window.setInterval(refreshHistory, 60000);
