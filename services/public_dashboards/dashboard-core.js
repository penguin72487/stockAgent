"use strict";

/*
 * Shared, dependency-free primitives for every public StockAgent dashboard.
 *
 * The public pages deliberately keep their market-specific renderers separate:
 * a TAIFEX position and a data-pipeline row do not share a semantic contract.
 * Navigation, request cancellation, formatting, safe text interpolation and
 * refresh scheduling are cross-cutting concerns and belong here.
 */
(function installStockAgentDashboard(global) {
  const DEFAULT_TIMEOUT_MS = 15000;
  const LOCALE = "zh-TW";
  const BYTE_UNITS = Object.freeze(["B", "KiB", "MiB", "GiB", "TiB", "PiB"]);
  const NAV_ITEMS = Object.freeze([
    Object.freeze({id: "overview", label: "總覽", slug: ""}),
    Object.freeze({id: "taifex", label: "TAIFEX", slug: "taifex"}),
    Object.freeze({id: "tw-day-trade", label: "台股當沖", slug: "tw-day-trade"}),
    Object.freeze({id: "tw-overnight", label: "隔日沖", slug: "tw-overnight"}),
    Object.freeze({id: "shioaji", label: "永豐 API", slug: "shioaji"}),
    Object.freeze({id: "finlab", label: "FinLab", slug: "finlab"}),
    Object.freeze({id: "finmind", label: "FinMind", slug: "finmind"}),
    Object.freeze({id: "openbb", label: "OpenBB", slug: "openbb"}),
    Object.freeze({id: "data-monitor", label: "全資料", slug: "data-monitor"}),
    Object.freeze({id: "traffic", label: "流量", slug: "traffic"}),
  ]);
  const HTML_ESCAPE = Object.freeze({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  });
  const formatterCache = new Map();
  const trustedHtmlCache = new WeakMap();
  const responseLifetimes = new WeakMap();
  const requestTimings = [];
  const PERFORMANCE_SCHEMA_VERSION = 1;
  const PERFORMANCE_STORAGE_KEY = "stockagent-dashboard-performance-v1";
  const PERFORMANCE_HISTORY_LIMIT = 256;
  const PERFORMANCE_SERIES_LIMIT = 32;
  const PERFORMANCE_STORAGE_MAX_BYTES = 256 * 1024;
  const INTERACTION_CONTEXT_MS = 750;
  const performanceHistory = [];
  const pendingInputMeasurements = new WeakMap();
  let performancePersistHandle = null;
  let activeInteraction = null;
  let interactionSequence = 0;
  const now = () => global.performance?.now?.() ?? Date.now();

  function currentRoute() {
    const pathname = String(global.location?.pathname || "/").split(/[?#]/, 1)[0];
    return pathname.startsWith("/") ? pathname.slice(0, 160) : "/";
  }

  function metricPath(value, fallback = "/") {
    const candidate = String(value ?? fallback).split(/[?#]/, 1)[0];
    return (candidate.startsWith("/") ? candidate : fallback).slice(0, 160);
  }

  function metricIdentifier(value, fallback = "unknown") {
    const text = String(value ?? "").trim();
    if (!text || !/^[A-Za-z0-9_.:/@\[\]-]+$/.test(text)) return fallback;
    return text.slice(0, 120);
  }

  function metricNumber(value, maximum = 600000) {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed < 0) return null;
    return Math.round(Math.min(parsed, maximum) * 1000) / 1000;
  }

  function sanitizePerformanceMetric(row) {
    if (!row || typeof row !== "object") return null;
    const kind = ["api", "interaction", "page_load", "render"].includes(row.kind)
      ? row.kind : null;
    if (!kind) return null;
    const observedAt = Number(row.observedAt);
    const metric = {
      schemaVersion: PERFORMANCE_SCHEMA_VERSION,
      observedAt: Number.isFinite(observedAt) && observedAt > 0
        ? Math.round(observedAt) : Date.now(),
      route: metricPath(row.route, "/"),
      kind,
      action: metricIdentifier(row.action),
    };
    const eventType = metricIdentifier(row.eventType, "");
    const requestPath = metricPath(row.requestPath, "");
    const outcome = metricIdentifier(row.outcome, "");
    if (eventType) metric.eventType = eventType;
    if (requestPath) metric.requestPath = requestPath;
    if (outcome) metric.outcome = outcome;
    const numericFields = [
      "durationMs", "inputDelayMs", "headersMs", "bodyMs", "parseMs",
      "serverMs", "paintMs", "domContentLoadedMs", "loadMs", "fcpMs",
      "lcpMs", "viewportWidth", "viewportHeight",
      "prepareMs", "drawMs", "pointCount", "seriesCount",
    ];
    for (const field of numericFields) {
      const parsed = metricNumber(row[field]);
      if (parsed != null) metric[field] = parsed;
    }
    const status = Number(row.status);
    if (Number.isInteger(status) && status >= 100 && status <= 599) {
      metric.status = status;
    }
    return Object.freeze(metric);
  }

  function performanceSeriesKey(row) {
    return [row.route, row.kind, row.action, row.eventType || "", row.requestPath || ""].join("|");
  }

  function appendPerformanceMetric(row, {notify = true, persist = true} = {}) {
    const metric = sanitizePerformanceMetric(row);
    if (!metric) return null;
    const seriesKey = performanceSeriesKey(metric);
    const matching = [];
    performanceHistory.forEach((candidate, index) => {
      if (performanceSeriesKey(candidate) === seriesKey) matching.push(index);
    });
    if (matching.length >= PERFORMANCE_SERIES_LIMIT) {
      performanceHistory.splice(matching[0], 1);
    }
    performanceHistory.push(metric);
    if (performanceHistory.length > PERFORMANCE_HISTORY_LIMIT) performanceHistory.shift();
    if (persist) schedulePerformancePersist();
    if (notify && global.document?.dispatchEvent && typeof global.CustomEvent === "function") {
      global.document.dispatchEvent(new global.CustomEvent(
        "stockagent-performance-recorded",
        {detail: {...metric}},
      ));
    }
    return metric;
  }

  function performanceStorage() {
    try { return global.localStorage || null; }
    catch (_error) { return null; }
  }

  function parseStoredPerformance(value) {
    if (typeof value !== "string" || value.length > PERFORMANCE_STORAGE_MAX_BYTES) return [];
    try {
      const decoded = JSON.parse(value);
      if (decoded?.schemaVersion !== PERFORMANCE_SCHEMA_VERSION || !Array.isArray(decoded.records)) return [];
      return decoded.records.slice(-PERFORMANCE_HISTORY_LIMIT);
    } catch (_error) {
      return [];
    }
  }

  function loadPerformanceHistory(value = null) {
    const storage = performanceStorage();
    let serialized = value;
    if (serialized === null && storage) {
      try { serialized = storage.getItem(PERFORMANCE_STORAGE_KEY); }
      catch (_error) { serialized = null; }
    }
    performanceHistory.length = 0;
    for (const row of parseStoredPerformance(serialized)) {
      appendPerformanceMetric(row, {notify: false, persist: false});
    }
  }

  function persistPerformanceHistory() {
    performancePersistHandle = null;
    const storage = performanceStorage();
    if (!storage) return;
    const envelope = {
      schemaVersion: PERFORMANCE_SCHEMA_VERSION,
      records: performanceHistory,
    };
    try {
      const serialized = JSON.stringify(envelope);
      if (serialized.length <= PERFORMANCE_STORAGE_MAX_BYTES) {
        storage.setItem(PERFORMANCE_STORAGE_KEY, serialized);
      }
    } catch (_error) {
      // Storage can be disabled or full. Measurements remain available in memory.
    }
  }

  function schedulePerformancePersist() {
    if (!performanceStorage() || performancePersistHandle !== null) return;
    if (typeof global.requestIdleCallback === "function") {
      performancePersistHandle = global.requestIdleCallback(
        persistPerformanceHistory,
        {timeout: 1000},
      );
      return;
    }
    performancePersistHandle = global.setTimeout(persistPerformanceHistory, 200);
  }

  function performanceHistorySnapshot() {
    return performanceHistory.map((row) => ({...row}));
  }

  function clearPerformanceHistory() {
    performanceHistory.length = 0;
    const storage = performanceStorage();
    try { storage?.removeItem(PERFORMANCE_STORAGE_KEY); }
    catch (_error) { /* Browser-local storage is optional. */ }
    if (global.document?.dispatchEvent && typeof global.CustomEvent === "function") {
      global.document.dispatchEvent(new global.CustomEvent("stockagent-performance-cleared"));
    }
  }

  function recordPerformanceMetric(row) {
    return appendPerformanceMetric({...row, route: row?.route || currentRoute()});
  }

  function afterNextPaint(callback) {
    if (typeof global.requestAnimationFrame !== "function" || global.document?.hidden) {
      global.setTimeout(callback, 0);
      return;
    }
    global.requestAnimationFrame(() => global.requestAnimationFrame(callback));
  }

  function activeInteractionContext(at = now()) {
    if (!activeInteraction || at - activeInteraction.started > INTERACTION_CONTEXT_MS) return null;
    return {...activeInteraction};
  }

  function safeElementAction(target) {
    if (!target || typeof target.closest !== "function") return null;
    if (target.closest("[data-performance-ignore]")) return null;
    const element = target.closest(
      "[data-performance-action],button,a,input,select,textarea,summary,[role='button'],[role='tab']",
    );
    if (!element) return null;
    const tag = String(element.tagName || "control").toLowerCase();
    const explicit = metricIdentifier(element.dataset?.performanceAction, "");
    if (explicit) return explicit;
    const id = metricIdentifier(element.id, "");
    if (id) return `${tag}:${id}`;
    for (const key of ["range", "filter", "series"]) {
      const value = metricIdentifier(element.dataset?.[key], "");
      if (value) return `${tag}[${key}:${value}]`;
    }
    if (tag === "a") {
      try {
        const path = new global.URL(element.href, global.location?.href).pathname;
        return `navigate:${metricPath(path)}`;
      } catch (_error) { return "navigate"; }
    }
    const name = metricIdentifier(element.getAttribute?.("name"), "");
    return name ? `${tag}[name:${name}]` : tag;
  }

  function eventInputDelay(event, receivedAt) {
    const timestamp = Number(event?.timeStamp);
    if (!Number.isFinite(timestamp)) return null;
    if (Math.abs(timestamp - receivedAt) < 60000) {
      return metricNumber(Math.max(0, receivedAt - timestamp), 60000);
    }
    const timeOrigin = Number(global.performance?.timeOrigin);
    if (Number.isFinite(timeOrigin) && Math.abs(timestamp - timeOrigin - receivedAt) < 60000) {
      return metricNumber(Math.max(0, receivedAt - (timestamp - timeOrigin)), 60000);
    }
    return null;
  }

  function beginInteractionMeasurement(event, {debounced = false} = {}) {
    const action = safeElementAction(event?.target);
    if (!action) return;
    const started = now();
    const context = {
      id: ++interactionSequence,
      action,
      eventType: debounced ? "input_settle" : String(event.type || "interaction"),
      started,
      inputDelayMs: eventInputDelay(event, started),
    };
    activeInteraction = context;
    const finish = () => afterNextPaint(() => appendPerformanceMetric({
      observedAt: Date.now(),
      route: currentRoute(),
      kind: "interaction",
      action: context.action,
      eventType: context.eventType,
      durationMs: now() - context.started,
      inputDelayMs: context.inputDelayMs,
      viewportWidth: global.innerWidth,
      viewportHeight: global.innerHeight,
    }));
    if (!debounced) {
      finish();
      return;
    }
    const prior = pendingInputMeasurements.get(event.target);
    if (prior) global.clearTimeout(prior);
    const timer = global.setTimeout(() => {
      pendingInputMeasurements.delete(event.target);
      finish();
    }, 200);
    pendingInputMeasurements.set(event.target, timer);
  }

  function installInteractionMeasurements() {
    if (!global.document?.addEventListener) return;
    global.document.addEventListener("click", (event) => beginInteractionMeasurement(event), true);
    global.document.addEventListener("change", (event) => {
      const prior = pendingInputMeasurements.get(event.target);
      if (prior) {
        global.clearTimeout(prior);
        pendingInputMeasurements.delete(event.target);
      }
      beginInteractionMeasurement(event);
    }, true);
    global.document.addEventListener("input", (event) => {
      beginInteractionMeasurement(event, {debounced: true});
    }, true);
  }

  function installPageLoadMeasurement() {
    if (!global.performance) return;
    let lastLcp = null;
    let observer = null;
    try {
      if (typeof global.PerformanceObserver === "function") {
        observer = new global.PerformanceObserver((list) => {
          const entries = list.getEntries();
          if (entries.length) lastLcp = entries.at(-1).startTime;
        });
        observer.observe({type: "largest-contentful-paint", buffered: true});
      }
    } catch (_error) { observer = null; }
    let recorded = false;
    const record = () => {
      if (recorded) return;
      recorded = true;
      afterNextPaint(() => {
        const navigation = global.performance.getEntriesByType?.("navigation")?.[0];
        const fcp = global.performance.getEntriesByName?.("first-contentful-paint")?.[0];
        const loadMs = metricNumber(navigation?.loadEventEnd || now());
        appendPerformanceMetric({
          observedAt: Date.now(),
          route: currentRoute(),
          kind: "page_load",
          action: "page_load",
          durationMs: loadMs,
          domContentLoadedMs: navigation?.domContentLoadedEventEnd,
          loadMs,
          fcpMs: fcp?.startTime,
          lcpMs: lastLcp,
          paintMs: lastLcp ?? fcp?.startTime ?? loadMs,
          viewportWidth: global.innerWidth,
          viewportHeight: global.innerHeight,
        });
        observer?.disconnect();
      });
    };
    if (global.document?.readyState === "complete") global.setTimeout(record, 0);
    else global.addEventListener?.("load", record, {once: true});
  }

  function byId(id) {
    return global.document?.getElementById(String(id)) || null;
  }

  function finiteNumber(value, fallback = null) {
    if (value == null || value === "") return fallback;
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  function numberFormatter(options = {}) {
    const key = JSON.stringify(options);
    if (!formatterCache.has(key)) {
      formatterCache.set(key, new Intl.NumberFormat(LOCALE, options));
    }
    return formatterCache.get(key);
  }

  function formatNumber(value, options = {}) {
    const parsed = finiteNumber(value);
    if (parsed == null) return options.fallback ?? "—";
    const {fallback: _fallback, normalizeTiny = true, ...intlOptions} = options;
    const maximumFractionDigits = Math.min(2, Math.max(0, Number(
      intlOptions.maximumFractionDigits ?? 2,
    )));
    const normalized = normalizeTiny && Math.abs(parsed) < 0.005 ? 0 : parsed;
    return numberFormatter({...intlOptions, maximumFractionDigits}).format(normalized);
  }

  function formatBytes(value, options = {}) {
    const parsed = finiteNumber(value);
    if (parsed == null) return options.fallback ?? "—";
    const sign = parsed < 0 ? "−" : options.showPositive && parsed > 0 ? "+" : "";
    let amount = Math.abs(parsed);
    let unit = 0;
    while (amount >= 1024 && unit < BYTE_UNITS.length - 1) {
      amount /= 1024;
      unit += 1;
    }
    const maximumFractionDigits = options.maximumFractionDigits
      ?? (amount >= 100 ? 0 : amount >= 10 ? 1 : 2);
    return `${sign}${formatNumber(amount, {maximumFractionDigits})} ${BYTE_UNITS[unit]}`;
  }

  function formatAge(seconds, options = {}) {
    const value = finiteNumber(seconds);
    const emptyLabel = options.emptyLabel ?? "無更新時間";
    if (value == null) return emptyLabel;
    const normalized = Math.max(0, value);
    const suffix = options.suffix === false ? "" : "前";
    if (normalized < 60) return `${Math.round(normalized)} 秒${suffix}`;
    if (normalized < 3600) return `${Math.round(normalized / 60)} 分鐘${suffix}`;
    if (normalized < 86400) return `${formatNumber(normalized / 3600, {maximumFractionDigits: options.hourDigits ?? 1})} 小時${suffix}`;
    return `${formatNumber(normalized / 86400, {maximumFractionDigits: options.dayDigits ?? 1})} 天${suffix}`;
  }

  function escapeHtml(value, fallback = "—") {
    return String(value ?? fallback).replace(/[&<>"']/g, (character) => HTML_ESCAPE[character]);
  }

  function setText(target, value, fallback = "—") {
    const node = typeof target === "string" ? byId(target) : target;
    if (!node) return false;
    const next = value == null || value === "" ? fallback : String(value);
    if (node.textContent === next) return false;
    node.textContent = next;
    return true;
  }

  /* Callers must escape every user/source string before passing markup here. */
  function setTrustedHtml(target, markup) {
    const node = typeof target === "string" ? byId(target) : target;
    if (!node) return false;
    const next = String(markup ?? "");
    if (trustedHtmlCache.get(node) === next) return false;
    node.innerHTML = next;
    trustedHtmlCache.set(node, next);
    return true;
  }

  function sameOriginUrl(input) {
    if (!global.location || typeof global.URL !== "function") {
      return input;
    }
    const target = typeof input === "string" || input instanceof global.URL
      ? input : typeof global.Request === "function" && input instanceof global.Request ? input.url : null;
    if (target === null) throw new TypeError("Unsupported dashboard request URL");
    const resolved = new global.URL(target, global.location.href);
    if (resolved.origin !== global.location.origin || !["http:", "https:"].includes(resolved.protocol)) {
      throw new TypeError("Public dashboard requests must stay on the same origin");
    }
    return input;
  }

  async function fetchWithTimeout(input, options = {}) {
    const {
      timeoutMs = DEFAULT_TIMEOUT_MS,
      signal: upstreamSignal,
      ...requestOptions
    } = options;
    const resolvedTimeout = finiteNumber(timeoutMs, DEFAULT_TIMEOUT_MS);
    if (resolvedTimeout <= 0 || resolvedTimeout > 120000) {
      throw new RangeError("timeoutMs must be between 1 and 120000 milliseconds");
    }
    sameOriginUrl(input);
    const controller = new AbortController();
    const started = now();
    const route = currentRoute();
    const requestPath = global.location
      ? new global.URL(typeof input === "string" ? input : input.url || input.href, global.location.href).pathname
      : metricPath(input);
    const interaction = activeInteractionContext(started);
    let response = null, complete = false;
    let headersAt = null, bodyAt = null, parseMs = 0, serverMs = null;
    const finish = (outcome = "ok") => {
      if (complete) return;
      complete = true;
      global.clearTimeout(timer);
      upstreamSignal?.removeEventListener("abort", forwardAbort);
      if (response) responseLifetimes.delete(response);
      const ended = now();
      const requestTiming = Object.freeze({
        path: requestPath,
        status: response?.status ?? null, outcome,
        headersMs: headersAt == null ? null : headersAt - started,
        bodyMs: bodyAt == null || headersAt == null ? null : bodyAt - headersAt,
        parseMs, serverMs, totalMs: ended - started,
      });
      requestTimings.push(requestTiming);
      if (requestTimings.length > 128) requestTimings.shift();
      const persistentMetric = {
        observedAt: Date.now(), route, kind: "api",
        action: interaction?.action || "automatic",
        eventType: interaction?.eventType || "automatic",
        requestPath,
        status: response?.status ?? null,
        outcome,
        headersMs: requestTiming.headersMs,
        bodyMs: requestTiming.bodyMs,
        parseMs,
        serverMs,
        durationMs: requestTiming.totalMs,
        viewportWidth: global.innerWidth,
        viewportHeight: global.innerHeight,
      };
      if (outcome === "ok") {
        afterNextPaint(() => appendPerformanceMetric({
          ...persistentMetric,
          paintMs: now() - started,
        }));
      } else {
        appendPerformanceMetric(persistentMetric);
      }
    };
    const abort = (reason) => { controller.abort(reason); finish(reason?.name || "aborted"); };
    const forwardAbort = () => abort(upstreamSignal?.reason);
    const timer = global.setTimeout(
      () => abort(new DOMException("Request timed out", "TimeoutError")),
      resolvedTimeout,
    );
    if (upstreamSignal?.aborted) forwardAbort();
    else upstreamSignal?.addEventListener("abort", forwardAbort, {once: true});
    try {
      response = await global.fetch(input, {
        ...requestOptions,
        credentials: "same-origin",
        redirect: "error",
        signal: controller.signal,
      });
      headersAt = now();
      const serverTiming = String(response.headers?.get?.("Server-Timing") || "");
      const appDuration = serverTiming.match(/(?:^|,)\s*app;dur=([0-9.]+)/i);
      serverMs = appDuration ? finiteNumber(appDuration[1]) : null;
      // fetch resolves at headers, not at the end of the body. Keep both the
      // timeout and caller cancellation alive through JSON/text consumption.
      responseLifetimes.set(response, {
        finish, abort, bodyRead() { bodyAt = now(); },
        parsed(ms) { parseMs = ms; },
      });
      return response;
    } catch (error) {
      finish(error?.name || "error");
      throw error;
    }
  }

  function cancelResponse(response) {
    responseLifetimes.get(response)?.abort(new DOMException("Response not needed", "AbortError"));
  }

  async function readTextResponse(response) {
    const lifetime = responseLifetimes.get(response);
    try {
      const text = await response.text();
      lifetime?.bodyRead();
      // A conditional GET has no JSON body to parse; its retained local
      // representation is a successful refresh, not an HTTP failure.
      lifetime?.finish(response.ok || response.status === 304 ? "ok" : "http_error");
      return text;
    } catch (error) {
      lifetime?.finish(error?.name || "error");
      throw error;
    }
  }

  function performanceSnapshot() {
    return requestTimings.map((row) => ({...row}));
  }

  function validateJsonRoot(payload, expectedRoot = null) {
    if (![null, "object", "array"].includes(expectedRoot)) {
      throw new TypeError("expectedRoot must be object, array, or null");
    }
    if (expectedRoot === "object" && (payload === null || typeof payload !== "object" || Array.isArray(payload))) {
      throw new TypeError("Dashboard API response root must be an object");
    }
    if (expectedRoot === "array" && !Array.isArray(payload)) {
      throw new TypeError("Dashboard API response root must be an array");
    }
    return payload;
  }

  async function readJsonResponse(response, {expectedRoot = null} = {}) {
    if (!response || typeof response.json !== "function") {
      throw new TypeError("Dashboard API response is not a Response-like object");
    }
    const lifetime = responseLifetimes.get(response);
    try {
      let payload;
      if (typeof response.text === "function") {
        const text = await response.text();
        lifetime?.bodyRead();
        const parseStarted = now();
        try { payload = JSON.parse(text); }
        finally { lifetime?.parsed(now() - parseStarted); }
      } else {
        payload = await response.json();
        lifetime?.bodyRead();
      }
      if (!response.ok) {
        const message = payload && typeof payload === "object" && !Array.isArray(payload)
          ? payload.error : null;
        throw new Error(message || `HTTP ${response.status}`);
      }
      const result = validateJsonRoot(payload, expectedRoot);
      lifetime?.finish();
      return result;
    } catch (error) {
      lifetime?.finish(response.ok ? error?.name || "error" : "http_error");
      throw error;
    }
  }

  async function fetchJson(input, options = {}) {
    const {expectedRoot = null, ...requestOptions} = options;
    const response = await fetchWithTimeout(input, requestOptions);
    return readJsonResponse(response, {expectedRoot});
  }

  function createFetch({timeoutMs = DEFAULT_TIMEOUT_MS} = {}) {
    return (input, options = {}) => fetchWithTimeout(input, {
      ...options,
      timeoutMs: options.timeoutMs ?? timeoutMs,
    });
  }

  function createJsonFetcher({timeoutMs = DEFAULT_TIMEOUT_MS, ...defaults} = {}) {
    return (input, options = {}) => fetchJson(input, {
      ...defaults,
      ...options,
      timeoutMs: options.timeoutMs ?? timeoutMs,
    });
  }

  function createLatestRequest() {
    let sequence = 0;
    let controller = null;
    return Object.freeze({
      begin() {
        controller?.abort();
        controller = new AbortController();
        const activeController = controller;
        const activeSequence = ++sequence;
        return Object.freeze({
          signal: activeController.signal,
          isCurrent: () => activeSequence === sequence && controller === activeController,
          finish() {
            if (controller === activeController) controller = null;
          },
        });
      },
      abort() {
        sequence += 1;
        controller?.abort();
        controller = null;
      },
    });
  }

  function svgElement(name, attributes = {}) {
    if (!global.document) return null;
    const node = global.document.createElementNS("http://www.w3.org/2000/svg", String(name));
    for (const [key, value] of Object.entries(attributes)) {
      node.setAttribute(key, String(value));
    }
    return node;
  }

  function dashboardHref(item, current) {
    if (current === "overview") return item.id === "overview" ? "./" : `${item.slug}/`;
    if (item.id === "overview") return "../";
    return item.id === current ? "./" : `../${item.slug}/`;
  }

  function mountNavigation(target) {
    if (!target || target.dataset.dashboardNavMounted === "true") return target;
    const current = global.location?.pathname?.startsWith("/tw-overnight/")
      ? "tw-overnight"
      : String(target.dataset.dashboardNav || "overview");
    if (!NAV_ITEMS.some((item) => item.id === current)) {
      throw new TypeError(`Unknown public dashboard id: ${current}`);
    }
    const fragment = global.document.createDocumentFragment();
    for (const item of NAV_ITEMS) {
      const link = global.document.createElement("a");
      link.href = dashboardHref(item, current);
      link.textContent = item.label;
      if (item.id === current) link.setAttribute("aria-current", "page");
      fragment.append(link);
    }
    target.replaceChildren(fragment);
    target.dataset.dashboardNavMounted = "true";
    target.dataset.expanded = "false";
    if (global.document?.documentElement) {
      global.document.documentElement.dataset.dashboardPage = current;
    }
    const header = target.parentElement;
    if (header?.insertBefore && header?.querySelector) {
      const menuId = `dashboard-menu-${current}`;
      target.id ||= menuId;
      let toggle = header.querySelector(".dashboard-nav-toggle");
      if (!toggle) {
        toggle = global.document.createElement("button");
        toggle.type = "button";
        toggle.className = "dashboard-nav-toggle";
        toggle.textContent = "面板選單";
        toggle.setAttribute("aria-controls", target.id);
        toggle.setAttribute("aria-expanded", "false");
        header.insertBefore(toggle, target);
        const setExpanded = (expanded) => {
          target.dataset.expanded = String(Boolean(expanded));
          toggle.setAttribute("aria-expanded", String(Boolean(expanded)));
        };
        toggle.addEventListener("click", () => {
          setExpanded(target.dataset.expanded !== "true");
        });
        target.addEventListener("click", (event) => {
          if (event.target?.closest?.("a")) setExpanded(false);
        });
        target.addEventListener("keydown", (event) => {
          if (event.key !== "Escape") return;
          setExpanded(false);
          toggle.focus();
        });
      }
    }
    return target;
  }

  function mountNavigations(root = global.document) {
    if (!root?.querySelectorAll) return 0;
    const targets = [...root.querySelectorAll("nav[data-dashboard-nav]")];
    targets.forEach(mountNavigation);
    if (global.document?.documentElement) {
      global.document.documentElement.dataset.dashboardCore = "ready";
    }
    return targets.length;
  }

  function responsiveTableEligible(table) {
    if (!table?.closest || !table?.classList) return false;
    if (table.dataset?.responsive === "scroll") return false;
    if (table.closest("#source-list") || table.classList.contains("strategy-table")) return false;
    return Boolean(table.closest(".table-scroll,.table-wrap"));
  }

  function enhanceResponsiveTable(table) {
    if (!responsiveTableEligible(table)) return false;
    const headers = [...table.querySelectorAll("thead th")]
      .map((cell) => String(cell.textContent || "").trim());
    if (!headers.length) return false;
    table.classList.add("dashboard-responsive-table");
    for (const row of table.querySelectorAll("tbody tr")) {
      const cells = [...row.children].filter((cell) => cell.tagName === "TD");
      if (cells.length !== headers.length) continue;
      cells.forEach((cell, index) => cell.setAttribute("data-label", headers[index] || `欄位 ${index + 1}`));
    }
    return true;
  }

  function enhanceResponsiveTables(root = global.document) {
    if (!root?.querySelectorAll) return 0;
    const tables = root.matches?.("table") ? [root] : [...root.querySelectorAll("table")];
    return tables.reduce((count, table) => count + Number(enhanceResponsiveTable(table)), 0);
  }

  function observeResponsiveTables() {
    if (!global.MutationObserver || !global.document?.body) return null;
    const observer = new global.MutationObserver((records) => {
      const tables = new Set();
      for (const record of records) {
        const owner = record.target?.closest?.("table");
        if (owner) tables.add(owner);
        for (const node of record.addedNodes || []) {
          if (node.nodeType !== 1) continue;
          if (node.matches?.("table")) tables.add(node);
          const closest = node.closest?.("table");
          if (closest) tables.add(closest);
          for (const table of node.querySelectorAll?.("table") || []) tables.add(table);
        }
      }
      tables.forEach(enhanceResponsiveTable);
    });
    observer.observe(global.document.body, {childList: true, subtree: true});
    return observer;
  }

  function scheduleRefresh(callback, options = {}) {
    if (typeof callback !== "function") throw new TypeError("callback must be a function");
    const intervalMs = finiteNumber(options.intervalMs);
    if (intervalMs == null || intervalMs < 250) {
      throw new RangeError("intervalMs must be at least 250 milliseconds");
    }
    let disposed = false;
    const run = () => {
      if (disposed || (options.pauseWhenHidden !== false && global.document?.hidden)) return;
      Promise.resolve(callback()).catch((error) => options.onError?.(error));
    };
    const visibilityHandler = () => {
      if (!global.document.hidden && options.refreshOnVisible !== false) run();
    };
    const timer = global.setInterval(run, intervalMs);
    global.document?.addEventListener("visibilitychange", visibilityHandler);
    if (options.immediate !== false) run();
    return Object.freeze({
      run,
      dispose() {
        if (disposed) return;
        disposed = true;
        global.clearInterval(timer);
        global.document?.removeEventListener("visibilitychange", visibilityHandler);
      },
    });
  }

  function subscribeRevisions(input, onRevision, fallback, {fallbackMs = 1000, reconcileMs = 15000} = {}) {
    sameOriginUrl(input);
    let source = null;
    let opened = false;
    let disposed = false;
    let lastReconcile = 0;
    const disconnect = () => {
      source?.close();
      source = null;
      opened = false;
    };
    const connect = () => {
      if (disposed || global.document?.hidden || source || typeof global.EventSource !== "function") return;
      try {
        const active = new global.EventSource(input);
        source = active;
        active.onopen = () => { if (source === active) opened = true; };
        active.onerror = () => { if (source === active) opened = false; };
        active.addEventListener("revision", (event) => {
          if (source !== active || disposed) return;
          try {
            const payload = validateJsonRoot(JSON.parse(event.data), "object");
            onRevision(payload);
          } catch (_error) { opened = false; }
        });
      } catch (_error) { disconnect(); }
    };
    const poll = () => {
      connect();
      if (opened && Date.now() - lastReconcile < reconcileMs) return;
      lastReconcile = Date.now();
      return fallback();
    };
    const visibility = () => {
      if (global.document?.hidden) disconnect();
      else { connect(); lastReconcile = 0; }
    };
    global.document?.addEventListener("visibilitychange", visibility);
    global.addEventListener?.("pagehide", disconnect);
    global.addEventListener?.("pageshow", visibility);
    connect();
    const scheduler = scheduleRefresh(poll, {intervalMs: fallbackMs});
    return Object.freeze({
      dispose() {
        disposed = true;
        disconnect();
        scheduler.dispose();
        global.document?.removeEventListener("visibilitychange", visibility);
        global.removeEventListener?.("pagehide", disconnect);
        global.removeEventListener?.("pageshow", visibility);
      },
    });
  }

  loadPerformanceHistory();
  installInteractionMeasurements();
  installPageLoadMeasurement();
  global.addEventListener?.("pagehide", persistPerformanceHistory);
  global.addEventListener?.("storage", (event) => {
    if (event.key !== PERFORMANCE_STORAGE_KEY) return;
    loadPerformanceHistory(event.newValue);
    if (global.document?.dispatchEvent && typeof global.CustomEvent === "function") {
      global.document.dispatchEvent(new global.CustomEvent("stockagent-performance-recorded"));
    }
  });

  const api = Object.freeze({
    version: 4,
    DEFAULT_TIMEOUT_MS,
    PERFORMANCE_SCHEMA_VERSION,
    PERFORMANCE_HISTORY_LIMIT,
    NAV_ITEMS,
    byId,
    finiteNumber,
    formatNumber,
    formatBytes,
    formatAge,
    escapeHtml,
    setText,
    setTrustedHtml,
    fetchWithTimeout,
    validateJsonRoot,
    readJsonResponse,
    fetchJson,
    readTextResponse,
    cancelResponse,
    performanceSnapshot,
    performanceHistorySnapshot,
    clearPerformanceHistory,
    recordPerformanceMetric,
    createFetch,
    createJsonFetcher,
    createLatestRequest,
    svgElement,
    mountNavigation,
    mountNavigations,
    enhanceResponsiveTable,
    enhanceResponsiveTables,
    scheduleRefresh,
    subscribeRevisions,
  });

  Object.defineProperty(global, "StockAgentDashboard", {
    value: api,
    configurable: false,
    enumerable: true,
    writable: false,
  });
  mountNavigations();
  enhanceResponsiveTables();
  observeResponsiveTables();
})(globalThis);
