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
    Object.freeze({id: "shioaji", label: "永豐 API", slug: "shioaji"}),
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
    node.textContent = value == null || value === "" ? fallback : String(value);
    return true;
  }

  function sameOriginUrl(input) {
    if (typeof input !== "string" || !global.location || typeof global.URL !== "function") {
      return input;
    }
    const resolved = new global.URL(input, global.location.href);
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
    const forwardAbort = () => controller.abort(upstreamSignal?.reason);
    if (upstreamSignal?.aborted) forwardAbort();
    else upstreamSignal?.addEventListener("abort", forwardAbort, {once: true});
    const timer = global.setTimeout(
      () => controller.abort(new DOMException("Request timed out", "TimeoutError")),
      resolvedTimeout,
    );
    try {
      return await global.fetch(input, {
        ...requestOptions,
        credentials: "same-origin",
        signal: controller.signal,
      });
    } finally {
      global.clearTimeout(timer);
      upstreamSignal?.removeEventListener("abort", forwardAbort);
    }
  }

  async function fetchJson(input, options = {}) {
    const response = await fetchWithTimeout(input, options);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.json();
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
    const current = String(target.dataset.dashboardNav || "overview");
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

  const api = Object.freeze({
    version: 1,
    DEFAULT_TIMEOUT_MS,
    NAV_ITEMS,
    byId,
    finiteNumber,
    formatNumber,
    formatBytes,
    formatAge,
    escapeHtml,
    setText,
    fetchWithTimeout,
    fetchJson,
    createFetch,
    createJsonFetcher,
    svgElement,
    mountNavigation,
    mountNavigations,
    scheduleRefresh,
  });

  Object.defineProperty(global, "StockAgentDashboard", {
    value: api,
    configurable: false,
    enumerable: true,
    writable: false,
  });
  mountNavigations();
})(globalThis);
