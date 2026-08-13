"use strict";

const REFRESH_MS = 15000;
let refreshInFlight = false;

const $ = (id) => document.getElementById(id);

function healthPresentation(value) {
  const health = String(value || "unavailable").toLowerCase();
  const labels = {
    active: "資料正常",
    ready: "資料正常",
    waiting: "休市監控",
    stale: "資料逾時",
    blocked: "策略阻擋",
    critical: "需要注意",
    degraded: "部分異常",
    unavailable: "暫時離線",
  };
  return {health: health === "ready" ? "active" : health, label: labels[health] || "狀態未知"};
}

function ageLabel(seconds) {
  const value = Number(seconds);
  if (!Number.isFinite(value)) return "無更新時間";
  if (value < 60) return `${Math.max(0, Math.round(value))} 秒前`;
  if (value < 3600) return `${Math.round(value / 60)} 分鐘前`;
  return `${Math.round(value / 3600)} 小時前`;
}

function setHealth(prefix, value, overrideLabel = null) {
  const target = $(`${prefix}-health`);
  const {health, label} = healthPresentation(value);
  target.className = `health ${health}`;
  target.lastChild.textContent = overrideLabel || label;
}

async function fetchJson(path) {
  const response = await fetch(path, {cache: "default"});
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

function renderTaifex(data) {
    setHealth("taifex", data.health);
    $("taifex-freshness").textContent = ageLabel(data.source_age_seconds);
    const live = Number(data.live_strategies || 0);
    const coverage = Number(data.book_coverage_ratio);
    $("taifex-summary").textContent = Number.isFinite(coverage)
      ? `${live} 策略 · ${(coverage * 100).toFixed(0)}% 行情`
      : `${live} 個策略`;
}

function renderTw(data) {
    setHealth("tw", data.health);
    $("tw-freshness").textContent = ageLabel(data.source_age_seconds);
    const modes = Number(data.modes || 0);
    const positions = Number(data.open_positions || 0);
    $("tw-summary").textContent = `${modes} 模式 · ${positions} 個持倉`;
}

function bytes(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return "—";
  const units = ["B", "KiB", "MiB", "GiB"];
  let amount = parsed;
  let unit = 0;
  while (amount >= 1024 && unit < units.length - 1) { amount /= 1024; unit += 1; }
  return `${amount.toFixed(amount >= 100 ? 0 : 1)} ${units[unit]}`;
}

function renderShioaji(data) {
    setHealth("shioaji", data.health, data.health === "waiting" ? "流量保護" : null);
    $("shioaji-traffic").textContent = `${(Number(data.traffic_used_ratio || 0) * 100).toFixed(1)}% · 安全剩 ${bytes(data.safe_remaining_bytes)}`;
    $("shioaji-progress").textContent = `${Number(data.completed_contracts || 0)}/${Number(data.inventory_contracts || 0)} 合約 · ${(Number(data.progress_ratio || 0) * 100).toFixed(2)}%`;
}

function renderUnavailable() {
  for (const prefix of ["taifex", "tw", "shioaji"]) setHealth(prefix, "unavailable");
  $("taifex-freshness").textContent = "無法取得";
  $("tw-freshness").textContent = "無法取得";
  $("taifex-summary").textContent = "進入面板查看";
  $("tw-summary").textContent = "進入面板查看";
  $("shioaji-traffic").textContent = "無法取得";
  $("shioaji-progress").textContent = "進入面板查看";
}

async function refresh() {
  if (document.hidden || refreshInFlight) return;
  refreshInFlight = true;
  try {
    const data = await fetchJson("api/overview");
    renderTaifex(data.taifex || {});
    renderTw(data.tw || {});
    renderShioaji(data.shioaji || {});
  } catch (_error) {
    renderUnavailable();
  } finally {
    refreshInFlight = false;
  }
}

document.addEventListener("visibilitychange", () => {
  if (!document.hidden) void refresh();
});
void refresh();
window.setInterval(() => void refresh(), REFRESH_MS);
