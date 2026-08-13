"use strict";

const REFRESH_MS = 15000;

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
  const response = await fetch(path, {cache: "no-cache"});
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

async function refreshTaifex() {
  try {
    const data = await fetchJson("taifex/api/status");
    setHealth("taifex", data.health);
    $("taifex-freshness").textContent = ageLabel(data.source_age_seconds);
    const live = Number(data.strategy_counts?.live_ideal ?? data.strategies?.length ?? 0);
    const coverage = Number(data.market?.book_coverage_ratio);
    $("taifex-summary").textContent = Number.isFinite(coverage)
      ? `${live} 策略 · ${(coverage * 100).toFixed(0)}% 行情`
      : `${live} 個策略`;
  } catch (_error) {
    setHealth("taifex", "unavailable");
    $("taifex-freshness").textContent = "無法取得";
    $("taifex-summary").textContent = "進入面板查看";
  }
}

async function refreshTw() {
  try {
    const data = await fetchJson("tw-day-trade/api/status");
    setHealth("tw", data.health);
    $("tw-freshness").textContent = ageLabel(data.source_age_seconds);
    const modes = Array.isArray(data.modes) ? data.modes.length : 0;
    const positions = Array.isArray(data.positions)
      ? data.positions.filter((row) => Number(row.signed_shares || 0) !== 0).length
      : 0;
    $("tw-summary").textContent = `${modes} 模式 · ${positions} 個持倉`;
  } catch (_error) {
    setHealth("tw", "unavailable");
    $("tw-freshness").textContent = "無法取得";
    $("tw-summary").textContent = "進入面板查看";
  }
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

async function refreshShioaji() {
  try {
    const data = await fetchJson("shioaji/api/status");
    setHealth("shioaji", data.health, data.health === "waiting" ? "流量保護" : null);
    $("shioaji-traffic").textContent = `${(Number(data.traffic?.used_ratio || 0) * 100).toFixed(1)}% · 安全剩 ${bytes(data.traffic?.safe_remaining_bytes)}`;
    $("shioaji-progress").textContent = `${Number(data.backfill?.completed_contracts || 0)}/${Number(data.backfill?.inventory_contracts || 0)} 合約 · ${(Number(data.backfill?.progress_ratio || 0) * 100).toFixed(2)}%`;
  } catch (_error) {
    setHealth("shioaji", "unavailable");
    $("shioaji-traffic").textContent = "無法取得";
    $("shioaji-progress").textContent = "進入面板查看";
  }
}

function refresh() {
  if (document.hidden) return;
  void Promise.allSettled([refreshTaifex(), refreshTw(), refreshShioaji()]);
}

document.addEventListener("visibilitychange", () => {
  if (!document.hidden) refresh();
});
refresh();
window.setInterval(refresh, REFRESH_MS);
