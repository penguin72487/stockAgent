"use strict";

(function installTwChart(global) {
  const COLORS = ["#37d3ff", "#5ee0a0", "#a98cff", "#f5bd4f", "#ff7ac8", "#73e6d1", "#ff9f68"];
  let plot = null;
  let resizeObserver = null;
  let resizeFrame = null;
  let renderedHistory = null;
  let renderedKey = "";
  let renderedNote = "";

  function taipeiDayNumber(epochMinute) {
    return Math.floor((Number(epochMinute) + 8 * 60) / (24 * 60));
  }

  function minuteLabel(epochMinute, oneSession) {
    const localMinute = ((Number(epochMinute) + 8 * 60) % (24 * 60) + 24 * 60) % (24 * 60);
    if (oneSession) {
      return `${String(Math.floor(localMinute / 60)).padStart(2, "0")}:${String(localMinute % 60).padStart(2, "0")}`;
    }
    const local = new Date((Number(epochMinute) + 8 * 60) * 60000);
    return `${String(local.getUTCMonth() + 1).padStart(2, "0")}/${String(local.getUTCDate()).padStart(2, "0")}`;
  }

  function tickPlan(minutes) {
    if (!minutes.length) return {positions: [], labels: new Map()};
    const oneSession = taipeiDayNumber(minutes[0]) === taipeiDayNumber(minutes.at(-1));
    let positions;
    if (oneSession) {
      const tickCount = Math.min(7, minutes.length);
      positions = [...new Set(Array.from({length: tickCount}, (_value, index) => (
        Math.round(index * (minutes.length - 1) / Math.max(1, tickCount - 1))
      )))];
    } else {
      const sessionStarts = [];
      let previousDay = null;
      minutes.forEach((minute, index) => {
        const day = taipeiDayNumber(minute);
        if (day !== previousDay) sessionStarts.push(index);
        previousDay = day;
      });
      const step = Math.max(1, Math.ceil(sessionStarts.length / 10));
      positions = sessionStarts.filter((_value, index) => (
        index % step === 0 || index === sessionStarts.length - 1
      ));
    }
    return {
      positions,
      labels: new Map(positions.map((position) => [
        position,
        minuteLabel(minutes[position], oneSession),
      ])),
    };
  }

  function formatMinute(epochMinute) {
    return new Date(Number(epochMinute) * 60000).toLocaleString("zh-TW", {
      timeZone: "Asia/Taipei",
      hour12: false,
    });
  }

  function dimensions(host) {
    const mobile = global.matchMedia("(max-width: 700px)").matches;
    return {
      width: Math.max(300, Math.floor(host.getBoundingClientRect().width - (mobile ? 4 : 16))),
      height: mobile ? 266 : 296,
    };
  }

  function destroy() {
    if (plot) plot.destroy();
    plot = null;
    const host = global.document.getElementById("equity-chart");
    if (host) host.replaceChildren();
  }

  function installResizeObserver(host) {
    if (resizeObserver || typeof global.ResizeObserver !== "function") return;
    resizeObserver = new global.ResizeObserver(() => {
      if (!plot || global.document.hidden || resizeFrame !== null) return;
      resizeFrame = global.requestAnimationFrame(() => {
        resizeFrame = null;
        if (!plot) return;
        const next = dimensions(host);
        if (plot.width !== next.width || plot.height !== next.height) plot.setSize(next);
      });
    });
    resizeObserver.observe(host);
  }

  function yRange(minimum, maximum) {
    if (!Number.isFinite(minimum) || !Number.isFinite(maximum)) return [-.01, .01];
    const low = Math.min(0, minimum);
    const high = Math.max(0, maximum);
    const padding = Math.max(.01, (high - low) * .08);
    return [low - padding, high + padding];
  }

  function stalePointHook(stalePoints) {
    return (chart) => {
      const ratio = global.devicePixelRatio || 1;
      chart.ctx.save();
      chart.ctx.fillStyle = "#f5bd4f";
      for (const points of stalePoints) {
        for (const [xValue, yValue] of points) {
          const x = chart.valToPos(xValue, "x", true);
          const y = chart.valToPos(yValue, "y", true);
          chart.ctx.beginPath();
          chart.ctx.arc(x, y, 2.5 * ratio, 0, Math.PI * 2);
          chart.ctx.fill();
        }
      }
      chart.ctx.restore();
    };
  }

  function legendButton(item, label, hidden, formatNumber, pnlClass) {
    const button = global.document.createElement("button");
    button.type = "button";
    button.className = `legend-toggle${hidden ? " is-hidden" : ""}`;
    button.dataset.seriesId = item.seriesId;
    button.setAttribute("aria-pressed", String(!hidden));
    button.setAttribute("aria-label", `${hidden ? "顯示" : "隱藏"}${label}曲線`);
    const marker = global.document.createElement("i");
    marker.className = `series-${item.index % COLORS.length}`;
    marker.setAttribute("aria-hidden", "true");
    button.append(marker, global.document.createTextNode(`${label} `));
    const value = global.document.createElement("strong");
    value.className = pnlClass(item.latest);
    value.textContent = item.latest == null
      ? "—"
      : `${item.latest >= 0 ? "+" : ""}${formatNumber(item.latest)}%`;
    button.append(value);
    return button;
  }

  function render(input) {
    const {
      data,
      history,
      historyMatchesSelection,
      historyPendingRevision,
      historyLoadError,
      historyInFlight,
      hiddenSeries,
      selectedMode,
      detailRangeKey,
      isOvernight,
      strategyLabel,
      chartWindowLabel,
      formatNumber,
      formatCount,
      pnlClass,
    } = input;
    const host = global.document.getElementById("equity-chart");
    const empty = global.document.getElementById("chart-empty");
    const legend = global.document.getElementById("chart-legend");
    const note = global.document.getElementById("equity-range-note");
    if (!historyMatchesSelection) {
      renderedHistory = null;
      renderedKey = "";
      renderedNote = "";
      legend.replaceChildren();
      destroy();
      host.classList.add("hidden");
      empty.classList.remove("hidden");
      if (historyLoadError) {
        empty.textContent = "歷史曲線載入失敗";
        note.textContent = `${chartWindowLabel} · ${historyLoadError}`;
      } else {
        empty.textContent = "歷史曲線載入中…";
        note.textContent = `${chartWindowLabel} · ${historyInFlight ? "正在讀取完整所選期間" : "等待完整所選期間資料"}；不以最新即時點代替歷史曲線。`;
      }
      return;
    }

    const renderKey = JSON.stringify([
      selectedMode,
      detailRangeKey,
      [...hiddenSeries],
      data.modes.map((row) => [row.market, strategyLabel(row)]),
      (data.benchmarks || []).map((row) => [row.benchmark_id, row.label]),
    ]);
    const staleNote = historyLoadError
      ? `；${historyLoadError}；保留上次成功資料（可能已過期）`
      : historyPendingRevision ? "；資料更新中，暫顯示上一份已驗證曲線" : "";
    if (renderedHistory === history && renderedKey === renderKey) {
      note.textContent = renderedNote + staleNote;
      return;
    }
    const renderStarted = global.performance.now();

    const selectedModes = selectedMode === "all"
      ? data.modes.map((row) => row.market)
      : [selectedMode];
    const labels = new Map([
      ...data.modes.map((row) => [row.market, strategyLabel(row)]),
      ...(data.benchmarks || []).map((row) => [row.benchmark_id, row.label || row.benchmark_id]),
    ]);
    const labelOrder = new Map([...labels.keys()].map((seriesId, index) => [seriesId, index]));
    const globalMinutes = history.minute_axis || [];
    const series = (history.minute_series || [])
      .filter((item) => item.series_type === "benchmark" || selectedModes.includes(item.series_id))
      .map((item, index) => {
        let validCount = 0;
        let latest = null;
        for (const value of item.return_pct || []) {
          if (!Number.isFinite(Number(value))) continue;
          validCount += 1;
          latest = Number(value);
        }
        return {
          ...item,
          seriesId: item.series_id,
          index: labelOrder.has(item.series_id) ? labelOrder.get(item.series_id) : index,
          validCount,
          latest,
        };
      });
    const allPointCount = series.reduce((total, item) => total + item.validCount, 0);
    const visibleSeries = series.filter((item) => !hiddenSeries.has(item.seriesId));
    const visiblePointCount = visibleSeries.reduce((total, item) => total + item.validCount, 0);
    legend.replaceChildren(...series.map((item) => legendButton(
      item,
      labels.get(item.seriesId) || item.seriesId,
      hiddenSeries.has(item.seriesId),
      formatNumber,
      pnlClass,
    )));
    empty.textContent = allPointCount
      ? "所有曲線已隱藏；點選圖例圓點可重新顯示。"
      : `目前尚無${isOvernight ? "集合競價事件" : "分鐘"}報酬率資料`;
    empty.classList.toggle("hidden", visiblePointCount > 0);
    host.classList.toggle("hidden", visiblePointCount === 0);
    if (!visiblePointCount || typeof global.uPlot !== "function") {
      destroy();
      if (visiblePointCount) {
        empty.textContent = "曲線繪圖元件載入失敗";
        empty.classList.remove("hidden");
        host.classList.add("hidden");
        note.textContent = `${chartWindowLabel} · 本機 Canvas 圖表元件不可用。`;
      } else {
        note.textContent = allPointCount
          ? `${chartWindowLabel} · 目前 ${formatCount(series.length)} 條曲線皆已隱藏。`
          : `${chartWindowLabel}內沒有可繪製資料。`;
        note.textContent += staleNote;
      }
      return;
    }

    const selectedMinuteMask = new Uint8Array(globalMinutes.length);
    for (const item of series) {
      for (const index of item.minute_indexes || []) selectedMinuteMask[index] = 1;
    }
    const selectedMinuteIndexes = [];
    for (let index = 0; index < selectedMinuteMask.length; index += 1) {
      if (selectedMinuteMask[index]) selectedMinuteIndexes.push(index);
    }
    const selectedMinutes = selectedMinuteIndexes.map((index) => globalMinutes[index]);
    const minutePosition = new Int32Array(globalMinutes.length);
    minutePosition.fill(-1);
    selectedMinuteIndexes.forEach((index, position) => { minutePosition[index] = position; });
    const plotData = [selectedMinutes.map((_value, index) => index)];
    const stalePoints = [];
    for (const item of visibleSeries) {
      const values = new Array(selectedMinutes.length).fill(null);
      const stale = [];
      for (let index = 0; index < item.minute_indexes.length; index += 1) {
        const position = minutePosition[item.minute_indexes[index]];
        const value = Number(item.return_pct[index]);
        if (position < 0 || !Number.isFinite(value)) continue;
        values[position] = value;
        const quality = Number(item.quality_flags?.[index] || 0);
        if ((quality & 1) && (!(quality & 2) || (quality & 4))) stale.push([position, value]);
      }
      plotData.push(values);
      stalePoints.push(stale);
    }
    const staleObservationCount = stalePoints.reduce((total, points) => total + points.length, 0);

    const ticks = tickPlan(selectedMinutes);
    const size = dimensions(host);
    const options = {
      width: size.width,
      height: size.height,
      padding: [12, 10, 0, 0],
      cursor: {drag: {x: true, y: false}, focus: {prox: 24}},
      legend: {show: false},
      scales: {
        x: {time: false},
        y: {range: (_chart, minimum, maximum) => yRange(minimum, maximum)},
      },
      axes: [
        {
          stroke: "#72899e",
          grid: {show: true, stroke: "rgba(44,64,84,.72)", width: 1},
          ticks: {show: true, stroke: "#2c4054", width: 1, size: 5},
          font: "10px Inter, Noto Sans TC, sans-serif",
          size: 42,
          splits: () => ticks.positions,
          values: (_chart, splits) => splits.map((value) => ticks.labels.get(value) || ""),
        },
        {
          stroke: "#72899e",
          grid: {show: true, stroke: "rgba(44,64,84,.72)", width: 1},
          ticks: {show: true, stroke: "#2c4054", width: 1, size: 5},
          font: "10px Inter, Noto Sans TC, sans-serif",
          size: 68,
          values: (_chart, splits) => splits.map((value) => `${formatNumber(value)}%`),
        },
      ],
      series: [
        {},
        ...visibleSeries.map((item) => ({
          label: labels.get(item.seriesId) || item.seriesId,
          stroke: COLORS[item.index % COLORS.length],
          width: 2,
          spanGaps: false,
          points: {show: false},
        })),
      ],
      hooks: {draw: [stalePointHook(stalePoints)]},
    };
    const drawStarted = global.performance.now();
    destroy();
    plot = new global.uPlot(options, plotData, host);
    const drawCompleted = global.performance.now();
    installResizeObserver(host);

    const start = formatMinute(selectedMinutes[0]);
    const end = formatMinute(selectedMinutes.at(-1));
    const sampled = history.downsampled
      ? `；已保留端點與區間極值縮圖（原 ${formatCount(history.raw_points_in_range)} 點）`
      : "";
    const replayPoints = Number(history.historical_minute_replay_points || 0);
    const replayMean = Number(history.historical_minute_mean_fresh_trade_notional_coverage_ratio);
    const replayMissing = Number(history.historical_minute_missing_price_points || 0);
    const replayQuality = replayPoints
      ? isOvernight
        ? `；歷史反事實集合競價事件 ${formatCount(replayPoints)} 點，缺價／延用 ${formatCount(replayMissing)} 點`
        : `；歷史分鐘 ${formatCount(replayPoints)} 點，平均新成交名目覆蓋 ${Number.isFinite(replayMean) ? `${formatNumber(replayMean * 100)}%` : "—"}，缺價 ${formatCount(replayMissing)} 點（其餘無成交分鐘延用上一筆）`
      : "";
    const coverageGaps = (history.range_summary || []).filter((row) => Number(row.minute_coverage_ratio) < .999999);
    const coverageQuality = coverageGaps.length
      ? `；${isOvernight ? "事件來源未齊" : "分鐘來源未齊"}：${coverageGaps.map((row) => `${labels.get(row.series_id) || row.series_id} ${formatCount(row.point_count)}/${formatCount(row.expected_minute_points)}`).join("、")}`
      : `；${isOvernight ? "集合競價事件" : "分鐘"}時間點齊全`;
    const staleQuality = staleObservationCount
      ? `；警告：${formatCount(staleObservationCount)} 個顯示點使用延用估值或缺價，非即時可成交報價（圖中以黃色標記）`
      : "";
    host.setAttribute("aria-label", `${isOvernight ? "集合競價事件" : "分鐘"}報酬率曲線，${start}至${end}，顯示${formatCount(visiblePointCount)}點與${formatCount(visibleSeries.length)}條線`);
    renderedNote = `${chartWindowLabel} · ${isOvernight ? "收盤／次日開盤事件曲線（歷史為反事實近似）" : "一分鐘曲線"} · 每條線第一個有效${isOvernight ? "事件" : "分鐘"}固定為 0%；期末權益與累積淨損益仍沿用原始帳本 · ${start} ～ ${end} · 顯示 ${formatCount(visiblePointCount)} 點、${formatCount(visibleSeries.length)}/${formatCount(series.length)} 條線；全體無資料的時間已壓縮${sampled}${replayQuality}${coverageQuality}${staleQuality}`;
    note.textContent = renderedNote + staleNote;
    renderedHistory = history;
    renderedKey = renderKey;
    global.requestAnimationFrame(() => global.requestAnimationFrame(() => {
      global.StockAgentDashboard?.recordPerformanceMetric?.({
        observedAt: Date.now(),
        kind: "render",
        action: "equity_chart",
        eventType: "history_update",
        durationMs: drawCompleted - renderStarted,
        prepareMs: drawStarted - renderStarted,
        drawMs: drawCompleted - drawStarted,
        paintMs: global.performance.now() - renderStarted,
        pointCount: visiblePointCount,
        seriesCount: visibleSeries.length,
        viewportWidth: global.innerWidth,
        viewportHeight: global.innerHeight,
      });
    }));
  }

  function decodeHistory(payload) {
    if (payload.history_encoding === "minute_columns_v2") return payload;
    const encodedSeries = [];
    const allMinutes = new Set();
    if (payload.history_encoding === "minute_columns_v1") {
      for (const series of payload.minute_series || []) {
        const sourceMinutes = [];
        const returns = [];
        const cumulative = [];
        const quality = [];
        for (const point of series.points || []) {
          const minute = Number(point[0]);
          allMinutes.add(minute);
          sourceMinutes.push(minute);
          returns.push(point[1]);
          cumulative.push(point[2]);
          quality.push(point[3]);
        }
        encodedSeries.push({
          series_id: series.series_id,
          series_type: series.series_type,
          sourceMinutes,
          returns,
          cumulative,
          quality,
        });
      }
    } else {
      const grouped = new Map();
      for (const row of payload.history || []) {
        const seriesId = row.series_id || row.market || row.benchmark_id;
        const minute = Math.floor(Date.parse(row.minute) / 60000);
        if (!seriesId || !Number.isFinite(minute)) continue;
        if (!grouped.has(seriesId)) {
          grouped.set(seriesId, {
            series_id: seriesId,
            series_type: row.series_type === "benchmark" ? "benchmark" : "strategy",
            rows: [],
          });
        }
        allMinutes.add(minute);
        grouped.get(seriesId).rows.push({minute, row});
      }
      for (const series of grouped.values()) {
        series.rows.sort((left, right) => left.minute - right.minute);
        encodedSeries.push({
          series_id: series.series_id,
          series_type: series.series_type,
          sourceMinutes: series.rows.map((item) => item.minute),
          returns: series.rows.map((item) => item.row.return_pct),
          cumulative: series.rows.map((item) => item.row.cumulative_return_pct),
          quality: series.rows.map((item) => (
            Number(Boolean(item.row.valuation_stale))
            | (Number(Boolean(item.row.historical_minute_replay)) << 1)
            | (Number(Number(item.row.missing_price_position_count || 0) > 0) << 2)
          )),
        });
      }
    }
    const minuteAxis = [...allMinutes].sort((left, right) => left - right);
    const minuteIndex = new Map(minuteAxis.map((minute, index) => [minute, index]));
    const minuteSeries = encodedSeries.map((series) => ({
      series_id: series.series_id,
      series_type: series.series_type,
      minute_indexes: series.sourceMinutes.map((minute) => minuteIndex.get(minute)),
      return_pct: series.returns,
      cumulative_return_pct: series.cumulative,
      quality_flags: series.quality,
    }));
    const {minute_series: _oldSeries, minute_axis: _oldAxis, ...metadata} = payload;
    return {
      ...metadata,
      history_encoding: "minute_columns_v2",
      minute_axis: minuteAxis,
      minute_series: minuteSeries,
      history: [],
    };
  }

  global.StockAgentTwChart = Object.freeze({
    version: 1,
    render,
    decodeHistory,
    destroy,
  });
})(globalThis);
