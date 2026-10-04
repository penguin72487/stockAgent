/** Shared display contracts: timestamps remain observations, never interpolated prices. */
export type Range = "1h" | "1d" | "1w" | "1mo" | "1q" | "1y" | "all";
export type CalendarUnit = "minute" | "hour" | "day" | "week" | "month" | "year";
export interface SessionMarker { label: string; minute: number; }
export interface TickSpec {
  unit: CalendarUnit; step: number; format: Intl.DateTimeFormat; rotate?: boolean;
}
export interface Tick {
  timestamp: number; label: string; kind: "regular" | "session"; rotate: boolean;
  observedTimestamp?: number; observedIndex?: number;
}
export interface TimeAxis {
  startMs: number; endMs: number; ticks: Tick[];
  observedTimes?: number[]; collapseEmptyIntervals?: boolean;
}
export interface TimeAxisInput {
  range: Range; timestamps: readonly unknown[];
  sessions?: readonly SessionMarker[]; collapseEmptyIntervals?: boolean;
}
export interface TimeAxisAPI {
  buildTimeAxis(input: TimeAxisInput): TimeAxis | null;
  position(axis: TimeAxis, timestamp: unknown, left: number, right: number): number;
}
declare global {
  var StockAgentTimeAxis: Readonly<TimeAxisAPI>;
  var module: { exports: Readonly<TimeAxisAPI> } | undefined;
}
