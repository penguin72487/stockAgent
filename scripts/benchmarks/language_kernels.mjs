// Standalone Node candidate. Its IPC/startup costs are reported separately.
import fs from 'node:fs';
import { performance } from 'node:perf_hooks';

const [kind, input, datesPath, output, metaPath, tickArg, ratioArg, windowArg, repeatsArg, ratioValue] = process.argv.slice(2);
const tickCutoff = Number(tickArg), ratioCutoff = Number(ratioArg);
const window = Number(windowArg), repeats = Number(repeatsArg), ratio = Number(ratioValue);
const readStart = performance.now();
const inputBytes = fs.readFileSync(input);
const values = new Float64Array(inputBytes.buffer, inputBytes.byteOffset, inputBytes.byteLength / 8);
const dateBytes = kind === 'limits' ? fs.readFileSync(datesPath) : null;
const dates = dateBytes ? new BigInt64Array(dateBytes.buffer, dateBytes.byteOffset, dateBytes.byteLength / 8) : null;
const readSeconds = (performance.now() - readStart) / 1000;

function run() {
    const n = values.length;
    const out = new Float64Array(n * (kind === 'limits' ? 1 : 2));
    if (kind === 'limits') {
        for (let i = 0; i < n; i++) {
            const day = Number(dates[i]);
            const r = day < ratioCutoff ? (ratio > 1 ? 1.07 : .93) : (ratio > 1 ? 1.10 : .90);
            const p = values[i] * r;
            if (!Number.isFinite(p) || p <= 0) { out[i] = NaN; continue; }
            const old = day < tickCutoff;
            const tick = p < (old ? 5 : 10) ? .01 : p < (old ? 15 : 50) ? .05 :
                p < (old ? 50 : 100) ? .1 : p < (old ? 150 : 500) ? .5 : p < 1000 ? 1 : 5;
            const rounded = (ratio < 1 ? Math.ceil(p / tick - 1e-12) : Math.floor(p / tick + 1e-12)) * tick;
            out[i] = Math.floor(rounded * 100 + .5) / 100;
        }
    } else {
        const sums = new Float64Array(n), squares = new Float64Array(n), counts = new Float64Array(n);
        let sum = 0, square = 0, count = 0;
        for (let i = 0; i < n; i++) {
            const finite = Number.isFinite(values[i]);
            const v = finite ? values[i] : 0;
            count += Number(finite); sum += v; square += v * v;
            counts[i] = count; sums[i] = sum; squares[i] = square;
        }
        for (let i = 0; i < n; i++) {
            const c = counts[i] - (i >= window ? counts[i - window] : 0);
            const s = sums[i] - (i >= window ? sums[i - window] : 0);
            const q = squares[i] - (i >= window ? squares[i - window] : 0);
            const mean = c ? s / c : 0;
            const second = c ? q / c : 0;
            const variance = second - mean * mean;
            out[i] = mean; out[n + i] = Math.sqrt(variance > 0 ? variance : 0);
        }
    }
    return out;
}

const firstStart = performance.now();
let result = run();
const firstSeconds = (performance.now() - firstStart) / 1000;
// Warm the optimizing JIT before collecting resident-kernel samples.
for (let i = 0; i < (repeats > 0 ? 8 : 0); i++) result = run();
const samples = [];
for (let i = 0; i < repeats; i++) {
    const cpuStart = process.cpuUsage(), start = performance.now();
    result = run();
    const elapsed = process.cpuUsage(cpuStart);
    samples.push({wall_s: (performance.now() - start) / 1000,
        cpu_s: (elapsed.user + elapsed.system) / 1e6});
}
const writeStart = performance.now();
fs.writeFileSync(output, Buffer.from(result.buffer, result.byteOffset, result.byteLength));
fs.writeFileSync(metaPath, JSON.stringify({node: process.version, read_s: readSeconds,
    first_call_s: firstSeconds, samples, write_s: (performance.now() - writeStart) / 1000}));
