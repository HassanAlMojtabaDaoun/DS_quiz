import { useEffect, useRef, useState } from "react";

export function fmtBytes(n) {
  if (n === null || n === undefined) return "—";
  const u = ["B", "KiB", "MiB", "GiB", "TiB"];
  let i = 0;
  let v = Number(n);
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
  return `${v >= 100 || i === 0 ? v.toFixed(0) : v.toFixed(1)} ${u[i]}`;
}

export function fmtDate(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toLocaleString(undefined, { year: "numeric", month: "short", day: "2-digit",
    hour: "2-digit", minute: "2-digit" });
}

export const pct = (a, b) => (b ? Math.round((100 * a) / b) : 0);

// Calls fn now and then every `ms` milliseconds while mounted.
export function usePoll(fn, ms, deps = []) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const saved = useRef(fn);
  saved.current = fn;
  useEffect(() => {
    let alive = true;
    let timer;
    const tick = async () => {
      try {
        const d = await saved.current();
        if (alive) { setData(d); setError(null); }
      } catch (e) {
        if (alive) setError(e);
      }
      if (alive) timer = setTimeout(tick, ms);
    };
    tick();
    return () => { alive = false; clearTimeout(timer); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);
  return [data, error, setData];
}

// Stable colour per storage node, used in every chart and shard map.
const PALETTE = ["#2563eb", "#16a34a", "#d97706", "#9333ea", "#0891b2", "#db2777",
  "#65a30d", "#dc2626"];
export function nodeColor(nodeId) {
  const m = /(\d+)$/.exec(nodeId || "");
  const i = m ? Number(m[1]) - 1 : [...(nodeId || "")].reduce((a, c) => a + c.charCodeAt(0), 0);
  return PALETTE[((i % PALETTE.length) + PALETTE.length) % PALETTE.length];
}

export function parseTagString(s) {
  const out = {};
  for (const part of (s || "").split(",")) {
    const i = part.indexOf("=");
    if (i > 0) out[part.slice(0, i).trim()] = part.slice(i + 1).trim();
  }
  return out;
}

export const tagString = (tags) =>
  Object.entries(tags || {}).map(([k, v]) => `${k}=${v}`).join(",");
