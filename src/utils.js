export const DEFAULT_MONTH_OFFSET = 0;

let _debounceTimer = null;
export function debounce(fn, delay = 250) {
  clearTimeout(_debounceTimer);
  _debounceTimer = setTimeout(fn, delay);
}

export function esc(s) {
  if (!s) return "";
  return String(s).replace(
    /[&<>"']/g,
    (c) =>
      ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;",
      })[c],
  );
}

export function getRecentYearMonths(count, startOffset = DEFAULT_MONTH_OFFSET) {
  const now = new Date();
  const months = [];
  for (let i = startOffset; i < startOffset + count; i++) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    months.push({
      value: `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, "0")}`,
      label: `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`,
    });
  }
  return months;
}

const _apiCache = new Map();
const _API_CACHE_TTL = 5 * 60 * 1000;

export async function fetchApiJson(url, { noCache = false } = {}) {
  if (!noCache) {
    const cached = _apiCache.get(url);
    if (cached && Date.now() - cached.ts < _API_CACHE_TTL) {
      return cached.data;
    }
  }
  const res = await fetch(url);
  const payload = await res.json().catch(() => null);
  if (!res.ok) {
    throw new Error(
      payload?.message || payload?.error || `HTTP ${res.status}`,
    );
  }
  const data = payload || {};
  _apiCache.set(url, { data, ts: Date.now() });
  return data;
}

export function normalizeSearchText(value) {
  return String(value || "")
    .replace(/\s+/g, "")
    .toLowerCase();
}

export function decorateTradeRows(rows) {
  const decorated = rows.map((row) => ({ ...row, newHigh: false }));
  if (decorated.length === 0) return decorated;

  const sorted = [...decorated].sort((a, b) => b.price - a.price);
  const threshold =
    sorted[Math.floor(sorted.length * 0.1)]?.price || Infinity;

  decorated.forEach((row) => {
    row.newHigh = row.price >= threshold;
  });

  return decorated;
}

export function formatPrice(p) {
  if (!p || p === 0) return "—";
  const sign = p < 0 ? "-" : "";
  const abs = Math.abs(p);
  const uk = Math.floor(abs / 10000);
  const rem = abs % 10000;
  if (uk >= 1) {
    if (rem === 0) return `${sign}${uk}억`;
    const ch = Math.floor(rem / 1000);
    return ch > 0 ? `${sign}${uk}억 ${ch}천` : `${sign}${uk}억 ${rem}`;
  }
  return `${sign}${abs.toLocaleString()}만`;
}

export function formatNearbyStation(value, distKm, pending = false) {
  if (pending) {
    return `<span class="muted">확인중...</span>`;
  }
  const text = String(value || "").trim();
  if (text) return esc(text);
  if (distKm != null && distKm > 0.8) {
    return `<span class="muted">도보 10분 초과</span>`;
  }
  return `<span class="muted">-</span>`;
}

export function formatNearbyAmenity(value, distKm, status, pending = false) {
  if (pending || status === "loading") {
    return `<span class="muted">확인중...</span>`;
  }
  const text = String(value || "").trim();
  if (text) return esc(text);
  if (distKm != null && distKm > 0.8) {
    return `<span class="muted">도보 10분 초과</span>`;
  }
  if (status === "no_match") {
    return `<span class="muted">없음</span>`;
  }
  if (status && !["ok", "no_match"].includes(status)) {
    return `<span class="muted">확인불가</span>`;
  }
  return `<span class="muted">-</span>`;
}

export function formatFlatLandStatus(status, pending = false) {
  if (pending || status === "loading") {
    return `<span class="muted">확인중...</span>`;
  }
  if (status === "flat") return "평지";
  if (status === "slope") return "경사";
  return `<span class="muted">확인불가</span>`;
}

export function isListingLocationInsightsPending(row) {
  return !!row?.locationInsightsPending;
}

export function hasListingLocationInsightFields(row) {
  if (!row || typeof row !== "object") return false;
  return (
    Object.prototype.hasOwnProperty.call(row, "nearbyStation") &&
    Object.prototype.hasOwnProperty.call(row, "nearbyElementarySchool") &&
    Object.prototype.hasOwnProperty.call(row, "nearbyPark") &&
    Object.prototype.hasOwnProperty.call(row, "flatLandStatus")
  );
}

// ── CSV 다운로드 (엑셀 호환, UTF-8 BOM) ──
export function downloadCsv(filename, headers, rows) {
  const csvEscape = (val) => {
    const s = String(val ?? "");
    if (s.includes(",") || s.includes('"') || s.includes("\n")) {
      return `"${s.replace(/"/g, '""')}"`;
    }
    return s;
  };
  const lines = [
    headers.map(csvEscape).join(","),
    ...rows.map((row) => row.map(csvEscape).join(",")),
  ];
  const bom = "\uFEFF";
  const blob = new Blob([bom + lines.join("\r\n")], {
    type: "text/csv;charset=utf-8;",
  });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

export function detectListingLocationInsightsStatus(summary, rows) {
  if (summary?.locationInsightsStatus) {
    return summary.locationInsightsStatus;
  }
  if (!Array.isArray(rows) || rows.length === 0) {
    return "ok";
  }
  if (rows.some((row) => isListingLocationInsightsPending(row))) {
    return "deferred";
  }
  if (rows.some((row) => hasListingLocationInsightFields(row))) {
    return "ok";
  }
  return "stale_backend";
}
