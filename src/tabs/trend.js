import { state } from '../state.js';
import {
  esc,
  formatPrice,
  fetchApiJson,
  normalizeSearchText,
  getRecentYearMonths,
} from '../utils.js';
import { getRegionName, initRegionPair, DEFAULT_REGION_CODE } from '../regions.js';

function sqmToPyeong(sqm) {
  return Math.round(sqm / 3.305);
}

// ── 선택 키 ──
export function getTrendSelectionKey(selection) {
  return `${String(selection?.regionCode || "").trim()}::${normalizeSearchText(selection?.name)}`;
}

export function createTrendSelection(name, regionCode) {
  const cleanRegionCode = String(regionCode || "").trim();
  return {
    id: `trend-${Date.now()}-${state.trendSelectionSeq++}`,
    name: String(name || "").trim(),
    regionCode: cleanRegionCode,
    regionName: getRegionName(cleanRegionCode),
    areaKey: "",
    areaOptions: [],
  };
}

export function getTrendAreaKey(area) {
  const numeric = Number(area);
  if (!Number.isFinite(numeric) || numeric <= 0) return "";
  return numeric.toFixed(4).replace(/\.?0+$/, "");
}

export function isTrendAreaMatch(area, areaKey) {
  if (!areaKey) return true;
  const numeric = Number(area);
  const target = Number(areaKey);
  if (!Number.isFinite(numeric) || !Number.isFinite(target)) return false;
  return Math.abs(numeric - target) < 0.01;
}

export function formatTrendAreaLabel(area) {
  const numeric = Number(area);
  if (!Number.isFinite(numeric) || numeric <= 0) return "평형 정보 없음";
  return `${numeric.toFixed(2).replace(/\.?0+$/, "")}㎡ · ${sqmToPyeong(numeric)}평`;
}

export function findTrendMatches(items, targetName) {
  const targetKey = normalizeSearchText(targetName);
  return (items || []).filter(
    (item) => normalizeSearchText(item?.apt) === targetKey,
  );
}

export function buildTrendAreaOptions(monthlyMatches) {
  const stats = new Map();

  (monthlyMatches || []).forEach(({ items }) => {
    (items || []).forEach((item) => {
      const area = Number(item?.area);
      const key = getTrendAreaKey(area);
      if (!key) return;
      const existing = stats.get(key) || { key, area, count: 0 };
      existing.count += 1;
      stats.set(key, existing);
    });
  });

  const options = [...stats.values()]
    .sort((a, b) => a.area - b.area || b.count - a.count)
    .map((item) => ({
      key: item.key,
      area: item.area,
      count: item.count,
      label: formatTrendAreaLabel(item.area),
    }));

  const defaultKey =
    [...stats.values()].sort(
      (a, b) => b.count - a.count || a.area - b.area,
    )[0]?.key || "";

  return { options, defaultKey };
}

export function buildTrendPoints(monthlyMatches, areaKey) {
  const points = [];

  (monthlyMatches || []).forEach(({ ym, items }) => {
    const filtered = (items || []).filter((item) =>
      isTrendAreaMatch(item?.area, areaKey),
    );
    if (filtered.length === 0) return;
    const avg = Math.round(
      filtered.reduce((sum, item) => sum + Number(item?.price || 0), 0) /
        filtered.length,
    );
    points.push({
      ym: ym.slice(0, 4) + "-" + ym.slice(4),
      price: avg,
    });
  });

  return points;
}

export function rebuildTrendDataFromSource() {
  const nextTrendData = {};

  state.trendApts = state.trendApts.map((selection) => {
    const monthlyMatches = state.trendSeriesSource[selection.id] || [];
    const { options, defaultKey } = buildTrendAreaOptions(monthlyMatches);
    const nextAreaKey = options.some(
      (item) => item.key === selection.areaKey,
    )
      ? selection.areaKey
      : defaultKey;
    const points = buildTrendPoints(monthlyMatches, nextAreaKey);

    if (points.length > 0) {
      nextTrendData[selection.id] = points;
    }

    return {
      ...selection,
      areaOptions: options,
      areaKey: nextAreaKey,
    };
  });

  state.trendData = nextTrendData;
  renderTrendChips();
  requestAnimationFrame(drawChart);
}

export function updateTrendArea(selectionId, areaKey) {
  state.trendApts = state.trendApts.map((selection) =>
    selection.id === selectionId
      ? {
            ...selection,
            areaKey,
          }
      : selection,
  );
  rebuildTrendDataFromSource();
}

export function buildTrendTradeUrl(region, ym, pageNo = 1) {
  return `/api/apt-trade?LAWD_CD=${region}&DEAL_YMD=${ym}&numOfRows=${state.TREND_SUGGEST_PAGE_SIZE}&pageNo=${pageNo}`;
}

export function updateTrendInputPlaceholder() {
  const input = document.getElementById("trendInput");
  if (!input) return;
  const regionName = getRegionName(
    document.getElementById("trendRegion")?.value,
  );
  input.placeholder = regionName
    ? `${regionName} 단지를 클릭하거나 직접 입력하세요`
    : "단지를 클릭하거나 직접 입력하세요";
}

export function closeTrendSuggestMenu() {
  state.trendSuggestOpen = false;
  state.trendSuggestActiveIndex = -1;
  state.trendSuggestResults = [];
  renderTrendSuggestMenu();
}

export async function fetchTrendSuggestionMonth(region, ym) {
  const items = [];
  let pageNo = 1;

  while (pageNo <= state.TREND_SUGGEST_MAX_PAGES) {
    const payload = await fetchApiJson(
      buildTrendTradeUrl(region, ym, pageNo),
    );
    const pageItems = Array.isArray(payload.items) ? payload.items : [];
    items.push(...pageItems);

    const totalCount = Number(payload.totalCount || 0);
    const totalPages =
      totalCount > 0
        ? Math.ceil(totalCount / state.TREND_SUGGEST_PAGE_SIZE)
        : 1;

    if (
      pageItems.length < state.TREND_SUGGEST_PAGE_SIZE ||
      pageNo >= totalPages
    ) {
      break;
    }
    pageNo += 1;
  }

  return items;
}

export async function ensureTrendSuggestions(force = false) {
  const region = document.getElementById("trendRegion")?.value;
  if (!region) return [];
  if (!force && Array.isArray(state.trendSuggestCache[region])) {
    state.trendSuggestError = "";
    return state.trendSuggestCache[region];
  }

  const requestToken = ++state.trendSuggestRequestToken;
  state.trendSuggestLoading = true;
  state.trendSuggestError = "";
  renderTrendSuggestMenu();

  try {
    const months = getRecentYearMonths(state.TREND_SUGGEST_MONTHS).map(
      ({ value }) => value,
    );
    const settled = await Promise.allSettled(
      months.map((ym) => fetchTrendSuggestionMonth(region, ym)),
    );

    if (requestToken !== state.trendSuggestRequestToken) {
      return state.trendSuggestCache[region] || [];
    }

    const fulfilled = settled
      .filter((result) => result.status === "fulfilled")
      .map((result) => result.value);

    if (fulfilled.length === 0) {
      state.trendSuggestError = "단지 목록을 불러오지 못했습니다.";
      return [];
    }

    const counts = new Map();
    fulfilled.flat().forEach((item) => {
      const name = String(item?.apt || "").trim();
      if (!name) return;
      counts.set(name, (counts.get(name) || 0) + 1);
    });

    const list = [...counts.entries()]
      .map(([name, count]) => ({ name, count }))
      .sort(
        (a, b) => b.count - a.count || a.name.localeCompare(b.name, "ko"),
      );

    state.trendSuggestCache[region] = list;
    return list;
  } catch (error) {
    if (requestToken === state.trendSuggestRequestToken) {
      state.trendSuggestError =
        error?.message || "단지 목록을 불러오지 못했습니다.";
    }
    return [];
  } finally {
    if (requestToken === state.trendSuggestRequestToken) {
      state.trendSuggestLoading = false;
      renderTrendSuggestMenu();
    }
  }
}

export function getTrendFilteredSuggestions(query) {
  const region = document.getElementById("trendRegion")?.value;
  const keyword = normalizeSearchText(query);
  const selected = new Set(
    state.trendApts.map((selection) => getTrendSelectionKey(selection)),
  );
  let items = (state.trendSuggestCache[region] || []).filter(
    (item) =>
      !selected.has(`${region}::${normalizeSearchText(item.name)}`),
  );

  if (keyword) {
    items = items.filter((item) =>
      normalizeSearchText(item.name).includes(keyword),
    );
  }

  return items.slice(
    0,
    keyword ? state.TREND_SUGGEST_SEARCH_LIMIT : state.TREND_SUGGEST_DEFAULT_LIMIT,
  );
}

export function renderTrendSuggestMenu() {
  const menu = document.getElementById("trendSuggestMenu");
  const region = document.getElementById("trendRegion")?.value;
  const input = document.getElementById("trendInput");
  if (!menu || !input) return;

  if (!state.trendSuggestOpen) {
    menu.classList.remove("open");
    menu.innerHTML = "";
    return;
  }

  menu.classList.add("open");

  if (state.trendSuggestLoading) {
    menu.innerHTML = `<div class="trend-suggest-state">${esc(getRegionName(region))} 단지 목록을 불러오는 중...</div>`;
    return;
  }

  if (state.trendSuggestError) {
    menu.innerHTML = `<div class="trend-suggest-state">${esc(state.trendSuggestError)}</div>`;
    return;
  }

  state.trendSuggestResults = getTrendFilteredSuggestions(input.value.trim());
  if (state.trendSuggestResults.length === 0) {
    const message = input.value.trim()
      ? `"${esc(input.value.trim())}"이 포함된 ${esc(getRegionName(region))} 단지가 없습니다`
      : `${esc(getRegionName(region))} 최근 거래 단지가 없습니다`;
    menu.innerHTML = `<div class="trend-suggest-state">${message}</div>`;
    state.trendSuggestActiveIndex = -1;
    return;
  }

  if (state.trendSuggestActiveIndex >= state.trendSuggestResults.length) {
    state.trendSuggestActiveIndex = state.trendSuggestResults.length - 1;
  }

  menu.innerHTML = state.trendSuggestResults
    .map(
      (item, index) => `
      <button
        type="button"
        class="trend-suggest-item${index === state.trendSuggestActiveIndex ? " active" : ""}"
        data-trend-name="${encodeURIComponent(item.name)}"
      >
        <span class="trend-suggest-name">${esc(item.name)}</span>
        <span class="trend-suggest-meta">최근 거래 ${item.count}건</span>
      </button>`,
    )
    .join("");
}

export async function openTrendSuggestMenu(force = false) {
  state.trendSuggestOpen = true;
  state.trendSuggestActiveIndex = -1;

  const region = document.getElementById("trendRegion")?.value;
  if (!region) {
    renderTrendSuggestMenu();
    return;
  }

  if (!force && Array.isArray(state.trendSuggestCache[region])) {
    renderTrendSuggestMenu();
    return;
  }

  await ensureTrendSuggestions(force);
}

export function moveTrendSuggestion(delta) {
  if (!state.trendSuggestOpen || state.trendSuggestResults.length === 0) return;

  if (state.trendSuggestActiveIndex < 0) {
    state.trendSuggestActiveIndex =
      delta > 0 ? 0 : state.trendSuggestResults.length - 1;
  } else {
    state.trendSuggestActiveIndex =
      (state.trendSuggestActiveIndex + delta + state.trendSuggestResults.length) %
      state.trendSuggestResults.length;
  }

  renderTrendSuggestMenu();
  document
    .querySelector("#trendSuggestMenu .trend-suggest-item.active")
    ?.scrollIntoView({ block: "nearest" });
}

export function selectTrendSuggestion(name) {
  if (!name) return;
  addTrendApt(name);
}

export function handleTrendInputKeydown(event) {
  if (event.isComposing || event.keyCode === 229) {
    return;
  }
  if (event.key === "ArrowDown") {
    event.preventDefault();
    if (!state.trendSuggestOpen) {
      openTrendSuggestMenu().catch(() => {});
      return;
    }
    moveTrendSuggestion(1);
    return;
  }
  if (event.key === "ArrowUp") {
    event.preventDefault();
    if (!state.trendSuggestOpen) {
      openTrendSuggestMenu().catch(() => {});
      return;
    }
    moveTrendSuggestion(-1);
    return;
  }
  if (event.key === "Escape") {
    closeTrendSuggestMenu();
    return;
  }
  if (event.key === "Enter") {
    event.preventDefault();

    const input = document.getElementById("trendInput");
    const keyword = normalizeSearchText(input?.value);
    const active = state.trendSuggestResults[state.trendSuggestActiveIndex];
    if (active) {
      selectTrendSuggestion(active.name);
      return;
    }

    if (keyword) {
      const exact = state.trendSuggestResults.find(
        (item) => normalizeSearchText(item.name) === keyword,
      );
      if (exact) {
        selectTrendSuggestion(exact.name);
        return;
      }
    }

    addTrendApt();
  }
}

export function initTrendAutocomplete() {
  const input = document.getElementById("trendInput");
  const regionSelect = document.getElementById("trendRegion");
  const menu = document.getElementById("trendSuggestMenu");
  const chips = document.getElementById("trendChips");
  if (!input || !regionSelect || !menu || !chips) return;

  updateTrendInputPlaceholder();

  input.addEventListener("focus", () => {
    openTrendSuggestMenu().catch(() => {});
  });
  input.addEventListener("click", () => {
    openTrendSuggestMenu().catch(() => {});
  });
  input.addEventListener("input", () => {
    state.trendSuggestOpen = true;
    state.trendSuggestActiveIndex = -1;
    if (
      !Array.isArray(state.trendSuggestCache[regionSelect.value]) &&
      !state.trendSuggestLoading
    ) {
      ensureTrendSuggestions().catch(() => {});
      return;
    }
    renderTrendSuggestMenu();
  });
  input.addEventListener("keydown", handleTrendInputKeydown);

  regionSelect.addEventListener("change", () => {
    state.trendSuggestActiveIndex = -1;
    state.trendSuggestError = "";
    updateTrendInputPlaceholder();
    if (document.activeElement === input) {
      openTrendSuggestMenu().catch(() => {});
    } else {
      closeTrendSuggestMenu();
    }
  });

  menu.addEventListener("mousedown", (event) => {
    const button = event.target.closest("[data-trend-name]");
    if (!button) return;
    event.preventDefault();
    selectTrendSuggestion(
      decodeURIComponent(button.dataset.trendName || ""),
    );
  });

  chips.addEventListener("click", (event) => {
    const removeButton = event.target.closest("[data-trend-chip]");
    if (!removeButton) return;
    removeTrendApt(removeButton.dataset.trendChip || "");
  });

  chips.addEventListener("change", (event) => {
    const areaSelect = event.target.closest("[data-trend-area]");
    if (!areaSelect) return;
    updateTrendArea(
      areaSelect.dataset.trendArea || "",
      areaSelect.value || "",
    );
  });

  document.addEventListener("click", (event) => {
    if (!event.target.closest(".trend-input-wrap")) {
      closeTrendSuggestMenu();
    }
  });
}

// ── 시세추이 ──
export function addTrendApt(nameOverride = "") {
  const input = document.getElementById("trendInput");
  const name = String(nameOverride || input.value).trim();
  const regionCode = document.getElementById("trendRegion")?.value || "";
  if (!name) return;
  if (!regionCode) return;
  const selectionKey = `${regionCode}::${normalizeSearchText(name)}`;
  if (
    state.trendApts.some(
      (selection) => getTrendSelectionKey(selection) === selectionKey,
    )
  ) {
    input.value = "";
    closeTrendSuggestMenu();
    return;
  }
  if (state.trendApts.length >= 6) {
    alert("최대 6개까지 비교할 수 있습니다.");
    return;
  }
  state.trendApts.push(createTrendSelection(name, regionCode));
  input.value = "";
  closeTrendSuggestMenu();
  renderTrendChips();
  loadTrendData().catch(() => {
    state.trendData = {};
    state.trendSeriesSource = {};
    requestAnimationFrame(drawChart);
  });
}

export function removeTrendApt(selectionId) {
  state.trendApts = state.trendApts.filter(
    (selection) => selection.id !== selectionId,
  );
  delete state.trendData[selectionId];
  delete state.trendSeriesSource[selectionId];
  renderTrendChips();
  if (state.trendSuggestOpen) {
    renderTrendSuggestMenu();
  }
  if (state.trendApts.length === 0) {
    state.trendData = {};
    state.trendSeriesSource = {};
    requestAnimationFrame(drawChart);
    return;
  }
  rebuildTrendDataFromSource();
}

export function renderTrendChips() {
  document.getElementById("trendChips").innerHTML = state.trendApts
    .map((selection, i) => {
      const color = state.TREND_COLORS[i % state.TREND_COLORS.length];
      const areaOptions = Array.isArray(selection.areaOptions)
        ? selection.areaOptions
        : [];
      const hasLoadedSource = Array.isArray(
        state.trendSeriesSource[selection.id],
      );
      const areaControl = areaOptions.length
        ? `<select class="trend-chip-area" data-trend-area="${selection.id}">
            ${areaOptions
              .map(
                (option) =>
                  `<option value="${option.key}"${option.key === selection.areaKey ? " selected" : ""}>${esc(option.label)}</option>`,
              )
              .join("")}
          </select>`
        : `<span class="trend-chip-area-state">${hasLoadedSource ? "평형 정보 없음" : "평형 불러오는 중"}</span>`;
      return `<div class="trend-chip" style="border-color:${color}">
        <span class="trend-chip-main">
          <span class="trend-chip-name" style="color:${color}">${esc(selection.name)}</span>
          <span class="trend-chip-meta">${esc(selection.regionName || getRegionName(selection.regionCode))}</span>
        </span>
        ${areaControl}
        <span class="remove" data-trend-chip="${selection.id}">&times;</span>
      </div>`;
    })
    .join("");
}

export function buildTrendDummy() {
  requestAnimationFrame(drawChart);
}

export async function loadTrendData() {
  if (state.trendApts.length === 0) {
    alert("먼저 단지를 선택하세요");
    return;
  }

  const months = getRecentYearMonths(state.TREND_LOOKBACK_MONTHS)
    .map(({ value }) => value)
    .reverse();
  const regionCodes = [
    ...new Set(
      state.trendApts.map((selection) => selection.regionCode).filter(Boolean),
    ),
  ];

  const fetches = regionCodes.flatMap((regionCode) =>
    months.map((ym) =>
      fetchApiJson(
        `/api/apt-trade?LAWD_CD=${regionCode}&DEAL_YMD=${ym}&numOfRows=1000&pageNo=1`,
      )
        .then((payload) => ({
          regionCode,
          ym,
          items: payload.items || [],
        }))
        .catch(() => ({ regionCode, ym, items: [] })),
    ),
  );
  const results = await Promise.all(fetches);

  const resultsByRegion = {};
  results.forEach(({ regionCode, ym, items }) => {
    if (!resultsByRegion[regionCode]) {
      resultsByRegion[regionCode] = [];
    }
    resultsByRegion[regionCode].push({ ym, items });
  });

  regionCodes.forEach((regionCode) => {
    resultsByRegion[regionCode] = (
      resultsByRegion[regionCode] || []
    ).sort((a, b) => a.ym.localeCompare(b.ym));
  });

  state.trendSeriesSource = {};
  state.trendApts.forEach((selection) => {
    const monthlyResults = resultsByRegion[selection.regionCode] || [];
    state.trendSeriesSource[selection.id] = monthlyResults.map(
      ({ ym, items }) => ({
        ym,
        items: findTrendMatches(items, selection.name),
      }),
    );
  });

  rebuildTrendDataFromSource();
}

let _chartRetries = 0;
export function drawChart() {
  const canvas = document.getElementById("trendCanvas");
  if (canvas.closest(".tab-content:not(.active)")) {
    _chartRetries = 0;
    return;
  }
  const W = canvas.offsetWidth,
    H = canvas.offsetHeight;
  if (!W || W < 10) {
    if (++_chartRetries < 10) requestAnimationFrame(drawChart);
    return;
  }
  _chartRetries = 0;
  canvas.width = W * devicePixelRatio;
  canvas.height = H * devicePixelRatio;
  const ctx = canvas.getContext("2d");
  ctx.scale(devicePixelRatio, devicePixelRatio);

  ctx.fillStyle = "#13161e";
  ctx.fillRect(0, 0, W, H);

  if (state.trendApts.length === 0) {
    ctx.fillStyle = "#8892a4";
    ctx.font = "14px 'Noto Sans KR'";
    ctx.textAlign = "center";
    ctx.fillText("비교할 단지를 추가하고 조회하세요", W / 2, H / 2);
    return;
  }

  if (Object.keys(state.trendData).length === 0) {
    ctx.fillStyle = "#8892a4";
    ctx.font = "14px 'Noto Sans KR'";
    ctx.textAlign = "center";
    ctx.fillText("불러온 추이 데이터가 없습니다", W / 2, H / 2);
    return;
  }

  const pad = { top: 30, right: 30, bottom: 40, left: 70 };
  const cw = W - pad.left - pad.right;
  const ch = H - pad.top - pad.bottom;

  let allPrices = [],
    allYms = new Set();
  Object.values(state.trendData).forEach((pts) => {
    pts.forEach((p) => {
      allPrices.push(p.price);
      allYms.add(p.ym);
    });
  });
  const yms = [...allYms].sort();
  const minP = Math.min(...allPrices) * 0.9;
  const maxP = Math.max(...allPrices) * 1.1;

  // 그리드
  ctx.strokeStyle = "#252a38";
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const y = pad.top + (ch * i) / 4;
    ctx.beginPath();
    ctx.moveTo(pad.left, y);
    ctx.lineTo(W - pad.right, y);
    ctx.stroke();
    const val = maxP - ((maxP - minP) * i) / 4;
    ctx.fillStyle = "#8892a4";
    ctx.font = "11px 'JetBrains Mono'";
    ctx.textAlign = "right";
    ctx.fillText(formatPrice(Math.round(val)), pad.left - 8, y + 4);
  }
  // X축
  yms.forEach((ym, i) => {
    const x = pad.left + (cw * i) / Math.max(yms.length - 1, 1);
    ctx.fillStyle = "#8892a4";
    ctx.font = "11px 'JetBrains Mono'";
    ctx.textAlign = "center";
    ctx.fillText(ym, x, H - pad.bottom + 20);
  });

  // 라인
  state.trendApts.forEach((selection, idx) => {
    const pts = state.trendData[selection.id];
    if (!pts || pts.length === 0) return;
    const color = state.TREND_COLORS[idx % state.TREND_COLORS.length];
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.beginPath();
    pts.forEach((p, i) => {
      const xi = yms.indexOf(p.ym);
      const x = pad.left + (cw * xi) / Math.max(yms.length - 1, 1);
      const y = pad.top + ch * (1 - (p.price - minP) / (maxP - minP));
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();
    // 점
    pts.forEach((p) => {
      const xi = yms.indexOf(p.ym);
      const x = pad.left + (cw * xi) / Math.max(yms.length - 1, 1);
      const y = pad.top + ch * (1 - (p.price - minP) / (maxP - minP));
      ctx.beginPath();
      ctx.arc(x, y, 4, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();
    });
  });
}

// ── select 빌드 ──
export function buildTrendSelects() {
  initRegionPair("trendSidoSelect", "trendRegion", DEFAULT_REGION_CODE);
}

// ── 초기화 ──
export function initTrend() {
  buildTrendSelects();
  initTrendAutocomplete();
}

export function activateTrend() {
  requestAnimationFrame(drawChart);
}
