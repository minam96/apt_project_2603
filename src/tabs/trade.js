import { state } from '../state.js';
import {
  esc,
  formatPrice,
  fetchApiJson,
  getRecentYearMonths,
  decorateTradeRows,
  formatNearbyStation,
  downloadCsv,
} from '../utils.js';
import { getRegionName, initRegionPair, DEFAULT_REGION_CODE } from '../regions.js';
import { renderThead, renderPagination, toggleSort, getSorted, toggleColFilter, closeAllFilters, registerRenderCallback } from '../table-utils.js';

// ── 헬퍼 ──
function sqmToPyeong(sqm) {
  return Math.round(sqm / 3.305);
}

function formatHouseholdCount(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) && numeric > 0
    ? `${numeric.toLocaleString("ko-KR")}세대`
    : `<span class="muted">—</span>`;
}

function formatCompletionYearMonth(value) {
  const raw = String(value || "").trim();
  if (!raw) return `<span class="muted">—</span>`;
  const [year, month = ""] = raw.split("-");
  if (!year) return `<span class="muted">—</span>`;
  if (!month || month === "00") return `${esc(year)}년`;
  return `${esc(year)}.${esc(month)}`;
}

// ── select 빌드 ──
export function buildSelects() {
  initRegionPair("tradeSidoSelect", "regionSelect", DEFAULT_REGION_CODE);
  const ms = document.getElementById("monthSelect");
  ms.innerHTML = "";
  getRecentYearMonths(6).forEach(({ value, label }) => {
    ms.innerHTML += `<option value="${value}">${label}</option>`;
  });
}

// ── 필터 & 조건 ──
export function setCond(c) {
  state.cond = c;
  document
    .querySelectorAll("#tabTrade .cond-btn")
    .forEach((b) => b.classList.toggle("active", b.dataset.cond === c));
  state.currentPage = 1;
  renderTable();
}

export function setPropertyType(t) {
  state.propertyType = t;
  document
    .querySelectorAll("#tradeTypeBar .type-chip")
    .forEach((b) => b.classList.toggle("active", b.dataset.type === t));
}

export function onRegionChange() {
  /* 사용자가 select 변경 시 — 조회 버튼 클릭 필요 */
}

// ── 필터 ──
export function getFiltered(data, search, filters, condFilter) {
  let f = data.slice();
  const q = (search || "").toLowerCase();
  if (q) f = f.filter((d) => d.apt.toLowerCase().includes(q));
  if (condFilter === "high") f = f.filter((d) => d.newHigh);
  else if (condFilter === "10") f = f.filter((d) => d.price >= 100000);
  else if (condFilter === "50") f = f.filter((d) => d.price >= 500000);
  Object.entries(filters).forEach(([k, v]) => {
    if (v != null) f = f.filter((d) => String(d[k]) === String(v));
  });
  return f;
}

// ── API 호출 — 매매 ──
const TRADE_API = {
  apt: "apt-trade",
  offi: "offi-trade",
  villa: "villa-trade",
  house: "house-trade",
};

export async function loadData() {
  const region = document.getElementById("regionSelect").value;
  const month = document.getElementById("monthSelect").value;
  const endpoint = TRADE_API[state.propertyType];
  const url = `/api/${endpoint}?LAWD_CD=${region}&DEAL_YMD=${month}&numOfRows=1000&pageNo=1`;

  state.tradeStateMessage = "로딩 중...";
  document.getElementById("tableBody").innerHTML =
    `<tr><td colspan="${state.TRADE_COLS.length}" class="state-msg">⏳ 로딩 중...</td></tr>`;
  try {
    const payload = await fetchApiJson(url);
    state.allData = decorateTradeRows(payload.items || []);
    state.tradeStateMessage =
      state.allData.length === 0
        ? "선택한 지역/월의 거래 데이터가 없습니다"
        : "";
  } catch (e) {
    console.error(e);
    state.tradeStateMessage = `오류: ${e.message}`;
    state.allData = [];
  }
  state.currentPage = 1;
  renderAll();
}

// ── 렌더링 — 매매 ──
export function renderAll() {
  renderTable();
  renderSidebar();
  renderTicker();
}

export function renderTable() {
  renderThead(state.TRADE_COLS, state.sortKey, state.sortDir, state.colFilters, false);
  const search = document.getElementById("searchInput").value;
  const filtered = getSorted(
    getFiltered(state.allData, search, state.colFilters, state.cond),
    state.sortKey,
    state.sortDir,
  );
  const total = filtered.length;
  const totalPages = Math.max(1, Math.ceil(total / state.PAGE_SIZE));
  if (state.currentPage > totalPages) state.currentPage = 1;
  const start = (state.currentPage - 1) * state.PAGE_SIZE;
  const page = filtered.slice(start, start + state.PAGE_SIZE);

  const tbody = document.getElementById("tableBody");
  if (page.length === 0) {
    const emptyMessage =
      total === 0
        ? state.tradeStateMessage || "데이터가 없습니다"
        : "검색/필터 결과가 없습니다";
    tbody.innerHTML = `<tr><td colspan="${state.TRADE_COLS.length}" class="state-msg">${esc(emptyMessage)}</td></tr>`;
  } else {
    window._pageItems = page;
    tbody.innerHTML = page
      .map((d, idx) => {
        const priceDisp = formatPrice(d.price);
        const pyeong = sqmToPyeong(d.area);
        const householdsDisp = formatHouseholdCount(d.households);
        const completionDisp = formatCompletionYearMonth(
          d.completionYearMonth,
        );
        const nearbyStationDisp = formatNearbyStation(
          d.nearbyStation,
          d.nearbyStationDistanceKm,
        );
        const changeTxt = d.newHigh
          ? '<span class="change-up">▲신고가</span>'
          : '<span class="change-eq">—</span>';
        const badge = d.newHigh
          ? '<span class="badge">신고가</span>'
          : "";
        const naverUrl = `https://search.naver.com/search.naver?query=${encodeURIComponent(d.apt + " 아파트")}`;
        return `<tr class="${d.newHigh ? "new-high" : ""}">
          <td><a class="apt-link" href="${naverUrl}" target="_blank" rel="noopener">${esc(d.apt)}</a>${badge}</td>
          <td>${esc(d.gu)}</td>
          <td class="price">${priceDisp}</td>
          <td>${changeTxt}</td>
          <td class="area">${esc(d.area)}㎡ / ${pyeong}평</td>
          <td>${esc(d.floor)}층</td>
          <td>${householdsDisp}</td>
          <td>${completionDisp}</td>
          <td>${nearbyStationDisp}</td>
          <td class="date">${esc(d.date)}</td>
          <td><button class="building-btn" type="button" title="${esc(d.apt)} 건축물대장 상세 보기" aria-label="${esc(d.apt)} 건축물대장 상세 보기" onclick="showBuildingModal(window._pageItems[${idx}])">🏗️</button></td>
        </tr>`;
      })
      .join("");
  }
  renderPagination("pagination", state.currentPage, totalPages, (n) => {
    state.currentPage = n;
    renderTable();
  });
}

// ── 사이드바 ──
export function renderSidebar() {
  const total = state.allData.length;
  const newHighs = state.allData.filter((d) => d.newHigh);
  const maxPrice = state.allData.reduce(
    (m, d) => (d.price > m ? d.price : m),
    0,
  );
  const avgPrice =
    total > 0
      ? Math.round(state.allData.reduce((s, d) => s + d.price, 0) / total)
      : 0;

  document.getElementById("sumTotal").textContent =
    total.toLocaleString() + "건";
  document.getElementById("sumHigh").textContent = newHighs.length + "건";
  document.getElementById("sumMax").textContent = formatPrice(maxPrice);
  document.getElementById("sumAvg").textContent = formatPrice(avgPrice);

  // TOP 10
  const top10 = [...state.allData]
    .sort((a, b) => b.price - a.price)
    .slice(0, 10);
  document.getElementById("topList").innerHTML = top10
    .map((d, i) => {
      const rankClass = i < 3 ? "gold" : "";
      return `<li>
        <span class="top-rank ${rankClass}">${i + 1}</span>
        <span class="top-name">${esc(d.apt)}</span>
        <span class="top-price">${formatPrice(d.price)}</span>
      </li>`;
    })
    .join("");

  // 지역별 거래량
  const guMap = {};
  state.allData.forEach((d) => {
    guMap[d.gu] = (guMap[d.gu] || 0) + 1;
  });
  const guArr = Object.entries(guMap)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8);
  const maxCount = guArr[0]?.[1] || 1;
  document.getElementById("regionChart").innerHTML = guArr
    .map(([gu, cnt]) => {
      const pct = Math.round((cnt / maxCount) * 100);
      return `<div class="bar-chart-item">
        <span class="bar-label">${esc(gu)}</span>
        <div class="bar-track"><div class="bar-fill" style="width:${pct}%"></div></div>
        <span class="bar-val">${cnt}건</span>
      </div>`;
    })
    .join("");
}

export function renderTicker() {
  const total = state.allData.length;
  const highs = state.allData.filter((d) => d.newHigh).length;
  const guMap = {};
  state.allData.forEach((d) => {
    guMap[d.gu] = (guMap[d.gu] || 0) + 1;
  });
  const guStr = Object.entries(guMap)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([g, c]) => `${esc(g)} ${c}건`)
    .join("  ");
  document.getElementById("ticker").innerHTML =
    `<span>오늘거래 <b>${total}</b>건</span>` +
    `<span>신고가 <b class="up">${highs}</b>건</span>` +
    `<span>${guStr}</span>`;
}

// ── 건축물대장 모달 ──
export function closeBuildingModal() {
  document.getElementById("buildingModal").classList.remove("open");
}

export async function showBuildingModal(d) {
  document.getElementById("buildingModal").classList.add("open");
  const content = document.getElementById("buildingModalContent");
  content.innerHTML = `<div class="state-msg">⏳ 건축물대장 조회 중...</div>`;

  try {
    if (!d.sigunguCd || !d.bjdongCd || !d.bun) {
      content.innerHTML = `<div class="state-msg">건축물대장 조회에 필요한 주소 정보가 없습니다</div>`;
      return;
    }
    const params = new URLSearchParams({
      sigunguCd: d.sigunguCd || "",
      bjdongCd: d.bjdongCd || "",
      bun: (d.bun || "").padStart(4, "0"),
      ji: (d.ji || "").padStart(4, "0"),
      numOfRows: "5",
      pageNo: "1",
    });
    const res = await fetch(
      `/api/building?${params}`,
    );
    const xml = await res.text();
    const doc = new DOMParser().parseFromString(xml, "application/xml");
    const item = doc.querySelector("item");
    if (!item) {
      content.innerHTML = `
      <div class="building-apt-name">${esc(d.apt)}</div>
      <div class="building-addr">${esc(d.dong)} · ${esc(d.area)}㎡ ${esc(d.floor)}층</div>
      <div class="state-msg" style="padding:20px 0">건축물대장 정보를 찾을 수 없습니다</div>`;
      return;
    }
    const g = (tag) => {
      const el = item.querySelector(tag);
      return el ? el.textContent.trim() : "—";
    };
    const fmtDate = (s) =>
      !s || s === "—" || s.length < 8
        ? s || "—"
        : `${s.slice(0, 4)}.${s.slice(4, 6)}.${s.slice(6, 8)}`;
    content.innerHTML = `
    <div class="building-apt-name">${esc(d.apt)}</div>
    <div class="building-addr">${esc(d.dong)} · 거래가 ${formatPrice(d.price)}</div>
    <div class="building-grid">
      <div class="building-item"><div class="building-item-label">총 세대수</div><div class="building-item-val">${g("hhldCnt") !== "—" ? Number(g("hhldCnt")).toLocaleString() + " 세대" : "—"}</div></div>
      <div class="building-item"><div class="building-item-label">총 층수</div><div class="building-item-val">${g("grndFlrCnt") !== "—" ? g("grndFlrCnt") + "층" : "—"}</div></div>
      <div class="building-item"><div class="building-item-label">사용승인일</div><div class="building-item-val" style="font-size:13px">${fmtDate(g("useAprDay"))}</div></div>
      <div class="building-item"><div class="building-item-label">건축년도</div><div class="building-item-val">${d.buildYear ? d.buildYear + "년" : "—"}</div></div>
      <div class="building-item"><div class="building-item-label">용적률</div><div class="building-item-val">${g("vlRat") !== "—" ? g("vlRat") + "%" : "—"}</div></div>
      <div class="building-item"><div class="building-item-label">건폐율</div><div class="building-item-val">${g("bcRat") !== "—" ? g("bcRat") + "%" : "—"}</div></div>
    </div>`;
  } catch (e) {
    content.innerHTML = `<div class="state-msg">오류: ${esc(e.message)}</div>`;
  }
}

// ── 엑셀 다운로드 ──
export function exportTradeExcel() {
  const search = document.getElementById("searchInput")?.value || "";
  const filtered = getSorted(
    getFiltered(state.allData, search, state.colFilters, state.cond),
    state.sortKey,
    state.sortDir,
  );
  if (filtered.length === 0) return alert("다운로드할 데이터가 없습니다.");
  const headers = ["단지명", "구/군", "거래가(만원)", "신고가", "면적(㎡)", "평", "층", "세대수", "준공년월", "근처역", "거래일"];
  const rows = filtered.map((d) => [
    d.apt,
    d.gu,
    d.price,
    d.newHigh ? "신고가" : "",
    d.area,
    sqmToPyeong(d.area),
    d.floor,
    d.households || "",
    d.completionYearMonth || "",
    d.nearbyStation || "",
    d.date,
  ]);
  downloadCsv(`실거래가_${new Date().toISOString().slice(0, 10)}.csv`, headers, rows);
}

// ── 초기화 ──
registerRenderCallback('trade', () => renderTable());

export function initTrade() {
  document
    .getElementById("buildingModal")
    .addEventListener("click", function (e) {
      if (e.target === this) closeBuildingModal();
    });
}

export function activateTrade() {
  // 탭 활성화 시 필요한 동작
}
