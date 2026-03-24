import { state } from '../state.js';
import {
  esc,
  formatPrice,
  fetchApiJson,
  getRecentYearMonths,
} from '../utils.js';
import { initRegionPair, DEFAULT_REGION_CODE } from '../regions.js';
import { renderThead, renderPagination, getSorted, registerRenderCallback } from '../table-utils.js';

function sqmToPyeong(sqm) {
  return Math.round(sqm / 3.305);
}

const RENT_API = {
  apt: "apt-rent",
  offi: "offi-rent",
  villa: "villa-rent",
  house: "house-rent",
};

// ── select 빌드 ──
export function buildRentSelects() {
  initRegionPair(
    "rentSidoSelect",
    "rentRegionSelect",
    DEFAULT_REGION_CODE,
  );
  const ms = document.getElementById("rentMonthSelect");
  ms.innerHTML = "";
  getRecentYearMonths(6).forEach(({ value, label }) => {
    ms.innerHTML += `<option value="${value}">${label}</option>`;
  });
}

// ── 필터 ──
export function setRentPropertyType(t) {
  state.rentPropertyType = t;
  document
    .querySelectorAll("#rentTypeBar .type-chip")
    .forEach((b) => b.classList.toggle("active", b.dataset.type === t));
}

export function setRentType(t) {
  state.rentTypeFilter = t;
  document
    .querySelectorAll(".rent-type-btn")
    .forEach((b) => b.classList.toggle("active", b.dataset.rt === t));
  state.rentPage = 1;
  renderRentTable();
}

export function getFilteredRent() {
  let f = state.rentData.slice();
  const q = document
    .getElementById("rentSearchInput")
    .value.toLowerCase();
  if (q) f = f.filter((d) => d.apt.toLowerCase().includes(q));
  if (state.rentTypeFilter === "jeonse") f = f.filter((d) => d.monthly === 0);
  else if (state.rentTypeFilter === "monthly")
    f = f.filter((d) => d.monthly > 0);
  Object.entries(state.rentColFilters).forEach(([k, v]) => {
    if (v != null) f = f.filter((d) => String(d[k]) === String(v));
  });
  return f;
}

// ── API 호출 — 전월세 ──
export async function loadRentData() {
  const region = document.getElementById("rentRegionSelect").value;
  const month = document.getElementById("rentMonthSelect").value;
  const endpoint = RENT_API[state.rentPropertyType];
  const url = `/api/${endpoint}?LAWD_CD=${region}&DEAL_YMD=${month}&numOfRows=1000&pageNo=1`;

  state.rentStateMessage = "로딩 중...";
  document.getElementById("rentTableBody").innerHTML =
    `<tr><td colspan="8" class="state-msg">⏳ 로딩 중...</td></tr>`;
  try {
    const payload = await fetchApiJson(url);
    state.rentData = payload.items || [];
    state.rentStateMessage =
      state.rentData.length === 0
        ? "선택한 지역/월의 전월세 데이터가 없습니다"
        : "";
  } catch (e) {
    console.error(e);
    state.rentStateMessage = `오류: ${e.message}`;
    state.rentData = [];
  }
  state.rentPage = 1;
  renderRentTable();
}

// ── 렌더링 — 전월세 ──
export function renderRentTable() {
  renderThead(state.RENT_COLS, state.rentSortKey, state.rentSortDir, state.rentColFilters, true);
  const filtered = getSorted(getFilteredRent(), state.rentSortKey, state.rentSortDir);
  const total = filtered.length;
  const totalPages = Math.max(1, Math.ceil(total / state.PAGE_SIZE));
  if (state.rentPage > totalPages) state.rentPage = 1;
  const start = (state.rentPage - 1) * state.PAGE_SIZE;
  const page = filtered.slice(start, start + state.PAGE_SIZE);

  const tbody = document.getElementById("rentTableBody");
  if (page.length === 0) {
    const emptyMessage =
      total === 0
        ? state.rentStateMessage || "데이터가 없습니다"
        : "검색/필터 결과가 없습니다";
    tbody.innerHTML = `<tr><td colspan="8" class="state-msg">${esc(emptyMessage)}</td></tr>`;
  } else {
    tbody.innerHTML = page
      .map((d) => {
        const pyeong = sqmToPyeong(d.area);
        const typeClass = d.type === "전세" ? "change-up" : "change-down";
        return `<tr>
          <td>${esc(d.apt)}</td>
          <td>${esc(d.gu)}</td>
          <td><span class="${typeClass}">${esc(d.type)}</span></td>
          <td class="price">${formatPrice(d.deposit)}</td>
          <td class="price">${d.monthly > 0 ? d.monthly + "만" : "—"}</td>
          <td class="area">${esc(d.area)}㎡ / ${pyeong}평</td>
          <td>${esc(d.floor)}층</td>
          <td class="date">${esc(d.date)}</td>
        </tr>`;
      })
      .join("");
  }
  renderPagination("rentPagination", state.rentPage, totalPages, (n) => {
    state.rentPage = n;
    renderRentTable();
  });
}

// ── 초기화 ──
registerRenderCallback('rent', () => renderRentTable());

export function initRent() {
  // 전월세 탭 이벤트 리스너 설정
}

export function activateRent() {
  // 탭 활성화 시 필요한 동작
}
