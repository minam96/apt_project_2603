// ══════════════════════════════════════════════════════════
// brokers.js — 부동산 찾기 탭
// ══════════════════════════════════════════════════════════

import { esc, fetchApiJson, debounce, downloadCsv } from '../utils.js';
import { state, BROKER_COLS, BROKER_PAGE_SIZE } from '../state.js';

const SEOUL_GU_LIST = [
  { code: "11110", name: "종로구" },
  { code: "11140", name: "중구" },
  { code: "11170", name: "용산구" },
  { code: "11200", name: "성동구" },
  { code: "11215", name: "광진구" },
  { code: "11230", name: "동대문구" },
  { code: "11260", name: "중랑구" },
  { code: "11290", name: "성북구" },
  { code: "11305", name: "강북구" },
  { code: "11320", name: "도봉구" },
  { code: "11350", name: "노원구" },
  { code: "11380", name: "은평구" },
  { code: "11410", name: "서대문구" },
  { code: "11440", name: "마포구" },
  { code: "11470", name: "양천구" },
  { code: "11500", name: "강서구" },
  { code: "11530", name: "구로구" },
  { code: "11545", name: "금천구" },
  { code: "11560", name: "영등포구" },
  { code: "11590", name: "동작구" },
  { code: "11620", name: "관악구" },
  { code: "11650", name: "서초구" },
  { code: "11680", name: "강남구" },
  { code: "11710", name: "송파구" },
  { code: "11740", name: "강동구" },
];

// ── 드롭다운 초기화 ──
export function buildBrokerSelects() {
  const guSelect = document.getElementById("brokerGuSelect");
  if (!guSelect) return;
  guSelect.innerHTML =
    '<option value="">-- 자치구 선택 --</option>' +
    SEOUL_GU_LIST.map(
      (gu) => `<option value="${esc(gu.code)}">${esc(gu.name)}</option>`
    ).join("");

  guSelect.addEventListener("change", () => {
    state.brokerGu = guSelect.value;
    state.brokerDong = "";
    state.brokerPage = 1;
    // 동 드롭다운은 데이터 로드 후 업데이트
    const dongSelect = document.getElementById("brokerDongSelect");
    if (dongSelect) {
      dongSelect.innerHTML = '<option value="">전체 동</option>';
    }
  });

  const dongSelect = document.getElementById("brokerDongSelect");
  if (dongSelect) {
    dongSelect.addEventListener("change", () => {
      state.brokerDong = dongSelect.value;
      state.brokerPage = 1;
      renderBrokerTable();
    });
  }
}

// ── 정렬 변경 ──
export function setBrokerSort(sort) {
  state.brokerSort = sort;
  state.brokerPage = 1;
  document.querySelectorAll("#tabBrokers .cond-btn").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.sort === sort);
  });
  loadBrokerData();
}

// ── 데이터 로드 ──
export async function loadBrokerData() {
  const guCode = state.brokerGu || document.getElementById("brokerGuSelect")?.value;
  if (!guCode) {
    state.brokerStateMessage = "자치구를 선택해주세요";
    renderBrokerTable();
    return;
  }
  state.brokerGu = guCode;
  state.brokerLoading = true;
  state.brokerStateMessage = "중개업소 데이터를 불러오는 중...";
  renderBrokerTable();

  try {
    const searchQuery = document.getElementById("brokerSearchInput")?.value?.trim() || "";
    const params = new URLSearchParams({
      guCode,
      sort: state.brokerSort,
      page: String(state.brokerPage),
      pageSize: String(BROKER_PAGE_SIZE),
    });
    if (state.brokerDong) params.set("dong", state.brokerDong);
    if (searchQuery) params.set("q", searchQuery);

    const data = await fetchApiJson(`/api/seoul/brokers?${params}`);
    state.brokerData = data.items || [];
    state.brokerTotalCount = data.totalCount || 0;
    state.brokerSummary = data.summary || null;
    state.brokerDongs = data.dongs || [];
    state.brokerLoaded = true;
    state.brokerStateMessage = "";

    // 동 드롭다운 업데이트
    updateDongSelect();
  } catch (err) {
    state.brokerStateMessage = `오류: ${err.message}`;
    state.brokerData = [];
  } finally {
    state.brokerLoading = false;
    renderBrokerTable();
  }
}

function updateDongSelect() {
  const dongSelect = document.getElementById("brokerDongSelect");
  if (!dongSelect || !state.brokerDongs.length) return;
  const currentDong = state.brokerDong;
  dongSelect.innerHTML =
    '<option value="">전체 동</option>' +
    state.brokerDongs
      .map((d) => `<option value="${esc(d)}"${d === currentDong ? " selected" : ""}>${esc(d)}</option>`)
      .join("");
}

// ── 요약 카드 렌더링 ──
function renderBrokerSummary() {
  const el = document.getElementById("brokerSummary");
  if (!el) return;
  const s = state.brokerSummary;
  if (!s) {
    el.innerHTML = "";
    return;
  }
  el.innerHTML = `
    <div class="broker-summary-card">
      <span class="broker-stat"><strong>${s.total}</strong>개 업소</span>
      <span class="broker-stat">평균 <strong>${s.avgBusinessYears}</strong>년차</span>
      <span class="broker-stat broker-stat-clean">처분없음 <strong>${s.cleanCount}</strong>곳</span>
      <span class="broker-stat broker-stat-veteran">10년+ <strong>${s.veteranCount}</strong>곳</span>
    </div>
  `;
}

// ── 배지 생성 ──
function yearsBadge(years) {
  if (years == null) return `<span class="broker-badge unknown">-</span>`;
  if (years >= 10)
    return `<span class="broker-badge veteran">${years}년차</span>`;
  if (years >= 5)
    return `<span class="broker-badge experienced">${years}년차</span>`;
  return `<span class="broker-badge newcomer">${years}년차</span>`;
}

function adminBadge(item) {
  if (!item.hasAdminAction)
    return `<span class="broker-badge clean">처분없음</span>`;
  return `<span class="broker-badge warning" title="${esc(item.adminActionPeriod || "")}">처분이력</span>`;
}

function trustBadge(score) {
  if (score >= 25)
    return `<span class="broker-badge trust-high">신뢰</span>`;
  if (score >= 10)
    return `<span class="broker-badge trust-mid">보통</span>`;
  return `<span class="broker-badge trust-low">주의</span>`;
}

// ── 테이블 렌더링 ──
export function renderBrokerTable() {
  const thead = document.getElementById("brokerTableHead");
  const tbody = document.getElementById("brokerTableBody");
  if (!thead || !tbody) return;

  // Header
  thead.innerHTML = `<tr>${BROKER_COLS.map(
    (col) => `<th>${esc(col.label)}</th>`
  ).join("")}</tr>`;

  // Summary
  renderBrokerSummary();

  // State message
  if (state.brokerStateMessage || state.brokerLoading) {
    tbody.innerHTML = `<tr><td colspan="${BROKER_COLS.length}" class="state-msg">${esc(
      state.brokerStateMessage || "로딩 중..."
    )}</td></tr>`;
    return;
  }

  if (!state.brokerLoaded) {
    tbody.innerHTML = `<tr><td colspan="${BROKER_COLS.length}" class="state-msg">자치구를 선택하고 검색해주세요</td></tr>`;
    return;
  }

  if (state.brokerData.length === 0) {
    tbody.innerHTML = `<tr><td colspan="${BROKER_COLS.length}" class="state-msg">검색 결과가 없습니다</td></tr>`;
    return;
  }

  // Rows
  tbody.innerHTML = state.brokerData
    .map(
      (item) => `<tr>
      <td>
        <div class="broker-name">${esc(item.companyName || "-")}</div>
        <div class="broker-addr">${esc(item.address || "")}</div>
      </td>
      <td>${esc(item.representativeName || "-")}</td>
      <td>${esc(item.dong || "-")}</td>
      <td>${yearsBadge(item.businessYears)}</td>
      <td>${adminBadge(item)}</td>
      <td>${item.tel ? `<a href="tel:${esc(item.tel)}" class="broker-tel">${esc(item.tel)}</a>` : "-"}</td>
      <td>${trustBadge(item.trustScore)}</td>
    </tr>`
    )
    .join("");

  // Pagination
  renderBrokerPagination();
}

function renderBrokerPagination() {
  const el = document.getElementById("brokerPagination");
  if (!el) return;
  const totalPages = Math.ceil(state.brokerTotalCount / BROKER_PAGE_SIZE);
  if (totalPages <= 1) {
    el.innerHTML = "";
    return;
  }
  const page = state.brokerPage;
  let html = "";
  if (page > 1) {
    html += `<button onclick="setBrokerPage(${page - 1})">이전</button>`;
  }
  const start = Math.max(1, page - 2);
  const end = Math.min(totalPages, page + 2);
  for (let i = start; i <= end; i++) {
    html += `<button class="${i === page ? "active" : ""}" onclick="setBrokerPage(${i})">${i}</button>`;
  }
  if (page < totalPages) {
    html += `<button onclick="setBrokerPage(${page + 1})">다음</button>`;
  }
  el.innerHTML = html;
}

export function setBrokerPage(page) {
  state.brokerPage = page;
  loadBrokerData();
}

// ── 엑셀 다운로드 ──
export function exportBrokerExcel() {
  if (state.brokerData.length === 0) return alert("다운로드할 데이터가 없습니다.");
  const headers = ["상호명", "대표", "동", "주소", "연락처", "영업연차", "행정처분", "신뢰도점수"];
  const rows = state.brokerData.map((d) => [
    d.companyName || "",
    d.representativeName || "",
    d.dong || "",
    d.address || "",
    d.tel || "",
    d.businessYears ?? "",
    d.hasAdminAction ? "처분이력" : "처분없음",
    d.trustScore ?? "",
  ]);
  const gu = state.brokerGu ? `_${document.getElementById("brokerGuSelect")?.selectedOptions?.[0]?.text || state.brokerGu}` : "";
  downloadCsv(`부동산찾기${gu}_${new Date().toISOString().slice(0, 10)}.csv`, headers, rows);
}

// ── 초기화 ──
export function initBrokers() {
  const searchInput = document.getElementById("brokerSearchInput");
  if (searchInput) {
    searchInput.addEventListener(
      "keydown",
      (e) => {
        if (e.key === "Enter") {
          state.brokerPage = 1;
          loadBrokerData();
        }
      }
    );
  }
}
