import { state } from '../state.js';
import {
  esc,
  formatPrice,
  fetchApiJson,
  normalizeSearchText,
  formatNearbyStation,
  formatNearbyAmenity,
  formatFlatLandStatus,
  isListingLocationInsightsPending,
  hasListingLocationInsightFields,
  detectListingLocationInsightsStatus,
  downloadCsv,
} from '../utils.js';
import { getRegionName, initRegionPair, DEFAULT_REGION_CODE } from '../regions.js';
import { renderPagination, closeAllFilters } from '../table-utils.js';

const LISTING_STATUS_LABELS = {
  idle: "대기",
  loading: "확인중",
  ok: "정상",
  estimated: "추정치 포함",
  kapt_forbidden: "보강 제한",
  empty_body: "응답 비어있음",
  no_match: "매칭 없음",
  config_error: "키 미설정",
  upstream_error: "호출 오류",
};

export function getListingFeasibilityLabel(row) {
  // feasibilityReason 기반 라벨 (서버에서 사유 전달)
  if (row?.feasibilityReason === "age_ineligible") return "연한미달";
  if (row?.feasibilityReason === "far_inversion") return "용적률 역전";
  if (row?.feasibility == null || row.feasibility === "") {
    if (row?.isGenericZoning) return "용도 미세분";
    return "계산불가";
  }
  if (row?.generalSaleUnits != null && row.generalSaleUnits < 0) {
    return "NO (역전)";
  }
  return row.analysisMode === "estimated"
    ? `추정 ${row.feasibility}`
    : row.feasibility;
}

export function getListingFeasibilityClass(row) {
  if (row?.feasibilityReason === "age_ineligible") return "age-ineligible";
  if (row?.feasibilityReason === "far_inversion") return "no";
  if (row?.analysisMode === "estimated" && row?.feasibility) {
    return "estimated";
  }
  if (row?.feasibility === "YES") return "yes";
  if (row?.feasibility === "NO") return "no";
  return "unavailable";
}

export function getListingBadges(row) {
  const badges = [];
  if (row?.analysisReason === "building_hub_unavailable") {
    badges.push("공식데이터 미가용");
  } else if (row?.analysisReason === "kapt_forbidden") {
    badges.push("KAPT 승인 필요");
  } else if (row?.analysisReason === "insufficient_fields") {
    badges.push("계산불가");
  } else if (row?.analysisReason === "suspicious_metrics") {
    badges.push("검증 필요");
  } else if (row?.analysisReason === "site_area_unverified") {
    badges.push("대지면적 검증");
  }
  if (row?.siteAreaCorrection === "gross_floor_area_divided_by_far") {
    badges.push("대지면적 보정");
  }
  if (
    !row?.zoning &&
    [
      "config_error",
      "forbidden",
      "rate_limited",
      "upstream_error",
    ].includes(row?.zoningStatus)
  ) {
    badges.push("용도지역 미가용");
  }
  if (row?.analysisMode === "estimated") {
    badges.push("추정치");
  }
  if (row?.parcelSource === "vworld_pnu") {
    badges.push("PNU 보정");
  }
  return badges;
}

export function getListingFilterValue(row, key) {
  if (key === "feasibility") {
    return getListingFeasibilityLabel(row);
  }
  const value = row?.[key];
  if (value == null || value === "") return "—";
  if (typeof value === "number") {
    return Number.isInteger(value) ? String(value) : value.toFixed(2);
  }
  return String(value);
}

export function formatListingMetric(value, suffix = "", digits = 0) {
  if (value == null || value === "") return "—";
  const num = Number(value);
  if (!Number.isFinite(num) || Math.abs(num) < Number.EPSILON) return "—";
  return `${num.toLocaleString("ko-KR", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })}${suffix}`;
}

export function updateListingZoneFilterOptions() {
  const select = document.getElementById("listingZoneFilter");
  if (!select) return;
  const current = select.value || "all";
  const zones = [
    ...new Set(state.listingData.map((item) => item.zoning).filter(Boolean)),
  ].sort((a, b) => a.localeCompare(b, "ko"));
  select.innerHTML =
    `<option value="all">용도지역 전체</option>` +
    zones
      .map((zone) => `<option value="${esc(zone)}">${esc(zone)}</option>`)
      .join("");
  select.value = zones.includes(current) ? current : "all";
}

export function getFilteredListings() {
  let filtered = state.listingData.slice();
  const query = normalizeSearchText(
    document.getElementById("listingSearch")?.value || "",
  );
  const zone =
    document.getElementById("listingZoneFilter")?.value || "all";
  const feasibility =
    document.getElementById("listingFeasibilityFilter")?.value || "all";

  if (query) {
    filtered = filtered.filter((row) =>
      normalizeSearchText(row.apt || "").includes(query),
    );
  }
  if (zone !== "all") {
    filtered = filtered.filter((row) => row.zoning === zone);
  }
  if (feasibility !== "all") {
    filtered = filtered.filter((row) =>
      feasibility === "UNAVAILABLE"
        ? row.feasibility == null
        : row.feasibility === feasibility,
    );
  }
  const ageFilterOn =
    document.getElementById("listingAgeFilter")?.checked ?? false;
  if (ageFilterOn) {
    filtered = filtered.filter(
      (row) => row.reconstructionEligible !== false,
    );
  }

  Object.entries(state.listingColFilters).forEach(([key, value]) => {
    if (value != null) {
      filtered = filtered.filter(
        (row) => getListingFilterValue(row, key) === String(value),
      );
    }
  });

  if (state.listingSortKey) {
    filtered.sort((a, b) => {
      let va = a[state.listingSortKey];
      let vb = b[state.listingSortKey];
      const aMissing = va == null || va === "";
      const bMissing = vb == null || vb === "";
      if (aMissing && bMissing) return 0;
      if (aMissing) return 1;
      if (bMissing) return -1;
      if (typeof va === "string") {
        va = va.toLowerCase();
        vb = String(vb || "").toLowerCase();
      }
      if (va < vb) return state.listingSortDir === "asc" ? -1 : 1;
      if (va > vb) return state.listingSortDir === "asc" ? 1 : -1;
      return 0;
    });
  }

  return filtered;
}

export function getListingMetrics(filtered) {
  const confirmedRows = filtered.filter(
    (row) => row.analysisMode === "confirmed",
  );
  const estimatedRows = filtered.filter(
    (row) => row.analysisMode === "estimated",
  );
  const unavailableRows = filtered.filter(
    (row) => row.analysisMode === "unavailable",
  );

  return {
    visibleCount: filtered.length,
    confirmedCount: confirmedRows.length,
    confirmedYesCount: confirmedRows.filter(
      (row) => row.feasibility === "YES",
    ).length,
    estimatedCount: estimatedRows.length,
    estimatedYesCount: estimatedRows.filter(
      (row) => row.feasibility === "YES",
    ).length,
    unavailableCount: unavailableRows.length,
  };
}

export function getListingSummaryStatus(summary, metrics) {
  if (state.listingLoading) return "loading";
  if (!state.listingLoaded) return "idle";
  if (
    summary?.kaptStatus === "forbidden" &&
    Number(summary?.kaptBlockedCount || 0) > 0
  ) {
    return "kapt_forbidden";
  }
  if (metrics.estimatedCount > 0) return "estimated";
  return summary?.buildingHubStatus || "upstream_error";
}

export function renderListingSummary(filtered) {
  const metrics = getListingMetrics(filtered);
  const status = getListingSummaryStatus(state.listingSummaryData, metrics);
  const statusCard = document.getElementById("listingStatusCard");
  const sourceCard = document.getElementById("listingSourceCard");

  document.getElementById("listingSumVisible").textContent =
    metrics.visibleCount.toLocaleString();
  document.getElementById("listingSumYes").textContent =
    metrics.estimatedYesCount > 0
      ? `${metrics.confirmedYesCount.toLocaleString()} (+추정 ${metrics.estimatedYesCount.toLocaleString()})`
      : metrics.confirmedYesCount.toLocaleString();
  document.getElementById("listingSumSource").textContent =
    metrics.confirmedCount.toLocaleString();
  document.getElementById("listingSumStatus").textContent =
    LISTING_STATUS_LABELS[status] || "확인 필요";

  statusCard?.classList.toggle(
    "warning",
    !["ok", "idle", "loading"].includes(status),
  );
  sourceCard?.classList.toggle(
    "warning",
    metrics.estimatedCount > 0 ||
      metrics.unavailableCount > 0 ||
      !["ok", "idle", "loading"].includes(status),
  );
}

export function getListingStatusNoteModel(summary) {
  const buildingStatus =
    summary?.buildingHubStatus ||
    (state.listingLoaded ? "upstream_error" : "idle");
  const kaptStatus = summary?.kaptStatus || "";
  const kaptBlockedCount = Number(summary?.kaptBlockedCount || 0);
  const landUseBlockedCount = Number(summary?.landUseBlockedCount || 0);
  const estimatedCount = Number(summary?.estimatedCount || 0);
  const estimatedYesCount = Number(summary?.estimatedYesCount || 0);
  const pnuFallbackCount = Number(summary?.pnuFallbackCount || 0);
  const guardedCount = Number(summary?.guardedCount || 0);
  const siteAreaUnverifiedCount = Number(
    summary?.siteAreaUnverifiedCount || 0,
  );
  const siteAreaCorrectedCount = Number(
    summary?.siteAreaCorrectedCount || 0,
  );
  const locationInsightsStatus = summary?.locationInsightsStatus || "";

  if (buildingStatus === "loading") {
    return {
      warning: false,
      text: "건축HUB 응답 상태를 확인하는 중입니다...",
    };
  }
  if (buildingStatus === "idle") {
    return {
      warning: false,
      text: "최근 실거래로 확인된 단지를 기준으로 건축HUB, KAPT, 브이월드 보조조회까지 묶어 계산합니다. 근처 역·초등학교·공원은 직선거리 기준 도보 10분 내 여부를 추정하고, 평지는 주변 표고 차이로 추정합니다. 검색창에 단지명을 입력한 뒤 Enter 또는 조회 버튼으로 서버 재조회를 할 수 있고, 이후 필터는 현재 화면 데이터에만 적용됩니다.",
    };
  }
  if (buildingStatus === "ok") {
    if (locationInsightsStatus === "deferred") {
      return {
        warning: false,
        text: "사업성 핵심 수치를 먼저 불러왔고, 근처 역·평지·초등학교·공원 컬럼은 현재 페이지 단지부터 순차적으로 채우는 중입니다.",
      };
    }
    if (locationInsightsStatus === "partial") {
      return {
        warning: false,
        text: "사업성 표는 먼저 표시했고, 위치·생활편의 컬럼은 현재 보는 페이지 기준으로 필요한 행만 추가 조회합니다.",
      };
    }
    if (locationInsightsStatus === "upstream_error") {
      return {
        warning: true,
        text: "사업성 표는 먼저 불러왔지만 위치·생활편의 보조 조회 중 일부가 실패했습니다. 새로고침하면 다시 조회합니다.",
      };
    }
    if (locationInsightsStatus === "stale_backend") {
      return {
        warning: true,
        text: "사업성 분석 위치 컬럼을 지원하지 않는 이전 API 서버가 응답 중입니다. `node server.js`를 다시 실행하거나 개발 서버를 재시작해 최신 백엔드로 갱신해 주세요.",
      };
    }
    if (kaptStatus === "forbidden" && kaptBlockedCount > 0) {
      return {
        warning: true,
        text:
          summary?.kaptMessage ||
          `건축HUB는 응답했지만 현재 API 키로는 KAPT 보강 API를 호출할 수 없습니다. ${kaptBlockedCount}개 단지는 계산불가로 남습니다.`,
      };
    }
    if (landUseBlockedCount > 0) {
      const pnuSuffix =
        pnuFallbackCount > 0
          ? ` 브이월드/PNU 보정 ${pnuFallbackCount}건을 적용했습니다.`
          : "";
      return {
        warning: true,
        text: `브이월드 2D 데이터 API에서 용도지역을 확인하지 못한 단지가 ${landUseBlockedCount}개 있습니다. 이 때문에 확정 계산이 제한되며, ${estimatedCount}개 단지는 추정치로 표시했습니다. 추정 YES는 ${estimatedYesCount}건입니다. VWORLD_DATA_API_KEY 또는 VWORLD_DATA_DOMAIN 설정을 확인해 주세요.${pnuSuffix}`,
      };
    }
    if (siteAreaUnverifiedCount > 0) {
      const correctedSuffix =
        siteAreaCorrectedCount > 0
          ? ` 연면적으로 보이는 면적 ${siteAreaCorrectedCount}건은 용적률 기준 대지면적으로 보정했습니다.`
          : "";
      return {
        warning: true,
        text: `대지면적 필드에 연면적이 섞인 것으로 의심되는 ${siteAreaUnverifiedCount}건은 계산에서 제외했습니다.${correctedSuffix} 대지면적이 확실하지 않은 단지는 사업성 판단을 계산불가로 남깁니다.`,
      };
    }
    if (siteAreaCorrectedCount > 0) {
      return {
        warning: true,
        text: `대지면적 대신 연면적으로 보이는 면적 ${siteAreaCorrectedCount}건은 현재 용적률을 이용해 대지면적으로 보정했습니다. 보정이 불가능한 단지는 계산에서 제외합니다.`,
      };
    }
    if (guardedCount > 0) {
      const pnuSuffix =
        pnuFallbackCount > 0
          ? ` 브이월드/PNU 보정 ${pnuFallbackCount}건을 적용했습니다.`
          : "";
      const estimatedSuffix =
        estimatedCount > 0
          ? ` 나머지 ${estimatedCount}개 단지는 추정치로 표시했습니다.`
          : "";
      return {
        warning: true,
        text: `추정치 또는 PNU 보정 결과 중 대지면적과 세대수 조합이 비정상적으로 보여 ${guardedCount}개 단지는 계산에서 제외했습니다.${estimatedSuffix}${pnuSuffix}`,
      };
    }
    if (estimatedCount > 0) {
      const pnuSuffix =
        pnuFallbackCount > 0
          ? ` 브이월드/PNU 보정 ${pnuFallbackCount}건을 적용했습니다.`
          : "";
      return {
        warning: true,
        text: `법정상한 또는 현재 용적률이 비어 있는 ${estimatedCount}개 단지는 추정치로 표시했습니다. 추정 YES는 ${estimatedYesCount}건입니다.${pnuSuffix}`,
      };
    }
    return {
      warning: false,
      text: "최근 실거래로 확인된 단지를 기준으로 건축HUB, KAPT, 브이월드 보조조회까지 묶어 계산합니다. 근처 역·초등학교·공원은 직선거리 기준 도보 10분 내 여부를 추정하고, 평지는 주변 표고 차이로 추정합니다. 최근 거래가 없던 단지는 표에 바로 보이지 않을 수 있습니다.",
    };
  }
  if (buildingStatus === "empty_body") {
    return {
      warning: true,
      text: "건축HUB 응답 본문이 비어 있어 공식 계산값을 만들 수 없습니다. data.go.kr에서 건축HUB 사용신청과 승인 상태를 먼저 확인해 주세요.",
    };
  }
  if (buildingStatus === "no_match") {
    return {
      warning: true,
      text: "건축HUB가 응답했지만 현재 조회 조건과 일치하는 공식 건축물대장이 없습니다. 일부 단지는 추정치 또는 계산불가로 표시됩니다.",
    };
  }
  if (buildingStatus === "config_error") {
    return {
      warning: true,
      text: "DATA_GO_KR_API_KEY 설정이 없어 건축HUB 공식 데이터를 조회할 수 없습니다.",
    };
  }
  return {
    warning: true,
    text: "건축HUB 호출 오류로 공식 계산값을 불러오지 못했습니다. 잠시 후 다시 시도해 주세요.",
  };
}

export function updateListingStatusNote(summary) {
  const note = document.getElementById("listingStatusNote");
  if (!note) return;
  const model = getListingStatusNoteModel(summary);
  note.classList.toggle("warning", model.warning);
  note.textContent = model.text;
}

export function toggleListingSort(key) {
  if (state.listingSortKey === key) {
    state.listingSortDir = state.listingSortDir === "asc" ? "desc" : "asc";
  } else {
    state.listingSortKey = key;
    state.listingSortDir = key === "apt" || key === "zoning" ? "asc" : "desc";
  }
  renderListingTable();
}

export function toggleListingColFilter(key, btn) {
  closeAllFilters();
  const values = [
    ...new Set(state.listingData.map((row) => getListingFilterValue(row, key))),
  ].sort((a, b) => String(a).localeCompare(String(b), "ko"));
  const dd = document.createElement("div");
  dd.className = "col-filter-dd";

  const all = document.createElement("div");
  all.textContent = "전체";
  if (state.listingColFilters[key] == null) all.classList.add("active");
  all.onclick = () => {
    delete state.listingColFilters[key];
    closeAllFilters();
    state.listingPage = 1;
    renderListingTable();
  };
  dd.appendChild(all);

  values.forEach((value) => {
    const item = document.createElement("div");
    item.textContent = value;
    if (String(state.listingColFilters[key]) === String(value)) {
      item.classList.add("active");
    }
    item.onclick = () => {
      state.listingColFilters[key] = value;
      closeAllFilters();
      state.listingPage = 1;
      renderListingTable();
    };
    dd.appendChild(item);
  });

  btn.closest("th").appendChild(dd);
  state.openFilter = dd;
}

export function renderListingHead() {
  const tr = document.createElement("tr");
  state.LISTING_COLS.forEach((col) => {
    const th = document.createElement("th");
    th.scope = "col";
    const isSortable = col.sortable !== false;
    const isFilterable = col.filterable !== false;
    if (isSortable && state.listingSortKey === col.key)
      th.classList.add("sort-active");
    if (isFilterable && state.listingColFilters[col.key] != null)
      th.classList.add("filter-active");
    th.setAttribute(
      "aria-sort",
      isSortable && state.listingSortKey === col.key
        ? state.listingSortDir === "asc"
          ? "ascending"
          : "descending"
        : "none",
    );

    const inner = document.createElement("span");
    inner.className = `th-inner${isSortable ? " th-sortable" : ""}`;
    if (isSortable) {
      inner.onclick = () => toggleListingSort(col.key);
      inner.tabIndex = 0;
      inner.setAttribute("role", "button");
      inner.setAttribute("aria-label", `${col.label} 정렬`);
      inner.title = `${col.label} 정렬`;
      inner.onkeydown = (event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          toggleListingSort(col.key);
        }
      };
    }

    const label = document.createElement("span");
    label.textContent = col.label;
    inner.appendChild(label);

    if (isSortable) {
      const icon = document.createElement("span");
      icon.className = "sort-icon";
      icon.textContent =
        state.listingSortKey === col.key
          ? state.listingSortDir === "asc"
            ? "▲"
            : "▼"
          : "⇅";
      inner.appendChild(icon);
    }

    th.appendChild(inner);

    if (isFilterable) {
      const fb = document.createElement("button");
      fb.className = "filter-btn";
      fb.type = "button";
      fb.title = `${col.label} 필터`;
      fb.setAttribute("aria-label", `${col.label} 필터`);
      fb.textContent = state.listingColFilters[col.key] != null ? "●" : "▾";
      fb.onclick = (event) => {
        event.stopPropagation();
        toggleListingColFilter(col.key, fb);
      };
      th.appendChild(fb);
    }

    tr.appendChild(th);
  });
  const thead = document.getElementById("listingTableHead");
  thead.innerHTML = "";
  thead.appendChild(tr);
}

export function renderListingRow(row) {
  const aptUrl = `https://search.naver.com/search.naver?query=${encodeURIComponent(`${row.apt} 아파트`)}`;
  const badges = getListingBadges(row)
    .map((label) => `<span class="listing-badge">${label}</span>`)
    .join("");
  const feasibilityLabel = getListingFeasibilityLabel(row);
  const feasibilityClass = getListingFeasibilityClass(row);
  const locationPending = isListingLocationInsightsPending(row);
  const nearbyStationDisp = formatNearbyStation(
    row.nearbyStation,
    row.nearbyStationDistanceKm,
    locationPending,
  );
  const flatLandDisp = formatFlatLandStatus(
    row.flatLandStatus,
    locationPending,
  );
  const nearbySchoolDisp = formatNearbyAmenity(
    row.nearbyElementarySchool,
    row.nearbyElementarySchoolDistanceKm,
    row.nearbyElementarySchoolStatus,
    locationPending,
  );
  const nearbyParkDisp = formatNearbyAmenity(
    row.nearbyPark,
    row.nearbyParkDistanceKm,
    row.nearbyParkStatus,
    locationPending,
  );

  return `<tr>
      <td><a class="apt-link" href="${aptUrl}" target="_blank" rel="noopener">${esc(row.apt)}</a>${badges}</td>
      <td class="num">${row.completionYear ? `${esc(row.completionYear)}년${row.buildingAge != null ? ` <span class="muted" style="font-size:11px">(${row.buildingAge}년)</span>` : ""}` : "-"}</td>
      <td>${row.zoning ? esc(row.zoning) : `<span class="muted">-</span>`}</td>
      <td class="num">${(() => {
        const farDisp = formatListingMetric(row.legalFarLimit, "%");
        const badges = [];
        if (row.isGenericZoning && row.legalFarLimit != null)
          badges.push("추정");
        if (
          row.residentialFarRatio != null &&
          row.residentialFarRatio < 1
        )
          badges.push(
            `주거${Math.round(row.residentialFarRatio * 100)}%`,
          );
        const badgeHtml = badges
          .map((b) => `<span class="listing-badge">${b}</span>`)
          .join(" ");
        const effectiveDisp =
          row.effectiveFarLimit != null &&
          row.effectiveFarLimit !== row.legalFarLimit
            ? `<br><span class="muted" style="font-size:11px">실효 ${row.effectiveFarLimit}%</span>`
            : "";
        return row.isGenericZoning && row.legalFarLimit != null
          ? `<span class="muted">${farDisp}</span> ${badgeHtml}${effectiveDisp}`
          : `${farDisp} ${badgeHtml}${effectiveDisp}`;
      })()}</td>
      <td class="num">${formatListingMetric(row.currentFar, "%", 2)}</td>
      <td class="num">${formatListingMetric(row.siteArea, "㎡", 2)}</td>
      <td class="num">${formatListingMetric(row.households, "세대")}</td>
      <td>${nearbyStationDisp}</td>
      <td>${flatLandDisp}</td>
      <td>${nearbySchoolDisp}</td>
      <td>${nearbyParkDisp}</td>
      <td class="num">${formatListingMetric(row.landPerHousehold, "㎡", 2)}</td>
      <td class="num">${formatListingMetric(row.expectedUnits, "세대")}</td>
      <td class="num">${row.generalSaleUnits != null && row.generalSaleUnits < 0 ? `<span style="color:#e74c3c">${formatListingMetric(row.generalSaleUnits, "세대")}</span>` : formatListingMetric(row.generalSaleUnits, "세대")}</td>
      <td><span class="listing-pill ${feasibilityClass}">${feasibilityLabel}</span></td>
    </tr>`;
}

export async function hydrateListingLocationInsights(pageRows) {
  if (
    !state.listingLoaded ||
    state.listingLoading ||
    !state.listingLocationRequestKey ||
    !Array.isArray(pageRows) ||
    pageRows.length === 0
  ) {
    return;
  }

  const ids = [
    ...new Set(
      pageRows
        .filter((row) => isListingLocationInsightsPending(row))
        .map((row) => String(row?.id || "").trim())
        .filter((id) => id && !state.listingLocationInFlightIds.has(id)),
    ),
  ];

  if (!ids.length) {
    return;
  }

  ids.forEach((id) => state.listingLocationInFlightIds.add(id));
  const requestKey = state.listingLocationRequestKey;
  const hydrationSeq = ++state.listingLocationHydrationSeq;
  const params = new URLSearchParams(requestKey);
  ids.forEach((id) => params.append("id", id));

  try {
    const payload = await fetchApiJson(
      `http://localhost:3000/api/redevelopment-location-insights?${params.toString()}`,
    );
    if (
      requestKey !== state.listingLocationRequestKey ||
      hydrationSeq !== state.listingLocationHydrationSeq
    ) {
      return;
    }

    const patchMap = new Map(
      (payload?.items || [])
        .map((item) => [String(item?.id || "").trim(), item])
        .filter(([id]) => id),
    );
    if (!patchMap.size) {
      return;
    }

    let changed = false;
    state.listingData = state.listingData.map((row) => {
      const patch = patchMap.get(String(row?.id || "").trim());
      if (!patch) {
        return row;
      }
      changed = true;
      return {
        ...row,
        ...patch,
        locationInsightsPending: false,
      };
    });

    if (!changed) {
      return;
    }

    state.listingSummaryData = {
      ...(state.listingSummaryData || {}),
      locationInsightsStatus: state.listingData.some((row) =>
        isListingLocationInsightsPending(row),
      )
        ? "partial"
        : "ok",
    };
    renderListingTable();
    updateListingStatusNote(state.listingSummaryData);
  } catch (error) {
    console.error(error);
    if (
      requestKey !== state.listingLocationRequestKey ||
      hydrationSeq !== state.listingLocationHydrationSeq
    ) {
      return;
    }

    const failedIds = new Set(ids);
    let changed = false;
    state.listingData = state.listingData.map((row) => {
      const rowId = String(row?.id || "").trim();
      if (
        !failedIds.has(rowId) ||
        !isListingLocationInsightsPending(row)
      ) {
        return row;
      }
      changed = true;
      return {
        ...row,
        nearbyElementarySchoolStatus: "unknown",
        nearbyParkStatus: "unknown",
        flatLandStatus: "unknown",
        locationInsightsPending: false,
      };
    });

    if (!changed) {
      return;
    }

    state.listingSummaryData = {
      ...(state.listingSummaryData || {}),
      locationInsightsStatus: "upstream_error",
    };
    renderListingTable();
    updateListingStatusNote(state.listingSummaryData);
  } finally {
    if (requestKey === state.listingLocationRequestKey) {
      ids.forEach((id) => state.listingLocationInFlightIds.delete(id));
    }
  }
}

export function renderListingTable() {
  renderListingHead();
  const filtered = getFilteredListings();
  renderListingSummary(filtered);

  const totalPages = Math.max(
    1,
    Math.ceil(filtered.length / state.LISTING_PAGE_SIZE),
  );
  if (state.listingPage > totalPages) {
    state.listingPage = totalPages;
  }

  const start = (state.listingPage - 1) * state.LISTING_PAGE_SIZE;
  const page = filtered.slice(start, start + state.LISTING_PAGE_SIZE);
  const tbody = document.getElementById("listingTableBody");

  if (page.length === 0) {
    const emptyMessage =
      filtered.length === 0
        ? state.listingStateMessage || "표시할 데이터가 없습니다"
        : "검색/필터 결과가 없습니다";
    tbody.innerHTML = `<tr><td colspan="15" class="state-msg">${esc(emptyMessage)}</td></tr>`;
  } else {
    tbody.innerHTML = page.map((row) => renderListingRow(row)).join("");
  }

  renderPagination(
    "listingPagination",
    state.listingPage,
    totalPages,
    (pageNo) => {
      state.listingPage = pageNo;
      renderListingTable();
    },
  );

  if (page.length > 0) {
    hydrateListingLocationInsights(page);
  }
}

export async function loadListingData() {
  const regionCode = document.getElementById("listingRegionSelect").value;
  const query = document.getElementById("listingSearch").value.trim();
  const params = new URLSearchParams({ regionCode });
  if (query) {
    params.set("q", query);
  }
  const requestKey = params.toString();
  const requestSeq = ++state.listingRequestSeq;

  state.listingLoading = true;
  state.listingLoaded = false;
  state.listingSummaryData = null;
  state.listingStateMessage = "로딩 중...";
  state.listingData = [];
  state.listingPage = 1;
  state.listingColFilters = {};
  state.listingLocationRequestKey = requestKey;
  state.listingLocationInFlightIds = new Set();
  state.listingLocationHydrationSeq = 0;
  updateListingZoneFilterOptions();
  renderListingTable();
  updateListingStatusNote({ buildingHubStatus: "loading" });
  try {
    const payload = await fetchApiJson(
      `http://localhost:3000/api/redevelopment-grid?${requestKey}`,
    );
    if (requestSeq !== state.listingRequestSeq) {
      return;
    }
    state.listingData = payload.items || [];
    state.listingLoaded = true;
    state.listingSummaryData = {
      ...(payload.summary || {}),
      locationInsightsStatus: detectListingLocationInsightsStatus(
        payload.summary || {},
        state.listingData,
      ),
    };
    updateListingStatusNote(state.listingSummaryData);
    state.listingStateMessage =
      state.listingData.length === 0
        ? "선택한 조건에서 표시할 단지가 없습니다"
        : "";
    updateListingZoneFilterOptions();
  } catch (error) {
    if (requestSeq !== state.listingRequestSeq) {
      return;
    }
    console.error(error);
    state.listingData = [];
    state.listingLoaded = false;
    state.listingSummaryData = {
      buildingHubStatus: /DATA_GO_KR_API_KEY/i.test(error.message)
        ? "config_error"
        : "upstream_error",
    };
    updateListingStatusNote(state.listingSummaryData);
    state.listingStateMessage = `오류: ${error.message}`;
    updateListingZoneFilterOptions();
  } finally {
    if (requestSeq !== state.listingRequestSeq) {
      return;
    }
    state.listingLoading = false;
    state.listingPage = 1;
    renderListingTable();
    updateListingStatusNote(state.listingSummaryData);
  }
}

// ── select 빌드 ──
export function buildListingSelects() {
  initRegionPair(
    "listingSidoSelect",
    "listingRegionSelect",
    DEFAULT_REGION_CODE,
  );
}

// ── 초기화 ──
// ── 엑셀 다운로드 ──
export function exportListingExcel() {
  const filtered = getFilteredListings();
  if (filtered.length === 0) return alert("다운로드할 데이터가 없습니다.");
  const headers = ["단지명", "준공년도", "건물나이", "용도지역", "법정상한(%)", "현재용적률(%)", "대지면적(㎡)", "세대수", "근처역", "평지추정", "세대당대지면적(㎡)", "예상신축세대", "일반분양분", "사업성판단"];
  const rows = filtered.map((r) => [
    r.apt,
    r.completionYear || "",
    r.buildingAge ?? "",
    r.zoning || "",
    r.legalFarLimit ?? "",
    r.currentFar ?? "",
    r.siteArea ?? "",
    r.households ?? "",
    r.nearbyStation || "",
    r.flatLandStatus === "flat" ? "평지" : r.flatLandStatus === "slope" ? "경사" : "",
    r.landPerHousehold ?? "",
    r.expectedUnits ?? "",
    r.generalSaleUnits ?? "",
    getListingFeasibilityLabel(r),
  ]);
  downloadCsv(`사업성분석_${new Date().toISOString().slice(0, 10)}.csv`, headers, rows);
}

export function initListings() {
  buildListingSelects();
}

export function activateListings() {
  if (!state.listingLoaded && !state.listingLoading) {
    loadListingData();
  }
}
