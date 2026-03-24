import { esc } from './utils.js';
import { state } from './state.js';

let openFilter = null;

// ── Callback registry to avoid circular imports ──
const _renderCallbacks = { trade: null, rent: null };

export function registerRenderCallback(key, fn) {
  _renderCallbacks[key] = fn;
}

export function toggleColFilter(key, btn, isRent) {
  closeAllFilters();
  const data = isRent ? state.rentData : state.allData;
  const filters = isRent ? state.rentColFilters : state.colFilters;
  const vals = [...new Set(data.map((d) => d[key]))].sort();
  const dd = document.createElement("div");
  dd.className = "col-filter-dd";
  const all = document.createElement("div");
  all.textContent = "전체";
  if (filters[key] == null) all.classList.add("active");
  all.onclick = () => {
    delete filters[key];
    closeAllFilters();
    if (isRent) {
      state.rentPage = 1;
      if (_renderCallbacks.rent) _renderCallbacks.rent();
    } else {
      state.currentPage = 1;
      if (_renderCallbacks.trade) _renderCallbacks.trade();
    }
  };
  dd.appendChild(all);
  vals.forEach((v) => {
    const item = document.createElement("div");
    item.textContent = v;
    if (filters[key] === v) item.classList.add("active");
    item.onclick = () => {
      filters[key] = v;
      closeAllFilters();
      if (isRent) {
        state.rentPage = 1;
        if (_renderCallbacks.rent) _renderCallbacks.rent();
      } else {
        state.currentPage = 1;
        if (_renderCallbacks.trade) _renderCallbacks.trade();
      }
    };
    dd.appendChild(item);
  });
  btn.closest("th").appendChild(dd);
  openFilter = dd;
}

export function closeAllFilters() {
  document.querySelectorAll(".col-filter-dd").forEach((d) => d.remove());
  openFilter = null;
}

export function renderThead(cols, sk, sd, filters, isRent) {
  const headId = isRent ? "rentTableHead" : "tableHead";
  const tr = document.createElement("tr");
  cols.forEach((col) => {
    const th = document.createElement("th");
    th.scope = "col";
    if (sk === col.key) th.classList.add("sort-active");
    if (filters[col.key] != null) th.classList.add("filter-active");
    th.setAttribute(
      "aria-sort",
      sk === col.key
        ? sd === "asc"
          ? "ascending"
          : "descending"
        : "none",
    );
    const inner = document.createElement("span");
    inner.className = "th-inner";
    if (col.sortable) {
      inner.classList.add("th-sortable");
      inner.onclick = () => toggleSort(col.key, isRent);
      inner.tabIndex = 0;
      inner.setAttribute("role", "button");
      inner.setAttribute("aria-label", `${col.label} 정렬`);
      inner.title = `${col.label} 정렬`;
      inner.onkeydown = (event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          toggleSort(col.key, isRent);
        }
      };
    }
    const label = document.createElement("span");
    label.textContent = col.label;
    inner.appendChild(label);
    if (col.sortable) {
      const si = document.createElement("span");
      si.className = "sort-icon";
      si.textContent = sk === col.key ? (sd === "asc" ? "▲" : "▼") : "⇅";
      inner.appendChild(si);
    }
    th.appendChild(inner);
    if (col.filterable) {
      const fb = document.createElement("button");
      fb.className = "filter-btn";
      fb.type = "button";
      fb.title = `${col.label} 필터`;
      fb.setAttribute("aria-label", `${col.label} 필터`);
      fb.textContent = filters[col.key] != null ? "●" : "▾";
      fb.onclick = (e) => {
        e.stopPropagation();
        toggleColFilter(col.key, fb, isRent);
      };
      th.appendChild(fb);
    }
    tr.appendChild(th);
  });
  const head = document.getElementById(headId);
  head.innerHTML = "";
  head.appendChild(tr);
}

export function renderPagination(elId, current, totalPages, goFn) {
  const pag = document.getElementById(elId);
  if (totalPages <= 1) {
    pag.innerHTML = "";
    return;
  }
  let html = "";
  const from = Math.max(1, current - 2);
  const to = Math.min(totalPages, current + 2);
  if (from > 1)
    html += `<button class="page-btn" data-page="1">1</button>`;
  for (let i = from; i <= to; i++) {
    html += `<button class="page-btn${i === current ? " active" : ""}" data-page="${i}">${i}</button>`;
  }
  if (to < totalPages)
    html += `<button class="page-btn" data-page="${totalPages}">${totalPages}</button>`;
  pag.innerHTML = html;
  pag.querySelectorAll(".page-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      goFn(parseInt(btn.dataset.page));
      window.scrollTo({ top: 0, behavior: "smooth" });
    });
  });
}

export function getSorted(arr, sk, sd) {
  if (!sk) return arr;
  return [...arr].sort((a, b) => {
    let va = a[sk],
      vb = b[sk];
    if (typeof va === "string") {
      va = va.toLowerCase();
      vb = (vb || "").toLowerCase();
    }
    if (va < vb) return sd === "asc" ? -1 : 1;
    if (va > vb) return sd === "asc" ? 1 : -1;
    return 0;
  });
}

export function toggleSort(key, isRent) {
  if (isRent) {
    if (state.rentSortKey === key)
      state.rentSortDir = state.rentSortDir === "asc" ? "desc" : "asc";
    else {
      state.rentSortKey = key;
      state.rentSortDir = "asc";
    }
    state.rentPage = 1;
    if (_renderCallbacks.rent) _renderCallbacks.rent();
  } else {
    if (state.sortKey === key)
      state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
    else {
      state.sortKey = key;
      state.sortDir = "asc";
    }
    state.currentPage = 1;
    if (_renderCallbacks.trade) _renderCallbacks.trade();
  }
}
