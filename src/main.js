// ══════════════════════════════════════════════════════════
// main.js — Entry point
// ══════════════════════════════════════════════════════════

// Styles
import './styles/base.css';
import './styles/components.css';
import './styles/rent.css';
import './styles/trend.css';
import './styles/calc.css';
import './styles/listings.css';
import './styles/brokers.css';

// Shared modules
import { getRecentYearMonths } from './utils.js';
import { state } from './state.js';
import {
  DEFAULT_REGION_CODE,
  loadRegionCatalog,
  initRegionPair,
} from './regions.js';

// Tab modules
import {
  buildSelects,
  setCond,
  setPropertyType,
  loadData,
  renderAll,
  renderTable,
  showBuildingModal,
  closeBuildingModal,
  initTrade,
  exportTradeExcel,
} from './tabs/trade.js';

import {
  buildRentSelects,
  setRentPropertyType,
  setRentType,
  loadRentData,
  renderRentTable,
  initRent,
} from './tabs/rent.js';

import {
  buildTrendSelects,
  initTrendAutocomplete,
  loadTrendData,
  drawChart,
  initTrend,
} from './tabs/trend.js';

import {
  buildListingSelects,
  loadListingData,
  initListings,
  exportListingExcel,
} from './tabs/listings.js';

import { calcLoan, calcInvest, calcCashflow, initCalc } from './tabs/calc.js';

import {
  buildBrokerSelects,
  loadBrokerData,
  renderBrokerTable,
  setBrokerSort,
  setBrokerPage,
  initBrokers,
  exportBrokerExcel,
} from './tabs/brokers.js';

// ── Tab Switching ──
function switchTab(tab) {
  document
    .querySelectorAll('.tab')
    .forEach((t) => t.classList.toggle('active', t.dataset.tab === tab));
  document
    .querySelectorAll('.tab-content')
    .forEach((c) => c.classList.remove('active'));
  const map = {
    trade: 'tabTrade',
    rent: 'tabRent',
    trend: 'tabTrend',
    calc: 'tabCalc',
    listings: 'tabListings',
    brokers: 'tabBrokers',
  };
  document.getElementById(map[tab]).classList.add('active');
  document.getElementById('sidebar').style.display =
    tab === 'trade' || tab === 'rent' ? '' : 'none';
  if (tab === 'trend') requestAnimationFrame(drawChart);
  if (tab === 'listings' && !state.listingLoaded && !state.listingLoading) {
    loadListingData();
  }
}

// ── Modal helpers ──
function openModal() {
  document.getElementById('modalOverlay').classList.add('open');
}
function closeModal() {
  document.getElementById('modalOverlay').classList.remove('open');
}

// ── API Status ──
function updateApiStatus(ok) {
  document.getElementById('statusDot').textContent = ok ? '🟢' : '🟠';
  document.getElementById('statusText').textContent = ok
    ? 'API 연결됨'
    : '데모 모드';
}

// ── Init ──
async function init() {
  await loadRegionCatalog();
  buildSelects();
  buildRentSelects();
  buildTrendSelects();
  buildListingSelects();
  buildBrokerSelects();
  initBrokers();
  initTrendAutocomplete();

  let isConnected = false;
  try {
    const res = await fetch('/api/config');
    if (res.ok) {
      const config = await res.json();
      const { connected } = config;
      state.localDatasetStatus = {
        stationsAvailable: !!config?.datasets?.stations?.available,
        apartmentCoordsAvailable:
          !!config?.datasets?.apartmentCoords?.available,
      };
      const apiAvailable = !!connected || !!config?.integrations?.dataGoKr;
      isConnected = apiAvailable;
      updateApiStatus(apiAvailable);
    } else {
      updateApiStatus(false);
    }
  } catch {
    updateApiStatus(false);
  }

  if (isConnected) {
    await loadData();
    await loadRentData();
  } else {
    state.allData = [];
    state.rentData = [];
    renderAll();
  }
}

// ── Expose globals for inline onclick handlers in HTML ──
window.switchTab = switchTab;
window.openModal = openModal;
window.closeModal = closeModal;
window.setCond = setCond;
window.setPropertyType = setPropertyType;
window.setRentPropertyType = setRentPropertyType;
window.setRentType = setRentType;
window.loadData = loadData;
window.loadRentData = loadRentData;
window.loadTrendData = loadTrendData;
window.loadListingData = loadListingData;
window.showBuildingModal = showBuildingModal;
window.closeBuildingModal = closeBuildingModal;
window.calcLoan = calcLoan;
window.calcInvest = calcInvest;
window.calcCashflow = calcCashflow;
window.loadBrokerData = loadBrokerData;
window.renderBrokerTable = renderBrokerTable;
window.setBrokerSort = setBrokerSort;
window.setBrokerPage = setBrokerPage;
window.exportTradeExcel = exportTradeExcel;
window.exportListingExcel = exportListingExcel;
window.exportBrokerExcel = exportBrokerExcel;

// ── Building modal outside click ──
document.getElementById('buildingModal')?.addEventListener('click', function (e) {
  if (e.target === this) closeBuildingModal();
});

// ── Start ──
init();
