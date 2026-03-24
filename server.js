const http = require("http");
const https = require("https");
const fs = require("fs");
const path = require("path");

const { Client } = require("@modelcontextprotocol/sdk/client/index.js");
const {
  StdioClientTransport,
} = require("@modelcontextprotocol/sdk/client/stdio.js");

const DEFAULT_PORT = 3000;
const SOURCE = "real-estate-mcp";
const USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36";
const MCP_SERVER_CWD = path.join(__dirname, "_ref_real-estate-mcp");
const REGION_CODE_FILE_PRIMARY = path.join(
  MCP_SERVER_CWD,
  "src",
  "real_estate",
  "resources",
  "region_codes.txt",
);
const REGION_CODE_FILE_FALLBACK = path.join(__dirname, "data", "region_codes.txt");
const REGION_CODE_FILE = fs.existsSync(REGION_CODE_FILE_PRIMARY)
  ? REGION_CODE_FILE_PRIMARY
  : REGION_CODE_FILE_FALLBACK;
const LOCAL_DATA_DIR = path.join(__dirname, "data");
const GENERATED_DATA_DIR = path.join(LOCAL_DATA_DIR, "generated");
const STATION_DATA_FILE = path.join(GENERATED_DATA_DIR, "subway-stations.json");
const APARTMENT_COORD_DATA_FILE = path.join(
  GENERATED_DATA_DIR,
  "apartment-coordinate-index.json",
);

function loadEnvFile() {
  const envPath = path.join(__dirname, ".env");
  try {
    const envText = fs.readFileSync(envPath, "utf8");
    return envText.split(/\r?\n/).reduce((acc, rawLine) => {
      const line = rawLine.trim();
      if (!line || line.startsWith("#")) {
        return acc;
      }

      const separatorIndex = line.indexOf("=");
      if (separatorIndex === -1) {
        return acc;
      }

      const key = line.slice(0, separatorIndex).trim();
      const value = line.slice(separatorIndex + 1).trim();
      acc[key] = value.replace(/^['"]|['"]$/g, "");
      return acc;
    }, {});
  } catch {
    return {};
  }
}

const ENV_FILE = loadEnvFile();
const API_KEY =
  ENV_FILE.DATA_GO_KR_API_KEY ||
  ENV_FILE.MOLIT_API_KEY ||
  process.env.DATA_GO_KR_API_KEY ||
  process.env.MOLIT_API_KEY ||
  "";
const LAW_API_OC = ENV_FILE.LAW_API_OC || process.env.LAW_API_OC || "";
const VWORLD_API_KEY = ENV_FILE.VWORLD_API_KEY || process.env.VWORLD_API_KEY || "";
const VWORLD_DATA_API_KEY =
  ENV_FILE.VWORLD_DATA_API_KEY ||
  ENV_FILE.VWORLD_API_KEY ||
  process.env.VWORLD_DATA_API_KEY ||
  process.env.VWORLD_API_KEY ||
  "";
const VWORLD_LANDUSE_API_KEY =
  ENV_FILE.VWORLD_API_KEY_getLandUse ||
  process.env.VWORLD_API_KEY_getLandUse ||
  "";
const VWORLD_DATA_DOMAIN =
  ENV_FILE.VWORLD_DATA_DOMAIN || process.env.VWORLD_DATA_DOMAIN || "";

// ── 서울 열린데이터 광장 API 키 ──
const SEOUL_BUILDING_API_KEY =
  ENV_FILE.DATA_SEOUL_getLandUse ||
  process.env.DATA_SEOUL_getLandUse ||
  "";
const SEOUL_REALESTATE_API_KEY =
  ENV_FILE.DATA_SEOUL_realestate ||
  process.env.DATA_SEOUL_realestate ||
  "";
const PORT =
  Number.parseInt(process.env.PORT || ENV_FILE.PORT || `${DEFAULT_PORT}`, 10) ||
  DEFAULT_PORT;

const API_URLS = {
  aptTrade:
    "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade",
  aptRent:
    "https://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent",
  aptListBySigungu:
    "https://apis.data.go.kr/1613000/AptListService3/getSigunguAptList3",
  aptBasicInfo:
    "https://apis.data.go.kr/1613000/AptBasisInfoServiceV4/getAphusBassInfoV4",
  aptDetailInfo:
    "https://apis.data.go.kr/1613000/AptBasisInfoServiceV4/getAphusDtlInfoV4",
  offiTrade:
    "https://apis.data.go.kr/1613000/RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade",
  offiRent:
    "https://apis.data.go.kr/1613000/RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent",
  villaTrade:
    "https://apis.data.go.kr/1613000/RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade",
  villaRent:
    "https://apis.data.go.kr/1613000/RTMSDataSvcRHRent/getRTMSDataSvcRHRent",
  houseTrade:
    "https://apis.data.go.kr/1613000/RTMSDataSvcSHTrade/getRTMSDataSvcSHTrade",
  houseRent:
    "https://apis.data.go.kr/1613000/RTMSDataSvcSHRent/getRTMSDataSvcSHRent",
  commTrade:
    "https://apis.data.go.kr/1613000/RTMSDataSvcNrgTrade/getRTMSDataSvcNrgTrade",
  buildingBasisOutline:
    "http://apis.data.go.kr/1613000/BldRgstHubService/getBrBasisOulnInfo",
  buildingTitle:
    "http://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo",
  buildingRecap:
    "http://apis.data.go.kr/1613000/BldRgstHubService/getBrRecapTitleInfo",
  buildingJijigu:
    "http://apis.data.go.kr/1613000/BldRgstHubService/getBrJijiguInfo",
  landUseRegulation:
    "http://apis.data.go.kr/1613000/arLandUseInfoService/DTarLandUseInfo",
};
API_URLS.building = API_URLS.buildingTitle;

const MCP_ROUTES = {
  "/api/apt-trade": {
    tool: "get_apartment_trades",
    kind: "trade",
    rawKind: "apt",
    rawUrl: API_URLS.aptTrade,
  },
  "/api/apt-rent": { tool: "get_apartment_rent", kind: "rent", rawKind: "apt", rawUrl: API_URLS.aptRent },
  "/api/offi-trade": {
    tool: "get_officetel_trades",
    kind: "trade",
    rawKind: "offi",
    rawUrl: API_URLS.offiTrade,
  },
  "/api/offi-rent": {
    tool: "get_officetel_rent",
    kind: "rent",
    rawKind: "offi",
    rawUrl: API_URLS.offiRent,
  },
  "/api/villa-trade": {
    tool: "get_villa_trades",
    kind: "trade",
    rawKind: "villa",
    rawUrl: API_URLS.villaTrade,
  },
  "/api/villa-rent": { tool: "get_villa_rent", kind: "rent", rawKind: "villa", rawUrl: API_URLS.villaRent },
  "/api/house-trade": {
    tool: "get_single_house_trades",
    kind: "trade",
    rawKind: "house",
    rawUrl: API_URLS.houseTrade,
  },
  "/api/house-rent": {
    tool: "get_single_house_rent",
    kind: "rent",
    rawKind: "house",
    rawUrl: API_URLS.houseRent,
  },
  "/api/comm-trade": {
    tool: "get_commercial_trade",
    kind: "trade",
    rawKind: "comm",
    rawUrl: API_URLS.commTrade,
  },
  "/api/molit": {
    tool: "get_apartment_trades",
    kind: "trade",
    rawKind: "apt",
    rawUrl: API_URLS.aptTrade,
  },
  "/api/trade": {
    tool: "get_apartment_trades",
    kind: "trade",
    rawKind: "apt",
    rawUrl: API_URLS.aptTrade,
  },
};

const mcpState = {
  connected: false,
  connecting: null,
  client: null,
  transport: null,
  toolNames: new Set(),
  lastError: null,
};

const LISTING_LOOKBACK_MONTHS = 6;
const LISTING_PAGE_SIZE = 1000;
const LISTING_MAX_PAGES = 2;
const LISTING_MAX_COMPLEXES = 50;
const LISTING_MAX_SEEDS = 80;
const LISTING_ENRICH_CONCURRENCY = 15;
const TRADE_ENRICH_CONCURRENCY = 8;
const LISTING_CACHE_TTL_MS = 60 * 60 * 1000;
const LISTING_SEED_CACHE_TTL_MS = 60 * 60 * 1000;
const LISTING_LOCATION_INSIGHT_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const BUILDING_SNAPSHOT_CACHE_TTL_MS = 2 * 60 * 60 * 1000;
const KAPT_DIRECTORY_CACHE_TTL_MS = 12 * 60 * 60 * 1000;
const KAPT_DETAIL_CACHE_TTL_MS = 12 * 60 * 60 * 1000;
const VWORLD_ADDRESS_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const VWORLD_ZONING_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const VWORLD_PLACE_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const ELEVATION_PROFILE_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const VWORLD_URBAN_ZONE_LAYER = "LT_C_UQ111";
const REBUILD_UNIT_AREA_SQM = 40;
const FEASIBILITY_RATIO_THRESHOLD = 0.1;
const REBUILD_MIN_AGE_YEARS = 30;
// 비주거 용도지역의 주거 허용 비율 (재건축 시 전체 용적률 중 주거로 사용 가능한 비율)
// 주거지역은 1.0(100%), 비주거는 서울시 정비사업 기준 보수적 추정
const LISTING_RESIDENTIAL_FAR_RATIO = {
  준공업지역: 0.5,
  일반공업지역: 0.4,
  근린상업지역: 0.6,
  일반상업지역: 0.5,
  유통상업지역: 0.4,
  중심상업지역: 0.3,
};
const WALKING_DISTANCE_KM = 0.8;
const LOCATION_FLATNESS_SAMPLE_DISTANCE_M = 120;
const LOCATION_FLATNESS_MAX_RANGE_M = 8;
const VWORLD_NEARBY_PLACE_SIZE = 15;
const LISTING_REASONABLE_LAND_PER_HOUSEHOLD_MIN_SQM = 5;
const LISTING_REASONABLE_LAND_PER_HOUSEHOLD_MAX_SQM = 220;
const LISTING_EXTREME_LAND_PER_HOUSEHOLD_MIN_SQM = 3;
const LISTING_EXTREME_LAND_PER_HOUSEHOLD_MAX_SQM = 320;
const LISTING_GROSS_AREA_CORRECTION_MIN_FAR = 150;
const LISTING_GROSS_AREA_CORRECTION_MIN_LAND_PER_HOUSEHOLD_SQM = 70;
const LISTING_GROSS_AREA_CORRECTION_MAX_LAND_PER_HOUSEHOLD_SQM = 140;
const LISTING_GROSS_AREA_UNVERIFIED_LAND_PER_HOUSEHOLD_SQM = 120;
const BUILDING_HUB_EMPTY_THRESHOLD = 3;
const BUILDING_HUB_DISABLED_TTL_MS = 5 * 60 * 1000;
const LISTING_LEGAL_FAR_LIMITS = {
  default: {
    "제1종전용주거지역": 100,
    "제2종전용주거지역": 150,
    "제1종일반주거지역": 200,
    "제2종일반주거지역": 250,
    "제3종일반주거지역": 300,
    준주거지역: 500,
    근린상업지역: 700,
    일반상업지역: 800,
    유통상업지역: 1100,
    중심상업지역: 1300,
    준공업지역: 400,
    일반공업지역: 350,
  },
  seoul: {},
  gyeonggi: {},
};

const listingCache = new Map();
const listingSeedCache = new Map();
const listingLocationInsightCache = new Map();
const buildingSnapshotCache = new Map();
const apartmentDirectoryCache = new Map();
const apartmentDetailCache = new Map();
const vworldAddressCache = new Map();
const vworldZoningCache = new Map();

// ── 파일 기반 영구 캐시 (VWorld 용도지역) ──
const PERSISTENT_CACHE_DIR = path.join(__dirname, "data", "cache");
const ZONING_CACHE_FILE = path.join(PERSISTENT_CACHE_DIR, "vworld-zoning.json");
const BUILDING_CACHE_FILE = path.join(PERSISTENT_CACHE_DIR, "building-snapshot.json");

function loadPersistentZoningCache() {
  try {
    if (!fs.existsSync(ZONING_CACHE_FILE)) return;
    const raw = fs.readFileSync(ZONING_CACHE_FILE, "utf-8");
    const entries = JSON.parse(raw);
    if (!Array.isArray(entries)) return;
    const now = Date.now();
    let loaded = 0;
    for (const [key, entry] of entries) {
      // 7일 이내 데이터만 로드 (용도지역은 자주 바뀌지 않음)
      if (entry?.ts && now - entry.ts < 7 * 24 * 60 * 60 * 1000) {
        vworldZoningCache.set(key, entry);
        loaded++;
      }
    }
    if (loaded > 0) console.log(`[persistent-cache] loaded ${loaded} zoning entries from disk`);
  } catch { /* silent */ }
}

function savePersistentZoningCache() {
  try {
    if (!fs.existsSync(PERSISTENT_CACHE_DIR)) {
      fs.mkdirSync(PERSISTENT_CACHE_DIR, { recursive: true });
    }
    const entries = [...vworldZoningCache.entries()].filter(
      ([, entry]) => entry?.value?.status === "ok" && entry?.value?.zoning
    );
    fs.writeFileSync(ZONING_CACHE_FILE, JSON.stringify(entries, null, 0), "utf-8");
  } catch { /* silent */ }
}

// 디바운스된 영구 캐시 저장 (5초 간격)
let _zoningSaveTimer = null;
function schedulePersistentZoningSave() {
  if (_zoningSaveTimer) return;
  _zoningSaveTimer = setTimeout(() => {
    _zoningSaveTimer = null;
    savePersistentZoningCache();
  }, 5000);
}

// ── 파일 기반 영구 캐시 (건축물대장 스냅샷) ──
function loadPersistentBuildingCache() {
  try {
    if (!fs.existsSync(BUILDING_CACHE_FILE)) return;
    const raw = fs.readFileSync(BUILDING_CACHE_FILE, "utf-8");
    const entries = JSON.parse(raw);
    if (!Array.isArray(entries)) return;
    const now = Date.now();
    let loaded = 0;
    for (const [key, entry] of entries) {
      // 3일 이내 데이터만 로드
      if (entry?.ts && now - entry.ts < 3 * 24 * 60 * 60 * 1000) {
        buildingSnapshotCache.set(key, entry);
        loaded++;
      }
    }
    if (loaded > 0) console.log(`[persistent-cache] loaded ${loaded} building snapshot entries from disk`);
  } catch { /* silent */ }
}

function savePersistentBuildingCache() {
  try {
    if (!fs.existsSync(PERSISTENT_CACHE_DIR)) {
      fs.mkdirSync(PERSISTENT_CACHE_DIR, { recursive: true });
    }
    const entries = [...buildingSnapshotCache.entries()].filter(
      ([, entry]) => entry?.value?.sourceReady === true
    );
    fs.writeFileSync(BUILDING_CACHE_FILE, JSON.stringify(entries, null, 0), "utf-8");
  } catch { /* silent */ }
}

let _buildingSaveTimer = null;
function schedulePersistentBuildingSave() {
  if (_buildingSaveTimer) return;
  _buildingSaveTimer = setTimeout(() => {
    _buildingSaveTimer = null;
    savePersistentBuildingCache();
  }, 5000);
}

// 서버 시작 시 로드
loadPersistentZoningCache();
loadPersistentBuildingCache();
const vworldPlaceCache = new Map();
const elevationProfileCache = new Map();
const buildingHubState = {
  hasWorkingResponse: false,
  emptyRuns: 0,
  disabledUntil: 0,
  lastProbeAt: null,
  lastStatusCode: null,
  lastBodyEmpty: null,
  lastSampleParams: null,
  lastMessage: null,
  lastProbeStatus: null,
  lastEndpointResults: [],
};
const localDataState = {
  stations: {
    loadedAt: null,
    checkedAtMs: 0,
    mtimeMs: null,
    sourceFile: STATION_DATA_FILE,
    message: "subway station dataset not loaded",
    items: [],
  },
  apartmentCoords: {
    loadedAt: null,
    checkedAtMs: 0,
    mtimeMs: null,
    sourceFile: APARTMENT_COORD_DATA_FILE,
    message: "apartment coordinate dataset not loaded",
    items: [],
    byParcel: new Map(),
    byNameDong: new Map(),
  },
};

const apiCache = new Map();
const API_CACHE_TTL_MS = 5 * 60 * 1000;
const MAX_CACHE_ENTRIES = 500;

function getCached(key) {
  const entry = apiCache.get(key);
  if (!entry) return null;
  if (Date.now() - entry.ts > API_CACHE_TTL_MS) {
    apiCache.delete(key);
    return null;
  }
  return entry.data;
}

function setCache(key, data) {
  if (apiCache.size >= MAX_CACHE_ENTRIES) {
    const oldest = apiCache.keys().next().value;
    apiCache.delete(oldest);
  }
  apiCache.set(key, { data, ts: Date.now() });
}

// --- Rate Limiting (IP 기반, 분당 120 요청) ---
const rateLimitMap = new Map();
const RATE_LIMIT_WINDOW_MS = 60 * 1000;
const RATE_LIMIT_MAX = 120;

function checkRateLimit(ip) {
  const now = Date.now();
  let entry = rateLimitMap.get(ip);
  if (!entry || now - entry.windowStart > RATE_LIMIT_WINDOW_MS) {
    entry = { windowStart: now, count: 0 };
    rateLimitMap.set(ip, entry);
  }
  entry.count++;
  if (entry.count > RATE_LIMIT_MAX) return false;
  return true;
}

// 오래된 rate limit 엔트리 정리 (5분마다)
setInterval(() => {
  const cutoff = Date.now() - RATE_LIMIT_WINDOW_MS * 2;
  for (const [ip, entry] of rateLimitMap) {
    if (entry.windowStart < cutoff) rateLimitMap.delete(ip);
  }
}, 5 * 60 * 1000);

// --- 입력 검증 ---
const PNU_REGEX = /^\d{19}$/;
const YEAR_REGEX = /^\d{4}$/;
const REGION_CODE_REGEX = /^\d{5}$/;

const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

function writeCorsHeaders(res, req) {
  const origin = (req && req.headers && req.headers.origin) || "";
  if (ALLOWED_ORIGINS.length > 0) {
    // 허용 목록에 있는 origin만 허용
    if (ALLOWED_ORIGINS.includes(origin)) {
      res.setHeader("Access-Control-Allow-Origin", origin);
    }
  } else {
    // 개발 환경: 모든 origin 허용
    res.setHeader("Access-Control-Allow-Origin", "*");
  }
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
}

function sendJson(res, statusCode, payload) {
  writeCorsHeaders(res);
  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
  });
  res.end(JSON.stringify(payload));
}

function sendText(res, statusCode, text) {
  writeCorsHeaders(res);
  res.writeHead(statusCode, {
    "Content-Type": "text/plain; charset=utf-8",
  });
  res.end(text);
}

function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(String(value || ""), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function normalizeText(value) {
  return String(value || "")
    .trim()
    .replace(/\s+/g, "")
    .toLowerCase();
}

function normalizeComplexNameKey(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/\([^)]*\)/g, "")
    .replace(/[^0-9a-z\u3131-\u318e\uac00-\ud7a3]/g, "");
}

function normalizeComplexMatchKey(value) {
  return normalizeComplexNameKey(value).replace(/(?:아파트|apt)$/g, "");
}

function extractComplexNumberTokens(value) {
  return normalizeComplexNameKey(value).match(/\d+/g) || [];
}

function scoreComplexNameSimilarity(targetName, candidateName) {
  const targetKey = normalizeComplexMatchKey(targetName);
  const candidateKey = normalizeComplexMatchKey(candidateName);
  if (!targetKey || !candidateKey) {
    return 0;
  }

  if (targetKey === candidateKey) {
    return 120;
  }
  if (candidateKey.includes(targetKey) || targetKey.includes(candidateKey)) {
    return 95;
  }

  const targetBase = targetKey.replace(/\d+/g, "");
  const candidateBase = candidateKey.replace(/\d+/g, "");
  if (!targetBase || !candidateBase) {
    return 0;
  }

  const targetNumbers = extractComplexNumberTokens(targetKey);
  const candidateNumbers = extractComplexNumberTokens(candidateKey);
  const numberCompatible =
    targetNumbers.length === 0 ||
    candidateNumbers.length === 0 ||
    targetNumbers.some((token) => candidateNumbers.includes(token));

  if (
    numberCompatible &&
    (candidateBase.includes(targetBase) || targetBase.includes(candidateBase))
  ) {
    return targetNumbers.length > 0 && candidateNumbers.length === 0 ? 70 : 80;
  }

  return 0;
}

function filterItemsByComplexName(items, aptName, options = {}) {
  const { allowRawFallback = true } = options;
  const sourceItems = toArray(items).filter(Boolean);
  if (!sourceItems.length || !aptName) {
    return sourceItems;
  }

  const scored = sourceItems
    .map((item) => ({
      item,
      score: scoreComplexNameSimilarity(
        aptName,
        pickFirstValue(item, ["bldNm", "platPlc", "newPlatPlc"]) || "",
      ),
    }))
    .filter((entry) => entry.score > 0);

  if (!scored.length) {
    return allowRawFallback ? sourceItems : [];
  }

  const bestScore = Math.max(...scored.map((entry) => entry.score));
  return scored
    .filter((entry) => entry.score === bestScore)
    .map((entry) => entry.item);
}

function hasMeaningfulBuildingMetric(value) {
  if (typeof value === "number") {
    return Number.isFinite(value) && value > 0;
  }
  return Boolean(String(value || "").trim());
}

function preferMeaningfulMetric(primaryValue, fallbackValue) {
  return hasMeaningfulBuildingMetric(primaryValue) ? primaryValue : fallbackValue;
}

function compareBuildingSnapshotCompleteness(candidate, current) {
  const orderedMetrics = ["siteArea", "currentFar", "households", "zoning"];
  for (const metric of orderedMetrics) {
    const candidateHas = hasMeaningfulBuildingMetric(candidate?.[metric]);
    const currentHas = hasMeaningfulBuildingMetric(current?.[metric]);
    if (candidateHas !== currentHas) {
      return candidateHas ? 1 : -1;
    }
  }
  return 0;
}

function normalizeSnapshotMetricNumber(value, digits = 2) {
  const numeric = Number.parseFloat(String(value ?? "").trim());
  return Number.isFinite(numeric) && numeric > 0 ? roundNullable(numeric, digits) : null;
}

function normalizeSnapshotMetricInteger(value) {
  const numeric = Number.parseInt(String(value ?? "").trim(), 10);
  return Number.isFinite(numeric) && numeric > 0 ? numeric : null;
}

function computeListingLandPerHousehold(siteArea, households) {
  const normalizedSiteArea = normalizeSnapshotMetricNumber(siteArea, 2);
  const normalizedHouseholds = normalizeSnapshotMetricInteger(households);
  if (normalizedSiteArea == null || normalizedHouseholds == null) {
    return null;
  }
  return roundNullable(normalizedSiteArea / normalizedHouseholds, 2);
}

function isApproximatelyEqualNumber(left, right, toleranceRatio = 0.08) {
  if (!Number.isFinite(left) || !Number.isFinite(right)) {
    return false;
  }
  const denominator = Math.max(Math.abs(left), Math.abs(right), 1);
  return Math.abs(left - right) / denominator <= toleranceRatio;
}

function isReasonableListingLandPerHousehold(value) {
  return (
    value != null &&
    value >= LISTING_REASONABLE_LAND_PER_HOUSEHOLD_MIN_SQM &&
    value <= LISTING_REASONABLE_LAND_PER_HOUSEHOLD_MAX_SQM
  );
}

function isExtremeListingLandPerHousehold(value) {
  return (
    value != null &&
    (value < LISTING_EXTREME_LAND_PER_HOUSEHOLD_MIN_SQM ||
      value > LISTING_EXTREME_LAND_PER_HOUSEHOLD_MAX_SQM)
  );
}

function isSuspiciousListingSiteAreaSource(snapshot) {
  return (
    snapshot?.parcelSource === "vworld_pnu" ||
    ["kapt", "kapt_apportioned", "vworld_pnu_building_hub"].includes(
      String(snapshot?.siteAreaSource || ""),
    ) ||
    String(snapshot?.siteAreaFieldKey || "") === "siteArea"
  );
}

function resolveListingSiteArea(snapshot) {
  const rawSiteArea = normalizeSnapshotMetricNumber(snapshot?.siteArea, 2);
  if (rawSiteArea == null) {
    return {
      siteArea: null,
      rawSiteArea: null,
      siteAreaCorrection: null,
      siteAreaValidationStatus: "missing",
    };
  }

  const currentFar = normalizeSnapshotMetricNumber(snapshot?.currentFar, 2);
  const households = normalizeSnapshotMetricInteger(snapshot?.households);
  const rawLandPerHousehold = computeListingLandPerHousehold(rawSiteArea, households);
  const grossFloorArea = normalizeSnapshotMetricNumber(snapshot?.grossFloorArea, 2);
  const adjustedSiteArea =
    currentFar != null && currentFar > 0
      ? roundNullable(rawSiteArea / (currentFar / 100), 2)
      : null;
  const adjustedLandPerHousehold = computeListingLandPerHousehold(
    adjustedSiteArea,
    households,
  );
  const directGrossAreaMatch =
    grossFloorArea != null && isApproximatelyEqualNumber(rawSiteArea, grossFloorArea);
  const suspiciousSource = isSuspiciousListingSiteAreaSource(snapshot);
  const canCorrectFromGrossArea =
    adjustedSiteArea != null &&
    currentFar != null &&
    currentFar >= LISTING_GROSS_AREA_CORRECTION_MIN_FAR &&
    adjustedLandPerHousehold != null &&
    adjustedLandPerHousehold >= LISTING_REASONABLE_LAND_PER_HOUSEHOLD_MIN_SQM &&
    adjustedLandPerHousehold <= LISTING_GROSS_AREA_CORRECTION_MAX_LAND_PER_HOUSEHOLD_SQM &&
    (directGrossAreaMatch ||
      ((suspiciousSource || String(snapshot?.grossFloorAreaSource || "").trim()) &&
        rawLandPerHousehold != null &&
        rawLandPerHousehold >=
          LISTING_GROSS_AREA_CORRECTION_MIN_LAND_PER_HOUSEHOLD_SQM));

  if (canCorrectFromGrossArea) {
    return {
      siteArea: adjustedSiteArea,
      rawSiteArea,
      siteAreaCorrection: "gross_floor_area_divided_by_far",
      siteAreaValidationStatus: "corrected",
    };
  }

  if (
    rawLandPerHousehold != null &&
    rawLandPerHousehold >= LISTING_GROSS_AREA_UNVERIFIED_LAND_PER_HOUSEHOLD_SQM &&
    (directGrossAreaMatch || suspiciousSource)
  ) {
    return {
      siteArea: null,
      rawSiteArea,
      siteAreaCorrection: null,
      siteAreaValidationStatus: "unverified",
    };
  }

  return {
    siteArea: rawSiteArea,
    rawSiteArea,
    siteAreaCorrection: null,
    siteAreaValidationStatus: "ok",
  };
}

function getSnapshotMetricSourceKey(metricKey) {
  return `${metricKey}Source`;
}

function mergeSnapshotMetric(baseSnapshot, supplementSnapshot, metricKey, metadataSource = null) {
  const sourceKey = getSnapshotMetricSourceKey(metricKey);
  const baseValue = baseSnapshot?.[metricKey];
  if (hasMeaningfulBuildingMetric(baseValue)) {
    return {
      value: baseValue,
      source: baseSnapshot?.[sourceKey] || null,
    };
  }

  const supplementValue = supplementSnapshot?.[metricKey];
  if (hasMeaningfulBuildingMetric(supplementValue)) {
    return {
      value: supplementValue,
      source: metadataSource || supplementSnapshot?.[sourceKey] || null,
    };
  }

  return {
    value: supplementValue ?? baseValue ?? null,
    source:
      baseSnapshot?.[sourceKey] ||
      supplementSnapshot?.[sourceKey] ||
      metadataSource ||
      null,
  };
}

function scoreZoningSpecificity(zoneName, regionCode = "") {
  const normalized = normalizeZoneName(zoneName);
  if (!normalized) {
    return 0;
  }

  let score = 1;
  if (!isGenericZoneName(normalized)) {
    score += 2;
  }
  if (resolveLegalFarLimit(regionCode, normalized) != null) {
    score += 4;
  }
  return score + Math.min(normalized.length, 80) / 100;
}

function choosePreferredZoningMetric(
  baseSnapshot,
  supplementSnapshot,
  metadata = {},
) {
  const regionCode = metadata.regionCode || "";
  const baseValue = String(baseSnapshot?.zoning || "").trim();
  const supplementValue = String(supplementSnapshot?.zoning || "").trim();

  if (!baseValue && !supplementValue) {
    return {
      value: null,
      source:
        baseSnapshot?.zoningSource ||
        supplementSnapshot?.zoningSource ||
        metadata.zoningSource ||
        null,
      status:
        baseSnapshot?.zoningStatus ||
        supplementSnapshot?.zoningStatus ||
        metadata.zoningStatus ||
        null,
    };
  }

  if (!baseValue) {
    return {
      value: supplementValue || null,
      source: supplementSnapshot?.zoningSource || metadata.zoningSource || null,
      status: supplementSnapshot?.zoningStatus || metadata.zoningStatus || null,
    };
  }

  if (!supplementValue) {
    return {
      value: baseValue || null,
      source: baseSnapshot?.zoningSource || metadata.zoningSource || null,
      status: baseSnapshot?.zoningStatus || metadata.zoningStatus || null,
    };
  }

  const baseScore = scoreZoningSpecificity(baseValue, regionCode);
  const supplementScore = scoreZoningSpecificity(supplementValue, regionCode);
  if (supplementScore > baseScore) {
    return {
      value: supplementValue,
      source: supplementSnapshot?.zoningSource || metadata.zoningSource || null,
      status: supplementSnapshot?.zoningStatus || metadata.zoningStatus || null,
    };
  }
  if (baseScore > supplementScore) {
    return {
      value: baseValue,
      source:
        baseSnapshot?.zoningSource ||
        supplementSnapshot?.zoningSource ||
        metadata.zoningSource ||
        null,
      status:
        baseSnapshot?.zoningStatus ||
        supplementSnapshot?.zoningStatus ||
        metadata.zoningStatus ||
        null,
    };
  }

  const normalizedBase = normalizeZoneName(baseValue);
  const normalizedSupplement = normalizeZoneName(supplementValue);
  if (
    normalizedSupplement.includes(normalizedBase) &&
    normalizedSupplement.length > normalizedBase.length
  ) {
    return {
      value: supplementValue,
      source: supplementSnapshot?.zoningSource || metadata.zoningSource || null,
      status: supplementSnapshot?.zoningStatus || metadata.zoningStatus || null,
    };
  }

  return {
    value: baseValue,
    source:
      baseSnapshot?.zoningSource ||
      supplementSnapshot?.zoningSource ||
      metadata.zoningSource ||
      null,
    status:
      baseSnapshot?.zoningStatus ||
      supplementSnapshot?.zoningStatus ||
      metadata.zoningStatus ||
      null,
  };
}

function scoreListingSnapshotReliability(snapshot) {
  let score = 0;

  if (hasMeaningfulBuildingMetric(snapshot?.siteArea)) score += 24;
  if (hasMeaningfulBuildingMetric(snapshot?.households)) score += 24;
  if (hasMeaningfulBuildingMetric(snapshot?.currentFar)) score += 16;
  if (snapshot?.zoning) score += 10;

  const landPerHousehold = computeListingLandPerHousehold(
    snapshot?.siteArea,
    snapshot?.households,
  );
  if (landPerHousehold != null) {
    if (isReasonableListingLandPerHousehold(landPerHousehold)) {
      score += 28;
    } else if (landPerHousehold >= 4 && landPerHousehold <= 260) {
      score += 8;
    } else if (isExtremeListingLandPerHousehold(landPerHousehold)) {
      score -= 45;
    } else {
      score -= 15;
    }
  }

  const siteAreaSource = String(snapshot?.siteAreaSource || "");
  const householdsSource = String(snapshot?.householdsSource || "");
  if (siteAreaSource && householdsSource && siteAreaSource === householdsSource) {
    score += 8;
  }

  if (snapshot?.kaptStatus === "ok") {
    score += 4;
  }

  if (
    snapshot?.parcelSource === "vworld_pnu" &&
    landPerHousehold != null &&
    !isReasonableListingLandPerHousehold(landPerHousehold)
  ) {
    score -= 18;
  }

  return score;
}

function compareListingSnapshotReliability(candidate, current) {
  const candidateScore = scoreListingSnapshotReliability(candidate);
  const currentScore = scoreListingSnapshotReliability(current);
  if (candidateScore !== currentScore) {
    return candidateScore > currentScore ? 1 : -1;
  }
  return compareBuildingSnapshotCompleteness(candidate, current);
}

function chooseBestListingSnapshotCandidate(candidates) {
  return toArray(candidates)
    .filter(Boolean)
    .reduce((best, candidate) => {
      if (!best) {
        return candidate;
      }
      return compareListingSnapshotReliability(candidate, best) > 0 ? candidate : best;
    }, null);
}

function buildKaptSnapshotCandidate(
  snapshot,
  metrics,
  { preferSupplement = false, matchedKaptCode = null, kaptStatus = null } = {},
) {
  const candidate = {
    ...snapshot,
    matchedKaptCode: matchedKaptCode || snapshot?.matchedKaptCode || null,
  };
  let applied = false;

  const assignMetric = (metricKey, value, normalizer, sourceKeyValue = "kapt") => {
    const normalizedValue = normalizer(value);
    if (!hasMeaningfulBuildingMetric(normalizedValue)) {
      return;
    }
    if (preferSupplement || !hasMeaningfulBuildingMetric(candidate?.[metricKey])) {
      candidate[metricKey] = normalizedValue;
      candidate[getSnapshotMetricSourceKey(metricKey)] = sourceKeyValue;
      applied = true;
    }
  };

  assignMetric("households", metrics?.households, normalizeSnapshotMetricInteger);
  assignMetric("siteArea", metrics?.siteArea, normalizeSnapshotMetricNumber);
  assignMetric("currentFar", metrics?.currentFar, normalizeSnapshotMetricNumber);
  const normalizedGrossFloorArea = normalizeSnapshotMetricNumber(
    metrics?.grossFloorArea,
  );
  if (
    hasMeaningfulBuildingMetric(normalizedGrossFloorArea) &&
    (preferSupplement || !hasMeaningfulBuildingMetric(candidate?.grossFloorArea))
  ) {
    candidate.grossFloorArea = normalizedGrossFloorArea;
    candidate.grossFloorAreaSource = "kapt";
  }
  if (metrics?.siteAreaFieldKey && candidate.siteAreaSource === "kapt") {
    candidate.siteAreaFieldKey = metrics.siteAreaFieldKey;
  }
  if (metrics?.grossFloorAreaFieldKey && candidate.grossFloorAreaSource === "kapt") {
    candidate.grossFloorAreaFieldKey = metrics.grossFloorAreaFieldKey;
  }

  if (applied) {
    candidate.sourceReady = true;
    candidate.probeStatus = snapshot?.probeStatus === "ok" && !preferSupplement
      ? "ok"
      : "kapt_supplemented";
  }
  if (kaptStatus) {
    candidate.kaptStatus = kaptStatus;
  }

  return candidate;
}

function sumDistinctPositiveIntegers(items, keys) {
  const seen = new Set();
  let total = 0;
  let found = false;

  for (const item of toArray(items)) {
    const value = pickFirstInteger(item, keys);
    if (value == null || value <= 0) {
      continue;
    }

    const dedupeKey =
      normalizeText(
        pickFirstValue(item, ["dongNm", "mainAtchGbCdNm", "bldNm"]) || "",
      ) || `row-${seen.size + 1}`;
    if (seen.has(dedupeKey)) {
      continue;
    }
    seen.add(dedupeKey);
    total += value;
    found = true;
  }

  return found ? total : null;
}

function extractBuildingHubHouseholds(recapItems, titleItems, basisItems) {
  const keys = ["hhldCnt", "houseHoldCnt", "totHhldCnt"];
  const recapHouseholds = pickFirstIntegerFromSources(recapItems, keys);
  if (recapHouseholds != null && recapHouseholds > 0) {
    return recapHouseholds;
  }

  const basisHouseholds = pickFirstIntegerFromSources(basisItems, keys);
  if (basisHouseholds != null && basisHouseholds > 0) {
    return basisHouseholds;
  }

  const summedTitleHouseholds = sumDistinctPositiveIntegers(titleItems, keys);
  if (summedTitleHouseholds != null && summedTitleHouseholds > 0) {
    return summedTitleHouseholds;
  }

  return pickFirstIntegerFromSources([...titleItems, ...basisItems], keys);
}

function classifyKaptStatusFromError(error) {
  const message = String(error?.message || "");
  if (/HTTP\s*403/i.test(message) || /forbidden/i.test(message)) {
    return "forbidden";
  }
  if (/HTTP\s*401/i.test(message) || /unauthorized/i.test(message)) {
    return "unauthorized";
  }
  return "upstream_error";
}

function buildKaptMessage(status, options = {}) {
  const blockedCount = Number.parseInt(String(options?.blockedCount || 0), 10) || 0;
  const blockedSuffix =
    blockedCount > 0
      ? ` 건축HUB에 대지면적 또는 세대수가 없는 ${blockedCount}개 단지는 계산불가로 남습니다.`
      : "";
  if (status === "forbidden") {
    return `현재 API 키로는 KAPT 보강 API(getSigunguAptList3/getAphusBassInfoV4)를 호출할 수 없습니다 (HTTP 403).${blockedSuffix} data.go.kr에서 KAPT API 활용신청/승인 후 서버 키를 갱신해 주세요.`;
  }
  if (status === "unauthorized") {
    return `현재 API 키로는 KAPT 보강 API 인증에 실패했습니다.${blockedSuffix}`;
  }
  if (status === "config_error") {
    return "KAPT 보강 API를 호출할 키가 설정되지 않았습니다.";
  }
  if (status === "upstream_error") {
    return options?.detail || "KAPT 보강 API 호출 중 오류가 발생했습니다.";
  }
  if (status === "empty_body") {
    return `KAPT 보강 API 응답 본문이 비어 있습니다.${blockedSuffix}`;
  }
  if (status === "no_match") {
    return blockedCount > 0
      ? `건축HUB 보강이 필요한 ${blockedCount}개 단지에 대해 KAPT 일치 후보를 찾지 못했습니다.`
      : "KAPT 일치 후보를 찾지 못했습니다.";
  }
  return options?.detail || "";
}

function buildAddressParcelFragment(bun, ji) {
  const bunDigits = String(bun || "").replace(/^0+/, "").replace(/\D/g, "");
  const jiDigits = String(ji || "").replace(/^0+/, "").replace(/\D/g, "");
  if (!bunDigits) {
    return "";
  }
  return jiDigits ? `${bunDigits}-${jiDigits}` : bunDigits;
}

function allocateSharedKaptSiteArea(items) {
  const grouped = new Map();

  for (const item of toArray(items)) {
    const kaptCode = item?.snapshot?.matchedKaptCode;
    if (!kaptCode) {
      continue;
    }
    if (!grouped.has(kaptCode)) {
      grouped.set(kaptCode, []);
    }
    grouped.get(kaptCode).push(item);
  }

  for (const group of grouped.values()) {
    const candidates = group.filter(
      (item) =>
        item?.snapshot?.siteAreaSource === "kapt" &&
        hasMeaningfulBuildingMetric(item?.snapshot?.siteArea),
    );
    if (candidates.length <= 1) {
      continue;
    }

    const allHaveHouseholds = group.every((item) =>
      hasMeaningfulBuildingMetric(item?.snapshot?.households),
    );
    if (!allHaveHouseholds) {
      continue;
    }

    const totalHouseholds = group.reduce(
      (sum, item) => sum + Number(item.snapshot.households || 0),
      0,
    );
    if (!Number.isFinite(totalHouseholds) || totalHouseholds <= 0) {
      continue;
    }

    const totalSiteArea = Math.max(
      ...candidates.map((item) => Number(item.snapshot.siteArea || 0)),
    );
    if (!Number.isFinite(totalSiteArea) || totalSiteArea <= 0) {
      continue;
    }

    for (const item of candidates) {
      item.snapshot = {
        ...item.snapshot,
        siteArea: roundNullable(
          totalSiteArea * (Number(item.snapshot.households || 0) / totalHouseholds),
          2,
        ),
        siteAreaSource: "kapt_apportioned",
      };
    }
  }

  return items;
}

function normalizeNumber(value) {
  const numeric = Number(value || 0);
  return Number.isFinite(numeric) ? numeric.toFixed(2) : "0.00";
}

function parseAmount(raw) {
  if (raw == null) {
    return 0;
  }

  const parsed = Number.parseInt(String(raw).replace(/,/g, "").trim(), 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function parseFloatSafe(raw) {
  const parsed = Number.parseFloat(String(raw || "").trim());
  return Number.isFinite(parsed) ? parsed : 0;
}

function parseIntSafe(raw) {
  const parsed = Number.parseInt(String(raw || "").trim(), 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function makeDate(year, month, day) {
  if (!year) {
    return "";
  }
  return `${year}-${String(month || "").padStart(2, "0")}-${String(day || "").padStart(2, "0")}`;
}

function decodeXml(value) {
  return String(value || "")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&amp;/g, "&");
}

function extractXmlItems(xmlText) {
  return String(xmlText || "").match(/<item\b[\s\S]*?<\/item>/g) || [];
}

function parseXmlItemObject(itemXml) {
  const item = {};
  const text = String(itemXml || "");
  const pattern = /<([A-Za-z0-9_]+)>([\s\S]*?)<\/\1>/g;
  let match = pattern.exec(text);

  while (match) {
    const key = match[1];
    if (!Object.prototype.hasOwnProperty.call(item, key)) {
      item[key] = decodeXml(match[2]).trim();
    }
    match = pattern.exec(text);
  }

  return item;
}

function getXmlTagValue(itemXml, tagName) {
  const pattern = new RegExp(`<${tagName}>([\\s\\S]*?)<\\/${tagName}>`);
  const match = String(itemXml || "").match(pattern);
  return match ? decodeXml(match[1]).trim() : "";
}

function splitJibun(rawJibun, rawBun, rawJi) {
  let bun = String(rawBun || "").trim();
  let ji = String(rawJi || "").trim();

  if (!bun && rawJibun) {
    const [main, sub = ""] = String(rawJibun).split("-");
    bun = String(main || "").trim();
    ji = String(sub || "").trim();
  }

  bun = bun.replace(/^0+(\d)/, "$1");
  ji = ji.replace(/^0+(\d)/, "$1");

  return { bun, ji };
}

function isExistingRegionStatus(status) {
  const codes = [...String(status || "")].map((char) => char.codePointAt(0));
  return codes.length === 2 && codes[0] === 0xc874 && codes[1] === 0xc7ac;
}

function buildRegionResourceState() {
  const state = {
    bjDongCodeMap: new Map(),
    regionCodeNameMap: new Map(),
    regionCatalog: {
      sidoOptions: [],
      sigunguOptionsBySido: {},
    },
  };

  try {
    const fileText = fs.readFileSync(REGION_CODE_FILE, "utf8");
    const sidoMap = new Map();
    const sigunguMap = new Map();

    for (const line of fileText.split(/\r?\n/).slice(1)) {
      const [fullCode, fullName, status] = line.split("\t");
      if (!fullCode || !fullName || !isExistingRegionStatus(status)) {
        continue;
      }

      const trimmedName = fullName.trim();
      if (!fullCode.endsWith("00000")) {
        const nameParts = trimmedName.split(/\s+/);
        const dongName = nameParts[nameParts.length - 1];
        state.bjDongCodeMap.set(
          `${fullCode.slice(0, 5)}|${normalizeText(dongName)}`,
          fullCode.slice(5, 10),
        );
        continue;
      }

      if (!/^\d{10}$/.test(fullCode)) {
        continue;
      }

      if (fullCode.slice(2) === "00000000") {
        const sidoCode = fullCode.slice(0, 2);
        sidoMap.set(sidoCode, {
          code: sidoCode,
          name: trimmedName,
        });
        continue;
      }

      const regionCode = fullCode.slice(0, 5);
      const sidoCode = regionCode.slice(0, 2);
      const tokens = trimmedName.split(/\s+/);
      const sidoName = tokens[0] || sidoCode;
      const sigunguName = tokens.slice(1).join(" ") || trimmedName;

      state.regionCodeNameMap.set(regionCode, trimmedName);

      if (!sidoMap.has(sidoCode)) {
        sidoMap.set(sidoCode, {
          code: sidoCode,
          name: sidoName,
        });
      }

      const items = sigunguMap.get(sidoCode) || [];
      if (!items.some((item) => item.code === regionCode)) {
        items.push({
          code: regionCode,
          name: sigunguName,
          fullName: trimmedName,
        });
      }
      sigunguMap.set(sidoCode, items);
    }

    const sigunguOptionsBySido = {};
    for (const [sidoCode, items] of sigunguMap.entries()) {
      sigunguOptionsBySido[sidoCode] = items.sort((a, b) =>
        a.name.localeCompare(b.name, "ko"),
      );
    }

    state.regionCatalog = {
      sidoOptions: [...sidoMap.values()]
        .filter((item) => (sigunguOptionsBySido[item.code] || []).length > 0)
        .sort((a, b) => a.name.localeCompare(b.name, "ko")),
      sigunguOptionsBySido,
    };
  } catch {
    return state;
  }

  return state;
}

const REGION_RESOURCE_STATE = buildRegionResourceState();

function loadBjdongCodeMap() {
  return REGION_RESOURCE_STATE.bjDongCodeMap;
}

const BJ_DONG_CODE_MAP = REGION_RESOURCE_STATE.bjDongCodeMap;

// 5자리 지역코드 → "서울특별시 관악구" 형태의 전체 이름 매핑
function loadRegionCodeNameMap() {
  return REGION_RESOURCE_STATE.regionCodeNameMap;
}

const REGION_CODE_NAME_MAP = REGION_RESOURCE_STATE.regionCodeNameMap;

function loadRegionCatalog() {
  return REGION_RESOURCE_STATE.regionCatalog;
}

const REGION_CATALOG = REGION_RESOURCE_STATE.regionCatalog;

function lookupBjdongCode(sigunguCd, dongName) {
  return (
    BJ_DONG_CODE_MAP.get(
      `${String(sigunguCd || "").slice(0, 5)}|${normalizeText(dongName)}`,
    ) || ""
  );
}

function emptyTradeSummary() {
  return {
    median_price_10k: 0,
    min_price_10k: 0,
    max_price_10k: 0,
    sample_count: 0,
  };
}

function emptyRentSummary() {
  return {
    median_deposit_10k: 0,
    min_deposit_10k: 0,
    max_deposit_10k: 0,
    monthly_rent_avg_10k: 0,
    jeonse_ratio_pct: null,
    sample_count: 0,
  };
}

function buildNoDataPayload(kind) {
  return {
    totalCount: 0,
    items: [],
    summary: kind === "trade" ? emptyTradeSummary() : emptyRentSummary(),
    source: SOURCE,
  };
}

function sanitizeUrl(targetUrl) {
  return String(targetUrl).replace(/serviceKey=[^&]+/gi, "serviceKey=***");
}

function fetchText(targetUrl, headers = {}) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(targetUrl);
    const client = parsedUrl.protocol === "http:" ? http : https;
    const request = client.request(
      {
        method: "GET",
        hostname: parsedUrl.hostname,
        port: parsedUrl.port || undefined,
        path: parsedUrl.pathname + parsedUrl.search,
        headers: {
          "User-Agent": USER_AGENT,
          ...headers,
        },
      },
      (response) => {
        let data = "";
        const MAX_RESPONSE_SIZE = 10 * 1024 * 1024;
        response.setEncoding("utf8");
        response.on("data", (chunk) => {
          data += chunk;
          if (data.length > MAX_RESPONSE_SIZE) {
            request.destroy(new Error("Response too large"));
            return;
          }
        });
        response.on("end", () => {
          const statusCode = response.statusCode || 500;
          if (statusCode >= 400) {
            reject(new Error(`HTTP ${statusCode}`));
            return;
          }
          resolve({
            statusCode,
            headers: response.headers,
            body: data,
          });
        });
      },
    );

    request.setTimeout(15000, () => {
      const err = new Error("Gateway Timeout: upstream did not respond within 15s");
      err.code = "ETIMEDOUT";
      request.destroy(err);
    });
    request.on("error", reject);
    request.end();
  });
}

function buildDataGoKrUrl(baseUrl, params) {
  const searchParams = new URLSearchParams();

  Object.entries(params).forEach(([key, value]) => {
    if (value != null && value !== "") {
      searchParams.set(key, String(value));
    }
  });

  if (API_KEY) {
    searchParams.set("serviceKey", API_KEY);
  }

  return `${baseUrl}?${searchParams.toString()}`;
}

function toArray(value) {
  if (Array.isArray(value)) {
    return value;
  }
  if (value == null) {
    return [];
  }
  return [value];
}

function getRecentYearMonthsServer(count, startOffset = 1) {
  const now = new Date();
  const months = [];
  for (let i = startOffset; i < startOffset + count; i += 1) {
    const date = new Date(now.getFullYear(), now.getMonth() - i, 1);
    months.push(
      `${date.getFullYear()}${String(date.getMonth() + 1).padStart(2, "0")}`,
    );
  }
  return months;
}

function parseDataGoResponseItems(body) {
  const text = String(body || "").trim();
  if (!text) {
    return [];
  }

  try {
    const parsed = JSON.parse(text);
    const bodyNode = parsed?.response?.body || {};
    const itemCandidates = [
      bodyNode?.items?.item,
      bodyNode?.items,
      bodyNode?.item,
    ];
    for (const candidate of itemCandidates) {
      const normalized = toArray(candidate).filter(Boolean);
      if (normalized.length > 0) {
        return normalized;
      }
    }
    return [];
  } catch {
    return extractXmlItems(text).map(parseXmlItemObject).filter(Boolean);
  }
}

function parseDataGoResponseMeta(body) {
  const text = String(body || "").trim();
  if (!text) {
    return {
      resultCode: "",
      resultMsg: "",
    };
  }

  try {
    const parsed = JSON.parse(text);
    return {
      resultCode: String(
        parsed?.response?.header?.resultCode ??
          parsed?.header?.resultCode ??
          "",
      ).trim(),
      resultMsg: String(
        parsed?.response?.header?.resultMsg ??
          parsed?.header?.resultMsg ??
          "",
      ).trim(),
    };
  } catch {
    return {
      resultCode: getXmlTagValue(text, "resultCode"),
      resultMsg: getXmlTagValue(text, "resultMsg"),
    };
  }
}

function pickFirstValue(source, keys) {
  for (const key of keys) {
    const value = source?.[key];
    if (value != null && value !== "") {
      return value;
    }
  }
  return "";
}

function pickFirstNumber(source, keys) {
  for (const key of keys) {
    const parsed = Number.parseFloat(String(source?.[key] ?? "").replace(/,/g, ""));
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function pickFirstNumberWithKey(source, keys) {
  for (const key of keys) {
    const parsed = Number.parseFloat(String(source?.[key] ?? "").replace(/,/g, ""));
    if (Number.isFinite(parsed)) {
      return {
        value: parsed,
        key,
      };
    }
  }
  return {
    value: null,
    key: "",
  };
}

function pickFirstInteger(source, keys) {
  for (const key of keys) {
    const parsed = Number.parseInt(String(source?.[key] ?? "").replace(/,/g, ""), 10);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function pickFirstValueFromSources(sources, keys) {
  for (const source of toArray(sources)) {
    const value = pickFirstValue(source, keys);
    if (value) {
      return value;
    }
  }
  return "";
}

function pickFirstNumberFromSources(sources, keys) {
  let fallback = null;
  for (const source of toArray(sources)) {
    const value = pickFirstNumber(source, keys);
    if (value != null) {
      if (value > 0) {
        return value;
      }
      if (fallback == null) {
        fallback = value;
      }
    }
  }
  return fallback;
}

function pickFirstNumberWithKeyFromSources(sources, keys) {
  let fallback = {
    value: null,
    key: "",
  };
  for (const source of toArray(sources)) {
    const candidate = pickFirstNumberWithKey(source, keys);
    if (candidate.value != null) {
      if (candidate.value > 0) {
        return candidate;
      }
      if (fallback.value == null) {
        fallback = candidate;
      }
    }
  }
  return fallback;
}

function pickFirstIntegerFromSources(sources, keys) {
  let fallback = null;
  for (const source of toArray(sources)) {
    const value = pickFirstInteger(source, keys);
    if (value != null) {
      if (value > 0) {
        return value;
      }
      if (fallback == null) {
        fallback = value;
      }
    }
  }
  return fallback;
}

function extractCompletionYear(source, fallback = 0) {
  const raw = String(
    pickFirstValue(source, [
      "useAprDay",
      "useAprvDe",
      "useAprvDate",
      "useAprDay",
    ]) || "",
  );
  const match = raw.match(/(\d{4})/);
  return match ? Number.parseInt(match[1], 10) : Number(fallback || 0);
}

function extractCompletionDateRaw(source) {
  return String(
    pickFirstValue(source, ["useAprDay", "useAprvDe", "useAprvDate"]) || "",
  ).trim();
}

function extractCompletionDateRawFromSources(sources) {
  for (const source of toArray(sources)) {
    const raw = extractCompletionDateRaw(source);
    if (raw) {
      return raw;
    }
  }
  return "";
}

function formatCompletionYearMonth(raw, fallbackYear = 0) {
  const digits = String(raw || "").replace(/\D/g, "");
  if (digits.length >= 6) {
    return `${digits.slice(0, 4)}-${digits.slice(4, 6)}`;
  }

  const fallbackDigits = String(fallbackYear || "").replace(/\D/g, "").slice(0, 4);
  if (fallbackDigits.length === 4) {
    return `${fallbackDigits}-00`;
  }

  return "";
}

function extractCompletionYearFromSources(sources, fallback = 0) {
  for (const source of toArray(sources)) {
    const year = extractCompletionYear(source, 0);
    if (year) {
      return year;
    }
  }
  return Number(fallback || 0);
}

function normalizeZoneName(rawName) {
  return String(rawName || "").replace(/\s+/g, "");
}

const GENERIC_ZONE_NAMES = new Set([
  "\uB3C4\uC2DC\uC9C0\uC5ED",
  "\uC8FC\uAC70\uC9C0\uC5ED",
  "\uC804\uC6A9\uC8FC\uAC70\uC9C0\uC5ED",
  "\uC77C\uBC18\uC8FC\uAC70\uC9C0\uC5ED",
  "\uC0C1\uC5C5\uC9C0\uC5ED",
  "\uACF5\uC5C5\uC9C0\uC5ED",
]);

function isGenericZoneName(rawName) {
  const normalized = normalizeZoneName(rawName);
  return normalized ? GENERIC_ZONE_NAMES.has(normalized) : false;
}

function getFarLimitPreset(regionCode) {
  const prefix = String(regionCode || "").slice(0, 2);
  if (prefix === "11") {
    return {
      ...LISTING_LEGAL_FAR_LIMITS.default,
      ...LISTING_LEGAL_FAR_LIMITS.seoul,
    };
  }
  if (prefix === "41") {
    return {
      ...LISTING_LEGAL_FAR_LIMITS.default,
      ...LISTING_LEGAL_FAR_LIMITS.gyeonggi,
    };
  }
  return LISTING_LEGAL_FAR_LIMITS.default;
}

function resolveLegalFarLimit(regionCode, zoneName) {
  const normalized = normalizeZoneName(zoneName);
  if (!normalized) {
    return null;
  }

  const presets = getFarLimitPreset(regionCode);
  const entries = Object.entries(presets)
    .map(([label, value]) => [normalizeZoneName(label), value])
    .sort((a, b) => b[0].length - a[0].length);

  for (const [label, value] of entries) {
    if (normalized === label) {
      return value;
    }
  }

  const partialMatches = entries
    .filter(([label]) => normalized.length > label.length && normalized.includes(label))
    .map(([, value]) => value);
  if (partialMatches.length > 0) {
    return Math.max(...partialMatches);
  }

  // 일반주거지역 등 세분화 안 된 용도지역 → 해당 카테고리 내 최솟값(보수적) 적용
  // e.g. "일반주거지역" → 제1~3종일반주거지역 중 최솟값(200%) — 사업성 과대평가 방지
  if (isGenericZoneName(normalized)) {
    const GENERIC_FALLBACKS = {
      "일반주거지역": "일반주거",
      "전용주거지역": "전용주거",
      "상업지역": "상업지역",
      "공업지역": "공업지역",
    };
    for (const [generic, keyword] of Object.entries(GENERIC_FALLBACKS)) {
      if (normalized.includes(generic) || normalized === generic) {
        const candidates = entries
          .filter(([label]) => label.includes(normalizeZoneName(keyword)))
          .map(([, value]) => value);
        if (candidates.length > 0) {
          return Math.min(...candidates);
        }
      }
    }
    return null;
  }

  return null;
}

function getResidentialFarRatio(zoneName) {
  if (!zoneName) return 1.0;
  const normalized = normalizeZoneName(zoneName);
  for (const [zone, ratio] of Object.entries(LISTING_RESIDENTIAL_FAR_RATIO)) {
    if (normalized === normalizeZoneName(zone) || normalized.includes(normalizeZoneName(zone))) {
      return ratio;
    }
  }
  return 1.0;
}

function roundNullable(value, digits = 2) {
  if (!Number.isFinite(value)) {
    return null;
  }
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function getTimedMapValue(cache, key, ttlMs) {
  const entry = cache.get(key);
  if (!entry) {
    return null;
  }
  if (Date.now() - entry.ts > ttlMs) {
    cache.delete(key);
    return null;
  }
  return entry.data;
}

function setTimedMapValue(cache, key, value) {
  cache.set(key, {
    ts: Date.now(),
    data: value,
  });
  return value;
}

function normalizeAddressQuery(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function uniqueNonEmptyStrings(values) {
  const seen = new Set();
  const items = [];
  for (const value of toArray(values)) {
    const normalized = normalizeAddressQuery(value);
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    items.push(normalized);
  }
  return items;
}

// seed의 sigunguCd + bjdongCd + bun + ji로 19자리 PNU 직접 구성
function buildPnuFromSeed(seed) {
  const sigunguCd = String(seed?.sigunguCd || "").replace(/\D/g, "");
  const bjdongCd = String(seed?.bjdongCd || "").replace(/\D/g, "");
  const bun = String(seed?.bun || "").replace(/\D/g, "");
  // SEC-INJ-001: 엄격한 숫자 전용 검증
  if (!/^\d{5}$/.test(sigunguCd) || !/^\d{5}$/.test(bjdongCd) || !bun || bun.length > 4) {
    return null;
  }
  const paddedBun = bun.padStart(4, "0").slice(0, 4);
  const paddedJi = (String(seed?.ji || "").replace(/\D/g, "") || "0000").padStart(4, "0").slice(0, 4);
  const pnu = `${sigunguCd}${bjdongCd}0${paddedBun}${paddedJi}`;
  return PNU_REGEX.test(pnu) ? pnu : null;
}

function parsePnuToParcel(pnu) {
  const digits = String(pnu || "").replace(/\D/g, "");
  if (digits.length !== 19) {
    return null;
  }
  return {
    pnu: digits,
    sigunguCd: digits.slice(0, 5),
    bjdongCd: digits.slice(5, 10),
    bun: digits.slice(11, 15),
    ji: digits.slice(15, 19),
  };
}

function extractPnuFromVworldPayload(payload) {
  const candidates = [
    payload?.response?.refined?.structure?.level4LC,
    payload?.response?.refined?.text,
    payload?.response?.result?.structure?.level4LC,
    payload?.response?.result?.pnu,
    payload?.response?.refined?.structure?.pnu,
  ];

  for (const candidate of candidates) {
    const parsed = parsePnuToParcel(candidate);
    if (parsed) {
      return parsed;
    }
  }
  return null;
}

function parseCoordinate(rawValue) {
  const numeric = Number.parseFloat(String(rawValue ?? "").trim());
  return Number.isFinite(numeric) ? numeric : null;
}

function extractPointFromVworldPayload(payload) {
  const lng = parseCoordinate(
    payload?.response?.result?.point?.x ?? payload?.response?.result?.point?.lon,
  );
  const lat = parseCoordinate(
    payload?.response?.result?.point?.y ?? payload?.response?.result?.point?.lat,
  );
  if (lng == null || lat == null) {
    return null;
  }
  return {
    lng,
    lat,
  };
}

function normalizeCoordinatePoint(point) {
  const lng = parseCoordinate(point?.lng ?? point?.x);
  const lat = parseCoordinate(point?.lat ?? point?.y);
  if (lng == null || lat == null) {
    return null;
  }
  return {
    lng,
    lat,
  };
}

function buildVworldSearchUrl(params) {
  const searchApiKey = VWORLD_API_KEY || VWORLD_DATA_API_KEY;
  const searchParams = new URLSearchParams({
    service: "search",
    request: "search",
    version: "2.0",
    type: "place",
    format: "json",
    errorFormat: "json",
    ...params,
  });

  if (searchApiKey) {
    searchParams.set("key", searchApiKey);
  }

  return `https://api.vworld.kr/req/search?${searchParams.toString()}`;
}

function buildVworldDataUrl(params) {
  const searchParams = new URLSearchParams();
  const mergedParams = {
    service: "data",
    version: "2.0",
    request: "GetFeature",
    format: "json",
    errorFormat: "json",
    geometry: "false",
    attribute: "true",
    ...params,
  };

  Object.entries(mergedParams).forEach(([key, value]) => {
    if (value != null && value !== "") {
      searchParams.set(key, String(value));
    }
  });

  if (VWORLD_DATA_API_KEY) {
    searchParams.set("key", VWORLD_DATA_API_KEY);
  }
  if (VWORLD_DATA_DOMAIN) {
    searchParams.set("domain", VWORLD_DATA_DOMAIN);
  }

  return `https://api.vworld.kr/req/data?${searchParams.toString()}`;
}

function extractVworldSearchItems(payload) {
  const candidates = [
    payload?.response?.result?.items?.item,
    payload?.response?.result?.items,
    payload?.response?.result?.item,
  ];

  for (const candidate of candidates) {
    const items = toArray(candidate).filter(Boolean);
    if (items.length > 0) {
      return items;
    }
  }

  return [];
}

function normalizeVworldSearchItem(item) {
  const point = normalizeCoordinatePoint(item?.point);
  if (!point) {
    return null;
  }

  return {
    title: String(item?.title || "").trim(),
    category: String(item?.category || "").trim(),
    address: String(
      item?.address?.parcel || item?.address?.road || item?.address || "",
    ).trim(),
    lng: point.lng,
    lat: point.lat,
  };
}

function extractVworldFeatureList(payload) {
  const candidates = [
    payload?.response?.result?.featureCollection?.features,
    payload?.response?.result?.features,
    payload?.response?.result?.items?.item,
    payload?.response?.result?.items,
    payload?.response?.result,
  ];

  for (const candidate of candidates) {
    const items = toArray(candidate).filter(Boolean);
    if (items.length > 0) {
      return items;
    }
  }

  return [];
}

function extractVworldZoningName(payload) {
  for (const item of extractVworldFeatureList(payload)) {
    const properties = item?.properties || item?.property || item;
    const zoneName = pickFirstValue(properties, ["uname", "UNAME", "name", "NAME"]);
    if (zoneName) {
      return String(zoneName).trim();
    }
  }
  return "";
}

async function fetchVworldZoning(parcelMetadata) {
  const pnu = String(parcelMetadata?.pnu || "").trim();
  const lng = roundNullable(
    parseCoordinate(parcelMetadata?.point?.lng ?? parcelMetadata?.point?.x),
    7,
  );
  const lat = roundNullable(
    parseCoordinate(parcelMetadata?.point?.lat ?? parcelMetadata?.point?.y),
    7,
  );
  if (lng == null || lat == null) {
    return {
      status: "bad_request",
      zoning: null,
    };
  }

  const cacheKey = PNU_REGEX.test(pnu) ? `pnu:${pnu}` : `${lng}|${lat}`;
  const cached = getTimedMapValue(
    vworldZoningCache,
    cacheKey,
    VWORLD_ZONING_CACHE_TTL_MS,
  );
  if (cached) {
    return cached;
  }

  if (!VWORLD_DATA_API_KEY) {
    return setTimedMapValue(vworldZoningCache, cacheKey, {
      status: "config_error",
      zoning: null,
    });
  }

  const targetUrl = buildVworldDataUrl({
    data: VWORLD_URBAN_ZONE_LAYER,
    geomFilter: `POINT(${lng} ${lat})`,
    columns: "uname,sido_name,sigg_name",
    crs: "EPSG:4326",
    size: "5",
    buffer: "1",
  });

  try {
    const { body } = await fetchText(targetUrl, {
      Accept: "application/json, */*",
    });
    const parsed = JSON.parse(body);
    const status = String(parsed?.response?.status || "").trim().toUpperCase();
    if (status === "OK") {
      const zoning = extractVworldZoningName(parsed) || null;
      return setTimedMapValue(vworldZoningCache, cacheKey, {
        status: zoning ? "ok" : "no_match",
        zoning,
      });
    }

    const errorCode = normalizeText(parsed?.response?.error?.code || "");
    const mappedStatus =
      errorCode === "invalidkey" ||
      errorCode === "incorrectkey" ||
      errorCode === "unavailablekey"
        ? "config_error"
        : errorCode === "overrequestlimit"
          ? "rate_limited"
          : status === "NOT_FOUND"
            ? "no_match"
            : "upstream_error";
    return setTimedMapValue(vworldZoningCache, cacheKey, {
      status: mappedStatus,
      zoning: null,
      code: errorCode || null,
      message: parsed?.response?.error?.text || "",
    });
  } catch (error) {
    return setTimedMapValue(vworldZoningCache, cacheKey, {
      status:
        error?.code === "ETIMEDOUT"
          ? "upstream_error"
          : /HTTP\s*40[13]/i.test(String(error?.message || ""))
            ? "forbidden"
            : "upstream_error",
      zoning: null,
      message: error.message,
    });
  }
}

// 토지이용계획속성조회 API — PNU 기반으로 세분화된 용도지역 반환
// (기존 좌표 기반 LT_C_UQ111 레이어보다 정확: "일반주거지역" → "제2종일반주거지역")
async function fetchVworldLandUseAttr(pnu) {
  if (!VWORLD_LANDUSE_API_KEY) {
    return { status: "config_error", zoning: null };
  }
  if (!PNU_REGEX.test(String(pnu || ""))) {
    return { status: "bad_request", zoning: null };
  }

  const cacheKey = `landuse:${pnu}`;
  const cached = getTimedMapValue(vworldZoningCache, cacheKey, VWORLD_ZONING_CACHE_TTL_MS);
  if (cached) return cached;

  const params = new URLSearchParams({
    key: VWORLD_LANDUSE_API_KEY,
    pnu: String(pnu),
    format: "json",
    numOfRows: "20",
    pageNo: "1",
  });
  if (VWORLD_DATA_DOMAIN) {
    params.set("domain", VWORLD_DATA_DOMAIN);
  }
  const targetUrl = `https://api.vworld.kr/ned/data/getLandUseAttr?${params}`;

  try {
    const { body } = await fetchText(targetUrl);
    if (!body) {
      const result = { status: "upstream_error", zoning: null };
      setTimedMapValue(vworldZoningCache, cacheKey, result);
      return result;
    }
    const parsed = JSON.parse(body);

    // INCORRECT_KEY 등 인증 오류 처리
    const resultCode =
      parsed?.landUses?.resultCode || parsed?.response?.resultCode || "";
    if (resultCode === "INCORRECT_KEY") {
      const result = { status: "config_error", zoning: null };
      setTimedMapValue(vworldZoningCache, cacheKey, result);
      return result;
    }

    // 응답 구조: { landUses: { field: [...], totalCount, ... } }
    // 또는 데이터 없을 때: { response: { totalCount: "0", ... } }
    const attrs = parsed?.landUses?.field || parsed?.landUseAttr?.field || null;
    const totalCount = Number(
      parsed?.landUses?.totalCount || parsed?.response?.totalCount || 0,
    );
    if (!Array.isArray(attrs) || attrs.length === 0 || totalCount === 0) {
      const result = { status: "no_match", zoning: null };
      setTimedMapValue(vworldZoningCache, cacheKey, result);
      return result;
    }

    // cnflcAt 기준: "1"=포함, "2"=저촉, "3"=접함
    // 해당 필지가 실제로 포함된 용도지역(cnflcAt="1")만 사용
    const includedAttrs = attrs.filter((item) => String(item.cnflcAt) === "1");
    const sourceAttrs = includedAttrs.length > 0 ? includedAttrs : attrs;

    // 용도지역(주거/상업/공업) 중 가장 구체적인 것을 추출
    const zoningCandidates = sourceAttrs
      .map((item) => (item.prposAreaDstrcCodeNm || "").trim())
      .filter((name) => {
        const n = normalizeZoneName(name);
        return n && (n.includes("주거") || n.includes("상업") || n.includes("공업"));
      });

    // 가장 구체적인(길이가 긴) 용도지역을 선택
    // e.g. "제2종일반주거지역" > "일반주거지역" > "주거지역"
    const bestZoning = zoningCandidates.length > 0
      ? zoningCandidates.sort((a, b) => b.length - a.length)[0]
      : null;

    if (bestZoning) {
      const result = { status: "ok", zoning: bestZoning };
      setTimedMapValue(vworldZoningCache, cacheKey, result);
      schedulePersistentZoningSave();
      return result;
    }

    // 주거/상업/공업이 아닌 경우 (자연녹지, 개발제한 등)
    const anyZone = sourceAttrs
      .map((item) => (item.prposAreaDstrcCodeNm || "").trim())
      .filter(Boolean);
    const result = {
      status: anyZone.length > 0 ? "ok" : "no_match",
      zoning: anyZone[0] || null,
    };
    setTimedMapValue(vworldZoningCache, cacheKey, result);
    return result;
  } catch (err) {
    const msg = err?.message || String(err);
    if (msg.includes("authenticate") || msg.includes("Unauthorized") || msg.includes("403")) {
      const result = { status: "forbidden", zoning: null };
      setTimedMapValue(vworldZoningCache, cacheKey, result);
      return result;
    }
    const result = { status: "upstream_error", zoning: null };
    setTimedMapValue(vworldZoningCache, cacheKey, result);
    return result;
  }
}

async function supplementSnapshotWithVworldZoning(snapshot, parcelMetadata) {
  if (
    snapshot?.zoning ||
    (!PNU_REGEX.test(String(parcelMetadata?.pnu || "")) && !parcelMetadata?.point)
  ) {
    // 이미 zoning이 있더라도 generic이면 getLandUseAttr로 세분화 시도
    if (snapshot?.zoning && isGenericZoneName(snapshot.zoning) && PNU_REGEX.test(String(parcelMetadata?.pnu || ""))) {
      const landUseResult = await fetchVworldLandUseAttr(parcelMetadata.pnu);
      if (landUseResult.status === "ok" && landUseResult.zoning && !isGenericZoneName(landUseResult.zoning)) {
        return {
          ...snapshot,
          zoning: landUseResult.zoning,
          zoningSource: "vworld_landuse_attr",
          zoningStatus: "ok",
        };
      }
    }
    return snapshot;
  }

  // PNU가 있으면 getLandUseAttr API 우선 시도 (더 정확한 세분화 결과)
  const pnu = String(parcelMetadata?.pnu || "");
  if (PNU_REGEX.test(pnu)) {
    const landUseResult = await fetchVworldLandUseAttr(pnu);
    if (landUseResult.status === "ok" && landUseResult.zoning) {
      return {
        ...snapshot,
        zoning: landUseResult.zoning,
        zoningSource: "vworld_landuse_attr",
        zoningStatus: "ok",
      };
    }
  }

  // 폴백: 기존 좌표 기반 LT_C_UQ111 레이어 조회
  const zoningResult = await fetchVworldZoning(parcelMetadata);
  if (zoningResult.status !== "ok" || !zoningResult.zoning) {
    return {
      ...snapshot,
      zoningStatus: snapshot?.zoningStatus || zoningResult.status,
    };
  }

  return {
    ...snapshot,
    zoning: zoningResult.zoning,
    zoningSource: snapshot?.zoningSource || "vworld_landuse",
    zoningStatus: "ok",
  };
}

async function fetchVworldParcelForAddress(query) {
  const normalizedQuery = normalizeAddressQuery(query);
  if (!normalizedQuery) {
    return {
      status: "bad_request",
      query: normalizedQuery,
      parcel: null,
    };
  }

  const cached = getTimedMapValue(
    vworldAddressCache,
    normalizedQuery,
    VWORLD_ADDRESS_CACHE_TTL_MS,
  );
  if (cached) {
    return cached;
  }

  if (!VWORLD_API_KEY) {
    return setTimedMapValue(vworldAddressCache, normalizedQuery, {
      status: "config_error",
      query: normalizedQuery,
      parcel: null,
    });
  }

  const targetUrl = `https://api.vworld.kr/req/address?service=address&request=getcoord&version=2.0&crs=epsg:4326&type=PARCEL&format=json&key=${encodeURIComponent(VWORLD_API_KEY)}&address=${encodeURIComponent(normalizedQuery)}`;

  try {
    const { body } = await fetchText(targetUrl);
    const parsed = JSON.parse(body);
    const parcel = extractPnuFromVworldPayload(parsed);
    const point = extractPointFromVworldPayload(parsed);
    const status = String(parsed?.response?.status || "").trim().toUpperCase();
    const refinedText =
      normalizeAddressQuery(
        parsed?.response?.refined?.text || parsed?.response?.input?.address || normalizedQuery,
      ) || normalizedQuery;

    return setTimedMapValue(vworldAddressCache, normalizedQuery, {
      status: parcel && status === "OK" ? "ok" : "no_match",
      query: normalizedQuery,
      parcel:
        parcel &&
        {
          ...parcel,
          address: refinedText,
          dongName:
            parsed?.response?.refined?.structure?.level4L ||
            extractDongFromAddress(refinedText),
          point,
        },
    });
  } catch (error) {
    return setTimedMapValue(vworldAddressCache, normalizedQuery, {
      status:
        error?.code === "ETIMEDOUT"
          ? "upstream_error"
          : /HTTP\s*40[13]/i.test(String(error?.message || ""))
            ? "forbidden"
            : "upstream_error",
      query: normalizedQuery,
      parcel: null,
      message: error.message,
    });
  }
}

function collectKaptAddressCandidates(match, basicItem) {
  return uniqueNonEmptyStrings([
    pickFirstValue(basicItem, [
      "kaptAddr",
      "bjdAddress",
      "jibunAddress",
      "address",
    ]),
    match?.address,
    pickFirstValue(basicItem, ["doroJuso", "roadAddress"]),
  ]);
}

function buildSeedFromParcel(seed, parcel) {
  if (!parcel?.sigunguCd || !parcel?.bjdongCd || !parcel?.bun) {
    return null;
  }

  return {
    ...seed,
    sigunguCd: parcel.sigunguCd,
    bjdongCd: parcel.bjdongCd,
    bun: parcel.bun,
    ji: parcel.ji || "0000",
    dong: parcel.dongName || seed?.dong || "",
  };
}

function mergeBuildingSnapshots(baseSnapshot, supplementSnapshot, metadata = {}) {
  if (!supplementSnapshot) {
    return baseSnapshot;
  }

  const mergedCurrentFar = mergeSnapshotMetric(
    baseSnapshot,
    supplementSnapshot,
    "currentFar",
    metadata.currentFarSource || null,
  );
  const mergedSiteArea = mergeSnapshotMetric(
    baseSnapshot,
    supplementSnapshot,
    "siteArea",
    metadata.siteAreaSource || null,
  );
  const mergedGrossFloorArea = mergeSnapshotMetric(
    baseSnapshot,
    supplementSnapshot,
    "grossFloorArea",
    metadata.grossFloorAreaSource || null,
  );
  const mergedHouseholds = mergeSnapshotMetric(
    baseSnapshot,
    supplementSnapshot,
    "households",
    metadata.householdsSource || null,
  );

  const merged = {
    ...baseSnapshot,
    completionYear:
      supplementSnapshot.completionYear || baseSnapshot?.completionYear || null,
    completionYearMonth:
      supplementSnapshot.completionYearMonth ||
      baseSnapshot?.completionYearMonth ||
      "",
    zoning: baseSnapshot?.zoning || supplementSnapshot.zoning || null,
    zoningSource:
      baseSnapshot?.zoningSource ||
      supplementSnapshot.zoningSource ||
      metadata.zoningSource ||
      null,
    zoningStatus:
      baseSnapshot?.zoningStatus ||
      supplementSnapshot.zoningStatus ||
      metadata.zoningStatus ||
      null,
    currentFar: mergedCurrentFar.value,
    currentFarSource: mergedCurrentFar.source,
    siteArea: mergedSiteArea.value,
    siteAreaSource: mergedSiteArea.source,
    siteAreaFieldKey:
      baseSnapshot?.siteAreaFieldKey || supplementSnapshot?.siteAreaFieldKey || null,
    grossFloorArea: mergedGrossFloorArea.value,
    grossFloorAreaSource: mergedGrossFloorArea.source,
    grossFloorAreaFieldKey:
      baseSnapshot?.grossFloorAreaFieldKey ||
      supplementSnapshot?.grossFloorAreaFieldKey ||
      null,
    households: mergedHouseholds.value,
    householdsSource: mergedHouseholds.source,
    sourceReady: Boolean(baseSnapshot?.sourceReady || supplementSnapshot.sourceReady),
    probeStatus:
      supplementSnapshot.probeStatus === "ok"
        ? "ok"
        : baseSnapshot?.probeStatus || supplementSnapshot.probeStatus || "no_match",
    matchedKaptCode:
      baseSnapshot?.matchedKaptCode || supplementSnapshot.matchedKaptCode || null,
    kaptStatus: baseSnapshot?.kaptStatus || supplementSnapshot.kaptStatus || null,
  };

  if (
    metadata.parcelSource &&
    compareBuildingSnapshotCompleteness(merged, baseSnapshot) >= 0
  ) {
    merged.parcelSource = metadata.parcelSource;
    merged.vworldPnu = metadata.vworldPnu || baseSnapshot?.vworldPnu || null;
    merged.vworldAddress =
      metadata.vworldAddress || baseSnapshot?.vworldAddress || null;
  } else {
    merged.parcelSource = baseSnapshot?.parcelSource || null;
    merged.vworldPnu = baseSnapshot?.vworldPnu || null;
    merged.vworldAddress = baseSnapshot?.vworldAddress || null;
  }

  return merged;
}

function needsOfficialAnalysisFallback(snapshot) {
  return (
    !hasMeaningfulBuildingMetric(snapshot?.siteArea) ||
    !hasMeaningfulBuildingMetric(snapshot?.households) ||
    !hasMeaningfulBuildingMetric(snapshot?.currentFar) ||
    !snapshot?.zoning ||
    isGenericZoneName(snapshot?.zoning)
  );
}

async function repairSnapshotWithVworldFallback(snapshot, seed, addressCandidates) {
  const candidates = uniqueNonEmptyStrings(addressCandidates);
  if (!candidates.length || !VWORLD_API_KEY) {
    return snapshot;
  }

  let bestSnapshot = snapshot;

  for (const address of candidates) {
    const parcelResult = await fetchVworldParcelForAddress(address);
    if (parcelResult.status !== "ok" || !parcelResult.parcel) {
      continue;
    }

    const refinedSeed = buildSeedFromParcel(seed, parcelResult.parcel);
    if (!refinedSeed) {
      continue;
    }
    if (
      seed?.sigunguCd &&
      refinedSeed.sigunguCd &&
      String(refinedSeed.sigunguCd).slice(0, 5) !== String(seed.sigunguCd).slice(0, 5)
    ) {
      continue;
    }

    const refinedSnapshot = await fetchBuildingSnapshot(refinedSeed);
    const refinedWithZoning = await supplementSnapshotWithVworldZoning(
      refinedSnapshot,
      parcelResult.parcel,
    );
    const mergedSnapshot = mergeBuildingSnapshots(snapshot, refinedSnapshot, {
      parcelSource: "vworld_pnu",
      vworldPnu: parcelResult.parcel.pnu,
      vworldAddress: parcelResult.parcel.address,
      currentFarSource: "vworld_pnu_building_hub",
      siteAreaSource: "vworld_pnu_building_hub",
      householdsSource: "vworld_pnu_building_hub",
    });
    const mergedWithZoning = mergeBuildingSnapshots(snapshot, refinedWithZoning, {
      parcelSource: "vworld_pnu",
      vworldPnu: parcelResult.parcel.pnu,
      vworldAddress: parcelResult.parcel.address,
      zoningSource: refinedWithZoning?.zoningSource || null,
      zoningStatus: refinedWithZoning?.zoningStatus || null,
      currentFarSource: "vworld_pnu_building_hub",
      siteAreaSource: "vworld_pnu_building_hub",
      householdsSource: "vworld_pnu_building_hub",
    });
    if (compareListingSnapshotReliability(mergedWithZoning, bestSnapshot) > 0) {
      bestSnapshot = mergedWithZoning;
      if (!needsOfficialAnalysisFallback(bestSnapshot)) {
        break;
      }
    }
  }

  return bestSnapshot;
}

function extractDongFromAddress(address) {
  const normalized = String(address || "").trim();
  if (!normalized) {
    return "";
  }

  const match = normalized.match(
    /([0-9a-zA-Z\u3131-\u318e\uac00-\ud7a3]+(?:동|읍|면|리))(?:\s|$)/,
  );
  return match ? match[1] : "";
}

function parseDistanceToKm(raw) {
  const text = String(raw || "").trim();
  if (!text) {
    return null;
  }

  const matched = text.replace(/,/g, "").match(/-?\d+(?:\.\d+)?/);
  const numeric = matched ? Number.parseFloat(matched[0]) : Number.NaN;
  if (!Number.isFinite(numeric)) {
    return null;
  }

  if (/m(?![a-z])/i.test(text) || text.includes("미터")) {
    return roundNullable(numeric / 1000, 2);
  }
  if (text.toLowerCase().includes("km") || text.includes("킬로")) {
    return roundNullable(numeric, 2);
  }
  if (numeric > 20) {
    return roundNullable(numeric / 1000, 2);
  }
  return roundNullable(numeric, 2);
}

function formatWalkingDistanceLabel(distanceKm) {
  if (distanceKm == null) {
    return "";
  }
  const walkMinutes = Math.round(distanceKm / 0.08);
  const distStr =
    distanceKm < 1 ? `${Math.round(distanceKm * 1000)}m` : `${distanceKm.toFixed(1)}km`;
  return `(${distStr}, 도보${walkMinutes}분)`;
}

function formatStationDistanceLabel(distanceKm) {
  return formatWalkingDistanceLabel(distanceKm);
}

function pickEntryValueByKeyPredicate(source, predicate) {
  for (const [key, value] of Object.entries(source || {})) {
    if (value == null || value === "") {
      continue;
    }
    if (predicate(normalizeText(key))) {
      return String(value).trim();
    }
  }
  return "";
}

function normalizeParcelPart(value) {
  const digits = String(value || "").replace(/\D/g, "");
  if (!digits) {
    return "";
  }
  return String(Number.parseInt(digits, 10));
}

function buildParcelKey(dong, bun, ji) {
  const dongKey = normalizeText(dong);
  const bunKey = normalizeParcelPart(bun);
  const jiKey = normalizeParcelPart(ji);
  if (!dongKey || !bunKey) {
    return "";
  }
  return `${dongKey}|${bunKey}|${jiKey || "0"}`;
}

function tryReadJson(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return null;
  }
}

function coerceCoordinate(value) {
  const numeric = Number.parseFloat(String(value ?? "").trim());
  return Number.isFinite(numeric) ? numeric : null;
}

function loadStationDataset() {
  const state = localDataState.stations;
  const now = Date.now();
  if (state.mtimeMs == null && state.checkedAtMs && now - state.checkedAtMs < 60 * 1000) {
    return state;
  }
  try {
    const stats = fs.statSync(state.sourceFile);
    state.checkedAtMs = now;
    if (state.mtimeMs === stats.mtimeMs && state.items.length > 0) {
      return state;
    }

    const parsed = tryReadJson(state.sourceFile);
    const sourceItems = Array.isArray(parsed) ? parsed : toArray(parsed?.items);
    state.items = sourceItems
      .map((item) => {
        const lat = coerceCoordinate(item?.lat);
        const lng = coerceCoordinate(item?.lng);
        if (!item?.name || lat == null || lng == null) {
          return null;
        }
        return {
          name: String(item.name).trim(),
          line: String(item.line || "").trim(),
          address: String(item.address || "").trim(),
          lat,
          lng,
        };
      })
      .filter(Boolean);
    state.loadedAt = new Date().toISOString();
    state.mtimeMs = stats.mtimeMs;
    state.message =
      state.items.length > 0
        ? `loaded ${state.items.length} stations`
        : "station dataset is empty";
  } catch (error) {
    state.loadedAt = new Date().toISOString();
    state.checkedAtMs = now;
    state.mtimeMs = null;
    state.items = [];
    state.message = error.code === "ENOENT" ? "station dataset file missing" : error.message;
  }
  return state;
}

function loadApartmentCoordinateDataset() {
  const state = localDataState.apartmentCoords;
  const now = Date.now();
  if (state.mtimeMs == null && state.checkedAtMs && now - state.checkedAtMs < 60 * 1000) {
    return state;
  }
  try {
    const stats = fs.statSync(state.sourceFile);
    state.checkedAtMs = now;
    if (state.mtimeMs === stats.mtimeMs && state.items.length > 0) {
      return state;
    }

    const parsed = tryReadJson(state.sourceFile);
    const sourceItems = Array.isArray(parsed) ? parsed : toArray(parsed?.items);
    const items = [];
    const byParcel = new Map();
    const byNameDong = new Map();

    sourceItems.forEach((item) => {
      const lat = coerceCoordinate(item?.lat);
      const lng = coerceCoordinate(item?.lng);
      const aptName = String(item?.aptName || item?.name || "").trim();
      if (!aptName || lat == null || lng == null) {
        return;
      }

      const dong = String(item?.dong || "").trim();
      const normalizedItem = {
        aptName,
        dong,
        address: String(item?.address || "").trim(),
        roadAddress: String(item?.roadAddress || "").trim(),
        lat,
        lng,
        bun: String(item?.bun || "").trim(),
        ji: String(item?.ji || "").trim(),
        parcelKey:
          String(item?.parcelKey || "").trim() ||
          buildParcelKey(dong, item?.bun, item?.ji),
        nameKey: normalizeComplexNameKey(aptName),
        dongKey: normalizeText(dong),
      };

      items.push(normalizedItem);
      if (normalizedItem.parcelKey && !byParcel.has(normalizedItem.parcelKey)) {
        byParcel.set(normalizedItem.parcelKey, normalizedItem);
      }
      const nameDongKey = `${normalizedItem.nameKey}|${normalizedItem.dongKey}`;
      if (!byNameDong.has(nameDongKey)) {
        byNameDong.set(nameDongKey, []);
      }
      byNameDong.get(nameDongKey).push(normalizedItem);
    });

    state.items = items;
    state.byParcel = byParcel;
    state.byNameDong = byNameDong;
    state.loadedAt = new Date().toISOString();
    state.mtimeMs = stats.mtimeMs;
    state.message =
      items.length > 0
        ? `loaded ${items.length} apartment coordinates`
        : "apartment coordinate dataset is empty";
  } catch (error) {
    state.loadedAt = new Date().toISOString();
    state.checkedAtMs = now;
    state.mtimeMs = null;
    state.items = [];
    state.byParcel = new Map();
    state.byNameDong = new Map();
    state.message =
      error.code === "ENOENT"
        ? "apartment coordinate dataset file missing"
        : error.message;
  }
  return state;
}

function findApartmentCoordinateMatch(row) {
  const dataset = loadApartmentCoordinateDataset();
  if (!dataset.items.length) {
    return null;
  }

  const parcelKey = buildParcelKey(row?.dong, row?.bun, row?.ji);
  if (parcelKey && dataset.byParcel.has(parcelKey)) {
    return dataset.byParcel.get(parcelKey);
  }

  const nameDongKey = `${normalizeComplexNameKey(row?.apt)}|${normalizeText(row?.dong)}`;
  return dataset.byNameDong.get(nameDongKey)?.[0] || null;
}

function buildApartmentAddressQuery(row, regionCode = "") {
  const sigunguCode = String(row?.sigunguCd || regionCode || "").slice(0, 5);
  const regionFullName = REGION_CODE_NAME_MAP.get(sigunguCode) || "";
  const dong = String(row?.dong || "").trim();
  const bun = String(row?.bun || "").replace(/^0+/, "");
  const ji = String(row?.ji || "").replace(/^0+/, "");
  const addrParts = [regionFullName, dong].filter(Boolean);
  if (bun) {
    addrParts.push(ji ? `${bun}-${ji}` : bun);
  }
  return addrParts.join(" ").trim();
}

async function resolveApartmentCoordinate(row, regionCode = "") {
  const localMatch = findApartmentCoordinateMatch(row);
  if (localMatch) {
    return {
      lng: localMatch.lng,
      lat: localMatch.lat,
    };
  }

  const addrQuery = buildApartmentAddressQuery(row, regionCode);
  if (!addrQuery || !VWORLD_API_KEY) {
    return null;
  }

  try {
    const parcelResult = await fetchVworldParcelForAddress(addrQuery);
    return normalizeCoordinatePoint(parcelResult?.parcel?.point);
  } catch {
    return null;
  }
}

function haversineDistanceKm(lat1, lng1, lat2, lng2) {
  const toRad = (deg) => (deg * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return 6371 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function findNearestStationForApartment(apartmentCoord) {
  const dataset = loadStationDataset();
  if (!dataset.items.length || !apartmentCoord) {
    return null;
  }

  let best = null;
  for (const station of dataset.items) {
    const distanceKm = haversineDistanceKm(
      apartmentCoord.lat,
      apartmentCoord.lng,
      station.lat,
      station.lng,
    );
    if (!best || distanceKm < best.distanceKm) {
      best = {
        station,
        distanceKm,
      };
    }
  }

  if (!best) {
    return null;
  }

  const distanceKm = roundNullable(best.distanceKm, 2);
  const linePrefix =
    best.station.line && !best.station.name.includes(best.station.line)
      ? `${best.station.line} `
      : "";
  return {
    name: `${linePrefix}${best.station.name}`.trim(),
    distanceKm,
    label: `${best.station.name} ${formatStationDistanceLabel(distanceKm)}`.trim(),
  };
}

function buildBoundingBoxAroundPoint(lng, lat, radiusKm = WALKING_DISTANCE_KM) {
  const latDelta = radiusKm / 111.32;
  const lngDelta =
    radiusKm / (111.32 * Math.max(Math.cos((lat * Math.PI) / 180), 0.2));
  return [
    roundNullable(lng - lngDelta, 6),
    roundNullable(lat - latDelta, 6),
    roundNullable(lng + lngDelta, 6),
    roundNullable(lat + latDelta, 6),
  ].join(",");
}

function mapVworldSearchStatus(payload) {
  const status = String(payload?.response?.status || "").trim().toUpperCase();
  if (status === "OK") {
    return "ok";
  }

  const errorCode = normalizeText(payload?.response?.error?.code || "");
  if (
    errorCode === "invalidkey" ||
    errorCode === "incorrectkey" ||
    errorCode === "unavailablekey" ||
    errorCode === "param_required"
  ) {
    return "config_error";
  }
  if (errorCode === "overrequestlimit") {
    return "rate_limited";
  }
  if (status === "NOT_FOUND") {
    return "no_match";
  }
  return "upstream_error";
}

async function fetchNearbyVworldPlaces(query, coord) {
  const point = normalizeCoordinatePoint(coord);
  if (!point) {
    return {
      status: "bad_request",
      items: [],
    };
  }

  const cacheKey = [
    query,
    roundNullable(point.lng, 5),
    roundNullable(point.lat, 5),
  ].join("|");
  const cached = getTimedMapValue(vworldPlaceCache, cacheKey, VWORLD_PLACE_CACHE_TTL_MS);
  if (cached) {
    return cached;
  }

  if (!(VWORLD_API_KEY || VWORLD_DATA_API_KEY)) {
    return setTimedMapValue(vworldPlaceCache, cacheKey, {
      status: "config_error",
      items: [],
    });
  }

  const targetUrl = buildVworldSearchUrl({
    query,
    size: String(VWORLD_NEARBY_PLACE_SIZE),
    page: "1",
    bbox: buildBoundingBoxAroundPoint(point.lng, point.lat),
  });

  try {
    const { body } = await fetchText(targetUrl, {
      Accept: "application/json, */*",
    });
    const parsed = JSON.parse(body);
    const status = mapVworldSearchStatus(parsed);
    const items =
      status === "ok"
        ? extractVworldSearchItems(parsed)
            .map(normalizeVworldSearchItem)
            .filter(Boolean)
        : [];
    return setTimedMapValue(vworldPlaceCache, cacheKey, {
      status: items.length > 0 ? "ok" : status === "ok" ? "no_match" : status,
      items,
      message: parsed?.response?.error?.text || "",
    });
  } catch (error) {
    return setTimedMapValue(vworldPlaceCache, cacheKey, {
      status: "upstream_error",
      items: [],
      message: error.message,
    });
  }
}

function pickNearestPlace(items, coord, predicate = () => true) {
  const point = normalizeCoordinatePoint(coord);
  if (!point) {
    return null;
  }

  let best = null;
  for (const item of toArray(items)) {
    if (!predicate(item)) {
      continue;
    }
    const distanceKm = haversineDistanceKm(point.lat, point.lng, item.lat, item.lng);
    if (!best || distanceKm < best.distanceKm) {
      best = {
        ...item,
        distanceKm,
      };
    }
  }

  if (!best) {
    return null;
  }

  const distanceKm = roundNullable(best.distanceKm, 2);
  return {
    name: best.title,
    distanceKm,
    label: `${best.title} ${formatWalkingDistanceLabel(distanceKm)}`.trim(),
  };
}

function matchElementarySchool(item) {
  const haystack = `${item?.title || ""} ${item?.category || ""}`;
  return haystack.includes("초등학교");
}

function matchPark(item) {
  const haystack = `${item?.title || ""} ${item?.category || ""}`;
  return haystack.includes("공원");
}

async function fetchNearbyElementarySchool(coord) {
  const result = await fetchNearbyVworldPlaces("초등학교", coord);
  if (result.status !== "ok") {
    return {
      status: result.status,
      label: "",
      distanceKm: null,
    };
  }

  const nearest = pickNearestPlace(result.items, coord, matchElementarySchool);
  if (!nearest) {
    return {
      status: "no_match",
      label: "",
      distanceKm: null,
    };
  }

  return {
    status: "ok",
    label: nearest.distanceKm <= WALKING_DISTANCE_KM ? nearest.label : "",
    distanceKm: nearest.distanceKm,
  };
}

async function fetchNearbyPark(coord) {
  const result = await fetchNearbyVworldPlaces("공원", coord);
  if (result.status !== "ok") {
    return {
      status: result.status,
      label: "",
      distanceKm: null,
    };
  }

  const nearest = pickNearestPlace(result.items, coord, matchPark);
  if (!nearest) {
    return {
      status: "no_match",
      label: "",
      distanceKm: null,
    };
  }

  return {
    status: "ok",
    label: nearest.distanceKm <= WALKING_DISTANCE_KM ? nearest.label : "",
    distanceKm: nearest.distanceKm,
  };
}

function buildElevationSamplePoints(coord) {
  const point = normalizeCoordinatePoint(coord);
  if (!point) {
    return [];
  }

  const latDelta = LOCATION_FLATNESS_SAMPLE_DISTANCE_M / 111320;
  const lngDelta =
    LOCATION_FLATNESS_SAMPLE_DISTANCE_M /
    (111320 * Math.max(Math.cos((point.lat * Math.PI) / 180), 0.2));

  return [
    { lng: point.lng, lat: point.lat },
    { lng: point.lng, lat: point.lat + latDelta },
    { lng: point.lng, lat: point.lat - latDelta },
    { lng: point.lng + lngDelta, lat: point.lat },
    { lng: point.lng - lngDelta, lat: point.lat },
  ].map((sample) => ({
    lng: roundNullable(sample.lng, 6),
    lat: roundNullable(sample.lat, 6),
  }));
}

async function fetchElevationProfile(coord) {
  const samples = buildElevationSamplePoints(coord);
  if (!samples.length) {
    return {
      status: "bad_request",
      elevations: [],
    };
  }

  const cacheKey = samples.map((sample) => `${sample.lng}|${sample.lat}`).join(",");
  const cached = getTimedMapValue(
    elevationProfileCache,
    cacheKey,
    ELEVATION_PROFILE_CACHE_TTL_MS,
  );
  if (cached) {
    return cached;
  }

  const targetUrl =
    `https://api.open-meteo.com/v1/elevation?latitude=${samples.map((sample) => sample.lat).join(",")}` +
    `&longitude=${samples.map((sample) => sample.lng).join(",")}`;

  try {
    const { body } = await fetchText(targetUrl, {
      Accept: "application/json, */*",
    });
    const parsed = JSON.parse(body);
    const elevations = toArray(parsed?.elevation)
      .map((value) => Number.parseFloat(String(value)))
      .filter((value) => Number.isFinite(value));
    return setTimedMapValue(elevationProfileCache, cacheKey, {
      status: elevations.length === samples.length ? "ok" : "upstream_error",
      elevations,
    });
  } catch (error) {
    return setTimedMapValue(elevationProfileCache, cacheKey, {
      status: "upstream_error",
      elevations: [],
      message: error.message,
    });
  }
}

async function estimateFlatLandStatus(coord) {
  const profile = await fetchElevationProfile(coord);
  if (profile.status !== "ok" || profile.elevations.length < 5) {
    return {
      flatLandStatus: profile.status === "bad_request" ? "no_data" : "unknown",
      elevationRangeM: null,
    };
  }

  const minElevation = Math.min(...profile.elevations);
  const maxElevation = Math.max(...profile.elevations);
  const elevationRangeM = roundNullable(maxElevation - minElevation, 1);
  return {
    flatLandStatus:
      elevationRangeM != null && elevationRangeM <= LOCATION_FLATNESS_MAX_RANGE_M
        ? "flat"
        : "slope",
    elevationRangeM,
  };
}

async function buildListingLocationInsights(row, regionCode) {
  const coord = await resolveApartmentCoordinate(row, regionCode);
  if (!coord) {
    return {
      nearbyStation: "",
      nearbyStationDistanceKm: null,
      nearbyElementarySchool: "",
      nearbyElementarySchoolDistanceKm: null,
      nearbyElementarySchoolStatus: "unknown",
      nearbyPark: "",
      nearbyParkDistanceKm: null,
      nearbyParkStatus: "unknown",
      flatLandStatus: "unknown",
      flatLandElevationRangeM: null,
    };
  }

  const nearestStation = findNearestStationForApartment(coord);
  const [elementarySchool, park, flatLand] = await Promise.all([
    fetchNearbyElementarySchool(coord),
    fetchNearbyPark(coord),
    estimateFlatLandStatus(coord),
  ]);

  return {
    nearbyStation:
      nearestStation && nearestStation.distanceKm <= WALKING_DISTANCE_KM
        ? nearestStation.label || ""
        : "",
    nearbyStationDistanceKm: nearestStation?.distanceKm ?? null,
    nearbyElementarySchool: elementarySchool.label || "",
    nearbyElementarySchoolDistanceKm: elementarySchool.distanceKm,
    nearbyElementarySchoolStatus: elementarySchool.status,
    nearbyPark: park.label || "",
    nearbyParkDistanceKm: park.distanceKm,
    nearbyParkStatus: park.status,
    flatLandStatus: flatLand.flatLandStatus,
    flatLandElevationRangeM: flatLand.elevationRangeM,
  };
}

function buildListingRowId(seed, regionCode) {
  return `${seed.sigunguCd || regionCode}-${seed.bjdongCd || ""}-${seed.bun || ""}-${seed.ji || ""}-${normalizeText(seed.apt)}`;
}

function buildPendingListingLocationInsights() {
  return {
    nearbyStation: "",
    nearbyStationDistanceKm: null,
    nearbyElementarySchool: "",
    nearbyElementarySchoolDistanceKm: null,
    nearbyElementarySchoolStatus: "loading",
    nearbyPark: "",
    nearbyParkDistanceKm: null,
    nearbyParkStatus: "loading",
    flatLandStatus: "loading",
    flatLandElevationRangeM: null,
    locationInsightsPending: true,
  };
}

function buildResolvedListingLocationInsights(locationInsights = {}) {
  return {
    ...buildPendingListingLocationInsights(),
    ...locationInsights,
    locationInsightsPending: false,
  };
}

function cloneSampleParams(params) {
  return {
    sigunguCd: params?.sigunguCd || "",
    bjdongCd: params?.bjdongCd || "",
    platGbCd: params?.platGbCd || "",
    bun: params?.bun || "",
    ji: params?.ji || "",
  };
}

function cloneEndpointResults(results) {
  return toArray(results).map((result) => ({
    endpointKey: result?.endpointKey || "",
    endpointUrl: result?.endpointUrl || "",
    statusCode:
      result?.statusCode == null ? null : Number.parseInt(String(result.statusCode), 10),
    bodyEmpty: result?.bodyEmpty == null ? null : Boolean(result.bodyEmpty),
    itemCount:
      result?.itemCount == null ? null : Number.parseInt(String(result.itemCount), 10),
    resultCode: result?.resultCode || "",
    resultMsg: result?.resultMsg || "",
  }));
}

function buildBuildingHubMessage(status, sampleParams, detail = "") {
  const suffix = sampleParams
    ? ` (sigunguCd=${sampleParams.sigunguCd}, bjdongCd=${sampleParams.bjdongCd}, bun=${sampleParams.bun}, ji=${sampleParams.ji || "0000"})`
    : "";
  if (status === "config_error") {
    return "DATA_GO_KR_API_KEY is not configured.";
  }
  if (status === "ok") {
    return `건축HUB가 정상 응답했습니다${suffix}`;
  }
  if (status === "empty_body") {
    return `건축HUB 응답 본문이 비어 있습니다${suffix}`;
  }
  if (status === "no_match") {
    return `건축HUB 응답은 있으나 매칭된 item이 없습니다${suffix}`;
  }
  if (status === "upstream_error") {
    return detail || `건축HUB 호출 중 오류가 발생했습니다${suffix}`;
  }
  return detail || "건축HUB 상태를 확인할 수 없습니다.";
}

function updateBuildingHubState(outcome) {
  const wasWorking = buildingHubState.hasWorkingResponse;
  buildingHubState.lastProbeAt = new Date().toISOString();
  buildingHubState.lastStatusCode =
    outcome?.statusCode == null ? null : Number(outcome.statusCode);
  buildingHubState.lastBodyEmpty =
    outcome?.bodyEmpty == null ? null : Boolean(outcome.bodyEmpty);
  buildingHubState.lastSampleParams = outcome?.sampleParams
    ? cloneSampleParams(outcome.sampleParams)
    : null;
  buildingHubState.lastMessage = outcome?.message || null;
  buildingHubState.lastProbeStatus = outcome?.status || null;
  buildingHubState.lastEndpointResults = cloneEndpointResults(
    outcome?.endpointResults,
  );

  if (outcome?.status === "ok") {
    buildingHubState.hasWorkingResponse = true;
    buildingHubState.emptyRuns = 0;
    buildingHubState.disabledUntil = 0;
    if (!wasWorking) {
      listingCache.clear();
      buildingSnapshotCache.clear();
    }
    return;
  }

  if (outcome?.status === "empty_body") {
    if (!buildingHubState.hasWorkingResponse) {
      buildingHubState.emptyRuns += 1;
      if (buildingHubState.emptyRuns >= BUILDING_HUB_EMPTY_THRESHOLD) {
        buildingHubState.disabledUntil = Date.now() + BUILDING_HUB_DISABLED_TTL_MS;
      }
    }
    return;
  }

  if (outcome?.status !== "ok" && !buildingHubState.hasWorkingResponse) {
    buildingHubState.disabledUntil = 0;
  }
}

function getCurrentBuildingHubStatus() {
  if (!API_KEY) {
    return "config_error";
  }
  if (buildingHubState.lastProbeStatus) {
    return buildingHubState.lastProbeStatus;
  }
  if (buildingHubState.hasWorkingResponse) {
    return "ok";
  }
  return "upstream_error";
}

function buildListingSeedKey(row) {
  const parcelKey = [
    normalizeText(row.apt),
    normalizeText(row.dong),
    row.sigunguCd || "",
    row.bjdongCd || "",
    normalizeParcelPart(row.bun),
    normalizeParcelPart(row.ji) || "0",
  ].join("|");
  if (parcelKey.replace(/\|/g, "")) {
    return parcelKey;
  }

  return [
    normalizeText(row.apt),
    normalizeText(row.dong),
    row.sigunguCd || "",
    row.bjdongCd || "",
    String(Number.parseInt(row.buildYear || 0, 10) || ""),
  ].join("|");
}

function buildListingEntityKey(seed, snapshot) {
  const kaptCode = String(snapshot?.matchedKaptCode || "").trim();
  if (kaptCode) {
    return `kapt:${kaptCode}`;
  }

  const parcelKey = [
    seed?.sigunguCd || "",
    seed?.bjdongCd || "",
    normalizeParcelPart(seed?.bun),
    normalizeParcelPart(seed?.ji) || "0",
  ].join("|");
  if (parcelKey.replace(/\|/g, "")) {
    return `parcel:${parcelKey}`;
  }

  return [
    "name",
    normalizeText(seed?.apt),
    normalizeText(seed?.dong),
    String(Number.parseInt(seed?.buildYear || 0, 10) || ""),
  ].join(":");
}

function padParcelCode(value) {
  const digits = String(value || "").replace(/\D/g, "");
  return digits ? digits.padStart(4, "0") : "0000";
}

function buildParcelVariants(seed) {
  const variants = [];
  const seen = new Set();
  const bunDigits = String(seed?.bun || "").replace(/\D/g, "");
  const jiDigits = String(seed?.ji || "").replace(/\D/g, "");
  const candidates = [
    { bun: padParcelCode(bunDigits), ji: padParcelCode(jiDigits) },
    { bun: bunDigits, ji: jiDigits },
    { bun: padParcelCode(bunDigits), ji: "" },
  ];

  for (const candidate of candidates) {
    if (!candidate.bun) {
      continue;
    }
    const key = `${candidate.bun}:${candidate.ji}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    variants.push(candidate);
  }

  return variants.length > 0 ? variants : [{ bun: "", ji: "" }];
}

async function fetchDataGoResult(endpointKey, baseUrl, params) {
  const targetUrl = buildDataGoKrUrl(baseUrl, {
    ...params,
    _type: "json",
  });

  const { statusCode, headers, body } = await fetchText(targetUrl, {
    Accept: "application/json, application/xml, text/xml;q=0.9, */*;q=0.8",
  });
  const bodyText = String(body || "");
  const meta = parseDataGoResponseMeta(bodyText);
  const items = parseDataGoResponseItems(bodyText);
  return {
    endpointKey,
    endpointUrl: baseUrl,
    statusCode,
    contentType: String(headers?.["content-type"] || ""),
    bodyEmpty: bodyText.trim() === "",
    items,
    itemCount: items.length,
    resultCode: meta.resultCode,
    resultMsg: meta.resultMsg,
  };
}

async function fetchBuildingHubResult(endpointKey, baseUrl, params) {
  return fetchDataGoResult(endpointKey, baseUrl, params);
}

// ── 서울 열린데이터 광장 건축물대장 총괄표제부 API ──
// 서비스명: vBigDjrRecapTitle (OA-22423)
// http://openapi.seoul.go.kr:8088/{KEY}/json/vBigDjrRecapTitle/{START}/{END}
// 전체 ~19,861건 → 서버 시작 시 벌크 로드 후 메모리 인덱싱
const seoulBuildingIndex = new Map(); // key: "본번|부번|법정동명" → item
let seoulBuildingLoaded = false;
let seoulBuildingLoading = false;
let seoulBuildingLoadPromise = null;

async function loadSeoulBuildingData() {
  if (!SEOUL_BUILDING_API_KEY) return;
  if (seoulBuildingLoaded || seoulBuildingLoading) return seoulBuildingLoadPromise;

  seoulBuildingLoading = true;
  seoulBuildingLoadPromise = (async () => {
    const PAGE_SIZE = 1000;
    let start = 1;
    let totalCount = 0;
    let loaded = 0;

    try {
      // 먼저 총 건수 파악
      const firstUrl = `http://openapi.seoul.go.kr:8088/${encodeURIComponent(SEOUL_BUILDING_API_KEY)}/json/vBigDjrRecapTitle/1/1/`;
      const firstResp = await fetchText(firstUrl, { Accept: "application/json" });
      const firstParsed = JSON.parse(String(firstResp.body || "{}"));
      totalCount = firstParsed?.vBigDjrRecapTitle?.list_total_count || 0;

      if (totalCount === 0) {
        console.log("[seoul-building] no records found");
        seoulBuildingLoading = false;
        return;
      }

      console.log(`[seoul-building] loading ${totalCount} records from vBigDjrRecapTitle...`);

      // 병렬로 페이지 로드 (4개씩)
      const totalPages = Math.ceil(totalCount / PAGE_SIZE);
      const pageGroups = [];
      for (let i = 0; i < totalPages; i += 4) {
        pageGroups.push(
          Array.from({ length: Math.min(4, totalPages - i) }, (_, j) => i + j)
        );
      }

      for (const group of pageGroups) {
        const results = await Promise.allSettled(
          group.map(async (pageIdx) => {
            const s = pageIdx * PAGE_SIZE + 1;
            const e = Math.min((pageIdx + 1) * PAGE_SIZE, totalCount);
            const url = `http://openapi.seoul.go.kr:8088/${encodeURIComponent(SEOUL_BUILDING_API_KEY)}/json/vBigDjrRecapTitle/${s}/${e}/`;
            const resp = await fetchText(url, { Accept: "application/json" });
            return JSON.parse(String(resp.body || "{}"));
          })
        );

        for (const result of results) {
          if (result.status !== "fulfilled") continue;
          const rows = result.value?.vBigDjrRecapTitle?.row;
          if (!Array.isArray(rows)) continue;

          for (const row of rows) {
            const mnLotno = String(row.MN_LOTNO || "").replace(/^0+/, "") || "0";
            const subLotno = String(row.SUB_LOTNO || "").replace(/^0+/, "") || "0";
            const dongNm = String(row.STDG_CD_NM || "").trim();
            const sggNm = String(row.SGG_CD_NM || "").trim();

            // 여러 키로 인덱싱 (다양한 조회 방식 지원)
            const keys = [
              `${dongNm}|${mnLotno}|${subLotno}`,
              `${dongNm}|${mnLotno}|0`,
            ];
            // 중복 키면 더 완전한 데이터(세대수 있는 것) 우선
            for (const key of keys) {
              const existing = seoulBuildingIndex.get(key);
              if (!existing || (row.HH_CNT && !existing.HH_CNT)) {
                seoulBuildingIndex.set(key, row);
              }
            }
            // SGG별 인덱스 (시군구명+법정동+본번+부번)
            const sggKey = `${sggNm}|${dongNm}|${mnLotno}|${subLotno}`;
            if (!seoulBuildingIndex.has(sggKey) || (row.HH_CNT && !seoulBuildingIndex.get(sggKey).HH_CNT)) {
              seoulBuildingIndex.set(sggKey, row);
            }
            loaded++;
          }
        }
      }

      seoulBuildingLoaded = true;
      console.log(`[seoul-building] indexed ${loaded} records (${seoulBuildingIndex.size} keys)`);
    } catch (err) {
      console.error(`[seoul-building] bulk load error: ${err.message}`);
    } finally {
      seoulBuildingLoading = false;
    }
  })();

  return seoulBuildingLoadPromise;
}

function lookupSeoulBuilding(dongName, bun, ji) {
  if (!seoulBuildingLoaded || seoulBuildingIndex.size === 0) return null;
  const mnLotno = String(bun || "").replace(/^0+/, "") || "0";
  const subLotno = String(ji || "").replace(/^0+/, "") || "0";
  const dong = String(dongName || "").trim();

  // 정확한 매칭
  const exact = seoulBuildingIndex.get(`${dong}|${mnLotno}|${subLotno}`);
  if (exact) return parseSeoulBuildingRow(exact);

  // 부번 0으로 폴백
  if (subLotno !== "0") {
    const fallback = seoulBuildingIndex.get(`${dong}|${mnLotno}|0`);
    if (fallback) return parseSeoulBuildingRow(fallback);
  }

  return null;
}

function parseSeoulBuildingRow(row) {
  if (!row) return null;
  return {
    households: parseNumber(row.HH_CNT) || null,
    siteArea: parseFloat(row.SIAR) || null,
    grossFloorArea: parseFloat(row.GFA) || null,
    currentFar: parseFloat(row.FART) || null,
    completionYear: extractYearFromDateStr(row.USE_APRV_YMD) || null,
    completionYearMonth: formatSeoulCompletionDate(row.USE_APRV_YMD) || null,
    mainUsage: row.MN_USG_CD_NM || null,
    etcUsage: row.ETC_USG_CN || null,
    source: "seoul_opendata",
  };
}

function extractYearFromDateStr(dateStr) {
  if (!dateStr) return null;
  const s = String(dateStr).replace(/\D/g, "");
  if (s.length >= 4) return Number.parseInt(s.slice(0, 4), 10) || null;
  return null;
}

function formatSeoulCompletionDate(dateStr) {
  if (!dateStr) return null;
  const s = String(dateStr).replace(/\D/g, "");
  if (s.length >= 6) {
    const y = s.slice(0, 4);
    const m = s.slice(4, 6);
    return `${y}.${m}`;
  }
  if (s.length >= 4) return s.slice(0, 4);
  return null;
}

function parseNumber(val) {
  if (val == null) return null;
  const n = Number(val);
  return Number.isFinite(n) ? n : null;
}

function hasDataGoResponseError(result) {
  const code = String(result?.resultCode || "").trim();
  return Boolean(code && !["00", "0", "INFO-000"].includes(code));
}

function summarizeBuildingHubResults(results) {
  return cloneEndpointResults(
    toArray(results).map((result) => ({
      endpointKey: result?.endpointKey,
      endpointUrl: result?.endpointUrl,
      statusCode: result?.statusCode,
      bodyEmpty: result?.bodyEmpty,
      itemCount: toArray(result?.items).length,
      resultCode: result?.resultCode,
      resultMsg: result?.resultMsg,
    })),
  );
}

function extractZoneFromJijigu(items) {
  const candidates = items
    .map((item) => ({
      name: pickFirstValue(item, [
        "regionNm",
        "jijiguNm",
        "jijiguCdNm",
        "jijiguKorNm",
      ]),
      category: pickFirstValue(item, [
        "jijiguGbCdNm",
        "jijiguGbCd",
        "regionGbCdNm",
        "regionGbCd",
      ]),
      representative: pickFirstValue(item, [
        "mainAt",
        "reprYn",
        "reprsAt",
        "mainYn",
      ]),
    }))
    .filter((item) => item.name);

  const zoneOnly = candidates.filter((item) => {
    const category = normalizeText(item.category);
    return category === "1" || category.includes("용도지역");
  });
  const preferred = zoneOnly.find((item) =>
    ["1", "y", "yes", "true"].includes(normalizeText(item.representative)),
  );
  return (preferred || zoneOnly[0] || candidates[0] || {}).name || "";
}

function deriveListingMetrics(seed, buildingInfo, regionCode) {
  const completionYear =
    buildingInfo.completionYear || Number.parseInt(seed.buildYear || 0, 10) || null;
  const rawFar = roundNullable(buildingInfo.currentFar, 2);
  const currentFar = rawFar != null && rawFar > 0 ? rawFar : null;
  const resolvedSiteArea = resolveListingSiteArea(buildingInfo);
  const siteArea = resolvedSiteArea.siteArea;
  const rawHouseholds =
    buildingInfo.households == null
      ? null
      : Number.parseInt(buildingInfo.households, 10);
  const households = rawHouseholds != null && rawHouseholds > 0 ? rawHouseholds : null;
  const landPerHousehold = computeListingLandPerHousehold(siteArea, households);
  const zoning = buildingInfo.zoning || null;
  const legalFarLimit = resolveLegalFarLimit(regionCode, zoning);

  // 비주거 용도지역의 주거 허용 비율 적용 (준공업 50%, 상업 30~60% 등)
  const residentialFarRatio = getResidentialFarRatio(zoning);
  const effectiveFarLimit =
    legalFarLimit != null ? Math.round(legalFarLimit * residentialFarRatio) : null;

  // Fix 3: 비주거 용적률 추정치 과대계산 방지 — effectiveFarLimit 반영
  const estimatedExpectedUnits =
    siteArea != null && effectiveFarLimit != null
      ? Math.floor((siteArea * effectiveFarLimit / 100) / REBUILD_UNIT_AREA_SQM)
      : siteArea != null
        ? Math.floor(siteArea / REBUILD_UNIT_AREA_SQM)
        : null;
  // 예상 신축 세대: 비주거 용도지역은 effectiveFarLimit(주거 허용분만) 사용
  const confirmedExpectedUnits =
    households != null &&
    currentFar != null &&
    currentFar > 0 &&
    effectiveFarLimit != null
      ? Math.floor((households * effectiveFarLimit) / currentFar)
      : null;
  const expectedUnits =
    confirmedExpectedUnits != null ? confirmedExpectedUnits : estimatedExpectedUnits;

  // 재건축 연한 체크 (준공 후 30년 이상)
  const currentYear = new Date().getFullYear();
  const buildingAge = completionYear != null ? currentYear - completionYear : null;
  const reconstructionEligible =
    buildingAge != null ? buildingAge >= REBUILD_MIN_AGE_YEARS : null;

  // Fix 2: generic 용도지역("일반주거지역" 등 미세분)은 신뢰할 수 없으므로 계산 제외
  // legalFarLimit=null인 경우도 계산 불가 (유령 계산 방지)
  const isGenericZoning = isGenericZoneName(zoning);
  const hasReliableFarBasis = legalFarLimit != null && !isGenericZoning;

  const generalSaleUnits =
    hasReliableFarBasis && expectedUnits != null && households != null && households > 0
      ? expectedUnits - households
      : null;

  const saleRatio =
    generalSaleUnits != null && households != null && households > 0
      ? generalSaleUnits / households
      : null;

  // Fix 1: 연한 미달(30년 미만)이면 명시적 "NO" + feasibilityReason으로 사유 구분
  const feasibility =
    saleRatio == null
      ? null
      : reconstructionEligible === false
        ? "NO"
        : generalSaleUnits > 0 && saleRatio >= FEASIBILITY_RATIO_THRESHOLD
          ? "YES"
          : "NO";

  const feasibilityReason =
    saleRatio == null
      ? null
      : reconstructionEligible === false
        ? "age_ineligible"
        : generalSaleUnits < 0
          ? "far_inversion"
          : generalSaleUnits > 0 && saleRatio >= FEASIBILITY_RATIO_THRESHOLD
            ? "positive"
            : "low_ratio";

  return {
    completionYear,
    zoning,
    legalFarLimit,
    effectiveFarLimit,
    currentFar,
    siteArea,
    rawSiteArea: resolvedSiteArea.rawSiteArea,
    siteAreaCorrection: resolvedSiteArea.siteAreaCorrection,
    siteAreaValidationStatus: resolvedSiteArea.siteAreaValidationStatus,
    households,
    landPerHousehold,
    estimatedExpectedUnits,
    confirmedExpectedUnits,
    expectedUnits,
    generalSaleUnits,
    feasibility,
    feasibilityReason: feasibilityReason || null,
    isGenericZoning: isGenericZoning || false,
    buildingAge,
    reconstructionEligible,
    residentialFarRatio,
  };
}

function getListingMetricGuardReason(buildingInfo, metrics, analysisMode) {
  if (metrics.siteAreaValidationStatus === "unverified") {
    return "site_area_unverified";
  }

  const usesFallback =
    analysisMode === "estimated" || buildingInfo?.parcelSource === "vworld_pnu";
  if (!usesFallback) {
    return null;
  }

  if (
    metrics.landPerHousehold != null &&
    !isReasonableListingLandPerHousehold(metrics.landPerHousehold)
  ) {
    return "suspicious_metrics";
  }

  return null;
}

function applyListingMetricGuard(metrics, analysisReason) {
  if (!["suspicious_metrics", "site_area_unverified"].includes(analysisReason)) {
    return metrics;
  }

  return {
    ...metrics,
    siteArea: null,
    landPerHousehold: null,
    estimatedExpectedUnits: null,
    confirmedExpectedUnits: null,
    expectedUnits: null,
    generalSaleUnits: null,
    feasibility: null,
  };
}

function classifyListingAnalysis(buildingInfo, metrics) {
  const estimatedFieldsReady =
    metrics.siteArea != null &&
    metrics.households != null &&
    metrics.landPerHousehold != null &&
    metrics.estimatedExpectedUnits != null &&
    metrics.generalSaleUnits != null &&
    metrics.feasibility != null;
  const officialFieldsReady =
    estimatedFieldsReady &&
    Boolean(metrics.zoning) &&
    !metrics.isGenericZoning &&
    metrics.currentFar != null &&
    metrics.legalFarLimit != null &&
    metrics.confirmedExpectedUnits != null;
  const analysisMode = officialFieldsReady
    ? "confirmed"
    : estimatedFieldsReady
      ? "estimated"
      : "unavailable";
  const guardReason = getListingMetricGuardReason(
    buildingInfo,
    metrics,
    analysisMode,
  );
  if (guardReason) {
    return {
      analysisMode: "unavailable",
      analysisReady: false,
      analysisReason: guardReason,
      calculationBasis: null,
    };
  }
  const analysisReady = analysisMode !== "unavailable";
  const analysisReason =
    analysisMode === "confirmed"
      ? "ok"
      : analysisMode === "estimated"
        ? "estimated"
        : ["config_error", "empty_body", "upstream_error"].includes(
            buildingInfo.probeStatus,
          )
          ? "building_hub_unavailable"
          : ["forbidden", "unauthorized"].includes(buildingInfo.kaptStatus)
            ? "kapt_forbidden"
            : ["config_error", "forbidden", "rate_limited", "upstream_error"].includes(
                buildingInfo.zoningStatus,
              )
              ? "land_use_unavailable"
            : "insufficient_fields";
  const calculationBasis =
    analysisMode === "confirmed"
      ? "official_far_ratio"
      : analysisMode === "estimated"
        ? "site_area_estimate"
        : null;

  return {
    analysisMode,
    analysisReady,
    analysisReason,
    calculationBasis,
  };
}

function buildListingRow(seed, buildingInfo, regionCode, locationInsights = {}) {
  const metrics = deriveListingMetrics(seed, buildingInfo, regionCode);
  const analysis = classifyListingAnalysis(buildingInfo, metrics);
  const displayMetrics = applyListingMetricGuard(metrics, analysis.analysisReason);

  return {
    id: buildListingRowId(seed, regionCode),
    apt: seed.apt || "N/A",
    ...displayMetrics,
    tradeCount: seed.tradeCount || 0,
    sourceReady: buildingInfo.sourceReady,
    ...locationInsights,
    ...analysis,
    kaptStatus: buildingInfo.kaptStatus || null,
    parcelSource: buildingInfo.parcelSource || null,
    vworldPnu: buildingInfo.vworldPnu || null,
    zoningSource: buildingInfo.zoningSource || null,
    zoningStatus: buildingInfo.zoningStatus || null,
  };
}

function chooseBetterListingGroupBase(currentItem, nextItem) {
  if (!currentItem) {
    return nextItem;
  }

  const completenessDiff = compareBuildingSnapshotCompleteness(
    nextItem?.snapshot,
    currentItem?.snapshot,
  );
  if (completenessDiff !== 0) {
    return completenessDiff > 0 ? nextItem : currentItem;
  }

  const nextTrades = Number(nextItem?.seed?.tradeCount || 0);
  const currentTrades = Number(currentItem?.seed?.tradeCount || 0);
  if (nextTrades !== currentTrades) {
    return nextTrades > currentTrades ? nextItem : currentItem;
  }

  const nextName = String(nextItem?.seed?.apt || "");
  const currentName = String(currentItem?.seed?.apt || "");
  if (nextName.length !== currentName.length) {
    return nextName.length > currentName.length ? nextItem : currentItem;
  }

  return currentItem;
}

function collapseListingEntities(items) {
  const grouped = new Map();

  for (const item of toArray(items)) {
    const key = buildListingEntityKey(item?.seed, item?.snapshot);
    const current = grouped.get(key);

    if (!current) {
      grouped.set(key, {
        seed: {
          ...item.seed,
          tradeCount: Number(item?.seed?.tradeCount || 0),
        },
        snapshot: item.snapshot,
      });
      continue;
    }

    const preferredBase = chooseBetterListingGroupBase(current, item);
    const supplement = preferredBase === current ? item : current;
    const mergedSnapshot = mergeBuildingSnapshots(
      preferredBase.snapshot,
      supplement.snapshot,
      {
        parcelSource:
          preferredBase.snapshot?.parcelSource || supplement.snapshot?.parcelSource || null,
        vworldPnu:
          preferredBase.snapshot?.vworldPnu || supplement.snapshot?.vworldPnu || null,
        vworldAddress:
          preferredBase.snapshot?.vworldAddress || supplement.snapshot?.vworldAddress || null,
      },
    );

    grouped.set(key, {
      seed: {
        ...preferredBase.seed,
        tradeCount:
          Number(current.seed?.tradeCount || 0) + Number(item.seed?.tradeCount || 0),
      },
      snapshot: mergedSnapshot,
    });
  }

  return [...grouped.values()];
}

function summarizeListingRows(rows, seeds, directory, currentStatus) {
  const stats = rows.reduce(
    (acc, row) => {
      if (row.sourceReady) acc.buildingMatchedCount += 1;
      if (row.analysisMode === "confirmed") acc.confirmedCount += 1;
      if (row.analysisMode === "estimated") acc.estimatedCount += 1;
      if (row.analysisMode === "unavailable") acc.unavailableCount += 1;
      if (row.analysisMode === "confirmed" && row.feasibility === "YES") {
        acc.confirmedYesCount += 1;
      }
      if (row.analysisMode === "estimated" && row.feasibility === "YES") {
        acc.estimatedYesCount += 1;
      }
      if (
        !row.zoning &&
        ["config_error", "forbidden", "rate_limited", "upstream_error"].includes(
          row.zoningStatus,
        )
      ) {
        acc.landUseBlockedCount += 1;
      }
      if (row.parcelSource === "vworld_pnu") {
        acc.pnuFallbackCount += 1;
      }
      if (row.analysisReason === "kapt_forbidden") {
        acc.kaptBlockedCount += 1;
      }
      if (row.analysisReason === "suspicious_metrics") {
        acc.guardedCount += 1;
      }
      if (row.analysisReason === "site_area_unverified") {
        acc.siteAreaUnverifiedCount += 1;
      }
      if (row.siteAreaCorrection === "gross_floor_area_divided_by_far") {
        acc.siteAreaCorrectedCount += 1;
      }
      if (row.kaptStatus === "ok") {
        acc.kaptMatchedCount += 1;
      }
      return acc;
    },
    {
      buildingMatchedCount: 0,
      confirmedCount: 0,
      confirmedYesCount: 0,
      estimatedCount: 0,
      estimatedYesCount: 0,
      unavailableCount: 0,
      landUseBlockedCount: 0,
      pnuFallbackCount: 0,
      guardedCount: 0,
      siteAreaUnverifiedCount: 0,
      siteAreaCorrectedCount: 0,
      kaptBlockedCount: 0,
      kaptMatchedCount: 0,
    },
  );
  const kaptStatus =
    stats.kaptBlockedCount > 0
      ? "forbidden"
      : directory?.status || "no_match";
  const hasReadySource = rows.some((row) => row.sourceReady);

  return {
    seedCount: seeds.length,
    entityCount: rows.length,
    buildingMatchedCount: stats.buildingMatchedCount,
    confirmedCount: stats.confirmedCount,
    confirmedYesCount: stats.confirmedYesCount,
    feasibilityYesCount: stats.confirmedYesCount,
    estimatedCount: stats.estimatedCount,
    estimatedYesCount: stats.estimatedYesCount,
    unavailableCount: stats.unavailableCount,
    landUseBlockedCount: stats.landUseBlockedCount,
    pnuFallbackCount: stats.pnuFallbackCount,
    guardedCount: stats.guardedCount,
    siteAreaUnverifiedCount: stats.siteAreaUnverifiedCount,
    siteAreaCorrectedCount: stats.siteAreaCorrectedCount,
    buildingHubStatus: hasReadySource ? "ok" : currentStatus,
    buildingHubAvailable: hasReadySource || currentStatus === "ok",
    buildingHubEndpoints: cloneEndpointResults(buildingHubState.lastEndpointResults),
    kaptStatus,
    kaptBlockedCount: stats.kaptBlockedCount,
    kaptMatchedCount: stats.kaptMatchedCount,
    kaptMessage: buildKaptMessage(kaptStatus, {
      blockedCount: stats.kaptBlockedCount,
      detail: directory?.message,
    }),
  };
}

async function asyncMapLimit(items, limit, mapper) {
  const results = new Array(items.length);
  let currentIndex = 0;

  async function worker() {
    while (currentIndex < items.length) {
      const index = currentIndex;
      currentIndex += 1;
      results[index] = await mapper(items[index], index);
    }
  }

  const workerCount = Math.max(1, Math.min(limit, items.length || 1));
  await Promise.all(Array.from({ length: workerCount }, () => worker()));
  return results;
}

function buildChildEnv() {
  const env = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (typeof value === "string") {
      env[key] = value;
    }
  }

  for (const [key, value] of Object.entries(ENV_FILE)) {
    if (typeof value === "string" && value) {
      env[key] = value;
    }
  }

  if (API_KEY) {
    env.DATA_GO_KR_API_KEY = API_KEY;
  }

  env.PYTHONUTF8 = "1";
  return env;
}

async function disconnectMcp() {
  const transport = mcpState.transport;
  mcpState.connected = false;
  mcpState.client = null;
  mcpState.transport = null;
  mcpState.toolNames = new Set();

  if (transport) {
    try {
      await transport.close();
    } catch {
      // Ignore close errors while resetting state.
    }
  }
}

async function ensureMcpConnected() {
  if (mcpState.connected && mcpState.client) {
    return;
  }

  if (mcpState.connecting) {
    return mcpState.connecting;
  }

  mcpState.connecting = (async () => {
    await disconnectMcp();

    const transport = new StdioClientTransport({
      command: ENV_FILE.PYTHON_COMMAND || process.env.PYTHON_COMMAND || "python",
      args: ["-m", "real_estate.mcp_server.server"],
      cwd: MCP_SERVER_CWD,
      env: buildChildEnv(),
      stderr: "pipe",
    });

    if (transport.stderr) {
      transport.stderr.on("data", (chunk) => {
        process.stderr.write(`[${SOURCE}] ${chunk}`);
      });
    }

    transport.onclose = () => {
      if (mcpState.transport === transport) {
        mcpState.connected = false;
        mcpState.client = null;
        mcpState.transport = null;
        mcpState.toolNames = new Set();
      }
    };

    transport.onerror = (error) => {
      mcpState.lastError = error;
      console.error(`[${SOURCE}] transport error: ${error.message}`);
    };

    const client = new Client(
      { name: "apt-dashboard", version: "1.0.0" },
      { capabilities: {} },
    );

    try {
      await client.connect(transport);
      const { tools } = await client.listTools();

      mcpState.client = client;
      mcpState.transport = transport;
      mcpState.toolNames = new Set(tools.map((tool) => tool.name));
      mcpState.connected = true;
      mcpState.lastError = null;
    } catch (error) {
      mcpState.lastError = error;
      try {
        await transport.close();
      } catch {
        // Ignore close errors during connection failure cleanup.
      }
      throw error;
    }
  })().finally(() => {
    mcpState.connecting = null;
  });

  return mcpState.connecting;
}

function extractPayloadFromToolResult(result) {
  if (
    result &&
    result.structuredContent &&
    typeof result.structuredContent === "object" &&
    !Array.isArray(result.structuredContent)
  ) {
    return result.structuredContent;
  }

  for (const block of result?.content || []) {
    if (block.type === "text" && typeof block.text === "string") {
      try {
        return JSON.parse(block.text);
      } catch {
        // Ignore non-JSON blocks and continue.
      }
    }
  }

  return null;
}

function extractToolResultMessage(result) {
  const textBlocks = (result?.content || [])
    .filter((block) => block.type === "text" && typeof block.text === "string")
    .map((block) => block.text.trim())
    .filter(Boolean);
  return textBlocks.join("\n");
}

async function callMcpTool(name, args) {
  await ensureMcpConnected();

  if (!mcpState.client) {
    throw new Error("MCP client is not connected.");
  }

  if (!mcpState.toolNames.has(name)) {
    throw new Error(`MCP tool unavailable: ${name}`);
  }

  const result = await mcpState.client.callTool({
    name,
    arguments: args,
  });

  const payload = extractPayloadFromToolResult(result);
  if (payload) {
    return payload;
  }

  const message = extractToolResultMessage(result) || "Tool returned no payload.";
  if (result?.isError) {
    return { error: "tool_error", message };
  }

  throw new Error(message);
}

function normalizeTradeName(rawKind, item) {
  if (rawKind === "apt") {
    return item.apt_name || "N/A";
  }
  if (rawKind === "offi") {
    return item.unit_name || "N/A";
  }
  if (rawKind === "villa") {
    return item.unit_name || item.house_type || "N/A";
  }
  if (rawKind === "house") {
    return item.unit_name || item.house_type || "N/A";
  }
  if (rawKind === "comm") {
    return item.building_use || item.building_type || "N/A";
  }
  return "N/A";
}

function normalizeTradeItems(rawKind, items) {
  return (items || []).map((item) => ({
    apt: normalizeTradeName(rawKind, item),
    dong: item.dong || "",
    gu: item.dong || "",
    price: Number(item.price_10k || 0),
    area: Number(item.area_sqm ?? item.building_ar ?? 0),
    floor: Number(item.floor || 0),
    date: item.trade_date || "",
    buildYear: item.build_year || "",
    dealType: item.deal_type || "",
    sigunguCd: "",
    bjdongCd: "",
    bun: "",
    ji: "",
    households: null,
    completionYearMonth: formatCompletionYearMonth("", item.build_year || ""),
    nearbyStation: "",
    nearbyStationDistanceKm: null,
  }));
}

function normalizeRentName(rawKind, item) {
  if (rawKind === "apt") {
    return item.unit_name || "N/A";
  }
  if (rawKind === "offi") {
    return item.unit_name || "N/A";
  }
  if (rawKind === "villa") {
    return item.unit_name || item.house_type || "N/A";
  }
  if (rawKind === "house") {
    return item.unit_name || item.house_type || "N/A";
  }
  return item.unit_name || "N/A";
}

function normalizeRentItems(rawKind, items) {
  return (items || []).map((item) => {
    const monthly = Number(item.monthly_rent_10k || 0);
    return {
      apt: normalizeRentName(rawKind, item),
      dong: item.dong || "",
      gu: item.dong || "",
      deposit: Number(item.deposit_10k || 0),
      monthly,
      area: Number(item.area_sqm || 0),
      floor: Number(item.floor || 0),
      date: item.trade_date || "",
      type: monthly > 0 ? "월세" : "전세",
      buildYear: item.build_year || "",
    };
  });
}

function buildTradeMatchKey(rawKind, row) {
  return [
    rawKind,
    normalizeText(row.apt),
    normalizeText(row.dong),
    normalizeText(row.date),
    normalizeNumber(row.price),
    normalizeNumber(row.area),
    normalizeNumber(row.floor),
  ].join("|");
}

function parseRawTradeRows(xmlText, rawKind) {
  return extractXmlItems(xmlText)
    .map((itemXml) => {
      const cdealType =
        getXmlTagValue(itemXml, "cdealType") ||
        getXmlTagValue(itemXml, "cdealtype");
      if (cdealType === "O") {
        return null;
      }

      const price = parseAmount(getXmlTagValue(itemXml, "dealAmount"));
      if (!price) {
        return null;
      }

      const jibun = splitJibun(
        getXmlTagValue(itemXml, "jibun"),
        getXmlTagValue(itemXml, "bonbun") || getXmlTagValue(itemXml, "bun"),
        getXmlTagValue(itemXml, "bubun") || getXmlTagValue(itemXml, "ji"),
      );

      const baseRow = {
        dong: getXmlTagValue(itemXml, "umdNm"),
        date: makeDate(
          getXmlTagValue(itemXml, "dealYear"),
          getXmlTagValue(itemXml, "dealMonth"),
          getXmlTagValue(itemXml, "dealDay"),
        ),
        price,
        sigunguCd:
          getXmlTagValue(itemXml, "sggCd") ||
          getXmlTagValue(itemXml, "sigunguCd"),
        bun: jibun.bun,
        ji: jibun.ji,
        buildYear:
          parseIntSafe(getXmlTagValue(itemXml, "buildYear")) || "",
      };

      baseRow.bjdongCd =
        getXmlTagValue(itemXml, "umdCd") ||
        getXmlTagValue(itemXml, "bjdongCd") ||
        lookupBjdongCode(baseRow.sigunguCd, baseRow.dong);

      if (rawKind === "apt") {
        return {
          ...baseRow,
          apt: getXmlTagValue(itemXml, "aptNm") || "N/A",
          area: parseFloatSafe(getXmlTagValue(itemXml, "excluUseAr")),
          floor: parseIntSafe(getXmlTagValue(itemXml, "floor")),
        };
      }

      if (rawKind === "offi") {
        return {
          ...baseRow,
          apt: getXmlTagValue(itemXml, "offiNm") || "N/A",
          area: parseFloatSafe(getXmlTagValue(itemXml, "excluUseAr")),
          floor: parseIntSafe(getXmlTagValue(itemXml, "floor")),
        };
      }

      if (rawKind === "villa") {
        return {
          ...baseRow,
          apt:
            getXmlTagValue(itemXml, "mhouseNm") ||
            getXmlTagValue(itemXml, "houseType") ||
            "N/A",
          area: parseFloatSafe(getXmlTagValue(itemXml, "excluUseAr")),
          floor: parseIntSafe(getXmlTagValue(itemXml, "floor")),
        };
      }

      if (rawKind === "house") {
        return {
          ...baseRow,
          apt: getXmlTagValue(itemXml, "houseType") || "N/A",
          area: parseFloatSafe(getXmlTagValue(itemXml, "totalFloorAr")),
          floor: 0,
        };
      }

      if (rawKind === "comm") {
        return {
          ...baseRow,
          apt:
            getXmlTagValue(itemXml, "buildingUse") ||
            getXmlTagValue(itemXml, "buildingType") ||
            "N/A",
          area: parseFloatSafe(getXmlTagValue(itemXml, "buildingAr")),
          floor: parseIntSafe(getXmlTagValue(itemXml, "floor")),
        };
      }

      return null;
    })
    .filter(Boolean);
}

function mergeRawTradeMetadata(mcpRows, rawRows, rawKind) {
  const rawMap = new Map();

  for (const rawRow of rawRows) {
    const key = buildTradeMatchKey(rawKind, rawRow);
    if (!rawMap.has(key)) {
      rawMap.set(key, []);
    }
    rawMap.get(key).push(rawRow);
  }

  return mcpRows.map((row) => {
    const key = buildTradeMatchKey(rawKind, row);
    const match = rawMap.get(key)?.shift();

    if (!match) {
      return row;
    }

    return {
      ...row,
      sigunguCd: match.sigunguCd || row.sigunguCd || "",
      bjdongCd: match.bjdongCd || row.bjdongCd || "",
      bun: match.bun || row.bun || "",
      ji: match.ji || row.ji || "",
      buildYear: row.buildYear || match.buildYear || "",
    };
  });
}

async function fetchRawTradeRows(routeConfig, regionCode, yearMonth, numOfRows) {
  const targetUrl = buildDataGoKrUrl(routeConfig.rawUrl, {
    LAWD_CD: regionCode,
    DEAL_YMD: yearMonth,
    numOfRows: numOfRows,
    pageNo: 1,
  });

  console.log(`[raw:${routeConfig.rawKind}] ${sanitizeUrl(targetUrl)}`);
  const { body } = await fetchText(targetUrl);
  return parseRawTradeRows(body, routeConfig.rawKind);
}

// ── 전월세 XML 파싱 (직접 API fallback 용) ──
function parseRawRentRows(xmlText, rawKind) {
  return extractXmlItems(xmlText)
    .map((itemXml) => {
      const deposit = parseAmount(
        getXmlTagValue(itemXml, "deposit") ||
        getXmlTagValue(itemXml, "보증금액"),
      );
      const monthlyRent = parseAmount(
        getXmlTagValue(itemXml, "monthlyRentAmount") ||
        getXmlTagValue(itemXml, "월세금액") ||
        getXmlTagValue(itemXml, "monthlyRent"),
      );

      const baseRow = {
        dong: getXmlTagValue(itemXml, "umdNm") || getXmlTagValue(itemXml, "법정동"),
        gu: getXmlTagValue(itemXml, "umdNm") || getXmlTagValue(itemXml, "법정동"),
        deposit: deposit || 0,
        monthly: monthlyRent || 0,
        date: makeDate(
          getXmlTagValue(itemXml, "dealYear") || getXmlTagValue(itemXml, "년"),
          getXmlTagValue(itemXml, "dealMonth") || getXmlTagValue(itemXml, "월"),
          getXmlTagValue(itemXml, "dealDay") || getXmlTagValue(itemXml, "일"),
        ),
        buildYear: parseIntSafe(getXmlTagValue(itemXml, "buildYear") || getXmlTagValue(itemXml, "건축년도")) || "",
      };
      baseRow.type = baseRow.monthly > 0 ? "월세" : "전세";

      if (rawKind === "apt") {
        return {
          ...baseRow,
          apt: getXmlTagValue(itemXml, "aptNm") || getXmlTagValue(itemXml, "단지명") || "N/A",
          area: parseFloatSafe(getXmlTagValue(itemXml, "excluUseAr") || getXmlTagValue(itemXml, "전용면적")),
          floor: parseIntSafe(getXmlTagValue(itemXml, "floor") || getXmlTagValue(itemXml, "층")),
        };
      }

      if (rawKind === "offi") {
        return {
          ...baseRow,
          apt: getXmlTagValue(itemXml, "offiNm") || "N/A",
          area: parseFloatSafe(getXmlTagValue(itemXml, "excluUseAr")),
          floor: parseIntSafe(getXmlTagValue(itemXml, "floor")),
        };
      }

      if (rawKind === "villa") {
        return {
          ...baseRow,
          apt: getXmlTagValue(itemXml, "mhouseNm") || getXmlTagValue(itemXml, "houseType") || "N/A",
          area: parseFloatSafe(getXmlTagValue(itemXml, "excluUseAr")),
          floor: parseIntSafe(getXmlTagValue(itemXml, "floor")),
        };
      }

      if (rawKind === "house") {
        return {
          ...baseRow,
          apt: getXmlTagValue(itemXml, "houseType") || "N/A",
          area: parseFloatSafe(getXmlTagValue(itemXml, "totalFloorAr") || getXmlTagValue(itemXml, "excluUseAr")),
          floor: 0,
        };
      }

      return null;
    })
    .filter(Boolean);
}

// ── MCP fallback: 직접 data.go.kr API 호출 ──
async function fetchDirectApiData(routeConfig, regionCode, yearMonth, numOfRows) {
  const targetUrl = buildDataGoKrUrl(routeConfig.rawUrl, {
    LAWD_CD: regionCode,
    DEAL_YMD: yearMonth,
    numOfRows: numOfRows,
    pageNo: 1,
  });

  console.log(`[direct-api:${routeConfig.rawKind}] ${sanitizeUrl(targetUrl)}`);
  const { body } = await fetchText(targetUrl);

  let items;
  if (routeConfig.kind === "trade") {
    items = parseRawTradeRows(body, routeConfig.rawKind);
    // trade items에 normalizeTradeItems와 동일한 형태 보장
    items = items.map((row) => ({
      apt: row.apt || "N/A",
      dong: row.dong || "",
      gu: row.dong || "",
      price: row.price || 0,
      area: row.area || 0,
      floor: row.floor || 0,
      date: row.date || "",
      buildYear: row.buildYear || "",
      dealType: row.dealType || "",
      sigunguCd: row.sigunguCd || "",
      bjdongCd: row.bjdongCd || "",
      bun: row.bun || "",
      ji: row.ji || "",
      households: null,
      completionYearMonth: formatCompletionYearMonth("", String(row.buildYear || "")),
      nearbyStation: "",
      nearbyStationDistanceKm: null,
    }));
  } else {
    // rent
    items = parseRawRentRows(body, routeConfig.rawKind);
  }

  return {
    items,
    total_count: items.length,
    summary: routeConfig.kind === "trade" ? emptyTradeSummary() : emptyRentSummary(),
  };
}

async function fetchRawTradeRowsPage(rawKind, regionCode, yearMonth, numOfRows, pageNo) {
  const targetUrl = buildDataGoKrUrl(API_URLS.aptTrade, {
    LAWD_CD: regionCode,
    DEAL_YMD: yearMonth,
    numOfRows,
    pageNo,
  });

  console.log(`[listing-seed:${yearMonth}] ${sanitizeUrl(targetUrl)}`);
  const { body } = await fetchText(targetUrl);
  const rows = parseRawTradeRows(body, rawKind);
  const totalCount = parsePositiveInt(getXmlTagValue(body, "totalCount"), 0);
  return {
    rows,
    totalCount,
  };
}

async function collectListingSeedPool(regionCode) {
  const months = getRecentYearMonthsServer(LISTING_LOOKBACK_MONTHS);
  const cacheKey = `${regionCode}:${months.join(",")}`;
  const cached = getTimedMapValue(listingSeedCache, cacheKey, LISTING_SEED_CACHE_TTL_MS);
  if (cached) {
    return cached;
  }

  const seedMap = new Map();

  // 월별 첫 페이지를 병렬로 가져옴
  const firstPages = await Promise.all(
    months.map((yearMonth) =>
      fetchRawTradeRowsPage("apt", regionCode, yearMonth, LISTING_PAGE_SIZE, 1)
        .then((result) => ({ yearMonth, ...result }))
        .catch(() => ({ yearMonth, rows: [], totalCount: 0 })),
    ),
  );

  // 추가 페이지가 필요한 월 수집
  const extraFetches = [];
  for (const { yearMonth, totalCount } of firstPages) {
    const totalPages = totalCount > 0 ? Math.ceil(totalCount / LISTING_PAGE_SIZE) : 1;
    for (let pageNo = 2; pageNo <= Math.min(totalPages, LISTING_MAX_PAGES); pageNo++) {
      extraFetches.push(
        fetchRawTradeRowsPage("apt", regionCode, yearMonth, LISTING_PAGE_SIZE, pageNo)
          .then((result) => ({ yearMonth, ...result }))
          .catch(() => ({ yearMonth, rows: [], totalCount: 0 })),
      );
    }
  }

  const extraPages = extraFetches.length > 0 ? await Promise.all(extraFetches) : [];
  const allPages = [...firstPages, ...extraPages];

  for (const { rows } of allPages) {
    for (const row of rows) {
      const aptName = String(row.apt || "").trim();
      if (!aptName) {
        continue;
      }

      const key = buildListingSeedKey(row);
      let entry = seedMap.get(key);
      if (!entry) {
        entry = {
          apt: aptName,
          dong: row.dong || "",
          buildYear: row.buildYear || "",
          tradeCount: 0,
          parcels: new Map(),
        };
        seedMap.set(key, entry);
      }

      entry.tradeCount += 1;
      if (!entry.buildYear && row.buildYear) {
        entry.buildYear = row.buildYear;
      }

      const parcelKey = [
        row.sigunguCd || "",
        row.bjdongCd || "",
        row.bun || "",
        row.ji || "",
      ].join("|");
      if (!entry.parcels.has(parcelKey)) {
        entry.parcels.set(parcelKey, {
          ...row,
          hits: 0,
        });
      }
      entry.parcels.get(parcelKey).hits += 1;
    }
  }

  const seeds = [...seedMap.values()]
    .map((entry) => {
      const representative =
        [...entry.parcels.values()].sort(
          (a, b) =>
            b.hits - a.hits ||
            Number(b.buildYear || 0) - Number(a.buildYear || 0),
        )[0] || {};

      return {
        ...representative,
        apt: entry.apt,
        dong: entry.dong,
        buildYear: representative.buildYear || entry.buildYear || "",
        tradeCount: entry.tradeCount,
      };
    })
    .sort(
      (a, b) =>
        b.tradeCount - a.tradeCount ||
        a.apt.localeCompare(b.apt, "ko"),
    );

  return setTimedMapValue(listingSeedCache, cacheKey, seeds);
}

async function collectListingSeeds(regionCode, searchQuery = "") {
  const seeds = await collectListingSeedPool(regionCode);
  const searchText = normalizeText(searchQuery);
  const filtered = searchText
    ? seeds.filter((seed) =>
        normalizeText(String(seed?.apt || "")).includes(searchText),
      )
    : seeds;
  return filtered.slice(0, LISTING_MAX_SEEDS);
}

async function probeBuildingHubForSeed(seed, options = {}) {
  const { ignoreDisabled = false } = options;
  const emptySnapshot = {
    completionYear: Number.parseInt(seed?.buildYear || 0, 10) || null,
    completionYearMonth: formatCompletionYearMonth(
      "",
      Number.parseInt(seed?.buildYear || 0, 10) || null,
    ),
    zoning: null,
    zoningSource: null,
    zoningStatus: null,
    currentFar: null,
    siteArea: null,
    siteAreaFieldKey: null,
    grossFloorArea: null,
    grossFloorAreaSource: null,
    grossFloorAreaFieldKey: null,
    households: null,
    sourceReady: false,
    probeStatus: !API_KEY ? "config_error" : "no_match",
  };

  if (!API_KEY) {
    const outcome = {
      status: "config_error",
      statusCode: null,
      bodyEmpty: null,
      sampleParams: null,
      message: buildBuildingHubMessage("config_error"),
      snapshot: emptySnapshot,
    };
    updateBuildingHubState(outcome);
    return outcome;
  }

  if (!seed?.sigunguCd || !seed?.bjdongCd) {
    const outcome = {
      status: "no_match",
      statusCode: null,
      bodyEmpty: null,
      sampleParams: null,
      message: "건축HUB 조회에 필요한 시군구/법정동 코드가 없습니다.",
      snapshot: {
        ...emptySnapshot,
        probeStatus: "no_match",
      },
    };
    updateBuildingHubState(outcome);
    return outcome;
  }

  if (
    !ignoreDisabled &&
    !buildingHubState.hasWorkingResponse &&
    buildingHubState.disabledUntil > Date.now()
  ) {
    const status = getCurrentBuildingHubStatus();
    const outcome = {
      status,
      statusCode: buildingHubState.lastStatusCode,
      bodyEmpty: buildingHubState.lastBodyEmpty,
      sampleParams: buildingHubState.lastSampleParams,
      message:
        buildingHubState.lastMessage ||
        buildBuildingHubMessage(status, buildingHubState.lastSampleParams),
      snapshot: {
        ...emptySnapshot,
        probeStatus: status,
      },
    };
    return outcome;
  }

  let bestOutcome = {
    status: "upstream_error",
    statusCode: null,
    bodyEmpty: null,
    sampleParams: null,
    message: buildBuildingHubMessage("upstream_error"),
    endpointResults: [],
    snapshot: {
      ...emptySnapshot,
      probeStatus: "upstream_error",
    },
  };
  let bestSuccessOutcome = null;

  for (const variant of buildParcelVariants(seed)) {
    const sampleParams = cloneSampleParams({
      sigunguCd: seed.sigunguCd,
      bjdongCd: seed.bjdongCd,
      platGbCd: "0",
      bun: variant.bun,
      ji: variant.ji,
    });

    const endpointRequests = [
      {
        endpointKey: "getBrBasisOulnInfo",
        url: API_URLS.buildingBasisOutline,
      },
      {
        endpointKey: "getBrRecapTitleInfo",
        url: API_URLS.buildingRecap,
      },
      {
        endpointKey: "getBrTitleInfo",
        url: API_URLS.buildingTitle,
      },
      {
        endpointKey: "getBrJijiguInfo",
        url: API_URLS.buildingJijigu,
      },
    ];

    const settled = await Promise.allSettled([
      fetchBuildingHubResult(
        endpointRequests[0].endpointKey,
        endpointRequests[0].url,
        {
          ...sampleParams,
          numOfRows: "10",
          pageNo: "1",
        },
      ),
      fetchBuildingHubResult(
        endpointRequests[1].endpointKey,
        endpointRequests[1].url,
        {
          ...sampleParams,
          numOfRows: "10",
          pageNo: "1",
        },
      ),
      fetchBuildingHubResult(
        endpointRequests[2].endpointKey,
        endpointRequests[2].url,
        {
          ...sampleParams,
          numOfRows: "10",
          pageNo: "1",
        },
      ),
      fetchBuildingHubResult(
        endpointRequests[3].endpointKey,
        endpointRequests[3].url,
        {
          ...sampleParams,
          numOfRows: "10",
          pageNo: "1",
        },
      ),
    ]);

    const successful = settled
      .filter((result) => result.status === "fulfilled")
      .map((result) => result.value);
    const endpointResults = summarizeBuildingHubResults(successful);
    const firstError = settled.find((result) => result.status === "rejected");

    const basisResult = settled[0].status === "fulfilled" ? settled[0].value : null;
    const recapResult = settled[1].status === "fulfilled" ? settled[1].value : null;
    const titleResult = settled[2].status === "fulfilled" ? settled[2].value : null;
    const jijiguResult = settled[3].status === "fulfilled" ? settled[3].value : null;
    const allowRawFallback = Boolean(variant.ji);
    const recapItems = filterItemsByComplexName(recapResult?.items, seed.apt, {
      allowRawFallback,
    });
    const titleItems = filterItemsByComplexName(titleResult?.items, seed.apt, {
      allowRawFallback,
    });
    const basisItems = filterItemsByComplexName(basisResult?.items, seed.apt, {
      allowRawFallback,
    });
    const namedCoreItems = [...recapItems, ...titleItems, ...basisItems];
    const zoneName =
      namedCoreItems.length > 0 ? extractZoneFromJijigu(jijiguResult?.items || []) : "";
    const extractedSiteAreaInfo = pickFirstNumberWithKeyFromSources(namedCoreItems, [
      "platArea",
      "siteArea",
    ]);
    const extractedSiteArea = extractedSiteAreaInfo.value;
    const extractedGrossFloorAreaInfo = pickFirstNumberWithKeyFromSources(namedCoreItems, [
      "totArea",
      "totalArea",
    ]);
    const extractedGrossFloorArea = extractedGrossFloorAreaInfo.value;
    const extractedCurrentFar = pickFirstNumberFromSources(namedCoreItems, [
      "vlRat",
      "vlrt",
    ]);
    const extractedHouseholds = extractBuildingHubHouseholds(
      recapItems,
      titleItems,
      basisItems,
    );
    const isRelevantResult =
      namedCoreItems.length > 0 &&
      ((extractedSiteArea != null && extractedSiteArea > 0) ||
        (extractedCurrentFar != null && extractedCurrentFar > 0) ||
        (extractedHouseholds != null && extractedHouseholds > 0) ||
        Boolean(zoneName));

    if (isRelevantResult) {
      const completionYear = extractCompletionYearFromSources(
        namedCoreItems,
        seed.buildYear,
      );
      const completionDateRaw = extractCompletionDateRawFromSources(namedCoreItems);
      const outcome = {
        status: "ok",
        statusCode:
          recapResult?.statusCode ||
          titleResult?.statusCode ||
          basisResult?.statusCode ||
          jijiguResult?.statusCode ||
          200,
        bodyEmpty: successful.every((result) => result.bodyEmpty),
        sampleParams,
        message: buildBuildingHubMessage("ok", sampleParams),
        endpointResults,
        snapshot: {
          completionYear,
          completionYearMonth: formatCompletionYearMonth(
            completionDateRaw,
            completionYear,
          ),
          zoning: zoneName || null,
          zoningSource: zoneName ? "building_hub" : null,
          zoningStatus: zoneName ? "ok" : null,
          currentFar: extractedCurrentFar,
          currentFarSource:
            extractedCurrentFar != null && extractedCurrentFar > 0
              ? "building_hub"
              : null,
          siteArea: extractedSiteArea,
          siteAreaSource:
            extractedSiteArea != null && extractedSiteArea > 0
              ? "building_hub"
              : null,
          siteAreaFieldKey: extractedSiteAreaInfo.key || null,
          grossFloorArea: extractedGrossFloorArea,
          grossFloorAreaSource:
            extractedGrossFloorArea != null && extractedGrossFloorArea > 0
              ? "building_hub"
              : null,
          grossFloorAreaFieldKey: extractedGrossFloorAreaInfo.key || null,
          households: extractedHouseholds,
          householdsSource:
            extractedHouseholds != null && extractedHouseholds > 0
              ? "building_hub"
              : null,
          sourceReady: true,
          probeStatus: "ok",
        },
      };

      if (
        !bestSuccessOutcome ||
        compareBuildingSnapshotCompleteness(
          outcome.snapshot,
          bestSuccessOutcome.snapshot,
        ) > 0
      ) {
        bestSuccessOutcome = outcome;
      }
      continue;
    }

    const responseError = successful.find(hasDataGoResponseError);
    const variantStatus =
      successful.length === 0
        ? "upstream_error"
        : responseError
          ? "upstream_error"
          : successful.some((result) => result.bodyEmpty)
            ? "empty_body"
            : "no_match";
    const variantStatusCode =
      successful.find((result) => result.statusCode != null)?.statusCode || null;
    const variantBodyEmpty =
      successful.length > 0 ? successful.some((result) => result.bodyEmpty) : null;
    const variantMessage =
      variantStatus === "upstream_error"
        ? responseError?.resultMsg ||
          firstError?.reason?.message ||
          buildBuildingHubMessage("upstream_error", sampleParams)
        : buildBuildingHubMessage(variantStatus, sampleParams);

    const priority = {
      empty_body: 3,
      no_match: 2,
      upstream_error: 1,
    };
    if ((priority[variantStatus] || 0) >= (priority[bestOutcome.status] || 0)) {
      bestOutcome = {
        status: variantStatus,
        statusCode: variantStatusCode,
        bodyEmpty: variantBodyEmpty,
        sampleParams,
        message: variantMessage,
        endpointResults,
        snapshot: {
          ...emptySnapshot,
          probeStatus: variantStatus,
        },
      };
    }
  }

  const finalOutcome = bestSuccessOutcome || bestOutcome;
  updateBuildingHubState(finalOutcome);
  return finalOutcome;
}

async function fetchBuildingSnapshot(seed) {
  const cacheKey = [
    seed.sigunguCd || "",
    seed.bjdongCd || "",
    seed.bun || "",
    seed.ji || "",
  ].join("|");
  const cached = getTimedMapValue(
    buildingSnapshotCache,
    cacheKey,
    BUILDING_SNAPSHOT_CACHE_TTL_MS,
  );
  if (cached) {
    return cached;
  }

  // 서울 지역: Seoul Open Data 벌크 인덱스에서 즉시 조회 (API 호출 없음)
  const isSeoul = String(seed.sigunguCd || "").startsWith("11");
  if (isSeoul && seoulBuildingLoaded) {
    const seoulResult = lookupSeoulBuilding(seed.dong, seed.bun, seed.ji);
    if (seoulResult && (seoulResult.siteArea || seoulResult.households || seoulResult.currentFar)) {
      const snapshot = {
        completionYear: seoulResult.completionYear,
        completionYearMonth: seoulResult.completionYearMonth,
        zoning: null,
        zoningSource: null,
        zoningStatus: null,
        currentFar: seoulResult.currentFar,
        currentFarSource: seoulResult.currentFar ? "seoul_opendata" : null,
        siteArea: seoulResult.siteArea,
        siteAreaSource: seoulResult.siteArea ? "seoul_opendata" : null,
        siteAreaFieldKey: null,
        grossFloorArea: seoulResult.grossFloorArea,
        grossFloorAreaSource: seoulResult.grossFloorArea ? "seoul_opendata" : null,
        grossFloorAreaFieldKey: null,
        households: seoulResult.households,
        householdsSource: seoulResult.households ? "seoul_opendata" : null,
        sourceReady: true,
        probeStatus: "ok",
      };
      console.log(`[seoul-building] fast-path hit for ${seed.apt || cacheKey}`);
      const result = setTimedMapValue(buildingSnapshotCache, cacheKey, snapshot);
      schedulePersistentBuildingSave();
      return result;
    }
  }

  const outcome = await probeBuildingHubForSeed(seed);
  const result = setTimedMapValue(buildingSnapshotCache, cacheKey, outcome.snapshot);
  if (outcome.snapshot?.sourceReady) {
    schedulePersistentBuildingSave();
  }
  return result;
}

function normalizeAptDirectoryEntry(item) {
  const aptName =
    pickFirstValue(item, [
      "kaptName",
      "kaptname",
      "aptName",
      "aptNm",
      "houseName",
    ]) ||
    pickEntryValueByKeyPredicate(
      item,
      (key) =>
        key.includes("kaptname") ||
        key.endsWith("aptnm") ||
        key.endsWith("aptname"),
    );
  const kaptCode =
    pickFirstValue(item, ["kaptCode", "aptCode", "houseCode"]) ||
    pickEntryValueByKeyPredicate(
      item,
      (key) => key.includes("kaptcode") || key.endsWith("aptcode"),
    );
  const address =
    pickFirstValue(item, [
      "kaptAddr",
      "bjdAddress",
      "jibunAddress",
      "doroJuso",
      "roadAddress",
      "address",
    ]) ||
    pickEntryValueByKeyPredicate(
      item,
      (key) =>
        key.includes("juso") ||
        key.includes("address") ||
        key.includes("addr"),
    );
  const dongName =
    pickFirstValue(item, ["dong", "umdNm", "bjdongNm", "legalDong"]) ||
    extractDongFromAddress(address);

  if (!aptName || !kaptCode) {
    return null;
  }

  return {
    kaptCode,
    aptName,
    nameKey: normalizeComplexNameKey(aptName),
    dongName,
    dongKey: normalizeText(dongName),
    address,
    addressKey: normalizeText(address),
  };
}

async function getApartmentDirectoryForRegion(regionCode) {
  const cacheKey = String(regionCode || "").slice(0, 5);
  const cached = getTimedMapValue(
    apartmentDirectoryCache,
    cacheKey,
    KAPT_DIRECTORY_CACHE_TTL_MS,
  );
  if (cached) {
    return cached;
  }

  if (!API_KEY || !cacheKey) {
    return setTimedMapValue(apartmentDirectoryCache, cacheKey, {
      status: "config_error",
      entries: [],
    });
  }

  try {
    const result = await fetchDataGoResult(
      "getSigunguAptList3",
      API_URLS.aptListBySigungu,
      {
        sigunguCode: cacheKey,
        pageNo: "1",
        numOfRows: "2000",
      },
    );
    const entries = (result.items || [])
      .map(normalizeAptDirectoryEntry)
      .filter(Boolean);
    return setTimedMapValue(apartmentDirectoryCache, cacheKey, {
      status:
        entries.length > 0
          ? "ok"
          : result.bodyEmpty
            ? "empty_body"
            : hasDataGoResponseError(result)
              ? "upstream_error"
              : "no_match",
      entries,
    });
  } catch (error) {
    return setTimedMapValue(apartmentDirectoryCache, cacheKey, {
      status: classifyKaptStatusFromError(error),
      message: error.message,
      entries: [],
    });
  }
}

function findApartmentDirectoryMatch(entries, row) {
  const targetDongKey = normalizeText(row?.dong);
  const targetParcelFragment = buildAddressParcelFragment(row?.bun, row?.ji);
  let bestEntry = null;
  let bestScore = 0;

  for (const entry of entries || []) {
    if (!entry?.nameKey) {
      continue;
    }

    const nameScore = scoreComplexNameSimilarity(row?.apt, entry.aptName);
    if (!nameScore) {
      continue;
    }
    let score = nameScore;

    if (targetDongKey) {
      if (entry.dongKey === targetDongKey) {
        score += 35;
      } else if (entry.addressKey.includes(targetDongKey)) {
        score += 18;
      }
    }

    if (targetParcelFragment && entry.addressKey.includes(targetParcelFragment)) {
      score += 25;
    }

    if (
      score > bestScore ||
      (score === bestScore &&
        entry.addressKey.length > (bestEntry?.addressKey || "").length)
    ) {
      bestEntry = entry;
      bestScore = score;
    }
  }

  return bestScore >= 70 ? bestEntry : null;
}

function extractNearbyStationInfo(detailItem) {
  const stationName =
    pickFirstValue(detailItem, [
      "subwayStation",
      "subwayStationName",
      "subwaySttn",
      "subway1",
      "subwayStation1",
    ]) ||
    pickEntryValueByKeyPredicate(
      detailItem,
      (key) =>
        (key.includes("subway") || key.includes("sttn") || key.includes("station")) &&
        !key.includes("line") &&
        !key.includes("dist") &&
        !key.includes("distance") &&
        !key.includes("useyn"),
    );
  const lineName =
    pickFirstValue(detailItem, [
      "subwayLine",
      "subwayLineName",
      "subwayRoute",
      "subway1Line",
      "subwayLine1",
    ]) ||
    pickEntryValueByKeyPredicate(
      detailItem,
      (key) =>
        (key.includes("subway") || key.includes("sttn")) && key.includes("line"),
    );
  const distanceRaw =
    pickFirstValue(detailItem, [
      "subwayDistance",
      "subwayStationDistance",
      "subwayDist",
      "subwayDistance1",
      "subwayStationDistance1",
    ]) ||
    pickEntryValueByKeyPredicate(
      detailItem,
      (key) =>
        (key.includes("subway") || key.includes("sttn") || key.includes("station")) &&
        (key.includes("dist") || key.includes("distance")),
    );

  const distanceKm = parseDistanceToKm(distanceRaw);
  const labelParts = [];

  if (lineName && stationName && !stationName.includes(lineName)) {
    labelParts.push(lineName);
  }
  if (stationName) {
    labelParts.push(stationName);
  }

  const baseLabel = labelParts.join(" ").trim();
  const distanceLabel = formatStationDistanceLabel(distanceKm);

  return {
    label: [baseLabel, distanceLabel].filter(Boolean).join(" ").trim(),
    distanceKm,
  };
}

async function getApartmentDetailForKaptCode(kaptCode) {
  const cacheKey = String(kaptCode || "").trim();
  const cached = getTimedMapValue(
    apartmentDetailCache,
    cacheKey,
    KAPT_DETAIL_CACHE_TTL_MS,
  );
  if (cached) {
    return cached;
  }

  if (!API_KEY || !cacheKey) {
    return setTimedMapValue(apartmentDetailCache, cacheKey, {
      status: "config_error",
      station: {
        label: "",
        distanceKm: null,
      },
    });
  }

  try {
    const detailResult = await fetchDataGoResult(
      "getAphusDtlInfoV4",
      API_URLS.aptDetailInfo,
      { kaptCode: cacheKey },
    );
    const basicResult = await fetchDataGoResult(
      "getAphusBassInfoV4",
      API_URLS.aptBasicInfo,
      { kaptCode: cacheKey },
    );
    const detailItem = detailResult.items?.[0] || basicResult.items?.[0] || null;
    const station = detailItem ? extractNearbyStationInfo(detailItem) : { label: "", distanceKm: null };
    return setTimedMapValue(apartmentDetailCache, cacheKey, {
      status:
        detailItem || basicResult.items?.[0]
          ? "ok"
          : detailResult.bodyEmpty || basicResult.bodyEmpty
            ? "empty_body"
            : hasDataGoResponseError(detailResult) || hasDataGoResponseError(basicResult)
              ? "upstream_error"
              : "no_match",
      station,
    });
  } catch (error) {
    return setTimedMapValue(apartmentDetailCache, cacheKey, {
      status: classifyKaptStatusFromError(error),
      message: error.message,
      station: {
        label: "",
        distanceKm: null,
      },
    });
  }
}

async function enrichTradeItems(rawKind, items, regionCode) {
  if (!Array.isArray(items) || items.length === 0) {
    return [];
  }

  const snapshotPromiseCache = new Map();

  function getSnapshotForRow(row) {
    const cacheKey = [
      row?.sigunguCd || "",
      row?.bjdongCd || "",
      row?.bun || "",
      row?.ji || "",
    ].join("|");
    if (!cacheKey.replace(/\|/g, "")) {
      return Promise.resolve(null);
    }
    if (!snapshotPromiseCache.has(cacheKey)) {
      snapshotPromiseCache.set(
        cacheKey,
        fetchBuildingSnapshot(row).catch(() => null),
      );
    }
    return snapshotPromiseCache.get(cacheKey);
  }

  return asyncMapLimit(items, TRADE_ENRICH_CONCURRENCY, async (row) => {
    const buildingInfo = await getSnapshotForRow(row);
    const completionYearMonth =
      buildingInfo?.completionYearMonth || formatCompletionYearMonth("", row?.buildYear);

    let nearbyStation = "";
    let nearbyStationDistanceKm = null;

    if (rawKind === "apt") {
      const coord = await resolveApartmentCoordinate(row, regionCode);

      const nearestStation = coord
        ? findNearestStationForApartment(coord)
        : null;

      if (nearestStation && nearestStation.distanceKm <= WALKING_DISTANCE_KM) {
        nearbyStation = nearestStation.label || "";
        nearbyStationDistanceKm = nearestStation.distanceKm;
      } else if (nearestStation) {
        nearbyStation = "";
        nearbyStationDistanceKm = nearestStation.distanceKm;
      }
    }

    return {
      ...row,
      households: buildingInfo?.households ?? null,
      completionYearMonth,
      nearbyStation,
      nearbyStationDistanceKm,
    };
  });
}

async function supplementFromKapt(snapshot, seed, regionCode, directoryState) {
  if (
    snapshot.sourceReady &&
    hasMeaningfulBuildingMetric(snapshot.siteArea) &&
    hasMeaningfulBuildingMetric(snapshot.households) &&
    !needsOfficialAnalysisFallback(snapshot)
  ) {
    return snapshot;
  }

  if (directoryState?.status && directoryState.status !== "ok") {
    return {
      ...snapshot,
      kaptStatus: directoryState.status,
    };
  }

  const match = findApartmentDirectoryMatch(directoryState?.entries || [], seed);
  if (!match?.kaptCode) {
    return {
      ...snapshot,
      kaptStatus: "no_match",
    };
  }

  try {
    const basicResult = await fetchDataGoResult(
      "getAphusBassInfoV4",
      API_URLS.aptBasicInfo,
      { kaptCode: match.kaptCode },
    );
    const basicItem = basicResult?.items?.[0] || null;
    const kaptHouseholds = pickFirstIntegerFromSources(
      [basicItem].filter(Boolean),
      ["kaptdaCnt", "hhldCnt", "houseHoldCnt", "totHhldCnt", "hoCnt"],
    );
    const kaptSiteAreaInfo = pickFirstNumberWithKeyFromSources(
      [basicItem].filter(Boolean),
      ["platArea", "siteArea"],
    );
    const kaptSiteArea = kaptSiteAreaInfo.value;
    const kaptGrossFloorAreaInfo = pickFirstNumberWithKeyFromSources(
      [basicItem].filter(Boolean),
      ["kaptTarea", "totArea", "totalArea"],
    );
    const kaptGrossFloorArea = kaptGrossFloorAreaInfo.value;
    const kaptFar = pickFirstNumberFromSources(
      [basicItem].filter(Boolean),
      ["kaptFar", "vlRat", "vlrt"],
    );
    const kaptMetrics = {
      households: kaptHouseholds,
      siteArea: kaptSiteArea,
      siteAreaFieldKey: kaptSiteAreaInfo.key || null,
      grossFloorArea: kaptGrossFloorArea,
      grossFloorAreaFieldKey: kaptGrossFloorAreaInfo.key || null,
      currentFar: kaptFar,
    };
    const resolvedKaptStatus = basicItem ? "ok" : "no_match";

    let mergedSnapshot = {
      ...snapshot,
      matchedKaptCode: match.kaptCode,
      kaptStatus: resolvedKaptStatus,
    };

    if (
      hasMeaningfulBuildingMetric(kaptHouseholds) ||
      hasMeaningfulBuildingMetric(kaptSiteArea) ||
      hasMeaningfulBuildingMetric(kaptFar)
    ) {
      mergedSnapshot = chooseBestListingSnapshotCandidate([
        mergedSnapshot,
        buildKaptSnapshotCandidate(snapshot, kaptMetrics, {
          preferSupplement: false,
          matchedKaptCode: match.kaptCode,
          kaptStatus: resolvedKaptStatus,
        }),
        buildKaptSnapshotCandidate(snapshot, kaptMetrics, {
          preferSupplement: true,
          matchedKaptCode: match.kaptCode,
          kaptStatus: resolvedKaptStatus,
        }),
      ]);
    }

    if (needsOfficialAnalysisFallback(mergedSnapshot)) {
      mergedSnapshot = await repairSnapshotWithVworldFallback(
        mergedSnapshot,
        seed,
        collectKaptAddressCandidates(match, basicItem),
      );
    }

    // seed PNU로 직접 용도지역 해소 (VWorld address lookup 실패 시 보완)
    if (isGenericZoneName(mergedSnapshot?.zoning)) {
      const seedPnu = mergedSnapshot?.vworldPnu || buildPnuFromSeed(seed);
      if (seedPnu) {
        const landUseResult = await fetchVworldLandUseAttr(seedPnu);
        if (landUseResult.status === "ok" && landUseResult.zoning && !isGenericZoneName(landUseResult.zoning)) {
          console.log(`[vworld-zoning] refined "${mergedSnapshot.zoning}" → "${landUseResult.zoning}" for ${seed.apt} (PNU: ${seedPnu})`);
          mergedSnapshot = {
            ...mergedSnapshot,
            zoning: landUseResult.zoning,
            zoningSource: "vworld_landuse_attr_seed",
            vworldPnu: seedPnu,
          };
        }
      }
    }

    if (mergedSnapshot !== snapshot) {
      return {
        ...mergedSnapshot,
        kaptStatus:
          mergedSnapshot.kaptStatus ||
          (basicItem
            ? resolvedKaptStatus
            : basicResult.bodyEmpty
              ? "empty_body"
              : hasDataGoResponseError(basicResult)
                ? "upstream_error"
                : "no_match"),
      };
    }

    return {
      ...snapshot,
      matchedKaptCode: match.kaptCode,
      kaptStatus:
        basicResult.bodyEmpty
          ? "empty_body"
          : hasDataGoResponseError(basicResult)
            ? "upstream_error"
            : "no_match",
    };
  } catch (error) {
    return {
      ...snapshot,
      matchedKaptCode: match.kaptCode,
      kaptStatus: classifyKaptStatusFromError(error),
    };
  }
}

async function getListingCacheEntryForRegion(regionCode, searchQuery = "") {
  const cacheKey = `${regionCode}:${normalizeText(searchQuery)}`;
  const currentStatus = getCurrentBuildingHubStatus();
  if (
    listingCache.has(cacheKey)
  ) {
    const cached = listingCache.get(cacheKey);
    if (
      cached &&
      Date.now() - cached.timestamp < LISTING_CACHE_TTL_MS &&
      cached.buildingHubStatus === currentStatus
    ) {
      return cached;
    }
    listingCache.delete(cacheKey);
  }

  const seeds = await collectListingSeeds(regionCode, searchQuery);

  const directory = await getApartmentDirectoryForRegion(regionCode);

  const enrichedItems = await asyncMapLimit(
    seeds,
    LISTING_ENRICH_CONCURRENCY,
    async (seed) => {
      let snapshot = await fetchBuildingSnapshot(seed);
      snapshot = await supplementFromKapt(snapshot, seed, regionCode, directory);
      return {
        seed,
        snapshot,
      };
    },
  );
  const collapsedItems = collapseListingEntities(enrichedItems)
    .sort(
      (a, b) =>
        Number(b.seed?.tradeCount || 0) - Number(a.seed?.tradeCount || 0) ||
        String(a.seed?.apt || "").localeCompare(String(b.seed?.apt || ""), "ko"),
    )
    .slice(0, LISTING_MAX_COMPLEXES);

  const rows = await Promise.all(
    collapsedItems.map(async (item) => {
      // 근처 역은 로컬 데이터로 즉시 계산 (VWorld 불필요)
      let partialInsights = buildPendingListingLocationInsights();
      try {
        const coord = await resolveApartmentCoordinate(item.seed, regionCode);
        if (coord) {
          const nearestStation = findNearestStationForApartment(coord);
          if (nearestStation) {
            const WALKING_KM = 2.0;
            partialInsights.nearbyStation =
              nearestStation.distanceKm <= WALKING_KM
                ? nearestStation.label || ""
                : "";
            partialInsights.nearbyStationDistanceKm = nearestStation.distanceKm;
          }
        }
      } catch { /* 실패 시 deferred로 유지 */ }
      return buildListingRow(
        item.seed,
        item.snapshot,
        regionCode,
        partialInsights,
      );
    }),
  );
  const rowContextsById = new Map();
  rows.forEach((row, index) => {
    rowContextsById.set(row.id, collapsedItems[index]);
  });

  const cacheEntry = {
    timestamp: Date.now(),
    buildingHubStatus: currentStatus,
    payload: {
      items: rows,
      summary: {
        ...summarizeListingRows(rows, seeds, directory, currentStatus),
        locationInsightsStatus: rows.length > 0 ? "deferred" : "ok",
      },
    },
    rowContextsById,
  };
  listingCache.set(cacheKey, cacheEntry);
  return cacheEntry;
}

async function buildListingRowsForRegion(regionCode, searchQuery = "") {
  const cacheEntry = await getListingCacheEntryForRegion(regionCode, searchQuery);
  return cacheEntry.payload;
}

async function resolveListingLocationInsightsForIds(
  regionCode,
  searchQuery = "",
  ids = [],
) {
  const cacheEntry = await getListingCacheEntryForRegion(regionCode, searchQuery);
  const uniqueIds = [...new Set(toArray(ids).map((id) => String(id || "").trim()).filter(Boolean))];

  return asyncMapLimit(uniqueIds, LISTING_ENRICH_CONCURRENCY, async (id) => {
    const cached = getTimedMapValue(
      listingLocationInsightCache,
      id,
      LISTING_LOCATION_INSIGHT_CACHE_TTL_MS,
    );
    if (cached) {
      return {
        id,
        ...cached,
      };
    }

    const rowContext = cacheEntry.rowContextsById.get(id);
    if (!rowContext?.seed) {
      return {
        id,
        ...buildResolvedListingLocationInsights({
          nearbyElementarySchoolStatus: "unknown",
          nearbyParkStatus: "unknown",
          flatLandStatus: "unknown",
        }),
      };
    }

    const locationInsights = buildResolvedListingLocationInsights(
      await buildListingLocationInsights(rowContext.seed, regionCode),
    );
    setTimedMapValue(listingLocationInsightCache, id, locationInsights);
    return {
      id,
      ...locationInsights,
    };
  });
}

async function handleBuildingHubHealth(searchParams, res) {
  const regionCode =
    (searchParams.get("LAWD_CD") || searchParams.get("regionCode") || "11680").trim();

  if (!API_KEY) {
    sendJson(res, 200, {
      connected: false,
      approvedOrResponsive: false,
      buildingHubStatus: "config_error",
      lastProbeAt: buildingHubState.lastProbeAt,
      lastStatusCode: buildingHubState.lastStatusCode,
      lastBodyEmpty: buildingHubState.lastBodyEmpty,
      sampleParams: buildingHubState.lastSampleParams,
      endpointResults: cloneEndpointResults(buildingHubState.lastEndpointResults),
      serviceBaseUrl: "http://apis.data.go.kr/1613000/BldRgstHubService",
      message: buildBuildingHubMessage("config_error"),
      source: SOURCE,
    });
    return;
  }

  try {
    const seeds = await collectListingSeeds(regionCode, "");
    const probeSeed = seeds.find(
      (seed) => seed.sigunguCd && seed.bjdongCd && seed.bun,
    );

    if (!probeSeed) {
      const message = "건축HUB 상태 점검에 사용할 최근 실거래 표본을 찾지 못했습니다.";
      updateBuildingHubState({
        status: "no_match",
        statusCode: null,
        bodyEmpty: null,
        sampleParams: null,
        message,
      });
    sendJson(res, 200, {
      connected: true,
      approvedOrResponsive: true,
      buildingHubStatus: "no_match",
      lastProbeAt: buildingHubState.lastProbeAt,
      lastStatusCode: buildingHubState.lastStatusCode,
      lastBodyEmpty: buildingHubState.lastBodyEmpty,
      sampleParams: buildingHubState.lastSampleParams,
      endpointResults: cloneEndpointResults(buildingHubState.lastEndpointResults),
      serviceBaseUrl: "http://apis.data.go.kr/1613000/BldRgstHubService",
      message,
      source: SOURCE,
      });
      return;
    }

    const outcome = await probeBuildingHubForSeed(probeSeed, {
      ignoreDisabled: true,
    });
    sendJson(res, 200, {
      connected: true,
      approvedOrResponsive: ["ok", "no_match"].includes(outcome.status),
      buildingHubStatus: outcome.status,
      lastProbeAt: buildingHubState.lastProbeAt,
      lastStatusCode: buildingHubState.lastStatusCode,
      lastBodyEmpty: buildingHubState.lastBodyEmpty,
      sampleParams: outcome.sampleParams || buildingHubState.lastSampleParams,
      endpointResults: cloneEndpointResults(outcome.endpointResults),
      serviceBaseUrl: "http://apis.data.go.kr/1613000/BldRgstHubService",
      message: outcome.message,
      source: SOURCE,
    });
  } catch (error) {
    const message = buildBuildingHubMessage("upstream_error", null, error.message);
    updateBuildingHubState({
      status: "upstream_error",
      statusCode: null,
      bodyEmpty: null,
      sampleParams: null,
      message,
    });
    sendJson(res, 200, {
      connected: true,
      approvedOrResponsive: false,
      buildingHubStatus: "upstream_error",
      lastProbeAt: buildingHubState.lastProbeAt,
      lastStatusCode: buildingHubState.lastStatusCode,
      lastBodyEmpty: buildingHubState.lastBodyEmpty,
      sampleParams: buildingHubState.lastSampleParams,
      endpointResults: cloneEndpointResults(buildingHubState.lastEndpointResults),
      serviceBaseUrl: "http://apis.data.go.kr/1613000/BldRgstHubService",
      message,
      source: SOURCE,
    });
  }
}

function handleRegions(res) {
  sendJson(res, 200, {
    sidoOptions: REGION_CATALOG.sidoOptions,
    sigunguOptionsBySido: REGION_CATALOG.sigunguOptionsBySido,
    source: SOURCE,
  });
}

async function handleListingGrid(searchParams, res) {
  const regionCode =
    (searchParams.get("LAWD_CD") || searchParams.get("regionCode") || "11680").trim();
  const query = (searchParams.get("q") || "").trim();

  if (!API_KEY) {
    sendJson(res, 500, {
      error: "config_error",
      message: "DATA_GO_KR_API_KEY is not configured.",
      source: SOURCE,
    });
    return;
  }

  try {
    const payload = await buildListingRowsForRegion(regionCode, query);
    sendJson(res, 200, {
      totalCount: payload.items.length,
      items: payload.items,
      summary: payload.summary,
      source: SOURCE,
    });
  } catch (error) {
    sendJson(res, 502, {
      error: "upstream_error",
      message: error.message,
      source: SOURCE,
    });
  }
}

async function handleListingLocationInsights(searchParams, res) {
  const regionCode =
    (searchParams.get("LAWD_CD") || searchParams.get("regionCode") || "11680").trim();
  const query = (searchParams.get("q") || "").trim();
  const ids = [
    ...searchParams.getAll("id"),
    ...String(searchParams.get("ids") || "")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean),
  ];

  if (!API_KEY) {
    sendJson(res, 500, {
      error: "config_error",
      message: "DATA_GO_KR_API_KEY is not configured.",
      source: SOURCE,
    });
    return;
  }

  if (!ids.length) {
    sendJson(res, 200, {
      items: [],
      source: SOURCE,
    });
    return;
  }

  try {
    const items = await resolveListingLocationInsightsForIds(regionCode, query, ids);
    sendJson(res, 200, {
      items,
      source: SOURCE,
    });
  } catch (error) {
    sendJson(res, 502, {
      error: "upstream_error",
      message: error.message,
      source: SOURCE,
    });
  }
}

function statusForToolPayload(payload) {
  if (payload.error === "network_error") {
    return 502;
  }
  if (payload.error === "config_error") {
    return 500;
  }
  if (payload.error === "tool_error") {
    return 502;
  }
  return 500;
}

async function handleConfig(res) {
  let connected = false;
  let message = null;

  try {
    await ensureMcpConnected();
    connected = true;
  } catch (error) {
    message = error.message;
  }

  sendJson(res, 200, {
    connected,
    source: SOURCE,
    transport: "stdio",
    datasets: {
      stations: {
        available: loadStationDataset().items.length > 0,
        message: loadStationDataset().message,
      },
      apartmentCoords: {
        available: loadApartmentCoordinateDataset().items.length > 0,
        message: loadApartmentCoordinateDataset().message,
      },
    },
    integrations: {
      dataGoKr: Boolean(API_KEY),
      vworldAddress: Boolean(VWORLD_API_KEY),
      vworldData: Boolean(VWORLD_DATA_API_KEY),
      vworldLandUse: Boolean(VWORLD_LANDUSE_API_KEY),
      vworldDataDomain: VWORLD_DATA_DOMAIN || null,
      seoulBuilding: Boolean(SEOUL_BUILDING_API_KEY),
      seoulBuildingLoaded: seoulBuildingLoaded,
      seoulBuildingRecords: seoulBuildingIndex.size,
      seoulRealestate: Boolean(SEOUL_REALESTATE_API_KEY),
      seoulBrokerLoaded: seoulBrokerLoaded,
      seoulBrokerDistricts: seoulBrokerIndex.size,
    },
    ...(message ? { message } : {}),
  });
}

async function handleMcpRoute(routeConfig, searchParams, res) {
  const regionCode = searchParams.get("LAWD_CD") || "";
  const yearMonth = searchParams.get("DEAL_YMD") || "";
  const numOfRows = parsePositiveInt(searchParams.get("numOfRows"), 1000);

  if (!regionCode || !yearMonth) {
    sendJson(res, 400, {
      error: "bad_request",
      message: "LAWD_CD and DEAL_YMD are required.",
      source: SOURCE,
    });
    return;
  }

  // 입력값 형식 검증 (파라미터 인젝션 방지)
  if (!/^\d{5}$/.test(regionCode)) {
    sendJson(res, 400, { error: "bad_request", message: "Invalid LAWD_CD format.", source: SOURCE });
    return;
  }
  if (!/^\d{6}$/.test(yearMonth)) {
    sendJson(res, 400, { error: "bad_request", message: "Invalid DEAL_YMD format.", source: SOURCE });
    return;
  }

  const cacheKey = `${routeConfig.tool}:${regionCode}:${yearMonth}:${numOfRows}`;
  const cached = getCached(cacheKey);
  if (cached) {
    sendJson(res, 200, cached);
    return;
  }

  let payload;
  let usedDirectFallback = false;
  try {
    payload = await callMcpTool(routeConfig.tool, {
      region_code: regionCode,
      year_month: yearMonth,
      num_of_rows: numOfRows,
    });
  } catch (error) {
    // MCP 미연결 시 직접 API 호출 fallback
    if (API_KEY && routeConfig.rawUrl) {
      console.log(`[mcp-fallback] MCP unavailable, using direct API for ${routeConfig.tool}`);
      try {
        const directResult = await fetchDirectApiData(routeConfig, regionCode, yearMonth, numOfRows);
        payload = directResult;
        usedDirectFallback = true;
      } catch (directError) {
        sendJson(res, 503, {
          error: "api_unavailable",
          message: `MCP: ${error.message}; Direct: ${directError.message}`,
          source: SOURCE,
        });
        return;
      }
    } else {
      sendJson(res, 503, {
        error: "mcp_unavailable",
        message: error.message,
        source: SOURCE,
      });
      return;
    }
  }

  if (payload?.error && !usedDirectFallback) {
    if (payload.error === "api_error" && payload.code === "03") {
      sendJson(res, 200, buildNoDataPayload(routeConfig.kind));
      return;
    }

    sendJson(res, statusForToolPayload(payload), {
      ...payload,
      source: SOURCE,
    });
    return;
  }

  const totalCount = Number(payload?.total_count ?? payload?.totalCount ?? 0);
  const summary =
    payload?.summary ||
    (routeConfig.kind === "trade" ? emptyTradeSummary() : emptyRentSummary());

  if (routeConfig.kind === "trade") {
    let items = usedDirectFallback
      ? (payload?.items || [])
      : normalizeTradeItems(routeConfig.rawKind, payload?.items || []);

    if (API_KEY && routeConfig.rawUrl) {
      try {
        const rawRows = await fetchRawTradeRows(
          routeConfig,
          regionCode,
          yearMonth,
          numOfRows,
        );
        items = mergeRawTradeMetadata(items, rawRows, routeConfig.rawKind);
      } catch (error) {
        console.warn(`[raw:${routeConfig.rawKind}] ${error.message}`);
      }
    }

    if (API_KEY) {
      try {
        items = await enrichTradeItems(routeConfig.rawKind, items, regionCode);
      } catch (error) {
        console.warn(`[trade-enrich:${routeConfig.rawKind}] ${error.message}`);
      }
    }

    const tradeResult = {
      totalCount: totalCount || items.length,
      items,
      summary,
      source: SOURCE,
    };
    setCache(cacheKey, tradeResult);
    sendJson(res, 200, tradeResult);
    return;
  }

  const items = usedDirectFallback
    ? (payload?.items || [])
    : normalizeRentItems(routeConfig.rawKind, payload?.items || []);
  const rentResult = {
    totalCount: totalCount || items.length,
    items,
    summary,
    source: SOURCE,
  };
  setCache(cacheKey, rentResult);
  sendJson(res, 200, rentResult);
}

async function handleBuilding(searchParams, res) {
  if (!API_KEY) {
    sendJson(res, 500, {
      error: "config_error",
      message: "DATA_GO_KR_API_KEY is not configured.",
      source: SOURCE,
    });
    return;
  }

  const targetUrl = buildDataGoKrUrl(API_URLS.building, {
    sigunguCd: searchParams.get("sigunguCd") || "",
    bjdongCd: searchParams.get("bjdongCd") || "",
    platGbCd: searchParams.get("platGbCd") || "",
    bun: searchParams.get("bun") || "",
    ji: searchParams.get("ji") || "",
    numOfRows: searchParams.get("numOfRows") || "5",
    pageNo: searchParams.get("pageNo") || "1",
  });

  try {
    console.log(`[building] ${sanitizeUrl(targetUrl)}`);
    const { body } = await fetchText(targetUrl);
    writeCorsHeaders(res);
    res.writeHead(200, {
      "Content-Type": "application/xml; charset=utf-8",
    });
    res.end(body);
  } catch (error) {
    sendJson(res, 502, {
      error: "upstream_error",
      message: error.message,
      source: SOURCE,
    });
  }
}

// ⚠️ DISCLAIMER: 네이버 부동산 API는 비공식(비공개) API입니다.
// 네이버의 이용약관 변경 또는 IP 차단에 의해 언제든 사용 불가할 수 있으며,
// 상업적 이용 시 법적 책임은 사용자에게 있습니다. Rate limit(429) 발생 빈도가 높습니다.
async function handleNaverSearch(searchParams, res) {
  const query = (searchParams.get("q") || "").trim();
  if (!query) {
    sendJson(res, 400, {
      error: "bad_request",
      message: "q parameter is required.",
    });
    return;
  }

  const searchUrl = `https://new.land.naver.com/api/search?query=${encodeURIComponent(query)}&scope=complex&type=complex&page=1&size=10`;
  try {
    console.log(`[naver] ${query}`);
    const { statusCode, body } = await fetchText(searchUrl, {
      Referer: "https://new.land.naver.com/",
      Accept: "application/json, text/plain, */*",
    });
    writeCorsHeaders(res);
    res.writeHead(statusCode, {
      "Content-Type": "application/json; charset=utf-8",
    });
    res.end(body);
  } catch (error) {
    const status = error.code === "ETIMEDOUT" ? 504 : 502;
    sendJson(res, status, {
      error: status === 504 ? "gateway_timeout" : "upstream_error",
      message: error.message,
    });
  }
}

async function handleNaverComplexDetail(searchParams, res) {
  const complexNo = (searchParams.get("complexNo") || "").trim();
  if (!complexNo) {
    sendJson(res, 400, { error: "bad_request", message: "complexNo parameter is required." });
    return;
  }

  const detailUrl = `https://new.land.naver.com/api/complexes/${encodeURIComponent(complexNo)}?sameAddressGroup=false`;
  try {
    console.log(`[naver-complex] ${complexNo}`);
    const { statusCode, body } = await fetchText(detailUrl, {
      Referer: "https://new.land.naver.com/",
      Accept: "application/json, text/plain, */*",
    });
    writeCorsHeaders(res);
    res.writeHead(statusCode, { "Content-Type": "application/json; charset=utf-8" });
    res.end(body);
  } catch (error) {
    const status = error.code === "ETIMEDOUT" ? 504 : 502;
    sendJson(res, status, { error: "upstream_error", message: error.message });
  }
}

async function handleLawSearch(searchParams, res) {
  const query = (searchParams.get("q") || "").trim();
  const lawId = (searchParams.get("lawId") || "").trim();

  if (!LAW_API_OC) {
    sendJson(res, 500, { error: "config_error", message: "LAW_API_OC is not configured." });
    return;
  }

  let targetUrl;
  if (lawId) {
    targetUrl = `https://www.law.go.kr/DRF/lawService.do?OC=${encodeURIComponent(LAW_API_OC)}&target=law&type=JSON&ID=${encodeURIComponent(lawId)}`;
  } else if (query) {
    targetUrl = `https://www.law.go.kr/DRF/lawSearch.do?OC=${encodeURIComponent(LAW_API_OC)}&target=law&type=JSON&query=${encodeURIComponent(query)}&display=5`;
  } else {
    sendJson(res, 400, { error: "bad_request", message: "q or lawId parameter is required." });
    return;
  }

  try {
    console.log(`[law] ${query || lawId}`);
    const { body } = await fetchText(targetUrl);
    let parsed;
    try { parsed = JSON.parse(body); } catch { parsed = body; }
    sendJson(res, 200, { source: "law.go.kr", data: parsed });
  } catch (error) {
    const status = error.code === "ETIMEDOUT" ? 504 : 502;
    sendJson(res, status, { error: "upstream_error", message: error.message });
  }
}

async function handleVworldLandPrice(searchParams, res) {
  const pnu = (searchParams.get("pnu") || "").trim();
  const stdrYear = (searchParams.get("year") || "2026").trim();

  if (!VWORLD_API_KEY) {
    sendJson(res, 500, { error: "config_error", message: "VWORLD_API_KEY is not configured in .env" });
    return;
  }
  if (!pnu || !PNU_REGEX.test(pnu)) {
    sendJson(res, 400, { error: "bad_request", message: "pnu must be a 19-digit parcel number." });
    return;
  }
  if (!YEAR_REGEX.test(stdrYear)) {
    sendJson(res, 400, { error: "bad_request", message: "year must be a 4-digit year." });
    return;
  }

  const targetUrl = `https://api.vworld.kr/ned/data/getIndvdLandPriceAttr?key=${encodeURIComponent(VWORLD_API_KEY)}&pnu=${encodeURIComponent(pnu)}&stdrYear=${encodeURIComponent(stdrYear)}&format=json&numOfRows=10&pageNo=1`;

  try {
    console.log(`[vworld] pnu=${pnu} year=${stdrYear}`);
    const { body } = await fetchText(targetUrl);
    let parsed;
    try { parsed = JSON.parse(body); } catch { parsed = body; }
    sendJson(res, 200, { source: "vworld", data: parsed });
  } catch (error) {
    const status = error.code === "ETIMEDOUT" ? 504 : 502;
    sendJson(res, status, { error: "upstream_error", message: error.message });
  }
}

async function handleVworldAddress(searchParams, res) {
  const query = (searchParams.get("q") || "").trim();

  if (!VWORLD_API_KEY) {
    sendJson(res, 500, { error: "config_error", message: "VWORLD_API_KEY is not configured in .env" });
    return;
  }
  if (!query) {
    sendJson(res, 400, { error: "bad_request", message: "q parameter is required." });
    return;
  }

  const targetUrl = `https://api.vworld.kr/req/address?service=address&request=getcoord&version=2.0&crs=epsg:4326&type=PARCEL&format=json&key=${encodeURIComponent(VWORLD_API_KEY)}&address=${encodeURIComponent(query)}`;

  try {
    console.log(`[vworld-addr] q=${query}`);
    const { body } = await fetchText(targetUrl);
    let parsed;
    try { parsed = JSON.parse(body); } catch { parsed = body; }
    sendJson(res, 200, { source: "vworld", data: parsed });
  } catch (error) {
    const status = error.code === "ETIMEDOUT" ? 504 : 502;
    sendJson(res, status, { error: "upstream_error", message: error.message });
  }
}

async function handleVworldZoning(searchParams, res) {
  const query = (searchParams.get("q") || "").trim();
  const lng = parseCoordinate(searchParams.get("lng"));
  const lat = parseCoordinate(searchParams.get("lat"));

  if (!VWORLD_DATA_API_KEY) {
    sendJson(res, 500, {
      error: "config_error",
      message: "VWORLD_DATA_API_KEY is not configured in .env",
    });
    return;
  }

  let parcel = null;
  if (query) {
    const parcelResult = await fetchVworldParcelForAddress(query);
    parcel = parcelResult?.parcel || null;
    if (parcelResult.status !== "ok" || !parcel) {
      sendJson(res, 200, {
        source: "vworld",
        status: parcelResult.status,
        query,
        parcel: null,
        zoning: null,
      });
      return;
    }
  } else if (lng != null && lat != null) {
    parcel = {
      point: { lng, lat },
    };
  } else {
    sendJson(res, 400, {
      error: "bad_request",
      message: "q or lng/lat parameters are required.",
    });
    return;
  }

  try {
    const zoningResult = await fetchVworldZoning(parcel);
    sendJson(res, 200, {
      source: "vworld",
      status: zoningResult.status,
      parcel,
      zoning: zoningResult.zoning || null,
      message: zoningResult.message || null,
      code: zoningResult.code || null,
    });
  } catch (error) {
    const status = error.code === "ETIMEDOUT" ? 504 : 502;
    sendJson(res, status, { error: "upstream_error", message: error.message });
  }
}

// ── 서울 부동산 중개업소 정보 (벌크 로드 + 메모리 인덱싱) ──
// 서비스명: landBizInfo (OA-15550)
const seoulBrokerIndex = new Map(); // key: SGG_CD → BrokerItem[]
const seoulBrokerDongs = new Map(); // key: SGG_CD → Set<dongName>
let seoulBrokerLoaded = false;
let seoulBrokerLoading = false;

function transformBrokerItem(item) {
  const currentYear = new Date().getFullYear();
  const regNo = item.REST_BRKR_INFO || "";
  const yearMatch = regNo.match(/-(\d{4})-/);
  const registeredYear = yearMatch ? Number.parseInt(yearMatch[1], 10) : null;
  const businessYears = registeredYear ? (currentYear - registeredYear) : null;
  const hasAdminAction = Boolean(item.PBADMS_DSPS_STRT_DD && String(item.PBADMS_DSPS_STRT_DD).trim());
  const adminStart = hasAdminAction ? String(item.PBADMS_DSPS_STRT_DD).trim() : null;
  const adminEnd = item.PBADMS_DSPS_END_DD && String(item.PBADMS_DSPS_END_DD).trim() ? String(item.PBADMS_DSPS_END_DD).trim() : null;
  const trustScore = (businessYears || 0) * 2 + (hasAdminAction ? -50 : 10);

  return {
    regNo,
    companyName: item.BZMN_CONM || null,
    representativeName: item.MDT_BSNS_NM || null,
    address: item.ADDR || null,
    tel: item.TELNO || null,
    dong: item.LGL_DONG_NM || null,
    gu: item.CGG_CD || null,
    sggCd: item.SGG_CD || null,
    registeredYear,
    businessYears,
    hasAdminAction,
    adminActionPeriod: adminStart ? `${adminStart}${adminEnd ? " ~ " + adminEnd : " ~"}` : null,
    views: parseNumber(item.INQ_CNT) || 0,
    trustScore,
  };
}

async function loadSeoulBrokerData() {
  if (!SEOUL_REALESTATE_API_KEY) return;
  if (seoulBrokerLoaded || seoulBrokerLoading) return;
  seoulBrokerLoading = true;

  const PAGE_SIZE = 1000;
  try {
    // 총 건수 확인
    const firstUrl = `http://openapi.seoul.go.kr:8088/${encodeURIComponent(SEOUL_REALESTATE_API_KEY)}/json/landBizInfo/1/1/`;
    const firstResp = await fetchText(firstUrl, { Accept: "application/json" });
    const firstParsed = JSON.parse(String(firstResp.body || "{}"));
    const totalCount = firstParsed?.landBizInfo?.list_total_count || 0;
    if (totalCount === 0) {
      console.log("[seoul-broker] no records found");
      seoulBrokerLoading = false;
      return;
    }
    console.log(`[seoul-broker] loading ${totalCount} records from landBizInfo...`);

    const totalPages = Math.ceil(totalCount / PAGE_SIZE);
    let loaded = 0;

    // 4페이지씩 병렬 로드
    for (let i = 0; i < totalPages; i += 4) {
      const group = Array.from(
        { length: Math.min(4, totalPages - i) },
        (_, j) => i + j
      );
      const results = await Promise.allSettled(
        group.map(async (pageIdx) => {
          const s = pageIdx * PAGE_SIZE + 1;
          const e = Math.min((pageIdx + 1) * PAGE_SIZE, totalCount);
          const url = `http://openapi.seoul.go.kr:8088/${encodeURIComponent(SEOUL_REALESTATE_API_KEY)}/json/landBizInfo/${s}/${e}/`;
          const resp = await fetchText(url, { Accept: "application/json" });
          return JSON.parse(String(resp.body || "{}"));
        })
      );

      for (const result of results) {
        if (result.status !== "fulfilled") continue;
        const rows = result.value?.landBizInfo?.row;
        if (!Array.isArray(rows)) continue;
        for (const row of rows) {
          if (row.STTS_SE !== "영업중") continue;
          const sggCd = row.SGG_CD || "";
          if (!sggCd) continue;
          const transformed = transformBrokerItem(row);
          if (!seoulBrokerIndex.has(sggCd)) {
            seoulBrokerIndex.set(sggCd, []);
            seoulBrokerDongs.set(sggCd, new Set());
          }
          seoulBrokerIndex.get(sggCd).push(transformed);
          if (transformed.dong) {
            seoulBrokerDongs.get(sggCd).add(transformed.dong);
          }
          loaded++;
        }
      }
    }

    // trustScore 내림차순 사전 정렬
    for (const [, items] of seoulBrokerIndex) {
      items.sort((a, b) => b.trustScore - a.trustScore);
    }

    seoulBrokerLoaded = true;
    console.log(`[seoul-broker] indexed ${loaded} active brokers across ${seoulBrokerIndex.size} districts`);
  } catch (err) {
    console.error(`[seoul-broker] bulk load error: ${err.message}`);
  } finally {
    seoulBrokerLoading = false;
  }
}

function handleSeoulBrokers(searchParams, res) {
  if (!SEOUL_REALESTATE_API_KEY) {
    sendJson(res, 400, { error: "config_error", message: "DATA_SEOUL_realestate API key not configured" });
    return;
  }
  if (!seoulBrokerLoaded) {
    sendJson(res, 503, { error: "loading", message: "Broker data is still loading. Please retry shortly." });
    return;
  }

  const guCode = (searchParams.get("guCode") || "").trim();
  if (!/^\d{5}$/.test(guCode) || !guCode.startsWith("11")) {
    sendJson(res, 400, { error: "invalid_param", message: "guCode must be a 5-digit Seoul district code" });
    return;
  }

  const dong = (searchParams.get("dong") || "").trim();
  const query = (searchParams.get("q") || "").trim().toLowerCase();
  const sort = (searchParams.get("sort") || "trust").trim();
  const page = Math.max(1, Number.parseInt(searchParams.get("page") || "1", 10) || 1);
  const pageSize = Math.min(100, Math.max(1, Number.parseInt(searchParams.get("pageSize") || "20", 10) || 20));

  let items = seoulBrokerIndex.get(guCode) || [];
  const dongs = seoulBrokerDongs.get(guCode) || new Set();

  // 법정동 필터
  if (dong) {
    items = items.filter(item => item.dong === dong);
  }
  // 상호명 검색
  if (query) {
    items = items.filter(item =>
      (item.companyName || "").toLowerCase().includes(query) ||
      (item.representativeName || "").toLowerCase().includes(query) ||
      (item.address || "").toLowerCase().includes(query)
    );
  }

  // 정렬
  const sorted = [...items];
  if (sort === "years") {
    sorted.sort((a, b) => (b.businessYears || 0) - (a.businessYears || 0));
  } else if (sort === "views") {
    sorted.sort((a, b) => b.views - a.views);
  }
  // sort === "trust"는 이미 사전 정렬됨

  const totalCount = sorted.length;
  const start = (page - 1) * pageSize;
  const paged = sorted.slice(start, start + pageSize);

  // 요약 통계
  const avgYears = items.length > 0
    ? Math.round(items.reduce((sum, i) => sum + (i.businessYears || 0), 0) / items.length)
    : 0;
  const cleanCount = items.filter(i => !i.hasAdminAction).length;
  const veteranCount = items.filter(i => (i.businessYears || 0) >= 10).length;

  sendJson(res, 200, {
    totalCount,
    page,
    pageSize,
    gu: items[0]?.gu || guCode,
    dongs: [...dongs].sort(),
    summary: {
      total: items.length,
      avgBusinessYears: avgYears,
      cleanCount,
      veteranCount,
    },
    items: paged,
  });
}

function handleStaticFile(filePath, contentType, res) {
  try {
    const content = fs.readFileSync(filePath, "utf8");
    writeCorsHeaders(res);
    res.writeHead(200, { "Content-Type": `${contentType}; charset=utf-8` });
    res.end(content);
  } catch {
    sendText(res, 404, "Not Found");
  }
}

function handleRoot(res) {
  const routeList = [
    ...Object.keys(MCP_ROUTES),
    "/api/redevelopment-grid?regionCode=<code>&q=<name>",
    "/api/redevelopment-location-insights?regionCode=<code>&q=<name>&id=<row-id>",
    "/api/building-hub-health?regionCode=<code>",
    "/api/regions",
    "/api/building",
    "/api/naver-search?q=<query>",
    "/api/naver-complex?complexNo=<id>",
    "/api/law-search?q=<query>&lawId=<id>",
    "/api/seoul/brokers?guCode=<5digit>&start=1&end=20",
    "/api/vworld/land-price?pnu=<19digit>&year=<yyyy>",
    "/api/vworld/land-use?pnu=<19digit>",
    "/api/vworld/address?q=<address>",
    "/api/vworld/zoning?q=<address> | lng=<x>&lat=<y>",
    "/api/config",
    "/calc.html",
  ].join("\n  ");

  sendText(
    res,
    200,
    `Apt dashboard API server is running on http://localhost:${PORT}
Source: ${SOURCE}
Transport: stdio
Configured API key: ${API_KEY ? "yes" : "no"}

Available routes:
  ${routeList}`,
  );
}

// ── Serverless handler export (Vercel) ──
const requestHandler = async (req, res) => {
  // 보안 헤더
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");

  // CORS 헤더를 최상단에서 1회 설정 (req.headers.origin 기반)
  writeCorsHeaders(res, req);

  if (req.method === "OPTIONS") {
    res.writeHead(204);
    res.end();
    return;
  }

  if (req.method !== "GET") {
    sendText(res, 405, "Method Not Allowed");
    return;
  }

  // Rate limiting
  const clientIp = req.socket?.remoteAddress || "unknown";
  if (!checkRateLimit(clientIp)) {
    sendJson(res, 429, { error: "too_many_requests", message: "Rate limit exceeded. Try again later." });
    return;
  }

  const requestUrl = new URL(
    req.url || "/",
    `http://${req.headers.host || `localhost:${PORT}`}`,
  );

  try {
    if (requestUrl.pathname === "/api/config") {
      await handleConfig(res);
      return;
    }

    if (MCP_ROUTES[requestUrl.pathname]) {
      await handleMcpRoute(MCP_ROUTES[requestUrl.pathname], requestUrl.searchParams, res);
      return;
    }

    if (requestUrl.pathname === "/api/building") {
      await handleBuilding(requestUrl.searchParams, res);
      return;
    }

    if (requestUrl.pathname === "/api/redevelopment-grid") {
      await handleListingGrid(requestUrl.searchParams, res);
      return;
    }

    if (requestUrl.pathname === "/api/redevelopment-location-insights") {
      await handleListingLocationInsights(requestUrl.searchParams, res);
      return;
    }

    if (requestUrl.pathname === "/api/building-hub-health") {
      await handleBuildingHubHealth(requestUrl.searchParams, res);
      return;
    }

    if (requestUrl.pathname === "/api/regions") {
      handleRegions(res);
      return;
    }

    if (requestUrl.pathname === "/api/naver-search") {
      await handleNaverSearch(requestUrl.searchParams, res);
      return;
    }

    if (requestUrl.pathname === "/api/naver-complex") {
      await handleNaverComplexDetail(requestUrl.searchParams, res);
      return;
    }

    if (requestUrl.pathname === "/api/law-search") {
      await handleLawSearch(requestUrl.searchParams, res);
      return;
    }

    if (requestUrl.pathname === "/api/seoul/brokers") {
      await handleSeoulBrokers(requestUrl.searchParams, res);
      return;
    }

    if (requestUrl.pathname === "/api/vworld/land-price") {
      await handleVworldLandPrice(requestUrl.searchParams, res);
      return;
    }

    if (requestUrl.pathname === "/api/vworld/land-use") {
      const pnu = (requestUrl.searchParams.get("pnu") || "").trim();
      if (!PNU_REGEX.test(pnu)) {
        sendJson(res, 400, { error: "pnu must be 19 digits" });
        return;
      }
      const result = await fetchVworldLandUseAttr(pnu);
      sendJson(res, result.status === "ok" ? 200 : 502, result);
      return;
    }

    if (requestUrl.pathname === "/api/vworld/address") {
      await handleVworldAddress(requestUrl.searchParams, res);
      return;
    }

    if (requestUrl.pathname === "/api/vworld/zoning") {
      await handleVworldZoning(requestUrl.searchParams, res);
      return;
    }

    if (requestUrl.pathname === "/calc.html") {
      handleStaticFile(path.join(__dirname, "calc.html"), "text/html", res);
      return;
    }

    if (requestUrl.pathname === "/") {
      handleRoot(res);
      return;
    }

    sendText(res, 404, "Not Found");
  } catch (error) {
    console.error(`[server] ${error.stack || error.message}`);
    sendJson(res, 500, {
      error: "internal_error",
      message: "An unexpected error occurred.",
      source: SOURCE,
    });
  }
};

// ── Serverless 감지: Vercel 환경에서는 listen/MCP 생략 ──
const IS_SERVERLESS = Boolean(process.env.VERCEL || process.env.AWS_LAMBDA_FUNCTION_NAME);

if (!IS_SERVERLESS) {
  const server = http.createServer(requestHandler);

  async function shutdown() {
    await disconnectMcp();
    process.exit(0);
  }

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  server.listen(PORT, async () => {
  console.log(`Apt dashboard server listening on http://localhost:${PORT}`);
  console.log(`Primary data source: ${SOURCE} (stdio)`);
  console.log(`[dataset:stations] ${loadStationDataset().message}`);
  console.log(`[dataset:apt-coords] ${loadApartmentCoordinateDataset().message}`);
  if (!API_KEY) {
    console.warn("DATA_GO_KR_API_KEY is not configured. MCP and building lookups will fail.");
  }

  try {
    await ensureMcpConnected();
    console.log("real-estate-mcp connected successfully.");
  } catch (error) {
    console.warn(`real-estate-mcp not connected yet: ${error.message}`);
  }

  // 서울 건축물대장 벌크 로드 (백그라운드, 서버 시작을 블로킹하지 않음)
  if (SEOUL_BUILDING_API_KEY) {
    loadSeoulBuildingData().catch((err) =>
      console.error(`[seoul-building] background load failed: ${err.message}`)
    );
  }

  // 서울 부동산 중개업소 벌크 로드
  if (SEOUL_REALESTATE_API_KEY) {
    loadSeoulBrokerData().catch((err) =>
      console.error(`[seoul-broker] background load failed: ${err.message}`)
    );
  }
});
} // end if (!IS_SERVERLESS)

// ── Serverless: 데이터셋은 첫 요청 시 로드 ──
if (IS_SERVERLESS) {
  loadStationDataset();
  loadApartmentCoordinateDataset();
}

// ── Vercel Serverless export ──
if (typeof module !== "undefined" && module.exports) {
  module.exports = requestHandler;
}
