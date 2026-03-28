const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const ENV_PATH = path.join(ROOT, ".env");
const REGION_CODES_PATH = path.join(ROOT, "data", "region_codes.txt");
const OUTPUT_PATH = path.join(
  ROOT,
  "data",
  "generated",
  "apartment-coordinate-index.json",
);

function loadEnvFile(filePath) {
  try {
    const text = fs.readFileSync(filePath, "utf8");
    return text.split(/\r?\n/).reduce((acc, rawLine) => {
      const line = String(rawLine || "").trim();
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

const ENV = loadEnvFile(ENV_PATH);
const DATA_GO_KR_API_KEY =
  ENV.DATA_GO_KR_API_KEY || ENV.MOLIT_API_KEY || process.env.DATA_GO_KR_API_KEY || "";
const VWORLD_API_KEY = ENV.VWORLD_API_KEY || process.env.VWORLD_API_KEY || "";
const VWORLD_DATA_DOMAIN =
  ENV.VWORLD_DATA_DOMAIN || process.env.VWORLD_DATA_DOMAIN || "";

function parseArgs(argv) {
  const args = {
    regionPrefix: "",
    limit: 0,
    concurrency: 5,
    out: OUTPUT_PATH,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const current = argv[i];
    if (current === "--region-prefix") {
      args.regionPrefix = String(argv[i + 1] || "").trim();
      i += 1;
      continue;
    }
    if (current === "--limit") {
      args.limit = Number.parseInt(String(argv[i + 1] || "0"), 10) || 0;
      i += 1;
      continue;
    }
    if (current === "--concurrency") {
      args.concurrency = Math.max(
        1,
        Number.parseInt(String(argv[i + 1] || "5"), 10) || 5,
      );
      i += 1;
      continue;
    }
    if (current === "--out") {
      args.out = path.resolve(ROOT, String(argv[i + 1] || OUTPUT_PATH));
      i += 1;
    }
  }

  return args;
}

function normalizeText(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/[^\p{L}\p{N}]/gu, "");
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
  const jiKey = normalizeParcelPart(ji) || "0";
  if (!dongKey || !bunKey) {
    return "";
  }
  return `${dongKey}|${bunKey}|${jiKey}`;
}

function extractDongFromAddress(value) {
  return (
    String(value || "")
      .split(/\s+/)
      .find((token) => /(동|읍|면|리)$/.test(token)) || ""
  );
}

function parseRegionCodes(filePath) {
  const text = fs.readFileSync(filePath, "utf8");
  const lines = text.split(/\r?\n/);
  const sigunguCodes = [];
  const seen = new Set();

  for (const line of lines) {
    const codeMatch = String(line || "").match(/^(\d{10})/);
    if (!codeMatch) {
      continue;
    }
    const code = codeMatch[1];
    if (!code.endsWith("00000")) {
      continue;
    }
    if (code.slice(2, 5) === "000") {
      continue;
    }
    const sigunguCode = code.slice(0, 5);
    if (!seen.has(sigunguCode)) {
      seen.add(sigunguCode);
      sigunguCodes.push(sigunguCode);
    }
  }

  return sigunguCodes;
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, {
    method: "GET",
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
      Accept: "application/json, text/plain, */*",
      ...(options.headers || {}),
    },
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} for ${url}`);
  }
  return JSON.parse(text);
}

function toArray(value) {
  if (Array.isArray(value)) return value;
  if (value == null) return [];
  return [value];
}

async function fetchKaptList(sigunguCode) {
  const items = [];
  const seenCodes = new Set();
  let pageNo = 1;
  const numOfRows = 1000;

  while (pageNo <= 20) {
    const params = new URLSearchParams({
      serviceKey: DATA_GO_KR_API_KEY,
      sigunguCode,
      pageNo: String(pageNo),
      numOfRows: String(numOfRows),
      _type: "json",
    });

    const payload = await fetchJson(
      `https://apis.data.go.kr/1613000/AptListService3/getSigunguAptList3?${params.toString()}`,
    );
    const body = payload?.response?.body || {};
    const pageItems = toArray(body?.items || []);
    for (const item of pageItems) {
      const kaptCode = String(item?.kaptCode || "").trim();
      if (kaptCode && !seenCodes.has(kaptCode)) {
        seenCodes.add(kaptCode);
        items.push({
          kaptCode,
          kaptName: String(item?.kaptName || "").trim(),
          bjdCode: String(item?.bjdCode || "").trim(),
          as1: String(item?.as1 || "").trim(),
          as2: String(item?.as2 || "").trim(),
          as3: String(item?.as3 || "").trim(),
          as4: String(item?.as4 || "").trim(),
        });
      }
    }

    if (pageItems.length < numOfRows) {
      break;
    }
    pageNo += 1;
  }

  return items;
}

async function fetchKaptBasicInfo(kaptCode) {
  const params = new URLSearchParams({
    serviceKey: DATA_GO_KR_API_KEY,
    kaptCode,
    _type: "json",
  });

  const payload = await fetchJson(
    `https://apis.data.go.kr/1613000/AptBasisInfoServiceV4/getAphusBassInfoV4?${params.toString()}`,
  );
  return payload?.response?.body?.item || null;
}

function stripComplexNameFromJibunAddress(address, aptName) {
  const raw = String(address || "").trim();
  if (!raw) {
    return "";
  }

  const trimmedByPattern =
    raw.match(/^(.*?(?:동|읍|면|리)\s+\d+(?:-\d+)?)/)?.[1]?.trim() || "";
  if (trimmedByPattern) {
    return trimmedByPattern;
  }

  const normalizedAptName = normalizeText(aptName);
  if (normalizedAptName) {
    const rawTokens = raw.split(/\s+/);
    while (rawTokens.length > 0) {
      const candidate = rawTokens.join(" ");
      if (!normalizeText(candidate).endsWith(normalizedAptName)) {
        return candidate;
      }
      rawTokens.pop();
    }
  }

  return raw;
}

function buildAddressCandidates(info, fallback = {}) {
  const aptName = String(info?.kaptName || fallback?.kaptName || "").trim();
  const kaptAddr = String(info?.kaptAddr || "").trim();
  const roadAddress = String(info?.doroJuso || "").trim();
  const baseArea = [
    String(fallback?.as1 || "").trim(),
    String(fallback?.as2 || "").trim(),
    String(fallback?.as3 || "").trim(),
    String(fallback?.as4 || "").trim(),
  ]
    .filter(Boolean)
    .join(" ")
    .trim();

  const candidates = [
    stripComplexNameFromJibunAddress(kaptAddr, aptName),
    kaptAddr,
    `${baseArea} ${aptName}`.trim(),
  ].filter(Boolean);

  return [...new Set(candidates)];
}

function buildRoadCandidates(info, fallback = {}) {
  const roadAddress = String(info?.doroJuso || "").trim();
  const aptName = String(info?.kaptName || fallback?.kaptName || "").trim();
  const candidates = [
    roadAddress,
    roadAddress && aptName ? `${roadAddress} ${aptName}` : "",
  ].filter(Boolean);
  return [...new Set(candidates)];
}

function parsePnuToParcel(pnu) {
  const digits = String(pnu || "").replace(/\D/g, "");
  if (digits.length !== 19) {
    return {
      bun: "",
      ji: "",
    };
  }
  return {
    bun: normalizeParcelPart(digits.slice(11, 15)),
    ji: normalizeParcelPart(digits.slice(15, 19)),
  };
}

async function geocodeWithVworldAddress(address, type) {
  const params = new URLSearchParams({
    service: "address",
    request: "getcoord",
    version: "2.0",
    crs: "epsg:4326",
    type,
    format: "json",
    key: VWORLD_API_KEY,
    address,
  });
  if (VWORLD_DATA_DOMAIN) {
    params.set("domain", VWORLD_DATA_DOMAIN);
  }

  const payload = await fetchJson(
    `https://api.vworld.kr/req/address?${params.toString()}`,
  );
  const status = String(payload?.response?.status || "").trim().toUpperCase();
  if (status !== "OK") {
    return null;
  }

  const point = payload?.response?.result?.point || {};
  const lng = Number.parseFloat(String(point?.x || ""));
  const lat = Number.parseFloat(String(point?.y || ""));
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
    return null;
  }

  const refined = payload?.response?.refined || {};
  const structure = refined?.structure || {};
  const parcel = parsePnuToParcel(structure?.level4LC || "");

  return {
    lat,
    lng,
    address: String(refined?.text || address).trim(),
    dong: String(structure?.level4L || "").trim(),
    bun: parcel.bun,
    ji: parcel.ji,
  };
}

async function resolveCoordinateEntry(basicInfo, fallbackInfo) {
  const aptName = String(
    basicInfo?.kaptName || fallbackInfo?.kaptName || "",
  ).trim();
  if (!aptName) {
    return null;
  }

  for (const address of buildAddressCandidates(basicInfo, fallbackInfo)) {
    try {
      const resolved = await geocodeWithVworldAddress(address, "PARCEL");
      if (resolved) {
        const dong = resolved.dong || extractDongFromAddress(resolved.address);
        return {
          aptName,
          dong,
          address: resolved.address,
          roadAddress: String(basicInfo?.doroJuso || "").trim(),
          lat: Number(resolved.lat.toFixed(7)),
          lng: Number(resolved.lng.toFixed(7)),
          bun: resolved.bun,
          ji: resolved.ji,
          parcelKey: buildParcelKey(dong, resolved.bun, resolved.ji),
        };
      }
    } catch {
      // Try next candidate.
    }
  }

  for (const roadAddress of buildRoadCandidates(basicInfo, fallbackInfo)) {
    try {
      const resolved = await geocodeWithVworldAddress(roadAddress, "ROAD");
      if (resolved) {
        const dong =
          extractDongFromAddress(String(basicInfo?.kaptAddr || "")) ||
          extractDongFromAddress(resolved.address);
        return {
          aptName,
          dong,
          address: String(basicInfo?.kaptAddr || resolved.address).trim(),
          roadAddress,
          lat: Number(resolved.lat.toFixed(7)),
          lng: Number(resolved.lng.toFixed(7)),
          bun: "",
          ji: "",
          parcelKey: "",
        };
      }
    } catch {
      // Try next candidate.
    }
  }

  return null;
}

function uniqueEntries(items) {
  const seen = new Set();
  const results = [];

  for (const item of items) {
    const key = [
      normalizeText(item?.aptName),
      normalizeText(item?.dong),
      String(item?.parcelKey || ""),
      `${item?.lat}:${item?.lng}`,
    ].join("|");
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    results.push(item);
  }

  return results;
}

async function asyncMapPool(items, concurrency, worker) {
  const results = new Array(items.length);
  let index = 0;

  async function runWorker() {
    while (true) {
      const current = index;
      index += 1;
      if (current >= items.length) {
        return;
      }
      results[current] = await worker(items[current], current);
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(concurrency, items.length || 1) }, () =>
      runWorker(),
    ),
  );

  return results;
}

async function main() {
  if (!DATA_GO_KR_API_KEY) {
    throw new Error("DATA_GO_KR_API_KEY or MOLIT_API_KEY is required.");
  }
  if (!VWORLD_API_KEY) {
    throw new Error("VWORLD_API_KEY is required.");
  }

  const args = parseArgs(process.argv.slice(2));
  const sigunguCodes = parseRegionCodes(REGION_CODES_PATH).filter((code) =>
    args.regionPrefix ? code.startsWith(args.regionPrefix) : true,
  );

  console.log(
    `[build:apt-coords-live] collecting KAPT codes for ${sigunguCodes.length} regions`,
  );

  const kaptMap = new Map();
  for (const sigunguCode of sigunguCodes) {
    const items = await fetchKaptList(sigunguCode);
    items.forEach((item) => {
      if (!kaptMap.has(item.kaptCode)) {
        kaptMap.set(item.kaptCode, item);
      }
    });
    console.log(
      `[build:apt-coords-live] ${sigunguCode}: ${items.length} complexes`,
    );
  }

  let complexes = [...kaptMap.values()];
  if (args.limit > 0) {
    complexes = complexes.slice(0, args.limit);
  }

  console.log(
    `[build:apt-coords-live] resolving coordinates for ${complexes.length} complexes`,
  );

  let successCount = 0;
  let failureCount = 0;
  const results = await asyncMapPool(
    complexes,
    args.concurrency,
    async (item, index) => {
      try {
        const basicInfo = await fetchKaptBasicInfo(item.kaptCode);
        const entry = await resolveCoordinateEntry(basicInfo, item);
        if (entry) {
          successCount += 1;
        } else {
          failureCount += 1;
        }
        if ((index + 1) % 100 === 0 || index + 1 === complexes.length) {
          console.log(
            `[build:apt-coords-live] progress ${index + 1}/${complexes.length} success=${successCount} fail=${failureCount}`,
          );
        }
        return entry;
      } catch (error) {
        failureCount += 1;
        console.warn(
          `[build:apt-coords-live] failed ${item.kaptCode} ${item.kaptName}: ${error.message}`,
        );
        return null;
      }
    },
  );

  const unique = uniqueEntries(results.filter(Boolean));
  fs.mkdirSync(path.dirname(args.out), { recursive: true });
  fs.writeFileSync(
    args.out,
    JSON.stringify(
      {
        sourceFiles: ["KAPT live basic info", "VWorld address geocoding"],
        count: unique.length,
        items: unique,
      },
      null,
      2,
    ),
    "utf8",
  );

  console.log(
    `[build:apt-coords-live] saved ${unique.length} coordinates to ${args.out}`,
  );
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});
