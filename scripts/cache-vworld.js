/**
 * VWorld 용도지역 데이터를 Supabase에 캐싱하는 스크립트
 * GitHub Actions Cron으로 매일 새벽 실행, 또는 수동 실행
 *
 * 법정동코드(10자리) 단위로 VWorld getLandUseAttr API를 호출하여
 * 해당 동의 모든 PNU 용도지역을 한번에 가져와 Supabase에 저장
 */
const https = require("https");
const http = require("http");
const crypto = require("crypto");

const VWORLD_API_KEY =
  process.env.VWORLD_API_KEY_getLandUse || process.env.VWORLD_API_KEY || "";
const VWORLD_DATA_DOMAIN = process.env.VWORLD_DATA_DOMAIN || "";
const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "";

// VWorld TLS renegotiation 대응
const vworldAgent = new https.Agent({
  rejectUnauthorized: true,
  secureOptions: crypto.constants.SSL_OP_LEGACY_SERVER_CONNECT,
});

// 서울 25개 구의 주요 법정동코드 (10자리)
// 시군구코드(5) + 법정동코드(5)
const SEOUL_DONG_CODES = [
  // 강남구
  "1168010100", "1168010300", "1168010600", "1168010700", "1168010800",
  "1168010900", "1168011000", "1168011100", "1168011200", "1168011300",
  "1168011400", "1168011500", "1168011700", "1168011800", "1168012000",
  "1168012100", "1168012200",
  // 서초구
  "1165010100", "1165010200", "1165010300", "1165010400", "1165010500",
  "1165010600", "1165010700", "1165010800", "1165010900", "1165011000",
  // 송파구
  "1171010100", "1171010200", "1171010300", "1171010400", "1171010500",
  "1171010600", "1171010700", "1171010800", "1171011200", "1171011300",
  "1171011400", "1171011500", "1171011600", "1171011700", "1171011800",
  "1171011900", "1171012000", "1171012100", "1171012200", "1171012300",
  "1171012400", "1171012500", "1171012600", "1171012700",
  // 강동구
  "1174010100", "1174010200", "1174010300", "1174010400", "1174010500",
  "1174010600", "1174010700", "1174010800", "1174010900", "1174011000",
  "1174011100", "1174011200", "1174011300", "1174011500", "1174011600",
  "1174011700", "1174011800",
  // 마포구
  "1144010100", "1144010200", "1144010300", "1144010400", "1144010500",
  "1144010600", "1144010700", "1144010800", "1144010900", "1144011000",
  "1144011100", "1144011200", "1144011300", "1144011400", "1144011500",
  "1144011600", "1144011700", "1144011800", "1144011900", "1144012000",
  "1144012100", "1144012200", "1144012300",
  // 용산구
  "1117010100", "1117010200", "1117010300", "1117010400", "1117010500",
  "1117010600", "1117010700", "1117010800", "1117010900", "1117011000",
  "1117011100", "1117011200", "1117011300", "1117011400", "1117011500",
  "1117011600", "1117011700", "1117011800", "1117011900", "1117012000",
  // 성동구
  "1120010100", "1120010200", "1120010300", "1120010400", "1120010500",
  "1120010600", "1120010700", "1120010800", "1120010900", "1120011000",
  "1120011100", "1120011200", "1120011300", "1120011400", "1120011500",
  "1120011600", "1120011700",
  // 영등포구
  "1156010100", "1156010200", "1156010300", "1156010400", "1156010500",
  "1156010600", "1156010700", "1156010800", "1156010900", "1156011000",
  "1156011100", "1156011200", "1156011300", "1156011400", "1156011500",
  "1156011600", "1156011700", "1156011800",
  // 동작구
  "1159010100", "1159010200", "1159010300", "1159010400", "1159010500",
  "1159010600", "1159010700", "1159010800", "1159010900",
  // 노원구
  "1135010100", "1135010200", "1135010300", "1135010400", "1135010500",
  "1135010600", "1135010700", "1135010800", "1135010900", "1135011000",
  "1135011100", "1135011200", "1135011300",
];

function fetchUrl(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const isVworld = parsed.hostname.endsWith("vworld.kr");
    const client = parsed.protocol === "http:" ? http : https;
    const req = client.request(
      {
        method: "GET",
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        ...(isVworld ? { agent: vworldAgent } : {}),
        headers: { "User-Agent": "apt-cache-worker/1.0", ...headers },
      },
      (res) => {
        let data = "";
        res.on("data", (c) => (data += c));
        res.on("end", () => {
          if (res.statusCode >= 400) reject(new Error(`HTTP ${res.statusCode}: ${data.slice(0, 200)}`));
          else resolve(data);
        });
      },
    );
    req.on("error", reject);
    req.setTimeout(30000, () => { req.destroy(new Error("timeout")); });
    req.end();
  });
}

function postJson(url, body, headers = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const jsonBody = JSON.stringify(body);
    const req = https.request(
      {
        method: "POST",
        hostname: parsed.hostname,
        path: parsed.pathname,
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(jsonBody),
          ...headers,
        },
      },
      (res) => {
        let data = "";
        res.on("data", (c) => (data += c));
        res.on("end", () => {
          if (res.statusCode >= 400) reject(new Error(`HTTP ${res.statusCode}: ${data.slice(0, 200)}`));
          else resolve(data);
        });
      },
    );
    req.on("error", reject);
    req.end(jsonBody);
  });
}

async function supabaseBatchUpsert(rows) {
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY || rows.length === 0) return;
  await postJson(`${SUPABASE_URL}/rest/v1/vworld_cache`, rows, {
    apikey: SUPABASE_ANON_KEY,
    Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
    Prefer: "resolution=merge-duplicates",
  });
}

async function supabaseExistingPnus() {
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY) return new Set();
  try {
    // 7일 이내 캐시된 PNU 목록
    const url = `${SUPABASE_URL}/rest/v1/vworld_cache?select=pnu&updated_at=gte.${new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString()}&limit=10000`;
    const text = await fetchUrl(url, {
      apikey: SUPABASE_ANON_KEY,
      Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
    });
    const rows = JSON.parse(text);
    return new Set(rows.map((r) => r.pnu));
  } catch {
    return new Set();
  }
}

// 법정동코드(10자리)로 해당 동의 모든 용도지역 데이터를 가져옴
async function fetchDongLandUse(dongCode) {
  const allAttrs = [];
  let pageNo = 1;
  const maxPages = 20;

  while (pageNo <= maxPages) {
    const params = new URLSearchParams({
      key: VWORLD_API_KEY,
      pnu: dongCode,
      format: "json",
      numOfRows: "100",
      pageNo: String(pageNo),
    });
    if (VWORLD_DATA_DOMAIN) params.set("domain", VWORLD_DATA_DOMAIN);

    const url = `https://api.vworld.kr/ned/data/getLandUseAttr?${params}`;
    const text = await fetchUrl(url);
    const parsed = JSON.parse(text);

    const resultCode = parsed?.landUses?.resultCode || "";
    if (resultCode === "INCORRECT_KEY") {
      throw new Error("INCORRECT_KEY");
    }

    const attrs = parsed?.landUses?.field || [];
    if (!Array.isArray(attrs) || attrs.length === 0) break;

    allAttrs.push(...attrs);

    const totalCount = Number(parsed?.landUses?.totalCount || 0);
    if (pageNo * 100 >= totalCount) break;
    pageNo++;
  }

  return allAttrs;
}

// 속성 배열에서 PNU별 용도지역을 추출
function extractZoningByPnu(attrs) {
  const pnuMap = new Map();

  for (const attr of attrs) {
    const pnu = String(attr.pnu || "").trim();
    const zone = (attr.prposAreaDstrcCodeNm || "").trim();
    const cnflcAt = String(attr.cnflcAt || "");

    if (!pnu || !zone) continue;

    if (!pnuMap.has(pnu)) {
      pnuMap.set(pnu, { included: [], other: [] });
    }
    const entry = pnuMap.get(pnu);
    if (cnflcAt === "1") entry.included.push(zone);
    else entry.other.push(zone);
  }

  const result = new Map();
  for (const [pnu, { included, other }] of pnuMap) {
    const source = included.length > 0 ? included : other;
    const residentialZones = source.filter(
      (z) => z.includes("주거") || z.includes("상업") || z.includes("공업"),
    );
    const best = residentialZones.sort((a, b) => b.length - a.length)[0] || null;
    if (best) result.set(pnu, best);
  }

  return result;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  if (!VWORLD_API_KEY) {
    console.error("VWORLD_API_KEY_getLandUse not set");
    process.exit(1);
  }
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
    console.error("SUPABASE_URL or SUPABASE_ANON_KEY not set");
    process.exit(1);
  }

  console.log("Loading existing cache...");
  const existing = await supabaseExistingPnus();
  console.log(`${existing.size} PNUs already cached (within 7 days)`);

  let totalDongs = 0;
  let totalPnus = 0;
  let newCached = 0;
  let skippedExisting = 0;
  let errors = 0;
  let batch = [];

  for (const dongCode of SEOUL_DONG_CODES) {
    totalDongs++;
    console.log(`[${totalDongs}/${SEOUL_DONG_CODES.length}] 법정동 ${dongCode} 처리중...`);

    try {
      const attrs = await fetchDongLandUse(dongCode);
      const zoningMap = extractZoningByPnu(attrs);

      for (const [pnu, zone] of zoningMap) {
        totalPnus++;
        if (existing.has(pnu)) {
          skippedExisting++;
          continue;
        }

        batch.push({
          pnu,
          land_use_zone: zone,
          updated_at: new Date().toISOString(),
        });
        newCached++;

        // 50개씩 배치 저장
        if (batch.length >= 50) {
          await supabaseBatchUpsert(batch);
          console.log(`  → ${newCached}건 저장됨`);
          batch = [];
        }
      }

      console.log(`  ${zoningMap.size} PNUs found, ${batch.length} pending`);
    } catch (e) {
      errors++;
      console.warn(`  Error: ${e.message}`);
      if (e.message === "INCORRECT_KEY") {
        console.error("API key is invalid. Exiting.");
        process.exit(1);
      }
    }

    // VWorld API 속도 제한 (동 단위 요청 간 2초 대기)
    await sleep(2000);
  }

  // 남은 배치 저장
  if (batch.length > 0) {
    await supabaseBatchUpsert(batch);
  }

  console.log(`\n=== 완료 ===`);
  console.log(`처리 동: ${totalDongs}`);
  console.log(`전체 PNU: ${totalPnus}`);
  console.log(`신규 캐시: ${newCached}`);
  console.log(`기존 스킵: ${skippedExisting}`);
  console.log(`에러: ${errors}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
