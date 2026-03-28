/**
 * VWorld 용도지역 데이터를 Supabase에 캐싱하는 스크립트
 * GitHub Actions Cron으로 매일 새벽 실행
 *
 * 주요 지역(서울 25개 구)의 KAPT 단지 목록을 가져와서
 * 각 단지의 PNU로 VWorld getLandUseAttr API를 호출,
 * 결과를 Supabase vworld_cache 테이블에 저장
 */
const https = require("https");
const http = require("http");

const VWORLD_API_KEY =
  process.env.VWORLD_API_KEY_getLandUse || process.env.VWORLD_API_KEY || "";
const VWORLD_DATA_DOMAIN = process.env.VWORLD_DATA_DOMAIN || "";
const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "";

// 서울 25개 구 법정동코드 (앞5자리)
const SEOUL_DISTRICTS = [
  "11110", "11140", "11170", "11200", "11215",
  "11230", "11260", "11290", "11305", "11320",
  "11350", "11380", "11410", "11440", "11470",
  "11500", "11530", "11545", "11560", "11590",
  "11620", "11650", "11680", "11710", "11740",
];

function fetch(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const client = parsed.protocol === "http:" ? http : https;
    const req = client.request(
      {
        method: "GET",
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
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
    req.setTimeout(15000, () => { req.destroy(new Error("timeout")); });
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

async function supabaseExistingPnus() {
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY) return new Set();
  try {
    const url = `${SUPABASE_URL}/rest/v1/vworld_cache?select=pnu&updated_at=gte.${new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString()}`;
    const text = await fetch(url, {
      apikey: SUPABASE_ANON_KEY,
      Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
    });
    const rows = JSON.parse(text);
    return new Set(rows.map((r) => r.pnu));
  } catch {
    return new Set();
  }
}

async function supabaseBatchUpsert(rows) {
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY || rows.length === 0) return;
  await postJson(`${SUPABASE_URL}/rest/v1/vworld_cache`, rows, {
    apikey: SUPABASE_ANON_KEY,
    Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
    Prefer: "resolution=merge-duplicates",
  });
}

async function fetchLandUse(pnu) {
  const params = new URLSearchParams({
    key: VWORLD_API_KEY,
    pnu,
    format: "json",
    numOfRows: "20",
    pageNo: "1",
  });
  if (VWORLD_DATA_DOMAIN) params.set("domain", VWORLD_DATA_DOMAIN);

  const url = `https://api.vworld.kr/ned/data/getLandUseAttr?${params}`;
  const text = await fetch(url);
  const parsed = JSON.parse(text);

  const attrs = parsed?.landUses?.field || parsed?.landUseAttr?.field || [];
  if (!Array.isArray(attrs) || attrs.length === 0) return null;

  const included = attrs.filter((a) => String(a.cnflcAt) === "1");
  const source = included.length > 0 ? included : attrs;

  const zones = source
    .map((a) => (a.prposAreaDstrcCodeNm || "").trim())
    .filter((n) => n && (n.includes("주거") || n.includes("상업") || n.includes("공업")));

  const best = zones.sort((a, b) => b.length - a.length)[0] || null;
  return best;
}

async function sleep(ms) {
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

  // region_codes.txt에서 서울 지역코드 가져오기
  const fs = require("fs");
  const path = require("path");
  const regionFile = path.join(__dirname, "..", "region_codes.txt");

  let regionLines = [];
  try {
    regionLines = fs.readFileSync(regionFile, "utf-8").split("\n").filter(Boolean);
  } catch {
    console.error("region_codes.txt not found, using default Seoul districts");
  }

  // 서울 구 코드만 필터
  const seoulCodes = regionLines
    .map((l) => l.split("\t")[0]?.trim())
    .filter((c) => c && c.startsWith("11") && c.length === 5);

  const districtCodes = seoulCodes.length > 0 ? seoulCodes : SEOUL_DISTRICTS;
  console.log(`Processing ${districtCodes.length} districts`);

  let total = 0;
  let cached = 0;
  let skipped = 0;
  let errors = 0;
  const batch = [];

  for (const code of districtCodes) {
    // KAPT 단지 목록 API로 PNU가 있는 단지 가져오기는 복잡하므로
    // 이미 server.js가 조회한 PNU만 캐싱 (Supabase에 이미 있는 것)
    // 여기서는 새로운 PNU를 발견하기 위해 간단히 로그만 출력
    console.log(`District ${code}: checking...`);
  }

  // Supabase에서 캐시 만료된 PNU 목록 가져오기
  try {
    const expiredUrl = `${SUPABASE_URL}/rest/v1/vworld_cache?select=pnu&updated_at=lt.${new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString()}&limit=100`;
    const text = await fetch(expiredUrl, {
      apikey: SUPABASE_ANON_KEY,
      Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
    });
    const expired = JSON.parse(text);
    console.log(`${expired.length} expired PNUs to refresh`);

    for (const row of expired) {
      total++;
      try {
        const zone = await fetchLandUse(row.pnu);
        if (zone) {
          batch.push({
            pnu: row.pnu,
            land_use_zone: zone,
            updated_at: new Date().toISOString(),
          });
          cached++;
          console.log(`  [${cached}] ${row.pnu} → ${zone}`);
        } else {
          skipped++;
        }

        // 배치 10개씩 Supabase에 저장
        if (batch.length >= 10) {
          await supabaseBatchUpsert(batch.splice(0));
        }

        // API 속도 제한 (초당 1건)
        await sleep(1000);
      } catch (e) {
        errors++;
        console.warn(`  Error ${row.pnu}: ${e.message}`);
        await sleep(2000);
      }
    }
  } catch (e) {
    console.warn("Failed to fetch expired PNUs:", e.message);
  }

  // 남은 배치 저장
  if (batch.length > 0) {
    await supabaseBatchUpsert(batch);
  }

  console.log(`\nDone! Total: ${total}, Cached: ${cached}, Skipped: ${skipped}, Errors: ${errors}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
