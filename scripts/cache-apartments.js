/**
 * 아파트 enrichment 데이터를 Supabase에 캐싱하는 스크립트
 * 로컬 PC에서 실행 (한국 IP → VWorld 정상 작동)
 *
 * 1) data.go.kr에서 최근 거래 데이터로 아파트 목록 수집
 * 2) Kakao 지오코딩으로 좌표 획득
 * 3) 근처 역 계산 (subway-stations.json)
 * 4) VWorld에서 용도지역 조회
 * 5) Supabase에 저장
 */
const https = require("https");
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

// .env 로드
const envPath = path.join(__dirname, "..", ".env");
const envLines = fs.readFileSync(envPath, "utf-8").split("\n");
for (const line of envLines) {
  const m = line.match(/^([^#=]+)=(.+)/);
  if (m) process.env[m[1].trim()] = m[2].trim();
}

const DATA_GO_KR_KEY = process.env.DATA_GO_KR_API_KEY || process.env.MOLIT_API_KEY || "";
const KAKAO_KEY = process.env.KAKAO_REST_API_KEY || "";
const VWORLD_KEY = process.env.VWORLD_API_KEY_getLandUse || "";
const VWORLD_DOMAIN = process.env.VWORLD_DATA_DOMAIN || "";
const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_KEY = process.env.SUPABASE_ANON_KEY || "";

// VWorld TLS
const vworldAgent = new https.Agent({
  rejectUnauthorized: true,
  secureOptions: crypto.constants.SSL_OP_LEGACY_SERVER_CONNECT,
});

// 역 데이터 로드
let stations = [];
const stationsPath = path.join(__dirname, "..", "data", "generated", "subway-stations.json");
const bundledPath = path.join(__dirname, "..", "subway-stations.json");
try {
  stations = JSON.parse(fs.readFileSync(fs.existsSync(stationsPath) ? stationsPath : bundledPath, "utf-8"));
  console.log(`역 데이터: ${stations.length}개 로드`);
} catch { console.warn("역 데이터 로드 실패"); }

// 서울 25개 구 코드
const REGION_CODES = [
  "11110","11140","11170","11200","11215","11230","11260","11290","11305","11320",
  "11350","11380","11410","11440","11470","11500","11530","11545","11560","11590",
  "11620","11650","11680","11710","11740",
];

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function fetchUrl(url, headers = {}, opts = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const isVworld = parsed.hostname.endsWith("vworld.kr");
    const client = parsed.protocol === "http:" ? http : https;
    const req = client.request({
      method: opts.method || "GET",
      hostname: parsed.hostname,
      path: parsed.pathname + parsed.search,
      ...(isVworld ? { agent: vworldAgent } : {}),
      headers: { "User-Agent": "apt-cache/1.0", ...headers },
    }, res => {
      let d = ""; res.on("data", c => d += c);
      res.on("end", () => res.statusCode >= 400 ? reject(new Error(`HTTP ${res.statusCode}`)) : resolve(d));
    });
    req.on("error", reject);
    req.setTimeout(30000, () => { req.destroy(new Error("timeout")); });
    req.end(opts.body || undefined);
  });
}

function postJson(url, body, headers = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const jsonBody = JSON.stringify(body);
    const req = https.request({
      method: "POST", hostname: parsed.hostname, path: parsed.pathname,
      headers: { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(jsonBody), ...headers },
    }, res => {
      let d = ""; res.on("data", c => d += c);
      res.on("end", () => res.statusCode >= 400 ? reject(new Error(`HTTP ${res.statusCode}: ${d.slice(0,200)}`)) : resolve(d));
    });
    req.on("error", reject); req.end(jsonBody);
  });
}

// XML 파싱 헬퍼
function getTag(xml, tag) {
  const m = xml.match(new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`));
  return m ? m[1].trim() : "";
}

function extractItems(xml) {
  const items = [];
  let idx = 0;
  while (true) {
    const start = xml.indexOf("<item>", idx);
    if (start === -1) break;
    const end = xml.indexOf("</item>", start);
    if (end === -1) break;
    items.push(xml.slice(start + 6, end));
    idx = end + 7;
  }
  return items;
}

// data.go.kr 아파트 거래 목록 가져오기
async function fetchTradeApts(regionCode, yearMonth) {
  const url = `https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev?serviceKey=${encodeURIComponent(DATA_GO_KR_KEY)}&LAWD_CD=${regionCode}&DEAL_YMD=${yearMonth}&numOfRows=1000&pageNo=1`;
  const xml = await fetchUrl(url);
  const items = extractItems(xml);
  const aptMap = new Map();

  for (const item of items) {
    const apt = getTag(item, "aptNm") || "";
    const dong = getTag(item, "umdNm") || "";
    const sigunguCd = getTag(item, "sggCd") || "";
    const bjdongCd = getTag(item, "umdCd") || "";
    const bun = getTag(item, "bonbun") || getTag(item, "jibun").split("-")[0] || "";
    const ji = getTag(item, "bubun") || (getTag(item, "jibun").split("-")[1] || "");
    if (!apt) continue;

    const key = `${regionCode}-${dong}-${apt}`;
    if (!aptMap.has(key)) {
      aptMap.set(key, { apt, dong, sigunguCd, bjdongCd, bun: bun.replace(/\D/g, ""), ji: ji.replace(/\D/g, ""), regionCode });
    }
  }
  return [...aptMap.values()];
}

// Kakao 지오코딩
async function kakaoGeocode(address) {
  if (!KAKAO_KEY) return null;
  try {
    const url = `https://dapi.kakao.com/v2/local/search/address.json?query=${encodeURIComponent(address)}&size=1`;
    const text = await fetchUrl(url, { Authorization: `KakaoAK ${KAKAO_KEY}` });
    const doc = JSON.parse(text)?.documents?.[0];
    if (!doc) return null;
    return { lat: parseFloat(doc.y), lng: parseFloat(doc.x) };
  } catch { return null; }
}

// 근처 역 계산 (haversine)
function findNearestStation(lat, lng) {
  if (!stations.length || !lat || !lng) return null;
  let best = null, bestDist = Infinity;
  for (const s of stations) {
    const sLat = parseFloat(s.lat), sLng = parseFloat(s.lng);
    if (!sLat || !sLng) continue;
    const dLat = (sLat - lat) * Math.PI / 180;
    const dLng = (sLng - lng) * Math.PI / 180;
    const a = Math.sin(dLat/2)**2 + Math.cos(lat*Math.PI/180)*Math.cos(sLat*Math.PI/180)*Math.sin(dLng/2)**2;
    const dist = 6371 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    if (dist < bestDist) { bestDist = dist; best = s; }
  }
  if (!best) return null;
  const label = best.line ? `${best.line} ${best.name}` : best.name;
  return { name: label, distanceKm: Math.round(bestDist * 100) / 100 };
}

// VWorld 용도지역 (법정동코드 10자리)
async function fetchZoning(sigunguCd, bjdongCd) {
  if (!VWORLD_KEY || !sigunguCd || !bjdongCd) return null;
  const dongCode = sigunguCd + bjdongCd;
  if (!/^\d{10}$/.test(dongCode)) return null;
  try {
    const params = new URLSearchParams({ key: VWORLD_KEY, pnu: dongCode, format: "json", numOfRows: "100", pageNo: "1" });
    if (VWORLD_DOMAIN) params.set("domain", VWORLD_DOMAIN);
    const text = await fetchUrl(`https://api.vworld.kr/ned/data/getLandUseAttr?${params}`);
    const attrs = JSON.parse(text)?.landUses?.field || [];
    const included = attrs.filter(a => String(a.cnflcAt) === "1");
    const src = included.length > 0 ? included : attrs;
    const zones = src.map(a => (a.prposAreaDstrcCodeNm || "").trim())
      .filter(z => z.includes("주거") || z.includes("상업") || z.includes("공업"));
    return zones.sort((a, b) => b.length - a.length)[0] || null;
  } catch { return null; }
}

// Supabase 저장
async function upsertBatch(rows) {
  if (!rows.length) return;
  await postJson(`${SUPABASE_URL}/rest/v1/apartment_enrichment`, rows, {
    apikey: SUPABASE_KEY,
    Authorization: `Bearer ${SUPABASE_KEY}`,
    Prefer: "resolution=merge-duplicates",
  });
}

// 기존 캐시 확인
async function getExistingIds() {
  try {
    const url = `${SUPABASE_URL}/rest/v1/apartment_enrichment?select=id&updated_at=gte.${new Date(Date.now() - 7*24*60*60*1000).toISOString()}&limit=10000`;
    const text = await fetchUrl(url, { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` });
    return new Set(JSON.parse(text).map(r => r.id));
  } catch { return new Set(); }
}

// 주소 생성
function buildAddress(regionCode, dong, apt) {
  // 시군구코드 → 시군구명 매핑 (간단 버전)
  const regionFile = path.join(__dirname, "..", "region_codes.txt");
  try {
    const lines = fs.readFileSync(regionFile, "utf-8").split("\n");
    for (const line of lines) {
      const [code, name] = line.split("\t");
      if (code?.trim() === regionCode) {
        return `${name?.trim()} ${dong} ${apt}`.trim();
      }
    }
  } catch {}
  return `${dong} ${apt}`.trim();
}

// 지역명 캐시
const regionNames = new Map();
function getRegionName(code) {
  if (regionNames.has(code)) return regionNames.get(code);
  const regionFile = path.join(__dirname, "..", "region_codes.txt");
  try {
    const lines = fs.readFileSync(regionFile, "utf-8").split("\n");
    for (const line of lines) {
      const [c, n] = line.split("\t");
      if (c?.trim()) regionNames.set(c.trim(), n?.trim() || "");
    }
  } catch {}
  return regionNames.get(code) || "";
}

// 동별 용도지역 캐시
const zoningCache = new Map();

async function main() {
  if (!DATA_GO_KR_KEY) { console.error("DATA_GO_KR_API_KEY not set"); process.exit(1); }
  if (!KAKAO_KEY) { console.error("KAKAO_REST_API_KEY not set"); process.exit(1); }
  if (!SUPABASE_URL || !SUPABASE_KEY) { console.error("SUPABASE not set"); process.exit(1); }

  const existing = await getExistingIds();
  console.log(`기존 캐시: ${existing.size}건 (7일 이내)`);

  // 최근 3개월
  const now = new Date();
  const months = [];
  for (let i = 0; i < 3; i++) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    months.push(`${d.getFullYear()}${String(d.getMonth()+1).padStart(2,"0")}`);
  }

  let totalApts = 0, newCached = 0, skipped = 0, errors = 0;
  let batch = [];

  for (const regionCode of REGION_CODES) {
    const regionName = getRegionName(regionCode);
    console.log(`\n[${regionCode}] ${regionName}`);

    // 아파트 목록 수집 (최근 3개월)
    const aptMap = new Map();
    for (const ym of months) {
      try {
        const apts = await fetchTradeApts(regionCode, ym);
        for (const a of apts) {
          const key = `${regionCode}-${a.dong}-${a.apt}`;
          if (!aptMap.has(key)) aptMap.set(key, a);
        }
        await sleep(500);
      } catch (e) { console.warn(`  ${ym}: ${e.message}`); }
    }

    const apts = [...aptMap.values()];
    totalApts += apts.length;
    console.log(`  ${apts.length}개 단지 발견`);

    for (const apt of apts) {
      const id = `${apt.regionCode}-${apt.dong}-${apt.apt}`.replace(/\s+/g, "");

      if (existing.has(id)) { skipped++; continue; }

      try {
        // 1) Kakao 지오코딩
        const address = buildAddress(regionCode, apt.dong, apt.apt);
        const coord = await kakaoGeocode(address);
        await sleep(100); // Kakao rate limit

        // 2) 근처 역
        let station = null, stationDist = null;
        if (coord) {
          const nearest = findNearestStation(coord.lat, coord.lng);
          if (nearest && nearest.distanceKm <= 2) {
            station = nearest.name;
            stationDist = nearest.distanceKm;
          }
        }

        // 3) 용도지역 (동 단위 캐시)
        const dongKey = `${apt.sigunguCd}${apt.bjdongCd}`;
        let zoning = zoningCache.get(dongKey) || null;
        if (!zoning && VWORLD_KEY && dongKey.length === 10) {
          zoning = await fetchZoning(apt.sigunguCd, apt.bjdongCd);
          if (zoning) zoningCache.set(dongKey, zoning);
          await sleep(1000); // VWorld rate limit
        }

        batch.push({
          id,
          region_code: regionCode,
          apt: apt.apt,
          dong: apt.dong,
          lat: coord?.lat || null,
          lng: coord?.lng || null,
          nearby_station: station,
          nearby_station_distance_km: stationDist,
          zoning,
          updated_at: new Date().toISOString(),
        });
        newCached++;

        if (batch.length >= 30) {
          await upsertBatch(batch);
          console.log(`  → ${newCached}건 저장`);
          batch = [];
        }
      } catch (e) {
        errors++;
        if (errors <= 5) console.warn(`  Error ${apt.apt}: ${e.message}`);
      }
    }
  }

  if (batch.length > 0) await upsertBatch(batch);

  console.log(`\n=== 완료 ===`);
  console.log(`전체 단지: ${totalApts}`);
  console.log(`신규 캐시: ${newCached}`);
  console.log(`기존 스킵: ${skipped}`);
  console.log(`에러: ${errors}`);
}

main().catch(e => { console.error(e); process.exit(1); });
