import { fetchApiJson } from './utils.js';

export const REGIONS = [
  { name: "서울", code: "11" },
  { name: "경기", code: "41" },
  { name: "인천", code: "28" },
  { name: "부산", code: "26" },
  { name: "대구", code: "27" },
  { name: "대전", code: "30" },
  { name: "광주", code: "29" },
  { name: "울산", code: "31" },
  { name: "세종", code: "36" },
  { name: "강원", code: "42" },
  { name: "충북", code: "43" },
  { name: "충남", code: "44" },
  { name: "전북", code: "45" },
  { name: "전남", code: "46" },
  { name: "경북", code: "47" },
  { name: "경남", code: "48" },
  { name: "제주", code: "50" },
];

export const REGION_OPTIONS = [
  { name: "서울 강남구", code: "11680" },
  { name: "서울 송파구", code: "11710" },
  { name: "서울 마포구", code: "11440" },
  { name: "부산 해운대구", code: "26350" },
  { name: "대구 수성구", code: "27260" },
  { name: "인천 연수구", code: "28185" },
  { name: "대전 유성구", code: "30200" },
  { name: "광주 서구", code: "29140" },
  { name: "울산 남구", code: "31140" },
  { name: "세종시", code: "36110" },
  { name: "경기 성남시 분당구", code: "41135" },
  { name: "경기 용인시 수지구", code: "41465" },
  { name: "강원 춘천시", code: "42110" },
  { name: "충북 청주시 상당구", code: "43111" },
  { name: "충남 천안시 동남구", code: "44131" },
  { name: "전북 전주시 완산구", code: "45111" },
  { name: "전남 목포시", code: "46110" },
  { name: "경북 포항시 남구", code: "47111" },
  { name: "경남 창원시 의창구", code: "48121" },
  { name: "제주시", code: "50110" },
];

export const DEFAULT_REGION_CODE = "11680";

export function createRegionCatalogFromFlatOptions(options) {
  const sidoMap = new Map();
  const sigunguMap = new Map();

  (options || []).forEach((option) => {
    const regionCode = String(option?.code || "").trim();
    const fullName = String(option?.name || "").trim();
    if (!regionCode || !fullName) return;

    const sidoCode = regionCode.slice(0, 2);
    const parts = fullName.split(/\s+/);
    const sidoName = parts[0] || fullName;
    const sigunguName = parts.slice(1).join(" ") || fullName;

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
        fullName,
      });
    }
    sigunguMap.set(sidoCode, items);
  });

  const sigunguOptionsBySido = {};
  for (const [sidoCode, items] of sigunguMap.entries()) {
    sigunguOptionsBySido[sidoCode] = items.sort((a, b) =>
      a.name.localeCompare(b.name, "ko"),
    );
  }

  const sidoOptions = [...sidoMap.values()].sort((a, b) =>
    a.name.localeCompare(b.name, "ko"),
  );

  return {
    sidoOptions,
    sigunguOptionsBySido,
  };
}

export let regionCatalog = createRegionCatalogFromFlatOptions(REGION_OPTIONS);

export async function loadRegionCatalog() {
  try {
    const payload = await fetchApiJson(
      "/api/regions",
    );
    if (
      Array.isArray(payload?.sidoOptions) &&
      payload?.sigunguOptionsBySido &&
      typeof payload.sigunguOptionsBySido === "object"
    ) {
      regionCatalog = {
        sidoOptions: payload.sidoOptions,
        sigunguOptionsBySido: payload.sigunguOptionsBySido,
      };
    }
  } catch {
    regionCatalog = createRegionCatalogFromFlatOptions(REGION_OPTIONS);
  }
}

export function getSidoCodeFromRegionCode(regionCode) {
  return String(regionCode || "").slice(0, 2);
}

export function getSigunguOptions(sidoCode) {
  return regionCatalog?.sigunguOptionsBySido?.[sidoCode] || [];
}

export function getFirstAvailableRegionCode() {
  const firstSidoCode = regionCatalog?.sidoOptions?.[0]?.code || "";
  return getSigunguOptions(firstSidoCode)?.[0]?.code || "";
}

export function populateSigunguSelect(
  sigunguSelectId,
  sidoCode,
  preferredRegionCode = "",
) {
  const sigunguSelect = document.getElementById(sigunguSelectId);
  if (!sigunguSelect) return "";

  const options = getSigunguOptions(sidoCode);
  sigunguSelect.innerHTML = options
    .map((item) => `<option value="${item.code}">${item.name}</option>`)
    .join("");

  const nextRegionCode = options.some(
    (item) => item.code === preferredRegionCode,
  )
    ? preferredRegionCode
    : options[0]?.code || "";

  if (nextRegionCode) {
    sigunguSelect.value = nextRegionCode;
  }

  return nextRegionCode;
}

export function initRegionPair(
  sidoSelectId,
  sigunguSelectId,
  preferredRegionCode = DEFAULT_REGION_CODE,
) {
  const sidoSelect = document.getElementById(sidoSelectId);
  const sigunguSelect = document.getElementById(sigunguSelectId);
  if (!sidoSelect || !sigunguSelect) return;

  const previousRegionCode =
    sigunguSelect.value ||
    preferredRegionCode ||
    getFirstAvailableRegionCode();
  const previousSidoCode =
    getSidoCodeFromRegionCode(previousRegionCode) ||
    regionCatalog?.sidoOptions?.[0]?.code ||
    "";

  sidoSelect.innerHTML = (regionCatalog?.sidoOptions || [])
    .map((item) => `<option value="${item.code}">${item.name}</option>`)
    .join("");

  if (previousSidoCode) {
    sidoSelect.value = previousSidoCode;
  }

  populateSigunguSelect(
    sigunguSelectId,
    sidoSelect.value,
    previousRegionCode,
  );

  sidoSelect.onchange = () => {
    populateSigunguSelect(sigunguSelectId, sidoSelect.value);
    sigunguSelect.dispatchEvent(new Event("change", { bubbles: true }));
  };
}

export function getRegionName(code) {
  const regionCode = String(code || "");
  const sidoCode = getSidoCodeFromRegionCode(regionCode);
  const sigungu = getSigunguOptions(sidoCode).find(
    (item) => item.code === regionCode,
  );
  if (sigungu) {
    return sigungu.fullName || sigungu.name;
  }
  return (
    REGION_OPTIONS.find((r) => r.code === regionCode)?.name ||
    regionCode ||
    ""
  );
}
