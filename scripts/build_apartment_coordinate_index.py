from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = ROOT / "data" / "generated" / "apartment-coordinate-index.json"
ENCODINGS = ("utf-8-sig", "cp949", "euc-kr", "utf-8")

HEADER_ALIASES = {
    "id": {
        "건물관리번호",
        "bdmgtsn",
        "bdmgtSn",
        "bd_mgt_sn",
        "buildingmanagementnumber",
        "building_id",
        "id",
    },
    "apt_name": {
        "건물명",
        "공동주택명",
        "단지명",
        "아파트명",
        "buldnm",
        "kaptname",
        "aptname",
        "buildingname",
        "name",
    },
    "use_type": {
        "건물용도명",
        "건물용도",
        "상세건물용도명",
        "주용도명",
        "buldusagenm",
        "usage",
        "usetype",
        "buildinguse",
    },
    "road_address": {
        "도로명주소",
        "roadaddress",
        "roadaddr",
        "rnmadr",
        "rnmAdres",
        "rnmadres",
    },
    "jibun_address": {
        "지번주소",
        "jibunaddress",
        "lnbradres",
        "lnbraddress",
        "address",
    },
    "dong": {
        "법정동명",
        "읍면동명",
        "동명",
        "dong",
        "emdnm",
        "bjdongnm",
    },
    "bun": {
        "본번",
        "지번본번",
        "bun",
        "buldmnnm",
        "lnbrmnnm",
    },
    "ji": {
        "부번",
        "지번부번",
        "ji",
        "buldslno",
        "lnbrslno",
    },
    "lat": {"lat", "latitude", "위도"},
    "lng": {"lng", "lon", "longitude", "경도"},
    "x": {"x", "entrcx", "coordx", "utmkx"},
    "y": {"y", "entrcy", "coordy", "utmky"},
}

RESIDENTIAL_HINTS = ("공동주택", "아파트", "주상복합", "연립주택")


def normalize_header(name: str) -> str:
    return "".join(ch for ch in str(name).strip().lower() if ch.isalnum() or ("가" <= ch <= "힣"))


def normalize_text(value: str) -> str:
    return "".join(ch for ch in str(value).strip().lower() if ch.isalnum() or ("가" <= ch <= "힣"))


def extract_dong(text: str) -> str:
    for token in str(text or "").replace("(", " ").replace(")", " ").split():
        if token.endswith(("동", "읍", "면", "리")):
            return token
    return ""


def normalize_parcel_part(value: str) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if not digits:
        return ""
    return str(int(digits))


def build_parcel_key(dong: str, bun: str, ji: str) -> str:
    dong_key = normalize_text(dong)
    bun_key = normalize_parcel_part(bun)
    ji_key = normalize_parcel_part(ji) or "0"
    if not dong_key or not bun_key:
        return ""
    return f"{dong_key}|{bun_key}|{ji_key}"


def detect_delimiter(sample: str) -> str:
    candidates = ["|", "\t", ","]
    counts = {delimiter: sample.count(delimiter) for delimiter in candidates}
    return max(counts, key=counts.get)


def open_text(path: Path):
    last_error = None
    for encoding in ENCODINGS:
        try:
            return path.open("r", encoding=encoding, newline="")
        except UnicodeDecodeError as error:
            last_error = error
    if last_error:
        raise last_error
    return path.open("r", encoding="utf-8", newline="")


def iter_rows(path: Path) -> Iterable[dict[str, str]]:
    with open_text(path) as handle:
        sample = handle.read(4096)
        handle.seek(0)
        delimiter = detect_delimiter(sample)
        reader = csv.DictReader(handle, delimiter=delimiter)
        for row in reader:
            yield {str(key).strip(): str(value or "").strip() for key, value in row.items() if key is not None}


def row_value(row: dict[str, str], field_name: str) -> str:
    aliases = HEADER_ALIASES[field_name]
    normalized = {normalize_header(key): value for key, value in row.items()}
    for alias in aliases:
        alias_key = normalize_header(alias)
        if alias_key in normalized and normalized[alias_key]:
            return normalized[alias_key]
    return ""


def parse_float(value: str) -> float | None:
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def should_keep_row(name: str, use_type: str) -> bool:
    if not name:
        return False
    text = f"{name} {use_type}"
    return any(hint in text for hint in RESIDENTIAL_HINTS)


def epsg5179_to_wgs84(x: float, y: float) -> tuple[float, float]:
    if x > 1_500_000 and y < 1_500_000:
        x, y = y, x

    a = 6378137.0
    f = 1 / 298.257222101
    e2 = 2 * f - f * f
    ep2 = e2 / (1 - e2)
    k0 = 0.9996
    lon0 = math.radians(127.5)
    false_easting = 1_000_000.0
    false_northing = 2_000_000.0

    m = (y - false_northing) / k0
    mu = m / (
        a
        * (
            1
            - e2 / 4
            - (3 * e2 * e2) / 64
            - (5 * e2 * e2 * e2) / 256
        )
    )
    e1 = (1 - math.sqrt(1 - e2)) / (1 + math.sqrt(1 - e2))

    j1 = (3 * e1 / 2) - (27 * e1**3 / 32)
    j2 = (21 * e1**2 / 16) - (55 * e1**4 / 32)
    j3 = (151 * e1**3 / 96)
    j4 = (1097 * e1**4 / 512)

    fp = (
        mu
        + j1 * math.sin(2 * mu)
        + j2 * math.sin(4 * mu)
        + j3 * math.sin(6 * mu)
        + j4 * math.sin(8 * mu)
    )

    sin_fp = math.sin(fp)
    cos_fp = math.cos(fp)
    tan_fp = math.tan(fp)

    c1 = ep2 * cos_fp**2
    t1 = tan_fp**2
    n1 = a / math.sqrt(1 - e2 * sin_fp**2)
    r1 = (a * (1 - e2)) / ((1 - e2 * sin_fp**2) ** 1.5)
    d = (x - false_easting) / (n1 * k0)

    lat = fp - (
        (n1 * tan_fp / r1)
        * (
            (d**2) / 2
            - (5 + 3 * t1 + 10 * c1 - 4 * c1**2 - 9 * ep2) * (d**4) / 24
            + (61 + 90 * t1 + 298 * c1 + 45 * t1**2 - 252 * ep2 - 3 * c1**2)
            * (d**6)
            / 720
        )
    )
    lon = lon0 + (
        d
        - (1 + 2 * t1 + c1) * (d**3) / 6
        + (5 - 2 * c1 + 28 * t1 - 3 * c1**2 + 8 * ep2 + 24 * t1**2) * (d**5) / 120
    ) / cos_fp

    return math.degrees(lat), math.degrees(lon)


def normalize_coordinate_row(row: dict[str, str]) -> dict[str, str]:
    return {
        "id": row_value(row, "id"),
        "lat": row_value(row, "lat"),
        "lng": row_value(row, "lng"),
        "x": row_value(row, "x"),
        "y": row_value(row, "y"),
    }


def resolve_coordinates(coord_row: dict[str, str]) -> tuple[float | None, float | None]:
    lat = parse_float(coord_row.get("lat", ""))
    lng = parse_float(coord_row.get("lng", ""))
    if lat is not None and lng is not None and abs(lat) <= 90 and abs(lng) <= 180:
        return lat, lng

    x = parse_float(coord_row.get("x", ""))
    y = parse_float(coord_row.get("y", ""))
    if x is None or y is None:
        return None, None
    return epsg5179_to_wgs84(x, y)


def build_entry(building_row: dict[str, str], coord_row: dict[str, str] | None) -> dict[str, object] | None:
    apt_name = row_value(building_row, "apt_name")
    use_type = row_value(building_row, "use_type")
    if not should_keep_row(apt_name, use_type):
        return None

    road_address = row_value(building_row, "road_address")
    jibun_address = row_value(building_row, "jibun_address")
    dong = row_value(building_row, "dong") or extract_dong(jibun_address) or extract_dong(road_address)
    bun = row_value(building_row, "bun")
    ji = row_value(building_row, "ji")

    lat, lng = resolve_coordinates(coord_row or {})
    if lat is None or lng is None:
        return None

    return {
        "aptName": apt_name,
        "dong": dong,
        "address": jibun_address or road_address,
        "roadAddress": road_address,
        "lat": round(lat, 7),
        "lng": round(lng, 7),
        "bun": normalize_parcel_part(bun),
        "ji": normalize_parcel_part(ji),
        "parcelKey": build_parcel_key(dong, bun, ji),
    }


def load_single_file(path: Path) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for row in iter_rows(path):
        entry = build_entry(row, row)
        if entry:
            items.append(entry)
    return items


def load_merged_files(buildings_path: Path, coords_path: Path) -> list[dict[str, object]]:
    coords_by_id: dict[str, dict[str, str]] = {}
    for row in iter_rows(coords_path):
        coord_row = normalize_coordinate_row(row)
        coord_id = coord_row["id"]
        if coord_id and coord_id not in coords_by_id:
            coords_by_id[coord_id] = coord_row

    items: list[dict[str, object]] = []
    for row in iter_rows(buildings_path):
        building_id = row_value(row, "id")
        coord_row = coords_by_id.get(building_id, {})
        entry = build_entry(row, coord_row)
        if entry:
            items.append(entry)
    return items


def unique_entries(items: list[dict[str, object]]) -> list[dict[str, object]]:
    seen: set[str] = set()
    unique: list[dict[str, object]] = []
    for item in items:
        key = "|".join(
            [
                normalize_text(str(item.get("aptName", ""))),
                normalize_text(str(item.get("dong", ""))),
                str(item.get("parcelKey", "")),
                f"{item.get('lat')}:{item.get('lng')}",
            ]
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="공식 주소정보 DB 또는 전처리 파일로 아파트 좌표 인덱스를 생성합니다."
    )
    parser.add_argument("--input", type=Path, help="좌표/주소가 한 파일에 같이 있는 경우")
    parser.add_argument("--buildings", type=Path, help="건물 정보 파일")
    parser.add_argument("--coords", type=Path, help="좌표 정보 파일")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT, help="출력 JSON 경로")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.input:
      items = load_single_file(args.input.resolve())
      sources = [args.input.name]
    elif args.buildings and args.coords:
      items = load_merged_files(args.buildings.resolve(), args.coords.resolve())
      sources = [args.buildings.name, args.coords.name]
    else:
      raise SystemExit("--input 하나 또는 --buildings/--coords 조합을 지정해 주세요.")

    unique = unique_entries(items)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "sourceFiles": sources,
        "count": len(unique),
        "items": unique,
    }
    args.out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"saved {len(unique)} apartment coordinates to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
