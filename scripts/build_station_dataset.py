from __future__ import annotations

import json
import sys
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_GLOB = "*도시철도역사정보*.xlsx"
DEFAULT_OUTPUT = ROOT / "data" / "generated" / "subway-stations.json"
NS = {
    "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}


def find_default_input() -> Path:
    matches = sorted(ROOT.glob(DEFAULT_INPUT_GLOB))
    if not matches:
        raise FileNotFoundError(
            f"역사정보 xlsx 파일을 찾지 못했습니다. 기본 패턴: {DEFAULT_INPUT_GLOB}"
        )
    return matches[0]


def get_cell_value(cell: ET.Element, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t")
    value_node = cell.find("main:v", NS)
    if value_node is None or value_node.text is None:
        return ""
    value = value_node.text
    if cell_type == "s":
        try:
            return shared_strings[int(value)]
        except (ValueError, IndexError):
            return value
    return value


def read_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    strings: list[str] = []
    for item in root:
        text_parts = [node.text or "" for node in item.iter("{%s}t" % NS["main"])]
        strings.append("".join(text_parts))
    return strings


def read_first_sheet_rows(xlsx_path: Path) -> list[list[str]]:
    with zipfile.ZipFile(xlsx_path) as zf:
        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        sheets = workbook.find("main:sheets", NS)
        if sheets is None or not list(sheets):
            return []
        first_sheet = list(sheets)[0]
        rel_id = first_sheet.attrib.get("{%s}id" % NS["rel"])
        if not rel_id:
            return []

        rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}
        target = rel_map[rel_id].lstrip("/")
        if not target.startswith("xl/"):
            target = f"xl/{target}"

        shared_strings = read_shared_strings(zf)
        worksheet = ET.fromstring(zf.read(target))
        rows: list[list[str]] = []
        for row in worksheet.findall(".//main:sheetData/main:row", NS):
            values = [get_cell_value(cell, shared_strings).strip() for cell in row]
            rows.append(values)
        return rows


def build_station_items(rows: list[list[str]]) -> list[dict[str, object]]:
    if not rows:
        return []

    header = rows[0]
    header_index = {name: idx for idx, name in enumerate(header)}

    required = ["역사명", "노선명", "역위도", "역경도", "역사도로명주소"]
    missing = [name for name in required if name not in header_index]
    if missing:
        raise ValueError(f"역 데이터 헤더가 예상과 다릅니다. 누락: {', '.join(missing)}")

    items: list[dict[str, object]] = []
    for row in rows[1:]:
        try:
            name = row[header_index["역사명"]].strip()
            line = row[header_index["노선명"]].strip()
            lat = float(row[header_index["역위도"]])
            lng = float(row[header_index["역경도"]])
            address = row[header_index["역사도로명주소"]].strip()
        except (IndexError, ValueError):
            continue

        if not name:
            continue

        items.append(
            {
                "name": name,
                "line": line,
                "lat": lat,
                "lng": lng,
                "address": address,
            }
        )

    return items


def main() -> int:
    input_path = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else find_default_input()
    output_path = Path(sys.argv[2]).resolve() if len(sys.argv) > 2 else DEFAULT_OUTPUT

    rows = read_first_sheet_rows(input_path)
    items = build_station_items(rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "sourceFile": input_path.name,
        "count": len(items),
        "items": items,
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"saved {len(items)} stations to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
