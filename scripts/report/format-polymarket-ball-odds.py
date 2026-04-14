from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path
from typing import Iterable
from xml.sax.saxutils import escape as xml_escape


LEAN_COLUMNS = [
    ("inning", "inning"),
    ("ball", "ball"),
    ("event", "event"),
    ("batting_team", "batting_team"),
    ("bowling_team", "bowling_team"),
    ("primary_team", "team_1"),
    ("primary_before_pct", "team_1_before_pct"),
    ("primary_after_pct", "team_1_after_pct"),
    ("primary_delta_pct", "team_1_delta_pct"),
    ("secondary_team", "team_2"),
    ("secondary_before_pct", "team_2_before_pct"),
    ("secondary_after_pct", "team_2_after_pct"),
    ("secondary_delta_pct", "team_2_delta_pct"),
]


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit(
            "Usage: python3 scripts/report/format-polymarket-ball-odds.py <input_csv> <output_dir> <workbook_xml>"
        )

    input_csv = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])
    workbook_xml = Path(sys.argv[3])

    rows = list(read_rows(input_csv))
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    match_meta: dict[str, dict[str, str]] = {}

    for row in rows:
        match_id = row["source_match_id"]
        grouped[match_id].append(row)
        match_meta[match_id] = {
            "match_date": row["match_date"],
            "batting_team": row["primary_team"],
            "bowling_team": row["secondary_team"],
        }

    output_dir.mkdir(parents=True, exist_ok=True)
    workbook_xml.parent.mkdir(parents=True, exist_ok=True)

    workbook_sheets: list[tuple[str, list[list[str]]]] = []
    used_sheet_names: set[str] = set()

    for match_id, match_rows in sorted(grouped.items(), key=lambda item: item[0]):
        sorted_rows = sorted(match_rows, key=row_sort_key)
        lean_rows = [to_lean_row(row) for row in sorted_rows]
        meta = match_meta[match_id]
        file_name = build_match_file_name(match_id, meta)
        write_csv(output_dir / file_name, lean_rows)

        sheet_name = unique_sheet_name(
            build_sheet_name(match_id, meta), used_sheet_names
        )
        workbook_sheets.append((sheet_name, [lean_header(), *lean_rows]))

    workbook_xml.write_text(build_workbook_xml(workbook_sheets), encoding="utf-8")

    print(
        f"formatted_matches={len(grouped)} per_match_csv_dir={output_dir} workbook_xml={workbook_xml}"
    )


def read_rows(path: Path) -> Iterable[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield {key: value or "" for key, value in row.items()}


def row_sort_key(row: dict[str, str]) -> tuple[int, float]:
    inning = int(row.get("inning", "0") or 0)
    try:
        ball = float(row.get("ball", "0") or 0)
    except ValueError:
        ball = 0.0
    return inning, ball


def lean_header() -> list[str]:
    return [column_name for _, column_name in LEAN_COLUMNS]


def to_lean_row(row: dict[str, str]) -> list[str]:
    return [row.get(source, "") for source, _ in LEAN_COLUMNS]


def write_csv(path: Path, rows: list[list[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(lean_header())
        writer.writerows(rows)


def build_match_file_name(match_id: str, meta: dict[str, str]) -> str:
    left = slugify(meta["batting_team"])
    right = slugify(meta["bowling_team"])
    return f"{meta['match_date']}_{match_id}_{left}_vs_{right}.csv"


def build_sheet_name(match_id: str, meta: dict[str, str]) -> str:
    left = team_short(meta["batting_team"])
    right = team_short(meta["bowling_team"])
    return f"{meta['match_date']} {left}-{right}"


def unique_sheet_name(base: str, used: set[str]) -> str:
    cleaned = sanitize_sheet_name(base)
    candidate = cleaned
    counter = 2
    while candidate in used:
        suffix = f"-{counter}"
        candidate = sanitize_sheet_name(cleaned[: 31 - len(suffix)] + suffix)
        counter += 1
    used.add(candidate)
    return candidate


def sanitize_sheet_name(value: str) -> str:
    cleaned = "".join(ch for ch in value if ch not in "[]:*?/\\")
    cleaned = cleaned.strip() or "Sheet"
    return cleaned[:31]


def slugify(value: str) -> str:
    text = value.lower()
    chars = [ch if ch.isalnum() else "-" for ch in text]
    slug = "".join(chars)
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-") or "match"


def team_short(value: str) -> str:
    mapping = {
        "Chennai Super Kings": "CSK",
        "Delhi Capitals": "DC",
        "Gujarat Titans": "GT",
        "Kolkata Knight Riders": "KKR",
        "Lucknow Super Giants": "LSG",
        "Mumbai Indians": "MI",
        "Punjab Kings": "PBKS",
        "Rajasthan Royals": "RR",
        "Royal Challengers Bengaluru": "RCB",
        "Royal Challengers Bangalore": "RCB",
        "Sunrisers Hyderabad": "SRH",
    }
    return mapping.get(value, value[:12])


def build_workbook_xml(sheets: list[tuple[str, list[list[str]]]]) -> str:
    lines = [
        '<?xml version="1.0"?>',
        '<?mso-application progid="Excel.Sheet"?>',
        '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"',
        ' xmlns:o="urn:schemas-microsoft-com:office:office"',
        ' xmlns:x="urn:schemas-microsoft-com:office:excel"',
        ' xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">',
        " <Styles>",
        '  <Style ss:ID="Header"><Font ss:Bold="1"/></Style>',
        " </Styles>",
    ]

    for sheet_name, rows in sheets:
        lines.append(f' <Worksheet ss:Name="{xml_escape(sheet_name)}">')
        lines.append("  <Table>")
        for row_index, row in enumerate(rows):
            lines.append("   <Row>")
            for cell in row:
                style = ' ss:StyleID="Header"' if row_index == 0 else ""
                lines.append(
                    f'    <Cell{style}><Data ss:Type="String">{xml_escape(str(cell))}</Data></Cell>'
                )
            lines.append("   </Row>")
        lines.append("  </Table>")
        lines.append(" </Worksheet>")

    lines.append("</Workbook>")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    main()
