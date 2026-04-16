from __future__ import annotations

import csv
import re
import sys
import zipfile
from pathlib import Path
from xml.sax.saxutils import escape as xml_escape


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(
            "Usage: python3 scripts/report/pack-match-csvs-xlsx.py <input_folder> <output_xlsx>"
        )

    input_folder = Path(sys.argv[1])
    output_xlsx = Path(sys.argv[2])

    csv_files = sorted(input_folder.glob("*.csv"))
    if not csv_files:
        raise SystemExit(f"No CSV files found in {input_folder}")

    sheets: list[tuple[str, list[list[str]]]] = []
    used_names: set[str] = set()

    for csv_file in csv_files:
        with csv_file.open("r", encoding="utf-8", newline="") as handle:
            rows = [row for row in csv.reader(handle)]
        sheet_name = unique_sheet_name(build_sheet_name(csv_file.stem), used_names)
        sheets.append((sheet_name, rows))

    output_xlsx.parent.mkdir(parents=True, exist_ok=True)
    write_xlsx(output_xlsx, sheets)
    print(f"xlsx_path={output_xlsx} sheet_count={len(sheets)}")


def build_sheet_name(stem: str) -> str:
    parts = stem.split("_")
    if len(parts) >= 3:
        date = parts[0]
        match_id = parts[1]
        teams = parts[2].replace("_vs_", " vs ").replace("-vs-", " vs ")
        return f"{date} {match_id} {teams}"
    return stem


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
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return (cleaned or "Sheet")[:31]


def write_xlsx(path: Path, sheets: list[tuple[str, list[list[str]]]]) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", build_content_types(len(sheets)))
        zf.writestr("_rels/.rels", ROOT_RELS)
        zf.writestr("xl/workbook.xml", build_workbook_xml(sheets))
        zf.writestr("xl/_rels/workbook.xml.rels", build_workbook_rels(len(sheets)))
        zf.writestr("xl/styles.xml", STYLES_XML)
        zf.writestr("docProps/core.xml", CORE_XML)
        zf.writestr("docProps/app.xml", build_app_xml(sheets))
        for index, (_name, rows) in enumerate(sheets, start=1):
            zf.writestr(f"xl/worksheets/sheet{index}.xml", build_sheet_xml(rows))


def build_content_types(sheet_count: int) -> str:
    overrides = [
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>',
        '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>',
        '<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>',
        '<Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>',
    ]
    for index in range(1, sheet_count + 1):
        overrides.append(
            f'<Override PartName="/xl/worksheets/sheet{index}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        + "".join(overrides)
        + "</Types>"
    )


ROOT_RELS = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
    '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
    '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>'
    '<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>'
    "</Relationships>"
)


def build_workbook_xml(sheets: list[tuple[str, list[list[str]]]]) -> str:
    sheet_xml = []
    for index, (name, _rows) in enumerate(sheets, start=1):
        sheet_xml.append(
            f'<sheet name="{xml_escape(name)}" sheetId="{index}" r:id="rId{index}"/>'
        )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        "<sheets>" + "".join(sheet_xml) + "</sheets></workbook>"
    )


def build_workbook_rels(sheet_count: int) -> str:
    rels = []
    for index in range(1, sheet_count + 1):
        rels.append(
            f'<Relationship Id="rId{index}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{index}.xml"/>'
        )
    rels.append(
        f'<Relationship Id="rId{sheet_count + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        + "".join(rels)
        + "</Relationships>"
    )


STYLES_XML = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
    '<fonts count="2">'
    '<font><sz val="11"/><name val="Calibri"/></font>'
    '<font><b/><sz val="11"/><name val="Calibri"/></font>'
    "</fonts>"
    '<fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>'
    '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
    '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
    '<cellXfs count="2">'
    '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>'
    '<xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/>'
    "</cellXfs>"
    '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
    "</styleSheet>"
)


CORE_XML = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" '
    'xmlns:dc="http://purl.org/dc/elements/1.1/" '
    'xmlns:dcterms="http://purl.org/dc/terms/" '
    'xmlns:dcmitype="http://purl.org/dc/dcmitype/" '
    'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
    "<dc:title>IPL 2025 Polymarket + Cricsheet</dc:title>"
    "<dc:creator>Hephaestus</dc:creator>"
    "</cp:coreProperties>"
)


def build_app_xml(sheets: list[tuple[str, list[list[str]]]]) -> str:
    titles = "".join(f"<vt:lpstr>{xml_escape(name)}</vt:lpstr>" for name, _ in sheets)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" '
        'xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">'
        "<Application>Microsoft Excel</Application>"
        '<HeadingPairs><vt:vector size="2" baseType="variant"><vt:variant><vt:lpstr>Worksheets</vt:lpstr></vt:variant><vt:variant><vt:i4>'
        + str(len(sheets))
        + "</vt:i4></vt:variant></vt:vector></HeadingPairs>"
        '<TitlesOfParts><vt:vector size="'
        + str(len(sheets))
        + '" baseType="lpstr">'
        + titles
        + "</vt:vector></TitlesOfParts></Properties>"
    )


def build_sheet_xml(rows: list[list[str]]) -> str:
    sheet_rows = []
    max_columns = max((len(row) for row in rows), default=0)
    if max_columns > 0:
        cols = "".join(
            f'<col min="{idx}" max="{idx}" width="18" customWidth="1"/>'
            for idx in range(1, max_columns + 1)
        )
        cols_xml = f"<cols>{cols}</cols>"
    else:
        cols_xml = ""

    for row_index, row in enumerate(rows, start=1):
        cells = []
        for col_index, value in enumerate(row, start=1):
            ref = f"{column_letter(col_index)}{row_index}"
            style = ' s="1"' if row_index == 1 else ""
            if is_number(value):
                cells.append(f'<c r="{ref}"{style}><v>{value}</v></c>')
            else:
                cells.append(
                    f'<c r="{ref}" t="inlineStr"{style}><is><t>{xml_escape(value)}</t></is></c>'
                )
        sheet_rows.append(f'<row r="{row_index}">' + "".join(cells) + "</row>")

    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        + cols_xml
        + "<sheetData>"
        + "".join(sheet_rows)
        + "</sheetData></worksheet>"
    )


def column_letter(index: int) -> str:
    result = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        result = chr(65 + remainder) + result
    return result


def is_number(value: str) -> bool:
    if value == "":
        return False
    try:
        float(value)
        return True
    except ValueError:
        return False


if __name__ == "__main__":
    main()
