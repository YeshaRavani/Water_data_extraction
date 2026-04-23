from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from google import genai


DEFAULT_MODEL = "gemini-2.5-flash"
DEFAULT_RESEARCH_DIR = Path("research")
DEFAULT_OUTPUT_DIR = Path("output/research_batch")
DEFAULT_DB_PATH = DEFAULT_OUTPUT_DIR / "measurements.db"
SCHEMA_SQL_PATH = Path("schemas/research_measurements.sql")


STAGE1_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "paper_overview": {
            "type": "object",
            "properties": {
                "citation": {"type": ["string", "null"]},
                "is_review_paper": {"type": ["boolean", "null"]},
                "study_region": {"type": ["string", "null"]},
                "data_sources": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "location": {"type": ["string", "null"]},
                            "description": {"type": ["string", "null"]},
                            "page": {"type": ["string", "null"]},
                        },
                        "required": ["location", "description", "page"],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["citation", "is_review_paper", "study_region", "data_sources"],
            "additionalProperties": False,
        },
        "sites": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "description": {"type": ["string", "null"]},
                    "latitude": {"type": ["number", "null"]},
                    "longitude": {"type": ["number", "null"]},
                    "matrix": {"type": ["string", "null"]},
                    "sample_type": {"type": ["string", "null"]},
                    "source_quote": {"type": ["string", "null"]},
                    "source_location": {"type": ["string", "null"]},
                },
                "required": [
                    "id",
                    "description",
                    "latitude",
                    "longitude",
                    "matrix",
                    "sample_type",
                    "source_quote",
                    "source_location",
                ],
                "additionalProperties": False,
            },
        },
        "parameters": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "name": {"type": ["string", "null"]},
                    "name_as_reported": {"type": ["string", "null"]},
                    "cas_number": {"type": ["string", "null"]},
                    "category": {"type": ["string", "null"]},
                    "unit_as_reported": {"type": ["string", "null"]},
                },
                "required": [
                    "id",
                    "name",
                    "name_as_reported",
                    "cas_number",
                    "category",
                    "unit_as_reported",
                ],
                "additionalProperties": False,
            },
        },
        "temporal_coverage": {
            "type": "object",
            "properties": {
                "time_periods": {
                    "type": "array",
                    "items": {"type": ["string", "null"]},
                },
                "sampling_dates": {"type": ["string", "null"]},
                "granularity": {"type": ["string", "null"]},
            },
            "required": ["time_periods", "sampling_dates", "granularity"],
            "additionalProperties": False,
        },
        "extraction_notes": {"type": ["string", "null"]},
    },
    "required": [
        "paper_overview",
        "sites",
        "parameters",
        "temporal_coverage",
        "extraction_notes",
    ],
    "additionalProperties": False,
}


STAGE2_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "measurements": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "site_id": {"type": ["string", "null"]},
                    "parameter_id": {"type": ["string", "null"]},
                    "time_period": {"type": ["string", "null"]},
                    "raw_value": {"type": ["number", "null"]},
                    "mean_value": {"type": ["number", "null"]},
                    "std_dev": {"type": ["number", "null"]},
                    "min_value": {"type": ["number", "null"]},
                    "max_value": {"type": ["number", "null"]},
                    "n_observations": {"type": ["integer", "null"]},
                    "aggregation_level": {"type": ["string", "null"]},
                    "limit_qualifier": {"type": ["string", "null"], "enum": ["<", ">", "=", None]},
                    "detection_limit": {"type": ["number", "null"]},
                    "unit": {"type": ["string", "null"]},
                    "original_source_citation": {"type": ["string", "null"]},
                    "source_location": {"type": ["string", "null"]},
                    "source_quote": {"type": ["string", "null"]},
                },
                "required": [
                    "site_id",
                    "parameter_id",
                    "time_period",
                    "raw_value",
                    "mean_value",
                    "std_dev",
                    "min_value",
                    "max_value",
                    "n_observations",
                    "aggregation_level",
                    "limit_qualifier",
                    "detection_limit",
                    "unit",
                    "original_source_citation",
                    "source_location",
                    "source_quote",
                ],
                "additionalProperties": False,
            },
        },
        "extraction_issues": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "location": {"type": ["string", "null"]},
                    "issue": {"type": ["string", "null"]},
                },
                "required": ["location", "issue"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["measurements", "extraction_issues"],
    "additionalProperties": False,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch-extract water quality measurements from all PDFs in a research folder."
    )
    parser.add_argument(
        "--research-dir",
        type=Path,
        default=DEFAULT_RESEARCH_DIR,
        help=f"Directory containing research PDFs. Default: {DEFAULT_RESEARCH_DIR}",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for per-paper JSON and combined tabular outputs. Default: {DEFAULT_OUTPUT_DIR}",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"SQLite database path. Default: {DEFAULT_DB_PATH}",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"Gemini model to use. Default: {DEFAULT_MODEL}",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on number of PDFs to process.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop on the first paper that fails instead of continuing.",
    )
    return parser.parse_args()


def get_api_key() -> str:
    load_dotenv()
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY. Create a .env file with GEMINI_API_KEY=your_key")
    return api_key


def list_pdfs(research_dir: Path, limit: int | None) -> list[Path]:
    resolved = research_dir.expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Research folder not found: {resolved}")

    pdfs = sorted(resolved.glob("*.pdf"))
    if not pdfs:
        raise FileNotFoundError(f"No PDF files found in research folder: {resolved}")

    if limit is not None:
        pdfs = pdfs[:limit]
    return pdfs


def sanitize_slug(value: str) -> str:
    slug = "".join(char.lower() if char.isalnum() else "_" for char in value).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug or "paper"


def build_stage1_prompt(pdf_name: str) -> str:
    return f"""
You are extracting structured water quality data from the PDF named "{pdf_name}".

Task:
- Analyze the full paper before extracting measurements.
- Identify the study overview, sampling sites, water-quality parameters, temporal coverage, and the most relevant data sources.
- Be general enough to handle river studies, drains, wastewater papers, review papers, and contaminant-monitoring papers.

Return only valid JSON matching the provided schema.

Guidelines:
- If the paper is a review or meta-analysis, capture that in is_review_paper and note cited original sources in data_sources.
- Create stable site IDs such as S1, S2 and parameter IDs such as P1, P2 for cross-referencing.
- Standardize parameter names where appropriate, but preserve the original wording in name_as_reported.
- Include coordinates only if explicitly reported.
- Use null instead of guessing.
- Be thorough about tables, figures, appendices, and supplementary material that contain measurements.
""".strip()


def build_stage2_prompt(pdf_name: str, stage1_output: dict[str, Any]) -> str:
    stage1_json = json.dumps(stage1_output, indent=2, ensure_ascii=True)
    return f"""
You are extracting quantitative water-quality measurements from the PDF named "{pdf_name}".

Context from Stage 1:
{stage1_json}

Task:
- Extract all quantitative measurements into normalized measurement records.
- Use the Stage 1 site IDs and parameter IDs wherever possible.
- Be general enough to handle single values, means, ranges, mean plus/minus SD, detection limits, review-paper citations, and figure-derived values when the figure is clearly readable.

Return only valid JSON matching the provided schema.

Guidelines:
- One record per unique (site, parameter, time_period, source_location) combination unless the paper clearly reports multiple distinct statistics that belong in the same record.
- If a value is reported as mean plus/minus SD, fill mean_value and std_dev.
- If a value is reported as a range, fill min_value and max_value and keep raw_value null unless the paper also gives a single main value.
- If the paper reports only one number for a cell, put it in raw_value.
- When value is ND, BDL, below detection limit, or similar, keep the value fields null, set limit_qualifier to "<" if appropriate, and fill detection_limit when available.
- source_quote must be verbatim from the paper when possible.
- source_location must identify the table, figure, appendix, or section.
- If a site or parameter cannot be mapped confidently, use null and report the ambiguity in extraction_issues.
- Do not invent rows or expand partial evidence into a full grid.
""".strip()


def generate_json(
    client: genai.Client,
    uploaded_file: Any,
    prompt: str,
    response_schema: dict[str, Any],
    model_name: str,
    debug_path: Path | None = None,
) -> dict[str, Any]:
    response = client.models.generate_content(
        model=model_name,
        contents=[uploaded_file, prompt],
        config={
            "response_mime_type": "application/json",
            "response_json_schema": response_schema,
            "temperature": 0,
        },
    )
    if not response.text:
        raise RuntimeError("Gemini returned an empty response.")
    return parse_model_json(response.text, debug_path)


def clean_model_json(text: str) -> str:
    cleaned = text.strip()

    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()

    cleaned = re.sub(r"\bNaN\b", "null", cleaned)
    cleaned = re.sub(r"\bInfinity\b", "null", cleaned)
    cleaned = re.sub(r"\b-Infinity\b", "null", cleaned)
    return cleaned


def parse_model_json(text: str, debug_path: Path | None) -> dict[str, Any]:
    cleaned = clean_model_json(text)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as exc:
        if debug_path is not None:
            debug_path.parent.mkdir(parents=True, exist_ok=True)
            debug_path.write_text(
                json.dumps(
                    {
                        "error": str(exc),
                        "raw_response": text,
                        "cleaned_response": cleaned,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
        raise RuntimeError(
            f"Model returned invalid JSON. Debug saved to {debug_path}."
            if debug_path is not None
            else "Model returned invalid JSON."
        ) from exc


def validate_stage1(stage1_output: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if not stage1_output.get("paper_overview", {}).get("citation"):
        issues.append("Missing paper citation.")
    if not stage1_output.get("parameters"):
        issues.append("No parameters found.")
    if not stage1_output.get("sites"):
        issues.append("No sites found.")
    return issues


def validate_stage2(stage2_output: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    measurements = stage2_output.get("measurements", [])
    if not measurements:
        issues.append("No measurements found.")
        return issues

    missing_sources = sum(
        1 for measurement in measurements if not measurement.get("source_location") or not measurement.get("source_quote")
    )
    if missing_sources:
        issues.append(f"{missing_sources} measurements are missing source verification fields.")

    missing_values = 0
    for measurement in measurements:
        numeric_fields = [
            measurement.get("raw_value"),
            measurement.get("mean_value"),
            measurement.get("std_dev"),
            measurement.get("min_value"),
            measurement.get("max_value"),
            measurement.get("detection_limit"),
        ]
        if all(value is None for value in numeric_fields):
            missing_values += 1
    if missing_values == len(measurements):
        issues.append("All extracted measurements are missing numeric content.")
    return issues


def run_stage1(
    client: genai.Client,
    uploaded_file: Any,
    pdf_path: Path,
    model_name: str,
    paper_output_dir: Path,
) -> dict[str, Any]:
    stage1 = generate_json(
        client=client,
        uploaded_file=uploaded_file,
        prompt=build_stage1_prompt(pdf_path.name),
        response_schema=STAGE1_SCHEMA,
        model_name=model_name,
        debug_path=paper_output_dir / "stage1_raw_error.json",
    )
    issues = validate_stage1(stage1)
    if not issues:
        return stage1

    retry_prompt = build_stage1_prompt(pdf_path.name) + "\n\nRetry guidance:\n- " + "\n- ".join(issues)
    stage1 = generate_json(
        client=client,
        uploaded_file=uploaded_file,
        prompt=retry_prompt,
        response_schema=STAGE1_SCHEMA,
        model_name=model_name,
        debug_path=paper_output_dir / "stage1_retry_raw_error.json",
    )
    retry_issues = validate_stage1(stage1)
    if retry_issues:
        raise RuntimeError("Stage 1 failed quality checks:\n- " + "\n- ".join(retry_issues))
    return stage1


def run_stage2(
    client: genai.Client,
    uploaded_file: Any,
    pdf_path: Path,
    stage1_output: dict[str, Any],
    model_name: str,
    paper_output_dir: Path,
) -> dict[str, Any]:
    stage2 = generate_json(
        client=client,
        uploaded_file=uploaded_file,
        prompt=build_stage2_prompt(pdf_path.name, stage1_output),
        response_schema=STAGE2_SCHEMA,
        model_name=model_name,
        debug_path=paper_output_dir / "stage2_raw_error.json",
    )
    issues = validate_stage2(stage2)
    if not issues:
        return stage2

    retry_prompt = (
        build_stage2_prompt(pdf_path.name, stage1_output)
        + "\n\nRetry guidance:\n- "
        + "\n- ".join(issues)
        + "\n- Focus on the clearest measurement tables or figures only."
    )
    stage2 = generate_json(
        client=client,
        uploaded_file=uploaded_file,
        prompt=retry_prompt,
        response_schema=STAGE2_SCHEMA,
        model_name=model_name,
        debug_path=paper_output_dir / "stage2_retry_raw_error.json",
    )
    retry_issues = validate_stage2(stage2)
    if retry_issues:
        raise RuntimeError("Stage 2 failed quality checks:\n- " + "\n- ".join(retry_issues))
    return stage2


def init_database(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    schema_sql = SCHEMA_SQL_PATH.read_text(encoding="utf-8")
    conn.executescript("DROP TABLE IF EXISTS measurements;\n" + schema_sql)
    return conn


def build_note(source_quote: str | None, source_issue_text: str | None) -> str | None:
    parts = []
    if source_quote:
        parts.append(f"source_quote: {source_quote}")
    if source_issue_text:
        parts.append(f"issues: {source_issue_text}")
    if not parts:
        return None
    return " | ".join(parts)


def insert_measurements(
    conn: sqlite3.Connection,
    stage1: dict[str, Any],
    stage2: dict[str, Any],
) -> int:
    sites_by_id = {site["id"]: site for site in stage1.get("sites", [])}
    params_by_id = {parameter["id"]: parameter for parameter in stage1.get("parameters", [])}
    paper_citation = stage1.get("paper_overview", {}).get("citation")
    time_granularity = stage1.get("temporal_coverage", {}).get("granularity")
    extraction_issues = stage2.get("extraction_issues", [])
    issue_text = "; ".join(
        f"{issue.get('location')}: {issue.get('issue')}" for issue in extraction_issues if issue.get("issue")
    ) or None

    cursor = conn.cursor()
    inserted = 0

    for measurement in stage2.get("measurements", []):
        site = sites_by_id.get(measurement.get("site_id"), {})
        parameter = params_by_id.get(measurement.get("parameter_id"), {})
        parameter_name = (
            parameter.get("name")
            or parameter.get("name_as_reported")
            or measurement.get("parameter_id")
            or "unknown_parameter"
        )
        cursor.execute(
            """
            INSERT INTO measurements (
                paper_citation,
                original_source_citation,
                source_location,
                site_description,
                latitude,
                longitude,
                parameter_name,
                cas_number,
                category,
                matrix,
                sample_type,
                time_period,
                time_granularity,
                raw_value,
                mean_value,
                std_dev,
                min_value,
                max_value,
                n_observations,
                aggregation_level,
                limit_qualifier,
                detection_limit,
                unit,
                notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                paper_citation,
                measurement.get("original_source_citation"),
                measurement.get("source_location"),
                site.get("description"),
                site.get("latitude"),
                site.get("longitude"),
                parameter_name,
                parameter.get("cas_number"),
                parameter.get("category"),
                site.get("matrix"),
                site.get("sample_type"),
                measurement.get("time_period"),
                time_granularity,
                measurement.get("raw_value"),
                measurement.get("mean_value"),
                measurement.get("std_dev"),
                measurement.get("min_value"),
                measurement.get("max_value"),
                measurement.get("n_observations"),
                measurement.get("aggregation_level"),
                measurement.get("limit_qualifier"),
                measurement.get("detection_limit"),
                measurement.get("unit") or parameter.get("unit_as_reported"),
                build_note(measurement.get("source_quote"), issue_text),
            ),
        )
        inserted += 1

    conn.commit()
    return inserted


def save_json_output(data: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def export_database_tables(db_path: Path, output_dir: Path) -> None:
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM measurements ORDER BY paper_citation, source_location, parameter_name", conn)
    conn.close()

    csv_path = output_dir / "measurements.csv"
    json_path = output_dir / "measurements.json"
    xlsx_path = output_dir / "measurements.xlsx"

    df.to_csv(csv_path, index=False)
    df.to_json(json_path, orient="records", indent=2)

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="measurements")
        sheet = writer.sheets["measurements"]
        sheet.freeze_panes = "A2"
        for index, column_name in enumerate(df.columns, start=1):
            column_letter = sheet.cell(row=1, column=index).column_letter
            sheet.column_dimensions[column_letter].width = max(14, min(40, len(str(column_name)) + 4))


def process_pdf(
    client: genai.Client,
    pdf_path: Path,
    model_name: str,
    output_dir: Path,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    paper_slug = sanitize_slug(pdf_path.stem)
    paper_output_dir = output_dir / paper_slug
    paper_output_dir.mkdir(parents=True, exist_ok=True)

    uploaded_file = client.files.upload(file=pdf_path)
    stage1 = run_stage1(client, uploaded_file, pdf_path, model_name, paper_output_dir)
    save_json_output(stage1, paper_output_dir / "stage1.json")

    stage2 = run_stage2(client, uploaded_file, pdf_path, stage1, model_name, paper_output_dir)
    save_json_output(stage2, paper_output_dir / "stage2.json")

    inserted = insert_measurements(conn, stage1, stage2)
    return {
        "paper": pdf_path.name,
        "status": "ok",
        "stage1_sites": len(stage1.get("sites", [])),
        "stage1_parameters": len(stage1.get("parameters", [])),
        "measurements": inserted,
        "issues": len(stage2.get("extraction_issues", [])),
    }


def main() -> None:
    args = parse_args()
    api_key = get_api_key()
    pdf_paths = list_pdfs(args.research_dir, args.limit)

    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = args.db.expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    client = genai.Client(api_key=api_key)
    conn = init_database(db_path)

    summary: list[dict[str, Any]] = []
    try:
        for index, pdf_path in enumerate(pdf_paths, start=1):
            print(f"[{index}/{len(pdf_paths)}] Processing {pdf_path.name}")
            try:
                paper_summary = process_pdf(
                    client=client,
                    pdf_path=pdf_path,
                    model_name=args.model,
                    output_dir=output_dir,
                    conn=conn,
                )
                summary.append(paper_summary)
                print(
                    f"  sites={paper_summary['stage1_sites']} "
                    f"parameters={paper_summary['stage1_parameters']} "
                    f"measurements={paper_summary['measurements']} "
                    f"issues={paper_summary['issues']}"
                )
            except Exception as exc:
                error_summary = {
                    "paper": pdf_path.name,
                    "status": "failed",
                    "error": str(exc),
                    "stage1_sites": 0,
                    "stage1_parameters": 0,
                    "measurements": 0,
                    "issues": 0,
                }
                summary.append(error_summary)
                print(f"  failed: {exc}")
                if args.fail_fast:
                    raise
    finally:
        conn.close()

    summary_path = output_dir / "summary.json"
    save_json_output({"papers": summary}, summary_path)
    export_database_tables(db_path, output_dir)

    total_measurements = sum(item["measurements"] for item in summary)
    failed_papers = sum(1 for item in summary if item["status"] == "failed")
    print(f"\nProcessed {len(summary)} papers")
    print(f"Failed papers: {failed_papers}")
    print(f"Total measurements inserted: {total_measurements}")
    print(f"SQLite database: {db_path}")
    print(f"Combined outputs written to: {output_dir}")


if __name__ == "__main__":
    main()
