from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from google import genai
from pydantic import BaseModel, ConfigDict, Field


DEFAULT_MODEL = "gemini-2.5-flash"
DEFAULT_OUTPUT_DIR = Path("output")


class ExtractedRecord(BaseModel):
    model_config = ConfigDict(extra="allow")


class ExtractionResult(BaseModel):
    records: list[ExtractedRecord] = Field(default_factory=list)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generic PDF-to-structured-data extractor using Gemini. "
            "You provide a schema config JSON that describes the fields to extract."
        )
    )
    parser.add_argument(
        "--pdf",
        type=Path,
        default=None,
        help="Path to the PDF. If omitted, the first PDF in the folder is used.",
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=Path("schemas/water_quality_schema.json"),
        help="Path to the extraction schema JSON file.",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"Gemini model to use. Default: {DEFAULT_MODEL}",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory. Default: {DEFAULT_OUTPUT_DIR}",
    )
    return parser.parse_args()


def load_schema(schema_path: Path) -> dict[str, Any]:
    path = schema_path.expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {path}")

    schema = json.loads(path.read_text(encoding="utf-8"))
    required_top_level = ["dataset_name", "record_label", "records_description", "fields"]
    missing = [key for key in required_top_level if key not in schema]
    if missing:
        raise ValueError(f"Schema file is missing required keys: {', '.join(missing)}")

    if not isinstance(schema["fields"], list) or not schema["fields"]:
        raise ValueError("Schema file must include a non-empty 'fields' list.")

    allowed_types = {"string", "integer", "number", "boolean"}
    for field in schema["fields"]:
        for required_key in ["key", "label", "type", "description"]:
            if required_key not in field:
                raise ValueError(f"Each field must include '{required_key}'. Problem field: {field}")
        if field["type"] not in allowed_types:
            raise ValueError(
                f"Unsupported field type '{field['type']}' for field '{field['key']}'. "
                f"Supported types: {', '.join(sorted(allowed_types))}."
            )

    return schema


def discover_pdf(explicit_pdf: Path | None) -> Path:
    if explicit_pdf:
        pdf_path = explicit_pdf.expanduser().resolve()
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {pdf_path}")
        return pdf_path

    pdf_files = sorted(Path.cwd().glob("*.pdf"))
    if not pdf_files:
        raise FileNotFoundError("No PDF files found in the current folder.")
    if len(pdf_files) > 1:
        print(f"Multiple PDFs found. Using the first one: {pdf_files[0].name}")
    return pdf_files[0].resolve()


def get_api_key() -> str:
    load_dotenv()
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "Missing GEMINI_API_KEY. Create a .env file with GEMINI_API_KEY=your_key"
        )
    return api_key


def build_response_schema(schema_config: dict[str, Any]) -> dict[str, Any]:
    properties: dict[str, Any] = {}
    required: list[str] = []

    type_map = {
        "string": "string",
        "integer": "integer",
        "number": "number",
        "boolean": "boolean",
    }

    for field in schema_config["fields"]:
        field_type = field["type"]
        nullable = field.get("nullable", True)
        json_type: Any = type_map[field_type]
        if nullable:
            json_type = [json_type, "null"]

        description = field["description"]
        if field.get("unit"):
            description = f"{description} Unit: {field['unit']}."
        aliases = field.get("aliases", [])
        if aliases:
            description = f"{description} Header aliases or nearby labels may include: {', '.join(aliases)}."

        properties[field["key"]] = {
            "type": json_type,
            "description": description,
        }

        if "enum" in field:
            properties[field["key"]]["enum"] = field["enum"]

        if field.get("required", True):
            required.append(field["key"])

    return {
        "type": "object",
        "properties": {
            "records": {
                "type": "array",
                "description": schema_config["records_description"],
                "items": {
                    "type": "object",
                    "properties": properties,
                    "required": required,
                    "additionalProperties": False,
                },
            }
        },
        "required": ["records"],
        "additionalProperties": False,
    }


def build_prompt(schema_config: dict[str, Any], pdf_name: str) -> str:
    field_lines = []
    for field in schema_config["fields"]:
        line = f"- {field['key']}: {field['description']}"
        if field.get("unit"):
            line += f" Unit: {field['unit']}."
        if field.get("aliases"):
            line += f" Possible header aliases: {', '.join(field['aliases'])}."
        if field.get("examples"):
            line += f" Example values: {', '.join(map(str, field['examples'][:5]))}."
        if field.get("normalization"):
            line += f" Normalization rule: {field['normalization']}."
        if field.get("nullable", True):
            line += " Use null if the value is missing or unclear."
        if field.get("enum"):
            enum_values = ", ".join(field["enum"])
            line += f" Allowed values: {enum_values}."
        field_lines.append(line)

    document_description = schema_config.get("document_description")
    extraction_scope = schema_config.get("extraction_scope")
    record_identity = schema_config.get("record_identity")
    extra_instructions = schema_config.get("extraction_instructions", [])
    extra_instruction_block = "\n".join(f"- {item}" for item in extra_instructions)
    preferred_table_signals = schema_config.get("preferred_table_signals", [])
    preferred_table_block = "\n".join(f"- {item}" for item in preferred_table_signals)
    fallback_sources = schema_config.get("fallback_sources", [])
    fallback_sources_block = "\n".join(f"- {item}" for item in fallback_sources)
    examples = schema_config.get("examples", [])
    examples_block = ""
    if examples:
        rendered_examples = []
        for index, example in enumerate(examples[:3], start=1):
            rendered_examples.append(
                f"Example {index}:\n{json.dumps(example, indent=2, ensure_ascii=True)}"
            )
        examples_block = "\n\nExamples of the target output shape:\n" + "\n\n".join(rendered_examples)

    return f"""
You are extracting structured information from the PDF named "{pdf_name}".

Dataset goal:
- Dataset name: {schema_config["dataset_name"]}
- One output record means: {schema_config["record_label"]}
- Extract only the information relevant to this dataset.
{f"- Document type or domain: {document_description}" if document_description else ""}
{f"- Extraction scope: {extraction_scope}" if extraction_scope else ""}
{f"- A record is unique by: {record_identity}" if record_identity else ""}

Fields to extract:
{chr(10).join(field_lines)}

Extraction rules:
- Return only valid JSON matching the provided schema.
- Do not invent facts, rows, or values.
- Preserve exact labels when the schema expects text copied from the paper.
- For numeric values, remove visual formatting such as commas in thousands separators.
- If a field is not present or is ambiguous, use null when allowed.
- If the paper contains multiple relevant tables, combine them into one records array.
- Ignore unrelated sections, references, and narrative text unless needed to interpret a relevant table.
- Prefer tables whose headers, units, caption, and row structure best match the requested fields.
- Do not use a table if it only partially matches the schema when a better matching table exists elsewhere in the PDF.
- If the schema implies a repeating series such as months, seasons, stations, or years, make sure the extracted records follow that series rather than mixing in a different grouping.
- If the best evidence is in a figure, appendix, supplement, or narrative table note, you may use it only when the schema or paper clearly indicates it is part of the target dataset.
{extra_instruction_block if extra_instruction_block else ""}
{f"Preferred table signals:\n{preferred_table_block}" if preferred_table_block else ""}
{f"Fallback sources if the best table is incomplete:\n{fallback_sources_block}" if fallback_sources_block else ""}{examples_block}
""".strip()


def generate_extraction(
    pdf_path: Path,
    schema_config: dict[str, Any],
    model_name: str,
    api_key: str,
    extra_prompt: str = "",
) -> ExtractionResult:
    client = genai.Client(api_key=api_key)
    uploaded_file = client.files.upload(file=pdf_path)
    prompt = build_prompt(schema_config, pdf_path.name)
    if extra_prompt:
        prompt = f"{prompt}\n\nRetry guidance:\n{extra_prompt}"

    response = client.models.generate_content(
        model=model_name,
        contents=[uploaded_file, prompt],
        config={
            "response_mime_type": "application/json",
            "response_json_schema": build_response_schema(schema_config),
            "temperature": 0,
        },
    )

    if not response.text:
        raise RuntimeError("Gemini returned an empty response.")

    return ExtractionResult.model_validate_json(response.text)


def find_field(schema_config: dict[str, Any], field_key: str) -> dict[str, Any] | None:
    for field in schema_config["fields"]:
        if field["key"] == field_key:
            return field
    return None


def validate_result(
    result: ExtractionResult,
    schema_config: dict[str, Any],
) -> list[str]:
    issues: list[str] = []
    records = [record.model_dump() for record in result.records]
    quality_checks = schema_config.get("quality_checks", {})

    min_records = quality_checks.get("min_records")
    if min_records is not None and len(records) < min_records:
        issues.append(f"Expected at least {min_records} records but got {len(records)}.")

    unique_by = quality_checks.get("unique_by", [])
    if unique_by:
        seen: set[tuple[Any, ...]] = set()
        duplicates = 0
        for record in records:
            key = tuple(record.get(field_key) for field_key in unique_by)
            if key in seen:
                duplicates += 1
            seen.add(key)
        if duplicates:
            issues.append(
                f"Found {duplicates} duplicate records using unique fields: {', '.join(unique_by)}."
            )

    allowed_values = quality_checks.get("allowed_values", {})
    for field_key, allowed in allowed_values.items():
        bad_values = sorted(
            {
                record.get(field_key)
                for record in records
                if record.get(field_key) is not None and record.get(field_key) not in allowed
            }
        )
        if bad_values:
            issues.append(
                f"Field '{field_key}' contains unexpected values: {', '.join(map(str, bad_values[:10]))}."
            )

    require_non_null_fields = quality_checks.get("require_non_null_fields", [])
    for field_key in require_non_null_fields:
        non_null_count = sum(1 for record in records if record.get(field_key) is not None)
        if non_null_count == 0:
            issues.append(f"Field '{field_key}' is null for every extracted record.")

    min_non_null_ratio = quality_checks.get("min_non_null_ratio", {})
    for field_key, ratio in min_non_null_ratio.items():
        if not records:
            continue
        non_null_count = sum(1 for record in records if record.get(field_key) is not None)
        actual_ratio = non_null_count / len(records)
        if actual_ratio < ratio:
            issues.append(
                f"Field '{field_key}' has non-null ratio {actual_ratio:.2f}, below required {ratio:.2f}."
            )

    for field in schema_config["fields"]:
        if field.get("required", True) and not field.get("nullable", True):
            missing_required = sum(1 for record in records if record.get(field["key"]) in (None, ""))
            if missing_required:
                issues.append(
                    f"Required field '{field['key']}' is missing in {missing_required} records."
                )

    return issues


def call_gemini(
    pdf_path: Path, schema_config: dict[str, Any], model_name: str, api_key: str
) -> ExtractionResult:
    first_result = generate_extraction(
        pdf_path=pdf_path,
        schema_config=schema_config,
        model_name=model_name,
        api_key=api_key,
    )
    issues = validate_result(first_result, schema_config)
    if not issues:
        return first_result

    retry_guidance = (
        "The previous extraction did not satisfy the schema quality checks.\n"
        + "\n".join(f"- {issue}" for issue in issues)
        + "\nFocus on the best matching table only. If a field such as month has allowed values, "
          "reject rows from tables using a different grouping."
    )

    second_result = generate_extraction(
        pdf_path=pdf_path,
        schema_config=schema_config,
        model_name=model_name,
        api_key=api_key,
        extra_prompt=retry_guidance,
    )
    second_issues = validate_result(second_result, schema_config)
    if second_issues:
        raise RuntimeError(
            "Extraction failed schema quality checks after retry:\n- "
            + "\n- ".join(second_issues)
        )
    return second_result


def sanitize_slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return slug or "dataset"


def build_dataframe(result: ExtractionResult, schema_config: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    fields = schema_config["fields"]

    for record in result.records:
        raw = record.model_dump()
        row: dict[str, Any] = {}
        for field in fields:
            row[field["label"]] = raw.get(field["key"])
        rows.append(row)

    df = pd.DataFrame(rows)

    sort_fields = schema_config.get("sort_by", [])
    if sort_fields:
        sort_labels = [
            next(field["label"] for field in fields if field["key"] == field_key)
            for field_key in sort_fields
            if any(field["key"] == field_key for field in fields)
        ]
        if sort_labels and not df.empty:
            df = df.sort_values(sort_labels, na_position="last").reset_index(drop=True)

    return df


def build_units_row(schema_config: dict[str, Any]) -> dict[str, Any] | None:
    if not schema_config.get("include_units_row", True):
        return None

    row: dict[str, Any] = {}
    has_any_unit = False

    for index, field in enumerate(schema_config["fields"]):
        label = field["label"]
        unit = field.get("unit", "")
        if unit:
            row[label] = unit
            has_any_unit = True
        else:
            row[label] = "units:" if index == 0 else ""

    return row if has_any_unit else None


def write_json(result: ExtractionResult, destination: Path) -> None:
    destination.write_text(
        json.dumps(result.model_dump(mode="json"), indent=2),
        encoding="utf-8",
    )


def write_csv(df: pd.DataFrame, destination: Path) -> None:
    df.to_csv(destination, index=False)


def write_excel(df: pd.DataFrame, schema_config: dict[str, Any], destination: Path) -> None:
    units_row = build_units_row(schema_config)
    excel_df = df
    if units_row is not None:
        excel_df = pd.concat([pd.DataFrame([units_row]), df], ignore_index=True)

    with pd.ExcelWriter(destination, engine="openpyxl") as writer:
        excel_df.to_excel(writer, index=False, sheet_name="extracted_data")
        sheet = writer.sheets["extracted_data"]
        sheet.freeze_panes = "A3" if units_row is not None else "A2"

        for index, column_name in enumerate(excel_df.columns, start=1):
            width = max(14, min(40, len(str(column_name)) + 4))
            column_letter = sheet.cell(row=1, column=index).column_letter
            sheet.column_dimensions[column_letter].width = width


def main() -> None:
    args = parse_args()
    schema_config = load_schema(args.schema)
    api_key = get_api_key()
    pdf_path = discover_pdf(args.pdf)

    print(f"Using PDF: {pdf_path.name}")
    print(f"Using schema: {args.schema}")
    print(f"Using model: {args.model}")

    result = call_gemini(
        pdf_path=pdf_path,
        schema_config=schema_config,
        model_name=args.model,
        api_key=api_key,
    )
    df = build_dataframe(result, schema_config)

    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(exist_ok=True)

    dataset_slug = sanitize_slug(schema_config["dataset_name"])
    json_path = output_dir / f"{dataset_slug}.json"
    csv_path = output_dir / f"{dataset_slug}.csv"
    xlsx_path = output_dir / f"{dataset_slug}.xlsx"

    write_json(result, json_path)
    write_csv(df, csv_path)
    write_excel(df, schema_config, xlsx_path)

    print(f"Records extracted: {len(df)}")
    print(f"JSON written to: {json_path}")
    print(f"CSV written to: {csv_path}")
    print(f"XLSX written to: {xlsx_path}")


if __name__ == "__main__":
    main()
