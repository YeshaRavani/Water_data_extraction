from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_INPUT_DIR = Path("output/research_batch")
FINAL_MEASUREMENT_COLUMNS = [
    ("location", "Location(actual name not some legend thing)"),
    ("date", "Date"),
    ("month", "Month"),
    ("year", "Year"),
    ("season", "Season"),
    ("parameter", "Parameter"),
    ("actual_value", "Actual Value"),
    ("mean", "Mean"),
    ("std_dev", "Std Dev"),
    ("unit", "Unit"),
    ("source", "Source"),
    ("notes", "Notes/Extraction Remark"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert local JSON output files to CSV without calling any external APIs."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help=f"Directory containing JSON outputs. Default: {DEFAULT_INPUT_DIR}",
    )
    return parser.parse_args()


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def to_dataframe(data: Any) -> pd.DataFrame:
    if isinstance(data, list):
        if not data:
            return pd.DataFrame()
        if all(isinstance(item, dict) for item in data):
            return pd.json_normalize(data, sep=".")
        return pd.DataFrame({"value": data})

    if not isinstance(data, dict):
        return pd.DataFrame({"value": [data]})

    if "measurements" in data and isinstance(data["measurements"], list):
        measurements_df = pd.json_normalize(data["measurements"], sep=".")
        if set(key for key, _ in FINAL_MEASUREMENT_COLUMNS).issubset(measurements_df.columns):
            measurements_df = measurements_df[[key for key, _ in FINAL_MEASUREMENT_COLUMNS]].rename(
                columns=dict(FINAL_MEASUREMENT_COLUMNS)
            )
            measurements_df.insert(0, "Sr No.", range(1, len(measurements_df) + 1))
            issue_text = "; ".join(
                f"{issue.get('location')}: {issue.get('issue')}"
                for issue in data.get("extraction_issues", [])
                if isinstance(issue, dict) and (issue.get("location") or issue.get("issue"))
            )
            if issue_text:
                notes_col = "Notes/Extraction Remark"
                existing_notes = measurements_df[notes_col].fillna("").astype(str)
                measurements_df[notes_col] = existing_notes.apply(
                    lambda note: f"{note} | issues: {issue_text}" if note else f"issues: {issue_text}"
                )
        return measurements_df

    if {
        "paper_overview",
        "sites",
        "parameters",
        "temporal_coverage",
    }.issubset(data.keys()):
        rows: list[dict[str, Any]] = []
        base = {
            "paper_overview.citation": data.get("paper_overview", {}).get("citation"),
            "paper_overview.is_review_paper": data.get("paper_overview", {}).get("is_review_paper"),
            "paper_overview.study_region": data.get("paper_overview", {}).get("study_region"),
            "temporal_coverage.sampling_dates": data.get("temporal_coverage", {}).get("sampling_dates"),
            "temporal_coverage.granularity": data.get("temporal_coverage", {}).get("granularity"),
            "temporal_coverage.time_periods": " | ".join(
                item for item in data.get("temporal_coverage", {}).get("time_periods", []) if item
            ),
            "extraction_notes": data.get("extraction_notes"),
        }

        for site in data.get("sites", []):
            rows.append(
                {
                    **base,
                    "row_type": "site",
                    **{f"site.{key}": value for key, value in site.items()},
                }
            )
        for parameter in data.get("parameters", []):
            rows.append(
                {
                    **base,
                    "row_type": "parameter",
                    **{f"parameter.{key}": value for key, value in parameter.items()},
                }
            )
        for source in data.get("paper_overview", {}).get("data_sources", []):
            rows.append(
                {
                    **base,
                    "row_type": "data_source",
                    **{f"data_source.{key}": value for key, value in source.items()},
                }
            )
        return pd.DataFrame(rows)

    if "papers" in data and isinstance(data["papers"], list):
        return pd.json_normalize(data["papers"], sep=".")

    return pd.json_normalize(data, sep=".")


def convert_json_file(path: Path) -> Path:
    data = load_json(path)
    df = to_dataframe(data)
    csv_path = path.with_suffix(".csv")
    df.to_csv(csv_path, index=False)
    return csv_path


def main() -> None:
    args = parse_args()
    input_dir = args.input_dir.expanduser().resolve()
    json_paths = sorted(input_dir.rglob("*.json"))

    if not json_paths:
        raise FileNotFoundError(f"No JSON files found under {input_dir}")

    converted = 0
    for path in json_paths:
        csv_path = convert_json_file(path)
        converted += 1
        print(f"{path} -> {csv_path}")

    print(f"\nConverted {converted} JSON files to CSV")


if __name__ == "__main__":
    main()
