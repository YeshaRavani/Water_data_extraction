"""
Transform ground truth CSVs from the old schema to the new schema.

Old columns: Location, Sampling period, Parameter, Value, Unit, Statistic, Sample matrix, Source table, Notes
New columns: Sr No., Location, Date, Month, Year, Season, Parameter, Actual Value, Mean, Std Dev, Unit, Source, Notes/Extraction Remark

Mapping logic:
  - Sr No.       : Auto-increment (1-based) per file
  - Location     : Direct copy from 'Location'
  - Date         : Empty (no exact dates available in the source data)
  - Month        : Parsed from 'Sampling period' if it is a recognizable month name/abbreviation
  - Year         : Parsed from 'Sampling period' if it is a 4-digit year (e.g. "2019")
  - Season       : Parsed from 'Sampling period' if it is a season label (POM, PRM, Pre-monsoon, etc.)
  - Parameter    : Direct copy from 'Parameter'
  - Actual Value : Direct copy from 'Value'
  - Mean         : Set to 'Value' when 'Statistic' is 'Mean' or 'Average'; empty otherwise
  - Std Dev      : Extracted from Notes using the pattern "SD=<number>"
  - Unit         : Direct copy from 'Unit'
  - Source       : Direct copy from 'Source table'
  - Notes/Extraction Remark : Direct copy from 'Notes'

Safety:
  - Original files are backed up to <paper_dir>/schema_aligned_ground_truth.OLD.csv
  - Row counts are verified before and after transformation
"""

import csv
import os
import re
import shutil
import sys

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "schema_aligned_ground_truths",
)

NEW_HEADER = [
    "Sr No.",
    "Location",
    "Date",
    "Month",
    "Year",
    "Season",
    "Parameter",
    "Actual Value",
    "Mean",
    "Std Dev",
    "Unit",
    "Source",
    "Notes/Extraction Remark",
]

# Month recognition (full names + 3-letter abbreviations)
FULL_MONTHS = {
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
}
ABBR_MONTHS = {
    "jan", "feb", "mar", "apr", "may", "jun",
    "jul", "aug", "sep", "oct", "nov", "dec",
}

# Season recognition
SEASON_LABELS = {
    "pom", "prm", "pre-monsoon", "post-monsoon", "monsoon",
    "summer", "winter", "spring", "autumn", "rainy",
}


def is_month(val: str) -> bool:
    """Return True if val looks like a month name or abbreviation."""
    return val.lower() in FULL_MONTHS or val.lower() in ABBR_MONTHS


def is_year(val: str) -> bool:
    """Return True if val is a plain 4-digit year like '2019'."""
    return bool(re.fullmatch(r"\d{4}", val.strip()))


def is_season(val: str) -> bool:
    """Return True if val matches a known season/campaign label."""
    return val.lower().strip() in SEASON_LABELS


def extract_std_dev(notes: str) -> str:
    """Try to pull an SD value from the Notes field (pattern: SD=<number>)."""
    if not notes:
        return ""
    m = re.search(r"SD\s*=\s*([0-9eE.+\-]+)", notes)
    if m:
        val = m.group(1).rstrip(".")  # strip trailing period from sentence punctuation
        return val
    return ""


def classify_sampling_period(sp: str):
    """
    Return (date, month, year, season) from the old 'Sampling period' value.
    Only one of month/year/season will be populated; date is always empty.
    """
    date = ""
    month = ""
    year = ""
    season = ""

    if not sp or not sp.strip():
        return date, month, year, season

    sp_stripped = sp.strip()

    if is_month(sp_stripped):
        month = sp_stripped
    elif is_year(sp_stripped):
        year = sp_stripped
    elif is_season(sp_stripped):
        season = sp_stripped
    else:
        # Fallback: put it in Season so no information is lost
        season = sp_stripped

    return date, month, year, season


def transform_row(row: dict, sr_no: int) -> dict:
    """Transform one row from old schema to new schema."""
    sp = row.get("Sampling period", "")
    date, month, year, season = classify_sampling_period(sp)

    statistic = row.get("Statistic", "").strip().lower()
    value = row.get("Value", "")
    notes = row.get("Notes", "")

    mean = value if statistic in ("mean", "average") else ""
    std_dev = extract_std_dev(notes)

    return {
        "Sr No.": sr_no,
        "Location": row.get("Location", ""),
        "Date": date,
        "Month": month,
        "Year": year,
        "Season": season,
        "Parameter": row.get("Parameter", ""),
        "Actual Value": value,
        "Mean": mean,
        "Std Dev": std_dev,
        "Unit": row.get("Unit", ""),
        "Source": row.get("Source table", ""),
        "Notes/Extraction Remark": notes,
    }


def process_file(csv_path: str) -> None:
    """Read an old-schema CSV, back it up, and write the new-schema CSV."""
    # --- Read original rows ---
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        old_rows = list(reader)

    original_count = len(old_rows)
    if original_count == 0:
        print(f"  ⚠  SKIPPED (0 data rows): {csv_path}")
        return

    # --- Back up ---
    backup_path = csv_path.replace(".csv", ".OLD.csv")
    shutil.copy2(csv_path, backup_path)

    # --- Transform ---
    new_rows = []
    for idx, row in enumerate(old_rows, start=1):
        new_rows.append(transform_row(row, sr_no=idx))

    # --- Safety check ---
    if len(new_rows) != original_count:
        print(f"  ✖  ROW COUNT MISMATCH for {csv_path}!")
        print(f"     Original: {original_count}  →  New: {len(new_rows)}")
        sys.exit(1)

    # --- Write ---
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=NEW_HEADER)
        writer.writeheader()
        writer.writerows(new_rows)

    print(f"  ✔  {os.path.basename(os.path.dirname(csv_path))}: "
          f"{original_count} rows transformed successfully.  Backup → {os.path.basename(backup_path)}")


def main():
    print("=" * 70)
    print("Ground Truth Schema Transformation")
    print("Old → New schema")
    print("=" * 70)

    csv_files = sorted(
        os.path.join(root, f)
        for root, _, files in os.walk(BASE_DIR)
        for f in files
        if f == "schema_aligned_ground_truth.csv"
    )

    if not csv_files:
        print("No ground truth CSVs found!")
        sys.exit(1)

    print(f"\nFound {len(csv_files)} ground truth file(s).\n")

    for csv_path in csv_files:
        process_file(csv_path)

    print("\n✅ All files transformed. Originals backed up as .OLD.csv")
    print("   Please verify a few files manually before committing.\n")


if __name__ == "__main__":
    main()
