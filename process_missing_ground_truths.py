"""
Process the missing ground truth JSON files for 'SAYANTAN SAMUI' and 'vaid etal'
and convert them directly to the new 13-column schema CSV.
"""

import json
import csv
import os
import re
from pathlib import Path

# Reuse the logic from the other script for consistency
FULL_MONTHS = {
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
}
ABBR_MONTHS = {
    "jan", "feb", "mar", "apr", "may", "jun",
    "jul", "aug", "sep", "oct", "nov", "dec",
}
SEASON_LABELS = {
    "pom", "prm", "pre-monsoon", "post-monsoon", "monsoon",
    "summer", "winter", "spring", "autumn", "rainy",
}

def is_month(val: str) -> bool:
    return val.lower() in FULL_MONTHS or val.lower() in ABBR_MONTHS

def is_year(val: str) -> bool:
    return bool(re.fullmatch(r"\d{4}", val.strip()))

def is_season(val: str) -> bool:
    return val.lower().strip() in SEASON_LABELS

def classify_sampling_period(sp: str):
    date, month, year, season = "", "", "", ""
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
        season = sp_stripped
    return date, month, year, season

NEW_HEADER = [
    "Sr No.", "Location", "Date", "Month", "Year", "Season", 
    "Parameter", "Actual Value", "Mean", "Std Dev", "Unit", 
    "Source", "Notes/Extraction Remark"
]

BASE_DIR = Path("schema_aligned_ground_truths")

FILES_TO_PROCESS = [
    BASE_DIR / "SAYANTAN SAMUI" / "ground_truth SAYANTAN_SAMUI.json",
    BASE_DIR / "vaid etal" / "ground_truth vaid etal.json"
]

def process_json_to_csv(json_path: Path):
    if not json_path.exists():
        print(f"File not found: {json_path}")
        return

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    output_csv = json_path.parent / "schema_aligned_ground_truth.csv"
    
    rows = []
    for idx, item in enumerate(data, start=1):
        sp = item.get("time_period", "")
        date, month, year, season = classify_sampling_period(sp)
        
        # Build notes
        notes_parts = []
        if item.get("source_location"):
            notes_parts.append(f"Source Location: {item.get('source_location')}")
        if item.get("source_quote"):
            notes_parts.append(f"Source Quote: {item.get('source_quote')}")
        if item.get("extraction_issues") and item.get("extraction_issues") != "none":
            notes_parts.append(f"Extraction Issues: {item.get('extraction_issues')}")
        
        row = {
            "Sr No.": idx,
            "Location": item.get("site_id", ""),
            "Date": date,
            "Month": month,
            "Year": year,
            "Season": season,
            "Parameter": item.get("parameter_id", ""),
            "Actual Value": item.get("raw_value", ""),
            "Mean": item.get("mean_value", ""),
            "Std Dev": item.get("std_dev", ""),
            "Unit": item.get("unit", ""),
            "Source": item.get("original_source_citation", ""),
            "Notes/Extraction Remark": " | ".join(notes_parts)
        }
        rows.append(row)

    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=NEW_HEADER)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"Created {output_csv} from {json_path} ({len(rows)} rows)")

if __name__ == "__main__":
    for file_path in FILES_TO_PROCESS:
        process_json_to_csv(file_path)
