"""
evaluate_accuracy.py — v3.1
--------------------------
Compares Stage-2 extraction output against schema-aligned ground truth CSVs
for all 10 Yamuna water quality papers.

Key improvements:
  - Evaluates the fixed final extraction schema.
  - Aggressive recovery of JSON from corrupted CSVs or error JSONs.
  - No sampling (uses full context).
  - Explicit instructions to ignore out-of-scope GT rows (infrastructure/metadata).

Usage:
    python evaluate_accuracy.py
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from google import genai
from pydantic import BaseModel, Field

# ─── CONFIG ──────────────────────────────────────────────────────────────────

DEFAULT_MODEL = "gemini-2.5-flash"

GT_DIR  = Path("schema_aligned_ground_truths")
OUT_DIR = Path("output/research_batch")

GT_TO_OUTPUT_NAME: dict[str, str] = {
    "SAYANTAN SAMUI": "samui_et_al_2025_yamuna_drain_participatory_survey",
    "vaid etal":      "vaid_et_al_2022_najafgarh_drain_statistical_analysis",
}

OUTPUT_COLS_KEEP = [
    "Location(actual name not some legend thing)", "Date", "Month", "Year", "Season",
    "Parameter", "Actual Value", "Mean", "Std Dev", "Unit", "Source",
    "Notes/Extraction Remark",
]

OUTPUT_COL_RENAME = {
    "location": "Location(actual name not some legend thing)",
    "date": "Date",
    "month": "Month",
    "year": "Year",
    "season": "Season",
    "parameter": "Parameter",
    "actual_value": "Actual Value",
    "mean": "Mean",
    "std_dev": "Std Dev",
    "unit": "Unit",
    "source": "Source",
    "notes": "Notes/Extraction Remark",
}

# ─── PYDANTIC SCHEMA ─────────────────────────────────────────────────────────

class EvaluationResult(BaseModel):
    accuracy_score:      float = Field(description="matched / (matched + missed + hallucinated)")
    precision:           float = Field(description="matched / (matched + hallucinated)")
    recall:              float = Field(description="matched / (matched + missed)")
    f1_score:            float = Field(description="2*P*R/(P+R)")
    matched_records:     int   = Field(description="GT rows correctly captured in output")
    missed_records:      int   = Field(description="GT rows absent or wrong in output")
    hallucinated_records: int  = Field(description="Output rows with no GT counterpart")
    reasoning:           str   = Field(description="Concise evaluation reasoning (max 1000 words)")
    key_discrepancies:   list[str] = Field(description="Top systematic errors, max 8 bullet points")


# ─── ID RESOLUTION ───────────────────────────────────────────────────────────

def load_id_maps(out_paper_dir: Path) -> tuple[dict[str, str], dict[str, str]]:
    """Return (site_id_map, param_id_map) from stage1.json."""
    s1_path = out_paper_dir / "stage1.json"
    site_map: dict[str, str] = {}
    param_map: dict[str, str] = {}
    if not s1_path.exists():
        return site_map, param_map
    try:
        data = json.loads(s1_path.read_text(encoding="utf-8"))
        for s in data.get("sites", []):
            sid  = s.get("id") or s.get("site_id") or ""
            desc = s.get("description") or s.get("site_name") or ""
            if sid:
                site_map[sid] = desc
        for p in data.get("parameters", []):
            pid  = p.get("id") or p.get("parameter_id") or ""
            name = p.get("name") or p.get("parameter_name") or ""
            unit = p.get("unit") or ""
            if pid:
                param_map[pid] = f"{name} ({unit})" if unit else name
    except Exception:
        pass
    return site_map, param_map


def resolve_ids(df: pd.DataFrame, site_map: dict, param_map: dict) -> pd.DataFrame:
    """Replace generic IDs with human-readable names."""
    df = df.copy()
    if "site_id" in df.columns and site_map:
        df["site_id"] = df["site_id"].apply(
            lambda v: f"{v} [{site_map[v]}]" if (pd.notna(v) and str(v) in site_map) else v
        )
    if "parameter_id" in df.columns and param_map:
        df["parameter_id"] = df["parameter_id"].apply(
            lambda v: f"{v} [{param_map[v]}]" if (pd.notna(v) and str(v) in param_map) else v
        )
    return df


# ─── FILE LOADING ────────────────────────────────────────────────────────────

def parse_embedded_json_csv(path: Path) -> Optional[pd.DataFrame]:
    """Extract JSON measurements from raw text/corrupted CSV files."""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
        json_starts = [m.start() for m in re.finditer(r'\{\s*"measurements"', text)]
        if not json_starts:
            json_starts = [m.start() for m in re.finditer(r'\[\s*\{', text)]
        for start in json_starts:
            chunk = text[start:]
            for end in range(len(chunk), 0, -1):
                try:
                    parsed = json.loads(chunk[:end])
                    if isinstance(parsed, dict) and "measurements" in parsed:
                        return pd.DataFrame(parsed["measurements"])
                    if isinstance(parsed, list):
                        return pd.DataFrame(parsed)
                except: continue
    except: pass
    return None

def load_output_csv(out_paper_dir: Path) -> tuple[Optional[pd.DataFrame], str]:
    # 1. Try CSV files
    for fname in ["stage2.csv", "stage2_raw_error.csv"]:
        p = out_paper_dir / fname
        if p.exists():
            try:
                df = pd.read_csv(p, on_bad_lines="skip", low_memory=False)
                if len(df) > 5: return df, fname
            except: pass
            df = parse_embedded_json_csv(p)
            if df is not None and len(df) > 0: return df, f"{fname} (recovered JSON)"

    # 2. Try JSON files
    for jname in ["stage2.json", "stage2_raw_error.json", "stage1.json"]:
        p = out_paper_dir / jname
        if p.exists():
            try:
                content = p.read_text(encoding="utf-8").strip()
                try:
                    data = json.loads(content)
                    if isinstance(data, dict):
                        for key in ["cleaned_response", "raw_response", "measurements"]:
                            if key in data:
                                val = data[key]
                                if isinstance(val, str):
                                    try: val = json.loads(val)
                                    except: pass
                                if isinstance(val, (list, dict)):
                                    data = val
                                    break
                    if isinstance(data, dict) and "measurements" in data:
                        data = data["measurements"]
                    if isinstance(data, (list, dict)) and len(data) > 0:
                        return pd.DataFrame(data), jname
                except:
                    df = parse_embedded_json_csv(p)
                    if df is not None and len(df) > 0: return df, f"{jname} (regex recovered)"
            except: pass
    return None, ""

def load_gt_csv(gt_paper_dir: Path) -> Optional[pd.DataFrame]:
    csv_path = gt_paper_dir / "schema_aligned_ground_truth.csv"
    if csv_path.exists():
        try: return pd.read_csv(csv_path)
        except: pass
    for json_path in [gt_paper_dir / "schema_aligned_ground_truth.json", *gt_paper_dir.glob("*.json")]:
        try:
            raw = json.loads(json_path.read_text(encoding="utf-8"))
            if isinstance(raw, list) and raw: return pd.DataFrame(raw)
            if isinstance(raw, dict):
                for key in ("records", "measurements", "data", "ground_truth"):
                    if key in raw and isinstance(raw[key], list): return pd.DataFrame(raw[key])
                return pd.DataFrame([raw])
        except: continue
    return None

# ─── PROMPT ──────────────────────────────────────────────────────────────────

PROMPT_TEMPLATE = """
You are a rigorous data extraction evaluator for water quality research.
Paper: **{paper_name}**

COLUMN MAPPING: Location→Location(actual name not some legend thing), Parameter→Parameter, Value→Actual Value/Mean, Unit→Unit, Sampling period→Date/Month/Year/Season, Source table→Source, Notes→Notes/Extraction Remark.

LENIENT MATCHING RULES & OUT-OF-SCOPE EXCEPTIONS:
① Numeric values: ±10% relative tolerance counts as a MATCH.
② Site names: partial match is OK (e.g. "Delhi" matches "S3 [Wazirabad, Delhi]").
③ BDL/ND: if GT says BDL/ND and output has limit_qualifier/detection_limit → MATCH.
④ OUT-OF-SCOPE: If a GT record is metadata, infrastructure capacity (e.g. "Sewage generated"), flow characteristics, or geography, IGNORE it. Do not count as missed.

GROUND TRUTH CSV:
{gt_str}

MODEL OUTPUT CSV (Stage-2):
{out_str}

SCORING METHOD:
1. Walk every GT row. Ignore if OUT-OF-SCOPE. Else mark MATCHED or MISSED.
2. Walk every Output row. Mark MATCHED or HALLUCINATED.
3. Compute matched, missed (valid only), hallucinated.
4. Accuracy = matched / (matched + missed + hallucinated).

Return valid JSON. Reasoning max 800 words.
"""

# ─── EVALUATOR ───────────────────────────────────────────────────────────────

def evaluate_paper(client: genai.Client, gt_paper_dir: Path, out_paper_dir: Path) -> Optional[EvaluationResult]:
    paper_name = out_paper_dir.name
    gt_df = load_gt_csv(gt_paper_dir)
    if gt_df is None: return None
    out_df, out_fname = load_output_csv(out_paper_dir)
    if out_df is None: return None
    print(f"  Using output: {out_fname} ({len(out_df)} rows)")
    
    site_map, param_map = load_id_maps(out_paper_dir)
    out_df = resolve_ids(out_df, site_map, param_map)
    out_df = out_df.rename(columns=OUTPUT_COL_RENAME)
    keep_cols = [c for c in OUTPUT_COLS_KEEP if c in out_df.columns]
    out_df = out_df[keep_cols]

    prompt = PROMPT_TEMPLATE.format(
        paper_name=paper_name,
        gt_str=gt_df.to_csv(index=False),
        out_str=out_df.to_csv(index=False)
    )

    try:
        response = client.models.generate_content(
            model=DEFAULT_MODEL,
            contents=prompt,
            config={"response_mime_type": "application/json", "response_schema": EvaluationResult, "temperature": 0.0},
        )
        return EvaluationResult.model_validate_json(response.text)
    except Exception as e:
        print(f"  [ERROR] {paper_name}: {e}")
        return None

# ─── MAIN ────────────────────────────────────────────────────────────────────

def main() -> None:
    load_dotenv()
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key: sys.exit(1)
    client = genai.Client(api_key=api_key)

    results: dict[str, dict] = {}
    gt_dirs = sorted([d for d in GT_DIR.iterdir() if d.is_dir()])
    
    for gt_paper_dir in gt_dirs:
        gt_name = gt_paper_dir.name
        out_name = GT_TO_OUTPUT_NAME.get(gt_name, gt_name)
        out_paper_dir = OUT_DIR / out_name
        print(f"Evaluating: {gt_name}")
        if not out_paper_dir.exists(): continue
        evaluation = evaluate_paper(client, gt_paper_dir, out_paper_dir)
        if evaluation:
            print(f"  ✓ Score={evaluation.accuracy_score:.3f} F1={evaluation.f1_score:.3f}")
            results[gt_name] = evaluation.model_dump()
            with open("accuracy_report.json", "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
        print()

if __name__ == "__main__":
    main()
