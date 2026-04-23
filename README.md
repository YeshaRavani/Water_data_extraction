# Water Quality Paper Extractor

This repo contains two active Gemini-based extraction workflows:

- `extract_water_quality.py`: schema-driven extraction for a single PDF
- `extract_research_batch.py`: two-stage extraction across all PDFs in `research/`, with normalized outputs and SQLite export

Generated files are written under `output/`, which is ignored by git.

## Setup

Create `.env` in the repo root:

```env
GEMINI_API_KEY=your_actual_gemini_api_key
```

Install dependencies:

```bash
python3 -m pip install -r requirements.txt
```

## Single-Paper Extraction

If there is one PDF in the current folder:

```bash
python3 extract_water_quality.py
```

To pass a PDF explicitly:

```bash
python3 extract_water_quality.py --pdf "research/Mandal et al 2010 Yamuna water quality.pdf"
```

To choose a schema explicitly:

```bash
python3 extract_water_quality.py \
  --pdf "research/Antil et al 2025 Yamuna water quality.pdf" \
  --schema "schemas/water_quality_schema.json"
```

Outputs:

- `output/<dataset_name>.json`
- `output/<dataset_name>.csv`
- `output/<dataset_name>.xlsx`

## Batch Research Extraction

To process every PDF in `research/`:

```bash
python3 extract_research_batch.py
```

This pipeline:

- runs a two-stage extraction for each paper
- writes per-paper `stage1.json` and `stage2.json` files under `output/research_batch/`
- inserts normalized records into `output/research_batch/measurements.db`
- exports combined `measurements.csv`, `measurements.json`, `measurements.xlsx`, and `summary.json`

## Schemas

The single-paper workflow is driven by a JSON schema such as [water_quality_schema.json](/Users/yesharavani/ILGC/schemas/water_quality_schema.json).

Key JSON schema fields:

- `dataset_name`
- `record_label`
- `records_description`
- `fields`

Useful optional controls:

- `document_description`
- `extraction_scope`
- `record_identity`
- `examples`
- `fallback_sources`
- `quality_checks`

The batch workflow uses the SQLite schema in [research_measurements.sql](/Users/yesharavani/ILGC/schemas/research_measurements.sql).

## Repo Layout

- `research/`: source PDFs
- `schemas/`: extraction and database schemas
- `extract_water_quality.py`: single-paper extractor
- `extract_research_batch.py`: batch extractor and SQLite exporter
- `convert_json_outputs_to_csv.py`: local utility for converting saved JSON outputs to CSV
