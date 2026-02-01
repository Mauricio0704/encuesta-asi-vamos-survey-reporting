# Encuesta Así Vamos – Tabulation Report Generator

Python tool to generate Excel tabulation reports from the survey database. It can produce one file per topic or a single workbook with one sheet per topic.

## Quick start

1. Create and activate a Python virtual environment (recommended):

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip3 install -e .
```

3. Run the main script for generating the full report:

```bash
python3 main.py
```

## Required data

This repository **does not include** the database. The database is obtained from another GitHub [repository](https://github.com/Mauricio0704/encuesta-asi-vamos-etl-pipeline). After downloading it, place the file at:

- data/db/survey.db

The process also uses the disaggregation configuration at:

- data/processed/disaggregations.json

## Usage

From the project root:

- Topic report (one file per topic):
  - `python main.py --reporte temas`
- Single workbook (one file with one sheet per topic):
  - `python main.py --reporte temas_unico`

Generated files are saved in:

- output/

## Structure

- [main.py](main.py): entry point.
- [src/](src/): query, aggregation, and Excel writing logic.
- data/: inputs (database and disaggregations).
- output/: results.

## Notes

- Some questions with the `cp` prefix also generate an additional “_sin_factor” report (without weighting).
- Tables include totals and, for numeric questions, a weighted average.
