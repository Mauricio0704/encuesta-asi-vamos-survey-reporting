from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = PROJECT_ROOT / "data"

PROCESSED_DATA_DIR = DATA_DIR / "processed"
DB_DIR = DATA_DIR / "db"


OUTPUT_DIR = PROJECT_ROOT / "output"
