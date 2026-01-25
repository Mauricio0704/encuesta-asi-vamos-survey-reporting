import sqlite3
from src.paths import DB_DIR


def get_connection() -> sqlite3.Connection:
    db_path = DB_DIR / "survey.db"
    conn = sqlite3.connect(db_path)
    return conn
