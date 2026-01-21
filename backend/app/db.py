import os
import sqlite3
from .config import DATA_DIR

def db_path() -> str:
    os.makedirs(DATA_DIR, exist_ok=True)
    return os.path.join(DATA_DIR, "app.sqlite")

def connect():
    con = sqlite3.connect(db_path(), check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    con = connect()
    cur = con.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS datasets (
        id TEXT PRIMARY KEY,
        name TEXT,
        status TEXT,
        summary_json TEXT,
        mapping_json TEXT,
        error TEXT,
        created_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS assets (
        dataset_id TEXT,
        asset_id TEXT,
        label TEXT,
        latitude REAL,
        longitude REAL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS facts (
        dataset_id TEXT,
        asset_id TEXT,
        latitude REAL,
        longitude REAL,
        year INTEGER,
        scenario TEXT,
        theme TEXT,
        indicator TEXT,
        value REAL,
        units TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ingest_jobs (
        dataset_id TEXT PRIMARY KEY,
        status TEXT,
        stage TEXT,
        total_rows INTEGER,
        processed_rows INTEGER,
        started_at TEXT,
        updated_at TEXT,
        error TEXT,
        cancel_requested INTEGER DEFAULT 0
    )
    """)

    con.commit()
    con.close()

