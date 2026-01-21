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
      source_filename TEXT,
      size_bytes INTEGER,
      created_at TEXT,
      mapping_json TEXT,
      summary_json TEXT,
      error TEXT,
      deleted_at TEXT
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
      units TEXT,
      extra_json TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS assets (
      dataset_id TEXT,
      asset_id TEXT,
      latitude REAL,
      longitude REAL,
      label TEXT
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_facts_ds_asset ON facts(dataset_id, asset_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_facts_ds_dim ON facts(dataset_id, theme, year, scenario, indicator)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_assets_ds ON assets(dataset_id)")
    con.commit()
    con.close()
