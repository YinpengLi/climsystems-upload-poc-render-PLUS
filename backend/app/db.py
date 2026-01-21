import os, sqlite3

def data_dir():
    # Prefer Render persistent disk at /data if present
    return os.environ.get("DATA_DIR") or ("/data" if os.path.exists("/data") else os.path.join(os.getcwd(), "data"))

def db_path():
    d = data_dir()
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, "app.sqlite")

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
        processed_rows INTEGER,
        total_rows INTEGER,
        updated_at TEXT,
        error TEXT,
        cancel_requested INTEGER DEFAULT 0
    )
    """)

    con.commit()
    con.close()
