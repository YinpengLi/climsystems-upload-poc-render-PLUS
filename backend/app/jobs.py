import json
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, Any
from .db import connect

_executor = ThreadPoolExecutor(max_workers=2)

def update_dataset(dataset_id: str, **fields):
    con = connect(); cur = con.cursor()
    cur.execute("SELECT id FROM datasets WHERE id=?", (dataset_id,))
    if not cur.fetchone():
        con.close(); return
    sets, vals = [], []
    for k,v in fields.items():
        sets.append(f"{k}=?"); vals.append(v)
    vals.append(dataset_id)
    cur.execute(f"UPDATE datasets SET {', '.join(sets)} WHERE id=?", vals)
    con.commit(); con.close()

def run_ingest(dataset_id: str, func: Callable[[], Dict[str, Any]]):
    def task():
        try:
            update_dataset(dataset_id, status="PROCESSING", error=None)
            summary = func()
            update_dataset(dataset_id, status="READY", summary_json=json.dumps(summary))
        except Exception as e:
            update_dataset(dataset_id, status="FAILED", error=str(e))
    _executor.submit(task)
