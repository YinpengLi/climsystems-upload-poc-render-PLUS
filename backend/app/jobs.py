import json, datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, Any, Optional
from .db import connect

_executor = ThreadPoolExecutor(max_workers=2)

def _now():
    return datetime.datetime.utcnow().isoformat() + "Z"

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

def job_upsert(dataset_id: str, **fields):
    con = connect(); cur = con.cursor()
    cur.execute("SELECT dataset_id FROM ingest_jobs WHERE dataset_id=?", (dataset_id,))
    exists = cur.fetchone() is not None
    if not exists:
        cur.execute("INSERT INTO ingest_jobs(dataset_id, status, stage, total_rows, processed_rows, started_at, updated_at, error, cancel_requested) VALUES (?,?,?,?,?,?,?,?,?)",
                    (dataset_id, fields.get("status"), fields.get("stage"), fields.get("total_rows"), fields.get("processed_rows"),
                     fields.get("started_at") or _now(), fields.get("updated_at") or _now(), fields.get("error"), fields.get("cancel_requested", 0)))
    else:
        sets, vals = [], []
        for k,v in fields.items():
            sets.append(f"{k}=?"); vals.append(v)
        vals.append(dataset_id)
        cur.execute(f"UPDATE ingest_jobs SET {', '.join(sets)} WHERE dataset_id=?", vals)
    con.commit(); con.close()

def job_get(dataset_id: str) -> Optional[dict]:
    con = connect(); cur = con.cursor()
    cur.execute("SELECT * FROM ingest_jobs WHERE dataset_id=?", (dataset_id,))
    row = cur.fetchone()
    con.close()
    return dict(row) if row else None

def request_cancel(dataset_id: str):
    job_upsert(dataset_id, cancel_requested=1, updated_at=_now())

def cancel_requested(dataset_id: str) -> bool:
    con = connect(); cur = con.cursor()
    cur.execute("SELECT cancel_requested FROM ingest_jobs WHERE dataset_id=?", (dataset_id,))
    row = cur.fetchone()
    con.close()
    return bool(row and row["cancel_requested"])

def run_ingest(dataset_id: str, func: Callable[[Callable[[int], None], Callable[[], bool]], Dict[str, Any]]):
    def task():
        try:
            update_dataset(dataset_id, status="PROCESSING", error=None)
            job_upsert(dataset_id, status="PROCESSING", stage="reading", processed_rows=0, total_rows=None, error=None, started_at=_now(), updated_at=_now(), cancel_requested=0)

            def progress(n: int):
                job_upsert(dataset_id, processed_rows=n, updated_at=_now(), stage="ingesting")

            def cancelled() -> bool:
                return cancel_requested(dataset_id)

            summary = func(progress, cancelled)

            job_upsert(dataset_id, status="READY", stage="done", processed_rows=summary.get("row_count"), total_rows=summary.get("row_count"), updated_at=_now(), error=None)
            update_dataset(dataset_id, status="READY", summary_json=json.dumps(summary))
        except Exception as e:
            job_upsert(dataset_id, status="FAILED", stage="error", updated_at=_now(), error=str(e))
            update_dataset(dataset_id, status="FAILED", error=str(e))
    _executor.submit(task)
