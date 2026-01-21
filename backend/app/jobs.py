import datetime
from .db import connect

def _now():
    return datetime.datetime.utcnow().isoformat() + "Z"

def job_get(dataset_id: str):
    con = connect(); cur = con.cursor()
    cur.execute("SELECT * FROM ingest_jobs WHERE dataset_id=?", (dataset_id,))
    row = cur.fetchone()
    con.close()
    return dict(row) if row else None

def job_upsert(dataset_id: str, **fields):
    con = connect(); cur = con.cursor()
    cur.execute("SELECT dataset_id FROM ingest_jobs WHERE dataset_id=?", (dataset_id,))
    exists = cur.fetchone() is not None
    if not exists:
        cur.execute(
            "INSERT INTO ingest_jobs(dataset_id,status,stage,processed_rows,total_rows,updated_at,error,cancel_requested) VALUES (?,?,?,?,?,?,?,?)",
            (dataset_id, fields.get("status"), fields.get("stage"), fields.get("processed_rows", 0), fields.get("total_rows"),
             fields.get("updated_at") or _now(), fields.get("error"), fields.get("cancel_requested", 0))
        )
    else:
        sets, vals = [], []
        for k,v in fields.items():
            sets.append(f"{k}=?"); vals.append(v)
        vals.append(dataset_id)
        cur.execute(f"UPDATE ingest_jobs SET {', '.join(sets)} WHERE dataset_id=?", vals)
    con.commit(); con.close()

def request_cancel(dataset_id: str):
    job_upsert(dataset_id, cancel_requested=1, updated_at=_now())

def cancel_requested(dataset_id: str) -> bool:
    con = connect(); cur = con.cursor()
    cur.execute("SELECT cancel_requested FROM ingest_jobs WHERE dataset_id=?", (dataset_id,))
    row = cur.fetchone()
    con.close()
    return bool(row and row["cancel_requested"])
