import os, json, datetime, shutil
from fastapi import APIRouter, HTTPException
from .db import connect
from .storage import dataset_dir
from .jobs import job_get, job_upsert, request_cancel, cancel_requested
from .ingest import detect_columns, ingest_step_sqlite

router = APIRouter()

def _now():
    return datetime.datetime.utcnow().isoformat()+"Z"

def get_dataset(dataset_id: str):
    con = connect(); cur = con.cursor()
    cur.execute("SELECT * FROM datasets WHERE id=?", (dataset_id,))
    row = cur.fetchone()
    con.close()
    if not row: return None
    d = dict(row)
    d["summary"] = json.loads(d["summary_json"]) if d.get("summary_json") else None
    d["mapping"] = json.loads(d["mapping_json"]) if d.get("mapping_json") else None
    return d

@router.get("/datasets")
def list_datasets():
    con = connect(); cur = con.cursor()
    cur.execute("SELECT * FROM datasets ORDER BY created_at DESC LIMIT 200")
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    out = []
    for r in rows:
        r["summary"] = json.loads(r["summary_json"]) if r.get("summary_json") else None
        out.append(r)
    return out

@router.get("/datasets/{dataset_id}")
def dataset_detail(dataset_id: str):
    ds = get_dataset(dataset_id)
    if not ds: raise HTTPException(404, "Dataset not found")
    return ds

@router.post("/datasets/{dataset_id}/rename")
def rename_dataset(dataset_id: str, name: str):
    con = connect(); cur = con.cursor()
    cur.execute("UPDATE datasets SET name=? WHERE id=?", (name, dataset_id))
    con.commit(); con.close()
    return {"ok": True}

@router.get("/datasets/{dataset_id}/status")
def dataset_status(dataset_id: str):
    ds = get_dataset(dataset_id)
    job = job_get(dataset_id)
    return {"dataset": ds, "job": job}

@router.post("/datasets/{dataset_id}/cancel")
def cancel_ingest(dataset_id: str):
    request_cancel(dataset_id)
    job_upsert(dataset_id, status="CANCEL_REQUESTED", stage="cancel", updated_at=_now())
    return {"ok": True}

@router.post("/datasets/{dataset_id}/ingest")
def start_ingest(dataset_id: str, mapping: dict):
    ds = get_dataset(dataset_id)
    if not ds: raise HTTPException(404, "Dataset not found")
    con = connect(); cur = con.cursor()
    cur.execute("UPDATE datasets SET mapping_json=?, status=?, error=NULL WHERE id=?", (json.dumps(mapping), "PROCESSING", dataset_id))
    con.commit(); con.close()
    job_upsert(dataset_id, status="PROCESSING", stage="queued", processed_rows=0, updated_at=_now(), error=None, cancel_requested=0)
    return {"status":"PROCESSING","dataset_id":dataset_id}

@router.post("/datasets/{dataset_id}/ingest-step")
def ingest_step(dataset_id: str, chunk_rows: int = 5000):
    ds = get_dataset(dataset_id)
    if not ds: raise HTTPException(404, "Dataset not found")

    # locate original file
    ddir = dataset_dir(dataset_id)
    meta_path = os.path.join(ddir, "meta.json")
    if not os.path.exists(meta_path):
        raise HTTPException(404, "Original file not found")
    meta = json.loads(open(meta_path, "r", encoding="utf-8").read())
    file_path = meta["original_path"]

    mapping = ds.get("mapping") or {}
    if not mapping:
        detected = detect_columns(file_path)
        mapping = detected.get("guess") or {}
        con = connect(); cur = con.cursor()
        cur.execute("UPDATE datasets SET mapping_json=? WHERE id=?", (json.dumps(mapping), dataset_id))
        con.commit(); con.close()

    if cancel_requested(dataset_id):
        con = connect(); cur = con.cursor()
        cur.execute("UPDATE datasets SET status=?, error=? WHERE id=?", ("FAILED", "Cancelled by user", dataset_id))
        con.commit(); con.close()
        job_upsert(dataset_id, status="FAILED", stage="cancelled", updated_at=_now(), error="Cancelled by user")
        return {"ok": True, "status": "FAILED", "error": "Cancelled by user"}

    job_upsert(dataset_id, status="PROCESSING", stage="ingesting", updated_at=_now())

    progress = ingest_step_sqlite(dataset_id, file_path, mapping, chunk_rows=chunk_rows, cancel_cb=lambda: cancel_requested(dataset_id))

    if progress.get("done"):
        summary = {"row_count": progress.get("row_count"), "asset_count": progress.get("asset_count")}
        con = connect(); cur = con.cursor()
        cur.execute("UPDATE datasets SET status=?, summary_json=?, error=NULL WHERE id=?", ("READY", json.dumps(summary), dataset_id))
        con.commit(); con.close()
        job_upsert(dataset_id, status="READY", stage="done", processed_rows=progress.get("row_count"), updated_at=_now(), error=None)
        return {"ok": True, "status": "READY", "summary": summary}
    else:
        job_upsert(dataset_id, status="PROCESSING", stage="ingesting", processed_rows=progress.get("processed_rows"), updated_at=_now(), error=None)
        return {"ok": True, "status": "PROCESSING", "progress": progress}

@router.get("/datasets/{dataset_id}/detect")
def detect_for_dataset(dataset_id: str):
    ddir = dataset_dir(dataset_id)
    meta_path = os.path.join(ddir, "meta.json")
    if not os.path.exists(meta_path):
        raise HTTPException(404, "Original file not found")
    meta = json.loads(open(meta_path, "r", encoding="utf-8").read())
    return detect_columns(meta["original_path"])

@router.delete("/datasets/{dataset_id}/hard-delete")
def hard_delete(dataset_id: str):
    con = connect(); cur = con.cursor()
    cur.execute("DELETE FROM facts WHERE dataset_id=?", (dataset_id,))
    cur.execute("DELETE FROM assets WHERE dataset_id=?", (dataset_id,))
    cur.execute("DELETE FROM ingest_jobs WHERE dataset_id=?", (dataset_id,))
    cur.execute("DELETE FROM datasets WHERE id=?", (dataset_id,))
    con.commit(); con.close()
    shutil.rmtree(dataset_dir(dataset_id), ignore_errors=True)
    return {"ok": True}
