import json, datetime, os
from fastapi import APIRouter, HTTPException
from .db import connect
from .storage import dataset_dir

router = APIRouter()

def _now():
    return datetime.datetime.utcnow().isoformat() + "Z"

@router.get("/datasets")
def list_datasets(include_deleted: bool = False):
    con = connect(); cur = con.cursor()
    if include_deleted:
        cur.execute("SELECT * FROM datasets ORDER BY created_at DESC")
    else:
        cur.execute("SELECT * FROM datasets WHERE deleted_at IS NULL ORDER BY created_at DESC")
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    for r in rows:
        r["mapping"] = json.loads(r["mapping_json"]) if r.get("mapping_json") else None
        r["summary"] = json.loads(r["summary_json"]) if r.get("summary_json") else None
        r.pop("mapping_json", None); r.pop("summary_json", None)
    return rows

@router.get("/datasets/{dataset_id}")
def get_dataset(dataset_id: str):
    con = connect(); cur = con.cursor()
    cur.execute("SELECT * FROM datasets WHERE id=?", (dataset_id,))
    row = cur.fetchone()
    con.close()
    if not row: raise HTTPException(404, "Dataset not found")
    r = dict(row)
    r["mapping"] = json.loads(r["mapping_json"]) if r.get("mapping_json") else None
    r["summary"] = json.loads(r["summary_json"]) if r.get("summary_json") else None
    r.pop("mapping_json", None); r.pop("summary_json", None)
    return r

@router.post("/datasets/{dataset_id}/rename")
def rename_dataset(dataset_id: str, payload: dict):
    name = (payload.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "name required")
    con = connect(); cur = con.cursor()
    cur.execute("UPDATE datasets SET name=? WHERE id=?", (name, dataset_id))
    con.commit(); con.close()
    return {"ok": True}

@router.get("/datasets/{dataset_id}/original")
def stream_original(dataset_id: str):
    ddir = dataset_dir(dataset_id)
    meta_path = os.path.join(ddir, "meta.json")
    if not os.path.exists(meta_path):
        raise HTTPException(404, "No file found")
    meta = json.loads(open(meta_path, "r", encoding="utf-8").read())
    path = meta.get("original_path")
    filename = meta.get("original_name") or "dataset"
    if not path or not os.path.exists(path):
        raise HTTPException(404, "File missing on disk")
    from fastapi.responses import FileResponse
    return FileResponse(path, filename=filename)

@router.delete("/datasets/{dataset_id}")
def soft_delete(dataset_id: str):
    con = connect(); cur = con.cursor()
    cur.execute("UPDATE datasets SET deleted_at=? WHERE id=?", (_now(), dataset_id))
    con.commit(); con.close()
    return {"ok": True}

@router.post("/datasets/{dataset_id}/restore")
def restore(dataset_id: str):
    con = connect(); cur = con.cursor()
    cur.execute("UPDATE datasets SET deleted_at=NULL WHERE id=?", (dataset_id,))
    con.commit(); con.close()
    return {"ok": True}

@router.get("/datasets/{dataset_id}/download-original")
def download_info(dataset_id: str):
    ddir = dataset_dir(dataset_id)
    meta_path = os.path.join(ddir, "meta.json")
    if not os.path.exists(meta_path):
        raise HTTPException(404, "No file found")
    meta = json.loads(open(meta_path, "r", encoding="utf-8").read())
    return {"path": meta.get("original_path"), "filename": meta.get("original_name")}
