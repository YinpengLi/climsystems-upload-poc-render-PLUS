import os, json, uuid, datetime, shutil
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from .storage import chunks_dir, dataset_dir
from .db import connect
from .ingest import detect_columns, ingest_to_sqlite
from .jobs import run_ingest

router = APIRouter()

def _now():
    return datetime.datetime.utcnow().isoformat() + "Z"

@router.post("/upload/init")
def upload_init(filename: str = Form(...), size_bytes: int = Form(0)):
    upload_id = str(uuid.uuid4())
    dataset_id = str(uuid.uuid4())
    os.makedirs(chunks_dir(upload_id), exist_ok=True)
    os.makedirs(dataset_dir(dataset_id), exist_ok=True)

    con = connect(); cur = con.cursor()
    cur.execute(
        "INSERT INTO datasets(id, name, status, source_filename, size_bytes, created_at, mapping_json, summary_json, error, deleted_at) VALUES (?,?,?,?,?,?,?,?,?,NULL)",
        (dataset_id, filename, "UPLOADING", filename, size_bytes, _now(), None, None, None)
    )
    con.commit(); con.close()

    return {"upload_id": upload_id, "dataset_id": dataset_id}

@router.post("/upload/chunk")
def upload_chunk(
    upload_id: str = Form(...),
    dataset_id: str = Form(...),
    part_number: int = Form(...),
    chunk: UploadFile = File(...)
):
    d = chunks_dir(upload_id)
    part_path = os.path.join(d, f"part_{part_number:06d}.bin")
    with open(part_path, "wb") as f:
        shutil.copyfileobj(chunk.file, f)
    return {"ok": True}

@router.post("/upload/finalize")
def upload_finalize(
    upload_id: str = Form(...),
    dataset_id: str = Form(...),
    filename: str = Form(...)
):
    d = chunks_dir(upload_id)
    parts = sorted([p for p in os.listdir(d) if p.startswith("part_")])
    if not parts:
        raise HTTPException(400, "No parts uploaded")
    ext = os.path.splitext(filename)[1].lower()
    out_dir = dataset_dir(dataset_id)
    original_path = os.path.join(out_dir, f"original{ext or ''}")
    with open(original_path, "wb") as out:
        for p in parts:
            with open(os.path.join(d, p), "rb") as f:
                shutil.copyfileobj(f, out)

    meta = {"original_path": original_path, "original_name": filename}
    with open(os.path.join(out_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f)

    detected = detect_columns(original_path)

    con = connect(); cur = con.cursor()
    cur.execute("UPDATE datasets SET status=? WHERE id=?", ("UPLOADED", dataset_id))
    con.commit(); con.close()

    shutil.rmtree(d, ignore_errors=True)
    return {"status": "UPLOADED", "dataset_id": dataset_id, "detected": detected}

@router.post("/datasets/{dataset_id}/ingest")
def start_ingest(dataset_id: str, mapping: dict):
    out_dir = dataset_dir(dataset_id)
    meta_path = os.path.join(out_dir, "meta.json")
    if not os.path.exists(meta_path):
        raise HTTPException(404, "Original file not found")
    meta = json.loads(open(meta_path, "r", encoding="utf-8").read())
    file_path = meta["original_path"]

    con = connect(); cur = con.cursor()
    cur.execute("UPDATE datasets SET mapping_json=? WHERE id=?", (json.dumps(mapping), dataset_id))
    con.commit(); con.close()

    run_ingest(dataset_id, lambda: ingest_to_sqlite(dataset_id, file_path, mapping))
    return {"status": "PROCESSING", "dataset_id": dataset_id}
