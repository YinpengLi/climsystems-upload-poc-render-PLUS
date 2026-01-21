import os
from .config import DATA_DIR

def dataset_dir(dataset_id: str) -> str:
    d = os.path.join(DATA_DIR, "datasets", dataset_id)
    os.makedirs(d, exist_ok=True)
    return d

def uploads_dir() -> str:
    d = os.path.join(DATA_DIR, "uploads")
    os.makedirs(d, exist_ok=True)
    return d

def chunks_dir(upload_id: str) -> str:
    d = os.path.join(uploads_dir(), upload_id)
    os.makedirs(d, exist_ok=True)
    return d
