import os
from .db import data_dir

def datasets_root():
    d = os.path.join(data_dir(), "datasets")
    os.makedirs(d, exist_ok=True)
    return d

def dataset_dir(dataset_id: str):
    d = os.path.join(datasets_root(), dataset_id)
    os.makedirs(d, exist_ok=True)
    return d

def chunks_dir(upload_id: str):
    d = os.path.join(data_dir(), "uploads", upload_id)
    os.makedirs(d, exist_ok=True)
    return d
