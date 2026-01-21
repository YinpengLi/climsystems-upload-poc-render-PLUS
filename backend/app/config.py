import os

DATA_DIR = os.getenv("DATA_DIR", "/tmp/data")
ALLOW_ORIGINS = os.getenv("ALLOW_ORIGINS", "*")
MAX_UPLOAD_MB = int(os.getenv("MAX_UPLOAD_MB", "500"))
CHUNK_SIZE_MB = int(os.getenv("CHUNK_SIZE_MB", "8"))
