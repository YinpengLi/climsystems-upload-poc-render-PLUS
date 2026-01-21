import os

# Prefer Render persistent disk if available.
_default = "/data" if os.path.exists("/data") else "/tmp/data"

DATA_DIR = os.getenv("DATA_DIR", _default)
ALLOW_ORIGINS = os.getenv("ALLOW_ORIGINS", "*")
MAX_UPLOAD_MB = int(os.getenv("MAX_UPLOAD_MB", "500"))
CHUNK_SIZE_MB = int(os.getenv("CHUNK_SIZE_MB", "8"))
