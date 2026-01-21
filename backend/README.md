# Backend (FastAPI)

## Local run
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export DATA_DIR=./data
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## Render
- Root Directory: backend
- Build: pip install -r requirements.txt
- Start: uvicorn app.main:app --host 0.0.0.0 --port $PORT
- Persistent Disk mount path: /data
- Env: DATA_DIR=/data
