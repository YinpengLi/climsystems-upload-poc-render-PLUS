# ClimSystems Render Upload POC (Latest)

This package is designed to work reliably on **Render** with large datasets by using **incremental ingestion**.

## Deploy on Render

### Backend (Web Service)
- Root Directory: `backend`
- Build Command: `pip install -r requirements.txt`
- Start Command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
- Add Persistent Disk mounted at `/data` (recommended)
- Env var: `DATA_DIR=/data`

Health: `https://YOUR-BACKEND.onrender.com/api/health`

### Frontend (Static Site)
- Root Directory: `frontend`
- Build Command: `npm install && npm run build`
- Publish Directory: `dist`
- Env var: `VITE_API_URL=https://YOUR-BACKEND.onrender.com/api`

## Using the app
1. Upload CSV (best for large files).
2. Select dataset, confirm mapping.
3. Click **Start ingest**.
4. Leave page open while it ingests in steps.
