# ClimSystems Upload-only Dashboard + Reports (Render-ready)

This is a deployable demo package (no Base44) focused on **large dataset upload + visualization + report generation**.

## What you get
- Chunked upload (browser → backend) for large CSV/XLSX
- Mapping wizard (choose lat/lon, asset id, theme/indicator/value/year/scenario columns)
- Portfolio overview map with basemap toggle incl. **satellite**
- Full filter panel (multi-select + select all): year, scenario, theme, indicator
- Asset detail page (lat/lon shown) with spider chart + time series
- PDF report preview generated from the filtered dataset (spider chart included)

## Deploy on Render
### Backend (Web Service)
- Root Directory: `backend`
- Build: `pip install -r requirements.txt`
- Start: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
- Add **Persistent Disk** and mount at `/data`
- Env vars:
  - `DATA_DIR=/data`
  - `ALLOW_ORIGINS=*` (demo) or your frontend URL

### Frontend (Static Site)
- Root Directory: `frontend`
- Build: `npm install && npm run build`
- Publish Directory: `dist`
- Env var:
  - `VITE_API_URL=https://<backend>.onrender.com/api`

## Large dataset guidance
- Prefer CSV for very large files (fast streaming ingest).
- XLSX is supported with streaming row reads, but still heavier than CSV.


## Added in PLUS version
- Map click → set selected asset for Asset Detail/Reports
- Lat/Lon search + nearest asset selection
- GeoJSON overlay upload (client-side)
- Download original dataset file
- Export filtered CSV (portfolio or asset)
- Rename dataset
- Portfolio PDF report preview
- AI Assistant (offline deterministic) for definitions + adaptation guidance
