import os, csv, datetime, json
from typing import Dict, Any
import pandas as pd
from .db import connect

def detect_columns(file_path: str) -> Dict[str, Any]:
    ext = os.path.splitext(file_path)[1].lower()
    cols = []
    if ext == ".csv":
        with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            cols = next(reader, [])
    elif ext in (".xlsx", ".xls"):
        df = pd.read_excel(file_path, sheet_name=0, nrows=0)
        cols = list(df.columns)
    else:
        cols = []
    # heuristic guesses
    def pick(options):
        lower = {c.lower(): c for c in cols}
        for o in options:
            if o in lower: return lower[o]
        return None
    guess = {
        "lat_col": pick(["latitude","lat","y"]),
        "lon_col": pick(["longitude","lon","lng","x"]),
        "asset_id_col": pick(["asset_id","assetid","id","location_id","site_id"]),
        "label_col": pick(["label","name","asset_name","site_name"]),
        "year_col": pick(["year"]),
        "scenario_col": pick(["scenario","ssp","rcp"]),
        "theme_col": pick(["theme"]),
        "indicator_col": pick(["indicator","metric","variable"]),
        "value_col": pick(["score","value"]),
        "units_col": pick(["units","unit"]),
    }
    return {"columns": cols, "guess": guess}

def _now():
    return datetime.datetime.utcnow().isoformat() + "Z"

def ingest_step_sqlite(dataset_id: str, file_path: str, mapping: Dict[str, Any], chunk_rows: int = 5000, cancel_cb=None) -> Dict[str, Any]:
    if cancel_cb is None:
        cancel_cb = lambda: False

    ext = os.path.splitext(file_path)[1].lower()
    if ext != ".csv":
        raise RuntimeError("For large datasets on Render, please upload CSV. (XLSX is supported only for small files.)")

    con = connect(); cur = con.cursor()

    # current progress
    cur.execute("SELECT processed_rows FROM ingest_jobs WHERE dataset_id=?", (dataset_id,))
    row = cur.fetchone()
    processed = int(row["processed_rows"] or 0) if row else 0

    # mapping
    def col(name, fallback=None):
        return mapping.get(name) or fallback
    lat_col = col("lat_col"); lon_col = col("lon_col")
    asset_id_col = col("asset_id_col"); label_col = col("label_col") or asset_id_col
    year_col = col("year_col"); scenario_col = col("scenario_col")
    theme_col = col("theme_col"); indicator_col = col("indicator_col")
    value_col = col("value_col") or "Score"
    units_col = col("units_col")

    def to_float(x):
        try:
            if x is None or x == "": return None
            return float(x)
        except: return None
    def to_int(x):
        try:
            if x is None or x == "": return None
            return int(float(x))
        except: return None

    def upsert_asset(aid, label, lat, lon):
        cur.execute("SELECT 1 FROM assets WHERE dataset_id=? AND asset_id=? LIMIT 1", (dataset_id, aid))
        if cur.fetchone():
            cur.execute("UPDATE assets SET label=?, latitude=?, longitude=? WHERE dataset_id=? AND asset_id=?",
                        (label, lat, lon, dataset_id, aid))
        else:
            cur.execute("INSERT INTO assets(dataset_id, asset_id, label, latitude, longitude) VALUES (?,?,?,?,?)",
                        (dataset_id, aid, label, lat, lon))

    inserted = 0
    batch = []

    with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        # skip processed rows
        for _ in range(processed):
            try: next(reader)
            except StopIteration: break

        for r in reader:
            if cancel_cb():
                break
            aid = (r.get(asset_id_col) or "").strip()
            if not aid:
                continue
            lat = to_float(r.get(lat_col)); lon = to_float(r.get(lon_col))
            label = (r.get(label_col) or aid).strip()
            upsert_asset(aid, label, lat, lon)

            batch.append((
                dataset_id,
                aid, lat, lon,
                to_int(r.get(year_col)),
                (r.get(scenario_col) or "").strip() or None,
                (r.get(theme_col) or "").strip() or None,
                (r.get(indicator_col) or "").strip() or None,
                to_float(r.get(value_col)),
                (r.get(units_col) or "").strip() or None
            ))
            if len(batch) >= 2000:
                cur.executemany("INSERT INTO facts(dataset_id, asset_id, latitude, longitude, year, scenario, theme, indicator, value, units) VALUES (?,?,?,?,?,?,?,?,?,?)", batch)
                inserted += len(batch)
                processed += len(batch)
                batch.clear()
                con.commit()
                if inserted >= chunk_rows:
                    break

        if batch and not cancel_cb():
            cur.executemany("INSERT INTO facts(dataset_id, asset_id, latitude, longitude, year, scenario, theme, indicator, value, units) VALUES (?,?,?,?,?,?,?,?,?,?)", batch)
            inserted += len(batch)
            processed += len(batch)
            con.commit()

    # update job progress
    cur.execute("UPDATE ingest_jobs SET processed_rows=?, updated_at=?, error=NULL WHERE dataset_id=?",
                (processed, _now(), dataset_id))
    con.commit()

    # if we inserted less than chunk_rows, assume EOF reached
    done = inserted < chunk_rows

    result = {"processed_rows": processed, "inserted_this_step": inserted, "done": done}
    if done:
        cur.execute("SELECT COUNT(*) AS c FROM facts WHERE dataset_id=?", (dataset_id,))
        row_count = int(cur.fetchone()["c"])
        cur.execute("SELECT COUNT(DISTINCT asset_id) AS c FROM assets WHERE dataset_id=?", (dataset_id,))
        asset_count = int(cur.fetchone()["c"])
        result.update({"row_count": row_count, "asset_count": asset_count})

    con.close()
    return result
