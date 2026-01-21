from typing import Optional, List
from fastapi import APIRouter, Query
from .db import connect

router = APIRouter()

def _in_clause(col, values):
    if not values:
        return ("", [])
    placeholders = ",".join(["?"]*len(values))
    return (f" AND {col} IN ({placeholders})", list(values))

@router.get("/datasets/{dataset_id}/filter-options")
def filter_options(dataset_id: str):
    con = connect(); cur = con.cursor()
    def distinct(col):
        cur.execute(f"SELECT DISTINCT {col} AS v FROM facts WHERE dataset_id=? AND {col} IS NOT NULL ORDER BY v", (dataset_id,))
        return [r["v"] for r in cur.fetchall()]
    out = {"years": distinct("year"), "scenarios": distinct("scenario"), "themes": distinct("theme"), "indicators": distinct("indicator")}
    con.close()
    return out

@router.get("/datasets/{dataset_id}/assets")
def list_assets(dataset_id: str, q: Optional[str]=None, limit: int = 5000):
    con = connect(); cur = con.cursor()
    if q:
        cur.execute("SELECT asset_id, latitude, longitude, label FROM assets WHERE dataset_id=? AND (asset_id LIKE ? OR label LIKE ?) LIMIT ?", (dataset_id, f"%{q}%", f"%{q}%", limit))
    else:
        cur.execute("SELECT asset_id, latitude, longitude, label FROM assets WHERE dataset_id=? LIMIT ?", (dataset_id, limit))
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows

@router.get("/datasets/{dataset_id}/facts")
def facts(
    dataset_id: str,
    assets: Optional[List[str]] = Query(default=None),
    years: Optional[List[int]] = Query(default=None),
    scenarios: Optional[List[str]] = Query(default=None),
    themes: Optional[List[str]] = Query(default=None),
    indicators: Optional[List[str]] = Query(default=None),
    limit: int = 5000,
    offset: int = 0,
):
    con = connect(); cur = con.cursor()
    sql = "SELECT asset_id, latitude, longitude, year, scenario, theme, indicator, value, units FROM facts WHERE dataset_id=?"
    params = [dataset_id]
    for col, vals in [("asset_id", assets), ("year", years), ("scenario", scenarios), ("theme", themes), ("indicator", indicators)]:
        clause, p = _in_clause(col, vals)
        sql += clause
        params += p
    sql += " ORDER BY asset_id LIMIT ? OFFSET ?"
    params += [limit, offset]
    cur.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return {"rows": rows, "limit": limit, "offset": offset}

@router.get("/datasets/{dataset_id}/portfolio/top-assets")
def top_assets(
    dataset_id: str,
    years: Optional[List[int]] = Query(default=None),
    scenarios: Optional[List[str]] = Query(default=None),
    themes: Optional[List[str]] = Query(default=None),
    indicators: Optional[List[str]] = Query(default=None),
    top_n: int = 20
):
    con = connect(); cur = con.cursor()
    sql = "SELECT asset_id, MAX(value) AS score FROM facts WHERE dataset_id=?"
    params = [dataset_id]
    for col, vals in [("year", years), ("scenario", scenarios), ("theme", themes), ("indicator", indicators)]:
        clause, p = _in_clause(col, vals)
        sql += clause
        params += p
    sql += " GROUP BY asset_id ORDER BY score DESC LIMIT ?"
    params.append(top_n)
    cur.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


@router.get("/datasets/{dataset_id}/export-csv")
def export_csv(
    dataset_id: str,
    assets: Optional[List[str]] = Query(default=None),
    years: Optional[List[int]] = Query(default=None),
    scenarios: Optional[List[str]] = Query(default=None),
    themes: Optional[List[str]] = Query(default=None),
    indicators: Optional[List[str]] = Query(default=None),
):
    from fastapi.responses import StreamingResponse
    import io, csv
    con = connect(); cur = con.cursor()
    sql = "SELECT asset_id, latitude, longitude, year, scenario, theme, indicator, value, units FROM facts WHERE dataset_id=?"
    params = [dataset_id]
    for col, vals in [("asset_id", assets), ("year", years), ("scenario", scenarios), ("theme", themes), ("indicator", indicators)]:
        clause, p = _in_clause(col, vals)
        sql += clause
        params += p
    sql += " ORDER BY asset_id"
    cur.execute(sql, params)

    def gen():
        out = io.StringIO()
        writer = csv.writer(out)
        writer.writerow(["asset_id","latitude","longitude","year","scenario","theme","indicator","value","units"])
        yield out.getvalue().encode("utf-8"); out.seek(0); out.truncate(0)
        for r in cur:
            writer.writerow([r["asset_id"], r["latitude"], r["longitude"], r["year"], r["scenario"], r["theme"], r["indicator"], r["value"], r["units"]])
            yield out.getvalue().encode("utf-8"); out.seek(0); out.truncate(0)
        con.close()

    return StreamingResponse(gen(), media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="{dataset_id}_export.csv"'})