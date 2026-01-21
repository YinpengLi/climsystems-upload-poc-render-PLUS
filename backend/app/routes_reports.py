import io, json, datetime
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from .db import connect

router = APIRouter()

def _now():
    return datetime.datetime.utcnow().isoformat() + "Z"

def _fetch_asset_rows(con, dataset_id: str, asset_id: str, filters: dict):
    cur = con.cursor()
    sql = "SELECT year, scenario, theme, indicator, value FROM facts WHERE dataset_id=? AND asset_id=?"
    params = [dataset_id, asset_id]
    for k, col in [("years","year"), ("scenarios","scenario"), ("themes","theme"), ("indicators","indicator")]:
        vals = filters.get(k)
        if vals:
            sql += " AND " + col + " IN (" + ",".join(["?"]*len(vals)) + ")"
            params += vals
    cur.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]

def _radar_png(rows):
    by = {}
    for r in rows:
        ind = r.get("indicator") or "Unknown"
        v = r.get("value")
        if v is None: 
            continue
        by[ind] = max(by.get(ind, float("-inf")), float(v))
    fig = plt.figure(figsize=(6,4))
    if not by:
        plt.text(0.5,0.5,"No data", ha="center", va="center")
    else:
        import numpy as np
        labels = list(by.keys())[:12]
        values = [by[l] for l in labels]
        angles = np.linspace(0, 2*np.pi, len(labels), endpoint=False).tolist()
        values += values[:1]
        angles += angles[:1]
        ax = plt.subplot(111, polar=True)
        ax.plot(angles, values)
        ax.fill(angles, values, alpha=0.25)
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(labels, fontsize=7)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf

@router.post("/reports/preview")
def preview(payload: dict):
    dataset_id = payload.get("dataset_id")
    rtype = payload.get("type", "asset")
    asset_id = payload.get("asset_id")
    filters = payload.get("filters", {})
    if not dataset_id:
        raise HTTPException(400, "dataset_id required")
    if rtype == "asset" and not asset_id:
        raise HTTPException(400, "asset_id required for asset report")

    con = connect()
    rows = _fetch_asset_rows(con, dataset_id, asset_id, filters)
    con.close()

    radar = _radar_png(rows)

    pdf = io.BytesIO()
    c = canvas.Canvas(pdf, pagesize=A4)
    w, h = A4
    c.setFont("Helvetica-Bold", 16)
    c.drawString(40, h-60, "Climate Risk Report (Upload POC)")
    c.setFont("Helvetica", 10)
    c.drawString(40, h-80, f"Generated: {_now()}")
    c.drawString(40, h-95, f"Dataset: {dataset_id}")
    c.drawString(40, h-110, f"Asset: {asset_id}")

    c.setFont("Helvetica-Bold", 12)
    c.drawString(40, h-140, "Filters snapshot")
    c.setFont("Helvetica", 9)
    fs = json.dumps(filters, ensure_ascii=False)
    c.drawString(40, h-155, fs[:110])
    if len(fs) > 110:
        c.drawString(40, h-168, fs[110:220])

    c.setFont("Helvetica-Bold", 12)
    c.drawString(40, h-200, "Spider (radar) — MAX(value) by indicator (capped to 12)")

    from reportlab.lib.utils import ImageReader
    img = ImageReader(radar)
    c.drawImage(img, 40, h-520, width=520, height=300, preserveAspectRatio=True, mask='auto')

    c.showPage()
    c.save()
    pdf.seek(0)
    return StreamingResponse(pdf, media_type="application/pdf")


@router.post("/reports/preview-portfolio")
def preview_portfolio(payload: dict):
    dataset_id = payload.get("dataset_id")
    filters = payload.get("filters", {})
    top_n = int(payload.get("top_n", 20))
    if not dataset_id:
        raise HTTPException(400, "dataset_id required")

    con = connect(); cur = con.cursor()
    sql = "SELECT asset_id, MAX(value) AS score FROM facts WHERE dataset_id=?"
    params = [dataset_id]
    for k, col in [("years","year"), ("scenarios","scenario"), ("themes","theme"), ("indicators","indicator")]:
        vals = filters.get(k)
        if vals:
            sql += " AND " + col + " IN (" + ",".join(["?"]*len(vals)) + ")"
            params += vals
    sql += " GROUP BY asset_id ORDER BY score DESC LIMIT ?"
    params.append(top_n)
    cur.execute(sql, params)
    top_rows = [dict(r) for r in cur.fetchall()]
    con.close()

    pdf = io.BytesIO()
    c = canvas.Canvas(pdf, pagesize=A4)
    w, h = A4
    c.setFont("Helvetica-Bold", 16)
    c.drawString(40, h-60, "Climate Risk Portfolio Report (Upload POC)")
    c.setFont("Helvetica", 10)
    c.drawString(40, h-80, f"Generated: {_now()}")
    c.drawString(40, h-95, f"Dataset: {dataset_id}")

    c.setFont("Helvetica-Bold", 12)
    c.drawString(40, h-125, "Top assets by MAX(value) under current filters")
    c.setFont("Helvetica", 9)
    y = h-145
    c.drawString(40, y, "Asset ID")
    c.drawString(300, y, "MAX(value)")
    y -= 10
    c.line(40, y, 560, y)
    y -= 12
    for r in top_rows:
        if y < 80:
            c.showPage()
            y = h-60
        c.drawString(40, y, str(r.get("asset_id","")))
        c.drawRightString(540, y, str(r.get("score","")))
        y -= 12

    c.setFont("Helvetica", 9)
    c.drawString(40, 60, "Note: Theme='Change' remains visible and exportable. Use filters to isolate Score/Data/Change.")
    c.showPage()
    c.save()
    pdf.seek(0)
    return StreamingResponse(pdf, media_type="application/pdf")