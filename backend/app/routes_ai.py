from fastapi import APIRouter, HTTPException
from .db import connect

router = APIRouter()

ADAPTATION_GUIDANCE = [
  "Adaptation focuses on actions that reduce vulnerability and exposure (e.g., retrofit, relocation, operational changes).",
  "When Theme='Change', interpret values as a delta across time windows or scenarios. Treat magnitude and direction separately if both exist in your data.",
  "For portfolio triage, use MAX(value) to surface hotspots, then drill into the indicator mix (spider chart) to understand drivers."
]

@router.post("/ai/ask")
def ask(payload: dict):
    question = (payload.get("question") or "").strip().lower()
    dataset_id = payload.get("dataset_id")
    if not question:
        raise HTTPException(400, "question required")

    # Lightweight, deterministic assistant (no external calls).
    if "adapt" in question or "adaptation" in question:
        return {"answer": " ".join(ADAPTATION_GUIDANCE), "type": "adaptation"}
    if "theme" in question and "change" in question:
        return {"answer": "Theme is a grouping field. 'Change' is treated as a normal theme here and remains visible/exportable. Use filters to isolate it.", "type":"definitions"}
    if "score" in question:
        return {"answer": "Score is ingested as 'value'. Charts and rankings use value (MAX aggregation) under the active filters.", "type":"definitions"}

    # Provide dataset-aware hints if possible
    if dataset_id:
        con = connect(); cur = con.cursor()
        cur.execute("SELECT COUNT(DISTINCT indicator) AS n_ind, COUNT(DISTINCT theme) AS n_theme FROM facts WHERE dataset_id=?", (dataset_id,))
        row = cur.fetchone(); con.close()
        if row:
            return {"answer": f"This dataset has about {row['n_ind']} indicators across {row['n_theme']} themes. Ask about a specific indicator/theme or how to interpret 'Change' vs 'Score'.", "type":"dataset"}

    return {"answer": "Ask about: indicator meaning, Theme (Score/Data/Change), scenario/year comparison, or adaptation implications for a sector.", "type":"help"}
