from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .config import ALLOW_ORIGINS
from .db import init_db
from .routes_upload import router as upload_router
from .routes_datasets import router as datasets_router
from .routes_analytics import router as analytics_router
from .routes_reports import router as reports_router
from .routes_ai import router as ai_router

app = FastAPI(title="ClimSystems Upload POC API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[ALLOW_ORIGINS] if ALLOW_ORIGINS != "*" else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup():
    init_db()

@app.get("/api/health")
def health():
    return {"ok": True}

app.include_router(upload_router, prefix="/api")
app.include_router(datasets_router, prefix="/api")
app.include_router(analytics_router, prefix="/api")
app.include_router(reports_router, prefix="/api")
app.include_router(ai_router, prefix="/api")
