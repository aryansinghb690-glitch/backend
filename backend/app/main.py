from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.executions import router as execution_router
from app.api.webhooks import router as webhook_router
from app.api.workflows import router as workflow_router
from app.core.config import get_settings
from app.db.base import Base
from app.db.session import engine
from app.models.workflow import ExecutionModel, WorkflowModel  

settings = get_settings()

app = FastAPI(title=settings.APP_NAME)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in settings.CORS_ORIGINS.split(",") if origin.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def on_startup() -> None:
    Base.metadata.create_all(bind=engine)


@app.get("/health")
def health():
    return {"status": "ok"}


app.include_router(workflow_router, prefix=settings.API_PREFIX)
app.include_router(webhook_router, prefix=settings.API_PREFIX)
app.include_router(execution_router, prefix=settings.API_PREFIX)
