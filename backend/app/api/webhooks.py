from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models.workflow import WorkflowModel
from app.schemas.workflow import RunWorkflowRequest
from app.api.workflows import run_workflow

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


@router.post("/{workflow_id}", status_code=202)
async def webhook_trigger(workflow_id: str, request: Request, db: Session = Depends(get_db)):
    model = db.get(WorkflowModel, workflow_id)
    if not model:
        raise HTTPException(status_code=404, detail="Workflow not found")
    payload = await request.json()
    return await run_workflow(
        workflow_id,
        RunWorkflowRequest(trigger_type="webhook_trigger", payload=payload),
        db,
    )
