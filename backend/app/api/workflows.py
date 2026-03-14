import uuid
from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.session import get_db
from app.models.workflow import ExecutionModel, WorkflowModel
from app.schemas.workflow import (
    RunWorkflowRequest,
    RunWorkflowResponse,
    ValidationErrorResponse,
    WorkflowCreate,
    WorkflowExport,
    WorkflowImport,
    WorkflowOut,
    WorkflowUpdate,
)
from app.services.temporal_client import get_temporal_client
from app.services.validation import validate_workflow_definition
from app.temporal.workflows import WorkflowExecutionInput

router = APIRouter(prefix="/workflows", tags=["workflows"])


@router.post("", response_model=WorkflowOut)
def create_workflow(payload: WorkflowCreate, db: Session = Depends(get_db)):
    errors = validate_workflow_definition(payload.graph)
    if errors:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=ValidationErrorResponse(errors=errors).model_dump())

    model = WorkflowModel(name=payload.name, graph_json=payload.graph.model_dump())
    db.add(model)
    db.commit()
    db.refresh(model)
    return WorkflowOut(
        workflow_id=model.id,
        name=model.name,
        graph=model.graph_json,
        created_at=model.created_at,
        updated_at=model.updated_at,
    )


@router.put("/{workflow_id}", response_model=WorkflowOut)
def update_workflow(workflow_id: str, payload: WorkflowUpdate, db: Session = Depends(get_db)):
    model = db.get(WorkflowModel, workflow_id)
    if not model:
        raise HTTPException(status_code=404, detail="Workflow not found")

    errors = validate_workflow_definition(payload.graph)
    if errors:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=ValidationErrorResponse(errors=errors).model_dump())

    model.name = payload.name
    model.graph_json = payload.graph.model_dump()
    db.add(model)
    db.commit()
    db.refresh(model)

    return WorkflowOut(
        workflow_id=model.id,
        name=model.name,
        graph=model.graph_json,
        created_at=model.created_at,
        updated_at=model.updated_at,
    )


@router.get("/{workflow_id}", response_model=WorkflowOut)
def get_workflow(workflow_id: str, db: Session = Depends(get_db)):
    model = db.get(WorkflowModel, workflow_id)
    if not model:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return WorkflowOut(
        workflow_id=model.id,
        name=model.name,
        graph=model.graph_json,
        created_at=model.created_at,
        updated_at=model.updated_at,
    )


@router.get("/{workflow_id}/export", response_model=WorkflowExport)
def export_workflow(workflow_id: str, db: Session = Depends(get_db)):
    model = db.get(WorkflowModel, workflow_id)
    if not model:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return WorkflowExport(
        workflow_id=model.id,
        name=model.name,
        created_at=model.created_at,
        updated_at=model.updated_at,
        nodes=model.graph_json["nodes"],
        edges=model.graph_json["edges"],
    )


@router.post("/import", response_model=WorkflowOut)
def import_workflow(payload: WorkflowImport, db: Session = Depends(get_db)):
    graph = {"nodes": [n.model_dump() for n in payload.nodes], "edges": [e.model_dump() for e in payload.edges]}
    from app.schemas.workflow import WorkflowGraph

    parsed = WorkflowGraph(**graph)
    errors = validate_workflow_definition(parsed)
    if errors:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=ValidationErrorResponse(errors=errors).model_dump())

    model = WorkflowModel(name=payload.name or f"Imported Workflow {str(uuid.uuid4())[:8]}", graph_json=parsed.model_dump())
    db.add(model)
    db.commit()
    db.refresh(model)

    return WorkflowOut(
        workflow_id=model.id,
        name=model.name,
        graph=model.graph_json,
        created_at=model.created_at,
        updated_at=model.updated_at,
    )


@router.post("/{workflow_id}/run", response_model=RunWorkflowResponse, status_code=202)
async def run_workflow(workflow_id: str, payload: RunWorkflowRequest, db: Session = Depends(get_db)):
    model = db.get(WorkflowModel, workflow_id)
    if not model:
        raise HTTPException(status_code=404, detail="Workflow not found")

    from app.schemas.workflow import WorkflowGraph

    graph = payload.graph or WorkflowGraph(**model.graph_json)
    errors = validate_workflow_definition(graph)
    if errors:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=ValidationErrorResponse(errors=errors).model_dump())

    has_requested_trigger = any(node.type == payload.trigger_type for node in graph.nodes)
    if not has_requested_trigger:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "MISSING_REQUESTED_TRIGGER",
                "message": f"Workflow does not contain a '{payload.trigger_type}' node. Save a workflow with that trigger type before running.",
            },
        )

    settings = get_settings()
    try:
        client = await get_temporal_client()
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "TEMPORAL_UNAVAILABLE",
                "message": "Temporal service is unavailable. Ensure Temporal server and worker are running.",
            },
        )

    execution_id = str(uuid.uuid4())
    temporal_workflow_id = f"sagepilot-{workflow_id}-{execution_id}"

    try:
        handle = await client.start_workflow(
            "sagepilot-workflow",
            WorkflowExecutionInput(
                workflow_id=workflow_id,
                nodes=[node.model_dump() for node in graph.nodes],
                edges=[edge.model_dump() for edge in graph.edges],
                trigger_type=payload.trigger_type,
                initial_payload=payload.payload,
            ),
            id=temporal_workflow_id,
            task_queue=settings.TEMPORAL_TASK_QUEUE,
            execution_timeout=timedelta(hours=1),
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "WORKFLOW_START_FAILED",
                "message": "Unable to start workflow execution in Temporal.",
            },
        )

    execution = ExecutionModel(
        id=execution_id,
        workflow_id=workflow_id,
        temporal_workflow_id=temporal_workflow_id,
        temporal_run_id=getattr(handle, "run_id", None) or execution_id,
        trigger_type=payload.trigger_type,
        status="running",
    )
    db.add(execution)
    db.commit()

    return RunWorkflowResponse(run_id=execution.id, workflow_execution_id=temporal_workflow_id)


@router.get("/{workflow_id}/executions")
def list_workflow_executions(workflow_id: str, db: Session = Depends(get_db)):
    exists = db.get(WorkflowModel, workflow_id)
    if not exists:
        raise HTTPException(status_code=404, detail="Workflow not found")

    rows = db.execute(
        select(ExecutionModel).where(ExecutionModel.workflow_id == workflow_id).order_by(ExecutionModel.started_at.desc()).limit(25)
    ).scalars().all()

    return [
        {
            "run_id": row.id,
            "status": row.status,
            "trigger_type": row.trigger_type,
            "started_at": row.started_at,
            "finished_at": row.finished_at,
        }
        for row in rows
    ]
