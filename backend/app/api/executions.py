from datetime import datetime
import re
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models.workflow import ExecutionModel, WorkflowModel
from app.services.temporal_client import get_temporal_client

router = APIRouter(prefix="/executions", tags=["executions"])


LEGACY_HTTP_RESPONSE_KEY_PATTERN = re.compile(r"^response_\d+(?:_\d+)?$")


def _slugify_node_label(label: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", label.strip().lower())
    return slug.strip("_") or "http_req"


def _normalize_api_responses(value: Any, *, labels_in_order: list[str] | None = None) -> Any:
    if not isinstance(value, dict):
        return value

    if not value:
        return value

    values_in_order = list(value.values())

    # Older runs used generic response keys, so we tidy them up for the UI here.
    if labels_in_order and len(labels_in_order) == len(values_in_order):
        renamed: dict[str, Any] = {}
        for index, (label, payload) in enumerate(zip(labels_in_order, values_in_order, strict=False), start=1):
            renamed[f"{label}_response_{index}"] = payload
        return renamed

    keys = list(value.keys())
    if not all(isinstance(key, str) and LEGACY_HTTP_RESPONSE_KEY_PATTERN.match(key) for key in keys):
        return value

    return {f"http_req_response_{index}": payload for index, payload in enumerate(values_in_order, start=1)}


def _extract_http_node_labels_in_execution_order(
    workflow_graph: dict[str, Any] | None,
    logs: list[dict[str, Any]] | None,
) -> list[str]:
    if not isinstance(workflow_graph, dict) or not isinstance(logs, list):
        return []

    node_labels: dict[str, str] = {}
    nodes = workflow_graph.get("nodes")
    if isinstance(nodes, list):
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_id = node.get("id")
            if not isinstance(node_id, str) or not node_id:
                continue
            raw_label = str(node.get("label") or "")
            node_labels[node_id] = _slugify_node_label(raw_label)

    labels_in_order: list[str] = []
    for log_item in logs:
        if not isinstance(log_item, dict):
            continue
        if str(log_item.get("node_type")) != "http_request":
            continue
        if str(log_item.get("status")) != "success":
            continue
        node_id = str(log_item.get("node_id") or "")
        if not node_id:
            labels_in_order.append("http_req")
            continue
        labels_in_order.append(node_labels.get(node_id, "http_req"))

    return labels_in_order


def _normalize_final_output(value: Any, *, labels_in_order: list[str] | None = None) -> Any:
    if not isinstance(value, dict):
        return value
    normalized = dict(value)
    normalized.pop("api_response", None)
    normalized["api_responses"] = _normalize_api_responses(normalized.get("api_responses"), labels_in_order=labels_in_order)
    return normalized


@router.get("/{run_id}")
async def get_execution(run_id: str, db: Session = Depends(get_db)):
    execution = db.get(ExecutionModel, run_id)
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")

    workflow_model = db.get(WorkflowModel, execution.workflow_id)

    current_node_id = None
    current_node_type = None

    if execution.status in {"running", "queued"}:
        try:
            # Best-effort sync from Temporal so the polling panel feels live-ish.
            client = await get_temporal_client()
            handle = client.get_workflow_handle(execution.temporal_workflow_id)
            desc = await handle.describe()
            status_name = str(desc.status.name).lower()

            if status_name == "running":
                execution.status = "running"
                try:
                    progress = await handle.query("get_progress")
                    if isinstance(progress, dict):
                        current_node_id = progress.get("current_node_id")
                        current_node_type = progress.get("current_node_type")
                        progress_logs = progress.get("logs")
                        if isinstance(progress_logs, list):
                            execution.logs = progress_logs
                except Exception:
                    pass
            elif status_name == "completed":
                result = await handle.result()
                execution.status = "completed"
                execution.logs = result.get("logs", [])
                labels_in_order = _extract_http_node_labels_in_execution_order(
                    workflow_model.graph_json if workflow_model else None,
                    execution.logs if isinstance(execution.logs, list) else None,
                )
                execution.final_output = _normalize_final_output(result.get("final_output"), labels_in_order=labels_in_order)
                execution.finished_at = datetime.utcnow()
            elif status_name == "failed":
                execution.status = "failed"
                execution.finished_at = datetime.utcnow()
                try:
                    await handle.result()
                except Exception as exc:
                    execution.error = str(exc)
            else:
                execution.status = status_name
                execution.finished_at = datetime.utcnow()
        except Exception as exc:
            execution.status = "failed"
            execution.error = f"Execution polling failed: {exc}"
            execution.finished_at = datetime.utcnow()

        db.add(execution)
        db.commit()
        db.refresh(execution)

    labels_in_order = _extract_http_node_labels_in_execution_order(
        workflow_model.graph_json if workflow_model else None,
        execution.logs if isinstance(execution.logs, list) else None,
    )

    return {
        "run_id": execution.id,
        "workflow_id": execution.workflow_id,
        "trigger_type": execution.trigger_type,
        "status": execution.status,
        "current_node_id": current_node_id,
        "current_node_type": current_node_type,
        "logs": execution.logs,
        "final_output": _normalize_final_output(execution.final_output, labels_in_order=labels_in_order),
        "error": execution.error,
        "started_at": execution.started_at,
        "finished_at": execution.finished_at,
    }
