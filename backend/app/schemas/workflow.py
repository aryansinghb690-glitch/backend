from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

NodeType = Literal[
    "manual_trigger",
    "webhook_trigger",
    "http_request",
    "transform_data",
    "decision",
    "wait",
    "end",
]


class Position(BaseModel):
    x: float
    y: float


class Node(BaseModel):
    id: str
    type: NodeType
    label: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    position: Position


class Edge(BaseModel):
    id: str
    source_node_id: str
    target_node_id: str
    source_handle: str | None = None


class WorkflowGraph(BaseModel):
    nodes: list[Node]
    edges: list[Edge]


class WorkflowCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    graph: WorkflowGraph


class WorkflowUpdate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    graph: WorkflowGraph


class WorkflowOut(BaseModel):
    workflow_id: str
    name: str
    graph: WorkflowGraph
    created_at: datetime
    updated_at: datetime


class WorkflowExport(BaseModel):
    workflow_id: str
    name: str
    created_at: datetime
    updated_at: datetime
    nodes: list[Node]
    edges: list[Edge]


class WorkflowImport(BaseModel):
    name: str | None = None
    nodes: list[Node]
    edges: list[Edge]


class RunWorkflowRequest(BaseModel):
    trigger_type: Literal["manual_trigger", "webhook_trigger"] = "manual_trigger"
    payload: dict[str, Any] = Field(default_factory=dict)
    graph: WorkflowGraph | None = None


class RunWorkflowResponse(BaseModel):
    run_id: str
    workflow_execution_id: str
    status: str = "accepted"


class ValidationErrorItem(BaseModel):
    code: str
    message: str
    node_id: str | None = None
    details: dict[str, Any] | None = None


class ValidationErrorResponse(BaseModel):
    errors: list[ValidationErrorItem]


class ExecutionLogItem(BaseModel):
    step: int
    node_id: str
    node_type: str
    status: str
    message: str
    output: dict[str, Any] | None = None


class ExecutionOut(BaseModel):
    run_id: str
    workflow_id: str
    trigger_type: str
    status: str
    logs: list[ExecutionLogItem] = Field(default_factory=list)
    final_output: dict[str, Any] | None = None
    error: str | None = None
    started_at: datetime
    finished_at: datetime | None = None
