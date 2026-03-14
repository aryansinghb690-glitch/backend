from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from app.temporal.activities import evaluate_decision_activity, run_node_activity


@dataclass
class WorkflowExecutionInput:
    workflow_id: str
    nodes: list[dict[str, Any]]
    edges: list[dict[str, Any]]
    trigger_type: str
    initial_payload: dict[str, Any]


def _merge_payloads(base: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    # When branches meet again later, this keeps nested payload bits from gettin stomped.
    merged: dict[str, Any] = dict(base)
    for key, value in incoming.items():
        existing = merged.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = _merge_payloads(existing, value)
        else:
            merged[key] = value
    return merged


def _get_retry_policy(config: dict[str, Any], default_initial_interval_seconds: float, default_maximum_attempts: int) -> RetryPolicy:
    retry_policy = config.get("retry_policy") if isinstance(config, dict) else None
    if not isinstance(retry_policy, dict):
        return RetryPolicy(
            initial_interval=timedelta(seconds=default_initial_interval_seconds),
            maximum_attempts=default_maximum_attempts,
        )

    initial_interval_raw = retry_policy.get("initial_interval_seconds", default_initial_interval_seconds)
    maximum_attempts_raw = retry_policy.get("maximum_attempts", default_maximum_attempts)

    try:
        initial_interval_seconds = float(initial_interval_raw)
    except (TypeError, ValueError):
        initial_interval_seconds = default_initial_interval_seconds

    try:
        maximum_attempts = int(maximum_attempts_raw)
    except (TypeError, ValueError):
        maximum_attempts = default_maximum_attempts

    if initial_interval_seconds <= 0:
        initial_interval_seconds = default_initial_interval_seconds
    if maximum_attempts < 1:
        maximum_attempts = default_maximum_attempts

    return RetryPolicy(
        initial_interval=timedelta(seconds=initial_interval_seconds),
        maximum_attempts=maximum_attempts,
    )


@workflow.defn(name="sagepilot-workflow")
class SagePilotWorkflow:
    def __init__(self) -> None:
        self._current_node_id: str | None = None
        self._current_node_type: str | None = None
        self._logs: list[dict[str, Any]] = []
        self._status: str = "running"

    @workflow.query
    def get_progress(self) -> dict[str, Any]:
        return {
            "status": self._status,
            "current_node_id": self._current_node_id,
            "current_node_type": self._current_node_type,
            "logs": self._logs,
        }

    @workflow.run
    async def run(self, data: WorkflowExecutionInput) -> dict[str, Any]:
        nodes = {node["id"]: node for node in data.nodes}
        outgoing: dict[str, list[dict[str, Any]]] = {}
        indegree: dict[str, int] = {node_id: 0 for node_id in nodes.keys()}
        for edge in data.edges:
            outgoing.setdefault(edge["source_node_id"], []).append(edge)
            target = edge["target_node_id"]
            if target in indegree:
                indegree[target] += 1

        # Keep the traversal dead simple: build one topo order up front, then walk it.
        topo_queue = [node_id for node_id, degree in indegree.items() if degree == 0]
        topo_order: list[str] = []
        while topo_queue:
            node_id = topo_queue.pop(0)
            topo_order.append(node_id)
            for edge in outgoing.get(node_id, []):
                target = edge["target_node_id"]
                if target not in indegree:
                    continue
                indegree[target] -= 1
                if indegree[target] == 0:
                    topo_queue.append(target)

        if len(topo_order) != len(nodes):
            raise ValueError("Workflow graph is not a DAG")

        trigger_candidates = [
            node_id for node_id, node in nodes.items() if node["type"] == data.trigger_type
        ]
        if not trigger_candidates:
            raise ValueError(f"No trigger node found for trigger_type '{data.trigger_type}'")

        active_payloads: dict[str, dict[str, Any]] = {
            node_id: dict(data.initial_payload) for node_id in trigger_candidates
        }
        logs: list[dict[str, Any]] = []
        final_output: dict[str, Any] | None = None
        step = 0
        self._status = "running"
        self._logs = []
        self._current_node_id = None
        self._current_node_type = None

        for node_id in topo_order:
            payload = active_payloads.get(node_id)
            if payload is None:
                continue

            node = nodes[node_id]
            node_type = node["type"]
            config = node.get("config") or {}
            activity_config = {**config, "__node_label": str(node.get("label") or "")}
            step += 1
            self._current_node_id = node_id
            self._current_node_type = node_type

            if node_type == "wait":
                # Wait stays inside the workflow so Temporal can durably park it.
                duration = float(config.get("duration", 1))
                unit = config.get("unit", "seconds")
                wait_seconds = duration * 60 if unit == "minutes" else duration
                await workflow.sleep(timedelta(seconds=wait_seconds))
                logs.append(
                    {
                        "step": step,
                        "node_id": node_id,
                        "node_type": node_type,
                        "status": "success",
                        "message": f"Paused for {wait_seconds} seconds",
                        "output": payload,
                    }
                )
                self._logs = logs
                for edge in outgoing.get(node_id, []):
                    target_node_id = edge["target_node_id"]
                    existing_target_payload = active_payloads.get(target_node_id)
                    if isinstance(existing_target_payload, dict):
                        active_payloads[target_node_id] = _merge_payloads(existing_target_payload, payload)
                    else:
                        active_payloads[target_node_id] = payload
                continue

            if node_type == "decision":
                try:
                    decision_result = await workflow.execute_activity(
                        evaluate_decision_activity,
                        args=[config, payload],
                        start_to_close_timeout=timedelta(seconds=15),
                        retry_policy=_get_retry_policy(config, default_initial_interval_seconds=1, default_maximum_attempts=2),
                    )
                except ActivityError as exc:
                    cause = exc.cause if exc.cause else exc
                    logs.append(
                        {
                            "step": step,
                            "node_id": node_id,
                            "node_type": node_type,
                            "status": "error",
                            "message": str(cause),
                            "output": None,
                        }
                    )
                    self._logs = logs
                    self._status = "failed"
                    self._current_node_id = None
                    self._current_node_type = None
                    return {"status": "failed", "logs": logs, "final_output": None}
                branch = "true" if decision_result else "false"
                logs.append(
                    {
                        "step": step,
                        "node_id": node_id,
                        "node_type": node_type,
                        "status": "success",
                        "message": f"Condition {branch.upper()} branch selected",
                        "output": {"condition": decision_result},
                    }
                )
                self._logs = logs
                # Only push payload down the branch that actualy won.
                selected_edges = [
                    edge
                    for edge in outgoing.get(node_id, [])
                    if str(edge.get("source_handle") or "").lower() == branch
                ]
                for edge in selected_edges:
                    target_node_id = edge["target_node_id"]
                    existing_target_payload = active_payloads.get(target_node_id)
                    if isinstance(existing_target_payload, dict):
                        active_payloads[target_node_id] = _merge_payloads(existing_target_payload, payload)
                    else:
                        active_payloads[target_node_id] = payload
                continue

            try:
                result = await workflow.execute_activity(
                    run_node_activity,
                    args=[node_type, activity_config, payload],
                    start_to_close_timeout=timedelta(seconds=60),
                    retry_policy=_get_retry_policy(config, default_initial_interval_seconds=2, default_maximum_attempts=3),
                )
            except ActivityError as exc:
                cause = exc.cause if exc.cause else exc
                logs.append(
                    {
                        "step": step,
                        "node_id": node_id,
                        "node_type": node_type,
                        "status": "error",
                        "message": str(cause),
                        "output": None,
                    }
                )
                self._logs = logs
                self._status = "failed"
                self._current_node_id = None
                self._current_node_type = None
                return {"status": "failed", "logs": logs, "final_output": None}

            logs.append(
                {
                    "step": step,
                    "node_id": node_id,
                    "node_type": node_type,
                    "status": "success",
                    "message": "Node executed",
                    "output": result,
                }
            )
            self._logs = logs

            if node_type == "end":
                final_output = result
                continue

            for edge in outgoing.get(node_id, []):
                target_node_id = edge["target_node_id"]
                existing_target_payload = active_payloads.get(target_node_id)
                if isinstance(existing_target_payload, dict):
                    active_payloads[target_node_id] = _merge_payloads(existing_target_payload, result)
                else:
                    active_payloads[target_node_id] = result

        self._status = "completed"
        self._current_node_id = None
        self._current_node_type = None
        return {
            "status": "completed",
            "logs": logs,
            "final_output": final_output,
        }
