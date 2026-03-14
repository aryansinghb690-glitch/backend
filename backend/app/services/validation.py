from collections import defaultdict, deque
from typing import Any

from app.schemas.workflow import Edge, Node, ValidationErrorItem, WorkflowGraph


TRIGGER_TYPES = {"manual_trigger", "webhook_trigger"}
ACTIVITY_NODE_TYPES = {"manual_trigger", "webhook_trigger", "http_request", "transform_data", "decision", "end"}


def _transform_param(config: dict[str, Any], key: str, default: Any = None) -> Any:
    # We still read from both places here, cuz older payloads were a bit inconsistant.
    parameters = config.get("parameters")
    if isinstance(parameters, dict) and key in parameters:
        return parameters[key]
    return config.get(key, default)


def _validate_node_config(node: Node) -> list[ValidationErrorItem]:
    config = node.config or {}
    errors: list[ValidationErrorItem] = []

    if node.type == "manual_trigger":
        payload = config.get("initial_payload", {})
        if not isinstance(payload, dict):
            errors.append(ValidationErrorItem(code="INVALID_CONFIG", node_id=node.id, message="Manual trigger initial_payload must be a JSON object"))

    if node.type == "webhook_trigger":
        schema = config.get("payload_schema")
        if schema is not None and not isinstance(schema, dict):
            errors.append(ValidationErrorItem(code="INVALID_CONFIG", node_id=node.id, message="Webhook payload_schema must be a JSON object"))

    if node.type == "http_request":
        if not config.get("url"):
            errors.append(ValidationErrorItem(code="INVALID_CONFIG", node_id=node.id, message="HTTP Request requires url"))
        method = str(config.get("method", "GET")).upper()
        if method not in {"GET", "POST"}:
            errors.append(ValidationErrorItem(code="INVALID_CONFIG", node_id=node.id, message="HTTP Request method must be GET or POST"))
        headers = config.get("headers", {})
        if headers and not isinstance(headers, dict):
            errors.append(ValidationErrorItem(code="INVALID_CONFIG", node_id=node.id, message="HTTP Request headers must be key/value object"))

    if node.type == "transform_data":
        transform_type = config.get("transform_type")
        target_field = config.get("target_field")
        parameters = config.get("parameters")
        if parameters is not None and not isinstance(parameters, dict):
            errors.append(ValidationErrorItem(code="INVALID_CONFIG", node_id=node.id, message="Transform Data parameters must be a JSON object"))
        if transform_type not in {"uppercase", "append_text", "multiply_numeric", "rename_key", "extract_key"}:
            errors.append(ValidationErrorItem(code="INVALID_CONFIG", node_id=node.id, message="Transform Data transform_type invalid"))
        if not target_field:
            errors.append(ValidationErrorItem(code="INVALID_CONFIG", node_id=node.id, message="Transform Data requires target_field"))
        if transform_type == "append_text":
            mode = _transform_param(config, "mode", "append")
            if mode not in {"append", "prepend"}:
                errors.append(ValidationErrorItem(code="INVALID_CONFIG", node_id=node.id, message="Transform Data append_text mode must be append or prepend"))
            text = _transform_param(config, "text")
            if text is None:
                errors.append(ValidationErrorItem(code="INVALID_CONFIG", node_id=node.id, message="Transform Data append_text requires text parameter"))
        if transform_type == "multiply_numeric":
            factor = _transform_param(config, "factor")
            if not isinstance(factor, (int, float)):
                errors.append(ValidationErrorItem(code="INVALID_CONFIG", node_id=node.id, message="Transform Data multiply_numeric requires numeric factor"))
        if transform_type == "rename_key":
            new_key = _transform_param(config, "new_key")
            if not isinstance(new_key, str) or not new_key.strip():
                errors.append(ValidationErrorItem(code="INVALID_CONFIG", node_id=node.id, message="Transform Data rename_key requires new_key"))
        if transform_type == "extract_key":
            extract_as = _transform_param(config, "extract_as")
            if extract_as is not None and extract_as != "" and (not isinstance(extract_as, str) or not extract_as.strip()):
                errors.append(ValidationErrorItem(code="INVALID_CONFIG", node_id=node.id, message="Transform Data extract_key extract_as must be a non-empty string when provided"))

    if node.type == "decision":
        field = config.get("field")
        op = config.get("operator")
        if not field:
            errors.append(ValidationErrorItem(code="INVALID_CONFIG", node_id=node.id, message="Decision requires field"))
        if op not in {"equals", "not_equals", "greater_than", "less_than", "contains", "is_empty"}:
            errors.append(ValidationErrorItem(code="INVALID_CONFIG", node_id=node.id, message="Decision operator invalid"))

    if node.type == "wait":
        duration = config.get("duration")
        unit = config.get("unit", "seconds")
        if not isinstance(duration, (int, float)) or duration <= 0:
            errors.append(ValidationErrorItem(code="INVALID_CONFIG", node_id=node.id, message="Wait duration must be positive number"))
        if unit not in {"seconds", "minutes"}:
            errors.append(ValidationErrorItem(code="INVALID_CONFIG", node_id=node.id, message="Wait unit must be seconds or minutes"))

    if node.type in ACTIVITY_NODE_TYPES and "retry_policy" in config:
        retry_policy = config.get("retry_policy")
        if not isinstance(retry_policy, dict):
            errors.append(ValidationErrorItem(code="INVALID_CONFIG", node_id=node.id, message="retry_policy must be a JSON object"))
        else:
            initial_interval_seconds = retry_policy.get("initial_interval_seconds")
            maximum_attempts = retry_policy.get("maximum_attempts")
            if initial_interval_seconds is not None and (not isinstance(initial_interval_seconds, (int, float)) or initial_interval_seconds <= 0):
                errors.append(ValidationErrorItem(code="INVALID_CONFIG", node_id=node.id, message="retry_policy.initial_interval_seconds must be a positive number"))
            if maximum_attempts is not None and (not isinstance(maximum_attempts, int) or maximum_attempts < 1):
                errors.append(ValidationErrorItem(code="INVALID_CONFIG", node_id=node.id, message="retry_policy.maximum_attempts must be an integer >= 1"))

    return errors


def validate_workflow_definition(graph: WorkflowGraph) -> list[ValidationErrorItem]:
    errors: list[ValidationErrorItem] = []
    nodes = graph.nodes
    edges = graph.edges

    if not nodes:
        return [ValidationErrorItem(code="INVALID_GRAPH", message="Workflow must contain nodes")]

    node_map: dict[str, Node] = {node.id: node for node in nodes}
    indegree: dict[str, int] = defaultdict(int)
    outdegree: dict[str, int] = defaultdict(int)
    outgoing: dict[str, list[Edge]] = defaultdict(list)

    for node in nodes:
        errors.extend(_validate_node_config(node))

    for edge in edges:
        if edge.source_node_id not in node_map or edge.target_node_id not in node_map:
            errors.append(ValidationErrorItem(code="INVALID_EDGE", message="Edge references missing node", details=edge.model_dump()))
            continue
        indegree[edge.target_node_id] += 1
        outdegree[edge.source_node_id] += 1
        outgoing[edge.source_node_id].append(edge)

    if not any(node.type in TRIGGER_TYPES for node in nodes):
        errors.append(ValidationErrorItem(code="MISSING_TRIGGER", message="Workflow requires at least one trigger node"))

    if not any(node.type == "end" for node in nodes):
        errors.append(ValidationErrorItem(code="MISSING_END", message="Workflow requires at least one end node"))

    for node in nodes:
        if indegree[node.id] == 0 and outdegree[node.id] == 0:
            errors.append(ValidationErrorItem(code="DISCONNECTED_NODE", node_id=node.id, message="Node is disconnected"))

    for node in nodes:
        if node.type == "decision":
            # For this take-home we just need at least one named branch wired up.
            branches = {str(edge.source_handle or "").lower() for edge in outgoing.get(node.id, [])}
            if "true" not in branches and "false" not in branches:
                errors.append(ValidationErrorItem(code="INVALID_DECISION_BRANCH", node_id=node.id, message="Decision node must connect at least one branch (true/false)"))

    # Plain Kahn topo pass so we can catch cycles without makin this too fancy.
    indegree_copy = {node.id: indegree[node.id] for node in nodes}
    queue = deque([node.id for node in nodes if indegree_copy[node.id] == 0])
    seen = 0

    while queue:
        node_id = queue.popleft()
        seen += 1
        for edge in outgoing.get(node_id, []):
            indegree_copy[edge.target_node_id] -= 1
            if indegree_copy[edge.target_node_id] == 0:
                queue.append(edge.target_node_id)

    if seen != len(nodes):
        errors.append(ValidationErrorItem(code="CYCLE_DETECTED", message="Workflow contains a cycle; only DAGs are supported"))

    return errors
