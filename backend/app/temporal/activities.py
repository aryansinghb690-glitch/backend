import re
from copy import deepcopy
from typing import Any

import httpx
from temporalio import activity
from temporalio.exceptions import ApplicationError


def _lookup(payload: dict[str, Any], key: str) -> Any:
    parts = key.split(".")
    value: Any = payload
    for part in parts:
        if not isinstance(value, dict) or part not in value:
            raise KeyError(f"Field '{key}' not found")
        value = value[part]
    return value


def _set_value(payload: dict[str, Any], key: str, value: Any) -> None:
    parts = key.split(".")
    current = payload
    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], dict):
            current[part] = {}
        current = current[part]
    current[parts[-1]] = value


def _delete_value(payload: dict[str, Any], key: str) -> None:
    parts = key.split(".")
    current: Any = payload
    for part in parts[:-1]:
        if not isinstance(current, dict) or part not in current:
            return
        current = current[part]
    if isinstance(current, dict):
        current.pop(parts[-1], None)


def _render_template(value: Any, payload: dict[str, Any]) -> Any:
    if isinstance(value, str):
        matches = re.findall(r"\{\{\s*([\w\.]+)\s*\}\}", value)
        rendered = value
        for key in matches:
            try:
                rendered = rendered.replace("{{" + key + "}}", str(_lookup(payload, key)))
                rendered = rendered.replace("{{ " + key + " }}", str(_lookup(payload, key)))
            except Exception:
                continue
        return rendered
    if isinstance(value, dict):
        return {k: _render_template(v, payload) for k, v in value.items()}
    if isinstance(value, list):
        return [_render_template(item, payload) for item in value]
    return value


def _combine_text(left: str, right: str) -> str:
    if not left or not right:
        return left + right
    if left[-1].isspace() or right[0].isspace():
        return left + right
    return f"{left} {right}"


def _transform_param(config: dict[str, Any], key: str, default: Any = None) -> Any:
    parameters = config.get("parameters")
    if isinstance(parameters, dict) and key in parameters:
        return parameters[key]
    return config.get(key, default)


def _next_unique_key(existing: dict[str, Any], preferred: str) -> str:
    if preferred not in existing:
        return preferred

    suffix = 2
    while f"{preferred}_{suffix}" in existing:
        suffix += 1
    return f"{preferred}_{suffix}"


def _slugify_node_label(label: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", label.strip().lower())
    return slug.strip("_")


@activity.defn
async def run_node_activity(
    node_type: str,
    config: dict[str, Any],
    payload: dict[str, Any],
) -> dict[str, Any]:
    working = deepcopy(payload)

    if node_type in {"manual_trigger", "webhook_trigger", "end"}:
        return working

    if node_type == "http_request":
        method = str(config.get("method", "GET")).upper()
        url = str(config["url"])
        headers = config.get("headers") or {}
        body_template = config.get("body_template")
        body = _render_template(body_template, working) if body_template is not None else None
        # Let network-ish errors bubble up (timeouts, conn refused, etc) so
        # Temporal retry can kick in for flaky stuff automaticaly.
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.request(method=method, url=url, headers=headers, json=body)
        body_content: Any
        try:
            body_content = response.json()
        except Exception:
            body_content = response.text
        response_payload = {
            "status": response.status_code,
            "ok": response.status_code < 400,
            "body": body_content,
        }
        existing_responses = working.get("api_responses")
        if not isinstance(existing_responses, dict):
            existing_responses = {}
        configured_output_key = config.get("output_key")
        if configured_output_key:
            response_key = _next_unique_key(existing_responses, str(configured_output_key))
        else:
            # Fall back to node label so chained HTTP nodes stay readable in the final output.
            node_label = str(config.get("__node_label") or "")
            node_key_prefix = _slugify_node_label(node_label) or "http_req"
            index = len(existing_responses) + 1
            response_key = f"{node_key_prefix}_response_{index}"
        return {
            **working,
            "api_responses": {
                **existing_responses,
                response_key: response_payload,
            },
        }

    if node_type == "transform_data":
        transform_type = config.get("transform_type")
        target_field = str(config.get("target_field"))
        try:
            # These transforms are intentionally basic, just enough to show data moving around.
            if transform_type == "uppercase":
                value = _lookup(working, target_field)
                _set_value(working, target_field, str(value).upper())
                return working

            if transform_type == "append_text":
                value = _lookup(working, target_field)
                mode = _transform_param(config, "mode", "append")
                text = str(_transform_param(config, "text", ""))
                if mode == "prepend":
                    _set_value(working, target_field, _combine_text(text, str(value)))
                else:
                    _set_value(working, target_field, _combine_text(str(value), text))
                return working

            if transform_type == "multiply_numeric":
                value = _lookup(working, target_field)
                factor = float(_transform_param(config, "factor", 1))
                _set_value(working, target_field, float(value) * factor)
                return working

            if transform_type == "rename_key":
                new_key = str(_transform_param(config, "new_key", ""))
                _set_value(working, target_field, new_key)
                return working

            if transform_type == "extract_key":
                value = _lookup(working, target_field)
                extract_as = str(_transform_param(config, "extract_as") or target_field.split(".")[-1])
                return {extract_as: value}

            raise ApplicationError("Unsupported transform_type", non_retryable=True)

        except ApplicationError:
            raise
        except KeyError as exc:
            raise ApplicationError(str(exc), non_retryable=True) from exc
        except (ValueError, TypeError) as exc:
            raise ApplicationError(f"Transform error on field '{target_field}': {exc}", non_retryable=True) from exc

    return working


@activity.defn
async def evaluate_decision_activity(config: dict[str, Any], payload: dict[str, Any]) -> bool:
    field = str(config.get("field"))
    op = str(config.get("operator"))
    expected = config.get("value")

    try:
        actual = _lookup(payload, field)
    except Exception:
        actual = None

    if op == "equals":
        return actual == expected
    if op == "not_equals":
        return actual != expected
    if op == "greater_than":
        return float(actual) > float(expected)
    if op == "less_than":
        return float(actual) < float(expected)
    if op == "contains":
        return str(expected) in str(actual)
    if op == "is_empty":
        return actual in (None, "", [], {})

    return False
