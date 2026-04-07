import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable


class PlaybookValidationError(ValueError):
    pass


@dataclass
class PlaybookRunResult:
    name: str
    success: bool
    logs: list[dict]
    paused: bool = False
    prompt_message: str = ""
    resume: dict | None = None


@dataclass
class PlaybookExecutionOutcome:
    status: str
    remaining_steps: list[dict]
    prompt_message: str = ""
    paused_step_id: str = ""
    paused_step_name: str = ""


class DocsPlaybook:
    SCHEMA_VERSION = 1
    SUPPORTED_TRIGGERS = {"everything"}
    SUPPORTED_ACTIONS = {
        "create_note",
        "update_note",
        "create_learning",
        "generate_ai_questions",
        "generate_ai_feedback",
        "add_note_tags",
        "create_todo",
        "update_todo",
        "delete_todo",
        "create_deadline",
        "inform_user",
        "perform_note_sync",
        "check_note_exists",
        "check_todo_exists",
        "check_note_compliant",
        "check_ai_feedback_min_score",
    }
    SUPPORTED_FLOW_OPS = {"if_else", "switch_case", "abort"}

    def __init__(self, conf: dict, action_handlers: dict[str, Callable[[dict, dict], dict]] | None = None) -> None:
        self.logger = logging.getLogger(__name__)
        playbooks_conf = conf.get("playbooks", {}) if isinstance(conf.get("playbooks", {}), dict) else {}
        self.enabled = bool(playbooks_conf.get("enabled", True))
        self.root_path = Path(playbooks_conf.get("path", "/the-knowledge/08_PLAYBOOKS")).resolve()
        self.max_depth = int(playbooks_conf.get("max_depth", 30))
        self.dry_run = bool(playbooks_conf.get("dry_run", False))
        self.action_handlers = action_handlers or {}

    def ensure_storage(self) -> None:
        self.root_path.mkdir(parents=True, exist_ok=True)

    def _safe_name(self, name: str) -> str:
        sanitized = re.sub(r"[^A-Za-z0-9._ -]+", "_", str(name or "").strip()).strip(" ._")
        if not sanitized:
            raise PlaybookValidationError("Playbook name is required.")
        if len(sanitized) > 120:
            raise PlaybookValidationError("Playbook name is too long.")
        return sanitized

    def _path_for_name(self, name: str) -> Path:
        safe_name = self._safe_name(name)
        target = (self.root_path / f"{safe_name}.md").resolve()
        if self.root_path not in target.parents:
            raise PlaybookValidationError("Invalid playbook path.")
        return target

    def _normalize_block_layout(self, layout: dict) -> dict:
        blocks = layout.get("blocks", []) if isinstance(layout.get("blocks", []), list) else []
        edges = layout.get("edges", []) if isinstance(layout.get("edges", []), list) else []

        normalized_blocks: list[dict] = []
        for block in blocks:
            if not isinstance(block, dict):
                continue
            block_id = str(block.get("id", "")).strip()
            block_type = str(block.get("type", "")).strip().lower()
            if not block_id or block_type not in {"trigger", "action", "flow"}:
                continue

            payload = block.get("payload", {}) if isinstance(block.get("payload", {}), dict) else {}
            normalized_blocks.append(
                {
                    "id": block_id,
                    "type": block_type,
                    "x": int(float(block.get("x", 0) or 0)),
                    "y": int(float(block.get("y", 0) or 0)),
                    "payload": payload,
                }
            )

        normalized_edges: list[dict] = []
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            source = str(edge.get("source", "")).strip()
            target = str(edge.get("target", "")).strip()
            branch = str(edge.get("branch", "next")).strip()
            if not source or not target:
                continue
            branch_key = branch.lower()
            if branch_key in {"next", "true", "false", "default"}:
                branch = branch_key
            elif not branch:
                branch = "next"
            normalized_edges.append({"source": source, "target": target, "branch": branch})

        normalized_blocks.sort(key=lambda item: (item["y"], item["x"], item["id"]))
        normalized_edges.sort(key=lambda item: (item["source"], item["branch"], item["target"]))
        return {"blocks": normalized_blocks, "edges": normalized_edges}

    def _build_definition_from_layout(self, layout: dict) -> dict:
        block_map = {block["id"]: block for block in layout["blocks"]}
        edges_by_source: dict[str, list[dict]] = {}
        for edge in layout["edges"]:
            edges_by_source.setdefault(edge["source"], []).append(edge)

        trigger_blocks = [block for block in layout["blocks"] if block["type"] == "trigger"]
        if len(trigger_blocks) != 1:
            raise PlaybookValidationError("Exactly one trigger block is required.")

        trigger_block = trigger_blocks[0]
        trigger_type = str(trigger_block.get("payload", {}).get("trigger_type", "")).strip().lower()
        if trigger_type not in self.SUPPORTED_TRIGGERS:
            raise PlaybookValidationError("Unsupported trigger type.")

        definition_steps: list[dict] = []
        visited: set[str] = set()

        def _walk(block_id: str, depth: int = 0) -> list[dict]:
            if depth > self.max_depth:
                raise PlaybookValidationError("Playbook graph exceeds max depth.")
            if block_id in visited:
                raise PlaybookValidationError("Cycle detected in playbook graph.")
            if block_id not in block_map:
                raise PlaybookValidationError("Edge points to unknown block.")

            visited.add(block_id)
            block = block_map[block_id]
            block_type = block["type"]
            payload = block.get("payload", {})
            outgoing = edges_by_source.get(block_id, [])

            if block_type == "action":
                if self._is_placeholder_action(payload):
                    next_edges = [edge for edge in outgoing if edge["branch"] == "next"]
                    if len(next_edges) > 1:
                        raise PlaybookValidationError("Branch placeholder may only have one next edge.")
                    if next_edges:
                        return _walk(next_edges[0]["target"], depth + 1)
                    return []
                action = str(payload.get("action", "")).strip()
                if action not in self.SUPPORTED_ACTIONS:
                    raise PlaybookValidationError(f"Unsupported action: {action}")
                step = {
                    "id": block_id,
                    "type": "action",
                    "action": action,
                    "label": str(payload.get("label", "")).strip(),
                    "input": payload.get("input", {}),
                }
                next_edges = [edge for edge in outgoing if edge["branch"] == "next"]
                if len(next_edges) > 1:
                    raise PlaybookValidationError("Action block may only have one next edge.")
                chain = [step]
                if next_edges:
                    chain.extend(_walk(next_edges[0]["target"], depth + 1))
                return chain

            if block_type == "flow":
                operator = str(payload.get("operator", "")).strip().lower()
                if operator not in self.SUPPORTED_FLOW_OPS:
                    raise PlaybookValidationError("Unsupported flow operator.")
                next_edges = [edge for edge in outgoing if edge["branch"] == "next"]
                if len(next_edges) > 1:
                    raise PlaybookValidationError("Flow block may only have one next edge.")
                next_steps = _walk(next_edges[0]["target"], depth + 1) if next_edges else []
                if operator == "abort":
                    non_next_edges = [edge for edge in outgoing if edge["branch"] != "next"]
                    if non_next_edges:
                        raise PlaybookValidationError("abort flow cannot define branch edges.")
                    return [
                        {
                            "id": block_id,
                            "type": "flow",
                            "operator": "abort",
                            "label": str(payload.get("label", "")).strip(),
                            "input": {},
                            "next_steps": next_steps,
                        }
                    ]
                if operator == "if_else":
                    true_edges = [edge for edge in outgoing if edge["branch"] == "true"]
                    false_edges = [edge for edge in outgoing if edge["branch"] == "false"]
                    if len(true_edges) != 1 or len(false_edges) != 1:
                        raise PlaybookValidationError("if_else flow requires one true and one false branch.")
                    return [
                        {
                            "id": block_id,
                            "type": "flow",
                            "operator": "if_else",
                            "label": str(payload.get("label", "")).strip(),
                            "input": payload.get("input", {}),
                            "true_branch": _walk(true_edges[0]["target"], depth + 1),
                            "false_branch": _walk(false_edges[0]["target"], depth + 1),
                            "next_steps": next_steps,
                        }
                    ]

                cases = payload.get("cases", []) if isinstance(payload.get("cases", []), list) else []
                normalized_cases: list[dict] = []
                seen_case_keys: set[str] = set()
                for case in cases:
                    if not isinstance(case, dict):
                        continue
                    case_key = str(case.get("key", "")).strip()
                    case_value = str(case.get("value", "")).strip()
                    case_label = str(case.get("label", "")).strip()
                    if not case_key:
                        raise PlaybookValidationError("switch_case flow contains an invalid case key.")
                    if case_key in seen_case_keys:
                        raise PlaybookValidationError("switch_case flow contains duplicate case keys.")
                    seen_case_keys.add(case_key)
                    normalized_cases.append({"key": case_key, "value": case_value, "label": case_label or case_value or case_key})

                if not normalized_cases:
                    raise PlaybookValidationError("switch_case flow requires at least one case.")

                switch_cases: list[dict] = []
                for case in normalized_cases:
                    case_edges = [edge for edge in outgoing if edge["branch"] == case["key"]]
                    if len(case_edges) != 1:
                        raise PlaybookValidationError(f"switch_case flow requires exactly one branch for case '{case['label']}'.")
                    switch_cases.append(
                        {
                            "key": case["key"],
                            "value": case["value"],
                            "label": case["label"],
                            "steps": _walk(case_edges[0]["target"], depth + 1),
                        }
                    )

                default_edges = [edge for edge in outgoing if edge["branch"] == "default"]
                if len(default_edges) != 1:
                    raise PlaybookValidationError("switch_case flow requires exactly one default branch.")

                return [
                    {
                        "id": block_id,
                        "type": "flow",
                        "operator": "switch_case",
                        "label": str(payload.get("label", "")).strip(),
                        "input": payload.get("input", {}),
                        "cases": switch_cases,
                        "default_branch": _walk(default_edges[0]["target"], depth + 1),
                        "next_steps": next_steps,
                    }
                ]

            raise PlaybookValidationError("Trigger blocks cannot be nested in graph.")

        trigger_next = [edge for edge in edges_by_source.get(trigger_block["id"], []) if edge["branch"] == "next"]
        if len(trigger_next) != 1:
            raise PlaybookValidationError("Trigger block must have exactly one next edge.")

        definition_steps = _walk(trigger_next[0]["target"])
        orphan_blocks = set(block_map.keys()) - visited - {trigger_block["id"]}
        if orphan_blocks:
            raise PlaybookValidationError("Playbook contains orphan blocks.")

        return {
            "trigger": {
                "type": trigger_type,
            },
            "steps": definition_steps,
        }

    def _is_placeholder_action(self, payload: dict) -> bool:
        if not isinstance(payload, dict):
            return False
        if bool(payload.get("placeholder")):
            return True
        action_name = str(payload.get("action", "")).strip()
        label = str(payload.get("label", "")).strip().lower()
        return action_name == "create_todo" and label.endswith("branch action")

    def validate_schema(self, payload: dict) -> dict:
        name = self._safe_name(str(payload.get("name", "")))
        layout = self._normalize_block_layout(payload.get("layout", {}))
        definition = self._build_definition_from_layout(layout)
        return {
            "schema_version": self.SCHEMA_VERSION,
            "name": name,
            "description": str(payload.get("description", "")).strip(),
            "definition": definition,
            "layout": layout,
        }

    def serialize_markdown(self, validated: dict) -> str:
        frontmatter = json.dumps(validated, ensure_ascii=False, indent=2, sort_keys=True)
        return f"---\n{frontmatter}\n---\n\n# {validated['name']}\n\nManaged by Playbooks editor.\n"

    def parse_markdown(self, content: str) -> dict:
        match = re.match(r"^---\n(.*?)\n---\n", str(content), flags=re.DOTALL)
        if not match:
            raise PlaybookValidationError("Invalid playbook file format.")
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError as exc:
            raise PlaybookValidationError(f"Playbook file has invalid JSON: {exc}") from exc

    def list_playbooks(self) -> list[dict]:
        self.ensure_storage()
        items: list[dict] = []
        for file_path in sorted(self.root_path.glob("*.md"), key=lambda value: value.name.casefold()):
            stat_result = file_path.stat()
            content = file_path.read_text(encoding="utf-8")
            config = self.parse_markdown(content)
            items.append(
                {
                    "name": file_path.stem,
                    "file_name": file_path.name,
                    "created_at": datetime.fromtimestamp(stat_result.st_ctime).isoformat(),
                    "modified_at": datetime.fromtimestamp(stat_result.st_mtime).isoformat(),
                    "config": config,
                }
            )
        return items

    def get_playbook(self, name: str) -> dict:
        target = self._path_for_name(name)
        if not target.exists():
            raise FileNotFoundError("Playbook does not exist.")
        stat_result = target.stat()
        parsed = self.parse_markdown(target.read_text(encoding="utf-8"))
        return {
            "name": target.stem,
            "file_name": target.name,
            "created_at": datetime.fromtimestamp(stat_result.st_ctime).isoformat(),
            "modified_at": datetime.fromtimestamp(stat_result.st_mtime).isoformat(),
            "config": parsed,
        }

    def save_playbook(self, payload: dict) -> dict:
        self.ensure_storage()
        validated = self.validate_schema(payload)
        target = self._path_for_name(validated["name"])
        target.write_text(self.serialize_markdown(validated), encoding="utf-8")
        return self.get_playbook(validated["name"])

    def delete_playbook(self, name: str) -> None:
        target = self._path_for_name(name)
        if target.exists() and target.is_file():
            target.unlink()

    def should_run_for_trigger(self, config: dict, context: dict) -> bool:
        trigger = config.get("definition", {}).get("trigger", {})
        trigger_type = str(trigger.get("type", "")).strip().lower()
        return trigger_type == "everything"

    def _interpolate_context_string(self, value: object, context: dict) -> object:
        if not isinstance(value, str):
            return value

        def _replace(match: re.Match[str]) -> str:
            context_key = str(match.group(1) or "").strip()
            if not context_key:
                return ""
            return str(context.get(context_key, ""))

        return re.sub(r"\{([^{}]+)\}", _replace, value)

    def _coerce_comparable_value(self, value: object) -> object:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return value
        normalized = str(value or "").strip()
        lowered = normalized.lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
        return normalized

    def _evaluate_if_else(self, flow_input: dict, context: dict) -> bool:
        candidate = self._resolve_flow_value(flow_input, context)
        right = self._interpolate_context_string(flow_input.get("equals", "true"), context)
        comparison = str(flow_input.get("comparison", "equals")).strip().lower()
        if comparison == "greater_than":
            try:
                return float(candidate) > float(right)
            except (TypeError, ValueError):
                return False
        if comparison == "less_than":
            try:
                return float(candidate) < float(right)
            except (TypeError, ValueError):
                return False
        return self._coerce_comparable_value(candidate) == self._coerce_comparable_value(right)

    def _resolve_flow_value(self, flow_input: dict, context: dict) -> object:
        raw_left = flow_input.get("value_to_compare", "")
        source = str(flow_input.get("source", "literal")).strip().lower()
        if source == "context":
            context_key = str(raw_left or "").strip()
            placeholder_match = re.fullmatch(r"\{([^{}]+)\}", context_key)
            if placeholder_match:
                context_key = str(placeholder_match.group(1) or "").strip()
            elif not context_key:
                context_key = str(self._interpolate_context_string(raw_left, context)).strip()
            return context.get(context_key, "")
        return self._interpolate_context_string(raw_left, context)

    def _flow_output_base(self, step: dict) -> str:
        raw_name = str(step.get("label") or step.get("operator") or step.get("id") or "flow").strip().lower()
        sanitized = re.sub(r"[^a-z0-9]+", "_", raw_name).strip("_")
        return sanitized or "flow"

    def _resolve_action_input(self, action_input: dict, context: dict) -> dict:
        resolved: dict[str, object] = {}
        for key, value in (context or {}).items():
            if str(value).strip():
                resolved[key] = value
        if isinstance(action_input, dict):
            for key, value in action_input.items():
                if key.startswith("override_context__"):
                    continue
                interpolated_value = self._interpolate_context_string(value, resolved)
                if str(interpolated_value).strip():
                    override_key = f"override_context__{key}"
                    override_field = str(action_input.get(override_key, "")).strip().lower() in {"1", "true", "yes", "on"}
                    if override_field or key not in resolved:
                        resolved[key] = interpolated_value
        return resolved

    def _execute_steps(self, steps: list[dict], context: dict, logs: list[dict]) -> PlaybookExecutionOutcome:
        success = True
        for index, step in enumerate(steps):
            remaining_after_current = steps[index + 1:]
            step_type = step.get("type")
            step_name = str(step.get("label") or step.get("action") or step.get("operator") or step.get("id") or step_type or "step").strip()
            if step_type == "action":
                action_name = str(step.get("action", "")).strip()
                handler = self.action_handlers.get(action_name)
                resolved_input = self._resolve_action_input(step.get("input", {}), context)
                self.logger.info(
                    "Playbook action starting: action=%s step=%s step_id=%s input_keys=%s",
                    action_name,
                    step_name,
                    step.get("id"),
                    sorted(resolved_input.keys()),
                )
                if handler is None:
                    self.logger.error(
                        "Playbook action missing handler: action=%s step=%s step_id=%s",
                        action_name,
                        step_name,
                        step.get("id"),
                    )
                    logs.append({
                        "step_id": step.get("id"),
                        "step_type": "action",
                        "step_name": step_name,
                        "action": action_name,
                        "success": False,
                        "reason": f"No handler for action {action_name}",
                    })
                    return PlaybookExecutionOutcome(status="failed", remaining_steps=[])
                try:
                    if not self.dry_run:
                        result = handler(resolved_input, context) or {}
                        prompt_message = ""
                        action_status = ""
                        action_control = ""
                        if isinstance(result, dict):
                            prompt_message = str(result.get("prompt_message", "")).strip()
                            action_status = str(result.get("status", "")).strip()
                            action_control = str(result.get("control", "")).strip().lower()
                            for key, value in result.items():
                                if key in {"status", "prompt_message", "control"}:
                                    continue
                                context[key] = value
                        reason = "Action executed."
                        if action_status:
                            reason = f"Action executed ({action_status})."
                        self.logger.info(
                            "Playbook action finished: action=%s step=%s step_id=%s status=%s result_keys=%s",
                            action_name,
                            step_name,
                            step.get("id"),
                            action_status or "ok",
                            sorted(result.keys()) if isinstance(result, dict) else [],
                        )
                        logs.append({
                            "step_id": step.get("id"),
                            "step_type": "action",
                            "step_name": step_name,
                            "action": action_name,
                            "success": True,
                            "reason": reason,
                            "prompt_message": prompt_message,
                        })
                        if action_control == "pause":
                            self.logger.info(
                                "Playbook execution paused by action: action=%s step=%s step_id=%s",
                                action_name,
                                step_name,
                                step.get("id"),
                            )
                            return PlaybookExecutionOutcome(
                                status="paused",
                                remaining_steps=remaining_after_current,
                                prompt_message=prompt_message,
                                paused_step_id=str(step.get("id") or "").strip(),
                                paused_step_name=step_name,
                            )
                        if action_control == "abort":
                            self.logger.info(
                                "Playbook execution aborted by action: action=%s step=%s step_id=%s",
                                action_name,
                                step_name,
                                step.get("id"),
                            )
                            return PlaybookExecutionOutcome(status="aborted", remaining_steps=[])
                    else:
                        self.logger.info(
                            "Playbook action skipped by dry run: action=%s step=%s step_id=%s",
                            action_name,
                            step_name,
                            step.get("id"),
                        )
                        logs.append({
                            "step_id": step.get("id"),
                            "step_type": "action",
                            "step_name": step_name,
                            "action": action_name,
                            "success": True,
                            "reason": "Dry run skipped action execution.",
                        })
                except Exception as exc:
                    self.logger.exception(
                        "Playbook action failed: action=%s step=%s step_id=%s",
                        action_name,
                        step_name,
                        step.get("id"),
                    )
                    logs.append({
                        "step_id": step.get("id"),
                        "step_type": "action",
                        "step_name": step_name,
                        "action": action_name,
                        "success": False,
                        "reason": str(exc),
                    })
                    return PlaybookExecutionOutcome(status="failed", remaining_steps=[])
            elif step_type == "flow":
                operator = str(step.get("operator", "")).strip()
                if operator == "abort":
                    self.logger.info(
                        "Playbook flow aborted run: flow=%s step=%s step_id=%s",
                        operator,
                        step_name,
                        step.get("id"),
                    )
                    logs.append({
                        "step_id": step.get("id"),
                        "step_type": "flow",
                        "step_name": step_name,
                        "flow": operator,
                        "success": False,
                        "reason": "Abort flow stopped the workflow.",
                    })
                    return PlaybookExecutionOutcome(status="aborted", remaining_steps=[])
                if operator == "if_else":
                    is_true = self._evaluate_if_else(step.get("input", {}), context)
                    branch_key = "true_branch" if is_true else "false_branch"
                    output_base = self._flow_output_base(step)
                    context[f"{output_base}_result"] = "true" if is_true else "false"
                    context[f"{output_base}_branch"] = "true" if is_true else "false"
                    self.logger.info(
                        "Playbook flow routed: flow=%s step=%s step_id=%s branch=%s compared_key=%s comparison=%s expected=%s",
                        operator,
                        step_name,
                        step.get("id"),
                        branch_key,
                        step.get("input", {}).get("value_to_compare", ""),
                        step.get("input", {}).get("comparison", "equals"),
                        step.get("input", {}).get("equals", ""),
                    )
                    logs.append({
                        "step_id": step.get("id"),
                        "step_type": "flow",
                        "step_name": step_name,
                        "flow": operator,
                        "success": True,
                        "reason": f"if_else routed to {branch_key}.",
                    })
                    branch_outcome = self._execute_steps(step.get(branch_key, []), context, logs)
                elif operator == "switch_case":
                    candidate = self._resolve_flow_value(step.get("input", {}), context)
                    cases = step.get("cases", []) if isinstance(step.get("cases", []), list) else []
                    matched_case = next(
                        (
                            case
                            for case in cases
                            if str(self._interpolate_context_string(case.get("value", ""), context)).strip() == candidate
                        ),
                        None,
                    )
                    output_base = self._flow_output_base(step)
                    if matched_case is not None:
                        branch_steps = matched_case.get("steps", [])
                        branch_label = str(matched_case.get('label', matched_case.get('value', 'case'))).strip() or "case"
                        context[f"{output_base}_matched"] = "true"
                        context[f"{output_base}_branch"] = branch_label
                        branch_reason = f"switch_case routed to {str(matched_case.get('label', matched_case.get('value', 'case'))).strip()}."
                    else:
                        branch_steps = step.get("default_branch", [])
                        context[f"{output_base}_matched"] = "false"
                        context[f"{output_base}_branch"] = "default"
                        branch_reason = "switch_case routed to default."
                    self.logger.info(
                        "Playbook flow routed: flow=%s step=%s step_id=%s candidate=%s branch=%s",
                        operator,
                        step_name,
                        step.get("id"),
                        candidate,
                        context.get(f"{output_base}_branch", ""),
                    )
                    logs.append({
                        "step_id": step.get("id"),
                        "step_type": "flow",
                        "step_name": step_name,
                        "flow": operator,
                        "success": True,
                        "reason": branch_reason,
                    })
                    branch_outcome = self._execute_steps(branch_steps, context, logs)
                else:
                    self.logger.error(
                        "Playbook flow unsupported: flow=%s step=%s step_id=%s",
                        operator,
                        step_name,
                        step.get("id"),
                    )
                    logs.append({
                        "step_id": step.get("id"),
                        "step_type": "flow",
                        "step_name": step_name,
                        "flow": operator,
                        "success": False,
                        "reason": "Unsupported flow operator.",
                    })
                    success = False
                    continue
                if branch_outcome.status == "paused":
                    return PlaybookExecutionOutcome(
                        status="paused",
                        remaining_steps=branch_outcome.remaining_steps + step.get("next_steps", []) + remaining_after_current,
                        prompt_message=branch_outcome.prompt_message,
                        paused_step_id=branch_outcome.paused_step_id,
                        paused_step_name=branch_outcome.paused_step_name,
                    )
                if branch_outcome.status == "aborted":
                    return PlaybookExecutionOutcome(status="aborted", remaining_steps=[])
                if branch_outcome.status == "failed":
                    return PlaybookExecutionOutcome(status="failed", remaining_steps=[])

                continuation_outcome = self._execute_steps(step.get("next_steps", []), context, logs)
                if continuation_outcome.status == "paused":
                    return PlaybookExecutionOutcome(
                        status="paused",
                        remaining_steps=continuation_outcome.remaining_steps + remaining_after_current,
                        prompt_message=continuation_outcome.prompt_message,
                        paused_step_id=continuation_outcome.paused_step_id,
                        paused_step_name=continuation_outcome.paused_step_name,
                    )
                if continuation_outcome.status == "aborted":
                    return PlaybookExecutionOutcome(status="aborted", remaining_steps=[])
                if continuation_outcome.status == "failed":
                    return PlaybookExecutionOutcome(status="failed", remaining_steps=[])
            else:
                logs.append({
                    "step_id": step.get("id"),
                    "step_type": str(step_type or "unknown"),
                    "step_name": step_name,
                    "success": False,
                    "reason": "Unknown step type.",
                })
                success = False
        return PlaybookExecutionOutcome(status="success" if success else "failed", remaining_steps=[])

    def execute_playbook(self, name: str, context: dict | None = None) -> PlaybookRunResult:
        details = self.get_playbook(name)
        config = details.get("config", {})
        run_context = dict(context or {})
        self.logger.info(
            "Playbook execution started: playbook=%s context_keys=%s",
            name,
            sorted(run_context.keys()),
        )

        if not self.should_run_for_trigger(config, run_context):
            self.logger.warning("Playbook trigger conditions not met: playbook=%s", name)
            return PlaybookRunResult(name=name, success=False, logs=[{"success": False, "reason": "Trigger conditions not met."}])

        logs: list[dict] = []
        outcome = self._execute_steps(config.get("definition", {}).get("steps", []), run_context, logs)
        success = outcome.status == "success"
        self.logger.info(
            "Playbook execution finished: playbook=%s success=%s paused=%s log_entries=%s",
            name,
            success,
            outcome.status == "paused",
            len(logs),
        )
        resume_payload = None
        if outcome.status == "paused":
            resume_payload = {
                "context": run_context,
                "remaining_steps": outcome.remaining_steps,
                "logs": logs,
                "prompt_message": outcome.prompt_message,
                "paused_step_id": outcome.paused_step_id,
                "paused_step_name": outcome.paused_step_name,
            }
        return PlaybookRunResult(
            name=name,
            success=success,
            logs=logs,
            paused=outcome.status == "paused",
            prompt_message=outcome.prompt_message,
            resume=resume_payload,
        )

    def resume_playbook(self, name: str, resume: dict, user_choice: str) -> PlaybookRunResult:
        self.get_playbook(name)
        run_context = dict(resume.get("context", {})) if isinstance(resume.get("context", {}), dict) else {}
        remaining_steps = resume.get("remaining_steps", []) if isinstance(resume.get("remaining_steps", []), list) else []
        logs = list(resume.get("logs", [])) if isinstance(resume.get("logs", []), list) else []
        prompt_message = str(resume.get("prompt_message", "")).strip()
        paused_step_id = str(resume.get("paused_step_id", "")).strip()
        paused_step_name = str(resume.get("paused_step_name", "")).strip() or "User prompt"
        normalized_choice = str(user_choice or "").strip().casefold()

        run_context["user_response"] = normalized_choice
        run_context["decision"] = normalized_choice

        if normalized_choice in {"abort", "no", "cancel", "false", "0"}:
            logs.append({
                "step_id": paused_step_id,
                "step_type": "action",
                "step_name": paused_step_name,
                "action": "inform_user",
                "success": False,
                "reason": "User aborted the playbook at the prompt.",
                "prompt_message": prompt_message,
            })
            return PlaybookRunResult(name=name, success=False, logs=logs)

        if normalized_choice not in {"confirm", "yes", "continue", "ok", "true", "1"}:
            raise PlaybookValidationError("inform_user resume requires a confirm or abort choice.")

        logs.append({
            "step_id": paused_step_id,
            "step_type": "action",
            "step_name": paused_step_name,
            "action": "inform_user",
            "success": True,
            "reason": "User confirmed the playbook prompt.",
            "prompt_message": prompt_message,
        })
        outcome = self._execute_steps(remaining_steps, run_context, logs)
        success = outcome.status == "success"
        resume_payload = None
        if outcome.status == "paused":
            resume_payload = {
                "context": run_context,
                "remaining_steps": outcome.remaining_steps,
                "logs": logs,
                "prompt_message": outcome.prompt_message,
                "paused_step_id": outcome.paused_step_id,
                "paused_step_name": outcome.paused_step_name,
            }
        return PlaybookRunResult(
            name=name,
            success=success,
            logs=logs,
            paused=outcome.status == "paused",
            prompt_message=outcome.prompt_message,
            resume=resume_payload,
        )
