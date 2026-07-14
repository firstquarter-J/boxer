from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

LocalizedText = str | dict[str, str]


@dataclass(slots=True)
class StarterEntry:
    key: str
    label: str


@dataclass(slots=True)
class WorkflowStep:
    field: str
    prompt: LocalizedText
    label: LocalizedText | None = None
    input_type: str = "text"
    required: bool = True
    choices: list[str] | None = None
    choice_labels: dict[str, LocalizedText] | None = None
    validation_regex: str | None = None
    retry_prompt: LocalizedText | None = None
    skip_allowed: bool = False
    action: str | None = None
    branches: dict[str, str] | None = None


@dataclass(slots=True)
class WorkflowDefinition:
    key: str
    label: LocalizedText
    steps: list[WorkflowStep]
    completion_message: LocalizedText


class WorkflowCatalog:
    def __init__(self, starter_entries: list[StarterEntry], workflows: dict[str, WorkflowDefinition]) -> None:
        self._starter_entries = starter_entries
        self._workflows = workflows

    @classmethod
    def from_config(
        cls,
        config: dict[str, Any],
        *,
        fallback_options: list[str],
    ) -> "WorkflowCatalog":
        starter_entries = _parse_starter_entries(config.get("starterEntries"), fallback_options=fallback_options)
        workflows = _parse_workflows(config.get("workflows"), starter_entries)
        return cls(starter_entries, workflows)

    def starter_entries(self) -> list[StarterEntry]:
        return list(self._starter_entries)

    def starter_options(self) -> list[str]:
        return [entry.label for entry in self._starter_entries]

    def get(self, workflow_key: str) -> WorkflowDefinition | None:
        return self._workflows.get(str(workflow_key or "").strip())

    def to_config_payload(self) -> list[dict[str, str]]:
        return [{"key": entry.key, "label": entry.label} for entry in self._starter_entries]

    def to_widget_option_payload(self) -> dict[str, list[dict[str, Any]]]:
        payload: dict[str, list[dict[str, Any]]] = {}
        for workflow_key, workflow in self._workflows.items():
            # 위젯은 currentStepIndex로 현재 step을 찾으므로 선택지가 없는 step도 순서를 유지한다.
            payload[workflow_key] = [
                {
                    "field": step.field,
                    "inputType": step.input_type,
                    "skipAllowed": step.skip_allowed,
                    "choices": _choice_payloads(step),
                }
                for step in workflow.steps
            ]
        return payload


def load_workflow_config(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    raw_payload = path.read_text(encoding="utf-8").strip()
    if not raw_payload:
        return {}
    loaded = json.loads(raw_payload)
    if not isinstance(loaded, dict):
        raise RuntimeError("workflow config root must be an object")
    return loaded


def _parse_starter_entries(raw_entries: Any, *, fallback_options: list[str]) -> list[StarterEntry]:
    entries: list[StarterEntry] = []
    if isinstance(raw_entries, list):
        for index, item in enumerate(raw_entries):
            if not isinstance(item, dict):
                continue
            label = str(item.get("label") or "").strip()
            if not label:
                continue
            key = str(item.get("key") or "").strip() or _slugify(label, index=index)
            entries.append(StarterEntry(key=key, label=label))

    if entries:
        return entries

    return [
        StarterEntry(key=_slugify(label, index=index), label=label)
        for index, label in enumerate(fallback_options)
        if str(label or "").strip()
    ]


def _parse_workflows(raw_workflows: Any, starter_entries: list[StarterEntry]) -> dict[str, WorkflowDefinition]:
    if not isinstance(raw_workflows, dict):
        return {}

    labels_by_key = {entry.key: entry.label for entry in starter_entries}
    workflows: dict[str, WorkflowDefinition] = {}
    for key, payload in raw_workflows.items():
        workflow_key = str(key or "").strip()
        if not workflow_key or not isinstance(payload, dict):
            continue
        raw_steps = payload.get("steps")
        if not isinstance(raw_steps, list):
            continue
        steps = []
        for raw_step in raw_steps:
            if not isinstance(raw_step, dict):
                continue
            field = str(raw_step.get("field") or "").strip()
            prompt = _parse_localized_text(raw_step.get("prompt"))
            if field and _has_localized_text(prompt):
                steps.append(
                    WorkflowStep(
                        field=field,
                        prompt=prompt,
                        label=_parse_optional_localized_text(raw_step.get("label")),
                        input_type=str(raw_step.get("inputType") or "text").strip() or "text",
                        required=_parse_bool(raw_step.get("required"), default=True),
                        choices=_parse_string_list(raw_step.get("choices")),
                        choice_labels=_parse_choice_labels(raw_step.get("choiceLabels")),
                        validation_regex=_parse_optional_string(raw_step.get("validationRegex")),
                        retry_prompt=_parse_optional_localized_text(raw_step.get("retryPrompt")),
                        skip_allowed=_parse_bool(raw_step.get("skipAllowed"), default=False),
                        action=_parse_optional_string(raw_step.get("action")),
                        branches=_parse_branches(raw_step.get("branches")),
                    )
                )
        if not steps:
            continue
        label = _parse_localized_text(payload.get("label")) or labels_by_key.get(workflow_key, workflow_key)
        completion_message = _parse_localized_text(payload.get("completionMessage"))
        workflows[workflow_key] = WorkflowDefinition(
            key=workflow_key,
            label=label,
            steps=steps,
            completion_message=completion_message,
        )
    return workflows


def resolve_localized_text(value: LocalizedText | None, language: str, *, default: str = "") -> str:
    if isinstance(value, dict):
        normalized_language = "ko" if str(language or "").lower().startswith("ko") else "en"
        selected = value.get(normalized_language) or value.get("default")
        if selected:
            return selected
        for fallback in value.values():
            if fallback:
                return fallback
        return default
    if value is None:
        return default
    return str(value).strip() or default


def _slugify(value: str, *, index: int) -> str:
    normalized = re.sub(r"[^0-9a-zA-Z가-힣]+", "_", str(value or "").strip().lower()).strip("_")
    return normalized or f"option_{index + 1}"


def _parse_localized_text(raw_value: Any) -> LocalizedText:
    if isinstance(raw_value, dict):
        return {
            str(key).strip(): str(value).strip()
            for key, value in raw_value.items()
            if str(key).strip() and str(value).strip()
        }
    return str(raw_value or "").strip()


def _parse_optional_localized_text(raw_value: Any) -> LocalizedText | None:
    parsed = _parse_localized_text(raw_value)
    return parsed if _has_localized_text(parsed) else None


def _has_localized_text(value: LocalizedText | None) -> bool:
    if isinstance(value, dict):
        return any(bool(item.strip()) for item in value.values())
    return bool(str(value or "").strip())


def _parse_optional_string(raw_value: Any) -> str | None:
    normalized = str(raw_value or "").strip()
    return normalized or None


def _parse_string_list(raw_value: Any) -> list[str] | None:
    if not isinstance(raw_value, list):
        return None
    values = [str(item).strip() for item in raw_value if str(item).strip()]
    return values or None


def _parse_choice_labels(raw_value: Any) -> dict[str, LocalizedText] | None:
    if not isinstance(raw_value, dict):
        return None

    labels: dict[str, LocalizedText] = {}
    for choice, label in raw_value.items():
        choice_value = str(choice or "").strip()
        parsed_label = _parse_optional_localized_text(label)
        if choice_value and parsed_label is not None:
            labels[choice_value] = parsed_label
    return labels or None


def _choice_payloads(step: WorkflowStep) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    labels_by_value = step.choice_labels or {}
    for choice in step.choices or []:
        choice_payload: dict[str, Any] = {
            "value": choice,
            "label": _humanize_choice(choice),
        }
        localized_labels = _localized_payload(labels_by_value.get(choice))
        if localized_labels is not None:
            choice_payload["labels"] = localized_labels
        payloads.append(choice_payload)
    return payloads


def _localized_payload(value: LocalizedText | None) -> str | dict[str, str] | None:
    if isinstance(value, dict):
        labels = {
            str(language).strip(): str(label).strip()
            for language, label in value.items()
            if str(language).strip() and str(label).strip()
        }
        return labels or None
    label = str(value or "").strip()
    return label or None


def _humanize_choice(value: str) -> str:
    normalized = re.sub(r"[_-]+", " ", str(value or "").strip())
    return normalized[:1].upper() + normalized[1:] if normalized else str(value or "")


def _parse_bool(raw_value: Any, *, default: bool) -> bool:
    if isinstance(raw_value, bool):
        return raw_value
    if raw_value is None:
        return default
    normalized = str(raw_value).strip().lower()
    if not normalized:
        return default
    return normalized in {"1", "true", "yes", "y", "on"}


def _parse_branches(raw_value: Any) -> dict[str, str] | None:
    if not isinstance(raw_value, dict):
        return None
    branches = {
        str(answer).strip().lower(): str(target_field).strip()
        for answer, target_field in raw_value.items()
        if str(answer).strip() and str(target_field).strip()
    }
    return branches or None
