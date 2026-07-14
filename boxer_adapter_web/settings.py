from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from boxer_adapter_web.policies import HandoffPolicy
from boxer_adapter_web.security import parse_bool
from boxer_adapter_web.workflows import WorkflowCatalog, load_workflow_config

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(slots=True)
class WebSettings:
    host: str
    port: int
    secret_key: str
    data_path: Path
    knowledge_source: str
    markdown_root: Path
    admin_dist_path: Path
    admin_cookie_name: str
    admin_csrf_cookie_name: str
    admin_csrf_header_name: str
    admin_cookie_secure: bool
    admin_session_max_age_sec: int
    widget_allowed_origins: list[str]
    admin_allowed_origins: list[str]
    ws_rate_limit_per_minute: int
    notion_page_ids: list[str]
    welcome_title: str
    welcome_message: str
    starter_options: list[str]
    welcome_timezone_ko: str
    welcome_timezone_en: str
    web_config_path: Path | None
    workflow_config_path: Path | None
    workflow_catalog: WorkflowCatalog
    handoff_policy: HandoffPolicy


def get_web_settings() -> WebSettings:
    # 웹 alpha는 단일 워크스페이스 전제라 설정도 env 한 벌만 읽는다.
    notion_page_ids = _split_csv(
        os.getenv("NOTION_TEST_PAGE_ID", "").strip()
    )
    web_config_path = _resolve_optional_path(os.getenv("BOXER_WEB_CONFIG_PATH", ""))
    web_config = load_workflow_config(web_config_path)
    starter_options = _parse_starter_options(
        _env_or_config("BOXER_WEB_STARTER_OPTIONS", web_config.get("starterOptions"), "")
    )
    workflow_config_path = _resolve_optional_path(os.getenv("BOXER_WEB_WORKFLOW_CONFIG_PATH", ""))
    workflow_config = load_workflow_config(workflow_config_path) if workflow_config_path else web_config
    workflow_catalog = WorkflowCatalog.from_config(
        workflow_config,
        fallback_options=starter_options,
    )
    welcome_timezones = web_config.get("welcomeTimeZones") if isinstance(web_config.get("welcomeTimeZones"), dict) else {}
    handoff_config = web_config.get("handoffPolicy") if isinstance(web_config.get("handoffPolicy"), dict) else {}
    legacy_allowed_origins = os.getenv("BOXER_WEB_ALLOWED_ORIGINS", "")

    return WebSettings(
        host=os.getenv("BOXER_WEB_HOST", "127.0.0.1").strip() or "127.0.0.1",
        port=int(os.getenv("BOXER_WEB_PORT", "8000")),
        secret_key=os.getenv("BOXER_WEB_SECRET_KEY", "boxer-web-dev-secret").strip()
        or "boxer-web-dev-secret",
        data_path=_resolve_path(os.getenv("BOXER_WEB_DATA_PATH", "data/web_chat.db")),
        knowledge_source=os.getenv("BOXER_WEB_KNOWLEDGE_SOURCE", "markdown").strip().lower() or "markdown",
        markdown_root=_resolve_path(
            os.getenv("BOXER_WEB_MARKDOWN_ROOT", "examples/web_knowledge/markdown")
        ),
        admin_dist_path=_resolve_path(
            os.getenv("BOXER_WEB_ADMIN_DIST_PATH", "widget/dist/admin")
        ),
        admin_cookie_name="boxer_web_admin_session",
        admin_csrf_cookie_name="boxer_web_admin_csrf",
        admin_csrf_header_name="X-Boxer-Csrf-Token",
        admin_cookie_secure=_env_or_config_bool(
            "BOXER_WEB_ADMIN_COOKIE_SECURE",
            web_config.get("adminCookieSecure"),
            default=False,
        ),
        admin_session_max_age_sec=60 * 60 * 24 * 7,
        widget_allowed_origins=_parse_csv_or_json_list(
            _env_or_config(
                "BOXER_WEB_WIDGET_ALLOWED_ORIGINS",
                web_config.get("widgetAllowedOrigins", web_config.get("allowedOrigins")),
                legacy_allowed_origins,
            )
        ),
        admin_allowed_origins=_parse_csv_or_json_list(
            _env_or_config(
                "BOXER_WEB_ADMIN_ALLOWED_ORIGINS",
                web_config.get("adminAllowedOrigins"),
                "",
            )
        ),
        ws_rate_limit_per_minute=_env_or_config_int(
            "BOXER_WEB_WS_RATE_LIMIT_PER_MINUTE",
            web_config.get("wsRateLimitPerMinute"),
            default=120,
        ),
        notion_page_ids=notion_page_ids,
        welcome_title=_env_or_config("BOXER_WEB_WELCOME_TITLE", web_config.get("welcomeTitle"), ""),
        welcome_message=_env_or_config("BOXER_WEB_WELCOME_MESSAGE", web_config.get("welcomeMessage"), ""),
        starter_options=starter_options,
        welcome_timezone_ko=_env_or_config("BOXER_WEB_WELCOME_TIMEZONE_KO", welcome_timezones.get("ko"), ""),
        welcome_timezone_en=_env_or_config("BOXER_WEB_WELCOME_TIMEZONE_EN", welcome_timezones.get("en"), ""),
        web_config_path=web_config_path,
        workflow_config_path=workflow_config_path,
        workflow_catalog=workflow_catalog,
        handoff_policy=HandoffPolicy(
            on_missing_evidence=_env_or_config_bool(
                "BOXER_WEB_HANDOFF_ON_MISSING_EVIDENCE",
                handoff_config.get("onMissingEvidence"),
                default=True,
            ),
            prompt_before_queue=_env_or_config_bool(
                "BOXER_WEB_HANDOFF_PROMPT_BEFORE_QUEUE",
                handoff_config.get("promptBeforeQueue"),
                default=False,
            ),
        ),
    )


def _resolve_path(raw_path: str) -> Path:
    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate
    return candidate


def _resolve_optional_path(raw_path: str) -> Path | None:
    normalized = (raw_path or "").strip()
    if not normalized:
        return None
    return _resolve_path(normalized)


def _split_csv(raw_value: str) -> list[str]:
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def _parse_csv_or_json_list(raw_value: str) -> list[str]:
    normalized = (raw_value or "").strip()
    if not normalized:
        return []
    if normalized.startswith("["):
        try:
            parsed = json.loads(normalized)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    return _split_csv(normalized)


def _parse_starter_options(raw_value: str) -> list[str]:
    normalized = (raw_value or "").strip()
    if not normalized:
        return []

    # env 한 줄로도 쓰기 쉽고, 필요하면 JSON 배열도 받을 수 있게 둘 다 허용한다.
    if normalized.startswith("["):
        try:
            parsed = json.loads(normalized)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]

    return [item.strip() for item in normalized.split("||") if item.strip()]


def _env_or_config(env_key: str, config_value: object, default: str) -> str:
    raw_env_value = os.getenv(env_key)
    if raw_env_value is not None:
        return raw_env_value.strip()
    if isinstance(config_value, list):
        return json.dumps(config_value, ensure_ascii=False)
    if config_value is None:
        return default
    return str(config_value).strip()


def _env_or_config_bool(env_key: str, config_value: object, *, default: bool) -> bool:
    raw_env_value = os.getenv(env_key)
    if raw_env_value is not None:
        return parse_bool(raw_env_value, default=default)
    if isinstance(config_value, bool):
        return config_value
    if config_value is None:
        return default
    return parse_bool(str(config_value), default=default)


def _env_or_config_int(env_key: str, config_value: object, *, default: int) -> int:
    raw_env_value = os.getenv(env_key)
    raw_value = raw_env_value if raw_env_value is not None else config_value
    if raw_value is None or str(raw_value).strip() == "":
        return default
    try:
        return int(str(raw_value).strip())
    except ValueError:
        return default
