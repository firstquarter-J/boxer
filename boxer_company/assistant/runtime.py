from __future__ import annotations

from dataclasses import dataclass, replace
import logging
from typing import Any, Callable, Literal, Mapping, Sequence

from boxer import AnswerEngine
from boxer.context.entries import ContextEntry
from boxer.context.windowing import (
    _render_context_text,
    window_context_entries,
)
from boxer.core import settings as core_settings
from boxer_company import settings as company_settings
from boxer_company.assistant.answer_composer import (
    CompanyEvidenceAnswerComposer,
    CompanyEvidenceAnswerComposerDeps,
)
from boxer_company.assistant.barcode_log_route import (
    BarcodeLogAssistantRoute,
)
from boxer_company.assistant.barcode_query_route import (
    BarcodeQueryAssistantRoute,
)
from boxer_company.assistant.contracts import (
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.assistant.device_led_routes import (
    DeviceLedLogAssistantRoute,
    DeviceLedPatternGuideAssistantRoute,
    NotionReferenceLoader,
)
from boxer_company.assistant.notion_route import (
    CompanyNotionAssistantRoute,
    CompanyNotionAssistantRouteDeps,
)
from boxer_company.assistant.recording_failure_route import (
    RecordingFailureAssistantRoute,
)
from boxer_company.assistant.service import (
    CompanyAssistantRoute,
    CompanyAssistantService,
    RequestScopedRecordingsContext,
)
from boxer_company.assistant.scope_guard import (
    AssistantRequestScopeMismatch,
    build_scope_mismatch_result,
    resolve_assistant_request_scope,
)
from boxer_company.assistant.structured_route import (
    StructuredAssistantRoute,
)
from boxer_company.notion_playbooks import _select_notion_references
from boxer_company.routers.barcode_log import (
    _extract_hospital_room_scope,
    _extract_log_date_with_presence,
)
from boxer_company.routers.box_db import _load_recordings_context_by_barcode
from boxer_company.routers.recording_failure_analysis import (
    _has_recording_failure_analysis_hints,
)
from boxer_company.utils import _extract_barcode


CompanyAssistantStage = Literal[
    "notion",
    "device",
    "failure",
    "log",
    "structured",
    "barcode",
    "knowledge",
]
RecordingsLoader = Callable[[str], dict[str, Any]]
ConfigFlag = Callable[[], bool]
PartialResultHandler = Callable[[CompanyAssistantResult], None]
KnowledgeRouteFactory = Callable[
    [
        RequestScopedRecordingsContext,
        CompanyEvidenceAnswerComposer,
    ],
    Sequence[CompanyAssistantRoute],
]

# Slack은 기존 글로벌 handler 사이에 각 stage를 끼워 넣고,
# HTTP API는 이 순서 전체를 한 번에 실행해 같은 우선순위를 공유한다.
COMPANY_ASSISTANT_STAGE_ORDER: tuple[CompanyAssistantStage, ...] = (
    "notion",
    "device",
    "failure",
    "log",
    "structured",
    "barcode",
    "knowledge",
)
COMPANY_ASSISTANT_MIGRATED_ROUTE_GROUPS: Mapping[
    CompanyAssistantStage,
    tuple[str, ...],
] = {
    "notion": ("company_notion",),
    "device": (
        "device_led_log_analysis",
        "device_led_pattern_guide",
    ),
    "failure": ("recording_failure_analysis",),
    "log": ("barcode_log_analysis",),
    "structured": ("structured",),
    "barcode": ("barcode_query",),
    # Knowledge는 별도 read-only route 묶음을 외부에서 주입한다.
    "knowledge": (),
}


def _default_s3_query_enabled() -> bool:
    return bool(core_settings.S3_QUERY_ENABLED)


def _default_db_configured() -> bool:
    return bool(
        core_settings.DB_HOST
        and core_settings.DB_USERNAME
        and core_settings.DB_PASSWORD
        and core_settings.DB_DATABASE
    )


@dataclass(frozen=True, slots=True)
class CompanyAssistantRuntimeDeps:
    """프로세스 공통 의존성을 요청 단위 route graph로 조립하는 입력이다."""

    answer_engine: AnswerEngine
    provider_ready: Callable[[], bool]
    actor_allowed_for_llm: Callable[[str | None], bool]
    get_s3_client: Callable[[], Any]
    synthesis_enabled: bool = True
    recordings_loader: RecordingsLoader = _load_recordings_context_by_barcode
    notion_reference_loader: NotionReferenceLoader = _select_notion_references
    s3_query_enabled: ConfigFlag = _default_s3_query_enabled
    db_configured: ConfigFlag = _default_db_configured
    timeout_message: str = (
        "AI 답변 생성 시간이 초과됐어. 잠시 후 다시 시도해줘"
    )
    context_max_chars: int = core_settings.THREAD_CONTEXT_MAX_CHARS
    notion_route_deps: CompanyNotionAssistantRouteDeps | None = None


@dataclass(frozen=True, slots=True)
class CompanyAssistantTurnScope:
    """adapter가 별도 parser 없이 후속 요청을 판단할 수 있는 중립 scope다."""

    barcode: str | None
    hospital_name: str | None
    room_name: str | None
    has_requested_date: bool
    is_scope_followup: bool
    thread_context: str
    has_failure_context_hint: bool


class CompanyAssistantRuntime:
    """Slack과 HTTP API가 공유하는 회사 read-only assistant 조립점이다."""

    def __init__(
        self,
        deps: CompanyAssistantRuntimeDeps,
        *,
        knowledge_routes: Sequence[CompanyAssistantRoute] = (),
        knowledge_route_factory: KnowledgeRouteFactory | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._deps = deps
        self._knowledge_routes = tuple(knowledge_routes)
        self._knowledge_route_factory = knowledge_route_factory
        self._logger = logger or logging.getLogger(__name__)
        _validate_route_names(self._knowledge_routes)

    @property
    def stage_order(self) -> tuple[CompanyAssistantStage, ...]:
        return COMPANY_ASSISTANT_STAGE_ORDER

    @staticmethod
    def needs_scope_context(question: str) -> bool:
        """병원·병실·날짜 보강 요청일 때만
        adapter가 대화 문맥을 읽게 한다.
        """
        return needs_assistant_scope_context(question)

    def start_turn(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantTurn:
        """요청마다 scope와 route를 새로 만들어
        다른 대화의 캐시를 격리한다.
        """
        normalized_request, barcode = _normalize_turn_request(
            request,
            context_max_chars=self._deps.context_max_chars,
        )
        scope_mismatch_result = _resolve_scope_mismatch_result(
            normalized_request
        )
        scope = _build_turn_scope(
            normalized_request,
            barcode=barcode,
            context_max_chars=self._deps.context_max_chars,
            has_scope_mismatch=scope_mismatch_result is not None,
        )
        recordings = RequestScopedRecordingsContext(
            barcode=barcode,
            loader=self._deps.recordings_loader,
        )
        s3_query_enabled = _read_config_flag(
            "s3_query_enabled",
            self._deps.s3_query_enabled,
            logger=self._logger,
        )
        db_configured = _read_config_flag(
            "db_configured",
            self._deps.db_configured,
            logger=self._logger,
        )
        route_groups = self._build_route_groups(
            recordings=recordings,
            s3_query_enabled=s3_query_enabled,
            db_configured=db_configured,
        )
        return CompanyAssistantTurn(
            request=normalized_request,
            scope=scope,
            recordings=recordings,
            route_groups=route_groups,
            prefetch_enabled=(
                db_configured and scope_mismatch_result is None
            ),
            scope_mismatch_result=scope_mismatch_result,
            logger=self._logger,
        )

    def answer(
        self,
        request: CompanyAssistantRequest,
        *,
        on_partial_result: PartialResultHandler | None = None,
    ) -> CompanyAssistantResult | None:
        """API용 통합 진입점으로
        표준 stage 순서를 처음부터 끝까지 실행한다.
        """
        return self.start_turn(request).answer(
            on_partial_result=on_partial_result,
        )

    def _build_route_groups(
        self,
        *,
        recordings: RequestScopedRecordingsContext,
        s3_query_enabled: bool,
        db_configured: bool,
    ) -> dict[CompanyAssistantStage, tuple[CompanyAssistantRoute, ...]]:
        composer = CompanyEvidenceAnswerComposer(
            CompanyEvidenceAnswerComposerDeps(
                answer_engine=self._deps.answer_engine,
                synthesis_enabled=self._deps.synthesis_enabled,
                provider_ready=self._deps.provider_ready,
                actor_allowed_for_llm=self._deps.actor_allowed_for_llm,
            ),
            logger=self._logger,
        )
        notion_deps = self._deps.notion_route_deps or (
            CompanyNotionAssistantRouteDeps(
                answer_engine=self._deps.answer_engine,
                synthesis_enabled=self._deps.synthesis_enabled,
                provider_ready=self._deps.provider_ready,
                actor_allowed_for_llm=self._deps.actor_allowed_for_llm,
            )
        )

        # 설정값은 turn 시작 시 한 번만 읽어 동일 요청의 모든 route가
        # 같은 DB/S3 가용성 판단을 사용하게 한다.
        groups: dict[
            CompanyAssistantStage,
            tuple[CompanyAssistantRoute, ...],
        ] = {
            "notion": (
                CompanyNotionAssistantRoute(
                    notion_deps,
                    logger=self._logger,
                ),
            ),
            "device": (
                DeviceLedLogAssistantRoute(
                    self._deps.get_s3_client,
                    s3_enabled=s3_query_enabled,
                    logger=self._logger,
                ),
                DeviceLedPatternGuideAssistantRoute(
                    composer,
                    self._deps.notion_reference_loader,
                    timeout_message=self._deps.timeout_message,
                    logger=self._logger,
                ),
            ),
            "failure": (
                RecordingFailureAssistantRoute(
                    recordings,
                    self._deps.get_s3_client,
                    composer,
                    s3_query_enabled=s3_query_enabled,
                    db_configured=db_configured,
                    timeout_message=self._deps.timeout_message,
                    logger=self._logger,
                ),
            ),
            "log": (
                BarcodeLogAssistantRoute(
                    recordings,
                    self._deps.get_s3_client,
                    composer,
                    s3_query_enabled=lambda: s3_query_enabled,
                    db_configured=lambda: db_configured,
                    logger=self._logger,
                ),
            ),
            "structured": (
                StructuredAssistantRoute(logger=self._logger),
            ),
            "barcode": (
                BarcodeQueryAssistantRoute(
                    recordings,
                    answer_composer=composer,
                    timeout_message=self._deps.timeout_message,
                    logger=self._logger,
                ),
            ),
            "knowledge": (
                self._knowledge_routes
                + tuple(
                    self._knowledge_route_factory(
                        recordings,
                        composer,
                    )
                    if self._knowledge_route_factory is not None
                    else ()
                )
            ),
        }
        _validate_route_groups(groups)
        return groups


class CompanyAssistantTurn:
    """한 요청 안에서 route 순서와 recordings 조회 상태를 공유한다."""

    def __init__(
        self,
        *,
        request: CompanyAssistantRequest,
        scope: CompanyAssistantTurnScope,
        recordings: RequestScopedRecordingsContext,
        route_groups: Mapping[
            CompanyAssistantStage,
            Sequence[CompanyAssistantRoute],
        ],
        prefetch_enabled: bool,
        scope_mismatch_result: CompanyAssistantResult | None,
        logger: logging.Logger,
    ) -> None:
        self.request = request
        self.scope = scope
        self.recordings = recordings
        self._route_groups = {
            stage: tuple(route_groups.get(stage, ()))
            for stage in COMPANY_ASSISTANT_STAGE_ORDER
        }
        self._services = {
            stage: CompanyAssistantService(routes)
            for stage, routes in self._route_groups.items()
        }
        self._prefetch_enabled = prefetch_enabled
        self._scope_mismatch_result = scope_mismatch_result
        self._logger = logger
        self._prefetch_attempted = False
        self._prefetch_value: dict[str, Any] | None = None
        self._prefetch_error_type: str | None = None

    @property
    def barcode(self) -> str | None:
        return self.scope.barcode

    @property
    def hospital_name(self) -> str | None:
        return self.scope.hospital_name

    @property
    def room_name(self) -> str | None:
        return self.scope.room_name

    @property
    def has_requested_date(self) -> bool:
        return self.scope.has_requested_date

    @property
    def is_scope_followup(self) -> bool:
        return self.scope.is_scope_followup

    @property
    def thread_context(self) -> str:
        return self.scope.thread_context

    @property
    def has_failure_context_hint(self) -> bool:
        return self.scope.has_failure_context_hint

    @property
    def route_names(self) -> tuple[str, ...]:
        return tuple(
            route.name
            for stage in COMPANY_ASSISTANT_STAGE_ORDER
            for route in self._route_groups[stage]
        )

    @property
    def prefetch_attempted(self) -> bool:
        return self._prefetch_attempted

    @property
    def prefetch_error_type(self) -> str | None:
        return self._prefetch_error_type

    def routes_for_stage(
        self,
        stage: CompanyAssistantStage,
    ) -> tuple[CompanyAssistantRoute, ...]:
        _validate_stage(stage)
        return self._route_groups[stage]

    def service_for_stage(
        self,
        stage: CompanyAssistantStage,
    ) -> CompanyAssistantService:
        """기존 Slack shim이 같은 turn route/cache를 재사용하게 한다."""
        _validate_stage(stage)
        return self._services[stage]

    def prefetch_recordings(self) -> dict[str, Any] | None:
        """선조회 실패는 route 실행을 막지 않고
        요청 캐시에 한 번만 기록한다.
        """
        if self._prefetch_attempted:
            return self._prefetch_value
        self._prefetch_attempted = True
        if not self._prefetch_enabled or not self.barcode:
            return None

        try:
            self._prefetch_value = self.recordings.prefetch()
        except Exception as exc:
            # secret이 섞일 수 있는 예외 문자열은 남기지 않고
            # 타입만 기록한다.
            self._prefetch_error_type = type(exc).__name__
            self._logger.warning(
                "Company assistant recordings prefetch failed "
                "request_id=%s error_type=%s",
                self.request.request_id,
                self._prefetch_error_type,
            )
        return self._prefetch_value

    def answer_stage(
        self,
        stage: CompanyAssistantStage,
        *,
        on_partial_result: PartialResultHandler | None = None,
    ) -> CompanyAssistantResult | None:
        """Slack adapter가 기존 중간 legacy route 위치에서 호출하는 진입점이다."""
        _validate_stage(stage)
        if stage != "notion" and self._scope_mismatch_result is not None:
            # 질문과 adapter scope가 다르면 DB/S3 선조회 전에 막는다.
            return self._scope_mismatch_result
        # recordings는 각 route가 matcher·권한·prompt 정책을 통과한 뒤
        # 요청 cache에서 지연 조회한다.
        service = self._services[stage]
        if on_partial_result is None:
            return service.answer(self.request)
        return service.answer_with_progress(
            self.request,
            on_partial_result,
        )

    def answer(
        self,
        *,
        on_partial_result: PartialResultHandler | None = None,
    ) -> CompanyAssistantResult | None:
        """API용으로 notion부터 knowledge까지 first-match 순서로 실행한다."""
        for stage in COMPANY_ASSISTANT_STAGE_ORDER:
            result = self.answer_stage(
                stage,
                on_partial_result=on_partial_result,
            )
            if result is not None:
                return result
        return None


def _normalize_turn_request(
    request: CompanyAssistantRequest,
    *,
    context_max_chars: int,
) -> tuple[CompanyAssistantRequest, str | None]:
    metadata_barcode = _metadata_barcode(request.metadata)
    question_barcode = _extract_barcode(request.question)
    barcode = question_barcode or metadata_barcode
    if barcode is None:
        barcode = _extract_latest_context_barcode(
            request,
            context_max_chars=context_max_chars,
        )

    # context에서 복원한 값도 route scope guard가 검증할 수 있도록
    # 새 mapping에만 넣고 adapter가 준 원본 metadata는 수정하지 않는다.
    if barcode and not metadata_barcode:
        normalized_metadata = dict(request.metadata)
        normalized_metadata["barcode"] = barcode
        return replace(request, metadata=normalized_metadata), barcode
    return request, barcode


def _extract_latest_context_barcode(
    request: CompanyAssistantRequest,
    *,
    context_max_chars: int,
) -> str | None:
    entries = _window_actor_context_entries(
        request,
        context_max_chars=context_max_chars,
    )
    for entry in reversed(entries):
        barcode = _extract_barcode(str(entry.get("text") or ""))
        if barcode:
            return barcode
    return None


def _window_actor_context_entries(
    request: CompanyAssistantRequest,
    *,
    context_max_chars: int,
) -> list[ContextEntry]:
    if not request.actor_id:
        return []
    # 다른 thread 참여자의 지시가 현재 요청의 조회 범위나 route 의도로
    # 승격되지 않도록 동일 actor 문맥만 먼저 고른 뒤 길이를 제한한다.
    actor_entries = [
        entry
        for entry in request.context_entries
        if str(entry.get("author_id") or "").strip()
        == request.actor_id
    ]
    return window_context_entries(
        actor_entries,
        max_chars=max(0, context_max_chars),
    )


def needs_assistant_scope_context(question: str) -> bool:
    """병원·병실과 날짜가 함께 온 2차 scope 후보만 문맥 복원이 필요하다."""
    hospital_name, room_name = _extract_hospital_room_scope(question)
    if not hospital_name or not room_name:
        return False
    try:
        _, has_requested_date = _extract_log_date_with_presence(question)
    except ValueError:
        # 날짜 형식 오류도 이전 요청의 바코드를 복원한 뒤
        # route가 안내해야 한다.
        return True
    return has_requested_date


def _build_turn_scope(
    request: CompanyAssistantRequest,
    *,
    barcode: str | None,
    context_max_chars: int,
    has_scope_mismatch: bool,
) -> CompanyAssistantTurnScope:
    thread_context = _render_context_text(
        list(request.context_entries),
        max_chars=max(0, context_max_chars),
    )
    actor_context = _render_context_text(
        _window_actor_context_entries(
            request,
            context_max_chars=context_max_chars,
        ),
        max_chars=max(0, context_max_chars),
    )
    try:
        _, has_requested_date = _extract_log_date_with_presence(
            request.question
        )
    except ValueError:
        # 기존 Slack 후속 처리와 동일하게
        # 형식 오류도 날짜 입력 시도로 본다.
        has_requested_date = True

    if has_scope_mismatch:
        hospital_name = None
        room_name = None
    else:
        resolved_scope = resolve_assistant_request_scope(request)
        barcode = resolved_scope.barcode or barcode
        hospital_name = resolved_scope.hospital_name
        room_name = resolved_scope.room_name

    return CompanyAssistantTurnScope(
        barcode=barcode,
        hospital_name=hospital_name,
        room_name=room_name,
        has_requested_date=has_requested_date,
        is_scope_followup=bool(
            not has_scope_mismatch
            and barcode
            and hospital_name
            and room_name
            and has_requested_date
        ),
        thread_context=thread_context,
        has_failure_context_hint=(
            _has_recording_failure_analysis_hints(actor_context)
        ),
    )


def _metadata_barcode(metadata: Mapping[str, Any]) -> str | None:
    value = metadata.get("barcode")
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _resolve_scope_mismatch_result(
    request: CompanyAssistantRequest,
) -> CompanyAssistantResult | None:
    metadata_barcode = _metadata_barcode(request.metadata)
    if (
        metadata_barcode
        and not company_settings.BARCODE_PATTERN.fullmatch(metadata_barcode)
    ):
        return build_scope_mismatch_result(
            AssistantRequestScopeMismatch("barcode")
        )
    try:
        resolve_assistant_request_scope(request)
    except AssistantRequestScopeMismatch as mismatch:
        return build_scope_mismatch_result(mismatch)
    return None


def _read_config_flag(
    name: str,
    reader: ConfigFlag,
    *,
    logger: logging.Logger,
) -> bool:
    try:
        return bool(reader())
    except Exception as exc:
        # 설정 provider 자체의 장애도 요청 경계를 깨지 않게 fail closed한다.
        logger.warning(
            "Company assistant config read failed name=%s error_type=%s",
            name,
            type(exc).__name__,
        )
        return False


def _validate_stage(stage: str) -> None:
    if stage not in COMPANY_ASSISTANT_STAGE_ORDER:
        raise ValueError(f"지원하지 않는 assistant stage야: {stage}")


def _validate_route_names(routes: Sequence[CompanyAssistantRoute]) -> None:
    names = [route.name for route in routes]
    if len(names) != len(set(names)):
        raise ValueError("Company assistant route names must be unique")


def _validate_route_groups(
    groups: Mapping[
        CompanyAssistantStage,
        Sequence[CompanyAssistantRoute],
    ],
) -> None:
    routes = [
        route
        for stage in COMPANY_ASSISTANT_STAGE_ORDER
        for route in groups.get(stage, ())
    ]
    _validate_route_names(routes)

    for stage, expected_names in COMPANY_ASSISTANT_MIGRATED_ROUTE_GROUPS.items():
        if stage == "knowledge":
            continue
        actual_names = tuple(route.name for route in groups.get(stage, ()))
        if actual_names != expected_names:
            raise ValueError(
                f"{stage} assistant route 순서가 표준과 달라: {actual_names}"
            )


__all__ = [
    "COMPANY_ASSISTANT_MIGRATED_ROUTE_GROUPS",
    "COMPANY_ASSISTANT_STAGE_ORDER",
    "CompanyAssistantRuntime",
    "CompanyAssistantRuntimeDeps",
    "CompanyAssistantStage",
    "CompanyAssistantTurn",
    "CompanyAssistantTurnScope",
    "KnowledgeRouteFactory",
    "needs_assistant_scope_context",
]
