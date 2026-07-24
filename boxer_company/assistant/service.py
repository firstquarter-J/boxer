from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol, Sequence

from boxer_company.assistant.contracts import (
    CompanyAssistantRequest,
    CompanyAssistantResult,
)


class CompanyAssistantRoute(Protocol):
    @property
    def name(self) -> str: ...

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None: ...


class CompanyAssistantService:
    def __init__(self, routes: Sequence[CompanyAssistantRoute]) -> None:
        self._routes = tuple(routes)
        names = [route.name for route in self._routes]
        if len(names) != len(set(names)):
            raise ValueError("Company assistant route names must be unique")

    @property
    def route_names(self) -> tuple[str, ...]:
        return tuple(route.name for route in self._routes)

    def answer(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        # 첫 match에서 종료하는 규칙을 service 한 곳에 둬 adapter마다 우선순위가 갈리지 않게 한다.
        for route in self._routes:
            result = route.handle(request)
            if result is not None:
                return result
        return None

    def answer_with_progress(
        self,
        request: CompanyAssistantRequest,
        on_partial_result: Callable[[CompanyAssistantResult], None],
    ) -> CompanyAssistantResult | None:
        """부분 결과를 지원하는 route만 callback으로 먼저 내보내고 최종 결과를 반환한다."""
        for route in self._routes:
            progressive_handler = getattr(
                route,
                "handle_with_progress",
                None,
            )
            if callable(progressive_handler):
                result = progressive_handler(
                    request,
                    on_partial_result,
                )
            else:
                result = route.handle(request)
            if result is not None:
                return result
        return None


class RecordingsContextBarcodeMismatch(ValueError):
    """요청 바코드와 요청 단위 recordings 캐시의 바코드가 다를 때 발생한다."""


@dataclass(slots=True)
class RequestScopedRecordingsContext:
    barcode: str | None
    loader: Callable[[str], dict[str, Any]]
    _loaded: bool = False
    _value: dict[str, Any] | None = None
    _error: Exception | None = None

    def prefetch(self) -> dict[str, Any] | None:
        if not self.barcode:
            return None
        return self.get()

    def validate_barcode(self, requested_barcode: str | None) -> None:
        fixed_barcode = str(self.barcode or "").strip()
        normalized_requested = str(requested_barcode or "").strip()
        if (
            fixed_barcode
            and normalized_requested
            and fixed_barcode != normalized_requested
        ):
            # 캐시 조회 전에 fail closed해 다른 바코드의 조회 결과가 섞이지 않게 한다.
            raise RecordingsContextBarcodeMismatch(
                "요청 바코드와 조회 컨텍스트가 일치하지 않아"
            )

    def get(
        self,
        *,
        requested_barcode: str | None = None,
    ) -> dict[str, Any]:
        self.validate_barcode(requested_barcode)
        if self._loaded:
            if self._error is not None:
                raise self._error
            if self._value is None:
                raise RuntimeError("recordings context cache is empty")
            return self._value

        self._loaded = True
        if not self.barcode:
            self._error = ValueError("바코드가 필요해")
            raise self._error
        try:
            self._value = self.loader(self.barcode)
        except Exception as exc:
            # 같은 요청 안에서는 실패도 memoize해 DB/S3 재시도를 중복 실행하지 않는다.
            self._error = exc
            raise
        return self._value

    @staticmethod
    def build_rows_evidence(context: dict[str, Any]) -> list[dict[str, Any]]:
        rows = context.get("rows") or []
        return [
            {
                "seq": row.get("seq"),
                "hospitalSeq": row.get("hospitalSeq"),
                "hospitalRoomSeq": row.get("hospitalRoomSeq"),
                "hospitalName": row.get("hospitalName"),
                "roomName": row.get("roomName"),
                "deviceSeq": row.get("deviceSeq"),
                "videoLength": row.get("videoLength"),
                "streamingStatus": row.get("streamingStatus"),
                "recordedAt": row.get("recordedAt"),
                "createdAt": row.get("createdAt"),
            }
            for row in rows
            if isinstance(row, dict)
        ]

    @classmethod
    def attach_to_evidence(
        cls,
        evidence: dict[str, Any],
        context: dict[str, Any],
    ) -> None:
        evidence["recordingsSummary"] = context.get("summary")
        evidence["recordingsContextLimit"] = context.get("limit")
        evidence["recordingsHasMore"] = context.get("has_more")
        evidence["recordingsRows"] = cls.build_rows_evidence(context)

    @staticmethod
    def has_device_mapping(context: dict[str, Any]) -> bool:
        rows = context.get("rows") or []
        return any(
            isinstance(row, dict) and row.get("deviceSeq") is not None
            for row in rows
        )


__all__ = [
    "CompanyAssistantRoute",
    "CompanyAssistantService",
    "RecordingsContextBarcodeMismatch",
    "RequestScopedRecordingsContext",
]
