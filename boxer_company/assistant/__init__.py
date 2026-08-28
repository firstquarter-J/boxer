from importlib import import_module
from typing import Any

from boxer_company.assistant.contracts import (
    AssistantLink,
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
    SourceReference,
)

# DTO만 필요한 HTTP schema import가 DB/S3 route 의존성까지 당기지 않도록
# 구현 클래스는 기존 package-level import 호환성을 유지하며 지연 로드한다.
_LAZY_EXPORTS = {
    "BarcodeLogAssistantRoute": (
        "boxer_company.assistant.barcode_log_route",
        "BarcodeLogAssistantRoute",
    ),
    "BarcodeEvidenceFreeformAssistantRoute": (
        "boxer_company.assistant.knowledge_routes",
        "BarcodeEvidenceFreeformAssistantRoute",
    ),
    "BarcodeEvidenceFreeformRouteDeps": (
        "boxer_company.assistant.knowledge_routes",
        "BarcodeEvidenceFreeformRouteDeps",
    ),
    "BarcodeQueryAssistantRoute": (
        "boxer_company.assistant.barcode_query_route",
        "BarcodeQueryAssistantRoute",
    ),
    "CompanyAssistantRoute": (
        "boxer_company.assistant.service",
        "CompanyAssistantRoute",
    ),
    "CompanyAssistantService": (
        "boxer_company.assistant.service",
        "CompanyAssistantService",
    ),
    "CompanyEvidenceAnswerComposer": (
        "boxer_company.assistant.answer_composer",
        "CompanyEvidenceAnswerComposer",
    ),
    "CompanyEvidenceAnswerComposerDeps": (
        "boxer_company.assistant.answer_composer",
        "CompanyEvidenceAnswerComposerDeps",
    ),
    "CompanyEvidenceAnswerPolicy": (
        "boxer_company.assistant.answer_composer",
        "CompanyEvidenceAnswerPolicy",
    ),
    "CompanyNotionAssistantRoute": (
        "boxer_company.assistant.notion_route",
        "CompanyNotionAssistantRoute",
    ),
    "CompanyNotionAssistantRouteDeps": (
        "boxer_company.assistant.notion_route",
        "CompanyNotionAssistantRouteDeps",
    ),
    "CompanyLlmHealthAssistantRoute": (
        "boxer_company.assistant.team_fun_route",
        "CompanyLlmHealthAssistantRoute",
    ),
    "CompanyDailyFortuneAssistantRoute": (
        "boxer_company.assistant.team_fun_route",
        "CompanyDailyFortuneAssistantRoute",
    ),
    "CompanyTeamFunAssistantRoute": (
        "boxer_company.assistant.team_fun_route",
        "CompanyTeamFunAssistantRoute",
    ),
    "CompanyReadOnlyKnowledgeRouteDeps": (
        "boxer_company.assistant.knowledge_routes",
        "CompanyReadOnlyKnowledgeRouteDeps",
    ),
    "CompanyAssistantRuntime": (
        "boxer_company.assistant.runtime",
        "CompanyAssistantRuntime",
    ),
    "CompanyAssistantRuntimeDeps": (
        "boxer_company.assistant.runtime",
        "CompanyAssistantRuntimeDeps",
    ),
    "CompanyAssistantTurn": (
        "boxer_company.assistant.runtime",
        "CompanyAssistantTurn",
    ),
    "DeviceDiagnosticFollowupAssistantRoute": (
        "boxer_company.assistant.knowledge_routes",
        "DeviceDiagnosticFollowupAssistantRoute",
    ),
    "DeviceDiagnosticFollowupRouteDeps": (
        "boxer_company.assistant.knowledge_routes",
        "DeviceDiagnosticFollowupRouteDeps",
    ),
    "DeviceDbDetailAssistantRoute": (
        "boxer_company.assistant.device_db_detail_route",
        "DeviceDbDetailAssistantRoute",
    ),
    "DeviceDetailAssistantRoute": (
        "boxer_company.assistant.device_db_detail_route",
        "DeviceDetailAssistantRoute",
    ),
    "DeviceLedLogAssistantRoute": (
        "boxer_company.assistant.device_led_routes",
        "DeviceLedLogAssistantRoute",
    ),
    "DeviceLedPatternGuideAssistantRoute": (
        "boxer_company.assistant.device_led_routes",
        "DeviceLedPatternGuideAssistantRoute",
    ),
    "NotionPlaybookQAAssistantRoute": (
        "boxer_company.assistant.knowledge_routes",
        "NotionPlaybookQAAssistantRoute",
    ),
    "NotionPlaybookQARouteDeps": (
        "boxer_company.assistant.knowledge_routes",
        "NotionPlaybookQARouteDeps",
    ),
    "RecordingFailureAssistantRoute": (
        "boxer_company.assistant.recording_failure_route",
        "RecordingFailureAssistantRoute",
    ),
    "RequestScopedRecordingsContext": (
        "boxer_company.assistant.service",
        "RequestScopedRecordingsContext",
    ),
    "StructuredAssistantRoute": (
        "boxer_company.assistant.structured_route",
        "StructuredAssistantRoute",
    ),
    "WeeklyRecordingsSummaryAssistantRoute": (
        "boxer_company.assistant.operational_read_routes",
        "WeeklyRecordingsSummaryAssistantRoute",
    ),
    "build_company_read_only_knowledge_routes": (
        "boxer_company.assistant.knowledge_routes",
        "build_company_read_only_knowledge_routes",
    ),
    "build_company_freeform_system_prompt": (
        "boxer_company.assistant.freeform_prompt",
        "build_company_freeform_system_prompt",
    ),
    "create_company_assistant_runtime": (
        "boxer_company.assistant.factory",
        "create_company_assistant_runtime",
    ),
}


def __getattr__(name: str) -> Any:
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(
            f"module {__name__!r} has no attribute {name!r}"
        )
    module_name, attribute_name = target
    value = getattr(import_module(module_name), attribute_name)
    globals()[name] = value
    return value

__all__ = [
    "AssistantLink",
    "AssistantMessage",
    "BarcodeEvidenceFreeformAssistantRoute",
    "BarcodeEvidenceFreeformRouteDeps",
    "BarcodeLogAssistantRoute",
    "BarcodeQueryAssistantRoute",
    "CompanyAssistantRequest",
    "CompanyAssistantResult",
    "CompanyAssistantRoute",
    "CompanyAssistantRuntime",
    "CompanyAssistantRuntimeDeps",
    "CompanyAssistantService",
    "CompanyAssistantTurn",
    "CompanyEvidenceAnswerComposer",
    "CompanyEvidenceAnswerComposerDeps",
    "CompanyEvidenceAnswerPolicy",
    "CompanyNotionAssistantRoute",
    "CompanyNotionAssistantRouteDeps",
    "CompanyDailyFortuneAssistantRoute",
    "CompanyLlmHealthAssistantRoute",
    "CompanyTeamFunAssistantRoute",
    "CompanyReadOnlyKnowledgeRouteDeps",
    "DeviceDiagnosticFollowupAssistantRoute",
    "DeviceDiagnosticFollowupRouteDeps",
    "DeviceDetailAssistantRoute",
    "DeviceDbDetailAssistantRoute",
    "DeviceLedLogAssistantRoute",
    "DeviceLedPatternGuideAssistantRoute",
    "NotionPlaybookQAAssistantRoute",
    "NotionPlaybookQARouteDeps",
    "RecordingFailureAssistantRoute",
    "RequestScopedRecordingsContext",
    "SourceReference",
    "StructuredAssistantRoute",
    "WeeklyRecordingsSummaryAssistantRoute",
    "build_company_freeform_system_prompt",
    "build_company_read_only_knowledge_routes",
    "create_company_assistant_runtime",
]
