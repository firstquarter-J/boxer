import logging
import threading
import time
from datetime import datetime

from boxer_company import settings as cs
from boxer_company.sms_delivery_cycle import (
    remember_accepted_sms_delivery,
    remember_sms_delivery_sheet_record,
    run_sms_delivery_cycle_once,
)
from boxer_company_adapter_slack.automation_api_client import (
    CompanyAutomationApiClient,
)
from boxer_company_adapter_slack.automation_reporter import (
    build_automation_request_id,
)

_SMS_DELIVERY_REPORTER_THREAD: threading.Thread | None = None
_SMS_DELIVERY_REPORTER_THREAD_LOCK = threading.Lock()


def _run_sms_delivery_reporter_once(
    logger: logging.Logger,
    *,
    now: datetime | None = None,
    automation_client: CompanyAutomationApiClient | None = None,
) -> int:
    """local 호환 또는 공통 API cycle을 정확히 한 번 실행한다."""

    if automation_client is not None:
        scheduled_at = now or datetime.now().astimezone()
        if scheduled_at.tzinfo is None:
            scheduled_at = scheduled_at.astimezone()
        cycle_key = "continuous"
        result = automation_client.run(
            request_id=build_automation_request_id(
                cycle="sms_delivery",
                cycle_key=cycle_key,
                scheduled_at=scheduled_at,
            ),
            cycle="sms_delivery",
            cycle_key=cycle_key,
            scheduled_at=scheduled_at,
        )
        if result.deliveries:
            raise RuntimeError("문자 최종 상태 API delivery 계약이 올바르지 않아")
        return max(0, int(result.metrics.get("updatedCount") or 0))
    return run_sms_delivery_cycle_once(logger, now=now)


def _sms_delivery_reporter_loop(
    logger: logging.Logger,
    automation_client: CompanyAutomationApiClient | None = None,
) -> None:
    # Slack 프로세스는 실행 시각만 결정하고 provider·outbox·Sheet 처리는 company에 맡긴다.
    poll_interval_sec = max(
        10,
        int(cs.SOLAPI_DELIVERY_REPORT_POLL_INTERVAL_SEC),
    )
    while True:
        try:
            _run_sms_delivery_reporter_once(
                logger,
                automation_client=automation_client,
            )
        except Exception as exc:
            logger.warning(
                "문자 최종 결과 확인 중 오류가 발생했어 error_type=%s",
                type(exc).__name__,
            )
        time.sleep(poll_interval_sec)


def attach_sms_delivery_reporter(
    *,
    logger: logging.Logger | None = None,
    automation_client: CompanyAutomationApiClient | None = None,
) -> None:
    if automation_client is None and (
        not cs.DEVICE_HEALTH_SHEET_ENABLED
        or str(cs.DEVICE_HEALTH_MONITOR_SMS_PROVIDER or "").strip().lower() != "solapi"
        or not cs.SOLAPI_API_KEY
        or not cs.SOLAPI_API_SECRET
    ):
        return

    actual_logger = logger or logging.getLogger(__name__)
    global _SMS_DELIVERY_REPORTER_THREAD
    with _SMS_DELIVERY_REPORTER_THREAD_LOCK:
        if (
            _SMS_DELIVERY_REPORTER_THREAD is not None
            and _SMS_DELIVERY_REPORTER_THREAD.is_alive()
        ):
            return
        _SMS_DELIVERY_REPORTER_THREAD = threading.Thread(
            target=_sms_delivery_reporter_loop,
            args=(actual_logger, automation_client),
            name="boxer-sms-delivery-reporter",
            daemon=True,
        )
        _SMS_DELIVERY_REPORTER_THREAD.start()
    actual_logger.info(
        "Started SMS delivery reporter interval=%ss",
        max(10, int(cs.SOLAPI_DELIVERY_REPORT_POLL_INTERVAL_SEC)),
    )
