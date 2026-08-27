from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Callable

from boxer_company.hpa_change_coordinator import (
    HpaChangeCoordinator,
    create_hpa_change_coordinator,
)
from boxer_company_api.security import validate_company_api_runtime_security


@dataclass(slots=True)
class HpaChangeCoordinatorRunner:
    """FastAPI worker와 분리해 GitHub poll만 수행하는 API companion runner다."""

    coordinator: HpaChangeCoordinator
    logger: logging.Logger

    def run_once(self) -> int:
        deliveries = self.coordinator.tick()
        # payload나 작업 원문 없이 pending 개수만 운영 로그에 남긴다.
        self.logger.info(
            "HPA coordinator cycle completed pending_delivery_count=%s",
            len(deliveries),
        )
        return len(deliveries)

    def run_forever(
        self,
        *,
        wait: Callable[[float], object] | None = None,
    ) -> None:
        interval = max(1, int(self.coordinator.config.poll_interval_sec))
        actual_wait = wait or (lambda seconds: threading.Event().wait(seconds))
        while True:
            try:
                self.run_once()
            except Exception as exc:
                self.logger.warning(
                    "HPA coordinator cycle failed error_type=%s",
                    type(exc).__name__,
                )
            actual_wait(interval)


def main() -> None:
    # FastAPI/automation companion과 같은 EC2 credential 경계를 기동 전에 적용한다.
    validate_company_api_runtime_security()
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("boxer.company_api.hpa_change_coordinator")
    coordinator = create_hpa_change_coordinator(logger=logger)
    if not coordinator.enabled:
        raise SystemExit("HPA change coordinator is disabled")
    try:
        HpaChangeCoordinatorRunner(coordinator, logger).run_forever()
    except KeyboardInterrupt:
        logger.info("HPA coordinator stopped")
    finally:
        coordinator.close()


if __name__ == "__main__":
    main()


__all__ = ["HpaChangeCoordinatorRunner", "main"]
