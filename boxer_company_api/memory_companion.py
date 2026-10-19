"""회사 API host에서만 실행하는 단일 대화 기억 companion."""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
import os
import signal
import threading
import time
from typing import Callable

from boxer_company.memory.backup import MemoryBackup, configured_memory_backup
from boxer_company.memory.extraction import build_configured_extractor
from boxer_company.memory.store import MemoryStore
from boxer_company.memory.worker import MemoryWorker
from boxer_company_api.security import validate_company_api_runtime_security


@dataclass(slots=True)
class MemoryCompanion:
    """장비 automation marker와 독립적으로 추출·삭제 내구화·보존을 진행한다."""

    worker: MemoryWorker
    backup: MemoryBackup | None = None
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger(__name__))
    clock: Callable[[], float] = time.monotonic
    _last_cleanup: float | None = None
    _last_backup: float | None = None

    def run_once(self) -> dict[str, object]:
        result: dict[str, object] = {"job": self.worker.run_once()}
        now = self.clock()
        if self.backup is not None:
            try:
                result["durableDeletions"] = self.backup.flush_deletions(self.worker.store)
            except Exception as exc:
                self.logger.warning("memory deletion durability pending error_type=%s", type(exc).__name__)
                result["durability"] = "pending"
        if self._last_cleanup is None or now - self._last_cleanup >= 3600:
            try:
                result["cleanup"] = self.worker.store.cleanup()
                self._last_cleanup = now
            except Exception as exc:
                self.logger.warning("memory cleanup failed error_type=%s", type(exc).__name__)
        if self.backup is not None and (self._last_backup is None or now - self._last_backup >= 86_400):
            try:
                self.backup.create_snapshot(self.worker.store)
                self._last_backup = now
                result["snapshot"] = "created"
                try:
                    result["removedSnapshots"] = self.backup.prune_snapshots()
                except Exception as exc:
                    self.logger.warning("memory snapshot retention failed error_type=%s", type(exc).__name__)
                    result["retention"] = "pending"
            except Exception as exc:
                self.logger.warning("memory snapshot failed error_type=%s", type(exc).__name__)
        return result

    def run_forever(self, *, stop: threading.Event | None = None, interval: float = 5) -> None:
        stopping = stop or threading.Event()
        while not stopping.is_set():
            try:
                result = self.run_once()
                job = result.get("job")
                now = self.clock()
                maintenance_due = (self._last_cleanup is None or now - self._last_cleanup >= 3600
                                   or self.backup is not None and (
                                       self._last_backup is None or now - self._last_backup >= 86_400))
                # 완료된 작업만 같은 worker로 바로 이어간다. 중단은 다음 claim 전 loop에서 확인하며,
                # 내구화·정리·백업 실패는 기존 대기를 유지해 실패한 provider를 연속 재호출하지 않는다.
                if (isinstance(job, dict) and job.get("state") == "succeeded" and not maintenance_due
                        and result.get("durability") != "pending" and result.get("retention") != "pending"):
                    continue
            except Exception as exc:
                self.logger.warning("memory companion failed error_type=%s", type(exc).__name__)
            stopping.wait(max(1, interval))


def main() -> None:
    enabled = str(os.environ.get("BOXER_COMPANY_API_MEMORY_ENABLED", "false")).strip().lower()
    if enabled not in {"true", "1", "yes", "on"}:
        raise SystemExit("대화 기억 기능이 꺼져 있어")
    validate_company_api_runtime_security()
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("boxer.company_api.memory")
    # 누락된 DB는 만들지 않는다. operator의 명시 초기화·migration이 선행돼야 한다.
    path = os.environ.get("BOXER_COMPANY_API_MEMORY_STATE_PATH", "/var/lib/boxer-company-api/conversation_memory.db")
    companion = MemoryCompanion(MemoryWorker(MemoryStore(path), build_configured_extractor(), logger=logger), configured_memory_backup(), logger)
    stopping = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stopping.set())
    companion.run_forever(stop=stopping)


if __name__ == "__main__":
    main()
