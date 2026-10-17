"""SQLite lease와 모델 실행을 분리하는 단일 기억 추출 worker."""

from __future__ import annotations

import logging
import json
import uuid
from typing import Any

from .extraction import MAX_INPUT_CHARS, SYSTEM_PROMPT, MemoryExtractor, _chronological_sources


class MemoryWorker:
    def __init__(self, store: Any, extractor: MemoryExtractor, *, worker_id: str | None = None, logger: logging.Logger | None = None) -> None:
        self.store = store
        self.extractor = extractor
        self.worker_id = worker_id or "memory-" + uuid.uuid4().hex
        self.logger = logger or logging.getLogger(__name__)

    def run_once(self) -> dict[str, Any] | None:
        # claim/각 stage/complete만 짧은 transaction이며 LLM 호출 중에는 잠금을 보유하지 않는다.
        job = self.store.claim_job(self.worker_id, lease_seconds=120)
        if job is None:
            return None
        tenant, job_id, token = job["tenantId"], job["jobId"], job["claimToken"]
        try:
            identity = {"tenant_id": tenant, "job_id": job_id, "claim_token": token}
            inputs = self.store.inputs(**identity)
            staged = {chunk["chunkNo"]: list(chunk.get("candidates") or ()) for chunk in inputs}
            source_map = {source["sourceId"]: {**source, "threadTs": job.get("threadTs", "")}
                          for chunk in inputs for source in chunk["sources"]}
            renew = getattr(self.store, "renew_claim", None)
            heartbeat = (lambda: renew(**identity, lease_seconds=120)) if renew is not None else None
            # 재시작 뒤에도 단일 전송 청크가 여러 모델 입력으로 나뉜 가능성을 보존한다.
            multiple_batches = len(json.dumps(list(source_map.values()), ensure_ascii=False)) + len(SYSTEM_PROMPT) + 4000 > MAX_INPUT_CHARS
            for chunk in inputs:
                if chunk.get("candidates") is not None:
                    continue
                sources = [{**source, "threadTs": job.get("threadTs", "")} for source in chunk["sources"]]
                result = self.extractor.extract(sources, previous=chunk.get("previousItems", ()),
                                                topic_hints=[item for rows in staged.values() for item in rows], before_call=heartbeat)
                multiple_batches = multiple_batches or result.batch_count > 1
                self.store.stage_candidates(**identity, chunk_no=chunk["chunkNo"], candidates=list(result.candidates), usage=dict(result.usage))
                staged[chunk["chunkNo"]] = list(result.candidates)
                self.logger.info(
                    "memory extraction model=%s input_tokens=%s output_tokens=%s cache_tokens=%s latency_ms=%s accepted=%s rejected=%s",
                    result.usage["model"], result.usage["inputTokens"], result.usage["outputTokens"], result.usage["cacheTokens"], result.usage["latencyMs"], result.usage["acceptedCount"], result.usage["rejectedCount"],
                )
            # 각 청크는 재시도 checkpoint이고 여러 청크의 의미 결합은 publish 전에 한 번 더 확인한다.
            # 최종 후보와 빈 나머지 청크를 한 transaction으로 바꾸므로 중간 상태를 노출하지 않는다.
            if inputs and not all(chunk.get("reconciled", False) for chunk in inputs) and (
                    (len(inputs) > 1 and len(source_map) > 1) or multiple_batches):
                result = self.extractor.reconcile([item for rows in staged.values() for item in rows],
                                                 _chronological_sources(list(source_map.values())),
                                                 previous=inputs[0].get("previousItems", ()), before_call=heartbeat)
                self.store.stage_reconciled_candidates(**identity, candidates=list(result.candidates), usage=dict(result.usage))
                self.logger.info("memory reconciliation model=%s input_tokens=%s output_tokens=%s accepted=%s rejected=%s",
                                 result.usage["model"], result.usage["inputTokens"], result.usage["outputTokens"],
                                 result.usage["acceptedCount"], result.usage["rejectedCount"])
            outcome = self.store.complete(**identity)
            self.logger.info("memory job state=%s published=%s", outcome.get("state"), outcome.get("publishedCount", 0))
            return outcome
        except Exception as exc:
            # 예외 본문에는 provider가 원문을 담을 수 있어 유형만 저장한다.
            error_type = type(exc).__name__
            try:
                outcome = self.store.fail(tenant_id=tenant, job_id=job_id, claim_token=token, error_type=error_type, retry=True, usage=getattr(exc, "usage", {}))
            except Exception:
                outcome = {"state": "stale", "errorType": error_type}
            self.logger.warning("memory job failed error_type=%s state=%s", error_type, outcome.get("state"))
            return outcome
