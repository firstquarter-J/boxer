"""단일 기억 worker의 성공 후 연속 처리와 실패·중단·maintenance 대기를 검증한다."""
from __future__ import annotations

import logging

import pytest

from boxer_company_api.memory_companion import MemoryCompanion


class StopOnWait:
    """실제 시간·thread 없이 첫 idle/backoff 대기에서 실행을 끝낸다."""

    def __init__(self):
        self.stopped = False
        self.waits = []

    def is_set(self):
        return self.stopped

    def set(self):
        self.stopped = True

    def wait(self, seconds):
        self.waits.append(seconds)
        self.set()
        return True


class Store:
    def __init__(self, clock, *, fail_cleanup=False):
        self.clock = clock
        self.fail_cleanup = fail_cleanup
        self.cleanups = []

    def cleanup(self):
        self.cleanups.append(self.clock())
        if self.fail_cleanup:
            raise RuntimeError("synthetic cleanup failure")
        return {"payloadsRemoved": 0}


class Worker:
    def __init__(self, store, outcomes, *, before_return=None):
        self.store = store
        self.outcomes = iter(outcomes)
        self.before_return = before_return
        self.claims = []

    def run_once(self):
        # 예상 큐를 넘는 claim은 companion의 일반 예외 처리에 묻히지 않고 테스트를 실패시킨다.
        try:
            outcome = next(self.outcomes)
        except StopIteration:
            pytest.fail("예상한 작업 이후에 새 claim을 호출했어")
        self.claims.append(outcome)
        if self.before_return is not None:
            self.before_return(len(self.claims))
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class Backup:
    def __init__(self, clock, *, failure=None):
        self.clock = clock
        self.failure = failure
        self.flushes = []
        self.snapshots = []
        self.prunes = []

    def flush_deletions(self, _store):
        self.flushes.append(self.clock())
        if self.failure == "durability":
            raise RuntimeError("synthetic deletion provider failure")
        return 0

    def create_snapshot(self, _store):
        self.snapshots.append(self.clock())
        if self.failure == "snapshot":
            raise RuntimeError("synthetic snapshot provider failure")

    def prune_snapshots(self):
        self.prunes.append(self.clock())
        if self.failure == "retention":
            raise RuntimeError("synthetic retention provider failure")
        return 0


def test_successful_jobs_drain_in_order_then_wait_when_empty():
    stop = StopOnWait()
    store = Store(lambda: 0)
    outcomes = [{"state": "succeeded", "jobId": name} for name in ("first", "second", "third")] + [None]
    worker = Worker(store, outcomes)
    companion = MemoryCompanion(worker, clock=lambda: 0)

    companion.run_forever(stop=stop)

    # 고정된 단일 worker가 첫 idle까지 순서대로 처리한다. 성공 사이에 대기했다면 첫 작업에서 끝난다.
    assert worker.claims == outcomes
    assert stop.waits == [5]
    assert store.cleanups == [0]


@pytest.mark.parametrize("interval,expected", [(-5, 1), (0, 1), (0.5, 1), (1, 1), (2.5, 2.5), (5, 5)])
def test_idle_retains_interval_and_minimum_wait(interval, expected):
    stop = StopOnWait()
    worker = Worker(Store(lambda: 0), [None])

    MemoryCompanion(worker, clock=lambda: 0).run_forever(stop=stop, interval=interval)

    assert worker.claims == [None]
    assert stop.waits == [expected]


@pytest.mark.parametrize("state", ["ready", "running", "retry", "failed", "stale", "superseded", "canceled", "unknown", None])
def test_non_success_states_wait_without_immediate_reclaim(state):
    stop = StopOnWait()
    outcome = {"state": state}
    worker = Worker(Store(lambda: 0), [outcome])

    MemoryCompanion(worker, clock=lambda: 0).run_forever(stop=stop)

    assert worker.claims == [outcome]
    assert stop.waits == [5]


def test_worker_exception_waits_and_logs_only_error_type(caplog):
    stop = StopOnWait()
    failure = RuntimeError("synthetic provider detail must stay private")
    worker = Worker(Store(lambda: 0), [failure])

    with caplog.at_level(logging.WARNING):
        MemoryCompanion(worker, clock=lambda: 0).run_forever(stop=stop)

    assert worker.claims == [failure]
    assert stop.waits == [5]
    assert "memory companion failed error_type=RuntimeError" in caplog.text
    assert "synthetic provider detail" not in caplog.text


def test_stop_during_success_prevents_next_claim():
    stop = StopOnWait()
    first = {"state": "succeeded", "jobId": "in-flight"}
    worker = Worker(Store(lambda: 0), [first], before_return=lambda _count: stop.set())

    MemoryCompanion(worker, clock=lambda: 0).run_forever(stop=stop)

    # 완료 직전에 들어온 SIGTERM과 같은 stop은 성공 후 빠른 경로에서도 다음 claim 전에 반영된다.
    assert worker.claims == [first]
    assert stop.waits == []


def test_already_stopped_does_not_claim_or_run_maintenance():
    stop = StopOnWait()
    stop.set()
    store = Store(lambda: 0)
    worker = Worker(store, [])

    MemoryCompanion(worker, clock=lambda: 0).run_forever(stop=stop)

    assert worker.claims == []
    assert store.cleanups == []
    assert stop.waits == []


@pytest.mark.parametrize("last_cleanup,now", [(None, 0), (0, 3600)])
def test_cleanup_failure_keeps_backoff_even_after_successful_job(last_cleanup, now):
    stop = StopOnWait()
    store = Store(lambda: now, fail_cleanup=True)
    outcome = {"state": "succeeded"}
    worker = Worker(store, [outcome])
    companion = MemoryCompanion(worker, clock=lambda: now)
    companion._last_cleanup = last_cleanup

    companion.run_forever(stop=stop)

    assert worker.claims == [outcome]
    assert store.cleanups == [now]
    assert companion._last_cleanup == last_cleanup
    assert stop.waits == [5]


@pytest.mark.parametrize("failure,last_backup,now", [
    ("durability", 0, 0), ("snapshot", None, 0), ("snapshot", 0, 86_400), ("retention", None, 0),
])
def test_backup_provider_failure_keeps_backoff_after_success(failure, last_backup, now):
    stop = StopOnWait()
    backup = Backup(lambda: now, failure=failure)
    outcome = {"state": "succeeded"}
    worker = Worker(Store(lambda: now), [outcome])
    companion = MemoryCompanion(worker, backup, clock=lambda: now)
    companion._last_backup = last_backup

    companion.run_forever(stop=stop)

    assert worker.claims == [outcome]
    assert stop.waits == [5]


def test_successful_drain_keeps_hourly_cleanup_and_daily_snapshot_cadence():
    now = [0]
    times = [0, 3599, 3600, 86_399, 86_400, 86_400]
    def clock():
        return now[0]
    stop = StopOnWait()
    store = Store(clock)
    backup = Backup(clock)
    outcomes = [{"state": "succeeded"} for _ in times[:-1]] + [None]
    worker = Worker(store, outcomes, before_return=lambda count: now.__setitem__(0, times[count - 1]))

    MemoryCompanion(worker, backup, clock=clock).run_forever(stop=stop)

    assert worker.claims == outcomes
    assert stop.waits == [5]
    assert store.cleanups == [0, 3600, 86_399]
    assert backup.snapshots == backup.prunes == [0, 86_400]
    assert backup.flushes == times
