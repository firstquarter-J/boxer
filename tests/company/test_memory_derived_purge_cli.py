"""파생물 삭제 CLI의 범위 전달과 내구화 완료 판정을 검증한다."""

import json

import pytest

from boxer_company.memory.store import MemoryStore, initialize_memory_store
from boxer_company_api import memory_cli


@pytest.fixture
def cli(tmp_path, monkeypatch):
    path = initialize_memory_store(tmp_path / "private" / "memory.db")
    monkeypatch.setattr(memory_cli, "validate_company_api_runtime_security", lambda: None)
    monkeypatch.setattr(memory_cli, "configured_memory_backup", lambda: None)
    arguments = ["purge-derived", "--path", str(path), "--tenant-id", "T1",
                 "--operator-id", "UOP", "--request-id", "purge-cli",
                 "--purge-channel-id", "C1", "--purge-channel-id", "C2",
                 "--start-ts", "1790780400.000000", "--end-ts", "1793458800.000000",
                 "--confirm-authorized-purge", "--confirm-service-stopped"]
    return MemoryStore(path), arguments


@pytest.mark.parametrize("missing", ["--confirm-authorized-purge", "--confirm-service-stopped"])
def test_cli_requires_operator_approval_and_stopped_service(cli, missing):
    store, arguments = cli
    arguments.remove(missing)
    with pytest.raises(SystemExit) as error:
        memory_cli.main(arguments)
    assert error.value.code == 2
    assert store.deletion_events() == []


def test_cli_without_backup_keeps_local_purge_pending(cli, capsys):
    store, arguments = cli
    memory_cli.main(arguments)
    result = json.loads(capsys.readouterr().out)
    assert result == {"state": "purged", "purgedItemCount": 0, "scopeChannelCount": 2,
                      "journalEventCount": 2, "durability": "pending"}
    assert {event["scopeChannelId"] for event in store.deletion_events()} == {"C1", "C2"}


def test_cli_reports_durable_only_after_all_journal_receipts(cli, monkeypatch, capsys):
    store, arguments = cli

    class Backup:
        def flush_deletions(self, target):
            # 외부 쓰기 대신 journal 영수증을 실제 저장소에 반영한다.
            for sequence, event in enumerate(target.deletion_events(), start=1):
                target.mark_deletion_durable(event["id"], sequence)

    monkeypatch.setattr(memory_cli, "configured_memory_backup", Backup)
    memory_cli.main(arguments)
    assert json.loads(capsys.readouterr().out)["durability"] == "durable"
    assert all(event["durabilityState"] == "durable" for event in store.deletion_events())


def test_cli_does_not_claim_durability_after_remote_failure(cli, monkeypatch, capsys):
    store, arguments = cli

    class Backup:
        def flush_deletions(self, target):
            raise RuntimeError("외부 오류 본문은 출력하면 안 돼")

    monkeypatch.setattr(memory_cli, "configured_memory_backup", Backup)
    memory_cli.main(arguments)
    output = capsys.readouterr().out
    assert json.loads(output)["durability"] == "pending"
    assert "외부 오류" not in output
    assert all(event["durabilityState"] == "pending" for event in store.deletion_events())
