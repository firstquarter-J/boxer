"""비공개 채널 종류 정정 CLI의 명시 승인과 메타데이터 전용 경계를 검증한다."""

import json
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

from boxer_company_api import memory_cli


@pytest.fixture
def cli(tmp_path, monkeypatch):
    # 저장소와 정정 함수를 대체해 실제 DB와 외부 provider 없이 CLI 경계만 검증한다.
    store = object()
    store_factory = Mock(return_value=store)
    security = Mock()
    result = {"state": "reconciled", "channelId": "C0621TL2HSB", "changedThreadCount": 2,
              "privateThreadCount": 3, "eventId": "evt_scope_test", "scopeDigest": "a" * 64,
              "replayed": False}
    reconcile = Mock(return_value=result)
    module = ModuleType("boxer_company.memory.channel_scope")
    module.operator_reconcile_private_channel = reconcile
    monkeypatch.setitem(sys.modules, module.__name__, module)
    backup = Mock(side_effect=AssertionError("백업을 시작하면 안 돼"))
    extractor = Mock(side_effect=AssertionError("모델 추출을 시작하면 안 돼"))
    worker = Mock(side_effect=AssertionError("추출 worker를 시작하면 안 돼"))
    for name, attribute, replacement in (
        ("boxer_company.memory.extraction", "build_configured_extractor", extractor),
        ("boxer_company.memory.worker", "MemoryWorker", worker),
    ):
        provider = ModuleType(name)
        setattr(provider, attribute, replacement)
        monkeypatch.setitem(sys.modules, name, provider)
    monkeypatch.setattr(memory_cli, "MemoryStore", store_factory)
    monkeypatch.setattr(memory_cli, "validate_company_api_runtime_security", security)
    monkeypatch.setattr(memory_cli, "configured_memory_backup", backup)
    path = tmp_path / "unused.db"
    arguments = ["reconcile-private-channel", "--path", str(path), "--tenant-id", "T19PA3EPL",
                 "--channel-id", "C0621TL2HSB", "--operator-id", "UOPERATOR", "--request-id", "scope-cli",
                 "--confirm-authorized-reconcile", "--confirm-slack-private-channel"]
    return SimpleNamespace(store=store, store_factory=store_factory, security=security,
                           reconcile=reconcile, backup=backup, extractor=extractor, worker=worker,
                           path=path, arguments=arguments, result=result)


@pytest.mark.parametrize("missing", ["--tenant-id", "--channel-id", "--operator-id", "--request-id",
                                    "--confirm-authorized-reconcile", "--confirm-slack-private-channel"])
def test_reconcile_cli_requires_scope_and_both_approvals_before_database_access(cli, missing):
    arguments = cli.arguments.copy()
    position = arguments.index(missing)
    del arguments[position:position + (1 if missing.startswith("--confirm-") else 2)]
    with pytest.raises(SystemExit) as error:
        memory_cli.main(arguments)
    assert error.value.code == 2
    cli.store_factory.assert_not_called()
    cli.reconcile.assert_not_called()
    assert not cli.path.exists()


@pytest.mark.parametrize("empty", ["--tenant-id", "--channel-id", "--operator-id", "--request-id"])
def test_reconcile_cli_rejects_empty_required_identity(cli, empty):
    arguments = cli.arguments.copy()
    arguments[arguments.index(empty) + 1] = ""
    with pytest.raises(SystemExit) as error:
        memory_cli.main(arguments)
    assert error.value.code == 2
    cli.store_factory.assert_not_called()
    cli.reconcile.assert_not_called()


def test_reconcile_cli_without_service_stop_forwards_exact_scope_and_only_metadata(cli, capsys):
    # API가 계속 실행 중이어도 운영자가 확인한 단일 채널 정정만 전달한다.
    memory_cli.main(cli.arguments)
    cli.security.assert_called_once_with()
    cli.store_factory.assert_called_once_with(cli.path)
    cli.reconcile.assert_called_once_with(cli.store, tenant_id="T19PA3EPL", channel_id="C0621TL2HSB",
        operator_id="UOPERATOR", request_id="scope-cli", confirm_slack_private_channel=True)
    captured = capsys.readouterr()
    assert json.loads(captured.out) == cli.result
    assert not captured.err
    cli.backup.assert_not_called()
    cli.extractor.assert_not_called()
    cli.worker.assert_not_called()
    assert not cli.path.exists()


def test_reconcile_cli_preserves_idempotent_replay_result(cli, capsys):
    cli.reconcile.return_value = {**cli.result, "changedThreadCount": 0, "replayed": True}
    memory_cli.main(cli.arguments)
    assert json.loads(capsys.readouterr().out)["replayed"] is True
    cli.backup.assert_not_called()


def test_reconcile_cli_runtime_security_failure_prevents_store_and_reconcile(cli, capsys):
    cli.security.side_effect = RuntimeError("비밀 원문 sentinel")
    with pytest.raises(SystemExit) as error:
        memory_cli.main(cli.arguments)
    assert str(error.value) == "기억 관리 실패: RuntimeError"
    cli.store_factory.assert_not_called()
    cli.reconcile.assert_not_called()
    assert capsys.readouterr() == ("", "")


def test_reconcile_cli_hides_domain_error_body_and_does_not_start_backup(cli, capsys):
    # 실패 메시지 안의 원문도 기존 CLI의 고정 예외 종류 외에는 출력하지 않는다.
    cli.reconcile.side_effect = ValueError("비밀 원문 sentinel https://invalid.example/private")
    with pytest.raises(SystemExit) as error:
        memory_cli.main(cli.arguments)
    assert str(error.value) == "기억 관리 실패: ValueError"
    assert capsys.readouterr() == ("", "")
    cli.backup.assert_not_called()
    cli.extractor.assert_not_called()
    cli.worker.assert_not_called()


@pytest.mark.parametrize("command", ["init", "migrate", "restore", "compact", "purge-derived"])
def test_existing_stop_required_commands_keep_their_service_guard(cli, command):
    # 새 메타데이터 명령의 예외가 기존 초기화와 삭제 명령의 중지 가드를 풀지 않는다.
    with pytest.raises(SystemExit) as error:
        memory_cli.main([command, "--path", str(cli.path)])
    assert error.value.code == 2
    cli.store_factory.assert_not_called()
    cli.reconcile.assert_not_called()
