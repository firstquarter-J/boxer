"""기억 저장소의 명시 초기화·정리·검증 복원 전용 운영 CLI."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path

from boxer_company.memory.backup import configured_memory_backup
from boxer_company.memory.contracts import MEMORY_SCHEMA_VERSION, MemoryError
from boxer_company.memory.store import MemoryStore, initialize_memory_store
from boxer_company_api.security import validate_company_api_runtime_security


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="대화 기억 SQLite와 삭제 journal을 관리해")
    parser.add_argument("command", choices=("init", "migrate", "health", "init-journal", "flush-deletions", "snapshot", "restore", "cleanup", "compact", "prune", "delete-item", "purge-derived", "import-history", "begin-raw-import", "import-raw", "process-pending", "reconcile-private-channel"))
    parser.add_argument("--path", default=os.environ.get("BOXER_COMPANY_API_MEMORY_STATE_PATH", "/var/lib/boxer-company-api/conversation_memory.db"))
    parser.add_argument("--snapshot-key")
    parser.add_argument("--tenant-id")
    parser.add_argument("--channel-id")
    parser.add_argument("--memory-id")
    parser.add_argument("--request-id")
    parser.add_argument("--input")
    parser.add_argument("--operator-id")
    parser.add_argument("--author-id", action="append", default=[])
    parser.add_argument("--purge-channel-id", action="append", default=[])
    parser.add_argument("--start-ts")
    parser.add_argument("--end-ts")
    parser.add_argument("--confirm-authorized-purge", action="store_true")
    parser.add_argument("--confirm-authorized-import", action="store_true")
    parser.add_argument("--confirm-current-source-revalidation", action="store_true")
    parser.add_argument("--confirm-authorized-reconcile", action="store_true")
    parser.add_argument("--confirm-slack-private-channel", action="store_true")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--confirm-service-stopped", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.confirm_current_source_revalidation and args.command not in {"begin-raw-import", "import-raw"}:
        parser.error("현재 원본 재검증 확인은 begin-raw-import와 import-raw에서만 사용할 수 있습니다.")
    if args.command in {"init", "migrate", "restore", "compact", "purge-derived"} and not args.confirm_service_stopped:
        parser.error("API와 기억 companion을 멈춘 뒤 --confirm-service-stopped가 필요해")
    # 채널 종류만 정정하는 명령은 서비스 중지를 요구하지 않고 두 승인을 DB 접근 전에 확인한다.
    if args.command == "reconcile-private-channel" and not all((args.tenant_id, args.channel_id,
            args.operator_id, args.request_id, args.confirm_authorized_reconcile, args.confirm_slack_private_channel)):
        parser.error("reconcile-private-channel에는 --tenant-id --channel-id --operator-id --request-id와 두 명시 확인이 필요해")
    path = Path(args.path).expanduser()
    try:
        validate_company_api_runtime_security()
        if args.command in {"init", "migrate"}:
            if args.command == "init" and path.exists():
                parser.error("이미 있는 DB야. schema 확인에는 migrate를 사용해")
            if args.command == "migrate" and not path.exists():
                parser.error("DB가 없어. 최초 생성에는 init을 사용해")
            initialize_memory_store(path)
            result = {"state": "ready", "schema": MEMORY_SCHEMA_VERSION}
        elif args.command in {"init-journal", "restore", "prune"}:
            backup = configured_memory_backup()
            if backup is None:
                parser.error("기억 전용 backup bucket/prefix/KMS 설정이 필요해")
            if args.command == "init-journal":
                backup.initialize_journal()
                result = {"journal": "initialized"}
            elif args.command == "prune":
                result = {"removedSnapshots": backup.prune_snapshots()}
            else:
                if not args.snapshot_key:
                    parser.error("restore에는 --snapshot-key가 필요해")
                result = backup.restore_snapshot(args.snapshot_key, path, store_factory=MemoryStore)
        else:
            store = MemoryStore(path)
            if args.command == "reconcile-private-channel":
                from boxer_company.memory.channel_scope import (
                    operator_reconcile_private_channel,
                )

                # 고정된 비공개 채널 범위의 메타데이터만 정정하며 추출과 백업은 시작하지 않는다.
                result = operator_reconcile_private_channel(store, tenant_id=args.tenant_id,
                    channel_id=args.channel_id, operator_id=args.operator_id, request_id=args.request_id,
                    confirm_slack_private_channel=args.confirm_slack_private_channel)
            elif args.command == "process-pending":
                if not 1 <= args.limit <= 1000:
                    parser.error("process-pending --limit은 1부터 1000까지야")
                from collections import Counter

                from boxer_company.memory.extraction import build_configured_extractor
                from boxer_company.memory.worker import MemoryWorker
                worker = MemoryWorker(store, build_configured_extractor())
                states = Counter()
                # 초기 적재 때만 고정 개수의 queue를 연속 처리한다. worker lease는 그대로 지킨다.
                for _ in range(args.limit):
                    job = worker.run_once()
                    if job is None:
                        break
                    states[str(job.get("state", "unknown"))] += 1
                    if sum(states.values()) % 10 == 0:
                        print(json.dumps({"processed": sum(states.values()), "states": dict(states)}), flush=True)
                result = {"processed": sum(states.values()), "states": dict(states)}
            elif args.command in {"begin-raw-import", "import-raw"}:
                if not all((args.input, args.operator_id, args.tenant_id, args.channel_id,
                            len(args.author_id) == 1, args.confirm_authorized_import)):
                    parser.error("원문 적재에는 입력 파일, 운영자, tenant, channel, author 1명과 명시 확인이 필요해")
                from boxer_company_api.memory_import import read_history_import
                from boxer_company_api.memory_raw_import import (
                    import_raw_archive,
                    revalidate_raw_archive,
                )
                # 수집 범위를 먼저 등록한 뒤 원본을 재검증하고 같은 파일을 게시한다.
                payload = read_history_import(args.input)
                scope = {"tenant_id": args.tenant_id, "channel_id": args.channel_id,
                         "author_id": args.author_id[0], "operator_id": args.operator_id}
                if args.command == "import-raw" and args.confirm_current_source_revalidation:
                    authorization = revalidate_raw_archive(store, payload, **scope)
                    event_ids = set(authorization["reauthorizationEventIds"])
                    if event_ids:
                        # 원문은 journal 내구화를 확인한 뒤에만 일반 import로 게시한다.
                        backup = configured_memory_backup()
                        if backup is None:
                            raise MemoryError("raw_import_revalidation_journal_required")
                        backup.flush_deletions(store)
                        durable = {event["id"] for event in store.deletion_events()
                                   if event.get("durabilityState") == "durable"}
                        if not event_ids <= durable:
                            raise MemoryError("raw_import_revalidation_durability_pending")
                result = import_raw_archive(store, payload, **scope,
                    begin_only=args.command == "begin-raw-import",
                    plan_revalidation=args.command == "begin-raw-import" and args.confirm_current_source_revalidation)
            elif args.command == "import-history":
                if not all((args.input, args.operator_id, args.tenant_id, args.channel_id, args.author_id, args.confirm_authorized_import)):
                    parser.error("import-history에는 입력 파일·운영자·tenant·channel·허용 author와 명시 확인이 필요해")
                from boxer_company_api.memory_import import (
                    enqueue_history_import,
                    read_history_import,
                )
                result = enqueue_history_import(store, read_history_import(args.input), tenant_id=args.tenant_id,
                    channel_id=args.channel_id, operator_id=args.operator_id, authorized_author_ids=set(args.author_id))
            elif args.command == "health":
                result = {"healthy": store.health()}
            elif args.command == "purge-derived":
                if not args.confirm_authorized_purge or not all((args.tenant_id, args.operator_id, args.request_id, args.purge_channel_id)):
                    parser.error("purge-derived에는 승인 확인과 --tenant-id --operator-id --request-id --purge-channel-id가 필요해")
                from boxer_company.memory.derived_purge import operator_purge_derived

                # 원문과 출처를 남기고 파생물만 제거한다. journal 내구화 실패는 pending으로 남긴다.
                scope = {"tenant_id": args.tenant_id, "channel_ids": args.purge_channel_id,
                         "operator_id": args.operator_id, "request_id": args.request_id,
                         "start_ts": args.start_ts, "end_ts": args.end_ts}
                result = operator_purge_derived(store, **scope)
                try:
                    backup = configured_memory_backup()
                    if backup is not None:
                        backup.flush_deletions(store)
                        result = operator_purge_derived(store, **scope)
                except Exception:  # noqa: BLE001 - 외부 오류를 숨기고 로컬 삭제의 pending 상태를 보존한다.
                    result["durability"] = "pending"
            elif args.command == "delete-item":
                if not all((args.tenant_id, args.channel_id, args.memory_id, args.request_id)):
                    parser.error("delete-item에는 --tenant-id --channel-id --memory-id --request-id가 필요해")
                # 운영자에게만 노출된 CLI이며 tenant/channel/item을 모두 지정해야 한다.
                event = store.operator_forget(tenant_id=args.tenant_id, channel_id=args.channel_id, memory_id=args.memory_id, request_id=args.request_id)
                result = {"state": "hidden", "eventId": event["id"], "durability": event.get("durabilityState", "pending")}
                try:
                    backup = configured_memory_backup()
                    if backup is not None:
                        backup.flush_deletions(store)
                        result["durability"] = "durable"
                except Exception:
                    # 삭제는 이미 로컬에서 적용됐다. 원격 실패를 삭제 완료로 꾸미지 않는다.
                    result["durability"] = "pending"
            elif args.command == "cleanup":
                result = {"cleanup": store.cleanup()}
            elif args.command == "compact":
                store.cleanup()
                # 서비스 중지 확인 뒤 WAL을 비우고 secure_delete 잔여 page를 재작성한다.
                with sqlite3.connect(path.as_uri() + "?mode=rw", uri=True) as connection:
                    connection.execute("PRAGMA secure_delete=ON")
                    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    connection.execute("VACUUM")
                result = {"compacted": True}
            else:
                backup = configured_memory_backup()
                if backup is None:
                    parser.error("기억 전용 backup bucket/prefix/KMS 설정이 필요해")
                if args.command == "flush-deletions":
                    result = {"durableDeletions": backup.flush_deletions(store)}
                else:
                    result = backup.create_snapshot(store)
    except Exception as exc:
        # 원문·credential이 담길 수 있는 exception 메시지는 운영 출력에서 제외한다.
        raise SystemExit("기억 관리 실패: " + type(exc).__name__) from None
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
