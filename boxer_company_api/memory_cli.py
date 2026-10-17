"""기억 저장소의 명시 초기화·정리·검증 복원 전용 운영 CLI."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sqlite3

from boxer_company.memory.backup import configured_memory_backup
from boxer_company.memory.store import MemoryStore, initialize_memory_store
from boxer_company_api.security import validate_company_api_runtime_security


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="대화 기억 SQLite와 삭제 journal을 관리해")
    parser.add_argument("command", choices=("init", "migrate", "health", "init-journal", "flush-deletions", "snapshot", "restore", "cleanup", "compact", "prune", "delete-item", "import-history", "process-pending"))
    parser.add_argument("--path", default=os.environ.get("BOXER_COMPANY_API_MEMORY_STATE_PATH", "/var/lib/boxer-company-api/conversation_memory.db"))
    parser.add_argument("--snapshot-key")
    parser.add_argument("--tenant-id")
    parser.add_argument("--channel-id")
    parser.add_argument("--memory-id")
    parser.add_argument("--request-id")
    parser.add_argument("--input")
    parser.add_argument("--operator-id")
    parser.add_argument("--author-id", action="append", default=[])
    parser.add_argument("--confirm-authorized-import", action="store_true")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--confirm-service-stopped", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.command in {"init", "migrate", "restore", "compact"} and not args.confirm_service_stopped:
        parser.error("API와 기억 companion을 멈춘 뒤 --confirm-service-stopped가 필요해")
    path = Path(args.path).expanduser()
    try:
        validate_company_api_runtime_security()
        if args.command in {"init", "migrate"}:
            if args.command == "init" and path.exists():
                parser.error("이미 있는 DB야. schema 확인에는 migrate를 사용해")
            if args.command == "migrate" and not path.exists():
                parser.error("DB가 없어. 최초 생성에는 init을 사용해")
            initialize_memory_store(path)
            result = {"state": "ready", "schema": 1}
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
            if args.command == "process-pending":
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
            elif args.command == "import-history":
                if not all((args.input, args.operator_id, args.tenant_id, args.channel_id, args.author_id, args.confirm_authorized_import)):
                    parser.error("import-history에는 입력 파일·운영자·tenant·channel·허용 author와 명시 확인이 필요해")
                from boxer_company_api.memory_import import enqueue_history_import, read_history_import
                result = enqueue_history_import(store, read_history_import(args.input), tenant_id=args.tenant_id,
                    channel_id=args.channel_id, operator_id=args.operator_id, authorized_author_ids=set(args.author_id))
            elif args.command == "health":
                result = {"healthy": store.health()}
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
