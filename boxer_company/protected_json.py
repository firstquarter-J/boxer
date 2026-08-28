from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
from typing import Any, Mapping


_MAX_PROTECTED_JSON_BYTES = 16 * 1024 * 1024


class ProtectedJsonFileError(RuntimeError):
    """create-only protected JSON 파일 계약을 지키지 못했을 때 발생한다."""


def create_protected_json_file(
    path_value: str | Path,
    payload: Mapping[str, Any],
    *,
    label: str,
) -> str:
    """기존 target을 덮지 않고 0600 JSON 파일을 원자 생성한다."""

    path = _validated_output_path(path_value, label=label)
    raw = _json_bytes(payload)
    if len(raw) > _MAX_PROTECTED_JSON_BYTES:
        raise ProtectedJsonFileError(f"{label} file is too large")
    temporary_path = _write_protected_temporary(path, raw)
    try:
        try:
            # hard link의 EEXIST가 concurrent initializer와 기존 revision을
            # 모두 같은 create-only 실패로 만든다.
            os.link(temporary_path, path)
        except FileExistsError as exc:
            raise ProtectedJsonFileError(f"{label} already exists") from exc
        _fsync_directory(path.parent)
    finally:
        temporary_path.unlink(missing_ok=True)
    return hashlib.sha256(raw).hexdigest()


def _validated_output_path(path_value: str | Path, *, label: str) -> Path:
    path = Path(path_value).expanduser()
    if not label.strip() or not path.is_absolute() or path == Path("/"):
        raise ProtectedJsonFileError(f"{label} path is invalid")
    _validate_protected_parent_chain(path, label=label)
    if not os.access(path.parent, os.W_OK | os.X_OK):
        raise ProtectedJsonFileError(f"{label} parent is not writable")
    return path


def _validate_protected_parent_chain(path: Path, *, label: str) -> None:
    """leaf부터 root까지 symlink·타인 writable directory를 모두 거부한다."""

    current = path.parent
    while True:
        try:
            current_stat = current.lstat()
        except OSError as exc:
            raise ProtectedJsonFileError(
                f"{label} parent is invalid"
            ) from exc
        if (
            stat.S_ISLNK(current_stat.st_mode)
            or not stat.S_ISDIR(current_stat.st_mode)
            or current_stat.st_uid not in {0, os.geteuid()}
            or current_stat.st_mode & 0o022
        ):
            raise ProtectedJsonFileError(f"{label} parent is not protected")
        if current.parent == current:
            break
        current = current.parent


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    try:
        return (
            json.dumps(
                dict(payload),
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ProtectedJsonFileError(
            "protected JSON payload is invalid"
        ) from exc


def _write_protected_temporary(path: Path, raw: bytes) -> Path:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    temporary_path = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise
    return temporary_path


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = [
    "create_protected_json_file",
    "ProtectedJsonFileError",
]
