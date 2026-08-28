import hashlib
import json
import threading
import time
from typing import Any, Callable
from urllib import error, request

from boxer_company import settings as cs
from boxer_company.utils import _display_value
from boxer_company.routers.device_ssh_security import (
    _company_api_device_ssh_open_attempted,
    _is_company_api_device_ssh_context,
    _mark_company_api_device_ssh_open_attempted,
    _mark_company_api_mutation_attempted,
)

_mda_access_token_cache: str | None = None
_DEVICE_SSH_LOCK_STRIPE_COUNT = 64
_device_ssh_lock_stripes = tuple(
    threading.Lock() for _ in range(_DEVICE_SSH_LOCK_STRIPE_COUNT)
)

_ADMIN_USER_QUERY = """
query AdminUser($userPassword: String!) {
  adminUser(userPassword: $userPassword) {
    seq
    userEmail
    enabledFlag
    superFlag
    accessToken
  }
}
"""

_SSH_ORDER_MUTATION = """
mutation SshOrder($deviceName: String!, $action: String!, $host: String!) {
  sshOrder(deviceName: $deviceName, action: $action, host: $host) {
    affected
    status
    message
  }
}
"""

_SEND_COMMAND_MUTATION = """
mutation SendCommand($deviceName: String!, $command: String!, $acme: JSON) {
  sendCommand(deviceName: $deviceName, command: $command, acme: $acme) {
    affected
    status
    message
  }
}
"""

_UPDATE_BOX_MUTATION = """
mutation UpdateBox($deviceName: String!, $version: String!, $silent: Boolean!) {
  updateBox(deviceName: $deviceName, version: $version, silent: $silent) {
    affected
    status
    message
  }
}
"""

_DEVICE_VERSIONS_QUERY = """
query DeviceVersions {
  deviceVersions {
    seq
    versionName
    buildNum
    buildHash
    versionDescription
    versionDate
    visibleFlag
    autoUpdate
    createdAt
    updatedAt
  }
}
"""

_CREATE_ACTIVITY_LOG_MUTATION = """
mutation CreateActivityLog($input: ActivityLogCreateInput!) {
  createActivityLog(input: $input) {
    affected
    status
    message
  }
}
"""

_PAGINATED_DEVICES_QUERY = """
query PaginatedDevices($listOptions: DeviceListOptions!) {
  paginatedDevices(listOptions: $listOptions) {
    nodes {
      deviceName
      version
      cfg1_use_diary_capture
      cfg1_check_invalid_barcode
      cfg1_check_expired_barcode
      cfg1_check_pink_barcode
      deviceState {
        captureBoardType
        isConnected
        isRecording
        isUploading
        status
        connectedAt
        updatedAt
        ip
      }
      hospital {
        hospitalName
      }
      hospitalRoom {
        roomName
      }
      agentState {
        isConnected
        agentVersion
        connectedAt
        updatedAt
        agentSsh {
          action
          host
          port
          status
          error
        }
      }
    }
  }
}
"""

_PAGINATED_SPECIAL_BARCODES_QUERY = """
query PaginatedSpecialBarcodes($listOptions: SpecialBarcodeListOptions) {
  paginatedSpecialBarcodes(listOptions: $listOptions) {
    nodes {
      seq
      barcode
      type
      reason
      createdAt
      updatedAt
    }
  }
}
"""

_STOPPED_RECORDING_RESTORE_CANDIDATES_QUERY = """
query StoppedRecordingRestoreCandidates($barcode: String!, $hospitalSeq: Int!) {
  stoppedRecordingRestoreCandidates(barcode: $barcode, hospitalSeq: $hospitalSeq) {
    seq
    fullBarcode
    fileId
    recordedAt
    currentS3FileKey
    expectedS3FileKey
    restorable
    failureReason
  }
}
"""

_RESTORE_STOPPED_RECORDINGS_MUTATION = """
mutation RestoreStoppedRecordings($input: RestoreStoppedRecordingsInput!) {
  restoreStoppedRecordings(input: $input) {
    status
    message
    requestedCount
    restoredCount
    failedCount
    failedItems {
      seq
      fileId
      reason
    }
  }
}
"""


def _is_mda_graphql_configured() -> bool:
    return bool(cs.MDA_GRAPHQL_URL and cs.MDA_ADMIN_USER_PASSWORD)


def _execute_mda_graphql_request(
    query: str,
    variables: dict[str, Any],
    *,
    timeout_sec: int | None = None,
    auth_token: str | None = None,
) -> dict[str, Any]:
    if not cs.MDA_GRAPHQL_URL:
        raise RuntimeError("MDA GraphQL 설정(MDA_GRAPHQL_URL)이 없어")

    actual_timeout = max(1, timeout_sec if timeout_sec is not None else cs.MDA_API_TIMEOUT_SEC)
    body = json.dumps(
        {
            "query": query,
            "variables": variables,
        }
    ).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/graphql-response+json,application/json;q=0.9",
        "Origin": cs.MDA_GRAPHQL_ORIGIN,
        "Referer": cs.MDA_GRAPHQL_REFERER,
        "User-Agent": cs.MDA_GRAPHQL_USER_AGENT,
    }
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    req = request.Request(
        url=cs.MDA_GRAPHQL_URL,
        data=body,
        headers=headers,
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=actual_timeout) as response:
            raw = response.read().decode("utf-8")
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"MDA GraphQL HTTP {exc.code}: {detail[:300]}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"MDA GraphQL 연결 실패: {exc.reason}") from exc

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("MDA GraphQL 응답 JSON 파싱에 실패했어") from exc

    graphql_errors = payload.get("errors")
    if isinstance(graphql_errors, list) and graphql_errors:
        messages = [
            str(item.get("message") or "").strip()
            for item in graphql_errors
            if isinstance(item, dict)
        ]
        detail = "; ".join(message for message in messages if message) or "unknown error"
        raise RuntimeError(f"MDA GraphQL 오류: {detail}")

    data = payload.get("data")
    if not isinstance(data, dict):
        raise RuntimeError("MDA GraphQL 응답에 data가 없어")
    return data


def _get_mda_access_token(*, force_refresh: bool = False) -> str:
    global _mda_access_token_cache

    if not _is_mda_graphql_configured():
        raise RuntimeError("MDA GraphQL 설정(MDA_GRAPHQL_URL, MDA_ADMIN_USER_PASSWORD)이 없어")

    if _mda_access_token_cache and not force_refresh:
        return _mda_access_token_cache

    data = _execute_mda_graphql_request(
        _ADMIN_USER_QUERY,
        {
            "userPassword": cs.MDA_ADMIN_USER_PASSWORD,
        },
    )
    result = data.get("adminUser")
    if not isinstance(result, dict):
        raise RuntimeError("MDA adminUser 응답 형식이 올바르지 않아")

    enabled_flag = bool(result.get("enabledFlag"))
    super_flag = bool(result.get("superFlag"))
    access_token = str(result.get("accessToken") or "").strip()
    if not enabled_flag or not super_flag or not access_token:
        raise RuntimeError("MDA adminUser 인증 결과가 유효하지 않아")

    _mda_access_token_cache = access_token
    return access_token


def _execute_mda_graphql(
    query: str,
    variables: dict[str, Any],
    *,
    timeout_sec: int | None = None,
    before_request: Callable[[], None] | None = None,
) -> dict[str, Any]:
    token = _get_mda_access_token()
    try:
        if before_request is not None:
            # 기존 auth-refresh retry를 유지하되 API request guard가
            # 실제 외부 write 시도를 놓치지 않게 전송 직전에 표시한다.
            before_request()
        return _execute_mda_graphql_request(
            query,
            variables,
            timeout_sec=timeout_sec,
            auth_token=token,
        )
    except RuntimeError as exc:
        if "Unauthorized" not in str(exc):
            raise
        token = _get_mda_access_token(force_refresh=True)
        if before_request is not None:
            before_request()
        return _execute_mda_graphql_request(
            query,
            variables,
            timeout_sec=timeout_sec,
            auth_token=token,
        )


def _execute_mda_graphql_once(
    query: str,
    variables: dict[str, Any],
    *,
    before_request: Callable[[], None] | None = None,
) -> dict[str, Any]:
    """API mutation은 인증 오류에도 결과 불명 request를 재전송하지 않는다."""

    token = _get_mda_access_token()
    # token 확보가 끝난 뒤 실제 mutation transport 직전에만 side-effect
    # marker를 세워 preflight 실패와 결과 불명 실패를 구분한다.
    marker = before_request or _mark_company_api_mutation_attempted
    marker()
    return _execute_mda_graphql_request(
        query,
        variables,
        auth_token=token,
    )


def _execute_mda_graphql_mutation(
    query: str,
    variables: dict[str, Any],
    *,
    before_request: Callable[[], None] | None = None,
) -> dict[str, Any]:
    """Slack local auth-refresh는 보존하고 API write만 at-most-once로 보낸다."""

    if _is_company_api_device_ssh_context():
        return _execute_mda_graphql_once(
            query,
            variables,
            before_request=before_request,
        )
    return _execute_mda_graphql(
        query,
        variables,
        before_request=before_request,
    )


def _normalize_agent_ssh(agent_ssh: Any) -> dict[str, Any] | None:
    if not isinstance(agent_ssh, dict):
        return None

    host = str(agent_ssh.get("host") or "").strip()
    port_raw = agent_ssh.get("port")
    port: int | None = None
    if isinstance(port_raw, int):
        port = port_raw
    elif isinstance(port_raw, str) and port_raw.strip():
        try:
            port = int(port_raw.strip())
        except ValueError:
            port = None

    return {
        "action": _display_value(agent_ssh.get("action"), default=""),
        "host": host,
        "port": port,
        "status": _display_value(agent_ssh.get("status"), default=""),
        "error": _display_value(agent_ssh.get("error"), default=""),
    }


def _normalize_mda_state_text(value: Any) -> str:
    text = _display_value(value, default="")
    return "" if text.upper() == "NONE" else text


def _normalize_mda_boolean(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    normalized = str(value or "").strip().lower()
    return normalized in {"1", "true", "t", "yes", "y", "on"}


def _normalize_optional_mda_boolean(value: Any) -> bool | None:
    if value is None:
        return None
    return _normalize_mda_boolean(value)


def _normalize_device_activity_boolean(value: Any) -> bool | None:
    """녹화·업로드 안전 가드는 GraphQL의 실제 Boolean만 신뢰한다."""

    return value if isinstance(value, bool) else None


def _normalize_optional_mda_int(value: Any) -> int | None:
    # MDA의 신규 장비 설정은 bool이 아니라 -1/0/1 숫자 상태를 유지해야 한다.
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"", "null", "none"}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_device_row(data: dict[str, Any], device_name: str) -> dict[str, Any] | None:
    paginated = data.get("paginatedDevices")
    if not isinstance(paginated, dict):
        return None
    rows = paginated.get("nodes")
    if not isinstance(rows, list):
        return None

    exact_match: dict[str, Any] | None = None
    fallback_match: dict[str, Any] | None = None
    target = device_name.strip()
    for row in rows:
        if not isinstance(row, dict):
            continue
        current_name = str(row.get("deviceName") or "").strip()
        if not current_name:
            continue
        if current_name == target:
            exact_match = row
            break
        if fallback_match is None and target in current_name:
            fallback_match = row
    return exact_match or fallback_match


def _normalize_mda_device_detail(row: dict[str, Any], *, device_name: str) -> dict[str, Any]:
    device_state = row.get("deviceState") if isinstance(row.get("deviceState"), dict) else {}
    hospital = row.get("hospital") if isinstance(row.get("hospital"), dict) else {}
    hospital_room = row.get("hospitalRoom") if isinstance(row.get("hospitalRoom"), dict) else {}
    agent_state = row.get("agentState") if isinstance(row.get("agentState"), dict) else {}
    agent_ssh = _normalize_agent_ssh(agent_state.get("agentSsh"))
    version = _normalize_mda_state_text(row.get("version"))

    return {
        "deviceName": _display_value(row.get("deviceName"), default=device_name),
        "version": version,
        "useDiaryCapture": _normalize_optional_mda_boolean(row.get("cfg1_use_diary_capture")),
        "checkInvalidBarcode": _normalize_optional_mda_boolean(row.get("cfg1_check_invalid_barcode")),
        "checkExpiredBarcode": _normalize_optional_mda_int(row.get("cfg1_check_expired_barcode")),
        "checkPinkBarcode": _normalize_optional_mda_int(row.get("cfg1_check_pink_barcode")),
        "captureBoardType": _normalize_mda_state_text(device_state.get("captureBoardType")),
        "hospitalName": _display_value(hospital.get("hospitalName"), default="미확인"),
        "roomName": _display_value(hospital_room.get("roomName"), default="미확인"),
        "isConnected": _normalize_mda_boolean(agent_state.get("isConnected")),
        "deviceIsConnected": _normalize_mda_boolean(device_state.get("isConnected")),
        "isRecording": _normalize_device_activity_boolean(
            device_state.get("isRecording")
        ),
        "isUploading": _normalize_device_activity_boolean(
            device_state.get("isUploading")
        ),
        "deviceStatus": _normalize_mda_state_text(device_state.get("status")),
        "deviceConnectedAt": _display_value(device_state.get("connectedAt"), default=""),
        "deviceUpdatedAt": _display_value(device_state.get("updatedAt"), default=""),
        "deviceIp": _display_value(device_state.get("ip"), default=""),
        "agentVersion": _display_value(agent_state.get("agentVersion"), default=""),
        "agentConnectedAt": _display_value(agent_state.get("connectedAt"), default=""),
        "agentUpdatedAt": _display_value(agent_state.get("updatedAt"), default=""),
        "agentSsh": agent_ssh,
    }


def _get_mda_device_detail(device_name: str) -> dict[str, Any] | None:
    data = _execute_mda_graphql(
        _PAGINATED_DEVICES_QUERY,
        {
            "listOptions": {
                "search": device_name,
                "page": 1,
                "limit": 5,
            }
        },
    )
    row = _extract_device_row(data, device_name)
    if not row:
        return None
    return _normalize_mda_device_detail(row, device_name=device_name)


def _get_mda_device_agent_ssh(device_name: str) -> dict[str, Any] | None:
    return _get_mda_device_detail(device_name)


def _send_mda_device_command(
    device_name: str,
    *,
    command: str,
    acme: Any | None = None,
) -> dict[str, Any]:
    # Slack local은 기존 auth-refresh를 유지하고 API mutation은 재전송하지 않는다.
    data = _execute_mda_graphql_mutation(
        _SEND_COMMAND_MUTATION,
        {
            "deviceName": device_name,
            "command": command,
            "acme": acme,
        },
        before_request=_mark_company_api_mutation_attempted,
    )
    result = data.get("sendCommand")
    if not isinstance(result, dict):
        raise RuntimeError("sendCommand 응답 형식이 올바르지 않아")
    return {
        "affected": result.get("affected"),
        "status": _normalize_mda_boolean(result.get("status")),
        "message": _display_value(result.get("message"), default=""),
        "command": _display_value(command, default=""),
        "acme": acme,
    }


def _send_mda_device_ping(device_name: str) -> dict[str, Any]:
    return _send_mda_device_command(device_name, command="ping")


def _get_mda_devices_details(device_names: list[str]) -> dict[str, dict[str, Any]]:
    details: dict[str, dict[str, Any]] = {}
    seen_names: set[str] = set()
    for raw_name in device_names:
        normalized_name = str(raw_name or "").strip()
        if not normalized_name or normalized_name in seen_names:
            continue
        seen_names.add(normalized_name)

        detail = _get_mda_device_detail(normalized_name)
        if isinstance(detail, dict):
            details[normalized_name] = detail
    return details


def _lookup_mda_special_barcodes_by_barcode(barcode: str) -> list[dict[str, Any]]:
    normalized_barcode = str(barcode or "").strip()
    if not normalized_barcode:
        return []

    data = _execute_mda_graphql(
        _PAGINATED_SPECIAL_BARCODES_QUERY,
        {
            "listOptions": {
                "barcode": normalized_barcode,
                "page": 1,
                "limit": 10,
            }
        },
    )
    paginated = data.get("paginatedSpecialBarcodes")
    if not isinstance(paginated, dict):
        return []
    rows = paginated.get("nodes")
    if not isinstance(rows, list):
        return []

    items: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_barcode = _display_value(row.get("barcode"), default="")
        if row_barcode != normalized_barcode:
            continue
        items.append(
            {
                "seq": row.get("seq"),
                "barcode": row_barcode,
                "type": _display_value(row.get("type"), default=""),
                "reason": _display_value(row.get("reason"), default=""),
                "createdAt": _display_value(row.get("createdAt"), default=""),
                "updatedAt": _display_value(row.get("updatedAt"), default=""),
            }
        )
    return items


def _get_mda_stopped_recording_restore_candidates(
    barcode: str,
    hospital_seq: int,
) -> list[dict[str, Any]]:
    normalized_barcode = str(barcode or "").strip()
    if not normalized_barcode or int(hospital_seq or 0) <= 0:
        return []

    data = _execute_mda_graphql(
        _STOPPED_RECORDING_RESTORE_CANDIDATES_QUERY,
        {
            "barcode": normalized_barcode,
            "hospitalSeq": int(hospital_seq),
        },
    )
    rows = data.get("stoppedRecordingRestoreCandidates")
    if not isinstance(rows, list):
        raise RuntimeError("stoppedRecordingRestoreCandidates 응답 형식이 올바르지 않아")

    candidates: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        candidates.append(
            {
                "seq": row.get("seq"),
                "fullBarcode": _display_value(row.get("fullBarcode"), default=""),
                "fileId": _display_value(row.get("fileId"), default=""),
                "recordedAt": row.get("recordedAt"),
                "currentS3FileKey": _display_value(row.get("currentS3FileKey"), default=""),
                "expectedS3FileKey": _display_value(row.get("expectedS3FileKey"), default=""),
                "restorable": _normalize_mda_boolean(row.get("restorable")),
                "failureReason": _display_value(row.get("failureReason"), default=""),
            }
        )
    return candidates


def _restore_mda_stopped_recordings(
    *,
    barcode: str,
    hospital_seq: int,
    recording_seqs: list[int],
    reason: str,
) -> dict[str, Any]:
    normalized_barcode = str(barcode or "").strip()
    normalized_reason = str(reason or "").strip()
    normalized_seqs = [int(seq) for seq in recording_seqs if int(seq or 0) > 0]
    if not normalized_barcode or int(hospital_seq or 0) <= 0 or not normalized_seqs:
        raise RuntimeError("스트리밍 종료 영상 복원 입력이 부족해")
    if not normalized_reason:
        raise RuntimeError("스트리밍 종료 영상 복원 사유가 필요해")

    data = _execute_mda_graphql_mutation(
        _RESTORE_STOPPED_RECORDINGS_MUTATION,
        {
            "input": {
                "barcode": normalized_barcode,
                "hospitalSeq": int(hospital_seq),
                "recordingSeqs": normalized_seqs,
                "reason": normalized_reason,
            }
        },
        before_request=_mark_company_api_mutation_attempted,
    )
    result = data.get("restoreStoppedRecordings")
    if not isinstance(result, dict):
        raise RuntimeError("restoreStoppedRecordings 응답 형식이 올바르지 않아")

    failed_items: list[dict[str, Any]] = []
    for item in result.get("failedItems") or []:
        if not isinstance(item, dict):
            continue
        failed_items.append(
            {
                "seq": item.get("seq"),
                "fileId": _display_value(item.get("fileId"), default=""),
                "reason": _display_value(item.get("reason"), default=""),
            }
        )

    return {
        "status": _normalize_mda_boolean(result.get("status")),
        "message": _display_value(result.get("message"), default=""),
        "requestedCount": int(result.get("requestedCount") or 0),
        "restoredCount": int(result.get("restoredCount") or 0),
        "failedCount": int(result.get("failedCount") or 0),
        "failedItems": failed_items,
    }


def _open_mda_device_ssh(
    device_name: str,
    *,
    host: str | None = None,
) -> dict[str, Any]:
    actual_host = (host or cs.MDA_SSH_OPEN_HOST).strip()
    if not actual_host:
        raise RuntimeError("MDA_SSH_OPEN_HOST가 비어 있어")

    # reverse tunnel을 끊을 수 있어 API에서는 Unauthorized도 재전송하지 않는다.
    data = _execute_mda_graphql_mutation(
        _SSH_ORDER_MUTATION,
        {
            "deviceName": device_name,
            "action": "open",
            "host": actual_host,
        },
        before_request=lambda: _mark_company_api_device_ssh_open_attempted(
            device_name
        ),
    )
    result = data.get("sshOrder")
    if not isinstance(result, dict):
        raise RuntimeError("sshOrder 응답 형식이 올바르지 않아")
    return {
        "affected": _display_value(result.get("affected"), default=""),
        "status": _display_value(result.get("status"), default=""),
        "message": _display_value(result.get("message"), default=""),
        "host": actual_host,
    }


def _update_mda_device_box(
    device_name: str,
    *,
    version: str,
    silent: bool = False,
) -> dict[str, Any]:
    data = _execute_mda_graphql_mutation(
        _UPDATE_BOX_MUTATION,
        {
            "deviceName": device_name,
            "version": version,
            "silent": bool(silent),
        },
        before_request=_mark_company_api_mutation_attempted,
    )
    result = data.get("updateBox")
    if not isinstance(result, dict):
        raise RuntimeError("updateBox 응답 형식이 올바르지 않아")
    return {
        "affected": result.get("affected"),
        "status": bool(result.get("status")),
        "message": _display_value(result.get("message"), default=""),
    }


def _parse_semver_parts(version: Any) -> tuple[int, int, int] | None:
    raw = str(version or "").strip()
    parts = raw.split(".")
    if len(parts) != 3 or any(not part.isdigit() for part in parts):
        return None
    return int(parts[0]), int(parts[1]), int(parts[2])


def _get_mda_latest_device_version() -> dict[str, Any]:
    data = _execute_mda_graphql(_DEVICE_VERSIONS_QUERY, {})
    rows = data.get("deviceVersions")
    if not isinstance(rows, list):
        raise RuntimeError("deviceVersions 응답 형식이 올바르지 않아")

    best_row: dict[str, Any] | None = None
    best_parts: tuple[int, int, int] | None = None
    best_visible = False
    for row in rows:
        if not isinstance(row, dict):
            continue
        version_name = _display_value(row.get("versionName"), default="")
        parts = _parse_semver_parts(version_name)
        if parts is None:
            continue
        is_visible = bool(row.get("visibleFlag"))
        if (
            best_parts is None
            or (is_visible and not best_visible)
            or (is_visible == best_visible and parts > best_parts)
        ):
            best_parts = parts
            best_row = row
            best_visible = is_visible

    if best_row is None:
        raise RuntimeError("MDA 최신 박스 버전을 찾지 못했어")

    return {
        "seq": best_row.get("seq"),
        "versionName": _display_value(best_row.get("versionName"), default=""),
        "buildNum": best_row.get("buildNum"),
        "buildHash": _display_value(best_row.get("buildHash"), default=""),
        "versionDescription": _display_value(best_row.get("versionDescription"), default=""),
        "versionDate": _display_value(best_row.get("versionDate"), default=""),
        "visibleFlag": bool(best_row.get("visibleFlag")),
        "autoUpdate": bool(best_row.get("autoUpdate")),
    }


def _create_mda_activity_log(input_payload: dict[str, Any]) -> dict[str, Any]:
    normalized_input = {
        key: value
        for key, value in (input_payload or {}).items()
        if value is not None and value != ""
    }
    if not normalized_input:
        raise RuntimeError("activity log 입력이 비어 있어")

    data = _execute_mda_graphql_mutation(
        _CREATE_ACTIVITY_LOG_MUTATION,
        {
            "input": normalized_input,
        },
        before_request=_mark_company_api_mutation_attempted,
    )
    result = data.get("createActivityLog")
    if not isinstance(result, dict):
        raise RuntimeError("createActivityLog 응답 형식이 올바르지 않아")
    if result.get("status") is False:
        raise RuntimeError(
            f"createActivityLog 실패: {_display_value(result.get('message'), default='unknown error')}"
        )
    return {
        "affected": result.get("affected"),
        "status": bool(result.get("status", True)),
        "message": _display_value(result.get("message"), default=""),
    }


def _device_ssh_lock(device_name: str) -> threading.Lock:
    """고정 stripe에서 같은 장비의 endpoint lifecycle lock을 고른다."""

    normalized = str(device_name or "").strip().casefold()
    digest = hashlib.sha256(normalized.encode("utf-8")).digest()
    stripe_index = int.from_bytes(digest[:4], "big") % len(
        _device_ssh_lock_stripes
    )
    return _device_ssh_lock_stripes[stripe_index]


def _has_usable_agent_ssh_endpoint(agent_ssh: Any) -> bool:
    """Redis의 stale close metadata를 열린 reverse tunnel로 오인하지 않는다."""

    if not isinstance(agent_ssh, dict):
        return False
    status = str(agent_ssh.get("status") or "").strip().lower()
    if status and status not in {"open", "opened", "ready", "connected"}:
        return False
    return bool(agent_ssh.get("host")) and bool(agent_ssh.get("port"))


def _wait_for_mda_device_agent_ssh(
    device_name: str,
    *,
    host: str | None = None,
    poll_timeout_sec: int | None = None,
    poll_interval_sec: int | None = None,
    resend_every: int | None = None,
    force_reopen: bool = False,
    resend_enabled: bool = True,
) -> dict[str, Any]:
    """동일 장비 lifecycle을 직렬화하고 API의 단일-open 예산을 강제한다."""

    api_context = _is_company_api_device_ssh_context()
    with _device_ssh_lock(device_name):
        result = _wait_for_mda_device_agent_ssh_locked(
            device_name,
            host=host,
            poll_timeout_sec=poll_timeout_sec,
            poll_interval_sec=poll_interval_sec,
            resend_every=resend_every,
            # API는 stale endpoint를 이유로 기존 tunnel을 끊지 않는다.
            force_reopen=force_reopen and not api_context,
            # Slack local 기본 재전송은 유지하되 API poll은 조회만 한다.
            resend_enabled=resend_enabled and not api_context,
            api_force_reopen_blocked=api_context and force_reopen,
        )
    return result


def _wait_for_mda_device_agent_ssh_locked(
    device_name: str,
    *,
    host: str | None = None,
    poll_timeout_sec: int | None = None,
    poll_interval_sec: int | None = None,
    resend_every: int | None = None,
    force_reopen: bool = False,
    resend_enabled: bool = True,
    api_force_reopen_blocked: bool = False,
) -> dict[str, Any]:
    actual_poll_timeout = max(
        1,
        poll_timeout_sec if poll_timeout_sec is not None else cs.MDA_SSH_POLL_TIMEOUT_SEC,
    )
    actual_poll_interval = max(
        1,
        poll_interval_sec if poll_interval_sec is not None else cs.MDA_SSH_POLL_INTERVAL_SEC,
    )
    actual_resend_every = max(
        1,
        resend_every if resend_every is not None else cs.MDA_SSH_POLL_RESEND_EVERY,
    )

    current_state = _get_mda_device_agent_ssh(device_name)
    current_agent_ssh = ((current_state or {}).get("agentSsh") or {}) if isinstance(current_state, dict) else {}
    current_endpoint = (
        (
            _display_value(current_agent_ssh.get("host"), default=""),
            current_agent_ssh.get("port"),
        )
        if _has_usable_agent_ssh_endpoint(current_agent_ssh)
        else ("", None)
    )
    current_agent_updated_at = _display_value(
        (current_state or {}).get("agentUpdatedAt")
        if isinstance(current_state, dict)
        else "",
        default="",
    )
    if (
        not force_reopen
        and not api_force_reopen_blocked
        and _has_usable_agent_ssh_endpoint(current_agent_ssh)
    ):
        return {
            "opened": None,
            "device": current_state,
            "pollCount": 0,
            "ready": True,
            "reusedExisting": True,
        }

    if api_force_reopen_blocked or (
        _is_company_api_device_ssh_context()
        and _company_api_device_ssh_open_attempted(device_name)
    ):
        # 같은 API turn의 stale refresh나 두 번째 target은 상태만 반환하고
        # 기존 tunnel을 끊거나 두 번째 sshOrder를 보내지 않는다.
        return {
            "opened": None,
            "device": current_state,
            "pollCount": 0,
            "ready": False,
            "reusedExisting": False,
        }

    host_to_use = (
        _display_value(current_agent_ssh.get("host"), default="")
        if _has_usable_agent_ssh_endpoint(current_agent_ssh)
        else ""
    ) or (host or cs.MDA_SSH_OPEN_HOST).strip()
    # 캐시된 endpoint의 실제 handshake가 실패한 호출자는 강제 재개방을 요청한다.
    # 이 경우 기존 host/port가 남아 있어도 sshOrder를 다시 보내고 poll 결과를 쓴다.
    open_result = _open_mda_device_ssh(device_name, host=host_to_use)

    deadline = time.monotonic() + actual_poll_timeout
    poll_count = 0
    last_state = current_state
    while time.monotonic() < deadline:
        time.sleep(actual_poll_interval)
        poll_count += 1
        last_state = _get_mda_device_agent_ssh(device_name)
        agent_ssh = ((last_state or {}).get("agentSsh") or {}) if isinstance(last_state, dict) else {}
        polled_endpoint = (
            _display_value(agent_ssh.get("host"), default=""),
            agent_ssh.get("port"),
        )
        polled_agent_updated_at = _display_value(
            last_state.get("agentUpdatedAt")
            if isinstance(last_state, dict)
            else "",
            default="",
        )
        has_ready_endpoint = _has_usable_agent_ssh_endpoint(agent_ssh)
        endpoint_refreshed = (
            not force_reopen
            or not all(current_endpoint)
            or polled_endpoint != current_endpoint
            or (
                bool(polled_agent_updated_at)
                and polled_agent_updated_at != current_agent_updated_at
            )
        )
        if has_ready_endpoint and endpoint_refreshed:
            return {
                "opened": open_result,
                "device": last_state,
                "pollCount": poll_count,
                "ready": True,
                "reusedExisting": False,
            }

        # 강제 재개방은 기존 reverse SSH를 끊을 수 있어 한 번만 보낸다.
        # 일반 최초 개방에서만 기존 주기 재전송 동작을 유지한다.
        if (
            resend_enabled
            and not force_reopen
            and poll_count % actual_resend_every == 0
        ):
            open_result = _open_mda_device_ssh(device_name, host=host_to_use)

    return {
        "opened": open_result,
        "device": last_state,
        "pollCount": poll_count,
        "ready": False,
        "reusedExisting": False,
    }
