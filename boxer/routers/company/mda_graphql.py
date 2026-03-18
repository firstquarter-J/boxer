import json
import time
from typing import Any
from urllib import error, request

from boxer.company import settings as cs
from boxer.core.utils import _display_value

_mda_access_token_cache: str | None = None

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
      deviceState {
        captureBoardType
      }
      hospital {
        hospitalName
      }
      hospitalRoom {
        roomName
      }
      agentState {
        isConnected
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
) -> dict[str, Any]:
    token = _get_mda_access_token()
    try:
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
        return _execute_mda_graphql_request(
            query,
            variables,
            timeout_sec=timeout_sec,
            auth_token=token,
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
        "captureBoardType": _normalize_mda_state_text(device_state.get("captureBoardType")),
        "hospitalName": _display_value(hospital.get("hospitalName"), default="미확인"),
        "roomName": _display_value(hospital_room.get("roomName"), default="미확인"),
        "isConnected": bool(agent_state.get("isConnected")),
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


def _get_mda_device_versions(device_names: list[str]) -> dict[str, str]:
    versions: dict[str, str] = {}
    for normalized_name, detail in _get_mda_devices_details(device_names).items():
        version = str((detail or {}).get("version") or "").strip()
        if version:
            versions[normalized_name] = version
    return versions


def _open_mda_device_ssh(
    device_name: str,
    *,
    host: str | None = None,
) -> dict[str, Any]:
    actual_host = (host or cs.MDA_SSH_OPEN_HOST).strip()
    if not actual_host:
        raise RuntimeError("MDA_SSH_OPEN_HOST가 비어 있어")

    data = _execute_mda_graphql(
        _SSH_ORDER_MUTATION,
        {
            "deviceName": device_name,
            "action": "open",
            "host": actual_host,
        },
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


def _create_mda_activity_log(input_payload: dict[str, Any]) -> dict[str, Any]:
    normalized_input = {
        key: value
        for key, value in (input_payload or {}).items()
        if value is not None and value != ""
    }
    if not normalized_input:
        raise RuntimeError("activity log 입력이 비어 있어")

    data = _execute_mda_graphql(
        _CREATE_ACTIVITY_LOG_MUTATION,
        {
            "input": normalized_input,
        },
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


def _wait_for_mda_device_agent_ssh(
    device_name: str,
    *,
    host: str | None = None,
    poll_timeout_sec: int | None = None,
    poll_interval_sec: int | None = None,
    resend_every: int | None = None,
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
    if current_agent_ssh.get("host") and current_agent_ssh.get("port"):
        return {
            "opened": None,
            "device": current_state,
            "pollCount": 0,
            "ready": True,
            "reusedExisting": True,
        }

    host_to_use = (
        _display_value(((current_state or {}).get("agentSsh") or {}).get("host"), default="")
        or (host or cs.MDA_SSH_OPEN_HOST).strip()
    )
    open_result = _open_mda_device_ssh(device_name, host=host_to_use)

    deadline = time.monotonic() + actual_poll_timeout
    poll_count = 0
    last_state = current_state
    while time.monotonic() < deadline:
        time.sleep(actual_poll_interval)
        poll_count += 1
        last_state = _get_mda_device_agent_ssh(device_name)
        agent_ssh = ((last_state or {}).get("agentSsh") or {}) if isinstance(last_state, dict) else {}
        if agent_ssh.get("host") and agent_ssh.get("port"):
            return {
                "opened": open_result,
                "device": last_state,
                "pollCount": poll_count,
                "ready": True,
                "reusedExisting": False,
            }

        if poll_count % actual_resend_every == 0:
            open_result = _open_mda_device_ssh(device_name, host=host_to_use)

    return {
        "opened": open_result,
        "device": last_state,
        "pollCount": poll_count,
        "ready": False,
        "reusedExisting": False,
    }
