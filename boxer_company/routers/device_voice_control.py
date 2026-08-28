from typing import Any, Callable

from boxer_company.utils import _display_value
from boxer_company import settings as cs


# 장비에 전달할 수 있는 값은 이 네 개로 고정한다. Slack 문구나 LLM 출력으로
# command/acme를 조립하지 않아 임의 장비 명령 실행 경로가 되지 않게 한다.
_DEVICE_VOICE_COMMANDS = {
    "귀여운 음성": "S_VOICE1",
    "진지한 음성": "S_VOICE2",
    "기존 귀여운 음성": "S_VOICE_LEGACY_1",
    "기존 진지한 음성": "S_VOICE_LEGACY_2",
}
_DEVICE_VOICE_TYPES = {
    "귀여운 음성": "n",
    "진지한 음성": "s",
    "기존 귀여운 음성": "ln",
    "기존 진지한 음성": "ls",
}


def _device_voice_labels() -> tuple[str, ...]:
    return tuple(_DEVICE_VOICE_COMMANDS)


def _extract_device_voice_label(question: str) -> str | None:
    text = " ".join(str(question or "").split())
    # "기존 귀여운 음성"이 "귀여운 음성"보다 먼저 매칭되도록 긴 이름부터 본다.
    for label in sorted(_DEVICE_VOICE_COMMANDS, key=len, reverse=True):
        if label in text:
            return label
    return None


def _build_device_voice_catalog_message() -> str:
    # 운영자가 MDA 입력과 장비 수신값을 바로 대조할 수 있게 내부 설정값까지 함께 보여준다.
    lines = [
        "*장비 음성 세트 목록*",
        "• MDA 전송 형식: `command=scansim`, `acme=<스캔값>`",
        "• 장비 수신 형식: `cmd=scansim`, `acme=<스캔값>`",
    ]
    for label in _device_voice_labels():
        lines.append(
            f"• *{label}*: 설정값 `{_DEVICE_VOICE_TYPES[label]}` | "
            f"스캔값 `{_DEVICE_VOICE_COMMANDS[label]}`"
        )
    lines.extend(
        (
            "• 예: 귀여운 음성 적용 → `command=scansim`, `acme=S_VOICE1`",
            "• Boxer 사용 예: `MB2-C00419 귀여운 음성으로 바꿔줘`",
        )
    )
    return "\n".join(lines)


def _build_device_voice_choices_message() -> str:
    return f"바꿀 음성을 정확히 골라줘.\n{_build_device_voice_catalog_message()}"


def _build_device_voice_device_required_message(voice_label: str) -> str:
    return (
        "음성을 바꿀 장비명이 필요해.\n"
        f"• 예: `MB2-C00419 {voice_label}으로 바꿔줘`"
    )


def _build_device_voice_config_message() -> str:
    return "장비 음성 변경을 위해 MDA_GRAPHQL_URL과 MDA_ADMIN_USER_PASSWORD 설정이 필요해"


def _change_device_voice(
    device_name: str,
    voice_label: str,
    *,
    command_dispatcher: Callable[..., dict[str, Any]],
) -> tuple[str, dict[str, Any]]:
    normalized_device_name = str(device_name or "").strip()
    if not normalized_device_name:
        raise ValueError("장비명이 없어")

    command_value = _DEVICE_VOICE_COMMANDS.get(str(voice_label or "").strip())
    if not command_value:
        raise ValueError("지원하지 않는 음성이야")

    dispatch = command_dispatcher(
        normalized_device_name,
        command="scansim",
        acme=command_value,
    )
    status = bool(dispatch.get("status"))
    result_label = "명령 전송 완료" if status else "명령 전송 실패"
    lines = [
        "*장비 음성 변경*",
        f"• 장비: `{normalized_device_name}`",
        f"• 음성: *{voice_label}*",
        f"• 결과: *{result_label}*",
    ]
    message = _display_value(dispatch.get("message"), default="")
    if message:
        lines.append(f"• MDA 응답: `{message}`")
    if status:
        lines.append("• 확인: 장비에서 준비 음성이 재생되면 적용 완료야")

    return "\n".join(lines), {
        "route": "device_voice_change",
        "deviceName": normalized_device_name,
        "voiceLabel": voice_label,
        "deviceCommand": command_value,
        "dispatch": dispatch,
    }


def _dispatch_device_voice_guide(
    device_name: str,
    *,
    command_dispatcher: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    normalized_device_name = str(device_name or "").strip()
    if (
        not normalized_device_name
        or not cs.S3_DEVICE_NAME_PATTERN.fullmatch(normalized_device_name)
    ):
        raise ValueError("장비명이 올바르지 않아")

    # Slack action payload에서 임의 command/acme를 주입할 수 없도록 현장 안내
    # 명령을 회사 도메인 경계에서 고정한다.
    return command_dispatcher(
        normalized_device_name,
        command="voice_guide",
    )
