from typing import Any, Callable

from boxer.core.utils import _display_value


# 장비에 전달할 수 있는 값은 이 네 개로 고정한다. Slack 문구나 LLM 출력으로
# command/acme를 조립하지 않아 임의 장비 명령 실행 경로가 되지 않게 한다.
_DEVICE_VOICE_COMMANDS = {
    "귀여운 음성": "S_VOICE1",
    "진지한 음성": "S_VOICE2",
    "기존 귀여운 음성": "S_VOICE_LEGACY_1",
    "기존 진지한 음성": "S_VOICE_LEGACY_2",
}
_DEVICE_VOICE_CHANGE_HINTS = (
    "바꿔",
    "바꾸",
    "변경해",
    "변경하",
    "설정해",
    "설정하",
    "적용해",
    "적용하",
    "전환해",
    "전환하",
)


def _device_voice_labels() -> tuple[str, ...]:
    return tuple(_DEVICE_VOICE_COMMANDS)


def _extract_device_voice_label(question: str) -> str | None:
    text = " ".join(str(question or "").split())
    # "기존 귀여운 음성"이 "귀여운 음성"보다 먼저 매칭되도록 긴 이름부터 본다.
    for label in sorted(_DEVICE_VOICE_COMMANDS, key=len, reverse=True):
        if label in text:
            return label
    return None


def _is_device_voice_change_request(question: str) -> bool:
    text = " ".join(str(question or "").split())
    return "음성" in text and any(hint in text for hint in _DEVICE_VOICE_CHANGE_HINTS)


def _build_device_voice_choices_message() -> str:
    choices = " / ".join(f"`{label}`" for label in _device_voice_labels())
    return f"바꿀 음성을 정확히 골라줘.\n• 음성 목록: {choices}"


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
