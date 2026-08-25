from __future__ import annotations

from collections.abc import Mapping
import re
from typing import Any


LED_USB_SIGNATURES = {
    (0x1A86, 0x7523): "MmtLEDv3",
    (0x0403, 0x6001): "MmtLED-FTDI",
}
_LED_USB_DEVICE_IDS = tuple(
    f"{vendor_id:04x}:{product_id:04x}"
    for vendor_id, product_id in LED_USB_SIGNATURES
)
_LED_USB_ID_PATTERN = re.compile(
    rf"(?<![0-9a-f])(?:{'|'.join(map(re.escape, _LED_USB_DEVICE_IDS))})(?![0-9a-f])",
    re.IGNORECASE,
)
_LED_USB_NAME_PATTERN = re.compile(
    r"(?:mmtled|마미톡\s*led|\bled(?:v\d+)?\b)",
    re.IGNORECASE,
)
_USB_ID_KEYS = ("ID", "id", "deviceId", "deviceID", "vendorProductId")
_USB_TEXT_KEYS = ("Name", "name", "alias", "type")


def redis_device_led_usb_presence(
    device_state: Mapping[str, Any] | None,
) -> bool | None:
    """Redis usbList가 명시한 LED 연결 여부를 반환한다.

    ``None``은 usbList 자체가 없어 판정할 수 없다는 뜻이고, 명시적인 빈
    목록은 LED가 없는 상태로 본다.
    """

    if not isinstance(device_state, Mapping):
        return None
    acme = device_state.get("acme")
    if not isinstance(acme, Mapping) or "usbList" not in acme:
        return None
    usb_items = acme.get("usbList")
    if not isinstance(usb_items, list):
        return None

    # MommyBox 원본의 대문자 ID/Name과 MDA가 Redis에 저장하는 소문자
    # 정규화 payload를 함께 받아 local/remote rollback 경계가 갈리지 않게 한다.
    return any(
        _redis_usb_item_is_led(item)
        for item in usb_items
        if isinstance(item, Mapping)
    )


def _redis_usb_item_is_led(item: Mapping[str, Any]) -> bool:
    id_text = " ".join(_text(item.get(key)) for key in _USB_ID_KEYS)
    if _LED_USB_ID_PATTERN.search(id_text):
        return True
    name_text = " ".join(_text(item.get(key)) for key in _USB_TEXT_KEYS)
    return bool(_LED_USB_NAME_PATTERN.search(name_text))


def _text(value: Any) -> str:
    return str(value or "").strip()
