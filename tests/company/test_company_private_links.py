from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from boxer_company.assistant import (
    AssistantLink,
    AssistantMessage,
    CompanyAssistantResult,
)
from boxer_company_adapter_slack.assistant_bridge import (
    render_company_assistant_result,
)
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiContractError,
    _deserialize_message,
)
from boxer_company_api.schemas import serialize_result


def _long_presigned_url() -> str:
    return (
        "https://download.example/a.mp4?X-Amz-Credential=requester"
        "&X-Amz-Security-Token=" + "a" * 3_500
    )


def test_api_serializes_links_only_for_requester_message() -> None:
    uri = _long_presigned_url()
    link = AssistantLink(label="a.mp4", uri=uri)
    payload = serialize_result(
        CompanyAssistantResult(
            route="device_file_download",
            outcome="answered",
            messages=(
                AssistantMessage(body="공개 안내", private_links=(link,)),
                AssistantMessage(
                    body="개인 다운로드 결과",
                    delivery_scope="requester",
                    private_links=(
                        link,
                        AssistantLink(
                            label="unsafe",
                            uri="http://download.example/unsafe",
                        ),
                    ),
                ),
            ),
        ),
        "REQ-PRIVATE-LINK-1",
    )

    assert "privateLinks" not in payload["messages"][0]
    assert payload["messages"][1]["privateLinks"] == [
        {"label": "a.mp4", "uri": uri}
    ]


def test_api_removes_private_uri_from_requester_body_and_code_block() -> None:
    uri = _long_presigned_url()
    payload = serialize_result(
        CompanyAssistantResult(
            route="device_file_download",
            outcome="answered",
            messages=(
                AssistantMessage(
                    body=f"개인 결과\n```\n{uri}\n```",
                    delivery_scope="requester",
                    private_links=(AssistantLink(label="a.mp4", uri=uri),),
                ),
            ),
        ),
        "REQ-PRIVATE-LINK-BODY",
    )

    assert uri not in payload["messages"][0]["body"]
    assert "[비공개 링크 생략]" in payload["messages"][0]["body"]
    assert payload["messages"][0]["privateLinks"][0]["uri"] == uri


def test_api_removes_private_uri_from_another_public_message() -> None:
    uri = _long_presigned_url()
    payload = serialize_result(
        CompanyAssistantResult(
            route="device_file_download",
            outcome="answered",
            messages=(
                AssistantMessage(body=f"공개 결과\n```\n{uri}\n```"),
                AssistantMessage(
                    body="개인 다운로드 결과",
                    delivery_scope="requester",
                    private_links=(AssistantLink(label="a.mp4", uri=uri),),
                ),
            ),
        ),
        "REQ-PRIVATE-LINK-CROSS-MESSAGE",
    )

    assert uri not in payload["messages"][0]["body"]
    assert "[비공개 링크 생략]" in payload["messages"][0]["body"]
    assert payload["messages"][1]["privateLinks"][0]["uri"] == uri


def test_client_accepts_additive_private_link_contract_and_fails_closed() -> None:
    uri = _long_presigned_url()
    message = _deserialize_message(
        {
            "body": "개인 다운로드 결과",
            "deliveryScope": "requester",
            "mentionActor": False,
            "format": "commonmark",
            "privateLinks": [{"label": "a.mp4", "uri": uri}],
        },
        "REQ-PRIVATE-LINK-2",
    )

    assert message.private_links == (AssistantLink(label="a.mp4", uri=uri),)
    with pytest.raises(CompanyApiContractError):
        _deserialize_message(
            {
                "body": "공개 결과",
                "deliveryScope": "conversation",
                "mentionActor": True,
                "format": "commonmark",
                "privateLinks": [{"label": "a.mp4", "uri": uri}],
            },
            "REQ-PRIVATE-LINK-2",
        )
    with pytest.raises(CompanyApiContractError):
        _deserialize_message(
            {
                "body": f"개인 결과\n```\n{uri}\n```",
                "deliveryScope": "requester",
                "mentionActor": False,
                "format": "commonmark",
                "privateLinks": [{"label": "a.mp4", "uri": uri}],
            },
            "REQ-PRIVATE-LINK-2",
        )
    with pytest.raises(CompanyApiContractError):
        _deserialize_message(
            {
                "body": "개인 결과",
                "deliveryScope": "requester",
                "mentionActor": False,
                "format": "commonmark",
                "privateLinks": [
                    {
                        "label": "unsafe",
                        "uri": "http://download.example/unsafe",
                    }
                ],
            },
            "REQ-PRIVATE-LINK-2",
        )


def test_slack_bridge_renders_private_link_in_dm_only() -> None:
    uri = _long_presigned_url()
    public_replies: list[str] = []
    dm_calls: list[dict[str, object]] = []
    client = SimpleNamespace(
        conversations_open=lambda **_kwargs: {"channel": {"id": "D1"}},
        chat_postMessage=lambda **kwargs: dm_calls.append(kwargs),
    )
    result = CompanyAssistantResult(
        route="device_file_download",
        outcome="answered",
        messages=(
            AssistantMessage(body="다운로드 링크는 요청자 DM으로 보냈어"),
            AssistantMessage(
                body="개인 다운로드 결과",
                delivery_scope="requester",
                mention_actor=False,
                private_links=(
                    AssistantLink(label="a.mp4", uri=uri),
                    AssistantLink(
                        label="unsafe",
                        uri="http://download.example/unsafe",
                    ),
                ),
            ),
        ),
    )

    sent = render_company_assistant_result(
        result,
        reply=lambda text, **_kwargs: public_replies.append(text),
        actor_id="U1",
        client=client,
        logger=logging.getLogger(__name__),
    )

    assert sent == 3
    assert len(public_replies) == 1
    assert uri not in public_replies[0]
    assert [call["channel"] for call in dm_calls] == ["D1", "D1"]
    assert dm_calls[0]["text"] == "개인 다운로드 결과"
    assert uri in str(dm_calls[1]["text"])
    assert "unsafe" not in str(dm_calls)
