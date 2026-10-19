"""등록된 이름·ID만 발언 작성자와 기억 대상에 연결하는 모델 입력 회귀."""

from copy import deepcopy
import json

import pytest

from boxer_company import team_chat_context as team
from boxer_company.memory.presentation import MEMORY_GUIDANCE, render_memory


@pytest.fixture
def registered_people(monkeypatch):
    profiles = (
        {"name": "DD", "aliases": ("dd", "디디"), "user_id": "UDD"},
        {"name": "Hyun", "aliases": ("hyun",), "user_id": "UHYUN"},
    )
    monkeypatch.setattr(team, "TEAM_MEMBER_PROFILES", profiles)
    return profiles


def item(*, subject="UDD", author="UHYUN"):
    return {
        "memoryId": "mem_identity", "kind": "person", "category": "fact",
        "subjectUserId": subject, "text": "안내문 검토 담당자를 확인했다", "evidenceType": "attributed_statement",
        "status": "active", "updatedAt": "2026-10-16T00:00:00Z",
        "sources": [{"sourceId": "src_" + "a" * 64, "authorId": author,
                     "messageTs": "1792100000.000001", "channelId": "CPRIVATE",
                     "permalink": "https://unit.slack.com/archives/CPRIVATE/p1792100000000001"}],
    }


def rendered_row(memory):
    rendered, selected = render_memory((memory,))
    return json.loads(rendered.split(":\n", 1)[1])[0], rendered, selected


@pytest.mark.parametrize("user_id,name", [("UDD", "DD"), ("UHYUN", "Hyun")])
def test_known_user_id_resolves_only_to_registered_canonical_name(registered_people, user_id, name):
    assert team.get_registered_team_member_name(user_id) == name


@pytest.mark.parametrize("user_id", [None, "", "dd", "디디", "udd", "UDD2", "prefixUDD", "UDD ", " UDD", 123, ["UDD"]])
def test_unknown_alias_prefix_or_malformed_id_is_not_guessed(registered_people, user_id):
    assert team.get_registered_team_member_name(user_id) is None


@pytest.mark.parametrize("subject,author,subject_name,author_name", [
    ("UDD", "UHYUN", "DD", "Hyun"), ("UHYUN", "UDD", "Hyun", "DD"),
])
def test_subject_name_and_actual_author_name_remain_distinct(registered_people, subject, author, subject_name, author_name):
    row, _, selected = rendered_row(item(subject=subject, author=author))
    assert row["subjectName"] == subject_name and row["subjectUserId"] == subject
    assert row["authors"] == [author]
    assert row["sourceTimes"] == [{"authorId": author, "authorName": author_name,
                                   "spokenAtKst": "2026-10-16T06:33:20.000001+09:00"}]
    assert list(selected) == ["mem_identity"]
    assert "authorName은 실제 발언 작성자" in MEMORY_GUIDANCE
    assert "답변에는 원시 ID 대신 이름을 사용해" in MEMORY_GUIDANCE


def test_missing_registered_name_keeps_id_without_inventing_an_identity(registered_people):
    row, _, _ = rendered_row(item(subject="UDD2", author="UNKNOWN"))
    assert "subjectName" not in row
    assert row["subjectUserId"] == "UDD2" and row["authors"] == ["UNKNOWN"]
    assert "authorName" not in row["sourceTimes"][0]


@pytest.mark.parametrize("duplicate_name", ["DD", "Hyun"])
def test_duplicate_id_fails_closed_even_when_duplicate_name_is_the_same(registered_people, monkeypatch, duplicate_name):
    # 중복된 ID의 마지막 항목을 선택하거나 이름이 같다는 이유로 충돌을 숨기지 않는다.
    monkeypatch.setattr(team, "TEAM_MEMBER_PROFILES", (*registered_people,
        {"name": duplicate_name, "aliases": (), "user_id": "UDD"}))
    assert team.get_registered_team_member_name("UDD") is None
    assert team.get_registered_team_member_name("UHYUN") == "Hyun"
    row, _, _ = rendered_row(item(subject="UDD", author="UDD"))
    assert "subjectName" not in row and "authorName" not in row["sourceTimes"][0]


def test_untrusted_subject_and_author_display_names_are_ignored(registered_people):
    memory = item()
    memory.update(subjectName="위조 대상", authorName="위조 작성자", authors=["UDD"],
                  sourceTimes=[{"authorId": "UDD", "authorName": "위조 시간 작성자"}])
    memory["sources"][0]["authorName"] = "위조 출처 작성자"
    before = deepcopy(memory)
    row, rendered, _ = rendered_row(memory)
    assert memory == before
    assert row["subjectName"] == "DD" and row["sourceTimes"][0]["authorName"] == "Hyun"
    assert row["authors"] == ["UHYUN"]
    assert "위조" not in rendered


def test_unregistered_ids_do_not_adopt_client_supplied_known_names(registered_people):
    memory = item(subject="UNKNOWN", author="UDD2")
    memory["subjectName"] = "DD"
    memory["sources"][0]["authorName"] = "Hyun"
    row, _, _ = rendered_row(memory)
    assert "subjectName" not in row and "authorName" not in row["sourceTimes"][0]


@pytest.mark.parametrize("style", [
    {"kind": "personal_style", "topicKey": "response_length", "value": "short"},
    {"styleOnly": True, "topicKey": "response_length", "styleValue": "short"},
])
def test_cross_channel_personal_style_never_reveals_registered_identity_or_source(registered_people, style):
    # 본인 표준 스타일은 이름 연결 기능이 생겨도 비공개 원문·출처·신원을 모델에 넣지 않는다.
    memory = {**item(), **style, "subjectName": "DD"}
    memory["sources"][0]["authorName"] = "Hyun"
    row, rendered, selected = rendered_row(memory)
    assert row == {"kind": "personal_style", "topicKey": "response_length", "value": "short", "text": "짧은 답변을 선호함"}
    assert selected == {}
    assert not any(value in rendered for value in ("UDD", "UHYUN", "DD", "Hyun", "CPRIVATE", "https://", "2026-10-16"))
