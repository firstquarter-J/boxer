"""팀원 이름 표기는 넓게 인식하고 다른 단어에 포함된 이름은 제외한다."""

import pytest

from boxer_company._operation_routing_knowledge import TEAM_MEMBER_ALIASES_BY_NAME
from boxer_company.team_member_aliases import matches_team_member_alias


@pytest.mark.parametrize("name", [
    "DD", "dd", "dD", "DD는", "dd는", "디디는", "디디",
    "DD가", "디디의", "DD를", "dd에게", "디디랑", "DD도", "디디만",
    "DD보다", "디디처럼", "DD부터", "디디까지", "DD님은", "디디씨가",
    "DD에 대해", "디디에게만", "DD에게서는", "디디한테만", "DD하고는",
    "(DD)", '"디디"', "DD:", "dd,", "\n디디\n", "ＤＤ는",
])
def test_dd_variants_identify_the_same_member(name):
    assert matches_team_member_alias(f"{name} 어떤 사람이야?", ("dd", "디디"))


@pytest.mark.parametrize("text", [
    "address", "add", "DDos", "HDD", "xDD", "DD1", "DD_helper",
    "DD는가짜", "디디다른이름", "D D", "디 디", "앞디디",
])
def test_embedded_or_unregistered_dd_spellings_are_not_members(text):
    assert not matches_team_member_alias(text, ("dd", "디디"))


@pytest.mark.parametrize("name,aliases", TEAM_MEMBER_ALIASES_BY_NAME.items())
def test_registered_aliases_and_particles_apply_to_every_member(name, aliases):
    # 한 사람 전용 예외가 아니라 전체 등록 명칭에 같은 경계를 적용한다.
    for alias in (name, *aliases):
        assert matches_team_member_alias(f"{alias}는 어떤 사람이야?", aliases)


@pytest.mark.parametrize("text,name", [
    ("message", "Sage"), ("usage", "Sage"), ("SageMaker", "Sage"),
    ("destroy", "Roy"), ("markdown", "Mark"), ("bookmark", "Mark"),
    ("폴더", "Paul"), ("주노트", "Juno"),
])
def test_other_member_names_do_not_match_inside_words(text, name):
    assert not matches_team_member_alias(text, TEAM_MEMBER_ALIASES_BY_NAME[name])


def test_empty_aliases_do_not_match_every_question():
    assert not matches_team_member_alias("어떤 사람이야?", ("", "  "))
