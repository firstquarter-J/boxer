import unittest
from unittest.mock import patch

from boxer_company import team_chat_context as tcc


class TeamChatContextTests(unittest.TestCase):
    def setUp(self) -> None:
        self._original_profile_by_user_id = dict(tcc._PROFILE_BY_USER_ID)

    def tearDown(self) -> None:
        tcc._PROFILE_BY_USER_ID.clear()
        tcc._PROFILE_BY_USER_ID.update(self._original_profile_by_user_id)

    def test_includes_speaker_profile_when_user_id_matches(self) -> None:
        tcc._PROFILE_BY_USER_ID["u_mark"] = tcc._PROFILE_BY_NAME["Mark"]

        context = tcc.build_team_chat_context(
            "배포 얘기하자",
            speaker_user_id="U_MARK",
        )

        self.assertIn("현재 말하는 사람:", context)
        self.assertIn("Mark: 판 설계형 / 공격적 낙관주의자", context)
        self.assertIn("MBTI ISFJ", context)
        self.assertIn("전투력 96, 메인 딜러 / 전장 장악형", context)

    def test_matches_profile_by_raw_user_id_in_thread_context(self) -> None:
        tcc._PROFILE_BY_USER_ID["u_roy"] = tcc._PROFILE_BY_NAME["Roy"]

        context = tcc.build_team_chat_context(
            "U_ROY: H룸 데스크톱 가져다 쓰면",
        )

        self.assertIn("Roy: 현실주의자 / 인프라형 사고", context)
        self.assertIn("전투력 77, 서포터 / 판 증폭기", context)

    def test_required_name_is_added_even_without_text_match(self) -> None:
        context = tcc.build_team_chat_context(
            "모대?",
            required_names=("DD",),
        )

        self.assertIn("DD: 감정 직결형 / 반응형 인간", context)
        self.assertIn("전투력 91, 메인 탱커 / 생존형 카운터", context)
        self.assertIn("MBTI ENFP", context)

    def test_dd_spellings_select_the_same_profile(self) -> None:
        for name in ("DD 는", "dd 는", "DD는", "dd는", "디디는", "디디", "DD님은", "디디에 대해"):
            with self.subTest(name=name):
                context = tcc.build_team_chat_context(f"{name} 어떤 사람이야?")
                self.assertIn("DD: 감정 직결형 / 반응형 인간", context)

    def test_words_containing_aliases_do_not_add_unrelated_profiles(self) -> None:
        context = tcc.build_team_chat_context("address message markdown 폴더 DDos")

        for name in ("DD", "Sage", "Mark", "Paul"):
            with self.subTest(name=name):
                self.assertNotIn(f"{name}:", context)

    def test_raw_user_id_prefix_does_not_select_another_member(self) -> None:
        tcc._PROFILE_BY_USER_ID["u_roy"] = tcc._PROFILE_BY_NAME["Roy"]

        context = tcc.build_team_chat_context("U_ROY2: 장비 얘기하자")

        self.assertNotIn("Roy:", context)

    def test_thread_user_ids_preserve_existing_profile_selection_order(self) -> None:
        for name in ("Roy", "Maru", "Paul"):
            tcc._PROFILE_BY_USER_ID[f"u_{name.lower()}"] = tcc._PROFILE_BY_NAME[name]

        # 제한된 문맥에서 thread 작성자 ID가 질문의 대상 인물을 밀어내지 않는다.
        context = tcc.build_team_chat_context("DD는 어때?", "U_ROY: 응 U_MARU: 응 U_PAUL: 응", limit=3)

        self.assertIn("DD:", context)
        self.assertIn("Roy:", context)
        self.assertIn("Maru:", context)
        self.assertNotIn("Paul:", context)

    def test_speaker_not_duplicated_in_related_profiles(self) -> None:
        tcc._PROFILE_BY_USER_ID["u_dd"] = tcc._PROFILE_BY_NAME["DD"]

        context = tcc.build_team_chat_context(
            "DD가 또 모댔네",
            speaker_user_id="U_DD",
            required_names=("DD",),
        )

        self.assertEqual(context.count("DD: 감정 직결형 / 반응형 인간"), 1)

    def test_freeform_context_includes_mbti_and_guardrail(self) -> None:
        tcc._PROFILE_BY_USER_ID["u_hyun"] = tcc._PROFILE_BY_NAME["Hyun"]

        context = tcc.build_team_freeform_context(
            "올리비아가 운영 얘기하네",
            speaker_user_id="U_HYUN",
        )

        self.assertIn("MBTI는 답변 개인화를 위한 보조 힌트", context)
        self.assertIn("- MBTI: ENTJ", context)
        self.assertIn("- MBTI: ISTJ", context)

    def test_selected_profiles_include_registered_name_id_and_alias_identity(self) -> None:
        # 모델이 실제 발언의 ID를 화자 Hyun·질문 대상 DD와 직접 연결할 수 있어야 한다.
        hyun = {**tcc._PROFILE_BY_NAME["Hyun"], "user_id": "UHYUN"}
        dd = {**tcc._PROFILE_BY_NAME["DD"], "user_id": "UDD"}
        with patch.object(tcc, "TEAM_MEMBER_PROFILES", (hyun, dd)), \
                patch.dict(tcc._PROFILE_BY_NAME, {"Hyun": hyun, "DD": dd}, clear=True), \
                patch.dict(tcc._PROFILE_BY_USER_ID, {"uhyun": hyun, "udd": dd}, clear=True):
            for builder in (tcc.build_team_chat_context, tcc.build_team_freeform_context):
                with self.subTest(builder=builder.__name__):
                    context = builder("디디가 오늘 한 말 요약", speaker_user_id="UHYUN")
                    self.assertIn("Slack 식별 정보: Hyun = UHYUN", context)
                    self.assertIn("Slack 식별 정보: DD = UDD (등록 별칭: dd, 디디)", context)
                    self.assertIn("사용자 ID 대신 등록된 이름을 사용해", context)

    def test_duplicate_registered_user_id_is_not_rendered_as_verified_identity(self) -> None:
        dd = {**tcc._PROFILE_BY_NAME["DD"], "user_id": "UDUPLICATE"}
        hyun = {**tcc._PROFILE_BY_NAME["Hyun"], "user_id": "UDUPLICATE"}
        with patch.object(tcc, "TEAM_MEMBER_PROFILES", (dd, hyun)), \
                patch.dict(tcc._PROFILE_BY_NAME, {"DD": dd, "Hyun": hyun}, clear=True):
            for builder in (tcc.build_team_chat_context, tcc.build_team_freeform_context):
                with self.subTest(builder=builder.__name__):
                    context = builder("디디가 오늘 한 말 요약")
                    self.assertNotIn("DD = UDUPLICATE", context)
                    self.assertNotIn("Hyun = UDUPLICATE", context)

    def test_factual_identity_context_excludes_speaker_and_target_personality(self) -> None:
        # 사실 조회에서는 질문자와 대상 모두 이름 연결만 남기고 캐릭터 편향을 제거한다.
        hyun = {**tcc._PROFILE_BY_NAME["Hyun"], "user_id": "UHYUN"}
        maru = {**tcc._PROFILE_BY_NAME["Maru"], "user_id": "UMARU"}
        with patch.object(tcc, "TEAM_MEMBER_PROFILES", (hyun, maru)), \
                patch.dict(tcc._PROFILE_BY_NAME, {"Hyun": hyun, "Maru": maru}, clear=True), \
                patch.dict(tcc._PROFILE_BY_USER_ID, {"uhyun": hyun, "umaru": maru}, clear=True):
            context = tcc.build_team_identity_context("오늘 마루가 한 말 요약해줘", speaker_user_id="UHYUN")

        self.assertIn("Hyun = UHYUN", context)
        self.assertIn("Maru = UMARU", context)
        for unsupported in ("MBTI", "완충재", "화자 지침", "반응 지침", "전투력", "fictional framing", "차분한 광기"):
            with self.subTest(unsupported=unsupported):
                self.assertNotIn(unsupported, context)

    def test_factual_identity_context_rejects_ambiguous_registration(self) -> None:
        duplicate = tuple({**tcc._PROFILE_BY_NAME[name], "user_id": "UDUPLICATE"} for name in ("Hyun", "Maru"))
        with patch.object(tcc, "TEAM_MEMBER_PROFILES", duplicate), \
                patch.dict(tcc._PROFILE_BY_NAME, {str(profile["name"]): profile for profile in duplicate}, clear=True):
            context = tcc.build_team_identity_context("현이랑 마루가 한 말 요약")

        self.assertEqual(context, "")

    def test_required_sage_profile_includes_mbti(self) -> None:
        context = tcc.build_team_chat_context(
            "세이지 얘기하자",
            required_names=("Sage",),
        )

        self.assertIn("Sage: 클라이언트 실무형 / 정보 보강 필요", context)
        self.assertIn("MBTI INTJ", context)


if __name__ == "__main__":
    unittest.main()
