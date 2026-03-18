import unittest

from boxer.company import team_chat_context as tcc


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

    def test_speaker_not_duplicated_in_related_profiles(self) -> None:
        tcc._PROFILE_BY_USER_ID["u_dd"] = tcc._PROFILE_BY_NAME["DD"]

        context = tcc.build_team_chat_context(
            "DD가 또 모댔네",
            speaker_user_id="U_DD",
            required_names=("DD",),
        )

        self.assertEqual(context.count("DD: 감정 직결형 / 반응형 인간"), 1)


if __name__ == "__main__":
    unittest.main()
