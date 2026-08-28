import logging
import re
import unittest
from unittest.mock import Mock, patch

from boxer_company.base_access import (
    BaseAccessMember,
    BaseAccessMutationResult,
    StoreUnavailable,
)
from boxer_company_adapter_slack import access_routes


_HYUN_USER_ID = "U0629HDSJHG"
_TARGET_USER_ID = "U037PL53L76"
_BOXER_USER_ID = "U08MWNAD5TM"
_WORKSPACE_ID = "T_LIFEX"


def _member(*, allowed: bool = True) -> BaseAccessMember:
    return BaseAccessMember(
        workspace_id=_WORKSPACE_ID,
        user_id=_TARGET_USER_ID,
        display_name="Zion",
        allowed=allowed,
        ordering_key="00000000001784800000.000002",
        updated_at="2026-08-11T08:00:00Z",
        updated_by=_HYUN_USER_ID,
    )


def _payload(question: str, *, actor_user_id: str = _HYUN_USER_ID) -> dict:
    return {
        "raw_text": f"<@{_BOXER_USER_ID}> {question}",
        "text": question.lower(),
        "question": re.sub(r"<@[^>]+>", "", question).strip(),
        "user_id": actor_user_id,
        "workspace_id": _WORKSPACE_ID,
        "channel_id": "C_TEST",
        "current_ts": "1784800000.000002",
        "thread_ts": "1784800000.000002",
        "request_log": {},
    }


def _runtime(store: Mock | None) -> access_routes.SlackBaseAccessRuntime:
    return access_routes.SlackBaseAccessRuntime(
        store=store,
        logger=logging.getLogger("test.slack_access"),
    )


def _active_internal_user(**overrides) -> dict:
    user = {
        "id": _TARGET_USER_ID,
        "team_id": _WORKSPACE_ID,
        "name": "zion",
        "real_name": "Zion",
        "deleted": False,
        "is_bot": False,
        "is_app_user": False,
        "is_restricted": False,
        "is_ultra_restricted": False,
        "is_stranger": False,
        "profile": {"display_name": "Zion"},
    }
    user.update(overrides)
    return user


class _SlackLookupError(RuntimeError):
    def __init__(self, error: str) -> None:
        super().__init__(error)
        self.response = {"error": error}


class _SlackResponse:
    def __init__(self, data: dict) -> None:
        self.data = data


class SlackBaseAccessRuntimeTests(unittest.TestCase):
    def test_runtime_fails_closed_for_missing_identity_store_or_error(self) -> None:
        store = Mock()
        store.is_allowed.side_effect = StoreUnavailable("down")

        self.assertFalse(_runtime(None).is_allowed(_WORKSPACE_ID, _TARGET_USER_ID))
        self.assertFalse(_runtime(store).is_allowed("", _TARGET_USER_ID))
        self.assertFalse(_runtime(store).is_allowed(_WORKSPACE_ID, ""))
        self.assertFalse(_runtime(store).is_allowed(_WORKSPACE_ID, _TARGET_USER_ID))

    def test_runtime_always_uses_local_membership_store(self) -> None:
        store = Mock()
        store.is_allowed.return_value = True

        self.assertTrue(_runtime(store).is_allowed(_WORKSPACE_ID, _TARGET_USER_ID))

        store.is_allowed.assert_called_once_with(_WORKSPACE_ID, _TARGET_USER_ID)

    def test_builder_uses_local_state_path_and_fails_closed_on_setup_error(self) -> None:
        store = Mock()
        with (
            patch.object(
                access_routes.cs,
                "BOXER_BASE_ACCESS_STATE_PATH",
                "/var/lib/boxer/base-access.json",
            ),
            patch.object(
                access_routes,
                "build_base_access_store",
                return_value=store,
            ) as build_store,
        ):
            runtime = access_routes.build_slack_base_access_runtime()

        self.assertIs(runtime.store, store)
        build_store.assert_called_once()
        built_settings = build_store.call_args.args[0]
        self.assertEqual(
            built_settings.state_path,
            "/var/lib/boxer/base-access.json",
        )

        with (
            patch.object(access_routes.cs, "BOXER_BASE_ACCESS_STATE_PATH", ""),
            patch.object(
                access_routes,
                "build_base_access_store",
                side_effect=access_routes.ConfigurationError("missing"),
            ),
        ):
            unavailable_runtime = access_routes.build_slack_base_access_runtime()

        self.assertFalse(
            unavailable_runtime.is_allowed(_WORKSPACE_ID, _TARGET_USER_ID)
        )


class SlackBaseAccessManagementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Mock()
        self.store.set_allowed.return_value = BaseAccessMutationResult(
            allowed=True,
            changed=True,
            stale=False,
        )
        self.runtime = _runtime(self.store)
        self.client = Mock()
        self.client.auth_test.return_value = {
            "team_id": _WORKSPACE_ID,
            "user_id": _BOXER_USER_ID,
        }
        self.client.users_list.return_value = {
            "members": [_active_internal_user()],
            "response_metadata": {"next_cursor": ""},
        }
        self.client.users_info.return_value = {"user": _active_internal_user()}
        self.replies: list[tuple[str, dict]] = []

    def _handle(
        self,
        question: str,
        *,
        actor_user_id: str = _HYUN_USER_ID,
        configured_hyun_user_id: str = _HYUN_USER_ID,
    ) -> bool:
        def reply(text: str, **kwargs) -> None:
            self.replies.append((text, kwargs))

        with patch.object(
            access_routes.cs,
            "HYUN_USER_ID",
            configured_hyun_user_id,
        ):
            return access_routes.handle_base_access_management_command(
                _payload(question, actor_user_id=actor_user_id),
                reply,
                self.client,
                logging.getLogger("test.slack_access.management"),
                runtime=self.runtime,
            )

    def test_only_exact_name_or_mention_commands_are_matched(self) -> None:
        for question in (
            f"<@{_TARGET_USER_ID}> 박서 사용자 추가",
            "박서 사용자 목록",
            f"<@{_TARGET_USER_ID}> 박서 사용 가능 확인",
            f"<@{_TARGET_USER_ID}|zion> 박서 사용 가능",
            "Zion 박서 사용 가능?",
            "박서 사용 가능",
        ):
            with self.subTest(question=question):
                self.assertFalse(self._handle(question))

        self.assertTrue(self._handle(f"<@{_TARGET_USER_ID}> 박서 사용 가능"))
        self.assertTrue(self._handle(f"<@{_TARGET_USER_ID}> 박서 사용 불가"))
        self.assertTrue(self._handle("Zion 박서 사용 가능"))
        self.assertTrue(self._handle("Zion 박서 사용 불가"))

    def test_first_mention_must_be_the_authenticated_boxer_user(self) -> None:
        payload = _payload(f"<@{_TARGET_USER_ID}> 박서 사용 가능")
        payload["raw_text"] = (
            f"<@U0999999999> <@{_TARGET_USER_ID}> 박서 사용 가능"
        )

        with patch.object(access_routes.cs, "HYUN_USER_ID", _HYUN_USER_ID):
            handled = access_routes.handle_base_access_management_command(
                payload,
                lambda text, **kwargs: self.replies.append((text, kwargs)),
                self.client,
                logging.getLogger("test.slack_access.management"),
                runtime=self.runtime,
            )

        self.assertFalse(handled)
        self.store.set_allowed.assert_not_called()

    def test_only_hyun_can_mutate_and_hyun_cannot_revoke_self(self) -> None:
        self.assertTrue(
            self._handle(
                f"<@{_TARGET_USER_ID}> 박서 사용 가능",
                actor_user_id="U_OTHER",
            )
        )
        self.assertTrue(self._handle(f"<@{_HYUN_USER_ID}> 박서 사용 불가"))
        self.assertTrue(
            self._handle(
                "Zion 박서 사용 가능",
                actor_user_id="U_OTHER",
            )
        )

        self.store.set_allowed.assert_not_called()
        self.client.auth_test.assert_not_called()
        self.client.users_list.assert_not_called()

    def test_misconfigured_hyun_env_fails_closed(self) -> None:
        # 배포 env가 다른 실제 사용자 ID로 바뀌어도 관리 권한을 넘기지 않는다.
        self.assertTrue(
            self._handle(
                f"<@{_TARGET_USER_ID}> 박서 사용 가능",
                actor_user_id="U_OTHER",
                configured_hyun_user_id="U_OTHER",
            )
        )

        self.store.set_allowed.assert_not_called()
        self.client.auth_test.assert_not_called()

    def test_grant_validates_active_internal_human_and_uses_slack_ts_order(self) -> None:
        handled = self._handle(f"<@{_TARGET_USER_ID}> 박서 사용 가능")

        self.assertTrue(handled)
        self.client.auth_test.assert_called_once_with()
        self.client.users_info.assert_called_once_with(user=_TARGET_USER_ID)
        self.store.set_allowed.assert_called_once_with(
            _WORKSPACE_ID,
            _TARGET_USER_ID,
            True,
            "Zion",
            _HYUN_USER_ID,
            "00000000001784800000.000002",
        )
        self.assertNotIn("client_msg_id", self.replies[-1][1])

    def test_plain_name_grant_resolves_unique_user_and_rechecks_with_users_info(self) -> None:
        handled = self._handle("  zIoN   박서 사용 가능")

        self.assertTrue(handled)
        self.client.users_list.assert_called_once_with(
            limit=200,
            team_id=_WORKSPACE_ID,
        )
        self.client.users_info.assert_called_once_with(user=_TARGET_USER_ID)
        self.store.set_allowed.assert_called_once_with(
            _WORKSPACE_ID,
            _TARGET_USER_ID,
            True,
            "Zion",
            _HYUN_USER_ID,
            "00000000001784800000.000002",
        )

    def test_plain_name_lookup_reads_every_page_and_accepts_space_name(self) -> None:
        other_user = _active_internal_user(
            id="U0999999998",
            name="other",
            real_name="Other",
            profile={"display_name": "Other"},
        )
        justin_user = _active_internal_user(
            name="justin.hyeon",
            real_name="Justin Hyeon",
            profile={"display_name": "Justin Hyeon"},
        )
        self.client.users_list.side_effect = (
            {
                "members": [other_user],
                "response_metadata": {"next_cursor": "PAGE_2"},
            },
            _SlackResponse(
                {
                    "members": [justin_user],
                    "response_metadata": {"next_cursor": ""},
                }
            ),
        )
        self.client.users_info.return_value = {"user": justin_user}

        self.assertTrue(self._handle("Justin   Hyeon 박서 사용 가능"))

        self.assertEqual(
            self.client.users_list.call_args_list[0].kwargs,
            {"limit": 200, "team_id": _WORKSPACE_ID},
        )
        self.assertEqual(
            self.client.users_list.call_args_list[1].kwargs,
            {"limit": 200, "team_id": _WORKSPACE_ID, "cursor": "PAGE_2"},
        )
        self.store.set_allowed.assert_called_once()

    def test_plain_name_lookup_rejects_ambiguous_or_missing_users(self) -> None:
        duplicate_user = _active_internal_user(id="U0999999997")
        for members, expected_reply in (
            (
                [_active_internal_user(), duplicate_user],
                "같은 이름의 사용자가 여러 명이야. 대상 사용자를 @멘션해줘",
            ),
            (
                [
                    _active_internal_user(
                        name="rosa",
                        real_name="Rosa",
                        profile={"display_name": "Rosa"},
                    )
                ],
                "이름으로 사용자를 찾지 못했어. 대상 사용자를 @멘션해줘",
            ),
        ):
            with self.subTest(expected_reply=expected_reply):
                self.store.reset_mock()
                self.client.users_info.reset_mock()
                self.replies.clear()
                self.client.users_list.return_value = {
                    "members": members,
                    "response_metadata": {"next_cursor": ""},
                }

                self.assertTrue(self._handle("Zion 박서 사용 가능"))

                self.store.set_allowed.assert_not_called()
                self.client.users_info.assert_not_called()
                self.assertEqual(self.replies[-1][0], expected_reply)

    def test_plain_name_grant_counts_guest_with_same_name_as_ambiguous(self) -> None:
        # 허용 불가 계정을 먼저 버리면 동명의 다른 사람을 잘못 허용할 수 있다.
        guest_user = _active_internal_user(
            id="U0999999996",
            is_restricted=True,
        )
        self.client.users_list.return_value = {
            "members": [_active_internal_user(), guest_user],
            "response_metadata": {"next_cursor": ""},
        }

        self.assertTrue(self._handle("Zion 박서 사용 가능"))

        self.store.set_allowed.assert_not_called()
        self.assertEqual(
            self.replies[-1][0],
            "같은 이름의 사용자가 여러 명이야. 대상 사용자를 @멘션해줘",
        )

    def test_plain_name_lookup_detects_ambiguous_users_across_pages(self) -> None:
        self.client.users_list.side_effect = (
            {
                "members": [_active_internal_user()],
                "response_metadata": {"next_cursor": "PAGE_2"},
            },
            {
                "members": [_active_internal_user(id="U0999999995")],
                "response_metadata": {"next_cursor": ""},
            },
        )

        self.assertTrue(self._handle("Zion 박서 사용 가능"))

        self.client.users_info.assert_not_called()
        self.store.set_allowed.assert_not_called()
        self.assertEqual(
            self.replies[-1][0],
            "같은 이름의 사용자가 여러 명이야. 대상 사용자를 @멘션해줘",
        )

    def test_plain_name_grant_rejects_unique_guest(self) -> None:
        self.client.users_list.return_value = {
            "members": [_active_internal_user(is_restricted=True)],
            "response_metadata": {"next_cursor": ""},
        }

        self.assertTrue(self._handle("Zion 박서 사용 가능"))

        self.client.users_info.assert_not_called()
        self.store.set_allowed.assert_not_called()
        self.assertEqual(
            self.replies[-1][0],
            "활성 상태인 내부 사람 계정만 박서 사용을 허용할 수 있어",
        )

    def test_plain_name_lookup_fails_closed_for_api_or_pagination_errors(self) -> None:
        cases = (
            RuntimeError("down"),
            {"members": "invalid", "response_metadata": {"next_cursor": ""}},
            {"members": [_active_internal_user()]},
            {
                "members": [_active_internal_user()],
                "response_metadata": {"next_cursor": "REPEATED"},
            },
        )
        for response in cases:
            with self.subTest(response_type=type(response).__name__):
                self.store.reset_mock()
                self.replies.clear()
                if isinstance(response, Exception):
                    self.client.users_list.side_effect = response
                elif (
                    response.get("response_metadata", {}).get("next_cursor")
                    == "REPEATED"
                ):
                    self.client.users_list.side_effect = (response, response)
                else:
                    self.client.users_list.side_effect = None
                    self.client.users_list.return_value = response

                self.assertTrue(self._handle("Zion 박서 사용 가능"))

                self.store.set_allowed.assert_not_called()
                self.assertEqual(
                    self.replies[-1][0],
                    access_routes.BASE_ACCESS_UNAVAILABLE_REPLY,
                )
                self.client.users_list.side_effect = None

    def test_plain_name_change_between_list_and_info_is_rejected(self) -> None:
        self.client.users_info.return_value = {
            "user": _active_internal_user(
                name="rosa",
                real_name="Rosa",
                profile={"display_name": "Rosa"},
            )
        }

        self.assertTrue(self._handle("Zion 박서 사용 가능"))

        self.store.set_allowed.assert_not_called()
        self.assertEqual(
            self.replies[-1][0],
            "사용자 이름이 변경됐어. 대상 사용자를 @멘션해줘",
        )

    def test_plain_name_cannot_revoke_hyun(self) -> None:
        hyun_user = _active_internal_user(
            id=_HYUN_USER_ID,
            name="hyun",
            real_name="Hyun",
            profile={"display_name": "Hyun"},
        )
        self.client.users_list.return_value = {
            "members": [hyun_user],
            "response_metadata": {"next_cursor": ""},
        }

        self.assertTrue(self._handle("Hyun 박서 사용 불가"))

        self.client.users_info.assert_not_called()
        self.store.set_allowed.assert_not_called()
        self.assertEqual(self.replies[-1][0], "현의 박서 사용 권한은 해제할 수 없어")

    def test_grant_accepts_real_slack_response_data_objects(self) -> None:
        self.client.auth_test.return_value = _SlackResponse(
            {"team_id": _WORKSPACE_ID, "user_id": _BOXER_USER_ID}
        )
        self.client.users_info.return_value = _SlackResponse(
            {"user": _active_internal_user()}
        )

        self.assertTrue(self._handle(f"<@{_TARGET_USER_ID}> 박서 사용 가능"))

        self.store.set_allowed.assert_called_once()

    def test_grant_rejects_non_human_external_deleted_and_boxer_accounts(self) -> None:
        invalid_users = (
            _active_internal_user(is_bot=True),
            _active_internal_user(bot_id="B123"),
            _active_internal_user(profile={"api_app_id": "A123"}),
            _active_internal_user(deleted=True),
            _active_internal_user(is_restricted=True),
            _active_internal_user(is_stranger=True),
            _active_internal_user(id=_BOXER_USER_ID),
            _active_internal_user(id="USLACK"),
            _active_internal_user(id="USLACKBOT"),
            _active_internal_user(team_id="T_OTHER"),
        )
        for user in invalid_users:
            with self.subTest(user=user):
                self.store.reset_mock()
                self.client.users_info.return_value = {"user": user}
                self._handle(f"<@{user['id']}> 박서 사용 가능")
                self.store.set_allowed.assert_not_called()

    def test_is_app_user_alone_does_not_turn_a_human_into_a_bot(self) -> None:
        self.client.users_info.return_value = {
            "user": _active_internal_user(is_app_user=True)
        }

        self.assertTrue(self._handle(f"<@{_TARGET_USER_ID}> 박서 사용 가능"))

        self.store.set_allowed.assert_called_once()

    def test_revoke_allows_deleted_account_tombstone_cleanup(self) -> None:
        self.client.users_info.side_effect = _SlackLookupError("user_not_found")
        self.store.get_member.return_value = _member()
        self.store.set_allowed.return_value = BaseAccessMutationResult(
            allowed=False,
            changed=True,
            stale=False,
        )

        self.assertTrue(self._handle(f"<@{_TARGET_USER_ID}> 박서 사용 불가"))

        self.store.get_member.assert_called_once_with(_WORKSPACE_ID, _TARGET_USER_ID)
        self.assertFalse(self.store.set_allowed.call_args.args[2])

    def test_plain_name_revoke_allows_deleted_account_tombstone_cleanup(self) -> None:
        deleted_user = _active_internal_user(deleted=True)
        self.client.users_list.return_value = {
            "members": [deleted_user],
            "response_metadata": {"next_cursor": ""},
        }
        self.client.users_info.side_effect = _SlackLookupError("user_not_found")
        self.store.get_member.return_value = _member()
        self.store.set_allowed.return_value = BaseAccessMutationResult(
            allowed=False,
            changed=True,
            stale=False,
        )

        self.assertTrue(self._handle("Zion 박서 사용 불가"))

        self.store.get_member.assert_called_once_with(_WORKSPACE_ID, _TARGET_USER_ID)
        self.assertFalse(self.store.set_allowed.call_args.args[2])

    def test_missing_boxer_identity_and_store_failure_are_fail_closed(self) -> None:
        self.client.auth_test.return_value = {"team_id": _WORKSPACE_ID}
        self._handle(f"<@{_TARGET_USER_ID}> 박서 사용 가능")
        self.store.set_allowed.assert_not_called()

        self.client.auth_test.return_value = {
            "team_id": _WORKSPACE_ID,
            "user_id": _BOXER_USER_ID,
        }
        self.store.set_allowed.side_effect = StoreUnavailable("down")
        self._handle(f"<@{_TARGET_USER_ID}> 박서 사용 가능")
        self.assertEqual(self.replies[-1][0], access_routes.BASE_ACCESS_UNAVAILABLE_REPLY)


if __name__ == "__main__":
    unittest.main()
