import unittest
from unittest.mock import patch

from boxer_company import settings as cs
from boxer_company.notion_workspace_search import (
    _CONTENT_CACHE,
    CompanyNotionSearchResult,
    _PARENT_CACHE,
    _build_company_notion_search_reply,
    _extract_company_notion_search_query,
    _is_company_notion_search_allowed,
    _load_company_notion_page_lines,
    _load_company_notion_references,
    _looks_like_company_notion_search,
    _search_company_notion,
    _select_company_notion_excerpts,
)

_ROOT_ID = "11111111111111111111111111111111"
_COLUMN_ID = "22222222222222222222222222222222"
_COLUMN_LIST_ID = "33333333333333333333333333333333"
_COMMERCE_PAGE_ID = "44444444444444444444444444444444"
_OUTSIDE_PAGE_ID = "55555555555555555555555555555555"
_DATABASE_ID = "66666666666666666666666666666666"
_DATABASE_PAGE_ID = "77777777777777777777777777777777"
_UNRELATED_PAGE_ID = "88888888888888888888888888888888"
_ARCHIVED_PAGE_ID = "99999999999999999999999999999999"
_TOGGLE_BLOCK_ID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_CHILD_PAGE_BLOCK_ID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def _page_result(object_id: str, title: str, parent: dict[str, str]) -> dict[str, object]:
    return {
        "object": "page",
        "id": object_id,
        "url": f"https://app.notion.com/p/{object_id}",
        "last_edited_time": "2026-07-16T05:33:00.000Z",
        "parent": parent,
        "properties": {
            "title": {
                "type": "title",
                "title": [{"plain_text": title}],
            }
        },
    }


def _text_block(
    block_id: str,
    text: str,
    *,
    block_type: str = "paragraph",
    has_children: bool = False,
) -> dict[str, object]:
    payload: dict[str, object]
    if block_type == "child_page":
        payload = {"title": text}
    else:
        payload = {"rich_text": [{"plain_text": text}]}
    return {
        "object": "block",
        "id": block_id,
        "type": block_type,
        block_type: payload,
        "has_children": has_children,
    }


class CompanyNotionQueryTests(unittest.TestCase):
    def test_explicit_notion_phrases_are_detected(self) -> None:
        self.assertTrue(_looks_like_company_notion_search("회사 노션에서 커머스 찾아줘"))
        self.assertTrue(_looks_like_company_notion_search("Work Board Core Engineering"))
        self.assertFalse(_looks_like_company_notion_search("커머스 조직이 뭐야?"))

    def test_query_extraction_removes_search_instruction(self) -> None:
        self.assertEqual(
            _extract_company_notion_search_query("회사 노션에서 커머스 관련 문서 찾아줘"),
            "커머스",
        )
        self.assertEqual(
            _extract_company_notion_search_query("워크보드에서 Core Engineering 조회해줘"),
            "Core Engineering",
        )

    def test_allowed_users_fail_closed_when_user_is_missing(self) -> None:
        with patch.object(cs, "COMPANY_NOTION_SEARCH_ALLOWED_USER_IDS", {"U-HYUN"}):
            self.assertTrue(_is_company_notion_search_allowed("U-HYUN"))
            self.assertFalse(_is_company_notion_search_allowed("U-OTHER"))
            self.assertFalse(_is_company_notion_search_allowed(None))


class CompanyNotionSearchTests(unittest.TestCase):
    def setUp(self) -> None:
        _PARENT_CACHE.clear()

    def test_search_keeps_only_work_board_descendants(self) -> None:
        calls: list[tuple[str, str, dict[str, object] | None, str | None]] = []

        def fake_request(
            path: str,
            *,
            method: str = "GET",
            payload: dict[str, object] | None = None,
            token: str | None = None,
        ) -> dict[str, object]:
            calls.append((path, method, payload, token))
            if path == "/search":
                archived_page = _page_result(
                    _ARCHIVED_PAGE_ID,
                    "Commerce Archive",
                    {"type": "block_id", "block_id": _COLUMN_ID},
                )
                archived_page["archived"] = True
                return {
                    "results": [
                        _page_result(
                            _COMMERCE_PAGE_ID,
                            "Commerce",
                            {"type": "block_id", "block_id": _COLUMN_ID},
                        ),
                        _page_result(
                            _OUTSIDE_PAGE_ID,
                            "Commerce Private",
                            {"type": "workspace", "workspace": True},
                        ),
                        _page_result(
                            _UNRELATED_PAGE_ID,
                            "Product Weekly",
                            {"type": "block_id", "block_id": _COLUMN_ID},
                        ),
                        archived_page,
                    ]
                }
            if path == f"/blocks/{_COLUMN_ID}":
                return {"parent": {"type": "block_id", "block_id": _COLUMN_LIST_ID}}
            if path == f"/blocks/{_COLUMN_LIST_ID}":
                return {"parent": {"type": "page_id", "page_id": _ROOT_ID}}
            raise AssertionError(f"unexpected path: {path}")

        with (
            patch("boxer_company.notion_workspace_search._notion_request", side_effect=fake_request),
            patch.object(cs, "COMPANY_NOTION_SEARCH_PARENT_MAX_DEPTH", 8),
        ):
            results = _search_company_notion(
                "Commerce",
                root_page_id=_ROOT_ID,
                token="company-token",
                max_results=5,
                max_candidates=7,
            )

        self.assertEqual([result.title for result in results], ["Commerce"])
        self.assertEqual(calls[0][0:2], ("/search", "POST"))
        self.assertEqual(calls[0][2], {"query": "Commerce", "page_size": 7})
        self.assertTrue(all(call[3] == "company-token" for call in calls))

    def test_korean_department_query_also_searches_english_alias(self) -> None:
        search_queries: list[str] = []

        def fake_request(
            path: str,
            *,
            method: str = "GET",
            payload: dict[str, object] | None = None,
            token: str | None = None,
        ) -> dict[str, object]:
            del method, token
            if path == "/search":
                search_queries.append(str((payload or {}).get("query") or ""))
                if (payload or {}).get("query") == "Sales":
                    return {
                        "results": [
                            _page_result(
                                _COMMERCE_PAGE_ID,
                                "B2H Sales Team",
                                {"type": "page_id", "page_id": _ROOT_ID},
                            )
                        ]
                    }
                return {"results": []}
            raise AssertionError(f"unexpected path: {path}")

        with (
            patch("boxer_company.notion_workspace_search._notion_request", side_effect=fake_request),
            patch.object(cs, "COMPANY_NOTION_SEARCH_PARENT_MAX_DEPTH", 8),
        ):
            results = _search_company_notion(
                "영업",
                root_page_id=_ROOT_ID,
                token="company-token",
                max_results=5,
            )

        self.assertEqual(search_queries, ["영업", "Sales"])
        self.assertEqual([result.title for result in results], ["B2H Sales Team"])

    def test_search_accepts_page_inside_work_board_database(self) -> None:
        def fake_request(
            path: str,
            *,
            method: str = "GET",
            payload: dict[str, object] | None = None,
            token: str | None = None,
        ) -> dict[str, object]:
            del method, payload, token
            if path == "/search":
                return {
                    "results": [
                        _page_result(
                            _DATABASE_PAGE_ID,
                            "Commerce Weekly Meeting",
                            {"type": "database_id", "database_id": _DATABASE_ID},
                        )
                    ]
                }
            if path == f"/databases/{_DATABASE_ID}":
                return {"parent": {"type": "page_id", "page_id": _ROOT_ID}}
            raise AssertionError(f"unexpected path: {path}")

        with (
            patch("boxer_company.notion_workspace_search._notion_request", side_effect=fake_request),
            patch.object(cs, "COMPANY_NOTION_SEARCH_PARENT_MAX_DEPTH", 8),
        ):
            results = _search_company_notion(
                "Commerce",
                root_page_id=_ROOT_ID,
                token="company-token",
                max_results=5,
            )

        self.assertEqual([result.title for result in results], ["Commerce Weekly Meeting"])

    def test_parent_lookup_failure_is_excluded(self) -> None:
        with (
            patch(
                "boxer_company.notion_workspace_search._notion_request",
                side_effect=[
                    {
                        "results": [
                            _page_result(
                                _COMMERCE_PAGE_ID,
                                "Commerce",
                                {"type": "block_id", "block_id": _COLUMN_ID},
                            )
                        ]
                    },
                    RuntimeError("forbidden"),
                ],
            ),
            patch.object(cs, "COMPANY_NOTION_SEARCH_PARENT_MAX_DEPTH", 8),
        ):
            results = _search_company_notion(
                "Commerce",
                root_page_id=_ROOT_ID,
                token="company-token",
            )

        self.assertEqual(results, [])

    def test_reply_contains_only_safe_notion_links(self) -> None:
        result = CompanyNotionSearchResult(
            object_id=_COMMERCE_PAGE_ID,
            object_type="page",
            title="Commerce & Sales",
            url=f"https://app.notion.com/p/{_COMMERCE_PAGE_ID}",
            last_edited_time="",
        )

        reply = _build_company_notion_search_reply("Commerce", [result])

        self.assertIn("회사 Notion 검색", reply)
        self.assertIn("Commerce &amp; Sales", reply)
        self.assertIn(result.url, reply)
        self.assertIn("제목 검색", reply)


class CompanyNotionContentTests(unittest.TestCase):
    def setUp(self) -> None:
        _CONTENT_CACHE.clear()

    def test_page_lines_follow_nested_blocks_and_pagination_without_crossing_child_page(self) -> None:
        calls: list[tuple[str, str | None, int, str | None]] = []

        def fake_fetch_children(
            block_id: str,
            *,
            start_cursor: str | None = None,
            page_size: int = 100,
            token: str | None = None,
        ) -> dict[str, object]:
            calls.append((block_id, start_cursor, page_size, token))
            if block_id == _COMMERCE_PAGE_ID and start_cursor is None:
                return {
                    "results": [
                        _text_block("cccccccccccccccccccccccccccccccc", "페이지 개요"),
                        _text_block(
                            _TOGGLE_BLOCK_ID,
                            "영업 목표",
                            block_type="toggle",
                            has_children=True,
                        ),
                        _text_block(
                            _CHILD_PAGE_BLOCK_ID,
                            "별도 하위 페이지",
                            block_type="child_page",
                            has_children=True,
                        ),
                    ],
                    "has_more": True,
                    "next_cursor": "cursor-2",
                }
            if block_id == _TOGGLE_BLOCK_ID:
                return {
                    "results": [
                        _text_block(
                            "dddddddddddddddddddddddddddddddd",
                            "하반기 영업 목표는 신규 병원 20곳이야",
                        )
                    ],
                    "has_more": False,
                }
            if block_id == _COMMERCE_PAGE_ID and start_cursor == "cursor-2":
                return {
                    "results": [
                        _text_block("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "마지막 문단")
                    ],
                    "has_more": False,
                }
            raise AssertionError(f"unexpected child lookup: {block_id} {start_cursor}")

        with patch(
            "boxer_company.notion_workspace_search._fetch_notion_block_children",
            side_effect=fake_fetch_children,
        ):
            content = _load_company_notion_page_lines(
                _COMMERCE_PAGE_ID,
                token="company-token",
                max_depth=2,
                max_blocks=20,
            )

        self.assertEqual(
            content,
            {
                "lines": [
                    "페이지 개요",
                    "영업 목표",
                    "하반기 영업 목표는 신규 병원 20곳이야",
                    "별도 하위 페이지",
                    "마지막 문단",
                ],
                "blockCount": 5,
                "truncated": False,
            },
        )
        self.assertEqual(
            [(block_id, cursor) for block_id, cursor, _, _ in calls],
            [
                (_COMMERCE_PAGE_ID, None),
                (_TOGGLE_BLOCK_ID, None),
                (_COMMERCE_PAGE_ID, "cursor-2"),
            ],
        )
        self.assertNotIn(_CHILD_PAGE_BLOCK_ID, [block_id for block_id, _, _, _ in calls])
        self.assertTrue(all(token == "company-token" for _, _, _, token in calls))

    def test_relevant_excerpt_keeps_matching_line_and_neighbor_context(self) -> None:
        excerpts = _select_company_notion_excerpts(
            [
                "문서 소개",
                "하반기 영업 목표는 신규 병원 20곳이야",
                "달성을 위해 파트너 채널을 확대해",
                "전혀 무관한 복지 안내",
                " 하반기   영업 목표는 신규 병원 20곳이야 ",
            ],
            "영업 목표",
            max_chars=800,
        )

        self.assertEqual(
            excerpts,
            [
                "문서 소개",
                "하반기 영업 목표는 신규 병원 20곳이야",
                "달성을 위해 파트너 채널을 확대해",
            ],
        )

    def test_korean_department_query_matches_english_body_alias(self) -> None:
        excerpts = _select_company_notion_excerpts(
            [
                "복지 제도 안내",
                "Sales pipeline and quarterly target",
                "The team focuses on hospital partnerships",
                "Core Engineering release notes",
            ],
            "영업",
            max_chars=800,
        )

        self.assertEqual(
            excerpts,
            [
                "복지 제도 안내",
                "Sales pipeline and quarterly target",
                "The team focuses on hospital partnerships",
            ],
        )

    def test_reference_loading_isolates_one_page_failure(self) -> None:
        failed_result = CompanyNotionSearchResult(
            object_id=_COMMERCE_PAGE_ID,
            object_type="page",
            title="Commerce",
            url=f"https://app.notion.com/p/{_COMMERCE_PAGE_ID}",
            last_edited_time="2026-07-16T05:33:00.000Z",
        )
        readable_result = CompanyNotionSearchResult(
            object_id=_OUTSIDE_PAGE_ID,
            object_type="page",
            title="Sales",
            url=f"https://app.notion.com/p/{_OUTSIDE_PAGE_ID}",
            last_edited_time="2026-07-15T05:33:00.000Z",
        )

        with (
            patch(
                "boxer_company.notion_workspace_search._load_company_notion_page_lines",
                side_effect=[
                    RuntimeError("forbidden"),
                    {
                        "lines": ["Sales 운영", "영업 목표는 신규 병원 20곳이야"],
                        "blockCount": 2,
                        "truncated": False,
                    },
                ],
            ) as load_content,
            patch.object(cs, "COMPANY_NOTION_CONTENT_MAX_DEPTH", 4),
            patch.object(cs, "COMPANY_NOTION_CONTENT_MAX_BLOCKS", 120),
        ):
            references = _load_company_notion_references(
                "영업 목표",
                [failed_result, readable_result],
                token="company-token",
                max_pages=2,
                max_total_chars=1200,
            )

        self.assertEqual(load_content.call_count, 2)
        self.assertEqual(references[0]["title"], "Commerce")
        self.assertEqual(references[0]["excerpts"], [])
        self.assertTrue(references[0]["contentUnavailable"])
        self.assertEqual(references[1]["title"], "Sales")
        self.assertEqual(
            references[1]["excerpts"],
            ["Sales 운영", "영업 목표는 신규 병원 20곳이야"],
        )
        self.assertEqual(references[1]["blockCount"], 2)
        self.assertFalse(references[1]["contentTruncated"])


if __name__ == "__main__":
    unittest.main()
