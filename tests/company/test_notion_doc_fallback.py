import unittest

from boxer_company_adapter_slack.company import (
    _build_notion_doc_fallback,
    _needs_notion_doc_fallback,
)


_BABYMAGIC_REFERENCES = [
    {
        "title": "베이비매직 장애 안내",
        "previewLines": [
            "바코드 등록 없이 베이비매직이 생성돼서 유저 정보 미매칭 → 앱 전송 안 됨",
            '스레드에도 "유저 번호 매칭이 안 되어 있다"고 명시됨',
            "MDA 베이비매직 관리에서 재전송 버튼 클릭하면 유저를 찾아서 앱으로 전송 가능",
        ],
    }
]
_MOMMYBOX_RECORDING_PROCESS_REFERENCES = [
    {
        "title": "마미박스 프로세스 순서",
        "previewLines": [
            "순서: 바코드 스캔 후 준비 음성이 나오고 세션이 생성된 뒤 모션 감지가 시작돼",
            "상태: 모션 감지 단계의 MDA 상태는 RECORDING이 아니라 SESSION이야",
            "전환: 모션 감지 성공 또는 타임아웃이면 녹화 시작 음성 후 본 녹화가 시작되고 상태가 RECORDING으로 바뀌어",
            "주의: 모션 감지 단계에서 종료 스캔하면 본 녹화 종료가 아니라 취소 성격으로 처리될 수 있어",
            "음성: 모션 감지 단계에서 종료 스캔하면 녹화 취소 안내 음성이 나올 수 있고, 아직 본 녹화는 시작되지 않은 상태야",
            "종료: 녹화 중 종료 스캔하면 종료 음성이 나오고 파일을 마무리한 뒤 업로드를 시도해",
        ],
    }
]
_PINK_BARCODE_OVERVIEW_REFERENCES = [
    {
        "title": "핑크 바코드: 운영 개요",
        "previewLines": [
            "개요: 핑크 바코드 질문은 동기화, 앱 표시, 검증 정책 3가지로 나눠 봐야 해",
        ],
    }
]
_BARCODE_FIRST_RECORDING_EDGE_CASE_REFERENCES = [
    {
        "title": "바코드 표시: 구매 병원과 첫 촬영 병원이 다른 경우",
        "previewLines": [
            "정책 첫 녹화가 발생한 병원 기준으로 앱의 바코드 표시가 정해질 수 있어",
        ],
    }
]
_PINK_BARCODE_VALIDATION_POLICY_REFERENCES = [
    {
        "title": "바코드 검증: 핑크 바코드만 예외 허용할 수 있는지",
        "previewLines": [
            "정책: 핑크 바코드만 따로 녹화 허용/차단하는 설정은 없어",
            "전제: 바코드 유효성 검증을 해제하면 검증 없이 녹화가 진행돼",
        ],
    }
]
_REMOTE_ACCESS_FIREWALL_REFERENCES = [
    {
        "title": "병원 방화벽으로 MDA/원격 접속이 안 될 때",
        "previewLines": [
            "영상 업로드는 정상이어도 원격 접속은 별도 경로라 제한될 수 있어",
            "병원 네트워크와 SSH 접근 정책을 같이 봐야 해",
        ],
    }
]


class NotionDocFallbackTests(unittest.TestCase):
    def test_mommybox_recording_process_explains_session_recording_and_upload(self) -> None:
        text = _build_notion_doc_fallback(
            "마미박스 녹화 프로세스 설명",
            _MOMMYBOX_RECORDING_PROCESS_REFERENCES,
        )

        self.assertIn(
            "• 결론: 바코드 스캔 후 준비 음성이 나오고 세션이 생성된 뒤 모션 감지가 시작돼",
            text,
        )
        self.assertIn(
            "• 확인: 모션 감지 단계의 상태는 RECORDING이 아니라 SESSION이고, 모션 감지 성공 또는 타임아웃이면 그때 녹화 시작 음성 후 본 녹화가 시작돼. 모션 감지 단계에서 종료 스캔하면 녹화 취소 안내 음성이 나올 수 있고 아직 본 녹화는 시작되지 않은 상태야",
            text,
        )
        self.assertIn(
            "• 조치: 모션 감지 통과 전 종료 스캔이면 녹화 취소 안내로 봐야 하고, 녹화 중 종료 스캔일 때만 종료 음성과 함께 파일 마무리 후 업로드를 시도한다고 안내해",
            text,
        )

    def test_mommybox_recording_cancel_voice_question_is_answered_directly(self) -> None:
        text = _build_notion_doc_fallback(
            "마미박스 녹화 취소 음성은 언제 나와?",
            _MOMMYBOX_RECORDING_PROCESS_REFERENCES,
        )

        self.assertIn(
            "녹화 취소 안내 음성이 나올 수 있고 아직 본 녹화는 시작되지 않은 상태야",
            text,
        )
        self.assertIn(
            "모션 감지 통과 전 종료 스캔이면 녹화 취소 안내로 봐야 하고",
            text,
        )

    def test_pink_barcode_overview_breaks_question_into_three_tracks(self) -> None:
        text = _build_notion_doc_fallback(
            "핑크 바코드 전체 정리해줘",
            _PINK_BARCODE_OVERVIEW_REFERENCES,
        )

        self.assertIn(
            "• 결론: 핑크 바코드 이슈는 동기화, 앱 표시, 검증 정책 3가지로 나눠 봐야 해",
            text,
        )
        self.assertIn(
            "• 조치: 스캔 이슈면 동기화 문서, 앱 표시 이슈면 첫 촬영 병원 문서, 허용/차단 정책이면 검증 정책 문서 기준으로 이어서 보면 돼",
            text,
        )

    def test_babymagic_send_issue_action_checks_barcode_before_resend(self) -> None:
        text = _build_notion_doc_fallback("베이비매직 전송 안 된 이유", _BABYMAGIC_REFERENCES)

        self.assertIn(
            "• 조치: 유저가 앱에서 생성한 아이에 바코드를 등록했는지 먼저 확인하고, "
            "그다음 MDA 베이비매직 관리에서 재전송을 시도해",
            text,
        )

    def test_babymagic_retry_action_missing_triggers_fallback(self) -> None:
        fallback_text = _build_notion_doc_fallback("베이비매직 전송 안 된 이유", _BABYMAGIC_REFERENCES)
        synthesized_text = """*문서 기반 답변*
• 결론: 바코드 등록 없이 베이비매직이 생성돼서 앱 전송이 안 됐어
• 확인: 유저 번호 매칭이 안 되어 있는지 확인해
• 조치: MDA 베이비매직 관리에서 재전송 버튼을 눌러봐"""

        self.assertTrue(
            _needs_notion_doc_fallback(synthesized_text, "notion playbook qa", fallback_text)
        )

    def test_barcode_first_recording_edge_case_mentions_no_extra_purchase(self) -> None:
        text = _build_notion_doc_fallback(
            "분만 병원에서 구매 후 첫 촬영이 비분만 병원이면 핑크 바코드로 보여?",
            _BARCODE_FIRST_RECORDING_EDGE_CASE_REFERENCES,
        )

        self.assertIn(
            "• 결론: 첫 녹화가 비분만 병원에서 먼저 나가면 앱에는 핑크 바코드로 보일 수 있어",
            text,
        )
        self.assertIn(
            "• 조치: 이건 표시상 엣지케이스라 실제 녹화 차단이나 신규 바코드 추가 구매가 필요한 건 아니라고 안내해",
            text,
        )

    def test_barcode_first_recording_edge_case_missing_purchase_guidance_triggers_fallback(self) -> None:
        fallback_text = _build_notion_doc_fallback(
            "분만 병원에서 구매 후 첫 촬영이 비분만 병원이면 핑크 바코드로 보여?",
            _BARCODE_FIRST_RECORDING_EDGE_CASE_REFERENCES,
        )
        synthesized_text = """*문서 기반 답변*
• 결론: 첫 촬영 병원 영향으로 핑크 바코드처럼 보일 수 있어
• 확인: 첫 촬영 병원이 어디인지 확인해
• 조치: 고객에게 상태를 안내해"""

        self.assertTrue(
            _needs_notion_doc_fallback(synthesized_text, "notion playbook qa", fallback_text)
        )

    def test_pink_barcode_validation_policy_explains_no_partial_exception(self) -> None:
        text = _build_notion_doc_fallback(
            "핑크 바코드만 분만 병원에서 녹화 허용할 수 있어?",
            _PINK_BARCODE_VALIDATION_POLICY_REFERENCES,
        )

        self.assertIn(
            "• 결론: 핑크 바코드만 따로 녹화 허용/차단하는 설정은 없어",
            text,
        )
        self.assertIn(
            "• 조치: 핑크 바코드도 녹화되게 하려면 바코드 유효성 검증 자체를 해제해야 하고, 그러면 검증 없이 녹화가 진행돼",
            text,
        )

    def test_pink_barcode_validation_disable_question_mentions_unvalidated_recording(self) -> None:
        text = _build_notion_doc_fallback(
            "바코드 유효성 검증 해제하면 검증 없이 녹화가 진행돼?",
            _PINK_BARCODE_VALIDATION_POLICY_REFERENCES,
        )

        self.assertIn(
            "• 결론: 맞아. 바코드 유효성 검증을 해제하면 검증 없이 녹화가 진행돼",
            text,
        )
        self.assertIn(
            "• 확인: 이건 핑크 바코드만 예외 허용하는 게 아니라 전체 검증을 푸는 설정이야",
            text,
        )

    def test_remote_access_issue_does_not_jump_to_firewall_without_https_signal(self) -> None:
        text = _build_notion_doc_fallback(
            "장비 ssh 연결이 안 되면 뭘 해야 해?",
            _REMOTE_ACCESS_FIREWALL_REFERENCES,
        )

        self.assertIn(
            "• 결론: SSH 연결 불가만으로는 병원 네트워크 문제인지 SSH 접근 제한인지 아직 단정 못 해",
            text,
        )
        self.assertIn(
            "HTTPS도 안 되면 네트워크 문제일 수 있고, HTTPS는 되는데 SSH만 안 되면 그때 SSH 방화벽이나 접근 제한으로 좁힐 수 있어",
            text,
        )
        self.assertIn(
            "• 조치: 먼저 장비 온라인 상태와 HTTPS 응답 여부를 확인해.",
            text,
        )

    def test_remote_access_issue_with_https_ping_response_points_to_ssh_block(self) -> None:
        text = _build_notion_doc_fallback(
            "HTTPS로 장비에 ping 보냈을 때 응답은 오는데 ssh 연결은 안 돼. 뭘 해야 해?",
            _REMOTE_ACCESS_FIREWALL_REFERENCES,
        )

        self.assertIn(
            "• 결론: HTTPS ping 응답이 오는데 SSH만 안 되면 병원 네트워크 전체 문제보다는 SSH 접근만 막힌 케이스로 보는 게 맞아",
            text,
        )
        self.assertIn(
            "• 조치: 병원 쪽 SSH 방화벽이나 접근 정책 허용 여부를 확인해.",
            text,
        )

    def test_remote_access_old_firewall_only_answer_triggers_fallback(self) -> None:
        fallback_text = _build_notion_doc_fallback(
            "장비 ssh 연결이 안 되면 뭘 해야 해?",
            _REMOTE_ACCESS_FIREWALL_REFERENCES,
        )
        synthesized_text = """*문서 기반 답변*
• 결론: 병원 방화벽이 SSH 연결을 막고 있을 가능성이 높아
• 확인: 병원 네트워크/방화벽 설정 여부를 확인해
• 조치: 병원 담당자와 방화벽 설정을 협의해"""

        self.assertTrue(
            _needs_notion_doc_fallback(synthesized_text, "notion playbook qa", fallback_text)
        )


if __name__ == "__main__":
    unittest.main()
