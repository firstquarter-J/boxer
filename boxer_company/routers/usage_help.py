import re

_PUBLIC_EXAMPLE_BARCODE = "12345678910"

_USAGE_HELP_PATTERN = re.compile(
    r"^(?:사용법|사용 방법|도움말|help|헬프|명령어(?:\s*목록)?)\s*(?:알려줘|보여줘|안내해줘)?$",
    re.IGNORECASE,
)


def _normalize_usage_help_question(question: str) -> str:
    normalized = " ".join(str(question or "").strip().split())
    return normalized.rstrip("!?.")


def _is_usage_help_request(question: str) -> bool:
    normalized = _normalize_usage_help_question(question)
    if not normalized:
        return False
    return bool(_USAGE_HELP_PATTERN.fullmatch(normalized))


def _build_usage_help_response() -> str:
    return "\n".join(
        [
            "*사용법*",
            "멘션 뒤에 아래 예시처럼 보내면 돼. 지금 실제 동작하는 라우터 기준이야.",
            "명령어처럼 딱 맞춰 안 써도 되고, 운영 중 궁금한 걸 문장으로 물어봐도 돼.",
            "바코드, 날짜, 병원명, 장비명 같이 주면 더 정확하게 찾아줘.",
            "",
            "*가장 많이 쓰는 기능*",
            f"• `{_PUBLIC_EXAMPLE_BARCODE} 2026-03-06 로그 분석`",
            f"• `{_PUBLIC_EXAMPLE_BARCODE} 2026-03-06 장비 파일 목록`",
            f"• `{_PUBLIC_EXAMPLE_BARCODE} 2026-03-06 파일 다운로드`",
            "",
            "*기본*",
            "• `사용법`",
            "• `ping`",
            "",
            "*바코드 영상 조회*",
            f"• `{_PUBLIC_EXAMPLE_BARCODE} 영상 개수`",
            f"• `{_PUBLIC_EXAMPLE_BARCODE} 영상 목록`",
            f"• `{_PUBLIC_EXAMPLE_BARCODE} 영상 정보`",
            f"• `{_PUBLIC_EXAMPLE_BARCODE} 영상 길이`",
            f"• `{_PUBLIC_EXAMPLE_BARCODE} 마지막 녹화일`",
            f"• `{_PUBLIC_EXAMPLE_BARCODE} 전체 녹화 날짜 목록`",
            f"• `{_PUBLIC_EXAMPLE_BARCODE} 2026-03-06 녹화 기록`",
            f"• `{_PUBLIC_EXAMPLE_BARCODE} 베이비매직 목록`",
            "",
            "*로그/원인 분석*",
            f"• `{_PUBLIC_EXAMPLE_BARCODE} 로그 분석`",
            f"• `{_PUBLIC_EXAMPLE_BARCODE} 2026-03-06 로그 분석`",
            f"• `{_PUBLIC_EXAMPLE_BARCODE} 2026-03-06 로그 에러 분석`",
            f"• `{_PUBLIC_EXAMPLE_BARCODE} 2026-03-06 녹화 실패 원인 분석`",
            "",
            "*장비 파일*",
            f"• `{_PUBLIC_EXAMPLE_BARCODE} 2026-03-06 파일 있나`",
            f"• `{_PUBLIC_EXAMPLE_BARCODE} 2026-03-06 fileid`",
            f"• `{_PUBLIC_EXAMPLE_BARCODE} 2026-03-06 파일 다운로드`",
            "",
            "*장비 점검*",
            "• `MB2-C00419 장비 상태`",
            "• `MB2-C00419 장비 소리 출력 점검`",
            "• `MB2-C00419 메모리 패치`",
            "• `MB2-C00419 pm2 상태`",
            "• `MB2-C00419 캡처보드 상태`",
            "• `MB2-C00419 LED 상태`",
            "",
            "*구조화 조회*",
            "• `2026년 병원 개수`",
            "• `병원명 서울병원 병실 목록`",
            "• `장비명 MB-200 장비 정보`",
            "• `2026-03-06 캡처 개수`",
            "• `병원명 서울병원 2026-03-06 영상 개수`",
            "",
            "*운영 조회*",
            f"• `s3 영상 {_PUBLIC_EXAMPLE_BARCODE}`",
            "• `s3 로그 MB-200 2026-03-06`",
            "• `db 조회 select seq, fullBarcode from recordings limit 3`",
            "• `요청 로그 최근 20`",
            "• `요청 로그 사용자 오늘`",
            "• `요청 로그 라우트 어제`",
            "• `요청 통계 오늘`",
            "",
            "*그냥 질문해도 돼*",
            "• `마미박스 동기화 안 될 때 조치`",
            "• `장비 ssh 연결이 안 되면 뭘 해야 해?`",
            "• `베이비매직 전송 안 된 이유`",
            "",
            "*팁*",
            "• 날짜는 `YYYY-MM-DD`로 쓰면 제일 정확해",
            "• 바코드 질문은 11자리 바코드를 같이 보내줘",
            "• 파일 다운로드 링크는 공개 채널이 아니라 DM으로만 보내",
        ]
    )
