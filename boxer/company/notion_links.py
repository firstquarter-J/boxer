import re
from typing import Any

_LOW_SIGNAL_COMPANY_DOC_TERMS = {
    "가이드",
    "문서",
    "설정",
    "박스",
    "마미박스",
    "방법",
    "문제",
    "안내",
    "조치",
    "정보",
}

_COMPANY_DOC_OVERVIEW_TOKENS = {
    "설명",
    "소개",
    "개요",
    "무엇",
    "뭐야",
    "전체",
    "운영",
}

_COMPOUND_TERM_SUFFIXES = (
    "방법",
    "가이드",
    "설정",
    "절차",
    "이슈",
    "문제",
    "오류",
    "장애",
    "불가",
    "안됨",
    "법",
)

_COMPANY_NOTION_DOCS: tuple[dict[str, Any], ...] = (
    {
        "title": "마미박스",
        "url": "https://www.notion.so/humanscape/65cfb753233b4dd1985b6f714a5a5b72?v=83e24c3c64284b0f853660be666f02be",
        "keywords": ("마미박스", "개요", "설명", "소개", "운영", "전체"),
    },
    {
        "title": "로그 패턴 분석 가이드",
        "url": "https://www.notion.so/79b6331571964dc7b033f917e2eb7cdc?pvs=21",
        "keywords": ("로그", "패턴", "분석", "error", "오류", "log"),
    },
    {
        "title": "베이비매직 장애 안내",
        "url": "https://www.notion.so/humanscape/1dab459793c880c7bdefd618030b2b12",
        "keywords": ("베이비매직", "babymagic", "장애", "안내", "이슈"),
    },
    {
        "title": "베이비매직 CS 자동화",
        "url": "https://www.notion.so/humanscape/CS-247b459793c880f79543f984098ab22e",
        "keywords": ("베이비매직", "babymagic", "cs", "자동화", "고객응대"),
    },
    {
        "title": "정상 촬영 전체 로그 예시",
        "url": "https://www.notion.so/c22d55a8564c4c86976dc5b8cb0736af?pvs=21",
        "keywords": ("정상", "촬영", "전체", "로그", "예시", "비교"),
    },
    {
        "title": "초음파 영상 이전 & 삭제 & 편집",
        "url": "https://www.notion.so/4cbfb5a4f2744270b80f0ce2e65e64c8?pvs=21",
        "keywords": ("이전", "삭제", "편집", "이동", "수정"),
    },
    {
        "title": "초음파 영상 소리 잡음(노이즈)",
        "url": "https://www.notion.so/e7edb528ce02490f989b3c276fa2e36c?pvs=21",
        "keywords": ("소리", "잡음", "노이즈", "지지직", "웅", "울림"),
    },
    {
        "title": "초음파 화면 잡음(전기적 아티팩트)",
        "url": "https://www.notion.so/177d235a53fd40858dee7691c1acf023?pvs=21",
        "keywords": ("화면", "잡음", "아티팩트", "전기", "노이즈", "줄"),
    },
    {
        "title": "마미박스 장비 그라운드 루프 아이솔레이터",
        "url": "https://www.notion.so/320cf826870c805ca1fcd80b6160ad9a",
        "keywords": ("그라운드 루프", "접지", "노이즈", "잡음", "아이솔레이터"),
    },
    {
        "title": "초음파 영상 업로드 반복 실패",
        "url": "https://www.notion.so/8d291bc21002487a970ed11394541752?pvs=21",
        "keywords": ("업로드", "반복", "실패", "재시도", "업로드 실패"),
    },
    {
        "title": "초음파 영상 업로드 안됨(네트워크 이슈)",
        "url": "https://www.notion.so/390aa941853c4c279e545de06e49dce7?pvs=21",
        "keywords": ("업로드", "안됨", "네트워크", "network", "eai_again", "통신"),
    },
    {
        "title": "병원 방화벽으로 MDA/원격 접속이 안 될 때",
        "url": "https://www.notion.so/MDA-322cf826870c812aaee6f9c62838b486",
        "keywords": (
            "병원 방화벽",
            "방화벽",
            "firewall",
            "mda",
            "원격 접속",
            "원격 연결",
            "ssh",
            "status none",
            "모니터링",
            "영상 업로드 정상",
        ),
    },
    {
        "title": "초음파 영상 녹화불가(화면 신호 없음)",
        "url": "https://www.notion.so/30a245b6fc2c4e28a605bb7ca9b34382?pvs=21",
        "keywords": ("녹화불가", "녹화", "신호 없음", "영상 입력", "캡처보드", "ffmpeg"),
    },
    {
        "title": "마미박스 소리 없음",
        "url": "https://www.notion.so/bffdbfccd62e46e1a34750a3502e0475?pvs=21",
        "keywords": ("소리", "오디오", "무음", "안나와", "안 나와", "스피커", "사운드"),
    },
    {
        "title": "마미박스 IP 충돌(초기 프로비저닝)",
        "url": "https://www.notion.so/IP-7b313fa9ddab434997cd843bc19b0168?pvs=21",
        "keywords": ("ip", "아이피", "충돌", "프로비저닝", "network", "네트워크"),
    },
    {
        "title": "프로비저닝",
        "url": "https://www.notion.so/humanscape/88ee8d577dc449738ce4f1e7d129b8c0",
        "keywords": ("프로비저닝", "설치", "초기 설정", "초기 세팅"),
    },
    {
        "title": "바이오스 설정",
        "url": "https://www.notion.so/humanscape/e5d28e4342874a1e9332ef832efe6bb8",
        "keywords": ("바이오스", "bios", "설정", "부팅 설정"),
    },
    {
        "title": "설정 스크립트 가이드",
        "url": "https://www.notion.so/humanscape/f20ec0d536a84df6a9d64f41fe659899",
        "keywords": ("설정 스크립트", "스크립트", "세팅", "설정", "가이드"),
    },
    {
        "title": "데스크탑 모드 가이드",
        "url": "https://www.notion.so/humanscape/99bc2e78ce764ebfa7aa624f16893177",
        "keywords": ("데스크탑 모드", "desktop mode", "데스크탑", "모드"),
    },
    {
        "title": "네트워크 설정",
        "url": "https://www.notion.so/humanscape/1fe2fa728ff94cacab9158364f6fb8a5",
        "keywords": ("네트워크 설정", "네트워크", "ip", "아이피", "gateway", "dns"),
    },
    {
        "title": "네트워크 환경 가이드라인",
        "url": "https://www.notion.so/humanscape/1a0b459793c880cca668cb8ae810efa4",
        "keywords": ("네트워크 환경", "가이드라인", "네트워크", "환경", "설치 환경"),
    },
    {
        "title": "마미박스 아이피 변경",
        "url": "https://www.notion.so/d5e4dc959e7d42d88d807c68db7927bd?pvs=21",
        "keywords": ("ip", "아이피", "변경", "네트워크", "주소"),
    },
    {
        "title": "마미박스 멈춤 & 비정상 재부팅",
        "url": "https://www.notion.so/95452aff33e2410dac2907f821a24a96?pvs=21",
        "keywords": ("멈춤", "멈춰", "프리징", "재부팅", "비정상 재부팅", "꺼짐", "restart"),
    },
    {
        "title": "마미박스 부팅 불가(파일 시스템 손상)",
        "url": "https://www.notion.so/c2454132185446639b6ba01297d7eadf?pvs=21",
        "keywords": ("부팅", "부팅 불가", "파일 시스템", "손상", "fsck", "mount"),
    },
    {
        "title": "마미박스 초음파 이미지 캡쳐 불가",
        "url": "https://www.notion.so/15fba1ff8d7e45d397ac576f48176b09?pvs=21",
        "keywords": ("이미지", "캡쳐", "캡처", "스냅샷", "capture"),
    },
    {
        "title": "마미박스 초기화",
        "url": "https://www.notion.so/humanscape/8205cca89057461193e18e9d362eb64a",
        "keywords": ("초기화", "리셋", "reset", "공장초기화"),
    },
    {
        "title": "추가 보조장비 작동불가 이슈",
        "url": "https://www.notion.so/90c218923fba406eaba7f989d632a78f?pvs=21",
        "keywords": ("보조장비", "작동불가", "작동 안함", "추가 장비", "장치"),
    },
    {
        "title": "바코드 스캐너 작동 문제",
        "url": "https://www.notion.so/97f0d0137b79421fa5b8c44c156ffef9?pvs=21",
        "keywords": ("바코드", "스캐너", "스캔", "작동 문제", "barcode"),
    },
    {
        "title": "바코드 동기화: 분만 병원에서 핑크 바코드가 스캔되는 경우",
        "url": "https://www.notion.so/humanscape/321b459793c880c5ba5af9705a905c15",
        "keywords": (
            "바코드 동기화",
            "핑크 바코드",
            "무료 바코드",
            "유료 바코드",
            "분만 병원",
            "비분만 병원",
            "온라인 상태",
            "cfg1_barcode_sync_date",
        ),
    },
    {
        "title": "캡쳐 스위치 문제",
        "url": "https://www.notion.so/6ae05f235f114801bf88bcc761c44dc3?pvs=21",
        "keywords": ("캡쳐 스위치", "캡처 스위치", "스위치", "버튼"),
    },
    {
        "title": "커스텀 크롭 설정",
        "url": "https://www.notion.so/humanscape/02daa3935f3c494899b116e4ef41e134",
        "keywords": ("커스텀 크롭", "크롭", "crop", "화면 설정"),
    },
    {
        "title": "QR 코드북",
        "url": "https://www.notion.so/humanscape/QR-e865f1d23818442db60defc6146d6c2c",
        "keywords": ("qr", "qr 코드북", "코드북", "qr코드"),
    },
    {
        "title": "박스 음량 조절",
        "url": "https://www.notion.so/humanscape/8f69d2ece36b466ca23ee39cc48a8475",
        "keywords": ("음량", "볼륨", "소리", "오디오", "사운드"),
    },
    {
        "title": "DVI 분배기",
        "url": "https://www.notion.so/humanscape/DVI-7b25d3d3f4274a30a330e47d5c6c3278",
        "keywords": ("dvi", "분배기", "분배", "영상 분배", "케이블"),
    },
    {
        "title": "진단기 모델별 정보",
        "url": "https://www.notion.so/humanscape/2f34909c3c964f039f134a99ba833840",
        "keywords": ("진단기", "모델", "모델별", "장비 모델", "초음파 기기"),
    },
    {
        "title": "마미박스 프로세스 순서",
        "url": "https://www.notion.so/cc18b16854024fecbe3c2292d2fe8bae?pvs=21",
        "keywords": ("프로세스", "순서", "흐름", "설명", "개요", "구성"),
    },
    {
        "title": "박스 원격 업데이트",
        "url": "https://www.notion.so/07dc2342d476497494b3570d7554b552?pvs=21",
        "keywords": ("원격", "업데이트", "패치", "배포", "버전"),
    },
    {
        "title": "2.11 버전 박스 원격 음성 설정",
        "url": "https://www.notion.so/humanscape/2-11-1fbb459793c880b2bf7cff420295cf1d",
        "keywords": ("2.11", "버전", "원격 음성", "음성 설정", "오디오", "사운드"),
    },
    {
        "title": "299버전 메모리 문제 확인 및 조치",
        "url": "https://www.notion.so/humanscape/299-2a4b459793c8800e86c5f26800e6b71e",
        "keywords": ("299", "299버전", "메모리", "메모리 문제", "패치", "조치"),
    },
    {
        "title": "신규 보조장비 등록",
        "url": "https://www.notion.so/d00c2160aaf64eecbd288ff7015a9314?pvs=21",
        "keywords": ("신규", "보조장비", "등록", "장비 추가"),
    },
    {
        "title": "캡쳐 스위치 셋팅 변경",
        "url": "https://www.notion.so/100c703226994940b13670a46b41222e?pvs=21",
        "keywords": ("캡쳐 스위치", "캡처 스위치", "셋팅", "설정", "변경"),
    },
    {
        "title": "초음파 영상 업로드 이슈 분석 가이드",
        "url": "https://www.notion.so/8fdbb40dc4964ef5abf30baa17371de5?pvs=21",
        "keywords": ("업로드", "이슈", "분석", "가이드", "로그", "네트워크"),
    },
    {
        "title": "초음파 영상 확인",
        "url": "https://www.notion.so/928b6cfcb7c7463d92c787c69d0ca7f1?pvs=21",
        "keywords": ("영상 확인", "조회", "재생", "보기"),
    },
    {
        "title": "초음파 영상 꺼내기",
        "url": "https://www.notion.so/4a38fa8b11c64554ae23ee91f5f904d3?pvs=21",
        "keywords": ("영상 꺼내기", "다운로드", "반출", "파일"),
    },
    {
        "title": "초음파 영상 업로드",
        "url": "https://www.notion.so/6876a871439e41d5a215ea9ab6068130?pvs=21",
        "keywords": ("업로드", "전송", "올리기"),
    },
    {
        "title": "초음파 영상 이전(타 업체)",
        "url": "https://www.notion.so/d8ebb22250ed40188c0e0a1a07c6b78d?pvs=21",
        "keywords": ("이전", "타 업체", "타업체", "마이그레이션", "이관"),
    },
)


def _normalize_lookup_text(text: str) -> str:
    return re.sub(r"[^0-9a-z가-힣]+", " ", str(text or "").strip().lower()).strip()


def _extract_lookup_terms(text: str) -> list[str]:
    normalized = _normalize_lookup_text(text)
    if not normalized:
        return []

    seen: set[str] = set()
    terms: list[str] = []
    for token in normalized.split():
        if len(token) < 2 and not token.isdigit():
            continue
        if token in seen:
            continue
        seen.add(token)
        terms.append(token)
        for suffix in _COMPOUND_TERM_SUFFIXES:
            if not token.endswith(suffix):
                continue
            prefix = token[: -len(suffix)].strip()
            if len(prefix) < 2:
                continue
            if prefix not in seen:
                seen.add(prefix)
                terms.append(prefix)
            if len(suffix) >= 2 and suffix not in seen:
                seen.add(suffix)
                terms.append(suffix)
    return terms


def _build_seed_titles(playbooks: list[dict[str, Any]] | None) -> list[str]:
    titles: list[str] = []
    seen: set[str] = set()
    for item in playbooks or []:
        if not isinstance(item, dict):
            continue
        title = _normalize_lookup_text(str(item.get("title") or ""))
        if not title or title in seen:
            continue
        seen.add(title)
        titles.append(title)
    return titles


def _build_seed_terms(question: str, playbooks: list[dict[str, Any]] | None) -> tuple[str, set[str]]:
    normalized_question = _normalize_lookup_text(question)
    terms = set(_extract_lookup_terms(question))
    for item in playbooks or []:
        if not isinstance(item, dict):
            continue
        for raw_keyword in item.get("matchedKeywords") or []:
            keyword = str(raw_keyword or "").strip()
            if not keyword:
                continue
            terms.update(_extract_lookup_terms(keyword))
    return normalized_question, terms


def select_company_notion_doc_links(
    question: str,
    *,
    notion_playbooks: list[dict[str, Any]] | None = None,
    max_results: int = 3,
) -> list[dict[str, str]]:
    normalized_question, seed_terms = _build_seed_terms(question, notion_playbooks)
    if not normalized_question and not seed_terms:
        return []

    seed_titles = _build_seed_titles(notion_playbooks)
    has_overview_intent = any(token in normalized_question for token in _COMPANY_DOC_OVERVIEW_TOKENS)
    require_exact_title_match = bool(seed_titles) and not has_overview_intent
    scored: list[tuple[int, dict[str, Any]]] = []

    for entry in _COMPANY_NOTION_DOCS:
        title = str(entry.get("title") or "").strip()
        normalized_title = _normalize_lookup_text(title)
        keywords = [str(keyword).strip() for keyword in entry.get("keywords") or () if str(keyword).strip()]
        normalized_keywords = [_normalize_lookup_text(keyword) for keyword in keywords]
        entry_terms = set(_extract_lookup_terms(title))
        for keyword in keywords:
            entry_terms.update(_extract_lookup_terms(keyword))

        title_matches_seed = normalized_title in seed_titles if seed_titles else False
        if require_exact_title_match and not title_matches_seed:
            continue

        score = 0
        for seed_title in seed_titles:
            if seed_title == normalized_title:
                score += 120

        matched_terms = sorted(seed_terms & entry_terms)
        strong_terms = [term for term in matched_terms if term not in _LOW_SIGNAL_COMPANY_DOC_TERMS]
        weak_terms = [term for term in matched_terms if term in _LOW_SIGNAL_COMPANY_DOC_TERMS]
        score += len(strong_terms) * 9
        score += len(weak_terms) * 2

        for keyword in normalized_keywords:
            if not keyword:
                continue
            if keyword in normalized_question:
                score += 5 if keyword in _LOW_SIGNAL_COMPANY_DOC_TERMS else 12

        if normalized_title and normalized_title in normalized_question:
            score += 30
        if has_overview_intent and normalized_title == "마미박스":
            score += 45
        elif normalized_title == "마미박스":
            score -= 20

        if score < 18:
            continue

        scored.append((score, entry))

    scored.sort(key=lambda item: (item[0], str((item[1] or {}).get("title") or "")), reverse=True)

    selected: list[dict[str, str]] = []
    seen_titles: set[str] = set()
    for _, entry in scored:
        title = str(entry.get("title") or "").strip()
        url = str(entry.get("url") or "").strip()
        if not title or not url or title in seen_titles:
            continue
        seen_titles.add(title)
        selected.append({"title": title, "url": url})
        if len(selected) >= max(1, max_results):
            break

    return selected
