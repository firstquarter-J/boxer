"""기간별 리포트의 요청 단위 조건. DB나 Slack 실행 의존성을 사용하지 않는다."""

import math
import re
from dataclasses import dataclass


class RecordingsReportOptionsError(ValueError):
    """사용자에게 그대로 안내할 수 있는 요청 조건 오류."""


@dataclass(frozen=True, slots=True)
class RecordingsReportOptions:
    hospitals: tuple[str, ...] = ()
    drop_percent: float | None = None
    minimum_drop: int | None = None

    def __post_init__(self) -> None:
        # 직접 호출에도 같은 범위를 적용해 0과 생략을 구별한다.
        if self.drop_percent is not None and (
            isinstance(self.drop_percent, bool)
            or not isinstance(self.drop_percent, (int, float))
            or not math.isfinite(self.drop_percent)
            or not 0 <= self.drop_percent <= 100
        ):
            raise RecordingsReportOptionsError("감소율은 0~100% 사이로 알려줘")
        if self.minimum_drop is not None and (
            type(self.minimum_drop) is not int or self.minimum_drop < 0
        ):
            raise RecordingsReportOptionsError("최소 감소량은 0 이상의 정수로 알려줘")
        if any(not name.strip() or len(name) > 200 for name in self.hospitals):
            raise RecordingsReportOptionsError("병원 이름을 확인해줘")


_NUMBER = r"(?<![\d.,])[+-]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?(?![\d.,])"
_RATE_LABEL = r"(?:감소율|감소\s*비율|퍼센트(?:\s*제한)?)"
_COUNT_LABEL = (
    r"(?:최소\s*감소량|감소량|최소\s*(?:건수|수량|개수)|(?:건수|수량)\s*제한|"
    r"최소(?=\s*[:：=]?\s*[+-]?\d))"
)
_HOSPITAL_LABEL = r"(?:대상\s*병원|병원\s*제한|병원명|병원)"
_NATURAL_HOSPITAL = re.compile(
    r"(?<![^\s,，])([^\s,:，]+?(?:병원|의원)(?:\([^)]*\))?)(?:만)?(?=\s|$|[,，])"
)
_REPORT_WORDS = r"(?:녹화|신규\s*바코드|초음파|영상|요약|리포트|현황|보고|집계|통계)"
_HOSPITAL_FIELD = re.compile(
    rf"{_HOSPITAL_LABEL}\s*[:：=]\s*(.*?)"
    rf"(?=[\s,;]*(?:{_RATE_LABEL}|{_COUNT_LABEL})|\s+{_REPORT_WORDS}|\n|$|"
    rf"[\s,;]+{_NUMBER}\s*(?:%|퍼센트|건|개))"
)


def has_recordings_report_options(question: str) -> bool:
    """날짜 범위 뒤 조건만 붙인 요청도 리포트 경로로 연결한다."""

    return bool(_NATURAL_HOSPITAL.search(question) or re.search(
        rf"{_HOSPITAL_LABEL}\s*[:：=]|{_RATE_LABEL}|{_COUNT_LABEL}|"
        rf"{_NUMBER}\s*(?:%|퍼센트)|{_NUMBER}\s*(?:건|개)\s*(?:이상|초과|이하|미만)|"
        r"(?:병원|의원)(?:\([^)]*\))?만(?:\s|$)", question,
    ))


def _threshold(question: str, *, count: bool) -> float | int | None:
    label = _COUNT_LABEL if count else _RATE_LABEL
    unit = r"(?:건|개)" if count else r"(?:%|퍼센트)"
    values: list[float | int] = []
    consumed: list[tuple[int, int]] = []
    value_pattern = re.compile(
        rf"({_NUMBER})\s*(?:{unit})?\s*(이상|초과|이하|미만)?(?=\s|$|[,;])"
    )

    def add(match: re.Match[str]) -> None:
        if match.group(2) not in (None, "이상"):
            raise RecordingsReportOptionsError("감소율·감소량은 '이상' 기준으로 지정해줘")
        number = match.group(1).replace(",", "")
        if count:
            if "." in number:
                raise RecordingsReportOptionsError("최소 감소량은 0 이상의 정수로 알려줘")
            values.append(int(number))
        else:
            values.append(float(number))

    # 명시한 키의 값이 잘못됐을 때 기본값으로 조회하지 않는다.
    for field in re.finditer(label, question):
        # '30퍼센트'의 단위는 새로운 조건 키가 아니다.
        if not count and field.group().startswith("퍼센트") and re.search(
            rf"{_NUMBER}\s*$", question[:field.start()]
        ):
            continue
        tail = question[field.end():]
        prefix = re.match(r"\s*[:：=]?\s*", tail).end()
        match = value_pattern.match(tail, prefix)
        if match is None:
            name = "감소량" if count else "감소율"
            raise RecordingsReportOptionsError(f"{name} 값을 숫자로 지정해줘")
        add(match)
        consumed.append((field.end() + match.start(), field.end() + match.end()))
    # '30% 이상, 5건 이상'처럼 키를 생략한 표현도 같은 조건으로 해석한다.
    operation = r"(이상|초과|이하|미만)" + ("" if count else "?")
    generic = re.compile(rf"({_NUMBER})\s*{unit}\s*{operation}")
    for match in generic.finditer(question):
        if not any(start <= match.start() < end for start, end in consumed):
            add(match)
    if len(set(values)) > 1:
        raise RecordingsReportOptionsError("감소율과 최소 감소량은 각각 하나씩 지정해줘")
    return values[0] if values else None


def parse_recordings_report_options(question: str) -> RecordingsReportOptions:
    """병원 목록과 두 감소 조건만 추출하며 자동 보고 설정은 변경하지 않는다."""

    text = re.sub(r"<@[A-Z0-9]+>", "", question).replace("`", "")
    percent = _threshold(text, count=False)
    minimum = _threshold(text, count=True)
    # 날짜를 제거해 병원 필드가 날짜 앞에 있어도 날짜를 병원명에 포함하지 않는다.
    date_token = r"\d{4}[-./]\d{1,2}[-./]\d{1,2}"
    hospital_text = re.sub(
        rf"{date_token}\s*(?:~|～|〜|–|—|-|부터|to)\s*{date_token}(?:까지)?|{date_token}",
        "", text,
    )
    fields = list(_HOSPITAL_FIELD.finditer(hospital_text))
    if len(re.findall(rf"{_HOSPITAL_LABEL}\s*[:：=]", hospital_text)) > 1:
        raise RecordingsReportOptionsError("병원은 '병원: A병원, B병원'처럼 한 목록으로 지정해줘")
    hospitals: tuple[str, ...] = ()
    if fields:
        raw = fields[0].group(1).strip()
        hospitals = tuple(dict.fromkeys(name.strip().strip("\"'") for name in re.split(r"[,，]", raw)))
    elif re.search(rf"{_HOSPITAL_LABEL}\s*[:：=]", text):
        raise RecordingsReportOptionsError("'병원: 동탄제일병원'처럼 병원 이름을 지정해줘")
    else:
        # 띄어쓰기가 있는 공식 병원명·복수 병원은 명시적인 병원 필드를 사용한다.
        only = _NATURAL_HOSPITAL.findall(text)
        hospitals = tuple(dict.fromkeys(only))
    if hospitals in (("전체",), ("전체 병원",), ("모든 병원",), ("전체병원",)):
        hospitals = ()
    elif any(name in {"전체", "전체 병원", "모든 병원", "전체병원"} for name in hospitals):
        raise RecordingsReportOptionsError("전체 병원 또는 특정 병원 목록 중 하나로 지정해줘")
    return RecordingsReportOptions(hospitals=hospitals, drop_percent=percent, minimum_drop=minimum)
