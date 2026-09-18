"""병원·진료실의 녹화와 신규 바코드를 네 개의 CommonMark 메시지로 표시한다."""

import re
from datetime import datetime
from typing import Any


def _label(value: object) -> str:
    # DB의 이름은 서식이나 멘션으로 해석되지 않게 일반 텍스트로 표시한다.
    text = " ".join(str(value or "미확인").split())
    return re.sub(r"([\\`*_\[\]<>])", r"\\\1", text)


def _rate(value: float | None) -> str:
    return "비교 불가" if value is None else f"{value:+.1f}%"


def _changes(rows: list[dict[str, Any]], *, unit: str) -> list[str]:
    lines = []
    for index, row in enumerate(rows, 1):
        name = _label(row["hospitalName"])
        if "roomName" in row:
            name += f" · {_label(row['roomName'])}"
        lines.append(
            f"{index}. {name} `{row['previousCount']:,}{unit} → "
            f"{row['currentCount']:,}{unit}` · `{row['delta']:+,}{unit}` "
            f"(`{_rate(row['changeRate'])}`)"
        )
    return lines or ["• 없어"]


def format_recordings_report_sections(
    summary: dict[str, Any], *, now: datetime,
) -> tuple[str, ...]:
    """주간과 명시 기간 모두 실제 날짜 범위와 직전 동일 일수 비교를 보여준다."""

    period = (
        f"**기간** `{summary['weekStartDate']} ~ {summary['weekEndDate']}`\n"
        f"**비교 기간** `{summary['previousWeekStartDate']} ~ "
        f"{summary['previousWeekEndDate']}`\n"
        "비교 기간은 바로 앞의 같은 일수야. 날짜는 양 끝을 포함해."
    )
    # 결과를 공유해도 적용한 대상과 임계값을 다시 확인할 수 있도록 함께 표시한다.
    options = summary.get("queryOptions", {})
    names = options.get("hospitalNames", [])
    period += "\n**대상 병원** " + (", ".join(_label(name) for name in names) if names else "전체")
    drop_percent = f"{options.get('dropPercent', 50):g}"
    minimum_drop = options.get("minimumDrop", 3)
    hospital_minimum = options.get("hospitalRecordingMinimumDrop", minimum_drop)

    def drop_criteria(unit: str, minimum: int = minimum_drop) -> str:
        return f"직전 기간 대비 {drop_percent}% 이상 · 감소량 {minimum:,}{unit} 이상"
    footer = f"조회: {now:%Y-%m-%d %H:%M:%S} KST"
    if summary["weekEndDate"] >= now.date().isoformat():
        footer += "\n※ 종료되지 않은 기간이므로 현재까지의 집계야."

    def section(title: str, lines: list[str]) -> str:
        return f"**{title}**\n{period}\n\n" + "\n".join(lines) + f"\n\n{footer}"

    hospital_lines = [
        f"**총 녹화** `{summary['totalCount']:,}건` · 병원 `{summary['hospitalCount']:,}곳`",
        (f"**직전 기간 대비** `{summary['previousTotalCount']:,}건 → {summary['totalCount']:,}건` "
         f"· `{summary['totalDelta']:+,}건` (`{_rate(summary['totalChangeRate'])}`)"),
        "", "**녹화 상위 병원**",
    ]
    hospital_lines += [
        f"{index}. {_label(row['hospitalName'])} `{row['rowCount']:,}건`"
        for index, row in enumerate(summary["topRows"], 1)
    ] or ["• 없어"]
    hospital_lines += [
        "", "**급증 기준** 2배 이상 · 증가량 20건 이상",
        "**급감 기준** " + drop_criteria("건", hospital_minimum),
    ]
    for label, key in (("급증", "surge"), ("급감", "drop")):
        rows = summary[f"{key}Rows"]
        hospital_lines += ["", f"**{label} 병원** `{summary[f'{key}Count']:,}곳`"]
        hospital_lines += _changes(rows, unit="건")
        if summary[f"{key}Count"] > len(rows):
            hospital_lines.append(f"• 상위 {len(rows):,}곳만 표시")

    room_lines = [
        f"**급감 진료실** `{summary['roomDropCount']:,}곳`",
        "**기준** " + drop_criteria("건"), "",
    ] + _changes(summary["roomDropRows"], unit="건")

    new = summary["newBarcodes"]
    definition = "**신규 기준** 같은 바코드의 전체 이력 중 최초 촬영 1회만 최초 촬영 병원·진료실에 집계해."
    new_lines = [
        f"**총 신규 바코드** `{new['totalCount']:,}개` · 병원 `{new['hospitalCount']:,}곳`",
        (f"**직전 기간 대비** `{new['previousTotalCount']:,}개 → {new['totalCount']:,}개` "
         f"· `{new['totalDelta']:+,}개` (`{_rate(new['totalChangeRate'])}`)"),
        definition, "", "**신규 바코드 상위 병원**",
    ]
    new_lines += [
        f"{index}. {_label(row['hospitalName'])} `{row['rowCount']:,}개`"
        for index, row in enumerate(new["topRows"], 1)
    ] or ["• 없어"]
    new_lines += [
        "", f"**급감 병원** `{new['dropCount']:,}곳`",
        "**기준** " + drop_criteria("개"), "",
    ] + _changes(new["dropRows"], unit="개")
    new_room_lines = [
        definition, "", f"**급감 진료실** `{new['roomDropCount']:,}곳`",
        "**기준** " + drop_criteria("개"), "",
    ] + _changes(new["roomDropRows"], unit="개")
    # 두 지표는 각각 판정한다. 녹화가 유지돼도 신규 바코드만 급감하면 남긴다.
    return (
        section("① 병원별 녹화 요약", hospital_lines),
        section("② 진료실별 녹화 급감", room_lines),
        section("③ 병원별 신규 바코드 요약", new_lines),
        section("④ 진료실별 신규 바코드 급감", new_room_lines),
    )
