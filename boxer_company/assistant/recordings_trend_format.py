"""병원·병실의 녹화와 신규 바코드 추이를 네 개의 CommonMark 항목으로 표시한다."""

from collections import Counter
from datetime import datetime
from typing import Any

from boxer_company.assistant.recordings_report_format import _label, _rate

# 각 항목은 최대 두 API 메시지에 담아 길어진 병실 목록이 뒤 항목을 밀어내지 않는다.
_SECTION_CHAR_BUDGET = 58_000
_SECTIONS = (
    ("① 병원별 녹화 추이", "hospitals", "totalCount", "건"),
    ("② 병원별 신규 바코드 추이", "hospitals", "newBarcodeCount", "개"),
    ("③ 병실별 녹화 추이", "rooms", "totalCount", "건"),
    ("④ 병실별 신규 바코드 추이", "rooms", "newBarcodeCount", "개"),
)


def _counts(values: list[int], unit: str) -> str:
    return " → ".join(f"{value:,}{unit}" for value in values)


def _entity_block(row: dict[str, Any], *, group: str, unit: str) -> str:
    # 동명 병원·병실도 서로 다른 ID의 시계열로 구별하며 DB 이름은 멘션으로 렌더링하지 않는다.
    key = row["key"]
    identity = f"병원 #{key[0]} · 병실 #{key[1]}" if group == "rooms" else f"병원 #{key or '미지정'}"
    lines = [f"**{_label(row['name'])}** ({identity})", f"주별: `{_counts(row['counts'], unit)}`"]
    if row["netDelta"] is None:
        lines.append("분석: 비교할 완료 주 부족")
    else:
        lines.append(
            f"분석: **{row['direction']}** · 첫 완료 주 대비 `{row['netDelta']:+,}{unit}` "
            f"(`{_rate(row['netRate'])}`) · 최근 전주 대비 `{row['latestDelta']:+,}{unit}` "
            f"(`{_rate(row['latestRate'])}`) · 조건 충족 연속 감소 `{row['declineStreak']}주`"
        )
    return "\n".join(lines)


def format_recordings_trend(summary: dict[str, Any], *, now: datetime) -> tuple[str, ...]:
    """동일 기간의 네 지표를 각각 분석하고 연속 감소 요청만 대상 필터를 적용한다."""

    weeks = summary["weeks"]
    names = summary["hospitalNames"]
    complete = [week for week in weeks if week["complete"]]
    scope_label = ", ".join(_label(name) for name in names) if names else "전체"
    if len(scope_label) > 3_000:
        # 긴 복수 병원 조건도 항목의 본문 예산을 침범하지 않게 대상 수를 표시한다.
        scope_label = f"선택한 {len(names):,}개 병원 (이름 목록은 길이 제한으로 생략)"
    periods = " · ".join(
        f"{index}주 `{week['startDate']}~{week['endDate']}`" + (" (부분 주/진행 중)" if not week["complete"] else "")
        for index, week in enumerate(weeks, 1)
    )
    header = (
        f"**기간** `{summary['startDate']} ~ {summary['endDate']}` (KST, 양 끝 포함)\n"
        "**대상 병원** " + scope_label +
        "\n**수량 순서** " + periods +
        "\n부분 주·진행 중인 주는 수량만 표시하고 증감·연속 감소 판정에서 제외해."
    )
    if len(complete) >= 2:
        header += (
            f"\n**전체 변화 비교** `{complete[0]['startDate']} ~ {complete[0]['endDate']}` → "
            f"`{complete[-1]['startDate']} ~ {complete[-1]['endDate']}`"
            f"\n**최근 전주 비교** `{complete[-2]['startDate']} ~ {complete[-2]['endDate']}` → "
            f"`{complete[-1]['startDate']} ~ {complete[-1]['endDate']}`"
        )
    required = summary["declineWeeks"]
    sections = []
    for title, group, metric, unit in _SECTIONS:
        all_rows = summary["entityTrends"][metric][group]
        rows = [row for row in all_rows if row["declineStreak"] >= required] if required else all_rows
        # 신규 0개인 곳과 증가·유지된 곳도 기본 분석에 포함한다. 감소 필터는 각 지표에 독립 적용한다.
        direction_counts = Counter(row["direction"] for row in rows)
        totals = [sum(row["counts"][index] for row in rows) for index in range(len(weeks))]
        lines = [f"**{title}**", header,
                 f"\n**분석 대상** `{len(rows):,}곳` / 기간 내 촬영 이력이 있는 대상 `{len(all_rows):,}곳`",
                 f"**분석 대상 주별 합계** `{_counts(totals, unit)}`",
                 "**흐름 분포** " + " · ".join(f"{label} `{direction_counts[label]:,}곳`" for label in (
                     "계속 증가", "계속 감소", "증가·보합", "감소·보합", "유지", "증감 혼재", "비교 불가",
                 )),
                 f"**연속 감소 기준** 매주 `{summary['dropPercent']:g}% 이상` · `{summary['minimumDrop']:,}{unit} 이상` 감소 (동일 수량 제외)."]
        if required:
            lines.append(f"**조회 조건** 마지막 완료 주까지 `{required}주 이상 연속 감소`한 대상만 분석해.")
        if metric == "newBarcodeCount":
            lines.append("신규 바코드는 전체 촬영 이력의 최초 1회를 최초 촬영 병원·병실에만 집계해.")
        if group == "rooms":
            unassigned = [week[metric] - sum(row[metric] for row in week[group].values()) for week in weeks]
            lines.append("녹화 당시 병실 기준이야. 병원·병실 미지정 기록은 병원 합계에만 포함해.")
            if any(unassigned):
                lines.append(f"병원·병실 미지정 주별 수량: `{_counts(unassigned, unit)}`")
        lines += ["", "**대상별 분석** (첫 완료 주 대비 변화량이 큰 순)"]
        body = "\n".join(lines)
        shown = 0
        # 행 전체 단위로 예산을 배분해 네 항목을 모두 남기고 생략한 수량은 명시한다.
        for row in rows:
            block = "\n\n" + _entity_block(row, group=group, unit=unit)
            if len(body) + len(block) > _SECTION_CHAR_BUDGET - 500:
                break
            body += block
            shown += 1
        if not rows:
            body += "\n해당 조건의 대상 없음."
        elif shown < len(rows):
            body += f"\n\n응답 길이 제한으로 분석 대상 {len(rows):,}곳 중 {shown:,}곳만 표시했어. 병원이나 기간을 좁히면 나머지 대상도 조회할 수 있어."
        body += f"\n\n증감 원인은 이 집계만으로 확정할 수 없어. 조회: {now:%Y-%m-%d %H:%M:%S} KST"
        sections.append(body)
    return tuple(sections)
