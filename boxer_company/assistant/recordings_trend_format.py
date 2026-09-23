"""주별 추이의 실제 기간·불완전한 주·연속 감소 근거를 CommonMark로 표현한다."""

from datetime import datetime
from itertools import pairwise
from typing import Any

from boxer_company.assistant.recordings_report_format import _label, _rate
from boxer_company.weekly_recordings_report import _weekly_recordings_report_change_rate


def format_recordings_trend(summary: dict[str, Any], *, now: datetime) -> tuple[str, ...]:
    weeks = summary["weeks"]
    names = summary["hospitalNames"]
    header = (
        f"**기간** `{summary['startDate']} ~ {summary['endDate']}` (KST, 양 끝 포함)\n"
        "**대상 병원** " + (", ".join(_label(name) for name in names) if names else "전체")
    )
    lines = ["**녹화·신규 바코드 주별 추이**", header,
             "월~일 기준이야. 부분 주·진행 중인 주는 증감률과 연속 감소 판정에서 제외해.", ""]
    for index, week in enumerate(weeks):
        marker = "" if week["complete"] else " · 부분 주/진행 중"
        lines.append(f"**{week['startDate']} ~ {week['endDate']}{marker}**")
        values = []
        for metric, label, unit in (("totalCount", "녹화", "건"), ("newBarcodeCount", "신규 바코드", "개")):
            value = f"{label} `{week[metric]:,}{unit}`"
            if index > 0 and week["complete"] and weeks[index - 1]["complete"]:
                previous = weeks[index - 1][metric]
                value += f" (전주 대비 `{week[metric] - previous:+,}{unit}`, `{_rate(_weekly_recordings_report_change_rate(week[metric], previous))}`)"
            values.append(value)
        lines.append(" · ".join(values))
    complete = [week for week in weeks if week["complete"]]
    lines += ["", "**흐름 요약**"]
    if len(complete) < 2:
        lines.append("완료된 온전한 주가 2개 미만이라 주간 추이 비교는 아직 불가해.")
    else:
        first, last = complete[0], complete[-1]
        lines.append(f"비교: `{first['startDate']} ~ {first['endDate']}` → `{last['startDate']} ~ {last['endDate']}`")
        for metric, label, unit in (("totalCount", "녹화", "건"), ("newBarcodeCount", "신규 바코드", "개")):
            counts = [week[metric] for week in complete]
            changes = [right - left for left, right in pairwise(counts)]
            direction = "계속 증가" if all(c > 0 for c in changes) else "계속 감소" if all(c < 0 for c in changes) else "유지" if all(c == 0 for c in changes) else "증감 혼재"
            peak = max(complete, key=lambda week: week[metric])
            lines.append(
                f"• {label}: {direction} · `{first[metric]:,}{unit} → {last[metric]:,}{unit}` "
                f"(`{_rate(_weekly_recordings_report_change_rate(last[metric], first[metric]))}`) · "
                f"최다 `{peak['startDate']} ~ {peak['endDate']}` `{peak[metric]:,}{unit}`"
            )
    lines += ["", "신규 바코드는 전체 촬영 이력 중 최초 1회만 집계해. 증감 원인은 이 집계만으로 확정할 수 없어.",
              f"조회: {now:%Y-%m-%d %H:%M:%S} KST"]

    required = summary["declineWeeks"] or 1
    drops = [f"**{required}주 이상 연속 감소한 병원·진료실**", header,
             "조회 기간 내 마지막 완료 주까지 이어진 전주 대비 감소만 포함해.",
             f"매주 감소 기준: `{summary['dropPercent']:g}% 이상` · `{summary['minimumDrop']:,}건/개 이상` (동일 수량 제외).",
             f"{required}회 감소를 확인하려면 온전한 주 {required + 1}개가 필요해."]
    for metric, label, unit in (("totalCount", "녹화", "건"), ("newBarcodeCount", "신규 바코드", "개")):
        for group, group_label in (("hospitals", "병원"), ("rooms", "진료실")):
            rows = summary["declines"][metric][group]
            drops += ["", f"**{label} 감소 {group_label}** `{len(rows):,}곳`"]
            # 조건 조회는 모든 일치 대상을 보존하고 일반 추이는 상위 10곳으로 요약한다.
            selected = rows if summary["declineWeeks"] else rows[:10]
            for row in selected:
                counts = " → ".join(f"{count:,}{unit}" for count in row["counts"])
                drops.append(f"• {_label(row['name'])}: {row['streak']}주 연속 · `{counts}` "
                             f"(`{_rate(row['changeRate'])}`) · `{row['startDate']} ~ {row['endDate']}`")
            if not rows:
                drops.append("• 해당 조건의 대상 없음" if len(complete) >= required + 1 else "• 비교할 완료 주 부족")
            elif len(selected) < len(rows):
                drops.append(f"• 상위 {len(selected)}곳 표시. 전체 대상은 '{required}주 이상 감소'로 조회해줘.")
    return "\n".join(lines), "\n".join(drops)
