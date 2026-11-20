from datetime import date
from unittest.mock import Mock

import pytest

from boxer.answering import AnswerEngine, AnswerResult
from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.natural_language_read import (
    NATURAL_LANGUAGE_READ_TIMEOUT_SEC,
    NaturalLanguageReadDecision,
    NaturalLanguageReadError,
    NaturalLanguageReadInterpreter,
)


def _request(question, *entries, metadata=None):
    return CompanyAssistantRequest(
        "request",
        "workspace",
        "author",
        "slack",
        "thread",
        question,
        "ko",
        context_entries=entries,
        metadata=metadata or {},
    )


def _entry(text, *, author="author", created_at="1795248000.000001"):
    return {
        "kind": "message",
        "source": "slack",
        "author_id": author,
        "text": text,
        "created_at": created_at,
    }


def _interpreter(output):
    engine = Mock(spec=AnswerEngine)
    engine.answer.return_value = AnswerResult(output, "claude", True)
    return NaturalLanguageReadInterpreter(engine), engine


def test_count_preserves_explicit_date_and_does_not_send_barcode_or_thread():
    interpreter, engine = _interpreter('{"intent":"video_count"}')
    result = interpreter.interpret(
        _request(
            "23456789012로 2026-11-20에 영상 몇 건 남았어?",
            _entry("34567890123로 찍힌 영상 알려줘"),
            metadata={"barcode": "45678901234"},
        )
    )

    assert result == NaturalLanguageReadDecision(
        "video_count",
        "23456789012",
        date(2026, 11, 20),
    )
    model_request = engine.answer.call_args.args[0]
    assert model_request.timeout_sec == NATURAL_LANGUAGE_READ_TIMEOUT_SEC
    assert not model_request.context_entries
    assert model_request.evidence == {
        "current_message": "[바코드]로 2026-11-20에 영상 몇 건 남았어?",
    }


def test_relative_date_uses_one_deployment_clock_and_same_author(monkeypatch):
    interpreter, engine = _interpreter('{"intent":"recorded_on_date"}')
    today = Mock(return_value=date(2026, 11, 21))
    monkeypatch.setattr(
        "boxer_company.assistant.natural_language_read._current_local_date", today
    )
    result = interpreter.interpret(
        _request(
            "그럼 어제는?",
            _entry("23456789012 녹화된 적 있어?"),
            _entry("34567890123은?", author="teammate"),
            _entry("그럼 어제는?"),
        )
    )

    assert result == NaturalLanguageReadDecision(
        "recorded_on_date",
        "23456789012",
        date(2026, 11, 20),
    )
    today.assert_called_once_with()
    assert engine.answer.call_args.args[0].evidence == {
        "current_message": "그럼 어제는?",
        "previous_same_actor_message": "[바코드] 녹화된 적 있어?",
    }


def test_followup_does_not_reinterpret_a_previous_yearless_date(monkeypatch):
    interpreter, _ = _interpreter('{"intent":"recorded_on_date"}')
    monkeypatch.setattr(
        "boxer_company.assistant.natural_language_read._current_local_date",
        lambda: date(2027, 1, 1),
    )

    result = interpreter.interpret(
        _request(
            "그럼 이걸로 찍혔어?",
            _entry("23456789012 12월 31일 녹화됐어?", created_at="1798675200.000001"),
        )
    )

    assert result and result.intent == "clarify"
    assert result.target_date is None


def test_bare_barcode_only_completes_a_missing_barcode_of_a_recent_read():
    interpreter, engine = _interpreter('{"intent":"video_count"}')
    result = interpreter.interpret(
        _request(
            "23456789012",
            _entry("녹화가 몇 건 있어?"),
        )
    )
    assert result == NaturalLanguageReadDecision("video_count", "23456789012")

    result = interpreter.interpret(
        _request(
            "23456789012",
            _entry("34567890123 녹화가 몇 건 있어?"),
        )
    )
    assert result and result.intent == "clarify"

    engine.reset_mock()
    result = interpreter.interpret(
        _request(
            "23456789012",
            _entry("녹화가 몇 건 있어?", author="teammate"),
        )
    )
    assert result and result.intent == "clarify" and not result.used_llm
    engine.answer.assert_not_called()


def test_referential_question_can_use_an_explicit_barcode_from_the_same_author():
    interpreter, _ = _interpreter('{"intent":"video_count"}')
    result = interpreter.interpret(_request("이걸로 몇 건이야?", _entry("23456789012")))
    assert result == NaturalLanguageReadDecision("video_count", "23456789012")


@pytest.mark.parametrize(
    "condition",
    [
        "지난달",
        "최근",
        "최근 3일",
        "월요일",
        "어제부터 오늘까지",
        "10월 혹은 오늘",
        "2026-02-30",
        "2026-11-19 혹은 2026-11-20",
    ],
)
def test_unresolved_date_does_not_default_to_an_unbounded_count(condition):
    interpreter, _ = _interpreter('{"intent":"video_count"}')
    result = interpreter.interpret(_request(f"23456789012 {condition} 영상 몇 개?"))
    assert result and result.intent == "clarify" and result.used_llm
    assert result.clarification and "날짜" in result.clarification


@pytest.mark.parametrize("time_text", ["오전", "오후 3시에", "14:15"])
def test_specified_date_does_not_drop_the_time_of_day(time_text):
    interpreter, _ = _interpreter('{"intent":"recorded_on_date"}')
    result = interpreter.interpret(
        _request(f"23456789012 2026-11-20 {time_text} 녹화됐어?")
    )
    assert result and result.intent == "clarify"
    assert result.target_date is None


def test_multiple_barcodes_do_not_select_the_first_identifier():
    interpreter, _ = _interpreter('{"intent":"video_count"}')
    result = interpreter.interpret(
        _request("23456789012 아니면 34567890123 영상 몇 개?")
    )
    assert result and result.intent == "clarify" and not result.barcode


@pytest.mark.parametrize(
    "output",
    [
        '{"intent":"video_count","barcode":"23456789012"}',
        '{"intent":"recorded_on_date","target_date":"2026-11-20"}',
        '{"intent":"playbook","clarification":"free text"}',
        '{"intent":"video_count","intent":"playbook"}',
        '{"intent":"operations"}',
        '{"intent":true}',
        '[{"intent":"playbook"}]',
        '```json\n{"intent":"playbook"}\n```',
    ],
)
def test_model_must_return_only_one_allowlisted_intent(output):
    interpreter, _ = _interpreter(output)
    with pytest.raises(NaturalLanguageReadError, match="^invalid_output$"):
        interpreter.interpret(_request("영상이 몇 개 찍혔는지 알아봐줘"))


def test_provider_timeout_uses_safe_code_and_unhandled_is_no_match():
    interpreter, engine = _interpreter('{"intent":"unhandled"}')
    assert interpreter.interpret(_request("오늘 회의 예약해줘")) is None

    engine.answer.side_effect = TimeoutError("sensitive provider message")
    with pytest.raises(NaturalLanguageReadError, match="^timeout$"):
        interpreter.interpret(_request("영상이 몇 개 찍혔어?"))
