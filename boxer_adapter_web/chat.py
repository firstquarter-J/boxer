from __future__ import annotations

import re
import json
from typing import Any

from anthropic import Anthropic

from boxer.core import settings as core_settings
from boxer.retrieval.synthesis import _synthesize_retrieval_answer

from boxer_adapter_web.policies import HandoffPolicy
from boxer_adapter_web.schemas import ConversationSnapshotDto, MessageDto, SourceReferenceDto
from boxer_adapter_web.storage import WebChatStore
from boxer_adapter_web.workflows import WorkflowCatalog, WorkflowDefinition, WorkflowStep, resolve_localized_text

_HANGUL_PATTERN = re.compile(r"[가-힣ㄱ-ㅎㅏ-ㅣ]")
_SMALL_TALK_NORMALIZE_PATTERN = re.compile(r"[^0-9a-zA-Z가-힣ㄱ-ㅎㅏ-ㅣ\s]")
_HANDOFF_HINTS = (
    "상담원",
    "사람",
    "담당자",
    "cs",
    "고객센터",
    "직접 대화",
    "연결해",
    "연결",
    "support agent",
    "human",
    "representative",
    "agent",
)


class ChatService:
    def __init__(
        self,
        store: WebChatStore,
        workflow_catalog: WorkflowCatalog | None = None,
        handoff_policy: HandoffPolicy | None = None,
    ) -> None:
        self._store = store
        self._workflow_catalog = workflow_catalog or WorkflowCatalog.from_config({}, fallback_options=[])
        self._handoff_policy = handoff_policy or HandoffPolicy()
        # Claude는 client 객체가 필요하고, Ollama는 synthesis helper가 직접 HTTP를 친다.
        self._claude_client = (
            Anthropic(
                api_key=core_settings.ANTHROPIC_API_KEY,
                timeout=core_settings.ANTHROPIC_TIMEOUT_SEC,
            )
            if core_settings.LLM_PROVIDER == "claude" and core_settings.ANTHROPIC_API_KEY
            else None
        )

    def initialize_session(
        self,
        *,
        session_id: str | None,
        identity: dict[str, Any] | None,
        context: dict[str, Any] | None,
    ) -> ConversationSnapshotDto:
        conversation = self._store.get_or_create_conversation(
            session_id=session_id,
            identity=identity,
            context=context,
        )
        return self._to_snapshot(conversation)

    def handle_user_message(self, *, session_id: str, text: str) -> tuple[ConversationSnapshotDto, list[MessageDto]]:
        conversation = self._store.get_conversation_by_session_id(session_id)
        if not conversation:
            raise ValueError("Conversation not found")
        preferred_language = _resolve_preferred_language(conversation, question=text)

        created_messages: list[dict[str, Any]] = []
        created_messages.append(
            self._store.create_message(
                conversation["id"],
                sender_type="user",
                body=text,
                source_refs=[],
            )
        )

        status = str(conversation.get("status") or "starter")
        if status == "handoff_offered":
            if _looks_like_handoff_confirmation(text):
                return self._request_handoff_from_conversation(
                    conversation,
                    preferred_language=preferred_language,
                    created_messages=created_messages,
                )
            if _looks_like_handoff_decline(text):
                self._store.update_conversation_state(conversation["id"], status="ai_active")
                created_messages.append(
                    self._store.create_message(
                        conversation["id"],
                        sender_type="assistant",
                        body=_handoff_declined_message(preferred_language),
                        source_refs=[],
                    )
                )
                updated = self._store.get_conversation_by_id(conversation["id"])
                if not updated:
                    raise ValueError("Conversation not found after handoff decline")
                return self._to_snapshot(updated), [self._to_message(message) for message in created_messages]
            conversation = self._store.update_conversation_state(conversation["id"], status="ai_active") or conversation
            status = str(conversation.get("status") or "ai_active")

        if status in {"handoff_pending", "handoff_live"}:
            updated = self._store.get_conversation_by_id(conversation["id"])
            if not updated:
                raise ValueError("Conversation not found after handoff message")
            return self._to_snapshot(updated), [self._to_message(message) for message in created_messages]

        if _looks_like_handoff_request(text):
            return self._request_handoff_from_conversation(
                conversation,
                preferred_language=preferred_language,
                created_messages=created_messages,
            )

        if status == "closed":
            conversation = self._store.update_conversation_state(
                conversation["id"],
                status="starter",
                workflow_key=None,
                workflow_state_json="{}",
                assigned_admin_user_id=None,
                handoff_requested_at=None,
                handoff_started_at=None,
                closed_at=None,
            ) or conversation

        if str(conversation.get("status") or "starter") == "workflow_active":
            return self._handle_workflow_message(
                conversation,
                preferred_language=preferred_language,
                created_messages=created_messages,
            )

        if str(conversation.get("status") or "starter") == "starter":
            conversation = self._store.update_conversation_state(
                conversation["id"],
                status="ai_active",
            ) or conversation

        # 인사/감사/작별 인사는 FAQ 검색보다 즉시 응답이 자연스러워서 작은 고정 응답 경로로 먼저 처리한다.
        small_talk_response = _small_talk_response(text, preferred_language)
        search_results: list[Any] = []
        source_refs: list[dict[str, Any]] = []
        if small_talk_response is None:
            search_results = self._store.search_knowledge_documents(text, limit=5)
            source_refs = [
                {
                    "documentId": result.document.id,
                    "title": result.document.title,
                    "score": result.score,
                    "sourceUri": result.document.source_uri,
                }
                for result in search_results
            ]

        assistant_body = self._build_assistant_body(
            question=text,
            preferred_language=preferred_language,
            previous_messages=(conversation.get("messages") or []) + created_messages,
            small_talk_response=small_talk_response,
            source_refs=source_refs,
            search_results=search_results,
        )
        missing_evidence = small_talk_response is None and (
            not search_results or assistant_body == _insufficient_evidence_message(preferred_language)
        )
        created_messages.append(
            self._store.create_message(
                conversation["id"],
                sender_type="assistant",
                body=assistant_body,
                source_refs=source_refs,
            )
        )

        if missing_evidence and self._handoff_policy.on_missing_evidence:
            if self._handoff_policy.prompt_before_queue:
                self._store.update_conversation_state(conversation["id"], status="handoff_offered")
                created_messages.append(
                    self._store.create_message(
                        conversation["id"],
                        sender_type="assistant",
                        body=_handoff_offer_message(preferred_language),
                        source_refs=[],
                    )
                )
                updated = self._store.get_conversation_by_id(conversation["id"])
                if not updated:
                    raise ValueError("Conversation not found after handoff offer")
                return self._to_snapshot(updated), [self._to_message(message) for message in created_messages]
            return self._request_handoff_from_conversation(
                conversation,
                preferred_language=preferred_language,
                created_messages=created_messages,
            )

        updated = self._store.get_conversation_by_id(conversation["id"])
        if not updated:
            raise ValueError("Conversation not found after update")
        return self._to_snapshot(updated), [self._to_message(message) for message in created_messages]

    def start_workflow(self, *, session_id: str, workflow_key: str) -> tuple[ConversationSnapshotDto, list[MessageDto]]:
        conversation = self._store.get_conversation_by_session_id(session_id)
        if not conversation:
            raise ValueError("Conversation not found")
        workflow = self._workflow_catalog.get(workflow_key)
        preferred_language = _resolve_preferred_language(conversation, question=workflow_key)
        if workflow is None:
            created_messages = [
                self._store.create_message(
                    conversation["id"],
                    sender_type="assistant",
                    body=_unknown_workflow_message(preferred_language),
                    source_refs=[],
                )
            ]
            return self._request_handoff_from_conversation(
                conversation,
                preferred_language=preferred_language,
                created_messages=created_messages,
                include_notice=False,
            )

        created_messages = [
            self._store.create_message(
                conversation["id"],
                sender_type="user",
                body=resolve_localized_text(workflow.label, preferred_language, default=workflow.key),
                source_refs=[],
            )
        ]
        workflow_state = _build_initial_workflow_state(workflow)
        self._store.update_conversation_state(
            conversation["id"],
            status="workflow_active",
            workflow_key=workflow.key,
            workflow_state_json=_encode_json(workflow_state),
            assigned_admin_user_id=None,
            handoff_requested_at=None,
            handoff_started_at=None,
            closed_at=None,
        )
        created_messages.append(
            self._store.create_message(
                conversation["id"],
                sender_type="assistant",
                body=resolve_localized_text(workflow.steps[0].prompt, preferred_language),
                source_refs=[],
            )
        )
        updated = self._store.get_conversation_by_id(conversation["id"])
        if not updated:
            raise ValueError("Conversation not found after workflow start")
        return self._to_snapshot(updated), [self._to_message(message) for message in created_messages]

    def request_handoff(self, *, session_id: str, reason: str | None = None) -> tuple[ConversationSnapshotDto, list[MessageDto]]:
        conversation = self._store.get_conversation_by_session_id(session_id)
        if not conversation:
            raise ValueError("Conversation not found")
        preferred_language = _resolve_preferred_language(conversation, question=reason or "")
        created_messages: list[dict[str, Any]] = []
        if reason and reason.strip():
            created_messages.append(
                self._store.create_message(
                    conversation["id"],
                    sender_type="user",
                    body=reason.strip(),
                    source_refs=[],
                )
            )
        return self._request_handoff_from_conversation(
            conversation,
            preferred_language=preferred_language,
            created_messages=created_messages,
        )

    def end_session(self, *, session_id: str) -> tuple[ConversationSnapshotDto, list[MessageDto]]:
        conversation = self._store.get_conversation_by_session_id(session_id)
        if not conversation:
            raise ValueError("Conversation not found")
        preferred_language = _resolve_preferred_language(conversation, question="")
        created_messages = [
            self._store.create_message(
                conversation["id"],
                sender_type="system",
                body=_session_closed_message(preferred_language),
                source_refs=[],
            )
        ]
        updated = self._store.end_widget_session(conversation["id"])
        if not updated:
            raise ValueError("Conversation not found after session end")
        return self._to_snapshot(updated), [self._to_message(message) for message in created_messages]

    def _handle_workflow_message(
        self,
        conversation: dict[str, Any],
        *,
        preferred_language: str,
        created_messages: list[dict[str, Any]],
    ) -> tuple[ConversationSnapshotDto, list[MessageDto]]:
        workflow_key = str(conversation.get("workflow_key") or "").strip()
        workflow = self._workflow_catalog.get(workflow_key)
        if workflow is None:
            return self._request_handoff_from_conversation(
                conversation,
                preferred_language=preferred_language,
                created_messages=created_messages,
            )

        state = _decode_context(conversation.get("workflow_state_json"))
        current_index = int(state.get("currentStepIndex") or 0)
        answers = state.get("answers") if isinstance(state.get("answers"), dict) else {}
        if current_index < len(workflow.steps):
            current_step = workflow.steps[current_index]
            answer = str(created_messages[-1]["body"] or "").strip()
            if _is_skip_answer(answer) and current_step.skip_allowed:
                answers[current_step.field] = None
            else:
                validation_message = _validate_workflow_answer(current_step, answer, preferred_language)
                if validation_message:
                    state.update(
                        {
                            "workflowKey": workflow.key,
                            "currentStepIndex": current_index,
                            "answers": answers,
                        }
                    )
                    self._store.update_conversation_state(
                        conversation["id"],
                        workflow_state_json=_encode_json(state),
                    )
                    created_messages.append(
                        self._store.create_message(
                            conversation["id"],
                            sender_type="assistant",
                            body=validation_message,
                            source_refs=[],
                        )
                    )
                    updated = self._store.get_conversation_by_id(conversation["id"])
                    if not updated:
                        raise ValueError("Conversation not found after workflow retry")
                    return self._to_snapshot(updated), [self._to_message(message) for message in created_messages]
                answers[current_step.field] = answer
                if current_step.action:
                    # action hook은 alpha 단계에서 실행하지 않고 state에 남겨 adapter 확장 지점으로 사용한다.
                    actions = state.get("actions") if isinstance(state.get("actions"), list) else []
                    actions.append({"step": current_step.field, "action": current_step.action, "value": answer})
                    state["actions"] = actions

        next_index = _resolve_next_workflow_index(workflow, current_index, created_messages[-1]["body"])
        state.update(
            {
                "workflowKey": workflow.key,
                "currentStepIndex": next_index,
                "answers": answers,
            }
        )
        if next_index < len(workflow.steps):
            self._store.update_conversation_state(
                conversation["id"],
                workflow_state_json=_encode_json(state),
            )
            created_messages.append(
                self._store.create_message(
                    conversation["id"],
                    sender_type="assistant",
                    body=resolve_localized_text(workflow.steps[next_index].prompt, preferred_language),
                    source_refs=[],
                )
            )
            updated = self._store.get_conversation_by_id(conversation["id"])
            if not updated:
                raise ValueError("Conversation not found after workflow step")
            return self._to_snapshot(updated), [self._to_message(message) for message in created_messages]

        state["completed"] = True
        self._store.update_conversation_state(
            conversation["id"],
            workflow_state_json=_encode_json(state),
        )
        created_messages.append(
            self._store.create_message(
                conversation["id"],
                sender_type="assistant",
                body=_workflow_completion_message(workflow, preferred_language),
                source_refs=[],
            )
        )
        return self._request_handoff_from_conversation(
            conversation,
            preferred_language=preferred_language,
            created_messages=created_messages,
            include_notice=False,
        )

    def _request_handoff_from_conversation(
        self,
        conversation: dict[str, Any],
        *,
        preferred_language: str,
        created_messages: list[dict[str, Any]],
        include_notice: bool = True,
    ) -> tuple[ConversationSnapshotDto, list[MessageDto]]:
        current_status = str(conversation.get("status") or "starter")
        if current_status in {"handoff_pending", "handoff_live"}:
            updated = self._store.get_conversation_by_id(conversation["id"])
            if not updated:
                raise ValueError("Conversation not found during active handoff")
            return self._to_snapshot(updated), [self._to_message(message) for message in created_messages]

        self._store.request_handoff(conversation["id"])
        if include_notice:
            created_messages.append(
                self._store.create_message(
                    conversation["id"],
                    sender_type="assistant",
                    body=_handoff_notice_message(preferred_language),
                    source_refs=[],
                )
            )
        updated = self._store.get_conversation_by_id(conversation["id"])
        if not updated:
            raise ValueError("Conversation not found after handoff request")
        return self._to_snapshot(updated), [self._to_message(message) for message in created_messages]

    def _build_assistant_body(
        self,
        *,
        question: str,
        preferred_language: str,
        previous_messages: list[dict[str, Any]],
        small_talk_response: str | None,
        source_refs: list[dict[str, Any]],
        search_results: list[Any],
    ) -> str:
        _ = source_refs
        if small_talk_response:
            return small_talk_response

        if not search_results:
            return _missing_evidence_message(preferred_language)

        evidence_payload = [
            {
                "documentId": result.document.id,
                "title": result.document.title,
                "sourceUri": result.document.source_uri,
                "content": result.document.content,
            }
            for result in search_results
        ]
        answer = _synthesize_retrieval_answer(
            question=question,
            thread_context=_render_thread_context(previous_messages),
            evidence_payload=evidence_payload,
            provider=core_settings.LLM_PROVIDER,
            claude_client=self._claude_client,
            system_prompt=(
                "You are a retrieval-grounded support assistant. "
                "Answer only from the provided FAQ evidence. "
                "Do not invent product behavior or policy. "
                f"{_language_instruction(preferred_language)}"
            ),
            extra_rules=_language_extra_rules(preferred_language),
        ).strip()

        if answer:
            return answer
        return _insufficient_evidence_message(preferred_language)

    def _to_snapshot(self, conversation: dict[str, Any]) -> ConversationSnapshotDto:
        messages = [self._to_message(message) for message in conversation.get("messages") or []]
        last_message_preview = messages[-1].body if messages else conversation.get("last_message_preview")
        return ConversationSnapshotDto(
            id=conversation["id"],
            sessionId=conversation["session_id"],
            customerId=conversation.get("customer_id"),
            customerName=conversation.get("customer_name"),
            customerEmail=conversation.get("customer_email"),
            context=_decode_context(conversation.get("context_json")),
            status=str(conversation.get("status") or "starter"),
            workflowKey=conversation.get("workflow_key"),
            workflowState=_decode_context(conversation.get("workflow_state_json")),
            assignedAdminUserId=conversation.get("assigned_admin_user_id"),
            assignedAdminUserName=conversation.get("assigned_admin_user_name"),
            handoffRequestedAt=conversation.get("handoff_requested_at"),
            handoffStartedAt=conversation.get("handoff_started_at"),
            closedAt=conversation.get("closed_at"),
            lastMessagePreview=last_message_preview,
            createdAt=conversation["created_at"],
            updatedAt=conversation["updated_at"],
            messages=messages,
        )

    def _to_message(self, message: dict[str, Any]) -> MessageDto:
        return MessageDto(
            id=message["id"],
            senderType=message["sender_type"],
            senderName=message.get("sender_name"),
            body=message["body"],
            sourceRefs=[
                SourceReferenceDto(
                    documentId=str(reference["documentId"]),
                    title=str(reference["title"]),
                    score=float(reference["score"]),
                    sourceUri=str(reference["sourceUri"]),
                )
                for reference in message.get("source_refs") or []
            ],
            createdAt=message["created_at"],
        )


def _render_thread_context(messages: list[dict[str, Any]]) -> str:
    if not core_settings.LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT:
        return ""

    rendered_lines: list[str] = []
    for message in messages[-max(1, core_settings.THREAD_CONTEXT_MAX_MESSAGES) :]:
        sender_type = str(message.get("sender_type") or "system")
        body = str(message.get("body") or "").strip()
        if not body:
            continue
        rendered_lines.append(f"{sender_type}: {body}")
    return "\n".join(rendered_lines).strip()


def _decode_context(raw_context: Any) -> dict[str, Any]:
    if isinstance(raw_context, dict):
        return raw_context
    if not raw_context:
        return {}
    import json

    try:
        loaded = json.loads(str(raw_context))
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _encode_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _build_initial_workflow_state(workflow: WorkflowDefinition) -> dict[str, Any]:
    return {
        "workflowKey": workflow.key,
        "currentStepIndex": 0,
        "answers": {},
        "completed": False,
    }


def _validate_workflow_answer(step: WorkflowStep, answer: str, preferred_language: str) -> str | None:
    normalized_answer = str(answer or "").strip()
    if not normalized_answer and step.required and not step.skip_allowed:
        return resolve_localized_text(
            step.retry_prompt,
            preferred_language,
            default=_workflow_retry_message(preferred_language),
        )

    if step.choices:
        normalized_choices = {choice.lower() for choice in step.choices}
        if normalized_answer.lower() not in normalized_choices:
            choices = ", ".join(step.choices)
            return resolve_localized_text(
                step.retry_prompt,
                preferred_language,
                default=_workflow_choice_retry_message(preferred_language, choices),
            )

    if step.validation_regex:
        try:
            is_valid = re.search(step.validation_regex, normalized_answer) is not None
        except re.error:
            is_valid = True
        if not is_valid:
            return resolve_localized_text(
                step.retry_prompt,
                preferred_language,
                default=_workflow_retry_message(preferred_language),
            )

    return None


def _resolve_next_workflow_index(workflow: WorkflowDefinition, current_index: int, answer: str) -> int:
    next_index = current_index + 1
    if current_index >= len(workflow.steps):
        return next_index

    current_step = workflow.steps[current_index]
    branches = current_step.branches or {}
    target_field = branches.get(str(answer or "").strip().lower())
    if not target_field:
        return next_index

    for index, step in enumerate(workflow.steps):
        if step.field == target_field:
            return index
    return next_index


def _is_skip_answer(answer: str) -> bool:
    return str(answer or "").strip().lower() in {"skip", "건너뛰기", "패스"}


def _workflow_retry_message(preferred_language: str) -> str:
    if preferred_language == "ko":
        return "입력 형식이 맞지 않아. 다시 입력해 줘."
    return "That input does not match the expected format. Please try again."


def _workflow_choice_retry_message(preferred_language: str, choices: str) -> str:
    if preferred_language == "ko":
        return f"아래 선택지 중 하나로 입력해 줘: {choices}"
    return f"Please choose one of these options: {choices}"


def _workflow_completion_message(workflow: WorkflowDefinition, preferred_language: str) -> str:
    completion_message = resolve_localized_text(workflow.completion_message, preferred_language)
    if completion_message:
        return completion_message
    if preferred_language == "ko":
        return "필요한 정보를 접수했어. 상담원이 확인하고 이어서 안내할게."
    return "I captured the required details. A support representative will continue from here."


def _handoff_notice_message(preferred_language: str) -> str:
    if preferred_language == "ko":
        return "상담원 연결을 요청했어. 이제 상담원이 확인하고 이어서 답변할게."
    return "I requested support handoff. A representative will continue from here."


def _handoff_offer_message(preferred_language: str) -> str:
    if preferred_language == "ko":
        return "도움말 근거로는 확답하기 어려워. 상담원에게 연결할까?"
    return "I could not answer from the help center evidence. Would you like me to connect support?"


def _handoff_declined_message(preferred_language: str) -> str:
    if preferred_language == "ko":
        return "알겠어. 다른 질문이나 선택지를 보내 주면 도움말 기준으로 다시 확인할게."
    return "Understood. Send another question or choose an option and I will check the help center again."


def _unknown_workflow_message(preferred_language: str) -> str:
    if preferred_language == "ko":
        return "선택한 문의 유형 설정을 찾지 못했어. 상담원이 확인할 수 있게 연결할게."
    return "I could not find that workflow configuration. I will route this to support."


def _session_closed_message(preferred_language: str) -> str:
    if preferred_language == "ko":
        return "상담이 종료됐어. 새 문의는 다시 시작해 줘."
    return "This conversation has ended. Start a new session for another question."


def _looks_like_handoff_request(text: str) -> bool:
    normalized = str(text or "").lower().strip()
    if not normalized:
        return False
    return any(hint in normalized for hint in _HANDOFF_HINTS)


def _looks_like_handoff_confirmation(text: str) -> bool:
    normalized = str(text or "").lower().strip()
    return _looks_like_handoff_request(normalized) or normalized in {"네", "예", "응", "ㅇㅇ", "yes", "y", "ok", "okay"}


def _looks_like_handoff_decline(text: str) -> bool:
    normalized = str(text or "").lower().strip()
    return normalized in {"아니", "아니요", "ㄴㄴ", "no", "n", "nope"}


def _resolve_preferred_language(conversation: dict[str, Any], *, question: str) -> str:
    context = _decode_context(conversation.get("context_json"))
    direct_language = _normalize_language(context.get("language"))
    if direct_language:
        return direct_language

    metadata = context.get("metadata")
    if isinstance(metadata, dict):
        metadata_language = _normalize_language(metadata.get("language")) or _normalize_language(metadata.get("locale"))
        if metadata_language:
            return metadata_language

    if _HANGUL_PATTERN.search(question):
        return "ko"
    return "en"


def _normalize_language(value: Any) -> str | None:
    normalized = str(value or "").strip().lower()
    if normalized.startswith("ko"):
        return "ko"
    if normalized.startswith("en"):
        return "en"
    return None


def _language_instruction(preferred_language: str) -> str:
    if preferred_language == "ko":
        return "Always answer in Korean."
    return "Always answer in English."


def _language_extra_rules(preferred_language: str) -> str:
    if preferred_language == "ko":
        return "7) Write the final answer in Korean."
    return "7) Write the final answer in English."


def _missing_evidence_message(preferred_language: str) -> str:
    if preferred_language == "ko":
        return "질문과 연결된 도움말 근거를 찾지 못했습니다."
    return "I could not find grounded help content for that question."


def _insufficient_evidence_message(preferred_language: str) -> str:
    if preferred_language == "ko":
        return "관련 FAQ는 찾았지만, 근거만으로 답변을 확정하지 못했습니다."
    return "I found related FAQ content, but I could not produce a grounded answer."


def _small_talk_response(question: str, preferred_language: str) -> str | None:
    normalized = _normalize_small_talk(question)
    if not normalized:
        return None

    if normalized in _greeting_variants():
        if preferred_language == "ko":
            return "안녕하세요. 도움말 문서를 기준으로 답변할게요. 궁금한 내용을 보내 주세요."
        return "Hello. I can help using your support docs. Tell me what you need."

    if normalized in _thanks_variants():
        if preferred_language == "ko":
            return "천만에요. 더 필요한 내용이 있으면 이어서 물어봐 주세요."
        return "You're welcome. Ask another question whenever you're ready."

    if normalized in _goodbye_variants():
        if preferred_language == "ko":
            return "언제든 다시 질문해 주세요."
        return "Feel free to come back with another question anytime."

    return None


def _normalize_small_talk(text: str) -> str:
    lowered = str(text or "").lower().strip()
    lowered = _SMALL_TALK_NORMALIZE_PATTERN.sub(" ", lowered)
    tokens = [token for token in lowered.split() if token not in {"boxer", "bot"}]
    return " ".join(tokens)


def _greeting_variants() -> set[str]:
    return {
        "hi",
        "hello",
        "hey",
        "good morning",
        "good afternoon",
        "good evening",
        "안녕",
        "안녕하세요",
        "ㅎㅇ",
        "ㅎㅇㅎㅇ",
        "하이",
        "하이요",
        "하잉",
        "헬로",
        "반가워",
        "반가워요",
        "반갑습니다",
    }


def _thanks_variants() -> set[str]:
    return {
        "thanks",
        "thank you",
        "thx",
        "ty",
        "appreciate it",
        "고마워",
        "고마워요",
        "감사",
        "감사해",
        "감사해요",
        "감사합니다",
        "ㄱㅅ",
    }


def _goodbye_variants() -> set[str]:
    return {
        "bye",
        "goodbye",
        "see you",
        "see ya",
        "later",
        "잘가",
        "안녕히가세요",
        "안녕히계세요",
        "다음에 봐",
        "다음에 봬요",
        "바이",
    }
