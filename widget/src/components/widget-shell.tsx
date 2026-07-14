import { useEffect, useLayoutEffect, useRef, useState, type KeyboardEvent } from "react";

import type {
  ConversationSnapshot,
  QuickActionEntry,
  StarterEntry,
  WidgetLanguage,
  WidgetTimeZoneMap,
} from "../app/types";

interface WidgetShellProps {
  conversation: ConversationSnapshot | null;
  draft: string;
  connectionState: string;
  isSending: boolean;
  language: WidgetLanguage;
  quickActions: QuickActionEntry[];
  unreadCount: number;
  welcomeTitle?: string | null;
  welcomeMessage?: string | null;
  starterEntries: StarterEntry[];
  welcomeTimeZones?: WidgetTimeZoneMap;
  onDraftChange: (value: string) => void;
  onLanguageChange: (language: WidgetLanguage) => void;
  onQuickActionSelect: (action: QuickActionEntry) => void;
  onStarterOptionSelect: (entry: StarterEntry) => void;
  onHandoffRequest: () => void;
  onEndSession: () => void;
  onSend: () => void;
}

export function WidgetShell({
  conversation,
  draft,
  connectionState,
  isSending,
  language,
  quickActions,
  unreadCount,
  welcomeTitle,
  welcomeMessage,
  starterEntries,
  welcomeTimeZones,
  onDraftChange,
  onLanguageChange,
  onQuickActionSelect,
  onStarterOptionSelect,
  onHandoffRequest,
  onEndSession,
  onSend,
}: WidgetShellProps) {
  const copy = getWidgetCopy(language);
  const resolvedConnectionState =
    copy.connectionStates[connectionState as keyof typeof copy.connectionStates] ?? connectionState;
  const textareaRef = useRef<HTMLTextAreaElement | null>(null);
  const messagesRef = useRef<HTMLDivElement | null>(null);
  const messagesEndRef = useRef<HTMLDivElement | null>(null);
  const previousLastMessageIdRef = useRef<string | null>(null);
  const scrollFrameRefs = useRef<{ first: number; second: number }>({ first: 0, second: 0 });
  const [welcomeTimestampLabel, setWelcomeTimestampLabel] = useState(() =>
    _formatWelcomeTimestamp(language, welcomeTimeZones),
  );
  const isActiveConversation = Boolean(conversation && !["starter", "closed"].includes(conversation.status));

  useEffect(() => {
    const textarea = textareaRef.current;
    if (!textarea) {
      return;
    }

    // 모바일에서도 입력 줄 수만큼 자연스럽게 늘어나되, 위젯 전체 레이아웃이 무너지지 않도록 상한을 둔다.
    textarea.style.height = "0px";
    const nextHeight = Math.min(textarea.scrollHeight, 144);
    textarea.style.height = `${Math.max(nextHeight, 48)}px`;
    textarea.style.overflowY = textarea.scrollHeight > 144 ? "auto" : "hidden";
  }, [draft]);

  useLayoutEffect(() => {
    const messagesElement = messagesRef.current;
    const messagesEndElement = messagesEndRef.current;
    if (!messagesElement || !messagesEndElement) {
      return;
    }

    if (!conversation?.messages.length) {
      // 첫 진입 화면은 starter 선택지가 핵심이라 대화 스크롤 보정 대상에서 제외한다.
      messagesElement.scrollTo({ top: 0, behavior: "auto" });
      previousLastMessageIdRef.current = null;
      return;
    }

    const latestMessageId = conversation?.messages.at(-1)?.id ?? null;
    const shouldAnimate = Boolean(previousLastMessageIdRef.current && previousLastMessageIdRef.current !== latestMessageId);
    previousLastMessageIdRef.current = latestMessageId;

    // 브라우저가 높이 재계산을 늦게 하는 경우가 있어서 sentinel scroll과 직접 scrollTop 보정을 같이 건다.
    const scrollToBottom = () => {
      messagesElement.scrollTo({
        top: messagesElement.scrollHeight,
        behavior: shouldAnimate ? "smooth" : "auto",
      });
      messagesEndElement.scrollIntoView({
        block: "end",
        behavior: shouldAnimate ? "smooth" : "auto",
      });
    };

    scrollToBottom();
    scrollFrameRefs.current.first = window.requestAnimationFrame(() => {
      scrollToBottom();
      scrollFrameRefs.current.second = window.requestAnimationFrame(scrollToBottom);
    });

    return () => {
      window.cancelAnimationFrame(scrollFrameRefs.current.first);
      window.cancelAnimationFrame(scrollFrameRefs.current.second);
    };
  }, [conversation?.id, conversation?.messages.length, conversation?.messages.at(-1)?.id]);

  useEffect(() => {
    const updateTimestamp = () => {
      setWelcomeTimestampLabel(_formatWelcomeTimestamp(language, welcomeTimeZones));
    };

    updateTimestamp();
    const timer = window.setInterval(updateTimestamp, 30_000);
    return () => window.clearInterval(timer);
  }, [language, welcomeTimeZones]);

  const handleDraftKeyDown = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    // 한글 IME 조합 중 Enter와 Shift/Ctrl/Meta 조합은 전송으로 취급하지 않고, 순수 Enter만 전송에 쓴다.
    if (
      event.key !== "Enter" ||
      event.shiftKey ||
      event.ctrlKey ||
      event.metaKey ||
      event.altKey ||
      event.nativeEvent.isComposing ||
      event.nativeEvent.keyCode === 229
    ) {
      return;
    }
    event.preventDefault();
    onSend();
  };

  const handleStarterOptionClick = (entry: StarterEntry) => {
    // starter 버튼은 첫 분류 라우터라서 클릭 즉시 서버 workflow로 진입한다.
    onStarterOptionSelect(entry);
    window.requestAnimationFrame(() => {
      textareaRef.current?.focus();
    });
  };

  const handleQuickActionClick = (action: QuickActionEntry) => {
    // workflow 선택 버튼은 message.send와 같은 경로로 보내서 서버 검증/branch 규칙을 그대로 따른다.
    onQuickActionSelect(action);
    window.requestAnimationFrame(() => {
      textareaRef.current?.focus();
    });
  };

  return (
    <div className="widget-page">
      <div className="widget-shell">
        <header className="widget-header">
          <div className="widget-brand">
            <div className="widget-brand-mark" aria-hidden="true">B</div>
            <div>
              <div className="widget-title">{welcomeTitle || copy.welcomeTitle}</div>
              <div className="widget-subtle">{copy.headerSubtitle}</div>
            </div>
          </div>
          <div className="widget-header-actions">
            <div className="widget-status-row">
              <span className={`connection-pill ${connectionState}`} aria-label={`${copy.connectionLabel}: ${resolvedConnectionState}`}>
                <span className="connection-dot" aria-hidden="true" />
                {resolvedConnectionState}
              </span>
              {unreadCount > 0 ? <span className="badge">{unreadCount}</span> : null}
              <span className="badge muted">{copy.messageCount(conversation?.messages.length ?? 0)}</span>
            </div>
            <div className="language-switch" role="tablist" aria-label={copy.languageLabel}>
              {(["en", "ko"] as const).map((option) => (
                <button
                  key={option}
                  type="button"
                  className={language === option ? "language-option active" : "language-option"}
                  onClick={() => onLanguageChange(option)}
                >
                  {option.toUpperCase()}
                </button>
              ))}
            </div>
          </div>
        </header>

        <div ref={messagesRef} className="widget-messages">
          {conversation?.messages.length ? (
            conversation.messages.map((message) => (
              <div key={message.id} className={`bubble-row ${message.senderType}`}>
                <div className={`bubble ${message.senderType}`}>
                  <div className="bubble-label">
                    {message.senderName || copy.senderLabels[message.senderType]}
                  </div>
                  <div className="bubble-body">{message.body}</div>
                </div>
                {message.sourceRefs.length > 0 ? (
                  <div className="source-refs">
                    {message.sourceRefs.map((reference) => (
                      <span key={reference.documentId} className="source-ref">
                        {reference.title}
                      </span>
                    ))}
                  </div>
                ) : null}
              </div>
            ))
          ) : (
            <div className="empty-state">
              <div className="empty-state-header">
                <div className="empty-state-meta">
                  <div className="empty-state-title">{welcomeTitle || copy.welcomeTitle}</div>
                  <div className="empty-state-time">{welcomeTimestampLabel}</div>
                </div>
                <span className="empty-state-badge">{copy.readyBadge}</span>
              </div>
              {starterEntries.length > 0 ? (
                <div className="starter-panel">
                  <div>
                    <div className="starter-title">{copy.starterTitle}</div>
                    <div className="starter-hint">{copy.starterHint}</div>
                  </div>
                  <div className="starter-options">
                    {starterEntries.map((entry, index) => (
                      <button
                        key={entry.key}
                        type="button"
                        className="starter-option"
                        onClick={() => handleStarterOptionClick(entry)}
                      >
                        <span className="starter-option-index">{index + 1}</span>
                        <span className="starter-option-label">{entry.label}</span>
                      </button>
                    ))}
                  </div>
                </div>
              ) : null}
              <div className="empty-state-message">{welcomeMessage || copy.emptyState}</div>
            </div>
          )}
          {quickActions.length > 0 ? (
            <div className="quick-reply-panel" role="group" aria-label={copy.quickActionsLabel}>
              <div className="quick-reply-title">{copy.quickActionsTitle}</div>
              <div className="quick-reply-options">
                {quickActions.map((action) => (
                  <button
                    key={action.key}
                    type="button"
                    className={`quick-reply-option ${action.tone ?? "primary"}`}
                    onClick={() => handleQuickActionClick(action)}
                    disabled={isSending}
                  >
                    {action.label}
                  </button>
                ))}
              </div>
            </div>
          ) : null}
          <div ref={messagesEndRef} className="messages-end-anchor" aria-hidden="true" />
        </div>

        <footer className="widget-footer">
          <div className="composer">
            <textarea
              ref={textareaRef}
              value={draft}
              rows={1}
              onChange={(event) => onDraftChange(event.target.value)}
              onKeyDown={handleDraftKeyDown}
              placeholder={copy.placeholder}
            />
            <button
              className="send-button"
              aria-label={isSending ? copy.sending : copy.send}
              title={isSending ? copy.sending : copy.send}
              onClick={onSend}
              disabled={!draft.trim() || isSending}
            >
              {isSending ? copy.sending : copy.send}
            </button>
          </div>
          <div className="footer-actions">
            <button className="handoff-button footer-action-button" type="button" onClick={onHandoffRequest}>
              {copy.handoff}
            </button>
            {isActiveConversation ? (
              <button
                className="end-session-button footer-action-button"
                type="button"
                onClick={onEndSession}
                disabled={isSending}
              >
                {copy.endSession}
              </button>
            ) : null}
          </div>
        </footer>
      </div>
    </div>
  );
}

function getWidgetCopy(language: WidgetLanguage) {
  if (language === "ko") {
    return {
      connectionLabel: "연결",
      connectionStates: {
        connected: "연결됨",
        connecting: "연결 중",
        reconnecting: "재연결 중",
        offline: "오프라인",
      },
      headerSubtitle: "업무 문의 지원",
      languageLabel: "언어 선택",
      messageCount: (count: number) => `메시지 ${count}개`,
      welcomeTitle: "Boxer",
      readyBadge: "대기 중",
      senderLabels: {
        user: "나",
        assistant: "Boxer",
        system: "시스템",
        admin: "상담원",
      },
      emptyState: "필요한 업무를 선택하면 확인 항목을 순서대로 안내할게.",
      starterTitle: "문의 유형",
      starterHint: "가장 가까운 항목을 선택해 줘.",
      quickActionsLabel: "빠른 선택",
      quickActionsTitle: "선택해서 계속 진행",
      placeholder: "추가 설명을 입력해 줘",
      send: "보내기",
      sending: "보내는 중",
      handoff: "상담원 연결",
      endSession: "상담종료",
    };
  }

  return {
    connectionLabel: "Connection",
    connectionStates: {
      connected: "connected",
      connecting: "connecting",
      reconnecting: "reconnecting",
      offline: "offline",
    },
    headerSubtitle: "Support desk",
    languageLabel: "Select language",
    messageCount: (count: number) => `${count} msgs`,
    welcomeTitle: "Boxer",
    readyBadge: "Ready",
    senderLabels: {
      user: "You",
      assistant: "Boxer",
      system: "System",
      admin: "Support",
    },
    emptyState: "Choose a request type and I will collect the needed details step by step.",
    starterTitle: "Request type",
    starterHint: "Pick the closest option.",
    quickActionsLabel: "Quick choices",
    quickActionsTitle: "Choose to continue",
    placeholder: "Add details here",
    send: "Send",
    sending: "Sending",
    handoff: "Contact support",
    endSession: "End chat",
  };
}

function _formatWelcomeTimestamp(language: WidgetLanguage, welcomeTimeZones?: WidgetTimeZoneMap): string {
  const fallbackTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone;
  const configuredTimeZone = welcomeTimeZones?.[language]?.trim() || fallbackTimeZone;
  const locale = language === "ko" ? "ko-KR" : "en-US";

  try {
    return new Intl.DateTimeFormat(locale, {
      hour: "numeric",
      minute: "2-digit",
      hour12: true,
      timeZone: configuredTimeZone,
    }).format(new Date());
  } catch {
    return new Intl.DateTimeFormat(locale, {
      hour: "numeric",
      minute: "2-digit",
      hour12: true,
    }).format(new Date());
  }
}
