import { useEffect, useMemo, useRef, useState } from "react";

import { WidgetShell } from "../components/widget-shell";
import { normalizeLanguage, readLanguageFromContext, resolveInitialLanguage } from "./language";
import type {
  ConversationContext,
  ConversationSnapshot,
  MessageRecord,
  QuickActionEntry,
  StarterEntry,
  WidgetConfig,
  WidgetLanguage,
  WidgetWorkflowChoice,
  WidgetServerEvent,
} from "./types";

const SESSION_STORAGE_KEY = "boxer:web:sessionId";
const LANGUAGE_STORAGE_KEY = "boxer:web:language";
const SDK_SOURCE = "boxer-widget-sdk";

export function WidgetApp() {
  const serverOrigin = useMemo(resolveServerOrigin, []);
  const initialLanguage = resolveInitialLanguage({
    storedLanguage: window.localStorage.getItem(LANGUAGE_STORAGE_KEY),
    navigatorLanguages: window.navigator.languages,
    navigatorLanguage: window.navigator.language,
    intlLocale: Intl.DateTimeFormat().resolvedOptions().locale,
    documentLanguage: document.documentElement.lang,
  });
  const [language, setLanguage] = useState<WidgetLanguage>(initialLanguage);
  const [conversation, setConversation] = useState<ConversationSnapshot | null>(null);
  const [draft, setDraft] = useState("");
  const [isSending, setIsSending] = useState(false);
  const [connectionState, setConnectionState] = useState("connecting");
  const [unreadCount, setUnreadCount] = useState(0);
  const [widgetConfig, setWidgetConfig] = useState<WidgetConfig>({});
  const socketRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<number | null>(null);
  const identityRef = useRef<Record<string, unknown> | null>(null);
  const contextRef = useRef<ConversationContext | null>(null);
  const languageRef = useRef<WidgetLanguage>(initialLanguage);

  const websocketUrl = useMemo(() => {
    const boxerWebUrl = new URL(serverOrigin);
    boxerWebUrl.protocol = boxerWebUrl.protocol === "https:" ? "wss:" : "ws:";
    boxerWebUrl.pathname = "/ws/widget";
    return boxerWebUrl.toString();
  }, [serverOrigin]);

  useEffect(() => {
    connect();
    return () => {
      if (reconnectTimerRef.current) {
        window.clearTimeout(reconnectTimerRef.current);
      }
      socketRef.current?.close();
    };
  }, [websocketUrl]);

  useEffect(() => {
    // 위젯 첫 화면 문구와 빠른 선택지는 서버 설정으로 받아 두고, 없으면 기본 empty state로 남긴다.
    void requestJson<WidgetConfig>(serverOrigin, "/api/widget/config")
      .then((config) => setWidgetConfig(config))
      .catch(() => setWidgetConfig({}));
  }, [serverOrigin]);

  useEffect(() => {
    languageRef.current = language;
    window.localStorage.setItem(LANGUAGE_STORAGE_KEY, language);
    reinitialize();
  }, [language]);

  useEffect(() => {
    const handleMessage = (event: MessageEvent) => {
      const parentOrigin = new URLSearchParams(window.location.search).get("parentOrigin");
      if (
        event.source !== window.parent ||
        (parentOrigin && event.origin !== parentOrigin) ||
        typeof event.data !== "object" ||
        event.data === null ||
        event.data.source !== SDK_SOURCE
      ) {
        return;
      }
      if (event.data.type === "identify") {
        identityRef.current = event.data.payload;
        reinitialize();
      }
      if (event.data.type === "setContext") {
        const nextContext = normalizeConversationContext(event.data.payload);
        contextRef.current = nextContext;

        const nextLanguage = readLanguageFromContext(nextContext);
        if (nextLanguage && nextLanguage !== languageRef.current) {
          setLanguage(nextLanguage);
          return;
        }
        reinitialize();
      }
    };

    window.addEventListener("message", handleMessage);
    return () => window.removeEventListener("message", handleMessage);
  }, []);

  useEffect(() => {
    const handleVisibility = () => {
      if (!document.hidden) {
        setUnreadCount(0);
      }
    };
    document.addEventListener("visibilitychange", handleVisibility);
    return () => document.removeEventListener("visibilitychange", handleVisibility);
  }, []);

  function connect() {
    // widget은 새로고침 뒤에도 같은 thread를 이어야 해서 소켓이 열리면 저장된 sessionId로 먼저 복구를 시도한다.
    setConnectionState(socketRef.current ? "reconnecting" : "connecting");
    const socket = new WebSocket(websocketUrl);
    socketRef.current = socket;

    socket.addEventListener("open", () => {
      setConnectionState("connected");
      sendEvent("session.init", {
        sessionId: window.localStorage.getItem(SESSION_STORAGE_KEY),
        identity: identityRef.current ?? undefined,
        context: buildSessionContext(contextRef.current, languageRef.current),
      });
    });

    socket.addEventListener("message", (message) => {
      const event = JSON.parse(String(message.data)) as WidgetServerEvent;
      if (event.type === "session.ready" || event.type === "conversation.updated") {
        setIsSending(false);
        const nextConversation = event.payload as ConversationSnapshot;
        if (event.type === "conversation.updated" && nextConversation.status === "closed") {
          window.localStorage.removeItem(SESSION_STORAGE_KEY);
          setDraft("");
          setUnreadCount(0);
          setConversation(null);
          return;
        }
        window.localStorage.setItem(SESSION_STORAGE_KEY, nextConversation.sessionId);
        const restoredLanguage = readLanguageFromContext(nextConversation.context);
        if (restoredLanguage && restoredLanguage !== languageRef.current) {
          languageRef.current = restoredLanguage;
          window.localStorage.setItem(LANGUAGE_STORAGE_KEY, restoredLanguage);
          setLanguage(restoredLanguage);
        }
        setConversation(nextConversation);
        return;
      }
      if (event.type === "session.ended") {
        window.localStorage.removeItem(SESSION_STORAGE_KEY);
        setIsSending(false);
        setDraft("");
        setUnreadCount(0);
        setConversation(null);
        sendEvent("session.init", {
          identity: identityRef.current ?? undefined,
          context: buildSessionContext(contextRef.current, languageRef.current),
        });
        return;
      }
      if (event.type === "message.created") {
        const nextMessage = event.payload as MessageRecord;
        if (nextMessage.senderType !== "user") {
          setIsSending(false);
        }
        setConversation((current) => {
          if (!current) {
            return current;
          }
          if (current.messages.some((messageRecord) => messageRecord.id === nextMessage.id)) {
            return current;
          }
          return {
            ...current,
            lastMessagePreview: nextMessage.body,
            updatedAt: nextMessage.createdAt,
            messages: [...current.messages, nextMessage],
          };
        });
        if (nextMessage.senderType !== "user" && document.hidden) {
          setUnreadCount((value) => value + 1);
        }
        return;
      }

      if (event.type === "error") {
        setIsSending(false);
      }
    });

    socket.addEventListener("close", () => {
      setConnectionState("offline");
      setIsSending(false);
      // alpha 단계는 단일 소켓만 유지하므로 짧은 interval 재연결이면 운영 복잡도 없이 충분하다.
      reconnectTimerRef.current = window.setTimeout(() => connect(), 1000);
    });
  }

  function sendEvent(type: string, payload: Record<string, unknown>) {
    const socket = socketRef.current;
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return false;
    }
    socket.send(JSON.stringify({ type, payload }));
    return true;
  }

  function reinitialize() {
    // SDK가 identify/context를 늦게 주입해도 현재 conversation에 바로 합쳐지도록 init 이벤트를 다시 보낸다.
    const sessionId = window.localStorage.getItem(SESSION_STORAGE_KEY);
    sendEvent("session.init", {
      sessionId,
      identity: identityRef.current ?? undefined,
      context: buildSessionContext(contextRef.current, languageRef.current),
    });
  }

  function sendMessage() {
    if (!conversation || !draft.trim() || isSending) {
      return;
    }
    const nextText = draft.trim();
    const didSend = sendEvent("message.send", {
      sessionId: conversation.sessionId,
      text: nextText,
    });
    if (!didSend) {
      return;
    }

    // 서버 응답을 받기 전까지는 중복 Enter 연타를 막아서 같은 메시지가 여러 번 저장되지 않게 한다.
    setIsSending(true);
    setDraft("");
  }

  function selectStarterOption(entry: StarterEntry) {
    if (!conversation || isSending) {
      return;
    }
    const didSend = sendEvent("workflow.start", {
      sessionId: conversation.sessionId,
      workflowKey: entry.key,
    });
    if (didSend) {
      setIsSending(true);
      setDraft("");
    }
  }

  function requestHandoff() {
    if (!conversation || isSending) {
      return;
    }
    const didSend = sendEvent("handoff.request", {
      sessionId: conversation.sessionId,
      reason: draft.trim() || undefined,
    });
    if (didSend) {
      setIsSending(true);
      setDraft("");
    }
  }

  function sendQuickAction(action: QuickActionEntry) {
    if (!conversation || isSending) {
      return;
    }
    const didSend = sendEvent("message.send", {
      sessionId: conversation.sessionId,
      text: action.value,
    });
    if (didSend) {
      setIsSending(true);
      setDraft("");
    }
  }

  function endSession() {
    if (!conversation || isSending) {
      return;
    }
    const didSend = sendEvent("session.end", {
      sessionId: conversation.sessionId,
    });
    if (didSend) {
      setIsSending(true);
      setDraft("");
    }
  }

  return (
    <WidgetShell
      connectionState={connectionState}
      conversation={conversation}
      draft={draft}
      isSending={isSending}
      language={language}
      quickActions={normalizeQuickActions(conversation, widgetConfig, language)}
      unreadCount={unreadCount}
      welcomeTitle={widgetConfig.welcomeTitle ?? null}
      welcomeMessage={widgetConfig.welcomeMessage ?? null}
      starterEntries={normalizeStarterEntries(widgetConfig)}
      welcomeTimeZones={widgetConfig.welcomeTimeZones ?? {}}
      onDraftChange={setDraft}
      onLanguageChange={setLanguage}
      onQuickActionSelect={sendQuickAction}
      onStarterOptionSelect={selectStarterOption}
      onHandoffRequest={requestHandoff}
      onEndSession={endSession}
      onSend={sendMessage}
    />
  );
}

function buildSessionContext(currentContext: ConversationContext | null, language: WidgetLanguage): ConversationContext {
  // host app이 넘긴 tags/metadata를 보존하면서 widget 선택 언어만 항상 최종값으로 덮어쓴다.
  return {
    ...(currentContext ?? {}),
    language,
    tags: Array.isArray(currentContext?.tags) ? currentContext.tags : [],
    metadata: isRecord(currentContext?.metadata) ? currentContext.metadata : {},
  };
}

function normalizeStarterEntries(config: WidgetConfig): StarterEntry[] {
  const entries = config.starterEntries ?? [];
  if (entries.length > 0) {
    return entries
      .map((entry) => ({
        key: String(entry.key ?? "").trim(),
        label: String(entry.label ?? "").trim(),
      }))
      .filter((entry) => entry.key && entry.label);
  }

  return (config.starterOptions ?? [])
    .map((item, index) => {
      const label = item.trim();
      return {
        key: `option_${index + 1}`,
        label,
      };
    })
    .filter((entry) => entry.label);
}

function normalizeQuickActions(
  conversation: ConversationSnapshot | null,
  config: WidgetConfig,
  language: WidgetLanguage,
): QuickActionEntry[] {
  if (!conversation) {
    return [];
  }

  if (conversation.status === "handoff_offered") {
    return language === "ko"
      ? [
          { key: "handoff_yes", label: "상담원 연결", value: "네", tone: "primary" },
          { key: "handoff_no", label: "계속 직접 확인", value: "아니요", tone: "neutral" },
        ]
      : [
          { key: "handoff_yes", label: "Contact support", value: "yes", tone: "primary" },
          { key: "handoff_no", label: "Keep checking", value: "no", tone: "neutral" },
        ];
  }

  if (conversation.status !== "workflow_active" || !conversation.workflowKey) {
    return [];
  }

  const currentStepIndex = parseStepIndex(conversation.workflowState.currentStepIndex);
  const currentStep = config.workflowOptions?.[conversation.workflowKey]?.[currentStepIndex];
  if (!currentStep) {
    return [];
  }

  // workflow choices는 사용자가 값을 직접 외우지 않아도 되게 현재 단계의 빠른 선택 버튼으로 바꾼다.
  const choiceActions = (currentStep.choices ?? [])
    .map((choice) => normalizeChoiceAction(currentStep.field, choice, language))
    .filter((action): action is QuickActionEntry => action !== null);

  if (currentStep.skipAllowed) {
    choiceActions.push(
      language === "ko"
        ? { key: `${currentStep.field}:skip`, label: "건너뛰기", value: "건너뛰기", tone: "neutral" }
        : { key: `${currentStep.field}:skip`, label: "Skip", value: "skip", tone: "neutral" },
    );
  }

  return choiceActions;
}

function normalizeChoiceAction(
  field: string,
  choice: WidgetWorkflowChoice,
  language: WidgetLanguage,
): QuickActionEntry | null {
  const value = String(choice.value ?? "").trim();
  if (!value) {
    return null;
  }
  return {
    key: `${field}:${value}`,
    label: resolveChoiceLabel(choice, language),
    value,
    tone: "primary",
  };
}

function resolveChoiceLabel(choice: WidgetWorkflowChoice, language: WidgetLanguage): string {
  if (typeof choice.labels === "string" && choice.labels.trim()) {
    return choice.labels.trim();
  }
  if (typeof choice.labels === "object" && choice.labels !== null) {
    const localized = choice.labels[language]?.trim();
    if (localized) {
      return localized;
    }
  }
  if (choice.label?.trim()) {
    return choice.label.trim();
  }
  return humanizeChoice(choice.value);
}

function humanizeChoice(value: string): string {
  const normalized = String(value ?? "").trim().replace(/[_-]+/g, " ");
  return normalized ? normalized.charAt(0).toUpperCase() + normalized.slice(1) : "";
}

function parseStepIndex(value: unknown): number {
  const parsed = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return 0;
  }
  return Math.floor(parsed);
}

function normalizeConversationContext(payload: unknown): ConversationContext | null {
  if (!isRecord(payload)) {
    return null;
  }

  const nextContext: ConversationContext = { ...payload };
  if (Array.isArray(payload.tags)) {
    nextContext.tags = payload.tags.filter((value): value is string => typeof value === "string");
  }
  if (isRecord(payload.metadata)) {
    nextContext.metadata = payload.metadata;
  }

  const language = normalizeLanguage(payload.language) ?? normalizeLanguage(payload.locale);
  if (language) {
    nextContext.language = language;
  }
  return nextContext;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function resolveServerOrigin(): string {
  const rawBoxerWebUrl = new URLSearchParams(window.location.search).get("boxerWebUrl");
  if (!rawBoxerWebUrl) {
    throw new Error("Boxer Widget boxerWebUrl query parameter is required.");
  }

  const boxerWebUrl = new URL(rawBoxerWebUrl);
  if (!["http:", "https:"].includes(boxerWebUrl.protocol)) {
    throw new Error("Boxer Widget boxerWebUrl must use http or https.");
  }
  return boxerWebUrl.origin;
}

async function requestJson<T>(serverOrigin: string, path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(new URL(path, serverOrigin), {
    ...init,
    headers: {
      "content-type": "application/json",
      ...(init?.headers ?? {}),
    },
    // 서비스 origin에서 실행되는 widget은 Boxer Web의 관리자 cookie를 전송하지 않는다.
    credentials: "omit",
  });
  if (!response.ok) {
    throw new Error(await response.text());
  }
  return (await response.json()) as T;
}
