import { useEffect, useEffectEvent, useMemo, useRef, useState } from "react";

import type {
  AdminAuthResponse,
  AdminRealtimeEvent,
  AdminUser,
  ConversationListResponse,
  ConversationSnapshot,
  KnowledgeDocumentDetail,
  KnowledgeDocumentSummary,
  KnowledgeStatus,
} from "./types";

const CONVERSATION_PAGE_SIZE = 50;

export function AdminApp() {
  const refreshTimerRef = useRef<number | null>(null);
  const [adminUser, setAdminUser] = useState<AdminUser | null>(null);
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [pathname, setPathname] = useState(window.location.pathname);
  const [knowledgeStatus, setKnowledgeStatus] = useState<KnowledgeStatus | null>(null);
  const [documents, setDocuments] = useState<KnowledgeDocumentSummary[]>([]);
  const [selectedDocument, setSelectedDocument] = useState<KnowledgeDocumentDetail | null>(null);
  const [conversations, setConversations] = useState<ConversationSnapshot[]>([]);
  const [selectedConversation, setSelectedConversation] = useState<ConversationSnapshot | null>(null);
  const [conversationFilter, setConversationFilter] = useState<"handoff" | "mine" | "all">("handoff");
  const [conversationSearch, setConversationSearch] = useState("");
  const [conversationTotal, setConversationTotal] = useState(0);
  const [replyDraft, setReplyDraft] = useState("");
  const activeTab = useMemo(() => (pathname.includes("/admin/conversations") ? "conversations" : "knowledge"), [pathname]);
  const filteredConversations = useMemo(() => {
    if (conversationFilter === "handoff") {
      return conversations.filter((conversation) => ["handoff_pending", "handoff_live"].includes(conversation.status));
    }
    if (conversationFilter === "mine") {
      return conversations.filter((conversation) => conversation.assignedAdminUserId === adminUser?.id);
    }
    return conversations;
  }, [adminUser?.id, conversationFilter, conversations]);

  useEffect(() => {
    void bootstrap();
    const handlePopState = () => setPathname(window.location.pathname);
    window.addEventListener("popstate", handlePopState);
    return () => {
      window.removeEventListener("popstate", handlePopState);
      if (refreshTimerRef.current !== null) {
        window.clearTimeout(refreshTimerRef.current);
      }
    };
  }, []);

  const scheduleConversationRefresh = useEffectEvent((options?: { delayMs?: number; refreshSelected?: boolean }) => {
    if (refreshTimerRef.current !== null) {
      window.clearTimeout(refreshTimerRef.current);
    }
    refreshTimerRef.current = window.setTimeout(() => {
      refreshTimerRef.current = null;
      const refreshOptions: { refreshSelected?: boolean } = {};
      if (options?.refreshSelected !== undefined) {
        refreshOptions.refreshSelected = options.refreshSelected;
      }
      void refreshConversationData(refreshOptions);
    }, options?.delayMs ?? 120);
  });

  const handleAdminRealtimeEvent = useEffectEvent((message: AdminRealtimeEvent) => {
    if (message.type !== "conversation.updated") {
      return;
    }
    const shouldRefreshSelected = message.payload?.id === selectedConversation?.id;
    scheduleConversationRefresh({ delayMs: 80, refreshSelected: shouldRefreshSelected });
  });

  useEffect(() => {
    if (!adminUser) {
      return;
    }
    const timer = window.setInterval(() => {
      scheduleConversationRefresh({ delayMs: 0 });
    }, 30000);
    return () => window.clearInterval(timer);
  }, [adminUser?.id]);

  useEffect(() => {
    if (!adminUser) {
      return;
    }
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const websocket = new WebSocket(`${protocol}//${window.location.host}/ws/admin`);
    websocket.onmessage = (event) => {
      handleAdminRealtimeEvent(JSON.parse(event.data) as AdminRealtimeEvent);
    };
    return () => websocket.close();
  }, [adminUser?.id]);

  useEffect(() => {
    if (!adminUser) {
      return;
    }
    scheduleConversationRefresh({ delayMs: 250 });
  }, [adminUser, conversationFilter, conversationSearch]);

  async function bootstrap() {
    try {
      const me = await requestJson<AdminAuthResponse>("/api/admin/auth/me");
      setAdminUser(me.adminUser);
      await refreshAdminData();
    } catch {
      setAdminUser(null);
    }
  }

  async function refreshAdminData() {
    const [status, docList, conversationList] = await Promise.all([
      requestJson<KnowledgeStatus>("/api/admin/knowledge/status"),
      requestJson<{ documents: KnowledgeDocumentSummary[] }>("/api/admin/knowledge/documents"),
      requestJson<ConversationListResponse>(buildConversationListPath(0)),
    ]);
    setKnowledgeStatus(status);
    setDocuments(docList.documents);
    setConversations(conversationList.conversations);
    setConversationTotal(conversationList.pagination.total);
    if (docList.documents[0]) {
      await loadDocument(docList.documents[0].id);
    }
    if (conversationList.conversations[0]) {
      await selectConversation(conversationList.conversations[0].id);
    }
  }

  async function refreshConversationData(options?: { append?: boolean; refreshSelected?: boolean }) {
    const offset = options?.append ? conversations.length : 0;
    const conversationList = await requestJson<ConversationListResponse>(buildConversationListPath(offset));
    setConversations((current) => (options?.append ? [...current, ...conversationList.conversations] : conversationList.conversations));
    setConversationTotal(conversationList.pagination.total);
    if (options?.refreshSelected !== false && selectedConversation) {
      await refreshSelectedConversation(selectedConversation.id);
    }
  }

  async function loadDocument(documentId: string) {
    const response = await requestJson<{ document: KnowledgeDocumentDetail }>(`/api/admin/knowledge/documents/${documentId}`);
    setSelectedDocument(response.document);
  }

  async function selectConversation(conversationId: string) {
    await refreshSelectedConversation(conversationId);
    setReplyDraft("");
  }

  async function refreshSelectedConversation(conversationId: string) {
    const response = await requestJson<{ conversation: ConversationSnapshot }>(`/api/admin/conversations/${conversationId}`);
    setSelectedConversation(response.conversation);
  }

  async function handleLogin() {
    const response = await requestJson<AdminAuthResponse>("/api/admin/auth/login", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    });
    setAdminUser(response.adminUser);
    navigate("/admin/knowledge");
    await refreshAdminData();
  }

  async function handleLogout() {
    await requestJson("/api/admin/auth/logout", {
      method: "POST",
      body: JSON.stringify({}),
    });
    setAdminUser(null);
    setKnowledgeStatus(null);
    setDocuments([]);
    setSelectedDocument(null);
    setConversations([]);
    setSelectedConversation(null);
    navigate("/admin/login");
  }

  async function handleSync() {
    await requestJson("/api/admin/knowledge/sync", {
      method: "POST",
      body: JSON.stringify({}),
    });
    await refreshAdminData();
  }

  async function handleClaim(conversationId: string) {
    const response = await requestJson<{ conversation: ConversationSnapshot }>(`/api/admin/conversations/${conversationId}/claim`, {
      method: "POST",
      body: JSON.stringify({}),
    });
    setSelectedConversation(response.conversation);
    await refreshConversationData();
  }

  async function handleRelease(conversationId: string) {
    const response = await requestJson<{ conversation: ConversationSnapshot }>(`/api/admin/conversations/${conversationId}/release`, {
      method: "POST",
      body: JSON.stringify({}),
    });
    setSelectedConversation(response.conversation);
    await refreshConversationData();
  }

  async function handleClose(conversationId: string) {
    const response = await requestJson<{ conversation: ConversationSnapshot }>(`/api/admin/conversations/${conversationId}/close`, {
      method: "POST",
      body: JSON.stringify({}),
    });
    setSelectedConversation(response.conversation);
    await refreshConversationData();
  }

  async function handleReply(conversationId: string) {
    if (!replyDraft.trim()) {
      return;
    }
    const response = await requestJson<{ conversation: ConversationSnapshot }>(`/api/admin/conversations/${conversationId}/reply`, {
      method: "POST",
      body: JSON.stringify({ text: replyDraft.trim() }),
    });
    setReplyDraft("");
    setSelectedConversation(response.conversation);
    await refreshConversationData();
  }

  function navigate(nextPath: string) {
    window.history.pushState({}, "", nextPath);
    setPathname(nextPath);
  }

  function buildConversationListPath(offset: number) {
    const params = new URLSearchParams({
      limit: String(CONVERSATION_PAGE_SIZE),
      offset: String(offset),
    });
    if (conversationSearch.trim()) {
      params.set("q", conversationSearch.trim());
    }
    if (conversationFilter === "handoff") {
      params.set("status", "handoff");
    }
    if (conversationFilter === "mine") {
      params.set("assigned", "me");
    }
    return `/api/admin/conversations?${params.toString()}`;
  }

  if (!adminUser) {
    return (
      <div className="admin-login-page">
        <div className="login-card">
          <h1>Boxer Admin</h1>
          <p>Sign in with the local bootstrap account.</p>
          <input value={email} onChange={(event) => setEmail(event.target.value)} placeholder="Email" />
          <input
            type="password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            placeholder="Password"
          />
          <button onClick={() => void handleLogin()}>Login</button>
        </div>
      </div>
    );
  }

  return (
    <div className="admin-page">
      <aside className="admin-sidebar">
        <div>
          <div className="admin-brand">Boxer Admin</div>
          <div className="admin-subtle">{adminUser.email}</div>
        </div>
        <div className="admin-nav">
          <button className={activeTab === "knowledge" ? "active" : ""} onClick={() => navigate("/admin/knowledge")}>
            Knowledge
          </button>
          <button className={activeTab === "conversations" ? "active" : ""} onClick={() => navigate("/admin/conversations")}>
            Conversations
          </button>
        </div>
        <button className="secondary-button" onClick={() => void handleLogout()}>
          Logout
        </button>
      </aside>

      <main className="admin-content">
        {activeTab === "knowledge" ? (
          <section className="admin-section-grid">
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h2>Knowledge</h2>
                  <p>Active source: {knowledgeStatus?.activeSource ?? "-"}</p>
                </div>
                <button onClick={() => void handleSync()}>Sync</button>
              </div>
              <div className="panel-meta">
                <div>Documents: {knowledgeStatus?.documentCount ?? 0}</div>
                <div>
                  Last sync: {knowledgeStatus?.lastSync?.status ?? "none"}
                  {knowledgeStatus?.lastSync?.finishedAt ? ` / ${knowledgeStatus.lastSync.finishedAt}` : ""}
                </div>
              </div>
              <div className="list">
                {documents.map((document) => (
                  <button
                    key={document.id}
                    className={selectedDocument?.id === document.id ? "list-item active" : "list-item"}
                    onClick={() => void loadDocument(document.id)}
                  >
                    <div className="list-title">{document.title}</div>
                    <div className="list-subtle">{document.excerpt}</div>
                  </button>
                ))}
              </div>
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h2>Preview</h2>
                  <p>{selectedDocument?.sourceUri ?? "Select a document"}</p>
                </div>
              </div>
              <pre className="document-preview">{selectedDocument?.content ?? "No document selected."}</pre>
            </div>
          </section>
        ) : null}

        {activeTab === "conversations" ? (
          <section className="admin-section-grid">
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h2>Conversations</h2>
                  <p>Stored widget threads</p>
                </div>
              </div>
              <div className="conversation-filters">
                {(["handoff", "mine", "all"] as const).map((filter) => (
                  <button
                    key={filter}
                    className={conversationFilter === filter ? "active" : ""}
                    onClick={() => setConversationFilter(filter)}
                  >
                    {filter}
                  </button>
                ))}
              </div>
              <div className="conversation-search">
                <input
                  value={conversationSearch}
                  onChange={(event) => setConversationSearch(event.target.value)}
                  placeholder="Search conversations"
                />
                <span>
                  {filteredConversations.length} / {conversationTotal}
                </span>
              </div>
              <div className="list">
                {filteredConversations.map((conversation) => (
                  <button
                    key={conversation.id}
                    className={selectedConversation?.id === conversation.id ? "list-item active" : "list-item"}
                    onClick={() => void selectConversation(conversation.id)}
                  >
                    <div className="list-title">{conversation.customerName || conversation.customerId || "Anonymous"}</div>
                    <div className="status-line">
                      <span className={`status-badge ${conversation.status}`}>{conversation.status}</span>
                      {conversation.assignedAdminUserName ? <span>{conversation.assignedAdminUserName}</span> : null}
                    </div>
                    <div className="list-subtle">{conversation.lastMessagePreview || "No messages yet"}</div>
                  </button>
                ))}
                {conversations.length < conversationTotal ? (
                  <button className="list-item load-more" onClick={() => void refreshConversationData({ append: true })}>
                    Load more
                  </button>
                ) : null}
              </div>
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h2>Thread</h2>
                  <p>{selectedConversation?.sessionId ?? "Select a conversation"}</p>
                </div>
              </div>
              {selectedConversation ? (
                <div className="conversation-toolbar">
                  <span className={`status-badge ${selectedConversation.status}`}>{selectedConversation.status}</span>
                  {selectedConversation.assignedAdminUserName ? (
                    <span className="assigned-admin">Assigned: {selectedConversation.assignedAdminUserName}</span>
                  ) : null}
                  {selectedConversation.status === "handoff_pending" ? (
                    <button onClick={() => void handleClaim(selectedConversation.id)}>Claim</button>
                  ) : null}
                  {selectedConversation.status === "handoff_live" &&
                  selectedConversation.assignedAdminUserId === adminUser.id ? (
                    <>
                      <button onClick={() => void handleRelease(selectedConversation.id)}>Release</button>
                      <button onClick={() => void handleClose(selectedConversation.id)}>Close</button>
                    </>
                  ) : null}
                </div>
              ) : null}
              <div className="thread">
                {selectedConversation?.messages.map((message) => (
                  <div key={message.id} className={`thread-message ${message.senderType}`}>
                    <div className="thread-label">{message.senderName || message.senderType}</div>
                    <div>{message.body}</div>
                  </div>
                )) ?? <div className="list-subtle">No conversation selected.</div>}
              </div>
              {selectedConversation?.status === "handoff_live" &&
              selectedConversation.assignedAdminUserId === adminUser.id ? (
                <div className="reply-box">
                  <textarea
                    value={replyDraft}
                    onChange={(event) => setReplyDraft(event.target.value)}
                    placeholder="Reply as support"
                  />
                  <button disabled={!replyDraft.trim()} onClick={() => void handleReply(selectedConversation.id)}>
                    Send reply
                  </button>
                </div>
              ) : selectedConversation?.status === "handoff_pending" ? (
                <div className="reply-hint">Claim this conversation before replying.</div>
              ) : null}
            </div>
          </section>
        ) : null}
      </main>
    </div>
  );
}

async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  const method = (init?.method ?? "GET").toUpperCase();
  const csrfHeaders =
    method === "GET" || method === "HEAD"
      ? {}
      : { "x-boxer-csrf-token": readCookieValue("boxer_web_admin_csrf") ?? "" };
  const response = await fetch(path, {
    ...init,
    headers: {
      "content-type": "application/json",
      ...csrfHeaders,
      ...(init?.headers ?? {}),
    },
    credentials: "include",
  });
  if (!response.ok) {
    throw new Error(await response.text());
  }
  return (await response.json()) as T;
}

function readCookieValue(name: string): string | null {
  const encodedPrefix = `${encodeURIComponent(name)}=`;
  for (const item of document.cookie.split(";")) {
    const normalized = item.trim();
    if (!normalized.startsWith(encodedPrefix)) {
      continue;
    }
    return decodeURIComponent(normalized.slice(encodedPrefix.length));
  }
  return null;
}
