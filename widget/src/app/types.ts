export type WidgetLanguage = "en" | "ko";

export type WidgetTimeZoneMap = Partial<Record<WidgetLanguage, string>>;

export type ConversationContext = Record<string, unknown> & {
  language?: WidgetLanguage;
  tags?: string[];
  metadata?: Record<string, unknown>;
};

export interface SourceReference {
  documentId: string;
  title: string;
  score: number;
  sourceUri: string;
}

export interface MessageRecord {
  id: string;
  senderType: "user" | "assistant" | "system" | "admin";
  senderName?: string | null;
  body: string;
  sourceRefs: SourceReference[];
  createdAt: string;
}

export interface ConversationSnapshot {
  id: string;
  sessionId: string;
  customerId?: string | null;
  customerName?: string | null;
  customerEmail?: string | null;
  context: ConversationContext;
  status: string;
  workflowKey?: string | null;
  workflowState: Record<string, unknown>;
  assignedAdminUserId?: string | null;
  assignedAdminUserName?: string | null;
  handoffRequestedAt?: string | null;
  handoffStartedAt?: string | null;
  closedAt?: string | null;
  lastMessagePreview?: string | null;
  createdAt: string;
  updatedAt: string;
  messages: MessageRecord[];
}

export interface ConversationListResponse {
  conversations: ConversationSnapshot[];
  pagination: {
    limit: number;
    offset: number;
    total: number;
  };
}

export interface AdminUser {
  id: string;
  email: string;
  name: string;
}

export interface AdminAuthResponse {
  adminUser: AdminUser;
  csrfToken: string;
}

export interface KnowledgeStatus {
  activeSource: string;
  documentCount: number;
  lastSync?: {
    id: number;
    sourceType: string;
    status: string;
    documentCount: number;
    errorMessage?: string | null;
    startedAt: string;
    finishedAt?: string | null;
  } | null;
}

export interface KnowledgeDocumentSummary {
  id: string;
  title: string;
  sourceType: string;
  sourceUri: string;
  excerpt: string;
  syncedAt: string;
}

export interface KnowledgeDocumentDetail extends KnowledgeDocumentSummary {
  content: string;
  metadata: Record<string, unknown>;
}

export interface WidgetConfig {
  welcomeTitle?: string | null;
  welcomeMessage?: string | null;
  starterOptions?: string[];
  starterEntries?: StarterEntry[];
  workflowOptions?: WidgetWorkflowOptions;
  welcomeTimeZones?: WidgetTimeZoneMap;
}

export interface StarterEntry {
  key: string;
  label: string;
}

export type WidgetWorkflowOptions = Record<string, WidgetWorkflowStep[]>;

export interface WidgetWorkflowStep {
  field: string;
  inputType?: string;
  skipAllowed?: boolean;
  choices?: WidgetWorkflowChoice[];
}

export interface WidgetWorkflowChoice {
  value: string;
  label?: string;
  labels?: string | Partial<Record<WidgetLanguage, string>>;
}

export interface QuickActionEntry {
  key: string;
  label: string;
  value: string;
  tone?: "primary" | "neutral" | "danger";
}

export interface ErrorPayload {
  code: string;
  message: string;
}

export interface WidgetSessionReadyEvent {
  type: "session.ready";
  payload: ConversationSnapshot;
}

export interface WidgetConversationUpdatedEvent {
  type: "conversation.updated";
  payload: ConversationSnapshot;
}

export interface WidgetMessageCreatedEvent {
  type: "message.created";
  payload: MessageRecord;
}

export interface WidgetSessionEndedEvent {
  type: "session.ended";
  payload: ConversationSnapshot;
}

export interface WidgetErrorEvent {
  type: "error";
  payload: ErrorPayload;
}

export type WidgetServerEvent =
  | WidgetSessionReadyEvent
  | WidgetConversationUpdatedEvent
  | WidgetMessageCreatedEvent
  | WidgetSessionEndedEvent
  | WidgetErrorEvent;

export interface AdminReadyEvent {
  type: "admin.ready";
  payload: {
    adminUser: AdminUser;
  };
}

export interface AdminMessageCreatedEvent {
  type: "message.created";
  payload: {
    conversationId: string;
    message: MessageRecord;
  };
}

export interface AdminConversationUpdatedEvent {
  type: "conversation.updated";
  payload: ConversationSnapshot;
}

export type AdminRealtimeEvent = AdminReadyEvent | AdminMessageCreatedEvent | AdminConversationUpdatedEvent;
