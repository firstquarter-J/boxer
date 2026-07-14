import type { ConversationContext, WidgetLanguage } from "./types";

export interface BrowserLanguageSnapshot {
  storedLanguage?: unknown;
  navigatorLanguages?: readonly unknown[];
  navigatorLanguage?: unknown;
  intlLocale?: unknown;
  documentLanguage?: unknown;
}

export function resolveInitialLanguage(snapshot: BrowserLanguageSnapshot): WidgetLanguage {
  const storedLanguage = normalizeLanguage(snapshot.storedLanguage);
  if (storedLanguage) {
    return storedLanguage;
  }

  // 브라우저가 여러 선호 언어를 주는 경우가 많아서 배열/단일값/document lang을 순서대로 훑고 첫 지원 언어를 택한다.
  const candidates = [
    ...(snapshot.navigatorLanguages ?? []),
    snapshot.navigatorLanguage,
    snapshot.intlLocale,
    snapshot.documentLanguage,
  ];

  for (const candidate of candidates) {
    const normalized = normalizeLanguage(candidate);
    if (normalized) {
      return normalized;
    }
  }

  return "en";
}

export function readLanguageFromContext(context: ConversationContext | null | undefined): WidgetLanguage | null {
  if (!context) {
    return null;
  }

  return (
    normalizeLanguage(context.language) ??
    normalizeLanguage(context.metadata?.language) ??
    normalizeLanguage(context.metadata?.locale) ??
    null
  );
}

export function normalizeLanguage(value: unknown): WidgetLanguage | null {
  const normalized = String(value ?? "").trim().toLowerCase().replace(/_/g, "-");
  if (!normalized) {
    return null;
  }

  if (normalized === "ko" || normalized === "kr" || normalized === "kor" || normalized === "korean") {
    return "ko";
  }
  if (normalized.startsWith("ko-") || normalized.startsWith("kr-")) {
    return "ko";
  }

  if (normalized === "en" || normalized === "eng" || normalized === "english") {
    return "en";
  }
  if (normalized.startsWith("en-")) {
    return "en";
  }

  return null;
}
