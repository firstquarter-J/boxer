export type WidgetLanguage = "en" | "ko";

export interface WidgetIdentity {
  id: string;
  email?: string;
  name?: string;
}

export interface WidgetContext {
  language?: WidgetLanguage;
  tags?: string[];
  metadata?: Record<string, unknown>;
}

export interface WidgetBootOptions {
  boxerWebUrl: string;
  widgetUrl?: string;
  mount?: HTMLElement;
  autoOpen?: boolean;
  identity?: WidgetIdentity;
  context?: WidgetContext;
}

export interface WidgetController {
  open(): void;
  close(): void;
  identify(identity: WidgetIdentity): void;
  setContext(context: WidgetContext): void;
  destroy(): void;
}

const SDK_SOURCE = "boxer-widget-sdk";

export function boot(options: WidgetBootOptions): WidgetController {
  const mount = options.mount ?? document.body;
  const widgetUrl = new URL(options.widgetUrl ?? "/boxer-widget/", window.location.href);
  const boxerWebUrl = normalizeHttpOrigin(options.boxerWebUrl, "boxerWebUrl");
  const root = document.createElement("div");
  const iframe = document.createElement("iframe");
  let ready = false;
  let destroyed = false;
  let latestIdentity = options.identity;
  let latestContext = options.context;

  root.style.position = "fixed";
  root.style.right = "24px";
  root.style.bottom = "24px";
  root.style.width = "min(420px, calc(100vw - 32px))";
  root.style.height = "min(720px, calc(100vh - 32px))";
  root.style.borderRadius = "12px";
  root.style.overflow = "hidden";
  root.style.zIndex = "9999";
  root.style.boxShadow = "0 16px 48px rgba(15, 23, 42, 0.18)";
  root.style.display = options.autoOpen === false ? "none" : "block";
  root.style.background = "white";

  // iframe 문서는 서비스가 호스팅하고, 실제 질의는 명시한 Boxer Web origin으로 보낸다.
  widgetUrl.searchParams.set("embedded", "1");
  widgetUrl.searchParams.set("boxerWebUrl", boxerWebUrl);
  widgetUrl.searchParams.set("parentOrigin", window.location.origin);
  iframe.src = widgetUrl.toString();
  iframe.title = "Boxer Widget";
  iframe.allow = "clipboard-read; clipboard-write";
  iframe.style.width = "100%";
  iframe.style.height = "100%";
  iframe.style.border = "0";

  iframe.addEventListener("load", () => {
    ready = true;
    flushState();
  });

  root.appendChild(iframe);
  mount.appendChild(root);

  function postMessage(type: string, payload?: unknown) {
    if (!ready || !iframe.contentWindow) {
      return;
    }
    iframe.contentWindow.postMessage(
      {
        source: SDK_SOURCE,
        type,
        payload,
      },
      widgetUrl.origin,
    );
  }

  function flushState() {
    if (latestIdentity) {
      postMessage("identify", latestIdentity);
    }
    if (latestContext) {
      postMessage("setContext", latestContext);
    }
  }

  return {
    open() {
      if (destroyed) {
        return;
      }
      root.style.display = "block";
    },
    close() {
      if (destroyed) {
        return;
      }
      root.style.display = "none";
    },
    identify(identity) {
      latestIdentity = identity;
      postMessage("identify", identity);
    },
    setContext(context) {
      latestContext = context;
      postMessage("setContext", context);
    },
    destroy() {
      if (destroyed) {
        return;
      }
      destroyed = true;
      root.remove();
    },
  };
}

function normalizeHttpOrigin(value: string, optionName: string): string {
  const normalizedValue = String(value ?? "").trim();
  if (!normalizedValue) {
    throw new Error(`Boxer Widget ${optionName} is required.`);
  }

  const url = new URL(normalizedValue, window.location.href);
  if (!["http:", "https:"].includes(url.protocol)) {
    throw new Error(`Boxer Widget ${optionName} must use http or https.`);
  }
  return url.origin;
}
