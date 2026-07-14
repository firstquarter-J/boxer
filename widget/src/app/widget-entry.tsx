import React from "react";
import { createRoot } from "react-dom/client";

import { WidgetApp } from "./widget-main";
import "./styles.css";

// 서비스가 호스팅한 iframe 문서 안에서만 widget React tree를 시작한다.
const rootElement = document.getElementById("root");
if (rootElement) {
  createRoot(rootElement).render(
    <React.StrictMode>
      <WidgetApp />
    </React.StrictMode>,
  );
}
