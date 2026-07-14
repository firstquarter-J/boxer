import React from "react";
import { createRoot } from "react-dom/client";

import { AdminApp } from "./admin-main";
import "./styles.css";

// admin artifact는 Boxer Web이 같은 origin에서 제공하므로 별도 entry로 시작한다.
const rootElement = document.getElementById("root");
if (rootElement) {
  createRoot(rootElement).render(
    <React.StrictMode>
      <AdminApp />
    </React.StrictMode>,
  );
}
