import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const projectRoot = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig(({ mode }) => {
  const isAdminBuild = mode === "admin";
  const entryName = isAdminBuild ? "admin" : "widget";

  return {
    // widget과 admin은 배포 주체가 다르므로 서로 독립된 정적 artifact로 만든다.
    base: "./",
    root: resolve(projectRoot, "src/entries", entryName),
    plugins: [react()],
    build: {
      outDir: resolve(projectRoot, "dist", entryName),
      emptyOutDir: true,
    },
  };
});
