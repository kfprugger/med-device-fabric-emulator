import React from "react";
import { useState, useEffect } from "react";
import ReactDOM from "react-dom/client";
import { FluentProvider } from "@fluentui/react-components";
import { App } from "./App";
import { fabricLightTheme, fabricDarkTheme } from "./theme";

/**
 * Root wrapper that selects light/dark theme based on system preference.
 * Per the Fabric theming guide: "light/dark mode support is mandatory."
 */
function Root() {
  const getInitialTheme = () => {
    if (typeof window === "undefined") return false;
    const stored = window.localStorage.getItem("orchestrator-theme");
    if (stored === "dark") return true;
    if (stored === "light") return false;
    return window.matchMedia?.("(prefers-color-scheme: dark)").matches ?? false;
  };

  const [isDark, setIsDark] = useState(getInitialTheme);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const handleThemeOverride = () => setIsDark(getInitialTheme());
    window.addEventListener("orchestrator-theme-change", handleThemeOverride);
    if (!window.matchMedia) return () => window.removeEventListener("orchestrator-theme-change", handleThemeOverride);
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    const handler = (e: MediaQueryListEvent) => {
      if (!window.localStorage.getItem("orchestrator-theme")) setIsDark(e.matches);
    };
    mq.addEventListener("change", handler);
    return () => {
      window.removeEventListener("orchestrator-theme-change", handleThemeOverride);
      mq.removeEventListener("change", handler);
    };
  }, []);

  return (
    <FluentProvider theme={isDark ? fabricDarkTheme : fabricLightTheme}>
      <App />
    </FluentProvider>
  );
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <Root />
  </React.StrictMode>
);
