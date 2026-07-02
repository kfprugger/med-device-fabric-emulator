import { Suspense, lazy } from "react";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { Button, Spinner, Text, Title2 } from "@fluentui/react-components";
import { Layout } from "./components/Layout";
import { RouteErrorBoundary } from "./components/RouteErrorBoundary";
import { AppStateProvider } from "./AppState";

const Preflight = lazy(() => import("./pages/Preflight").then((m) => ({ default: m.Preflight })));
const DeployWizard = lazy(() => import("./pages/DeployWizard").then((m) => ({ default: m.DeployWizard })));
const PhaseMonitor = lazy(() => import("./pages/PhaseMonitor").then((m) => ({ default: m.PhaseMonitor })));
const DeploymentHistory = lazy(() => import("./pages/DeploymentHistory").then((m) => ({ default: m.DeploymentHistory })));
const TeardownView = lazy(() => import("./pages/TeardownView").then((m) => ({ default: m.TeardownView })));
const TeardownMonitor = lazy(() => import("./pages/TeardownMonitor").then((m) => ({ default: m.TeardownMonitor })));
const TeardownBatch = lazy(() => import("./pages/TeardownBatch").then((m) => ({ default: m.TeardownBatch })));

function PageLoading() {
  return (
    <div style={{ minHeight: 280, display: "grid", placeItems: "center" }}>
      <Spinner label="Loading page..." />
    </div>
  );
}

function NotFound() {
  return (
    <div style={{ display: "grid", gap: 12 }}>
      <Title2>Page not found</Title2>
      <Text>The requested orchestrator page does not exist.</Text>
      <Button appearance="primary" as="a" href="/deploy">
        Go to Deploy
      </Button>
    </div>
  );
}

export function App() {
  return (
    <AppStateProvider>
      <BrowserRouter>
        <RouteErrorBoundary>
          <Suspense fallback={<PageLoading />}>
            <Routes>
              <Route element={<Layout />}>
                <Route path="/" element={<Navigate to="/deploy" replace />} />
                <Route path="/preflight" element={<Preflight />} />
                <Route path="/deploy" element={<DeployWizard />} />
                <Route path="/monitor/:instanceId" element={<PhaseMonitor />} />
                <Route path="/history" element={<DeploymentHistory />} />
                <Route path="/teardown" element={<TeardownView />} />
                <Route path="/teardown/monitor" element={<TeardownMonitor />} />
                <Route path="/teardown/batch/:batchId" element={<TeardownBatch />} />
                <Route path="*" element={<NotFound />} />
              </Route>
            </Routes>
          </Suspense>
        </RouteErrorBoundary>
      </BrowserRouter>
    </AppStateProvider>
  );
}
